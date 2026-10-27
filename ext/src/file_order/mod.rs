//! Verified file order: the writer checks every file's rows against the
//! table's sort key and records the files that follow it, and a scan declares
//! that order to DataFusion only for the recorded files.
//!
//! Register [`FileOrder`] in a table's write options to check and record, and
//! in its read options to declare (or both, with
//! `DuckLakeCatalog::with_extension`). [`FileOrderTableExt::verify_file_orders`]
//! records files written without it.
//!
//! A record is a row of [`table`], in the catalog's database. It describes
//! the object at its path, which DuckLake never rewrites in place, so it
//! holds at every snapshot that lists the file. A file without a record is
//! scanned as it always was. [`prune_file_orders`] drops records whose file
//! has left the catalog.

mod check;
pub mod table;
mod verify;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_ducklake::extension::{
    DuckLakeExtension, FieldSort, FileObserver, FileWriteHook, ScanContext, ScanFile, ScanHook,
    StoredFile, WriteContext, WriteOrigin,
};
use datafusion_ducklake::metadata_provider::DuckLakeTableFile;
use datafusion_ducklake::sort::{NullOrder, SortDirection};
use parquet::file::metadata::SortingColumn;

use crate::db::{ExtensionDb, Found, RowMemo};
use check::{AttestedKey, SortCheck};
use table::OrderRow;
pub use verify::{FileOrderTableExt, VerifyOrderOptions, VerifyOrderResult};

/// The extension's name.
pub const NAME: &str = "file_order";

/// Recorded by the writer, for a file it wrote.
const BY_WRITE: &str = "write";
/// Recorded by [`FileOrderTableExt::verify_file_orders`], after reading the file.
const BY_VERIFY: &str = "verify";

/// Records a [`FileOrder`] keeps in memory, across its tables.
const MEMO_MAX_ENTRIES: u64 = 200_000;

/// A recorded file's size and sort key.
type Recorded = (i64, String);

/// The verified file order extension. See the module docs.
#[derive(Debug, Clone)]
pub struct FileOrder(Arc<Shared>);

#[derive(Debug)]
struct Shared {
    db: ExtensionDb,
    memo: RowMemo<Recorded>,
}

impl FileOrder {
    /// Records kept in `db`, the catalog's database (the pool given to
    /// core). Register one instance per catalog: records are keyed by table
    /// id.
    pub fn new(db: ExtensionDb) -> Self {
        Self(Arc::new(Shared {
            db,
            memo: RowMemo::new(NAME, MEMO_MAX_ENTRIES),
        }))
    }

    /// Where the records are kept.
    pub fn db(&self) -> &ExtensionDb {
        &self.0.db
    }
}

impl DuckLakeExtension for FileOrder {
    fn name(&self) -> &str {
        NAME
    }

    /// Checks every file against the table's live sort key, whether or not
    /// the writer sorts: the check only vouches for what it observes, so rows
    /// a caller sorted get a record and rows nobody sorted get none. `None`
    /// for an unsorted table or a key this crate cannot evaluate.
    fn write_hook(
        &self,
        ctx: &WriteContext<'_>,
    ) -> datafusion_ducklake::Result<Option<Arc<dyn FileWriteHook>>> {
        order_write_hook(ctx, &self.0, None)
    }

    fn scan_hook(&self, _ctx: &ScanContext<'_>) -> Option<Arc<dyn ScanHook>> {
        Some(Arc::new(OrderScanHook(Arc::clone(&self.0))))
    }
}

/// What a backfill adds to the write hook: the files to pass over (already
/// recorded under the current key) and a count of the files it records.
#[derive(Debug, Default)]
struct VerifyPass {
    recorded: HashSet<(String, i64)>,
    files_recorded: AtomicU64,
}

/// The write hook for the table's live sort key.
fn order_write_hook(
    ctx: &WriteContext<'_>,
    shared: &Arc<Shared>,
    verify: Option<Arc<VerifyPass>>,
) -> datafusion_ducklake::Result<Option<Arc<dyn FileWriteHook>>> {
    let Some(spec) = ctx.live_sort_spec()? else {
        return Ok(None);
    };
    let Some(keys) = spec.producible_columns().filter(|keys| !keys.is_empty()) else {
        return Ok(None);
    };
    Ok(Some(Arc::new(OrderWriteHook {
        shared: Arc::clone(shared),
        table_id: ctx.table_id,
        verified_by: match ctx.origin {
            WriteOrigin::Backfill => BY_VERIFY,
            _ => BY_WRITE,
        },
        keys: keys
            .into_iter()
            .map(|(name, direction, nulls)| {
                (
                    name,
                    SortOptions {
                        descending: direction == SortDirection::Desc,
                        nulls_first: nulls == NullOrder::NullsFirst,
                    },
                )
            })
            .collect(),
        verify,
    })))
}

/// One write's check: the table's sort key, by column name.
#[derive(Debug)]
struct OrderWriteHook {
    shared: Arc<Shared>,
    table_id: i64,
    verified_by: &'static str,
    keys: Vec<(String, SortOptions)>,
    verify: Option<Arc<VerifyPass>>,
}

impl FileWriteHook for OrderWriteHook {
    fn open_file(&self, file_schema: &SchemaRef) -> Option<Box<dyn FileObserver>> {
        let columns = self
            .keys
            .iter()
            .map(|(name, options)| {
                let (index, _) = file_schema.column_with_name(name)?;
                Some(SortingColumn {
                    column_idx: i32::try_from(index).ok()?,
                    descending: options.descending,
                    nulls_first: options.nulls_first,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        let check = SortCheck::new(file_schema, &columns)?;
        Some(Box::new(OrderObserver {
            shared: Arc::clone(&self.shared),
            table_id: self.table_id,
            verified_by: self.verified_by,
            check,
            verify: self.verify.clone(),
        }))
    }

    /// Only the key columns, and nothing of a file already recorded.
    fn backfill_columns(&self, file: &DuckLakeTableFile) -> Option<Vec<String>> {
        let done = self.verify.as_ref().is_some_and(|verify| {
            verify
                .recorded
                .contains(&(file.file.path.clone(), file.file.file_size_bytes))
        });
        Some(if done {
            Vec::new()
        } else {
            self.keys.iter().map(|(name, _)| name.clone()).collect()
        })
    }
}

#[derive(Debug)]
struct OrderObserver {
    shared: Arc<Shared>,
    table_id: i64,
    verified_by: &'static str,
    check: SortCheck,
    verify: Option<Arc<VerifyPass>>,
}

#[async_trait]
impl FileObserver for OrderObserver {
    fn observe(&mut self, batch: &RecordBatch) -> datafusion_ducklake::Result<()> {
        self.check.observe(batch)
    }

    /// A record when every row was in order; nothing otherwise, and the file
    /// reads as unordered, as it always did.
    async fn stored(self: Box<Self>, file: &StoredFile) -> datafusion_ducklake::Result<()> {
        let Some(sort_key) = self.check.attestation() else {
            return Ok(());
        };
        let row = OrderRow {
            path: file.catalog_path.clone(),
            file_size_bytes: file.file_size_bytes,
            sort_key: sort_key.clone(),
        };
        table::record(&self.shared.db, self.table_id, &row, self.verified_by).await?;
        self.shared.memo.put(
            self.table_id,
            &file.catalog_path,
            0,
            (file.file_size_bytes, sort_key),
        );
        if let Some(verify) = &self.verify {
            verify.files_recorded.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// The records of `table_id`'s files at `paths`, as the memo keeps them.
async fn found(
    db: &ExtensionDb,
    table_id: i64,
    paths: Vec<String>,
) -> datafusion_ducklake::Result<Found<Recorded>> {
    Ok(table::read(db, table_id, &paths)
        .await?
        .into_iter()
        .map(|row| ((row.path, 0), (row.file_size_bytes, row.sort_key)))
        .collect())
}

/// Drop the records whose file no catalog row of their table lists any more
/// (expired, or never committed). Optional: such records are harmless. A
/// record of a write still in flight can be dropped too, which leaves that
/// file unknown, not wrong. Returns how many were dropped.
pub async fn prune_file_orders(db: &ExtensionDb) -> datafusion_ducklake::Result<u64> {
    table::prune(db).await
}

#[derive(Debug)]
struct OrderScanHook(Arc<Shared>);

#[async_trait]
impl ScanHook for OrderScanHook {
    /// One read for the files the memo does not know, none once it knows
    /// them all.
    async fn orderings(
        &self,
        ctx: &ScanContext<'_>,
        files: &[ScanFile<'_>],
    ) -> HashMap<i64, Vec<FieldSort>> {
        let paths: Vec<&str> = files.iter().map(|f| f.file.file.path.as_str()).collect();
        let recorded = self
            .0
            .memo
            .get(ctx.table_id, &paths, &[0], |missing| {
                found(&self.0.db, ctx.table_id, missing)
            })
            .await;
        files
            .iter()
            .filter_map(|f| {
                let (size, sort_key) = recorded.get(&(f.file.file.path.clone(), 0))?.as_ref();
                if *size != f.file.file.file_size_bytes {
                    return None;
                }
                let keys = check::decode(sort_key)?;
                Some((
                    f.file.data_file_id,
                    keys.iter()
                        .map(|k: &AttestedKey| FieldSort {
                            field_id: i64::from(k.field_id),
                            options: k.options,
                        })
                        .collect(),
                ))
            })
            .collect()
    }
}

/// [`FileOrder`]'s write hook for a backfill.
#[derive(Debug)]
struct VerifyOrder {
    shared: Arc<Shared>,
    pass: Arc<VerifyPass>,
}

impl DuckLakeExtension for VerifyOrder {
    fn name(&self) -> &str {
        NAME
    }

    fn write_hook(
        &self,
        ctx: &WriteContext<'_>,
    ) -> datafusion_ducklake::Result<Option<Arc<dyn FileWriteHook>>> {
        order_write_hook(ctx, &self.shared, Some(Arc::clone(&self.pass)))
    }
}
