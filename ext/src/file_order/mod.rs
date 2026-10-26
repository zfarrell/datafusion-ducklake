//! Verified file order: the writer checks every file's rows against the
//! table's sort key and records the files that follow it, and a scan declares
//! that order to DataFusion only for the recorded files.
//!
//! Register [`FileOrder`] in a table's write options to check and record, and
//! in its read options to declare (or both, with
//! `DuckLakeCatalog::with_extension`). [`FileOrderTableExt::verify_file_orders`]
//! records files written without it.
//!
//! A record is a [`FileFact`] of extension [`NAME`], key `0`, whose value is
//! `{"sort_key": "<field_id>:<asc|desc>:<nulls_first|nulls_last>,...",
//! "verified_by": "write" | "verify"}`. It describes the object at its path,
//! which DuckLake never rewrites in place, so it holds at every snapshot that
//! lists the file. A file without a record is scanned as it always was.
//! [`prune_file_orders`] drops records whose file has left the catalog.

mod check;
mod verify;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_ducklake::extension::{
    DuckLakeExtension, FieldSort, FileFact, FileObserver, FileWriteHook, ScanContext, ScanFile,
    ScanHook, StoredFile, WriteContext, WriteOrigin,
};
use datafusion_ducklake::metadata_provider::DuckLakeTableFile;
use datafusion_ducklake::metadata_writer::MetadataWriter;
use datafusion_ducklake::sort::{NullOrder, SortDirection};
use parquet::file::metadata::SortingColumn;

use check::{AttestedKey, SortCheck};
pub use verify::{FileOrderTableExt, VerifyOrderOptions, VerifyOrderResult};

/// The extension's name, which namespaces its facts.
pub const NAME: &str = "file_order";

/// Recorded by the writer, for a file it wrote.
const BY_WRITE: &str = "write";
/// Recorded by [`FileOrderTableExt::verify_file_orders`], after reading the file.
const BY_VERIFY: &str = "verify";

/// Recorded sort keys a catalog's tables keep across queries, by `(path,
/// size)`: a record describes immutable bytes, so it never goes stale.
const KNOWN_MAX_ENTRIES: usize = 200_000;

/// The verified file order extension. See the module docs.
#[derive(Debug, Default)]
pub struct FileOrder;

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
        order_write_hook(ctx, None)
    }

    fn scan_hook(&self, _ctx: &ScanContext<'_>) -> Option<Arc<dyn ScanHook>> {
        Some(Arc::new(OrderScanHook))
    }
}

/// The write hook for the table's live sort key. `skip` lists the files a
/// backfill passes over: already recorded under that key.
fn order_write_hook(
    ctx: &WriteContext<'_>,
    skip: Option<Arc<HashSet<(String, i64)>>>,
) -> datafusion_ducklake::Result<Option<Arc<dyn FileWriteHook>>> {
    let Some(spec) = ctx.live_sort_spec()? else {
        return Ok(None);
    };
    let Some(keys) = spec.producible_columns().filter(|keys| !keys.is_empty()) else {
        return Ok(None);
    };
    Ok(Some(Arc::new(OrderWriteHook {
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
        skip,
    })))
}

/// One write's check: the table's sort key, by column name.
#[derive(Debug)]
struct OrderWriteHook {
    verified_by: &'static str,
    keys: Vec<(String, SortOptions)>,
    skip: Option<Arc<HashSet<(String, i64)>>>,
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
            verified_by: self.verified_by,
            check,
        }))
    }

    /// Only the key columns, and nothing of a file already recorded.
    fn backfill_columns(&self, file: &DuckLakeTableFile) -> Option<Vec<String>> {
        let done = self.skip.as_ref().is_some_and(|skip| {
            skip.contains(&(file.file.path.clone(), file.file.file_size_bytes))
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
    verified_by: &'static str,
    check: SortCheck,
}

#[async_trait]
impl FileObserver for OrderObserver {
    fn observe(&mut self, batch: &RecordBatch) -> datafusion_ducklake::Result<()> {
        self.check.observe(batch)
    }

    /// A record when every row was in order; nothing otherwise, and the file
    /// reads as unordered, as it always did.
    async fn stored(
        self: Box<Self>,
        file: &StoredFile,
    ) -> datafusion_ducklake::Result<Vec<FileFact>> {
        Ok(self
            .check
            .attestation()
            .map(|sort_key| {
                order_fact(
                    &file.catalog_path,
                    file.file_size_bytes,
                    &sort_key,
                    self.verified_by,
                )
            })
            .into_iter()
            .collect())
    }
}

/// A record of `path` (of `file_size_bytes`) following `sort_key`, as a
/// [`FileFact`] to record under [`NAME`].
pub fn order_fact(path: &str, file_size_bytes: i64, sort_key: &str, verified_by: &str) -> FileFact {
    FileFact {
        path: path.to_string(),
        file_size_bytes,
        key: 0,
        value: serde_json::json!({ "sort_key": sort_key, "verified_by": verified_by }).to_string(),
    }
}

/// The sort key a record's value states; `None` for a value this build
/// cannot read, which reads as no record.
fn sort_key_of(value: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(value).ok()?;
    Some(value.get("sort_key")?.as_str()?.to_string())
}

/// The records of `table_id`'s files at `paths`: each recorded file's path
/// and size, with its sort key.
fn recorded_orders(
    provider: &dyn datafusion_ducklake::metadata_provider::MetadataProvider,
    table_id: i64,
    paths: &[&str],
) -> datafusion_ducklake::Result<Vec<(String, i64, String)>> {
    Ok(provider
        .file_facts(NAME, table_id, paths, Some(&[0]))?
        .into_iter()
        .filter_map(|fact| {
            let sort_key = sort_key_of(&fact.value)?;
            Some((fact.path, fact.file_size_bytes, sort_key))
        })
        .collect())
}

/// Drop the records whose file no catalog row of their table lists any more
/// (expired, or never committed). Optional: such records are harmless. A
/// record of a write still in flight can be dropped too, which leaves that
/// file unknown, not wrong. Returns how many were dropped.
/// `DuckLakeCatalog::prune_extension_facts` does this for every registered
/// extension at once.
pub fn prune_file_orders(writer: &dyn MetadataWriter) -> datafusion_ducklake::Result<u64> {
    Ok(writer
        .prune_file_facts(NAME, std::time::Duration::ZERO)?
        .len() as u64)
}

/// Recorded sort keys by `(path, size)`, shared across a catalog's tables
/// ([`ScanContext::shared_state`]). Only records: a record never goes stale.
#[derive(Debug, Default)]
struct KnownOrders(Mutex<HashMap<(String, i64), String>>);

/// Files a table instance found without a record
/// ([`ScanContext::table_state`]). Kept per instance, since the verify job
/// may record them later: a table built for the next query asks again.
#[derive(Debug, Default)]
struct UnknownOrders(Mutex<HashSet<(String, i64)>>);

#[derive(Debug)]
struct OrderScanHook;

#[async_trait]
impl ScanHook for OrderScanHook {
    /// One query for the files neither memo knows, none once all are. A
    /// failed lookup is remembered as nothing, so the next scan asks again.
    fn orderings(
        &self,
        ctx: &ScanContext<'_>,
        files: &[ScanFile<'_>],
    ) -> HashMap<i64, Vec<FieldSort>> {
        let known = ctx.shared_state.get_or_default::<KnownOrders>();
        let unknown_memo = ctx.table_state.get_or_default::<UnknownOrders>();
        let key = |f: &ScanFile<'_>| (f.file.file.path.clone(), f.file.file.file_size_bytes);
        let known_lock = || {
            known
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        };
        let unknown_lock = || {
            unknown_memo
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        };
        let unknown: Vec<&ScanFile<'_>> = {
            let known = known_lock();
            let absent = unknown_lock();
            files
                .iter()
                .filter(|f| {
                    let key = key(f);
                    !known.contains_key(&key) && !absent.contains(&key)
                })
                .collect()
        };
        // No lock is held across the query: a concurrent scan may ask too,
        // and both store the same answer.
        if !unknown.is_empty() {
            let paths: Vec<&str> = unknown.iter().map(|f| f.file.file.path.as_str()).collect();
            match recorded_orders(ctx.metadata.as_ref(), ctx.table_id, &paths) {
                Ok(recorded) => {
                    let mut recorded: HashMap<String, (i64, String)> = recorded
                        .into_iter()
                        .map(|(path, size, sort_key)| (path, (size, sort_key)))
                        .collect();
                    let mut known = known_lock();
                    let mut absent = unknown_lock();
                    if known.len() + unknown.len() > KNOWN_MAX_ENTRIES {
                        known.clear();
                    }
                    for f in unknown {
                        match recorded
                            .remove(&f.file.file.path)
                            .filter(|(size, _)| *size == f.file.file.file_size_bytes)
                        {
                            Some((_, sort_key)) => {
                                known.insert(key(f), sort_key);
                            },
                            None => {
                                absent.insert(key(f));
                            },
                        }
                    }
                },
                Err(error) => {
                    tracing::debug!(%error, "file_order lookup failed; scanning unordered");
                },
            }
        }
        let known = known_lock();
        files
            .iter()
            .filter_map(|f| {
                let keys = check::decode(known.get(&key(f))?)?;
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

/// [`FileOrder`]'s write hook for a backfill, passing over `recorded` files.
#[derive(Debug)]
struct VerifyOrder {
    recorded: Arc<HashSet<(String, i64)>>,
}

impl DuckLakeExtension for VerifyOrder {
    fn name(&self) -> &str {
        NAME
    }

    fn write_hook(
        &self,
        ctx: &WriteContext<'_>,
    ) -> datafusion_ducklake::Result<Option<Arc<dyn FileWriteHook>>> {
        order_write_hook(ctx, Some(Arc::clone(&self.recorded)))
    }
}
