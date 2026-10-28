//! A per-file secondary index: for each data file and indexed column, a
//! posting list of `(value, row)` stored beside the file, which a scan reads
//! to skip files and rows an equality or `IN` filter cannot match.
//!
//! Register [`FileIndex`] in a table's write options to build indexes as
//! files are written (for the columns in the table setting
//! [`COLUMNS_SETTING`]), and in its read options to use them (or both, with
//! `DuckLakeCatalog::with_extension`). [`FileIndexTableExt::build_file_indexes`]
//! indexes files written without it. [`counts::BucketCountRule`] answers
//! hourly counts from the index's per-file bucket counts.
//!
//! - [`format`]: the index object and its metadata (no catalog or scan logic).
//! - `write`: the write hook.
//! - [`table`]: the catalog table that records each index object.
//! - [`scan`]: the scan hook.
//! - [`counts`]: hourly counts.

mod backfill;
pub mod counts;
pub mod format;
pub mod scan;
pub mod table;
mod write;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::config::ConfigExtension;
use datafusion_ducklake::extension::{
    CatalogDb, DuckLakeExtension, FileWriteHook, ScanContext, ScanHook, WriteContext,
};
use object_store::{ObjectStore, ObjectStoreExt};

use crate::db::{Found, GroupCommit, RowMemo};
use format::FileIndexMeta;

pub use backfill::{BuildIndexOptions, BuildIndexResult, FileIndexTableExt};
pub use write::{COLUMNS_SETTING, COUNT_COLUMN_SETTING, FileIndexRow, object_key};

/// The extension's name.
pub const NAME: &str = "file_index";

/// Bytes of index rows a [`FileIndex`] keeps in memory, across its catalogs.
const MEMO_MAX_BYTES: u64 = 64 << 20;

/// The file index extension. See the module docs.
#[derive(Debug, Clone)]
pub struct FileIndex {
    scans: bool,
    counts: bool,
    shared: Arc<Shared>,
}

/// What every clone of one [`FileIndex`] shares: what it has read, and the
/// rows it is recording.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) memo: RowMemo<FileIndexMeta>,
    pub(crate) commits: GroupCommit<FileIndexRow>,
}

/// `read`'s rows as the memo keeps them.
async fn found(
    db: &CatalogDb,
    table_id: i64,
    paths: Vec<String>,
    column_ids: &[i64],
) -> datafusion_ducklake::Result<Found<FileIndexMeta>> {
    Ok(table::read(db, table_id, paths, column_ids.to_vec())
        .await?
        .into_iter()
        .map(|row| ((row.meta.path.clone(), row.meta.column_id), row.meta))
        .collect())
}

impl Shared {
    /// The index metas in `db` of `table_id`'s files at `paths` for
    /// `column_ids`, by `(path, column_id)`. A caller applies one only when
    /// its size (and format version and value type) match the listed file.
    pub(crate) async fn rows(
        &self,
        db: &CatalogDb,
        table_id: i64,
        paths: &[&str],
        column_ids: &[i64],
    ) -> HashMap<(String, i64), Arc<FileIndexMeta>> {
        self.memo
            .get(db, table_id, paths, column_ids, |missing| {
                found(db, table_id, missing, column_ids)
            })
            .await
    }

    /// [`Self::rows`] from synchronous code, on any runtime: it blocks the
    /// thread for the read.
    pub(crate) fn rows_blocking(
        &self,
        db: &CatalogDb,
        table_id: i64,
        paths: &[&str],
        column_ids: &[i64],
    ) -> HashMap<(String, i64), Arc<FileIndexMeta>> {
        self.memo
            .get_blocking(db, table_id, paths, column_ids, |missing| {
                let column_ids = column_ids.to_vec();
                db.run_blocking(|pool| table::read_on(pool, table_id, missing, column_ids))?
                    .map(|rows| {
                        rows.into_iter()
                            .map(|row| ((row.meta.path.clone(), row.meta.column_id), row.meta))
                            .collect()
                    })
            })
    }

    /// Record `rows` in `db`, with any other file's rows being recorded for
    /// the table at the same time, and keep them in memory.
    pub(crate) async fn record(
        &self,
        db: &CatalogDb,
        table_id: i64,
        rows: Vec<FileIndexRow>,
    ) -> datafusion_ducklake::Result<()> {
        self.commits
            .record(
                db,
                table_id,
                rows.clone(),
                |db, table_id, rows| async move { table::record(&db, table_id, rows).await },
            )
            .await?;
        for row in rows {
            let (path, column_id) = (row.meta.path.clone(), row.meta.column_id);
            self.memo.put(db, table_id, &path, column_id, row.meta);
        }
        Ok(())
    }
}

impl Default for FileIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl FileIndex {
    /// Index rows kept in the database of each catalog it runs in (see
    /// [`table`]); scans use the index, and [`counts::BucketCountRule`]
    /// leaves the table alone. One instance can serve every catalog.
    pub fn new() -> Self {
        Self {
            scans: true,
            counts: false,
            shared: Arc::new(Shared {
                memo: RowMemo::new(NAME, MEMO_MAX_BYTES, FileIndexMeta::heap_bytes),
                commits: GroupCommit::default(),
            }),
        }
    }

    /// Whether scans read the index to skip files and rows. On by default.
    #[must_use]
    pub fn with_scans(mut self, enabled: bool) -> Self {
        self.scans = enabled;
        self
    }

    /// Whether [`counts::BucketCountRule`] may answer `count(*) GROUP BY
    /// date_trunc('hour', ts)` for the table from per-file bucket counts. Off
    /// by default.
    #[must_use]
    pub fn with_counts(mut self, enabled: bool) -> Self {
        self.counts = enabled;
        self
    }

    /// Whether the counts rewrite is on.
    pub fn counts(&self) -> bool {
        self.counts
    }
}

impl DuckLakeExtension for FileIndex {
    fn name(&self) -> &str {
        NAME
    }

    fn write_hook(
        &self,
        ctx: &WriteContext<'_>,
    ) -> datafusion_ducklake::Result<Option<Arc<dyn FileWriteHook>>> {
        write::write_hook(ctx, &self.shared, None)
    }

    /// `None` when scans are off, for the table or for the session
    /// ([`FileIndexSessionOptions`]), when no filter is one the index
    /// answers, or on a catalog without a database these extensions can use:
    /// the table is then never read.
    fn scan_hook(&self, ctx: &ScanContext<'_>) -> Option<Arc<dyn ScanHook>> {
        if !scans_enabled(ctx.session.config_options(), self.scans) {
            return None;
        }
        let predicates =
            scan::eligible_predicates(ctx.filters, ctx.physical_schema, ctx.column_ids);
        if predicates.is_empty() {
            return None;
        }
        Some(Arc::new(scan::IndexScanHook {
            shared: Arc::clone(&self.shared),
            db: ctx.db()?,
            predicates,
        }))
    }
}

/// The table's `scans` setting, unless the session overrides it.
fn scans_enabled(options: &datafusion::common::config::ConfigOptions, table: bool) -> bool {
    options
        .extensions
        .get::<FileIndexSessionOptions>()
        .and_then(|options| options.enabled)
        .unwrap_or(table)
}

datafusion::common::extensions_options! {
    /// Per-session overrides, set with `SET ducklake_file_index.enabled = ...`
    /// once registered on the session's `SessionConfig`
    /// (`with_option_extension`). Unset leaves each table's setting as it is.
    pub struct FileIndexSessionOptions {
        /// Overrides [`FileIndex::with_scans`], and turns the counts rewrite
        /// off with it.
        pub enabled: Option<bool>, default = None
    }
}

impl ConfigExtension for FileIndexSessionOptions {
    const PREFIX: &'static str = "ducklake_file_index";
}

/// The index rows in `db` of `table_id`'s files at `paths` for `column_ids`,
/// read now. A caller applies one only when its size (and format version and
/// value type) match the listed file; anything else, including no row, means
/// "scan normally". A row this build cannot read is left out.
pub async fn file_index_rows(
    db: &CatalogDb,
    table_id: i64,
    paths: &[&str],
    column_ids: &[i64],
) -> datafusion_ducklake::Result<Vec<FileIndexRow>> {
    let paths: Vec<String> = paths.iter().map(|path| path.to_string()).collect();
    table::read(db, table_id, paths, column_ids.to_vec()).await
}

/// Drop the rows in `db` of index objects whose data file no catalog row
/// lists any more and that are older than `min_age`, then delete those
/// objects from `object_store`. Run it after expiry and cleanup; `min_age`
/// must exceed the longest write still running, as for the orphan sweep's
/// cutoff. Returns the keys deleted. An object already gone counts as
/// deleted; any other delete failure is warned and left for the orphan
/// sweep, since its row is already gone. `db` is the catalog's
/// (`MetadataProvider::catalog_db`).
pub async fn prune_file_indexes(
    db: &CatalogDb,
    object_store: Arc<dyn ObjectStore>,
    min_age: Duration,
) -> datafusion_ducklake::Result<Vec<String>> {
    let pruned = table::prune(db, min_age).await?;
    let mut deleted = Vec::with_capacity(pruned.len());
    for index_key in pruned {
        match object_store.delete(&object_key(&index_key)).await {
            Ok(())
            | Err(object_store::Error::NotFound {
                ..
            }) => deleted.push(index_key),
            Err(error) => {
                tracing::warn!(%error, key = index_key, "file_index: pruned row's object not deleted");
            },
        }
    }
    Ok(deleted)
}
