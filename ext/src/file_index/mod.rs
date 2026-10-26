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
//! - `write`: the write hook and the facts it records.
//! - [`scan`]: the scan hook.
//! - [`counts`]: hourly counts.

mod backfill;
pub mod counts;
pub mod format;
pub mod scan;
mod write;

use std::sync::Arc;
use std::time::Duration;

use datafusion::common::config::ConfigExtension;
use datafusion_ducklake::extension::{
    DuckLakeExtension, FileWriteHook, ScanContext, ScanHook, WriteContext,
};
use datafusion_ducklake::metadata_provider::MetadataProvider;
use datafusion_ducklake::metadata_writer::MetadataWriter;
use object_store::{ObjectStore, ObjectStoreExt};

pub use backfill::{BuildIndexOptions, BuildIndexResult, FileIndexTableExt};
pub use write::{COLUMNS_SETTING, COUNT_COLUMN_SETTING, FileIndexRow, object_key};

/// The extension's name, which namespaces its facts.
pub const NAME: &str = "file_index";

/// The file index extension. See the module docs.
#[derive(Debug, Clone)]
pub struct FileIndex {
    scans: bool,
    counts: bool,
}

impl Default for FileIndex {
    fn default() -> Self {
        Self {
            scans: true,
            counts: false,
        }
    }
}

impl FileIndex {
    /// Scans use the index; [`counts::BucketCountRule`] leaves the table alone.
    pub fn new() -> Self {
        Self::default()
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
        write::write_hook(ctx, None)
    }

    /// `None` when scans are off, for the table or for the session
    /// ([`FileIndexSessionOptions`]), or when no filter is one the index
    /// answers: the facts are then never read.
    fn scan_hook(&self, ctx: &ScanContext<'_>) -> Option<Arc<dyn ScanHook>> {
        if !scans_enabled(ctx.session.config_options(), self.scans) {
            return None;
        }
        let predicates =
            scan::eligible_predicates(ctx.filters, ctx.physical_schema, ctx.column_ids);
        (!predicates.is_empty()).then(|| {
            Arc::new(scan::IndexScanHook {
                predicates,
            }) as Arc<dyn ScanHook>
        })
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

/// The index facts of `table_id`'s files at `paths` for `column_ids`. A
/// caller applies one only when its size (and format version and value
/// type) match the listed file; anything else, including no fact, means
/// "scan normally". A fact this build cannot read is left out.
pub fn file_index_rows(
    provider: &dyn MetadataProvider,
    table_id: i64,
    paths: &[&str],
    column_ids: &[i64],
) -> datafusion_ducklake::Result<Vec<FileIndexRow>> {
    if column_ids.is_empty() {
        return Ok(Vec::new());
    }
    Ok(provider
        .file_facts(NAME, table_id, paths, Some(column_ids))?
        .iter()
        .filter_map(FileIndexRow::from_fact)
        .collect())
}

/// Drop the facts of index objects whose data file no catalog row lists any
/// more and that are older than `min_age`, then delete those objects from
/// `object_store`. Run it after expiry and cleanup; `min_age` must exceed the
/// longest write still running, as for the orphan sweep's cutoff. Returns the
/// keys deleted. An object already gone counts as deleted; any other delete
/// failure is warned and left for the orphan sweep, since its fact is
/// already gone.
pub async fn prune_file_indexes(
    writer: &dyn MetadataWriter,
    object_store: Arc<dyn ObjectStore>,
    min_age: Duration,
) -> datafusion_ducklake::Result<Vec<String>> {
    let pruned = writer.prune_file_facts(NAME, min_age)?;
    let mut deleted = Vec::with_capacity(pruned.len());
    for fact in pruned {
        let Some(row) = FileIndexRow::from_fact(&fact) else {
            continue;
        };
        match object_store.delete(&object_key(&row.index_key)).await {
            Ok(())
            | Err(object_store::Error::NotFound {
                ..
            }) => deleted.push(row.index_key),
            Err(error) => {
                tracing::warn!(%error, key = row.index_key, "file_index: pruned fact's object not deleted");
            },
        }
    }
    Ok(deleted)
}
