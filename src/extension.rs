//! Extension points: code outside this crate can add work to a table's writes
//! and scans without changing them.
//!
//! An extension implements [`DuckLakeExtension`] and is registered on the
//! options a table is opened with:
//!
//! - in [`DuckLakeWriteOptions`](crate::table_writer::DuckLakeWriteOptions),
//!   its [`write_hook`](DuckLakeExtension::write_hook) sees every data file
//!   this crate writes;
//! - in [`DuckLakeReadOptions`](crate::DuckLakeReadOptions), its
//!   [`scan_hook`](DuckLakeExtension::scan_hook) decides per file how a scan
//!   reads it.
//!
//! [`DuckLakeCatalog::with_extension`](crate::DuckLakeCatalog::with_extension)
//! registers it in both.
//!
//! # The contract: an extension is only a hint
//!
//! Everything an extension supplies is advisory. A file it says nothing about
//! is read exactly as it would be without it, and a hook that returns an error
//! or panics is left out with a warning, so an extension cannot fail a write
//! or a scan and correctness never depends on it. Hooks run inline, so one
//! that blocks or never returns does stall its write or scan: keep them short,
//! and bound any I/O they do. This matters because other DuckLake clients
//! (official DuckDB included) write and compact files without running any
//! extension.
//!
//! Core keeps the decisions that need its own knowledge of a file's layout:
//!
//! - A scan offers hooks only files bound by field id. A name-mapped file is
//!   read as before.
//! - A row selection ([`FileSelection::Select`]) applies only to a file that
//!   has no deletes and needs no snapshot filter, in a scan without `rowid`.
//!   The scan reads any other file whole.
//! - A sort order ([`ScanHook::orderings`]) is declared only for a fixed-width,
//!   totally ordered column, for files whose statistics show no NULL in it,
//!   and only when a sort asks for it.
//! - A write hook runs only for a file whose name this crate chose.
//!
//! # Storing what an extension learns
//!
//! Core stores nothing for an extension. An extension keeps what it learns
//! about a file in its own tables in the catalog's database ([`CatalogDb`],
//! from [`WriteContext::db`] and [`ScanContext::db`]), from
//! [`FileObserver::stored`], which every write path calls once the file is
//! uploaded and before its commit. Key it by
//! the file's catalog path and size: DuckLake never rewrites a data file in
//! place, so a record about the object at that path stays true for as long as
//! the catalog lists it, at every snapshot. A record of a file whose write
//! never commits matches no listed file.
//!
//! An extension may also store objects beside a data file. Name them
//! `<data file key>.<suffix>`, where the data file key ends in `.parquet` and
//! the suffix does not. The orphan sweep in [`crate::maintenance`] deletes
//! such an object once its data file is unreferenced. Official DuckLake's
//! sweep ignores it, because it does not end in `.parquet`.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_datasource_parquet::ParquetAccessPlan;
use object_store::path::Path as ObjectPath;

use crate::metadata_provider::{DuckLakeTableFile, MetadataProvider};

#[cfg(all(
    feature = "unstable-extensions",
    any(feature = "metadata-sqlite", feature = "metadata-postgres")
))]
mod db;
#[cfg(all(
    feature = "unstable-extensions",
    any(feature = "metadata-sqlite", feature = "metadata-postgres")
))]
pub use db::{CatalogDb, CatalogPool};

/// Code that adds work to a table's writes and scans. See the module docs for
/// the contract every hook follows.
pub trait DuckLakeExtension: std::any::Any + Send + Sync + fmt::Debug {
    /// A stable name. Registering a second extension of the same name
    /// replaces the first.
    fn name(&self) -> &str;

    /// The per-file work for one write of a table, or `None` for none. Called
    /// once per write: an insert, an update, a merge or rewrite, an inlined
    /// flush. An error leaves the write without this extension.
    #[cfg(feature = "write")]
    fn write_hook(&self, _ctx: &WriteContext<'_>) -> crate::Result<Option<Arc<dyn FileWriteHook>>> {
        Ok(None)
    }

    /// How one scan of a table uses this extension, or `None` for not at all.
    fn scan_hook(&self, _ctx: &ScanContext<'_>) -> Option<Arc<dyn ScanHook>> {
        None
    }
}

/// The extensions registered on a table's read or write options, in
/// registration order.
#[derive(Clone, Default)]
pub struct DuckLakeExtensions(Vec<Arc<dyn DuckLakeExtension>>);

impl DuckLakeExtensions {
    /// These extensions and `extension`. One with the same
    /// [`name`](DuckLakeExtension::name) is replaced.
    #[must_use]
    pub fn with(mut self, extension: Arc<dyn DuckLakeExtension>) -> Self {
        self.0.retain(|e| e.name() != extension.name());
        self.0.push(extension);
        self
    }

    /// Whether none is registered.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The registered extensions, in order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn DuckLakeExtension>> {
        self.0.iter()
    }

    /// The registered extension of type `T`, if any.
    pub fn get<T: DuckLakeExtension>(&self) -> Option<&T> {
        self.0
            .iter()
            .find_map(|e| (e.as_ref() as &dyn std::any::Any).downcast_ref::<T>())
    }
}

impl fmt::Debug for DuckLakeExtensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|e| e.name()))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/// Why a data file is being written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteOrigin {
    /// Rows a caller wrote: an insert, an update, a keyed mutation.
    Insert,
    /// A merge or rewrite of the table's existing files.
    Compaction,
    /// The table's inlined rows, moved to a data file.
    InlinedFlush,
    /// An existing file read again for its observers
    /// ([`DuckLakeTable::backfill`](crate::DuckLakeTable::backfill)); nothing
    /// is written.
    Backfill,
}

/// What a [`DuckLakeExtension::write_hook`] knows about the write.
#[cfg(feature = "write")]
#[derive(Clone)]
#[non_exhaustive]
pub struct WriteContext<'a> {
    pub table_id: i64,
    pub schema_name: &'a str,
    pub table_name: &'a str,
    pub origin: WriteOrigin,
    /// The catalog the write commits to.
    pub metadata: &'a Arc<dyn crate::metadata_writer::MetadataWriter>,
    /// The store the data files are written to.
    pub object_store: &'a Arc<dyn object_store::ObjectStore>,
    lookups: &'a WriteLookups,
}

/// What one write's hooks read from the catalog, read at most once and
/// shared by every extension.
#[cfg(feature = "write")]
#[derive(Debug, Default)]
pub(crate) struct WriteLookups {
    /// The table's `ext_` settings as the catalog resolved them when the table
    /// was opened (see `DuckLakeWriteOptions`); `None` when the writer was
    /// given options of its own, and each key is then read when first asked.
    preloaded: Option<Arc<HashMap<String, String>>>,
    settings: std::sync::Mutex<HashMap<String, Option<String>>>,
    sort_spec: std::sync::OnceLock<Option<crate::sort::SortSpec>>,
}

#[cfg(feature = "write")]
impl WriteLookups {
    pub(crate) fn new(preloaded: Option<Arc<HashMap<String, String>>>) -> Self {
        Self {
            preloaded,
            ..Self::default()
        }
    }
}

#[cfg(feature = "write")]
impl<'a> WriteContext<'a> {
    pub(crate) fn new(
        table_id: i64,
        schema_name: &'a str,
        table_name: &'a str,
        origin: WriteOrigin,
        metadata: &'a Arc<dyn crate::metadata_writer::MetadataWriter>,
        object_store: &'a Arc<dyn object_store::ObjectStore>,
        lookups: &'a WriteLookups,
    ) -> Self {
        Self {
            table_id,
            schema_name,
            table_name,
            origin,
            metadata,
            object_store,
            lookups,
        }
    }

    /// The table's stored `key` setting, table scope first, then schema, then
    /// catalog. Keys an extension owns start with `ext_<name>`; this crate
    /// stores them as given. Read once per write, and not at all when the
    /// table was opened through a catalog, which already read its settings.
    pub fn setting(&self, key: &str) -> crate::Result<Option<String>> {
        if let Some(preloaded) = &self.lookups.preloaded
            && key.starts_with("ext_")
        {
            return Ok(preloaded.get(key).cloned());
        }
        if let Some(value) = self
            .lookups
            .settings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(key)
        {
            return Ok(value.clone());
        }
        let value = self
            .metadata
            .table_setting(self.schema_name, self.table_name, key)?;
        self.lookups
            .settings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key.to_string(), value.clone());
        Ok(value)
    }

    /// The table's sort spec at the catalog head, read once per write.
    pub fn live_sort_spec(&self) -> crate::Result<Option<crate::sort::SortSpec>> {
        if let Some(spec) = self.lookups.sort_spec.get() {
            return Ok(spec.clone());
        }
        let spec = self.metadata.live_sort_spec(self.table_id)?;
        Ok(self.lookups.sort_spec.get_or_init(|| spec).clone())
    }

    /// The database of the catalog the write commits to, or `None` for a
    /// backend that does not offer one.
    #[cfg(all(
        feature = "unstable-extensions",
        any(feature = "metadata-sqlite", feature = "metadata-postgres")
    ))]
    pub fn db(&self) -> Option<CatalogDb> {
        self.metadata.catalog_db()
    }
}

/// The per-file work of one write. See [`DuckLakeExtension::write_hook`].
#[cfg(feature = "write")]
pub trait FileWriteHook: Send + Sync + fmt::Debug {
    /// A data file is opened with `file_schema`: its top-level fields carry
    /// `PARQUET:field_id` (a column's id; system columns this crate embeds
    /// carry reserved ids). Returns what watches this file, or `None`.
    fn open_file(&self, file_schema: &SchemaRef) -> Option<Box<dyn FileObserver>>;

    /// For [`DuckLakeTable::backfill`](crate::DuckLakeTable::backfill): the
    /// top-level columns, by name, this hook needs to observe of the existing
    /// `file`. `None` reads every column; an empty list skips the file, which
    /// is how a hook passes over files it has already observed.
    fn backfill_columns(&self, _file: &DuckLakeTableFile) -> Option<Vec<String>> {
        None
    }
}

/// Watches the rows of one data file as it is written.
#[cfg(feature = "write")]
#[async_trait]
pub trait FileObserver: Send + fmt::Debug {
    /// The next batch written to the file, in file order: a row's position in
    /// the file is the number of rows observed before it. An error drops this
    /// observer for the rest of the file; the write goes on.
    fn observe(&mut self, batch: &arrow::record_batch::RecordBatch) -> crate::Result<()>;

    /// The file is complete and stored at `file.object_path`, and its commit
    /// has not happened yet; it may never happen. Keep what was learned about
    /// the file here (see the module docs). An error means the observer could
    /// not finish its work for the file (warned; a backfill counts the file as
    /// failed).
    async fn stored(self: Box<Self>, file: &StoredFile) -> crate::Result<()>;
}

/// A data file written and stored, before its commit.
#[cfg(feature = "write")]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StoredFile {
    /// The path the catalog will record (`ducklake_data_file.path`).
    pub catalog_path: String,
    /// The key the file is stored under.
    pub object_path: ObjectPath,
    pub file_size_bytes: i64,
    /// Rows in each row group, in file order.
    pub row_group_rows: Vec<u64>,
}

/// Run `f`, a call into an extension, turning a panic into `None`.
fn guarded<T>(what: &str, extension: &str, f: impl FnOnce() -> T) -> Option<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(value) => Some(value),
        Err(_) => {
            tracing::warn!(extension, "extension {what} panicked; going on without it");
            None
        },
    }
}

/// Await `f`, a call into an extension, turning a panic into `None`.
async fn guarded_async<T>(
    what: &str,
    extension: &str,
    f: impl std::future::Future<Output = T>,
) -> Option<T> {
    use futures::FutureExt;
    match std::panic::AssertUnwindSafe(f).catch_unwind().await {
        Ok(value) => Some(value),
        Err(_) => {
            tracing::warn!(extension, "extension {what} panicked; going on without it");
            None
        },
    }
}

/// The write hooks of one write, from every extension that returned one, as
/// `(extension name, hook)`.
#[cfg(feature = "write")]
#[derive(Clone, Debug)]
pub(crate) struct FileHooks(Arc<Vec<(String, Arc<dyn FileWriteHook>)>>);

#[cfg(feature = "write")]
impl FileHooks {
    /// Ask every extension for its hook. An extension that errs or panics is
    /// left out.
    pub(crate) fn begin(extensions: &DuckLakeExtensions, ctx: &WriteContext<'_>) -> Option<Self> {
        let hooks: Vec<(String, Arc<dyn FileWriteHook>)> = extensions
            .iter()
            .filter_map(|extension| {
                let name = extension.name();
                match guarded("write hook", name, || extension.write_hook(ctx))? {
                    Ok(hook) => hook.map(|hook| (name.to_string(), hook)),
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            extension = name,
                            "extension write hook failed; writing without it"
                        );
                        None
                    },
                }
            })
            .collect();
        (!hooks.is_empty()).then(|| Self(Arc::new(hooks)))
    }

    /// The observers of one file written with `file_schema`.
    pub(crate) fn open_file(&self, file_schema: &SchemaRef) -> FileObservers {
        self.open_file_with(file_schema, None)
    }

    /// [`Self::open_file`] for the hooks `include` marks (all for `None`).
    pub(crate) fn open_file_with(
        &self,
        file_schema: &SchemaRef,
        include: Option<&[bool]>,
    ) -> FileObservers {
        FileObservers {
            observers: self
                .0
                .iter()
                .enumerate()
                .filter(|(i, _)| include.is_none_or(|include| include[*i]))
                .filter_map(|(_, (name, hook))| {
                    let observer = guarded("open_file", name, || hook.open_file(file_schema))??;
                    Some((name.clone(), observer))
                })
                .collect(),
        }
    }

    /// For a backfill of `file`: the columns the hooks need (`None` = all,
    /// empty = none), and which hooks want the file at all.
    pub(crate) fn backfill_plan(
        &self,
        file: &DuckLakeTableFile,
    ) -> (Option<Vec<String>>, Vec<bool>) {
        let mut all = false;
        let mut columns: Vec<String> = Vec::new();
        let mut wants = Vec::with_capacity(self.0.len());
        for (name, hook) in self.0.iter() {
            match guarded("backfill_columns", name, || hook.backfill_columns(file)) {
                // A hook that panics wants nothing.
                None => wants.push(false),
                Some(None) => {
                    all = true;
                    wants.push(true);
                },
                Some(Some(names)) => {
                    wants.push(!names.is_empty());
                    for name in names {
                        if !columns.contains(&name) {
                            columns.push(name);
                        }
                    }
                },
            }
        }
        let columns = if all && wants.iter().any(|w| *w) {
            None
        } else {
            Some(columns)
        };
        (columns, wants)
    }
}

/// The observers of one data file.
#[cfg(feature = "write")]
#[derive(Debug, Default)]
pub(crate) struct FileObservers {
    observers: Vec<(String, Box<dyn FileObserver>)>,
}

#[cfg(feature = "write")]
impl FileObservers {
    pub(crate) fn is_empty(&self) -> bool {
        self.observers.is_empty()
    }

    /// Feed `batch` to every observer. One that errs or panics is dropped.
    pub(crate) fn observe(&mut self, batch: &arrow::record_batch::RecordBatch) {
        self.observers.retain_mut(|(name, observer)| {
            match guarded("observer", name, || observer.observe(batch)) {
                Some(Ok(())) => true,
                Some(Err(error)) => {
                    tracing::warn!(%error, extension = name.as_str(), "extension file observer failed; dropping it");
                    false
                },
                None => false,
            }
        });
    }

    /// Tell every observer the file is stored. Returns whether every observer
    /// finished.
    pub(crate) async fn stored(self, file: &StoredFile) -> bool {
        let mut finished = true;
        for (name, observer) in self.observers {
            match guarded_async("stored", &name, observer.stored(file)).await {
                Some(Ok(())) => {},
                Some(Err(error)) => {
                    tracing::warn!(
                        %error,
                        extension = name.as_str(),
                        path = file.catalog_path,
                        "extension observer did not finish; the file reads as unknown"
                    );
                    finished = false;
                },
                None => finished = false,
            }
        }
        finished
    }
}

// ---------------------------------------------------------------------------
// Scans
// ---------------------------------------------------------------------------

/// What a [`DuckLakeExtension::scan_hook`] knows about the scan.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct ScanContext<'a> {
    pub session: &'a dyn Session,
    pub table_id: i64,
    /// The table's columns as the scan reads them, before projection.
    pub physical_schema: &'a SchemaRef,
    /// The DuckLake `column_id` of each field of `physical_schema`.
    pub column_ids: &'a [i64],
    /// The scan's filters, over `physical_schema`, split at `AND`. They stay
    /// in the plan above the scan, so a hook only has to keep every row that
    /// can match, never drop exactly the rows that do not.
    pub filters: &'a [Arc<dyn PhysicalExpr>],
    /// The catalog the scan reads.
    pub metadata: &'a Arc<dyn MetadataProvider>,
    /// The session object store the table's files are read from.
    pub object_store_url: &'a ObjectStoreUrl,
}

impl ScanContext<'_> {
    /// The database of the catalog the scan reads, or `None` for a backend
    /// that does not offer one.
    #[cfg(all(
        feature = "unstable-extensions",
        any(feature = "metadata-sqlite", feature = "metadata-postgres")
    ))]
    pub fn db(&self) -> Option<CatalogDb> {
        self.metadata.catalog_db()
    }
}

/// One data file a scan may read, as offered to a [`ScanHook`].
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct ScanFile<'a> {
    pub file: &'a DuckLakeTableFile,
    /// The file's catalog statistics over `physical_schema`, when known.
    pub statistics: Option<&'a Arc<Statistics>>,
    /// Whether a [`FileSelection::Select`] would be applied to this file.
    /// Skipping is always applied.
    pub selectable: bool,
    pub(crate) table: &'a crate::DuckLakeTable,
}

impl ScanFile<'_> {
    /// Where the file is stored, in the session's object store for the table.
    /// Resolved on each call, so a hook pays only for the files it reads.
    pub fn location(&self) -> crate::Result<ObjectPath> {
        Ok(self.table.scan_location(&self.file.file)?)
    }
}

/// How a scan reads one file, as a [`ScanHook`] decides.
#[derive(Debug, Clone)]
pub enum FileSelection {
    /// No row of the file can match the filters. The scan skips the file.
    Skip,
    /// Only the rows this plan selects can match. Applied to a
    /// [`selectable`](ScanFile::selectable) file only; another is read whole.
    Select(Arc<ParquetAccessPlan>),
}

/// One key of a file's sort order: a column, by field id, and its direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldSort {
    pub field_id: i64,
    pub options: SortOptions,
}

/// One scan's use of an extension. Both methods are called once per page of
/// candidate files, after the catalog statistics have pruned them.
#[async_trait]
pub trait ScanHook: Send + Sync + fmt::Debug {
    /// How to read each of `files`. A file without an entry is read whole.
    /// When several hooks decide for one file, a skip wins, then the first
    /// selection.
    async fn select(
        &self,
        _ctx: &ScanContext<'_>,
        _files: &[ScanFile<'_>],
    ) -> HashMap<i64, FileSelection> {
        HashMap::new()
    }

    /// The order each of `files` keeps its rows in, primary key first. The
    /// claim must hold for every row of the file: core declares it to the
    /// planner, which may then skip a sort. Offered only files the scan reads
    /// without deletes or a snapshot filter, in a scan without `rowid`.
    async fn orderings(
        &self,
        _ctx: &ScanContext<'_>,
        _files: &[ScanFile<'_>],
    ) -> HashMap<i64, Vec<FieldSort>> {
        HashMap::new()
    }
}

/// The scan hooks of one scan, by extension name.
#[derive(Clone, Debug, Default)]
pub(crate) struct ScanHooks(Vec<(String, Arc<dyn ScanHook>)>);

impl ScanHooks {
    /// Every extension's hook. One that panics is left out.
    pub(crate) fn begin(extensions: &DuckLakeExtensions, ctx: &ScanContext<'_>) -> Self {
        Self(
            extensions
                .iter()
                .filter_map(|extension| {
                    let name = extension.name();
                    let hook = guarded("scan hook", name, || extension.scan_hook(ctx))??;
                    Some((name.to_string(), hook))
                })
                .collect(),
        )
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Every hook's selections, combined: a skip wins, then the first
    /// selection. Selections for files that are not selectable are dropped.
    pub(crate) async fn select(
        &self,
        ctx: &ScanContext<'_>,
        files: &[ScanFile<'_>],
    ) -> HashMap<i64, FileSelection> {
        let mut out: HashMap<i64, FileSelection> = HashMap::new();
        if files.is_empty() {
            return out;
        }
        let selectable: HashMap<i64, bool> = files
            .iter()
            .map(|f| (f.file.data_file_id, f.selectable))
            .collect();
        for (name, hook) in &self.0 {
            let Some(selections) = guarded_async("select", name, hook.select(ctx, files)).await
            else {
                continue;
            };
            for (id, selection) in selections {
                let Some(&selectable) = selectable.get(&id) else {
                    continue;
                };
                match (out.get(&id), &selection) {
                    (Some(FileSelection::Skip), _) => {},
                    (_, FileSelection::Skip) => {
                        out.insert(id, selection);
                    },
                    (Some(FileSelection::Select(_)), _) => {},
                    (None, FileSelection::Select(_)) if selectable => {
                        out.insert(id, selection);
                    },
                    (None, FileSelection::Select(_)) => {},
                }
            }
        }
        out
    }

    /// Every hook's orderings: the first claim for a file wins.
    pub(crate) async fn orderings(
        &self,
        ctx: &ScanContext<'_>,
        files: &[ScanFile<'_>],
    ) -> HashMap<i64, Vec<FieldSort>> {
        let mut out: HashMap<i64, Vec<FieldSort>> = HashMap::new();
        if files.is_empty() {
            return out;
        }
        for (name, hook) in &self.0 {
            let Some(orderings) =
                guarded_async("orderings", name, hook.orderings(ctx, files)).await
            else {
                continue;
            };
            for (id, keys) in orderings {
                if !keys.is_empty() {
                    out.entry(id).or_insert(keys);
                }
            }
        }
        out
    }
}

/// The data file key a companion object belongs to (see the module docs):
/// `<data>.parquet` for a key `<data>.parquet.<suffix>`, where the suffix is
/// one path segment that does not end in `.parquet`.
pub fn companion_data_key(key: &str) -> Option<&str> {
    let at = key.rfind(".parquet.")?;
    let (stem, data, suffix) = (&key[..at], &key[..at + 8], &key[at + 9..]);
    (!stem.is_empty()
        && !stem.ends_with('/')
        && !suffix.is_empty()
        && !suffix.contains('/')
        && !suffix.ends_with(".parquet"))
    .then_some(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn companion_data_key_needs_a_data_file_and_a_suffix() {
        assert_eq!(
            companion_data_key("t/ducklake-1.parquet.c5.v1.pidx"),
            Some("t/ducklake-1.parquet")
        );
        assert_eq!(companion_data_key("t/ducklake-1.parquet"), None);
        assert_eq!(companion_data_key("t/ducklake-1.parquet."), None);
        assert_eq!(companion_data_key("t/a.parquet.b.parquet"), None);
        assert_eq!(companion_data_key("t/.parquet.x"), None);
        assert_eq!(companion_data_key("t/a.parquet.x/y"), None);
        assert_eq!(
            companion_data_key("t/a.parquet.x/y.parquet.z"),
            Some("t/a.parquet.x/y.parquet")
        );
    }
}
