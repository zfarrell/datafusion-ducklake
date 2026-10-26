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
//! is read exactly as it would be without it, and a hook that fails is ignored
//! with a warning. An extension can never fail a write or a scan, and so
//! correctness never depends on it. This matters because other DuckLake clients
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
//! An extension records facts about a data file with
//! [`MetadataWriter::record_file_facts`](crate::metadata_writer::MetadataWriter::record_file_facts)
//! and reads them back with
//! [`MetadataProvider::file_facts`](crate::metadata_provider::MetadataProvider::file_facts).
//! A fact is keyed by table, path and size. DuckLake never rewrites a data file
//! in place, so a fact stays true for as long as its file is listed. It needs
//! no snapshot, and it may be recorded before the file's commit.
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

/// Code that adds work to a table's writes and scans. See the module docs for
/// the contract every hook follows.
pub trait DuckLakeExtension: std::any::Any + Send + Sync + fmt::Debug {
    /// A stable name. It namespaces the extension's file facts.
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

/// One record an extension keeps about a data file. See the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileFact {
    /// The data file's catalog path (`ducklake_data_file.path`).
    pub path: String,
    /// The data file's size. A file of another size at `path` is another
    /// file, and the fact does not apply to it.
    pub file_size_bytes: i64,
    /// Distinguishes several facts about one file, such as one per column.
    /// Use `0` when there is one.
    pub key: i64,
    /// The extension's own encoding.
    pub value: String,
}

impl FileFact {
    /// Whether this fact describes `file`: same path and same size.
    pub fn applies_to(&self, file: &DuckLakeTableFile) -> bool {
        self.path == file.file.path && self.file_size_bytes == file.file.file_size_bytes
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
}

#[cfg(feature = "write")]
impl WriteContext<'_> {
    /// The table's stored `key` setting, table scope first. Keys an extension
    /// owns start with `ext_`; this crate stores them as given.
    pub fn setting(&self, key: &str) -> crate::Result<Option<String>> {
        self.metadata
            .table_setting(self.schema_name, self.table_name, key)
    }
}

/// The per-file work of one write. See [`DuckLakeExtension::write_hook`].
#[cfg(feature = "write")]
pub trait FileWriteHook: Send + Sync + fmt::Debug {
    /// A data file is opened with `file_schema`: its top-level fields carry
    /// `PARQUET:field_id` (a column's id; system columns this crate embeds
    /// carry reserved ids). Returns what watches this file, or `None`.
    fn open_file(&self, file_schema: &SchemaRef) -> Option<Box<dyn FileObserver>>;
}

/// Watches the rows of one data file as it is written.
#[cfg(feature = "write")]
#[async_trait]
pub trait FileObserver: Send + fmt::Debug {
    /// The next batch written to the file, in file order: a row's position in
    /// the file is the number of rows observed before it. An error drops this
    /// observer for the rest of the file; the write goes on.
    fn observe(&mut self, batch: &arrow::record_batch::RecordBatch) -> crate::Result<()>;

    /// The file is complete and stored at `file.object_path`. It is not
    /// committed yet, and may never be: a fact recorded here describes the
    /// object, not the catalog.
    async fn stored(self: Box<Self>, file: &StoredFile);
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

/// The write hooks of one write, from every extension that returned one.
#[cfg(feature = "write")]
#[derive(Clone, Debug, Default)]
pub(crate) struct FileHooks(Arc<Vec<Arc<dyn FileWriteHook>>>);

#[cfg(feature = "write")]
impl FileHooks {
    /// Ask every extension for its hook. An extension that errs is left out.
    pub(crate) fn begin(extensions: &DuckLakeExtensions, ctx: &WriteContext<'_>) -> Option<Self> {
        let hooks: Vec<Arc<dyn FileWriteHook>> = extensions
            .iter()
            .filter_map(|extension| match extension.write_hook(ctx) {
                Ok(hook) => hook,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        extension = extension.name(),
                        "extension write hook failed; writing without it"
                    );
                    None
                },
            })
            .collect();
        (!hooks.is_empty()).then(|| Self(Arc::new(hooks)))
    }

    /// The observers of one file written with `file_schema`.
    pub(crate) fn open_file(&self, file_schema: &SchemaRef) -> FileObservers {
        FileObservers(
            self.0
                .iter()
                .filter_map(|hook| hook.open_file(file_schema))
                .collect(),
        )
    }
}

/// The observers of one data file.
#[cfg(feature = "write")]
#[derive(Debug, Default)]
pub(crate) struct FileObservers(Vec<Box<dyn FileObserver>>);

#[cfg(feature = "write")]
impl FileObservers {
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Feed `batch` to every observer. One that errs is dropped.
    pub(crate) fn observe(&mut self, batch: &arrow::record_batch::RecordBatch) {
        self.0.retain_mut(|observer| match observer.observe(batch) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "extension file observer failed; dropping it");
                false
            },
        });
    }

    /// Tell every observer the file is stored.
    pub(crate) async fn stored(self, file: &StoredFile) {
        for observer in self.0 {
            observer.stored(file).await;
        }
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
    /// The catalog the scan reads. Use it for [`FileFact`]s.
    pub metadata: &'a Arc<dyn MetadataProvider>,
    /// The session object store the table's files are read from.
    pub object_store_url: &'a ObjectStoreUrl,
    /// State the extension keeps for this table: shared by the table's clones
    /// and rebinds, and new for a table built again.
    pub table_state: &'a ExtensionState,
}

/// State extensions keep for one [`DuckLakeTable`](crate::DuckLakeTable),
/// one value per type. The table's clones share it, including one rebound to
/// another provider; a table built again starts empty. Use it for what a
/// table may remember about its files, such as facts already read.
#[derive(Default)]
pub struct ExtensionState(
    std::sync::Mutex<HashMap<std::any::TypeId, Arc<dyn std::any::Any + Send + Sync>>>,
);

impl ExtensionState {
    /// The table's `T`, created with `T::default()` on first use.
    pub fn get_or_default<T: Default + Send + Sync + 'static>(&self) -> Arc<T> {
        let mut map = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let value = map
            .entry(std::any::TypeId::of::<T>())
            .or_insert_with(|| Arc::new(T::default()));
        Arc::clone(value)
            .downcast::<T>()
            .expect("keyed by its own type id")
    }
}

impl fmt::Debug for ExtensionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtensionState").finish_non_exhaustive()
    }
}

/// One data file a scan may read, as offered to a [`ScanHook`].
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct ScanFile<'a> {
    pub file: &'a DuckLakeTableFile,
    /// Where the file is stored, in the session's object store for the table.
    pub location: &'a ObjectPath,
    /// The file's catalog statistics over `physical_schema`, when known.
    pub statistics: Option<&'a Arc<Statistics>>,
    /// Whether a [`FileSelection::Select`] would be applied to this file.
    /// Skipping is always applied.
    pub selectable: bool,
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
    fn orderings(
        &self,
        _ctx: &ScanContext<'_>,
        _files: &[ScanFile<'_>],
    ) -> HashMap<i64, Vec<FieldSort>> {
        HashMap::new()
    }
}

/// The scan hooks of one scan.
#[derive(Clone, Debug, Default)]
pub(crate) struct ScanHooks(Vec<Arc<dyn ScanHook>>);

impl ScanHooks {
    pub(crate) fn begin(extensions: &DuckLakeExtensions, ctx: &ScanContext<'_>) -> Self {
        Self(
            extensions
                .iter()
                .filter_map(|extension| extension.scan_hook(ctx))
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
        for hook in &self.0 {
            for (id, selection) in hook.select(ctx, files).await {
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
    pub(crate) fn orderings(
        &self,
        ctx: &ScanContext<'_>,
        files: &[ScanFile<'_>],
    ) -> HashMap<i64, Vec<FieldSort>> {
        let mut out: HashMap<i64, Vec<FieldSort>> = HashMap::new();
        if files.is_empty() {
            return out;
        }
        for hook in &self.0 {
            for (id, keys) in hook.orderings(ctx, files) {
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
