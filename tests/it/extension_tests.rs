//! The extension API (`datafusion_ducklake::extension`) through a probe
//! extension that keeps its own records in memory: what its hooks see, when
//! an observer is told a file is stored, that a hook which errs or panics
//! never fails a write or a scan, and the generic backfill.

#![cfg(all(
    feature = "write-sqlite",
    feature = "metadata-sqlite",
    feature = "unstable-extensions"
))]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::prelude::*;
use datafusion_ducklake::extension::{
    DuckLakeExtension, FileObserver, FileSelection, FileWriteHook, ScanContext, ScanFile, ScanHook,
    StoredFile, WriteContext, WriteOrigin,
};
use datafusion_ducklake::metadata_provider::{DuckLakeTableFile, MetadataProvider};
use datafusion_ducklake::metadata_writer::DataFileInfo;
use datafusion_ducklake::{
    BackfillOptions, ColumnDef, DuckLakeCatalog, DuckLakeTable, DuckLakeTableWriter,
    DuckLakeWriteOptions, MergeOptions, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter, WriteMode,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use tempfile::TempDir;

const NAME: &str = "probe";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn batch(values: std::ops::Range<i64>) -> RecordBatch {
    let labels: Vec<String> = values.clone().map(|v| format!("l{v}")).collect();
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from_iter_values(values)), Arc::new(StringArray::from(labels))],
    )
    .unwrap()
}

/// Where a [`Probe`] panics, or returns an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    PanicWriteHook,
    PanicOpenFile,
    PanicObserve,
    PanicStored,
    ErrObserve,
    ErrStored,
    PanicScanHook,
    PanicSelect,
}

/// `(name, field id)` of each field of an opened file.
type OpenedFields = Vec<(String, String)>;

/// What one stored file's observer kept.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    origin: WriteOrigin,
    rows: usize,
    object_path: ObjectPath,
    /// Whether the catalog listed the file when its observer was told.
    listed_when_stored: bool,
}

/// What a [`Probe`] saw and kept, shared with the test.
#[derive(Debug, Default)]
struct Seen {
    /// `(origin, schema field names with their field ids)` per opened file.
    opened: Mutex<Vec<(WriteOrigin, OpenedFields)>>,
    /// The probe's own store: one record per `(path, size)`.
    records: Mutex<HashMap<(String, i64), Record>>,
    /// The locations scan hooks resolved, by path.
    resolved: Mutex<HashMap<String, ObjectPath>>,
}

/// Keeps each file's row count, and skips scans of files it recorded with
/// exactly `skip_rows` rows (a test-only breach of the "only skip what cannot
/// match" contract, to make a skip observable in the result).
#[derive(Debug)]
struct Probe {
    fault: Fault,
    skip_rows: Option<i64>,
    seen: Arc<Seen>,
    /// Files a backfill passes over.
    done: Vec<String>,
    /// The catalog to check, when told a file is stored, whether it lists it.
    catalog_url: Option<String>,
}

impl Probe {
    fn new(fault: Fault) -> Self {
        Self {
            fault,
            skip_rows: None,
            seen: Arc::default(),
            done: Vec::new(),
            catalog_url: None,
        }
    }
}

impl DuckLakeExtension for Probe {
    fn name(&self) -> &str {
        NAME
    }

    fn write_hook(
        &self,
        ctx: &WriteContext<'_>,
    ) -> datafusion_ducklake::Result<Option<Arc<dyn FileWriteHook>>> {
        if self.fault == Fault::PanicWriteHook {
            panic!("probe: write_hook");
        }
        Ok(Some(Arc::new(ProbeHook {
            fault: self.fault,
            origin: ctx.origin,
            table_id: ctx.table_id,
            seen: Arc::clone(&self.seen),
            done: self.done.clone(),
            catalog_url: self.catalog_url.clone(),
        })))
    }

    fn scan_hook(&self, _ctx: &ScanContext<'_>) -> Option<Arc<dyn ScanHook>> {
        if self.fault == Fault::PanicScanHook {
            panic!("probe: scan_hook");
        }
        Some(Arc::new(ProbeScan {
            fault: self.fault,
            skip_rows: self.skip_rows,
            seen: Arc::clone(&self.seen),
        }))
    }
}

#[derive(Debug)]
struct ProbeHook {
    fault: Fault,
    origin: WriteOrigin,
    table_id: i64,
    seen: Arc<Seen>,
    done: Vec<String>,
    catalog_url: Option<String>,
}

impl FileWriteHook for ProbeHook {
    fn open_file(&self, file_schema: &SchemaRef) -> Option<Box<dyn FileObserver>> {
        if self.fault == Fault::PanicOpenFile {
            panic!("probe: open_file");
        }
        let fields = file_schema
            .fields()
            .iter()
            .map(|f| {
                (
                    f.name().clone(),
                    f.metadata()
                        .get("PARQUET:field_id")
                        .cloned()
                        .unwrap_or_default(),
                )
            })
            .collect();
        self.seen.opened.lock().unwrap().push((self.origin, fields));
        Some(Box::new(ProbeObserver {
            fault: self.fault,
            origin: self.origin,
            table_id: self.table_id,
            rows: 0,
            seen: Arc::clone(&self.seen),
            catalog_url: self.catalog_url.clone(),
        }))
    }

    fn backfill_columns(&self, file: &DuckLakeTableFile) -> Option<Vec<String>> {
        Some(if self.done.contains(&file.file.path) {
            Vec::new()
        } else {
            vec!["v".to_string()]
        })
    }
}

#[derive(Debug)]
struct ProbeObserver {
    fault: Fault,
    origin: WriteOrigin,
    table_id: i64,
    rows: usize,
    seen: Arc<Seen>,
    catalog_url: Option<String>,
}

#[async_trait]
impl FileObserver for ProbeObserver {
    fn observe(&mut self, batch: &RecordBatch) -> datafusion_ducklake::Result<()> {
        match self.fault {
            Fault::PanicObserve => panic!("probe: observe"),
            Fault::ErrObserve => {
                return Err(datafusion_ducklake::DuckLakeError::Internal(
                    "probe: observe".into(),
                ));
            },
            _ => {},
        }
        self.rows += batch.num_rows();
        Ok(())
    }

    async fn stored(self: Box<Self>, file: &StoredFile) -> datafusion_ducklake::Result<()> {
        match self.fault {
            Fault::PanicStored => panic!("probe: stored"),
            Fault::ErrStored => {
                return Err(datafusion_ducklake::DuckLakeError::Internal(
                    "probe: stored".into(),
                ));
            },
            _ => {},
        }
        let listed_when_stored = match &self.catalog_url {
            Some(url) => listed_paths(url, self.table_id)
                .await
                .contains(&file.catalog_path),
            None => false,
        };
        self.seen.records.lock().unwrap().insert(
            (file.catalog_path.clone(), file.file_size_bytes),
            Record {
                origin: self.origin,
                rows: self.rows,
                object_path: file.object_path.clone(),
                listed_when_stored,
            },
        );
        Ok(())
    }
}

/// The paths the catalog at `url` lists for `table_id` at its head.
async fn listed_paths(url: &str, table_id: i64) -> Vec<String> {
    let provider = SqliteMetadataProvider::new(url).await.unwrap();
    let snapshot = provider.get_current_snapshot().unwrap();
    provider
        .get_table_files_for_select(table_id, snapshot)
        .unwrap()
        .into_iter()
        .map(|f| f.file.path)
        .collect()
}

#[derive(Debug)]
struct ProbeScan {
    fault: Fault,
    skip_rows: Option<i64>,
    seen: Arc<Seen>,
}

#[async_trait]
impl ScanHook for ProbeScan {
    async fn select(
        &self,
        _ctx: &ScanContext<'_>,
        files: &[ScanFile<'_>],
    ) -> HashMap<i64, FileSelection> {
        if self.fault == Fault::PanicSelect {
            panic!("probe: select");
        }
        let records = self.seen.records.lock().unwrap();
        let mut resolved = self.seen.resolved.lock().unwrap();
        let mut out = HashMap::new();
        for f in files {
            resolved.insert(f.file.file.path.clone(), f.location().unwrap());
            let rows = records
                .get(&(f.file.file.path.clone(), f.file.file.file_size_bytes))
                .map(|record| record.rows as i64);
            if self.skip_rows.is_some() && rows == self.skip_rows {
                out.insert(f.file.data_file_id, FileSelection::Skip);
            }
        }
        out
    }
}

struct Env {
    temp: TempDir,
    table_id: i64,
}

impl Env {
    async fn new() -> Self {
        let temp = TempDir::new().unwrap();
        std::fs::create_dir_all(temp.path().join("data")).unwrap();
        let env = Self {
            temp,
            table_id: 0,
        };
        let writer = env.writer().await;
        writer.set_data_path(env.data().to_str().unwrap()).unwrap();
        let s = writer
            .begin_write_transaction("main", "t", &columns(), WriteMode::Replace)
            .unwrap();
        writer
            .publish_snapshot(
                s.table_id,
                "main",
                "t",
                s.snapshot_id,
                WriteMode::Replace,
                s.base_snapshot_id,
                &columns(),
                &s.column_ids,
            )
            .unwrap();
        Self {
            table_id: s.table_id,
            ..env
        }
    }

    fn data(&self) -> std::path::PathBuf {
        self.temp.path().join("data")
    }

    fn url(&self) -> String {
        format!(
            "sqlite:{}?mode=rwc",
            self.temp.path().join("test.db").display()
        )
    }

    async fn writer(&self) -> SqliteMetadataWriter {
        SqliteMetadataWriter::new_with_init(&self.url())
            .await
            .unwrap()
    }

    async fn provider(&self) -> SqliteMetadataProvider {
        SqliteMetadataProvider::new(&self.url()).await.unwrap()
    }

    fn options(extension: Option<Arc<Probe>>) -> DuckLakeWriteOptions {
        let options = DuckLakeWriteOptions::default().with_data_inlining_row_limit(0);
        match extension {
            Some(extension) => options.with_extension(extension),
            None => options,
        }
    }

    async fn table_writer(&self, extension: Option<Arc<Probe>>) -> DuckLakeTableWriter {
        DuckLakeTableWriter::new(
            Arc::new(self.writer().await),
            Arc::new(LocalFileSystem::new()),
        )
        .unwrap()
        .with_options(&Self::options(extension))
    }

    /// One data file of `rows`, written with `extension` (or none).
    async fn append(&self, rows: std::ops::Range<i64>, extension: Option<Arc<Probe>>) {
        self.table_writer(extension)
            .await
            .append_table("main", "t", &[batch(rows)])
            .await
            .unwrap();
    }

    /// One data file of `rows`, through a streaming write session.
    async fn stream(&self, rows: std::ops::Range<i64>, extension: Arc<Probe>) {
        let writer = self.table_writer(Some(extension)).await;
        let mut session = writer
            .begin_write("main", "t", schema().as_ref(), WriteMode::Append)
            .unwrap();
        session.write_batch(&batch(rows)).unwrap();
        session.finish().await.unwrap();
    }

    async fn catalog(&self, extension: Arc<Probe>) -> DuckLakeCatalog {
        DuckLakeCatalog::with_writer(
            Arc::new(self.provider().await),
            Arc::new(self.writer().await),
        )
        .unwrap()
        .with_write_options(Self::options(None))
        .with_extension(extension)
    }

    async fn ctx(&self, extension: Arc<Probe>) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_catalog("lake", Arc::new(self.catalog(extension).await));
        ctx
    }

    /// Live data files `(path, size)`.
    async fn files(&self) -> Vec<(String, i64)> {
        let provider = self.provider().await;
        let snapshot = provider.get_current_snapshot().unwrap();
        let mut files: Vec<(String, i64)> = provider
            .get_table_files_for_select(self.table_id, snapshot)
            .unwrap()
            .into_iter()
            .map(|f| (f.file.path, f.file.file_size_bytes))
            .collect();
        files.sort();
        files
    }
}

fn columns() -> Vec<ColumnDef> {
    schema()
        .fields()
        .iter()
        .map(|f| ColumnDef::from_arrow(f.name(), f.data_type(), f.is_nullable()).unwrap())
        .collect()
}

/// `probe`'s records of the live files, `(path, size) -> record`.
async fn live_records(env: &Env, probe: &Probe) -> HashMap<(String, i64), Record> {
    let live = env.files().await;
    probe
        .seen
        .records
        .lock()
        .unwrap()
        .iter()
        .filter(|(key, _)| live.contains(key))
        .map(|(key, record)| (key.clone(), record.clone()))
        .collect()
}

async fn sum_v(ctx: &SessionContext) -> i64 {
    let batches = ctx
        .sql("SELECT sum(v) FROM lake.main.t WHERE v >= 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn table(ctx: &SessionContext) -> DuckLakeTable {
    let table = ctx
        .catalog("lake")
        .unwrap()
        .schema("main")
        .unwrap()
        .table("t")
        .await
        .unwrap()
        .unwrap();
    (table.as_ref() as &dyn std::any::Any)
        .downcast_ref::<DuckLakeTable>()
        .unwrap()
        .clone()
}

/// Every file a write names reaches its observer once stored and before its
/// commit, on every write path: a buffered append, a streaming session and a
/// merge. The observer saw the written schema: catalog columns with field ids.
#[tokio::test(flavor = "multi_thread")]
async fn observers_see_each_file_stored_before_its_commit() {
    let env = Env::new().await;
    let probe = Arc::new(Probe {
        catalog_url: Some(env.url()),
        ..Probe::new(Fault::None)
    });
    env.append(0..3, Some(Arc::clone(&probe))).await;
    env.stream(3..7, Arc::clone(&probe)).await;
    let records = live_records(&env, &probe).await;
    assert_eq!(records.len(), 2, "{records:?}");
    let mut rows: Vec<usize> = records.values().map(|r| r.rows).collect();
    rows.sort();
    assert_eq!(rows, vec![3, 4]);
    assert!(
        records.values().all(|r| r.origin == WriteOrigin::Insert),
        "{records:?}"
    );

    let ctx = env.ctx(Arc::clone(&probe)).await;
    let merged = table(&ctx)
        .await
        .merge_adjacent_files(&ctx.state(), MergeOptions::default())
        .await
        .unwrap();
    assert_eq!(merged.files_processed, 2, "{merged:?}");
    let records = live_records(&env, &probe).await;
    assert_eq!(records.len(), 1, "the merge output: {records:?}");
    let output = records.values().next().unwrap();
    assert_eq!((output.origin, output.rows), (WriteOrigin::Compaction, 7));

    let all = probe.seen.records.lock().unwrap().clone();
    assert!(
        all.values().all(|r| !r.listed_when_stored),
        "told before the commit: {all:?}"
    );
    let opened = probe.seen.opened.lock().unwrap();
    assert!(opened.iter().all(|(_, fields)| {
        fields
            .iter()
            .any(|(name, id)| name == "v" && !id.is_empty())
    }));
}

/// A hook that panics or errs, at any point of a write or a scan, leaves the
/// write committed and the scan's answer unchanged; a failed observer keeps
/// nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_hook_never_fails_a_write_or_a_scan() {
    for fault in [
        Fault::PanicWriteHook,
        Fault::PanicOpenFile,
        Fault::PanicObserve,
        Fault::PanicStored,
        Fault::ErrObserve,
        Fault::ErrStored,
        Fault::PanicScanHook,
        Fault::PanicSelect,
    ] {
        let env = Env::new().await;
        let probe = Arc::new(Probe::new(fault));
        env.append(0..10, Some(Arc::clone(&probe))).await;
        let write_fault = !matches!(fault, Fault::PanicScanHook | Fault::PanicSelect);
        assert_eq!(
            live_records(&env, &probe).await.is_empty(),
            write_fault,
            "{fault:?}: a failed observer keeps nothing"
        );
        // Through a catalog, so the scan runs the hooks too.
        let ctx = env.ctx(Arc::clone(&probe)).await;
        ctx.sql("INSERT INTO lake.main.t VALUES (10, 'l10')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(sum_v(&ctx).await, (0..=10).sum::<i64>(), "{fault:?}");
    }
}

/// A scan hook's skip is applied: the probe skips the file it recorded with
/// 3 rows. The location a scan hook resolves is where the file was stored.
#[tokio::test(flavor = "multi_thread")]
async fn a_skip_leaves_the_file_out() {
    let env = Env::new().await;
    let probe = Arc::new(Probe::new(Fault::None));
    env.append(0..3, Some(Arc::clone(&probe))).await;
    env.append(100..104, Some(Arc::clone(&probe))).await;
    let skipping = Arc::new(Probe {
        skip_rows: Some(3),
        seen: Arc::clone(&probe.seen),
        ..Probe::new(Fault::None)
    });
    assert_eq!(
        sum_v(&env.ctx(skipping).await).await,
        (100..104).sum::<i64>()
    );
    assert_eq!(
        sum_v(&env.ctx(Arc::clone(&probe)).await).await,
        (0..3).sum::<i64>() + (100..104).sum::<i64>()
    );
    let records = probe.seen.records.lock().unwrap();
    let resolved = probe.seen.resolved.lock().unwrap();
    assert_eq!(resolved.len(), 2);
    for ((path, _), record) in records.iter() {
        assert_eq!(resolved.get(path), Some(&record.object_path), "{path}");
    }
}

/// `backfill` replays the write hook over files written without it: only
/// the columns the hook asks for, typed and tagged as the catalog has them;
/// files the hook passes over are skipped; an observer that errs fails its
/// file.
#[tokio::test(flavor = "multi_thread")]
async fn backfill_observes_files_written_without_the_extension() {
    let env = Env::new().await;
    env.append(0..3, None).await;
    env.append(3..10, None).await;

    let probe = Arc::new(Probe::new(Fault::None));
    let ctx = env.ctx(Arc::clone(&probe)).await;
    let table = table(&ctx).await;
    let result = table
        .backfill(&ctx.state(), probe.clone(), BackfillOptions::default())
        .await
        .unwrap();
    assert_eq!(
        (result.files_read, result.files_skipped, result.files_failed),
        (2, 0, 0),
        "{result:?}"
    );
    assert!(result.bytes_read > 0);
    let records = live_records(&env, &probe).await;
    let mut rows: Vec<usize> = records.values().map(|r| r.rows).collect();
    rows.sort();
    assert_eq!(rows, vec![3, 7]);
    assert!(records.values().all(|r| r.origin == WriteOrigin::Backfill));
    {
        let opened = probe.seen.opened.lock().unwrap();
        assert_eq!(opened.len(), 2);
        for (origin, fields) in opened.iter() {
            assert_eq!(*origin, WriteOrigin::Backfill);
            let names: Vec<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
            assert_eq!(names, vec!["v"], "only the column the hook asked for");
            assert!(!fields[0].1.is_empty(), "tagged with its field id");
        }
    }

    // A hook that has seen both files passes over them.
    let done = Arc::new(Probe {
        done: env.files().await.into_iter().map(|(p, _)| p).collect(),
        ..Probe::new(Fault::None)
    });
    let again = table
        .backfill(&ctx.state(), done, BackfillOptions::default())
        .await
        .unwrap();
    assert_eq!((again.files_read, again.files_skipped), (0, 2), "{again:?}");

    // An observer that errs counts its file as failed and keeps nothing.
    let erring = Arc::new(Probe::new(Fault::ErrStored));
    let failed = table
        .backfill(
            &ctx.state(),
            Arc::clone(&erring) as Arc<dyn DuckLakeExtension>,
            BackfillOptions::default().with_max_files(1),
        )
        .await
        .unwrap();
    assert_eq!(
        (failed.files_read, failed.files_failed),
        (0, 1),
        "{failed:?}"
    );
    assert!(erring.seen.records.lock().unwrap().is_empty());
}

/// A file whose column cannot be cast to the catalog's type is skipped by
/// the backfill, not read and failed on every pass.
#[tokio::test(flavor = "multi_thread")]
async fn backfill_skips_a_column_that_does_not_cast() {
    let env = Env::new().await;
    // `v` stored as binary, with no field ids: an upload registered as is.
    let stored = Arc::new(Schema::new(vec![
        Field::new("v", DataType::Binary, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let rows = RecordBatch::try_new(
        Arc::clone(&stored),
        vec![
            Arc::new(BinaryArray::from_vec(vec![b"a".as_slice(), b"b"])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .unwrap();
    let dir = env.data().join("main").join("t");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("binary.parquet");
    let mut out =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), stored, None)
            .unwrap();
    out.write(&rows).unwrap();
    out.close().unwrap();
    let size = std::fs::metadata(&path).unwrap().len() as i64;
    let writer = env.writer().await;
    let setup = writer
        .begin_write_transaction("main", "t", &columns(), WriteMode::Append)
        .unwrap();
    writer
        .register_data_file(
            setup.table_id,
            "main",
            "t",
            setup.snapshot_id,
            &DataFileInfo::new("binary.parquet", size, 2),
            WriteMode::Append,
            setup.base_snapshot_id,
            &columns(),
            &setup.column_ids,
        )
        .unwrap();
    assert_eq!(env.files().await.len(), 1);

    let probe = Arc::new(Probe::new(Fault::None));
    let ctx = env.ctx(Arc::clone(&probe)).await;
    let result = table(&ctx)
        .await
        .backfill(&ctx.state(), probe.clone(), BackfillOptions::default())
        .await
        .unwrap();
    assert_eq!(
        (result.files_read, result.files_skipped, result.files_failed),
        (0, 1, 0),
        "{result:?}"
    );
    assert!(probe.seen.opened.lock().unwrap().is_empty());
}
