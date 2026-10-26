//! The extension API (`datafusion_ducklake::extension`) through a probe
//! extension: what its hooks see, that a hook which errs or panics never
//! fails a write or a scan, how its facts are recorded, the generic backfill,
//! the state kept across queries, and pruning.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::prelude::*;
use datafusion_ducklake::extension::{
    DuckLakeExtension, FileFact, FileObserver, FileSelection, FileWriteHook, ScanContext, ScanFile,
    ScanHook, StoredFile, WriteContext, WriteOrigin,
};
use datafusion_ducklake::metadata_provider::{DuckLakeTableFile, MetadataProvider};
use datafusion_ducklake::{
    BackfillOptions, ColumnDef, DuckLakeCatalog, DuckLakeTable, DuckLakeTableWriter,
    DuckLakeWriteOptions, MetadataWriter, SqliteMetadataProvider, SqliteMetadataWriter, WriteMode,
};
use object_store::local::LocalFileSystem;
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

/// What a [`Probe`] saw, shared with the test.
#[derive(Debug, Default)]
struct Seen {
    /// `(origin, schema field names with their field ids)` per opened file.
    opened: Mutex<Vec<(WriteOrigin, OpenedFields)>>,
    /// `file_facts` reads made by scan hooks.
    fact_reads: AtomicUsize,
}

/// Records each file's row count, and skips scans of files it recorded with
/// exactly `skip_rows` rows (a test-only breach of the "only skip what cannot
/// match" contract, to make a skip observable in the result).
#[derive(Debug)]
struct Probe {
    fault: Fault,
    skip_rows: Option<i64>,
    seen: Arc<Seen>,
    /// Files a backfill passes over.
    done: Vec<String>,
}

impl Probe {
    fn new(fault: Fault) -> Self {
        Self {
            fault,
            skip_rows: None,
            seen: Arc::default(),
            done: Vec::new(),
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
            seen: Arc::clone(&self.seen),
            done: self.done.clone(),
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
    seen: Arc<Seen>,
    done: Vec<String>,
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
            rows: 0,
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
    rows: usize,
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

    async fn stored(
        self: Box<Self>,
        file: &StoredFile,
    ) -> datafusion_ducklake::Result<Vec<FileFact>> {
        match self.fault {
            Fault::PanicStored => panic!("probe: stored"),
            Fault::ErrStored => {
                return Err(datafusion_ducklake::DuckLakeError::Internal(
                    "probe: stored".into(),
                ));
            },
            _ => {},
        }
        Ok(vec![FileFact {
            path: file.catalog_path.clone(),
            file_size_bytes: file.file_size_bytes,
            key: 0,
            value: self.rows.to_string(),
        }])
    }
}

/// The facts a catalog's scans have read, kept across queries.
#[derive(Debug, Default)]
struct KnownCounts(Mutex<HashMap<(String, i64), i64>>);

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
        ctx: &ScanContext<'_>,
        files: &[ScanFile<'_>],
    ) -> HashMap<i64, FileSelection> {
        if self.fault == Fault::PanicSelect {
            panic!("probe: select");
        }
        let known = ctx.shared_state.get_or_default::<KnownCounts>();
        let key = |f: &ScanFile<'_>| (f.file.file.path.clone(), f.file.file.file_size_bytes);
        let missing: Vec<&str> = {
            let known = known.0.lock().unwrap();
            files
                .iter()
                .filter(|f| !known.contains_key(&key(f)))
                .map(|f| f.file.file.path.as_str())
                .collect()
        };
        if !missing.is_empty() {
            self.seen.fact_reads.fetch_add(1, Ordering::SeqCst);
            let facts = ctx
                .metadata
                .file_facts(NAME, ctx.table_id, &missing, None)
                .unwrap();
            let mut known = known.0.lock().unwrap();
            for fact in facts {
                known.insert(
                    (fact.path, fact.file_size_bytes),
                    fact.value.parse().unwrap(),
                );
            }
        }
        let known = known.0.lock().unwrap();
        files
            .iter()
            .filter(|f| self.skip_rows.is_some() && known.get(&key(f)).copied() == self.skip_rows)
            .map(|f| (f.file.data_file_id, FileSelection::Skip))
            .collect()
    }
}

struct Env {
    temp: TempDir,
    table_id: i64,
}

impl Env {
    async fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let data = temp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        let env = Self {
            temp,
            table_id: 0,
        };
        let writer = env.writer().await;
        writer.set_data_path(data.to_str().unwrap()).unwrap();
        let cols: Vec<ColumnDef> = schema()
            .fields()
            .iter()
            .map(|f| ColumnDef::from_arrow(f.name(), f.data_type(), f.is_nullable()).unwrap())
            .collect();
        let s = writer
            .begin_write_transaction("main", "t", &cols, WriteMode::Replace)
            .unwrap();
        writer
            .publish_snapshot(
                s.table_id,
                "main",
                "t",
                s.snapshot_id,
                WriteMode::Replace,
                s.base_snapshot_id,
                &cols,
                &s.column_ids,
            )
            .unwrap();
        Self {
            table_id: s.table_id,
            ..env
        }
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

    /// One data file of `rows`, written with `extension` (or none).
    async fn append(&self, rows: std::ops::Range<i64>, extension: Option<Arc<Probe>>) {
        let mut options = DuckLakeWriteOptions::default().with_data_inlining_row_limit(0);
        if let Some(extension) = extension {
            options = options.with_extension(extension);
        }
        DuckLakeTableWriter::new(
            Arc::new(self.writer().await),
            Arc::new(LocalFileSystem::new()),
        )
        .unwrap()
        .with_options(&options)
        .append_table("main", "t", &[batch(rows)])
        .await
        .unwrap();
    }

    async fn catalog(&self, extension: Arc<Probe>) -> DuckLakeCatalog {
        DuckLakeCatalog::with_writer(
            Arc::new(self.provider().await),
            Arc::new(self.writer().await),
        )
        .unwrap()
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

    /// The probe's facts, `(path, size, value)`, sorted.
    async fn facts(&self) -> Vec<(String, i64, String)> {
        let files = self.files().await;
        let paths: Vec<&str> = files.iter().map(|(p, _)| p.as_str()).collect();
        let mut facts: Vec<(String, i64, String)> = self
            .provider()
            .await
            .file_facts(NAME, self.table_id, &paths, None)
            .unwrap()
            .into_iter()
            .map(|f| (f.path, f.file_size_bytes, f.value))
            .collect();
        facts.sort();
        facts
    }
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

/// Every file a write names gets its observers' facts recorded, keyed by its
/// path and size, with what the observer saw.
#[tokio::test(flavor = "multi_thread")]
async fn each_written_file_gets_its_facts() {
    let env = Env::new().await;
    let probe = Arc::new(Probe::new(Fault::None));
    env.append(0..3, Some(Arc::clone(&probe))).await;
    env.append(3..7, Some(Arc::clone(&probe))).await;
    let files = env.files().await;
    assert_eq!(files.len(), 2);
    let facts = env.facts().await;
    let mut want: Vec<(String, i64, String)> = files
        .iter()
        .map(|(path, size)| (path.clone(), *size, String::new()))
        .collect();
    for (fact, want) in facts.iter().zip(&mut want) {
        want.2 = fact.2.clone();
    }
    assert_eq!(facts, want, "one fact per file, keyed by path and size");
    let mut counts: Vec<&str> = facts.iter().map(|f| f.2.as_str()).collect();
    counts.sort();
    assert_eq!(counts, vec!["3", "4"]);
    // The observer saw the written schema: catalog columns with field ids.
    let opened = probe.seen.opened.lock().unwrap();
    assert!(opened.iter().all(|(origin, fields)| {
        *origin == WriteOrigin::Insert
            && fields
                .iter()
                .any(|(name, id)| name == "v" && !id.is_empty())
    }));
}

/// A hook that panics or errs, at any point of a write or a scan, leaves the
/// write committed and the scan's answer unchanged, without its facts.
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
            env.facts().await.is_empty(),
            write_fault,
            "{fault:?}: a failed observer records nothing"
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
/// 3 rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_skip_leaves_the_file_out() {
    let env = Env::new().await;
    let probe = Arc::new(Probe::new(Fault::None));
    env.append(0..3, Some(Arc::clone(&probe))).await;
    env.append(100..104, Some(Arc::clone(&probe))).await;
    let skipping = Arc::new(Probe {
        skip_rows: Some(3),
        ..Probe::new(Fault::None)
    });
    assert_eq!(
        sum_v(&env.ctx(skipping).await).await,
        (100..104).sum::<i64>()
    );
    let plain = Arc::new(Probe::new(Fault::None));
    assert_eq!(
        sum_v(&env.ctx(plain).await).await,
        (0..3).sum::<i64>() + (100..104).sum::<i64>()
    );
}

/// A catalog keeps `shared_state` across queries: the second query reads no
/// facts. A table built by another catalog starts over.
#[tokio::test(flavor = "multi_thread")]
async fn shared_state_outlives_a_query() {
    let env = Env::new().await;
    env.append(0..3, Some(Arc::new(Probe::new(Fault::None))))
        .await;
    env.append(3..5, Some(Arc::new(Probe::new(Fault::None))))
        .await;
    let probe = Arc::new(Probe::new(Fault::None));
    let ctx = env.ctx(Arc::clone(&probe)).await;
    for _ in 0..3 {
        assert_eq!(sum_v(&ctx).await, (0..5).sum::<i64>());
    }
    assert_eq!(probe.seen.fact_reads.load(Ordering::SeqCst), 1);
    let other = env.ctx(Arc::clone(&probe)).await;
    sum_v(&other).await;
    assert_eq!(probe.seen.fact_reads.load(Ordering::SeqCst), 2);
}

/// `backfill` replays the write hook over files written without it: only
/// the columns the hook asks for, typed and tagged as the catalog has them;
/// files the hook passes over are skipped; facts are recorded per file.
#[tokio::test(flavor = "multi_thread")]
async fn backfill_observes_files_written_without_the_extension() {
    let env = Env::new().await;
    env.append(0..3, None).await;
    env.append(3..10, None).await;
    assert!(env.facts().await.is_empty());

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
    assert_eq!(result.facts.len(), 2);
    let mut counts: Vec<String> = env.facts().await.into_iter().map(|f| f.2).collect();
    counts.sort();
    assert_eq!(counts, vec!["3", "7"]);
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

    // An observer that errs counts its file as failed and records nothing.
    let erring = Arc::new(Probe::new(Fault::ErrStored));
    let failed = table
        .backfill(
            &ctx.state(),
            erring,
            BackfillOptions::default().with_max_files(1),
        )
        .await
        .unwrap();
    assert_eq!(
        (failed.files_read, failed.files_failed),
        (0, 1),
        "{failed:?}"
    );
    assert!(failed.facts.is_empty());
}

/// `prune_extension_facts` drops, for every registered extension, the facts
/// of files no catalog row lists, and keeps the live files' facts.
#[tokio::test(flavor = "multi_thread")]
async fn prune_drops_facts_of_unlisted_files() {
    let env = Env::new().await;
    let probe = Arc::new(Probe::new(Fault::None));
    env.append(0..3, Some(Arc::clone(&probe))).await;
    let ghost = FileFact {
        path: "never-committed.parquet".to_string(),
        file_size_bytes: 1,
        key: 0,
        value: "0".to_string(),
    };
    env.writer()
        .await
        .record_file_facts(NAME, env.table_id, std::slice::from_ref(&ghost))
        .unwrap();
    let catalog = env.catalog(probe).await;
    assert!(
        catalog
            .prune_extension_facts(Duration::from_secs(3_600))
            .unwrap()
            .is_empty(),
        "inside the grace period"
    );
    assert_eq!(
        catalog.prune_extension_facts(Duration::ZERO).unwrap(),
        vec![(NAME.to_string(), ghost)]
    );
    assert_eq!(env.facts().await.len(), 1, "the live file keeps its fact");
}
