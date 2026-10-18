//! The catalog side of the experimental `ext_file_index` (see
//! `src/file_indexing.rs`): `build_file_indexes` builds a per-file posting
//! index for the declared columns of files from every writer path, stores it
//! next to the data file and records it in the `ext_ducklake_file_index`
//! sidecar; writes and compaction build none; prune and the orphan sweep
//! remove an index only with its data file.
//!
//! Every check rebuilds the postings by brute force from the committed data
//! file and compares them with the stored index object, so an index that
//! misses or misplaces a row fails here.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::array::{
    Array, AsArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, TimeUnit, UInt32Type};
use datafusion::prelude::*;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;

use datafusion_ducklake::ext_file_index::IndexKey;
use datafusion_ducklake::maintenance::{CleanupCriteria, ExpireCriteria};
use datafusion_ducklake::{
    BuildIndexOptions, BuildIndexResult, ColumnDef, DuckLakeCatalog, DuckLakeTable,
    DuckLakeTableWriter, DuckLakeWriteOptions, MergeOptions, MetadataWriter, RewriteOptions,
    SqliteMetadataProvider, SqliteMetadataWriter, WriteMode,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
    ]))
}

/// Rows `range`: `user_id` skewed (u0 dense, many rare), every 7th NULL;
/// `n` = i % 13, every 11th NULL.
fn batch(range: std::ops::Range<i64>) -> RecordBatch {
    let users: Vec<Option<String>> = range
        .clone()
        .map(|i| {
            if i % 7 == 0 {
                None
            } else if i % 3 == 0 {
                Some("u0".to_string())
            } else {
                Some(format!("u{}", (i * 31) % 97))
            }
        })
        .collect();
    let n: Vec<Option<i64>> = range
        .clone()
        .map(|i| (i % 11 != 0).then_some(i % 13))
        .collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from_iter_values(range.clone())),
            Arc::new(StringArray::from(users)),
            Arc::new(Int64Array::from(n)),
        ],
    )
    .unwrap()
}

/// A store over the local filesystem that can refuse index uploads.
#[derive(Debug)]
struct Store {
    inner: LocalFileSystem,
    fail_index_puts: AtomicBool,
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Store({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self.fail_index_puts.load(Ordering::SeqCst) && location.as_ref().ends_with(".pidx") {
            return Err(object_store::Error::Generic {
                store: "Store",
                source: "injected index upload failure".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct Env {
    temp: TempDir,
    table_id: i64,
    store: Arc<Store>,
}

/// One sidecar row, as stored.
#[derive(Debug, Clone)]
struct Row {
    path: String,
    file_size_bytes: i64,
    column_id: i64,
    index_path: String,
    index_key: String,
    index_size_bytes: i64,
    value_type: String,
    row_count: i64,
    row_group_rows: String,
    built_by: String,
}

impl Env {
    fn url(&self) -> String {
        format!(
            "sqlite:{}?mode=rwc",
            self.temp.path().join("test.db").display()
        )
    }

    fn data_path(&self) -> PathBuf {
        self.temp.path().join("data")
    }

    async fn writer(&self) -> SqliteMetadataWriter {
        SqliteMetadataWriter::new_with_init(&self.url())
            .await
            .unwrap()
    }

    fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store) as Arc<dyn ObjectStore>
    }

    async fn declare(&self, columns: &str) {
        self.writer()
            .await
            .set_table_setting(self.table_id, "ext_file_index_columns", columns)
            .unwrap();
    }

    async fn table_writer(&self, options: &DuckLakeWriteOptions) -> DuckLakeTableWriter {
        DuckLakeTableWriter::new(Arc::new(self.writer().await), self.store())
            .unwrap()
            .with_options(options)
    }

    /// Append through the streaming session RuntimeDB ingests with.
    async fn append(&self, batches: &[RecordBatch], options: &DuckLakeWriteOptions) {
        let writer = self.table_writer(options).await;
        let mut session = writer
            .begin_write("main", "logs", schema().as_ref(), WriteMode::Append)
            .unwrap();
        for b in batches {
            session.write_batch(b).unwrap();
        }
        session.finish().await.unwrap();
    }

    async fn catalog(&self) -> DuckLakeCatalog {
        self.catalog_with(options()).await
    }

    async fn catalog_with(&self, options: DuckLakeWriteOptions) -> DuckLakeCatalog {
        DuckLakeCatalog::with_writer(
            Arc::new(SqliteMetadataProvider::new(&self.url()).await.unwrap()),
            Arc::new(self.writer().await),
        )
        .unwrap()
        .with_write_options(options)
    }

    async fn table(&self) -> (SessionContext, DuckLakeTable) {
        self.table_with(options()).await
    }

    async fn table_with(&self, options: DuckLakeWriteOptions) -> (SessionContext, DuckLakeTable) {
        let ctx = SessionContext::new();
        ctx.runtime_env()
            .register_object_store(&url::Url::parse("file://").unwrap(), self.store());
        ctx.register_catalog("ducklake", Arc::new(self.catalog_with(options).await));
        let table = ctx
            .catalog("ducklake")
            .unwrap()
            .schema("main")
            .unwrap()
            .table("logs")
            .await
            .unwrap()
            .unwrap();
        let table = (table.as_ref() as &dyn std::any::Any)
            .downcast_ref::<DuckLakeTable>()
            .unwrap()
            .clone();
        (ctx, table)
    }

    async fn pool(&self) -> sqlx::SqlitePool {
        sqlx::SqlitePool::connect(&self.url()).await.unwrap()
    }

    async fn has_sidecar(&self) -> bool {
        use sqlx::Row as _;
        sqlx::query("SELECT count(*) > 0 FROM sqlite_master WHERE name = 'ext_ducklake_file_index'")
            .fetch_one(&self.pool().await)
            .await
            .unwrap()
            .get(0)
    }

    async fn rows(&self) -> Vec<Row> {
        use sqlx::Row as _;
        if !self.has_sidecar().await {
            return Vec::new();
        }
        sqlx::query(
            "SELECT path, file_size_bytes, column_id, index_path, index_key, index_size_bytes,
                    value_type, row_count, row_group_rows, built_by
             FROM ext_ducklake_file_index ORDER BY path, column_id",
        )
        .fetch_all(&self.pool().await)
        .await
        .unwrap()
        .iter()
        .map(|r| Row {
            path: r.get(0),
            file_size_bytes: r.get(1),
            column_id: r.get(2),
            index_path: r.get(3),
            index_key: r.get(4),
            index_size_bytes: r.get(5),
            value_type: r.get(6),
            row_count: r.get(7),
            row_group_rows: r.get(8),
            built_by: r.get(9),
        })
        .collect()
    }

    /// Live data files: `(path, size)`.
    async fn live_files(&self) -> Vec<(String, i64)> {
        use sqlx::Row as _;
        sqlx::query(
            "SELECT path, file_size_bytes FROM ducklake_data_file
             WHERE end_snapshot IS NULL ORDER BY path",
        )
        .fetch_all(&self.pool().await)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
    }

    /// `column name -> column_id`.
    async fn column_ids(&self) -> HashMap<String, i64> {
        use sqlx::Row as _;
        sqlx::query(
            "SELECT column_name, column_id FROM ducklake_column
             WHERE table_id = ? AND end_snapshot IS NULL",
        )
        .bind(self.table_id)
        .fetch_all(&self.pool().await)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
    }

    /// The local file of a table-relative data path.
    fn data_file(&self, path: &str) -> PathBuf {
        self.data_path().join("main").join("logs").join(path)
    }

    fn pidx_files(&self) -> Vec<PathBuf> {
        fn walk(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().is_some_and(|e| e == "pidx") {
                    out.push(path);
                }
            }
        }
        let mut out = Vec::new();
        walk(&self.data_path(), &mut out);
        out.sort();
        out
    }

    /// Every live file has a row and an object for each of `columns`, the
    /// object's postings equal a brute-force rebuild of the file, and the
    /// row's metadata matches the file. Returns the rows checked.
    async fn assert_indexed(&self, columns: &[&str], built_by: &str) -> Vec<Row> {
        let ids = self.column_ids().await;
        let rows = self.rows().await;
        let mut checked = Vec::new();
        let live = self.live_files().await;
        assert!(!live.is_empty());
        for (path, size) in live {
            let local = self.data_file(&path);
            let (postings, groups) = brute_force(&local);
            for column in columns {
                let column_id = ids[*column];
                let row = rows
                    .iter()
                    .find(|r| r.path == path && r.column_id == column_id)
                    .unwrap_or_else(|| panic!("no index row for {path} {column}: {rows:?}"));
                assert_eq!(row.file_size_bytes, size, "{path}");
                assert_eq!(row.built_by, built_by, "{path}");
                let name = path.rsplit('/').next().unwrap();
                assert_eq!(row.index_path, format!("{name}.c{column_id}.v1.pidx"));
                assert!(
                    row.index_key
                        .ends_with(&format!("{path}.c{column_id}.v1.pidx"))
                );
                let object = PathBuf::from(format!("/{}", row.index_key));
                let bytes = std::fs::read(&object).unwrap();
                assert_eq!(row.index_size_bytes, bytes.len() as i64);
                let total: u64 = groups.iter().sum();
                assert_eq!(row.row_count, total as i64);
                assert_eq!(
                    row.row_group_rows,
                    format!(
                        "[{}]",
                        groups
                            .iter()
                            .map(u64::to_string)
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                );
                let expected_type = if *column == "n" {
                    "int64"
                } else {
                    "utf8"
                };
                assert_eq!(row.value_type, expected_type);
                assert_eq!(
                    decode_index(bytes),
                    postings[*column],
                    "postings of {path} {column}"
                );
                checked.push(row.clone());
            }
        }
        checked
    }
}

/// `column -> key -> sorted positions`, and the file's row-group row counts.
fn brute_force(
    file: &std::path::Path,
) -> (HashMap<String, BTreeMap<IndexKey, Vec<u32>>>, Vec<u64>) {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file).unwrap()).unwrap();
    let groups = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as u64)
        .collect();
    let mut out: HashMap<String, BTreeMap<IndexKey, Vec<u32>>> = HashMap::new();
    let mut base = 0u32;
    for batch in builder.build().unwrap() {
        let batch = batch.unwrap();
        for name in ["user_id", "n"] {
            let Some(column) = batch.column_by_name(name) else {
                continue;
            };
            let map = out.entry(name.to_string()).or_default();
            for i in 0..column.len() {
                if column.is_null(i) {
                    continue;
                }
                let key = match column.data_type() {
                    DataType::Int64 => IndexKey::Int64(column.as_primitive::<Int64Type>().value(i)),
                    DataType::Int32 => IndexKey::Int64(
                        column
                            .as_primitive::<arrow::datatypes::Int32Type>()
                            .value(i) as i64,
                    ),
                    _ => {
                        let utf8 = arrow::compute::cast(column, &DataType::Utf8).unwrap();
                        IndexKey::Utf8(utf8.as_string::<i32>().value(i).as_bytes().into())
                    },
                };
                map.entry(key).or_default().push(base + i as u32);
            }
        }
        base += batch.num_rows() as u32;
    }
    (out, groups)
}

/// The `(value, row)` pairs of an index object, grouped by value.
fn decode_index(bytes: Vec<u8>) -> BTreeMap<IndexKey, Vec<u32>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap();
    let mut out: BTreeMap<IndexKey, Vec<u32>> = BTreeMap::new();
    for batch in reader {
        let batch = batch.unwrap();
        let value = batch.column_by_name("value").unwrap();
        let rows = batch.column_by_name("row").unwrap();
        let rows = arrow::compute::cast(rows, &DataType::UInt32).unwrap();
        let rows = rows.as_primitive::<UInt32Type>();
        let value = match value.data_type() {
            DataType::Dictionary(_, inner) => arrow::compute::cast(value, inner).unwrap(),
            _ => Arc::clone(value),
        };
        for i in 0..batch.num_rows() {
            let key = match value.data_type() {
                DataType::Int64 => IndexKey::Int64(value.as_primitive::<Int64Type>().value(i)),
                _ => {
                    let bin = arrow::compute::cast(&value, &DataType::Binary).unwrap();
                    IndexKey::Utf8(bin.as_binary::<i32>().value(i).into())
                },
            };
            out.entry(key).or_default().push(rows.value(i));
        }
    }
    for rows in out.values_mut() {
        let sorted = rows.windows(2).all(|w| w[0] < w[1]);
        assert!(sorted, "positions sorted and unique within a value");
    }
    out
}

fn options() -> DuckLakeWriteOptions {
    let mut options = DuckLakeWriteOptions::default();
    // Small row groups, so files have several and positions cross them.
    options.max_row_group_rows = Some(300);
    options.data_inlining_row_limit = Some(0);
    options
}

/// [`options`] with the experimental write option on.
fn index_options() -> DuckLakeWriteOptions {
    options().with_ext_file_index(true)
}

async fn setup() -> Env {
    let temp = TempDir::new().unwrap();
    std::fs::create_dir_all(temp.path().join("data")).unwrap();
    let store = Arc::new(Store {
        inner: LocalFileSystem::new(),
        fail_index_puts: AtomicBool::new(false),
    });
    let env = Env {
        temp,
        table_id: 0,
        store,
    };
    let writer = env.writer().await;
    writer
        .set_data_path(env.data_path().to_str().unwrap())
        .unwrap();
    let cols: Vec<ColumnDef> = schema()
        .fields()
        .iter()
        .map(|f| ColumnDef::from_arrow(f.name(), f.data_type(), f.is_nullable()).unwrap())
        .collect();
    let s = writer
        .begin_write_transaction("main", "logs", &cols, WriteMode::Replace)
        .unwrap();
    writer
        .publish_snapshot(
            s.table_id,
            "main",
            "logs",
            s.snapshot_id,
            WriteMode::Replace,
            s.base_snapshot_id,
            &cols,
            &s.column_ids,
        )
        .unwrap();
    Env {
        table_id: s.table_id,
        ..env
    }
}

async fn indexed_env() -> Env {
    let env = setup().await;
    env.declare("user_id, n").await;
    env
}

// ---------------------------------------------------------------------------
// Files from every writer path
// ---------------------------------------------------------------------------

impl Env {
    async fn backfill(&self) -> BuildIndexResult {
        self.backfill_with(BuildIndexOptions::default()).await
    }

    async fn backfill_with(&self, opts: BuildIndexOptions) -> BuildIndexResult {
        let (ctx, table) = self.table().await;
        table.build_file_indexes(&ctx.state(), opts).await.unwrap()
    }
}

/// Session appends (several batches, several row groups) and a rolled
/// session: every file gets an exact index.
#[tokio::test(flavor = "multi_thread")]
async fn session_and_rolled_files_are_indexed() {
    let env = indexed_env().await;
    env.append(&[batch(0..700), batch(700..1_000)], &options())
        .await;
    let mut rolled = options();
    rolled.target_file_size = Some(4_000);
    env.append(
        &(0..10)
            .map(|i| batch(1_000 + i * 500..1_000 + (i + 1) * 500))
            .collect::<Vec<_>>(),
        &rolled,
    )
    .await;
    let files = env.live_files().await.len();
    assert!(files > 2, "the second write rolled");
    let result = env.backfill().await;
    assert_eq!(result.files_indexed as usize, files, "{result:?}");
    let rows = env.assert_indexed(&["user_id", "n"], "backfill").await;
    assert_eq!(rows.len(), files * 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_and_flushed_files_are_indexed() {
    let env = indexed_env().await;
    let mut opts = options();
    opts.target_file_size = Some(4_000);
    env.table_writer(&opts)
        .await
        .append_table(
            "main",
            "logs",
            &(0..6)
                .map(|i| batch(i * 500..(i + 1) * 500))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let mut inline = options();
    inline.data_inlining_row_limit = Some(1_000);
    let writer = env.table_writer(&inline).await;
    for i in 0..3 {
        writer
            .append_table(
                "main",
                "logs",
                &[batch(5_000 + i * 100..5_000 + (i + 1) * 100)],
            )
            .await
            .unwrap();
    }
    let flushed = env
        .catalog()
        .await
        .flush_inlined_data(&SessionContext::new().state(), Some("main"), Some("logs"))
        .await
        .unwrap();
    assert_eq!(flushed.len(), 1, "{flushed:?}");
    env.backfill().await;
    env.assert_indexed(&["user_id", "n"], "backfill").await;
}

/// Compaction outputs are new files: no index until the backfill runs
/// again, which is safe after compaction and indexes only the new file.
#[tokio::test(flavor = "multi_thread")]
async fn backfill_after_merge_and_rewrite() {
    let env = indexed_env().await;
    for i in 0..3 {
        env.append(&[batch(i * 400..(i + 1) * 400)], &options())
            .await;
    }
    assert_eq!(env.backfill().await.files_indexed, 3);

    let (ctx, table) = env.table().await;
    let merged = table
        .merge_adjacent_files(&ctx.state(), MergeOptions::default())
        .await
        .unwrap();
    assert_eq!(merged.files_processed, 3, "{merged:?}");
    let after = env.backfill().await;
    assert_eq!(after.files_indexed, 1, "only the merge output: {after:?}");
    env.assert_indexed(&["user_id", "n"], "backfill").await;

    let (ctx, table) = env.table().await;
    let ids: Vec<i64> = {
        use sqlx::Row as _;
        sqlx::query("SELECT data_file_id FROM ducklake_data_file WHERE end_snapshot IS NULL")
            .fetch_all(&env.pool().await)
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect()
    };
    let before = env.live_files().await;
    let rewritten = table
        .rewrite_data_files(
            &ctx.state(),
            RewriteOptions {
                data_file_ids: Some(ids),
                ..RewriteOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(rewritten.files_processed, 1, "{rewritten:?}");
    assert_ne!(env.live_files().await, before, "a new file was written");
    assert_eq!(env.backfill().await.files_indexed, 1);
    env.assert_indexed(&["user_id", "n"], "backfill").await;
}

/// With the write option off (the default), writes and compaction build
/// nothing, even with columns declared.
#[tokio::test(flavor = "multi_thread")]
async fn option_off_writes_and_compaction_build_nothing() {
    let env = indexed_env().await;
    env.append(&[batch(0..500)], &options()).await;
    env.append(&[batch(500..900)], &options()).await;
    let (ctx, table) = env.table().await;
    table
        .merge_adjacent_files(&ctx.state(), MergeOptions::default())
        .await
        .unwrap();
    assert!(!env.has_sidecar().await);
    assert!(env.pidx_files().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_declared_builds_nothing() {
    let env = setup().await;
    env.append(&[batch(0..500)], &options()).await;
    let result = env.backfill().await;
    assert_eq!(result, BuildIndexResult::default());
    assert!(!env.has_sidecar().await);
    assert!(env.pidx_files().is_empty());
}

/// An index object that fails to upload gets no row; the next pass retries.
#[tokio::test(flavor = "multi_thread")]
async fn failed_index_upload_records_nothing() {
    let env = indexed_env().await;
    env.append(&[batch(0..500)], &options()).await;
    env.store.fail_index_puts.store(true, Ordering::SeqCst);
    let failed = env.backfill().await;
    assert_eq!(failed.files_failed, 1, "{failed:?}");
    assert_eq!(failed.files_indexed, 0);
    assert!(env.rows().await.is_empty(), "no row without its object");
    env.store.fail_index_puts.store(false, Ordering::SeqCst);
    assert_eq!(env.backfill().await.files_indexed, 1);
    env.assert_indexed(&["user_id", "n"], "backfill").await;
}

// ---------------------------------------------------------------------------
// Prune and the orphan sweep
// ---------------------------------------------------------------------------

/// A row for a path no file lists (a backfill racing a compaction that
/// retired the file) is pruned once older than the grace period, with its
/// object.
#[tokio::test(flavor = "multi_thread")]
async fn unlisted_row_is_pruned_after_grace() {
    let env = indexed_env().await;
    env.append(&[batch(0..500)], &options()).await;
    env.backfill().await;
    let live = env.rows().await;
    let ghost = format!("{}.ghost", live[0].index_key);
    std::fs::write(format!("/{ghost}"), b"x").unwrap();
    env.writer()
        .await
        .record_file_indexes(
            env.table_id,
            "backfill",
            &[datafusion_ducklake::file_indexing::FileIndexRow {
                meta: datafusion_ducklake::ext_file_index::FileIndexMeta {
                    path: "never-committed.parquet".to_string(),
                    ..sample_meta()
                },
                index_key: ghost.clone(),
            }],
        )
        .unwrap();
    assert_eq!(env.rows().await.len(), 3);

    let writer = env.writer().await;
    // Inside the grace period: kept.
    let kept = datafusion_ducklake::maintenance::prune_file_indexes(
        &writer,
        env.store(),
        Duration::from_secs(3_600),
    )
    .await
    .unwrap();
    assert!(kept.is_empty());
    assert_eq!(env.rows().await.len(), 3);

    let pruned =
        datafusion_ducklake::maintenance::prune_file_indexes(&writer, env.store(), Duration::ZERO)
            .await
            .unwrap();
    assert_eq!(pruned, vec![ghost.clone()]);
    assert!(!std::path::Path::new(&format!("/{ghost}")).exists());
    assert_eq!(env.rows().await.len(), 2, "the live file's rows stay");
    env.assert_indexed(&["user_id", "n"], "backfill").await;
}

fn sample_meta() -> datafusion_ducklake::ext_file_index::FileIndexMeta {
    datafusion_ducklake::ext_file_index::FileIndexMeta {
        path: String::new(),
        file_size_bytes: 1,
        column_id: 1,
        index_path: "x.pidx".to_string(),
        index_size_bytes: 1,
        format_version: 1,
        value_type: datafusion_ducklake::ext_file_index::IndexValueType::Utf8,
        row_count: 1,
        row_group_rows: vec![1],
        heavy_hitters: vec![],
        bucket_counts: None,
    }
}

/// A file compacted away keeps its index while any snapshot lists it, and
/// loses row and object once expiry removes the file.
#[tokio::test(flavor = "multi_thread")]
async fn prune_after_expiry_removes_index_with_file() {
    let env = indexed_env().await;
    for i in 0..2 {
        env.append(&[batch(i * 400..(i + 1) * 400)], &options())
            .await;
    }
    env.backfill().await;
    let old = env.rows().await;
    assert_eq!(old.len(), 4);
    let (ctx, table) = env.table().await;
    table
        .merge_adjacent_files(&ctx.state(), MergeOptions::default())
        .await
        .unwrap();
    env.backfill().await;
    let writer = env.writer().await;

    // Retired files a snapshot still lists keep their rows.
    let listed: usize = {
        use sqlx::Row as _;
        sqlx::query("SELECT count(*) FROM ducklake_data_file")
            .fetch_one(&env.pool().await)
            .await
            .unwrap()
            .get::<i64, _>(0) as usize
    };
    let mut pruned =
        datafusion_ducklake::maintenance::prune_file_indexes(&writer, env.store(), Duration::ZERO)
            .await
            .unwrap();
    if listed > 1 {
        assert!(pruned.is_empty(), "{pruned:?}");
    }

    writer
        .expire_snapshots(ExpireCriteria::OlderThan(
            chrono::Utc::now() + chrono::Duration::seconds(1),
        ))
        .unwrap();
    datafusion_ducklake::maintenance::cleanup_old_files_sqlite(
        &writer,
        env.store(),
        CleanupCriteria::All,
        false,
    )
    .await
    .unwrap();
    pruned.extend(
        datafusion_ducklake::maintenance::prune_file_indexes(&writer, env.store(), Duration::ZERO)
            .await
            .unwrap(),
    );
    let mut want: Vec<String> = old.iter().map(|r| r.index_key.clone()).collect();
    want.sort();
    pruned.sort();
    assert_eq!(pruned, want);
    for row in &old {
        assert!(!std::path::Path::new(&format!("/{}", row.index_key)).exists());
    }
    // The merge output keeps its index.
    let rows = env.assert_indexed(&["user_id", "n"], "backfill").await;
    assert_eq!(env.rows().await.len(), rows.len());
}

/// The orphan sweep keeps every index whose data file is referenced and
/// deletes one whose data file is gone.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_sweep_keeps_live_indexes() {
    let env = indexed_env().await;
    env.append(&[batch(0..500)], &options()).await;
    env.backfill().await;
    assert_eq!(env.pidx_files().len(), 2);
    // An index of a data file that is not referenced (a retired file whose
    // row prune already dropped).
    let orphan = env.data_path().join("main/logs/aborted.parquet.c2.v1.pidx");
    std::fs::write(&orphan, b"x").unwrap();
    // Not an index name: the sweep's `.parquet`-only rule leaves it alone.
    let other = env.data_path().join("main/logs/notes.pidx");
    std::fs::write(&other, b"x").unwrap();

    let deleted = datafusion_ducklake::maintenance::delete_orphaned_files_sqlite(
        &env.writer().await,
        env.store(),
        CleanupCriteria::All,
        false,
    )
    .await
    .unwrap();
    assert_eq!(deleted.len(), 1, "{deleted:?}");
    assert!(deleted[0].ends_with("aborted.parquet.c2.v1.pidx"));
    assert!(!orphan.exists());
    assert!(other.exists());
    assert_eq!(env.pidx_files().len(), 3);
    env.assert_indexed(&["user_id", "n"], "backfill").await;
}

// ---------------------------------------------------------------------------
// Backfill passes
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn backfill_is_idempotent_and_resumes() {
    let env = indexed_env().await;
    for i in 0..4 {
        env.append(&[batch(i * 400..(i + 1) * 400)], &options())
            .await;
    }
    assert!(env.rows().await.is_empty());

    // A bounded pass, as an interrupted one leaves it.
    let first = env
        .backfill_with(BuildIndexOptions::default().with_max_files(1))
        .await;
    assert_eq!(first.files_indexed, 1, "{first:?}");
    assert!(first.bytes_read > 0 && first.bytes_written > 0);
    assert_eq!(env.rows().await.len(), 2);

    let rest = env
        .backfill_with(BuildIndexOptions::default().with_parallelism(3))
        .await;
    assert_eq!(rest.files_indexed, 3, "{rest:?}");
    assert_eq!(rest.files_skipped, 1, "the first pass's file is done");
    assert_eq!(rest.files_failed, 0);
    env.assert_indexed(&["user_id", "n"], "backfill").await;

    let again = env.backfill().await;
    assert_eq!(again.files_indexed, 0, "{again:?}");
    assert_eq!(again.files_skipped, 4);
    assert_eq!(again.bytes_written, 0);
}

/// A row whose size does not match the file (a stale row) is rebuilt; the
/// matching row of the other column is left alone.
#[tokio::test(flavor = "multi_thread")]
async fn backfill_rebuilds_a_mismatched_row() {
    let env = indexed_env().await;
    env.append(&[batch(0..500)], &options()).await;
    env.backfill().await;
    let n_id = env.column_ids().await["n"];
    sqlx::query(
        "UPDATE ext_ducklake_file_index SET file_size_bytes = 1, built_by = 'stale'
         WHERE column_id = ?",
    )
    .bind(n_id)
    .execute(&env.pool().await)
    .await
    .unwrap();
    sqlx::query("UPDATE ext_ducklake_file_index SET built_by = 'kept' WHERE column_id != ?")
        .bind(n_id)
        .execute(&env.pool().await)
        .await
        .unwrap();
    let result = env.backfill().await;
    assert_eq!(result.files_indexed, 1, "{result:?}");
    let rows = env.rows().await;
    assert_eq!(rows.len(), 2);
    let n = rows.iter().find(|r| r.column_id == n_id).unwrap();
    assert_eq!(n.built_by, "backfill");
    assert!(rows.iter().any(|r| r.built_by == "kept"));
    let (path, size) = env.live_files().await[0].clone();
    assert_eq!(n.path, path);
    assert_eq!(n.file_size_bytes, size);
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_of_explicit_columns() {
    let env = setup().await;
    env.append(&[batch(0..500)], &options()).await;
    let some = env
        .backfill_with(BuildIndexOptions::default().with_columns(vec!["n".to_string()]))
        .await;
    assert_eq!(some.files_indexed, 1);
    env.assert_indexed(&["n"], "backfill").await;
    let (ctx, table) = env.table().await;
    for bad in ["ts", "missing"] {
        assert!(
            table
                .build_file_indexes(
                    &ctx.state(),
                    BuildIndexOptions::default().with_columns(vec![bad.to_string()]),
                )
                .await
                .is_err(),
            "{bad}: not indexable"
        );
    }
}

// ---------------------------------------------------------------------------
// Schema evolution
// ---------------------------------------------------------------------------

/// A column added later: files written before it get no row for it (they
/// scan normally) and are not counted as failures.
#[tokio::test(flavor = "multi_thread")]
async fn column_added_later() {
    let env = setup().await;
    env.declare("user_id, tag").await;
    env.append(&[batch(0..400)], &options()).await;

    // Appending rows with a new column evolves the table.
    let wide = Arc::new(Schema::new(
        schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .chain([Field::new("tag", DataType::Utf8, true)])
            .collect::<Vec<_>>(),
    ));
    let mut columns = batch(400..800).columns().to_vec();
    columns.push(Arc::new(StringArray::from_iter_values(
        (400..800).map(|i| format!("t{}", i % 5)),
    )));
    let wide_batch = RecordBatch::try_new(Arc::clone(&wide), columns).unwrap();
    let table_writer = env.table_writer(&options()).await;
    let mut session = table_writer
        .begin_write("main", "logs", wide.as_ref(), WriteMode::Append)
        .unwrap();
    session.write_batch(&wide_batch).unwrap();
    session.finish().await.unwrap();

    let result = env.backfill().await;
    assert_eq!(result.files_indexed, 2, "{result:?}");
    assert_eq!(result.files_failed, 0);
    let ids = env.column_ids().await;
    let rows = env.rows().await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert_eq!(rows.iter().filter(|r| r.column_id == ids["tag"]).count(), 1);

    // The old file still lacks `tag`; another pass reads it again (it is
    // missing a row) but builds nothing.
    let again = env.backfill().await;
    assert_eq!(again.files_indexed, 0, "{again:?}");
    assert_eq!(again.files_failed, 0);
    assert_eq!(env.rows().await.len(), 3);
}

/// A rename keeps the column id, so existing rows stay valid; the setting
/// names columns, so it is re-declared under the new name.
#[tokio::test(flavor = "multi_thread")]
async fn renamed_column_keeps_its_rows() {
    let env = indexed_env().await;
    env.append(&[batch(0..400)], &options()).await;
    env.backfill().await;
    let before = env.rows().await;
    let id = env.column_ids().await["user_id"];
    // The catalog side of a rename: same column id, new name.
    sqlx::query("UPDATE ducklake_column SET column_name = 'uid' WHERE column_id = ?")
        .bind(id)
        .execute(&env.pool().await)
        .await
        .unwrap();
    assert_eq!(env.column_ids().await["uid"], id);
    env.declare("uid, n").await;
    let result = env.backfill().await;
    assert_eq!(
        result.files_indexed, 0,
        "already indexed under its id: {result:?}"
    );
    assert_eq!(env.rows().await.len(), before.len());
}

/// The read side's contract: the provider's rows, resolved from the data
/// file's directory and looked up through the format library, give exactly
/// the positions of each key, including keys the file lacks.
#[tokio::test(flavor = "multi_thread")]
async fn provider_rows_serve_exact_lookups() {
    use datafusion_ducklake::metadata_provider::MetadataProvider;
    let env = indexed_env().await;
    env.append(&[batch(0..700), batch(700..1_000)], &options())
        .await;
    env.append(&[batch(1_000..1_400)], &options()).await;
    env.backfill().await;

    let ids = env.column_ids().await;
    let live = env.live_files().await;
    let paths: Vec<&str> = live.iter().map(|(p, _)| p.as_str()).collect();
    let provider = SqliteMetadataProvider::new(&env.url()).await.unwrap();
    let metas = provider
        .file_indexes(env.table_id, &paths, &[ids["user_id"], ids["n"]])
        .unwrap();
    assert_eq!(metas.len(), live.len() * 2);
    for meta in &metas {
        let (_, size) = live.iter().find(|(p, _)| *p == meta.path).unwrap();
        assert_eq!(meta.file_size_bytes, *size);
        let data = env.data_file(&meta.path);
        let location =
            ObjectPath::from_filesystem_path(data.parent().unwrap().join(&meta.index_path))
                .unwrap();
        let (postings, _) = brute_force(&data);
        let column = if meta.column_id == ids["n"] {
            "n"
        } else {
            "user_id"
        };
        let mut keys: Vec<IndexKey> = postings[column].keys().cloned().collect();
        keys.push(match meta.value_type {
            datafusion_ducklake::ext_file_index::IndexValueType::Int64 => IndexKey::Int64(-99),
            _ => IndexKey::Utf8(b"absent".to_vec().into()),
        });
        for key in &keys {
            let got = datafusion_ducklake::ext_file_index::lookup_postings(
                env.store(),
                &location,
                meta,
                std::slice::from_ref(key),
                None,
            )
            .await
            .unwrap();
            let want = postings[column].get(key).cloned().unwrap_or_default();
            assert_eq!(got.rows, want, "{} {column} {key:?}", meta.path);
        }
    }
}

// ---------------------------------------------------------------------------
// Write option on: writers and compaction build indexes themselves
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn written_session_and_rolled_files_are_indexed() {
    let env = indexed_env().await;
    env.append(&[batch(0..700), batch(700..1_000)], &index_options())
        .await;
    let mut rolled = index_options();
    rolled.target_file_size = Some(4_000);
    env.append(
        &(0..10)
            .map(|i| batch(1_000 + i * 500..1_000 + (i + 1) * 500))
            .collect::<Vec<_>>(),
        &rolled,
    )
    .await;
    let files = env.live_files().await.len();
    assert!(files > 2, "the second write rolled");
    let rows = env.assert_indexed(&["user_id", "n"], "write").await;
    assert_eq!(rows.len(), files * 2);
    // The backfill finds nothing left to do.
    let result = env.backfill().await;
    assert_eq!(result.files_indexed, 0, "{result:?}");
    assert_eq!(result.files_skipped as usize, files);
}

#[tokio::test(flavor = "multi_thread")]
async fn written_buffered_and_flushed_files_are_indexed() {
    let env = indexed_env().await;
    let mut opts = index_options();
    opts.target_file_size = Some(4_000);
    env.table_writer(&opts)
        .await
        .append_table(
            "main",
            "logs",
            &(0..6)
                .map(|i| batch(i * 500..(i + 1) * 500))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    assert!(env.live_files().await.len() > 1, "the write rolled");
    let mut inline = index_options();
    inline.data_inlining_row_limit = Some(1_000);
    let writer = env.table_writer(&inline).await;
    for i in 0..3 {
        writer
            .append_table(
                "main",
                "logs",
                &[batch(5_000 + i * 100..5_000 + (i + 1) * 100)],
            )
            .await
            .unwrap();
    }
    let flushed = env
        .catalog_with(index_options())
        .await
        .flush_inlined_data(&SessionContext::new().state(), Some("main"), Some("logs"))
        .await
        .unwrap();
    assert_eq!(flushed.len(), 1, "{flushed:?}");
    env.assert_indexed(&["user_id", "n"], "write").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_and_rewrite_outputs_are_indexed() {
    let env = indexed_env().await;
    for i in 0..3 {
        env.append(&[batch(i * 400..(i + 1) * 400)], &options())
            .await;
    }
    assert!(env.rows().await.is_empty());

    let (ctx, table) = env.table_with(index_options()).await;
    let merged = table
        .merge_adjacent_files(&ctx.state(), MergeOptions::default())
        .await
        .unwrap();
    assert_eq!(merged.files_processed, 3, "{merged:?}");
    assert_eq!(env.live_files().await.len(), 1);
    env.assert_indexed(&["user_id", "n"], "compact").await;

    let (ctx, table) = env.table_with(index_options()).await;
    let ids: Vec<i64> = {
        use sqlx::Row as _;
        sqlx::query("SELECT data_file_id FROM ducklake_data_file WHERE end_snapshot IS NULL")
            .fetch_all(&env.pool().await)
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect()
    };
    let before = env.live_files().await;
    let rewritten = table
        .rewrite_data_files(
            &ctx.state(),
            RewriteOptions {
                data_file_ids: Some(ids),
                ..RewriteOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(rewritten.files_processed, 1, "{rewritten:?}");
    assert_ne!(env.live_files().await, before, "a new file was written");
    env.assert_indexed(&["user_id", "n"], "compact").await;
}

/// A failed index upload never fails the data write; the file is simply
/// unindexed until the backfill reaches it.
#[tokio::test(flavor = "multi_thread")]
async fn failed_index_upload_keeps_the_data_write() {
    let env = indexed_env().await;
    env.store.fail_index_puts.store(true, Ordering::SeqCst);
    env.append(&[batch(0..500)], &index_options()).await;
    assert_eq!(env.live_files().await.len(), 1, "the data write committed");
    assert!(env.rows().await.is_empty(), "no row without its object");
    env.store.fail_index_puts.store(false, Ordering::SeqCst);
    assert_eq!(env.backfill().await.files_indexed, 1);
    env.assert_indexed(&["user_id", "n"], "backfill").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn option_on_with_nothing_declared_writes_nothing() {
    let env = setup().await;
    env.append(&[batch(0..500)], &index_options()).await;
    assert!(!env.has_sidecar().await);
    assert!(env.pidx_files().is_empty());
}
