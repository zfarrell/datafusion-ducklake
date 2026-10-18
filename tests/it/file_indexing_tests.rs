//! The write side of the experimental `ext_file_index` option (see
//! `src/file_indexing.rs`): every writer path builds a per-file posting index
//! for the declared columns, stores it next to the data file and records it
//! in the `ext_ducklake_file_index` sidecar; a backfill indexes older files;
//! prune and the orphan sweep remove an index only with its data file.
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
    BuildIndexOptions, ColumnDef, DuckLakeCatalog, DuckLakeTable, DuckLakeTableWriter,
    DuckLakeWriteOptions, MergeOptions, MetadataWriter, RewriteOptions, SqliteMetadataProvider,
    SqliteMetadataWriter, WriteMode,
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

    async fn catalog(&self, index: bool) -> DuckLakeCatalog {
        DuckLakeCatalog::with_writer(
            Arc::new(SqliteMetadataProvider::new(&self.url()).await.unwrap()),
            Arc::new(self.writer().await),
        )
        .unwrap()
        .with_write_options(options(index))
    }

    async fn table(&self, index: bool) -> (SessionContext, DuckLakeTable) {
        let ctx = SessionContext::new();
        ctx.runtime_env()
            .register_object_store(&url::Url::parse("file://").unwrap(), self.store());
        ctx.register_catalog("ducklake", Arc::new(self.catalog(index).await));
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

fn options(index: bool) -> DuckLakeWriteOptions {
    let mut options = DuckLakeWriteOptions::default().with_ext_file_index(index);
    // Small row groups, so files have several and positions cross them.
    options.max_row_group_rows = Some(300);
    options.data_inlining_row_limit = Some(0);
    options
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
// Writer paths
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn session_append_is_indexed() {
    let env = indexed_env().await;
    env.append(&[batch(0..700), batch(700..1_000)], &options(true))
        .await;
    env.append(&[batch(1_000..1_100)], &options(true)).await;
    let rows = env.assert_indexed(&["user_id", "n"], "write").await;
    assert_eq!(rows.len(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn rolling_session_indexes_every_file() {
    let env = indexed_env().await;
    let mut opts = options(true);
    opts.target_file_size = Some(4_000);
    env.append(
        &(0..10)
            .map(|i| batch(i * 500..(i + 1) * 500))
            .collect::<Vec<_>>(),
        &opts,
    )
    .await;
    assert!(env.live_files().await.len() > 1, "the write rolled");
    env.assert_indexed(&["user_id", "n"], "write").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_append_indexes_every_file() {
    let env = indexed_env().await;
    let mut opts = options(true);
    opts.target_file_size = Some(4_000);
    let writer = env.table_writer(&opts).await;
    writer
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
    env.assert_indexed(&["user_id", "n"], "write").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn inlined_flush_output_is_indexed() {
    let env = indexed_env().await;
    let mut opts = options(true);
    opts.data_inlining_row_limit = Some(1_000);
    let writer = env.table_writer(&opts).await;
    for i in 0..3 {
        writer
            .append_table("main", "logs", &[batch(i * 100..(i + 1) * 100)])
            .await
            .unwrap();
    }
    assert!(env.live_files().await.is_empty(), "all rows inlined");
    let catalog = env.catalog(true).await;
    let ctx = SessionContext::new();
    let flushed = catalog
        .flush_inlined_data(&ctx.state(), Some("main"), Some("logs"))
        .await
        .unwrap();
    assert_eq!(flushed.len(), 1, "{flushed:?}");
    env.assert_indexed(&["user_id", "n"], "write").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_and_rewrite_outputs_are_indexed() {
    let env = indexed_env().await;
    for i in 0..3 {
        env.append(&[batch(i * 400..(i + 1) * 400)], &options(false))
            .await;
    }
    assert!(env.rows().await.is_empty());

    let (ctx, table) = env.table(true).await;
    let merged = table
        .merge_adjacent_files(&ctx.state(), MergeOptions::default())
        .await
        .unwrap();
    assert_eq!(merged.files_processed, 3, "{merged:?}");
    assert_eq!(env.live_files().await.len(), 1);
    env.assert_indexed(&["user_id", "n"], "compact").await;

    let (ctx, table) = env.table(true).await;
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

#[tokio::test(flavor = "multi_thread")]
async fn option_off_writes_nothing() {
    let env = indexed_env().await;
    env.append(&[batch(0..500)], &options(false)).await;
    let (ctx, table) = env.table(false).await;
    env.append(&[batch(500..900)], &options(false)).await;
    table
        .merge_adjacent_files(&ctx.state(), MergeOptions::default())
        .await
        .unwrap();
    assert!(!env.has_sidecar().await);
    assert!(env.pidx_files().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_declared_writes_nothing() {
    let env = setup().await;
    env.append(&[batch(0..500)], &options(true)).await;
    assert!(!env.has_sidecar().await);
    assert!(env.pidx_files().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_index_upload_keeps_the_data_write() {
    let env = indexed_env().await;
    env.store.fail_index_puts.store(true, Ordering::SeqCst);
    env.append(&[batch(0..500)], &options(true)).await;
    assert_eq!(env.live_files().await.len(), 1, "the data write committed");
    assert!(env.rows().await.is_empty(), "no row without its object");
    env.store.fail_index_puts.store(false, Ordering::SeqCst);
    env.append(&[batch(500..900)], &options(true)).await;
    assert_eq!(env.rows().await.len(), 2, "later writes index normally");
}

// ---------------------------------------------------------------------------
// Prune and the orphan sweep
// ---------------------------------------------------------------------------

/// A write whose commit failed leaves a row (and object) no file lists; prune
/// drops it once it is older than the grace period, and deletes its object.
#[tokio::test(flavor = "multi_thread")]
async fn uncommitted_write_index_is_pruned_after_grace() {
    let env = indexed_env().await;
    env.append(&[batch(0..500)], &options(true)).await;
    let live = env.rows().await;
    // Simulate a commit that failed after the index was recorded: forge a row
    // and object for a path no data file has.
    let ghost = format!("{}.ghost", live[0].index_key);
    std::fs::write(format!("/{ghost}"), b"x").unwrap();
    env.writer()
        .await
        .record_file_indexes(
            env.table_id,
            "write",
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
    // Inside the grace period: kept (it may be an in-flight write).
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
    // The live file's rows stay.
    assert_eq!(env.rows().await.len(), 2);
    env.assert_indexed(&["user_id", "n"], "write").await;
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
    }
}

/// A file compacted away keeps its index while time travel can read it, and
/// loses row and object once expiry removes the file.
#[tokio::test(flavor = "multi_thread")]
async fn prune_after_expiry_removes_index_with_file() {
    let env = indexed_env().await;
    for i in 0..2 {
        env.append(&[batch(i * 400..(i + 1) * 400)], &options(true))
            .await;
    }
    let old = env.rows().await;
    assert_eq!(old.len(), 4);
    let (ctx, table) = env.table(true).await;
    table
        .merge_adjacent_files(&ctx.state(), MergeOptions::default())
        .await
        .unwrap();
    let writer = env.writer().await;

    // Retired files still listed by an older snapshot keep their rows.
    let still_listed: usize = {
        use sqlx::Row as _;
        sqlx::query("SELECT count(*) FROM ducklake_data_file")
            .fetch_one(&env.pool().await)
            .await
            .unwrap()
            .get::<i64, _>(0) as usize
    };
    let pruned =
        datafusion_ducklake::maintenance::prune_file_indexes(&writer, env.store(), Duration::ZERO)
            .await
            .unwrap();
    if still_listed > 1 {
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
    let mut pruned_after = pruned;
    pruned_after.extend(
        datafusion_ducklake::maintenance::prune_file_indexes(&writer, env.store(), Duration::ZERO)
            .await
            .unwrap(),
    );
    let mut want: Vec<String> = old.iter().map(|r| r.index_key.clone()).collect();
    want.sort();
    pruned_after.sort();
    assert_eq!(pruned_after, want);
    for row in &old {
        assert!(!std::path::Path::new(&format!("/{}", row.index_key)).exists());
    }
    // The merge output keeps its index.
    let rows = env.assert_indexed(&["user_id", "n"], "compact").await;
    assert_eq!(env.rows().await.len(), rows.len());
}

/// The orphan sweep keeps every index whose data file is referenced and
/// deletes one whose data file is gone.
#[tokio::test(flavor = "multi_thread")]
async fn orphan_sweep_keeps_live_indexes() {
    let env = indexed_env().await;
    env.append(&[batch(0..500)], &options(true)).await;
    let live = env.pidx_files();
    assert_eq!(live.len(), 2);
    // An index of a data file that is not referenced (an aborted write).
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
    env.assert_indexed(&["user_id", "n"], "write").await;
}

// ---------------------------------------------------------------------------
// Backfill
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn backfill_is_idempotent_and_resumes() {
    let env = indexed_env().await;
    for i in 0..4 {
        env.append(&[batch(i * 400..(i + 1) * 400)], &options(false))
            .await;
    }
    assert!(env.rows().await.is_empty());
    let (ctx, table) = env.table(true).await;

    // A bounded pass, as an interrupted one leaves it.
    let first = table
        .build_file_indexes(&ctx.state(), BuildIndexOptions::default().with_max_files(1))
        .await
        .unwrap();
    assert_eq!(first.files_indexed, 1, "{first:?}");
    assert!(first.bytes_read > 0 && first.bytes_written > 0);
    assert_eq!(env.rows().await.len(), 2);

    let rest = table
        .build_file_indexes(
            &ctx.state(),
            BuildIndexOptions::default().with_parallelism(3),
        )
        .await
        .unwrap();
    assert_eq!(rest.files_indexed, 3, "{rest:?}");
    assert_eq!(rest.files_skipped, 1, "the first pass's file is done");
    assert_eq!(rest.files_failed, 0);
    env.assert_indexed(&["user_id", "n"], "backfill").await;

    let again = table
        .build_file_indexes(&ctx.state(), BuildIndexOptions::default())
        .await
        .unwrap();
    assert_eq!(again.files_indexed, 0, "{again:?}");
    assert_eq!(again.files_skipped, 4);
    assert_eq!(again.bytes_written, 0);
}

/// A row whose size does not match the file (a stale row) is rebuilt.
#[tokio::test(flavor = "multi_thread")]
async fn backfill_rebuilds_a_mismatched_row() {
    let env = indexed_env().await;
    env.append(&[batch(0..500)], &options(true)).await;
    sqlx::query("UPDATE ext_ducklake_file_index SET file_size_bytes = 1 WHERE column_id = ?")
        .bind(env.column_ids().await["n"])
        .execute(&env.pool().await)
        .await
        .unwrap();
    let (ctx, table) = env.table(true).await;
    let result = table
        .build_file_indexes(&ctx.state(), BuildIndexOptions::default())
        .await
        .unwrap();
    assert_eq!(result.files_indexed, 1, "{result:?}");
    let rows = env.rows().await;
    assert_eq!(rows.len(), 2);
    let n_id = env.column_ids().await["n"];
    let n = rows.iter().find(|r| r.column_id == n_id).unwrap();
    assert_eq!(n.built_by, "backfill");
    assert_eq!(
        rows.iter().filter(|r| r.built_by == "write").count(),
        1,
        "the matching user_id row is left as written"
    );
    let (path, size) = env.live_files().await[0].clone();
    assert_eq!(n.path, path);
    assert_eq!(n.file_size_bytes, size);
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_of_explicit_columns() {
    let env = setup().await;
    env.append(&[batch(0..500)], &options(false)).await;
    let (ctx, table) = env.table(true).await;
    // Nothing declared: nothing to do.
    let none = table
        .build_file_indexes(&ctx.state(), BuildIndexOptions::default())
        .await
        .unwrap();
    assert_eq!(none.files_indexed, 0);
    let some = table
        .build_file_indexes(
            &ctx.state(),
            BuildIndexOptions::default().with_columns(vec!["n".to_string()]),
        )
        .await
        .unwrap();
    assert_eq!(some.files_indexed, 1);
    env.assert_indexed(&["n"], "backfill").await;
    assert!(
        table
            .build_file_indexes(
                &ctx.state(),
                BuildIndexOptions::default().with_columns(vec!["ts".to_string()]),
            )
            .await
            .is_err(),
        "a timestamp column cannot be keyed"
    );
}

// ---------------------------------------------------------------------------
// Schema evolution
// ---------------------------------------------------------------------------

/// A column added later: older files get no row for it (they scan
/// normally), newer files do; the backfill skips files without it.
#[tokio::test(flavor = "multi_thread")]
async fn column_added_later() {
    let env = setup().await;
    env.declare("user_id, tag").await;
    env.append(&[batch(0..400)], &options(true)).await;
    let before = env.rows().await;
    assert_eq!(before.len(), 1, "only user_id exists yet");

    // Appending rows with a new column evolves the table.
    let wide = Arc::new(Schema::new(
        schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .chain([Field::new("tag", DataType::Utf8, true)])
            .collect::<Vec<_>>(),
    ));
    let base = batch(400..800);
    let mut columns = base.columns().to_vec();
    columns.push(Arc::new(StringArray::from_iter_values(
        (400..800).map(|i| format!("t{}", i % 5)),
    )));
    let wide_batch = RecordBatch::try_new(Arc::clone(&wide), columns).unwrap();
    let table_writer = env.table_writer(&options(true)).await;
    let mut session = table_writer
        .begin_write("main", "logs", wide.as_ref(), WriteMode::Append)
        .unwrap();
    session.write_batch(&wide_batch).unwrap();
    session.finish().await.unwrap();

    let ids = env.column_ids().await;
    let rows = env.rows().await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert_eq!(rows.iter().filter(|r| r.column_id == ids["tag"]).count(), 1);

    let (ctx, table) = env.table(true).await;
    let result = table
        .build_file_indexes(&ctx.state(), BuildIndexOptions::default())
        .await
        .unwrap();
    assert_eq!(result.files_indexed, 0, "{result:?}");
    assert_eq!(result.files_failed, 0);
    assert_eq!(env.rows().await.len(), 3);
}

/// A rename keeps the column id, so existing rows stay valid; the setting
/// names columns, so it is re-declared under the new name.
#[tokio::test(flavor = "multi_thread")]
async fn renamed_column_keeps_its_rows() {
    let env = indexed_env().await;
    env.append(&[batch(0..400)], &options(true)).await;
    let before = env.rows().await;
    let id = env.column_ids().await["user_id"];
    // The catalog side of a rename: same column id, new name.
    sqlx::query("UPDATE ducklake_column SET column_name = 'uid' WHERE column_id = ?")
        .bind(id)
        .execute(&env.pool().await)
        .await
        .unwrap();
    assert_eq!(env.column_ids().await["uid"], id);
    assert_eq!(env.rows().await.len(), before.len());

    env.declare("uid, n").await;
    let (ctx, table) = env.table(true).await;
    let result = table
        .build_file_indexes(&ctx.state(), BuildIndexOptions::default())
        .await
        .unwrap();
    assert_eq!(
        result.files_indexed, 0,
        "already indexed under its id: {result:?}"
    );
}
