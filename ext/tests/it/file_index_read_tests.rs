//! With the experimental `ext_file_index` read option, a scan whose filter
//! holds `col = lit` or `col IN (...)` on an indexed column drops files with no
//! posting and reads the rest through a `ParquetAccessPlan` (see
//! `src/file_index_scan.rs`).
//!
//! The bar is differential: every query returns exactly what it returns with
//! the option off, across file layouts (many files and row groups, deletes,
//! inlined rows, compaction, time travel, stale or missing index objects) and
//! scan shapes (1 and 7 partitions with byte-range splits, verified file order
//! on and off). Index objects are built here from the committed data files,
//! the way the backfill does, so the read path is tested on its own.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::context::SessionConfig;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::*;
use datafusion_datasource_parquet::ParquetAccessPlan;
use object_store::local::LocalFileSystem;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;

use datafusion_ducklake::extension::CatalogDb;
use datafusion_ducklake::{
    ColumnDef, DuckLakeCatalog, DuckLakeReadOptions, DuckLakeTable, DuckLakeTableWriter,
    DuckLakeWriteOptions, MergeOptions, MetadataWriter, NullOrder, SortDirection, SortField,
    SqliteMetadataProvider, SqliteMetadataWriter, WriteMode,
};
use datafusion_ducklake_ext::file_index::format::{
    FileIndexMeta, IndexValueType, PostingBuilder, index_file_name,
};
use datafusion_ducklake_ext::file_index::table as index_table;
use datafusion_ducklake_ext::file_index::{
    FileIndex, FileIndexRow, FileIndexSessionOptions, file_index_rows,
};
use datafusion_ducklake_ext::file_order::FileOrder;

/// Rows per appended file. Large enough that a key seen a few times in a file
/// is not a heavy hitter (`count > rows / 4096`), so it gets a selection.
const FILE_ROWS: i64 = 20_000;
const FILES: i64 = 8;
const ROW_GROUP_ROWS: usize = 4_096;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("shard", DataType::Int64, true),
        Field::new("id", DataType::Int64, false),
    ]))
}

/// The value of `user_id` for global row `g`:
/// - `dense`: every 7th row (a heavy hitter in every file);
/// - NULL: every 11th row;
/// - `few`: rows with `g % 5003 == 17` (about 4 per file: selected, not heavy);
/// - `solo`: one row, in file 3 only;
/// - otherwise `u<hash>` over 60,000 ids: about 2 rows per id in the table.
fn user_id(g: i64) -> Option<String> {
    if g % 7 == 0 {
        Some("dense".into())
    } else if g % 11 == 0 {
        None
    } else if g % 5003 == 17 {
        Some("few".into())
    } else if g == 3 * FILE_ROWS + 12_345 {
        Some("solo".into())
    } else {
        Some(format!("u{}", (g.wrapping_mul(2_654_435_761)) % 60_000))
    }
}

/// `shard` for row `g`: rare-ish Int64 keys, NULL every 13th row.
fn shard(g: i64) -> Option<i64> {
    (g % 13 != 0).then_some(g % 40_009)
}

/// Rows `range` (global row numbers), newest first: `ts` = `id` = `g` seconds.
fn batch(range: std::ops::Range<i64>) -> RecordBatch {
    let gs: Vec<i64> = range.rev().collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                gs.iter().map(|g| g * 1_000_000),
            )),
            Arc::new(StringArray::from_iter(gs.iter().map(|&g| user_id(g)))),
            Arc::new(Int64Array::from_iter(gs.iter().map(|&g| shard(g)))),
            Arc::new(Int64Array::from(gs.clone())),
        ],
    )
    .unwrap()
}

struct Env {
    temp: TempDir,
    table_id: i64,
    /// Writes (and compaction) build indexes for the declared columns.
    write_index: bool,
    /// The catalog database's pool: the extensions', and the test's own SQL.
    pool: sqlx::SqlitePool,
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

    fn db(&self) -> CatalogDb {
        crate::sqlite_db(&self.pool)
    }

    fn write_options(&self) -> DuckLakeWriteOptions {
        let mut options =
            DuckLakeWriteOptions::default().with_extension(Arc::new(FileOrder::new()));
        options.max_row_group_rows = Some(ROW_GROUP_ROWS);
        if self.write_index {
            options = options.with_extension(Arc::new(FileIndex::new()));
        }
        options
    }

    /// One data file per call, rows in the order given.
    async fn append(&self, batches: &[RecordBatch]) {
        let writer = DuckLakeTableWriter::new(
            Arc::new(self.writer().await),
            Arc::new(LocalFileSystem::new()),
        )
        .unwrap()
        .with_options(&self.write_options());
        let mut session = writer
            .begin_write("main", "logs", schema().as_ref(), WriteMode::Append)
            .unwrap();
        for b in batches {
            session.write_batch(b).unwrap();
        }
        session.finish().await.unwrap();
    }

    /// Rows that stay in the catalog (inlined), not in a data file.
    async fn append_inlined(&self, batch: RecordBatch) {
        let mut options = self.write_options();
        options.data_inlining_row_limit = Some(1_000);
        DuckLakeTableWriter::new(
            Arc::new(self.writer().await),
            Arc::new(LocalFileSystem::new()),
        )
        .unwrap()
        .with_options(&options)
        .append_table("main", "logs", &[batch])
        .await
        .unwrap();
    }

    async fn pool(&self) -> sqlx::SqlitePool {
        self.pool.clone()
    }

    /// DuckLake column id of `name`.
    async fn column_id(&self, name: &str) -> i64 {
        use sqlx::Row;
        sqlx::query(
            "SELECT column_id FROM ducklake_column
             WHERE table_id = ? AND column_name = ? AND end_snapshot IS NULL",
        )
        .bind(self.table_id)
        .bind(name)
        .fetch_one(&self.pool().await)
        .await
        .unwrap()
        .get(0)
    }

    /// Every data file the catalog has ever listed for the table (live or
    /// not, for time travel): `(catalog path, size)`.
    async fn data_files(&self) -> Vec<(String, i64)> {
        use sqlx::Row;
        sqlx::query(
            "SELECT path, file_size_bytes FROM ducklake_data_file WHERE table_id = ?
             ORDER BY data_file_id",
        )
        .bind(self.table_id)
        .fetch_all(&self.pool().await)
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
    }

    /// Build and record the index of `column` for every data file without
    /// one, as the backfill does: read the column, feed the builder in file
    /// order, write the object beside the file, then record the row.
    async fn build_indexes(&self, column: &str) -> usize {
        let column_id = self.column_id(column).await;
        let indexed: std::collections::HashSet<String> = {
            use sqlx::Row;
            sqlx::query("SELECT path FROM ext_ducklake_file_index WHERE column_id = ?")
                .bind(column_id)
                .fetch_all(&self.pool().await)
                .await
                .map(|rows| rows.iter().map(|r| r.get::<String, _>(0)).collect())
                .unwrap_or_default()
        };
        let on_disk = parquet_files(&self.data_path());
        let mut metas = Vec::new();
        for (path, size) in self.data_files().await {
            if indexed.contains(&path) {
                continue;
            }
            let name = path.rsplit('/').next().unwrap();
            let local = on_disk
                .iter()
                .find(|p| p.file_name().unwrap().to_str().unwrap() == name)
                .unwrap_or_else(|| panic!("data file {path} not on disk"));
            assert_eq!(std::fs::metadata(local).unwrap().len() as i64, size);
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(local).unwrap())
                    .unwrap();
            let row_group_rows: Vec<u64> = builder
                .metadata()
                .row_groups()
                .iter()
                .map(|rg| rg.num_rows() as u64)
                .collect();
            let field = builder.schema().field_with_name(column).unwrap().clone();
            let value_type = IndexValueType::for_arrow(field.data_type()).unwrap();
            let mut postings = PostingBuilder::new(column_id, value_type);
            for batch in builder.with_batch_size(1_000).build().unwrap() {
                let batch = batch.unwrap();
                postings
                    .observe(batch.column_by_name(column).unwrap())
                    .unwrap();
            }
            let built = postings.finish(row_group_rows).unwrap();
            let index_name = index_file_name(&path, column_id);
            std::fs::write(local.with_file_name(&index_name), &built.bytes).unwrap();
            let index_local = local.with_file_name(&index_name);
            metas.push(FileIndexRow {
                meta: built.meta(&path, size, column_id, value_type, index_name),
                index_key: index_local
                    .to_str()
                    .unwrap()
                    .trim_start_matches('/')
                    .to_string(),
                built_by: "backfill".to_string(),
            });
        }
        let n = metas.len();
        index_table::record(&self.db(), self.table_id, metas)
            .await
            .unwrap();
        n
    }

    async fn index_rows(&self) -> Vec<FileIndexMeta> {
        let paths: Vec<String> = self.data_files().await.into_iter().map(|f| f.0).collect();
        let paths: Vec<&str> = paths.iter().map(String::as_str).collect();
        let ids = [self.column_id("user_id").await, self.column_id("shard").await];
        file_index_rows(&self.db(), self.table_id, &paths, &ids)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.meta)
            .collect()
    }

    async fn ctx(&self, shape: Shape, index: bool) -> SessionContext {
        let writer = self.writer().await;
        let provider = SqliteMetadataProvider::new(&self.url()).await.unwrap();
        // Registered with scans off rather than left out, so a session's
        // `SET ducklake_file_index.enabled = true` can still turn it on.
        let mut read = DuckLakeReadOptions::default()
            .with_extension(Arc::new(FileIndex::new().with_scans(index)));
        if shape.order {
            read = read.with_extension(Arc::new(FileOrder::new()));
        }
        let catalog = match shape.snapshot {
            Some(snapshot) => DuckLakeCatalog::with_snapshot(Arc::new(provider), snapshot)
                .unwrap()
                .with_read_options(read),
            None => DuckLakeCatalog::with_writer(Arc::new(provider), Arc::new(writer))
                .unwrap()
                .with_read_options(read)
                .with_write_options(self.write_options()),
        };
        let mut config = SessionConfig::new()
            .with_target_partitions(shape.partitions)
            .with_option_extension(FileIndexSessionOptions::default());
        // Split files into byte ranges whenever there are spare partitions.
        config.options_mut().optimizer.repartition_file_min_size = 1;
        config.options_mut().execution.parquet.pushdown_filters = shape.pushdown;
        let ctx = SessionContext::new_with_config(config);
        ctx.register_catalog("ducklake", Arc::new(catalog));
        ctx
    }

    async fn snapshot(&self) -> i64 {
        let provider = SqliteMetadataProvider::new(&self.url()).await.unwrap();
        datafusion_ducklake::MetadataProvider::get_current_snapshot(&provider).unwrap()
    }
}

#[derive(Clone, Copy, Debug)]
struct Shape {
    partitions: usize,
    order: bool,
    /// Parquet filter pushdown (RuntimeDB runs with it on): the filter moves
    /// into the scan instead of a `FilterExec`, which is what lets the
    /// verified-order path serve a filtered `ORDER BY ... LIMIT`.
    pushdown: bool,
    snapshot: Option<i64>,
}

const fn shape(partitions: usize, order: bool, pushdown: bool) -> Shape {
    Shape {
        partitions,
        order,
        pushdown,
        snapshot: None,
    }
}

const SHAPES: [Shape; 8] = [
    shape(1, false, false),
    shape(7, false, false),
    shape(1, true, false),
    shape(7, true, false),
    shape(1, false, true),
    shape(7, false, true),
    shape(1, true, true),
    shape(7, true, true),
];

async fn setup() -> Env {
    let temp = TempDir::new().unwrap();
    std::fs::create_dir_all(temp.path().join("data")).unwrap();
    let url = format!("sqlite:{}?mode=rwc", temp.path().join("test.db").display());
    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    let env = Env {
        temp,
        table_id: 0,
        write_index: false,
        pool,
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
    writer
        .set_sort_spec(
            s.table_id,
            &[SortField::column(0, "ts", SortDirection::Desc, NullOrder::NullsFirst)],
        )
        .unwrap();
    Env {
        table_id: s.table_id,
        ..env
    }
}

/// `FILES` disjoint, newest-first files of `FILE_ROWS` rows, indexed on
/// `user_id` and `shard`.
async fn indexed_table() -> Env {
    let env = setup().await;
    for f in 0..FILES {
        env.append(&[batch(f * FILE_ROWS..(f + 1) * FILE_ROWS)])
            .await;
    }
    assert_eq!(env.build_indexes("user_id").await, FILES as usize);
    assert_eq!(env.build_indexes("shard").await, FILES as usize);
    env
}

fn parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(parquet_files(&path));
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
    out
}

fn index_objects(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(index_objects(&path));
        } else if path.extension().is_some_and(|e| e == "pidx") {
            out.push(path);
        }
    }
    out
}

/// A `user_id` seen `n` times in the table (`u` ids only), lowest first.
fn rare_user(n: usize) -> String {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for g in 0..FILES * FILE_ROWS {
        if let Some(u) = user_id(g).filter(|u| u.starts_with('u')) {
            *counts.entry(u).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .find(|(_, c)| *c == n)
        .map(|(u, _)| u)
        .unwrap_or_else(|| panic!("no user with {n} rows"))
}

/// Seconds as a SQL timestamp literal (`ts` = `g` seconds after the epoch).
fn at(seconds: i64) -> String {
    format!("to_timestamp_seconds({seconds})")
}

/// The differential matrix: hits (`SELECT *` newest first, LIMIT) and
/// histogram shapes, `=` and `IN`, rare, absent, dense and NULL values, two
/// indexed columns at once, and time windows that cut files.
fn queries() -> Vec<String> {
    let one = rare_user(1);
    let two = rare_user(2);
    let three = rare_user(3);
    let users = [
        format!("'{one}'"),
        format!("'{two}'"),
        format!("'{three}'"),
        "'few'".into(),
        "'solo'".into(),
        "'dense'".into(),
        "'nobody'".into(),
    ];
    // A window from the middle of file 2 to the middle of file 5.
    let window = format!(
        "ts >= {} AND ts < {}",
        at(2 * FILE_ROWS + FILE_ROWS / 2),
        at(5 * FILE_ROWS + FILE_ROWS / 3)
    );
    let mut out = Vec::new();
    for u in &users {
        out.push(format!(
            "SELECT * FROM ducklake.main.logs WHERE user_id = {u} ORDER BY ts DESC LIMIT 50"
        ));
        out.push(format!(
            "SELECT ts, id FROM ducklake.main.logs WHERE user_id = {u} AND {window} \
             ORDER BY ts DESC LIMIT 3"
        ));
        out.push(format!(
            "SELECT date_trunc('hour', ts) AS h, count(*) AS n FROM ducklake.main.logs \
             WHERE user_id = {u} AND {window} GROUP BY h ORDER BY h"
        ));
        out.push(format!(
            "SELECT count(*), min(id), max(id) FROM ducklake.main.logs WHERE user_id = {u}"
        ));
        out.push(format!(
            "SELECT * FROM ducklake.main.logs WHERE {u} = user_id ORDER BY id"
        ));
    }
    out.extend([
        format!(
            "SELECT * FROM ducklake.main.logs WHERE user_id IN ('{one}', '{two}', 'nobody') \
             ORDER BY ts DESC LIMIT 10"
        ),
        format!(
            "SELECT user_id, count(*) FROM ducklake.main.logs \
             WHERE user_id IN ('{three}', 'few', 'solo', 'nobody', 'dense') \
             GROUP BY user_id ORDER BY user_id"
        ),
        "SELECT count(*) FROM ducklake.main.logs WHERE user_id IN ('nobody', 'nobody2')".into(),
        "SELECT count(*) FROM ducklake.main.logs WHERE user_id IS NULL".into(),
        "SELECT count(*) FROM ducklake.main.logs WHERE user_id = NULL".into(),
        "SELECT * FROM ducklake.main.logs WHERE shard = 17 ORDER BY id".into(),
        "SELECT * FROM ducklake.main.logs WHERE shard IN (17, 40008, -1) ORDER BY ts DESC LIMIT 5"
            .into(),
        format!(
            "SELECT * FROM ducklake.main.logs WHERE user_id = 'few' AND shard = {} ORDER BY id",
            shard(5003 + 17).unwrap()
        ),
        format!(
            "SELECT * FROM ducklake.main.logs WHERE user_id = 'dense' AND shard = {} ORDER BY id",
            shard(7 * 101).unwrap()
        ),
        format!(
            "SELECT * FROM ducklake.main.logs WHERE user_id = '{one}' OR shard = 17 ORDER BY id"
        ),
        format!(
            "SELECT * FROM ducklake.main.logs WHERE user_id <> '{one}' AND shard = 99 ORDER BY id"
        ),
        "SELECT count(*) FROM ducklake.main.logs".into(),
        "SELECT ts, user_id FROM ducklake.main.logs WHERE user_id = 'few' \
         ORDER BY ts DESC LIMIT 2 OFFSET 3"
            .into(),
    ]);
    out
}

async fn run(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    // An empty result is empty whether it came as no batch or an empty one.
    let batches: Vec<RecordBatch> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
    arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

/// Every query in `queries()` returns the same rows, in the same order, with
/// the index on as off, under every shape.
async fn assert_differential(env: &Env, shapes: &[Shape]) {
    let queries = queries();
    for shape in shapes {
        let off = env.ctx(*shape, false).await;
        let on = env.ctx(*shape, true).await;
        for sql in &queries {
            assert_eq!(
                run(&on, sql).await,
                run(&off, sql).await,
                "{shape:?}\n{sql}"
            );
        }
    }
}

async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

/// Per parquet scan in `plan`: `(files, files carrying an access plan)`, by
/// distinct path (a byte-range split counts once).
fn scanned_files(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
    let mut paths: HashMap<String, bool> = HashMap::new();
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if let Some(scan) = node.downcast_ref::<DataSourceExec>()
            && let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>()
        {
            for file in config.file_groups.iter().flat_map(|g| g.iter()) {
                *paths
                    .entry(file.object_meta.location.to_string())
                    .or_default() |= file.extensions.get::<ParquetAccessPlan>().is_some();
            }
        }
        stack.extend(node.children().into_iter().cloned());
    }
    (paths.len(), paths.values().filter(|s| **s).count())
}

fn text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

// ---------------------------------------------------------------------------
// Differential: index on == index off
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn on_equals_off_across_shapes() {
    let env = indexed_table().await;
    assert_differential(&env, &SHAPES).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_equals_off_with_deletes_and_inlined_rows() {
    let env = indexed_table().await;
    let ctx = env.ctx(SHAPES[0], false).await;
    // Deletes in two files, including rows of the rare and selected keys.
    let two = rare_user(2);
    for sql in [
        format!("DELETE FROM ducklake.main.logs WHERE user_id = '{two}'"),
        "DELETE FROM ducklake.main.logs WHERE id % 5003 = 17 AND id < 30000".into(),
        format!(
            "DELETE FROM ducklake.main.logs WHERE id BETWEEN {} AND {}",
            6 * FILE_ROWS,
            6 * FILE_ROWS + 500
        ),
    ] {
        ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    }
    // Inlined rows holding indexed keys: the index never sees them.
    let base = FILES * FILE_ROWS;
    env.append_inlined(batch(base..base + 200)).await;
    let few_row = 5003 * ((base + 5003) / 5003) + 17;
    env.append_inlined(batch(few_row..few_row + 1)).await;
    assert_differential(&env, &SHAPES).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_equals_off_across_compaction_and_time_travel() {
    let env = indexed_table().await;
    // Mid-ingest: once the files are merged, reading here needs the merged
    // file's snapshot filter (a partial file), which only ever prunes.
    let mid: i64 = {
        use sqlx::Row;
        sqlx::query(
            "SELECT begin_snapshot FROM ducklake_data_file WHERE table_id = ?
             ORDER BY data_file_id LIMIT 1 OFFSET 3",
        )
        .bind(env.table_id)
        .fetch_one(&env.pool().await)
        .await
        .unwrap()
        .get(0)
    };
    let before = env.snapshot().await;
    let ctx = env.ctx(SHAPES[0], false).await;
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
    let options = MergeOptions {
        target_file_size: u64::MAX,
        ..MergeOptions::default()
    };
    let merged = table
        .merge_adjacent_files(&ctx.state(), options)
        .await
        .unwrap();
    let after = env.snapshot().await;
    assert!(after > before, "{merged:?}");

    // The merged output has no index yet: scanned as before, while reads at
    // the old snapshots still use the old files' indexes.
    let at = |snapshot| -> Vec<Shape> {
        SHAPES
            .iter()
            .map(|s| Shape {
                snapshot,
                ..*s
            })
            .collect()
    };
    assert_differential(&env, &SHAPES).await;
    assert_differential(&env, &at(Some(before))).await;

    // Index the merged output, then delete from it.
    assert!(env.build_indexes("user_id").await > 0);
    let ctx = env.ctx(SHAPES[0], false).await;
    ctx.sql("DELETE FROM ducklake.main.logs WHERE id % 97 = 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for snapshot in [None, Some(mid), Some(before), Some(after)] {
        assert_differential(&env, &at(snapshot)).await;
    }
}

/// A row whose size no longer matches, a missing object and a corrupt
/// object each mean "scan the file as before".
#[tokio::test(flavor = "multi_thread")]
async fn broken_indexes_fall_back() {
    let env = indexed_table().await;
    let pool = env.pool().await;
    let files = env.data_files().await;
    sqlx::query(
        "UPDATE ext_ducklake_file_index SET file_size_bytes = file_size_bytes + 1
         WHERE path = ?",
    )
    .bind(&files[1].0)
    .execute(&pool)
    .await
    .unwrap();
    let objects = index_objects(&env.data_path());
    let name = |p: &str| p.rsplit('/').next().unwrap().to_string();
    let of = |i: usize| {
        objects
            .iter()
            .filter(|o| {
                o.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with(&name(&files[i].0))
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    for o in of(2) {
        std::fs::remove_file(o).unwrap();
    }
    for o in of(3) {
        std::fs::write(o, b"not an index").unwrap();
    }
    // A row claiming a different row count than the catalog's.
    sqlx::query("UPDATE ext_ducklake_file_index SET row_count = row_count + 1 WHERE path = ?")
        .bind(&files[4].0)
        .execute(&pool)
        .await
        .unwrap();
    assert_differential(&env, &SHAPES).await;
}

// ---------------------------------------------------------------------------
// Plans: what the index removes and selects
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn rare_key_scans_only_its_files_with_selections() {
    let env = indexed_table().await;
    assert_eq!(env.index_rows().await.len(), 2 * FILES as usize);
    let one = rare_user(1);
    for shape in SHAPES {
        let sql = format!("SELECT * FROM ducklake.main.logs WHERE user_id = '{one}'");
        let off = scanned_files(&physical_plan(&env.ctx(shape, false).await, &sql).await);
        let on_plan = physical_plan(&env.ctx(shape, true).await, &sql).await;
        let on = scanned_files(&on_plan);
        assert_eq!(off, (FILES as usize, 0), "{shape:?}");
        // One file holds the row; it is read through a selection. Fewer than
        // 10% of the files would hold for a 60-file window too.
        assert_eq!(on, (1, 1), "{shape:?}\n{}", text(&on_plan));
        // The filter stays.
        assert!(text(&on_plan).contains("user_id@"), "{}", text(&on_plan));

        let absent = "SELECT * FROM ducklake.main.logs WHERE user_id = 'nobody'";
        let plan = physical_plan(&env.ctx(shape, true).await, absent).await;
        assert_eq!(scanned_files(&plan), (0, 0), "{}", text(&plan));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn heavy_hitters_and_unindexed_predicates_scan_every_file() {
    let env = indexed_table().await;
    let ctx = env.ctx(SHAPES[1], true).await;
    for sql in [
        "SELECT * FROM ducklake.main.logs WHERE user_id = 'dense'",
        "SELECT * FROM ducklake.main.logs WHERE user_id IS NULL",
        "SELECT * FROM ducklake.main.logs WHERE user_id LIKE 'u1%'",
        "SELECT * FROM ducklake.main.logs WHERE id = 5",
    ] {
        let plan = physical_plan(&ctx, sql).await;
        assert_eq!(scanned_files(&plan).1, 0, "{sql}\n{}", text(&plan));
    }
    // `few` is in every file but never heavy: every file is selected.
    let plan = physical_plan(
        &ctx,
        "SELECT * FROM ducklake.main.logs WHERE user_id = 'few'",
    )
    .await;
    assert_eq!(
        scanned_files(&plan),
        (FILES as usize, FILES as usize),
        "{}",
        text(&plan)
    );
}

/// The verified-order path still serves `ORDER BY ts DESC LIMIT n` without a
/// sort, over the selected files only, and stops early.
#[tokio::test(flavor = "multi_thread")]
async fn ordered_limit_reads_selected_files() {
    let env = indexed_table().await;
    for partitions in [1, 7] {
        let ctx = env.ctx(shape(partitions, true, true), true).await;
        let plan = physical_plan(
            &ctx,
            "SELECT * FROM ducklake.main.logs WHERE user_id = 'few' ORDER BY ts DESC LIMIT 3",
        )
        .await;
        let t = text(&plan);
        assert!(!t.contains("SortExec"), "{t}");
        assert_eq!(
            scanned_files(&plan),
            (FILES as usize, FILES as usize),
            "{t}"
        );

        let one = rare_user(1);
        let plan = physical_plan(
            &ctx,
            &format!(
                "SELECT * FROM ducklake.main.logs WHERE user_id = '{one}' \
                 ORDER BY ts DESC LIMIT 3"
            ),
        )
        .await;
        assert_eq!(scanned_files(&plan), (1, 1), "{}", text(&plan));
    }
}

/// Files with deletes are dropped when the key is absent, but never read
/// through a selection in v1.
#[tokio::test(flavor = "multi_thread")]
async fn files_with_deletes_are_pruned_not_selected() {
    let env = indexed_table().await;
    let ctx = env.ctx(SHAPES[0], false).await;
    ctx.sql("DELETE FROM ducklake.main.logs WHERE id % 1000 = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let ctx = env.ctx(SHAPES[0], true).await;
    let plan = physical_plan(
        &ctx,
        "SELECT * FROM ducklake.main.logs WHERE user_id = 'few'",
    )
    .await;
    assert_eq!(scanned_files(&plan), (FILES as usize, 0), "{}", text(&plan));
    let one = rare_user(1);
    let plan = physical_plan(
        &ctx,
        &format!("SELECT * FROM ducklake.main.logs WHERE user_id = '{one}'"),
    )
    .await;
    assert_eq!(scanned_files(&plan), (1, 0), "{}", text(&plan));
}

/// With the option off the sidecar is never read: a broken sidecar row
/// changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn option_off_never_reads_the_sidecar() {
    let env = indexed_table().await;
    let ctx = env.ctx(SHAPES[1], false).await;
    let one = rare_user(1);
    let plan = physical_plan(
        &ctx,
        &format!("SELECT * FROM ducklake.main.logs WHERE user_id = '{one}'"),
    )
    .await;
    assert_eq!(scanned_files(&plan), (FILES as usize, 0));
}

/// `SET ducklake.ext_file_index` overrides the table's option per session,
/// both ways (the spot check compares a query with it off).
#[tokio::test(flavor = "multi_thread")]
async fn session_setting_overrides_the_read_option() {
    let env = indexed_table().await;
    let one = rare_user(1);
    let sql = format!("SELECT * FROM ducklake.main.logs WHERE user_id = '{one}'");
    let on = env.ctx(SHAPES[1], true).await;
    assert_eq!(scanned_files(&physical_plan(&on, &sql).await), (1, 1));
    on.sql("SET ducklake_file_index.enabled = false")
        .await
        .unwrap();
    assert_eq!(
        scanned_files(&physical_plan(&on, &sql).await),
        (FILES as usize, 0)
    );
    let off = env.ctx(SHAPES[1], false).await;
    off.sql("SET ducklake_file_index.enabled = true")
        .await
        .unwrap();
    assert_eq!(scanned_files(&physical_plan(&off, &sql).await), (1, 1));
    assert_eq!(run(&on, &sql).await, run(&off, &sql).await);
}

/// Bytes the parquet scans of an executed plan read.
fn bytes_scanned(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut total = 0;
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.downcast_ref::<DataSourceExec>().is_some()
            && let Some(metrics) = node.metrics()
            && let Some(bytes) = metrics.sum_by_name("bytes_scanned")
        {
            total += bytes.as_usize();
        }
        stack.extend(node.children().into_iter().cloned());
    }
    total
}

async fn execute(ctx: &SessionContext, sql: &str) -> (String, Arc<dyn ExecutionPlan>) {
    let plan = physical_plan(ctx, sql).await;
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap();
    (
        arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string(),
        plan,
    )
}

/// The newest rows of a selected key come from the newest files: each
/// ordered chain stops after its newest file instead of reading every file's
/// selected rows (the hits query's cost once a key has thousands of
/// postings). One chain per partition, so a LIMIT reads about one file per
/// partition: 1 of 8 here at 1 partition, 3 of 8 at 3.
#[tokio::test(flavor = "multi_thread")]
async fn ordered_limit_stops_early_on_selected_files() {
    let env = indexed_table().await;
    for partitions in [1, 3] {
        let ctx = env.ctx(shape(partitions, true, true), true).await;
        let all = "SELECT * FROM ducklake.main.logs WHERE user_id = 'few' ORDER BY ts DESC";
        let top = format!("{all} LIMIT 3");
        let (rows_all, plan_all) = execute(&ctx, all).await;
        let (rows_top, plan_top) = execute(&ctx, &top).await;
        assert_eq!(scanned_files(&plan_top), (FILES as usize, FILES as usize));
        assert!(!text(&plan_top).contains("SortExec"), "{}", text(&plan_top));
        let (read_all, read_top) = (bytes_scanned(&plan_all), bytes_scanned(&plan_top));
        assert!(read_all > 0, "{}", text(&plan_all));
        // About `partitions` files of 8, with slack for the merge's buffering.
        assert!(
            read_top * 8 <= read_all * (partitions + 1),
            "partitions={partitions}: LIMIT read {read_top} of {read_all} bytes\n{}",
            text(&plan_top)
        );
        let off = env.ctx(shape(partitions, true, true), false).await;
        assert_eq!(rows_top, execute(&off, &top).await.0);
        assert_eq!(rows_all, execute(&off, all).await.0);
    }
}

/// Indexes the writer and the merge build themselves (not a backfill) serve
/// the read path the same way.
#[tokio::test(flavor = "multi_thread")]
async fn indexes_built_on_write_and_merge_serve_reads() {
    let mut env = setup().await;
    env.write_index = true;
    env.writer()
        .await
        .set_table_setting(env.table_id, "ext_file_index_columns", "user_id,shard")
        .unwrap();
    for f in 0..FILES {
        env.append(&[batch(f * FILE_ROWS..(f + 1) * FILE_ROWS)])
            .await;
    }
    assert_eq!(env.index_rows().await.len(), 2 * FILES as usize);
    assert_differential(&env, &SHAPES).await;
    let one = rare_user(1);
    let sql = format!("SELECT * FROM ducklake.main.logs WHERE user_id = '{one}'");
    let plan = physical_plan(&env.ctx(SHAPES[1], true).await, &sql).await;
    assert_eq!(scanned_files(&plan), (1, 1), "{}", text(&plan));

    let ctx = env.ctx(SHAPES[0], false).await;
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
    let options = MergeOptions {
        target_file_size: u64::MAX,
        ..MergeOptions::default()
    };
    table
        .merge_adjacent_files(&ctx.state(), options)
        .await
        .unwrap();
    // One merged file, indexed by the merge: read through a selection.
    let plan = physical_plan(&env.ctx(SHAPES[1], true).await, &sql).await;
    assert_eq!(scanned_files(&plan), (1, 1), "{}", text(&plan));
    let plan = physical_plan(
        &env.ctx(SHAPES[1], true).await,
        "SELECT * FROM ducklake.main.logs WHERE user_id = 'nobody'",
    )
    .await;
    assert_eq!(scanned_files(&plan), (0, 0), "{}", text(&plan));
    assert_differential(&env, &SHAPES).await;
}
