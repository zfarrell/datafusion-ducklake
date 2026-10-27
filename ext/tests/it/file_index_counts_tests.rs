//! `ext_file_index_counts`: the hourly histogram answered from per-file
//! heavy-hitter bucket counts must equal the raw query exactly.
//!
//! Every query runs twice, on a context with the rule and the read option and
//! on one without, and the results (ordered by bucket) must be identical. The
//! counts are recorded by the backfill (`build_file_indexes` with a count
//! column), so the test covers the whole path from parquet to answer.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::util::pretty::pretty_format_batches;
use datafusion::execution::context::SessionConfig;
use datafusion::physical_plan::displayable;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    ColumnDef, DuckLakeCatalog, DuckLakeReadOptions, DuckLakeTable, DuckLakeTableWriter,
    DuckLakeWriteOptions, MetadataWriter, SqliteMetadataProvider, SqliteMetadataWriter, WriteMode,
};
use datafusion_ducklake_ext::file_index::counts::{
    BUCKET_GRAIN_HOUR_US, BucketCountRule, bucket_counts_from_json, bucket_counts_to_json,
};
use datafusion_ducklake_ext::file_index::format::{IndexKey, IndexValueType};
use datafusion_ducklake_ext::file_index::{BuildIndexOptions, FileIndex, FileIndexTableExt};

const H: i64 = BUCKET_GRAIN_HOUR_US;

/// 2026-10-01T00:00:00Z in µs.
const T0: i64 = 1_790_812_800_000_000;

fn schema(tz: Option<&str>) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, tz.map(Arc::from)),
            true,
        ),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
    ]))
}

struct Env {
    temp: TempDir,
    table_id: i64,
    tz: Option<&'static str>,
    next_id: std::cell::Cell<i64>,
    /// The catalog database's pool: the extension's, and the test's own SQL.
    pool: sqlx::SqlitePool,
}

/// Deterministic rows: `n` rows from `start` with `step` µs between them;
/// user `u0` every 2nd row, `u1` every 3rd, `u2` every 7th, `u3` otherwise,
/// and every 11th ts NULL.
fn rows(env: &Env, start: i64, step: i64, n: usize) -> RecordBatch {
    let mut ts = Vec::with_capacity(n);
    let mut users = Vec::with_capacity(n);
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let id = env.next_id.get();
        env.next_id.set(id + 1);
        ts.push((id % 11 != 0).then_some(start + i as i64 * step));
        users.push(Some(match id {
            _ if id % 2 == 0 => "u0",
            _ if id % 3 == 0 => "u1",
            _ if id % 7 == 0 => "u2",
            _ => "u3",
        }));
        ids.push(id);
    }
    let ts = TimestampMicrosecondArray::from(ts).with_timezone_opt(env.tz.map(Arc::<str>::from));
    RecordBatch::try_new(
        schema(env.tz),
        vec![Arc::new(ts), Arc::new(StringArray::from(users)), Arc::new(Int64Array::from(ids))],
    )
    .unwrap()
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

    /// One data file per call.
    async fn append(&self, batch: RecordBatch) {
        let writer = DuckLakeTableWriter::new(
            Arc::new(self.writer().await),
            Arc::new(LocalFileSystem::new()),
        )
        .unwrap();
        let mut session = writer
            .begin_write("main", "logs", schema(self.tz).as_ref(), WriteMode::Append)
            .unwrap();
        session.write_batch(&batch).unwrap();
        session.finish().await.unwrap();
    }

    /// Rows inlined into the catalog instead of a file.
    async fn append_inlined(&self, batch: RecordBatch) {
        let options = DuckLakeWriteOptions::default().with_data_inlining_row_limit(1_000);
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

    /// Read options with the counts rewrite on or off, and index scans off:
    /// a fresh extension instance, which has read nothing yet.
    fn counts_options(&self, counts: bool) -> DuckLakeReadOptions {
        DuckLakeReadOptions::default().with_extension(Arc::new(
            FileIndex::new().with_scans(false).with_counts(counts),
        ))
    }

    /// Live data files: catalog path and size, in data_file_id order.
    async fn live_files(&self) -> Vec<(String, i64)> {
        use sqlx::Row;
        sqlx::query(
            "SELECT path, file_size_bytes FROM ducklake_data_file
             WHERE table_id = ? AND end_snapshot IS NULL ORDER BY data_file_id",
        )
        .bind(self.table_id)
        .fetch_all(&self.pool().await)
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
    }

    /// Index `user_id` with hourly counts over `ts`, as the backfill does.
    async fn record_counts(&self) {
        let ctx = self.ctx(false).await;
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
        let result = table
            .build_file_indexes(
                &ctx.state(),
                BuildIndexOptions::default()
                    .with_columns(vec!["user_id".to_string()])
                    .with_count_column("ts"),
            )
            .await
            .unwrap();
        assert_eq!(result.files_failed, 0);
        assert!(result.files_indexed > 0);
    }

    /// The users with recorded counts in each live file, in file order.
    async fn counted_users(&self) -> Vec<Vec<String>> {
        use sqlx::Row;
        let pool = self.pool().await;
        let mut out = Vec::new();
        for (path, _) in self.live_files().await {
            let json: Option<String> =
                sqlx::query("SELECT bucket_counts FROM ext_ducklake_file_index WHERE path = ?")
                    .bind(&path)
                    .fetch_optional(&pool)
                    .await
                    .unwrap()
                    .and_then(|row| row.get(0));
            let users = json
                .and_then(|j| bucket_counts_from_json(&j, IndexValueType::Utf8))
                .map(|c| {
                    c.keys
                        .iter()
                        .map(|(k, _)| match k {
                            IndexKey::Utf8(b) => String::from_utf8(b.to_vec()).unwrap(),
                            other => panic!("{other:?}"),
                        })
                        .collect()
                })
                .unwrap_or_default();
            out.push(users);
        }
        out
    }

    async fn ctx(&self, counts: bool) -> SessionContext {
        let provider = SqliteMetadataProvider::new(&self.url()).await.unwrap();
        let catalog =
            DuckLakeCatalog::with_writer(Arc::new(provider), Arc::new(self.writer().await))
                .unwrap()
                .with_read_options(self.counts_options(counts));
        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(3)
                .with_batch_size(50),
        );
        if counts {
            ctx.add_optimizer_rule(Arc::new(BucketCountRule::default()));
        }
        ctx.register_catalog("ducklake", Arc::new(catalog));
        ctx
    }
}

async fn setup(tz: Option<&'static str>) -> Env {
    let temp = TempDir::new().unwrap();
    std::fs::create_dir_all(temp.path().join("data")).unwrap();
    let url = format!("sqlite:{}?mode=rwc", temp.path().join("test.db").display());
    let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
    let mut env = Env {
        temp,
        table_id: 0,
        tz,
        next_id: std::cell::Cell::new(0),
        pool,
    };
    let writer = env.writer().await;
    writer
        .set_data_path(env.data_path().to_str().unwrap())
        .unwrap();
    let cols: Vec<ColumnDef> = schema(tz)
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
    env.table_id = s.table_id;
    env
}

/// Files spread over ~10 hours, some overlapping, rows not aligned to hours.
/// A heavy hitter needs more than 1024 rows in a file: u0 is one in the four
/// big files, u1 only in the two biggest, u2 nowhere; the small files have
/// none, so they are never counted.
async fn populate(env: &Env) {
    // 4 time-local files, ~2.5h each.
    for f in 0..4 {
        let (step, n) = if f % 2 == 0 {
            (1_000_003, 9_000)
        } else {
            (1_500_001, 6_000)
        };
        env.append(rows(env, T0 + f * 9_000_000_000 + 1_234_567, step, n))
            .await;
    }
    // A file spanning the whole range (as a merge of unsorted data would).
    env.append(rows(env, T0 - 5, 29_999_999, 1_250)).await;
    // Rows exactly on hour (and quarter-hour) boundaries, and one µs before.
    env.append(rows(env, T0, H / 4, 61)).await;
    env.append(rows(env, T0 - 1, H / 4, 61)).await;
}

async fn run(ctx: &SessionContext, sql: &str) -> (String, String) {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let plan = displayable(plan.as_ref()).indent(true).to_string();
    let batches = df.collect().await.unwrap();
    (pretty_format_batches(&batches).unwrap().to_string(), plan)
}

fn ts_lit(us: i64, tz: Option<&str>) -> String {
    let secs = us.div_euclid(1_000_000);
    let micros = us.rem_euclid(1_000_000);
    let dt = chrono_like(secs);
    match tz {
        Some(_) => format!("TIMESTAMPTZ '{dt}.{micros:06}+00:00'"),
        None => format!("TIMESTAMP '{dt}.{micros:06}'"),
    }
}

/// `YYYY-MM-DD HH:MM:SS` of epoch seconds (UTC), without a chrono dep.
fn chrono_like(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 {
        mp + 3
    } else {
        mp - 9
    };
    let y = if m <= 2 {
        y + 1
    } else {
        y
    };
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn hist(user_pred: &str, lo: &str, hi: &str) -> String {
    format!(
        "SELECT count(1) AS doc_count, date_trunc('hour', ts) AS key FROM ducklake.main.logs \
         WHERE ts {lo} AND ts {hi} AND {user_pred} GROUP BY key ORDER BY key DESC"
    )
}

const REWRITTEN: &str = "__ducklake_bucket";

/// The windows: aligned, cut on both sides, one-µs cuts, inclusive and
/// exclusive bounds, a window with no whole hour.
fn windows(tz: Option<&str>) -> Vec<(String, String, bool)> {
    let l = |us| ts_lit(us, tz);
    vec![
        (
            format!(">= {}", l(T0)),
            format!("<= {}", l(T0 + 10 * H)),
            true,
        ),
        (
            format!(">= {}", l(T0 + H / 3)),
            format!("<= {}", l(T0 + 7 * H + 17)),
            true,
        ),
        (
            format!("> {}", l(T0 + H - 1)),
            format!("< {}", l(T0 + 5 * H + 1)),
            true,
        ),
        (
            format!("> {}", l(T0 + H)),
            format!("< {}", l(T0 + 5 * H)),
            true,
        ),
        (
            format!(">= {}", l(T0 + 2 * H + 1)),
            format!("<= {}", l(T0 + 4 * H - 1)),
            true,
        ),
        (
            format!(">= {}", l(T0 - 3 * H)),
            format!("< {}", l(T0 + 30 * H)),
            true,
        ),
        // No whole hour: not rewritten.
        (
            format!(">= {}", l(T0 + H + 5)),
            format!("< {}", l(T0 + 2 * H)),
            false,
        ),
    ]
}

async fn assert_matrix(env: &Env, preds: &[(&str, bool)]) {
    let on = env.ctx(true).await;
    let off = env.ctx(false).await;
    for (lo, hi, whole) in windows(env.tz) {
        for (pred, has_rows) in preds {
            let sql = hist(pred, &lo, &hi);
            let (got, plan) = run(&on, &sql).await;
            let (want, base_plan) = run(&off, &sql).await;
            assert_eq!(got, want, "{sql}\n{plan}");
            assert!(!base_plan.contains(REWRITTEN));
            assert_eq!(plan.contains(REWRITTEN), whole, "{sql}\n{plan}");
            assert!(
                want.lines().count() > 4 || !whole || !has_rows,
                "empty answer for {sql}"
            );
        }
    }
}

const PREDS: &[(&str, bool)] = &[
    ("user_id = 'u0'", true),
    ("'u1' = user_id", true),
    ("user_id = 'u2'", true),
    ("user_id IN ('u0', 'u1')", true),
    ("user_id IN ('u0', 'u3')", true),
    ("user_id IN ('u0', 'u1', 'u2', 'u3')", true),
    ("user_id = 'nobody'", false),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counts_equal_raw_on_every_window() {
    let env = setup(None).await;
    populate(&env).await;
    env.record_counts().await;
    let counted = env.counted_users().await;
    assert_eq!(
        &counted[..4],
        &[vec!["u0", "u1", "u3"], vec!["u0", "u3"], vec!["u0", "u1", "u3"], vec!["u0", "u3"]],
    );
    assert!(
        counted[4..].iter().all(|users| users.is_empty()),
        "{counted:?}"
    );
    assert_matrix(&env, PREDS).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counts_equal_raw_with_utc_timestamps() {
    let env = setup(Some("UTC")).await;
    populate(&env).await;
    env.record_counts().await;
    assert_eq!(env.counted_users().await[0], vec!["u0", "u1", "u3"]);
    assert_matrix(&env, PREDS).await;
}

/// A deleted row, inlined rows and a stale or corrupt sidecar row are all
/// served by scanning, never by counts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deletes_inlined_rows_and_bad_rows_scan() {
    let env = setup(None).await;
    populate(&env).await;
    env.record_counts().await;
    // Inlined rows land in hours the counts cover.
    env.append_inlined(rows(&env, T0 + 2 * H + 11, 1_000_003, 300))
        .await;
    // Delete u0 rows from the first file (ids 0..9000 are in it).
    env.ctx(false)
        .await
        .sql("DELETE FROM ducklake.main.logs WHERE id IN (2, 4, 600, 8998)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // Wrong counts behind a stale size on the second file, behind a value
    // type mismatch on the fourth, and corrupt JSON on the third: any of them
    // being used would change the answer.
    let files = env.live_files().await;
    let pool = env.pool().await;
    for (file, set) in [
        (&files[1].0, "file_size_bytes = file_size_bytes + 1"),
        (&files[3].0, "value_type = 'int64'"),
    ] {
        inflate_counts(&pool, file).await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE ext_ducklake_file_index SET {set} WHERE path = ?"
        )))
        .bind(file)
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE ext_ducklake_file_index SET bucket_counts = '{\"v\":1' WHERE path = ?")
        .bind(&files[2].0)
        .execute(&pool)
        .await
        .unwrap();
    assert_matrix(&env, PREDS).await;
}

/// Other conjuncts, other aggregates and other grains are left alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn other_shapes_are_not_rewritten() {
    let env = setup(None).await;
    populate(&env).await;
    env.record_counts().await;
    let on = env.ctx(true).await;
    let off = env.ctx(false).await;
    let lo = ts_lit(T0, None);
    let hi = ts_lit(T0 + 5 * H, None);
    for sql in [
        format!(
            "SELECT count(1), date_trunc('hour', ts) k FROM ducklake.main.logs WHERE ts >= {lo} AND ts < {hi} AND user_id = 'u0' AND id > 5 GROUP BY k ORDER BY k"
        ),
        format!(
            "SELECT count(id), date_trunc('hour', ts) k FROM ducklake.main.logs WHERE ts >= {lo} AND ts < {hi} AND user_id = 'u0' GROUP BY k ORDER BY k"
        ),
        format!(
            "SELECT count(1), date_trunc('day', ts) k FROM ducklake.main.logs WHERE ts >= {lo} AND ts < {hi} AND user_id = 'u0' GROUP BY k ORDER BY k"
        ),
        format!(
            "SELECT count(1), date_trunc('hour', ts) k FROM ducklake.main.logs WHERE ts >= {lo} AND user_id = 'u0' GROUP BY k ORDER BY k"
        ),
        format!(
            "SELECT count(1), date_trunc('hour', ts) k FROM ducklake.main.logs WHERE ts >= {lo} AND ts < {hi} GROUP BY k ORDER BY k"
        ),
        format!(
            "SELECT count(1), date_trunc('hour', ts) k FROM ducklake.main.logs WHERE ts >= {lo} AND ts < {hi} AND (user_id = 'u0' OR id = 3) GROUP BY k ORDER BY k"
        ),
    ] {
        let (got, plan) = run(&on, &sql).await;
        let (want, _) = run(&off, &sql).await;
        assert_eq!(got, want, "{sql}");
        assert!(!plan.contains(REWRITTEN), "{sql}\n{plan}");
    }
    // The matched shape fires, and counts serve it (no count of the whole
    // window over counted files).
    let sql = hist("user_id = 'u0'", &format!(">= {lo}"), &format!("< {hi}"));
    let (_, plan) = run(&on, &sql).await;
    assert!(plan.contains(REWRITTEN), "{plan}");
}

/// The counts really serve the whole hours: inflating one recorded bucket
/// inside the window changes the answer by exactly that much, and only there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_counts_are_what_serves_whole_hours() {
    use sqlx::Row;
    let env = setup(None).await;
    populate(&env).await;
    env.record_counts().await;
    let files = env.live_files().await;
    let pool = env.pool().await;
    let json: String =
        sqlx::query("SELECT bucket_counts FROM ext_ducklake_file_index WHERE path = ?")
            .bind(&files[0].0)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
    let mut counts = bucket_counts_from_json(&json, IndexValueType::Utf8).unwrap();
    // keys[0] is u0.
    let bucket = &mut counts.keys[0].1[1];
    let inflated_hour = bucket.0;
    bucket.1 += 1000;
    sqlx::query("UPDATE ext_ducklake_file_index SET bucket_counts = ? WHERE path = ?")
        .bind(bucket_counts_to_json(&counts))
        .bind(&files[0].0)
        .execute(&pool)
        .await
        .unwrap();
    let sql = format!(
        "SELECT date_trunc('hour', ts) AS key, count(1) AS c FROM ducklake.main.logs \
         WHERE ts >= {} AND ts <= {} AND user_id = 'u0' GROUP BY key ORDER BY key",
        ts_lit(T0 - H, None),
        ts_lit(T0 + 12 * H, None)
    );
    let collect = |ctx: SessionContext| {
        let sql = sql.clone();
        async move {
            let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
            let mut out = BTreeMap::new();
            for b in batches {
                let k = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..b.num_rows() {
                    out.insert(k.value(i), c.value(i));
                }
            }
            out
        }
    };
    let got = collect(env.ctx(true).await).await;
    let mut want = collect(env.ctx(false).await).await;
    *want.get_mut(&inflated_hour).unwrap() += 1000;
    assert_eq!(got, want);
}

/// Adds 1000 to every recorded bucket of `path`.
async fn inflate_counts(pool: &sqlx::SqlitePool, path: &str) {
    use sqlx::Row;
    let json: String =
        sqlx::query("SELECT bucket_counts FROM ext_ducklake_file_index WHERE path = ?")
            .bind(path)
            .fetch_one(pool)
            .await
            .unwrap()
            .get(0);
    let mut counts = bucket_counts_from_json(&json, IndexValueType::Utf8).unwrap();
    for (_, buckets) in &mut counts.keys {
        for bucket in buckets {
            bucket.1 += 1000;
        }
    }
    sqlx::query("UPDATE ext_ducklake_file_index SET bucket_counts = ? WHERE path = ?")
        .bind(bucket_counts_to_json(&counts))
        .bind(path)
        .execute(pool)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Catalog reads per query (the counting provider lives in a duckdb-gated module)
// ---------------------------------------------------------------------------

mod catalog_reads {
    use super::*;
    use crate::counting::CountingProvider;

    /// Listing statements among `calls`.
    fn listings(calls: &BTreeMap<&'static str, usize>) -> usize {
        calls
            .iter()
            .filter(|(method, _)| method.starts_with("get_table_file_metadata_page"))
            .map(|(_, n)| n)
            .sum()
    }

    /// A context over one kept table, `logs`, read through a counting provider:
    /// the shape of a caller that caches built tables across queries.
    async fn kept(
        env: &Env,
        options: DuckLakeReadOptions,
        rule: bool,
    ) -> (SessionContext, CountingProvider) {
        let counting = CountingProvider::new(Arc::new(
            SqliteMetadataProvider::new(&env.url()).await.unwrap(),
        ));
        let catalog = DuckLakeCatalog::new(counting.clone())
            .unwrap()
            .with_read_options(options);
        let table = datafusion::catalog::CatalogProvider::schema(&catalog, "main")
            .unwrap()
            .table("logs")
            .await
            .unwrap()
            .unwrap();
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(3));
        if rule {
            ctx.add_optimizer_rule(Arc::new(BucketCountRule::default()));
        }
        ctx.register_table("logs", table).unwrap();
        counting.take();
        (ctx, counting)
    }

    fn kept_hist(user: &str) -> String {
        format!(
            "SELECT count(1) AS doc_count, date_trunc('hour', ts) AS key FROM logs \
             WHERE ts >= {} AND ts <= {} AND user_id = '{user}' GROUP BY key ORDER BY key DESC",
            ts_lit(T0 + H / 3, None),
            ts_lit(T0 + 7 * H + 17, None)
        )
    }

    const KEPT_HITS: &str = "SELECT ts, user_id, id FROM logs WHERE user_id = 'u1' ORDER BY ts DESC NULLS LAST, id LIMIT 50";

    /// The rewrite's resolution and its two branch scans share one listing: it
    /// lists the table no more often than the plain query does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_rewrite_lists_the_files_once() {
        let env = setup(None).await;
        populate(&env).await;
        env.record_counts().await;
        let options = env.counts_options(true);
        let (plain, plain_calls) = kept(&env, DuckLakeReadOptions::default(), false).await;
        let (rewritten, rewritten_calls) = kept(&env, options, true).await;
        let sql = kept_hist("u0");
        let (want, _) = run(&plain, &sql).await;
        let plain_listings = listings(&plain_calls.take());
        let (got, plan) = run(&rewritten, &sql).await;
        let rewritten_listings = listings(&rewritten_calls.take());
        assert!(plan.contains(REWRITTEN), "{plan}");
        assert_eq!(got, want);
        assert!(plain_listings > 0);
        // `run` plans the query twice (plan text, then collect).
        assert_eq!(rewritten_listings, plain_listings, "{plan}");
    }

    /// With the listing memo, a kept table lists its files on its first scan and
    /// never again, for the plain scan and the rewrite alike, and every answer
    /// equals a table without the memo.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_kept_table_with_the_listing_memo_lists_once() {
        let env = setup(None).await;
        populate(&env).await;
        env.record_counts().await;
        for counts in [false, true] {
            let base = env.counts_options(counts);
            let (memo, memo_calls) =
                kept(&env, base.clone().with_listing_memo(1 << 20), counts).await;
            let (plain, _) = kept(&env, base, counts).await;
            let queries = [kept_hist("u0"), kept_hist("u2"), KEPT_HITS.to_string()];
            for round in 0..3 {
                for sql in &queries {
                    let (got, _) = run(&memo, sql).await;
                    let (want, _) = run(&plain, sql).await;
                    assert_eq!(got, want, "counts={counts} round {round}: {sql}");
                }
                let listed = listings(&memo_calls.take());
                if round == 0 {
                    assert_eq!(listed, 1, "counts={counts}: one listing fills the memo");
                } else {
                    assert_eq!(
                        listed, 0,
                        "counts={counts} round {round}: served from the memo"
                    );
                }
            }
        }
    }

    /// A listing over the memo's budget is read on every scan, as without it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_listing_over_the_budget_is_read_every_time() {
        let env = setup(None).await;
        populate(&env).await;
        let (ctx, calls) = kept(
            &env,
            DuckLakeReadOptions::default().with_listing_memo(1),
            false,
        )
        .await;
        for _ in 0..3 {
            run(&ctx, KEPT_HITS).await;
            assert!(listings(&calls.take()) > 0);
        }
    }
}
