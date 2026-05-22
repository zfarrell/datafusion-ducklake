//! MERGE INTO tests for DuckLake.
//!
//! Tests the MERGE execution plan that combines DELETE + INSERT/UPDATE
//! using the MOR (Merge-On-Read) pattern.
//!
//! Test patterns:
//! - MERGE with WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT (upsert)
//! - MERGE with WHEN MATCHED DELETE
//! - MERGE with only WHEN NOT MATCHED INSERT (insert-only merge)
//! - Cross-engine: DuckDB MERGE → DataFusion reads
//!
//! Requires features: `write-sqlite`, `metadata-duckdb`, `metadata-sqlite`

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeQueryPlanner, DuckLakeTableWriter, DuckdbMetadataProvider,
    MergeMatchedAction, MetadataWriter, SqliteMetadataProvider, SqliteMetadataWriter,
};

// ==================== Setup helpers ====================

struct MergeTestEnv {
    _temp_dir: TempDir,
    catalog_db_path: PathBuf,
    #[allow(dead_code)]
    data_path: PathBuf,
}

/// Create a fresh SQLite-backed DuckLake catalog with initial data.
async fn setup_merge_env(batches: &[RecordBatch], table_name: &str) -> MergeTestEnv {
    let temp_dir = TempDir::new().expect("create temp dir");
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("create data dir");

    let conn_str = format!("sqlite:{}?mode=rwc", catalog_db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .expect("init sqlite catalog");
    let data_path_str = format!("{}/", data_path.display());
    writer.set_data_path(&data_path_str).expect("set data path");

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer), object_store).expect("create table writer");
    let result = table_writer
        .write_table("main", table_name, batches)
        .await
        .expect("write initial data");
    assert!(result.records_written > 0);

    MergeTestEnv {
        _temp_dir: temp_dir,
        catalog_db_path,
        data_path,
    }
}

/// Open writable DataFusion context.
async fn open_writable_ctx(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}?mode=rwc", catalog_path.display());
    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(DuckLakeQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Open read-only DataFusion context via SQLite provider.
async fn open_readonly_ctx(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}", catalog_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

use common::test_utils::df_query as query_results;

/// Build the standard test schema: (id INT32, name VARCHAR, value INT32)
fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
    ]))
}

/// Build initial target data: [(1, Alice, 100), (2, Bob, 200), (3, Charlie, 300)]
fn target_data() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Charlie"),
            ])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
        ],
    )
    .unwrap()
}

/// Build source data for upsert: [(2, Bob, 250), (4, Dave, 400)]
/// - id=2 matches target (should UPDATE)
/// - id=4 doesn't match (should INSERT)
fn upsert_source_data() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2, 4])),
            Arc::new(StringArray::from(vec![Some("Bob"), Some("Dave")])),
            Arc::new(Int32Array::from(vec![250, 400])),
        ],
    )
    .unwrap()
}

// ==================== Tests ====================

/// Test MERGE with WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT (upsert pattern).
///
/// Target: [(1, Alice, 100), (2, Bob, 200), (3, Charlie, 300)]
/// Source: [(2, Bob, 250), (4, Dave, 400)]
/// ON: target.id = source.id
/// WHEN MATCHED: UPDATE SET name = source.name, value = source.value
/// WHEN NOT MATCHED: INSERT
///
/// Expected result: [(1, Alice, 100), (2, Bob, 250), (3, Charlie, 300), (4, Dave, 400)]
#[tokio::test(flavor = "multi_thread")]
async fn merge_upsert() {
    let target = target_data();
    let env = setup_merge_env(&[target], "employees").await;

    // Open writable context and get the table
    let ctx = open_writable_ctx(&env.catalog_db_path).await;
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema_provider = catalog.schema("main").unwrap();
    let table = schema_provider.table("employees").await.unwrap().unwrap();
    let ducklake_table = table
        .as_any()
        .downcast_ref::<datafusion_ducklake::DuckLakeTable>()
        .expect("should be DuckLakeTable");

    // Build source data
    let source = upsert_source_data();

    // Join key: target.id (col 0) = source.id (col 0)
    let join_keys = vec![(0usize, 0usize)];

    // WHEN MATCHED: UPDATE — replaces matched target rows with source row values
    let matched_action = Some(MergeMatchedAction::Update);

    // Execute merge
    let state = ctx.state();
    let plan = ducklake_table
        .merge(
            &state,
            vec![source.clone()],
            join_keys,
            matched_action,
            true,
        )
        .await
        .expect("merge should succeed");

    let task_ctx = Arc::new(datafusion::execution::TaskContext::default());
    let stream = plan.execute(0, task_ctx).unwrap();
    let results: Vec<RecordBatch> = futures::stream::TryStreamExt::try_collect(stream)
        .await
        .expect("merge execution failed");

    // Check the count of affected rows (1 updated + 1 inserted = 2)
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "should affect 2 rows (1 update + 1 insert)");

    // Read back and verify
    let read_ctx = open_readonly_ctx(&env.catalog_db_path).await;
    let mut actual = query_results(
        &read_ctx,
        "SELECT id, name, value FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    actual.sort();

    // After merge: Alice unchanged, Bob updated to 250, Charlie unchanged, Dave inserted
    let expected: Vec<Vec<String>> = vec![
        vec!["1".into(), "Alice".into(), "100".into()],
        vec!["2".into(), "Bob".into(), "250".into()],
        vec!["3".into(), "Charlie".into(), "300".into()],
        vec!["4".into(), "Dave".into(), "400".into()],
    ];

    assert_eq!(actual.len(), expected.len(), "row count mismatch");
    for (i, (exp, act)) in expected.iter().zip(actual.iter()).enumerate() {
        assert_eq!(exp, act, "row {i} mismatch: expected {exp:?}, got {act:?}");
    }
}

/// Test MERGE with WHEN MATCHED DELETE only (no INSERT for unmatched).
///
/// Target: [(1, Alice, 100), (2, Bob, 200), (3, Charlie, 300)]
/// Source: [(2, Bob, 0)]
/// ON: target.id = source.id
/// WHEN MATCHED: DELETE
///
/// Expected result: [(1, Alice, 100), (3, Charlie, 300)]
#[tokio::test(flavor = "multi_thread")]
async fn merge_matched_delete() {
    let target = target_data();
    let env = setup_merge_env(&[target], "employees").await;

    let ctx = open_writable_ctx(&env.catalog_db_path).await;
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema_provider = catalog.schema("main").unwrap();
    let table = schema_provider.table("employees").await.unwrap().unwrap();
    let ducklake_table = table
        .as_any()
        .downcast_ref::<datafusion_ducklake::DuckLakeTable>()
        .expect("should be DuckLakeTable");

    // Source has id=2 to match against target
    let source = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec![Some("Bob")])),
            Arc::new(Int32Array::from(vec![0])),
        ],
    )
    .unwrap();

    let join_keys = vec![(0usize, 0usize)];
    let matched_action = Some(MergeMatchedAction::Delete);

    let state = ctx.state();
    let plan = ducklake_table
        .merge(&state, vec![source], join_keys, matched_action, false)
        .await
        .expect("merge should succeed");

    let task_ctx = Arc::new(datafusion::execution::TaskContext::default());
    let stream = plan.execute(0, task_ctx).unwrap();
    let results: Vec<RecordBatch> = futures::stream::TryStreamExt::try_collect(stream)
        .await
        .expect("merge execution failed");

    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1, "should delete 1 matched row");

    // Read back and verify Bob is deleted
    let read_ctx = open_readonly_ctx(&env.catalog_db_path).await;
    let mut actual = query_results(
        &read_ctx,
        "SELECT id, name, value FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    actual.sort();

    let expected: Vec<Vec<String>> = vec![
        vec!["1".into(), "Alice".into(), "100".into()],
        vec!["3".into(), "Charlie".into(), "300".into()],
    ];

    assert_eq!(actual.len(), expected.len(), "row count mismatch");
    for (i, (exp, act)) in expected.iter().zip(actual.iter()).enumerate() {
        assert_eq!(exp, act, "row {i} mismatch");
    }
}

/// Test MERGE with only WHEN NOT MATCHED INSERT (insert-only, no matched action).
///
/// Target: [(1, Alice, 100), (2, Bob, 200)]
/// Source: [(2, Bob, 250), (3, Charlie, 300)]
/// ON: target.id = source.id
/// No WHEN MATCHED action
/// WHEN NOT MATCHED: INSERT
///
/// Expected result: [(1, Alice, 100), (2, Bob, 200), (3, Charlie, 300)]
/// id=2 in source is matched but no action taken, id=3 is inserted.
#[tokio::test(flavor = "multi_thread")]
async fn merge_insert_only() {
    let target = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    let env = setup_merge_env(&[target], "employees").await;

    let ctx = open_writable_ctx(&env.catalog_db_path).await;
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema_provider = catalog.schema("main").unwrap();
    let table = schema_provider.table("employees").await.unwrap().unwrap();
    let ducklake_table = table
        .as_any()
        .downcast_ref::<datafusion_ducklake::DuckLakeTable>()
        .expect("should be DuckLakeTable");

    let source = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec![Some("Bob"), Some("Charlie")])),
            Arc::new(Int32Array::from(vec![250, 300])),
        ],
    )
    .unwrap();

    let join_keys = vec![(0usize, 0usize)];
    // No matched action, only insert unmatched
    let matched_action: Option<MergeMatchedAction> = None;

    let state = ctx.state();
    let plan = ducklake_table
        .merge(&state, vec![source], join_keys, matched_action, true)
        .await
        .expect("merge should succeed");

    let task_ctx = Arc::new(datafusion::execution::TaskContext::default());
    let stream = plan.execute(0, task_ctx).unwrap();
    let results: Vec<RecordBatch> = futures::stream::TryStreamExt::try_collect(stream)
        .await
        .expect("merge execution failed");

    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1, "should insert 1 unmatched row");

    let read_ctx = open_readonly_ctx(&env.catalog_db_path).await;
    let mut actual = query_results(
        &read_ctx,
        "SELECT id, name, value FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    actual.sort();

    let expected: Vec<Vec<String>> = vec![
        vec!["1".into(), "Alice".into(), "100".into()],
        vec!["2".into(), "Bob".into(), "200".into()],
        vec!["3".into(), "Charlie".into(), "300".into()],
    ];

    assert_eq!(actual.len(), expected.len(), "row count mismatch");
    for (i, (exp, act)) in expected.iter().zip(actual.iter()).enumerate() {
        assert_eq!(exp, act, "row {i} mismatch");
    }
}

// ==================== Cross-engine tests ====================

use common::test_utils::DuckDbConn;

/// Cross-engine test: DuckDB MERGE → DataFusion reads.
///
/// DuckDB performs MERGE INTO on a DuckLake table, DataFusion reads the result.
/// This verifies that DuckLake's MERGE-generated delete files and new data files
/// are correctly read by DataFusion's MOR implementation.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_merge_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("test.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB creates and populates the catalog
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.target (id INTEGER, name VARCHAR, value INTEGER)");
    duckdb.execute(
        "INSERT INTO ducklake.main.target VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)",
    );

    // Create a source table for MERGE
    duckdb.execute("CREATE TABLE ducklake.main.source (id INTEGER, name VARCHAR, value INTEGER)");
    duckdb.execute("INSERT INTO ducklake.main.source VALUES (2, 'Bob', 250), (4, 'Dave', 400)");

    // Execute MERGE in DuckDB
    duckdb.execute(
        "MERGE INTO ducklake.main.target t \
         USING ducklake.main.source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET value = s.value \
         WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value)",
    );

    // Verify in DuckDB first
    let duckdb_results = duckdb.query("SELECT * FROM ducklake.main.target ORDER BY id");
    assert_eq!(duckdb_results.len(), 4);

    // Now read via DataFusion
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())
        .expect("create DuckdbMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create catalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let df_results = query_results(
        &ctx,
        "SELECT id, name, value FROM ducklake.main.target ORDER BY id",
    )
    .await;

    let expected: Vec<Vec<String>> = vec![
        vec!["1".into(), "Alice".into(), "100".into()],
        vec!["2".into(), "Bob".into(), "250".into()],
        vec!["3".into(), "Charlie".into(), "300".into()],
        vec!["4".into(), "Dave".into(), "400".into()],
    ];

    assert_eq!(
        df_results.len(),
        expected.len(),
        "Row count mismatch: DF got {:?}",
        df_results
    );
    for (i, (exp, act)) in expected.iter().zip(df_results.iter()).enumerate() {
        assert_eq!(exp, act, "row {i} mismatch after DuckDB MERGE");
    }
}

/// Cross-engine test: DuckDB MERGE with DELETE → DataFusion reads.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_merge_delete_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("test.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.target (id INTEGER, name VARCHAR, value INTEGER)");
    duckdb.execute(
        "INSERT INTO ducklake.main.target VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300)",
    );

    duckdb
        .execute("CREATE TABLE ducklake.main.to_delete (id INTEGER, name VARCHAR, value INTEGER)");
    duckdb.execute("INSERT INTO ducklake.main.to_delete VALUES (2, 'Bob', 0)");

    // MERGE with DELETE for matched rows
    duckdb.execute(
        "MERGE INTO ducklake.main.target t \
         USING ducklake.main.to_delete s ON t.id = s.id \
         WHEN MATCHED THEN DELETE",
    );

    // Read via DataFusion
    let provider =
        DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).expect("create provider");
    let catalog = DuckLakeCatalog::new(provider).expect("create catalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let df_results = query_results(
        &ctx,
        "SELECT id, name, value FROM ducklake.main.target ORDER BY id",
    )
    .await;

    let expected: Vec<Vec<String>> = vec![
        vec!["1".into(), "Alice".into(), "100".into()],
        vec!["3".into(), "Charlie".into(), "300".into()],
    ];

    assert_eq!(
        df_results.len(),
        expected.len(),
        "Row count mismatch: DF got {:?}",
        df_results
    );
    for (i, (exp, act)) in expected.iter().zip(df_results.iter()).enumerate() {
        assert_eq!(exp, act, "row {i} mismatch after DuckDB MERGE DELETE");
    }
}

// ==================== Acceptance criteria tests for #19 ====================

use datafusion::physical_plan::ExecutionPlan;

/// Execute a pre-built MERGE plan against `ctx`, draining the count column.
async fn run_merge_plan(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> datafusion::error::Result<u64> {
    use datafusion::execution::SendableRecordBatchStream;
    use futures::StreamExt;
    let task_ctx = ctx.task_ctx();
    let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx)?;
    let mut total = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);
        total += count;
    }
    Ok(total)
}

/// Build a MERGE plan from a writable context without executing it yet
/// (snapshot id captured at plan time).
async fn build_merge_plan(
    ctx: &SessionContext,
    table: &str,
    source: RecordBatch,
    join_keys: Vec<(usize, usize)>,
    matched_action: Option<MergeMatchedAction>,
    insert_unmatched: bool,
) -> Arc<dyn ExecutionPlan> {
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema = catalog.schema("main").unwrap();
    let table = schema.table(table).await.unwrap().unwrap();
    let ducklake_table = table
        .as_any()
        .downcast_ref::<datafusion_ducklake::DuckLakeTable>()
        .expect("should be DuckLakeTable");
    let state = ctx.state();
    ducklake_table
        .merge(&state, vec![source], join_keys, matched_action, insert_unmatched)
        .await
        .expect("merge plan")
}

/// Latest snapshot id in the catalog (sqlx path; mirrors helper in
/// `update_tests.rs`).
async fn current_snapshot_id(catalog_path: &Path) -> i64 {
    use sqlx::Row;
    let conn_str = format!("sqlite:{}", catalog_path.display());
    let pool = sqlx::sqlite::SqlitePool::connect(&conn_str).await.unwrap();
    let row = sqlx::query("SELECT COALESCE(MAX(snapshot_id), -1) FROM ducklake_snapshot")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    row.try_get(0).unwrap()
}

/// Walk the test data directory and return every `*.parquet` file.
fn list_parquet_files_on_disk(data_path: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p
                    .extension()
                    .and_then(|x| x.to_str())
                    .is_some_and(|x| x == "parquet")
                {
                    out.push(p);
                }
            }
        }
    }
    walk(data_path, &mut out);
    out
}

/// R11-S-003: when two source rows match the same target row, MERGE must
/// reject the statement with a SQL-standard violation error.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_multi_match_error() {
    let target = target_data();
    let env = setup_merge_env(&[target], "employees").await;
    let ctx = open_writable_ctx(&env.catalog_db_path).await;

    // Source: two rows that both have id=2 (UNION ALL of the same row).
    let dup_source = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2, 2])),
            Arc::new(StringArray::from(vec![Some("Bob1"), Some("Bob2")])),
            Arc::new(Int32Array::from(vec![250, 260])),
        ],
    )
    .unwrap();

    let before_disk = list_parquet_files_on_disk(&env.data_path).len();
    let before_snap = current_snapshot_id(&env.catalog_db_path).await;

    let plan = build_merge_plan(
        &ctx,
        "employees",
        dup_source,
        vec![(0, 0)],
        Some(MergeMatchedAction::Update),
        false,
    )
    .await;
    let err = run_merge_plan(&ctx, plan)
        .await
        .expect_err("MERGE with multi-match source must error");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("merge")
            && (msg.contains("multiple source rows")
                || msg.contains("multi")
                || msg.contains("matched the same target")),
        "expected a multi-match violation error, got: {err}"
    );

    // No files written, no snapshot advance.
    let mut after_disk = list_parquet_files_on_disk(&env.data_path).len();
    for _ in 0..50 {
        if after_disk == before_disk {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        after_disk = list_parquet_files_on_disk(&env.data_path).len();
    }
    assert_eq!(
        after_disk, before_disk,
        "failed MERGE must not leave orphan files"
    );
    assert_eq!(
        current_snapshot_id(&env.catalog_db_path).await,
        before_snap,
        "failed MERGE must not advance snapshot"
    );

    // Data still reflects the original three rows untouched.
    let read_ctx = open_readonly_ctx(&env.catalog_db_path).await;
    let mut actual = query_results(
        &read_ctx,
        "SELECT id, name, value FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    actual.sort();
    assert_eq!(actual.len(), 3);
}

/// Empty source: MERGE must be a no-op with no spurious snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_empty_source_is_noop() {
    let target = target_data();
    let env = setup_merge_env(&[target], "employees").await;
    let ctx = open_writable_ctx(&env.catalog_db_path).await;

    let empty_source = RecordBatch::new_empty(test_schema());

    let before_disk = list_parquet_files_on_disk(&env.data_path).len();
    let before_snap = current_snapshot_id(&env.catalog_db_path).await;

    let plan = build_merge_plan(
        &ctx,
        "employees",
        empty_source,
        vec![(0, 0)],
        Some(MergeMatchedAction::Update),
        true,
    )
    .await;
    let count = run_merge_plan(&ctx, plan).await.expect("MERGE empty source");
    assert_eq!(count, 0, "empty source must affect zero rows");

    assert_eq!(
        current_snapshot_id(&env.catalog_db_path).await,
        before_snap,
        "no-op MERGE must not advance snapshot"
    );

    let mut after_disk = list_parquet_files_on_disk(&env.data_path).len();
    for _ in 0..50 {
        if after_disk == before_disk {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        after_disk = list_parquet_files_on_disk(&env.data_path).len();
    }
    assert_eq!(
        after_disk, before_disk,
        "no-op MERGE must not write any files"
    );
}

/// MERGE on an empty target with NOT MATCHED INSERT only is equivalent to a
/// bulk INSERT.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_empty_target_insert_only() {
    // Set up the catalog manually so we can produce a table with no data
    // file at all (target_data() always writes one). Workaround: write a
    // single row then immediately delete it so the table has zero live rows.
    let initial = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![999])),
            Arc::new(StringArray::from(vec![Some("seed")])),
            Arc::new(Int32Array::from(vec![0])),
        ],
    )
    .unwrap();
    let env = setup_merge_env(&[initial], "employees").await;
    let ctx = open_writable_ctx(&env.catalog_db_path).await;

    // Delete the seed row via a MERGE … WHEN MATCHED DELETE.
    let delete_src = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![999])),
            Arc::new(StringArray::from(vec![Some("x")])),
            Arc::new(Int32Array::from(vec![0])),
        ],
    )
    .unwrap();
    let plan = build_merge_plan(
        &ctx,
        "employees",
        delete_src,
        vec![(0, 0)],
        Some(MergeMatchedAction::Delete),
        false,
    )
    .await;
    let cnt = run_merge_plan(&ctx, plan).await.unwrap();
    assert_eq!(cnt, 1);

    // Now MERGE-INSERT three new rows into the (logically) empty target.
    let ctx2 = open_writable_ctx(&env.catalog_db_path).await;
    let insert_src = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("C")])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let plan = build_merge_plan(
        &ctx2,
        "employees",
        insert_src,
        vec![(0, 0)],
        None,
        true,
    )
    .await;
    let inserted = run_merge_plan(&ctx2, plan).await.unwrap();
    assert_eq!(inserted, 3, "MERGE NOT MATCHED INSERT into empty target must insert all source rows");

    let read_ctx = open_readonly_ctx(&env.catalog_db_path).await;
    let mut actual = query_results(
        &read_ctx,
        "SELECT id, name, value FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    actual.sort();
    let expected: Vec<Vec<String>> = vec![
        vec!["1".into(), "A".into(), "10".into()],
        vec!["2".into(), "B".into(), "20".into()],
        vec!["3".into(), "C".into(), "30".into()],
    ];
    assert_eq!(actual, expected);
}

/// Concurrent MERGEs that target the same data file conflict — first wins,
/// second fails with a transaction-conflict error and its uploads are cleaned
/// up. Mirrors the path #17/#18 added for DELETE/UPDATE.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_merge_overlapping_conflicts() {
    let target = target_data();
    let env = setup_merge_env(&[target], "employees").await;

    let ctx_a = open_writable_ctx(&env.catalog_db_path).await;
    let ctx_b = open_writable_ctx(&env.catalog_db_path).await;

    let src_a = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec![Some("BobA")])),
            Arc::new(Int32Array::from(vec![201])),
        ],
    )
    .unwrap();
    let src_b = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec![Some("BobB")])),
            Arc::new(Int32Array::from(vec![202])),
        ],
    )
    .unwrap();

    let plan_a = build_merge_plan(
        &ctx_a,
        "employees",
        src_a,
        vec![(0, 0)],
        Some(MergeMatchedAction::Update),
        false,
    )
    .await;
    let plan_b = build_merge_plan(
        &ctx_b,
        "employees",
        src_b,
        vec![(0, 0)],
        Some(MergeMatchedAction::Update),
        false,
    )
    .await;

    let before_disk = list_parquet_files_on_disk(&env.data_path).len();

    let a_count = run_merge_plan(&ctx_a, plan_a).await.expect("A wins");
    assert_eq!(a_count, 1);

    let b_err = run_merge_plan(&ctx_b, plan_b)
        .await
        .expect_err("B must conflict");
    assert!(
        b_err.to_string().to_lowercase().contains("conflict"),
        "expected a transaction-conflict, got: {b_err}"
    );

    // A's commit added one delete + one data file. B's orphan uploads must be
    // cleaned up.
    let expected = before_disk + 2;
    let mut after_disk = list_parquet_files_on_disk(&env.data_path).len();
    for _ in 0..50 {
        if after_disk == expected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        after_disk = list_parquet_files_on_disk(&env.data_path).len();
    }
    assert_eq!(
        after_disk, expected,
        "conflict failure must not leave orphan files (before={before_disk}, after={after_disk})"
    );

    let read_ctx = open_readonly_ctx(&env.catalog_db_path).await;
    let mut actual = query_results(
        &read_ctx,
        "SELECT id, name, value FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    actual.sort();
    // Only A's commit visible: Bob -> BobA, value 201.
    let names: Vec<&str> = actual.iter().map(|r| r[1].as_str()).collect();
    assert_eq!(names, vec!["Alice", "BobA", "Charlie"]);
}

/// Writable context with an explicit `max_buffered_rows_per_dml` cap. Used to
/// exercise the safety valve in MERGE the same way `update_tests` does.
async fn open_writable_ctx_with_max_buffered_rows(
    catalog_path: &Path,
    cap: usize,
) -> SessionContext {
    use datafusion::common::config::ConfigOptions;
    use datafusion::execution::config::SessionConfig;
    use datafusion_ducklake::config::DuckLakeConfig;

    let conn_str = format!("sqlite:{}?mode=rwc", catalog_path.display());
    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let mut options = ConfigOptions::default();
    options
        .extensions
        .insert(DuckLakeConfig { max_buffered_rows_per_dml: cap });
    let session_config = SessionConfig::from(options);

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(session_config)
        .with_query_planner(Arc::new(DuckLakeQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// The `ducklake.max_buffered_rows_per_dml` knob bounds MERGE's buffered
/// matched + unmatched-insert row set. Dial it low and merge more rows than
/// fit: the exec must error with `ResourcesExhausted` and leave no orphans.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_max_buffered_rows_boundary_errors() {
    // 1000-row target keyed 0..1000.
    let target = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from((0i32..1000).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..1000).map(|_| Some("t")).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from((0i32..1000).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let env = setup_merge_env(&[target], "big").await;

    let ctx = open_writable_ctx_with_max_buffered_rows(&env.catalog_db_path, 100).await;

    // Source matches all 1000 target rows for UPDATE.
    let source = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from((0i32..1000).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..1000).map(|_| Some("s")).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from((0i32..1000).collect::<Vec<_>>())),
        ],
    )
    .unwrap();

    let before_disk = list_parquet_files_on_disk(&env.data_path).len();

    let plan = build_merge_plan(
        &ctx,
        "big",
        source,
        vec![(0, 0)],
        Some(MergeMatchedAction::Update),
        false,
    )
    .await;
    let err = run_merge_plan(&ctx, plan)
        .await
        .expect_err("MERGE exceeding the cap must error");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("too many rows")
            || msg.contains("resourcesexhausted")
            || msg.contains("buffered"),
        "expected a ResourcesExhausted-style error, got: {err}"
    );

    let mut after_disk = list_parquet_files_on_disk(&env.data_path).len();
    for _ in 0..50 {
        if after_disk == before_disk {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        after_disk = list_parquet_files_on_disk(&env.data_path).len();
    }
    assert_eq!(
        after_disk, before_disk,
        "max_buffered_rows trip must not leave orphan files"
    );
}

/// NOT NULL pre-write validation: MERGE that would write a NULL into a NOT
/// NULL column must error BEFORE any file is uploaded, leaving no orphan
/// parquet files and not advancing the snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_not_null_pre_write_validation() {
    // Use a tighter schema where `name` is NOT NULL to guarantee the trip.
    let strict_schema: Arc<Schema> = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false), // NOT NULL
        Field::new("value", DataType::Int32, true),
    ]));
    let target = RecordBatch::try_new(
        strict_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let env = setup_merge_env(&[target], "strict").await;
    let ctx = open_writable_ctx(&env.catalog_db_path).await;

    // Source row has NULL `name` — schema declares it nullable so the source
    // batch is constructible, but the table's column is NOT NULL.
    let source_schema: Arc<Schema> = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
    ]));
    let source = RecordBatch::try_new(
        source_schema,
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int32Array::from(vec![Some(30)])),
        ],
    )
    .unwrap();

    let before_disk = list_parquet_files_on_disk(&env.data_path).len();
    let before_snap = current_snapshot_id(&env.catalog_db_path).await;

    let plan = build_merge_plan(
        &ctx,
        "strict",
        source,
        vec![(0, 0)],
        None, // matched_action irrelevant — id=3 unmatched
        true, // INSERT unmatched
    )
    .await;
    let err = run_merge_plan(&ctx, plan)
        .await
        .expect_err("MERGE INSERT with NULL into NOT NULL must error");

    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not null")
            || msg.contains("not_null")
            || msg.contains("non-nullable")
            || msg.contains("constraint")
            || msg.contains("contains null values"),
        "expected NOT NULL / nullability error, got: {err}"
    );

    let mut after_disk = list_parquet_files_on_disk(&env.data_path).len();
    for _ in 0..50 {
        if after_disk == before_disk {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        after_disk = list_parquet_files_on_disk(&env.data_path).len();
    }
    assert_eq!(
        after_disk, before_disk,
        "NOT NULL pre-write validation must not leave orphans"
    );
    assert_eq!(
        current_snapshot_id(&env.catalog_db_path).await,
        before_snap,
        "failed MERGE must not advance snapshot"
    );

    // Read-back: data unchanged.
    let read_ctx = open_readonly_ctx(&env.catalog_db_path).await;
    let actual = query_results(
        &read_ctx,
        "SELECT id, name, value FROM ducklake.main.strict ORDER BY id",
    )
    .await;
    assert_eq!(actual.len(), 2);
}

// The unit-level mixed-signedness collation test lives at
// `src/merge_exec.rs::test_hash_key_signed_unsigned_disjoint`. It exercises
// the hash extractor directly because the public MERGE entry point requires
// source and target schemas to align — a target column declared `Int64`
// will reject a source batch whose corresponding column is `UInt64` at the
// `write_and_upload_parquet` stage with an Arrow schema error long before
// the hash extractor is reached. That schema-alignment requirement is the
// planner's responsibility (the planner inserts a CAST), so a code path
// delivering mismatched-signedness arrays into the MERGE exec is currently
// unreachable. The unit test pins the hash-extractor behavior in case a
// future code path makes it reachable.
