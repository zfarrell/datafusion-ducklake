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

/// Query helper: run SQL and return results as Vec<Vec<String>>.
async fn query_results(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let df = ctx.sql(sql).await.expect("SQL failed");
    let batches = df.collect().await.expect("collect failed");
    batches_to_strings(&batches)
}

use common::test_utils::batches_to_strings;

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

/// Wrapper for DuckDB operations on a DuckLake catalog.
struct DuckDbConn {
    conn: duckdb::Connection,
}

impl DuckDbConn {
    fn open_with_data_path(catalog_path: &Path, data_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", []).expect("load ducklake");
        let attach_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(
            &format!(
                "ATTACH '{}' AS ducklake (DATA_PATH '{}');",
                attach_path,
                data_path.display()
            ),
            [],
        )
        .expect("attach ducklake catalog with data path");
        DuckDbConn {
            conn,
        }
    }

    fn execute(&self, sql: &str) {
        self.conn
            .execute(sql, [])
            .unwrap_or_else(|e| panic!("DuckDB execute failed: {e}\nSQL: {sql}"));
    }

    fn query(&self, sql: &str) -> Vec<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(sql)
            .unwrap_or_else(|e| panic!("DuckDB prepare failed: {e}\nSQL: {sql}"));
        let mut rows = stmt.query([]).expect("DuckDB query failed");

        let mut results = Vec::new();
        while let Some(row) = rows.next().expect("DuckDB row iteration") {
            let mut vals = Vec::new();
            for i in 0.. {
                match row.get::<_, duckdb::types::Value>(i) {
                    Ok(v) => vals.push(duckdb_value_to_string(&v)),
                    Err(_) => break,
                }
            }
            results.push(vals);
        }
        results
    }
}

use common::test_utils::duckdb_value_to_string;

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
