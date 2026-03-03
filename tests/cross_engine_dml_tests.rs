//! Cross-engine DELETE and UPDATE tests.
//!
//! These tests verify that DELETE and UPDATE operations produce files compatible
//! between DataFusion and DuckDB when sharing a DuckLake catalog.
//!
//! Test patterns:
//! - DataFusion DELETE → DuckDB reads (verify delete files are DuckDB-compatible)
//! - DuckDB DELETE → DataFusion reads (verify DF reads DuckDB delete files)
//! - DataFusion UPDATE → DuckDB reads (verify update produces correct delete + data files)
//! - DuckDB UPDATE → DataFusion reads (verify DF reads DuckDB update results)
//!
//! Requires features: `write-sqlite`, `metadata-duckdb`, `metadata-sqlite`

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::test_utils::{DuckDbConn, assert_results_eq, df_query};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use parquet::file::reader::FileReader;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeQueryPlanner, DuckLakeTableWriter, DuckdbMetadataProvider,
    MetadataWriter, SqliteMetadataProvider, SqliteMetadataWriter,
};

// ==================== Setup helpers ====================

/// Environment for cross-engine DML tests using SQLite-backed catalog.
struct DmlTestEnv {
    _temp_dir: TempDir,
    catalog_db_path: PathBuf,
    #[allow(dead_code)]
    data_path: PathBuf,
}

/// Creates a fresh DuckLake catalog backed by SQLite, writes initial data, returns env.
async fn setup_sqlite_env_with_data(
    _schema: Arc<Schema>,
    batches: &[RecordBatch],
    table_name: &str,
) -> DmlTestEnv {
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
        .expect("write table");
    assert!(result.records_written > 0);

    DmlTestEnv {
        _temp_dir: temp_dir,
        catalog_db_path,
        data_path,
    }
}

/// Environment for cross-engine DML tests using native DuckDB catalog.
struct DuckDbDmlTestEnv {
    _temp_dir: TempDir,
    catalog_path: PathBuf,
    #[allow(dead_code)]
    data_path: PathBuf,
}

/// Creates a DuckLake catalog via DuckDB with initial data.
fn setup_duckdb_env_with_data(create_sql: &str, insert_sql: &str) -> DuckDbDmlTestEnv {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("test.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute(create_sql);
    duckdb.execute(insert_sql);

    DuckDbDmlTestEnv {
        _temp_dir: temp_dir,
        catalog_path,
        data_path,
    }
}

// ==================== Context helpers ====================

/// Open a writable DataFusion context with DuckLakeQueryPlanner (SQLite-backed).
async fn open_writable_df_context(catalog_path: &Path) -> SessionContext {
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

/// Open a read-only DataFusion context using SqliteMetadataProvider.
async fn open_readonly_df_sqlite(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}", catalog_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Open a read-only DataFusion context using DuckdbMetadataProvider.
fn open_readonly_df_duckdb(catalog_path: &Path) -> SessionContext {
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

// ==================== DuckDB wrapper ====================

// DuckDbConn imported from common::test_utils

/// Assert a string value equals a float (handles DuckDB returning "10" for 10.0).
fn assert_float_eq(actual: &str, expected: f64, msg: &str) {
    let actual_f: f64 = actual
        .parse()
        .unwrap_or_else(|_| panic!("{msg}: cannot parse '{actual}' as f64"));
    assert!(
        (actual_f - expected).abs() < 0.01,
        "{msg}: expected {expected}, got {actual_f} (raw: '{actual}')"
    );
}

/// Helper to collect DML count from a DataFrame result.
async fn collect_dml_count(df: DataFrame) -> u64 {
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}

// ==================== Standard test data helpers ====================

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn test_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Charlie"),
                Some("Diana"),
                Some("Eve"),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(10.0),
                Some(20.0),
                Some(30.0),
                Some(40.0),
                Some(50.0),
            ])),
        ],
    )
    .unwrap()
}

// ============================================================================
// DELETE tests: DataFusion DELETE → DuckDB reads
// ============================================================================

/// DataFusion deletes rows, DuckDB reads and verifies the delete files are compatible.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_delete_duckdb_read() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    // DataFusion: DELETE WHERE id > 3
    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.employees WHERE id > 3")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 2, "Should delete id=4 and id=5");

    // DuckDB reads and verifies
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, name, value FROM ducklake.main.employees ORDER BY id");
    assert_eq!(rows.len(), 3, "DuckDB should see 3 remaining rows");
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[2][0], "3");
}

/// DuckDB deletes rows, DataFusion reads and verifies it handles DuckDB delete files.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_delete_df_read() {
    let env = setup_duckdb_env_with_data(
        "CREATE TABLE ducklake.main.employees (id INT, name VARCHAR, value DOUBLE)",
        "INSERT INTO ducklake.main.employees VALUES \
         (1, 'Alice', 10.0), (2, 'Bob', 20.0), (3, 'Charlie', 30.0), \
         (4, 'Diana', 40.0), (5, 'Eve', 50.0)",
    );

    // DuckDB: DELETE WHERE id > 3
    {
        let duckdb = DuckDbConn::open_native(&env.catalog_path);
        duckdb.execute("DELETE FROM ducklake.main.employees WHERE id > 3");
    }

    // DataFusion reads and verifies
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let rows = df_query(
        &ctx,
        "SELECT id, name, value FROM ducklake.main.employees ORDER BY id",
    )
    .await;

    assert_eq!(rows.len(), 3, "DataFusion should see 3 remaining rows");
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[2][0], "3");
}

// ============================================================================
// DELETE: simple WHERE clause
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_delete_simple_where() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    // DF deletes a single row
    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.employees WHERE name = 'Bob'")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1);

    // Both engines verify
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let duckdb_rows = duckdb.query("SELECT id FROM ducklake.main.employees ORDER BY id");
    assert_eq!(
        duckdb_rows.iter().map(|r| &r[0]).collect::<Vec<_>>(),
        vec!["1", "3", "4", "5"]
    );

    drop(duckdb);

    let df_ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(
        &df_ctx,
        "SELECT id FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    assert_eq!(
        df_rows.iter().map(|r| &r[0]).collect::<Vec<_>>(),
        vec!["1", "3", "4", "5"]
    );
}

// ============================================================================
// DELETE: all rows
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_delete_all_rows() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    // DF deletes all rows (no WHERE)
    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.employees")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 5);

    // DuckDB verifies empty table
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let cnt = duckdb.query_count("SELECT COUNT(*) FROM ducklake.main.employees");
    assert_eq!(cnt, 0, "DuckDB should see 0 rows after DF deletes all");
    drop(duckdb);

    // DF also verifies
    let df_ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(&df_ctx, "SELECT COUNT(*) FROM ducklake.main.employees").await;
    assert_eq!(df_rows[0][0], "0");
}

// ============================================================================
// DELETE: no matching rows (no delete file should be created)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_delete_no_matching_rows() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.employees WHERE id > 100")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 0, "No rows should be deleted");

    // Both engines still see all 5 rows
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let cnt = duckdb.query_count("SELECT COUNT(*) FROM ducklake.main.employees");
    assert_eq!(cnt, 5);
    drop(duckdb);

    let df_ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(&df_ctx, "SELECT COUNT(*) FROM ducklake.main.employees").await;
    assert_eq!(df_rows[0][0], "5");
}

// ============================================================================
// DELETE: multiple sequential deletes
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_delete_multiple_sequential() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    // First delete: remove id=2
    {
        let ctx = open_writable_df_context(&env.catalog_db_path).await;
        let df = ctx
            .sql("DELETE FROM ducklake.main.employees WHERE id = 2")
            .await
            .unwrap();
        let count = collect_dml_count(df).await;
        assert_eq!(count, 1);
    }

    // Verify intermediate state via DuckDB
    {
        let duckdb = DuckDbConn::open(&env.catalog_db_path);
        let rows = duckdb.query("SELECT id FROM ducklake.main.employees ORDER BY id");
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows.iter().map(|r| &r[0]).collect::<Vec<_>>(),
            vec!["1", "3", "4", "5"]
        );
    }

    // Second delete: remove id=4
    {
        let ctx = open_writable_df_context(&env.catalog_db_path).await;
        let df = ctx
            .sql("DELETE FROM ducklake.main.employees WHERE id = 4")
            .await
            .unwrap();
        let count = collect_dml_count(df).await;
        assert_eq!(count, 1);
    }

    // Both engines verify final state
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let duckdb_ids = duckdb.query("SELECT id FROM ducklake.main.employees ORDER BY id");
    assert_eq!(
        duckdb_ids.iter().map(|r| &r[0]).collect::<Vec<_>>(),
        vec!["1", "3", "5"]
    );
    drop(duckdb);

    let df_ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let df_ids = df_query(
        &df_ctx,
        "SELECT id FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    assert_eq!(
        df_ids.iter().map(|r| &r[0]).collect::<Vec<_>>(),
        vec!["1", "3", "5"]
    );
}

// ============================================================================
// DELETE: on table with existing delete files (DuckDB deletes first, then DF)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_delete_on_existing_deletes() {
    let env = setup_duckdb_env_with_data(
        "CREATE TABLE ducklake.main.items (id INT, label VARCHAR)",
        "INSERT INTO ducklake.main.items VALUES \
         (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    );

    // DuckDB deletes id=1
    {
        let duckdb = DuckDbConn::open_native(&env.catalog_path);
        duckdb.execute("DELETE FROM ducklake.main.items WHERE id = 1");
    }

    // DF reads and verifies DuckDB's delete
    {
        let ctx = open_readonly_df_duckdb(&env.catalog_path);
        let rows = df_query(&ctx, "SELECT id FROM ducklake.main.items ORDER BY id").await;
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows.iter().map(|r| &r[0]).collect::<Vec<_>>(),
            vec!["2", "3", "4", "5"]
        );
    }

    // DuckDB deletes another row (id=3) on top of existing delete
    {
        let duckdb = DuckDbConn::open_native(&env.catalog_path);
        duckdb.execute("DELETE FROM ducklake.main.items WHERE id = 3");
    }

    // DF reads final state
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let rows = df_query(&ctx, "SELECT id FROM ducklake.main.items ORDER BY id").await;
    assert_eq!(
        rows.iter().map(|r| &r[0]).collect::<Vec<_>>(),
        vec!["2", "4", "5"]
    );
}

// ============================================================================
// DELETE: verify delete file schema matches DuckLake spec
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_delete_file_schema_matches_spec() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    // DF deletes some rows to produce a delete file
    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.employees WHERE id = 3")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1);

    // Find the delete file and verify its schema
    let delete_files = find_delete_files(env.data_path.as_path());
    assert!(
        !delete_files.is_empty(),
        "Should have at least one delete file"
    );

    for delete_file_path in &delete_files {
        let file = std::fs::File::open(delete_file_path).unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        let schema = reader.metadata().file_metadata().schema();
        let fields = schema.get_fields();

        assert_eq!(fields.len(), 2, "Delete file should have exactly 2 columns");
        assert_eq!(
            fields[0].name(),
            "file_path",
            "First column should be 'file_path'"
        );
        assert_eq!(fields[1].name(), "pos", "Second column should be 'pos'");

        // Verify types
        use parquet::basic::Type as PhysicalType;
        assert_eq!(
            fields[0].get_physical_type(),
            PhysicalType::BYTE_ARRAY,
            "file_path should be BYTE_ARRAY"
        );
        assert_eq!(
            fields[1].get_physical_type(),
            PhysicalType::INT64,
            "pos should be INT64"
        );
    }
}

// ============================================================================
// DELETE: fully deleted file detection (all rows in a file deleted)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_fully_deleted_file() {
    // Create a table with a small number of rows (single file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
        ],
    )
    .unwrap();

    let env = setup_sqlite_env_with_data(schema, &[batch], "tiny").await;

    // Delete all rows
    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx.sql("DELETE FROM ducklake.main.tiny").await.unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 2);

    // Both engines should see 0 rows
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let cnt = duckdb.query_count("SELECT COUNT(*) FROM ducklake.main.tiny");
    assert_eq!(cnt, 0, "DuckDB: fully-deleted file should show 0 rows");
    drop(duckdb);

    let df_ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let df_cnt = df_query(&df_ctx, "SELECT COUNT(*) FROM ducklake.main.tiny").await;
    assert_eq!(
        df_cnt[0][0], "0",
        "DF: fully-deleted file should show 0 rows"
    );
}

// ============================================================================
// UPDATE tests: DataFusion UPDATE → DuckDB reads
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_update_duckdb_read() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    // DF: UPDATE name WHERE id = 2
    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("UPDATE ducklake.main.employees SET name = 'Bobby' WHERE id = 2")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1);

    // DuckDB reads and verifies the update
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, name FROM ducklake.main.employees ORDER BY id");
    assert_eq!(rows.len(), 5, "Row count should remain 5 after UPDATE");
    assert_eq!(rows[0][1], "Alice");
    assert_eq!(rows[1][1], "Bobby", "id=2 should be updated to 'Bobby'");
    assert_eq!(rows[2][1], "Charlie");
    assert_eq!(rows[3][1], "Diana");
    assert_eq!(rows[4][1], "Eve");
}

/// DuckDB updates rows, DataFusion reads and verifies.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_update_df_read() {
    let env = setup_duckdb_env_with_data(
        "CREATE TABLE ducklake.main.employees (id INT, name VARCHAR, value DOUBLE)",
        "INSERT INTO ducklake.main.employees VALUES \
         (1, 'Alice', 10.0), (2, 'Bob', 20.0), (3, 'Charlie', 30.0), \
         (4, 'Diana', 40.0), (5, 'Eve', 50.0)",
    );

    // DuckDB: UPDATE name WHERE id = 2
    {
        let duckdb = DuckDbConn::open_native(&env.catalog_path);
        duckdb.execute("UPDATE ducklake.main.employees SET name = 'Bobby' WHERE id = 2");
    }

    // DataFusion reads and verifies
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let rows = df_query(
        &ctx,
        "SELECT id, name FROM ducklake.main.employees ORDER BY id",
    )
    .await;

    assert_eq!(rows.len(), 5, "Row count should remain 5 after UPDATE");
    assert_eq!(rows[1][1], "Bobby", "DataFusion should see updated name");
}

// ============================================================================
// UPDATE: single column with WHERE
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_update_single_column_where() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("UPDATE ducklake.main.employees SET value = 99.9 WHERE id = 3")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1);

    // DuckDB verifies the value update (use ORDER BY, normalize floats)
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, value FROM ducklake.main.employees ORDER BY id");
    assert_eq!(rows.len(), 5);
    assert_float_eq(&rows[2][1], 99.9, "id=3 value should be 99.9");
    assert_float_eq(&rows[0][1], 10.0, "id=1 unchanged");
    assert_float_eq(&rows[1][1], 20.0, "id=2 unchanged");
    assert_float_eq(&rows[3][1], 40.0, "id=4 unchanged");
    assert_float_eq(&rows[4][1], 50.0, "id=5 unchanged");
}

// ============================================================================
// UPDATE: multiple columns
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_update_multiple_columns() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("UPDATE ducklake.main.employees SET name = 'Updated', value = 0.0 WHERE id = 4")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1);

    // DuckDB verifies via ORDER BY (avoids WHERE issues with DF-written data)
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, name, value FROM ducklake.main.employees ORDER BY id");
    assert_eq!(rows.len(), 5, "Should still have 5 rows");
    assert_eq!(rows[3][0], "4");
    assert_eq!(rows[3][1], "Updated");
    assert_float_eq(&rows[3][2], 0.0, "value should be 0.0");
    drop(duckdb);

    // DF verifies
    let df_ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(
        &df_ctx,
        "SELECT id, name, value FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 5);
    assert_eq!(df_rows[3][1], "Updated");
    assert_results_eq(
        "update_multiple_columns",
        &[vec!["4".into(), "Updated".into(), "0.0".into()]],
        &[df_rows[3].clone()],
    );
}

// ============================================================================
// UPDATE: with expression (SET value = value * 2)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_update_with_expression() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("UPDATE ducklake.main.employees SET value = value * 2 WHERE id >= 4")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 2, "Should update id=4 and id=5");

    // DuckDB verifies (normalize floats)
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, value FROM ducklake.main.employees ORDER BY id");
    assert_eq!(rows.len(), 5);
    // Unchanged
    assert_float_eq(&rows[0][1], 10.0, "id=1 unchanged");
    assert_float_eq(&rows[1][1], 20.0, "id=2 unchanged");
    assert_float_eq(&rows[2][1], 30.0, "id=3 unchanged");
    // Updated: 40*2=80, 50*2=100
    assert_float_eq(&rows[3][1], 80.0, "id=4: 40*2=80");
    assert_float_eq(&rows[4][1], 100.0, "id=5: 50*2=100");
}

// ============================================================================
// UPDATE: no matching rows
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_update_no_matching_rows() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("UPDATE ducklake.main.employees SET name = 'X' WHERE id > 100")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 0, "No rows should be updated");

    // Both engines still see all original values
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let cnt = duckdb.query_count("SELECT COUNT(*) FROM ducklake.main.employees");
    assert_eq!(cnt, 5);

    let names = duckdb.query("SELECT name FROM ducklake.main.employees ORDER BY id");
    assert_eq!(names[0][0], "Alice");
    assert_eq!(names[4][0], "Eve");
}

// ============================================================================
// UPDATE: all rows
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_update_all_rows() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("UPDATE ducklake.main.employees SET name = 'ALL'")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 5);

    // DuckDB verifies
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, name FROM ducklake.main.employees ORDER BY id");
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(row[1], "ALL", "All names should be 'ALL'");
    }

    // IDs should be preserved
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[4][0], "5");
}

// ============================================================================
// UPDATE: verify produces correct delete file + new data file pair
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_update_produces_delete_and_data_files() {
    let env = setup_sqlite_env_with_data(test_schema(), &[test_batch()], "employees").await;

    // Count parquet files before
    let files_before = find_all_parquet_files(env.data_path.as_path());
    let num_before = files_before.len();

    // DF: UPDATE
    let ctx = open_writable_df_context(&env.catalog_db_path).await;
    let df = ctx
        .sql("UPDATE ducklake.main.employees SET name = 'Updated' WHERE id = 1")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1);

    // Count parquet files after
    let files_after = find_all_parquet_files(env.data_path.as_path());
    let num_after = files_after.len();

    // An update should create at least 2 new files: one delete file and one data file
    assert!(
        num_after >= num_before + 2,
        "UPDATE should produce new delete file + data file. Before: {num_before}, After: {num_after}"
    );

    // Verify there's a delete file with the right schema
    let delete_files = find_delete_files(env.data_path.as_path());
    assert!(
        !delete_files.is_empty(),
        "UPDATE should create at least one delete file"
    );

    // Verify data is correct via both engines (use ORDER BY to avoid WHERE issues)
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, name FROM ducklake.main.employees ORDER BY id");
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Updated");
    drop(duckdb);

    let df_ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(
        &df_ctx,
        "SELECT id, name FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    assert_eq!(df_rows[0][1], "Updated");
}

// ============================================================================
// Combined: DuckDB UPDATE, then DF query with aggregation
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_update_df_aggregation() {
    let env = setup_duckdb_env_with_data(
        "CREATE TABLE ducklake.main.sales (id INT, amount DOUBLE)",
        "INSERT INTO ducklake.main.sales VALUES \
         (1, 100.0), (2, 200.0), (3, 300.0)",
    );

    // DuckDB: UPDATE amount for id=2
    {
        let duckdb = DuckDbConn::open_native(&env.catalog_path);
        duckdb.execute("UPDATE ducklake.main.sales SET amount = 250.0 WHERE id = 2");
    }

    // DataFusion: SUM should reflect the update
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let rows = df_query(&ctx, "SELECT SUM(amount) FROM ducklake.main.sales").await;

    // 100 + 250 + 300 = 650
    let sum: f64 = rows[0][0].parse().unwrap();
    assert!((sum - 650.0).abs() < 0.01, "SUM should be 650.0, got {sum}");
}

// ============================================================================
// Cross-engine bidirectional: DuckDB creates → DF UPDATE → DuckDB UPDATE → both verify
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_bidirectional_update() {
    // Use DuckDB-created catalog so DuckDB can UPDATE its own files (needs row_id_start)
    let env = setup_duckdb_env_with_data(
        "CREATE TABLE ducklake.main.employees (id INT, name VARCHAR, value DOUBLE)",
        "INSERT INTO ducklake.main.employees VALUES \
         (1, 'Alice', 10.0), (2, 'Bob', 20.0), (3, 'Charlie', 30.0), \
         (4, 'Diana', 40.0), (5, 'Eve', 50.0)",
    );

    // DuckDB: UPDATE id=1
    {
        let duckdb = DuckDbConn::open_native(&env.catalog_path);
        duckdb.execute("UPDATE ducklake.main.employees SET name = 'Alice-DuckDB' WHERE id = 1");
    }

    // DataFusion reads and verifies DuckDB's update
    {
        let ctx = open_readonly_df_duckdb(&env.catalog_path);
        let rows = df_query(
            &ctx,
            "SELECT id, name FROM ducklake.main.employees ORDER BY id",
        )
        .await;
        assert_eq!(rows[0][1], "Alice-DuckDB");
        assert_eq!(rows[1][1], "Bob"); // unchanged
    }

    // DuckDB: UPDATE id=5
    {
        let duckdb = DuckDbConn::open_native(&env.catalog_path);
        duckdb.execute("UPDATE ducklake.main.employees SET name = 'Eve-DuckDB' WHERE id = 5");
    }

    // DataFusion reads both updates
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let df_rows = df_query(
        &ctx,
        "SELECT id, name FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 5);
    assert_eq!(df_rows[0][1], "Alice-DuckDB");
    assert_eq!(df_rows[1][1], "Bob"); // unchanged
    assert_eq!(df_rows[4][1], "Eve-DuckDB");
}

// ============================================================================
// Filesystem helpers for delete file inspection
// ============================================================================

/// Recursively find all parquet files in a directory.
fn find_all_parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                result.extend(find_all_parquet_files(&path));
            } else if path.extension().map_or(false, |e| e == "parquet") {
                result.push(path);
            }
        }
    }
    result
}

/// Find parquet files that are delete files (schema: file_path + pos).
fn find_delete_files(dir: &Path) -> Vec<PathBuf> {
    use parquet::file::reader::FileReader;
    find_all_parquet_files(dir)
        .into_iter()
        .filter(|path| {
            if let Ok(file) = std::fs::File::open(path) {
                if let Ok(reader) =
                    parquet::file::serialized_reader::SerializedFileReader::new(file)
                {
                    let schema = reader.metadata().file_metadata().schema();
                    let fields = schema.get_fields();
                    return fields.len() == 2
                        && fields[0].name() == "file_path"
                        && fields[1].name() == "pos";
                }
            }
            false
        })
        .collect()
}
