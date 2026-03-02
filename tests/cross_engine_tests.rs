//! Cross-engine test infrastructure for DataFusion + DuckDB interoperability.
//!
//! This module provides helpers for three cross-engine test patterns:
//! 1. `df_write_df_read`: DataFusion writes → DataFusion reads
//! 2. `df_write_duckdb_read`: DataFusion writes → DuckDB reads and verifies
//! 3. `duckdb_write_df_read`: DuckDB writes → DataFusion reads and verifies
//!
//! Each pattern uses a shared DuckLake catalog (SQLite-backed) with local Parquet storage.
//!
//! Requires features: `write-sqlite`, `metadata-duckdb`, `metadata-sqlite`

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::test_utils::{arrow_value_to_string, batches_to_strings, duckdb_value_to_string};
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, DuckdbMetadataProvider, MetadataWriter,
    SqliteMetadataProvider, SqliteMetadataWriter,
};

// ==================== Setup helpers ====================

/// Context for a cross-engine test environment.
/// Holds all paths and temp resources needed.
struct CrossEngineEnv {
    /// Temp directory (kept alive for the test's lifetime)
    _temp_dir: TempDir,
    /// Path to the SQLite catalog file
    catalog_db_path: PathBuf,
    /// Path to the data directory (Parquet files)
    #[allow(dead_code)]
    data_path: PathBuf,
}

/// Creates a fresh DuckLake catalog backed by SQLite in a temp directory.
/// Returns the environment with paths to the catalog DB and data directory.
async fn setup_ducklake_catalog() -> CrossEngineEnv {
    let temp_dir = TempDir::new().expect("create temp dir");
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("create data dir");

    // Initialize the SQLite catalog
    let conn_str = format!("sqlite:{}?mode=rwc", catalog_db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .expect("init sqlite catalog");
    // data_path must end with "/" for DuckDB compatibility
    let data_path_str = format!("{}/", data_path.display());
    writer.set_data_path(&data_path_str).expect("set data path");

    CrossEngineEnv {
        _temp_dir: temp_dir,
        catalog_db_path,
        data_path,
    }
}

/// Opens a DuckLake catalog in DataFusion (read-only) using DuckdbMetadataProvider.
fn open_in_datafusion_duckdb(catalog_path: &Path) -> SessionContext {
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())
        .expect("create DuckdbMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Opens a DuckLake catalog in DataFusion (read-only) using SqliteMetadataProvider.
async fn open_in_datafusion_sqlite(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}", catalog_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str)
        .await
        .expect("create SqliteMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Opens a DuckLake catalog in DataFusion with write support (SQLite backend).
#[allow(dead_code)]
async fn open_in_datafusion_writable(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}?mode=rwc", catalog_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create SqliteMetadataWriter");
    let provider = SqliteMetadataProvider::new(&conn_str)
        .await
        .expect("create SqliteMetadataProvider");
    let catalog = DuckLakeCatalog::with_writer(Arc::new(provider), Arc::new(writer))
        .expect("create writable DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Wrapper for DuckDB operations on a DuckLake catalog.
struct DuckDbConn {
    conn: duckdb::Connection,
}

impl DuckDbConn {
    /// Open a DuckLake catalog in DuckDB using the SQLite backend.
    fn open(catalog_db_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", []).expect("load ducklake");
        let attach_path = format!("ducklake:sqlite:{}", catalog_db_path.display());
        conn.execute(&format!("ATTACH '{}' AS ducklake;", attach_path), [])
            .expect("attach ducklake catalog");
        DuckDbConn {
            conn,
        }
    }

    /// Open a DuckLake catalog in DuckDB using the native DuckDB backend (read-only re-attach).
    #[allow(dead_code)]
    fn open_native(catalog_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", []).expect("load ducklake");
        let attach_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(&format!("ATTACH '{}' AS ducklake;", attach_path), [])
            .expect("attach ducklake catalog");
        DuckDbConn {
            conn,
        }
    }

    /// Open/create a DuckLake catalog in DuckDB with a specified DATA_PATH.
    /// Used when DuckDB is the writer and needs to create the catalog from scratch.
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

    /// Execute a SQL statement (no results expected).
    fn execute(&self, sql: &str) {
        self.conn
            .execute(sql, [])
            .unwrap_or_else(|e| panic!("DuckDB execute failed: {e}\nSQL: {sql}"));
    }

    /// Query and return results as Vec<Vec<String>>.
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

// ==================== Query + comparison helpers ====================

/// Run a SQL query via DataFusion and return results as string rows.
async fn df_query(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let df = ctx.sql(sql).await.expect("DataFusion SQL failed");
    let batches = df.collect().await.expect("DataFusion collect failed");
    batches_to_strings(&batches)
}

/// Normalize a string value for comparison (handle float precision differences).
fn normalize_value(s: &str) -> String {
    if s == "NULL" {
        return s.to_string();
    }
    if let Ok(f) = s.parse::<f64>() {
        return format!("{:.6}", f);
    }
    s.to_string()
}

/// Assert two result sets are equal (after normalizing floats).
fn assert_query_eq(scenario: &str, expected: &[Vec<String>], actual: &[Vec<String>]) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "[{scenario}] Row count mismatch: expected {} rows, got {}.\n  Expected: {expected:?}\n  Actual:   {actual:?}",
        expected.len(),
        actual.len()
    );
    for (i, (exp_row, act_row)) in expected.iter().zip(actual.iter()).enumerate() {
        assert_eq!(
            exp_row.len(),
            act_row.len(),
            "[{scenario}] Column count mismatch at row {i}: expected {} cols, got {}.\n  Expected row: {exp_row:?}\n  Actual row:   {act_row:?}",
            exp_row.len(),
            act_row.len()
        );
        for (j, (exp_val, act_val)) in exp_row.iter().zip(act_row.iter()).enumerate() {
            let exp_norm = normalize_value(exp_val);
            let act_norm = normalize_value(act_val);
            assert_eq!(
                exp_norm, act_norm,
                "[{scenario}] Mismatch at row {i}, col {j}: expected '{exp_val}', got '{act_val}'"
            );
        }
    }
}

// ==================== Test Pattern 1: df_write_df_read ====================
// DataFusion writes data via DuckLakeTableWriter → DataFusion reads back

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_write_df_read() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Write using DuckLakeTableWriter
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Charlie"),
            ])),
            Arc::new(Float64Array::from(vec![Some(10.5), Some(20.0), Some(30.5)])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer
        .write_table("main", "test_table", &[batch])
        .await
        .unwrap();
    assert_eq!(result.records_written, 3);

    // Read back using DataFusion + SqliteMetadataProvider
    let ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
    let actual = df_query(
        &ctx,
        "SELECT id, name, value FROM ducklake.main.test_table ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "Alice".into(), "10.5".into()],
        vec!["2".into(), "Bob".into(), "20.0".into()],
        vec!["3".into(), "Charlie".into(), "30.5".into()],
    ];

    assert_query_eq("df_write_df_read", &expected, &actual);
}

// ==================== Test Pattern 2: df_write_duckdb_read ====================
// DataFusion writes data → DuckDB reads and verifies

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_write_duckdb_read() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Write using DuckLakeTableWriter
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec![Some("Xena"), Some("Yuri"), None])),
            Arc::new(Float64Array::from(vec![Some(95.5), Some(87.3), Some(92.1)])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer
        .write_table("main", "scores", &[batch])
        .await
        .unwrap();
    assert_eq!(result.records_written, 3);

    // Read using DuckDB
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, name, score FROM ducklake.main.scores ORDER BY id");

    assert_eq!(rows.len(), 3, "DuckDB should see 3 rows");
    assert_eq!(rows[0][0], "10");
    assert_eq!(rows[0][1], "Xena");
    assert_eq!(rows[1][0], "20");
    assert_eq!(rows[1][1], "Yuri");
    assert_eq!(rows[2][0], "30");
    assert_eq!(rows[2][1], "NULL");
}

// ==================== Test Pattern 3: duckdb_write_df_read ====================
// DuckDB writes data via native DuckLake → DataFusion reads via DuckdbMetadataProvider

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_write_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("duckdb_created.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB creates the catalog natively (not via our SQLite writer)
    {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();
        conn.execute(
            &format!(
                "ATTACH 'ducklake:{}' AS ducklake (DATA_PATH '{}');",
                catalog_path.display(),
                data_path.display()
            ),
            [],
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE ducklake.main.orders (id INT, product VARCHAR, amount DOUBLE)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO ducklake.main.orders VALUES \
             (1, 'Widget', 19.99), \
             (2, 'Gadget', 49.99), \
             (3, 'Doohickey', 9.99)",
            [],
        )
        .unwrap();
    }

    // Read using DataFusion + DuckdbMetadataProvider
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let actual = df_query(
        &ctx,
        "SELECT id, product, amount FROM ducklake.main.orders ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "Widget".into(), "19.99".into()],
        vec!["2".into(), "Gadget".into(), "49.99".into()],
        vec!["3".into(), "Doohickey".into(), "9.99".into()],
    ];

    assert_query_eq("duckdb_write_df_read", &expected, &actual);
}

// ==================== Validation: bidirectional roundtrip ====================
// DuckDB creates catalog → DataFusion reads → DuckDB adds more → DataFusion reads again.
// Uses native DuckDB DuckLake format for full bidirectional compatibility.

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_bidirectional_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("roundtrip.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // Step 1: DuckDB creates the catalog and initial data
    {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();
        conn.execute(
            &format!(
                "ATTACH 'ducklake:{}' AS ducklake (DATA_PATH '{}');",
                catalog_path.display(),
                data_path.display()
            ),
            [],
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE ducklake.main.scores (id INT, name VARCHAR, points BIGINT)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO ducklake.main.scores VALUES (1, 'Alice', 100), (2, 'Bob', 200)",
            [],
        )
        .unwrap();
    }

    // Step 2: DataFusion reads and verifies DuckDB-written data
    {
        let ctx = open_in_datafusion_duckdb(&catalog_path);
        let actual = df_query(
            &ctx,
            "SELECT id, name, points FROM ducklake.main.scores ORDER BY id",
        )
        .await;
        assert_eq!(actual.len(), 2, "DataFusion should see 2 rows");
        assert_eq!(actual[0], vec!["1", "Alice", "100"]);
        assert_eq!(actual[1], vec!["2", "Bob", "200"]);
    }

    // Step 3: DuckDB adds more data
    {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();
        conn.execute(
            &format!("ATTACH 'ducklake:{}' AS ducklake;", catalog_path.display()),
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO ducklake.main.scores VALUES (3, 'Charlie', 300)",
            [],
        )
        .unwrap();
    }

    // Step 4: DataFusion reads all data (fresh provider to pick up new snapshot)
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let actual = df_query(
        &ctx,
        "SELECT id, name, points FROM ducklake.main.scores ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "Alice".into(), "100".into()],
        vec!["2".into(), "Bob".into(), "200".into()],
        vec!["3".into(), "Charlie".into(), "300".into()],
    ];

    assert_query_eq("bidirectional_roundtrip", &expected, &actual);
}

// ==================== Validation: query result comparison ====================
// DuckDB writes → query both engines → compare results

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_assert_query_eq_both_engines() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("items.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB creates and populates a table using native DuckLake format
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute(
        "CREATE TABLE ducklake.main.items (id INT, name VARCHAR, price DOUBLE, active BOOLEAN)",
    );
    duckdb.execute(
        "INSERT INTO ducklake.main.items VALUES \
         (1, 'Laptop', 999.99, true), \
         (2, 'Mouse', 25.50, true), \
         (3, 'Cable', 5.99, false), \
         (4, NULL, NULL, NULL)",
    );

    // Query DuckDB for expected results
    let duckdb_results =
        duckdb.query("SELECT id, name, price, active FROM ducklake.main.items ORDER BY id");
    drop(duckdb);

    // Query DataFusion for actual results
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_results = df_query(
        &ctx,
        "SELECT id, name, price, active FROM ducklake.main.items ORDER BY id",
    )
    .await;

    // Compare
    assert_query_eq("both_engines_full_comparison", &duckdb_results, &df_results);
}

// ==================== Validation: NULL handling roundtrip ====================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_null_handling() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // DataFusion writes data with NULLs
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                None,
                Some("Charlie"),
            ])),
            Arc::new(Int64Array::from(vec![Some(100), Some(200), None])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "nulls_test", &[batch])
        .await
        .unwrap();

    // DuckDB verifies NULLs
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, name, value FROM ducklake.main.nulls_test ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "Alice", "100"]);
    assert_eq!(rows[1], vec!["2", "NULL", "200"]);
    assert_eq!(rows[2], vec!["3", "Charlie", "NULL"]);
    drop(duckdb);

    // DataFusion also verifies NULLs
    let ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(
        &ctx,
        "SELECT id, name, value FROM ducklake.main.nulls_test ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 3);
    assert_eq!(df_rows[1][1], "NULL");
    assert_eq!(df_rows[2][2], "NULL");
}

// ==================== Validation: count queries ====================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_count_query() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("count.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB writes data using native DuckLake format
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.nums (val INT)");
    duckdb.execute("INSERT INTO ducklake.main.nums VALUES (1), (2), (3), (4), (5)");

    let duckdb_count = duckdb.query("SELECT COUNT(*) FROM ducklake.main.nums");
    drop(duckdb);

    // DataFusion reads
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_count = df_query(&ctx, "SELECT COUNT(*) FROM ducklake.main.nums").await;

    assert_query_eq("count_query", &duckdb_count, &df_count);
}
