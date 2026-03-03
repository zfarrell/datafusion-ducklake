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
//!
//! TODO(R5-S-067): These tests only use SQLite backend. Add PG/MySQL cross-engine
//! tests when Docker-based test infrastructure is available.

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::test_utils::{assert_results_eq, df_query};
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

use common::test_utils::DuckDbConn;

// ==================== Query + comparison helpers ====================

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

    assert_results_eq("df_write_df_read", &expected, &actual);
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
    // R5-S-077: Verify float score column values
    assert_eq!(rows[0][2], "95.5");
    assert_eq!(rows[1][2], "87.3");
    assert_eq!(rows[2][2], "92.1");
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

    assert_results_eq("duckdb_write_df_read", &expected, &actual);
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

    assert_results_eq("bidirectional_roundtrip", &expected, &actual);
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
    assert_results_eq("both_engines_full_comparison", &duckdb_results, &df_results);
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
    // R5-S-081: Verify all rows completely, not just 2 cells
    assert_eq!(df_rows.len(), 3);
    assert_eq!(df_rows[0], vec!["1", "Alice", "100"]);
    assert_eq!(df_rows[1], vec!["2", "NULL", "200"]);
    assert_eq!(df_rows[2], vec!["3", "Charlie", "NULL"]);
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

    assert_results_eq("count_query", &duckdb_count, &df_count);
}

// ==================== DML: DELETE (DuckDB → DF) ====================
// DuckDB creates table, inserts rows, deletes some → DataFusion reads remaining rows

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_delete_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("delete.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB: create, insert, then delete some rows
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.employees (id INT, name VARCHAR, dept VARCHAR)");
    duckdb.execute(
        "INSERT INTO ducklake.main.employees VALUES \
         (1, 'Alice', 'Engineering'), \
         (2, 'Bob', 'Marketing'), \
         (3, 'Charlie', 'Engineering'), \
         (4, 'Diana', 'Sales'), \
         (5, 'Eve', 'Marketing')",
    );
    // Delete all Marketing employees (rows 2 and 5)
    duckdb.execute("DELETE FROM ducklake.main.employees WHERE dept = 'Marketing'");

    // Verify DuckDB sees correct remaining rows
    let duckdb_rows =
        duckdb.query("SELECT id, name, dept FROM ducklake.main.employees ORDER BY id");
    drop(duckdb);

    // Verify DataFusion sees same results
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_rows = df_query(
        &ctx,
        "SELECT id, name, dept FROM ducklake.main.employees ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "Alice".into(), "Engineering".into()],
        vec!["3".into(), "Charlie".into(), "Engineering".into()],
        vec!["4".into(), "Diana".into(), "Sales".into()],
    ];

    assert_results_eq("duckdb_delete_duckdb_verify", &expected, &duckdb_rows);
    assert_results_eq("duckdb_delete_df_verify", &expected, &df_rows);
}

// ==================== DML: DELETE multiple rounds (DuckDB → DF) ====================
// Tests multiple DELETE operations creating multiple delete files

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_multiple_deletes_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("multi_delete.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.items (id INT, val VARCHAR)");
    duckdb.execute(
        "INSERT INTO ducklake.main.items VALUES \
         (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    );

    // First delete
    duckdb.execute("DELETE FROM ducklake.main.items WHERE id = 2");
    // Second delete (different snapshot)
    duckdb.execute("DELETE FROM ducklake.main.items WHERE id = 4");
    // Third delete
    duckdb.execute("DELETE FROM ducklake.main.items WHERE id IN (1, 5)");

    let duckdb_rows = duckdb.query("SELECT id, val FROM ducklake.main.items ORDER BY id");
    drop(duckdb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_rows = df_query(&ctx, "SELECT id, val FROM ducklake.main.items ORDER BY id").await;

    let expected = vec![vec!["3".into(), "c".into()]];

    assert_results_eq("multi_delete_duckdb", &expected, &duckdb_rows);
    assert_results_eq("multi_delete_df", &expected, &df_rows);
}

// ==================== DML: UPDATE (DuckDB → DF) ====================
// DuckDB updates rows → DataFusion reads correct updated values

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_update_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("update.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB: create, insert, then update some rows
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.products (id INT, name VARCHAR, price DOUBLE)");
    duckdb.execute(
        "INSERT INTO ducklake.main.products VALUES \
         (1, 'Laptop', 999.99), \
         (2, 'Mouse', 25.50), \
         (3, 'Keyboard', 75.00)",
    );

    // Update single column
    duckdb.execute("UPDATE ducklake.main.products SET price = 899.99 WHERE id = 1");
    // Update multiple columns
    duckdb.execute(
        "UPDATE ducklake.main.products SET name = 'Wireless Mouse', price = 35.00 WHERE id = 2",
    );

    // Verify DuckDB
    let duckdb_rows =
        duckdb.query("SELECT id, name, price FROM ducklake.main.products ORDER BY id");
    drop(duckdb);

    // Verify DataFusion
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_rows = df_query(
        &ctx,
        "SELECT id, name, price FROM ducklake.main.products ORDER BY id",
    )
    .await;

    // Verify row counts match
    assert_eq!(duckdb_rows.len(), 3, "DuckDB should see 3 rows");
    assert_eq!(df_rows.len(), 3, "DataFusion should see 3 rows");

    // Verify names match (string columns should be identical)
    assert_eq!(df_rows[0][1], "Laptop");
    assert_eq!(df_rows[1][1], "Wireless Mouse");
    assert_eq!(df_rows[2][1], "Keyboard");
    assert_eq!(duckdb_rows[0][1], df_rows[0][1]);
    assert_eq!(duckdb_rows[1][1], df_rows[1][1]);
    assert_eq!(duckdb_rows[2][1], df_rows[2][1]);

    // Verify prices semantically (float formatting may differ across engines)
    let df_prices: Vec<f64> = df_rows.iter().map(|r| r[2].parse().unwrap()).collect();
    let duckdb_prices: Vec<f64> = duckdb_rows.iter().map(|r| r[2].parse().unwrap()).collect();
    assert_eq!(df_prices, duckdb_prices);
    assert!((df_prices[0] - 899.99).abs() < 0.001);
    assert!((df_prices[1] - 35.00).abs() < 0.001);
    assert!((df_prices[2] - 75.00).abs() < 0.001);
}

// ==================== DML: MERGE (DuckDB → DF) ====================
// DuckDB merges source data into target → DataFusion reads merged result

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_merge_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("merge.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB: create target table and populate
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.inventory (id INT, item VARCHAR, qty INT)");
    duckdb.execute(
        "INSERT INTO ducklake.main.inventory VALUES \
         (1, 'Widget', 100), \
         (2, 'Gadget', 200), \
         (3, 'Doohickey', 50)",
    );

    // MERGE: update existing row id=2, insert new row id=4
    duckdb.execute(
        "MERGE INTO ducklake.main.inventory AS t \
         USING (VALUES (2, 'Gadget Pro', 250), (4, 'Thingamajig', 75)) AS s(id, item, qty) \
         ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET item = s.item, qty = s.qty \
         WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.item, s.qty)",
    );

    // Verify DuckDB
    let duckdb_rows = duckdb.query("SELECT id, item, qty FROM ducklake.main.inventory ORDER BY id");
    drop(duckdb);

    // Verify DataFusion
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_rows = df_query(
        &ctx,
        "SELECT id, item, qty FROM ducklake.main.inventory ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "Widget".into(), "100".into()],
        vec!["2".into(), "Gadget Pro".into(), "250".into()],
        vec!["3".into(), "Doohickey".into(), "50".into()],
        vec!["4".into(), "Thingamajig".into(), "75".into()],
    ];

    assert_results_eq("duckdb_merge_duckdb_verify", &expected, &duckdb_rows);
    assert_results_eq("duckdb_merge_df_verify", &expected, &df_rows);
}

// ==================== ALTER TABLE ADD COLUMN (DuckDB → DF) ====================
// DuckDB adds column → DataFusion sees new column with NULLs for existing rows

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_alter_add_column_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("alter_add.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB: create table, insert data, then add a new column
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.users (id INT, name VARCHAR)");
    duckdb.execute("INSERT INTO ducklake.main.users VALUES (1, 'Alice'), (2, 'Bob')");

    // Add new column — existing rows should have NULL for this column
    duckdb.execute("ALTER TABLE ducklake.main.users ADD COLUMN email VARCHAR");

    // Insert a new row with the email column populated
    duckdb.execute("INSERT INTO ducklake.main.users VALUES (3, 'Charlie', 'charlie@example.com')");

    // Verify DuckDB
    let duckdb_rows = duckdb.query("SELECT id, name, email FROM ducklake.main.users ORDER BY id");
    drop(duckdb);

    // Verify DataFusion sees new column with NULLs for old rows
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_rows = df_query(
        &ctx,
        "SELECT id, name, email FROM ducklake.main.users ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "Alice".into(), "NULL".into()],
        vec!["2".into(), "Bob".into(), "NULL".into()],
        vec!["3".into(), "Charlie".into(), "charlie@example.com".into()],
    ];

    assert_results_eq("alter_add_column_duckdb_verify", &expected, &duckdb_rows);
    assert_results_eq("alter_add_column_df_verify", &expected, &df_rows);
}

// ==================== ALTER TABLE DROP COLUMN (DuckDB → DF) ====================
// DuckDB drops column → DataFusion schema reflects removal

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_alter_drop_column_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("alter_drop.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB: create table with 3 columns, insert data, then drop one column
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.records (id INT, name VARCHAR, obsolete VARCHAR)");
    duckdb.execute(
        "INSERT INTO ducklake.main.records VALUES \
         (1, 'Alice', 'old_data'), \
         (2, 'Bob', 'old_data')",
    );
    duckdb.execute("ALTER TABLE ducklake.main.records DROP COLUMN obsolete");

    // Verify DuckDB only sees 2 columns
    let duckdb_rows = duckdb.query("SELECT id, name FROM ducklake.main.records ORDER BY id");
    drop(duckdb);

    // Verify DataFusion sees reduced schema
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_rows = df_query(
        &ctx,
        "SELECT id, name FROM ducklake.main.records ORDER BY id",
    )
    .await;

    let expected = vec![vec!["1".into(), "Alice".into()], vec!["2".into(), "Bob".into()]];

    assert_results_eq("alter_drop_column_duckdb_verify", &expected, &duckdb_rows);
    assert_results_eq("alter_drop_column_df_verify", &expected, &df_rows);

    // Verify the dropped column is not in the schema
    let schema_check = df_query(&ctx, "SELECT * FROM ducklake.main.records ORDER BY id").await;
    // SELECT * should return only id and name (2 columns per row)
    assert_eq!(
        schema_check[0].len(),
        2,
        "Dropped column should not appear in schema"
    );
}

// ==================== Type roundtrip: TIMESTAMP (DuckDB → DF) ====================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_timestamp_type_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("timestamps.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB writes timestamps
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.events (id INT, ts TIMESTAMP, event VARCHAR)");
    duckdb.execute(
        "INSERT INTO ducklake.main.events VALUES \
         (1, '2024-01-15 10:30:00', 'login'), \
         (2, '2024-06-30 23:59:59', 'logout'), \
         (3, '2024-12-25 00:00:00', 'holiday')",
    );

    let duckdb_rows = duckdb.query("SELECT id, ts, event FROM ducklake.main.events ORDER BY id");
    drop(duckdb);

    // Verify DataFusion reads timestamps correctly
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_rows = df_query(
        &ctx,
        "SELECT id, ts, event FROM ducklake.main.events ORDER BY id",
    )
    .await;

    assert_eq!(df_rows.len(), 3);
    assert_eq!(df_rows[0][1], "2024-01-15 10:30:00");
    assert_eq!(df_rows[1][1], "2024-06-30 23:59:59");
    assert_eq!(df_rows[2][1], "2024-12-25 00:00:00");
    assert_results_eq("timestamp_roundtrip", &duckdb_rows, &df_rows);
}

// ==================== Type roundtrip: DATE (DuckDB → DF) ====================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_date_type_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("dates.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB writes dates
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.calendar (id INT, d DATE, label VARCHAR)");
    duckdb.execute(
        "INSERT INTO ducklake.main.calendar VALUES \
         (1, '1992-01-01', 'start'), \
         (2, '2024-02-29', 'leap'), \
         (3, '2030-12-31', 'future')",
    );

    let duckdb_rows = duckdb.query("SELECT id, d, label FROM ducklake.main.calendar ORDER BY id");
    drop(duckdb);

    // Verify DataFusion reads dates correctly
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_rows = df_query(
        &ctx,
        "SELECT id, d, label FROM ducklake.main.calendar ORDER BY id",
    )
    .await;

    assert_eq!(df_rows.len(), 3);
    assert_eq!(df_rows[0][1], "1992-01-01");
    assert_eq!(df_rows[1][1], "2024-02-29");
    assert_eq!(df_rows[2][1], "2030-12-31");
    assert_results_eq("date_roundtrip", &duckdb_rows, &df_rows);
}

// ==================== Type roundtrip: DECIMAL (DuckDB → DF) ====================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_decimal_type_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("decimals.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB writes decimals with different precisions/scales
    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute(
        "CREATE TABLE ducklake.main.prices (id INT, price DECIMAL(10,2), tax DECIMAL(5,4))",
    );
    duckdb.execute(
        "INSERT INTO ducklake.main.prices VALUES \
         (1, 999.99, 0.0825), \
         (2, 0.01, 0.0000), \
         (3, 12345.67, 0.1000)",
    );

    let duckdb_rows = duckdb.query("SELECT id, price, tax FROM ducklake.main.prices ORDER BY id");
    drop(duckdb);

    // Verify DataFusion reads decimals correctly
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df_rows = df_query(
        &ctx,
        "SELECT id, price, tax FROM ducklake.main.prices ORDER BY id",
    )
    .await;

    assert_eq!(df_rows.len(), 3);
    // Verify decimal values roundtrip correctly
    assert_eq!(df_rows[0][1], "999.99");
    assert_eq!(df_rows[1][1], "0.01");
    assert_eq!(df_rows[2][1], "12345.67");
    assert_results_eq("decimal_roundtrip", &duckdb_rows, &df_rows);
}

// ==================== Type roundtrip: DF writes types → DuckDB reads ====================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_write_typed_data_duckdb_read() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Write data with Date32 and Int64 types via DuckLakeTableWriter
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("d", DataType::Date32, true),
        Field::new("amount", DataType::Int64, true),
    ]));

    // Date32 values: days since epoch
    // 2024-01-15 = 19737 days since 1970-01-01
    // 2024-06-30 = 19904 days
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Date32Array::from(vec![Some(19737), Some(19904)])),
            Arc::new(Int64Array::from(vec![Some(1000), Some(2000)])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer
        .write_table("main", "typed_data", &[batch])
        .await
        .unwrap();
    assert_eq!(result.records_written, 2);

    // DataFusion reads and verifies its own written data
    let ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(
        &ctx,
        "SELECT id, d, amount FROM ducklake.main.typed_data ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 2);
    assert_eq!(df_rows[0][0], "1");
    assert_eq!(df_rows[0][1], "2024-01-15");
    assert_eq!(df_rows[0][2], "1000");
    assert_eq!(df_rows[1][0], "2");
    assert_eq!(df_rows[1][1], "2024-06-30");
    assert_eq!(df_rows[1][2], "2000");

    // DuckDB reads DF-written data and verifies integer columns
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, amount FROM ducklake.main.typed_data ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "1000");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[1][1], "2000");
    // NOTE: Date32 cross-engine is affected by R5-S-014 (inlined Date serialization
    // uses epoch integers instead of ISO strings). DuckDB→DF direction works because
    // DuckDB writes ISO dates to Parquet. DF→DuckDB direction for inlined data is
    // tracked separately.
}

// ==================== Combined: DML interleaved with reads ====================
// DuckDB inserts → DF reads → DuckDB updates → DF reads → DuckDB deletes → DF reads

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_interleaved_dml_reads() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("interleaved.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // Step 1: DuckDB creates and inserts
    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute("CREATE TABLE ducklake.main.log (id INT, msg VARCHAR, level INT)");
        duckdb.execute(
            "INSERT INTO ducklake.main.log VALUES \
             (1, 'start', 1), (2, 'process', 2), (3, 'error', 3)",
        );
    }

    // Step 2: DF reads initial state
    {
        let ctx = open_in_datafusion_duckdb(&catalog_path);
        let rows = df_query(
            &ctx,
            "SELECT id, msg, level FROM ducklake.main.log ORDER BY id",
        )
        .await;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec!["1", "start", "1"]);
        assert_eq!(rows[1], vec!["2", "process", "2"]);
        assert_eq!(rows[2], vec!["3", "error", "3"]);
    }

    // Step 3: DuckDB updates
    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute("UPDATE ducklake.main.log SET msg = 'warning', level = 2 WHERE id = 3");
    }

    // Step 4: DF reads updated state (fresh provider)
    {
        let ctx = open_in_datafusion_duckdb(&catalog_path);
        let rows = df_query(
            &ctx,
            "SELECT id, msg, level FROM ducklake.main.log ORDER BY id",
        )
        .await;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2], vec!["3", "warning", "2"]);
    }

    // Step 5: DuckDB deletes
    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute("DELETE FROM ducklake.main.log WHERE id = 1");
    }

    // Step 6: DF reads final state (fresh provider)
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let rows = df_query(
        &ctx,
        "SELECT id, msg, level FROM ducklake.main.log ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["2", "process", "2"]);
    assert_eq!(rows[1], vec!["3", "warning", "2"]);
}
