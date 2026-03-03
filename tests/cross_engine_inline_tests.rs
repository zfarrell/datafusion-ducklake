//! Cross-engine tests for DuckLake data inlining support.
//!
//! Tests that DataFusion can correctly read inlined data (stored directly in
//! the catalog database) from DuckLake catalogs created by DuckDB.

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

mod common;

use std::path::Path;
use std::sync::Arc;

use common::test_utils::{batches_to_strings_filtered, DuckDbConn};
use datafusion::prelude::*;
use tempfile::TempDir;

use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider, SqliteMetadataProvider};

// ==================== Setup helpers ====================

fn open_in_datafusion_duckdb(catalog_path: &Path) -> SessionContext {
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())
        .expect("create DuckdbMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

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

// ==================== Tests: DuckDB writes inlined data → DataFusion reads ====================

/// Test: DuckDB creates inlined data → DataFusion reads via DuckDB provider
#[tokio::test]
async fn test_duckdb_inlined_data_df_duckdb_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB creates catalog with inlining enabled
    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.small_data (id INTEGER, name VARCHAR, value DOUBLE)");
    ddb.execute("CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 100)");
    ddb.execute(
        "INSERT INTO ducklake.main.small_data VALUES (1, 'Alice', 10.0), (2, 'Bob', 20.0), (3, 'Charlie', 30.0)",
    );

    // Verify DuckDB can read it
    let ddb_rows = ddb.query("SELECT id, name, value FROM ducklake.main.small_data ORDER BY id");
    assert_eq!(ddb_rows.len(), 3);
    assert_eq!(ddb_rows[0][0], "1");
    assert_eq!(ddb_rows[0][1], "Alice");
    drop(ddb);

    // DataFusion reads the inlined data
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, name, value FROM ducklake.main.small_data ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 3);
    // Float values: DuckDB stores as "10.0" but DataFusion may format as "10"
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");
    assert!(rows[0][2] == "10" || rows[0][2] == "10.0");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[1][1], "Bob");
    assert_eq!(rows[2][0], "3");
    assert_eq!(rows[2][1], "Charlie");
}

/// Test: DuckDB creates inlined data (DuckDB-native catalog) → DataFusion reads via DuckDB provider
/// Also tests that the DuckDB provider correctly handles inlined data tables.
#[tokio::test]
async fn test_duckdb_inlined_data_integer_types() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.t1 (id INTEGER, count BIGINT)");
    ddb.execute("CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 100)");
    ddb.execute("INSERT INTO ducklake.main.t1 VALUES (1, 100), (2, 200), (3, 300)");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, count FROM ducklake.main.t1 ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "100"]);
    assert_eq!(rows[1], vec!["2", "200"]);
    assert_eq!(rows[2], vec!["3", "300"]);
}

/// Test: Inlined data with deletes
#[tokio::test]
async fn test_duckdb_inlined_data_with_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.t1 (id INTEGER, name VARCHAR)");
    ddb.execute("CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 100)");
    ddb.execute("INSERT INTO ducklake.main.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    ddb.execute("DELETE FROM ducklake.main.t1 WHERE id = 2");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.t1 ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "a"]);
    assert_eq!(rows[1], vec!["3", "c"]);
}

/// Test: Mix of inlined data and Parquet files
#[tokio::test]
async fn test_duckdb_mixed_inline_and_parquet() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.t1 (id INTEGER, name VARCHAR)");
    // First insert without inlining → goes to Parquet
    ddb.execute("INSERT INTO ducklake.main.t1 VALUES (1, 'parquet1'), (2, 'parquet2')");
    // Enable inlining
    ddb.execute("CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 100)");
    // Second insert with inlining → goes to catalog
    ddb.execute("INSERT INTO ducklake.main.t1 VALUES (3, 'inline1'), (4, 'inline2')");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.t1 ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], vec!["1", "parquet1"]);
    assert_eq!(rows[1], vec!["2", "parquet2"]);
    assert_eq!(rows[2], vec!["3", "inline1"]);
    assert_eq!(rows[3], vec!["4", "inline2"]);
}

/// Test: Flush inlined data → DataFusion reads Parquet result
/// Uses the duckdb CLI to flush since DuckDB's Rust API has transaction limitations
#[tokio::test]
async fn test_duckdb_flush_inlined_data() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    // Insert inlined data
    {
        let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        ddb.execute("CREATE TABLE ducklake.main.t1 (id INTEGER, name VARCHAR)");
        ddb.execute("CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 100)");
        ddb.execute("INSERT INTO ducklake.main.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    }

    // Flush using duckdb CLI (avoids DuckDB Rust API transaction limitations)
    let flush_sql = format!(
        "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:{}' AS ducklake (DATA_PATH '{}'); CALL ducklake_flush_inlined_data('ducklake');",
        catalog_path.display(),
        data_path.display()
    );
    let output = std::process::Command::new("duckdb")
        .arg(":memory:")
        .arg("-c")
        .arg(&flush_sql)
        .output()
        .expect("duckdb CLI not found");
    assert!(
        output.status.success(),
        "duckdb flush failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // After flush, data should be in Parquet files
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.t1 ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "a"]);
    assert_eq!(rows[1], vec!["2", "b"]);
    assert_eq!(rows[2], vec!["3", "c"]);
}

/// Test: COUNT(*) on inlined data
#[tokio::test]
async fn test_duckdb_inlined_count() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.t1 (id INTEGER)");
    ddb.execute("CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 100)");
    ddb.execute("INSERT INTO ducklake.main.t1 VALUES (1), (2), (3), (4), (5)");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.t1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_strings_filtered(&batches);
    assert_eq!(rows[0], vec!["5"]);
}

/// Test: Inlined data with multiple inserts
#[tokio::test]
async fn test_duckdb_inlined_multiple_inserts() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.t1 (id INTEGER, name VARCHAR)");
    ddb.execute("CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 100)");
    ddb.execute("INSERT INTO ducklake.main.t1 VALUES (1, 'first')");
    ddb.execute("INSERT INTO ducklake.main.t1 VALUES (2, 'second')");
    ddb.execute("INSERT INTO ducklake.main.t1 VALUES (3, 'third')");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.t1 ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "first"]);
    assert_eq!(rows[1], vec!["2", "second"]);
    assert_eq!(rows[2], vec!["3", "third"]);
}

/// Test: Empty inlined table (inlining enabled but no data)
#[tokio::test]
async fn test_duckdb_empty_inlined_table() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.t1 (id INTEGER, name VARCHAR)");
    ddb.execute("CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 100)");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx.sql("SELECT * FROM ducklake.main.t1").await.unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_strings_filtered(&batches);
    assert_eq!(rows.len(), 0);
}

/// Test: Inlined data with NULLs
#[tokio::test]
async fn test_duckdb_inlined_data_with_nulls() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.t1 (id INTEGER, name VARCHAR, score DOUBLE)");
    ddb.execute("CALL ducklake_set_option('ducklake', 'data_inlining_row_limit', 100)");
    ddb.execute(
        "INSERT INTO ducklake.main.t1 VALUES (1, 'a', 10.0), (2, NULL, 20.0), (3, 'c', NULL)",
    );
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, name, score FROM ducklake.main.t1 ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "a", "10"]);
    assert_eq!(rows[1], vec!["2", "NULL", "20"]);
    assert_eq!(rows[2], vec!["3", "c", "NULL"]);
}
