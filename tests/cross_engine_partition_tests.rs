//! Cross-engine tests for DuckLake partitioning support.
//!
//! Tests that DataFusion can correctly read partitioned DuckLake tables
//! created by DuckDB, including partition pruning and hive-style directories.
//!
//! TODO(R7-S-042): Add partition DML tests (DELETE/UPDATE on partitioned tables)
//! once DF write-path supports partition-aware DML operations.

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::test_utils::{DuckDbConn, batches_to_strings_filtered};
use datafusion::prelude::*;
use tempfile::TempDir;

use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider, SqliteMetadataProvider};

// ==================== Setup helpers ====================

struct CrossEngineEnv {
    _temp_dir: TempDir,
    catalog_db_path: PathBuf,
    data_path: PathBuf,
}

fn open_in_datafusion_duckdb(catalog_path: &Path) -> SessionContext {
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())
        .expect("create DuckdbMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

// ==================== Tests ====================

/// Test: DuckDB creates a partitioned table → DataFusion reads all data
#[tokio::test]
async fn test_duckdb_partitioned_table_df_read_all() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB creates and populates a partitioned table
    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.events (id INTEGER, category VARCHAR, value DOUBLE)");
    ddb.execute("ALTER TABLE ducklake.main.events SET PARTITIONED BY (category)");
    ddb.execute("INSERT INTO ducklake.main.events VALUES (1, 'A', 10.0), (2, 'B', 20.0)");
    ddb.execute("INSERT INTO ducklake.main.events VALUES (3, 'A', 30.0), (4, 'C', 40.0)");
    drop(ddb);

    // DataFusion reads the partitioned table
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, category, value FROM ducklake.main.events ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], vec!["1", "A", "10"]);
    assert_eq!(rows[1], vec!["2", "B", "20"]);
    assert_eq!(rows[2], vec!["3", "A", "30"]);
    assert_eq!(rows[3], vec!["4", "C", "40"]);
}

/// Test: DuckDB creates partitioned table → DataFusion reads with filter (partition pruning)
#[tokio::test]
async fn test_duckdb_partitioned_table_df_read_with_filter() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.events (id INTEGER, category VARCHAR, value DOUBLE)");
    ddb.execute("ALTER TABLE ducklake.main.events SET PARTITIONED BY (category)");
    ddb.execute("INSERT INTO ducklake.main.events VALUES (1, 'A', 10.0), (2, 'B', 20.0), (3, 'A', 30.0), (4, 'C', 40.0)");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, value FROM ducklake.main.events WHERE category = 'A' ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_strings_filtered(&batches);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "10"]);
    assert_eq!(rows[1], vec!["3", "30"]);
}

/// Test: DuckDB creates table with pre-partition data and post-partition data
#[tokio::test]
async fn test_duckdb_partition_pre_and_post_data() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.events (id INTEGER, category VARCHAR, value DOUBLE)");
    // Insert BEFORE partitioning
    ddb.execute("INSERT INTO ducklake.main.events VALUES (1, 'A', 10.0), (2, 'B', 20.0)");
    // Then partition
    ddb.execute("ALTER TABLE ducklake.main.events SET PARTITIONED BY (category)");
    // Insert AFTER partitioning
    ddb.execute("INSERT INTO ducklake.main.events VALUES (3, 'A', 30.0), (4, 'C', 40.0)");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT id, category, value FROM ducklake.main.events ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], vec!["1", "A", "10"]);
    assert_eq!(rows[1], vec!["2", "B", "20"]);
    assert_eq!(rows[2], vec!["3", "A", "30"]);
    assert_eq!(rows[3], vec!["4", "C", "40"]);
}

/// Test: Multi-column partitioning with identity transform
#[tokio::test]
async fn test_duckdb_multi_column_partition() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute(
        "CREATE TABLE ducklake.main.events (id INTEGER, region VARCHAR, category VARCHAR, value DOUBLE)",
    );
    ddb.execute("ALTER TABLE ducklake.main.events SET PARTITIONED BY (region, category)");
    ddb.execute("INSERT INTO ducklake.main.events VALUES (1, 'US', 'A', 10.0), (2, 'EU', 'B', 20.0), (3, 'US', 'B', 30.0)");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);

    // Read all
    let df = ctx
        .sql("SELECT id, region, category, value FROM ducklake.main.events ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "US", "A", "10"]);
    assert_eq!(rows[1], vec!["2", "EU", "B", "20"]);
    assert_eq!(rows[2], vec!["3", "US", "B", "30"]);

    // Filter on one partition column
    let df = ctx
        .sql("SELECT id, value FROM ducklake.main.events WHERE region = 'US' ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_strings_filtered(&batches);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "10"]);
    assert_eq!(rows[1], vec!["3", "30"]);
}

/// Test: Partitioning with MONTH transform
#[tokio::test]
async fn test_duckdb_month_partition_transform() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.events (id INTEGER, event_date DATE, value DOUBLE)");
    ddb.execute("ALTER TABLE ducklake.main.events SET PARTITIONED BY (MONTH(event_date))");
    ddb.execute("INSERT INTO ducklake.main.events VALUES (1, '2024-01-15', 10.0), (2, '2024-02-20', 20.0), (3, '2024-01-25', 30.0)");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);

    // Read all data (MONTH transform doesn't affect identity pruning, data should still be readable)
    let df = ctx
        .sql("SELECT id, value FROM ducklake.main.events ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = batches_to_strings_filtered(&batches);
    rows.sort();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "10"]);
    assert_eq!(rows[1], vec!["2", "20"]);
    assert_eq!(rows[2], vec!["3", "30"]);
}

/// Test: COUNT(*) on partitioned table
#[tokio::test]
async fn test_duckdb_partitioned_count() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.events (id INTEGER, category VARCHAR)");
    ddb.execute("ALTER TABLE ducklake.main.events SET PARTITIONED BY (category)");
    ddb.execute(
        "INSERT INTO ducklake.main.events VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')",
    );
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.events")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_strings_filtered(&batches);
    assert_eq!(rows[0], vec!["5"]);
}

/// Test: Empty partitioned table
#[tokio::test]
async fn test_duckdb_empty_partitioned_table() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data/");
    std::fs::create_dir_all(&data_path).unwrap();

    let ddb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    ddb.execute("CREATE TABLE ducklake.main.events (id INTEGER, category VARCHAR)");
    ddb.execute("ALTER TABLE ducklake.main.events SET PARTITIONED BY (category)");
    drop(ddb);

    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let df = ctx.sql("SELECT * FROM ducklake.main.events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_strings_filtered(&batches);
    assert_eq!(rows.len(), 0);
}
