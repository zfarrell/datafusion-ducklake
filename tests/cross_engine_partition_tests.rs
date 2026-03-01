//! Cross-engine tests for DuckLake partitioning support.
//!
//! Tests that DataFusion can correctly read partitioned DuckLake tables
//! created by DuckDB, including partition pruning and hive-style directories.

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use tempfile::TempDir;

use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider, SqliteMetadataProvider};

// ==================== Setup helpers ====================

struct CrossEngineEnv {
    _temp_dir: TempDir,
    catalog_db_path: PathBuf,
    data_path: PathBuf,
}

/// Open a DuckDB connection to a DuckLake catalog backed by a native DuckDB file.
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

fn duckdb_value_to_string(v: &duckdb::types::Value) -> String {
    match v {
        duckdb::types::Value::Null => "NULL".to_string(),
        duckdb::types::Value::Boolean(b) => b.to_string(),
        duckdb::types::Value::TinyInt(i) => i.to_string(),
        duckdb::types::Value::SmallInt(i) => i.to_string(),
        duckdb::types::Value::Int(i) => i.to_string(),
        duckdb::types::Value::BigInt(i) => i.to_string(),
        duckdb::types::Value::Float(f) => format!("{f}"),
        duckdb::types::Value::Double(f) => format!("{f}"),
        duckdb::types::Value::Text(s) => s.clone(),
        _ => format!("{v:?}"),
    }
}

fn batches_to_strings(batches: &[arrow::record_batch::RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let col_indices: Vec<usize> = (0..batch.num_columns())
            .filter(|&i| {
                let name = schema.field(i).name();
                name != "filename" && name != "file_row_number"
            })
            .collect();
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for &col_idx in &col_indices {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    row.push("NULL".to_string());
                } else {
                    row.push(arrow_value_to_string(col, row_idx));
                }
            }
            rows.push(row);
        }
    }
    rows
}

fn arrow_value_to_string(array: &dyn Array, idx: usize) -> String {
    match array.data_type() {
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{}", a.value(idx))
        },
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = a.value(idx);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163).unwrap();
            date.format("%Y-%m-%d").to_string()
        },
        _ => format!("{:?}", array),
    }
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
    let mut rows = batches_to_strings(&batches);
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
    let rows = batches_to_strings(&batches);

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
    let mut rows = batches_to_strings(&batches);
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
    let mut rows = batches_to_strings(&batches);
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
    let rows = batches_to_strings(&batches);

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
    let mut rows = batches_to_strings(&batches);
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
    let rows = batches_to_strings(&batches);
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
    let rows = batches_to_strings(&batches);
    assert_eq!(rows.len(), 0);
}
