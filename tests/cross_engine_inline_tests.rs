//! Cross-engine tests for DuckLake data inlining support.
//!
//! Tests that DataFusion can correctly read inlined data (stored directly in
//! the catalog database) from DuckLake catalogs created by DuckDB.

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

use std::path::Path;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::DataType;
use datafusion::prelude::*;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckdbMetadataProvider, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter,
};

// ==================== Setup helpers ====================

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

    fn open_sqlite(catalog_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", []).expect("load ducklake");
        let attach_path = format!("ducklake:sqlite:{}", catalog_path.display());
        conn.execute(&format!("ATTACH '{}' AS ducklake;", attach_path), [])
            .expect("attach ducklake catalog");
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
    let mut rows = batches_to_strings(&batches);
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
    let mut rows = batches_to_strings(&batches);
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
    let mut rows = batches_to_strings(&batches);
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
    let mut rows = batches_to_strings(&batches);
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
    let mut rows = batches_to_strings(&batches);
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
    let rows = batches_to_strings(&batches);
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
    let mut rows = batches_to_strings(&batches);
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
    let rows = batches_to_strings(&batches);
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
    let mut rows = batches_to_strings(&batches);
    rows.sort();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "a", "10"]);
    assert_eq!(rows[1], vec!["2", "NULL", "20"]);
    assert_eq!(rows[2], vec!["3", "c", "NULL"]);
}
