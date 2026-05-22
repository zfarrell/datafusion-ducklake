//! Integration tests for extended virtual columns (rowid, snapshot_id, file_index).
//!
//! Tests cover individual virtual columns, multi-file scenarios, cross-engine
//! value matching, and WHERE clause filtering.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite", feature = "metadata-duckdb"))]

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, UInt64Array};
use arrow::datatypes::DataType;
use common::test_utils::DuckDbConn;
use datafusion::prelude::*;
use tempfile::TempDir;

use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};

/// Create a multi-file test catalog via DuckDB and return a DataFusion context
async fn setup_multi_file_catalog() -> (SessionContext, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("test.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB writes two inserts to create two files with different snapshots
    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute("CREATE TABLE ducklake.main.people (id INT, name VARCHAR)");
        duckdb.execute(
            "INSERT INTO ducklake.main.people VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        );
        duckdb.execute("INSERT INTO ducklake.main.people VALUES (4, 'Dave'), (5, 'Eve')");
    }

    // Create DataFusion read context
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    (ctx, temp_dir)
}

/// Helper to collect (i32, i64) pairs from two columns
fn collect_i32_i64(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(i32, i64)> {
    let mut pairs = Vec::new();
    for batch in batches {
        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            pairs.push((col0.value(i), col1.value(i)));
        }
    }
    pairs.sort_by_key(|(id, _)| *id);
    pairs
}

/// Helper to collect (i32, u64) pairs from two columns
fn collect_i32_u64(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(i32, u64)> {
    let mut pairs = Vec::new();
    for batch in batches {
        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            pairs.push((col0.value(i), col1.value(i)));
        }
    }
    pairs.sort_by_key(|(id, _)| *id);
    pairs
}

// ── Individual virtual column tests ──

#[tokio::test(flavor = "multi_thread")]
async fn test_rowid_column() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    let df = ctx
        .sql("SELECT id, rowid FROM ducklake.main.people ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let pairs = collect_i32_i64(&batches);

    assert_eq!(pairs.len(), 5);
    // File 1: row_id_start=0, rows 0,1,2 → rowids 0,1,2
    // File 2: row_id_start=3, rows 0,1   → rowids 3,4
    assert_eq!(pairs[0], (1, 0));
    assert_eq!(pairs[1], (2, 1));
    assert_eq!(pairs[2], (3, 2));
    assert_eq!(pairs[3], (4, 3));
    assert_eq!(pairs[4], (5, 4));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_id_column() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    let df = ctx
        .sql("SELECT id, snapshot_id FROM ducklake.main.people ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let pairs = collect_i32_i64(&batches);

    assert_eq!(pairs.len(), 5);

    // File 1 and File 2 should have different snapshot_ids
    let snap_file1 = pairs[0].1;
    let snap_file2 = pairs[3].1;

    assert!(snap_file1 > 0, "snapshot_id should be positive");
    assert!(
        snap_file2 > snap_file1,
        "second file's snapshot should be greater"
    );

    // All rows from same file should have same snapshot_id
    assert_eq!(pairs[1].1, snap_file1);
    assert_eq!(pairs[2].1, snap_file1);
    assert_eq!(pairs[4].1, snap_file2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_file_index_column() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    let df = ctx
        .sql("SELECT id, file_index FROM ducklake.main.people ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let pairs = collect_i32_u64(&batches);

    assert_eq!(pairs.len(), 5);
    // First file gets index 0, second file gets index 1
    assert_eq!(pairs[0].1, 0);
    assert_eq!(pairs[1].1, 0);
    assert_eq!(pairs[2].1, 0);
    assert_eq!(pairs[3].1, 1);
    assert_eq!(pairs[4].1, 1);
}

// ── Projection tests ──

#[tokio::test(flavor = "multi_thread")]
async fn test_select_only_new_virtual_columns() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    let df = ctx
        .sql("SELECT rowid, snapshot_id, file_index FROM ducklake.main.people ORDER BY rowid")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "rowid");
    assert_eq!(schema.field(1).name(), "snapshot_id");
    assert_eq!(schema.field(2).name(), "file_index");

    // Verify types
    assert_eq!(*schema.field(0).data_type(), DataType::Int64);
    assert_eq!(*schema.field(1).data_type(), DataType::Int64);
    assert_eq!(*schema.field(2).data_type(), DataType::UInt64);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mixed_virtual_and_real_columns() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    let df = ctx
        .sql("SELECT rowid, id, file_index FROM ducklake.main.people ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "rowid");
    assert_eq!(schema.field(1).name(), "id");
    assert_eq!(schema.field(2).name(), "file_index");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_all_virtual_columns_together() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    let df = ctx
        .sql("SELECT filename, file_row_number, rowid, snapshot_id, file_index FROM ducklake.main.people ORDER BY rowid")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 5);
    assert_eq!(schema.field(0).name(), "filename");
    assert_eq!(schema.field(1).name(), "file_row_number");
    assert_eq!(schema.field(2).name(), "rowid");
    assert_eq!(schema.field(3).name(), "snapshot_id");
    assert_eq!(schema.field(4).name(), "file_index");
}

// ── Cross-engine value matching ──

#[tokio::test(flavor = "multi_thread")]
async fn test_cross_engine_rowid_values() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("xengine.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute("CREATE TABLE ducklake.main.test (id INT, val VARCHAR)");
        duckdb.execute("INSERT INTO ducklake.main.test VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        duckdb.execute("INSERT INTO ducklake.main.test VALUES (4, 'd'), (5, 'e')");
    }

    // DuckDB values
    let duckdb_vals: Vec<(i32, i64)> = {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("LOAD ducklake;").unwrap();
        conn.execute(
            &format!("ATTACH 'ducklake:{}' AS ducklake", catalog_path.display()),
            [],
        )
        .unwrap();
        let mut stmt = conn
            .prepare("SELECT id, rowid FROM ducklake.main.test ORDER BY id")
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    };

    // DataFusion values
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let df = ctx
        .sql("SELECT id, rowid FROM ducklake.main.test ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let df_vals = collect_i32_i64(&batches);

    assert_eq!(
        duckdb_vals, df_vals,
        "rowid values should match between DuckDB and DataFusion"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cross_engine_snapshot_id_values() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("xengine_snap.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute("CREATE TABLE ducklake.main.test (id INT)");
        duckdb.execute("INSERT INTO ducklake.main.test VALUES (1), (2), (3)");
        duckdb.execute("INSERT INTO ducklake.main.test VALUES (4), (5)");
    }

    let duckdb_vals: Vec<(i32, i64)> = {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("LOAD ducklake;").unwrap();
        conn.execute(
            &format!("ATTACH 'ducklake:{}' AS ducklake", catalog_path.display()),
            [],
        )
        .unwrap();
        let mut stmt = conn
            .prepare("SELECT id, snapshot_id FROM ducklake.main.test ORDER BY id")
            .unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    };

    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let df = ctx
        .sql("SELECT id, snapshot_id FROM ducklake.main.test ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let df_vals = collect_i32_i64(&batches);

    assert_eq!(
        duckdb_vals, df_vals,
        "snapshot_id values should match between DuckDB and DataFusion"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cross_engine_file_index_values() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("xengine_fidx.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute("CREATE TABLE ducklake.main.test (id INT)");
        duckdb.execute("INSERT INTO ducklake.main.test VALUES (1), (2), (3)");
        duckdb.execute("INSERT INTO ducklake.main.test VALUES (4), (5)");
    }

    let duckdb_vals: Vec<(i32, u64)> = {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("LOAD ducklake;").unwrap();
        conn.execute(
            &format!("ATTACH 'ducklake:{}' AS ducklake", catalog_path.display()),
            [],
        )
        .unwrap();
        let mut stmt = conn
            .prepare("SELECT id, file_index FROM ducklake.main.test ORDER BY id")
            .unwrap();
        stmt.query_map([], |row| {
            let id: i32 = row.get(0)?;
            let fi: u64 = row.get(1)?;
            Ok((id, fi))
        })
        .unwrap()
        .filter_map(|r| r.ok())
        .collect()
    };

    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let df = ctx
        .sql("SELECT id, file_index FROM ducklake.main.test ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let df_vals = collect_i32_u64(&batches);

    assert_eq!(
        duckdb_vals, df_vals,
        "file_index values should match between DuckDB and DataFusion"
    );
}

// ── WHERE clause tests ──

#[tokio::test(flavor = "multi_thread")]
async fn test_where_on_rowid() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    let df = ctx
        .sql("SELECT id, rowid FROM ducklake.main.people WHERE rowid = 3")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    let id = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert_eq!(id, 4); // rowid 3 = second file, first row
}

#[tokio::test(flavor = "multi_thread")]
async fn test_where_on_file_index() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    let df = ctx
        .sql("SELECT id FROM ducklake.main.people WHERE file_index = 1 ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let mut ids: Vec<i32> = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            ids.push(col.value(i));
        }
    }
    ids.sort();
    assert_eq!(ids, vec![4, 5]); // File index 1 = second file
}

#[tokio::test(flavor = "multi_thread")]
async fn test_where_on_snapshot_id() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    // Get the distinct snapshot_ids
    let df = ctx
        .sql("SELECT DISTINCT snapshot_id FROM ducklake.main.people ORDER BY snapshot_id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let mut snap_ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            snap_ids.push(col.value(i));
        }
    }
    assert_eq!(snap_ids.len(), 2, "should have two distinct snapshot_ids");

    // Filter by first snapshot
    let query = format!(
        "SELECT id FROM ducklake.main.people WHERE snapshot_id = {} ORDER BY id",
        snap_ids[0]
    );
    let df = ctx.sql(&query).await.unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3); // First snapshot has 3 rows
}

// ── COUNT(*) still works ──

#[tokio::test(flavor = "multi_thread")]
async fn test_count_with_all_virtual_columns() {
    let (ctx, _dir) = setup_multi_file_catalog().await;

    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.people")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let cnt = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 5);
}
