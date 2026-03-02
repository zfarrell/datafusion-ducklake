//! Tests for write-side partitioning support.
//!
//! Verifies that DataFusion correctly writes partitioned Parquet files with
//! Hive-style directory layout and registers partition values in metadata.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::path::Path;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::DataType;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use tempfile::TempDir;

use datafusion_ducklake::metadata_writer::{
    AlterTableOp, ColumnDef, MetadataWriter, PartitionColumnDef, WriteMode,
};
use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeQueryPlanner, MetadataProvider, SqliteMetadataProvider,
    SqliteMetadataWriter,
};

// ==================== Setup helpers ====================

/// Create the catalog database and a table using the writer API.
/// Returns the connection string.
async fn create_catalog_with_table(
    catalog_path: &Path,
    data_path: &Path,
    table_name: &str,
    columns: &[ColumnDef],
) -> String {
    let conn_str = format!("sqlite:{}?mode=rwc", catalog_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer
        .set_data_path(&format!("{}/", data_path.display()))
        .unwrap();

    let _setup = writer
        .begin_write_transaction("main", table_name, columns, WriteMode::Append)
        .unwrap();

    conn_str
}

/// Open a DataFusion SessionContext pointing at an existing catalog.
async fn open_df_context(conn_str: &str) -> SessionContext {
    let provider: Arc<dyn MetadataProvider> =
        Arc::new(SqliteMetadataProvider::new(conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(DuckLakeQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Set partitioning on a table using the SQLite writer directly.
async fn set_partitioning(
    conn_str: &str,
    table_name: &str,
    partition_columns: Vec<(&str, Option<&str>)>,
) {
    let writer = SqliteMetadataWriter::new(conn_str).await.unwrap();

    let provider = SqliteMetadataProvider::new(conn_str).await.unwrap();
    let snapshot = provider.get_current_snapshot().unwrap();
    let schemas = provider.list_schemas(snapshot).unwrap();
    let schema = schemas.iter().find(|s| s.schema_name == "main").unwrap();
    let tables = provider.list_tables(schema.schema_id, snapshot).unwrap();
    let table = tables.iter().find(|t| t.table_name == table_name).unwrap();

    let op = AlterTableOp::SetPartitionedBy {
        partition_columns: partition_columns
            .iter()
            .map(|(name, transform)| PartitionColumnDef {
                column_name: name.to_string(),
                transform: transform.map(|t| t.to_string()),
            })
            .collect(),
    };
    writer.alter_table(table.table_id, &op).unwrap();
}

fn batches_to_sorted_strings(batches: &[arrow::record_batch::RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    row.push("NULL".to_string());
                } else {
                    row.push(arrow_val_to_string(col, row_idx));
                }
            }
            rows.push(row);
        }
    }
    rows.sort();
    rows
}

fn arrow_val_to_string(array: &dyn Array, idx: usize) -> String {
    match array.data_type() {
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(idx)
            .to_string(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(idx)
            .to_string(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(idx)
            .to_string(),
        DataType::Float64 => format!(
            "{}",
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(idx)
        ),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(idx)
            .to_string(),
        _ => arrow::util::display::array_value_to_string(array, idx)
            .unwrap_or_else(|_| format!("<unsupported:{:?}>", array.data_type())),
    }
}

// ==================== Tests ====================

/// Test: Create table, set partitioning, insert → data is split by partition
#[tokio::test(flavor = "multi_thread")]
async fn test_write_partitioned_basic() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let columns = vec![
        ColumnDef::new("id", "int32", false).unwrap(),
        ColumnDef::new("category", "varchar", true).unwrap(),
        ColumnDef::new("value", "float64", true).unwrap(),
    ];
    let conn_str = create_catalog_with_table(&catalog_path, &data_path, "events", &columns).await;
    set_partitioning(&conn_str, "events", vec![("category", None)]).await;

    // INSERT
    let ctx = open_df_context(&conn_str).await;
    ctx.sql("INSERT INTO ducklake.main.events (id, category, value) VALUES (1, 'A', 10.0), (2, 'B', 20.0), (3, 'A', 30.0), (4, 'C', 40.0)")
        .await.unwrap().collect().await.unwrap();
    drop(ctx);

    // Re-open to get fresh snapshot for SELECT
    let ctx = open_df_context(&conn_str).await;
    let df = ctx
        .sql("SELECT id, category, value FROM ducklake.main.events ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_sorted_strings(&batches);

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], vec!["1", "A", "10"]);
    assert_eq!(rows[1], vec!["2", "B", "20"]);
    assert_eq!(rows[2], vec!["3", "A", "30"]);
    assert_eq!(rows[3], vec!["4", "C", "40"]);
}

/// Test: Partitioned write → partition pruning on read works
#[tokio::test(flavor = "multi_thread")]
async fn test_write_partitioned_with_filter() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let columns = vec![
        ColumnDef::new("id", "int32", false).unwrap(),
        ColumnDef::new("kind", "varchar", true).unwrap(),
        ColumnDef::new("price", "float64", true).unwrap(),
    ];
    let conn_str = create_catalog_with_table(&catalog_path, &data_path, "items", &columns).await;
    set_partitioning(&conn_str, "items", vec![("kind", None)]).await;

    let ctx = open_df_context(&conn_str).await;
    ctx.sql("INSERT INTO ducklake.main.items (id, kind, price) VALUES (1, 'book', 10.0), (2, 'toy', 20.0), (3, 'book', 15.0)")
        .await.unwrap().collect().await.unwrap();
    drop(ctx);

    let ctx = open_df_context(&conn_str).await;
    let df = ctx
        .sql("SELECT id, price FROM ducklake.main.items WHERE kind = 'book' ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_sorted_strings(&batches);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "10"]);
    assert_eq!(rows[1], vec!["3", "15"]);
}

/// Test: Non-partitioned table write still works (no regression)
#[tokio::test(flavor = "multi_thread")]
async fn test_write_non_partitioned_no_regression() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let columns = vec![
        ColumnDef::new("id", "int32", false).unwrap(),
        ColumnDef::new("name", "varchar", true).unwrap(),
    ];
    let conn_str = create_catalog_with_table(&catalog_path, &data_path, "simple", &columns).await;

    let ctx = open_df_context(&conn_str).await;
    ctx.sql("INSERT INTO ducklake.main.simple (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    drop(ctx);

    let ctx = open_df_context(&conn_str).await;
    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.simple ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_sorted_strings(&batches);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "Alice"]);
    assert_eq!(rows[1], vec!["2", "Bob"]);
}

/// Test: COUNT(*) on partitioned table after DF write
#[tokio::test(flavor = "multi_thread")]
async fn test_write_partitioned_count() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let columns = vec![
        ColumnDef::new("id", "int32", false).unwrap(),
        ColumnDef::new("group_name", "varchar", true).unwrap(),
    ];
    let conn_str = create_catalog_with_table(&catalog_path, &data_path, "counts", &columns).await;
    set_partitioning(&conn_str, "counts", vec![("group_name", None)]).await;

    let ctx = open_df_context(&conn_str).await;
    ctx.sql("INSERT INTO ducklake.main.counts (id, group_name) VALUES (1, 'X'), (2, 'Y'), (3, 'X'), (4, 'Z'), (5, 'Y')")
        .await.unwrap().collect().await.unwrap();
    drop(ctx);

    let ctx = open_df_context(&conn_str).await;
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.counts")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_sorted_strings(&batches);
    assert_eq!(rows[0], vec!["5"]);
}

/// Test: Hive-style directory structure is created
#[tokio::test(flavor = "multi_thread")]
async fn test_write_partitioned_creates_hive_dirs() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let columns = vec![
        ColumnDef::new("id", "int32", false).unwrap(),
        ColumnDef::new("region", "varchar", true).unwrap(),
        ColumnDef::new("value", "float64", true).unwrap(),
    ];
    let conn_str = create_catalog_with_table(&catalog_path, &data_path, "events", &columns).await;
    set_partitioning(&conn_str, "events", vec![("region", None)]).await;

    let ctx = open_df_context(&conn_str).await;
    ctx.sql("INSERT INTO ducklake.main.events (id, region, value) VALUES (1, 'US', 10.0), (2, 'EU', 20.0)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Check that Hive-style directories were created
    let main_events_dir = data_path.join("main").join("events");

    let mut found_us = false;
    let mut found_eu = false;
    if main_events_dir.exists() {
        for entry in std::fs::read_dir(&main_events_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            if name == "region=US" {
                found_us = true;
                let parquet_files: Vec<_> = std::fs::read_dir(entry.path())
                    .unwrap()
                    .filter(|e| {
                        e.as_ref()
                            .unwrap()
                            .path()
                            .extension()
                            .map_or(false, |ext| ext == "parquet")
                    })
                    .collect();
                assert!(
                    !parquet_files.is_empty(),
                    "region=US directory should contain parquet files"
                );
            } else if name == "region=EU" {
                found_eu = true;
            }
        }
    }
    assert!(
        found_us,
        "Should have region=US directory under {:?}",
        main_events_dir
    );
    assert!(
        found_eu,
        "Should have region=EU directory under {:?}",
        main_events_dir
    );
}

/// Test: Multiple inserts into partitioned table (append)
#[tokio::test(flavor = "multi_thread")]
async fn test_write_partitioned_multiple_inserts() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let columns = vec![
        ColumnDef::new("id", "int32", false).unwrap(),
        ColumnDef::new("level", "varchar", true).unwrap(),
        ColumnDef::new("msg", "varchar", true).unwrap(),
    ];
    let conn_str = create_catalog_with_table(&catalog_path, &data_path, "log", &columns).await;
    set_partitioning(&conn_str, "log", vec![("level", None)]).await;

    // First insert
    let ctx = open_df_context(&conn_str).await;
    ctx.sql("INSERT INTO ducklake.main.log (id, level, msg) VALUES (1, 'INFO', 'hello'), (2, 'ERROR', 'fail')")
        .await.unwrap().collect().await.unwrap();
    drop(ctx);

    // Second insert (fresh context picks up first insert's snapshot)
    let ctx = open_df_context(&conn_str).await;
    ctx.sql("INSERT INTO ducklake.main.log (id, level, msg) VALUES (3, 'INFO', 'world'), (4, 'WARN', 'careful')")
        .await.unwrap().collect().await.unwrap();
    drop(ctx);

    // Read all data
    let ctx = open_df_context(&conn_str).await;
    let df = ctx
        .sql("SELECT id, level, msg FROM ducklake.main.log ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let rows = batches_to_sorted_strings(&batches);

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], vec!["1", "INFO", "hello"]);
    assert_eq!(rows[1], vec!["2", "ERROR", "fail"]);
    assert_eq!(rows[2], vec!["3", "INFO", "world"]);
    assert_eq!(rows[3], vec!["4", "WARN", "careful"]);
}
