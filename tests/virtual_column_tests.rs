//! Integration tests for virtual column support (filename, file_row_number).

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter,
};

fn create_object_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

/// Helper to create a test environment: write data, return a read-only SessionContext
async fn setup_test_table() -> (SessionContext, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer.set_data_path(data_path.to_str().unwrap()).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "people", &[batch])
        .await
        .unwrap();

    // Create read context
    let read_conn_str = format!("sqlite:{}", db_path.display());
    let provider = SqliteMetadataProvider::new(&read_conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    (ctx, temp_dir)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_select_star_includes_virtual_columns() {
    let (ctx, _dir) = setup_test_table().await;

    let df = ctx
        .sql("SELECT * FROM ducklake.main.people ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // Schema should have 7 columns: id, name, filename, file_row_number, rowid, snapshot_id, file_index
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 7);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "filename");
    assert_eq!(schema.field(3).name(), "file_row_number");
    assert_eq!(schema.field(4).name(), "rowid");
    assert_eq!(schema.field(5).name(), "snapshot_id");
    assert_eq!(schema.field(6).name(), "file_index");

    // Verify filename is non-empty and ends with .parquet
    let filename_col = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..filename_col.len() {
        let val = filename_col.value(i);
        assert!(
            val.ends_with(".parquet"),
            "Expected parquet path, got: {}",
            val
        );
    }

    // Verify file_row_number is sequential starting from 0
    let row_num_col = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // After ORDER BY, row numbers may not be in order, but they should be 0, 1, 2
    let mut row_nums: Vec<i64> = (0..row_num_col.len())
        .map(|i| row_num_col.value(i))
        .collect();
    row_nums.sort();
    assert_eq!(row_nums, vec![0, 1, 2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_select_only_virtual_columns() {
    let (ctx, _dir) = setup_test_table().await;

    let df = ctx
        .sql("SELECT filename, file_row_number FROM ducklake.main.people")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "filename");
    assert_eq!(schema.field(1).name(), "file_row_number");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_select_no_virtual_columns() {
    let (ctx, _dir) = setup_test_table().await;

    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.people ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // Schema should have only 2 columns (no virtual columns)
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");

    // Verify data
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mixed_projection_virtual_and_real() {
    let (ctx, _dir) = setup_test_table().await;

    let df = ctx
        .sql("SELECT file_row_number, id FROM ducklake.main.people ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "file_row_number");
    assert_eq!(schema.field(1).name(), "id");

    // Verify ids
    let ids = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filename_column_only() {
    let (ctx, _dir) = setup_test_table().await;

    let df = ctx
        .sql("SELECT filename FROM ducklake.main.people")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "filename");

    // All filenames should be the same (all data in one file)
    let filenames = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let first = filenames.value(0);
    for i in 1..filenames.len() {
        assert_eq!(filenames.value(i), first);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_count_with_virtual_columns_in_schema() {
    let (ctx, _dir) = setup_test_table().await;

    // COUNT(*) should work even though virtual columns are in the schema
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.people")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let cnt = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(cnt.value(0), 3);
}
