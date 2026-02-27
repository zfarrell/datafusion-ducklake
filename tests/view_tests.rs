//! Integration tests for views support.
//!
//! Tests verify that views are resolved correctly via `DuckLakeSchema::table()`,
//! listed in `table_names()`, and handled by `table_exist()`.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, MetadataProvider, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter,
};

fn create_object_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

async fn create_test_env() -> (Arc<SqliteMetadataWriter>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer.set_data_path(data_path.to_str().unwrap()).unwrap();

    (Arc::new(writer), temp_dir)
}

async fn write_test_data(writer: Arc<SqliteMetadataWriter>, batches: &[RecordBatch]) {
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    table_writer
        .write_table("main", "test_table", batches)
        .await
        .unwrap();
}

async fn create_read_context(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("test", Arc::new(catalog));
    ctx
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn make_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![Arc::new(Int32Array::from(ids)), Arc::new(StringArray::from(names))],
    )
    .unwrap()
}

/// Helper to get the schema_id for "main" schema.
async fn get_main_schema_id(temp_dir: &TempDir) -> i64 {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let snapshot = provider.get_current_snapshot().unwrap();
    let schema = provider
        .get_schema_by_name("main", snapshot)
        .unwrap()
        .expect("main schema should exist");
    schema.schema_id
}

/// Helper to create a view in the catalog.
async fn create_view(temp_dir: &TempDir, view_name: &str, sql: &str) -> (i64, i64) {
    let schema_id = get_main_schema_id(temp_dir).await;
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();
    writer.create_view(schema_id, view_name, sql).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_select_all() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
    write_test_data(writer, &[batch]).await;

    // Create a view that selects all rows
    create_view(&temp_dir, "all_rows", "SELECT id, name FROM test_table").await;

    // Query the view
    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, name FROM test.main.all_rows ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "alice".to_string()));
    assert_eq!(rows[1], (2, "bob".to_string()));
    assert_eq!(rows[2], (3, "charlie".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_filter() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    // Create a view with a WHERE clause
    create_view(
        &temp_dir,
        "high_ids",
        "SELECT id, name FROM test_table WHERE id > 3",
    )
    .await;

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, name FROM test.main.high_ids ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (4, "d".to_string()));
    assert_eq!(rows[1], (5, "e".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_with_projection() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
    write_test_data(writer, &[batch]).await;

    // Create a view that only selects the name column
    create_view(&temp_dir, "names_only", "SELECT name FROM test_table").await;

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT name FROM test.main.names_only ORDER BY name")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let mut names = Vec::new();
    for batch in &batches {
        let name_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            names.push(name_col.value(i).to_string());
        }
    }
    assert_eq!(names, vec!["alice", "bob", "charlie"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_listed_in_table_names() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1], vec!["a"]);
    write_test_data(writer, &[batch]).await;

    create_view(&temp_dir, "my_view", "SELECT id FROM test_table").await;

    let ctx = create_read_context(&temp_dir).await;
    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();

    let names = schema.table_names();
    assert!(names.contains(&"test_table".to_string()));
    assert!(names.contains(&"my_view".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_exists() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1], vec!["a"]);
    write_test_data(writer, &[batch]).await;

    create_view(&temp_dir, "my_view", "SELECT id FROM test_table").await;

    let ctx = create_read_context(&temp_dir).await;
    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();

    assert!(schema.table_exist("test_table"));
    assert!(schema.table_exist("my_view"));
    assert!(!schema.table_exist("nonexistent"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_view() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2], vec!["a", "b"]);
    write_test_data(writer, &[batch]).await;

    let (view_id, _snapshot_id) =
        create_view(&temp_dir, "my_view", "SELECT id FROM test_table").await;

    // Verify the view is accessible
    let ctx = create_read_context(&temp_dir).await;
    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();
    assert!(schema.table_exist("my_view"));

    // Drop the view via writer
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let drop_writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();
    drop_writer.drop_view(view_id).unwrap();

    // Verify the view is no longer accessible (need fresh context for new snapshot)
    let ctx2 = create_read_context(&temp_dir).await;
    let catalog2 = ctx2.catalog("test").unwrap();
    let schema2 = catalog2.schema("main").unwrap();
    assert!(!schema2.table_exist("my_view"));
    assert!(schema2.table_exist("test_table")); // table still exists
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rename_view() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
    write_test_data(writer, &[batch]).await;

    let (view_id, _snapshot_id) =
        create_view(&temp_dir, "old_name", "SELECT id, name FROM test_table WHERE id > 1").await;

    // Verify the view is accessible under old name
    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, name FROM test.main.old_name ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (2, "bob".to_string()));

    // Rename the view
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let rename_writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();
    rename_writer.rename_view(view_id, "new_name").unwrap();

    // Fresh context should see the renamed view
    let ctx2 = create_read_context(&temp_dir).await;

    // New name works
    let df2 = ctx2
        .sql("SELECT id, name FROM test.main.new_name ORDER BY id")
        .await
        .unwrap();
    let batches2 = df2.collect().await.unwrap();
    let mut rows2 = Vec::new();
    for batch in &batches2 {
        let ids = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            rows2.push((ids.value(i), names.value(i).to_string()));
        }
    }
    assert_eq!(rows2.len(), 2);
    assert_eq!(rows2[0], (2, "bob".to_string()));

    // Old name should not work
    let catalog = ctx2.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();
    assert!(!schema.table_exist("old_name"));
    assert!(schema.table_exist("new_name"));
    assert!(schema.table_exist("test_table"));
}
