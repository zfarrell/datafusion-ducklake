//! Integration tests for DELETE support.
//!
//! Tests verify that `DuckLakeTable::delete()` correctly writes delete files
//! and that subsequent reads reflect the deletions.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTable, DuckLakeTableWriter, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter,
};

/// Create a local filesystem object store
fn create_object_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

/// Helper to create a test environment with writer and data directory.
/// Returns the writer, temp directory, and connection string.
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

/// Write test data: a simple (id, name) table with the given rows.
async fn write_test_data(writer: Arc<SqliteMetadataWriter>, batches: &[RecordBatch]) {
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    table_writer
        .write_table("main", "test_table", batches)
        .await
        .unwrap();
}

/// Create a writable SessionContext for the test environment.
async fn create_writable_context(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("test", Arc::new(catalog));
    ctx
}

/// Create a read-only SessionContext for the test environment.
async fn create_read_context(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("test", Arc::new(catalog));
    ctx
}

/// Helper to make a simple (id INT32, name UTF8) schema.
fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Helper to make a RecordBatch with (id, name) data.
fn make_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![Arc::new(Int32Array::from(ids)), Arc::new(StringArray::from(names))],
    )
    .unwrap()
}

/// Execute a delete on the table and return the count of deleted rows.
async fn execute_delete(ctx: &SessionContext, filters: &[Expr]) -> u64 {
    use datafusion::execution::SendableRecordBatchStream;
    use futures::StreamExt;

    // Get the table provider
    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();
    let table = schema.table("test_table").await.unwrap().unwrap();

    // Downcast to DuckLakeTable
    let ducklake_table = table
        .as_any()
        .downcast_ref::<DuckLakeTable>()
        .expect("Expected DuckLakeTable");

    // Create delete plan
    let state = ctx.state();
    let plan = ducklake_table.delete(&state, filters).await.unwrap();

    // Execute
    let task_ctx = ctx.task_ctx();
    let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx).unwrap();

    let mut total_deleted = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        total_deleted += count_col.value(0);
    }
    total_deleted
}

/// Helper to query the count of rows in the table.
async fn query_count(ctx: &SessionContext) -> i64 {
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM test.main.test_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0)
}

/// Helper to query all ids in sorted order.
async fn query_ids(ctx: &SessionContext) -> Vec<i32> {
    let df = ctx
        .sql("SELECT id FROM test.main.test_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_with_where_clause() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    // Delete rows where id > 3
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").gt(lit(3))]).await;
    assert_eq!(deleted, 2, "Should have deleted 2 rows (id=4 and id=5)");

    // Read back with a fresh context to see the deletes
    let ctx = create_read_context(&temp_dir).await;
    let ids = query_ids(&ctx).await;
    assert_eq!(ids, vec![1, 2, 3], "Only ids 1,2,3 should remain");
    assert_eq!(query_count(&ctx).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_all_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // Delete all rows (no filter)
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[]).await;
    assert_eq!(deleted, 3, "Should have deleted all 3 rows");

    // Read back
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 0, "Table should be empty");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_no_matching_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // Delete rows where id > 100 (no matches)
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").gt(lit(100))]).await;
    assert_eq!(deleted, 0, "Should have deleted 0 rows");

    // Read back - all rows should still exist
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 3, "All 3 rows should remain");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_then_select_reflects_deletion() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4], vec!["alice", "bob", "charlie", "diana"]);
    write_test_data(writer, &[batch]).await;

    // Verify initial state
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 4);

    // Delete bob (id=2) and charlie (id=3)
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(
        &ctx,
        &[col("id").gt_eq(lit(2)).and(col("id").lt_eq(lit(3)))],
    )
    .await;
    assert_eq!(deleted, 2);

    // Verify remaining rows
    let ctx = create_read_context(&temp_dir).await;
    let ids = query_ids(&ctx).await;
    assert_eq!(ids, vec![1, 4]);

    // Verify names match
    let df = ctx
        .sql("SELECT name FROM test.main.test_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "diana");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_with_existing_deletes() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    // First delete: remove id=2
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").eq(lit(2))]).await;
    assert_eq!(deleted, 1);

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_ids(&ctx).await, vec![1, 3, 4, 5]);

    // Second delete: remove id=4 (on a table that already has a delete file)
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").eq(lit(4))]).await;
    assert_eq!(deleted, 1);

    // Verify both deletions are reflected
    let ctx = create_read_context(&temp_dir).await;
    let ids = query_ids(&ctx).await;
    assert_eq!(ids, vec![1, 3, 5]);
    assert_eq!(query_count(&ctx).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_with_string_filter() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["keep", "remove", "keep"]);
    write_test_data(writer, &[batch]).await;

    // Delete rows where name = 'remove'
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("name").eq(lit("remove"))]).await;
    assert_eq!(deleted, 1);

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    let ids = query_ids(&ctx).await;
    assert_eq!(ids, vec![1, 3]);
}
