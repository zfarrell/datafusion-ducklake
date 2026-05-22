//! Integration tests for UPDATE support.
//!
//! Tests verify that `DuckLakeTable::update()` correctly writes delete files
//! for old rows and new data files with updated values, and that subsequent
//! reads reflect the updates.

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
    SqliteMetadataWriter, UpdateAssignment,
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

/// Execute an update on the table and return the count of updated rows.
async fn execute_update(
    ctx: &SessionContext,
    assignments: Vec<UpdateAssignment>,
    filters: &[Expr],
) -> u64 {
    use datafusion::execution::SendableRecordBatchStream;
    use futures::StreamExt;

    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();
    let table = schema.table("test_table").await.unwrap().unwrap();

    let ducklake_table = table
        .as_any()
        .downcast_ref::<DuckLakeTable>()
        .expect("Expected DuckLakeTable");

    let state = ctx.state();
    let plan = ducklake_table
        .update(&state, assignments, filters)
        .await
        .unwrap();

    let task_ctx = ctx.task_ctx();
    let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx).unwrap();

    let mut total_updated = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        total_updated += count_col.value(0);
    }
    total_updated
}

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

async fn query_rows(ctx: &SessionContext) -> Vec<(i32, String)> {
    let df = ctx
        .sql("SELECT id, name FROM test.main.test_table ORDER BY id")
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
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_with_literal_value() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE test_table SET name = 'updated' WHERE id = 2
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1, // name column
            expr: lit("updated"),
        }],
        &[col("id").eq(lit(2))],
    )
    .await;
    assert_eq!(updated, 1);

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "alice".to_string()));
    assert_eq!(rows[1], (2, "updated".to_string()));
    assert_eq!(rows[2], (3, "charlie".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_multiple_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE test_table SET name = 'X' WHERE id > 3
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("X"),
        }],
        &[col("id").gt(lit(3))],
    )
    .await;
    assert_eq!(updated, 2);

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[1], (2, "b".to_string()));
    assert_eq!(rows[2], (3, "c".to_string()));
    assert_eq!(rows[3], (4, "X".to_string()));
    assert_eq!(rows[4], (5, "X".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_all_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE test_table SET name = 'all' (no WHERE)
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("all"),
        }],
        &[],
    )
    .await;
    assert_eq!(updated, 3);

    // Verify - all names changed but ids preserved
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "all".to_string()));
    assert_eq!(rows[1], (2, "all".to_string()));
    assert_eq!(rows[2], (3, "all".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_no_matching_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE test_table SET name = 'X' WHERE id > 100
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("X"),
        }],
        &[col("id").gt(lit(100))],
    )
    .await;
    assert_eq!(updated, 0);

    // Verify - nothing changed
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 3);
    let rows = query_rows(&ctx).await;
    assert_eq!(rows[0], (1, "a".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_preserves_row_count() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4], vec!["a", "b", "c", "d"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE should not change row count
    let ctx = create_writable_context(&temp_dir).await;
    execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("new"),
        }],
        &[col("id").lt_eq(lit(2))],
    )
    .await;

    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_with_existing_deletes() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    // First: delete id=3
    {
        use datafusion::execution::SendableRecordBatchStream;
        use futures::StreamExt;

        let ctx = create_writable_context(&temp_dir).await;
        let catalog = ctx.catalog("test").unwrap();
        let schema = catalog.schema("main").unwrap();
        let table = schema.table("test_table").await.unwrap().unwrap();
        let ducklake_table = table.as_any().downcast_ref::<DuckLakeTable>().unwrap();
        let state = ctx.state();
        let plan = ducklake_table
            .delete(&state, &[col("id").eq(lit(3))])
            .await
            .unwrap();
        let task_ctx = ctx.task_ctx();
        let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx).unwrap();
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
    }

    // Now update: SET name = 'X' WHERE id >= 2
    // id=3 is deleted, so only ids 2, 4, 5 should be updated
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("X"),
        }],
        &[col("id").gt_eq(lit(2))],
    )
    .await;
    assert_eq!(
        updated, 3,
        "Should update ids 2, 4, 5 (not 3, which is deleted)"
    );

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 4); // 1, 2, 4, 5 (3 was deleted)
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[1], (2, "X".to_string()));
    assert_eq!(rows[2], (4, "X".to_string()));
    assert_eq!(rows[3], (5, "X".to_string()));
}
