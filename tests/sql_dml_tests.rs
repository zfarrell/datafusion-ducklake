//! Integration tests for SQL DELETE and UPDATE via DuckLakeQueryPlanner.
//!
//! These tests verify that `DELETE FROM table WHERE ...` and `UPDATE table SET ...`
//! work correctly through DataFusion's standard SQL interface when using
//! `DuckLakeQueryPlanner`.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeQueryPlanner, DuckLakeTableWriter, MetadataWriter,
    SqliteMetadataProvider, SqliteMetadataWriter,
};

fn create_object_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(LocalFileSystem::new())
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
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

/// Create test environment: init DB, write test data, return temp dir.
async fn setup_test_data(batches: &[RecordBatch]) -> TempDir {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer.set_data_path(data_path.to_str().unwrap()).unwrap();

    let object_store = create_object_store();
    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "test_table", batches)
        .await
        .unwrap();

    temp_dir
}

/// Create a SessionContext with DuckLakeQueryPlanner and a writable catalog.
async fn create_ctx_with_planner(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(DuckLakeQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Create a read-only SessionContext (fresh catalog to see updated data).
async fn create_read_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Helper: collect count from a DML result DataFrame.
async fn collect_dml_count(df: DataFrame) -> u64 {
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0)
}

/// Helper: query all ids sorted.
async fn query_ids(ctx: &SessionContext) -> Vec<i32> {
    let df = ctx
        .sql("SELECT id FROM ducklake.main.test_table ORDER BY id")
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

/// Helper: query all names sorted by id.
async fn query_names(ctx: &SessionContext) -> Vec<String> {
    let df = ctx
        .sql("SELECT name FROM ducklake.main.test_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut names = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..col.len() {
            names.push(col.value(i).to_string());
        }
    }
    names
}

/// Helper: query row count.
async fn query_count(ctx: &SessionContext) -> i64 {
    let df = ctx
        .sql("SELECT COUNT(*) FROM ducklake.main.test_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

// ============================================================================
// DELETE tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_sql_delete_with_where() {
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    let temp_dir = setup_test_data(&[batch]).await;

    let ctx = create_ctx_with_planner(&temp_dir).await;

    // DELETE via SQL
    let df = ctx
        .sql("DELETE FROM ducklake.main.test_table WHERE id > 3")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 2, "Should have deleted 2 rows (id=4 and id=5)");

    // Verify with fresh read context
    let read_ctx = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&read_ctx).await;
    assert_eq!(ids, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sql_delete_all_rows() {
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    let temp_dir = setup_test_data(&[batch]).await;

    let ctx = create_ctx_with_planner(&temp_dir).await;

    // DELETE all rows (no WHERE clause)
    let df = ctx
        .sql("DELETE FROM ducklake.main.test_table")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 3, "Should have deleted all 3 rows");

    // Verify empty table
    let read_ctx = create_read_ctx(&temp_dir).await;
    let cnt = query_count(&read_ctx).await;
    assert_eq!(cnt, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sql_delete_no_matching_rows() {
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    let temp_dir = setup_test_data(&[batch]).await;

    let ctx = create_ctx_with_planner(&temp_dir).await;

    // DELETE with no matching rows
    let df = ctx
        .sql("DELETE FROM ducklake.main.test_table WHERE id > 100")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 0, "No rows should be deleted");

    // Verify all rows remain
    let read_ctx = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&read_ctx).await;
    assert_eq!(ids, vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sql_delete_with_equality() {
    let batch = make_batch(vec![1, 2, 3], vec!["Alice", "Bob", "Charlie"]);
    let temp_dir = setup_test_data(&[batch]).await;

    let ctx = create_ctx_with_planner(&temp_dir).await;

    // DELETE a single row by equality
    let df = ctx
        .sql("DELETE FROM ducklake.main.test_table WHERE name = 'Bob'")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1);

    let read_ctx = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&read_ctx).await;
    assert_eq!(ids, vec![1, 3]);
}

// ============================================================================
// UPDATE tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_sql_update_single_column() {
    let batch = make_batch(vec![1, 2, 3], vec!["Alice", "Bob", "Charlie"]);
    let temp_dir = setup_test_data(&[batch]).await;

    let ctx = create_ctx_with_planner(&temp_dir).await;

    // UPDATE via SQL
    let df = ctx
        .sql("UPDATE ducklake.main.test_table SET name = 'Updated' WHERE id = 2")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1, "Should have updated 1 row");

    // Verify with fresh read context
    let read_ctx = create_read_ctx(&temp_dir).await;
    let names = query_names(&read_ctx).await;
    assert_eq!(names, vec!["Alice", "Updated", "Charlie"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sql_update_all_rows() {
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    let temp_dir = setup_test_data(&[batch]).await;

    let ctx = create_ctx_with_planner(&temp_dir).await;

    // UPDATE all rows (no WHERE)
    let df = ctx
        .sql("UPDATE ducklake.main.test_table SET name = 'same'")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 3, "Should have updated all 3 rows");

    let read_ctx = create_read_ctx(&temp_dir).await;
    let names = query_names(&read_ctx).await;
    assert_eq!(names, vec!["same", "same", "same"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sql_update_with_expression() {
    // Use integer values to test expression-based updates
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer.set_data_path(data_path.to_str().unwrap()).unwrap();

    let object_store = create_object_store();
    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "test_table", &[batch])
        .await
        .unwrap();

    let ctx = create_ctx_with_planner(&temp_dir).await;

    // UPDATE with expression: value = value * 2
    let df = ctx
        .sql("UPDATE ducklake.main.test_table SET value = value * 2 WHERE id >= 2")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 2, "Should have updated 2 rows");

    // Verify
    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT id, value FROM ducklake.main.test_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let mut results: Vec<(i32, i32)> = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let vals = batch.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..batch.num_rows() {
            results.push((ids.value(i), vals.value(i)));
        }
    }
    assert_eq!(results, vec![(1, 10), (2, 40), (3, 60)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sql_update_no_matching_rows() {
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    let temp_dir = setup_test_data(&[batch]).await;

    let ctx = create_ctx_with_planner(&temp_dir).await;

    // UPDATE with no matching rows
    let df = ctx
        .sql("UPDATE ducklake.main.test_table SET name = 'x' WHERE id > 100")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 0, "No rows should be updated");

    let read_ctx = create_read_ctx(&temp_dir).await;
    let names = query_names(&read_ctx).await;
    assert_eq!(names, vec!["a", "b", "c"]);
}

// ============================================================================
// Combined flow tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_insert_then_delete_via_sql() {
    let batch = make_batch(vec![1, 2], vec!["a", "b"]);
    let temp_dir = setup_test_data(&[batch]).await;

    // First: INSERT via SQL (this uses the standard DataFusion path)
    let ctx = create_ctx_with_planner(&temp_dir).await;

    let insert_batch = make_batch(vec![3, 4], vec!["c", "d"]);
    ctx.register_batch("insert_source", insert_batch).unwrap();

    let df = ctx
        .sql("INSERT INTO ducklake.main.test_table SELECT * FROM insert_source")
        .await
        .unwrap();
    let _ = df.collect().await.unwrap();

    // Re-create context (fresh catalog sees new data)
    let ctx2 = create_ctx_with_planner(&temp_dir).await;

    // Verify 4 rows
    let read_ctx = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&read_ctx).await, 4);

    // DELETE row via SQL
    let df = ctx2
        .sql("DELETE FROM ducklake.main.test_table WHERE id = 3")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1);

    // Verify
    let read_ctx2 = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&read_ctx2).await;
    assert_eq!(ids, vec![1, 2, 4]);
}

// ============================================================================
// Verify planner fallback (normal queries still work)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_select_works_with_custom_planner() {
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    let temp_dir = setup_test_data(&[batch]).await;

    let ctx = create_ctx_with_planner(&temp_dir).await;

    // Normal SELECT should work fine through the custom planner
    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.test_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}
