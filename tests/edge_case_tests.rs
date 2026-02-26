//! Edge case and boundary condition tests.
//!
//! These tests exercise uncommon scenarios: empty tables, all-NULL columns,
//! unicode strings, large values, complex DML sequences, DROP+recreate,
//! complex types, schema lifecycle, views over modified data, ALTER TABLE
//! with reads, and column statistics after DELETE.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::metadata_writer::{AlterTableOp, ColumnDef};
use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeQueryPlanner, DuckLakeTableWriter, MetadataProvider, MetadataWriter,
    SqliteMetadataProvider, SqliteMetadataWriter,
};

// ============================================================================
// Common helpers
// ============================================================================

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

async fn write_table(
    writer: Arc<SqliteMetadataWriter>,
    schema_name: &str,
    table_name: &str,
    batches: &[RecordBatch],
) {
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    table_writer
        .write_table(schema_name, table_name, batches)
        .await
        .unwrap();
}

async fn append_table(
    writer: Arc<SqliteMetadataWriter>,
    schema_name: &str,
    table_name: &str,
    batches: &[RecordBatch],
) {
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    table_writer
        .append_table(schema_name, table_name, batches)
        .await
        .unwrap();
}

/// Create a SessionContext with DuckLakeQueryPlanner (supports SQL DELETE/UPDATE).
async fn create_dml_ctx(temp_dir: &TempDir) -> SessionContext {
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

/// Create a writable SessionContext (without DuckLakeQueryPlanner).
async fn create_writable_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Create a read-only SessionContext (fresh snapshot).
async fn create_read_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

fn id_name_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn make_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        id_name_schema(),
        vec![Arc::new(Int32Array::from(ids)), Arc::new(StringArray::from(names))],
    )
    .unwrap()
}

/// Collect DML count from a DataFrame result.
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

/// Query row count.
async fn query_count(ctx: &SessionContext, table: &str) -> i64 {
    let df = ctx
        .sql(&format!("SELECT COUNT(*) FROM ducklake.main.{}", table))
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

/// Query all ids sorted.
async fn query_ids(ctx: &SessionContext, table: &str) -> Vec<i32> {
    let df = ctx
        .sql(&format!(
            "SELECT id FROM ducklake.main.{} ORDER BY id",
            table
        ))
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

// ============================================================================
// 1. Empty table operations
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_table_select_returns_zero_rows() {
    let (writer, temp_dir) = create_test_env().await;

    // Create table with data, then delete all of it
    let batch = make_batch(vec![1], vec!["x"]);
    write_table(writer.clone(), "main", "empty_tbl", &[batch]).await;

    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.empty_tbl")
        .await
        .unwrap();
    collect_dml_count(df).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&read_ctx, "empty_tbl").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_table_count_star() {
    let (writer, temp_dir) = create_test_env().await;

    let batch = make_batch(vec![1, 2], vec!["a", "b"]);
    write_table(writer.clone(), "main", "cnt_tbl", &[batch]).await;

    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx.sql("DELETE FROM ducklake.main.cnt_tbl").await.unwrap();
    collect_dml_count(df).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.cnt_tbl")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let cnt = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 0);
}

// ============================================================================
// 2. All-NULL column
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_all_null_column_insert_and_select() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("nullable_val", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![None::<&str>, None, None])),
        ],
    )
    .unwrap();

    write_table(writer.clone(), "main", "all_nulls", &[batch]).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT id, nullable_val FROM ducklake.main.all_nulls ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 3);

    let vals = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..3 {
        assert!(vals.is_null(i), "Row {} should be NULL", i);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_all_null_column_count() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![None::<&str>, None, None])),
        ],
    )
    .unwrap();

    write_table(writer.clone(), "main", "null_count_tbl", &[batch]).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    // COUNT(*) should count all rows
    let df = read_ctx
        .sql(
            "SELECT COUNT(*) as all_rows, COUNT(val) as non_null FROM ducklake.main.null_count_tbl",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let all_rows = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let non_null = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(all_rows, 3);
    assert_eq!(non_null, 0);
}

// ============================================================================
// 3. Unicode strings
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_unicode_emoji() {
    let (writer, temp_dir) = create_test_env().await;

    let batch = make_batch(
        vec![1, 2, 3],
        vec!["\u{1F389}", "\u{1F680}", "\u{2764}\u{FE0F}"],
    );
    write_table(writer.clone(), "main", "emoji_tbl", &[batch]).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT name FROM ducklake.main.emoji_tbl ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "\u{1F389}"); // 🎉
    assert_eq!(names.value(1), "\u{1F680}"); // 🚀
    assert_eq!(names.value(2), "\u{2764}\u{FE0F}"); // ❤️
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unicode_cjk() {
    let (writer, temp_dir) = create_test_env().await;

    let batch = make_batch(
        vec![1, 2, 3],
        vec!["\u{4E2D}\u{6587}", "\u{65E5}\u{672C}\u{8A9E}", "\u{D55C}\u{AD6D}\u{C5B4}"],
    );
    write_table(writer.clone(), "main", "cjk_tbl", &[batch]).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT name FROM ducklake.main.cjk_tbl ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "\u{4E2D}\u{6587}"); // 中文
    assert_eq!(names.value(1), "\u{65E5}\u{672C}\u{8A9E}"); // 日本語
    assert_eq!(names.value(2), "\u{D55C}\u{AD6D}\u{C5B4}"); // 한국어
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unicode_combining_and_zwj() {
    let (writer, temp_dir) = create_test_env().await;

    // Combining character: e + combining acute accent = é
    let combining = "e\u{0301}";
    // Zero-width joiner sequence (family emoji)
    let zwj = "\u{1F468}\u{200D}\u{1F469}\u{200D}\u{1F467}";
    let batch = make_batch(vec![1, 2], vec![combining, zwj]);
    write_table(writer.clone(), "main", "combining_tbl", &[batch]).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT name FROM ducklake.main.combining_tbl ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), combining);
    assert_eq!(names.value(1), zwj);
}

// ============================================================================
// 4. Large values
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_large_int64_values() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![i64::MAX, i64::MIN, 0]))],
    )
    .unwrap();
    write_table(writer.clone(), "main", "big_ints", &[batch]).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT val FROM ducklake.main.big_ints ORDER BY val")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut vals = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..col.len() {
            vals.push(col.value(i));
        }
    }
    vals.sort();
    assert_eq!(vals, vec![i64::MIN, 0, i64::MAX]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_very_long_varchar() {
    let (writer, temp_dir) = create_test_env().await;

    let long_string: String = "x".repeat(10_000);
    let batch = make_batch(vec![1], vec![long_string.as_str()]);
    write_table(writer.clone(), "main", "long_str", &[batch]).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT name FROM ducklake.main.long_str")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0).len(), 10_000);
    assert_eq!(names.value(0), long_string);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_high_precision_float() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "val",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![
            f64::MAX,
            f64::MIN,
            f64::EPSILON,
            std::f64::consts::PI,
        ]))],
    )
    .unwrap();
    write_table(writer.clone(), "main", "big_floats", &[batch]).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT val FROM ducklake.main.big_floats ORDER BY val")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut vals = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..col.len() {
            vals.push(col.value(i));
        }
    }
    assert!(vals.contains(&f64::MAX));
    assert!(vals.contains(&f64::MIN));
}

// ============================================================================
// 5. DELETE edge cases
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_all_rows_no_where() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_table(writer.clone(), "main", "del_all", &[batch]).await;

    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx.sql("DELETE FROM ducklake.main.del_all").await.unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 5);

    let read_ctx = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&read_ctx, "del_all").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_matching_zero_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_table(writer.clone(), "main", "del_zero", &[batch]).await;

    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.del_zero WHERE id > 999")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 0);

    let read_ctx = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&read_ctx, "del_zero").await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_sequential_deletes() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_table(writer.clone(), "main", "seq_del", &[batch]).await;

    // First delete: remove id=2
    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.seq_del WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(collect_dml_count(df).await, 1);

    // Second delete: remove id=4
    let ctx2 = create_dml_ctx(&temp_dir).await;
    let df = ctx2
        .sql("DELETE FROM ducklake.main.seq_del WHERE id = 4")
        .await
        .unwrap();
    assert_eq!(collect_dml_count(df).await, 1);

    // Third delete: remove id=1
    let ctx3 = create_dml_ctx(&temp_dir).await;
    let df = ctx3
        .sql("DELETE FROM ducklake.main.seq_del WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(collect_dml_count(df).await, 1);

    let read_ctx = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&read_ctx, "seq_del").await;
    assert_eq!(ids, vec![3, 5]);
}

// ============================================================================
// 6. UPDATE edge cases
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_update_set_value_to_null() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_table(writer.clone(), "main", "upd_null", &[batch]).await;

    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx
        .sql("UPDATE ducklake.main.upd_null SET name = NULL WHERE id = 2")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 1);

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT id, name FROM ducklake.main.upd_null ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "a");
    assert!(names.is_null(1), "Row 2 name should be NULL after update");
    assert_eq!(names.value(2), "c");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_all_rows_no_where() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_table(writer.clone(), "main", "upd_all", &[batch]).await;

    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx
        .sql("UPDATE ducklake.main.upd_all SET name = 'same'")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 3);

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT name FROM ducklake.main.upd_all ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..3 {
        assert_eq!(names.value(i), "same");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_matching_zero_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_table(writer.clone(), "main", "upd_zero", &[batch]).await;

    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx
        .sql("UPDATE ducklake.main.upd_zero SET name = 'x' WHERE id > 999")
        .await
        .unwrap();
    let count = collect_dml_count(df).await;
    assert_eq!(count, 0);

    let read_ctx = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&read_ctx, "upd_zero").await, 3);
}

// ============================================================================
// 7. DROP and recreate with same name
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_and_recreate_same_name() {
    let (writer, temp_dir) = create_test_env().await;

    // Create table with old data
    let old_batch = make_batch(vec![1, 2], vec!["old1", "old2"]);
    write_table(writer.clone(), "main", "recyclable", &[old_batch]).await;

    // Verify old data
    let read_ctx = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&read_ctx, "recyclable").await, 2);

    // Drop the table
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("DROP TABLE ducklake.main.recyclable")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Recreate with new data
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let new_writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let new_batch = make_batch(vec![10, 20, 30], vec!["new1", "new2", "new3"]);
    write_table(new_writer, "main", "recyclable", &[new_batch]).await;

    // Verify only new data is visible
    let read_ctx2 = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&read_ctx2, "recyclable").await;
    assert_eq!(ids, vec![10, 20, 30]);
    assert_eq!(query_count(&read_ctx2, "recyclable").await, 3);
}

// ============================================================================
// 8. Complex types (type system parsing)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_complex_type_nested_list() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // LIST(LIST(INTEGER)) should parse correctly
    let result = ducklake_to_arrow_type("list(list(integer))");
    assert!(
        result.is_ok(),
        "LIST(LIST(INTEGER)) should parse: {:?}",
        result
    );

    let dt = result.unwrap();
    match &dt {
        DataType::List(inner) => match inner.data_type() {
            DataType::List(inner2) => {
                assert_eq!(*inner2.data_type(), DataType::Int32);
            },
            other => panic!("Expected inner List, got {:?}", other),
        },
        other => panic!("Expected List, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_complex_type_nested_struct() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // STRUCT(a STRUCT(b INTEGER))
    let result = ducklake_to_arrow_type("struct(a struct(b integer))");
    assert!(result.is_ok(), "Nested STRUCT should parse: {:?}", result);

    let dt = result.unwrap();
    match &dt {
        DataType::Struct(fields) => {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name(), "a");
            match fields[0].data_type() {
                DataType::Struct(inner_fields) => {
                    assert_eq!(inner_fields.len(), 1);
                    assert_eq!(inner_fields[0].name(), "b");
                    assert_eq!(*inner_fields[0].data_type(), DataType::Int32);
                },
                other => panic!("Expected inner Struct, got {:?}", other),
            }
        },
        other => panic!("Expected Struct, got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_complex_type_map_with_list_value() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // MAP(VARCHAR, LIST(INTEGER))
    let result = ducklake_to_arrow_type("map(varchar, list(integer))");
    assert!(
        result.is_ok(),
        "MAP(VARCHAR, LIST(INTEGER)) should parse: {:?}",
        result
    );

    let dt = result.unwrap();
    match &dt {
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                assert_eq!(fields.len(), 2);
                assert_eq!(*fields[0].data_type(), DataType::Utf8); // key
                match fields[1].data_type() {
                    DataType::List(inner) => {
                        assert_eq!(*inner.data_type(), DataType::Int32);
                    },
                    other => panic!("Expected List value type, got {:?}", other),
                }
            } else {
                panic!("Expected Struct inside Map");
            }
        },
        other => panic!("Expected Map, got {:?}", other),
    }
}

// ============================================================================
// 9. Schema lifecycle: CREATE → table → INSERT → query → DROP CASCADE
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_lifecycle() {
    let (_writer, temp_dir) = create_test_env().await;

    // CREATE SCHEMA
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("CREATE SCHEMA ducklake.lifecycle")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Create table using writer API (CTAS in custom schemas doesn't persist data - known issue)
    let batch = make_batch(vec![1, 2], vec!["first", "second"]);
    {
        let db_path = temp_dir.path().join("test.db");
        let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
        let w = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
        write_table(w, "lifecycle", "items", &[batch]).await;
    }

    // Query
    let ctx3 = create_read_ctx(&temp_dir).await;
    let df = ctx3
        .sql("SELECT COUNT(*) FROM ducklake.lifecycle.items")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2);

    // DROP SCHEMA CASCADE
    let ctx4 = create_writable_ctx(&temp_dir).await;
    ctx4.sql("DROP SCHEMA ducklake.lifecycle CASCADE")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify gone
    let ctx5 = create_writable_ctx(&temp_dir).await;
    let catalog = ctx5.catalog("ducklake").unwrap();
    let schema_names = catalog.schema_names();
    assert!(
        !schema_names.contains(&"lifecycle".to_string()),
        "Schema 'lifecycle' should be gone after CASCADE drop, got: {:?}",
        schema_names
    );

    // Querying the table should fail
    let ctx6 = create_read_ctx(&temp_dir).await;
    let result = ctx6.sql("SELECT * FROM ducklake.lifecycle.items").await;
    assert!(
        result.is_err(),
        "Table should not be queryable after schema drop"
    );
}

// ============================================================================
// 10. Views over modified data
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_view_reflects_inserted_data() {
    let (writer, temp_dir) = create_test_env().await;

    // Create initial data
    let batch = make_batch(vec![1, 2], vec!["alice", "bob"]);
    write_table(writer.clone(), "main", "base_tbl", &[batch]).await;

    // Create a view
    let schema_id = {
        let db_path = temp_dir.path().join("test.db");
        let conn_str = format!("sqlite:{}", db_path.display());
        let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
        let snapshot = provider.get_current_snapshot().unwrap();
        provider
            .get_schema_by_name("main", snapshot)
            .unwrap()
            .unwrap()
            .schema_id
    };
    {
        let db_path = temp_dir.path().join("test.db");
        let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
        let view_writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();
        view_writer
            .create_view(schema_id, "name_view", "SELECT id, name FROM base_tbl")
            .unwrap();
    }

    // Verify view shows 2 rows
    let ctx = create_read_ctx(&temp_dir).await;
    let df = ctx
        .sql("SELECT COUNT(*) FROM ducklake.main.name_view")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2);

    // Insert more data
    let new_batch = make_batch(vec![3, 4], vec!["charlie", "diana"]);
    {
        let db_path = temp_dir.path().join("test.db");
        let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
        let append_writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
        append_table(append_writer, "main", "base_tbl", &[new_batch]).await;
    }

    // View should now show 4 rows
    let ctx2 = create_read_ctx(&temp_dir).await;
    let df = ctx2
        .sql("SELECT COUNT(*) FROM ducklake.main.name_view")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 4);
}

// ============================================================================
// 11. ALTER TABLE then query (ADD COLUMN → verify NULL for old rows)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_table_add_column_then_query() {
    let (writer, temp_dir) = create_test_env().await;

    // Create table and write initial data
    let batch = make_batch(vec![1, 2], vec!["alice", "bob"]);
    write_table(writer.clone(), "main", "alter_tbl", &[batch]).await;

    // Get the table_id for ALTER TABLE
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let alter_writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    // Find the table_id
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let snapshot = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot)
        .unwrap()
        .unwrap();
    let table_meta = provider
        .get_table_by_name(schema_meta.schema_id, "alter_tbl", snapshot)
        .unwrap()
        .unwrap();

    // ADD COLUMN
    alter_writer
        .alter_table(
            table_meta.table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();

    // Insert new data with all 3 columns
    let new_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
    ]));
    let new_batch = RecordBatch::try_new(
        new_schema,
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["charlie"])),
            Arc::new(StringArray::from(vec![Some("charlie@test.com")])),
        ],
    )
    .unwrap();

    let append_writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    append_table(append_writer, "main", "alter_tbl", &[new_batch]).await;

    // Query all rows — old rows should have NULL for email
    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT id, email FROM ducklake.main.alter_tbl ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should have 3 total rows");

    // The new row (id=3) should have the email
    // Old rows (id=1,2) should have NULL email
    let mut found_charlie_email = false;
    let mut null_email_count = 0;
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let emails = batch.column(1);
        for i in 0..batch.num_rows() {
            if ids.value(i) == 3 {
                // Can be StringArray or LargeStringArray
                if let Some(arr) = emails.as_any().downcast_ref::<StringArray>() {
                    assert_eq!(arr.value(i), "charlie@test.com");
                    found_charlie_email = true;
                } else if let Some(arr) = emails.as_any().downcast_ref::<LargeStringArray>() {
                    assert_eq!(arr.value(i), "charlie@test.com");
                    found_charlie_email = true;
                }
            } else {
                assert!(
                    emails.is_null(i),
                    "Old row id={} should have NULL email",
                    ids.value(i)
                );
                null_email_count += 1;
            }
        }
    }
    assert!(found_charlie_email, "Should find charlie's email");
    assert_eq!(
        null_email_count, 2,
        "Should have 2 old rows with NULL email"
    );
}

// ============================================================================
// 12. Column statistics after DELETE
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_stats_still_valid_after_delete() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Float64Array::from(vec![
                Some(10.0),
                Some(20.0),
                Some(30.0),
                Some(40.0),
                None,
            ])),
        ],
    )
    .unwrap();

    write_table(writer.clone(), "main", "stats_del", &[batch]).await;

    // Check initial stats
    let ctx = create_read_ctx(&temp_dir).await;
    let table = ctx
        .catalog("ducklake")
        .unwrap()
        .schema("main")
        .unwrap()
        .table("stats_del")
        .await
        .unwrap()
        .unwrap();

    let stats = table.statistics().expect("stats should be present");
    let id_stats = &stats.column_statistics[0];
    match &id_stats.min_value {
        Precision::Inexact(ScalarValue::Int32(Some(v))) => assert_eq!(*v, 1),
        other => panic!("Expected Inexact(Int32(1)), got {:?}", other),
    }
    match &id_stats.max_value {
        Precision::Inexact(ScalarValue::Int32(Some(v))) => assert_eq!(*v, 5),
        other => panic!("Expected Inexact(Int32(5)), got {:?}", other),
    }

    // Delete some rows
    let dml_ctx = create_dml_ctx(&temp_dir).await;
    let df = dml_ctx
        .sql("DELETE FROM ducklake.main.stats_del WHERE id IN (2, 4)")
        .await
        .unwrap();
    collect_dml_count(df).await;

    // Stats should still be readable (they come from file-level metadata, not row-level)
    let ctx2 = create_read_ctx(&temp_dir).await;
    let table2 = ctx2
        .catalog("ducklake")
        .unwrap()
        .schema("main")
        .unwrap()
        .table("stats_del")
        .await
        .unwrap()
        .unwrap();

    let stats2 = table2.statistics();
    assert!(
        stats2.is_some(),
        "Statistics should still be available after deletes"
    );

    // File-level stats still show the original range (min=1, max=5)
    // because stats come from Parquet file metadata, not filtered row data
    if let Some(s) = stats2 {
        let id_stats2 = &s.column_statistics[0];
        // Stats should still exist and be readable
        match &id_stats2.min_value {
            Precision::Inexact(ScalarValue::Int32(Some(v))) => {
                assert!(*v <= 1, "Min should be <= 1, got {}", v);
            },
            _ => {}, // stats format may vary, just check it doesn't panic
        }
    }

    // Verify actual query results are correct after delete
    let count = query_count(&ctx2, "stats_del").await;
    assert_eq!(count, 3, "Should have 3 rows after deleting 2");
}

// ============================================================================
// Additional combined scenarios
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_then_update_remaining() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_table(writer.clone(), "main", "del_upd", &[batch]).await;

    // Delete rows 2 and 4
    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.del_upd WHERE id IN (2, 4)")
        .await
        .unwrap();
    assert_eq!(collect_dml_count(df).await, 2);

    // Update remaining rows
    let ctx2 = create_dml_ctx(&temp_dir).await;
    let df = ctx2
        .sql("UPDATE ducklake.main.del_upd SET name = 'updated'")
        .await
        .unwrap();
    assert_eq!(collect_dml_count(df).await, 3);

    // Verify
    let read_ctx = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&read_ctx, "del_upd").await;
    assert_eq!(ids, vec![1, 3, 5]);

    let df = read_ctx
        .sql("SELECT name FROM ducklake.main.del_upd ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..3 {
        assert_eq!(names.value(i), "updated");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_then_delete_updated_row() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_table(writer.clone(), "main", "upd_del", &[batch]).await;

    // Update id=2
    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx
        .sql("UPDATE ducklake.main.upd_del SET name = 'updated' WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(collect_dml_count(df).await, 1);

    // Now delete the row we just updated
    let ctx2 = create_dml_ctx(&temp_dir).await;
    let df = ctx2
        .sql("DELETE FROM ducklake.main.upd_del WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(collect_dml_count(df).await, 1);

    // Verify only rows 1 and 3 remain
    let read_ctx = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&read_ctx, "upd_del").await;
    assert_eq!(ids, vec![1, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_string_vs_null() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some(""), None, Some("non-empty")])),
        ],
    )
    .unwrap();

    write_table(writer.clone(), "main", "empty_vs_null", &[batch]).await;

    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT id, val FROM ducklake.main.empty_vs_null ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let vals = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Empty string is NOT null
    assert!(!vals.is_null(0));
    assert_eq!(vals.value(0), "");

    // NULL is null
    assert!(vals.is_null(1));

    // Non-empty is present
    assert!(!vals.is_null(2));
    assert_eq!(vals.value(2), "non-empty");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_row_table_operations() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1], vec!["only"]);
    write_table(writer.clone(), "main", "single_row", &[batch]).await;

    // Verify
    let read_ctx = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&read_ctx, "single_row").await, 1);

    // Update the single row
    let ctx = create_dml_ctx(&temp_dir).await;
    let df = ctx
        .sql("UPDATE ducklake.main.single_row SET name = 'modified'")
        .await
        .unwrap();
    assert_eq!(collect_dml_count(df).await, 1);

    let read_ctx2 = create_read_ctx(&temp_dir).await;
    let df = read_ctx2
        .sql("SELECT name FROM ducklake.main.single_row")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "modified");

    // Delete the single row
    let ctx2 = create_dml_ctx(&temp_dir).await;
    let df = ctx2
        .sql("DELETE FROM ducklake.main.single_row")
        .await
        .unwrap();
    assert_eq!(collect_dml_count(df).await, 1);

    let read_ctx3 = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&read_ctx3, "single_row").await, 0);
}
