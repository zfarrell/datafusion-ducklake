//! Integration tests for column statistics write and read.
//!
//! Tests verify that per-column statistics (null_count, min, max) are:
//! 1. Written to `ducklake_file_column_stats` during INSERT
//! 2. Exposed via `TableProvider::statistics()` for query optimization

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
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

async fn create_read_context(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stats_written_to_metadata() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("alice"),
                None,
                Some("charlie"),
            ])),
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "stats_table", &[batch])
        .await
        .unwrap();

    // Read stats directly from the metadata provider
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let snapshot_id = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot_id)
        .unwrap()
        .unwrap();
    let table = provider
        .get_table_by_name(schema_meta.schema_id, "stats_table", snapshot_id)
        .unwrap()
        .unwrap();
    let file_stats = provider
        .get_file_column_stats(table.table_id, snapshot_id)
        .unwrap();

    // Should have stats for both columns
    assert!(
        file_stats.len() >= 2,
        "Expected at least 2 column stats, got {}",
        file_stats.len()
    );

    // Find id column stats
    let id_stats: Vec<_> = file_stats
        .iter()
        .filter(|s| s.column_name == "id")
        .collect();
    assert_eq!(id_stats.len(), 1);
    assert_eq!(id_stats[0].null_count, Some(0));
    assert_eq!(id_stats[0].min_value.as_deref(), Some("1"));
    assert_eq!(id_stats[0].max_value.as_deref(), Some("3"));

    // Find name column stats — has 1 null
    let name_stats: Vec<_> = file_stats
        .iter()
        .filter(|s| s.column_name == "name")
        .collect();
    assert_eq!(name_stats.len(), 1);
    assert_eq!(name_stats[0].null_count, Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_statistics_exposed_via_table_provider() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])),
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "provider_test", &[batch])
        .await
        .unwrap();

    // Get the table via catalog and check statistics
    let ctx = create_read_context(&temp_dir).await;
    let table = ctx
        .catalog("ducklake")
        .unwrap()
        .schema("main")
        .unwrap()
        .table("provider_test")
        .await
        .unwrap()
        .unwrap();

    let stats = table.statistics().expect("statistics should be present");

    // Check id column (index 0): min=10, max=30, null_count=0
    let id_stats = &stats.column_statistics[0];
    match &id_stats.min_value {
        Precision::Inexact(ScalarValue::Int32(Some(v))) => assert_eq!(*v, 10),
        other => panic!("Expected Inexact(Int32(10)), got {:?}", other),
    }
    match &id_stats.max_value {
        Precision::Inexact(ScalarValue::Int32(Some(v))) => assert_eq!(*v, 30),
        other => panic!("Expected Inexact(Int32(30)), got {:?}", other),
    }
    match &id_stats.null_count {
        Precision::Inexact(v) => assert_eq!(*v, 0),
        other => panic!("Expected Inexact(0), got {:?}", other),
    }

    // Check value column (index 1): has 1 null
    let val_stats = &stats.column_statistics[1];
    match &val_stats.null_count {
        Precision::Inexact(v) => assert_eq!(*v, 1),
        other => panic!("Expected Inexact(1), got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stats_with_multiple_appends() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));

    // First write: values 1, 2, 3
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
    table_writer
        .write_table("main", "multi_test", &[batch1])
        .await
        .unwrap();

    // Second write (append): values 10, 20
    let batch2 =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![10, 20]))]).unwrap();

    let table_writer2 = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer2
        .append_table("main", "multi_test", &[batch2])
        .await
        .unwrap();

    // Statistics should aggregate across both files
    let ctx = create_read_context(&temp_dir).await;
    let table = ctx
        .catalog("ducklake")
        .unwrap()
        .schema("main")
        .unwrap()
        .table("multi_test")
        .await
        .unwrap()
        .unwrap();

    let stats = table.statistics().expect("statistics should be present");
    let col_stats = &stats.column_statistics[0];

    // Overall min = 1, overall max = 20
    match &col_stats.min_value {
        Precision::Inexact(ScalarValue::Int32(Some(v))) => assert_eq!(*v, 1),
        other => panic!("Expected Inexact(Int32(1)), got {:?}", other),
    }
    match &col_stats.max_value {
        Precision::Inexact(ScalarValue::Int32(Some(v))) => assert_eq!(*v, 20),
        other => panic!("Expected Inexact(Int32(20)), got {:?}", other),
    }
    // Null count sum = 0
    match &col_stats.null_count {
        Precision::Inexact(v) => assert_eq!(*v, 0),
        other => panic!("Expected Inexact(0), got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stats_with_string_columns() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["banana", "apple", "cherry"]))],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "str_test", &[batch])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let table = ctx
        .catalog("ducklake")
        .unwrap()
        .schema("main")
        .unwrap()
        .table("str_test")
        .await
        .unwrap()
        .unwrap();

    let stats = table.statistics().expect("statistics should be present");
    let col_stats = &stats.column_statistics[0];

    match &col_stats.min_value {
        Precision::Inexact(ScalarValue::Utf8(Some(v))) => assert_eq!(v, "apple"),
        other => panic!("Expected Inexact(Utf8('apple')), got {:?}", other),
    }
    match &col_stats.max_value {
        Precision::Inexact(ScalarValue::Utf8(Some(v))) => assert_eq!(v, "cherry"),
        other => panic!("Expected Inexact(Utf8('cherry')), got {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_no_stats_returns_none() {
    let (writer, temp_dir) = create_test_env().await;

    // Create a table via SQL that has no data files → no stats
    writer
        .begin_write_transaction(
            "main",
            "empty_table",
            &[],
            datafusion_ducklake::WriteMode::Replace,
        )
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let table = ctx
        .catalog("ducklake")
        .unwrap()
        .schema("main")
        .unwrap()
        .table("empty_table")
        .await
        .unwrap()
        .unwrap();

    // No files = no stats = None
    assert!(table.statistics().is_none());
}
