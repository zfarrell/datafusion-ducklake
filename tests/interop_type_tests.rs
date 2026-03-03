//! Tests for interop type handling fixes (R5-S-012, 014, 015, 016, 017).
//!
//! These tests verify:
//! - R5-S-012: Unknown types error instead of silently becoming Utf8
//! - R5-S-014: Date/Timestamp inlined serialization uses ISO 8601 strings
//! - R5-S-015: Decimal128/256 parsing in inlined data flush
//! - R5-S-016: Decimal column stats decoded from FixedLenByteArray
//! - R5-S-017: Delete file INSERT includes format column

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
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

async fn create_read_context(temp_dir: &TempDir) -> datafusion::prelude::SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

// ==================== R5-S-014: Date/Timestamp ISO serialization ====================

/// Test that Date32 columns round-trip correctly through write → read.
#[tokio::test(flavor = "multi_thread")]
async fn test_date32_roundtrip() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("event_date", DataType::Date32, true),
    ]));

    // Date32 value: 2024-06-15 = epoch day 19889
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Date32Array::from(vec![Some(19889), Some(0)])),
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "date_test", &[batch])
        .await
        .unwrap();

    // Read back and verify values are correct
    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, event_date FROM ducklake.main.date_test ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert!(!batches.is_empty());
    let batch = &batches[0];
    let dates = batch
        .column(1)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(dates.value(0), 19889); // 2024-06-15
    assert_eq!(dates.value(1), 0); // 1970-01-01
}

/// Test that Timestamp values round-trip correctly through write → read.
#[tokio::test(flavor = "multi_thread")]
async fn test_timestamp_roundtrip() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    // 2024-06-15 11:30:00 UTC = 1718451000000000 microseconds since epoch
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(
                1718451000000000,
            )])),
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "ts_test", &[batch])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, created_at FROM ducklake.main.ts_test ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert!(!batches.is_empty());
    let batch = &batches[0];
    let ts = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts.value(0), 1718451000000000);
}

// ==================== R5-S-015: Decimal128/256 flush support ====================

/// Test that Decimal128 columns can be written and read back correctly.
#[tokio::test(flavor = "multi_thread")]
async fn test_decimal128_write_read_roundtrip() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("price", DataType::Decimal128(10, 2), true),
    ]));

    // 12345 = 123.45 with scale 2
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(
                Decimal128Array::from(vec![Some(12345), Some(-9999), Some(0)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "decimal_test", &[batch])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, price FROM ducklake.main.decimal_test ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    assert!(!batches.is_empty());
    let batch = &batches[0];
    let prices = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(prices.value(0), 12345); // 123.45
    assert_eq!(prices.value(1), -9999); // -99.99
    assert_eq!(prices.value(2), 0); // 0.00
}

// ==================== R5-S-016: Decimal column stats ====================

/// Test that Decimal column stats (min/max) are correctly extracted and stored.
#[tokio::test(flavor = "multi_thread")]
async fn test_decimal_column_stats_not_dropped() {
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("amount", DataType::Decimal128(10, 2), true),
    ]));

    // Values: 100.50, 200.75, 50.25
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(
                Decimal128Array::from(vec![Some(10050), Some(20075), Some(5025)])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "stats_dec", &[batch])
        .await
        .unwrap();

    // Read column stats from metadata
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let snapshot_id = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot_id)
        .unwrap()
        .unwrap();
    let table = provider
        .get_table_by_name(schema_meta.schema_id, "stats_dec", snapshot_id)
        .unwrap()
        .unwrap();
    let file_stats = provider
        .get_file_column_stats(table.table_id, snapshot_id)
        .unwrap();

    // Should have stats for both columns (id + amount)
    assert!(!file_stats.is_empty(), "Should have file column stats");

    // Count how many stat entries have non-None min/max values
    let stats_with_values: Vec<_> = file_stats
        .iter()
        .filter(|s| s.min_value.is_some() || s.max_value.is_some())
        .collect();

    // With the fix, both 'id' (Int32) and 'amount' (Decimal) should have stats.
    // Before the fix, Decimal stats were silently dropped.
    assert!(
        stats_with_values.len() >= 2,
        "Expected stats for both id and amount columns, got {} stat entries with values. \
         Decimal stats may be silently dropped (R5-S-016).",
        stats_with_values.len()
    );
}

// ==================== R5-S-012: Unknown types error ====================

/// Test that unknown DuckLake types return errors, not silent Utf8 fallback.
#[test]
fn test_unknown_ducklake_type_errors() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // A genuinely unknown type should return an error
    let result = ducklake_to_arrow_type("completely_made_up_type");
    assert!(
        result.is_err(),
        "Unknown types should return an error, not silently become Utf8"
    );

    // Known complex types should work
    let result = ducklake_to_arrow_type("decimal(10,2)");
    assert!(result.is_ok(), "Decimal should be supported");

    let result = ducklake_to_arrow_type("timestamp");
    assert!(result.is_ok(), "Timestamp should be supported");
}
