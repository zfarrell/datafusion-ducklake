//! Reproduction tests for DuckLake type system issues.
//!
//! Each test corresponds to a specific GitHub issue and verifies that
//! our DataFusion-DuckLake extension handles the described scenario correctly.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, ListArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::types::{arrow_to_ducklake_type, ducklake_to_arrow_type};
use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter,
};

// ============================================================================
// Common helpers
// ============================================================================

fn create_object_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

async fn create_test_env() -> (SqliteMetadataWriter, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer.set_data_path(data_path.to_str().unwrap()).unwrap();

    (writer, temp_dir)
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

// ============================================================================
// Issue #44: JSON Type not supported
// https://github.com/duckdb/ducklake/issues/44
//
// DuckLake did not support JSON data types, causing "Unsupported user-defined
// type" errors. Our extension maps JSON → Utf8 (Arrow string).
// ============================================================================

#[test]
fn test_issue_44_json_type_mapping() {
    // Verify our type mapper handles the "json" type
    let result = ducklake_to_arrow_type("json");
    assert!(result.is_ok(), "JSON type should be supported");
    assert_eq!(result.unwrap(), DataType::Utf8, "JSON should map to Utf8");

    // Also verify JSON roundtrip is not lossy (it becomes varchar on the way back)
    let arrow_type = ducklake_to_arrow_type("json").unwrap();
    let back = arrow_to_ducklake_type(&arrow_type).unwrap();
    // JSON maps to Utf8, which maps back to "varchar" - this is acceptable
    assert_eq!(back, "varchar");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_44_json_column_read() {
    // Test that a table with JSON-like string data can be written and read
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Utf8, true), // JSON stored as Utf8
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![
                Some(r#"{"key": "value"}"#),
                Some(r#"{"nested": {"a": 1}}"#),
            ])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "json_test", &[batch])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT * FROM test.main.json_test ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    let data_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(data_col.value(0), r#"{"key": "value"}"#);
}

// ============================================================================
// Issue #229: ducklake_expire_snapshots fails with SQLite metadata due to
// TIMESTAMPTZ/VARCHAR type mismatch
// https://github.com/duckdb/ducklake/issues/229
//
// The snapshot_time column is stored as VARCHAR in SQLite instead of
// TIMESTAMPTZ. This is a DuckLake catalog maintenance issue. We test that
// our type mapper handles both timestamp representations.
// ============================================================================

#[test]
fn test_issue_229_timestamptz_type_mapping() {
    // Verify our type mapper handles timestamptz correctly
    let result = ducklake_to_arrow_type("timestamptz");
    assert!(result.is_ok(), "TIMESTAMPTZ type should be supported");
    assert_eq!(
        result.unwrap(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "TIMESTAMPTZ should map to Timestamp with UTC timezone"
    );

    // Also verify "timestamp with time zone" alias
    let result2 = ducklake_to_arrow_type("timestamp with time zone");
    assert!(
        result2.is_ok(),
        "TIMESTAMP WITH TIME ZONE should be supported"
    );
    assert_eq!(
        result2.unwrap(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );

    // VARCHAR should also work (since SQLite stores snapshot_time as VARCHAR)
    let varchar_result = ducklake_to_arrow_type("varchar");
    assert!(varchar_result.is_ok());
}

// ============================================================================
// Issue #479: Default column values cause "Invalid Error: Unknown exception"
// https://github.com/duckdb/ducklake/issues/479
//
// Creating a table with DEFAULT FALSE on a boolean column caused an internal
// error in DuckLake. This is a DuckLake write-path issue. We verify our
// extension can read tables with boolean columns correctly.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_479_boolean_default_column_read() {
    // Test that boolean columns (the type involved in the DEFAULT issue) work
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("x", DataType::Float64, true),
        Field::new("y", DataType::Float64, true),
        Field::new("is_deleted", DataType::Boolean, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                Some(1000000),
                Some(2000000),
            ])),
            Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])),
            Arc::new(Float64Array::from(vec![Some(3.0), Some(4.0)])),
            Arc::new(arrow::array::BooleanArray::from(vec![
                Some(false),
                Some(false),
            ])),
        ],
    )
    .unwrap();

    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "timeseries", &[batch])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT * FROM test.main.timeseries")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    // Verify boolean column reads correctly
    let bool_col = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(!bool_col.value(0));
    assert!(!bool_col.value(1));
}

#[test]
fn test_issue_479_boolean_type_mapping() {
    // Verify boolean type mapping works in both directions
    let arrow_type = ducklake_to_arrow_type("boolean").unwrap();
    assert_eq!(arrow_type, DataType::Boolean);

    let back = arrow_to_ducklake_type(&DataType::Boolean).unwrap();
    assert_eq!(back, "boolean");

    // Also check "bool" alias
    let bool_alias = ducklake_to_arrow_type("bool").unwrap();
    assert_eq!(bool_alias, DataType::Boolean);
}

// ============================================================================
// Issue #517: Adding new JSON column via ALTER TABLE fails
// https://github.com/duckdb/ducklake/issues/517
//
// ALTER TABLE ADD COLUMN with JSON type failed with "Unsupported user-defined
// type". This is a DuckLake write issue. We verify that our extension can
// handle tables where JSON columns might have been added later (nullable,
// with NULLs in older data).
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_517_json_column_with_nulls() {
    // Simulate a scenario where a JSON column was added later (has NULLs)
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("metadata_json", DataType::Utf8, true), // JSON stored as Utf8
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Charlie"),
            ])),
            Arc::new(StringArray::from(vec![
                Some(r#"{"role": "admin"}"#),
                None, // NULL - simulates row without JSON data
                Some(r#"{"role": "user"}"#),
            ])),
        ],
    )
    .unwrap();

    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "users_with_json", &[batch])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, metadata_json FROM test.main.users_with_json ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);

    let json_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(json_col.value(0), r#"{"role": "admin"}"#);
    assert!(json_col.is_null(1)); // NULL for row without JSON
    assert_eq!(json_col.value(2), r#"{"role": "user"}"#);
}

// ============================================================================
// Issue #534: ENUM columns are not supported and not graceful
// https://github.com/duckdb/ducklake/issues/534
//
// ENUM types are not supported by DuckLake. Users must cast ENUMs to VARCHAR.
// Our type mapper should return an error for ENUM types (since DuckLake stores
// them as VARCHAR after casting, we'd never see the raw ENUM in metadata).
// ============================================================================

#[test]
fn test_issue_534_enum_type_unsupported() {
    // DuckLake doesn't support ENUM types - they should be cast to VARCHAR.
    // If we ever encounter an ENUM type string in metadata, it should error.
    let result = ducklake_to_arrow_type("ENUM('forward', 'reverse')");
    assert!(
        result.is_err(),
        "ENUM type should not be directly supported"
    );

    // However, the expected workaround (VARCHAR) should work fine
    let varchar = ducklake_to_arrow_type("varchar").unwrap();
    assert_eq!(varchar, DataType::Utf8);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_534_enum_as_varchar_read() {
    // Test that ENUM data stored as VARCHAR (the workaround) reads correctly
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("direction", DataType::Utf8, true), // ENUM cast to VARCHAR
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("forward"),
                Some("reverse"),
                Some("forward"),
            ])),
        ],
    )
    .unwrap();

    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "ship", &[batch])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT direction FROM test.main.ship ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);

    let dir_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(dir_col.value(0), "forward");
    assert_eq!(dir_col.value(1), "reverse");
    assert_eq!(dir_col.value(2), "forward");
}

// ============================================================================
// Issue #591: Data inlining fails with PostgreSQL metadata catalog when
// creating a table with complex types (structs, maps, arrays)
// https://github.com/duckdb/ducklake/issues/591
//
// Complex types (STRUCT, MAP, ARRAY) caused failures with PostgreSQL metadata
// due to unnamed composite types. We verify that our type mapper handles
// complex types correctly for reading.
// ============================================================================

#[test]
fn test_issue_591_complex_type_mapping() {
    // Verify our type mapper handles complex types that caused issues with Postgres

    // STRUCT type
    let struct_result = ducklake_to_arrow_type("struct(name varchar, age int32)");
    assert!(
        struct_result.is_ok(),
        "STRUCT type should be supported: {:?}",
        struct_result.err()
    );
    if let DataType::Struct(fields) = struct_result.unwrap() {
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "name");
        assert_eq!(fields[1].name(), "age");
    } else {
        panic!("Expected Struct type");
    }

    // MAP type
    let map_result = ducklake_to_arrow_type("map(varchar, int32)");
    assert!(
        map_result.is_ok(),
        "MAP type should be supported: {:?}",
        map_result.err()
    );

    // ARRAY / LIST type
    let list_result = ducklake_to_arrow_type("list(int32)");
    assert!(
        list_result.is_ok(),
        "LIST type should be supported: {:?}",
        list_result.err()
    );

    // Nested: struct containing a list
    let nested_result =
        ducklake_to_arrow_type("struct(tags list(varchar), scores list(float64))");
    assert!(
        nested_result.is_ok(),
        "Nested struct with lists should work: {:?}",
        nested_result.err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_591_complex_type_data_read() {
    // Test reading a table with complex types (list, struct)
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    // Create a schema with a list column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));

    let tag_values = StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d")]);
    let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 4].into());
    let list_array = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        offsets,
        Arc::new(tag_values),
        None,
    );

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(list_array),
        ],
    )
    .unwrap();

    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "complex_types", &[batch])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, tags FROM test.main.complex_types ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);

    // Verify list column
    let tags_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(tags_col.len(), 2);
}

// ============================================================================
// Issue #637: DOUBLE type mismatch with PostgreSQL
// https://github.com/duckdb/ducklake/issues/637
//
// DuckLake generated DDL with "DOUBLE" keyword for PostgreSQL, but PostgreSQL
// requires "DOUBLE PRECISION". This is a DuckLake Postgres DDL generation bug.
// We verify our type mapper handles both "double" and "float64" correctly.
// ============================================================================

#[test]
fn test_issue_637_double_type_mapping() {
    // "double" should map to Float64 (this is the type DuckLake stores in metadata)
    let double_result = ducklake_to_arrow_type("double");
    assert!(
        double_result.is_ok(),
        "DOUBLE type should be supported: {:?}",
        double_result.err()
    );
    assert_eq!(double_result.unwrap(), DataType::Float64);

    // "float64" alias should also work
    let float64_result = ducklake_to_arrow_type("float64");
    assert!(float64_result.is_ok());
    assert_eq!(float64_result.unwrap(), DataType::Float64);

    // Roundtrip: Float64 -> ducklake -> Float64
    let ducklake_str = arrow_to_ducklake_type(&DataType::Float64).unwrap();
    let back = ducklake_to_arrow_type(&ducklake_str).unwrap();
    assert_eq!(back, DataType::Float64, "Float64 roundtrip should work");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_637_double_column_read() {
    // Test reading a table with DOUBLE (float64) columns - the scenario from the issue
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("latitude", DataType::Float64, true),
        Field::new("longitude", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![
                Some(40.7128),
                Some(34.0522),
                Some(51.5074),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(-74.0060),
                Some(-118.2437),
                Some(-0.1278),
            ])),
        ],
    )
    .unwrap();

    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "locations", &[batch])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;
    let df = ctx
        .sql("SELECT * FROM test.main.locations ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);

    let lat_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((lat_col.value(0) - 40.7128).abs() < 0.0001);
    assert!((lat_col.value(1) - 34.0522).abs() < 0.0001);

    let lon_col = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((lon_col.value(0) - (-74.0060)).abs() < 0.0001);
}
