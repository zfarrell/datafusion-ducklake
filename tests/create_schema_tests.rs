//! Integration tests for CREATE SCHEMA support.
//!
//! Tests verify that CREATE SCHEMA statements properly create schemas
//! in DuckLake metadata via the MetadataWriter.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::prelude::*;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, MetadataWriter, SqliteMetadataProvider, SqliteMetadataWriter,
};

/// Helper to create a test environment with writer and data directory.
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

/// Helper to create a fresh writable context for an existing temp_dir.
async fn create_writable_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::with_writer(Arc::new(provider), Arc::new(writer)).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

// ==================== Basic CREATE SCHEMA ====================

/// CREATE SCHEMA should create a new schema visible in the catalog.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_schema_basic() {
    let (_writer, temp_dir) = create_test_env().await;
    let ctx = create_writable_ctx(&temp_dir).await;

    // Create schema
    ctx.sql("CREATE SCHEMA ducklake.analytics")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify schema is visible - create a fresh context to avoid stale snapshot
    let ctx2 = create_writable_ctx(&temp_dir).await;
    let schemas = ctx2
        .sql("SELECT schema_name FROM ducklake.information_schema.schemata ORDER BY schema_name")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let names: Vec<&str> = schemas[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .filter_map(|v| v)
        .collect();

    assert!(
        names.contains(&"analytics"),
        "Schema 'analytics' should be in list: {:?}",
        names
    );
}

/// CREATE SCHEMA IF NOT EXISTS should succeed when schema doesn't exist.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_schema_if_not_exists_new() {
    let (_writer, temp_dir) = create_test_env().await;
    let ctx = create_writable_ctx(&temp_dir).await;

    let result = ctx
        .sql("CREATE SCHEMA IF NOT EXISTS ducklake.new_schema")
        .await
        .unwrap()
        .collect()
        .await;

    assert!(result.is_ok());
}

/// CREATE SCHEMA IF NOT EXISTS should succeed silently when schema already exists.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_schema_if_not_exists_already_exists() {
    let (_writer, temp_dir) = create_test_env().await;
    let ctx = create_writable_ctx(&temp_dir).await;

    // Create schema first
    ctx.sql("CREATE SCHEMA ducklake.test_schema")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // IF NOT EXISTS should succeed silently - need fresh ctx for updated snapshot
    let ctx2 = create_writable_ctx(&temp_dir).await;
    let result = ctx2
        .sql("CREATE SCHEMA IF NOT EXISTS ducklake.test_schema")
        .await
        .unwrap()
        .collect()
        .await;

    assert!(
        result.is_ok(),
        "IF NOT EXISTS should not error: {:?}",
        result
    );
}

/// CREATE SCHEMA without IF NOT EXISTS should fail when schema already exists.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_schema_already_exists_error() {
    let (_writer, temp_dir) = create_test_env().await;
    let ctx = create_writable_ctx(&temp_dir).await;

    // Create schema
    ctx.sql("CREATE SCHEMA ducklake.dupe_schema")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Try to create again - need fresh ctx
    let ctx2 = create_writable_ctx(&temp_dir).await;
    let result = ctx2.sql("CREATE SCHEMA ducklake.dupe_schema").await;

    // DataFusion should return an error about the schema already existing
    assert!(
        result.is_err() || {
            let collect_result = result.unwrap().collect().await;
            collect_result.is_err()
        },
        "Should error when creating duplicate schema"
    );
}

// ==================== CREATE SCHEMA + table operations ====================

/// Creating a table in a newly created schema should work.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_schema_then_create_table() {
    let (_writer, temp_dir) = create_test_env().await;
    let ctx = create_writable_ctx(&temp_dir).await;

    // Create schema
    ctx.sql("CREATE SCHEMA ducklake.myschema")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Create a table in the new schema using CTAS
    let ctx2 = create_writable_ctx(&temp_dir).await;
    ctx2.sql(
        "CREATE TABLE ducklake.myschema.users AS SELECT 1 as id, 'alice' as name UNION ALL SELECT 2, 'bob'",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Query the table in the new schema and verify the actual row count value
    let ctx3 = create_writable_ctx(&temp_dir).await;
    let results = ctx3
        .sql("SELECT count(*) as cnt FROM ducklake.myschema.users")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 1);
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "CTAS in non-main schema should create 2 rows");
}

// ==================== Multiple schemas ====================

/// Creating multiple schemas should all be visible.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_multiple_schemas() {
    let (_writer, temp_dir) = create_test_env().await;

    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("CREATE SCHEMA ducklake.schema_a")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let ctx2 = create_writable_ctx(&temp_dir).await;
    ctx2.sql("CREATE SCHEMA ducklake.schema_b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let ctx3 = create_writable_ctx(&temp_dir).await;
    ctx3.sql("CREATE SCHEMA ducklake.schema_c")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // All schemas should be visible
    let ctx4 = create_writable_ctx(&temp_dir).await;
    let schemas = ctx4
        .sql("SELECT schema_name FROM ducklake.information_schema.schemata ORDER BY schema_name")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let names: Vec<&str> = schemas[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .filter_map(|v| v)
        .collect();

    assert!(names.contains(&"schema_a"), "Missing schema_a: {:?}", names);
    assert!(names.contains(&"schema_b"), "Missing schema_b: {:?}", names);
    assert!(names.contains(&"schema_c"), "Missing schema_c: {:?}", names);
}

// ==================== CREATE then DROP ====================

/// Creating a schema and then dropping it should work.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_schema_then_drop() {
    let (_writer, temp_dir) = create_test_env().await;

    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("CREATE SCHEMA ducklake.temp_schema")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Drop the schema
    let ctx2 = create_writable_ctx(&temp_dir).await;
    ctx2.sql("DROP SCHEMA ducklake.temp_schema")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Schema should no longer be visible
    let ctx3 = create_writable_ctx(&temp_dir).await;
    let schemas = ctx3
        .sql("SELECT schema_name FROM ducklake.information_schema.schemata ORDER BY schema_name")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let names: Vec<&str> = schemas
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .filter_map(|v| v)
                .collect::<Vec<_>>()
        })
        .collect();

    assert!(
        !names.contains(&"temp_schema"),
        "Dropped schema should not be visible: {:?}",
        names
    );
}

// ==================== Reserved schema names ====================

/// CREATE SCHEMA information_schema should be rejected as a reserved name.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_schema_information_schema_reserved() {
    let (_writer, temp_dir) = create_test_env().await;
    let ctx = create_writable_ctx(&temp_dir).await;

    let result = ctx.sql("CREATE SCHEMA ducklake.information_schema").await;

    // Should fail at planning or execution stage
    let is_error = match result {
        Err(_) => true,
        Ok(df) => df.collect().await.is_err(),
    };
    assert!(
        is_error,
        "CREATE SCHEMA information_schema should be rejected as a reserved name"
    );
}

/// CREATE SCHEMA IF NOT EXISTS information_schema is a no-op since it already exists
/// as a virtual schema. DataFusion sees it exists and skips register_schema.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_schema_if_not_exists_information_schema_is_noop() {
    let (_writer, temp_dir) = create_test_env().await;
    let ctx = create_writable_ctx(&temp_dir).await;

    // IF NOT EXISTS should succeed silently because information_schema exists as a virtual schema
    let result = ctx
        .sql("CREATE SCHEMA IF NOT EXISTS ducklake.information_schema")
        .await;

    match result {
        Err(e) => panic!("IF NOT EXISTS should not error for existing virtual schema: {}", e),
        Ok(df) => {
            let collect_result = df.collect().await;
            assert!(
                collect_result.is_ok(),
                "IF NOT EXISTS should silently succeed: {:?}",
                collect_result.err()
            );
        }
    }
}

// ==================== MetadataWriter-level test ====================

/// Test that get_or_create_schema via register_schema creates the schema in metadata.
#[tokio::test(flavor = "multi_thread")]
async fn test_create_schema_metadata_writer_level() {
    let (writer, _temp) = create_test_env().await;

    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, was_created) = writer
        .get_or_create_schema("test_meta", None, snapshot_id)
        .unwrap();

    assert!(was_created, "Schema should be newly created");
    assert!(schema_id > 0, "Schema ID should be positive");

    // Creating again should return existing
    let snapshot_id2 = writer.create_snapshot().unwrap();
    let (schema_id2, was_created2) = writer
        .get_or_create_schema("test_meta", None, snapshot_id2)
        .unwrap();

    assert!(!was_created2, "Schema should already exist");
    assert_eq!(schema_id, schema_id2, "Should return same schema ID");
}
