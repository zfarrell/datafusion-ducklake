#![cfg(feature = "metadata-duckdb")]
//! Integration tests for DuckLake table functions (Phase 5.1/5.2)
//!
//! Tests verify that table function output schemas match DuckDB's expected format.

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_ducklake::{
    DuckLakeCatalog, DuckdbMetadataProvider, register_ducklake_compaction_functions,
    register_ducklake_functions,
};
use tempfile::TempDir;

/// Helper to create a session context with table functions registered
fn setup_context(catalog_path: &str) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let provider = DuckdbMetadataProvider::new(catalog_path)?;
    let catalog = DuckLakeCatalog::new(provider)?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let provider2 = DuckdbMetadataProvider::new(catalog_path)?;
    register_ducklake_functions(&ctx, Arc::new(provider2));
    register_ducklake_compaction_functions(&ctx, catalog_path);
    Ok(ctx)
}

/// Helper to get string column values (None for NULLs)
fn get_string_values(batch: &RecordBatch, col_idx: usize) -> Vec<Option<String>> {
    let column = batch.column(col_idx);
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(array.value(i).to_string())
                }
            })
            .collect()
    } else {
        vec![]
    }
}

/// Helper to get i64 column values
fn get_i64_values(batch: &RecordBatch, col_idx: usize) -> Vec<Option<i64>> {
    let column = batch.column(col_idx);
    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        (0..array.len())
            .map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(array.value(i))
                }
            })
            .collect()
    } else {
        vec![]
    }
}

// ==================== ducklake_snapshots tests ====================

#[tokio::test]
async fn test_snapshots_schema_matches_duckdb() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    common::create_catalog_no_deletes(&catalog_path)?;

    let ctx = setup_context(catalog_path.to_str().unwrap())?;

    let df = ctx.sql("SELECT * FROM ducklake_snapshots()").await?;

    // Verify the schema matches DuckDB's expected columns
    let schema = df.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec![
            "snapshot_id",
            "snapshot_time",
            "schema_version",
            "changes",
            "author",
            "commit_message",
            "commit_extra_info"
        ],
        "ducklake_snapshots() schema should match DuckDB output"
    );

    let results = df.collect().await?;
    assert!(!results.is_empty(), "Should have snapshots");

    let batch = &results[0];
    let num_rows = batch.num_rows();
    assert!(
        num_rows >= 2,
        "Should have at least 2 snapshots (schema creation + table creation + insert)"
    );

    // Verify snapshot_id is present and starts at 0
    let ids = get_i64_values(batch, 0);
    assert_eq!(ids[0], Some(0), "First snapshot should be id 0");

    // Verify snapshot_time is populated
    let times = get_string_values(batch, 1);
    assert!(times[0].is_some(), "Snapshot time should be populated");

    // Verify schema_version is populated
    let versions = get_i64_values(batch, 2);
    assert!(versions[0].is_some(), "Schema version should be populated");

    // Verify changes column is populated
    let changes = get_string_values(batch, 3);
    assert!(
        changes[0].is_some(),
        "Changes should be populated for first snapshot"
    );

    Ok(())
}

// ==================== ducklake_table_info tests ====================

#[tokio::test]
async fn test_table_info_schema_matches_duckdb() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    common::create_catalog_with_deletes(&catalog_path)?;

    let ctx = setup_context(catalog_path.to_str().unwrap())?;

    let df = ctx.sql("SELECT * FROM ducklake_table_info()").await?;

    // Verify schema matches DuckDB's expected columns
    let schema = df.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec![
            "table_name",
            "schema_id",
            "table_id",
            "table_uuid",
            "file_count",
            "file_size_bytes",
            "delete_file_count",
            "delete_file_size_bytes"
        ],
        "ducklake_table_info() schema should match DuckDB output"
    );

    let results = df.collect().await?;
    assert!(!results.is_empty());

    let batch = &results[0];
    assert_eq!(batch.num_rows(), 1, "Should have 1 table (products)");

    // Verify table name
    let names = get_string_values(batch, 0);
    assert_eq!(names[0], Some("products".to_string()));

    // Verify schema_id is present
    let schema_ids = get_i64_values(batch, 1);
    assert!(schema_ids[0].is_some(), "schema_id should be populated");

    // Verify table_uuid is present
    let uuids = get_string_values(batch, 3);
    assert!(uuids[0].is_some(), "table_uuid should be populated");

    // Verify file counts
    let file_counts = get_i64_values(batch, 4);
    assert!(file_counts[0].unwrap() > 0, "Should have data files");

    // Verify delete file counts (we created deletes)
    let delete_counts = get_i64_values(batch, 6);
    assert!(delete_counts[0].unwrap() > 0, "Should have delete files");

    Ok(())
}

// ==================== ducklake_list_files tests ====================

#[tokio::test]
async fn test_list_files_schema_matches_duckdb() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    common::create_catalog_with_deletes(&catalog_path)?;

    let ctx = setup_context(catalog_path.to_str().unwrap())?;

    let df = ctx
        .sql("SELECT * FROM ducklake_list_files('products')")
        .await?;

    // Verify schema matches DuckDB's expected columns
    let schema = df.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec![
            "data_file",
            "data_file_size_bytes",
            "data_file_footer_size",
            "data_file_encryption_key",
            "delete_file",
            "delete_file_size_bytes",
            "delete_file_footer_size",
            "delete_file_encryption_key"
        ],
        "ducklake_list_files() schema should match DuckDB output"
    );

    let results = df.collect().await?;
    assert!(!results.is_empty());

    let batch = &results[0];
    assert!(batch.num_rows() > 0, "Should have files");

    // Verify data_file paths are populated
    let data_files = get_string_values(batch, 0);
    assert!(
        data_files[0].is_some(),
        "data_file path should be populated"
    );
    assert!(
        data_files[0].as_ref().unwrap().ends_with(".parquet"),
        "data_file should be a parquet path"
    );

    // Verify data file sizes > 0
    let sizes = get_i64_values(batch, 1);
    assert!(sizes[0].unwrap() > 0, "data_file_size_bytes should be > 0");

    Ok(())
}

#[tokio::test]
async fn test_list_files_requires_table_arg() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    common::create_catalog_no_deletes(&catalog_path)?;

    let ctx = setup_context(catalog_path.to_str().unwrap())?;

    // Should fail without table name argument
    let result = ctx.sql("SELECT * FROM ducklake_list_files()").await;
    assert!(result.is_err(), "Should require table name argument");

    Ok(())
}

// ==================== ducklake_options tests ====================

#[tokio::test]
async fn test_options_returns_catalog_options() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    common::create_catalog_no_deletes(&catalog_path)?;

    let ctx = setup_context(catalog_path.to_str().unwrap())?;

    let df = ctx.sql("SELECT * FROM ducklake_options()").await?;

    // Verify schema
    let schema = df.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        field_names,
        vec!["option_name", "description", "value", "scope", "scope_entry"],
        "ducklake_options() schema should match DuckDB output"
    );

    let results = df.collect().await?;
    assert!(!results.is_empty());

    let batch = &results[0];
    assert!(
        batch.num_rows() >= 3,
        "Should have at least 3 options (created_by, data_path, version)"
    );

    // Verify known options are present
    let option_names = get_string_values(batch, 0);
    let option_name_strs: Vec<&str> = option_names.iter().filter_map(|o| o.as_deref()).collect();
    assert!(
        option_name_strs.contains(&"data_path"),
        "Should have data_path option"
    );
    assert!(
        option_name_strs.contains(&"version"),
        "Should have version option"
    );

    Ok(())
}

// ==================== ducklake_current_snapshot / ducklake_last_committed_snapshot ====================

#[tokio::test]
async fn test_current_snapshot_returns_value() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    common::create_catalog_no_deletes(&catalog_path)?;

    let ctx = setup_context(catalog_path.to_str().unwrap())?;

    let df = ctx.sql("SELECT * FROM ducklake_current_snapshot()").await?;
    let results = df.collect().await?;
    assert_eq!(results[0].num_rows(), 1);
    let id = get_i64_values(&results[0], 0);
    assert!(id[0].unwrap() >= 0, "Snapshot ID should be non-negative");

    let df2 = ctx
        .sql("SELECT * FROM ducklake_last_committed_snapshot()")
        .await?;
    let results2 = df2.collect().await?;
    assert_eq!(results2[0].num_rows(), 1);

    Ok(())
}

// ==================== Cross-engine verification ====================

#[tokio::test]
async fn test_snapshots_cross_engine_verify() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("cross.ducklake");
    common::create_catalog_multiple_snapshots(&catalog_path)?;

    let ctx = setup_context(catalog_path.to_str().unwrap())?;

    // Query via DataFusion
    let df = ctx
        .sql("SELECT snapshot_id, schema_version FROM ducklake_snapshots() ORDER BY snapshot_id")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];

    let ids = get_i64_values(batch, 0);
    // Snapshots: 0 (schema created), 1 (table created), 2 (first insert), 3 (second insert), 4 (delete)
    assert!(ids.len() >= 4, "Should have at least 4 snapshots");
    assert_eq!(ids[0], Some(0));

    // Verify schema_versions are present
    let versions = get_i64_values(batch, 1);
    assert!(
        versions.iter().all(|v| v.is_some()),
        "All schema versions should be present"
    );

    Ok(())
}

#[tokio::test]
async fn test_table_info_multiple_tables() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("multi.ducklake");
    common::create_catalog_basic_test(&catalog_path)?;

    let ctx = setup_context(catalog_path.to_str().unwrap())?;

    let df = ctx
        .sql("SELECT table_name, table_id, table_uuid, file_count FROM ducklake_table_info() ORDER BY table_name")
        .await?;
    let results = df.collect().await?;
    let batch = &results[0];

    assert_eq!(batch.num_rows(), 2, "Should have 2 tables (test, test2)");

    let names = get_string_values(batch, 0);
    assert_eq!(names[0], Some("test".to_string()));
    assert_eq!(names[1], Some("test2".to_string()));

    // All tables should have UUIDs
    let uuids = get_string_values(batch, 2);
    assert!(uuids[0].is_some(), "test table should have UUID");
    assert!(uuids[1].is_some(), "test2 table should have UUID");

    Ok(())
}
