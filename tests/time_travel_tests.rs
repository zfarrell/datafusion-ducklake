#![cfg(feature = "metadata-duckdb")]
//! Integration tests for time travel table functions:
//! - ducklake_table_insertions()
//! - ducklake_current_snapshot()
//! - ducklake_last_committed_snapshot()
//!
//! Also verifies existing functions:
//! - ducklake_table_changes()
//! - ducklake_table_deletions()
//! - ducklake_snapshots()

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider, register_ducklake_functions};
use tempfile::TempDir;

/// Helper to create a context with catalog and registered table functions
async fn create_context(path: &str) -> DataFusionResult<SessionContext> {
    let provider = DuckdbMetadataProvider::new(path)?;
    let provider_arc: Arc<dyn datafusion_ducklake::MetadataProvider> =
        Arc::new(DuckdbMetadataProvider::new(path)?);

    let catalog = DuckLakeCatalog::new(provider)?;

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    let snapshot_id = provider_arc.get_current_snapshot()?;
    register_ducklake_functions(&ctx, provider_arc, snapshot_id);

    Ok(ctx)
}

/// Helper to collect all i32 values from a column
fn collect_int32(batches: &[RecordBatch], col_idx: usize) -> Vec<Option<i32>> {
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expected Int32Array");
        for i in 0..array.len() {
            if array.is_null(i) {
                values.push(None);
            } else {
                values.push(Some(array.value(i)));
            }
        }
    }
    values
}

/// Helper to collect all i64 values from a column
fn collect_int64(batches: &[RecordBatch], col_idx: usize) -> Vec<i64> {
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        for i in 0..array.len() {
            values.push(array.value(i));
        }
    }
    values
}

/// Helper to get total row count from batches
fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// =============================================================================
// ducklake_table_insertions tests
// =============================================================================

#[tokio::test]
async fn test_table_insertions_returns_inserted_rows() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("insertions.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    // Query insertions across all snapshots
    let df = ctx
        .sql("SELECT id, event_type, value FROM ducklake_table_insertions('main.events', 0, 10) ORDER BY id")
        .await?;

    let batches = df.collect().await?;

    // Should have 5 inserted rows (3 from first insert + 2 from second)
    assert_eq!(total_rows(&batches), 5);

    let ids = collect_int32(&batches, 0);
    assert_eq!(ids, vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);

    Ok(())
}

#[tokio::test]
async fn test_table_insertions_no_cdc_columns() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("insertions_schema.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    let df = ctx
        .sql("SELECT * FROM ducklake_table_insertions('main.events', 0, 10)")
        .await?;

    let schema = df.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // Should have only table columns, no snapshot_id or change_type
    assert_eq!(field_names, vec!["id", "event_type", "value"]);
    assert!(!field_names.contains(&"snapshot_id"));
    assert!(!field_names.contains(&"change_type"));

    Ok(())
}

#[tokio::test]
async fn test_table_insertions_empty_range() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("insertions_empty.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    // Same snapshot for start and end, should return no rows
    let df = ctx
        .sql("SELECT * FROM ducklake_table_insertions('main.events', 1, 1)")
        .await?;

    let batches = df.collect().await?;
    assert_eq!(total_rows(&batches), 0);

    Ok(())
}

#[tokio::test]
async fn test_table_insertions_partial_range() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("insertions_partial.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    // Query only the second batch of insertions (snapshot 2 -> 3)
    // The catalog has: snapshot 1 = create table, snapshot 2 = insert 3 rows,
    // snapshot 3 = insert 2 rows, snapshot 4 = delete 1 row
    let df = ctx
        .sql("SELECT id FROM ducklake_table_insertions('main.events', 2, 3) ORDER BY id")
        .await?;

    let batches = df.collect().await?;
    // Second insert only has 2 rows (ids 4, 5)
    assert_eq!(total_rows(&batches), 2);

    let ids = collect_int32(&batches, 0);
    assert_eq!(ids, vec![Some(4), Some(5)]);

    Ok(())
}

#[tokio::test]
async fn test_table_insertions_default_schema() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("insertions_default.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    // Use just table name (defaults to 'main' schema)
    let df = ctx
        .sql("SELECT * FROM ducklake_table_insertions('events', 0, 10)")
        .await?;

    let batches = df.collect().await?;
    assert!(total_rows(&batches) > 0);

    Ok(())
}

#[tokio::test]
async fn test_table_insertions_invalid_args() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("insertions_invalid.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    // Wrong number of args
    let result = ctx
        .sql("SELECT * FROM ducklake_table_insertions('events', 0)")
        .await;
    assert!(result.is_err());

    // Invalid snapshot range
    let result = ctx
        .sql("SELECT * FROM ducklake_table_insertions('events', 10, 5)")
        .await;
    assert!(result.is_err());

    // Non-existent table
    let result = ctx
        .sql("SELECT * FROM ducklake_table_insertions('nonexistent', 0, 10)")
        .await;
    assert!(result.is_err());

    Ok(())
}

// =============================================================================
// ducklake_current_snapshot tests
// =============================================================================

#[tokio::test]
async fn test_current_snapshot_returns_latest() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("current_snap.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    let df = ctx.sql("SELECT * FROM ducklake_current_snapshot()").await?;

    let schema = df.schema().clone();
    let batches = df.collect().await?;
    assert_eq!(total_rows(&batches), 1);

    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["id"]);

    // The catalog has 4 snapshots (0=init, 1=create table, 2=insert, 3=insert, 4=delete)
    let ids = collect_int64(&batches, 0);
    assert!(ids[0] > 0, "Should have a positive snapshot ID");

    Ok(())
}

#[tokio::test]
async fn test_current_snapshot_no_args() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("current_snap_args.ducklake");

    common::create_catalog_no_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    // Should error with arguments
    let result = ctx
        .sql("SELECT * FROM ducklake_current_snapshot('extra_arg')")
        .await;
    assert!(result.is_err());

    Ok(())
}

// =============================================================================
// ducklake_last_committed_snapshot tests
// =============================================================================

#[tokio::test]
async fn test_last_committed_snapshot() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("last_committed.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    let df = ctx
        .sql("SELECT * FROM ducklake_last_committed_snapshot()")
        .await?;

    let schema = df.schema().clone();
    let batches = df.collect().await?;
    assert_eq!(total_rows(&batches), 1);

    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["id"]);

    // In read-only mode, equals current snapshot
    let last_committed = collect_int64(&batches, 0);

    // Get current snapshot for comparison
    let df2 = ctx.sql("SELECT * FROM ducklake_current_snapshot()").await?;
    let batches2 = df2.collect().await?;
    let current = collect_int64(&batches2, 0);

    assert_eq!(
        last_committed, current,
        "In read-only mode, last committed should equal current"
    );

    Ok(())
}

// =============================================================================
// ducklake_snapshots verification
// =============================================================================

#[tokio::test]
async fn test_snapshots_lists_all() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("snapshots_list.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    let df = ctx
        .sql("SELECT snapshot_id, snapshot_time FROM ducklake_snapshots() ORDER BY snapshot_id")
        .await?;

    let batches = df.collect().await?;

    // Should have multiple snapshots
    let count = total_rows(&batches);
    assert!(count >= 4, "Expected at least 4 snapshots, got {}", count);

    // Snapshot IDs should start at 0
    let ids = collect_int64(&batches, 0);
    assert_eq!(ids[0], 0);

    // Should be monotonically increasing
    for i in 1..ids.len() {
        assert!(ids[i] > ids[i - 1], "Snapshot IDs should be increasing");
    }

    Ok(())
}

// =============================================================================
// ducklake_table_changes verification
// =============================================================================

#[tokio::test]
async fn test_table_changes_includes_cdc_columns() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("changes_verify.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    let df = ctx
        .sql("SELECT * FROM ducklake_table_changes('main.events', 0, 10)")
        .await?;

    let schema = df.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // Should have table columns + CDC columns
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"event_type"));
    assert!(field_names.contains(&"value"));
    assert!(field_names.contains(&"snapshot_id"));
    assert!(field_names.contains(&"change_type"));

    Ok(())
}

// =============================================================================
// ducklake_table_deletions verification
// =============================================================================

#[tokio::test]
#[ignore = "TODO(#21): CDC delete-file scan trips Parquet schema validation against DuckDB-written delete files"]
async fn test_table_deletions_returns_deleted_rows() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("deletions_verify.ducklake");

    common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    let df = ctx
        .sql("SELECT id, change_type FROM ducklake_table_deletions('main.products', 0, 100) ORDER BY id")
        .await?;

    let batches = df.collect().await?;
    assert_eq!(total_rows(&batches), 2, "Should have 2 deleted rows");

    // Get the id column
    let schema = batches[0].schema();
    let id_idx = schema.index_of("id").expect("id column");
    let ids = collect_int32(&batches, id_idx);
    assert_eq!(ids, vec![Some(2), Some(4)]);

    Ok(())
}

// =============================================================================
// Cross-function consistency tests
// =============================================================================

#[tokio::test]
#[ignore = "TODO(#21): CDC delete-file scan trips Parquet schema validation against DuckDB-written delete files"]
async fn test_insertions_vs_changes_row_count() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("consistency.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    // table_insertions and table_changes (insert-only) should return the same data
    let insertions = ctx
        .sql("SELECT id FROM ducklake_table_insertions('events', 0, 10) ORDER BY id")
        .await?
        .collect()
        .await?;

    let changes = ctx
        .sql("SELECT id FROM ducklake_table_changes('events', 0, 10) WHERE change_type = 'insert' ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(
        total_rows(&insertions),
        total_rows(&changes),
        "table_insertions and table_changes should have the same insert count"
    );

    let insertion_ids = collect_int32(&insertions, 0);
    let change_ids = collect_int32(&changes, 0);
    assert_eq!(
        insertion_ids, change_ids,
        "table_insertions and table_changes should return the same ids"
    );

    Ok(())
}

#[tokio::test]
async fn test_current_snapshot_matches_snapshots_max() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("snap_consistency.ducklake");

    common::create_catalog_multiple_snapshots(&catalog_path)
        .map_err(common::to_datafusion_error)?;

    let ctx = create_context(catalog_path.to_str().unwrap()).await?;

    // Current snapshot should match max snapshot_id from ducklake_snapshots()
    let current = ctx
        .sql("SELECT * FROM ducklake_current_snapshot()")
        .await?
        .collect()
        .await?;
    let current_id = collect_int64(&current, 0)[0];

    let max_snap = ctx
        .sql("SELECT MAX(snapshot_id) as max_id FROM ducklake_snapshots()")
        .await?
        .collect()
        .await?;
    let max_id = collect_int64(&max_snap, 0)[0];

    assert_eq!(
        current_id, max_id,
        "Current snapshot should match max snapshot from ducklake_snapshots()"
    );

    Ok(())
}
