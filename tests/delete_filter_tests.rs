#![cfg(feature = "metadata-duckdb")]
//! Integration tests for delete file filtering
//!
//! These tests verify that the delete file implementation correctly filters out
//! deleted rows from query results while maintaining backward compatibility.

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use parquet::file::reader::FileReader;
use tempfile::TempDir;

/// Test helper to extract integer values from a RecordBatch column
/// Supports both Int32 and Int64
fn get_int_column(batch: &RecordBatch, col_idx: usize) -> Vec<i32> {
    let column = batch.column(col_idx);

    // Try Int32 first
    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return (0..array.len())
            .filter_map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(array.value(i))
                }
            })
            .collect();
    }

    // Try Int64
    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return (0..array.len())
            .filter_map(|i| {
                if array.is_null(i) {
                    None
                } else {
                    Some(array.value(i) as i32)
                }
            })
            .collect();
    }

    panic!(
        "Column should be Int32Array or Int64Array, got {:?}",
        column.data_type()
    );
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Helper to create a catalog from a DuckLake database file
    fn create_catalog(path: &str) -> DataFusionResult<Arc<DuckLakeCatalog>> {
        let provider = DuckdbMetadataProvider::new(path)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog = DuckLakeCatalog::new(provider)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(Arc::new(catalog))
    }

    /// Test querying a table without delete files (backward compatibility)
    #[tokio::test]
    async fn test_table_without_delete_files() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("no_deletes.ducklake");

        // Generate test data
        common::create_catalog_no_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("no_deletes", catalog);

        // Query the table
        let df = ctx
            .sql("SELECT * FROM no_deletes.main.users ORDER BY id")
            .await?;
        let results = df.collect().await?;

        // Verify we got all rows (no deletes)
        assert!(!results.is_empty(), "Should have at least one batch");
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "Should have 4 rows (no deletes)");

        // Verify the IDs are correct
        let mut all_ids = Vec::new();
        for batch in &results {
            all_ids.extend(get_int_column(batch, 0));
        }
        assert_eq!(all_ids, vec![1, 2, 3, 4]);

        Ok(())
    }

    /// Test querying a table with delete files
    #[tokio::test]
    async fn test_table_with_delete_files() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("with_deletes.ducklake");

        // Generate test data
        common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_deletes", catalog);

        // Query the table
        let df = ctx
            .sql("SELECT * FROM with_deletes.main.products ORDER BY id")
            .await?;
        let results = df.collect().await?;

        // Verify we got the correct rows (excluding deleted IDs 2 and 4)
        assert!(!results.is_empty(), "Should have at least one batch");

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Should have 3 rows after filtering deletes");

        // Collect all IDs from all batches
        let mut all_ids = Vec::new();
        for batch in &results {
            all_ids.extend(get_int_column(batch, 0));
        }

        // Should only have IDs 1, 3, and 5 (2 and 4 were deleted)
        assert_eq!(all_ids, vec![1, 3, 5]);

        Ok(())
    }

    /// Test that deleted rows are actually excluded from results
    #[tokio::test]
    async fn test_deleted_rows_excluded() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("with_deletes.ducklake");

        // Generate test data
        common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_deletes", catalog);

        // Query for a specific deleted row (should return no results)
        let df = ctx
            .sql("SELECT * FROM with_deletes.main.products WHERE id = 2")
            .await?;
        let results = df.collect().await?;

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Deleted row with id=2 should not appear");

        // Query for a non-deleted row (should return 1 result)
        let df = ctx
            .sql("SELECT * FROM with_deletes.main.products WHERE id = 1")
            .await?;
        let results = df.collect().await?;

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "Non-deleted row with id=1 should appear");

        Ok(())
    }

    /// Test updated rows show new values (UPDATE = DELETE old + INSERT new)
    #[tokio::test]
    async fn test_updated_rows_show_new_values() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("with_updates.ducklake");

        // Generate test data
        common::create_catalog_with_updates(&catalog_path).map_err(common::to_datafusion_error)?;

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_updates", catalog);

        // Query the updated row
        let df = ctx
            .sql("SELECT id, quantity FROM with_updates.main.inventory WHERE id = 1")
            .await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1, "Should have exactly one row for id=1");

        // Verify the updated quantity (should be 120, not 100)
        let quantities = get_int_column(batch, 1);
        assert_eq!(quantities[0], 120, "Updated quantity should be 120");

        // Query another updated row
        let df = ctx
            .sql("SELECT id, quantity FROM with_updates.main.inventory WHERE id = 3")
            .await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1);

        let quantities = get_int_column(batch, 1);
        assert_eq!(quantities[0], 180, "Updated quantity should be 180");

        Ok(())
    }

    /// Test count query with delete files
    #[tokio::test]
    async fn test_count_with_deletes() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("with_deletes.ducklake");

        // Generate test data
        common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_deletes", catalog);

        // Count should exclude deleted rows
        let df = ctx
            .sql("SELECT COUNT(*) as count FROM with_deletes.main.products")
            .await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        let batch = &results[0];
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(
            counts.value(0),
            3,
            "Count should be 3 after filtering deletes"
        );

        Ok(())
    }

    /// Test aggregation with delete files
    #[tokio::test]
    async fn test_aggregation_with_deletes() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("with_updates.ducklake");

        // Generate test data
        common::create_catalog_with_updates(&catalog_path).map_err(common::to_datafusion_error)?;

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_updates", catalog);

        // Sum of quantities should use updated values
        let df = ctx
            .sql("SELECT SUM(quantity) as total FROM with_updates.main.inventory")
            .await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        let batch = &results[0];
        let totals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Updated quantities: 120 (id=1), 200 (id=2 unchanged), 180 (id=3)
        // Total should be 120 + 200 + 180 = 500
        assert_eq!(totals.value(0), 500, "Sum should reflect updated values");

        Ok(())
    }

    /// Test that empty result sets work correctly
    #[tokio::test]
    async fn test_empty_result_with_all_deleted() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("with_deletes.ducklake");

        // Generate test data
        common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_deletes", catalog);

        // Query only for deleted rows
        let df = ctx
            .sql("SELECT * FROM with_deletes.main.products WHERE id IN (2, 4)")
            .await?;
        let results = df.collect().await?;

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 0,
            "Should return empty result for all deleted rows"
        );

        Ok(())
    }

    /// Test that a table with all rows deleted returns 0 rows
    ///
    /// This is the bug scenario from https://github.com/hotdata-dev/datafusion-ducklake/issues/30
    /// When all rows are deleted from a table, querying should return 0 rows.
    #[tokio::test]
    async fn test_table_with_all_rows_deleted() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("all_deleted.ducklake");

        // Generate test data - inserts a row, then deletes it
        common::create_catalog_empty_table(&catalog_path).map_err(common::to_datafusion_error)?;

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("all_deleted", catalog);

        // Query the table - should return 0 rows since all data was deleted
        let df = ctx.sql("SELECT * FROM all_deleted.main.tbl").await?;
        let results = df.collect().await?;

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 0,
            "Table with all rows deleted should return 0 rows, but got {}",
            total_rows
        );

        // Also verify COUNT(*) returns 0
        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM all_deleted.main.tbl")
            .await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        let batch = &results[0];
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(
            counts.value(0),
            0,
            "COUNT(*) should be 0 after all rows deleted"
        );

        Ok(())
    }

    /// Test filter pushdown correctness with delete files
    ///
    /// This test verifies that WHERE filters are applied AFTER delete filtering,
    /// not before. This is critical for correct query semantics.
    ///
    /// Scenario:
    /// - Table has rows with id=[1,2,3,4,5]
    /// - Row with id=3 (position 2) is deleted
    /// - Query: WHERE id > 2
    ///
    /// Expected: [4, 5]
    /// Incorrect if filter applied before deletes: [2, 4, 5] (wrong - includes deleted row)
    /// Incorrect if deletes ignored: [3, 4, 5] (wrong - includes deleted row)
    ///
    /// This verifies the correct operation order:
    /// 1. Scan Parquet file (yields rows with id=[1,2,3,4,5])
    /// 2. Apply delete filtering (removes id=3, yields [1,2,4,5])
    /// 3. Apply WHERE filter (filters id > 2, yields [4,5])
    #[tokio::test]
    async fn test_filter_pushdown_correctness_with_deletes() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("filter_pushdown.ducklake");

        // Generate test data
        common::create_catalog_filter_pushdown(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("filter_pushdown", catalog);

        // Query with WHERE filter that should be applied AFTER delete filtering
        let df = ctx
            .sql("SELECT id FROM filter_pushdown.main.items WHERE id > 2 ORDER BY id")
            .await?;
        let results = df.collect().await?;

        assert!(!results.is_empty(), "Should have results");

        // Collect all IDs
        let mut all_ids = Vec::new();
        for batch in &results {
            all_ids.extend(get_int_column(batch, 0));
        }

        // Should return [4, 5] - the rows that remain after:
        // 1. Delete filtering removes id=3
        // 2. WHERE id > 2 filter is applied to [1,2,4,5], yielding [4,5]
        //
        // Common bugs this catches:
        // - Filter before delete: would incorrectly include deleted rows that match filter
        // - Filter on original positions: would return wrong rows
        assert_eq!(
            all_ids,
            vec![4, 5],
            "Filter should be applied AFTER delete filtering. \
             Expected [4,5] (rows with id>2 after id=3 deleted), got {:?}",
            all_ids
        );

        // Verify the deleted row (id=3) is NOT in results
        assert!(
            !all_ids.contains(&3),
            "Deleted row with id=3 should not appear, even though it matches id>2"
        );

        // Additional verification: query for id <= 2 should return [1, 2]
        let df = ctx
            .sql("SELECT id FROM filter_pushdown.main.items WHERE id <= 2 ORDER BY id")
            .await?;
        let results = df.collect().await?;

        let mut all_ids = Vec::new();
        for batch in &results {
            all_ids.extend(get_int_column(batch, 0));
        }

        assert_eq!(
            all_ids,
            vec![1, 2],
            "Filter id<=2 should return [1,2] after delete filtering"
        );

        Ok(())
    }

    /// Test that querying fails with a clear error when a delete file is missing from storage
    ///
    /// This is the bug scenario from issue #52:
    /// When a delete file referenced in catalog metadata is physically missing from storage,
    /// the query should fail with a clear error rather than silently returning rows that
    /// should have been deleted (data corruption).
    ///
    /// Repro:
    /// 1. Write data, then delete some rows (creates delete file in metadata)
    /// 2. Remove the delete file from disk
    /// 3. Query the table — should error, NOT silently return deleted rows
    #[tokio::test]
    async fn test_missing_delete_file_returns_error() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("missing_delete.ducklake");

        // Generate test data with deletes (products table: 5 rows, 2 deleted)
        common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

        // Find and remove delete files from the filesystem
        let removed_count = find_and_remove_delete_files(temp_dir.path());
        assert!(
            removed_count > 0,
            "Should have found and removed at least one delete file"
        );

        // Create catalog and session
        let catalog = create_catalog(&catalog_path.to_string_lossy())?;

        let ctx = SessionContext::new();
        ctx.register_catalog("test", catalog);

        // Query should fail because delete files are missing
        let result = ctx
            .sql("SELECT * FROM test.main.products")
            .await?
            .collect()
            .await;

        assert!(
            result.is_err(),
            "Query should fail when delete file is missing from storage, \
             but it succeeded (silent data corruption). Got {} rows.",
            result
                .as_ref()
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
        );

        // Verify the error message mentions the missing delete file
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Delete file") || err_msg.contains("delete file"),
            "Error message should mention the missing delete file, got: {}",
            err_msg
        );

        Ok(())
    }

    /// Test that COUNT(*) also fails when delete file is missing
    ///
    /// Even aggregate queries must not silently return wrong results
    /// when delete files are missing from storage.
    #[tokio::test]
    async fn test_missing_delete_file_count_also_errors() -> DataFusionResult<()> {
        let temp_dir = TempDir::new()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("missing_delete_count.ducklake");

        // Generate test data with deletes
        common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

        // Remove delete files
        let removed_count = find_and_remove_delete_files(temp_dir.path());
        assert!(removed_count > 0);

        let catalog = create_catalog(&catalog_path.to_string_lossy())?;
        let ctx = SessionContext::new();
        ctx.register_catalog("test", catalog);

        // COUNT(*) should also fail, not return a wrong count
        let result = ctx
            .sql("SELECT COUNT(*) FROM test.main.products")
            .await?
            .collect()
            .await;

        assert!(
            result.is_err(),
            "COUNT(*) should fail when delete file is missing, not return wrong count"
        );

        Ok(())
    }
}

/// Recursively find all parquet files in a directory
fn find_parquet_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut result = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                result.extend(find_parquet_files(&path));
            } else if path.extension().map_or(false, |e| e == "parquet") {
                result.push(path);
            }
        }
    }
    result
}

/// Check if a parquet file is a DuckLake delete file by examining its schema.
/// Delete files have schema: (file_path: BYTE_ARRAY/UTF8, pos: INT64)
fn is_delete_file(path: &std::path::Path) -> bool {
    if let Ok(file) = std::fs::File::open(path) {
        if let Ok(reader) = parquet::file::serialized_reader::SerializedFileReader::new(file) {
            let schema = reader.metadata().file_metadata().schema();
            let fields = schema.get_fields();
            return fields.len() == 2
                && fields[0].name() == "file_path"
                && fields[1].name() == "pos";
        }
    }
    false
}

/// Find and remove delete files (identified by their Parquet schema) from a directory tree.
/// Returns the number of files removed.
fn find_and_remove_delete_files(dir: &std::path::Path) -> usize {
    let parquet_files = find_parquet_files(dir);
    let mut removed = 0;
    for file_path in parquet_files {
        if is_delete_file(&file_path) {
            std::fs::remove_file(&file_path).unwrap();
            removed += 1;
        }
    }
    removed
}
