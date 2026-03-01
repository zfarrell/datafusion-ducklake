#![cfg(feature = "metadata-duckdb")]
//! Test for issue #59: Negative footer_size wraps to usize::MAX via unchecked cast
//!
//! Validates that negative footer_size values in catalog metadata are handled
//! gracefully (skipped) instead of wrapping to usize::MAX.

mod common;

use std::sync::Arc;

use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use tempfile::TempDir;

/// Helper to create a catalog from a DuckLake database file
fn create_catalog(path: &str) -> DataFusionResult<Arc<DuckLakeCatalog>> {
    let provider = DuckdbMetadataProvider::new(path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    Ok(Arc::new(catalog))
}

/// Directly modify footer_size in the .ducklake DuckDB metadata file.
/// The .ducklake file is a plain DuckDB database containing the catalog tables.
fn set_footer_size_in_metadata(
    catalog_path: &std::path::Path,
    table_name: &str,
    value: i64,
) -> DataFusionResult<()> {
    let conn = duckdb::Connection::open(catalog_path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    conn.execute(
        &format!("UPDATE {} SET footer_size = ?;", table_name),
        [value],
    )
    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    Ok(())
}

/// Test that a table with negative footer_size in metadata can still be queried.
///
/// Before the fix, `footer_size as usize` would wrap -1i64 to usize::MAX,
/// passing a bogus hint to DataFusion. After the fix, negative values are
/// simply skipped (no hint applied).
#[tokio::test]
async fn test_negative_footer_size_skipped() -> DataFusionResult<()> {
    let temp_dir =
        TempDir::new().map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("neg_footer.ducklake");

    // Create a catalog with data
    common::create_catalog_no_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    // Inject a negative footer_size directly into the metadata database
    set_footer_size_in_metadata(&catalog_path, "ducklake_data_file", -1)?;

    // Query should succeed despite the negative footer_size
    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("neg_footer", catalog);

    let df = ctx
        .sql("SELECT * FROM neg_footer.main.users ORDER BY id")
        .await?;
    let results = df.collect().await?;

    // Verify correct results (all 4 rows)
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 4,
        "Should have 4 rows despite negative footer_size"
    );

    Ok(())
}

/// Test that zero footer_size is also skipped (only positive values should be used as hints).
#[tokio::test]
async fn test_zero_footer_size_skipped() -> DataFusionResult<()> {
    let temp_dir =
        TempDir::new().map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("zero_footer.ducklake");

    common::create_catalog_no_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    // Inject footer_size = 0
    set_footer_size_in_metadata(&catalog_path, "ducklake_data_file", 0)?;

    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("zero_footer", catalog);

    let df = ctx
        .sql("SELECT * FROM zero_footer.main.users ORDER BY id")
        .await?;
    let results = df.collect().await?;

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "Should have 4 rows despite zero footer_size");

    Ok(())
}

/// Test that negative footer_size on delete files is also handled gracefully.
#[tokio::test]
async fn test_negative_footer_size_on_delete_file() -> DataFusionResult<()> {
    let temp_dir =
        TempDir::new().map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("neg_del_footer.ducklake");

    // Create catalog with deletes so there are delete files
    common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    // Inject negative footer_size on both data files and delete files
    set_footer_size_in_metadata(&catalog_path, "ducklake_data_file", -1)?;
    set_footer_size_in_metadata(&catalog_path, "ducklake_delete_file", -1)?;

    // Query should succeed — delete filtering should still work
    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("neg_del_footer", catalog);

    let df = ctx
        .sql("SELECT * FROM neg_del_footer.main.products ORDER BY id")
        .await?;
    let results = df.collect().await?;

    // Should have 3 rows (ids 2 and 4 were deleted)
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "Should have 3 rows after delete filtering with negative footer_size"
    );

    Ok(())
}
