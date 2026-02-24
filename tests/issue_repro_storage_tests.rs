#![cfg(feature = "metadata-duckdb")]
//! Issue reproduction tests for path resolution, storage, and delete file bugs.
//!
//! Each test targets a specific GitHub issue from duckdb/ducklake and attempts
//! to trigger the same bug through our DataFusion-DuckLake extension.

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use tempfile::TempDir;

/// Helper to create a DuckLakeCatalog from a catalog database path
fn create_catalog(path: &str) -> DataFusionResult<Arc<DuckLakeCatalog>> {
    let provider = DuckdbMetadataProvider::new(path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    Ok(Arc::new(catalog))
}

/// Collect all rows from a query as RecordBatches
async fn query_batches(ctx: &SessionContext, sql: &str) -> DataFusionResult<Vec<RecordBatch>> {
    let df = ctx.sql(sql).await?;
    df.collect().await
}

/// Count total rows across batches
fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Collect i32 values from the first column across all batches
fn collect_i32_col(batches: &[RecordBatch], col_idx: usize) -> Vec<i32> {
    let mut vals = Vec::new();
    for batch in batches {
        let col = batch.column(col_idx);
        if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    vals.push(arr.value(i));
                }
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    vals.push(arr.value(i) as i32);
                }
            }
        }
    }
    vals
}

// ============================================================================
// Issue #84: Unexpected parquet file deletions with S3+Postgres
// https://github.com/duckdb/ducklake/issues/84
//
// Bug: Multiple writers to the same S3 bucket can cause data files to be
// unexpectedly deleted. When our reader tries to scan, referenced files may
// be missing.
//
// Repro approach: Create a catalog, then manually remove a data file. The
// scanner should handle the missing file gracefully (or at least report a
// clear error rather than panicking).
// ============================================================================
#[tokio::test]
async fn test_issue_84_missing_data_file_after_deletion() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("issue84.ducklake");

    common::create_catalog_no_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    // Find and delete one of the parquet files to simulate the bug
    let data_dir = temp_dir.path();
    let mut removed_file = false;
    for entry in std::fs::read_dir(data_dir).unwrap().flatten() {
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
            std::fs::remove_file(&p).unwrap();
            removed_file = true;
            break;
        }
    }

    // Also check subdirectories for parquet files
    if !removed_file {
        fn find_and_remove_parquet(dir: &std::path::Path) -> bool {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if p.is_dir() {
                        if find_and_remove_parquet(&p) {
                            return true;
                        }
                    } else if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                        std::fs::remove_file(&p).unwrap();
                        return true;
                    }
                }
            }
            false
        }
        removed_file = find_and_remove_parquet(data_dir);
    }

    assert!(removed_file, "Should have found and removed a parquet file");

    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Query should fail with an IO error (file not found), not panic
    let result = query_batches(&ctx, "SELECT * FROM test.main.users ORDER BY id").await;

    // We expect an error because the file is missing
    assert!(
        result.is_err(),
        "Expected error when data file is missing, but query succeeded. \
         Issue #84: scanner should detect missing files."
    );

    let err_msg = result.unwrap_err().to_string();
    eprintln!("Issue #84 error (expected): {}", err_msg);

    // The error should be IO-related, not a panic or internal error
    assert!(
        err_msg.contains("Object") || err_msg.contains("not found") || err_msg.contains("No such file")
            || err_msg.contains("IO") || err_msg.contains("Execution"),
        "Expected file-not-found error, got: {}",
        err_msg
    );

    Ok(())
}

// ============================================================================
// Issue #189: Deleted record problem after merge_adjacent_files
// https://github.com/duckdb/ducklake/issues/189
//
// Bug: After calling merge_adjacent_files(), deleted records reappear.
// The merge operation doesn't correctly handle delete files.
//
// Repro approach: Insert data, delete a row, then call merge_adjacent_files
// via DuckDB, and verify our reader still shows deleted records as deleted.
// ============================================================================
#[tokio::test]
async fn test_issue_189_deleted_record_after_merge() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("issue189.ducklake");

    // Create catalog with specific merge scenario from the issue
    {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();

        let ducklake_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(&format!("ATTACH '{}' AS my_ducklake;", ducklake_path), [])
            .unwrap();

        conn.execute("CREATE TABLE my_ducklake.test (id INT);", [])
            .unwrap();
        conn.execute("INSERT INTO my_ducklake.test VALUES (1);", [])
            .unwrap();
        conn.execute("DELETE FROM my_ducklake.test WHERE id = 1;", [])
            .unwrap();
        conn.execute("INSERT INTO my_ducklake.test VALUES (2);", [])
            .unwrap();
        conn.execute("INSERT INTO my_ducklake.test VALUES (3);", [])
            .unwrap();

        // This is where the bug occurs in DuckDB - merge_adjacent_files
        // may cause deleted records to reappear
        let merge_result = conn.execute("CALL my_ducklake.merge_adjacent_files();", []);
        eprintln!("merge_adjacent_files result: {:?}", merge_result);

        // If merge fails, that's also informative
        if let Err(e) = merge_result {
            eprintln!(
                "Issue #189: merge_adjacent_files failed (may be expected): {}",
                e
            );
        }
    }

    // Now read via DataFusion
    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    let batches = query_batches(&ctx, "SELECT id FROM test.main.test ORDER BY id").await?;
    let ids = collect_i32_col(&batches, 0);

    eprintln!("Issue #189: ids after merge = {:?}", ids);

    // id=1 was deleted, should NOT appear in results
    // The bug in issue #189 is that id=1 reappears after merge
    assert!(
        !ids.contains(&1),
        "Issue #189: Deleted record (id=1) reappeared after merge_adjacent_files! Got: {:?}",
        ids
    );

    // Should only see 2 and 3
    assert_eq!(
        ids,
        vec![2, 3],
        "Issue #189: Expected [2, 3] after delete + merge, got {:?}",
        ids
    );

    Ok(())
}

// ============================================================================
// Issue #198: DuckLake renders wrong abfss path (backslashes instead of forward slashes)
// https://github.com/duckdb/ducklake/issues/198
//
// Bug: On Windows or when created via mounted storage, paths use backslashes
// instead of forward slashes: "main\\table1\\file.parquet" instead of
// "main/table1/file.parquet"
//
// Repro approach: Test that our path resolver handles backslash paths correctly.
// ============================================================================
#[tokio::test]
async fn test_issue_198_backslash_path_resolution() -> DataFusionResult<()> {
    use datafusion_ducklake::path_resolver::{join_paths, resolve_path};

    // Test that path resolution works with backslash-style paths
    // This simulates what happens when DuckLake stores Windows-style paths

    // Case 1: base path with backslash, relative path with forward slash
    let resolved = resolve_path("C:\\data\\schema1\\", "table1/file.parquet", true).unwrap();
    eprintln!("Issue #198 case 1: {}", resolved);
    // The path should be usable (even if mixed slashes)
    assert!(
        resolved.contains("table1") && resolved.contains("file.parquet"),
        "Path should contain table1 and file.parquet"
    );

    // Case 2: paths with backslashes throughout
    let resolved = join_paths("main\\", "table1\\data.parquet").unwrap();
    eprintln!("Issue #198 case 2: {}", resolved);
    assert!(
        resolved.contains("table1") && resolved.contains("data.parquet"),
        "Issue #198: Backslash paths should resolve correctly"
    );

    // Case 3: Mixed slashes in hierarchical resolution
    let resolved = resolve_path("/data/", "schema1\\table1\\", true).unwrap();
    eprintln!("Issue #198 case 3: {}", resolved);
    assert!(
        resolved.contains("schema1"),
        "Issue #198: Mixed slash paths should work"
    );

    // Case 4: Test via actual DuckLake catalog with a table
    // Even though this is on Linux, we verify our resolver doesn't choke on backslashes
    let temp_dir = TempDir::new()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("issue198.ducklake");

    common::create_catalog_no_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Basic query should work (verifies path resolution with local paths)
    let batches = query_batches(
        &ctx,
        "SELECT COUNT(*) as cnt FROM test.main.users",
    )
    .await?;

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 4, "Should have 4 users");

    Ok(())
}

// ============================================================================
// Issue #255: DuckLake DataPath Exception for Root of S3 Bucket
// https://github.com/duckdb/ducklake/issues/255
//
// Bug: When DATA_PATH is the root of an S3 bucket (s3://bucket), DuckLake
// fails with: "Cannot write to s3://bucket/ - it exists and is a file"
//
// Repro approach: Test our path resolver handles bucket-root paths correctly.
// ============================================================================
#[tokio::test]
async fn test_issue_255_s3_bucket_root_data_path() -> DataFusionResult<()> {
    use datafusion_ducklake::path_resolver::{parse_object_store_url, PathResolver};

    // Test 1: Parse S3 URL with just bucket name (no trailing path)
    let (url, path) = parse_object_store_url("s3://aggregate.lake")
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    eprintln!("Issue #255 test 1: url={:?}, path='{}'", url, path);
    // The path from just a bucket should be empty or "/"
    assert!(
        path.is_empty() || path == "/",
        "Issue #255: Bucket-only URL should have empty or root path, got: '{}'",
        path
    );

    // Test 2: Parse S3 URL with bucket name and trailing slash
    let (url2, path2) = parse_object_store_url("s3://aggregate.lake/")
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    eprintln!("Issue #255 test 2: url={:?}, path='{}'", url2, path2);
    assert_eq!(path2, "/");

    // Test 3: PathResolver should work with bucket-root paths
    let resolver = PathResolver::new(Arc::new(url), path);

    let schema_path = resolver.resolve("main/", true).unwrap();
    eprintln!("Issue #255 test 3 schema_path: {}", schema_path);
    // Should produce a valid path like "/main/" or "main/"
    assert!(
        schema_path.contains("main"),
        "Issue #255: Schema path from bucket root should contain 'main'"
    );

    // Test 4: Full hierarchy from bucket root
    let schema_resolver = resolver.child_resolver("main/", true).unwrap();
    let table_resolver = schema_resolver.child_resolver("my_table/", true).unwrap();
    let file_path = table_resolver.resolve("data.parquet", true).unwrap();
    eprintln!("Issue #255 test 4 file_path: {}", file_path);
    assert!(
        file_path.contains("main") && file_path.contains("my_table") && file_path.contains("data.parquet"),
        "Issue #255: Full path from bucket root should contain all components. Got: {}",
        file_path
    );

    Ok(())
}

// ============================================================================
// Issue #373: Renaming table breaks partition folder structure
// https://github.com/duckdb/ducklake/issues/373
//
// Bug: After renaming a table via ALTER TABLE RENAME, newly inserted data
// goes to the wrong folder path. The old table path is still referenced.
//
// Repro approach: Create a table, rename it via DuckDB, then read it via
// DataFusion. Verify path resolution works for the renamed table.
// ============================================================================
#[tokio::test]
async fn test_issue_373_table_rename_path_resolution() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("issue373.ducklake");

    // Create table, insert data, rename, insert more data
    {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();

        let ducklake_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
            .unwrap();

        // Create original table and insert data
        conn.execute(
            "CREATE TABLE test_catalog.original_name (id INT, value VARCHAR);",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO test_catalog.original_name VALUES (1, 'before_rename');",
            [],
        )
        .unwrap();

        // Rename the table
        conn.execute(
            "ALTER TABLE test_catalog.original_name RENAME TO renamed_table;",
            [],
        )
        .unwrap();

        // Insert data after rename
        conn.execute(
            "INSERT INTO test_catalog.renamed_table VALUES (2, 'after_rename');",
            [],
        )
        .unwrap();
    }

    // Read via DataFusion - the table should be accessible under the new name
    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Query the renamed table
    let batches = query_batches(
        &ctx,
        "SELECT id, value FROM test.main.renamed_table ORDER BY id",
    )
    .await?;

    let ids = collect_i32_col(&batches, 0);
    eprintln!("Issue #373: ids from renamed table = {:?}", ids);

    // Both rows (before and after rename) should be visible
    assert_eq!(
        total_rows(&batches),
        2,
        "Issue #373: Should see both rows from before and after rename"
    );
    assert_eq!(
        ids,
        vec![1, 2],
        "Issue #373: Both rows should be accessible via renamed table"
    );

    // The old name should NOT be accessible
    let old_result = query_batches(
        &ctx,
        "SELECT * FROM test.main.original_name",
    )
    .await;
    assert!(
        old_result.is_err(),
        "Issue #373: Old table name should not be accessible after rename"
    );

    Ok(())
}

// ============================================================================
// Issue #378: Writing Incorrect Field IDs for delete files
// https://github.com/duckdb/ducklake/issues/378
//
// Bug: Delete files are written with field IDs 2147483646 and 2147483645
// instead of the correct Iceberg field IDs (2147483546 for file_path,
// 2147483545 for pos).
//
// Repro approach: Create a table with deletes and verify our reader can
// still parse the delete files despite the wrong field IDs. Our reader
// uses column names, not field IDs, so this should work.
// ============================================================================
#[tokio::test]
async fn test_issue_378_delete_file_field_ids() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("issue378.ducklake");

    // Create catalog with deletes (DuckDB will write delete files with "wrong" field IDs)
    common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Query should work despite potentially wrong field IDs in delete files
    let batches = query_batches(
        &ctx,
        "SELECT id FROM test.main.products ORDER BY id",
    )
    .await?;

    let ids = collect_i32_col(&batches, 0);
    eprintln!("Issue #378: ids = {:?}", ids);

    // IDs 2 and 4 were deleted
    assert_eq!(
        ids,
        vec![1, 3, 5],
        "Issue #378: Delete file reading should work regardless of field IDs. \
         Expected [1,3,5], got {:?}",
        ids
    );

    // Also verify COUNT(*) works
    let batches = query_batches(
        &ctx,
        "SELECT COUNT(*) as cnt FROM test.main.products",
    )
    .await?;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Issue #378: COUNT should be 3 after deletes");

    Ok(())
}

// ============================================================================
// Issue #605: DuckLake fails with IO Error on S3 with table scan queries
// https://github.com/duckdb/ducklake/issues/605
//
// Bug: Simple SELECT * fails with IO error on S3, but filtered queries work.
// This could be caused by too many concurrent file reads or path issues when
// scanning multiple files.
//
// Repro approach: Create a catalog with multiple data files (multiple inserts)
// and verify that a full table scan works correctly. This tests the case where
// many files need to be read simultaneously.
// ============================================================================
#[tokio::test]
async fn test_issue_605_full_table_scan_multiple_files() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("issue605.ducklake");

    // Create catalog with multiple separate inserts to generate multiple data files
    {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();

        let ducklake_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
            .unwrap();

        conn.execute(
            "CREATE TABLE test_catalog.events (id INT, event_type VARCHAR, date VARCHAR);",
            [],
        )
        .unwrap();

        // Multiple separate inserts to create multiple parquet files
        for i in 0..5 {
            conn.execute(
                &format!(
                    "INSERT INTO test_catalog.events VALUES ({}, 'type_{}', '2024-01-0{}');",
                    i * 2 + 1,
                    i,
                    i + 1
                ),
                [],
            )
            .unwrap();
            conn.execute(
                &format!(
                    "INSERT INTO test_catalog.events VALUES ({}, 'type_{}', '2024-01-0{}');",
                    i * 2 + 2,
                    i,
                    i + 1
                ),
                [],
            )
            .unwrap();
        }
    }

    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Test 1: Full table scan (this is what fails in the issue)
    let batches = query_batches(
        &ctx,
        "SELECT * FROM test.main.events ORDER BY id",
    )
    .await?;

    let total = total_rows(&batches);
    eprintln!("Issue #605 test 1: {} rows from full scan", total);
    assert_eq!(
        total, 10,
        "Issue #605: Full table scan should return all 10 rows"
    );

    // Test 2: Filtered query (this works in the issue)
    let batches = query_batches(
        &ctx,
        "SELECT * FROM test.main.events WHERE date = '2024-01-01'",
    )
    .await?;
    let filtered_count = total_rows(&batches);
    eprintln!("Issue #605 test 2: {} rows from filtered scan", filtered_count);
    assert!(
        filtered_count > 0,
        "Issue #605: Filtered query should return some rows"
    );

    // Test 3: COUNT(*) across all files
    let batches = query_batches(
        &ctx,
        "SELECT COUNT(*) as cnt FROM test.main.events",
    )
    .await?;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count, 10,
        "Issue #605: COUNT(*) should return 10"
    );

    Ok(())
}

// ============================================================================
// Issue #680: No magic bytes found at end of file (corrupted parquet)
// https://github.com/duckdb/ducklake/issues/680
//
// Bug: After dirty shutdown, DuckLake may leave partially-written parquet files
// that lack the magic bytes footer. Reading these files fails.
//
// Repro approach: Create a catalog, then corrupt one of the parquet files by
// truncating it. Verify our scanner reports a clear error.
// ============================================================================
#[tokio::test]
async fn test_issue_680_corrupted_parquet_no_magic_bytes() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("issue680.ducklake");

    common::create_catalog_no_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    // Find and corrupt a parquet file by truncating it
    fn find_and_corrupt_parquet(dir: &std::path::Path) -> bool {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    if find_and_corrupt_parquet(&p) {
                        return true;
                    }
                } else if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    // Truncate the file to simulate a dirty write
                    // A valid parquet file needs magic bytes "PAR1" at start and end
                    std::fs::write(&p, b"GARBAGE_NOT_PARQUET").unwrap();
                    eprintln!("Corrupted parquet file: {:?}", p);
                    return true;
                }
            }
        }
        false
    }

    let corrupted = find_and_corrupt_parquet(temp_dir.path());
    assert!(corrupted, "Should have found and corrupted a parquet file");

    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Query should fail with a clear error about parquet format
    let result = query_batches(&ctx, "SELECT * FROM test.main.users").await;
    assert!(
        result.is_err(),
        "Issue #680: Should fail when reading corrupted parquet file"
    );

    let err_msg = result.unwrap_err().to_string();
    eprintln!("Issue #680 error (expected): {}", err_msg);

    // Error should mention parquet parsing/magic bytes issues
    assert!(
        err_msg.contains("Parquet") || err_msg.contains("parquet")
            || err_msg.contains("magic") || err_msg.contains("EOF")
            || err_msg.contains("External") || err_msg.contains("Execution"),
        "Issue #680: Error should be about corrupted parquet, got: {}",
        err_msg
    );

    Ok(())
}

// ============================================================================
// Path resolver edge case tests (additional coverage for issues #198, #217, #255)
// ============================================================================
#[tokio::test]
async fn test_path_resolver_edge_cases_for_storage_issues() -> DataFusionResult<()> {
    use datafusion_ducklake::path_resolver::{
        parse_object_store_url, resolve_path, PathResolver,
    };

    // Edge case from #198: ABFSS-style paths
    // abfss://container@account.dfs.core.windows.net/path
    // Our resolver currently doesn't handle abfss:// but should not panic
    let abfss_result = parse_object_store_url("abfss://container@account.dfs.core.windows.net/data");
    eprintln!("ABFSS parse result: {:?}", abfss_result.is_ok());
    // Currently this will fail (unsupported scheme) - document behavior
    if abfss_result.is_err() {
        eprintln!("ABFSS scheme not supported (expected for now): {:?}", abfss_result.err());
    }

    // Edge case from #217: Empty relative path
    let resolved = resolve_path("/data/", "", true).unwrap();
    assert_eq!(resolved, "/data/", "Empty relative path should return base path");

    // Edge case from #255: Resolve with empty base path
    let resolved = resolve_path("", "schema/table/file.parquet", true).unwrap();
    eprintln!("Empty base + relative: {}", resolved);
    assert!(
        resolved.contains("schema"),
        "Should still contain the relative path components"
    );

    // Additional: PathResolver with S3 bucket root
    let (url, path) = parse_object_store_url("s3://my-bucket/")
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let resolver = PathResolver::new(Arc::new(url), path);

    // Build a full hierarchy from bucket root
    let schema_resolver = resolver.child_resolver("main/", true).unwrap();
    let table_resolver = schema_resolver.child_resolver("users/", true).unwrap();
    let file_path = table_resolver.resolve("00001.parquet", true).unwrap();
    eprintln!("Full path from bucket root: {}", file_path);
    assert!(
        file_path.ends_with("00001.parquet"),
        "File path should end with the filename"
    );

    Ok(())
}
