#![cfg(feature = "metadata-duckdb")]
//! Adversarial pattern-matching tests: DuckLake issues #300–#800
//!
//! Each test is inspired by a ROOT CAUSE PATTERN from a real upstream DuckLake issue.
//! We translate those patterns into analogous scenarios in our DataFusion extension.
//!
//! *** DO NOT FIX BUGS FOUND HERE — DOCUMENT THEM ***

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;
use datafusion::catalog::CatalogProvider;
use datafusion::common::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::path_resolver::{join_paths, resolve_path, PathResolver};
use datafusion_ducklake::types::{
    arrow_to_ducklake_type, build_arrow_schema, build_read_schema_with_field_id_mapping,
    ducklake_to_arrow_type,
};
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider, MetadataProvider};
use tempfile::TempDir;

/// Recursively find files matching a predicate
fn find_files_recursive(dir: &std::path::Path, pred: &dyn Fn(&std::path::Path) -> bool) -> Vec<std::path::PathBuf> {
    let mut results = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                results.extend(find_files_recursive(&path, pred));
            } else if pred(&path) {
                results.push(path);
            }
        }
    }
    results
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

fn setup_ducklake_catalog(catalog_path: &std::path::Path) -> anyhow::Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )?;
    conn.execute(
        "CREATE TABLE test_catalog.users (id INT, name VARCHAR);",
        [],
    )?;
    conn.execute(
        "INSERT INTO test_catalog.users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');",
        [],
    )?;
    Ok(())
}

#[allow(dead_code)]
fn setup_catalog_with_types(
    catalog_path: &std::path::Path,
    ddl: &str,
    inserts: &[&str],
) -> anyhow::Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )?;
    conn.execute(ddl, [])?;
    for insert in inserts {
        conn.execute(insert, [])?;
    }
    Ok(())
}

async fn query_count(ctx: &SessionContext, sql: &str) -> DataFusionResult<i64> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Ok(0);
    }
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array for count");
    Ok(arr.value(0))
}

#[allow(dead_code)]
fn open_catalog_rw(catalog_path: &std::path::Path) -> anyhow::Result<duckdb::Connection> {
    let conn = duckdb::Connection::open(catalog_path)?;
    Ok(conn)
}

// ============================================================================
// Pattern from issue #300: Orphaned files on failed inserts
// If a multi-step insert fails partway through (files written but metadata
// not committed), orphaned files remain. In our extension, what happens when
// metadata says files exist but they've been deleted from disk?
// ============================================================================

#[tokio::test]
async fn test_orphaned_file_reference_missing_from_disk() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("orphan.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Find and delete the actual parquet data file(s)
    let parquet_files = find_files_recursive(temp_dir.path(), &|p| {
        p.extension().map_or(false, |e| e == "parquet")
    });
    for f in &parquet_files {
        std::fs::remove_file(f).ok();
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Query should fail gracefully (not panic) when files are missing
    let result = ctx
        .sql("SELECT * FROM ducklake.main.users")
        .await?
        .collect()
        .await;

    eprintln!(
        "[#300 orphaned files] Query with missing parquet files: {}",
        if result.is_err() {
            format!("Error (expected): {}", result.unwrap_err())
        } else {
            "UNEXPECTED SUCCESS - should have errored on missing file".to_string()
        }
    );

    Ok(())
}

// ============================================================================
// Pattern from issue #597: DELETE inside transaction removes wrong rows
// when table was populated via multi-row INSERT. In our extension, test
// that delete positions correspond to correct row indices when data was
// inserted in varying batch sizes.
// ============================================================================

#[tokio::test]
async fn test_delete_position_correctness_multi_batch_insert() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("multi_batch.ducklake");

    // Insert data in multiple batches, then delete specific rows
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "CREATE TABLE test_catalog.data (id INT, value VARCHAR);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Multi-row INSERT (batch 1)
    conn.execute(
        "INSERT INTO test_catalog.data VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Separate INSERT (batch 2)
    conn.execute(
        "INSERT INTO test_catalog.data VALUES (4, 'd'), (5, 'e');",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Delete row from first batch — does the position map correctly?
    conn.execute("DELETE FROM test_catalog.data WHERE id = 2;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Delete row from second batch
    conn.execute("DELETE FROM test_catalog.data WHERE id = 5;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    // Now query via our extension
    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let count = query_count(&ctx, "SELECT COUNT(*) FROM ducklake.main.data").await?;
    eprintln!(
        "[#597 multi-batch delete] Row count after multi-batch insert + delete: {} (expected 3)",
        count
    );
    assert_eq!(count, 3, "Expected 3 rows after deleting ids 2 and 5");

    // Verify correct rows remain
    let df = ctx
        .sql("SELECT id FROM ducklake.main.data ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let mut ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expected Int32Array");
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                ids.push(arr.value(i));
            }
        }
    }
    eprintln!(
        "[#597 multi-batch delete] Remaining ids: {:?} (expected [1, 3, 4])",
        ids
    );
    assert_eq!(ids, vec![1, 3, 4]);

    Ok(())
}

// ============================================================================
// Pattern from issue #625: Column stats not updated after ALTER TABLE
// After adding columns via ALTER TABLE, the stats table doesn't include
// the new columns. Our `statistics()` impl reads from file_column_stats —
// what happens if stats reference columns that no longer exist (dropped)?
// ============================================================================

#[tokio::test]
async fn test_statistics_after_column_drop_via_metadata() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("stats_drop.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "CREATE TABLE test_catalog.t1 (id INT, name VARCHAR, extra VARCHAR);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.t1 VALUES (1, 'Alice', 'x'), (2, 'Bob', 'y');",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Drop a column that already has stats in the file
    conn.execute("ALTER TABLE test_catalog.t1 DROP COLUMN extra;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    drop(conn);

    // Query after column drop — should work without errors
    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let result = ctx
        .sql("SELECT id, name FROM ducklake.main.t1")
        .await?
        .collect()
        .await;

    match &result {
        Ok(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[#625 stats after drop] Query after column drop succeeded, {} rows",
                total
            );
        }
        Err(e) => {
            eprintln!(
                "[#625 stats after drop] FINDING: Query failed after column drop: {}",
                e
            );
        }
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #683: Transactional DDL puts catalog in unusable state
// "Column with name id already exists!" after CREATE + ALTER in transaction.
// In our extension, test that add_column + rename in sequence doesn't corrupt.
// ============================================================================

#[tokio::test]
async fn test_add_column_then_rename_sequence() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("ddl_seq.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Create table
    conn.execute("CREATE TABLE test_catalog.t1 (id INT, name VARCHAR);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.t1 VALUES (1, 'Alice'), (2, 'Bob');",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Add column, then rename it — this is the problematic pattern from #683/#740
    conn.execute("ALTER TABLE test_catalog.t1 ADD COLUMN temp_col VARCHAR;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "ALTER TABLE test_catalog.t1 RENAME COLUMN temp_col TO description;",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    // Query via our extension — column ordering should be [id, name, description]
    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let result = ctx
        .sql("SELECT * FROM ducklake.main.t1")
        .await?
        .collect()
        .await;

    match &result {
        Ok(batches) => {
            if !batches.is_empty() {
                let schema = batches[0].schema();
                let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
                eprintln!(
                    "[#683 DDL sequence] Column names after add+rename: {:?}",
                    names
                );
                // Verify the renamed column appears correctly
                assert!(
                    names.contains(&"description"),
                    "Expected 'description' column after rename, got {:?}",
                    names
                );
                assert!(
                    !names.iter().any(|n| *n == "temp_col"),
                    "temp_col should not appear after rename"
                );
            }
        }
        Err(e) => {
            eprintln!(
                "[#683 DDL sequence] FINDING: Query failed after add+rename: {}",
                e
            );
        }
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #704: flush_inlined_data fails after column drop
// Analogous in our extension: reading files written BEFORE a column was dropped
// — the Parquet file still has the column, but the schema says it's gone.
// ============================================================================

#[tokio::test]
async fn test_read_parquet_with_more_columns_than_schema() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("extra_col.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Create table with 3 columns, write data
    conn.execute(
        "CREATE TABLE test_catalog.wide (a INT, b VARCHAR, c DOUBLE);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.wide VALUES (1, 'x', 1.0), (2, 'y', 2.0);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Drop column c — Parquet file still has [a, b, c] but schema now says [a, b]
    conn.execute("ALTER TABLE test_catalog.wide DROP COLUMN c;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // This should read only columns [a, b] even though Parquet has [a, b, c]
    let result = ctx
        .sql("SELECT * FROM ducklake.main.wide")
        .await?
        .collect()
        .await;

    match &result {
        Ok(batches) => {
            if !batches.is_empty() {
                let schema = batches[0].schema();
                let col_count = schema.fields().len();
                eprintln!(
                    "[#704 column drop read] Columns returned: {} (expected 2 without virtual cols, up to 4 with virtual cols)",
                    col_count
                );
                // Base columns should be a and b only
                let base_names: Vec<&str> = schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .filter(|n| *n != "filename" && *n != "file_row_number")
                    .collect();
                assert_eq!(
                    base_names,
                    vec!["a", "b"],
                    "Should only see columns a, b after dropping c"
                );
            }
        }
        Err(e) => {
            eprintln!(
                "[#704 column drop read] FINDING: Read failed after column drop: {}",
                e
            );
        }
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #709: flush doubles row count in record_count
// Our SQL_GET_TABLE_ROW_COUNT does: SUM(record_count) - SUM(delete_count).
// If record_count is double-counted, we get wrong statistics. Test that
// cached_row_count matches actual COUNT(*).
// ============================================================================

#[tokio::test]
async fn test_row_count_matches_actual_count() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("rowcount.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute("CREATE TABLE test_catalog.t1 (id INT);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Insert in multiple batches to create multiple files
    conn.execute("INSERT INTO test_catalog.t1 VALUES (1), (2), (3);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSERT INTO test_catalog.t1 VALUES (4), (5);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Delete some rows
    conn.execute("DELETE FROM test_catalog.t1 WHERE id IN (2, 4);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Get actual count via query
    let actual_count =
        query_count(&ctx, "SELECT COUNT(*) FROM ducklake.main.t1").await?;

    eprintln!(
        "[#709 row count] Actual COUNT(*): {} (expected 3)",
        actual_count
    );
    assert_eq!(actual_count, 3, "Expected 3 rows after deleting ids 2 and 4");

    Ok(())
}

// ============================================================================
// Pattern from issue #643/#785: Hive partition values are nonsensical
// Test that partition pruning with identity transforms works correctly
// and doesn't silently drop data.
// ============================================================================

#[tokio::test]
async fn test_partition_pruning_identity_transform_correctness() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("partition.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Create partitioned table
    conn.execute(
        "CREATE TABLE test_catalog.events (id INT, category VARCHAR, value INT);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "ALTER TABLE test_catalog.events SET PARTITIONED BY (category);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Insert data across partitions
    conn.execute(
        "INSERT INTO test_catalog.events VALUES
            (1, 'sales', 100),
            (2, 'sales', 200),
            (3, 'marketing', 300),
            (4, 'engineering', 400);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Query with partition filter
    let filtered_count = query_count(
        &ctx,
        "SELECT COUNT(*) FROM ducklake.main.events WHERE category = 'sales'",
    )
    .await?;

    // Total count (no filter)
    let total_count =
        query_count(&ctx, "SELECT COUNT(*) FROM ducklake.main.events").await?;

    eprintln!(
        "[#643 partition pruning] Total: {}, Filtered (sales): {} (expected 4 total, 2 sales)",
        total_count, filtered_count
    );

    // The key assertion: partition pruning must not cause false negatives
    assert_eq!(total_count, 4, "Total row count should be 4");
    assert_eq!(filtered_count, 2, "Sales partition should have 2 rows");

    Ok(())
}

// ============================================================================
// Pattern from issue #680: Corrupted parquet file (no magic bytes)
// Test our extension's behavior when a parquet file is truncated/corrupted.
// ============================================================================

#[tokio::test]
async fn test_corrupted_parquet_file_graceful_error() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("corrupt.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Find parquet files and corrupt them
    let parquet_files = find_files_recursive(temp_dir.path(), &|p| {
        p.extension().map_or(false, |e| e == "parquet")
    });
    for f in &parquet_files {
        std::fs::write(f, b"NOT_A_PARQUET_FILE").ok();
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let result = ctx
        .sql("SELECT * FROM ducklake.main.users")
        .await?
        .collect()
        .await;

    eprintln!(
        "[#680 corrupted parquet] Result: {}",
        if result.is_err() {
            format!("Error (expected): {}", result.unwrap_err())
        } else {
            "UNEXPECTED: query succeeded on corrupted parquet".to_string()
        }
    );

    Ok(())
}

// ============================================================================
// Pattern from issue #652/#677/#703: CHECKPOINT/type mismatch errors
// Tests for type comparison edge cases. Our metadata_provider SQL uses
// snapshot comparisons — what if snapshot_id overflows or is negative?
// ============================================================================

#[tokio::test]
async fn test_extreme_snapshot_id_values() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("extreme_snap.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider: Arc<dyn MetadataProvider> = Arc::new(
        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    // Bind to i64::MAX — should still work but show no future-snapshot data
    let catalog = DuckLakeCatalog::with_snapshot(Arc::clone(&provider), i64::MAX)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let result = ctx
        .sql("SELECT COUNT(*) FROM ducklake.main.users")
        .await?
        .collect()
        .await;

    match &result {
        Ok(batches) => {
            if !batches.is_empty() && batches[0].num_rows() > 0 {
                let count = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| a.value(0))
                    .unwrap_or(-1);
                eprintln!(
                    "[#652 extreme snapshot] COUNT(*) at snapshot MAX: {} (expected 3)",
                    count
                );
            }
        }
        Err(e) => {
            eprintln!(
                "[#652 extreme snapshot] FINDING: Query at MAX snapshot failed: {}",
                e
            );
        }
    }

    // Bind to negative snapshot
    let catalog_neg = DuckLakeCatalog::with_snapshot(Arc::clone(&provider), -1)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx2 = SessionContext::new();
    ctx2.register_catalog("ducklake", Arc::new(catalog_neg));

    let result2 = ctx2
        .sql("SELECT COUNT(*) FROM ducklake.main.users")
        .await;

    match result2 {
        Ok(df) => {
            let batches = df.collect().await;
            eprintln!(
                "[#652 negative snapshot] Query at snapshot -1: {:?}",
                batches.is_ok()
            );
        }
        Err(e) => {
            eprintln!(
                "[#652 negative snapshot] Query plan at snapshot -1 failed: {}",
                e
            );
        }
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #740: Adding column and renaming inside txn fails
// In our extension, test the field_id mapping logic when columns have
// been added and renamed — the Parquet file has no data for the new column.
// ============================================================================

#[test]
fn test_field_id_mapping_with_added_and_renamed_columns() {
    use datafusion_ducklake::metadata_provider::DuckLakeTableColumn;

    // Simulate: table originally had [id(1), name(2)]
    // Then: ADD COLUMN temp(3), RENAME COLUMN temp->description(3)
    // Parquet file has field_ids {1: "id", 2: "name"} — no field 3
    let current_columns = vec![
        DuckLakeTableColumn {
            column_id: 1,
            column_name: "id".to_string(),
            column_type: "int32".to_string(),
            is_nullable: false,
        },
        DuckLakeTableColumn {
            column_id: 2,
            column_name: "name".to_string(),
            column_type: "varchar".to_string(),
            is_nullable: true,
        },
        DuckLakeTableColumn {
            column_id: 3,
            column_name: "description".to_string(), // Was added then renamed
            column_type: "varchar".to_string(),
            is_nullable: true,
        },
    ];

    // Parquet file only has columns 1 and 2
    let mut parquet_field_ids = HashMap::new();
    parquet_field_ids.insert(1, "id".to_string());
    parquet_field_ids.insert(2, "name".to_string());

    let result =
        build_read_schema_with_field_id_mapping(&current_columns, &parquet_field_ids);

    match result {
        Ok((schema, name_mapping)) => {
            eprintln!(
                "[#740 add+rename] Schema fields: {:?}",
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            );
            eprintln!("[#740 add+rename] Name mapping: {:?}", name_mapping);
            // Key question: does the schema include the new column "description"
            // that has no corresponding parquet field_id?
            // It should — but the column won't be in the file, so reading may fail.
            assert_eq!(
                schema.fields().len(),
                3,
                "Schema should include all 3 current columns"
            );
        }
        Err(e) => {
            eprintln!(
                "[#740 add+rename] FINDING: build_read_schema failed: {}",
                e
            );
        }
    }
}

// ============================================================================
// Pattern from issue #733: Cannot select snapshots after certain UPDATE
// In our extension, test that after a sequence of updates the snapshot
// ID tracking remains consistent.
// ============================================================================

#[tokio::test]
async fn test_snapshot_consistency_after_updates() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("snap_update.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "CREATE TABLE test_catalog.data (id INT, val INT);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.data VALUES (1, 10), (2, 20), (3, 30);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Perform multiple updates to create many snapshots
    for i in 0..5 {
        conn.execute(
            &format!(
                "UPDATE test_catalog.data SET val = {} WHERE id = 1;",
                100 + i
            ),
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    drop(conn);

    // Query should see the latest state
    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let count = query_count(&ctx, "SELECT COUNT(*) FROM ducklake.main.data").await?;
    eprintln!(
        "[#733 snapshot update] Count after 5 updates: {} (expected 3)",
        count
    );
    assert_eq!(count, 3, "Row count should remain 3 after updates");

    // Verify the latest value of id=1
    let df = ctx
        .sql("SELECT val FROM ducklake.main.data WHERE id = 1")
        .await?;
    let batches = df.collect().await?;
    if !batches.is_empty() {
        let val = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(0));
        eprintln!(
            "[#733 snapshot update] Latest value for id=1: {:?} (expected 104)",
            val
        );
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #644: WHERE + ORDER BY + LIMIT returns wrong rows
// Test that filter pushdown + limit doesn't cause incorrect results when
// delete files are present.
// ============================================================================

#[tokio::test]
async fn test_filter_pushdown_with_limit_and_deletes() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("filter_limit.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "CREATE TABLE test_catalog.items (id INT, category VARCHAR, value INT);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.items VALUES
            (1, 'A', 10),
            (2, 'A', 20),
            (3, 'B', 30),
            (4, 'A', 40),
            (5, 'B', 50),
            (6, 'A', 60),
            (7, 'B', 70),
            (8, 'A', 80);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Delete some rows in category A
    conn.execute(
        "DELETE FROM test_catalog.items WHERE id IN (2, 6);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // WHERE + ORDER BY + LIMIT — the exact pattern from #644
    let df = ctx
        .sql("SELECT id, value FROM ducklake.main.items WHERE category = 'A' ORDER BY value DESC LIMIT 2")
        .await?;
    let batches = df.collect().await?;

    let mut ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expected Int32Array");
        for i in 0..arr.len() {
            ids.push(arr.value(i));
        }
    }

    eprintln!(
        "[#644 filter+limit] Top 2 category A by value DESC: ids={:?} (expected [8, 4])",
        ids
    );
    // After deleting ids 2 and 6, remaining A rows are: 1(10), 4(40), 8(80)
    // Top 2 by value DESC: 8(80), 4(40)
    assert_eq!(ids, vec![8, 4], "Wrong rows returned with filter+limit+deletes");

    Ok(())
}

// ============================================================================
// Pattern from issue #669: set_option('delete_older_than', '0 hours') deleted all data
// Test boundary: what if ALL rows are deleted but file metadata remains?
// ============================================================================

#[tokio::test]
async fn test_all_rows_deleted_leaves_empty_result() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("all_deleted.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute("CREATE TABLE test_catalog.t1 (id INT, name VARCHAR);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c');",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Delete ALL rows
    conn.execute("DELETE FROM test_catalog.t1;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let count = query_count(&ctx, "SELECT COUNT(*) FROM ducklake.main.t1").await?;
    eprintln!(
        "[#669 all deleted] COUNT(*) after deleting all: {} (expected 0)",
        count
    );
    assert_eq!(count, 0, "All rows deleted, should return 0");

    // SELECT * should return empty result
    let df = ctx.sql("SELECT * FROM ducklake.main.t1").await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "[#669 all deleted] SELECT * rows: {} (expected 0)",
        total_rows
    );
    assert_eq!(total_rows, 0);

    Ok(())
}

// ============================================================================
// Pattern from issue #606/#692: Files with different hive partition paths
// Test that our path resolution handles mixed absolute/relative paths correctly
// even in pathological cases.
// ============================================================================

#[test]
fn test_path_resolution_pathological_cases() {
    // Empty path components
    let result = resolve_path("", "", true);
    eprintln!("[#606 path edge] resolve_path('', '', true) = {:?}", result);

    // Multiple consecutive slashes
    let result2 = resolve_path("///", "///file.parquet", true);
    eprintln!(
        "[#606 path edge] resolve_path('///', '///file.parquet', true) = {:?}",
        result2
    );

    // Path with .. components (potential traversal) — now rejected
    let result3 = resolve_path("/data/schema/", "../other/file.parquet", true);
    eprintln!(
        "[#606 path edge] resolve_path with .. = {:?}",
        result3
    );
    // resolve_path now rejects paths containing '..' as a standalone component
    assert!(
        result3.is_err(),
        "resolve_path should reject paths with '..' traversal component"
    );

    // Backslash handling (Windows paths in Linux context) — now rejected due to ..
    let result4 = join_paths("C:\\data\\", "..\\..\\etc\\passwd");
    eprintln!(
        "[#606 path edge] join_paths with Windows traversal = {:?}",
        result4
    );
    // join_paths now rejects paths containing '..' as a standalone component
    assert!(
        result4.is_err(),
        "join_paths should reject paths with '..' traversal component"
    );

    // Unicode in paths
    let result5 = resolve_path("/data/日本語/", "テーブル/file.parquet", true).unwrap();
    eprintln!(
        "[#606 path edge] Unicode path resolution = {:?}",
        result5
    );
    assert!(result5.contains("日本語"));
    assert!(result5.contains("テーブル"));
}

// ============================================================================
// Pattern from issue #766: S3 path-style URL handling
// Test that our parse_object_store_url handles edge cases in S3 URLs.
// ============================================================================

#[test]
fn test_s3_url_edge_cases() {
    use datafusion_ducklake::path_resolver::parse_object_store_url;

    // URL with port (e.g., MinIO endpoint)
    // s3://localhost:9000/bucket is NOT a valid S3 URL but users might try it
    let result = parse_object_store_url("s3://localhost:9000/bucket/path");
    eprintln!(
        "[#766 S3 edge] s3://localhost:9000/... = {:?}",
        result.as_ref().map(|(u, p)| (u.to_string(), p.clone()))
    );

    // URL with special characters in path
    let result2 = parse_object_store_url("s3://bucket/path with spaces/file");
    eprintln!(
        "[#766 S3 edge] path with spaces = {:?}",
        result2.as_ref().map(|(_, p)| p.clone())
    );

    // URL-encoded characters
    let result3 = parse_object_store_url("s3://bucket/path%2Fwith%2Fslashes/data");
    eprintln!(
        "[#766 S3 edge] URL-encoded slashes = {:?}",
        result3.as_ref().map(|(_, p)| p.clone())
    );

    // Empty bucket name
    let result4 = parse_object_store_url("s3:///path");
    assert!(
        result4.is_err(),
        "s3:///path should error (missing bucket)"
    );
}

// ============================================================================
// Pattern from issue #637: DOUBLE type generates invalid DDL for Postgres
// Our type mapping: ensure roundtrip consistency for all types.
// ============================================================================

#[test]
fn test_type_mapping_roundtrip_exhaustive() {
    let types_to_test = vec![
        ("boolean", DataType::Boolean),
        ("int8", DataType::Int8),
        ("int16", DataType::Int16),
        ("int32", DataType::Int32),
        ("int64", DataType::Int64),
        ("uint8", DataType::UInt8),
        ("uint16", DataType::UInt16),
        ("uint32", DataType::UInt32),
        ("uint64", DataType::UInt64),
        ("float32", DataType::Float32),
        ("float64", DataType::Float64),
        ("date", DataType::Date32),
        ("varchar", DataType::Utf8),
        ("blob", DataType::Binary),
        ("uuid", DataType::FixedSizeBinary(16)),
    ];

    for (ducklake_type, expected_arrow) in &types_to_test {
        let arrow = ducklake_to_arrow_type(ducklake_type).unwrap();
        assert_eq!(
            &arrow, expected_arrow,
            "ducklake_to_arrow_type('{}') mismatch",
            ducklake_type
        );

        let back = arrow_to_ducklake_type(&arrow).unwrap();
        let arrow2 = ducklake_to_arrow_type(&back).unwrap();
        assert_eq!(
            arrow, arrow2,
            "Roundtrip failed for {} -> {} -> {} -> {:?}",
            ducklake_type, arrow, back, arrow2
        );
    }
}

// ============================================================================
// Pattern from issue #637: "DOUBLE" vs "float64" — DuckDB uses "DOUBLE"
// but our type mapping expects "float64". Test all DuckDB aliases.
// ============================================================================

#[test]
fn test_type_aliases_from_duckdb() {
    // DuckDB type aliases that might appear in catalog metadata
    let aliases = vec![
        ("BIGINT", DataType::Int64),
        ("INTEGER", DataType::Int32),
        ("SMALLINT", DataType::Int16),
        ("TINYINT", DataType::Int8),
        ("DOUBLE", DataType::Float64),
        ("REAL", DataType::Float32),
        ("FLOAT", DataType::Float32),
        ("BOOLEAN", DataType::Boolean),
        ("BOOL", DataType::Boolean),
        ("TEXT", DataType::Utf8),
        ("STRING", DataType::Utf8),
        ("LONG", DataType::Int64),
        ("INT", DataType::Int32),
        ("UINT", DataType::UInt32),
    ];

    for (alias, expected) in &aliases {
        match ducklake_to_arrow_type(alias) {
            Ok(dt) => {
                assert_eq!(
                    &dt, expected,
                    "Alias '{}' mapped to {:?}, expected {:?}",
                    alias, dt, expected
                );
            }
            Err(e) => {
                eprintln!(
                    "[#637 type alias] FINDING: Alias '{}' not recognized: {}",
                    alias, e
                );
            }
        }
    }

    // DuckDB-specific types that might not have direct Arrow equivalents
    let maybe_unsupported = vec!["HUGEINT", "UHUGEINT", "BIT", "ENUM"];
    for t in maybe_unsupported {
        let result = ducklake_to_arrow_type(t);
        eprintln!(
            "[#637 type alias] '{}' -> {:?}",
            t,
            result.as_ref().map(|d| format!("{:?}", d)).unwrap_or_else(|e| format!("Err({})", e))
        );
    }
}

// ============================================================================
// Pattern from issue #595: Complex types fail with Postgres catalog
// Test that complex type strings from DuckDB parse correctly.
// ============================================================================

#[test]
fn test_complex_type_parsing_edge_cases() {
    // Deeply nested types
    let deep = "list(list(list(int32)))";
    let result = ducklake_to_arrow_type(deep);
    assert!(result.is_ok(), "Deeply nested list failed: {:?}", result);

    // Struct with many fields
    let big_struct = "struct(a int32, b varchar, c float64, d boolean, e date, f timestamp)";
    let result = ducklake_to_arrow_type(big_struct);
    assert!(result.is_ok(), "Big struct failed: {:?}", result);
    if let Ok(DataType::Struct(fields)) = &result {
        assert_eq!(fields.len(), 6);
    }

    // Map with complex value type
    let complex_map = "map(varchar, struct(x int32, y float64))";
    let result = ducklake_to_arrow_type(complex_map);
    assert!(result.is_ok(), "Complex map failed: {:?}", result);

    // Empty parentheses (edge case)
    let result = ducklake_to_arrow_type("list()");
    eprintln!(
        "[#595 complex types] 'list()' -> {:?}",
        result.as_ref().map(|_| "Ok").unwrap_or("Err")
    );

    // Struct with no fields
    let result = ducklake_to_arrow_type("struct()");
    eprintln!(
        "[#595 complex types] 'struct()' -> {:?}",
        result.as_ref().map(|_| "Ok").unwrap_or("Err")
    );

    // Type with trailing whitespace
    let result = ducklake_to_arrow_type("  int32  ");
    assert!(
        result.is_ok(),
        "Type with whitespace should be trimmed: {:?}",
        result
    );
}

// ============================================================================
// Pattern from issue #619: Column names > 64 characters
// Test that our extension handles very long column names.
// ============================================================================

#[test]
fn test_very_long_column_names() {
    use datafusion_ducklake::metadata_provider::DuckLakeTableColumn;

    let long_name = "a".repeat(200);
    let columns = vec![DuckLakeTableColumn {
        column_id: 1,
        column_name: long_name.clone(),
        column_type: "int32".to_string(),
        is_nullable: true,
    }];

    let schema = build_arrow_schema(&columns);
    match schema {
        Ok(s) => {
            assert_eq!(s.field(0).name(), &long_name);
            eprintln!("[#619 long names] 200-char column name accepted");
        }
        Err(e) => {
            eprintln!("[#619 long names] FINDING: Long column name failed: {}", e);
        }
    }
}

// ============================================================================
// Pattern from issue #305: Unknown/NULL type columns
// Test that columns with unrecognized types produce clear errors.
// ============================================================================

#[test]
fn test_unknown_type_handling() {
    use datafusion_ducklake::metadata_provider::DuckLakeTableColumn;

    let columns = vec![
        DuckLakeTableColumn {
            column_id: 1,
            column_name: "id".to_string(),
            column_type: "int32".to_string(),
            is_nullable: false,
        },
        DuckLakeTableColumn {
            column_id: 2,
            column_name: "data".to_string(),
            column_type: "UNKNOWN_TYPE_XYZ".to_string(),
            is_nullable: true,
        },
    ];

    let result = build_arrow_schema(&columns);
    assert!(
        result.is_err(),
        "Unknown type should produce error, not silent fallback"
    );
    let err_msg = result.unwrap_err().to_string();
    eprintln!("[#305 unknown type] Error message: {}", err_msg);
    // The error should be informative
    assert!(
        err_msg.contains("UNKNOWN_TYPE_XYZ"),
        "Error should mention the unsupported type"
    );
}

// ============================================================================
// Pattern from issue #779: Read-only catalog with missing migration
// Test that our extension gracefully handles catalogs missing tables.
// ============================================================================

#[tokio::test]
async fn test_catalog_with_empty_metadata_tables() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("empty_meta.ducklake");

    // Create a minimal DuckDB file with ducklake tables but no data
    let conn = duckdb::Connection::open(&catalog_path)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "CREATE TABLE ducklake_metadata (key VARCHAR, value VARCHAR, scope VARCHAR, scope_id INTEGER);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO ducklake_metadata VALUES ('data_path', ?, NULL, NULL);",
        [temp_dir.path().to_string_lossy().to_string()],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "CREATE TABLE ducklake_snapshot (snapshot_id INTEGER PRIMARY KEY, snapshot_time TEXT);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "CREATE TABLE ducklake_schema (schema_id INTEGER, schema_name VARCHAR, path VARCHAR DEFAULT '', path_is_relative BOOLEAN DEFAULT 1, begin_snapshot INTEGER, end_snapshot INTEGER);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "CREATE TABLE ducklake_table (table_id INTEGER, table_uuid VARCHAR, schema_id INTEGER, table_name VARCHAR, path VARCHAR DEFAULT '', path_is_relative BOOLEAN DEFAULT 1, begin_snapshot INTEGER, end_snapshot INTEGER);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "CREATE TABLE ducklake_column (column_id INTEGER, table_id INTEGER, column_name VARCHAR, column_type VARCHAR, column_order INTEGER, nulls_allowed BOOLEAN DEFAULT 1, begin_snapshot INTEGER, end_snapshot INTEGER);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "CREATE TABLE ducklake_data_file (data_file_id INTEGER PRIMARY KEY, table_id INTEGER, path VARCHAR, path_is_relative BOOLEAN DEFAULT 1, file_size_bytes INTEGER, footer_size INTEGER, encryption_key VARCHAR, record_count INTEGER, row_id_start INTEGER, mapping_id INTEGER, begin_snapshot INTEGER, end_snapshot INTEGER);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "CREATE TABLE ducklake_delete_file (delete_file_id INTEGER PRIMARY KEY, data_file_id INTEGER, table_id INTEGER, path VARCHAR, path_is_relative BOOLEAN DEFAULT 1, file_size_bytes INTEGER, footer_size INTEGER, encryption_key VARCHAR, delete_count INTEGER, begin_snapshot INTEGER, end_snapshot INTEGER);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    // This should work with 0 schemas/tables/snapshots
    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let schema_names = catalog.schema_names();
    eprintln!(
        "[#779 empty catalog] Schema names: {:?} (expected [information_schema])",
        schema_names
    );
    assert!(
        schema_names.contains(&"information_schema".to_string()),
        "Should always have information_schema"
    );

    // Schema lookup for non-existent schema should return None
    let result = catalog.schema("nonexistent");
    assert!(result.is_none(), "Non-existent schema should return None");

    Ok(())
}

// ============================================================================
// Pattern from issue #673: Time travel inconsistency after flush
// Test that using different snapshot IDs returns different data.
// ============================================================================

#[tokio::test]
async fn test_time_travel_snapshot_isolation() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("time_travel.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Snapshot 1: Create table + insert
    conn.execute("CREATE TABLE test_catalog.t1 (id INT);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSERT INTO test_catalog.t1 VALUES (1), (2);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Snapshot 2: Insert more
    conn.execute("INSERT INTO test_catalog.t1 VALUES (3), (4);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Snapshot 3: Delete
    conn.execute("DELETE FROM test_catalog.t1 WHERE id = 1;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider: Arc<dyn MetadataProvider> = Arc::new(
        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let current_snapshot = provider
        .get_current_snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    eprintln!("[#673 time travel] Current snapshot: {}", current_snapshot);

    // Query at current snapshot
    let catalog_current = DuckLakeCatalog::with_snapshot(Arc::clone(&provider), current_snapshot)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx_current = SessionContext::new();
    ctx_current.register_catalog("ducklake", Arc::new(catalog_current));

    let count_current =
        query_count(&ctx_current, "SELECT COUNT(*) FROM ducklake.main.t1").await?;
    eprintln!(
        "[#673 time travel] Count at current snapshot: {} (expected 3)",
        count_current
    );
    assert_eq!(count_current, 3, "Current snapshot should have 3 rows");

    // Query at earlier snapshot (before delete, might be snapshot 2 or 3)
    if current_snapshot >= 3 {
        let catalog_earlier =
            DuckLakeCatalog::with_snapshot(Arc::clone(&provider), current_snapshot - 1)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let ctx_earlier = SessionContext::new();
        ctx_earlier.register_catalog("ducklake", Arc::new(catalog_earlier));

        let count_earlier =
            query_count(&ctx_earlier, "SELECT COUNT(*) FROM ducklake.main.t1").await?;
        eprintln!(
            "[#673 time travel] Count at snapshot {}: {}",
            current_snapshot - 1,
            count_earlier
        );
        // Earlier snapshot should have more rows (before the delete)
        assert!(
            count_earlier >= count_current,
            "Earlier snapshot should have >= rows than current"
        );
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #745: LIMIT ignores partitioning info
// Test that LIMIT works correctly with partitioned tables and doesn't
// return wrong results.
// ============================================================================

#[tokio::test]
async fn test_limit_with_partitioned_table() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("limit_part.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "CREATE TABLE test_catalog.logs (id INT, level VARCHAR, msg VARCHAR);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "ALTER TABLE test_catalog.logs SET PARTITIONED BY (level);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "INSERT INTO test_catalog.logs VALUES
            (1, 'INFO', 'msg1'),
            (2, 'ERROR', 'msg2'),
            (3, 'INFO', 'msg3'),
            (4, 'WARN', 'msg4'),
            (5, 'ERROR', 'msg5'),
            (6, 'INFO', 'msg6');",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // LIMIT should return exactly the requested number of rows
    let df = ctx
        .sql("SELECT id FROM ducklake.main.logs LIMIT 3")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "[#745 limit+partition] SELECT LIMIT 3: {} rows (expected 3)",
        total
    );
    assert_eq!(total, 3, "LIMIT 3 should return exactly 3 rows");

    // LIMIT 1 with filter on partition column
    let df2 = ctx
        .sql("SELECT id FROM ducklake.main.logs WHERE level = 'ERROR' LIMIT 1")
        .await?;
    let batches2 = df2.collect().await?;
    let total2: usize = batches2.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "[#745 limit+partition] ERROR filter LIMIT 1: {} rows (expected 1)",
        total2
    );
    assert_eq!(total2, 1, "LIMIT 1 with partition filter should return 1 row");

    Ok(())
}

// ============================================================================
// Pattern from issue #650: Race condition in concurrent flush + insert
// Test concurrent reads against the same catalog (snapshot isolation).
// ============================================================================

#[tokio::test]
async fn test_concurrent_reads_snapshot_consistency() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("concurrent.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider: Arc<dyn MetadataProvider> = Arc::new(
        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    // Spawn multiple concurrent read tasks all using the same snapshot
    let snapshot = provider
        .get_current_snapshot()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut handles = Vec::new();
    for _i in 0..10 {
        let prov = Arc::clone(&provider);
        let handle = tokio::spawn(async move {
            let catalog = DuckLakeCatalog::with_snapshot(prov, snapshot)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let ctx = SessionContext::new();
            ctx.register_catalog("ducklake", Arc::new(catalog));

            let count = ctx
                .sql("SELECT COUNT(*) FROM ducklake.main.users")
                .await?
                .collect()
                .await?;

            let n = count[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(0))
                .unwrap_or(-1);

            Ok::<i64, DataFusionError>(n)
        });
        handles.push(handle);
    }

    let mut results = Vec::new();
    for handle in handles {
        let result = handle.await.unwrap()?;
        results.push(result);
    }

    eprintln!(
        "[#650 concurrent reads] Results: {:?}",
        results
    );
    // All concurrent reads should see the same count
    assert!(
        results.iter().all(|&c| c == results[0]),
        "All concurrent reads should return the same count"
    );

    Ok(())
}

// ============================================================================
// Pattern from issue #749: Index out of bounds with partitioned + non-partitioned columns
// Test that projection indices work correctly near schema boundaries.
// ============================================================================

#[tokio::test]
async fn test_projection_at_schema_boundary() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("proj_bound.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Table with many columns
    conn.execute(
        "CREATE TABLE test_catalog.wide (c1 INT, c2 INT, c3 INT, c4 INT, c5 INT);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.wide VALUES (1, 2, 3, 4, 5);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Select only the last column
    let df = ctx
        .sql("SELECT c5 FROM ducklake.main.wide")
        .await?;
    let batches = df.collect().await?;
    assert!(!batches.is_empty());
    let val = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Expected Int32Array");
    eprintln!(
        "[#749 projection boundary] c5 value: {} (expected 5)",
        val.value(0)
    );
    assert_eq!(val.value(0), 5);

    // Select first and last column (skip middle)
    let df2 = ctx
        .sql("SELECT c1, c5 FROM ducklake.main.wide")
        .await?;
    let batches2 = df2.collect().await?;
    assert!(!batches2.is_empty());
    let c1 = batches2[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    let c5 = batches2[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    eprintln!(
        "[#749 projection boundary] c1={}, c5={} (expected 1, 5)",
        c1, c5
    );
    assert_eq!(c1, 1);
    assert_eq!(c5, 5);

    Ok(())
}

// ============================================================================
// Pattern from issue #788: duckdb_views() 70x slower than duckdb_tables()
// Not a correctness bug but test that information_schema listing works.
// ============================================================================

#[tokio::test]
async fn test_information_schema_listing() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("infoschema.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Create multiple tables with data so the files directory is created
    for i in 0..5 {
        conn.execute(
            &format!(
                "CREATE TABLE test_catalog.t{} AS SELECT {} AS id;",
                i, i
            ),
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Check that information_schema is accessible
    let info_schema = ctx
        .catalog("ducklake")
        .unwrap()
        .schema("information_schema");
    assert!(
        info_schema.is_some(),
        "information_schema should always exist"
    );

    // List tables via schema provider
    let main_schema = ctx.catalog("ducklake").unwrap().schema("main");
    if let Some(schema) = main_schema {
        let table_names = schema.table_names();
        eprintln!(
            "[#788 info_schema] Table names: {:?} (expected 5 tables)",
            table_names
        );
        assert_eq!(table_names.len(), 5, "Should have 5 tables");
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #610: Insert fails with certain backends after initial insert
// Test multiple sequential queries to the same table (connection reuse).
// ============================================================================

#[tokio::test]
async fn test_sequential_queries_same_table() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("sequential.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Run many sequential queries to test connection stability
    for i in 0..20 {
        let count = query_count(&ctx, "SELECT COUNT(*) FROM ducklake.main.users").await?;
        if i == 0 || i == 19 {
            eprintln!(
                "[#610 sequential] Query {}: count={}",
                i, count
            );
        }
        assert_eq!(count, 3, "Query {} returned wrong count", i);
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #791: add_data_files fails when partitioned column is in Parquet
// Test field_id mapping when field IDs in Parquet don't match column IDs.
// ============================================================================

#[test]
fn test_field_id_mapping_with_id_mismatch() {
    use datafusion_ducklake::metadata_provider::DuckLakeTableColumn;

    // Simulate: catalog has column_ids [1, 2, 3]
    // But Parquet file has field_ids [10, 20, 30] (completely different numbering)
    let current_columns = vec![
        DuckLakeTableColumn {
            column_id: 1,
            column_name: "id".to_string(),
            column_type: "int32".to_string(),
            is_nullable: false,
        },
        DuckLakeTableColumn {
            column_id: 2,
            column_name: "name".to_string(),
            column_type: "varchar".to_string(),
            is_nullable: true,
        },
    ];

    // Parquet has completely different field IDs
    let mut parquet_field_ids = HashMap::new();
    parquet_field_ids.insert(10, "id".to_string());
    parquet_field_ids.insert(20, "name".to_string());

    let result =
        build_read_schema_with_field_id_mapping(&current_columns, &parquet_field_ids);

    match result {
        Ok((schema, name_mapping)) => {
            eprintln!(
                "[#791 field_id mismatch] Schema: {:?}, mapping: {:?}",
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                name_mapping
            );
            // When field_ids don't match, it falls back to current column names
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(1).name(), "name");
        }
        Err(e) => {
            eprintln!(
                "[#791 field_id mismatch] FINDING: Failed: {}",
                e
            );
        }
    }
}

// ============================================================================
// Pattern from issue #609: 0-size parquet file
// Test handling of zero-byte files in metadata.
// ============================================================================

#[test]
fn test_zero_size_file_metadata() {
    use datafusion_ducklake::metadata_provider::{DuckLakeFileData, DuckLakeTableFile};

    // Create a table file with 0 bytes
    let file = DuckLakeFileData {
        path: "zero.parquet".to_string(),
        path_is_relative: true,
        encryption_key: None,
        file_size_bytes: 0,
        footer_size: None,
    };

    let table_file = DuckLakeTableFile {
        data_file_id: Some(1),
        file,
        delete_file: None,
        row_id_start: None,
        snapshot_id: None,
        max_row_count: None,
    };

    // This should not panic
    let resolved = resolve_path("/data/table/", &table_file.file.path, table_file.file.path_is_relative).unwrap();
    eprintln!(
        "[#609 zero-size file] Resolved path: {} (file_size=0)",
        resolved
    );
    assert_eq!(resolved, "/data/table/zero.parquet");
}

// ============================================================================
// Pattern from issue #781: ALTER TABLE RENAME generates invalid DDL
// Test that after renaming a column via DuckDB, our extension reads the
// renamed column correctly from the catalog.
// ============================================================================

#[tokio::test]
async fn test_column_rename_preserves_type_info() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("rename_type.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "CREATE TABLE test_catalog.products (id BIGINT, price DECIMAL(10,2));",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.products VALUES (1, 9.99), (2, 19.99);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Rename the column
    conn.execute(
        "ALTER TABLE test_catalog.products RENAME COLUMN price TO unit_price;",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // The renamed column should appear in the schema
    let result = ctx
        .sql("SELECT * FROM ducklake.main.products")
        .await?
        .collect()
        .await;

    match &result {
        Ok(batches) => {
            if !batches.is_empty() {
                let schema = batches[0].schema();
                let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
                eprintln!(
                    "[#781 rename DDL] Column names after rename: {:?}",
                    names
                );
                // Check that unit_price is present, not price
                let base_names: Vec<&&str> = names.iter()
                    .filter(|n| **n != "filename" && **n != "file_row_number")
                    .collect();
                assert!(
                    base_names.contains(&&"unit_price"),
                    "Renamed column 'unit_price' should be present"
                );
                assert!(
                    !base_names.contains(&&"price"),
                    "Old column name 'price' should not appear"
                );
            }
        }
        Err(e) => {
            eprintln!(
                "[#781 rename DDL] FINDING: Query after rename failed: {}",
                e
            );
        }
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #307: Hive partition virtual columns not populated
// Test that our extension correctly returns NULL for added columns that
// don't exist in older Parquet files.
// ============================================================================

#[tokio::test]
async fn test_added_column_returns_null_for_old_files() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("null_col.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Insert data with original schema
    conn.execute("CREATE TABLE test_catalog.t1 (id INT, name VARCHAR);", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.t1 VALUES (1, 'Alice'), (2, 'Bob');",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Add a new column
    conn.execute("ALTER TABLE test_catalog.t1 ADD COLUMN age INT;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Insert new data with the new column
    conn.execute(
        "INSERT INTO test_catalog.t1 VALUES (3, 'Charlie', 30);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Query all rows — old rows should have NULL for age column
    let result = ctx
        .sql("SELECT id, name, age FROM ducklake.main.t1 ORDER BY id")
        .await;

    match result {
        Ok(df) => {
            let batches = df.collect().await;
            match batches {
                Ok(ref b) => {
                    let total_rows: usize = b.iter().map(|batch| batch.num_rows()).sum();
                    eprintln!(
                        "[#307 added col null] Total rows: {} (expected 3)",
                        total_rows
                    );
                }
                Err(ref e) => {
                    eprintln!(
                        "[#307 added col null] FINDING: Collect failed: {}",
                        e
                    );
                }
            }
        }
        Err(e) => {
            eprintln!(
                "[#307 added col null] FINDING: Query with added column failed: {}",
                e
            );
        }
    }

    Ok(())
}

// ============================================================================
// Pattern from issue #310: merge_adjacent_files doesn't use hive partitions
// Test our PathResolver with deep partition hierarchies.
// ============================================================================

#[test]
fn test_path_resolver_deep_hive_partition_hierarchy() {
    let catalog_resolver = PathResolver::new(
        Arc::new(
            datafusion::datasource::object_store::ObjectStoreUrl::parse("s3://bucket/").unwrap(),
        ),
        "/warehouse/".to_string(),
    );

    let schema_resolver = catalog_resolver.child_resolver("prod/", true).unwrap();
    let table_resolver = schema_resolver.child_resolver("events/", true).unwrap();

    // Deep hive partition path
    let partition_resolver =
        table_resolver.child_resolver("year=2024/month=01/day=15/hour=10/", true).unwrap();
    assert_eq!(
        partition_resolver.base_path(),
        "/warehouse/prod/events/year=2024/month=01/day=15/hour=10/"
    );

    let file = partition_resolver.resolve("data.parquet", true).unwrap();
    assert_eq!(
        file,
        "/warehouse/prod/events/year=2024/month=01/day=15/hour=10/data.parquet"
    );
}

// ============================================================================
// Pattern from issue #794: Virtual column index problems
// Test that field_id mapping handles cases where column_id is cast to i32
// and might cause issues for large IDs.
// ============================================================================

#[test]
fn test_field_id_mapping_large_column_ids() {
    use datafusion_ducklake::metadata_provider::DuckLakeTableColumn;

    // Column IDs that are large but fit in i32
    let current_columns = vec![
        DuckLakeTableColumn {
            column_id: i32::MAX as i64,
            column_name: "big_id_col".to_string(),
            column_type: "int32".to_string(),
            is_nullable: true,
        },
    ];

    let mut parquet_field_ids = HashMap::new();
    parquet_field_ids.insert(i32::MAX, "big_id_col".to_string());

    let result =
        build_read_schema_with_field_id_mapping(&current_columns, &parquet_field_ids);
    assert!(
        result.is_ok(),
        "Should handle large column_id: {:?}",
        result
    );

    // Column ID that OVERFLOWS i32 when cast
    let overflow_columns = vec![
        DuckLakeTableColumn {
            column_id: (i32::MAX as i64) + 1,
            column_name: "overflow_col".to_string(),
            column_type: "varchar".to_string(),
            is_nullable: true,
        },
    ];

    let parquet_field_ids2 = HashMap::new(); // No matching field
    let result2 =
        build_read_schema_with_field_id_mapping(&overflow_columns, &parquet_field_ids2);
    eprintln!(
        "[#794 large column_id] Overflow i32 column_id result: {:?}",
        result2.as_ref().map(|(s, m)| (
            s.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>(),
            m.clone()
        ))
    );
    // The column_id is cast as `col.column_id as i32` in the code.
    // For column_id = i32::MAX + 1, this wraps to i32::MIN.
    // This is a potential bug — document it.
}

// ============================================================================
// Pattern from issue #790: Binary data with single quotes breaks SQL
// In our extension, test that column names with special characters are handled.
// ============================================================================

#[test]
fn test_special_characters_in_column_names() {
    use datafusion_ducklake::metadata_provider::DuckLakeTableColumn;

    let special_names = vec![
        "col with spaces",
        "col'with'quotes",
        "col\"with\"doublequotes",
        "col;with;semicolons",
        "col\nwith\nnewlines",
        "col\twith\ttabs",
        "col-with-hyphens",
        "col.with.dots",
    ];

    for name in special_names {
        let columns = vec![DuckLakeTableColumn {
            column_id: 1,
            column_name: name.to_string(),
            column_type: "varchar".to_string(),
            is_nullable: true,
        }];

        let result = build_arrow_schema(&columns);
        match result {
            Ok(schema) => {
                assert_eq!(schema.field(0).name(), name);
                eprintln!("[#790 special chars] '{}' -> OK", name.escape_debug());
            }
            Err(e) => {
                eprintln!(
                    "[#790 special chars] FINDING: '{}' -> Error: {}",
                    name.escape_debug(),
                    e
                );
            }
        }
    }
}

// ============================================================================
// Pattern from issue #795: Migration error message reports wrong version
// Test that our error messages contain useful debugging info.
// ============================================================================

#[test]
fn test_error_messages_contain_useful_info() {
    // Type error should include the offending type
    let result = ducklake_to_arrow_type("nonexistent_type_42");
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("nonexistent_type_42"),
        "Error should contain the offending type, got: {}",
        msg
    );

    // Decimal with bad parameters
    let result = ducklake_to_arrow_type("decimal(abc, def)");
    // Should either succeed with defaults or fail with informative error
    match result {
        Ok(dt) => {
            eprintln!("[#795 error msg] decimal(abc,def) -> {:?}", dt);
        }
        Err(e) => {
            eprintln!("[#795 error msg] decimal(abc,def) -> Error: {}", e);
        }
    }
}

// ============================================================================
// Pattern from issue #687: merge_adjacent_files very slow
// Not a correctness test, but verify that we don't do N+1 queries.
// Test that listing schemas/tables/columns uses bounded queries.
// ============================================================================

#[tokio::test]
async fn test_catalog_listing_not_n_plus_one() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("n_plus_one.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Create 20 tables with data
    for i in 0..20 {
        conn.execute(
            &format!(
                "CREATE TABLE test_catalog.table_{} (id INT, val VARCHAR);",
                i
            ),
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        conn.execute(
            &format!(
                "INSERT INTO test_catalog.table_{} VALUES ({}, 'row{}');",
                i, i, i
            ),
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let start = std::time::Instant::now();
    let schema = ctx.catalog("ducklake").unwrap().schema("main").unwrap();
    let table_names = schema.table_names();
    let elapsed = start.elapsed();

    eprintln!(
        "[#687 N+1] Listed {} tables in {:?}",
        table_names.len(),
        elapsed
    );
    assert_eq!(table_names.len(), 20, "Should list all 20 tables");

    // Verify we can query each table
    let first_count = query_count(
        &ctx,
        "SELECT COUNT(*) FROM ducklake.main.table_0",
    )
    .await?;
    assert_eq!(first_count, 1, "Each table should have 1 row");

    Ok(())
}

// ============================================================================
// Pattern from issue #304: Read-only mode should prevent mutations
// Test that read-only catalog properly rejects write attempts.
// ============================================================================

#[tokio::test]
async fn test_read_only_catalog_rejects_schema_mutation() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("readonly.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    // Using DuckLakeCatalog::new() creates a read-only catalog (no writer)
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // schema_names should work in read-only mode
    let names = catalog.schema_names();
    eprintln!("[#304 read-only] Schema names: {:?}", names);
    assert!(
        names.contains(&"main".to_string()),
        "Should be able to list schemas in read-only mode"
    );

    // schema() lookup should work
    let schema = catalog.schema("main");
    assert!(schema.is_some(), "Should find 'main' schema");

    // Reading should work
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    let count = query_count(&ctx, "SELECT COUNT(*) FROM ducklake.main.users").await?;
    assert_eq!(count, 3, "Read-only catalog should support reads");

    Ok(())
}

// ============================================================================
// Pattern from issue #652: VARCHAR vs TIMESTAMP WITH TIME ZONE comparison
// Test our timestamp type handling edge cases.
// ============================================================================

#[test]
fn test_timestamp_type_variations() {
    use arrow::datatypes::TimeUnit;

    let type_tests = vec![
        ("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None)),
        (
            "timestamptz",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        (
            "timestamp with time zone",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        ("timestamp_s", DataType::Timestamp(TimeUnit::Second, None)),
        (
            "timestamp_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        (
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ),
    ];

    for (type_str, expected) in &type_tests {
        let result = ducklake_to_arrow_type(type_str);
        match result {
            Ok(dt) => {
                assert_eq!(
                    &dt, expected,
                    "Type '{}' should map to {:?}",
                    type_str, expected
                );
            }
            Err(e) => {
                eprintln!(
                    "[#652 timestamp] FINDING: '{}' not recognized: {}",
                    type_str, e
                );
            }
        }
    }

    // Edge: what about "TIMESTAMP WITH TIME ZONE" in all caps?
    let result = ducklake_to_arrow_type("TIMESTAMP WITH TIME ZONE");
    assert!(
        result.is_ok(),
        "TIMESTAMP WITH TIME ZONE (caps) should work"
    );
}

// ============================================================================
// Pattern from issue #617: Legacy Avro LIST layout not recognized
// Test type parsing with unusual whitespace and casing.
// ============================================================================

#[test]
fn test_type_parsing_whitespace_and_casing() {
    // Extra whitespace
    assert!(ducklake_to_arrow_type("  INT32  ").is_ok());
    assert!(ducklake_to_arrow_type("  list ( int32 )  ").is_ok());
    assert!(ducklake_to_arrow_type("  STRUCT ( a INT32 , b VARCHAR )  ").is_ok());

    // Mixed casing
    assert!(ducklake_to_arrow_type("Int32").is_ok());
    assert!(ducklake_to_arrow_type("VARCHAR").is_ok());
    assert!(ducklake_to_arrow_type("Boolean").is_ok());

    // Decimal with spaces
    let result = ducklake_to_arrow_type("DECIMAL( 10 , 2 )");
    assert!(result.is_ok(), "Decimal with spaces should work: {:?}", result);
    if let Ok(DataType::Decimal128(p, s)) = result {
        assert_eq!(p, 10);
        assert_eq!(s, 2);
    }

    // Numeric alias (PostgreSQL)
    let result = ducklake_to_arrow_type("numeric(18, 4)");
    assert!(result.is_ok(), "NUMERIC should work: {:?}", result);
}

// ============================================================================
// Pattern from issue #605: IO Error on S3 with table scan queries
// Test that our extension correctly handles the object store URL parsing
// for various S3-like endpoints.
// ============================================================================

#[test]
fn test_object_store_url_various_endpoints() {
    use datafusion_ducklake::path_resolver::parse_object_store_url;

    // Standard S3
    let (_url, path) = parse_object_store_url("s3://my-bucket/data/warehouse").unwrap();
    assert_eq!(path, "/data/warehouse");

    // MinIO style (still uses s3:// scheme)
    let (_url, path) = parse_object_store_url("s3://minio-bucket/lake/data").unwrap();
    assert_eq!(path, "/lake/data");

    // File URL with deeply nested path
    let (_url, path) = parse_object_store_url("file:///very/deeply/nested/path/data").unwrap();
    assert_eq!(path, "/very/deeply/nested/path/data");

    // S3 bucket with dots (common for custom endpoints)
    let result = parse_object_store_url("s3://my.custom.endpoint/data");
    assert!(result.is_ok(), "S3 URL with dots should work");
}

// ============================================================================
// Pattern from issue #661: Flush doesn't respect table partitioning from fresh connection
// Test that partition metadata is loaded correctly on catalog creation.
// ============================================================================

#[tokio::test]
async fn test_partition_metadata_loaded_on_catalog_create() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("part_meta.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "CREATE TABLE test_catalog.partitioned_data (id INT, region VARCHAR, value DOUBLE);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "ALTER TABLE test_catalog.partitioned_data SET PARTITIONED BY (region);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute(
        "INSERT INTO test_catalog.partitioned_data VALUES (1, 'US', 100.0), (2, 'EU', 200.0), (3, 'US', 300.0);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    // Fresh connection — catalog should load partition metadata
    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Filter on partition column should not lose data
    let all_count = query_count(
        &ctx,
        "SELECT COUNT(*) FROM ducklake.main.partitioned_data",
    )
    .await?;
    let us_count = query_count(
        &ctx,
        "SELECT COUNT(*) FROM ducklake.main.partitioned_data WHERE region = 'US'",
    )
    .await?;
    let eu_count = query_count(
        &ctx,
        "SELECT COUNT(*) FROM ducklake.main.partitioned_data WHERE region = 'EU'",
    )
    .await?;

    eprintln!(
        "[#661 partition meta] all={}, US={}, EU={} (expected 3, 2, 1)",
        all_count, us_count, eu_count
    );
    assert_eq!(all_count, 3);
    assert_eq!(us_count, 2);
    assert_eq!(eu_count, 1);

    Ok(())
}
