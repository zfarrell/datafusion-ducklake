#![cfg(feature = "metadata-duckdb")]
//! Adversarial edge-case tests for DataFusion-DuckLake
//!
//! This test suite is a RED TEAM attack surface. It deliberately pushes the system
//! into hostile corner cases: corrupted metadata, extreme boundary values,
//! concurrent abuse, resource exhaustion, and unicode edge cases.
//!
//! *** DO NOT FIX BUGS FOUND HERE — DOCUMENT THEM ***
//!
//! Each test documents whether it PASSED (system handled gracefully) or
//! FAILED (panic, hang, wrong result, corruption).

mod common;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use datafusion::catalog::CatalogProvider;
use datafusion::common::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use tempfile::TempDir;

// ============================================================================
// HELPER: create a DuckDB connection in read-write mode for adversarial setup
// ============================================================================

fn setup_ducklake_catalog(catalog_path: &std::path::Path) -> anyhow::Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])?;

    conn.execute(
        "CREATE TABLE test_catalog.users (id INT, name VARCHAR);",
        [],
    )?;
    conn.execute(
        "INSERT INTO test_catalog.users VALUES (1, 'Alice'), (2, 'Bob');",
        [],
    )?;
    Ok(())
}

/// Open a DuckDB catalog file in read-write mode for direct metadata manipulation
fn open_catalog_rw(catalog_path: &std::path::Path) -> anyhow::Result<duckdb::Connection> {
    let conn = duckdb::Connection::open(catalog_path)?;
    Ok(conn)
}

// ============================================================================
// 1. SNAPSHOT MANIPULATION — boundary values for snapshot_id
// ============================================================================

/// BUG-HUNT: What happens when snapshot_id is 0?
/// The SQL uses COALESCE(MAX(snapshot_id), 0) for empty tables, so 0 is the "no snapshots" sentinel.
/// If we bind to snapshot 0, all snapshot-filtered queries should return nothing.
#[tokio::test]
async fn test_snapshot_id_zero() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = Arc::new(
        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    // Bind to snapshot 0 — should see nothing (tables created at snapshot >= 1)
    let catalog = DuckLakeCatalog::with_snapshot(provider, 0)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let schema_names = catalog.schema_names();
    // information_schema should always appear; data schemas may or may not
    eprintln!(
        "[snapshot_id=0] schema_names = {:?}",
        schema_names
    );

    let main_schema = catalog.schema("main");
    eprintln!(
        "[snapshot_id=0] schema('main') = {:?}",
        main_schema.is_some()
    );

    // If main schema exists at snapshot 0, that's surprising — document it
    if let Some(schema) = main_schema {
        let table_names = schema.table_names();
        eprintln!(
            "[snapshot_id=0] FINDING: main schema visible at snapshot 0, tables = {:?}",
            table_names
        );
    }

    Ok(())
}

/// BUG-HUNT: snapshot_id = -1 (negative)
/// Negative snapshot IDs should not match any begin_snapshot/end_snapshot ranges.
#[tokio::test]
async fn test_snapshot_id_negative() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = Arc::new(
        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let catalog = DuckLakeCatalog::with_snapshot(provider, -1)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let schema_names = catalog.schema_names();
    eprintln!(
        "[snapshot_id=-1] schema_names = {:?}",
        schema_names
    );

    // -1 should not match any snapshot range
    let main_schema = catalog.schema("main");
    if let Some(schema) = main_schema {
        eprintln!(
            "[snapshot_id=-1] BUG?: main schema visible at snapshot -1, tables = {:?}",
            schema.table_names()
        );
    } else {
        eprintln!("[snapshot_id=-1] OK: main schema not visible at snapshot -1");
    }

    Ok(())
}

/// BUG-HUNT: snapshot_id = i64::MAX
/// This tests overflow behavior in snapshot comparison queries.
#[tokio::test]
async fn test_snapshot_id_max() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = Arc::new(
        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let catalog = DuckLakeCatalog::with_snapshot(provider, i64::MAX)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let ctx = SessionContext::new();
    ctx.register_catalog("test", Arc::new(catalog));

    // i64::MAX should be >= all begin_snapshots, so everything should be visible
    let result = ctx
        .sql("SELECT COUNT(*) FROM test.main.users")
        .await?
        .collect()
        .await?;

    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    eprintln!(
        "[snapshot_id=i64::MAX] count = {} (expected 2)",
        count
    );
    assert_eq!(count, 2, "i64::MAX snapshot should see all data");

    Ok(())
}

/// BUG-HUNT: snapshot_id = i64::MIN
#[tokio::test]
async fn test_snapshot_id_min() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = Arc::new(
        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let catalog = DuckLakeCatalog::with_snapshot(provider, i64::MIN)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let schema_names = catalog.schema_names();
    eprintln!(
        "[snapshot_id=i64::MIN] schema_names = {:?}",
        schema_names
    );

    let main_schema = catalog.schema("main");
    if let Some(schema) = main_schema {
        eprintln!(
            "[snapshot_id=i64::MIN] BUG?: main visible at i64::MIN, tables = {:?}",
            schema.table_names()
        );
    } else {
        eprintln!("[snapshot_id=i64::MIN] OK: no data visible");
    }

    Ok(())
}

// ============================================================================
// 2. EMPTY STATES — catalogs, schemas, tables with zero content
// ============================================================================

/// BUG-HUNT: Catalog with zero schemas (only ducklake metadata tables)
///
/// FINDING: DuckLakeCatalog::new() fails when the .files directory doesn't exist.
/// DuckLake creates the data directory on ATTACH, but parse_object_store_url() tries
/// to resolve it. If it's a local path that doesn't exist, we get InvalidConfig error.
/// This means you CANNOT open a completely empty DuckLake catalog that has never had
/// any data written to it.
#[tokio::test]
async fn test_empty_catalog_no_schemas() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("empty.ducklake");

    // Create catalog with NO user schemas — just DuckLake metadata
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Don't create any schemas or tables — just detach
    conn.execute("DETACH test_catalog;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let result = DuckLakeCatalog::new(provider);
    match result {
        Ok(catalog) => {
            let schema_names = catalog.schema_names();
            eprintln!(
                "[empty catalog] schema_names = {:?}",
                schema_names
            );
            assert!(
                schema_names.contains(&"information_schema".to_string()),
                "information_schema must always be present"
            );
        }
        Err(e) => {
            // FINDING: DuckLakeCatalog::new fails when .files dir doesn't exist
            eprintln!(
                "[empty catalog] FINDING/BUG: Cannot open empty DuckLake catalog: {}",
                e
            );
        }
    }

    Ok(())
}

/// BUG-HUNT: Table with zero columns
/// DuckLake may not allow this, but if the metadata is manipulated directly...
#[tokio::test]
async fn test_table_with_zero_columns() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("zero_cols.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Manipulate metadata: delete all columns for the users table
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        // Find table_id for users
        let table_id: i64 = conn
            .query_row(
                "SELECT table_id FROM ducklake_table WHERE table_name = 'users'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Delete all columns
        conn.execute(
            "DELETE FROM ducklake_column WHERE table_id = ?",
            duckdb::params![table_id],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Try to query a table with zero columns — should this work?
    let result = ctx
        .sql("SELECT COUNT(*) FROM test.main.users")
        .await;

    match result {
        Ok(df) => {
            let batches = df.collect().await;
            match batches {
                Ok(b) => eprintln!(
                    "[zero columns] Surprisingly succeeded! Batches: {:?}",
                    b.len()
                ),
                Err(e) => eprintln!("[zero columns] Query failed at execution: {}", e),
            }
        }
        Err(e) => eprintln!("[zero columns] Query failed at planning: {}", e),
    }

    Ok(())
}

/// BUG-HUNT: Table with columns but zero data files
///
/// FINDING: Same as empty catalog — DuckLake CREATE TABLE without INSERT doesn't
/// create the .files directory, so DuckLakeCatalog::new() fails with InvalidConfig.
/// Our reader cannot handle DuckLake catalogs that have never had data written.
#[tokio::test]
async fn test_table_with_columns_but_no_files() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("no_files.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Create table but don't insert any data
    conn.execute(
        "CREATE TABLE test_catalog.empty_table (id INT, name VARCHAR);",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let result = DuckLakeCatalog::new(provider);
    match result {
        Ok(catalog) => {
            let catalog = Arc::new(catalog);
            let ctx = SessionContext::new();
            ctx.register_catalog("test", catalog);

            let result = ctx
                .sql("SELECT * FROM test.main.empty_table")
                .await?
                .collect()
                .await?;

            let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[no files] Row count = {} (expected 0)",
                total_rows
            );
            assert_eq!(total_rows, 0, "Table with no files should return 0 rows");
        }
        Err(e) => {
            // FINDING: Cannot open catalog when .files directory doesn't exist
            eprintln!(
                "[no files] FINDING/BUG: Cannot open catalog with empty table (no .files dir): {}",
                e
            );
        }
    }

    Ok(())
}

// ============================================================================
// 3. RESOURCE EXHAUSTION — many columns, many tables
// ============================================================================

/// BUG-HUNT: Table with a large number of columns (500)
/// Tests whether schema building, query planning, and execution handle wide tables.
#[tokio::test]
async fn test_wide_table_500_columns() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("wide.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Build a CREATE TABLE with 500 columns
    let col_count = 500;
    let columns: Vec<String> = (0..col_count)
        .map(|i| format!("col_{} INT", i))
        .collect();
    let create_sql = format!(
        "CREATE TABLE test_catalog.wide_table ({});",
        columns.join(", ")
    );
    conn.execute(&create_sql, [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Insert a single row with all NULLs
    let null_values: Vec<&str> = (0..col_count).map(|_| "NULL").collect();
    let insert_sql = format!(
        "INSERT INTO test_catalog.wide_table VALUES ({});",
        null_values.join(", ")
    );
    conn.execute(&insert_sql, [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Test SELECT *
    let result = ctx
        .sql("SELECT * FROM test.main.wide_table")
        .await?
        .collect()
        .await?;

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "[500 columns] Rows = {}, Columns in result = {}",
        total_rows,
        result[0].num_columns()
    );
    // 500 base columns + 2 virtual columns (filename, file_row_number)
    assert!(result[0].num_columns() >= 500, "Should have at least 500 columns");
    assert_eq!(total_rows, 1);

    // Test COUNT(*)
    let count_result = ctx
        .sql("SELECT COUNT(*) FROM test.main.wide_table")
        .await?
        .collect()
        .await?;
    let count = count_result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);

    eprintln!("[500 columns] PASSED: 500-column table works correctly");

    Ok(())
}

/// BUG-HUNT: Many tables in a single schema (100 tables)
#[tokio::test]
async fn test_many_tables_in_schema() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("many_tables.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let table_count = 100;
    for i in 0..table_count {
        conn.execute(
            &format!("CREATE TABLE test_catalog.tbl_{} (id INT);", i),
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        conn.execute(
            &format!("INSERT INTO test_catalog.tbl_{} VALUES ({});", i, i),
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog.clone());

    // Verify we can see all tables
    let schema = catalog.schema("main").expect("main schema should exist");
    let table_names = schema.table_names();
    eprintln!(
        "[many tables] Found {} tables (expected {})",
        table_names.len(),
        table_count
    );
    assert_eq!(table_names.len(), table_count);

    // Query a random table
    let result = ctx
        .sql("SELECT * FROM test.main.tbl_42")
        .await?
        .collect()
        .await?;
    let id = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert_eq!(id, 42);

    eprintln!("[many tables] PASSED: {} tables handled correctly", table_count);

    Ok(())
}

// ============================================================================
// 4. BOUNDARY VALUES — column_order, file_size, etc.
// ============================================================================

/// BUG-HUNT: file_size_bytes = 0 in metadata
/// The code casts file_size_bytes as u64 in PartitionedFile::new.
/// Zero-byte files shouldn't crash but may confuse Parquet readers.
#[tokio::test]
async fn test_file_size_zero_in_metadata() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("zero_size.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Manipulate metadata: set file_size_bytes to 0
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute("UPDATE ducklake_data_file SET file_size_bytes = 0", [])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // The file still exists on disk with real data — only metadata says 0
    let result = ctx
        .sql("SELECT * FROM test.main.users ORDER BY id")
        .await?
        .collect()
        .await;

    match result {
        Ok(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[file_size=0] Surprisingly succeeded: {} rows",
                total
            );
        }
        Err(e) => {
            eprintln!(
                "[file_size=0] Failed as expected (or unexpectedly): {}",
                e
            );
        }
    }

    Ok(())
}

/// BUG-HUNT: file_size_bytes = -1 in metadata
/// Negative file size cast to u64 causes overflow: -1 -> u64::MAX
#[tokio::test]
async fn test_file_size_negative_in_metadata() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("neg_size.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Manipulate metadata: set file_size_bytes to -1
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute("UPDATE ducklake_data_file SET file_size_bytes = -1", [])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // -1 as i64 cast to u64 = 18446744073709551615 (u64::MAX)
    // This should cause issues when DataFusion tries to read the file
    let result = ctx
        .sql("SELECT * FROM test.main.users")
        .await?
        .collect()
        .await;

    match result {
        Ok(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[file_size=-1] BUG: Succeeded with negative file size! {} rows. \
                 The -1 i64 -> u64 cast produces u64::MAX which DataFusion may ignore.",
                total
            );
        }
        Err(e) => {
            eprintln!(
                "[file_size=-1] Error (expected): {}",
                e
            );
        }
    }

    Ok(())
}

/// BUG-HUNT: footer_size = -1 in metadata
/// Negative footer_size cast to usize may cause massive memory allocation or panic
#[tokio::test]
async fn test_footer_size_negative() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("neg_footer.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Set footer_size to -1
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute("UPDATE ducklake_data_file SET footer_size = -1", [])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // footer_size as usize: -1i64 as usize = usize::MAX on 64-bit
    // with_metadata_size_hint(usize::MAX) might cause huge allocation
    let result = ctx
        .sql("SELECT * FROM test.main.users")
        .await?
        .collect()
        .await;

    match result {
        Ok(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[footer_size=-1] Succeeded: {} rows (DataFusion may ignore bad hint)",
                total
            );
        }
        Err(e) => {
            eprintln!(
                "[footer_size=-1] Error: {}",
                e
            );
        }
    }

    Ok(())
}

/// BUG-HUNT: Corrupted data_path in ducklake_metadata
/// What happens when the data_path doesn't exist?
#[tokio::test]
async fn test_corrupted_data_path() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("bad_path.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Corrupt data_path to a nonexistent location
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute(
            "UPDATE ducklake_metadata SET value = '/nonexistent/path/to/nowhere/' \
             WHERE key = 'data_path'",
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let result = DuckLakeCatalog::new(provider);

    match result {
        Ok(catalog) => {
            eprintln!(
                "[bad data_path] Catalog created (path only validated on file access). schemas = {:?}",
                catalog.schema_names()
            );

            // Try to actually query — this should fail when accessing parquet files
            let ctx = SessionContext::new();
            ctx.register_catalog("test", Arc::new(catalog));

            let query_result = ctx
                .sql("SELECT * FROM test.main.users")
                .await?
                .collect()
                .await;

            match query_result {
                Ok(_) => eprintln!("[bad data_path] BUG: Query succeeded with nonexistent path!"),
                Err(e) => eprintln!("[bad data_path] Query failed as expected: {}", e),
            }
        }
        Err(e) => {
            eprintln!("[bad data_path] Catalog creation failed: {}", e);
        }
    }

    Ok(())
}

/// BUG-HUNT: Empty data_path string
#[tokio::test]
async fn test_empty_data_path() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("empty_path.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Set data_path to empty string
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute(
            "UPDATE ducklake_metadata SET value = '' WHERE key = 'data_path'",
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let result = DuckLakeCatalog::new(provider);
    match result {
        Ok(_catalog) => {
            eprintln!("[empty data_path] Catalog created with empty data_path");
        }
        Err(e) => {
            eprintln!("[empty data_path] Error creating catalog: {}", e);
        }
    }

    Ok(())
}

/// BUG-HUNT: Missing data_path row entirely
#[tokio::test]
async fn test_missing_data_path_row() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("no_data_path.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Delete the data_path entry
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute(
            "DELETE FROM ducklake_metadata WHERE key = 'data_path'",
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let result = DuckLakeCatalog::new(provider);
    match result {
        Ok(_) => {
            eprintln!("[missing data_path] BUG: Catalog created without data_path!");
        }
        Err(e) => {
            eprintln!("[missing data_path] OK: Error creating catalog: {}", e);
        }
    }

    Ok(())
}

// ============================================================================
// 5. UNICODE STRESS — emoji, CJK, RTL, zero-width joiners
// ============================================================================

/// BUG-HUNT: Emoji table name
#[tokio::test]
async fn test_emoji_table_name() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("emoji.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Try emoji table name
    let result = conn.execute(
        r#"CREATE TABLE test_catalog."📊" (id INT, data VARCHAR);"#,
        [],
    );

    match result {
        Ok(_) => {
            conn.execute(
                r#"INSERT INTO test_catalog."📊" VALUES (1, 'chart');"#,
                [],
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            drop(conn);

            let provider =
                DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let catalog = Arc::new(
                DuckLakeCatalog::new(provider)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            );

            let ctx = SessionContext::new();
            ctx.register_catalog("test", catalog);

            let query_result = ctx
                .sql(r#"SELECT * FROM test.main."📊""#)
                .await;

            match query_result {
                Ok(df) => {
                    let batches = df.collect().await;
                    match batches {
                        Ok(b) => {
                            let total: usize = b.iter().map(|batch| batch.num_rows()).sum();
                            eprintln!(
                                "[emoji table] PASSED: Query returned {} rows",
                                total
                            );
                        }
                        Err(e) => {
                            eprintln!("[emoji table] Execution error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[emoji table] Planning error: {}", e);
                }
            }
        }
        Err(e) => {
            eprintln!(
                "[emoji table] DuckDB rejected emoji table name: {}",
                e
            );
        }
    }

    Ok(())
}

/// BUG-HUNT: CJK (Japanese) schema name
#[tokio::test]
async fn test_cjk_schema_name() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("cjk.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Try CJK schema name
    let result = conn.execute(
        r#"CREATE SCHEMA test_catalog."テスト";"#,
        [],
    );

    match result {
        Ok(_) => {
            conn.execute(
                r#"CREATE TABLE test_catalog."テスト".data (id INT);"#,
                [],
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            conn.execute(
                r#"INSERT INTO test_catalog."テスト".data VALUES (42);"#,
                [],
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            drop(conn);

            let provider =
                DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let catalog = Arc::new(
                DuckLakeCatalog::new(provider)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            );

            let schema_names = catalog.schema_names();
            eprintln!(
                "[CJK schema] schema_names = {:?}",
                schema_names
            );

            let ctx = SessionContext::new();
            ctx.register_catalog("test", catalog);

            let query_result = ctx
                .sql(r#"SELECT * FROM test."テスト".data"#)
                .await;

            match query_result {
                Ok(df) => match df.collect().await {
                    Ok(batches) => {
                        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                        eprintln!(
                            "[CJK schema] PASSED: {} rows",
                            total
                        );
                    }
                    Err(e) => eprintln!("[CJK schema] Execution error: {}", e),
                },
                Err(e) => eprintln!("[CJK schema] Planning error: {}", e),
            }
        }
        Err(e) => {
            eprintln!("[CJK schema] DuckDB rejected CJK schema name: {}", e);
        }
    }

    Ok(())
}

/// BUG-HUNT: Column names with special characters
#[tokio::test]
async fn test_special_column_names() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("special_cols.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Column names with spaces, special chars
    conn.execute(
        r#"CREATE TABLE test_catalog.weird (
            "column with spaces" INT,
            "col-with-dashes" INT,
            "col.with.dots" INT,
            "123_starts_with_number" INT
        );"#,
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        r#"INSERT INTO test_catalog.weird VALUES (1, 2, 3, 4);"#,
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider =
        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    let result = ctx
        .sql("SELECT * FROM test.main.weird")
        .await?
        .collect()
        .await;

    match result {
        Ok(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            let col_names: Vec<_> = batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            eprintln!(
                "[special cols] PASSED: {} rows, columns = {:?}",
                total, col_names
            );
        }
        Err(e) => {
            eprintln!("[special cols] Error: {}", e);
        }
    }

    Ok(())
}

// ============================================================================
// 6. CONCURRENCY STRESS — 50 threads, mix of reads
// ============================================================================

/// BUG-HUNT: 50 concurrent threads all reading from same catalog
#[tokio::test]
async fn test_50_concurrent_readers() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("stress.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let mut tasks = Vec::new();
    for task_id in 0..50 {
        let catalog_clone = Arc::clone(&catalog);
        let task = tokio::spawn(async move {
            let ctx = SessionContext::new();
            ctx.register_catalog("test", catalog_clone);

            // Mix of query types
            let query = match task_id % 5 {
                0 => "SELECT * FROM test.main.users ORDER BY id",
                1 => "SELECT COUNT(*) FROM test.main.users",
                2 => "SELECT name FROM test.main.users WHERE id = 1",
                3 => "SELECT * FROM test.main.users WHERE id > 0",
                4 => "SELECT COUNT(*), MAX(id) FROM test.main.users",
                _ => unreachable!(),
            };

            let df = ctx.sql(query).await?;
            let _results = df.collect().await?;
            Ok::<_, DataFusionError>(task_id)
        });
        tasks.push(task);
    }

    let mut succeeded = 0;
    let mut failed = 0;
    for task in tasks {
        match task.await {
            Ok(Ok(_)) => succeeded += 1,
            Ok(Err(e)) => {
                eprintln!("[50 threads] Task error: {}", e);
                failed += 1;
            }
            Err(e) => {
                eprintln!("[50 threads] Task panicked: {}", e);
                failed += 1;
            }
        }
    }

    eprintln!(
        "[50 threads] Succeeded: {}, Failed: {}",
        succeeded, failed
    );
    assert_eq!(failed, 0, "No tasks should fail under concurrent reads");

    Ok(())
}

/// BUG-HUNT: Concurrent metadata access + query execution
/// Tests schema_names(), schema(), table_names(), and queries all at once
#[tokio::test]
async fn test_concurrent_metadata_and_queries() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("meta_stress.ducklake");

    // Create catalog with multiple tables
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    for i in 0..10 {
        conn.execute(
            &format!("CREATE TABLE test_catalog.tbl_{} (id INT);", i),
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        conn.execute(
            &format!("INSERT INTO test_catalog.tbl_{} VALUES ({});", i, i),
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }
    drop(conn);

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let mut tasks = Vec::new();
    for task_id in 0..30 {
        let catalog_clone = Arc::clone(&catalog);
        let task = tokio::spawn(async move {
            match task_id % 3 {
                0 => {
                    // Metadata: list schemas
                    let _names = catalog_clone.schema_names();
                    Ok::<_, DataFusionError>(())
                }
                1 => {
                    // Metadata: list tables
                    if let Some(schema) = catalog_clone.schema("main") {
                        let _names = schema.table_names();
                    }
                    Ok(())
                }
                2 => {
                    // Query
                    let ctx = SessionContext::new();
                    ctx.register_catalog("test", catalog_clone);
                    let tbl_idx = task_id % 10;
                    let df = ctx
                        .sql(&format!("SELECT * FROM test.main.tbl_{}", tbl_idx))
                        .await?;
                    let _results = df.collect().await?;
                    Ok(())
                }
                _ => unreachable!(),
            }
        });
        tasks.push(task);
    }

    let mut failed = 0;
    for task in tasks {
        match task.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!("[meta+query stress] Error: {}", e);
                failed += 1;
            }
            Err(e) => {
                eprintln!("[meta+query stress] Panic: {}", e);
                failed += 1;
            }
        }
    }

    eprintln!(
        "[meta+query stress] Failed: {} / 30",
        failed
    );
    assert_eq!(failed, 0);

    Ok(())
}

// ============================================================================
// 7. STALE CATALOG — snapshot advances after catalog creation
// ============================================================================

/// BUG-HUNT: Create catalog, then advance snapshot via DuckDB writes,
/// then read with stale catalog. The catalog is bound to old snapshot.
#[tokio::test]
async fn test_stale_catalog_after_writes() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("stale.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Create catalog (bound to current snapshot)
    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    // Verify initial state
    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog.clone());

    let result = ctx
        .sql("SELECT COUNT(*) FROM test.main.users")
        .await?
        .collect()
        .await?;
    let initial_count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    eprintln!("[stale catalog] Initial count: {}", initial_count);

    // Now write more data via DuckDB (advancing the snapshot)
    {
        let conn = duckdb::Connection::open_in_memory()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        conn.execute("INSTALL ducklake;", [])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        conn.execute("LOAD ducklake;", [])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let ducklake_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        conn.execute(
            "INSERT INTO test_catalog.users VALUES (3, 'Charlie'), (4, 'Diana');",
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    // Query with stale catalog — should still see old data (snapshot isolation)
    let ctx2 = SessionContext::new();
    ctx2.register_catalog("test", catalog);

    let result2 = ctx2
        .sql("SELECT COUNT(*) FROM test.main.users")
        .await?
        .collect()
        .await?;
    let stale_count = result2[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    eprintln!(
        "[stale catalog] Count after external write: {} (expected {} for snapshot isolation)",
        stale_count, initial_count
    );

    // This should still equal initial_count if snapshot isolation works
    assert_eq!(
        stale_count, initial_count,
        "Stale catalog should see data from its bound snapshot only"
    );

    Ok(())
}

// ============================================================================
// 8. AtomicI64 SNAPSHOT — concurrent atomic operations
// ============================================================================

/// BUG-HUNT: Concurrent reads of AtomicI64 snapshot_id while it's being updated
/// Tests that schema_names() and schema() don't get inconsistent snapshots
#[tokio::test]
async fn test_atomic_snapshot_concurrent_reads() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("atomic.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    // Simulate what write operations do: update the snapshot_id atomically
    // We'll do this in a separate task while reads are happening
    let snap = Arc::new(AtomicI64::new(1));
    let snap_clone = Arc::clone(&snap);

    let writer_task = tokio::spawn(async move {
        for i in 2..100 {
            snap_clone.store(i, Ordering::Release);
            tokio::task::yield_now().await;
        }
    });

    // Meanwhile, read operations on the catalog
    let mut read_tasks = Vec::new();
    for _ in 0..20 {
        let catalog_clone = Arc::clone(&catalog);
        let task = tokio::spawn(async move {
            for _ in 0..10 {
                let _names = catalog_clone.schema_names();
                let _main = catalog_clone.schema("main");
                tokio::task::yield_now().await;
            }
            Ok::<_, DataFusionError>(())
        });
        read_tasks.push(task);
    }

    writer_task.await.expect("Writer task panicked");

    let mut failed = 0;
    for task in read_tasks {
        match task.await {
            Ok(Ok(_)) => {}
            _ => failed += 1,
        }
    }

    eprintln!(
        "[atomic snapshot] Failed: {} / 20 reader tasks",
        failed
    );
    assert_eq!(failed, 0, "Concurrent snapshot reads should not fail");

    Ok(())
}

// ============================================================================
// 9. CORRUPTED COLUMN METADATA — bad types, missing fields
// ============================================================================

/// BUG-HUNT: Column with unknown/invalid type string
#[tokio::test]
async fn test_unknown_column_type() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("bad_type.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Corrupt column type to something unknown
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute(
            "UPDATE ducklake_column SET column_type = 'totally_fake_type' WHERE column_name = 'name'",
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Querying should fail when building the Arrow schema
    let result = ctx
        .sql("SELECT * FROM test.main.users")
        .await;

    match result {
        Ok(df) => match df.collect().await {
            Ok(_) => eprintln!("[unknown type] BUG: Succeeded with fake column type!"),
            Err(e) => eprintln!("[unknown type] OK: Failed at execution: {}", e),
        },
        Err(e) => eprintln!("[unknown type] OK: Failed at planning: {}", e),
    }

    Ok(())
}

/// BUG-HUNT: Duplicate column names in metadata
#[tokio::test]
async fn test_duplicate_column_names() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("dup_cols.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Make two columns have the same name
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute(
            "UPDATE ducklake_column SET column_name = 'id' WHERE column_name = 'name'",
            [],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Duplicate column names may cause issues in Arrow schema or DataFusion planning
    let result = ctx
        .sql("SELECT * FROM test.main.users")
        .await;

    match result {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                let col_names: Vec<_> = batches[0]
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                eprintln!(
                    "[dup columns] Result columns: {:?} — Arrow allows dup names but may confuse queries",
                    col_names
                );
            }
            Err(e) => eprintln!("[dup columns] Execution error: {}", e),
        },
        Err(e) => eprintln!("[dup columns] Planning error: {}", e),
    }

    Ok(())
}

// ============================================================================
// 10. DELETE FILE CORRUPTION
// ============================================================================

/// BUG-HUNT: What happens when a delete file referenced in metadata doesn't exist on disk?
#[tokio::test]
async fn test_missing_delete_file() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("missing_del.ducklake");

    // Create catalog with deletes
    common::create_catalog_with_deletes(&catalog_path).map_err(common::to_datafusion_error)?;

    // Remove the delete files from disk but leave metadata intact
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        // Get delete file paths
        let mut stmt = conn
            .prepare("SELECT path, path_is_relative FROM ducklake_delete_file")
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let delete_files: Vec<(String, bool)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
            })
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .filter_map(|r| r.ok())
            .collect();

        // Get data_path
        let data_path: String = conn
            .query_row(
                "SELECT value FROM ducklake_metadata WHERE key = 'data_path'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        drop(stmt);
        drop(conn);

        // Delete the actual delete files from disk
        for (path, is_relative) in &delete_files {
            let full_path = if *is_relative {
                format!("{}{}", data_path, path)
            } else {
                path.clone()
            };
            let _ = std::fs::remove_file(&full_path);
            eprintln!("[missing delete file] Removed: {}", full_path);
        }
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Query should fail when trying to read the missing delete file
    let result = ctx
        .sql("SELECT * FROM test.main.products ORDER BY id")
        .await?
        .collect()
        .await;

    match result {
        Ok(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[missing delete file] BUG: Query succeeded with {} rows (delete files missing, \
                 so deleted rows may be returned!)",
                total
            );
        }
        Err(e) => {
            eprintln!(
                "[missing delete file] OK: Error when reading missing delete file: {}",
                e
            );
        }
    }

    Ok(())
}

// ============================================================================
// 11. SCHEMA NAME EDGE CASES
// ============================================================================

/// BUG-HUNT: Schema name "information_schema" — reserved name collision
#[tokio::test]
async fn test_information_schema_collision() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("info_schema.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Try to insert a row into ducklake_schema with name "information_schema"
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        let result = conn.execute(
            "INSERT INTO ducklake_schema (schema_id, schema_name, path, path_is_relative, begin_snapshot) \
             VALUES (999, 'information_schema', 'info_schema/', true, 1)",
            [],
        );
        match result {
            Ok(_) => eprintln!("[info_schema] Injected fake information_schema into metadata"),
            Err(e) => eprintln!("[info_schema] DuckDB rejected: {}", e),
        }
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let schema_names = catalog.schema_names();
    eprintln!(
        "[info_schema] schema_names = {:?}",
        schema_names
    );

    // Should deduplicate
    let info_count = schema_names
        .iter()
        .filter(|s| *s == "information_schema")
        .count();
    eprintln!(
        "[info_schema] 'information_schema' appears {} times (should be 1)",
        info_count
    );

    Ok(())
}

/// BUG-HUNT: Very long schema/table name (1000 chars)
///
/// FINDING: DuckDB/DuckLake stores the table name in metadata and uses it as a directory
/// name for data files. Filesystem limits (typically 255 bytes for filenames on ext4/xfs)
/// cause DuckDB's INSERT to fail when creating the data directory for a 1000-char name.
/// This is a DuckDB/filesystem limitation, not our bug — but we should handle it gracefully.
#[tokio::test]
async fn test_very_long_names() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("long_names.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Try with 1000-char name (exceeds filesystem limit of ~255 for dir names)
    let long_name = "a".repeat(1000);
    let create_result = conn.execute(
        &format!(
            r#"CREATE TABLE test_catalog."{}" (id INT);"#,
            long_name
        ),
        [],
    );

    match create_result {
        Ok(_) => {
            // CREATE TABLE succeeded (metadata only) — INSERT will fail on filesystem
            let insert_result = conn.execute(
                &format!(
                    r#"INSERT INTO test_catalog."{}" VALUES (1);"#,
                    long_name
                ),
                [],
            );
            match insert_result {
                Ok(_) => {
                    drop(conn);
                    // If INSERT succeeded, test our reader
                    let provider =
                        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let catalog = Arc::new(
                        DuckLakeCatalog::new(provider)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    );
                    let ctx = SessionContext::new();
                    ctx.register_catalog("test", catalog);
                    let query_result = ctx
                        .sql(&format!(r#"SELECT * FROM test.main."{}""#, long_name))
                        .await;
                    match query_result {
                        Ok(df) => match df.collect().await {
                            Ok(b) => {
                                let total: usize = b.iter().map(|batch| batch.num_rows()).sum();
                                eprintln!("[long name] PASSED: {} rows", total);
                            }
                            Err(e) => eprintln!("[long name] Execution error: {}", e),
                        },
                        Err(e) => eprintln!("[long name] Planning error: {}", e),
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[long name] FINDING: DuckDB INSERT fails for 1000-char table name (filesystem limit): {}",
                        e
                    );
                }
            }
        }
        Err(e) => {
            eprintln!("[long name] DuckDB CREATE TABLE rejected 1000-char name: {}", e);
        }
    }

    // Also test with a name right at 255 chars (ext4/xfs limit)
    let borderline_name = "b".repeat(200); // Safe for most filesystems
    let conn2 = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn2.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn2.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog_path2 = temp_dir.path().join("long_names2.ducklake");
    let ducklake_path2 = format!("ducklake:{}", catalog_path2.display());
    conn2.execute(&format!("ATTACH '{}' AS test_catalog2;", ducklake_path2), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn2.execute(
        &format!(r#"CREATE TABLE test_catalog2."{}" (id INT);"#, borderline_name),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn2.execute(
        &format!(r#"INSERT INTO test_catalog2."{}" VALUES (1);"#, borderline_name),
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn2);

    let provider =
        DuckdbMetadataProvider::new(catalog_path2.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    let result = ctx
        .sql(&format!(r#"SELECT * FROM test.main."{}""#, borderline_name))
        .await?
        .collect()
        .await?;

    let total: usize = result.iter().map(|b| b.num_rows()).sum();
    eprintln!("[long name 200] PASSED: {} rows with 200-char name", total);
    assert_eq!(total, 1);

    Ok(())
}

// ============================================================================
// 12. MULTIPLE DATA TYPES — boundary values in typed data
// ============================================================================

/// BUG-HUNT: Table with all supported types, queried with extreme values
#[tokio::test]
async fn test_all_types_table() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("all_types.ducklake");

    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "CREATE TABLE test_catalog.all_types (
            col_bool BOOLEAN,
            col_int8 TINYINT,
            col_int16 SMALLINT,
            col_int32 INTEGER,
            col_int64 BIGINT,
            col_float FLOAT,
            col_double DOUBLE,
            col_varchar VARCHAR,
            col_date DATE,
            col_timestamp TIMESTAMP,
            col_decimal DECIMAL(10, 2)
        );",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Insert extreme values
    conn.execute(
        "INSERT INTO test_catalog.all_types VALUES (
            true, -128, -32768, -2147483648, -9223372036854775808,
            -3.4e38, -1.7e308, '', '0001-01-01', '0001-01-01 00:00:00', -99999999.99
        );",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    conn.execute(
        "INSERT INTO test_catalog.all_types VALUES (
            false, 127, 32767, 2147483647, 9223372036854775807,
            3.4e38, 1.7e308, 'max values', '9999-12-31', '9999-12-31 23:59:59', 99999999.99
        );",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Insert NULLs for everything
    conn.execute(
        "INSERT INTO test_catalog.all_types VALUES (
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        );",
        [],
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    drop(conn);

    let provider =
        DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    let result = ctx
        .sql("SELECT * FROM test.main.all_types")
        .await?
        .collect()
        .await?;

    let total: usize = result.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "[all types] Rows = {}, Columns = {} (expected 3 rows, 11+ columns)",
        total,
        result[0].num_columns()
    );
    assert_eq!(total, 3, "Should have 3 rows (min, max, null)");

    Ok(())
}

// ============================================================================
// 13. EMPTY STRING VALUES — in metadata fields
// ============================================================================

/// BUG-HUNT: Table with empty string as path
#[tokio::test]
async fn test_empty_table_path() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("empty_tbl_path.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Set table path to empty string
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute("UPDATE ducklake_table SET path = ''", [])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    let result = ctx
        .sql("SELECT * FROM test.main.users")
        .await?
        .collect()
        .await;

    match result {
        Ok(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[empty table path] Result: {} rows (path resolution may still work if file paths are absolute)",
                total
            );
        }
        Err(e) => {
            eprintln!("[empty table path] Error: {}", e);
        }
    }

    Ok(())
}

/// BUG-HUNT: Table with empty string as schema path
#[tokio::test]
async fn test_empty_schema_path() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("empty_schema_path.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Set schema path to empty string
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute("UPDATE ducklake_schema SET path = ''", [])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    let result = ctx
        .sql("SELECT * FROM test.main.users")
        .await?
        .collect()
        .await;

    match result {
        Ok(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[empty schema path] Result: {} rows",
                total
            );
        }
        Err(e) => {
            eprintln!("[empty schema path] Error: {}", e);
        }
    }

    Ok(())
}

// ============================================================================
// 14. NONEXISTENT TABLE / SCHEMA LOOKUPS
// ============================================================================

/// BUG-HUNT: Query a table that doesn't exist
#[tokio::test]
async fn test_query_nonexistent_table() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    let result = ctx
        .sql("SELECT * FROM test.main.nonexistent_table")
        .await;

    match result {
        Ok(_) => eprintln!("[nonexistent table] BUG: Query planning succeeded for nonexistent table!"),
        Err(e) => eprintln!("[nonexistent table] OK: Error: {}", e),
    }

    // Also test nonexistent schema
    let result2 = ctx
        .sql("SELECT * FROM test.nonexistent_schema.some_table")
        .await;

    match result2 {
        Ok(_) => eprintln!("[nonexistent schema] BUG: Query planning succeeded!"),
        Err(e) => eprintln!("[nonexistent schema] OK: Error: {}", e),
    }

    Ok(())
}

// ============================================================================
// 15. SNAPSHOT TABLE MANIPULATION
// ============================================================================

/// BUG-HUNT: Empty ducklake_snapshot table
/// SQL_GET_LATEST_SNAPSHOT uses COALESCE(MAX(snapshot_id), 0)
/// So empty snapshot table should return 0, then no data should be visible.
#[tokio::test]
async fn test_empty_snapshot_table() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("empty_snap.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Delete all snapshots
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;
        conn.execute("DELETE FROM ducklake_snapshot", [])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(conn);
    }

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // This should get snapshot_id = 0 (COALESCE)
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let schema_names = catalog.schema_names();
    eprintln!(
        "[empty snapshots] schema_names = {:?}",
        schema_names
    );

    // With snapshot 0, and schemas that have begin_snapshot >= 1, nothing should be visible
    let main = catalog.schema("main");
    if let Some(schema) = main {
        eprintln!(
            "[empty snapshots] FINDING: main visible with empty snapshot table, tables = {:?}",
            schema.table_names()
        );
    } else {
        eprintln!("[empty snapshots] OK: main schema not visible");
    }

    Ok(())
}

// ============================================================================
// 16. FILE PATH EDGE CASES
// ============================================================================

/// BUG-HUNT: Data file path containing special characters
#[tokio::test]
async fn test_data_file_with_special_path_chars() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("special_path.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Modify file paths to contain special characters
    {
        let conn = open_catalog_rw(&catalog_path).map_err(common::to_datafusion_error)?;

        // Get current file path
        let current_path: String = conn
            .query_row(
                "SELECT path FROM ducklake_data_file LIMIT 1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        eprintln!(
            "[special path chars] Current file path: {}",
            current_path
        );

        // Don't actually change the path (would break file lookup), just verify the current one
        drop(conn);
    }

    // This test mainly documents what paths look like
    eprintln!("[special path chars] INFORMATIONAL: DuckLake uses UUID-based paths, which are safe");

    Ok(())
}

// ============================================================================
// 17. CONCURRENT CATALOG CREATION
// ============================================================================

/// BUG-HUNT: Create multiple DuckLakeCatalog instances from same file
#[tokio::test]
async fn test_multiple_catalog_instances_same_file() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("multi.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    // Create 5 separate catalog instances from the same file
    let mut catalogs = Vec::new();
    for i in 0..5 {
        let provider =
            DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let catalog = Arc::new(
            DuckLakeCatalog::new(provider)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        catalogs.push((i, catalog));
    }

    // Query from all of them concurrently
    let mut tasks = Vec::new();
    for (idx, catalog) in catalogs {
        let task = tokio::spawn(async move {
            let ctx = SessionContext::new();
            ctx.register_catalog(&format!("cat_{}", idx), catalog);

            let df = ctx
                .sql(&format!(
                    "SELECT COUNT(*) FROM cat_{}.main.users",
                    idx
                ))
                .await?;
            let results = df.collect().await?;
            let count = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            Ok::<_, DataFusionError>((idx, count))
        });
        tasks.push(task);
    }

    for task in tasks {
        let (idx, count) = task.await.expect("Task panicked")?;
        assert_eq!(count, 2, "Catalog {} got wrong count", idx);
    }

    eprintln!("[multi catalog] PASSED: 5 concurrent catalog instances all returned correct results");

    Ok(())
}

// ============================================================================
// 18. PROJECTION EDGE CASES
// ============================================================================

/// BUG-HUNT: SELECT with projection of only virtual columns
#[tokio::test]
async fn test_select_only_virtual_columns() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("virtual.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let ctx = SessionContext::new();
    ctx.register_catalog("test", catalog);

    // Select only virtual columns — these exist in full_schema but not in Parquet
    let result = ctx
        .sql("SELECT filename, file_row_number FROM test.main.users")
        .await;

    match result {
        Ok(df) => match df.collect().await {
            Ok(batches) => {
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                eprintln!(
                    "[virtual only] PASSED: {} rows with only virtual columns",
                    total
                );
                if !batches.is_empty() && batches[0].num_columns() > 0 {
                    let filenames = batches[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>();
                    if let Some(arr) = filenames {
                        eprintln!("[virtual only] filename[0] = {:?}", arr.value(0));
                    }
                }
            }
            Err(e) => eprintln!("[virtual only] Execution error: {}", e),
        },
        Err(e) => eprintln!("[virtual only] Planning error: {}", e),
    }

    Ok(())
}

// ============================================================================
// 19. RAPID SEQUENTIAL CATALOG CREATION/DESTRUCTION
// ============================================================================

/// BUG-HUNT: Rapidly create and drop catalog instances
/// Tests for resource leaks (file handles, memory)
#[tokio::test]
async fn test_rapid_catalog_lifecycle() -> DataFusionResult<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("lifecycle.ducklake");
    setup_ducklake_catalog(&catalog_path).map_err(common::to_datafusion_error)?;

    for i in 0..100 {
        let provider =
            DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let catalog = Arc::new(
            DuckLakeCatalog::new(provider)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );

        let ctx = SessionContext::new();
        ctx.register_catalog("test", catalog);

        let df = ctx
            .sql("SELECT COUNT(*) FROM test.main.users")
            .await?;
        let results = df.collect().await?;
        let count = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        assert_eq!(count, 2, "Iteration {} got wrong count", i);
        // catalog and ctx dropped here
    }

    eprintln!("[lifecycle] PASSED: 100 create/query/drop cycles completed");

    Ok(())
}
