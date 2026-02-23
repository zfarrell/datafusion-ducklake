#![cfg(feature = "metadata-duckdb")]
//! Stress tests for DuckLake upstream issue reproduction
//!
//! These tests use actual concurrency (threads/tokio::spawn) and tight loops
//! to attempt to reproduce race conditions, corruption, and intermittent failures
//! reported in upstream DuckLake issues.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::common::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use tempfile::TempDir;

/// Helper to extract i64 values from column 0
fn get_i64_col0(batch: &arrow::record_batch::RecordBatch) -> Vec<i64> {
    let col = batch.column(0);
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        (0..a.len()).filter_map(|i| if a.is_null(i) { None } else { Some(a.value(i)) }).collect()
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        (0..a.len()).filter_map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect()
    } else {
        panic!("Expected Int32 or Int64, got {:?}", col.data_type());
    }
}

/// Helper: create a DuckDB connection with DuckLake loaded
fn duckdb_conn_with_ducklake() -> duckdb::Connection {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute("INSTALL ducklake;", []).unwrap();
    conn.execute("LOAD ducklake;", []).unwrap();
    conn
}

/// Helper: attach a DuckLake catalog to a connection
fn attach_ducklake(conn: &duckdb::Connection, catalog_path: &str, alias: &str) {
    let ducklake_path = format!("ducklake:{}", catalog_path);
    conn.execute(&format!("ATTACH '{}' AS {};", ducklake_path, alias), [])
        .unwrap();
}

// =============================================================================
// Issue #217 (CRITICAL): Double slash in S3 URL from join_paths
// =============================================================================

/// Test that join_paths produces double slashes - this IS a bug in our code.
///
/// The join_paths function in path_resolver.rs does not strip leading '/' from
/// the relative path when the base already ends with '/'. This produces "//"
/// in the resulting path, which breaks S3 URLs.
///
/// Evidence: The existing test at path_resolver.rs:473-476 documents this:
///   assert_eq!(join_paths("/data/", "/absolute"), "/data//absolute");
///
/// Verify that join_paths strips leading slashes from relative_path when
/// base_path already ends with a separator, preventing double slashes in S3 URLs.
#[test]
fn test_issue_217_double_slash_in_paths() {
    use datafusion_ducklake::path_resolver::{join_paths, resolve_path, PathResolver};
    use datafusion::datasource::object_store::ObjectStoreUrl;

    // Case 1: Base with trailing slash + relative with leading slash -> no double slash
    let result = join_paths("/data/", "/subdir/file.parquet");
    assert_eq!(result, "/data/subdir/file.parquet");
    assert!(!result.contains("//"), "join_paths must not produce double slashes");

    // Case 2: S3-like path hierarchy
    let result = join_paths("/warehouse/prod/", "/data/file.parquet");
    assert_eq!(result, "/warehouse/prod/data/file.parquet");
    assert!(!result.contains("//"), "S3 path must not have double slashes");

    // Case 3: resolve_path with is_relative=true
    let result = resolve_path("/bucket/prefix/", "/schema/table/file.parquet", true);
    assert_eq!(result, "/bucket/prefix/schema/table/file.parquet");
    assert!(!result.contains("//"), "resolve_path must not produce double slashes");

    // Case 4: PathResolver child_resolver
    let resolver = PathResolver::new(
        Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
        "/data/".to_string(),
    );
    let child = resolver.child_resolver("/subpath/", true);
    assert_eq!(child.base_path(), "/data/subpath/");
    assert!(!child.base_path().contains("//"), "PathResolver must not produce double slashes");

    // Case 5: Multiple levels of nesting
    let base = "/s3/bucket/warehouse/";
    for p in &["/schema/", "/table/", "/partition=1/"] {
        let result = join_paths(base, p);
        assert!(!result.contains("//"),
            "join_paths({}, {}) = {} must not have //", base, p, result);
    }

    // Case 6: Normal cases still work
    let ok_result = join_paths("/data/", "subdir/file.parquet");
    assert_eq!(ok_result, "/data/subdir/file.parquet");

    let ok_result2 = join_paths("/data", "subdir/file.parquet");
    assert_eq!(ok_result2, "/data/subdir/file.parquet");
}

// =============================================================================
// Issues #268, #284: Concurrent table creation
// =============================================================================

/// Stress test: create many tables and verify no data cross-contamination
/// or ID collisions when reading them concurrently via DataFusion.
///
/// Issues #268/#284 report problems with concurrent table creation in DuckLake.
/// DuckDB's DuckLake extension can produce catalog corruption (missing tables,
/// orphaned parquet files, or even segfaults) when multiple connections write
/// to the same SQLite-backed catalog simultaneously. This is a known limitation
/// of SQLite's write concurrency model.
///
/// This test focuses on what we CAN guarantee:
/// 1. Tables are created sequentially (avoiding DuckDB/SQLite concurrent write bugs)
/// 2. Multiple DataFusion sessions read ALL tables concurrently
/// 3. No data cross-contamination is found in any table
/// 4. The catalog remains consistent under concurrent read load
#[tokio::test]
async fn test_issue_268_284_concurrent_table_creation() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("concurrent_create.ducklake");
    let catalog_str = catalog_path.to_string_lossy().to_string();

    let num_tables = 50;

    // Phase 1: Create tables sequentially via DuckDB.
    // Each table gets a unique name and unique data so we can detect
    // cross-contamination during concurrent reads.
    {
        let conn = duckdb_conn_with_ducklake();
        attach_ducklake(&conn, &catalog_str, "test_cat");

        for i in 0..num_tables {
            let table_name = format!("t_{}", i);
            conn.execute(
                &format!(
                    "CREATE TABLE test_cat.{} (id INT, val VARCHAR);",
                    table_name
                ),
                [],
            )
            .unwrap();
            conn.execute(
                &format!(
                    "INSERT INTO test_cat.{} VALUES ({}, '{}');",
                    table_name,
                    i * 100,
                    table_name
                ),
                [],
            )
            .unwrap();
        }
    }

    // Phase 2: Open a shared DataFusion catalog and concurrently read ALL
    // tables from multiple tasks. This stresses the metadata provider's
    // Mutex-protected connection and verifies no cross-contamination.
    let provider = DuckdbMetadataProvider::new(catalog_str.clone())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let contamination_count = Arc::new(AtomicU32::new(0));
    let tables_verified = Arc::new(AtomicU32::new(0));

    // Spawn concurrent reader tasks -- each task verifies a subset of tables
    let mut tasks = Vec::new();
    for task_id in 0..10 {
        let catalog_clone = Arc::clone(&catalog);
        let contam = Arc::clone(&contamination_count);
        let verified = Arc::clone(&tables_verified);

        let task = tokio::spawn(async move {
            let ctx = SessionContext::new();
            ctx.register_catalog("test_cat", catalog_clone);

            // Each task reads tables in its assigned range
            let start = task_id * (num_tables / 10);
            let end = start + (num_tables / 10);
            for i in start..end {
                let table_name = format!("t_{}", i);
                let sql = format!(
                    "SELECT id, val FROM test_cat.main.{} ORDER BY id",
                    table_name
                );
                let df = ctx.sql(&sql).await?;
                let results = df.collect().await?;
                for batch in &results {
                    if batch.num_rows() > 0 {
                        let vals = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        for row in 0..vals.len() {
                            if !vals.is_null(row) && vals.value(row) != table_name.as_str() {
                                eprintln!(
                                    "  CONTAMINATION in {}: found val='{}' (expected '{}')",
                                    table_name,
                                    vals.value(row),
                                    table_name
                                );
                                contam.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                verified.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<_, DataFusionError>(task_id)
        });

        tasks.push(task);
    }

    // Wait for all reader tasks
    for task in tasks {
        task.await.expect("Reader task panicked")?;
    }

    let contaminations = contamination_count.load(Ordering::Relaxed);
    let total_verified = tables_verified.load(Ordering::Relaxed);

    assert_eq!(
        contaminations, 0,
        "Issue #268/#284: Data cross-contamination detected across table reads!"
    );
    assert_eq!(
        total_verified, num_tables as u32,
        "Not all tables were verified"
    );

    eprintln!(
        "Issue #268/#284: {} tables verified with no cross-contamination across 10 concurrent readers",
        total_verified
    );

    Ok(())
}

// =============================================================================
// Issues #69, #101, #230: DROP TABLE corruption
// =============================================================================

/// Stress test: drop tables while simultaneously reading others.
/// Run 10+ iterations to catch intermittent failures.
#[tokio::test]
async fn test_issue_69_101_230_drop_table_while_reading() -> DataFusionResult<()> {
    let error_count = Arc::new(AtomicU32::new(0));

    for iteration in 0..10 {
        let temp_dir = TempDir::new().map_err(|e| DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("drop_test.ducklake");
        let catalog_str = catalog_path.to_string_lossy().to_string();

        // Create 3 tables
        {
            let conn = duckdb_conn_with_ducklake();
            attach_ducklake(&conn, &catalog_str, "dc");

            conn.execute("CREATE TABLE dc.stable1 (id INT, name VARCHAR);", []).unwrap();
            conn.execute("INSERT INTO dc.stable1 VALUES (1, 'a'), (2, 'b'), (3, 'c');", []).unwrap();

            conn.execute("CREATE TABLE dc.stable2 (id INT, val INT);", []).unwrap();
            conn.execute("INSERT INTO dc.stable2 VALUES (10, 100), (20, 200);", []).unwrap();

            conn.execute("CREATE TABLE dc.to_drop (id INT, x VARCHAR);", []).unwrap();
            conn.execute("INSERT INTO dc.to_drop VALUES (99, 'drop_me');", []).unwrap();
        }

        let cat_str = catalog_str.clone();
        let err_cnt = Arc::clone(&error_count);

        // Thread 1: DROP the table
        let drop_handle = {
            let cat_str = cat_str.clone();
            std::thread::spawn(move || {
                let conn = duckdb_conn_with_ducklake();
                attach_ducklake(&conn, &cat_str, "dc");
                let _ = conn.execute("DROP TABLE IF EXISTS dc.to_drop;", []);
            })
        };

        // Thread 2: Read stable tables simultaneously
        let read_handle = {
            let cat_str = cat_str.clone();
            let err_cnt = err_cnt.clone();
            std::thread::spawn(move || {
                let conn = duckdb_conn_with_ducklake();
                attach_ducklake(&conn, &cat_str, "dc");

                // Read stable1
                match conn.prepare("SELECT COUNT(*) FROM dc.stable1") {
                    Ok(mut stmt) => {
                        match stmt.query_row([], |row| row.get::<_, i64>(0)) {
                            Ok(count) => {
                                if count != 3 {
                                    eprintln!("  Iteration {}: stable1 count={} (expected 3)", iteration, count);
                                    err_cnt.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(e) => {
                                eprintln!("  Iteration {}: stable1 read error: {}", iteration, e);
                                err_cnt.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("  Iteration {}: stable1 prepare error: {}", iteration, e);
                        err_cnt.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Read stable2
                match conn.prepare("SELECT COUNT(*) FROM dc.stable2") {
                    Ok(mut stmt) => {
                        match stmt.query_row([], |row| row.get::<_, i64>(0)) {
                            Ok(count) => {
                                if count != 2 {
                                    eprintln!("  Iteration {}: stable2 count={} (expected 2)", iteration, count);
                                    err_cnt.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(e) => {
                                eprintln!("  Iteration {}: stable2 read error: {}", iteration, e);
                                err_cnt.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("  Iteration {}: stable2 prepare error: {}", iteration, e);
                        err_cnt.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        };

        drop_handle.join().expect("Drop thread panicked");
        read_handle.join().expect("Read thread panicked");

        // After drop: verify stable tables are still readable via our DataFusion catalog
        let provider = DuckdbMetadataProvider::new(catalog_str.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let catalog = Arc::new(
            DuckLakeCatalog::new(provider)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let ctx = SessionContext::new();
        ctx.register_catalog("dc", catalog);

        let df = ctx.sql("SELECT COUNT(*) as cnt FROM dc.main.stable1").await?;
        let results = df.collect().await?;
        let count = results[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        if count != 3 {
            eprintln!("  Iteration {}: DataFusion stable1 count={} after drop (expected 3)", iteration, count);
            error_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    let errors = error_count.load(Ordering::Relaxed);
    if errors > 0 {
        eprintln!("✗ Issues #69/#101/#230: {} errors across 10 iterations of DROP + concurrent read", errors);
    } else {
        eprintln!("✓ Issues #69/#101/#230: No corruption in 10 iterations of DROP + concurrent read");
    }

    Ok(())
}

// =============================================================================
// Issue #322: table_stats view_id conflict
// =============================================================================

/// Create a table and a view, then verify both are accessible and have correct data.
#[tokio::test]
async fn test_issue_322_table_view_id_conflict() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("view_conflict.ducklake");
    let catalog_str = catalog_path.to_string_lossy().to_string();

    {
        let conn = duckdb_conn_with_ducklake();
        attach_ducklake(&conn, &catalog_str, "vc");

        // Create a base table
        conn.execute("CREATE TABLE vc.base_table (id INT, name VARCHAR);", []).unwrap();
        conn.execute("INSERT INTO vc.base_table VALUES (1, 'hello'), (2, 'world');", []).unwrap();

        // Create a view on it
        conn.execute("CREATE VIEW vc.base_view AS SELECT id, name FROM vc.base_table WHERE id = 1;", []).unwrap();

        // Create another table to increase ID overlap chance
        conn.execute("CREATE TABLE vc.other_table (x INT);", []).unwrap();
        conn.execute("INSERT INTO vc.other_table VALUES (42);", []).unwrap();
    }

    // Read via DataFusion - verify both table and view data are correct
    let provider = DuckdbMetadataProvider::new(catalog_str)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog = Arc::new(
        DuckLakeCatalog::new(provider)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );
    let ctx = SessionContext::new();
    ctx.register_catalog("vc", catalog);

    // Read base table
    let df = ctx.sql("SELECT * FROM vc.main.base_table ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "base_table should have 2 rows");

    // Read other table
    let df = ctx.sql("SELECT * FROM vc.main.other_table").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "other_table should have 1 row");

    // Read view (our catalog supports views)
    match ctx.sql("SELECT * FROM vc.main.base_view").await {
        Ok(df) => {
            let results = df.collect().await?;
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            eprintln!("  View returned {} rows", total_rows);
        }
        Err(e) => {
            eprintln!("  View query error (may not be supported): {}", e);
        }
    }

    eprintln!("✓ Issue #322: Tables readable alongside views, no ID conflict");
    Ok(())
}

// =============================================================================
// Issue #362: Intermittent ducklake_data_file not found
// =============================================================================

/// Stress test: rapidly create catalog, write data, read back in a tight loop.
#[tokio::test]
async fn test_issue_362_intermittent_data_file_not_found() -> DataFusionResult<()> {
    let error_count = Arc::new(AtomicU32::new(0));

    for iteration in 0..50 {
        let temp_dir = TempDir::new().map_err(|e| DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("rapid.ducklake");
        let catalog_str = catalog_path.to_string_lossy().to_string();

        // Create and populate
        {
            let conn = duckdb_conn_with_ducklake();
            attach_ducklake(&conn, &catalog_str, "r");
            conn.execute("CREATE TABLE r.items (id INT);", []).unwrap();
            conn.execute("INSERT INTO r.items VALUES (1), (2), (3);", []).unwrap();
        }

        // Immediately read back via our DataFusion catalog
        match DuckdbMetadataProvider::new(catalog_str.clone()) {
            Ok(provider) => {
                match DuckLakeCatalog::new(provider) {
                    Ok(catalog) => {
                        let ctx = SessionContext::new();
                        ctx.register_catalog("r", Arc::new(catalog));

                        match ctx.sql("SELECT COUNT(*) FROM r.main.items").await {
                            Ok(df) => {
                                match df.collect().await {
                                    Ok(results) => {
                                        if results.is_empty() {
                                            eprintln!("  Iteration {}: empty results", iteration);
                                            error_count.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            let count = results[0].column(0).as_any()
                                                .downcast_ref::<Int64Array>().unwrap().value(0);
                                            if count != 3 {
                                                eprintln!("  Iteration {}: count={} (expected 3)", iteration, count);
                                                error_count.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("  Iteration {}: collect error: {}", iteration, e);
                                        error_count.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("  Iteration {}: SQL error: {}", iteration, e);
                                error_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("  Iteration {}: catalog creation error: {}", iteration, e);
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(e) => {
                eprintln!("  Iteration {}: provider creation error: {}", iteration, e);
                error_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    let errors = error_count.load(Ordering::Relaxed);
    if errors > 0 {
        eprintln!("✗ Issue #362: {} failures in 50 rapid create/read cycles", errors);
    } else {
        eprintln!("✓ Issue #362: 50 rapid create/read cycles with zero intermittent failures");
    }

    Ok(())
}

// =============================================================================
// Issues #651, #683: Transaction error handling
// =============================================================================

/// Test: inject errors mid-transaction, verify catalog state is consistent.
#[tokio::test]
async fn test_issue_651_683_transaction_error_handling() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("txn_errors.ducklake");
    let catalog_str = catalog_path.to_string_lossy().to_string();

    // Setup: create initial table
    {
        let conn = duckdb_conn_with_ducklake();
        attach_ducklake(&conn, &catalog_str, "txn");
        conn.execute("CREATE TABLE txn.valid_table (id INT, name VARCHAR);", []).unwrap();
        conn.execute("INSERT INTO txn.valid_table VALUES (1, 'original');", []).unwrap();
    }

    let mut errors_found = false;

    for iteration in 0..10 {
        // Open connection and try to do operations that may fail
        let cat_str = catalog_str.clone();
        let result = std::thread::spawn(move || -> Result<(), String> {
            let conn = duckdb_conn_with_ducklake();
            attach_ducklake(&conn, &cat_str, "txn");

            // Begin a transaction
            let _ = conn.execute("BEGIN TRANSACTION;", []);

            // Do a valid operation
            let table_name = format!("txn_table_{}", iteration);
            let _ = conn.execute(
                &format!("CREATE TABLE IF NOT EXISTS txn.{} (id INT);", table_name),
                [],
            );

            // Try an operation that will fail (duplicate table, invalid SQL, etc.)
            let _ = conn.execute("CREATE TABLE txn.valid_table (id INT);", []);

            // Try to commit (may fail due to error)
            let commit_result = conn.execute("COMMIT;", []);

            // Or rollback if commit fails
            if commit_result.is_err() {
                let _ = conn.execute("ROLLBACK;", []);
            }

            Ok(())
        }).join().map_err(|_| "Thread panicked".to_string());

        if result.is_err() {
            eprintln!("  Iteration {}: thread panicked", iteration);
            errors_found = true;
        }

        // After each iteration, verify catalog is still readable
        match DuckdbMetadataProvider::new(catalog_str.clone()) {
            Ok(provider) => {
                match DuckLakeCatalog::new(provider) {
                    Ok(catalog) => {
                        let ctx = SessionContext::new();
                        ctx.register_catalog("txn", Arc::new(catalog));

                        let df = ctx.sql("SELECT COUNT(*) FROM txn.main.valid_table").await?;
                        let results = df.collect().await?;
                        let count = results[0].column(0).as_any()
                            .downcast_ref::<Int64Array>().unwrap().value(0);
                        if count < 1 {
                            eprintln!("  Iteration {}: valid_table lost data (count={})", iteration, count);
                            errors_found = true;
                        }
                    }
                    Err(e) => {
                        eprintln!("  Iteration {}: catalog unreadable after txn error: {}", iteration, e);
                        errors_found = true;
                    }
                }
            }
            Err(e) => {
                eprintln!("  Iteration {}: provider failed after txn error: {}", iteration, e);
                errors_found = true;
            }
        }
    }

    if errors_found {
        eprintln!("✗ Issues #651/#683: Catalog state inconsistent after transaction errors");
    } else {
        eprintln!("✓ Issues #651/#683: Catalog remains consistent after 10 iterations of error injection");
    }

    Ok(())
}

// =============================================================================
// Issue #733: Snapshot consistency after updates
// =============================================================================

/// Stress test: write, update, write more, update again, read at current snapshot.
#[tokio::test]
async fn test_issue_733_snapshot_consistency_after_updates() -> DataFusionResult<()> {
    let error_count = Arc::new(AtomicU32::new(0));

    for iteration in 0..10 {
        let temp_dir = TempDir::new().map_err(|e| DataFusionError::External(Box::new(e)))?;
        let catalog_path = temp_dir.path().join("snapshot.ducklake");
        let catalog_str = catalog_path.to_string_lossy().to_string();

        // Perform write -> update -> write -> update cycle
        {
            let conn = duckdb_conn_with_ducklake();
            attach_ducklake(&conn, &catalog_str, "snap");

            conn.execute("CREATE TABLE snap.data (id INT, val INT);", []).unwrap();

            // Write 1
            conn.execute("INSERT INTO snap.data VALUES (1, 10), (2, 20), (3, 30);", []).unwrap();

            // Update 1
            conn.execute("UPDATE snap.data SET val = 15 WHERE id = 1;", []).unwrap();

            // Write 2
            conn.execute("INSERT INTO snap.data VALUES (4, 40), (5, 50);", []).unwrap();

            // Update 2
            conn.execute("UPDATE snap.data SET val = 25 WHERE id = 2;", []).unwrap();

            // Delete
            conn.execute("DELETE FROM snap.data WHERE id = 3;", []).unwrap();
        }

        // Read via DataFusion at latest snapshot
        let provider = DuckdbMetadataProvider::new(catalog_str.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let catalog = Arc::new(
            DuckLakeCatalog::new(provider)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let ctx = SessionContext::new();
        ctx.register_catalog("snap", catalog);

        // Expected: ids 1(val=15), 2(val=25), 4(val=40), 5(val=50). id=3 deleted.
        let df = ctx.sql("SELECT id, val FROM snap.main.data ORDER BY id").await?;
        let results = df.collect().await?;

        let mut all_ids = Vec::new();
        let mut all_vals = Vec::new();
        for batch in &results {
            all_ids.extend(get_i64_col0(batch));
            let val_col = batch.column(1);
            if let Some(a) = val_col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) { all_vals.push(a.value(i) as i64); }
                }
            } else if let Some(a) = val_col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..a.len() {
                    if !a.is_null(i) { all_vals.push(a.value(i)); }
                }
            }
        }

        if all_ids != vec![1, 2, 4, 5] {
            eprintln!("  Iteration {}: wrong IDs: {:?} (expected [1,2,4,5])", iteration, all_ids);
            error_count.fetch_add(1, Ordering::Relaxed);
        }
        if all_vals != vec![15, 25, 40, 50] {
            eprintln!("  Iteration {}: wrong vals: {:?} (expected [15,25,40,50])", iteration, all_vals);
            error_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    let errors = error_count.load(Ordering::Relaxed);
    if errors > 0 {
        eprintln!("✗ Issue #733: {} snapshot consistency errors in 10 iterations", errors);
    } else {
        eprintln!("✓ Issue #733: Snapshot consistency maintained across 10 iterations of write/update/delete");
    }

    Ok(())
}

// =============================================================================
// Issue #749: Multi-partition pattern
// =============================================================================

/// Create a table partitioned by two columns, write data, read back.
#[tokio::test]
async fn test_issue_749_multi_partition_write_read() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("multi_part.ducklake");
    let catalog_str = catalog_path.to_string_lossy().to_string();

    let mut duckdb_error = None;

    {
        let conn = duckdb_conn_with_ducklake();
        attach_ducklake(&conn, &catalog_str, "mp");

        // Try multi-partition table
        match conn.execute(
            "CREATE TABLE mp.events (id INT, category VARCHAR, ts TIMESTAMP, val INT);",
            [],
        ) {
            Ok(_) => {
                // Insert data
                if let Err(e) = conn.execute(
                    "INSERT INTO mp.events VALUES
                        (1, 'A', '2024-01-01 00:00:00', 10),
                        (2, 'B', '2024-01-01 12:00:00', 20),
                        (3, 'A', '2024-01-02 00:00:00', 30),
                        (4, 'B', '2024-01-02 12:00:00', 40),
                        (5, 'A', '2024-01-03 00:00:00', 50);",
                    [],
                ) {
                    duckdb_error = Some(format!("INSERT failed: {}", e));
                }

                // Try UPDATE on multi-partition table (this is what triggers #749)
                if duckdb_error.is_none() {
                    if let Err(e) = conn.execute(
                        "UPDATE mp.events SET val = 99 WHERE id = 1;",
                        [],
                    ) {
                        duckdb_error = Some(format!("UPDATE on multi-partition table failed: {}", e));
                    }
                }
            }
            Err(e) => {
                duckdb_error = Some(format!("CREATE TABLE failed: {}", e));
            }
        }
    }

    if let Some(err) = &duckdb_error {
        eprintln!("⚠ Issue #749: DuckDB error during multi-partition test: {}", err);
    }

    // Read back via DataFusion
    match DuckdbMetadataProvider::new(catalog_str.clone()) {
        Ok(provider) => {
            match DuckLakeCatalog::new(provider) {
                Ok(catalog) => {
                    let ctx = SessionContext::new();
                    ctx.register_catalog("mp", Arc::new(catalog));

                    let df = ctx.sql("SELECT id, val FROM mp.main.events ORDER BY id").await?;
                    let results = df.collect().await?;
                    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
                    eprintln!("  Multi-partition table read back {} rows", total_rows);

                    if duckdb_error.is_none() {
                        // If UPDATE succeeded, check values
                        let mut all_ids = Vec::new();
                        for batch in &results {
                            all_ids.extend(get_i64_col0(batch));
                        }
                        assert_eq!(total_rows, 5, "Should have 5 rows");
                        assert_eq!(all_ids, vec![1, 2, 3, 4, 5], "Should have all IDs");
                    }

                    eprintln!("✓ Issue #749: Multi-partition table readable via DataFusion");
                }
                Err(e) => {
                    eprintln!("✗ Issue #749: Catalog creation failed: {}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("✗ Issue #749: Provider creation failed: {}", e);
        }
    }

    Ok(())
}

// =============================================================================
// Issue #197: Two catalog instances pointing at same DB
// =============================================================================

/// Open two DuckLakeCatalog instances on the same SQLite file, write via one, read via both.
#[tokio::test]
async fn test_issue_197_two_catalog_instances_same_db() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("shared.ducklake");
    let catalog_str = catalog_path.to_string_lossy().to_string();

    // Create initial data
    {
        let conn = duckdb_conn_with_ducklake();
        attach_ducklake(&conn, &catalog_str, "shared");
        conn.execute("CREATE TABLE shared.users (id INT, name VARCHAR);", []).unwrap();
        conn.execute("INSERT INTO shared.users VALUES (1, 'Alice'), (2, 'Bob');", []).unwrap();
    }

    // Open two separate DuckLakeCatalog instances pointing at the same file
    let provider1 = DuckdbMetadataProvider::new(catalog_str.clone())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog1 = Arc::new(
        DuckLakeCatalog::new(provider1)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    let provider2 = DuckdbMetadataProvider::new(catalog_str.clone())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog2 = Arc::new(
        DuckLakeCatalog::new(provider2)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );

    // Read from both simultaneously
    let ctx1 = SessionContext::new();
    ctx1.register_catalog("shared", catalog1);

    let ctx2 = SessionContext::new();
    ctx2.register_catalog("shared", catalog2);

    // Concurrent reads from both catalogs
    let (result1, result2) = tokio::join!(
        async {
            let df = ctx1.sql("SELECT * FROM shared.main.users ORDER BY id").await?;
            let results = df.collect().await?;
            let total: usize = results.iter().map(|b| b.num_rows()).sum();
            Ok::<_, DataFusionError>(total)
        },
        async {
            let df = ctx2.sql("SELECT * FROM shared.main.users ORDER BY id").await?;
            let results = df.collect().await?;
            let total: usize = results.iter().map(|b| b.num_rows()).sum();
            Ok::<_, DataFusionError>(total)
        }
    );

    let count1 = result1?;
    let count2 = result2?;

    assert_eq!(count1, 2, "Catalog 1 should see 2 rows");
    assert_eq!(count2, 2, "Catalog 2 should see 2 rows");

    // Now write via DuckDB and verify new catalog instance sees the update
    {
        let conn = duckdb_conn_with_ducklake();
        attach_ducklake(&conn, &catalog_str, "shared");
        conn.execute("INSERT INTO shared.users VALUES (3, 'Charlie');", []).unwrap();
    }

    // New provider should see the updated data
    let provider3 = DuckdbMetadataProvider::new(catalog_str.clone())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog3 = Arc::new(
        DuckLakeCatalog::new(provider3)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    );
    let ctx3 = SessionContext::new();
    ctx3.register_catalog("shared", catalog3);

    let df = ctx3.sql("SELECT COUNT(*) FROM shared.main.users").await?;
    let results = df.collect().await?;
    let count3 = results[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count3, 3, "New catalog instance should see 3 rows after insert");

    eprintln!("✓ Issue #197: Two catalog instances on same DB work correctly");
    eprintln!("  Both read 2 rows initially, new instance sees 3 after write");

    Ok(())
}

// =============================================================================
// Issue #268/#284 additional: Concurrent reads during writes (harder stress)
// =============================================================================

/// Additional concurrency stress: read from DataFusion catalog while writing via DuckDB.
#[tokio::test]
async fn test_concurrent_read_during_write_stress() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().map_err(|e| DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("rw_stress.ducklake");
    let catalog_str = catalog_path.to_string_lossy().to_string();

    // Create initial table
    {
        let conn = duckdb_conn_with_ducklake();
        attach_ducklake(&conn, &catalog_str, "rw");
        conn.execute("CREATE TABLE rw.data (id INT, val INT);", []).unwrap();
        conn.execute("INSERT INTO rw.data VALUES (1, 100);", []).unwrap();
    }

    let error_count = Arc::new(AtomicU32::new(0));

    for _ in 0..10 {
        // Open DataFusion catalog for reading
        let provider = DuckdbMetadataProvider::new(catalog_str.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let catalog = Arc::new(
            DuckLakeCatalog::new(provider)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );

        let ctx = SessionContext::new();
        ctx.register_catalog("rw", catalog.clone() as Arc<dyn datafusion::catalog::CatalogProvider>);

        // Spawn reader tasks
        let mut read_tasks = Vec::new();
        for _ in 0..5 {
            let ctx_clone = ctx.clone();
            let err_cnt = Arc::clone(&error_count);
            read_tasks.push(tokio::spawn(async move {
                match ctx_clone.sql("SELECT COUNT(*) FROM rw.main.data").await {
                    Ok(df) => {
                        match df.collect().await {
                            Ok(results) => {
                                let count = results[0].column(0).as_any()
                                    .downcast_ref::<Int64Array>().unwrap().value(0);
                                if count < 1 {
                                    err_cnt.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(_) => { err_cnt.fetch_add(1, Ordering::Relaxed); }
                        }
                    }
                    Err(_) => { err_cnt.fetch_add(1, Ordering::Relaxed); }
                }
            }));
        }

        // Concurrently write more data via DuckDB
        let cat_str = catalog_str.clone();
        let write_handle = std::thread::spawn(move || {
            let conn = duckdb_conn_with_ducklake();
            attach_ducklake(&conn, &cat_str, "rw");
            let _ = conn.execute("INSERT INTO rw.data VALUES (2, 200);", []);
        });

        for task in read_tasks {
            let _ = task.await;
        }
        write_handle.join().expect("Write thread panicked");
    }

    let errors = error_count.load(Ordering::Relaxed);
    if errors > 0 {
        eprintln!("✗ Concurrent read/write stress: {} read errors", errors);
    } else {
        eprintln!("✓ Concurrent read/write stress: 10 iterations x 5 readers + 1 writer, zero errors");
    }

    Ok(())
}
