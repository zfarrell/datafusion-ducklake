#![cfg(feature = "metadata-duckdb")]
//! Reproduction tests for catalog corruption issues from duckdb/ducklake.
//!
//! Each test attempts to reproduce a reported bug and verify that the
//! DataFusion-DuckLake extension handles the resulting catalog state correctly.

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int32Array, StringArray};
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use tempfile::TempDir;

/// Helper to create a DuckLakeCatalog from a catalog path.
fn create_catalog(path: &str) -> DataFusionResult<Arc<DuckLakeCatalog>> {
    let provider = DuckdbMetadataProvider::new(path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    Ok(Arc::new(catalog))
}

/// Helper to create a DuckLake catalog via DuckDB and return (conn, catalog_path, temp_dir).
/// The connection is in-memory but the catalog file is on disk.
fn setup_ducklake() -> (duckdb::Connection, String, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir
        .path()
        .join("test.ducklake")
        .to_string_lossy()
        .to_string();

    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute("INSTALL ducklake;", []).unwrap();
    conn.execute("LOAD ducklake;", []).unwrap();

    let ducklake_uri = format!("ducklake:{}", catalog_path);
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_uri),
        [],
    )
    .unwrap();

    (conn, catalog_path, temp_dir)
}

/// Helper to create a SessionContext with the DuckLake catalog registered.
fn create_session(catalog_path: &str) -> DataFusionResult<SessionContext> {
    let catalog = create_catalog(catalog_path)?;
    let ctx = SessionContext::new();
    ctx.register_catalog("test_catalog", catalog);
    Ok(ctx)
}

// ============================================================================
// Issue #69: DROP TABLE on partitioned table causes "Invalid Input Error"
// https://github.com/duckdb/ducklake/issues/69
//
// After dropping a partitioned table, all subsequent queries fail with
// "Could not find matching table for partition entry".
// We verify that our reader still works after such a drop.
// ============================================================================
#[tokio::test]
async fn test_issue_069_drop_partitioned_table() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Create a non-partitioned table that should survive
    conn.execute(
        "CREATE TABLE test_catalog.survivors (id INT, value VARCHAR);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.survivors VALUES (1, 'alive'), (2, 'well');",
        [],
    )
    .unwrap();

    // Create a partitioned table then drop it
    conn.execute(
        "CREATE TABLE test_catalog.test_update (id INT, greeting VARCHAR, amount INT);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.test_update VALUES (1, 'hello', 60);",
        [],
    )
    .unwrap();

    // Partition and drop — this is the operation that triggered the bug
    let partition_result =
        conn.execute("ALTER TABLE test_catalog.test_update SET PARTITIONED BY (greeting);", []);
    if partition_result.is_err() {
        // If DuckLake version doesn't support partitioning, skip gracefully
        eprintln!(
            "SKIP issue #69: partitioning not supported in this DuckLake version: {:?}",
            partition_result.err()
        );
        return Ok(());
    }

    conn.execute("DROP TABLE test_catalog.test_update;", [])
        .unwrap();

    // Now verify the catalog is still usable via DataFusion
    let ctx = create_session(&catalog_path)?;

    let result = ctx
        .sql("SELECT id, value FROM test_catalog.main.survivors ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(result.len(), 1);
    let ids: Vec<i32> = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(ids, vec![1, 2]);

    Ok(())
}

// ============================================================================
// Issue #101: Dropping partitioned table breaks metadata
// https://github.com/duckdb/ducklake/issues/101
//
// Similar to #69 but specifically tests: create, partition, insert, select, drop.
// The last select after drop should not affect other tables.
// ============================================================================
#[tokio::test]
async fn test_issue_101_drop_partitioned_table_metadata() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Create table that should survive
    conn.execute(
        "CREATE TABLE test_catalog.stable_table (name VARCHAR);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.stable_table VALUES ('james'), ('sarah');",
        [],
    )
    .unwrap();

    // Create the problematic partitioned table
    conn.execute(
        "CREATE TABLE test_catalog.bob (name VARCHAR);",
        [],
    )
    .unwrap();

    let partition_result =
        conn.execute("ALTER TABLE test_catalog.bob SET PARTITIONED BY (name);", []);
    if partition_result.is_err() {
        eprintln!(
            "SKIP issue #101: partitioning not supported: {:?}",
            partition_result.err()
        );
        return Ok(());
    }

    conn.execute("INSERT INTO test_catalog.bob VALUES ('james');", [])
        .unwrap();

    // Drop the partitioned table
    conn.execute("DROP TABLE test_catalog.bob;", []).unwrap();

    // Verify catalog is usable — the dropped table should not corrupt metadata
    let ctx = create_session(&catalog_path)?;

    let result = ctx
        .sql("SELECT name FROM test_catalog.main.stable_table ORDER BY name")
        .await?
        .collect()
        .await?;

    assert_eq!(result.len(), 1);
    let names: Vec<&str> = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(names, vec!["james", "sarah"]);

    Ok(())
}

// ============================================================================
// Issue #197: Two ducklakes, one catalog
// https://github.com/duckdb/ducklake/issues/197
//
// Tests that multiple schemas in one catalog don't interfere with each other.
// While the original issue is a feature request, we verify our reader handles
// multi-schema catalogs correctly.
// ============================================================================
#[tokio::test]
async fn test_issue_197_multi_schema_catalog() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Create two schemas with tables that have the same column names
    conn.execute("CREATE SCHEMA test_catalog.tenant_a;", [])
        .unwrap();
    conn.execute("CREATE SCHEMA test_catalog.tenant_b;", [])
        .unwrap();

    conn.execute(
        "CREATE TABLE test_catalog.tenant_a.users (id INT, name VARCHAR);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.tenant_a.users VALUES (1, 'Alice');",
        [],
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE test_catalog.tenant_b.users (id INT, name VARCHAR);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.tenant_b.users VALUES (2, 'Bob');",
        [],
    )
    .unwrap();

    // Verify both schemas are readable and isolated
    let ctx = create_session(&catalog_path)?;

    let result_a = ctx
        .sql("SELECT id, name FROM test_catalog.tenant_a.users")
        .await?
        .collect()
        .await?;
    assert_eq!(result_a.len(), 1);
    let ids_a: Vec<i32> = result_a[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(ids_a, vec![1]);

    let result_b = ctx
        .sql("SELECT id, name FROM test_catalog.tenant_b.users")
        .await?
        .collect()
        .await?;
    assert_eq!(result_b.len(), 1);
    let ids_b: Vec<i32> = result_b[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(ids_b, vec![2]);

    Ok(())
}

// ============================================================================
// Issue #230: Duck Lake unusable when you drop a table
// https://github.com/duckdb/ducklake/issues/230
//
// After dropping a table, the entire data lake becomes unusable — cannot
// create new tables or query existing ones. We verify our reader still works.
// ============================================================================
#[tokio::test]
async fn test_issue_230_catalog_usable_after_drop() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Create two tables
    conn.execute(
        "CREATE TABLE test_catalog.table_a (id INT, val VARCHAR);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.table_a VALUES (1, 'a'), (2, 'b');",
        [],
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE test_catalog.table_b (id INT, data VARCHAR);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.table_b VALUES (10, 'x'), (20, 'y');",
        [],
    )
    .unwrap();

    // Drop table_a
    conn.execute("DROP TABLE test_catalog.table_a;", [])
        .unwrap();

    // Verify table_b is still readable
    let ctx = create_session(&catalog_path)?;

    let result = ctx
        .sql("SELECT id, data FROM test_catalog.main.table_b ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(result.len(), 1);
    let ids: Vec<i32> = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(ids, vec![10, 20]);

    // The dropped table should not be found
    let dropped = ctx
        .sql("SELECT * FROM test_catalog.main.table_a")
        .await;
    assert!(
        dropped.is_err(),
        "Dropped table should not be queryable"
    );

    Ok(())
}

// ============================================================================
// Issue #268: Catalog metadata corruption during concurrent table creation
// https://github.com/duckdb/ducklake/issues/268
//
// When multiple processes create tables concurrently, catalog metadata can
// become corrupted with file path mismatches. We test sequential creation
// (concurrent DuckDB writes aren't safe from Rust) and verify no cross-talk.
// ============================================================================
#[tokio::test]
async fn test_issue_268_concurrent_table_creation_metadata() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Create multiple tables rapidly (simulating the concurrent scenario)
    for i in 0..5 {
        conn.execute(
            &format!(
                "CREATE TABLE test_catalog.analytics_type{}_results (id INT, score DOUBLE, label VARCHAR);",
                i
            ),
            [],
        )
        .unwrap();
        conn.execute(
            &format!(
                "INSERT INTO test_catalog.analytics_type{}_results VALUES ({}, {}.5, 'label_{}');",
                i, i, i, i
            ),
            [],
        )
        .unwrap();
    }

    // Verify each table has the correct data (no cross-contamination)
    let ctx = create_session(&catalog_path)?;

    for i in 0..5 {
        let result = ctx
            .sql(&format!(
                "SELECT id, label FROM test_catalog.main.analytics_type{}_results",
                i
            ))
            .await?
            .collect()
            .await?;

        assert_eq!(result.len(), 1, "Table {} should have one batch", i);
        let ids: Vec<i32> = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(ids, vec![i as i32], "Table {} should have id={}", i, i);

        let labels: Vec<&str> = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(
            labels,
            vec![format!("label_{}", i).as_str()],
            "Table {} should have correct label",
            i
        );
    }

    Ok(())
}

// ============================================================================
// Issue #284: Two tables created concurrently mix data
// https://github.com/duckdb/ducklake/issues/284
//
// Creating two tables with different schemas at the same time causes data
// from one table to load into the other. We verify schema/data isolation.
// ============================================================================
#[tokio::test]
async fn test_issue_284_table_data_isolation() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Create two tables with very different schemas
    conn.execute(
        "CREATE TABLE test_catalog.orders (order_id INT, amount DECIMAL(12,2), status VARCHAR);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.orders VALUES (100, 49.99, 'shipped');",
        [],
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE test_catalog.metrics (ts TIMESTAMP, value DOUBLE, sensor_id INT);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.metrics VALUES ('2024-01-01 12:00:00', 23.5, 7);",
        [],
    )
    .unwrap();

    // Verify each table has correct schema and data
    let ctx = create_session(&catalog_path)?;

    let orders = ctx
        .sql("SELECT order_id, status FROM test_catalog.main.orders")
        .await?
        .collect()
        .await?;
    assert_eq!(orders.len(), 1);
    let order_ids: Vec<i32> = orders[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(order_ids, vec![100]);
    let statuses: Vec<&str> = orders[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(statuses, vec!["shipped"]);

    let metrics = ctx
        .sql("SELECT sensor_id FROM test_catalog.main.metrics")
        .await?
        .collect()
        .await?;
    assert_eq!(metrics.len(), 1);
    let sensor_ids: Vec<i32> = metrics[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(sensor_ids, vec![7]);

    Ok(())
}

// ============================================================================
// Issue #322: NULL shared_ptr from table_stats table_id matching a view_id
// https://github.com/duckdb/ducklake/issues/322
//
// A row in ducklake_table_stats with table_id matching a view_id causes
// a crash. We test that our reader handles catalogs with views correctly.
// ============================================================================
#[tokio::test]
async fn test_issue_322_view_table_stats_conflict() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Create a table
    conn.execute(
        "CREATE TABLE test_catalog.base_data (id INT, value VARCHAR);",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.base_data VALUES (1, 'one'), (2, 'two'), (3, 'three');",
        [],
    )
    .unwrap();

    // Create a view on top of the table
    let view_result = conn.execute(
        "CREATE VIEW test_catalog.data_view AS SELECT id, value FROM test_catalog.base_data WHERE id > 1;",
        [],
    );

    if view_result.is_err() {
        eprintln!(
            "SKIP issue #322: CREATE VIEW not supported: {:?}",
            view_result.err()
        );
        return Ok(());
    }

    // Verify the base table is still readable via DataFusion
    // (The bug was in DuckDB's information_schema.tables, our reader should handle this)
    let ctx = create_session(&catalog_path)?;

    let result = ctx
        .sql("SELECT id, value FROM test_catalog.main.base_data ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(result.len(), 1);
    let ids: Vec<i32> = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);

    Ok(())
}

// ============================================================================
// Issue #362: Catalog sometimes can't find ducklake_data_file table
// https://github.com/duckdb/ducklake/issues/362
//
// The catalog intermittently fails to find the ducklake_data_file table.
// We stress-test multiple sequential reads to catch intermittent failures.
// ============================================================================
#[tokio::test]
async fn test_issue_362_data_file_table_accessible() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Create several tables with data to populate ducklake_data_file
    for i in 0..3 {
        conn.execute(
            &format!(
                "CREATE TABLE test_catalog.t{} (id INT, data VARCHAR);",
                i
            ),
            [],
        )
        .unwrap();
        conn.execute(
            &format!(
                "INSERT INTO test_catalog.t{} VALUES ({}, 'data_{}');",
                i, i, i
            ),
            [],
        )
        .unwrap();
    }

    // Read from the catalog multiple times to check for intermittent failures
    for attempt in 0..5 {
        let ctx = create_session(&catalog_path)?;

        for i in 0..3 {
            let result = ctx
                .sql(&format!(
                    "SELECT id FROM test_catalog.main.t{}",
                    i
                ))
                .await?
                .collect()
                .await;

            assert!(
                result.is_ok(),
                "Attempt {}: Failed to query t{}: {:?}",
                attempt,
                i,
                result.err()
            );
            let batches = result.unwrap();
            assert_eq!(
                batches.len(),
                1,
                "Attempt {}: t{} should have one batch",
                attempt,
                i
            );
        }
    }

    Ok(())
}

// ============================================================================
// Issue #651: Transaction succeeds when there are errors
// https://github.com/duckdb/ducklake/issues/651
//
// A transaction containing parser errors still commits successfully,
// potentially creating incomplete metadata. We test that tables created
// in such a transaction are readable.
// ============================================================================
#[tokio::test]
async fn test_issue_651_transaction_with_errors() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Begin a transaction, include a statement that will error, then create a table
    conn.execute("BEGIN TRANSACTION;", []).unwrap();

    // This should fail but the transaction may continue
    let _bad = conn.execute(
        "CREATE TABLE test_catalog.nonexistent_schema_xyz.bad_table (x INT);",
        [],
    );

    // Create a valid table in the same transaction
    conn.execute(
        "CREATE TABLE test_catalog.accounts (id INT, name VARCHAR, balance DECIMAL(10,2));",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO test_catalog.accounts VALUES (1, 'Alice', 1000.00), (2, 'Bob', 500.00);",
        [],
    )
    .unwrap();

    conn.execute("COMMIT;", []).unwrap();

    // Verify the successfully-created table is readable
    let ctx = create_session(&catalog_path)?;

    let result = ctx
        .sql("SELECT id, name FROM test_catalog.main.accounts ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(result.len(), 1);
    let ids: Vec<i32> = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(ids, vec![1, 2]);

    Ok(())
}

// ============================================================================
// Issue #683: Transactional DDL puts the DuckLake catalog in unusable state
// https://github.com/duckdb/ducklake/issues/683
//
// Running CREATE TABLE and ALTER TABLE in separate transactions causes
// "Column with name id already exists!" on any subsequent query.
// We verify our reader handles catalogs after transactional DDL.
// ============================================================================
#[tokio::test]
async fn test_issue_683_transactional_ddl() -> DataFusionResult<()> {
    let (conn, catalog_path, _temp_dir) = setup_ducklake();

    // Create table in one transaction
    conn.execute("BEGIN TRANSACTION;", []).unwrap();
    conn.execute(
        "CREATE TABLE test_catalog.users (id INT, name VARCHAR);",
        [],
    )
    .unwrap();
    conn.execute("COMMIT;", []).unwrap();

    // Insert data
    conn.execute(
        "INSERT INTO test_catalog.users VALUES (1, 'Alice'), (2, 'Bob');",
        [],
    )
    .unwrap();

    // ALTER TABLE in a separate transaction (this was the problematic pattern)
    conn.execute("BEGIN TRANSACTION;", []).unwrap();
    let alter_result = conn.execute(
        "ALTER TABLE test_catalog.users ADD COLUMN email VARCHAR;",
        [],
    );
    if alter_result.is_ok() {
        conn.execute("COMMIT;", []).unwrap();
    } else {
        // If ALTER fails, rollback and just verify with original schema
        let _ = conn.execute("ROLLBACK;", []);
        eprintln!(
            "Issue #683: ALTER TABLE failed (may be expected): {:?}",
            alter_result.err()
        );
    }

    // Verify the catalog is still usable via DataFusion
    let ctx = create_session(&catalog_path)?;

    let result = ctx
        .sql("SELECT id, name FROM test_catalog.main.users ORDER BY id")
        .await?
        .collect()
        .await?;

    assert_eq!(result.len(), 1);
    let ids: Vec<i32> = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(ids, vec![1, 2]);

    let names: Vec<&str> = result[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .flatten()
        .collect();
    assert_eq!(names, vec!["Alice", "Bob"]);

    Ok(())
}
