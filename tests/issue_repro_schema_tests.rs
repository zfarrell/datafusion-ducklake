#![cfg(feature = "metadata-duckdb")]
//! Reproduction tests for schema evolution and partition issues from DuckLake GitHub issues.
//!
//! Issues covered: #125, #332, #457, #470, #478, #509, #643, #733, #745, #749

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use tempfile::TempDir;

fn create_catalog(path: &str) -> DataFusionResult<Arc<DuckLakeCatalog>> {
    let provider = DuckdbMetadataProvider::new(path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    Ok(Arc::new(catalog))
}

/// Helper: open an in-memory DuckDB connection, install+load ducklake, attach a catalog
fn duckdb_setup(catalog_path: &std::path::Path) -> duckdb::Connection {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute("INSTALL ducklake;", []).unwrap();
    conn.execute("LOAD ducklake;", []).unwrap();
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{}' AS test_catalog;", ducklake_path),
        [],
    )
    .unwrap();
    conn
}

/// Helper: create a SessionContext registered with a DuckLake catalog at the given path
async fn create_ctx(catalog_path: &std::path::Path) -> DataFusionResult<SessionContext> {
    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", catalog);
    Ok(ctx)
}

// ==================== #125: partitioning breaks queries ====================
// https://github.com/duckdb/ducklake/issues/125
//
// When using DuckLake with partitioned tables, WHERE clauses on partition columns
// can return wrong data. We test that our extension can at least read a partitioned
// table and that the data is correct.

#[tokio::test]
async fn test_issue_125_partitioned_table_read() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue125.ducklake");
    let conn = duckdb_setup(&catalog_path);

    // Create a table, partition it, and insert data
    conn.execute(
        "CREATE TABLE test_catalog.orders (
            id INT,
            country VARCHAR,
            amount DECIMAL(10,2)
        );",
        [],
    )
    .unwrap();

    // Set partitioning by country
    conn.execute(
        "ALTER TABLE test_catalog.orders SET PARTITIONED BY (country);",
        [],
    )
    .unwrap();

    // Insert rows for multiple partitions
    conn.execute(
        "INSERT INTO test_catalog.orders VALUES
            (1, 'DE', 100.00),
            (2, 'FR', 200.00),
            (3, 'DE', 150.00),
            (4, 'US', 300.00);",
        [],
    )
    .unwrap();
    drop(conn);

    // Read via our extension
    let ctx = create_ctx(&catalog_path).await?;
    let df = ctx
        .sql("SELECT id, country, amount FROM ducklake.main.orders ORDER BY id")
        .await?;
    let batches = df.collect().await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "Should read all 4 rows from partitioned table");

    // Verify filtering on partition column returns correct data
    let df = ctx
        .sql("SELECT id, country FROM ducklake.main.orders WHERE country = 'DE' ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should get exactly 2 DE rows");

    // Verify the returned country values are all 'DE' (issue #125 returned wrong partition data)
    for batch in &batches {
        let countries = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..countries.len() {
            assert_eq!(
                countries.value(i),
                "DE",
                "Partition filter should only return DE rows"
            );
        }
    }

    std::mem::forget(temp_dir);
    Ok(())
}

// ==================== #332: column_id concurrency with ALTER TABLE ADD COLUMN ====================
// https://github.com/duckdb/ducklake/issues/332
//
// column_id not auto-incremented from next_catalog_id. We test that reading a table
// after ADD COLUMN works correctly and columns are properly ordered.

#[tokio::test]
async fn test_issue_332_add_column_id_ordering() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue332.ducklake");
    let conn = duckdb_setup(&catalog_path);

    // Create table with initial columns
    conn.execute(
        "CREATE TABLE test_catalog.users (
            id INT,
            name VARCHAR
        );",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO test_catalog.users VALUES (1, 'Alice'), (2, 'Bob');",
        [],
    )
    .unwrap();

    // Add columns via ALTER TABLE (the issue is about column_id assignment)
    conn.execute(
        "ALTER TABLE test_catalog.users ADD COLUMN email VARCHAR;",
        [],
    )
    .unwrap();

    conn.execute(
        "ALTER TABLE test_catalog.users ADD COLUMN age INT;",
        [],
    )
    .unwrap();

    // Insert new row with all columns
    conn.execute(
        "INSERT INTO test_catalog.users VALUES (3, 'Charlie', 'charlie@test.com', 30);",
        [],
    )
    .unwrap();

    drop(conn);

    // Read via our extension — verify schema has all 4 columns
    let ctx = create_ctx(&catalog_path).await?;
    let df = ctx
        .sql("SELECT id, name, email, age FROM ducklake.main.users ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should have 3 rows after ADD COLUMN + INSERT");

    // Row 3 should have the email and age values
    let last_batch = batches.last().unwrap();
    let schema = last_batch.schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "email");
    assert_eq!(schema.field(3).name(), "age");

    std::mem::forget(temp_dir);
    Ok(())
}

// ==================== #457: version compatibility ====================
// https://github.com/duckdb/ducklake/issues/457
//
// Automatic version update breaks older clients. We test that our extension can
// read the current metadata format correctly (we use whatever ducklake version is installed).

#[tokio::test]
async fn test_issue_457_metadata_version_read() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue457.ducklake");
    let conn = duckdb_setup(&catalog_path);

    // Create and populate a basic table
    conn.execute(
        "CREATE TABLE test_catalog.versioned (
            id INT,
            value VARCHAR
        );",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO test_catalog.versioned VALUES (1, 'v1'), (2, 'v2');",
        [],
    )
    .unwrap();

    drop(conn);

    // Read via our extension — just verify we can read the metadata successfully
    let ctx = create_ctx(&catalog_path).await?;
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.versioned")
        .await?;
    let batches = df.collect().await?;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "Should read 2 rows from versioned table");

    // Verify we can enumerate schemas and tables
    let df = ctx
        .sql("SELECT id, value FROM ducklake.main.versioned ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    assert!(!batches.is_empty());

    std::mem::forget(temp_dir);
    Ok(())
}

// ==================== #470: filter pushdown with OR/IN on partitioned tables ====================
// https://github.com/duckdb/ducklake/issues/470
//
// OR and IN filter pushdown reads all files instead of pruning. We test that
// queries with these operators return correct results on partitioned tables.

#[tokio::test]
async fn test_issue_470_filter_or_in_partitioned() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue470.ducklake");
    let conn = duckdb_setup(&catalog_path);

    conn.execute(
        "CREATE TABLE test_catalog.partitioned (
            id BIGINT,
            data_col VARCHAR,
            partition_col VARCHAR
        );",
        [],
    )
    .unwrap();

    conn.execute(
        "ALTER TABLE test_catalog.partitioned SET PARTITIONED BY (partition_col);",
        [],
    )
    .unwrap();

    // Insert data into multiple partitions
    conn.execute(
        "INSERT INTO test_catalog.partitioned VALUES
            (1, 'a', 'p1'),
            (2, 'b', 'p2'),
            (3, 'c', 'p3'),
            (4, 'd', 'p1'),
            (5, 'e', 'p2'),
            (6, 'f', 'p4');",
        [],
    )
    .unwrap();

    drop(conn);

    let ctx = create_ctx(&catalog_path).await?;

    // Test single equality (baseline)
    let df = ctx
        .sql("SELECT id FROM ducklake.main.partitioned WHERE partition_col = 'p1' ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "Single equality filter should return 2 rows for p1");

    // Test OR condition (issue #470: this reads all files)
    let df = ctx
        .sql("SELECT id FROM ducklake.main.partitioned WHERE partition_col = 'p1' OR partition_col = 'p2' ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4, "OR filter should return 4 rows for p1+p2");

    // Test IN clause (issue #470: this reads all files)
    let df = ctx
        .sql("SELECT id FROM ducklake.main.partitioned WHERE partition_col IN ('p1', 'p3') ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "IN filter should return 3 rows for p1+p3");

    std::mem::forget(temp_dir);
    Ok(())
}

// ==================== #478: MERGE on partitioned table causes internal error ====================
// https://github.com/duckdb/ducklake/issues/478
//
// MERGE INTO on partitioned table with date function partitioning fails.
// We test reading a table that was created with date-function partitioning after
// inserts (not MERGE, since that's a DuckDB-side issue).

#[tokio::test]
async fn test_issue_478_partition_by_date_function() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue478.ducklake");
    let conn = duckdb_setup(&catalog_path);

    conn.execute(
        "CREATE TABLE test_catalog.events (
            id INT,
            name VARCHAR,
            created_at TIMESTAMP
        );",
        [],
    )
    .unwrap();

    // Partition by day(created_at) — the type of setup that triggers the MERGE bug
    conn.execute(
        "ALTER TABLE test_catalog.events SET PARTITIONED BY (day(created_at));",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO test_catalog.events VALUES
            (1, 'evt1', TIMESTAMP '2025-01-15 10:00:00'),
            (2, 'evt2', TIMESTAMP '2025-01-16 11:00:00'),
            (3, 'evt3', TIMESTAMP '2025-01-15 12:00:00');",
        [],
    )
    .unwrap();

    drop(conn);

    // Read via our extension — verify we can handle date-function partitioned tables
    let ctx = create_ctx(&catalog_path).await?;
    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.events ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 3,
        "Should read all 3 rows from date-function partitioned table"
    );

    std::mem::forget(temp_dir);
    Ok(())
}

// ==================== #509: ALTER TABLE RENAME fails with prior table name ====================
// https://github.com/duckdb/ducklake/issues/509
//
// Renaming tables in sequence (dbt pattern) fails. We test reading after a table
// has been renamed.

#[tokio::test]
async fn test_issue_509_table_rename() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue509.ducklake");
    let conn = duckdb_setup(&catalog_path);

    // Create original table
    conn.execute(
        "CREATE TABLE test_catalog.customer (
            id INT,
            name VARCHAR
        );",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO test_catalog.customer VALUES (1, 'Alice'), (2, 'Bob');",
        [],
    )
    .unwrap();

    // dbt-style rename pattern: create tmp, rename old to backup, rename tmp to original
    conn.execute(
        "CREATE TABLE test_catalog.customer_tmp (
            id INT,
            name VARCHAR,
            email VARCHAR
        );",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO test_catalog.customer_tmp VALUES (1, 'Alice', 'alice@test.com'), (2, 'Bob', 'bob@test.com'), (3, 'Charlie', 'charlie@test.com');",
        [],
    )
    .unwrap();

    // Rename old table out of the way
    conn.execute(
        "ALTER TABLE test_catalog.customer RENAME TO customer_backup;",
        [],
    )
    .unwrap();

    // Rename tmp to the original name
    conn.execute(
        "ALTER TABLE test_catalog.customer_tmp RENAME TO customer;",
        [],
    )
    .unwrap();

    drop(conn);

    // Read via our extension — should see the new table (3 rows, 3 columns)
    let ctx = create_ctx(&catalog_path).await?;
    let df = ctx
        .sql("SELECT id, name, email FROM ducklake.main.customer ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 3,
        "Should see 3 rows from renamed table (customer_tmp -> customer)"
    );

    // Also verify the backup table is accessible
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.customer_backup")
        .await?;
    let batches = df.collect().await?;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "Backup table should have 2 rows");

    std::mem::forget(temp_dir);
    Ok(())
}

// ==================== #643: hive partitioning on date part produces wrong values ====================
// https://github.com/duckdb/ducklake/issues/643
//
// Hive partitioning with YEAR(timestamp) produces nonsensical partition values.
// We test reading a table partitioned by year function.

#[tokio::test]
async fn test_issue_643_partition_by_year() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue643.ducklake");
    let conn = duckdb_setup(&catalog_path);

    conn.execute(
        "CREATE TABLE test_catalog.ts_data (
            id INT,
            ts TIMESTAMP,
            value VARCHAR
        );",
        [],
    )
    .unwrap();

    // Partition by YEAR(ts)
    conn.execute(
        "ALTER TABLE test_catalog.ts_data SET PARTITIONED BY (year(ts));",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO test_catalog.ts_data VALUES
            (1, TIMESTAMP '2024-03-15 10:00:00', 'val1'),
            (2, TIMESTAMP '2025-06-20 11:00:00', 'val2'),
            (3, TIMESTAMP '2024-11-01 12:00:00', 'val3');",
        [],
    )
    .unwrap();

    drop(conn);

    // Read via our extension — verify data is correct
    let ctx = create_ctx(&catalog_path).await?;
    let df = ctx
        .sql("SELECT id, value FROM ducklake.main.ts_data ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 3,
        "Should read all 3 rows from year-partitioned table"
    );

    // Verify the actual data values are correct (not corrupted by partition weirdness)
    let mut all_ids: Vec<i32> = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..ids.len() {
            if !ids.is_null(i) {
                all_ids.push(ids.value(i));
            }
        }
    }
    all_ids.sort();
    assert_eq!(all_ids, vec![1, 2, 3], "Data values should be intact");

    std::mem::forget(temp_dir);
    Ok(())
}

// ==================== #733: snapshots broken after update ====================
// https://github.com/duckdb/ducklake/issues/733
//
// Cannot select snapshots after running UPDATE statements. We test that our
// extension can read a table correctly after UPDATE operations.

#[tokio::test]
async fn test_issue_733_read_after_updates() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue733.ducklake");
    let conn = duckdb_setup(&catalog_path);

    conn.execute(
        "CREATE TABLE test_catalog.data (
            id INT,
            status VARCHAR,
            value INT
        );",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO test_catalog.data VALUES
            (1, 'active', 100),
            (2, 'active', 200),
            (3, 'inactive', 300);",
        [],
    )
    .unwrap();

    // Perform UPDATE operations (this triggers the snapshot issue in #733)
    conn.execute(
        "UPDATE test_catalog.data SET status = 'archived' WHERE id = 1;",
        [],
    )
    .unwrap();

    conn.execute(
        "UPDATE test_catalog.data SET value = 250 WHERE id = 2;",
        [],
    )
    .unwrap();

    drop(conn);

    // Read via our extension — should see the latest snapshot correctly
    let ctx = create_ctx(&catalog_path).await?;
    let df = ctx
        .sql("SELECT id, status, value FROM ducklake.main.data ORDER BY id")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "Should have 3 rows after updates");

    // Verify updated values are reflected
    let mut rows: Vec<(i32, String, i32)> = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let statuses = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let values = batch.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                ids.value(i),
                statuses.value(i).to_string(),
                values.value(i),
            ));
        }
    }
    rows.sort_by_key(|r| r.0);

    assert_eq!(rows[0], (1, "archived".to_string(), 100));
    assert_eq!(rows[1], (2, "active".to_string(), 250));
    assert_eq!(rows[2], (3, "inactive".to_string(), 300));

    std::mem::forget(temp_dir);
    Ok(())
}

// ==================== #745: LIMIT ignores partition pruning ====================
// https://github.com/duckdb/ducklake/issues/745
//
// Queries with LIMIT clause scan all partitions instead of just relevant ones.
// We test that LIMIT queries on partitioned tables return correct results.

#[tokio::test]
async fn test_issue_745_limit_on_partitioned() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue745.ducklake");
    let conn = duckdb_setup(&catalog_path);

    conn.execute(
        "CREATE TABLE test_catalog.sensor_data (
            reading_id INT,
            sensor_id INT,
            value DOUBLE
        );",
        [],
    )
    .unwrap();

    conn.execute(
        "ALTER TABLE test_catalog.sensor_data SET PARTITIONED BY (sensor_id);",
        [],
    )
    .unwrap();

    // Insert multiple partitions
    conn.execute(
        "INSERT INTO test_catalog.sensor_data VALUES
            (1, 1, 10.0),
            (2, 1, 20.0),
            (3, 2, 30.0),
            (4, 2, 40.0),
            (5, 3, 50.0),
            (6, 3, 60.0);",
        [],
    )
    .unwrap();

    drop(conn);

    let ctx = create_ctx(&catalog_path).await?;

    // Query with filter + ORDER BY + LIMIT (the problematic pattern from issue #745)
    let df = ctx
        .sql("SELECT reading_id, value FROM ducklake.main.sensor_data WHERE sensor_id = 2 ORDER BY value DESC LIMIT 1")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "LIMIT 1 should return exactly 1 row");

    // Verify we got the right row (highest value for sensor_id=2)
    let batch = &batches[0];
    let reading_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(reading_ids.value(0), 4, "Should get reading_id=4 (value=40.0)");

    // Also test without filter but with LIMIT
    let df = ctx
        .sql("SELECT reading_id FROM ducklake.main.sensor_data ORDER BY reading_id LIMIT 3")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "LIMIT 3 should return exactly 3 rows");

    std::mem::forget(temp_dir);
    Ok(())
}

// ==================== #749: UPDATE on multi-partition table causes index error ====================
// https://github.com/duckdb/ducklake/issues/749
//
// UPDATE on table partitioned by (column, day(ts)) causes "Attempted to access index 5
// within vector of size 5". We test reading after such operations.

#[tokio::test]
async fn test_issue_749_multi_partition_update() -> DataFusionResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("issue749.ducklake");
    let conn = duckdb_setup(&catalog_path);

    conn.execute(
        "CREATE TABLE test_catalog.multi_part (
            p VARCHAR,
            ts TIMESTAMP,
            v VARCHAR
        );",
        [],
    )
    .unwrap();

    // Partition by (p, day(ts)) — the exact setup from issue #749
    conn.execute(
        "ALTER TABLE test_catalog.multi_part SET PARTITIONED BY (p, day(ts));",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO test_catalog.multi_part VALUES
            ('p1', TIMESTAMP '2026-02-05 10:00:00', 'va'),
            ('p2', TIMESTAMP '2026-02-06 11:00:00', 'vb');",
        [],
    )
    .unwrap();

    // The UPDATE in #749 causes the crash. We try it but catch errors since it may
    // fail in DuckLake itself. Either way, we should still be able to read existing data.
    let update_result = conn.execute(
        "UPDATE test_catalog.multi_part SET p = 'p3' WHERE v = 'va';",
        [],
    );

    drop(conn);

    // Read via our extension — should read whatever state the table is in
    let ctx = create_ctx(&catalog_path).await?;
    let df = ctx
        .sql("SELECT p, v FROM ducklake.main.multi_part ORDER BY v")
        .await?;
    let batches = df.collect().await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();

    if update_result.is_ok() {
        // If UPDATE succeeded, p1 should now be p3
        assert_eq!(total, 2, "Should have 2 rows after update");
        let mut rows: Vec<(String, String)> = Vec::new();
        for batch in &batches {
            let ps = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let vs = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                rows.push((ps.value(i).to_string(), vs.value(i).to_string()));
            }
        }
        rows.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(rows[0].0, "p3", "va row should be updated to p3");
        assert_eq!(rows[1].0, "p2", "vb row should remain p2");
    } else {
        // If UPDATE failed (reproducing the bug), we should still read the original 2 rows
        assert_eq!(
            total, 2,
            "Should have 2 rows (original data, update failed)"
        );
        eprintln!(
            "Issue #749 reproduced: UPDATE on multi-partition table failed: {:?}",
            update_result.err()
        );
    }

    std::mem::forget(temp_dir);
    Ok(())
}
