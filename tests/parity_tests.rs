#![cfg(feature = "metadata-duckdb")]
//! Parity tests: verify DataFusion+DuckLake produces the same results as DuckDB+DuckLake.
//!
//! Each scenario:
//! 1. Creates a DuckLake catalog via DuckDB (writes data, performs DML)
//! 2. Queries via DuckDB to get expected results
//! 3. Queries via DataFusion+DuckLake to get actual results
//! 4. Compares row-by-row

mod common;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use common::test_utils::{assert_results_eq, batches_to_strings_filtered, duckdb_value_to_string};
use datafusion::error::Result as DfResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use tempfile::TempDir;

/// Helper: create a read-only DataFusion session with a DuckLake catalog.
fn create_df_session(catalog_path: &str) -> DfResult<SessionContext> {
    let provider = DuckdbMetadataProvider::new(catalog_path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let ctx = SessionContext::new();
    ctx.register_catalog("dl", Arc::new(catalog));
    Ok(ctx)
}

/// Helper: run a SQL query via DataFusion and collect all batches.
async fn df_query(ctx: &SessionContext, sql: &str) -> DfResult<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Helper: run a DuckDB query on an existing DuckLake catalog and return results as strings.
/// Each row is a Vec<String> where NULL → "NULL".
fn duckdb_query(catalog_path: &str, sql: &str) -> Vec<Vec<String>> {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute("INSTALL ducklake;", []).unwrap();
    conn.execute("LOAD ducklake;", []).unwrap();
    let ducklake_path = format!("ducklake:{catalog_path}");
    conn.execute(&format!("ATTACH '{ducklake_path}' AS dl (READ_ONLY);"), [])
        .unwrap();

    let mut stmt = conn.prepare(sql).unwrap();
    let mut duckdb_rows = stmt.query([]).unwrap();

    let mut results = Vec::new();
    while let Some(row) = duckdb_rows.next().unwrap() {
        let mut vals = Vec::new();
        // Try columns until we get an error (no column_count before execution)
        for i in 0.. {
            match row.get::<_, duckdb::types::Value>(i) {
                Ok(v) => vals.push(duckdb_value_to_string(&v)),
                Err(_) => break,
            }
        }
        results.push(vals);
    }
    results
}

/// Helper: create a DuckLake catalog with DuckDB and run setup SQL.
fn setup_ducklake_catalog(catalog_path: &std::path::Path, setup_sql: &str) {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute("INSTALL ducklake;", []).unwrap();
    conn.execute("LOAD ducklake;", []).unwrap();
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{ducklake_path}' AS dl;"), [])
        .unwrap();

    // Execute each statement separately
    for stmt in setup_sql.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        conn.execute(&format!("{stmt};"), []).unwrap();
    }
}

// ========================= SCENARIO 1: Basic CRUD =========================

#[tokio::test]
async fn parity_basic_crud_after_insert() -> DfResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("crud.ducklake");

    setup_ducklake_catalog(
        &catalog_path,
        "CREATE TABLE dl.main.t (id INTEGER NOT NULL, name VARCHAR, value DOUBLE);
         INSERT INTO dl.main.t VALUES (1, 'alice', 10.5), (2, 'bob', 20.0), (3, NULL, 30.5)",
    );

    let path_str = catalog_path.to_string_lossy().to_string();
    let expected = duckdb_query(&path_str, "SELECT * FROM dl.main.t ORDER BY id");
    assert_eq!(expected.len(), 3);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(&ctx, "SELECT * FROM dl.main.t ORDER BY id").await?;
    let actual = batches_to_strings_filtered(&actual_batches);

    assert_results_eq("CRUD after INSERT", &expected, &actual);
    Ok(())
}

#[tokio::test]
async fn parity_basic_crud_after_delete() -> DfResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("crud_del.ducklake");

    setup_ducklake_catalog(
        &catalog_path,
        "CREATE TABLE dl.main.t (id INTEGER NOT NULL, name VARCHAR, value DOUBLE);
         INSERT INTO dl.main.t VALUES (1, 'alice', 10.5), (2, 'bob', 20.0), (3, NULL, 30.5);
         DELETE FROM dl.main.t WHERE id = 2",
    );

    let path_str = catalog_path.to_string_lossy().to_string();
    let expected = duckdb_query(&path_str, "SELECT * FROM dl.main.t ORDER BY id");
    assert_eq!(expected.len(), 2);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(&ctx, "SELECT * FROM dl.main.t ORDER BY id").await?;
    let actual = batches_to_strings_filtered(&actual_batches);

    assert_results_eq("CRUD after DELETE", &expected, &actual);
    Ok(())
}

#[tokio::test]
async fn parity_basic_crud_after_update() -> DfResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("crud_upd.ducklake");

    setup_ducklake_catalog(
        &catalog_path,
        "CREATE TABLE dl.main.t (id INTEGER NOT NULL, name VARCHAR, value DOUBLE);
         INSERT INTO dl.main.t VALUES (1, 'alice', 10.5), (2, 'bob', 20.0), (3, NULL, 30.5);
         DELETE FROM dl.main.t WHERE id = 2;
         UPDATE dl.main.t SET value = 99.9 WHERE id = 1",
    );

    let path_str = catalog_path.to_string_lossy().to_string();
    let expected = duckdb_query(&path_str, "SELECT * FROM dl.main.t ORDER BY id");
    assert_eq!(expected.len(), 2);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(&ctx, "SELECT * FROM dl.main.t ORDER BY id").await?;
    let actual = batches_to_strings_filtered(&actual_batches);

    assert_results_eq("CRUD after UPDATE", &expected, &actual);
    Ok(())
}

// ========================= SCENARIO 2: Type Handling =========================

#[tokio::test]
async fn parity_type_handling() -> DfResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("types.ducklake");

    setup_ducklake_catalog(
        &catalog_path,
        "CREATE TABLE dl.main.types_test (
            i8 TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT,
            f32 FLOAT, f64 DOUBLE,
            s VARCHAR, b BOOLEAN,
            d DATE, ts TIMESTAMP,
            dec DECIMAL(10,2)
        );
         INSERT INTO dl.main.types_test VALUES (1, 2, 3, 4, 1.5, 2.5, 'hello', true, '2024-01-01', '2024-01-01 12:00:00', 123.45)",
    );

    let path_str = catalog_path.to_string_lossy().to_string();
    let expected = duckdb_query(&path_str, "SELECT * FROM dl.main.types_test");
    assert_eq!(expected.len(), 1);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(&ctx, "SELECT * FROM dl.main.types_test").await?;
    let actual = batches_to_strings_filtered(&actual_batches);

    assert_results_eq("Type handling", &expected, &actual);
    Ok(())
}

// ========================= SCENARIO 3: Schema Operations =========================

#[tokio::test]
async fn parity_schema_operations() -> DfResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("schema.ducklake");

    setup_ducklake_catalog(
        &catalog_path,
        "CREATE SCHEMA dl.test_schema;
         CREATE TABLE dl.test_schema.t1 (id INTEGER);
         INSERT INTO dl.test_schema.t1 VALUES (1)",
    );

    let path_str = catalog_path.to_string_lossy().to_string();
    let expected = duckdb_query(&path_str, "SELECT * FROM dl.test_schema.t1");
    assert_eq!(expected.len(), 1);
    assert_eq!(expected[0], vec!["1"]);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(&ctx, "SELECT * FROM dl.test_schema.t1").await?;
    let actual = batches_to_strings_filtered(&actual_batches);

    assert_results_eq("Schema operations", &expected, &actual);
    Ok(())
}

// ========================= SCENARIO 4: NULL Handling =========================

#[tokio::test]
async fn parity_null_is_null_filter() -> DfResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("nulls.ducklake");

    setup_ducklake_catalog(
        &catalog_path,
        "CREATE TABLE dl.main.nulls (id INTEGER NOT NULL, val VARCHAR);
         INSERT INTO dl.main.nulls VALUES (1, NULL), (2, 'a'), (3, NULL)",
    );

    let path_str = catalog_path.to_string_lossy().to_string();
    let expected = duckdb_query(
        &path_str,
        "SELECT * FROM dl.main.nulls WHERE val IS NULL ORDER BY id",
    );
    assert_eq!(expected.len(), 2);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(
        &ctx,
        "SELECT * FROM dl.main.nulls WHERE val IS NULL ORDER BY id",
    )
    .await?;
    let actual = batches_to_strings_filtered(&actual_batches);

    assert_results_eq("NULL IS NULL filter", &expected, &actual);
    Ok(())
}

#[tokio::test]
async fn parity_null_count_semantics() -> DfResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("nullcnt.ducklake");

    setup_ducklake_catalog(
        &catalog_path,
        "CREATE TABLE dl.main.nulls (id INTEGER NOT NULL, val VARCHAR);
         INSERT INTO dl.main.nulls VALUES (1, NULL), (2, 'a'), (3, NULL)",
    );

    let path_str = catalog_path.to_string_lossy().to_string();
    let expected = duckdb_query(&path_str, "SELECT COUNT(*), COUNT(val) FROM dl.main.nulls");
    assert_eq!(expected.len(), 1);
    assert_eq!(expected[0], vec!["3", "1"]);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(&ctx, "SELECT COUNT(*), COUNT(val) FROM dl.main.nulls").await?;
    let actual = batches_to_strings_filtered(&actual_batches);

    assert_results_eq("NULL count semantics", &expected, &actual);
    Ok(())
}

// ========================= SCENARIO 5: ALTER TABLE =========================

#[tokio::test]
async fn parity_alter_table_add_column() -> DfResult<()> {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("alter.ducklake");

    setup_ducklake_catalog(
        &catalog_path,
        "CREATE TABLE dl.main.alter_test (id INTEGER, name VARCHAR);
         INSERT INTO dl.main.alter_test VALUES (1, 'alice');
         ALTER TABLE dl.main.alter_test ADD COLUMN email VARCHAR;
         INSERT INTO dl.main.alter_test VALUES (2, 'bob', 'bob@test.com')",
    );

    let path_str = catalog_path.to_string_lossy().to_string();
    let expected = duckdb_query(&path_str, "SELECT * FROM dl.main.alter_test ORDER BY id");
    assert_eq!(expected.len(), 2);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(&ctx, "SELECT * FROM dl.main.alter_test ORDER BY id").await?;
    let actual = batches_to_strings_filtered(&actual_batches);

    assert_results_eq("ALTER TABLE ADD COLUMN", &expected, &actual);
    Ok(())
}
