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

use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
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
    conn.execute(
        &format!("ATTACH '{ducklake_path}' AS dl (READ_ONLY);"),
        [],
    )
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

fn duckdb_value_to_string(v: &duckdb::types::Value) -> String {
    match v {
        duckdb::types::Value::Null => "NULL".to_string(),
        duckdb::types::Value::Boolean(b) => b.to_string(),
        duckdb::types::Value::TinyInt(i) => i.to_string(),
        duckdb::types::Value::SmallInt(i) => i.to_string(),
        duckdb::types::Value::Int(i) => i.to_string(),
        duckdb::types::Value::BigInt(i) => i.to_string(),
        duckdb::types::Value::Float(f) => format!("{f}"),
        duckdb::types::Value::Double(f) => format!("{f}"),
        duckdb::types::Value::Text(s) => s.clone(),
        duckdb::types::Value::HugeInt(i) => i.to_string(),
        duckdb::types::Value::Date32(days) => {
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163).unwrap();
            date.format("%Y-%m-%d").to_string()
        }
        duckdb::types::Value::Timestamp(unit, val) => {
            // DuckDB timestamps: convert to seconds + subsec based on unit
            let (secs, nsecs) = match unit {
                duckdb::types::TimeUnit::Second => (*val, 0u32),
                duckdb::types::TimeUnit::Millisecond => (val / 1000, ((val % 1000) * 1_000_000) as u32),
                duckdb::types::TimeUnit::Microsecond => (val / 1_000_000, ((val % 1_000_000) * 1_000) as u32),
                duckdb::types::TimeUnit::Nanosecond => (val / 1_000_000_000, (val % 1_000_000_000) as u32),
            };
            let dt = chrono::DateTime::from_timestamp(secs, nsecs).unwrap();
            dt.format("%Y-%m-%d %H:%M:%S").to_string()
        }
        _ => {
            // Fallback: use Debug format but try to extract the value
            let s = format!("{v:?}");
            // Handle Decimal(123.45) -> 123.45
            if s.starts_with("Decimal(") && s.ends_with(')') {
                return s[8..s.len() - 1].to_string();
            }
            s
        }
    }
}

/// Convert DataFusion RecordBatch results to Vec<Vec<String>> for comparison.
fn batches_to_strings(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    row.push("NULL".to_string());
                } else {
                    row.push(arrow_value_to_string(col, row_idx));
                }
            }
            rows.push(row);
        }
    }
    rows
}

fn arrow_value_to_string(array: &dyn Array, idx: usize) -> String {
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.value(idx).to_string()
        }
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
            a.value(idx).to_string()
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            a.value(idx).to_string()
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            a.value(idx).to_string()
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            a.value(idx).to_string()
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            format!("{}", a.value(idx))
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{}", a.value(idx))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            a.value(idx).to_string()
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            a.value(idx).to_string()
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            // Date32 stores days since epoch
            let days = a.value(idx);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163).unwrap();
            date.format("%Y-%m-%d").to_string()
        }
        DataType::Timestamp(unit, _) => {
            let s = match unit {
                arrow::datatypes::TimeUnit::Microsecond => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let us = a.value(idx);
                    let secs = us / 1_000_000;
                    let subsec_us = (us % 1_000_000) as u32;
                    let dt = chrono::DateTime::from_timestamp(secs, subsec_us * 1000).unwrap();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    let ns = a.value(idx);
                    let secs = ns / 1_000_000_000;
                    let subsec_ns = (ns % 1_000_000_000) as u32;
                    let dt = chrono::DateTime::from_timestamp(secs, subsec_ns).unwrap();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let ms = a.value(idx);
                    let secs = ms / 1_000;
                    let subsec_ms = (ms % 1_000) as u32;
                    let dt =
                        chrono::DateTime::from_timestamp(secs, subsec_ms * 1_000_000).unwrap();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                }
                arrow::datatypes::TimeUnit::Second => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    let s = a.value(idx);
                    let dt = chrono::DateTime::from_timestamp(s, 0).unwrap();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                }
            };
            s
        }
        DataType::Decimal128(_, scale) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = a.value(idx);
            let scale = *scale as u32;
            let divisor = 10i128.pow(scale);
            let whole = raw / divisor;
            let frac = (raw % divisor).unsigned_abs();
            format!("{whole}.{frac:0>width$}", width = scale as usize)
        }
        other => format!("<unsupported:{other:?}>"),
    }
}

/// Assert two result sets are equal (after normalizing floats).
fn assert_results_equal(
    scenario: &str,
    expected: &[Vec<String>],
    actual: &[Vec<String>],
) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "[{scenario}] Row count mismatch: expected {} rows, got {}.\n  Expected: {expected:?}\n  Actual:   {actual:?}",
        expected.len(),
        actual.len()
    );
    for (i, (exp_row, act_row)) in expected.iter().zip(actual.iter()).enumerate() {
        assert_eq!(
            exp_row.len(),
            act_row.len(),
            "[{scenario}] Column count mismatch at row {i}: expected {} cols, got {}",
            exp_row.len(),
            act_row.len()
        );
        for (j, (exp_val, act_val)) in exp_row.iter().zip(act_row.iter()).enumerate() {
            // Normalize floats for comparison
            let exp_norm = normalize_value(exp_val);
            let act_norm = normalize_value(act_val);
            assert_eq!(
                exp_norm, act_norm,
                "[{scenario}] Mismatch at row {i}, col {j}: expected '{exp_val}', got '{act_val}'"
            );
        }
    }
}

/// Normalize a string value for comparison (handle float precision differences).
fn normalize_value(s: &str) -> String {
    if s == "NULL" {
        return s.to_string();
    }
    // Try parsing as f64 for float comparison
    if let Ok(f) = s.parse::<f64>() {
        // Round to 6 decimal places to handle precision differences
        return format!("{:.6}", f);
    }
    s.to_string()
}

/// Helper: create a DuckLake catalog with DuckDB and run setup SQL.
fn setup_ducklake_catalog(catalog_path: &std::path::Path, setup_sql: &str) {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute("INSTALL ducklake;", []).unwrap();
    conn.execute("LOAD ducklake;", []).unwrap();
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(
        &format!("ATTACH '{ducklake_path}' AS dl;"),
        [],
    )
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
    let actual = batches_to_strings(&actual_batches);

    assert_results_equal("CRUD after INSERT", &expected, &actual);
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
    let actual = batches_to_strings(&actual_batches);

    assert_results_equal("CRUD after DELETE", &expected, &actual);
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
    let actual = batches_to_strings(&actual_batches);

    assert_results_equal("CRUD after UPDATE", &expected, &actual);
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
    let actual = batches_to_strings(&actual_batches);

    assert_results_equal("Type handling", &expected, &actual);
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
    let actual = batches_to_strings(&actual_batches);

    assert_results_equal("Schema operations", &expected, &actual);
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
    let actual = batches_to_strings(&actual_batches);

    assert_results_equal("NULL IS NULL filter", &expected, &actual);
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
    let expected = duckdb_query(
        &path_str,
        "SELECT COUNT(*), COUNT(val) FROM dl.main.nulls",
    );
    assert_eq!(expected.len(), 1);
    assert_eq!(expected[0], vec!["3", "1"]);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(&ctx, "SELECT COUNT(*), COUNT(val) FROM dl.main.nulls").await?;
    let actual = batches_to_strings(&actual_batches);

    assert_results_equal("NULL count semantics", &expected, &actual);
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
    let expected = duckdb_query(
        &path_str,
        "SELECT * FROM dl.main.alter_test ORDER BY id",
    );
    assert_eq!(expected.len(), 2);

    let ctx = create_df_session(&path_str)?;
    let actual_batches = df_query(&ctx, "SELECT * FROM dl.main.alter_test ORDER BY id").await?;
    let actual = batches_to_strings(&actual_batches);

    assert_results_equal("ALTER TABLE ADD COLUMN", &expected, &actual);
    Ok(())
}
