//! Shared test utility functions for cross-engine and integration tests.
//!
//! This module provides common helpers for converting DuckDB values, Arrow arrays,
//! and RecordBatches to strings for comparison, as well as assertion helpers.

#![allow(dead_code)]

use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// Convert a DuckDB value to a string representation.
///
/// Handles all common DuckDB types including Date32, Timestamp (all units),
/// and Decimal. Falls back to Debug formatting for unknown types.
pub fn duckdb_value_to_string(v: &duckdb::types::Value) -> String {
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
        },
        duckdb::types::Value::Timestamp(unit, val) => {
            let (secs, nsecs) = match unit {
                duckdb::types::TimeUnit::Second => (*val, 0u32),
                duckdb::types::TimeUnit::Millisecond => (
                    val.div_euclid(1000),
                    (val.rem_euclid(1000) * 1_000_000) as u32,
                ),
                duckdb::types::TimeUnit::Microsecond => (
                    val.div_euclid(1_000_000),
                    (val.rem_euclid(1_000_000) * 1_000) as u32,
                ),
                duckdb::types::TimeUnit::Nanosecond => (
                    val.div_euclid(1_000_000_000),
                    val.rem_euclid(1_000_000_000) as u32,
                ),
            };
            let dt = chrono::DateTime::from_timestamp(secs, nsecs).unwrap();
            dt.format("%Y-%m-%d %H:%M:%S").to_string()
        },
        _ => {
            let s = format!("{v:?}");
            // Handle Decimal(123.45) -> 123.45
            if s.starts_with("Decimal(") && s.ends_with(')') {
                return s[8..s.len() - 1].to_string();
            }
            s
        },
    }
}

/// Convert an Arrow array value at a given index to a string.
///
/// Supports all common Arrow types including Boolean, Int8-64, UInt8-64,
/// Float32/64, Utf8, LargeUtf8, Date32, Timestamp (all units), and Decimal128.
pub fn arrow_value_to_string(array: &dyn Array, idx: usize) -> String {
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = a.value(idx);
            let s = v.to_string();
            // Ensure float values always have a decimal point (e.g., "20" → "20.0")
            if !s.contains('.') {
                format!("{s}.0")
            } else {
                s
            }
        },
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = a.value(idx);
            let s = v.to_string();
            if !s.contains('.') {
                format!("{s}.0")
            } else {
                s
            }
        },
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            a.value(idx).to_string()
        },
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = a.value(idx);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163).unwrap();
            date.format("%Y-%m-%d").to_string()
        },
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Microsecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let us = a.value(idx);
                let secs = us.div_euclid(1_000_000);
                let subsec_us = us.rem_euclid(1_000_000) as u32;
                let dt = chrono::DateTime::from_timestamp(secs, subsec_us * 1000).unwrap();
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            },
            arrow::datatypes::TimeUnit::Nanosecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let ns = a.value(idx);
                let secs = ns.div_euclid(1_000_000_000);
                let subsec_ns = ns.rem_euclid(1_000_000_000) as u32;
                let dt = chrono::DateTime::from_timestamp(secs, subsec_ns).unwrap();
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            },
            arrow::datatypes::TimeUnit::Millisecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let ms = a.value(idx);
                let secs = ms.div_euclid(1_000);
                let subsec_ms = ms.rem_euclid(1_000) as u32;
                let dt = chrono::DateTime::from_timestamp(secs, subsec_ms * 1_000_000).unwrap();
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            },
            arrow::datatypes::TimeUnit::Second => {
                let a = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap();
                let s = a.value(idx);
                let dt = chrono::DateTime::from_timestamp(s, 0).unwrap();
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            },
        },
        DataType::Decimal128(_, scale) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = a.value(idx);
            let scale = *scale as u32;
            let divisor = 10i128.pow(scale);
            let whole = raw / divisor;
            let frac = (raw % divisor).unsigned_abs();
            let sign = if raw < 0 && whole == 0 {
                "-"
            } else {
                ""
            };
            format!("{sign}{whole}.{frac:0>width$}", width = scale as usize)
        },
        other => format!("<unsupported:{other:?}>"),
    }
}

/// Virtual columns added by the extension that should be filtered out of results.
const VIRTUAL_COLUMNS: &[&str] =
    &["filename", "file_row_number", "rowid", "snapshot_id", "file_index"];

/// Convert RecordBatches to Vec<Vec<String>>, including all columns.
pub fn batches_to_strings(batches: &[RecordBatch]) -> Vec<Vec<String>> {
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

/// Convert RecordBatches to Vec<Vec<String>>, filtering out virtual columns.
pub fn batches_to_strings_filtered(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let col_indices: Vec<usize> = (0..batch.num_columns())
            .filter(|&i| {
                let name = schema.field(i).name().as_str();
                !VIRTUAL_COLUMNS.contains(&name)
            })
            .collect();
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for &col_idx in &col_indices {
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

/// Convert RecordBatches to sorted Vec<Vec<String>>, filtering out virtual columns.
pub fn batches_to_sorted_strings(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = batches_to_strings_filtered(batches);
    rows.sort();
    rows
}

/// Extract i32 values from a specific column in a record batch.
/// Supports both Int32 and Int64 columns, skipping nulls.
pub fn get_int_column(batch: &RecordBatch, col_idx: usize) -> Vec<i32> {
    let column = batch.column(col_idx);

    if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
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

    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
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
        "Column {} is not Int32 or Int64, got {:?}",
        col_idx,
        column.data_type()
    );
}

/// Normalize a string value for comparison (handle float precision differences).
///
/// Only normalizes values that look like floats (contain '.' or 'e'/'E') to avoid
/// collapsing distinct integer/float representations or losing large-integer precision.
pub fn normalize_value(s: &str) -> String {
    if s == "NULL" {
        return s.to_string();
    }
    // Only normalize values that look like floats — containing '.' or scientific notation.
    // Integer strings are compared exactly to preserve precision and detect type confusion.
    if s.contains('.') || s.contains('e') || s.contains('E') {
        if let Ok(f) = s.parse::<f64>() {
            return format!("{:.6}", f);
        }
    }
    s.to_string()
}

/// Assert two result sets are equal WITHOUT normalization.
///
/// Same structure checks as assert_results_eq but compares values directly.
/// Use this when you need strict type confusion detection (e.g., "1" vs "1.0").
pub fn assert_results_eq_strict(scenario: &str, expected: &[Vec<String>], actual: &[Vec<String>]) {
    if expected.len() != actual.len() {
        let max_show = 5;
        let exp_preview: Vec<_> = expected.iter().take(max_show).collect();
        let act_preview: Vec<_> = actual.iter().take(max_show).collect();
        panic!(
            "[{scenario}] Row count mismatch: expected {} rows, got {}.\n  \
             Expected (first {max_show}):\n{exp_preview:#?}\n  \
             Actual (first {max_show}):\n{act_preview:#?}",
            expected.len(),
            actual.len(),
        );
    }
    for (i, (exp_row, act_row)) in expected.iter().zip(actual.iter()).enumerate() {
        assert_eq!(
            exp_row.len(),
            act_row.len(),
            "[{scenario}] Column count mismatch at row {i}: expected {} cols, got {}.\n  Expected row: {exp_row:?}\n  Actual row:   {act_row:?}",
            exp_row.len(),
            act_row.len()
        );
        for (j, (exp_val, act_val)) in exp_row.iter().zip(act_row.iter()).enumerate() {
            assert_eq!(
                exp_val, act_val,
                "[{scenario}] Strict mismatch at row {i}, col {j}: expected '{exp_val}', got '{act_val}'"
            );
        }
    }
}

/// Assert two result sets are equal (after normalizing floats).
///
/// Checks both row count and column count before comparing values,
/// preventing false passes from zip truncation.
pub fn assert_results_eq(scenario: &str, expected: &[Vec<String>], actual: &[Vec<String>]) {
    if expected.len() != actual.len() {
        let max_show = 5;
        let exp_preview: Vec<_> = expected.iter().take(max_show).collect();
        let act_preview: Vec<_> = actual.iter().take(max_show).collect();
        panic!(
            "[{scenario}] Row count mismatch: expected {} rows, got {}.\n  \
             Expected (first {max_show}):\n{exp_preview:#?}\n  \
             Actual (first {max_show}):\n{act_preview:#?}",
            expected.len(),
            actual.len(),
        );
    }
    for (i, (exp_row, act_row)) in expected.iter().zip(actual.iter()).enumerate() {
        assert_eq!(
            exp_row.len(),
            act_row.len(),
            "[{scenario}] Column count mismatch at row {i}: expected {} cols, got {}.\n  Expected row: {exp_row:?}\n  Actual row:   {act_row:?}",
            exp_row.len(),
            act_row.len()
        );
        for (j, (exp_val, act_val)) in exp_row.iter().zip(act_row.iter()).enumerate() {
            let exp_norm = normalize_value(exp_val);
            let act_norm = normalize_value(act_val);
            assert_eq!(
                exp_norm, act_norm,
                "[{scenario}] Mismatch at row {i}, col {j}: expected '{exp_val}', got '{act_val}'"
            );
        }
    }
}

/// Run a SQL query via DataFusion and return results as string rows (filters virtual columns).
pub async fn df_query(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Vec<Vec<String>> {
    let df = ctx.sql(sql).await.expect("DataFusion SQL failed");
    let batches = df.collect().await.expect("DataFusion collect failed");
    batches_to_strings_filtered(&batches)
}

/// Wrapper for DuckDB operations on a DuckLake catalog.
///
/// Provides connection management and query helpers for cross-engine tests
/// where DuckDB writes data and DataFusion reads it.
pub struct DuckDbConn {
    pub conn: duckdb::Connection,
}

impl DuckDbConn {
    /// Open a DuckLake catalog in DuckDB using the SQLite backend.
    /// Attaches as `ducklake:sqlite:<path>`.
    pub fn open(catalog_db_path: &std::path::Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", []).expect("load ducklake");
        let attach_path = format!("ducklake:sqlite:{}", catalog_db_path.display());
        conn.execute(&format!("ATTACH '{}' AS ducklake;", attach_path), [])
            .expect("attach ducklake catalog");
        DuckDbConn {
            conn,
        }
    }

    /// Open a DuckLake catalog in DuckDB using the native DuckDB backend.
    /// Attaches as `ducklake:<path>` (no sqlite: prefix).
    pub fn open_native(catalog_path: &std::path::Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", []).expect("load ducklake");
        let attach_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(&format!("ATTACH '{}' AS ducklake;", attach_path), [])
            .expect("attach ducklake catalog");
        DuckDbConn {
            conn,
        }
    }

    /// Open/create a DuckLake catalog in DuckDB with a specified DATA_PATH.
    pub fn open_with_data_path(
        catalog_path: &std::path::Path,
        data_path: &std::path::Path,
    ) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", []).expect("load ducklake");
        let attach_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(
            &format!(
                "ATTACH '{}' AS ducklake (DATA_PATH '{}');",
                attach_path,
                data_path.display()
            ),
            [],
        )
        .expect("attach ducklake catalog with data path");
        DuckDbConn {
            conn,
        }
    }

    /// Execute a SQL statement (no results expected).
    pub fn execute(&self, sql: &str) {
        self.conn
            .execute(sql, [])
            .unwrap_or_else(|e| panic!("DuckDB execute failed: {e}\nSQL: {sql}"));
    }

    /// Query and return results as Vec<Vec<String>>.
    ///
    /// Uses the Rows column_count() API (available after execution) to iterate
    /// columns precisely instead of trial-and-error.
    pub fn query(&self, sql: &str) -> Vec<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(sql)
            .unwrap_or_else(|e| panic!("DuckDB prepare failed: {e}\nSQL: {sql}"));
        let mut rows = stmt.query([]).expect("DuckDB query failed");
        let col_count = rows.as_ref().map(|r| r.column_count()).unwrap_or(0);

        let mut results = Vec::new();
        while let Some(row) = rows.next().expect("DuckDB row iteration") {
            let mut vals = Vec::new();
            for i in 0..col_count {
                let v: duckdb::types::Value = row
                    .get(i)
                    .unwrap_or_else(|e| panic!("DuckDB column {i} decode error: {e}\nSQL: {sql}"));
                vals.push(duckdb_value_to_string(&v));
            }
            results.push(vals);
        }
        results
    }

    /// Query and return single-column results as Vec<String>.
    pub fn query_single_string(&self, sql: &str) -> Vec<String> {
        self.query(sql)
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    }

    /// Query a single scalar count value (e.g. SELECT COUNT(*) ...).
    pub fn query_count(&self, sql: &str) -> i64 {
        let rows = self.query(sql);
        assert_eq!(rows.len(), 1, "query_count expects exactly 1 row");
        rows[0][0].parse().unwrap()
    }

    /// Fallible query — returns Result instead of panicking.
    pub fn try_query(&self, sql: &str) -> std::result::Result<Vec<Vec<String>>, duckdb::Error> {
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query([])?;
        let col_count = rows.as_ref().map(|r| r.column_count()).unwrap_or(0);
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let mut vals = Vec::new();
            for i in 0..col_count {
                let v: duckdb::types::Value = row.get(i)?;
                vals.push(duckdb_value_to_string(&v));
            }
            results.push(vals);
        }
        Ok(results)
    }
}
