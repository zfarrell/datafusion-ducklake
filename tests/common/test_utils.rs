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
                duckdb::types::TimeUnit::Millisecond => {
                    (val / 1000, ((val % 1000) * 1_000_000) as u32)
                },
                duckdb::types::TimeUnit::Microsecond => {
                    (val / 1_000_000, ((val % 1_000_000) * 1_000) as u32)
                },
                duckdb::types::TimeUnit::Nanosecond => {
                    (val / 1_000_000_000, (val % 1_000_000_000) as u32)
                },
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
            format!("{}", a.value(idx))
        },
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{}", a.value(idx))
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
                let secs = us / 1_000_000;
                let subsec_us = (us % 1_000_000) as u32;
                let dt = chrono::DateTime::from_timestamp(secs, subsec_us * 1000).unwrap();
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            },
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
            },
            arrow::datatypes::TimeUnit::Millisecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let ms = a.value(idx);
                let secs = ms / 1_000;
                let subsec_ms = (ms % 1_000) as u32;
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
            format!("{whole}.{frac:0>width$}", width = scale as usize)
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
pub fn normalize_value(s: &str) -> String {
    if s == "NULL" {
        return s.to_string();
    }
    if let Ok(f) = s.parse::<f64>() {
        return format!("{:.6}", f);
    }
    s.to_string()
}

/// Assert two result sets are equal (after normalizing floats).
///
/// Checks both row count and column count before comparing values,
/// preventing false passes from zip truncation.
pub fn assert_results_eq(scenario: &str, expected: &[Vec<String>], actual: &[Vec<String>]) {
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
