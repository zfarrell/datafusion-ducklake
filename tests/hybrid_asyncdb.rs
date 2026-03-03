#![cfg(feature = "metadata-duckdb")]
//! Hybrid AsyncDB adapter for running DuckDB DuckLake tests
//!
//! This adapter uses a hybrid approach:
//! - WRITE operations (CREATE/INSERT/UPDATE/DELETE) → DuckDB
//! - READ operations (SELECT) → DataFusion
//! - After each WRITE → Refresh DataFusion catalog to pick up metadata changes
//! - Table references rewritten for DataFusion: ducklake.table → ducklake.main.table
//!
//! This allows running DuckDB tests through DataFusion's read path.

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::prelude::*;
use datafusion_ducklake::DuckdbMetadataProvider;
use datafusion_ducklake::catalog::DuckLakeCatalog;
use duckdb::Connection;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Custom error type for hybrid adapter
#[derive(Debug)]
pub struct HybridError(String);

impl std::fmt::Display for HybridError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hybrid error: {}", self.0)
    }
}

impl std::error::Error for HybridError {}

impl From<duckdb::Error> for HybridError {
    fn from(e: duckdb::Error) -> Self {
        HybridError(format!("DuckDB error: {}", e))
    }
}

impl From<datafusion::error::DataFusionError> for HybridError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        HybridError(format!("DataFusion error: {}", e))
    }
}

impl From<datafusion_ducklake::error::DuckLakeError> for HybridError {
    fn from(e: datafusion_ducklake::error::DuckLakeError) -> Self {
        HybridError(format!("DuckLake error: {}", e))
    }
}

/// Hybrid database adapter: DuckDB for writes, DataFusion for reads
#[derive(Clone)]
pub struct HybridDuckLakeDB {
    /// DuckDB connection for WRITE operations
    duckdb_conn: Arc<Mutex<Connection>>,
    /// DataFusion context for READ operations
    datafusion_ctx: Arc<Mutex<SessionContext>>,
    /// Path to DuckLake catalog file
    catalog_path: PathBuf,
    /// Whether USE ducklake has been executed (shared across clones)
    use_ducklake: Arc<std::sync::atomic::AtomicBool>,
    /// Whether we're inside a transaction (between BEGIN and COMMIT/ROLLBACK)
    in_transaction: Arc<std::sync::atomic::AtomicBool>,
}

impl HybridDuckLakeDB {
    pub fn new(catalog_path: PathBuf) -> Result<Self, HybridError> {
        // Create data files directory
        let data_path = catalog_path.with_extension("files");
        std::fs::create_dir_all(&data_path)
            .map_err(|e| HybridError(format!("Failed to create data directory: {}", e)))?;

        // Create DuckDB connection for WRITE operations
        let conn = Connection::open_in_memory()?;
        conn.execute("INSTALL ducklake;", [])?;
        conn.execute("LOAD ducklake;", [])?;

        let ducklake_path = format!("ducklake:{}", catalog_path.display());
        let attach_sql = format!(
            "ATTACH '{}' AS ducklake (DATA_PATH '{}')",
            ducklake_path,
            data_path.display()
        );
        conn.execute(&attach_sql, [])?;

        // Create DataFusion context for READ operations
        let ctx = SessionContext::new();
        Self::register_compat_udfs(&ctx);
        let metadata_provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())?;
        let catalog = Arc::new(DuckLakeCatalog::new(metadata_provider)?);
        ctx.register_catalog("ducklake", catalog);

        Ok(Self {
            duckdb_conn: Arc::new(Mutex::new(conn)),
            datafusion_ctx: Arc::new(Mutex::new(ctx)),
            catalog_path,
            use_ducklake: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            in_transaction: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Register DuckDB-compatible UDFs (year, month, day) in DataFusion context
    fn register_compat_udfs(ctx: &SessionContext) {
        // year(date/timestamp) → date_part('year', input)
        ctx.register_udf(ScalarUDF::new_from_impl(DatePartAliasUdf::new("year")));
        // month(date/timestamp) → date_part('month', input)
        ctx.register_udf(ScalarUDF::new_from_impl(DatePartAliasUdf::new("month")));
        // day(date/timestamp) → date_part('day', input)
        ctx.register_udf(ScalarUDF::new_from_impl(DatePartAliasUdf::new("day")));
    }

    /// Detect if SQL should be routed to DuckDB
    /// Includes WRITE operations, catalog management, and DuckDB-specific statements
    fn is_write_statement(sql: &str) -> bool {
        let trimmed = sql.trim().trim_end_matches(';').trim().to_uppercase();
        trimmed.starts_with("CREATE ")
            || trimmed.starts_with("INSERT ")
            || trimmed.starts_with("UPDATE ")
            || trimmed.starts_with("DELETE ")
            || trimmed.starts_with("DROP ")
            || trimmed.starts_with("ALTER ")
            || trimmed.starts_with("MERGE ")
            || trimmed.starts_with("USE ")
            || trimmed.starts_with("SHOW ")
            || trimmed.starts_with("CALL ")
            || trimmed.starts_with("SET ")
            || trimmed.starts_with("RESET ")
            || trimmed.starts_with("PREPARE ")
            || trimmed.starts_with("EXECUTE ")
            || trimmed.starts_with("DEALLOCATE ")
            || trimmed.starts_with("COPY ")
            || trimmed.starts_with("COMMENT ")
            || trimmed.starts_with("PRAGMA ")
            || trimmed == "BEGIN"
            || trimmed.starts_with("BEGIN ")
            || trimmed == "COMMIT"
            || trimmed.starts_with("COMMIT ")
            || trimmed == "ROLLBACK"
            || trimmed.starts_with("ROLLBACK ")
    }

    /// Rewrite table references from 2-part to 3-part names
    /// ducklake.table → ducklake.main.table
    /// ducklake.schema.table → unchanged (already 3-part)
    ///
    /// String-literal aware: does not rewrite occurrences inside single-quoted strings.
    fn rewrite_table_references(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len() + 100);
        let mut chars = sql.char_indices().peekable();
        let mut last_pos = 0;

        while let Some(&(i, ch)) = chars.peek() {
            if ch == '\'' {
                // Inside a single-quoted string literal — copy verbatim until closing quote
                result.push_str(&sql[last_pos..i]);
                last_pos = i;
                chars.next(); // consume opening quote
                while let Some(&(_, c)) = chars.peek() {
                    chars.next();
                    if c == '\'' {
                        // Check for escaped quote ('')
                        if let Some(&(_, next_c)) = chars.peek() {
                            if next_c == '\'' {
                                chars.next(); // skip escaped quote
                                continue;
                            }
                        }
                        break;
                    }
                }
                // Copy the entire string literal as-is
                let end = chars.peek().map(|&(idx, _)| idx).unwrap_or(sql.len());
                result.push_str(&sql[last_pos..end]);
                last_pos = end;
                continue;
            }

            // Check for "ducklake." at current position
            if sql[i..].starts_with("ducklake.") {
                result.push_str(&sql[last_pos..i]);
                result.push_str("ducklake.");
                let after = &sql[i + 9..];

                if after.starts_with("main.") {
                    // Already has main schema
                    // advance past "ducklake."
                    for _ in 0..9 {
                        chars.next();
                    }
                    last_pos = i + 9;
                } else {
                    let is_three_part = Self::is_three_part_ref(after);
                    if is_three_part {
                        for _ in 0..9 {
                            chars.next();
                        }
                        last_pos = i + 9;
                    } else {
                        result.push_str("main.");
                        for _ in 0..9 {
                            chars.next();
                        }
                        last_pos = i + 9;
                    }
                }
                continue;
            }

            chars.next();
        }
        result.push_str(&sql[last_pos..]);
        result
    }

    /// Check if an identifier appears as a standalone word in a SQL clause.
    /// Prevents matching substrings (e.g., "ROWID" inside "MYROWID").
    fn is_identifier_in_clause(clause: &str, name: &str) -> bool {
        let mut start = 0;
        while let Some(pos) = clause[start..].find(name) {
            let abs_pos = start + pos;
            let before_ok = abs_pos == 0
                || !clause.as_bytes()[abs_pos - 1].is_ascii_alphanumeric()
                    && clause.as_bytes()[abs_pos - 1] != b'_';
            let end_pos = abs_pos + name.len();
            let after_ok = end_pos >= clause.len()
                || !clause.as_bytes()[end_pos].is_ascii_alphanumeric()
                    && clause.as_bytes()[end_pos] != b'_';
            if before_ok && after_ok {
                return true;
            }
            start = abs_pos + 1;
        }
        false
    }

    /// Check if text after "ducklake." is already a 3-part reference (schema.table)
    fn is_three_part_ref(after: &str) -> bool {
        // Extract the first identifier (schema candidate)
        let first_id_end = after.find(|c: char| !c.is_alphanumeric() && c != '_');
        if let Some(end) = first_id_end {
            if end > 0 && after.as_bytes().get(end) == Some(&b'.') {
                // There's a dot after the first identifier
                let after_dot = &after[end + 1..];
                // Check if what follows is an identifier (not empty)
                if !after_dot.is_empty()
                    && after_dot
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_alphabetic() || c == '_')
                {
                    return true;
                }
            }
        }
        false
    }

    /// Refresh catalog snapshot after a write
    fn refresh_catalog(&self) -> Result<(), HybridError> {
        let mut ctx_guard = self.datafusion_ctx.lock().unwrap();

        // Create new session context with fresh catalog
        // If USE ducklake was executed, set default catalog/schema so bare table names resolve
        let new_ctx = if self.use_ducklake.load(std::sync::atomic::Ordering::Relaxed) {
            let config = SessionConfig::new().with_default_catalog_and_schema("ducklake", "main");
            SessionContext::new_with_config(config)
        } else {
            SessionContext::new()
        };
        Self::register_compat_udfs(&new_ctx);
        let metadata_provider = DuckdbMetadataProvider::new(self.catalog_path.to_str().unwrap())?;
        let catalog = Arc::new(DuckLakeCatalog::new(metadata_provider)?);
        new_ctx.register_catalog("ducklake", catalog);

        // Replace the context
        *ctx_guard = new_ctx;

        Ok(())
    }

    /// Execute WRITE via DuckDB, returns changed row count
    fn execute_write(&self, sql: &str) -> Result<usize, HybridError> {
        // Auto-create parent directories for COPY TO statements
        let sql_upper = sql.to_uppercase();
        if sql_upper.contains(" TO '") || sql_upper.contains(" TO \"") {
            if let Some(start) = sql.find(" TO '").or(sql.find(" TO \"")) {
                let quote = sql.as_bytes()[start + 4] as char;
                let path_start = start + 5;
                if let Some(end) = sql[path_start..].find(quote) {
                    let path = &sql[path_start..path_start + end];
                    if let Some(parent) = std::path::Path::new(path).parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                }
            }
        }
        let conn = self.duckdb_conn.lock().unwrap();
        let count = conn.execute(sql, [])?;
        Ok(count)
    }

    /// Virtual columns added by our extension that DuckDB's DuckLake doesn't include in SELECT *
    const EXTENSION_VIRTUAL_COLS: &'static [&'static str] =
        &["filename", "file_row_number", "file_index"];
    /// DuckLake virtual columns that appear in SELECT * from our extension
    /// but not in DuckDB's SELECT *
    const DUCKLAKE_VIRTUAL_COLS: &'static [&'static str] = &["rowid", "snapshot_id"];

    /// Rewrite ORDER BY ALL since DataFusion's parser dialect doesn't support it
    fn rewrite_order_by_all(sql: &str) -> String {
        let upper = sql.to_uppercase();
        if let Some(pos) = upper.find("ORDER BY ALL") {
            let before = &sql[..pos];
            let after = &sql[pos + 12..];
            format!("{}{}", before.trim_end(), after)
        } else {
            sql.to_string()
        }
    }

    /// Execute READ via DataFusion
    async fn execute_read(&self, sql: &str) -> Result<Vec<RecordBatch>, HybridError> {
        let sql_rewritten = Self::rewrite_table_references(sql);
        let sql_rewritten = Self::rewrite_order_by_all(&sql_rewritten);

        // Clone the context to release the lock before await
        let ctx = {
            let ctx_guard = self.datafusion_ctx.lock().unwrap();
            ctx_guard.clone()
        };

        let df = ctx.sql(&sql_rewritten).await?;
        let batches = df.collect().await?;

        // Strip virtual columns from results unless explicitly referenced in SQL
        // This matches DuckDB behavior where virtual columns don't appear in SELECT *
        if batches.is_empty() {
            return Ok(batches);
        }

        let sql_upper = sql.to_uppercase();
        // Extract just the SELECT clause (before FROM) for virtual column matching.
        // This prevents matching column names in WHERE clauses or string literals.
        let select_clause = if let Some(from_pos) = sql_upper.find(" FROM ") {
            &sql_upper[..from_pos]
        } else {
            &sql_upper
        };
        let schema = batches[0].schema();
        let keep_indices: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                let name = f.name().as_str();
                // Always strip extension-specific virtual columns unless explicitly referenced in SELECT
                if Self::EXTENSION_VIRTUAL_COLS.contains(&name) {
                    return Self::is_identifier_in_clause(select_clause, &name.to_uppercase());
                }
                // Strip DuckLake virtual columns (rowid, snapshot_id) unless explicitly referenced in SELECT
                if Self::DUCKLAKE_VIRTUAL_COLS.contains(&name) {
                    return Self::is_identifier_in_clause(select_clause, &name.to_uppercase());
                }
                true
            })
            .map(|(i, _)| i)
            .collect();

        // If all columns are kept, no need to project
        if keep_indices.len() == schema.fields().len() {
            return Ok(batches);
        }

        let projected: Result<Vec<RecordBatch>, _> = batches
            .into_iter()
            .map(|batch| {
                batch
                    .project(&keep_indices)
                    .map_err(|e| HybridError(format!("Projection error: {}", e)))
            })
            .collect();
        projected
    }
}

#[async_trait::async_trait]
impl AsyncDB for HybridDuckLakeDB {
    type Error = HybridError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        if Self::is_write_statement(sql) {
            // Track USE ducklake for default catalog/schema resolution
            let trimmed_upper = sql.trim().to_uppercase();
            if trimmed_upper.starts_with("USE ") {
                if trimmed_upper.contains("DUCKLAKE") {
                    self.use_ducklake
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                } else {
                    self.use_ducklake
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                }
            }

            // Track transaction state
            if trimmed_upper == "BEGIN" || trimmed_upper.starts_with("BEGIN ") {
                self.in_transaction
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            } else if trimmed_upper == "COMMIT"
                || trimmed_upper.starts_with("COMMIT ")
                || trimmed_upper == "ROLLBACK"
                || trimmed_upper.starts_with("ROLLBACK ")
            {
                self.in_transaction
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }

            // DuckDB path: WRITE operations and catalog management
            // Includes: CREATE, INSERT, UPDATE, DELETE, USE, SHOW, CALL, etc.
            let changed_rows = self.execute_write(sql)?;

            // Refresh catalog to pick up changes (skip during transactions - data not committed yet)
            if !self
                .in_transaction
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                self.refresh_catalog()?;
            }

            // Return row count for DML statements (INSERT/UPDATE/DELETE)
            // The sqllogictest framework uses this for `query I` blocks that test row counts
            if trimmed_upper.starts_with("INSERT ")
                || trimmed_upper.starts_with("UPDATE ")
                || trimmed_upper.starts_with("DELETE ")
            {
                Ok(DBOutput::Rows {
                    types: vec![DefaultColumnType::Integer],
                    rows: vec![vec![changed_rows.to_string()]],
                })
            } else {
                Ok(DBOutput::StatementComplete(changed_rows as u64))
            }
        } else if self
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            // Inside a transaction: route reads to DuckDB since DataFusion can't see
            // uncommitted data (it uses a separate read-only connection)
            let conn = self.duckdb_conn.lock().unwrap();
            let mut stmt = conn.prepare(sql).map_err(HybridError::from)?;

            // Use query() to execute and get result rows
            let mut duckdb_rows = stmt.query([]).map_err(HybridError::from)?;

            // Get column count from the result
            let column_count = duckdb_rows.as_ref().map(|r| r.column_count()).unwrap_or(0);
            if column_count == 0 {
                return Ok(DBOutput::StatementComplete(0));
            }

            let types = (0..column_count)
                .map(|_| DefaultColumnType::Any)
                .collect::<Vec<_>>();

            let mut rows = Vec::new();
            while let Some(row) = duckdb_rows.next().map_err(HybridError::from)? {
                let mut vals = Vec::new();
                for i in 0..column_count {
                    let val: String = match row.get::<_, duckdb::types::Value>(i) {
                        Ok(duckdb::types::Value::Null) => "NULL".to_string(),
                        Ok(duckdb::types::Value::Boolean(v)) => v.to_string(),
                        Ok(duckdb::types::Value::TinyInt(v)) => v.to_string(),
                        Ok(duckdb::types::Value::SmallInt(v)) => v.to_string(),
                        Ok(duckdb::types::Value::Int(v)) => v.to_string(),
                        Ok(duckdb::types::Value::BigInt(v)) => v.to_string(),
                        Ok(duckdb::types::Value::HugeInt(v)) => v.to_string(),
                        Ok(duckdb::types::Value::UTinyInt(v)) => v.to_string(),
                        Ok(duckdb::types::Value::USmallInt(v)) => v.to_string(),
                        Ok(duckdb::types::Value::UInt(v)) => v.to_string(),
                        Ok(duckdb::types::Value::UBigInt(v)) => v.to_string(),
                        Ok(duckdb::types::Value::Float(v)) => v.to_string(),
                        Ok(duckdb::types::Value::Double(v)) => v.to_string(),
                        Ok(duckdb::types::Value::Text(v)) => v,
                        Ok(other) => format!("{:?}", other),
                        Err(e) => {
                            return Err(HybridError(format!(
                                "DuckDB column {i} decode error: {e}"
                            )));
                        },
                    };
                    vals.push(val);
                }
                rows.push(vals);
            }

            Ok(DBOutput::Rows {
                types,
                rows,
            })
        } else {
            // DataFusion path: READ operations (SELECT, etc.)
            let batches = self.execute_read(sql).await?;

            if batches.is_empty() || batches.iter().all(|b| b.num_columns() == 0) {
                return Ok(DBOutput::StatementComplete(0));
            }

            // Convert to sqllogictest format
            let schema = batches[0].schema();
            let types = schema
                .fields()
                .iter()
                .map(|f| match f.data_type() {
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64 => DefaultColumnType::Integer,
                    DataType::Float32 | DataType::Float64 => DefaultColumnType::FloatingPoint,
                    DataType::Utf8 | DataType::LargeUtf8 => DefaultColumnType::Text,
                    _ => DefaultColumnType::Any,
                })
                .collect::<Vec<_>>();

            let mut rows = Vec::new();
            for batch in batches {
                rows.extend(convert_batch_to_strings(&batch)?);
            }

            Ok(DBOutput::Rows {
                types,
                rows,
            })
        }
    }

    fn engine_name(&self) -> &str {
        "HybridDuckLake(DuckDB+DataFusion)"
    }
}

/// Format a float value to match DuckDB display conventions.
/// DuckDB always shows at least one decimal place for float/double values,
/// and displays NaN as "nan" and Infinity as "inf".
fn format_float(v: f64) -> String {
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    let s = v.to_string();
    if !s.contains('.') {
        format!("{}.0", s)
    } else {
        s
    }
}

/// Convert RecordBatch to string rows for sqllogictest
fn convert_batch_to_strings(batch: &RecordBatch) -> Result<Vec<Vec<String>>, HybridError> {
    let mut rows = Vec::new();

    for row_idx in 0..batch.num_rows() {
        let mut row = Vec::new();
        for col_idx in 0..batch.num_columns() {
            let column = batch.column(col_idx);
            let value = if column.is_null(row_idx) {
                "NULL".to_string()
            } else {
                match column.data_type() {
                    DataType::Int8 => {
                        let arr = column.as_any().downcast_ref::<Int8Array>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::Int16 => {
                        let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::Int32 => {
                        let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::Int64 => {
                        let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::UInt8 => {
                        let arr = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::UInt16 => {
                        let arr = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::UInt32 => {
                        let arr = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::UInt64 => {
                        let arr = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::Float32 => {
                        let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
                        format_float(arr.value(row_idx) as f64)
                    },
                    DataType::Float64 => {
                        let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        format_float(arr.value(row_idx))
                    },
                    DataType::Utf8 => {
                        let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::LargeUtf8 => {
                        let arr = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::Boolean => {
                        let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        arr.value(row_idx).to_string()
                    },
                    DataType::Date32 => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .expect("Date32 downcast");
                        arr.value_as_date(row_idx)
                            .expect("Date32 value_as_date")
                            .to_string()
                    },
                    DataType::Timestamp(unit, _) => {
                        use datafusion::arrow::datatypes::TimeUnit;
                        // Use consistent format with test_utils.rs: %Y-%m-%d %H:%M:%S
                        // (truncates sub-seconds to match DuckDB display format)
                        match unit {
                            TimeUnit::Second => {
                                let arr = column
                                    .as_any()
                                    .downcast_ref::<TimestampSecondArray>()
                                    .unwrap();
                                let s = arr.value(row_idx);
                                let dt = chrono::DateTime::from_timestamp(s, 0).unwrap();
                                dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            },
                            TimeUnit::Millisecond => {
                                let arr = column
                                    .as_any()
                                    .downcast_ref::<TimestampMillisecondArray>()
                                    .unwrap();
                                let ms = arr.value(row_idx);
                                let secs = ms.div_euclid(1_000);
                                let subsec_ms = ms.rem_euclid(1_000) as u32;
                                let dt =
                                    chrono::DateTime::from_timestamp(secs, subsec_ms * 1_000_000)
                                        .unwrap();
                                dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            },
                            TimeUnit::Microsecond => {
                                let arr = column
                                    .as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>()
                                    .unwrap();
                                let us = arr.value(row_idx);
                                let secs = us.div_euclid(1_000_000);
                                let subsec_us = us.rem_euclid(1_000_000) as u32;
                                let dt = chrono::DateTime::from_timestamp(secs, subsec_us * 1_000)
                                    .unwrap();
                                dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            },
                            TimeUnit::Nanosecond => {
                                let arr = column
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()
                                    .unwrap();
                                let ns = arr.value(row_idx);
                                let secs = ns.div_euclid(1_000_000_000);
                                let subsec_ns = ns.rem_euclid(1_000_000_000) as u32;
                                let dt = chrono::DateTime::from_timestamp(secs, subsec_ns).unwrap();
                                dt.format("%Y-%m-%d %H:%M:%S").to_string()
                            },
                        }
                    },
                    DataType::Decimal128(_, scale) => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .expect("Decimal128 downcast");
                        let raw = arr.value(row_idx);
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
                    DataType::Binary => {
                        let arr = column.as_any().downcast_ref::<BinaryArray>().unwrap();
                        let bytes = arr.value(row_idx);
                        // Format as hex string
                        bytes
                            .iter()
                            .map(|b| format!("{:02X}", b))
                            .collect::<String>()
                    },
                    other => {
                        panic!(
                            "convert_batch_to_strings: unsupported Arrow type {:?} at col {} row {}",
                            other, col_idx, row_idx
                        );
                    },
                }
            };
            row.push(value);
        }
        rows.push(row);
    }

    Ok(rows)
}

/// UDF that aliases date_part for DuckDB compatibility (year, month, day)
#[derive(Debug)]
struct DatePartAliasUdf {
    name: String,
    part_name: String,
    signature: datafusion::logical_expr::Signature,
}

impl std::hash::Hash for DatePartAliasUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for DatePartAliasUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for DatePartAliasUdf {}

impl DatePartAliasUdf {
    fn new(part: &str) -> Self {
        Self {
            name: part.to_string(),
            part_name: part.to_string(),
            signature: datafusion::logical_expr::Signature::new(
                datafusion::logical_expr::TypeSignature::Any(1),
                Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for DatePartAliasUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        // date_part returns Int32 in DataFusion
        Ok(DataType::Int32)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        use datafusion::physical_plan::ColumnarValue;
        // Create the part name as a scalar
        let part = ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
            self.part_name.clone(),
        )));
        // Call the built-in date_part function
        let date_part_udf = datafusion::functions::datetime::date_part();
        let new_args = datafusion::logical_expr::ScalarFunctionArgs {
            args: vec![
                part,
                args.args
                    .into_iter()
                    .next()
                    .expect("date_part requires at least one argument"),
            ],
            ..args
        };
        date_part_udf.invoke_with_args(new_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_detection() {
        // Write operations - routed to DuckDB
        assert!(HybridDuckLakeDB::is_write_statement(
            "CREATE TABLE foo (id INT)"
        ));
        assert!(HybridDuckLakeDB::is_write_statement(
            "INSERT INTO foo VALUES (1)"
        ));
        assert!(HybridDuckLakeDB::is_write_statement(
            "UPDATE foo SET id = 2"
        ));
        assert!(HybridDuckLakeDB::is_write_statement("DELETE FROM foo"));
        assert!(HybridDuckLakeDB::is_write_statement("DROP TABLE foo"));
        assert!(HybridDuckLakeDB::is_write_statement(
            "ALTER TABLE foo ADD COLUMN bar INT"
        ));

        // Transaction control - routed to DuckDB
        assert!(HybridDuckLakeDB::is_write_statement("BEGIN"));
        assert!(HybridDuckLakeDB::is_write_statement("COMMIT"));
        assert!(HybridDuckLakeDB::is_write_statement("ROLLBACK"));

        // Catalog management - routed to DuckDB
        assert!(HybridDuckLakeDB::is_write_statement("USE ducklake"));
        assert!(HybridDuckLakeDB::is_write_statement("SHOW TABLES"));
        assert!(HybridDuckLakeDB::is_write_statement(
            "CALL some_procedure()"
        ));

        // Read operations - routed to DataFusion
        assert!(!HybridDuckLakeDB::is_write_statement("SELECT * FROM foo"));
        assert!(!HybridDuckLakeDB::is_write_statement(
            "WITH cte AS (...) SELECT ..."
        ));
    }

    #[test]
    fn test_table_rewrite() {
        // 2-part → 3-part: ducklake.test → ducklake.main.test
        assert_eq!(
            HybridDuckLakeDB::rewrite_table_references("SELECT * FROM ducklake.test"),
            "SELECT * FROM ducklake.main.test"
        );

        assert_eq!(
            HybridDuckLakeDB::rewrite_table_references("INSERT INTO ducklake.test VALUES (1)"),
            "INSERT INTO ducklake.main.test VALUES (1)"
        );

        // Avoid double-rewrite
        assert_eq!(
            HybridDuckLakeDB::rewrite_table_references("SELECT * FROM ducklake.main.test"),
            "SELECT * FROM ducklake.main.test"
        );

        // Preserve 3-part names (ducklake.schema.table)
        assert_eq!(
            HybridDuckLakeDB::rewrite_table_references("SELECT * FROM ducklake.s1.v1"),
            "SELECT * FROM ducklake.s1.v1",
        );
    }

    #[test]
    fn test_is_write_statement_basic() {
        assert!(HybridDuckLakeDB::is_write_statement(
            "CREATE TABLE t (x INT)"
        ));
        assert!(HybridDuckLakeDB::is_write_statement(
            "INSERT INTO t VALUES (1)"
        ));
        assert!(HybridDuckLakeDB::is_write_statement("UPDATE t SET x = 1"));
        assert!(HybridDuckLakeDB::is_write_statement("DELETE FROM t"));
        assert!(HybridDuckLakeDB::is_write_statement("DROP TABLE t"));
        assert!(HybridDuckLakeDB::is_write_statement(
            "ALTER TABLE t ADD COLUMN y INT"
        ));
        assert!(HybridDuckLakeDB::is_write_statement(
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = 1"
        ));
        assert!(HybridDuckLakeDB::is_write_statement("BEGIN"));
        assert!(HybridDuckLakeDB::is_write_statement("COMMIT"));
        assert!(HybridDuckLakeDB::is_write_statement("ROLLBACK"));
    }

    #[test]
    fn test_is_write_statement_read_only() {
        assert!(!HybridDuckLakeDB::is_write_statement("SELECT * FROM t"));
        assert!(!HybridDuckLakeDB::is_write_statement("SELECT 1"));
        assert!(!HybridDuckLakeDB::is_write_statement(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ));
    }

    #[test]
    fn test_is_write_statement_mixed_case() {
        assert!(HybridDuckLakeDB::is_write_statement(
            "create table t (x int)"
        ));
        assert!(HybridDuckLakeDB::is_write_statement(
            "Insert Into t VALUES (1)"
        ));
        assert!(HybridDuckLakeDB::is_write_statement("uPdAtE t SET x = 1"));
        assert!(HybridDuckLakeDB::is_write_statement("Delete FROM t"));
    }

    #[test]
    fn test_is_write_statement_leading_whitespace() {
        assert!(HybridDuckLakeDB::is_write_statement(
            "  CREATE TABLE t (x INT)"
        ));
        assert!(HybridDuckLakeDB::is_write_statement(
            "\tINSERT INTO t VALUES (1)"
        ));
        assert!(HybridDuckLakeDB::is_write_statement(
            "\n  UPDATE t SET x = 1"
        ));
    }

    #[test]
    fn test_is_write_statement_trailing_semicolons() {
        assert!(HybridDuckLakeDB::is_write_statement(
            "CREATE TABLE t (x INT);"
        ));
        assert!(HybridDuckLakeDB::is_write_statement(
            "INSERT INTO t VALUES (1) ;"
        ));
        assert!(HybridDuckLakeDB::is_write_statement("BEGIN;"));
        assert!(HybridDuckLakeDB::is_write_statement("COMMIT ;"));
    }

    #[test]
    fn test_is_write_statement_ctes_not_writes() {
        // WITH...SELECT should NOT be detected as write
        assert!(!HybridDuckLakeDB::is_write_statement(
            "WITH x AS (SELECT 1) SELECT * FROM x"
        ));
    }

    #[test]
    fn test_is_write_statement_additional_keywords() {
        assert!(HybridDuckLakeDB::is_write_statement("USE ducklake"));
        assert!(HybridDuckLakeDB::is_write_statement(
            "SET search_path = main"
        ));
        assert!(HybridDuckLakeDB::is_write_statement("COPY t TO 'file.csv'"));
        assert!(HybridDuckLakeDB::is_write_statement(
            "PREPARE stmt AS SELECT 1"
        ));
        assert!(HybridDuckLakeDB::is_write_statement("EXECUTE stmt"));
    }

    #[test]
    fn test_transaction_state_tracking() {
        // Verify that in_transaction flag is toggled by BEGIN/COMMIT/ROLLBACK
        let temp_dir = tempfile::TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("test_tx.ducklake");
        let db = HybridDuckLakeDB::new(catalog_path).unwrap();

        assert!(
            !db.in_transaction.load(std::sync::atomic::Ordering::Relaxed),
            "Should not be in transaction initially"
        );
    }

    #[test]
    fn test_rewrite_preserves_string_literals() {
        // R5-S-033: rewrite_table_references should NOT rewrite inside string literals
        let result = HybridDuckLakeDB::rewrite_table_references(
            "SELECT * FROM ducklake.t WHERE col = 'ducklake.value'",
        );
        assert!(
            result.contains("ducklake.main.t"),
            "Table reference should be rewritten: {result}"
        );
        assert!(
            result.contains("'ducklake.value'"),
            "String literal should NOT be rewritten: {result}"
        );
    }

    #[test]
    fn test_virtual_column_identifier_matching() {
        // R5-S-041: should match as standalone identifier, not substring
        assert!(HybridDuckLakeDB::is_identifier_in_clause(
            "SELECT ROWID, COL",
            "ROWID"
        ));
        assert!(!HybridDuckLakeDB::is_identifier_in_clause(
            "SELECT MYROWID, COL",
            "ROWID"
        ));
        assert!(HybridDuckLakeDB::is_identifier_in_clause(
            "SELECT ROWID",
            "ROWID"
        ));
        assert!(!HybridDuckLakeDB::is_identifier_in_clause(
            "SELECT _ROWID_",
            "ROWID"
        ));
    }

    #[test]
    fn test_decimal128_negative_sign() {
        // R5-S-010: verify negative decimals in (-1, 0) range preserve sign
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![Field::new("d", DataType::Decimal128(10, 2), true)]);
        let arr = Decimal128Array::from(vec![
            Some(-45i128),
            Some(45i128),
            Some(-100i128),
            Some(0i128),
        ]);
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![std::sync::Arc::new(arr.with_precision_and_scale(10, 2).unwrap())],
        )
        .unwrap();
        let rows = convert_batch_to_strings(&batch).unwrap();
        assert_eq!(
            rows[0][0], "-0.45",
            "Negative sub-unit should have negative sign"
        );
        assert_eq!(
            rows[1][0], "0.45",
            "Positive sub-unit should not have negative sign"
        );
        assert_eq!(
            rows[2][0], "-1.00",
            "Negative whole should have negative sign"
        );
        assert_eq!(rows[3][0], "0.00", "Zero should not have negative sign");
    }
}
