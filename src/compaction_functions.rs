//! Compaction table functions for DuckLake catalog maintenance.
//!
//! These functions delegate to DuckDB's native ducklake compaction functions,
//! providing DataFusion table function wrappers for:
//!
//! - `ducklake_merge_adjacent_files()` — merge small adjacent files into larger ones
//! - `ducklake_rewrite_data_files(table, delete_threshold)` — rewrite files exceeding a delete threshold
//! - `ducklake_expire_snapshots(older_than)` — remove old snapshot metadata
//! - `ducklake_cleanup_old_files()` — remove files no longer referenced after snapshot expiration
//! - `ducklake_delete_orphaned_files(dry_run)` — remove orphaned/uncommitted files
//!
//! Execution is deferred to scan time (R6-S-002): `call()` validates arguments and
//! builds the SQL string, but the actual DuckDB connection and query execution
//! happen only when the query is executed (not during EXPLAIN).
//!
//! Requires the `metadata-duckdb` feature.

#![cfg(feature = "metadata-duckdb")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{Result as DataFusionResult, ScalarValue, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;

// ==================== Constants ====================

/// Default `older_than` timestamp for operations that need a far-future cutoff.
/// Used as default when no explicit `older_than` is provided (R6-S-052).
const DEFAULT_OLDER_THAN: &str = "2099-01-01";

/// Global flag to track whether `INSTALL ducklake` has succeeded (R6-S-026, R7-S-001).
/// Uses `AtomicBool` for lock-free reads on the fast path. Retries on failure
/// (unlike `Once` which would permanently cache the failure).
static DUCKLAKE_INSTALLED: AtomicBool = AtomicBool::new(false);

// ==================== Schemas ====================

/// Return schema for merge_adjacent_files and rewrite_data_files: `(Success: Boolean)`
fn success_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "Success",
        DataType::Boolean,
        true,
    )]))
}

/// Return schema for expire_snapshots:
/// `(snapshot_id: Int64, snapshot_time: Utf8, schema_version: Int64, changes: Utf8, author: Utf8, commit_message: Utf8, commit_extra_info: Utf8)`
fn expire_snapshots_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("snapshot_id", DataType::Int64, true),
        Field::new("snapshot_time", DataType::Utf8, true),
        Field::new("schema_version", DataType::Int64, true),
        Field::new("changes", DataType::Utf8, true),
        Field::new("author", DataType::Utf8, true),
        Field::new("commit_message", DataType::Utf8, true),
        Field::new("commit_extra_info", DataType::Utf8, true),
    ]))
}

/// Return schema for cleanup_old_files and delete_orphaned_files: `(path: Utf8)`
fn path_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("path", DataType::Utf8, true)]))
}

// ==================== Helper: DuckDB connection ====================

/// Open a temporary DuckDB connection with the ducklake extension and ATTACH the catalog.
/// Returns the connection with the catalog attached as `__compaction`.
///
/// Note: Creates a fresh connection per call. Since compaction is an infrequent
/// maintenance operation, this overhead is acceptable and avoids connection caching
/// complexity.
fn open_compaction_connection(catalog_path: &str) -> DataFusionResult<duckdb::Connection> {
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    // Only INSTALL once per process — the extension is cached on disk (R6-S-026).
    // INSTALL is idempotent but has overhead; LOAD is still needed per-connection.
    // Uses AtomicBool so failures can be retried on subsequent calls (R7-S-001).
    if !DUCKLAKE_INSTALLED.load(Ordering::Acquire) {
        conn.execute("INSTALL ducklake;", [])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        DUCKLAKE_INSTALLED.store(true, Ordering::Release);
    }
    conn.execute("LOAD ducklake;", [])
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let attach_sql = format!(
        "ATTACH 'ducklake:{}' AS __compaction;",
        catalog_path.replace('\'', "''")
    );
    conn.execute(&attach_sql, [])
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    Ok(conn)
}

/// Helper to map a DuckDB error to a DataFusionError.
fn duckdb_err(e: duckdb::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(e))
}

// ==================== Deferred execution provider (R6-S-002) ====================

/// Result collection strategy for deferred compaction queries.
#[derive(Debug, Clone)]
enum CompactionResultType {
    /// Single boolean column (e.g., Success).
    BooleanSuccess,
    /// All columns are nullable strings.
    AllStrings,
    /// Expire snapshots: Int64, String, Int64, String×4 (7 columns).
    ExpireSnapshots,
}

/// A TableProvider that defers DuckDB query execution to scan time.
///
/// `call()` on compaction UDTFs validates arguments and builds the SQL string,
/// then returns this provider. Actual DuckDB connection opening and query execution
/// happen only in `scan()`, preventing side effects during EXPLAIN (R6-S-002).
#[derive(Debug)]
struct DeferredCompactionProvider {
    catalog_path: String,
    sql: String,
    schema: SchemaRef,
    result_type: CompactionResultType,
}

impl DeferredCompactionProvider {
    fn new(
        catalog_path: String,
        sql: String,
        schema: SchemaRef,
        result_type: CompactionResultType,
    ) -> Arc<dyn TableProvider> {
        Arc::new(Self {
            catalog_path,
            sql,
            schema,
            result_type,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for DeferredCompactionProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        // Run synchronous DuckDB operations on a blocking thread to avoid
        // blocking the async executor (R8-S-040).
        let catalog_path = self.catalog_path.clone();
        let sql = self.sql.clone();
        let schema = self.schema.clone();
        let result_type = self.result_type.clone();
        let batch = tokio::task::spawn_blocking(move || {
            let conn = open_compaction_connection(&catalog_path)?;
            match &result_type {
                CompactionResultType::BooleanSuccess => collect_boolean_batch(&conn, &sql, &schema),
                CompactionResultType::AllStrings => collect_string_batch(&conn, &sql, &schema),
                CompactionResultType::ExpireSnapshots => {
                    collect_expire_snapshots_batch(&conn, &sql, &schema)
                },
            }
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))??;
        let mem = datafusion::datasource::memory::MemTable::try_new(
            self.schema.clone(),
            vec![vec![batch]],
        )?;
        mem.scan(state, projection, filters, limit).await
    }
}

// ==================== Batch collection helpers ====================

/// Execute a DuckDB query that returns `(Success: Boolean)` and collect into a RecordBatch.
fn collect_boolean_batch(
    conn: &duckdb::Connection,
    sql: &str,
    schema: &SchemaRef,
) -> DataFusionResult<RecordBatch> {
    let mut stmt = conn.prepare(sql).map_err(duckdb_err)?;
    let mut rows = stmt.query([]).map_err(duckdb_err)?;
    let mut values: Vec<bool> = Vec::new();
    while let Some(row) = rows.next().map_err(duckdb_err)? {
        values.push(row.get(0).map_err(duckdb_err)?);
    }
    if values.is_empty() {
        Ok(RecordBatch::new_empty(schema.clone()))
    } else {
        let arr: ArrayRef = Arc::new(BooleanArray::from(values));
        RecordBatch::try_new(schema.clone(), vec![arr])
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Execute a DuckDB query where all columns are nullable strings and collect into a RecordBatch.
fn collect_string_batch(
    conn: &duckdb::Connection,
    sql: &str,
    schema: &SchemaRef,
) -> DataFusionResult<RecordBatch> {
    let num_cols = schema.fields().len();
    let mut stmt = conn.prepare(sql).map_err(duckdb_err)?;
    let mut rows = stmt.query([]).map_err(duckdb_err)?;
    let mut columns: Vec<Vec<Option<String>>> = (0..num_cols).map(|_| Vec::new()).collect();
    while let Some(row) = rows.next().map_err(duckdb_err)? {
        for (i, col) in columns.iter_mut().enumerate() {
            col.push(row.get::<_, Option<String>>(i).map_err(duckdb_err)?);
        }
    }
    let first_col = columns.first().ok_or_else(|| {
        datafusion::error::DataFusionError::Internal(
            "collect_string_batch: schema has zero columns".to_string(),
        )
    })?;
    if first_col.is_empty() {
        Ok(RecordBatch::new_empty(schema.clone()))
    } else {
        let arrays: Vec<ArrayRef> = columns
            .into_iter()
            .map(|col| Arc::new(StringArray::from(col)) as ArrayRef)
            .collect();
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Execute the expire_snapshots DuckDB query (mixed Int64/String columns) into a RecordBatch.
fn collect_expire_snapshots_batch(
    conn: &duckdb::Connection,
    sql: &str,
    schema: &SchemaRef,
) -> DataFusionResult<RecordBatch> {
    let mut stmt = conn.prepare(sql).map_err(duckdb_err)?;
    let mut rows = stmt.query([]).map_err(duckdb_err)?;
    let mut col_snapshot_id: Vec<Option<i64>> = Vec::new();
    let mut col_snapshot_time: Vec<Option<String>> = Vec::new();
    let mut col_schema_version: Vec<Option<i64>> = Vec::new();
    let mut col_changes: Vec<Option<String>> = Vec::new();
    let mut col_author: Vec<Option<String>> = Vec::new();
    let mut col_commit_message: Vec<Option<String>> = Vec::new();
    let mut col_commit_extra_info: Vec<Option<String>> = Vec::new();

    while let Some(row) = rows.next().map_err(duckdb_err)? {
        col_snapshot_id.push(row.get::<_, Option<i64>>(0).map_err(duckdb_err)?);
        col_snapshot_time.push(row.get::<_, Option<String>>(1).map_err(duckdb_err)?);
        col_schema_version.push(row.get::<_, Option<i64>>(2).map_err(duckdb_err)?);
        col_changes.push(row.get::<_, Option<String>>(3).map_err(duckdb_err)?);
        col_author.push(row.get::<_, Option<String>>(4).map_err(duckdb_err)?);
        col_commit_message.push(row.get::<_, Option<String>>(5).map_err(duckdb_err)?);
        col_commit_extra_info.push(row.get::<_, Option<String>>(6).map_err(duckdb_err)?);
    }

    if col_snapshot_id.is_empty() {
        Ok(RecordBatch::new_empty(schema.clone()))
    } else {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(col_snapshot_id)),
            Arc::new(StringArray::from(col_snapshot_time)),
            Arc::new(Int64Array::from(col_schema_version)),
            Arc::new(StringArray::from(col_changes)),
            Arc::new(StringArray::from(col_author)),
            Arc::new(StringArray::from(col_commit_message)),
            Arc::new(StringArray::from(col_commit_extra_info)),
        ];
        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

// ==================== ducklake_merge_adjacent_files ====================

/// Merges small adjacent Parquet data files into larger ones.
///
/// Usage:
///   `SELECT * FROM ducklake_merge_adjacent_files()`  — merge all tables
///   `SELECT * FROM ducklake_merge_adjacent_files('table_name')`  — merge a specific table
///
/// Returns: `(Success: Boolean)` — typically 0 rows.
#[derive(Debug)]
pub struct DucklakeMergeAdjacentFilesFunction {
    catalog_path: String,
}

impl DucklakeMergeAdjacentFilesFunction {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
        }
    }
}

impl TableFunctionImpl for DucklakeMergeAdjacentFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let sql = match exprs.len() {
            0 => "SELECT * FROM ducklake_merge_adjacent_files('__compaction')".to_string(),
            1 => {
                let table_name = extract_string_arg(&exprs[0], "ducklake_merge_adjacent_files", 1)?;
                format!(
                    "SELECT * FROM ducklake_merge_adjacent_files('__compaction', '{}')",
                    table_name.replace('\'', "''")
                )
            },
            _ => {
                return plan_err!(
                    "ducklake_merge_adjacent_files() takes 0 or 1 arguments (optional table_name)"
                );
            },
        };
        Ok(DeferredCompactionProvider::new(
            self.catalog_path.clone(),
            sql,
            success_schema(),
            CompactionResultType::BooleanSuccess,
        ))
    }
}

// ==================== ducklake_rewrite_data_files ====================

/// Rewrites data files that exceed a delete threshold, removing delete files.
///
/// `delete_threshold` is a float between 0.0 and 1.0 (0.0 = rewrite all files with any deletes).
#[derive(Debug)]
pub struct DucklakeRewriteDataFilesFunction {
    catalog_path: String,
}

impl DucklakeRewriteDataFilesFunction {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
        }
    }
}

impl TableFunctionImpl for DucklakeRewriteDataFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let sql = match exprs.len() {
            1 => {
                let threshold = extract_float_arg(&exprs[0], "ducklake_rewrite_data_files", 1)?;
                if threshold < 0.0 || threshold > 1.0 {
                    return plan_err!(
                        "delete_threshold must be between 0.0 and 1.0, got {}",
                        threshold
                    );
                }
                format!(
                    "SELECT * FROM ducklake_rewrite_data_files('__compaction', delete_threshold := {})",
                    threshold
                )
            },
            2 => {
                let table_name = extract_string_arg(&exprs[0], "ducklake_rewrite_data_files", 1)?;
                let threshold = extract_float_arg(&exprs[1], "ducklake_rewrite_data_files", 2)?;
                if threshold < 0.0 || threshold > 1.0 {
                    return plan_err!(
                        "delete_threshold must be between 0.0 and 1.0, got {}",
                        threshold
                    );
                }
                format!(
                    "SELECT * FROM ducklake_rewrite_data_files('__compaction', '{}', delete_threshold := {})",
                    table_name.replace('\'', "''"),
                    threshold
                )
            },
            _ => {
                return plan_err!(
                    "ducklake_rewrite_data_files() requires 1-2 arguments: (optional table_name, delete_threshold)"
                );
            },
        };
        Ok(DeferredCompactionProvider::new(
            self.catalog_path.clone(),
            sql,
            success_schema(),
            CompactionResultType::BooleanSuccess,
        ))
    }
}

// ==================== ducklake_expire_snapshots ====================

#[derive(Debug)]
pub struct DucklakeExpireSnapshotsFunction {
    catalog_path: String,
}

impl DucklakeExpireSnapshotsFunction {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
        }
    }
}

impl TableFunctionImpl for DucklakeExpireSnapshotsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() != 1 {
            return plan_err!(
                "ducklake_expire_snapshots() requires 1 argument: (older_than_timestamp)"
            );
        }
        let older_than = extract_string_arg(&exprs[0], "ducklake_expire_snapshots", 1)?;
        let sql = format!(
            "SELECT snapshot_id, snapshot_time::VARCHAR as snapshot_time, schema_version, \
             changes::VARCHAR as changes, author, commit_message, commit_extra_info \
             FROM ducklake_expire_snapshots('__compaction', older_than := '{}'::TIMESTAMPTZ)",
            older_than.replace('\'', "''")
        );
        Ok(DeferredCompactionProvider::new(
            self.catalog_path.clone(),
            sql,
            expire_snapshots_schema(),
            CompactionResultType::ExpireSnapshots,
        ))
    }
}

// ==================== ducklake_cleanup_old_files ====================

#[derive(Debug)]
pub struct DucklakeCleanupOldFilesFunction {
    catalog_path: String,
}

impl DucklakeCleanupOldFilesFunction {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
        }
    }
}

impl TableFunctionImpl for DucklakeCleanupOldFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let sql = match exprs.len() {
            0 => {
                format!(
                    "SELECT * FROM ducklake_cleanup_old_files('__compaction', older_than := '{}'::TIMESTAMP)",
                    DEFAULT_OLDER_THAN
                )
            },
            1 => {
                let older_than = extract_string_arg(&exprs[0], "ducklake_cleanup_old_files", 1)?;
                format!(
                    "SELECT * FROM ducklake_cleanup_old_files('__compaction', older_than := '{}'::TIMESTAMP)",
                    older_than.replace('\'', "''")
                )
            },
            _ => {
                return plan_err!(
                    "ducklake_cleanup_old_files() takes 0 or 1 arguments (optional older_than_timestamp)"
                );
            },
        };
        Ok(DeferredCompactionProvider::new(
            self.catalog_path.clone(),
            sql,
            path_schema(),
            CompactionResultType::AllStrings,
        ))
    }
}

// ==================== ducklake_delete_orphaned_files ====================

/// Removes orphaned files that are not referenced by any snapshot.
///
/// Usage:
///   `SELECT * FROM ducklake_delete_orphaned_files()`  — delete orphaned files
///   `SELECT * FROM ducklake_delete_orphaned_files(true)`  — dry run
///   `SELECT * FROM ducklake_delete_orphaned_files(true, '2024-01-01')`  — dry run with custom older_than (R6-S-052)
#[derive(Debug)]
pub struct DucklakeDeleteOrphanedFilesFunction {
    catalog_path: String,
}

impl DucklakeDeleteOrphanedFilesFunction {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
        }
    }
}

impl TableFunctionImpl for DucklakeDeleteOrphanedFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let sql = match exprs.len() {
            0 => {
                format!(
                    "SELECT * FROM ducklake_delete_orphaned_files('__compaction', older_than := '{}'::TIMESTAMP)",
                    DEFAULT_OLDER_THAN
                )
            },
            1 => {
                let dry_run = extract_bool_arg(&exprs[0], "ducklake_delete_orphaned_files", 1)?;
                format!(
                    "SELECT * FROM ducklake_delete_orphaned_files('__compaction', dry_run := {}, older_than := '{}'::TIMESTAMP)",
                    dry_run, DEFAULT_OLDER_THAN
                )
            },
            2 => {
                let dry_run = extract_bool_arg(&exprs[0], "ducklake_delete_orphaned_files", 1)?;
                let older_than =
                    extract_string_arg(&exprs[1], "ducklake_delete_orphaned_files", 2)?;
                format!(
                    "SELECT * FROM ducklake_delete_orphaned_files('__compaction', dry_run := {}, older_than := '{}'::TIMESTAMP)",
                    dry_run,
                    older_than.replace('\'', "''")
                )
            },
            _ => {
                return plan_err!(
                    "ducklake_delete_orphaned_files() takes 0-2 arguments: (optional dry_run, optional older_than)"
                );
            },
        };
        Ok(DeferredCompactionProvider::new(
            self.catalog_path.clone(),
            sql,
            path_schema(),
            CompactionResultType::AllStrings,
        ))
    }
}

// ==================== Argument extraction helpers ====================

fn extract_string_arg(expr: &Expr, func_name: &str, pos: usize) -> DataFusionResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
        _ => plan_err!(
            "Argument {} to {}() must be a string literal",
            pos,
            func_name
        ),
    }
}

fn extract_float_arg(expr: &Expr, func_name: &str, pos: usize) -> DataFusionResult<f64> {
    match expr {
        Expr::Literal(ScalarValue::Float64(Some(v)), _) => Ok(*v),
        Expr::Literal(ScalarValue::Float32(Some(v)), _) => Ok(f64::from(*v)),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => {
            const MAX_SAFE: i64 = 1_i64 << 53;
            if *v > MAX_SAFE || *v < -MAX_SAFE {
                plan_err!(
                    "Argument {} to {}() integer value {} exceeds safe f64 range (2^53)",
                    pos,
                    func_name,
                    v
                )
            } else {
                Ok(*v as f64)
            }
        },
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(f64::from(*v)),
        _ => plan_err!(
            "Argument {} to {}() must be a numeric literal",
            pos,
            func_name
        ),
    }
}

fn extract_bool_arg(expr: &Expr, func_name: &str, pos: usize) -> DataFusionResult<bool> {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v)), _) => Ok(*v),
        _ => plan_err!(
            "Argument {} to {}() must be a boolean literal",
            pos,
            func_name
        ),
    }
}

// ==================== ducklake_options ====================

#[derive(Debug)]
pub struct DucklakeOptionsFunction {
    catalog_path: String,
}

impl DucklakeOptionsFunction {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
        }
    }
}

impl TableFunctionImpl for DucklakeOptionsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_options() takes no arguments");
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("option_name", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
            Field::new("scope", DataType::Utf8, true),
            Field::new("scope_entry", DataType::Utf8, true),
        ]));
        let sql = "SELECT option_name, description, value, scope, scope_entry FROM ducklake_options('__compaction')".to_string();
        Ok(DeferredCompactionProvider::new(
            self.catalog_path.clone(),
            sql,
            schema,
            CompactionResultType::AllStrings,
        ))
    }
}

// ==================== ducklake_add_data_files ====================

#[derive(Debug)]
pub struct DucklakeAddDataFilesFunction {
    catalog_path: String,
}

impl DucklakeAddDataFilesFunction {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
        }
    }
}

impl TableFunctionImpl for DucklakeAddDataFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() < 2 || exprs.len() > 3 {
            return plan_err!(
                "ducklake_add_data_files() requires 2-3 arguments: (table_name, file_pattern, [schema])"
            );
        }
        let table_name = extract_string_arg(&exprs[0], "ducklake_add_data_files", 1)?;
        let file_pattern = extract_string_arg(&exprs[1], "ducklake_add_data_files", 2)?;
        let sql = if exprs.len() == 3 {
            let schema_name = extract_string_arg(&exprs[2], "ducklake_add_data_files", 3)?;
            format!(
                "SELECT * FROM ducklake_add_data_files('__compaction', '{}', '{}', schema := '{}')",
                table_name.replace('\'', "''"),
                file_pattern.replace('\'', "''"),
                schema_name.replace('\'', "''")
            )
        } else {
            format!(
                "SELECT * FROM ducklake_add_data_files('__compaction', '{}', '{}')",
                table_name.replace('\'', "''"),
                file_pattern.replace('\'', "''")
            )
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "filename",
            DataType::Utf8,
            true,
        )]));
        Ok(DeferredCompactionProvider::new(
            self.catalog_path.clone(),
            sql,
            schema,
            CompactionResultType::AllStrings,
        ))
    }
}

// ==================== ducklake_set_option ====================

#[derive(Debug)]
pub struct DucklakeSetOptionFunction {
    catalog_path: String,
}

impl DucklakeSetOptionFunction {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
        }
    }
}

impl TableFunctionImpl for DucklakeSetOptionFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() != 2 {
            return plan_err!("ducklake_set_option() requires 2 arguments: (option_name, value)");
        }
        let option_name = extract_string_arg(&exprs[0], "ducklake_set_option", 1)?;
        let value = extract_string_arg(&exprs[1], "ducklake_set_option", 2)?;
        let sql = format!(
            "SELECT * FROM ducklake_set_option('__compaction', '{}', '{}')",
            option_name.replace('\'', "''"),
            value.replace('\'', "''")
        );
        Ok(DeferredCompactionProvider::new(
            self.catalog_path.clone(),
            sql,
            success_schema(),
            CompactionResultType::BooleanSuccess,
        ))
    }
}

// ==================== ducklake_set_commit_message ====================

#[derive(Debug)]
pub struct DucklakeSetCommitMessageFunction {
    catalog_path: String,
}

impl DucklakeSetCommitMessageFunction {
    pub fn new(catalog_path: impl Into<String>) -> Self {
        Self {
            catalog_path: catalog_path.into(),
        }
    }
}

impl TableFunctionImpl for DucklakeSetCommitMessageFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() < 2 || exprs.len() > 3 {
            return plan_err!(
                "ducklake_set_commit_message() requires 2-3 arguments: (author, message, [extra_info])"
            );
        }
        let author = extract_string_arg(&exprs[0], "ducklake_set_commit_message", 1)?;
        let message = extract_string_arg(&exprs[1], "ducklake_set_commit_message", 2)?;
        let sql = if exprs.len() == 3 {
            let extra_info = extract_string_arg(&exprs[2], "ducklake_set_commit_message", 3)?;
            format!(
                "SELECT * FROM ducklake_set_commit_message('__compaction', '{}', '{}', extra_info := '{}')",
                author.replace('\'', "''"),
                message.replace('\'', "''"),
                extra_info.replace('\'', "''")
            )
        } else {
            format!(
                "SELECT * FROM ducklake_set_commit_message('__compaction', '{}', '{}')",
                author.replace('\'', "''"),
                message.replace('\'', "''")
            )
        };
        Ok(DeferredCompactionProvider::new(
            self.catalog_path.clone(),
            sql,
            success_schema(),
            CompactionResultType::BooleanSuccess,
        ))
    }
}

// ==================== Registration ====================

pub fn register_ducklake_compaction_functions(
    ctx: &datafusion::execution::context::SessionContext,
    catalog_path: impl Into<String>,
) {
    let path: String = catalog_path.into();
    ctx.register_udtf(
        "ducklake_merge_adjacent_files",
        Arc::new(DucklakeMergeAdjacentFilesFunction::new(path.clone())),
    );
    ctx.register_udtf(
        "ducklake_rewrite_data_files",
        Arc::new(DucklakeRewriteDataFilesFunction::new(path.clone())),
    );
    ctx.register_udtf(
        "ducklake_expire_snapshots",
        Arc::new(DucklakeExpireSnapshotsFunction::new(path.clone())),
    );
    ctx.register_udtf(
        "ducklake_cleanup_old_files",
        Arc::new(DucklakeCleanupOldFilesFunction::new(path.clone())),
    );
    ctx.register_udtf(
        "ducklake_delete_orphaned_files",
        Arc::new(DucklakeDeleteOrphanedFilesFunction::new(path.clone())),
    );
    ctx.register_udtf(
        "ducklake_options",
        Arc::new(DucklakeOptionsFunction::new(path.clone())),
    );
    ctx.register_udtf(
        "ducklake_add_data_files",
        Arc::new(DucklakeAddDataFilesFunction::new(path.clone())),
    );
    ctx.register_udtf(
        "ducklake_set_option",
        Arc::new(DucklakeSetOptionFunction::new(path.clone())),
    );
    ctx.register_udtf(
        "ducklake_set_commit_message",
        Arc::new(DucklakeSetCommitMessageFunction::new(path)),
    );
}
