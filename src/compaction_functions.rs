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
//! Requires the `metadata-duckdb` feature.

#![cfg(feature = "metadata-duckdb")]

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{Result as DataFusionResult, ScalarValue, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;

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
    Arc::new(Schema::new(vec![Field::new(
        "path",
        DataType::Utf8,
        true,
    )]))
}

// ==================== Helper: DuckDB connection ====================

/// Open a temporary DuckDB connection with the ducklake extension and ATTACH the catalog.
/// Returns the connection with the catalog attached as `__compaction`.
fn open_compaction_connection(catalog_path: &str) -> DataFusionResult<duckdb::Connection> {
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
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

/// Execute a DuckDB query that returns `(Success: Boolean)` and collect results.
fn execute_success_query(
    conn: &duckdb::Connection,
    sql: &str,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let schema = success_schema();
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    // This function typically returns 0 rows; collect any that do appear.
    let mut values: Vec<bool> = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
    {
        let v: bool = row
            .get(0)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        values.push(v);
    }

    let batch = if values.is_empty() {
        RecordBatch::new_empty(schema.clone())
    } else {
        let arr: ArrayRef = Arc::new(BooleanArray::from(values));
        RecordBatch::try_new(schema.clone(), vec![arr])
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?
    };

    let mem = datafusion::datasource::memory::MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(Arc::new(mem))
}

/// Execute a DuckDB query that returns `(path: VARCHAR)` and collect results.
fn execute_path_query(
    conn: &duckdb::Connection,
    sql: &str,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let schema = path_schema();
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let mut paths: Vec<Option<String>> = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
    {
        let v: Option<String> = row
            .get(0)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        paths.push(v);
    }

    let batch = if paths.is_empty() {
        RecordBatch::new_empty(schema.clone())
    } else {
        let arr: ArrayRef = Arc::new(StringArray::from(paths));
        RecordBatch::try_new(schema.clone(), vec![arr])
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?
    };

    let mem = datafusion::datasource::memory::MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(Arc::new(mem))
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
        let conn = open_compaction_connection(&self.catalog_path)?;

        let sql = match exprs.len() {
            0 => {
                "SELECT * FROM ducklake_merge_adjacent_files('__compaction')".to_string()
            }
            1 => {
                let table_name = extract_string_arg(&exprs[0], "ducklake_merge_adjacent_files", 1)?;
                format!(
                    "SELECT * FROM ducklake_merge_adjacent_files('__compaction', '{}')",
                    table_name.replace('\'', "''")
                )
            }
            _ => {
                return plan_err!(
                    "ducklake_merge_adjacent_files() takes 0 or 1 arguments (optional table_name)"
                );
            }
        };

        execute_success_query(&conn, &sql)
    }
}

// ==================== ducklake_rewrite_data_files ====================

/// Rewrites data files that exceed a delete threshold, removing delete files.
///
/// Usage:
///   `SELECT * FROM ducklake_rewrite_data_files('table_name', delete_threshold)`
///   `SELECT * FROM ducklake_rewrite_data_files(delete_threshold)`  — all tables
///
/// `delete_threshold` is a float between 0.0 and 1.0 (0.0 = rewrite all files with any deletes).
///
/// Returns: `(Success: Boolean)` — typically 0 rows.
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
        let conn = open_compaction_connection(&self.catalog_path)?;

        let sql = match exprs.len() {
            1 => {
                // rewrite_data_files(delete_threshold) — all tables
                let threshold =
                    extract_float_arg(&exprs[0], "ducklake_rewrite_data_files", 1)?;
                format!(
                    "SELECT * FROM ducklake_rewrite_data_files('__compaction', delete_threshold := {})",
                    threshold
                )
            }
            2 => {
                // rewrite_data_files('table_name', delete_threshold)
                let table_name =
                    extract_string_arg(&exprs[0], "ducklake_rewrite_data_files", 1)?;
                let threshold =
                    extract_float_arg(&exprs[1], "ducklake_rewrite_data_files", 2)?;
                format!(
                    "SELECT * FROM ducklake_rewrite_data_files('__compaction', '{}', delete_threshold := {})",
                    table_name.replace('\'', "''"),
                    threshold
                )
            }
            _ => {
                return plan_err!(
                    "ducklake_rewrite_data_files() requires 1-2 arguments: (optional table_name, delete_threshold)"
                );
            }
        };

        execute_success_query(&conn, &sql)
    }
}

// ==================== ducklake_expire_snapshots ====================

/// Expires old snapshots, making their metadata eligible for cleanup.
///
/// Usage:
///   `SELECT * FROM ducklake_expire_snapshots('2024-01-01T00:00:00Z')`
///
/// Returns expired snapshot rows: `(snapshot_id, snapshot_time, schema_version, changes, author, commit_message, commit_extra_info)`
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
        let conn = open_compaction_connection(&self.catalog_path)?;

        let sql = format!(
            "SELECT snapshot_id, snapshot_time::VARCHAR as snapshot_time, schema_version, \
             changes::VARCHAR as changes, author, commit_message, commit_extra_info \
             FROM ducklake_expire_snapshots('__compaction', older_than := '{}'::TIMESTAMPTZ)",
            older_than.replace('\'', "''")
        );

        let schema = expire_snapshots_schema();
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let mut snapshot_ids: Vec<Option<i64>> = Vec::new();
        let mut snapshot_times: Vec<Option<String>> = Vec::new();
        let mut schema_versions: Vec<Option<i64>> = Vec::new();
        let mut changes_col: Vec<Option<String>> = Vec::new();
        let mut authors: Vec<Option<String>> = Vec::new();
        let mut commit_messages: Vec<Option<String>> = Vec::new();
        let mut commit_extra_infos: Vec<Option<String>> = Vec::new();

        while let Some(row) = rows
            .next()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        {
            snapshot_ids.push(
                row.get(0)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
            );
            snapshot_times.push(
                row.get(1)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
            );
            schema_versions.push(
                row.get(2)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
            );
            changes_col.push(
                row.get(3)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
            );
            authors.push(
                row.get(4)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
            );
            commit_messages.push(
                row.get(5)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
            );
            commit_extra_infos.push(
                row.get(6)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
            );
        }

        let batch = if snapshot_ids.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(snapshot_ids)),
                Arc::new(StringArray::from(snapshot_times)),
                Arc::new(Int64Array::from(schema_versions)),
                Arc::new(StringArray::from(changes_col)),
                Arc::new(StringArray::from(authors)),
                Arc::new(StringArray::from(commit_messages)),
                Arc::new(StringArray::from(commit_extra_infos)),
            ];
            RecordBatch::try_new(schema.clone(), arrays)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?
        };

        let mem = datafusion::datasource::memory::MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(mem))
    }
}

// ==================== ducklake_cleanup_old_files ====================

/// Removes physical files that are no longer referenced after snapshot expiration.
///
/// Usage:
///   `SELECT * FROM ducklake_cleanup_old_files()`
///   `SELECT * FROM ducklake_cleanup_old_files('2024-01-01T00:00:00Z')`  — with explicit older_than
///
/// Returns: `(path: Utf8)` — paths of cleaned-up files.
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
        let conn = open_compaction_connection(&self.catalog_path)?;

        let sql = match exprs.len() {
            0 => {
                // Use a far-future timestamp to avoid TIMESTAMPTZ arithmetic issues
                // in some DuckDB versions. Equivalent to "clean up everything eligible".
                "SELECT * FROM ducklake_cleanup_old_files('__compaction', older_than := '2099-01-01'::TIMESTAMP)".to_string()
            }
            1 => {
                let older_than = extract_string_arg(&exprs[0], "ducklake_cleanup_old_files", 1)?;
                format!(
                    "SELECT * FROM ducklake_cleanup_old_files('__compaction', older_than := '{}'::TIMESTAMP)",
                    older_than.replace('\'', "''")
                )
            }
            _ => {
                return plan_err!(
                    "ducklake_cleanup_old_files() takes 0 or 1 arguments (optional older_than_timestamp)"
                );
            }
        };

        execute_path_query(&conn, &sql)
    }
}

// ==================== ducklake_delete_orphaned_files ====================

/// Removes orphaned files that are not referenced by any snapshot.
///
/// Usage:
///   `SELECT * FROM ducklake_delete_orphaned_files()`  — delete orphaned files
///   `SELECT * FROM ducklake_delete_orphaned_files(true)`  — dry run (list but don't delete)
///
/// Returns: `(path: Utf8)` — paths of orphaned files.
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
        let conn = open_compaction_connection(&self.catalog_path)?;

        // Always pass older_than to avoid TIMESTAMPTZ arithmetic issues in some DuckDB versions.
        let sql = match exprs.len() {
            0 => {
                "SELECT * FROM ducklake_delete_orphaned_files('__compaction', older_than := '2099-01-01'::TIMESTAMP)".to_string()
            }
            1 => {
                let dry_run = extract_bool_arg(&exprs[0], "ducklake_delete_orphaned_files", 1)?;
                format!(
                    "SELECT * FROM ducklake_delete_orphaned_files('__compaction', dry_run := {}, older_than := '2099-01-01'::TIMESTAMP)",
                    dry_run
                )
            }
            _ => {
                return plan_err!(
                    "ducklake_delete_orphaned_files() takes 0 or 1 arguments (optional dry_run boolean)"
                );
            }
        };

        execute_path_query(&conn, &sql)
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
        Expr::Literal(ScalarValue::Float32(Some(v)), _) => Ok(*v as f64),
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Ok(*v as f64),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(*v as f64),
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

// ==================== Registration ====================

/// Registers all ducklake compaction table functions with a SessionContext.
///
/// The `catalog_path` should be the path to the DuckLake catalog database file
/// (the same path used to create a `DuckdbMetadataProvider`).
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
        Arc::new(DucklakeDeleteOrphanedFilesFunction::new(path)),
    );
}
