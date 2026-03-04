//! User-Defined Table Functions (UDTFs) for DuckLake catalog metadata

use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{Result as DataFusionResult, ScalarValue, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::information_schema::{SnapshotsTable, TableInfoTable};
use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::{parse_object_store_url, resolve_path};
use crate::table_changes::TableChangesTable;
use crate::table_deletions::TableDeletionsTable;
use crate::table_insertions::TableInsertionsTable;
use crate::types::build_arrow_schema;

#[derive(Debug)]
pub struct DucklakeSnapshotsFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeSnapshotsFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self {
            provider,
        }
    }
}

impl TableFunctionImpl for DucklakeSnapshotsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_snapshots() takes no arguments");
        }

        Ok(Arc::new(SnapshotsTable::new(self.provider.clone())))
    }
}

#[derive(Debug)]
pub struct DucklakeTableInfoFunction {
    provider: Arc<dyn MetadataProvider>,
    snapshot_id: i64,
}

impl DucklakeTableInfoFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>, snapshot_id: i64) -> Self {
        Self {
            provider,
            snapshot_id,
        }
    }
}

impl TableFunctionImpl for DucklakeTableInfoFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_table_info() takes no arguments");
        }

        Ok(Arc::new(TableInfoTable::new(
            self.provider.clone(),
            self.snapshot_id,
        )))
    }
}

#[derive(Debug)]
pub struct DucklakeListFilesFunction {
    provider: Arc<dyn MetadataProvider>,
    snapshot_id: i64,
}

impl DucklakeListFilesFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>, snapshot_id: i64) -> Self {
        Self {
            provider,
            snapshot_id,
        }
    }
}

impl TableFunctionImpl for DucklakeListFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() != 1 {
            return plan_err!(
                "ducklake_list_files() requires 1 argument: ducklake_list_files('table_name')"
            );
        }

        let table_name = match &exprs[0] {
            Expr::Literal(ScalarValue::Utf8(Some(name)), _) => name.clone(),
            _ => {
                return plan_err!(
                    "First argument to ducklake_list_files() must be a string literal"
                );
            },
        };

        let resolved = resolve_table_for_function(&*self.provider, &table_name, self.snapshot_id)?;

        let files = self
            .provider
            .get_table_files_for_select(resolved.table_id, self.snapshot_id)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Build the DuckDB-compatible schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("data_file", DataType::Utf8, false),
            Field::new("data_file_size_bytes", DataType::Int64, false),
            Field::new("data_file_footer_size", DataType::Int64, true),
            Field::new("data_file_encryption_key", DataType::Utf8, true),
            Field::new("delete_file", DataType::Utf8, true),
            Field::new("delete_file_size_bytes", DataType::Int64, true),
            Field::new("delete_file_footer_size", DataType::Int64, true),
            Field::new("delete_file_encryption_key", DataType::Utf8, true),
        ]));

        let mut data_file_paths: Vec<String> = Vec::with_capacity(files.len());
        let mut data_file_sizes: Vec<i64> = Vec::with_capacity(files.len());
        let mut data_file_footer_sizes: Vec<Option<i64>> = Vec::with_capacity(files.len());
        let mut data_file_encryption_keys: Vec<Option<String>> = Vec::with_capacity(files.len());
        let mut delete_file_paths: Vec<Option<String>> = Vec::with_capacity(files.len());
        let mut delete_file_sizes: Vec<Option<i64>> = Vec::with_capacity(files.len());
        let mut delete_file_footer_sizes: Vec<Option<i64>> = Vec::with_capacity(files.len());
        let mut delete_file_encryption_keys: Vec<Option<String>> = Vec::with_capacity(files.len());

        // Resolve the table path for constructing full file paths
        let data_path = self
            .provider
            .get_data_path()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        for f in &files {
            // Resolve the data file path
            let file_path = if f.file.path_is_relative {
                resolve_path(&resolved.table_path, &f.file.path, true)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            } else {
                f.file.path.clone()
            };

            // Strip the data_path prefix if the file path starts with the base URL path.
            // Use path-aware stripping: require data_path + "/" to avoid
            // "/data" matching "/database/file.parquet" (R5-S-009).
            let display_path = strip_path_prefix(&file_path, &data_path);

            data_file_paths.push(display_path);
            data_file_sizes.push(f.file.file_size_bytes);
            data_file_footer_sizes.push(f.file.footer_size);
            data_file_encryption_keys.push(f.file.encryption_key.clone());

            if let Some(del) = &f.delete_file {
                let del_path = if del.path_is_relative {
                    resolve_path(&resolved.table_path, &del.path, true)
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                } else {
                    del.path.clone()
                };
                let display_del = strip_path_prefix(&del_path, &data_path);
                delete_file_paths.push(Some(display_del));
                delete_file_sizes.push(Some(del.file_size_bytes));
                delete_file_footer_sizes.push(del.footer_size);
                delete_file_encryption_keys.push(del.encryption_key.clone());
            } else {
                delete_file_paths.push(None);
                delete_file_sizes.push(None);
                delete_file_footer_sizes.push(None);
                delete_file_encryption_keys.push(None);
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(data_file_paths)) as ArrayRef,
                Arc::new(Int64Array::from(data_file_sizes)) as ArrayRef,
                Arc::new(Int64Array::from(data_file_footer_sizes)) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(data_file_encryption_keys)) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(delete_file_paths)) as ArrayRef,
                Arc::new(Int64Array::from(delete_file_sizes)) as ArrayRef,
                Arc::new(Int64Array::from(delete_file_footer_sizes)) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(delete_file_encryption_keys)) as ArrayRef,
            ],
        )
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        let mem_table =
            datafusion::datasource::memory::MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(mem_table))
    }
}

#[derive(Debug)]
pub struct DucklakeTableChangesFunction {
    provider: Arc<dyn MetadataProvider>,
    snapshot_id: i64,
}

impl DucklakeTableChangesFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>, snapshot_id: i64) -> Self {
        Self {
            provider,
            snapshot_id,
        }
    }
}

impl TableFunctionImpl for DucklakeTableChangesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (table_name, start_snapshot, end_snapshot) =
            parse_change_function_args(exprs, "ducklake_table_changes")?;

        let resolved = resolve_table_for_function(&*self.provider, &table_name, self.snapshot_id)?;

        Ok(Arc::new(TableChangesTable::new(
            self.provider.clone(),
            resolved.table_id,
            start_snapshot,
            end_snapshot,
            Arc::new(resolved.object_store_url),
            resolved.table_path,
            resolved.table_schema,
        )))
    }
}

#[derive(Debug)]
pub struct DucklakeTableDeletionsFunction {
    provider: Arc<dyn MetadataProvider>,
    snapshot_id: i64,
}

impl DucklakeTableDeletionsFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>, snapshot_id: i64) -> Self {
        Self {
            provider,
            snapshot_id,
        }
    }
}

impl TableFunctionImpl for DucklakeTableDeletionsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (table_name, start_snapshot, end_snapshot) =
            parse_change_function_args(exprs, "ducklake_table_deletions")?;

        let resolved = resolve_table_for_function(&*self.provider, &table_name, self.snapshot_id)?;

        Ok(Arc::new(TableDeletionsTable::new(
            self.provider.clone(),
            resolved.table_id,
            start_snapshot,
            end_snapshot,
            Arc::new(resolved.object_store_url),
            resolved.table_path,
            resolved.table_schema,
        )))
    }
}

/// Helper to resolve a table name, its schema, path, and Arrow schema.
/// Shared by table_changes, table_deletions, and table_insertions functions.
struct ResolvedTable {
    table_id: i64,
    object_store_url: datafusion::execution::object_store::ObjectStoreUrl,
    table_path: String,
    table_schema: Arc<Schema>,
}

fn resolve_table_for_function(
    provider: &dyn MetadataProvider,
    table_name: &str,
    snapshot_id: i64,
) -> DataFusionResult<ResolvedTable> {
    let (schema_name, table_name_only) = parse_table_name(table_name)?;

    let schema = provider
        .get_schema_by_name(&schema_name, snapshot_id)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "Schema '{}' not found in catalog",
                schema_name
            ))
        })?;

    let table = provider
        .get_table_by_name(schema.schema_id, &table_name_only, snapshot_id)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "Table '{}.{}' not found in catalog",
                schema_name, table_name_only
            ))
        })?;

    let data_path = provider
        .get_data_path()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let (object_store_url, catalog_path) = parse_object_store_url(&data_path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let schema_path = resolve_path(&catalog_path, &schema.path, schema.path_is_relative)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let table_path = resolve_path(&schema_path, &table.path, table.path_is_relative)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let columns = provider
        .get_table_structure(table.table_id, snapshot_id)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let table_schema = Arc::new(
        build_arrow_schema(&columns)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
    );

    Ok(ResolvedTable {
        table_id: table.table_id,
        object_store_url,
        table_path,
        table_schema,
    })
}

/// Strip a directory prefix from a path using path-aware matching.
///
/// Only strips when the path starts with `prefix + "/"` (or equals `prefix`),
/// preventing `/data` from stripping from `/database/file.parquet`.
fn strip_path_prefix(path: &str, prefix: &str) -> String {
    let prefix_with_slash = if prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{}/", prefix)
    };

    if path.starts_with(&prefix_with_slash) {
        path[prefix_with_slash.len()..].to_string()
    } else if path == prefix {
        String::new()
    } else {
        path.to_string()
    }
}

/// Parse a potentially qualified table name into (schema, table).
///
/// Handles quoted identifiers: dots inside double-quotes are not treated as
/// separators (R6-S-011). Quotes are stripped and escaped `""` is unescaped.
/// Empty parts (e.g., ".foo" or "foo.") return an error (R6-S-028).
fn parse_table_name(table_name: &str) -> DataFusionResult<(String, String)> {
    // Find the first dot that is not inside double-quotes
    let mut in_quotes = false;
    let mut dot_pos = None;
    for (i, ch) in table_name.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            '.' if !in_quotes => {
                dot_pos = Some(i);
                break;
            },
            _ => {},
        }
    }

    if let Some(pos) = dot_pos {
        let schema_raw = &table_name[..pos];
        let table_raw = &table_name[pos + 1..];
        if schema_raw.is_empty() || table_raw.is_empty() {
            return plan_err!(
                "Malformed table name '{}': both schema and table parts must be non-empty",
                table_name
            );
        }
        Ok((
            unquote_identifier(schema_raw),
            unquote_identifier(table_raw),
        ))
    } else {
        Ok(("main".to_string(), unquote_identifier(table_name)))
    }
}

/// Strip surrounding double-quotes from a SQL identifier and unescape `""` → `"`.
fn unquote_identifier(s: &str) -> String {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        s[1..s.len() - 1].replace("\"\"", "\"")
    } else {
        s.to_string()
    }
}

/// Extract a snapshot ID from a scalar expression, accepting wider numeric types (R5-S-038).
fn extract_snapshot_arg(
    expr: &Expr,
    func_name: &str,
    ordinal: &str,
    param_name: &str,
) -> DataFusionResult<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Ok(*v),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Ok(i64::from(*v)),
        Expr::Literal(ScalarValue::Int16(Some(v)), _) => Ok(i64::from(*v)),
        Expr::Literal(ScalarValue::Int8(Some(v)), _) => Ok(i64::from(*v)),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => i64::try_from(*v).map_err(|_| {
            datafusion::error::DataFusionError::Plan(format!(
                "{} argument to {}() value {} overflows i64 ({})",
                ordinal, func_name, v, param_name
            ))
        }),
        Expr::Literal(ScalarValue::UInt32(Some(v)), _) => Ok(i64::from(*v)),
        Expr::Literal(ScalarValue::UInt16(Some(v)), _) => Ok(i64::from(*v)),
        Expr::Literal(ScalarValue::UInt8(Some(v)), _) => Ok(i64::from(*v)),
        _ => {
            plan_err!(
                "{} argument to {}() must be an integer ({})",
                ordinal,
                func_name,
                param_name
            )
        },
    }
}

/// Parse the 3-argument pattern: (table_name, start_snapshot, end_snapshot)
fn parse_change_function_args(
    exprs: &[Expr],
    func_name: &str,
) -> DataFusionResult<(String, i64, i64)> {
    if exprs.len() != 3 {
        return plan_err!(
            "{}() requires 3 arguments: {}('schema.table', start_snapshot, end_snapshot)",
            func_name,
            func_name
        );
    }

    let table_name = match &exprs[0] {
        Expr::Literal(ScalarValue::Utf8(Some(name)), _) => name.clone(),
        _ => {
            return plan_err!(
                "First argument to {}() must be a string literal (e.g., 'main.users' or 'users')",
                func_name
            );
        },
    };

    let start_snapshot = extract_snapshot_arg(&exprs[1], func_name, "Second", "start_snapshot")?;
    let end_snapshot = extract_snapshot_arg(&exprs[2], func_name, "Third", "end_snapshot")?;

    if start_snapshot > end_snapshot {
        return plan_err!(
            "start_snapshot ({}) must be less than or equal to end_snapshot ({})",
            start_snapshot,
            end_snapshot
        );
    }

    Ok((table_name, start_snapshot, end_snapshot))
}

#[derive(Debug)]
pub struct DucklakeTableInsertionsFunction {
    provider: Arc<dyn MetadataProvider>,
    snapshot_id: i64,
}

impl DucklakeTableInsertionsFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>, snapshot_id: i64) -> Self {
        Self {
            provider,
            snapshot_id,
        }
    }
}

impl TableFunctionImpl for DucklakeTableInsertionsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (table_name, start_snapshot, end_snapshot) =
            parse_change_function_args(exprs, "ducklake_table_insertions")?;

        let resolved = resolve_table_for_function(&*self.provider, &table_name, self.snapshot_id)?;

        Ok(Arc::new(TableInsertionsTable::new(
            self.provider.clone(),
            resolved.table_id,
            start_snapshot,
            end_snapshot,
            Arc::new(resolved.object_store_url),
            resolved.table_path,
            resolved.table_schema,
        )))
    }
}

/// Simple MemTable-like provider that returns a single snapshot ID value
#[derive(Debug)]
struct SingleValueTable {
    schema: SchemaRef,
    value: i64,
}

impl SingleValueTable {
    fn new(value: i64) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        Self {
            schema,
            value,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for SingleValueTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::View
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![self.value]));
        let batch = RecordBatch::try_new(self.schema.clone(), vec![id_array])
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        let mem_table = datafusion::datasource::memory::MemTable::try_new(
            self.schema.clone(),
            vec![vec![batch]],
        )?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

#[derive(Debug)]
pub struct DucklakeCurrentSnapshotFunction {
    snapshot_id: i64,
}

impl DucklakeCurrentSnapshotFunction {
    pub fn new(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
        }
    }
}

impl TableFunctionImpl for DucklakeCurrentSnapshotFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_current_snapshot() takes no arguments");
        }

        Ok(Arc::new(SingleValueTable::new(self.snapshot_id)))
    }
}

#[derive(Debug)]
pub struct DucklakeLastCommittedSnapshotFunction {
    snapshot_id: i64,
}

impl DucklakeLastCommittedSnapshotFunction {
    pub fn new(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
        }
    }
}

impl TableFunctionImpl for DucklakeLastCommittedSnapshotFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_last_committed_snapshot() takes no arguments");
        }

        Ok(Arc::new(SingleValueTable::new(self.snapshot_id)))
    }
}

/// Registers all ducklake_*() table functions with a SessionContext.
///
/// The `snapshot_id` parameter pins all metadata lookups to a specific snapshot,
/// ensuring consistency with the catalog's pinned snapshot. Use
/// `DuckLakeCatalog::snapshot_id()` to obtain the pinned snapshot ID.
pub fn register_ducklake_functions(
    ctx: &datafusion::execution::context::SessionContext,
    provider: Arc<dyn MetadataProvider>,
    snapshot_id: i64,
) {
    ctx.register_udtf(
        "ducklake_snapshots",
        Arc::new(DucklakeSnapshotsFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_table_info",
        Arc::new(DucklakeTableInfoFunction::new(
            provider.clone(),
            snapshot_id,
        )),
    );
    ctx.register_udtf(
        "ducklake_list_files",
        Arc::new(DucklakeListFilesFunction::new(
            provider.clone(),
            snapshot_id,
        )),
    );
    ctx.register_udtf(
        "ducklake_table_changes",
        Arc::new(DucklakeTableChangesFunction::new(
            provider.clone(),
            snapshot_id,
        )),
    );
    ctx.register_udtf(
        "ducklake_table_deletions",
        Arc::new(DucklakeTableDeletionsFunction::new(
            provider.clone(),
            snapshot_id,
        )),
    );
    ctx.register_udtf(
        "ducklake_table_insertions",
        Arc::new(DucklakeTableInsertionsFunction::new(
            provider.clone(),
            snapshot_id,
        )),
    );
    ctx.register_udtf(
        "ducklake_current_snapshot",
        Arc::new(DucklakeCurrentSnapshotFunction::new(snapshot_id)),
    );
    ctx.register_udtf(
        "ducklake_last_committed_snapshot",
        Arc::new(DucklakeLastCommittedSnapshotFunction::new(snapshot_id)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== R5-S-009: strip_path_prefix tests ====================

    #[test]
    fn test_strip_path_prefix_exact_match() {
        assert_eq!(
            strip_path_prefix("/data/table/file.parquet", "/data"),
            "table/file.parquet"
        );
    }

    #[test]
    fn test_strip_path_prefix_with_trailing_slash() {
        assert_eq!(
            strip_path_prefix("/data/table/file.parquet", "/data/"),
            "table/file.parquet"
        );
    }

    #[test]
    fn test_strip_path_prefix_no_match() {
        // "/data" should NOT strip from "/database/file.parquet" — this was the original bug
        assert_eq!(
            strip_path_prefix("/database/file.parquet", "/data"),
            "/database/file.parquet"
        );
    }

    #[test]
    fn test_strip_path_prefix_exact_path_equals() {
        assert_eq!(strip_path_prefix("/data", "/data"), "");
    }

    #[test]
    fn test_strip_path_prefix_no_prefix_overlap() {
        assert_eq!(
            strip_path_prefix("/other/file.parquet", "/data"),
            "/other/file.parquet"
        );
    }

    #[test]
    fn test_strip_path_prefix_nested() {
        assert_eq!(
            strip_path_prefix("/data/schemas/main/table/f.parquet", "/data/schemas"),
            "main/table/f.parquet"
        );
    }

    // ==================== parse_table_name tests (R5-S-030, R6-S-011, R6-S-028) ====================

    #[test]
    fn test_parse_table_name_simple() {
        let (s, t) = parse_table_name("users").unwrap();
        assert_eq!(s, "main");
        assert_eq!(t, "users");
    }

    #[test]
    fn test_parse_table_name_qualified() {
        let (s, t) = parse_table_name("myschema.users").unwrap();
        assert_eq!(s, "myschema");
        assert_eq!(t, "users");
    }

    #[test]
    fn test_parse_table_name_quoted_with_dot() {
        // Dots inside double-quotes should not split; quotes are stripped (R6-S-011)
        let (schema, table) = parse_table_name("\"my.schema\".users").unwrap();
        assert_eq!(schema, "my.schema");
        assert_eq!(table, "users");
    }

    #[test]
    fn test_parse_table_name_quoted_table() {
        let (schema, table) = parse_table_name("main.\"my.table\"").unwrap();
        assert_eq!(schema, "main");
        assert_eq!(table, "my.table");
    }

    #[test]
    fn test_parse_table_name_both_quoted() {
        let (s, t) = parse_table_name("\"my.schema\".\"my.table\"").unwrap();
        assert_eq!(s, "my.schema");
        assert_eq!(t, "my.table");
    }

    #[test]
    fn test_parse_table_name_escaped_quotes() {
        let (s, t) = parse_table_name("\"say\"\"hello\"").unwrap();
        assert_eq!(s, "main");
        assert_eq!(t, "say\"hello");
    }

    #[test]
    fn test_parse_table_name_empty_schema_error() {
        assert!(parse_table_name(".foo").is_err());
    }

    #[test]
    fn test_parse_table_name_empty_table_error() {
        assert!(parse_table_name("foo.").is_err());
    }

    #[test]
    fn test_parse_table_name_double_dot_error() {
        assert!(parse_table_name("..").is_err());
    }

    // ==================== R5-S-038: extract_snapshot_arg tests ====================

    #[test]
    fn test_extract_snapshot_uint32() {
        let expr = Expr::Literal(ScalarValue::UInt32(Some(42)), None);
        let result = extract_snapshot_arg(&expr, "test", "First", "start").unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_extract_snapshot_uint64() {
        let expr = Expr::Literal(ScalarValue::UInt64(Some(100)), None);
        let result = extract_snapshot_arg(&expr, "test", "First", "start").unwrap();
        assert_eq!(result, 100);
    }

    #[test]
    fn test_extract_snapshot_uint64_overflow() {
        let expr = Expr::Literal(ScalarValue::UInt64(Some(u64::MAX)), None);
        let result = extract_snapshot_arg(&expr, "test", "First", "start");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_snapshot_uint8() {
        let expr = Expr::Literal(ScalarValue::UInt8(Some(5)), None);
        let result = extract_snapshot_arg(&expr, "test", "First", "start").unwrap();
        assert_eq!(result, 5);
    }

    #[test]
    fn test_extract_snapshot_int16() {
        let expr = Expr::Literal(ScalarValue::Int16(Some(-10)), None);
        let result = extract_snapshot_arg(&expr, "test", "First", "start").unwrap();
        assert_eq!(result, -10);
    }

    #[test]
    fn test_extract_snapshot_string_rejected() {
        let expr = Expr::Literal(ScalarValue::Utf8(Some("5".to_string())), None);
        let result = extract_snapshot_arg(&expr, "test", "First", "start");
        assert!(result.is_err());
    }
}
