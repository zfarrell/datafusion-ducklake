//! User-Defined Table Functions (UDTFs) for DuckLake catalog metadata

use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{Result as DataFusionResult, ScalarValue, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::information_schema::{FilesTable, SnapshotsTable, TableInfoTable};
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
}

impl DucklakeTableInfoFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self {
            provider,
        }
    }
}

impl TableFunctionImpl for DucklakeTableInfoFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_table_info() takes no arguments");
        }

        Ok(Arc::new(TableInfoTable::new(self.provider.clone())))
    }
}

#[derive(Debug)]
pub struct DucklakeListFilesFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeListFilesFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self {
            provider,
        }
    }
}

impl TableFunctionImpl for DucklakeListFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_list_files() takes no arguments");
        }

        Ok(Arc::new(FilesTable::new(self.provider.clone())))
    }
}

#[derive(Debug)]
pub struct DucklakeTableChangesFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeTableChangesFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self { provider }
    }
}

impl TableFunctionImpl for DucklakeTableChangesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (table_name, start_snapshot, end_snapshot) =
            parse_change_function_args(exprs, "ducklake_table_changes")?;

        let resolved =
            resolve_table_for_function(&*self.provider, &table_name, "ducklake_table_changes")?;

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
}

impl DucklakeTableDeletionsFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self { provider }
    }
}

impl TableFunctionImpl for DucklakeTableDeletionsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (table_name, start_snapshot, end_snapshot) =
            parse_change_function_args(exprs, "ducklake_table_deletions")?;

        let resolved =
            resolve_table_for_function(&*self.provider, &table_name, "ducklake_table_deletions")?;

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
    _func_name: &str,
) -> DataFusionResult<ResolvedTable> {
    let (schema_name, table_name_only) = parse_table_name(table_name);

    let snapshot_id = provider
        .get_current_snapshot()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let schema = provider
        .get_schema_by_name(schema_name, snapshot_id)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(format!(
                "Schema '{}' not found in catalog",
                schema_name
            ))
        })?;

    let table = provider
        .get_table_by_name(schema.schema_id, table_name_only, snapshot_id)
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
        .get_table_structure(table.table_id)
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

fn parse_table_name(table_name: &str) -> (&str, &str) {
    if let Some(dot_pos) = table_name.find('.') {
        let schema = &table_name[..dot_pos];
        let table = &table_name[dot_pos + 1..];
        (schema, table)
    } else {
        ("main", table_name)
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
        }
    };

    let start_snapshot = match &exprs[1] {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v,
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as i64,
        _ => {
            return plan_err!(
                "Second argument to {}() must be an integer (start_snapshot)",
                func_name
            );
        }
    };

    let end_snapshot = match &exprs[2] {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v,
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as i64,
        _ => {
            return plan_err!(
                "Third argument to {}() must be an integer (end_snapshot)",
                func_name
            );
        }
    };

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
}

impl DucklakeTableInsertionsFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self { provider }
    }
}

impl TableFunctionImpl for DucklakeTableInsertionsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (table_name, start_snapshot, end_snapshot) =
            parse_change_function_args(exprs, "ducklake_table_insertions")?;

        let resolved =
            resolve_table_for_function(&*self.provider, &table_name, "ducklake_table_insertions")?;

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
        Self { schema, value }
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
        let mem_table =
            datafusion::datasource::memory::MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

#[derive(Debug)]
pub struct DucklakeCurrentSnapshotFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeCurrentSnapshotFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self { provider }
    }
}

impl TableFunctionImpl for DucklakeCurrentSnapshotFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_current_snapshot() takes no arguments");
        }

        let snapshot_id = self
            .provider
            .get_current_snapshot()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(SingleValueTable::new(snapshot_id)))
    }
}

#[derive(Debug)]
pub struct DucklakeLastCommittedSnapshotFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeLastCommittedSnapshotFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self { provider }
    }
}

impl TableFunctionImpl for DucklakeLastCommittedSnapshotFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_last_committed_snapshot() takes no arguments");
        }

        // In read-only mode, current snapshot equals last committed.
        // With write support, this would track the last snapshot committed by this session.
        let snapshot_id = self
            .provider
            .get_current_snapshot()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(SingleValueTable::new(snapshot_id)))
    }
}

/// Registers all ducklake_*() table functions with a SessionContext.
pub fn register_ducklake_functions(
    ctx: &datafusion::execution::context::SessionContext,
    provider: Arc<dyn MetadataProvider>,
) {
    ctx.register_udtf(
        "ducklake_snapshots",
        Arc::new(DucklakeSnapshotsFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_table_info",
        Arc::new(DucklakeTableInfoFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_list_files",
        Arc::new(DucklakeListFilesFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_table_changes",
        Arc::new(DucklakeTableChangesFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_table_deletions",
        Arc::new(DucklakeTableDeletionsFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_table_insertions",
        Arc::new(DucklakeTableInsertionsFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_current_snapshot",
        Arc::new(DucklakeCurrentSnapshotFunction::new(provider.clone())),
    );
    ctx.register_udtf(
        "ducklake_last_committed_snapshot",
        Arc::new(DucklakeLastCommittedSnapshotFunction::new(provider.clone())),
    );
}
