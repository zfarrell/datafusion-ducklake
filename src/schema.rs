//! DuckLake schema provider implementation

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::datasource::ViewTable;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::catalog::DuckLakeCatalog;
use crate::metadata_provider::{MetadataProvider, ViewMetadata};
use crate::path_resolver::resolve_path;
use crate::table::DuckLakeTable;

#[cfg(feature = "write")]
use crate::metadata_writer::{ColumnDef, MetadataWriter, WriteMode};
#[cfg(feature = "write")]
use crate::table_writer::DuckLakeTableWriter;
#[cfg(feature = "write")]
use datafusion::error::DataFusionError;

/// Validate table name to prevent path traversal attacks.
/// Table names are used to construct file paths, so we must ensure they
/// don't contain path separators or parent directory references.
#[cfg(feature = "write")]
fn validate_table_name(name: &str) -> DataFusionResult<()> {
    if name.is_empty() {
        return Err(DataFusionError::Plan(
            "Table name cannot be empty".to_string(),
        ));
    }
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return Err(DataFusionError::Plan(format!(
            "Invalid table name '{}': must not contain path separators or '..'",
            name
        )));
    }
    // Also reject names that are just dots
    if name.chars().all(|c| c == '.') {
        return Err(DataFusionError::Plan(format!(
            "Invalid table name '{}': must not be only dots",
            name
        )));
    }
    Ok(())
}

/// DuckLake schema provider
///
/// Represents a schema within a DuckLake catalog and provides access to tables.
/// Uses dynamic metadata lookup - tables are queried on-demand from the catalog database.
/// Caches snapshot_id received from catalog.schema() call for query consistency.
#[derive(Debug)]
pub struct DuckLakeSchema {
    schema_id: i64,
    schema_name: String,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    provider: Arc<dyn MetadataProvider>,
    /// Cached snapshot_id from catalog.schema() call
    snapshot_id: i64,
    /// Schema path for resolving relative table paths
    schema_path: String,
    /// Metadata writer for write operations (when write feature is enabled)
    #[cfg(feature = "write")]
    writer: Option<Arc<dyn MetadataWriter>>,
}

impl DuckLakeSchema {
    /// Create a new DuckLake schema
    pub fn new(
        schema_id: i64,
        schema_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64, // Received from catalog
        object_store_url: Arc<ObjectStoreUrl>,
        schema_path: String,
    ) -> Self {
        Self {
            schema_id,
            schema_name: schema_name.into(),
            provider,
            snapshot_id,
            object_store_url,
            schema_path,
            #[cfg(feature = "write")]
            writer: None,
        }
    }

    /// Configure this schema for write operations.
    ///
    /// This method enables write support by attaching a metadata writer.
    /// Once configured, the schema can handle CREATE TABLE AS and tables can handle INSERT INTO.
    ///
    /// # Arguments
    /// * `writer` - Metadata writer for catalog operations
    #[cfg(feature = "write")]
    pub fn with_writer(mut self, writer: Arc<dyn MetadataWriter>) -> Self {
        self.writer = Some(writer);
        self
    }

    /// Plan a view's SQL definition and return a ViewTable.
    ///
    /// Creates a temporary SessionContext with a DuckLakeCatalog registered
    /// under the default catalog name, so unqualified and schema-qualified
    /// table references in the view SQL resolve correctly.
    async fn plan_view(&self, view: &ViewMetadata) -> DataFusionResult<ViewTable> {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.default_catalog = "ducklake".to_string();
        config.options_mut().catalog.default_schema = self.schema_name.clone();
        config
            .options_mut()
            .catalog
            .create_default_catalog_and_schema = false;

        let temp_ctx = SessionContext::new_with_config(config);

        let temp_catalog = DuckLakeCatalog::with_snapshot(self.provider.clone(), self.snapshot_id)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        temp_ctx.register_catalog("ducklake", Arc::new(temp_catalog));

        let plan = temp_ctx.state().create_logical_plan(&view.sql).await?;

        Ok(ViewTable::new(plan, Some(view.sql.clone())))
    }
}

#[async_trait]
impl SchemaProvider for DuckLakeSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Use cached snapshot_id
        let mut names: Vec<String> = self
            .provider
            .list_tables(self.schema_id, self.snapshot_id)
            .inspect_err(|e| {
                tracing::error!(
                    error = %e,
                    schema_id = %self.schema_id,
                    snapshot_id = %self.snapshot_id,
                    schema_name = %self.schema_name,
                    "Failed to list tables from catalog"
                )
            })
            .unwrap_or_default()
            .into_iter()
            .map(|t| t.table_name)
            .collect();

        // Also include views
        if let Ok(views) = self.provider.list_views(self.schema_id, self.snapshot_id) {
            names.extend(views.into_iter().map(|v| v.view_name));
        }

        names
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // Use cached snapshot_id - check tables first
        match self
            .provider
            .get_table_by_name(self.schema_id, name, self.snapshot_id)
        {
            Ok(Some(meta)) => {
                // Resolve table path hierarchically using path_resolver utility
                let table_path = resolve_path(&self.schema_path, &meta.path, meta.path_is_relative)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                // Pass snapshot_id to table
                let table = DuckLakeTable::new(
                    meta.table_id,
                    meta.table_name.clone(),
                    self.provider.clone(),
                    self.snapshot_id, // Propagate snapshot_id
                    self.object_store_url.clone(),
                    table_path,
                )
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                // Configure writer if this schema is writable
                #[cfg(feature = "write")]
                let table = if let Some(writer) = self.writer.as_ref() {
                    table.with_writer(self.schema_name.clone(), Arc::clone(writer))
                } else {
                    table
                };

                return Ok(Some(Arc::new(table) as Arc<dyn TableProvider>));
            },
            Ok(None) => {},
            Err(e) => return Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }

        // Table not found — check for views
        match self
            .provider
            .get_view_by_name(self.schema_id, name, self.snapshot_id)
        {
            Ok(Some(view_meta)) => {
                let view_table = self.plan_view(&view_meta).await?;
                Ok(Some(Arc::new(view_table) as Arc<dyn TableProvider>))
            },
            Ok(None) => Ok(None),
            Err(e) => Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        // Use cached snapshot_id — check tables and views
        self.provider
            .table_exists(self.schema_id, name, self.snapshot_id)
            .unwrap_or(false)
            || self
                .provider
                .view_exists(self.schema_id, name, self.snapshot_id)
                .unwrap_or(false)
    }

    /// Deregister (drop) a table from this schema.
    ///
    /// This is called by DataFusion for DROP TABLE statements.
    /// It marks the table as dropped in metadata (sets end_snapshot).
    /// Data files are NOT deleted (preserved for time travel).
    /// Returns the dropped table provider, or None if the table doesn't exist.
    #[cfg(feature = "write")]
    fn deregister_table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let writer = self.writer.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Schema is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        // Look up the table to get its ID
        let table_meta = self
            .provider
            .get_table_by_name(self.schema_id, name, self.snapshot_id)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let Some(meta) = table_meta else {
            // Table doesn't exist - return None (DataFusion handles IF EXISTS)
            return Ok(None);
        };

        // Resolve table path for constructing the table provider to return
        let table_path = resolve_path(&self.schema_path, &meta.path, meta.path_is_relative)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let table = DuckLakeTable::new(
            meta.table_id,
            meta.table_name.clone(),
            self.provider.clone(),
            self.snapshot_id,
            self.object_store_url.clone(),
            table_path,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Drop the table in metadata (creates new snapshot, sets end_snapshot)
        writer
            .drop_table(meta.table_id)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Some(Arc::new(table) as Arc<dyn TableProvider>))
    }

    /// Register a new table in this schema.
    ///
    /// This is called by DataFusion for CREATE TABLE and CREATE TABLE AS SELECT (CTAS).
    /// For CTAS, DataFusion collects the SELECT data into a MemTable and passes it here.
    /// We extract the data, create table metadata, and write data to Parquet files.
    #[cfg(feature = "write")]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        use crate::metadata_provider::block_on;
        use datafusion::physical_plan::ExecutionPlanProperties;
        use futures::TryStreamExt;

        // Validate table name to prevent path traversal attacks
        validate_table_name(&name)?;

        let writer = self.writer.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Schema is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        // Extract data batches from the input table (for CTAS, this is a MemTable with data).
        // We scan all partitions using a temporary SessionContext to collect all rows.
        let batches: Vec<arrow::record_batch::RecordBatch> = block_on(async {
            let ctx = datafusion::prelude::SessionContext::new();
            let plan = table.scan(&ctx.state(), None, &[], None).await?;
            let task_ctx = ctx.task_ctx();
            let num_partitions = plan.output_partitioning().partition_count();
            let mut all_batches = Vec::new();
            for partition in 0..num_partitions {
                let stream = plan.execute(partition, Arc::clone(&task_ctx))?;
                let partition_batches: Vec<arrow::record_batch::RecordBatch> =
                    stream.try_collect().await?;
                all_batches.extend(partition_batches);
            }
            Ok(all_batches) as DataFusionResult<Vec<_>>
        })?;

        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            // Empty table (CREATE TABLE without AS SELECT, or empty SELECT).
            // Just create metadata.
            let arrow_schema = table.schema();
            let columns: Vec<ColumnDef> = arrow_schema
                .fields()
                .iter()
                .map(|field| {
                    ColumnDef::from_arrow(field.name(), field.data_type(), field.is_nullable())
                        .map_err(|e| DataFusionError::External(Box::new(e)))
                })
                .collect::<DataFusionResult<Vec<_>>>()?;

            writer
                .begin_write_transaction(&self.schema_name, &name, &columns, WriteMode::Replace)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        } else {
            // CTAS with data — write to Parquet and create metadata in one operation.
            let object_store: Arc<dyn object_store::ObjectStore> =
                Arc::new(object_store::local::LocalFileSystem::new());
            let table_writer = DuckLakeTableWriter::new(Arc::clone(writer), object_store)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Filter out empty batches
            let non_empty: Vec<_> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();

            block_on(table_writer.write_table(&self.schema_name, &name, &non_empty))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        // Return None to indicate a newly created table.
        // DataFusion uses this to distinguish new tables from replaced ones.
        Ok(None)
    }
}

#[cfg(all(test, feature = "write"))]
mod tests {
    use super::*;

    #[test]
    fn test_validate_table_name_valid() {
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("my_table").is_ok());
        assert!(validate_table_name("Table123").is_ok());
        assert!(validate_table_name("a").is_ok());
    }

    #[test]
    fn test_validate_table_name_empty() {
        let result = validate_table_name("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validate_table_name_path_traversal() {
        // Forward slash
        let result = validate_table_name("../etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path separators"));

        // Backslash
        let result = validate_table_name("..\\windows\\system32");
        assert!(result.is_err());

        // Double dot
        let result = validate_table_name("foo..bar");
        assert!(result.is_err());

        // Just slashes
        let result = validate_table_name("foo/bar");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_table_name_only_dots() {
        assert!(validate_table_name(".").is_err());
        assert!(validate_table_name("..").is_err());
        assert!(validate_table_name("...").is_err());
    }
}
