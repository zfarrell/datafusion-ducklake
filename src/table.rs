//! DuckLake table provider implementation

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::Result;
use crate::column_rename::ColumnRenameExec;
use crate::delete_filter::DeleteFilterExec;
use crate::metadata_provider::{
    DuckLakeFileData, DuckLakeTableColumn, DuckLakeTableFile, MetadataProvider,
};
use crate::path_resolver::resolve_path;
use crate::types::{
    build_arrow_schema, build_read_schema_with_field_id_mapping, extract_parquet_field_ids,
};
use crate::virtual_column_exec::{
    VIRTUAL_COL_FILE_ROW_NUMBER, VIRTUAL_COL_FILENAME, VirtualColumnExec,
};

#[cfg(feature = "write")]
use crate::delete_exec::DuckLakeDeleteExec;
#[cfg(feature = "write")]
use crate::insert_exec::DuckLakeInsertExec;
#[cfg(feature = "write")]
use crate::metadata_writer::{MetadataWriter, WriteMode};

#[cfg(feature = "encryption")]
use crate::encryption::EncryptionFactoryBuilder;
use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::ObjectStoreUrl;
#[cfg(feature = "write")]
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
#[cfg(feature = "write")]
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::ExecutionPlan;
#[cfg(feature = "write")]
use datafusion::physical_plan::projection::ProjectionExec;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use tokio::sync::OnceCell;

#[cfg(feature = "encryption")]
use datafusion::execution::parquet_encryption::EncryptionFactory;

// Delete file schema constants (public for testing)
pub const DELETE_FILE_PATH_COL: &str = "file_path";
pub const DELETE_POS_COL: &str = "pos";

/// Returns the expected schema for DuckLake delete files
///
/// Delete files have a standard schema: (file_path: VARCHAR, pos: INT64)
/// The file_path column is metadata/documentation only (for Iceberg compatibility).
/// The pos column contains the row positions to delete.
pub fn delete_file_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(DELETE_FILE_PATH_COL, DataType::Utf8, false),
        Field::new(DELETE_POS_COL, DataType::Int64, false),
    ]))
}

/// Cached schema mapping for renamed columns
type SchemaMappingCache = (SchemaRef, HashMap<String, String>);

/// DuckLake table provider
///
/// Represents a table within a DuckLake schema and provides access to data via Parquet files.
/// Caches snapshot_id and uses it to load all metadata atomically.
pub struct DuckLakeTable {
    table_id: i64,
    table_name: String,
    provider: Arc<dyn MetadataProvider>,
    /// Snapshot ID this table is bound to
    snapshot_id: i64,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    /// Table path for resolving relative file paths
    table_path: String,
    /// Base schema without virtual columns
    schema: SchemaRef,
    /// Full schema including virtual columns (filename, file_row_number)
    full_schema: SchemaRef,
    /// Column metadata from DuckLake (needed for field_id mapping)
    columns: Vec<DuckLakeTableColumn>,
    /// Table files with paths as stored in metadata (resolved on-the-fly when needed)
    table_files: Vec<DuckLakeTableFile>,
    /// Cached schema mapping (read_schema, name_mapping) - computed once on first scan
    schema_mapping_cache: OnceCell<SchemaMappingCache>,
    /// Encryption factory for decrypting encrypted Parquet files (when encryption feature is enabled)
    #[cfg(feature = "encryption")]
    encryption_factory: Option<Arc<dyn EncryptionFactory>>,
    /// Schema name (needed for write operations)
    #[cfg(feature = "write")]
    schema_name: Option<String>,
    /// Metadata writer for write operations (when write feature is enabled)
    #[cfg(feature = "write")]
    writer: Option<Arc<dyn MetadataWriter>>,
}

impl std::fmt::Debug for DuckLakeTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckLakeTable")
            .field("table_id", &self.table_id)
            .field("table_name", &self.table_name)
            .field("table_path", &self.table_path)
            .field("schema", &self.schema)
            .field("columns", &self.columns)
            .field("table_files", &self.table_files)
            .finish_non_exhaustive()
    }
}

impl DuckLakeTable {
    /// Create a new DuckLake table
    pub fn new(
        table_id: i64,
        table_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64, // Received from schema
        object_store_url: Arc<ObjectStoreUrl>,
        table_path: String,
    ) -> Result<Self> {
        // Load ALL metadata with this snapshot_id
        let columns = provider.get_table_structure(table_id)?;
        let schema = Arc::new(build_arrow_schema(&columns)?);
        let full_schema = {
            let mut fields = schema.fields().to_vec();
            fields.push(Arc::new(Field::new(
                VIRTUAL_COL_FILENAME,
                DataType::Utf8,
                true,
            )));
            fields.push(Arc::new(Field::new(
                VIRTUAL_COL_FILE_ROW_NUMBER,
                DataType::Int64,
                true,
            )));
            Arc::new(Schema::new(fields))
        };
        let table_files = provider.get_table_files_for_select(table_id, snapshot_id)?;

        // Build encryption factory from file encryption keys (when encryption feature is enabled)
        #[cfg(feature = "encryption")]
        let encryption_factory = {
            let mut builder = EncryptionFactoryBuilder::new();
            for table_file in &table_files {
                // Resolve the file path for the mapping
                let resolved_path = resolve_path(
                    &table_path,
                    &table_file.file.path,
                    table_file.file.path_is_relative,
                );
                builder.add_file(&resolved_path, table_file.file.encryption_key.as_deref());

                // Also add delete file encryption key if present
                if let Some(ref delete_file) = table_file.delete_file {
                    let resolved_delete_path =
                        resolve_path(&table_path, &delete_file.path, delete_file.path_is_relative);
                    builder.add_file(&resolved_delete_path, delete_file.encryption_key.as_deref());
                }
            }
            let factory = builder.build();
            if factory.has_encrypted_files() {
                Some(Arc::new(factory) as Arc<dyn EncryptionFactory>)
            } else {
                None
            }
        };

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            provider,
            snapshot_id,
            object_store_url,
            table_path,
            schema,
            full_schema,
            columns,
            table_files,
            #[cfg(feature = "encryption")]
            encryption_factory,
            schema_mapping_cache: OnceCell::new(),
            #[cfg(feature = "write")]
            schema_name: None,
            #[cfg(feature = "write")]
            writer: None,
        })
    }

    /// Resolve a file path (data or delete file) to its absolute path
    fn resolve_file_path(&self, file: &DuckLakeFileData) -> String {
        resolve_path(&self.table_path, &file.path, file.path_is_relative)
    }

    /// Create a ParquetSource with encryption support if enabled and needed
    fn create_parquet_source(&self) -> ParquetSource {
        #[cfg(feature = "encryption")]
        if let Some(ref factory) = self.encryption_factory {
            return ParquetSource::default().with_encryption_factory(Arc::clone(factory));
        }
        ParquetSource::default()
    }

    /// Get the cached schema mapping, computing it once from the first file if needed.
    /// All files in a DuckLake table have the same schema structure, so we only need to check one.
    async fn get_schema_mapping(
        &self,
        state: &dyn Session,
    ) -> DataFusionResult<&SchemaMappingCache> {
        self.schema_mapping_cache
            .get_or_try_init(|| async {
                // If no files, use current schema with no rename mapping
                let Some(first_file) = self.table_files.first() else {
                    return Ok((self.schema.clone(), HashMap::new()));
                };

                let resolved_path = self.resolve_file_path(&first_file.file);
                let object_store = state
                    .runtime_env()
                    .object_store(self.object_store_url.as_ref())?;
                let object_path = ObjectPath::from(resolved_path.as_str());

                let reader = ParquetObjectReader::new(object_store, object_path);

                // Build the ParquetRecordBatchStreamBuilder with decryption if needed
                #[cfg(feature = "encryption")]
                let builder = {
                    use parquet::arrow::arrow_reader::ArrowReaderOptions;

                    // Check if file has encryption key
                    let options = if let Some(ref key) = first_file.file.encryption_key {
                        if !key.is_empty() {
                            let key_bytes =
                                crate::encryption::DuckLakeEncryptionFactory::decode_key(key)?;
                            let decryption_props =
                                parquet::encryption::decrypt::FileDecryptionProperties::builder(
                                    key_bytes,
                                )
                                .build()
                                .map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Failed to create decryption properties: {}",
                                        e
                                    ))
                                })?;
                            ArrowReaderOptions::new()
                                .with_file_decryption_properties(decryption_props)
                        } else {
                            ArrowReaderOptions::new()
                        }
                    } else {
                        ArrowReaderOptions::new()
                    };

                    ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?
                };

                #[cfg(not(feature = "encryption"))]
                let builder = ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let field_id_map = extract_parquet_field_ids(builder.metadata());

                // No field_ids means external file - use current schema directly
                if field_id_map.is_empty() {
                    return Ok((self.schema.clone(), HashMap::new()));
                }

                let (read_schema, name_mapping) =
                    build_read_schema_with_field_id_mapping(&self.columns, &field_id_map)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                Ok((Arc::new(read_schema), name_mapping))
            })
            .await
    }

    /// Read a delete file and extract all deleted row positions
    ///
    /// The delete file is already associated with a specific data file via metadata.
    /// We only need to extract the "pos" column - the "file_path" column is
    /// metadata/documentation only (for Iceberg compatibility).
    async fn read_delete_file_positions(
        &self,
        state: &dyn Session,
        delete_file: &DuckLakeFileData,
    ) -> DataFusionResult<HashSet<i64>> {
        // Get the standard delete file schema
        let delete_schema = delete_file_schema();

        // Resolve the delete file path
        let resolved_delete_path = self.resolve_file_path(delete_file);

        // Create PartitionedFile with footer size hint if available
        let mut pf =
            PartitionedFile::new(&resolved_delete_path, delete_file.file_size_bytes as u64);
        if let Some(footer_size) = delete_file.footer_size {
            pf = pf.with_metadata_size_hint(footer_size as usize);
        }

        // Create file scan config for the delete file
        let file_scan_config = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            delete_schema,
            Arc::new(self.create_parquet_source()),
        )
        .with_file_group(FileGroup::new(vec![pf]))
        .build();

        // Use DataSourceExec directly to preserve our ParquetSource with encryption factory
        let exec = DataSourceExec::from_data_source(file_scan_config);

        // Execute and collect all batches
        let task_ctx = state.task_ctx();
        let stream = exec.execute(0, task_ctx)?;

        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<DataFusionResult<Vec<_>>>()?;

        // Extract all positions from all batches
        let mut positions = HashSet::new();
        for batch in batches {
            extract_deleted_positions_from_batch(&batch, &mut positions)?;
        }

        Ok(positions)
    }

    /// Build a single execution plan for all files without delete files
    ///
    /// Groups multiple files into a single efficient execution plan since they don't
    /// need delete filtering.
    async fn build_exec_for_files_without_deletes(
        &self,
        state: &dyn Session,
        files: &[&DuckLakeTableFile],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (read_schema, name_mapping) = self.get_schema_mapping(state).await?;

        let partitioned_files: Vec<PartitionedFile> = files
            .iter()
            .map(|table_file| {
                let resolved_path = self.resolve_file_path(&table_file.file);
                let mut pf =
                    PartitionedFile::new(&resolved_path, table_file.file.file_size_bytes as u64);

                // Apply footer size hint if available from DuckLake metadata
                // This reduces I/O from 2 reads to 1 read per file (especially beneficial for S3/MinIO)
                if let Some(footer_size) = table_file.file.footer_size {
                    pf = pf.with_metadata_size_hint(footer_size as usize);
                }

                pf
            })
            .collect();

        // Use read_schema (with original Parquet names) for reading
        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            read_schema.clone(),
            Arc::new(self.create_parquet_source()),
        )
        .with_limit(limit)
        .with_file_group(FileGroup::new(partitioned_files));

        // Apply projection if provided
        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()));
        }

        let file_scan_config = builder.build();
        // Use DataSourceExec directly to preserve our ParquetSource with encryption factory
        let parquet_exec: Arc<dyn ExecutionPlan> =
            DataSourceExec::from_data_source(file_scan_config);

        // Wrap with ColumnRenameExec if column names differ
        if !name_mapping.is_empty() {
            let output_schema = match projection {
                Some(indices) => Arc::new(self.schema.project(indices)?),
                None => self.schema.clone(),
            };
            Ok(Arc::new(ColumnRenameExec::new(
                parquet_exec,
                output_schema,
                name_mapping.clone(),
            )))
        } else {
            Ok(parquet_exec)
        }
    }

    /// Configure this table for write operations.
    ///
    /// This method enables write support by attaching a metadata writer and data path.
    /// Once configured, the table can handle INSERT INTO operations.
    ///
    /// # Arguments
    /// * `schema_name` - Name of the schema this table belongs to
    /// * `writer` - Metadata writer for catalog operations
    #[cfg(feature = "write")]
    pub fn with_writer(mut self, schema_name: String, writer: Arc<dyn MetadataWriter>) -> Self {
        self.schema_name = Some(schema_name);
        self.writer = Some(writer);
        self
    }

    /// Create a DELETE execution plan for this table.
    ///
    /// Returns an execution plan that, when executed, will:
    /// 1. Scan each data file and apply the filter
    /// 2. Collect positions of matching (non-deleted) rows
    /// 3. Write Parquet delete files
    /// 4. Register them in catalog metadata
    /// 5. Return the count of deleted rows
    ///
    /// If `filters` is empty, all rows are deleted.
    #[cfg(feature = "write")]
    pub async fn delete(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let writer = self.writer.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Table is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        // Pre-load existing delete positions for files that have delete files
        let mut existing_deletes = HashMap::new();
        for table_file in &self.table_files {
            if let Some(ref delete_file) = table_file.delete_file {
                let resolved_path = self.resolve_file_path(&table_file.file);
                let positions = self.read_delete_file_positions(state, delete_file).await?;
                existing_deletes.insert(resolved_path, positions);
            }
        }

        Ok(Arc::new(DuckLakeDeleteExec::new(
            self.table_id,
            self.table_name.clone(),
            self.schema.clone(),
            self.table_files.clone(),
            filters.to_vec(),
            Arc::clone(writer),
            self.object_store_url.clone(),
            self.table_path.clone(),
            existing_deletes,
        )))
    }

    /// Create an UPDATE execution plan for this table.
    ///
    /// Returns an execution plan that, when executed, will:
    /// 1. Scan each data file and apply the WHERE filter
    /// 2. Collect matching rows' full data and positions
    /// 3. Apply SET transformations to the matched rows
    /// 4. Write delete files for old rows
    /// 5. Write new data files with updated rows
    /// 6. Register both in catalog metadata
    /// 7. Return the count of updated rows
    ///
    /// If `filters` is empty, all rows are updated.
    #[cfg(feature = "write")]
    pub async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<crate::update_exec::UpdateAssignment>,
        filters: &[Expr],
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        use crate::update_exec::DuckLakeUpdateExec;

        let writer = self.writer.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Table is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        let schema_name = self.schema_name.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Schema name not set for writable table".to_string())
        })?;

        // Pre-load existing delete positions for files that have delete files
        let mut existing_deletes = HashMap::new();
        for table_file in &self.table_files {
            if let Some(ref delete_file) = table_file.delete_file {
                let resolved_path = self.resolve_file_path(&table_file.file);
                let positions = self.read_delete_file_positions(state, delete_file).await?;
                existing_deletes.insert(resolved_path, positions);
            }
        }

        Ok(Arc::new(DuckLakeUpdateExec::new(
            self.table_id,
            self.table_name.clone(),
            schema_name.clone(),
            self.schema.clone(),
            self.table_files.clone(),
            filters.to_vec(),
            assignments,
            Arc::clone(writer),
            self.object_store_url.clone(),
            self.table_path.clone(),
            existing_deletes,
        )))
    }

    /// Build a Parquet scan for a single file (no delete filtering).
    /// Used when virtual columns are requested (files must be scanned individually).
    async fn build_exec_for_single_file(
        &self,
        state: &dyn Session,
        table_file: &DuckLakeTableFile,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (read_schema, name_mapping) = self.get_schema_mapping(state).await?;
        let resolved_path = self.resolve_file_path(&table_file.file);
        let mut pf = PartitionedFile::new(&resolved_path, table_file.file.file_size_bytes as u64);
        if let Some(footer_size) = table_file.file.footer_size {
            pf = pf.with_metadata_size_hint(footer_size as usize);
        }
        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            read_schema.clone(),
            Arc::new(self.create_parquet_source()),
        )
        .with_limit(limit)
        .with_file_group(FileGroup::new(vec![pf]));
        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()));
        }
        let file_scan_config = builder.build();
        let parquet_exec: Arc<dyn ExecutionPlan> =
            DataSourceExec::from_data_source(file_scan_config);
        if !name_mapping.is_empty() {
            let output_schema = match projection {
                Some(indices) => Arc::new(self.schema.project(indices)?),
                None => self.schema.clone(),
            };
            Ok(Arc::new(ColumnRenameExec::new(
                parquet_exec,
                output_schema,
                name_mapping.clone(),
            )))
        } else {
            Ok(parquet_exec)
        }
    }

    /// Build an execution plan for a single file with delete filtering
    ///
    /// Creates a Parquet scan wrapped with a delete filter to exclude deleted rows.
    async fn build_exec_for_file_with_deletes(
        &self,
        state: &dyn Session,
        table_file: &DuckLakeTableFile,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (read_schema, name_mapping) = self.get_schema_mapping(state).await?;

        // Resolve the data file path for scanning
        let resolved_path = self.resolve_file_path(&table_file.file);

        // Create PartitionedFile with footer size hint if available
        let mut pf = PartitionedFile::new(&resolved_path, table_file.file.file_size_bytes as u64);
        if let Some(footer_size) = table_file.file.footer_size {
            pf = pf.with_metadata_size_hint(footer_size as usize);
        }

        // Use read_schema (with original Parquet names) for reading
        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            read_schema.clone(),
            Arc::new(self.create_parquet_source()),
        )
        .with_limit(limit)
        .with_file_group(FileGroup::new(vec![pf]));

        // Apply projection if provided
        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()));
        }

        let file_scan_config = builder.build();
        // Use DataSourceExec directly to preserve our ParquetSource with encryption factory
        let parquet_exec: Arc<dyn ExecutionPlan> =
            DataSourceExec::from_data_source(file_scan_config);

        // Wrap with delete filter - we know there's a delete file since we partitioned
        // The metadata already tells us which delete file goes with this data file
        let exec_after_delete: Arc<dyn ExecutionPlan> =
            if let Some(ref delete_file) = table_file.delete_file {
                let deleted_positions = self.read_delete_file_positions(state, delete_file).await?;

                if !deleted_positions.is_empty() {
                    Arc::new(DeleteFilterExec::new(
                        parquet_exec,
                        table_file.file.path.clone(),
                        Arc::new(deleted_positions),
                    ))
                } else {
                    parquet_exec
                }
            } else {
                parquet_exec
            };

        // Wrap with ColumnRenameExec if column names differ
        if !name_mapping.is_empty() {
            let output_schema = match projection {
                Some(indices) => Arc::new(self.schema.project(indices)?),
                None => self.schema.clone(),
            };
            Ok(Arc::new(ColumnRenameExec::new(
                exec_after_delete,
                output_schema,
                name_mapping.clone(),
            )))
        } else {
            Ok(exec_after_delete)
        }
    }
}

#[async_trait]
impl TableProvider for DuckLakeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.full_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        use datafusion::common::stats::Precision;
        use datafusion::common::{ColumnStatistics, Statistics};

        let file_stats = self
            .provider
            .get_file_column_stats(self.table_id, self.snapshot_id)
            .ok()?;

        if file_stats.is_empty() {
            return None;
        }

        // Build a map: column_name -> index in schema
        let col_name_to_idx: HashMap<&str, usize> = self
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.column_name.as_str(), idx))
            .collect();

        let num_cols = self.columns.len();
        let mut col_stats: Vec<ColumnStatistics> = vec![ColumnStatistics::new_unknown(); num_cols];

        // Track per-column aggregated null_count and min/max across all files
        let mut null_counts: Vec<i64> = vec![0; num_cols];
        let mut has_null_count: Vec<bool> = vec![false; num_cols];

        for fs in &file_stats {
            let Some(&col_idx) = col_name_to_idx.get(fs.column_name.as_str()) else {
                continue;
            };
            if col_idx >= num_cols {
                continue;
            }

            // Accumulate null counts
            if let Some(nc) = fs.null_count {
                null_counts[col_idx] += nc;
                has_null_count[col_idx] = true;
            }

            let data_type = self.schema.field(col_idx).data_type();

            // Update min
            if let Some(ref min_str) = fs.min_value
                && let Some(sv) = parse_stat_value(min_str, data_type)
            {
                col_stats[col_idx].min_value = match &col_stats[col_idx].min_value {
                    Precision::Absent => Precision::Inexact(sv),
                    Precision::Inexact(current) | Precision::Exact(current) => {
                        if sv < *current {
                            Precision::Inexact(sv)
                        } else {
                            col_stats[col_idx].min_value.clone()
                        }
                    },
                };
            }

            // Update max
            if let Some(ref max_str) = fs.max_value
                && let Some(sv) = parse_stat_value(max_str, data_type)
            {
                col_stats[col_idx].max_value = match &col_stats[col_idx].max_value {
                    Precision::Absent => Precision::Inexact(sv),
                    Precision::Inexact(current) | Precision::Exact(current) => {
                        if sv > *current {
                            Precision::Inexact(sv)
                        } else {
                            col_stats[col_idx].max_value.clone()
                        }
                    },
                };
            }
        }

        // Set null counts
        for (i, cs) in col_stats.iter_mut().enumerate() {
            if has_null_count[i] {
                cs.null_count = Precision::Inexact(null_counts[i] as usize);
            }
        }

        Some(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: col_stats,
        })
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Mark all filters as Inexact because we apply delete filters after the scan.
        // DataFusion will reapply these filters after DeleteFilterExec to ensure
        // correctness, but Parquet can still use them for:
        // - Row group pruning via statistics
        // - Page-level filtering with late materialization
        // - Bloom filter lookups (if available)
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let base_field_count = self.schema.fields().len();
        let filename_idx = base_field_count;
        let row_number_idx = base_field_count + 1;

        // Determine if any virtual columns are requested
        let (needs_virtual, include_filename, include_row_number) = match projection {
            Some(indices) => {
                let has_filename = indices.contains(&filename_idx);
                let has_row_number = indices.contains(&row_number_idx);
                (has_filename || has_row_number, has_filename, has_row_number)
            },
            None => (true, true, true), // SELECT * includes virtual columns
        };

        if !needs_virtual {
            // No virtual columns — use optimized grouped scan path
            let (files_with_deletes, files_without_deletes): (Vec<_>, Vec<_>) = self
                .table_files
                .iter()
                .partition(|tf| tf.delete_file.is_some());
            let mut execs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
            if !files_without_deletes.is_empty() {
                execs.push(
                    self.build_exec_for_files_without_deletes(
                        state,
                        &files_without_deletes,
                        projection,
                        limit,
                    )
                    .await?,
                );
            }
            for table_file in files_with_deletes {
                execs.push(
                    self.build_exec_for_file_with_deletes(state, table_file, projection, limit)
                        .await?,
                );
            }
            if execs.is_empty() {
                use datafusion::physical_plan::empty::EmptyExec;
                let projected_schema = match projection {
                    Some(indices) => Arc::new(self.schema.project(indices)?),
                    None => self.schema.clone(),
                };
                return Ok(Arc::new(EmptyExec::new(projected_schema)));
            }
            return combine_execution_plans(execs);
        }

        // Virtual columns requested — scan files individually
        // Build the "real" projection (base schema indices only)
        let real_projection: Option<Vec<usize>> = projection.map(|indices| {
            indices
                .iter()
                .filter(|&&idx| idx < base_field_count)
                .copied()
                .collect()
        });

        // Build VirtualColumnExec output schema: [real projected cols..., filename?, row_number?]
        let real_output_schema = match &real_projection {
            Some(indices) if !indices.is_empty() => Arc::new(self.schema.project(indices)?),
            Some(_) => Arc::new(Schema::empty()),
            None => self.schema.clone(),
        };
        let mut vc_fields = real_output_schema.fields().to_vec();
        if include_filename {
            vc_fields.push(Arc::new(Field::new(
                VIRTUAL_COL_FILENAME,
                DataType::Utf8,
                true,
            )));
        }
        if include_row_number {
            vc_fields.push(Arc::new(Field::new(
                VIRTUAL_COL_FILE_ROW_NUMBER,
                DataType::Int64,
                true,
            )));
        }
        let vc_output_schema = Arc::new(Schema::new(vc_fields));
        let real_proj_ref = real_projection.as_ref();

        let mut execs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        for table_file in &self.table_files {
            let resolved_path = self.resolve_file_path(&table_file.file);
            let file_exec = if table_file.delete_file.is_some() {
                self.build_exec_for_file_with_deletes(state, table_file, real_proj_ref, limit)
                    .await?
            } else {
                self.build_exec_for_single_file(state, table_file, real_proj_ref, limit)
                    .await?
            };
            execs.push(Arc::new(VirtualColumnExec::new(
                file_exec,
                resolved_path,
                include_filename,
                include_row_number,
                Arc::clone(&vc_output_schema),
            )));
        }

        if execs.is_empty() {
            use datafusion::physical_plan::empty::EmptyExec;
            let projected_schema = match projection {
                Some(indices) => Arc::new(self.full_schema.project(indices)?),
                None => self.full_schema.clone(),
            };
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let combined = combine_execution_plans(execs)?;

        // Check if we need to reorder columns (virtual cols not at the end of projection)
        if let Some(indices) = projection {
            // Build expected order: real indices first, then virtual
            let mut expected: Vec<usize> = Vec::new();
            for &idx in indices {
                if idx < base_field_count {
                    expected.push(idx);
                }
            }
            if include_filename {
                expected.push(filename_idx);
            }
            if include_row_number {
                expected.push(row_number_idx);
            }

            if indices != expected.as_slice() {
                // Need to reorder: map each requested index to its position in vc_output_schema
                let mut real_col_pos = 0usize;
                let mut index_to_vc_pos: HashMap<usize, usize> = HashMap::new();
                for &idx in indices {
                    if idx < base_field_count {
                        index_to_vc_pos.insert(idx, real_col_pos);
                        real_col_pos += 1;
                    }
                }
                let mut vc_pos = real_col_pos;
                if include_filename {
                    index_to_vc_pos.insert(filename_idx, vc_pos);
                    vc_pos += 1;
                }
                if include_row_number {
                    index_to_vc_pos.insert(row_number_idx, vc_pos);
                }

                use datafusion::physical_expr::expressions::Column;
                use datafusion::physical_plan::projection::ProjectionExec;
                let proj_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
                    indices
                        .iter()
                        .map(|&idx| {
                            let vc_idx = index_to_vc_pos[&idx];
                            let name = vc_output_schema.field(vc_idx).name().clone();
                            (
                                Arc::new(Column::new(&name, vc_idx))
                                    as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                                name,
                            )
                        })
                        .collect();
                return Ok(Arc::new(ProjectionExec::try_new(proj_exprs, combined)?));
            }
        }

        Ok(combined)
    }

    #[cfg(feature = "write")]
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let writer = self.writer.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Table is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        let schema_name = self.schema_name.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Schema name not set for writable table".to_string())
        })?;

        let write_mode = match insert_op {
            InsertOp::Append => WriteMode::Append,
            InsertOp::Overwrite | InsertOp::Replace => WriteMode::Replace,
        };

        // Strip virtual columns from input if present (DataFusion may include them
        // because schema() returns full_schema with virtual columns)
        let base_col_count = self.schema.fields().len();
        let actual_input = if input.schema().fields().len() > base_col_count {
            let exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = (0
                ..base_col_count)
                .map(|i| {
                    let name = input.schema().field(i).name().to_string();
                    (
                        Arc::new(PhysicalColumn::new(&name, i))
                            as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                        name,
                    )
                })
                .collect();
            Arc::new(ProjectionExec::try_new(exprs, input)?) as Arc<dyn ExecutionPlan>
        } else {
            input
        };

        Ok(Arc::new(DuckLakeInsertExec::new(
            actual_input,
            Arc::clone(writer),
            schema_name.clone(),
            self.table_name.clone(),
            Arc::clone(&self.schema),
            write_mode,
            self.object_store_url.clone(),
        )))
    }
}

/// Parse a string-encoded statistic value into a DataFusion ScalarValue.
///
/// Supports common numeric, string, and boolean types. Returns None for
/// unsupported types or parse failures.
fn parse_stat_value(s: &str, data_type: &DataType) -> Option<datafusion::common::ScalarValue> {
    use datafusion::common::ScalarValue;

    match data_type {
        DataType::Boolean => s
            .parse::<bool>()
            .ok()
            .map(|v| ScalarValue::Boolean(Some(v))),
        DataType::Int8 => s.parse::<i8>().ok().map(|v| ScalarValue::Int8(Some(v))),
        DataType::Int16 => s.parse::<i16>().ok().map(|v| ScalarValue::Int16(Some(v))),
        DataType::Int32 => s.parse::<i32>().ok().map(|v| ScalarValue::Int32(Some(v))),
        DataType::Int64 => s.parse::<i64>().ok().map(|v| ScalarValue::Int64(Some(v))),
        DataType::UInt8 => s.parse::<u8>().ok().map(|v| ScalarValue::UInt8(Some(v))),
        DataType::UInt16 => s.parse::<u16>().ok().map(|v| ScalarValue::UInt16(Some(v))),
        DataType::UInt32 => s.parse::<u32>().ok().map(|v| ScalarValue::UInt32(Some(v))),
        DataType::UInt64 => s.parse::<u64>().ok().map(|v| ScalarValue::UInt64(Some(v))),
        DataType::Float32 => s.parse::<f32>().ok().map(|v| ScalarValue::Float32(Some(v))),
        DataType::Float64 => s.parse::<f64>().ok().map(|v| ScalarValue::Float64(Some(v))),
        DataType::Utf8 => Some(ScalarValue::Utf8(Some(s.to_string()))),
        DataType::LargeUtf8 => Some(ScalarValue::LargeUtf8(Some(s.to_string()))),
        DataType::Date32 => s.parse::<i32>().ok().map(|v| ScalarValue::Date32(Some(v))),
        DataType::Date64 => s.parse::<i64>().ok().map(|v| ScalarValue::Date64(Some(v))),
        _ => None,
    }
}

/// Combines multiple execution plans into a single plan
fn combine_execution_plans(
    execs: Vec<Arc<dyn ExecutionPlan>>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    if execs.len() == 1 {
        Ok(execs.into_iter().next().unwrap())
    } else {
        use datafusion::physical_plan::union::UnionExec;
        UnionExec::try_new(execs)
    }
}

/// Extract deleted row positions from a delete file RecordBatch
///
/// Delete files have schema: (file_path: VARCHAR, pos: INT64)
/// We only extract the "pos" column - the "file_path" column is metadata/documentation
/// only (for Iceberg compatibility). The metadata catalog already tells us which delete
/// file is associated with which data file.
fn extract_deleted_positions_from_batch(
    batch: &RecordBatch,
    positions: &mut HashSet<i64>,
) -> DataFusionResult<()> {
    // Get the pos column index by name (not magic number)
    let schema = batch.schema();
    let pos_idx = schema.index_of(DELETE_POS_COL)?;

    // Get the pos column
    let pos_array = batch
        .column(pos_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{} column not found or wrong type", DELETE_POS_COL))
        })?;

    // Extract all non-null positions
    for i in 0..batch.num_rows() {
        if !pos_array.is_null(i) {
            positions.insert(pos_array.value(i));
        }
    }

    Ok(())
}
