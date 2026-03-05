//! DuckLake table provider implementation

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::Result;
use crate::column_rename::ColumnRenameExec;
use crate::delete_filter::DeleteFilterExec;
use crate::metadata_provider::{
    DuckLakeFileData, DuckLakeTableColumn, DuckLakeTableFile, InlinedDataRow, MetadataProvider,
    PartitionColumn,
};
use crate::path_resolver::resolve_path;
use crate::types::{
    build_arrow_schema, build_read_schema_with_field_id_mapping, extract_parquet_field_ids,
};
use crate::virtual_column_exec::{
    VIRTUAL_COL_FILE_INDEX, VIRTUAL_COL_FILE_ROW_NUMBER, VIRTUAL_COL_FILENAME, VIRTUAL_COL_ROWID,
    VIRTUAL_COL_SNAPSHOT_ID, VirtualColumnExec, VirtualColumnFileInfo, VirtualColumnSet,
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

/// Validate and convert file_size_bytes from i64 (as stored in DuckLake metadata) to u64.
///
/// DuckLake stores file sizes as signed integers in SQL. A negative value indicates
/// corrupt or invalid metadata. Without this check, a negative i64 cast to u64 would
/// wrap to a huge value (e.g., -1 becomes u64::MAX), causing confusing downstream errors.
pub(crate) fn validated_file_size(file_size_bytes: i64, file_path: &str) -> DataFusionResult<u64> {
    u64::try_from(file_size_bytes).map_err(|_| {
        DataFusionError::Execution(format!(
            "Invalid file_size_bytes ({}) for file '{}': value must be non-negative",
            file_size_bytes, file_path
        ))
    })
}

/// Returns the expected schema for DuckLake delete files
///
/// Delete files have a standard schema: (file_path: VARCHAR, pos: INT64)
/// The file_path column is metadata/documentation only (for Iceberg compatibility).
/// The pos column contains the row positions to delete.
pub fn delete_file_schema() -> SchemaRef {
    // R4-S-024: Add PARQUET:field_id metadata matching DuckDB's sentinel values
    // for delete file columns (0x7FFFFFFE for file_path, 0x7FFFFFFD for pos)
    let mut file_path_metadata = HashMap::new();
    file_path_metadata.insert("PARQUET:field_id".to_string(), "2147483646".to_string()); // 0x7FFFFFFE
    let mut pos_metadata = HashMap::new();
    pos_metadata.insert("PARQUET:field_id".to_string(), "2147483645".to_string()); // 0x7FFFFFFD

    Arc::new(Schema::new(vec![
        Field::new(DELETE_FILE_PATH_COL, DataType::Utf8, false).with_metadata(file_path_metadata),
        Field::new(DELETE_POS_COL, DataType::Int64, false).with_metadata(pos_metadata),
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
    /// Cached exact row count from metadata (None if not available)
    cached_row_count: Option<i64>,
    /// Partition columns for this table (empty if not partitioned)
    partition_columns: Vec<PartitionColumn>,
    /// Partition values per file: data_file_id -> [(partition_key_index, value)]
    file_partition_values: HashMap<i64, Vec<(i32, Option<String>)>>,
    /// Inlined data rows stored directly in the catalog database
    inlined_data: Vec<InlinedDataRow>,
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
        let columns = provider.get_table_structure(table_id, snapshot_id)?;
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
            fields.push(Arc::new(Field::new(
                VIRTUAL_COL_ROWID,
                DataType::Int64,
                true,
            )));
            fields.push(Arc::new(Field::new(
                VIRTUAL_COL_SNAPSHOT_ID,
                DataType::Int64,
                true,
            )));
            fields.push(Arc::new(Field::new(
                VIRTUAL_COL_FILE_INDEX,
                DataType::UInt64,
                true,
            )));
            Arc::new(Schema::new(fields))
        };
        let table_files = provider.get_table_files_for_select(table_id, snapshot_id)?;

        // Load row count from metadata for COUNT(*) optimization
        let cached_row_count = match provider.get_table_row_count(table_id, snapshot_id) {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!(
                    table_id,
                    snapshot_id,
                    error = %e,
                    "Failed to load row count from metadata; COUNT(*) optimization disabled"
                );
                None
            },
        };

        // Load partition metadata for partition pruning
        let partition_columns = match provider.get_partition_columns(table_id, snapshot_id) {
            Ok(cols) => cols,
            Err(e) => {
                tracing::warn!(
                    table_id,
                    snapshot_id,
                    error = %e,
                    "Failed to load partition columns; partition pruning disabled"
                );
                Vec::new()
            },
        };
        let file_partition_values = if !partition_columns.is_empty() {
            let raw_values = match provider.get_file_partition_values(table_id, snapshot_id) {
                Ok(vals) => vals,
                Err(e) => {
                    tracing::warn!(
                        table_id,
                        snapshot_id,
                        error = %e,
                        "Failed to load file partition values; partition pruning disabled"
                    );
                    Vec::new()
                },
            };
            let mut map: HashMap<i64, Vec<(i32, Option<String>)>> = HashMap::new();
            for v in raw_values {
                map.entry(v.data_file_id)
                    .or_default()
                    .push((v.partition_key_index, v.partition_value));
            }
            map
        } else {
            HashMap::new()
        };

        // Load inlined data from catalog database
        let inlined_data = match provider.get_inlined_data(table_id, snapshot_id) {
            Ok(data) => data,
            Err(e) => {
                tracing::warn!(
                    table_id,
                    snapshot_id,
                    error = %e,
                    "Failed to load inlined data from catalog"
                );
                Vec::new()
            },
        };

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
                )?;
                builder.add_file(&resolved_path, table_file.file.encryption_key.as_deref());

                // Also add delete file encryption key if present
                if let Some(ref delete_file) = table_file.delete_file {
                    let resolved_delete_path =
                        resolve_path(&table_path, &delete_file.path, delete_file.path_is_relative)?;
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
            cached_row_count,
            partition_columns,
            file_partition_values,
            inlined_data,
            #[cfg(feature = "encryption")]
            encryption_factory,
            schema_mapping_cache: OnceCell::new(),
            #[cfg(feature = "write")]
            schema_name: None,
            #[cfg(feature = "write")]
            writer: None,
        })
    }

    /// Build a MemTable-based plan for inlined data rows.
    ///
    /// Converts the cached inlined data rows into Arrow RecordBatches and wraps them
    /// in a MemTable scan for inclusion in the scan plan.
    async fn build_inlined_data_exec(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if self.inlined_data.is_empty() {
            return Ok(None);
        }

        let schema = &self.schema;
        let num_rows = self.inlined_data.len();

        // R5-S-061: Pre-build column name→index maps to avoid O(n²) lookup.
        // Each row's column_names are mapped to positions once, then field
        // lookups are O(1) instead of O(columns) per row per field.
        let row_col_maps: Vec<HashMap<&str, usize>> = self
            .inlined_data
            .iter()
            .map(|row| {
                row.column_names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| (name.as_str(), i))
                    .collect()
            })
            .collect();

        // Build column arrays from inlined data
        let mut column_arrays: Vec<Arc<dyn Array>> = Vec::new();
        for field in schema.fields().iter() {
            let col_name = field.name();
            let data_type = field.data_type();

            // Collect values for this column from all inlined rows
            let mut string_values: Vec<Option<String>> = Vec::with_capacity(num_rows);
            for (row, col_map) in self.inlined_data.iter().zip(row_col_maps.iter()) {
                let value = col_map
                    .get(col_name.as_str())
                    .and_then(|&pos| row.values.get(pos))
                    .and_then(|v| v.clone());
                string_values.push(value);
            }

            // Parse string values into the appropriate Arrow array type
            let array = crate::parse_values::parse_string_values_to_array(
                &string_values,
                data_type,
                crate::parse_values::ParseMode::Lenient,
            )?;
            column_arrays.push(array);
        }

        let batch = RecordBatch::try_new(schema.clone(), column_arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mem_table =
            datafusion::datasource::memory::MemTable::try_new(schema.clone(), vec![vec![batch]])?;

        let exec = mem_table.scan(state, projection, &[], None).await?;

        Ok(Some(exec))
    }

    /// Resolve a file path (data or delete file) to its absolute path
    fn resolve_file_path(&self, file: &DuckLakeFileData) -> DataFusionResult<String> {
        resolve_path(&self.table_path, &file.path, file.path_is_relative)
            .map_err(|e| DataFusionError::External(Box::new(e)))
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

                let resolved_path = self.resolve_file_path(&first_file.file)?;
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
    ///
    /// # Errors
    /// Returns an error if the delete file is missing from storage. A missing delete
    /// file indicates data corruption — without it, deleted rows would silently
    /// reappear in query results.
    async fn read_delete_file_positions(
        &self,
        state: &dyn Session,
        delete_file: &DuckLakeFileData,
    ) -> DataFusionResult<HashSet<i64>> {
        // Get the standard delete file schema
        let delete_schema = delete_file_schema();

        // Resolve the delete file path
        let resolved_delete_path = self.resolve_file_path(delete_file)?;

        // Create PartitionedFile with footer size hint if available
        let mut pf = PartitionedFile::new(
            &resolved_delete_path,
            validated_file_size(delete_file.file_size_bytes, &resolved_delete_path)?,
        );
        if let Some(footer_size) = delete_file.footer_size
            && footer_size > 0
            && let Ok(hint) = usize::try_from(footer_size)
        {
            pf = pf.with_metadata_size_hint(hint);
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
            .collect::<DataFusionResult<Vec<_>>>()
            .map_err(|e| {
                if is_object_store_not_found(&e) {
                    DataFusionError::Execution(format!(
                        "Delete file '{}' referenced in catalog metadata was not found. This may indicate catalog corruption or that the file was deleted outside of DuckLake.",
                        resolved_delete_path
                    ))
                } else {
                    e
                }
            })?;

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
                let resolved_path = self.resolve_file_path(&table_file.file)?;
                let mut pf = PartitionedFile::new(
                    &resolved_path,
                    validated_file_size(table_file.file.file_size_bytes, &resolved_path)?,
                );

                // Apply footer size hint if available from DuckLake metadata
                // This reduces I/O from 2 reads to 1 read per file (especially beneficial for S3/MinIO)
                if let Some(footer_size) = table_file.file.footer_size
                    && footer_size > 0
                    && let Ok(hint) = usize::try_from(footer_size)
                {
                    pf = pf.with_metadata_size_hint(hint);
                }

                Ok(pf)
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

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
    #[must_use]
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
                let resolved_path = self.resolve_file_path(&table_file.file)?;
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
            Arc::clone(&self.object_store_url),
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

        // Pre-load existing delete positions for files that have delete files
        let mut existing_deletes = HashMap::new();
        for table_file in &self.table_files {
            if let Some(ref delete_file) = table_file.delete_file {
                let resolved_path = self.resolve_file_path(&table_file.file)?;
                let positions = self.read_delete_file_positions(state, delete_file).await?;
                existing_deletes.insert(resolved_path, positions);
            }
        }

        let column_ids: Vec<i64> = self.columns.iter().map(|c| c.column_id).collect();

        Ok(Arc::new(DuckLakeUpdateExec::new(
            self.table_id,
            self.table_name.clone(),
            self.schema.clone(),
            column_ids,
            self.table_files.clone(),
            filters.to_vec(),
            assignments,
            Arc::clone(writer),
            Arc::clone(&self.object_store_url),
            self.table_path.clone(),
            existing_deletes,
        )))
    }

    /// Create a MERGE INTO execution plan for this table.
    ///
    /// Returns an execution plan that, when executed, will:
    /// 1. Scan each target data file and join with source data on key columns
    /// 2. For matched rows: apply the matched action (UPDATE or DELETE)
    /// 3. For unmatched source rows: insert them as new data
    /// 4. Write delete files for matched rows and new data files for updated/inserted rows
    /// 5. Return the total count of affected rows
    #[cfg(feature = "write")]
    pub async fn merge(
        &self,
        state: &dyn Session,
        source_batches: Vec<RecordBatch>,
        join_key_pairs: Vec<(usize, usize)>,
        matched_action: Option<crate::merge_exec::MergeMatchedAction>,
        insert_unmatched: bool,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        use crate::merge_exec::DuckLakeMergeExec;

        let writer = self.writer.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Table is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        let mut existing_deletes = HashMap::new();
        for table_file in &self.table_files {
            if let Some(ref delete_file) = table_file.delete_file {
                let resolved_path = self.resolve_file_path(&table_file.file)?;
                let positions = self.read_delete_file_positions(state, delete_file).await?;
                existing_deletes.insert(resolved_path, positions);
            }
        }

        let column_ids: Vec<i64> = self.columns.iter().map(|c| c.column_id).collect();

        Ok(Arc::new(DuckLakeMergeExec::new(
            self.table_id,
            self.table_name.clone(),
            self.schema.clone(),
            column_ids,
            self.table_files.clone(),
            source_batches,
            join_key_pairs,
            matched_action,
            insert_unmatched,
            Arc::clone(writer),
            Arc::clone(&self.object_store_url),
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
        let resolved_path = self.resolve_file_path(&table_file.file)?;
        let mut pf = PartitionedFile::new(
            &resolved_path,
            validated_file_size(table_file.file.file_size_bytes, &resolved_path)?,
        );
        if let Some(footer_size) = table_file.file.footer_size
            && footer_size > 0
            && let Ok(hint) = usize::try_from(footer_size)
        {
            pf = pf.with_metadata_size_hint(hint);
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
        let resolved_path = self.resolve_file_path(&table_file.file)?;

        // Create PartitionedFile with footer size hint if available
        let mut pf = PartitionedFile::new(
            &resolved_path,
            validated_file_size(table_file.file.file_size_bytes, &resolved_path)?,
        );
        if let Some(footer_size) = table_file.file.footer_size
            && footer_size > 0
            && let Ok(hint) = usize::try_from(footer_size)
        {
            pf = pf.with_metadata_size_hint(hint);
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

impl DuckLakeTable {
    /// Filter table files based on partition pruning.
    ///
    /// Extracts simple equality filters (column = literal) that reference partition columns
    /// and prunes files whose partition values don't match.
    /// Only applies to tables with partition metadata and identity transforms.
    fn prune_files_by_partition<'a>(
        &'a self,
        files: &'a [DuckLakeTableFile],
        filters: &[Expr],
    ) -> Vec<&'a DuckLakeTableFile> {
        if self.partition_columns.is_empty() || self.file_partition_values.is_empty() {
            return files.iter().collect();
        }

        // Extract equality constraints on partition columns from filters
        let partition_filters =
            extract_partition_equality_filters(filters, &self.partition_columns);
        if partition_filters.is_empty() {
            return files.iter().collect();
        }

        files
            .iter()
            .filter(|tf| {
                let Some(file_id) = tf.data_file_id else {
                    // No file_id means we can't check partition values — include the file
                    return true;
                };
                let Some(values) = self.file_partition_values.get(&file_id) else {
                    // No partition values recorded for this file — include it to be safe
                    return true;
                };

                // Check all partition filter constraints
                for (key_index, expected_value) in &partition_filters {
                    let file_value = values.iter().find(|(ki, _)| ki == key_index);
                    match file_value {
                        Some((_, actual_value)) => {
                            let matches = match actual_value.as_deref() {
                                Some(actual) => partition_values_equal(actual, expected_value),
                                None => false,
                            };
                            if !matches {
                                return false; // Partition value doesn't match
                            }
                        },
                        None => {
                            // No partition value for this key — can't prune, include the file
                        },
                    }
                }
                true
            })
            .collect()
    }
}

/// Per-file column statistics for pruning decisions
struct FileStats {
    /// column_name -> (min_value, max_value)
    columns: HashMap<
        String,
        (
            Option<datafusion::common::ScalarValue>,
            Option<datafusion::common::ScalarValue>,
        ),
    >,
}

impl DuckLakeTable {
    /// Prune files based on per-file column statistics.
    ///
    /// Loads file-level column stats from metadata and evaluates filter predicates
    /// against each file's min/max range. Files where no rows can possibly match
    /// are excluded.
    fn prune_files_by_stats<'a>(
        &'a self,
        files: Vec<&'a DuckLakeTableFile>,
        filters: &[Expr],
    ) -> Vec<&'a DuckLakeTableFile> {
        if files.is_empty() || filters.is_empty() {
            return files;
        }

        // Load per-file column stats
        let raw_stats = match self
            .provider
            .get_file_column_stats(self.table_id, self.snapshot_id)
        {
            Ok(stats) if !stats.is_empty() => stats,
            _ => return files, // No stats available — can't prune
        };

        // Build column name -> data type map from schema
        let col_type_map: HashMap<&str, &DataType> = self
            .columns
            .iter()
            .zip(self.schema.fields().iter())
            .map(|(col, field)| (col.column_name.as_str(), field.data_type()))
            .collect();

        // Build per-file stats: data_file_id -> FileStats
        let mut file_stats_map: HashMap<i64, FileStats> = HashMap::new();
        for stat in &raw_stats {
            let data_type = match col_type_map.get(stat.column_name.as_str()) {
                Some(dt) => dt,
                None => continue,
            };

            let min_sv = stat
                .min_value
                .as_deref()
                .and_then(|s| parse_stat_value(s, data_type));
            let max_sv = stat
                .max_value
                .as_deref()
                .and_then(|s| parse_stat_value(s, data_type));

            file_stats_map
                .entry(stat.data_file_id)
                .or_insert_with(|| FileStats {
                    columns: HashMap::new(),
                })
                .columns
                .insert(stat.column_name.clone(), (min_sv, max_sv));
        }

        // If no stats loaded, can't prune
        if file_stats_map.is_empty() {
            return files;
        }

        // Extract prunable filter predicates
        let pruning_predicates = extract_pruning_predicates(filters);
        if pruning_predicates.is_empty() {
            return files;
        }

        files
            .into_iter()
            .filter(|tf| {
                let Some(file_id) = tf.data_file_id else {
                    return true; // No file_id — can't check stats
                };
                let Some(stats) = file_stats_map.get(&file_id) else {
                    return true; // No stats for this file — include it
                };

                // File is included unless ALL rows are definitively excluded
                for pred in &pruning_predicates {
                    if file_definitely_excluded(stats, pred) {
                        return false;
                    }
                }
                true
            })
            .collect()
    }
}

/// A simple pruning predicate extracted from a filter expression.
struct PruningPredicate {
    column_name: String,
    op: datafusion::logical_expr::Operator,
    value: datafusion::common::ScalarValue,
}

/// Extract simple comparison predicates that can be used for file pruning.
///
/// Supports: column op literal and literal op column for =, <, >, <=, >=, !=
fn extract_pruning_predicates(filters: &[Expr]) -> Vec<PruningPredicate> {
    use datafusion::logical_expr::Operator;

    let mut result = Vec::new();

    for filter in filters {
        if let Expr::BinaryExpr(binary) = filter {
            match binary.op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {},
                _ => continue,
            }

            // Try column op literal
            if let (Expr::Column(col), Expr::Literal(scalar, _)) =
                (binary.left.as_ref(), binary.right.as_ref())
            {
                if !scalar.is_null() {
                    result.push(PruningPredicate {
                        column_name: col.name.clone(),
                        op: binary.op,
                        value: scalar.clone(),
                    });
                }
            }
            // Try literal op column (flip the operator)
            else if let (Expr::Literal(scalar, _), Expr::Column(col)) =
                (binary.left.as_ref(), binary.right.as_ref())
            {
                if !scalar.is_null() {
                    let flipped_op = match binary.op {
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        other => other, // Eq and NotEq are symmetric
                    };
                    result.push(PruningPredicate {
                        column_name: col.name.clone(),
                        op: flipped_op,
                        value: scalar.clone(),
                    });
                }
            }
        }
    }

    result
}

/// Check if a file can be definitively excluded based on its column stats and a predicate.
///
/// Returns true if the file's min/max stats prove no rows can match the predicate.
fn file_definitely_excluded(stats: &FileStats, pred: &PruningPredicate) -> bool {
    use datafusion::logical_expr::Operator;

    let Some((min_val, max_val)) = stats.columns.get(&pred.column_name) else {
        return false; // No stats for this column — can't exclude
    };

    match pred.op {
        // column = value: exclude if value < min or value > max
        Operator::Eq => {
            if let Some(min) = min_val {
                if pred.value < *min {
                    return true;
                }
            }
            if let Some(max) = max_val {
                if pred.value > *max {
                    return true;
                }
            }
            false
        },
        // column != value: exclude only if min == max == value (all rows have this value)
        Operator::NotEq => {
            if let (Some(min), Some(max)) = (min_val, max_val) {
                min == max && pred.value == *min
            } else {
                false
            }
        },
        // column < value: exclude if min >= value
        Operator::Lt => {
            if let Some(min) = min_val {
                pred.value <= *min
            } else {
                false
            }
        },
        // column <= value: exclude if min > value
        Operator::LtEq => {
            if let Some(min) = min_val {
                pred.value < *min
            } else {
                false
            }
        },
        // column > value: exclude if max <= value
        Operator::Gt => {
            if let Some(max) = max_val {
                pred.value >= *max
            } else {
                false
            }
        },
        // column >= value: exclude if max < value
        Operator::GtEq => {
            if let Some(max) = max_val {
                pred.value > *max
            } else {
                false
            }
        },
        _ => false,
    }
}

/// Extract equality filters on partition columns from filter expressions.
///
/// Returns a list of (partition_key_index, string_value) pairs for simple
/// `column = literal` or `literal = column` equality expressions where:
/// - The column matches a partition column name
/// - The partition column uses an identity transform (or no transform)
/// - The literal value can be converted to a string for comparison
fn extract_partition_equality_filters(
    filters: &[Expr],
    partition_columns: &[PartitionColumn],
) -> Vec<(i32, String)> {
    use datafusion::logical_expr::Operator;

    let mut result = Vec::new();

    for filter in filters {
        if let Expr::BinaryExpr(binary) = filter {
            if binary.op != Operator::Eq {
                continue;
            }

            // Try both orderings: column = literal and literal = column
            let (col_name, lit_value) = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), Expr::Literal(scalar, _)) => (&col.name, scalar),
                (Expr::Literal(scalar, _), Expr::Column(col)) => (&col.name, scalar),
                _ => continue,
            };

            // Check if this column is a partition column with identity transform
            if let Some(pc) = partition_columns
                .iter()
                .find(|pc| &pc.column_name == col_name)
            {
                let is_identity = pc.transform.is_none()
                    || pc
                        .transform
                        .as_deref()
                        .is_some_and(|t| t.is_empty() || t.eq_ignore_ascii_case("identity"));
                if !is_identity {
                    continue; // Non-identity transforms require special handling
                }

                // Convert ScalarValue to string for comparison with partition_value
                if let Some(val_str) = scalar_value_to_partition_string(lit_value) {
                    result.push((pc.partition_key_index, val_str));
                }
            }
        }
    }

    result
}

/// Convert a DataFusion ScalarValue to a string for partition value comparison.
///
/// Returns None for null values or unsupported types.
fn scalar_value_to_partition_string(value: &datafusion::common::ScalarValue) -> Option<String> {
    use datafusion::common::ScalarValue;

    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        ScalarValue::Int8(Some(v)) => Some(v.to_string()),
        ScalarValue::Int16(Some(v)) => Some(v.to_string()),
        ScalarValue::Int32(Some(v)) => Some(v.to_string()),
        ScalarValue::Int64(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
        ScalarValue::Float32(Some(v)) => Some(v.to_string()),
        ScalarValue::Float64(Some(v)) => Some(v.to_string()),
        ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
        // R5-S-005: Format dates as ISO 8601 strings (YYYY-MM-DD) instead of
        // raw epoch-day/ms integers. DuckDB stores partition values as ISO date
        // strings, so using integer representations breaks cross-engine partition pruning.
        ScalarValue::Date32(Some(v)) => arrow::temporal_conversions::date32_to_datetime(*v)
            .map(|dt| dt.format("%Y-%m-%d").to_string()),
        ScalarValue::Date64(Some(v)) => arrow::temporal_conversions::date64_to_datetime(*v)
            .map(|dt| dt.format("%Y-%m-%d").to_string()),
        _ => None,
    }
}

/// Type-aware comparison of partition values (R7-S-005).
///
/// Tries numeric (f64) comparison first so that "10" and "10.0" are considered
/// equal and ordering is numeric rather than lexicographic. Falls back to
/// exact string comparison for non-numeric values.
fn partition_values_equal(actual: &str, expected: &str) -> bool {
    // Fast path: exact string match
    if actual == expected {
        return true;
    }
    // Try numeric comparison for values that parse as numbers
    if let (Ok(a), Ok(b)) = (actual.parse::<f64>(), expected.parse::<f64>()) {
        return a == b;
    }
    false
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

        // Set null counts (clamp negative values to 0 to avoid wrapping)
        for (i, cs) in col_stats.iter_mut().enumerate() {
            if has_null_count[i] {
                cs.null_count = Precision::Inexact(null_counts[i].max(0) as usize);
            }
        }

        let num_rows = match self.cached_row_count {
            Some(count) if count >= 0 => {
                Precision::Exact(usize::try_from(count).unwrap_or(usize::MAX))
            },
            _ => Precision::Absent,
        };

        // Append unknown stats for virtual columns so column_statistics length
        // matches full_schema (which schema() returns)
        let virtual_col_count = self.full_schema.fields().len() - self.columns.len();
        for _ in 0..virtual_col_count {
            col_stats.push(ColumnStatistics::new_unknown());
        }

        Some(Statistics {
            num_rows,
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let base_field_count = self.schema.fields().len();
        let filename_idx = base_field_count;
        let row_number_idx = base_field_count + 1;
        let rowid_idx = base_field_count + 2;
        let snapshot_id_idx = base_field_count + 3;
        let file_index_idx = base_field_count + 4;

        // Apply partition pruning to filter out files that don't match partition filters
        let partition_pruned = self.prune_files_by_partition(&self.table_files, filters);

        // Apply stats-based file pruning using per-file column statistics
        let active_files = self.prune_files_by_stats(partition_pruned, filters);

        // Determine which virtual columns are requested
        let included = match projection {
            Some(indices) => VirtualColumnSet {
                filename: indices.contains(&filename_idx),
                file_row_number: indices.contains(&row_number_idx),
                rowid: indices.contains(&rowid_idx),
                snapshot_id: indices.contains(&snapshot_id_idx),
                file_index: indices.contains(&file_index_idx),
            },
            None => VirtualColumnSet {
                filename: true,
                file_row_number: true,
                rowid: true,
                snapshot_id: true,
                file_index: true,
            },
        };
        let needs_virtual = included.any();

        if !needs_virtual {
            // No virtual columns — use optimized grouped scan path
            let (files_with_deletes, files_without_deletes): (
                Vec<&DuckLakeTableFile>,
                Vec<&DuckLakeTableFile>,
            ) = active_files
                .iter()
                .copied()
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
                    // Don't push limit into Parquet scan for files with deletes:
                    // DeleteFilterExec may remove rows after the scan, yielding fewer than N.
                    // DataFusion will apply the limit after DeleteFilterExec.
                    self.build_exec_for_file_with_deletes(state, table_file, projection, None)
                        .await?,
                );
            }
            // Add inlined data if available
            if let Some(inlined_exec) = self.build_inlined_data_exec(state, projection).await? {
                execs.push(inlined_exec);
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

        // Build VirtualColumnExec output schema: [real projected cols..., virtual cols...]
        let real_output_schema = match &real_projection {
            Some(indices) if !indices.is_empty() => Arc::new(self.schema.project(indices)?),
            Some(_) => Arc::new(Schema::empty()),
            None => self.schema.clone(),
        };
        let mut vc_fields = real_output_schema.fields().to_vec();
        if included.filename {
            vc_fields.push(Arc::new(Field::new(
                VIRTUAL_COL_FILENAME,
                DataType::Utf8,
                true,
            )));
        }
        if included.file_row_number {
            vc_fields.push(Arc::new(Field::new(
                VIRTUAL_COL_FILE_ROW_NUMBER,
                DataType::Int64,
                true,
            )));
        }
        if included.rowid {
            vc_fields.push(Arc::new(Field::new(
                VIRTUAL_COL_ROWID,
                DataType::Int64,
                true,
            )));
        }
        if included.snapshot_id {
            vc_fields.push(Arc::new(Field::new(
                VIRTUAL_COL_SNAPSHOT_ID,
                DataType::Int64,
                true,
            )));
        }
        if included.file_index {
            vc_fields.push(Arc::new(Field::new(
                VIRTUAL_COL_FILE_INDEX,
                DataType::UInt64,
                true,
            )));
        }
        let vc_output_schema = Arc::new(Schema::new(vc_fields));
        let real_proj_ref = real_projection.as_ref();

        let mut execs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        for (file_idx, table_file) in active_files.iter().enumerate() {
            let resolved_path = self.resolve_file_path(&table_file.file)?;
            let file_info = VirtualColumnFileInfo {
                filename: resolved_path,
                row_id_start: table_file.row_id_start,
                snapshot_id: table_file.snapshot_id,
                file_index: file_idx as u64,
            };
            let file_exec = if table_file.delete_file.is_some() {
                // Don't push limit for files with deletes (same as non-virtual path)
                self.build_exec_for_file_with_deletes(state, table_file, real_proj_ref, None)
                    .await?
            } else {
                // Don't push limit to individual file scans — the combined plan
                // handles the overall limit, pushing it here would read limit*N rows
                self.build_exec_for_single_file(state, table_file, real_proj_ref, None)
                    .await?
            };
            execs.push(Arc::new(VirtualColumnExec::new(
                file_exec,
                file_info,
                included.clone(),
                Arc::clone(&vc_output_schema),
            )));
        }
        // Add inlined data with virtual columns (empty path for inlined rows)
        if let Some(inlined_exec) = self.build_inlined_data_exec(state, real_proj_ref).await? {
            let inlined_info = VirtualColumnFileInfo {
                filename: String::new(),
                row_id_start: None,
                snapshot_id: None,
                file_index: active_files.len() as u64,
            };
            execs.push(Arc::new(VirtualColumnExec::new(
                inlined_exec,
                inlined_info,
                included.clone(),
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
            if included.filename {
                expected.push(filename_idx);
            }
            if included.file_row_number {
                expected.push(row_number_idx);
            }
            if included.rowid {
                expected.push(rowid_idx);
            }
            if included.snapshot_id {
                expected.push(snapshot_id_idx);
            }
            if included.file_index {
                expected.push(file_index_idx);
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
                if included.filename {
                    index_to_vc_pos.insert(filename_idx, vc_pos);
                    vc_pos += 1;
                }
                if included.file_row_number {
                    index_to_vc_pos.insert(row_number_idx, vc_pos);
                    vc_pos += 1;
                }
                if included.rowid {
                    index_to_vc_pos.insert(rowid_idx, vc_pos);
                    vc_pos += 1;
                }
                if included.snapshot_id {
                    index_to_vc_pos.insert(snapshot_id_idx, vc_pos);
                    vc_pos += 1;
                }
                if included.file_index {
                    index_to_vc_pos.insert(file_index_idx, vc_pos);
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

        // Build write-side partition columns from the table's partition metadata
        let write_partition_cols: Vec<crate::insert_exec::WritePartitionColumn> = self
            .partition_columns
            .iter()
            .filter_map(|pc| {
                let col_idx = self
                    .schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == &pc.column_name)?;
                let resolved_transform =
                    crate::insert_exec::PartitionTransform::from_str_opt(pc.transform.as_deref())
                        .map_err(|e| DataFusionError::External(Box::new(e)));
                Some(
                    resolved_transform.map(|rt| crate::insert_exec::WritePartitionColumn {
                        column_name: pc.column_name.clone(),
                        column_index: col_idx,
                        resolved_transform: rt,
                        transform: pc.transform.clone(),
                    }),
                )
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        let exec = DuckLakeInsertExec::new(
            actual_input,
            Arc::clone(writer),
            schema_name.clone(),
            self.table_name.clone(),
            Arc::clone(&self.schema),
            write_mode,
            Arc::clone(&self.object_store_url),
        )
        .with_partition_columns(write_partition_cols);

        Ok(Arc::new(exec))
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
        DataType::Date32 => s
            .parse::<i32>()
            .ok()
            .or_else(|| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .map(|d| {
                        (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
                    })
            })
            .map(|v| ScalarValue::Date32(Some(v))),
        DataType::Date64 => s
            .parse::<i64>()
            .ok()
            .or_else(|| {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .map(|d| {
                        (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days()
                            * 86_400_000
                    })
            })
            .map(|v| ScalarValue::Date64(Some(v))),
        _ => None,
    }
}

/// Combines multiple execution plans into a single plan
fn combine_execution_plans(
    execs: Vec<Arc<dyn ExecutionPlan>>,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    if execs.len() == 1 {
        Ok(execs.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("Expected at least one execution plan".into())
        })?)
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

/// Check if a DataFusion error is caused by an object store NotFound error.
fn is_object_store_not_found(err: &DataFusionError) -> bool {
    if let DataFusionError::ObjectStore(os_err) = err {
        return matches!(os_err.as_ref(), object_store::Error::NotFound { .. });
    }
    let mut source = std::error::Error::source(err);
    while let Some(e) = source {
        if let Some(os_err) = e.downcast_ref::<object_store::Error>() {
            return matches!(os_err, object_store::Error::NotFound { .. });
        }
        source = e.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validated_file_size_positive() {
        assert_eq!(validated_file_size(0, "test.parquet").unwrap(), 0);
        assert_eq!(validated_file_size(1024, "test.parquet").unwrap(), 1024);
        assert_eq!(
            validated_file_size(i64::MAX, "test.parquet").unwrap(),
            i64::MAX as u64
        );
    }

    #[test]
    fn test_validated_file_size_negative() {
        let err = validated_file_size(-1, "data/test.parquet").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("-1"),
            "Error should contain the negative value: {}",
            msg
        );
        assert!(
            msg.contains("data/test.parquet"),
            "Error should contain the file path: {}",
            msg
        );
    }

    #[test]
    fn test_validated_file_size_large_negative() {
        let err = validated_file_size(i64::MIN, "bad.parquet").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("bad.parquet"));
        assert!(msg.contains(&i64::MIN.to_string()));
    }

    /// R5-S-005: Date32 partition values should be ISO 8601 date strings, not epoch-day integers.
    #[test]
    fn test_scalar_value_to_partition_string_date32() {
        use datafusion::common::ScalarValue;
        // 2024-01-15 is day 19737 since epoch (1970-01-01)
        let val = ScalarValue::Date32(Some(19737));
        let result = scalar_value_to_partition_string(&val);
        assert_eq!(result, Some("2024-01-15".to_string()));
    }

    /// R5-S-005: Date64 partition values should be ISO 8601 date strings, not epoch-ms integers.
    #[test]
    fn test_scalar_value_to_partition_string_date64() {
        use datafusion::common::ScalarValue;
        // 2024-01-15 00:00:00 UTC in milliseconds since epoch
        let val = ScalarValue::Date64(Some(19737 * 86400 * 1000));
        let result = scalar_value_to_partition_string(&val);
        assert_eq!(result, Some("2024-01-15".to_string()));
    }

    /// Epoch (1970-01-01) should format correctly.
    #[test]
    fn test_scalar_value_to_partition_string_date32_epoch() {
        use datafusion::common::ScalarValue;
        let val = ScalarValue::Date32(Some(0));
        let result = scalar_value_to_partition_string(&val);
        assert_eq!(result, Some("1970-01-01".to_string()));
    }
}
