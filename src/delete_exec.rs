//! DuckLake DELETE execution plan.
//!
//! Implements DELETE FROM table WHERE condition by:
//! 1. Scanning each data file to find matching rows
//! 2. Tracking row positions of matching rows
//! 3. Writing Parquet delete files with (file_path, pos) schema
//! 4. Registering delete files in catalog metadata
//!
//! If metadata registration fails after a delete file has been uploaded,
//! best-effort cleanup removes the orphaned file. See `table_writer.rs`
//! for full write atomicity guarantees.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, create_physical_expr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::{self, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::metadata_provider::DuckLakeTableFile;
use crate::metadata_writer::{DeleteFileInfo, MetadataWriter};
use crate::path_resolver::join_paths;
use crate::table::delete_file_schema;
use crate::table_writer::{calculate_footer_size_from_bytes, cleanup_orphaned_files};

/// Schema for the output of delete operations (count of rows deleted)
fn make_delete_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// Execution plan that deletes rows from a DuckLake table by writing delete files.
pub struct DuckLakeDeleteExec {
    /// Table ID in the catalog
    table_id: i64,
    /// Table name (for display)
    table_name: String,
    /// Arrow schema of the table (for filter evaluation)
    table_schema: SchemaRef,
    /// Files in the table (with their data_file_ids and existing delete info)
    table_files: Vec<DuckLakeTableFile>,
    /// Filter expressions (WHERE clause). Empty means delete all rows.
    filters: Vec<Expr>,
    /// Metadata writer for registering delete files
    writer: Arc<dyn MetadataWriter>,
    /// Object store URL for reading/writing files
    object_store_url: Arc<ObjectStoreUrl>,
    /// Table path for resolving relative file paths
    table_path: String,
    /// Existing deleted positions per file (pre-loaded)
    existing_deletes: HashMap<String, HashSet<i64>>,
    /// Cached plan properties
    cache: PlanProperties,
}

impl DuckLakeDeleteExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: i64,
        table_name: String,
        table_schema: SchemaRef,
        table_files: Vec<DuckLakeTableFile>,
        filters: Vec<Expr>,
        writer: Arc<dyn MetadataWriter>,
        object_store_url: Arc<ObjectStoreUrl>,
        table_path: String,
        existing_deletes: HashMap<String, HashSet<i64>>,
    ) -> Self {
        let cache = Self::compute_properties();
        Self {
            table_id,
            table_name,
            table_schema,
            table_files,
            filters,
            writer,
            object_store_url,
            table_path,
            existing_deletes,
            cache,
        }
    }

    fn compute_properties() -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(make_delete_count_schema()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl Debug for DuckLakeDeleteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckLakeDeleteExec")
            .field("table_name", &self.table_name)
            .field("num_files", &self.table_files.len())
            .field("num_filters", &self.filters.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DuckLakeDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DuckLakeDeleteExec: table={}, files={}, filters={}",
                    self.table_name,
                    self.table_files.len(),
                    self.filters.len()
                )
            },
        }
    }
}

impl ExecutionPlan for DuckLakeDeleteExec {
    fn name(&self) -> &str {
        "DuckLakeDeleteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "DuckLakeDeleteExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DuckLakeDeleteExec only supports partition 0, got {}",
                partition
            )));
        }

        // Clone everything needed for the async block
        let table_id = self.table_id;
        let table_schema = Arc::clone(&self.table_schema);
        let table_files = self.table_files.clone();
        let filters = self.filters.clone();
        let writer = Arc::clone(&self.writer);
        let object_store_url = self.object_store_url.clone();
        let table_path = self.table_path.clone();
        let existing_deletes = self.existing_deletes.clone();
        let output_schema = make_delete_count_schema();

        let stream = stream::once(async move {
            let object_store = context
                .runtime_env()
                .object_store(object_store_url.as_ref())?;

            // Create a snapshot for this delete operation
            let snapshot_id = writer
                .create_snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Compile filter expressions into physical expressions
            let df_schema = DFSchema::try_from(table_schema.as_ref().clone())?;
            let physical_filters: Vec<_> = filters
                .iter()
                .map(|expr| create_physical_expr(expr, &df_schema, &Default::default()))
                .collect::<DataFusionResult<Vec<_>>>()?;

            let mut total_deleted: u64 = 0;
            // Track uploaded files for cleanup if metadata commit fails
            let mut uploaded_files: Vec<ObjectPath> = Vec::new();
            // Collect delete file metadata for atomic registration
            let mut pending_delete_files: Vec<DeleteFileInfo> = Vec::new();

            // Process each data file
            for table_file in &table_files {
                let data_file_id = table_file.data_file_id.ok_or_else(|| {
                    DataFusionError::Internal(
                        "data_file_id is required for DELETE operations".to_string(),
                    )
                })?;

                // Resolve the data file path
                let resolved_path = crate::path_resolver::resolve_path(
                    &table_path,
                    &table_file.file.path,
                    table_file.file.path_is_relative,
                )?;

                // Get existing deleted positions for this file
                let existing_positions = existing_deletes.get(&resolved_path);

                // Read all rows from this data file
                let object_path = ObjectPath::from(resolved_path.as_str());
                let reader = parquet::arrow::async_reader::ParquetObjectReader::new(
                    Arc::clone(&object_store),
                    object_path,
                );

                let builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let mut parquet_stream = builder
                    .build()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let mut positions_to_delete: Vec<i64> = Vec::new();
                let mut global_row_offset: i64 = 0;

                while let Some(batch_result) = parquet_stream.try_next().await? {
                    let batch = batch_result;
                    let num_rows = batch.num_rows();

                    // Determine which rows match the filter
                    let matching_mask = if physical_filters.is_empty() {
                        // No filter = delete all rows
                        None // means all rows match
                    } else {
                        // Evaluate all filters and AND them together
                        let mut combined_mask =
                            arrow::array::BooleanArray::from(vec![true; num_rows]);
                        for filter in &physical_filters {
                            let result = filter.evaluate(&batch)?;
                            let bool_arr = result.into_array(num_rows)?;
                            let filter_arr = bool_arr
                                .as_any()
                                .downcast_ref::<arrow::array::BooleanArray>()
                                .ok_or_else(|| {
                                    DataFusionError::Internal(
                                        "Filter did not return boolean array".to_string(),
                                    )
                                })?;
                            combined_mask = compute::and(&combined_mask, filter_arr)
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        }
                        Some(combined_mask)
                    };

                    // Collect matching row positions (excluding already-deleted rows)
                    for i in 0..num_rows {
                        let global_pos = global_row_offset + i as i64;

                        // Skip if already deleted
                        if let Some(existing) = existing_positions
                            && existing.contains(&global_pos)
                        {
                            continue;
                        }

                        // Check if row matches filter
                        let matches = match &matching_mask {
                            None => true, // no filter = all match
                            Some(mask) => mask.value(i),
                        };

                        if matches {
                            positions_to_delete.push(global_pos);
                        }
                    }

                    global_row_offset += num_rows as i64;
                }

                // Skip this file if no rows to delete
                if positions_to_delete.is_empty() {
                    continue;
                }

                let delete_count = positions_to_delete.len() as i64;
                total_deleted += delete_count as u64;

                // Merge with existing deletes if any
                if let Some(existing) = existing_positions {
                    for pos in existing {
                        positions_to_delete.push(*pos);
                    }
                    positions_to_delete.sort_unstable();
                    positions_to_delete.dedup();
                }

                // Write the delete file
                let delete_file_name = format!("ducklake-{}-delete.parquet", Uuid::new_v4());
                // Use the table path structure for delete files
                let schema_table_prefix = table_path.trim_start_matches('/');
                let delete_object_key = join_paths(schema_table_prefix, &delete_file_name)?;
                let delete_object_path =
                    ObjectPath::from(delete_object_key.trim_start_matches('/'));

                // Build the delete file content
                let del_schema = delete_file_schema();
                let file_path_values: Vec<&str> =
                    vec![&table_file.file.path; positions_to_delete.len()];
                let file_path_array: ArrayRef = Arc::new(StringArray::from(file_path_values));
                let pos_array: ArrayRef = Arc::new(Int64Array::from(positions_to_delete.clone()));

                let delete_batch =
                    RecordBatch::try_new(del_schema.clone(), vec![file_path_array, pos_array])?;

                let props = WriterProperties::builder()
                    .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                    .build();
                let mut arrow_writer = ArrowWriter::try_new(Vec::new(), del_schema, Some(props))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                arrow_writer
                    .write(&delete_batch)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let buffer = arrow_writer
                    .into_inner()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let file_size = buffer.len() as i64;
                let footer_size = calculate_footer_size_from_bytes(&buffer)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Upload to object store
                object_store
                    .put(&delete_object_path, PutPayload::from(buffer))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                uploaded_files.push(delete_object_path);

                // Collect delete file info for atomic batch registration
                let delete_file_info =
                    DeleteFileInfo::new(data_file_id, &delete_file_name, file_size, delete_count)
                        .with_footer_size(footer_size);

                pending_delete_files.push(delete_file_info);
            }

            // Atomically register all delete files
            if let Err(e) =
                writer.register_dml_files(table_id, snapshot_id, &pending_delete_files, &[])
            {
                cleanup_orphaned_files(&*object_store, &uploaded_files).await;
                return Err(DataFusionError::External(Box::new(e)));
            }

            // Return the count of deleted rows
            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![total_deleted]));
            Ok(RecordBatch::try_new(output_schema, vec![count_array])?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            make_delete_count_schema(),
            stream,
        )))
    }
}
