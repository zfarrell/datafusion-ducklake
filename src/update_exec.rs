//! DuckLake UPDATE execution plan.
//!
//! Implements UPDATE table SET col = val WHERE condition by:
//! 1. Scanning each data file to find matching rows (collecting full row data + positions)
//! 2. Writing delete files for matched rows
//! 3. Applying SET transformations to matched row data
//! 4. Writing new data files with transformed rows
//! 5. Registering both delete files and new data files in catalog metadata
//!
//! This implements the copy-on-write (MOR) pattern: old rows are marked deleted,
//! new rows with updated values are written as new data files.
//!
//! If metadata registration fails after files have been uploaded,
//! best-effort cleanup removes all orphaned files (both delete and data files).
//! See `table_writer.rs` for full write atomicity guarantees.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::compute;
use arrow::datatypes::SchemaRef;
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

use crate::metadata_provider::DuckLakeTableFile;
use crate::metadata_writer::{DataFileInfo, DeleteFileInfo, MetadataWriter};

use crate::delete_exec::make_dml_count_schema;

/// Represents a column assignment in an UPDATE SET clause.
#[derive(Debug, Clone)]
pub struct UpdateAssignment {
    /// Index of the column to update in the table schema
    pub column_index: usize,
    /// Expression that computes the new value
    pub expr: Expr,
}

/// Execution plan that updates rows in a DuckLake table using delete + insert pattern.
pub struct DuckLakeUpdateExec {
    /// Table ID in the catalog
    table_id: i64,
    /// Table name (for display)
    table_name: String,
    /// Arrow schema of the table
    table_schema: SchemaRef,
    /// Column IDs from catalog metadata (for embedding PARQUET:field_id in written files)
    column_ids: Vec<i64>,
    /// Files in the table
    table_files: Arc<Vec<DuckLakeTableFile>>,
    /// Filter expressions (WHERE clause). Empty means update all rows.
    filters: Vec<Expr>,
    /// Column assignments (SET clause)
    assignments: Vec<UpdateAssignment>,
    /// Metadata writer for registering files
    writer: Arc<dyn MetadataWriter>,
    /// Object store URL for reading/writing files
    object_store_url: Arc<ObjectStoreUrl>,
    /// Table path for resolving relative file paths
    table_path: String,
    /// Existing deleted positions per file (pre-loaded)
    existing_deletes: Arc<HashMap<String, HashSet<i64>>>,
    /// Cached plan properties
    cache: PlanProperties,
}

impl DuckLakeUpdateExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: i64,
        table_name: String,
        table_schema: SchemaRef,
        column_ids: Vec<i64>,
        table_files: Vec<DuckLakeTableFile>,
        filters: Vec<Expr>,
        assignments: Vec<UpdateAssignment>,
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
            column_ids,
            table_files: Arc::new(table_files),
            filters,
            assignments,
            writer,
            object_store_url,
            table_path,
            existing_deletes: Arc::new(existing_deletes),
            cache,
        }
    }

    fn compute_properties() -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(make_dml_count_schema()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl Debug for DuckLakeUpdateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckLakeUpdateExec")
            .field("table_name", &self.table_name)
            .field("num_files", &self.table_files.len())
            .field("num_filters", &self.filters.len())
            .field("num_assignments", &self.assignments.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DuckLakeUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DuckLakeUpdateExec: table={}, files={}, filters={}, assignments={}",
                    self.table_name,
                    self.table_files.len(),
                    self.filters.len(),
                    self.assignments.len()
                )
            },
        }
    }
}

impl ExecutionPlan for DuckLakeUpdateExec {
    fn name(&self) -> &str {
        "DuckLakeUpdateExec"
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
                "DuckLakeUpdateExec does not accept children".to_string(),
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
                "DuckLakeUpdateExec only supports partition 0, got {}",
                partition
            )));
        }

        // Clone Arcs (cheap) instead of data for the async block
        let table_id = self.table_id;
        let table_schema = Arc::clone(&self.table_schema);
        let column_ids = self.column_ids.clone();
        let table_files = Arc::clone(&self.table_files);
        let filters = self.filters.clone();
        let assignments = self.assignments.clone();
        let writer = Arc::clone(&self.writer);
        let object_store_url = self.object_store_url.clone();
        let table_path = self.table_path.clone();
        let existing_deletes = Arc::clone(&self.existing_deletes);
        let output_schema = make_dml_count_schema();

        let stream = stream::once(async move {
            let object_store = context
                .runtime_env()
                .object_store(object_store_url.as_ref())?;

            // Compile filter expressions into physical expressions
            let df_schema = DFSchema::try_from(table_schema.as_ref().clone())?;
            let physical_filters: Vec<_> = filters
                .iter()
                .map(|expr| create_physical_expr(expr, &df_schema, &Default::default()))
                .collect::<DataFusionResult<Vec<_>>>()?;

            // Compile SET expressions into physical expressions
            let physical_assignments: Vec<_> = assignments
                .iter()
                .map(|a| {
                    let phys_expr = create_physical_expr(&a.expr, &df_schema, &Default::default())?;
                    Ok((a.column_index, phys_expr))
                })
                .collect::<DataFusionResult<Vec<_>>>()?;

            let mut total_updated: u64 = 0;
            // Collect all updated rows across all files for writing as new data.
            // Track buffered row count to guard against OOM on large updates.
            let mut updated_batches: Vec<RecordBatch> = Vec::new();
            let mut buffered_rows: usize = 0;
            const MAX_BUFFERED_ROWS: usize = 10_000_000; // 10M row safety limit
            // Cleanup guard ensures orphan files are removed on any error path
            let mut upload_guard =
                crate::table_writer::UploadCleanupGuard::new(Arc::clone(&object_store));
            // Collect file metadata for atomic registration
            let mut pending_delete_files: Vec<DeleteFileInfo> = Vec::new();

            // Process each data file
            for table_file in &*table_files {
                let data_file_id = table_file.data_file_id.ok_or_else(|| {
                    DataFusionError::Internal(
                        "data_file_id is required for UPDATE operations".to_string(),
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
                let mut matching_rows: Vec<RecordBatch> = Vec::new();
                let mut global_row_offset: i64 = 0;

                while let Some(batch) = parquet_stream.try_next().await? {
                    let num_rows = batch.num_rows();

                    // Determine which rows match the filter
                    let matching_mask = if physical_filters.is_empty() {
                        None // no filter = all rows match
                    } else {
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

                    // Build a mask that includes filter match AND excludes already-deleted rows
                    let mut mask_values = vec![false; num_rows];
                    for (i, mask_val) in mask_values.iter_mut().enumerate().take(num_rows) {
                        let i_i64 = i64::try_from(i).map_err(|e| {
                            DataFusionError::Execution(format!("Row index overflow: {}", e))
                        })?;
                        let global_pos = global_row_offset + i_i64;

                        // Skip if already deleted
                        if let Some(existing) = existing_positions
                            && existing.contains(&global_pos)
                        {
                            continue;
                        }

                        // NULL predicate = no match
                        let matches = match &matching_mask {
                            None => true,
                            Some(mask) => mask.is_valid(i) && mask.value(i),
                        };

                        if matches {
                            positions_to_delete.push(global_pos);
                            *mask_val = true;
                        }
                    }

                    // Filter the batch to get only matching rows
                    let effective_mask = arrow::array::BooleanArray::from(mask_values);
                    if effective_mask.true_count() > 0 {
                        let filtered = compute::filter_record_batch(&batch, &effective_mask)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        matching_rows.push(filtered);
                    }

                    global_row_offset += i64::try_from(num_rows).map_err(|e| {
                        DataFusionError::Execution(format!("Row count overflow: {}", e))
                    })?;
                }

                // Skip this file if no rows to update
                if positions_to_delete.is_empty() {
                    continue;
                }

                let new_update_count = u64::try_from(positions_to_delete.len()).map_err(|e| {
                    DataFusionError::Execution(format!("Update count overflow: {}", e))
                })?;
                total_updated = total_updated.checked_add(new_update_count).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Total updated row count overflow: {} + {} exceeds u64::MAX",
                        total_updated, new_update_count
                    ))
                })?;

                // Apply SET transformations to matching rows
                for matched_batch in &matching_rows {
                    if matched_batch.num_rows() == 0 {
                        continue;
                    }

                    // Start with the original columns
                    let mut columns: Vec<ArrayRef> = matched_batch.columns().to_vec();

                    // Apply each SET assignment
                    for (col_idx, phys_expr) in &physical_assignments {
                        if *col_idx >= columns.len() {
                            return Err(DataFusionError::Plan(format!(
                                "UPDATE assignment column index {} is out of bounds \
                                 (table has {} columns)",
                                col_idx,
                                columns.len()
                            )));
                        }
                        let result = phys_expr.evaluate(matched_batch)?;
                        let new_values = result.into_array(matched_batch.num_rows())?;
                        columns[*col_idx] = new_values;
                    }

                    let updated_batch = RecordBatch::try_new(table_schema.clone(), columns)?;
                    buffered_rows += updated_batch.num_rows();
                    updated_batches.push(updated_batch);

                    if buffered_rows > MAX_BUFFERED_ROWS {
                        return Err(DataFusionError::ResourcesExhausted(format!(
                            "UPDATE affects too many rows ({} rows buffered, limit is {}). \
                             Consider updating in smaller batches using a more selective WHERE clause.",
                            buffered_rows, MAX_BUFFERED_ROWS
                        )));
                    }
                }

                // Merge with existing deletes for the delete file
                let mut all_positions = positions_to_delete;
                if let Some(existing) = existing_positions {
                    for pos in existing {
                        all_positions.push(*pos);
                    }
                    all_positions.sort_unstable();
                    all_positions.dedup();
                }

                // Write and upload delete file using shared helper
                let delete_file_info = crate::table_writer::write_delete_file(
                    &*object_store,
                    &table_path,
                    &resolved_path,
                    data_file_id,
                    all_positions,
                    &mut upload_guard,
                )
                .await?;

                pending_delete_files.push(delete_file_info);
            }

            // Enforce NOT NULL constraints on updated rows before writing
            crate::table_writer::validate_not_null_constraints(&table_schema, &updated_batches)?;

            // Write updated rows as new data file(s)
            let mut pending_data_files: Vec<DataFileInfo> = Vec::new();
            if !updated_batches.is_empty() {
                let data_file_info = crate::table_writer::write_and_upload_parquet(
                    &updated_batches,
                    &table_schema,
                    &column_ids,
                    &table_path,
                    &*object_store,
                    &mut upload_guard,
                )
                .await?;
                pending_data_files.push(data_file_info);
            }

            // R3F-032: Skip snapshot creation if no rows were affected
            if total_updated == 0 {
                let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
                return Ok(RecordBatch::try_new(output_schema, vec![count_array])?);
            }

            // Create snapshot (guard cleans up uploaded files on error)
            let snapshot_id = writer
                .create_snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Atomically register all delete files and data files (guard cleans up on error)
            writer
                .register_dml_files(
                    table_id,
                    snapshot_id,
                    &pending_delete_files,
                    &pending_data_files,
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Success — disarm the cleanup guard
            upload_guard.disarm();

            // R3F-013: Record snapshot changes for UPDATE
            // R4-S-008: Use standard DuckDB tokens (inserted + deleted) instead of non-standard "updated_table"
            // R4-S-016: Non-fatal — DML data is already committed
            if let Err(e) = writer.record_snapshot_changes(
                snapshot_id,
                &format!(
                    "inserted_into_table:{},deleted_from_table:{}",
                    table_id, table_id
                ),
            ) {
                tracing::warn!(
                    snapshot_id,
                    error = %e,
                    "Failed to record snapshot changes after UPDATE commit"
                );
            }

            // Return the count of updated rows
            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![total_updated]));
            Ok(RecordBatch::try_new(output_schema, vec![count_array])?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            make_dml_count_schema(),
            stream,
        )))
    }
}
