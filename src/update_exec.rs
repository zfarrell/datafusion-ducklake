//! DuckLake UPDATE execution plan.
//!
//! Implements UPDATE table SET col = val WHERE condition by:
//! 1. Scanning each data file to find matching rows (collecting full row data + positions)
//! 2. Applying SET transformations to the matched row data (in memory)
//! 3. Validating NOT NULL constraints on the transformed rows **before any
//!    disk I/O** — so a failing constraint never leaves orphan files behind
//! 4. Writing delete files for matched rows
//! 5. Writing a new data file containing the transformed rows
//! 6. Atomically registering both in catalog metadata
//!
//! This implements the copy-on-write (MOR) pattern: old rows are marked deleted,
//! new rows with updated values are written as new data files.
//!
//! ## Concurrency
//!
//! Like DELETE (see `delete_exec.rs`), the exec captures the table's
//! `snapshot_id` at plan time and threads it through
//! `MetadataWriter::register_dml_files` as `since_snapshot`. The writer's
//! conflict check runs inside the same transaction as the metadata mutations,
//! so a concurrent DML that committed against the same `data_file_id` after
//! `since_snapshot` will cause this UPDATE to fail with
//! `TransactionConflict` and the `UploadCleanupGuard` will clean up any files
//! already uploaded to the object store.
//!
//! **Granularity note (see TODO at the call site):** the conflict check is
//! currently *file-level* — any two transactions that target the same
//! `data_file_id` and were planned at the same snapshot will conflict, even
//! if their predicates select disjoint row positions. This is correct for
//! correctness but is conservative for UPDATE: two UPDATEs that touch
//! disjoint rows in the same file *could* in principle both commit. Refining
//! the check to be row-position-aware is tracked as a follow-up.
//!
//! ## Memory bounds
//!
//! UPDATE buffers the full set of transformed rows in memory before writing
//! them out as a new data file. The size cap is read from
//! [`crate::config::DuckLakeConfig::max_buffered_rows_per_dml`] (default
//! 10M); raise it via session config for legitimate large UPDATEs.
//!
//! ## Partition-column updates
//!
//! Updating a column that is part of the table's partitioning expression is
//! ambiguous in the DuckLake spec (the row's partition assignment would have
//! to change). The current implementation does not move rows across
//! partition files — the SET expression is applied to the row in-place and
//! the resulting batch is written to the same new data file regardless of
//! whether the partition value changed. Tracked as a follow-up; do not
//! redesign in this exec.
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
    /// Snapshot id this UPDATE was planned against, for optimistic-concurrency
    /// conflict detection. A concurrent DML that ended any of this UPDATE's
    /// target data files (or installed a newer active delete file on them)
    /// after this snapshot will cause `register_dml_files` to reject the
    /// commit with `TransactionConflict`.
    since_snapshot: i64,
    /// Cached plan properties
    cache: Arc<PlanProperties>,
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
        since_snapshot: i64,
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
            since_snapshot,
            cache,
        }
    }

    fn compute_properties() -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(make_dml_count_schema()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ))
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

    fn properties(&self) -> &Arc<PlanProperties> {
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
        let since_snapshot = self.since_snapshot;
        let output_schema = make_dml_count_schema();

        // Honour the session-config override for `max_buffered_rows_per_dml`
        // if present; otherwise fall back to the legacy 10M default.
        let max_buffered_rows = context
            .session_config()
            .options()
            .extensions
            .get::<crate::config::DuckLakeConfig>()
            .map(|c| c.max_buffered_rows_per_dml)
            .unwrap_or_else(|| crate::config::DuckLakeConfig::default().max_buffered_rows_per_dml);

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
            // Cleanup guard ensures orphan files are removed on any error path.
            // It is initialised here so that subsequent uploads (delete files,
            // new data file) register their `ObjectPath` with the guard and
            // are removed on any `?`-propagated error.
            let mut upload_guard =
                crate::table_writer::UploadCleanupGuard::new(Arc::clone(&object_store));
            // Collect file metadata for atomic registration
            let mut pending_delete_files: Vec<DeleteFileInfo> = Vec::new();

            // Phase 1: For each data file, scan, identify matching positions,
            // apply SET, and validate NOT NULL on the resulting batches —
            // BEFORE any delete file or data file is uploaded. Collect the
            // per-file work so phase 2 can perform the disk I/O once all
            // validation passes have succeeded.
            struct PerFileWork {
                resolved_path: String,
                data_file_id: i64,
                positions_to_delete: Vec<i64>,
                existing_positions: Option<HashSet<i64>>,
            }
            let mut per_file_work: Vec<PerFileWork> = Vec::new();

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

                // Apply SET transformations to matching rows. We do this here
                // (in phase 1, before any disk writes) so that NOT NULL
                // validation below can short-circuit before we upload
                // anything. The buffered batches are accumulated into
                // `updated_batches` and become the new data file in phase 3.
                let updated_batches_before = updated_batches.len();
                for matched_batch in &matching_rows {
                    if matched_batch.num_rows() == 0 {
                        continue;
                    }

                    // Start with the original columns
                    let mut columns: Vec<ArrayRef> = matched_batch.columns().to_vec();

                    // Apply each SET assignment.
                    //
                    // Self-referencing expressions (e.g. `SET x = x + 1`) are
                    // evaluated against `matched_batch`, which holds the
                    // *pre-update* column values, so they see the prior `x`
                    // and not any previously-assigned value within the same
                    // SET clause.
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
                    buffered_rows = buffered_rows.checked_add(updated_batch.num_rows())
                        .ok_or_else(|| DataFusionError::Execution(
                            "UPDATE buffered_rows overflow".to_string()
                        ))?;
                    updated_batches.push(updated_batch);

                    if buffered_rows > max_buffered_rows {
                        return Err(DataFusionError::ResourcesExhausted(format!(
                            "UPDATE affects too many rows ({} rows buffered, limit is {}). \
                             Raise `ducklake.max_buffered_rows_per_dml` in the session config \
                             or use a more selective WHERE clause.",
                            buffered_rows, max_buffered_rows
                        )));
                    }
                }

                // Enforce NOT NULL constraints on the rows that were just
                // generated for THIS file, before any delete file or data
                // file has been written to disk. This guarantees the
                // ticket's acceptance criterion that "UPDATE setting NOT
                // NULL column to NULL returns an error before any write".
                if updated_batches.len() > updated_batches_before {
                    crate::table_writer::validate_not_null_constraints(
                        &table_schema,
                        &updated_batches[updated_batches_before..],
                    )?;
                }

                per_file_work.push(PerFileWork {
                    resolved_path,
                    data_file_id,
                    positions_to_delete,
                    existing_positions: existing_positions.cloned(),
                });
            }

            // Phase 2: now that ALL per-file SET applications and NOT NULL
            // checks have passed, perform the disk I/O. Any error from this
            // point on still cleans up via `upload_guard`.
            for work in per_file_work {
                let PerFileWork {
                    resolved_path,
                    data_file_id,
                    positions_to_delete,
                    existing_positions,
                } = work;

                let mut all_positions = positions_to_delete;
                if let Some(existing) = existing_positions {
                    for pos in &existing {
                        all_positions.push(*pos);
                    }
                    all_positions.sort_unstable();
                    all_positions.dedup();
                }

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

            // Phase 3: write the new data file. NOT NULL has already been
            // validated per-batch above, but we keep the full-set check here
            // as a safety net (e.g. in case future refactors reorder phases).
            let mut pending_data_files: Vec<DataFileInfo> = Vec::new();
            if !updated_batches.is_empty() {
                crate::table_writer::validate_not_null_constraints(
                    &table_schema,
                    &updated_batches,
                )?;
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

            // Atomically register all delete files and data files (guard
            // cleans up on error). Passing `since_snapshot` opts UPDATE into
            // the same optimistic-concurrency conflict detection DELETE got
            // in #17: any concurrent DML that ended a targeted data file or
            // installed a newer active delete file on it since this UPDATE
            // was planned will cause `register_dml_files` to fail with
            // `TransactionConflict`.
            //
            // Granularity: the writer's check is file-level (any conflict
            // on the same `data_file_id`). For UPDATE this is conservative:
            // two UPDATEs that touch disjoint row positions in the same file
            // will conflict. Refining this to be row-position-aware is
            // tracked as a follow-up; see the module-level docs.
            writer
                .register_dml_files(
                    table_id,
                    snapshot_id,
                    &pending_delete_files,
                    &pending_data_files,
                    Some(since_snapshot),
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
