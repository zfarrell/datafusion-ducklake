//! DuckLake MERGE INTO execution plan.
//!
//! Implements MERGE INTO target USING source ON condition
//!   WHEN MATCHED THEN UPDATE SET ... / DELETE
//!   WHEN NOT MATCHED THEN INSERT (cols) VALUES (vals)
//!
//! Internally decomposes MERGE into:
//! 1. Join source and target on the ON condition
//! 2. For matched rows: apply UPDATE or DELETE
//! 3. For unmatched source rows: INSERT
//!
//! Uses the MOR (Merge-On-Read) pattern:
//! - Matched+updated rows → delete file for old positions + new data file with updated values
//! - Matched+deleted rows → delete file for old positions
//! - Unmatched rows → new data file with inserted values

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::{self, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::metadata_provider::DuckLakeTableFile;
use crate::metadata_writer::{DataFileInfo, DeleteFileInfo, MetadataWriter};
use crate::path_resolver::join_paths;
use crate::table::delete_file_schema;
use crate::table_writer::{
    build_schema_with_field_ids, calculate_footer_size_from_bytes, cleanup_orphaned_files,
};

/// Schema for the output of merge operations (count of rows affected)
fn make_merge_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// Action to take when rows match (WHEN MATCHED THEN ...).
#[derive(Debug, Clone)]
pub enum MergeMatchedAction {
    /// UPDATE: replace matched target rows with corresponding source rows.
    /// This is the standard upsert pattern where source values replace target values.
    Update,
    /// DELETE: remove matched target rows.
    Delete,
}

/// Execution plan that performs a MERGE INTO operation on a DuckLake table.
///
/// The merge operates by:
/// 1. Reading all source data into memory
/// 2. Scanning each target data file
/// 3. For each target row, checking if any source row matches the ON condition
/// 4. For matched target rows: applying the WHEN MATCHED action (UPDATE or DELETE)
/// 5. For unmatched source rows: writing them as new data (WHEN NOT MATCHED INSERT)
pub struct DuckLakeMergeExec {
    /// Table ID in the catalog
    table_id: i64,
    /// Table name (for display)
    table_name: String,
    /// Arrow schema of the target table
    table_schema: SchemaRef,
    /// Column IDs from catalog metadata
    column_ids: Vec<i64>,
    /// Files in the target table
    table_files: Vec<DuckLakeTableFile>,
    /// Source data to merge (pre-collected RecordBatches)
    source_batches: Vec<RecordBatch>,
    /// Join condition columns: (target_col_index, source_col_index) pairs
    /// For simple equi-join conditions like `target.id = source.id`
    join_key_pairs: Vec<(usize, usize)>,
    /// Action when rows match
    matched_action: Option<MergeMatchedAction>,
    /// Whether to insert unmatched source rows
    insert_unmatched: bool,
    /// Metadata writer for registering files
    writer: Arc<dyn MetadataWriter>,
    /// Object store URL for reading/writing files
    object_store_url: Arc<ObjectStoreUrl>,
    /// Table path for resolving relative file paths
    table_path: String,
    /// Existing deleted positions per file
    existing_deletes: HashMap<String, HashSet<i64>>,
    /// Cached plan properties
    cache: PlanProperties,
}

impl DuckLakeMergeExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: i64,
        table_name: String,
        table_schema: SchemaRef,
        column_ids: Vec<i64>,
        table_files: Vec<DuckLakeTableFile>,
        source_batches: Vec<RecordBatch>,
        join_key_pairs: Vec<(usize, usize)>,
        matched_action: Option<MergeMatchedAction>,
        insert_unmatched: bool,
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
            table_files,
            source_batches,
            join_key_pairs,
            matched_action,
            insert_unmatched,
            writer,
            object_store_url,
            table_path,
            existing_deletes,
            cache,
        }
    }

    fn compute_properties() -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(make_merge_count_schema()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl Debug for DuckLakeMergeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckLakeMergeExec")
            .field("table_name", &self.table_name)
            .field("num_files", &self.table_files.len())
            .field("num_source_batches", &self.source_batches.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DuckLakeMergeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DuckLakeMergeExec: table={}, files={}, source_batches={}",
                    self.table_name,
                    self.table_files.len(),
                    self.source_batches.len()
                )
            },
        }
    }
}

/// Compare a single value from two arrays for equality.
/// Returns Ok(true) if the values at the given indices are equal,
/// Ok(false) if not equal or either is null,
/// Err if the data type is not supported for comparison.
fn values_equal(
    target_col: &dyn arrow::array::Array,
    target_row: usize,
    source_col: &dyn arrow::array::Array,
    source_row: usize,
) -> DataFusionResult<bool> {
    use arrow::array::*;
    use arrow::datatypes::TimeUnit;

    if target_col.is_null(target_row) || source_col.is_null(source_row) {
        return Ok(false); // NULL != NULL in SQL semantics
    }

    macro_rules! compare_typed {
        ($arr_type:ty) => {{
            let t = target_col
                .as_any()
                .downcast_ref::<$arr_type>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "MERGE: failed to downcast target column to {}",
                        stringify!($arr_type)
                    ))
                })?;
            let s = source_col
                .as_any()
                .downcast_ref::<$arr_type>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "MERGE: failed to downcast source column to {}",
                        stringify!($arr_type)
                    ))
                })?;
            Ok(t.value(target_row) == s.value(source_row))
        }};
    }

    match target_col.data_type() {
        DataType::Boolean => compare_typed!(BooleanArray),
        DataType::Int8 => compare_typed!(Int8Array),
        DataType::Int16 => compare_typed!(Int16Array),
        DataType::Int32 => compare_typed!(Int32Array),
        DataType::Int64 => compare_typed!(Int64Array),
        DataType::UInt8 => compare_typed!(UInt8Array),
        DataType::UInt16 => compare_typed!(UInt16Array),
        DataType::UInt32 => compare_typed!(UInt32Array),
        DataType::UInt64 => compare_typed!(UInt64Array),
        DataType::Float32 => compare_typed!(Float32Array),
        DataType::Float64 => compare_typed!(Float64Array),
        DataType::Utf8 => compare_typed!(StringArray),
        DataType::LargeUtf8 => compare_typed!(LargeStringArray),
        DataType::Date32 => compare_typed!(Date32Array),
        DataType::Date64 => compare_typed!(Date64Array),
        DataType::Timestamp(TimeUnit::Second, _) => compare_typed!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            compare_typed!(TimestampMillisecondArray)
        },
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            compare_typed!(TimestampMicrosecondArray)
        },
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            compare_typed!(TimestampNanosecondArray)
        },
        DataType::Decimal128(_, _) => compare_typed!(Decimal128Array),
        dt => Err(DataFusionError::NotImplemented(format!(
            "MERGE join key comparison not supported for data type: {dt:?}"
        ))),
    }
}

impl ExecutionPlan for DuckLakeMergeExec {
    fn name(&self) -> &str {
        "DuckLakeMergeExec"
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
                "DuckLakeMergeExec does not accept children".to_string(),
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
                "DuckLakeMergeExec only supports partition 0, got {}",
                partition
            )));
        }

        let table_id = self.table_id;
        let table_schema = Arc::clone(&self.table_schema);
        let column_ids = self.column_ids.clone();
        let table_files = self.table_files.clone();
        let source_batches = self.source_batches.clone();
        let join_key_pairs = self.join_key_pairs.clone();
        let matched_action = self.matched_action.clone();
        let insert_unmatched = self.insert_unmatched;
        let writer = Arc::clone(&self.writer);
        let object_store_url = self.object_store_url.clone();
        let table_path = self.table_path.clone();
        let existing_deletes = self.existing_deletes.clone();
        let output_schema = make_merge_count_schema();

        let stream = stream::once(async move {
            let object_store = context
                .runtime_env()
                .object_store(object_store_url.as_ref())?;

            let has_matched_action = matched_action.is_some();

            let mut total_affected: u64 = 0;
            let mut new_data_batches: Vec<RecordBatch> = Vec::new();
            // Collect file metadata for atomic registration
            let mut pending_delete_files: Vec<DeleteFileInfo> = Vec::new();
            // R3F-003: Track uploaded files for cleanup on metadata failure
            let mut uploaded_files: Vec<ObjectPath> = Vec::new();

            // Track how many target rows each source row has matched
            // R3F-033: SQL standard requires error when source row matches multiple targets
            let total_source_rows: usize = source_batches.iter().map(|b| b.num_rows()).sum();
            let mut source_match_count = vec![0u32; total_source_rows];

            // For UPDATE: collect matched source rows to write as replacement data
            let mut matched_source_rows: Vec<RecordBatch> = Vec::new();

            // Process each target data file
            for table_file in &table_files {
                let data_file_id = table_file.data_file_id.ok_or_else(|| {
                    DataFusionError::Internal(
                        "data_file_id is required for MERGE operations".to_string(),
                    )
                })?;

                let resolved_path = crate::path_resolver::resolve_path(
                    &table_path,
                    &table_file.file.path,
                    table_file.file.path_is_relative,
                )?;

                let existing_positions = existing_deletes.get(&resolved_path);

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

                // Per-source-batch masks for matched source rows in this file
                let mut source_match_masks: Vec<Vec<bool>> = source_batches
                    .iter()
                    .map(|b| vec![false; b.num_rows()])
                    .collect();

                while let Some(batch_result) = parquet_stream.try_next().await? {
                    let batch = batch_result;
                    let num_rows = batch.num_rows();

                    for target_row_idx in 0..num_rows {
                        let target_row_i64 = i64::try_from(target_row_idx)
                            .map_err(|e| DataFusionError::Execution(format!("Row index overflow: {}", e)))?;
                        let global_pos = global_row_offset + target_row_i64;

                        // Skip already-deleted rows
                        if let Some(existing) = existing_positions {
                            if existing.contains(&global_pos) {
                                continue;
                            }
                        }

                        // Check each source row for a match
                        let mut source_global_idx = 0usize;
                        'source_scan: for (batch_idx, src_batch) in
                            source_batches.iter().enumerate()
                        {
                            for src_row_idx in 0..src_batch.num_rows() {
                                let mut all_keys_match = true;
                                for &(target_col, source_col) in &join_key_pairs {
                                    let target_arr = batch.column(target_col);
                                    let source_arr = src_batch.column(source_col);
                                    if !values_equal(
                                        target_arr.as_ref(),
                                        target_row_idx,
                                        source_arr.as_ref(),
                                        src_row_idx,
                                    )? {
                                        all_keys_match = false;
                                        break;
                                    }
                                }
                                if all_keys_match {
                                    let src_global = source_global_idx + src_row_idx;
                                    source_match_count[src_global] += 1;

                                    // R3F-033: Error if source row matches multiple target rows
                                    if source_match_count[src_global] > 1 {
                                        return Err(DataFusionError::Execution(
                                            "MERGE violation: a source row matched more than one target row. \
                                             SQL standard requires each source row to match at most one target row."
                                                .to_string(),
                                        ));
                                    }

                                    // Only process matched action if one exists
                                    if has_matched_action {
                                        positions_to_delete.push(global_pos);
                                        // Track which source row matched (for UPDATE)
                                        source_match_masks[batch_idx][src_row_idx] = true;
                                    }
                                    break 'source_scan;
                                }
                            }
                            source_global_idx += src_batch.num_rows();
                        }
                    }

                    global_row_offset += i64::try_from(num_rows)
                        .map_err(|e| DataFusionError::Execution(format!("Row count overflow: {}", e)))?;
                }

                // Skip writing delete files if no positions to delete
                if positions_to_delete.is_empty() {
                    continue;
                }

                let new_match_count = u64::try_from(positions_to_delete.len())
                    .map_err(|e| DataFusionError::Execution(format!("Delete count overflow: {}", e)))?;
                total_affected += new_match_count;

                // For UPDATE: collect the matched source rows (these replace the deleted target rows)
                if matches!(&matched_action, Some(MergeMatchedAction::Update)) {
                    for (batch_idx, src_batch) in source_batches.iter().enumerate() {
                        let mask = BooleanArray::from(source_match_masks[batch_idx].clone());
                        if mask.true_count() > 0 {
                            let filtered = compute::filter_record_batch(src_batch, &mask)
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                            matched_source_rows.push(filtered);
                        }
                    }
                }

                // Merge with existing deletes
                let mut all_positions = positions_to_delete;
                if let Some(existing) = existing_positions {
                    for pos in existing {
                        all_positions.push(*pos);
                    }
                    all_positions.sort_unstable();
                    all_positions.dedup();
                }

                // Write delete file
                let delete_file_name = format!("ducklake-{}-delete.parquet", Uuid::new_v4());
                let schema_table_prefix = table_path.trim_start_matches('/');
                let delete_object_key = join_paths(schema_table_prefix, &delete_file_name)?;
                let delete_object_path =
                    ObjectPath::from(delete_object_key.trim_start_matches('/'));

                let del_schema = delete_file_schema();
                // R3F-034: delete_count tracks total positions in delete file, not just new matches
                let total_delete_count = i64::try_from(all_positions.len())
                    .map_err(|e| DataFusionError::Execution(format!("Delete count overflow: {}", e)))?;
                // R4-S-009: Use resolved path (from data_path root) instead of raw catalog filename
                let file_path_values: Vec<&str> = vec![resolved_path.as_str(); all_positions.len()];
                let file_path_array: ArrayRef = Arc::new(StringArray::from(file_path_values));
                let pos_array: ArrayRef = Arc::new(Int64Array::from(all_positions));

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

                let file_size = i64::try_from(buffer.len())
                    .map_err(|e| DataFusionError::Execution(format!("File size overflow: {}", e)))?;
                let footer_size = calculate_footer_size_from_bytes(&buffer)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                object_store
                    .put(&delete_object_path, PutPayload::from(buffer))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                uploaded_files.push(delete_object_path);

                let delete_file_info = DeleteFileInfo::new(
                    data_file_id,
                    &delete_file_name,
                    file_size,
                    total_delete_count,
                )
                .with_footer_size(footer_size);

                pending_delete_files.push(delete_file_info);
            }

            // Add matched source rows as replacement data (for UPDATE)
            new_data_batches.extend(matched_source_rows);

            // Collect unmatched source rows for INSERT
            if insert_unmatched {
                let mut source_global_idx = 0usize;
                for src_batch in &source_batches {
                    let mut mask_values = vec![false; src_batch.num_rows()];
                    for (i, mask_val) in mask_values.iter_mut().enumerate() {
                        if source_match_count[source_global_idx + i] == 0 {
                            *mask_val = true;
                        }
                    }
                    let unmatched_mask = BooleanArray::from(mask_values);
                    if unmatched_mask.true_count() > 0 {
                        let filtered = compute::filter_record_batch(src_batch, &unmatched_mask)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        total_affected += u64::try_from(filtered.num_rows())
                            .map_err(|e| DataFusionError::Execution(format!("Row count overflow: {}", e)))?;
                        new_data_batches.push(filtered);
                    }
                    source_global_idx += src_batch.num_rows();
                }
            }

            // Enforce NOT NULL constraints on data to be written
            crate::table_writer::validate_not_null_constraints(
                &table_schema,
                &new_data_batches,
            )?;

            // Write new data file(s) for updated + inserted rows
            let mut pending_data_files: Vec<DataFileInfo> = Vec::new();
            if !new_data_batches.is_empty() {
                let data_file_name = format!("ducklake-{}.parquet", Uuid::new_v4());

                // Use the catalog's stored table_path instead of deriving from names,
                // so writes go to the correct location even after table rename.
                let object_key = join_paths(table_path.trim_start_matches('/'), &data_file_name)?;
                let data_object_path = ObjectPath::from(object_key.trim_start_matches('/'));

                let write_schema = Arc::new(
                    build_schema_with_field_ids(&table_schema, &column_ids)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                );

                let props = WriterProperties::builder()
                    .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
                    .build();
                let mut arrow_writer =
                    ArrowWriter::try_new(Vec::new(), write_schema.clone(), Some(props))
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let mut total_records: i64 = 0;
                for batch in &new_data_batches {
                    let batch_with_ids =
                        RecordBatch::try_new(write_schema.clone(), batch.columns().to_vec())?;
                    total_records += i64::try_from(batch_with_ids.num_rows())
                        .map_err(|e| DataFusionError::Execution(format!("Row count overflow: {}", e)))?;
                    arrow_writer
                        .write(&batch_with_ids)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                }

                let buffer = arrow_writer
                    .into_inner()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let file_size = i64::try_from(buffer.len())
                    .map_err(|e| DataFusionError::Execution(format!("File size overflow: {}", e)))?;
                let footer_size = calculate_footer_size_from_bytes(&buffer)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                object_store
                    .put(&data_object_path, PutPayload::from(buffer))
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                uploaded_files.push(data_object_path);

                let data_file_info = DataFileInfo::new(&data_file_name, file_size, total_records)
                    .with_footer_size(footer_size);

                pending_data_files.push(data_file_info);
            }

            // R3F-032: Skip snapshot creation if no rows were affected
            if total_affected == 0 {
                let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
                return Ok(RecordBatch::try_new(output_schema, vec![count_array])?);
            }

            // Create a snapshot for this merge operation (deferred until we know rows are affected)
            let snapshot_id = writer
                .create_snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // R3F-003: Atomically register all delete files and data files with cleanup on failure
            if let Err(e) = writer.register_dml_files(
                table_id,
                snapshot_id,
                &pending_delete_files,
                &pending_data_files,
            ) {
                cleanup_orphaned_files(&*object_store, &uploaded_files).await;
                return Err(DataFusionError::External(Box::new(e)));
            }

            // R3F-013: Record snapshot changes for MERGE
            // R4-S-008: Use standard DuckDB tokens (inserted + deleted) instead of non-standard "merged_into_table"
            writer
                .record_snapshot_changes(
                    snapshot_id,
                    &format!(
                        "inserted_into_table:{},deleted_from_table:{}",
                        table_id, table_id
                    ),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![total_affected]));
            Ok(RecordBatch::try_new(output_schema, vec![count_array])?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            make_merge_count_schema(),
            stream,
        )))
    }
}
