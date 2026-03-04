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
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::compute;
use arrow::datatypes::{DataType, SchemaRef};
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
    extract_column_stats,
};

use crate::delete_exec::make_dml_count_schema;

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
            EquivalenceProperties::new(make_dml_count_schema()),
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

/// A hashable key value extracted from an Arrow array for hash-based join lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HashableKeyValue {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    /// Floats use bit-level comparison (NaN == NaN for SQL semantics)
    Float32Bits(u32),
    Float64Bits(u64),
    String(String),
    Decimal128(i128),
}

impl Hash for HashableKeyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            HashableKeyValue::Null => {},
            HashableKeyValue::Bool(v) => v.hash(state),
            HashableKeyValue::Int64(v) => v.hash(state),
            HashableKeyValue::UInt64(v) => v.hash(state),
            HashableKeyValue::Float32Bits(v) => v.hash(state),
            HashableKeyValue::Float64Bits(v) => v.hash(state),
            HashableKeyValue::String(v) => v.hash(state),
            HashableKeyValue::Decimal128(v) => v.hash(state),
        }
    }
}

/// Extract a hashable key value from an Arrow array at a given row index.
fn extract_key_value(
    col: &dyn arrow::array::Array,
    row: usize,
) -> DataFusionResult<HashableKeyValue> {
    use arrow::array::*;
    use arrow::datatypes::TimeUnit;

    if col.is_null(row) {
        return Ok(HashableKeyValue::Null);
    }

    macro_rules! extract_int {
        ($arr_type:ty) => {{
            let a = col.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "MERGE: failed to downcast to {}",
                    stringify!($arr_type)
                ))
            })?;
            Ok(HashableKeyValue::Int64(a.value(row) as i64))
        }};
    }

    macro_rules! extract_uint {
        ($arr_type:ty) => {{
            let a = col.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "MERGE: failed to downcast to {}",
                    stringify!($arr_type)
                ))
            })?;
            Ok(HashableKeyValue::UInt64(a.value(row) as u64))
        }};
    }

    match col.data_type() {
        DataType::Boolean => {
            let a = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(HashableKeyValue::Bool(a.value(row)))
        },
        DataType::Int8 => extract_int!(Int8Array),
        DataType::Int16 => extract_int!(Int16Array),
        DataType::Int32 => extract_int!(Int32Array),
        DataType::Int64 => extract_int!(Int64Array),
        DataType::UInt8 => extract_uint!(UInt8Array),
        DataType::UInt16 => extract_uint!(UInt16Array),
        DataType::UInt32 => extract_uint!(UInt32Array),
        DataType::UInt64 => extract_uint!(UInt64Array),
        DataType::Float32 => {
            let a = col.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = a.value(row);
            // Normalize NaN to a canonical bit pattern for consistent hashing
            let bits = if v.is_nan() {
                f32::NAN.to_bits()
            } else {
                v.to_bits()
            };
            Ok(HashableKeyValue::Float32Bits(bits))
        },
        DataType::Float64 => {
            let a = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = a.value(row);
            let bits = if v.is_nan() {
                f64::NAN.to_bits()
            } else {
                v.to_bits()
            };
            Ok(HashableKeyValue::Float64Bits(bits))
        },
        DataType::Utf8 => {
            let a = col.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(HashableKeyValue::String(a.value(row).to_string()))
        },
        DataType::LargeUtf8 => {
            let a = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(HashableKeyValue::String(a.value(row).to_string()))
        },
        DataType::Date32 => extract_int!(Date32Array),
        DataType::Date64 => extract_int!(Date64Array),
        DataType::Timestamp(TimeUnit::Second, _) => extract_int!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => extract_int!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => extract_int!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => extract_int!(TimestampNanosecondArray),
        DataType::Decimal128(_, _) => {
            let a = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(HashableKeyValue::Decimal128(a.value(row)))
        },
        dt => Err(DataFusionError::NotImplemented(format!(
            "MERGE hash join key not supported for data type: {dt:?}"
        ))),
    }
}

/// Build a hash index from source batches for O(1) join key lookups.
/// Returns a map from composite key → list of (batch_index, row_index).
fn build_source_hash_index(
    source_batches: &[RecordBatch],
    join_key_pairs: &[(usize, usize)],
) -> DataFusionResult<HashMap<Vec<HashableKeyValue>, Vec<(usize, usize)>>> {
    let source_col_indices: Vec<usize> = join_key_pairs.iter().map(|&(_, s)| s).collect();
    let mut index: HashMap<Vec<HashableKeyValue>, Vec<(usize, usize)>> = HashMap::new();

    for (batch_idx, batch) in source_batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            let key: Vec<HashableKeyValue> = source_col_indices
                .iter()
                .map(|&col_idx| extract_key_value(batch.column(col_idx).as_ref(), row_idx))
                .collect::<DataFusionResult<Vec<_>>>()?;
            index.entry(key).or_default().push((batch_idx, row_idx));
        }
    }

    Ok(index)
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
        let output_schema = make_dml_count_schema();

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

            // Build hash index on source join keys for O(1) lookups instead of O(N*M)
            let source_hash_index = build_source_hash_index(&source_batches, &join_key_pairs)?;
            let target_col_indices: Vec<usize> = join_key_pairs.iter().map(|&(t, _)| t).collect();

            // Track how many target rows each source row has matched
            // R3F-033: SQL standard requires error when source row matches multiple targets
            let total_source_rows: usize = source_batches.iter().map(|b| b.num_rows()).sum();
            let mut source_match_count = vec![0u32; total_source_rows];

            // Precompute cumulative source batch offsets for global index computation
            let source_batch_offsets: Vec<usize> = {
                let mut offsets = Vec::with_capacity(source_batches.len());
                let mut acc = 0usize;
                for b in &source_batches {
                    offsets.push(acc);
                    acc += b.num_rows();
                }
                offsets
            };

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

                while let Some(batch) = parquet_stream.try_next().await? {
                    let num_rows = batch.num_rows();

                    for target_row_idx in 0..num_rows {
                        let target_row_i64 = i64::try_from(target_row_idx).map_err(|e| {
                            DataFusionError::Execution(format!("Row index overflow: {}", e))
                        })?;
                        let global_pos = global_row_offset + target_row_i64;

                        // Skip already-deleted rows
                        if let Some(existing) = existing_positions {
                            if existing.contains(&global_pos) {
                                continue;
                            }
                        }

                        // Build target key and look up in hash index (O(1) instead of O(M))
                        let target_key: Vec<HashableKeyValue> = target_col_indices
                            .iter()
                            .map(|&col_idx| {
                                extract_key_value(batch.column(col_idx).as_ref(), target_row_idx)
                            })
                            .collect::<DataFusionResult<Vec<_>>>()?;

                        // NULL keys never match (SQL semantics)
                        if target_key
                            .iter()
                            .any(|k| matches!(k, HashableKeyValue::Null))
                        {
                            continue;
                        }

                        if let Some(candidates) = source_hash_index.get(&target_key) {
                            for &(batch_idx, src_row_idx) in candidates {
                                let src_global = source_batch_offsets[batch_idx] + src_row_idx;
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
                                    source_match_masks[batch_idx][src_row_idx] = true;
                                }
                                break; // First match is sufficient
                            }
                        }
                    }

                    global_row_offset += i64::try_from(num_rows).map_err(|e| {
                        DataFusionError::Execution(format!("Row count overflow: {}", e))
                    })?;
                }

                // Skip writing delete files if no positions to delete
                if positions_to_delete.is_empty() {
                    continue;
                }

                let new_match_count = u64::try_from(positions_to_delete.len()).map_err(|e| {
                    DataFusionError::Execution(format!("Delete count overflow: {}", e))
                })?;
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
                let total_delete_count = i64::try_from(all_positions.len()).map_err(|e| {
                    DataFusionError::Execution(format!("Delete count overflow: {}", e))
                })?;
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

                let file_size = i64::try_from(buffer.len()).map_err(|e| {
                    DataFusionError::Execution(format!("File size overflow: {}", e))
                })?;
                let footer_size = calculate_footer_size_from_bytes(&buffer)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // R6-S-037: Clean up prior uploads on failure
                if let Err(e) = object_store
                    .put(&delete_object_path, PutPayload::from(buffer))
                    .await
                {
                    cleanup_orphaned_files(&*object_store, &uploaded_files).await;
                    return Err(DataFusionError::External(Box::new(e)));
                }
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
                        total_affected += u64::try_from(filtered.num_rows()).map_err(|e| {
                            DataFusionError::Execution(format!("Row count overflow: {}", e))
                        })?;
                        new_data_batches.push(filtered);
                    }
                    source_global_idx += src_batch.num_rows();
                }
            }

            // Enforce NOT NULL constraints on data to be written
            crate::table_writer::validate_not_null_constraints(&table_schema, &new_data_batches)?;

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
                    total_records += i64::try_from(batch_with_ids.num_rows()).map_err(|e| {
                        DataFusionError::Execution(format!("Row count overflow: {}", e))
                    })?;
                    arrow_writer
                        .write(&batch_with_ids)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                }

                // R4-S-005: Extract column stats before consuming the writer
                arrow_writer
                    .flush()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let column_stats =
                    extract_column_stats(arrow_writer.flushed_row_groups(), &column_ids);

                let buffer = arrow_writer
                    .into_inner()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let file_size = i64::try_from(buffer.len()).map_err(|e| {
                    DataFusionError::Execution(format!("File size overflow: {}", e))
                })?;
                let footer_size = calculate_footer_size_from_bytes(&buffer)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // R6-S-037: Clean up prior uploads on failure
                if let Err(e) = object_store
                    .put(&data_object_path, PutPayload::from(buffer))
                    .await
                {
                    cleanup_orphaned_files(&*object_store, &uploaded_files).await;
                    return Err(DataFusionError::External(Box::new(e)));
                }
                uploaded_files.push(data_object_path);

                let data_file_info = DataFileInfo::new(&data_file_name, file_size, total_records)
                    .with_footer_size(footer_size)
                    .with_column_stats(column_stats);

                pending_data_files.push(data_file_info);
            }

            // R3F-032: Skip snapshot creation if no rows were affected
            if total_affected == 0 {
                let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
                return Ok(RecordBatch::try_new(output_schema, vec![count_array])?);
            }

            // R6-S-038: Create snapshot; clean up uploads if snapshot creation fails
            let snapshot_id = match writer.create_snapshot() {
                Ok(id) => id,
                Err(e) => {
                    cleanup_orphaned_files(&*object_store, &uploaded_files).await;
                    return Err(DataFusionError::External(Box::new(e)));
                },
            };

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
            // R5-S-064: Only record actual operations (insert and/or delete) instead of always both
            // R4-S-016: Non-fatal — DML data is already committed
            let has_deletes = !pending_delete_files.is_empty();
            let has_inserts = !pending_data_files.is_empty();
            let changes = match (has_inserts, has_deletes) {
                (true, true) => format!(
                    "inserted_into_table:{},deleted_from_table:{}",
                    table_id, table_id
                ),
                (true, false) => format!("inserted_into_table:{}", table_id),
                (false, true) => format!("deleted_from_table:{}", table_id),
                (false, false) => String::new(), // shouldn't happen since total_affected > 0
            };
            if let Err(e) = writer.record_snapshot_changes(snapshot_id, &changes) {
                tracing::warn!(
                    snapshot_id,
                    error = %e,
                    "Failed to record snapshot changes after MERGE commit"
                );
            }

            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![total_affected]));
            Ok(RecordBatch::try_new(output_schema, vec![count_array])?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            make_dml_count_schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn test_extract_key_value_nan_equality() {
        // R5-S-021: NaN values should hash to the same key (SQL semantics: NaN = NaN)
        let f32_array = Float32Array::from(vec![f32::NAN, 1.0, f32::NAN]);
        let key1 = extract_key_value(&f32_array, 0).unwrap();
        let key2 = extract_key_value(&f32_array, 2).unwrap();
        assert_eq!(
            key1, key2,
            "NaN Float32 values should produce equal hash keys"
        );

        let f64_array = Float64Array::from(vec![f64::NAN, 1.0, f64::NAN]);
        let key1 = extract_key_value(&f64_array, 0).unwrap();
        let key2 = extract_key_value(&f64_array, 2).unwrap();
        assert_eq!(
            key1, key2,
            "NaN Float64 values should produce equal hash keys"
        );
    }

    #[test]
    fn test_extract_key_value_null() {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        let key = extract_key_value(&array, 1).unwrap();
        assert!(matches!(key, HashableKeyValue::Null));
    }

    #[test]
    fn test_extract_key_value_various_types() {
        let i32_arr = Int32Array::from(vec![42]);
        assert!(matches!(
            extract_key_value(&i32_arr, 0).unwrap(),
            HashableKeyValue::Int64(42)
        ));

        let str_arr = StringArray::from(vec!["hello"]);
        assert!(matches!(
            extract_key_value(&str_arr, 0).unwrap(),
            HashableKeyValue::String(ref s) if s == "hello"
        ));

        let bool_arr = BooleanArray::from(vec![true]);
        assert!(matches!(
            extract_key_value(&bool_arr, 0).unwrap(),
            HashableKeyValue::Bool(true)
        ));
    }

    #[test]
    fn test_build_source_hash_index() {
        // R5-S-035: Hash index should provide O(1) lookups
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
        )
        .unwrap();

        // Join on column 0 (id)
        let join_pairs = vec![(0, 0)];
        let index = build_source_hash_index(&[batch], &join_pairs).unwrap();

        // Should have 3 entries
        assert_eq!(index.len(), 3);

        // Look up key=2 → should find (batch_idx=0, row_idx=1)
        let key = vec![HashableKeyValue::Int64(2)];
        let candidates = index.get(&key).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], (0, 1));
    }

    #[test]
    fn test_build_source_hash_index_nan_keys() {
        // R5-S-021: NaN keys should be found via hash lookup
        let schema = Arc::new(Schema::new(vec![Field::new(
            "key",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![f64::NAN, 1.0, 2.0])) as ArrayRef],
        )
        .unwrap();

        let join_pairs = vec![(0, 0)];
        let index = build_source_hash_index(&[batch], &join_pairs).unwrap();

        // Look up NaN key
        let nan_key = vec![HashableKeyValue::Float64Bits(f64::NAN.to_bits())];
        let candidates = index.get(&nan_key).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], (0, 0));
    }

    #[test]
    fn test_build_source_hash_index_composite_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "a"])) as ArrayRef,
            ],
        )
        .unwrap();

        // Join on both columns
        let join_pairs = vec![(0, 0), (1, 1)];
        let index = build_source_hash_index(&[batch], &join_pairs).unwrap();

        // 3 unique composite keys: (1,"a"), (1,"b"), (2,"a")
        assert_eq!(index.len(), 3);
    }

    #[test]
    fn test_dml_count_schema() {
        // R5-S-045: Shared schema function
        let schema = make_dml_count_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "count");
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
    }
}
