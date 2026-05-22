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
//!
//! ## Concurrency
//!
//! Like DELETE (#17) and UPDATE (#18), MERGE captures the table's
//! `snapshot_id` at plan time and threads it through
//! [`MetadataWriter::register_dml_files`] as `since_snapshot`. Inside the
//! writer's transaction, any data file that this MERGE targets and that was
//! ended (or had a newer active delete file installed on it) since
//! `since_snapshot` causes the commit to fail with `TransactionConflict` and
//! the `UploadCleanupGuard` removes any uploaded files.
//!
//! **Granularity:** the writer's conflict check is file-level — two MERGEs
//! planned at the same snapshot that target the same `data_file_id` will
//! conflict even when they touch disjoint row positions. This is conservative
//! but correct; refining it to be row-position-aware is tracked as a
//! follow-up (the same deferral applies to DELETE and UPDATE).
//!
//! ## Memory bounds
//!
//! MERGE buffers updated/inserted rows in memory before writing them as a
//! single new data file. The cap is read at execute time from
//! [`crate::config::DuckLakeConfig::max_buffered_rows_per_dml`] (default
//! 10M). Raise it via session config for legitimate large MERGEs.
//!
//! ## Write atomicity / NOT NULL pre-validation
//!
//! MERGE executes in three phases so that a NOT NULL violation can never
//! leave orphan files on the object store:
//!
//! 1. **Build phase (in-memory):** scan target files, hash-join against
//!    source, apply UPDATE SET to matched rows, collect INSERT rows, and
//!    validate NOT NULL on the buffered batches. No object-store I/O yet.
//! 2. **Delete-file upload:** only runs if phase 1 completes without error.
//! 3. **New data-file upload:** writes the combined updated + inserted batches.
//!
//! Any failure after phase 1 still triggers `UploadCleanupGuard`-based
//! removal of any uploaded files.
//!
//! ## Hash-key signed / unsigned collation
//!
//! The hash key extractor coerces every signed-integer width
//! (`Int8`/`Int16`/`Int32`/`Int64`, plus the temporal types stored as `i64`)
//! into a single [`HashableKeyValue::Int64`] slot, and every unsigned-integer
//! width into a single [`HashableKeyValue::UInt64`] slot, via lossy `as`
//! casts. This means:
//!
//! - **Two keys of the same Arrow signedness** always compare correctly:
//!   `Int8(-1)` and `Int64(-1)` hash equal and compare equal, which is the
//!   intended SQL semantics for an equi-join across promoted integer widths.
//! - **Cross-signedness equality is NOT defined by this exec.** Joining a
//!   target `Int64` column against a source `UInt64` column means the two
//!   values inhabit *different* [`HashableKeyValue`] discriminants. They will
//!   hash to different buckets and `PartialEq` will return false — which is
//!   correct: a signed/unsigned compare in SQL needs a type promotion at the
//!   planner level (to a wider common type) before MERGE sees the data. The
//!   audit verdict on this code flagged that "large `UInt64` keys can
//!   collide with negative `Int64`s when mixed types share a hash slot" —
//!   that scenario would require both source and target to be cast to the
//!   same signedness before reaching this exec, which the planner enforces.
//!   If a future code path delivers mismatched-signedness columns into this
//!   exec, they will *not* falsely match, but they also will *not* be
//!   detected and rejected here; the keys simply sort into disjoint hash
//!   slots. Documented as a deliberate design choice (the discriminant
//!   prefix in `Hash` for `HashableKeyValue` is what enforces this) — see
//!   the `test_hash_key_signed_unsigned_disjoint` test below.
//!
//! No DuckLake-side coercion is attempted; the planner is responsible for
//! aligning the source and target join-key types before invoking MERGE.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch, UInt64Array};
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

use crate::metadata_provider::DuckLakeTableFile;
use crate::metadata_writer::{DataFileInfo, DeleteFileInfo, MetadataWriter};

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
    table_files: Arc<Vec<DuckLakeTableFile>>,
    /// Source data to merge (pre-collected RecordBatches).
    /// Wrapped in Arc to prevent deep cloning during optimizer passes.
    source_batches: Arc<Vec<RecordBatch>>,
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
    existing_deletes: Arc<HashMap<String, HashSet<i64>>>,
    /// Snapshot id this MERGE was planned against. Threaded through to
    /// `MetadataWriter::register_dml_files` for optimistic-concurrency
    /// conflict detection — a concurrent DML that ended any of this MERGE's
    /// target data files (or installed a newer active delete file on them)
    /// after `since_snapshot` will cause the commit to fail with
    /// `TransactionConflict` (matching the path #17 added for DELETE and
    /// #18 for UPDATE).
    since_snapshot: i64,
    /// Cached plan properties
    cache: Arc<PlanProperties>,
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
        since_snapshot: i64,
    ) -> Self {
        let cache = Self::compute_properties();
        Self {
            table_id,
            table_name,
            table_schema,
            column_ids,
            table_files: Arc::new(table_files),
            source_batches: Arc::new(source_batches),
            join_key_pairs,
            matched_action,
            insert_unmatched,
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

    macro_rules! downcast_key {
        ($arr_type:ty) => {
            col.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "MERGE: failed to downcast to {}",
                    stringify!($arr_type)
                ))
            })
        };
    }

    match col.data_type() {
        DataType::Boolean => {
            let a = downcast_key!(BooleanArray)?;
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
            let a = downcast_key!(Float32Array)?;
            let v = a.value(row);
            let bits = if v.is_nan() {
                f32::NAN.to_bits()
            } else {
                v.to_bits()
            };
            Ok(HashableKeyValue::Float32Bits(bits))
        },
        DataType::Float64 => {
            let a = downcast_key!(Float64Array)?;
            let v = a.value(row);
            let bits = if v.is_nan() {
                f64::NAN.to_bits()
            } else {
                v.to_bits()
            };
            Ok(HashableKeyValue::Float64Bits(bits))
        },
        DataType::Utf8 => {
            let a = downcast_key!(StringArray)?;
            Ok(HashableKeyValue::String(a.value(row).to_string()))
        },
        DataType::LargeUtf8 => {
            let a = downcast_key!(LargeStringArray)?;
            Ok(HashableKeyValue::String(a.value(row).to_string()))
        },
        DataType::Date32 => extract_int!(Date32Array),
        DataType::Date64 => extract_int!(Date64Array),
        DataType::Timestamp(TimeUnit::Second, _) => extract_int!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => extract_int!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => extract_int!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => extract_int!(TimestampNanosecondArray),
        DataType::Decimal128(_, _) => {
            let a = downcast_key!(Decimal128Array)?;
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
        let table_files = Arc::clone(&self.table_files);
        let source_batches = Arc::clone(&self.source_batches);
        let join_key_pairs = self.join_key_pairs.clone();
        let matched_action = self.matched_action.clone();
        let insert_unmatched = self.insert_unmatched;
        let writer = Arc::clone(&self.writer);
        let object_store_url = self.object_store_url.clone();
        let table_path = self.table_path.clone();
        let existing_deletes = Arc::clone(&self.existing_deletes);
        let since_snapshot = self.since_snapshot;
        let output_schema = make_dml_count_schema();

        // Honour the session-config override for `max_buffered_rows_per_dml`
        // if present; otherwise fall back to the default. Shared with UPDATE
        // (#18); MERGE opts into the same safety valve since both buffer
        // arbitrary numbers of source/matched/insert rows before writing.
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

            let has_matched_action = matched_action.is_some();

            let mut total_affected: u64 = 0;
            let mut new_data_batches: Vec<RecordBatch> = Vec::new();
            // Buffered-row counter (matched-source + unmatched-insert combined).
            // Bounds the in-memory write set against the configurable cap.
            let mut buffered_rows: usize = 0;
            // Collect file metadata for atomic registration
            let mut pending_delete_files: Vec<DeleteFileInfo> = Vec::new();
            // Cleanup guard ensures orphan files are removed on any error path.
            // Initialised early so subsequent uploads register their `ObjectPath`
            // with the guard and are removed on any `?`-propagated error.
            let mut upload_guard =
                crate::table_writer::UploadCleanupGuard::new(Arc::clone(&object_store));

            // Build hash index on source join keys for O(1) lookups instead of O(N*M)
            let source_hash_index = build_source_hash_index(&*source_batches, &join_key_pairs)?;
            let target_col_indices: Vec<usize> = join_key_pairs.iter().map(|&(t, _)| t).collect();

            // Track how many target rows each source row has matched
            // R3F-033: SQL standard requires error when source row matches multiple targets
            let total_source_rows: usize = source_batches.iter().map(|b| b.num_rows()).sum();
            let mut source_match_count = vec![0u32; total_source_rows];

            // Precompute cumulative source batch offsets for global index computation
            let source_batch_offsets: Vec<usize> = {
                let mut offsets = Vec::with_capacity(source_batches.len());
                let mut acc = 0usize;
                for b in &*source_batches {
                    offsets.push(acc);
                    acc += b.num_rows();
                }
                offsets
            };

            // For UPDATE: collect matched source rows to write as replacement data
            let mut matched_source_rows: Vec<RecordBatch> = Vec::new();

            // Phase 1 (in-memory build): per target file, hash-join, compute
            // positions to delete and matched-source row masks, but DO NOT
            // upload any files yet. After this loop completes, NOT NULL
            // validation runs on the buffered data set. Only then does phase
            // 2 perform the disk I/O.
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

                // Reusable key buffer to avoid per-row Vec allocation
                let mut target_key: Vec<HashableKeyValue> =
                    Vec::with_capacity(target_col_indices.len());

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
                        target_key.clear();
                        for &col_idx in &target_col_indices {
                            target_key.push(extract_key_value(
                                batch.column(col_idx).as_ref(),
                                target_row_idx,
                            )?);
                        }

                        // NULL keys never match (SQL semantics)
                        if target_key
                            .iter()
                            .any(|k| matches!(k, HashableKeyValue::Null))
                        {
                            continue;
                        }

                        if let Some(candidates) = source_hash_index.get(&target_key) {
                            // R11-S-003: SQL standard requires error when multiple source
                            // rows match the same target row.
                            if candidates.len() > 1 {
                                return Err(DataFusionError::Execution(
                                    "MERGE violation: multiple source rows matched the same \
                                     target row. SQL standard requires each target row to be \
                                     matched by at most one source row."
                                        .to_string(),
                                ));
                            }

                            // First match is sufficient — but we still need to
                            // record the source-row match for the SQL-standard
                            // "source row matches more than one target" check.
                            if let Some(&(batch_idx, src_row_idx)) = candidates.first() {
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
                total_affected = total_affected.checked_add(new_match_count).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Total affected row count overflow: {} + {} exceeds u64::MAX",
                        total_affected, new_match_count
                    ))
                })?;

                // For UPDATE: collect the matched source rows (these replace the deleted target rows)
                if matches!(&matched_action, Some(MergeMatchedAction::Update)) {
                    for (batch_idx, src_batch) in source_batches.iter().enumerate() {
                        let mask = BooleanArray::from(source_match_masks[batch_idx].clone());
                        if mask.true_count() > 0 {
                            let filtered = compute::filter_record_batch(src_batch, &mask)
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                            buffered_rows =
                                buffered_rows.checked_add(filtered.num_rows()).ok_or_else(
                                    || {
                                        DataFusionError::Execution(
                                            "MERGE buffered_rows overflow".to_string(),
                                        )
                                    },
                                )?;
                            if buffered_rows > max_buffered_rows {
                                return Err(DataFusionError::ResourcesExhausted(format!(
                                    "MERGE affects too many rows ({} rows buffered, limit is {}). \
                                     Raise `ducklake.max_buffered_rows_per_dml` in the session \
                                     config or use a more selective ON / source filter.",
                                    buffered_rows, max_buffered_rows
                                )));
                            }
                            matched_source_rows.push(filtered);
                        }
                    }
                }

                per_file_work.push(PerFileWork {
                    resolved_path,
                    data_file_id,
                    positions_to_delete,
                    existing_positions: existing_positions.cloned(),
                });
            }

            // Add matched source rows as replacement data (for UPDATE)
            new_data_batches.extend(matched_source_rows);

            // Collect unmatched source rows for INSERT
            if insert_unmatched {
                let mut source_global_idx = 0usize;
                for src_batch in &*source_batches {
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
                        let unmatched_count = u64::try_from(filtered.num_rows()).map_err(|e| {
                            DataFusionError::Execution(format!("Row count overflow: {}", e))
                        })?;
                        total_affected =
                            total_affected.checked_add(unmatched_count).ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "Total affected row count overflow: {} + {} exceeds u64::MAX",
                                    total_affected, unmatched_count
                                ))
                            })?;
                        buffered_rows =
                            buffered_rows.checked_add(filtered.num_rows()).ok_or_else(|| {
                                DataFusionError::Execution(
                                    "MERGE buffered_rows overflow".to_string(),
                                )
                            })?;
                        if buffered_rows > max_buffered_rows {
                            return Err(DataFusionError::ResourcesExhausted(format!(
                                "MERGE affects too many rows ({} rows buffered, limit is {}). \
                                 Raise `ducklake.max_buffered_rows_per_dml` in the session \
                                 config or use a more selective ON / source filter.",
                                buffered_rows, max_buffered_rows
                            )));
                        }
                        new_data_batches.push(filtered);
                    }
                    source_global_idx += src_batch.num_rows();
                }
            }

            // Enforce NOT NULL constraints on data to be written — BEFORE
            // any disk I/O. Mirrors the reorder #18 applied to UPDATE: a
            // failing constraint must never leave orphan files on the
            // object store, so we validate in memory before phase 2
            // uploads any delete file or new data file.
            crate::table_writer::validate_not_null_constraints(&table_schema, &new_data_batches)?;

            // Phase 2: now that all in-memory work has passed (including
            // NOT NULL validation), upload the per-file delete files. Any
            // error from this point on cleans up via `upload_guard`.
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

            // Phase 3: write new data file(s) for updated + inserted rows.
            // NOT NULL was already validated above, but the helper re-runs
            // the check as a safety net in case future refactors reorder
            // phases.
            let mut pending_data_files: Vec<DataFileInfo> = Vec::new();
            if !new_data_batches.is_empty() {
                let data_file_info = crate::table_writer::write_and_upload_parquet(
                    &new_data_batches,
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
            if total_affected == 0 {
                let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
                return Ok(RecordBatch::try_new(output_schema, vec![count_array])?);
            }

            // Create snapshot (guard cleans up uploaded files on error)
            let snapshot_id = writer
                .create_snapshot()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Atomically register all delete files and data files (guard
            // cleans up on error). Passing `since_snapshot` opts MERGE into
            // the optimistic-concurrency conflict detection DELETE got in
            // #17 and UPDATE in #18: any concurrent DML that ended one of
            // this MERGE's target data files (or installed a newer active
            // delete file on it) since this MERGE was planned will cause
            // `register_dml_files` to fail with `TransactionConflict`, and
            // `upload_guard` will remove the orphan files this MERGE
            // uploaded.
            //
            // Granularity: file-level (keyed on `data_file_id`). Two MERGEs
            // that target the same file conflict even if they touch
            // disjoint row positions; refining this to be row-position-aware
            // is tracked as a follow-up (same deferral as #17/#18).
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

    /// Mixed-signedness collation: a target keyed `Int64(-1)` and a source
    /// keyed `UInt64(u64::MAX)` MUST NOT collide in the hash index. Both
    /// values would `as i64` to `-1` in the extractor's lossy cast, but the
    /// `HashableKeyValue` discriminant prefix in `Hash` (and the enum
    /// `PartialEq`) keeps them in disjoint slots.
    ///
    /// This is the unit-level expression of the documented behavior at the
    /// top of this module; an end-to-end MERGE test lives in
    /// `tests/merge_tests.rs::test_merge_mixed_signedness_keys_do_not_match`.
    #[test]
    fn test_hash_key_signed_unsigned_disjoint() {
        // Build a source hash index over a UInt64 batch containing
        // `u64::MAX`. Probe with an Int64(-1) key — must miss.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef],
        )
        .unwrap();
        let idx = build_source_hash_index(&[batch], &[(0, 0)]).unwrap();

        let signed_probe = vec![HashableKeyValue::Int64(-1)];
        assert!(
            !idx.contains_key(&signed_probe),
            "Int64(-1) probe must not collide with UInt64(u64::MAX) source key"
        );

        // The matching unsigned probe is present.
        let unsigned_probe = vec![HashableKeyValue::UInt64(u64::MAX)];
        let hit = idx.get(&unsigned_probe).expect("UInt64 key should be present");
        assert_eq!(hit.len(), 1);

        // Symmetric direction: hash over an Int64(-1) batch, probe with
        // UInt64(u64::MAX) — must miss.
        let schema2 = Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::Int64,
            false,
        )]));
        let batch2 = RecordBatch::try_new(
            schema2,
            vec![Arc::new(Int64Array::from(vec![-1i64])) as ArrayRef],
        )
        .unwrap();
        let idx2 = build_source_hash_index(&[batch2], &[(0, 0)]).unwrap();
        let cross_probe = vec![HashableKeyValue::UInt64(u64::MAX)];
        assert!(
            !idx2.contains_key(&cross_probe),
            "UInt64(u64::MAX) probe must not collide with Int64(-1) source key"
        );

        // Direct PartialEq sanity: the discriminants make these unequal.
        assert_ne!(HashableKeyValue::Int64(-1), HashableKeyValue::UInt64(u64::MAX));
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
