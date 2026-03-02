//! Table deletions functionality for DuckLake
//!
//! This module provides the `ducklake_table_deletions()` table function that returns
//! the actual deleted rows between snapshots, with CDC metadata columns.
//!
//! For each data file with deletions:
//! 1. Read positions from current delete file (or all positions for full file delete)
//! 2. Subtract positions from previous delete file (if exists)
//! 3. Read the data file and return only the rows at the newly deleted positions
//! 4. Append CDC columns (snapshot_id, change_type='delete')

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;

use crate::metadata_provider::{DeleteFileChange, MetadataProvider};
use crate::path_resolver::resolve_path;
use crate::table::{delete_file_schema, validated_file_size};

/// Projection analysis result for deletions table
struct DeletionProjectionInfo {
    /// Table column indices to read from Parquet (in original order)
    table_indices: Vec<usize>,
    /// Whether snapshot_id is requested
    need_snapshot_id: bool,
    /// Whether change_type is requested
    need_change_type: bool,
    /// The projected output schema
    output_schema: SchemaRef,
    /// Maps from natural column order (table_cols + CDC cols) to projection order.
    /// None when no reordering is needed.
    reorder_indices: Option<Vec<usize>>,
}

/// TableProvider that exposes deleted rows between snapshots
///
/// For each data file with deletions:
/// 1. Read positions from current delete file (or generate all positions for full file delete)
/// 2. Subtract positions from previous delete file (if exists)
/// 3. Read the data file and filter to only deleted row positions
/// 4. Append snapshot_id and change_type columns
#[derive(Debug)]
pub struct TableDeletionsTable {
    provider: Arc<dyn MetadataProvider>,
    table_id: i64,
    start_snapshot: i64,
    end_snapshot: i64,
    object_store_url: Arc<ObjectStoreUrl>,
    table_path: String,
    /// Original table schema (without CDC columns)
    table_schema: SchemaRef,
    /// Combined schema: table columns + snapshot_id + change_type
    output_schema: SchemaRef,
}

impl TableDeletionsTable {
    pub fn new(
        provider: Arc<dyn MetadataProvider>,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
        object_store_url: Arc<ObjectStoreUrl>,
        table_path: String,
        table_schema: SchemaRef,
    ) -> Self {
        // Build output schema: table columns + CDC metadata columns
        let mut fields: Vec<Field> = table_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new("snapshot_id", DataType::Int64, false));
        fields.push(Field::new("change_type", DataType::Utf8, false));
        let output_schema = Arc::new(Schema::new(fields));

        Self {
            provider,
            table_id,
            start_snapshot,
            end_snapshot,
            object_store_url,
            table_path,
            table_schema,
            output_schema,
        }
    }

    /// Analyze projection and split into table columns and CDC columns
    fn analyze_projection(&self, projection: Option<&Vec<usize>>) -> DeletionProjectionInfo {
        let num_table_cols = self.table_schema.fields().len();
        let snapshot_id_idx = num_table_cols;
        let change_type_idx = num_table_cols + 1;

        match projection {
            None => DeletionProjectionInfo {
                table_indices: (0..num_table_cols).collect(),
                need_snapshot_id: true,
                need_change_type: true,
                output_schema: self.output_schema.clone(),
                reorder_indices: None,
            },
            Some(indices) => {
                let mut table_indices: Vec<usize> = Vec::new();
                let mut need_snapshot_id = false;
                let mut need_change_type = false;

                for &idx in indices {
                    if idx < num_table_cols {
                        table_indices.push(idx);
                    } else if idx == snapshot_id_idx {
                        need_snapshot_id = true;
                    } else if idx == change_type_idx {
                        need_change_type = true;
                    }
                }

                // Build projected output schema in requested order
                let fields: Vec<Field> = indices
                    .iter()
                    .map(|&idx| self.output_schema.field(idx).clone())
                    .collect();
                let output_schema = Arc::new(Schema::new(fields));

                // Compute reorder mapping
                let table_idx_pos: std::collections::HashMap<usize, usize> = table_indices
                    .iter()
                    .enumerate()
                    .map(|(pos, &idx)| (idx, pos))
                    .collect();
                let snapshot_natural_pos = table_indices.len();
                let change_type_natural_pos = table_indices.len()
                    + if need_snapshot_id {
                        1
                    } else {
                        0
                    };

                let natural_pos_map: Vec<usize> = indices
                    .iter()
                    .map(|&idx| {
                        if idx < num_table_cols {
                            table_idx_pos[&idx]
                        } else if idx == snapshot_id_idx {
                            snapshot_natural_pos
                        } else {
                            change_type_natural_pos
                        }
                    })
                    .collect();

                let needs_reorder = natural_pos_map.iter().enumerate().any(|(i, &pos)| i != pos);

                DeletionProjectionInfo {
                    table_indices,
                    need_snapshot_id,
                    need_change_type,
                    output_schema,
                    reorder_indices: if needs_reorder {
                        Some(natural_pos_map)
                    } else {
                        None
                    },
                }
            },
        }
    }

    /// Build execution plan for a single delete file entry
    fn build_exec_for_delete_entry(
        &self,
        delete_file: &DeleteFileChange,
        proj_info: &DeletionProjectionInfo,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Resolve data file path
        let data_file_path = resolve_path(
            &self.table_path,
            &delete_file.data_file_path,
            delete_file.data_file_path_is_relative,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Create scan for current delete file (if exists - None means full file delete)
        let current_delete_exec = if let Some(ref current_path) = delete_file.current_delete_path {
            Some(self.build_delete_file_scan(
                current_path,
                delete_file.current_delete_path_is_relative.unwrap_or(true),
                delete_file.current_delete_file_size_bytes.unwrap_or(0),
                delete_file.current_delete_footer_size.unwrap_or(0),
            )?)
        } else {
            None
        };

        // Create scan for previous delete file (if exists)
        let previous_delete_exec = if let Some(ref prev_path) = delete_file.previous_delete_path {
            Some(self.build_delete_file_scan(
                prev_path,
                delete_file.previous_delete_path_is_relative.unwrap_or(true),
                delete_file.previous_delete_file_size_bytes.unwrap_or(0),
                delete_file.previous_delete_footer_size.unwrap_or(0),
            )?)
        } else {
            None
        };

        // Push table column projection to data file scan
        let data_file_exec = self.build_data_file_scan(
            &data_file_path,
            delete_file.data_file_size_bytes,
            delete_file.data_file_footer_size,
            if proj_info.table_indices.len() == self.table_schema.fields().len() {
                None
            } else {
                Some(&proj_info.table_indices)
            },
        )?;

        Ok(Arc::new(DeletedRowsExec::new(
            current_delete_exec,
            previous_delete_exec,
            data_file_exec,
            delete_file.data_record_count,
            delete_file.snapshot_id,
            proj_info.output_schema.clone(),
            proj_info.need_snapshot_id,
            proj_info.need_change_type,
            proj_info.reorder_indices.clone(),
        )))
    }

    /// Build a ParquetExec for a delete file
    fn build_delete_file_scan(
        &self,
        path: &str,
        is_relative: bool,
        size_bytes: i64,
        footer_size: i64,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let resolved_path = resolve_path(&self.table_path, path, is_relative)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut pf = PartitionedFile::new(
            &resolved_path,
            validated_file_size(size_bytes, &resolved_path)?,
        );
        if footer_size > 0
            && let Ok(hint) = usize::try_from(footer_size)
        {
            pf = pf.with_metadata_size_hint(hint);
        }

        let builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            delete_file_schema(),
            Arc::new(ParquetSource::default()),
        )
        .with_file_group(FileGroup::new(vec![pf]));

        Ok(DataSourceExec::from_data_source(builder.build()))
    }

    /// Build a ParquetExec for a data file with optional projection
    fn build_data_file_scan(
        &self,
        path: &str,
        size_bytes: i64,
        footer_size: Option<i64>,
        projection: Option<&[usize]>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut pf = PartitionedFile::new(path, validated_file_size(size_bytes, path)?);
        if let Some(fs) = footer_size
            && fs > 0
            && let Ok(hint) = usize::try_from(fs)
        {
            pf = pf.with_metadata_size_hint(hint);
        }

        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            self.table_schema.clone(),
            Arc::new(ParquetSource::default()),
        )
        .with_file_group(FileGroup::new(vec![pf]));

        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.to_vec()));
        }

        Ok(DataSourceExec::from_data_source(builder.build()))
    }
}

#[async_trait]
impl TableProvider for TableDeletionsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Analyze projection to determine what to read
        let proj_info = self.analyze_projection(projection);

        // Get delete files added between snapshots
        let delete_files = self
            .provider
            .get_delete_files_added_between_snapshots(
                self.table_id,
                self.start_snapshot,
                self.end_snapshot,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Handle empty case
        if delete_files.is_empty() {
            use datafusion::physical_plan::empty::EmptyExec;
            return Ok(Arc::new(EmptyExec::new(proj_info.output_schema)));
        }

        // Build execution plan for each delete entry with projection pushdown
        let mut execs: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(delete_files.len());
        for delete_file in &delete_files {
            let exec = self.build_exec_for_delete_entry(delete_file, &proj_info)?;
            execs.push(exec);
        }

        // Combine with UnionExec if multiple
        if execs.len() == 1 {
            Ok(execs.into_iter().next().expect("checked len == 1 above"))
        } else {
            UnionExec::try_new(execs)
        }
    }
}

/// Execution plan that reads deleted rows from a data file
///
/// 1. Reads current delete file to get deleted positions
/// 2. Reads previous delete file to get previously deleted positions (if exists)
/// 3. Computes delta: positions in current but not in previous
/// 4. Reads data file and filters to only include rows at deleted positions
/// 5. Appends CDC columns (snapshot_id, change_type='delete')
#[derive(Debug)]
pub struct DeletedRowsExec {
    /// Scan of current delete file (None for full file deletes)
    current_delete_scan: Option<Arc<dyn ExecutionPlan>>,
    /// Scan of previous delete file (if exists)
    previous_delete_scan: Option<Arc<dyn ExecutionPlan>>,
    /// Scan of data file
    data_file_scan: Arc<dyn ExecutionPlan>,
    /// Total record count in data file (used for full file deletes)
    record_count: i64,
    /// Snapshot ID for CDC column
    snapshot_id: i64,
    /// Output schema (projected table columns + requested CDC columns)
    output_schema: SchemaRef,
    /// Whether to include snapshot_id in output
    include_snapshot_id: bool,
    /// Whether to include change_type in output
    include_change_type: bool,
    /// Reorder mapping: output_pos -> natural_pos. None if no reordering needed.
    reorder_indices: Option<Vec<usize>>,
    /// Cached plan properties
    properties: PlanProperties,
}

impl DeletedRowsExec {
    pub fn new(
        current_delete_scan: Option<Arc<dyn ExecutionPlan>>,
        previous_delete_scan: Option<Arc<dyn ExecutionPlan>>,
        data_file_scan: Arc<dyn ExecutionPlan>,
        record_count: i64,
        snapshot_id: i64,
        output_schema: SchemaRef,
        include_snapshot_id: bool,
        include_change_type: bool,
        reorder_indices: Option<Vec<usize>>,
    ) -> Self {
        let eq_properties = EquivalenceProperties::new(output_schema.clone());
        let data_props = data_file_scan.properties();
        let properties = PlanProperties::new(
            eq_properties,
            data_props.output_partitioning().clone(),
            data_props.emission_type,
            data_props.boundedness,
        );

        Self {
            current_delete_scan,
            previous_delete_scan,
            data_file_scan,
            record_count,
            snapshot_id,
            output_schema,
            include_snapshot_id,
            include_change_type,
            reorder_indices,
            properties,
        }
    }
}

impl DisplayAs for DeletedRowsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DeletedRowsExec: snapshot_id={}, full_delete={}, has_previous={}",
                    self.snapshot_id,
                    self.current_delete_scan.is_none(),
                    self.previous_delete_scan.is_some()
                )
            },
        }
    }
}

impl ExecutionPlan for DeletedRowsExec {
    fn name(&self) -> &str {
        "DeletedRowsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        let mut children = Vec::with_capacity(3);
        if let Some(ref curr) = self.current_delete_scan {
            children.push(curr);
        }
        if let Some(ref prev) = self.previous_delete_scan {
            children.push(prev);
        }
        children.push(&self.data_file_scan);
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut idx = 0;

        let current = if self.current_delete_scan.is_some() {
            let c = children
                .get(idx)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("Missing current delete child".into()))?;
            idx += 1;
            Some(c)
        } else {
            None
        };

        let previous = if self.previous_delete_scan.is_some() {
            let p = children
                .get(idx)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("Missing previous delete child".into()))?;
            idx += 1;
            Some(p)
        } else {
            None
        };

        let data = children
            .get(idx)
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("Missing data file child".into()))?;

        Ok(Arc::new(DeletedRowsExec::new(
            current,
            previous,
            data,
            self.record_count,
            self.snapshot_id,
            self.output_schema.clone(),
            self.include_snapshot_id,
            self.include_change_type,
            self.reorder_indices.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let current_stream = self
            .current_delete_scan
            .as_ref()
            .map(|p| p.execute(partition, context.clone()))
            .transpose()?;
        let previous_stream = self
            .previous_delete_scan
            .as_ref()
            .map(|p| p.execute(partition, context.clone()))
            .transpose()?;
        let data_stream = self.data_file_scan.execute(partition, context)?;

        Ok(Box::pin(DeletedRowsStream::new(
            current_stream,
            previous_stream,
            data_stream,
            self.record_count,
            self.snapshot_id,
            self.output_schema.clone(),
            self.include_snapshot_id,
            self.include_change_type,
            self.reorder_indices.clone(),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

/// Stream state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamState {
    /// Reading current delete file
    ReadingCurrentDelete,
    /// Reading previous delete file
    ReadingPreviousDelete,
    /// Reading data file and filtering
    ReadingData,
    /// Done
    Done,
}

/// Represents current delete positions, avoiding large allocations for full-file deletes
enum CurrentDeletePositions {
    /// All rows in the file are deleted (full-file delete)
    All {
        record_count: i64,
    },
    /// Only specific positions are deleted
    Partial(HashSet<i64>),
}

/// Represents computed delta positions (newly deleted rows)
enum DeltaPositions {
    /// All rows in the file are newly deleted
    All,
    /// Specific sorted positions are newly deleted
    Specific(Vec<i64>),
}

/// Stream that reads deleted rows from a data file
///
/// Uses a state machine to read delete files, compute delta positions,
/// then filter data file rows to only include deleted rows.
struct DeletedRowsStream {
    // Note: manual Debug is not derived because SendableRecordBatchStream
    // does not implement Debug. Use the struct name for debug output.
    /// Current delete file stream (None for full file delete)
    current_delete_stream: Option<SendableRecordBatchStream>,
    /// Previous delete file stream (if exists)
    previous_delete_stream: Option<SendableRecordBatchStream>,
    /// Data file stream
    data_stream: SendableRecordBatchStream,
    /// Snapshot ID for CDC column
    snapshot_id: i64,
    /// Output schema
    output_schema: SchemaRef,
    /// Whether to include snapshot_id in output
    include_snapshot_id: bool,
    /// Whether to include change_type in output
    include_change_type: bool,
    /// Reorder mapping: output_pos -> natural_pos. None if no reordering needed.
    reorder_indices: Option<Vec<usize>>,
    /// Collected current positions (or All for full delete)
    current_positions: CurrentDeletePositions,
    /// Collected previous positions
    previous_positions: HashSet<i64>,
    /// Computed delta positions (sorted)
    deleted_positions: Option<DeltaPositions>,
    /// Current row offset in data file
    row_offset: i64,
    /// State machine
    state: StreamState,
}

impl fmt::Debug for DeletedRowsStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeletedRowsStream")
            .field("snapshot_id", &self.snapshot_id)
            .field("include_snapshot_id", &self.include_snapshot_id)
            .field("include_change_type", &self.include_change_type)
            .field("row_offset", &self.row_offset)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl DeletedRowsStream {
    fn new(
        current_delete_stream: Option<SendableRecordBatchStream>,
        previous_delete_stream: Option<SendableRecordBatchStream>,
        data_stream: SendableRecordBatchStream,
        record_count: i64,
        snapshot_id: i64,
        output_schema: SchemaRef,
        include_snapshot_id: bool,
        include_change_type: bool,
        reorder_indices: Option<Vec<usize>>,
    ) -> Self {
        // Determine initial state and compute positions if needed
        let (initial_state, current_positions, deleted_positions) =
            if current_delete_stream.is_some() {
                (
                    StreamState::ReadingCurrentDelete,
                    CurrentDeletePositions::Partial(HashSet::new()),
                    None,
                )
            } else if previous_delete_stream.is_some() {
                // Full file delete but has previous - need to subtract previous positions
                (
                    StreamState::ReadingPreviousDelete,
                    CurrentDeletePositions::All {
                        record_count,
                    },
                    None,
                )
            } else {
                // Full file delete with no previous - all positions are deleted
                (
                    StreamState::ReadingData,
                    CurrentDeletePositions::All {
                        record_count,
                    },
                    Some(DeltaPositions::All),
                )
            };

        Self {
            current_delete_stream,
            previous_delete_stream,
            data_stream,
            snapshot_id,
            output_schema,
            include_snapshot_id,
            include_change_type,
            reorder_indices,
            current_positions,
            previous_positions: HashSet::new(),
            deleted_positions,
            row_offset: 0,
            state: initial_state,
        }
    }

    /// Extract positions from a delete file batch
    fn extract_positions(batch: &RecordBatch) -> DataFusionResult<HashSet<i64>> {
        if batch.num_columns() < 2 {
            return Ok(HashSet::new());
        }

        let pos_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal("delete file pos column is not Int64".to_string())
            })?;

        // Use .iter() to respect null bitmap; skip null entries
        Ok(pos_array.iter().flatten().collect())
    }

    /// Compute the delta and sort it
    fn compute_deleted_positions(&mut self) {
        match &self.current_positions {
            CurrentDeletePositions::All {
                record_count,
            } => {
                if self.previous_positions.is_empty() {
                    self.deleted_positions = Some(DeltaPositions::All);
                } else {
                    let mut delta: Vec<i64> = (0..*record_count)
                        .filter(|pos| !self.previous_positions.contains(pos))
                        .collect();
                    delta.sort_unstable();
                    self.deleted_positions = Some(DeltaPositions::Specific(delta));
                }
            },
            CurrentDeletePositions::Partial(current) => {
                let mut delta: Vec<i64> = current
                    .iter()
                    .filter(|pos| !self.previous_positions.contains(pos))
                    .copied()
                    .collect();
                delta.sort_unstable();
                self.deleted_positions = Some(DeltaPositions::Specific(delta));
            },
        }
    }

    /// Filter batch to only include deleted rows and append CDC columns
    fn filter_batch(&mut self, batch: &RecordBatch) -> DataFusionResult<Option<RecordBatch>> {
        let num_rows = batch.num_rows();
        let num_rows_i64 = i64::try_from(num_rows).map_err(|_| {
            DataFusionError::Internal(format!("batch row count {} exceeds i64::MAX", num_rows))
        })?;

        let keep_indices: Vec<u32> = match self.deleted_positions.as_ref().unwrap() {
            DeltaPositions::All => {
                // All rows are deleted — keep all rows in this batch
                if num_rows > u32::MAX as usize {
                    return Err(DataFusionError::Internal(format!(
                        "batch row count {} exceeds u32::MAX",
                        num_rows
                    )));
                }
                (0..num_rows as u32).collect()
            },
            DeltaPositions::Specific(deleted_positions) => {
                if num_rows > u32::MAX as usize {
                    return Err(DataFusionError::Internal(format!(
                        "batch row count {} exceeds u32::MAX",
                        num_rows
                    )));
                }
                let mut indices = Vec::new();
                for i in 0..num_rows {
                    // Safe: i < num_rows and num_rows fits in u32 (checked above)
                    let global_pos = self.row_offset + i as i64;
                    if deleted_positions.binary_search(&global_pos).is_ok() {
                        indices.push(i as u32);
                    }
                }
                indices
            },
        };

        // Update row offset for next batch
        self.row_offset += num_rows_i64;

        // If no deleted rows in this batch, return None
        if keep_indices.is_empty() {
            return Ok(None);
        }

        // Use Arrow's take kernel to select rows
        let indices = UInt32Array::from(keep_indices.clone());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 2);

        for col in batch.columns() {
            let filtered = take(col.as_ref(), &indices, None)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            columns.push(filtered);
        }

        // Append only requested CDC columns
        let num_output_rows = keep_indices.len();
        if self.include_snapshot_id {
            columns.push(Arc::new(Int64Array::from(vec![
                self.snapshot_id;
                num_output_rows
            ])));
        }
        if self.include_change_type {
            columns.push(Arc::new(StringArray::from(vec!["delete"; num_output_rows])));
        }

        // Apply reorder if projection requested non-natural column order
        let columns = if let Some(ref reorder) = self.reorder_indices {
            reorder.iter().map(|&i| columns[i].clone()).collect()
        } else {
            columns
        };

        RecordBatch::try_new(self.output_schema.clone(), columns)
            .map(Some)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Stream for DeletedRowsStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                StreamState::ReadingCurrentDelete => {
                    let current = self.current_delete_stream.as_mut().unwrap();
                    match Pin::new(current).poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let positions = match Self::extract_positions(&batch) {
                                Ok(p) => p,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };
                            if let CurrentDeletePositions::Partial(ref mut set) =
                                self.current_positions
                            {
                                set.extend(positions);
                            }
                        },
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => {
                            if self.previous_delete_stream.is_some() {
                                self.state = StreamState::ReadingPreviousDelete;
                            } else {
                                self.compute_deleted_positions();
                                self.state = StreamState::ReadingData;
                            }
                        },
                        Poll::Pending => return Poll::Pending,
                    }
                },
                StreamState::ReadingPreviousDelete => {
                    let prev = self.previous_delete_stream.as_mut().unwrap();
                    match Pin::new(prev).poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let positions = match Self::extract_positions(&batch) {
                                Ok(p) => p,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };
                            self.previous_positions.extend(positions);
                        },
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => {
                            self.compute_deleted_positions();
                            self.state = StreamState::ReadingData;
                        },
                        Poll::Pending => return Poll::Pending,
                    }
                },
                StreamState::ReadingData => {
                    match Pin::new(&mut self.data_stream).poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            match self.filter_batch(&batch)? {
                                Some(filtered) => return Poll::Ready(Some(Ok(filtered))),
                                None => continue, // No deleted rows in this batch
                            }
                        },
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => {
                            self.state = StreamState::Done;
                            return Poll::Ready(None);
                        },
                        Poll::Pending => return Poll::Pending,
                    }
                },
                StreamState::Done => {
                    return Poll::Ready(None);
                },
            }
        }
    }
}

impl RecordBatchStream for DeletedRowsStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}
