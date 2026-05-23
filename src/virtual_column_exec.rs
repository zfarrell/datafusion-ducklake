//! Custom execution plan for appending scan-time virtual columns.
//!
//! This exec emits four DuckLake virtual columns whose values are entirely
//! scan-time metadata — they describe *where* a row came from rather than
//! anything stored in the row itself:
//!
//! * [`VIRTUAL_COL_FILENAME`] — the data file's resolved path
//! * [`VIRTUAL_COL_FILE_ROW_NUMBER`] — 0-based position within the scanned file
//! * [`VIRTUAL_COL_SNAPSHOT_ID`] — snapshot in which the file was committed
//! * [`VIRTUAL_COL_FILE_INDEX`] — 0-based ordinal of the file in the scan
//!
//! `rowid` is intentionally *not* handled here. The DuckLake spec requires
//! rowid to survive `UPDATE` / compaction rewrites, which is fundamentally
//! incompatible with synthesizing values from `row_id_start + position`:
//! UPDATE-rewritten files embed the original rowids in a column tagged with
//! parquet field-id [`crate::row_id::ROW_ID_PARQUET_FIELD_ID`]. Reading those
//! values back out requires inspecting the parquet schema and dispatching
//! per-file, which is the job of [`crate::row_id::RowIdExec`] (and the
//! field-id-detection path in `table.rs`). See ticket #22 for the design
//! reconciliation that landed this split.
//!
//! Ordering of virtual columns in the output schema is:
//! `[ real_cols..., filename?, file_row_number?, rowid?, snapshot_id?, file_index? ]`
//! where `rowid` is contributed by the upstream RowIdExec when row lineage is
//! enabled. `VirtualColumnExec` simply leaves the rowid column untouched and
//! passes it through.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties};
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::Stream;

/// Virtual column name for the file path
pub const VIRTUAL_COL_FILENAME: &str = "filename";
/// Virtual column name for the row number within a file
pub const VIRTUAL_COL_FILE_ROW_NUMBER: &str = "file_row_number";
/// Virtual column name for the global row ID (owned by [`crate::row_id`], not this exec).
pub const VIRTUAL_COL_ROWID: &str = "rowid";
/// Virtual column name for the snapshot ID when the file was committed
pub const VIRTUAL_COL_SNAPSHOT_ID: &str = "snapshot_id";
/// Virtual column name for the 0-based file index within the table
pub const VIRTUAL_COL_FILE_INDEX: &str = "file_index";

/// Per-file metadata needed to populate the scan-time virtual columns.
///
/// `rowid` metadata (`row_id_start`) is *not* included here — rowid is
/// handled by [`crate::row_id::RowIdExec`] / per-file embedded-column
/// detection, not by this exec. See module docs.
#[derive(Debug, Clone)]
pub struct VirtualColumnFileInfo {
    /// The filename/path for the `filename` virtual column
    pub filename: String,
    /// Snapshot ID when this file was committed (from `ducklake_data_file.begin_snapshot`)
    pub snapshot_id: Option<i64>,
    /// 0-based ordinal position of this file in the table's file list
    pub file_index: u64,
}

/// Tracks which scan-time virtual columns are requested in the query.
///
/// `rowid` is intentionally absent — it is provisioned separately via the
/// row_id machinery (see module docs).
#[derive(Debug, Clone, Default)]
pub struct VirtualColumnSet {
    pub filename: bool,
    pub file_row_number: bool,
    pub snapshot_id: bool,
    pub file_index: bool,
}

impl VirtualColumnSet {
    pub fn any(&self) -> bool {
        self.filename || self.file_row_number || self.snapshot_id || self.file_index
    }
}

/// Custom execution plan that appends scan-time virtual columns to the output.
///
/// The exec consumes a single-partition input. Row-number synthesis requires
/// strict file ordering; emitting `file_row_number` from a repartitioned
/// stream would yield interleaved cursor values that don't correspond to
/// real file offsets. We enforce that invariant via
/// [`required_input_distribution`] returning `SinglePartition`, mirroring the
/// guard rail in [`crate::row_id::RowIdExec`] so DataFusion's
/// `EnforceDistribution` rule can't legally insert a `RepartitionExec`
/// underneath us and break the cursor.
#[derive(Debug)]
pub struct VirtualColumnExec {
    /// The input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Per-file metadata for virtual column values
    file_info: VirtualColumnFileInfo,
    /// Which virtual columns to include
    included: VirtualColumnSet,
    /// The output schema (input schema + virtual columns)
    output_schema: SchemaRef,
    /// Cached plan properties
    properties: Arc<PlanProperties>,
}

impl VirtualColumnExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        file_info: VirtualColumnFileInfo,
        included: VirtualColumnSet,
        output_schema: SchemaRef,
    ) -> Self {
        // Preserve the child's orderings on the wider output schema. We only
        // append new columns to the right, so column-referenced sort exprs
        // from the child stay valid. Without this propagation an upstream
        // `SortPreservingMergeExec` sanity-check rejects the plan because
        // `VirtualColumnExec` would advertise an empty ordering even though
        // a `SortExec` ran underneath.
        let child_orderings: Vec<_> = input
            .equivalence_properties()
            .oeq_class()
            .iter()
            .cloned()
            .collect();
        let eq_props = EquivalenceProperties::new_with_orderings(
            Arc::clone(&output_schema),
            child_orderings,
        );
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            Boundedness::Bounded,
        ));

        Self {
            input,
            file_info,
            included,
            output_schema,
            properties,
        }
    }
}

impl DisplayAs for VirtualColumnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "VirtualColumnExec: file={}, filename={}, row_number={}, snapshot_id={}, file_index={}",
            self.file_info.filename,
            self.included.filename,
            self.included.file_row_number,
            self.included.snapshot_id,
            self.included.file_index,
        )
    }
}

impl ExecutionPlan for VirtualColumnExec {
    fn name(&self) -> &str {
        "VirtualColumnExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// `file_row_number` is a position-in-file cursor; if DataFusion's
    /// `EnforceDistribution` rule inserted a `RepartitionExec` below us, the
    /// cursor would interleave across partitions and the emitted row numbers
    /// would no longer correspond to real file offsets. Pin the child to a
    /// single partition to prevent that.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    /// Order-preserving wrapper — we only append columns.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "VirtualColumnExec expects exactly one child".into(),
            ));
        }

        Ok(Arc::new(VirtualColumnExec::new(
            Arc::clone(&children[0]),
            self.file_info.clone(),
            self.included.clone(),
            Arc::clone(&self.output_schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(VirtualColumnStream {
            input: input_stream,
            file_info: self.file_info.clone(),
            included: self.included.clone(),
            row_offset: 0,
            output_schema: Arc::clone(&self.output_schema),
        }))
    }
}

/// Stream that appends virtual columns to each output batch
struct VirtualColumnStream {
    input: SendableRecordBatchStream,
    file_info: VirtualColumnFileInfo,
    included: VirtualColumnSet,
    row_offset: i64,
    output_schema: SchemaRef,
}

impl Stream for VirtualColumnStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let num_rows = batch.num_rows();
                let row_offset = self.row_offset;

                // Start with input columns (which may include rowid already if
                // RowIdExec ran upstream of us — we pass it through unchanged).
                let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();

                // Append virtual columns in schema order:
                //   filename, file_row_number, snapshot_id, file_index
                if self.included.filename {
                    let filename_array =
                        StringArray::from(vec![self.file_info.filename.as_str(); num_rows]);
                    columns.push(Arc::new(filename_array));
                }

                if self.included.file_row_number {
                    let num_rows_i64 = i64::try_from(num_rows).map_err(|e| {
                        DataFusionError::Execution(format!("Row count overflow: {}", e))
                    })?;
                    let end = row_offset.checked_add(num_rows_i64).ok_or_else(|| {
                        DataFusionError::Execution(
                            "Row offset overflow computing file_row_number".to_string(),
                        )
                    })?;
                    let row_numbers: Vec<i64> = (row_offset..end).collect();
                    let row_number_array = Int64Array::from(row_numbers);
                    columns.push(Arc::new(row_number_array));
                }

                if self.included.snapshot_id {
                    match self.file_info.snapshot_id {
                        Some(snap_id) => {
                            let snapshot_ids = Int64Array::from(vec![snap_id; num_rows]);
                            columns.push(Arc::new(snapshot_ids));
                        },
                        None => {
                            columns.push(Arc::new(Int64Array::from(vec![None::<i64>; num_rows])));
                        },
                    }
                }

                if self.included.file_index {
                    let file_idx = self.file_info.file_index;
                    let file_indices = UInt64Array::from(vec![file_idx; num_rows]);
                    columns.push(Arc::new(file_indices));
                }

                // Update row offset
                let num_rows_incr = i64::try_from(num_rows).map_err(|e| {
                    DataFusionError::Execution(format!("Row count overflow: {}", e))
                })?;
                self.row_offset = self.row_offset.checked_add(num_rows_incr).ok_or_else(|| {
                    DataFusionError::Execution("Row offset overflow updating position".to_string())
                })?;

                let result = RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));

                Poll::Ready(Some(result))
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for VirtualColumnStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::EmptyRecordBatchStream;

    #[test]
    fn test_virtual_column_stream_schema() {
        let input_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(VIRTUAL_COL_FILENAME, DataType::Utf8, true),
            Field::new(VIRTUAL_COL_FILE_ROW_NUMBER, DataType::Int64, true),
            Field::new(VIRTUAL_COL_SNAPSHOT_ID, DataType::Int64, true),
            Field::new(VIRTUAL_COL_FILE_INDEX, DataType::UInt64, true),
        ]));

        let stream = VirtualColumnStream {
            input: Box::pin(EmptyRecordBatchStream::new(input_schema)),
            file_info: VirtualColumnFileInfo {
                filename: "test.parquet".to_string(),
                snapshot_id: Some(1),
                file_index: 0,
            },
            included: VirtualColumnSet {
                filename: true,
                file_row_number: true,
                snapshot_id: true,
                file_index: true,
            },
            row_offset: 0,
            output_schema: Arc::clone(&output_schema),
        };

        assert_eq!(stream.schema().fields().len(), 5);
        assert_eq!(stream.schema().field(1).name(), VIRTUAL_COL_FILENAME);
        assert_eq!(stream.schema().field(2).name(), VIRTUAL_COL_FILE_ROW_NUMBER);
        assert_eq!(stream.schema().field(3).name(), VIRTUAL_COL_SNAPSHOT_ID);
        assert_eq!(stream.schema().field(4).name(), VIRTUAL_COL_FILE_INDEX);
    }
}
