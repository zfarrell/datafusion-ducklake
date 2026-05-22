//! Custom execution plan for appending virtual columns
//!
//! TODO(#22): This module is currently a compilable orphan. Upstream landed
//! its own `row_id` design (`src/row_id.rs`) which is what the active scan
//! path uses. The virtual-column reconciliation (`rowid` collision, scan
//! wiring, public surface) is tracked in ticket #22. Until then, the types
//! here remain reachable from `lib.rs` re-exports so downstream code can
//! continue to reference them, but the wiring into `DuckLakeTable::scan`
//! has been intentionally left out.
//!
//! This module implements a DataFusion execution plan that wraps a scan
//! and appends virtual columns (`filename`, `file_row_number`, `rowid`,
//! `snapshot_id`, `file_index`) to the output.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::Stream;

/// Virtual column name for the file path
pub const VIRTUAL_COL_FILENAME: &str = "filename";
/// Virtual column name for the row number within a file
pub const VIRTUAL_COL_FILE_ROW_NUMBER: &str = "file_row_number";
/// Virtual column name for the global row ID
pub const VIRTUAL_COL_ROWID: &str = "rowid";
/// Virtual column name for the snapshot ID when the file was committed
pub const VIRTUAL_COL_SNAPSHOT_ID: &str = "snapshot_id";
/// Virtual column name for the 0-based file index within the table
pub const VIRTUAL_COL_FILE_INDEX: &str = "file_index";

/// Per-file metadata needed to populate virtual columns
#[derive(Debug, Clone)]
pub struct VirtualColumnFileInfo {
    /// The filename/path for the `filename` virtual column
    pub filename: String,
    /// Starting row ID for this file (from `ducklake_data_file.row_id_start`)
    pub row_id_start: Option<i64>,
    /// Snapshot ID when this file was committed (from `ducklake_data_file.begin_snapshot`)
    pub snapshot_id: Option<i64>,
    /// 0-based ordinal position of this file in the table's file list
    pub file_index: u64,
}

/// Tracks which virtual columns are requested in the query
#[derive(Debug, Clone, Default)]
pub struct VirtualColumnSet {
    pub filename: bool,
    pub file_row_number: bool,
    pub rowid: bool,
    pub snapshot_id: bool,
    pub file_index: bool,
}

impl VirtualColumnSet {
    pub fn any(&self) -> bool {
        self.filename || self.file_row_number || self.rowid || self.snapshot_id || self.file_index
    }
}

/// Custom execution plan that appends virtual columns to the output
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
        // When row-number-dependent virtual columns are requested and the input
        // has multiple partitions, coalesce into a single partition to avoid
        // duplicate row numbers across partitions (F-033).
        let needs_single_partition = included.file_row_number || included.rowid;
        let input = if needs_single_partition && input.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>
        } else {
            input
        };

        let eq_props = EquivalenceProperties::new(Arc::clone(&output_schema));
        let partitioning = if needs_single_partition {
            Partitioning::UnknownPartitioning(1)
        } else {
            input.output_partitioning().clone()
        };
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            partitioning,
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
            "VirtualColumnExec: file={}, filename={}, row_number={}, rowid={}, snapshot_id={}, file_index={}",
            self.file_info.filename,
            self.included.filename,
            self.included.file_row_number,
            self.included.rowid,
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

                // Start with input columns
                let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();

                // Append virtual columns in schema order: filename, file_row_number, rowid, snapshot_id, file_index
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

                if self.included.rowid {
                    match self.file_info.row_id_start {
                        Some(row_id_start) => {
                            let num_rows_i64 = i64::try_from(num_rows).map_err(|e| {
                                DataFusionError::Execution(format!("Row count overflow: {}", e))
                            })?;
                            let end = row_offset.checked_add(num_rows_i64).ok_or_else(|| {
                                DataFusionError::Execution(
                                    "Row offset overflow computing rowid".to_string(),
                                )
                            })?;
                            // R5-S-020: Return error on rowid overflow instead of
                            // silently clipping to i64::MAX (which causes duplicate rowids).
                            let rowids: Vec<i64> = (row_offset..end)
                                .map(|offset| {
                                    row_id_start.checked_add(offset).ok_or_else(|| {
                                        DataFusionError::Execution(format!(
                                            "Rowid overflow: row_id_start={} + offset={} exceeds i64::MAX",
                                            row_id_start, offset
                                        ))
                                    })
                                })
                                .collect::<std::result::Result<Vec<_>, _>>()?;
                            columns.push(Arc::new(Int64Array::from(rowids)));
                        },
                        None => {
                            columns.push(Arc::new(Int64Array::from(vec![None::<i64>; num_rows])));
                        },
                    }
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
            Field::new(VIRTUAL_COL_ROWID, DataType::Int64, true),
            Field::new(VIRTUAL_COL_SNAPSHOT_ID, DataType::Int64, true),
            Field::new(VIRTUAL_COL_FILE_INDEX, DataType::UInt64, true),
        ]));

        let stream = VirtualColumnStream {
            input: Box::pin(EmptyRecordBatchStream::new(input_schema)),
            file_info: VirtualColumnFileInfo {
                filename: "test.parquet".to_string(),
                row_id_start: Some(0),
                snapshot_id: Some(1),
                file_index: 0,
            },
            included: VirtualColumnSet {
                filename: true,
                file_row_number: true,
                rowid: true,
                snapshot_id: true,
                file_index: true,
            },
            row_offset: 0,
            output_schema: Arc::clone(&output_schema),
        };

        assert_eq!(stream.schema().fields().len(), 6);
        assert_eq!(stream.schema().field(1).name(), VIRTUAL_COL_FILENAME);
        assert_eq!(stream.schema().field(2).name(), VIRTUAL_COL_FILE_ROW_NUMBER);
        assert_eq!(stream.schema().field(3).name(), VIRTUAL_COL_ROWID);
        assert_eq!(stream.schema().field(4).name(), VIRTUAL_COL_SNAPSHOT_ID);
        assert_eq!(stream.schema().field(5).name(), VIRTUAL_COL_FILE_INDEX);
    }
}
