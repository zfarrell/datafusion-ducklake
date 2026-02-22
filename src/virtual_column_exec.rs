//! Custom execution plan for appending virtual columns
//!
//! This module implements a DataFusion execution plan that wraps a scan
//! and appends virtual columns (`filename` and `file_row_number`) to the output.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::Stream;

/// Virtual column name for the file path
pub const VIRTUAL_COL_FILENAME: &str = "filename";
/// Virtual column name for the row number within a file
pub const VIRTUAL_COL_FILE_ROW_NUMBER: &str = "file_row_number";

/// Custom execution plan that appends virtual columns to the output
#[derive(Debug)]
pub struct VirtualColumnExec {
    /// The input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The filename to populate the `filename` virtual column with
    filename: String,
    /// Whether to include the `filename` virtual column
    include_filename: bool,
    /// Whether to include the `file_row_number` virtual column
    include_row_number: bool,
    /// The output schema (input schema + virtual columns)
    output_schema: SchemaRef,
    /// Cached plan properties
    properties: PlanProperties,
}

impl VirtualColumnExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        filename: String,
        include_filename: bool,
        include_row_number: bool,
        output_schema: SchemaRef,
    ) -> Self {
        let eq_props = EquivalenceProperties::new(Arc::clone(&output_schema));
        let properties = PlanProperties::new(
            eq_props,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            Boundedness::Bounded,
        );

        Self {
            input,
            filename,
            include_filename,
            include_row_number,
            output_schema,
            properties,
        }
    }
}

impl DisplayAs for VirtualColumnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "VirtualColumnExec: file={}, filename={}, row_number={}",
            self.filename, self.include_filename, self.include_row_number
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

    fn properties(&self) -> &PlanProperties {
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
            self.filename.clone(),
            self.include_filename,
            self.include_row_number,
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
            filename: self.filename.clone(),
            include_filename: self.include_filename,
            include_row_number: self.include_row_number,
            row_offset: 0,
            output_schema: Arc::clone(&self.output_schema),
        }))
    }
}

/// Stream that appends virtual columns to each output batch
struct VirtualColumnStream {
    input: SendableRecordBatchStream,
    filename: String,
    include_filename: bool,
    include_row_number: bool,
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

                // Append filename column if requested
                if self.include_filename {
                    let filename_array = StringArray::from(vec![self.filename.as_str(); num_rows]);
                    columns.push(Arc::new(filename_array));
                }

                // Append file_row_number column if requested
                if self.include_row_number {
                    let row_numbers: Vec<i64> =
                        (row_offset..row_offset + num_rows as i64).collect();
                    let row_number_array = Int64Array::from(row_numbers);
                    columns.push(Arc::new(row_number_array));
                }

                // Update row offset
                self.row_offset += num_rows as i64;

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
        ]));

        let stream = VirtualColumnStream {
            input: Box::pin(EmptyRecordBatchStream::new(input_schema)),
            filename: "test.parquet".to_string(),
            include_filename: true,
            include_row_number: true,
            row_offset: 0,
            output_schema: Arc::clone(&output_schema),
        };

        assert_eq!(stream.schema().fields().len(), 3);
        assert_eq!(stream.schema().field(1).name(), VIRTUAL_COL_FILENAME);
        assert_eq!(stream.schema().field(2).name(), VIRTUAL_COL_FILE_ROW_NUMBER);
    }
}
