//! DuckLake INSERT execution plan.
//!
//! Delegates to [`DuckLakeTableWriter`] for file writing and metadata commit.
//! See `table_writer.rs` module docs for write atomicity guarantees.
//!
//! Supports partitioned writes: when partition columns are configured, rows
//! are routed to per-partition Parquet files in Hive-style directory layout.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::{self, TryStreamExt};

use crate::metadata_writer::{MetadataWriter, WriteMode};
use crate::table_writer::DuckLakeTableWriter;

// chrono re-export from sqlx for partition transform date handling
#[cfg(any(
    feature = "metadata-sqlite",
    feature = "metadata-postgres",
    feature = "metadata-mysql"
))]
use sqlx::types::chrono;

/// Schema for the output of insert operations (count of rows inserted)
fn make_insert_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// Partition column info for write-side partitioning.
#[derive(Debug, Clone)]
pub struct WritePartitionColumn {
    /// Name of the source column in the table schema
    pub column_name: String,
    /// Index of this column in the table schema
    pub column_index: usize,
    /// Transform to apply (identity, year, month, day, hour)
    pub transform: Option<String>,
}

/// Execution plan that writes input data to a DuckLake table.
pub struct DuckLakeInsertExec {
    input: Arc<dyn ExecutionPlan>,
    writer: Arc<dyn MetadataWriter>,
    schema_name: String,
    table_name: String,
    arrow_schema: SchemaRef,
    write_mode: WriteMode,
    object_store_url: Arc<ObjectStoreUrl>,
    /// Partition columns for write-side partitioning (empty = no partitioning)
    partition_columns: Vec<WritePartitionColumn>,
    cache: PlanProperties,
}

impl DuckLakeInsertExec {
    /// Create a new DuckLakeInsertExec
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        writer: Arc<dyn MetadataWriter>,
        schema_name: String,
        table_name: String,
        arrow_schema: SchemaRef,
        write_mode: WriteMode,
        object_store_url: Arc<ObjectStoreUrl>,
    ) -> Self {
        let cache = Self::compute_properties();
        Self {
            input,
            writer,
            schema_name,
            table_name,
            arrow_schema,
            write_mode,
            object_store_url,
            partition_columns: Vec::new(),
            cache,
        }
    }

    /// Set partition columns for write-side partitioning.
    pub fn with_partition_columns(mut self, columns: Vec<WritePartitionColumn>) -> Self {
        self.partition_columns = columns;
        self
    }

    fn compute_properties() -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(make_insert_count_schema()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl Debug for DuckLakeInsertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckLakeInsertExec")
            .field("schema_name", &self.schema_name)
            .field("table_name", &self.table_name)
            .field("write_mode", &self.write_mode)
            .field("partitioned", &!self.partition_columns.is_empty())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DuckLakeInsertExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DuckLakeInsertExec: schema={}, table={}, mode={:?}",
                    self.schema_name, self.table_name, self.write_mode
                )?;
                if !self.partition_columns.is_empty() {
                    let names: Vec<&str> = self
                        .partition_columns
                        .iter()
                        .map(|c| c.column_name.as_str())
                        .collect();
                    write!(f, ", partitioned_by=[{}]", names.join(", "))?;
                }
                Ok(())
            },
        }
    }
}

impl ExecutionPlan for DuckLakeInsertExec {
    fn name(&self) -> &str {
        "DuckLakeInsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "DuckLakeInsertExec requires exactly one child".to_string(),
            ));
        }
        let mut exec = Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.writer),
            self.schema_name.clone(),
            self.table_name.clone(),
            Arc::clone(&self.arrow_schema),
            self.write_mode,
            self.object_store_url.clone(),
        );
        exec.partition_columns = self.partition_columns.clone();
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DuckLakeInsertExec only supports partition 0, got {}",
                partition
            )));
        }

        let input = Arc::clone(&self.input);
        let writer = Arc::clone(&self.writer);
        let schema_name = self.schema_name.clone();
        let table_name = self.table_name.clone();
        let arrow_schema = Arc::clone(&self.arrow_schema);
        let write_mode = self.write_mode;
        let object_store_url = self.object_store_url.clone();
        let partition_columns = self.partition_columns.clone();
        let output_schema = make_insert_count_schema();

        let stream = stream::once(async move {
            // Collect batches from ALL input partitions to avoid dropping data
            let num_partitions = input.output_partitioning().partition_count();
            let mut batches: Vec<RecordBatch> = Vec::new();
            for p in 0..num_partitions {
                let partition_stream = input.execute(p, Arc::clone(&context))?;
                let partition_batches: Vec<RecordBatch> = partition_stream.try_collect().await?;
                batches.extend(partition_batches);
            }

            if batches.is_empty() {
                let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
                return Ok(RecordBatch::try_new(output_schema, vec![count_array])?);
            }

            // Enforce NOT NULL constraints before writing
            for batch in &batches {
                for (i, field) in arrow_schema.fields().iter().enumerate() {
                    if !field.is_nullable() {
                        let column = batch.column(i);
                        if column.null_count() > 0 {
                            return Err(DataFusionError::Execution(format!(
                                "NOT NULL constraint failed: {}",
                                field.name()
                            )));
                        }
                    }
                }
            }

            // Get object store from runtime environment
            let object_store = context
                .runtime_env()
                .object_store(object_store_url.as_ref())?;

            let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer), object_store)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let schema_without_metadata =
                Schema::new(arrow_schema.fields().iter().cloned().collect::<Vec<_>>());

            let row_count = if partition_columns.is_empty() {
                // Non-partitioned write: use existing write_or_inline path
                let result = table_writer
                    .write_or_inline(
                        &schema_name,
                        &table_name,
                        &schema_without_metadata,
                        &batches,
                        write_mode,
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                result.records_written as u64
            } else {
                // Partitioned write: route rows to per-partition files
                write_partitioned(
                    &table_writer,
                    &writer,
                    &schema_name,
                    &table_name,
                    &schema_without_metadata,
                    &batches,
                    &partition_columns,
                    write_mode,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![row_count]));
            Ok(RecordBatch::try_new(output_schema, vec![count_array])?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            make_insert_count_schema(),
            stream,
        )))
    }
}

/// Compute partition value for a single element from a column array.
///
/// Applies the configured transform to produce the Hive directory value.
fn compute_partition_value(
    array: &dyn arrow::array::Array,
    row: usize,
    transform: Option<&str>,
) -> Option<String> {
    use arrow::array::*;

    if array.is_null(row) {
        return None;
    }

    let transform = transform
        .map(|t| t.to_lowercase())
        .unwrap_or_else(|| "identity".to_string());

    match transform.as_str() {
        "identity" | "" => {
            // Extract the raw value as a string
            match array.data_type() {
                DataType::Int8 => Some(
                    array
                        .as_any()
                        .downcast_ref::<Int8Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::Int16 => Some(
                    array
                        .as_any()
                        .downcast_ref::<Int16Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::Int32 => Some(
                    array
                        .as_any()
                        .downcast_ref::<Int32Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::Int64 => Some(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::UInt8 => Some(
                    array
                        .as_any()
                        .downcast_ref::<UInt8Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::UInt16 => Some(
                    array
                        .as_any()
                        .downcast_ref::<UInt16Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::UInt32 => Some(
                    array
                        .as_any()
                        .downcast_ref::<UInt32Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::UInt64 => Some(
                    array
                        .as_any()
                        .downcast_ref::<UInt64Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::Float32 => Some(
                    array
                        .as_any()
                        .downcast_ref::<Float32Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::Float64 => Some(
                    array
                        .as_any()
                        .downcast_ref::<Float64Array>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::Utf8 => Some(
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::LargeUtf8 => Some(
                    array
                        .as_any()
                        .downcast_ref::<LargeStringArray>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::Boolean => Some(
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()?
                        .value(row)
                        .to_string(),
                ),
                DataType::Date32 => {
                    let days = array.as_any().downcast_ref::<Date32Array>()?.value(row);
                    let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)?;
                    Some(date.format("%Y-%m-%d").to_string())
                },
                DataType::Date64 => {
                    let ms = array.as_any().downcast_ref::<Date64Array>()?.value(row);
                    let date = chrono::DateTime::from_timestamp_millis(ms)?;
                    Some(date.format("%Y-%m-%d").to_string())
                },
                _ => Some(format!(
                    "{}",
                    arrow::util::display::ArrayFormatter::try_new(array, &Default::default())
                        .ok()?
                        .value(row)
                )),
            }
        },
        "year" => extract_temporal_component(array, row, TemporalComponent::Year),
        "month" => extract_temporal_component(array, row, TemporalComponent::Month),
        "day" => extract_temporal_component(array, row, TemporalComponent::Day),
        "hour" => extract_temporal_component(array, row, TemporalComponent::Hour),
        _ => None,
    }
}

enum TemporalComponent {
    Year,
    Month,
    Day,
    Hour,
}

fn extract_temporal_component(
    array: &dyn arrow::array::Array,
    row: usize,
    component: TemporalComponent,
) -> Option<String> {
    use arrow::array::*;

    match array.data_type() {
        DataType::Date32 => {
            let days = array.as_any().downcast_ref::<Date32Array>()?.value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)?;
            match component {
                TemporalComponent::Year => Some(date.format("%Y").to_string()),
                TemporalComponent::Month => Some(date.format("%-m").to_string()),
                TemporalComponent::Day => Some(date.format("%-d").to_string()),
                TemporalComponent::Hour => Some("0".to_string()),
            }
        },
        DataType::Date64 => {
            let ms = array.as_any().downcast_ref::<Date64Array>()?.value(row);
            let dt = chrono::DateTime::from_timestamp_millis(ms)?;
            match component {
                TemporalComponent::Year => Some(dt.format("%Y").to_string()),
                TemporalComponent::Month => Some(dt.format("%-m").to_string()),
                TemporalComponent::Day => Some(dt.format("%-d").to_string()),
                TemporalComponent::Hour => Some(dt.format("%-H").to_string()),
            }
        },
        DataType::Timestamp(_, _) => {
            // Handle Timestamp types via cast to Date32 for year/month/day
            let ts_array = array.as_any().downcast_ref::<TimestampMicrosecondArray>();
            if let Some(ts) = ts_array {
                let us = ts.value(row);
                let dt = chrono::DateTime::from_timestamp_micros(us)?;
                match component {
                    TemporalComponent::Year => Some(dt.format("%Y").to_string()),
                    TemporalComponent::Month => Some(dt.format("%-m").to_string()),
                    TemporalComponent::Day => Some(dt.format("%-d").to_string()),
                    TemporalComponent::Hour => Some(dt.format("%-H").to_string()),
                }
            } else {
                None
            }
        },
        _ => None,
    }
}

/// Build a Hive-style partition directory path from partition values.
///
/// Example: for partition columns [(category, "A"), (year, "2024")]:
/// Returns "category=A/year=2024"
fn build_hive_dir(partition_columns: &[WritePartitionColumn], values: &[Option<String>]) -> String {
    partition_columns
        .iter()
        .zip(values.iter())
        .map(|(col, val)| {
            let name = &col.column_name;
            match val {
                Some(v) => format!("{}={}", name, v),
                None => format!("{}=__HIVE_DEFAULT_PARTITION__", name),
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Route rows into partition buckets, keyed by their partition values.
///
/// Returns a map from partition_key (Hive dir string) to (partition_values, row_indices).
fn route_batches_to_partitions(
    batches: &[RecordBatch],
    partition_columns: &[WritePartitionColumn],
) -> HashMap<String, (Vec<Option<String>>, Vec<(usize, usize)>)> {
    let mut partitions: HashMap<String, (Vec<Option<String>>, Vec<(usize, usize)>)> =
        HashMap::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(partition_columns.len());
            for pc in partition_columns {
                let array = batch.column(pc.column_index);
                let val = compute_partition_value(array.as_ref(), row_idx, pc.transform.as_deref());
                values.push(val);
            }
            let key = build_hive_dir(partition_columns, &values);
            partitions
                .entry(key)
                .or_insert_with(|| (values, Vec::new()))
                .1
                .push((batch_idx, row_idx));
        }
    }

    partitions
}

/// Extract a sub-batch containing only the specified row indices.
fn extract_rows(
    batches: &[RecordBatch],
    indices: &[(usize, usize)],
) -> DataFusionResult<RecordBatch> {
    if indices.is_empty() {
        return Err(DataFusionError::Internal(
            "Cannot extract empty row set".to_string(),
        ));
    }

    let schema = batches[indices[0].0].schema();
    let num_cols = schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        // Build indices for arrow::compute::take
        let mut take_indices = Vec::with_capacity(indices.len());
        let mut arrays_to_concat: Vec<(&RecordBatch, Vec<u32>)> = Vec::new();

        // Group consecutive indices by batch for efficiency
        let mut current_batch_idx: Option<usize> = None;
        for &(batch_idx, row_idx) in indices {
            if current_batch_idx != Some(batch_idx) {
                if let Some(prev) = current_batch_idx {
                    if !take_indices.is_empty() {
                        arrays_to_concat.push((&batches[prev], std::mem::take(&mut take_indices)));
                    }
                }
                current_batch_idx = Some(batch_idx);
            }
            take_indices.push(row_idx as u32);
        }
        if let Some(prev) = current_batch_idx {
            if !take_indices.is_empty() {
                arrays_to_concat.push((&batches[prev], take_indices));
            }
        }

        // Take from each batch and concat
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(arrays_to_concat.len());
        for (batch, idxs) in &arrays_to_concat {
            let source = batch.column(col_idx);
            let idx_array = arrow::array::UInt32Array::from(idxs.clone());
            let taken = compute::take(source, &idx_array, None)?;
            arrays.push(taken);
        }

        let array_refs: Vec<&dyn arrow::array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
        let combined = compute::concat(&array_refs)?;
        columns.push(combined);
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Write batches partitioned by the configured partition columns.
///
/// For each unique partition value combination:
/// 1. Extract matching rows into a sub-batch
/// 2. Write to a Hive-style directory (e.g., category=A/year=2024/uuid.parquet)
/// 3. Register partition values in metadata
async fn write_partitioned(
    table_writer: &DuckLakeTableWriter,
    metadata_writer: &Arc<dyn MetadataWriter>,
    schema_name: &str,
    table_name: &str,
    arrow_schema: &Schema,
    batches: &[RecordBatch],
    partition_columns: &[WritePartitionColumn],
    write_mode: WriteMode,
) -> crate::Result<u64> {
    let partition_map = route_batches_to_partitions(batches, partition_columns);

    let mut total_rows: u64 = 0;
    let mut first_partition = true;

    for (hive_dir, (partition_values, row_indices)) in &partition_map {
        let sub_batch = extract_rows(batches, row_indices).map_err(|e| {
            crate::error::DuckLakeError::Internal(format!(
                "Failed to extract partition rows: {}",
                e
            ))
        })?;

        // First partition handles Replace mode (ends existing files); subsequent partitions append
        let mode = if first_partition {
            write_mode
        } else {
            WriteMode::Append
        };
        first_partition = false;

        let mut session = table_writer.begin_write_partitioned(
            schema_name,
            table_name,
            arrow_schema,
            hive_dir,
            mode,
        )?;

        session.write_batch(&sub_batch)?;
        let row_count = session.row_count();
        let result = session.finish().await?;

        // Register partition values for this file
        let data_file_id = result.last_data_file_id;
        for (key_index, pval) in partition_values.iter().enumerate() {
            metadata_writer.register_file_partition_value(
                data_file_id,
                result.table_id,
                key_index as i32,
                pval.as_deref(),
            )?;
        }

        total_rows += row_count as u64;
    }

    Ok(total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_count_schema() {
        let schema = make_insert_count_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "count");
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
    }

    #[test]
    fn test_build_hive_dir() {
        let cols = vec![
            WritePartitionColumn {
                column_name: "category".to_string(),
                column_index: 1,
                transform: None,
            },
            WritePartitionColumn {
                column_name: "year".to_string(),
                column_index: 2,
                transform: Some("year".to_string()),
            },
        ];
        let values = vec![Some("A".to_string()), Some("2024".to_string())];
        assert_eq!(build_hive_dir(&cols, &values), "category=A/year=2024");
    }

    #[test]
    fn test_build_hive_dir_with_null() {
        let cols = vec![WritePartitionColumn {
            column_name: "region".to_string(),
            column_index: 0,
            transform: None,
        }];
        let values = vec![None];
        assert_eq!(
            build_hive_dir(&cols, &values),
            "region=__HIVE_DEFAULT_PARTITION__"
        );
    }

    #[test]
    fn test_compute_partition_value_identity() {
        let array = arrow::array::StringArray::from(vec!["hello", "world"]);
        assert_eq!(
            compute_partition_value(&array, 0, Some("identity")),
            Some("hello".to_string())
        );
        assert_eq!(
            compute_partition_value(&array, 1, None),
            Some("world".to_string())
        );
    }

    #[test]
    fn test_compute_partition_value_year() {
        // Date32: days since epoch
        let date = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let days_since_epoch = date
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        let array = arrow::array::Date32Array::from(vec![days_since_epoch]);
        assert_eq!(
            compute_partition_value(&array, 0, Some("year")),
            Some("2024".to_string())
        );
    }

    #[test]
    fn test_compute_partition_value_month() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let days_since_epoch = date
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        let array = arrow::array::Date32Array::from(vec![days_since_epoch]);
        assert_eq!(
            compute_partition_value(&array, 0, Some("month")),
            Some("3".to_string())
        );
    }

    #[test]
    fn test_route_batches_single_partition() {
        use arrow::array::StringArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["A", "A", "A"])),
            ],
        )
        .unwrap();

        let cols = vec![WritePartitionColumn {
            column_name: "category".to_string(),
            column_index: 1,
            transform: None,
        }];

        let result = route_batches_to_partitions(&[batch], &cols);
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("category=A"));
        assert_eq!(result["category=A"].1.len(), 3);
    }

    #[test]
    fn test_route_batches_multiple_partitions() {
        use arrow::array::StringArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["A", "B", "A", "C"])),
            ],
        )
        .unwrap();

        let cols = vec![WritePartitionColumn {
            column_name: "category".to_string(),
            column_index: 1,
            transform: None,
        }];

        let result = route_batches_to_partitions(&[batch], &cols);
        assert_eq!(result.len(), 3);
        assert_eq!(result["category=A"].1.len(), 2);
        assert_eq!(result["category=B"].1.len(), 1);
        assert_eq!(result["category=C"].1.len(), 1);
    }
}
