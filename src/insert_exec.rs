//! DuckLake INSERT execution plan.
//!
//! Delegates to [`DuckLakeTableWriter`] for file writing and metadata commit.
//! See `table_writer.rs` module docs for write atomicity guarantees.
//!
//! Supports partitioned writes: when partition columns are configured, rows
//! are routed to per-partition Parquet files in Hive-style directory layout.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::compute;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::{self, TryStreamExt};

use crate::error::DuckLakeError;
use crate::metadata_writer::{MetadataWriter, WriteMode};
use crate::table_writer::DuckLakeTableWriter;

// chrono re-export from sqlx for partition transform date handling
#[cfg(any(
    feature = "metadata-sqlite",
    feature = "metadata-postgres",
    feature = "metadata-mysql"
))]
use sqlx::types::chrono;

use crate::delete_exec::make_dml_count_schema;

/// Resolved partition transform, computed once at planning time to avoid
/// repeated `to_lowercase()` + string matching on the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
}

impl PartitionTransform {
    /// Parse an optional transform string into a [`PartitionTransform`].
    /// `None`, empty, or `"identity"` all map to [`PartitionTransform::Identity`].
    pub fn from_str_opt(s: Option<&str>) -> Self {
        match s.map(|t| t.to_lowercase()).as_deref() {
            None | Some("") | Some("identity") => Self::Identity,
            Some("year") => Self::Year,
            Some("month") => Self::Month,
            Some("day") => Self::Day,
            Some("hour") => Self::Hour,
            Some(_) => Self::Identity, // unknown transforms default to identity
        }
    }
}

/// Partition column info for write-side partitioning.
#[derive(Debug, Clone)]
pub struct WritePartitionColumn {
    /// Name of the source column in the table schema
    pub column_name: String,
    /// Index of this column in the table schema
    pub column_index: usize,
    /// Pre-resolved transform enum (avoids per-row string matching)
    pub resolved_transform: PartitionTransform,
    /// Original transform string (kept for metadata/serialization)
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
            EquivalenceProperties::new(make_dml_count_schema()),
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
        let output_schema = make_dml_count_schema();

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
            crate::table_writer::validate_not_null_constraints(&arrow_schema, &batches)?;

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
                u64::try_from(result.records_written).map_err(|e| {
                    DataFusionError::Execution(format!("Record count overflow: {}", e))
                })?
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
            make_dml_count_schema(),
            stream,
        )))
    }
}

/// Compute partition value for a single element from a column array.
///
/// Applies the configured transform to produce the Hive directory value.
/// Returns an error for unsupported column types or unknown transforms.
fn compute_partition_value(
    array: &dyn arrow::array::Array,
    row: usize,
    transform: PartitionTransform,
) -> crate::Result<Option<String>> {
    use arrow::array::*;

    if array.is_null(row) {
        return Ok(None);
    }

    match transform {
        PartitionTransform::Identity => {
            // Extract the raw value as a string
            macro_rules! downcast_partition {
                ($arr_type:ty) => {
                    array.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                        DuckLakeError::Internal(format!(
                            "Failed to downcast partition column to {}",
                            stringify!($arr_type)
                        ))
                    })
                };
            }
            let value = match array.data_type() {
                DataType::Int8 => Some(downcast_partition!(Int8Array)?.value(row).to_string()),
                DataType::Int16 => Some(downcast_partition!(Int16Array)?.value(row).to_string()),
                DataType::Int32 => Some(downcast_partition!(Int32Array)?.value(row).to_string()),
                DataType::Int64 => Some(downcast_partition!(Int64Array)?.value(row).to_string()),
                DataType::UInt8 => Some(downcast_partition!(UInt8Array)?.value(row).to_string()),
                DataType::UInt16 => Some(downcast_partition!(UInt16Array)?.value(row).to_string()),
                DataType::UInt32 => Some(downcast_partition!(UInt32Array)?.value(row).to_string()),
                DataType::UInt64 => Some(downcast_partition!(UInt64Array)?.value(row).to_string()),
                DataType::Float32 => {
                    Some(downcast_partition!(Float32Array)?.value(row).to_string())
                },
                DataType::Float64 => {
                    Some(downcast_partition!(Float64Array)?.value(row).to_string())
                },
                DataType::Utf8 => Some(downcast_partition!(StringArray)?.value(row).to_string()),
                DataType::LargeUtf8 => Some(
                    downcast_partition!(LargeStringArray)?
                        .value(row)
                        .to_string(),
                ),
                DataType::Boolean => {
                    Some(downcast_partition!(BooleanArray)?.value(row).to_string())
                },
                DataType::Date32 => {
                    let days = downcast_partition!(Date32Array)?.value(row);
                    chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                        .map(|date| date.format("%Y-%m-%d").to_string())
                },
                DataType::Date64 => {
                    let ms = downcast_partition!(Date64Array)?.value(row);
                    chrono::DateTime::from_timestamp_millis(ms)
                        .map(|dt| dt.format("%Y-%m-%d").to_string())
                },
                dt => {
                    return Err(DuckLakeError::InvalidConfig(format!(
                        "Unsupported partition column type {:?} for identity transform",
                        dt
                    )));
                },
            };
            Ok(value)
        },
        PartitionTransform::Year => extract_temporal_component(array, row, TemporalComponent::Year),
        PartitionTransform::Month => {
            extract_temporal_component(array, row, TemporalComponent::Month)
        },
        PartitionTransform::Day => extract_temporal_component(array, row, TemporalComponent::Day),
        PartitionTransform::Hour => extract_temporal_component(array, row, TemporalComponent::Hour),
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
) -> crate::Result<Option<String>> {
    use arrow::array::*;

    match array.data_type() {
        DataType::Date32 => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|a| a.value(row));
            let date = days.and_then(|d| chrono::NaiveDate::from_num_days_from_ce_opt(d + 719_163));
            Ok(date.map(|d| match component {
                TemporalComponent::Year => d.format("%Y").to_string(),
                TemporalComponent::Month => d.format("%-m").to_string(),
                TemporalComponent::Day => d.format("%-d").to_string(),
                TemporalComponent::Hour => "0".to_string(),
            }))
        },
        DataType::Date64 => {
            let ms = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .map(|a| a.value(row));
            let dt = ms.and_then(chrono::DateTime::from_timestamp_millis);
            Ok(dt.map(|d| match component {
                TemporalComponent::Year => d.format("%Y").to_string(),
                TemporalComponent::Month => d.format("%-m").to_string(),
                TemporalComponent::Day => d.format("%-d").to_string(),
                TemporalComponent::Hour => d.format("%-H").to_string(),
            }))
        },
        DataType::Timestamp(unit, _) => {
            // Convert all timestamp precisions to microseconds for uniform handling
            let us = match unit {
                arrow::datatypes::TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .map(|a| a.value(row) * 1_000_000),
                arrow::datatypes::TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .map(|a| a.value(row) * 1_000),
                arrow::datatypes::TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .map(|a| a.value(row)),
                arrow::datatypes::TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .map(|a| a.value(row) / 1_000),
            };
            let dt = us.and_then(chrono::DateTime::from_timestamp_micros);
            Ok(dt.map(|d| match component {
                TemporalComponent::Year => d.format("%Y").to_string(),
                TemporalComponent::Month => d.format("%-m").to_string(),
                TemporalComponent::Day => d.format("%-d").to_string(),
                TemporalComponent::Hour => d.format("%-H").to_string(),
            }))
        },
        dt => Err(DuckLakeError::InvalidConfig(format!(
            "Cannot apply temporal partition transform to non-temporal type {:?}",
            dt
        ))),
    }
}

/// URL-encode a partition value per Hive convention.
fn url_encode_partition_value(value: &str) -> String {
    use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
    const HIVE_ENCODE_SET: &AsciiSet = &CONTROLS
        .add(b' ')
        .add(b'/')
        .add(b'=')
        .add(b'%')
        .add(b'#')
        .add(b'?')
        .add(b'\\')
        .add(b'"')
        .add(b'<')
        .add(b'>')
        .add(b'{')
        .add(b'}')
        .add(b'|')
        .add(b'^')
        .add(b'`')
        .add(b'[')
        .add(b']');
    utf8_percent_encode(value, HIVE_ENCODE_SET).to_string()
}

/// Build a Hive-style partition directory path from partition values.
///
/// Partition values are URL-encoded to prevent path traversal and malformed paths.
fn build_hive_dir(partition_columns: &[WritePartitionColumn], values: &[Option<String>]) -> String {
    partition_columns
        .iter()
        .zip(values.iter())
        .map(|(col, val)| {
            let name = &col.column_name;
            match val {
                Some(v) => format!("{}={}", name, url_encode_partition_value(v)),
                None => format!("{}=__HIVE_DEFAULT_PARTITION__", name),
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Route rows into partition buckets, keyed by their partition values.
///
/// Returns an error if any partition value computation fails (unsupported type or transform).
fn route_batches_to_partitions(
    batches: &[RecordBatch],
    partition_columns: &[WritePartitionColumn],
) -> crate::Result<BTreeMap<String, (Vec<Option<String>>, Vec<(usize, usize)>)>> {
    // For identity-only partitions, use optimized path that pre-computes
    // partition values per column per batch (avoids per-row type dispatch).
    let all_identity = partition_columns
        .iter()
        .all(|pc| pc.resolved_transform == PartitionTransform::Identity);

    if all_identity {
        route_batches_identity(batches, partition_columns)
    } else {
        route_batches_generic(batches, partition_columns)
    }
}

/// Optimized partition routing for identity transforms.
///
/// Pre-computes string values for each partition column per batch (single type
/// dispatch per column), then groups rows by partition key. This avoids
/// O(rows × columns) type dispatches in the inner loop.
fn route_batches_identity(
    batches: &[RecordBatch],
    partition_columns: &[WritePartitionColumn],
) -> crate::Result<BTreeMap<String, (Vec<Option<String>>, Vec<(usize, usize)>)>> {
    let mut partitions: BTreeMap<String, (Vec<Option<String>>, Vec<(usize, usize)>)> =
        BTreeMap::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        // Pre-compute all partition values per column (one type dispatch per column per batch)
        let col_values: Vec<Vec<Option<String>>> = partition_columns
            .iter()
            .map(|pc| {
                let array = batch.column(pc.column_index);
                precompute_identity_values(array.as_ref())
            })
            .collect::<crate::Result<Vec<_>>>()?;

        for row_idx in 0..batch.num_rows() {
            let values: Vec<Option<String>> =
                col_values.iter().map(|col| col[row_idx].clone()).collect();
            let key = build_hive_dir(partition_columns, &values);
            partitions
                .entry(key)
                .or_insert_with(|| (values, Vec::new()))
                .1
                .push((batch_idx, row_idx));
        }
    }

    Ok(partitions)
}

/// Pre-compute identity partition values for all rows in an array.
///
/// Performs a single type dispatch, then iterates all rows with the resolved
/// typed array reference. This is much faster than dispatching per row.
fn precompute_identity_values(
    array: &dyn arrow::array::Array,
) -> crate::Result<Vec<Option<String>>> {
    use arrow::array::*;

    let len = array.len();
    let mut values = Vec::with_capacity(len);

    macro_rules! extract_all {
        ($array_type:ty) => {{
            let a = array
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or_else(|| {
                    DuckLakeError::Internal(format!(
                        "Failed to downcast {:?} array",
                        array.data_type()
                    ))
                })?;
            for i in 0..len {
                values.push(if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i).to_string())
                });
            }
        }};
    }

    match array.data_type() {
        DataType::Int8 => extract_all!(Int8Array),
        DataType::Int16 => extract_all!(Int16Array),
        DataType::Int32 => extract_all!(Int32Array),
        DataType::Int64 => extract_all!(Int64Array),
        DataType::UInt8 => extract_all!(UInt8Array),
        DataType::UInt16 => extract_all!(UInt16Array),
        DataType::UInt32 => extract_all!(UInt32Array),
        DataType::UInt64 => extract_all!(UInt64Array),
        DataType::Float32 => extract_all!(Float32Array),
        DataType::Float64 => extract_all!(Float64Array),
        DataType::Utf8 => extract_all!(StringArray),
        DataType::LargeUtf8 => extract_all!(LargeStringArray),
        DataType::Boolean => extract_all!(BooleanArray),
        DataType::Date32 => {
            let a = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DuckLakeError::Internal("Failed to downcast Date32 array".to_string())
                })?;
            for i in 0..len {
                values.push(if a.is_null(i) {
                    None
                } else {
                    chrono::NaiveDate::from_num_days_from_ce_opt(a.value(i) + 719_163)
                        .map(|date| date.format("%Y-%m-%d").to_string())
                });
            }
        },
        DataType::Date64 => {
            let a = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    DuckLakeError::Internal("Failed to downcast Date64 array".to_string())
                })?;
            for i in 0..len {
                values.push(if a.is_null(i) {
                    None
                } else {
                    chrono::DateTime::from_timestamp_millis(a.value(i))
                        .map(|dt| dt.format("%Y-%m-%d").to_string())
                });
            }
        },
        dt => {
            return Err(DuckLakeError::InvalidConfig(format!(
                "Unsupported partition column type {:?} for identity transform",
                dt
            )));
        },
    }

    Ok(values)
}

/// Generic row-by-row partition routing for non-identity transforms.
fn route_batches_generic(
    batches: &[RecordBatch],
    partition_columns: &[WritePartitionColumn],
) -> crate::Result<BTreeMap<String, (Vec<Option<String>>, Vec<(usize, usize)>)>> {
    let mut partitions: BTreeMap<String, (Vec<Option<String>>, Vec<(usize, usize)>)> =
        BTreeMap::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(partition_columns.len());
            for pc in partition_columns {
                let array = batch.column(pc.column_index);
                let val = compute_partition_value(array.as_ref(), row_idx, pc.resolved_transform)?;
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

    Ok(partitions)
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
            take_indices.push(
                u32::try_from(row_idx)
                    .map_err(|e| DuckLakeError::Internal(format!("Row index overflow: {}", e)))?,
            );
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
/// Uses a single write transaction for ALL partitions to ensure atomicity:
/// 1. Set up catalog metadata once (single snapshot, single set of column IDs)
/// 2. Upload all partition files (Parquet serialization + object store upload)
/// 3. Commit all files atomically (end old files for Replace, register all new files)
///
/// If any upload fails, previously uploaded files are cleaned up and no metadata
/// is committed.
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
    use crate::table_writer::arrow_schema_to_column_defs;

    let partition_map = route_batches_to_partitions(batches, partition_columns)?;

    // 1. Single write transaction setup for ALL partitions
    let columns = arrow_schema_to_column_defs(arrow_schema)?;
    let setup =
        metadata_writer.begin_write_transaction(schema_name, table_name, &columns, write_mode)?;

    // 2. Upload phase: write and upload all partition files (no metadata commit yet)
    let mut uploaded_files = Vec::with_capacity(partition_map.len());

    for (hive_dir, (partition_values, row_indices)) in &partition_map {
        let sub_batch = extract_rows(batches, row_indices).map_err(|e| {
            crate::error::DuckLakeError::Internal(format!(
                "Failed to extract partition rows: {}",
                e
            ))
        })?;

        let mut session = table_writer.begin_write_partitioned_with_setup(
            schema_name,
            table_name,
            arrow_schema,
            hive_dir,
            &setup,
        )?;

        session.write_batch(&sub_batch)?;

        match session.upload().await {
            Ok(upload) => {
                uploaded_files.push((upload, partition_values.clone()));
            },
            Err(e) => {
                // Clean up any files already uploaded before this failure
                let already_uploaded: Vec<_> = uploaded_files.into_iter().map(|(u, _)| u).collect();
                table_writer.cleanup_uploaded_files(&already_uploaded).await;
                return Err(e);
            },
        }
    }

    // 3. Atomic commit: end old files (if Replace) and register all new files + partition values
    let result = table_writer
        .commit_uploaded_files(&setup, uploaded_files, write_mode)
        .await?;

    Ok(u64::try_from(result.records_written)
        .map_err(|e| DataFusionError::Execution(format!("Record count overflow: {}", e)))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn test_insert_count_schema() {
        let schema = make_dml_count_schema();
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
                resolved_transform: PartitionTransform::Identity,
                transform: None,
            },
            WritePartitionColumn {
                column_name: "year".to_string(),
                column_index: 2,
                resolved_transform: PartitionTransform::Year,
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
            resolved_transform: PartitionTransform::Identity,
            transform: None,
        }];
        let values = vec![None];
        assert_eq!(
            build_hive_dir(&cols, &values),
            "region=__HIVE_DEFAULT_PARTITION__"
        );
    }

    #[test]
    fn test_build_hive_dir_url_encodes_special_chars() {
        let cols = vec![WritePartitionColumn {
            column_name: "path".to_string(),
            column_index: 0,
            resolved_transform: PartitionTransform::Identity,
            transform: None,
        }];
        let values = vec![Some("a/b".to_string())];
        let result = build_hive_dir(&cols, &values);
        assert_eq!(result, "path=a%2Fb");

        let values = vec![Some("x=y".to_string())];
        let result = build_hive_dir(&cols, &values);
        assert_eq!(result, "path=x%3Dy");

        let values = vec![Some("../../etc/passwd".to_string())];
        let result = build_hive_dir(&cols, &values);
        assert!(result.contains("%2F"));
    }

    #[test]
    fn test_compute_partition_value_identity() {
        let array = arrow::array::StringArray::from(vec!["hello", "world"]);
        assert_eq!(
            compute_partition_value(&array, 0, PartitionTransform::Identity).unwrap(),
            Some("hello".to_string())
        );
        assert_eq!(
            compute_partition_value(&array, 1, PartitionTransform::Identity).unwrap(),
            Some("world".to_string())
        );
    }

    #[test]
    fn test_compute_partition_value_year() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let days_since_epoch: i32 = date
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days()
            .try_into()
            .expect("test date within i32 range");
        let array = arrow::array::Date32Array::from(vec![days_since_epoch]);
        assert_eq!(
            compute_partition_value(&array, 0, PartitionTransform::Year).unwrap(),
            Some("2024".to_string())
        );
    }

    #[test]
    fn test_compute_partition_value_month() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let days_since_epoch: i32 = date
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days()
            .try_into()
            .expect("test date within i32 range");
        let array = arrow::array::Date32Array::from(vec![days_since_epoch]);
        assert_eq!(
            compute_partition_value(&array, 0, PartitionTransform::Month).unwrap(),
            Some("3".to_string())
        );
    }

    #[test]
    fn test_partition_transform_from_str_opt() {
        assert_eq!(
            PartitionTransform::from_str_opt(None),
            PartitionTransform::Identity
        );
        assert_eq!(
            PartitionTransform::from_str_opt(Some("")),
            PartitionTransform::Identity
        );
        assert_eq!(
            PartitionTransform::from_str_opt(Some("identity")),
            PartitionTransform::Identity
        );
        assert_eq!(
            PartitionTransform::from_str_opt(Some("IDENTITY")),
            PartitionTransform::Identity
        );
        assert_eq!(
            PartitionTransform::from_str_opt(Some("year")),
            PartitionTransform::Year
        );
        assert_eq!(
            PartitionTransform::from_str_opt(Some("Year")),
            PartitionTransform::Year
        );
        assert_eq!(
            PartitionTransform::from_str_opt(Some("month")),
            PartitionTransform::Month
        );
        assert_eq!(
            PartitionTransform::from_str_opt(Some("day")),
            PartitionTransform::Day
        );
        assert_eq!(
            PartitionTransform::from_str_opt(Some("hour")),
            PartitionTransform::Hour
        );
        // Unknown transforms default to Identity
        assert_eq!(
            PartitionTransform::from_str_opt(Some("yer")),
            PartitionTransform::Identity
        );
    }

    #[test]
    fn test_compute_partition_value_unsupported_type_errors() {
        let array = arrow::array::BinaryArray::from(vec![b"data".as_slice()]);
        let result = compute_partition_value(&array, 0, PartitionTransform::Identity);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported partition column type")
        );
    }

    #[test]
    fn test_temporal_transform_all_timestamp_precisions() {
        // 2024-06-15 09:30:00 UTC
        let us = 1_718_443_800_000_000i64;

        let sec_array = arrow::array::TimestampSecondArray::from(vec![us / 1_000_000]);
        assert_eq!(
            compute_partition_value(&sec_array, 0, PartitionTransform::Year).unwrap(),
            Some("2024".to_string())
        );
        assert_eq!(
            compute_partition_value(&sec_array, 0, PartitionTransform::Hour).unwrap(),
            Some("9".to_string())
        );

        let ms_array = arrow::array::TimestampMillisecondArray::from(vec![us / 1_000]);
        assert_eq!(
            compute_partition_value(&ms_array, 0, PartitionTransform::Year).unwrap(),
            Some("2024".to_string())
        );

        let us_array = arrow::array::TimestampMicrosecondArray::from(vec![us]);
        assert_eq!(
            compute_partition_value(&us_array, 0, PartitionTransform::Month).unwrap(),
            Some("6".to_string())
        );

        let ns_array = arrow::array::TimestampNanosecondArray::from(vec![us * 1_000]);
        assert_eq!(
            compute_partition_value(&ns_array, 0, PartitionTransform::Day).unwrap(),
            Some("15".to_string())
        );
    }

    #[test]
    fn test_temporal_transform_on_non_temporal_type_errors() {
        let array = arrow::array::StringArray::from(vec!["not-a-date"]);
        let result = compute_partition_value(&array, 0, PartitionTransform::Year);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("non-temporal type")
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
            resolved_transform: PartitionTransform::Identity,
            transform: None,
        }];

        let result = route_batches_to_partitions(&[batch], &cols).unwrap();
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
            resolved_transform: PartitionTransform::Identity,
            transform: None,
        }];

        let result = route_batches_to_partitions(&[batch], &cols).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result["category=A"].1.len(), 2);
        assert_eq!(result["category=B"].1.len(), 1);
        assert_eq!(result["category=C"].1.len(), 1);
    }
}
