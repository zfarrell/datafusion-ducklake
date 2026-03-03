//! Table changes (CDC) functionality for DuckLake
//!
//! This module provides the `ducklake_table_changes()` table function that returns
//! actual row data from Parquet files with additional CDC metadata columns.
//!
//! Note: Ordering across files is undefined unless explicitly requested via ORDER BY.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, Int64Array, StringArray};
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

use crate::metadata_provider::{DataFileChange, MetadataProvider};
use crate::path_resolver::resolve_path;
use crate::table::validated_file_size;

#[cfg(feature = "encryption")]
use crate::encryption::EncryptionFactoryBuilder;
#[cfg(feature = "encryption")]
use datafusion::execution::parquet_encryption::EncryptionFactory;

/// Type of change captured in CDC output
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    Insert,
    Delete,
}

impl ChangeType {
    /// Returns the string representation for Arrow output
    fn as_str(&self) -> &'static str {
        match self {
            ChangeType::Insert => "insert",
            ChangeType::Delete => "delete",
        }
    }
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Custom execution plan that appends CDC columns (snapshot_id, change_type) to each batch
///
/// This plan wraps a ParquetExec and appends CDC metadata columns to each output batch.
/// It supports projection pushdown by:
/// - Reading only requested table columns from Parquet
/// - Including only requested CDC columns in output
/// - Optionally skipping input columns entirely when only CDC columns are needed
#[derive(Debug)]
pub struct AppendCDCColumnsExec {
    /// The input execution plan (typically ParquetExec)
    input: Arc<dyn ExecutionPlan>,
    /// Snapshot ID for this file
    snapshot_id: i64,
    /// Change type for this file
    change_type: ChangeType,
    /// Whether to include snapshot_id in output
    include_snapshot_id: bool,
    /// Whether to include change_type in output
    include_change_type: bool,
    /// If true, input columns are dummy (for row count only) and should not be included
    skip_input_columns: bool,
    /// Output schema (projected input schema + requested CDC columns)
    output_schema: SchemaRef,
    /// Reorder mapping: output_pos -> natural_pos. None if no reordering needed.
    reorder_indices: Option<Vec<usize>>,
    /// Cached plan properties with updated schema
    properties: PlanProperties,
}

impl AppendCDCColumnsExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        snapshot_id: i64,
        change_type: ChangeType,
        include_snapshot_id: bool,
        include_change_type: bool,
        skip_input_columns: bool,
        output_schema: SchemaRef,
    ) -> Self {
        Self::new_with_reorder(
            input,
            snapshot_id,
            change_type,
            include_snapshot_id,
            include_change_type,
            skip_input_columns,
            output_schema,
            None,
        )
    }

    pub fn new_with_reorder(
        input: Arc<dyn ExecutionPlan>,
        snapshot_id: i64,
        change_type: ChangeType,
        include_snapshot_id: bool,
        include_change_type: bool,
        skip_input_columns: bool,
        output_schema: SchemaRef,
        reorder_indices: Option<Vec<usize>>,
    ) -> Self {
        // Create new equivalence properties with the output schema.
        // We preserve partitioning and execution semantics from input.
        // Note: This resets equivalences which is pessimistic but correct.
        // Future optimization: carry forward equivalences for projected table columns.
        let eq_properties = EquivalenceProperties::new(output_schema.clone());

        let input_props = input.properties();
        let properties = PlanProperties::new(
            eq_properties,
            input_props.output_partitioning().clone(),
            input_props.emission_type,
            input_props.boundedness,
        );

        Self {
            input,
            snapshot_id,
            change_type,
            include_snapshot_id,
            include_change_type,
            skip_input_columns,
            output_schema,
            reorder_indices,
            properties,
        }
    }
}

impl DisplayAs for AppendCDCColumnsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "AppendCDCColumnsExec: snapshot_id={}, change_type={}, \
                     include_snapshot={}, include_change={}, skip_input={}",
                    self.snapshot_id,
                    self.change_type,
                    self.include_snapshot_id,
                    self.include_change_type,
                    self.skip_input_columns
                )
            },
        }
    }
}

impl ExecutionPlan for AppendCDCColumnsExec {
    fn name(&self) -> &str {
        "AppendCDCColumnsExec"
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
                "AppendCDCColumnsExec expects exactly one child".into(),
            ));
        }

        Ok(Arc::new(AppendCDCColumnsExec::new_with_reorder(
            children[0].clone(),
            self.snapshot_id,
            self.change_type,
            self.include_snapshot_id,
            self.include_change_type,
            self.skip_input_columns,
            self.output_schema.clone(),
            self.reorder_indices.clone(),
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(AppendCDCColumnsStream {
            input: input_stream,
            snapshot_id: self.snapshot_id,
            change_type: self.change_type,
            include_snapshot_id: self.include_snapshot_id,
            include_change_type: self.include_change_type,
            skip_input_columns: self.skip_input_columns,
            reorder_indices: self.reorder_indices.clone(),
            output_schema: self.output_schema.clone(),
        }))
    }
}

/// Stream that appends CDC columns to input batches
///
/// Takes a stream of record batches and appends `snapshot_id` and/or
/// `change_type` columns to each batch.
struct AppendCDCColumnsStream {
    // Note: manual Debug is not derived because SendableRecordBatchStream
    // does not implement Debug. Use the struct name for debug output.
    input: SendableRecordBatchStream,
    snapshot_id: i64,
    change_type: ChangeType,
    include_snapshot_id: bool,
    include_change_type: bool,
    skip_input_columns: bool,
    reorder_indices: Option<Vec<usize>>,
    output_schema: SchemaRef,
}

impl fmt::Debug for AppendCDCColumnsStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendCDCColumnsStream")
            .field("snapshot_id", &self.snapshot_id)
            .field("change_type", &self.change_type)
            .field("include_snapshot_id", &self.include_snapshot_id)
            .field("include_change_type", &self.include_change_type)
            .field("skip_input_columns", &self.skip_input_columns)
            .finish_non_exhaustive()
    }
}

impl Stream for AppendCDCColumnsStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = self.transform_batch(&batch);
                Poll::Ready(Some(result))
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AppendCDCColumnsStream {
    fn transform_batch(&self, batch: &RecordBatch) -> DataFusionResult<RecordBatch> {
        let num_rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::new();

        // Build columns in natural order: table columns, then CDC columns
        if !self.skip_input_columns {
            columns.extend(batch.columns().iter().cloned());
        }

        // Append requested CDC columns
        if self.include_snapshot_id {
            columns.push(Arc::new(Int64Array::from(vec![self.snapshot_id; num_rows])));
        }
        if self.include_change_type {
            columns.push(Arc::new(StringArray::from(vec![
                self.change_type.as_str();
                num_rows
            ])));
        }

        // Apply reorder if projection requested non-natural column order
        let columns = if let Some(ref reorder) = self.reorder_indices {
            reorder.iter().map(|&i| columns[i].clone()).collect()
        } else {
            columns
        };

        RecordBatch::try_new(self.output_schema.clone(), columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl RecordBatchStream for AppendCDCColumnsStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

// R4-S-027: Use shared CDC projection analysis from cdc_common module
use crate::cdc_common::{CdcProjectionAnalysis, analyze_cdc_projection};

#[derive(Debug)]
pub struct TableChangesTable {
    provider: Arc<dyn MetadataProvider>,
    table_id: i64,
    start_snapshot: i64,
    end_snapshot: i64,
    /// Object store URL for resolving file paths
    object_store_url: Arc<ObjectStoreUrl>,
    /// Table path for resolving relative file paths
    table_path: String,
    /// Original table schema (without CDC columns)
    table_schema: SchemaRef,
    /// Combined schema: table columns + snapshot_id + change_type
    output_schema: SchemaRef,
}

impl TableChangesTable {
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
    fn analyze_projection(&self, projection: Option<&Vec<usize>>) -> DataFusionResult<CdcProjectionAnalysis> {
        analyze_cdc_projection(projection, &self.table_schema, &self.output_schema)
    }

    /// Build the schema that AppendCDCColumnsExec will output
    fn build_cdc_exec_schema(
        &self,
        table_indices: &[usize],
        need_snapshot_id: bool,
        need_change_type: bool,
    ) -> SchemaRef {
        let mut fields: Vec<Field> = table_indices
            .iter()
            .map(|&i| self.table_schema.field(i).clone())
            .collect();

        if need_snapshot_id {
            fields.push(Field::new("snapshot_id", DataType::Int64, false));
        }
        if need_change_type {
            fields.push(Field::new("change_type", DataType::Utf8, false));
        }

        Arc::new(Schema::new(fields))
    }

    /// Build a ParquetExec wrapped with AppendCDCColumnsExec for a single file
    #[cfg(feature = "encryption")]
    async fn build_exec_for_file(
        &self,
        state: &dyn Session,
        data_file: &DataFileChange,
        proj_info: &CdcProjectionAnalysis,
        encryption_factory: &Option<Arc<dyn EncryptionFactory>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let parquet_source = if let Some(factory) = encryption_factory {
            ParquetSource::default().with_encryption_factory(Arc::clone(factory))
        } else {
            ParquetSource::default()
        };
        self.build_exec_for_file_impl(state, data_file, proj_info, parquet_source)
            .await
    }

    /// Build a ParquetExec wrapped with AppendCDCColumnsExec for a single file
    #[cfg(not(feature = "encryption"))]
    async fn build_exec_for_file(
        &self,
        state: &dyn Session,
        data_file: &DataFileChange,
        proj_info: &CdcProjectionAnalysis,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.build_exec_for_file_impl(state, data_file, proj_info, ParquetSource::default())
            .await
    }

    /// Internal implementation for building a ParquetExec wrapped with AppendCDCColumnsExec
    async fn build_exec_for_file_impl(
        &self,
        _state: &dyn Session,
        data_file: &DataFileChange,
        proj_info: &CdcProjectionAnalysis,
        parquet_source: ParquetSource,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Resolve file path
        let resolved_path = resolve_path(
            &self.table_path,
            &data_file.path,
            data_file.path_is_relative,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Create PartitionedFile with footer size hint if available
        let mut pf = PartitionedFile::new(
            &resolved_path,
            validated_file_size(data_file.file_size_bytes, &resolved_path)?,
        );
        if let Some(footer_size) = data_file.footer_size
            && footer_size > 0
            && let Ok(hint) = usize::try_from(footer_size)
        {
            pf = pf.with_metadata_size_hint(hint);
        }

        // Determine what to read from Parquet
        let parquet_projection = if proj_info.table_indices.is_empty() {
            // Only CDC columns requested - read minimal data for row counts
            Some(vec![0])
        } else {
            Some(proj_info.table_indices.clone())
        };

        // Create file scan config with projection pushdown
        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            self.table_schema.clone(),
            Arc::new(parquet_source),
        )
        .with_file_group(FileGroup::new(vec![pf]));

        if let Some(proj) = parquet_projection {
            builder = builder.with_projection_indices(Some(proj));
        }

        let file_scan_config = builder.build();

        // Use DataSourceExec directly to preserve our ParquetSource with encryption factory
        let parquet_exec: Arc<dyn ExecutionPlan> =
            DataSourceExec::from_data_source(file_scan_config);

        // Determine if we should skip input columns (only CDC columns requested)
        let skip_input_columns = proj_info.table_indices.is_empty();

        // Build output schema for AppendCDCColumnsExec
        let cdc_exec_schema = if skip_input_columns {
            // Only CDC columns - build schema with just those
            let mut fields = Vec::new();
            if proj_info.need_snapshot_id {
                fields.push(Field::new("snapshot_id", DataType::Int64, false));
            }
            if proj_info.need_change_type {
                fields.push(Field::new("change_type", DataType::Utf8, false));
            }
            Arc::new(Schema::new(fields))
        } else {
            self.build_cdc_exec_schema(
                &proj_info.table_indices,
                proj_info.need_snapshot_id,
                proj_info.need_change_type,
            )
        };

        // Use the projection-ordered schema when reordering is needed
        let exec_output_schema = if proj_info.reorder_indices.is_some() {
            proj_info.output_schema.clone()
        } else {
            cdc_exec_schema
        };

        Ok(Arc::new(AppendCDCColumnsExec::new_with_reorder(
            parquet_exec,
            data_file.begin_snapshot,
            ChangeType::Insert,
            proj_info.need_snapshot_id,
            proj_info.need_change_type,
            skip_input_columns,
            exec_output_schema,
            proj_info.reorder_indices.clone(),
        )))
    }
}

#[async_trait]
impl TableProvider for TableChangesTable {
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::prelude::Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Analyze projection to determine what to read
        let proj_info = self.analyze_projection(projection)?;

        // Get data files added between snapshots (INSERT changes)
        let data_files = self
            .provider
            .get_data_files_added_between_snapshots(
                self.table_id,
                self.start_snapshot,
                self.end_snapshot,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Handle empty case
        if data_files.is_empty() {
            use datafusion::physical_plan::empty::EmptyExec;
            return Ok(Arc::new(EmptyExec::new(proj_info.output_schema)));
        }

        // Build encryption factory from file encryption keys (when encryption feature is enabled)
        #[cfg(feature = "encryption")]
        let encryption_factory: Option<Arc<dyn EncryptionFactory>> = {
            let mut builder = EncryptionFactoryBuilder::new();
            for data_file in &data_files {
                let resolved_path = resolve_path(
                    &self.table_path,
                    &data_file.path,
                    data_file.path_is_relative,
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                builder.add_file(&resolved_path, data_file.encryption_key.as_deref());
            }
            let factory = builder.build();
            if factory.has_encrypted_files() {
                Some(Arc::new(factory) as Arc<dyn EncryptionFactory>)
            } else {
                None
            }
        };

        // Build execution plan for each file with projection pushdown
        let mut execs: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(data_files.len());
        for data_file in &data_files {
            #[cfg(feature = "encryption")]
            let exec = self
                .build_exec_for_file(state, data_file, &proj_info, &encryption_factory)
                .await?;
            #[cfg(not(feature = "encryption"))]
            let exec = self
                .build_exec_for_file(state, data_file, &proj_info)
                .await?;
            execs.push(exec);
        }

        // Combine with UnionExec if multiple files
        if execs.len() == 1 {
            Ok(execs.into_iter().next().expect("checked len == 1 above"))
        } else {
            UnionExec::try_new(execs)
        }
    }
}
