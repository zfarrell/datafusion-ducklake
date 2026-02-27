//! Table insertions functionality for DuckLake
//!
//! This module provides the `ducklake_table_insertions()` table function that returns
//! the actual inserted rows between snapshots. Unlike `ducklake_table_changes()`,
//! this function does NOT include CDC metadata columns (snapshot_id, change_type).
//! It returns only the table's own columns from data files added between snapshots.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;

use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::resolve_path;
use crate::table::validated_file_size;

#[cfg(feature = "encryption")]
use crate::encryption::EncryptionFactoryBuilder;
#[cfg(feature = "encryption")]
use datafusion::execution::parquet_encryption::EncryptionFactory;

/// TableProvider that exposes inserted rows between snapshots
///
/// Returns only the table columns from data files added between the start
/// and end snapshots (exclusive start, inclusive end). No CDC metadata
/// columns are appended.
#[derive(Debug)]
pub struct TableInsertionsTable {
    provider: Arc<dyn MetadataProvider>,
    table_id: i64,
    start_snapshot: i64,
    end_snapshot: i64,
    object_store_url: Arc<ObjectStoreUrl>,
    table_path: String,
    table_schema: SchemaRef,
}

impl TableInsertionsTable {
    pub fn new(
        provider: Arc<dyn MetadataProvider>,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
        object_store_url: Arc<ObjectStoreUrl>,
        table_path: String,
        table_schema: SchemaRef,
    ) -> Self {
        Self {
            provider,
            table_id,
            start_snapshot,
            end_snapshot,
            object_store_url,
            table_path,
            table_schema,
        }
    }
}

#[async_trait]
impl TableProvider for TableInsertionsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
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
        let data_files = self
            .provider
            .get_data_files_added_between_snapshots(
                self.table_id,
                self.start_snapshot,
                self.end_snapshot,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if data_files.is_empty() {
            use datafusion::physical_plan::empty::EmptyExec;
            let output_schema = match projection {
                Some(indices) => {
                    let fields: Vec<arrow::datatypes::Field> = indices
                        .iter()
                        .map(|&i| self.table_schema.field(i).clone())
                        .collect();
                    Arc::new(arrow::datatypes::Schema::new(fields))
                }
                None => self.table_schema.clone(),
            };
            return Ok(Arc::new(EmptyExec::new(output_schema)));
        }

        // Build encryption factory when encryption feature is enabled
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

        // Build PartitionedFile entries for all inserted files
        let mut files = Vec::with_capacity(data_files.len());
        for data_file in &data_files {
            let resolved_path = resolve_path(
                &self.table_path,
                &data_file.path,
                data_file.path_is_relative,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

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
            files.push(pf);
        }

        // Group all files into a single ParquetExec for efficiency
        let parquet_source = ParquetSource::default();
        #[cfg(feature = "encryption")]
        let parquet_source = if let Some(factory) = &encryption_factory {
            parquet_source.with_encryption_factory(Arc::clone(factory))
        } else {
            parquet_source
        };

        let file_groups: Vec<FileGroup> = files
            .into_iter()
            .map(|f| FileGroup::new(vec![f]))
            .collect();

        let mut builder = FileScanConfigBuilder::new(
            self.object_store_url.as_ref().clone(),
            self.table_schema.clone(),
            Arc::new(parquet_source),
        );
        for fg in file_groups {
            builder = builder.with_file_group(fg);
        }
        if let Some(proj) = projection {
            builder = builder.with_projection_indices(Some(proj.clone()));
        }

        let file_scan_config = builder.build();
        Ok(DataSourceExec::from_data_source(file_scan_config))
    }
}
