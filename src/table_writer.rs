//! High-level table writer for DuckLake catalogs.
//!
//! # Write Atomicity Guarantees
//!
//! The write path follows a **write-then-commit** pattern:
//!
//! 1. **Parquet file upload** — Data is serialized and uploaded to the object store.
//! 2. **Metadata commit** — The catalog database is updated to reference the new file.
//!
//! ## Failure Modes
//!
//! - **Process crash between steps 1 and 2**: The Parquet file exists on the object
//!   store but is not referenced by any catalog entry. It becomes an orphaned file
//!   that can be cleaned up via a garbage-collection sweep (not yet implemented).
//!
//! - **Object store upload succeeds, metadata commit fails**: The `finish()` method
//!   performs best-effort cleanup by deleting the uploaded Parquet file. If cleanup
//!   also fails (e.g., object store is temporarily unavailable), a warning is logged
//!   and the original commit error is propagated. The file becomes orphaned.
//!
//! - **Object store upload fails**: No metadata is written; the operation is cleanly
//!   aborted with no side effects.
//!
//! ## Guarantees
//!
//! - A successful `finish()` call means both the file and metadata are committed.
//! - A failed `finish()` call means the metadata was NOT committed; the file may
//!   or may not exist (best-effort cleanup is attempted).
//! - The `Drop` implementation for `TableWriteSession` is a no-op: if `finish()`
//!   is never called, no file is uploaded and no metadata is written.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::Result;
use crate::metadata_provider::InlinedDataRow;
use crate::metadata_writer::{
    ColumnDef, ColumnStatInfo, DataFileInfo, MetadataWriter, WriteMode, WriteResult,
};
use crate::path_resolver::join_paths;

/// High-level writer for DuckLake tables.
#[derive(Debug)]
pub struct DuckLakeTableWriter {
    metadata: Arc<dyn MetadataWriter>,
    object_store: Arc<dyn ObjectStore>,
    /// The key path portion of the data_path (e.g., "/prefix/data/")
    base_key_path: String,
}

impl DuckLakeTableWriter {
    pub fn new(
        metadata: Arc<dyn MetadataWriter>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let data_path_str = metadata.get_data_path()?;
        let (_, key_path) = crate::path_resolver::parse_object_store_url(&data_path_str)?;

        Ok(Self {
            metadata,
            object_store,
            base_key_path: key_path,
        })
    }

    /// Begin a streaming write session.
    /// If mode is `WriteMode::Replace`, ends existing files.
    pub fn begin_write(
        &self,
        schema_name: &str,
        table_name: &str,
        arrow_schema: &Schema,
        mode: WriteMode,
    ) -> Result<TableWriteSession> {
        let table_key = join_paths(&join_paths(&self.base_key_path, schema_name)?, table_name)?;
        let file_name = format!("{}.parquet", Uuid::new_v4());
        self.begin_write_internal(
            schema_name,
            table_name,
            arrow_schema,
            table_key,
            file_name.clone(),
            file_name,
            true,
            mode,
        )
    }

    /// Begin a streaming write session for a specific partition using Hive-style paths.
    ///
    /// Creates a file at `<table_key>/<partition_dir>/<uuid>.parquet` where
    /// `partition_dir` is a Hive-style path like `category=A/year=2024`.
    pub fn begin_write_partitioned(
        &self,
        schema_name: &str,
        table_name: &str,
        arrow_schema: &Schema,
        partition_dir: &str,
        mode: WriteMode,
    ) -> Result<TableWriteSession> {
        let table_key = join_paths(&join_paths(&self.base_key_path, schema_name)?, table_name)?;
        let partition_key = join_paths(&table_key, partition_dir)?;
        let file_name = format!("{}.parquet", Uuid::new_v4());
        // Catalog path includes the partition directory
        let catalog_path = join_paths(partition_dir, &file_name)?;
        self.begin_write_internal(
            schema_name,
            table_name,
            arrow_schema,
            partition_key,
            file_name,
            catalog_path,
            true,
            mode,
        )
    }

    /// Begin a streaming write session with a custom file path (registered as absolute).
    pub fn begin_write_to_path(
        &self,
        schema_name: &str,
        table_name: &str,
        arrow_schema: &Schema,
        file_dir: &str,
        file_name: String,
        mode: WriteMode,
    ) -> Result<TableWriteSession> {
        let full_path = join_paths(file_dir, &file_name)?;
        self.begin_write_internal(
            schema_name,
            table_name,
            arrow_schema,
            file_dir.to_string(),
            file_name,
            full_path,
            false,
            mode,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn begin_write_internal(
        &self,
        schema_name: &str,
        table_name: &str,
        arrow_schema: &Schema,
        file_dir: String,
        file_name: String,
        catalog_path: String,
        path_is_relative: bool,
        mode: WriteMode,
    ) -> Result<TableWriteSession> {
        let columns = arrow_schema_to_column_defs(arrow_schema)?;
        let setup =
            self.metadata
                .begin_write_transaction(schema_name, table_name, &columns, mode)?;
        let schema_with_ids = Arc::new(build_schema_with_field_ids(
            arrow_schema,
            &setup.column_ids,
        )?);

        let object_path_str = join_paths(&file_dir, &file_name)?;
        // Strip leading slash for object_store Path (it expects relative keys)
        let object_path = ObjectPath::from(object_path_str.trim_start_matches('/'));

        let props = WriterProperties::builder()
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();
        let writer = ArrowWriter::try_new(Vec::new(), schema_with_ids.clone(), Some(props))?;

        Ok(TableWriteSession {
            metadata: Arc::clone(&self.metadata),
            object_store: Arc::clone(&self.object_store),
            object_path,
            snapshot_id: setup.snapshot_id,
            schema_id: setup.schema_id,
            table_id: setup.table_id,
            column_ids: setup.column_ids,
            schema_with_ids,
            writer: Some(writer),
            catalog_path,
            path_is_relative,
            row_count: 0,
            write_mode: mode,
        })
    }

    /// Write batches to a table, replacing any existing data.
    pub async fn write_table(
        &self,
        schema_name: &str,
        table_name: &str,
        batches: &[RecordBatch],
    ) -> Result<WriteResult> {
        if batches.is_empty() {
            return Err(crate::error::DuckLakeError::InvalidConfig(
                "No batches to write".to_string(),
            ));
        }

        let arrow_schema = batches[0].schema();
        let mut session =
            self.begin_write(schema_name, table_name, &arrow_schema, WriteMode::Replace)?;

        for batch in batches {
            session.write_batch(batch)?;
        }

        session.finish().await
    }

    /// Write batches to a table, appending to existing data.
    pub async fn append_table(
        &self,
        schema_name: &str,
        table_name: &str,
        batches: &[RecordBatch],
    ) -> Result<WriteResult> {
        if batches.is_empty() {
            return Err(crate::error::DuckLakeError::InvalidConfig(
                "No batches to write".to_string(),
            ));
        }

        let arrow_schema = batches[0].schema();
        let mut session =
            self.begin_write(schema_name, table_name, &arrow_schema, WriteMode::Append)?;

        for batch in batches {
            session.write_batch(batch)?;
        }

        session.finish().await
    }

    /// Write data, inlining small inserts into the catalog database when possible.
    ///
    /// If the `data_inlining_row_limit` option is set and the total row count
    /// (existing inlined + new rows) is within the limit, data is stored directly
    /// in the catalog database. Otherwise, any existing inlined data is flushed
    /// to Parquet and the new data is also written to Parquet.
    pub async fn write_or_inline(
        &self,
        schema_name: &str,
        table_name: &str,
        arrow_schema: &Schema,
        batches: &[RecordBatch],
        mode: WriteMode,
    ) -> Result<WriteResult> {
        if batches.is_empty() {
            return Err(crate::error::DuckLakeError::InvalidConfig(
                "No batches to write".to_string(),
            ));
        }

        let total_new_rows: i64 = batches
            .iter()
            .map(|b| {
                i64::try_from(b.num_rows()).map_err(|_| {
                    crate::error::DuckLakeError::Internal(format!(
                        "Batch row count {} exceeds i64 range",
                        b.num_rows()
                    ))
                })
            })
            .try_fold(0i64, |acc, r| r.and_then(|n| Ok(acc.saturating_add(n))))?;

        // Only try inlining for Append mode
        if mode == WriteMode::Append {
            if let Some(limit) = self.metadata.get_data_inlining_row_limit()? {
                if limit > 0 {
                    let columns = arrow_schema_to_column_defs(arrow_schema)?;
                    let setup = self.metadata.begin_write_transaction(
                        schema_name,
                        table_name,
                        &columns,
                        mode,
                    )?;

                    let current_inline = self.metadata.get_inlined_row_count(setup.table_id)?;

                    if current_inline + total_new_rows <= limit {
                        // Inline path: store data directly in catalog
                        let rows = batches_to_inlined_rows(batches, arrow_schema)?;
                        let stored = self.metadata.store_inlined_data(
                            setup.table_id,
                            setup.snapshot_id,
                            &columns,
                            &rows,
                        )?;
                        return Ok(WriteResult {
                            snapshot_id: setup.snapshot_id,
                            table_id: setup.table_id,
                            schema_id: setup.schema_id,
                            files_written: 0,
                            records_written: stored,
                            last_data_file_id: -1,
                        });
                    }

                    // Threshold exceeded: flush existing inline data + write new data to Parquet.
                    // Use the setup we already have (snapshot, table, columns created).
                    let schema_with_ids = Arc::new(build_schema_with_field_ids(
                        arrow_schema,
                        &setup.column_ids,
                    )?);

                    // Collect all data: existing inline + new batches
                    let mut all_batches: Vec<RecordBatch> = Vec::new();

                    if current_inline > 0 {
                        // Get existing inline data and convert to RecordBatch
                        if let Ok(inline_rows) = self.get_inlined_data_as_batch(
                            setup.table_id,
                            setup.snapshot_id,
                            arrow_schema,
                        ) {
                            all_batches.push(inline_rows);
                        }
                        // Clear the inlined data
                        self.metadata
                            .clear_inlined_data(setup.table_id, setup.snapshot_id)?;
                    }

                    all_batches.extend(batches.iter().cloned());

                    // Write combined data to Parquet using the existing setup
                    return self
                        .write_parquet_with_setup(
                            schema_name,
                            &all_batches,
                            &schema_with_ids,
                            setup,
                        )
                        .await;
                }
            }
        }

        // Normal path (no inlining)
        match mode {
            WriteMode::Replace => self.write_table(schema_name, table_name, batches).await,
            WriteMode::Append => self.append_table(schema_name, table_name, batches).await,
        }
    }

    /// Force-flush inlined data for a table to Parquet.
    ///
    /// Reads all inlined rows from the catalog, writes them to a Parquet file,
    /// and clears the inline data from the catalog database.
    ///
    /// Returns `WriteResult` with `records_written = 0` and `files_written = 0`
    /// if the table has no inlined data.
    pub async fn flush_inlined_data(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<WriteResult> {
        // Look up the table to check if it has inlined data
        let table_id = self
            .metadata
            .find_table_id(schema_name, table_name)?
            .ok_or_else(|| {
                crate::error::DuckLakeError::InvalidConfig(format!(
                    "Table {}.{} not found",
                    schema_name, table_name
                ))
            })?;

        let inline_rows = self.metadata.read_inlined_data(table_id)?;
        if inline_rows.is_empty() {
            return Ok(WriteResult {
                snapshot_id: -1,
                table_id,
                schema_id: -1,
                files_written: 0,
                records_written: 0,
                last_data_file_id: -1,
            });
        }

        // Get the table's column schema
        let active_columns = self.metadata.get_active_columns(table_id)?;
        let column_defs: Vec<ColumnDef> = active_columns
            .iter()
            .map(|(name, dtype, nullable)| ColumnDef::new(name, dtype, *nullable))
            .collect::<Result<Vec<_>>>()?;

        let arrow_fields: Vec<Field> = active_columns
            .iter()
            .map(|(name, dtype, nullable)| {
                let arrow_type = crate::types::ducklake_to_arrow_type(dtype)
                    .unwrap_or(arrow::datatypes::DataType::Utf8);
                Field::new(name, arrow_type, *nullable)
            })
            .collect();
        let arrow_schema = Schema::new(arrow_fields);

        // Begin a write transaction with the proper columns
        let setup = self.metadata.begin_write_transaction(
            schema_name,
            table_name,
            &column_defs,
            WriteMode::Append,
        )?;

        // Convert inline rows to RecordBatch
        let batch = inlined_rows_to_batch(&inline_rows, &arrow_schema)?;
        let schema_with_ids = Arc::new(build_schema_with_field_ids(
            &arrow_schema,
            &setup.column_ids,
        )?);

        // Clear the inlined data
        self.metadata
            .clear_inlined_data(setup.table_id, setup.snapshot_id)?;

        // Write to Parquet
        self.write_parquet_with_setup(schema_name, &[batch], &schema_with_ids, setup)
            .await
    }

    /// Get inlined data from the catalog and convert to a RecordBatch.
    fn get_inlined_data_as_batch(
        &self,
        table_id: i64,
        _snapshot_id: i64,
        arrow_schema: &Schema,
    ) -> Result<RecordBatch> {
        let rows = self.metadata.read_inlined_data(table_id)?;
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(arrow_schema.clone())));
        }

        inlined_rows_to_batch(&rows, arrow_schema)
    }

    /// Write batches to Parquet using an already-created write setup.
    async fn write_parquet_with_setup(
        &self,
        schema_name: &str,
        batches: &[RecordBatch],
        schema_with_ids: &SchemaRef,
        setup: crate::metadata_writer::WriteSetupResult,
    ) -> Result<WriteResult> {
        let table_key = join_paths(
            &join_paths(&self.base_key_path, schema_name)?,
            // Use table_id-based path since we don't have table_name separately
            &format!("t{}/", setup.table_id),
        )?;
        let file_name = format!("{}.parquet", Uuid::new_v4());
        let object_path_str = join_paths(&table_key, &file_name)?;
        let object_path = ObjectPath::from(object_path_str.trim_start_matches('/'));

        let props = WriterProperties::builder()
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema_with_ids.clone(), Some(props))?;

        let mut row_count: i64 = 0;
        for batch in batches {
            let batch_with_ids =
                RecordBatch::try_new(schema_with_ids.clone(), batch.columns().to_vec())?;
            writer.write(&batch_with_ids)?;
            row_count += i64::try_from(batch.num_rows()).map_err(|_| {
                crate::error::DuckLakeError::Internal(format!(
                    "Batch row count {} exceeds i64 range",
                    batch.num_rows()
                ))
            })?;
        }

        writer.flush()?;
        let column_stats = extract_column_stats(writer.flushed_row_groups(), &setup.column_ids);
        let buffer = writer.into_inner()?;

        let file_size = i64::try_from(buffer.len()).map_err(|e| {
            crate::error::DuckLakeError::Internal(format!("File size overflow: {}", e))
        })?;
        let footer_size = calculate_footer_size_from_bytes(&buffer)?;

        self.object_store
            .put(&object_path, PutPayload::from(buffer))
            .await?;

        let file_info =
            DataFileInfo::new(&file_name, file_size, row_count).with_footer_size(footer_size);
        let data_file_id =
            self.metadata
                .register_data_file(setup.table_id, setup.snapshot_id, &file_info)?;

        if !column_stats.is_empty() {
            self.metadata
                .register_column_stats(data_file_id, setup.table_id, &column_stats)?;
        }

        Ok(WriteResult {
            snapshot_id: setup.snapshot_id,
            table_id: setup.table_id,
            schema_id: setup.schema_id,
            files_written: 1,
            records_written: row_count,
            last_data_file_id: data_file_id,
        })
    }

    /// Begin a streaming write session for a partition using an existing write setup.
    ///
    /// Reuses the snapshot, table, and column IDs from a previous `begin_write_transaction`
    /// call, avoiding per-partition snapshot/column creation. This is critical for
    /// partitioned writes where all partitions must share the same column IDs.
    pub fn begin_write_partitioned_with_setup(
        &self,
        schema_name: &str,
        table_name: &str,
        arrow_schema: &Schema,
        partition_dir: &str,
        setup: &crate::metadata_writer::WriteSetupResult,
    ) -> Result<TableWriteSession> {
        let table_key = join_paths(&join_paths(&self.base_key_path, schema_name)?, table_name)?;
        let partition_key = join_paths(&table_key, partition_dir)?;
        let file_name = format!("{}.parquet", Uuid::new_v4());
        let catalog_path = join_paths(partition_dir, &file_name)?;

        let schema_with_ids = Arc::new(build_schema_with_field_ids(
            arrow_schema,
            &setup.column_ids,
        )?);

        let object_path_str = join_paths(&partition_key, &file_name)?;
        let object_path = ObjectPath::from(object_path_str.trim_start_matches('/'));

        let props = WriterProperties::builder()
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();
        let writer = ArrowWriter::try_new(Vec::new(), schema_with_ids.clone(), Some(props))?;

        Ok(TableWriteSession {
            metadata: Arc::clone(&self.metadata),
            object_store: Arc::clone(&self.object_store),
            object_path,
            snapshot_id: setup.snapshot_id,
            schema_id: setup.schema_id,
            table_id: setup.table_id,
            column_ids: setup.column_ids.clone(),
            schema_with_ids,
            writer: Some(writer),
            catalog_path,
            path_is_relative: true,
            row_count: 0,
            // Always Append — Replace-mode file ending is handled by the caller
            // after all partition uploads succeed.
            write_mode: WriteMode::Append,
        })
    }

    /// Commit uploaded files and register partition values in the catalog.
    ///
    /// For Replace mode, ends existing data files before registering new ones.
    /// This ensures atomicity: old files are only ended after ALL new files are
    /// uploaded, and partition values are registered alongside their files.
    pub async fn commit_uploaded_files(
        &self,
        setup: &crate::metadata_writer::WriteSetupResult,
        uploaded_files: Vec<(UploadedFile, Vec<Option<String>>)>,
        write_mode: WriteMode,
    ) -> Result<WriteResult> {
        // End existing data files for Replace mode AFTER all uploads succeeded.
        if write_mode == WriteMode::Replace {
            self.metadata
                .end_table_files(setup.table_id, setup.snapshot_id)?;
        }

        let mut total_rows: i64 = 0;
        let mut last_data_file_id: i64 = -1;

        for (upload, partition_values) in &uploaded_files {
            let mut file_info =
                DataFileInfo::new(&upload.catalog_path, upload.file_size, upload.row_count)
                    .with_footer_size(upload.footer_size);
            if !upload.path_is_relative {
                file_info = file_info.with_absolute_path();
            }

            let data_file_id =
                self.metadata
                    .register_data_file(setup.table_id, setup.snapshot_id, &file_info)?;

            if !upload.column_stats.is_empty() {
                self.metadata.register_column_stats(
                    data_file_id,
                    setup.table_id,
                    &upload.column_stats,
                )?;
            }

            // Register partition values for this file
            for (key_index, pval) in partition_values.iter().enumerate() {
                self.metadata.register_file_partition_value(
                    data_file_id,
                    setup.table_id,
                    i32::try_from(key_index).map_err(|e| {
                        crate::error::DuckLakeError::Internal(format!(
                            "Partition key index overflow: {}",
                            e
                        ))
                    })?,
                    pval.as_deref(),
                )?;
            }

            total_rows += upload.row_count;
            last_data_file_id = data_file_id;
        }

        Ok(WriteResult {
            snapshot_id: setup.snapshot_id,
            table_id: setup.table_id,
            schema_id: setup.schema_id,
            files_written: uploaded_files.len(),
            records_written: total_rows,
            last_data_file_id,
        })
    }

    /// Best-effort cleanup of uploaded files that failed to commit.
    pub async fn cleanup_uploaded_files(&self, files: &[UploadedFile]) {
        for upload in files {
            if let Err(e) = self.object_store.delete(&upload.object_path).await {
                tracing::warn!(
                    path = %upload.object_path,
                    error = %e,
                    "Failed to clean up orphaned Parquet file after commit failure"
                );
            }
        }
    }
}

/// Result of uploading a Parquet file (before metadata commit).
///
/// Used by partitioned writes to separate the upload phase from the commit phase.
#[derive(Debug)]
pub struct UploadedFile {
    /// Path to register in catalog
    pub catalog_path: String,
    /// Whether the path is relative to table path
    pub path_is_relative: bool,
    /// Size of the uploaded file in bytes
    pub file_size: i64,
    /// Size of the Parquet footer in bytes
    pub footer_size: i64,
    /// Number of rows written
    pub row_count: i64,
    /// Column-level statistics
    pub column_stats: Vec<ColumnStatInfo>,
    /// Object store path (for cleanup on failure)
    pub object_path: ObjectPath,
}

/// Streaming write session. Buffer is dropped if not finished (no data uploaded).
#[derive(Debug)]
pub struct TableWriteSession {
    metadata: Arc<dyn MetadataWriter>,
    object_store: Arc<dyn ObjectStore>,
    object_path: ObjectPath,
    snapshot_id: i64,
    schema_id: i64,
    table_id: i64,
    column_ids: Vec<i64>,
    schema_with_ids: SchemaRef,
    writer: Option<ArrowWriter<Vec<u8>>>,
    /// Path to register in catalog (may be relative filename or absolute path)
    catalog_path: String,
    /// Whether the catalog_path is relative to table path
    path_is_relative: bool,
    row_count: i64,
    /// Write mode (Append or Replace) - determines whether old files are ended on commit
    write_mode: WriteMode,
}

impl TableWriteSession {
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.writer.is_none() {
            return Err(crate::error::DuckLakeError::Internal(
                "Writer already closed".to_string(),
            ));
        }
        self.validate_batch_schema(batch)?;

        let batch_with_ids =
            RecordBatch::try_new(self.schema_with_ids.clone(), batch.columns().to_vec())?;
        let writer = self.writer.as_mut().ok_or_else(|| {
            crate::error::DuckLakeError::Internal("Writer already closed".to_string())
        })?;
        writer.write(&batch_with_ids)?;
        self.row_count += i64::try_from(batch.num_rows()).map_err(|_| {
            crate::error::DuckLakeError::Internal(format!(
                "batch row count {} exceeds i64::MAX",
                batch.num_rows()
            ))
        })?;
        Ok(())
    }

    fn validate_batch_schema(&self, batch: &RecordBatch) -> Result<()> {
        let batch_schema = batch.schema();
        let expected_schema = &self.schema_with_ids;

        if batch_schema.fields().len() != expected_schema.fields().len() {
            return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                "Schema mismatch: batch has {} columns, expected {}",
                batch_schema.fields().len(),
                expected_schema.fields().len()
            )));
        }

        for (i, (batch_field, expected_field)) in batch_schema
            .fields()
            .iter()
            .zip(expected_schema.fields().iter())
            .enumerate()
        {
            if batch_field.name() != expected_field.name() {
                return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                    "Schema mismatch at column {}: batch has name '{}', expected '{}'",
                    i,
                    batch_field.name(),
                    expected_field.name()
                )));
            }
            if batch_field.data_type() != expected_field.data_type() {
                return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                    "Schema mismatch at column {}: batch has type {:?}, expected {:?}",
                    i,
                    batch_field.data_type(),
                    expected_field.data_type()
                )));
            }
        }
        Ok(())
    }

    pub fn row_count(&self) -> i64 {
        self.row_count
    }

    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// Returns the object path that will be written to
    pub fn file_path(&self) -> &str {
        self.object_path.as_ref()
    }

    /// Upload the Parquet file to the object store without committing metadata.
    ///
    /// Used by partitioned writes to separate the upload phase from the commit phase,
    /// ensuring all partition files are uploaded before any metadata is committed.
    pub async fn upload(mut self) -> Result<UploadedFile> {
        let mut writer = self.writer.take().ok_or_else(|| {
            crate::error::DuckLakeError::Internal("Writer already closed".to_string())
        })?;

        writer.flush()?;
        let column_stats = extract_column_stats(writer.flushed_row_groups(), &self.column_ids);
        let buffer = writer.into_inner()?;

        let file_size = i64::try_from(buffer.len()).map_err(|_| {
            crate::error::DuckLakeError::Internal(format!(
                "file size {} exceeds i64::MAX",
                buffer.len()
            ))
        })?;
        let footer_size = calculate_footer_size_from_bytes(&buffer)?;

        self.object_store
            .put(&self.object_path, PutPayload::from(buffer))
            .await?;

        Ok(UploadedFile {
            catalog_path: self.catalog_path,
            path_is_relative: self.path_is_relative,
            file_size,
            footer_size,
            row_count: self.row_count,
            column_stats,
            object_path: self.object_path,
        })
    }

    /// Upload, then commit metadata (including ending old files for Replace mode).
    ///
    /// For non-partitioned writes, this is the standard finish path. Old files
    /// are ended only AFTER the upload succeeds, preventing data loss on upload failure.
    pub async fn finish(mut self) -> Result<WriteResult> {
        let mut writer = self.writer.take().ok_or_else(|| {
            crate::error::DuckLakeError::Internal("Writer already closed".to_string())
        })?;

        // Flush pending data so flushed_row_groups() has all row groups
        writer.flush()?;
        let column_stats = extract_column_stats(writer.flushed_row_groups(), &self.column_ids);

        let buffer = writer.into_inner()?;

        let file_size = i64::try_from(buffer.len()).map_err(|_| {
            crate::error::DuckLakeError::Internal(format!(
                "file size {} exceeds i64::MAX",
                buffer.len()
            ))
        })?;
        let footer_size = calculate_footer_size_from_bytes(&buffer)?;

        // Upload via object_store
        self.object_store
            .put(&self.object_path, PutPayload::from(buffer))
            .await?;

        // Attempt to commit metadata. If this fails, the uploaded file is orphaned
        // and we make a best-effort attempt to clean it up.
        match self.commit_metadata(file_size, footer_size, &column_stats) {
            Ok(result) => Ok(result),
            Err(commit_err) => {
                // Best-effort cleanup: delete the orphaned Parquet file
                if let Err(cleanup_err) = self.object_store.delete(&self.object_path).await {
                    tracing::warn!(
                        path = %self.object_path,
                        error = %cleanup_err,
                        "Failed to clean up orphaned Parquet file after metadata commit failure"
                    );
                }
                Err(commit_err)
            },
        }
    }

    /// Commit file metadata to the catalog. Separated from `finish()` so that
    /// cleanup of orphaned files can happen if this step fails.
    ///
    /// For Replace mode, ends existing data files before registering the new file.
    /// This ensures old files are only ended after the upload succeeds.
    fn commit_metadata(
        &self,
        file_size: i64,
        footer_size: i64,
        column_stats: &[ColumnStatInfo],
    ) -> Result<WriteResult> {
        // End existing data files for Replace mode AFTER upload succeeded.
        // This ensures the table is never left empty if the upload fails.
        if self.write_mode == WriteMode::Replace {
            self.metadata
                .end_table_files(self.table_id, self.snapshot_id)?;
        }

        let mut file_info = DataFileInfo::new(&self.catalog_path, file_size, self.row_count)
            .with_footer_size(footer_size);
        if !self.path_is_relative {
            file_info = file_info.with_absolute_path();
        }
        let data_file_id =
            self.metadata
                .register_data_file(self.table_id, self.snapshot_id, &file_info)?;

        if !column_stats.is_empty() {
            self.metadata
                .register_column_stats(data_file_id, self.table_id, column_stats)?;
        }

        Ok(WriteResult {
            snapshot_id: self.snapshot_id,
            table_id: self.table_id,
            schema_id: self.schema_id,
            files_written: 1,
            records_written: self.row_count,
            last_data_file_id: data_file_id,
        })
    }
}

// Drop is a no-op: buffer is simply dropped, nothing was uploaded to the store.

/// Convert RecordBatches to InlinedDataRow format for catalog storage.
pub(crate) fn batches_to_inlined_rows(
    batches: &[RecordBatch],
    schema: &Schema,
) -> crate::Result<Vec<InlinedDataRow>> {
    let column_names: Arc<Vec<String>> =
        Arc::new(schema.fields().iter().map(|f| f.name().clone()).collect());
    let mut rows = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    values.push(None);
                } else {
                    values.push(Some(arrow_array_value_to_string(col.as_ref(), row_idx)?));
                }
            }
            rows.push(InlinedDataRow {
                column_names: Arc::clone(&column_names),
                values,
            });
        }
    }

    Ok(rows)
}

/// Convert an Arrow array value at a given index to a string representation.
///
/// This produces strings compatible with the `parse_inlined_column` function
/// in `table.rs` for round-tripping through the catalog database.
fn arrow_array_value_to_string(
    array: &dyn arrow::array::Array,
    idx: usize,
) -> crate::Result<String> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    macro_rules! downcast_value {
        ($array_type:ty) => {{
            let a = array.as_any().downcast_ref::<$array_type>().ok_or_else(|| {
                crate::error::DuckLakeError::Internal(format!(
                    "Failed to downcast {:?} array",
                    array.data_type()
                ))
            })?;
            Ok(a.value(idx).to_string())
        }};
    }

    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                crate::error::DuckLakeError::Internal(
                    "Failed to downcast Boolean array".to_string(),
                )
            })?;
            Ok(if a.value(idx) { "true" } else { "false" }.to_string())
        },
        DataType::Int8 => downcast_value!(Int8Array),
        DataType::Int16 => downcast_value!(Int16Array),
        DataType::Int32 => downcast_value!(Int32Array),
        DataType::Int64 => downcast_value!(Int64Array),
        DataType::UInt8 => downcast_value!(UInt8Array),
        DataType::UInt16 => downcast_value!(UInt16Array),
        DataType::UInt32 => downcast_value!(UInt32Array),
        DataType::UInt64 => downcast_value!(UInt64Array),
        DataType::Float32 => downcast_value!(Float32Array),
        DataType::Float64 => downcast_value!(Float64Array),
        DataType::Utf8 => downcast_value!(StringArray),
        DataType::LargeUtf8 => downcast_value!(LargeStringArray),
        DataType::Date32 => downcast_value!(Date32Array),
        DataType::Date64 => downcast_value!(Date64Array),
        DataType::Timestamp(unit, _) => {
            use arrow::datatypes::TimeUnit;
            match unit {
                TimeUnit::Second => downcast_value!(TimestampSecondArray),
                TimeUnit::Millisecond => downcast_value!(TimestampMillisecondArray),
                TimeUnit::Microsecond => downcast_value!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => downcast_value!(TimestampNanosecondArray),
            }
        },
        _ => {
            // Fallback: use Arrow's default display
            let formatter = arrow::util::display::ArrayFormatter::try_new(
                array,
                &arrow::util::display::FormatOptions::default(),
            );
            match formatter {
                Ok(f) => Ok(f.value(idx).to_string()),
                Err(_) => Ok(String::new()),
            }
        },
    }
}

/// Convert InlinedDataRow values back to a RecordBatch.
///
/// Used when flushing inlined data to Parquet. Re-uses the same parsing
/// logic as the read-side `parse_inlined_column` in `table.rs`.
pub(crate) fn inlined_rows_to_batch(
    rows: &[InlinedDataRow],
    schema: &Schema,
) -> Result<RecordBatch> {
    let num_rows = rows.len();
    let mut column_arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

    for field in schema.fields().iter() {
        let col_name = field.name();
        let data_type = field.data_type();

        // Collect values for this column from all rows
        let mut string_values: Vec<Option<String>> = Vec::with_capacity(num_rows);
        for row in rows {
            let value = row
                .column_names
                .iter()
                .position(|n| n == col_name)
                .and_then(|pos| row.values.get(pos))
                .and_then(|v| v.clone());
            string_values.push(value);
        }

        // Parse string values into the appropriate Arrow array type
        let array = parse_string_to_array(&string_values, data_type)?;
        column_arrays.push(array);
    }

    Ok(RecordBatch::try_new(
        Arc::new(schema.clone()),
        column_arrays,
    )?)
}

/// Parse string values into an Arrow array of the given data type.
fn parse_string_to_array(
    values: &[Option<String>],
    data_type: &arrow::datatypes::DataType,
) -> Result<Arc<dyn arrow::array::Array>> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    macro_rules! parse_primitive {
        ($builder_ty:ty, $values:expr) => {{
            let mut builder = <$builder_ty>::with_capacity($values.len());
            for val in $values {
                match val {
                    Some(s) => match s.parse() {
                        Ok(v) => builder.append_value(v),
                        Err(_) => {
                            return Err(crate::error::DuckLakeError::Internal(format!(
                                "Failed to parse inlined value '{}' as {}",
                                s,
                                std::any::type_name::<$builder_ty>()
                            )));
                        },
                    },
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as Arc<dyn Array>
        }};
    }

    let array: Arc<dyn arrow::array::Array> = match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for val in values {
                match val {
                    Some(s) => match s.to_lowercase().as_str() {
                        "true" | "1" | "t" => builder.append_value(true),
                        "false" | "0" | "f" => builder.append_value(false),
                        _ => {
                            return Err(crate::error::DuckLakeError::Internal(format!(
                                "Failed to parse inlined value '{}' as Boolean",
                                s
                            )));
                        },
                    },
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        },
        DataType::Int8 => parse_primitive!(Int8Builder, values),
        DataType::Int16 => parse_primitive!(Int16Builder, values),
        DataType::Int32 => parse_primitive!(Int32Builder, values),
        DataType::Int64 => parse_primitive!(Int64Builder, values),
        DataType::UInt8 => parse_primitive!(UInt8Builder, values),
        DataType::UInt16 => parse_primitive!(UInt16Builder, values),
        DataType::UInt32 => parse_primitive!(UInt32Builder, values),
        DataType::UInt64 => parse_primitive!(UInt64Builder, values),
        DataType::Float32 => parse_primitive!(Float32Builder, values),
        DataType::Float64 => parse_primitive!(Float64Builder, values),
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for val in values {
                match val {
                    Some(s) => builder.append_value(s),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        },
        DataType::LargeUtf8 => {
            let mut builder = LargeStringBuilder::new();
            for val in values {
                match val {
                    Some(s) => builder.append_value(s),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        },
        DataType::Date32 => parse_primitive!(Date32Builder, values),
        DataType::Date64 => parse_primitive!(Date64Builder, values),
        DataType::Timestamp(unit, tz) => {
            use arrow::datatypes::TimeUnit;

            macro_rules! build_timestamp {
                ($builder_ty:ty) => {{
                    let mut builder = <$builder_ty>::with_capacity(values.len());
                    for val in values {
                        match val {
                            Some(s) => match s.parse::<i64>() {
                                Ok(v) => builder.append_value(v),
                                Err(_) => {
                                    return Err(crate::error::DuckLakeError::Internal(format!(
                                        "Failed to parse inlined value '{}' as Timestamp",
                                        s
                                    )));
                                },
                            },
                            None => builder.append_null(),
                        }
                    }
                    let arr = builder.finish();
                    match tz {
                        Some(tz) => Arc::new(arr.with_timezone(tz.as_ref())) as Arc<dyn Array>,
                        None => Arc::new(arr) as Arc<dyn Array>,
                    }
                }};
            }

            match unit {
                TimeUnit::Second => build_timestamp!(TimestampSecondBuilder),
                TimeUnit::Millisecond => build_timestamp!(TimestampMillisecondBuilder),
                TimeUnit::Microsecond => build_timestamp!(TimestampMicrosecondBuilder),
                TimeUnit::Nanosecond => build_timestamp!(TimestampNanosecondBuilder),
            }
        },
        _ => {
            // Fallback: store as strings
            let mut builder = StringBuilder::new();
            for val in values {
                match val {
                    Some(s) => builder.append_value(s),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        },
    };

    Ok(array)
}

pub(crate) fn arrow_schema_to_column_defs(schema: &Schema) -> Result<Vec<ColumnDef>> {
    schema
        .fields()
        .iter()
        .map(|field| ColumnDef::from_arrow(field.name(), field.data_type(), field.is_nullable()))
        .collect()
}

pub(crate) fn build_schema_with_field_ids(schema: &Schema, column_ids: &[i64]) -> Result<Schema> {
    if schema.fields().len() != column_ids.len() {
        return Err(crate::error::DuckLakeError::Internal(format!(
            "Schema field count ({}) does not match column ID count ({})",
            schema.fields().len(),
            column_ids.len()
        )));
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(column_ids.iter())
        .map(|(field, &col_id)| {
            let mut metadata: HashMap<String, String> = field.metadata().clone();
            metadata.insert("PARQUET:field_id".to_string(), col_id.to_string());
            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Extract column-level statistics from flushed Parquet row groups.
///
/// Merges statistics across multiple row groups by summing null counts
/// and taking the overall min/max across all groups.
fn extract_column_stats(
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
    column_ids: &[i64],
) -> Vec<ColumnStatInfo> {
    if row_groups.is_empty() {
        return Vec::new();
    }

    let num_columns = column_ids.len();

    // Accumulators: (null_count, min_string, max_string)
    let mut null_counts: Vec<i64> = vec![0; num_columns];
    let mut min_values: Vec<Option<String>> = vec![None; num_columns];
    let mut max_values: Vec<Option<String>> = vec![None; num_columns];
    let mut has_stats: Vec<bool> = vec![false; num_columns];

    for rg in row_groups {
        for (col_idx, col_chunk) in rg.columns().iter().enumerate() {
            if col_idx >= num_columns {
                break;
            }

            if let Some(stats) = col_chunk.statistics() {
                has_stats[col_idx] = true;

                if let Some(nc) = stats.null_count_opt() {
                    let nc_i64 = i64::try_from(nc).unwrap_or(i64::MAX);
                    null_counts[col_idx] = null_counts[col_idx].saturating_add(nc_i64);
                }

                let (batch_min, batch_max) = parquet_stats_min_max(stats);

                if let Some(ref bm) = batch_min {
                    match &min_values[col_idx] {
                        None => min_values[col_idx] = Some(bm.clone()),
                        Some(current) => {
                            if should_replace_min(stats, bm, current) {
                                min_values[col_idx] = Some(bm.clone());
                            }
                        },
                    }
                }

                if let Some(ref bm) = batch_max {
                    match &max_values[col_idx] {
                        None => max_values[col_idx] = Some(bm.clone()),
                        Some(current) => {
                            if should_replace_max(stats, bm, current) {
                                max_values[col_idx] = Some(bm.clone());
                            }
                        },
                    }
                }
            }
        }
    }

    (0..num_columns)
        .filter(|&i| has_stats[i])
        .map(|i| ColumnStatInfo {
            column_id: column_ids[i],
            null_count: Some(null_counts[i]),
            min_value: min_values[i].clone(),
            max_value: max_values[i].clone(),
        })
        .collect()
}

/// Extract min/max values from Parquet statistics as strings.
fn parquet_stats_min_max(
    stats: &parquet::file::statistics::Statistics,
) -> (Option<String>, Option<String>) {
    use parquet::file::statistics::Statistics;
    match stats {
        Statistics::Boolean(vs) => (
            vs.min_opt().map(|v| v.to_string()),
            vs.max_opt().map(|v| v.to_string()),
        ),
        Statistics::Int32(vs) => (
            vs.min_opt().map(|v| v.to_string()),
            vs.max_opt().map(|v| v.to_string()),
        ),
        Statistics::Int64(vs) => (
            vs.min_opt().map(|v| v.to_string()),
            vs.max_opt().map(|v| v.to_string()),
        ),
        Statistics::Float(vs) => (
            vs.min_opt().filter(|v| !v.is_nan()).map(|v| v.to_string()),
            vs.max_opt().filter(|v| !v.is_nan()).map(|v| v.to_string()),
        ),
        Statistics::Double(vs) => (
            vs.min_opt().filter(|v| !v.is_nan()).map(|v| v.to_string()),
            vs.max_opt().filter(|v| !v.is_nan()).map(|v| v.to_string()),
        ),
        Statistics::ByteArray(vs) => (
            vs.min_opt()
                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
            vs.max_opt()
                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
        ),
        Statistics::FixedLenByteArray(vs) => (
            vs.min_opt()
                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
            vs.max_opt()
                .and_then(|v| String::from_utf8(v.data().to_vec()).ok()),
        ),
        Statistics::Int96(_) => (None, None),
    }
}

/// Decide whether a new min value should replace the current one.
///
/// For numeric types, compares as numbers; for strings, compares lexicographically.
fn should_replace_min(
    stats: &parquet::file::statistics::Statistics,
    new_val: &str,
    current: &str,
) -> bool {
    use parquet::file::statistics::Statistics;
    match stats {
        Statistics::Int32(_) => new_val.parse::<i32>().ok() < current.parse::<i32>().ok(),
        Statistics::Int64(_) => new_val.parse::<i64>().ok() < current.parse::<i64>().ok(),
        Statistics::Float(_) => {
            match (new_val.parse::<f32>().ok(), current.parse::<f32>().ok()) {
                (Some(n), Some(c)) if !n.is_nan() && !c.is_nan() => n.total_cmp(&c).is_lt(),
                (Some(n), Some(_)) if !n.is_nan() => true, // new is non-NaN, current is NaN → replace
                _ => false,
            }
        },
        Statistics::Double(_) => {
            match (new_val.parse::<f64>().ok(), current.parse::<f64>().ok()) {
                (Some(n), Some(c)) if !n.is_nan() && !c.is_nan() => n.total_cmp(&c).is_lt(),
                (Some(n), Some(_)) if !n.is_nan() => true, // new is non-NaN, current is NaN → replace
                _ => false,
            }
        },
        _ => new_val < current,
    }
}

/// Decide whether a new max value should replace the current one.
fn should_replace_max(
    stats: &parquet::file::statistics::Statistics,
    new_val: &str,
    current: &str,
) -> bool {
    use parquet::file::statistics::Statistics;
    match stats {
        Statistics::Int32(_) => new_val.parse::<i32>().ok() > current.parse::<i32>().ok(),
        Statistics::Int64(_) => new_val.parse::<i64>().ok() > current.parse::<i64>().ok(),
        Statistics::Float(_) => {
            match (new_val.parse::<f32>().ok(), current.parse::<f32>().ok()) {
                (Some(n), Some(c)) if !n.is_nan() && !c.is_nan() => n.total_cmp(&c).is_gt(),
                (Some(n), Some(_)) if !n.is_nan() => true, // new is non-NaN, current is NaN → replace
                _ => false,
            }
        },
        Statistics::Double(_) => {
            match (new_val.parse::<f64>().ok(), current.parse::<f64>().ok()) {
                (Some(n), Some(c)) if !n.is_nan() && !c.is_nan() => n.total_cmp(&c).is_gt(),
                (Some(n), Some(_)) if !n.is_nan() => true, // new is non-NaN, current is NaN → replace
                _ => false,
            }
        },
        _ => new_val > current,
    }
}

pub(crate) fn calculate_footer_size_from_bytes(buffer: &[u8]) -> Result<i64> {
    if buffer.len() < 8 {
        return Err(crate::error::DuckLakeError::Internal(
            "Invalid Parquet file: too small".to_string(),
        ));
    }

    let footer_bytes = &buffer[buffer.len() - 8..];

    if &footer_bytes[4..8] != b"PAR1" {
        return Err(crate::error::DuckLakeError::Internal(
            "Invalid Parquet file: missing PAR1 magic".to_string(),
        ));
    }

    let metadata_len =
        i32::from_le_bytes([footer_bytes[0], footer_bytes[1], footer_bytes[2], footer_bytes[3]]);
    if metadata_len < 0 {
        return Err(crate::error::DuckLakeError::Internal(format!(
            "Invalid Parquet file: negative metadata length {}",
            metadata_len
        )));
    }
    Ok(i64::from(metadata_len))
}

/// Best-effort cleanup of uploaded files after a metadata commit failure.
///
/// Attempts to delete each file from the object store. If any deletion fails,
/// a warning is logged but the error is not propagated — the caller should
/// propagate the original commit error instead.
pub async fn cleanup_orphaned_files(object_store: &dyn ObjectStore, paths: &[ObjectPath]) {
    for path in paths {
        if let Err(e) = object_store.delete(path).await {
            tracing::warn!(
                path = %path,
                error = %e,
                "Failed to clean up orphaned file after metadata commit failure"
            );
        }
    }
}

// ==================== ducklake_flush_inlined_data table function ====================

use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{Result as DataFusionResult, ScalarValue, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;

/// Table function to force-flush inlined data to Parquet files.
///
/// Usage:
///   `SELECT * FROM ducklake_flush_inlined_data('schema_name', 'table_name')`
///
/// Returns: `(rows_flushed: Int64, files_written: Int64)`
#[derive(Debug)]
pub struct DucklakeFlushInlinedDataFunction {
    table_writer: Arc<DuckLakeTableWriter>,
}

impl DucklakeFlushInlinedDataFunction {
    pub fn new(table_writer: Arc<DuckLakeTableWriter>) -> Self {
        Self {
            table_writer,
        }
    }
}

impl TableFunctionImpl for DucklakeFlushInlinedDataFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (schema_name, table_name) = match exprs.len() {
            1 => {
                let table_name =
                    extract_string_literal(&exprs[0], "ducklake_flush_inlined_data", 1)?;
                ("main".to_string(), table_name)
            },
            2 => {
                let schema_name =
                    extract_string_literal(&exprs[0], "ducklake_flush_inlined_data", 1)?;
                let table_name =
                    extract_string_literal(&exprs[1], "ducklake_flush_inlined_data", 2)?;
                (schema_name, table_name)
            },
            _ => {
                return plan_err!(
                    "ducklake_flush_inlined_data() requires 1-2 arguments: (table_name) or (schema_name, table_name)"
                );
            },
        };

        // Use block_on to bridge async object store operations
        let result = crate::metadata_provider::block_on(
            self.table_writer
                .flush_inlined_data(&schema_name, &table_name),
        )
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Build result table
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("rows_flushed", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("files_written", arrow::datatypes::DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![result.records_written])),
                Arc::new(arrow::array::Int64Array::from(vec![
                    i64::try_from(result.files_written).unwrap_or(i64::MAX),
                ])),
            ],
        )
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        let mem = datafusion::datasource::memory::MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Arc::new(mem))
    }
}

fn extract_string_literal(expr: &Expr, func_name: &str, pos: usize) -> DataFusionResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
        _ => plan_err!(
            "Argument {} to {}() must be a string literal",
            pos,
            func_name
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::DataType;

    #[test]
    fn test_arrow_schema_to_column_defs() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let columns = arrow_schema_to_column_defs(&schema).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].ducklake_type, "int32");
        assert!(!columns[0].is_nullable);
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].ducklake_type, "varchar");
        assert!(columns[1].is_nullable);
    }

    #[test]
    fn test_build_schema_with_field_ids() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let column_ids = vec![1, 2];
        let schema_with_ids = build_schema_with_field_ids(&schema, &column_ids).unwrap();

        // Check that field_ids are embedded in metadata
        let field0_metadata = schema_with_ids.field(0).metadata();
        assert_eq!(
            field0_metadata.get("PARQUET:field_id"),
            Some(&"1".to_string())
        );

        let field1_metadata = schema_with_ids.field(1).metadata();
        assert_eq!(
            field1_metadata.get("PARQUET:field_id"),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn test_write_parquet_to_buffer_with_field_ids() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let column_ids = vec![10, 20];
        let schema_with_ids = Arc::new(build_schema_with_field_ids(&schema, &column_ids).unwrap());

        let props = WriterProperties::builder()
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();
        let mut writer =
            ArrowWriter::try_new(Vec::new(), schema_with_ids.clone(), Some(props)).unwrap();

        let batch_with_ids =
            RecordBatch::try_new(schema_with_ids, batch.columns().to_vec()).unwrap();
        writer.write(&batch_with_ids).unwrap();
        let buffer = writer.into_inner().unwrap();

        let file_size = buffer.len() as i64;
        let footer_size = calculate_footer_size_from_bytes(&buffer).unwrap();

        assert!(file_size > 0);
        assert!(footer_size > 0);
        assert!(footer_size < file_size);
    }

    #[test]
    fn test_calculate_footer_size_from_bytes() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        let props = WriterProperties::builder()
            .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
            .build();
        let schema_with_ids = Arc::new(build_schema_with_field_ids(&batch.schema(), &[1]).unwrap());
        let mut writer =
            ArrowWriter::try_new(Vec::new(), schema_with_ids.clone(), Some(props)).unwrap();

        let batch_with_ids =
            RecordBatch::try_new(schema_with_ids, batch.columns().to_vec()).unwrap();
        writer.write(&batch_with_ids).unwrap();
        let buffer = writer.into_inner().unwrap();

        let footer_size = calculate_footer_size_from_bytes(&buffer).unwrap();

        // Footer should be the raw Thrift metadata size (without PAR1 magic + length field)
        assert!(footer_size > 0);
        assert!(footer_size < 10000);
    }

    #[test]
    fn test_arrow_array_value_to_string_date32_epoch_days() {
        use arrow::array::Date32Array;
        // 2024-06-15 is 19889 days since epoch — serialized as epoch-days integer
        let array = Date32Array::from(vec![19889]);
        assert_eq!(arrow_array_value_to_string(&array, 0).unwrap(), "19889");
    }

    #[test]
    fn test_arrow_array_value_to_string_date64_epoch_ms() {
        use arrow::array::Date64Array;
        let ms: i64 = 19889 * 86400 * 1000;
        let array = Date64Array::from(vec![ms]);
        assert_eq!(
            arrow_array_value_to_string(&array, 0).unwrap(),
            ms.to_string()
        );
    }

    #[test]
    fn test_arrow_array_value_to_string_epoch_date32_zero() {
        use arrow::array::Date32Array;
        let array = Date32Array::from(vec![0]);
        assert_eq!(arrow_array_value_to_string(&array, 0).unwrap(), "0");
    }

    #[test]
    fn test_build_schema_with_field_ids_mismatch() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        // Fewer IDs than fields
        let result = build_schema_with_field_ids(&schema, &[1]);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("does not match"));

        // More IDs than fields
        let result = build_schema_with_field_ids(&schema, &[1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_string_to_array_error_on_invalid() {
        let values = vec![Some("not_a_number".to_string())];
        let result = parse_string_to_array(&values, &DataType::Int32);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to parse"));
    }

    #[test]
    fn test_parse_string_to_array_bool_error_on_invalid() {
        let values = vec![Some("maybe".to_string())];
        let result = parse_string_to_array(&values, &DataType::Boolean);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to parse"));
    }

    #[test]
    fn test_date32_inlined_roundtrip() {
        use arrow::array::Date32Array;
        // Write: Date32 value 19889 (2024-06-15) -> string "19889"
        let array = Date32Array::from(vec![19889]);
        let serialized = arrow_array_value_to_string(&array, 0).unwrap();
        assert_eq!(serialized, "19889");

        // Read: string "19889" -> Date32 value 19889
        let values = vec![Some(serialized)];
        let result = parse_string_to_array(&values, &DataType::Date32).unwrap();
        let date_array = result.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(date_array.value(0), 19889);
    }

    #[test]
    fn test_timestamp_inlined_roundtrip() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;
        // Write: Timestamp microseconds value -> string of epoch value
        let epoch_us: i64 = 1_718_451_000_000_000; // ~2024-06-15T12:30:00
        let array = TimestampMicrosecondArray::from(vec![epoch_us]);
        let serialized = arrow_array_value_to_string(&array, 0).unwrap();
        assert_eq!(serialized, epoch_us.to_string());

        // Read: string -> Timestamp microseconds
        let values = vec![Some(serialized)];
        let result = parse_string_to_array(
            &values,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        let ts_array = result
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_array.value(0), epoch_us);
    }

    #[test]
    fn test_timestamp_with_tz_inlined_roundtrip() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;
        let epoch_us: i64 = 1_718_451_000_000_000;
        let values = vec![Some(epoch_us.to_string())];
        let result = parse_string_to_array(
            &values,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        )
        .unwrap();
        let ts_array = result
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_array.value(0), epoch_us);
        // Verify timezone is set
        assert_eq!(
            result.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_nan_skipped_in_stats_min_max() {
        use parquet::file::statistics::Statistics;
        // NaN as current should be replaced by non-NaN new value
        assert!(should_replace_min(
            &Statistics::Float(parquet::file::statistics::ValueStatistics::new(
                Some(0.0f32),
                Some(0.0f32),
                None,
                None,
                false,
            )),
            "1.0",
            "NaN",
        ));
        assert!(should_replace_max(
            &Statistics::Double(parquet::file::statistics::ValueStatistics::new(
                Some(0.0f64),
                Some(0.0f64),
                None,
                None,
                false,
            )),
            "1.0",
            "NaN",
        ));
        // NaN as new value should not replace current
        assert!(!should_replace_min(
            &Statistics::Float(parquet::file::statistics::ValueStatistics::new(
                Some(0.0f32),
                Some(0.0f32),
                None,
                None,
                false,
            )),
            "NaN",
            "1.0",
        ));
    }
}
