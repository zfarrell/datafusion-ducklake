//! [`DuckLakeTable::backfill`]: an extension's write hook run over files that
//! were written without it.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::StreamExt;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};

use crate::extension::{
    DuckLakeExtension, DuckLakeExtensions, FileHooks, StoredFile, WriteContext, WriteLookups,
    WriteOrigin,
};
use crate::metadata_provider::DuckLakeTableFile;
use crate::table::DuckLakeTable;
use crate::{DuckLakeError, Result};

/// Options for [`DuckLakeTable::backfill`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct BackfillOptions {
    /// Only these data files; `None` = every live file.
    pub data_file_ids: Option<Vec<i64>>,
    /// Read at most this many files (bounds one pass); `None` = all.
    pub max_files: Option<usize>,
    /// Files read at once; 0 is taken as 1.
    pub parallelism: usize,
}

impl BackfillOptions {
    /// Read only these data files.
    #[must_use]
    pub fn with_data_file_ids(mut self, data_file_ids: Vec<i64>) -> Self {
        self.data_file_ids = Some(data_file_ids);
        self
    }

    /// Read at most `max_files` files.
    #[must_use]
    pub fn with_max_files(mut self, max_files: usize) -> Self {
        self.max_files = Some(max_files);
        self
    }

    /// Read `parallelism` files at once.
    #[must_use]
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }
}

/// What [`DuckLakeTable::backfill`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BackfillResult {
    /// Files read and observed to the end.
    pub files_read: u64,
    /// Files not read: no hook wanted them, or they are name-mapped,
    /// encrypted, or lack every column the hooks need.
    pub files_skipped: u64,
    /// Files whose read failed, whose values did not cast to the catalog's
    /// types, or whose observers reported an error (warned; a later pass
    /// retries them).
    pub files_failed: u64,
    /// Compressed bytes of the column chunks read.
    pub bytes_read: u64,
}

enum Outcome {
    Read {
        bytes_read: u64,
        failed: bool,
    },
    Skipped,
}

impl DuckLakeTable {
    unstable_extension_api! {
    /// Run `extension`'s write hook over live data files as if they were being
    /// written now ([`WriteOrigin::Backfill`]): files written before the
    /// extension was on, by another DuckLake client, or registered as is. No
    /// file is rewritten and no snapshot is taken.
    ///
    /// Per file, only the columns the hooks ask for
    /// ([`FileWriteHook::backfill_columns`](crate::extension::FileWriteHook::backfill_columns))
    /// are read, located by field id (by name in a file without field ids),
    /// cast to the catalog's types and fed to the observers in file order,
    /// as the reader sees them. A column the file lacks, or whose type does
    /// not cast to the catalog's, is left out. Each observer keeps what it
    /// learns as its file finishes, so an interrupted pass keeps its progress.
    pub async fn backfill(
        &self,
        state: &dyn Session,
        extension: Arc<dyn DuckLakeExtension>,
        opts: BackfillOptions,
    ) -> Result<BackfillResult> {
        let mut result = BackfillResult::default();
        let writer = self.writer().ok_or_else(|| {
            DuckLakeError::InvalidConfig(format!(
                "{}: table is read-only; open the catalog with a writer",
                extension.name()
            ))
        })?;
        let schema_name = self.schema_name().unwrap_or_default().to_string();
        let object_store = state
            .runtime_env()
            .object_store(self.object_store_url().as_ref())?;
        let lookups = WriteLookups::new(self.write_options().ext_settings());
        let Some(hooks) = FileHooks::begin(
            &DuckLakeExtensions::default().with(extension),
            &WriteContext::new(
                self.table_id(),
                &schema_name,
                self.table_name(),
                WriteOrigin::Backfill,
                writer,
                &object_store,
                &lookups,
            ),
        ) else {
            return Ok(result);
        };

        let selected = opts
            .data_file_ids
            .map(|ids| ids.into_iter().collect::<HashSet<_>>());
        let mut work = Vec::new();
        for tf in self.data_files()? {
            if selected
                .as_ref()
                .is_some_and(|ids| !ids.contains(&tf.data_file_id))
            {
                continue;
            }
            let (wanted, wants) = hooks.backfill_plan(&tf);
            if !wants.iter().any(|w| *w)
                || wanted.as_ref().is_some_and(Vec::is_empty)
                || tf.file.mapping_id.is_some()
                || tf
                    .file
                    .encryption_key
                    .as_deref()
                    .is_some_and(|k| !k.is_empty())
            {
                result.files_skipped += 1;
                continue;
            }
            work.push((tf, wanted, wants));
        }
        if let Some(max) = opts.max_files {
            // Left for a later pass; not counted as skipped.
            work.truncate(max);
        }

        let readers = DefaultParquetFileReaderFactory::new(Arc::clone(&object_store));
        let reader_metrics = ExecutionPlanMetricsSet::new();
        let physical_schema = self.physical_schema();
        let column_ids = self.top_level_column_ids();
        let read_one = |tf: DuckLakeTableFile, wanted: Option<Vec<String>>, wants: Vec<bool>| {
            let readers = &readers;
            let reader_metrics = &reader_metrics;
            let hooks = &hooks;
            let physical_schema = &physical_schema;
            let column_ids = &column_ids;
            async move {
                self.observe_file(
                    hooks,
                    readers,
                    reader_metrics,
                    physical_schema,
                    column_ids,
                    &tf,
                    wanted,
                    &wants,
                )
                .await
            }
        };
        let mut outcomes = futures::stream::iter(work.into_iter().map(|(tf, wanted, wants)| {
            let path = tf.file.path.clone();
            let outcome = read_one(tf, wanted, wants);
            async move { (path, outcome.await) }
        }))
        .buffer_unordered(opts.parallelism.max(1));
        while let Some((path, outcome)) = outcomes.next().await {
            match outcome {
                Ok(Outcome::Read {
                    bytes_read,
                    failed,
                }) => {
                    result.bytes_read += bytes_read;
                    if failed {
                        result.files_failed += 1;
                    } else {
                        result.files_read += 1;
                    }
                },
                Ok(Outcome::Skipped) => result.files_skipped += 1,
                Err(error) => {
                    tracing::warn!(%error, path, "backfill: file not read");
                    result.files_failed += 1;
                },
            }
        }
        Ok(result)
    }
    }

    /// Read `tf`'s `wanted` columns (`None` = all) and feed them to the
    /// observers of `hooks` that want the file.
    #[allow(clippy::too_many_arguments)]
    async fn observe_file(
        &self,
        hooks: &FileHooks,
        readers: &DefaultParquetFileReaderFactory,
        reader_metrics: &ExecutionPlanMetricsSet,
        physical_schema: &SchemaRef,
        column_ids: &[i64],
        tf: &DuckLakeTableFile,
        wanted: Option<Vec<String>>,
        wants: &[bool],
    ) -> Result<Outcome> {
        let location = self.data_file_location(&tf.file)?;
        let size = u64::try_from(tf.file.file_size_bytes).map_err(|_| {
            DuckLakeError::Internal(format!("negative file size for {}", tf.file.path))
        })?;
        let open = || {
            readers.create_reader(
                0,
                PartitionedFile::new(location.to_string(), size),
                crate::table::metadata_size_hint(tf.file.footer_size),
                reader_metrics,
            )
        };
        let footer = open()?.get_metadata(None).await?;
        let reader_metadata =
            ArrowReaderMetadata::try_new(Arc::clone(&footer), Default::default())?;
        let file_schema = reader_metadata.schema().clone();
        let field_id = |field: &Field| {
            field
                .metadata()
                .get("PARQUET:field_id")
                .and_then(|id| id.parse::<i64>().ok())
        };
        // By field id; a file with no field ids at all (an external or
        // pre-DuckLake upload) by name, as the reader resolves it.
        let by_name = file_schema.fields().iter().all(|f| field_id(f).is_none());
        // (index in the physical schema, root in the file), in table order.
        let mut columns: Vec<(usize, usize)> = Vec::new();
        for (index, field) in physical_schema.fields().iter().enumerate() {
            if wanted
                .as_ref()
                .is_some_and(|wanted| !wanted.iter().any(|name| name == field.name()))
            {
                continue;
            }
            let root = if by_name {
                file_schema.index_of(field.name()).ok()
            } else {
                file_schema
                    .fields()
                    .iter()
                    .position(|f| field_id(f) == Some(column_ids[index]))
            };
            // A type the cast cannot take fails every batch: left out, as the
            // column of a file that lacks it.
            if let Some(root) = root
                && arrow::compute::can_cast_types(
                    file_schema.field(root).data_type(),
                    field.data_type(),
                )
            {
                columns.push((index, root));
            }
        }
        if columns.is_empty() {
            return Ok(Outcome::Skipped);
        }
        // The batch the observers see: catalog names, types and field ids.
        let observed: SchemaRef = Arc::new(Schema::new(
            columns
                .iter()
                .map(|&(index, _)| {
                    let field = physical_schema.field(index);
                    Field::new(field.name(), field.data_type().clone(), true).with_metadata(
                        [(
                            "PARQUET:field_id".to_string(),
                            column_ids[index].to_string(),
                        )]
                        .into(),
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let mut observers = hooks.open_file_with(&observed, Some(wants));
        if observers.is_empty() {
            return Ok(Outcome::Skipped);
        }

        let mut roots: Vec<usize> = columns.iter().map(|&(_, root)| root).collect();
        roots.sort_unstable();
        roots.dedup();
        let descr = footer.file_metadata().schema_descr();
        let bytes_read = footer
            .row_groups()
            .iter()
            .flat_map(|rg| rg.columns().iter().enumerate())
            .filter(|(leaf, _)| roots.contains(&descr.get_column_root_idx(*leaf)))
            .map(|(_, column)| column.compressed_size().max(0) as u64)
            .sum::<u64>();
        let row_group_rows: Vec<u64> = footer
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows().max(0) as u64)
            .collect();
        let mask = ProjectionMask::roots(descr, roots.clone());
        let reader: Box<dyn AsyncFileReader> = open()?;
        let mut stream =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, reader_metadata)
                .with_projection(mask)
                .build()?;
        // The projected batch holds the roots in ascending order.
        let position = |root: usize| roots.binary_search(&root).expect("projected");
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let cast: std::result::Result<Vec<_>, _> = columns
                .iter()
                .zip(observed.fields())
                .map(|(&(_, root), field)| {
                    arrow::compute::cast_with_options(
                        batch.column(position(root)),
                        field.data_type(),
                        &datafusion::common::format::DEFAULT_CAST_OPTIONS,
                    )
                })
                .collect();
            // A value the cast refuses fails the reader too: not observed.
            let Ok(cast) = cast else {
                return Ok(Outcome::Read {
                    bytes_read,
                    failed: true,
                });
            };
            observers.observe(&RecordBatch::try_new(Arc::clone(&observed), cast)?);
        }
        let failed = !observers
            .stored(&StoredFile {
                catalog_path: tf.file.path.clone(),
                object_path: location,
                file_size_bytes: tf.file.file_size_bytes,
                row_group_rows,
            })
            .await;
        Ok(Outcome::Read {
            bytes_read,
            failed,
        })
    }
}
