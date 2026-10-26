//! Index files written without the extension.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_ducklake::metadata_provider::DuckLakeTableFile;
use datafusion_ducklake::{DuckLakeError, DuckLakeTable, Result};
use futures::StreamExt;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};

use super::format::{
    BUCKET_GRAIN_HOUR_US, BUILT_BY_BACKFILL, FILE_INDEX_FORMAT_VERSION, IndexValueType,
};
use super::write::{
    COLUMNS_SETTING, COUNT_COLUMN_SETTING, FileIndexColumn, FileIndexRequest, FileIndexTs,
    parse_columns_setting,
};

/// Options for [`FileIndexTableExt::build_file_indexes`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct BuildIndexOptions {
    /// Only these data files; `None` = every live file.
    pub data_file_ids: Option<Vec<i64>>,
    /// Stop after indexing this many files (bounds one pass); `None` = all.
    pub max_files: Option<usize>,
    /// Index these columns; `None` = the table's [`COLUMNS_SETTING`].
    pub columns: Option<Vec<String>>,
    /// Also record hourly heavy-hitter counts over this top-level timestamp
    /// column; `None` = the table's [`COUNT_COLUMN_SETTING`], if any.
    pub count_column: Option<String>,
    /// Files read and indexed at once; 0 is taken as 1.
    pub parallelism: usize,
}

impl BuildIndexOptions {
    /// Index only these data files.
    #[must_use]
    pub fn with_data_file_ids(mut self, data_file_ids: Vec<i64>) -> Self {
        self.data_file_ids = Some(data_file_ids);
        self
    }

    /// Stop after indexing `max_files` files.
    #[must_use]
    pub fn with_max_files(mut self, max_files: usize) -> Self {
        self.max_files = Some(max_files);
        self
    }

    /// Index these columns instead of the declared ones.
    #[must_use]
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
        self
    }

    /// Record hourly heavy-hitter counts over this timestamp column.
    #[must_use]
    pub fn with_count_column(mut self, column: impl Into<String>) -> Self {
        self.count_column = Some(column.into());
        self
    }

    /// Read and index `parallelism` files at once.
    #[must_use]
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }
}

/// What [`FileIndexTableExt::build_file_indexes`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BuildIndexResult {
    /// Files whose missing indexes were built and recorded.
    pub files_indexed: u64,
    /// Files not read: every requested column already has a matching fact,
    /// or the file is name-mapped, encrypted, or lacks the columns.
    pub files_skipped: u64,
    /// Files whose read, build, upload or record failed (warned; a later
    /// pass retries them).
    pub files_failed: u64,
    /// Compressed bytes of the indexed column chunks read.
    pub bytes_read: u64,
    /// Bytes of index objects written.
    pub bytes_written: u64,
}

/// [`DuckLakeTable`] methods of the file index extension.
#[async_trait]
pub trait FileIndexTableExt {
    /// Build the missing per-file indexes of live data files for the declared
    /// (or given) columns: files written before the extension was on, by
    /// another client, or registered as is. No file is rewritten and no
    /// snapshot is taken.
    ///
    /// Per file: read only the indexed columns (by field id, else by name for
    /// a file without field ids), build the postings, PUT the index object
    /// next to the data file and record its fact. Each file is recorded as
    /// soon as it is built, so an interrupted pass keeps its progress and the
    /// next pass skips what is done: a file whose fact matches its path,
    /// size, column, format version and value type. A fact for a file
    /// compacted away meanwhile matches no live file and is dropped by
    /// [`super::prune_file_indexes`]. Idempotent.
    async fn build_file_indexes(
        &self,
        state: &dyn Session,
        opts: BuildIndexOptions,
    ) -> Result<BuildIndexResult>;
}

enum Outcome {
    Indexed { bytes_read: u64, bytes_written: u64 },
    Skipped,
}

#[async_trait]
impl FileIndexTableExt for DuckLakeTable {
    async fn build_file_indexes(
        &self,
        state: &dyn Session,
        opts: BuildIndexOptions,
    ) -> Result<BuildIndexResult> {
        let mut result = BuildIndexResult::default();
        let writer = self.writer().ok_or_else(|| {
            DuckLakeError::InvalidConfig(
                "build_file_indexes: table is read-only; open the catalog with a writer"
                    .to_string(),
            )
        })?;
        let schema_name = self.schema_name().map(str::to_string);
        let setting = |key: &str| match &schema_name {
            Some(schema_name) => writer.table_setting(schema_name, self.table_name(), key),
            None => Ok(None),
        };
        let declared = match opts.columns {
            Some(columns) => columns,
            None => match setting(COLUMNS_SETTING)? {
                Some(value) => parse_columns_setting(&value),
                None => return Ok(result),
            },
        };
        // (column_id, catalog name, value type of the catalog type).
        let physical_schema = self.physical_schema();
        let column_ids = self.top_level_column_ids();
        let mut wanted: Vec<(i64, String, IndexValueType)> = Vec::new();
        for name in &declared {
            let Ok(index) = physical_schema.index_of(name) else {
                return Err(DuckLakeError::InvalidConfig(format!(
                    "build_file_indexes: no top-level column '{name}'"
                )));
            };
            let Some(value_type) =
                IndexValueType::for_arrow(physical_schema.field(index).data_type())
            else {
                return Err(DuckLakeError::InvalidConfig(format!(
                    "build_file_indexes: column '{name}' has a type the index cannot key"
                )));
            };
            if !wanted.iter().any(|(id, _, _)| *id == column_ids[index]) {
                wanted.push((column_ids[index], name.clone(), value_type));
            }
        }
        if wanted.is_empty() {
            return Ok(result);
        }
        let wanted_ids: Vec<i64> = wanted.iter().map(|(id, _, _)| *id).collect();
        // (column_id, name) of the time column to count heavy hitters over.
        let count_column = match opts.count_column.clone() {
            Some(name) => Some(name),
            None => setting(COUNT_COLUMN_SETTING)?
                .map(|value| value.trim().to_string())
                .filter(|name| !name.is_empty()),
        };
        let ts_wanted: Option<(i64, String)> = match count_column {
            None => None,
            Some(name) => {
                let index = physical_schema.index_of(&name).map_err(|_| {
                    DuckLakeError::InvalidConfig(format!(
                        "build_file_indexes: no top-level count column '{name}'"
                    ))
                })?;
                if !matches!(
                    physical_schema.field(index).data_type(),
                    DataType::Timestamp(..)
                ) {
                    return Err(DuckLakeError::InvalidConfig(format!(
                        "build_file_indexes: count column '{name}' is not a timestamp"
                    )));
                }
                Some((column_ids[index], name))
            },
        };

        let object_store = state
            .runtime_env()
            .object_store(self.object_store_url().as_ref())?;
        let readers = DefaultParquetFileReaderFactory::new(Arc::clone(&object_store));
        let reader_metrics = ExecutionPlanMetricsSet::new();
        let selected = opts
            .data_file_ids
            .map(|ids| ids.into_iter().collect::<HashSet<_>>());
        let candidates: Vec<DuckLakeTableFile> = self
            .data_files()?
            .into_iter()
            .filter(|tf| {
                selected
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&tf.data_file_id))
            })
            .collect();
        let paths: Vec<&str> = candidates.iter().map(|tf| tf.file.path.as_str()).collect();
        let done: HashSet<(String, i64, i64)> = super::file_index_rows(
            self.metadata_provider().as_ref(),
            self.table_id(),
            &paths,
            &wanted_ids,
        )?
        .into_iter()
        .map(|row| row.meta)
        .filter(|meta| {
            meta.format_version == FILE_INDEX_FORMAT_VERSION
                && wanted
                    .iter()
                    .any(|(id, _, ty)| *id == meta.column_id && *ty == meta.value_type)
                // With counts asked for, a fact without them is rebuilt.
                && ts_wanted.as_ref().is_none_or(|(ts_id, _)| {
                    meta.bucket_counts.as_ref().is_some_and(|c| {
                        c.ts_column_id == *ts_id && c.grain_us == BUCKET_GRAIN_HOUR_US
                    })
                })
        })
        .map(|meta| (meta.path, meta.file_size_bytes, meta.column_id))
        .collect();

        // Per candidate, the columns it still lacks; files lacking none, and
        // files this reader cannot place columns in, are skipped.
        let mut work = Vec::new();
        for tf in candidates {
            let missing: Vec<(i64, String, IndexValueType)> = wanted
                .iter()
                .filter(|(id, _, _)| {
                    !done.contains(&(tf.file.path.clone(), tf.file.file_size_bytes, *id))
                })
                .cloned()
                .collect();
            if missing.is_empty()
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
            work.push((tf, missing));
        }
        if let Some(max) = opts.max_files {
            // Left for a later pass; not counted as skipped.
            work.truncate(max);
        }

        let index_one = |tf: DuckLakeTableFile, missing: Vec<(i64, String, IndexValueType)>| {
            let readers = &readers;
            let reader_metrics = &reader_metrics;
            let object_store = &object_store;
            let ts_wanted = &ts_wanted;
            async move {
                let source = self.data_file_location(&tf.file)?;
                let size = u64::try_from(tf.file.file_size_bytes).map_err(|_| {
                    DuckLakeError::Internal(format!("negative file size for {}", tf.file.path))
                })?;
                let open = || {
                    readers.create_reader(
                        0,
                        PartitionedFile::new(source.to_string(), size),
                        datafusion_ducklake::table::metadata_size_hint(tf.file.footer_size),
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
                let by_name = file_schema.fields().iter().all(|f| field_id(f).is_none());
                // (root in the file, column id, value type); a column the file
                // lacks (added later) or stores as an unkeyable or different
                // key type is left unindexed for this file.
                let mut roots: Vec<(usize, i64, IndexValueType)> = Vec::new();
                for (id, name, ty) in &missing {
                    let root = if by_name {
                        file_schema.index_of(name).ok()
                    } else {
                        file_schema
                            .fields()
                            .iter()
                            .position(|f| field_id(f) == Some(*id))
                    };
                    let Some(root) = root else {
                        continue;
                    };
                    if IndexValueType::for_arrow(file_schema.field(root).data_type()) == Some(*ty) {
                        roots.push((root, *id, *ty));
                    }
                }
                if roots.is_empty() {
                    return Ok::<_, DuckLakeError>(Outcome::Skipped);
                }
                roots.sort_unstable_by_key(|(root, _, _)| *root);
                // The time column, when counting and the file holds it as a
                // timestamp; without it the file gets an index but no counts.
                let ts_root = ts_wanted.as_ref().and_then(|(ts_id, name)| {
                    let root = if by_name {
                        file_schema.index_of(name).ok()
                    } else {
                        file_schema
                            .fields()
                            .iter()
                            .position(|f| field_id(f) == Some(*ts_id))
                    }?;
                    matches!(file_schema.field(root).data_type(), DataType::Timestamp(..))
                        .then_some((root, *ts_id))
                });
                let mut root_indices: Vec<usize> = roots.iter().map(|(r, _, _)| *r).collect();
                if let Some((root, _)) = ts_root {
                    root_indices.push(root);
                }
                root_indices.sort_unstable();
                root_indices.dedup();
                let position = |root: usize| root_indices.binary_search(&root).expect("projected");
                let descr = footer.file_metadata().schema_descr();
                let bytes_read = footer
                    .row_groups()
                    .iter()
                    .flat_map(|rg| rg.columns().iter().enumerate())
                    .filter(|(leaf, _)| root_indices.contains(&descr.get_column_root_idx(*leaf)))
                    .map(|(_, column)| column.compressed_size().max(0) as u64)
                    .sum::<u64>();
                let row_group_rows: Vec<u64> = footer
                    .row_groups()
                    .iter()
                    .map(|rg| rg.num_rows().max(0) as u64)
                    .collect();
                let mask = ProjectionMask::roots(descr, root_indices.clone());
                let reader: Box<dyn AsyncFileReader> = open()?;
                let mut stream =
                    ParquetRecordBatchStreamBuilder::new_with_metadata(reader, reader_metadata)
                        .with_projection(mask)
                        .build()?;
                // The projected batch holds the roots in ascending order.
                let columns: Vec<FileIndexColumn> = roots
                    .iter()
                    .map(|(root, column_id, value_type)| FileIndexColumn {
                        column_id: *column_id,
                        field_index: position(*root),
                        value_type: *value_type,
                    })
                    .collect();
                let ts = ts_root.map(|(root, column_id)| FileIndexTs {
                    column_id,
                    field_index: position(root),
                });
                let request = FileIndexRequest::from_columns(
                    Arc::clone(writer),
                    Arc::clone(object_store),
                    self.table_id(),
                    BUILT_BY_BACKFILL,
                    columns,
                    ts,
                );
                let mut builders = request.builders();
                while let Some(batch) = stream.next().await {
                    builders.observe(&batch?);
                }
                let recorded = request
                    .try_record(
                        &tf.file.path,
                        &source,
                        tf.file.file_size_bytes,
                        builders,
                        row_group_rows,
                    )
                    .await?;
                Ok(Outcome::Indexed {
                    bytes_read,
                    bytes_written: recorded.bytes_written,
                })
            }
        };

        let mut outcomes = futures::stream::iter(work.into_iter().map(|(tf, missing)| {
            let path = tf.file.path.clone();
            let fut = index_one(tf, missing);
            async move { (path, fut.await) }
        }))
        .buffer_unordered(opts.parallelism.max(1));
        while let Some((path, outcome)) = outcomes.next().await {
            match outcome {
                Ok(Outcome::Indexed {
                    bytes_read,
                    bytes_written,
                }) => {
                    result.files_indexed += 1;
                    result.bytes_read += bytes_read;
                    result.bytes_written += bytes_written;
                },
                Ok(Outcome::Skipped) => result.files_skipped += 1,
                Err(error) => {
                    tracing::warn!(%error, path, "build_file_indexes: file not indexed");
                    result.files_failed += 1;
                },
            }
        }
        Ok(result)
    }
}
