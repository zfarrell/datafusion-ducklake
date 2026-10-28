//! Index files written without the extension.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::DataType;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion_ducklake::extension::{DuckLakeExtension, FileWriteHook, WriteContext};
use datafusion_ducklake::{BackfillOptions, DuckLakeError, DuckLakeTable, Result};

use super::format::{BUCKET_GRAIN_HOUR_US, FILE_INDEX_FORMAT_VERSION, IndexValueType};
use super::write::{BackfillConfig, COLUMNS_SETTING, COUNT_COLUMN_SETTING, parse_columns_setting};

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
    /// Runs [`DuckLakeTable::backfill`]: per file, read only the columns it
    /// has no index of (by field id, else by name for a file without field
    /// ids), build the postings, PUT the index object next to the data file
    /// and record its fact. Each file is recorded as
    /// soon as it is built, so an interrupted pass keeps its progress and the
    /// next pass skips what is done: a file whose fact matches its path,
    /// size, column, format version and value type. A fact for a file
    /// compacted away meanwhile matches no live file and is dropped by
    /// [`super::prune_file_indexes`]. Idempotent.
    ///
    /// Facts go to the [`super::FileIndex`] registered in the table's read
    /// options (`DuckLakeCatalog::with_extension` registers it there).
    async fn build_file_indexes(
        &self,
        state: &dyn Session,
        opts: BuildIndexOptions,
    ) -> Result<BuildIndexResult>;
}

/// [`FileIndex`]'s write hook for a backfill, with its overrides and the
/// columns each file already has.
#[derive(Debug)]
struct IndexBackfill {
    shared: Arc<super::Shared>,
    config: Arc<BackfillConfig>,
}

impl DuckLakeExtension for IndexBackfill {
    fn name(&self) -> &str {
        super::NAME
    }

    fn write_hook(&self, ctx: &WriteContext<'_>) -> Result<Option<Arc<dyn FileWriteHook>>> {
        super::write::write_hook(ctx, &self.shared, Some(&self.config))
    }
}

#[async_trait]
impl FileIndexTableExt for DuckLakeTable {
    async fn build_file_indexes(
        &self,
        state: &dyn Session,
        opts: BuildIndexOptions,
    ) -> Result<BuildIndexResult> {
        let writer = self.writer().ok_or_else(|| {
            DuckLakeError::InvalidConfig(
                "build_file_indexes: table is read-only; open the catalog with a writer"
                    .to_string(),
            )
        })?;
        let Some(index) = self.read_options().extensions().get::<super::FileIndex>() else {
            return Err(DuckLakeError::InvalidConfig(
                "build_file_indexes: no FileIndex registered on the table's read options"
                    .to_string(),
            ));
        };
        let Some(db) = self.metadata_provider().catalog_db() else {
            return Err(DuckLakeError::Unsupported(
                "build_file_indexes: the catalog's backend keeps no extension tables".to_string(),
            ));
        };
        let setting = |key: &str| match self.schema_name() {
            Some(schema_name) => writer.table_setting(schema_name, self.table_name(), key),
            None => Ok(None),
        };
        let declared = match opts.columns {
            Some(columns) => columns,
            None => match setting(COLUMNS_SETTING)? {
                Some(value) => parse_columns_setting(&value),
                None => return Ok(BuildIndexResult::default()),
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
            return Ok(BuildIndexResult::default());
        }
        let wanted_ids: Vec<i64> = wanted.iter().map(|(id, _, _)| *id).collect();
        let count_column = match opts.count_column.clone() {
            Some(name) => Some(name),
            None => setting(COUNT_COLUMN_SETTING)?
                .map(|value| value.trim().to_string())
                .filter(|name| !name.is_empty()),
        };
        // The column id of the time column to count heavy hitters over.
        let ts_wanted: Option<i64> = match &count_column {
            None => None,
            Some(name) => {
                let index = physical_schema.index_of(name).map_err(|_| {
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
                Some(column_ids[index])
            },
        };

        // The columns each file already has a usable index of.
        let files = self.data_files()?;
        let paths: Vec<&str> = files.iter().map(|tf| tf.file.path.as_str()).collect();
        let done: HashSet<(String, i64, String)> =
            super::file_index_rows(&db, self.table_id(), &paths, &wanted_ids)
                .await?
                .into_iter()
                .map(|row| row.meta)
                .filter_map(|meta| {
                    let (_, name, _) = wanted.iter().find(|(id, _, ty)| {
                        *id == meta.column_id
                            && *ty == meta.value_type
                            && meta.format_version == FILE_INDEX_FORMAT_VERSION
                    })?;
                    // With counts asked for, an index without them is rebuilt.
                    let counted = ts_wanted.is_none_or(|ts_id| {
                        meta.bucket_counts.as_ref().is_some_and(|c| {
                            c.ts_column_id == ts_id && c.grain_us == BUCKET_GRAIN_HOUR_US
                        })
                    });
                    counted.then(|| (meta.path, meta.file_size_bytes, name.clone()))
                })
                .collect();

        let mut backfill = BackfillOptions::default().with_parallelism(opts.parallelism);
        if let Some(ids) = opts.data_file_ids {
            backfill = backfill.with_data_file_ids(ids);
        }
        if let Some(max) = opts.max_files {
            backfill = backfill.with_max_files(max);
        }
        let config = Arc::new(BackfillConfig {
            columns: Some(wanted.iter().map(|(_, name, _)| name.clone()).collect()),
            count_column,
            done,
            bytes_written: Default::default(),
        });
        let result = self
            .backfill(
                state,
                Arc::new(IndexBackfill {
                    shared: Arc::clone(&index.shared),
                    config: Arc::clone(&config),
                }),
                backfill,
            )
            .await?;
        Ok(BuildIndexResult {
            files_indexed: result.files_read,
            files_skipped: result.files_skipped,
            files_failed: result.files_failed,
            bytes_read: result.bytes_read,
            bytes_written: config
                .bytes_written
                .load(std::sync::atomic::Ordering::Relaxed),
        })
    }
}
