//! The write side of the file index: postings built from every row a write
//! puts in a file, stored as an object beside it, and recorded as a row of
//! [`super::table`].
//!
//! Indexes come from writes and compaction outputs and from
//! [`super::FileIndexTableExt::build_file_indexes`]. Writes are best effort:
//! the index object is PUT after the data object, and its row is recorded
//! after the index object and before the data file's commit, so a row never
//! normally points at a missing object. Any failure warns and leaves the file
//! unindexed.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_ducklake::extension::{
    CatalogDb, FileObserver, FileWriteHook, StoredFile, WriteContext, WriteOrigin,
};
use datafusion_ducklake::metadata_provider::DuckLakeTableFile;
use datafusion_ducklake::{DuckLakeError, Result};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use super::format::{
    BUCKET_GRAIN_HOUR_US, BUILT_BY_BACKFILL, BUILT_BY_COMPACT, BUILT_BY_WRITE, FileIndexMeta,
    IndexValueType, PostingBuilder, index_file_name, index_object_path,
};

/// Table setting: comma-separated top-level column names whose files get an
/// index. Read by every write that runs the extension.
pub const COLUMNS_SETTING: &str = "ext_file_index_columns";

/// Table setting: the top-level timestamp column the backfill records hourly
/// heavy-hitter counts over ([`super::counts`]). Unset = no counts.
pub const COUNT_COLUMN_SETTING: &str = "ext_file_index_count_column";

/// One row of [`super::table`]: the reader-facing meta, the object's store
/// key, and what built it.
#[derive(Debug, Clone, PartialEq)]
pub struct FileIndexRow {
    pub meta: FileIndexMeta,
    /// Full object-store key of the index object (no leading slash).
    pub index_key: String,
    /// `write`, `compact` or `backfill`.
    pub built_by: String,
}

/// Parse [`COLUMNS_SETTING`]: trimmed, non-empty, de-duplicated names.
pub(crate) fn parse_columns_setting(value: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for name in value.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        if !out.iter().any(|n| n == name) {
            out.push(name.to_string());
        }
    }
    out
}

/// The object path a stored `index_key` names. Keys are stored as
/// `ObjectPath` renders them (already encoded), so parse rather than encode.
pub fn object_key(index_key: &str) -> ObjectPath {
    ObjectPath::parse(index_key).unwrap_or_else(|_| ObjectPath::from(index_key))
}

/// What a backfill asks of the write hook, beyond the table's settings.
#[derive(Debug, Default)]
pub(crate) struct BackfillConfig {
    /// Index these columns instead of the declared ones.
    pub(crate) columns: Option<Vec<String>>,
    /// Count heavy hitters per hour over this column instead of the table's
    /// [`COUNT_COLUMN_SETTING`].
    pub(crate) count_column: Option<String>,
    /// `(path, size, column)` already indexed: a file is read only for the
    /// columns it lacks, and not at all when it lacks none.
    pub(crate) done: HashSet<(String, i64, String)>,
    /// Bytes of the index objects this backfill wrote.
    pub(crate) bytes_written: AtomicU64,
}

/// The write hook: postings for the table's declared columns, and on a
/// backfill hourly counts over the count column. `None` when no column is
/// declared. A setting that cannot be read leaves the write unindexed rather
/// than failing it.
pub(crate) fn write_hook(
    ctx: &WriteContext<'_>,
    shared: &Arc<super::Shared>,
    backfill: Option<&Arc<BackfillConfig>>,
) -> Result<Option<Arc<dyn FileWriteHook>>> {
    let Some(db) = ctx.db() else {
        return Ok(None);
    };
    let setting = |key: &str| match ctx.setting(key) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, key, "file_index: setting unreadable; not indexing");
            None
        },
    };
    let declared = match backfill.and_then(|config| config.columns.clone()) {
        Some(columns) => columns,
        None => match setting(COLUMNS_SETTING) {
            Some(value) => parse_columns_setting(&value),
            None => return Ok(None),
        },
    };
    if declared.is_empty() {
        return Ok(None);
    }
    // Writes build postings only; hourly counts come from the backfill.
    let ts_column = match ctx.origin {
        WriteOrigin::Backfill => backfill
            .and_then(|config| config.count_column.clone())
            .or_else(|| setting(COUNT_COLUMN_SETTING))
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty()),
        _ => None,
    };
    Ok(Some(Arc::new(IndexWriteHook {
        shared: Arc::clone(shared),
        db,
        table_id: ctx.table_id,
        store: Arc::clone(ctx.object_store),
        built_by: match ctx.origin {
            WriteOrigin::Compaction => BUILT_BY_COMPACT,
            WriteOrigin::Backfill => BUILT_BY_BACKFILL,
            _ => BUILT_BY_WRITE,
        },
        declared,
        ts_column,
        backfill: backfill.cloned(),
    })))
}

#[derive(Debug)]
struct IndexWriteHook {
    shared: Arc<super::Shared>,
    db: CatalogDb,
    table_id: i64,
    store: Arc<dyn ObjectStore>,
    built_by: &'static str,
    declared: Vec<String>,
    ts_column: Option<String>,
    backfill: Option<Arc<BackfillConfig>>,
}

impl FileWriteHook for IndexWriteHook {
    fn open_file(&self, file_schema: &SchemaRef) -> Option<Box<dyn FileObserver>> {
        let request = FileIndexRequest::new(
            Arc::clone(&self.store),
            self.built_by,
            &self.declared,
            self.ts_column.as_deref(),
            file_schema,
        )?;
        Some(Box::new(IndexObserver {
            builders: request.builders(),
            shared: Arc::clone(&self.shared),
            db: self.db.clone(),
            table_id: self.table_id,
            backfill: self.backfill.clone(),
        }))
    }

    /// The declared columns `file` has no index of yet, and the count column
    /// with them.
    fn backfill_columns(&self, file: &DuckLakeTableFile) -> Option<Vec<String>> {
        let done = |name: &String| {
            self.backfill.as_ref().is_some_and(|config| {
                config.done.contains(&(
                    file.file.path.clone(),
                    file.file.file_size_bytes,
                    name.clone(),
                ))
            })
        };
        let mut columns: Vec<String> = self
            .declared
            .iter()
            .filter(|name| !done(name))
            .cloned()
            .collect();
        if !columns.is_empty()
            && let Some(ts) = &self.ts_column
            && !columns.contains(ts)
        {
            columns.push(ts.clone());
        }
        Some(columns)
    }
}

#[derive(Debug)]
struct IndexObserver {
    builders: FileIndexBuilders,
    shared: Arc<super::Shared>,
    db: CatalogDb,
    table_id: i64,
    backfill: Option<Arc<BackfillConfig>>,
}

#[async_trait]
impl FileObserver for IndexObserver {
    fn observe(&mut self, batch: &RecordBatch) -> Result<()> {
        self.builders.observe(batch);
        Ok(())
    }

    /// Build and PUT the index objects, then record their rows.
    async fn stored(self: Box<Self>, file: &StoredFile) -> Result<()> {
        let Self {
            builders,
            shared,
            db,
            table_id,
            backfill,
        } = *self;
        let request = builders.request.clone();
        let rows = request
            .build(
                &file.catalog_path,
                &file.object_path,
                file.file_size_bytes,
                builders,
                file.row_group_rows.clone(),
            )
            .await?;
        let bytes: i64 = rows
            .iter()
            .map(|row| row.meta.index_size_bytes.max(0))
            .sum();
        shared.record(&db, table_id, rows).await?;
        if let Some(backfill) = &backfill {
            backfill
                .bytes_written
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// One indexed column of a written file's schema.
#[derive(Debug, Clone)]
pub(crate) struct FileIndexColumn {
    pub(crate) column_id: i64,
    /// Top-level field index in the written (field-id-tagged) schema.
    pub(crate) field_index: usize,
    pub(crate) value_type: IndexValueType,
}

/// The time column hourly bucket counts are over, as read.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FileIndexTs {
    pub(crate) column_id: i64,
    /// Column index in the batches fed to the builders.
    pub(crate) field_index: usize,
}

/// What one file's index needs: build postings for its columns and PUT the
/// index objects next to the data object.
#[derive(Clone)]
pub(crate) struct FileIndexRequest(Arc<Inner>);

struct Inner {
    store: Arc<dyn ObjectStore>,
    built_by: &'static str,
    columns: Vec<FileIndexColumn>,
    ts: Option<FileIndexTs>,
}

impl std::fmt::Debug for FileIndexRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIndexRequest")
            .field("built_by", &self.0.built_by)
            .field("columns", &self.0.columns)
            .finish()
    }
}

impl FileIndexRequest {
    /// The request for `declared` column names over `schema`, the file's
    /// schema, whose fields carry `PARQUET:field_id`, with hourly counts over
    /// `ts_column` when it is a timestamp field of `schema`. A name that is
    /// missing, without an id, or of a type the index cannot key is skipped
    /// with a debug log. `None` when nothing is left.
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        built_by: &'static str,
        declared: &[String],
        ts_column: Option<&str>,
        schema: &Schema,
    ) -> Option<Self> {
        let columns: Vec<FileIndexColumn> = declared
            .iter()
            .filter_map(|name| {
                let column = resolve_column(schema, name);
                if column.is_none() {
                    tracing::debug!(
                        column = name.as_str(),
                        "file_index: declared column not indexable in this file"
                    );
                }
                column
            })
            .collect();
        let ts = ts_column.and_then(|name| {
            let (field_index, field) = schema.column_with_name(name)?;
            if !matches!(field.data_type(), arrow::datatypes::DataType::Timestamp(..)) {
                return None;
            }
            Some(FileIndexTs {
                column_id: field.metadata().get("PARQUET:field_id")?.parse().ok()?,
                field_index,
            })
        });
        (!columns.is_empty()).then(|| {
            Self(Arc::new(Inner {
                store,
                built_by,
                columns,
                ts,
            }))
        })
    }

    /// Fresh builders for one file.
    pub(crate) fn builders(&self) -> FileIndexBuilders {
        FileIndexBuilders {
            request: self.clone(),
            columns: self
                .0
                .columns
                .iter()
                .map(|c| {
                    let builder = PostingBuilder::new(c.column_id, c.value_type);
                    let builder = match self.0.ts {
                        Some(ts) => builder.with_bucket_counts(ts.column_id, BUCKET_GRAIN_HOUR_US),
                        None => builder,
                    };
                    (c.clone(), builder)
                })
                .collect(),
            ts_field_index: self.0.ts.map(|ts| ts.field_index),
            failed: false,
        }
    }

    /// Build and PUT the indexes of the finished file at catalog `path`
    /// (relative to the table directory, as committed), stored at
    /// `data_object`, and return their rows to record. Called after the data
    /// object is stored.
    pub(crate) async fn build(
        &self,
        path: &str,
        data_object: &ObjectPath,
        file_size_bytes: i64,
        builders: FileIndexBuilders,
        row_group_rows: Vec<u64>,
    ) -> Result<Vec<FileIndexRow>> {
        if builders.failed {
            return Err(DuckLakeError::Internal("postings incomplete".to_string()));
        }
        let rows_seen = builders.rows_seen();
        if rows_seen == 0 {
            return Ok(Vec::new());
        }
        if row_group_rows.iter().sum::<u64>() != rows_seen {
            return Err(DuckLakeError::Internal(format!(
                "row groups hold {} rows, the builder saw {rows_seen}",
                row_group_rows.iter().sum::<u64>()
            )));
        }
        let mut rows = Vec::with_capacity(builders.columns.len());
        for (column, builder) in builders.columns {
            let groups = row_group_rows.clone();
            // CPU-bound (sort + encode); off the async worker.
            let built = tokio::task::spawn_blocking(move || builder.finish(groups))
                .await
                .map_err(|e| DuckLakeError::Internal(e.to_string()))?
                .map_err(|e| DuckLakeError::Internal(e.to_string()))?;
            let index_key = index_object_path(data_object.as_ref(), column.column_id);
            let index_path = index_file_name(path, column.column_id);
            self.0
                .store
                .put(
                    &object_key(&index_key),
                    PutPayload::from_bytes(built.bytes.clone()),
                )
                .await
                .map_err(|e| DuckLakeError::Internal(e.to_string()))?;
            rows.push(FileIndexRow {
                meta: built.meta(
                    path,
                    file_size_bytes,
                    column.column_id,
                    column.value_type,
                    index_path,
                ),
                index_key,
                built_by: self.0.built_by.to_string(),
            });
        }
        Ok(rows)
    }
}

/// The top-level `name` of `schema` as an index column: it needs a
/// `PARQUET:field_id` and a keyable type.
fn resolve_column(schema: &Schema, name: &str) -> Option<FileIndexColumn> {
    let (field_index, field) = schema.column_with_name(name)?;
    let column_id = field.metadata().get("PARQUET:field_id")?.parse().ok()?;
    let value_type = IndexValueType::for_arrow(field.data_type())?;
    Some(FileIndexColumn {
        column_id,
        field_index,
        value_type,
    })
}

/// The postings of one open file, fed every batch in write order.
pub(crate) struct FileIndexBuilders {
    request: FileIndexRequest,
    columns: Vec<(FileIndexColumn, PostingBuilder)>,
    /// Where the time column is in each batch, when counting buckets.
    ts_field_index: Option<usize>,
    /// A batch failed to observe: the postings are incomplete, so nothing
    /// is recorded for this file.
    failed: bool,
}

impl std::fmt::Debug for FileIndexBuilders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIndexBuilders")
            .field("columns", &self.columns.len())
            .field("failed", &self.failed)
            .finish()
    }
}

impl FileIndexBuilders {
    /// Feed the next batch (the file's full schema). Never errors: a failure
    /// marks the file unindexed.
    pub(crate) fn observe(&mut self, batch: &RecordBatch) {
        if self.failed {
            return;
        }
        let ts = self
            .ts_field_index
            .and_then(|index| batch.columns().get(index));
        for (column, builder) in &mut self.columns {
            let Some(array) = batch.columns().get(column.field_index) else {
                self.failed = true;
                return;
            };
            // A missing time column leaves the builder short of rows, so it
            // records no counts.
            if let Some(ts) = ts {
                builder.observe_ts(ts);
            }
            if let Err(error) = builder.observe(array) {
                tracing::warn!(%error, "file_index: postings incomplete; file unindexed");
                self.failed = true;
                return;
            }
        }
    }

    pub(crate) fn rows_seen(&self) -> u64 {
        self.columns.first().map_or(0, |(_, b)| b.rows_seen())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_setting_parses() {
        assert_eq!(parse_columns_setting(" a, b ,,a,c "), vec!["a", "b", "c"]);
        assert!(parse_columns_setting("").is_empty());
    }
}
