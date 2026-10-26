//! The write side of the file index: postings built from every row a write
//! puts in a file, stored as an object beside it, and recorded as a fact.
//!
//! One fact per (data file, indexed column): extension [`super::NAME`], key
//! the column id, value a JSON object holding the [`FileIndexMeta`] fields
//! that are not the fact's own (path, size, key), plus the object's full
//! store key (`index_key`, kept because prune runs after expiry, when the
//! table's path chain may no longer resolve) and who built it (`built_by`).
//! `row_group_rows`, `heavy_hitters` and `bucket_counts` hold their own JSON
//! encodings as strings, so malformed counts lose only the counts.
//!
//! Indexes come from writes and compaction outputs and from
//! [`super::FileIndexTableExt::build_file_indexes`]. Writes are best effort:
//! the index object is PUT after the data object, and the fact is recorded
//! after the index object, so a fact never normally points at a missing
//! object. Any failure warns and leaves the file unindexed.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_ducklake::extension::{
    FileFact, FileObserver, FileWriteHook, StoredFile, WriteContext, WriteOrigin,
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

/// One fact to record: the reader-facing meta, the object's store key, and
/// what built it.
#[derive(Debug, Clone, PartialEq)]
pub struct FileIndexRow {
    pub meta: FileIndexMeta,
    /// Full object-store key of the index object (no leading slash).
    pub index_key: String,
    /// `write`, `compact` or `backfill`.
    pub built_by: String,
}

impl FileIndexRow {
    /// This row as a [`FileFact`].
    pub fn to_fact(&self) -> FileFact {
        let meta = &self.meta;
        FileFact {
            path: meta.path.clone(),
            file_size_bytes: meta.file_size_bytes,
            key: meta.column_id,
            value: serde_json::json!({
                "index_path": meta.index_path,
                "index_key": self.index_key,
                "index_size_bytes": meta.index_size_bytes,
                "format_version": meta.format_version,
                "value_type": meta.value_type.as_str(),
                "row_count": meta.row_count,
                "row_group_rows": super::format::row_group_rows_to_json(&meta.row_group_rows),
                "heavy_hitters": super::format::heavy_hitters_to_json(&meta.heavy_hitters),
                "built_by": self.built_by,
                "bucket_counts": meta.bucket_counts.as_ref().map(super::counts::bucket_counts_to_json),
            })
            .to_string(),
        }
    }

    /// A [`FileFact`] read back. `None` for one this build cannot interpret
    /// (unknown value type, malformed JSON), which a reader treats like a
    /// missing fact.
    pub fn from_fact(fact: &FileFact) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_str(&fact.value).ok()?;
        let text = |name: &str| value.get(name)?.as_str();
        let value_type = IndexValueType::parse(text("value_type")?)?;
        Some(Self {
            meta: FileIndexMeta {
                path: fact.path.clone(),
                file_size_bytes: fact.file_size_bytes,
                column_id: fact.key,
                index_path: text("index_path")?.to_string(),
                index_size_bytes: value.get("index_size_bytes")?.as_i64()?,
                format_version: i32::try_from(value.get("format_version")?.as_i64()?).ok()?,
                value_type,
                row_count: value.get("row_count")?.as_i64()?,
                row_group_rows: super::format::row_group_rows_from_json(text("row_group_rows")?)
                    .ok()?,
                heavy_hitters: super::format::heavy_hitters_from_json(
                    text("heavy_hitters")?,
                    value_type,
                )
                .ok()?,
                // Malformed counts lose only the counts, never the index.
                bucket_counts: text("bucket_counts")
                    .and_then(|counts| super::counts::bucket_counts_from_json(counts, value_type)),
            },
            index_key: text("index_key")?.to_string(),
            built_by: text("built_by").unwrap_or_default().to_string(),
        })
    }
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
}

/// The write hook: postings for the table's declared columns, and on a
/// backfill hourly counts over the count column. `None` when no column is
/// declared. A setting that cannot be read leaves the write unindexed rather
/// than failing it.
pub(crate) fn write_hook(
    ctx: &WriteContext<'_>,
    backfill: Option<&Arc<BackfillConfig>>,
) -> Result<Option<Arc<dyn FileWriteHook>>> {
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
        Some(Box::new(IndexObserver(request.builders())))
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
struct IndexObserver(FileIndexBuilders);

#[async_trait]
impl FileObserver for IndexObserver {
    fn observe(&mut self, batch: &RecordBatch) -> Result<()> {
        self.0.observe(batch);
        Ok(())
    }

    /// Build and PUT the index objects; their facts, once stored.
    async fn stored(self: Box<Self>, file: &StoredFile) -> Result<Vec<FileFact>> {
        let builders = self.0;
        let request = builders.request.clone();
        request
            .build(
                &file.catalog_path,
                &file.object_path,
                file.file_size_bytes,
                builders,
                file.row_group_rows.clone(),
            )
            .await
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
    /// `data_object`, and return their facts for core to record. Called after
    /// the data object is stored.
    pub(crate) async fn build(
        &self,
        path: &str,
        data_object: &ObjectPath,
        file_size_bytes: i64,
        builders: FileIndexBuilders,
        row_group_rows: Vec<u64>,
    ) -> Result<Vec<FileFact>> {
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
        let mut facts = Vec::with_capacity(builders.columns.len());
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
            facts.push(
                FileIndexRow {
                    meta: built.meta(
                        path,
                        file_size_bytes,
                        column.column_id,
                        column.value_type,
                        index_path,
                    ),
                    index_key,
                    built_by: self.0.built_by.to_string(),
                }
                .to_fact(),
            );
        }
        Ok(facts)
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
    use crate::file_index::format::IndexKey;

    #[test]
    fn columns_setting_parses() {
        assert_eq!(parse_columns_setting(" a, b ,,a,c "), vec!["a", "b", "c"]);
        assert!(parse_columns_setting("").is_empty());
    }

    #[test]
    fn a_row_survives_its_fact() {
        let row = FileIndexRow {
            meta: FileIndexMeta {
                path: "a/b.parquet".into(),
                file_size_bytes: 42,
                column_id: 7,
                index_path: "b.parquet.c7.v1.pidx".into(),
                index_size_bytes: 9,
                format_version: 1,
                value_type: IndexValueType::Utf8,
                row_count: 3,
                row_group_rows: vec![2, 1],
                heavy_hitters: vec![(IndexKey::Utf8(b"x".to_vec().into()), 2)],
                bucket_counts: None,
            },
            index_key: "t/a/b.parquet.c7.v1.pidx".into(),
            built_by: "write".into(),
        };
        let fact = row.to_fact();
        assert_eq!((fact.key, fact.file_size_bytes), (7, 42));
        assert_eq!(FileIndexRow::from_fact(&fact), Some(row));
    }

    #[test]
    fn malformed_counts_lose_only_the_counts() {
        let mut fact = FileIndexRow {
            meta: FileIndexMeta {
                path: "p".into(),
                file_size_bytes: 1,
                column_id: 1,
                index_path: "i".into(),
                index_size_bytes: 1,
                format_version: 1,
                value_type: IndexValueType::Int64,
                row_count: 1,
                row_group_rows: vec![1],
                heavy_hitters: vec![],
                bucket_counts: None,
            },
            index_key: "k".into(),
            built_by: "write".into(),
        }
        .to_fact();
        let mut value: serde_json::Value = serde_json::from_str(&fact.value).unwrap();
        value["bucket_counts"] = serde_json::Value::String("{\"v\":1".into());
        fact.value = value.to_string();
        let row = FileIndexRow::from_fact(&fact).unwrap();
        assert_eq!(row.meta.bucket_counts, None);
        fact.value = "{".into();
        assert_eq!(FileIndexRow::from_fact(&fact), None);
    }
}
