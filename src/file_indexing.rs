//! EXPERIMENTAL, opt-in: the write side of the per-file secondary index (see
//! [`crate::ext_file_index`] for the object format) and its catalog sidecar:
//!
//! ```sql
//! CREATE TABLE ext_ducklake_file_index (
//!     table_id         BIGINT  NOT NULL,
//!     path             VARCHAR NOT NULL,  -- ducklake_data_file.path
//!     file_size_bytes  BIGINT  NOT NULL,  -- must equal the listed file's size
//!     column_id        BIGINT  NOT NULL,  -- ducklake_column.column_id
//!     index_path       VARCHAR NOT NULL,  -- relative to the data file's directory
//!     index_key        VARCHAR NOT NULL,  -- full object-store key, for prune
//!     index_size_bytes BIGINT  NOT NULL,
//!     format_version   INTEGER NOT NULL,
//!     value_type       VARCHAR NOT NULL,  -- 'utf8' | 'int64'
//!     row_count        BIGINT  NOT NULL,
//!     row_group_rows   VARCHAR NOT NULL,  -- JSON [u64,...]
//!     heavy_hitters    VARCHAR NOT NULL,  -- JSON [[value, count],...]
//!     built_by         VARCHAR NOT NULL,  -- 'write' | 'compact' | 'backfill'
//!     built_at         TIMESTAMP NOT NULL,
//!     PRIMARY KEY (table_id, path, column_id)
//! )
//! ```
//!
//! Same contract as `ext_ducklake_file_order`: a row is a fact about the
//! immutable object at `path`, applied only when table, path and size match,
//! so it needs no snapshot and may land before the data file commits. A
//! missing row means "scan normally". `index_key` is stored because prune
//! runs after expiry, when the table's path chain may no longer resolve.
//!
//! Writes are best effort: the index object is PUT after the data object, and
//! the row is recorded after the index object, so a row never normally
//! points at a missing object. Any failure warns and leaves the file unindexed.

use crate::ext_file_index::{IndexKey, IndexValueType};

/// The sidecar table's name.
pub const TABLE: &str = "ext_ducklake_file_index";

/// Table-scoped `ducklake_metadata` key: comma-separated top-level column
/// names whose files get an index. Read by writes with the option on.
pub const COLUMNS_SETTING: &str = "ext_file_index_columns";

/// One row to record: the reader-facing meta plus the object's store key.
#[derive(Debug, Clone, PartialEq)]
pub struct FileIndexRow {
    pub meta: crate::ext_file_index::FileIndexMeta,
    /// Full object-store key of the index object (no leading slash).
    pub index_key: String,
}

#[cfg(feature = "write-sqlite")]
pub(crate) const SQLITE_DDL: &str = "CREATE TABLE IF NOT EXISTS ext_ducklake_file_index (
    table_id BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    index_path VARCHAR NOT NULL,
    index_key VARCHAR NOT NULL,
    index_size_bytes BIGINT NOT NULL,
    format_version INTEGER NOT NULL,
    value_type VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    row_group_rows VARCHAR NOT NULL,
    heavy_hitters VARCHAR NOT NULL,
    built_by VARCHAR NOT NULL,
    built_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_id, path, column_id)
)";

#[cfg(feature = "write-postgres")]
pub(crate) const POSTGRES_DDL: &str = "CREATE TABLE IF NOT EXISTS ext_ducklake_file_index (
    table_id BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    index_path VARCHAR NOT NULL,
    index_key VARCHAR NOT NULL,
    index_size_bytes BIGINT NOT NULL,
    format_version INTEGER NOT NULL,
    value_type VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    row_group_rows VARCHAR NOT NULL,
    heavy_hitters VARCHAR NOT NULL,
    built_by VARCHAR NOT NULL,
    built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, path, column_id)
)";

/// The columns every provider selects, in [`decode_row`]'s order.
#[cfg(any(feature = "metadata-sqlite", feature = "multicatalog-postgres"))]
pub(crate) const SELECT_COLUMNS: &str = "path, file_size_bytes, column_id, index_path, \
     index_size_bytes, format_version, value_type, row_count, row_group_rows, heavy_hitters";

/// One sidecar row as read, in [`SELECT_COLUMNS`] order. `None` for a row
/// this build cannot interpret (unknown value type, malformed JSON), which a
/// reader treats like a missing row.
#[allow(clippy::too_many_arguments)]
pub(crate) fn meta_from_parts(
    path: String,
    file_size_bytes: i64,
    column_id: i64,
    index_path: String,
    index_size_bytes: i64,
    format_version: i64,
    value_type: &str,
    row_count: i64,
    row_group_rows: &str,
    heavy_hitters: &str,
) -> Option<crate::ext_file_index::FileIndexMeta> {
    let value_type = IndexValueType::parse(value_type)?;
    Some(crate::ext_file_index::FileIndexMeta {
        path,
        file_size_bytes,
        column_id,
        index_path,
        index_size_bytes,
        format_version: i32::try_from(format_version).ok()?,
        value_type,
        row_count,
        row_group_rows: decode_u64_list(row_group_rows)?,
        heavy_hitters: decode_heavy_hitters(heavy_hitters, value_type)?,
    })
}

/// Read one [`SELECT_COLUMNS`] row with sqlx.
#[cfg(any(feature = "metadata-sqlite", feature = "multicatalog-postgres"))]
macro_rules! decode_file_index_row {
    ($row:expr) => {{
        use sqlx::Row as _;
        let row = $row;
        let value_type: String = row.try_get(6)?;
        let row_group_rows: String = row.try_get(8)?;
        let heavy_hitters: String = row.try_get(9)?;
        let format_version: i64 = row
            .try_get::<i32, _>(5)
            .map(i64::from)
            .or_else(|_| row.try_get::<i64, _>(5))?;
        crate::file_indexing::meta_from_parts(
            row.try_get(0)?,
            row.try_get(1)?,
            row.try_get(2)?,
            row.try_get(3)?,
            row.try_get(4)?,
            format_version,
            &value_type,
            row.try_get(7)?,
            &row_group_rows,
            &heavy_hitters,
        )
    }};
}
#[cfg(any(feature = "metadata-sqlite", feature = "multicatalog-postgres"))]
pub(crate) use decode_file_index_row;

/// Catalog encodings are the format library's, so every reader agrees.
pub(crate) fn encode_u64_list(values: &[u64]) -> String {
    crate::ext_file_index::row_group_rows_to_json(values)
}

pub(crate) fn decode_u64_list(text: &str) -> Option<Vec<u64>> {
    crate::ext_file_index::row_group_rows_from_json(text).ok()
}

pub(crate) fn encode_heavy_hitters(hitters: &[(IndexKey, u64)]) -> String {
    crate::ext_file_index::heavy_hitters_to_json(hitters)
}

pub(crate) fn decode_heavy_hitters(
    text: &str,
    value_type: IndexValueType,
) -> Option<Vec<(IndexKey, u64)>> {
    crate::ext_file_index::heavy_hitters_from_json(text, value_type).ok()
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

/// The data object key an index object belongs to, when `key` names one:
/// `<data>.c<digits>.v<digits>.pidx`. Used by the orphan sweep to keep an
/// index exactly as long as its data file is referenced.
pub(crate) fn data_key_of_index(key: &str) -> Option<&str> {
    let rest = key.strip_suffix(&format!(".{}", crate::ext_file_index::INDEX_SUFFIX))?;
    let (rest, version) = rest.rsplit_once(".v")?;
    let (data, column) = rest.rsplit_once(".c")?;
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    (digits(version) && digits(column) && !data.is_empty()).then_some(data)
}

/// The object path a stored `index_key` names. Keys are stored as
/// `ObjectPath` renders them (already encoded), so parse rather than encode.
pub fn object_key(index_key: &str) -> object_store::path::Path {
    object_store::path::Path::parse(index_key)
        .unwrap_or_else(|_| object_store::path::Path::from(index_key))
}

#[cfg(feature = "write")]
pub(crate) use write::{FileIndexBuilders, FileIndexColumn, FileIndexRequest};

#[cfg(feature = "write")]
mod write {
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    use super::{FileIndexRow, object_key};
    use crate::ext_file_index::{
        IndexValueType, PostingBuilder, index_file_name, index_object_path,
    };
    use crate::metadata_writer::MetadataWriter;

    /// One indexed column of a written file's schema.
    #[derive(Debug, Clone)]
    pub(crate) struct FileIndexColumn {
        pub(crate) column_id: i64,
        /// Top-level field index in the written (field-id-tagged) schema.
        pub(crate) field_index: usize,
        pub(crate) value_type: IndexValueType,
    }

    /// What a write asks of each file under the `ext_file_index` option:
    /// build postings for the declared columns, PUT the index objects next to
    /// the data object, and record their sidecar rows. One pointer wide, like
    /// `FileOrderRequest`.
    #[derive(Clone)]
    pub(crate) struct FileIndexRequest(Arc<Inner>);

    struct Inner {
        metadata: Arc<dyn MetadataWriter>,
        store: Arc<dyn ObjectStore>,
        table_id: i64,
        built_by: &'static str,
        columns: Vec<FileIndexColumn>,
    }

    impl std::fmt::Debug for FileIndexRequest {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FileIndexRequest")
                .field("table_id", &self.0.table_id)
                .field("built_by", &self.0.built_by)
                .field("columns", &self.0.columns)
                .finish()
        }
    }

    impl FileIndexRequest {
        /// The request for `declared` column names over `schema`, whose
        /// top-level fields are the written file's leading columns. Column ids
        /// come from `column_ids` (one per field of `schema`) when given, else
        /// from each field's `PARQUET:field_id`. A name that is missing,
        /// without an id, or of a type the index cannot key is skipped with a
        /// debug log. `None` when nothing is left.
        pub(crate) fn new(
            metadata: Arc<dyn MetadataWriter>,
            store: Arc<dyn ObjectStore>,
            table_id: i64,
            built_by: &'static str,
            declared: &[String],
            schema: &Schema,
            column_ids: Option<&[i64]>,
        ) -> Option<Self> {
            let columns: Vec<FileIndexColumn> = declared
                .iter()
                .filter_map(|name| {
                    let column = resolve_column(schema, name, column_ids);
                    if column.is_none() {
                        tracing::debug!(
                            column = name.as_str(),
                            "ext_file_index: declared column not indexable in this write"
                        );
                    }
                    column
                })
                .collect();
            (!columns.is_empty()).then(|| {
                Self(Arc::new(Inner {
                    metadata,
                    store,
                    table_id,
                    built_by,
                    columns,
                }))
            })
        }

        /// A request for already-resolved `columns` (the backfill, which
        /// resolves them per file against the file's own schema).
        pub(crate) fn from_columns(
            metadata: Arc<dyn MetadataWriter>,
            store: Arc<dyn ObjectStore>,
            table_id: i64,
            built_by: &'static str,
            columns: Vec<FileIndexColumn>,
        ) -> Self {
            Self(Arc::new(Inner {
                metadata,
                store,
                table_id,
                built_by,
                columns,
            }))
        }

        /// Fresh builders for one file.
        pub(crate) fn builders(&self) -> FileIndexBuilders {
            FileIndexBuilders {
                request: self.clone(),
                columns: self
                    .0
                    .columns
                    .iter()
                    .map(|c| (c.clone(), PostingBuilder::new(c.column_id, c.value_type)))
                    .collect(),
                failed: false,
            }
        }

        /// Build, PUT and record the indexes of the finished file at catalog
        /// `path` (relative to the table directory, as committed), stored at
        /// `data_object`. Call after the data object is uploaded and before
        /// the commit. Best effort: never fails the write. Returns the rows
        /// recorded.
        pub(crate) async fn record(
            &self,
            path: &str,
            data_object: &ObjectPath,
            file_size_bytes: i64,
            builders: FileIndexBuilders,
            row_group_rows: Vec<u64>,
        ) -> u64 {
            match self
                .try_record(path, data_object, file_size_bytes, builders, row_group_rows)
                .await
            {
                Ok(done) => done.rows,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        path,
                        "ext_file_index: not recorded; the file scans unindexed"
                    );
                    0
                },
            }
        }

        /// [`Self::record`], reporting failure to the caller (the backfill).
        pub(crate) async fn try_record(
            &self,
            path: &str,
            data_object: &ObjectPath,
            file_size_bytes: i64,
            builders: FileIndexBuilders,
            row_group_rows: Vec<u64>,
        ) -> crate::Result<Recorded> {
            if builders.failed {
                return Err(crate::DuckLakeError::Internal(
                    "postings incomplete".to_string(),
                ));
            }
            let rows_seen = builders.rows_seen();
            if rows_seen == 0 {
                return Ok(Recorded::default());
            }
            if row_group_rows.iter().sum::<u64>() != rows_seen {
                return Err(crate::DuckLakeError::Internal(format!(
                    "row groups hold {} rows, the builder saw {rows_seen}",
                    row_group_rows.iter().sum::<u64>()
                )));
            }
            let mut rows = Vec::with_capacity(builders.columns.len());
            let mut bytes_written = 0u64;
            for (column, builder) in builders.columns {
                let groups = row_group_rows.clone();
                // CPU-bound (sort + encode); off the async worker.
                let built = tokio::task::spawn_blocking(move || builder.finish(groups))
                    .await
                    .map_err(|e| crate::DuckLakeError::Internal(e.to_string()))??;
                let index_key = index_object_path(data_object.as_ref(), column.column_id);
                let index_path = index_file_name(path, column.column_id);
                bytes_written += built.bytes.len() as u64;
                self.0
                    .store
                    .put(
                        &object_key(&index_key),
                        PutPayload::from_bytes(built.bytes.clone()),
                    )
                    .await?;
                rows.push(FileIndexRow {
                    meta: built.meta(
                        path,
                        file_size_bytes,
                        column.column_id,
                        column.value_type,
                        index_path,
                    ),
                    index_key,
                });
            }
            let rows =
                self.0
                    .metadata
                    .record_file_indexes(self.0.table_id, self.0.built_by, &rows)?;
            Ok(Recorded {
                rows,
                bytes_written,
            })
        }
    }

    /// What [`FileIndexRequest::try_record`] stored.
    #[derive(Debug, Default, Clone, Copy)]
    pub(crate) struct Recorded {
        pub(crate) rows: u64,
        pub(crate) bytes_written: u64,
    }

    /// The top-level `name` of `schema` as an index column: it needs a
    /// catalog column id and a keyable type.
    pub(crate) fn resolve_column(
        schema: &Schema,
        name: &str,
        column_ids: Option<&[i64]>,
    ) -> Option<FileIndexColumn> {
        let (field_index, field) = schema.column_with_name(name)?;
        let column_id = match column_ids {
            Some(ids) => *ids.get(field_index)?,
            None => field.metadata().get("PARQUET:field_id")?.parse().ok()?,
        };
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
        /// Feed the next batch (the file's full schema). Never errors: a
        /// failure marks the file unindexed.
        pub(crate) fn observe(&mut self, batch: &RecordBatch) {
            if self.failed {
                return;
            }
            for (column, builder) in &mut self.columns {
                let Some(array) = batch.columns().get(column.field_index) else {
                    self.failed = true;
                    return;
                };
                if let Err(error) = builder.observe(array) {
                    tracing::warn!(%error, "ext_file_index: postings incomplete; file unindexed");
                    self.failed = true;
                    return;
                }
            }
        }

        pub(crate) fn rows_seen(&self) -> u64 {
            self.columns.first().map_or(0, |(_, b)| b.rows_seen())
        }

        /// The request these builders belong to.
        pub(crate) fn request(&self) -> &FileIndexRequest {
            &self.request
        }
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

    #[test]
    fn index_keys_map_back_to_their_data_file() {
        assert_eq!(
            data_key_of_index("t/x/ab.parquet.c12.v1.pidx"),
            Some("t/x/ab.parquet")
        );
        assert_eq!(data_key_of_index("t/ab.parquet"), None);
        assert_eq!(data_key_of_index("t/ab.parquet.c.v1.pidx"), None);
        assert_eq!(data_key_of_index("t/ab.parquet.c1.vx.pidx"), None);
        assert_eq!(data_key_of_index(".c1.v1.pidx"), None);
    }
}
