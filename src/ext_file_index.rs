//! Per-file secondary index ("file index") format.
//!
//! One sidecar object per (data file, indexed column): a parquet-encoded
//! posting list of `(value, row)` sorted by `(value, row)`, where `row` is the
//! row's position in the data file. The catalog row (`ext_ducklake_file_index`,
//! see [`FileIndexMeta`]) carries the per-row-group row counts and the
//! heavy-hitter list so the read side can decide per file without touching
//! the object.
//!
//! The object is named `<data file>.c<column_id>.v1.pidx` and must never end
//! in `.parquet`: the orphan sweep deletes every unreferenced `.parquet`
//! under the data path.
//!
//! This module has no catalog or scan logic; it is the format library shared
//! by the write path (posting builder) and the read path (lookups, access
//! plans, posting cache).

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion_datasource_parquet::ParquetAccessPlan;
use object_store::ObjectStore;

pub const FILE_INDEX_FORMAT_VERSION: i32 = 1;
/// A value is heavy in a file if its count > `row_count / HEAVY_HITTER_DIVISOR`.
pub const HEAVY_HITTER_DIVISOR: u64 = 4096;
/// Never ".parquet" (orphan sweep).
pub const INDEX_SUFFIX: &str = "pidx";
pub const BUILT_BY_WRITE: &str = "write";
pub const BUILT_BY_COMPACT: &str = "compact";
pub const BUILT_BY_BACKFILL: &str = "backfill";

/// Canonical equality key. Utf8/LargeUtf8/Utf8View → Utf8 bytes; Int32/Int64 → Int64.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IndexKey {
    Utf8(Box<[u8]>),
    Int64(i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexValueType {
    Utf8,
    Int64,
}

impl IndexValueType {
    /// None = not indexable.
    pub fn for_arrow(dt: &DataType) -> Option<Self> {
        match dt {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(Self::Utf8),
            DataType::Int32 | DataType::Int64 => Some(Self::Int64),
            _ => None,
        }
    }

    /// `"utf8"` | `"int64"`, the catalog's `value_type`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Utf8 => "utf8",
            Self::Int64 => "int64",
        }
    }

    /// Inverse of [`Self::as_str`].
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "utf8" => Some(Self::Utf8),
            "int64" => Some(Self::Int64),
            _ => None,
        }
    }
}

impl IndexKey {
    /// None for NULL or a type that does not canonicalise to `ty`.
    pub fn from_scalar(v: &ScalarValue, ty: IndexValueType) -> Option<Self> {
        let _ = (v, ty);
        todo!()
    }
}

/// One `ext_ducklake_file_index` row.
#[derive(Clone, Debug, PartialEq)]
pub struct FileIndexMeta {
    /// As in `ducklake_data_file.path`.
    pub path: String,
    pub file_size_bytes: i64,
    pub column_id: i64,
    /// Relative to the data file's directory.
    pub index_path: String,
    pub index_size_bytes: i64,
    pub format_version: i32,
    pub value_type: IndexValueType,
    pub row_count: i64,
    pub row_group_rows: Vec<u64>,
    /// Sorted by key; count > row_count / HEAVY_HITTER_DIVISOR.
    pub heavy_hitters: Vec<(IndexKey, u64)>,
}

impl FileIndexMeta {
    /// The key's posting count if it is a heavy hitter in this file.
    pub fn heavy_hitter_count(&self, key: &IndexKey) -> Option<u64> {
        let _ = key;
        todo!()
    }
}

/// Catalog column `row_group_rows`: JSON `[u64,...]`.
pub fn row_group_rows_to_json(rows: &[u64]) -> String {
    let _ = rows;
    todo!()
}

/// Inverse of [`row_group_rows_to_json`].
pub fn row_group_rows_from_json(s: &str) -> Result<Vec<u64>> {
    let _ = s;
    todo!()
}

/// Catalog column `heavy_hitters`: JSON `[[value, count],...]`; Utf8 values
/// are base64 (standard alphabet, padded), Int64 values are JSON numbers.
pub fn heavy_hitters_to_json(hh: &[(IndexKey, u64)]) -> String {
    let _ = hh;
    todo!()
}

/// Inverse of [`heavy_hitters_to_json`]; `ty` decides how values decode.
pub fn heavy_hitters_from_json(s: &str, ty: IndexValueType) -> Result<Vec<(IndexKey, u64)>> {
    let _ = (s, ty);
    todo!()
}

/// Per-file, per-column builder fed in write order.
pub struct PostingBuilder {
    column_id: i64,
    value_type: IndexValueType,
    rows_seen: u64,
}

impl PostingBuilder {
    pub fn new(column_id: i64, value_type: IndexValueType) -> Self {
        Self {
            column_id,
            value_type,
            rows_seen: 0,
        }
    }

    pub fn column_id(&self) -> i64 {
        self.column_id
    }

    pub fn value_type(&self) -> IndexValueType {
        self.value_type
    }

    /// `column` is this batch's indexed column; NULLs get no posting.
    pub fn observe(&mut self, column: &ArrayRef) -> Result<()> {
        let _ = column;
        todo!()
    }

    pub fn rows_seen(&self) -> u64 {
        self.rows_seen
    }

    /// Errors if `row_group_rows` does not sum to [`Self::rows_seen`].
    pub fn finish(self, row_group_rows: Vec<u64>) -> Result<BuiltIndex> {
        let _ = row_group_rows;
        todo!()
    }
}

pub struct BuiltIndex {
    /// The parquet index object.
    pub bytes: bytes::Bytes,
    pub row_count: u64,
    pub row_group_rows: Vec<u64>,
    pub heavy_hitters: Vec<(IndexKey, u64)>,
}

impl BuiltIndex {
    /// `format_version` is [`FILE_INDEX_FORMAT_VERSION`]; `index_size_bytes`
    /// is `bytes.len()`.
    pub fn meta(
        &self,
        path: &str,
        file_size_bytes: i64,
        column_id: i64,
        value_type: IndexValueType,
        index_path: String,
    ) -> FileIndexMeta {
        let _ = (path, file_size_bytes, column_id, value_type, index_path);
        todo!()
    }
}

/// `"<data_file_path>.c<column_id>.v1.pidx"`: same directory, same form
/// (relative stays relative, absolute stays absolute).
pub fn index_object_path(data_file_path: &str, column_id: i64) -> String {
    format!("{data_file_path}.c{column_id}.v{FILE_INDEX_FORMAT_VERSION}.{INDEX_SUFFIX}")
}

/// The object's file name alone (what [`FileIndexMeta::index_path`] stores,
/// since it is relative to the data file's directory).
pub fn index_file_name(data_file_path: &str, column_id: i64) -> String {
    let base = data_file_path.rsplit('/').next().unwrap_or(data_file_path);
    index_object_path(base, column_id)
}

/// Sorted, de-duplicated file positions.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Postings {
    pub rows: Vec<u32>,
}

/// Point lookup of `keys` (union) in one index object. Reads the footer (via
/// `metadata_cache`), the page index and only the pages whose min/max cover a key.
/// Errors on version/footer mismatch; callers treat an error as "no index".
pub async fn lookup_postings(
    store: Arc<dyn ObjectStore>,
    index_location: &object_store::path::Path,
    meta: &FileIndexMeta,
    keys: &[IndexKey],
    metadata_cache: Option<Arc<FileMetadataCache>>,
) -> Result<Postings> {
    let _ = (store, index_location, meta, keys, metadata_cache);
    todo!()
}

/// [`lookup_postings`] through `cache`: keys already cached are not read;
/// misses are read in one lookup and inserted per key.
pub async fn lookup_postings_cached(
    cache: &PostingCache,
    store: Arc<dyn ObjectStore>,
    index_location: &object_store::path::Path,
    meta: &FileIndexMeta,
    keys: &[IndexKey],
    metadata_cache: Option<Arc<FileMetadataCache>>,
) -> Result<Postings> {
    let _ = (cache, store, index_location, meta, keys, metadata_cache);
    todo!()
}

/// `Skip` for row groups without postings, `Selection` otherwise. Panics never;
/// returns None if `row_group_rows` doesn't sum to `meta.row_count`, or a
/// posting lies past the end.
pub fn access_plan(postings: &Postings, row_group_rows: &[u64]) -> Option<ParquetAccessPlan> {
    let _ = (postings, row_group_rows);
    todo!()
}

/// Marker for Phase 2: this file's selection is exact for `column_id IN keys`.
#[derive(Clone, Debug, PartialEq)]
pub struct FileIndexCoverage {
    pub column_id: i64,
    pub keys: Vec<IndexKey>,
}

/// Process-wide posting cache (byte-weighted), keyed `(index_path, key)`.
/// Postings of an immutable index object never go stale; callers key by the
/// resolved object location so two tables never share an entry.
pub struct PostingCache {
    max_bytes: u64,
}

impl PostingCache {
    pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;

    pub fn new(max_bytes: u64) -> Self {
        Self { max_bytes }
    }

    /// 256 MiB default.
    pub fn global() -> &'static PostingCache {
        todo!()
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub fn get(&self, index_path: &str, key: &IndexKey) -> Option<Arc<Postings>> {
        let _ = (index_path, key);
        todo!()
    }

    pub fn insert(&self, index_path: &str, key: IndexKey, postings: Arc<Postings>) {
        let _ = (index_path, key, postings);
        todo!()
    }
}
