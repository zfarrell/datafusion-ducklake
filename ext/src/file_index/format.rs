//! Per-file secondary index ("file index") format.
//!
//! One sidecar object per (data file, indexed column): a parquet-encoded
//! posting list of `(value, row)` sorted by `(value, row)`, where `row` is the
//! row's position in the data file. The file's fact (see [`FileIndexMeta`])
//! carries the per-row-group row counts and the
//! heavy-hitter list so the read side can decide per file without touching
//! the object.
//!
//! The object is named `<data file>.c<column_id>.v1.pidx` and must never end
//! in `.parquet`: the orphan sweep deletes every unreferenced `.parquet`
//! under the data path.
//!
//! Object layout: columns `value` (Binary for Utf8 keys, Int64 for Int64
//! keys) and `row` (UInt32), ZSTD(3), 8K-row pages, page statistics and the
//! page index on, 1M-row row groups. Footer key/values carry the format
//! version, column id, value type and row count, all checked on read.
//!
//! This module has no catalog or scan logic; it is the format library shared
//! by the write path (posting builder) and the read path (lookups, access
//! plans, posting cache).

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, AsArray, BinaryBuilder, Int64Builder, RecordBatch, UInt32Array,
    UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema, SchemaRef};
use base64::Engine;
use chrono::{DateTime, Utc};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion_datasource_parquet::metadata::DFParquetMetadata;
use datafusion_datasource_parquet::{ParquetAccessPlan, RowGroupAccess};
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::basic::{Compression, Encoding, Type as PhysicalType, ZstdLevel};
use parquet::file::metadata::{KeyValue, PageIndexPolicy, ParquetMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

pub const FILE_INDEX_FORMAT_VERSION: i32 = 1;
/// A value is heavy in a file if its count > `row_count / HEAVY_HITTER_DIVISOR`
/// (and > [`HEAVY_HITTER_MIN_COUNT`]).
pub const HEAVY_HITTER_DIVISOR: u64 = 4096;
/// Floor on the heavy-hitter threshold, so a small file (where
/// `row_count / 4096` is 0) does not list every value it holds.
pub const HEAVY_HITTER_MIN_COUNT: u64 = 1024;
/// At most this many heavy hitters (the highest counts) are kept, to bound
/// the catalog row. An unlisted heavy value only gets a selection it may
/// not profit from; results are unaffected.
pub const HEAVY_HITTER_MAX: usize = 1024;
/// Never ".parquet" (orphan sweep).
pub const INDEX_SUFFIX: &str = "pidx";
pub const BUILT_BY_WRITE: &str = "write";
pub const BUILT_BY_COMPACT: &str = "compact";
pub const BUILT_BY_BACKFILL: &str = "backfill";

/// Rows per data page in the index object.
const PAGE_ROWS: usize = 8192;
/// Rows per row group in the index object.
const ROW_GROUP_ROWS: usize = 1 << 20;
/// Postings per record batch handed to the parquet writer.
const WRITE_BATCH_ROWS: usize = 64 * 1024;

const KV_VERSION: &str = "ext_file_index.version";
const KV_COLUMN_ID: &str = "ext_file_index.column_id";
const KV_VALUE_TYPE: &str = "ext_file_index.value_type";
const KV_ROW_COUNT: &str = "ext_file_index.row_count";
const KV_POSTINGS: &str = "ext_file_index.postings";

const VALUE_COL: &str = "value";
const ROW_COL: &str = "row";

fn err(msg: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("ext_file_index: {msg}"))
}

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
    /// None = not indexable. Dictionary columns index by their value type.
    pub fn for_arrow(dt: &DataType) -> Option<Self> {
        match dt {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(Self::Utf8),
            DataType::Int32 | DataType::Int64 => Some(Self::Int64),
            DataType::Dictionary(_, value) => Self::for_arrow(value),
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

    fn arrow_type(&self) -> DataType {
        match self {
            Self::Utf8 => DataType::Binary,
            Self::Int64 => DataType::Int64,
        }
    }

    fn physical_type(&self) -> PhysicalType {
        match self {
            Self::Utf8 => PhysicalType::BYTE_ARRAY,
            Self::Int64 => PhysicalType::INT64,
        }
    }
}

impl IndexKey {
    /// None for NULL or a type that does not canonicalise to `ty`.
    pub fn from_scalar(v: &ScalarValue, ty: IndexValueType) -> Option<Self> {
        match (v, ty) {
            (
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)),
                IndexValueType::Utf8,
            ) => Some(Self::Utf8(s.as_bytes().into())),
            (ScalarValue::Int32(Some(i)), IndexValueType::Int64) => Some(Self::Int64(*i as i64)),
            (ScalarValue::Int64(Some(i)), IndexValueType::Int64) => Some(Self::Int64(*i)),
            (ScalarValue::Dictionary(_, inner), _) => Self::from_scalar(inner, ty),
            _ => None,
        }
    }

    pub fn value_type(&self) -> IndexValueType {
        match self {
            Self::Utf8(_) => IndexValueType::Utf8,
            Self::Int64(_) => IndexValueType::Int64,
        }
    }

    /// Approximate heap + inline size, for cache weighing.
    fn approx_bytes(&self) -> usize {
        match self {
            Self::Utf8(b) => 16 + b.len(),
            Self::Int64(_) => 16,
        }
    }
}

/// One file index fact, as read.
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
    /// Sorted by key; count > max(row_count / HEAVY_HITTER_DIVISOR, HEAVY_HITTER_MIN_COUNT).
    pub heavy_hitters: Vec<(IndexKey, u64)>,
    /// Catalog column `bucket_counts` (NULL = none); see [`BucketCounts`].
    pub bucket_counts: Option<BucketCounts>,
}

/// Grain of hourly bucket counts, in microseconds.
pub const BUCKET_GRAIN_HOUR_US: i64 = 3_600_000_000;
/// Cap on `(key, bucket)` pairs in one catalog row.
pub const MAX_BUCKET_COUNT_ENTRIES: usize = 16_384;

/// Per-file counts of rows per (heavy key, timestamp bucket) (W7, stretch).
/// `count` is the exact number of rows in the data file with key = k and a
/// non-null `ts` in `[bucket_start, bucket_start + grain_us)`, UTC epoch µs.
/// Deletes are not applied. A key not listed has no counts in this file.
#[derive(Clone, Debug, PartialEq)]
pub struct BucketCounts {
    pub ts_column_id: i64,
    pub grain_us: i64,
    /// Sorted by key; buckets ascending by start.
    pub keys: Vec<(IndexKey, Vec<(i64, u64)>)>,
}

impl FileIndexMeta {
    /// About the bytes this meta holds, for bounding what is kept in memory.
    pub(crate) fn heap_bytes(&self) -> usize {
        let key = |key: &IndexKey| match key {
            IndexKey::Utf8(bytes) => 24 + bytes.len(),
            IndexKey::Int64(_) => 24,
        };
        self.path.len()
            + self.index_path.len()
            + self.row_group_rows.len() * 8
            + self
                .heavy_hitters
                .iter()
                .map(|(k, _)| key(k) + 8)
                .sum::<usize>()
            + self.bucket_counts.as_ref().map_or(0, |counts| {
                counts
                    .keys
                    .iter()
                    .map(|(k, buckets)| key(k) + 24 + buckets.len() * 16)
                    .sum()
            })
    }

    /// The key's posting count if it is a heavy hitter in this file.
    pub fn heavy_hitter_count(&self, key: &IndexKey) -> Option<u64> {
        self.heavy_hitters
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|i| self.heavy_hitters[i].1)
    }
}

/// Catalog column `row_group_rows`: JSON `[u64,...]`.
pub fn row_group_rows_to_json(rows: &[u64]) -> String {
    serde_json::to_string(rows).expect("u64 slice serialises")
}

/// Inverse of [`row_group_rows_to_json`].
pub fn row_group_rows_from_json(s: &str) -> Result<Vec<u64>> {
    serde_json::from_str(s).map_err(|e| err(format!("bad row_group_rows JSON: {e}")))
}

/// Catalog column `heavy_hitters`: JSON `[[value, count],...]`; Utf8 values
/// are base64 (standard alphabet, padded), Int64 values are JSON numbers.
pub fn heavy_hitters_to_json(hh: &[(IndexKey, u64)]) -> String {
    let arr: Vec<serde_json::Value> = hh
        .iter()
        .map(|(k, c)| {
            let v = match k {
                IndexKey::Utf8(b) => {
                    serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
                },
                IndexKey::Int64(i) => serde_json::Value::from(*i),
            };
            serde_json::Value::Array(vec![v, serde_json::Value::from(*c)])
        })
        .collect();
    serde_json::Value::Array(arr).to_string()
}

/// Inverse of [`heavy_hitters_to_json`]; `ty` decides how values decode.
/// The result is sorted by key.
pub fn heavy_hitters_from_json(s: &str, ty: IndexValueType) -> Result<Vec<(IndexKey, u64)>> {
    let raw: Vec<(serde_json::Value, u64)> =
        serde_json::from_str(s).map_err(|e| err(format!("bad heavy_hitters JSON: {e}")))?;
    let mut out = Vec::with_capacity(raw.len());
    for (v, c) in raw {
        let key = match (ty, &v) {
            (IndexValueType::Utf8, serde_json::Value::String(b64)) => IndexKey::Utf8(
                base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|e| err(format!("bad heavy hitter base64: {e}")))?
                    .into(),
            ),
            (IndexValueType::Int64, serde_json::Value::Number(n)) => IndexKey::Int64(
                n.as_i64()
                    .ok_or_else(|| err(format!("heavy hitter {n} is not an i64")))?,
            ),
            _ => return Err(err(format!("heavy hitter {v} is not a {}", ty.as_str()))),
        };
        out.push((key, c));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

enum PostingMap {
    Utf8(HashMap<Box<[u8]>, Vec<u32>>),
    Int64(HashMap<i64, Vec<u32>>),
}

/// Per-file, per-column builder fed in write order.
pub struct PostingBuilder {
    column_id: i64,
    value_type: IndexValueType,
    rows_seen: u64,
    map: PostingMap,
    /// Opt-in (W7): per-row time buckets, histogrammed per heavy hitter at
    /// `finish` into [`BuiltIndex::bucket_counts`].
    buckets: Option<crate::file_index::counts::BucketCountBuilder>,
}

impl std::fmt::Debug for PostingBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys = match &self.map {
            PostingMap::Utf8(m) => m.len(),
            PostingMap::Int64(m) => m.len(),
        };
        f.debug_struct("PostingBuilder")
            .field("column_id", &self.column_id)
            .field("value_type", &self.value_type)
            .field("rows_seen", &self.rows_seen)
            .field("keys", &keys)
            .finish()
    }
}

impl PostingBuilder {
    pub fn new(column_id: i64, value_type: IndexValueType) -> Self {
        let map = match value_type {
            IndexValueType::Utf8 => PostingMap::Utf8(HashMap::new()),
            IndexValueType::Int64 => PostingMap::Int64(HashMap::new()),
        };
        Self {
            column_id,
            value_type,
            rows_seen: 0,
            map,
            buckets: None,
        }
    }

    /// Also count each heavy hitter's rows per `grain_us` bucket of the time
    /// column `ts_column_id`: feed that column to [`Self::observe_ts`] for
    /// every batch given to [`Self::observe`]. Costs 4 B per row.
    #[must_use]
    pub fn with_bucket_counts(mut self, ts_column_id: i64, grain_us: i64) -> Self {
        self.buckets = Some(crate::file_index::counts::BucketCountBuilder::new(
            ts_column_id,
            grain_us,
        ));
        self
    }

    /// The batch's time column, when counting buckets. Never errors: a column
    /// it cannot bucket leaves the file without counts.
    pub fn observe_ts(&mut self, ts: &ArrayRef) {
        if let Some(buckets) = &mut self.buckets {
            buckets.observe(ts);
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
        let len = column.len() as u64;
        // Positions are u32; the last position is rows_seen + len - 1.
        if self.rows_seen + len > u64::from(u32::MAX) + 1 {
            return Err(err("data file exceeds 2^32 rows"));
        }
        let column = match column.data_type() {
            DataType::Dictionary(_, value) => arrow::compute::cast(column, value)?,
            _ => Arc::clone(column),
        };
        let base = self.rows_seen as u32;
        fn add_bytes<'a>(
            map: &mut HashMap<Box<[u8]>, Vec<u32>>,
            base: u32,
            it: impl Iterator<Item = Option<&'a [u8]>>,
        ) {
            for (i, v) in it.enumerate() {
                if let Some(v) = v {
                    let pos = base + i as u32;
                    match map.get_mut(v) {
                        Some(rows) => rows.push(pos),
                        None => {
                            map.insert(v.into(), vec![pos]);
                        },
                    }
                }
            }
        }
        fn add_ints(
            map: &mut HashMap<i64, Vec<u32>>,
            base: u32,
            it: impl Iterator<Item = Option<i64>>,
        ) {
            for (i, v) in it.enumerate() {
                if let Some(v) = v {
                    map.entry(v).or_default().push(base + i as u32);
                }
            }
        }
        match (&mut self.map, column.data_type()) {
            (PostingMap::Utf8(m), DataType::Utf8) => add_bytes(
                m,
                base,
                column
                    .as_string::<i32>()
                    .iter()
                    .map(|v| v.map(str::as_bytes)),
            ),
            (PostingMap::Utf8(m), DataType::LargeUtf8) => add_bytes(
                m,
                base,
                column
                    .as_string::<i64>()
                    .iter()
                    .map(|v| v.map(str::as_bytes)),
            ),
            (PostingMap::Utf8(m), DataType::Utf8View) => add_bytes(
                m,
                base,
                column.as_string_view().iter().map(|v| v.map(str::as_bytes)),
            ),
            (PostingMap::Int64(m), DataType::Int32) => add_ints(
                m,
                base,
                column
                    .as_primitive::<Int32Type>()
                    .iter()
                    .map(|v| v.map(i64::from)),
            ),
            (PostingMap::Int64(m), DataType::Int64) => {
                add_ints(m, base, column.as_primitive::<Int64Type>().iter())
            },
            (_, dt) => {
                return Err(err(format!(
                    "column type {dt} does not match index value type {}",
                    self.value_type.as_str()
                )));
            },
        }
        self.rows_seen += len;
        Ok(())
    }

    pub fn rows_seen(&self) -> u64 {
        self.rows_seen
    }

    /// Errors if `row_group_rows` does not sum to [`Self::rows_seen`].
    pub fn finish(self, row_group_rows: Vec<u64>) -> Result<BuiltIndex> {
        let sum: u64 = row_group_rows.iter().sum();
        if sum != self.rows_seen {
            return Err(err(format!(
                "row_group_rows sum {sum} != rows observed {}",
                self.rows_seen
            )));
        }
        let mut entries: Vec<(IndexKey, Vec<u32>)> = match self.map {
            PostingMap::Utf8(m) => m.into_iter().map(|(k, v)| (IndexKey::Utf8(k), v)).collect(),
            PostingMap::Int64(m) => m
                .into_iter()
                .map(|(k, v)| (IndexKey::Int64(k), v))
                .collect(),
        };
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let threshold = heavy_hitter_threshold(self.rows_seen);
        let mut heavy: Vec<(IndexKey, u64)> = entries
            .iter()
            .filter(|(_, rows)| rows.len() as u64 > threshold)
            .map(|(k, rows)| (k.clone(), rows.len() as u64))
            .collect();
        if heavy.len() > HEAVY_HITTER_MAX {
            heavy.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            heavy.truncate(HEAVY_HITTER_MAX);
            heavy.sort_by(|a, b| a.0.cmp(&b.0));
        }

        // Counts only when every row was bucketed, in step with the postings.
        let bucket_counts = self
            .buckets
            .filter(|b| b.rows_seen() == self.rows_seen)
            .and_then(|b| {
                b.finish(heavy.iter().filter_map(|(key, _)| {
                    let i = entries.binary_search_by(|(k, _)| k.cmp(key)).ok()?;
                    Some((key.clone(), entries[i].1.as_slice()))
                }))
            });
        let bytes = write_object(self.column_id, self.value_type, self.rows_seen, &entries)?;
        Ok(BuiltIndex {
            bytes,
            row_count: self.rows_seen,
            row_group_rows,
            heavy_hitters: heavy,
            bucket_counts,
        })
    }
}

/// A value is a heavy hitter in a file of `row_count` rows if its count
/// exceeds this.
pub fn heavy_hitter_threshold(row_count: u64) -> u64 {
    (row_count / HEAVY_HITTER_DIVISOR).max(HEAVY_HITTER_MIN_COUNT)
}

fn index_schema(ty: IndexValueType) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(VALUE_COL, ty.arrow_type(), false),
        Field::new(ROW_COL, DataType::UInt32, false),
    ]))
}

fn write_object(
    column_id: i64,
    ty: IndexValueType,
    row_count: u64,
    entries: &[(IndexKey, Vec<u32>)],
) -> Result<bytes::Bytes> {
    let postings: usize = entries.iter().map(|(_, r)| r.len()).sum();
    let schema = index_schema(ty);
    // Dictionary only pays when values repeat a lot; otherwise the sorted
    // values delta-encode well.
    let dictionary = (entries.len() as f64) < 0.2 * postings as f64;
    let mut props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("valid level"),
        ))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_data_page_row_count_limit(PAGE_ROWS)
        .set_write_batch_size(1024)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_dictionary_enabled(ROW_COL.into(), false)
        .set_column_encoding(ROW_COL.into(), Encoding::DELTA_BINARY_PACKED)
        .set_key_value_metadata(Some(vec![
            KeyValue::new(KV_VERSION.into(), FILE_INDEX_FORMAT_VERSION.to_string()),
            KeyValue::new(KV_COLUMN_ID.into(), column_id.to_string()),
            KeyValue::new(KV_VALUE_TYPE.into(), ty.as_str().to_string()),
            KeyValue::new(KV_ROW_COUNT.into(), row_count.to_string()),
            KeyValue::new(KV_POSTINGS.into(), postings.to_string()),
        ]));
    if dictionary {
        props = props.set_column_dictionary_enabled(VALUE_COL.into(), true);
    } else {
        let enc = match ty {
            IndexValueType::Utf8 => Encoding::DELTA_BYTE_ARRAY,
            IndexValueType::Int64 => Encoding::DELTA_BINARY_PACKED,
        };
        props = props
            .set_column_dictionary_enabled(VALUE_COL.into(), false)
            .set_column_encoding(VALUE_COL.into(), enc);
    }

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(props.build()))?;
    let mut rows = UInt32Builder::with_capacity(WRITE_BATCH_ROWS.min(postings));
    let mut values: ValueBuilder = match ty {
        IndexValueType::Utf8 => ValueBuilder::Utf8(BinaryBuilder::new()),
        IndexValueType::Int64 => ValueBuilder::Int64(Int64Builder::new()),
    };
    let mut flush = |values: &mut ValueBuilder, rows: &mut UInt32Builder| -> Result<()> {
        if rows.len() == 0 {
            return Ok(());
        }
        let v: ArrayRef = match values {
            ValueBuilder::Utf8(b) => Arc::new(b.finish()),
            ValueBuilder::Int64(b) => Arc::new(b.finish()),
        };
        let r: ArrayRef = Arc::new(rows.finish());
        writer.write(&RecordBatch::try_new(Arc::clone(&schema), vec![v, r])?)?;
        Ok(())
    };
    for (key, positions) in entries {
        for &pos in positions {
            match (&mut values, key) {
                (ValueBuilder::Utf8(b), IndexKey::Utf8(k)) => b.append_value(k),
                (ValueBuilder::Int64(b), IndexKey::Int64(k)) => b.append_value(*k),
                _ => return Err(err("key type does not match index value type")),
            }
            rows.append_value(pos);
            if rows.len() >= WRITE_BATCH_ROWS {
                flush(&mut values, &mut rows)?;
            }
        }
    }
    flush(&mut values, &mut rows)?;

    writer.close()?;
    Ok(bytes::Bytes::from(buf))
}

enum ValueBuilder {
    Utf8(BinaryBuilder),
    Int64(Int64Builder),
}

pub struct BuiltIndex {
    /// The parquet index object.
    pub bytes: bytes::Bytes,
    pub row_count: u64,
    pub row_group_rows: Vec<u64>,
    pub heavy_hitters: Vec<(IndexKey, u64)>,
    /// None unless bucket counting was requested (W7).
    pub bucket_counts: Option<BucketCounts>,
}

impl BuiltIndex {
    /// `format_version` is [`FILE_INDEX_FORMAT_VERSION`]; `index_size_bytes`
    /// is `bytes.len()`. `column_id` and `value_type` must be the builder's:
    /// the object's footer records them and lookups reject a mismatch.
    pub fn meta(
        &self,
        path: &str,
        file_size_bytes: i64,
        column_id: i64,
        value_type: IndexValueType,
        index_path: String,
    ) -> FileIndexMeta {
        FileIndexMeta {
            path: path.to_string(),
            file_size_bytes,
            column_id,
            index_path,
            index_size_bytes: self.bytes.len() as i64,
            format_version: FILE_INDEX_FORMAT_VERSION,
            value_type,
            row_count: self.row_count as i64,
            row_group_rows: self.row_group_rows.clone(),
            heavy_hitters: self.heavy_hitters.clone(),
            bucket_counts: self.bucket_counts.clone(),
        }
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

impl Postings {
    /// Sorted, de-duplicated union.
    fn union<'a>(parts: impl IntoIterator<Item = &'a [u32]>) -> Self {
        let mut rows: Vec<u32> = parts.into_iter().flatten().copied().collect();
        rows.sort_unstable();
        rows.dedup();
        Self {
            rows,
        }
    }
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
    let mut keys = keys.to_vec();
    keys.sort_unstable();
    keys.dedup();
    let per_key = read_keys(store, index_location, meta, &keys, metadata_cache).await?;
    Ok(Postings::union(per_key.iter().map(Vec::as_slice)))
}

/// [`lookup_postings`] through `cache`: keys already cached are not read;
/// misses are read in one lookup and inserted per key (absent keys too, as
/// empty postings).
pub async fn lookup_postings_cached(
    cache: &PostingCache,
    store: Arc<dyn ObjectStore>,
    index_location: &object_store::path::Path,
    meta: &FileIndexMeta,
    keys: &[IndexKey],
    metadata_cache: Option<Arc<FileMetadataCache>>,
) -> Result<Postings> {
    let mut keys = keys.to_vec();
    keys.sort_unstable();
    keys.dedup();
    let path = index_location.as_ref();
    let mut hits: Vec<Arc<Postings>> = Vec::with_capacity(keys.len());
    let mut misses: Vec<IndexKey> = Vec::new();
    for key in keys {
        match cache.get(path, &key) {
            Some(p) => hits.push(p),
            None => misses.push(key),
        }
    }
    let mut fetched: Vec<Vec<u32>> = Vec::new();
    if !misses.is_empty() {
        fetched = read_keys(store, index_location, meta, &misses, metadata_cache).await?;
        for (key, rows) in misses.into_iter().zip(&fetched) {
            cache.insert(
                path,
                key,
                Arc::new(Postings {
                    rows: rows.clone(),
                }),
            );
        }
    }
    Ok(Postings::union(
        hits.iter()
            .map(|p| p.rows.as_slice())
            .chain(fetched.iter().map(Vec::as_slice)),
    ))
}

/// The `ObjectMeta` shape that shares `FileMetadataCache` entries across
/// readers: epoch `last_modified`, no e_tag (see `table.rs`'s
/// `epoch_object_meta`).
fn epoch_object_meta(location: object_store::path::Path, size: u64) -> ObjectMeta {
    ObjectMeta {
        location,
        last_modified: DateTime::<Utc>::from_timestamp_nanos(0),
        size,
        e_tag: None,
        version: None,
    }
}

fn kv<'a>(md: &'a ParquetMetaData, key: &str) -> Option<&'a str> {
    md.file_metadata()
        .key_value_metadata()?
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_deref())
}

/// Checks the object against its catalog row. Any mismatch is an error.
fn validate_object(md: &ParquetMetaData, meta: &FileIndexMeta) -> Result<()> {
    let expect = |key: &str, want: String| -> Result<()> {
        match kv(md, key) {
            Some(got) if got == want => Ok(()),
            got => Err(err(format!("footer {key}={got:?}, expected {want:?}"))),
        }
    };
    expect(KV_VERSION, FILE_INDEX_FORMAT_VERSION.to_string())?;
    expect(KV_COLUMN_ID, meta.column_id.to_string())?;
    expect(KV_VALUE_TYPE, meta.value_type.as_str().to_string())?;
    expect(KV_ROW_COUNT, meta.row_count.to_string())?;
    let schema = md.file_metadata().schema_descr();
    let cols = schema.columns();
    if cols.len() != 2
        || cols[0].name() != VALUE_COL
        || cols[0].physical_type() != meta.value_type.physical_type()
        || cols[1].name() != ROW_COL
        || cols[1].physical_type() != PhysicalType::INT32
    {
        return Err(err("unexpected index object schema"));
    }
    let postings: i64 = md.row_groups().iter().map(|rg| rg.num_rows()).sum();
    match kv(md, KV_POSTINGS).and_then(|s| s.parse::<i64>().ok()) {
        Some(p) if p == postings => Ok(()),
        _ => Err(err("footer postings count does not match row groups")),
    }
}

/// Whether some key in sorted `keys` lies in `[min, max]`.
fn any_key_in<T: Ord + ?Sized, K>(
    keys: &[K],
    project: impl Fn(&K) -> &T,
    min: &T,
    max: &T,
) -> bool {
    let i = keys.partition_point(|k| project(k) < min);
    i < keys.len() && project(&keys[i]) <= max
}

/// Per row group, the row ranges (within the group) of value pages whose
/// min/max may hold a key. Without a column index every row is a candidate.
fn candidate_ranges(md: &ParquetMetaData, keys: &[IndexKey]) -> Result<Vec<Vec<Range<usize>>>> {
    if md.num_row_groups() == 0 {
        // An empty object has no page index at all.
        return Ok(vec![]);
    }
    let column_index = md.column_index();
    let offset_index = md
        .offset_index()
        .ok_or_else(|| err("index object has no offset index"))?;
    let int_keys: Vec<i64> = keys
        .iter()
        .filter_map(|k| match k {
            IndexKey::Int64(i) => Some(*i),
            IndexKey::Utf8(_) => None,
        })
        .collect();
    let mut out = Vec::with_capacity(md.num_row_groups());
    for (rg_idx, rg) in md.row_groups().iter().enumerate() {
        let n = rg.num_rows() as usize;
        let locs = offset_index
            .get(rg_idx)
            .and_then(|c| c.first())
            .ok_or_else(|| err("offset index missing a row group"))?
            .page_locations();
        let ci = column_index
            .and_then(|c| c.get(rg_idx))
            .and_then(|c| c.first());
        let mut ranges: Vec<Range<usize>> = Vec::new();
        for (p, loc) in locs.iter().enumerate() {
            let start = loc.first_row_index as usize;
            let end = locs.get(p + 1).map_or(n, |l| l.first_row_index as usize);
            if start >= end || end > n {
                if start > end || end > n {
                    return Err(err("offset index page rows out of range"));
                }
                continue;
            }
            let hit = match ci {
                Some(ColumnIndexMetaData::BYTE_ARRAY(idx)) => {
                    match (idx.min_value(p), idx.max_value(p)) {
                        (Some(min), Some(max)) => any_key_in(
                            keys,
                            |k| match k {
                                IndexKey::Utf8(b) => b.as_ref(),
                                IndexKey::Int64(_) => &[][..],
                            },
                            min,
                            max,
                        ),
                        // All-null page: no postings there.
                        _ => false,
                    }
                },
                Some(ColumnIndexMetaData::INT64(idx)) => match (idx.min_value(p), idx.max_value(p))
                {
                    (Some(min), Some(max)) => any_key_in(&int_keys, |k| k, min, max),
                    _ => false,
                },
                // No usable statistics: read the page.
                _ => true,
            };
            if hit {
                match ranges.last_mut() {
                    Some(last) if last.end == start => last.end = end,
                    _ => ranges.push(start..end),
                }
            }
        }
        out.push(ranges);
    }
    Ok(out)
}

/// Feeds the parquet async reader from an `ObjectStore`, serving the
/// already-fetched metadata.
struct IndexObjectReader {
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    metadata: Arc<ParquetMetaData>,
}

impl AsyncFileReader for IndexObjectReader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        async move {
            let mut v = self
                .store
                .get_ranges(&self.location, &[range])
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
            v.pop()
                .ok_or_else(|| parquet::errors::ParquetError::General("empty range read".into()))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.location, &ranges)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let md = Arc::clone(&self.metadata);
        async move { Ok(md) }.boxed()
    }
}

/// Wraps a store so a ranged read that comes back shorter than requested
/// (the object is smaller than the catalog says) is an error.
#[derive(Debug)]
struct ExactRangeStore {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for ExactRangeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExactRangeStore({})", self.inner)
    }
}

fn short_read(location: &object_store::path::Path, want: u64, got: u64) -> object_store::Error {
    object_store::Error::Generic {
        store: "ext_file_index",
        source: format!("short read of {location}: wanted {want} bytes, got {got}").into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for ExactRangeStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let bounded = match &options.range {
            Some(object_store::GetRange::Bounded(r)) => Some(r.end - r.start),
            _ => None,
        };
        let res = self.inner.get_opts(location, options).await?;
        if let Some(want) = bounded {
            let got = res.range.end - res.range.start;
            if got != want {
                return Err(short_read(location, want, got));
            }
        }
        Ok(res)
    }

    async fn get_ranges(
        &self,
        location: &object_store::path::Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        let out = self.inner.get_ranges(location, ranges).await?;
        for (r, b) in ranges.iter().zip(&out) {
            if b.len() as u64 != r.end - r.start {
                return Err(short_read(location, r.end - r.start, b.len() as u64));
            }
        }
        Ok(out)
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Postings per key, in `keys` order (`keys` sorted, de-duplicated, and of
/// `meta.value_type`). An absent key gets an empty list.
async fn read_keys(
    store: Arc<dyn ObjectStore>,
    index_location: &object_store::path::Path,
    meta: &FileIndexMeta,
    keys: &[IndexKey],
    metadata_cache: Option<Arc<FileMetadataCache>>,
) -> Result<Vec<Vec<u32>>> {
    if meta.format_version != FILE_INDEX_FORMAT_VERSION {
        return Err(err(format!(
            "unsupported format_version {}",
            meta.format_version
        )));
    }
    if let Some(k) = keys.iter().find(|k| k.value_type() != meta.value_type) {
        return Err(err(format!(
            "key {k:?} is not a {}",
            meta.value_type.as_str()
        )));
    }
    let mut out = vec![Vec::new(); keys.len()];
    if keys.is_empty() {
        return Ok(out);
    }
    let size = u64::try_from(meta.index_size_bytes).map_err(|_| {
        err(format!(
            "negative index_size_bytes {}",
            meta.index_size_bytes
        ))
    })?;
    let object_meta = epoch_object_meta(index_location.clone(), size);
    // A catalog size larger than the object makes stores return short ranges,
    // which the parquet decoder asserts on (a panic, not an error).
    let store: Arc<dyn ObjectStore> = Arc::new(ExactRangeStore {
        inner: store,
    });
    let md = DFParquetMetadata::new(store.as_ref(), &object_meta)
        .with_file_metadata_cache(metadata_cache)
        .with_page_index_policy(Some(PageIndexPolicy::Required))
        .fetch_metadata()
        .await?;
    validate_object(&md, meta)?;

    let candidates = candidate_ranges(&md, keys)?;
    let mut row_groups = Vec::new();
    let mut selectors: Vec<RowSelector> = Vec::new();
    for (rg, ranges) in candidates.iter().enumerate() {
        if ranges.is_empty() {
            continue;
        }
        row_groups.push(rg);
        let n = md.row_group(rg).num_rows() as usize;
        let mut at = 0;
        for r in ranges {
            if r.start > at {
                selectors.push(RowSelector::skip(r.start - at));
            }
            selectors.push(RowSelector::select(r.end - r.start));
            at = r.end;
        }
        if at < n {
            selectors.push(RowSelector::skip(n - at));
        }
    }
    if row_groups.is_empty() {
        return Ok(out);
    }

    let reader = IndexObjectReader {
        store,
        location: index_location.clone(),
        metadata: Arc::clone(&md),
    };
    let arm = ArrowReaderMetadata::try_new(
        md,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
    )?;
    let mut stream = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arm)
        .with_row_groups(row_groups)
        .with_row_selection(RowSelection::from(selectors))
        .with_batch_size(PAGE_ROWS)
        .build()?;

    let row_count = u64::try_from(meta.row_count).unwrap_or(0);
    while let Some(batch) = stream.try_next().await? {
        let rows = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| err("row column is not UInt32"))?;
        let values = batch.column(0);
        let mut push = |i: usize, slot: usize| -> Result<()> {
            let r = rows.value(i);
            if u64::from(r) >= row_count {
                return Err(err(format!("posting {r} >= row_count {row_count}")));
            }
            out[slot].push(r);
            Ok(())
        };
        match meta.value_type {
            IndexValueType::Utf8 => {
                let v = values.as_binary::<i32>();
                for i in 0..batch.num_rows() {
                    let val = v.value(i);
                    if let Ok(slot) = keys.binary_search_by(|k| match k {
                        IndexKey::Utf8(b) => b.as_ref().cmp(val),
                        IndexKey::Int64(_) => std::cmp::Ordering::Less,
                    }) {
                        push(i, slot)?;
                    }
                }
            },
            IndexValueType::Int64 => {
                let v = values.as_primitive::<Int64Type>();
                for i in 0..batch.num_rows() {
                    let val = v.value(i);
                    if let Ok(slot) = keys.binary_search(&IndexKey::Int64(val)) {
                        push(i, slot)?;
                    }
                }
            },
        }
    }
    for rows in &mut out {
        // Written sorted within a key; a reorder means a corrupt object.
        if rows.windows(2).any(|w| w[0] >= w[1]) {
            return Err(err("postings not strictly increasing"));
        }
    }
    Ok(out)
}

/// `Skip` for row groups without postings, `Selection` otherwise (`Scan` when
/// every row of the group is selected). Panics never; returns None if
/// `postings` is not strictly increasing or a posting lies past the sum of
/// `row_group_rows`. Callers check that `row_group_rows` matches the data
/// file (`meta.row_count` and the footer) before attaching the plan.
pub fn access_plan(postings: &Postings, row_group_rows: &[u64]) -> Option<ParquetAccessPlan> {
    let rows = &postings.rows;
    if rows.windows(2).any(|w| w[0] >= w[1]) {
        return None;
    }
    let total: u64 = row_group_rows
        .iter()
        .try_fold(0u64, |a, &n| a.checked_add(n))?;
    if let Some(&last) = rows.last()
        && u64::from(last) >= total
    {
        return None;
    }
    let mut access = Vec::with_capacity(row_group_rows.len());
    let mut base = 0u64;
    let mut i = 0usize;
    for &n in row_group_rows {
        let end = base + n;
        let j = i + rows[i..].partition_point(|&r| u64::from(r) < end);
        let in_rg = &rows[i..j];
        if in_rg.is_empty() {
            access.push(RowGroupAccess::Skip);
        } else if in_rg.len() as u64 == n {
            access.push(RowGroupAccess::Scan);
        } else {
            let mut selectors = Vec::new();
            let mut at = 0usize;
            let mut k = 0;
            while k < in_rg.len() {
                let start = (u64::from(in_rg[k]) - base) as usize;
                let mut stop = start + 1;
                k += 1;
                while k < in_rg.len() && (u64::from(in_rg[k]) - base) as usize == stop {
                    stop += 1;
                    k += 1;
                }
                if start > at {
                    selectors.push(RowSelector::skip(start - at));
                }
                selectors.push(RowSelector::select(stop - start));
                at = stop;
            }
            if (at as u64) < n {
                selectors.push(RowSelector::skip(n as usize - at));
            }
            access.push(RowGroupAccess::Selection(RowSelection::from(selectors)));
        }
        base = end;
        i = j;
    }
    Some(ParquetAccessPlan::new(access))
}

/// Marker for Phase 2: this file's selection is exact for `column_id IN keys`.
#[derive(Clone, Debug, PartialEq)]
pub struct FileIndexCoverage {
    pub column_id: i64,
    /// The column's name in the table's physical schema (what a plan's
    /// `Column` carries).
    pub column_name: String,
    pub keys: Vec<IndexKey>,
}

/// Process-wide posting cache (byte-weighted), keyed `(index_path, key)`.
/// Postings of an immutable index object never go stale; callers key by the
/// resolved object location so two tables never share an entry.
pub struct PostingCache {
    max_bytes: u64,
    cache: moka::sync::Cache<(Arc<str>, IndexKey), Arc<Postings>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl PostingCache {
    pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;

    pub fn new(max_bytes: u64) -> Self {
        let cache = moka::sync::Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|k: &(Arc<str>, IndexKey), v: &Arc<Postings>| {
                let bytes = 64 + k.0.len() + k.1.approx_bytes() + 4 * v.rows.len();
                u32::try_from(bytes).unwrap_or(u32::MAX)
            })
            .build();
        Self {
            max_bytes,
            cache,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// 256 MiB default.
    pub fn global() -> &'static PostingCache {
        static GLOBAL: OnceLock<PostingCache> = OnceLock::new();
        GLOBAL.get_or_init(|| PostingCache::new(Self::DEFAULT_MAX_BYTES))
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Counts a hit or a miss.
    pub fn get(&self, index_path: &str, key: &IndexKey) -> Option<Arc<Postings>> {
        let got = self.cache.get(&(Arc::from(index_path), key.clone()));
        let counter = if got.is_some() {
            &self.hits
        } else {
            &self.misses
        };
        counter.fetch_add(1, AtomicOrdering::Relaxed);
        got
    }

    /// Per-key lookups served from the cache, since process start.
    pub fn hits(&self) -> u64 {
        self.hits.load(AtomicOrdering::Relaxed)
    }

    /// Per-key lookups that had to read the index object.
    pub fn misses(&self) -> u64 {
        self.misses.load(AtomicOrdering::Relaxed)
    }

    pub fn insert(&self, index_path: &str, key: IndexKey, postings: Arc<Postings>) {
        self.cache.insert((Arc::from(index_path), key), postings);
    }

    /// Weighted size of the live entries (after pending maintenance).
    pub fn weighted_size(&self) -> u64 {
        self.cache.run_pending_tasks();
        self.cache.weighted_size()
    }
}

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;
