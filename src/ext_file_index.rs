//! PLACEHOLDER for W1's `ext_file_index` format library, so the write path
//! compiles and its tests run before W1's branch lands. Replaced wholesale by
//! W1's file on merge; only the shared-interface signatures matter here.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, Int64Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};

pub const FILE_INDEX_FORMAT_VERSION: i32 = 1;
pub const HEAVY_HITTER_DIVISOR: u64 = 4096;
pub const INDEX_SUFFIX: &str = "pidx";
pub const BUILT_BY_WRITE: &str = "write";
pub const BUILT_BY_COMPACT: &str = "compact";
pub const BUILT_BY_BACKFILL: &str = "backfill";

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
    pub fn for_arrow(dt: &DataType) -> Option<Self> {
        match dt {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(Self::Utf8),
            DataType::Int32 | DataType::Int64 => Some(Self::Int64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Utf8 => "utf8",
            Self::Int64 => "int64",
        }
    }
}

impl IndexKey {
    pub fn from_scalar(v: &ScalarValue, ty: IndexValueType) -> Option<Self> {
        match (v, ty) {
            (
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)),
                IndexValueType::Utf8,
            ) => Some(Self::Utf8(s.as_bytes().into())),
            (ScalarValue::Int64(Some(i)), IndexValueType::Int64) => Some(Self::Int64(*i)),
            (ScalarValue::Int32(Some(i)), IndexValueType::Int64) => Some(Self::Int64(*i as i64)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FileIndexMeta {
    pub path: String,
    pub file_size_bytes: i64,
    pub column_id: i64,
    pub index_path: String,
    pub index_size_bytes: i64,
    pub format_version: i32,
    pub value_type: IndexValueType,
    pub row_count: i64,
    pub row_group_rows: Vec<u64>,
    pub heavy_hitters: Vec<(IndexKey, u64)>,
}

#[derive(Debug)]
pub struct PostingBuilder {
    value_type: IndexValueType,
    postings: HashMap<IndexKey, Vec<u32>>,
    rows_seen: u64,
}

impl PostingBuilder {
    pub fn new(_column_id: i64, value_type: IndexValueType) -> Self {
        Self {
            value_type,
            postings: HashMap::new(),
            rows_seen: 0,
        }
    }

    pub fn observe(&mut self, column: &ArrayRef) -> Result<()> {
        let base = self.rows_seen;
        let column = match (self.value_type, column.data_type()) {
            (IndexValueType::Utf8, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
                arrow::compute::cast(column, &DataType::Utf8)?
            },
            (IndexValueType::Int64, DataType::Int32 | DataType::Int64) => {
                arrow::compute::cast(column, &DataType::Int64)?
            },
            (_, other) => {
                return Err(DataFusionError::Internal(format!(
                    "posting builder: unexpected type {other}"
                )));
            },
        };
        for i in 0..column.len() {
            if column.is_null(i) {
                continue;
            }
            let key = match self.value_type {
                IndexValueType::Utf8 => {
                    IndexKey::Utf8(column.as_string::<i32>().value(i).as_bytes().into())
                },
                IndexValueType::Int64 => {
                    IndexKey::Int64(column.as_primitive::<Int64Type>().value(i))
                },
            };
            let row = u32::try_from(base + i as u64)
                .map_err(|_| DataFusionError::Internal("file too large to index".into()))?;
            self.postings.entry(key).or_default().push(row);
        }
        self.rows_seen += column.len() as u64;
        let _ = std::marker::PhantomData::<Int32Type>;
        Ok(())
    }

    pub fn rows_seen(&self) -> u64 {
        self.rows_seen
    }

    pub fn finish(self, row_group_rows: Vec<u64>) -> Result<BuiltIndex> {
        let mut keys: Vec<_> = self.postings.into_iter().collect();
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        let threshold = self.rows_seen / HEAVY_HITTER_DIVISOR;
        let heavy_hitters = keys
            .iter()
            .filter(|(_, rows)| rows.len() as u64 > threshold)
            .map(|(k, rows)| (k.clone(), rows.len() as u64))
            .collect();
        let mut rows = Vec::new();
        let mut values: Vec<IndexKey> = Vec::new();
        for (k, r) in keys {
            for row in r {
                values.push(k.clone());
                rows.push(row);
            }
        }
        let value: ArrayRef = match self.value_type {
            IndexValueType::Utf8 => Arc::new(BinaryArray::from_iter_values(values.iter().map(
                |k| match k {
                    IndexKey::Utf8(b) => b.to_vec(),
                    IndexKey::Int64(_) => unreachable!(),
                },
            ))),
            IndexValueType::Int64 => Arc::new(Int64Array::from_iter_values(values.iter().map(
                |k| match k {
                    IndexKey::Int64(i) => *i,
                    IndexKey::Utf8(_) => unreachable!(),
                },
            ))),
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", value.data_type().clone(), false),
            Field::new("row", DataType::UInt32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![value, Arc::new(UInt32Array::from(rows))],
        )?;
        let mut buf = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(BuiltIndex {
            bytes: bytes::Bytes::from(buf),
            row_count: self.rows_seen,
            row_group_rows,
            heavy_hitters,
        })
    }
}

#[derive(Debug)]
pub struct BuiltIndex {
    pub bytes: bytes::Bytes,
    pub row_count: u64,
    pub row_group_rows: Vec<u64>,
    pub heavy_hitters: Vec<(IndexKey, u64)>,
}

impl BuiltIndex {
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
        }
    }
}

pub fn index_object_path(data_file_path: &str, column_id: i64) -> String {
    format!("{data_file_path}.c{column_id}.v{FILE_INDEX_FORMAT_VERSION}.{INDEX_SUFFIX}")
}
