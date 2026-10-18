//! EXPERIMENTAL, opt-in: the read side of the per-file secondary index (see
//! [`crate::ext_file_index`]).
//!
//! A scan whose filters hold `col = lit` or `col IN (lit, ...)` on an indexed
//! column looks up the value's postings in each file's index object, then:
//! - drops a file with no posting (whatever deletes or snapshot filters apply,
//!   since the index is exact for the file's rows);
//! - gives a file without deletes a [`ParquetAccessPlan`] that skips row
//!   groups and rows without postings.
//!
//! The filter always stays in the plan (the provider declares it `Inexact`),
//! so a selection only has to cover every match, never exactly them. Anything
//! unexpected (no catalog row, a stale size, an unreadable object, a mismatched
//! row count) means "scan the file as before".

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::Schema;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, InListExpr, Literal};
use datafusion_datasource_parquet::ParquetAccessPlan;

use crate::ext_file_index::{FileIndexCoverage, FileIndexMeta, IndexKey, IndexValueType};

/// Longest `IN` list the index answers; longer lists scan as before.
pub(crate) const MAX_IN_LIST: usize = 64;

/// Lookups in flight per scan.
pub(crate) const LOOKUP_CONCURRENCY: usize = 32;

/// Entries the per-table catalog memo keeps before it starts over.
pub(crate) const MEMO_MAX_ENTRIES: usize = 200_000;

/// An eligible conjunct: `column IN keys` (an equality is a one-key list).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexPredicate {
    /// Position in the table's physical schema.
    pub(crate) field_index: usize,
    pub(crate) column_id: i64,
    pub(crate) value_type: IndexValueType,
    /// Sorted, de-duplicated, non-empty.
    pub(crate) keys: Vec<IndexKey>,
}

/// The conjuncts of `conjuncts` (physical, over `physical_schema`) the index
/// can answer. `column_ids[i]` is the DuckLake column id of field `i`.
pub(crate) fn eligible_predicates(
    conjuncts: &[Arc<dyn PhysicalExpr>],
    physical_schema: &Schema,
    column_ids: &[i64],
) -> Vec<IndexPredicate> {
    conjuncts
        .iter()
        .filter_map(|conjunct| eligible_predicate(conjunct.as_ref(), physical_schema, column_ids))
        .collect()
}

fn eligible_predicate(
    expr: &dyn PhysicalExpr,
    physical_schema: &Schema,
    column_ids: &[i64],
) -> Option<IndexPredicate> {
    let (column, literals): (&Column, Vec<&Literal>) = if let Some(binary) =
        expr.downcast_ref::<BinaryExpr>()
    {
        if *binary.op() != Operator::Eq {
            return None;
        }
        let (l, r) = (binary.left().as_ref(), binary.right().as_ref());
        match (l.downcast_ref::<Column>(), r.downcast_ref::<Literal>()) {
            (Some(c), Some(v)) => (c, vec![v]),
            _ => (
                r.downcast_ref::<Column>()?,
                vec![l.downcast_ref::<Literal>()?],
            ),
        }
    } else if let Some(in_list) = expr.downcast_ref::<InListExpr>() {
        if in_list.negated() || in_list.list().is_empty() || in_list.list().len() > MAX_IN_LIST {
            return None;
        }
        let literals = in_list
            .list()
            .iter()
            .map(|item| item.downcast_ref::<Literal>())
            .collect::<Option<Vec<_>>>()?;
        (in_list.expr().downcast_ref::<Column>()?, literals)
    } else {
        return None;
    };
    let field = physical_schema.fields().get(column.index())?;
    if field.name() != column.name() {
        return None;
    }
    let column_id = *column_ids.get(column.index())?;
    let value_type = IndexValueType::for_arrow(field.data_type())?;
    // A NULL literal (or one that does not canonicalise to the column's key
    // type) turns the whole conjunct ineligible rather than being dropped.
    let keys: BTreeSet<IndexKey> = literals
        .iter()
        .map(|literal| IndexKey::from_scalar(literal.value(), value_type))
        .collect::<Option<_>>()?;
    Some(IndexPredicate {
        field_index: column.index(),
        column_id,
        value_type,
        keys: keys.into_iter().collect(),
    })
}

/// Why a file is scanned without its index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FallbackReason {
    /// No catalog row for (path, column).
    NoRow,
    /// A row whose `file_size_bytes` is not the listed file's.
    StaleSize,
    /// A row of another format version or value type, or whose row counts
    /// disagree with the catalog.
    Mismatch,
    /// The index object could not be read or did not check out.
    LookupError,
    /// A key is a heavy hitter in this file: every page has hits, so a
    /// selection buys nothing while the filter is kept.
    HeavyHitter,
    /// Postings exist but no access plan could be built from them.
    NoAccessPlan,
    /// The file has postings but sits on a path (deletes, a snapshot filter,
    /// the rowid detour, a name mapping) that gets no selection in v1.
    NotSelectable,
}

impl FallbackReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NoRow => "no_row",
            Self::StaleSize => "stale_size",
            Self::Mismatch => "mismatch",
            Self::LookupError => "lookup_error",
            Self::HeavyHitter => "heavy_hitter",
            Self::NoAccessPlan => "no_access_plan",
            Self::NotSelectable => "not_selectable",
        }
    }

    const ALL: [Self; 7] = [
        Self::NoRow,
        Self::StaleSize,
        Self::Mismatch,
        Self::LookupError,
        Self::HeavyHitter,
        Self::NoAccessPlan,
        Self::NotSelectable,
    ];
}

/// What the index decided for one file.
#[derive(Clone, Debug)]
pub(crate) enum FileDecision {
    /// No row of the file matches: leave it out of the scan.
    Drop,
    /// Read only these rows (a file without deletes).
    Select {
        plan: Arc<ParquetAccessPlan>,
        coverage: Arc<FileIndexCoverage>,
        rows_selected: u64,
    },
    /// Scan as before.
    Scan(FallbackReason),
}

/// A catalog row usable for `predicate` on a file listed with `file_size_bytes`
/// and `record_count` rows, or why not.
pub(crate) fn usable_meta<'a>(
    meta: Option<&'a FileIndexMeta>,
    file_size_bytes: i64,
    record_count: Option<i64>,
    predicate: &IndexPredicate,
) -> Result<&'a FileIndexMeta, FallbackReason> {
    let meta = meta.ok_or(FallbackReason::NoRow)?;
    if meta.file_size_bytes != file_size_bytes {
        return Err(FallbackReason::StaleSize);
    }
    let rows_agree = meta.row_group_rows.iter().sum::<u64>() == meta.row_count as u64
        && record_count.is_none_or(|n| n == meta.row_count);
    if meta.format_version != crate::ext_file_index::FILE_INDEX_FORMAT_VERSION
        || meta.value_type != predicate.value_type
        || meta.column_id != predicate.column_id
        || meta.row_count < 0
        || !rows_agree
    {
        return Err(FallbackReason::Mismatch);
    }
    Ok(meta)
}

/// Upper bound on `predicate`'s postings in a file: a heavy hitter's count, or
/// the heavy-hitter threshold for any other key (absent keys cost that too).
pub(crate) fn estimated_postings(meta: &FileIndexMeta, predicate: &IndexPredicate) -> u64 {
    let threshold = meta.row_count.max(0) as u64 / crate::ext_file_index::HEAVY_HITTER_DIVISOR;
    predicate
        .keys
        .iter()
        .map(|key| meta.heavy_hitter_count(key).unwrap_or(threshold))
        .sum()
}

/// Process-wide read counters: the scan's contract with dashboards (RuntimeDB
/// exports them). Monotonic.
#[derive(Debug, Default)]
pub struct FileIndexReadStats {
    scans: AtomicU64,
    files_pruned: AtomicU64,
    files_selected: AtomicU64,
    rows_selected: AtomicU64,
    lookup_micros: AtomicU64,
    fallback: [AtomicU64; 7],
}

/// A snapshot of [`FileIndexReadStats`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileIndexReadStatsSnapshot {
    /// Scans that used the index (an eligible predicate and the option on).
    pub scans: u64,
    pub files_pruned: u64,
    pub files_selected: u64,
    pub rows_selected: u64,
    pub lookup_micros: u64,
    /// `(reason, files)` for each fallback reason: `no_row`, `stale_size`,
    /// `mismatch`, `lookup_error`, `heavy_hitter`, `no_access_plan`,
    /// `not_selectable`.
    pub fallback: Vec<(&'static str, u64)>,
}

impl FileIndexReadStats {
    pub(crate) fn global() -> &'static FileIndexReadStats {
        static STATS: std::sync::OnceLock<FileIndexReadStats> = std::sync::OnceLock::new();
        STATS.get_or_init(FileIndexReadStats::default)
    }

    pub(crate) fn record(&self, summary: &ScanSummary) {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.files_pruned
            .fetch_add(summary.files_pruned, Ordering::Relaxed);
        self.files_selected
            .fetch_add(summary.files_selected, Ordering::Relaxed);
        self.rows_selected
            .fetch_add(summary.rows_selected, Ordering::Relaxed);
        self.lookup_micros
            .fetch_add(summary.lookup_micros, Ordering::Relaxed);
        for (reason, n) in &summary.fallback {
            let i = FallbackReason::ALL
                .iter()
                .position(|r| r == reason)
                .expect("every reason is listed");
            self.fallback[i].fetch_add(*n, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> FileIndexReadStatsSnapshot {
        FileIndexReadStatsSnapshot {
            scans: self.scans.load(Ordering::Relaxed),
            files_pruned: self.files_pruned.load(Ordering::Relaxed),
            files_selected: self.files_selected.load(Ordering::Relaxed),
            rows_selected: self.rows_selected.load(Ordering::Relaxed),
            lookup_micros: self.lookup_micros.load(Ordering::Relaxed),
            fallback: FallbackReason::ALL
                .iter()
                .zip(&self.fallback)
                .map(|(reason, n)| (reason.as_str(), n.load(Ordering::Relaxed)))
                .collect(),
        }
    }
}

/// The process-wide index read counters.
pub fn file_index_read_stats() -> FileIndexReadStatsSnapshot {
    FileIndexReadStats::global().snapshot()
}

/// One scan's decisions, summed for counters and tracing.
#[derive(Clone, Debug, Default)]
pub(crate) struct ScanSummary {
    pub(crate) column_id: i64,
    pub(crate) keys: usize,
    pub(crate) files_pruned: u64,
    pub(crate) files_selected: u64,
    pub(crate) rows_selected: u64,
    pub(crate) lookup_micros: u64,
    pub(crate) fallback: HashMap<FallbackReason, u64>,
}

impl ScanSummary {
    pub(crate) fn add(&mut self, decision: &FileDecision) {
        match decision {
            FileDecision::Drop => self.files_pruned += 1,
            FileDecision::Select {
                rows_selected,
                ..
            } => {
                self.files_selected += 1;
                self.rows_selected += rows_selected;
            },
            FileDecision::Scan(reason) => *self.fallback.entry(*reason).or_default() += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::{col, in_list, lit};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("user_id", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("v", DataType::Utf8View, true),
        ])
    }

    const IDS: &[i64] = &[10, 11, 12, 13];

    fn eq(c: &str, v: ScalarValue) -> Arc<dyn PhysicalExpr> {
        let s = schema();
        Arc::new(BinaryExpr::new(col(c, &s).unwrap(), Operator::Eq, lit(v)))
    }

    fn one(e: Arc<dyn PhysicalExpr>) -> Option<IndexPredicate> {
        let mut found = eligible_predicates(&[e], &schema(), IDS);
        assert!(found.len() <= 1);
        found.pop()
    }

    #[test]
    fn equality_either_side() {
        let p = one(eq("user_id", ScalarValue::from("u1"))).unwrap();
        assert_eq!(p.column_id, 10);
        assert_eq!(p.keys, vec![IndexKey::Utf8(b"u1".to_vec().into())]);
        let s = schema();
        let flipped: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            lit(ScalarValue::Int64(Some(7))),
            Operator::Eq,
            col("n", &s).unwrap(),
        ));
        let p = one(flipped).unwrap();
        assert_eq!((p.column_id, p.keys), (11, vec![IndexKey::Int64(7)]));
    }

    #[test]
    fn utf8_view_column_keys_like_utf8() {
        let p = one(eq("v", ScalarValue::Utf8View(Some("u1".into())))).unwrap();
        assert_eq!(p.keys, vec![IndexKey::Utf8(b"u1".to_vec().into())]);
    }

    #[test]
    fn ineligible_shapes() {
        let s = schema();
        // NULL literal, other operators, floats, column on both sides.
        assert!(one(eq("user_id", ScalarValue::Utf8(None))).is_none());
        assert!(one(eq("f", ScalarValue::Float64(Some(1.0)))).is_none());
        let lt: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("n", &s).unwrap(),
            Operator::Lt,
            lit(ScalarValue::Int64(Some(1))),
        ));
        assert!(one(lt).is_none());
        let cols: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("n", &s).unwrap(),
            Operator::Eq,
            col("n", &s).unwrap(),
        ));
        assert!(one(cols).is_none());
        // NOT IN, an IN list with a NULL, an over-long IN list.
        let not_in = in_list(
            col("n", &s).unwrap(),
            vec![lit(ScalarValue::Int64(Some(1)))],
            &true,
            &s,
        )
        .unwrap();
        assert!(one(not_in).is_none());
        let with_null = in_list(
            col("n", &s).unwrap(),
            vec![lit(ScalarValue::Int64(Some(1))), lit(ScalarValue::Int64(None))],
            &false,
            &s,
        )
        .unwrap();
        assert!(one(with_null).is_none());
        let long = in_list(
            col("n", &s).unwrap(),
            (0..=MAX_IN_LIST as i64)
                .map(|i| lit(ScalarValue::Int64(Some(i))))
                .collect(),
            &false,
            &s,
        )
        .unwrap();
        assert!(one(long).is_none());
    }

    #[test]
    fn in_list_keys_sorted_and_deduplicated() {
        let s = schema();
        let e = in_list(
            col("n", &s).unwrap(),
            [3, 1, 3, 2]
                .into_iter()
                .map(|i| lit(ScalarValue::Int64(Some(i))))
                .collect(),
            &false,
            &s,
        )
        .unwrap();
        let p = one(e).unwrap();
        assert_eq!(
            p.keys,
            vec![IndexKey::Int64(1), IndexKey::Int64(2), IndexKey::Int64(3)]
        );
    }

    fn meta(size: i64) -> FileIndexMeta {
        FileIndexMeta {
            path: "a.parquet".into(),
            file_size_bytes: size,
            column_id: 11,
            index_path: "a.parquet.c11.v1.pidx".into(),
            index_size_bytes: 1,
            format_version: crate::ext_file_index::FILE_INDEX_FORMAT_VERSION,
            value_type: IndexValueType::Int64,
            row_count: 10,
            row_group_rows: vec![4, 6],
            heavy_hitters: vec![],
            bucket_counts: None,
        }
    }

    #[test]
    fn usable_meta_checks() {
        let p = IndexPredicate {
            field_index: 1,
            column_id: 11,
            value_type: IndexValueType::Int64,
            keys: vec![IndexKey::Int64(1)],
        };
        let m = meta(100);
        assert!(usable_meta(Some(&m), 100, Some(10), &p).is_ok());
        assert_eq!(
            usable_meta(None, 100, Some(10), &p).unwrap_err(),
            FallbackReason::NoRow
        );
        assert_eq!(
            usable_meta(Some(&m), 101, Some(10), &p).unwrap_err(),
            FallbackReason::StaleSize
        );
        assert_eq!(
            usable_meta(Some(&m), 100, Some(11), &p).unwrap_err(),
            FallbackReason::Mismatch
        );
        let mut bad = meta(100);
        bad.row_group_rows = vec![4, 5];
        assert_eq!(
            usable_meta(Some(&bad), 100, Some(10), &p).unwrap_err(),
            FallbackReason::Mismatch
        );
        let mut bad = meta(100);
        bad.value_type = IndexValueType::Utf8;
        assert_eq!(
            usable_meta(Some(&bad), 100, Some(10), &p).unwrap_err(),
            FallbackReason::Mismatch
        );
        let mut bad = meta(100);
        bad.format_version += 1;
        assert_eq!(
            usable_meta(Some(&bad), 100, Some(10), &p).unwrap_err(),
            FallbackReason::Mismatch
        );
    }
}
