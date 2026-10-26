//! EXPERIMENTAL, opt-in: per-file time-bucket counts for an indexed column's
//! heavy hitters, and the rewrite that answers
//! `SELECT count(*), date_trunc('hour', ts) ... WHERE col = <lit> AND ts <range> GROUP BY 1`
//! from them.
//!
//! **Storage.** Each `ext_ducklake_file_index` row (one per data file and
//! indexed column) may carry a `bucket_counts` JSON value: for each of the
//! file's heavy-hitter keys, the number of the file's rows with that key whose
//! `ts` falls in each `[b, b + grain)` bucket (UTC epoch microseconds; rows
//! with a NULL `ts` are in no bucket). The counts state a fact about the
//! immutable parquet object, exactly like the posting list, so they hold at
//! every snapshot that lists the file. They ignore deletes, so they are only
//! used for files that have none.
//!
//! **Rewrite.** [`BucketCountRule`] matches
//! `Aggregate(group=[date_trunc(grain, ts)], aggr=[count(<non-null literal>)])`
//! over `Filter`s and column-only `Projection`s down to a `TableScan` of a
//! [`DuckLakeTable`], where every conjunct is a `ts`-vs-literal bound or
//! `col = lit` / `col IN (lits)` on one other column. With `[lo, hi)` the
//! whole buckets inside the window, it plans
//!
//! ```text
//! Aggregate(group=[key], aggr=[sum(c)])
//!   Union
//!     count by bucket <- Filter(P)                       <- files NOT counted + inlined rows
//!     count by bucket <- Filter(P AND (ts < lo OR ts >= hi)) <- counted files (edge buckets only)
//!     recorded counts of the counted files for buckets in [lo, hi)
//! ```
//!
//! Every row of the table is in exactly one branch: a counted file's rows in
//! `[lo, hi)` are its recorded counts, its other rows go through the edge
//! scan, and everything else is scanned as before. Which files are counted is
//! decided **once** per plan ([`SharedResolution`]) against the table's pinned
//! snapshot, and all three branches read that one decision, so they can never
//! disagree. Any failure while deciding counts no file, which plans the raw
//! query.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use base64::Engine as _;
use datafusion::catalog::Session;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchemaRef, ScalarValue};
use datafusion::datasource::{TableProvider, TableType, provider_as_source, source_as_provider};
use datafusion::error::{DataFusionError, Result};
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_expr::expr::{AggregateFunction, InList, ScalarFunction};
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    Aggregate, Between, BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator,
    TableProviderFilterPushDown, TableScan, lit,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::memory::MemorySourceConfig;

use datafusion_ducklake::DuckLakeTable;
use datafusion_ducklake::metadata_provider::DuckLakeTableFile;
use datafusion_ducklake::table::ScanRestriction;

pub use super::format::{BUCKET_GRAIN_HOUR_US, BucketCounts, MAX_BUCKET_COUNT_ENTRIES};
use super::format::{FILE_INDEX_FORMAT_VERSION, IndexKey, IndexValueType};

/// Version of the `bucket_counts` JSON.
pub const BUCKET_COUNTS_VERSION: i64 = 1;

fn key_to_json(key: &IndexKey) -> serde_json::Value {
    match key {
        IndexKey::Utf8(bytes) => {
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        },
        IndexKey::Int64(v) => serde_json::Value::from(*v),
    }
}

fn key_from_json(value: &serde_json::Value, ty: IndexValueType) -> Option<IndexKey> {
    match ty {
        IndexValueType::Utf8 => Some(IndexKey::Utf8(
            base64::engine::general_purpose::STANDARD
                .decode(value.as_str()?)
                .ok()?
                .into(),
        )),
        IndexValueType::Int64 => Some(IndexKey::Int64(value.as_i64()?)),
    }
}

/// Catalog column `bucket_counts`:
/// `{"v":1,"ts_column_id":N,"grain_us":G,"keys":[[key,[[start_us,count],...]],...]}`,
/// keys encoded as in `heavy_hitters` (Utf8 base64, Int64 a number).
pub fn bucket_counts_to_json(counts: &BucketCounts) -> String {
    let keys: Vec<serde_json::Value> = counts
        .keys
        .iter()
        .map(|(key, buckets)| {
            serde_json::json!([
                key_to_json(key),
                buckets
                    .iter()
                    .map(|(b, c)| serde_json::json!([b, c]))
                    .collect::<Vec<_>>()
            ])
        })
        .collect();
    serde_json::json!({
        "v": BUCKET_COUNTS_VERSION,
        "ts_column_id": counts.ts_column_id,
        "grain_us": counts.grain_us,
        "keys": keys,
    })
    .to_string()
}

/// Inverse of [`bucket_counts_to_json`]. `None` for anything malformed or
/// unsorted, which reads as "no counts".
pub fn bucket_counts_from_json(text: &str, ty: IndexValueType) -> Option<BucketCounts> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    if value.get("v")?.as_i64()? != BUCKET_COUNTS_VERSION {
        return None;
    }
    let ts_column_id = value.get("ts_column_id")?.as_i64()?;
    let grain_us = value.get("grain_us")?.as_i64()?;
    if grain_us <= 0 {
        return None;
    }
    let mut keys: Vec<(IndexKey, Buckets)> = Vec::new();
    for entry in value.get("keys")?.as_array()? {
        let entry = entry.as_array()?;
        if entry.len() != 2 {
            return None;
        }
        let key = key_from_json(&entry[0], ty)?;
        let mut buckets: Buckets = Vec::new();
        for pair in entry[1].as_array()? {
            let pair = pair.as_array()?;
            if pair.len() != 2 {
                return None;
            }
            let start = pair[0].as_i64()?;
            if start.rem_euclid(grain_us) != 0 || buckets.last().is_some_and(|(b, _)| *b >= start) {
                return None;
            }
            buckets.push((start, pair[1].as_u64()?));
        }
        // Lookups binary-search the keys.
        if keys.last().is_some_and(|(k, _)| *k >= key) {
            return None;
        }
        keys.push((key, buckets));
    }
    Some(BucketCounts {
        ts_column_id,
        grain_us,
        keys,
    })
}

fn buckets_of<'a>(counts: &'a BucketCounts, key: &IndexKey) -> Option<&'a [(i64, u64)]> {
    counts
        .keys
        .binary_search_by(|(k, _)| k.cmp(key))
        .ok()
        .map(|i| counts.keys[i].1.as_slice())
}

/// One key's `(bucket start µs, count)` pairs.
type Buckets = Vec<(i64, u64)>;

/// Sentinel bucket id of a row with a NULL timestamp.
const NULL_BUCKET: i32 = i32::MIN;

/// Records each row's bucket id (4 B/row) while a file is written, in write
/// order. At `finish` each heavy hitter's postings (its rows' file positions)
/// are histogrammed against them.
#[derive(Debug)]
pub struct BucketCountBuilder {
    ts_column_id: i64,
    grain_us: i64,
    ids: Vec<i32>,
    /// A batch could not be bucketed: record no counts for the file.
    failed: bool,
}

impl BucketCountBuilder {
    pub fn new(ts_column_id: i64, grain_us: i64) -> Self {
        Self {
            ts_column_id,
            grain_us,
            ids: Vec::new(),
            failed: grain_us <= 0,
        }
    }

    pub fn rows_seen(&self) -> u64 {
        self.ids.len() as u64
    }

    /// `ts` is the batch's time column. A non-timestamp column, a grain that
    /// does not divide into its unit or an id out of range fails the builder
    /// (no counts), never the write.
    pub fn observe(&mut self, ts: &ArrayRef) {
        if self.failed {
            self.ids.extend(std::iter::repeat_n(NULL_BUCKET, ts.len()));
            return;
        }
        let DataType::Timestamp(unit, _) = ts.data_type() else {
            self.failed = true;
            return self.observe(ts);
        };
        let Some(grain) = grain_in_unit(*unit, self.grain_us) else {
            self.failed = true;
            return self.observe(ts);
        };
        macro_rules! push {
            ($ty:ty) => {{
                let array = ts.as_any().downcast_ref::<$ty>().expect("timestamp array");
                for i in 0..array.len() {
                    if array.is_null(i) {
                        self.ids.push(NULL_BUCKET);
                        continue;
                    }
                    match i32::try_from(array.value(i).div_euclid(grain)) {
                        Ok(id) if id != NULL_BUCKET => self.ids.push(id),
                        _ => {
                            self.failed = true;
                            self.ids.push(NULL_BUCKET);
                        },
                    }
                }
            }};
        }
        match unit {
            TimeUnit::Second => push!(TimestampSecondArray),
            TimeUnit::Millisecond => push!(TimestampMillisecondArray),
            TimeUnit::Microsecond => push!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => push!(TimestampNanosecondArray),
        }
    }

    /// Counts for `heavy` (each key with its sorted file positions). `None`
    /// when the builder failed or a posting is past the rows it saw.
    pub fn finish<'a>(
        self,
        heavy: impl IntoIterator<Item = (IndexKey, &'a [u32])>,
    ) -> Option<BucketCounts> {
        if self.failed {
            return None;
        }
        // (key, buckets, rows counted)
        let mut keys: Vec<(IndexKey, Buckets, u64)> = Vec::new();
        for (key, rows) in heavy {
            let mut buckets: BTreeMap<i32, u64> = BTreeMap::new();
            let mut total = 0;
            for &row in rows {
                let id = *self.ids.get(row as usize)?;
                if id != NULL_BUCKET {
                    *buckets.entry(id).or_default() += 1;
                    total += 1;
                }
            }
            let buckets = buckets
                .into_iter()
                .map(|(id, c)| (i64::from(id) * self.grain_us, c))
                .collect();
            keys.push((key, buckets, total));
        }
        // Keep the largest keys whole within the cap.
        keys.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        let mut entries = 0;
        let mut kept: Vec<(IndexKey, Vec<(i64, u64)>)> = Vec::new();
        for (key, buckets, _) in keys {
            if entries + buckets.len() > MAX_BUCKET_COUNT_ENTRIES {
                continue;
            }
            entries += buckets.len();
            kept.push((key, buckets));
        }
        kept.sort_by(|a, b| a.0.cmp(&b.0));
        Some(BucketCounts {
            ts_column_id: self.ts_column_id,
            grain_us: self.grain_us,
            keys: kept,
        })
    }
}

/// `grain_us` in `unit`s, `None` when it is not a whole number of them.
fn grain_in_unit(unit: TimeUnit, grain_us: i64) -> Option<i64> {
    match unit {
        TimeUnit::Second => (grain_us % 1_000_000 == 0).then_some(grain_us / 1_000_000),
        TimeUnit::Millisecond => (grain_us % 1_000 == 0).then_some(grain_us / 1_000),
        TimeUnit::Microsecond => Some(grain_us),
        TimeUnit::Nanosecond => grain_us.checked_mul(1_000),
    }
    .filter(|g| *g > 0)
}

/// A bucket start in UTC microseconds, in `unit`s.
fn us_to_unit(unit: TimeUnit, us: i64) -> Option<i64> {
    match unit {
        TimeUnit::Second => Some(us.div_euclid(1_000_000)),
        TimeUnit::Millisecond => Some(us.div_euclid(1_000)),
        TimeUnit::Microsecond => Some(us),
        TimeUnit::Nanosecond => us.checked_mul(1_000),
    }
}

/// What a matched query needs from the counts, in the time column's unit.
#[derive(Clone, Debug, PartialEq)]
pub struct BucketCountRequest {
    pub ts_column_id: i64,
    pub ts_unit: TimeUnit,
    pub key_column_id: i64,
    pub value_type: IndexValueType,
    pub keys: Vec<IndexKey>,
    pub grain_us: i64,
    /// Whole buckets served from counts: `[lo, hi)`, grain-aligned.
    pub lo: i64,
    pub hi: i64,
}

/// The once-per-plan decision: the files whose rows in `[lo, hi)` are served
/// by their counts, and the summed counts per bucket start (in `ts_unit`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Resolution {
    pub counted: HashSet<(String, i64)>,
    pub buckets: BTreeMap<i64, u64>,
}

impl Resolution {
    /// Adds `file`'s counts when they cover every key, else leaves it out.
    pub(crate) fn add_file(
        &mut self,
        file: &DuckLakeTableFile,
        counts: &BucketCounts,
        req: &BucketCountRequest,
    ) {
        if counts.ts_column_id != req.ts_column_id || counts.grain_us != req.grain_us {
            return;
        }
        let mut per_key = Vec::with_capacity(req.keys.len());
        for key in &req.keys {
            match buckets_of(counts, key) {
                Some(buckets) => per_key.push(buckets),
                None => return,
            }
        }
        let mut add = BTreeMap::new();
        for buckets in per_key {
            for &(start_us, n) in buckets {
                let Some(start) = us_to_unit(req.ts_unit, start_us) else {
                    return;
                };
                if start >= req.lo && start < req.hi {
                    *add.entry(start).or_insert(0u64) += n;
                }
            }
        }
        if !self
            .counted
            .insert((file.file.path.clone(), file.file.file_size_bytes))
        {
            return;
        }
        for (start, n) in add {
            *self.buckets.entry(start).or_insert(0) += n;
        }
    }
}

/// The resolution all branches of one rewrite share, computed on first use.
pub struct SharedResolution {
    table: Arc<DuckLakeTable>,
    request: BucketCountRequest,
    cell: OnceLock<Arc<Resolution>>,
}

impl fmt::Debug for SharedResolution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedResolution")
            .field("request", &self.request)
            .field("resolution", &self.cell.get())
            .finish()
    }
}

impl SharedResolution {
    pub fn new(table: Arc<DuckLakeTable>, request: BucketCountRequest) -> Arc<Self> {
        Arc::new(Self {
            table,
            request,
            cell: OnceLock::new(),
        })
    }

    pub fn get(&self) -> Arc<Resolution> {
        Arc::clone(
            self.cell
                .get_or_init(|| Arc::new(resolve_bucket_counts(&self.table, &self.request))),
        )
    }
}

/// Which rows of the table a [`BranchScan`] reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Branch {
    /// Files not counted, and inlined rows: the whole window.
    Uncounted,
    /// Counted files: the caller filters them to the edge buckets.
    CountedEdges,
}

/// The table restricted to one branch's files.
#[derive(Debug)]
pub struct BranchScan {
    shared: Arc<SharedResolution>,
    branch: Branch,
}

#[async_trait]
impl TableProvider for BranchScan {
    fn schema(&self) -> SchemaRef {
        self.shared.table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.shared.table.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let resolution = self.shared.get();
        let only_counted = self.branch == Branch::CountedEdges;
        let keep = move |file: &DuckLakeTableFile| {
            let key = (file.file.path.clone(), file.file.file_size_bytes);
            resolution.counted.contains(&key) == only_counted
        };
        // Inlined rows are never counted, so only the uncounted branch reads them.
        let restriction = ScanRestriction {
            keep: &keep,
            include_inlined: !only_counted,
        };
        self.shared
            .table
            .scan_restricted(state, projection, filters, limit, Some(&restriction))
            .await
    }
}

/// The counted files' summed counts for the whole buckets, as
/// `(key, count)` rows.
#[derive(Debug)]
pub struct CountsScan {
    shared: Arc<SharedResolution>,
    schema: SchemaRef,
}

impl CountsScan {
    fn new(shared: Arc<SharedResolution>, ts_type: DataType) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new(KEY_COL, ts_type, true),
            Field::new(COUNT_COL, DataType::Int64, false),
        ]));
        Self {
            shared,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for CountsScan {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let resolution = self.shared.get();
        let starts: Vec<i64> = resolution.buckets.keys().copied().collect();
        let counts: Vec<i64> = resolution
            .buckets
            .values()
            .map(|c| i64::try_from(*c).map_err(|e| DataFusionError::External(Box::new(e))))
            .collect::<Result<_>>()?;
        let DataType::Timestamp(unit, tz) = self.schema.field(0).data_type().clone() else {
            return Err(DataFusionError::Internal(
                "bucket key is not a timestamp".into(),
            ));
        };
        let keys: ArrayRef = match unit {
            TimeUnit::Second => Arc::new(TimestampSecondArray::from(starts).with_timezone_opt(tz)),
            TimeUnit::Millisecond => {
                Arc::new(TimestampMillisecondArray::from(starts).with_timezone_opt(tz))
            },
            TimeUnit::Microsecond => {
                Arc::new(TimestampMicrosecondArray::from(starts).with_timezone_opt(tz))
            },
            TimeUnit::Nanosecond => {
                Arc::new(TimestampNanosecondArray::from(starts).with_timezone_opt(tz))
            },
        };
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![keys, Arc::new(Int64Array::from(counts))],
        )?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?)
    }
}

const KEY_COL: &str = "__ducklake_bucket";
const COUNT_COL: &str = "__ducklake_bucket_count";

/// Finds the [`DuckLakeTable`] behind a scan's provider: the provider itself,
/// or what a wrapper (RuntimeDB's index-aware provider) delegates to.
pub type TableResolver =
    Arc<dyn Fn(&Arc<dyn TableProvider>) -> Option<Arc<dyn TableProvider>> + Send + Sync>;

/// The rewrite (module docs). Fires only for tables whose read options turn
/// `ext_file_index_counts` on.
pub struct BucketCountRule {
    resolver: TableResolver,
}

impl fmt::Debug for BucketCountRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BucketCountRule")
    }
}

impl Default for BucketCountRule {
    fn default() -> Self {
        Self::new(Arc::new(|provider| Some(Arc::clone(provider))))
    }
}

impl BucketCountRule {
    pub fn new(resolver: TableResolver) -> Self {
        Self {
            resolver,
        }
    }

    fn table(&self, scan: &TableScan) -> Option<Arc<DuckLakeTable>> {
        let provider = source_as_provider(&scan.source).ok()?;
        let resolved = (self.resolver)(&provider)?;
        let table = resolved.downcast_ref::<DuckLakeTable>()?;
        // The rewrite's resolution and both branch scans share one listing.
        table
            .read_options()
            .extensions
            .get::<super::FileIndex>()
            .is_some_and(super::FileIndex::counts)
            .then(|| Arc::new(table.with_plan_listing_memo()))
    }

    fn try_rewrite(&self, aggregate: &Aggregate) -> Result<Option<LogicalPlan>> {
        let Some(matched) = self.match_aggregate(aggregate) else {
            return Ok(None);
        };
        let Matched {
            scan,
            table,
            predicates,
            bucket_expr,
            ts_col,
            ts_type,
            request,
        } = matched;
        let (DataType::Timestamp(_, tz), Some(lo), Some(hi)) = (
            &ts_type,
            timestamp_scalar(&ts_type, request.lo),
            timestamp_scalar(&ts_type, request.hi),
        ) else {
            return Ok(None);
        };
        let _ = tz;
        let shared = SharedResolution::new(table, request);
        let predicate = conjunction(predicates.clone()).expect("at least the time bounds");
        let branch = |branch: Branch, predicate: Expr| -> Result<LogicalPlan> {
            let source = provider_as_source(Arc::new(BranchScan {
                shared: Arc::clone(&shared),
                branch,
            }));
            LogicalPlanBuilder::scan_with_filters(
                scan.table_name.clone(),
                source,
                scan.projection.clone(),
                scan.filters.clone(),
            )?
            .filter(predicate)?
            .aggregate(
                vec![bucket_expr.clone().alias(KEY_COL)],
                vec![count(lit(1i64)).alias(COUNT_COL)],
            )?
            .build()
        };
        let uncounted = branch(Branch::Uncounted, predicate.clone())?;
        let ts = Expr::Column(ts_col.clone());
        let edges = ts.clone().lt(lit(lo)).or(ts.gt_eq(lit(hi)));
        let counted = branch(Branch::CountedEdges, predicate.and(edges))?;
        let counts = LogicalPlanBuilder::scan(
            "__ducklake_bucket_counts",
            provider_as_source(Arc::new(CountsScan::new(Arc::clone(&shared), ts_type))),
            None,
        )?
        .build()?;
        let plan = LogicalPlanBuilder::from(uncounted)
            .union(counted)?
            .union(counts)?
            .aggregate(
                vec![Expr::Column(Column::from_name(KEY_COL))],
                vec![sum(Expr::Column(Column::from_name(COUNT_COL))).alias(COUNT_COL)],
            )?;
        // The original aggregate's output names and types, so every node above
        // reads the rewrite exactly as it read the aggregate.
        let out = aggregate.schema.as_ref();
        let (group_qualifier, group_field) = out.qualified_field(0);
        let (count_qualifier, count_field) = out.qualified_field(1);
        let schema = plan_schema(&plan)?;
        let plan = plan
            .project(vec![
                Expr::Column(Column::from_name(KEY_COL))
                    .alias_qualified(group_qualifier.cloned(), group_field.name()),
                Expr::Column(Column::from_name(COUNT_COL))
                    .cast_to(count_field.data_type(), schema.as_ref())?
                    .alias_qualified(count_qualifier.cloned(), count_field.name()),
            ])?
            .build()?;
        Ok(Some(plan))
    }

    fn match_aggregate(&self, aggregate: &Aggregate) -> Option<Matched> {
        if aggregate.group_expr.len() != 1 || aggregate.aggr_expr.len() != 1 {
            return None;
        }
        if aggregate.schema.fields().len() != 2 {
            return None;
        }
        let bucket_expr = unalias(&aggregate.group_expr[0]).clone();
        let (grain_us, ts_col) = date_trunc_of_column(&bucket_expr)?;
        if !is_count_star(unalias(&aggregate.aggr_expr[0])) {
            return None;
        }
        // Walk to the scan through filters and column-only projections.
        let mut predicates: Vec<Expr> = Vec::new();
        let mut node = aggregate.input.as_ref();
        let scan = loop {
            match node {
                LogicalPlan::Filter(filter) => {
                    predicates.extend(split_conjunction(&filter.predicate).into_iter().cloned());
                    node = filter.input.as_ref();
                },
                LogicalPlan::Projection(projection) => {
                    if !projection.expr.iter().all(|e| matches!(e, Expr::Column(_))) {
                        return None;
                    }
                    node = projection.input.as_ref();
                },
                LogicalPlan::TableScan(scan) => break scan,
                _ => return None,
            }
        };
        if scan.fetch.is_some() {
            return None;
        }
        for filter in &scan.filters {
            predicates.extend(split_conjunction(filter).into_iter().cloned());
        }
        // An Inexact pushdown leaves each conjunct both in the scan and in
        // the filter above it.
        let mut seen = HashSet::new();
        predicates.retain(|p| seen.insert(p.clone()));
        let table = self.table(scan)?;
        let schema = table.schema();
        let field_of = |col: &Column| -> Option<usize> {
            let projected = scan.projected_schema.index_of_column(col).ok()?;
            Some(match &scan.projection {
                Some(p) => p[projected],
                None => projected,
            })
        };
        let ts_index = field_of(&ts_col)?;
        let ts_type = schema.field(ts_index).data_type().clone();
        let DataType::Timestamp(ts_unit, tz) = &ts_type else {
            return None;
        };
        // Buckets are UTC; `date_trunc` truncates in the column's zone.
        if !tz.as_deref().is_none_or(is_utc) {
            return None;
        }
        let grain = grain_in_unit(*ts_unit, grain_us)?;
        let ts_column_id = table.top_level_column_id(schema.field(ts_index).name())?;

        let mut lower: Option<i64> = None; // inclusive
        let mut upper: Option<i64> = None; // exclusive
        let mut key: Option<(Column, Vec<ScalarValue>)> = None;
        for predicate in &predicates {
            if let Some((op, v)) = ts_bound(predicate, &ts_col) {
                let (lo, hi) = match op {
                    Operator::GtEq => (Some(v), None),
                    Operator::Gt => (Some(v.checked_add(1)?), None),
                    Operator::Lt => (None, Some(v)),
                    Operator::LtEq => (None, Some(v.checked_add(1)?)),
                    _ => return None,
                };
                if let Some(lo) = lo {
                    lower = Some(lower.map_or(lo, |l| l.max(lo)));
                }
                if let Some(hi) = hi {
                    upper = Some(upper.map_or(hi, |u| u.min(hi)));
                }
                continue;
            }
            if let Some((col, values)) = key_equality(predicate) {
                if col == ts_col || key.is_some() {
                    return None;
                }
                key = Some((col, values));
                continue;
            }
            if let Expr::Between(Between {
                expr,
                negated: false,
                low,
                high,
            }) = predicate
                && matches!(expr.as_ref(), Expr::Column(c) if *c == ts_col)
            {
                let lo = timestamp_value(low, &ts_type)?;
                let hi = timestamp_value(high, &ts_type)?.checked_add(1)?;
                lower = Some(lower.map_or(lo, |l| l.max(lo)));
                upper = Some(upper.map_or(hi, |u| u.min(hi)));
                continue;
            }
            // Any other conjunct: the counts do not answer it.
            return None;
        }
        let (lower, upper, (key_col, values)) = (lower?, upper?, key?);
        let lo = lower
            .checked_add(grain - 1)?
            .div_euclid(grain)
            .checked_mul(grain)?;
        let hi = upper.div_euclid(grain).checked_mul(grain)?;
        if lo >= hi {
            return None;
        }
        let key_index = field_of(&key_col)?;
        let key_field = schema.field(key_index);
        let value_type = IndexValueType::for_arrow(key_field.data_type())?;
        let keys: Vec<IndexKey> = values
            .iter()
            .map(|v| IndexKey::from_scalar(v, value_type))
            .collect::<Option<_>>()?;
        let mut keys = keys;
        keys.sort();
        keys.dedup();
        let key_column_id = table.top_level_column_id(key_field.name())?;
        let request = BucketCountRequest {
            ts_column_id,
            ts_unit: *ts_unit,
            key_column_id,
            value_type,
            keys,
            grain_us,
            lo,
            hi,
        };
        Some(Matched {
            scan: scan.clone(),
            table,
            predicates,
            bucket_expr,
            ts_col,
            ts_type,
            request,
        })
    }
}

struct Matched {
    scan: TableScan,
    table: Arc<DuckLakeTable>,
    predicates: Vec<Expr>,
    bucket_expr: Expr,
    ts_col: Column,
    ts_type: DataType,
    request: BucketCountRequest,
}

impl OptimizerRule for BucketCountRule {
    fn name(&self) -> &str {
        "ducklake_bucket_counts"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = &plan else {
            return Ok(Transformed::no(plan));
        };
        // `SET ducklake_file_index.enabled = false` (the on/off spot check)
        // turns the counts off with the rest of the index.
        if !super::scans_enabled(&config.options(), true) {
            return Ok(Transformed::no(plan));
        }
        match self.try_rewrite(aggregate) {
            Ok(Some(rewritten)) => Ok(Transformed::yes(rewritten)),
            Ok(None) => Ok(Transformed::no(plan)),
            Err(error) => {
                tracing::debug!(%error, "ducklake_bucket_counts: not rewriting");
                Ok(Transformed::no(plan))
            },
        }
    }
}

fn plan_schema(builder: &LogicalPlanBuilder) -> Result<DFSchemaRef> {
    Ok(Arc::clone(builder.schema()))
}

fn unalias(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => unalias(&alias.expr),
        other => other,
    }
}

fn is_utc(tz: &str) -> bool {
    matches!(tz, "UTC" | "utc" | "+00:00" | "Z" | "+00" | "Etc/UTC")
}

/// `date_trunc('<grain>', <column>)` → the grain in µs and the column.
fn date_trunc_of_column(expr: &Expr) -> Option<(i64, Column)> {
    let Expr::ScalarFunction(ScalarFunction {
        func,
        args,
    }) = expr
    else {
        return None;
    };
    if func.name() != "date_trunc" || args.len() != 2 {
        return None;
    }
    let (Expr::Literal(granularity, _), Expr::Column(col)) = (&args[0], &args[1]) else {
        return None;
    };
    let granularity = match granularity {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s,
        _ => return None,
    };
    match granularity.to_ascii_lowercase().as_str() {
        "hour" => Some((BUCKET_GRAIN_HOUR_US, col.clone())),
        _ => None,
    }
}

/// `count(<non-null literal>)`, which is what `count(*)` plans as.
fn is_count_star(expr: &Expr) -> bool {
    let Expr::AggregateFunction(AggregateFunction {
        func,
        params,
    }) = expr
    else {
        return false;
    };
    func.name() == "count"
        && !params.distinct
        && params.filter.is_none()
        && params.order_by.is_empty()
        && params.args.len() == 1
        && matches!(&params.args[0], Expr::Literal(v, _) if !v.is_null())
}

/// `ts <op> lit` (or flipped) → the operator as `ts <op> v` and `v`.
fn ts_bound(expr: &Expr, ts: &Column) -> Option<(Operator, i64)> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op,
        right,
    }) = expr
    else {
        return None;
    };
    let (op, value) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) if c == ts => (*op, v),
        (Expr::Literal(v, _), Expr::Column(c)) if c == ts => (op.swap()?, v),
        _ => return None,
    };
    if !matches!(
        op,
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
    ) {
        return None;
    }
    Some((op, timestamp_scalar_value(value)?))
}

/// A non-null timestamp literal's value. Its unit is the column's: the
/// analyzer casts the literal to the column type.
fn timestamp_scalar_value(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampSecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
        _ => None,
    }
}

fn timestamp_value(expr: &Expr, ts_type: &DataType) -> Option<i64> {
    let Expr::Literal(v, _) = expr else {
        return None;
    };
    (v.data_type() == *ts_type)
        .then(|| timestamp_scalar_value(v))
        .flatten()
}

fn timestamp_scalar(ts_type: &DataType, v: i64) -> Option<ScalarValue> {
    let DataType::Timestamp(unit, tz) = ts_type else {
        return None;
    };
    Some(match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(v), tz.clone()),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(v), tz.clone()),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(v), tz.clone()),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(v), tz.clone()),
    })
}

/// `col = lit`, `lit = col`, `col IN (lits)` or an `OR` of equalities on one
/// column (what the simplifier makes of a short `IN`), non-null literals.
fn key_equality(expr: &Expr) -> Option<(Column, Vec<ScalarValue>)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            let (l, mut lv) = key_equality(left)?;
            let (r, rv) = key_equality(right)?;
            if l != r || lv.len() + rv.len() > 64 {
                return None;
            }
            lv.extend(rv);
            Some((l, lv))
        },
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(v, _)) | (Expr::Literal(v, _), Expr::Column(c))
                if !v.is_null() =>
            {
                Some((c.clone(), vec![v.clone()]))
            },
            _ => None,
        },
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) => {
            let Expr::Column(c) = expr.as_ref() else {
                return None;
            };
            if list.is_empty() || list.len() > 64 {
                return None;
            }
            let values = list
                .iter()
                .map(|e| match e {
                    Expr::Literal(v, _) if !v.is_null() => Some(v.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            Some((c.clone(), values))
        },
        _ => None,
    }
}

/// The files whose rows in `[req.lo, req.hi)` their recorded counts serve,
/// at `table`'s pinned snapshot. A failure counts no file.
fn resolve_bucket_counts(table: &DuckLakeTable, req: &BucketCountRequest) -> Resolution {
    match try_resolve_bucket_counts(table, req) {
        Ok(resolution) => resolution,
        Err(error) => {
            tracing::debug!(%error, "bucket counts unavailable; scanning every file");
            Default::default()
        },
    }
}

fn try_resolve_bucket_counts(
    table: &DuckLakeTable,
    req: &BucketCountRequest,
) -> Result<Resolution> {
    // Counts ignore deletes and snapshot filters, and are keyed by column id,
    // so only files read exactly as stored qualify.
    let eligible = table.plain_files()?;
    let paths: Vec<&str> = eligible.iter().map(|tf| tf.file.path.as_str()).collect();
    let rows = table
        .shared_extension_state()
        .get_or_default::<super::scan::IndexMemo>()
        .rows(
            table.metadata_provider().as_ref(),
            table.table_id(),
            &paths,
            &[req.key_column_id],
        );
    let mut resolution = Resolution::default();
    for tf in &eligible {
        let Some(meta) = rows.get(&(tf.file.path.clone(), req.key_column_id)) else {
            continue;
        };
        // A fact applies only to the file it was built from.
        if meta.file_size_bytes != tf.file.file_size_bytes
            || meta.format_version != FILE_INDEX_FORMAT_VERSION
            || meta.value_type != req.value_type
        {
            continue;
        }
        if let Some(counts) = &meta.bucket_counts {
            resolution.add_file(tf, counts, req);
        }
    }
    tracing::debug!(
        counted_files = resolution.counted.len(),
        buckets = resolution.buckets.len(),
        "bucket counts resolved"
    );
    Ok(resolution)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_round_trips() {
        let counts = BucketCounts {
            ts_column_id: 3,
            grain_us: BUCKET_GRAIN_HOUR_US,
            keys: vec![
                (
                    IndexKey::Utf8(b"u0".as_slice().into()),
                    vec![(0, 5), (BUCKET_GRAIN_HOUR_US, 2)],
                ),
                (
                    IndexKey::Utf8(b"\xff\x00".as_slice().into()),
                    vec![(-BUCKET_GRAIN_HOUR_US, 1)],
                ),
            ],
        };
        let back =
            bucket_counts_from_json(&bucket_counts_to_json(&counts), IndexValueType::Utf8).unwrap();
        // from_json keeps the stored order; to_json is fed sorted keys.
        assert_eq!(back, counts);
        assert!(bucket_counts_from_json("{}", IndexValueType::Utf8).is_none());
        assert!(
            bucket_counts_from_json(&bucket_counts_to_json(&counts), IndexValueType::Int64)
                .is_none()
        );
    }

    #[test]
    fn builder_counts_heavy_postings_per_bucket() {
        let h = BUCKET_GRAIN_HOUR_US;
        let mut b = BucketCountBuilder::new(7, h);
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(h - 1),
            Some(h),
            None,
            Some(-1),
        ]));
        b.observe(&ts);
        let rows: Vec<u32> = vec![0, 1, 2, 3, 4];
        let counts = b.finish([(IndexKey::Int64(9), rows.as_slice())]).unwrap();
        assert_eq!(
            counts.keys,
            vec![(IndexKey::Int64(9), vec![(-h, 1), (0, 2), (h, 1)])]
        );
    }

    #[test]
    fn builder_cap_keeps_whole_keys_largest_first() {
        let h = BUCKET_GRAIN_HOUR_US;
        let n = MAX_BUCKET_COUNT_ENTRIES + 1;
        let mut b = BucketCountBuilder::new(1, h);
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            (0..n as i64).map(|i| i * h).collect::<Vec<_>>(),
        ));
        b.observe(&ts);
        let big: Vec<u32> = (0..n as u32).collect();
        let small: Vec<u32> = vec![0];
        let counts = b
            .finish([(IndexKey::Int64(1), big.as_slice()), (IndexKey::Int64(2), small.as_slice())])
            .unwrap();
        // The big key does not fit whole, so it is dropped; the small one stays.
        assert_eq!(counts.keys, vec![(IndexKey::Int64(2), vec![(0, 1)])]);
    }

    #[test]
    fn builder_fails_on_non_timestamp() {
        let mut b = BucketCountBuilder::new(1, BUCKET_GRAIN_HOUR_US);
        let ts: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        b.observe(&ts);
        assert!(b.finish([]).is_none());
    }

    #[test]
    fn nanosecond_and_second_units() {
        let h = BUCKET_GRAIN_HOUR_US;
        let mut b = BucketCountBuilder::new(1, h);
        let ts: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![h * 1000 + 5]));
        b.observe(&ts);
        let rows = [0u32];
        let c = b.finish([(IndexKey::Int64(1), rows.as_slice())]).unwrap();
        assert_eq!(c.keys[0].1, vec![(h, 1)]);
        assert_eq!(us_to_unit(TimeUnit::Second, h), Some(3600));
        assert_eq!(grain_in_unit(TimeUnit::Second, h), Some(3600));
    }

    /// Through `PostingBuilder`: heavy hitters (> 1024 rows) get exact
    /// per-hour counts against a brute force; light keys and NULL keys none.
    #[test]
    fn posting_builder_counts_match_brute_force() {
        use crate::file_index::format::PostingBuilder;
        use arrow::array::StringArray;
        let h = BUCKET_GRAIN_HOUR_US;
        let mut b = PostingBuilder::new(2, IndexValueType::Utf8).with_bucket_counts(1, h);
        let mut want: BTreeMap<(String, i64), u64> = BTreeMap::new();
        let mut id = 0i64;
        let mut rows = 0u64;
        for batch in 0..7 {
            let n = 1000 + batch * 37;
            let mut users = Vec::new();
            let mut ts = Vec::new();
            for _ in 0..n {
                id += 1;
                let user = match id % 10 {
                    0..=5 => Some("hot"),
                    6..=8 => Some("warm"),
                    _ => (id % 20 == 9).then_some("cold"),
                };
                let t = (id % 13 != 0).then_some(id * 997_000_003 % (30 * h) - 5 * h);
                if let (Some(u), Some(t)) = (user, t) {
                    *want
                        .entry((u.to_string(), t.div_euclid(h) * h))
                        .or_default() += 1;
                }
                users.push(user);
                ts.push(t);
            }
            rows += n as u64;
            let users: ArrayRef = Arc::new(StringArray::from(users));
            let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(ts));
            b.observe(&users).unwrap();
            b.observe_ts(&ts);
        }
        let built = b.finish(vec![rows]).unwrap();
        let counts = built.bucket_counts.expect("counts");
        assert_eq!((counts.ts_column_id, counts.grain_us), (1, h));
        let keys: Vec<&IndexKey> = counts.keys.iter().map(|(k, _)| k).collect();
        // hot: 60% of 7.8K rows, warm: 30%; cold: 5%, under the 1024 floor.
        assert_eq!(
            keys,
            vec![
                &IndexKey::Utf8(b"hot".as_slice().into()),
                &IndexKey::Utf8(b"warm".as_slice().into())
            ]
        );
        for (key, buckets) in &counts.keys {
            let IndexKey::Utf8(user) = key else {
                unreachable!()
            };
            let user = std::str::from_utf8(user).unwrap();
            let expect: Vec<(i64, u64)> = want
                .iter()
                .filter(|((u, _), _)| u == user)
                .map(|((_, b), c)| (*b, *c))
                .collect();
            assert_eq!(buckets, &expect, "{user}");
        }
        let back = bucket_counts_from_json(&bucket_counts_to_json(&counts), IndexValueType::Utf8);
        assert_eq!(back.as_ref(), Some(&counts));
    }

    /// A batch whose time column was not fed leaves the file without counts.
    #[test]
    fn posting_builder_without_every_ts_batch_has_no_counts() {
        use crate::file_index::format::PostingBuilder;
        let mut b = PostingBuilder::new(2, IndexValueType::Int64)
            .with_bucket_counts(1, BUCKET_GRAIN_HOUR_US);
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![7; 2000]));
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![0; 2000]));
        b.observe(&keys).unwrap();
        b.observe_ts(&ts);
        b.observe(&keys).unwrap();
        assert!(b.finish(vec![4000]).unwrap().bucket_counts.is_none());
        // Without the option, never any counts.
        let mut b = PostingBuilder::new(2, IndexValueType::Int64);
        b.observe(&keys).unwrap();
        assert!(b.finish(vec![2000]).unwrap().bucket_counts.is_none());
    }

    #[test]
    fn unsorted_json_is_rejected() {
        let h = BUCKET_GRAIN_HOUR_US;
        let keys = format!(
            r#"{{"v":1,"ts_column_id":1,"grain_us":{h},"keys":[[2,[[0,1]]],[1,[[0,1]]]]}}"#
        );
        assert!(bucket_counts_from_json(&keys, IndexValueType::Int64).is_none());
        let buckets =
            format!(r#"{{"v":1,"ts_column_id":1,"grain_us":{h},"keys":[[1,[[{h},1],[0,1]]]]}}"#);
        assert!(bucket_counts_from_json(&buckets, IndexValueType::Int64).is_none());
        let unaligned =
            format!(r#"{{"v":1,"ts_column_id":1,"grain_us":{h},"keys":[[1,[[5,1]]]]}}"#);
        assert!(bucket_counts_from_json(&unaligned, IndexValueType::Int64).is_none());
        let ok = format!(
            r#"{{"v":1,"ts_column_id":1,"grain_us":{h},"keys":[[1,[[0,1],[{h},2]]],[2,[]]]}}"#
        );
        assert!(bucket_counts_from_json(&ok, IndexValueType::Int64).is_some());
    }
}
