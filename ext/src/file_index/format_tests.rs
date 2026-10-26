use std::collections::BTreeMap;
use std::sync::Mutex;

use arrow::array::{
    DictionaryArray, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
};
use arrow::datatypes::Int32Type as DictKey;
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;

use super::*;

/// Deterministic xorshift; no rand dependency.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Brute-force postings of every non-null value in `batches`.
fn brute_force(batches: &[ArrayRef], ty: IndexValueType) -> BTreeMap<IndexKey, Vec<u32>> {
    let mut out: BTreeMap<IndexKey, Vec<u32>> = BTreeMap::new();
    let mut pos = 0u32;
    for b in batches {
        for i in 0..b.len() {
            let v = ScalarValue::try_from_array(b, i).unwrap();
            if let Some(k) = IndexKey::from_scalar(&v, ty) {
                out.entry(k).or_default().push(pos);
            }
            pos += 1;
        }
    }
    out
}

/// Built object, stored in an in-memory store, with its catalog meta.
struct Fixture {
    store: Arc<dyn ObjectStore>,
    location: ObjectPath,
    meta: FileIndexMeta,
    built: BuiltIndex,
}

async fn build(batches: &[ArrayRef], ty: IndexValueType, row_group_rows: Vec<u64>) -> Fixture {
    let mut b = PostingBuilder::new(7, ty);
    for batch in batches {
        b.observe(batch).unwrap();
    }
    let built = b.finish(row_group_rows).unwrap();
    let data_path = "data/ducklake-0001.parquet";
    let location = ObjectPath::from(index_object_path(data_path, 7));
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(&location, built.bytes.clone().into())
        .await
        .unwrap();
    let meta = built.meta(data_path, 1234, 7, ty, index_file_name(data_path, 7));
    Fixture {
        store,
        location,
        meta,
        built,
    }
}

impl Fixture {
    async fn lookup(&self, keys: &[IndexKey]) -> Result<Postings> {
        lookup_postings(
            Arc::clone(&self.store),
            &self.location,
            &self.meta,
            keys,
            None,
        )
        .await
    }
}

fn total_rows(batches: &[ArrayRef]) -> u64 {
    batches.iter().map(|b| b.len() as u64).sum()
}

fn utf8_key(s: &str) -> IndexKey {
    IndexKey::Utf8(s.as_bytes().into())
}

/// Every present key, plus 100 absent ones, looked up alone equals brute
/// force; the per-key read of all keys at once does too.
async fn check_against_brute_force(batches: Vec<ArrayRef>, ty: IndexValueType, rng: &mut Rng) {
    let n = total_rows(&batches);
    let rgs = if n > 10 {
        vec![n / 3, n - n / 3]
    } else {
        vec![n]
    };
    let f = build(&batches, ty, rgs).await;
    let expected = brute_force(&batches, ty);

    // All keys at once, per key.
    let keys: Vec<IndexKey> = expected.keys().cloned().collect();
    let per_key = read_keys(Arc::clone(&f.store), &f.location, &f.meta, &keys, None)
        .await
        .unwrap();
    for (k, got) in keys.iter().zip(&per_key) {
        assert_eq!(got, &expected[k], "key {k:?}");
    }

    // Single-key lookups on a sample of present keys (all, when few).
    let step = (keys.len() / 200).max(1);
    for k in keys.iter().step_by(step) {
        let got = f.lookup(std::slice::from_ref(k)).await.unwrap();
        assert_eq!(got.rows, expected[k], "single lookup {k:?}");
    }

    // Absent keys.
    for i in 0..100u64 {
        let k = match ty {
            IndexValueType::Utf8 => utf8_key(&format!("absent-{i}-{}", rng.next())),
            IndexValueType::Int64 => {
                // Odd multiples of 1_000_003 / 2 never collide with the
                // generator's values (multiples of 1_000_003).
                IndexKey::Int64((rng.below(1 << 20) as i64 - (1 << 19)) * 1_000_003 + 500_001)
            },
        };
        assert!(!expected.contains_key(&k));
        let got = f.lookup(std::slice::from_ref(&k)).await.unwrap();
        assert!(got.rows.is_empty(), "absent key {k:?} gave {:?}", got.rows);
    }

    // Union of a few present and absent keys, with a duplicate.
    if keys.len() >= 3 {
        let pick =
            vec![keys[0].clone(), keys[keys.len() / 2].clone(), keys[keys.len() - 1].clone()];
        let mut want: Vec<u32> = pick.iter().flat_map(|k| expected[k].clone()).collect();
        want.sort_unstable();
        let mut query = pick.clone();
        query.push(match ty {
            IndexValueType::Utf8 => utf8_key("zz-absent"),
            IndexValueType::Int64 => IndexKey::Int64(i64::MIN),
        });
        query.push(pick[0].clone());
        assert_eq!(f.lookup(&query).await.unwrap().rows, want);
    }
}

fn batch_len(rng: &mut Rng, left: usize) -> usize {
    // Some empty batches, some single rows, mostly mid-size.
    match rng.below(10) {
        0 => 0,
        1 => 1.min(left),
        _ => (rng.below(20_000) as usize + 1).min(left),
    }
}

fn random_utf8_batches(rng: &mut Rng, rows: usize, ndv: u64, null_pct: u64) -> Vec<ArrayRef> {
    let mut out = Vec::new();
    let mut left = rows;
    while left > 0 {
        let len = batch_len(rng, left);
        let v: StringArray = (0..len)
            .map(|_| {
                if rng.below(100) < null_pct {
                    None
                } else {
                    // Long shared prefixes, past the 64-byte page-index truncation.
                    let id = rng.below(ndv);
                    Some(if id.is_multiple_of(5) {
                        format!("{}{id}", "p".repeat(80))
                    } else {
                        format!("user-{id}")
                    })
                }
            })
            .collect();
        out.push(Arc::new(v) as ArrayRef);
        left -= len;
    }
    out
}

fn random_int_batches(rng: &mut Rng, rows: usize, ndv: u64, null_pct: u64) -> Vec<ArrayRef> {
    let mut out = Vec::new();
    let mut left = rows;
    while left > 0 {
        let len = batch_len(rng, left);
        let v: Int64Array = (0..len)
            .map(|_| {
                if rng.below(100) < null_pct {
                    None
                } else {
                    Some((rng.below(ndv) as i64 - (ndv as i64 / 2)) * 1_000_003)
                }
            })
            .collect();
        out.push(Arc::new(v) as ArrayRef);
        left -= len;
    }
    out
}

#[tokio::test]
async fn random_utf8_matches_brute_force() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for (rows, ndv, nulls) in [(1_000, 10, 0), (50_000, 5_000, 10), (120_000, 100_000, 30)] {
        let batches = random_utf8_batches(&mut rng, rows, ndv, nulls);
        check_against_brute_force(batches, IndexValueType::Utf8, &mut rng).await;
    }
}

#[tokio::test]
async fn random_int64_matches_brute_force() {
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    for (rows, ndv, nulls) in [(1_000, 3, 5), (80_000, 40_000, 0), (60_000, 500, 50)] {
        let batches = random_int_batches(&mut rng, rows, ndv, nulls);
        check_against_brute_force(batches, IndexValueType::Int64, &mut rng).await;
    }
}

/// Over 2^20 postings, so the object has more than one row group.
#[tokio::test]
async fn many_values_span_index_row_groups() {
    let mut rng = Rng(42);
    let rows = (1 << 20) + 5_000;
    let batches = random_int_batches(&mut rng, rows, 200_000, 0);
    let f = build(&batches, IndexValueType::Int64, vec![rows as u64]).await;
    let md = parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(&f.built.bytes)
        .unwrap();
    assert!(
        md.num_row_groups() >= 2,
        "{} row groups",
        md.num_row_groups()
    );
    check_against_brute_force(batches, IndexValueType::Int64, &mut rng).await;
}

#[tokio::test]
async fn empty_file() {
    let f = build(&[], IndexValueType::Utf8, vec![]).await;
    assert_eq!(f.meta.row_count, 0);
    assert!(f.meta.heavy_hitters.is_empty());
    assert!(f.lookup(&[utf8_key("x")]).await.unwrap().rows.is_empty());
    // Empty batches and all-null batches also give an empty object.
    let batches: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        Arc::new(StringArray::from(vec![None::<&str>; 10])),
    ];
    let f = build(&batches, IndexValueType::Utf8, vec![10]).await;
    assert_eq!(f.meta.row_count, 10);
    assert!(f.lookup(&[utf8_key("")]).await.unwrap().rows.is_empty());
    // No keys: no read at all, empty answer.
    assert!(f.lookup(&[]).await.unwrap().rows.is_empty());
}

#[tokio::test]
async fn single_row_and_empty_string() {
    let batches: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![Some("")]))];
    let f = build(&batches, IndexValueType::Utf8, vec![1]).await;
    assert_eq!(f.lookup(&[utf8_key("")]).await.unwrap().rows, vec![0]);
    assert!(f.lookup(&[utf8_key("a")]).await.unwrap().rows.is_empty());
}

#[tokio::test]
async fn all_equal_values() {
    let batches: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![5i64; 30_000])),
        Arc::new(Int64Array::from(vec![5i64; 20_000])),
    ];
    let f = build(&batches, IndexValueType::Int64, vec![25_000, 25_000]).await;
    let got = f.lookup(&[IndexKey::Int64(5)]).await.unwrap();
    assert_eq!(got.rows, (0..50_000).collect::<Vec<u32>>());
    assert!(
        f.lookup(&[IndexKey::Int64(4)])
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(
        f.lookup(&[IndexKey::Int64(6)])
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(f.meta.heavy_hitters, vec![(IndexKey::Int64(5), 50_000)]);
}

#[tokio::test]
async fn string_encodings_give_the_same_object() {
    let values = vec![Some("b"), None, Some("a"), Some("b"), Some("ü-ñ")];
    let utf8: ArrayRef = Arc::new(StringArray::from(values.clone()));
    let large: ArrayRef = Arc::new(LargeStringArray::from(values.clone()));
    let view: ArrayRef = Arc::new(StringViewArray::from(values.clone()));
    let dict: ArrayRef = Arc::new(values.iter().copied().collect::<DictionaryArray<DictKey>>());
    let reference = build(&[utf8], IndexValueType::Utf8, vec![5]).await;
    for other in [large, view, dict] {
        let f = build(&[other], IndexValueType::Utf8, vec![5]).await;
        assert_eq!(f.built.bytes, reference.built.bytes);
    }
    assert_eq!(
        IndexValueType::for_arrow(&DataType::Utf8View),
        Some(IndexValueType::Utf8)
    );
    assert_eq!(
        IndexKey::from_scalar(
            &ScalarValue::Utf8View(Some("b".into())),
            IndexValueType::Utf8
        ),
        IndexKey::from_scalar(&ScalarValue::Utf8(Some("b".into())), IndexValueType::Utf8),
    );
    assert_eq!(
        reference.lookup(&[utf8_key("b")]).await.unwrap().rows,
        vec![0, 3]
    );
    assert_eq!(
        reference.lookup(&[utf8_key("ü-ñ")]).await.unwrap().rows,
        vec![4]
    );
}

#[tokio::test]
async fn int32_widens_to_int64() {
    let i32s: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(-3),
        Some(i32::MAX),
        None,
        Some(-3),
    ]));
    let i64s: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(-3),
        Some(i32::MAX as i64),
        None,
        Some(-3),
    ]));
    assert_eq!(
        IndexValueType::for_arrow(&DataType::Int32),
        Some(IndexValueType::Int64)
    );
    let a = build(&[i32s], IndexValueType::Int64, vec![4]).await;
    let b = build(&[i64s], IndexValueType::Int64, vec![4]).await;
    assert_eq!(a.built.bytes, b.built.bytes);
    let key = IndexKey::from_scalar(&ScalarValue::Int32(Some(-3)), IndexValueType::Int64).unwrap();
    assert_eq!(key, IndexKey::Int64(-3));
    assert_eq!(a.lookup(&[key]).await.unwrap().rows, vec![0, 3]);
}

#[test]
fn from_scalar_rejects_null_and_mismatch() {
    use IndexValueType::*;
    assert_eq!(IndexKey::from_scalar(&ScalarValue::Utf8(None), Utf8), None);
    assert_eq!(
        IndexKey::from_scalar(&ScalarValue::Int64(None), Int64),
        None
    );
    assert_eq!(
        IndexKey::from_scalar(&ScalarValue::Int64(Some(1)), Utf8),
        None
    );
    assert_eq!(
        IndexKey::from_scalar(&ScalarValue::Utf8(Some("1".into())), Int64),
        None
    );
    assert_eq!(
        IndexKey::from_scalar(&ScalarValue::Float64(Some(1.0)), Int64),
        None
    );
    assert_eq!(IndexKey::from_scalar(&ScalarValue::Null, Int64), None);
    let dict = ScalarValue::Dictionary(
        Box::new(DataType::Int32),
        Box::new(ScalarValue::Utf8(Some("x".into()))),
    );
    assert_eq!(IndexKey::from_scalar(&dict, Utf8), Some(utf8_key("x")));
    assert_eq!(IndexValueType::for_arrow(&DataType::Float64), None);
    assert_eq!(IndexValueType::for_arrow(&DataType::Int16), None);
    for ty in [Utf8, Int64] {
        assert_eq!(IndexValueType::parse(ty.as_str()), Some(ty));
    }
    assert_eq!(IndexValueType::parse("float64"), None);
}

#[test]
fn observe_rejects_wrong_type_and_finish_checks_row_groups() {
    let mut b = PostingBuilder::new(1, IndexValueType::Int64);
    let strings: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    assert!(b.observe(&strings).is_err());
    assert_eq!(b.rows_seen(), 0);
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    b.observe(&ints).unwrap();
    assert_eq!(b.rows_seen(), 3);
    assert!(
        PostingBuilder::new(1, IndexValueType::Int64)
            .finish(vec![1])
            .is_err()
    );
    assert!(b.finish(vec![1, 1]).is_err());
}

#[test]
fn heavy_hitter_threshold_and_list() {
    assert_eq!(heavy_hitter_threshold(0), HEAVY_HITTER_MIN_COUNT);
    assert_eq!(heavy_hitter_threshold(4096 * 1024), 1024);
    assert_eq!(heavy_hitter_threshold(5_500_000), 5_500_000 / 4096);
    assert_eq!(heavy_hitter_threshold(4096 * 5000 + 4095), 5000);

    // Floor case: above 1024 is heavy, exactly 1024 is not.
    let mut v = vec![1i64; 1026];
    v.extend(vec![2i64; 1024]);
    v.extend(3..100i64);
    let mut b = PostingBuilder::new(1, IndexValueType::Int64);
    b.observe(&(Arc::new(Int64Array::from(v.clone())) as ArrayRef))
        .unwrap();
    let built = b.finish(vec![v.len() as u64]).unwrap();
    assert_eq!(built.heavy_hitters, vec![(IndexKey::Int64(1), 1026)]);
    let meta = built.meta("p", 1, 1, IndexValueType::Int64, "q".into());
    assert_eq!(meta.heavy_hitter_count(&IndexKey::Int64(1)), Some(1026));
    assert_eq!(meta.heavy_hitter_count(&IndexKey::Int64(2)), None);
}

#[test]
fn heavy_hitter_list_is_capped_by_count() {
    // HEAVY_HITTER_MAX + 2 values just above the threshold, two of them busier.
    let per = HEAVY_HITTER_MIN_COUNT as usize + 1;
    let n_vals = HEAVY_HITTER_MAX + 2;
    let mut v: Vec<i64> = Vec::with_capacity(per * n_vals + 10);
    for k in 0..n_vals as i64 {
        let extra = if k == 3 || k == 900 {
            5
        } else {
            0
        };
        v.extend(std::iter::repeat_n(k, per + extra));
    }
    let mut b = PostingBuilder::new(1, IndexValueType::Int64);
    b.observe(&(Arc::new(Int64Array::from(v.clone())) as ArrayRef))
        .unwrap();
    let built = b.finish(vec![v.len() as u64]).unwrap();
    assert_eq!(built.heavy_hitters.len(), HEAVY_HITTER_MAX);
    assert!(built.heavy_hitters.windows(2).all(|w| w[0].0 < w[1].0));
    for k in [3, 900] {
        assert!(
            built
                .heavy_hitters
                .iter()
                .any(|(key, c)| *key == IndexKey::Int64(k) && *c == per as u64 + 5)
        );
    }
}

#[test]
fn json_round_trips() {
    let rgs = vec![1u64 << 20, 7, 0, u64::MAX];
    assert_eq!(
        row_group_rows_from_json(&row_group_rows_to_json(&rgs)).unwrap(),
        rgs
    );
    assert_eq!(row_group_rows_to_json(&[]), "[]");
    assert!(row_group_rows_from_json("[1,-2]").is_err());
    assert!(row_group_rows_from_json("nope").is_err());

    let hh = vec![
        (IndexKey::Utf8(Box::new([])), 3),
        (IndexKey::Utf8(vec![0x00, 0xff, b'"', b'\\'].into()), 9),
        (utf8_key("user-1"), 5000),
    ];
    let json = heavy_hitters_to_json(&hh);
    assert_eq!(
        heavy_hitters_from_json(&json, IndexValueType::Utf8).unwrap(),
        hh
    );
    let ints = vec![
        (IndexKey::Int64(i64::MIN), 1),
        (IndexKey::Int64(-1), 2),
        (IndexKey::Int64(i64::MAX), 3),
    ];
    let json = heavy_hitters_to_json(&ints);
    assert_eq!(
        heavy_hitters_from_json(&json, IndexValueType::Int64).unwrap(),
        ints
    );
    // Out-of-order input comes back sorted.
    assert_eq!(
        heavy_hitters_from_json("[[3,1],[1,2]]", IndexValueType::Int64).unwrap(),
        vec![(IndexKey::Int64(1), 2), (IndexKey::Int64(3), 1)]
    );
    assert!(heavy_hitters_from_json(&json, IndexValueType::Utf8).is_err());
    assert!(heavy_hitters_from_json("[[\"!!notbase64\",1]]", IndexValueType::Utf8).is_err());
    assert!(heavy_hitters_from_json("[[1.5,1]]", IndexValueType::Int64).is_err());
    assert_eq!(
        heavy_hitters_from_json("[]", IndexValueType::Int64).unwrap(),
        vec![]
    );
}

#[test]
fn object_paths() {
    assert_eq!(
        index_object_path("a/b/f.parquet", 12),
        "a/b/f.parquet.c12.v1.pidx"
    );
    assert_eq!(index_object_path("f.parquet", 3), "f.parquet.c3.v1.pidx");
    assert_eq!(
        index_file_name("s3://bkt/t/f.parquet", 3),
        "f.parquet.c3.v1.pidx"
    );
    assert_eq!(index_file_name("f.parquet", 3), "f.parquet.c3.v1.pidx");
    assert!(!index_object_path("x.parquet", 1).ends_with(".parquet"));
}

fn rg_access(plan: &ParquetAccessPlan) -> Vec<RowGroupAccess> {
    plan.inner().to_vec()
}

fn sel(selectors: Vec<RowSelector>) -> RowGroupAccess {
    RowGroupAccess::Selection(RowSelection::from(selectors))
}

#[test]
fn access_plan_row_group_edges() {
    let rgs = [10u64, 10, 5, 3];
    // Last row of rg0, first and last of rg1; rg2 untouched; rg3 full.
    let p = Postings {
        rows: vec![9, 10, 19, 25, 26, 27],
    };
    let plan = access_plan(&p, &rgs).unwrap();
    assert_eq!(
        rg_access(&plan),
        vec![
            sel(vec![RowSelector::skip(9), RowSelector::select(1)]),
            sel(vec![
                RowSelector::select(1),
                RowSelector::skip(8),
                RowSelector::select(1)
            ]),
            RowGroupAccess::Skip,
            RowGroupAccess::Scan,
        ]
    );
    // Consecutive rows merge into one selector; first row of the file.
    let p = Postings {
        rows: vec![0, 1, 2, 5],
    };
    assert_eq!(
        rg_access(&access_plan(&p, &[8]).unwrap()),
        vec![sel(vec![
            RowSelector::select(3),
            RowSelector::skip(2),
            RowSelector::select(1),
            RowSelector::skip(2)
        ])]
    );
    // Empty postings: every group skipped.
    let plan = access_plan(&Postings::default(), &rgs).unwrap();
    assert!(
        rg_access(&plan)
            .iter()
            .all(|a| matches!(a, RowGroupAccess::Skip))
    );
    // Zero-row groups are skipped without consuming postings.
    let plan = access_plan(
        &Postings {
            rows: vec![0],
        },
        &[0, 1, 0],
    )
    .unwrap();
    assert_eq!(
        rg_access(&plan),
        vec![RowGroupAccess::Skip, RowGroupAccess::Scan, RowGroupAccess::Skip]
    );
    // Out of range, unsorted or duplicated postings: no plan.
    assert!(
        access_plan(
            &Postings {
                rows: vec![28]
            },
            &rgs
        )
        .is_none()
    );
    assert!(
        access_plan(
            &Postings {
                rows: vec![0]
            },
            &[]
        )
        .is_none()
    );
    assert!(
        access_plan(
            &Postings {
                rows: vec![3, 2]
            },
            &rgs
        )
        .is_none()
    );
    assert!(
        access_plan(
            &Postings {
                rows: vec![2, 2]
            },
            &rgs
        )
        .is_none()
    );
    // Selections cover each group's rows exactly.
    let p = Postings {
        rows: vec![1, 13, 22],
    };
    for (a, n) in rg_access(&access_plan(&p, &rgs).unwrap()).iter().zip(rgs) {
        if let RowGroupAccess::Selection(s) = a {
            assert_eq!(s.row_count() + s.skipped_row_count(), n as usize);
        }
    }
}

#[tokio::test]
async fn footer_mismatch_is_an_error() {
    let batches: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let f = build(&batches, IndexValueType::Int64, vec![3]).await;
    let key = [IndexKey::Int64(1)];
    assert_eq!(f.lookup(&key).await.unwrap().rows, vec![0]);
    type Mutate = Box<dyn Fn(&mut FileIndexMeta)>;
    let cases: Vec<(&str, Mutate)> = vec![
        ("column_id", Box::new(|m| m.column_id += 1)),
        ("row_count", Box::new(|m| m.row_count += 1)),
        ("format_version", Box::new(|m| m.format_version = 2)),
        (
            "value_type",
            Box::new(|m| m.value_type = IndexValueType::Utf8),
        ),
        ("size short", Box::new(|m| m.index_size_bytes -= 1)),
        ("size long", Box::new(|m| m.index_size_bytes += 1)),
        ("size negative", Box::new(|m| m.index_size_bytes = -1)),
    ];
    for (what, mutate) in cases {
        let mut meta = f.meta.clone();
        mutate(&mut meta);
        let keys: Vec<IndexKey> = match meta.value_type {
            IndexValueType::Utf8 => vec![utf8_key("1")],
            IndexValueType::Int64 => key.to_vec(),
        };
        let r = lookup_postings(Arc::clone(&f.store), &f.location, &meta, &keys, None).await;
        assert!(r.is_err(), "{what} mismatch was accepted: {r:?}");
    }
    // A key of the wrong type is an error, not an empty answer.
    assert!(f.lookup(&[utf8_key("1")]).await.is_err());
}

#[tokio::test]
async fn corrupt_or_missing_object_is_an_error() {
    let batches: Vec<ArrayRef> = vec![Arc::new(Int64Array::from((0..5000).collect::<Vec<i64>>()))];
    let f = build(&batches, IndexValueType::Int64, vec![5000]).await;
    let key = [IndexKey::Int64(17)];
    let good = f.built.bytes.to_vec();

    let mut variants: Vec<(&str, Vec<u8>)> = vec![
        ("garbage", vec![0x5a; good.len()]),
        ("zeroed footer", {
            let mut b = good.clone();
            let n = b.len();
            b[n - 200..n - 8].fill(0);
            b
        }),
        ("bad magic", {
            let mut b = good.clone();
            let n = b.len();
            b[n - 1] = b'X';
            b
        }),
    ];
    // A data parquet file is not an index object.
    let plain = {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        buf
    };
    variants.push(("plain parquet", plain));
    for (what, bytes) in variants {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store.put(&f.location, bytes.clone().into()).await.unwrap();
        let mut meta = f.meta.clone();
        meta.index_size_bytes = bytes.len() as i64;
        let r = lookup_postings(store, &f.location, &meta, &key, None).await;
        assert!(r.is_err(), "{what} accepted: {r:?}");
    }

    // A corrupt data page (footer intact) errors rather than returning rows.
    let md = parquet::file::metadata::ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&f.built.bytes)
        .unwrap();
    let loc = &md.offset_index().unwrap()[0][1].page_locations()[0];
    let mut b = good.clone();
    let start = loc.offset as usize + 20;
    let end = loc.offset as usize + loc.compressed_page_size as usize;
    b[start..end].fill(0xa5);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store.put(&f.location, b.into()).await.unwrap();
    let r = lookup_postings(store, &f.location, &f.meta, &key, None).await;
    assert!(r.is_err(), "corrupt row page accepted: {r:?}");

    // Missing object.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    assert!(
        lookup_postings(store, &f.location, &f.meta, &key, None)
            .await
            .is_err()
    );
}

/// Records every byte range read, by any read method.
#[derive(Debug)]
struct RecordingStore {
    inner: Arc<dyn ObjectStore>,
    ranges: Mutex<Vec<Range<u64>>>,
}

impl RecordingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            ranges: Mutex::new(Vec::new()),
        })
    }

    fn take(&self) -> Vec<Range<u64>> {
        std::mem::take(&mut self.ranges.lock().unwrap())
    }
}

impl std::fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let r = self.inner.get_opts(location, options).await?;
        self.ranges.lock().unwrap().push(r.range.clone());
        Ok(r)
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        self.ranges.lock().unwrap().extend(ranges.iter().cloned());
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Data pages (per column) touched by `ranges`.
fn pages_touched(md: &ParquetMetaData, ranges: &[Range<u64>]) -> [usize; 2] {
    let mut out = [0usize; 2];
    for rg in md.offset_index().unwrap() {
        for (col, oi) in rg.iter().enumerate() {
            for loc in oi.page_locations() {
                let p = loc.offset as u64..loc.offset as u64 + loc.compressed_page_size as u64;
                if ranges.iter().any(|r| r.start < p.end && p.start < r.end) {
                    out[col] += 1;
                }
            }
        }
    }
    out
}

/// A 5.5M-row object (a production user_id file): a point lookup reads at
/// most 3 data pages per column, and a warm metadata cache serves the footer
/// and page index with no reads.
#[tokio::test]
async fn point_lookup_reads_few_pages() {
    const ROWS: usize = 5_500_000;
    const NDV: u64 = 200_000;
    let mut rng = Rng(7);
    let mut b = PostingBuilder::new(7, IndexValueType::Utf8);
    let mut probe_rows = Vec::new();
    let mut pos = 0u32;
    for _ in 0..(ROWS / 100_000) {
        let v: StringArray = (0..100_000)
            .map(|_| {
                let id = rng.below(NDV);
                if id == 4242 {
                    probe_rows.push(pos);
                }
                pos += 1;
                Some(format!("user-{id:08}"))
            })
            .collect();
        b.observe(&(Arc::new(v) as ArrayRef)).unwrap();
    }
    assert!(!probe_rows.is_empty());
    let built = b.finish(vec![ROWS as u64]).unwrap();
    let location = ObjectPath::from("t/f.parquet.c7.v1.pidx");
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    mem.put(&location, built.bytes.clone().into())
        .await
        .unwrap();
    let meta = built.meta(
        "t/f.parquet",
        1,
        7,
        IndexValueType::Utf8,
        "f.parquet.c7.v1.pidx".into(),
    );
    let md = parquet::file::metadata::ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&built.bytes)
        .unwrap();

    let rec = RecordingStore::new(mem);
    let store: Arc<dyn ObjectStore> = rec.clone();
    let cache = RuntimeEnv::default()
        .cache_manager
        .get_file_metadata_cache();
    let key = [utf8_key("user-00004242")];

    let got = lookup_postings(
        Arc::clone(&store),
        &location,
        &meta,
        &key,
        Some(Arc::clone(&cache)),
    )
    .await
    .unwrap();
    assert_eq!(got.rows, probe_rows);
    let [value_pages, row_pages] = pages_touched(&md, &rec.take());
    assert!(
        value_pages <= 3 && row_pages <= 3,
        "cold read {value_pages} value / {row_pages} row pages"
    );

    let got = lookup_postings(Arc::clone(&store), &location, &meta, &key, Some(cache))
        .await
        .unwrap();
    assert_eq!(got.rows, probe_rows);
    let warm = rec.take();
    let footer_start = built.bytes.len() as u64 - 8;
    assert!(
        warm.iter().all(|r| r.end <= footer_start),
        "warm lookup re-read the footer: {warm:?}"
    );
    let [value_pages, row_pages] = pages_touched(&md, &warm);
    assert!(value_pages <= 3 && row_pages <= 3);
    let read: u64 = warm.iter().map(|r| r.end - r.start).sum();
    assert!(
        read < built.bytes.len() as u64 / 20,
        "warm read {read} of {}",
        built.bytes.len()
    );

    // An absent key reads no row pages.
    let absent = [utf8_key("user-00004242a")];
    assert!(
        lookup_postings(store, &location, &meta, &absent, None)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    let [value_pages, _] = pages_touched(&md, &rec.take());
    assert!(
        value_pages <= 2,
        "{value_pages} value pages for an absent key"
    );
}

#[tokio::test]
async fn posting_cache_serves_repeat_lookups() {
    let batches: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2, 1]))];
    let f = build(&batches, IndexValueType::Int64, vec![6]).await;
    let rec = RecordingStore::new(Arc::clone(&f.store));
    let store: Arc<dyn ObjectStore> = rec.clone();
    let cache = PostingCache::new(1 << 20);
    let q = |keys: Vec<IndexKey>| {
        let store = Arc::clone(&store);
        let (cache, f) = (&cache, &f);
        async move {
            lookup_postings_cached(cache, store, &f.location, &f.meta, &keys, None)
                .await
                .unwrap()
        }
    };

    assert_eq!(
        q(vec![IndexKey::Int64(1), IndexKey::Int64(9)]).await.rows,
        vec![0, 2, 5]
    );
    assert!(!rec.take().is_empty());
    // Both keys (the absent one too) are cached.
    assert_eq!(
        q(vec![IndexKey::Int64(9), IndexKey::Int64(1)]).await.rows,
        vec![0, 2, 5]
    );
    assert!(rec.take().is_empty(), "cached lookup read the store");
    // A partial hit reads the missing key and unions correctly.
    assert_eq!(
        q(vec![IndexKey::Int64(1), IndexKey::Int64(2)]).await.rows,
        vec![0, 1, 2, 4, 5]
    );
    assert!(!rec.take().is_empty());
    // Absent key 9 is cached as empty; key 2 as its rows. Peek without
    // counting, then check the counters.
    let peek = |k: i64| {
        cache
            .cache
            .get(&(Arc::from(f.location.as_ref()), IndexKey::Int64(k)))
    };
    assert_eq!(peek(9).unwrap().rows, Vec::<u32>::new());
    assert_eq!(peek(2).unwrap().rows, vec![1, 4]);
    // Per-key counters: miss 1,9; hit 9,1; hit 1, miss 2.
    assert_eq!((cache.hits(), cache.misses()), (3, 3));
    // Entries are per object path.
    assert!(cache.get("other/path", &IndexKey::Int64(1)).is_none());
    assert_eq!(cache.misses(), 4);
}

#[test]
fn posting_cache_is_byte_bounded() {
    let cache = PostingCache::new(64 * 1024);
    for i in 0..100 {
        cache.insert(
            "p",
            IndexKey::Int64(i),
            Arc::new(Postings {
                rows: (0..1000).collect(),
            }),
        );
    }
    let size = cache.weighted_size();
    assert!(size <= 64 * 1024, "weighted size {size}");
    assert!(size > 0);
    assert_eq!(
        PostingCache::global().max_bytes(),
        PostingCache::DEFAULT_MAX_BYTES
    );
    assert!(std::ptr::eq(PostingCache::global(), PostingCache::global()));
}

#[test]
fn builder_and_index_are_send_static() {
    fn assert_send<T: Send + 'static>() {}
    assert_send::<PostingBuilder>();
    assert_send::<BuiltIndex>();
    assert_send::<FileIndexMeta>();
    let b = PostingBuilder::new(3, IndexValueType::Utf8);
    assert!(format!("{b:?}").contains("column_id: 3"));
}
