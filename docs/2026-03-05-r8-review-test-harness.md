# R8 Test Harness Review

**Date**: 2026-03-05
**Reviewer**: r8-test-harness-review agent
**Branch**: `ducklake-features/integration`
**Commit**: c7d79d1

---

## Executive Summary

The R7 P3 fixes resolved 7 of the 11 findings from the R7 test harness review. Specifically:
- **R7-TH-001** (assert_results_eq_strict dead code): Now wired into timestamp and date roundtrip tests.
- **R7-TH-003** (rewrite_order_by_all naive): `hybrid_asyncdb.rs` version is now string-literal aware.
- **R7-TH-004** (transaction routing untested): End-to-end test added (`test_transaction_routing_end_to_end`).
- **R7-TH-005** (weak boolean NULL assertion): Fixed to `assert_eq!(rows[2][1], "NULL")`.
- **R7-TH-009** (concurrent write read-back): Now reads back and verifies count = 11.
- Write test value assertions strengthened (append_test, streaming_write values verified).
- Cross-engine alter tests expanded with ADD COLUMN and DROP COLUMN tests.

The remaining test infrastructure is in good shape. This review found **3 Low** and **4 Informational** findings — no medium or high severity issues. The test suite is mature and well-guarded against false positives.

**Severity summary**: 0 Medium, 3 Low, 4 Informational

---

## Findings

### R8-TH-001: `rewrite_order_by_all` in `sqllogictest_runner.rs` still naive [Low]

**File**: `tests/sqllogictest_runner.rs:518-536`

The `rewrite_order_by_all` function in `sqllogictest_runner.rs` still uses naive `str::find("ORDER BY ALL")` matching on uppercased SQL. The R7 P3 fix only updated the version in `hybrid_asyncdb.rs` (line 381) to be string-literal aware. The two implementations have diverged:

- `hybrid_asyncdb.rs:381`: Counts single-quote chars to detect string context — **correct**
- `sqllogictest_runner.rs:518`: Raw `upper.find("ORDER BY ALL")` — **will match inside string literals**

**Risk**: A DuckLake SLT test file containing `WHERE col = 'ORDER BY ALL'` would have the string literal corrupted during preprocessing. Low probability in practice since DuckLake test files rarely contain this pattern, but the discrepancy between the two implementations is a maintenance hazard.

**Recommendation**: Port the string-literal-aware logic from `hybrid_asyncdb.rs` to `sqllogictest_runner.rs`, or extract a shared helper.

---

### R8-TH-002: Decimal roundtrip test uses `assert_results_eq` instead of strict [Low]

**File**: `tests/cross_engine_tests.rs:891`

The `cross_engine_decimal_type_roundtrip` test uses `assert_results_eq("decimal_roundtrip", ...)` with float normalization. The R7 P3 fix correctly switched timestamp and date roundtrips to `assert_results_eq_strict`, but decimal was left with the normalizing version.

`normalize_value` (test_utils.rs:299-311) detects values containing `.` as floats and reformats them to 6 decimal places via `format!("{:.6}", f)`. This means a decimal value like `"999.99"` would be normalized to `"999.990000"` and `"999.9900"` → `"999.990000"`, masking precision/scale differences between DuckDB and DataFusion.

The test also has explicit `assert_eq!` checks on individual cells (lines 888-890), which mitigates this somewhat, but the final `assert_results_eq` call is weaker than it should be for a type-roundtrip test.

**Risk**: A bug that changes decimal scale (e.g., `999.99` → `999.990`) would be silently accepted.

**Recommendation**: Change line 891 to `assert_results_eq_strict("decimal_roundtrip", &duckdb_rows, &df_rows)`.

---

### R8-TH-003: `assert_results_eq_strict` only used in 2 of 7+ type roundtrip tests [Low]

**File**: `tests/cross_engine_tests.rs`

The strict comparison function is now used for timestamp (line 814) and date (line 851) roundtrips, but the following type roundtrip tests still use the normalizing `assert_results_eq`:
- `cross_engine_decimal_type_roundtrip` (line 891)
- `cross_engine_df_write_typed_data_duckdb_read` (no cross-engine assert at all — only DF-side checks)
- `cross_engine_assert_query_eq_both_engines` (line 405 — general data, not type-specific)

For type roundtrip tests specifically, strict comparison is the correct choice since the whole point is verifying exact type fidelity.

**Risk**: Type confusion between engines (e.g., integer formatted as float) would be masked by normalization.

**Recommendation**: Audit all `cross_engine_*_type_roundtrip` tests and switch to `assert_results_eq_strict` where type fidelity is being tested.

---

### R8-TH-004: `convert_batch_to_strings` in hybrid_asyncdb still duplicates test_utils logic [Informational]

**File**: `tests/hybrid_asyncdb.rs:654-800` vs `tests/common/test_utils.rs:66-198`

This was noted in R7-TH-008 and remains unchanged. The two implementations are currently in sync, but the technical debt remains. The documented reason (hybrid_asyncdb.rs can't declare `mod common`) is legitimate.

One subtle difference: `hybrid_asyncdb.rs` uses `format_float()` (line 628-644) which handles NaN/Inf specially, while `test_utils.rs` does not. This is intentional (SLT adapter needs DuckDB format compatibility), but the divergence is undocumented.

**Status**: Informational carry-forward. No action required.

---

### R8-TH-005: R7-TH-002 (cte_wraps_dml double-quote handling) was fixed but not documented [Informational]

**File**: `tests/hybrid_asyncdb.rs:155-207`

The R7-TH-002 finding noted that `cte_wraps_dml` didn't handle double-quoted identifiers. The R7 P3 fix added double-quote tracking (lines 159, 176-185). The fix is correct — it now tracks `in_double_quote` state and skips escaped double quotes (`""`).

However, there is no unit test exercising this specific case (a CTE with a double-quoted identifier containing a DML keyword). The existing `test_cte_wraps_dml_string_literals` test only covers single-quoted strings.

**Risk**: Very low — the fix is correct and unlikely to regress without a code change to the function.

**Recommendation**: Consider adding a test case like:
```rust
assert!(!HybridDuckLakeDB::cte_wraps_dml(
    r#"WITH SRC AS (SELECT "INSERT" FROM T) SELECT * FROM SRC"#
));
```

---

### R8-TH-006: SLT preprocessor `statement error` → `statement ok` conversion lacks tests [Informational]

**File**: `tests/sqllogictest_runner.rs:687-736`

The `HYBRID_INCOMPATIBLE_PATTERNS` array (lines 687-714) defines 5 error patterns that are automatically converted from `statement error` to `statement ok`. These patterns represent real behavioral differences between hybrid mode and pure DuckDB mode, and the conversion is logged via `tracing::warn`.

However, there are no unit tests for `is_hybrid_incompatible_error_with_sql`. If a new pattern is added incorrectly or an existing pattern becomes too broad, it could silently convert real error tests to `statement ok`, creating false passes.

**Risk**: Low — the function is simple pattern matching, and `tracing::warn` provides visibility. But the lack of tests means changes aren't validated.

**Recommendation**: Add a few unit tests for `is_hybrid_incompatible_error_with_sql` covering match and non-match cases.

---

### R8-TH-007: `batches_to_sorted_strings` masks order-dependent bugs [Informational]

**File**: `tests/common/test_utils.rs:253-257`

`batches_to_sorted_strings` sorts results before comparison. It is used in `write_partition_tests.rs` and can be used anywhere ordering is non-deterministic. However, tests that call `.sort()` directly on results (26 call sites across 11 test files) may be masking order-dependent bugs in the query engine. If a query specifies `ORDER BY` but the implementation ignores it, sorting the results before comparison would hide the bug.

The call sites were reviewed and most are appropriate (results from unordered queries or concurrent writes). A few border cases:
- `merge_tests.rs:212` sorts after an `ORDER BY id` query — this could mask a broken ORDER BY implementation
- Same pattern at `merge_tests.rs:291` and `merge_tests.rs:376`

**Risk**: Very low — ORDER BY correctness is a DataFusion core feature, not a DuckLake issue.

**Status**: Informational. The merge tests could remove the `.sort()` since they use `ORDER BY id`, but this is a minor style issue.

---

## R7 Findings Resolution Status

| R7 ID | Severity | Status | Resolution |
|-------|----------|--------|------------|
| R7-TH-001 | Low | **Fixed** | `assert_results_eq_strict` now used in timestamp/date roundtrips |
| R7-TH-002 | Low | **Fixed** | Double-quote handling added to `cte_wraps_dml` |
| R7-TH-003 | Low | **Partial** | Fixed in `hybrid_asyncdb.rs` only; `sqllogictest_runner.rs` still naive (R8-TH-001) |
| R7-TH-004 | Medium | **Fixed** | End-to-end transaction routing test added |
| R7-TH-005 | Low | **Fixed** | Boolean NULL assertion uses strict `assert_eq!` |
| R7-TH-006 | Low | Open | `parse_table_name` integration test added in `table_function_tests.rs` |
| R7-TH-007 | Info | Accepted | Vacuous-pass guard with warning is sufficient |
| R7-TH-008 | Info | Open | Code duplication remains (R8-TH-004) |
| R7-TH-009 | Low | **Fixed** | Concurrent write test reads back and verifies count |
| R7-TH-010 | Info | Open | Partitioned DML tests still missing (acceptable for read-only scope) |
| R7-TH-011 | Info | Accepted | Quoted edge case prevented by parent function |

---

## Coverage Gaps Remaining

1. **No negative tests for SLT preprocessor error conversion**: `is_hybrid_incompatible_error_with_sql` has no unit tests (R8-TH-006).

2. **Decimal type roundtrip uses normalizing comparison**: Could mask precision/scale bugs (R8-TH-002).

3. **`rewrite_order_by_all` divergence**: Two implementations, one fixed, one not (R8-TH-001).

4. **No cross-engine test for DF-written boolean → DuckDB read**: Documented as DuckDB internal assertion failure on inlined boolean data. Tracked but not tested.

5. **`test_write_multiple_batches`** (write_tests.rs:229): Still only checks the first value of the second column (`values.value(0) == "a"`), not all 4 values. This was noted in the R7 review's coverage gap #5.

---

## Summary Table

| ID | Severity | Component | Issue |
|----|----------|-----------|-------|
| R8-TH-001 | Low | sqllogictest_runner | `rewrite_order_by_all` still naive (diverged from hybrid_asyncdb fix) |
| R8-TH-002 | Low | cross_engine_tests | Decimal roundtrip uses normalizing comparison |
| R8-TH-003 | Low | cross_engine_tests | `assert_results_eq_strict` not used in all type roundtrips |
| R8-TH-004 | Info | hybrid_asyncdb | Duplicated type conversion logic (carry-forward) |
| R8-TH-005 | Info | hybrid_asyncdb | `cte_wraps_dml` double-quote fix lacks unit test |
| R8-TH-006 | Info | sqllogictest_runner | SLT error→ok conversion patterns lack unit tests |
| R8-TH-007 | Info | test infrastructure | `.sort()` after `ORDER BY` queries in merge tests |

---

## Verdict

The R7 P3 fixes were **well-targeted and correct**. The test harness has improved meaningfully since R7:
- Transaction routing is now tested end-to-end
- Type roundtrip tests use strict comparison for timestamp/date
- Concurrent write tests verify data integrity
- Boolean NULL assertions are strict
- Cross-engine ALTER tests cover ADD/DROP COLUMN

The remaining findings are all Low or Informational. The most actionable is R8-TH-001 (divergent `rewrite_order_by_all` implementations) — a simple port of the string-literal-aware logic. The test infrastructure is mature and provides strong false-positive resistance.
