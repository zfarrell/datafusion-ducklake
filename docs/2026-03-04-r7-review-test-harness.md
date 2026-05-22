# R7 Test Harness Review — Post-R6-Fixes

**Date**: 2026-03-04
**Reviewer**: test-harness-review agent
**Branch**: `ducklake-features/integration`
**Commit**: f7eadb0

---

## Executive Summary

The R6 test infrastructure changes significantly improved test coverage and correctness. The hybrid adapter (`hybrid_asyncdb.rs`) now properly handles CTE-wrapped DML, double-quoted identifiers, ORDER BY ALL, and transaction state tracking. Cross-engine tests were expanded with 10+ new tests for DML operations (R6-S-032), schema assertions (R6-S-048), and BOOLEAN roundtrips (R6-S-049). The SLT preprocessor was tightened to avoid vacuous passes.

However, several findings require attention: dead code (`assert_results_eq_strict` is never used), missing `parse_table_name` integration tests, edge cases in the CTE parser and ORDER BY ALL rewriter, and partition validation tests that exist only in a separate file without cross-engine verification.

**Severity summary**: 1 Medium, 6 Low, 4 Informational

---

## Findings

### R7-TH-001: `assert_results_eq_strict` defined but never called [Low]

**File**: `tests/common/test_utils.rs:317`

The `assert_results_eq_strict` function was added in R6 to enable strict (non-normalized) value comparison, preventing false passes from float normalization. However, no test actually calls it. All cross-engine and write tests use `assert_results_eq` (with normalization), meaning type confusion bugs (e.g., integer `"1"` matching float `"1.0"`) could slip through undetected.

**Risk**: False positive — tests that should detect type mismatches won't catch them because float normalization rounds both to the same value.

**Recommendation**: Use `assert_results_eq_strict` in at least the type-roundtrip tests (`cross_engine_decimal_type_roundtrip`, `cross_engine_timestamp_type_roundtrip`, etc.) where exact string matching is the whole point.

---

### R7-TH-002: `cte_wraps_dml` does not handle double-quoted identifiers [Low]

**File**: `tests/hybrid_asyncdb.rs:152-191`

The CTE-wrapped DML parser (`cte_wraps_dml`) correctly handles single-quoted strings and parenthesis depth, but operates on the already-uppercased SQL. Double-quoted identifiers are not tracked, so a query like:

```sql
WITH src AS (SELECT "INSERT" FROM t) SELECT * FROM src
```

would be incorrectly classified as a DML statement because `INSERT` appears at depth 0 after a closing paren. While unlikely in practice (DuckDB DuckLake test files rarely use such patterns), this is a correctness gap.

**Risk**: Misrouting a read query to DuckDB. Low practical impact since the result would still be correct (DuckDB can handle reads).

**Recommendation**: Add `'"'` handling in the parser, or add a comment documenting the known limitation. Add a test case for this edge case.

---

### R7-TH-003: `rewrite_order_by_all` uses naive string matching [Low]

**File**: `tests/hybrid_asyncdb.rs:362-371`

The function uses `str::find("ORDER BY ALL")` on the uppercased SQL, which will match inside string literals. The existing test at line 1158-1164 acknowledges this:

```rust
fn test_rewrite_order_by_all_inside_string() {
    let input = "SELECT * FROM t WHERE col = 'ORDER BY ALL'";
    let result = HybridDuckLakeDB::rewrite_order_by_all(input);
    // The function uses simple string matching so it will rewrite even inside literals
    assert!(result.len() <= input.len());
}
```

The test acknowledges the bug but doesn't assert correct behavior — it only checks the output is shorter or equal, which is a weak assertion that could mask regressions.

**Risk**: Corrupted SQL when string literals contain "ORDER BY ALL". Low probability in real DuckLake tests.

**Recommendation**: Either fix the function to be string-literal aware (like `rewrite_table_references`) or strengthen the test to assert the exact (incorrect) output so regressions are caught explicitly.

---

### R7-TH-004: Transaction state test doesn't verify read routing [Medium]

**File**: `tests/hybrid_asyncdb.rs:1023-1062`

The `test_transaction_state_tracking` test verifies the `in_transaction` flag is set/cleared correctly on BEGIN/COMMIT/ROLLBACK. However, it does NOT verify the critical behavioral consequence: that reads inside a transaction are routed to DuckDB instead of DataFusion. The flag tracking is tested, but the actual read-routing behavior is not.

A bug in the `run()` method's transaction routing logic (lines 495-551) could go undetected.

**Risk**: False positive — the flag could be correctly tracked while the routing condition could be broken (e.g., wrong `Ordering` parameter, wrong conditional logic).

**Recommendation**: Add a test that:
1. Begins a transaction
2. Inserts data (not yet committed)
3. Reads the data (should see uncommitted data via DuckDB)
4. Commits
5. Reads via DataFusion (should now see committed data)

---

### R7-TH-005: Cross-engine boolean test has weak NULL assertion [Low]

**File**: `tests/cross_engine_tests.rs:1449-1453`

```rust
assert!(
    rows[2][1] == "NULL" || rows[2][1].is_empty(),
    "null boolean should be NULL, got: '{}'",
    rows[2][1]
);
```

This accepts both "NULL" and "" (empty string) as valid NULL representations. The same pattern repeats at line 1502-1506 for the DuckDB-write direction. This is overly permissive — if the `DuckDbConn::query` implementation changes to return something unexpected, this test would still pass. The `DuckDbConn::query` helper consistently returns "NULL" for null values (line 18 of `test_utils.rs`), so the `is_empty()` branch is dead code that weakens the assertion.

**Risk**: Masking a regression where NULLs are silently dropped instead of represented.

**Recommendation**: Since `duckdb_value_to_string` always returns "NULL" for null values, simplify to `assert_eq!(rows[2][1], "NULL")`.

---

### R7-TH-006: `parse_table_name` has unit tests but no integration test [Low]

**File**: `src/table_functions.rs:706-764`

R6 added 9 unit tests for `parse_table_name` covering simple names, qualified names, quoted identifiers with dots, escaped quotes, and error cases. These are well-designed and test the correct edge cases. However, there are no integration tests that exercise `parse_table_name` through the actual table function execution path (e.g., calling `ducklake_snapshots("my.schema.table")` or `table_changes("main", "my_table")`).

**Risk**: The unit tests could pass while the function fails in context (e.g., due to how args are extracted upstream).

**Recommendation**: Add 1-2 integration tests in `tests/table_function_tests.rs` that exercise quoted-identifier table names through the full DataFusion SQL path.

---

### R7-TH-007: SLT preprocessor vacuous-pass guard is good but threshold is soft [Informational]

**File**: `tests/sqllogictest_runner.rs:823-837`

The preprocessor now asserts `meaningful_count > 0` (preventing fully-vacuous passes) and warns when `meaningful_count < 3`. This is a significant improvement from R5. However, the threshold of 3 is arbitrary — a test file with exactly 1 or 2 meaningful statements could still be testing very little while appearing to pass.

**Risk**: Low — the warning is logged, and the test does run the remaining statements.

**Status**: Acceptable as-is. The warning provides visibility.

---

### R7-TH-008: `convert_batch_to_strings` duplicates `arrow_value_to_string` [Informational]

**File**: `tests/hybrid_asyncdb.rs:626-787` vs `tests/common/test_utils.rs:66-198`

The `hybrid_asyncdb.rs` file has its own `convert_batch_to_strings` with inline type dispatch, duplicating the logic in `test_utils::arrow_value_to_string`. The file documents why (line 622-625): `hybrid_asyncdb.rs` is included as a submodule and can't declare its own `mod common`.

This is a legitimate technical constraint. However, the two implementations could diverge silently (e.g., `test_utils` handles `LargeUtf8` while `hybrid_asyncdb` does too — both are in sync currently, but there's no automated check for this).

**Risk**: Low — code duplication could lead to inconsistent behavior if one side is updated without the other.

**Recommendation**: Consider a shared standalone file that both can include, or add a comment cross-referencing the two.

---

### R7-TH-009: Concurrent write tests don't verify final data correctness [Low]

**File**: `tests/concurrent_write_tests.rs:120-153`

The `test_concurrent_writes_same_table_append` test spawns 10 concurrent append operations to the same table and verifies each returns `records_written = 1`. However, it never reads the table back to verify that all 11 rows (1 initial + 10 appended) are actually present and correct. A lost-write bug could pass this test.

Similarly, `test_stress_concurrent_writes` (line 271-299) verifies 50 successes but never reads back to confirm data integrity.

**Risk**: False positive — concurrent write conflicts could silently lose data.

**Recommendation**: Add a read-back assertion after the concurrent writes complete:
```rust
// After all tasks complete:
let ctx = create_read_context(&temp_dir).await;
let count = df_query(&ctx, "SELECT COUNT(*) FROM test.main.shared_table").await;
assert_eq!(count[0][0], "11"); // 1 initial + 10 appended
```

---

### R7-TH-010: Partition validation tests exist but lack cross-engine DML [Informational]

**File**: `tests/cross_engine_partition_tests.rs`

R6 added 7 partition validation tests that cover: reading all partitions, filtered reads (partition pruning), pre/post-partition data, empty partitions, multiple partition columns, and aggregation with partition filters. These are well-structured.

However, none of the partition tests exercise DML operations (DELETE, UPDATE) on partitioned tables through the cross-engine path. A bug in how delete files interact with partitioned data would go undetected.

**Risk**: Low for current read-only DataFusion use case, but relevant if write support is extended.

**Status**: Acceptable given read-only scope. File as TODO for write support expansion.

---

### R7-TH-011: `is_three_part_ref` doesn't handle quoted identifiers [Informational]

**File**: `tests/hybrid_asyncdb.rs:289-308`

The `is_three_part_ref` function checks whether text after "ducklake." is already a 3-part reference by looking for `identifier.identifier`. However, it doesn't handle double-quoted identifiers containing dots:

```sql
SELECT * FROM ducklake."my.schema".table
```

This would be parsed as 3-part (correct), but:

```sql
SELECT * FROM ducklake."my.schema"
```

Would incorrectly detect a dot inside the quoted identifier and think it's 3-part when it's actually 2-part with a quoted schema name.

**Risk**: Very low — DuckLake test files rarely use quoted identifiers in table references. The `rewrite_table_references` function already skips double-quoted regions (line 204), so the identifier after "ducklake." would only reach `is_three_part_ref` if unquoted.

**Status**: No action needed — the quoted-region handling in the parent function prevents this case.

---

## Coverage Gaps Remaining After R6

1. **No test for failed transaction rollback behavior**: Tests verify flag tracking but not that a failed DML inside a transaction triggers proper rollback of the DataFusion catalog refresh.

2. **No test for concurrent SLT execution**: The SLT runner processes one test file at a time. There's no test for running multiple SLT files concurrently (which `cargo test` does by default with `--test-threads`). Each test creates its own temp dir, so isolation should be fine, but this is unverified.

3. **No negative tests for `rewrite_table_references`**: The function has tests for correct rewrites but no tests for SQL injection-like edge cases (e.g., extremely long identifiers, Unicode characters in identifiers, deeply nested quotes).

4. **`assert_results_eq_strict` dead code**: Defined but never called — either integrate or remove.

5. **Write test value assertions**: `test_write_and_read_basic_types` asserts `ids.values() == &[1, 2, 3]` and checks `names` column values, which is good. But `test_write_multiple_batches` only checks the first value (`values.value(0) == "a"`) instead of all 4 values, which is a weak assertion.

---

## Summary Table

| ID | Severity | Component | Issue |
|----|----------|-----------|-------|
| R7-TH-001 | Low | test_utils | `assert_results_eq_strict` never called |
| R7-TH-002 | Low | hybrid_asyncdb | `cte_wraps_dml` ignores double quotes |
| R7-TH-003 | Low | hybrid_asyncdb | `rewrite_order_by_all` naive string matching |
| R7-TH-004 | Medium | hybrid_asyncdb | Transaction routing not tested end-to-end |
| R7-TH-005 | Low | cross_engine_tests | Weak NULL assertion in boolean roundtrip |
| R7-TH-006 | Low | table_functions | `parse_table_name` lacks integration tests |
| R7-TH-007 | Informational | sqllogictest_runner | Vacuous-pass threshold is soft |
| R7-TH-008 | Informational | hybrid_asyncdb | Duplicated type conversion logic |
| R7-TH-009 | Low | concurrent_write_tests | Missing read-back verification |
| R7-TH-010 | Informational | partition_tests | No DML on partitioned tables |
| R7-TH-011 | Informational | hybrid_asyncdb | `is_three_part_ref` quoted edge case |

---

## Verdict

The R6 test infrastructure improvements are **substantive and well-targeted**. The new tests genuinely test what they claim to test, and the SLT preprocessor guards against vacuous passes. The one medium-severity finding (R7-TH-004) is about a missing behavioral test — the transaction flag tracking tests are correct but incomplete. The remaining findings are low-severity edge cases and dead code. Overall, test quality has improved significantly from R5 to R6.
