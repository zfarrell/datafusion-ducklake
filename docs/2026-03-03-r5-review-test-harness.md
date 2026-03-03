# R5 Test Harness Review — 2026-03-03

**Reviewer**: test-harness-review (Claude agent)
**Scope**: Test infrastructure correctness, false positives, coverage gaps
**Files reviewed**:
- `tests/hybrid_asyncdb.rs` (843 lines)
- `tests/common/test_utils.rs` (461 lines)
- `tests/common/mod.rs` (390 lines)
- `tests/cross_engine_tests.rs` (484 lines)
- `tests/cross_engine_dml_tests.rs` (940 lines)
- `tests/cross_engine_insert_tests.rs` (1035 lines)
- `tests/parity_tests.rs` (298 lines)
- `tests/delete_filter_tests.rs` (577 lines)
- `tests/concurrent_tests.rs` (543 lines)
- `tests/adversarial_catalog_tests.rs` (1113 lines)
- `tests/sqllogictest_runner.rs` (821 lines)
- `build.rs` (69 lines)

**Prior review cross-check**: Verified R4-TH-001 (DuckDbConn duplication) FIXED, R4-TH-006 (Decimal128 divergence) FIXED, R4-TH-012 (no is_write_statement tests) FIXED, R4-TH-013 (catch-all formatting) FIXED.

---

## New Findings

### R5-TH-001 [OPEN] — Decimal128 negative value sign loss when whole part is zero (P1)

**Files**: `tests/hybrid_asyncdb.rs:604-609`, `tests/common/test_utils.rs:169-174`

**Description**: Both Decimal128 formatting paths use truncating integer division that loses the negative sign when the absolute value is less than 1.

```rust
let raw = arr.value(row_idx);
let scale = *scale as u32;
let divisor = 10i128.pow(scale);
let whole = raw / divisor;        // For raw=-45, scale=2: -45/100 = 0
let frac = (raw % divisor).unsigned_abs();
format!("{whole}.{frac:0>width$}", width = scale as usize)
// Produces "0.45" instead of "-0.45"
```

For a value like -0.45 (raw=-45, scale=2):
- `whole = -45 / 100 = 0` (Rust truncating division rounds toward zero)
- `frac = (-45 % 100).unsigned_abs() = 45`
- Result: `"0.45"` — **the negative sign is silently lost**

**Impact**: Any Decimal128 value in the range (-1, 0) exclusive will have its sign stripped. This could cause false-positive test passes where negative decimals are compared as positive, or cause SLT result mismatches with DuckDB output that correctly shows the sign.

**Fix**: Check `raw < 0 && whole == 0` and prepend `"-"`:
```rust
let sign = if raw < 0 && whole == 0 { "-" } else { "" };
format!("{sign}{whole}.{frac:0>width$}", width = scale as usize)
```

---

### R5-TH-002 [OPEN] — normalize_value() over-normalizes all numeric strings (P2)

**File**: `tests/common/test_utils.rs:278-288`

**Description**: `normalize_value()` parses ALL numeric-looking strings through `f64` and formats them to 6 decimal places. This means:
- Integer "100" becomes "100.000000"
- "1.5" becomes "1.500000"
- Any value representable as f64 gets normalized regardless of original type

```rust
if let Ok(f) = s.parse::<f64>() {
    return format!("{:.6}", f);
}
```

**Impact**:
- Hides precision differences between engines (e.g., DuckDB returns "1.50" vs DataFusion returns "1.5" — both become "1.500000")
- Makes it impossible to detect integer/float type confusion (both "100" and "100.0" normalize identically)
- Could mask f64 precision loss for large integers or high-precision decimals
- The 6-decimal-place formatting may round values differently than either engine would natively

**Fix**: Consider type-aware normalization that preserves the distinction between integers, floats, and decimals, or at minimum only normalize values that are already floating-point (contain `.` or `e`/`E`).

---

### R5-TH-003 [OPEN] — Virtual column stripping uses naive SQL string matching (P2)

**File**: `tests/hybrid_asyncdb.rs:291`

**Description**: The virtual column filter checks if a column name appears anywhere in the uppercase SQL string:

```rust
sql_upper.contains(&name.to_uppercase())
```

This matches column names that appear in WHERE clauses, string literals, table names, comments, or aliases — not just the SELECT list.

**Example false positive**: A query like `SELECT * FROM t WHERE filename = 'test'` would strip a result column named `filename` because `"FILENAME"` appears in the SQL string, even though the user explicitly selected it.

**Impact**: Could cause silent column removal from query results in SLT tests, leading to hard-to-diagnose test failures or false passes when a virtual column name coincidentally appears elsewhere in the query.

**Fix**: Parse only the SELECT clause (before FROM), or use DataFusion's schema metadata to identify virtual columns rather than SQL string matching.

---

### R5-TH-004 [OPEN] — Empty batch returns StatementComplete instead of empty Rows (P2)

**File**: `tests/hybrid_asyncdb.rs:438-439`

**Description**: When DataFusion returns results where all batches are empty (zero rows total), the code returns `DBOutput::StatementComplete(0)`:

```rust
if rows.is_empty() {
    return Ok(DBOutput::StatementComplete(0));
}
```

**Impact**: SLT `query` directives expect `DBOutput::Rows { types, rows }` for SELECT statements, even if the result set is empty. Returning `StatementComplete(0)` could cause the SLT runner to report a type mismatch (expected Rows, got StatementComplete) or silently pass when it shouldn't. A SELECT that returns no rows is semantically different from a statement that affected 0 rows.

**Fix**: Return `DBOutput::Rows { types: expected_types, rows: vec![] }` for empty SELECT results.

---

### R5-TH-005 [OPEN] — Timestamp sub-second precision inconsistency between formatters (P2)

**Files**: `tests/common/test_utils.rs` (Timestamp formatting), `tests/hybrid_asyncdb.rs` (Timestamp formatting)

**Description**: The two Timestamp formatting paths differ in sub-second handling:
- `test_utils.rs` uses `chrono::NaiveDateTime` with `format!("{}", datetime)` which includes sub-second precision when non-zero
- `hybrid_asyncdb.rs` uses `value_as_datetime()` and `format!("{}", datetime)` similarly

However, the `normalize_value()` function in cross-engine tests formats timestamps as strings and then potentially parses them through the f64 path if the timestamp somehow looks numeric. More critically, there is no explicit sub-second truncation or rounding policy, so tests comparing timestamps with sub-second components may produce inconsistent results depending on which formatting path is used.

**Impact**: Tests involving timestamps with fractional seconds could produce different string representations depending on which code path formats them, potentially causing false failures or masking real precision differences.

**Fix**: Establish a consistent timestamp formatting policy (e.g., always include microseconds, or always truncate to seconds) and apply it uniformly in both formatters.

---

### R5-TH-006 [OPEN] — DuckDbConn::query column count discovery is fragile (P3)

**File**: `tests/common/test_utils.rs:414`

**Description**: `DuckDbConn::query()` discovers the number of columns by iterating `for i in 0..` and breaking when `row.get::<usize, String>(i)` returns an error:

```rust
for i in 0.. {
    match row.get::<usize, String>(i) {
        Ok(val) => values.push(val),
        Err(_) => break,
    }
}
```

**Impact**: This approach conflates "no more columns" with "column exists but cannot be converted to String." If a column contains a value that fails String conversion (e.g., a BLOB or NULL that the duckdb crate doesn't auto-convert), iteration stops early and silently drops remaining columns. This could cause result truncation in tests without any error indication.

**Fix**: Use the DuckDB Rust API to query column count from the statement/row metadata before iterating, or at minimum use `row.column_count()` if available.

---

### R5-TH-007 [OPEN] — No test for transaction-aware read routing in hybrid adapter (P3)

**File**: `tests/hybrid_asyncdb.rs:230-260`

**Description**: The hybrid SLT adapter has logic to route reads to DuckDB when inside a transaction (since DataFusion cannot see uncommitted data):

```rust
if self.in_transaction {
    // Route to DuckDB for reads inside transactions
}
```

However, there is no dedicated test that verifies this behavior. The SLT tests may exercise it incidentally, but there is no explicit test that:
1. Begins a transaction
2. Inserts data
3. Reads within the transaction (should go to DuckDB)
4. Commits
5. Reads outside the transaction (should go to DataFusion)

**Impact**: If the transaction-routing logic is broken, reads inside transactions could silently go to DataFusion and return stale/empty results, potentially causing subtle test failures that are hard to diagnose.

**Fix**: Add a targeted integration test for transaction-aware routing behavior.

---

### R5-TH-008 [OPEN] — SLT preprocessor vacuous-test guard may be too weak (P2)

**File**: `tests/sqllogictest_runner.rs:784-791`

**Description**: The `has_statements` guard prevents vacuous test passes by checking if any `statement` or `query` lines remain after preprocessing. However, a test file that contains only `statement ok` directives where all statements are skipped by the preprocessor (due to unsupported functions, directives, etc.) could still have `has_statements = true` if at least one `statement` line survived preprocessing — even if all the corresponding SQL was rewritten to no-ops or trivial operations.

Additionally, the guard counts `statement error` lines that get converted to `statement ok` by `is_hybrid_incompatible_error()` as "having statements," so a test where all error-expecting statements are converted to no-op successes could pass vacuously.

**Impact**: Some SLT tests may appear to pass while exercising no meaningful functionality, giving false confidence in test coverage.

**Fix**: Consider tracking the count of meaningful (non-trivial) statements executed and warning or failing when a test executes fewer than a threshold (e.g., 0 query directives).

---

## Summary

| ID | Priority | Category | Title |
|----|----------|----------|-------|
| R5-TH-001 | P1 | Correctness | Decimal128 negative value sign loss when whole=0 |
| R5-TH-002 | P2 | False Positive | normalize_value() over-normalizes all numerics |
| R5-TH-003 | P2 | False Positive | Virtual column stripping naive SQL string match |
| R5-TH-004 | P2 | Correctness | Empty batch returns StatementComplete not Rows |
| R5-TH-005 | P2 | Consistency | Timestamp sub-second precision inconsistency |
| R5-TH-006 | P3 | Robustness | DuckDbConn::query fragile column iteration |
| R5-TH-007 | P3 | Coverage Gap | No test for transaction-aware read routing |
| R5-TH-008 | P2 | False Positive | SLT vacuous-test guard may be too weak |

**Total: 8 new findings** (1 P1, 4 P2, 2 P3, 1 P2)
- P1: 1 finding (Decimal128 sign loss — affects correctness of all tests involving negative decimals < 1)
- P2: 5 findings
- P3: 2 findings
