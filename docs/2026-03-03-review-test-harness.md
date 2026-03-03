# R4 Test Harness Review — 2026-03-03

**Reviewer role**: test-harness-review
**Scope**: All files in `tests/`, SLT runner, hybrid adapter, shared utilities
**Focus**: False positives, weak assertions, tautological tests, missing error-path coverage, disabled tests, coverage gaps, SLT routing edge cases

---

## Findings

### R4-TH-001 — `DuckDbConn` struct duplicated across 10 test files
**Priority**: P2
**Files**:
- `tests/cross_engine_tests.rs:110`
- `tests/cross_engine_dml_tests.rs:145`
- `tests/cross_engine_insert_tests.rs:97`
- `tests/cross_engine_inline_tests.rs:25`
- `tests/cross_engine_feature_tests.rs:91`
- `tests/cross_engine_partition_tests.rs:30`
- `tests/cross_engine_alter_tests.rs:136`
- `tests/cross_engine_ddl_tests.rs:154`
- `tests/merge_tests.rs:400`
- `tests/virtual_column_extended_tests.rs:18`

**Description**: The `DuckDbConn` wrapper (providing `execute` / `query_rows` helpers around a raw `duckdb::Connection`) is copy-pasted identically in 10 files. Any future change (e.g. adding error context, changing return types) must be replicated in all 10 places. This was noted tangentially in R3 but never extracted.

**Suggested fix**: Move `DuckDbConn` to `tests/common/mod.rs` or `tests/common/test_utils.rs` and import from all consumers.

---

### R4-TH-002 — `merge_tests.rs` uses non-filtered `batches_to_strings`
**Priority**: P1
**File**: `tests/merge_tests.rs:100-103`

**Description**: The `query_results()` helper at line 100 calls `batches_to_strings(&batches)` (line 103) instead of `batches_to_strings_filtered(&batches)`. This means virtual columns (`filename`, `file_row_number`, `rowid`, `snapshot_id`, `file_index`) are included in the result vectors. Assertions that compare against expected data may pass only because DataFusion currently doesn't project these columns for these specific queries, but any change that causes virtual columns to appear will silently shift column indices and produce false passes or confusing failures.

**Suggested fix**: Replace `batches_to_strings` with `batches_to_strings_filtered` in `query_results()`, or switch to the shared `df_query()` from `test_utils`.

---

### R4-TH-003 — `cross_engine_alter_tests.rs` local `df_query` does not filter virtual columns
**Priority**: P1
**File**: `tests/cross_engine_alter_tests.rs:214`

**Description**: This file defines its own `df_query()` that uses `ArrayFormatter` directly to format results. Unlike the shared `df_query()` in `tests/common/test_utils.rs`, it does NOT strip virtual columns. Any ALTER TABLE test that triggers virtual column projection will include unexpected columns in assertions.

**Suggested fix**: Delete the local `df_query` and use the shared `tests::common::test_utils::df_query` instead.

---

### R4-TH-004 — `cross_engine_feature_tests.rs` local `df_query_all` inconsistency
**Priority**: P2
**File**: `tests/cross_engine_feature_tests.rs:158`

**Description**: This file defines a local `df_query_all()` that intentionally includes all columns (including virtual columns) for virtual-column-specific tests. While this is correct for those tests, the file also imports shared test utils but uses a mix of both helpers, making it unclear which tests need virtual columns and which don't. This creates maintenance risk.

**Suggested fix**: Add a doc comment to `df_query_all` explaining it intentionally includes virtual columns, and verify each call site uses the correct variant.

---

### R4-TH-005 — `hybrid_asyncdb.rs` uses `unwrap()` on all 20 `downcast_ref` calls
**Priority**: P2
**File**: `tests/hybrid_asyncdb.rs:511-610`

**Description**: The `convert_batch_to_strings()` function performs ~20 `downcast_ref::<T>().unwrap()` calls in its type-dispatch match block. If a new Arrow data type is encountered that the match correctly routes but somehow fails to downcast (or if a future refactor introduces a mismatch), the entire SLT test run will panic with an opaque "called `unwrap()` on a None" instead of a meaningful error.

**Suggested fix**: Replace `.unwrap()` with `.expect("downcast to {TypeName} failed for column {col_name}")` or use `unwrap_or_else` with a descriptive message. Low-urgency since the match arms guard the types, but the diagnostic improvement is trivial.

---

### R4-TH-006 — Decimal128 formatting divergence between `hybrid_asyncdb` and `test_utils`
**Priority**: P1
**File**: `tests/hybrid_asyncdb.rs:599-607`, `tests/common/test_utils.rs:167-180`

**Description**: Two different Decimal128 formatting implementations exist:
- **hybrid_asyncdb** (line 599): Uses `value as f64 / 10_f64.powi(scale)` — floating-point division that can lose precision for large values (e.g. Decimal128 with 38 digits of precision exceeds f64's 15-17 significant digits).
- **test_utils** (line 167): Uses integer arithmetic (`raw / divisor`, `raw % divisor`) — preserves exact representation.

This means the same Decimal128 value may format differently depending on which code path is used, potentially causing spurious SLT failures or false passes when comparing cross-engine results.

**Suggested fix**: Unify on the integer-arithmetic approach from `test_utils`. Extract it to a shared helper if both paths need it.

---

### R4-TH-007 — Float formatting inconsistency between `hybrid_asyncdb` and `test_utils`
**Priority**: P2
**File**: `tests/hybrid_asyncdb.rs:479-498`, `tests/common/test_utils.rs:24-25`

**Description**:
- **hybrid_asyncdb** `format_float()` (line 479): Appends `.0` to whole-number floats (e.g. `10.0` → `"10.0"`) to match DuckDB display convention.
- **test_utils** `duckdb_value_to_string()` (lines 24-25): Uses plain `format!("{f}")` with no `.0` suffix guarantee.

This inconsistency is acknowledged in `cross_engine_inline_tests.rs:149` where a loose assertion (`== "10" || == "10.0"`) papers over the mismatch. The divergence could mask real formatting regressions.

**Suggested fix**: Decide on a canonical float format and apply it consistently. If DuckDB convention is the target, adopt `format_float()` in `test_utils` as well.

---

### R4-TH-008 — Loose float assertion in `cross_engine_inline_tests.rs`
**Priority**: P2
**File**: `tests/cross_engine_inline_tests.rs:149`

**Description**: The assertion `assert!(rows[0][2] == "10" || rows[0][2] == "10.0")` accepts two different representations. This hides the fact that the formatting is non-deterministic or inconsistent between code paths. If the value is unexpectedly `"10.00"` or `"1e1"`, the test would fail, but other near-misses might not be caught.

**Suggested fix**: Fix the root cause (R4-TH-007) and pin the assertion to a single expected format.

---

### R4-TH-009 — `is_write_statement` does not handle `WITH ... INSERT/UPDATE/DELETE`
**Priority**: P1
**File**: `tests/hybrid_asyncdb.rs:117`

**Description**: `is_write_statement()` detects writes by checking if the trimmed, uppercased SQL starts with keywords like `INSERT`, `UPDATE`, `DELETE`, `CREATE`, etc. However, CTEs (`WITH ... AS (...) INSERT INTO ...`) are a valid DuckDB pattern that starts with `WITH` — a keyword not in the write list. These statements would be routed to the DataFusion read path, which cannot execute writes, causing SLT test failures or silent misrouting.

This was noted as R3F-046 but remains open. The risk grows as more DuckDB SLT tests are onboarded.

**Suggested fix**: After checking the first keyword, also scan for write keywords after the CTE block. A regex like `WITH\s+.*\b(INSERT|UPDATE|DELETE|MERGE)\b` or a simple secondary keyword scan after stripping CTEs would cover this.

---

### R4-TH-010 — Adversarial tests discard results with `let _ = result`
**Priority**: P2
**File**: `tests/adversarial_catalog_tests.rs:772, 809, 813, 817`

**Description**: Four instances silently discard results:
- **Line 772**: `DuckLakeCatalog::new(provider)` result discarded — does not verify whether catalog creation succeeds or fails on a corrupted DB.
- **Lines 809, 813, 817**: `ducklake_to_arrow_type()` results for extreme decimal specs (`decimal(999999999, 999999999)`, `decimal(-1, -1)`, `decimal(0, 0)`) are discarded. The tests prove the function doesn't panic, but don't assert whether it returns `Ok` or `Err`, nor what the error message contains.

These are tautological: they pass regardless of behavior, only proving absence of panics.

**Suggested fix**: Add `assert!(result.is_err(), "should reject ...")` or `assert!(result.is_ok())` with expected-value checks. For the corrupted-DB test, assert on the specific error variant.

---

### R4-TH-011 — `is_hybrid_incompatible_error` pattern matching is prefix-only
**Priority**: P3
**File**: `tests/sqllogictest_runner.rs:703`

**Description**: `is_hybrid_incompatible_error()` checks if the uppercased error string contains any of the patterns in `HYBRID_INCOMPATIBLE_PATTERNS`. The patterns are broad substrings (e.g. `"UNIQUE"`, `"CHECK CONSTRAINT"`). If a genuine DataFusion error happens to contain one of these substrings, it will be silently converted from `statement error` to `statement ok`, masking a real failure.

**Suggested fix**: Where feasible, tighten patterns to be more specific (e.g. `"UNIQUE CONSTRAINT"` instead of just `"UNIQUE"`). Add a counter/log of how many conversions occur per test run so unexpected spikes are visible.

---

### R4-TH-012 — No tests for `is_write_statement` itself
**Priority**: P2
**File**: `tests/hybrid_asyncdb.rs:117`

**Description**: `is_write_statement()` is a critical routing function (determines whether SQL goes to DuckDB or DataFusion) but has no unit tests. Edge cases like mixed-case keywords, leading whitespace, comments before keywords, and CTEs (R4-TH-009) are untested.

**Suggested fix**: Add a `#[cfg(test)]` module with unit tests covering: basic keywords, mixed case, leading whitespace/newlines, `--comment\nINSERT`, `/* block */ DELETE`, `WITH ... INSERT`, empty string, whitespace-only string.

---

### R4-TH-013 — `convert_batch_to_strings` missing types silently fall through
**Priority**: P2
**File**: `tests/hybrid_asyncdb.rs:612-615`

**Description**: The catch-all arm of the type-dispatch match in `convert_batch_to_strings` formats unrecognized types as `format!("?{}?", column.data_type())`. This produces output like `?List(...)? ` in SLT results. Since SLT tests compare exact output, this will cause test failures, but the error message is cryptic. More importantly, if a new type is added to Arrow and DuckLake starts using it, the `?...?` placeholder could match expected output in a poorly-written test.

**Suggested fix**: Replace the catch-all with `panic!("unsupported Arrow type in SLT conversion: {}", ...)` or return an `Err` to make unsupported types immediately visible.

---

### R4-TH-014 — `preprocess_test_file` ORDER BY ALL rewriting is fragile
**Priority**: P3
**File**: `tests/sqllogictest_runner.rs` (preprocessing section)

**Description**: The `ORDER BY ALL` rewriting in `preprocess_test_file()` replaces `ORDER BY ALL` with `ORDER BY 1, 2, 3, ...` based on the column count from the preceding `query` directive's type string. This is a best-effort heuristic: if the type string doesn't match the actual column count (e.g. due to earlier preprocessing or unusual formatting), the rewrite produces incorrect SQL. The rewriting also doesn't handle `ORDER BY ALL DESC` or `ORDER BY ALL NULLS FIRST`.

**Suggested fix**: Document the known limitations in a comment. Consider adding a warning log when the column count seems suspicious (e.g. 0 or >20). Low priority since affected tests would simply fail rather than produce false passes.

---

### R4-TH-015 — `rewrite_table_references` may over-match in string literals
**Priority**: P3
**File**: `tests/hybrid_asyncdb.rs` (table reference rewriting)

**Description**: `rewrite_table_references()` rewrites `ducklake.tablename` → `ducklake.main.tablename` using regex. The regex operates on the full SQL string and could match inside string literals (e.g. `WHERE name = 'ducklake.test'`). While unlikely in current test data, this is a latent correctness issue.

**Suggested fix**: Add a comment documenting the limitation. A proper fix would require SQL parsing, which is overkill unless false rewrites are observed.

---

### R4-TH-016 — No negative tests for `preprocess_test_file` loop expansion
**Priority**: P3
**File**: `tests/sqllogictest_runner.rs` (loop expansion section)

**Description**: The loop expansion code handles `foreach` and `loop` directives by expanding iterations. There are no tests for malformed loops (e.g. missing `endloop`, nested loops, empty iteration lists, extremely large iteration counts). A malformed test file could cause infinite loops or panics during preprocessing.

**Suggested fix**: Add bounds checking (max iterations, max nesting depth) and tests for malformed input. Low priority since test files are curated.

---

### R4-TH-017 — `assert_results_eq` does not report which rows differ
**Priority**: P3
**File**: `tests/common/test_utils.rs` (assert_results_eq function)

**Description**: `assert_results_eq()` checks row count and column count, then iterates and asserts per-cell equality. When a cell mismatches, the panic message shows the cell coordinates and values, which is good. However, if the row count itself mismatches, it only reports the counts without showing any actual vs expected rows, making debugging harder for large result sets.

**Suggested fix**: On row-count mismatch, print the first few rows of both actual and expected for quick diagnosis.

---

## Summary by Priority

| Priority | Count | Description |
|----------|-------|-------------|
| P0       | 0     | —           |
| P1       | 4     | R4-TH-002, R4-TH-003, R4-TH-006, R4-TH-009 |
| P2       | 7     | R4-TH-001, R4-TH-004, R4-TH-005, R4-TH-007, R4-TH-008, R4-TH-010, R4-TH-012, R4-TH-013 |
| P3       | 5     | R4-TH-011, R4-TH-014, R4-TH-015, R4-TH-016, R4-TH-017 |
| **Total**| **17**|             |

Note: P2 count is 8 (includes R4-TH-013). Table corrected: P2 = 8, Total = 17.

---

## Cross-references to Prior Reviews

- **R4-TH-009** overlaps with **R3F-046** (WITH...INSERT routing) — still open
- **R4-TH-001** (DuckDbConn duplication) was tangentially noted in R3 but not formally tracked
- All `#[ignore]` tests from `sql_write_tests.rs` noted in R3F-050 appear to have been resolved (zero `#[ignore]` found in current codebase)
