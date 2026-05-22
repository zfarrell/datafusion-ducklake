# R11 Test Harness Review

## Summary
- Total findings: 14
- By priority: P0: 0, P1: 2, P2: 6, P3: 6

## Findings

### R11-TH-001: merge_tests.rs uses zip without length guard
**Priority**: P2
**Files**: `tests/merge_tests.rs:223,299,385,455,509`
**Description**: Five test functions compare expected vs actual rows using `expected.iter().zip(actual.iter())` after an `assert_eq!(len, len)` check. While the length guard prevents zip truncation from hiding mismatches, the assertion message at lines 455/509 only shows "Row count mismatch" without dumping both sets for debugging. More critically, these tests duplicate the assertion pattern rather than using the shared `assert_results_eq` helper from `common/test_utils.rs`, which provides better diagnostics (shows first 5 rows of each set).
**Suggested fix**: Replace manual zip+assert loops with `assert_results_eq("merge_...", &expected, &actual)` from the shared helper.

### R11-TH-002: Massive helper duplication across 17+ test files
**Priority**: P1
**Files**: `tests/delete_tests.rs:23-79`, `tests/update_tests.rs:23-64`, `tests/write_tests.rs:26-58`, `tests/stats_tests.rs:25-55`, `tests/edge_case_tests.rs:34-58`, `tests/view_tests.rs:22-50`, and 11+ more
**Description**: `create_object_store()`, `create_test_env()`, `create_read_context()`, and `create_writable_context()` are copy-pasted across 17 test files with identical or near-identical implementations. Each returns `Arc<LocalFileSystem>`, sets up a SQLite-backed catalog in a temp dir, etc. This makes maintenance costly — any change to setup logic (e.g., adding a config option) requires editing 17+ files.
**Suggested fix**: Move shared helpers to `tests/common/test_utils.rs` (or a new `tests/common/setup.rs`) and import everywhere. The `DuckDbConn` helper is already properly centralized there — follow the same pattern.

### R11-TH-003: SLT statement error→ok conversion may mask real failures
**Priority**: P2
**Files**: `tests/sqllogictest_runner.rs:318-404`
**Description**: The preprocessor converts `statement error` to `statement ok` when error text matches `HYBRID_INCOMPATIBLE_PATTERNS` (5 patterns: READ-ONLY, DOES NOT EXIST, MISSING EXTENSION ERROR, etc.). This is logged via `tracing::warn` but there's no mechanism to assert these conversions stay bounded. If a DuckDB upstream test change causes more statements to be silently converted, coverage silently degrades. The `meaningful_count > 0` guard (line 823) catches total elimination but not gradual erosion.
**Suggested fix**: Add a count of statement error→ok conversions and warn/fail if it exceeds a threshold per test file. Alternatively, add a `--strict` mode that disables conversion.

### R11-TH-004: Cross-engine PG/MySQL tests silently skip on missing DuckDB extension
**Priority**: P2
**Files**: `tests/cross_engine_postgres_tests.rs:290-294,322-326`
**Description**: Two PG cross-engine tests (`cross_engine_pg_df_write_duckdb_read`, `cross_engine_pg_duckdb_write_df_read`) have a `match DuckDbPgConn::try_open()` pattern that prints to stderr and returns early when the postgres DuckDB extension is unavailable. Combined with `#[ignore]`, these tests pass in CI by doing nothing — a double skip. The `eprintln` is not visible in normal test output.
**Suggested fix**: Either remove the inner skip (rely on `#[ignore]` alone), or use `#[cfg_attr(feature = "skip-tests-with-docker", ignore)]` consistently and make the inner try_open failure a hard error when the test is actually run.

### R11-TH-005: hybrid_asyncdb duplicates arrow_value_to_string from test_utils
**Priority**: P2
**Files**: `tests/hybrid_asyncdb.rs:654-800`, `tests/common/test_utils.rs:66-199`
**Description**: `convert_batch_to_strings` in hybrid_asyncdb.rs reimplements the same type dispatch logic as `arrow_value_to_string` in test_utils.rs (Int8-64, UInt8-64, Float32/64, Utf8, Date32, Timestamp, Decimal128). The comment at line 651 explains this is because hybrid_asyncdb.rs "cannot declare its own `mod common`". However, both are compiled in the same crate — a `pub use` or function-level import could resolve this.
**Suggested fix**: Extract the shared conversion logic into a standalone module (e.g., `tests/arrow_format.rs`) and import from both hybrid_asyncdb and test_utils. Alternatively, since sqllogictest_runner.rs already does `mod common; mod hybrid_asyncdb;`, the hybrid_asyncdb could use `super::common::test_utils::arrow_value_to_string`.

### R11-TH-006: No test coverage for R10 checked_add/Arc wrapping fixes
**Priority**: P1
**Files**: R10 commits: `4bd934b`, `d7a8d5a`, `4624df5`
**Description**: R10 applied several correctness fixes — `checked_add` for overflow safety, `Arc<Vec>` dereference for iteration, and transaction wrapping for append-mode file registration. None of these have dedicated regression tests that exercise the specific edge cases they fix (e.g., arithmetic overflow in metadata values, iteration over Arc-wrapped vectors in merge/DML exec). The fixes are implicitly tested by existing tests passing, but there's no guard against regression.
**Suggested fix**: Add targeted regression tests: (1) a test that inserts enough data to trigger large row counts near overflow boundaries for checked_add paths, (2) a merge_exec test that verifies Arc<Vec> iteration works correctly with multiple source batches.

### R11-TH-007: SLT skip_query_results may truncate if no ---- separator
**Priority**: P3
**Files**: `tests/sqllogictest_runner.rs:768-790`
**Description**: `skip_query_results` first scans for `----` separator, consuming SQL lines along the way. If a query block lacks the separator (malformed test file), it will consume lines until a blank line or next directive, potentially eating into the next test record. This is a minor robustness issue since upstream DuckDB test files are well-formed.
**Suggested fix**: Add a warning when no `----` is found within a reasonable number of lines.

### R11-TH-008: information_schema_test permanently ignored with stale reason
**Priority**: P3
**Files**: `tests/information_schema_test.rs:13-14`
**Description**: `test_information_schema_snapshots` is `#[ignore]` with comment "Snapshots table requires ducklake_snapshot table which test catalogs don't create". This test has been ignored since creation and provides no coverage. The information_schema module is a significant feature surface.
**Suggested fix**: Either fix the test setup to create proper catalogs with snapshot tables, or remove the test if information_schema is covered elsewhere.

### R11-TH-009: DuckDB value Debug fallback in test_utils loses type info
**Priority**: P3
**Files**: `tests/common/test_utils.rs:51-58`
**Description**: The catch-all branch in `duckdb_value_to_string` uses `format!("{v:?}")` with a special case for `Decimal(...)`. Any new DuckDB value type will produce Debug output like `Blob([1,2,3])` which won't match DataFusion's string representation, causing silent comparison failures that are hard to diagnose.
**Suggested fix**: Add explicit handling for common types (Blob, Time, Interval) or log a warning when the fallback is used.

### R11-TH-010: SLT ORDER BY ALL rewriting doesn't handle subqueries
**Priority**: P3
**Files**: `tests/sqllogictest_runner.rs:518-536`, `tests/hybrid_asyncdb.rs:381-398`
**Description**: Both `rewrite_order_by_all` implementations do a simple string replacement of `ORDER BY ALL`. If a query has `ORDER BY ALL` in a subquery and also in the outer query, only the first occurrence is replaced (in sqllogictest_runner.rs) or the first non-string-literal match (in hybrid_asyncdb.rs). This could cause test failures for complex queries but is unlikely given current DuckDB test files.
**Suggested fix**: Document the limitation. A more robust approach would replace all occurrences outside string literals, but the current approach works for DuckDB's test corpus.

### R11-TH-011: normalize_value masks integer/float type confusion
**Priority**: P2
**Files**: `tests/common/test_utils.rs:299-311`
**Description**: `normalize_value` parses strings containing '.' as f64 and reformats to 6 decimal places. This means "999.990000" will match "999.99" — good for precision normalization, but it also means "999.99" (Decimal) and "999.989990234375" (Float32) would both normalize to "999.990000" and match. The `assert_results_eq_strict` function exists for strict comparison but isn't used in most cross-engine tests.
**Suggested fix**: Consider using `assert_results_eq_strict` for cross-engine tests that compare Decimal values, where type precision matters.

### R11-TH-012: Cross-engine tests only cover SQLite backend
**Priority**: P3
**Files**: `tests/cross_engine_tests.rs:1` (TODO comment at line 13)
**Description**: The main cross-engine test file has a TODO: "R5-S-067: These tests only use SQLite backend. Add PG/MySQL cross-engine tests when Docker-based test infrastructure is available." PG and MySQL cross-engine tests exist in separate files but are all `#[ignore]`, so CI only exercises SQLite-backed cross-engine flows.
**Suggested fix**: This is a known limitation. When Docker CI is available, remove `#[ignore]` from PG/MySQL tests.

### R11-TH-013: SLT preprocessor statement error→ok conversion for hybrid-incompatible errors may be overly broad
**Priority**: P2
**Files**: `tests/sqllogictest_runner.rs:687-736`
**Description**: The `HYBRID_INCOMPATIBLE_PATTERNS` list includes `"DOES NOT EXIST!"` which converts any statement error containing that text to statement ok. This could match errors unrelated to DETACH (e.g., "Table X does not exist!") and convert them to statement ok, hiding genuine DataFusion errors where a table lookup should fail.
**Suggested fix**: Tighten the pattern to match only DETACH-related errors, e.g., by adding an SQL pattern requirement: `("DOES NOT EXIST!", Some("DETACH"), "DETACH is skipped in hybrid mode")`.

### R11-TH-014: SLT preprocess vacuous test guard doesn't detect halt-dominated files
**Priority**: P3
**Files**: `tests/sqllogictest_runner.rs:816-837`
**Description**: The guard `assert!(meaningful_count > 0, ...)` counts statements/queries in the preprocessed output. However, if a test file produces a `halt` directive early (e.g., line 83-86 for `require spatial`), the file has 0 directives and the assertion correctly fires. But if the file has 1-2 trivial setup statements before the halt, it passes the guard despite providing no meaningful coverage. The `meaningful_count < 3` warning at line 827 partially addresses this but only prints to stderr.
**Suggested fix**: Consider counting only post-halt directives, or tracking whether a halt was emitted and treating the entire file as vacuous in that case.
