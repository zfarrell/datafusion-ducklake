# Test Harness Review — 2026-03-02

## Summary

Reviewed ~48,000 lines of test code across 60+ test files. The test infrastructure is comprehensive in scope but has several structural issues that could mask real failures. The most critical findings are: (1) multiple SQL write tests silently pass on error, (2) roundtrip interop tests silently skip without `#[ignore]`, (3) significant helper code duplication with subtle behavioral differences, and (4) virtual column filtering inconsistencies across test files that could cause false positives or failures.

## SLT Adapter Analysis

**File**: `tests/sqllogictest_runner.rs` + `tests/hybrid_asyncdb.rs`

### Routing Logic

The hybrid adapter correctly routes writes to DuckDB and reads to DataFusion via `is_write_statement()` at `hybrid_asyncdb.rs:117`. The routing is conservative — anything not clearly a SELECT goes to DuckDB. The write detection covers CREATE, INSERT, UPDATE, DELETE, DROP, ALTER, MERGE, USE, SHOW, CALL, SET, RESET, PREPARE, EXECUTE, DEALLOCATE, COPY, COMMENT, PRAGMA, BEGIN, COMMIT, ROLLBACK.

**Risk**: Prefix matching on SQL text. A table named `CREATE_LOG` in `SELECT * FROM CREATE_LOG` would not trigger `is_write_statement()` since the full string starts with "SELECT". This is correct. However, something like `SHOW TABLES` gets routed to DuckDB, so any `SHOW`-based test validates DuckDB, not DataFusion.

### Result Normalization

`hybrid_asyncdb.rs:567` always downcasts `Timestamp(_, _)` as `TimestampMicrosecondArray`. If a timestamp column uses nanosecond or millisecond precision, this will **panic** at runtime. This is a latent bug that hasn't triggered only because DuckDB DuckLake currently uses microsecond timestamps.

### Skip Logic

The preprocessor (`sqllogictest_runner.rs:33`) silently removes many test categories:
- `concurrentloop` blocks (line 102)
- Multi-connection statements (`con1`, `con2`, etc.) (line 156)
- `EXPLAIN`, `DESCRIBE`, `SHOW TABLES` queries (line 231)
- `ORDER BY ALL` is rewritten to removal + `rowsort` (line 266) — weaker ordering guarantee
- Queries with DuckDB named params (`=>`) (line 254)
- Queries mixing virtual columns with `*` (line 260)
- ~30 unsupported DuckDB functions (line 647–707)

These are all reasonable exclusions, but the skip volume is not quantified. The test summary (line 837–880) reports pass/fail counts but doesn't report the number of **skipped** directives within passing tests. A test file could have 50 queries, skip 49, pass the 1 remaining, and show as "passed."

### Error Handling

`statement error` blocks can be converted to `statement ok` via `is_hybrid_incompatible_error()` (line 711). This converts errors matching "READ-ONLY", "DOES NOT EXIST!", "MISSING EXTENSION ERROR", etc. to `statement ok`. This is correct for hybrid mode but means those error paths are never tested via DataFusion.

### Single Mega-Test

All SLT files run inside `run_all_sqllogictests()` (line 813) — a single `#[tokio::test]`. If one file panics, subsequent files don't run. Also makes CI granularity poor (can't re-run a single SLT test).

## Cross-Engine Test Analysis

### Roundtrip Interop Tests (`tests/roundtrip_interop_tests.rs`)

**Silent Skip Without `#[ignore]`**: All 6 tests use `find_duckdb()` → `return` pattern. When DuckDB CLI is not installed, tests silently pass with an `eprintln`. They are not marked `#[ignore]`, so CI reports them as passing. This is the most significant false-positive risk in the test suite.

**Weak Assertions**:
- `test_datafusion_writes_duckdb_reads` (line 168): Asserts `stdout.contains("Alice")` — substring match on DuckDB CLI text output. Would pass if "Alice" appeared in an error message.
- `test_datafusion_writes_duckdb_reads_count` (line 218): Asserts `stdout.contains('3')` — any output containing the character "3" passes (e.g., "13 rows", "Error on line 3").
- `test_schema_evolution_roundtrip` (line 389): Intentionally does NOT panic on DuckDB failure — just prints a finding and returns.

### Cross-Engine Tests (`tests/cross_engine_tests.rs`)

Much stronger assertions. Uses `assert_query_eq()` with row-by-row, column-by-column comparison including float normalization. The `DuckDbConn` wrapper properly converts DuckDB values to strings for comparison. These tests are well-structured.

### Coverage Matrix

| Operation | DF→DF | DF→DuckDB | DuckDB→DF | DuckDB→DuckDB |
|-----------|-------|-----------|-----------|---------------|
| INSERT    | ✅    | ✅        | ✅        | N/A           |
| DELETE    | ✅    | ✅        | ✅        | N/A           |
| UPDATE    | ✅    | ✅        | ✅        | N/A           |
| ALTER     | ✅    | Partial   | Partial   | N/A           |
| PARTITION | ✅    | ❌        | ❌        | N/A           |
| MERGE     | ❌    | ❌        | ❌        | N/A           |
| NULL handling | ✅ | ✅      | ✅        | N/A           |

## Integration Test Analysis

### sql_write_tests.rs — Silent Error Swallowing (P0)

Six tests use `match result { Ok(df) => { ... }, Err(e) => { println!("...not yet supported: {}", e); } }` pattern:
- `test_create_table_as_select` (line 94)
- `test_insert_into_existing_table` (line 184)
- `test_insert_overwrite` (line 362)
- `test_sql_insert_values` (line 433)
- `test_schema_evolution_via_sql` (line 527)
- `test_insert_from_query_with_filter` (line 622)

If these features regress from working to broken, the tests will continue to pass. This is the definition of a false positive. If they are expected to fail, they should use `#[ignore]` or `#[should_panic]`.

### write_partition_tests.rs

Uses a local `batches_to_sorted_strings()` (line 93) that does NOT filter virtual columns. Tests work because they project specific columns in SELECT. This is fine but inconsistent with other test files.

The `arrow_val_to_string()` helper (line 113) has a catch-all `_ => format!("{:?}", array)` which would print the entire array debug representation instead of a single value — a latent bug that would produce confusing output on unsupported types.

### sql_dml_tests.rs

Well-structured. Uses fresh read contexts for verification. Assertions check exact row counts, exact IDs, exact names. No false positive risks identified. This is the gold standard for test structure in this codebase.

### Test Isolation

All tests use `TempDir::new()` for isolation. No shared state between tests. Temporary directories are automatically cleaned up. This is correct.

## Coverage Gap Analysis

### What Write Operations Lack Tests

1. **MERGE operations via DataFusion** — `merge_tests.rs` exists but only tests DuckDB-originated merges; no DF-originated MERGE
2. **Partitioned DELETE** — no test for DELETE on a partitioned table written by DataFusion
3. **Partitioned UPDATE** — no test for UPDATE on a partitioned table
4. **Multi-file DELETE** — tests only delete from tables with a single data file
5. **Cross-engine partitioned writes** — DF writes partitioned → DuckDB reads (not tested)

### What Error Paths Are Untested

1. **Object store failures during write** — no test simulates S3 errors
2. **Metadata commit failure with cleanup** — `test_orphaned_file_cleanup` tests cleanup directly but not the failure→cleanup flow
3. **Concurrent write conflicts** — `concurrent_write_tests.rs` exists but worth verifying assertion strength
4. **Write to non-existent schema** — not tested
5. **Write with schema mismatch in Parquet** — partially tested via `test_append_type_mismatch_fails`

### Multi-Backend Coverage

- **DuckDB backend**: Well-tested via SLT adapter, cross-engine tests
- **SQLite backend**: Well-tested via write tests, DML tests, partition tests
- **PostgreSQL backend**: All tests `#[ignore]` (require Docker) — zero CI coverage
- **MySQL backend**: All tests `#[ignore]` (require Docker) — zero CI coverage

## Findings

### [TH-1] sql_write_tests.rs silently passes on errors (Severity: P0)

- **File(s)**: `tests/sql_write_tests.rs:94,184,362,433,527,622`
- **Description**: Six test functions catch `Err(e)` from SQL execution and `println!` instead of failing. If a working feature regresses, these tests will continue to pass.
- **Impact**: Regressions in CTAS, INSERT VALUES, INSERT OVERWRITE, schema evolution, and filtered INSERT would go undetected.
- **Suggestion**: Either assert the error is expected (`assert!(result.is_err())`) or mark tests `#[ignore]` with a tracking issue. If the feature works, remove the `Err` arm entirely.
- **Effort**: S

### [TH-2] Roundtrip interop tests silently skip without `#[ignore]` (Severity: P0)

- **File(s)**: `tests/roundtrip_interop_tests.rs:132-136,189-193,229-233,300-304,430-434,527-531`
- **Description**: All 6 roundtrip tests use `find_duckdb() → return` instead of `#[ignore]`. CI reports them as passing even when DuckDB CLI is absent, giving false confidence in interop.
- **Impact**: The most critical interop tests (DF writes → DuckDB reads) may never actually run in CI.
- **Suggestion**: Use `#[ignore]` and run with `cargo test -- --ignored` in a CI job that has DuckDB. Or fail with a clear skip message: `panic!("DuckDB CLI required")`.
- **Effort**: S

### [TH-3] Weak substring assertions in roundtrip tests (Severity: P1)

- **File(s)**: `tests/roundtrip_interop_tests.rs:168-182,218-222`
- **Description**: `stdout.contains("Alice")` and `stdout.contains('3')` are used to validate DuckDB CLI output. These are substring matches on unstructured text output that could match error messages or irrelevant content.
- **Impact**: A DuckDB error containing "Alice" or "3" in its message would pass the test. `contains('3')` would also match "13", "30", "300", etc.
- **Suggestion**: Parse DuckDB output into structured rows (split by newlines and tabs/pipes) and compare exact values. Or use `DuckDbConn` wrapper (from `cross_engine_tests.rs`) which provides structured output.
- **Effort**: M

### [TH-4] Massive helper code duplication across test files (Severity: P1)

- **File(s)**: `tests/cross_engine_tests.rs`, `tests/cross_engine_insert_tests.rs`, `tests/cross_engine_dml_tests.rs`, `tests/common/test_utils.rs`, `tests/write_partition_tests.rs`
- **Description**: `duckdb_value_to_string()`, `arrow_value_to_string()`, `batches_to_strings()`, `normalize_value()`, `assert_query_eq()`, setup helpers are duplicated across 5+ files with subtle differences (e.g., different virtual column filtering, different fallback formatting).
- **Impact**: Bug fixes in one copy don't propagate. Virtual column filtering differences mean tests may include or exclude different columns in comparisons.
- **Suggestion**: Consolidate into `tests/common/test_utils.rs` (already partially started). Each test file should `use common::test_utils::*` instead of redefining helpers.
- **Effort**: M

### [TH-5] Virtual column filtering inconsistency (Severity: P1)

- **File(s)**: `tests/cross_engine_tests.rs:245`, `tests/cross_engine_insert_tests.rs:209`, `tests/common/test_utils.rs:181`
- **Description**: `cross_engine_tests.rs` and `cross_engine_insert_tests.rs` filter only `filename` and `file_row_number` (2 columns), while `common/test_utils.rs` filters 5 columns (`filename`, `file_row_number`, `rowid`, `snapshot_id`, `file_index`). Tests using the 2-column filter include `rowid`, `snapshot_id`, `file_index` in DataFusion results that DuckDB results don't have, causing column count mismatches or wrong comparisons.
- **Impact**: Cross-engine tests may compare DataFusion results (with extra virtual columns) against DuckDB results (without them), leading to either test failures or tests that happen to work only because they SELECT specific columns.
- **Suggestion**: All `batches_to_strings()` implementations should filter the same set of virtual columns. Use the centralized definition from `test_utils.rs`.
- **Effort**: S

### [TH-6] Timestamp downcast assumes microsecond precision in hybrid adapter (Severity: P1)

- **File(s)**: `tests/hybrid_asyncdb.rs:566-571`
- **Description**: `convert_batch_to_strings()` matches `DataType::Timestamp(_, _)` and always downcasts to `TimestampMicrosecondArray`. Nanosecond or millisecond timestamps will cause a panic.
- **Impact**: Any SLT test that produces a non-microsecond timestamp column will crash. Currently benign because DuckDB DuckLake uses microsecond, but fragile.
- **Suggestion**: Match on the `TimeUnit` within the timestamp variant and downcast to the correct array type (like `test_utils.rs` does).
- **Effort**: S

### [TH-7] Single mega-test for SLT runner (Severity: P2)

- **File(s)**: `tests/sqllogictest_runner.rs:813-880`
- **Description**: All SLT files run inside a single `#[tokio::test]` function. If one file panics, subsequent files don't execute. CI shows one pass/fail for all SLT tests combined.
- **Impact**: Hard to identify which SLT test fails. Can't re-run individual SLT tests. A panic in an early test file hides failures in later files.
- **Suggestion**: Generate individual test functions per `.test` file using a build script or macro (like `sqllogictest` crate's `sqllogictest_test!` macro).
- **Effort**: M

### [TH-8] `rewrite_unqualified_tables()` is a no-op (Severity: P2)

- **File(s)**: `tests/sqllogictest_runner.rs:604-612`
- **Description**: This function just returns `line.to_string()`. The comment says "Already handled by HybridDuckLakeDB::rewrite_table_references" — but the preprocessor calls it on raw SQL lines before they reach the hybrid adapter. If a test file uses bare table names after `USE ducklake`, the preprocessor won't add `ducklake.main.` prefixes.
- **Impact**: Tables referenced without `ducklake.` prefix after `USE ducklake` would fail to resolve in DataFusion. However, the hybrid adapter's `rewrite_table_references()` at `hybrid_asyncdb.rs:148` handles the `ducklake.` prefix case. The gap is for truly unqualified names (just `table_name`), which rely on `SessionConfig::default_catalog_and_schema` set via `refresh_catalog()` — this does work.
- **Suggestion**: Remove the dead function to avoid confusion. The current approach of setting default catalog/schema in the session config is correct.
- **Effort**: S

### [TH-9] No skip counting in SLT adapter (Severity: P2)

- **File(s)**: `tests/sqllogictest_runner.rs:33-427`
- **Description**: The preprocessor silently removes queries, statements, and entire sections but doesn't count how many directives were skipped. The test summary shows pass/fail for test files but not the actual query coverage within each file.
- **Impact**: A test file could have all its queries skipped by preprocessing and still report as "passed." Regression in skip logic (accidentally skipping too much) would go unnoticed.
- **Suggestion**: Count skipped directives and log them in the summary. Optionally fail if skip percentage exceeds a threshold (e.g., >80% of a file skipped).
- **Effort**: S

### [TH-10] `arrow_val_to_string()` catch-all prints entire array (Severity: P3)

- **File(s)**: `tests/write_partition_tests.rs:148`
- **Description**: The fallback case `_ => format!("{:?}", array)` formats the entire array, not a single value. This would produce huge, confusing output for any unsupported type.
- **Impact**: Low — only triggers on unsupported types, which don't currently appear in partition tests. But would cause test output confusion if types are added.
- **Suggestion**: Use `datafusion::arrow::util::display::array_value_to_string(array, idx)` as fallback, matching the pattern in `hybrid_asyncdb.rs:594`.
- **Effort**: S

### [TH-11] No cross-engine tests for partitioned writes (Severity: P2)

- **File(s)**: `tests/write_partition_tests.rs`, `tests/cross_engine_partition_tests.rs`
- **Description**: Partition write tests (`write_partition_tests.rs`) only test DF-write→DF-read. Cross-engine partition tests exist but should verify that DuckDB can read DF-partitioned data and vice versa.
- **Impact**: Partitioned data written by DataFusion may not be readable by DuckDB (Hive directory layout differences, partition metadata format), but this interop gap would not be caught.
- **Suggestion**: Add at least one test: DF writes partitioned → DuckDB reads, verifying row count and values.
- **Effort**: M

### [TH-12] PostgreSQL and MySQL cross-engine tests have zero CI coverage (Severity: P2)

- **File(s)**: `tests/cross_engine_postgres_tests.rs`, `tests/cross_engine_mysql_tests.rs`
- **Description**: All tests in these files are `#[ignore]` with "Requires Docker" comments. Without a CI job that provisions Docker with PostgreSQL/MySQL, these tests never run.
- **Impact**: Backend-specific interop bugs would only be caught by manual testing.
- **Suggestion**: Add a CI job with Docker services for PostgreSQL and MySQL that runs `cargo test -- --ignored`. Or add lightweight unit tests that mock the backend.
- **Effort**: L

### [TH-13] `test_schema_evolution_roundtrip` intentionally doesn't fail (Severity: P2)

- **File(s)**: `tests/roundtrip_interop_tests.rs:389-403`
- **Description**: When DuckDB fails to read the evolved schema catalog, the test prints a diagnostic message and `return`s instead of panicking. This means the test always passes regardless of interop status.
- **Impact**: Schema evolution interop regressions would never be caught.
- **Suggestion**: If the feature is expected to work, assert success. If it's a known gap, use `#[ignore]` with a comment.
- **Effort**: S
