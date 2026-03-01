# Test Harness & Cross-Engine Test Pattern Review

**Date**: 2026-03-01
**Branch**: `ducklake-features/integration`
**Scope**: All test harnesses, SLT adapter/runner, cross-engine tests, new test files

---

## Executive Summary

The test infrastructure for DataFusion-DuckLake is extensive, covering 15+ test files with ~10,000 lines of test code. The cross-engine testing pattern (DF writes → DuckDB reads, DuckDB writes → DF reads, bidirectional roundtrip) is well-designed and provides genuine interoperability confidence.

However, the review identified several correctness risks, code duplication issues, and coverage gaps:

1. **Virtual column stripping uses substring matching** (`sql_upper.contains()`) which can false-positive on column names that appear as substrings in SQL keywords or string literals.
2. **`rewrite_unqualified_tables()` in `sqllogictest_runner.rs` is a dead function** — it returns its input unchanged.
3. **`duckdb_value_to_string()` is duplicated across 6+ files** with inconsistent type coverage — the DML variant silently falls through to `Debug` formatting for Date32, Timestamp, and Decimal128.
4. **No tests exist for combined inlining + partitioning**, error paths for invalid partition columns, or concurrent write scenarios.
5. **PostgreSQL and MySQL tests are `#[ignore]`** and may drift from the main test patterns without CI enforcement.

---

## SLT Hybrid Adapter Findings (`tests/hybrid_asyncdb.rs`)

### Architecture
The hybrid adapter routes writes to DuckDB and reads to DataFusion, implementing the `AsyncDB` trait from the `sqllogictest` crate. This enables running DuckDB's own SQL logic tests against the DataFusion-DuckLake implementation.

### Findings

#### 1. Virtual Column Stripping — False Positive Risk (Medium)
**File**: `hybrid_asyncdb.rs:282-301`
**Issue**: Virtual columns are stripped from results unless `sql_upper.contains(&name.to_uppercase())` returns true. This substring check can false-positive:
- A column named `id` would be found in the SQL `SELECT rowid, * FROM t` because `"ROWID"` contains no match, but `"ID"` would be found in `"ROWID"`. Actually, the check is `sql_upper.contains("FILENAME")`, so the column name is the one being searched for. Let me clarify:
  - The virtual columns are `filename`, `file_row_number`, `rowid`, `snapshot_id`, `file_index`.
  - If a user query contains the literal word "filename" anywhere (e.g., in a string literal or table alias), the column will NOT be stripped, leading to unexpected extra columns in results.
  - Example: `SELECT * FROM files WHERE description = 'filename'` would keep the `filename` virtual column because `"FILENAME"` appears in the SQL.
  - More practically, `rowid` is short enough to appear in identifiers like `ROWID_MAPPING` or aliased column names.

**Recommendation**: Use word-boundary matching or parse column references instead of substring search. A regex like `\bFILENAME\b` would be more precise.

#### 2. `is_three_part_ref` Check (Low)
**File**: `hybrid_asyncdb.rs:178`
**Issue**: The function counts dots in identifiers to determine if a reference is fully qualified. This works for simple cases but can be confused by:
- Quoted identifiers containing dots (e.g., `"my.schema".table`)
- Schema-qualified function calls (e.g., `pg_catalog.func()`)

In practice, DuckLake identifiers rarely contain dots, so the risk is low.

#### 3. Transaction Routing (Correct)
**File**: `hybrid_asyncdb.rs:339-350`
**Behavior**: When `in_transaction` is true, all statements (including reads) are routed to DuckDB. This is correct — transaction isolation requires consistent reads within the same connection.

#### 4. `DatePartAliasUdf` (Correct)
**File**: `hybrid_asyncdb.rs:608-677`
**Behavior**: Provides `year()`, `month()`, `day()` UDFs that delegate to DataFusion's `date_part()`. This correctly bridges DuckDB's function syntax to DataFusion's equivalent.

#### 5. `format_float()` (Correct)
**File**: `hybrid_asyncdb.rs:479`
**Behavior**: Handles NaN, infinity, and ensures decimal points in float output. Matches DuckDB's formatting behavior.

#### 6. Unit Tests (Good)
**File**: `hybrid_asyncdb.rs:680-748`
**Coverage**: Tests for `is_write_statement()` and `rewrite_table_references()` cover the critical routing logic. Could benefit from negative cases (e.g., `SELECT * INTO` which is NOT a write in DuckDB context).

---

## SLT Runner Findings (`tests/sqllogictest_runner.rs`)

### Architecture
The runner preprocesses DuckDB `.test` files by expanding loops, substituting variables, stripping unsupported directives, and skipping incompatible tests. It then feeds the processed tests to the hybrid adapter.

### Findings

#### 1. `rewrite_unqualified_tables()` Is a No-Op (High)
**File**: `sqllogictest_runner.rs:604-612`
**Issue**: This function is documented as rewriting unqualified table names for DataFusion, but the implementation simply returns `line.to_string()`. The comment says "Already handled by HybridDuckLakeDB::rewrite_table_references". This dead code should either be removed or properly implemented as a fallback.

**Risk**: If `HybridDuckLakeDB::rewrite_table_references` ever fails to rewrite a reference, there is no safety net. The no-op function creates a false sense of coverage.

**Recommendation**: Remove the function and its call sites, or implement proper rewriting logic.

#### 2. Loop Expansion — Off-by-One Verified (Correct)
**File**: `sqllogictest_runner.rs:446`
**Behavior**: Uses `for i in start..end` (exclusive of `end`), which matches DuckDB's `loop` semantics. This was verified against DuckDB documentation.

#### 3. Silently Skipped Tests (Medium)
**File**: `sqllogictest_runner.rs:647` (`contains_unsupported_function()`)
**Issue**: An extensive list of unsupported functions causes entire test blocks to be silently skipped. There is no logging or counting of skipped tests, making it impossible to know:
- How many tests are being skipped in a given `.test` file
- Whether a previously-unsupported function has been implemented and the skip is now unnecessary

**Recommendation**: Add a counter or summary log at the end of each test file showing `X of Y statements skipped due to unsupported functions`.

#### 4. `is_hybrid_incompatible_error()` Conversions (Medium)
**File**: `sqllogictest_runner.rs:711`
**Issue**: Certain `statement error` directives are converted to `statement ok`. This means the test no longer validates that the correct error is raised. While necessary for hybrid execution (where DataFusion may not raise the same error as DuckDB), this effectively reduces test coverage.

**Risk**: A regression that causes a statement to silently succeed instead of erroring would not be caught.

#### 5. `has_virtual_column_star_conflict()` (Correct)
**File**: `sqllogictest_runner.rs:617`
**Behavior**: Correctly detects `SELECT rowid, *` patterns that cause DataFusion to error on duplicate projection names. These tests are properly skipped.

#### 6. Auto-Discovery (Good)
**File**: `sqllogictest_runner.rs:813` (`run_all_sqllogictests()`)
**Behavior**: Automatically discovers all `.test` files, which means new test files are picked up without manual registration. This is the correct approach.

---

## Cross-Engine Test Findings

### Helper Code Duplication (High)

The following helper functions/structs are duplicated across 6+ test files with varying implementations:

| Helper | Files | Consistency Issue |
|--------|-------|-------------------|
| `DuckDbConn` | cross_engine_tests, dml_tests, ddl_tests, alter_tests, feature_tests, insert_tests, inline_tests, partition_tests | Identical struct, different method sets |
| `duckdb_value_to_string()` | cross_engine_tests, dml_tests, insert_tests | **DML variant missing Date32, Timestamp, Decimal128** |
| `batches_to_strings()` | cross_engine_tests, dml_tests, insert_tests, ddl_tests | Minor variations in virtual column filtering |
| `arrow_value_to_string()` | cross_engine_tests, dml_tests | Same implementation |
| `assert_results_eq()` / `assert_query_eq()` | Multiple files | Different names, similar logic |
| `setup_ducklake_catalog()` | cross_engine_tests, dml_tests | Different parameter signatures |

**Risk**: The `duckdb_value_to_string()` inconsistency is the most serious. In `cross_engine_dml_tests.rs` (line 226-239), the function falls through to `format!("{v:?}")` for Date32, Timestamp, and Decimal128 values. This produces Rust Debug output (e.g., `Date32(19723)`) instead of formatted values (e.g., `2023-12-25`). If a DML test ever operates on date/timestamp/decimal columns, the assertion comparison would use Debug formatting on the DuckDB side and proper formatting on the DataFusion side, causing **false test failures** rather than false passes.

**Recommendation**: Extract shared helpers into `tests/common/mod.rs` (which already exists for test data generation). Use a single `duckdb_value_to_string()` with full type coverage.

### Core Cross-Engine Tests (`cross_engine_tests.rs`)

**6 tests, all passing**: df_write_df_read, df_write_duckdb_read, duckdb_write_df_read, bidirectional_roundtrip, both_engines_comparison, null_handling.

**Assertion strength**: Good. Tests compare actual cell values, not just row counts. `assert_query_eq()` normalizes floats to 6 decimal places.

**Missing**: No test for empty tables or tables with only NULL rows read cross-engine.

### DML Tests (`cross_engine_dml_tests.rs`)

**20+ tests covering DELETE and UPDATE operations.**

**Assertion strength**: Strong. Tests verify row counts, specific values, and edge cases (delete all rows, no matching rows, multiple sequential deletes).

**Issue**: Uses simplified `duckdb_value_to_string()` (see duplication finding above).

**Good pattern**: `delete_file_schema_verification` test directly inspects delete file Parquet schema.

### DDL Tests (`cross_engine_ddl_tests.rs`)

**Coverage**: Views, DROP TABLE, DROP SCHEMA, CREATE SCHEMA.

**Good pattern**: Tests verify that dropped objects are truly gone (queries fail after drop).

**Missing**: No test for DROP TABLE with concurrent reads.

### ALTER Tests (`cross_engine_alter_tests.rs`)

**14 tests covering rename, defaults, NOT NULL, and comments.**

**Good pattern**: Uses `catch_unwind` to verify old table name doesn't exist after rename.

**Issue**: Gets `table_id` via direct SQLite query to `ducklake_table` (line ~190). This couples the test to the SQLite metadata backend implementation. If a test ever uses a different backend, this query would fail.

### Feature Tests (`cross_engine_feature_tests.rs`)

**Coverage**: Virtual columns, query planner routing, column statistics, conflict detection.

**Good pattern**: Conflict detection tests verify specific error messages.

**Missing**: No test for virtual columns with partitioned tables.

### INSERT Tests (`cross_engine_insert_tests.rs`)

**Coverage**: Basic INSERT, NOT NULL constraints, INSERT INTO...SELECT, CTAS, DEFAULT values, WriteMode::Replace, multi-batch INSERT, footer size metadata.

**Good pattern**: Footer size metadata test verifies the optimization hint is correctly stored.

---

## New Test File Findings

### Write Partition Tests (`write_partition_tests.rs`)

**6 tests covering partitioned writes.**

**Strengths**:
- Verifies Hive-style directory structure (`region=US/`, `region=EU/`)
- Tests filter pushdown with partitioned data
- Non-partitioned regression test ensures no breakage
- COUNT(*) optimization verified

**Gaps**:
- No test for partition column with NULL values
- No test for high-cardinality partition columns
- No test for partition column type coercion (e.g., integer partition keys)
- No test verifying partition pruning actually reduces files scanned (only verifies correct results)

### Write Inline Tests (`write_inline_tests.rs`)

**8 tests covering data inlining.**

**Strengths**:
- Tests threshold-based inlining (small data stays inline, large data goes to Parquet)
- Tests flush operation (inline → Parquet conversion)
- Verifies `files_written == 0` for inlined data
- Tests disabled inlining (limit=0)
- Tests flush no-op (no inlined data to flush)

**Issues**:
- `set_inlining_limit()` writes directly to `ducklake_metadata` table via raw sqlx query (line ~460). This bypasses the metadata provider abstraction and couples to the SQLite schema.
- No test for concurrent inline writes
- No test for inline data surviving catalog reopen

**Gap**: No test combining inlining with partitioning — what happens when a partitioned table has data below the inlining threshold?

### PostgreSQL Backend Tests (`cross_engine_postgres_tests.rs`)

**8 tests, all `#[ignore]` (require Docker).**

**Strengths**:
- Uses `testcontainers::Postgres` for reproducible test environments
- `DuckDbPgConn::try_open()` returns `Option<Self>` for graceful degradation when DuckDB postgres extension is unavailable
- `sqlx_to_libpq()` connection string conversion is tested implicitly
- Mirrors the core cross-engine test patterns

**Issues**:
- Tests are never run in CI (all `#[ignore]`). Patterns can drift from main test suite.
- `try_open()` graceful degradation means some assertions are **silently skipped** when DuckDB can't connect to Postgres. The test passes but does less verification.
- No test for PostgreSQL-specific types (e.g., UUID, JSONB, arrays)

### MySQL Backend Tests (`cross_engine_mysql_tests.rs`)

**8 tests, all `#[ignore]` (require Docker).**

**Same strengths and issues as PostgreSQL tests**, with MySQL-specific connection string conversion (`sqlx_mysql_to_duckdb()`).

**Additional Issue**: `sqlx_mysql_to_duckdb()` extracts host/port/user/password from the sqlx URL format. If the MySQL container assigns a non-standard port (which testcontainers does), the conversion must handle this correctly. No explicit unit test for this conversion.

---

## Coverage Gaps

### Missing Test Patterns

| Gap | Priority | Description |
|-----|----------|-------------|
| Inlining + Partitioning combo | High | No test for partitioned tables with inlined data |
| Error path: invalid partition column | High | No test for `SET PARTITIONED BY` with non-existent column |
| Concurrent writes | Medium | No test for multiple writers to the same table |
| Large data volumes | Medium | Only `#[ignored]` benchmarks exist |
| Schema evolution + partitioning | Medium | No test for ALTER TABLE on partitioned tables |
| Schema evolution + inlining | Medium | No test for ALTER TABLE on tables with inlined data |
| Partition column with NULLs | Medium | Undefined behavior for NULL partition keys |
| Virtual columns + partitioning | Low | No test combining virtual column queries with partitioned tables |
| Catalog reopen with inline data | Low | No test verifying inline data survives closing and reopening the catalog |
| Cross-backend roundtrip | Low | No test for SQLite catalog → Postgres catalog migration |

### Silent Skip Accounting

The SLT runner silently skips tests for:
- Unsupported functions (extensive list at `sqllogictest_runner.rs:647`)
- `concurrentloop` blocks
- Multi-connection statements (`connection con1`)
- `EXPLAIN` and `DESCRIBE` statements
- `__internal_decompress_string` and similar internal functions
- Hybrid-incompatible errors (converted to `statement ok`)

There is no summary of how many statements are skipped per test file. This makes it impossible to track whether test coverage is improving or regressing over time.

---

## False Positive Risks

### Risk 1: Virtual Column Substring Matching (Medium)
**Location**: `hybrid_asyncdb.rs:292`
**Scenario**: A SQL query containing the word "filename" in a string literal or alias would cause the `filename` virtual column to NOT be stripped, adding an unexpected column to results. This would cause a false test failure (extra column), not a false pass.

### Risk 2: Simplified `duckdb_value_to_string()` (Medium)
**Location**: `cross_engine_dml_tests.rs:226-239`
**Scenario**: If DML tests operate on Date/Timestamp/Decimal columns, the DuckDB side would produce Debug-formatted strings while DataFusion produces properly formatted strings. This would cause false test failures.

### Risk 3: `try_open()` Silent Degradation (Low-Medium)
**Location**: `cross_engine_postgres_tests.rs`, `cross_engine_mysql_tests.rs`
**Scenario**: When DuckDB's postgres/mysql extension is unavailable, `try_open()` returns `None` and the DuckDB-side verification is skipped. The test passes but only verifies the DataFusion side, not cross-engine correctness.

### Risk 4: Float Normalization Masking Precision Issues (Low)
**Location**: `cross_engine_tests.rs` (`assert_query_eq`)
**Scenario**: Normalizing floats to 6 decimal places could mask legitimate precision differences between engines. If DataFusion rounds differently than DuckDB beyond 6 decimals, the test would not catch it.

### Risk 5: `rewrite_unqualified_tables()` No-Op (Low)
**Location**: `sqllogictest_runner.rs:604-612`
**Scenario**: If `HybridDuckLakeDB::rewrite_table_references()` misses a table reference, the no-op fallback provides no safety net. The test would fail with a "table not found" error that might be misdiagnosed.

---

### Risk 6: `assert_results_eq` Missing Column-Count Check (High)
**Location**: `cross_engine_postgres_tests.rs:289-290`
**Scenario**: The `assert_results_eq` function uses `zip` to compare rows, which silently truncates the longer row. If one engine returns 3 columns and the other returns 4, `zip` stops at 3 and the extra column is never compared. This is a **false pass** risk.

### Risk 7: Sorting Helpers Mask ORDER BY Regressions (Medium)
**Location**: `write_partition_tests.rs:93, 182`
**Scenario**: Several tests apply `rows.sort()` after a query that includes `ORDER BY`. If the query's ORDER BY is broken, the sort mask hides the regression and the test still passes.

### Risk 8: Partial Hive Directory Verification (Low)
**Location**: `write_partition_tests.rs:324, 340`
**Scenario**: The Hive directory test verifies parquet presence for `region=US` but only checks that `region=EU` directory exists without confirming it contains parquet files. A partial write failure for EU data would not be caught.

---

## Codex CLI Findings

Codex CLI (`/usr/local/bin/codex exec --full-auto`) was successfully invoked to analyze `tests/hybrid_asyncdb.rs`, `tests/cross_engine_postgres_tests.rs`, `tests/write_partition_tests.rs`, and `tests/write_inline_tests.rs`.

### Codex-Identified Issues

1. **High: `assert_results_eq` column-count false positive** — Uses `zip` without asserting per-row width, so extra columns are silently ignored. (`cross_engine_postgres_tests.rs:289-290`)

2. **High: Silent pass when DuckDB+Postgres extensions unavailable** — Several cross-engine tests contain early `return`/conditional blocks when DuckDB can't connect to Postgres, meaning interop can regress without CI failure. (`cross_engine_postgres_tests.rs:394, 426, 696, 747`)

3. **Medium: Weak partial assertions** — Some cross-engine tests validate only partial cells/rows rather than full result sets. (`cross_engine_postgres_tests.rs:403, 653, 661, 749`)

4. **Medium: `test_table_rewrite` uses `contains`** — Table rewrite unit tests use `contains()` for verification, so malformed rewrites can still pass. (`hybrid_asyncdb.rs:723, 732`)

5. **Medium: Partition pruning claimed but not verified** — Tests verify correct final rows but don't confirm that partition pruning actually reduced the number of files scanned. (`write_partition_tests.rs:191`)

6. **Medium: Sort helpers mask ORDER BY regressions** — `rows.sort()` applied after `ORDER BY` queries hides broken ordering. (`write_partition_tests.rs:93, 182`)

7. **Low: Partial Hive directory verification** — Only `region=US` parquet presence is verified; `region=EU` existence is checked but not its contents. (`write_partition_tests.rs:324, 340`)

8. **Low: Inline/flush tests assert counts not content** — Some inline transition tests only verify row counts, not full row content, leaving room for corruption or reordering to go undetected. (`write_inline_tests.rs:382, 421, 509`)

**Cleanup**: No resource leak or cleanup defects found. `TempDir` and container lifetimes are RAII-managed.

---

## Recommendations

### Immediate (before merge)

1. **Extract shared test helpers** into `tests/common/mod.rs`:
   - `DuckDbConn`, `duckdb_value_to_string()`, `batches_to_strings()`, `arrow_value_to_string()`
   - Use the most comprehensive `duckdb_value_to_string()` from `cross_engine_tests.rs`

2. **Remove `rewrite_unqualified_tables()`** from `sqllogictest_runner.rs` — it's dead code that creates confusion.

3. **Add skip-count logging** to the SLT runner so test coverage can be tracked.

### Short-term (next sprint)

4. **Fix virtual column stripping** to use word-boundary matching instead of `contains()`.

5. **Add inlining + partitioning combo tests** — this is the highest-priority coverage gap.

6. **Add partition NULL handling test** to document expected behavior.

### Long-term

7. **Enable PG/MySQL tests in CI** with Docker/testcontainers to prevent drift.

8. **Add concurrent write tests** using `tokio::spawn` with multiple writers.

9. **Add test coverage metrics** to track how many SLT statements are actually executed vs. skipped.
