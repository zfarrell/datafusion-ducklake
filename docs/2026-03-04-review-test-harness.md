# Review Cycle 6: Test Harness Review
Date: 2026-03-04

## Summary

Thorough review of the test infrastructure across ~48K lines of test code in 68+ files. Found 28 issues across false-positive risks, assertion quality, coverage gaps, test isolation, helper duplication, and routing logic. The most critical findings relate to duplicated type-conversion code between `hybrid_asyncdb.rs` and `test_utils.rs`, a test that only validates initial state (never exercising transitions), and the SLT preprocessor potentially hiding too many tests via aggressive filtering. Overall the test infrastructure is solid—`assert_results_eq` guards against zip truncation and float normalization covers common formatting drift—but several specific gaps could mask regressions.

## Findings

### R6-T-001: Transaction state tracking test is a false positive
- **File(s)**: `tests/hybrid_asyncdb.rs:971-982`
- **Severity**: P1
- **Category**: false-positive
- **Description**: `test_transaction_state_tracking` creates a `HybridDuckLakeDB` and only asserts the initial state (`in_transaction == false`). It never calls `run("BEGIN")`, `run("COMMIT")`, or `run("ROLLBACK")` to test actual state transitions. The test name implies full transition coverage but passes vacuously.
- **Impact**: The `BEGIN`/`COMMIT`/`ROLLBACK` state machine could break without any test failure. Since transaction-in-progress routes reads to DuckDB instead of DataFusion (line 446-501), a broken tracker could cause silent data divergence in SLT tests.
- **Suggested Fix**: Make the test async, call `db.run("BEGIN")` and assert `in_transaction == true`, then `db.run("COMMIT")` and assert `false`, and similarly for `ROLLBACK`. Also test `BEGIN` → `ROLLBACK` path.
- **Effort**: S

### R6-T-002: Duplicated type-to-string conversion between hybrid_asyncdb and test_utils
- **File(s)**: `tests/hybrid_asyncdb.rs:571-735`, `tests/common/test_utils.rs:66-198`
- **Severity**: P1
- **Category**: helper-duplication
- **Description**: `convert_batch_to_strings` in `hybrid_asyncdb.rs` duplicates virtually the same Arrow-to-string conversion logic as `arrow_value_to_string` + `batches_to_strings` in `test_utils.rs`. The two implementations handle Date32 differently: `hybrid_asyncdb.rs` uses `value_as_date()`, while `test_utils.rs` uses manual `from_num_days_from_ce_opt(days + 719_163)`. They could diverge (and produce different output for edge-case dates), making cross-engine comparisons appear to "match" when they shouldn't, or vice versa.
- **Impact**: A formatting fix applied to one location but not the other could silently cause SLT tests to pass/fail differently from cross-engine tests. The two code paths are never cross-validated.
- **Suggested Fix**: Extract the conversion into `test_utils.rs` and have `hybrid_asyncdb.rs` call it (possibly via a shared trait or function that returns `Result<String, E>` so it's compatible with both error types). Alternatively, add a fuzz test that feeds identical data through both paths and asserts equal output.
- **Effort**: M

### R6-T-003: `create_object_store()` helper duplicated in 17 test files
- **File(s)**: `tests/write_tests.rs:26`, `tests/sql_write_tests.rs:22`, `tests/update_tests.rs:23`, `tests/stats_tests.rs:25`, `tests/virtual_column_tests.rs:19`, `tests/deep_edge_case_tests.rs:31`, `tests/concurrent_write_tests.rs:14`, `tests/delete_tests.rs:23`, `tests/edge_case_tests.rs:34`, `tests/view_tests.rs:22`, `tests/sql_dml_tests.rs:24`, `tests/file_pruning_tests.rs:22`, `tests/interop_type_tests.rs:25`, `tests/issue_repro_type_tests.rs:27`, `tests/issue_repro_misc_tests.rs:27`, `tests/roundtrip_interop_tests.rs:69`, `tests/adversarial_catalog_tests.rs:59`
- **Severity**: P3
- **Category**: helper-duplication
- **Description**: The trivial `fn create_object_store() -> Arc<dyn ObjectStore>` wrapping `LocalFileSystem::new()` is copy-pasted across 17 test files. Similarly, `create_test_env()` has ~20 slightly different copies with varying return types (`SqliteMetadataWriter` vs `Arc<SqliteMetadataWriter>`).
- **Impact**: Low direct bug risk, but makes maintenance harder. A change in setup logic (e.g., adding a custom runtime) would need to be applied in 20 places.
- **Suggested Fix**: Move `create_object_store()` and a canonical `create_test_env()` into `tests/common/test_utils.rs`. Different return type variants can be thin wrappers.
- **Effort**: M

### R6-T-004: `open_in_datafusion_duckdb` duplicated in 4 test files
- **File(s)**: `tests/cross_engine_tests.rs:74`, `tests/cross_engine_insert_tests.rs:62`, `tests/cross_engine_inline_tests.rs:21`, `tests/cross_engine_partition_tests.rs:27`
- **Severity**: P3
- **Category**: helper-duplication
- **Description**: Same helper function copy-pasted. Also `open_in_datafusion_sqlite` is duplicated in 3 files, and `open_in_datafusion_writable` in 2.
- **Impact**: Same as R6-T-003.
- **Suggested Fix**: Move to `tests/common/test_utils.rs`.
- **Effort**: S

### R6-T-005: SLT preprocessor may hide too many tests via aggressive filtering
- **File(s)**: `tests/sqllogictest_runner.rs:616-676`
- **Severity**: P2
- **Category**: silent-skip
- **Description**: `contains_unsupported_function()` matches very broad patterns like `COLUMNS(` and `STATS(` which could match legitimate SQL (e.g., `information_schema.columns`). The filter `SHOW TABLES` catches both `SHOW TABLES` and `SHOW ALL TABLES` as separate entries but also catches any SQL line containing those words in a different context. There are 47+ SLT test directories, and the `meaningful_count` guard (line 794) only checks `> 0`, meaning a test file with just 1 remaining statement would still pass, even though 99% of its content was filtered out.
- **Impact**: Entire test files could have most of their content filtered away, reducing the effective SLT coverage to near-zero for those files while still appearing as "passing" in test output.
- **Suggested Fix**: (1) Log the total directive count vs. meaningful count in the eprintln warning. (2) Consider raising the `meaningful_count` threshold, or at least tracking a ratio (e.g., if >80% of directives are filtered, warn prominently). (3) Make the `COLUMNS(` and `STATS(` patterns more specific to avoid false matches.
- **Effort**: M

### R6-T-006: `normalize_value` hides type confusion between integers and floats
- **File(s)**: `tests/common/test_utils.rs:299-311`
- **Severity**: P2
- **Category**: assertion-quality
- **Description**: `normalize_value` converts string values containing `.` to `format!("{:.6}", f)`. This means a value like `"100.0"` (float) and `"100.000000"` (unexpected precision) would be normalized to the same string. While this is intentional for float formatting drift, it could hide a bug where an integer column returns `"100.0"` instead of `"100"`. The comment correctly notes this, but the `assert_results_eq` function always applies normalization—there's no "strict mode" available.
- **Impact**: A regression where an integer column starts returning `"100.0"` instead of `"100"` would be caught (no `.` in expected), but if DuckDB returns `"100.0"` and DataFusion returns `"100.000000"`, both normalize to the same string, hiding a formatting difference that might indicate a type issue.
- **Suggested Fix**: Add an optional `strict: bool` parameter to `assert_results_eq` (defaulting to current behavior) for tests that want exact string comparison. Use strict mode in type-roundtrip tests.
- **Effort**: S

### R6-T-007: `format_float` in hybrid_asyncdb silently converts integer-like floats
- **File(s)**: `tests/hybrid_asyncdb.rs:551-568`
- **Severity**: P3
- **Category**: assertion-quality
- **Description**: `format_float(20.0)` returns `"20.0"`, but DuckDB might format the same value as `"20"`. This formatting difference is handled during normalization in `assert_results_eq`, but the SLT framework does exact string comparison. If a DuckDB SLT test expects `20` and DataFusion's hybrid adapter returns `20.0`, the test correctly fails. However, if both paths use `format_float` (e.g., both DataFusion reads), the formatting is consistent but doesn't match DuckDB conventions.
- **Impact**: Potential SLT test failures could be masked or introduced by this formatting choice, depending on the expected values in `.test` files.
- **Suggested Fix**: Verify that `format_float` output matches DuckDB's actual formatting for all edge cases (0.0, -0.0, very large numbers, subnormals). Add test cases for these.
- **Effort**: S

### R6-T-008: `is_write_statement` doesn't handle subqueries starting with SELECT
- **File(s)**: `tests/hybrid_asyncdb.rs:117-143`
- **Severity**: P2
- **Category**: routing-logic
- **Description**: The routing logic uses `starts_with` on the uppercased, trimmed SQL. A `WITH ... INSERT ...` CTE would not match because it starts with `WITH`, routing it to DataFusion (read path) instead of DuckDB. While the test at line 951-956 confirms `WITH...SELECT` is correctly routed to DataFusion, there's no test for `WITH...INSERT` or `WITH...DELETE`.
- **Impact**: If any SLT test uses `WITH cte AS (...) INSERT INTO ...`, the statement would be incorrectly routed to DataFusion's read path instead of DuckDB's write path, causing a confusing error.
- **Suggested Fix**: Add handling for CTEs that contain DML: check if the SQL contains `INSERT`, `UPDATE`, or `DELETE` keywords after any `WITH...AS` prefix. Add unit tests for `WITH cte AS (...) INSERT INTO t SELECT * FROM cte`.
- **Effort**: M

### R6-T-009: `rewrite_table_references` doesn't handle double-quoted identifiers
- **File(s)**: `tests/hybrid_asyncdb.rs:150-216`
- **Severity**: P2
- **Category**: routing-logic
- **Description**: The string-literal-aware rewriter handles single-quoted strings but not double-quoted identifiers. SQL like `SELECT * FROM "ducklake"."my_table"` would have `ducklake.` matched inside a quoted identifier context, potentially rewriting it to `"ducklake.main."my_table"` (mangled output).
- **Impact**: If any SLT test uses double-quoted identifiers containing `ducklake.`, the rewriter would corrupt the SQL.
- **Suggested Fix**: Extend the char-by-char parser to also skip double-quoted regions (same logic as single-quoted). Add test case.
- **Effort**: S

### R6-T-010: SELECT clause virtual column check uses uppercase comparison but column names are lowercase
- **File(s)**: `tests/hybrid_asyncdb.rs:344-370`
- **Severity**: P3
- **Category**: routing-logic
- **Description**: The virtual column stripping logic converts the SQL to uppercase for matching (`sql_upper`) but then calls `is_identifier_in_clause(select_clause, &name.to_uppercase())` where `name` comes from the Arrow schema (which uses lowercase like `"rowid"`, `"snapshot_id"`). The `to_uppercase()` call on name ensures the comparison works, but the `EXTENSION_VIRTUAL_COLS` and `DUCKLAKE_VIRTUAL_COLS` arrays are defined in lowercase. If someone adds a mixed-case virtual column name, the logic would need updating in two places.
- **Impact**: Minor—current code works correctly since both sides uppercase. But fragile to future changes.
- **Suggested Fix**: Add a comment explaining the uppercase invariant, or normalize both arrays to uppercase at definition time.
- **Effort**: S

### R6-T-011: Cross-engine UPDATE test uses f64 equality for float comparison
- **File(s)**: `tests/cross_engine_tests.rs:630-635`
- **Severity**: P3
- **Category**: assertion-quality
- **Description**: `assert_eq!(df_prices, duckdb_prices)` on line 632 compares `Vec<f64>` directly. While this is technically correct when both values are parsed from the same string representation, if the strings differ slightly (e.g., `"899.99"` vs `"899.990000000001"`), the `parse::<f64>()` would produce different bit patterns, and the comparison would fail with an unhelpful error message. The epsilon comparisons on lines 633-635 are a better pattern.
- **Impact**: Low—the test currently works because both engines produce the same string. But brittle to future changes.
- **Suggested Fix**: Remove the `assert_eq!(df_prices, duckdb_prices)` line (it's redundant with the per-element epsilon checks that follow) or replace with element-wise epsilon comparison.
- **Effort**: S

### R6-T-012: No test coverage for `ORDER BY ALL` rewriting edge cases
- **File(s)**: `tests/hybrid_asyncdb.rs:312-322`, `tests/sqllogictest_runner.rs:516-534`
- **Severity**: P2
- **Category**: coverage-gap
- **Description**: `rewrite_order_by_all` in `hybrid_asyncdb.rs` and `sqllogictest_runner.rs` has no unit tests. The function uses `to_uppercase().find("ORDER BY ALL")` which would also match inside string literals (`WHERE col = 'ORDER BY ALL'`) because the hybrid version doesn't have string-literal awareness for this rewrite (unlike `rewrite_table_references`). There are two independent implementations of the same logic.
- **Impact**: An SLT test with a string containing "ORDER BY ALL" would have that string corrupted.
- **Suggested Fix**: Add unit tests for: (1) basic `ORDER BY ALL` removal, (2) `ORDER BY ALL` at end vs. followed by `LIMIT`, (3) `ORDER BY ALL` inside a string literal (should not be rewritten). Consider merging the two implementations.
- **Effort**: S

### R6-T-013: `is_three_part_ref` doesn't handle numeric schema names
- **File(s)**: `tests/hybrid_asyncdb.rs:240-259`
- **Severity**: P3
- **Category**: routing-logic
- **Description**: `is_three_part_ref` checks `c.is_alphabetic() || c == '_'` for the start of the identifier after the dot. SQL identifiers can also start with a digit when quoted (e.g., `ducklake."123schema".table`). While unquoted numeric identifiers are invalid SQL, this means the function would fail to detect a three-part reference with a quoted numeric schema name.
- **Impact**: Very low—unlikely to appear in DuckLake tests.
- **Suggested Fix**: Document the limitation or add quoted-identifier handling.
- **Effort**: S

### R6-T-014: DuckDB in-transaction read path uses `DefaultColumnType::Any` for all columns
- **File(s)**: `tests/hybrid_asyncdb.rs:464-466`
- **Severity**: P3
- **Category**: assertion-quality
- **Description**: When inside a transaction, the hybrid adapter routes reads to DuckDB and returns `DefaultColumnType::Any` for all columns. The SLT framework uses column types for result validation (e.g., `query I` expects Integer). Using `Any` means type validation is skipped for in-transaction queries.
- **Impact**: An in-transaction query that returns wrong column types would not be caught by the SLT framework.
- **Suggested Fix**: Map DuckDB column types to appropriate `DefaultColumnType` values. Or document this as a known limitation.
- **Effort**: M

### R6-T-015: `write_tests.rs` doesn't verify actual data values for temporal types
- **File(s)**: `tests/write_tests.rs:133-182`
- **Severity**: P2
- **Category**: assertion-quality
- **Description**: `test_write_temporal_types` writes Date32 and Timestamp data, then only verifies `COUNT(*) == 2`. It never reads back the actual date/timestamp values to verify they roundtrip correctly. A bug that writes garbage dates but correct row counts would pass.
- **Impact**: A data corruption bug in temporal type writing would not be caught.
- **Suggested Fix**: Add `SELECT id, date, timestamp FROM test.main.events ORDER BY id` and assert the actual values match what was written.
- **Effort**: S

### R6-T-016: `test_write_multiple_batches` and `test_append_semantics` only check COUNT(*)
- **File(s)**: `tests/write_tests.rs:184-343`
- **Severity**: P2
- **Category**: assertion-quality
- **Description**: Both tests write specific data values but only verify the total row count. `test_write_multiple_batches` writes `["a", "b", "c", "d"]` but never verifies these values. `test_append_semantics` writes `[100, 200, 300, 400]` but only checks `COUNT(*) == 4`.
- **Impact**: Bugs in value writing (e.g., all values written as NULL, or columns swapped) would pass these tests.
- **Suggested Fix**: Add value verification: `SELECT value FROM test.main.data ORDER BY id` and assert expected values.
- **Effort**: S

### R6-T-017: Schema evolution tests only check COUNT(*), not actual data
- **File(s)**: `tests/write_tests.rs:600-912`
- **Severity**: P2
- **Category**: assertion-quality
- **Description**: `test_append_add_nullable_column`, `test_append_remove_column`, and `test_append_reorder_columns` all only verify `COUNT(*) == 4` after schema evolution. None of them verify that the original rows have NULL for the new column, or that reordered columns map correctly to the right values.
- **Impact**: A bug where schema evolution corrupts column mapping (e.g., `value` column reads from `name` column's data) would pass all these tests.
- **Suggested Fix**: Add queries that read back specific column values and verify correctness. For the reorder test, verify that `id=3` has `value=300` (not `name="Charlie"`).
- **Effort**: S

### R6-T-018: `test_replace_semantics` doesn't verify old data is actually gone
- **File(s)**: `tests/write_tests.rs:229-287`
- **Severity**: P3
- **Category**: assertion-quality
- **Description**: The replace test writes `[1,2,3]` then replaces with `[4,5]`. It verifies `ids == [4,5]` but doesn't check that `id=1,2,3` are truly absent. In a subtle bug where replace appends instead of replacing, the `ORDER BY id` with `num_rows == 2` assertion would catch it, but an explicit `WHERE id IN (1,2,3)` returning 0 rows would be more robust.
- **Impact**: Low—current assertions are sufficient to catch replace failures.
- **Suggested Fix**: Optional: add a `WHERE id = 1` check returning 0 rows for extra safety.
- **Effort**: S

### R6-T-019: No tests for `convert_batch_to_strings` with unsupported types
- **File(s)**: `tests/hybrid_asyncdb.rs:721-727`
- **Severity**: P3
- **Category**: coverage-gap
- **Description**: `convert_batch_to_strings` panics on unsupported Arrow types (line 722-726). There's no test that exercises this panic path, and no test that verifies the supported types list is complete (e.g., what about `Decimal256`, `Time32`, `Time64`, `Duration`, `Interval`, `Map`, `List`, `Struct`?).
- **Impact**: A DuckLake table with an unsupported type would cause a panic instead of a clear error.
- **Suggested Fix**: Add tests for each handled type and at least one test that catches the panic for an unhandled type. Consider changing the panic to return a `HybridError`.
- **Effort**: M

### R6-T-020: SLT `statement error` to `statement ok` conversion may mask real errors
- **File(s)**: `tests/sqllogictest_runner.rs:316-402`
- **Severity**: P2
- **Category**: false-positive
- **Description**: The preprocessor converts `statement error` to `statement ok` for "hybrid-incompatible" error patterns. The matching is done on the _error text_ (e.g., `READ-ONLY`, `DOES NOT EXIST!`). If DuckDB adds a new test case where `statement error` is expected for a different reason but the error text happens to contain one of these patterns, the test would be silently converted to `statement ok` and would pass even if the statement actually fails with a different error.
- **Impact**: Future DuckDB test additions could be silently neutered by overly broad pattern matching.
- **Suggested Fix**: (1) Log each conversion with the full error text (currently done via `tracing::warn`, good). (2) Consider matching on the _combination_ of SQL + error text, not just error text alone.
- **Effort**: S

### R6-T-021: No coverage for multi-line SQL statements in `is_write_statement`
- **File(s)**: `tests/hybrid_asyncdb.rs:117-143`
- **Severity**: P3
- **Category**: coverage-gap
- **Description**: `is_write_statement` checks `trimmed.starts_with(...)` which works for single-line SQL. But the SLT framework may pass multi-line SQL (e.g., a `CREATE TABLE` spanning multiple lines where the first line is just the keyword). The function is called with the full SQL string including newlines, but `trim()` only removes leading/trailing whitespace, not internal newlines. A SQL like `\nCREATE TABLE\n  t (x INT)` would work correctly because `trim()` removes the leading `\n`. However, a SQL like `-- comment\nCREATE TABLE t (x INT)` would NOT match because it starts with `--`.
- **Impact**: SQL with leading comments would be misrouted to DataFusion.
- **Suggested Fix**: Strip leading SQL comments before the `starts_with` check, or add a test confirming that SLT never sends SQL with leading comments.
- **Effort**: S

### R6-T-022: `refresh_catalog` creates entire new SessionContext on every write
- **File(s)**: `tests/hybrid_asyncdb.rs:262-282`
- **Severity**: P3
- **Category**: routing-logic
- **Description**: Every write statement triggers a complete `SessionContext` replacement, including re-registering UDFs and creating a new metadata provider. This is correct for catalog freshness but creates a new DuckDB connection for the metadata provider on every write (since `DuckdbMetadataProvider::new` opens a new connection). For SLT tests with many writes, this could be slow and could hit DuckDB connection limits.
- **Impact**: Performance issue for large SLT tests; not a correctness issue.
- **Suggested Fix**: Consider caching the metadata provider or using a single connection that can be refreshed. Document the design choice if intentional.
- **Effort**: M

### R6-T-023: Cross-engine tests don't verify column names/types, only values
- **File(s)**: `tests/cross_engine_tests.rs:119-1031`
- **Severity**: P2
- **Category**: assertion-quality
- **Description**: Cross-engine tests compare result values (strings) but never verify that column names or column types match between DuckDB and DataFusion results. For example, `cross_engine_duckdb_write_df_read` verifies the data values but doesn't check that DataFusion's schema has `id: Int32, product: Utf8, amount: Float64`.
- **Impact**: A bug where DataFusion returns the correct values but with wrong column names or types (e.g., all columns as `Utf8`) would pass.
- **Suggested Fix**: Add schema assertions in at least the core test patterns (pattern 1-3). Compare column names and types from both engines.
- **Effort**: M

### R6-T-024: `df_query` silently filters virtual columns, hiding schema differences
- **File(s)**: `tests/common/test_utils.rs:350-354`
- **Severity**: P3
- **Category**: silent-skip
- **Description**: `df_query` calls `batches_to_strings_filtered` which removes 5 virtual columns (`filename`, `file_row_number`, `rowid`, `snapshot_id`, `file_index`). This is correct for cross-engine comparison, but if a bug introduces a new phantom column (or removes a real column), `df_query` would silently filter it and the test would still pass.
- **Impact**: Low—the virtual columns are well-known and stable.
- **Suggested Fix**: Add a column count assertion in cross-engine tests: `assert_eq!(df_result_columns, expected_column_count)` before filtering.
- **Effort**: S

### R6-T-025: No cross-engine test for BOOLEAN type roundtrip
- **File(s)**: `tests/cross_engine_tests.rs`
- **Severity**: P2
- **Category**: coverage-gap
- **Description**: Cross-engine tests cover INT, VARCHAR, DOUBLE, TIMESTAMP, DATE, and DECIMAL roundtrips. There is no dedicated BOOLEAN roundtrip test. While `cross_engine_assert_query_eq_both_engines` includes a BOOLEAN column in its test data, it uses `assert_results_eq` which normalizes floats—the boolean column values (`true`/`false`) are compared as exact strings, which is correct, but there's no dedicated edge case testing (e.g., DuckDB's `true` vs DataFusion's `true` string formatting, or NULL booleans).
- **Impact**: A BOOLEAN formatting difference between engines would only be caught by the one general test.
- **Suggested Fix**: Add a dedicated `cross_engine_boolean_type_roundtrip` test that covers `true`, `false`, and `NULL` boolean values.
- **Effort**: S

### R6-T-026: No test coverage for BLOB/BINARY type cross-engine roundtrip
- **File(s)**: `tests/cross_engine_tests.rs`, `tests/hybrid_asyncdb.rs:712-720`
- **Severity**: P3
- **Category**: coverage-gap
- **Description**: `convert_batch_to_strings` handles `DataType::Binary` by formatting as hex. But there are no cross-engine tests for BINARY/BLOB data. The `batches_to_strings` in `test_utils.rs` doesn't handle `Binary` at all (would return `<unsupported:Binary>` via the fallback).
- **Impact**: BINARY data might format differently in the hybrid adapter vs. test_utils, or might not roundtrip correctly between engines. No tests would catch this.
- **Suggested Fix**: Add BINARY type to `arrow_value_to_string` in `test_utils.rs`, and add a cross-engine BINARY roundtrip test.
- **Effort**: S

### R6-T-027: Concurrent tests don't verify test isolation (TempDir overlap)
- **File(s)**: `tests/concurrent_tests.rs:1-542`
- **Severity**: P3
- **Category**: test-isolation
- **Description**: Each concurrent test creates its own `TempDir` for isolation, and within that test, multiple async tasks share the same catalog. This is correct design. However, `cargo test` runs different test functions in parallel by default. Two concurrent test functions that both use `create_catalog_with_deletes` could theoretically interfere if DuckDB's `INSTALL ducklake` has global side effects. The `Once` guard in `common/mod.rs:17-26` protects against duplicate installation, but doesn't protect against parallel connections to the same extension.
- **Impact**: Very low—DuckDB extension installation is idempotent and TempDirs are unique per test.
- **Suggested Fix**: No action needed; document that test isolation relies on TempDir uniqueness.
- **Effort**: S

### R6-T-028: `DatePartAliasUdf` return type is hardcoded to Int32
- **File(s)**: `tests/hybrid_asyncdb.rs:786-788`
- **Severity**: P3
- **Category**: routing-logic
- **Description**: The `year()`, `month()`, `day()` UDFs all return `Ok(DataType::Int32)`. But DataFusion's built-in `date_part` returns `Float64` in many versions. If the UDF declares `Int32` but the actual execution returns `Float64`, DataFusion may error or silently cast.
- **Impact**: SLT tests using `year()`, `month()`, `day()` functions might get type errors or unexpected results.
- **Suggested Fix**: Verify the return type matches DataFusion's `date_part` output type, or use `DataType::Float64` to match DataFusion's native behavior.
- **Effort**: S

## Coverage Gap Analysis

| Operation | DF-Write Tests | Cross-Engine Tests | SLT Tests | Notes |
|---|---|---|---|---|
| CREATE TABLE | ✅ (implicit) | ✅ | ✅ | Well covered |
| INSERT | ✅ write_tests | ✅ cross_engine_tests | ✅ | Well covered |
| SELECT (basic) | ✅ | ✅ | ✅ | Well covered |
| DELETE | ✅ delete_tests | ✅ cross_engine_tests | ✅ | Well covered |
| UPDATE | ✅ update_tests | ✅ cross_engine_tests | ✅ | Well covered |
| MERGE | ❌ | ✅ (DuckDB only) | ✅ | No DF-native MERGE test |
| ALTER TABLE ADD | ✅ write_tests | ✅ | ✅ | Well covered |
| ALTER TABLE DROP | ❌ | ✅ | ✅ | No DF-native DROP test |
| CREATE TABLE AS | ✅ sql_write_tests | ❌ | ✅ | No cross-engine CTAS test |
| INT types | ✅ | ✅ | ✅ | Well covered |
| VARCHAR/UTF8 | ✅ | ✅ | ✅ | Well covered |
| FLOAT/DOUBLE | ✅ | ✅ | ✅ | Well covered |
| BOOLEAN | ✅ write_tests | ⚠️ (only in 1 test) | ✅ | Dedicated test missing |
| DATE | ✅ write_tests | ✅ | ✅ | Well covered |
| TIMESTAMP | ✅ write_tests | ✅ | ✅ | Well covered |
| DECIMAL | ❌ | ✅ | ✅ | No DF-write DECIMAL test |
| BINARY/BLOB | ❌ | ❌ | ❓ | No coverage |
| LIST/STRUCT/MAP | ❌ | ❌ | ❌ | Not supported (returns error) |
| NULL handling | ✅ | ✅ | ✅ | Well covered |
| COUNT(*) opt | ✅ | ✅ | ✅ | Well covered |
| Filter pushdown | ✅ delete_filter | ❌ | ✅ | No cross-engine filter test |
| Concurrent reads | ✅ concurrent | ❌ | ❌ | Only single-engine concurrent |
| Schema evolution | ✅ write_tests | ✅ cross_engine | ✅ | Good coverage |
| Time travel | ❌ | ❌ | ⚠️ (SLT only) | No native DF test |
| Views | ✅ view_tests | ❌ | ✅ | No cross-engine view test |
| Compaction | ✅ compaction_tests | ❌ | ✅ | No cross-engine compact test |
| Transaction state | ⚠️ (R6-T-001) | ❌ | ✅ (SLT) | Unit test is false positive |

## Codex Findings

Codex review confirmed R6-T-001 (transaction state tracking test is a false positive) and R6-T-002 (duplicated conversion logic). Additionally flagged:

1. **test_transaction_state_tracking is vacuous** — confirmed as P1 (R6-T-001 above). Only initial state checked.
2. **Duplicated type conversion** — confirmed as P1 (R6-T-002 above). Two independent implementations.
3. **COUNT-only assertions in write_tests.rs** — confirmed as P2 (R6-T-015, R6-T-016, R6-T-017 above).
4. **`create_object_store` duplication** — confirmed as P3 (R6-T-003 above).
5. **No strict assertion mode** — Codex noted `assert_results_eq` always normalizes floats, matching R6-T-006.
6. **`format_float` edge cases** — Codex flagged potential mismatch with DuckDB for `-0.0` and subnormal floats, matching R6-T-007.

All codex findings were validated against source code. No additional P0/P1 issues identified by codex beyond what was found in manual review.

## Statistics

- **Total findings**: 28
- **By severity**: P0: 0, P1: 2, P2: 9, P3: 17
- **By category**: false-positive: 2, assertion-quality: 7, coverage-gap: 5, test-isolation: 1, silent-skip: 2, helper-duplication: 4, routing-logic: 7
