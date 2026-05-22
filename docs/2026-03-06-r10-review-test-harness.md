# R10 Test Harness Review -- 2026-03-06

**Branch**: `ducklake-features/integration`
**Reviewer**: Claude Opus 4.6 (test harness agent)
**Scope**: Full test infrastructure, coverage gaps, false positives, SLT status

## 1. Test Suite Results

### Build & Run
```
cargo test --features write-sqlite
```

**Summary**: Build succeeds. 40 test failures across 9 test binaries.

| Test Binary | Passed | Failed | Root Cause |
|---|---|---|---|
| Unit tests (src/) | 405 | 0 | -- |
| adversarial_*_tests (5 files) | 242 | 0 | -- |
| alter_table_tests | 24 | 0 | -- |
| compaction_tests | 10 | 0 | -- |
| concurrent_tests | 6 | 0 | -- |
| concurrent_write_tests | 7 | 0 | -- |
| conflict_detection_tests | 15 | 0 | -- |
| create_schema_tests | ~10 | 0 | -- |
| delete_tests | 6 | 0 | -- |
| delete_filter_tests | 11 | 0 | -- |
| deep_edge_case_tests | ~15 | 0 | -- |
| drop_and_constraints_tests | ~5 | 0 | -- |
| edge_case_tests | ~10 | 0 | -- |
| encryption_tests | 3 | 0 | -- |
| file_pruning_tests | 9 | 0 | -- |
| information_schema_test | 17 | 0 | -- |
| issue_repro_*_tests (6 files) | ~45 | 0 | -- |
| merge_tests | 5 | 0 | -- |
| renamed_columns_tests | 7 | 0 | -- |
| sql_dml_tests | 10 | 0 | -- |
| sql_write_tests | 7 | 0 | -- |
| stats_tests | 7 | 0 | -- |
| table_changes_tests | 13 | 0 | -- |
| table_function_tests | 10 | 0 | -- |
| table_tests | 5 | 0 | -- |
| time_travel_tests | 14 | 0 | -- |
| update_tests | 6 | 0 | -- |
| view_tests | 7 | 0 | -- |
| virtual_column_tests | ~5 | 0 | -- |
| virtual_column_extended_tests | ~16 | 0 | -- |
| write_partition_tests | 6 | 0 | -- |
| **write_tests** | **15** | **1** | **Column removal regression** |
| **cross_engine_alter_tests** | **13** | **3** | **Parquet footer length** |
| **cross_engine_ddl_tests** | **25** | **1** | **Parquet footer length** |
| **cross_engine_dml_tests** | **6** | **13** | **Parquet footer length** |
| **cross_engine_feature_tests** | **10** | **4** | **Parquet footer length** |
| **cross_engine_inline_tests** | **8** | **1** | **Parquet footer length** |
| **cross_engine_insert_tests** | **8** | **6** | **Parquet footer length** |
| **cross_engine_partition_tests** | **2** | **5** | **Parquet footer length** |
| **cross_engine_tests** | **24** | **6** | **Parquet footer length** |
| **parity_tests** | **7** | **1** | **Type confusion (float formatting)** |

**Totals**: ~820+ integration tests passed, 40 failed. 405 unit tests passed, 0 failed.

### SLT Results
```
cargo test --features write-sqlite --test sqllogictest_runner
```
**153 passed, 123 failed** (55.4% pass rate, down from 61.8% reported in R9)

---

## 2. Failure Analysis

### R10-F-001: Parquet footer length mismatch (39 cross-engine failures) -- Priority: P0

**Pattern**: All 39 cross-engine failures have the same error:
```
DuckDBFailure: "Parquet footer length stored in file is not equal to footer length provided"
```

**Root cause**: When DataFusion writes Parquet files, the `footer_size` metadata stored in the DuckLake catalog doesn't match what DuckDB expects. This only affects the DF-writes-then-DuckDB-reads direction. DuckDB-writes-then-DF-reads tests pass.

**Affected tests**: All tests in `cross_engine_*` where DF writes data that DuckDB subsequently reads. This accounts for 39 of 40 total failures.

**Impact**: The DF write path produces Parquet files that DuckDB cannot read, breaking the core interoperability promise.

### R10-F-002: `test_append_remove_column` failure -- Priority: P1

**File**: `tests/write_tests.rs`
**Error**: `Removing column should succeed` -- the schema evolution path for column removal during append is broken.

### R10-F-003: `parity_basic_crud_after_insert` type confusion -- Priority: P2

**Error**: `[CRUD after INSERT] Mismatch at row 1, col 2: expected '20', got '20.0'`

DuckDB returns float value `20` without decimal point; DataFusion returns `20.0`. The `normalize_value()` helper in test_utils.rs should handle this, but only normalizes values that already contain `.`. The DuckDB side returns "20" (no decimal) for a float, which bypasses normalization.

---

## 3. Test Coverage Audit

### Well-Covered Areas (adequate-to-strong)

| Area | Tests | Assessment |
|---|---|---|
| INSERT (simple/partitioned) | 16 write + 7 sql_write + 14 cross_engine_insert + 6 partition | Strong |
| DELETE (all patterns) | 6 delete + 11 delete_filter + 4 sql_dml | Strong |
| UPDATE (simple/partitioned) | 6 update + 4 sql_dml | Adequate |
| MERGE (matched/not matched) | 5 merge | Adequate |
| ALTER TABLE (all ops) | 24 alter_table + 16 cross_engine_alter | Strong |
| Views (create/drop/rename) | 7 view | Adequate |
| Time travel | 14 time_travel | Strong |
| CDC (table_changes) | 13 table_changes | Strong |
| Compaction | 10 compaction | Adequate |
| Cross-engine (DuckDB->DF) | 80+ passing cross_engine | Strong |
| Concurrency | 6 concurrent + 7 concurrent_write | Adequate |
| Virtual columns | 5 + 16 extended | Strong |
| Conflict detection | 15 conflict_detection | Strong |
| Path resolution | 118 unit tests | Very strong |
| Type mapping | 56 unit tests | Very strong |
| Metadata writer validation | 47 unit tests | Very strong |
| Adversarial/edge cases | 242 adversarial tests | Very strong |

### Coverage Gaps

#### R10-G-001: Nested types in DF-side writes -- Priority: P2
No tests for writing struct, list, or map types via DataFusion's write path. `write_tests.rs` and `sql_write_tests.rs` only test primitive types. The CLAUDE.md notes "Complex types (nested lists, structs, maps) have limited schema evolution support" -- but basic write support for these types is untested.

#### R10-G-002: Cross-engine DF->DuckDB entirely broken -- Priority: P0
Due to R10-F-001, the entire DF->DuckDB write interoperability path has no passing tests. 39 tests fail with the Parquet footer issue.

#### R10-G-003: TRUNCATE TABLE coverage thin -- Priority: P3
Only 1 test file references TRUNCATE. This is a valid DML operation.

#### R10-G-004: Streaming INSERT edge cases -- Priority: P3
Only 3 streaming tests in `write_tests.rs` (basic, custom path, empty). No tests for streaming with partitioned tables, schema evolution during streaming, or streaming with large batches that cross flush thresholds.

#### R10-G-005: S3/MinIO object store integration -- Priority: P3
Only 2 object_store_integration_tests exist. Both appear to be smoke tests. No Docker-based S3/MinIO integration tests for the full write path.

---

## 4. False Positive Risk Analysis

### R10-FP-001: Low -- Test infrastructure is well-designed

The test infrastructure uses strong verification patterns:
- **`assert_results_eq()`**: Checks row count, column count, AND cell values with float normalization
- **`assert_results_eq_strict()`**: Same but without float normalization for type confusion detection
- **`batches_to_sorted_strings()`**: Deterministic comparison by sorting results
- **Virtual column filtering**: `batches_to_strings_filtered()` automatically strips virtual columns
- **DuckDbConn wrapper**: Consistent DuckDB query helper with proper error propagation

### R10-FP-002: Float normalization gap in parity_tests -- Priority: P2

The `normalize_value()` function only normalizes values containing `.` or `e`/`E`. DuckDB may return integer-formatted floats (e.g., `"20"` for `20.0`), which bypasses normalization. This causes `parity_basic_crud_after_insert` to fail. The normalization approach is sound for its stated purpose (detecting type confusion), but the parity test should use a different comparison strategy for cross-engine results.

### R10-FP-003: Tests using only `.contains()` assertions -- Priority: P3

12 tests in `adversarial_pattern_tests_1.rs` use `.contains()` assertions. While appropriate for error message validation (the primary use case), loose string matching can mask regressions if error messages change. These are low risk since they're supplementary assertions alongside stronger checks.

### R10-FP-004: Heavy `unwrap()` usage in test code -- Priority: P3

Test files average 50-130 `unwrap()` calls each. While panicking on error is acceptable in test code, it can produce unhelpful stack traces. The test infrastructure's custom `DuckDbConn` methods already include context in panic messages. Consider extending this pattern to other common operations.

---

## 5. Test Infrastructure Quality

### Strengths
- **Modular helpers**: `create_test_env()`, `create_writable_context()`, `create_read_context()` patterns are consistent across test files
- **Temporary directory isolation**: All tests use `TempDir` for catalog and data files -- full isolation
- **Cross-engine comparison framework**: `DuckDbConn` + `df_query()` + `assert_results_eq()` is a solid triple for cross-engine testing
- **Thread safety**: `Once::call_once` for DuckLake extension installation
- **Value conversion**: Comprehensive `arrow_value_to_string()` and `duckdb_value_to_string()` cover all common types including timestamps and decimals

### Weaknesses
- **Test helper duplication**: `create_test_env()` and similar helpers are re-implemented in multiple test files (`delete_tests.rs`, `update_tests.rs`, `merge_tests.rs`, `sql_dml_tests.rs`) with slight variations. A shared setup module would reduce duplication.
- **`common/mod.rs` gated on `metadata-duckdb`**: The common test utilities require the DuckDB feature. Tests can't use them with `--no-default-features --features write-sqlite`, making SQLite-only testing impossible. This was hit during initial build attempts.
- **No test categorization**: No `#[cfg_attr(feature = "slow", ignore)]` or similar markers. All tests run in the same pass.

---

## 6. SLT (SQL Logic Test) Status

**153 passed / 276 total = 55.4%** (down from 61.8% in earlier reports)

The SLT runner uses a hybrid approach where DuckDB handles writes and DataFusion handles reads. The 123 failures likely cluster around:
- Features not yet implemented in DataFusion (e.g., certain SQL syntax)
- DuckDB-specific functions/syntax
- Concurrent/transaction-related tests
- Checkpoint/compaction operations that delegate to DuckDB

The SLT pass rate declining may indicate regressions from recent refactoring or new test files being added.

---

## 7. Findings Summary

| ID | Title | Priority | Effort |
|---|---|---|---|
| R10-F-001 | Parquet footer length mismatch (39 failures) | P0 | L |
| R10-F-002 | `test_append_remove_column` column removal broken | P1 | S |
| R10-F-003 | Float formatting type confusion in parity test | P2 | S |
| R10-G-001 | No nested type write tests | P2 | M |
| R10-G-002 | DF->DuckDB interop entirely broken | P0 | L |
| R10-G-003 | TRUNCATE TABLE coverage thin | P3 | S |
| R10-G-004 | Streaming INSERT edge cases missing | P3 | M |
| R10-G-005 | S3/MinIO object store untested | P3 | L |
| R10-FP-002 | Float normalization gap | P2 | S |
| R10-FP-003 | Loose `.contains()` assertions | P3 | S |
| R10-FP-004 | Heavy `unwrap()` without context | P3 | M |

**Overall assessment**: The test infrastructure is mature and well-designed. The primary issue is R10-F-001 (Parquet footer length) which causes 39 of 40 total failures and blocks the entire DF->DuckDB write interoperability path. All DF-internal tests pass. The one non-footer failure (`test_append_remove_column`) represents a genuine column removal regression.

**Test count**: 67 test files, ~820+ integration tests, 405 unit tests = **1,225+ total tests**.
