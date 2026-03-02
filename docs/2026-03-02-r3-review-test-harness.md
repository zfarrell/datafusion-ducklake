# R3 Test Harness Review — 2026-03-02

## Scope

Review cycle 3 of test infrastructure: verify previous fixes, identify new false-positive
risks, assess coverage gaps, and evaluate test helper consistency.

---

## 1. Verification of Previous Fixes

### 1.1 `tests/sql_write_tests.rs` — Error Handling

**Status: PARTIALLY FIXED — one remaining false-positive path**

Previous fix ensured tests use `.expect()` and `panic!()` on error paths. Review findings:

- `test_insert_into_existing_table` (line 176–188): Correctly panics on error with `.expect("INSERT INTO should succeed")` and then checks the count. **Good.**
- `test_insert_into_read_only_fails` (line 256–288): **FALSE POSITIVE RISK REMAINS.**
  The `Ok(_)` arm at line 269–272 silently passes when `df.collect()` succeeds:
  ```rust
  Ok(_) => {
      // If insert_into is not implemented, it might just return empty
      // This is acceptable behavior during development
  },
  ```
  This means if the read-only guard is completely broken and inserts succeed,
  the test passes silently. The comment says "acceptable during development" but
  this is a production test — it should `panic!("Expected error for read-only insert, but it succeeded")`.

- `test_create_table_as_select` (line 67): Correctly `#[ignore]`d with reason. **Good.**
- `test_insert_overwrite` (line 290): Correctly `#[ignore]`d with reason. **Good.**
- `test_sql_insert_values` (line 370): Correctly `#[ignore]`d with reason. **Good.**
- `test_schema_evolution_via_sql` (line 436): Correctly `#[ignore]`d with reason. **Good.**
- `test_insert_from_query_with_filter` (line 526): Correctly `#[ignore]`d with reason. **Good.**

**Finding F-TH-001 (Medium)**: `test_insert_into_read_only_fails` has a silent success path
that should panic. If the write guard is ever removed or broken, this test will pass silently.

### 1.2 `tests/roundtrip_interop_tests.rs` — #[ignore] Annotations

**Status: FIXED — all properly annotated**

All 5 roundtrip tests have `#[ignore = "requires DuckDB CLI — run with: cargo test -- --ignored"]`:
- `test_datafusion_writes_duckdb_reads` (line 130)
- `test_datafusion_writes_duckdb_reads_count` (line 196)
- `test_duckdb_writes_datafusion_reads` (line 236)
- `test_schema_evolution_roundtrip` (line 302)
- `test_full_bidirectional_roundtrip` (line 417)
- `test_catalog_metadata_diagnostic` (line 509)

All assertions are strong — exact row counts, specific value comparisons, `panic!` on
DuckDB CLI failures with detailed diagnostics. **No false-positive risk.**

### 1.3 `tests/sqllogictest_runner.rs` — Failure Propagation

**Status: FIXED — properly fails on SLT errors**

- `run_hybrid_test()` (line 802–830): Uses `await??` (double `?`) — propagates both
  `JoinError` from `spawn_blocking` and `sqllogictest::Error` from `runner.run_file()`.
- Build.rs-generated tests (line 841, `include!`) call `run_hybrid_test(...).await.unwrap_or_else(|e| panic!(...))`, which ensures test failures are fatal.
- The preprocessor correctly emits `halt` for incompatible tests rather than silently
  converting them to passing tests.

**No false-positive risk in the SLT runner itself.**

### 1.4 `build.rs` — Individual SLT Test Generation

**Status: FIXED — correct and robust**

- Properly discovers `.test` files recursively via `find_test_files()` (line 48–59)
- Generates `#[tokio::test]` for each file (line 28)
- Test body uses `unwrap_or_else(|e| panic!(...))` with file path in error message (line 32–41)
- `path_to_test_name()` correctly sanitizes paths for Rust identifiers (line 61–68)
- `cargo:rerun-if-changed` directive ensures tests regenerate when `.test` files change

---

## 2. New False Positive Risks

### 2.1 `batches_to_strings` Semantic Divergence

**Finding F-TH-002 (High)**: Two semantically different versions of `batches_to_strings` exist:

1. **`tests/common/test_utils.rs:185`** — Filters out virtual columns (rowid, snapshot_id, filename, file_row_number, file_index). This is the shared version.
2. **Local copies in ~6 files** (cross_engine_dml_tests.rs:234, cross_engine_feature_tests.rs:163, cross_engine_ddl_tests.rs:260, merge_tests.rs:108, compaction_tests.rs inlined in df_query) — **Do NOT filter virtual columns.**

This divergence means:
- Tests using the shared version get virtual columns stripped automatically
- Tests using local copies include virtual columns in results
- If a test imports `batches_to_strings` from test_utils but expected results don't account
  for virtual column filtering, comparisons will fail or succeed for the wrong reasons
- Some files import `arrow_value_to_string` from test_utils but define their own
  `batches_to_strings` locally — this is an inconsistent partial migration

**Risk**: When tests are migrated to use the shared helper, column counts will change,
potentially exposing hidden assertion failures or causing unexpected passes.

### 2.2 `convert_batch_to_strings` in `hybrid_asyncdb.rs` — Third Variant

**Finding F-TH-003 (Low)**: `hybrid_asyncdb.rs:499` has its own `convert_batch_to_strings()`
with SLT-specific formatting (format_float for DuckDB compatibility). This is intentionally
different from test_utils but represents a third implementation that could drift.

### 2.3 `statement error` → `statement ok` Conversion

**Finding F-TH-004 (Medium)**: The SLT preprocessor converts `statement error` to `statement ok`
when `is_hybrid_incompatible_error()` matches (line 362). The conversion patterns are:
- `READ-ONLY` / `READ ONLY` — correct, hybrid always has writable DuckDB
- `DOES NOT EXIST!` — correct, DETACH is skipped
- `MISSING EXTENSION ERROR` — correct, parquet is always loaded
- `COULD NOT LOAD THE COPY FUNCTION` — correct
- `TRANSACTION-LOCAL INLINED DATA` — less certain, depends on DuckDB version

The `eprintln!` logging on conversion (line 737) is good for visibility, but these messages
go to stderr and are not captured by the test framework's `--nocapture` by default. **Consider
using `log::warn!` or accumulating conversion counts for a summary at test end.**

---

## 3. Coverage Gaps

### 3.1 Missing Test Coverage

**Finding F-TH-005 (Medium)**: Operations with no test coverage:

1. **DROP TABLE / DROP SCHEMA via SQL** — `drop_and_constraints_tests.rs` exists but only tests
   constraints; no dedicated DROP TABLE SQL test that verifies the table is actually gone
2. **Concurrent writes with conflict detection** — `concurrent_write_tests.rs` exists but
   `concurrent_tests.rs` only tests read concurrency
3. **DELETE filtering with multiple delete files per data file** — tests only have single
   delete file scenarios
4. **Schema evolution reading** — old files read with new schema (null backfill) after
   ALTER TABLE ADD COLUMN. The roundtrip test (line 302) only tests via DuckDB CLI, not
   DataFusion reading directly
5. **Large file counts** — no test with >100 files to verify performance characteristics
6. **SLT preprocessing edge cases** — no unit tests for `preprocess_test_file()`, only
   integration-level SLT execution tests

### 3.2 `sql_write_tests.rs` All Ignored Tests

**Finding F-TH-006 (Low)**: 5 of 7 tests in `sql_write_tests.rs` are `#[ignore]`d:
- `test_create_table_as_select` — CTAS virtual column mismatch
- `test_insert_overwrite` — column count mismatch
- `test_sql_insert_values` — data length mismatch
- `test_schema_evolution_via_sql` — field not found
- `test_insert_from_query_with_filter` — column count mismatch

All share the root cause of **virtual columns causing schema mismatch**. These are well-
documented but represent a significant gap in SQL write path testing. The only non-ignored
write tests are `test_insert_into_existing_table` and `test_insert_into_read_only_fails`.

---

## 4. Test Helper Consistency

### 4.1 Helper Duplication Summary

**Finding F-TH-007 (Medium)**: Significant helper duplication across test files:

| Helper | Shared (test_utils.rs) | Duplicated In |
|--------|----------------------|---------------|
| `df_query()` | Yes | cross_engine_ddl_tests, cross_engine_dml_tests, cross_engine_feature_tests, cross_engine_tests, cross_engine_alter_tests, cross_engine_insert_tests, cross_engine_postgres_tests, cross_engine_mysql_tests, compaction_tests |
| `batches_to_strings()` | Yes (filters virtuals) | cross_engine_dml_tests, cross_engine_feature_tests, cross_engine_ddl_tests, merge_tests (no virtual filtering) |
| `assert_results_eq()` | Yes | cross_engine_dml_tests (slightly different error msgs) |
| `get_int_column()` | **No** | delete_filter_tests, concurrent_tests, renamed_columns_tests |
| `duckdb_value_to_string()` | Yes | Used by ~12 files via import (**good**) |
| `arrow_value_to_string()` | Yes | Used by ~12 files via import (**good**) |

**Root cause**: test_utils.rs was introduced after many cross-engine tests were already written.
The migration is incomplete — files import the value converters but keep local versions of
higher-level helpers.

### 4.2 `get_int_column()` — Missing from test_utils

**Finding F-TH-008 (Low)**: `get_int_column()` is duplicated identically in 3 test files
but not available in test_utils.rs. Should be added there and shared.

### 4.3 Feature Gate Mismatch Risk

**Finding F-TH-009 (Low)**: `tests/common/mod.rs` is gated on `#[cfg(feature = "metadata-duckdb")]`
only, but `test_utils.rs` inside it imports from `duckdb::types::Value`. Files that use
`test_utils` with `metadata-sqlite` only (without `metadata-duckdb`) would fail to compile.
Currently all cross-engine tests require both features, so this isn't triggered, but it's
a latent issue if test_utils is used more broadly.

---

## 5. SLT Adapter Analysis

### 5.1 Routing Logic (`hybrid_asyncdb.rs`)

**Finding F-TH-010 (Low)**: The `is_write_statement()` function (line 117–143) routes statements
by prefix matching. Potential misroutes:

- `COPY ... TO ...` is correctly routed to DuckDB (line 134)
- `WITH ... INSERT ...` is NOT caught — a CTE-based insert starting with `WITH` would be
  routed to DataFusion instead of DuckDB. However, this is unlikely in SLT tests.
- `SELECT INTO` (if supported) would be routed to DataFusion, not DuckDB. Also unlikely.

### 5.2 Table Reference Rewriting

The 3-part name detection in `is_three_part_ref()` (line 178–196) is heuristic-based. It
checks if the text after `ducklake.` contains `identifier.identifier`. Edge cases:

- `ducklake.123abc` — starts with digit, wouldn't be detected as 3-part, would get `main.`
  inserted. This is correct behavior (not a valid schema name).
- `ducklake.s1.` (trailing dot, no table) — would be detected as 3-part. Benign since the
  SQL would fail anyway.

### 5.3 Virtual Column Stripping in execute_read

The `execute_read()` method (line 263–317) strips virtual columns from results unless
explicitly referenced in the SQL. The check is substring-based:
```rust
sql_upper.contains(&name.to_uppercase())
```
This could have false positives if a column name appears in a comment or string literal,
but for SLT tests this is unlikely to be an issue.

### 5.4 Transaction Handling

**Finding F-TH-011 (Low)**: When `in_transaction` is true, reads are routed to DuckDB
(line 378–428). The DuckDB value formatting is independent of `convert_batch_to_strings()`
and could have formatting differences (e.g., timestamp precision, decimal formatting).
This is a potential source of subtle SLT mismatches during transactional tests.

---

## 6. Build Verification

The build environment had corrupted dependency artifacts requiring a `cargo clean` and
full rebuild. Build was initiated from a clean state. The `--features write-sqlite` flag
activates the SQL write tests and the cross-engine tests that depend on both SQLite writer
and DuckDB metadata provider.

### Test Run Results (--features write-sqlite)

| Binary | Passed | Failed | Ignored |
|--------|--------|--------|---------|
| Unit tests (lib) | 55 | 0 | 0 |
| Integration batch 1 | 299 | 0 | 0 |
| Integration batch 2 | 45 | 0 | 0 |
| Integration batch 3 | 41 | 0 | 0 |
| Integration batch 4 | 36 | 0 | 0 |
| adversarial_storage_tests | 48 | **1** | 0 |
| **Total** | **524** | **1** | **0** |

**One failure**: `test_double_slash_in_various_positions` in `adversarial_storage_tests.rs:173`
```
assertion `left == right` failed
  left: "/data/schema/table/file.parquet"
 right: "/data/schema///table///file.parquet"
```
This appears to be a test that expects path normalization to preserve double slashes,
but the code now normalizes them away. The test expectation needs updating to match
the current path resolution behavior (or the path resolver needs to preserve double
slashes — which is unusual).

---

## 7. Summary of Findings

| ID | Severity | Description |
|----|----------|-------------|
| F-TH-001 | Medium | `test_insert_into_read_only_fails` silent success path |
| F-TH-002 | High | `batches_to_strings` semantic divergence (virtual column filtering) |
| F-TH-003 | Low | Third `convert_batch_to_strings` variant in hybrid_asyncdb.rs |
| F-TH-004 | Medium | `statement error` → `statement ok` conversion visibility |
| F-TH-005 | Medium | Missing test coverage for DROP TABLE, multi-delete-file, schema evolution read |
| F-TH-006 | Low | 5/7 sql_write_tests are #[ignore]d due to virtual column issues |
| F-TH-007 | Medium | Incomplete migration to shared test helpers (~10 files with local copies) |
| F-TH-008 | Low | `get_int_column()` duplicated in 3 files, missing from test_utils |
| F-TH-009 | Low | test_utils feature gate could break if used without metadata-duckdb |
| F-TH-010 | Low | `WITH ... INSERT` not caught by write statement detection |
| F-TH-011 | Low | Transaction-mode DuckDB formatting may differ from DataFusion formatting |
| F-TH-012 | Medium | `test_double_slash_in_various_positions` fails — test/code mismatch on path normalization |

### Priority Recommendations

1. **Fix F-TH-001**: Change the `Ok(_)` arm to `panic!("Expected error for read-only insert")`
2. **Fix F-TH-002**: Either standardize all local `batches_to_strings` to use the shared
   version (with virtual column filtering), or create two explicit variants (`batches_to_strings_raw`
   and `batches_to_strings_filtered`) in test_utils
3. **Fix F-TH-007**: Complete the migration to shared helpers for `df_query()` and `assert_results_eq()`
4. **Fix F-TH-008**: Add `get_int_column()` to test_utils.rs and update the 3 files
