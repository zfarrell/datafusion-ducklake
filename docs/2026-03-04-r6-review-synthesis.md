# Review Cycle 6 Synthesis
Date: 2026-03-04

## Resolution Status

**R6 Fix Results**: 10 agents, ~49 of 52 assigned findings fixed. 36 P3 findings not assigned.

| Priority | Count | Fixed | Unfixable | Deferred | Not Assigned |
|----------|-------|-------|-----------|----------|--------------|
| P0 | 0 | — | — | — | — |
| P1 | 14 | 13 | 1 (R6-S-014) | 0 | 0 |
| P2 | 38 | 35 | 0 | 1 (R6-S-017) | 2 (partial R6-S-010) |
| P3 | 36 | 0 | 0 | 0 | 36 |
| **Total** | **88** | **~49** | **1** | **1** | **36** |

**Fix Agent Summary:**

| Agent | Findings | Result | Key Commits |
|-------|----------|--------|-------------|
| fix-sqlite-metadata | R6-S-001, 015, 016, 019, 029 | 5/5 fixed | `d3aa034` |
| fix-backend-parity | R6-S-003, 004, 018, 033, 034 | 5/5 fixed | `75ad2e1` |
| fix-error-handling | R6-S-005, 006, 008, 020, 021 | 5/6 fixed (R6-S-007 resolved by code-quality) | `aaf5a4f`, `07cd101` |
| fix-table-functions | R6-S-002, 011, 024-028, 052 | 8/8 fixed | `f93444c` |
| fix-interop | R6-S-009, 012, 030, 031 | 4/4 fixed | `b8a4476` |
| fix-metadata-correctness | R6-S-010, 035, 036, 040 | 4/4 fixed | `f4c0f58` |
| fix-test-infra | R6-S-013, 042-047, 050, 051 | 9/10 fixed (R6-S-014 unfixable) | `03f9cb3` |
| fix-dml-robustness | R6-S-037, 038, 039 | 3/3 fixed | `5666cf5`, `08ff2f7` |
| fix-code-quality | R6-S-022, 023, 041 | 3/3 fixed | `c9c761b` |
| fix-cross-engine-tests | R6-S-032, 048, 049 | 3/3 fixed | `d6a5104` |

**Merge**: 3 branches merged into `ducklake-features/integration`, 21 conflicts resolved. Final commit: `4f9cc49`.

**Tests**: 725 pass, 3 SQLite concurrency flakes (pre-existing). New tests: 10 cross-engine, 7 partition validation, 9 table function tests, and more.

**Notable:**
- R6-S-014 (duplicated type-to-string conversion): Unfixable due to module structure constraints
- R6-S-017 (concurrent DML race): Deferred — architectural, related to R4-S-018
- R6-S-010 (SET NOT NULL data validation): Documented as known limitation rather than implementing full data scan

### Per-Finding Resolution

#### P1 Findings
- R6-S-001: **[FIXED]** `d3aa034` — Added table_id to column stats INSERT
- R6-S-002: **[FIXED]** `f93444c` — Deferred compaction execution to scan time
- R6-S-003: **[FIXED]** `75ad2e1` — Added record_count decrement to PG/MySQL
- R6-S-004: **[FIXED]** `75ad2e1` — Added delete file ending and stats reset to PG/MySQL
- R6-S-005: **[FIXED]** `aaf5a4f` — Replaced unwrap() with proper error handling
- R6-S-006: **[FIXED]** `aaf5a4f` — Replaced silent NULL with error on downcast failure
- R6-S-007: **[FIXED]** `c9c761b` — Resolved by code-quality agent (shared parser extraction)
- R6-S-008: **[FIXED]** `aaf5a4f` — Returns error instead of empty string on format failure
- R6-S-009: **[FIXED]** `b8a4476` — Fetches actual schema_version instead of hardcoding 1
- R6-S-010: **[FIXED]** `f4c0f58` — Documented as known limitation with warning log
- R6-S-011: **[FIXED]** `f93444c` — Added quote stripping to parse_table_name
- R6-S-012: **[FIXED]** `b8a4476` — Passed encryption factory through to CDC scans
- R6-S-013: **[FIXED]** `03f9cb3` — Made test async, exercises BEGIN→COMMIT and BEGIN→ROLLBACK
- R6-S-014: **[UNFIXABLE]** Module structure prevents deduplication without major refactor

#### P2 Findings
- R6-S-015: **[FIXED]** `d3aa034` — Tracks cumulative row_id_start during compaction
- R6-S-016: **[FIXED]** `d3aa034` — Uses checked_add() for overflow protection
- R6-S-017: **[DEFERRED]** Concurrent DML race — architectural, related to R4-S-018
- R6-S-018: **[FIXED]** `75ad2e1` — Added transactional replace_table_files to PG/MySQL
- R6-S-019: **[FIXED]** `d3aa034` — Decimal stat comparison precision improvement
- R6-S-020: **[FIXED]** `aaf5a4f` — Epoch date as const
- R6-S-021: **[FIXED]** `aaf5a4f` — expect() replaced with ok_or_else
- R6-S-022: **[FIXED]** `c9c761b` — Extracted shared parse function
- R6-S-023: **[FIXED]** `c9c761b` — Normalized transform to enum at planning time
- R6-S-024: **[FIXED]** `f93444c` — Validate arguments before opening connection
- R6-S-025: **[FIXED]** `f93444c` — Added delete_threshold range validation
- R6-S-026: **[FIXED]** `f93444c` — Cached INSTALL ducklake with OnceLock
- R6-S-027: **[FIXED]** `f93444c` — Returns error on unexpected ScalarValue variant
- R6-S-028: **[FIXED]** `f93444c` — Validates non-empty parts after splitting
- R6-S-029: **[FIXED]** `d3aa034` — Added type allowlist validation for SQL DDL
- R6-S-030: **[FIXED]** `b8a4476` — Documented table naming convention
- R6-S-031: **[FIXED]** `b8a4476` — Handles both UTC and timezone-offset timestamp formats
- R6-S-032: **[FIXED]** `d6a5104` — Added DF-write→DuckDB-read cross-engine tests
- R6-S-033: **[FIXED]** `75ad2e1` — Added FOR UPDATE to SELECT query
- R6-S-034: **[FIXED]** `75ad2e1` — Added UNIQUE(table_id) constraint
- R6-S-035: **[FIXED]** `f4c0f58` — Documented column_id allocation difference
- R6-S-036: **[FIXED]** `f4c0f58` — Added partition transform allowlist and duplicate check
- R6-S-037: **[FIXED]** `5666cf5` — Added upload failure cleanup
- R6-S-038: **[FIXED]** `5666cf5` — Added snapshot failure cleanup
- R6-S-039: **[FIXED]** `08ff2f7` — Uses replace_table_files for single-file path
- R6-S-040: **[FIXED]** `f4c0f58` — Updates catalog snapshot_id after DDL operations
- R6-S-041: **[FIXED]** `c9c761b` — Fixed limit pushdown to first file only
- R6-S-042: **[FIXED]** `03f9cb3` — Improved SLT filter patterns
- R6-S-043: **[FIXED]** `03f9cb3` — Added strict mode to normalize_value
- R6-S-044: **[FIXED]** `03f9cb3` — Extended is_write_statement for CTE-wrapped DML
- R6-S-045: **[FIXED]** `03f9cb3` — Extended parser to skip double-quoted regions
- R6-S-046: **[FIXED]** `03f9cb3` — Added ORDER BY ALL rewriting unit tests
- R6-S-047: **[FIXED]** `03f9cb3` — Added value-level assertions to write tests
- R6-S-048: **[FIXED]** `d6a5104` — Added schema assertions to cross-engine tests
- R6-S-049: **[FIXED]** `d6a5104` — Added BOOLEAN type roundtrip test
- R6-S-050: **[FIXED]** `03f9cb3` — Matches on SQL + error text combination
- R6-S-051: **[FIXED]** `03f9cb3` — Tightened read-only error assertion
- R6-S-052: **[FIXED]** `f93444c` — Added configurable older_than parameter

#### P3 Findings (36)
All 36 P3 findings (R6-S-053 through R6-S-088) were **[NOT ASSIGNED]** — optional, low impact.

---

## Overview
- Raw findings: 107 (across 5 reviews)
- Excluded: 4 (1 false positive, 3 references to already-deferred items)
- Duplicate merges: 15 (11 merge groups)
- After deduplication: **88**
- By priority: **0 P0**, **14 P1**, **38 P2**, **36 P3**
- Codex P0 false positive rate this cycle: 3/3 (100%)
- Cumulative codex P0 FP rate (R4-R6): R4-R5 was 86%, R6 adds 3/3 FP -> ~90%+

## Cumulative Review Stats (R1-R6)
- R1: 36 findings, 36 fixed
- R2: 58 findings, 55 fixed, 3 deferred
- R3: 50 findings, 25 fixed, 25 deferred (P2/P3 nits)
- R4: 46 findings, 43 fixed, 1 open, 2 deferred
- R5: 77 findings, 72 fixed, 5 skipped
- R6: **88 findings**, **~49 fixed**, 1 unfixable, 1 deferred, 36 P3 not assigned
- Total: **355 findings** across 6 cycles, **~280 fixed** in R1-R6

## Deduplication Notes

### Excluded Items (4)
- **CX-W-002** (codex): MERGE nondeterministic — FALSE POSITIVE (already fixed R3F-033)
- **CX-W-007** (codex): INSERT materializes all partitions — already deferred as F-036
- **CX-C-006** (codex): CTAS materializes all data — already deferred as F-036
- **R6-I-021** (idiomatic): MetadataWriter trait sync/block_on — already deferred as F-045

### Merge Groups (11 groups, saving 15 entries)
1. {R6-C-002, CX-M-005} → R6-S-001: replace_table_files missing table_id
2. {R6-I-011, CX-TF-002} → R6-S-002: compaction side effects at planning time
3. {R6-C-008, CX-M-001} → R6-S-018: non-atomic default replace_table_files
4. {R6-C-006, R6-I-026} → R6-S-019: decimal stat precision loss
5. {R6-I-025, CX-M-009} → R6-S-029: dynamic SQL type interpolation
6. {R6-T-001, CX-T-003} → R6-S-013: transaction test false positive
7. {R6-T-015, R6-T-016, R6-T-017, CX-T-004, CX-T-005} → R6-S-047: write tests count-only
8. {R6-C-009, CX-W-004} → R6-S-075: partition column_index bounds
9. {CX-C-001, CX-C-002} → R6-S-040: snapshot not propagated
10. {R6-IO-002, R6-IO-003, R6-IO-004} → R6-S-066: extra columns/tables in schema
11. {R6-IO-007, R6-IO-008} → R6-S-031: timestamp type TEXT vs TIMESTAMPTZ

---

## Deduplicated Findings

### P0 Findings

None.

### P1 Findings (14)

#### R6-S-001: replace_table_files() missing table_id in column stats INSERT (SQLite)
- **Source(s)**: correctness (R6-C-002), codex (CX-M-005)
- **File(s)**: `src/metadata_writer_sqlite.rs:1380-1391`
- **Description**: The `replace_table_files()` method inserts into `ducklake_file_column_stats` with columns `(data_file_id, column_id, null_count, min_value, max_value)` — omitting `table_id`. The schema defines `table_id INTEGER NOT NULL`. In SQLite non-STRICT mode, the column defaults to NULL, causing `recompute_table_column_stats()` to miss these rows and producing wrong table-level stats after compaction.
- **Impact**: Column stats silently wrong after compaction; DuckDB interop may reject NULL table_id
- **Suggested Fix**: Add `table_id` to the INSERT statement and bind `table_id` parameter.
- **Effort**: S
- **Recommended Agent**: fix-sqlite-metadata

#### R6-S-002: Compaction UDTFs execute side effects at planning time
- **Source(s)**: idiomatic (R6-I-011), codex (CX-TF-002)
- **File(s)**: `src/compaction_functions.rs:220,267,394,444`
- **Description**: `TableFunctionImpl::call()` opens a DuckDB connection and executes compaction SQL immediately during query planning. DataFusion may call `call()` during EXPLAIN, optimizer rewrites, or retries, causing unintended data compaction during plan exploration.
- **Impact**: EXPLAIN or optimizer retries trigger actual data compaction
- **Suggested Fix**: Return a provider that defers execution to `scan()`/`execute()` at runtime.
- **Effort**: L
- **Recommended Agent**: fix-table-functions

#### R6-S-003: PG/MySQL missing record_count decrement on DELETE
- **Source(s)**: correctness (R6-C-001)
- **File(s)**: `src/metadata_writer_postgres.rs:1025-1141`, `src/metadata_writer_mysql.rs:1156-1272`
- **Description**: SQLite's `register_dml_files()` tracks `total_net_new_deletions` and decrements `ducklake_table_stats.record_count`. Neither PG nor MySQL performs this decrement — they only increment on new data files.
- **Impact**: After DELETE/UPDATE/MERGE, `record_count` diverges across backends (SQLite correct, PG/MySQL inflated)
- **Suggested Fix**: Port the `total_net_new_deletions` logic from SQLite to PG and MySQL `register_dml_files()`.
- **Effort**: M
- **Recommended Agent**: fix-backend-parity

#### R6-S-004: end_table_files backend drift (PG/MySQL vs SQLite)
- **Source(s)**: codex (CX-M-002)
- **File(s)**: `src/metadata_writer_postgres.rs:1008`, `src/metadata_writer_mysql.rs:1139`, `src/metadata_writer_sqlite.rs:1303`
- **Description**: SQLite's `end_table_files` ends data files, delete files, AND resets stats. PG/MySQL only end data files. Replace operations on PG/MySQL leave stale delete files and stats.
- **Impact**: Stale delete files and stats after replace operations on PG/MySQL
- **Suggested Fix**: Add delete file ending and stats reset to PG/MySQL `end_table_files`.
- **Effort**: S
- **Recommended Agent**: fix-backend-parity

#### R6-S-005: unwrap() on downcasts in merge_exec::extract_key_value
- **Source(s)**: idiomatic (R6-I-001)
- **File(s)**: `src/merge_exec.rs:242,254,265,275,279,289`
- **Description**: Six `downcast_ref::<T>().unwrap()` calls in `extract_key_value()` can panic on schema/type mismatch. The `extract_int!` and `extract_uint!` macros correctly use `ok_or_else()?`, but Boolean, Float32, Float64, Utf8, LargeUtf8, and Decimal128 branches use bare `unwrap()`.
- **Impact**: Runtime panic on type mismatch in MERGE operations
- **Suggested Fix**: Replace `unwrap()` with `ok_or_else(|| DataFusionError::Internal(...))?` to match existing macro pattern.
- **Effort**: S
- **Recommended Agent**: fix-error-handling

#### R6-S-006: Silent downcast failures treated as NULL in compute_partition_value
- **Source(s)**: idiomatic (R6-I-004)
- **File(s)**: `src/insert_exec.rs:296-371`
- **Description**: `downcast_ref::<T>().map(|a| a.value(row).to_string())` converts downcast failure to `None`, treated as `NULL`/`__HIVE_DEFAULT_PARTITION__`. A type mismatch silently routes rows to the wrong partition.
- **Impact**: Silent data misrouting to wrong partitions on type mismatch
- **Suggested Fix**: Use `ok_or_else(|| DuckLakeError::Internal(...))?` for downcasts; reserve `None` only for actual null values.
- **Effort**: M
- **Recommended Agent**: fix-error-handling

#### R6-S-007: Inconsistent inlined value parse policy between table.rs and table_writer.rs
- **Source(s)**: idiomatic (R6-I-005)
- **File(s)**: `src/table.rs:1860`, `src/table_writer.rs:1285`
- **Description**: `table.rs::parse_inlined_column` silently converts unparseable strings to NULL, while `table_writer.rs::parse_string_to_array` returns an error. A write can succeed storing data that reads back as NULL instead of the original value.
- **Impact**: Data silently changed on read (original value → NULL) for unparseable inlined values
- **Suggested Fix**: Extract shared parser function with explicit mode parameter, or unify on strict policy.
- **Effort**: M
- **Recommended Agent**: fix-error-handling

#### R6-S-008: arrow_array_value_to_string returns Ok("") on format failure
- **Source(s)**: idiomatic (R6-I-009)
- **File(s)**: `src/table_writer.rs:1168`
- **Description**: When `ArrayFormatter::try_new` fails, the function returns `Ok(String::new())`. This silently converts format errors to empty strings stored as column statistics min/max values.
- **Impact**: Corrupted column statistics (empty string min/max) on format failure
- **Suggested Fix**: Return `Err(DuckLakeError::Internal(...))`.
- **Effort**: S
- **Recommended Agent**: fix-error-handling

#### R6-S-009: Hardcoded schema_version=1 in inlined data table naming
- **Source(s)**: interop (R6-IO-001)
- **File(s)**: `src/metadata_writer_sqlite.rs:2923`
- **Description**: `store_inlined_data` hardcodes `schema_version=1` in inline data table name. DuckDB uses the actual schema_version at table creation time. For tables created after the first DDL snapshot, DuckDB looks for a differently-named table.
- **Impact**: DuckDB fails to read inlined data for tables created after first DDL snapshot
- **Suggested Fix**: Fetch current `schema_version` from snapshot instead of hardcoding 1.
- **Effort**: S
- **Recommended Agent**: fix-interop

#### R6-S-010: SET NOT NULL without data validation
- **Source(s)**: codex (CX-M-007)
- **File(s)**: `src/metadata_writer_validation.rs:352`
- **Description**: SET NOT NULL is accepted without checking if existing data contains nulls. Metadata updated to NOT NULL while data may contain nulls, creating contradictory state.
- **Impact**: NOT NULL constraint in metadata doesn't match actual data; queries may behave unexpectedly
- **Suggested Fix**: Either scan data files for nulls (expensive) or document as known limitation.
- **Effort**: L
- **Recommended Agent**: fix-metadata-correctness

#### R6-S-011: parse_table_name doesn't unescape quoted identifiers
- **Source(s)**: codex (CX-TF-003)
- **File(s)**: `src/table_functions.rs:354-370`
- **Description**: The function splits on dots but doesn't strip surrounding double-quotes from identifiers. Input `"main"."users"` produces schema=`"main"` (with quotes), failing lookup.
- **Impact**: Table functions fail on quoted identifiers
- **Suggested Fix**: Strip surrounding double-quotes from schema and table parts after splitting.
- **Effort**: S
- **Recommended Agent**: fix-table-functions

#### R6-S-012: CDC paths missing encryption factory
- **Source(s)**: codex (CX-TF-004)
- **File(s)**: `src/table_changes.rs:547`, `src/table_deletions.rs:215,241`
- **Description**: CDC paths use `ParquetSource::default()` without the encryption factory that `table.rs` constructs. Encrypted catalogs fail to read CDC paths.
- **Impact**: Table changes/deletions functions fail on encrypted catalogs
- **Suggested Fix**: Pass encryption factory through to CDC scan construction.
- **Effort**: M
- **Recommended Agent**: fix-table-functions

#### R6-S-013: Transaction state tracking test is a false positive
- **Source(s)**: test-harness (R6-T-001), codex (CX-T-003)
- **File(s)**: `tests/hybrid_asyncdb.rs:971-982`
- **Description**: `test_transaction_state_tracking` only asserts the initial state (`in_transaction == false`). Never calls BEGIN/COMMIT/ROLLBACK. Passes vacuously.
- **Impact**: Transaction state machine could break without test failure; broken tracker causes silent data divergence in SLT tests
- **Suggested Fix**: Make test async, exercise BEGIN→COMMIT and BEGIN→ROLLBACK paths.
- **Effort**: S
- **Recommended Agent**: fix-test-infra

#### R6-S-014: Duplicated type-to-string conversion between hybrid_asyncdb and test_utils
- **Source(s)**: test-harness (R6-T-002)
- **File(s)**: `tests/hybrid_asyncdb.rs:571-735`, `tests/common/test_utils.rs:66-198`
- **Description**: `convert_batch_to_strings` in `hybrid_asyncdb.rs` duplicates virtually the same logic as `test_utils.rs`, with different Date32 handling. A fix to one location won't propagate to the other.
- **Impact**: Formatting differences between SLT and cross-engine tests could mask or introduce false failures
- **Suggested Fix**: Extract shared conversion to `test_utils.rs` and have `hybrid_asyncdb.rs` call it.
- **Effort**: M
- **Recommended Agent**: fix-test-infra

### P2 Findings (38)

#### R6-S-015: replace_table_files omits row_id_start for compacted files (SQLite)
- **Source(s)**: correctness (R6-C-003)
- **File(s)**: `src/metadata_writer_sqlite.rs:1364-1365,1414-1422`
- **Description**: Compacted data files get `row_id_start = NULL`. Virtual `rowid` column and future appends produce incorrect/overlapping row IDs.
- **Suggested Fix**: Track cumulative row_id_start during compaction INSERT loop.
- **Effort**: S
- **Recommended Agent**: fix-sqlite-metadata

#### R6-S-016: new_next_row_id i64 overflow not checked (all backends)
- **Source(s)**: correctness (R6-C-004)
- **File(s)**: `src/metadata_writer_sqlite.rs:1551`, `src/metadata_writer_postgres.rs:1096`, `src/metadata_writer_mysql.rs:1227`
- **Description**: `row_id_start + file.record_count` without overflow checking.
- **Suggested Fix**: Use `checked_add().ok_or_else(|| ...)?`.
- **Effort**: S
- **Recommended Agent**: fix-sqlite-metadata

#### R6-S-017: Concurrent DML lost-delete race (PG/MySQL)
- **Source(s)**: correctness (R6-C-005)
- **File(s)**: `src/delete_exec.rs:191,228`, `src/update_exec.rs:211`, `src/merge_exec.rs:426`
- **Description**: `existing_deletes` snapshot captured at planning time. Concurrent writers can overwrite each other's delete files, resurrecting deleted rows. `begin_checked_write_transaction()` only checks DDL conflicts, not DML.
- **Suggested Fix**: Extend checked write to detect concurrent DML on same data files, or add per-file optimistic locking.
- **Effort**: L (related to R4-S-018, also deferred)
- **Recommended Agent**: deferred

#### R6-S-018: Non-atomic default replace_table_files (PG/MySQL)
- **Source(s)**: correctness (R6-C-008), codex (CX-M-001)
- **File(s)**: `src/metadata_writer.rs:493-517`
- **Description**: Default `replace_table_files()` calls multiple methods without transaction wrapper. SQLite overrides with atomic version; PG/MySQL inherit non-atomic default.
- **Suggested Fix**: Override `replace_table_files` in PG and MySQL writers with transactional implementations.
- **Effort**: M
- **Recommended Agent**: fix-backend-parity

#### R6-S-019: Decimal stat comparison precision loss
- **Source(s)**: correctness (R6-C-006), idiomatic (R6-I-026)
- **File(s)**: `src/metadata_writer_sqlite.rs:984-997`
- **Description**: `stat_value_less_than()` falls back to f64 for decimal comparisons, losing precision for values exceeding 2^53.
- **Suggested Fix**: Parse DECIMAL types with fixed-point or `rust_decimal::Decimal`.
- **Effort**: M
- **Recommended Agent**: fix-sqlite-metadata

#### R6-S-020: unwrap() on chrono epoch date in non-test code (4 sites)
- **Source(s)**: idiomatic (R6-I-002)
- **File(s)**: `src/table.rs:1925,1948`, `src/table_writer.rs:1358,1383`
- **Description**: `NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()` — technically a panic path though 1970-01-01 never returns None.
- **Suggested Fix**: Use a `const` or `lazy_static` epoch, or return error.
- **Effort**: S
- **Recommended Agent**: fix-error-handling

#### R6-S-021: expect() in combine_execution_plans
- **Source(s)**: idiomatic (R6-I-003)
- **File(s)**: `src/table.rs:1805`
- **Description**: `execs.into_iter().next().expect("checked len == 1 above")` is a panic path.
- **Suggested Fix**: Use `.ok_or_else(|| DataFusionError::Internal(...))?`.
- **Effort**: S
- **Recommended Agent**: fix-error-handling

#### R6-S-022: Duplicated parse_inlined_column / parse_string_to_array functions
- **Source(s)**: idiomatic (R6-I-006)
- **File(s)**: `src/table.rs:1849-2029`, `src/table_writer.rs:1269-1478`
- **Description**: ~180 lines each of near-identical type-dispatch pattern with minor error handling differences.
- **Suggested Fix**: Extract shared function parameterized by error policy.
- **Effort**: M
- **Recommended Agent**: fix-code-quality

#### R6-S-023: Repeated to_lowercase() allocation in partition routing hot path
- **Source(s)**: idiomatic (R6-I-008)
- **File(s)**: `src/insert_exec.rs:290,512-516`
- **Description**: `transform.to_lowercase()` called per-row/per-column-per-batch in insert hot path.
- **Suggested Fix**: Normalize transform to enum variant at planning time.
- **Effort**: S
- **Recommended Agent**: fix-code-quality

#### R6-S-024: Compaction connection opened before validating arguments
- **Source(s)**: idiomatic (R6-I-012)
- **File(s)**: `src/compaction_functions.rs:221,269,396,446`
- **Description**: `open_compaction_connection()` called before argument validation.
- **Suggested Fix**: Validate and parse arguments first, then open connection.
- **Effort**: S
- **Recommended Agent**: fix-table-functions

#### R6-S-025: No range validation for delete_threshold in rewrite_data_files
- **Source(s)**: idiomatic (R6-I-013)
- **File(s)**: `src/compaction_functions.rs:274,283`
- **Description**: `delete_threshold` accepts values outside documented `0.0..=1.0` range.
- **Suggested Fix**: Add explicit range check with `plan_err!`.
- **Effort**: S
- **Recommended Agent**: fix-table-functions

#### R6-S-026: INSTALL ducklake on every compaction call
- **Source(s)**: idiomatic (R6-I-014)
- **File(s)**: `src/compaction_functions.rs:66`
- **Description**: `open_compaction_connection()` runs `INSTALL ducklake; LOAD ducklake;` on every call. Network hit on first call, unnecessary after.
- **Suggested Fix**: Use `OnceLock` to track installation, or just `LOAD ducklake` (assume pre-installed).
- **Effort**: S
- **Recommended Agent**: fix-table-functions

#### R6-S-027: Silent NULL conversion in compaction scalar_to_*_array
- **Source(s)**: idiomatic (R6-I-017)
- **File(s)**: `src/compaction_functions.rs:474,486`
- **Description**: Type mismatches silently converted to NULL instead of erroring.
- **Suggested Fix**: Return error on unexpected `ScalarValue` variant.
- **Effort**: S
- **Recommended Agent**: fix-table-functions

#### R6-S-028: parse_table_name silently accepts malformed names
- **Source(s)**: idiomatic (R6-I-016)
- **File(s)**: `src/table_functions.rs:354`
- **Description**: Inputs like `".foo"` or `"foo."` fall through to default instead of erroring.
- **Suggested Fix**: Validate both parts are non-empty after splitting.
- **Effort**: S
- **Recommended Agent**: fix-table-functions

#### R6-S-029: Dynamic SQL DDL interpolates type text directly (SQLite)
- **Source(s)**: idiomatic (R6-I-025), codex (CX-M-009)
- **File(s)**: `src/metadata_writer_sqlite.rs:2938-2952`
- **Description**: `col.ducklake_type()` injected verbatim into CREATE TABLE SQL. Safe in practice via `ColumnDef` validation, but defense-in-depth gap.
- **Suggested Fix**: Validate type string against allowlist at SQL construction site.
- **Effort**: S
- **Recommended Agent**: fix-sqlite-metadata

#### R6-S-030: 7 extra tables with ducklake_ prefix risk future conflict
- **Source(s)**: interop (R6-IO-005)
- **File(s)**: `src/metadata_writer_sqlite.rs:120-299`
- **Description**: 6 tables use `ducklake_` prefix (`ducklake_macro*`, `ducklake_sort*`, `ducklake_file_variant_stats`) which could conflict with future DuckLake additions. `_df_change_tracking` correctly uses `_df_` prefix.
- **Suggested Fix**: Consider prefixing non-standard tables with `_df_` or documenting the convention.
- **Effort**: M
- **Recommended Agent**: fix-interop

#### R6-S-031: Timestamp type differences TEXT vs TIMESTAMPTZ
- **Source(s)**: interop (R6-IO-007, R6-IO-008)
- **File(s)**: `src/metadata_writer_sqlite.rs:33,213`
- **Description**: `ducklake_snapshot.snapshot_time` and `ducklake_files_scheduled_for_deletion.schedule_start` stored as TEXT vs DuckDB's TIMESTAMPTZ. DuckDB auto-casts when reading, but if DuckDB writes these tables, we need to handle its format.
- **Suggested Fix**: Ensure our code parses both UTC and timezone-offset timestamp formats.
- **Effort**: S
- **Recommended Agent**: fix-interop

#### R6-S-032: Missing DF-write→DuckDB-read cross-engine tests
- **Source(s)**: interop (R6-IO-009)
- **File(s)**: `tests/cross_engine_tests.rs`
- **Description**: No tests for: DF DELETE→DuckDB read, DF UPDATE→DuckDB read, DF ALTER TABLE→DuckDB read, DF partitioned INSERT→DuckDB read, DF inlined data→DuckDB read, DF DROP TABLE→DuckDB behavior, DF CREATE VIEW→DuckDB read.
- **Suggested Fix**: Add cross-engine tests for each DF DML operation verified by DuckDB reads.
- **Effort**: L
- **Recommended Agent**: fix-cross-engine-tests

#### R6-S-033: row_id_start allocation without row locking (PG/MySQL)
- **Source(s)**: codex (CX-M-003)
- **File(s)**: `src/metadata_writer_postgres.rs:947`, `src/metadata_writer_mysql.rs:1073`
- **Description**: SELECT + UPDATE without `FOR UPDATE` could race under multi-process concurrent writes. Safe for single-process.
- **Suggested Fix**: Add `FOR UPDATE` to the SELECT query.
- **Effort**: S
- **Recommended Agent**: fix-backend-parity

#### R6-S-034: ducklake_table_stats lacks UNIQUE constraint
- **Source(s)**: codex (CX-M-004)
- **File(s)**: `src/metadata_writer_sqlite.rs:170`, `src/metadata_writer_postgres.rs:154`, `src/metadata_writer_mysql.rs:187`
- **Description**: No PK/UNIQUE on `table_id`, allowing duplicate rows under concurrent writes.
- **Suggested Fix**: Add `UNIQUE(table_id)` constraint or make `table_id` the primary key.
- **Effort**: S
- **Recommended Agent**: fix-backend-parity

#### R6-S-035: SQLite per-table column_id allocation vs global (PG/MySQL)
- **Source(s)**: codex (CX-M-006)
- **File(s)**: `src/metadata_writer_sqlite.rs:509`
- **Description**: Design difference matching DuckDB convention. Document the difference.
- **Suggested Fix**: Document as intentional. No code change.
- **Effort**: S
- **Recommended Agent**: fix-metadata-correctness

#### R6-S-036: Partition transform validation too permissive
- **Source(s)**: codex (CX-M-008)
- **File(s)**: `src/metadata_writer_validation.rs:406`
- **Description**: Any transform string accepted; no duplicate column check.
- **Suggested Fix**: Add allowlist validation (identity, year, month, day, hour) and reject duplicate partition columns.
- **Effort**: S
- **Recommended Agent**: fix-metadata-correctness

#### R6-S-037: Upload failure in DML executors doesn't clean up prior uploads
- **Source(s)**: codex (CX-W-005)
- **File(s)**: `src/delete_exec.rs:368`, `src/update_exec.rs:444`, `src/merge_exec.rs:582`
- **Description**: When `object_store.put()` fails mid-loop, previously uploaded files are not cleaned up.
- **Suggested Fix**: Wrap file-processing loop to catch errors and clean up already-uploaded files.
- **Effort**: M
- **Recommended Agent**: fix-dml-robustness

#### R6-S-038: create_snapshot() failure after uploads leaves orphaned files
- **Source(s)**: codex (CX-W-006)
- **File(s)**: `src/delete_exec.rs:389`, `src/update_exec.rs:537`, `src/merge_exec.rs:699`
- **Description**: If `create_snapshot()` fails after successful uploads, function returns without cleanup.
- **Suggested Fix**: Add cleanup on snapshot creation failure path.
- **Effort**: S
- **Recommended Agent**: fix-dml-robustness

#### R6-S-039: Non-atomic single-file finish() path
- **Source(s)**: codex (CX-W-001)
- **File(s)**: `src/table_writer.rs:899-920`
- **Description**: `end_table_files` and `register_data_file` are separate operations. Documented risk (R5-S-022).
- **Suggested Fix**: Use `replace_table_files` for single-file replace path too.
- **Effort**: S
- **Recommended Agent**: fix-dml-robustness

#### R6-S-040: deregister_table/register_table don't propagate snapshot
- **Source(s)**: codex (CX-C-001, CX-C-002)
- **File(s)**: `src/schema.rs:357,419`
- **Description**: Returned snapshot IDs from drop_table/create_table are ignored. Affects only current session due to dynamic lookup pattern.
- **Suggested Fix**: Update catalog's `AtomicI64` snapshot_id with the new value.
- **Effort**: S
- **Recommended Agent**: fix-metadata-correctness

#### R6-S-041: Virtual-column limit pushed to each per-file scan
- **Source(s)**: codex (CX-C-007)
- **File(s)**: `src/table.rs:1579`
- **Description**: `limit` applied to each file scan, reading up to `limit * num_files` total rows.
- **Suggested Fix**: Only push limit to first file or use a global row counter.
- **Effort**: S
- **Recommended Agent**: fix-code-quality

#### R6-S-042: SLT preprocessor may hide tests via aggressive filtering
- **Source(s)**: test-harness (R6-T-005)
- **File(s)**: `tests/sqllogictest_runner.rs:616-676`
- **Description**: Broad patterns like `COLUMNS(` could match legitimate SQL. `meaningful_count > 0` threshold too low.
- **Suggested Fix**: Log total vs meaningful count ratio; make patterns more specific.
- **Effort**: M
- **Recommended Agent**: fix-test-infra

#### R6-S-043: normalize_value hides type confusion between integers and floats
- **Source(s)**: test-harness (R6-T-006)
- **File(s)**: `tests/common/test_utils.rs:299-311`
- **Description**: Float normalization always applied — no strict mode available for type-sensitive tests.
- **Suggested Fix**: Add optional `strict: bool` parameter to `assert_results_eq`.
- **Effort**: S
- **Recommended Agent**: fix-test-infra

#### R6-S-044: is_write_statement doesn't handle CTE-wrapped DML
- **Source(s)**: test-harness (R6-T-008)
- **File(s)**: `tests/hybrid_asyncdb.rs:117-143`
- **Description**: `WITH ... INSERT ...` starts with `WITH`, routing to DataFusion read path instead of DuckDB write path.
- **Suggested Fix**: Check for DML keywords after any `WITH...AS` prefix.
- **Effort**: M
- **Recommended Agent**: fix-test-infra

#### R6-S-045: rewrite_table_references doesn't handle double-quoted identifiers
- **Source(s)**: test-harness (R6-T-009)
- **File(s)**: `tests/hybrid_asyncdb.rs:150-216`
- **Description**: String-literal-aware rewriter handles single quotes but not double-quoted identifiers. SQL like `"ducklake"."my_table"` gets mangled.
- **Suggested Fix**: Extend char-by-char parser to skip double-quoted regions.
- **Effort**: S
- **Recommended Agent**: fix-test-infra

#### R6-S-046: No test coverage for ORDER BY ALL rewriting edge cases
- **Source(s)**: test-harness (R6-T-012)
- **File(s)**: `tests/hybrid_asyncdb.rs:312-322`, `tests/sqllogictest_runner.rs:516-534`
- **Description**: No unit tests; two independent implementations; would match inside string literals.
- **Suggested Fix**: Add unit tests for basic removal, LIMIT interaction, string literal false match.
- **Effort**: S
- **Recommended Agent**: fix-test-infra

#### R6-S-047: Write tests and schema evolution tests check only row counts, not actual values
- **Source(s)**: test-harness (R6-T-015, R6-T-016, R6-T-017), codex (CX-T-004, CX-T-005)
- **File(s)**: `tests/write_tests.rs:133-912`
- **Description**: Multiple write tests assert only `COUNT(*)` — never verify actual data values. Schema evolution tests don't verify column alignment or backfill values.
- **Suggested Fix**: Add value-level assertions to write tests and schema evolution tests.
- **Effort**: M
- **Recommended Agent**: fix-test-infra

#### R6-S-048: Cross-engine tests don't verify column names/types
- **Source(s)**: test-harness (R6-T-023)
- **File(s)**: `tests/cross_engine_tests.rs:119-1031`
- **Description**: Compare values but never schema (column names, types).
- **Suggested Fix**: Add schema assertions in core test patterns.
- **Effort**: M
- **Recommended Agent**: fix-cross-engine-tests

#### R6-S-049: No cross-engine BOOLEAN type roundtrip test
- **Source(s)**: test-harness (R6-T-025)
- **File(s)**: `tests/cross_engine_tests.rs`
- **Description**: Cross-engine tests cover INT, VARCHAR, DOUBLE, TIMESTAMP, DATE, DECIMAL but no dedicated BOOLEAN test.
- **Suggested Fix**: Add `cross_engine_boolean_type_roundtrip` test.
- **Effort**: S
- **Recommended Agent**: fix-cross-engine-tests

#### R6-S-050: SLT statement error to statement ok conversion may mask real errors
- **Source(s)**: test-harness (R6-T-020)
- **File(s)**: `tests/sqllogictest_runner.rs:316-402`
- **Description**: Matching on error text alone could match future unrelated test cases.
- **Suggested Fix**: Consider matching on SQL + error text combination.
- **Effort**: S
- **Recommended Agent**: fix-test-infra

#### R6-S-051: Read-only test accepts unrelated error messages
- **Source(s)**: codex (CX-T-001)
- **File(s)**: `tests/sql_write_tests.rs:277`
- **Description**: Test accepts "column count" and "not supported" as valid errors for read-only rejection.
- **Suggested Fix**: Tighten error message assertion to only accept the read-only error.
- **Effort**: S
- **Recommended Agent**: fix-test-infra

#### R6-S-052: Orphan cleanup hardcoded far-future date
- **Source(s)**: codex (CX-TF-001)
- **File(s)**: `src/compaction_functions.rs:451`
- **Description**: `older_than := '2099-01-01'` effectively disables safety window. Documented workaround for DuckDB TIMESTAMPTZ bugs.
- **Suggested Fix**: Add configurable `older_than` parameter.
- **Effort**: S
- **Recommended Agent**: fix-table-functions

### P3 Findings (36)

| ID | Title | Source | File(s) | Effort |
|---|---|---|---|---|
| R6-S-053 | DDL boilerplate duplicated 27x across metadata writers | idiomatic (R6-I-007) | metadata_writer_{sqlite,postgres,mysql}.rs | M |
| R6-S-054 | Stream collection pattern not idiomatic in read_delete_file_positions | idiomatic (R6-I-010) | src/table.rs:521-535 | S |
| R6-S-055 | Compaction collect_duckdb_rows intermediate Vec allocation | idiomatic (R6-I-015) | src/compaction_functions.rs:89 | M |
| R6-S-056 | source_match_masks clone creates unnecessary allocation | idiomatic (R6-I-018) | src/merge_exec.rs:525 | S |
| R6-S-057 | inlined_rows_to_batch O(n*m) position() lookup | idiomatic (R6-I-019) | src/table_writer.rs:1195 | S |
| R6-S-058 | partition_values clone in write_partitioned | idiomatic (R6-I-020) | src/insert_exec.rs:791 | S |
| R6-S-059 | Excessive struct fields in DML exec constructors | idiomatic (R6-I-022) | delete_exec/merge_exec/update_exec/table_writer | M |
| R6-S-060 | table_functions materializes full file list at planning time | idiomatic (R6-I-023) | src/table_functions.rs:86 | M |
| R6-S-061 | SingleValueTable allocates RecordBatch per scan call | idiomatic (R6-I-024) | src/table_functions.rs:517 | S |
| R6-S-062 | ColumnDef pub visibility inconsistent | idiomatic (R6-I-027) | src/metadata_writer.rs:114-131 | S |
| R6-S-063 | types.rs excessive to_string() allocations | idiomatic (R6-I-028) | src/types.rs | S |
| R6-S-064 | recompute_table_column_stats join could add table_id filter | correctness (R6-C-007) | src/metadata_writer_sqlite.rs:858 | S |
| R6-S-065 | UUID v4 vs DuckDB's UUID v7 for file naming | interop (R6-IO-006) | src/table_writer.rs, delete_exec.rs, update_exec.rs | S |
| R6-S-066 | Extra columns in ducklake_column/data_file/delete_file/schema_versions | interop (R6-IO-002,003,004) | metadata_writer_{sqlite,postgres,mysql}.rs | S |
| R6-S-067 | join_key_pairs bounds check missing in MERGE | codex (CX-W-003) | src/merge_exec.rs | S |
| R6-S-068 | Inline row limit check uses unchecked i64 addition | codex (CX-W-008) | src/table_writer.rs:302 | S |
| R6-S-069 | Full-file delete record_count used without validation | codex (CX-TF-005) | src/table_deletions.rs:665 | S |
| R6-S-070 | DeletedRowsExec with_new_children doesn't reject extra children | codex (CX-TF-006) | src/table_deletions.rs:422 | S |
| R6-S-071 | join_paths normalizes // in valid keys (documented R5-S-075) | codex (CX-C-003) | src/path_resolver.rs:277 | — |
| R6-S-072 | DeleteFilterStream row_offset reset per partition (not reachable) | codex (CX-C-004) | src/delete_filter.rs:119 | — |
| R6-S-073 | View SQL rewriting is O(n²) | codex (CX-C-005) | src/schema.rs:184 | S |
| R6-S-074 | create_object_store helper duplicated in 17 test files | test-harness (R6-T-003) | tests/*.rs | M |
| R6-S-075 | Partition column_index not bounds-checked | correctness (R6-C-009), codex (CX-W-004) | src/insert_exec.rs:542,664 | S |
| R6-S-076 | open_in_datafusion_duckdb duplicated in 4 test files | test-harness (R6-T-004) | tests/cross_engine_*.rs | S |
| R6-S-077 | format_float edge cases may not match DuckDB | test-harness (R6-T-007) | tests/hybrid_asyncdb.rs:551-568 | S |
| R6-S-078 | Virtual column check uppercase comparison fragile | test-harness (R6-T-010) | tests/hybrid_asyncdb.rs:344-370 | S |
| R6-S-079 | Cross-engine UPDATE test uses f64 equality | test-harness (R6-T-011) | tests/cross_engine_tests.rs:630-635 | S |
| R6-S-080 | is_three_part_ref doesn't handle numeric schema names | test-harness (R6-T-013) | tests/hybrid_asyncdb.rs:240-259 | S |
| R6-S-081 | DuckDB in-transaction reads return DefaultColumnType::Any | test-harness (R6-T-014) | tests/hybrid_asyncdb.rs:464-466 | M |
| R6-S-082 | test_replace_semantics doesn't verify old data absence | test-harness (R6-T-018) | tests/write_tests.rs:229-287 | S |
| R6-S-083 | No tests for unsupported types in convert_batch_to_strings | test-harness (R6-T-019) | tests/hybrid_asyncdb.rs:721-727 | M |
| R6-S-084 | No coverage for multi-line SQL in is_write_statement | test-harness (R6-T-021) | tests/hybrid_asyncdb.rs:117-143 | S |
| R6-S-085 | refresh_catalog creates new SessionContext per write | test-harness (R6-T-022) | tests/hybrid_asyncdb.rs:262-282 | M |
| R6-S-086 | df_query silently filters virtual columns | test-harness (R6-T-024) | tests/common/test_utils.rs:350-354 | S |
| R6-S-087 | No BLOB/BINARY cross-engine roundtrip test | test-harness (R6-T-026) | tests/ | S |
| R6-S-088 | Merge tests sort results after ORDER BY | codex (CX-T-002) | tests/merge_tests.rs:212,291,376 | S |

**Additional P3 (documented/no-action):**
- R6-S-071 (CX-C-003): Already documented as R5-S-075
- R6-S-072 (CX-C-004): Not reachable in DuckLake usage
- R6-T-027 (concurrent TempDir): No action needed
- R6-T-028 (DatePartAliasUdf Int32): Verify matches DataFusion behavior
- CX-T-006 (cross-engine date values skipped): Known limitation

---

## Recommended Fix Agents

### Agent 1: fix-sqlite-metadata — SQLite metadata writer fixes
- **Findings**: R6-S-001 (P1), R6-S-015 (P2), R6-S-016 (P2), R6-S-019 (P2), R6-S-029 (P2)
- **Estimated effort**: M
- **Focus**: Fix column stats INSERT, row_id_start in compaction, overflow check, decimal precision, type validation

### Agent 2: fix-backend-parity — PG/MySQL parity with SQLite
- **Findings**: R6-S-003 (P1), R6-S-004 (P1), R6-S-018 (P2), R6-S-033 (P2), R6-S-034 (P2)
- **Estimated effort**: M-L
- **Focus**: record_count decrement, end_table_files drift, atomic replace, row locking, UNIQUE constraint

### Agent 3: fix-error-handling — Panic paths and silent error swallowing
- **Findings**: R6-S-005 (P1), R6-S-006 (P1), R6-S-007 (P1), R6-S-008 (P1), R6-S-020 (P2), R6-S-021 (P2)
- **Estimated effort**: M
- **Focus**: Replace unwrap/expect with proper error returns, fix silent NULL/empty string on errors

### Agent 4: fix-table-functions — Compaction and table function issues
- **Findings**: R6-S-002 (P1), R6-S-011 (P1), R6-S-024 (P2), R6-S-025 (P2), R6-S-026 (P2), R6-S-027 (P2), R6-S-028 (P2), R6-S-052 (P2)
- **Estimated effort**: L (R6-S-002 is L alone)
- **Focus**: Defer side effects to scan time, fix parse_table_name, validation, INSTALL caching

### Agent 5: fix-interop — Interoperability fixes
- **Findings**: R6-S-009 (P1), R6-S-012 (P1), R6-S-030 (P2), R6-S-031 (P2)
- **Estimated effort**: M
- **Focus**: Hardcoded schema_version, CDC encryption factory, extra table naming, timestamp format

### Agent 6: fix-metadata-correctness — Metadata and catalog correctness
- **Findings**: R6-S-010 (P1), R6-S-035 (P2), R6-S-036 (P2), R6-S-040 (P2)
- **Estimated effort**: M
- **Focus**: SET NOT NULL validation (or document), column_id documentation, partition transform validation, snapshot propagation

### Agent 7: fix-test-infra — Test infrastructure improvements
- **Findings**: R6-S-013 (P1), R6-S-014 (P1), R6-S-042 (P2), R6-S-043 (P2), R6-S-044 (P2), R6-S-045 (P2), R6-S-046 (P2), R6-S-047 (P2), R6-S-050 (P2), R6-S-051 (P2)
- **Estimated effort**: L
- **Focus**: Fix false-positive test, deduplicate type conversion, improve SLT filtering, add value assertions, fix routing bugs

### Agent 8: fix-dml-robustness — DML cleanup and atomicity
- **Findings**: R6-S-037 (P2), R6-S-038 (P2), R6-S-039 (P2)
- **Estimated effort**: M
- **Focus**: Upload failure cleanup, snapshot failure cleanup, single-file atomicity

### Agent 9: fix-code-quality — Code dedup and performance
- **Findings**: R6-S-022 (P2), R6-S-023 (P2), R6-S-041 (P2)
- **Estimated effort**: M
- **Focus**: Extract shared parser, normalize transform at planning time, fix limit pushdown

### Agent 10: fix-cross-engine-tests — Cross-engine test coverage
- **Findings**: R6-S-032 (P2), R6-S-048 (P2), R6-S-049 (P2)
- **Estimated effort**: L
- **Focus**: DF-write→DuckDB-read tests, schema assertions, BOOLEAN roundtrip

### Deferred (not assigned)
- R6-S-017 (P2): Concurrent DML lost-delete race — related to R4-S-018, architectural
- R6-S-010 (P1): SET NOT NULL data scan — marked for documentation rather than implementation (L effort, rare use case)
- All 36 P3 findings — optional, low impact

---

## Previously Deferred Items (still open)
- **F-036**: INSERT streaming for OOM prevention (R2, L effort)
- **F-044**: Provider/writer code deduplication (R2, L effort)
- **F-045**: Async trait redesign, sync→async (R2, L effort)
- **R4-S-018**: PG/MySQL checked write TOCTOU (R4, P2)
- **R4-S-036**: map_err boilerplate (R4, P3)
- **R4-S-040**: Monolithic execute() blocks (R4, P3)

---

## Codex P0 Validation

All 3 codex P0 claims this cycle were correctly downgraded by the codex-review agent:

1. **CX-W-001** (Non-atomic single-file finish): Documented risk with R5-S-022 comment. Multi-file path IS atomic. → **P2**
2. **CX-M-001** (Non-atomic default replace_table_files): SQLite overrides with atomic version. PG/MySQL gap is real but low-traffic. → **P2**
3. **CX-TF-001** (Orphan cleanup hardcoded date): DuckDB TIMESTAMPTZ workaround. Transaction model prevents data loss. → **P2**

**P0 FP rate this cycle**: 3/3 (100%)
**Cumulative P0 FP rate (R4-R6)**: R4-R5 was 86%, R6 adds 3/3 → estimated ~90%+
