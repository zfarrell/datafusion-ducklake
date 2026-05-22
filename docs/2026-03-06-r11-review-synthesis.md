# R11 Review Synthesis

## Overview
- Raw findings: 97 across 5 reviews (idiomatic: 16, correctness: 11, interop: 8, test-harness: 14, codex: 48)
- After dedup: 45 unique
- By priority: P0: 0, P1: 11, P2: 22, P3: 9
- Pre-existing/deferred: 3 (F-036, F-045, R4-S-018)
- False positives removed: 3 (R11-CX-011, R11-CX-044, R11-CX-045)

## Key Takeaway

The codebase is structurally sound after 10 prior review cycles. The most significant finding is `append_table_files` starting row_id at 0 and never updating table stats — this affects all partitioned INSERTs to non-empty tables. Several snapshot-awareness gaps exist in metadata provider queries (column stats, partition columns), and a PostgreSQL-specific bug prevents DDL snapshots from working. The codex P0 (R11-CX-001) was validated as a real bug but downgraded to P1 because delete files use `(file_path, pos)` not rowid, limiting the blast radius to stale statistics and incorrect virtual rowid values.

## Codex P0 Validation

**R11-CX-001 (append_table_files row_id_start=0)**: VALIDATED as real bug, DOWNGRADED to P1.

Evidence: `src/metadata_writer_impl.rs:812` initializes `cumulative_row_id = 0` without reading `next_row_id` from `ducklake_table_stats`. Compare with `register_data_file` (line 462-473) which correctly reads `next_row_id` via `stats_sql` with `FOR UPDATE`. Additionally, `append_table_files` never calls `recompute_table_column_stats` or updates `ducklake_table_stats`.

Production usage: Called from `table_writer.rs:719` in the partitioned INSERT commit path. Every partitioned INSERT to a non-empty table gets overlapping row_ids.

Why P1 not P0: Delete files store `(file_path, pos)` — file_path-scoped, not rowid-scoped. So overlapping row_ids don't cause delete corruption. Impact is: (1) stale `record_count`/`next_row_id` in `ducklake_table_stats`, (2) incorrect `rowid` virtual column values, (3) degraded query planning from stale statistics. Subsequent single-file INSERTs also inherit stale `next_row_id`.

---

## Validated Findings

### R11-S-001: append_table_files row_id=0 and missing stats update (P1)
**Source**: R11-C-001, R11-CX-001, R11-C-005
**Files**: `src/metadata_writer_impl.rs:812, 765-883`
**Description**: `append_table_files` initializes `cumulative_row_id = 0` instead of reading current `next_row_id` from `ducklake_table_stats`. Never updates `ducklake_table_stats` (record_count, next_row_id, file_size_bytes) or calls `recompute_table_column_stats`. Compare with `register_data_file` (line 462) and `register_dml_files` (line 1033) which do both correctly.
**Validation**: Confirmed by reading source. `register_data_file` reads stats with FOR UPDATE; `append_table_files` does not.
**Suggested fix**: Read `next_row_id` from `ducklake_table_stats` (locked) at tx start, use as base for `cumulative_row_id`, update/insert stats, and call `recompute_table_column_stats` before commit.
**Effort**: M

### R11-S-002: Orphan Parquet files on partitioned INSERT commit failure (P1)
**Source**: R11-I-001, R11-CX-027
**Files**: `src/insert_exec.rs:804-816`
**Description**: In `write_partitioned()`, after all partition Parquet files are uploaded, earlier partitions' `UploadCleanupGuard`s have been defused (`.uploaded_path()` consumed). If `register_files_for_table` fails, only the last writer's guard is active. Earlier files become orphans.
**Validation**: Confirmed — the loop consumes guards sequentially.
**Suggested fix**: Collect all uploaded paths into a composite cleanup guard; defuse only after metadata commit succeeds.
**Effort**: M

### R11-S-003: MERGE does not detect multiple source rows matching same target (P1)
**Source**: R11-C-002
**Files**: `src/merge_exec.rs:493-513`
**Description**: The SQL standard requires an error when multiple source rows match the same target row. The code tracks `source_match_count` (source matching multiple targets) but not the inverse. When duplicate source keys exist, the inner candidate loop `break`s after the first match — silently using a nondeterministic source row with no error.
**Validation**: Confirmed. R11-CX-011 (false positive) validated the inverse direction (source→multiple targets) is correctly detected, but the direction reported in R11-C-002 (multiple sources→same target) is NOT checked.
**Suggested fix**: Track per-target match count. If any target is matched by >1 source row, return execution error.
**Effort**: M

### R11-S-004: FOR UPDATE with aggregate fails on PostgreSQL (P1)
**Source**: R11-CX-005
**Files**: `src/metadata_writer_impl.rs:1213-1216`
**Description**: `create_ddl_snapshot` macro builds `SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot FOR UPDATE`. PostgreSQL rejects `FOR UPDATE` on aggregate queries at runtime.
**Validation**: Confirmed — PostgreSQL spec disallows `FOR UPDATE` with aggregates.
**Suggested fix**: Use `SELECT schema_version FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1 FOR UPDATE` or remove FOR UPDATE and use advisory lock.
**Effort**: S

### R11-S-005: get_file_column_stats ignores snapshot_id for column join (P1)
**Source**: R11-CX-006
**Files**: `src/metadata_provider_impl.rs:694`
**Description**: Column stats query joins `ducklake_column` with `c.end_snapshot IS NULL` only, ignoring the target snapshot. For historical snapshot queries (time travel), this returns current column names instead of names valid at that snapshot. Would cause incorrect stats attribution after column renames.
**Validation**: Confirmed — line 694 uses `c.end_snapshot IS NULL` without temporal predicates.
**Suggested fix**: Replace `c.end_snapshot IS NULL` with `c.begin_snapshot <= snapshot_id AND (c.end_snapshot IS NULL OR c.end_snapshot > snapshot_id)`.
**Effort**: S

### R11-S-006: get_partition_columns ignores snapshot_id for column join (P1)
**Source**: R11-CX-007
**Files**: `src/metadata_provider_impl.rs:791`
**Description**: Same pattern as R11-S-005. Partition column query joins columns with `c.end_snapshot IS NULL` only.
**Validation**: Confirmed — line 791 uses identical non-temporal predicate.
**Suggested fix**: Apply snapshot-window predicates to `ducklake_column` join.
**Effort**: S

### R11-S-007: DuckDB delete_file_id silent error swallowing (P1)
**Source**: R11-CX-015
**Files**: `src/metadata_provider_duckdb.rs:269, 462`
**Description**: `if let Ok(Some(_)) = row.get::<_, Option<i64>>(6)` silently treats type-conversion errors as "no delete file". A corrupt or mistyped column value would be ignored, potentially serving deleted rows.
**Validation**: Confirmed — error branch maps to None (no delete file).
**Suggested fix**: Use `row.get::<_, Option<i64>>(6)?` with `?` propagation, then match on Option.
**Effort**: S

### R11-S-008: DuckDB get_inlined_data missing schema qualification (P1)
**Source**: R11-CX-016
**Files**: `src/metadata_provider_duckdb.rs:629`
**Description**: Table existence check queries `information_schema.tables` by `table_name` only, without `table_schema = 'main'`. Could match wrong table when names collide across schemas.
**Validation**: Confirmed — `count_inlined_rows` at different location uses schema predicate but `get_inlined_data` does not.
**Suggested fix**: Add `AND table_schema = 'main'` to the query.
**Effort**: S

### R11-S-009: Type promotion case-sensitive comparison (P1)
**Source**: R11-CX-023
**Files**: `src/metadata_writer_validation.rs:286`
**Description**: `is_type_promotion_allowed` uses lowercase string literals for comparison. If external catalogs or DuckDB store type names in mixed case, valid promotions would be incorrectly rejected, preventing ALTER TABLE operations.
**Validation**: Confirmed — no case normalization before comparison.
**Suggested fix**: Normalize both types to lowercase before comparison.
**Effort**: S

### R11-S-010: Compaction delete_threshold NaN passthrough (P1)
**Source**: R11-CX-024
**Files**: `src/compaction_functions.rs:364`
**Description**: `NaN < 0.0` and `NaN > 1.0` are both false in IEEE 754, so NaN passes the range check and gets interpolated into SQL, potentially causing unexpected DuckDB behavior.
**Validation**: Confirmed — IEEE 754 NaN comparison semantics bypass the range guard.
**Suggested fix**: Add `threshold.is_finite()` check before range validation.
**Effort**: S

### R11-S-011: No cross-engine test for DF-written DELETE files read by DuckDB (P1)
**Source**: R11-IO-005
**Files**: `tests/cross_engine_dml_tests.rs`
**Description**: Cross-engine tests cover DF DELETE->DF read and DuckDB DELETE->DF read, but not DF DELETE->DuckDB read. The MOR delete file uses sentinel field_ids (`0x7FFFFFFE`, `0x7FFFFFFD`) that haven't been verified cross-engine. If DuckDB expects different values, deleted rows could reappear.
**Validation**: Confirmed — no test exercises this path. Interop reviewer rated P1 as critical gap.
**Suggested fix**: Add cross-engine test: DuckDB creates table + inserts, DataFusion deletes rows, DuckDB reads and verifies exclusion.
**Effort**: M

### R11-S-012: Schema equality ignores nullability in all backends (P2)
**Source**: R11-CX-026
**Files**: `src/metadata_writer_sqlite.rs:559`, `src/metadata_writer_postgres.rs:442`, `src/metadata_writer_mysql.rs:559`
**Description**: Schema match checks compare only name/type, ignoring nullable state. Nullability changes during INSERT schema evolution go unrecorded in catalog metadata.
**Suggested fix**: Include nullability in schema comparison.
**Effort**: S

### R11-S-013: Timestamp partition overflow in unchecked multiplication (P2)
**Source**: R11-I-002, R11-CX-010
**Files**: `src/table_writer.rs:1158-1169`, `src/insert_exec.rs:433`
**Description**: Timestamp unit conversions use unchecked multiplication (`value * 1_000_000` for ms->ns, `value * 1_000` for us->ns). Extreme timestamps silently overflow i64, producing incorrect partition keys.
**Suggested fix**: Use `checked_mul` or convert directly via `DateTime::from_timestamp_micros`.
**Effort**: S

### R11-S-014: Null count saturation/overflow across multiple locations (P2)
**Source**: R11-I-004, R11-C-004, R11-CX-032
**Files**: `src/table_writer.rs:1325,1329`, `src/table.rs:1379`
**Description**: Null count computed as `n as i64` (wraps on overflow) and accumulated via `saturating_add` (silently caps). Multiple locations have unchecked null count arithmetic.
**Suggested fix**: Use `i64::try_from` with error, and `checked_add` with error propagation.
**Effort**: S

### R11-S-015: O(n*m) position lookup in inlined_rows_to_batch (P2)
**Source**: R11-I-005, R11-CX-029
**Files**: `src/table_writer.rs:1241`
**Description**: Per-row per-column linear name lookup via `.iter().position()`. O(n*m) for n rows and m columns.
**Suggested fix**: Precompute `HashMap<&str, usize>` from column names.
**Effort**: S

### R11-S-016: Per-row allocation pressure in MERGE hash join (P2)
**Source**: R11-I-006, R11-I-007
**Files**: `src/merge_exec.rs:285, 478`
**Description**: `Vec<HashableKeyValue>` allocated per row, plus string cloning per key value. Thousands of small heap allocations per batch in the MERGE hot path.
**Suggested fix**: Pre-allocate reusable Vec; consider columnar hashing approach.
**Effort**: M

### R11-S-017: Duplicated Parquet write boilerplate in merge_exec/update_exec (P2)
**Source**: R11-I-008
**Files**: `src/merge_exec.rs:580-650`, `src/update_exec.rs:350-420`
**Description**: Nearly identical Parquet file writing code (create ArrowWriter, write batches, close, upload, register) in both exec plans.
**Suggested fix**: Extract shared helper into `table_writer.rs`.
**Effort**: M

### R11-S-018: Unchecked `as i64` casts in metadata_writer_impl (P2)
**Source**: R11-I-009
**Files**: `src/metadata_writer_impl.rs:313, 1476`
**Description**: `partition_key_index as i64` and `column_order as i64` cast usize to i64 without overflow checking.
**Suggested fix**: Use `i64::try_from()` with error propagation or debug assertions.
**Effort**: S

### R11-S-019: Date32 num_days i32 truncation in parse_values (P2)
**Source**: R11-CX-003
**Files**: `src/parse_values.rs:130`
**Description**: `num_days() as i32` performs unchecked narrowing. Out-of-range parsed dates wrap into incorrect offsets.
**Suggested fix**: Use `i32::try_from(num_days)` and handle per ParseMode.
**Effort**: S

### R11-S-020: Decimal pow(scale) can overflow for invalid metadata (P2)
**Source**: R11-CX-004
**Files**: `src/parse_values.rs:362, 382`
**Description**: `10i128.pow(scale_u)` computed before use in `checked_mul`. Valid scales (0-38) fit, but corrupt metadata could overflow.
**Suggested fix**: Use checked power helper for defense-in-depth.
**Effort**: S

### R11-S-021: delete_filter row_offset unchecked arithmetic (P2)
**Source**: R11-CX-009, R11-C-010
**Files**: `src/delete_filter.rs:160, 188`
**Description**: `row_offset += num_rows` and `row_offset + i64::from(i)` use unchecked addition. Theoretical overflow at >9.2 quintillion rows.
**Suggested fix**: Use `checked_add` and return DataFusionError on overflow.
**Effort**: S

### R11-S-022: register_data_file omits file_format in replace/append paths (P2)
**Source**: R11-IO-007
**Files**: `src/metadata_writer_impl.rs:594-606, 775-787`
**Description**: `replace_table_files` and `append_table_files` omit `file_format` from INSERT. SQLite default fills it; DuckDB schema may leave NULL.
**Suggested fix**: Add `file_format = 'parquet'` to INSERT column lists.
**Effort**: S

### R11-S-023: Massive test helper duplication across 17+ files (P2)
**Source**: R11-TH-002
**Files**: `tests/delete_tests.rs:23-79`, `tests/update_tests.rs:23-64`, and 15+ more
**Description**: `create_object_store()`, `create_test_env()`, `create_read_context()`, `create_writable_context()` copy-pasted across 17 test files.
**Suggested fix**: Centralize in `tests/common/setup.rs` and import.
**Effort**: M

### R11-S-024: No regression tests for R10 checked_add/Arc fixes (P2)
**Source**: R11-TH-006
**Files**: R10 commits: `4bd934b`, `d7a8d5a`, `4624df5`
**Description**: R10 correctness fixes (checked_add, Arc<Vec> deref, transaction wrapping) have no dedicated regression tests.
**Suggested fix**: Add targeted regression tests exercising overflow boundaries and multi-batch iteration.
**Effort**: M

### R11-S-025: Default trait impls for register_dml_files non-atomic (P2)
**Source**: R11-CX-002
**Files**: `src/metadata_writer.rs:468, 499`
**Description**: Default implementations perform per-file writes without transaction. All backends override with atomic implementations, but defaults could be accidentally used.
**Suggested fix**: Make defaults return `Err(Unsupported)` to prevent accidental use.
**Effort**: S

### R11-S-026: SLT DOES NOT EXIST pattern overly broad (P2)
**Source**: R11-TH-013
**Files**: `tests/sqllogictest_runner.rs:687-736`
**Description**: `HYBRID_INCOMPATIBLE_PATTERNS` includes `"DOES NOT EXIST!"` which could convert genuine table-not-found errors to statement ok.
**Suggested fix**: Tighten pattern to match only DETACH-related errors.
**Effort**: S

### R11-S-027: Compaction DUCKLAKE_INSTALLED check-then-set race (P2)
**Source**: R11-CX-033
**Files**: `src/compaction_functions.rs:85`
**Description**: Concurrent calls can both see `false` and execute `INSTALL ducklake` simultaneously.
**Suggested fix**: Use `OnceLock` or `std::sync::Once`.
**Effort**: S

### R11-S-028: TOCTOU in inlined data reads across all sqlx backends (P2)
**Source**: R11-CX-042
**Files**: `src/metadata_provider_sqlite.rs:222`, `src/metadata_provider_postgres.rs:208`, `src/metadata_provider_mysql.rs:216`
**Description**: Separate existence/columns/data queries for inlined data are vulnerable to concurrent DDL between queries.
**Suggested fix**: Execute within a single transaction.
**Effort**: S

### R11-S-029: store_inlined_data O(rows*cols^2) in SQLite writer (P2)
**Source**: R11-CX-038
**Files**: `src/metadata_writer_sqlite.rs:1328`
**Description**: Per-cell linear name lookup in nested loops.
**Suggested fix**: Precompute HashMap of column name to index.
**Effort**: S

### R11-S-030: next_entity_id table_id unwrap can panic (P2)
**Source**: R11-CX-039
**Files**: `src/metadata_writer_sqlite.rs:706`
**Description**: `table_id.unwrap()` can panic on contract violation.
**Suggested fix**: Use `ok_or_else(|| DuckLakeError::Internal(...))`.
**Effort**: S

### R11-S-031: DF-created partitioned data not tested cross-engine (P2)
**Source**: R11-IO-008
**Files**: `tests/cross_engine_partition_tests.rs`
**Description**: Partition tests only cover DuckDB-created partitions read by DF. DF-created partitions read by DuckDB are untested.
**Suggested fix**: Add reverse cross-engine partition test.
**Effort**: M

### R11-S-032: normalize_value masks integer/float type confusion in tests (P2)
**Source**: R11-TH-011
**Files**: `tests/common/test_utils.rs:299-311`
**Description**: Float normalization to 6 decimal places means Decimal and Float32 values can match incorrectly.
**Suggested fix**: Use `assert_results_eq_strict` for cross-engine Decimal comparisons.
**Effort**: S

### R11-S-033: cdc_common O(n^2) reorder mapping (P2)
**Source**: R11-CX-035
**Files**: `src/cdc_common.rs:98`
**Description**: Reorder mapping uses `position()` per projected index.
**Suggested fix**: Precompute positions in single pass.
**Effort**: S

### R11-S-034: Unnecessary partition_values clone in partitioned INSERT (P3)
**Source**: R11-I-010
**Files**: `src/insert_exec.rs:804`
**Description**: `partition_values` cloned but not used afterward.
**Suggested fix**: Use `std::mem::take` or pass ownership.
**Effort**: S

### R11-S-035: Widening `as i64` casts vs `From` trait in merge_exec (P3)
**Source**: R11-I-011
**Files**: `src/merge_exec.rs:223-235`
**Description**: `value as i64` for widening casts doesn't communicate intent. `i64::from(value)` is idiomatic.
**Suggested fix**: Replace with `i64::from()`.
**Effort**: S

### R11-S-036: Avoidable mask clone in MERGE matched-row processing (P3)
**Source**: R11-I-012
**Files**: `src/merge_exec.rs:540`
**Description**: Boolean mask cloned but not used afterward.
**Suggested fix**: Remove `.clone()`.
**Effort**: S

### R11-S-037: Date32 partition None for extreme dates (P3)
**Source**: R11-C-009
**Files**: `src/insert_exec.rs:362-365`
**Description**: `from_num_days_from_ce_opt` silently returns None for extreme dates, routing to default partition.
**Suggested fix**: Use `checked_add` and return error on overflow.
**Effort**: S

### R11-S-038: Extra ducklake_column columns vs DuckDB schema (P3)
**Source**: R11-IO-001
**Files**: `src/metadata_writer_sqlite.rs:140-141`
**Description**: `default_value_type` and `default_value_dialect` not in DuckDB v1.4.4 schema. Forward-compatible; DuckDB ignores extra columns.
**Suggested fix**: Document as forward-compatible extensions. No code change.
**Effort**: S

### R11-S-039: SLT ORDER BY ALL rewriting doesn't handle subqueries (P3)
**Source**: R11-TH-010
**Files**: `tests/sqllogictest_runner.rs:518-536`
**Description**: Simple string replacement of ORDER BY ALL. Subquery edge case unlikely.
**Suggested fix**: Document limitation.
**Effort**: S

### R11-S-040: information_schema test permanently ignored (P3)
**Source**: R11-TH-008
**Files**: `tests/information_schema_test.rs:13-14`
**Description**: `#[ignore]` since creation, providing no coverage.
**Suggested fix**: Fix test setup or remove dead test.
**Effort**: S

### R11-S-041: DuckDB value Debug fallback loses type info in tests (P3)
**Source**: R11-TH-009
**Files**: `tests/common/test_utils.rs:51-58`
**Description**: Catch-all Debug format for unknown DuckDB types produces non-comparable output.
**Suggested fix**: Add explicit handling for Blob, Time, Interval.
**Effort**: S

### R11-S-042: Complex char-by-char view SQL rewriting (P3)
**Source**: R11-I-014
**Files**: `src/schema.rs:480-560`
**Description**: Manual state machine for DuckDB->DataFusion SQL translation. Fragile but functional.
**Suggested fix**: Consider sqlparser-rs for robustness, or add comprehensive unit tests.
**Effort**: L

### R11-S-043: write_parquet_with_setup derives path from names, not stored path (P3)
**Source**: R11-C-011
**Files**: `src/table_writer.rs:512-515`
**Description**: Inlining flush path constructs table_key from names, not catalog-stored path. Edge case after table rename + inlined flush.
**Suggested fix**: Pass catalog-stored table_path into the function.
**Effort**: S

### R11-S-044: block_on panics if no Tokio runtime active (P3)
**Source**: R11-CX-036
**Files**: `src/metadata_provider.rs:784`
**Description**: `Handle::current()` panics without active runtime.
**Suggested fix**: Use `Handle::try_current()` with fallback. Deferred per F-045.
**Effort**: S

### R11-S-045: SLT skip_query_results may consume next record on malformed input (P3)
**Source**: R11-TH-007
**Files**: `tests/sqllogictest_runner.rs:768-790`
**Description**: Missing `----` separator causes line over-consumption.
**Suggested fix**: Add warning when separator not found.
**Effort**: S

---

## Pre-existing / Deferred Issues

| ID | Issue | Deferred As | Status |
|----|-------|-------------|--------|
| R11-I-003 / R11-CX-028 | INSERT full stream materialization OOM | F-036 | Deferred (L effort, architectural) |
| R11-I-016 | block_on sync-over-async in metadata providers | F-045 | Deferred (L effort, waiting on DataFusion async traits) |
| R11-C-003 | register_dml_files TOCTOU with concurrent DML | R4-S-018 / R6-S-017 | Deferred (L effort, requires PG/MySQL advisory locks) |

## False Positives Removed

| Source | Claim | Reason |
|--------|-------|--------|
| R11-CX-011 | MERGE multi-match silent first-candidate | Code correctly tracks `source_match_count` for source->multi-target direction. The inverse (R11-C-002) IS a real bug. |
| R11-CX-044 | path_resolver normalize_path_separators | Documented behavior (R5-S-075); DuckLake controls path generation. |
| R11-CX-045 | Missing active-name uniqueness constraints | By design — DuckLake follows DuckDB's snapshot-based visibility with application-level uniqueness. |

---

## Recommended Fix Agents

### Agent 1: Metadata Correctness
- **Findings**: R11-S-001, R11-S-004, R11-S-005, R11-S-006, R11-S-018, R11-S-022, R11-S-025
- **Summary**: Fix append_table_files row_id/stats, FOR UPDATE aggregate on PG, snapshot-aware column joins, file_format in replace/append, unchecked casts, default trait guards
- **Effort**: M total (1 M + 6 S)
- **Dependencies**: None

### Agent 2: Write Safety
- **Findings**: R11-S-002, R11-S-003, R11-S-009, R11-S-010, R11-S-013, R11-S-014, R11-S-019, R11-S-020, R11-S-021, R11-S-027, R11-S-030
- **Summary**: Fix orphan file cleanup, MERGE multi-source detection, NaN guard, timestamp/null-count overflow, checked arithmetic, OnceLock for compaction, unwrap->error
- **Effort**: M total (2 M + 9 S)
- **Dependencies**: None

### Agent 3: DuckDB Provider & Interop
- **Findings**: R11-S-007, R11-S-008, R11-S-011, R11-S-012, R11-S-028, R11-S-031
- **Summary**: Fix DuckDB error swallowing, schema qualification, add DF DELETE->DuckDB cross-engine test, add DF partition->DuckDB test, nullability in schema equality, TOCTOU in inlined reads
- **Effort**: M total (2 M + 4 S)
- **Dependencies**: None

### Agent 4: Test Infrastructure & Performance
- **Findings**: R11-S-015, R11-S-016, R11-S-017, R11-S-023, R11-S-024, R11-S-026, R11-S-029, R11-S-032, R11-S-033
- **Summary**: Fix O(n*m) lookups (precompute HashMaps), MERGE allocation, extract shared Parquet write helper, centralize test helpers, add R10 regression tests, tighten SLT patterns
- **Effort**: L total (3 M + 6 S)
- **Dependencies**: None (can run in parallel with all agents)

### Fix Priority Order
1. Agent 1 (metadata correctness) — highest impact P1s
2. Agent 2 (write safety) — correctness guards
3. Agent 3 (interop) — cross-engine confidence
4. Agent 4 (test/perf) — quality improvements

### P3 Items (defer or address opportunistically)
R11-S-034 through R11-S-045 are low priority. Can be addressed in a future cleanup pass.
