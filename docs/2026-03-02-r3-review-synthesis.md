# Code Review Synthesis — 2026-03-02 R3

## Overview
- Reviews: 5 (idiomatic, correctness, interop, test-harness, codex)
- Raw findings: 67
- Not a finding: 1 (R3-009 — correct Arc::clone usage)
- After deduplication: 50
- By priority: 3 P0, 9 P1, 16 P2, 22 P3
- **Fixed: 25 of 50** (all P0 + all P1 + 13 of 16 P2)
- **Open: 25** (3 P2 + 22 P3)

### Fix Agents (2026-03-03)

| Agent | Commit | Findings Fixed |
|-------|--------|---------------|
| Agent 1 — fix-sql-quoting | `065c411` | R3F-008 |
| Agent 2 — fix-inlined-types | (integration branch) | R3F-004, 005, 012, 018, 026, 027 |
| Agent 3 — fix-pg-mysql-parity | `d9d54ce` | R3F-006 |
| Agent 4 — fix-numeric-safety | `888705e` | R3F-009, 010, 025 |
| Agent 5 — fix-interop-critical | `3203d33` | R3F-001, 002, 003, 007, 011, 013, 014, 017 |
| Agent 6 — fix-test-harness | `3930b56` | R3F-015, 016, 020, 021, 023, 024 |

## Deduplication Notes

Key merges:
- `register_dml_files` missing `row_id_start`/`table_stats`: C3-001 + CX3-001
- MERGE orphan cleanup: C3-002 + CX3-002
- Date32/Date64 inlined data roundtrip: C3-003 + CX3-009
- Unchecked `as` casts (DML execs + supporting code): R3-001 + R3-003 + R3-005 + C3-004 + CX3-018
- `create_snapshot` schema_version: I-R3-02 + C3-005 (partial)
- DML snapshot_changes: I-R3-04 + CX3-010 + C3-005 (partial)
- PG/MySQL fix parity gap: CX3-004 + CX3-005 + CX3-006 + CX3-007
- `quote_identifier` consistency: CX3-008 + CX3-014 + C3-007 + C3-008
- Read-only test silent pass: F-TH-001 + CX3-026

### R2 Deferred Cross-Reference
- **F-036** (INSERT streaming): Not re-raised in R3.
- **F-044** (Provider/writer dedup): R3F-031 (DML boilerplate) is a narrower sub-case. Noted as related.
- **F-045** (Async trait redesign): Not re-raised in R3.

No R3 findings filtered — all are either genuinely new or narrower than R2 deferred items.

---

## Deduplicated Findings

### P0 — Critical

#### R3F-001: Missing `ducklake_table_column_stats` causes DuckDB crash **[FIXED]** (commit 3203d33)
- **Source reviews**: interop (I-R3-01)
- **File(s)**: `src/table_writer.rs`, all `metadata_writer_*.rs`
- **Description**: DuckDB's `GetGlobalTableStats` queries `ducklake_table_column_stats` and crashes with `INTERNAL Error: Calling GetValueInternal on a value that is NULL` when the table has NULL values. Our writer never populates this table during INSERT — only `ducklake_file_column_stats` (per-file stats) is written. DuckDB expects aggregate table-level column stats in `ducklake_table_column_stats`.
- **Impact**: **All DF-created catalogs are unreadable by DuckDB.** Complete interop blocker. 2 of 7 cross-engine tests fail.
- **Suggested fix**: After `register_column_stats`, compute and upsert aggregate stats into `ducklake_table_column_stats` for each column. Must include `contains_null` (non-NULL boolean), `min_value`, `max_value`.
- **Effort**: M
- **Fix group**: Agent 1 — Critical Interop

#### R3F-002: `register_dml_files` omits `row_id_start` and `table_stats` for new data files **[FIXED]** (commit 3203d33)
- **Source reviews**: correctness (C3-001), codex (CX3-001)
- **File(s)**: `metadata_writer_sqlite.rs:1100-1114`, `metadata_writer_postgres.rs:930`, `metadata_writer_mysql.rs:1029`
- **Description**: The `register_dml_files` method (UPDATE/MERGE new data files) inserts into `ducklake_data_file` WITHOUT `row_id_start` and does NOT update `ducklake_table_stats`. Compare with `register_data_file` which correctly reads `next_row_id`, sets `row_id_start`, and updates stats.
- **Impact**: (1) NULL `row_id_start` breaks delete-file position tracking for DML-created files. (2) `ducklake_table_stats` becomes stale. (3) Subsequent INSERTs may use stale `next_row_id`, risking overlapping row IDs.
- **Suggested fix**: Inside `register_dml_files`, for each data file: read current `next_row_id`, set `row_id_start`, update `ducklake_table_stats` (within existing transaction).
- **Effort**: S
- **Fix group**: Agent 1 — Critical Interop

#### R3F-003: MERGE execution has no orphaned file cleanup on metadata commit failure **[FIXED]** (commit 3203d33)
- **Source reviews**: correctness (C3-002), codex (CX3-002)
- **File(s)**: `merge_exec.rs:579-587`
- **Description**: When `register_dml_files` fails in the MERGE path, the error propagates without cleaning up already-uploaded Parquet files. DELETE exec and UPDATE exec both track `uploaded_files` and call `cleanup_orphaned_files` on failure. MERGE has neither.
- **Impact**: MERGE metadata commit failure permanently orphans delete files + data files in object storage.
- **Suggested fix**: Track uploaded file paths in `Vec<ObjectPath>` during MERGE loop; call `cleanup_orphaned_files` on failure.
- **Effort**: S
- **Fix group**: Agent 1 — Critical Interop

---

### P1 — High

#### R3F-004: Date32/Date64 inlined data roundtrip broken — silent data loss **[FIXED]**
- **Source reviews**: correctness (C3-003), codex (CX3-009)
- **File(s)**: `table_writer.rs:979-993` (write), `table_writer.rs:1127-1128` (flush read), `table.rs:1869-1870` (query read)
- **Description**: Write side serializes Date32 as ISO 8601 strings (`"2024-06-15"`). Read side uses `parse_primitive!(Date32Builder, values)` calling `.parse::<i32>()` on the date string — always fails. Query path: NULL (data loss). Flush path: error preventing flush.
- **Impact**: Inlined Date32/Date64 values silently become NULL; flush fails for Date columns.
- **Suggested fix**: Add Date32/Date64 handlers parsing ISO dates (e.g., `NaiveDate::parse_from_str`) or change write side to emit epoch-days values.
- **Effort**: S
- **Fix group**: Agent 2 — Inlined Data + Type Roundtrip

#### R3F-005: Timestamp inlined data roundtrip broken — silent data loss **[FIXED]**
- **Source reviews**: correctness (C3-006)
- **File(s)**: `table_writer.rs:995-1003` (write), `table.rs:1871` (read)
- **Description**: Write side uses Arrow's display formatter (`"2024-06-15T12:30:00"`). Read side uses `parse_primitive!(Int64Builder, values)` calling `.parse::<i64>()` — always fails. Same pattern as R3F-004 but for timestamps.
- **Impact**: Inlined Timestamp values silently become NULL in query results.
- **Suggested fix**: Add explicit Timestamp serialization (epoch-microseconds) and deserialization handlers.
- **Effort**: S
- **Fix group**: Agent 2 — Inlined Data + Type Roundtrip

#### R3F-006: PG/MySQL writers missing multiple fixes applied only to SQLite **[FIXED]** (commit d9d54ce)
- **Source reviews**: codex (CX3-004, CX3-005, CX3-006, CX3-007)
- **File(s)**: `metadata_writer_postgres.rs`, `metadata_writer_mysql.rs` (multiple locations)
- **Description**: R2 fixes F-012 (schema_versions), F-013 (column ID preservation), F-026 (UUID generation), and F-027 (changes_made format) were applied to the SQLite writer only. PostgreSQL and MySQL writers remain unfixed.
- **Sub-items**:
  - (a) PG/MySQL missing `schema_version` tracking and `ducklake_schema_versions` population
  - (b) PG/MySQL missing UUID generation on create paths
  - (c) PG/MySQL use wrong `changes_made` format (human-readable vs tokenized)
  - (d) PG/MySQL don't preserve column IDs for no-op schema writes
- **Impact**: PG/MySQL-backed catalogs have multiple interop failures with DuckDB. Column ID instability breaks Parquet field-ID mapping.
- **Suggested fix**: Port all 4 fix patterns from SQLite writer to PG/MySQL writers.
- **Effort**: M
- **Fix group**: Agent 3 — PG/MySQL Fix Parity

#### R3F-007: `create_snapshot()` doesn't inherit `schema_version` **[FIXED]** (commit 3203d33)
- **Source reviews**: interop (I-R3-02), correctness (C3-005 partial)
- **File(s)**: `metadata_writer_sqlite.rs:770-779` (and PG/MySQL equivalents)
- **Description**: The standalone `create_snapshot()` (used by DELETE/UPDATE/MERGE) relies on DDL default `schema_version INTEGER DEFAULT 1` instead of inheriting from previous snapshot. The `begin_write_transaction` path handles this correctly.
- **Impact**: DML snapshots may have incorrect `schema_version`, breaking DuckDB schema version resolution.
- **Suggested fix**: Query `MAX(schema_version) FROM ducklake_snapshot` in `create_snapshot()`.
- **Effort**: S
- **Fix group**: Agent 1 — Critical Interop

#### R3F-008: `quote_identifier` not applied consistently across inlined data paths **[FIXED]** (commit 065c411)
- **Source reviews**: codex (CX3-008, CX3-014), correctness (C3-007, C3-008)
- **File(s)**: `metadata_provider_sqlite.rs:997`, `metadata_provider_postgres.rs:1065`, `metadata_provider_mysql.rs:1004`, `metadata_writer_sqlite.rs:2253,2288,2311,2322,2381,2405,2448`
- **Description**: `count_inlined_rows()` in all 3 providers interpolates table names without `quote_identifier()`. Writer-side inlined data methods use raw `\"{}\"` instead of `quote_identifier()`. `PRAGMA table_info` uses single-quote wrapping.
- **Impact**: CX3-008 is an SQL injection vector via crafted catalog entries in the row-count path. Others are not exploitable (table names are `ducklake_inlined_data_{integer}`) but inconsistent with the F-001 convention.
- **Suggested fix**: Apply `quote_identifier()` to all dynamic identifiers in inlined data paths (read + write).
- **Effort**: S
- **Fix group**: Agent 5 — SQL Injection + Quoting

#### R3F-009: `.unwrap()` on downcasts in non-test production code **[FIXED]** (commit 888705e)
- **Source reviews**: idiomatic (R3-002)
- **File(s)**: `table_writer.rs:923-988`, `table_writer.rs:694`, `insert_exec.rs:593`
- **Description**: `arrow_array_value_to_string()` uses `.unwrap()` on every `downcast_ref` call (12+ occurrences). While the `match` on `DataType` makes it "logically safe," a schema mismatch, extension type, or dictionary-encoded array would panic. Also: `self.writer.as_mut().unwrap()` on potential None.
- **Impact**: Panics in production on unexpected array types or state.
- **Suggested fix**: Replace `.unwrap()` with `.ok_or_else(|| DuckLakeError::Internal(...))`.
- **Effort**: S
- **Fix group**: Agent 4 — Numeric Safety + Panic Prevention

#### R3F-010: Unchecked `as` casts across DML execs and supporting code **[FIXED]** (commit 888705e)
- **Source reviews**: idiomatic (R3-001, R3-003, R3-005), correctness (C3-004), codex (CX3-018)
- **File(s)**:
  - `merge_exec.rs:379,424,432,484,497,521,554,564`
  - `delete_exec.rs:283,303,311,312,353`
  - `update_exec.rs:327,355,363,364,423,468,478`
  - `virtual_column_exec.rs:218,226,256`
  - `table_writer.rs:485,608,1371`
  - `insert_exec.rs:263,711,803,888,901`
- **Description**: Extensive use of `num_rows as i64`, `buffer.len() as i64`, `positions.len() as i64`, etc. without overflow checks. Inconsistent with `delete_filter.rs` (which uses `i64::try_from()`) and `table_writer.rs:finish()` (which uses `i64::try_from(buffer.len())`).
- **Impact**: Theoretical data corruption on pathologically large batches. Inconsistency creates maintenance burden.
- **Suggested fix**: Replace `as i64`/`as i32`/`as u64` with `try_from()` throughout, matching existing patterns.
- **Effort**: M
- **Fix group**: Agent 4 — Numeric Safety + Panic Prevention

#### R3F-011: `next_catalog_id` and `next_file_id` never populated in snapshots **[FIXED]** (commit 3203d33)
- **Source reviews**: interop (I-R3-03)
- **File(s)**: All `metadata_writer_*.rs`
- **Description**: Only snapshot 0 has `next_catalog_id=0, next_file_id=0`. All subsequent snapshots default to 0. DuckDB uses these for ID allocation.
- **Impact**: DuckDB writing to DF-created catalogs may allocate conflicting IDs. Also affects catalog validation.
- **Suggested fix**: Track and update per snapshot: `next_catalog_id = MAX(schema_id, table_id, view_id) + 1`, `next_file_id = MAX(data_file_id, delete_file_id) + 1`.
- **Effort**: M
- **Fix group**: Agent 1 — Critical Interop

#### R3F-012: Timestamp non-UTC timezone silently replaced with UTC on roundtrip **[FIXED]**
- **Source reviews**: codex (CX3-003)
- **File(s)**: `types.rs:138-146` (arrow_to_ducklake_type), `types.rs:60-72` (ducklake_to_arrow_type)
- **Description**: `arrow_to_ducklake_type` maps `Timestamp(_, Some("America/New_York"))` to `"timestamptz"`. Roundtrip maps back to `Timestamp(Microsecond, Some("UTC"))`. Non-UTC timezone information is silently lost.
- **Impact**: Data corruption for non-UTC-normalized timestamp columns during schema evolution or cross-engine writes.
- **Suggested fix**: Either preserve timezone in type string or document that DuckLake normalizes all timestamps to UTC.
- **Effort**: M
- **Fix group**: Agent 2 — Inlined Data + Type Roundtrip

---

### P2 — Medium

#### R3F-013: DML snapshots missing `snapshot_changes` records **[FIXED]** (commit 3203d33)
- **Source reviews**: correctness (C3-005 partial), interop (I-R3-04), codex (CX3-010)
- **File(s)**: `delete_exec.rs`, `update_exec.rs`, `merge_exec.rs`, `insert_exec.rs`
- **Description**: DML operations don't record `changes_made` entries in `ducklake_snapshot_changes`. DDL operations do this correctly.
- **Impact**: DuckDB's `ducklake_table_changes()` function and CDC tracking incomplete for DML snapshots.
- **Suggested fix**: Record appropriate `changes_made` strings: `inserted_into_table:{id}`, `deleted_from_table:{id}`, `updated_table:{id}`, `merged_into_table:{id}`.
- **Effort**: S
- **Fix group**: Agent 1 — Critical Interop

#### R3F-014: Missing `created_schema` change tracking **[FIXED]** (commit 3203d33)
- **Source reviews**: interop (I-R3-05)
- **File(s)**: `metadata_writer_sqlite.rs:781-825`, all writer backends
- **Description**: Schema creation doesn't record `created_schema:"name"` in `snapshot_changes`. DuckDB creates separate snapshots with this entry.
- **Impact**: Incomplete audit trail.
- **Suggested fix**: Add `created_schema:"name"` change entry on schema creation.
- **Effort**: S
- **Fix group**: Agent 1 — Critical Interop

#### R3F-015: `batches_to_strings` semantic divergence across test files **[FIXED]** (commit 3930b56)
- **Source reviews**: test-harness (F-TH-002)
- **File(s)**: `tests/common/test_utils.rs:185`, ~6 test files with local copies
- **Description**: Shared version filters virtual columns; local copies do not. Tests using shared version get auto-filtered results; local copies include virtual columns. Inconsistent partial migration.
- **Impact**: Test migration could expose hidden assertion failures or cause unexpected passes.
- **Suggested fix**: Standardize: either migrate all to shared helper or create explicit `batches_to_strings_raw` and `batches_to_strings_filtered` variants.
- **Effort**: M
- **Fix group**: Agent 6 — Test Harness

#### R3F-016: Read-only write test silently passes if write succeeds **[FIXED]** (commit 3930b56)
- **Source reviews**: test-harness (F-TH-001), codex (CX3-026)
- **File(s)**: `tests/sql_write_tests.rs:256,269-272`
- **Description**: `test_insert_into_read_only_fails` has `Ok(_) => { /* acceptable during development */ }` arm. If write guard breaks, test passes silently.
- **Impact**: Write-to-read-only regression goes undetected.
- **Suggested fix**: Change `Ok(_)` arm to `panic!("Expected error for read-only insert, but it succeeded")`.
- **Effort**: S
- **Fix group**: Agent 6 — Test Harness

#### R3F-017: `set_data_path` is not atomic (DELETE + INSERT without transaction) **[FIXED]** (commit 3203d33)
- **Source reviews**: codex (CX3-011)
- **File(s)**: `metadata_writer_sqlite.rs:1138-1153`
- **Description**: Executes DELETE then INSERT on `ducklake_metadata` without a transaction. Crash between operations leaves catalog with missing `data_path`.
- **Impact**: Catalog becomes unusable on crash during `set_data_path`.
- **Suggested fix**: Wrap in a transaction.
- **Effort**: S
- **Fix group**: Agent 1 — Critical Interop

#### R3F-018: `table_deletions` returns wrong column order for reordered full projections **[FIXED]**
- **Source reviews**: codex (CX3-012)
- **File(s)**: `table_deletions.rs:125,173,233`
- **Description**: When all columns are requested in non-natural order, `build_exec_for_delete_entry` disables Parquet projection (`None`), returning columns in natural order. Reordering may be skipped.
- **Impact**: CDC deletion queries can return columns in wrong order.
- **Suggested fix**: Apply projection even for full-column requests when order differs.
- **Effort**: M
- **Fix group**: Agent 2 — Inlined Data + Type Roundtrip (or standalone)

#### R3F-019: `get_table_structure` is not snapshot-aware in table functions **[OPEN]**
- **Source reviews**: codex (CX3-013)
- **File(s)**: `table_functions.rs:331-333`
- **Description**: `resolve_table_for_function()` pins schema/table lookup with `snapshot_id`, but `get_table_structure(table_id)` has no snapshot parameter — returns current-version columns. Historical queries see wrong schema after evolution.
- **Impact**: Table function results inconsistent after schema changes.
- **Suggested fix**: Add snapshot parameter to `get_table_structure` (trait change required).
- **Effort**: M
- **Fix group**: Deferred (trait change)

#### R3F-020: ORDER BY ALL rewriting incorrect for multi-line SQL in SLT runner **[FIXED]** (commit 3930b56)
- **Source reviews**: codex (CX3-019)
- **File(s)**: `sqllogictest_runner.rs:265,270`
- **Description**: Detection uses full multi-line preview but rewriting only mutates first SQL line. `ORDER BY ALL` on later lines is not removed but directive changes to `rowsort`.
- **Impact**: Invalid transformed SLT tests.
- **Suggested fix**: Apply rewriting to the correct line containing `ORDER BY ALL`.
- **Effort**: M
- **Fix group**: Agent 6 — Test Harness

#### R3F-021: Incomplete migration to shared test helpers **[FIXED]** (commit 3930b56)
- **Source reviews**: test-harness (F-TH-007)
- **File(s)**: ~10 test files with local `df_query()`, `assert_results_eq()` copies
- **Description**: `test_utils.rs` was introduced after many tests existed. Migration incomplete — files import value converters but keep local copies of higher-level helpers with subtle differences.
- **Impact**: Maintenance burden; subtle divergence risk.
- **Suggested fix**: Complete migration to shared helpers.
- **Effort**: M
- **Fix group**: Agent 6 — Test Harness

#### R3F-022: Missing test coverage areas **[OPEN]**
- **Source reviews**: test-harness (F-TH-005)
- **Description**: No tests for: (1) DROP TABLE via SQL verifying table is gone, (2) concurrent writes with conflict detection, (3) DELETE with multiple delete files per data file, (4) schema evolution reading via DataFusion (only DuckDB CLI), (5) >100 files, (6) unit tests for SLT `preprocess_test_file()`.
- **Impact**: Coverage gaps in edge cases.
- **Effort**: M (aggregate)
- **Fix group**: Deferred (informational)

#### R3F-023: `statement error` → `statement ok` conversion visibility **[FIXED]** (commit 3930b56)
- **Source reviews**: test-harness (F-TH-004)
- **File(s)**: `sqllogictest_runner.rs:362,737`
- **Description**: Conversion patterns `eprintln!` to stderr, not captured by test framework. Should use `log::warn!` or accumulate conversion counts.
- **Impact**: Reduced visibility of error-path conversion decisions.
- **Suggested fix**: Use structured logging or summary counts.
- **Effort**: S
- **Fix group**: Agent 6 — Test Harness

#### R3F-024: `test_double_slash_in_various_positions` test failure **[FIXED]** (commit 3930b56)
- **Source reviews**: test-harness (F-TH-012)
- **File(s)**: `tests/adversarial_storage_tests.rs:173`
- **Description**: Test expects double slashes preserved but path resolver normalizes them. Either test expectation or code behavior needs updating.
- **Impact**: 1 test failure.
- **Suggested fix**: Update test expectation to match current normalization behavior.
- **Effort**: S
- **Fix group**: Agent 6 — Test Harness

#### R3F-025: Dead variables with underscore prefix (unused allocations) **[FIXED]** (commit 888705e)
- **Source reviews**: idiomatic (R3-004)
- **File(s)**: `merge_exec.rs:307-308`, `update_exec.rs:221-222`, `metadata_provider_duckdb.rs:247,574`, `table_functions.rs:294`
- **Description**: Variables cloned/fetched but never used. Underscore prefix suppresses warning but clones still allocate.
- **Impact**: Minor wasted allocations; code clarity.
- **Suggested fix**: Remove dead clones or use them for logging.
- **Effort**: S
- **Fix group**: Agent 4 — Numeric Safety + Panic Prevention

#### R3F-026: `Date64` → `"date"` → `Date32` lossy roundtrip in type system **[FIXED]**
- **Source reviews**: codex (CX3-015)
- **File(s)**: `types.rs:127,58`
- **Description**: Both `Date32` and `Date64` map to `"date"`, which maps back to `Date32`. Date64 (milliseconds) loses precision → Date32 (days).
- **Impact**: Lossy for Date64 columns.
- **Suggested fix**: Map Date64 to a distinct type string (e.g., `"date_ms"`) or document the lossy behavior.
- **Effort**: S
- **Fix group**: Agent 2 — Inlined Data + Type Roundtrip

#### R3F-027: `Interval` variant lost on roundtrip **[FIXED]**
- **Source reviews**: codex (CX3-016)
- **File(s)**: `types.rs:147,76`
- **Description**: All `Interval` variants (`YearMonth`, `DayTime`, `MonthDayNano`) map to `"interval"` → `Interval(MonthDayNano)`. Lossy for non-MonthDayNano.
- **Impact**: Lossy for YearMonth/DayTime intervals.
- **Suggested fix**: Map each variant to a distinct type string or document lossy behavior.
- **Effort**: S
- **Fix group**: Agent 2 — Inlined Data + Type Roundtrip

#### R3F-028: Extra columns in catalog tables vs DuckDB reference **[OPEN]**
- **Source reviews**: interop (I-R3-06)
- **File(s)**: All `metadata_writer_*.rs` DDL sections
- **Description**: Our DDL includes 5 columns not in DuckDB's schema: `default_value_type`, `default_value_dialect` (ducklake_column), `partial_max` (data_file + delete_file), `table_id` (schema_versions).
- **Impact**: Low — DuckDB ignores unknown columns. May cause issues if DuckDB validates strictly in future.
- **Suggested fix**: Remove if unused, or document as DataFusion extensions.
- **Effort**: S
- **Fix group**: Deferred (low risk)

---

### P3 — Low

#### R3F-029: `parse_decimal` silently ignores trailing garbage after closing parenthesis **[OPEN]**
- **Source reviews**: codex (CX3-017)
- **File(s)**: `types.rs:255-259`
- **Description**: `"decimal(10,2)extra_garbage"` parses successfully as `Decimal128(10,2)`.
- **Effort**: S

#### R3F-030: `parse_string_to_array` fallback silently downcasts unsupported types to string **[OPEN]**
- **Source reviews**: correctness (C3-009)
- **File(s)**: `table_writer.rs:1129-1139`
- **Description**: Unrecognized types (Decimal128, Binary, etc.) stored as `StringBuilder` — schema mismatch on `RecordBatch::try_new`.
- **Effort**: S

#### R3F-031: Duplicated Parquet write + delete-file boilerplate across DML execs **[OPEN]**
- **Source reviews**: idiomatic (R3-006)
- **File(s)**: `delete_exec.rs`, `update_exec.rs`, `merge_exec.rs`
- **Description**: ~40-50 lines identical boilerplate per exec. Related to R2 deferred F-044 (provider/writer dedup).
- **Effort**: M

#### R3F-032: Empty snapshots created when DML affects zero rows **[OPEN]**
- **Source reviews**: codex (CX3-020)
- **File(s)**: `delete_exec.rs:199-201`, `update_exec.rs:232-234`, `merge_exec.rs:317-319`
- **Description**: `create_snapshot()` called unconditionally, even for zero-match WHERE clauses.
- **Effort**: S

#### R3F-033: MERGE source rows can match multiple target rows without error **[OPEN]**
- **Source reviews**: codex (CX3-021)
- **File(s)**: `merge_exec.rs:388-421`
- **Description**: SQL standard MERGE should error when a source row matches multiple targets. Ours silently performs multiple deletes/updates.
- **Effort**: M

#### R3F-034: `delete_count` metadata tracks delta, not total positions in delete file **[OPEN]**
- **Source reviews**: codex (CX3-022)
- **File(s)**: `delete_exec.rs:311`, `update_exec.rs:434`, `merge_exec.rs:493-498`
- **Description**: `delete_count` set to new deletion count, but file includes merged existing positions. Metadata understates total.
- **Effort**: S

#### R3F-035: `CoalescePartitionsExec` wrapping fragile across `with_new_children` **[OPEN]**
- **Source reviews**: codex (CX3-023)
- **File(s)**: `virtual_column_exec.rs:88-96,153-168`
- **Description**: `with_new_children()` calls `new()` on already-coalesced child. Works now but fragile if optimizer inserts repartitioning.
- **Effort**: S

#### R3F-036: `changes_made` format doesn't escape quotes in schema/table names **[OPEN]**
- **Source reviews**: codex (CX3-024)
- **File(s)**: `metadata_writer_sqlite.rs:550`
- **Description**: `format!("created_table:\"{}\".\"{}\"")` doesn't escape internal double-quotes.
- **Effort**: S

#### R3F-037: `rewrite_unqualified_tables` is a no-op wasting allocations **[OPEN]**
- **Source reviews**: codex (CX3-025)
- **File(s)**: `sqllogictest_runner.rs:604-612`
- **Description**: Returns `line.to_string()` without transformation, called for every non-directive line.
- **Effort**: S

#### R3F-038: DuckDB error handling inconsistency for missing `data_path` **[OPEN]**
- **Source reviews**: codex (CX3-027)
- **File(s)**: `metadata_provider_duckdb.rs:119`
- **Description**: SQLx providers return `InvalidConfig`; DuckDB provider bubbles raw `QueryReturnedNoRows`.
- **Effort**: S

#### R3F-039: Bare `decimal`/`numeric` without parameters produces misleading error **[OPEN]**
- **Source reviews**: codex (CX3-028)
- **File(s)**: `types.rs:242-253,93-99`
- **Description**: `"decimal"` falls to unsupported-type error. Should default to `Decimal128(18,0)` or give descriptive error.
- **Effort**: S

#### R3F-040: `information_schema.rs` `table_exist()` allocates on every call **[OPEN]**
- **Source reviews**: idiomatic (R3-010)
- **File(s)**: `information_schema.rs:820-822`
- **Description**: `table_names()` builds `Vec<String>` just for membership check. Use `matches!()` instead.
- **Effort**: S

#### R3F-041: `Arc::clone` vs `.clone()` inconsistency on Arc types **[OPEN]**
- **Source reviews**: idiomatic (R3-008)
- **Description**: Codebase mostly uses `Arc::clone(&x)` but scattered `.clone()` on Arc types in `information_schema.rs`.
- **Effort**: S

#### R3F-042: `information_schema.rs` TableProvider boilerplate **[OPEN]**
- **Source reviews**: idiomatic (R3-007)
- **File(s)**: `information_schema.rs`
- **Description**: 5-6 TableProvider impls share identical `scan()`, `as_any()`, `schema()`, `table_type()` bodies.
- **Effort**: S

#### R3F-043: `get_int_column()` duplicated in 3 test files **[OPEN]**
- **Source reviews**: test-harness (F-TH-008)
- **File(s)**: `delete_filter_tests.rs`, `concurrent_tests.rs`, `renamed_columns_tests.rs`
- **Description**: Identical helper not in `test_utils.rs`.
- **Effort**: S

#### R3F-044: Third `convert_batch_to_strings` variant in `hybrid_asyncdb.rs` **[OPEN]**
- **Source reviews**: test-harness (F-TH-003)
- **File(s)**: `hybrid_asyncdb.rs:499`
- **Description**: SLT-specific formatting (float compat). Intentionally different but could drift.
- **Effort**: S

#### R3F-045: `test_utils` feature gate could break without `metadata-duckdb` **[OPEN]**
- **Source reviews**: test-harness (F-TH-009)
- **File(s)**: `tests/common/mod.rs`, `tests/common/test_utils.rs`
- **Description**: `test_utils.rs` imports from `duckdb::types::Value`. Only safe because all tests requiring it also enable `metadata-duckdb`.
- **Effort**: S

#### R3F-046: `WITH ... INSERT` not caught by SLT write statement detection **[OPEN]**
- **Source reviews**: test-harness (F-TH-010)
- **File(s)**: `hybrid_asyncdb.rs:117-143`
- **Description**: CTE-based inserts starting with `WITH` routed to DataFusion instead of DuckDB. Unlikely in SLT tests.
- **Effort**: S

#### R3F-047: Transaction-mode DuckDB formatting may differ from DataFusion **[OPEN]**
- **Source reviews**: test-harness (F-TH-011)
- **File(s)**: `hybrid_asyncdb.rs:378-428`
- **Description**: In-transaction reads routed to DuckDB with independent formatting; potential subtle SLT mismatches.
- **Effort**: S

#### R3F-048: Decimal type string spacing difference **[OPEN]**
- **Source reviews**: interop (I-R3-08)
- **File(s)**: `types.rs:159`
- **Description**: Ours: `"decimal(10, 2)"` (with space). DuckDB: `"decimal(10,2)"` (no space).
- **Suggested fix**: Remove space in format string.
- **Effort**: S

#### R3F-049: Extra non-standard tables in DDL (informational) **[OPEN]**
- **Source reviews**: interop (I-R3-07)
- **Description**: 7 tables not in DuckDB reference: `_df_change_tracking`, `ducklake_file_variant_stats`, `ducklake_macro*`, `ducklake_sort*`. DuckDB ignores unknown tables.
- **Effort**: N/A

#### R3F-050: 5/7 `sql_write_tests` are `#[ignore]`d (informational) **[OPEN]**
- **Source reviews**: test-harness (F-TH-006)
- **Description**: All share root cause of virtual columns causing schema mismatch. Well-documented but reduces SQL write path test coverage.
- **Effort**: M (root cause fix)

---

## Recommended Fix Agents

### Agent 1: Critical Interop + DML Metadata (P0 + related P1/P2)
- **Findings**: R3F-001, R3F-002, R3F-003, R3F-007, R3F-011, R3F-013, R3F-014, R3F-017
- **Estimated effort**: L (aggregate)
- **Files**: All `metadata_writer_*.rs`, `merge_exec.rs`, `delete_exec.rs`, `update_exec.rs`, `insert_exec.rs`, `table_writer.rs`
- **Description**: Populate `ducklake_table_column_stats`, fix `register_dml_files` (row_id_start + table_stats), add MERGE orphan cleanup, inherit schema_version in create_snapshot, populate next_catalog_id/next_file_id, add DML snapshot_changes, add created_schema tracking, make set_data_path atomic.

### Agent 2: Inlined Data + Type Roundtrip (P1 + P2)
- **Findings**: R3F-004, R3F-005, R3F-012, R3F-018, R3F-026, R3F-027
- **Estimated effort**: M
- **Files**: `table_writer.rs`, `table.rs`, `types.rs`, `table_deletions.rs`
- **Description**: Fix Date32/Date64 and Timestamp inlined data serialization/deserialization, fix non-UTC timezone handling, fix table_deletions projection, fix Date64 and Interval lossy roundtrips.

### Agent 3: PG/MySQL Fix Parity (P1)
- **Findings**: R3F-006 (4 sub-items)
- **Estimated effort**: M
- **Files**: `metadata_writer_postgres.rs`, `metadata_writer_mysql.rs`
- **Description**: Port R2 fixes F-012 (schema_versions), F-013 (column ID preservation), F-026 (UUID generation), F-027 (changes_made format) from SQLite writer to PG/MySQL.

### Agent 4: Numeric Safety + Panic Prevention (P1)
- **Findings**: R3F-009, R3F-010, R3F-025
- **Estimated effort**: M
- **Files**: `merge_exec.rs`, `delete_exec.rs`, `update_exec.rs`, `virtual_column_exec.rs`, `table_writer.rs`, `insert_exec.rs`
- **Description**: Replace all unchecked `as` casts with `try_from()`, replace `.unwrap()` on downcasts with error returns, remove dead variables.

### Agent 5: SQL Quoting Consistency (P1)
- **Findings**: R3F-008
- **Estimated effort**: S
- **Files**: All `metadata_provider_*.rs`, `metadata_writer_sqlite.rs`
- **Description**: Apply `quote_identifier()` to all dynamic identifiers in inlined data paths (providers + writer).

### Agent 6: Test Harness Fixes (P2)
- **Findings**: R3F-015, R3F-016, R3F-020, R3F-021, R3F-023, R3F-024
- **Estimated effort**: M
- **Files**: `tests/sql_write_tests.rs`, `tests/common/test_utils.rs`, `tests/sqllogictest_runner.rs`, `tests/adversarial_storage_tests.rs`, various `tests/cross_engine_*.rs`
- **Description**: Standardize batches_to_strings, fix read-only test assertion, fix ORDER BY ALL multi-line rewriting, complete test helper migration, improve statement-conversion logging, fix double-slash test.

---

## Priority Summary

| Priority | Count | Fixed | Open | Key Themes |
|----------|-------|-------|------|-----------|
| P0 | 3 | 3 | 0 | Table column stats crash, DML row_id_start, MERGE orphan cleanup |
| P1 | 9 | 9 | 0 | Inlined data roundtrip, PG/MySQL fix parity, schema_version, quoting, casts, panics |
| P2 | 16 | 13 | 3 | Snapshot changes, test harness, type lossy roundtrips, atomicity gaps |
| P3 | 22 | 0 | 22 | Code quality, minor consistency, informational items |
| **Total** | **50** | **25** | **25** | |

## Cross-Cutting Observations

### 1. Fix Parity Gap: SQLite vs PostgreSQL/MySQL
The most systemic issue. Fixes F-011 (row_id_start), F-012 (schema_versions), F-013 (column IDs), F-026 (UUIDs), and F-027 (changes_made) were applied to SQLite only. PG/MySQL writers have 4+ interop regressions.

### 2. DML vs INSERT Path Divergence
`register_dml_files` (DELETE/UPDATE/MERGE) diverges from `register_data_file` (INSERT):
- No `row_id_start` (R3F-002)
- No `table_stats` update (R3F-002)
- No `snapshot_changes` records (R3F-013)
- No orphan cleanup in MERGE (R3F-003)

### 3. Inlined Data Serialization Fragility
Date32, Date64, and Timestamp types serialize as human-readable strings but parse back as numeric epoch values, causing silent data loss (R3F-004, R3F-005).

### 4. Inconsistent Numeric Safety Patterns
`delete_filter.rs` and `table_writer.rs:finish()` use `try_from()` guards, but DML execs use unchecked `as` casts extensively (R3F-010). ~30 locations need updating.

## Notes

- R3 is the third review cycle after two rounds of fixes in R2.
- All R2 deferred items (F-036, F-044, F-045) remain deferred; none re-raised as new findings.
- R3F-031 (DML boilerplate) is related to F-044 but is a narrower sub-case.
- The P0 finding R3F-001 (table_column_stats) is the single highest-impact issue — it blocks ALL DF→DuckDB interop for write operations.
