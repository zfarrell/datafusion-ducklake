# Code Review Synthesis — 2026-03-03 R5

## Overview

- **Reviews**: 5 (idiomatic, correctness, interop, test-harness, codex)
- **Raw findings**: 95 (17 + 14 + 17 + 8 + 39)
- **After deduplication**: 77
- **By priority**: 0 P0, 11 P1, 28 P2, 38 P3
- **Known deferred excluded**: 3 (I-007, I-009, I-017 — all F-044 territory)
- **Excluded as non-findings**: 5 (correctness review INFO/correct items)
- **False positives excluded**: 1 (CX-007 — NOT NULL enforcement already in insert_exec.rs:226)
- **Merged duplicates**: 7 merge groups removing 9 findings

## P0 Validation (Codex Claims)

The codex review reported 4 P0 findings. All 4 were validated against actual source code and **downgraded**:

| Codex ID | Claim | Actual Code | Verdict |
|----------|-------|-------------|---------|
| CX-001 | Replace mode drops data if register_data_file fails after end_table_files | Both calls are within the same metadata writer transaction. If register_data_file fails, the transaction is never committed, rolling back end_table_files. | **P2** — defense-in-depth improvement but not data loss |
| CX-002 | Inlining flush `if let Ok(...)` swallows error, then clears inline data | Confirmed at table_writer.rs:321-351. Error IS swallowed, and clear_inlined_data IS called after. Real data loss if get_inlined_data_as_batch fails. | **P1** — real but requires specific failure mode |
| CX-003 | Date32 partition pruning uses epoch-days vs ISO date strings | Confirmed at table.rs:1269. Both scalar_value_to_partition_string AND parse_partition_value_to_scalar use epoch-days, so DF-to-DF is consistent. But DuckDB uses ISO dates. | **P1** — cross-engine partition pruning broken for Date types |
| CX-004 | normalize_value creates false positives | Confirmed at test_utils.rs:284. Parses all numerics through f64. Already found by R5-TH-002. | **P2** — test infrastructure, not production code |

## Deduplication Notes

### Cross-Review Merges (7 groups)

1. **I-002 + CX-023**: Unknown types silently → string/Utf8 (idiomatic + codex both found)
2. **CX-004 + R5-TH-002**: normalize_value over-normalizes (codex + test-harness)
3. **CX-019 + R5-TH-004**: Zero-row DML returns StatementComplete (codex + test-harness)
4. **CX-024 + IO-015**: MySQL VARCHAR limits truncation (codex + interop)
5. **R5-S-010 + CX-017**: Table function name parsing fragile (correctness + codex)
6. **IO-006 + IO-007**: Inlined Date/Timestamp serialization (both interop, same root cause)
7. **IO-001 + IO-003 + IO-004 + IO-005**: Cross-engine test coverage gaps (all interop, same theme)

### Excluded Known Deferred Items

- **I-007** (DDL constant style inconsistency across writers) → F-044 territory
- **I-009** (SQL duplication across providers) → F-044 territory
- **I-017** (last_insert_id duplication in MySQL) → F-044 territory

### False Positive

- **CX-007** (Insert path doesn't enforce NOT NULL): `validate_not_null_constraints` IS called at `insert_exec.rs:226`. This was fixed in R4 (R4-S-011). Codex finding is stale.

---

## Deduplicated Findings

### P0 — Critical

**None.** All 4 codex P0 claims downgraded after source code validation (see table above).

---

### P1 — Major (11 findings)

#### R5-S-001: Lexicographic MIN/MAX on VARCHAR column stats
- **Source**: correctness
- **Files**: `src/metadata_writer_sqlite.rs` (register_column_stats SQL)
- **Description**: Table-level column stats aggregation uses SQL `MIN()`/`MAX()` on VARCHAR-typed min_value/max_value columns. For numeric columns, this produces lexicographically-correct but numerically-wrong bounds (e.g., `MIN('9','10') = '10'`). DuckDB reading our catalog could use these wrong stats for row group pruning, potentially dropping matching rows.
- **Fix**: Cast to native type before aggregating, or aggregate in application code with type-aware parsing
- **Effort**: M

#### R5-S-002: replace_table_files doesn't update table_stats after compaction
- **Source**: correctness (R5-S-004 in original)
- **Files**: `src/metadata_writer_sqlite.rs` — `replace_table_files` method
- **Description**: After compaction replaces data files, `ducklake_table_stats` (record_count, file_size_bytes, next_row_id) is not recalculated. Stale counts affect COUNT(*) optimization and can cause row ID collisions in subsequent inserts.
- **Fix**: Recalculate table_stats from new files after replacement
- **Effort**: M

#### R5-S-003: contains_null inconsistency in PG/MySQL ALTER TABLE ADD COLUMN
- **Source**: idiomatic (I-001)
- **Files**: `src/metadata_writer_postgres.rs:1845`, `src/metadata_writer_mysql.rs:1972`
- **Description**: SQLite writer correctly sets `contains_null = 1` (per R3F-001) when adding a column. PG and MySQL writers set `contains_null = NULL`. Existing rows have NULL for the new column, so contains_null must be TRUE. DuckDB crashes when reading NULL contains_null from the catalog.
- **Fix**: Change PG/MySQL to use `TRUE`/`1` for contains_null in InsertColumn branch
- **Effort**: S

#### R5-S-004: Inlining flush path silently loses existing inline rows
- **Source**: codex (CX-002, downgraded from P0)
- **Files**: `src/table_writer.rs:321-329`
- **Description**: `get_inlined_data_as_batch` errors swallowed by `if let Ok(...)`, but `clear_inlined_data` still runs after the Parquet write. If the inline-to-batch conversion fails, existing inline rows are cleared without being included in the written file.
- **Fix**: Propagate the error from `get_inlined_data_as_batch` instead of swallowing it; fail the operation before writing/clearing
- **Effort**: S

#### R5-S-005: Date32/Date64 partition pruning uses epoch-days, DuckDB uses ISO strings
- **Source**: codex (CX-003, downgraded from P0)
- **Files**: `src/table.rs:1269-1270` (`scalar_value_to_partition_string`)
- **Description**: Date32/Date64 filter values are converted to raw epoch-day/ms integers for partition matching. DuckDB stores partition values as ISO date strings (e.g., "2024-01-15"). Cross-engine partition pruning incorrectly excludes matching files, dropping rows.
- **Fix**: Format date scalars as ISO 8601 strings (`YYYY-MM-DD`) in `scalar_value_to_partition_string`. Also update `parse_partition_value_to_scalar` to parse ISO dates.
- **Effort**: S

#### R5-S-006: UPDATE can panic on invalid assignment column index
- **Source**: codex (CX-008)
- **Files**: `src/update_exec.rs:374`
- **Description**: `columns[*col_idx]` without bounds check. If planner produces an invalid column index, this panics at runtime instead of returning an error.
- **Fix**: Validate `column_index < schema.fields().len()` during plan construction and return `DataFusionError::Plan`
- **Effort**: S

#### R5-S-007: Delete-delta boundary uses BETWEEN (inclusive) instead of strict lower bound
- **Source**: codex (CX-013)
- **Files**: `src/metadata_provider.rs:174` (`SQL_GET_DELETE_FILES_ADDED_BETWEEN_SNAPSHOTS`)
- **Description**: The shared SQL constant uses `df.begin_snapshot BETWEEN p.start_snapshot AND p.finish_snapshot` (inclusive both bounds), while `SQL_GET_DATA_FILES_ADDED_BETWEEN_SNAPSHOTS` uses `data.begin_snapshot > ? AND data.begin_snapshot <= ?` (strict lower). This inconsistency can double-report delete file changes at snapshot boundaries.
- **Fix**: Change to `df.begin_snapshot > p.start_snapshot AND df.begin_snapshot <= p.finish_snapshot`
- **Effort**: S

#### R5-S-008: statistics() sized to base columns, schema() returns full_schema with virtuals
- **Source**: codex (CX-015)
- **Files**: `src/table.rs:1289-1311`
- **Description**: `schema()` returns `full_schema` (base + virtual columns), but `statistics()` builds column_statistics only for `self.columns.len()` (base columns). DataFusion expects statistics vector to match schema column count. Misalignment may cause optimizer panics or incorrect cost estimates.
- **Fix**: Append `ColumnStatistics::new_unknown()` for each virtual column
- **Effort**: S

#### R5-S-009: strip_prefix can mis-trim paths in ducklake_list_files
- **Source**: codex (CX-016)
- **Files**: `src/table_functions.rs:150-153`, `:167-169`
- **Description**: `strip_prefix(&data_path)` on full paths matches string prefixes, not path prefixes. `/data` strips from `/database/file.parquet` → `base/file.parquet`. Returns incorrect file paths.
- **Fix**: Only strip when path starts with `data_path + "/"` after canonicalization
- **Effort**: S

#### R5-S-010: Decimal128 negative sign loss when whole part is zero (test formatter)
- **Source**: test-harness (R5-TH-001)
- **Files**: `tests/hybrid_asyncdb.rs:604-609`, `tests/common/test_utils.rs:169-174`
- **Description**: For Decimal128 values in (-1, 0), truncating integer division `raw / divisor` produces `whole = 0`, losing the negative sign. `-0.45` renders as `"0.45"`. Affects test correctness for all negative sub-unit decimals.
- **Fix**: Check `raw < 0 && whole == 0` and prepend `"-"`
- **Effort**: S

#### R5-S-011: Missing cross-engine tests for DML, ALTER TABLE, partitions, and complex types
- **Source**: interop (IO-001, IO-003, IO-004, IO-005)
- **Files**: `tests/cross_engine_tests.rs`
- **Description**: Cross-engine test suite only covers INSERT + SELECT. No tests for: DELETE, UPDATE, MERGE (DML), ALTER TABLE ADD/DROP COLUMN, partitioned tables, or TIMESTAMP/DATE/DECIMAL types. These are the highest-risk interop scenarios.
- **Fix**: Add comprehensive cross-engine tests for each DML operation, ALTER TABLE, partitions, and complex types in both DF→DuckDB and DuckDB→DF directions
- **Effort**: L

---

### P2 — Moderate (28 findings)

#### R5-S-012: Unknown types silently converted to string/Utf8
- **Source**: idiomatic (I-002) + codex (CX-023)
- **Files**: `src/table_writer.rs` (`parse_inlined_column`, `flush_inlined_data`)
- **Description**: Unrecognized DuckLake types silently fall through to StringArray or `unwrap_or(Utf8)`. STRUCT/MAP columns become strings with no warning, producing wrong physical schema.
- **Fix**: Return error on unknown types instead of defaulting
- **Effort**: S

#### R5-S-013: schema_version column type INTEGER in PG/MySQL, DuckDB uses BIGINT
- **Source**: interop (IO-002)
- **Files**: `src/metadata_writer_postgres.rs:30`, `src/metadata_writer_mysql.rs:32`
- **Description**: PostgreSQL/MySQL `schema_version INTEGER` is 4 bytes (max ~2.1B), DuckDB uses BIGINT. SQLite is fine (8-byte INTEGER). Theoretical overflow with extremely long-running catalogs.
- **Fix**: Change to BIGINT in PG/MySQL DDL
- **Effort**: S

#### R5-S-014: Inlined Date/Timestamp serialization uses epoch integers
- **Source**: interop (IO-006, IO-007)
- **Files**: `src/table_writer.rs:1016-1025`
- **Description**: Date32 serialized as epoch-days integer ("19889"), Timestamp as epoch-microseconds ("1718451000000000"). DuckDB expects ISO strings ("2024-06-15", "2024-06-15 12:30:00"). Cross-engine inlined data is broken for date/time types.
- **Fix**: Serialize as ISO 8601 strings
- **Effort**: S

#### R5-S-015: Inlined data flush fails for Decimal types
- **Source**: interop (IO-008)
- **Files**: `src/table_writer.rs:1196-1201`
- **Description**: Decimal128/Decimal256 reach fallback arm in `parse_string_to_array` and return UnsupportedType error. Tables with Decimal columns that use inlined data fail to flush.
- **Fix**: Add Decimal128/256 parsing arm
- **Effort**: S

#### R5-S-016: Decimal column stats silently dropped (FixedLenByteArray as UTF-8)
- **Source**: interop (IO-009)
- **Files**: `src/table_writer.rs:1336-1347`
- **Description**: `String::from_utf8(v.data().to_vec()).ok()` on Decimal (binary) stats returns None, silently dropping min/max. File-level pruning won't work for Decimal columns.
- **Fix**: Decode FixedLenByteArray based on logical type (big-endian two's complement for Decimal)
- **Effort**: M

#### R5-S-017: register_dml_files omits format column for delete files
- **Source**: interop (IO-010)
- **Files**: `src/metadata_writer_sqlite.rs:1317-1319`, PG `:1052-1053`, MySQL `:1154-1155`
- **Description**: Batch delete file INSERT omits `format` column. SQLite DEFAULT handles it; PG/MySQL may produce NULL, confusing DuckDB.
- **Fix**: Add `format` column and bind `'parquet'` in all backends
- **Effort**: S

#### R5-S-018: _df_change_tracking blind to DuckDB-side changes
- **Source**: interop (IO-011)
- **Files**: All three metadata writers
- **Description**: DataFusion's change_tracking table isn't populated by DuckDB. Conflict detection only works for DF-to-DF concurrent writes, not DF-to-DuckDB.
- **Fix**: Also check ducklake_snapshot_changes or ducklake_table.end_snapshot for conflict detection
- **Effort**: M

#### R5-S-019: saturating_add silently clips row count to i64::MAX
- **Source**: correctness (R5-S-002)
- **Files**: `src/table_writer.rs:275`
- **Description**: Total row count uses `i64::saturating_add()`. Overflow clips silently instead of erroring. Stored in catalog as record_count.
- **Fix**: Use `checked_add` and return error on overflow
- **Effort**: S

#### R5-S-020: unwrap_or(i64::MAX) clips rowid overflow
- **Source**: correctness (R5-S-003)
- **Files**: `src/virtual_column_exec.rs:236`
- **Description**: Rowid overflow produces `i64::MAX` for all overflowing rows → duplicate rowids. Should error instead.
- **Fix**: Return `DataFusionError::Execution` on overflow
- **Effort**: S

#### R5-S-021: values_equal doesn't handle NaN for float join keys
- **Source**: correctness (R5-S-005)
- **Files**: `src/merge_exec.rs` — `values_equal`
- **Description**: `NaN != NaN` per IEEE 754, but SQL semantics treat NaN = NaN in joins. MERGE on float keys with NaN values fails to match rows.
- **Fix**: Add explicit NaN handling in Float32/Float64 match arms
- **Effort**: S

#### R5-S-022: Replace mode atomicity improvement
- **Source**: codex (CX-001, downgraded from P0)
- **Files**: `src/table_writer.rs:872-897`
- **Description**: In Replace mode, `end_table_files` then `register_data_file` are separate calls. Both are within the same transaction (safe), but making them a single atomic operation would be defense-in-depth.
- **Fix**: Consider `replace_table_files` single-call API or verify transaction guarantees in all backends
- **Effort**: S

#### R5-S-023: normalize_value over-normalizes all numeric strings
- **Source**: codex (CX-004) + test-harness (R5-TH-002)
- **Files**: `tests/common/test_utils.rs:278-288`
- **Description**: All numeric strings parsed through f64 and formatted to 6 decimals. Hides integer/float type confusion, large-integer precision loss. Comment says "only floats" but code normalizes everything.
- **Fix**: Only normalize values containing `.` or `e`/`E`; compare integers exactly
- **Effort**: S

#### R5-S-024: commit_uploaded_files non-atomic in append mode
- **Source**: codex (CX-005)
- **Files**: `src/table_writer.rs:637-653`
- **Description**: Files registered one at a time in append mode. Mid-loop failure leaves partially committed files. Within a transaction (safe), but a batch API would be more robust.
- **Fix**: Add batch registration API or verify rollback semantics
- **Effort**: S

#### R5-S-025: column_stats failure propagates after data file committed
- **Source**: codex (CX-006)
- **Files**: `src/table_writer.rs:516-519`
- **Description**: If register_column_stats fails after register_data_file succeeds (within same transaction), the error propagates and the transaction rolls back both. This could cause retry-driven duplicate writes if callers retry.
- **Fix**: Treat column-stats registration as non-fatal warning once data file is registered
- **Effort**: S

#### R5-S-026: SQLite INSERT OR REPLACE erases snapshot_changes metadata
- **Source**: codex (CX-009)
- **Files**: `src/metadata_writer_sqlite.rs:559`, `:704`, `:907` and 8 more sites
- **Description**: `INSERT OR REPLACE INTO ducklake_snapshot_changes` deletes then re-inserts, silently erasing `author`, `commit_message`, `commit_extra_info` columns.
- **Fix**: Use SQLite UPSERT: `INSERT ... ON CONFLICT(snapshot_id) DO UPDATE SET changes_made=excluded.changes_made`
- **Effort**: M

#### R5-S-027: MySQL ID allocation race-prone with MAX+1
- **Source**: codex (CX-011)
- **Files**: `src/metadata_writer_mysql.rs:470`, `:541`, `:923`, `:968` and 3 more
- **Description**: `MAX(...)+1 ... FOR UPDATE` on non-unique columns is race-prone for table_id, column_id, view_id, partition_id. Empty-table bootstrap especially risky.
- **Fix**: Use AUTO_INCREMENT/sequence-backed columns or dedicated sequence table
- **Effort**: M

#### R5-S-028: Inlined-table resolution ignores snapshot/version semantics
- **Source**: codex (CX-012)
- **Files**: All 4 metadata providers (`get_inlined_data` queries)
- **Description**: `SELECT FROM ducklake_inlined_data_tables WHERE table_id = ?` has no snapshot/schema_version filter. Historical reads can pick the wrong inlined table version.
- **Fix**: Add `schema_version <= X ORDER BY schema_version DESC LIMIT 1` filter
- **Effort**: M

#### R5-S-029: DuckLakeSchema snapshot not refreshed after DDL writes
- **Source**: codex (CX-014)
- **Files**: `src/schema.rs:75`, `:314`, `:361`
- **Description**: DuckLakeSchema pins snapshot_id and doesn't refresh after register_table/deregister_table. If a reference is held across DDL operations, subsequent reads use stale snapshot. Normal SQL query patterns are unaffected (catalog creates fresh schema objects).
- **Fix**: Share AtomicI64 snapshot_id with catalog, or document that schema objects are ephemeral
- **Effort**: S

#### R5-S-030: Table function dot-splitting breaks quoted identifiers
- **Source**: codex (CX-017) + correctness (R5-S-010)
- **Files**: `src/table_functions.rs:344-352`
- **Description**: Splitting on first `.` breaks for quoted identifiers containing dots and 3-part names. Also doesn't validate empty parts (`.foo` or `foo.`).
- **Fix**: Implement SQL-identifier-aware parsing or support explicit separate arguments
- **Effort**: S

#### R5-S-031: DuckDB decode error silently converted to "NULL" in tests
- **Source**: codex (CX-018)
- **Files**: `tests/hybrid_asyncdb.rs:420`
- **Description**: In transaction-mode reads, any DuckDB decode error → "NULL", masking real type/conversion bugs and letting tests pass incorrectly.
- **Fix**: Return an error instead of substituting "NULL"
- **Effort**: S

#### R5-S-032: Zero-row DML returns StatementComplete instead of count
- **Source**: codex (CX-019) + test-harness (R5-TH-004)
- **Files**: `tests/hybrid_asyncdb.rs:366`, `:438-439`
- **Description**: Zero-row DML and empty SELECT results return `StatementComplete(0)` instead of `DBOutput::Rows`. Breaks SLT `query I` semantics for zero-count assertions and empty result sets.
- **Fix**: Always return Rows with appropriate types for SELECT and DML count results
- **Effort**: S

#### R5-S-033: rewrite_table_references does raw substring replacement
- **Source**: codex (CX-020)
- **Files**: `tests/hybrid_asyncdb.rs:152`
- **Description**: Raw `replace()` of `ducklake.` without SQL parsing can rewrite inside string literals, comments, and aliases, corrupting queries.
- **Fix**: Use token-aware logic or parsed SQL AST for table reference rewriting
- **Effort**: M

#### R5-S-034: UPDATE buffers all matched rows in memory
- **Source**: codex (CX-021)
- **Files**: `src/update_exec.rs:244`, `:447-471`
- **Description**: All matched rows accumulated in `updated_batches` before writing a single Parquet file. Large updates can cause OOM. (Related to F-036 theme but affects UPDATE, not INSERT.)
- **Fix**: Stream updated batches to Parquet writer or chunked temp files
- **Effort**: M

#### R5-S-035: MERGE uses O(N*M) nested loop join
- **Source**: codex (CX-022)
- **Files**: `src/merge_exec.rs:374-428`
- **Description**: Nested target-row × source-row scans are quadratic. Degrades severely on non-trivial datasets.
- **Fix**: Build hash index on source join keys
- **Effort**: M

#### R5-S-036: Filters advertised as Inexact but not pushed to Parquet scan
- **Source**: codex (CX-028)
- **Files**: `src/table.rs:1387`, `:571`, `:797`, `:853`
- **Description**: `supports_filters_pushdown()` reports Inexact for all filters, but scan planning may not pass filters into Parquet scan config. Pushdown would be advertised but not implemented. (Needs verification — DataFusion's ParquetExec may handle this automatically.)
- **Fix**: Verify filter pushdown actually works; if not, wire filters into scan config or return Unsupported
- **Effort**: M

#### R5-S-037: CDC projection HashMap dedup breaks duplicate column projections
- **Source**: codex (CX-030)
- **Files**: `src/cdc_common.rs:88`
- **Description**: HashMap deduplication collapses duplicate projected columns, producing incorrect reorder map for `SELECT col, col, ...` patterns.
- **Fix**: Build reorder indices position-by-position without HashMap deduplication
- **Effort**: S

#### R5-S-038: Snapshot bounds only accept Int32/Int64 literals
- **Source**: codex (CX-032)
- **Files**: `src/table_functions.rs:377-388`
- **Description**: Table change functions reject UInt*, Decimal*, and other numeric types for snapshot bounds. DuckDB workflows may use wider types.
- **Fix**: Accept wider numeric scalar variants and convert
- **Effort**: S

#### R5-S-039: table_changes only emits Insert, never Delete
- **Source**: codex (CX-033)
- **Files**: `src/table_changes.rs:549`, `:45`
- **Description**: `ducklake_table_changes()` only reads added data files (always Insert). ChangeType::Delete exists but is unused. DuckDB's table_changes returns full CDC with both insert and delete.
- **Fix**: Union insert plans with deletion plans
- **Effort**: M

#### R5-S-040: Pre-1970 timestamp conversion can panic
- **Source**: codex (CX-035)
- **Files**: `tests/common/test_utils.rs:36`, `:131`
- **Description**: Timestamp conversion uses `%` on potentially negative epoch values and casts to `u32`, which panics for pre-1970 timestamps.
- **Fix**: Use `div_euclid`/`rem_euclid` before constructing timestamps
- **Effort**: S

#### R5-S-041: Virtual column stripping uses naive SQL string matching
- **Source**: test-harness (R5-TH-003)
- **Files**: `tests/hybrid_asyncdb.rs:291`
- **Description**: `sql_upper.contains(&name.to_uppercase())` matches column names anywhere in SQL (WHERE clauses, string literals, etc.), not just SELECT list. Can silently strip legitimate columns from results.
- **Fix**: Parse only SELECT clause or use schema metadata for virtual column identification
- **Effort**: M

#### R5-S-042: Timestamp sub-second precision inconsistency between formatters
- **Source**: test-harness (R5-TH-005)
- **Files**: `tests/common/test_utils.rs`, `tests/hybrid_asyncdb.rs`
- **Description**: Two timestamp formatting paths differ in sub-second handling. No explicit truncation/rounding policy, potentially causing false failures or masking precision differences.
- **Fix**: Establish consistent timestamp formatting policy across both formatters
- **Effort**: S

#### R5-S-043: SLT preprocessor vacuous-test guard may be too weak
- **Source**: test-harness (R5-TH-008)
- **Files**: `tests/sqllogictest_runner.rs:784-791`
- **Description**: Tests with only error-expecting statements converted to no-ops can pass vacuously. No minimum meaningful-statement threshold.
- **Fix**: Track count of meaningful statements and warn when below threshold
- **Effort**: S

---

### P3 — Minor (38 findings)

#### Code Quality (from idiomatic review)

| ID | Description | Files | Effort |
|----|-------------|-------|--------|
| R5-S-044 | Duplicated `quote_identifier` function | `metadata_provider.rs:782`, `metadata_writer_validation.rs:67` | S |
| R5-S-045 | Duplicated `make_*_count_schema()` across DML exec plans | `delete_exec.rs`, `update_exec.rs`, `merge_exec.rs`, `insert_exec.rs` | S |
| R5-S-046 | Manual type dispatch in `values_equal()` — Arrow eq kernel available | `merge_exec.rs` | S |
| R5-S-047 | Repetitive TableProvider boilerplate in information_schema.rs (6 near-identical impls) | `information_schema.rs` | M |
| R5-S-048 | Inline helper lambdas (`to_string_array`, `to_i64_array`) duplicated in compaction_functions.rs | `compaction_functions.rs` | S |
| R5-S-049 | Unnecessary `positions_to_delete.clone()` | `delete_exec.rs:340` | S |
| R5-S-050 | Redundant `let batch = batch_result;` binding | `delete_exec.rs` | S |
| R5-S-051 | Inconsistent `Arc::clone(&x)` vs `x.clone()` style | Throughout codebase | S |
| R5-S-052 | `.iter().map(\|r\| r.clone())` instead of `.into_iter()` | `metadata_provider_sqlite.rs` | S |
| R5-S-053 | Public `pool` fields on metadata providers leak implementation | PG/MySQL/SQLite providers | S |
| R5-S-054 | `DuckLakeEncryptionFactory` derives `Clone` but never cloned (security concern) | `encryption.rs` | S |
| R5-S-055 | `keep_indices.clone()` may be unnecessary | `table_deletions.rs:702` | S |

#### Correctness Edge Cases

| ID | Description | Files | Effort |
|----|-------------|-------|--------|
| R5-S-056 | `rewrite_duckdb_view_sql` uses byte length for char-based offset (ASCII-only today) | `schema.rs:166-192` | S |
| R5-S-057 | `delete_filter.rs` u32 indices limit batch to 4B rows (no guard) | `delete_filter.rs` | S |
| R5-S-058 | `extract_update_info` silently skips extra projection expressions | `query_planner.rs:202-205` | S |
| R5-S-059 | `schema_names()` swallows errors with `unwrap_or_default` (DF trait limitation) | `catalog.rs:344-354` | S |
| R5-S-060 | `deregister_schema` cascade creates multiple snapshots | `catalog.rs:242-258` | S |
| R5-S-061 | `build_inlined_data_exec` column lookup is O(n²) per row | `table.rs` | S |
| R5-S-062 | Compaction SQL string interpolation — defense-in-depth note (currently safe) | `compaction_functions.rs` | S |

#### Interop Nits

| ID | Description | Files | Effort |
|----|-------------|-------|--------|
| R5-S-063 | UUID v4 (ours) vs v7 (DuckDB) — cosmetic | `table_writer.rs:85`, DML exec plans | S |
| R5-S-064 | MERGE snapshot_changes always records both inserted+deleted regardless of actual ops | `merge_exec.rs:638-643` | S |
| R5-S-065 | MySQL `TIMESTAMP(6)` has Y2038 limit — use `DATETIME(6)` | `metadata_writer_mysql.rs:31` | S |
| R5-S-066 | MySQL VARCHAR(255/1024) limits may truncate long paths | `metadata_writer_mysql.rs:22-324` | S |
| R5-S-067 | Cross-engine tests only use SQLite backend, not PG/MySQL | `tests/cross_engine_tests.rs:48-68` | M |
| R5-S-068 | Parquet v2.0 vs DuckDB default v1.0 — no functional issue | `table_writer.rs:176`, DML exec plans | — |

#### Test/Robustness Nits

| ID | Description | Files | Effort |
|----|-------------|-------|--------|
| R5-S-069 | DuckDbConn::query discovers columns by trial-and-error, may truncate on non-String types | `tests/common/test_utils.rs:414` | S |
| R5-S-070 | No dedicated test for transaction-aware read routing in hybrid adapter | `tests/hybrid_asyncdb.rs:230-260` | S |

#### Codex Miscellaneous

| ID | Description | Files | Effort |
|----|-------------|-------|--------|
| R5-S-071 | `set_data_path` non-atomic (DELETE then INSERT) in PG/MySQL — theoretical concurrent issue | `metadata_writer_postgres.rs:1160`, `metadata_writer_mysql.rs:1263` | S |
| R5-S-072 | `initialize_schema` not atomic across backends — partial catalog state on failure | All writers | S |
| R5-S-073 | DuckDB `table_exists`/`get_table_row_count` `unwrap_or(false/0)` suppresses DB errors | `metadata_provider_duckdb.rs:100`, `:602` | S |
| R5-S-074 | `get_table_row_count` uses separate statements (file + inline count) without transaction | All providers | S |
| R5-S-075 | `join_paths` normalizes interior `//` — can rewrite valid object-store keys | `path_resolver.rs:277-279` | S |
| R5-S-076 | Delete file size `unwrap_or(0)` hides metadata errors | `table_deletions.rs:119-132` | S |
| R5-S-077 | `cross_engine_df_write_duckdb_read` doesn't check float `score` column | `tests/cross_engine_tests.rs:213` | S |
| R5-S-078 | MySQL snapshot change format inconsistent (`"Altered table"` vs `"altered_table:"`) | `metadata_writer_mysql.rs:2410` | S |
| R5-S-079 | `nulls_allowed` NULL→true coercion hides catalog corruption | All 4 providers | S |
| R5-S-080 | `test_table_rewrite` uses `contains()` instead of exact SQL match | `tests/hybrid_asyncdb.rs:758` | S |
| R5-S-081 | `cross_engine_null_handling` test only checks 2 cells + row count | `tests/cross_engine_tests.rs:457` | S |

---

## Recommended Fix Agents

### Agent 1: fix-write-safety (6 findings — R5-S-003, 004, 005, 019, 020, 022)
- **Scope**: Data integrity and safety in write paths
- **Findings**: contains_null PG/MySQL, inlining flush error propagation, Date partition pruning, saturating_add→checked_add, rowid overflow, Replace atomicity
- **Effort**: S-M
- **Priority**: HIGH — fixes P1 data integrity issues

### Agent 2: fix-metadata-correctness (5 findings — R5-S-001, 002, 007, 008, 026)
- **Scope**: Metadata query correctness
- **Findings**: Lexicographic MIN/MAX stats, replace_table_files stale stats, delete-delta boundary, statistics/schema alignment, INSERT OR REPLACE erasing metadata
- **Effort**: M
- **Priority**: HIGH — fixes P1 correctness issues

### Agent 3: fix-interop-types (5 findings — R5-S-012, 014, 015, 016, 017)
- **Scope**: Cross-engine type handling
- **Findings**: Unknown types → string, Date/Timestamp inlined serialization, Decimal flush, Decimal stats, delete file format column
- **Effort**: S-M
- **Priority**: MEDIUM — fixes P2 interop issues

### Agent 4: fix-dml-robustness (5 findings — R5-S-006, 021, 034, 035, 037)
- **Scope**: DML execution plan robustness
- **Findings**: UPDATE panic, NaN merge keys, UPDATE OOM, MERGE O(N*M), CDC dedup
- **Effort**: S-M
- **Priority**: MEDIUM

### Agent 5: fix-test-infrastructure (8 findings — R5-S-010, 023, 031, 032, 033, 040, 041, 042)
- **Scope**: Test harness correctness
- **Findings**: Decimal128 sign loss, normalize_value, DuckDB decode errors, zero-row DML, raw substring rewrite, pre-1970 timestamps, virtual column stripping, timestamp precision
- **Effort**: S-M
- **Priority**: MEDIUM — fixes test masking issues

### Agent 6: fix-table-functions (5 findings — R5-S-009, 028, 030, 038, 039)
- **Scope**: Table function and CDC improvements
- **Findings**: strip_prefix paths, inlined-table snapshot, table name parsing, snapshot bounds types, table_changes no Delete
- **Effort**: S-M
- **Priority**: MEDIUM

### Agent 7: fix-backend-parity (4 findings — R5-S-013, 018, 027, 029)
- **Scope**: Multi-backend consistency
- **Findings**: schema_version BIGINT, change_tracking blind to DuckDB, MySQL ID race, DuckLakeSchema snapshot
- **Effort**: S-M
- **Priority**: LOW

### Agent 8: cross-engine-test-coverage (1 finding — R5-S-011)
- **Scope**: New cross-engine tests
- **Findings**: DML, ALTER TABLE, partitions, complex types
- **Effort**: L
- **Priority**: MEDIUM — addresses largest test coverage gap

---

## Priority Summary

| Priority | Count | Key Themes |
|----------|-------|------------|
| P0 | 0 | All codex P0s downgraded after source validation |
| P1 | 11 | Stats correctness (2), data integrity (2), partition pruning (1), test formatter (1), missing tests (1), runtime safety (4) |
| P2 | 28 | Interop types (5), test infra (6), atomicity/robustness (5), DML performance (2), metadata (5), feature gaps (3), filter pushdown (1), CDC (1) |
| P3 | 38 | Code quality DRY (12), edge cases (7), interop nits (6), test nits (2), codex misc (11) |
| **Total** | **77** | |

### Effort Distribution

| Effort | Count | % |
|--------|-------|---|
| S (< 30 min) | 56 | 73% |
| M (30 min - 2 hrs) | 20 | 26% |
| L (> 2 hrs) | 1 | 1% |

### Recommended Fix Priority

1. **Agent 1 + 2** (P1 fixes): 11 findings, all P1 — fix first
2. **Agent 3 + 4** (P2 interop + DML): 10 findings — fix second
3. **Agent 5** (test infrastructure): 8 findings — fix third
4. **Agent 6 + 7** (functions + backend): 9 findings — fix fourth
5. **Agent 8** (test coverage): 1 finding but L effort — schedule separately
6. **P3 items**: Address opportunistically or defer

### Key Observations

1. **No P0 findings after validation** — The codebase is in good shape. Prior review cycles (R1-R4) addressed all critical issues.
2. **Codex over-reports severity** — All 4 P0s downgraded, 10+ P1s downgraded to P2/P3. Always validate codex claims against actual source.
3. **Test infrastructure is the largest P2 cluster** — 6 findings about test masking/false positives. Fixing these improves confidence in all other test results.
4. **Cross-engine interop remains the biggest coverage gap** — Missing DML/ALTER/partition cross-engine tests (R5-S-011) is the single most impactful finding.
5. **Most fixes are small** — 73% of findings are S effort, suggesting rapid fix cycles are possible.
