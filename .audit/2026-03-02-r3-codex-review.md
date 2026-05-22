# R3 Codex Review — 2026-03-02

Review cycle 3, focusing on correctness of recent fix code and NEW issues not found in R1/R2.

## Methodology

- **6 codex batches** covering write path, providers, virtual columns/types, interop writers, table functions, and test infrastructure
- **3 deep-dive agent reviews** for supplementary analysis of all recently-changed files
- Focus: bugs in fix code, regressions introduced by changes, issues missed by R1/R2

## Summary

| Severity | Count | New vs Re-raised |
|----------|-------|------------------|
| P0 (Critical) | 2 | 2 new |
| P1 (High) | 7 | 5 new, 2 re-raised |
| P2 (Medium) | 10 | 8 new, 2 re-raised |
| P3 (Low) | 9 | all new |
| **Total** | **28** | **24 new, 4 re-raised** |

---

## P0 — Critical

### CX3-001: `register_dml_files` omits `row_id_start` and `table_stats` update for new data files (ALL writers)
- **Files**: `metadata_writer_sqlite.rs:1100-1114`, `metadata_writer_postgres.rs:930`, `metadata_writer_mysql.rs:1029`
- **Description**: When UPDATE/MERGE produce replacement data files, `register_dml_files` inserts them WITHOUT `row_id_start` and does NOT update `ducklake_table_stats`. Compare with `register_data_file` (which correctly reads `next_row_id`, sets `row_id_start`, and updates stats). This was a known fix target (F-011) but the fix was only applied to the INSERT path, not the DML path.
- **Impact**: (1) DML-created data files have NULL `row_id_start`, breaking delete-file position tracking. (2) `ducklake_table_stats` becomes stale after any UPDATE/MERGE. (3) Subsequent INSERTs use stale `next_row_id`, potentially overlapping row IDs.
- **Effort**: M

### CX3-002: MERGE execution has no orphaned file cleanup on metadata commit failure
- **Files**: `merge_exec.rs:579-587`
- **Description**: Unlike `delete_exec.rs` (line 212, 362, 376) and `update_exec.rs` (line 501) which track uploaded files and call `cleanup_orphaned_files` on failure, `merge_exec.rs` has NO `uploaded_files` tracking vector and NO cleanup call. If `register_dml_files` fails, both delete files and new data files are permanently orphaned in object storage.
- **Impact**: Object storage leak; orphaned files accumulate permanently on any MERGE metadata failure.
- **Effort**: M

---

## P1 — High

### CX3-003: Timestamp roundtrip silently replaces non-UTC timezone with UTC
- **Files**: `types.rs:138-146` (arrow_to_ducklake_type), `types.rs:60-72` (ducklake_to_arrow_type)
- **Description**: `arrow_to_ducklake_type` maps `Timestamp(_, Some("America/New_York"))` to `"timestamptz"`. On roundtrip, `ducklake_to_arrow_type` maps it back to `Timestamp(Microsecond, Some("UTC"))`. Non-UTC timezone information is silently lost. Tests only cover UTC timezones.
- **Impact**: Data corruption for non-UTC-normalized timestamp columns during schema evolution or cross-engine writes.
- **Effort**: M

### CX3-004: PostgreSQL/MySQL writers missing schema_version tracking and ducklake_schema_versions population
- **Files**: `metadata_writer_postgres.rs:303,1271,1304,1674`, `metadata_writer_mysql.rs:374,1388,1420,1790`
- **Description**: DDL paths (create_view, drop_view, rename_table, etc.) insert snapshots without incrementing `schema_version` and never insert into `ducklake_schema_versions`. SQLite correctly does both (lines 381-403, 592). This fix (F-012) was only applied to the SQLite writer.
- **Impact**: DuckDB cannot resolve schema versions for PG/MySQL catalogs; time-travel schema resolution fails.
- **Effort**: M

### CX3-005: PostgreSQL/MySQL writers missing UUID generation on create paths
- **Files**: `metadata_writer_postgres.rs:331,363,1288`, `metadata_writer_mysql.rs:402,437,1404`
- **Description**: `schema_uuid`, `table_uuid`, `view_uuid` columns exist in DDL but are never populated. SQLite correctly generates UUIDs (lines 420, 445, 1447). This fix (F-026) was only applied to SQLite.
- **Impact**: DuckDB may fail or produce incorrect results reading PG/MySQL catalogs with NULL UUIDs.
- **Effort**: S

### CX3-006: PostgreSQL/MySQL writers use wrong `changes_made` format
- **Files**: `metadata_writer_postgres.rs:512,600,1407,1642`, `metadata_writer_mysql.rs:599,688,1519,1758`
- **Description**: PG/MySQL use human-readable strings (`"Dropped table (id=5)"`) while DuckDB expects tokenized format (`"dropped_table:5"`). SQLite was fixed (F-027) but PG/MySQL were not updated.
- **Impact**: DuckDB interop broken for change history on PG/MySQL catalogs.
- **Effort**: M

### CX3-007: PostgreSQL/MySQL writers don't preserve column IDs for no-op schema writes
- **Files**: `metadata_writer_postgres.rs:399,410`, `metadata_writer_mysql.rs:474,485`
- **Description**: `write_transaction_inner` always ends active columns and allocates new IDs, even when schema matches. SQLite preserves existing IDs (lines 486, 494). This fix (F-013) was only applied to SQLite.
- **Impact**: Column ID instability breaks Parquet field-ID mapping across snapshots on PG/MySQL.
- **Effort**: M

### CX3-008: `count_inlined_rows()` still interpolates raw table names (3 providers)
- **Files**: `metadata_provider_sqlite.rs:997`, `metadata_provider_postgres.rs:1065`, `metadata_provider_mysql.rs:1004`
- **Description**: `get_inlined_data()` correctly uses `quote_identifier()`, but `count_inlined_rows()` still interpolates `inlined_table_name` without quoting. The `quote_identifier` fix (F-001) was applied inconsistently.
- **Impact**: SQL injection vector via crafted catalog entries in the row count path.
- **Effort**: S

### CX3-009: `Date32`/`Date64` round-trip is broken in `parse_string_to_array`
- **Files**: `table_writer.rs:1127-1128`
- **Description**: `arrow_array_value_to_string` serializes dates as `"2024-01-15"` format strings, but `parse_string_to_array` uses `parse_primitive!(Date32Builder, values)` which calls `.parse::<i32>()` on the date string. This always fails because the string is not an integer.
- **Impact**: Inlined data with Date columns silently drops to fallback string type or errors on flush.
- **Effort**: S

---

## P2 — Medium

### CX3-010: No `snapshot_changes` records for DELETE/UPDATE/MERGE operations
- **Files**: `delete_exec.rs`, `update_exec.rs`, `merge_exec.rs` (entire files)
- **Description**: DDL operations correctly write `ducklake_snapshot_changes` records, but none of the DML execution plans do. DML snapshots are "invisible" in the change history.
- **Impact**: Incomplete catalog change history; DuckDB cannot determine what DML operations occurred.
- **Effort**: S

### CX3-011: `set_data_path` is not atomic (DELETE + INSERT without transaction)
- **Files**: `metadata_writer_sqlite.rs:1138-1153`
- **Description**: Executes a `DELETE` followed by an `INSERT` on `ducklake_metadata` without wrapping in a transaction. Process crash between operations leaves catalog with missing `data_path`.
- **Impact**: Catalog becomes unusable on crash during `set_data_path`.
- **Effort**: S

### CX3-012: `table_deletions` returns wrong column order for reordered full projections
- **Files**: `table_deletions.rs:125,173,233`
- **Description**: When all columns are requested but in non-natural order, `analyze_projection` records the order, but `build_exec_for_delete_entry` disables Parquet projection (`None`) when `len == all_cols`. Batches come back in natural order, but reordering may be skipped.
- **Impact**: CDC deletion queries can return columns in wrong order.
- **Effort**: M

### CX3-013: `get_table_structure` is not snapshot-aware in table functions
- **Files**: `table_functions.rs:331-333`
- **Description**: `resolve_table_for_function()` correctly pins schema/table lookup with `snapshot_id`, but column resolution via `get_table_structure(table_id)` has no snapshot parameter — it returns current-version columns. After schema evolution, historical queries see wrong schema.
- **Impact**: Table function results inconsistent after schema changes.
- **Effort**: M (trait change required)

### CX3-014: Inlined table name read from DB, interpolated without `quote_identifier` in writer
- **Files**: `metadata_writer_sqlite.rs:2253,2405,2448`
- **Description**: In `get_inlined_row_count`, `read_inlined_data`, and `clear_inlined_data`, the `inlined_table_name` is read from `ducklake_inlined_data_tables` and interpolated using raw double-quote wrapping instead of `quote_identifier()`.
- **Impact**: Names containing `"` could break out of the quoting context.
- **Effort**: S

### CX3-015: `Date64 -> "date" -> Date32` lossy roundtrip in type system
- **Files**: `types.rs:127,58`
- **Description**: `arrow_to_ducklake_type` maps both `Date32` and `Date64` to `"date"`, but `ducklake_to_arrow_type` maps `"date"` back to `Date32`. Date64 stores milliseconds; Date32 stores days. Precision is lost.
- **Effort**: S

### CX3-016: `Interval` variant lost on roundtrip
- **Files**: `types.rs:147,76`
- **Description**: All `Interval` variants (`YearMonth`, `DayTime`, `MonthDayNano`) map to `"interval"`, which maps back to `Interval(MonthDayNano)`. Lossy for non-MonthDayNano variants.
- **Effort**: S

### CX3-017: `parse_decimal` silently ignores trailing garbage after closing parenthesis
- **Files**: `types.rs:255-259`
- **Description**: `type_str.find(')')` finds the first `)`, so `"decimal(10,2)extra_garbage"` parses successfully as `Decimal128(10,2)`, silently ignoring trailing text.
- **Effort**: S

### CX3-018: `write_parquet_with_setup` uses unchecked `buffer.len() as i64`
- **Files**: `table_writer.rs:485`
- **Description**: The `finish()` and `upload()` methods correctly use `i64::try_from(buffer.len())`, but `write_parquet_with_setup` uses `as i64`. Same pattern in `update_exec.rs:423,478`, `merge_exec.rs:484,564`, `delete_exec.rs:353`.
- **Impact**: Theoretically wraps to negative on >9.2EB files. Inconsistent with safer pattern elsewhere.
- **Effort**: S

### CX3-019: ORDER BY ALL rewriting incorrect for multi-line SQL in SLT runner
- **Files**: `sqllogictest_runner.rs:265,270`
- **Description**: Detection uses full multi-line preview but rewriting only mutates the first SQL line. If `ORDER BY ALL` appears on later lines, it is not removed, but the directive is changed to `rowsort`, producing invalid transformed tests.
- **Effort**: M

---

## P3 — Low

### CX3-020: Empty snapshots created when DML affects zero rows
- **Files**: `delete_exec.rs:199-201`, `update_exec.rs:232-234`, `merge_exec.rs:317-319`
- **Description**: `create_snapshot()` is called unconditionally. If WHERE clause matches nothing, an empty snapshot is committed with no changes. Creates noise in catalog history.
- **Effort**: S

### CX3-021: MERGE source rows can match multiple target rows without error
- **Files**: `merge_exec.rs:388-421`
- **Description**: The inner loop scans all source rows for each target row. A single source row can match multiple targets, causing multiple deletions/updates. SQL standard MERGE should raise an error in this case.
- **Effort**: M

### CX3-022: `delete_count` metadata tracks delta, not total positions in file
- **Files**: `delete_exec.rs:311`, `update_exec.rs:434`, `merge_exec.rs:493-498`
- **Description**: `delete_count` is set to count of new deletions, but the actual delete file includes merged existing positions. The metadata may understate total deleted rows.
- **Effort**: S

### CX3-023: `CoalescePartitionsExec` wrapping fragile across `with_new_children`
- **Files**: `virtual_column_exec.rs:88-96,153-168`
- **Description**: `VirtualColumnExec::new()` conditionally wraps input in `CoalescePartitionsExec`. `with_new_children()` calls `new()` again on the already-coalesced child. Currently works because single-partition check prevents double-wrap, but fragile if optimizer inserts repartitioning.
- **Effort**: S

### CX3-024: `changes_made` format does not escape quotes in schema/table names
- **Files**: `metadata_writer_sqlite.rs:550`
- **Description**: `format!("created_table:\"{}\".\"{}\"", schema_name, table_name)` does not escape internal double-quotes. Malformed if names contain `"`.
- **Effort**: S

### CX3-025: `rewrite_unqualified_tables` is a no-op that wastes allocations
- **Files**: `sqllogictest_runner.rs:604-612`
- **Description**: Returns `line.to_string()` without transformation but is called for every non-directive line, creating unnecessary string allocations.
- **Effort**: S

### CX3-026: Read-only write test silently passes if write unexpectedly succeeds
- **Files**: `sql_write_tests.rs:256,269-271`
- **Description**: `test_insert_into_read_only_fails` explicitly accepts `Ok(_)` as "acceptable behavior", masking a potential write-to-read-only regression.
- **Effort**: S

### CX3-027: DuckDB error handling inconsistency for missing `data_path`
- **Files**: `metadata_provider_duckdb.rs:119`
- **Description**: SQLx providers return explicit `InvalidConfig` on missing `data_path`. DuckDB provider bubbles a raw `QueryReturnedNoRows` error instead.
- **Effort**: S

### CX3-028: Bare `decimal`/`numeric` without parameters produces misleading `UnsupportedType` error
- **Files**: `types.rs:242-253,93-99`
- **Description**: Bare `"decimal"` or `"numeric"` falls through to the unsupported type path. Error says "Unsupported type: decimal" which is misleading since decimal IS supported with parameters. Should default to `Decimal128(18,0)` or give a descriptive error.
- **Effort**: S

---

## Cross-cutting Observations

### Fix Parity Gap: SQLite vs PostgreSQL/MySQL

The most significant finding is that fixes F-011 (row_id_start), F-012 (schema_versions), F-013 (column ID preservation), F-026 (UUID generation), and F-027 (changes_made format) were applied to the **SQLite writer only**. The PostgreSQL and MySQL writers remain unfixed for these items. This creates a significant interop gap where:
- SQLite-backed catalogs work correctly with DuckDB
- PostgreSQL/MySQL-backed catalogs have multiple interop failures

### DML Path vs INSERT Path Divergence

The `register_dml_files` function (used by DELETE/UPDATE/MERGE) diverges from `register_data_file` (used by INSERT) in that it:
1. Does not set `row_id_start` (CX3-001)
2. Does not update `ducklake_table_stats` (CX3-001)
3. Does not write `snapshot_changes` records (CX3-010)

These are all operations that `register_data_file` correctly handles.

### Test Infrastructure

The SLT runner's ORDER BY ALL rewriting (CX3-019) and the read-only test's permissive assertion (CX3-026) are the most impactful test issues. Additionally, several implemented table functions (TABLE_CHANGES, TABLE_DELETIONS, etc.) are being skipped by `contains_unsupported_function` in the hybrid runner, reducing test coverage unnecessarily.

---

## Deduplication with R1/R2

| Finding | R1/R2 Status | R3 Status |
|---------|-------------|-----------|
| CX3-001 (DML row_id_start) | F-011 fixed for INSERT only | NEW: DML path missed |
| CX3-004 (PG/MySQL schema_versions) | F-012 fixed for SQLite only | NEW: PG/MySQL missed |
| CX3-005 (PG/MySQL UUIDs) | F-026 fixed for SQLite only | NEW: PG/MySQL missed |
| CX3-006 (PG/MySQL changes_made) | F-027 fixed for SQLite only | NEW: PG/MySQL missed |
| CX3-007 (PG/MySQL column IDs) | F-013 fixed for SQLite only | NEW: PG/MySQL missed |
| CX3-008 (count_inlined_rows quoting) | F-001 partially fixed | RE-RAISED: incomplete |
| CX3-013 (get_table_structure snapshot) | Similar to F-018 | RE-RAISED: trait limitation |
| CX3-014 (inlined table quoting) | Related to F-001 | NEW: writer-side variant |
| CX3-015 (Date64 roundtrip) | Related to F-037 | NEW: different type |
| CX3-016 (Interval roundtrip) | Related to F-037 | NEW: different type |
| All others | — | NEW |

---

## Recommended Fix Priority

1. **CX3-001** (P0): Add `row_id_start` and `table_stats` update to `register_dml_files` in all 3 writers
2. **CX3-002** (P0): Add `uploaded_files` tracking and cleanup to `merge_exec.rs`
3. **CX3-004,005,006,007** (P1): Port SQLite fixes to PostgreSQL and MySQL writers
4. **CX3-008,014** (P1): Apply `quote_identifier` consistently to all dynamic SQL paths
5. **CX3-003,009** (P1): Fix type system roundtrip for timestamps and Date32/64
6. **CX3-010** (P2): Add `snapshot_changes` records for all DML operations
7. **CX3-012,013** (P2): Fix projection handling and snapshot awareness in table functions
8. Remaining P2/P3 items as capacity allows
