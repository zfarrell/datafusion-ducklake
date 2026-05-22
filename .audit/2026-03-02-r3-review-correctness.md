# R3 Correctness Review — 2026-03-02

## Scope

Third review cycle focused on NEW correctness issues, regressions from R2 fixes, and spot-checking previous P0/P1 fix implementations. Key areas: DML execution paths, metadata writer atomicity, inlined data roundtrip, SQL injection hygiene.

## Findings

### P1 — High

#### C3-001: `register_dml_files` omits `row_id_start` and `table_stats` for new data files

- **File(s)**: `metadata_writer_sqlite.rs:1100-1113`
- **Also affects**: `metadata_writer_postgres.rs`, `metadata_writer_mysql.rs` (same pattern)
- **Description**: The `register_dml_files` method (used for UPDATE/MERGE new data files) inserts into `ducklake_data_file` WITHOUT setting `row_id_start` and WITHOUT updating `ducklake_table_stats`. Compare with `register_data_file` (line 971-1038) which properly reads `next_row_id` from `ducklake_table_stats`, sets `row_id_start` in the INSERT, and updates `ducklake_table_stats` afterward.
- **Impact**: After UPDATE or MERGE operations, new data files have NULL `row_id_start`. This causes:
  1. Virtual column `rowid` returns NULL for rows in these files
  2. DuckDB cannot correlate delete file positions with data file rows for these files
  3. `ducklake_table_stats.record_count` / `next_row_id` / `file_size_bytes` become stale
- **Root cause**: F-002 fix introduced `register_dml_files` for atomicity but only copied the basic INSERT, not the `row_id_start` / `table_stats` logic from `register_data_file`.
- **Suggested fix**: Inside `register_dml_files`, for each data file: read current `next_row_id` from `ducklake_table_stats`, set `row_id_start` in the INSERT, and update `ducklake_table_stats` (all within the existing transaction).
- **Effort**: S

#### C3-002: MERGE exec does not clean up orphaned files on metadata commit failure

- **File(s)**: `merge_exec.rs:579-587`
- **Description**: When `register_dml_files` fails in the MERGE execution path, the error is propagated via `.map_err()` without cleaning up already-uploaded Parquet files (delete files + data files). The DELETE exec (`delete_exec.rs:372-378`) and UPDATE exec (`update_exec.rs:494-502`) both properly call `cleanup_orphaned_files` on failure, but MERGE does not.
- **Impact**: MERGE metadata commit failure leaves orphaned Parquet files on the object store. These files consume storage indefinitely since no garbage collection exists.
- **Root cause**: MERGE exec was likely written before the cleanup pattern was established in DELETE/UPDATE. Additionally, MERGE exec doesn't maintain an `uploaded_files` list for tracking upload paths.
- **Suggested fix**: Track uploaded file paths in a `Vec<ObjectPath>` during the MERGE loop (as DELETE and UPDATE do), and call `cleanup_orphaned_files` if `register_dml_files` fails.
- **Effort**: S

#### C3-003: Date32/Date64 inlined data roundtrip is broken — data loss on flush

- **File(s)**: `table_writer.rs:979-993` (write), `table_writer.rs:1127-1128` (flush read), `table.rs:1869-1870` (query read)
- **Description**: The inlined data write path (`arrow_array_value_to_string`) serializes Date32 values as ISO 8601 strings (e.g., `"2024-06-15"` for epoch day 19889). However, both read-side parsers use `parse_primitive!(Date32Builder, values)` which calls `"2024-06-15".parse::<i32>()` — this **always fails** because ISO date strings are not valid integers.
  - In the query read path (`table.rs:1869`): parse failure silently produces NULL (data loss)
  - In the flush path (`table_writer.rs:1127`): parse failure returns an error, preventing flush entirely

  The same issue affects Date64 (line 987-993 write, line 1128 parse).
- **Impact**:
  1. Inlined Date32/Date64 values return NULL in queries (silent data loss)
  2. `ducklake_flush_inlined_data()` fails for tables with Date columns
- **Root cause**: The write side was improved (in what appears to be a prior fix) to produce human-readable dates, but the read side was not updated to parse them.
- **Suggested fix**: In both `parse_string_to_array` and `parse_inlined_column`, add Date32/Date64 handlers that parse ISO date strings (e.g., `NaiveDate::parse_from_str(s, "%Y-%m-%d")` then convert to epoch days). Alternatively, change the write side to emit numeric epoch-days values instead of formatted strings.
- **Effort**: S

### P2 — Medium

#### C3-004: Unchecked `as i64` casts for `buffer.len()` and `num_rows()` in DML execs

- **File(s)**:
  - `merge_exec.rs:484,554,564` — `buffer.len() as i64`, `batch_with_ids.num_rows() as i64`
  - `update_exec.rs:423,468,478` — same pattern
  - `delete_exec.rs:311,353` — `positions_to_delete.len() as i64`, `buffer.len() as i64`
- **Description**: The table writer's `finish()` method (line 807) properly uses `i64::try_from(buffer.len())` with error handling. However, the DML execution plans (DELETE, UPDATE, MERGE) all use unchecked `as i64` casts for buffer sizes and row counts. While file sizes >2^63 bytes are practically impossible, `positions_to_delete.len()` and `num_rows()` with `as i64` would silently wrap on overflow.
- **Impact**: Theoretical data corruption on pathologically large batches (file sizes stored as negative values, incorrect record counts in metadata).
- **Root cause**: DML execs were written separately from `table_writer.rs` where the safe casts were applied (F-028/F-042 fixes).
- **Suggested fix**: Replace `as i64` with `i64::try_from(...).map_err(...)` throughout DML execution paths, matching the pattern in `table_writer.rs`.
- **Effort**: S

#### C3-005: DML snapshots don't record `snapshot_changes` or carry `schema_version`

- **File(s)**: `delete_exec.rs:199-201`, `update_exec.rs:232-234`, `merge_exec.rs:317-319`
- **Description**: DELETE, UPDATE, and MERGE operations create snapshots via `writer.create_snapshot()` which inserts a bare snapshot row (no `schema_version`, no `snapshot_changes` entry). The `create_snapshot` method at `metadata_writer_sqlite.rs:770-779` uses `default` for `schema_version` (1) instead of inheriting from the previous snapshot's value. Also, no `ducklake_snapshot_changes` record is created.
- **Impact**:
  1. DML snapshots reset `schema_version` to 1 (the DDL default) instead of carrying forward the current version. This breaks DuckDB's schema version tracking — after any DML op, DuckDB may think a schema change occurred.
  2. No `changes_made` entry means DuckDB's change tracking / CDC functions can't identify what happened in these snapshots.
- **Suggested fix**: Either modify `create_snapshot()` to inherit `schema_version` from the latest snapshot (matching `write_transaction_inner`'s logic), or add optional parameters for `schema_version` and `changes_made`. DML callers should pass along appropriate change descriptions (e.g., `"deleted_from_table:3"`, `"updated_table:3"`).
- **Effort**: M

#### C3-006: Timestamp inlined data roundtrip silently corrupts values

- **File(s)**: `table_writer.rs:995-1003` (write fallback), `table.rs:1871` (read)
- **Description**: The write side (`arrow_array_value_to_string`) handles Timestamps via the generic fallback at line 995-1003 which uses Arrow's default display formatter, producing strings like `"2024-06-15T12:30:00"`. The read side (`table.rs:1871`) parses Timestamp as `parse_primitive!(Int64Builder, values)`, calling `"2024-06-15T12:30:00".parse::<i64>()` which fails.
  - In `table.rs`: Parse failure silently produces NULL (data loss)
  - The `table_writer.rs` flush path has the same issue via `parse_string_to_array`'s fallback (stores as string instead of Timestamp)
- **Impact**: Inlined Timestamp values silently become NULL in query results. Same pattern as C3-003 but for Timestamps.
- **Suggested fix**: Add explicit Timestamp serialization/deserialization handlers in both write and read paths. Write side should emit epoch-microsecond values; read side should parse them back.
- **Effort**: S

### P3 — Low

#### C3-007: `PRAGMA table_info` in writer uses single-quote wrapping instead of `quote_identifier`

- **File(s)**: `metadata_writer_sqlite.rs:2381`
- **Description**: `format!("PRAGMA table_info('{}')", inlined_table_name)` uses single-quote wrapping. The metadata providers (`metadata_provider_sqlite.rs:872`, `metadata_provider_duckdb.rs:587`) correctly use `quote_identifier`. While not exploitable because `inlined_table_name` is derived from `format!("ducklake_inlined_data_{}", table_id)` where `table_id` is an integer, it's inconsistent with the codebase convention established by the F-001 fix.
- **Suggested fix**: Change to `format!("PRAGMA table_info({})", quote_identifier(&inlined_table_name))` for consistency.
- **Effort**: S

#### C3-008: Inlined data writer methods use `\"{}\"` directly instead of `quote_identifier`

- **File(s)**: `metadata_writer_sqlite.rs:2253,2288,2311,2322,2405,2448`
- **Description**: Several methods in the inlined data writer path (`get_inlined_row_count`, `store_inlined_data`, `read_inlined_data`, `clear_inlined_data`) use `format!("... \"{}\" ...", inlined_table_name)` instead of `quote_identifier()`. The `quote_identifier` function properly handles embedded double-quotes by doubling them. The direct `\"{}\"` pattern does not.
- **Impact**: Not exploitable in practice because `inlined_table_name` is always `ducklake_inlined_data_{integer}`. However, it's inconsistent with the quote-everything convention.
- **Suggested fix**: Replace all `\"{}\"` patterns with `{}` using `quote_identifier(&inlined_table_name)`.
- **Effort**: S

#### C3-009: `parse_string_to_array` fallback silently downcasts unsupported types to string

- **File(s)**: `table_writer.rs:1129-1139`
- **Description**: The fallback branch in `parse_string_to_array` stores values as `StringBuilder` for any unrecognized type (Decimal128, Binary, etc.). This means the returned array is `StringArray` but the caller expects the array to match `data_type`. The `RecordBatch::try_new` call at line 1041-1044 may succeed (if the column is nullable and types happen to be compatible) or fail with a schema mismatch error.
- **Impact**: Inlined data flush for tables with Decimal, Binary, or other unsupported types will either fail or silently store incorrect data types.
- **Suggested fix**: Return an explicit error for unsupported types rather than silently falling back to string storage.
- **Effort**: S

---

## Spot-Check of Previous Fixes

### F-001 (SQL Injection) — VERIFIED CORRECT
All 4 metadata providers use `quote_identifier()` for dynamic column names in inlined data queries. The fix is consistent across `metadata_provider_sqlite.rs:897`, `metadata_provider_duckdb.rs:604`, `metadata_provider_postgres.rs:964`. However, the writer-side inlined data queries have inconsistencies (C3-007, C3-008 above).

### F-002 (Atomic DML) — PARTIALLY CORRECT
The `register_dml_files` method properly uses transactions in all 3 sqlx backends (SQLite, Postgres, MySQL). However, the method is missing `row_id_start` and `table_stats` updates (C3-001 above), which is a regression from the `register_data_file` behavior.

### F-009 (MERGE safe downcasts) — VERIFIED CORRECT
`merge_exec.rs:198-253` now uses `ok_or_else` for all downcasts and returns `DataFusionError` for unsupported types. The macro-based approach is clean and comprehensive, covering Boolean, all integer types, floats, strings, dates, timestamps, and Decimal128.

### F-033 (Virtual column CoalescePartitions) — VERIFIED CORRECT
`virtual_column_exec.rs:91-96` properly wraps the input in `CoalescePartitionsExec` when row-number-dependent virtual columns are requested and the input has multiple partitions. The `needs_single_partition` flag is correctly derived from `file_row_number || rowid`.

### F-023 (Encryption key decoding) — VERIFIED CORRECT
`encryption.rs:134-200` correctly implements hex-first decoding priority with explicit prefix support. The key length validation is thorough, checking for valid AES lengths (16, 24, 32 bytes) at every decode path.

### F-028-031 (Numeric safety) — MOSTLY CORRECT
- `table_writer.rs` `finish()` uses `i64::try_from(buffer.len())` — correct
- `table_writer.rs` `extract_column_stats` uses `i64::try_from(nc).unwrap_or(i64::MAX)` and `.saturating_add()` — correct
- `table_writer.rs` NaN handling filters NaN values with `.filter(|v| !v.is_nan())` — correct
- `should_replace_min/max` uses `total_cmp` for NaN-safe comparison — correct
- **However**, DML execs (merge/update/delete) still use unchecked `as i64` casts (C3-004)

---

## Summary

| Priority | Count | Findings |
|----------|-------|----------|
| P1 | 3 | C3-001, C3-002, C3-003 |
| P2 | 3 | C3-004, C3-005, C3-006 |
| P3 | 3 | C3-007, C3-008, C3-009 |
| **Total** | **9** | |

### Key Themes
1. **DML data file registration gap** (C3-001): The F-002 atomicity fix introduced `register_dml_files` but didn't carry over the `row_id_start` / `table_stats` logic from `register_data_file`. This is the most impactful finding.
2. **Inlined data roundtrip breakage** (C3-003, C3-006): Date and Timestamp types serialize as human-readable strings but parse back as numeric epoch values, causing silent data loss.
3. **MERGE exec cleanup gap** (C3-002): Unlike DELETE and UPDATE, MERGE doesn't clean up orphaned files on metadata failure.
4. **Inconsistent safety patterns** (C3-004, C3-007, C3-008): DML execs and writer inlined-data methods don't follow the safety patterns established in the table_writer and provider code.
