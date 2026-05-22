# Review Cycle 6 — Correctness Review

**Date**: 2026-03-04
**Reviewer**: correctness-review agent
**Branch**: `ducklake-features/integration`
**Scope**: All Rust source files, focusing on logic bugs, edge cases, data integrity, and security
**Codex**: Yes (write path + catalog DDL)

---

## Findings Summary

| Severity | Count |
|----------|-------|
| P0       | 0     |
| P1       | 2     |
| P2       | 3     |
| P3       | 4     |
| **Total**| **9** |

---

## P1 — High

### R6-C-001: Backend parity — PG/MySQL missing record_count decrement on DELETE

- **Severity**: P1
- **Category**: Data Integrity / Backend Parity
- **Files**:
  - `src/metadata_writer_sqlite.rs:1458-1517` (correct — decrements)
  - `src/metadata_writer_postgres.rs:1025-1141` (missing decrement)
  - `src/metadata_writer_mysql.rs:1156-1272` (missing decrement)
- **Description**: The SQLite `register_dml_files()` tracks `total_net_new_deletions` (net of old delete count vs new delete count) and decrements `ducklake_table_stats.record_count` accordingly (line 1510: `SET record_count = COALESCE(record_count, 0) - ?`). Neither the PostgreSQL nor MySQL implementations perform this decrement — they only increment `record_count` when new data files are added.
- **Impact**: After DELETE/UPDATE/MERGE operations, `record_count` in `ducklake_table_stats` diverges across backends:
  - SQLite: reflects live rows (total minus deleted)
  - PG/MySQL: reflects total rows ever inserted (ignores deletes)
  This causes inconsistent behavior if any code path relies on `record_count` for statistics, query planning estimates, or cross-engine interop validation.
- **Suggested Fix**: Port the `total_net_new_deletions` logic from SQLite to PG and MySQL `register_dml_files()` implementations. Query the old `delete_count` before ending the existing delete file, compute net new deletions, and decrement `record_count` in the same transaction.
- **Effort**: Medium (2-3 hours)

### R6-C-002: replace_table_files() missing table_id in column stats INSERT (SQLite)

- **Severity**: P1
- **Category**: Data Integrity / SQL Bug
- **File**: `src/metadata_writer_sqlite.rs:1380-1391`
- **Description**: The `replace_table_files()` method inserts into `ducklake_file_column_stats` with columns `(data_file_id, column_id, null_count, min_value, max_value)` — omitting `table_id`. The table schema (line 128-138) defines `table_id INTEGER NOT NULL`.
- **Impact**: In SQLite (non-STRICT mode), the missing column defaults to NULL despite the NOT NULL declaration. This means column stats after compaction will have `table_id = NULL`, causing:
  1. `recompute_table_column_stats()` (line 856: `WHERE fcs.table_id = ?`) will not find these rows
  2. Table-level column stats will be silently empty/wrong after compaction
  3. DuckDB interop may reject catalogs with NULL table_id in this table
- **Suggested Fix**: Add `table_id` to the INSERT statement and bind `table_id` parameter:
  ```sql
  INSERT INTO ducklake_file_column_stats (data_file_id, table_id, column_id, null_count, min_value, max_value)
  VALUES (?, ?, ?, ?, ?, ?)
  ```
- **Effort**: Low (15 minutes)

---

## P2 — Medium

### R6-C-003: replace_table_files() omits row_id_start for compacted files (SQLite)

- **Severity**: P2
- **Category**: Data Integrity
- **File**: `src/metadata_writer_sqlite.rs:1364-1365, 1414-1422`
- **Description**: When `replace_table_files()` inserts new data files after compaction (line 1364), it omits `row_id_start` from the INSERT. Subsequently, it resets `next_row_id = total_record_count` (line 1416-1421). The compacted data files will have `row_id_start = NULL` (SQLite default).
- **Impact**: The virtual `rowid` column (`virtual_column_exec.rs`) computes row IDs as `row_id_start + offset`. With NULL `row_id_start`, rows in compacted files will have incorrect/NULL row IDs. Additionally, future appends after compaction may produce overlapping row ID ranges if the NULL row_id_start is treated as 0.
- **Suggested Fix**: Track cumulative row_id_start during the compaction INSERT loop and assign sequential values. Set `next_row_id` to the actual cumulative total, not just `total_record_count`.
- **Effort**: Low (30 minutes)

### R6-C-004: new_next_row_id i64 overflow not checked (all backends)

- **Severity**: P2
- **Category**: Arithmetic Overflow
- **Files**:
  - `src/metadata_writer_sqlite.rs:1551`
  - `src/metadata_writer_postgres.rs:1096`
  - `src/metadata_writer_mysql.rs:1227`
- **Description**: All three backends compute `let new_next_row_id = row_id_start + file.record_count;` without overflow checking. Other arithmetic in the codebase (e.g., `merge_exec.rs:654`, `delete_exec.rs:278`) correctly uses `i64::try_from()` with error handling.
- **Impact**: For extremely large tables, row_id_start + record_count could overflow i64, wrapping to a negative value. This would corrupt `next_row_id` in `ducklake_table_stats` and cause subsequent INSERTs to assign incorrect `row_id_start` values.
- **Suggested Fix**: Use `row_id_start.checked_add(file.record_count).ok_or_else(|| ...)` and return an error on overflow.
- **Effort**: Low (15 minutes per backend, 45 minutes total)

### R6-C-005: Concurrent DML lost-delete race (PG/MySQL)

- **Severity**: P2
- **Category**: Concurrency / Data Integrity
- **Files**:
  - `src/delete_exec.rs:191,228` (existing_deletes snapshotted at table creation)
  - `src/update_exec.rs:211` (same pattern)
  - `src/merge_exec.rs:426` (same pattern)
  - `src/metadata_writer_postgres.rs:1801+` (checked_write_transaction only checks DDL)
- **Description**: The `existing_deletes` map is captured when `DuckLakeTable` is created (during query planning). The DELETE/UPDATE/MERGE execution plans read this snapshot, merge new deletions with existing ones, and write a replacement delete file. If another concurrent writer commits additional deletes for the same data file between the snapshot and the `register_dml_files` commit, the second writer's delete file will overwrite the first's — losing the first writer's new deletions.

  The `begin_checked_write_transaction()` detects DDL conflicts (table drops, schema drops) but does NOT detect concurrent DML on the same data files.
- **Impact**: On PostgreSQL and MySQL (which allow true concurrent connections), concurrent DELETE/UPDATE/MERGE operations on overlapping data files can silently lose delete positions, effectively resurrecting previously-deleted rows. SQLite is less affected due to file-level write serialization.
- **Suggested Fix**: Either:
  1. Extend `begin_checked_write_transaction()` to check for concurrent DML on the same data files (compare delete file versions), or
  2. Add per-data-file optimistic locking (check that delete file version hasn't changed since snapshot)
- **Effort**: High (4-8 hours)

---

## P3 — Low

### R6-C-006: stat_value_less_than f64 fallback loses precision for large integers

- **Severity**: P3
- **Category**: Numeric Precision
- **File**: `src/metadata_writer_sqlite.rs:990-993`
- **Description**: The `stat_value_less_than()` function tries `i128` parsing first, then falls back to `f64`. For numeric types with values that don't parse as i128 (e.g., very large DECIMAL values or values with exponent notation), the f64 fallback has only 53 bits of mantissa precision.
- **Impact**: Min/max column statistics could be slightly incorrect for decimal values exceeding 2^53, potentially causing incorrect row group pruning. Extremely unlikely in practice.
- **Suggested Fix**: Consider parsing as `f64` only for known float types, or use a decimal parsing library for DECIMAL/NUMERIC types.
- **Effort**: Low (30 minutes)

### R6-C-007: recompute_table_column_stats join could be more defensive

- **Severity**: P3
- **Category**: Defense in Depth
- **File**: `src/metadata_writer_sqlite.rs:858`
- **Description**: The JOIN `INNER JOIN ducklake_column c ON fcs.column_id = c.column_id` does not include `AND c.table_id = ?` as a secondary filter. Column IDs are globally unique (generated via `MAX(column_id) + 1`), so this is safe in practice. However, adding the table_id filter would provide defense against data corruption scenarios where column_id uniqueness is violated.
- **Impact**: Negligible under normal operation. In data corruption scenarios, could produce incorrect column type lookups.
- **Suggested Fix**: Add `AND c.table_id = ?` to the JOIN condition, binding `table_id`.
- **Effort**: Low (10 minutes)

### R6-C-008: Default replace_table_files() is non-atomic (trait default)

- **Severity**: P3
- **Category**: Atomicity
- **File**: `src/metadata_writer.rs:493-517`
- **Description**: The default `replace_table_files()` implementation calls multiple individual trait methods (`end_table_files`, `register_data_file`, `register_column_stats`, `register_file_partition_value`) without a transaction wrapper. If any call fails partway through, the table could be left in an inconsistent state (some old files ended, some new files registered).

  The SQLite backend overrides this with a transactional implementation (line 1340). PG and MySQL use the default non-atomic version.
- **Impact**: If PG/MySQL compaction fails mid-operation, the table metadata could be left in a partially-replaced state. This would require manual recovery.
- **Suggested Fix**: Override `replace_table_files()` in PG and MySQL backends with transactional implementations (matching SQLite).
- **Effort**: Medium (2-3 hours per backend)

### R6-C-009: Partition column_index not bounds-checked before array access

- **Severity**: P3
- **Category**: Defensive Programming
- **File**: `src/insert_exec.rs:542, 664`
- **Description**: Partition routing uses `batch.column(pc.column_index)` without validating that `column_index < batch.num_columns()`. The `column_index` comes from partition metadata which is validated during table creation, so this should always be in bounds. However, corrupted metadata could cause a panic (array index out of bounds).
- **Impact**: Runtime panic instead of a handled error for corrupted partition metadata. Very unlikely in practice since metadata is validated during table creation.
- **Suggested Fix**: Add bounds check: `batch.columns().get(pc.column_index).ok_or_else(|| DataFusionError::Internal(...))?`
- **Effort**: Low (15 minutes)

---

## Codex Findings Cross-Reference

Codex was run against the write path and catalog DDL files. It produced 5 findings:

| Codex Finding | Our Assessment | Action |
|---------------|----------------|--------|
| #1: Concurrent DELETE/UPDATE lost-delete race | Valid — captured as R6-C-005 | Included |
| #2: replace_table_files missing table_id in column stats | Valid — captured as R6-C-002 | Included |
| #3: recompute_table_column_stats cross-table join | False positive — column_id is globally unique | Noted as R6-C-007 (defense-in-depth) |
| #4: replace_table_files missing row_id_start | Valid — captured as R6-C-003 | Included |
| #5: Partition column_index not bounds-checked | Valid — captured as R6-C-009 | Included |

---

## Files Reviewed

| File | Lines | Status |
|------|-------|--------|
| `src/insert_exec.rs` | 1049 | Full review |
| `src/delete_exec.rs` | 423 | Full review |
| `src/update_exec.rs` | 580 | Full review |
| `src/merge_exec.rs` | 888 | Full review |
| `src/metadata_writer.rs` | 824 | Full review |
| `src/metadata_writer_sqlite.rs` | ~4066 | Full review (in chunks) |
| `src/metadata_writer_postgres.rs` | ~2100 | Partial (DDL, write_transaction, register_dml_files) |
| `src/metadata_writer_mysql.rs` | ~2500 | Partial (DDL, write_transaction, register_dml_files) |
| `src/metadata_writer_validation.rs` | 871 | Full review |
| `src/catalog.rs` | 426 | Full review |
| `src/delete_filter.rs` | 357 | Full review |
| `src/table.rs` | ~2070 | Partial (first 300 lines + targeted sections) |
| `src/types.rs` | ~1550 | Full review (saved to file) |
| `src/path_resolver.rs` | ~900 | Full review (saved to file) |
| `src/table_writer.rs` | ~2020 | Partial (first 300 lines + targeted sections) |

---

## Observations (Not Findings)

1. **SQL injection prevention**: All SQL queries use parameterized queries via sqlx. Dynamic identifier quoting uses `quote_identifier()` which correctly doubles internal double-quotes. No SQL injection vectors found.

2. **Path traversal prevention**: `path_resolver.rs` includes null byte validation, percent-decode for traversal detection, and `has_dotdot_component()`. URL-encoding for partition values prevents path traversal. No path traversal vectors found.

3. **Error handling**: Write path methods properly clean up orphaned Parquet files when metadata commit fails (`cleanup_orphaned_files`). This is implemented consistently across DELETE, UPDATE, and MERGE.

4. **Overflow handling**: Most arithmetic operations use `i64::try_from()` or `u64::try_from()` with proper error propagation. The exception is `new_next_row_id` (R6-C-004).

5. **MERGE source_match_masks**: The per-file allocation of `source_match_masks` (line 446) was initially flagged as a potential issue but is correct. The global `source_match_count` array (line 396) tracks cross-file match counts, while `source_match_masks` tracks per-file matches for UPDATE row collection. The R3F-033 violation check correctly uses the global counter.

6. **Snapshot isolation**: All DML operations create snapshots atomically and use snapshot IDs consistently for begin/end tracking.
