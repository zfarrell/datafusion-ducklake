# R4 Correctness Review — 2026-03-03

**Reviewer**: correctness-review agent
**Branch**: `ducklake-features/integration`
**Scope**: All `src/*.rs` files — logic bugs, data integrity, edge cases, DuckDB interop
**Prior art**: R3 synthesis (50 deduplicated findings, 38 fixed as R3F-001 through R3F-050)

---

## Findings Summary

| Priority | Count | Description |
|----------|-------|-------------|
| P1       | 5     | Data integrity / DuckDB interop breakage |
| P2       | 2     | Partial failure cleanup / consistency |
| **Total**| **7** | |

---

## P1 — High Priority

### R4-C-001: `register_dml_files` does not update snapshot `next_file_id`

**Files**: `metadata_writer_sqlite.rs:1151-1254`, `metadata_writer_postgres.rs:997-1058`, `metadata_writer_mysql.rs:1099-1160`

**Description**: When DELETE, UPDATE, or MERGE operations call `register_dml_files`, new delete files (and possibly new data files) are inserted into `ducklake_delete_file` / `ducklake_data_file`. However, the snapshot's `next_file_id` is never updated afterward. Compare with the INSERT path in `write_setup()` (sqlite line 578-594) which correctly runs:

```sql
UPDATE ducklake_snapshot SET next_file_id = COALESCE((SELECT MAX(v) + 1 FROM (
    SELECT COALESCE(MAX(data_file_id), 0) AS v FROM ducklake_data_file
    UNION ALL SELECT COALESCE(MAX(delete_file_id), 0) FROM ducklake_delete_file
)), 0) WHERE snapshot_id = ?
```

The DML path skips this entirely. DuckDB reads `next_file_id` from the snapshot to allocate IDs for new files. A stale value causes ID collisions when DuckDB subsequently creates files.

**Suggested fix**: Add the same `UPDATE ducklake_snapshot SET next_file_id = ...` query at the end of `register_dml_files` in all three backend implementations (sqlite, postgres, mysql), inside the existing transaction before `tx.commit()`.

---

### R4-C-002: DML data files missing `register_column_stats`

**Files**: `metadata_writer_sqlite.rs:1193-1249` (data file loop in `register_dml_files`), `update_exec.rs:504-513`, `merge_exec.rs:608-617`

**Description**: When UPDATE or MERGE produce new data files (rewritten rows), they are registered via `register_dml_files`. This function inserts into `ducklake_data_file` and updates `ducklake_table_stats`, but never calls `register_column_stats`. Compare with the INSERT path in `table_writer.rs` which calls `register_column_stats` at lines 502 and 598.

Consequences:
1. `ducklake_file_column_stats` has no entries for DML-created data files — DuckDB's row-group pruning and predicate pushdown cannot use stats for these files
2. `ducklake_table_column_stats` (aggregate table-level stats) becomes stale because `register_column_stats` is the only function that recomputes it (sqlite line 1032-1057)

**Suggested fix**: Either (a) have the DML exec plans compute column stats (like `table_writer.rs` does) and pass them into `register_dml_files`, or (b) add a `column_stats` field to `DataFileInfo` and have `register_dml_files` call `register_column_stats` per file. Also trigger a recomputation of `ducklake_table_column_stats` after DELETE-only operations (which don't produce new data files but invalidate aggregate stats).

---

### R4-C-003: Postgres/MySQL `register_dml_files` missing `row_id_start` and `table_stats` updates

**Files**: `metadata_writer_postgres.rs:1039-1053`, `metadata_writer_mysql.rs:1141-1155`

**Description**: The SQLite implementation of `register_dml_files` was fixed in R3F-002 to set `row_id_start` on new data files and update `ducklake_table_stats` (record_count, next_row_id, file_size_bytes). However, the Postgres and MySQL implementations were not updated with the same fix. Compare:

- **SQLite** (lines 1193-1248): Fetches `next_row_id`, sets `row_id_start`, updates `ducklake_table_stats`
- **Postgres** (lines 1039-1053): Just `INSERT INTO ducklake_data_file` — no `row_id_start`, no `table_stats` update
- **MySQL** (lines 1141-1155): Same — no `row_id_start`, no `table_stats` update

This means UPDATE/MERGE data files in Postgres/MySQL catalogs have no `row_id_start` (NULL) and `ducklake_table_stats` doesn't reflect the new files.

**Suggested fix**: Port the R3F-002 fix (row_id_start + table_stats update) from the SQLite `register_dml_files` to the Postgres and MySQL implementations.

---

### R4-C-004: `ducklake_table_stats.record_count` never decremented after DELETE

**Files**: `metadata_writer_sqlite.rs:1164-1191` (delete file registration), `delete_exec.rs:386-401`

**Description**: When a DELETE operation removes rows, `register_dml_files` only processes delete files — it never adjusts `ducklake_table_stats.record_count`. The `record_count` is only incremented when data files are added (line 1225: `record_count = COALESCE(record_count, 0) + ?`). After a DELETE, the table's `record_count` overstates the actual number of live rows.

DuckDB uses `record_count` for query planning (cardinality estimation). Inflated counts lead to suboptimal plans — e.g., choosing hash joins over index lookups, or overallocating memory for aggregations.

**Suggested fix**: In `register_dml_files`, when processing delete files, compute the net new deletions (new `delete_count` minus previous `delete_count` for the same `data_file_id`) and subtract from `ducklake_table_stats.record_count`. This requires reading the old delete file's `delete_count` before ending it:

```sql
-- Before ending the old delete file, read its delete_count
SELECT COALESCE(delete_count, 0) FROM ducklake_delete_file
WHERE data_file_id = ? AND table_id = ? AND end_snapshot IS NULL
```

Then: `new_deletions = new_file.delete_count - old_delete_count` and `UPDATE ducklake_table_stats SET record_count = record_count - new_deletions`.

---

### R4-C-005: `end_table_files` (Replace mode) does not reset `ducklake_table_stats`

**Files**: `metadata_writer_sqlite.rs:1134-1149`

**Description**: In Replace (overwrite) mode, `end_table_files` sets `end_snapshot` on all existing data files for a table. However, it does not reset `ducklake_table_stats`. The INSERT path then additively updates `record_count`, `next_row_id`, and `file_size_bytes` on top of the stale values.

Example: Table has 1000 rows (record_count=1000). Replace-mode INSERT writes 500 new rows. Result: record_count=1500 (should be 500).

The `end_table_files` function also doesn't end associated delete files or clean up `ducklake_file_column_stats` for the ended data files, leaving orphaned stats rows.

**Suggested fix**: In `end_table_files`, after ending data files:
1. Reset `ducklake_table_stats`: `UPDATE ducklake_table_stats SET record_count = 0, next_row_id = 0, file_size_bytes = 0 WHERE table_id = ?`
2. End active delete files: `UPDATE ducklake_delete_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL`
3. (Optional) Clean up file column stats for ended files

---

## P2 — Medium Priority

### R4-C-006: `record_snapshot_changes` failure after `register_dml_files` success leaves orphaned files

**Files**: `delete_exec.rs:396-401`, `update_exec.rs:515-518`, `merge_exec.rs:619-622`

**Description**: In all three DML execution plans, the flow is:
1. `register_dml_files` — succeeds, metadata committed
2. `record_snapshot_changes` — fails with error

If step 2 fails, the uploaded Parquet files are now referenced by metadata (from step 1) but the error propagates to the caller. The `cleanup_orphaned_files` path only runs if `register_dml_files` fails (e.g., delete_exec line 388-393). There is no cleanup path for `record_snapshot_changes` failure.

This is a minor inconsistency rather than a data corruption issue: the metadata is actually committed successfully in step 1, so the files aren't truly orphaned. The issue is that the DML operation reports failure to the user even though data was persisted. A subsequent query will see the changes.

**Suggested fix**: Either (a) make `record_snapshot_changes` non-fatal (log warning, don't propagate error), or (b) move it inside the `register_dml_files` transaction so both succeed or fail atomically.

---

### R4-C-007: `delete_filter.rs` uses bare `as i64` cast instead of `try_from`

**File**: `delete_filter.rs:182`

**Description**: The `filter_batch` method computes `global_pos = self.row_offset + i as i64` where `i: usize`. While there is a guard at line 172 (`num_rows > u32::MAX as usize`), the cast `i as i64` is technically safe on 64-bit platforms but uses a different pattern from the DML exec files which consistently use `i64::try_from(i).map_err(...)` (e.g., delete_exec.rs:278-279, :300-301).

This is a consistency/style issue rather than a bug on current platforms, but `as` casts silently truncate on platforms where `usize > i64` (hypothetical 128-bit).

**Suggested fix**: Replace `i as i64` with `i64::try_from(i).map_err(...)` to match the project-wide pattern, or add a comment explaining why the `u32::MAX` guard is sufficient.

---

## Files Reviewed

All `src/*.rs` files were examined. The following were primary review targets:

| File | Lines | Notes |
|------|-------|-------|
| `metadata_writer_sqlite.rs` | ~2400 | Core metadata operations; bulk of findings |
| `metadata_writer_postgres.rs` | ~1100 | Postgres backend; R4-C-003 divergence |
| `metadata_writer_mysql.rs` | ~1200 | MySQL backend; R4-C-003 divergence |
| `delete_exec.rs` | 413 | DELETE execution plan |
| `update_exec.rs` | 529 | UPDATE execution plan |
| `merge_exec.rs` | 636 | MERGE execution plan |
| `insert_exec.rs` | ~350 | INSERT execution plan (reference for correct patterns) |
| `table_writer.rs` | ~900 | Parquet writer + column stats (reference) |
| `delete_filter.rs` | ~200 | MOR delete filtering |
| `table_changes.rs` | ~350 | CDC table_changes() |
| `table_deletions.rs` | ~400 | CDC table_deletions() |
| `virtual_column_exec.rs` | ~300 | Virtual column injection |
| `metadata_writer.rs` | ~420 | Trait definitions |
| `types.rs` | ~500 | Type mapping |
| `path_resolver.rs` | ~400 | Path resolution |
| `table.rs` | ~800 | Table provider |

---

## Cross-reference with R3

The following R3 items informed this review:
- **R3F-001** (table column stats): Fixed for INSERT path; R4-C-002 identifies the same gap in DML path
- **R3F-002** (row_id_start in DML): Fixed in SQLite only; R4-C-003 identifies Postgres/MySQL gap
- **R3F-011** (next_file_id): Fixed for INSERT path in `write_setup()`; R4-C-001 identifies the same gap in `register_dml_files`
- **R3F-013** (record_snapshot_changes): Added to all DML paths; R4-C-006 identifies the failure-handling gap
