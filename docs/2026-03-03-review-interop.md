# R4 Interop Review — 2026-03-03

## Overview

Review Cycle 4 focuses on DuckLake model compliance and cross-engine compatibility with DuckDB's native DuckLake extension. Methodology: created reference DuckLake catalogs with DuckDB CLI (INSERT, DELETE, UPDATE, inlined data) and compared DDL, data formats, and runtime behavior against our implementation.

Prior cycles (R1–R3) fixed 129 findings. This review covers only NEW issues not previously identified. Notably, R3F-013 (DML snapshot_changes) was marked FIXED but the fix introduced non-standard change token formats — those are reported here.

**Findings: 11 total** — 0 P0, 4 P1, 4 P2, 3 P3

---

## Findings

### P1 — High

#### R4-IO-001: UPDATE `snapshot_changes` uses non-standard `updated_table` token
- **File(s)**: `src/update_exec.rs:517`
- **Description**: Our UPDATE exec records `updated_table:{table_id}` in `ducklake_snapshot_changes.changes_made`. DuckDB records UPDATE as `inserted_into_table:{id},deleted_from_table:{id}` (comma-separated dual tokens). DuckDB's `ducklake_table_changes()` function parses these tokens to determine change types — UPDATE is inferred when both `inserted_into_table` and `deleted_from_table` appear for the same table in one snapshot.
- **Impact**: DuckDB's `ducklake_table_changes()` will NOT correctly classify our UPDATE operations. The `update_preimage` and `update_postimage` change types will be missing. CDC tracking is broken for DF-written UPDATE snapshots.
- **Verified**: Created reference catalog with DuckDB UPDATE → `changes_made = "inserted_into_table:1,deleted_from_table:1"` and `ducklake_table_changes()` returns `update_preimage`/`update_postimage` rows. Our format `updated_table:1` is not recognized.
- **Suggested fix**: Change `update_exec.rs:517` from `format!("updated_table:{}", table_id)` to `format!("inserted_into_table:{0},deleted_from_table:{0}", table_id)`.

#### R4-IO-002: MERGE `snapshot_changes` uses non-standard `merged_into_table` token
- **File(s)**: `src/merge_exec.rs:621`
- **Description**: MERGE exec records `merged_into_table:{table_id}`. DuckDB does not have a native MERGE operation; equivalent operations produce standard `inserted_into_table` / `deleted_from_table` tokens. Our non-standard token is not parseable by DuckDB's change tracking.
- **Impact**: Same as R4-IO-001 — `ducklake_table_changes()` broken for MERGE snapshots.
- **Suggested fix**: Use `inserted_into_table:{id},deleted_from_table:{id}` when MERGE produces both inserts and deletes; use only the applicable token when MERGE is insert-only or delete-only.

#### R4-IO-003: DML execs never register column statistics for new data files
- **File(s)**: `src/update_exec.rs`, `src/merge_exec.rs` (all DML paths that create new data files)
- **Description**: When UPDATE or MERGE creates new data files (for updated/inserted rows), they call `register_dml_files()` which registers the file metadata and updates `ducklake_table_stats`. However, neither exec calls `register_column_stats()` for the newly created data files. Compare with the INSERT path (`table_writer.rs:502`) which calls `register_column_stats()` for every new data file.
- **Impact**: (1) `ducklake_file_column_stats` is incomplete — new data files from UPDATE/MERGE have no per-file statistics. (2) `ducklake_table_column_stats` becomes stale — aggregated min/max/null stats don't reflect DML-created files. (3) DuckDB queries against DF-written catalogs cannot use file-level pruning for DML-created files, degrading query performance.
- **Verified**: Grepped `register_column_stats` in `*_exec.rs` — zero matches. INSERT path in `table_writer.rs` correctly calls it.
- **Suggested fix**: After writing each new data file in UPDATE/MERGE, compute and register column stats via `register_column_stats()`, then call `update_table_column_stats()` to refresh aggregated stats.

#### R4-IO-004: Delete file `file_path` column uses relative catalog path instead of resolved path
- **File(s)**: `src/delete_exec.rs:336-337`, `src/update_exec.rs:400`, `src/merge_exec.rs:477`
- **Description**: Delete files contain `(file_path VARCHAR, pos INT64)`. DuckDB populates `file_path` with the fully resolved path from the `data_path` root (e.g., `data_path/schema/table/ducklake-uuid.parquet`). Our code uses `table_file.file.path` which is the raw catalog-relative path (e.g., `ducklake-uuid.parquet` or just `uuid.parquet`).
- **Verified**: DuckDB reference delete file contains `file_path = "ref_interop.db.files/main/test_table/ducklake-019cb0fd-...parquet"`. Our code would write `file_path = "ducklake-{uuid}.parquet"` (just the filename).
- **Impact**: While our reader (and DuckDB's reader) currently matches delete files to data files via the catalog's `data_file_id` foreign key (not via `file_path` matching), the divergence means: (1) Any tool that reads delete files directly and matches by `file_path` will fail. (2) DuckDB's internal delete file reader may validate `file_path` against the resolved data file path. (3) Iceberg compatibility layer (the stated reason for the `file_path` column) requires resolved paths.
- **Suggested fix**: Resolve the full path (data_path + schema_path + table_path + file.path) before writing it into the delete file's `file_path` column.

---

### P2 — Medium

#### R4-IO-005: Inlined data table uses TEXT for all user columns; DuckDB uses original types
- **File(s)**: `src/metadata_writer_sqlite.rs:2450-2452`
- **Description**: Our inlined data table creation uses `TEXT` for all user columns: `format!(", {} TEXT", quote_identifier(col.name()))`. DuckDB creates inlined data tables with the original column types: e.g., `id INTEGER, "name" VARCHAR, val DOUBLE`.
- **Verified**: DuckDB reference: `CREATE TABLE ducklake_inlined_data_1_1(row_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, id INTEGER, "name" VARCHAR, val DOUBLE)`. Our code creates: `(row_id INTEGER, begin_snapshot INTEGER, end_snapshot INTEGER, id TEXT, name TEXT, val TEXT)`.
- **Impact**: (1) DuckDB reading our inlined data will get TEXT values where it expects typed values. SQLite's dynamic typing may mask this, but explicit type checking will fail. (2) Our reader already parses TEXT→typed values, so DF→DF is OK. (3) DuckDB→DF inlined data reading could also have issues if our reader expects TEXT but gets typed values (though `parse_inlined_column` handles string parsing). (4) Type metadata (`row_id BIGINT` vs `INTEGER`) differs but SQLite treats them equivalently.
- **Suggested fix**: Use `arrow_to_ducklake_type()` to convert column types to DuckLake type strings for the inlined data table DDL, matching DuckDB's convention.

#### R4-IO-006: Inlined data table naming convention differs from DuckDB
- **File(s)**: `src/metadata_writer_sqlite.rs:2432`
- **Description**: Our naming: `ducklake_inlined_data_{table_id}`. DuckDB naming: `ducklake_inlined_data_{table_id}_{schema_version}`. Both register the name in `ducklake_inlined_data_tables`, so the lookup-by-registry works regardless of naming. However, if DuckDB or any tool ever constructs the name directly (without registry lookup), our tables won't be found.
- **Impact**: Low functional impact currently (registry lookup works). Convention mismatch could cause issues with future DuckDB versions or third-party tools.
- **Suggested fix**: Use `format!("ducklake_inlined_data_{}_{}", table_id, schema_version)` to match DuckDB's convention.

#### R4-IO-007: Data file naming convention differs from DuckDB
- **File(s)**: `src/table_writer.rs:84`, `src/update_exec.rs:440`, `src/merge_exec.rs:546`
- **Description**: Our data files: `{uuid4}.parquet` (e.g., `a3b2c1d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6.parquet`). DuckDB data files: `ducklake-{uuid7}.parquet` (e.g., `ducklake-019cb0fd-a7d1-75a7-ab5f-46a26ac4b7f4.parquet`). Delete files already use the DuckDB convention (`ducklake-{uuid}-delete.parquet`).
- **Impact**: Not functionally broken (path stored in catalog). But: (1) Inconsistency between data file naming (no `ducklake-` prefix) and delete file naming (has `ducklake-` prefix). (2) Makes it harder to identify DF-written vs DuckDB-written files in mixed catalogs. (3) DuckDB uses UUID v7 (time-ordered) while we use UUID v4 (random) — both produce valid UUIDs but ordering properties differ.
- **Suggested fix**: Use `format!("ducklake-{}.parquet", Uuid::new_v4())` for data files to match the delete file convention and DuckDB's naming.

#### R4-IO-008: Delete file Parquet schema missing field_id values
- **File(s)**: `src/table.rs:82-86`
- **Description**: Our `delete_file_schema()` creates `Field::new("file_path", Utf8, false)` and `Field::new("pos", Int64, false)` without Parquet field IDs. DuckDB writes delete files with sentinel field IDs: `file_path` has `field_id=2147483646` (0x7FFFFFFE), `pos` has `field_id=2147483645` (0x7FFFFFFD).
- **Verified**: `parquet_schema()` on DuckDB reference delete file shows `field_id=2147483646` for file_path, `field_id=2147483645` for pos.
- **Impact**: Readers that use field_id-based column resolution (rather than column name-based) may not correctly identify our delete file columns. Our reader uses name-based resolution so DF→DF is fine.
- **Suggested fix**: Add `PARQUET:field_id` metadata to delete file fields: `Field::new("file_path", Utf8, false).with_metadata(HashMap::from([("PARQUET:field_id".into(), "2147483646".into())]))` and similar for pos with `2147483645`.

---

### P3 — Low

#### R4-IO-009: SQLite `schedule_start` uses TEXT type; DuckDB uses TIMESTAMP WITH TIME ZONE
- **File(s)**: `src/metadata_writer_sqlite.rs:213`
- **Description**: `ducklake_files_scheduled_for_deletion.schedule_start` is `TEXT` in our SQLite DDL. DuckDB's reference DDL uses `TIMESTAMP WITH TIME ZONE`. PostgreSQL writer correctly uses `TIMESTAMP WITH TIME ZONE`.
- **Impact**: Format differences in timestamp strings may cause parsing failures when DuckDB reads our scheduled-deletion records, or vice versa. Low impact because this table is for garbage collection (not query correctness).
- **Suggested fix**: Use a consistent timestamp format string (ISO 8601 with timezone) when writing to this column, matching `strftime('%Y-%m-%d %H:%M:%f+00:00', 'now')` pattern used elsewhere.

#### R4-IO-010: Data file naming inconsistency between table_writer and DML execs
- **File(s)**: `src/table_writer.rs:84` vs `src/delete_exec.rs:327`
- **Description**: INSERT path (`table_writer.rs`): `format!("{}.parquet", Uuid::new_v4())` — no `ducklake-` prefix. DELETE/UPDATE/MERGE paths: `format!("ducklake-{}-delete.parquet", Uuid::new_v4())` — with `ducklake-` prefix. Within the same codebase, data files and delete files follow different naming conventions.
- **Impact**: Cosmetic inconsistency. No functional impact since paths are stored in catalog.
- **Suggested fix**: Standardize on `ducklake-{uuid}.parquet` for data files and `ducklake-{uuid}-delete.parquet` for delete files.

#### R4-IO-011: DDL column ordering differs from DuckDB reference
- **File(s)**: All `src/metadata_writer_*.rs` DDL sections
- **Description**: Our column order in DDL statements differs from DuckDB's reference. Example for `ducklake_column`:
  - DuckDB: `column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column`
  - Ours: `column_id, table_id, column_name, column_type, column_order, nulls_allowed, initial_default, default_value, parent_column, default_value_type, default_value_dialect, begin_snapshot, end_snapshot`
- **Impact**: No functional impact — all SQL queries use column names, not positional references. SQLite, PostgreSQL, and MySQL all support named column access. DuckDB reads by column name. The extra columns (`default_value_type`, `default_value_dialect`) are covered by R3F-028 (deferred).
- **Suggested fix**: Not required for functionality. If desired for convention alignment, reorder DDL columns to match DuckDB reference.

---

## Cross-Cutting Observations

### 1. R3F-013 Fix Created New Problems
The R3F-013 fix added snapshot_changes recording for DML operations, but used non-standard change tokens (`updated_table`, `merged_into_table`). The correct DuckDB format uses only `inserted_into_table` and `deleted_from_table` tokens — UPDATE and MERGE are expressed as combinations of these primitives.

### 2. Column Statistics Gap for DML-Created Files
The INSERT path (via `table_writer.rs`) correctly calls `register_column_stats()` for every file, but the DML path (via `register_dml_files()`) does not. This creates a growing gap in `ducklake_file_column_stats` and `ducklake_table_column_stats` as more DML operations occur.

### 3. Delete File Content Format Divergence
DuckDB writes fully-resolved paths in the delete file's `file_path` column; we write relative catalog paths. While both implementations currently use catalog metadata (not file content) to match delete files to data files, the divergence breaks the contract described in the DuckLake format documentation.

### 4. Inlined Data Type Mismatch
SQLite's dynamic typing masks the TEXT-vs-typed-column difference for DF→DF roundtrips, but cross-engine scenarios (DF-write → DuckDB-read or vice versa) may expose type coercion issues.

---

## Recommended Fix Priority

| Finding | Priority | Effort | Description |
|---------|----------|--------|-------------|
| R4-IO-001 | P1 | S | Fix UPDATE snapshot_changes format |
| R4-IO-002 | P1 | S | Fix MERGE snapshot_changes format |
| R4-IO-003 | P1 | M | Add column stats registration in DML execs |
| R4-IO-004 | P1 | S | Resolve full path for delete file file_path column |
| R4-IO-005 | P2 | S | Use typed columns in inlined data tables |
| R4-IO-006 | P2 | S | Match DuckDB inlined data table naming convention |
| R4-IO-007 | P2 | S | Add `ducklake-` prefix to data file names |
| R4-IO-008 | P2 | S | Add field_ids to delete file Parquet schema |
| R4-IO-009 | P3 | S | Document or fix schedule_start type |
| R4-IO-010 | P3 | S | Standardize file naming across paths |
| R4-IO-011 | P3 | N/A | DDL column ordering (informational) |
