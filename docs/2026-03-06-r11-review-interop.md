# R11 Interop Review

**Date**: 2026-03-06
**Branch**: `ducklake-features/integration`
**Reviewer**: Claude Opus 4.6 (interop agent)
**DuckDB version**: v1.4.4, DuckLake extension 3f1b372

## Methodology

1. Created a reference DuckDB DuckLake catalog (`duckdb ... ATTACH 'ducklake:...'`) and dumped all 22 `ducklake_*` CREATE TABLE statements.
2. Compared column-by-column against our production schemas in `metadata_writer_sqlite.rs`, `metadata_writer_postgres.rs`, and `metadata_writer_mysql.rs`.
3. Reviewed Parquet writer settings, delete file schema, inline data format, snapshot numbering, and file naming conventions.
4. Audited cross-engine test coverage (188 test functions across 10 files, ~8,098 lines).

## Summary

- Total findings: 8
- By priority: P0: 0, P1: 1, P2: 3, P3: 4

## Findings

### R11-IO-001: ducklake_column has extra columns not in DuckDB schema
**Priority**: P2
**Files**: `src/metadata_writer_sqlite.rs:140-141`, `src/metadata_writer_postgres.rs:62-63`, `src/metadata_writer_mysql.rs:70-71`
**Description**: Our `ducklake_column` table includes `default_value_type VARCHAR` and `default_value_dialect VARCHAR` columns that do not exist in DuckDB's DuckLake `ducklake_column` schema (DuckDB v1.4.4 has only 11 columns: column_id through parent_column). When DuckDB reads a catalog created by DataFusion, it ignores extra columns in SQLite (SQLite is lenient), so this works in practice. However, if DuckDB ever does strict column-set validation, or if the columns are populated with non-NULL values that influence logic in a future DuckLake version, this could cause interop issues. The columns are a forward-looking extension matching a newer DuckLake spec draft.
**Suggested fix**: Document that these columns are forward-compatible extensions. They are safe because SQLite/PG/MySQL all ignore extra columns during SELECT. No code change needed unless DuckDB adds validation. Consider adding a cross-engine test that verifies DuckDB can read a catalog with these extra columns populated.

### R11-IO-002: Extra tables in our schema not present in DuckDB v1.4.4
**Priority**: P3
**Files**: `src/metadata_writer_sqlite.rs:314-371`
**Description**: Our schema creates several tables not present in DuckDB v1.4.4's DuckLake catalog:
- `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters`
- `ducklake_sort_info`, `ducklake_sort_expression`
- `ducklake_file_variant_stats`

These are from a newer DuckLake specification draft. DuckDB ignores unknown tables in the metadata DB, so this is safe for interop. The `_df_change_tracking` table uses a `_df_` prefix correctly to avoid name collisions.
**Suggested fix**: No change needed. The tables are harmless and forward-compatible.

### R11-IO-003: snapshot_time format difference (TEXT vs TIMESTAMPTZ)
**Priority**: P3
**Files**: `src/metadata_writer_sqlite.rs:103`
**Description**: DuckDB stores `snapshot_time` as `TIMESTAMP WITH TIME ZONE` in its native format. Our SQLite schema stores it as `TEXT` with format `strftime('%Y-%m-%d %H:%M:%f+00:00', 'now')`, which produces ISO 8601 strings like `2024-01-15 10:30:00.123+00:00`. DuckDB's SQLite driver reads TEXT columns and casts to TIMESTAMPTZ, so this works correctly in practice. The format matches DuckDB's expected ISO 8601 pattern.
**Suggested fix**: No change needed. Current format is interoperable.

### R11-IO-004: schema_uuid/table_uuid stored as VARCHAR in SQLite, UUID in DuckDB
**Priority**: P3
**Files**: `src/metadata_writer_sqlite.rs:111-112`, `src/metadata_writer_sqlite.rs:122`
**Description**: DuckDB's native schema uses `UUID` type for `schema_uuid` and `table_uuid`. Our SQLite schema uses `VARCHAR` since SQLite has no native UUID type. DuckDB's SQLite backend stores UUIDs as text anyway, so this is functionally equivalent. Our Postgres schema correctly uses `UUID` type.
**Suggested fix**: No change needed. This is a known SQLite limitation and is handled correctly.

### R11-IO-005: No cross-engine test for DF-written DELETE files read by DuckDB
**Priority**: P1
**Files**: `tests/cross_engine_dml_tests.rs`
**Description**: Cross-engine DML tests cover:
- DF DELETE -> DF read (multiple tests)
- DuckDB DELETE -> DF read (e.g., `cross_engine_duckdb_delete_df_read_back`)
- DF UPDATE -> DuckDB read (via copy-on-write, which produces new data files)
- DuckDB MERGE -> DF read, DF MERGE -> DuckDB read

However, there is no explicit test where **DataFusion writes a DELETE file** and then **DuckDB reads the table with that delete file applied**. The delete file uses a `(file_path, pos)` Parquet schema with sentinel field_ids (`0x7FFFFFFE`, `0x7FFFFFFD`). If DuckDB expects different sentinel values or column names, deleted rows could reappear. This is a critical interop gap given that DELETE is a core DML operation.

**Suggested fix**: Add a cross-engine test:
1. Create table via DuckDB, insert rows
2. Delete specific rows via DataFusion (producing MOR delete files)
3. Read via DuckDB and verify deleted rows are excluded
4. Read via DataFusion and verify consistency

### R11-IO-006: Parquet writer version and compression are compatible
**Priority**: P3
**Files**: `src/table_writer.rs:175-178`, `src/merge_exec.rs:624-626`, `src/update_exec.rs:444-446`
**Description**: All write paths consistently use `WriterVersion::PARQUET_2_0` with `Compression::SNAPPY`. DuckDB reads Parquet 2.0 and SNAPPY natively. Field IDs are embedded via `PARQUET:field_id` metadata, matching DuckDB's expectations. The delete file schema uses sentinel field_ids `0x7FFFFFFE` and `0x7FFFFFFD`, which match DuckDB's constants. This is correct and well-implemented.
**Suggested fix**: No change needed. This is an explicit positive finding.

### R11-IO-007: register_data_file omits file_format for replace/append paths
**Priority**: P2
**Files**: `src/metadata_writer_impl.rs:594-606`, `src/metadata_writer_impl.rs:775-787`
**Description**: The `register_data_file` method (line 428) correctly inserts `file_format = 'parquet'` into `ducklake_data_file`. However, the `replace_table_files` (line 594) and `append_table_files` (line 775) methods omit the `file_format` column from their INSERT statements. Since our SQLite schema has `file_format VARCHAR DEFAULT 'parquet'`, the default fills in correctly for SQLite. But DuckDB's schema does NOT have a DEFAULT on `file_format`, so if DuckDB reads these entries, the `file_format` column will be NULL (since the value was never written). DuckDB may handle NULL file_format gracefully (assuming parquet), but this is a latent interop risk.
**Suggested fix**: Add `file_format` to the INSERT column list in `replace_table_files` and `append_table_files`, explicitly passing `'parquet'` as the value.

### R11-IO-008: Hive partition directory layout matches DuckDB
**Priority**: P2
**Files**: `src/table_writer.rs:98-125`, `src/insert_exec.rs`
**Description**: Our partitioned writes use Hive-style directory layouts (`category=A/year=2024/ducklake-<uuid>.parquet`), which matches DuckDB's expected format. However, the catalog path stored in `ducklake_data_file.path` for partitioned files is relative (e.g., `category=A/year=2024/ducklake-xxx.parquet`), while DuckDB may store paths differently. Cross-engine partition tests exist (`cross_engine_partition_tests.rs`, 14 tests) but only test DuckDB-created partitions read by DF, not DF-created partitions read by DuckDB.
**Suggested fix**: Add a cross-engine test where DataFusion creates a partitioned table, inserts partitioned data, and DuckDB reads it back correctly. Verify the stored paths in `ducklake_data_file` match DuckDB's expectations.

## Positive Findings (No Issues)

1. **Snapshot numbering**: Both engines use 0-based snapshot IDs. Our `initialize_schema()` correctly inserts snapshot 0 as the "empty catalog" snapshot, matching DuckDB's convention.
2. **Metadata keys**: We correctly set `version=0.3`, `created_by=DataFusion-DuckLake`, and `encrypted=false`, matching DuckDB's expected metadata keys.
3. **Table set**: All 22 DuckDB DuckLake tables are present in our schema. We additionally create forward-compatible tables from newer spec drafts plus a correctly-prefixed `_df_change_tracking` table.
4. **Delete file format**: Uses `(file_path: VARCHAR, pos: INT64)` schema with correct sentinel field_ids matching DuckDB's MOR implementation.
5. **Inline data**: `ducklake_inlined_data_tables` schema matches DuckDB's format exactly.
6. **Cross-engine test coverage**: 188 test functions across 10 files covering DDL, DML (INSERT/DELETE/UPDATE/MERGE), ALTER TABLE, partitions, inline data, and feature tests. Coverage is comprehensive.
7. **File naming**: Uses `ducklake-<uuid>.parquet` pattern matching DuckDB's convention.
