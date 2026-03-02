# Interoperability Review — 2026-03-02

## Summary

Comprehensive comparison of our SQLite/Postgres/MySQL metadata writer output against DuckDB's DuckLake extension (v0.3). While the roundtrip tests pass (DF writes → DuckDB reads, DuckDB writes → DF reads), several schema and behavioral differences create fragility and forward-compatibility risks. The most critical issues are: delete file format mismatch (`POSITION_DELETES` vs `parquet`), missing `row_id_start` tracking, missing UUID generation, and divergent `snapshot_changes` format.

**Reference catalog**: Created via DuckDB v1.4.x with DuckLake extension v0.3.

## Reference Schema Comparison

### ducklake_metadata

| Column | DuckDB | Ours | Match? |
|--------|--------|------|--------|
| key | VARCHAR NOT NULL | VARCHAR NOT NULL | OK |
| value | VARCHAR NOT NULL | VARCHAR NOT NULL | OK |
| scope | VARCHAR | VARCHAR | OK |
| scope_id | BIGINT | INTEGER | Type mismatch (SQLite-specific, functionally OK) |

**Values written**:
- DuckDB writes: `version=0.3`, `created_by=DuckDB xxxx`, `data_path=file:///...`, `encrypted=false`
- We write: `version=0.3`, `created_by=DataFusion-DuckLake`, `data_path=...` (via `set_data_path`)
- **Missing**: We never write `encrypted=false`

### ducklake_snapshot

| Column | DuckDB | Ours | Match? |
|--------|--------|------|--------|
| snapshot_id | BIGINT PK | INTEGER PK | Type difference (functionally OK in SQLite) |
| snapshot_time | TIMESTAMP WITH TIME ZONE | TEXT DEFAULT CURRENT_TIMESTAMP | Format mismatch |
| schema_version | BIGINT | INTEGER DEFAULT 1 | **Different default**: DuckDB starts at 0, ours at 1 |
| next_catalog_id | BIGINT | INTEGER DEFAULT 0 | **Different init**: DuckDB snapshot 0 has next_catalog_id=1 |
| next_file_id | BIGINT | INTEGER DEFAULT 0 | OK |

**snapshot_time format**: DuckDB stores ISO 8601 with timezone offset (e.g., `2026-03-02 03:57:26.271137+01`). SQLite CURRENT_TIMESTAMP produces `YYYY-MM-DD HH:MM:SS` without timezone. DuckDB's reader likely tolerates this since it casts from TEXT, but it's a fidelity loss.

**schema_version**: DuckDB's snapshot 0 has `schema_version=0`; new snapshots after CREATE TABLE get `schema_version=1`. Our default for new snapshots is `1`, which may confuse DuckDB's schema version tracking.

### ducklake_schema

| Column | DuckDB | Ours | Match? |
|--------|--------|------|--------|
| schema_id | BIGINT PK | INTEGER PK | OK |
| schema_uuid | UUID | VARCHAR | We never generate UUIDs (left NULL) |
| begin_snapshot | BIGINT | INTEGER NOT NULL | OK |
| end_snapshot | BIGINT | INTEGER | OK |
| schema_name | VARCHAR | VARCHAR NOT NULL | OK |
| path | VARCHAR | VARCHAR NOT NULL DEFAULT '' | OK |
| path_is_relative | BOOLEAN | BOOLEAN NOT NULL DEFAULT 1 | OK |

**Column ordering**: DuckDB has `schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative`. Ours has different ordering (schema_id, schema_uuid, schema_name, path, path_is_relative, begin_snapshot, end_snapshot). This is irrelevant for SQLite (column-name based access) but matters for positional queries.

### ducklake_table

| Column | DuckDB | Ours | Match? |
|--------|--------|------|--------|
| table_id | BIGINT | INTEGER NOT NULL | OK |
| table_uuid | UUID | VARCHAR | We never generate UUIDs (left NULL) |
| begin_snapshot | BIGINT | INTEGER NOT NULL | OK |
| end_snapshot | BIGINT | INTEGER | OK |
| schema_id | BIGINT | INTEGER NOT NULL | OK |
| table_name | VARCHAR | VARCHAR NOT NULL | OK |
| path | VARCHAR | VARCHAR NOT NULL DEFAULT '' | OK |
| path_is_relative | BOOLEAN | BOOLEAN NOT NULL DEFAULT 1 | OK |

### ducklake_column

| Column | DuckDB | Ours | Match? |
|--------|--------|------|--------|
| column_id | BIGINT | INTEGER NOT NULL | OK |
| begin_snapshot | BIGINT | INTEGER NOT NULL | OK |
| end_snapshot | BIGINT | INTEGER | OK |
| table_id | BIGINT | INTEGER NOT NULL | OK |
| column_order | BIGINT | INTEGER NOT NULL | OK |
| column_name | VARCHAR | VARCHAR NOT NULL | OK |
| column_type | VARCHAR | VARCHAR NOT NULL | OK |
| initial_default | VARCHAR | VARCHAR | OK |
| default_value | VARCHAR | VARCHAR | OK |
| nulls_allowed | BOOLEAN | BOOLEAN DEFAULT 1 | OK |
| parent_column | BIGINT | INTEGER | OK |
| **default_value_type** | N/A | VARCHAR | **Extra column** (DuckDB does not have this) |
| **default_value_dialect** | N/A | VARCHAR | **Extra column** (DuckDB does not have this) |

### ducklake_data_file

| Column | DuckDB | Ours | Match? |
|--------|--------|------|--------|
| data_file_id | BIGINT PK | INTEGER PK | OK |
| table_id | BIGINT | INTEGER NOT NULL | OK |
| begin_snapshot | BIGINT | INTEGER NOT NULL | OK |
| end_snapshot | BIGINT | INTEGER | OK |
| file_order | BIGINT | INTEGER | OK |
| path | VARCHAR | VARCHAR NOT NULL | OK |
| path_is_relative | BOOLEAN | BOOLEAN NOT NULL DEFAULT 1 | OK |
| file_format | VARCHAR | VARCHAR DEFAULT 'PARQUET' | **Value mismatch**: DuckDB writes `parquet` (lowercase) |
| record_count | BIGINT | INTEGER | OK |
| file_size_bytes | BIGINT | INTEGER NOT NULL | OK |
| footer_size | BIGINT | INTEGER | OK |
| row_id_start | BIGINT | INTEGER | **Never populated** by our writer |
| partition_id | BIGINT | INTEGER | OK |
| encryption_key | VARCHAR | VARCHAR | OK |
| partial_file_info | VARCHAR | VARCHAR | OK |
| mapping_id | BIGINT | INTEGER | OK |
| **partial_max** | N/A | INTEGER | **Extra column** (DuckDB does not have this) |

### ducklake_delete_file

| Column | DuckDB | Ours | Match? |
|--------|--------|------|--------|
| delete_file_id | BIGINT PK | INTEGER PK | OK |
| table_id | BIGINT | INTEGER NOT NULL | OK |
| begin_snapshot | BIGINT | INTEGER NOT NULL | OK |
| end_snapshot | BIGINT | INTEGER | OK |
| data_file_id | BIGINT | INTEGER NOT NULL | OK |
| path | VARCHAR | VARCHAR NOT NULL | OK |
| path_is_relative | BOOLEAN | BOOLEAN NOT NULL DEFAULT 1 | OK |
| format | VARCHAR | VARCHAR DEFAULT 'POSITION_DELETES' | **Value mismatch**: DuckDB writes `parquet` |
| delete_count | BIGINT | INTEGER | OK |
| file_size_bytes | BIGINT | INTEGER NOT NULL | OK |
| footer_size | BIGINT | INTEGER | OK |
| encryption_key | VARCHAR | VARCHAR | OK |
| **partial_max** | N/A | INTEGER | **Extra column** (DuckDB does not have this) |

### ducklake_snapshot_changes

| Column | DuckDB | Ours | Match? |
|--------|--------|------|--------|
| snapshot_id | BIGINT PK | INTEGER PK | OK |
| changes_made | VARCHAR | VARCHAR | **Format mismatch** (see below) |
| author | VARCHAR | VARCHAR | OK |
| commit_message | VARCHAR | VARCHAR | OK |
| commit_extra_info | VARCHAR | VARCHAR | OK |

### ducklake_schema_versions

| Column | DuckDB | Ours | Match? |
|--------|--------|------|--------|
| begin_snapshot | BIGINT | INTEGER | OK |
| schema_version | BIGINT | INTEGER | OK |
| **table_id** | N/A | INTEGER | **Extra column** (DuckDB does not have this) |

### Non-DuckLake Table

| Table | Origin | Issue |
|-------|--------|-------|
| `_df_change_tracking` | DataFusion-only | Extra table for conflict detection. Not part of DuckLake spec. DuckDB ignores it on ATTACH (verified by roundtrip tests), but it's a DF-specific extension. |

---

## Findings

### INTEROP-1: Delete file format default mismatch (Severity: P1)
- **File(s)**: `src/metadata_writer_sqlite.rs:106`, `src/metadata_writer_postgres.rs:97`, `src/metadata_writer_mysql.rs:109`
- **Description**: Our DDL default for `ducklake_delete_file.format` is `'POSITION_DELETES'`, but DuckDB writes `'parquet'` (lowercase). Since our `register_delete_file` INSERT doesn't explicitly set the `format` column, new delete files get the default `'POSITION_DELETES'`. DuckDB may not recognize this value.
- **Impact**: DF-created delete files may not be properly interpreted by DuckDB's reader if it checks the format field. The roundtrip tests pass because DuckDB's reader may not strictly validate this field, but it creates a latent compatibility risk.
- **Suggestion**: Change default to `'parquet'` (matching DuckDB) and explicitly set `format = 'parquet'` in `register_delete_file`.
- **Effort**: S

### INTEROP-2: Data file file_format casing mismatch (Severity: P2)
- **File(s)**: `src/metadata_writer_sqlite.rs:88`, `src/metadata_writer_postgres.rs:80`, `src/metadata_writer_mysql.rs:90`
- **Description**: Our DDL default for `ducklake_data_file.file_format` is `'PARQUET'` (uppercase), but DuckDB writes `'parquet'` (lowercase). Since our `register_data_file` doesn't explicitly set `file_format`, new files get uppercase.
- **Impact**: If DuckDB does case-sensitive comparison on file_format, our files could be misinterpreted. Currently tolerated by DuckDB's reader.
- **Suggestion**: Change default to `'parquet'` (lowercase) to match DuckDB exactly.
- **Effort**: S

### INTEROP-3: Missing `row_id_start` in data file registration (Severity: P1)
- **File(s)**: `src/metadata_writer_sqlite.rs:819`, `src/metadata_writer.rs:200-215`
- **Description**: Our `register_data_file` never sets `row_id_start`. DuckDB assigns monotonically increasing row IDs (`row_id_start = 0` for first file, `row_id_start = N` for next file where N = cumulative record count). This is used for:
  1. Delete file position mapping (global row IDs)
  2. Virtual column `rowid` generation
  3. DuckDB's internal row tracking
- **Impact**: Without `row_id_start`, DuckDB cannot correctly correlate delete file positions with data file rows in DF-created catalogs. Currently works because our delete files use file-local positions, but breaks if DuckDB's reader expects global positions. Virtual column `rowid` will return incorrect values.
- **Suggestion**: Add `row_id_start` tracking in `DataFileInfo` and populate it during `register_data_file` by querying the current table's next_row_id from `ducklake_table_stats`.
- **Effort**: M

### INTEROP-4: Missing UUID generation for schemas and tables (Severity: P2)
- **File(s)**: `src/metadata_writer_sqlite.rs:371,404,658,708`
- **Description**: DuckDB always generates UUIDv7 for `schema_uuid`, `table_uuid`, and `view_uuid`. Our writer leaves these fields NULL. UUIDs serve as stable identifiers for schema evolution and cross-catalog references.
- **Impact**: DuckDB may tolerate NULL UUIDs, but any downstream tooling that relies on UUIDs for table identity (e.g., Iceberg-style UUID-based references) will break. DuckDB's column mapping feature (`ducklake_column_mapping`) may also depend on UUIDs.
- **Suggestion**: Generate UUIDv7 (or UUIDv4 as fallback) when creating schemas, tables, and views.
- **Effort**: S

### INTEROP-5: `snapshot_changes.changes_made` format mismatch (Severity: P2)
- **File(s)**: `src/metadata_writer_sqlite.rs:560,607,1261,1502,1629,1687,1765`
- **Description**: DuckDB uses structured format strings: `created_schema:"main"`, `created_table:"main"."test_table"`, `inserted_into_table:1`, `deleted_from_table:1`. Our writer uses human-readable strings: `"Dropped table (id=1)"`, `"Altered table (id=1)"`, `"Renamed view (id=1)"`.
- **Impact**: Any tooling that parses `changes_made` for change tracking (e.g., DuckDB's own catalog browser, CDC tools) will not understand our format. This breaks semantic change auditing.
- **Suggestion**: Adopt DuckDB's exact format: `dropped_table:ID`, `altered_table:ID`, `renamed_table:ID`, etc.
- **Effort**: M

### INTEROP-6: Missing `encrypted=false` metadata key (Severity: P3)
- **File(s)**: `src/metadata_writer_sqlite.rs:886-916`
- **Description**: DuckDB always writes `encrypted=false` to `ducklake_metadata`. We don't set this key. DuckDB's reader may check this flag before processing catalog data.
- **Impact**: Low — DuckDB likely defaults to unencrypted if key is missing. But explicit is better than implicit for forward compatibility.
- **Suggestion**: Add `encrypted=false` to `initialize_schema`.
- **Effort**: S

### INTEROP-7: Extra columns in our schema (Severity: P3)
- **File(s)**: `src/metadata_writer_sqlite.rs:70-71` (default_value_type, default_value_dialect), `src/metadata_writer_sqlite.rs:90` (partial_max in data_file), `src/metadata_writer_sqlite.rs:107` (partial_max in delete_file), `src/metadata_writer_sqlite.rs:240` (table_id in schema_versions)
- **Description**: Our DDL adds columns that DuckDB does not expect:
  - `ducklake_column`: `default_value_type`, `default_value_dialect`
  - `ducklake_data_file`: `partial_max`
  - `ducklake_delete_file`: `partial_max`
  - `ducklake_schema_versions`: `table_id`
- **Impact**: Extra columns are generally harmless for SQLite (DuckDB reads by column name, not position). However, `partial_max` is not part of the spec and could cause confusion. The extra `table_id` in `schema_versions` changes the table's semantics.
- **Suggestion**: Remove `partial_max` columns (not in DuckDB spec). Keep `default_value_type`/`default_value_dialect` as they may be added to DuckDB in future. Remove `table_id` from `ducklake_schema_versions`.
- **Effort**: S

### INTEROP-8: `_df_change_tracking` is a non-spec table (Severity: P3)
- **File(s)**: `src/metadata_writer_sqlite.rs:120-126`
- **Description**: Our writer creates a `_df_change_tracking` table for conflict detection. This is not part of the DuckLake spec. DuckDB ignores unknown tables (verified by roundtrip tests), but it pollutes the catalog namespace.
- **Impact**: Minimal — DuckDB tolerates it. The `_df_` prefix clearly marks it as DataFusion-specific.
- **Suggestion**: Acceptable as-is. Consider documenting it as a DF extension. The `_df_` prefix convention is good.
- **Effort**: N/A

### INTEROP-9: `schema_version` default mismatch in snapshots (Severity: P2)
- **File(s)**: `src/metadata_writer_sqlite.rs:34`
- **Description**: Our DDL defaults `schema_version` to `1` for new snapshots, but DuckDB starts at `0` for the initial (empty catalog) snapshot and increments to `1` after the first schema change (CREATE TABLE). Our snapshot 0 uses `schema_version=0` (correct), but any new snapshots created by `create_snapshot()` get `schema_version=1` by default even when no schema change has occurred.
- **Impact**: DuckDB's schema version tracking may be confused if `schema_version` doesn't match the actual schema evolution state. This could affect time-travel queries.
- **Suggestion**: New snapshots should inherit the `schema_version` from the latest snapshot, not use a constant default.
- **Effort**: M

### INTEROP-10: Missing `ducklake_schema_versions` and `ducklake_table_stats` population (Severity: P1)
- **File(s)**: `src/metadata_writer_sqlite.rs` (no INSERT into these tables)
- **Description**: DuckDB populates `ducklake_schema_versions` with `(begin_snapshot, schema_version)` pairs and `ducklake_table_stats` with `(table_id, record_count, next_row_id, file_size_bytes)`. Our writer creates these tables but never populates them (except `ducklake_table_column_stats` during ALTER TABLE ADD COLUMN).
- **Impact**: DuckDB uses `ducklake_table_stats` for query optimization (row count estimates). Missing stats means DuckDB will not have accurate cardinality estimates for DF-created tables. Missing `schema_versions` may affect DuckDB's schema version resolution.
- **Suggestion**: Populate `ducklake_table_stats` when registering data files (update record_count and file_size_bytes). Populate `ducklake_schema_versions` when schema changes occur.
- **Effort**: M

### INTEROP-11: `snapshot_time` format in SQLite (Severity: P3)
- **File(s)**: `src/metadata_writer_sqlite.rs:33`
- **Description**: SQLite's `CURRENT_TIMESTAMP` produces `YYYY-MM-DD HH:MM:SS` without timezone info. DuckDB stores `TIMESTAMP WITH TIME ZONE` (e.g., `2026-03-02 03:57:26.271137+01`). DuckDB's reader casts TEXT → TIMESTAMPTZ which may interpret the value as UTC.
- **Impact**: Snapshot times will be off by the server's timezone offset. Functionally harmless for snapshot ordering (still monotonic) but incorrect for time-travel with absolute timestamps.
- **Suggestion**: Use `strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')` for ISO 8601 with explicit UTC timezone.
- **Effort**: S

## Cross-Engine Test Coverage Matrix

### By Operation × Direction

| Operation | DF→DF | DF→DuckDB | DuckDB→DF | Bidirectional |
|-----------|-------|-----------|-----------|---------------|
| INSERT | Yes | Yes | Yes | Yes |
| DELETE | Yes | Yes | Yes | Yes |
| UPDATE | Yes | Yes | Yes | Yes |
| CREATE TABLE | Yes | Yes | Yes | Yes |
| DROP TABLE | Yes | Yes | Yes | - |
| CREATE/DROP SCHEMA | Yes | Yes | Yes | Yes |
| ALTER TABLE (Add/Drop Col) | Yes | Yes | Yes | Yes |
| ALTER TABLE (Rename/Default/NotNull) | Yes | Yes | Yes | - |
| RENAME TABLE | Yes | Yes | Yes | - |
| CREATE/DROP VIEW | Yes | - | Yes | - |
| Partitioned writes (Hive dirs) | Yes | - | Yes | - |
| Inline data | - | - | Yes | - |
| Schema evolution | Yes | Partial | - | - |
| Column stats | Yes | Yes | - | - |
| Conflict detection | Yes | - | - | - |

### By Catalog Backend

| Backend | Write Tests | Read Tests | Cross-Engine |
|---------|-------------|------------|--------------|
| SQLite | ~66 tests | ~66 tests | DF↔DuckDB |
| DuckDB-native | - | ~31 tests | DuckDB→DF only |
| PostgreSQL | ~8 tests | ~8 tests | DF↔DuckDB (Docker) |
| MySQL | ~8 tests | ~8 tests | DF↔DuckDB (Docker) |

### Coverage Gaps

1. **DF partitioned writes → DuckDB reads**: Not tested. Partition directories are created but no test verifies DuckDB can read DF-created partitioned catalogs.
2. **DF views → DuckDB reads**: Views are created by DF but no test verifies DuckDB can resolve them.
3. **DF inline data → DuckDB reads**: DF can store inline data but no test verifies DuckDB can read it.
4. **Schema evolution roundtrip**: `test_schema_evolution_roundtrip` exists but the test gracefully handles failure (doesn't assert DuckDB success). Known interop gap with column_id mapping.
5. **Catalog schema DDL comparison**: No test compares our DDL output against DuckDB's expected schema table-by-table.
6. **Type fidelity**: Tests cover basic types (INT, VARCHAR, DOUBLE) but not all 16 supported types.

## Parquet File Format Compatibility

Our Parquet files use standard Arrow→Parquet mapping which is compatible with DuckDB. The delete file schema `(file_path: VARCHAR, pos: INT64)` matches DuckDB's format exactly. Key observations:

- **Delete file `file_path` values**: DuckDB uses absolute paths with `file://` scheme. Our code should use the same absolute path format for cross-engine compatibility.
- **Delete file `pos` values**: 0-indexed row positions within the file. Both engines agree on this.
- **Parquet metadata**: Footer sizes are stored correctly.

## Hive Directory Layout

Our partition writer creates `key=value` directories matching DuckDB's format. The `ducklake_file_partition_value` entries correctly map data files to partition values. No URL encoding issues detected for simple string values. Special characters in partition values are not tested.

## Inline Data Format

Our SQLite inline data tables use the format `ducklake_inlined_data_{table_id}` with columns `(row_id, begin_snapshot, end_snapshot, user_col_1, user_col_2, ...)`. All user columns are stored as TEXT. DuckDB's inline data format has not been verified against this layout — cross-engine inline data tests only go DuckDB→DF, never DF→DuckDB.

## Priority Summary

| Severity | Count | Issues |
|----------|-------|--------|
| P0 | 0 | - |
| P1 | 3 | INTEROP-1 (delete format), INTEROP-3 (row_id_start), INTEROP-10 (missing stats) |
| P2 | 4 | INTEROP-2 (file_format case), INTEROP-4 (UUIDs), INTEROP-5 (changes_made), INTEROP-9 (schema_version) |
| P3 | 4 | INTEROP-6 (encrypted key), INTEROP-7 (extra columns), INTEROP-8 (_df_change_tracking), INTEROP-11 (timestamp format) |
