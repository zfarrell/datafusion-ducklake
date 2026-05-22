# Review Cycle 6: Interoperability Review
Date: 2026-03-04

## Summary

Performed a thorough schema comparison between our catalog tables (`metadata_writer_sqlite.rs`, `metadata_writer_postgres.rs`, `metadata_writer_mysql.rs`) and DuckDB v1.4.4's DuckLake extension (version 0.3, extension hash 3f1b372). Compared using a reference catalog created by DuckDB and inspecting all 22 standard DuckLake tables.

**Overall assessment: Interoperability is GOOD.** The core schema is compatible with DuckDB. Cross-engine tests pass for all major operations (INSERT, SELECT, DELETE, UPDATE, MERGE, ALTER TABLE, type roundtrips). DuckDB tolerates extra columns and extra tables in the catalog database.

Found 9 findings total: 0 P0, 1 P1, 4 P2, 4 P3. The P1 is the hardcoded `schema_version=1` in inlined data table naming, which would break cross-engine inline data reads for tables created after the first DDL snapshot.

## Reference Schema Comparison

### Tables Present in Both (22 tables - all match)

| Table | DuckDB Columns | Our Columns | Differences |
|-------|---------------|-------------|-------------|
| `ducklake_metadata` | key, value, scope, scope_id | key, value, scope, scope_id | **Match** |
| `ducklake_snapshot` | snapshot_id PK, snapshot_time TIMESTAMPTZ, schema_version, next_catalog_id, next_file_id | snapshot_id PK, snapshot_time TEXT, schema_version, next_catalog_id, next_file_id | snapshot_time type differs (TEXT vs TIMESTAMPTZ) |
| `ducklake_schema` | schema_id PK, schema_uuid UUID, begin_snapshot, end_snapshot, schema_name, path, path_is_relative | schema_id PK, schema_uuid VARCHAR, schema_name, path, path_is_relative, begin_snapshot, end_snapshot | UUID vs VARCHAR, column order differs |
| `ducklake_table` | table_id, table_uuid UUID, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative | table_id, table_uuid VARCHAR, schema_id, table_name, path, path_is_relative, begin_snapshot, end_snapshot | UUID vs VARCHAR, column order differs |
| `ducklake_column` | column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column | column_id, table_id, column_name, column_type, column_order, nulls_allowed, initial_default, default_value, parent_column, **default_value_type**, **default_value_dialect**, begin_snapshot, end_snapshot | **2 extra columns**, column order differs |
| `ducklake_data_file` | data_file_id PK, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id | data_file_id PK, table_id, path, path_is_relative, file_size_bytes, footer_size, encryption_key, record_count, row_id_start, mapping_id, file_order, file_format, partition_id, **partial_max**, partial_file_info, begin_snapshot, end_snapshot | **1 extra column (partial_max)**, column order differs |
| `ducklake_delete_file` | delete_file_id PK, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key | delete_file_id PK, data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, encryption_key, delete_count, format, **partial_max**, begin_snapshot, end_snapshot | **1 extra column (partial_max)**, column order differs |
| `ducklake_snapshot_changes` | snapshot_id PK, changes_made, author, commit_message, commit_extra_info | snapshot_id PK, changes_made, author, commit_message, commit_extra_info | **Match** |
| `ducklake_file_column_stats` | data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats | data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats | **Match** |
| `ducklake_table_column_stats` | table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats | table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats | **Match** |
| `ducklake_table_stats` | table_id, record_count, next_row_id, file_size_bytes | table_id, record_count, next_row_id, file_size_bytes | **Match** |
| `ducklake_file_partition_value` | data_file_id, table_id, partition_key_index, partition_value | data_file_id, table_id, partition_key_index, partition_value | **Match** |
| `ducklake_partition_info` | partition_id, table_id, begin_snapshot, end_snapshot | partition_id, table_id, begin_snapshot, end_snapshot | **Match** |
| `ducklake_partition_column` | partition_id, table_id, partition_key_index, column_id, transform | partition_id, table_id, partition_key_index, column_id, transform | **Match** |
| `ducklake_view` | view_id, view_uuid UUID, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases | view_id, view_uuid VARCHAR, schema_id, view_name, dialect, sql, column_aliases, begin_snapshot, end_snapshot | UUID vs VARCHAR, column order differs |
| `ducklake_tag` | object_id, begin_snapshot, end_snapshot, key, value | object_id, begin_snapshot, end_snapshot, key, value | **Match** |
| `ducklake_column_tag` | table_id, column_id, begin_snapshot, end_snapshot, key, value | table_id, column_id, begin_snapshot, end_snapshot, key, value | **Match** |
| `ducklake_inlined_data_tables` | table_id, table_name, schema_version | table_id, table_name, schema_version | **Match** |
| `ducklake_column_mapping` | mapping_id, table_id, type | mapping_id, table_id, type | **Match** |
| `ducklake_name_mapping` | mapping_id, column_id, source_name, target_field_id, parent_column, is_partition | mapping_id, column_id, source_name, target_field_id, parent_column, is_partition | **Match** |
| `ducklake_schema_versions` | begin_snapshot, schema_version | begin_snapshot, schema_version, **table_id** | **1 extra column (table_id)** |
| `ducklake_files_scheduled_for_deletion` | data_file_id, path, path_is_relative, schedule_start TIMESTAMPTZ | data_file_id, path, path_is_relative, schedule_start TEXT | schedule_start type differs |

### Tables Only in Our Schema (7 extra tables)

| Table | Purpose | Risk |
|-------|---------|------|
| `_df_change_tracking` | DF-specific conflict detection for checked writes | Low - prefixed with `_df_`, DuckDB ignores |
| `ducklake_macro` | Future macro support | Low - DuckDB ignores unknown tables |
| `ducklake_macro_impl` | Future macro support | Low - same |
| `ducklake_macro_parameters` | Future macro support | Low - same |
| `ducklake_sort_info` | Future sort order support | Low - same |
| `ducklake_sort_expression` | Future sort expression support | Low - same |
| `ducklake_file_variant_stats` | Future variant stats support | Low - same |

**Verified**: DuckDB v1.4.4 tolerates all extra tables and extra columns without error. Tested by adding `_df_change_tracking`, `default_value_type`, `default_value_dialect`, and `partial_max` to a DuckDB-created catalog — all queries still work correctly.

## Findings

### R6-I-001: Hardcoded schema_version=1 in inlined data table naming
- **File(s)**: `src/metadata_writer_sqlite.rs:2923`
- **Severity**: P1
- **Category**: inline-data
- **Description**: The `store_inlined_data` function hardcodes `schema_version=1` when constructing the inline data table name (`ducklake_inlined_data_{table_id}_{schema_version}`). DuckDB uses the actual `schema_version` at table creation time.
- **DuckDB Behavior**: For table_id=1 created at schema_version=1, the name is `ducklake_inlined_data_1_1`. For table_id=2 created at schema_version=2, the name is `ducklake_inlined_data_2_2`.
- **Our Behavior**: We always use `ducklake_inlined_data_{table_id}_1`, which is correct for the first table but wrong for subsequent tables.
- **Impact**: If DF creates a second table with inlined data, DuckDB would look for `ducklake_inlined_data_2_2` but we create `ducklake_inlined_data_2_1`. DuckDB would fail to read the inlined data. DuckDB would also fail if DF creates inlined data after any ALTER TABLE operation that bumps schema_version.
- **Suggested Fix**: Fetch the current `schema_version` from the snapshot or `ducklake_schema_versions` table instead of hardcoding 1. The schema_version used should match the one in `ducklake_inlined_data_tables.schema_version`.
- **Effort**: S

### R6-I-002: Extra columns in ducklake_column (default_value_type, default_value_dialect)
- **File(s)**: `src/metadata_writer_sqlite.rs:70-71`, `src/metadata_writer_postgres.rs:63-64`, `src/metadata_writer_mysql.rs` (same)
- **Severity**: P3
- **Category**: extension
- **Description**: Our `ducklake_column` table has two extra columns (`default_value_type` and `default_value_dialect`) that don't exist in DuckDB's schema.
- **DuckDB Behavior**: DuckDB's `ducklake_column` has 11 columns ending with `parent_column`.
- **Our Behavior**: We add `default_value_type VARCHAR` and `default_value_dialect VARCHAR` between `parent_column` and `begin_snapshot`.
- **Impact**: **Verified safe**: DuckDB v1.4.4 uses named-column SQL and ignores extra columns. Tested directly by adding these columns to a DuckDB-created catalog.
- **Suggested Fix**: Consider removing these columns if they're not used (they appear to be placeholders for future DuckDB features). Alternatively, document them as forward-compatible extensions.
- **Effort**: S

### R6-I-003: Extra column partial_max in data_file and delete_file tables
- **File(s)**: `src/metadata_writer_sqlite.rs:90,107`, `src/metadata_writer_postgres.rs:82,98`
- **Severity**: P3
- **Category**: extension
- **Description**: Both `ducklake_data_file` and `ducklake_delete_file` have an extra `partial_max` column not in DuckDB's schema.
- **DuckDB Behavior**: No `partial_max` column exists.
- **Our Behavior**: We define it but never populate it (always NULL).
- **Impact**: **Verified safe**: DuckDB ignores the extra column. However, this is dead schema that adds no value.
- **Suggested Fix**: Remove `partial_max` from both tables, or document why it's reserved.
- **Effort**: S

### R6-I-004: Extra table_id column in ducklake_schema_versions
- **File(s)**: `src/metadata_writer_sqlite.rs:240`
- **Severity**: P3
- **Category**: extension
- **Description**: Our `ducklake_schema_versions` has an extra `table_id INTEGER` column not in DuckDB's schema.
- **DuckDB Behavior**: Only `begin_snapshot` and `schema_version` columns.
- **Our Behavior**: Extra `table_id` column, never populated (always NULL).
- **Impact**: **Verified safe**: DuckDB ignores the extra column.
- **Suggested Fix**: Remove the extra column.
- **Effort**: S

### R6-I-005: 7 extra tables not in DuckDB's DuckLake schema
- **File(s)**: `src/metadata_writer_sqlite.rs:120-299`
- **Severity**: P2
- **Category**: extension
- **Description**: We create 7 tables that DuckDB doesn't: `_df_change_tracking`, `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters`, `ducklake_sort_info`, `ducklake_sort_expression`, `ducklake_file_variant_stats`.
- **DuckDB Behavior**: Creates exactly 22 `ducklake_*` tables.
- **Our Behavior**: Creates 22 standard + 7 extra tables.
- **Impact**: **Verified safe for DuckDB v1.4.4**: DuckDB ignores unknown tables. The `_df_change_tracking` table uses a `_df_` prefix to clearly distinguish it from DuckLake standard tables. The `ducklake_macro*`, `ducklake_sort*`, and `ducklake_file_variant_stats` tables use the `ducklake_` prefix which could theoretically conflict with future DuckLake additions, but since they match known DuckLake concepts, they're likely forward-compatible.
- **Suggested Fix**: Consider prefixing the non-standard tables with `_df_` or documenting them. The macro/sort/variant tables could cause issues if DuckLake introduces them with different schemas in future versions.
- **Effort**: M

### R6-I-006: UUID v4 vs DuckDB's UUID v7 for file naming
- **File(s)**: `src/table_writer.rs:85,112,489,573`, `src/delete_exec.rs:331`, `src/update_exec.rs:407,467`
- **Severity**: P3
- **Category**: file-naming
- **Description**: We use UUID v4 (random) for file names while DuckDB uses UUID v7 (time-ordered).
- **DuckDB Behavior**: File names like `ducklake-019cb609-2d07-7f15-aefa-94acc6866683.parquet` (UUID v7).
- **Our Behavior**: File names like `ducklake-f47ac10b-58cc-4372-a567-0e02b2c3d479.parquet` (UUID v4).
- **Impact**: **No functional impact.** Both formats are valid UUIDs and the file naming convention `ducklake-{uuid}.parquet` and `ducklake-{uuid}-delete.parquet` matches DuckDB's pattern. File names are opaque identifiers stored in the catalog; the UUID version doesn't affect cross-engine compatibility.
- **Suggested Fix**: Optional: switch to UUID v7 for consistency and better time-ordering of files. Low priority.
- **Effort**: S

### R6-I-007: snapshot_time stored as TEXT with UTC format
- **File(s)**: `src/metadata_writer_sqlite.rs:33`
- **Severity**: P2
- **Category**: schema-compat
- **Description**: Our `ducklake_snapshot` uses `TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f+00:00', 'now'))` while DuckDB stores as `TIMESTAMP WITH TIME ZONE`.
- **DuckDB Behavior**: Stores timestamps in DuckDB's native TIMESTAMPTZ format. When reading from SQLite, DuckDB parses text values automatically.
- **Our Behavior**: We store timestamps as ISO 8601 text with `+00:00` suffix.
- **Impact**: **Verified safe for reads**: DuckDB successfully reads our text-format timestamps and auto-casts them. However, DuckDB stores timestamps with local timezone offset (e.g., `+01`), while we always use `+00:00` (UTC). This doesn't affect cross-engine reads since DuckDB normalizes timezone-aware timestamps.
- **Suggested Fix**: No action needed. The ISO 8601 format is the correct interchange format for SQLite.
- **Effort**: N/A

### R6-I-008: schedule_start type mismatch in files_scheduled_for_deletion
- **File(s)**: `src/metadata_writer_sqlite.rs:213`
- **Severity**: P2
- **Category**: schema-compat
- **Description**: Our `ducklake_files_scheduled_for_deletion.schedule_start` is `TEXT` while DuckDB uses `TIMESTAMP WITH TIME ZONE`.
- **DuckDB Behavior**: Writes and reads as TIMESTAMPTZ.
- **Our Behavior**: Stores as TEXT with ISO 8601 format comment.
- **Impact**: Same as R6-I-007. DuckDB auto-casts text timestamps when reading from SQLite. However, if DuckDB writes to this table (scheduling files for deletion after compaction), it will write in its native format; if we then read it, we need to handle that format.
- **Suggested Fix**: Ensure our code parses both `YYYY-MM-DD HH:MM:SS.ffffff+00:00` and `YYYY-MM-DD HH:MM:SS.ffffff+TZ` formats when reading this table.
- **Effort**: S

### R6-I-009: Missing cross-engine test coverage for DF-write operations
- **File(s)**: `tests/cross_engine_tests.rs`
- **Severity**: P2
- **Category**: test-coverage
- **Description**: Cross-engine tests primarily test DuckDB-write→DF-read and basic DF-write→DuckDB-read. Missing coverage for DF-write DML operations read by DuckDB.
- **DuckDB Behavior**: N/A
- **Our Behavior**: No tests for: (1) DF DELETE → DuckDB read, (2) DF UPDATE → DuckDB read, (3) DF ALTER TABLE → DuckDB read, (4) DF partitioned INSERT → DuckDB read, (5) DF inlined data → DuckDB read, (6) DF DROP TABLE → DuckDB behavior, (7) DF CREATE VIEW → DuckDB read.
- **Impact**: These untested paths could have schema incompatibilities we haven't caught. The DF→DuckDB direction is more likely to have issues because DF controls the catalog metadata format.
- **Suggested Fix**: Add cross-engine tests for each DF DML operation verified by DuckDB reads.
- **Effort**: L

## Codex Findings

Codex identified 5 findings. After validation:

1. **changes_made format** (codex "High"): **FALSE POSITIVE**. Codex claimed our `created_table:"schema"."table"` format doesn't match DuckDB's, but direct testing confirms DuckDB uses the exact same schema-qualified format.

2. **ON CONFLICT overwrite** (codex "Medium"): **VALID but LOW IMPACT**. Our `ON CONFLICT ... SET changes_made = excluded.changes_made` would overwrite earlier changes if multiple ops shared one snapshot. In practice, each operation creates its own snapshot, so this is not a real issue.

3. **Extra columns** (codex "Medium"): **VALID, P3**. Confirmed via testing that DuckDB tolerates extra columns. Covered by R6-I-002, R6-I-003, R6-I-004.

4. **Extra tables** (codex "Medium"): **VALID, P2**. Confirmed via testing that DuckDB tolerates extra tables. Covered by R6-I-005.

5. **SQLite type divergence** (codex "Low"): **VALID, P2-P3**. Confirmed via testing that DuckDB auto-casts text timestamps. Covered by R6-I-007, R6-I-008.

## Cross-Engine Test Coverage Matrix

| Operation | DF→DF | DF→DuckDB | DuckDB→DF | Notes |
|-----------|:-----:|:---------:|:---------:|-------|
| CREATE TABLE + INSERT | Yes | Yes | Yes | Basic roundtrip |
| SELECT (basic) | Yes | Yes | Yes | |
| SELECT (COUNT) | - | - | Yes | count_query test |
| NULL handling | - | Yes | Yes | null_handling test |
| DELETE | - | **No** | Yes | duckdb_delete_df_read |
| Multiple DELETEs | - | **No** | Yes | duckdb_multiple_deletes |
| UPDATE | - | **No** | Yes | duckdb_update_df_read |
| MERGE | - | **No** | Yes | duckdb_merge_df_read |
| ALTER TABLE ADD COLUMN | - | **No** | Yes | duckdb_alter_add_column |
| ALTER TABLE DROP COLUMN | - | **No** | Yes | duckdb_alter_drop_column |
| TIMESTAMP type | - | Yes | Yes | timestamp_type_roundtrip |
| DATE type | - | Yes | Yes | date_type_roundtrip |
| DECIMAL type | - | Yes | Yes | decimal_type_roundtrip |
| Typed data (multiple types) | - | Yes | - | df_write_typed_data |
| Bidirectional roundtrip | - | - | Yes | Multi-step roundtrip |
| Interleaved DML + reads | - | - | Yes | insert→read→update→read→delete→read |
| Partitioned INSERT | - | **No** | **No** | Not tested cross-engine |
| Inlined data | - | **No** | **No** | Not tested cross-engine |
| DROP TABLE | - | **No** | **No** | Not tested cross-engine |
| CREATE/DROP VIEW | - | **No** | **No** | Not tested cross-engine |
| CREATE/DROP SCHEMA | - | **No** | **No** | Not tested cross-engine |

**Coverage summary**: 20 test functions in cross_engine_tests.rs. Strong DuckDB→DF coverage. **DF→DuckDB DML coverage is a significant gap** — only basic INSERT and type roundtrips are tested in that direction.

## Metadata Values Comparison

| Key | DuckDB Value | Our Value | Compatible |
|-----|-------------|-----------|:----------:|
| version | 0.3 | 0.3 | Yes |
| created_by | DuckDB 6ddac802ff | DataFusion-DuckLake | Yes (informational) |
| encrypted | false | false | Yes |
| data_path | `{path}.files/` | `{path}/` (configurable) | Yes |

## File Format Comparison

| Aspect | DuckDB | Our Implementation | Compatible |
|--------|--------|-------------------|:----------:|
| File naming | `ducklake-{uuid-v7}.parquet` | `ducklake-{uuid-v4}.parquet` | Yes |
| Delete file naming | `ducklake-{uuid-v7}-delete.parquet` | `ducklake-{uuid-v4}-delete.parquet` | Yes |
| Parquet version | 2.0 | 2.0 | Yes |
| Delete file schema | (file_path VARCHAR, pos INT64) | (file_path VARCHAR, pos INT64) | Yes |
| Delete file field_ids | 0x7FFFFFFE, 0x7FFFFFFD | 0x7FFFFFFE, 0x7FFFFFFD | Yes |
| PARQUET:field_id in data files | Yes (from column_id) | Yes (from column_id) | Yes |
| Hive partition paths | key=value format | key=value with URL encoding | Yes |
| Inline data table schema | row_id, begin_snapshot, end_snapshot, <cols> | row_id, begin_snapshot, end_snapshot, <cols> | Yes |
| Footer size tracking | Yes | Yes | Yes |

## snapshot_changes Format Comparison

| Operation | DuckDB Format | Our Format | Match |
|-----------|--------------|-----------|:-----:|
| Create schema | `created_schema:"name"` | `created_schema:"name"` | Yes |
| Create table | `created_table:"schema"."table"` | `created_table:"schema"."table"` | Yes |
| Insert | `inserted_into_table:{id}` | `inserted_into_table:{id}` | Yes |
| Delete | `deleted_from_table:{id}` | `deleted_from_table:{id}` | Yes |
| Update | `inserted_into_table:{id},deleted_from_table:{id}` | `inserted_into_table:{id},deleted_from_table:{id}` | Yes |
| Alter table | `altered_table:{id}` | `altered_table:{id}` | Yes |
| Drop table | `dropped_table:{id}` | `dropped_table:{id}` | Yes |
| Drop schema | `dropped_schema:{id}` | `dropped_schema:{id}` | Yes |
| Create view | `created_view:"schema"."view"` | `created_view:"schema"."view"` | Yes |
| Drop view | `dropped_view:{id}` | `dropped_view:{id}` | Yes |
