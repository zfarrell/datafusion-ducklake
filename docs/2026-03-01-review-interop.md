# DuckLake Interoperability Review

**Date**: 2026-03-01
**Branch**: `ducklake-features/integration`
**Scope**: Schema compliance, cross-engine data exchange, partition/inlining interop

## Executive Summary

The integration branch is **largely interop-safe** for the core read/write path. All 22 DuckLake catalog tables that DuckDB creates are present in our DDL, with matching column names and compatible types. Cross-engine tests confirm DF-written data is readable by DuckDB (via `ducklake:sqlite:` ATTACH) for basic INSERT, typed data, NULL handling, REPLACE, APPEND, and bidirectional roundtrips.

**However, there are gaps:**

1. **No DF→DuckDB partition tests** — partition tests only verify DuckDB→DF direction
2. **Inlined data uses TEXT-only columns** in SQLite — needs verification that DuckDB's inlined reader handles this
3. **Postgres/MySQL backends lack inlining support** — `store_inlined_data` returns `Unsupported`
4. **Extra tables/columns beyond DuckDB 0.3 spec** — benign but worth documenting
5. **`_df_change_tracking` is a DF-invented table** — benign (DuckDB ignores unknown tables)

---

## Schema Comparison: Our DDL vs DuckDB's DuckLake Extension

### Reference DuckDB Schema (v0.3, DuckDB v1.4.4)

DuckDB's DuckLake extension creates 22 tables. Our writer creates 28+ tables.

| Table | DuckDB 0.3 | Our SQLite Writer | Match |
|-------|-----------|-------------------|-------|
| `ducklake_metadata` | `key, value, scope, scope_id` | `key, value, scope, scope_id` | **Exact** |
| `ducklake_snapshot` | `snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id` | Same columns | **Exact** |
| `ducklake_schema` | `schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative` | Same columns (order differs) | **Exact** |
| `ducklake_table` | `table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative` | Same columns (order differs) | **Exact** |
| `ducklake_column` | `column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column` | Same + `default_value_type`, `default_value_dialect` | **Extra cols** |
| `ducklake_data_file` | `data_file_id, table_id, begin/end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id` | Same + `partial_max` | **Extra col** |
| `ducklake_delete_file` | `delete_file_id, table_id, begin/end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key` | Same + `partial_max` | **Extra col** |
| `ducklake_snapshot_changes` | `snapshot_id, changes_made, author, commit_message, commit_extra_info` | Same | **Exact** |
| `ducklake_file_column_stats` | `data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats` | Same | **Exact** |
| `ducklake_view` | 9 columns | Same | **Exact** |
| `ducklake_tag` | 5 columns | Same | **Exact** |
| `ducklake_column_tag` | 6 columns | Same | **Exact** |
| `ducklake_table_stats` | `table_id, record_count, next_row_id, file_size_bytes` | Same | **Exact** |
| `ducklake_table_column_stats` | 7 columns | Same | **Exact** |
| `ducklake_partition_info` | `partition_id, table_id, begin_snapshot, end_snapshot` | Same | **Exact** |
| `ducklake_partition_column` | `partition_id, table_id, partition_key_index, column_id, transform` | Same | **Exact** |
| `ducklake_file_partition_value` | `data_file_id, table_id, partition_key_index, partition_value` | Same | **Exact** |
| `ducklake_files_scheduled_for_deletion` | 4 columns | Same | **Exact** |
| `ducklake_inlined_data_tables` | `table_id, table_name, schema_version` | Same | **Exact** |
| `ducklake_column_mapping` | `mapping_id, table_id, type` | Same | **Exact** |
| `ducklake_name_mapping` | 6 columns | Same | **Exact** |
| `ducklake_schema_versions` | `begin_snapshot, schema_version` | Same + `table_id` | **Extra col** |

### Tables We Create That DuckDB 0.3 Does Not

| Table | Purpose | Risk |
|-------|---------|------|
| `_df_change_tracking` | DF-internal conflict detection for concurrent writes | **None** — `_df_` prefix, DuckDB ignores |
| `ducklake_macro` | Macro definitions | **None** — likely newer DuckLake spec |
| `ducklake_macro_impl` | Macro implementations | **None** — likely newer DuckLake spec |
| `ducklake_macro_parameters` | Macro parameters | **None** — likely newer DuckLake spec |
| `ducklake_sort_info` | Sort order metadata | **None** — likely newer DuckLake spec |
| `ducklake_sort_expression` | Sort expressions | **None** — likely newer DuckLake spec |
| `ducklake_file_variant_stats` | Variant column statistics | **None** — likely newer DuckLake spec |

**Impact**: DuckDB ignores tables it doesn't know about. Extra tables pose zero interop risk.

### Extra Columns We Add

| Table | Extra Column(s) | Risk |
|-------|----------------|------|
| `ducklake_column` | `default_value_type`, `default_value_dialect` | **None** — DuckDB queries by name |
| `ducklake_data_file` | `partial_max` | **None** — DuckDB queries by name |
| `ducklake_delete_file` | `partial_max` | **None** — DuckDB queries by name |
| `ducklake_schema_versions` | `table_id` | **None** — DuckDB queries by name |

**Impact**: SQLite and PostgreSQL both return columns by name in queries. Extra columns are simply never selected by DuckDB. Zero interop risk.

---

## Findings by Severity

### Critical

**None identified.** The core schema is compatible and tested.

### Major

#### M1: No DF→DuckDB partition test coverage

All 7 partition tests in `cross_engine_partition_tests.rs` test **DuckDB→DF** only (DuckDB creates partitioned table, DF reads). There is no test verifying that DataFusion can write a partitioned table that DuckDB can subsequently read.

**Risk**: DF's Hive-style directory layout or partition metadata registration may differ from DuckDB's expectations. The partition DDL (`ducklake_partition_info`, `ducklake_partition_column`, `ducklake_file_partition_value`) matches, but the file path conventions and partition_id assignment logic are untested in the DF→DuckDB direction.

**Suggested fix**: Add `test_df_write_partitioned_duckdb_read()` that:
1. Creates a table via DF writer
2. Sets partition via `ALTER TABLE ... SET PARTITIONED BY`
3. Inserts partitioned data via DF
4. Opens catalog with `DuckDbConn::open()` and verifies DuckDB reads all rows correctly
5. Verifies DuckDB can filter by partition column

#### M2: Inlined data uses TEXT-only columns in SQLite

`metadata_writer_sqlite.rs:1867`: Dynamic inlined data tables use `TEXT` for all user columns:
```rust
create_sql.push_str(&format!(", \"{}\" TEXT", col.name()));
```

DuckDB's inlined data tables use native DuckDB types (INT32, VARCHAR, etc.) when the catalog is a DuckDB database. When DuckDB reads a SQLite-backed catalog's inlined data, it reads column values as-is from SQLite. Since SQLite is dynamically typed, TEXT storage works for most types via implicit conversion.

**Risk**: MEDIUM — DuckDB's inlined data reader may expect typed columns when the catalog is SQLite. The `ducklake_inlined_data_tables` metadata table matches perfectly, but the dynamic table's column types may cause issues for numeric comparisons or sorting within DuckDB.

**Suggested fix**: Verify with a test that DuckDB can read DF-written SQLite inlined data. Consider mapping DuckLake types to SQLite-compatible types (INTEGER, REAL, TEXT) instead of all-TEXT.

#### M3: Postgres and MySQL inlining not implemented

`store_inlined_data()` returns `Err(DuckLakeError::Unsupported(...))` for both Postgres and MySQL backends. The trait default (in `metadata_writer.rs:581-591`) returns this error.

**Risk**: MEDIUM — Users who configure `data_inlining_row_limit` with a Postgres or MySQL catalog will get runtime errors on INSERT.

**Suggested fix**: Either implement inlining for Postgres/MySQL, or clearly document the limitation and handle the error gracefully at the catalog level (fall back to Parquet).

### Minor

#### m1: `_df_change_tracking` table is DF-specific

This table is used for internal conflict detection during concurrent writes. It uses a `_df_` prefix to signal it's non-standard.

**Risk**: LOW — DuckDB will ignore it. But if DuckDB adds a table with a conflicting name in the future, there could be issues.

#### m2: Version metadata hardcoded to `0.3`

`metadata_writer_sqlite.rs:898`: The writer sets `version = '0.3'` in `ducklake_metadata`. This matches the currently tested DuckDB version. If DuckDB's DuckLake extension upgrades to a newer version, our catalogs may need version migration.

**Risk**: LOW — DuckDB's DuckLake extension is expected to handle version compatibility.

#### m3: `snapshot_time` uses TEXT in SQLite vs TIMESTAMP WITH TIME ZONE in DuckDB

SQLite DDL: `snapshot_time TEXT DEFAULT CURRENT_TIMESTAMP`
DuckDB DDL: `snapshot_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()`

SQLite stores timestamps as TEXT. DuckDB reads them via the SQLite scanner and converts automatically. This is standard SQLite practice.

**Risk**: LOW — DuckDB's SQLite reader handles TEXT→TIMESTAMP conversion.

#### m4: `created_by` metadata value

Our writer sets `created_by = 'DataFusion-DuckLake'`. DuckDB sets `created_by = 'DuckDB {version}'`. This is informational and doesn't affect interop.

#### m5: Snapshot INSERT doesn't populate `schema_version`, `next_catalog_id`, `next_file_id`

The `create_snapshot()` method inserts only `snapshot_time`:
```sql
INSERT INTO ducklake_snapshot (snapshot_time) VALUES (CURRENT_TIMESTAMP) RETURNING snapshot_id
```
The columns `schema_version`, `next_catalog_id`, `next_file_id` get SQLite defaults (`1`, `0`, `0`). DuckDB uses these for bookkeeping. If DuckDB tries to continue from a DF-created snapshot, the sequence values may conflict.

**Risk**: LOW for read-only interop; MEDIUM if DuckDB then writes to the same catalog.

---

## Cross-Engine Test Coverage Summary

### Tests That Exist and Pass

| Test File | Count | DF→DuckDB | DuckDB→DF | Bidirectional |
|-----------|-------|-----------|-----------|---------------|
| `cross_engine_tests.rs` | 7 | 2 | 2 | 1 |
| `cross_engine_insert_tests.rs` | 14 | ~10 | 2 | 0 |
| `cross_engine_ddl_tests.rs` | ~15 | Yes | Yes | 0 |
| `cross_engine_dml_tests.rs` | ~12 | Yes | Yes | 0 |
| `cross_engine_feature_tests.rs` | ~10 | Yes | Yes | 0 |
| `cross_engine_partition_tests.rs` | 7 | **0** | 7 | 0 |
| `cross_engine_inline_tests.rs` | 9 | **0** | 9 | 0 |
| `cross_engine_alter_tests.rs` | ~8 | Yes | Yes | 0 |
| `cross_engine_postgres_tests.rs` | ~8 | Yes | Yes | 0 |
| `cross_engine_mysql_tests.rs` | ~8 | Yes | Yes | 0 |

### DuckDB Read-Back Mechanism

Tests use the `duckdb` crate to open an in-memory DuckDB connection, install the `ducklake` extension, and ATTACH the DF-created SQLite catalog:
```rust
let conn = duckdb::Connection::open_in_memory();
conn.execute("INSTALL ducklake;", []);
conn.execute("LOAD ducklake;", []);
conn.execute(&format!("ATTACH 'ducklake:sqlite:{}' AS ducklake;", path), []);
let rows = conn.query("SELECT * FROM ducklake.main.table_name");
```

This validates true DF→DuckDB interop at the catalog level.

### Coverage Gaps

1. **DF→DuckDB partitioned data**: No tests verify DuckDB can read DF-written partitioned tables
2. **DF→DuckDB inlined data**: No tests (DF writer creates inlined data only in SQLite; DuckDB read-back untested)
3. **Multi-write bidirectional**: Limited tests for DF-write → DuckDB-write → DF-read → DuckDB-read sequences
4. **Postgres/MySQL inlining**: Not testable (backend doesn't implement it)

---

## Backend-Specific Analysis

### SQLite Writer (`metadata_writer_sqlite.rs`)

- **Schema**: Complete (28+ tables, all DuckDB tables present)
- **Inlining**: Implemented via dynamic `ducklake_inlined_data_{table_id}` tables
- **Partitioning**: DDL matches DuckDB; `register_file_partition_value()` implemented
- **Interop tested**: Yes, via `DuckDbConn::open()` with `ducklake:sqlite:` prefix

### PostgreSQL Writer (`metadata_writer_postgres.rs`)

- **Schema**: Same 28+ tables as SQLite (BIGINT types, GENERATED ALWAYS AS IDENTITY)
- **Inlining**: **NOT implemented** (returns `Unsupported` error)
- **Partitioning**: DDL present
- **Interop tested**: Yes, via `cross_engine_postgres_tests.rs` with `ducklake:postgres:` prefix

### MySQL Writer (`metadata_writer_mysql.rs`)

- **Schema**: Same 28+ tables (BIGINT AUTO_INCREMENT, VARCHAR with size limits)
- **Inlining**: **NOT implemented** (returns `Unsupported` error)
- **Partitioning**: DDL present
- **Interop tested**: Yes, via `cross_engine_mysql_tests.rs` with `ducklake:mysql:` prefix

---

## Recommendations

1. **Add DF→DuckDB partition test**: Critical gap. Create a test that writes partitioned data via DF and reads it back with DuckDB.
2. **Add DF→DuckDB inlined data test**: Verify DuckDB can read SQLite inlined tables created by DF with TEXT-only columns.
3. **Populate snapshot metadata fields**: Set `schema_version`, `next_catalog_id`, `next_file_id` properly in `create_snapshot()` for forward compatibility with DuckDB writes.
4. **Consider typed inlined columns**: Map DuckLake types to SQLite affinities (INTEGER, REAL, TEXT, BLOB) instead of all-TEXT.
5. **Document Postgres/MySQL inlining gap**: Make it clear that inlining is SQLite-only and fails gracefully for other backends.
