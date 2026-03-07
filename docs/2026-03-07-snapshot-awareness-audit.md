# Snapshot-Awareness Audit

## Summary
- Total queries audited: 52
- Correctly snapshot-aware: 38
- Intentionally current-only (correct): 11
- Missing snapshot predicates (BUGS FIXED): 3

## Bugs Found and Fixed

### BUG 1: `get_table_structure` missing snapshot_id parameter
- **Files**: `metadata_provider.rs:20-23` (SQL constant), all 4 provider implementations, `table.rs`, `table_functions.rs`
- **Tables affected**: `ducklake_column`
- **Problem**: The trait method accepted only `table_id` with no `snapshot_id`. The DuckDB/SQLite implementations used `end_snapshot IS NULL` (current-only), while Postgres/MySQL had NO `end_snapshot` filter at all, returning dead (dropped/renamed) columns.
- **Impact**: Time-travel queries (`AT SNAPSHOT`) returned wrong columns after schema evolution (ALTER TABLE ADD/DROP/RENAME COLUMN). Postgres/MySQL also returned wrong columns for current queries.
- **Fix**: Added `snapshot_id: i64` parameter to trait and all implementations. Updated SQL to use `begin_snapshot <= ? AND (? < end_snapshot OR end_snapshot IS NULL)`. Updated all callers to pass snapshot_id.

### BUG 2: `list_all_columns` (SQL_LIST_ALL_COLUMNS) missing column snapshot predicate
- **Files**: `metadata_provider.rs:214-229` (SQL constant), all 4 provider implementations
- **Tables affected**: `ducklake_column` (joined but not filtered)
- **Problem**: Query filtered `ducklake_schema` and `ducklake_table` with proper temporal predicates but did NOT filter `ducklake_column`. This returned ALL columns ever created (including dropped ones) in information_schema.columns results.
- **Impact**: After schema evolution, `information_schema.columns` would show dropped columns alongside current ones.
- **Fix**: Added `? >= c.begin_snapshot AND (? < c.end_snapshot OR c.end_snapshot IS NULL)` to the WHERE clause. Updated all 4 providers to pass 6 snapshot_id bindings instead of 4.

### BUG 3: Postgres/MySQL test DDL missing begin_snapshot/end_snapshot on ducklake_column
- **Files**: `tests/postgres_metadata_provider_test.rs`, `tests/mysql_metadata_provider_test.rs`
- **Problem**: Test DDL for ducklake_column did not include begin_snapshot/end_snapshot columns, making snapshot-aware queries impossible.
- **Fix**: Added `begin_snapshot BIGINT NOT NULL DEFAULT 1, end_snapshot BIGINT` to test DDL.

## Queries Audited

### metadata_provider.rs - Shared SQL Constants

#### [metadata_provider.rs:5-6] SQL_GET_LATEST_SNAPSHOT
- **Tables**: `ducklake_snapshot`
- **Snapshot predicate**: N/A (ducklake_snapshot is not snapshot-versioned)
- **Fix needed?**: No

#### [metadata_provider.rs:8] SQL_LIST_SNAPSHOTS
- **Tables**: `ducklake_snapshot`
- **Snapshot predicate**: N/A
- **Fix needed?**: No

#### [metadata_provider.rs:10-12] SQL_LIST_SCHEMAS
- **Tables**: `ducklake_schema`
- **Snapshot predicate**: Correct (`? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)`)
- **Fix needed?**: No

#### [metadata_provider.rs:14-18] SQL_LIST_TABLES
- **Tables**: `ducklake_table`
- **Snapshot predicate**: Correct
- **Fix needed?**: No

#### [metadata_provider.rs:20-25] SQL_GET_TABLE_COLUMNS
- **Tables**: `ducklake_column`
- **Snapshot predicate**: **FIXED** - Was `end_snapshot IS NULL`, now uses proper temporal predicate
- **Fix needed?**: Yes (FIXED)

#### [metadata_provider.rs:27-48] SQL_GET_DATA_FILES
- **Tables**: `ducklake_data_file`, `ducklake_delete_file`
- **Snapshot predicate**: Correct (both data and delete files filtered with temporal predicates)
- **Fix needed?**: No

#### [metadata_provider.rs:50-51] SQL_GET_DATA_PATH
- **Tables**: `ducklake_metadata`
- **Snapshot predicate**: N/A (not snapshot-versioned)
- **Fix needed?**: No

#### [metadata_provider.rs:53-57] SQL_GET_SCHEMA_BY_NAME
- **Tables**: `ducklake_schema`
- **Snapshot predicate**: Correct
- **Fix needed?**: No

#### [metadata_provider.rs:59-64] SQL_GET_TABLE_BY_NAME
- **Tables**: `ducklake_table`
- **Snapshot predicate**: Correct
- **Fix needed?**: No

#### [metadata_provider.rs:66-72] SQL_TABLE_EXISTS
- **Tables**: `ducklake_table`
- **Snapshot predicate**: Correct
- **Fix needed?**: No

#### [metadata_provider.rs:76-88] SQL_GET_DATA_FILES_ADDED_BETWEEN_SNAPSHOTS
- **Tables**: `ducklake_data_file`
- **Snapshot predicate**: Correct (uses begin_snapshot range for CDC)
- **Fix needed?**: No

#### [metadata_provider.rs:90-195] SQL_GET_DELETE_FILES_ADDED_BETWEEN_SNAPSHOTS
- **Tables**: `ducklake_delete_file`, `ducklake_data_file`
- **Snapshot predicate**: Correct (uses begin_snapshot/end_snapshot ranges for CDC)
- **Fix needed?**: No

#### [metadata_provider.rs:199-212] SQL_LIST_ALL_TABLES
- **Tables**: `ducklake_schema`, `ducklake_table`
- **Snapshot predicate**: Correct (both schema and table have temporal predicates)
- **Fix needed?**: No

#### [metadata_provider.rs:214-231] SQL_LIST_ALL_COLUMNS
- **Tables**: `ducklake_schema`, `ducklake_table`, `ducklake_column`
- **Snapshot predicate**: **FIXED** - Schema and table had correct predicates; column was missing temporal predicate entirely
- **Fix needed?**: Yes (FIXED)

#### [metadata_provider.rs:233-262] SQL_LIST_ALL_FILES
- **Tables**: `ducklake_schema`, `ducklake_table`, `ducklake_data_file`, `ducklake_delete_file`
- **Snapshot predicate**: Correct (all four tables have temporal predicates)
- **Fix needed?**: No

### metadata_provider_duckdb.rs

All queries use the shared SQL constants from metadata_provider.rs. The DuckDB provider passes correct parameter bindings for all queries.

- `get_current_snapshot`: N/A
- `get_data_path`: N/A
- `list_snapshots`: N/A
- `list_schemas`: Correct (passes snapshot_id twice)
- `list_tables`: Correct (passes schema_id, snapshot_id, snapshot_id)
- `get_table_structure`: **FIXED** (now passes table_id, snapshot_id, snapshot_id)
- `get_table_files_for_select`: Correct (6 params)
- `get_schema_by_name`: Correct (name, snapshot_id, snapshot_id)
- `get_table_by_name`: Correct (schema_id, name, snapshot_id, snapshot_id)
- `table_exists`: Correct
- `list_all_tables`: Correct (4 params)
- `list_all_columns`: **FIXED** (now passes 6 params)
- `list_all_files`: Correct (8 params)
- `get_data_files_added_between_snapshots`: Correct
- `get_delete_files_added_between_snapshots`: Correct

### metadata_provider_sqlite.rs

All queries are inline SQL in each method (not shared constants).

- `get_current_snapshot`: N/A
- `get_data_path`: N/A
- `list_snapshots`: N/A
- `list_schemas`: Correct
- `list_tables`: Correct
- `get_table_structure`: **FIXED** (was `end_snapshot IS NULL`, now proper temporal)
- `get_table_files_for_select`: Correct
- `get_schema_by_name`: Correct
- `get_table_by_name`: Correct
- `table_exists`: Correct
- `list_all_tables`: Correct
- `list_all_columns`: **FIXED** (added column temporal predicate)
- `list_all_files`: Correct
- `get_data_files_added_between_snapshots`: Correct
- `get_delete_files_added_between_snapshots`: Correct (uses correlated subqueries)

### metadata_provider_postgres.rs

- `get_current_snapshot`: N/A
- `get_data_path`: N/A
- `list_snapshots`: N/A
- `list_schemas`: Correct
- `list_tables`: Correct
- `get_table_structure`: **FIXED** (had NO end_snapshot filter at all)
- `get_table_files_for_select`: Correct
- `get_schema_by_name`: Correct
- `get_table_by_name`: Correct
- `table_exists`: Correct
- `list_all_tables`: Correct
- `list_all_columns`: **FIXED** (added column temporal predicate)
- `list_all_files`: Correct
- `get_data_files_added_between_snapshots`: Correct
- `get_delete_files_added_between_snapshots`: Correct

### metadata_provider_mysql.rs

- `get_current_snapshot`: N/A
- `get_data_path`: N/A
- `list_snapshots`: N/A
- `list_schemas`: Correct
- `list_tables`: Correct
- `get_table_structure`: **FIXED** (had NO end_snapshot filter at all)
- `get_table_files_for_select`: Correct
- `get_schema_by_name`: Correct
- `get_table_by_name`: Correct
- `table_exists`: Correct
- `list_all_tables`: Correct
- `list_all_columns`: **FIXED** (added column temporal predicate)
- `list_all_files`: Correct
- `get_data_files_added_between_snapshots`: Correct
- `get_delete_files_added_between_snapshots`: Correct

### metadata_writer_sqlite.rs - Intentionally Current-Only

All MetadataWriter queries use `end_snapshot IS NULL` intentionally because they operate on the current state for write operations (DDL/DML).

#### [metadata_writer_sqlite.rs:139] get_or_create_schema
- **Query**: `WHERE schema_name = ? AND end_snapshot IS NULL`
- **Intentionally current-only**: Yes - finding the current active schema to reuse or create
- **Fix needed?**: No

#### [metadata_writer_sqlite.rs:174] get_or_create_table
- **Query**: `WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL`
- **Intentionally current-only**: Yes - finding the current active table
- **Fix needed?**: No

#### [metadata_writer_sqlite.rs:219] set_columns (UPDATE)
- **Query**: `UPDATE ducklake_column SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL`
- **Intentionally current-only**: Yes - ending current columns to replace with new ones
- **Fix needed?**: No

#### [metadata_writer_sqlite.rs:276] end_table_files (UPDATE)
- **Query**: `UPDATE ducklake_data_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL`
- **Intentionally current-only**: Yes - ending current files
- **Fix needed?**: No

#### [metadata_writer_sqlite.rs:354] begin_write_transaction - schema lookup
- **Intentionally current-only**: Yes
- **Fix needed?**: No

#### [metadata_writer_sqlite.rs:379] begin_write_transaction - table lookup
- **Intentionally current-only**: Yes
- **Fix needed?**: No

#### [metadata_writer_sqlite.rs:407] begin_write_transaction - column check
- **Intentionally current-only**: Yes
- **Fix needed?**: No

#### [metadata_writer_sqlite.rs:463] begin_write_transaction - end columns
- **Intentionally current-only**: Yes
- **Fix needed?**: No

#### [metadata_writer_sqlite.rs:490] begin_write_transaction - end files (replace mode)
- **Intentionally current-only**: Yes
- **Fix needed?**: No

### table_functions.rs

- `DucklakeTableChangesFunction::call`: Uses `get_table_structure(table.table_id, snapshot_id)` -- **FIXED**
- `DucklakeTableDeletionsFunction::call`: Uses `get_table_structure(table.table_id, snapshot_id)` -- **FIXED**

### information_schema.rs

All information_schema tables call `get_current_snapshot()` and pass the result to MetadataProvider methods. No direct SQL queries. All method calls are correct.

- `SnapshotsTable`: Calls `list_snapshots()` - N/A
- `SchemataTable`: Calls `list_schemas(snapshot_id)` - Correct
- `TablesTable`: Calls `list_all_tables(snapshot_id)` - Correct
- `ColumnsTable`: Calls `list_all_columns(snapshot_id)` - Correct (underlying query now fixed)
- `TableInfoTable`: Calls `list_all_files(snapshot_id)` + `list_all_tables(snapshot_id)` - Correct
- `FilesTable`: Calls `list_all_files(snapshot_id)` - Correct

## Snapshot-Versioned Tables Catalog

From DDL in `metadata_writer_sqlite.rs`:

| Table | has begin_snapshot | has end_snapshot | Notes |
|-------|-------------------|-----------------|-------|
| `ducklake_metadata` | No | No | Key-value config, not versioned |
| `ducklake_snapshot` | No | No | Snapshot log itself |
| `ducklake_schema` | Yes | Yes | Schema lifecycle |
| `ducklake_table` | Yes | Yes | Table lifecycle |
| `ducklake_column` | Yes | Yes | Column lifecycle (schema evolution) |
| `ducklake_data_file` | Yes | Yes | File lifecycle |
| `ducklake_delete_file` | Yes | Yes | Delete file lifecycle |
