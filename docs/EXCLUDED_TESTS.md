# Excluded DuckLake SQLLogic Tests

This document catalogs DuckLake test files from `ducklake/test/sql/` (342 total) that were
**not ported** to the DataFusion extension test suite, along with exclusion reasons.

Tests that **were ported** (adapted and placed in `tests/sqllogictests/sql/`):
- `insert/insert_column_list.test` - INSERT with column ordering and DEFAULT values
- `insert/insert_into_self.test` - Self-referential INSERT (adapted: `STRLEN`->`length`, `query I INSERT`->`statement ok`)
- `general/metadata_cache.test` - COUNT(*) after DELETE (adapted: removed `SET parquet_metadata_cache`)
- `types/floats.test` - FLOAT/DOUBLE with NaN/Inf predicates (adapted: unrolled `foreach`/`endloop`)
- `types/timestamp.test` - TIMESTAMP with infinity values (no adaptation needed)
- `ducklake_basic.test` - Basic end-to-end INSERT/SELECT (adapted: removed DETACH/re-ATTACH, SHOW TABLES, USE, second catalog)

---

## insert/insert_file_size.test
- **Reason**: Uses DuckDB-specific `CALL ducklake.set_option('target_file_size')` and `glob()` for file verification
- **DuckDB constructs**: `CALL ducklake.set_option()`, `glob()`
- **Category**: C (Not Portable)
- **Port later**: No — tests write-side file splitting, not read behavior

## general/attach_at_snapshot.test
- **Reason**: Tests SNAPSHOT_VERSION attach option (DuckDB-specific)
- **DuckDB constructs**: `SNAPSHOT_VERSION`, `DETACH`/re-ATTACH, `READ_WRITE`
- **Category**: C (Not Portable)

## general/data_path_tag.test
- **Reason**: Uses `duckdb_databases()`, `duckdb_tables()` DuckDB catalog functions
- **DuckDB constructs**: `duckdb_databases()`, `duckdb_tables()`, `DETACH`/re-ATTACH, `OVERRIDE_DATA_PATH`
- **Category**: C (Not Portable)

## general/database_size.test
- **Reason**: Uses `PRAGMA database_size`, `PRAGMA_database_size()` DuckDB functions
- **DuckDB constructs**: `PRAGMA`, `PRAGMA_database_size()`
- **Category**: C (Not Portable)

## general/default_path.test
- **Reason**: Uses `CALL ducklake_flush_inlined_data()` and `glob()` for path verification
- **DuckDB constructs**: `CALL ducklake_flush_inlined_data()`, `glob()`, emoji in identifiers
- **Category**: C (Not Portable)

## general/detach_ducklake.test
- **Reason**: Tests DETACH/re-ATTACH and metadata catalog detachment behavior
- **DuckDB constructs**: `DETACH`, `METADATA_CATALOG`
- **Category**: C (Not Portable)

## general/ducklake_read_only.test
- **Reason**: Tests READ_ONLY attach option
- **DuckDB constructs**: `READ_ONLY`, `DETACH`/re-ATTACH
- **Category**: C (Not Portable)

## general/generated_columns.test
- **Reason**: Tests error message for generated columns (DuckDB-specific error format)
- **DuckDB constructs**: Generated column syntax, DuckDB error messages
- **Category**: C (Not Portable)

## general/metadata_parameters.test
- **Reason**: Tests META_TYPE attach option
- **DuckDB constructs**: `META_TYPE`, error message checking
- **Category**: C (Not Portable)

## general/missing_parquet.test
- **Reason**: Tests missing parquet extension autoloading behavior
- **DuckDB constructs**: `require no_extension_autoloading`, `ducklake_snapshots()`
- **Category**: C (Not Portable)

## general/paths.test
- **Reason**: Uses internal metadata tables and `CALL ducklake_flush_inlined_data()` for path verification
- **DuckDB constructs**: `CALL ducklake_flush_inlined_data()`, `glob()`, `ducklake_meta.*` tables
- **Category**: C (Not Portable)

## general/prepared_statement.test
- **Reason**: Uses `mode skip` (test is skipped even in DuckDB), `SET parquet_metadata_cache`
- **DuckDB constructs**: `mode skip`, `SET`, `PREPARE`/`EXECUTE`
- **Category**: C (Not Portable)

## general/recursive_metadata_catalog.test
- **Reason**: Tests recursive metadata catalog error (DuckDB-specific)
- **DuckDB constructs**: `METADATA_CATALOG` with self-reference
- **Category**: C (Not Portable)

## types/all_types.test
- **Reason**: Uses `test_all_types()` DuckDB function, `EXCLUDE` syntax, complex types
- **DuckDB constructs**: `test_all_types()`, `EXCLUDE`, `CREATE VIEW ... FROM`
- **Category**: C (Not Portable)

## types/json.test
- **Reason**: Requires DuckDB json extension
- **DuckDB constructs**: `require json`, `JSON` type, `typeof()`
- **Category**: C (Not Portable)

## types/json_alter_table.test
- **Reason**: JSON type with ALTER TABLE (requires json extension)
- **DuckDB constructs**: `require json`, `JSON` type
- **Category**: C (Not Portable)

## types/list.test
- **Reason**: LIST type not yet supported by DataFusion DuckLake extension
- **DuckDB constructs**: `stats()`, LIST column operations
- **Category**: C (Not Portable)
- **Port later**: Yes — when complex type support is implemented

## types/struct.test
- **Reason**: STRUCT type not yet supported by DataFusion DuckLake extension
- **DuckDB constructs**: `stats()`, STRUCT column operations
- **Category**: C (Not Portable)
- **Port later**: Yes — when complex type support is implemented

## types/map.test
- **Reason**: MAP type not supported, uses `CALL ducklake_flush_inlined_data()`
- **DuckDB constructs**: MAP type, `ducklake_flush_inlined_data()`
- **Category**: C (Not Portable)

## types/unsupported.test
- **Reason**: Tests DuckDB-specific unsupported types (UNION, ENUM, fixed arrays)
- **DuckDB constructs**: DuckDB-specific type system errors
- **Category**: C (Not Portable)

## types/variant.test
- **Reason**: VARIANT type not supported by DataFusion
- **DuckDB constructs**: `VARIANT` type, `variant_typeof()`, `foreach`/`endloop`
- **Category**: C (Not Portable)

## types/null_byte.test
- **Reason**: Uses `FROM table` shorthand (not supported by DataFusion) and null byte string representation differs between DuckDB and DataFusion
- **DuckDB constructs**: `FROM table` without SELECT, `chr(0)`, `\0` in expected output
- **Category**: B (Needs Adaptation)
- **Port later**: Yes — when `FROM table` shorthand handling is added to preprocessor

---

## Entire directories excluded (all files Not Portable)

### add_files/ (31 files)
- **Reason**: All tests use `ducklake_add_data_files()`, `COPY ... TO`, metadata tables
- **Category**: C (Not Portable)

### alter/ (25 files)
- **Reason**: Mix of portable and non-portable. Portable tests (`add_column`, `drop_column`, `rename_column`, `rename_table`, `promote_type`) deferred to Phase 2. Non-portable tests use struct evolution, metadata tables, or `ducklake_expire_snapshots`.
- **Port later**: Yes — basic ALTER tests in Phase 2

### attach/ (2 files)
- **Reason**: Tests ATTACH/DETACH behaviors (DuckDB-specific)
- **Category**: C (Not Portable)

### audit/ (1 file)
- **Reason**: Uses `ducklake.set_commit_message()`, `snapshots()` functions
- **Category**: C (Not Portable)

### autoloading/ (1 file)
- **Reason**: Tests DuckDB extension autoloading
- **Category**: C (Not Portable)

### catalog/ (4 files)
- **Reason**: Portable tests (`schema`, `drop_table`, `quoted_identifiers`) deferred to Phase 2. One file (`create_then_drop_macro.test`) is DuckDB-specific.
- **Port later**: Yes — Phase 2

### checkpoint/ (4 files)
- **Reason**: Tests CHECKPOINT behavior, metadata tables, multi-connection
- **Category**: C (Not Portable)

### cleanup/ (2 files)
- **Reason**: Uses `ducklake_cleanup_old_files()`, `ducklake_rewrite_data_files()`
- **Category**: C (Not Portable)

### clickbench/ (1 slow test)
- **Reason**: Complex benchmark setup
- **Category**: C (Not Portable)

### cloud/ (1 file)
- **Reason**: Cloud/S3 specific testing
- **Category**: C (Not Portable)

### comments/ (5 files)
- **Reason**: COMMENT ON not supported by DataFusion
- **Category**: C (Not Portable)

### compaction/ (27 files + 1 slow)
- **Reason**: All compaction/maintenance operations are DuckDB write-side operations
- **Category**: C (Not Portable)

### concurrent/ (4 files + 1 slow)
- **Reason**: Multi-connection (`con1`/`con2`) not supported
- **Category**: C (Not Portable)

### constraints/ (3 files)
- **Reason**: Deferred to Phase 4. `not_null.test` is portable but DESCRIBE format may differ.
- **Port later**: Yes — Phase 4

### data_inlining/ (28 files)
- **Reason**: All tests deeply tied to DuckDB internal data inlining optimization
- **Category**: C (Not Portable)

### default/ (4 files)
- **Reason**: Deferred. `default_values.test` is portable.
- **Port later**: Yes — Phase 4

### delete/ (11 files)
- **Reason**: Portable tests (`basic_delete`, `empty_delete`, `delete_same_transaction`) deferred. Non-portable tests use S3, EXPLAIN ANALYZE regex, metadata tables, time travel.
- **Port later**: Yes — Phase 1 (when delete verification is prioritized)

### deletion_inlining/ (15 files + 1 slow)
- **Reason**: All tests tied to DuckDB internal deletion inlining
- **Category**: C (Not Portable)

### encryption/ (2 files)
- **Reason**: Tests encrypted DuckLake (DuckDB-specific)
- **Category**: C (Not Portable)

### functions/ (2 files)
- **Reason**: Tests `ducklake_snapshots()`, `ducklake_table_info()` functions
- **Category**: C (Not Portable)

### geo/ (5 files)
- **Reason**: Requires `spatial` extension, GEOMETRY type
- **Category**: C (Not Portable)

### initialize/ (2 files)
- **Reason**: Tests ATTACH with various options, read-only mode
- **Category**: C (Not Portable)

### issues/ (1 file)
- **Reason**: `late_materialization.test` is portable but deferred to Phase 4
- **Port later**: Yes — Phase 4

### list_files/ (1 file)
- **Reason**: Tests `ducklake_list_files()` function
- **Category**: C (Not Portable)

### macros/ (10 files)
- **Reason**: DuckLake macro catalog (DuckDB-specific)
- **Category**: C (Not Portable)

### merge/ (5 files + 1 slow)
- **Reason**: MERGE INTO not supported by DataFusion
- **Category**: C (Not Portable)

### metadata/ (5 files)
- **Reason**: Tests DuckDB-specific metadata and settings
- **Category**: C (Not Portable)

### migration/ (4 files)
- **Reason**: Internal catalog format migration
- **Category**: C (Not Portable)

### partitioning/ (10 files + 1 slow)
- **Reason**: Write-side partitioning feature
- **Category**: C (Not Portable)

### remove_orphans/ (2 files)
- **Reason**: Uses `ducklake_remove_orphaned_files()`
- **Category**: C (Not Portable)

### rewrite_data_files/ (10 files)
- **Reason**: Data file rewriting/compaction
- **Category**: C (Not Portable)

### rowid/ (2 files)
- **Reason**: DuckLake row ID tracking not exposed in DataFusion
- **Category**: C (Not Portable)

### schema_evolution/ (1 file)
- **Reason**: Uses `ducklake_flush_inlined_data`, internal metadata tables
- **Category**: C (Not Portable)

### secrets/ (1 file)
- **Reason**: DuckDB secrets management
- **Category**: C (Not Portable)

### settings/ (5 files)
- **Reason**: DuckLake Parquet settings (DuckDB-specific)
- **Category**: C (Not Portable)

### snapshot_info/ (2 files)
- **Reason**: Uses `ducklake_current_snapshot()`, multi-connection
- **Category**: C (Not Portable)

### sorted_table/ (26 files)
- **Reason**: SET SORTED BY metadata, merge-on-sorted compaction
- **Category**: C (Not Portable)

### stats/ (11 files)
- **Reason**: Portable tests (`filter_pushdown`, `count_star_optimization_basic`) deferred to Phase 4. Non-portable tests use `stats()`, EXPLAIN ANALYZE, metadata tables.
- **Port later**: Yes — Phase 4

### table_changes/ (8 files)
- **Reason**: CDC / table change tracking functions
- **Category**: C (Not Portable)

### time_travel/ (2 files)
- **Reason**: Time travel not supported by DataFusion
- **Category**: C (Not Portable)

### tpch/ (1 slow test)
- **Reason**: Complex benchmark with PRAGMA, foreach/endloop
- **Category**: C (Not Portable)

### transaction/ (12 files)
- **Reason**: Portable tests (`basic_transaction`) deferred to Phase 5. Non-portable tests use multi-connection, glob(), metadata tables.
- **Port later**: Yes — Phase 5

### update/ (7 files)
- **Reason**: Portable tests (`basic_update`, `test_update_expression`) deferred. Non-portable tests use EXPLAIN ANALYZE, metadata tables, partitioning.
- **Port later**: Yes — when UPDATE support is implemented

### view/ (8 files)
- **Reason**: Portable tests (`ducklake_view`, `ducklake_view_schema`) deferred to Phase 4. Non-portable tests use DuckDB catalog functions.
- **Port later**: Yes — Phase 4

### virtualcolumns/ (2 files)
- **Reason**: DuckLake virtual columns (filename, file_row_number, snapshot_id) not exposed in DataFusion
- **Category**: C (Not Portable)
- **Port later**: Yes — when virtual column support is implemented
