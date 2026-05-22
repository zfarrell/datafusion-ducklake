# Excluded DuckLake SQLLogic Tests

This document catalogs DuckLake test files from `ducklake/test/sql/` (342 total) that were
**not ported** to the DataFusion extension test suite, along with exclusion reasons.

Tests that **were ported** (adapted and placed in `tests/sqllogictests/sql/`):
- `insert/insert_column_list.test` - INSERT with column ordering and DEFAULT values
- `insert/insert_into_self.test` - Self-referential INSERT (adapted: `STRLEN`->`length`, `query I INSERT`->`statement ok`)
- `general/metadata_cache.test` - COUNT(*) after DELETE (adapted: removed `SET parquet_metadata_cache`)
- `types/floats.test` - FLOAT/DOUBLE with NaN predicates (adapted: unrolled `foreach`/`endloop`, removed infinity section)
- `ducklake_basic.test` - Basic end-to-end INSERT/SELECT (adapted: removed DETACH/re-ATTACH, SHOW TABLES, USE, second catalog)
- `delete/basic_delete.test` - DELETE + read verification (adapted: removed transaction wrapper, time travel)
- `delete/empty_delete.test` - DELETE WHERE false (adapted: `query I DELETE` → `statement ok`)
- `delete/delete_same_transaction.test` - Multiple deletes (adapted: removed transaction wrapper, glob())
- `delete/truncate_table.test` - DELETE all rows (adapted: removed transaction wrappers, glob())
- `update/basic_update.test` - UPDATE + read verification (adapted: removed transaction wrapper, time travel)
- `update/update_not_null.test` - UPDATE with NOT NULL constraint (adapted: `FROM table` → `SELECT * FROM`, statement error format)
- `update/update_same_transaction.test` - UPDATE + read (adapted: removed transaction wrapper)
- `issues/late_materialization.test` - Filter + ORDER BY + LIMIT (adapted: removed `USE`, fully-qualified names)
- `catalog/drop_table.test` - DROP TABLE (adapted: removed transaction-local tests, duckdb_tables())
- `catalog/schema.test` - CREATE/DROP SCHEMA (adapted: removed duckdb_schemas(), cross-schema reads, foreach)
- `alter/add_column.test` - ALTER TABLE ADD COLUMN (adapted: removed DESCRIBE, statement error format)
- `alter/drop_column.test` - ALTER TABLE DROP COLUMN (adapted: `FROM table` → `SELECT * FROM`, statement error format)
- `alter/rename_column.test` - ALTER TABLE RENAME COLUMN (adapted: removed DESCRIBE, statement error format)
- `default/default_values.test` - DEFAULT column values (adapted: removed special values test)

---

## Known Limitations of Hybrid Test Adapter

The following limitations affect which tests can be ported:

1. **Transaction-local visibility**: DataFusion reads committed metadata only. Reads within BEGIN...COMMIT see stale data. Tests that verify state within transactions must have transaction wrappers removed.

2. **Table reference rewriting**: The adapter rewrites `ducklake.X` to `ducklake.main.X`. This breaks 3-part names like `ducklake.s1.tbl` (rewrites to 4-part `ducklake.main.s1.tbl`). Cross-schema reads cannot be tested until this is fixed.

3. **`COMMIT;` with semicolons**: The write statement detection doesn't match `COMMIT;` (with trailing semicolon). Remove semicolons from COMMIT/ROLLBACK in test files.

4. **Infinity literal parsing**: DataFusion may not parse `'inf'`/`'-inf'` as float infinity in WHERE clauses. NaN comparisons via `'NaN'` work correctly.

5. **`statement error` format**: DuckDB uses `statement error\nSQL\n----\npattern` but the sqllogictest crate expects `statement error pattern\nSQL`. All ported tests use the standard format.

---

## types/timestamp.test
- **Reason**: DataFusion's `simplify_expressions` optimizer fails with 'infinity' timestamp literal
- **DuckDB constructs**: `TIMESTAMP 'infinity'` literal
- **Category**: B (Needs Adaptation)
- **Port later**: Yes — when DataFusion supports infinity timestamps or test is restructured to avoid them

## insert/insert_file_size.test
- **Reason**: Uses DuckDB-specific `CALL ducklake.set_option('target_file_size')` and `glob()` for file verification
- **DuckDB constructs**: `CALL ducklake.set_option()`, `glob()`
- **Category**: C (Not Portable)
- **Port later**: No — tests write-side file splitting, not read behavior

## types/null_byte.test
- **Reason**: Null byte string representation differs between DuckDB and DataFusion
- **DuckDB constructs**: `chr(0)`, `\0` in expected output
- **Category**: B (Needs Adaptation)
- **Port later**: Yes — requires investigation of null byte handling in DataFusion output

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

## delete/delete_metadata.test
- **Reason**: S3 test, EXPLAIN ANALYZE regex, ducklake_table_info
- **Category**: C (Not Portable)

## delete/multi_deletes.test
- **Reason**: Multiple deletes + glob() + metadata tables + time travel
- **Category**: C (Not Portable)

## delete/delete_file_stats.test
- **Reason**: Stats verification via metadata tables
- **Category**: C (Not Portable)

## delete/delete_rollback_cleanup.test
- **Reason**: Uses glob() for file cleanup verification
- **Category**: C (Not Portable)

## delete/delete_ignore_extra_columns.test
- **Reason**: Tests internal delete file handling
- **Category**: C (Not Portable)

## delete/delete_join.test
- **Reason**: `DELETE ... USING` syntax (DuckDB-specific)
- **Category**: C (Not Portable)

## update/update_partitioning.test
- **Reason**: Uses `SET PARTITIONED BY`, `EXPLAIN ANALYZE`, `regexp_extract()`, metadata tables
- **Category**: C (Not Portable)

## update/update_rollback.test
- **Reason**: Uses `glob()` for file cleanup verification
- **Category**: C (Not Portable)

## update/update_join_duplicates.test
- **Reason**: `UPDATE ... FROM` join syntax (DuckDB-specific)
- **Category**: B (Needs Adaptation)
- **Port later**: Yes — requires DuckDB UPDATE ... FROM syntax investigation

## constraints/not_null.test
- **Reason**: Uses DESCRIBE (output format differs), ALTER SET/DROP NOT NULL in transactions
- **Category**: B (Needs Adaptation)
- **Port later**: Yes — when DESCRIBE format is aligned or constraint ALTER tests restructured

## constraints/not_null_drop_column.test
- **Reason**: Combines NOT NULL constraints with column drops
- **Category**: B (Needs Adaptation)
- **Port later**: Yes

## constraints/unsupported.test
- **Reason**: Tests DuckDB-specific unsupported constraints (CHECK, UNIQUE, etc.)
- **Category**: C (Not Portable)

## default/default_expressions.test
- **Reason**: Uses DuckDB-specific default expressions
- **Category**: B (Needs Adaptation)
- **Port later**: Yes — when expression defaults are investigated

## default/struct_field_default.test
- **Reason**: DEFAULT values for struct fields (complex types)
- **Category**: C (Not Portable)

## default/add_column_with_default.test
- **Reason**: Uses `ducklake_flush_inlined_data`, `foreach`/`endloop`, metadata tables
- **Category**: C (Not Portable)

## catalog/create_then_drop_macro.test
- **Reason**: DuckLake macro catalog management (DuckDB-specific)
- **Category**: C (Not Portable)

---

## Entire directories excluded (all files Not Portable)

### add_files/ (31 files)
- **Reason**: All tests use `ducklake_add_data_files()`, `COPY ... TO`, metadata tables
- **Category**: C (Not Portable)

### alter/ (remaining 22 of 25 files)
- **Reason**: Non-portable tests use struct evolution, metadata tables, `ducklake_expire_snapshots`, DESCRIBE format differences, or transaction-local operations
- **Ported**: `add_column.test`, `drop_column.test`, `rename_column.test`
- **Port later**: `rename_table.test`, `promote_type.test`, `mixed_alter.test` when transaction-local reads work

### attach/ (2 files)
- **Reason**: Tests ATTACH/DETACH behaviors (DuckDB-specific)
- **Category**: C (Not Portable)

### audit/ (1 file)
- **Reason**: Uses `ducklake.set_commit_message()`, `snapshots()` functions
- **Category**: C (Not Portable)

### autoloading/ (1 file)
- **Reason**: Tests DuckDB extension autoloading
- **Category**: C (Not Portable)

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

### data_inlining/ (28 files)
- **Reason**: All tests deeply tied to DuckDB internal data inlining optimization
- **Category**: C (Not Portable)

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

### stats/ (remaining 9 of 11 files)
- **Reason**: Non-portable tests use `stats()`, EXPLAIN ANALYZE, metadata tables
- **Ported**: `filter_pushdown.test`, `cardinality.test` (pass without adaptation)

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
- **Reason**: Most tests use multi-connection or glob(). Basic transaction tests require hybrid adapter improvements for transaction-local visibility.
- **Category**: C/B (Mixed)
- **Port later**: Yes — when hybrid adapter supports transaction-local reads

### view/ (8 files)
- **Reason**: View tests use `duckdb_views()`, `USE` catalog switching, or test error messages
- **Category**: B/C (Mixed)
- **Port later**: Yes — when views support is implemented and table reference rewriting handles multi-schema

### virtualcolumns/ (2 files)
- **Reason**: DuckLake virtual columns (filename, file_row_number, snapshot_id) not exposed in DataFusion
- **Category**: C (Not Portable)
- **Port later**: Yes — when virtual column support is implemented
