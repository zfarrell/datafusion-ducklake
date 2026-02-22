# DuckLake SQLLogic Test Portability Survey for DataFusion

## 1. Executive Summary

The DuckLake test suite at `/ducklake/test/sql/` contains **342 test files** across **48 directories** (plus 1 top-level file). After analyzing representative test files from every directory, the portability assessment is:

| Category | Directories | Test Files (approx.) | Description |
|----------|------------|---------------------|-------------|
| **A - Directly Portable** | 4 | ~20 | Standard SQL, minimal changes needed |
| **B - Needs Adaptation** | 22 | ~165 | Relevant features, DuckDB-specific syntax to replace |
| **C - Not Portable** | 22 | ~157 | DuckDB-internal features, no DataFusion equivalent |

**Key Finding**: The hybrid test runner already handles the most common DuckDB directives (`require ducklake`, `test-env`, `ATTACH`/`DETACH`). The main barriers to portability are:
1. DuckDB-specific CALL statements (ducklake_flush_inlined_data, ducklake_merge_adjacent, etc.) -- found in **149 files** (520 occurrences)
2. DuckDB metadata catalog queries (`__ducklake_metadata_*`, `ducklake_meta.*`) -- found in **64 files**
3. DuckDB system functions (`duckdb_tables()`, `duckdb_schemas()`, `stats()`, `glob()`) -- found in ~30 files
4. Time travel syntax (`AT (VERSION => ...)`) -- found in **28 files**
5. DuckDB test directives (`foreach`/`endloop`, `mode skip`, `con1`/`con2` multi-connection) -- found in ~41+ files
6. `USE` catalog switching -- found in ~110 files

## 2. DuckDB-Specific Constructs Reference

### Already Handled by the Hybrid Test Runner
| Construct | Handling | Notes |
|-----------|---------|-------|
| `require ducklake` / `require parquet` | Stripped | Preprocessor removes these directives |
| `test-env` directives | Stripped | Preprocessor removes these |
| `# name:` / `# description:` / `# group:` | Stripped | Preprocessor removes these comment directives |
| `ATTACH 'ducklake:...'` / `DETACH` | Stripped | Connection managed in Rust |
| `EXPLAIN` statements | Stripped | Skipped as output format differs |

### DuckDB Constructs NOT Supported by DataFusion

| Construct | Frequency | DataFusion Equivalent | Adaptation |
|-----------|-----------|----------------------|------------|
| `CALL ducklake_flush_inlined_data()` | 94 files | None (write-side operation) | Remove or skip |
| `CALL ducklake_merge_adjacent_files()` | ~40 files | None (compaction) | Remove or skip |
| `CALL ducklake_expire_snapshots()` | ~25 files | None (maintenance) | Remove or skip |
| `CALL ducklake_rewrite_data_files()` | ~10 files | None (compaction) | Remove or skip |
| `CALL ducklake_cleanup_old_files()` | ~10 files | None (maintenance) | Remove or skip |
| `CALL ducklake_add_data_files()` | ~31 files | None (file management) | Remove or skip |
| `CALL ducklake.set_option()` | ~30 files | None (DuckLake config) | Remove or skip |
| `CALL ducklake.set_commit_message()` | 1 file | None (audit) | Skip entirely |
| `ducklake_snapshots()` / `snapshots()` | ~20 files | None | Skip verification queries |
| `ducklake_table_info()` | ~5 files | None | Skip verification queries |
| `ducklake_current_snapshot()` | ~2 files | None | Skip entirely |
| `ducklake_list_files()` | 1 file | None | Skip entirely |
| `table_changes()` | 8 files | None | Skip entirely |
| `__ducklake_metadata_*` tables | ~64 files | None (internal catalog) | Skip metadata verification |
| `duckdb_tables()` / `duckdb_schemas()` / `duckdb_views()` | ~12 files | `information_schema` | Replace with DF equivalent |
| `stats()` function | ~12 files | None | Skip stats verification |
| `PRAGMA database_size` | 1 file | None | Skip |
| `glob()` / `FROM glob(...)` | ~19 files | None | Skip file system verification |
| `SHOW TABLES` | ~5 files | `SELECT * FROM information_schema.tables` | Replace |
| `DESCRIBE` | ~5 files | `DESCRIBE` (DataFusion supports this) | May work directly |
| `USE <catalog>` | ~110 files | Not supported for catalog switching | Must use fully-qualified names |
| `AT (VERSION => ...)` time travel | ~28 files | None | Skip or remove time travel queries |
| `AT (TIMESTAMP => ...)` time travel | ~5 files | None | Skip |
| `foreach` / `endloop` directives | ~41 files | Not supported by sqllogictest crate | Unroll loops or skip |
| `mode skip` | ~5 files | Not supported | Remove (already skipped) |
| `con1` / `con2` (multi-connection) | ~15 files | Not supported | Skip concurrent tests |
| `COPY ... TO` | ~30 files | None (write-side) | Skip |
| `CREATE TEMPORARY TABLE` | ~5 files | DataFusion supports | May work |
| `SET parquet_metadata_cache` | ~2 files | None | Remove |
| `SET VARIABLE` / `getvariable()` | ~21 files | None | Skip or restructure |
| `MERGE INTO` | 5 files | Not supported by DataFusion | Skip |
| `ALTER TABLE ... SET PARTITIONED BY` | ~10 files | None | Skip (write-side) |
| `ALTER TABLE ... SET SORTED BY` | ~26 files | None | Skip (write-side) |
| `printf()` function | ~5 files | `format()` or similar | Replace |
| `range()` table function | Many files | `generate_series()` | Replace |
| `test_all_types()` | 1 file | None | Skip |
| `VARIANT` type | ~2 files | None | Skip |
| `JSON` type | ~2 files | None | Skip |
| `GEOMETRY` type / spatial functions | 5 files | None | Skip |
| `require icu` / `require httpfs` / `require spatial` | ~10 files | None | Skip |
| `parquet_metadata()` | ~2 files | None | Skip |
| `CREATE OR REPLACE TABLE` | ~5 files | DataFusion supports | Works |
| `INSERT INTO ... FROM <table>` (without SELECT) | Many files | `INSERT INTO ... SELECT * FROM` | Replace |
| `FROM <table>` (without SELECT) | Many files | `SELECT * FROM <table>` | Replace |
| `ORDER BY ALL` | ~10 files | DataFusion supports this | Works directly |
| `COUNT(*) FILTER(WHERE ...)` | ~5 files | DataFusion supports this | Works directly |
| `regexp_extract()` | ~5 files | None | Skip verification queries |

### DataFusion-Supported Constructs Found in Tests
- Standard `CREATE TABLE`, `INSERT`, `SELECT`, `DELETE`, `UPDATE`, `DROP TABLE`
- `ALTER TABLE ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `RENAME TABLE`
- `ALTER TABLE ALTER COLUMN SET DATA TYPE` (type promotion)
- `CREATE SCHEMA`, `DROP SCHEMA`
- `CREATE VIEW`, `DROP VIEW`
- `BEGIN`, `COMMIT`, `ROLLBACK` transactions
- `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, `JOIN`
- Aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- `COUNT(*) FILTER(WHERE ...)` syntax
- `NOT NULL` constraints
- `DEFAULT` values
- `TRUNCATE TABLE`
- `CREATE TABLE ... AS SELECT`
- Subqueries, CTEs
- Type casting
- `IN` lists, `BETWEEN`, `LIKE`
- `UNION ALL`, `EXCEPT`
- `ORDER BY ALL`

## 3. Test Directory Analysis

### `general/` -- 13 files
**Feature area**: Core DuckLake setup, attach/detach, paths, metadata parameters, read-only mode
**Category**: C (Not Portable) -- 11 files, B (Needs Adaptation) -- 2 files
**DuckDB constructs**: `ATTACH`/`DETACH`/`USE`, `PRAGMA`, `duckdb_databases()`, `CALL ducklake_flush_inlined_data()`, `glob()`, `SET parquet_metadata_cache`, `SNAPSHOT_VERSION`, multiple attach configs, `test-env`, `mode skip`
**Estimated adaptation effort**: Significant
**Notes**: Most tests exercise DuckDB-specific catalog management. `metadata_cache.test` and `ducklake_read_only.test` have some portable read queries but are wrapped in DuckDB setup.
**Portable queries**: COUNT(*) after insert/delete (in metadata_cache.test)
**Priority**: Low -- mostly infrastructure tests

### `insert/` -- 3 files
**Feature area**: INSERT with column lists, default values, self-referential inserts, file size splitting
**Category**: B (Needs Adaptation) -- 2 files, C (Not Portable) -- 1 file
**DuckDB constructs**: `CALL ducklake.set_option('target_file_size')`, `glob()`, `INSERT INTO ... FROM` syntax, `STRLEN()` function, `concat()` function
**Specific files**:
- `insert_column_list.test` -- **B**: Standard INSERT with column ordering, DEFAULT VALUES. Needs `FROM` -> `SELECT * FROM` adaptation. Core read queries are portable.
- `insert_into_self.test` -- **B**: INSERT INTO ... FROM self-join. Uses `STRLEN()` (replace with `length()`). Core logic is standard SQL.
- `insert_file_size.test` -- **C**: Tests DuckLake file splitting via `set_option` and `glob()`.
**Estimated adaptation effort**: Trivial for the B tests
**Priority**: High -- tests basic INSERT/read round-trips

### `delete/` -- 11 files
**Feature area**: DELETE operations, truncate, delete joins, delete file stats, rollback cleanup
**Category**: B (Needs Adaptation) -- 5 files, C (Not Portable) -- 6 files
**DuckDB constructs**: `AT (VERSION =>)`, `glob()`, `EXPLAIN ANALYZE` regex matching, `CALL ducklake_flush_inlined_data`, `require-env S3_TEST_SERVER_AVAILABLE`, `DELETE ... USING` syntax, `__ducklake_metadata` tables, `COUNT(*) FILTER(WHERE ...)`
**Specific files**:
- `basic_delete.test` -- **B**: Core DELETE + verification. Last query uses time travel (skip it). Main read queries are standard. `COUNT(*) FILTER(WHERE ...)` is DataFusion-supported.
- `delete_join.test` -- **B**: DELETE ... USING (DuckDB syntax, may need adaptation). Core count verification is portable.
- `empty_delete.test` -- **B**: DELETE WHERE false. Standard SQL.
- `truncate_table.test` -- **B**: TRUNCATE followed by reads. Uses `glob()` for file checks (skip those).
- `delete_same_transaction.test` -- **B**: Transaction-local delete verification. Mostly standard SQL.
- `delete_metadata.test` -- **C**: S3 test, EXPLAIN ANALYZE regex, ducklake_table_info.
- `multi_deletes.test` -- **C**: Multiple deletes + `glob()` file verification + `__ducklake_metadata` queries + time travel.
- `delete_file_stats.test` -- **C**: Stats verification via metadata tables.
- `delete_rollback_cleanup.test` -- **C**: Uses `glob()` for file cleanup verification.
- `delete_ignore_extra_columns.test` -- **C**: Tests internal delete file handling.
- `test_delete_partial_max_snapshot.test` -- **C**: Internal snapshot handling.
**Estimated adaptation effort**: Trivial to moderate for B tests
**Priority**: High -- tests core DELETE + read verification

### `update/` -- 7 files
**Feature area**: UPDATE operations, expression updates, join updates, partitioned updates
**Category**: B (Needs Adaptation) -- 4 files, C (Not Portable) -- 3 files
**DuckDB constructs**: `AT (VERSION =>)`, `USE`, `EXPLAIN ANALYZE`, `regexp_extract()`, `ALTER TABLE SET PARTITIONED BY`, `UPDATE ... FROM` syntax
**Specific files**:
- `basic_update.test` -- **B**: Standard UPDATE + verification reads. Time travel queries (skip). Core aggregation queries are portable.
- `test_update_expression.test` -- **B**: UPDATE with CASE expression. Uses `USE ducklake` (adapt to FQ names). Standard SQL otherwise.
- `update_join_duplicates.test` -- **B**: UPDATE ... FROM join. Uses `CREATE TEMPORARY TABLE`. Read verification is standard.
- `update_not_null.test` -- **B**: UPDATE with NOT NULL constraints. Standard SQL.
- `update_same_transaction.test` -- **B**: In-transaction update. Standard SQL.
- `update_partitioning.test` -- **C**: Uses `SET PARTITIONED BY`, `EXPLAIN ANALYZE`, `regexp_extract()`, metadata tables.
- `update_rollback.test` -- **C**: Uses `glob()` for file cleanup verification.
**Estimated adaptation effort**: Trivial for B tests
**Priority**: High -- tests UPDATE + read verification

### `alter/` -- 25 files
**Feature area**: ALTER TABLE (add/drop/rename column, rename table, type promotion, struct evolution)
**Category**: B (Needs Adaptation) -- 12 files, C (Not Portable) -- 13 files
**DuckDB constructs**: `DESCRIBE`, `USE`, `ORDER BY ALL`, struct evolution (`ALTER COLUMN SET DATA TYPE STRUCT(...)`), `FROM <table>` without SELECT, `ducklake_expire_snapshots`, metadata tables
**Specific files**:
- `add_column.test` -- **B**: Standard ALTER TABLE ADD COLUMN + DESCRIBE + reads. Highly portable.
- `drop_column.test` -- **B**: Standard ALTER TABLE DROP COLUMN. `FROM ducklake.test` -> `SELECT * FROM`. Portable.
- `rename_column.test` -- **B**: RENAME COLUMN + DESCRIBE. Portable.
- `rename_table.test` -- **B**: RENAME TABLE in transactions. Complex but standard SQL operations.
- `promote_type.test` -- **B**: ALTER COLUMN SET DATA TYPE (widening). Portable.
- `mixed_alter.test` / `mixed_alter2.test` -- **B**: Combination of ALTER operations. Mostly standard SQL.
- `rename_entity.test` -- **B**: Rename schemas/tables. Uses `duckdb_schemas()`.
- `rename_table_case.test` -- **B**: Case sensitivity testing.
- `rename_table_dbt_workload.test` -- **B**: dbt-style rename workflows.
- `rename_table_within_transaction.test` -- **B**: Transaction-local renames.
- `add_column_transaction_local.test` -- **B**: Transaction-local column add.
- `struct_evolution.test` -- **C**: ALTER COLUMN SET DATA TYPE STRUCT(...) with field addition/removal. Complex struct evolution.
- `struct_evolution_*.test` (7 files) -- **C**: All test complex struct/list/map field evolution.
- `add_column_nested.test` -- **C**: Nested struct column add.
- `drop_column_nested.test` -- **C**: Nested struct column drop.
- `alter_timestamptz_promotion.test` -- **C**: TIMESTAMPTZ promotion.
- `expire_snapshot_bug.test` -- **C**: Uses ducklake_expire_snapshots, metadata tables.
**Estimated adaptation effort**: Trivial for simple alter tests, significant for struct evolution
**Priority**: High for basic alter operations (add/drop/rename column/table)

### `types/` -- 11 files
**Feature area**: Data type support (all types, floats, timestamps, structs, lists, maps, JSON, variant)
**Category**: B (Needs Adaptation) -- 5 files, C (Not Portable) -- 6 files
**DuckDB constructs**: `test_all_types()`, `stats()`, `foreach`/`endloop`, `typeof()`, `VARIANT` type, `JSON` type, `MAP` accessor syntax, `CREATE OR REPLACE TABLE`, `CALL ducklake_flush_inlined_data`, `mode skip`
**Specific files**:
- `floats.test` -- **B**: Tests FLOAT/DOUBLE with NaN/Inf comparisons. Uses `foreach`/`endloop` (unroll). Core filter queries are portable. `CREATE OR REPLACE TABLE` supported.
- `timestamp.test` -- **B**: Tests TIMESTAMP with infinity values. Simple and portable queries.
- `null_byte.test` -- **B**: Tests null bytes in VARCHARs. Uses `chr()` function (DataFusion supports this). Portable.
- `struct.test` -- **B**: Tests STRUCT storage/retrieval. Uses `stats()` (remove). Core struct queries may be portable if DataFusion supports DuckLake struct types.
- `list.test` -- **B**: Tests LIST storage/retrieval. Uses `stats()` (remove). Core queries may work if complex types are supported.
- `all_types.test` -- **C**: Uses `test_all_types()`, `EXCLUDE`, `CREATE VIEW ... FROM test_all_types()`.
- `json.test` -- **C**: Requires `require json`, uses `JSON` type and `typeof()`.
- `json_alter_table.test` -- **C**: JSON type with ALTER TABLE.
- `map.test` -- **C**: MAP type with accessor syntax and `ducklake_flush_inlined_data`.
- `unsupported.test` -- **C**: Tests DuckDB-specific unsupported types (UNION, ENUM, fixed arrays).
- `variant.test` -- **C**: VARIANT type, `variant_typeof()`, `foreach`/`endloop`.
**Estimated adaptation effort**: Moderate for struct/list, trivial for floats/timestamps
**Priority**: High for basic types (floats, timestamps, null_byte), medium for complex types

### `view/` -- 8 files
**Feature area**: CREATE VIEW, rename view, view schemas, view-table conflicts
**Category**: B (Needs Adaptation) -- 5 files, C (Not Portable) -- 3 files
**DuckDB constructs**: `duckdb_views()`, `USE`, `duckdb_schemas()`, `FROM <table>` without SELECT
**Specific files**:
- `ducklake_view.test` -- **B**: CREATE/DROP VIEW, transaction-local views. Uses `duckdb_views()` (skip). Core view queries are portable.
- `ducklake_view_schema.test` -- **B**: Views across schemas. Uses `duckdb_schemas()`, `duckdb_views()`.
- `ducklake_view_table_conflict.test` -- **B**: View vs table naming conflicts. Standard SQL.
- `view_alias_binding.test` -- **B**: View with column aliases. Uses `USE`.
- `view_missing_table_similar_entry.test` -- **B**: Error messages for missing tables referenced by views.
- `ducklake_rename_view.test` -- **C**: View rename uses DuckDB-specific catalog rename.
- `ducklake_rename_view_incorect.test` -- **C**: Error handling for incorrect renames.
- `ducklake_view_info_columns.test` -- **C**: View metadata queries.
**Estimated adaptation effort**: Moderate
**Priority**: Medium

### `catalog/` -- 4 files
**Feature area**: Schema management, table lifecycle, quoted identifiers, macros
**Category**: B (Needs Adaptation) -- 3 files, C (Not Portable) -- 1 file
**DuckDB constructs**: `duckdb_schemas()`, `duckdb_tables()`, `foreach`/`endloop`, `USE`
**Specific files**:
- `schema.test` -- **B**: CREATE/DROP SCHEMA, multi-schema operations. Uses `duckdb_schemas()` (replace). Core DDL/read is portable.
- `drop_table.test` -- **B**: DROP TABLE with transactions. Uses `duckdb_tables()`. Core logic is standard.
- `quoted_identifiers.test` -- **B**: Tests quoted column/table names. Standard SQL.
- `create_then_drop_macro.test` -- **C**: DuckLake macro catalog management.
**Estimated adaptation effort**: Moderate
**Priority**: High for schema/drop_table/quoted_identifiers

### `constraints/` -- 3 files
**Feature area**: NOT NULL constraints
**Category**: B (Needs Adaptation) -- 2 files, C (Not Portable) -- 1 file
**DuckDB constructs**: `DESCRIBE`, `ALTER ... SET NOT NULL` / `DROP NOT NULL`
**Specific files**:
- `not_null.test` -- **B**: NOT NULL constraint creation, violation, ALTER. DESCRIBE output may differ. Core constraint logic is portable.
- `not_null_drop_column.test` -- **B**: NOT NULL with column drops.
- `unsupported.test` -- **C**: Tests DuckDB-specific unsupported constraints (CHECK, UNIQUE, etc.).
**Estimated adaptation effort**: Moderate (DESCRIBE format may differ)
**Priority**: Medium

### `default/` -- 4 files
**Feature area**: DEFAULT values for columns
**Category**: B (Needs Adaptation) -- 3 files, C (Not Portable) -- 1 file
**DuckDB constructs**: `CALL ducklake_flush_inlined_data`, `foreach`/`endloop`, metadata tables
**Specific files**:
- `default_values.test` -- **B**: Standard DEFAULT value handling. Portable reads.
- `default_expressions.test` -- **B**: DEFAULT with expressions. Mostly standard SQL.
- `struct_field_default.test` -- **B**: DEFAULT values for struct fields.
- `add_column_with_default.test` -- **C**: Uses `ducklake_flush_inlined_data`, `foreach`/`endloop`, metadata tables.
**Estimated adaptation effort**: Trivial for basic defaults
**Priority**: Medium

### `transaction/` -- 12 files
**Feature area**: Transaction support (begin/commit/rollback), conflicts, multi-connection
**Category**: B (Needs Adaptation) -- 3 files, C (Not Portable) -- 9 files
**DuckDB constructs**: `USE`, `glob()`, multi-connection (`con1`/`con2`), `ducklake_flush_inlined_data`, metadata tables
**Specific files**:
- `basic_transaction.test` -- **B**: BEGIN/COMMIT/ROLLBACK with reads. Uses `glob()` (skip file checks) and `USE`. Core transaction reads are portable.
- `transaction_insert_update_delete.test` -- **B**: INSERT/UPDATE/DELETE in transactions. Uses `USE`.
- `transaction_schema.test` -- **B**: Schema DDL in transactions.
- `transaction_conflicts.test` -- **C**: Multi-connection conflict testing (con1/con2).
- `transaction_conflicts_delete.test` -- **C**: Multi-connection.
- `transaction_conflicts_view.test` -- **C**: Multi-connection.
- `concurrent_table_creation.test` -- **C**: Multi-connection.
- `transaction_conflict_cleanup.test` -- **C**: Uses glob() for file verification.
- `transaction_conflict_inlining.test` -- **C**: Data inlining + conflicts.
- `transaction_inlining.test` -- **C**: Data inlining.
- `create_conflict.test` -- **C**: Multi-connection conflicts.
- `multiple_column_changes.test` -- **C**: Uses metadata tables.
**Estimated adaptation effort**: Moderate for basic transaction tests
**Priority**: Medium

### `stats/` -- 11 files
**Feature area**: Statistics, filter pushdown, COUNT(*) optimization, TopN file pruning
**Category**: B (Needs Adaptation) -- 3 files, C (Not Portable) -- 8 files
**DuckDB constructs**: `stats()`, `EXPLAIN ANALYZE` with regex, `CALL ducklake_flush_inlined_data`, metadata tables, `SET VARIABLE`, time travel, `variant_shredded_stats`
**Specific files**:
- `count_star_optimization_basic.test` -- **B**: COUNT(*) after insert/delete/truncate. Core queries are standard SQL. Transaction sections are portable.
- `filter_pushdown.test` -- **B**: Filter queries on integer/date/decimal/varchar. Core SELECT/COUNT queries are highly portable. Only EXPLAIN ANALYZE regex parts need removal.
- `cardinality.test` -- **B**: Estimated row count. May need adaptation for how cardinality is checked.
- `global_stats.test` -- **C**: Uses `stats()` function extensively.
- `global_stats_transactions.test` -- **C**: stats() + transactions.
- `filter_stress.test` -- **C**: Uses ducklake_flush_inlined_data, EXPLAIN ANALYZE.
- `count_star_optimization_file_operations.test` -- **C**: Uses CALL functions extensively.
- `count_star_optimization_inlined.test` -- **C**: Inlining-specific.
- `count_star_optimization_time_travel.test` -- **C**: Time travel.
- `topn_file_pruning.test` -- **C**: EXPLAIN ANALYZE regex, stats().
- `variant_shredded_stats.test` -- **C**: Variant type stats.
**Estimated adaptation effort**: Moderate (remove EXPLAIN ANALYZE, keep filter queries)
**Priority**: High for filter_pushdown, count_star_optimization_basic

### `partitioning/` -- 10 files (+ 1 slow test)
**Feature area**: Hive-style partitioning
**Category**: C (Not Portable) -- all 10 files
**DuckDB constructs**: `ALTER TABLE SET PARTITIONED BY`, `CALL ducklake_flush_inlined_data`, `EXPLAIN ANALYZE`, `regexp_extract()`, metadata tables, `USE`, `DETACH`/re-attach, `glob()`
**Notes**: Partitioning is entirely a write-side DuckLake feature. The read queries after partition setup are standard but depend on partition structure being set up by DuckDB.
**Estimated adaptation effort**: N/A
**Priority**: Low (write-side feature)

### `data_inlining/` -- 28 files
**Feature area**: DuckLake data inlining (storing small data inline in metadata)
**Category**: C (Not Portable) -- all 28 files
**DuckDB constructs**: `DATA_INLINING_ROW_LIMIT`, `CALL ducklake_flush_inlined_data`, `GLOB()`, metadata tables, `rowid`, `snapshot_id`, `filename`, `file_row_number`, `file_index` virtual columns
**Notes**: Data inlining is an internal DuckLake optimization. All tests are deeply tied to DuckDB internals.
**Estimated adaptation effort**: N/A
**Priority**: None

### `deletion_inlining/` -- 15 files (+ 1 slow test)
**Feature area**: DuckLake deletion inlining (inlining delete markers)
**Category**: C (Not Portable) -- all 15 files
**DuckDB constructs**: `ducklake_flush_inlined_data`, metadata tables, CALL functions, `glob()`, multi-connection
**Estimated adaptation effort**: N/A
**Priority**: None

### `compaction/` -- 27 files (+ 1 slow test)
**Feature area**: File compaction, snapshot expiration, file merging
**Category**: C (Not Portable) -- all 27 files
**DuckDB constructs**: `ducklake_merge_adjacent_files`, `ducklake_expire_snapshots`, `ducklake_cleanup_old_files`, `ducklake_rewrite_data_files`, metadata tables, multi-connection, `foreach`/`endloop`
**Notes**: All compaction/maintenance operations are DuckDB write-side operations.
**Estimated adaptation effort**: N/A
**Priority**: None

### `sorted_table/` -- 26 files
**Feature area**: SET SORTED BY metadata, merge-on-sorted compaction
**Category**: C (Not Portable) -- all 26 files
**DuckDB constructs**: `ALTER TABLE SET SORTED BY`, `ducklake_merge_adjacent_files`, `ducklake_flush_inlined_data`, metadata tables, `foreach`/`endloop`
**Estimated adaptation effort**: N/A
**Priority**: None

### `add_files/` -- 31 files
**Feature area**: Adding external Parquet files to DuckLake catalog
**Category**: C (Not Portable) -- all 31 files
**DuckDB constructs**: `ducklake_add_data_files()`, `COPY ... TO`, metadata tables, `foreach`/`endloop`, type checking internals
**Estimated adaptation effort**: N/A
**Priority**: None

### `time_travel/` -- 2 files
**Feature area**: Time travel queries
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `AT (VERSION => ...)`, `AT (TIMESTAMP => ...)`, `require icu`, `SET VARIABLE`, `getvariable()`, `DROP SCHEMA CASCADE`
**Notes**: Time travel is not supported by DataFusion. The read queries within are standard SQL but require time travel context.
**Estimated adaptation effort**: N/A
**Priority**: None (DataFusion does not support time travel)

### `table_changes/` -- 8 files
**Feature area**: CDC / table change tracking
**Category**: C (Not Portable) -- all 8 files
**DuckDB constructs**: `table_changes()` function, `SET VARIABLE`, `getvariable()`, `ducklake_flush_inlined_data`
**Estimated adaptation effort**: N/A
**Priority**: None

### `comments/` -- 5 files
**Feature area**: COMMENT ON TABLE/VIEW/COLUMN
**Category**: C (Not Portable) -- all 5 files
**DuckDB constructs**: `COMMENT ON`, `duckdb_tables()`, `duckdb_views()`, metadata tables
**Notes**: DataFusion does not support COMMENT ON syntax.
**Estimated adaptation effort**: N/A
**Priority**: None

### `macros/` -- 10 files
**Feature area**: DuckLake macro catalog (scalar + table macros)
**Category**: C (Not Portable) -- all 10 files
**DuckDB constructs**: `CREATE MACRO`, `DROP MACRO`, metadata tables, multi-connection, `USE`, `snapshots()`
**Notes**: DuckLake macro catalog is DuckDB-specific.
**Estimated adaptation effort**: N/A
**Priority**: None

### `functions/` -- 2 files
**Feature area**: DuckLake catalog functions (snapshots, table_info)
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `ducklake_snapshots()`, `ducklake_table_info()`, `SET VARIABLE`, `getvariable()`, `regexp_extract()`
**Estimated adaptation effort**: N/A
**Priority**: None

### `metadata/` -- 5 files
**Feature area**: DuckLake metadata and settings
**Category**: C (Not Portable) -- all 5 files
**DuckDB constructs**: `duckdb_tables()`, `ducklake_settings()`, `USE`, DuckDB-specific metadata queries
**Estimated adaptation effort**: N/A
**Priority**: None

### `settings/` -- 5 files
**Feature area**: DuckLake Parquet settings (compression, row group size, per-table settings)
**Category**: C (Not Portable) -- all 5 files
**DuckDB constructs**: `CALL ducklake.set_option()`, `parquet_metadata()`, `DETACH`/re-attach, metadata tables
**Estimated adaptation effort**: N/A
**Priority**: None

### `merge/` -- 5 files (+ 1 slow test)
**Feature area**: MERGE INTO statement
**Category**: C (Not Portable) -- all 5 files
**DuckDB constructs**: `MERGE INTO`, `USE`, `require icu`, `uuidv7()`, `ALTER TABLE SET PARTITIONED BY`, `ducklake_flush_inlined_data`
**Notes**: DataFusion does not support MERGE INTO.
**Estimated adaptation effort**: N/A
**Priority**: None

### `rewrite_data_files/` -- 10 files
**Feature area**: Data file rewriting/compaction
**Category**: C (Not Portable) -- all 10 files
**DuckDB constructs**: `ducklake_rewrite_data_files()`, `ducklake_merge_adjacent_files()`, metadata tables, multi-connection, `SET VARIABLE`
**Estimated adaptation effort**: N/A
**Priority**: None

### `concurrent/` -- 4 files (+ 1 slow test)
**Feature area**: Concurrent operations
**Category**: C (Not Portable) -- all 4 files
**DuckDB constructs**: Multi-connection (`con1`/`con2`), `USE`, `ducklake_flush_inlined_data`
**Estimated adaptation effort**: N/A
**Priority**: None

### `rowid/` -- 2 files
**Feature area**: DuckLake row ID tracking
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `rowid` virtual column, transaction-local row IDs, row-id-based UPDATE/DELETE
**Notes**: DataFusion does not expose DuckLake rowids.
**Estimated adaptation effort**: N/A
**Priority**: None

### `virtualcolumns/` -- 2 files
**Feature area**: DuckLake virtual columns (filename, file_row_number, snapshot_id)
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `filename`, `file_row_number`, `file_index`, `snapshot_id` virtual columns, `CALL ducklake_flush_inlined_data`
**Estimated adaptation effort**: N/A
**Priority**: None

### `snapshot_info/` -- 2 files
**Feature area**: Snapshot information functions
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `ducklake_current_snapshot()`, `current_snapshot()`, multi-connection (`con1`/`con2`/`con3`)
**Estimated adaptation effort**: N/A
**Priority**: None

### `encryption/` -- 2 files
**Feature area**: DuckLake encryption support
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `ENCRYPTED` attach option, `require httpfs`, file-level encryption verification
**Estimated adaptation effort**: N/A
**Priority**: None

### `geo/` -- 5 files
**Feature area**: Geometry type support
**Category**: C (Not Portable) -- all 5 files
**DuckDB constructs**: `require spatial`, `GEOMETRY` type, `ST_POINT()`, spatial functions, metadata tables
**Estimated adaptation effort**: N/A
**Priority**: None

### `cleanup/` -- 2 files
**Feature area**: Old file cleanup
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `ducklake_cleanup_old_files()`, `ducklake_rewrite_data_files()`, `ducklake_merge_adjacent_files()`, `ducklake_expire_snapshots()`
**Estimated adaptation effort**: N/A
**Priority**: None

### `remove_orphans/` -- 2 files
**Feature area**: Orphaned file removal
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `ducklake_remove_orphaned_files()`, `ducklake_flush_inlined_data`, `COPY TO`
**Estimated adaptation effort**: N/A
**Priority**: None

### `list_files/` -- 1 file
**Feature area**: List DuckLake data files
**Category**: C (Not Portable)
**DuckDB constructs**: `ducklake_list_files()`, metadata tables
**Estimated adaptation effort**: N/A
**Priority**: None

### `initialize/` -- 2 files
**Feature area**: DuckLake catalog initialization
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `ATTACH` with various options, read-only mode testing
**Estimated adaptation effort**: N/A
**Priority**: None

### `migration/` -- 4 files
**Feature area**: DuckLake catalog format migration
**Category**: C (Not Portable) -- all 4 files
**DuckDB constructs**: Internal migration logic, metadata tables, `DETACH`/re-attach
**Estimated adaptation effort**: N/A
**Priority**: None

### `schema_evolution/` -- 1 file
**Feature area**: Field ID tracking for schema evolution
**Category**: C (Not Portable)
**DuckDB constructs**: `ducklake_flush_inlined_data`, metadata tables (`ducklake_column`, `ducklake_column_mapping`)
**Estimated adaptation effort**: N/A
**Priority**: None

### `checkpoint/` -- 4 files
**Feature area**: Checkpoint behavior
**Category**: C (Not Portable) -- all 4 files
**DuckDB constructs**: `CHECKPOINT`, metadata tables, multi-connection, `CALL ducklake_flush_inlined_data`
**Estimated adaptation effort**: N/A
**Priority**: None

### `attach/` -- 2 files
**Feature area**: ATTACH/DETACH behaviors
**Category**: C (Not Portable) -- 2 files
**DuckDB constructs**: `ATTACH`/`DETACH` with various options, `ducklake_flush_inlined_data`
**Estimated adaptation effort**: N/A
**Priority**: None

### `audit/` -- 1 file
**Feature area**: Commit audit trail (author, message)
**Category**: C (Not Portable)
**DuckDB constructs**: `ducklake.set_commit_message()`, `snapshots()`, `ducklake.set_option('require_commit_message')`
**Estimated adaptation effort**: N/A
**Priority**: None

### `autoloading/` -- 1 file
**Feature area**: DuckDB extension autoloading
**Category**: C (Not Portable)
**DuckDB constructs**: DuckDB-specific extension loading
**Estimated adaptation effort**: N/A
**Priority**: None

### `cloud/` -- 1 file
**Feature area**: Cloud/S3 testing
**Category**: C (Not Portable)
**DuckDB constructs**: S3 configuration, cloud-specific test environment
**Estimated adaptation effort**: N/A
**Priority**: None

### `secrets/` -- 1 file
**Feature area**: DuckDB secrets for credential management
**Category**: C (Not Portable)
**DuckDB constructs**: `CREATE SECRET`, DuckDB secret management
**Estimated adaptation effort**: N/A
**Priority**: None

### `issues/` -- 1 file
**Feature area**: Bug fix regression tests
**Category**: B (Needs Adaptation) -- 1 file
**DuckDB constructs**: `USE`
**Specific files**:
- `late_materialization.test` -- **B**: Tests late materialization with filter + ORDER BY + LIMIT. Uses `USE ducklake`. Core query is standard SQL: `SELECT * FROM my_table WHERE id > 3 ORDER BY value DESC LIMIT 1`.
**Estimated adaptation effort**: Trivial
**Priority**: Medium

### `ducklake_basic.test` -- 1 file (top-level)
**Feature area**: Basic end-to-end DuckLake test
**Category**: B (Needs Adaptation)
**DuckDB constructs**: `ATTACH`/`DETACH`/`USE`, `SHOW TABLES`, `SHOW ALL TABLES`, `CREATE TABLE ... AS SELECT`
**Notes**: Core INSERT/SELECT queries are standard. SHOW TABLES and ATTACH are DuckDB-specific but already handled by preprocessor.
**Estimated adaptation effort**: Moderate
**Priority**: Medium

### `clickbench/` -- 0 test files (1 slow test)
**Feature area**: ClickBench benchmark
**Category**: C (Not Portable) -- uses `USE`, likely complex setup
**Priority**: None

### `tpch/` -- 0 test files (1 slow test)
**Feature area**: TPC-H benchmark
**Category**: C (Not Portable) -- uses `PRAGMA`, `foreach`/`endloop`, `USE`
**Priority**: None

## 4. Recommended Porting Order

### Phase 1: Core Read Path Verification (Highest Priority)
These tests exercise basic DDL + DML followed by read verification, which is exactly what the DataFusion extension needs to validate.

1. **`insert/insert_column_list.test`** -- Trivial adaptation. Tests INSERT with column ordering and DEFAULT, then reads back.
2. **`insert/insert_into_self.test`** -- Trivial. Tests self-referential INSERT, then reads back.
3. **`delete/basic_delete.test`** -- Trivial. Tests DELETE + read verification. Remove time travel query.
4. **`delete/empty_delete.test`** -- Trivial. Tests DELETE WHERE false.
5. **`delete/delete_same_transaction.test`** -- Trivial. Transaction-local deletes.
6. **`update/basic_update.test`** -- Trivial. Tests UPDATE + read verification. Remove time travel queries.
7. **`update/test_update_expression.test`** -- Trivial. UPDATE with CASE expression.
8. **`update/update_not_null.test`** -- Trivial. UPDATE with constraints.
9. **`ducklake_basic.test`** -- Moderate. Basic end-to-end test.

### Phase 2: DDL Operations
10. **`alter/add_column.test`** -- Trivial. ALTER TABLE ADD COLUMN + reads.
11. **`alter/drop_column.test`** -- Trivial. ALTER TABLE DROP COLUMN.
12. **`alter/rename_column.test`** -- Trivial. ALTER TABLE RENAME COLUMN.
13. **`alter/rename_table.test`** -- Moderate. RENAME TABLE in transactions.
14. **`alter/promote_type.test`** -- Moderate. Type widening.
15. **`catalog/schema.test`** -- Moderate. Multi-schema CREATE/DROP.
16. **`catalog/drop_table.test`** -- Moderate. DROP TABLE with transactions.
17. **`catalog/quoted_identifiers.test`** -- Trivial. Quoted names.

### Phase 3: Type Coverage
18. **`types/floats.test`** -- Moderate. NaN/Inf filter behavior. Needs `foreach` unrolling.
19. **`types/timestamp.test`** -- Trivial. Timestamp infinity comparisons.
20. **`types/null_byte.test`** -- Trivial. Null bytes in strings.
21. **`types/struct.test`** -- Moderate. Struct storage/retrieval (if complex types supported).
22. **`types/list.test`** -- Moderate. List storage/retrieval (if complex types supported).

### Phase 4: Query Features
23. **`stats/filter_pushdown.test`** -- Moderate. Core filter queries (remove EXPLAIN ANALYZE). Tests integer/date/decimal/varchar filtering.
24. **`stats/count_star_optimization_basic.test`** -- Trivial. COUNT(*) correctness.
25. **`view/ducklake_view.test`** -- Moderate. CREATE/DROP VIEW.
26. **`constraints/not_null.test`** -- Moderate. NOT NULL constraints.
27. **`default/default_values.test`** -- Trivial. DEFAULT column values.
28. **`issues/late_materialization.test`** -- Trivial. Filter + ORDER BY + LIMIT.

### Phase 5: Advanced Operations
29. **`alter/mixed_alter.test`** / `mixed_alter2.test` -- Moderate.
30. **`alter/rename_table_dbt_workload.test`** -- Moderate.
31. **`transaction/basic_transaction.test`** -- Moderate.
32. **`delete/truncate_table.test`** -- Moderate.
33. **`update/update_join_duplicates.test`** -- Moderate.

## 5. Common Adaptation Patterns

### Pattern 1: Remove DuckDB Directives (Already Handled)
The existing preprocessor in `sqllogictest_runner.rs` already strips:
- `require ducklake` / `require parquet`
- `test-env` lines
- `# name:` / `# description:` / `# group:` comments
- `ATTACH` / `DETACH` statements
- `EXPLAIN` statements

### Pattern 2: Replace `FROM <table>` with `SELECT * FROM <table>`
DuckDB supports `FROM table` as shorthand for `SELECT * FROM table`. DataFusion requires the explicit `SELECT * FROM` form.
```
-- DuckDB:     FROM ducklake.test
-- DataFusion: SELECT * FROM ducklake.test
```
**Frequency**: Very common across nearly all test files.

### Pattern 3: Replace `INSERT INTO ... FROM` with `INSERT INTO ... SELECT * FROM`
```
-- DuckDB:     INSERT INTO ducklake.test FROM range(1000)
-- DataFusion: INSERT INTO ducklake.test SELECT * FROM generate_series(0, 999)
```
**Frequency**: Very common.

### Pattern 4: Replace `range()` with DataFusion Equivalent
```
-- DuckDB:     SELECT i FROM range(1000) t(i)
-- DataFusion: (handled by DuckDB write side in hybrid mode)
```
**Note**: In the hybrid test runner, INSERT/CREATE TABLE AS are executed by DuckDB, so `range()` works. Only SELECT queries sent to DataFusion need adaptation.

### Pattern 5: Remove `USE <catalog>` and Use Fully-Qualified Names
```
-- DuckDB:     USE ducklake; SELECT * FROM test;
-- DataFusion: SELECT * FROM ducklake.main.test;
```
**Note**: The hybrid runner already rewrites table references from `ducklake.table` to `ducklake.main.table`.
**Frequency**: ~110 files use `USE`.

### Pattern 6: Remove Verification Queries That Use DuckDB Functions
Skip or remove queries that use:
- `stats()` -- DuckDB internal statistics
- `glob()` -- file system listing
- `duckdb_tables()` / `duckdb_schemas()` / `duckdb_views()` -- DuckDB catalog functions
- `parquet_metadata()` -- Parquet file inspection
- `ducklake_snapshots()` / `ducklake_table_info()` -- DuckLake catalog functions
- `__ducklake_metadata_*` table queries -- internal metadata

**Frequency**: ~100+ files contain at least one such verification query.

### Pattern 7: Remove Time Travel Queries
```
-- Remove: SELECT * FROM ducklake.test AT (VERSION => 2)
```
**Frequency**: ~28 files.

### Pattern 8: Remove CALL Statements
```
-- Remove: CALL ducklake_flush_inlined_data('ducklake')
-- Remove: CALL ducklake_merge_adjacent_files('ducklake')
-- Remove: CALL ducklake.set_option(...)
```
**Frequency**: ~149 files.

### Pattern 9: Unroll `foreach`/`endloop` Loops
The DuckDB sqllogictest runner supports `foreach` / `endloop` for parameterized tests. These must be manually unrolled for the standard sqllogictest crate.
```
-- DuckDB:
foreach type FLOAT DOUBLE
CREATE OR REPLACE TABLE ducklake.test(f ${type});
endloop

-- DataFusion: Write separate test blocks for each type
```
**Frequency**: ~41 files.

### Pattern 10: Skip Multi-Connection Tests
Tests using `con1`, `con2`, etc. require concurrent connection support not available in the hybrid runner.
**Frequency**: ~15 files.

### Pattern 11: Handle `DESCRIBE` Output Format Differences
DuckDB's DESCRIBE returns 6 columns. DataFusion's DESCRIBE may return a different format.
**Frequency**: ~5 files.

### Pattern 12: Adjust `SHOW TABLES` Syntax
```
-- DuckDB:     SHOW TABLES
-- DataFusion: SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'
```
**Frequency**: ~5 files.
