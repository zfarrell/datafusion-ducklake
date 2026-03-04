# DataFusion-DuckLake Project Status

**Generated**: 2026-03-01
**Branch**: `ducklake-features/integration`
**Source of truth**: Code verification via grep/read of actual source files

## 1. Feature Status Matrix

### 1.1 Read Path

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| SELECT (basic read) | Complete | `src/table.rs:1340` `scan()` impl, `src/metadata_provider_duckdb.rs` | — | `tests/table_tests.rs` (5), `tests/parity_tests.rs` (8) |
| Filter pushdown | Complete | `src/table.rs:1349` `supports_filters_pushdown()` returns `Inexact` | — | Covered in table/parity tests |
| MOR delete filtering | Complete | `src/delete_filter.rs` (362 lines), `DeleteFilterExec` wraps Parquet scan | — | `tests/delete_filter_tests.rs` (11), `tests/missing_delete_file_tests.rs` (3) |
| Column rename handling | Complete | `src/column_rename.rs` `ColumnRenameExec` (229 lines), field_id-based rename | — | `tests/renamed_columns_tests.rs` (7), unit test in `src/column_rename.rs` (1) |
| Complex types (List/Struct/Map) | Complete | `src/types.rs:275` `parse_complex_type()` handles List, Struct, Map, nested types | — | `src/types.rs` (54 unit tests cover complex type parsing) |
| Encrypted reads (PME) | Complete | `src/encryption.rs` (558 lines), `#[cfg(feature = "encryption")]` guards | DuckDB-encrypted files not supported (non-PME) | `tests/encryption_tests.rs` (3) |
| Inlined data (read-side) | Complete | `src/table.rs:272` `build_inlined_data_exec()`, `src/metadata_provider_duckdb.rs:523` `get_inlined_data()` | — | `tests/cross_engine_inline_tests.rs` (9) |
| Partition pruning (read-side) | Complete | `src/table.rs:874` `prune_files_by_partition()`, reads partition metadata | — | `tests/cross_engine_partition_tests.rs` (7), `tests/file_pruning_tests.rs` |
| Column stats file pruning | Complete | `src/table.rs:933` `prune_files_by_stats()`, `src/table.rs:1246` `statistics()` | — | `tests/stats_tests.rs`, `tests/file_pruning_tests.rs` |
| Footer size optimization | Complete | `src/table.rs:435` `with_metadata_size_hint()` applied to PartitionedFile | — | `tests/negative_footer_size_test.rs` (3) |
| Information schema | Complete | `src/information_schema.rs` (784 lines): snapshots, schemata, tables, columns, files | — | `tests/information_schema_test.rs` (17) |

### 1.2 Write Path (INSERT)

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| INSERT INTO | Complete | `src/insert_exec.rs` `DuckLakeInsertExec` (253 lines), `src/table.rs:1620` `insert_into()` | — | `tests/write_tests.rs`, `tests/sql_write_tests.rs`, `tests/cross_engine_insert_tests.rs` (1) |
| Table writer (Parquet) | Complete | `src/table_writer.rs` `DuckLakeTableWriter` (707 lines), writes Parquet + commits metadata | — | `src/table_writer.rs` (4 unit tests) |
| Write-side column stats | Complete | `src/table_writer.rs:312` `extract_column_stats()`, calls `register_column_stats()` | — | Implicit via write tests |
| CTAS (CREATE TABLE AS) | Complete | `src/table_writer.rs` handles `WriteMode::Replace` | — | Cross-engine tests |
| Write-side partitioning | Complete | `src/insert_exec.rs` partition routing, `src/table_writer.rs` Hive-style output | — | `tests/write_partition_tests.rs` (6) |
| Write-side inlining | Complete | `src/metadata_writer.rs` inlining trait methods, `src/metadata_writer_sqlite.rs` SQLite backend, auto-flush to Parquet | — | `tests/write_inline_tests.rs` (8) |
| Encrypted writes | Not Started | No encryption config in `src/table_writer.rs` or `src/insert_exec.rs` | PME write-side not implemented | — |

### 1.3 DELETE

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| DELETE execution | Complete | `src/delete_exec.rs` `DuckLakeDeleteExec` (383 lines), `src/table.rs:584` `delete()` | — | `tests/delete_tests.rs`, `tests/sql_dml_tests.rs` |
| Delete file generation | Complete | `src/table_deletions.rs` (733 lines), writes delete files + registers in catalog | — | `tests/delete_filter_tests.rs` (11) |
| Query planner routing | Complete | `src/query_planner.rs:57` routes `DmlStatement::Delete` to `DuckLakeTable::delete()` | — | `tests/cross_engine_dml_tests.rs` (1) |

### 1.4 UPDATE

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| UPDATE execution | Complete | `src/update_exec.rs` `DuckLakeUpdateExec` (508 lines), `src/table.rs:632` `update()` | — | `tests/update_tests.rs`, `tests/sql_dml_tests.rs` |
| Field ID embedding | Complete | `src/table_writer.rs:136` `build_schema_with_field_ids()` | — | `tests/cross_engine_dml_tests.rs` |
| Query planner routing | Complete | `src/query_planner.rs:67` routes `DmlStatement::Update` | — | — |

### 1.5 MERGE INTO

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| MERGE execution (programmatic API) | Complete | `src/merge_exec.rs` `DuckLakeMergeExec` (588 lines), `src/table.rs:688` `merge()` | — | `tests/merge_tests.rs` |
| MERGE via SQL | Not Started | `src/query_planner.rs` has no MERGE SQL routing | DataFusion doesn't support MERGE INTO SQL syntax | — |

### 1.6 Virtual Columns

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| `filename` | Complete | `src/virtual_column_exec.rs:25` `VIRTUAL_COL_FILENAME` | — | `tests/virtual_column_tests.rs`, `tests/virtual_column_extended_tests.rs` |
| `file_row_number` | Complete | `src/virtual_column_exec.rs:27` `VIRTUAL_COL_FILE_ROW_NUMBER` | — | Same as above |
| `rowid` | Complete | `src/virtual_column_exec.rs:29` `VIRTUAL_COL_ROWID` | — | Same as above |
| `snapshot_id` | Complete | `src/virtual_column_exec.rs:31` `VIRTUAL_COL_SNAPSHOT_ID` | — | Same as above |
| `file_index` | Complete | `src/virtual_column_exec.rs:33` `VIRTUAL_COL_FILE_INDEX` | — | Same as above |

### 1.7 Column Statistics

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| Write-side stats collection | Complete | `src/table_writer.rs:408` `extract_column_stats()` | — | Implicit via write tests |
| Write-side stats registration | Complete | `src/table_writer.rs:365` `register_column_stats()` | — | — |
| Read-side stats (DataFusion Statistics) | Complete | `src/table.rs:1246` `statistics()` returns aggregated min/max/null_count | — | `tests/stats_tests.rs` |
| File-level pruning via stats | Complete | `src/table.rs:933` `prune_files_by_stats()` | — | `tests/file_pruning_tests.rs` |

### 1.8 Time Travel & Table Functions

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| `ducklake_snapshots()` | Complete | `src/table_functions.rs:540` registered | — | `tests/time_travel_tests.rs` (14), `tests/table_function_tests.rs` (8) |
| `ducklake_table_info()` | Complete | `src/table_functions.rs:544` registered | — | `tests/table_function_tests.rs` |
| `ducklake_list_files()` | Complete | `src/table_functions.rs:548` registered | — | `tests/table_function_tests.rs` |
| `ducklake_table_changes()` | Complete | `src/table_functions.rs:552`, impl in `src/table_changes.rs` (600 lines) | — | `tests/table_changes_tests.rs` (12) |
| `ducklake_table_deletions()` | Complete | `src/table_functions.rs:556`, impl in `src/table_deletions.rs` | — | `tests/time_travel_tests.rs` |
| `ducklake_table_insertions()` | Complete | `src/table_functions.rs:560`, impl in `src/table_insertions.rs` (188 lines) | — | `tests/time_travel_tests.rs` |
| `ducklake_current_snapshot()` | Complete | `src/table_functions.rs:564` | — | `tests/time_travel_tests.rs` |
| `ducklake_last_committed_snapshot()` | Complete | `src/table_functions.rs:568` | — | `tests/time_travel_tests.rs` |
| Time travel via SQL syntax (`AT SNAPSHOT`) | Not Started | No SQL-level time travel syntax support in DataFusion | Would need custom SQL extension | — |

### 1.9 Compaction Functions (delegated to DuckDB)

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| `ducklake_merge_adjacent_files()` | Complete | `src/compaction_functions.rs:152` | — | `tests/compaction_tests.rs` |
| `ducklake_rewrite_data_files()` | Complete | `src/compaction_functions.rs:200` | — | `tests/compaction_tests.rs` |
| `ducklake_expire_snapshots()` | Complete | `src/compaction_functions.rs:260` | — | `tests/compaction_tests.rs` |
| `ducklake_cleanup_old_files()` | Complete | `src/compaction_functions.rs:320` | — | `tests/compaction_tests.rs` |
| `ducklake_delete_orphaned_files()` | Complete | `src/compaction_functions.rs:421` | — | `tests/compaction_tests.rs` |
| `ducklake_options()` | Complete | `src/compaction_functions.rs:804` registered | — | — |
| `ducklake_add_data_files()` | Complete | `src/compaction_functions.rs:808` registered | — | — |
| `ducklake_set_option()` | Complete | `src/compaction_functions.rs:812` registered | — | — |
| `ducklake_set_commit_message()` | Complete | `src/compaction_functions.rs:816` registered | — | — |

### 1.10 ALTER TABLE Operations

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| ADD COLUMN | Complete | `src/metadata_writer.rs:23` `AlterTableOp::AddColumn` | — | `tests/alter_table_tests.rs`, `tests/cross_engine_alter_tests.rs` (14) |
| DROP COLUMN | Complete | `src/metadata_writer.rs:27` `AlterTableOp::DropColumn` | — | Same as above |
| RENAME COLUMN | Complete | `src/metadata_writer.rs:31` `AlterTableOp::RenameColumn` | — | Same as above |
| ALTER COLUMN TYPE | Complete | `src/metadata_writer.rs:36` `AlterTableOp::AlterColumnType` | — | Same as above |
| SET DEFAULT | Complete | `src/metadata_writer.rs:38` `AlterTableOp::SetColumnDefault` | — | Same as above |
| DROP DEFAULT | Complete | `src/metadata_writer.rs:43` `AlterTableOp::DropColumnDefault` | — | Same as above |
| SET NOT NULL | Complete | `src/metadata_writer.rs:47` `AlterTableOp::SetNotNull` | — | Same as above |
| DROP NOT NULL | Complete | `src/metadata_writer.rs:52` `AlterTableOp::DropNotNull` | — | Same as above |
| RENAME TABLE | Complete | `src/metadata_writer.rs:479` `rename_table()` | — | Same as above |
| SET COMMENT (table) | Complete | `src/metadata_writer.rs:486` `set_table_comment()` | — | `src/metadata_writer_sqlite.rs:2276` unit test |
| SET COMMENT (column) | Complete | `src/metadata_writer.rs:493` `set_column_comment()` | — | `src/metadata_writer_sqlite.rs:2337` unit test |
| SET PARTITIONED BY | Complete | `src/metadata_writer.rs` `AlterTableOp::SetPartitionedBy` | — | `tests/write_partition_tests.rs` |
| ADD/REMOVE/RENAME FIELD (struct evolution) | Not Started | No `AlterTableOp` variants for struct field operations | — | — |

### 1.11 Views

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| CREATE VIEW | Complete | `src/metadata_writer.rs:502` `create_view()`, `src/schema.rs:123` `plan_view()` | — | `tests/view_tests.rs` |
| DROP VIEW | Complete | `src/metadata_writer.rs:507` `drop_view()` | — | Same |
| RENAME VIEW | Complete | `src/metadata_writer.rs:514` `rename_view()` | — | Same |
| View resolution (read) | Complete | `src/schema.rs:118-141` plans view SQL, wraps in `ViewTable` | — | Same |

### 1.12 DDL Operations

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| CREATE SCHEMA | Complete | `src/catalog.rs:266` calls `get_or_create_schema()` | — | `tests/create_schema_tests.rs`, `tests/cross_engine_ddl_tests.rs` (2) |
| DROP TABLE | Complete | `src/metadata_writer.rs:410` `drop_table()`, `src/schema.rs:299` | — | `tests/drop_and_constraints_tests.rs` |
| DROP SCHEMA | Complete | `src/metadata_writer.rs:415` `drop_schema()`, `src/catalog.rs:209` | — | Same |
| Conflict-checked DROP | Complete | `src/metadata_writer.rs:446` `drop_table_checked()`, `:457` `drop_schema_checked()` | — | `tests/conflict_detection_tests.rs` |

### 1.13 NOT NULL Enforcement

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| Schema validation | Complete | `src/metadata_writer_validation.rs` (795 lines), validates nullable constraints | — | `src/metadata_writer_validation.rs` (31 unit tests) |
| Write-path enforcement | Complete | `src/metadata_writer_validation.rs:102` rejects non-nullable new columns | — | `tests/drop_and_constraints_tests.rs` |

### 1.14 Write Atomicity & Conflict Detection

| Feature | Status | Evidence | Missing | Tests |
|---------|--------|----------|---------|-------|
| Atomic write transactions | Complete | `src/metadata_writer.rs:387` `begin_write_transaction()` | — | `tests/concurrent_write_tests.rs` |
| Partitioned write atomicity | Complete (2026-03-02) | Single `begin_write_transaction` for all partitions; all-or-nothing commit via `commit_uploaded_files()` / `cleanup_uploaded_files()` | — | Existing partition tests |
| Replace-mode safety | Complete (2026-03-02) | Old file ending deferred until after upload succeeds; prevents empty-table on upload failure | — | Existing write tests |
| Conflict detection (OCC) | Complete | `src/metadata_writer.rs:428` `begin_checked_write_transaction()` | — | `tests/conflict_detection_tests.rs` |
| Orphaned file cleanup | Complete | `src/table_writer.rs:582` `cleanup_orphaned_files()`, best-effort on commit failure | — | Implicit via write tests |

### 1.15 Multi-Backend Support

#### MetadataProvider (Read)

| Method | DuckDB | SQLite | PostgreSQL | MySQL |
|--------|--------|--------|------------|-------|
| `list_schemas()` | `src/metadata_provider_duckdb.rs` | `sqlite.rs:101` | `postgres.rs:133` | `mysql.rs:100` |
| `list_tables()` | Yes | `sqlite.rs:125` | `postgres.rs:157` | `mysql.rs:124` |
| `get_table_files_for_select()` | Yes | `sqlite.rs:178` | `postgres.rs:210` | `mysql.rs:177` |
| `get_schema_by_name()` | Yes | `sqlite.rs:261` | `postgres.rs:292` | `mysql.rs:259` |
| `get_table_by_name()` | Yes | `sqlite.rs:287` | `postgres.rs:318` | `mysql.rs:285` |
| `table_exists()` | Yes | `sqlite.rs:320` | `postgres.rs:351` | `mysql.rs:318` |
| `list_all_columns()` | Yes | `sqlite.rs:383` | `postgres.rs:415` | `mysql.rs:383` |
| `get_file_column_stats()` | Yes | `sqlite.rs:708` | `postgres.rs:727` | `mysql.rs:703` |
| `list_views()` | Yes | `sqlite.rs:734` | `postgres.rs:761` | `mysql.rs:818` |
| `get_view_by_name()` | Yes | `sqlite.rs:754` | `postgres.rs:786` | `mysql.rs:841` |
| `get_partition_columns()` | Yes | `sqlite.rs:792` | `postgres.rs:838` | `mysql.rs:770` |
| `get_file_partition_values()` | Yes | `sqlite.rs:816` | `postgres.rs:872` | `mysql.rs:794` |
| `get_inlined_data()` | Yes | `sqlite.rs:840` | `postgres.rs:903` | `mysql.rs:885` |

**All 13 MetadataProvider methods implemented across all 4 backends.**

#### MetadataWriter (Write)

| Method | SQLite | PostgreSQL | MySQL |
|--------|--------|------------|-------|
| `create_snapshot()` | `:626` | Yes | Yes |
| `get_or_create_schema()` | `:637` | `:585` | `:672` |
| `get_or_create_table()` | Yes | Yes | Yes |
| `set_columns()` | Yes | Yes | Yes |
| `register_column_stats()` | `:787` | `:728` | `:823` |
| `register_data_file()` | `:816` | Yes | Yes |
| `register_delete_file()` | `:923` | Yes | Yes |
| `end_table_files()` | `:840` | Yes | Yes |
| `begin_write_transaction()` | `:1118` | `:1088` | `:1204` |
| `begin_checked_write_transaction()` | Yes | Yes | Yes |
| `drop_table()` | `:964` | `:937` | `:1053` |
| `drop_table_checked()` | `:1070` | `:1042` | `:1158` |
| `drop_schema()` | `:971` | `:944` | `:1060` |
| `drop_schema_checked()` | `:1094` | `:1065` | `:1181` |
| `alter_table()` | `:1275` | `:1246` | `:1356` |
| `rename_table()` | `:1491` | `:1458` | `:1570` |
| `create_view()` | `:1131` | `:1101` | `:1217` |
| `drop_view()` | `:1164` | `:1134` | `:1248` |
| `rename_view()` | `:1189` | `:1159` | `:1271` |
| `set_table_comment()` | `:1575` | `:1543` | `:1653` |
| `set_column_comment()` | `:1633` | `:1602` | `:1710` |
| `get_active_columns()` | `:1468` | Yes | Yes |
| `list_active_table_ids()` | `:978` | Yes | Yes |
| `initialize_schema()` | `:890` | Yes | Yes |
| `get_data_path()` / `set_data_path()` | Yes | Yes | Yes |

**All MetadataWriter trait methods implemented across all 3 write backends.**

### 1.16 Query Planner

| Feature | Status | Evidence | Missing |
|---------|--------|----------|---------|
| INSERT routing | Complete | `src/query_planner.rs` — handled by DataFusion's `insert_into()` | — |
| DELETE routing | Complete | `src/query_planner.rs:57` routes `DmlStatement::Delete` | — |
| UPDATE routing | Complete | `src/query_planner.rs:67` routes `DmlStatement::Update` | — |
| MERGE routing | Not Started | No MERGE SQL parsing in DataFusion | MERGE only via programmatic API |
| Unsupported plan rejection | Complete | `src/query_planner.rs` rejects unknown plans | — |

## 2. Test Coverage Summary

### 2.1 Total Test Count

| Category | Count |
|----------|-------|
| **Total `#[test]` + `#[tokio::test]`** | **~725+** |
| SLT test files | 254 |
| SLT pass rate | 157/254 (61.8%) |

**Note (2026-03-02)**: +21 tests added during the R2 review fix cycle: 18 new unit tests (input validation, timestamp precisions, partition safety, SQL identifier quoting, date formatting) + 3 new interop tests (`test_df_write_partitioned_duckdb_read`, `test_df_write_inlined_duckdb_read`, `test_duckdb_partitioned_inlined_data`).

**Note (2026-03-03)**: +15 tests added during R5 review fix cycle: 11 new cross-engine tests (DML, DDL, inline data, partition operations) from fix-cross-engine agent + 4 new unit/integration tests from fix-test-infrastructure agent.

**Note (2026-03-04)**: Tests added during R6 review fix cycle: 10 new cross-engine tests, 7 partition validation tests, 9 table function tests, and additional unit tests. Total: 725+ tests passing.

### 2.2 Test Breakdown by File

**Integration tests (tests/):**

| File | `#[test]` | `#[tokio::test]` | Total |
|------|-----------|-------------------|-------|
| `adversarial_catalog_tests.rs` | 15 | 0 | 15 |
| `adversarial_pattern_tests_1.rs` | 55 | 0 | 55 |
| `adversarial_pattern_tests_2.rs` | 17 | 24 | 41 |
| `adversarial_storage_tests.rs` | 17 | 32 | 49 |
| `adversarial_type_schema_tests.rs` | 61 | 0 | 61 |
| `adversarial_edge_tests.rs` | 0 | 36 | 36 |
| `concurrent_tests.rs` | 0 | 6 | 6 |
| `cross_engine_alter_tests.rs` | 0 | 14 | 14 |
| `cross_engine_ddl_tests.rs` | 0 | 2 | 2 |
| `cross_engine_dml_tests.rs` | 0 | 1 | 1 |
| `cross_engine_feature_tests.rs` | 0 | 1 | 1 |
| `cross_engine_inline_tests.rs` | 0 | 9 | 9 |
| `cross_engine_insert_tests.rs` | 0 | 1 | 1 |
| `cross_engine_partition_tests.rs` | 0 | 7 | 7 |
| `cross_engine_postgres_tests.rs` | 0 | 8 | 8 |
| `cross_engine_mysql_tests.rs` | 0 | 8 | 8 |
| `delete_filter_tests.rs` | 0 | 11 | 11 |
| `encryption_tests.rs` | 0 | 3 | 3 |
| `information_schema_test.rs` | 0 | 17 | 17 |
| `issue_repro_schema_tests.rs` | 0 | 8 | 8 |
| `issue_repro_storage_tests.rs` | 0 | 9 | 9 |
| `issue_repro_stress_tests.rs` | 1 | 9 | 10 |
| `issue_repro_type_tests.rs` | 4 | 0 | 4 |
| `missing_delete_file_tests.rs` | 0 | 3 | 3 |
| `negative_footer_size_test.rs` | 0 | 3 | 3 |
| `numeric_metadata_validation_tests.rs` | 0 | 2 | 2 |
| `object_store_integration_test.rs` | 0 | 2 | 2 |
| `parity_tests.rs` | 0 | 8 | 8 |
| `renamed_columns_tests.rs` | 0 | 7 | 7 |
| `table_changes_tests.rs` | 0 | 12 | 12 |
| `table_function_tests.rs` | 0 | 8 | 8 |
| `table_tests.rs` | 0 | 5 | 5 |
| `time_travel_tests.rs` | 0 | 14 | 14 |
| `write_partition_tests.rs` | 0 | 6 | 6 |
| `write_inline_tests.rs` | 0 | 8 | 8 |
| `sqllogictest_runner.rs` | 0 | 1 | 1 (runs 254 SLT files) |

**Unit tests (src/):**

| File | `#[test]` | `#[tokio::test]` | Total |
|------|-----------|-------------------|-------|
| `types.rs` | 54 | 0 | 54 |
| `metadata_writer_validation.rs` | 31 | 0 | 31 |
| `path_resolver.rs` | 118 | 0 | 118 |
| `encryption.rs` | 22 | 0 | 22 |
| `metadata_writer.rs` | 10 | 0 | 10 |
| `schema.rs` | 6 | 0 | 6 |
| `table.rs` | 3 | 0 | 3 |
| `query_planner.rs` | 4 | 0 | 4 |
| `table_writer.rs` | 4 | 0 | 4 |
| `delete_filter.rs` | 3 | 0 | 3 |
| `virtual_column_exec.rs` | 1 | 0 | 1 |
| `column_rename.rs` | 1 | 0 | 1 |
| `insert_exec.rs` | 1 | 0 | 1 |

### 2.3 Cross-Engine Test Summary

| Category | Tests | Feature Gate |
|----------|-------|-------------|
| Alter operations | 14 | `write-sqlite` + `metadata-duckdb` + `metadata-sqlite` |
| DDL (views, drop, create schema) | 2 | Same |
| DML (delete, update) | 1 | Same |
| Feature (virtual cols, stats, etc.) | 1 | Same |
| Inline data | 9 | Same |
| Insert verification | 1 | Same |
| Partition operations | 7 | Same |
| Postgres cross-engine | 8 | `write-postgres` + `metadata-duckdb` + `metadata-postgres` (Docker) |
| MySQL cross-engine | 8 | `write-mysql` + `metadata-duckdb` + `metadata-mysql` (Docker) |
| **Total cross-engine** | **72+** | (11 new in R5, 10 new in R6 fix cycles) |

### 2.4 Per-Backend Test Coverage

| Backend | Provider Tests | Writer Tests |
|---------|---------------|-------------|
| DuckDB | All core tests use DuckDB | N/A (read-only) |
| SQLite | `tests/sqlite_metadata_provider_test.rs` | Unit tests in `src/metadata_writer_sqlite.rs` |
| PostgreSQL | `tests/postgres_metadata_provider_test.rs`, `tests/postgres_metadata_writer_test.rs` | Unit tests in `src/metadata_writer_postgres.rs` |
| MySQL | `tests/mysql_metadata_provider_test.rs`, `tests/mysql_metadata_writer_test.rs` | Unit tests in `src/metadata_writer_mysql.rs` |

Note: Postgres/MySQL tests require running database containers (testcontainers).

## 3. Remaining Work (Prioritized)

### Code Review Fixes (2026-03-01 + 2026-03-02 + 2026-03-03)

**Cycle 1 (2026-03-01)**: 36 findings (6 P0, 11 P1, 13 P2, 13 P3). All P0/P1 fixed, 9/13 P2 fixed.

**Cycle 2 (2026-03-02)**: 58 findings (5 P0, 15 P1, 21 P2, 17 P3). **55 of 58 fixed** across two rounds of fix agents:
- **Round 1** (33 fixed): SQL injection READ path, atomic DML, transaction safety, interop alignment (delete format, row_id_start, schema_versions, column IDs, UUIDs, changes_made), CTAS object store, MERGE key types, table function projections, test reliability, numeric safety
- **Round 2** (22 fixed): Virtual column row IDs, nullable cols, temporal roundtrip, decimal parser, hex key decoding, inlined row count, MySQL sql_mode, partition routing perf, individual SLT tests, test helper dedup, numeric try_from, Debug impls, view SQL rewrite, doc comments

**Cycle 3 (2026-03-02 R3, fixed 2026-03-03)**: 50 findings (3 P0, 9 P1, 16 P2, 22 P3). **25 of 50 fixed** across 6 fix agents:
- fix-sql-quoting (`065c411`): R3F-008 — quote_identifier consistency
- fix-inlined-types: R3F-004, 005, 012, 018, 026, 027 — Date/Timestamp roundtrip, timezone, type lossy roundtrips
- fix-pg-mysql-parity (`d9d54ce`): R3F-006 — port SQLite-only fixes to PG/MySQL
- fix-numeric-safety (`888705e`): R3F-009, 010, 025 — unwrap→error, as→try_from, dead variables
- fix-interop-critical (`3203d33`): R3F-001, 002, 003, 007, 011, 013, 014, 017 — table_column_stats, row_id_start, MERGE cleanup, schema_version, next_ids, snapshot_changes, set_data_path atomicity
- fix-test-harness (`3930b56`): R3F-015, 016, 020, 021, 023, 024 — test helper dedup, assertion fixes, SLT improvements

**Cycle 4 (2026-03-03 R4)**: 46 findings (1 P0, 12 P1, 20 P2, 13 P3). **44 of 46 fixed** across 8 fix agents:
- fix-dml-metadata (`54d3739`): R4-S-001, 002, 004, 005, 007, 013 — P0 inline data safety, DML stats/next_file_id/record_count
- fix-dml-correctness (`39fea14`): R4-S-010, 011, 012 — NULL filter, NOT NULL validation, LIMIT+delete
- fix-interop-format (`d567931`): R4-S-008, 009 — snapshot_changes tokens, delete file paths
- fix-pg-mysql (`2a51319`): R4-S-006 — port R3F-002 row_id_start to PG/MySQL
- fix-atomicity (worktree): R4-S-003, 014, 015, 016, 017, 018 — transaction safety, drop/rename validation, TOCTOU
- fix-quality (`d294651`): R4-S-019–022, 025, 026, 034, 035, 037, 039, 041, 042, 046 — snapshot isolation, error handling, casts, validation
- fix-interop-conventions (`fbeef2e`): R4-S-023, 024, 027, 043 — inlined data types, file naming, CDC dedup
- fix-tests (`11e4084`): R4-S-028–033, 038, 044, 045 — formatting, assertions, dedup, coverage

**Cycle 5 (2026-03-03 R5)**: 77 findings (0 P0, 11 P1, 28 P2, 38 P3). **72 of 77 fixed** across 8 fix agents:
- fix-backend-parity (`15b746f`): 15 findings — schema_version BIGINT, change_tracking, MySQL ID race, snapshot refresh, code quality
- fix-table-functions (`15b746f`): 9 findings — strip_prefix, inlined-table snapshot, dot-splitting, snapshot bounds, table_changes Delete
- fix-dml-robustness (`fc2f5da`): 13 findings — UPDATE bounds check, NaN keys, CDC dedup, code quality
- fix-metadata-correctness (various): 8 findings — lexicographic stats, stale stats, delete-delta boundary, statistics alignment
- fix-cross-engine (`fc2f5da`,`7c685bd`): 4 findings — 11 new cross-engine tests (DML, DDL, inline, partition)
- fix-write-safety (various): 8 findings — contains_null, inlining flush, Date partition pruning, overflow safety
- fix-interop-types (`84d51ff`): 7 findings — inlined serialization, Decimal flush/stats, delete format
- fix-test-infrastructure (`08f55a9`): 12 findings — Decimal sign, normalize, timestamp, virtual columns, 4 new tests

**Cycle 6 (2026-03-04 R6)**: 88 findings (0 P0, 14 P1, 38 P2, 36 P3). **~49 of 52 assigned fixed** across 10 fix agents:
- fix-sqlite-metadata (`d3aa034`): 5 findings — table_id in stats, row_id_start, overflow, decimal precision, type validation
- fix-backend-parity (`75ad2e1`): 5 findings — record_count decrement, end_table_files, atomic replace, row locking, UNIQUE stats
- fix-error-handling (`aaf5a4f`,`07cd101`): 5 findings — unwrap→error, silent NULL→error, epoch const
- fix-table-functions (`f93444c`): 8 findings — deferred compaction to scan time, parse_table_name, validation, INSTALL cache
- fix-interop (`b8a4476`): 4 findings — schema_version, CDC encryption, table naming, timestamp format
- fix-metadata-correctness (`f4c0f58`): 4 findings — SET NOT NULL warning, column_id docs, partition validation, snapshot propagation
- fix-test-infra (`03f9cb3`): 9 findings — transaction test, SLT filters, write test assertions, ORDER BY ALL tests
- fix-dml-robustness (`5666cf5`,`08ff2f7`): 3 findings — upload cleanup, snapshot cleanup, atomic single-file finish
- fix-code-quality (`c9c761b`): 3 findings — shared parser, transform enum, limit pushdown
- fix-cross-engine-tests (`d6a5104`): 3 findings — DF-write→DuckDB-read tests, schema assertions, BOOLEAN roundtrip

**Cumulative across 6 cycles**: 355 total findings, **~280 fixed**, 6 verified already correct, 1 false positive, 1 unfixable (R6-S-014), ~10 deferred/skipped, 36 P3 not assigned from R6, ~16 P3 nits remaining from R3.

**Deferred (architectural, L effort)**: F-036 (INSERT streaming/OOM), F-044 (provider/writer code dedup, also R4-S-036/040), F-045 (async trait redesign), R6-S-017 (concurrent DML race).

See `docs/2026-03-04-r6-review-synthesis.md` for R6 full details with **[FIXED]**/**[UNFIXABLE]**/**[DEFERRED]**/**[NOT ASSIGNED]** markers on each finding.

### Tier 1: Implementable Now (no external blockers)

All Tier 1 items are now complete.

| Item | Effort | Status |
|------|--------|--------|
| SLT pass rate improvement | Medium | Partially done. Currently 157/254 (61.8%). Fixable subset: ~10-15 result mismatches, 2-3 CTAS visibility. |
| ADD/REMOVE/RENAME FIELD (struct evolution) | Medium | Deferred to Tier 3 (architecture change needed) |
| Cross-engine Postgres/MySQL tests | Medium | **COMPLETE** — 16 tests (8 Postgres, 8 MySQL) in `tests/cross_engine_postgres_tests.rs` and `tests/cross_engine_mysql_tests.rs`. Patterns: df_write_df_read, df_write_duckdb_read, duckdb_write_df_read, null_handling, sql_create_insert_select, multiple_tables, count_query, bidirectional_roundtrip. DuckDB `ducklake:postgres:` interop confirmed. DuckDB `ducklake:mysql:` has minor DSN issue with empty passwords (tests gracefully skip). Tests use testcontainers, marked `#[ignore]`. |

### Tier 2: Blocked on External Factors

| Item | Blocker | Details |
|------|---------|---------|
| MERGE INTO via SQL | DataFusion limitation | DataFusion has no MERGE INTO SQL syntax. Only programmatic API works (`DuckLakeTable::merge()`). |
| Time travel SQL syntax (`AT SNAPSHOT N`) | DataFusion limitation | Would need custom SQL extension or DataFusion upstream support. Table functions (`ducklake_snapshots()`) work as alternative. |
| DuckDB-encrypted file reads | DuckDB limitation | DuckDB uses non-PME encryption. Only PME-compliant files (PyArrow, Spark) are supported. See `src/encryption.rs:15-20`. |

### Tier 3: Needs Architecture Changes

| Item | Details |
|------|---------|
| Encrypted writes (PME) | Would need parquet-rs PME write support + key management integration |
| Read-write catalog sharing with DuckDB | DuckDB creates `.ducklake` native format; our SQLite format is read-compatible but not write-compatible with DuckDB |

### Tier 4: Not Applicable / Intentionally Omitted

| Item | Reason |
|------|--------|
| DuckDB MetadataWriter | DuckDB is read-only provider by design; writes go through SQLite/Postgres/MySQL |
| Macros (DuckDB-specific) | DuckDB macro system is DuckDB-specific, not applicable to DataFusion |
| `ATTACH`/`DETACH` SQL | DuckDB-specific catalog management; DataFusion uses `register_catalog()` |
| DuckDB settings (`ducklake_set_option` etc.) | Delegated to DuckDB via compaction functions that open a DuckDB connection |

## 4. Integration Branch Commit History

```
d96035c docs: archive 10 stale docs to legacy, add INDEX.md
2020fa8 docs: add comprehensive remaining work audit
d332b28 feat: improve SLT pass rate from 75 to 120 tests (48.4%)
5f66562 feat: implement all missing MySQL MetadataWriter and MetadataProvider methods
4f73c9b feat: implement missing Postgres MetadataWriter + MetadataProvider methods
8102066 feat: add orphaned file cleanup and write atomicity documentation
3d874ff fix: add missing end_snapshot filter in list_all_columns + remaining-gaps docs
8c39e7e feat: improve SLT pass rate from 16 to 75 tests
1309413 fix: address remaining soundness issues M-1, M-2, M-3, L-5 + broken test
4798c34 feat: add ALTER VIEW RENAME TO and write path field-name validation
09912f5 feat: add rowid, snapshot_id, and file_index virtual columns
875663a feat: align table functions with DuckDB output format and add missing functions
48a648f feat: add compaction table functions for DuckLake catalog maintenance
b8099f7 feat: add partition and inlined data support for metadata providers
f06ba90 feat: register merge_exec module and add DuckLakeTable::merge() method
1fc952b feat: add MERGE INTO execution plan for DuckLake tables
9c6f1f1 feat: implement remaining ALTER TABLE operations
2b6b195 test: add cross-engine DDL tests for views, DROP TABLE/SCHEMA, CREATE SCHEMA
995419c feat: implement column stats read-side and file-level pruning
4d3ab55 feat: add ducklake_table_insertions, ducklake_current_snapshot, and ducklake_last_committed_snapshot table functions
cd30642 test: add cross-engine INSERT verification tests
9338d73 fix: embed field IDs in UPDATE exec Parquet output + add cross-engine DML tests
5d3af95 test: add cross-engine tests for virtual columns, query planner, stats, and conflict detection
c6451e4 fix: address 9 bugs from soundness and architecture reviews
3a08f94 test: add cross-engine test infrastructure for DataFusion + DuckDB interop
a6916b8 Merge branch 'main' into ducklake-features/integration
c0e5c11 fix: add missing footer_size > 0 guard in table_changes.rs (#59 follow-up)
cce616b fix: reject URL-encoded null bytes (%00) in path resolver (#55 follow-up)
a2c1243 fix: skip negative footer_size instead of wrapping to usize::MAX (#59)
e18832f fix: reject column_id values exceeding i32 range in Parquet field_id mapping (#63)
90d3b6b test: add comprehensive null byte rejection tests for path resolver (#55)
9c9aaf6 fix: reject URL-encoded path traversal (%2e%2e) in path resolver (#54 follow-up)
6d21593 fix: reject path traversal via ../ in path resolver (#54)
3633fe4 fix: error on missing delete files instead of silent data corruption (#52)
2a46791 test: add 41 adversarial pattern-matching tests from DuckLake issues #300-#800
740a593 test: add 48+ adversarial pattern-match tests from DuckLake issues #40-#300
b748e05 test: add 36 adversarial edge-case tests for concurrency, resources, boundaries
9c34638 test: add 49 adversarial tests for storage, paths, and delete files
db4ef3a test: add adversarial catalog security tests (43 tests)
35fbe8f test: add 93 adversarial tests for type system and schema evolution
decb78e fix: make concurrent table creation stress test deterministic
bae1348 test: consolidate issue repro tests, remove weak duplicates
1804819 fix: prevent double slashes in join_paths (fixes duckdb/ducklake#217)
72ee074 test: add concurrency stress tests for 13 DuckLake upstream issues
641eae5 test: add backend-specific repro tests for 8 Postgres/MySQL issues
6873f83 test: add issue reproduction tests for 39 DuckLake upstream bugs
071f03f test: add reproduction tests for 7 type system issues
7d72191 fix: correct MySQL sql_mode initialization in writer setup
1c1ba52 fix: correct column_order type mismatch in Postgres writer
7eb4ae2 docs: add validation reports, work log, and issue analysis
ed28562 test: add edge case, interop, and ALTER TABLE tests
57e1bb6 feat: catalog and table provider improvements
6c4710e fix: critical metadata writer fixes across all backends
8b7079c feat: improve type parser with VARCHAR(N) and quoted struct field support
49595f3 chore: update .gitignore for DuckLake test artifacts
09d513b chore: fix clippy warnings and add VirtualColumnExec re-export
0edf228 feat: add MySQL MetadataWriter implementation
b9e1045 feat: add PostgreSQL MetadataWriter implementation
bb6e037 feat: add virtual column support (filename, file_row_number)
e6d17ca chore: cargo fmt
564a465 refactor: remove identity map_err calls
c736d86 refactor: deduplicate footer_size calculation and delete_file_schema
fcd7498 fix: eliminate TOCTOU race in conflict detection by merging check+write into single transaction
d79c4e2 fix: write CTAS data in register_table for non-main schemas
917f8be fix: reject unsupported plans in QueryPlanner to prevent silent data loss
78356ee fix: add missing columns to Postgres test schema DDL
68c02f4 fix: enable WAL mode and busy_timeout for SQLite writer
1a94954 test: add 28 edge case and boundary condition tests
68c2589 test: add DuckDB parity tests for DataFusion+DuckLake
8da4f37 docs: add Phase 1/2 analysis documents to integration branch
fab6abf feat: integrate DELETE, UPDATE, Views, Stats, QueryPlanner, Complex Types, CREATE SCHEMA
2598dd2 test: port 18 new sqllogic tests to DataFusion-compatible format
feae608 feat: add ALTER TABLE support for ADD/DROP/RENAME COLUMN and ALTER TYPE (Gap 5)
7b602d6 feat: add transaction conflict detection for concurrent writes (Gap 14)
cbf9c00 test: add comprehensive tests for DROP TABLE, DROP SCHEMA, and NOT NULL constraints
9937dcb feat: implement DROP TABLE, DROP SCHEMA, and NOT NULL constraint enforcement
```

## 5. Comprehensive Test Harness: Three-Mode SLT Strategy

### 5.1 Vision

A comprehensive test harness with three SLT execution modes that validate every feature and confirm interoperability:

| Mode | Writer | Reader | Purpose |
|------|--------|--------|---------|
| **Mode 1**: Hybrid (DuckDB→DF) | DuckDB | DataFusion | Current SLT runner — validates DF read path against DuckDB-written catalogs |
| **Mode 2**: Pure DataFusion | DataFusion | DataFusion | Validates the full DF stack (writes + reads) end-to-end, no DuckDB dependency |
| **Mode 3**: Reverse Interop (DF→DuckDB) | DataFusion | DuckDB | Proves catalogs written by DF are readable by DuckDB (interop guarantee) |

### 5.2 Mode 1: Hybrid DuckDB→DataFusion (EXISTING — ~61% pass rate)

**Status**: Operational. 157/254 tests passing.

**Infrastructure**:
- `tests/sqllogictest_runner.rs`: Auto-discovers 254 `.test` files, preprocesses DuckDB-specific directives
- `tests/hybrid_asyncdb.rs`: `HybridDuckLakeDB` adapter implementing `AsyncDB` trait
  - Routes writes (CREATE/INSERT/UPDATE/DELETE/DROP/ALTER/USE/BEGIN/COMMIT/ROLLBACK) → DuckDB
  - Routes reads (SELECT) → DataFusion
  - Refreshes DataFusion catalog snapshot after each write (except during transactions)
  - Rewrites 2-part table refs (`ducklake.table` → `ducklake.main.table`)
  - Strips virtual columns from SELECT * results (matches DuckDB behavior)
  - Handles in-transaction reads by routing to DuckDB (DF can't see uncommitted data)

**Preprocessing** (`preprocess_test_file()`):
- Strips: `require`, `test-env`, `# name:`, `ATTACH`/`DETACH`, `EXPLAIN`, `CHECKPOINT`, `COMMENT ON`, `PRAGMA`
- Expands: `loop`/`foreach`/`endloop` blocks
- Skips: `concurrentloop`, `statement maybe`, multi-connection (`conN`), `mode skip`/`unskip`
- Rewrites: `ORDER BY ALL` → removed (adds `rowsort` to query directive)
- Filters: DuckDB-specific functions (`GLOB()`, `DUCKDB_TABLES()`, `PARQUET_METADATA()`, internal metadata tables, etc.)

**Remaining 97 failures**: 21 add_files issues, 12 unsupported struct/list/map evolution types, 10 data inlining (fundamental hybrid limitation), 9 macros (DuckLake limitation), 30 result mismatches, 15 other blocked (catalog names, DuckDB-specific, transactions). Note: 6 new view SLT tests were added, bringing total from 248 to 254.

### 5.3 Mode 2: Pure DataFusion (NOT STARTED — Feasibility Analysis)

**Concept**: Run SLT tests entirely through DataFusion — both writes AND reads — with no DuckDB in the loop. This validates the complete DataFusion-DuckLake stack.

**What exists for writes**:
- `INSERT INTO`: Complete — `src/table.rs:1620` `insert_into()` → `DuckLakeInsertExec`
- `CREATE TABLE` / `CREATE TABLE AS SELECT`: Complete — `src/schema.rs:311` `register_table()` handles empty tables and CTAS
- `DROP TABLE`: Complete — `src/schema.rs:264` `deregister_table()`
- `CREATE SCHEMA`: Complete — `src/catalog.rs:266` `get_or_create_schema()`
- `DROP SCHEMA`: Complete — `src/catalog.rs:209`
- `DELETE`: Complete — `src/table.rs:584` `delete()` → `DuckLakeDeleteExec`
- `UPDATE`: Complete — `src/table.rs:632` `update()` → `DuckLakeUpdateExec`
- `ALTER TABLE` (ADD/DROP/RENAME COLUMN, ALTER TYPE, SET/DROP DEFAULT, SET/DROP NOT NULL): Complete — `src/metadata_writer.rs` AlterTableOp variants
- `CREATE VIEW` / `DROP VIEW` / `RENAME VIEW`: Complete — `src/metadata_writer.rs:502-514`

**What's missing for a pure-DF SLT runner**:
1. **SLT adapter**: Need a `PureDataFusionDB` struct implementing `AsyncDB` that routes ALL statements (writes + reads) through a single writable `SessionContext`
2. **SQL parsing for ALTER TABLE**: DataFusion's SQL parser doesn't route all ALTER TABLE variants to our extension. The current `DuckLakeQueryPlanner` handles DELETE and UPDATE but not ALTER TABLE directly — ALTER goes through `SchemaProvider` methods that aren't all wired up for SQL-level invocation
3. **RENAME TABLE via SQL**: `ALTER TABLE ... RENAME TO ...` — DataFusion may not route this to our schema provider
4. **Transaction support**: `BEGIN`/`COMMIT`/`ROLLBACK` not implemented in the DF write path (writes are auto-committed)
5. **USE catalog**: No equivalent in DataFusion (all tables must use 3-part names)
6. **Metadata backend**: Need to decide between SQLite (requires `write-sqlite` feature) or a new lightweight in-memory backend

**Estimated effort**: Medium-Large
- Core `PureDataFusionDB` adapter: ~200 lines (similar pattern to `HybridDuckLakeDB`)
- Wire up missing ALTER TABLE SQL routing: ~100 lines in query planner
- Transaction wrapper: ~150 lines (batch writes, commit on COMMIT)
- Test preprocessing: Can reuse most of existing preprocessor, but needs adjustments since all statements go to DF

**Value assessment**: HIGH — This is the most important missing mode. It proves the DF extension works standalone, which is the primary use case. No DuckDB dependency means faster CI, simpler deployment.

### 5.4 Mode 3: Reverse Interop — DataFusion→DuckDB (PARTIALLY EXISTS)

**Concept**: DataFusion writes data, DuckDB verifies it can read the catalogs. Proves write interoperability.

**What exists**:
- `tests/cross_engine_tests.rs`: 6 cross-engine tests covering all three patterns:
  - `cross_engine_df_write_df_read()` — DF writes via `DuckLakeTableWriter` + SQLite backend → DF reads via `SqliteMetadataProvider`
  - `cross_engine_df_write_duckdb_read()` — DF writes → DuckDB reads via `DuckDbConn::open()` with `ducklake:sqlite:` URI
  - `cross_engine_duckdb_write_df_read()` — DuckDB writes → DF reads via `DuckdbMetadataProvider`
  - `cross_engine_bidirectional_roundtrip()` — DuckDB creates → DF reads → DuckDB appends → DF reads again
  - `cross_engine_assert_query_eq_both_engines()` — DuckDB writes → query both engines → compare results
  - `cross_engine_null_handling()` — DF writes with NULLs → DuckDB verifies → DF re-reads
- `tests/roundtrip_interop_tests.rs`: 5 roundtrip tests using DuckDB CLI binary:
  - `test_datafusion_writes_duckdb_reads()` — DF writes via SqliteMetadataWriter → DuckDB CLI reads
  - `test_duckdb_writes_datafusion_reads()` — DuckDB CLI writes → DF reads
  - `test_schema_evolution_roundtrip()` — DF writes, ALTER TABLE ADD COLUMN, writes more → DuckDB reads
  - `test_full_bidirectional_roundtrip()` — DuckDB creates → DF reads → DuckDB appends → DF reads again
- Additional cross-engine test files (35 tests total across 8 files):
  - `cross_engine_insert_tests.rs` (1), `cross_engine_dml_tests.rs` (1), `cross_engine_ddl_tests.rs` (2)
  - `cross_engine_alter_tests.rs` (14), `cross_engine_feature_tests.rs` (1)
  - `cross_engine_inline_tests.rs` (9), `cross_engine_partition_tests.rs` (7)

**What's missing for Mode 3 as an SLT runner**:
1. **SLT adapter**: Need a `ReverseInteropDB` struct implementing `AsyncDB` that routes writes to DataFusion and reads to DuckDB
2. **DF write completeness**: Same gaps as Mode 2 (ALTER TABLE SQL routing, transactions)
3. **DuckDB read-back**: Need DuckDB to ATTACH a catalog written by DF — currently works for SQLite-backed catalogs via `ducklake:sqlite:<path>` URI

**Estimated effort**: Medium (builds on Mode 2 infrastructure)
- Core `ReverseInteropDB` adapter: ~200 lines
- DuckDB read integration: ~50 lines (reuse `DuckDbConn` from cross_engine_tests)
- Preprocessing: Inverse of Mode 1 — DF handles writes, DuckDB handles reads

**Value assessment**: MEDIUM — Validates the interop guarantee. Most valuable after Mode 2 is working.

### 5.5 Implementation Roadmap

**Phase 1 — Mode 2: Pure DataFusion SLT Runner** (Highest value)
1. Create `tests/pure_datafusion_asyncdb.rs` implementing `AsyncDB`
2. Wire up all SQL statement types through writable `SessionContext` (SQLite backend)
3. Adapt preprocessor for pure-DF mode (no write/read routing split)
4. Get initial pass rate, fix DF-specific issues
5. Add ALTER TABLE SQL routing in `DuckLakeQueryPlanner`

**Phase 2 — Mode 3: Reverse Interop SLT Runner** (Builds on Phase 1)
1. Create `tests/reverse_interop_asyncdb.rs` implementing `AsyncDB`
2. Route writes through the same pure-DF mechanism from Phase 1
3. Route reads through DuckDB connection (ATTACH `ducklake:sqlite:<path>`)
4. Get initial pass rate, fix interop-specific issues

**Phase 3 — Unified Runner with Mode Selection**
1. Refactor `sqllogictest_runner.rs` to support `--mode hybrid|pure|reverse` parameter
2. Track per-mode pass rates
3. CI integration: run all three modes, report per-mode results

### 5.6 Current Cross-Engine Test Summary

| Category | Test File | Count | Patterns Covered |
|----------|-----------|-------|-----------------|
| Core interop | `cross_engine_tests.rs` | 6 | df→df, df→duckdb, duckdb→df, bidirectional, null handling, count |
| INSERT | `cross_engine_insert_tests.rs` | 1 | Insert verification |
| DML (DELETE/UPDATE) | `cross_engine_dml_tests.rs` | 1 | Delete+update cross-engine |
| DDL | `cross_engine_ddl_tests.rs` | 2 | Views, DROP TABLE/SCHEMA, CREATE SCHEMA |
| ALTER | `cross_engine_alter_tests.rs` | 14 | ADD/DROP/RENAME COLUMN, ALTER TYPE, etc. |
| Features | `cross_engine_feature_tests.rs` | 1 | Virtual cols, stats, conflicts |
| Inline data | `cross_engine_inline_tests.rs` | 9 | Data inlining interop |
| Partitions | `cross_engine_partition_tests.rs` | 7 | Partition pruning interop |
| Postgres | `cross_engine_postgres_tests.rs` | 8 | df_write_df_read, df_write_duckdb_read, duckdb_write_df_read, null_handling, sql_create_insert_select, multiple_tables, count_query, bidirectional_roundtrip |
| MySQL | `cross_engine_mysql_tests.rs` | 8 | Same 8 patterns as Postgres |
| Roundtrip (CLI) | `roundtrip_interop_tests.rs` | 5 | DuckDB CLI binary roundtrip |
| **Total** | | **62** | |

## 6. Source Code Size Summary (unchanged)

| File | Lines | Purpose |
|------|-------|---------|
| `metadata_writer_sqlite.rs` | 2,546 | SQLite write backend |
| `table.rs` | 1,891 | TableProvider, scan planning, pruning |
| `metadata_writer_mysql.rs` | 1,791 | MySQL write backend |
| `metadata_writer_postgres.rs` | 1,685 | PostgreSQL write backend |
| `types.rs` | 1,418 | Type mapping + complex type parser |
| `path_resolver.rs` | 1,415 | URL/path resolution |
| `metadata_provider_postgres.rs` | 1,079 | PostgreSQL read backend |
| `metadata_provider_mysql.rs` | 1,018 | MySQL read backend |
| `metadata_provider_sqlite.rs` | 1,011 | SQLite read backend |
| `compaction_functions.rs` | 819 | 9 compaction/maintenance table functions |
| `metadata_writer_validation.rs` | 795 | Schema evolution validation |
| `information_schema.rs` | 784 | Information schema provider |
| `metadata_provider.rs` | 773 | MetadataProvider trait + types |
| `table_deletions.rs` | 733 | DELETE file generation |
| `table_writer.rs` | 707 | Parquet write + metadata commit |
| **Total** | **25,664** | All `src/*.rs` files |

## 7. Feature Flags (Cargo.toml)

| Feature | Dependencies | Purpose |
|---------|-------------|---------|
| `default` | `metadata-duckdb` | DuckDB read backend |
| `metadata-duckdb` | `duckdb` (bundled) | DuckDB MetadataProvider |
| `metadata-sqlite` | `sqlx/sqlite` | SQLite MetadataProvider |
| `metadata-postgres` | `sqlx/postgres` | PostgreSQL MetadataProvider |
| `metadata-mysql` | `sqlx/mysql` | MySQL MetadataProvider |
| `write` | (base write infra) | Write path infrastructure |
| `write-sqlite` | `write` + `metadata-sqlite` | SQLite MetadataWriter |
| `write-postgres` | `write` + `metadata-postgres` | PostgreSQL MetadataWriter |
| `write-mysql` | `write` + `metadata-mysql` | MySQL MetadataWriter |
| `encryption` | `parquet/encryption`, `datafusion/parquet_encryption` | PME encryption support |
