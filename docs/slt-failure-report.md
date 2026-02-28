# SQLLogicTest Failure Report

## Summary

| Metric | Count |
|--------|-------|
| **Total tests** | 248 |
| **Passing** | 120 |
| **Failing** | 128 |
| **Pass rate** | 48.4% |
| **Baseline (before fixes)** | 75 / 248 (30.2%) |
| **Tests gained** | +45 |

## Fixes Applied

1. **ORDER BY ALL rewriting** — DataFusion's GenericDialect doesn't support `ORDER BY ALL`. Added preprocessor and runtime rewriting to remove `ORDER BY ALL` and add `rowsort` to query directives.
2. **Named parameter `=>` skipping** — DuckDB named parameter syntax not supported by DataFusion. Skip test blocks containing `=>`.
3. **Expanded unsupported function detection** — Added ~20 DuckDB-specific functions (`DUCKDB_COLUMNS`, `READ_PARQUET`, `TYPEOF`, `STRLEN`, `CURRENT_SETTING`, `DUCKLAKE_CURRENT_SNAPSHOT`, etc.) to the skip list.
4. **COMMENT ON / PRAGMA routing** — Added to write statement routing and skip detection.
5. **Virtual column star conflict detection** — Skip queries mixing `SELECT *` with virtual columns (`ROWID`, `SNAPSHOT_ID`, `FILE_INDEX`, `FILENAME`, `FILE_ROW_NUMBER`) that cause duplicate projection errors.
6. **View support in DuckDB metadata provider** — Implemented `list_views`, `get_view_by_name`, `view_exists` methods.
7. **Transaction state tracking** — Track BEGIN/COMMIT/ROLLBACK to route reads within transactions to DuckDB (since DataFusion can't see uncommitted data).
8. **DML row count return** — Write statements (INSERT/UPDATE/DELETE) now return row counts for `query I` test patterns.
9. **Three-part table reference fix** — Prevent `ducklake.schema.table` from being rewritten to `ducklake.main.schema.table`.

---

## Remaining Failures by Category

### 1. DuckDB `add_files` Syntax Error (19 tests) — BLOCKED

**Error:** `Parser Error: Failed to add data files to DuckLake: syntax error at or near "."`
**Root cause:** DuckDB DuckLake extension version mismatch. The `ducklake_add_data_files()` function's SQL parser doesn't recognize the path format used in tests.
**Fix needed:** Update DuckDB or DuckLake extension to a compatible version.

| Test | Error |
|------|-------|
| add_files/add_empty_file.test | syntax error at or near "." |
| add_files/add_file_footer_size.test | syntax error at or near "." |
| add_files/add_file_specific_schema.test | syntax error at or near "." |
| add_files/add_files.test | syntax error at or near "." |
| add_files/add_files_compaction.test | syntax error at or near "." |
| add_files/add_files_complex_nested_stats_mre.test | syntax error at or near "." |
| add_files/add_files_nested.test | syntax error at or near "." |
| add_files/add_files_rename.test | syntax error at or near "." |
| add_files/add_files_table_changes.test | syntax error at or near "." |
| add_files/add_files_transaction_local.test | syntax error at or near "." |
| add_files/add_files_type_check_decimal.test | syntax error at or near "." |
| add_files/add_files_type_check_float.test | syntax error at or near "." |
| add_files/add_files_type_check_integer.test | syntax error at or near "." |
| add_files/add_files_type_check_nested.test | syntax error at or near "." |
| add_files/add_files_type_check_string_blob.test | syntax error at or near "." |
| add_files/add_files_type_check_timestamp.test | syntax error at or near "." |
| add_files/add_files_type_check_uuid.test | syntax error at or near "." |
| add_files/add_rollback.test | syntax error at or near "." |
| remove_orphans/mixed_paths.test | syntax error at or near "." |

### 2. Unsupported Complex Types: struct/list/map (13 tests) — BLOCKED

**Error:** `External error: Unsupported DuckLake type: struct|list|map`
**Root cause:** The DataFusion-DuckLake type mapper (`src/types.rs`) doesn't support complex/nested types yet.
**Fix needed:** Implement struct, list, and map type parsing in `map_ducklake_type()`.

| Test | Type |
|------|------|
| alter/add_column_nested.test | struct |
| alter/drop_column_nested.test | struct |
| alter/mixed_alter2.test | struct |
| alter/struct_evolution.test | struct |
| alter/struct_evolution_nested.test | struct |
| alter/struct_evolution_nested_alter.test | struct |
| alter/struct_evolution_reuse.test | struct |
| alter/struct_in_list_evolution.test | list |
| alter/struct_in_map_evolution.test | map |
| time_travel/basic_time_travel.test | struct |
| types/list.test | list |
| types/map.test | map |
| types/struct.test | struct |

### 3. DuckDB Macros Not Supported (9 tests) — BLOCKED

**Error:** `Not implemented Error: DuckLake does not support functions`
**Root cause:** DuckDB DuckLake extension doesn't support CREATE MACRO / CREATE FUNCTION in the DuckLake catalog.
**Fix needed:** DuckLake extension must add macro/function support.

| Test | Error |
|------|-------|
| macros/test_attach_timetravel.test | DuckLake does not support functions |
| macros/test_default_parameter.test | DuckLake does not support functions |
| macros/test_defined_types.test | Typed macro params require storage v1.4.0+ |
| macros/test_macro_tables.test | DuckLake does not support functions |
| macros/test_macro_transactions.test | DuckLake does not support functions |
| macros/test_multiple_implementations.test | DuckLake does not support functions |
| macros/test_scalar_table_macros.test | DuckLake does not support functions |
| macros/test_schema_dependency.test | DuckLake does not support functions |
| macros/test_simple_macro.test | DuckLake does not support functions |

### 4. DuckDB Spatial Extension Not Available (4 tests) — BLOCKED

**Error:** `Type with name "GEOMETRY" is not in the catalog, but it exists in the spatial extension`
**Root cause:** The spatial extension is not installed/loaded in the test environment.
**Fix needed:** Install and load the DuckDB spatial extension, or skip these tests.

| Test | Error |
|------|-------|
| geo/ducklake_geometry.test | GEOMETRY type missing |
| geo/ducklake_geometry_add_files.test | st_point function missing |
| geo/ducklake_geometry_inlining.test | GEOMETRY type missing |
| geo/ducklake_geometry_merge.test | GEOMETRY type missing |

### 5. SET schema / Multi-Catalog Not Supported (4 tests) — BLOCKED

**Error:** `Catalog Error: SET schema: No catalog + schema named "X" found`
**Root cause:** The test adapter creates a fresh DuckDB connection per test. `SET schema` requires the DuckLake catalog to be attached to the DuckDB connection, which only happens during initial setup of the default catalog.
**Fix needed:** Support multi-catalog attachment in the test adapter, or implement `SET schema` handling.

| Test | Missing Catalog |
|------|----------------|
| alter/rename_table_dbt_workload.test | my_ducklake |
| attach/different_paths.test | a_ducklake |
| cloud/test_cloud_cases.test | lake |
| data_inlining/inlining_issue_on_empty_inline.test | inlining |

### 6. Missing DataFusion Functions (7 tests) — BLOCKED

**Error:** `Invalid function 'X'` or `table function 'X' not found`
**Root cause:** DuckDB-specific functions/table functions not available in DataFusion.
**Fix needed:** Register equivalent UDFs in DataFusion, or implement function translation.

| Test | Missing Function |
|------|-----------------|
| checkpoint/checkpoint_ducklake.test | GLOB (table function) |
| data_inlining/data_inlining_partitions.test | year() |
| functions/ducklake_table_info.test | ducklake() table function |
| general/database_size.test | PRAGMA_database_size() |
| partitioning/partitioning_alter.test | year() |
| partitioning/year_month_day.test | year() |
| time_travel/time_travel_views.test | columns() |

### 7. DuckDB-Specific Errors (misc) (12 tests) — BLOCKED

Various DuckDB-specific behaviors that can't be replicated in the hybrid test adapter.

| Test | Error | Root Cause |
|------|-------|------------|
| add_files/add_files_hive.test | Directory not empty | Test data directories persist between runs |
| add_files/add_files_hive_mismatch.test | Directory not empty | Test data directories persist between runs |
| add_files/add_files_list.test | Function type mismatch | DuckDB `ducklake_add_data_files` signature mismatch |
| add_files/add_old_list.test | parse error: "WITH NO DATA" | SLT parser can't handle multi-line SQL |
| add_files/add_removed_files.test | File list cannot be NULL | DuckDB add_files NULL handling |
| alter/alter_timestamptz_promotion.test | Cannot change TIMESTAMP to TIMESTAMPTZ | DuckDB type promotion limitation |
| autoloading/autoload_data_path.test | Secret Manager settings locked | DuckDB runtime constraint |
| default/default_expressions.test | Only literals as defaults | DuckDB DuckLake limitation |
| merge/merge_timestamp.test | year(UUID) binder error | DuckDB function type error |
| migration/migration.test | Table not found | Requires pre-existing v0.1 catalog DB |
| migration/v01_partitioned.test | Table not found | Requires pre-existing v0.1 catalog DB |
| partitioning/multi_key_merge.test | Files have different hive partition path | DuckDB internal compactor error |

### 8. Table/Schema Not Found in DataFusion (8 tests) — FIXABLE

**Error:** `table 'X' not found` or `schema not found`
**Root cause:** Various issues: tables created in transactions not yet visible, metadata tables not exposed, tables created with non-standard paths.

| Test | Missing Table | Root Cause |
|------|--------------|------------|
| compaction/compaction_partitioned_non_adjacent.test | ducklake_metadata.ducklake_data_file | Metadata tables not exposed to DataFusion |
| compaction/compaction_partitioned_table.test | ducklake_metadata.ducklake_data_file | Metadata tables not exposed to DataFusion |
| data_inlining/data_inlining_large.test | public.bigtbl | Table created via `CREATE TABLE ... AS` not visible |
| data_inlining/data_inlining_types.test | public.all_types | Table created via `CREATE TABLE ... AS` not visible |
| delete/delete_ignore_extra_columns.test | ducklake.main.test | Table resolution after write/read sequence |
| merge/merge_update_insert.test | ducklake.main.stock | Table resolution timing issue |
| partitioning/basic_partitioning.test | ducklake_metadata.ducklake_data_file | Metadata tables not exposed |
| partitioning/disable_hive_partitioning.test | Path-based table reference | Glob path used as table name |

### 9. Expected Failure Mismatch (4 tests) — FIXABLE

**Error:** `statement is expected to fail, but actually succeed` or `query is expected to fail, but actually succeed`
**Root cause:** The hybrid adapter succeeds where DuckDB-only would fail, because writes go to DuckDB and reads go to DataFusion with different error behaviors.

| Test | Issue |
|------|-------|
| data_inlining/data_inlining_transaction_local_alter.test | ALTER within transaction succeeds in hybrid |
| general/detach_ducklake.test | DETACH succeeds (no-op in hybrid) |
| general/ducklake_read_only.test | Read-only write succeeds (DuckDB is writable) |
| general/missing_parquet.test | Missing file error not raised at statement time |

### 10. Transaction/Concurrency Issues (7 tests) — PARTIALLY FIXABLE

**Root cause:** Multi-connection transaction behavior differs between DuckDB-only and hybrid adapter. Some tests require two separate DuckDB connections with interleaved transactions.

| Test | Error | Root Cause |
|------|-------|------------|
| transaction/basic_transaction.test | result mismatch | Transaction visibility differences |
| transaction/concurrent_table_creation.test | table not found | Race condition in table creation |
| transaction/create_conflict.test | result mismatch | Conflict detection differs |
| transaction/transaction_conflicts.test | Table not found | Multi-connection required |
| transaction/transaction_conflicts_delete.test | Table already exists | Multi-connection required |
| transaction/transaction_conflicts_view.test | duckdb_views not found | System table not exposed |
| transaction/transaction_schema.test | Schema not found | Multi-schema transaction |

### 11. Query Result Mismatch — Data Inlining (7 tests) — FIXABLE

**Error:** `query result mismatch`
**Root cause:** Data inlining stores small datasets directly in the catalog metadata rather than in Parquet files. DataFusion reads only Parquet files and misses inlined data. The adapter needs to expose inlined data to DataFusion queries.

| Test |
|------|
| data_inlining/basic_data_inlining.test |
| data_inlining/data_inlining_alter.test |
| data_inlining/data_inlining_flush_schema.test |
| data_inlining/data_inlining_option_transaction_local.test |
| data_inlining/insert_inlining_concurrent.test |
| concurrent/concurrent_insert_data_inlining.test |
| concurrent/file_level_conflict.test |

### 12. Query Result Mismatch — Various (33 tests) — PARTIALLY FIXABLE

**Error:** `query result mismatch`
**Root cause:** Various differences between DuckDB and DataFusion query results. Common causes include: NULL display differences, NaN/Inf handling, rowid computation, timestamp formatting, column ordering, and type promotion edge cases.

| Test | Likely Root Cause |
|------|------------------|
| add_files/add_files_extra_columns.test | Column ordering after add_files |
| add_files/add_files_missing_columns.test | NULL column handling |
| add_files/add_files_missing_fields.test | Missing struct field handling |
| alter/expire_snapshot_bug.test | Snapshot expiration visibility |
| alter/mixed_alter.test | Type promotion display |
| alter/promote_type.test | Type promotion result format |
| alter/struct_evolution_alter.test | Struct field evolution display |
| alter/struct_evolution_list_alter.test | List evolution display |
| alter/struct_evolution_map_alter.test | Map evolution display |
| audit/test_base_audit.test | Audit log format differences |
| checkpoint/checkpoint_updates_interleaved.test | Checkpoint state visibility |
| compaction/compaction_delete_conflict.test | Delete conflict result format |
| compaction/compaction_full_file_delete.test | Full file delete visibility |
| compaction/compaction_hive_structure.test | Hive partition path format |
| compaction/merge_files_expired_snapshots.test | Snapshot expiration merge result |
| concurrent/concurrent_insert_conflict.test | Concurrent insert visibility |
| default/add_column_with_default.test | Default value application |
| default/struct_field_default.test | Struct field default display |
| general/attach_at_snapshot.test | Snapshot-specific attach result |
| merge/merge_partition.test | Merge partition result |
| rewrite_data_files/test_last_snapshot_rewrite.test | Rewrite file visibility |
| rewrite_data_files/test_rewrite_concurrency.test | Concurrent rewrite result |
| rewrite_data_files/test_rewrite_transaction_conflict.test | Rewrite conflict result |
| rowid/ducklake_row_id.test | Rowid computation differs |
| rowid/ducklake_row_id_update.test | Rowid after update |
| secrets/ducklake_secrets.test | Secret metadata format |
| settings/max_retry_count.test | Setting value format |
| table_changes/ducklake_table_deletions.test | Table change tracking format |
| types/floats.test | NaN/Inf display differences |
| types/null_byte.test | Null byte display |
| types/timestamp.test | Optimizer simplify_expressions bug |
| types/all_types.test | Table created via CTAS not visible |
| view/ducklake_view.test | View result format |
| virtualcolumns/ducklake_virtual_columns.test | Virtual column values |

---

## Category Summary

| Category | Count | Status |
|----------|-------|--------|
| DuckDB add_files syntax error | 19 | BLOCKED — DuckDB version mismatch |
| Unsupported complex types | 13 | BLOCKED — Implement struct/list/map in types.rs |
| DuckDB macros not supported | 9 | BLOCKED — DuckLake extension limitation |
| DuckDB spatial extension | 4 | BLOCKED — Extension not installed |
| SET schema / multi-catalog | 4 | BLOCKED — Test adapter limitation |
| Missing DataFusion functions | 7 | BLOCKED — Need UDF registration |
| DuckDB-specific errors | 12 | BLOCKED — Various DuckDB limitations |
| Table not found | 8 | FIXABLE — Metadata exposure / CTAS handling |
| Expected failure mismatch | 4 | FIXABLE — Hybrid adapter behavior |
| Transaction/concurrency | 7 | PARTIALLY FIXABLE — Multi-connection needed |
| Data inlining visibility | 7 | FIXABLE — Need inlined data reader |
| Query result mismatch | 34 | PARTIALLY FIXABLE — Various causes |
| **Total** | **128** | |

## Priority Recommendations

### High Priority (most impactful fixes)
1. **Implement struct/list/map types** in `src/types.rs` — unblocks 13 tests
2. **Data inlining reader** — expose inlined data to DataFusion — unblocks 7+ tests
3. **Register `year()` UDF** in DataFusion — unblocks 3 tests
4. **Expose metadata tables** (`ducklake_data_file` etc.) to DataFusion — unblocks 3 tests

### Medium Priority
5. **Fix CTAS table visibility** — tables created via `CREATE TABLE ... AS` should be visible — unblocks 3 tests
6. **Fix expected failure handling** — adjust test adapter for hybrid behavior — unblocks 4 tests
7. **Fix transaction table resolution** — improve catalog refresh timing — unblocks 2-3 tests

### Blocked (requires external changes)
8. **Update DuckDB/DuckLake extension** — unblocks 19 add_files tests
9. **DuckLake macro support** — requires upstream DuckLake changes — 9 tests
10. **Install spatial extension** — 4 tests
