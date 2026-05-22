# R5 Interop Review — 2026-03-03

## Summary
- Files reviewed: 8 (metadata_writer_sqlite.rs, metadata_writer_postgres.rs, metadata_writer_mysql.rs, table_writer.rs, delete_exec.rs, update_exec.rs, merge_exec.rs, cross_engine_tests.rs)
- Findings: 17 (0 P0, 4 P1, 7 P2, 6 P3)
- Focus: NEW issues not covered by R3/R4 reviews

## Methodology

1. Created fresh DuckDB DuckLake catalog (DuckDB v1.4.4, ducklake extension) with INSERT, DELETE, UPDATE ops
2. Dumped all catalog table schemas and data for column-by-column comparison
3. Read all writer files and DML execution plans
4. Analyzed cross-engine test coverage gaps
5. Compared snapshot_changes tokens, file paths, UUID format, inline data encoding

## DuckDB Schema Comparison (Delta from R3)

R3 covered the main schema comparison. R5 re-verified and found these **additional** or **persistent** issues:

| Issue | Status | Notes |
|-------|--------|-------|
| Extra cols in ducklake_column (default_value_type, default_value_dialect) | Still present | Known from R3 (I-R3-06), DuckDB ignores |
| Extra col in ducklake_data_file (partial_max) | Still present | Known from R3 |
| Extra col in ducklake_delete_file (partial_max) | Still present | Known from R3 |
| Extra col in ducklake_schema_versions (table_id) | Still present | Known from R3 |
| `schema_version` is INTEGER not BIGINT in all writers | NEW | See IO-002 |
| Extra tables (_df_change_tracking, ducklake_macro*, ducklake_sort*, ducklake_file_variant_stats) | Still present | Known from R3, informational |

## Findings

### IO-001: No cross-engine tests for DELETE, UPDATE, or MERGE operations
- **Severity**: P1
- **Files**: `tests/cross_engine_tests.rs`
- **Description**: The entire cross-engine test suite only covers INSERT + SELECT. There are zero tests verifying:
  - DF DELETE produces delete files that DuckDB can read
  - DF UPDATE (delete + insert pattern) produces metadata DuckDB can interpret
  - DF MERGE produces correct cross-engine output
  - DuckDB DELETE/UPDATE produces metadata that DF's DeleteFilterExec handles correctly
- **Impact**: The highest-risk interop scenarios (MOR delete files, row_id_start accounting) are completely untested between engines.
- **Suggested fix**: Add cross-engine tests for each DML operation in both DF->DuckDB and DuckDB->DF directions.

### IO-002: `schema_version` column type is INTEGER in all writers, DuckDB uses BIGINT
- **Severity**: P2
- **Files**: `src/metadata_writer_sqlite.rs:34`, `src/metadata_writer_postgres.rs:30`, `src/metadata_writer_mysql.rs:32`
- **Description**: DuckDB's `ducklake_snapshot` has `schema_version BIGINT`. Our writers all use `schema_version INTEGER DEFAULT 1`. In SQLite, INTEGER is 8-byte (functionally BIGINT), so no issue there. In PostgreSQL, INTEGER is 4 bytes (max ~2.1B), creating a theoretical overflow. In MySQL, same 4-byte limit.
- **Impact**: For SQLite: no issue. For Postgres/MySQL: theoretical overflow with extremely long-running catalogs. Inconsistent with spec.
- **Suggested fix**: Change `INTEGER` to `BIGINT` for `schema_version` in Postgres and MySQL DDL.

### IO-003: No cross-engine tests for ALTER TABLE schema evolution
- **Severity**: P1
- **Files**: `tests/cross_engine_tests.rs`
- **Description**: No test covers DF adding a column via ALTER TABLE then DuckDB reading the table (or vice versa). Column ID reuse, schema_version bumps, and ducklake_schema_versions consistency are untested cross-engine.
- **Suggested fix**: Add ALTER TABLE ADD COLUMN and DROP COLUMN cross-engine tests.

### IO-004: No cross-engine tests for partitioned tables
- **Severity**: P1
- **Files**: `tests/cross_engine_tests.rs`
- **Description**: No test creates a partitioned table in one engine and reads it from the other. Partition metadata (partition_info, partition_column, file_partition_value) interop is untested.
- **Suggested fix**: Add partitioned table cross-engine tests.

### IO-005: No cross-engine tests for TIMESTAMP, DATE, or DECIMAL types
- **Severity**: P1
- **Files**: `tests/cross_engine_tests.rs`
- **Description**: Only basic types (INT, VARCHAR, DOUBLE, BIGINT, BOOLEAN) are tested cross-engine. Timestamps are particularly high-risk because each writer uses different timestamp formats (Postgres: `TIMESTAMP WITH TIME ZONE`, MySQL: `TIMESTAMP(6)`, SQLite: `TEXT` with strftime), and Parquet timestamp encoding varies.
- **Suggested fix**: Add cross-engine tests covering TIMESTAMP, DATE, DECIMAL, BLOB types.

### IO-006: Inlined Date32 serialization uses epoch-days integer, DuckDB uses ISO date strings
- **Severity**: P2
- **Files**: `src/table_writer.rs:1016`
- **Description**: When serializing Date32 values for inlined storage, `arrow_array_value_to_string` outputs the raw epoch-days integer (e.g., `"19889"`). DuckDB stores dates as ISO strings (e.g., `"2024-06-15"`). DF-written inlined dates are unreadable by DuckDB, and DuckDB-written inlined dates fail to parse in DataFusion.
- **Impact**: Cross-engine interop for inlined data containing Date columns is broken. Parquet data files are unaffected.
- **Suggested fix**: Serialize Date32 as ISO 8601 date strings (e.g., `NaiveDate::from_num_days_from_ce(epoch_days + UNIX_EPOCH_DAYS).format("%Y-%m-%d")`).

### IO-007: Inlined Timestamp serialization uses raw epoch integers
- **Severity**: P2
- **Files**: `src/table_writer.rs:1018-1025`
- **Description**: Similar to IO-006, timestamps are serialized as raw epoch integers (e.g., `"1718451000000000"` for microseconds). DuckDB stores timestamps as ISO strings (e.g., `"2024-06-15 12:30:00"`). Same cross-engine interop issue.
- **Impact**: Cross-engine interop for inlined data with Timestamp columns is broken.
- **Suggested fix**: Serialize timestamps as ISO 8601 strings with appropriate precision.

### IO-008: Inlined data flush fails for Decimal types
- **Severity**: P2
- **Files**: `src/table_writer.rs:1196-1201`
- **Description**: When flushing inlined data to Parquet via `inlined_rows_to_batch`, Decimal128/Decimal256 types reach the fallback `other` arm and return an `UnsupportedType` error. Tables with Decimal columns that use inlined data will fail to flush.
- **Impact**: `ducklake_flush_inlined_data()` and the `write_or_inline` threshold-exceeded path will fail for any table with Decimal columns.
- **Suggested fix**: Add a Decimal128/256 parsing arm in `parse_string_to_array`.

### IO-009: Column stats for Decimal/FixedLenByteArray silently dropped
- **Severity**: P2
- **Files**: `src/table_writer.rs:1336-1347`
- **Description**: For `FixedLenByteArray` statistics (Decimal128 Parquet encoding), min/max values are extracted via `String::from_utf8(v.data().to_vec()).ok()`. Decimal values as fixed-length big-endian byte arrays are binary, not valid UTF-8. `from_utf8` returns `None`, silently dropping min/max stats.
- **Impact**: File-level pruning based on Decimal column min/max will not work. Files are still queryable.
- **Suggested fix**: Decode FixedLenByteArray stats based on logical type. For Decimal: interpret bytes as big-endian two's complement integer and divide by 10^scale.

### IO-010: `register_dml_files` omits `format` column for delete file INSERT
- **Severity**: P2
- **Files**: `src/metadata_writer_sqlite.rs:1317-1319`, `src/metadata_writer_postgres.rs:1052-1053`, `src/metadata_writer_mysql.rs:1154-1155`
- **Description**: The batch `register_dml_files` INSERT for delete files omits the `format` column. The individual `register_delete_file` method correctly includes `format = 'parquet'`. SQLite's `DEFAULT 'parquet'` handles this, but Postgres/MySQL may produce NULL if the DEFAULT isn't explicitly defined.
- **Impact**: A NULL `format` field may confuse DuckDB when reading back the catalog.
- **Suggested fix**: Add `format` to the INSERT column list and bind `'parquet'` in all three backends' `register_dml_files`.

### IO-011: `_df_change_tracking` is blind to DuckDB-side changes
- **Severity**: P2
- **Files**: All three metadata writers
- **Description**: The `_df_change_tracking` table is DataFusion-specific. DuckDB will NOT populate it when dropping tables/schemas. This means conflict detection only works for DF-to-DF concurrent writes, not for DF-to-DuckDB concurrent writes.
- **Impact**: If DuckDB drops a table and DataFusion tries to write to it, the conflict check won't detect it.
- **Suggested fix**: Consider also checking ducklake_snapshot_changes or ducklake_table.end_snapshot for conflict detection as a fallback.

### IO-012: UUID version mismatch (v4 vs v7)
- **Severity**: P3
- **Files**: `src/table_writer.rs:85`, `src/delete_exec.rs:327`, `src/update_exec.rs:392`, `src/merge_exec.rs:468`, all writer files for schema/table UUIDs
- **Description**: DuckDB uses UUID v7 (time-ordered, e.g., `019cb0fd-a7d1-75a7-...`) while our code uses UUID v4 (random). Both are valid UUIDs. DuckDB treats file paths as opaque strings.
- **Impact**: No functional impact. Files written by DataFusion are visually distinguishable from DuckDB-written files. v7 provides natural time-ordering for debugging.
- **Suggested fix**: Consider switching to UUID v7 for consistency. Low priority.

### IO-013: MERGE snapshot_changes token always records both inserted+deleted
- **Severity**: P3
- **Files**: `src/merge_exec.rs:638-643`
- **Description**: MERGE always records `"inserted_into_table:{id},deleted_from_table:{id}"` regardless of actual operations. For insert-only merges, `deleted_from_table` is misleading; for delete-only merges, `inserted_into_table` is misleading.
- **Impact**: Cosmetic — affects audit trail accuracy but not functionality.
- **Suggested fix**: Build token string conditionally based on whether delete_files and data_files are non-empty.

### IO-014: MySQL `TIMESTAMP(6)` has Y2038 problem
- **Severity**: P3
- **Files**: `src/metadata_writer_mysql.rs:31`
- **Description**: MySQL `TIMESTAMP` type has a range of 1970-01-01 to 2038-01-19. DuckDB uses `TIMESTAMP WITH TIME ZONE` with much wider range. `snapshot_time` is informational so not critical.
- **Suggested fix**: Use `DATETIME(6)` instead of `TIMESTAMP(6)` in MySQL DDL.

### IO-015: MySQL VARCHAR length limits could truncate long paths
- **Severity**: P3
- **Files**: `src/metadata_writer_mysql.rs:22-324`
- **Description**: MySQL forces `VARCHAR(255)` for table_name and `VARCHAR(1024)` for path/partial_file_info. Deep S3 paths or large JSON payloads could be silently truncated.
- **Suggested fix**: Use `TEXT` for path and partial_file_info columns in MySQL. Increase VARCHAR limits for names.

### IO-016: Cross-engine tests only use SQLite backend, not Postgres/MySQL
- **Severity**: P3
- **Files**: `tests/cross_engine_tests.rs:48-68`
- **Description**: DF-write cross-engine tests only use the SQLite writer. Postgres and MySQL writer interop with DuckDB is untested.
- **Suggested fix**: Add feature-gated cross-engine tests for Postgres and MySQL backends.

### IO-017: Parquet writer uses v2.0, DuckDB defaults to v1.0
- **Severity**: P3
- **Files**: `src/table_writer.rs:176`, `src/delete_exec.rs:346`, `src/update_exec.rs:411,464`, `src/merge_exec.rs:487,567`
- **Description**: All Parquet files are written with `WriterVersion::PARQUET_2_0`. DuckDB writes Parquet 1.0 by default. DuckDB can read both versions correctly. Informational.
- **Impact**: No functional issue.

## Previously Known (Deferred)

These were identified in R3/R4 and are not re-reported:
- F-036, F-044, F-045, R4-S-018, R4-S-036, R4-S-040
- I-R3-06: Extra columns in catalog tables (still present, low risk)
- I-R3-07: Extra non-standard tables (still present, low risk)

## R3 Finding Status Check

| R3 Finding | Status in R5 |
|------------|-------------|
| I-R3-01 (P0): Missing table_column_stats | **FIXED** — `register_column_stats` now computes aggregates |
| I-R3-02 (P1): create_snapshot schema_version | **FIXED** — inherits from MAX(schema_version) |
| I-R3-03 (P1): next_catalog_id/next_file_id | **FIXED** — computed in create_snapshot and write_transaction |
| I-R3-04 (P2): DML changes_made | **FIXED** — all DML ops record snapshot_changes |
| I-R3-05 (P2): created_schema tracking | **FIXED** — schema creation recorded |
| I-R3-08 (P3): Decimal spacing | **NOT VERIFIED** — not re-checked in R5 |

## Priority Summary

| Priority | Count | Findings |
|----------|-------|----------|
| P0 | 0 | — |
| P1 | 4 | IO-001 (DML cross-engine tests), IO-003 (ALTER TABLE tests), IO-004 (partition tests), IO-005 (type tests) |
| P2 | 7 | IO-002 (schema_version type), IO-006 (Date inlined), IO-007 (Timestamp inlined), IO-008 (Decimal inlined flush), IO-009 (Decimal stats), IO-010 (format column), IO-011 (change_tracking blind) |
| P3 | 6 | IO-012 (UUID v4 vs v7), IO-013 (MERGE token), IO-014 (MySQL Y2038), IO-015 (MySQL VARCHAR), IO-016 (Postgres/MySQL tests), IO-017 (Parquet v2) |

## Recommended Fix Priority

1. **IO-001/003/004/005** (P1): Add comprehensive cross-engine test coverage for DML, ALTER TABLE, partitions, and complex types
2. **IO-006/007** (P2): Fix inlined data Date/Timestamp serialization to use ISO 8601 strings
3. **IO-008** (P2): Add Decimal support to inlined data flush path
4. **IO-009** (P2): Fix Decimal column stats extraction from Parquet FixedLenByteArray
5. **IO-010** (P2): Add `format` column to `register_dml_files` DELETE INSERT
6. **IO-002** (P2): Change schema_version to BIGINT in Postgres/MySQL DDL
7. **IO-011** (P2): Enhance conflict detection to handle DuckDB-side changes
