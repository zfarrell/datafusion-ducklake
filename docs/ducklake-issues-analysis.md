# DuckLake GitHub Issues Analysis

Analysis of DuckLake extension issues at https://github.com/duckdb/ducklake and their relevance to our DataFusion-DuckLake implementation.

**Date**: 2026-02-22
**DuckLake Version**: 0.4-dev (current HEAD)
**Open Issues**: ~74

---

## Table of Contents

1. [Critical Issues Affecting Our Implementation](#critical-issues)
2. [Schema / Catalog Format Changes](#schema-changes)
3. [Delete File Handling Issues](#delete-file-issues)
4. [Type System Issues](#type-issues)
5. [Concurrency / Transaction Issues](#concurrency-issues)
6. [Performance Issues](#performance-issues)
7. [MetadataWriter Compatibility Concerns](#metadata-writer-concerns)
8. [DuckLake Test Patterns We Should Mirror](#test-patterns)
9. [Recommended Actions](#recommendations)

---

## 1. Critical Issues Affecting Our Implementation <a name="critical-issues"></a>

### Issue #457: Automatic Version Migration is a Breaking Change
- **Status**: Open
- **Impact**: HIGH
- **URL**: https://github.com/duckdb/ducklake/issues/457
- **Summary**: DuckLake automatically migrates metadata schema when a newer client connects (e.g., 0.2 -> 0.3). Once migrated, older clients are blocked by strict version checks.
- **Relevance to us**: Our `MetadataWriter` implementations create tables at the current schema version. If DuckLake changes its schema in 0.4+, catalogs created by our extension could be incompatible with older DuckLake clients, or vice versa. We need to:
  - Track the `ducklake_metadata` table's version key
  - Write the correct version string to maintain compatibility
  - Test against multiple DuckLake versions

### Issue #683: Transactional DDL Puts Catalog in Unusable State
- **Status**: Closed (fixed in PR #714)
- **Impact**: MEDIUM
- **URL**: https://github.com/duckdb/ducklake/issues/683
- **Summary**: Multiple ALTER TABLE statements in a single transaction can create duplicate column metadata, causing `"Column with name id already exists!"` errors.
- **Relevance to us**: Our `MetadataWriter` ALTER TABLE implementations should avoid modifying the same column multiple times in a single transaction. Fixed upstream, but we should ensure our schema evolution code doesn't hit the same pattern.

### Issue #625: column_stats Not Updated After ALTER TABLE
- **Status**: Open (reproduced)
- **Impact**: MEDIUM
- **URL**: https://github.com/duckdb/ducklake/issues/625
- **Summary**: `ducklake_table_column_stats` is not updated when columns are added via ALTER TABLE.
- **Relevance to us**: Our `MetadataWriter` should ensure column stats are properly initialized for new columns. Currently, our add_column implementations may not populate `ducklake_table_column_stats` either.

### Issue #328: Catalog Database Spontaneously Detaches (ducklake_table_stats missing)
- **Status**: Closed (fixed)
- **URL**: https://github.com/duckdb/ducklake/issues/328
- **Summary**: After large write operations, the catalog detaches with "Table with name ducklake_table_stats does not exist!" or "ducklake_data_file does not exist!".
- **Relevance to us**: Confirms `ducklake_table_stats` is a critical table. Our MetadataWriter implementations create it correctly, but we should ensure our write transactions are properly scoped to avoid similar state corruption.

---

## 2. Schema / Catalog Format Changes <a name="schema-changes"></a>

### Complete DuckLake Catalog Schema (v0.4-dev)

From `ducklake_metadata_manager.cpp` lines 131-158, the canonical schema includes **27 tables**:

| Table | Present in our MetadataWriter? | Present in our MetadataProvider? |
|-------|-------------------------------|--------------------------------|
| `ducklake_metadata` | Yes | Yes |
| `ducklake_snapshot` | Yes | Yes |
| `ducklake_snapshot_changes` | Yes | No (not needed for reads) |
| `ducklake_schema` | Yes | Yes |
| `ducklake_table` | Yes | Yes |
| `ducklake_view` | Yes | Yes |
| `ducklake_tag` | Yes | No |
| `ducklake_column_tag` | Yes | No |
| `ducklake_data_file` | Yes | Yes |
| `ducklake_file_column_stats` | Yes | No (not needed yet) |
| `ducklake_file_variant_stats` | Yes | No |
| `ducklake_delete_file` | Yes | Yes |
| `ducklake_column` | Yes | Yes |
| `ducklake_table_stats` | Yes | No (not queried for reads) |
| `ducklake_table_column_stats` | Yes | No |
| `ducklake_partition_info` | Yes | No (future: partition pruning) |
| `ducklake_partition_column` | Yes | No |
| `ducklake_file_partition_value` | Yes | No |
| `ducklake_files_scheduled_for_deletion` | Yes | No |
| `ducklake_inlined_data_tables` | Yes | No |
| `ducklake_column_mapping` | Yes | No (future) |
| `ducklake_name_mapping` | Yes | No (future) |
| `ducklake_schema_versions` | Yes | No |
| `ducklake_macro` | Yes | No |
| `ducklake_macro_impl` | Yes | No |
| `ducklake_macro_parameters` | Yes | No |
| `ducklake_sort_info` | Yes | No |
| `ducklake_sort_expression` | Yes | No |

### Key Column Differences to Watch

1. **`ducklake_data_file.partial_max`** (upstream) vs **previously `partial_file_info`** (spec docs): The upstream C++ code uses `partial_max BIGINT`. Our MetadataWriter implementations correctly use `partial_max`. The online docs may refer to it as `partial_file_info VARCHAR` which appears to be outdated.

2. **`ducklake_column.default_value_type`** and **`ducklake_column.default_value_dialect`**: Added in v0.3+ migrations. Our MetadataWriter implementations correctly include these columns.

3. **`ducklake_view`** columns include `dialect` and `column_aliases` in the upstream schema. Our writer implementations should verify they include all columns.

4. **`ducklake_name_mapping.is_partition`**: Added as a new column in a later version. Our writers include it. The upstream migration code at line 181 shows an older version without `is_partition`, suggesting this was added in v0.4-dev.

---

## 3. Delete File Handling Issues <a name="delete-file-issues"></a>

### Multi-Delete Consolidation (test: `multi_deletes.test`)
- DuckLake consolidates multiple delete files from the same transaction into a single file.
- After commit, only one delete file should exist per data file.
- Old delete files get their `end_snapshot` set and are scheduled for deletion.
- **Relevance**: Our `DeleteFilterExec` should handle the case where a data file has been modified by multiple delete operations across snapshots. The metadata provider correctly filters by snapshot range.

### Delete File Schema
- Delete files contain `(file_path: VARCHAR, pos: INT64)` columns.
- DuckLake test `delete_ignore_extra_columns.test` confirms delete files may contain extra columns beyond the standard schema - the reader should ignore them.
- **Relevance**: Our `DeleteFilterExec` in `delete_filter.rs` correctly handles the standard schema. We should test with delete files that have extra columns to ensure robustness.

### Issue #586: Cleanup of Delete Files with External Postgres
- **Status**: Open
- **URL**: https://github.com/duckdb/ducklake/issues/586
- **Summary**: `ducklake_cleanup_old_files()` doesn't work correctly with external Postgres catalogs.
- **Relevance**: Low - we don't implement cleanup, but shows that Postgres-backed catalogs have edge cases we should be aware of.

---

## 4. Type System Issues <a name="type-issues"></a>

### Supported Types (from `all_types.test`)
DuckLake excludes these types that DuckDB supports:
- `BIGNUM`, `BIT`, enums (`small_enum`, `medium_enum`, `large_enum`)
- `UNION` type
- Fixed-size arrays (`fixed_int_array`, `fixed_varchar_array`, etc.)
- `HUGEINT`, `UHUGEINT`
- `INTERVAL`, `TIME WITH TIME ZONE`, `TIME_NS` (nanosecond precision time)

### Types DuckLake Does Support
- All standard integer types, floats, decimals
- VARCHAR, BLOB, DATE, TIMESTAMP, TIMESTAMPTZ
- LIST, STRUCT, MAP (complex types)
- JSON, VARIANT (new in recent versions)
- **Relevance**: Our `types.rs` type mapping should handle all of these. We currently return errors for complex types (LIST, STRUCT, MAP) which is fine for now but limits compatibility with catalogs using these types.

### Issue #619: Long Column Names (>64 chars) Break Postgres Inlining
- **Status**: Open (reproduced)
- **URL**: https://github.com/duckdb/ducklake/issues/619
- **Summary**: PostgreSQL's 63-byte identifier limit truncates column names during data inlining.
- **Relevance**: Our PostgreSQL MetadataWriter should be aware of this limitation. Column names in `ducklake_column` are stored as VARCHAR, not as identifiers, so the metadata itself is fine. But if we ever implement inlining, we'll hit this.

### Issue #790: Binary Data Inlining Fails with Postgres
- **Status**: Open
- **URL**: https://github.com/duckdb/ducklake/issues/790
- **Summary**: Data inlining for binary columns fails with Postgres catalogs.
- **Relevance**: Low for now since we don't implement inlining, but flagged for future reference.

---

## 5. Concurrency / Transaction Issues <a name="concurrency-issues"></a>

### Issue #243: Concurrent Writes Fail on First Write
- **Status**: Open
- **URL**: https://github.com/duckdb/ducklake/issues/243
- **Summary**: Concurrent writes to a freshly created catalog fail with PK constraint violation on `ducklake_data_file` instead of proper serialization error.
- **Relevance**: Our MetadataWriter uses sequential ID assignment (MAX(id) + 1 pattern). Under concurrent writes, this could produce duplicate IDs. We should use database-level sequences or atomic increment patterns where possible.

### Issue #650: Race Condition in ducklake_flush_inlined_data
- **Status**: Open
- **URL**: https://github.com/duckdb/ducklake/issues/650
- **Summary**: Concurrent flush and insert operations cause data duplication (~60% reproduction rate).
- **Relevance**: We don't implement inlining, so not directly relevant. But confirms that concurrent metadata operations are a known weak area in DuckLake.

### Issue #740: Column Rename + Add in Transaction Breaks Inlining
- **Status**: Open (reproduced)
- **URL**: https://github.com/duckdb/ducklake/issues/740
- **Summary**: Adding and renaming a column within the same transaction causes stale column references.
- **Relevance**: Our MetadataWriter's ALTER TABLE operations should be tested for multi-step schema changes within transactions. Our current implementation handles individual operations but hasn't been tested for compound operations.

---

## 6. Performance Issues <a name="performance-issues"></a>

### Issue #584: SELECT COUNT(*) Makes Unnecessary HTTP Requests
- **Status**: Open
- **URL**: https://github.com/duckdb/ducklake/issues/584
- **Summary**: COUNT(*) triggers full Parquet scans instead of using metadata.
- **Relevance**: Our `DeleteFilterExec` has COUNT(*) optimization (zero-column batches). For tables without deletes, DataFusion can potentially optimize using file metadata (`record_count` from `ducklake_data_file`), but we don't implement this optimization yet.

### Issue #745: LIMIT Ignores Partition Pruning
- **Status**: Open
- **URL**: https://github.com/duckdb/ducklake/issues/745
- **Summary**: Adding LIMIT to queries on partitioned tables bypasses partition pruning due to late materialization optimization.
- **Relevance**: DataFusion has its own optimizer, so we may not hit this exact bug. But if we implement partition-based file pruning in the future, we should test with LIMIT.

### Issue #572: Metadata Catalog Size Inflation
- **Status**: Open
- **URL**: https://github.com/duckdb/ducklake/issues/572
- **Summary**: Repeatedly opening/closing DuckDB connections inflates catalog to 100x+ its actual data size.
- **Relevance**: Our DuckDB MetadataProvider uses a single shared connection (protected by Mutex), which avoids this issue. However, if users create many short-lived `DuckLakeCatalog` instances, they could trigger catalog inflation. Document best practice of reusing catalog instances.

### Issue #788: duckdb_views() ~70x Slower Than duckdb_tables()
- **Status**: Open
- **URL**: https://github.com/duckdb/ducklake/issues/788
- **Summary**: View listing performance is significantly worse than table listing.
- **Relevance**: Our view listing uses similar SQL patterns. If view counts are high, our `list_views()` may also be slow.

---

## 7. MetadataWriter Compatibility Concerns <a name="metadata-writer-concerns"></a>

### Version String Tracking
The `ducklake_metadata` table must contain the correct version string. The DuckLake initializer checks:
```
"Only DuckLake versions 0.1, 0.2, 0.3-dev1, 0.3, 0.4-dev1 and 0.4 are supported"
```
Our MetadataWriter implementations should write the version string that matches the current catalog schema we create.

### Migration Versions
From `migration.test`, DuckLake tests migration from: `v01`, `v02`, `v03-dev1`, `v03`, `v04-dev1`. Each version may have different table schemas.

Key migrations:
- **v0.2 -> v0.3**: Added `ducklake_column.default_value_type`, `ducklake_column.default_value_dialect`
- **v0.3 -> v0.4-dev1**: Added `ducklake_schema_versions.table_id`, `ducklake_name_mapping.is_partition`, `ducklake_macro_parameters.default_value_type`

### MySQL-Specific: Issue #210
- `ducklake_cleanup_old_files()` fails with MySQL because of RIGHT_DELIM_JOIN in DELETE.
- **Relevance**: Our MySQL MetadataWriter should use simpler SQL patterns to avoid MySQL-specific limitations.

---

## 8. DuckLake Test Patterns We Should Mirror <a name="test-patterns"></a>

### Test Directory Structure (`/home/zac/ducklake/test/sql/`)

Key test categories and what they cover:

| Directory | Tests | Relevance |
|-----------|-------|-----------|
| `delete/` | 11 tests including multi_deletes, delete rollback, truncate | HIGH - test our DeleteFilterExec |
| `alter/` | 25 tests including add/drop/rename columns, struct evolution | HIGH - test our MetadataWriter ALTER TABLE |
| `types/` | 11 tests including all_types, JSON, list, struct, map, variant | HIGH - test our type mapping |
| `partitioning/` | 11 tests including multi-key, year/month/day transforms | MEDIUM - future partition pruning |
| `view/` | 8 tests including rename, schema, table conflict | MEDIUM - test our view support |
| `migration/` | 4 tests for version upgrade paths | HIGH - ensure our catalog creation is compatible |
| `virtualcolumns/` | 2 tests for filename, file_row_number, snapshot_id | MEDIUM - test our virtual column support |
| `schema_evolution/` | 1 test for Parquet field IDs | MEDIUM - schema evolution compatibility |
| `concurrent/` | Tests for thread safety | MEDIUM - test our concurrent access |
| `data_inlining/` | Tests for inline data | LOW - we don't implement inlining reads |
| `issues/` | 1 test for late materialization bug | LOW - DataFusion-specific |

### Key Test Patterns to Adopt

1. **Multi-delete consolidation** (`multi_deletes.test`): Test that our delete filtering works correctly when a data file has had multiple rounds of deletes across different snapshots.

2. **Delete with time travel** (`basic_delete.test`): Test reading tables at different snapshots - some with deletes, some without.

3. **Virtual column filtering** (`ducklake_virtual_columns.test`): Test `WHERE file_row_number=1` and `WHERE contains(filename, 'path')`.

4. **Snapshot ID virtual column** (`ducklake_snapshot_id.test`): Test that snapshot_id is NULL for uncommitted data and set correctly for committed files.

5. **All types roundtrip** (`all_types.test`): Create a table with every supported type, write data, read it back.

6. **Schema evolution with field IDs** (`field_ids.test`): Parquet field IDs must match column_id from `ducklake_column`. This is critical for schema evolution correctness.

---

## 9. Recommended Actions <a name="recommendations"></a>

### Immediate (Bugs/Correctness)

1. **Verify version string compatibility**: Ensure our MetadataWriter writes a version string (in `ducklake_metadata`) that DuckLake 0.4 can read. Test bi-directional: create catalog with our extension, read with DuckLake.

2. **Test multi-delete scenarios**: Add tests for data files with multiple delete operations across snapshots to ensure our `DeleteFilterExec` handles consolidated delete files correctly.

3. **Test ALTER TABLE + column stats**: Verify our `add_column` MetadataWriter implementations properly initialize `ducklake_table_column_stats` entries for new columns (upstream bug #625 suggests this is commonly missed).

4. **Concurrent ID assignment**: Our MetadataWriter uses `MAX(id) + 1` for generating IDs. Under concurrent writes, this can produce duplicates (upstream issue #243). Consider using database sequences (Postgres) or proper locking.

### Medium-Term (Compatibility)

5. **Add partition pruning**: DuckLake stores partition info in `ducklake_partition_info`, `ducklake_partition_column`, and `ducklake_file_partition_value`. Our MetadataProvider should expose partition info for file pruning.

6. **Support complex types**: Our `types.rs` currently returns errors for LIST, STRUCT, MAP. These are fully supported by DuckLake and common in real catalogs.

7. **Handle schema evolution / field IDs**: DuckLake uses `column_id` as Parquet field IDs. When columns are added/dropped, field IDs remain stable. Our Parquet reading should use field ID matching, not column name or position.

8. **Handle extra columns in delete files**: DuckLake test `delete_ignore_extra_columns.test` shows delete files may have extra columns. Our `DeleteFilterExec` should gracefully handle this.

### Long-Term (Robustness)

9. **Migration compatibility testing**: Test our MetadataWriter-created catalogs against each DuckLake version (v0.1 through v0.4).

10. **COUNT(*) metadata optimization**: Use `ducklake_table_stats.record_count` to optimize COUNT(*) queries on tables without delete files, avoiding full Parquet scans.

11. **Column mapping support**: DuckLake's `ducklake_column_mapping` and `ducklake_name_mapping` tables enable Parquet column name remapping. Needed for full schema evolution support.

---

## Summary of Most Impactful Issues

| Priority | Issue | Our Risk |
|----------|-------|----------|
| P0 | Version compatibility (#457) | Catalogs we create may not be readable by DuckLake |
| P0 | Concurrent ID assignment (#243) | Our MetadataWriter can produce duplicate IDs |
| P1 | Column stats after ALTER (#625) | Schema changes may leave incomplete metadata |
| P1 | Multi-delete handling | Our DeleteFilterExec needs more comprehensive testing |
| P1 | Schema evolution field IDs | Column ID mismatch after schema changes can corrupt reads |
| P2 | Complex type support | Limits catalog compatibility |
| P2 | Partition pruning | Performance gap for partitioned tables |
| P2 | Extra columns in delete files | Edge case that could cause read failures |
