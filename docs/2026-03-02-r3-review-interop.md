# R3 Interop Review — 2026-03-02

## Methodology

1. Created DuckDB reference catalogs using DuckLake extension (v3f1b372, DuckDB v1.4.4)
2. Compared every catalog table column-by-column against our SQLite/Postgres/MySQL writer DDL
3. Ran actual cross-engine tests: `cargo test --features "write-sqlite,metadata-duckdb,metadata-sqlite" --test cross_engine_tests`
4. Manually created catalogs with our DDL and attempted DuckDB reads
5. Verified previous R2 fix status for F-010 through F-027

## Cross-Engine Test Results

| Test | Result | Notes |
|------|--------|-------|
| `cross_engine_df_write_df_read` | PASS | DF→DF works |
| `cross_engine_df_write_duckdb_read` | **FAIL** | DuckDB assertion failure: NULL in GetGlobalTableStats |
| `cross_engine_duckdb_write_df_read` | PASS | DuckDB→DF works |
| `cross_engine_bidirectional_roundtrip` | PASS | |
| `cross_engine_assert_query_eq_both_engines` | PASS | |
| `cross_engine_null_handling` | **FAIL** | Same DuckDB assertion failure |
| `cross_engine_count_query` | PASS | |

**2 of 7 cross-engine tests fail.** Both failures are DF→DuckDB direction where DuckDB crashes reading DF-created catalogs.

## Findings

### I-R3-01 (P0): Missing `ducklake_table_column_stats` causes DuckDB crash

- **Files**: `src/table_writer.rs`, all `metadata_writer_*.rs`
- **Description**: DuckDB's `GetGlobalTableStats` queries `ducklake_table_column_stats` and crashes with `INTERNAL Error: Calling GetValueInternal on a value that is NULL` when the table has no rows or NULL values. Our writer never populates this table during normal INSERT operations — only `ducklake_file_column_stats` (per-file stats) is populated. DuckDB expects aggregate table-level stats in `ducklake_table_column_stats`.
- **Impact**: **All DF-created catalogs are unreadable by DuckDB.** This is a complete interop blocker.
- **Root cause**: `table_writer.rs` writes per-file stats to `ducklake_file_column_stats` but has no code to compute or write aggregate stats to `ducklake_table_column_stats`. The only code touching this table is in ALTER TABLE ADD COLUMN (F-047 fix), which inserts a stub row with NULL min/max.
- **Suggested fix**: After `register_column_stats`, compute and write/update aggregate stats into `ducklake_table_column_stats` for each column. Must include `contains_null` (non-NULL), `min_value`, and `max_value`. DuckDB reference shows `contains_null=false, min_value='1', max_value='3'` etc.
- **Effort**: M
- **Priority**: P0 — blocks all DF→DuckDB interop

### I-R3-02 (P1): `create_snapshot()` doesn't inherit `schema_version`

- **File**: `src/metadata_writer_sqlite.rs:770-779` (and Postgres/MySQL equivalents)
- **Description**: The standalone `create_snapshot()` method relies on DDL default `schema_version INTEGER DEFAULT 1`. DuckDB snapshots inherit `schema_version` from the previous snapshot (e.g., all DML snapshots after DDL with schema_version=1 should have schema_version=1, not the DDL default). Our `begin_write_transaction` paths handle this correctly, but `create_snapshot()` (used by DELETE, UPDATE, MERGE exec) does not.
- **Impact**: DML snapshots may have incorrect schema_version values. DuckDB uses schema_version for catalog version resolution and may fail to resolve schemas correctly.
- **Suggested fix**: In `create_snapshot()`, query `MAX(schema_version) FROM ducklake_snapshot` and use that value instead of relying on the default.
- **Effort**: S

### I-R3-03 (P1): `next_catalog_id` and `next_file_id` never populated in snapshots

- **File**: All `metadata_writer_*.rs`
- **Description**: Only the initial snapshot 0 has `next_catalog_id=0, next_file_id=0`. All subsequent snapshots use the DDL default of 0. DuckDB uses these fields for ID allocation — `next_catalog_id` tracks the next available catalog object ID (schema_id, table_id) and `next_file_id` tracks the next data_file_id.
- **Impact**: If DuckDB opens a DF-created catalog and tries to write, it may allocate IDs starting from 0, conflicting with existing IDs. Also used for catalog validation.
- **Suggested fix**: Track and update `next_catalog_id` and `next_file_id` in each snapshot. `next_catalog_id` should be MAX(schema_id, table_id, view_id) + 1, `next_file_id` should be MAX(data_file_id, delete_file_id) + 1.
- **Effort**: M

### I-R3-04 (P2): Missing DML `changes_made` in `ducklake_snapshot_changes`

- **Files**: `src/insert_exec.rs`, `src/delete_exec.rs`, `src/update_exec.rs`, `src/merge_exec.rs`
- **Description**: DML operations (INSERT, DELETE, UPDATE, MERGE) don't record `changes_made` entries. DuckDB records `inserted_into_table:1`, `deleted_from_table:1` etc. for every DML snapshot.
- **Impact**: DuckDB's `ducklake_table_changes()` function may not work correctly for DF-created DML snapshots. Also affects audit trail.
- **Suggested fix**: After each DML operation, record the appropriate `changes_made` string:
  - INSERT: `inserted_into_table:{table_id}`
  - DELETE: `deleted_from_table:{table_id}`
  - UPDATE: `updated_table:{table_id}` (verify exact DuckDB format)
  - MERGE: `merged_into_table:{table_id}` (verify exact DuckDB format)
- **Effort**: S

### I-R3-05 (P2): Missing `created_schema` change tracking

- **Files**: `src/metadata_writer_sqlite.rs:781-825`, all writer backends
- **Description**: When a schema is created (either standalone via CREATE SCHEMA or as part of table creation), no `created_schema:"name"` entry is recorded in `snapshot_changes`. DuckDB creates a separate snapshot with `created_schema:"main"` for each schema creation. Our `begin_write_transaction` collapses schema+table creation into one snapshot and only records `created_table`.
- **Impact**: Audit trail incomplete. DuckDB's table_changes function may not properly track schema creation events.
- **Effort**: S

### I-R3-06 (P2): Extra columns in catalog tables

- **Files**: All `metadata_writer_*.rs` DDL sections
- **Description**: Our DDL includes columns not present in DuckDB's reference schema:
  - `ducklake_column`: `default_value_type VARCHAR`, `default_value_dialect VARCHAR` (2 extra)
  - `ducklake_data_file`: `partial_max INTEGER` (1 extra)
  - `ducklake_delete_file`: `partial_max INTEGER` (1 extra)
  - `ducklake_schema_versions`: `table_id INTEGER` (1 extra)
- **Impact**: Low risk — DuckDB ignores unknown columns when reading via SQLite. But may cause issues if DuckDB validates schema strictly in future versions.
- **Effort**: S (remove if not needed, or document as extensions)

### I-R3-07 (P2): Extra non-standard tables

- **Tables**: `_df_change_tracking`, `ducklake_file_variant_stats`, `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters`, `ducklake_sort_expression`, `ducklake_sort_info`
- **Description**: 7 tables in our DDL that don't exist in DuckDB v1.4.4's DuckLake extension. `_df_change_tracking` is DataFusion-specific. The `ducklake_macro*` and `ducklake_sort*` tables may be for future DuckLake versions.
- **Impact**: Low risk — DuckDB should ignore unknown tables. `_df_change_tracking` uses a non-`ducklake_` prefix which is fine.
- **Effort**: N/A (informational)

### I-R3-08 (P3): Decimal type string spacing

- **File**: `src/types.rs:159`
- **Description**: Our `arrow_to_ducklake_type` produces `"decimal(10, 2)"` (with space after comma). DuckDB stores `"decimal(10,2)"` (no space). Our parser handles both formats, but DuckDB may use exact string matching.
- **Impact**: Cosmetic — DuckDB's type parser also handles spaces. Very low risk.
- **Suggested fix**: Change format string from `"decimal({}, {})"` to `"decimal({},{})"`.
- **Effort**: S

## Verification of Previous R2 Fixes

### Fixed and Verified ✓

| Finding | Fix | Verification |
|---------|-----|-------------|
| F-010: Delete file format default | Changed to `'parquet'` | Confirmed in DDL line 106 |
| F-011: Missing row_id_start | Track from table_stats | Confirmed in register_data_file (line 980-1001) |
| F-012: Schema versions/table_stats | Populated on DDL and file registration | Confirmed: schema_versions populated on DDL, table_stats on register_data_file |
| F-013: Column ID preservation | Preserved in begin_write_transaction | Confirmed: column ID reuse logic present |
| F-025: file_format casing | Changed to `'parquet'` (lowercase) | Confirmed in DDL line 88 |
| F-026: UUID generation | uuid::Uuid::new_v4() | Confirmed in schema/table creation (lines 420, 445) |
| F-027: changes_made format | Adopted DuckDB format | Confirmed: `created_table:"schema"."table"`, `dropped_table:id`, `altered_table:id` |
| F-037: Temporal type roundtrip | Added unit-specific type strings | Confirmed: `time_s`, `time_ms`, `time_ns`, `timestamptz_s/ms/ns` |
| F-047: encrypted=false, snapshot_time | Added metadata key, UTC format | Confirmed: `encrypted=false` in init, `strftime('%Y-%m-%d %H:%M:%f+00:00')` |

### Fixes Introducing New Gaps

| Finding | Fix Status | New Gap |
|---------|-----------|---------|
| F-012: table_stats | Partially fixed | `ducklake_table_column_stats` not populated → I-R3-01 (DuckDB crash) |
| F-027: changes_made | DDL only | DML operations missing changes_made → I-R3-04 |

## Schema Comparison Summary

### Tables Present in Both (22/22 DuckDB tables matched)

All 22 DuckDB reference tables exist in our DDL. No missing tables.

### Column Comparison (Critical Tables)

| Table | DuckDB Cols | Our Cols | Extra in Ours | Notes |
|-------|-------------|----------|---------------|-------|
| ducklake_snapshot | 5 | 5 | 0 | Match ✓ |
| ducklake_schema | 7 | 7 | 0 | Match ✓ |
| ducklake_table | 8 | 8 | 0 | Match ✓ |
| ducklake_column | 11 | 13 | 2 | `default_value_type`, `default_value_dialect` |
| ducklake_data_file | 16 | 17 | 1 | `partial_max` |
| ducklake_delete_file | 12 | 13 | 1 | `partial_max` |
| ducklake_schema_versions | 2 | 3 | 1 | `table_id` |
| ducklake_snapshot_changes | 5 | 5 | 0 | Match ✓ |
| ducklake_file_column_stats | 10 | 10 | 0 | Match ✓ |
| ducklake_table_stats | 4 | 4 | 0 | Match ✓ |
| ducklake_table_column_stats | 7 | 7 | 0 | Match ✓ |
| ducklake_view | 9 | 9 | 0 | Match ✓ |
| ducklake_metadata | 4 | 4 | 0 | Match ✓ |

### Type System Roundtrip

All standard types roundtrip correctly between DuckDB and our type system:
- `boolean`, `int8`..`int64`, `uint8`..`uint64` ✓
- `float32`, `float64` ✓
- `varchar`, `blob`, `uuid` ✓
- `date`, `timestamp`, `timestamptz` ✓
- `decimal(p,s)` ✓ (minor spacing difference, functionally equivalent)
- `time`, `interval` ✓

### Metadata Values

| Key | DuckDB | Ours | Match |
|-----|--------|------|-------|
| version | 0.3 | 0.3 | ✓ |
| data_path | file:///path.db.files/ | file:///path/ | ✓ (different path, same format) |
| created_by | DuckDB 6ddac802ff | DataFusion-DuckLake | ✓ (expected difference) |
| encrypted | false | false | ✓ |

## Priority Summary

| Priority | Count | Findings |
|----------|-------|----------|
| P0 | 1 | I-R3-01 (table_column_stats crash) |
| P1 | 2 | I-R3-02 (schema_version), I-R3-03 (next_catalog_id/next_file_id) |
| P2 | 3 | I-R3-04 (DML changes_made), I-R3-05 (created_schema), I-R3-06 (extra columns) |
| P3 | 1 | I-R3-08 (decimal spacing) |
| Info | 1 | I-R3-07 (extra tables) |

## Recommended Fix Priority

1. **I-R3-01** (P0): Populate `ducklake_table_column_stats` on INSERT — unblocks all DF→DuckDB reads
2. **I-R3-02** (P1): Fix `create_snapshot()` to inherit schema_version
3. **I-R3-03** (P1): Track and populate `next_catalog_id`/`next_file_id` in snapshots
4. **I-R3-04** (P2): Add DML changes_made entries
5. **I-R3-05** (P2): Add created_schema change tracking
6. **I-R3-08** (P3): Fix decimal spacing (trivial)
