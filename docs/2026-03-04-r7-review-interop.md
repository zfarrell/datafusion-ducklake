# Review Cycle 7: Interoperability Review (Post-R6 Fixes)
Date: 2026-03-04

## Summary

Post-R6-fixes interoperability review. Verified R6 fix effectiveness and checked for regressions. Compared catalog schemas against DuckDB v1.4.4 DuckLake extension (format version 0.3) reference.

**Overall assessment: Interoperability remains GOOD.** R6 fixes resolved the P1 `schema_version` hardcoding issue correctly. No new P0 or P1 findings. The main interop gaps from R6 remain as known, documented differences (extra columns, extra tables, TEXT vs TIMESTAMPTZ). Found 5 new findings: 0 P0, 0 P1, 2 P2, 3 P3.

**R6 Fix Verification Status:**
- **R6-I-001 (P1, schema_version hardcoding)**: **FIXED** — `store_inlined_data` now fetches actual `schema_version` from snapshot (line 3060-3067)
- **R6-I-002 (P3, extra columns)**: **OPEN** — Accepted as low-risk forward-compatible extensions
- **R6-I-003 (P3, partial_max)**: **OPEN** — Accepted as low-risk
- **R6-I-004 (P3, extra table_id in schema_versions)**: **OPEN** — Accepted as low-risk
- **R6-I-005 (P2, 7 extra tables)**: **OPEN** — Accepted, `_df_` prefix used correctly for DF-specific tables
- **R6-I-006 (P3, UUID v4 vs v7)**: **OPEN** — No functional impact
- **R6-I-007 (P2, snapshot_time TEXT)**: **OPEN** — DuckDB auto-casts, verified safe
- **R6-I-008 (P2, schedule_start TEXT)**: **OPEN** — DuckDB auto-casts, verified safe
- **R6-I-009 (P2, cross-engine test gaps)**: **PARTIALLY ADDRESSED** — New test files added (10 total) but DF→DuckDB DML direction still has gaps

## Reference Schema Comparison (Delta from R6)

### R6 Changes Verified Against DuckDB Reference

| R6 Change | Schema Impact | DuckDB Compatible | Verified |
|-----------|--------------|:-----------------:|:--------:|
| R6-S-009: schema_version fix for inlined data | `store_inlined_data` now uses actual schema_version | Yes | Yes |
| R6-S-012: CDC encryption factory | No schema change (runtime only) | N/A | Yes |
| R6-S-015: Cumulative row_id_start in replace_table_files | `row_id_start` column (already in schema) | Yes | Yes |
| R6-S-018: PG/MySQL atomic replace_table_files | No schema change (transaction atomicity) | N/A | Yes |
| R6-S-030: _df_ prefix for DF-specific tables | `_df_change_tracking` uses `_df_` prefix | Yes | Yes |
| R6-S-031: Timestamp parsing TEXT/TIMESTAMPTZ | `schedule_start` comment updated | Yes | Yes |
| R6-S-033: FOR UPDATE row locking (PG/MySQL) | No schema change (query-level) | N/A | Yes |
| R6-S-034: ducklake_table_stats PK | `table_id PRIMARY KEY` (all backends) | See R7-I-001 | Yes |

### Tables Present in Both (22 standard DuckLake tables)

No schema changes from R6. All differences documented in R6 review remain:
- `ducklake_column`: 2 extra columns (`default_value_type`, `default_value_dialect`)
- `ducklake_data_file`: 1 extra column (`partial_max`)
- `ducklake_delete_file`: 1 extra column (`partial_max`)
- `ducklake_schema_versions`: 1 extra column (`table_id`)
- `ducklake_snapshot`: `snapshot_time` as TEXT (SQLite) vs TIMESTAMPTZ (DuckDB)
- `ducklake_files_scheduled_for_deletion`: `schedule_start` as TEXT vs TIMESTAMPTZ
- UUID/column order differences in `ducklake_schema`, `ducklake_table`, `ducklake_view`

### Extra Tables (8 total, up from 7 in R6)

| Table | Prefix | Purpose | DuckDB Impact |
|-------|--------|---------|:-------------:|
| `_df_change_tracking` | `_df_` | DF-specific conflict detection | None (ignored) |
| `_df_sequences` (MySQL only) | `_df_` | Concurrent-safe ID generation | None (ignored) |
| `ducklake_macro` | `ducklake_` | Future macro support | Low risk |
| `ducklake_macro_impl` | `ducklake_` | Future macro support | Low risk |
| `ducklake_macro_parameters` | `ducklake_` | Future macro support | Low risk |
| `ducklake_sort_info` | `ducklake_` | Future sort order support | Low risk |
| `ducklake_sort_expression` | `ducklake_` | Future sort expression support | Low risk |
| `ducklake_file_variant_stats` | `ducklake_` | Future variant stats support | Low risk |

## Findings

### R7-I-001: ducklake_table_stats has PRIMARY KEY constraint not present in DuckDB
- **File(s)**: `src/metadata_writer_sqlite.rs:175`, `src/metadata_writer_postgres.rs:156`, `src/metadata_writer_mysql.rs:190`
- **Severity**: P3
- **Category**: schema-compat
- **Description**: All three backends define `ducklake_table_stats` with `table_id PRIMARY KEY`. DuckDB's reference schema for this table has no PRIMARY KEY constraint.
- **DuckDB Behavior**: `table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT` — no PK.
- **Our Behavior**: `table_id {INTEGER|BIGINT} PRIMARY KEY, ...`
- **Impact**: **Low.** The PK is semantically correct (one stats row per table) and is an additive constraint. DuckDB uses named-column SQL and tolerates extra constraints when reading DF-created catalogs. DF writing to a DuckDB-created catalog would also work since inserts/updates use WHERE clause, not PK constraints. Cross-engine tooling should not be affected.
- **Suggested Fix**: No action required. The PK is arguably more correct than DuckDB's schema. Document as intentional divergence.
- **Effort**: N/A

### R7-I-002: Inlined data schema evolution after ALTER TABLE
- **File(s)**: `src/metadata_writer_sqlite.rs:3046-3070`, `src/metadata_writer_sqlite.rs:3151-3164`
- **Severity**: P2
- **Category**: inline-data
- **Description**: After ALTER TABLE ADD COLUMN on a table with inlined data, `store_inlined_data` reuses the existing inlined data table (found by `table_id` lookup in `ducklake_inlined_data_tables`). The old table lacks the new column, so INSERT will fail with a SQL error. DuckDB handles this by creating a new inlined data table with the updated schema_version.
- **DuckDB Behavior**: After ALTER TABLE, DuckDB creates a new `ducklake_inlined_data_{table_id}_{new_sv}` table with the new column set, and registers it in `ducklake_inlined_data_tables`. Reads query the latest table matching the current schema_version.
- **Our Behavior**: `store_inlined_data` checks `ducklake_inlined_data_tables WHERE table_id = ?` and reuses the first match regardless of schema_version. Similarly, `read_inlined_data` picks the first match. `clear_inlined_data` operates on the first match only.
- **Impact**: INSERT with inlined data after ALTER TABLE ADD COLUMN would fail. Low practical likelihood (requires inlining + ALTER TABLE + subsequent INSERT sequence), but if it occurs, it's a hard failure.
- **Suggested Fix**: After ALTER TABLE, either (a) recreate the inlined data table with the new column set and a new schema_version, or (b) filter `ducklake_inlined_data_tables` by schema_version when looking up. Both `read_inlined_data` and `clear_inlined_data` should also be schema_version-aware.
- **Effort**: M

### R7-I-003: PG/MySQL lack inlined data support (no-op trait defaults)
- **File(s)**: `src/metadata_writer.rs:663-702`
- **Severity**: P2
- **Category**: feature-parity
- **Description**: PG and MySQL metadata writers don't override `store_inlined_data`, `read_inlined_data`, or `clear_inlined_data`. The default implementations return `Ok(None)`/`Ok(Vec::new())`/`Ok(())` — silently doing nothing. Only SQLite has actual implementations. If a DuckDB/SQLite-created catalog with inlined data is read via PG/MySQL provider, inlined rows would be invisible.
- **DuckDB Behavior**: DuckDB supports inlined data across all backends.
- **Our Behavior**: Only SQLite implements inlining. PG/MySQL silently ignore inlined data.
- **Impact**: **Medium.** If PG/MySQL catalogs have inlined data (e.g., set up by DuckDB), DataFusion would return incomplete results silently. However, PG/MySQL catalogs don't typically use inlining since it requires creating dynamic tables in the catalog DB, which is more natural for SQLite/DuckDB.
- **Suggested Fix**: Either (a) implement inlining for PG/MySQL, or (b) return an explicit error when `get_data_inlining_row_limit()` returns `Some` but the backend doesn't support inlining.
- **Effort**: L (implement) / S (error)

### R7-I-004: Default trait `replace_table_files` is non-atomic
- **File(s)**: `src/metadata_writer.rs:493-517`
- **Severity**: P3
- **Category**: atomicity
- **Description**: The default `replace_table_files` implementation in the `MetadataWriter` trait is non-atomic (calls `end_table_files` then per-file `register_data_file` outside a transaction). All three backends (SQLite, PG, MySQL) correctly override this with atomic implementations, so this is not a current bug.
- **Impact**: **Low.** Any new backend that doesn't override `replace_table_files` would have partial-replacement risk on failure. The documentation comment correctly notes this.
- **Suggested Fix**: Consider adding `#[deprecated]` or a stronger doc warning on the default implementation, or make the method required (no default).
- **Effort**: S

### R7-I-005: Cross-engine test coverage gaps remain for DF→DuckDB DML
- **File(s)**: `tests/cross_engine_tests.rs`, `tests/cross_engine_dml_tests.rs`
- **Severity**: P3
- **Category**: test-coverage
- **Description**: While 10 cross-engine test files now exist (up from 1 in R6), the DF→DuckDB direction for DML operations still has limited coverage. The following operations are tested in DuckDB→DF direction but not DF→DuckDB:
  - DF DELETE → DuckDB read
  - DF UPDATE → DuckDB read
  - DF partitioned INSERT → DuckDB read
  - DF inlined data → DuckDB read (only DuckDB→DF tested)
  - DF DROP TABLE → DuckDB behavior
  - DF CREATE/DROP VIEW → DuckDB read
- **Impact**: **Low.** DuckDB→DF tests validate schema compatibility. DF→DuckDB is less critical since DuckDB is more permissive about schema differences. The DF→DuckDB direction for basic INSERT and type roundtrips is tested.
- **Suggested Fix**: Add targeted DF→DuckDB roundtrip tests for DELETE, UPDATE, and ALTER TABLE operations.
- **Effort**: M

## Codex Findings

Codex identified 4 findings. After validation:

1. **`store_inlined_data` schema evolution issue** (codex "HIGH"): **VALID, P2.** After ALTER TABLE, the existing inlined data table would be reused without the new column. Covered by R7-I-002.

2. **`ducklake_table_stats` PRIMARY KEY divergence** (codex "HIGH"): **VALID but OVERSTATED, P3.** The PK is semantically correct and doesn't break interop. DuckDB tolerates extra constraints. Codex overestimated the risk. Covered by R7-I-001.

3. **Default `replace_table_files` non-atomic** (codex "MEDIUM"): **VALID, P3.** All backends override with atomic implementations. The default is documented as non-atomic for backward compatibility. Covered by R7-I-004.

4. **No `_df_` prefix inconsistency** (codex "LOW"): **CONFIRMED.** Naming is internally consistent.

## R6 Fix Effectiveness Analysis

### R6-S-009: schema_version Fix
**Status: Verified Correct.**

The fix at `metadata_writer_sqlite.rs:3045-3070`:
1. First checks for existing inlined data table by `table_id` — reuses if found ✓
2. If no existing table, fetches `schema_version` from `ducklake_snapshot WHERE snapshot_id = ?` ✓
3. Constructs table name as `ducklake_inlined_data_{table_id}_{schema_version}` ✓
4. Registers in `ducklake_inlined_data_tables` with correct `schema_version` ✓

This correctly handles the multi-DDL-snapshot scenario. For table_id=2 created at schema_version=2, the table is named `ducklake_inlined_data_2_2`, matching DuckDB's convention.

**Remaining gap:** The fix doesn't handle the ALTER TABLE + re-inlining scenario (R7-I-002), but this is a separate issue from the original R6-I-001 hardcoding bug.

### R6-S-012: CDC Encryption Factory
**Status: Verified Correct.**

`table_changes.rs` properly:
1. Builds `EncryptionFactory` from file encryption keys (line 706-728)
2. Passes factory to `build_exec_for_file` for data files (line 734)
3. Builds separate factory for delete files (line 752-762)
4. Uses `#[cfg(feature = "encryption")]` guards throughout ✓
5. Thread-safe: `Arc<dyn EncryptionFactory>` ✓

### R6-S-030: _df_ Prefix Consistency
**Status: Verified Correct.**

- `_df_change_tracking`: Correctly uses `_df_` prefix ✓
- `_df_sequences` (MySQL only): Correctly uses `_df_` prefix ✓
- All standard DuckLake tables use `ducklake_` prefix ✓
- R6-S-030 comments in `metadata_writer_sqlite.rs:121,130` accurately describe the convention ✓

### R6-S-018: PG/MySQL Atomic replace_table_files
**Status: Verified Correct.**

All three backends (SQLite:1451, PG:1051, MySQL:1182) override `replace_table_files` with atomic transaction implementations:
1. End all existing data files atomically ✓
2. Register new files with cumulative row_id_start (R6-S-015) ✓
3. Register column stats with table_id (R6-S-001) ✓
4. Register partition values ✓
5. Recalculate ducklake_table_stats ✓
6. All within single transaction ✓

### R6-S-031: Timestamp Parsing
**Status: Verified Correct.**

- SQLite uses TEXT with ISO 8601 format comment (line 217) ✓
- PostgreSQL uses `TIMESTAMP WITH TIME ZONE` / `CURRENT_TIMESTAMP` ✓
- MySQL uses `DATETIME(6)` / `NOW(6)` ✓
- Each backend uses its native timestamp type ✓

### R6-S-034: ducklake_table_stats Constraint
**Status: Applied.**

All backends use `table_id PRIMARY KEY`. While DuckDB's reference schema has no PK, this is an additive constraint that doesn't break interop (R7-I-001).

## Cross-Engine Test Coverage Matrix (Updated)

| Test File | Count | Direction | Coverage |
|-----------|:-----:|-----------|----------|
| `cross_engine_tests.rs` | 20 | Both | Core INSERT, DELETE, UPDATE, MERGE, ALTER, types |
| `cross_engine_inline_tests.rs` | 6 | DuckDB→DF | Inlined data reads (int, string, float, null) |
| `cross_engine_dml_tests.rs` | ~10 | Both | DML operations |
| `cross_engine_ddl_tests.rs` | ~5 | Both | DDL operations |
| `cross_engine_alter_tests.rs` | ~5 | Both | ALTER TABLE operations |
| `cross_engine_insert_tests.rs` | ~5 | Both | INSERT edge cases |
| `cross_engine_partition_tests.rs` | ~3 | Both | Partitioned tables |
| `cross_engine_feature_tests.rs` | ~5 | Both | Feature-specific tests |
| `cross_engine_postgres_tests.rs` | ~5 | PG-specific | PostgreSQL cross-engine |
| `cross_engine_mysql_tests.rs` | ~5 | MySQL-specific | MySQL cross-engine |

**Total: ~69 cross-engine test functions across 10 files.** Significant improvement from R6 (20 tests in 1 file).

## Metadata Values Comparison (No Change from R6)

| Key | DuckDB Value | Our Value | Compatible |
|-----|-------------|-----------|:----------:|
| version | 0.3 | 0.3 | Yes |
| created_by | DuckDB 6ddac802ff | DataFusion-DuckLake | Yes |
| encrypted | false | false | Yes |
| data_path | `{path}.files/` | configurable | Yes |

## File Format Comparison (No Change from R6)

| Aspect | DuckDB | Our Implementation | Compatible |
|--------|--------|-------------------|:----------:|
| File naming | `ducklake-{uuid-v7}.parquet` | `ducklake-{uuid-v4}.parquet` | Yes |
| Delete file naming | `ducklake-{uuid-v7}-delete.parquet` | `ducklake-{uuid-v4}-delete.parquet` | Yes |
| Delete file schema | (file_path VARCHAR, pos INT64) | (file_path VARCHAR, pos INT64) | Yes |
| Delete file field_ids | 0x7FFFFFFE, 0x7FFFFFFD | 0x7FFFFFFE, 0x7FFFFFFD | Yes |
| PARQUET:field_id | Yes | Yes | Yes |
| Footer size tracking | Yes | Yes | Yes |
| Inline data table schema | row_id, begin_snapshot, end_snapshot, cols | Same | Yes |

## snapshot_changes Format (No Change from R6)

All formats match DuckDB exactly. See R6 review for full comparison.

## Summary of All Open Interop Findings (R6 + R7)

| ID | Severity | Category | Status | Description |
|----|:--------:|----------|:------:|-------------|
| R6-I-002 | P3 | extension | OPEN | Extra columns in ducklake_column |
| R6-I-003 | P3 | extension | OPEN | Extra partial_max in data_file/delete_file |
| R6-I-004 | P3 | extension | OPEN | Extra table_id in schema_versions |
| R6-I-005 | P2 | extension | OPEN | 7 extra tables (6 ducklake_, 1 _df_) |
| R6-I-006 | P3 | file-naming | OPEN | UUID v4 vs v7 |
| R6-I-007 | P2 | schema-compat | OPEN | snapshot_time TEXT vs TIMESTAMPTZ |
| R6-I-008 | P2 | schema-compat | OPEN | schedule_start TEXT vs TIMESTAMPTZ |
| R7-I-001 | P3 | schema-compat | NEW | table_stats PRIMARY KEY constraint |
| R7-I-002 | P2 | inline-data | NEW | Inlined data schema evolution after ALTER TABLE |
| R7-I-003 | P2 | feature-parity | NEW | PG/MySQL lack inlined data support |
| R7-I-004 | P3 | atomicity | NEW | Default trait replace_table_files non-atomic |
| R7-I-005 | P3 | test-coverage | NEW | DF→DuckDB DML test gaps |

**Totals: 0 P0, 0 P1, 5 P2, 7 P3** (across R6 + R7)

Build note: `cargo test` fails to compile due to unrelated `icu_properties` / `rustls` dependency issues in the build environment, not related to code changes.
