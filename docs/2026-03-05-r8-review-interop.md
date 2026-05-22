# R8 Interoperability Review — 2026-03-05

Reviewer: `r8-interop-review` agent
Branch: `ducklake-features/integration`
DuckDB version tested: v1.4.4 (Andium)
DuckLake extension: core (v0.3 format)

---

## Methodology

1. Created a real DuckDB DuckLake catalog (`duckdb` CLI) and extracted the complete DDL
2. Compared column-by-column against our SQLite/Postgres/MySQL DDL
3. Experimentally tested interop: added extra columns/tables to DuckDB catalogs and verified read/write behavior
4. Reviewed cross-engine test coverage (direction, operation type, gaps)
5. Reviewed partition layout, inlined data format, file naming conventions

---

## Findings Summary

| Severity | Count |
|----------|-------|
| P1 (Interop breakage) | 2 |
| P2 (Coverage gaps / minor issues) | 6 |
| P3 (Documentation / cosmetic) | 3 |

---

## P1 — Interop Breakage

### P1-001: `ducklake_schema_versions` has extra `table_id` column (SQLite only)

**File:** `src/metadata_writer_sqlite.rs:312-316`

Our SQLite DDL creates:
```sql
CREATE TABLE IF NOT EXISTS ducklake_schema_versions (
    begin_snapshot INTEGER,
    schema_version INTEGER,
    table_id INTEGER          -- ← NOT in DuckDB
);
```

DuckDB's DuckLake creates:
```sql
CREATE TABLE ducklake_schema_versions (
    begin_snapshot BIGINT,
    schema_version BIGINT
);
```

**Impact:** When DataFusion creates the catalog (via `initialize_schema()`), the extra `table_id` column causes DuckDB to fail on subsequent DDL writes with: `table ducklake_schema_versions has 3 columns but 2 values were supplied`. DuckDB uses column-count INSERT without explicit column list.

**Experimentally confirmed:** Adding `table_id` to a DuckDB-created catalog breaks DuckDB writes.

**Note:** Postgres and MySQL backends do NOT have this extra column — they match DuckDB correctly. Only the SQLite backend is affected.

**Recommendation:** Remove `table_id` from `ducklake_schema_versions` in the SQLite DDL. Grep shows we never INSERT `table_id` into this table anyway — all inserts use `(begin_snapshot, schema_version)` only.

---

### P1-002: `ducklake_data_file` and `ducklake_delete_file` have extra `partial_max` column

**Files:**
- `src/metadata_writer_sqlite.rs:161` (`ducklake_data_file`)
- `src/metadata_writer_sqlite.rs:178` (`ducklake_delete_file`)
- `src/metadata_writer_postgres.rs:84,100`
- `src/metadata_writer_mysql.rs:94,112`

Our DDL includes `partial_max INTEGER` on both tables. DuckDB's DuckLake does NOT have this column.

**Impact:** When DataFusion creates the catalog, the extra column causes DuckDB writes to fail with: `table ducklake_data_file has 17 columns but 16 values were supplied`.

**Experimentally confirmed:** Adding `partial_max` to a DuckDB-created catalog breaks DuckDB INSERT operations on data files.

**Note:** Grep shows we never read or write `partial_max` anywhere in the codebase. It appears to be a forward-compatibility column that was included in DDL but never used. Since DuckDB doesn't create it, this is pure schema bloat that breaks interop.

**Recommendation:** Remove `partial_max` from both `ducklake_data_file` and `ducklake_delete_file` across all three backends (SQLite, Postgres, MySQL).

---

## P2 — Missing Coverage / Minor Issues

### P2-001: No DF-write → DuckDB-read cross-engine tests for partitioned tables

**File:** `tests/cross_engine_partition_tests.rs`

All 7 partition tests are DuckDB→DF direction only:
- `test_duckdb_partitioned_table_df_read_all`
- `test_duckdb_partitioned_table_df_read_with_filter`
- `test_duckdb_partition_pre_and_post_data`
- `test_duckdb_multi_column_partition`
- `test_duckdb_month_partition_transform`
- `test_duckdb_partitioned_count`
- `test_duckdb_empty_partitioned_table`

No tests verify that DuckDB can read partitioned tables written by DataFusion. This is significant because:
- Hive-style directory naming (`key=value/`) could differ in encoding
- Partition value metadata in `ducklake_file_partition_value` must match DuckDB expectations
- URL-encoding of special characters in partition values is untested cross-engine

### P2-002: No cross-engine MERGE tests (DF→DuckDB direction)

**File:** `tests/cross_engine_tests.rs:642`

Only one MERGE test exists: `cross_engine_duckdb_merge_df_read` (DuckDB→DF). No test verifies that MERGE operations performed by DataFusion produce catalogs/files that DuckDB can read.

### P2-003: `ducklake_files_scheduled_for_deletion.schedule_start` type mismatch

**Files:**
- `src/metadata_writer_sqlite.rs:287`: `schedule_start TEXT`
- DuckDB: `schedule_start TIMESTAMP WITH TIME ZONE`

Our comment acknowledges this: "TEXT for cross-engine compat; ISO 8601 UTC". Since DuckDB reads with `CAST(... AS VARCHAR)`, reads work. However, if DuckDB performs timestamp arithmetic on `schedule_start` (e.g., for `expire_snapshots()`), it would fail because TEXT can't be used with interval operations.

**Impact:** Low — compaction/expiration is delegated to DuckDB anyway (via `compaction_functions.rs`). But if a user calls `expire_snapshots()` through DuckDB on a DF-created catalog, the TEXT type could cause issues.

### P2-004: `ducklake_column` has extra columns not in DuckDB

**Files:** All three backends

Our DDL includes `default_value_type VARCHAR` and `default_value_dialect VARCHAR` on `ducklake_column`. DuckDB's column table does NOT have these columns.

**Impact:** Experimentally verified that DuckDB tolerates extra columns on `ducklake_column` — both reads and writes succeed. This is because DuckDB uses named-column INSERTs for this table (unlike `ducklake_data_file`). No interop breakage, but these are schema extensions that DuckDB doesn't understand.

**Note:** These columns are populated as NULL in all cases (forward-compatibility). Safe for now but should be monitored.

### P2-005: UUID format difference (UUIDv4 vs UUIDv7)

Our code generates UUIDv4 for `schema_uuid`, `table_uuid`, `view_uuid`, and file names (`ducklake-{uuid}.parquet`). DuckDB generates UUIDv7 (time-ordered) UUIDs. Both are valid UUIDs; the difference is cosmetic and doesn't affect functionality. However:

- File names with UUIDv4 are not time-ordered, making filesystem listing less informative
- DuckDB stores UUIDs as native UUID type; we store as VARCHAR — this is compatible because DuckDB casts on read

### P2-006: Cross-engine test coverage only uses SQLite backend

**File:** `tests/cross_engine_tests.rs:13`

```rust
// TODO(R5-S-067): These tests only use SQLite backend. Add PG/MySQL cross-engine
// tests when Docker-based test infrastructure is available.
```

All cross-engine tests create SQLite-backed catalogs. No tests verify that Postgres or MySQL-backed catalogs work with DuckDB. Since Postgres and MySQL backends have different DDL (different type names, different ID allocation strategies), there may be untested interop issues specific to those backends.

---

## P3 — Documentation / Cosmetic

### P3-001: `_df_change_tracking` table presence

**File:** `src/metadata_writer_sqlite.rs:192-198`

The `_df_change_tracking` table uses the `_df_` prefix (per R6-S-030) to avoid conflicts with DuckLake catalog tables.

**Experimentally confirmed:** DuckDB completely ignores unknown tables — adding `_df_change_tracking` to a DuckDB catalog causes no issues. This is correctly implemented.

### P3-002: Column ordering differences in DDL

Our DDL uses a different column order than DuckDB in several tables (e.g., `ducklake_data_file`, `ducklake_delete_file`, `ducklake_column`). This is cosmetic — SQLite, Postgres, and MySQL all use named column access, so ordering doesn't matter. However, if any DuckDB code path uses positional column access, this could theoretically cause issues.

**Impact:** None observed. All our SQL queries use named columns.

### P3-003: Missing `ducklake_macro`, `ducklake_sort_info`, `ducklake_file_variant_stats` in DuckDB default catalog

Our DDL pre-creates several tables that DuckDB only creates on demand:
- `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters`
- `ducklake_sort_info`, `ducklake_sort_expression`
- `ducklake_file_variant_stats`

**Experimentally confirmed:** Extra tables don't affect DuckDB — it ignores tables it doesn't know about. Pre-creating them is forward-compatible and poses no interop risk.

---

## Cross-Engine Test Coverage Summary

| Operation | DuckDB→DF Tests | DF→DuckDB Tests |
|-----------|-----------------|-----------------|
| Basic SELECT | 10+ | 10+ |
| INSERT | 9+ | 5+ |
| DELETE | 4+ | 4+ |
| UPDATE | 4+ | 4+ |
| MERGE | 1 | 0 |
| DDL (CREATE/DROP) | 22+ | 0* |
| ALTER TABLE | 7+ | 0* |
| Partitions | 7 | 0 |
| Inlined data | 9+ | 0** |
| Type roundtrip | 5+ | 5+ |

\* DDL operations create catalog entries; DuckDB→DF tests verify DF can read DuckDB-created schemas
\*\* Inlined data is SQLite-only; DuckDB uses its own inlining mechanism

**Total cross-engine tests:** ~160 (181 test functions across 10 cross-engine files + 2 interop files)

---

## Pre-existing Known Issues (Not New)

1. **3 DuckDB assertion crashes in ALTER TABLE tests** — upstream DuckDB bug, pre-existing
2. **R7 fixes applied:** inlined data schema evolution, PG/MySQL inlined data errors, snapshot propagation — all resolved

---

## Recommendations Priority

1. **Fix P1-001 and P1-002** — Remove `table_id` from `ducklake_schema_versions` (SQLite) and `partial_max` from `ducklake_data_file`/`ducklake_delete_file` (all backends). These are unused columns that break DuckDB write interop.

2. **Add DF→DuckDB partition tests** (P2-001) — Critical coverage gap for a key feature.

3. **Add DF→DuckDB MERGE test** (P2-002) — Single test would close this gap.

4. **Monitor P2-003** (`schedule_start` TEXT vs TIMESTAMPTZ) — Document the limitation; consider storing as ISO 8601 string that DuckDB can parse.
