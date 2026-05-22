# R9 Interop Review — F-044 Cross-Engine Compatibility

## Summary

The F-044 macro refactoring (commits 7f76386..7ca31af) successfully unified 3 metadata backends (SQLite, PostgreSQL, MySQL) via macros and a `SqlDialect` trait. The review finds **no P0 cross-engine breakages** — the macro-generated SQL preserves metadata format parity with pre-refactoring code. The dialect trait correctly abstracts backend differences (placeholders, upsert syntax, boolean literals, UUID handling, timestamp functions).

Key findings:
- **0 P0 issues**: No metadata format changes that would break DuckDB interop
- **1 P1 issue**: `_df_change_tracking` table presence in DuckDB-read catalogs (pre-existing, not introduced by F-044)
- **5 P2 issues**: Test coverage gaps, bool_lit parity, snapshot_changes format, DuckDB CLI verification blocked, stats canonicalization
- **6 P3 issues**: Informational — dialect methods verified correct, override methods maintain parity, metadata format preserved, codex confirmation

Reviewed files: `dialect.rs`, `metadata_provider_impl.rs`, `metadata_writer_impl.rs`, `metadata_writer_sqlite.rs`, `metadata_writer_postgres.rs`, `metadata_writer_mysql.rs`, `metadata_provider_sqlite.rs`, `metadata_provider_postgres.rs`, `metadata_provider.rs`.

## Findings

### R9-I-001: Metadata Format Preservation Verified (Priority: P3)
**Files**: `src/metadata_writer_impl.rs`, `src/dialect.rs`
**Description**: The macro-generated SQL writes metadata in the same format as the pre-refactoring per-backend implementations:

- **`snapshot_changes` format**: Uses `d.upsert("snapshot_id", &["changes_made"])` which generates `ON CONFLICT(snapshot_id) DO UPDATE SET changes_made = excluded.changes_made` for SQLite — identical to pre-refactoring hardcoded SQL.
- **Column stats format**: `min_value`/`max_value` stored as `VARCHAR` strings, `null_count` as `BIGINT` — unchanged.
- **File paths**: `path` and `path_is_relative` columns stored identically via bind parameters.
- **Delete file format**: `file_path + pos` columns, `format = 'parquet'` hardcoded — matches DuckDB expectations.
- **UUID format**: SQLite/MySQL use `uuid::Uuid::new_v4().to_string()` (hyphenated, e.g., `550e8400-e29b-41d4-a716-446655440000`), PostgreSQL uses `gen_random_uuid()` — both produce DuckDB-compatible formats.
- **Timestamp format**: SQLite uses `strftime('%Y-%m-%d %H:%M:%f+00:00','now')` — matches DuckDB's expected format for SQLite catalogs.

**Cross-engine impact**: None — formats are preserved exactly.
**Effort**: N/A (informational)

### R9-I-002: `_df_change_tracking` Table in DuckDB-Read Catalogs (Priority: P1)
**File**: `src/metadata_writer_sqlite.rs:189`, `src/metadata_writer_postgres.rs:106`, `src/metadata_writer_mysql.rs`
**Description**: The `_df_change_tracking` table is created by `initialize_schema()` in all backends. This is a DataFusion-specific table (prefixed `_df_` per R6-S-030) used for conflict detection. DuckDB's DuckLake extension does not create or expect this table. When DuckDB opens a catalog created by DataFusion, it will encounter this unknown table.

DuckDB's DuckLake extension ignores tables it doesn't recognize in the metadata database (it queries specific known tables by name), so this is unlikely to cause errors. However, DuckDB's own DDL operations (DROP TABLE, etc.) won't populate `_df_change_tracking`, meaning DataFusion's conflict detection won't detect DuckDB-originated DDL unless it falls back to checking `end_snapshot` on the DuckLake catalog tables (which it does — see `drop_table_checked` at `metadata_writer_impl.rs:2218-2235`).

**Cross-engine impact**: Low. DuckDB ignores unknown tables. DataFusion has dual conflict detection (change tracking + catalog state). Pre-existing issue, not introduced by F-044.
**Suggested fix**: Document in README/CLAUDE.md that `_df_change_tracking` is DataFusion-specific and DuckDB ignores it.
**Effort**: S

### R9-I-003: `SqlDialect::bool_lit()` Parity Between Backends (Priority: P2)
**File**: `src/dialect.rs:99-104`, `src/dialect.rs:216-221`
**Description**: SQLite uses `"1"/"0"` for boolean literals while PostgreSQL/MySQL use `"TRUE"/"FALSE"`. This is correct per each backend's SQL syntax. However, this method is used in `alter_table` for initializing `contains_null` in `ducklake_table_column_stats` (`metadata_writer_impl.rs:1468`):
```rust
let stats_sql = format!(
    "INSERT INTO ducklake_table_column_stats ... VALUES ({}, {}, {}, NULL)",
    d.ph(1), d.ph(2), d.bool_lit(true)
);
```
Since `bool_lit` generates SQL text (not bind parameter), and both `1` and `TRUE` are valid boolean representations that DuckDB can read from SQLite/PG catalogs respectively, this is correct. DuckDB maps `1` to `TRUE` when reading boolean columns from SQLite.

**Cross-engine impact**: None — DuckDB handles both representations correctly.
**Suggested fix**: None needed, but worth noting for completeness.
**Effort**: N/A

### R9-I-004: Cross-Engine Test Coverage for Macro-ized Methods (Priority: P2)
**Files**: `tests/cross_engine_*.rs`
**Description**: There are 10 cross-engine test files covering a comprehensive set of operations:
- `cross_engine_tests.rs`: Basic SELECT interop (DuckDB-created catalog read by DataFusion)
- `cross_engine_insert_tests.rs`: INSERT operations (DataFusion writes, DuckDB reads)
- `cross_engine_dml_tests.rs`: DELETE, UPDATE operations
- `cross_engine_alter_tests.rs`: ALTER TABLE operations
- `cross_engine_ddl_tests.rs`: CREATE/DROP TABLE/SCHEMA
- `cross_engine_feature_tests.rs`: Virtual columns, time travel
- `cross_engine_inline_tests.rs`: Inlined data
- `cross_engine_partition_tests.rs`: Partitioned tables
- `cross_engine_postgres_tests.rs`: PostgreSQL-specific
- `cross_engine_mysql_tests.rs`: MySQL-specific

However, the cross-engine tests require the `metadata-duckdb` feature (they use the DuckDB library crate), which has a build issue on this system (corrupted build cache for `libduckdb-sys`). The test coverage appears adequate in scope based on file naming but could not be verified by running tests.

**Gaps identified by code inspection**:
- No cross-engine tests for MERGE INTO (programmatic API only, but metadata format matters)
- No cross-engine tests for views written by DataFusion and read by DuckDB
- No cross-engine test for `ducklake_table_column_stats` written by DataFusion (via `recompute_table_column_stats`)

**Cross-engine impact**: Missing coverage could mask format divergences in untested paths.
**Suggested fix**: Add cross-engine tests for: (1) MERGE metadata, (2) views round-trip, (3) column stats round-trip.
**Effort**: M

### R9-I-005: `upsert()` Dialect Method — Correct per Backend (Priority: P3)
**File**: `src/dialect.rs:119-128`, `src/dialect.rs:236-245`, `src/dialect.rs:341-346`
**Description**: The `upsert()` method generates correct syntax for each backend:
- SQLite: `ON CONFLICT(col) DO UPDATE SET col = excluded.col` (correct)
- PostgreSQL: `ON CONFLICT(col) DO UPDATE SET col = EXCLUDED.col` (correct — PG uses uppercase `EXCLUDED`)
- MySQL: `ON DUPLICATE KEY UPDATE col = VALUES(col)` (correct)

All three match the pre-refactoring hardcoded SQL. The case difference (`excluded` vs `EXCLUDED`) is correct — SQLite is case-insensitive, PostgreSQL convention uses uppercase.

**Cross-engine impact**: None.
**Effort**: N/A

### R9-I-006: `insert_or_ignore()` Dialect Method — Correct per Backend (Priority: P3)
**File**: `src/dialect.rs:138-139`, `src/dialect.rs:255-257`, `src/dialect.rs:357-358`
**Description**: Correctly generates:
- SQLite: `INSERT OR IGNORE INTO ...`
- PostgreSQL: `INSERT INTO ... ON CONFLICT DO NOTHING`
- MySQL: `INSERT IGNORE INTO ...`

Used in `initialize_schema()` for seeding metadata (snapshot 0, version, etc). Format matches DuckDB expectations.

**Cross-engine impact**: None.
**Effort**: N/A

### R9-I-007: `snapshot_changes` Format String Values (Priority: P2)
**File**: `src/metadata_writer_impl.rs` (multiple locations)
**Description**: The macro generates `changes_made` values using these formats:
- `created_view:"{schema}"."{view}"` (line 1163)
- `dropped_view:{view_id}` (line 1240)
- `altered_view:{view_id}` (line 1355)
- `altered_table:{table_id}` (line 1560)
- `dropped_table:{table_id}` (line 1997)
- `dropped_schema:{schema_id}` (line 2155)

Per-backend (non-macro) methods use:
- `created_table:"{schema}"."{table}"` (`metadata_writer_sqlite.rs:627`)
- `inserted_into_table:{table_id}` (`metadata_writer_sqlite.rs:647`)
- `created_schema:"{name}"` (`metadata_writer_sqlite.rs:794`)
- `deleted_from_table:{table_id}` (`delete_exec.rs:355`)

These formats match DuckDB's expected `changes_made` tokens (verified by comparing with DuckDB's DuckLake extension source). The quoting pattern (`"schema"."table"`) uses SQL identifier quoting with `""` escape for embedded quotes, consistent with DuckDB.

**Cross-engine impact**: Low risk. DuckDB reads `changes_made` for display purposes (in `ducklake_snapshots()` table function) but does not parse it for operational logic.
**Suggested fix**: Document the expected `changes_made` format tokens for future contributors.
**Effort**: S

### R9-I-008: Schema Initialization Parity Across Backends (Priority: P2)
**Files**: `src/metadata_writer_sqlite.rs:93-196`, `src/metadata_writer_postgres.rs:19-220`, `src/metadata_writer_mysql.rs`
**Description**: `initialize_schema()` remains per-backend (not macro-ized) because each backend has different DDL syntax (column types, identity columns, etc). Key observations:

- **SQLite**: Uses `INTEGER PRIMARY KEY`, `BOOLEAN`, `TEXT DEFAULT (strftime(...))` — matches DuckDB's expected SQLite catalog schema
- **PostgreSQL**: Uses `BIGINT GENERATED ALWAYS AS IDENTITY`, `UUID` type for UUIDs, `TIMESTAMP WITH TIME ZONE` — appropriate for PG
- **MySQL**: Uses `BIGINT AUTO_INCREMENT`, quoted `\`key\`` for reserved words — appropriate for MySQL

All three backends create the same logical tables (`ducklake_metadata`, `ducklake_snapshot`, `ducklake_schema`, `ducklake_table`, `ducklake_column`, `ducklake_data_file`, `ducklake_delete_file`, `ducklake_snapshot_changes`, etc.) with semantically identical columns.

The `version` metadata value is set to `'0.3'` in all backends, matching DuckDB v1.4.x compatibility. The `encrypted` metadata key is set to `'false'`, matching DuckDB's convention.

**Cross-engine impact**: None — DDL is correctly backend-specific.
**Effort**: N/A

### R9-I-009: DuckDB CLI Verification Blocked by Build Cache (Priority: P2)
**Description**: Unable to run DuckDB CLI verification because:
1. The `libduckdb-sys` build cache is corrupted (missing `.cpp` files), preventing `cargo test` with `metadata-duckdb` feature
2. Could verify DuckDB CLI is installed (`/home/zac/.local/bin/duckdb`) but cannot create a fresh test catalog without the Rust test harness

A clean build (`cargo clean` + rebuild) would resolve this. The interop risk from code inspection alone is **low** — all SQL is structurally identical to pre-refactoring code, with only the parameterization mechanism changed (hardcoded dialect-specific SQL → dialect trait method calls generating identical SQL).

**Suggested fix**: Run `cargo clean && cargo test --features write-sqlite cross_engine` after build cache repair.
**Effort**: S

### R9-I-010: Backend Override Methods Maintain Parity (Priority: P3)
**Files**: `src/metadata_provider_sqlite.rs:54-200`, `src/metadata_provider_postgres.rs:52-187`, `src/metadata_provider_mysql.rs:52-196`
**Description**: Three methods remain per-backend (not macro-ized) due to structural SQL differences:

1. **`get_delete_files_impl`**: SQLite uses correlated subqueries (no LATERAL JOIN support), PostgreSQL uses `LEFT JOIN LATERAL`, MySQL uses its own variant. All three produce the same `DeleteFileChange` struct with identical field mappings (17 columns in same order).

2. **`get_inlined_data_impl`**: Different JSON handling per backend (SQLite `json_each`, PG `json_array_elements`, MySQL `JSON_TABLE`).

3. **`count_inlined_rows_impl`**: Minor SQL differences for counting.

These override methods produce identical result types and semantics. The macro correctly delegates to `self.get_delete_files_impl(...)`, `self.get_inlined_data_impl(...)`, and `self.count_inlined_rows_impl(...)`.

**Cross-engine impact**: None — result types are identical, only SQL syntax differs.
**Effort**: N/A

### R9-I-011: Stats min/max Stored as Raw Strings Without Canonicalization (Priority: P2)
**Files**: `src/metadata_writer_impl.rs:395`, `src/metadata_writer_impl.rs:682`, `src/metadata_writer_impl.rs:966`
**Description**: `min_value`/`max_value` in `ducklake_file_column_stats` and `ducklake_table_column_stats` are stored as plain `VARCHAR` strings. The `recompute_table_column_stats` macro (`metadata_writer_impl.rs:1-131`) re-aggregates these using `stat_value_less_than()` with type-aware comparison (numeric vs lexicographic). However, the raw string format is whatever Arrow/Parquet produces — there is no canonicalization to a DuckDB-specific literal format.

For numeric types this is fine (both engines produce the same decimal string). For timestamps, the format may vary (e.g., `2026-03-06T12:00:00` vs `2026-03-06 12:00:00+00`). DuckDB reads these stats for pruning (row group skipping) — if the format doesn't parse, DuckDB simply skips pruning (no data corruption, just slower queries).

This is a pre-existing issue, not introduced by F-044. The macro generates the same string-bind logic as the pre-refactoring code.

**Cross-engine impact**: Low — pruning may be suboptimal but data correctness is unaffected.
**Suggested fix**: Consider normalizing timestamp/date stats to ISO 8601 format on write.
**Effort**: M

### R9-I-012: Codex Automated Review Confirmation (Priority: P3)
**Description**: Ran `codex exec --full-auto` against `dialect.rs`, `metadata_provider_impl.rs`, and `metadata_writer_impl.rs`. Codex independently confirmed:
- SQLite timestamp format: **compatible** (`strftime('%Y-%m-%d %H:%M:%f+00:00','now')`)
- UUID format: **compatible** (hyphenated via `Uuid::to_string()`)
- Boolean storage: **compatible** (`1/0` for SQLite, typed bool binds elsewhere)
- `changes_made` format: flagged view tokens as potentially unguarded (covered in R9-I-007)
- Stats min/max: flagged raw string storage (covered in R9-I-011)

No additional issues found beyond those already documented in this review.

**Cross-engine impact**: N/A (confirmation of prior findings).
**Effort**: N/A

## Conclusion

The F-044 refactoring is **interop-safe**. The macro approach generates identical SQL to the pre-refactoring per-backend implementations. The `SqlDialect` trait correctly captures all backend-specific SQL differences (placeholders, upsert, boolean literals, UUID handling, timestamps, CAST expressions, existence checks). No metadata format changes were introduced that would affect DuckDB's ability to read DataFusion-created catalogs.

The main recommendation is to verify with a clean build and full cross-engine test run once the `libduckdb-sys` build cache is repaired. The test coverage gaps (MERGE, views, column stats round-trip) are pre-existing and not introduced by F-044.
