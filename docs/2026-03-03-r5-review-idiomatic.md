# R5 Idiomatic Review — Rust Idioms, DataFusion API, and DuckLake Patterns

**Reviewer**: idiomatic-review agent
**Date**: 2026-03-03
**Scope**: All 34 source files in `src/` (~31,382 lines)
**Focus**: Error handling, ownership/borrowing, trait design, consistency, performance, dead code, API misuse
**Excludes deferred items**: F-036, F-044, F-045, R4-S-018, R4-S-036, R4-S-040

---

## Summary

| Severity | Count |
|----------|-------|
| P0 (Critical) | 0 |
| P1 (High) | 2 |
| P2 (Medium) | 8 |
| P3 (Low) | 7 |
| **Total** | **17** |

---

## P1 — High

### I-001 `contains_null` inconsistency across metadata writers [P1]

**Files**: `metadata_writer_sqlite.rs:2164`, `metadata_writer_postgres.rs:1845`, `metadata_writer_mysql.rs:1972`

In `alter_table` → `AlterTableAction::InsertColumn`, the SQLite writer correctly sets `contains_null = 1` (TRUE) per R3F-001 to prevent a DuckDB crash when reading table-level column stats. However, the PostgreSQL and MySQL writers set `contains_null = NULL`:

```sql
-- SQLite (CORRECT - R3F-001)
INSERT INTO ducklake_table_column_stats (..., contains_null, contains_nan) VALUES (?, ?, 1, NULL)

-- PostgreSQL (INCORRECT)
INSERT INTO ducklake_table_column_stats (..., contains_null, contains_nan) VALUES ($1, $2, NULL, NULL)

-- MySQL (INCORRECT)
INSERT INTO ducklake_table_column_stats (..., contains_null, contains_nan) VALUES (?, ?, NULL, NULL)
```

When a column is added via ALTER TABLE, existing rows have NULL for the new column, so `contains_null` must be TRUE. Setting it to NULL can cause DuckDB to crash when reading this catalog.

**Fix**: Change PG and MySQL writers to use `TRUE`/`1` for `contains_null` in the InsertColumn branch, matching the SQLite writer's R3F-001 fix.

---

### I-002 `parse_inlined_column` silently converts unknown types to strings [P1]

**File**: `table_writer.rs` (in `parse_inlined_column` function)

When `parse_inlined_column` encounters an unrecognized DuckLake type, it silently falls through to creating a `StringArray` from the raw string values. This masks type-mapping bugs — a column declared as `STRUCT<...>` or `MAP<...>` would silently become a string column with no warning or error, potentially producing incorrect query results.

**Fix**: Log a warning or return an error for unrecognized types rather than silently falling back to string conversion.

---

## P2 — Medium

### I-003 Duplicated `quote_identifier` function [P2]

**Files**: `metadata_provider.rs:782`, `metadata_writer_validation.rs:67`

The `quote_identifier()` function (escapes SQL identifiers with double-quote doubling) is defined identically in two modules. Both are `pub(crate)`.

**Fix**: Move to a shared utility module (e.g., `src/sql_utils.rs`) and re-export, or make one canonical and import from the other.

---

### I-004 Duplicated `make_*_count_schema()` across DML exec plans [P2]

**Files**: `delete_exec.rs`, `update_exec.rs`, `merge_exec.rs`, `insert_exec.rs`

Each DML execution plan defines its own `make_*_count_schema()` function that creates an identical single-column `UInt64` schema for reporting affected row counts:

- `delete_exec.rs`: `make_delete_count_schema()`
- `update_exec.rs`: `make_update_count_schema()`
- `merge_exec.rs`: `make_merge_count_schema()`
- `insert_exec.rs`: `make_insert_count_schema()`

All return `SchemaRef` with a single `UInt64` "count" field.

**Fix**: Extract a shared `fn make_dml_count_schema() -> SchemaRef` in a common module.

---

### I-005 Manual type dispatch in `merge_exec.rs` `values_equal()` [P2]

**File**: `merge_exec.rs` (in `values_equal` function)

The `values_equal()` function uses a large `match` over Arrow `DataType` variants with a `downcast_and_compare!` macro to check equality between two arrays at a given index. Arrow's `arrow::compute::eq` kernel or `arrow_ord::cmp::eq` can compare arbitrary arrays without manual type dispatch.

**Fix**: Replace the manual dispatch with `arrow::compute::eq_dyn(&left.slice(i, 1), &right.slice(i, 1))` or similar, which handles all types generically and is maintained upstream.

---

### I-006 Repetitive `TableProvider` boilerplate in `information_schema.rs` [P2]

**File**: `information_schema.rs`

The module defines 6 table types (SnapshotsTable, SchemataTable, TablesTable, ColumnsTable, TableInfoTable, FilesTable) with nearly identical `TableProvider` implementations. Each has identical `table_type()`, `schema()`, and `supports_filters_pushdown()` methods, with `scan()` differing only in the SQL query and row-mapping logic.

**Fix**: Consider a generic struct `LiveMetadataTable<F>` parameterized by a query/mapping closure, or use a macro to reduce the ~100+ lines of duplicated trait impl boilerplate.

---

### I-007 DDL constant style inconsistency across writers [P2]

**Files**: `metadata_writer_postgres.rs:20-269`, `metadata_writer_mysql.rs:20-324`, `metadata_writer_sqlite.rs`

The PostgreSQL writer stores all DDL as a single `SQL_CREATE_TABLES: &[&str]` array constant, while the MySQL writer uses ~30 individual `SQL_CREATE_*` constants, and the SQLite writer uses yet another style. This makes cross-writer maintenance error-prone since adding a new table requires different patterns in each writer.

**Fix**: Standardize on one approach (prefer the array-of-strings style used by PostgreSQL for conciseness) across all writers.

---

### I-008 Inline helper lambdas defined multiple times in `compaction_functions.rs` [P2]

**File**: `compaction_functions.rs`

The helper closures `to_string_array` and `to_i64_array` (which extract typed values from Arrow arrays) are defined inline multiple times across different function bodies within this file.

**Fix**: Extract as module-level helper functions defined once and used by all compaction function implementations.

---

### I-009 SQL duplication: providers inline SQL instead of using shared constants [P2]

**Files**: `metadata_provider_sqlite.rs`, `metadata_provider_postgres.rs`, `metadata_provider_mysql.rs`, `metadata_provider.rs`

The `metadata_provider.rs` module defines shared SQL constants (`SQL_GET_DATA_FILES`, `SQL_LIST_SCHEMAS`, `SQL_LIST_TABLES`, etc.) for common queries. However, the SQLite, PostgreSQL, and MySQL providers re-define many of these queries inline instead of using the shared constants, because they need different placeholder syntax ($N vs ? vs ?).

Currently the DuckDB provider uses the shared constants while the other providers mostly don't, leading to SQL drift risk.

**Fix**: Either (a) parameterize the shared SQL with a placeholder builder, or (b) document the divergence clearly and add integration tests that verify consistent behavior across providers.

---

### I-010 Unnecessary `positions_to_delete.clone()` in `delete_exec.rs` [P2]

**File**: `delete_exec.rs:340`

The `positions_to_delete` vector is cloned before being passed to a function that takes ownership. Since `positions_to_delete` is not used after this point, the clone is unnecessary.

**Fix**: Remove `.clone()` and pass the owned value directly.

---

## P3 — Low

### I-011 Redundant `let batch = batch_result;` binding in `delete_exec.rs` [P3]

**File**: `delete_exec.rs` (in the execute stream processing)

A variable binding `let batch = batch_result;` creates an alias with no additional transformation or type conversion, adding no value.

**Fix**: Use the original name directly.

---

### I-012 Inconsistent `Arc` cloning style [P3]

**Files**: Throughout codebase

The codebase mixes `Arc::clone(&x)` (Clippy-recommended, makes the cheap clone explicit) and `x.clone()` (implicit Arc clone). Most files use `Arc::clone(&x)` but some use `x.clone()` for Arc types.

**Fix**: Standardize on `Arc::clone(&x)` throughout. Could be enforced via `clippy::clone_on_ref_ptr` lint.

---

### I-013 `list_views` in SQLite provider uses `.iter()` instead of `.into_iter()` [P3]

**File**: `metadata_provider_sqlite.rs`

In `list_views()`, the code uses `.iter()` on a `Vec` and then clones each element, when `.into_iter()` would consume the vector and avoid unnecessary clones.

**Fix**: Replace `.iter().map(|r| r.clone())` with `.into_iter()`.

---

### I-014 Public `pool` fields on metadata providers leak implementation details [P3]

**Files**: `metadata_provider_sqlite.rs`, `metadata_provider_postgres.rs`, `metadata_provider_mysql.rs`

The `pool` field on `SqliteMetadataProvider`, `PostgresMetadataProvider`, and `MySqlMetadataProvider` is `pub`, exposing the internal connection pool to downstream consumers. This makes it hard to change the pool implementation without breaking the public API.

**Fix**: Make `pool` private and add accessor methods if external access is genuinely needed.

---

### I-015 `DuckLakeEncryptionFactory` derives `Clone` but it's never cloned [P3]

**File**: `encryption.rs`

`DuckLakeEncryptionFactory` derives `Clone` but there's no code path in the crate that clones it. The struct holds encryption keys, so allowing cloning may not be desirable from a security perspective.

**Fix**: Remove the `Clone` derive unless there's a concrete use case. The custom `Debug` impl already prevents key exposure.

---

### I-016 `keep_indices.clone()` may be unnecessary in `table_deletions.rs` [P3]

**File**: `table_deletions.rs:702`

The `keep_indices` `UInt32Array` is cloned before being passed to `take()`, which takes a reference. If `keep_indices` is not used after this call, the clone is wasteful.

**Fix**: Verify whether `keep_indices` is used after the `take()` call; if not, remove the clone.

---

### I-017 `metadata_writer_mysql.rs` `last_insert_id` / `last_insert_id_conn` duplication [P3]

**File**: `metadata_writer_mysql.rs:327-340`

Two nearly identical functions `last_insert_id` (takes `&mut Transaction`) and `last_insert_id_conn` (takes `&mut PoolConnection`) exist. Both execute the same SQL (`SELECT CAST(LAST_INSERT_ID() AS SIGNED) as id`) but accept different connection types.

**Fix**: Use a generic approach or the `Executor` trait from sqlx to unify into a single function.

---

## Observations (not findings)

These are patterns that are noted but not flagged because they're already tracked as deferred or are acceptable trade-offs:

1. **Provider/writer code duplication** (F-044): The massive duplication across metadata_writer_sqlite/postgres/mysql is known and deferred.
2. **`map_err` boilerplate** (R4-S-036): The `map_err(|e| DataFusionError::External(Box::new(e)))` pattern is pervasive in compaction_functions.rs and all writers.
3. **Monolithic execute blocks** (R4-S-040): Many DML exec plans have single large `execute()` functions.
4. **Async trait design** (F-045): The sync `MetadataWriter` trait wrapping async via `block_on` is a known limitation.
5. **TOCTOU in PG/MySQL writers** (R4-S-018): Known and deferred.
6. **INSERT streaming** (F-036): Known and deferred.

---

## Files Reviewed

All 34 files in `src/`:
- `lib.rs`, `error.rs`, `types.rs`, `path_resolver.rs`
- `catalog.rs`, `schema.rs`, `table.rs`
- `metadata_provider.rs`, `metadata_provider_duckdb.rs`, `metadata_provider_sqlite.rs`, `metadata_provider_postgres.rs`, `metadata_provider_mysql.rs`
- `metadata_writer.rs`, `metadata_writer_validation.rs`, `metadata_writer_sqlite.rs`, `metadata_writer_postgres.rs`, `metadata_writer_mysql.rs`
- `table_writer.rs`, `insert_exec.rs`, `delete_exec.rs`, `update_exec.rs`, `merge_exec.rs`
- `query_planner.rs`, `delete_filter.rs`
- `virtual_column_exec.rs`, `column_rename.rs`, `cdc_common.rs`
- `table_changes.rs`, `table_insertions.rs`, `table_deletions.rs`
- `table_functions.rs`, `information_schema.rs`, `compaction_functions.rs`
- `encryption.rs`
