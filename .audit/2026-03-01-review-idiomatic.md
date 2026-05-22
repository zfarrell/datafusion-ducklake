# Idiomatic Patterns Review — Tier 1 Sprint

**Date:** 2026-03-01
**Scope:** Write-side partitioning, write-side inlining, SLT fixes, cross-engine PG/MySQL tests, benchmark, pre-commit hook
**Reviewer:** Senior Rust/DataFusion reviewer

## Executive Summary

The Tier 1 sprint code is generally well-structured and follows established codebase patterns. The `MetadataWriter` trait is cleanly extended with default no-op implementations for backward compatibility. Error handling is consistent and uses the `?` operator idiomatically throughout. The main areas for improvement are: (1) duplicated test helper functions across 5+ test files that should be consolidated, (2) row-by-row processing in `route_batches_to_partitions` and `batches_to_inlined_rows` that could use columnar Arrow operations for large datasets, (3) SQL injection surface in `metadata_writer_sqlite.rs` from format-string table names, and (4) several minor clippy/style nits.

---

## Findings by Severity

### Critical

#### C-1: SQL injection via format-string table names in SQLite inlining
**File:** `src/metadata_writer_sqlite.rs`, lines 1826–1830, 1861–1869, 1896–1901, 1979–1983, 2020–2023
**Issue:** The inlined data table name is constructed from `table_id` (safe — always an integer), but all SQL queries against it use `format!()` string interpolation rather than parameterized queries. While `table_id` is an i64 (not user-controlled strings), the pattern is fragile — if the table name derivation ever changes to include user input, it becomes exploitable. More critically, column names from `ColumnDef` are interpolated directly into CREATE TABLE and INSERT statements (line 1867: `format!(", \"{}\" TEXT", col.name())`).

**Suggested fix:** Column names from `ColumnDef` should be validated/sanitized for SQL-safe identifiers (no quotes, semicolons, etc.) before interpolation, or use a whitelist approach. Consider adding a `validate_sql_identifier()` helper. The `table_id`-based names are safe but should have a comment noting the invariant.

---

### Major

#### M-1: Row-by-row partition routing is O(rows × partitions) and non-columnar
**File:** `src/insert_exec.rs`, lines 506–531
**Issue:** `route_batches_to_partitions` iterates row-by-row across all batches, calling `compute_partition_value()` per row per partition column. For large inserts this is significantly slower than Arrow's columnar processing. The subsequent `extract_rows()` (lines 534–587) also builds indices per-row and calls `arrow::compute::take` per batch group.

**Suggested fix:** For identity partitions on string columns, use `arrow::compute::partition` or build a dictionary-based grouping. For the common case (single string partition column), a vectorized hash-group approach would be ~10x faster. Not critical for correctness but will become a bottleneck at scale.

#### M-2: Duplicated test helper functions across 5+ integration test files
**Files:**
- `tests/write_partition_tests.rs` — `batches_to_sorted_strings()`, `arrow_val_to_string()`
- `tests/write_inline_tests.rs` — `batches_to_strings()`, `arrow_value_to_string()`
- `tests/cross_engine_postgres_tests.rs` — `batches_to_strings()`, `arrow_value_to_string()`, `normalize_value()`, `assert_results_eq()`
- `tests/cross_engine_mysql_tests.rs` — identical copies of all PG helpers
- `tests/hybrid_asyncdb.rs` — `convert_batch_to_strings()` (yet another variant)

**Issue:** 5 near-identical implementations of "convert RecordBatch to Vec<Vec<String>>". The cross-engine PG and MySQL test files are ~95% structurally identical (753 vs 757 lines), differing only in provider/writer types and connection strings.

**Suggested fix:** Extract shared helpers into `tests/common/test_utils.rs`. For the cross-engine tests, consider a macro or generic test harness parameterized over backend type.

#### M-3: `InlinedDataRow` clones `column_names` per row
**File:** `src/table_writer.rs`, lines 663–690
**Issue:** `batches_to_inlined_rows()` clones the `column_names: Vec<String>` for every single row. For a 10,000-row inline insert, this creates 10,000 identical `Vec<String>` allocations.

**Suggested fix:** Change `InlinedDataRow.column_names` to `Arc<Vec<String>>` or restructure to store column names once per batch/table rather than per row.

#### M-4: `write_partitioned` iterates HashMap in non-deterministic order
**File:** `src/insert_exec.rs`, lines 595–653
**Issue:** The first partition gets `write_mode` (which may be `Replace`) while subsequent ones get `Append`. Since `HashMap` iteration order is non-deterministic, which partition gets the `Replace` semantics is random. This could lead to different results on different runs when using `WriteMode::Replace` with partitioned writes.

**Suggested fix:** Sort partition keys or use `BTreeMap` to ensure deterministic ordering. Alternatively, handle `Replace` mode by ending all existing files in a separate step before the partition loop, then use `Append` for all partitions.

---

### Minor

#### m-1: `rewrite_duckdb_view_sql` uses case-insensitive find but case-sensitive replace range
**File:** `src/schema.rs`, lines 149–156
**Issue:** The function calls `result.to_lowercase().find("count_star()")` to find the position, then does `result.replace_range(pos..pos + 12, "COUNT(*)")` on the original (mixed-case) string. This works because `to_lowercase()` preserves byte offsets for ASCII, but it's fragile — if the original contains non-ASCII characters before the match, the byte offset from `to_lowercase()` won't correspond to the same position in the original.

**Suggested fix:** Use a case-insensitive regex or search the original string directly. Since this is DuckDB-generated SQL (always ASCII), the current code works in practice, but adding a comment documenting this assumption would help.

#### m-2: `compute_partition_value` returns `None` for unsupported types (silent data loss)
**File:** `src/insert_exec.rs`, lines 293–427
**Issue:** If a partition column has an unsupported type (e.g., `Binary`, `Decimal128`), `compute_partition_value` returns `None`, which maps to `__HIVE_DEFAULT_PARTITION__`. This silently groups all rows with unsupported types into a single NULL partition rather than returning an error.

**Suggested fix:** Return `Result<Option<String>>` and propagate an error for unsupported partition column types, or at minimum log a warning.

#### m-3: `extract_temporal_component` only handles `TimestampMicrosecondArray`
**File:** `src/insert_exec.rs`, lines 436–482
**Issue:** The timestamp branch (line 466) only handles `TimestampMicrosecondArray`. Timestamps with second, millisecond, or nanosecond precision will silently return `None`.

**Suggested fix:** Handle all four Arrow timestamp precisions (Second, Millisecond, Microsecond, Nanosecond) or return an error for unsupported precisions.

#### m-4: Unused `_snapshot_id` parameter in `get_inlined_data_as_batch`
**File:** `src/table_writer.rs`, line 421
**Issue:** The `_snapshot_id` parameter is prefixed with underscore but is a required argument. This suggests an incomplete implementation or a parameter that was intended for snapshot-filtered reads.

**Suggested fix:** Either use the parameter for snapshot filtering in `read_inlined_data()` or remove it from the signature.

#### m-5: `pre-commit` hook uses hardcoded absolute path
**File:** `.githooks/pre-commit`, line 4
**Issue:** `CARGO_FMT="/home/zac/.cargo/bin/cargo fmt"` is a user-specific absolute path that won't work for other contributors.

**Suggested fix:** Use `cargo fmt` directly (relying on PATH) or use `$(which cargo) fmt`.

#### m-6: `DuckDbPgConn::query` uses infinite column iteration with error-based break
**Files:** `tests/cross_engine_postgres_tests.rs:162–168`, `tests/cross_engine_mysql_tests.rs:162–168`
**Issue:** The column iteration `for i in 0..` with `Err(_) => break` is a code smell — it relies on the DuckDB crate returning an error for out-of-bounds column access. This is correct but fragile.

**Suggested fix:** Get the column count from the statement metadata first, then iterate `0..column_count`.

#### m-7: `write_parquet_with_setup` uses `table_id` in path instead of table name
**File:** `src/table_writer.rs`, lines 439–443
**Issue:** When flushing inlined data via `write_parquet_with_setup`, the Parquet file goes to `<base>/schema/t<table_id>/uuid.parquet` instead of `<base>/schema/<table_name>/uuid.parquet`. This creates an inconsistency with normal writes that use the table name.

**Suggested fix:** Pass `table_name` through to this method, or look it up from `table_id`.

---

### Nit

#### N-1: Inconsistent `map_err` patterns
**File:** `src/insert_exec.rs`
**Issue:** Line 245 uses `.map_err(|e| DataFusionError::External(Box::new(e)))` while line 276 uses the same pattern. Consider a helper: `fn to_df_err(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> DataFusionError`.

#### N-2: `stream.map_err(|e: DataFusionError| e)` is a no-op
**File:** `src/insert_exec.rs`, line 285 (original main branch version)
**Issue:** The identity map_err was present in the original code but has been cleaned up in the integration branch. Good.

#### N-3: `file_name.clone()` on line 88-89 of original `table_writer.rs`
The `begin_write` method creates `file_name` then passes `file_name.clone()` and `file_name` separately. The integration branch version is cleaner.

#### N-4: `batches_to_inlined_rows` should use `arrow::util::display::ArrayFormatter` consistently
**File:** `src/table_writer.rs`, lines 696–778
**Issue:** `arrow_array_value_to_string` has a large match statement for each type. The fallback at line 768 uses `ArrayFormatter` — this could be used for all types to reduce code.

**Suggested fix:** Consider using `ArrayFormatter` for all types, unless specific formatting is needed for round-trip fidelity (which may be the case for some numeric types).

#### N-5: Test assertions use index-based access without bounds checking
**Files:** `tests/write_partition_tests.rs`, `tests/write_inline_tests.rs`
**Issue:** Assertions like `assert_eq!(rows[0], vec!["1", "Alice"])` will panic with an unhelpful message if `rows` is empty. Consider using `assert!(rows.len() >= N)` before indexing.

#### N-6: `normalize_value` in cross-engine tests doesn't handle edge cases
**Files:** `tests/cross_engine_postgres_tests.rs:271`, `tests/cross_engine_mysql_tests.rs:275`
**Issue:** `s.parse::<f64>()` will normalize "NaN", "inf", "-inf" which may not be desired. Also, comparing floats at 6 decimal places may mask precision differences.

---

## Codex CLI Findings

Codex was not available in this environment for a second-opinion review. The review was performed manually with thorough file-by-file analysis.

---

## Summary Statistics

| Severity | Count |
|----------|-------|
| Critical | 1     |
| Major    | 4     |
| Minor    | 7     |
| Nit      | 6     |
| **Total**| **18**|

## Positive Observations

1. **Clean trait extension**: `MetadataWriter` new methods all have default no-op implementations — backward compatible and idiomatic.
2. **Builder pattern**: `DataFileInfo::with_footer_size()`, `with_absolute_path()`, `DuckLakeInsertExec::with_partition_columns()` — consistent with Rust conventions.
3. **Proper error handling**: No `unwrap()` in production code. The `?` operator is used consistently. Error types are well-structured.
4. **Orphan cleanup**: `TableWriteSession::finish()` has best-effort cleanup of orphaned Parquet files on metadata commit failure (lines 611–626 of `table_writer.rs`).
5. **Snapshot consistency**: Write operations correctly create new snapshots and use end_snapshot for time-travel safety.
6. **Security-conscious**: `validate_table_name()` in `schema.rs` guards against path traversal — good defense-in-depth.
7. **Transaction usage**: `begin_write_transaction` in SQLite uses proper transactions with `tx.commit()` for atomicity.
8. **Comprehensive unit tests**: Good unit test coverage for pure functions (`build_hive_dir`, `compute_partition_value`, etc.).
