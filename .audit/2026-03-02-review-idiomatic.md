# Idiomatic Rust & Code Quality Review

**Date**: 2026-03-02
**Branch**: `ducklake-features/integration`
**Scope**: All 33 Rust source files in `src/`
**Focus**: Idiomatic Rust patterns, DataFusion API usage, code quality, performance

---

## Summary

The codebase is well-structured overall with clear separation of concerns, proper use of DataFusion traits, and consistent error handling patterns. The main systemic issues are: (1) massive code duplication across metadata provider and writer backends, (2) a sync MetadataWriter/MetadataProvider trait design forcing `block_on()` bridges everywhere, and (3) several places where `unwrap()` is used in non-test code paths.

**Findings by severity:**
- P0 (Critical): 0
- P1 (High): 3
- P2 (Medium): 10
- P3 (Low): 8

---

## Findings

### ID-01: `unwrap()` on array downcasts in `merge_exec.rs::values_equal()`
- **Severity**: P1
- **File(s)**: `src/merge_exec.rs:195-238`
- **Description**: The `values_equal()` function uses `.unwrap()` on `downcast_ref()` calls for every Arrow array type (Int32Array, Int64Array, Utf8Array, etc.). If the array type doesn't match the expected type (e.g., due to schema evolution or unexpected input), this will panic in production.
- **Suggestion**: Replace `.unwrap()` with `.ok_or_else(|| DataFusionError::Internal("unexpected array type ..."))` and propagate the error. Change the function signature to return `DataFusionResult<bool>`.
- **Effort**: S

### ID-02: `unwrap()` on `into_iter().next()` in scan methods
- **Severity**: P1
- **File(s)**: `src/table_deletions.rs:260`, `src/table_changes.rs:594`
- **Description**: When `execs.len() == 1`, the code calls `execs.into_iter().next().unwrap()`. While the length check makes this logically safe, it's non-idiomatic and fragile to refactoring. A future change could introduce a bug where the length check diverges from the unwrap.
- **Suggestion**: Use `execs.into_iter().next().expect("checked len == 1 above")` or better, restructure to avoid the pattern entirely (e.g., `if let [single] = &execs[..]`).
- **Effort**: S

### ID-03: `#[allow(dead_code)]` on `catalog_path` field
- **Severity**: P1
- **File(s)**: `src/metadata_provider_duckdb.rs:29`
- **Description**: The `catalog_path` field in `DuckdbMetadataProvider` is marked `#[allow(dead_code)]` with a comment "retained for logging/debugging". If it's not being used, it should either be removed or actually used in error messages/tracing spans. Suppressing dead code warnings is a code smell.
- **Suggestion**: Either remove the field, or use it in error messages (e.g., in the `connection()` method's error path).
- **Effort**: S

### ID-04: Massive code duplication across metadata provider backends
- **Severity**: P2
- **File(s)**: `src/metadata_provider_sqlite.rs`, `src/metadata_provider_postgres.rs`, `src/metadata_provider_mysql.rs`
- **Description**: The SQLite, PostgreSQL, and MySQL metadata providers contain nearly identical row-mapping logic. Each implements ~20 methods that differ only in: (1) SQL placeholder syntax (`?` vs `$1`), (2) pool type, and (3) minor dialect differences. The row-mapping closures (`|row| { ... }`) are copy-pasted across all three backends.
- **Suggestion**: Extract shared row-mapping functions into `metadata_provider.rs` (e.g., `fn map_schema_row(row: &impl Row) -> Result<SchemaMetadata>`). Consider a macro or generic helper to generate the bind+fetch+map boilerplate. The DuckDB provider shares the same SQL constants already defined in `metadata_provider.rs`, but the sqlx providers inline their own SQL.
- **Effort**: L

### ID-05: Massive code duplication across metadata writer backends
- **Severity**: P2
- **File(s)**: `src/metadata_writer_sqlite.rs`, `src/metadata_writer_postgres.rs`, `src/metadata_writer_mysql.rs`
- **Description**: Similar to ID-04, the three writer backends share nearly identical transaction logic (~1000+ lines each) with differences only in SQL dialect. The `write_transaction_inner()`, `drop_table_inner()`, `drop_schema_inner()`, `alter_table_inner()` methods are essentially the same across backends.
- **Suggestion**: Extract the shared logic into a generic transaction helper that takes a database-specific SQL builder/adapter. The `metadata_writer_validation.rs` extraction is a good start—continue this pattern for the remaining shared logic.
- **Effort**: L

### ID-06: Sync trait with `block_on()` bridge pattern
- **Severity**: P2
- **File(s)**: `src/metadata_provider.rs:766-771`, `src/metadata_writer.rs`, all sqlx-based backends
- **Description**: Both `MetadataProvider` and `MetadataWriter` are sync traits (`fn method(&self) -> Result<T>`), but the SQLite/Postgres/MySQL implementations use async sqlx. This forces every method to wrap its body in `block_on(async { ... })`, adding runtime overhead and obscuring the code. The `block_on` helper uses `tokio::task::block_in_place()` which requires a multi-threaded Tokio runtime.
- **Suggestion**: Consider making the traits async (using `#[async_trait]`). The DuckDB provider can use a trivial async wrapper. This would eliminate ~60+ `block_on()` calls across the codebase and simplify the code. Note: DataFusion's `TableProvider` is already async, so this aligns with the ecosystem.
- **Effort**: L

### ID-07: Repetitive `map_err(|e| DataFusionError::External(Box::new(e)))` pattern
- **Severity**: P2
- **File(s)**: `src/compaction_functions.rs` (throughout), `src/table_insertions.rs`, `src/table_deletions.rs`, `src/table_changes.rs`, `src/schema.rs`, `src/catalog.rs`
- **Description**: The pattern `map_err(|e| DataFusionError::External(Box::new(e)))` is repeated 50+ times across the codebase. In `compaction_functions.rs` alone, it appears ~30 times for DuckDB error conversion.
- **Suggestion**: Add a helper trait extension: `trait IntoDataFusionError<T> { fn into_df_err(self) -> DataFusionResult<T>; }` or a helper function `fn to_df_err(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError`. This is partially addressed by the `From<DuckLakeError> for DataFusionError` impl, but many call sites use raw DuckDB/sqlx errors directly.
- **Effort**: S

### ID-08: Heavy field cloning in `execute()` methods
- **Severity**: P2
- **File(s)**: `src/insert_exec.rs`, `src/delete_exec.rs`, `src/update_exec.rs`, `src/merge_exec.rs`
- **Description**: The `execute()` methods on all DML execution plans clone many `Arc<>` and `String` fields to move into async blocks. For example, `insert_exec.rs` clones `table_writer`, `metadata_writer`, `table_id`, `snapshot_id`, `table_path`, `object_store_url`, `partition_columns`, and more. While `Arc::clone()` is cheap, the `String` clones are unnecessary allocations.
- **Suggestion**: Wrap the frequently-cloned String fields in `Arc<str>` or `Arc<String>` to make cloning O(1). Alternatively, restructure to pass a single `Arc<ExecutionContext>` struct. This is a common pattern in DataFusion execution plans.
- **Effort**: M

### ID-09: Row-by-row partition routing in `insert_exec.rs`
- **Severity**: P2
- **File(s)**: `src/insert_exec.rs` (`route_batches_to_partitions()`)
- **Description**: The `route_batches_to_partitions()` function iterates row-by-row to compute partition values and route rows into a `BTreeMap<Vec<String>, Vec<(usize, Vec<usize>)>>`. For each row, it evaluates partition expressions and performs string formatting. This is O(rows × partitions) and creates many small allocations.
- **Suggestion**: For the common case of identity partitions, compute partition values column-wise (vectorized) using Arrow's dictionary/group-by operations, then split the batch. The row-by-row approach is only needed for computed transforms (year, month, etc.).
- **Effort**: M

### ID-10: `compaction_functions.rs` boilerplate for DuckDB row extraction
- **Severity**: P2
- **File(s)**: `src/compaction_functions.rs:273-359`, `src/compaction_functions.rs:520-595`
- **Description**: The `expire_snapshots` and `ducklake_options` functions manually extract 5-7 columns from DuckDB rows into parallel `Vec`s, one column at a time, with identical error mapping. This is extremely verbose (~80 lines for each function that could be ~10 lines).
- **Suggestion**: Create a helper that takes a DuckDB `Rows` iterator and a mapping closure (similar to `query_map` in the DuckDB crate). Or use the `duckdb` crate's `query_map` method directly instead of `prepare` + `query` + manual iteration.
- **Effort**: S

### ID-11: `bind_repeat!` macro in Postgres provider
- **Severity**: P2
- **File(s)**: `src/metadata_provider_postgres.rs:16-49`
- **Description**: The `bind_repeat!` macro is defined to bind the same value N times to a sqlx query. It has hardcoded variants for 1, 2, 3, 4, 6, and 8 repetitions. This is fragile (adding a new count requires a new variant) and non-standard.
- **Suggestion**: Use a loop or fold instead: `(0..n).fold(query, |q, _| q.bind(value))`. Or better yet, share SQL constants that use `?` placeholders (SQLite/MySQL style) and convert to `$N` style programmatically for Postgres. This would also help with ID-04.
- **Effort**: S

### ID-12: `pub pool` field exposure on metadata providers
- **Severity**: P2
- **File(s)**: `src/metadata_provider_sqlite.rs:23`, `src/metadata_provider_postgres.rs:56`, `src/metadata_provider_mysql.rs:23`
- **Description**: The `pool` field is `pub` on all three sqlx-based metadata providers. This breaks encapsulation and allows external code to bypass the `MetadataProvider` trait, execute arbitrary SQL, or misuse the pool.
- **Suggestion**: Make the `pool` field `pub(crate)` or private, and add a `pool()` accessor method if external access is genuinely needed.
- **Effort**: S

### ID-13: `SQL_GET_DELETE_FILES_ADDED_BETWEEN_SNAPSHOTS` uses `LEFT JOIN LATERAL`
- **Severity**: P2
- **File(s)**: `src/metadata_provider.rs:150-255`
- **Description**: This complex SQL query uses `LEFT JOIN LATERAL ... ON true` which is a PostgreSQL/DuckDB syntax extension. SQLite does not support `LATERAL` joins, so the sqlx-based SQLite provider cannot use this shared constant and must inline its own version. This creates a divergence between the DuckDB provider (which uses the shared constant) and the sqlx providers.
- **Suggestion**: Either: (a) document that this constant is DuckDB-only, (b) provide SQLite-compatible alternatives using correlated subqueries, or (c) move database-specific SQL into the respective provider modules.
- **Effort**: M

### ID-14: Missing `Vec::with_capacity()` pre-allocation
- **Severity**: P3
- **File(s)**: `src/table_deletions.rs:639`, `src/table_changes.rs:240`, `src/compaction_functions.rs:89,125,299-305,545-549`
- **Description**: Several places create `Vec::new()` where the final size is known or estimable. For example, `columns` vectors in `filter_batch()` and `transform_batch()` could use `with_capacity()`.
- **Suggestion**: Use `Vec::with_capacity()` where the size is known. Minor performance impact but more idiomatic.
- **Effort**: S

### ID-15: `DuckLakeTableFile` has many `Option` fields used inconsistently
- **Severity**: P3
- **File(s)**: `src/metadata_provider.rs:534-548`
- **Description**: `DuckLakeTableFile` has 6 fields, 5 of which are `Option`. The `data_file_id`, `row_id_start`, `snapshot_id`, and `max_row_count` fields are only populated in certain contexts (read vs write). This makes the struct a "god object" that serves multiple purposes.
- **Suggestion**: Consider splitting into separate structs for read (`DataFileForRead`) and write (`DataFileForWrite`) contexts, or use a builder pattern. This would make the API clearer about which fields are available in which context.
- **Effort**: M

### ID-16: Inconsistent error type usage
- **Severity**: P3
- **File(s)**: `src/error.rs`, various callers
- **Description**: Some functions return `crate::Result<T>` (using `DuckLakeError`), while others return `DataFusionResult<T>`. The boundary between these two error domains is not always consistent. For example, `path_resolver` functions return `crate::Result`, but their callers in table providers immediately `map_err` to `DataFusionError`.
- **Suggestion**: Establish a clear convention: internal modules use `crate::Result`, DataFusion trait implementations use `DataFusionResult`. The `From<DuckLakeError> for DataFusionError` impl already supports this—use `?` operator with the From impl instead of explicit `map_err`.
- **Effort**: S

### ID-17: `schema()` methods clone `Arc<Schema>` unnecessarily
- **Severity**: P3
- **File(s)**: Multiple files implementing `TableProvider::schema()` and `ExecutionPlan::schema()`
- **Description**: Many `schema()` implementations return `self.output_schema.clone()` where `output_schema` is already an `Arc<Schema>`. The clone is just an Arc refcount bump (cheap), but some DataFusion APIs accept `&SchemaRef` and the clone is truly unnecessary.
- **Suggestion**: This is actually the correct pattern for DataFusion's API which returns `SchemaRef` (= `Arc<Schema>`) by value. No change needed, but noting for awareness.
- **Effort**: N/A (informational)

### ID-18: `open_compaction_connection()` creates a new DuckDB connection per function call
- **Severity**: P3
- **File(s)**: `src/compaction_functions.rs:59-73`
- **Description**: Each compaction table function creates a fresh in-memory DuckDB connection, installs+loads the ducklake extension, and ATTACHes the catalog. This is expensive and happens every time the function is called.
- **Suggestion**: Consider caching the connection (e.g., in a `OnceCell` or `Lazy` static keyed by catalog path). However, since these are maintenance operations that run infrequently, this is low priority.
- **Effort**: M

### ID-19: `DeletedRowsStream` does not implement `Debug`
- **Severity**: P3
- **File(s)**: `src/table_deletions.rs:472`
- **Description**: `DeletedRowsStream` is missing a `Debug` implementation. While internal streams don't strictly need it, it makes debugging harder. The `CurrentDeletePositions` and `DeltaPositions` enums are also missing `Debug`.
- **Suggestion**: Add `#[derive(Debug)]` to these types, or implement `Debug` manually if the stream fields don't support it.
- **Effort**: S

### ID-20: `AppendCDCColumnsStream` does not implement `Debug`
- **Severity**: P3
- **File(s)**: `src/table_changes.rs:211`
- **Description**: Same as ID-19 but for `AppendCDCColumnsStream`.
- **Suggestion**: Add `Debug` implementation.
- **Effort**: S

### ID-21: Feature gate duplication with `#[cfg(feature = "encryption")]` blocks
- **Severity**: P3
- **File(s)**: `src/table_insertions.rs:116-134`, `src/table_changes.rs:401-428`, `src/table.rs`
- **Description**: Multiple files have duplicated `#[cfg(feature = "encryption")]` / `#[cfg(not(feature = "encryption"))]` blocks for encryption factory setup and method signatures. This creates two copies of methods that differ only in whether they accept an encryption parameter.
- **Suggestion**: Use a wrapper type that encapsulates the encryption-or-not logic, or use `Option<Arc<dyn EncryptionFactory>>` unconditionally (with the type defined as a no-op when the feature is disabled).
- **Effort**: M

---

## Positive Patterns (Worth Preserving)

1. **Clean DataFusion trait implementations**: `DuckLakeCatalog`, `DuckLakeSchema`, and `DuckLakeTable` correctly implement DataFusion's catalog interfaces with dynamic lookup and proper async support.

2. **Proper `ExecutionPlan` implementations**: All custom execution plans (`DeleteFilterExec`, `DeletedRowsExec`, `AppendCDCColumnsExec`, `VirtualColumnExec`, `ColumnRenameExec`) correctly implement `children()`, `with_new_children()`, `properties()`, and `DisplayAs`.

3. **Shared validation extraction**: `metadata_writer_validation.rs` properly extracts common validation logic, demonstrating the right direction for reducing duplication.

4. **Security-conscious design**: Path traversal prevention in `path_resolver.rs`, null byte validation, input sanitization in schema/table names, and encryption key hiding in `Debug` impls.

5. **Footer size optimization**: Passing Parquet footer size hints to reduce I/O operations—a meaningful performance optimization for remote object stores.

6. **Snapshot isolation**: Consistent use of snapshot IDs throughout the query path ensures temporal consistency.

7. **Good use of `thiserror`**: The `DuckLakeError` enum uses `thiserror` for clean error definitions with proper `From` implementations.

8. **`block_on` helper**: The centralized `block_on()` helper using `tokio::task::block_in_place()` + `Handle::current().block_on()` is the correct way to bridge sync/async when needed.

---

## Architecture Observations

### Module Organization
The module structure cleanly separates concerns:
- **Provider layer**: `metadata_provider*.rs` (read), `metadata_writer*.rs` (write)
- **DataFusion integration**: `catalog.rs`, `schema.rs`, `table.rs`
- **Execution plans**: `*_exec.rs` (insert, delete, update, merge)
- **Utilities**: `types.rs`, `path_resolver.rs`, `error.rs`
- **Features**: `table_changes.rs`, `table_insertions.rs`, `table_deletions.rs`, `compaction_functions.rs`

### Feature Gating
Feature gates are well-organized but create some code duplication (ID-21). The feature structure is:
- `write`: DML operations (insert, delete, update, merge)
- `metadata-sqlite`, `metadata-postgres`, `metadata-mysql`: Database backends
- `metadata-duckdb`: DuckDB provider + compaction functions
- `encryption`: Parquet Modular Encryption support

### DataFusion API Usage
The codebase uses DataFusion APIs correctly:
- `FileScanConfigBuilder` + `ParquetSource` + `DataSourceExec` for Parquet scanning
- `UnionExec` for combining multiple file scans
- `TableFunctionImpl` for UDTFs
- `QueryPlanner` for intercepting DML statements
- `EquivalenceProperties` + `PlanProperties` for plan metadata
