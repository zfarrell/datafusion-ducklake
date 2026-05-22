# R10 Idiomatic Review

## Summary

R10 is a full-codebase idiomatic Rust review on branch `ducklake-features/integration`, following 9 prior review cycles (~360 fixes applied). The codebase is in good shape — R9's macro/dialect refactoring landed cleanly with unit tests, Cow-based `ph()`, `unreachable!` for MySQL's `next_id_sql`, and `debug_assert!` on `upsert()`.

This review focuses on issues outside the macro/dialect code that prior reviews may have missed: DataFusion integration, write path executors, read path, and general Rust idioms.

**Findings**: 25 total (2 P0, 2 P1, 11 P2, 10 P3)
**Codex validation**: 5 findings from codex, 4 validated (1 overlaps with R10-I-008)

---

## Findings

### R10-I-CX1: `clear_inlined_data` failure after Parquet commit can cause duplicate data on retry (Priority: P0)
**File**: `src/table_writer.rs:368-371`
**Category**: Correctness
**Source**: Codex (validated)
**Description**: In `write_or_inline()`, after a successful Parquet write and metadata commit (`write_parquet_with_setup`), `clear_inlined_data()` is called. If `clear_inlined_data` fails and the error propagates, the caller may retry the entire write. On retry, the old inline rows are still present AND the Parquet file with those rows is already committed. The retry would write both the inline rows and new data again, causing data duplication. The comment "R4-S-001: Clear inlined data only AFTER successful Parquet write" correctly orders the operations but doesn't handle the failure-after-commit case.
**Suggested fix**: Treat `clear_inlined_data` failure as non-fatal after successful Parquet commit — log a warning and return success. The inline data will be cleared on the next write or can be cleaned up via a maintenance operation.
**Effort**: S

### R10-I-CX2: Same duplicate-data pattern in `flush_inlined_data` (Priority: P0)
**File**: `src/table_writer.rs:459-461`
**Category**: Correctness
**Source**: Codex (validated)
**Description**: Same pattern as R10-I-CX1: `flush_inlined_data()` calls `clear_inlined_data` after successful Parquet write. If the clear fails, the error propagates, and retrying would re-flush the same inline rows to a new Parquet file — duplicating data.
**Suggested fix**: Same as R10-I-CX1 — make `clear_inlined_data` failure non-fatal after successful Parquet commit.
**Effort**: S

### R10-I-CX3: `filter_map` silently drops partition columns not found in schema (Priority: P1)
**File**: `src/table.rs:1776-1797`
**Category**: Correctness
**Source**: Codex (validated)
**Description**: When building write-side partition columns, `filter_map` silently skips partition columns that aren't found in `self.schema` (the `.position()` returns `None`). This means if a partition column was renamed or doesn't exist, the write proceeds with incomplete partitioning — files are written without the expected partition directory structure, leading to incorrect data layout that may not be queryable by partition-aware readers.
**Suggested fix**: Replace `filter_map` with `map` and return an error when a partition column is not found in the schema: `return Err(DataFusionError::Plan(format!("Partition column '{}' not found in table schema", pc.column_name)))`.
**Effort**: S

### R10-I-001: `UploadCleanupGuard::drop` spawns thread + tokio runtime for cleanup (Priority: P2)
**File**: `src/table_writer.rs:1604-1622`
**Category**: Performance / Correctness
**Description**: The `Drop` impl spawns a new OS thread and builds a new tokio current-thread runtime to perform async cleanup. This has several issues: (1) spawning a thread in `drop()` is heavyweight and can silently fail if the thread pool is exhausted, (2) the spawned thread is detached (`let _ = std::thread::spawn(...)`) so cleanup may not complete before process exit, (3) this runs in a context where a tokio runtime already exists (inside `execute()`).
**Suggested fix**: Use `tokio::task::spawn` to run cleanup on the existing runtime. Since we're inside `Drop` and may be on a sync stack, use `tokio::runtime::Handle::try_current()` to get the existing handle and spawn on it. If no runtime is available, fall back to the current approach.
**Effort**: S

### R10-I-002: `from_ymd_opt(1970, 1, 1).unwrap()` in production code (Priority: P3)
**File**: `src/table.rs:1845,1856`
**Category**: Error handling
**Description**: `parse_stat_value()` uses `chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()` twice. While the UNIX epoch date is always valid (making the `unwrap` technically safe), this is not idiomatic. Any `unwrap` in non-test code triggers scrutiny.
**Suggested fix**: Extract a `const UNIX_EPOCH_DATE` (like `parse_values.rs` already does) or use `NaiveDate::from_ymd_opt(1970, 1, 1).expect("UNIX epoch is valid")`. Even better, share `UNIX_EPOCH_DATE` from `parse_values.rs` which already defines it.
**Effort**: S

### R10-I-003: Overly broad public API surface — 28 `pub mod` vs 2 `pub(crate) mod` (Priority: P2)
**File**: `src/lib.rs:38-102`
**Category**: Code organization
**Description**: Almost every module is `pub mod`, exposing internal implementation details as public API. Modules like `cdc_common`, `column_rename`, `delete_filter`, `parse_values`, `types`, `path_resolver`, `table_changes`, `table_deletions`, `table_insertions` contain internal types that external consumers shouldn't depend on. Key public types are already re-exported at crate root. Having 28 public modules creates a large semver surface area where any internal refactoring becomes a breaking change.
**Suggested fix**: Change internal modules to `pub(crate) mod`. Keep `pub mod` only for modules with types explicitly intended for external use: `catalog`, `error`, `metadata_provider`, `schema`, `table`, `table_functions`, `virtual_column_exec`, and the feature-gated provider/writer modules. Re-export needed public types from `lib.rs`.
**Effort**: M

### R10-I-004: Repeated error wrapping pattern — 122 instances of `.map_err(|e| DataFusionError::External(Box::new(e)))` (Priority: P3)
**File**: Multiple files (merge_exec.rs, update_exec.rs, delete_exec.rs, table_changes.rs, etc.)
**Category**: Code organization / Consistency
**Description**: The pattern `.map_err(|e| DataFusionError::External(Box::new(e)))` appears 122 times across the codebase. Since `DuckLakeError` already implements `Into<DataFusionError>`, and `DuckLakeError` has `From` impls for Arrow, Parquet, ObjectStore, and IO errors, many of these could be simplified. However, within `execute()` closures that return `DataFusionResult`, the intermediate error types (e.g., `parquet::errors::ParquetError`) don't implement `Into<DataFusionError>` directly.
**Suggested fix**: Add a helper trait extension `trait IntoDfError { fn into_df_err(self) -> DataFusionError; }` or use a local `map_df_err` utility function to reduce boilerplate. Alternatively, wrap these closures' return types to go through `DuckLakeError` first.
**Effort**: M

### R10-I-005: DML exec plans clone `table_files`, `existing_deletes` and `filters` on every `execute()` call (Priority: P2)
**File**: `src/delete_exec.rs:177-184`, `src/update_exec.rs:188-199`, `src/merge_exec.rs:369-380`
**Category**: Performance / Ownership
**Description**: In `execute()`, DELETE/UPDATE/MERGE exec plans clone `Vec<DuckLakeTableFile>`, `HashMap<String, HashSet<i64>>`, and `Vec<Expr>` into the async block. `DuckLakeTableFile` contains multiple `String` fields and `Option<DuckLakeFileData>` (which has more strings). For tables with many files, this is O(N) allocations per execute call. Since `execute()` is typically called only once per partition (partition 0), this is not hot-path, but it's wasteful for large tables.
**Suggested fix**: Wrap these fields in `Arc` at construction time: `table_files: Arc<Vec<DuckLakeTableFile>>`, `existing_deletes: Arc<HashMap<...>>`. This changes O(N) clones to O(1) Arc increments. The `filters` could also be wrapped in `Arc<[Expr]>`.
**Effort**: S

### R10-I-006: `DuckLakeMergeExec` stores source data in the plan itself (Priority: P1)
**File**: `src/merge_exec.rs:78`
**Category**: DataFusion API misuse
**Description**: `DuckLakeMergeExec` stores `source_batches: Vec<RecordBatch>` directly in the execution plan struct. This is problematic because: (1) Execution plans are long-lived and may be cached by DataFusion's plan cache, causing the source data to live in memory indefinitely, (2) `with_new_children()` (line 345-355) returns `self` directly, which means the data follows the plan through optimizer passes, (3) The plan is not serializable (breaks distributed execution), (4) If `execute()` is called multiple times (e.g., speculative execution), the data is re-processed each time (by design but not documented).
**Suggested fix**: Use an `Arc<Vec<RecordBatch>>` to allow shared ownership without deep cloning during optimizer passes. Document that this plan is not suitable for distributed execution. Long-term, consider reading source data via a child `ExecutionPlan` instead.
**Effort**: M

### R10-I-007: `Schema::new()` called repeatedly to build identical DML count schema (Priority: P3)
**File**: `src/delete_exec.rs:37-43`
**Category**: Performance
**Description**: `make_dml_count_schema()` allocates a new `Arc<Schema>` on every call. It's called from `compute_properties()` in all 4 DML exec plans (DELETE, INSERT, UPDATE, MERGE) and also from `execute()` in INSERT. The schema is always identical: `(count: UInt64, non-null)`.
**Suggested fix**: Use `std::sync::LazyLock` or `once_cell::sync::Lazy` to compute the schema once: `static DML_COUNT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(Schema::new(...)))`.
**Effort**: S

### R10-I-008: `insert_exec::execute()` collects ALL input partitions sequentially (Priority: P2)
**File**: `src/insert_exec.rs:237-243`
**Category**: Performance
**Description**: The INSERT execute loop `for p in 0..num_partitions { ... try_collect().await }` collects input partitions sequentially rather than concurrently. For multi-partition inputs (e.g., after a repartition), this serializes what could be parallel reads, increasing latency proportional to partition count.
**Suggested fix**: Use `futures::future::try_join_all` to collect all partitions concurrently, or use `datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec` as a child to merge partitions before reaching INSERT.
**Effort**: S

### R10-I-009: `table.rs` field ID mapping rebuilds on every scan (Priority: P3)
**File**: `src/table.rs:130` (schema_mapping_cache), but scan still does work
**Category**: Performance
**Description**: `build_read_schema_with_field_id_mapping` is called per-file in `build_exec_for_file_with_deletes` and `build_exec_for_single_file`. The `OnceCell` cache at line 130 exists but is only used for the schema mapping, not for the file-building logic itself. The `PartitionedFile` construction iterates through columns each time. This is acceptable for typical file counts but could be optimized for tables with thousands of files.
**Suggested fix**: Low priority — the current pattern is correct. Consider caching the `FileGroup` construction for files that share the same schema.
**Effort**: M

### R10-I-010: `Schema::new(arrow_schema.fields().iter().cloned().collect::<Vec<_>>())` strips metadata (Priority: P2)
**File**: `src/insert_exec.rs:261-262`
**Category**: Correctness
**Description**: `Schema::new(arrow_schema.fields().iter().cloned().collect::<Vec<_>>())` creates a new schema from the fields but drops all schema-level metadata. This is intentional (the code comment says "schema_without_metadata") but loses metadata that Parquet writers may need (e.g., for round-trip fidelity). If the original schema had custom metadata keys, they're silently dropped.
**Suggested fix**: Add a comment explaining why metadata is stripped. If only specific metadata needs removal, use `Schema::new_with_metadata(fields, HashMap::new())` explicitly.
**Effort**: S

### R10-I-012: `source_match_masks` rebuilt per target file but only used for UPDATE (Priority: P3)
**File**: `src/merge_exec.rs:455-458`
**Category**: Performance
**Description**: In the MERGE execute loop, `source_match_masks` is allocated for every target file, even when `matched_action` is `Delete` or `None`. The allocation `source_batches.iter().map(|b| vec![false; b.num_rows()]).collect()` creates `O(source_rows)` booleans per file.
**Suggested fix**: Only allocate `source_match_masks` when `matches!(&matched_action, Some(MergeMatchedAction::Update))`.
**Effort**: S

### R10-I-013: `object_store_url.clone()` clones the Arc contents instead of Arc (Priority: P3)
**File**: `src/delete_exec.rs:182`, `src/merge_exec.rs:378`, `src/update_exec.rs:194`
**Category**: Ownership
**Description**: `self.object_store_url.clone()` on an `Arc<ObjectStoreUrl>` clones the Arc (cheap), but in some places `self.object_store_url.as_ref().clone()` is used which clones the inner `ObjectStoreUrl`. These are mixed in the codebase — some use `Arc::clone(&self.object_store_url)`, some use `.clone()`. Inconsistent but not a bug.
**Suggested fix**: Standardize on `Arc::clone(&self.object_store_url)` to make the cheap clone explicit.
**Effort**: S

### R10-I-014: `table_changes.rs` duplicates encryption/non-encryption methods via `#[cfg]` (Priority: P3)
**File**: `src/table_changes.rs:404-431`, `src/table_deletions.rs`, `src/table_insertions.rs`
**Category**: Code organization
**Description**: `build_exec_for_file` is duplicated with `#[cfg(feature = "encryption")]` and `#[cfg(not(feature = "encryption"))]` variants across table_changes.rs, table_deletions.rs, and table_insertions.rs (6 copies total). Each pair differs only in whether it accepts an `encryption_factory` parameter.
**Suggested fix**: Use a single method that takes `Option<Arc<dyn EncryptionFactory>>` — when the `encryption` feature is disabled, the `EncryptionFactory` type doesn't exist, but a cfg-gated type alias or a generic approach could unify them. Low priority since this is a common Rust pattern for feature gates.
**Effort**: M

### R10-I-015: `compaction_functions.rs` uses global `Mutex<bool>` for install tracking (Priority: P2)
**File**: `src/compaction_functions.rs:38`
**Category**: Correctness / Performance
**Description**: `static DUCKLAKE_INSTALLED: Mutex<bool>` is used to track whether `INSTALL ducklake` has been called. The mutex is acquired on every compaction function call. Since `INSTALL ducklake` is idempotent, the initial check could use `AtomicBool` with `Ordering::Relaxed` for the fast path, only acquiring the mutex for the install operation itself.
**Suggested fix**: Use `AtomicBool` for the check: `if !INSTALLED.load(Relaxed) { /* acquire mutex, double-check, install, store true */ }`. Or use `std::sync::Once` which is designed for exactly this pattern.
**Effort**: S

### R10-I-016: `information_schema` tables re-query metadata on every scan (Priority: P3)
**File**: `src/information_schema.rs:44-75` (macro)
**Category**: Performance
**Description**: Information schema tables (snapshots, schemata, tables, columns, files) re-query the entire metadata catalog on every scan, even for repeated queries within the same session. This is by design (documented as "live querying") but means `SELECT * FROM information_schema.tables` triggers a full metadata scan every time. For catalogs with many tables, this adds latency.
**Suggested fix**: Document this as a known characteristic. A future optimization could cache results for the duration of a snapshot (since DuckLake metadata is immutable within a snapshot).
**Effort**: L (if fixing), S (if documenting)

### R10-I-017: `DuckLakeDeleteExec` and `DuckLakeUpdateExec` lack `with_new_children` (Priority: P2)
**File**: `src/delete_exec.rs:152-162`, `src/update_exec.rs:162-172`
**Category**: DataFusion API
**Description**: `DuckLakeDeleteExec` and `DuckLakeUpdateExec` return `children() -> vec![]` but their `with_new_children` rejects non-empty children. While this is correct for leaf plans, these plans scan data files internally (in `execute`). If DataFusion's optimizer tries to inject nodes below them, it can't. This is the correct design for plans that manage their own I/O, but it should be documented.
**Suggested fix**: Add a doc comment on the struct explaining that these are leaf execution plans that manage their own file I/O. The optimizer cannot push filters or projections into them (filters are compiled at construction time from `self.filters`).
**Effort**: S

### R10-I-018: `extract_key_value` in merge_exec.rs allocates `String` for every key comparison (Priority: P2)
**File**: `src/merge_exec.rs:282-288`
**Category**: Performance
**Description**: For `Utf8` and `LargeUtf8` join keys, `extract_key_value` calls `.value(row).to_string()` which allocates a new `String` for every row. In `build_source_hash_index`, this means O(source_rows) string allocations for building the index, and then O(target_rows) allocations during the join probe. For large MERGE operations with string keys, this is significant.
**Suggested fix**: Use `Cow<'_, str>` or store borrowed `&str` with a lifetime tied to the batch. Alternatively, hash the string in-place without allocating.
**Effort**: M

### R10-I-019: `validate_not_null_constraints` iterates all columns for every batch (Priority: P3)
**File**: `src/table_writer.rs` (called from insert_exec.rs:251, merge_exec.rs:594)
**Category**: Performance
**Description**: NOT NULL validation iterates through all columns of every batch, calling `null_count()` which may need to count bits for non-null columns. For tables with many non-nullable columns, this adds overhead proportional to `columns * batches`.
**Suggested fix**: Only check columns that are non-nullable (`!field.is_nullable()`). Pre-compute the non-nullable column indices once.
**Effort**: S

### R10-I-020: `parse_stat_value` returns `None` for timestamp types (Priority: P2)
**File**: `src/table.rs:1818-1900` (approximately)
**Category**: Correctness / Completeness
**Description**: `parse_stat_value()` handles Int/UInt/Float/String/Boolean/Date types but falls through to `None` for Timestamp types. This means timestamp column statistics (min/max) stored in the catalog are silently ignored during query planning, preventing timestamp-based file pruning. Since DuckLake stores timestamps in catalog column stats, this is a real data-skipping opportunity being missed.
**Suggested fix**: Add timestamp parsing cases that handle the various `TimeUnit` variants (Second, Millisecond, Microsecond, Nanosecond) with appropriate string-to-timestamp conversion.
**Effort**: M

---

## Summary Table

| Priority | Count | Key Themes |
|----------|-------|------------|
| P0 | 2 | Inline data clear-after-commit can cause duplicates on retry |
| P1 | 2 | MERGE stores data in plan, silent partition column drop |
| P2 | 11 | API surface, cleanup guard, DML clones, timestamp stats, insert partition collection, error conversion |
| P3 | 10 | Epoch unwrap, error wrapping, schema allocation, style consistency, panic messages |

## Recommended Fix Priority

1. **R10-I-CX1, R10-I-CX2** (P0): Make `clear_inlined_data` failure non-fatal after Parquet commit
2. **R10-I-CX3** (P1): Error on missing partition column instead of silent skip
3. **R10-I-006** (P1): Wrap `source_batches` in `Arc` to prevent deep cloning during optimizer passes
4. **R10-I-020** (P2): Add timestamp parsing to `parse_stat_value` — real performance win for timestamp-heavy workloads
5. **R10-I-001** (P2): Fix `UploadCleanupGuard::drop` to use existing tokio runtime
6. **R10-I-005** (P2): Arc-wrap DML plan fields to eliminate O(N) clones
7. **R10-I-003** (P2): Tighten module visibility to reduce semver surface
8. **R10-I-008** (P2): Parallelize input partition collection in INSERT
9. Remaining P2/P3 in any order

### R10-I-021: Hardcoded `0` in unreachable panic messages (Priority: P3)
**File**: `src/table_changes.rs:789`, `src/table_deletions.rs:350`
**Category**: Error handling
**Source**: Agent (metadata/table functions)
**Description**: Both files contain `.unwrap_or_else(|| panic!("expected exactly 1 exec, got {}", 0))` where the format argument is hardcoded as `0` instead of `execs.len()`. The panic is unreachable (guarded by `execs.len() == 1`), but if it ever fired, the error message would always say "got 0" regardless of the actual count.
**Suggested fix**: Change to `unreachable!("execs.len() == 1 but .next() returned None")` since the panic path is logically impossible.
**Effort**: S

### R10-I-022: `DuckLakeError → DataFusionError` conversion wraps Arrow/IO errors as opaque External (Priority: P2)
**File**: `src/error.rs:79-87`
**Category**: Error handling
**Source**: Agent (read path)
**Description**: The `From<DuckLakeError> for DataFusionError` impl wraps all non-DataFusion errors as `External(Box::new(...))`. This loses specific error type information — `DuckLakeError::Arrow(e)` becomes `External(Box(Arrow(e)))` instead of `DataFusionError::ArrowError(e)`. Callers matching on specific DataFusion error variants can't distinguish DuckLake Arrow errors from other external errors.
**Suggested fix**: Map specific variants: `DuckLakeError::Arrow(e) → DataFusionError::ArrowError(Box::new(e), None)`, `DuckLakeError::Io(e) → DataFusionError::IoError(e)`.
**Effort**: S

### R10-I-023: DuckDB metadata provider uses `SqliteDialect` for identifier quoting (Priority: P3)
**File**: `src/metadata_provider_duckdb.rs:2,118,641,658,663`
**Category**: Consistency
**Source**: Agent (metadata/table functions)
**Description**: The DuckDB metadata provider imports and uses `SqliteDialect` for `quote_id()`. This works because both DuckDB and SQLite use double-quote (`"`) identifier quoting, but the naming is misleading — it suggests the DuckDB provider depends on SQLite-specific behavior.
**Suggested fix**: Create a `DuckDbDialect` struct (with the same `quote_id` implementation as SQLite) or rename to clarify that both share standard SQL double-quote quoting. Alternatively, add a comment explaining why `SqliteDialect` is used here.
**Effort**: S

## Pre-existing Issues (Not New)

The following R9 findings remain unfixed and are still valid:
- R9-S-008: DDL snapshot creation boilerplate repeated 7x in macro
- R9-S-011: `pool_type` macro parameter accepted but never used
- R9-S-016: Many dialect methods allocate where Cow would suffice
- R9-S-018: Repeated `use crate::dialect::SqlDialect` in every macro method body
