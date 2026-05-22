# R11 Idiomatic Rust & DataFusion Review

**Date**: 2026-03-06
**Branch**: `ducklake-features/integration`
**Reviewer**: Claude Opus 4.6 (agent: r11-idiomatic)
**Codex second opinion**: Applied to table_writer.rs, merge_exec.rs, insert_exec.rs

## Summary

Reviewed all `src/*.rs` files (~35k lines) with focus on error handling, ownership/borrowing,
DataFusion API idiomacy, consistency, performance (allocations, clone abuse, Arc usage), and dead code.

**Totals**: 0 P0, 3 P1, 8 P2, 5 P3 (16 findings)

---

## P1 — High (correctness or data-loss risk)

### R11-I-001: Orphan Parquet files leaked on metadata commit failure in partitioned INSERT

**File**: `src/insert_exec.rs:804-816`
**Severity**: P1
**Description**: In `write_partitioned()`, after all partition Parquet files are uploaded, the metadata
commit (`register_files_for_table`) can fail. If it does, only the *last* writer's `UploadCleanupGuard`
is active — earlier partitions' guards have already been defused (`.uploaded_path()` consumed). This
leaves orphaned Parquet files in object storage with no catalog reference.
**Suggested fix**: Collect all uploaded paths into a `Vec` and wrap them in a composite cleanup guard
that deletes all files on drop. Only defuse after the metadata commit succeeds.

### R11-I-002: Timestamp arithmetic overflow in arrow_array_value_to_string

**File**: `src/table_writer.rs:1158-1169`
**Severity**: P1
**Description**: Timestamp extraction uses unchecked multiplication (`value * 1_000` for microseconds,
`value * 1_000_000` for milliseconds) to normalize to nanoseconds before calling
`DateTime::from_timestamp_nanos`. Extreme timestamp values (e.g., year 9999 in microseconds) will
silently overflow `i64`, producing incorrect partition keys and misrouted data.
**Suggested fix**: Use `checked_mul()` and return an error on overflow, or use `DateTime::from_timestamp_micros` /
`DateTime::from_timestamp_millis` directly without converting to nanos.

### R11-I-003: Full stream materialization in INSERT risks OOM

**File**: `src/insert_exec.rs:246`
**Severity**: P1
**Description**: `execute_insert` calls `common::collect(input.execute(0, …)?)` which materializes the
entire input stream into memory before writing. For large INSERT...SELECT queries this can exhaust memory.
The non-partitioned path (`write_single`) also materializes fully before writing Parquet.
**Suggested fix**: Stream batches through an `ArrowWriter` incrementally. For partitioned writes, buffer
per-partition with a spill-to-disk threshold (similar to `MAX_BUFFERED_ROWS` in update_exec).

---

## P2 — Medium (performance, robustness, or idiom issues)

### R11-I-004: Silent saturation in null-count statistics

**File**: `src/table_writer.rs:1325`
**Severity**: P2
**Description**: Null count is computed as `stats.null_count.map(|n| n as i64)`. If the `u64` null count
exceeds `i64::MAX`, this silently wraps/saturates. While unlikely in practice, the pattern is non-idiomatic.
**Suggested fix**: Use `i64::try_from(n).unwrap_or(i64::MAX)` or store as `u64` if the metadata schema allows.

### R11-I-005: O(n*m) position lookup in inlined_rows_to_batch

**File**: `src/table_writer.rs:1241`
**Severity**: P2
**Description**: `inlined_rows_to_batch` iterates over `positions` (n) and for each calls `.iter().position()`
on the column names list (m). For tables with many columns this is O(n*m). The function is called per
flush of inlined data.
**Suggested fix**: Build a `HashMap<&str, usize>` from column names to field indices once, then look up
each position in O(1).

### R11-I-006: Per-row Vec<HashableKeyValue> allocation in MERGE join

**File**: `src/merge_exec.rs:478`
**Severity**: P2
**Description**: The hash-join loop allocates a `Vec<HashableKeyValue>` for every row in the source batch
to build the join key. For large batches (e.g., 8192 rows), this means thousands of small heap allocations
per batch.
**Suggested fix**: Pre-allocate a reusable `Vec` outside the row loop and `.clear()` + refill each iteration,
or switch to a columnar hash approach using DataFusion's `HashJoinExec`.

### R11-I-007: String allocation per key value in MERGE hash extraction

**File**: `src/merge_exec.rs:285`
**Severity**: P2
**Description**: `extract_key_values` converts every key column value to a `HashableKeyValue`, which for
string types clones the string via `value.to_string()`. Combined with R11-I-006, this creates significant
allocation pressure in the MERGE hot path.
**Suggested fix**: Use borrowed references or interned strings where possible. Consider a batch-columnar
hashing approach that avoids per-row extraction entirely.

### R11-I-008: Duplicated Parquet write boilerplate between merge_exec and update_exec

**File**: `src/merge_exec.rs:580-650`, `src/update_exec.rs:350-420`
**Severity**: P2
**Description**: Both `merge_exec.rs` and `update_exec.rs` contain nearly identical Parquet file writing
code (create ArrowWriter, write batches, close, upload, register). This violates DRY and increases
maintenance burden.
**Suggested fix**: Extract shared Parquet write logic into a helper in `table_writer.rs` (e.g.,
`write_batches_to_parquet_file`) that both exec plans can call.

### R11-I-009: Unchecked `as i64` casts in metadata_writer_impl macros

**File**: `src/metadata_writer_impl.rs:313`, `src/metadata_writer_impl.rs:1476`
**Severity**: P2
**Description**: `partition_key_index as i64` (line 313) and `column_order as i64` (line 1476) cast
`usize` to `i64`. On 64-bit platforms, `usize` values above `i64::MAX` would silently wrap. While
partition key indices and column orders are practically small, the cast is non-idiomatic.
**Suggested fix**: Use `i64::try_from(value).expect("index out of range")` or propagate an error. For
values known to be small, add a debug assertion.

### R11-I-010: Unnecessary partition_values.clone() in partitioned INSERT

**File**: `src/insert_exec.rs:804`
**Severity**: P2
**Description**: `partition_values` is cloned when passed to the metadata registration call, but the
original is not used afterward (the loop iteration is complete). The clone allocates a new `Vec<String>`.
**Suggested fix**: Use `std::mem::take` or pass ownership directly since the value is not needed after.

### R11-I-011: `as u64` / `as i64` widening casts in extract_key macros

**File**: `src/merge_exec.rs:223-235`
**Severity**: P2
**Description**: The `extract_key!` macro uses `as i64` and `as u64` for widening integer casts (e.g.,
`Int8` to `i64`). While these particular casts are lossless (widening), the `as` keyword in Rust doesn't
communicate intent — `i64::from(value)` is idiomatic for infallible widening conversions and prevents
accidental narrowing if types change.
**Suggested fix**: Replace `value as i64` with `i64::from(value)` for widening casts.

---

## P3 — Low (style, minor optimization)

### R11-I-012: Avoidable mask clone in MERGE matched-row processing

**File**: `src/merge_exec.rs:540`
**Severity**: P3
**Description**: The matched-row boolean mask is cloned before being passed to a filtering function.
Since the mask is not used afterward in that branch, ownership could be transferred directly.
**Suggested fix**: Remove the `.clone()` and pass the mask by value.

### R11-I-013: Unchecked u32 counter increment in MERGE row tracking

**File**: `src/merge_exec.rs:496`
**Severity**: P3
**Description**: A `u32` row counter is incremented without overflow checking. For batches exceeding
4 billion rows (theoretical), this would wrap. Practically harmless but non-idiomatic.
**Suggested fix**: Use `checked_add(1).expect("row counter overflow")` or switch to `usize`.

### R11-I-014: Complex char-by-char view SQL rewriting in schema.rs

**File**: `src/schema.rs:480-560`
**Severity**: P3
**Description**: `rewrite_duckdb_view_sql` uses a manual char-by-char state machine to translate DuckDB
SQL syntax to DataFusion-compatible SQL. This is fragile and hard to maintain. It handles identifier
quoting conversion and some function rewrites.
**Suggested fix**: Consider using a lightweight SQL parser (e.g., `sqlparser-rs`, already a transitive
dependency via DataFusion) for more robust view SQL translation, or add comprehensive unit tests for
edge cases.

### R11-I-015: `file_idx as u64` casts in table.rs virtual column handling

**File**: `src/table.rs:1608`, `src/table.rs:1633`
**Severity**: P3
**Description**: `file_idx` (a loop counter of type `usize`) is cast to `u64` via `as`. On 64-bit
platforms this is lossless, but `u64::try_from()` or direct use of `u64` counters would be more explicit.
**Suggested fix**: Use a `u64` loop counter or `u64::try_from(file_idx).unwrap()`.

### R11-I-016: block_on usage for sync-over-async in metadata providers

**File**: `src/metadata_provider_impl.rs` (throughout)
**Severity**: P3
**Description**: All sqlx-based metadata provider methods use `block_on()` to run async queries
synchronously. This is a known limitation (deferred as F-045: async trait redesign) but worth noting:
it prevents use within an async runtime context and blocks the calling thread.
**Suggested fix**: Deferred to F-045. When DataFusion's `CatalogProvider`/`SchemaProvider` traits gain
async support, migrate to native async. Current approach is correct given the sync trait constraint.

---

## Codex Cross-Validation

Codex independently reviewed `table_writer.rs`, `merge_exec.rs`, and `insert_exec.rs` and confirmed:
1. Timestamp overflow risk in partition key extraction (R11-I-002)
2. Per-row allocation in MERGE hash join (R11-I-006)
3. Full materialization OOM risk in INSERT (R11-I-003)
4. Duplicated Parquet write code between merge_exec and update_exec (R11-I-008)
5. O(n*m) column lookup in inlined row reconstruction (R11-I-005)
6. Silent null-count saturation (R11-I-004)
7. Orphan file risk in partitioned writes (R11-I-001)
8. Unnecessary clone in partitioned INSERT (R11-I-010)
9. Widening `as` casts vs `From` trait (R11-I-011)

## Other Observations (No Finding)

- **No `unwrap()` in production code**: All `unwrap()`/`expect()` calls are confined to test code. Verified via grep.
- **No `panic!`/`todo!`/`unimplemented!` in production code**: All confined to test code.
- **Arc usage is appropriate**: Shared state (providers, writers, object stores) correctly uses `Arc`. No unnecessary `Arc` wrapping of small types.
- **Error handling is consistent**: `DuckLakeError` with `thiserror` derive, proper `From` implementations preserving Arrow/IO error types (error.rs:79-94).
- **DataFusion trait implementations are correct**: `ExecutionPlan` impls properly handle `properties()`, `children()`, `with_new_children()`, and `execute()`.
- **UploadCleanupGuard pattern is sound**: RAII cleanup guard in table_writer.rs correctly handles the happy path (defuse on success) and error path (delete on drop).
- **Dialect abstraction is well-designed**: `SqlDialect` trait in dialect.rs cleanly separates SQL syntax differences with proper safety documentation for SQL injection concerns.
- **Snapshot isolation is correctly implemented**: AtomicI64 with proper Acquire/Release ordering in catalog.rs.
