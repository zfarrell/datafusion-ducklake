# R3 Idiomatic Rust Review

**Reviewer:** idiomatic-review agent
**Date:** 2026-03-02
**Branch:** `ducklake-features/integration`
**Scope:** All Rust source files in `src/` (33 files)
**Prior context:** 55/58 findings fixed in R1+R2 cycles (see `docs/2026-03-02-review-synthesis.md`)

---

## Summary

The codebase is generally well-structured with good separation of concerns, proper use of `?` propagation, consistent `Arc::clone()` patterns, and clean feature gating. The R1+R2 fix cycle addressed most of the major issues (type roundtrip, numeric safety, partition routing). This R3 review focuses on remaining idiomatic patterns, with particular attention to the DML exec plans (`merge_exec.rs`, `delete_exec.rs`, `update_exec.rs`) that were heavily modified in recent fix commits.

**Findings:** 10 total (2 high, 3 medium, 5 low)

---

## Findings

### R3-001 [HIGH] Unchecked `as` casts in DML exec plans

**Files:**
- `src/merge_exec.rs` lines 379, 424, 432, 484, 497, 521, 554, 564
- `src/delete_exec.rs` lines 283, 303, 311, 312, 353
- `src/update_exec.rs` lines 327, 355, 363, 364, 423, 468, 478

**Issue:** The F-028/F-042 fixes from R1+R2 added `TryFrom`/`try_from` guards in `table.rs` and `delete_filter.rs`, but the DML exec plans still use unchecked `as` casts extensively:

```rust
// merge_exec.rs:424
global_row_offset += num_rows as i64;

// delete_exec.rs:353
let file_size = buffer.len() as i64;

// update_exec.rs:363
let update_count = positions_to_delete.len() as i64;
```

All three files share the same patterns: `num_rows as i64`, `buffer.len() as i64`, `positions_to_delete.len() as i64`, and `count as u64`. While these are unlikely to overflow in practice (batches are typically < 1M rows, files < 2GB), the inconsistency with `delete_filter.rs` (which uses `i64::try_from(batch.num_rows())` at line 141) creates a correctness gap. The `buffer.len() as i64` casts are the most concerning since Parquet buffers can theoretically exceed `i64::MAX` bytes on 64-bit systems.

**Recommendation:** Apply the same `TryFrom` pattern used in `delete_filter.rs` to all DML exec plans. Priority: the `buffer.len() as i64` casts (file size) and `num_rows as i64` (row counting).

---

### R3-002 [HIGH] `.unwrap()` on downcasts in non-test code

**File:** `src/table_writer.rs` lines 923-988

**Issue:** The `arrow_array_value_to_string()` function uses `.unwrap()` on every `downcast_ref` call:

```rust
DataType::Boolean => {
    let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    // ...
}
DataType::Int8 => {
    let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
    // ...
}
// ... 12 more unwrap() calls
```

While the `match` on `DataType` makes the downcast "logically safe" (the type tag matches the concrete type), this is not guaranteed by the type system. A schema mismatch, extension type, or dictionary-encoded array would panic. The function already returns `Result<String>`, so using `.ok_or_else(|| ...)` would be zero-cost in the success path.

**Also:** `src/table_writer.rs:694` — `self.writer.as_mut().unwrap()` panics if the writer is None. This should be an internal error.

**Also:** `src/insert_exec.rs:593` — `precompute_identity_values()` has `.unwrap()` on an `as_any().downcast_ref()` call inside the identity column precompute logic.

**Recommendation:** Replace `.unwrap()` with `.ok_or_else(|| DuckLakeError::Internal(...))` or `DataFusionError::Internal(...)`.

---

### R3-003 [MEDIUM] Unchecked `as` casts in `virtual_column_exec.rs`

**File:** `src/virtual_column_exec.rs` lines 218, 226, 256

**Issue:** The virtual column stream uses `num_rows as i64` without overflow checks:

```rust
let row_numbers: Vec<i64> = (row_offset..row_offset + num_rows as i64).collect();
// ...
self.row_offset += num_rows as i64;
```

On a 64-bit system, `usize` and `i64` have the same max positive range, so `num_rows as i64` is safe in practice. However, this is a different module from `delete_filter.rs` which explicitly uses `i64::try_from()`. Consistency matters for maintainability.

**Recommendation:** Use `i64::try_from(num_rows).map_err(...)` to match the pattern in `delete_filter.rs`.

---

### R3-004 [MEDIUM] Dead variables with underscore prefix

**Files:**
- `src/merge_exec.rs` lines 307-308: `let _schema_name = self.schema_name.clone();` / `let _table_name = self.table_name.clone();`
- `src/update_exec.rs` lines 221-222: same pattern
- `src/metadata_provider_duckdb.rs` line 247: `let _delete_count: Option<i64> = row.get(12)?;`
- `src/metadata_provider_duckdb.rs` line 574: `let _schema_version: i64 = row.get(1)?;`
- `src/table_functions.rs` line 294: `_func_name: &str` parameter

**Issue:** These variables are cloned/fetched but never used. The underscore prefix suppresses the compiler warning but the clones still allocate. In `merge_exec.rs` and `update_exec.rs`, `schema_name` and `table_name` are `String` fields that get cloned into unused locals inside `execute()`.

**Recommendation:**
- For `merge_exec.rs` / `update_exec.rs`: Remove the dead clones. If the names were intended for tracing/logging, add the tracing call or remove them.
- For `metadata_provider_duckdb.rs`: Use `let _: Option<i64> = row.get(12)?;` or just skip the column.
- For `table_functions.rs`: Remove `_func_name` parameter or use it in error messages.

---

### R3-005 [MEDIUM] Unchecked `as` casts in `table_writer.rs` and `insert_exec.rs`

**Files:**
- `src/table_writer.rs:485` — `buffer.len() as i64`
- `src/table_writer.rs:608` — `key_index as i32` (partition key index)
- `src/table_writer.rs:1371` — `metadata_len as i64`
- `src/insert_exec.rs:263,803` — `result.records_written as u64`
- `src/insert_exec.rs:711` — `row_idx as u32`
- `src/insert_exec.rs:888,901` — `.num_days() as i32`

**Issue:** These casts were partially addressed by F-028/F-042 but several remain. The `key_index as i32` is particularly notable — if a table has more than `i32::MAX` partition keys, this wraps silently. The `.num_days() as i32` casts are safe for date ranges Arrow supports, but lack documentation of that invariant.

**Recommendation:** Add `i32::try_from()` / `i64::try_from()` guards, or add comments documenting why the cast is safe (e.g., "Arrow Date32 guarantees num_days fits in i32").

---

### R3-006 [LOW] Duplicated Parquet write + delete-file boilerplate across DML execs

**Files:** `src/delete_exec.rs`, `src/update_exec.rs`, `src/merge_exec.rs`

**Issue:** All three DML exec plans contain nearly identical code for:
1. Writing a Parquet delete file (buffer creation, `ArrowWriter`, field ID annotation, footer size calculation)
2. Registering the delete file with the metadata writer
3. Writing replacement data files with the same pattern

This duplication was noted in F-044 (deferred) but remains. Each copy has ~40-50 lines of identical boilerplate.

**Recommendation:** Extract shared helpers (e.g., `write_delete_file()`, `write_data_file()`) into `table_writer.rs` or a shared utilities module. This would reduce ~150 duplicated lines to ~30.

---

### R3-007 [LOW] `information_schema.rs` TableProvider boilerplate

**File:** `src/information_schema.rs`

**Issue:** Five TableProvider implementations (`SnapshotsTable`, `SchemataTable`, `TablesTable`, `ColumnsTable`, `FilesTable`, `TableInfoTable`) share identical `scan()` method bodies:

```rust
async fn scan(...) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let batch = self.query_xxx()?;
    let mem_table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
    mem_table.scan(state, projection, filters, limit).await
}
```

Each also has identical `as_any()`, `schema()`, and `table_type()` implementations.

**Recommendation:** Consider a generic wrapper or macro to reduce boilerplate. Low priority since the pattern is straightforward and unlikely to diverge.

---

### R3-008 [LOW] `Arc::clone` vs `.clone()` inconsistency

**Issue:** The codebase mostly uses the idiomatic `Arc::clone(&x)` pattern (good for clarity that it's a cheap clone), but there are instances of `.clone()` on `Arc` types scattered throughout, particularly in `information_schema.rs` (e.g., `self.provider.clone()` where `provider: Arc<dyn MetadataProvider>`).

**Recommendation:** Standardize on `Arc::clone()` for `Arc` types. This is purely a style/clarity issue — no behavioral difference.

---

### R3-009 [LOW] `schema()` returns `SchemaRef` clone on every call

**Files:** Multiple TableProvider implementations

**Issue:** Several `TableProvider::schema()` implementations return `self.schema.clone()` where `schema` is already an `Arc<Schema>` (`SchemaRef`). This is cheap (Arc clone) but the DataFusion trait requires `SchemaRef` by value, so this is correct behavior. No action needed — noting for completeness that this is the expected pattern.

**Status:** Not a finding — correct idiomatic usage of DataFusion APIs.

---

### R3-010 [LOW] `information_schema.rs` `table_exist()` allocates on every call

**File:** `src/information_schema.rs:820-822`

```rust
fn table_exist(&self, name: &str) -> bool {
    self.table_names().iter().any(|t| t == name)
}
```

**Issue:** `table_names()` allocates a `Vec<String>` on every call just to check membership. For 6 known table names, a `matches!()` on the name would be zero-allocation.

**Recommendation:** Replace with `matches!(name, "snapshots" | "schemata" | "tables" | "table_info" | "columns" | "files")`.

---

## Positive Observations

1. **Error handling** is consistently good — `?` propagation throughout, proper `map_err` conversions at module boundaries, and `DuckLakeError` variants are well-chosen.

2. **Feature gating** in `lib.rs` is clean — write modules gated behind `#[cfg(feature = "write")]`, metadata backends behind their respective features.

3. **`delete_filter.rs`** sets the gold standard for numeric safety with `i64::try_from()` and `u32::MAX` guards. This should be the template for the DML exec plans.

4. **`metadata_writer_validation.rs`** is an excellent example of extracting shared validation logic with comprehensive test coverage (30+ tests).

5. **`query_planner.rs`** has clear safety comments explaining why certain plan shapes are rejected (preventing silent data loss from empty filter extraction).

6. **`path_resolver.rs`** properly handles null bytes and path traversal attacks with percent-decode validation.

7. **`encryption.rs`** correctly hides encryption keys in `Debug` output.

---

## Previously Fixed (R1+R2) — Confirmed Still Fixed

Spot-checked the following fixes and confirmed they remain in place:
- F-028/F-042: `try_from` guards in `table.rs` and `delete_filter.rs`
- F-033: `CoalescePartitionsExec` for row-number virtual columns
- F-034: Null coercion for virtual columns
- F-037: Temporal type roundtrip in `types.rs`
- F-053: Decimal parser fix in `types.rs`
- F-023: Hex-before-base64 decoding in `encryption.rs`

---

## Priority Summary

| ID | Severity | File(s) | Category |
|----|----------|---------|----------|
| R3-001 | HIGH | merge/delete/update_exec | Numeric safety |
| R3-002 | HIGH | table_writer, insert_exec | Panic in non-test code |
| R3-003 | MEDIUM | virtual_column_exec | Numeric safety consistency |
| R3-004 | MEDIUM | merge/update_exec, metadata_provider | Dead code |
| R3-005 | MEDIUM | table_writer, insert_exec | Numeric safety |
| R3-006 | LOW | merge/delete/update_exec | Code duplication |
| R3-007 | LOW | information_schema | Boilerplate |
| R3-008 | LOW | multiple | Style consistency |
| R3-010 | LOW | information_schema | Minor allocation |
