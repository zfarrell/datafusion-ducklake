# R8 Idiomatic Rust Review — Agent 2

**Date**: 2026-03-05
**Reviewer**: r8-idiomatic-review-2
**Branch**: `ducklake-features/integration`
**Focus**: Error handling, ownership/borrowing, DataFusion APIs, performance, Rust idioms
**Scope**: Core source files in `src/` (table.rs, table_writer.rs, schema.rs, insert_exec.rs, delete_exec.rs, update_exec.rs, merge_exec.rs, metadata_writer_sqlite.rs, compaction_functions.rs, catalog.rs, table_deletions.rs, parse_values.rs)

---

## Summary

| Severity | Count |
|----------|-------|
| P1 (bugs/API misuse) | 1 |
| P2 (non-idiomatic/perf) | 5 |
| P3 (style nits) | 5 |
| **Total** | **11** |

---

## Findings

### P1 — Bugs / API Misuse

#### R8-I-001: `rewrite_duckdb_view_sql` allocates O(n²) per iteration
**File**: `src/schema.rs:200`
**Issue**: On each iteration of the while-loop, a new `String` is allocated from the `lower_chars[i..]` slice just to check `starts_with("count_star()")`. This is O(n) per iteration × O(n) iterations = O(n²) allocations. For very large view SQL this could cause significant latency during table lookups.
```rust
let remaining: String = lower_chars[i..].iter().collect();
if remaining.starts_with("count_star()") {
```
**Suggested fix**: Compare directly by checking the next 12 characters inline instead of allocating a String:
```rust
fn starts_with_at(chars: &[char], pos: usize, needle: &[char]) -> bool {
    if pos + needle.len() > chars.len() { return false; }
    chars[pos..pos + needle.len()] == *needle
}
// Pre-compute: let count_star_chars: Vec<char> = "count_star()".chars().collect();
// Then: if starts_with_at(&lower_chars, i, &count_star_chars) { ... }
```
This reduces runtime from O(n²) to O(n) and eliminates all intermediate allocations. While view SQL is typically short, this is a correctness concern for the general case and a clear algorithmic improvement.

---

### P2 — Non-Idiomatic / Performance

#### R8-I-002: Massive code duplication across delete/update/merge exec plans
**Files**: `src/delete_exec.rs`, `src/update_exec.rs`, `src/merge_exec.rs`
**Issue**: All three exec plans contain ~80 lines of nearly identical code for:
- Writing delete files (build schema, create ArrowWriter, serialize, upload, record metadata)
- File cleanup on failure (`cleanup_orphaned_files` pattern)
- Snapshot creation with error-path cleanup

For example, the delete-file-writing block (schema creation → ArrowWriter → buffer → upload → DeleteFileInfo construction) is duplicated verbatim across all three files (delete_exec:331-382, update_exec:407-462, merge_exec:555-610).

**Impact**: If a bug or protocol change occurs in delete-file writing, all three files must be updated. This is a maintenance risk.
**Suggested fix**: Extract a shared helper function:
```rust
// In table_writer.rs or a new shared module:
pub async fn write_delete_file(
    object_store: &dyn ObjectStore,
    table_path: &str,
    resolved_path: &str,
    data_file_id: i64,
    positions: Vec<i64>,
) -> Result<(ObjectPath, DeleteFileInfo)> { ... }
```

#### R8-I-003: `extract_rows` in insert_exec.rs clones UInt32Array indices unnecessarily
**File**: `src/insert_exec.rs:737`
```rust
let idx_array = arrow::array::UInt32Array::from(idxs.clone());
```
**Issue**: `idxs` is a `Vec<u32>` inside a `&(_, Vec<u32>)` tuple from the iteration. The `.clone()` creates a full copy of the index vector. Since `UInt32Array::from(Vec<u32>)` takes ownership, you could restructure the code to avoid the clone by using `idxs.as_slice()` with `UInt32Array::from_iter_values(idxs.iter().copied())` or by consuming the vector.
**Impact**: Minor — only matters for large partition extractions.

#### R8-I-004: `source_match_masks` rebuilt per target file in merge_exec
**File**: `src/merge_exec.rs:456-459`
```rust
let mut source_match_masks: Vec<Vec<bool>> = source_batches
    .iter()
    .map(|b| vec![false; b.num_rows()])
    .collect();
```
**Issue**: This allocates and zeros a Vec<Vec<bool>> matching all source batch dimensions for EACH target file. Since the masks are only used to collect matched source rows for the current file, and then the entire mask is OR-accumulated, the per-file reset is correct. However, the allocation overhead could be avoided by reusing the buffers.
**Impact**: Allocates O(source_rows × target_files) booleans total.
**Suggested fix**: Pre-allocate the mask vectors once before the loop and use `.fill(false)` to reset them per file.

#### R8-I-005: `for (i, mask_val) in mask_values.iter_mut().enumerate().take(num_rows)` is redundant
**File**: `src/update_exec.rs:311`
```rust
let mut mask_values = vec![false; num_rows];
for (i, mask_val) in mask_values.iter_mut().enumerate().take(num_rows) {
```
**Issue**: `mask_values` already has exactly `num_rows` elements, so `.take(num_rows)` is a no-op. This adds visual noise without purpose.
**Suggested fix**: Remove `.take(num_rows)`.

#### R8-I-006: `parse_values.rs` truncating cast `num_days() as i32`
**File**: `src/parse_values.rs:130`
```rust
date.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32
```
**Issue**: `num_days()` returns `i64`. Using `as i32` silently truncates if the date is more than ~5.8 million days (~16,000 years) from epoch. While practically dates won't exceed this, the idiomatic pattern is to use `i32::try_from()` to fail explicitly rather than silently truncate.
**Suggested fix**:
```rust
i32::try_from(date.signed_duration_since(UNIX_EPOCH_DATE).num_days())
    .map_err(|_| DuckLakeError::Internal(format!("Date '{}' exceeds i32 range", s)))?
```

---

### P3 — Style Nits

#### R8-I-007: `table.rs:1410` — `null_counts[i].max(0) as usize` truncating cast
**File**: `src/table.rs:1410`
```rust
cs.null_count = Precision::Inexact(null_counts[i].max(0) as usize);
```
**Issue**: After `.max(0)`, the value is non-negative but still i64. On 32-bit platforms, large counts would silently truncate via `as usize`. More idiomatic to use `usize::try_from()` or at minimum document that `usize` is 64-bit on all supported platforms.
**Impact**: No practical impact on 64-bit platforms.

#### R8-I-008: `table.rs:1597,1622` — `file_idx as u64` unchecked cast
**File**: `src/table.rs:1597,1622`
```rust
file_index: file_idx as u64,
file_index: active_files.len() as u64,
```
**Issue**: `file_idx` is `usize` from `enumerate()`. On 64-bit platforms `usize as u64` is lossless, but idiomatic Rust uses explicit conversion. This is extremely minor since `usize` == `u64` on all target platforms.

#### R8-I-009: `metadata_writer_sqlite.rs:597,606` — `order as i64` unchecked casts
**File**: `src/metadata_writer_sqlite.rs:597,606`
```rust
let column_id = next_column_id + order as i64;
.bind((order + 1) as i64)
```
**Issue**: `order` is `usize`. While safe on 64-bit, the idiomatic pattern would be `i64::try_from(order)`. This pattern repeats in the postgres and mysql writers as well.
**Impact**: None practical — column counts will never approach usize::MAX.

#### R8-I-010: `compaction_functions.rs:85` — `unwrap_or_else(|e| e.into_inner())` on poisoned mutex
**File**: `src/compaction_functions.rs:85`
```rust
let mut installed = DUCKLAKE_INSTALLED.lock().unwrap_or_else(|e| e.into_inner());
```
**Issue**: While recovering from a poisoned mutex is intentional here (to allow retry after a failed INSTALL), it would benefit from a comment explaining why poison recovery is correct — i.e., the bool value is still valid even if a previous thread panicked.
**Suggested fix**: Add a brief inline comment:
```rust
// Recover from poison — the bool is still valid if a prior thread panicked during INSTALL.
let mut installed = DUCKLAKE_INSTALLED.lock().unwrap_or_else(|e| e.into_inner());
```

#### R8-I-011: `table_deletions.rs:739,804,830` — `unwrap()` in non-test production code
**File**: `src/table_deletions.rs:739,804,830`
```rust
let keep_indices: Vec<u32> = match self.deleted_positions.as_ref().unwrap() {
let current = self.current_delete_stream.as_mut().unwrap();
let prev = self.previous_delete_stream.as_mut().unwrap();
```
**Issue**: These `unwrap()` calls are in Stream::poll_next implementation code, not test code. While the struct invariant guarantees these are Some at the time of call (state machine ensures it), bare `unwrap()` will panic with an unhelpful message if the invariant is ever violated.
**Suggested fix**: Use `expect()` with a descriptive message documenting the invariant:
```rust
self.deleted_positions.as_ref().expect("deleted_positions must be set before ReadingData state")
self.current_delete_stream.as_mut().expect("current_delete_stream must be Some in ReadingCurrentDelete state")
```

---

## Notes on Prior Fixes

The R7 review cycle addressed many P3 issues (checked arithmetic, bounds checks, `eq_ignore_ascii_case`, etc.). The codebase is generally well-structured with proper error handling. The remaining findings are primarily:

1. One algorithmic performance issue (R8-I-001) that should be fixed
2. Significant code duplication across DML exec plans (R8-I-002) that is a maintenance risk
3. Minor style issues that are low priority

The DataFusion API usage is correct throughout — proper `ExecutionPlan` implementations, correct `PlanProperties`, appropriate use of `SendableRecordBatchStream` with `RecordBatchStreamAdapter`, and correct filter pushdown semantics.
