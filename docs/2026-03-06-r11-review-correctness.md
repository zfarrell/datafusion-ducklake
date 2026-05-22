# R11 Correctness Review

## Summary
- Total findings: 11
- By priority: P0: 0, P1: 3, P2: 5, P3: 3

## Findings

### R11-C-001: `append_table_files` assigns row_id_start from 0, not from `next_row_id`
**Priority**: P1
**Files**: `src/metadata_writer_impl.rs:812`
**Description**: The `append_table_files` method initializes `cumulative_row_id` to `0` and uses that as `row_id_start` for newly appended files, instead of reading the current `next_row_id` from `ducklake_table_stats`. This produces overlapping `row_id_start` values with existing data files when appending to a table that already contains data. Additionally, `append_table_files` never updates `ducklake_table_stats` (record_count, next_row_id, file_size_bytes), leaving table-level statistics stale after commit.

In contrast, `register_data_file` (line 462) and `register_dml_files` (line 1033) both correctly read `next_row_id` with `FOR UPDATE` and update `ducklake_table_stats` after insertion.

**Suggested fix**: Read `next_row_id` from `ducklake_table_stats` (with `FOR UPDATE`) at the start of the transaction, use it as the base for `cumulative_row_id`, and update/insert `ducklake_table_stats` with the new record_count, next_row_id, and file_size_bytes totals before committing.

---

### R11-C-002: MERGE does not detect multiple source rows matching one target row
**Priority**: P1
**Files**: `src/merge_exec.rs:493-513`
**Description**: The SQL standard (and the R3F-033 check at line 499) requires a MERGE to error when cardinality is violated. The current code checks for one source matching multiple targets, but does NOT check for multiple source rows matching the same target row. When the source hash index contains multiple entries for the same key (i.e., duplicate source keys), the code iterates all candidates but `break`s after the first match (line 512). This means:

1. The target row is only deleted/updated once (correct).
2. Only the first matching source row is used for UPDATE (nondeterministic if insertion order varies).
3. No error is raised — violating the SQL standard cardinality requirement.

The `source_match_count` tracking (line 496) detects source-matches-multiple-targets, but the inverse (target-matched-by-multiple-sources) is silently handled by taking the first match.

**Suggested fix**: After the candidate loop, if `candidates.len() > 1` for any target match, raise an execution error: "MERGE violation: multiple source rows matched the same target row."

---

### R11-C-003: `register_dml_files` does not validate data_file is still active
**Priority**: P1
**Files**: `src/metadata_writer_impl.rs:973-991`
**Description**: The `register_dml_files` method registers delete files and decrements `record_count` for the table without verifying that the referenced `data_file_id`s are still active (i.e., `ducklake_data_file.end_snapshot IS NULL`). Under concurrent operations (e.g., a concurrent REPLACE or DROP), a delete file could be registered against a data file that was already ended by another transaction, corrupting `record_count` (decrementing it for rows that no longer exist in active files).

This is noted as a deferred item (R4-S-018 / R6-S-017 in CLAUDE.md) for PG/MySQL, but the issue also affects SQLite when multiple processes share the same catalog.

**Suggested fix**: Within the transaction, verify each `data_file_id` is active: `SELECT 1 FROM ducklake_data_file WHERE data_file_id = ? AND table_id = ? AND end_snapshot IS NULL`. If not active, return a `TransactionConflict` error.

---

### R11-C-004: `extract_column_stats` uses `saturating_add` for null_count accumulation
**Priority**: P2
**Files**: `src/table_writer.rs:1329`
**Description**: The `extract_column_stats` function accumulates null counts across row groups using `saturating_add`. If the null count overflows `i64::MAX` (extremely unlikely but theoretically possible with very large files), the count silently saturates rather than returning an error. This could produce incorrect column statistics stored in the catalog, potentially leading to incorrect query results when statistics are used for pruning.

The code also has a `debug_assert!(false, ...)` at line 1326 for the initial u64-to-i64 conversion, which only fires in debug builds.

**Suggested fix**: Replace `saturating_add` with `checked_add` and propagate an error if overflow occurs, consistent with the pattern used elsewhere in the codebase.

---

### R11-C-005: `append_table_files` does not recompute table column stats
**Priority**: P2
**Files**: `src/metadata_writer_impl.rs:765-883`
**Description**: `append_table_files` registers per-file column stats but never calls `recompute_table_column_stats()` to update table-level aggregated statistics. In contrast, `replace_table_files` (line 752), `register_column_stats` (line 404), and `register_dml_files` all call `recompute_table_column_stats` after inserting file stats. This means table-level column stats (min/max/null) become stale after partitioned appends, potentially degrading query planning quality.

**Suggested fix**: Add a `Self::recompute_table_column_stats(&mut tx, table_id).await?;` call before the commit in `append_table_files`.

---

### R11-C-006: MERGE `source_match_masks` accumulate across files instead of per-file
**Priority**: P2
**Files**: `src/merge_exec.rs:456-546`
**Description**: The `source_match_masks` (line 456) is initialized once per target file (correctly), but the matched source rows are extracted from the masks and added to `matched_source_rows` (lines 538-546) inside the per-file loop. If the same source row matches target rows in multiple different data files (which shouldn't happen with correct data, but could occur with overlapping row keys across files), the source row would be added to `matched_source_rows` multiple times, causing duplicate replacement rows.

This is mitigated by the `source_match_count` check (line 499) which errors on second match, but only for the same source row matching different targets. If two files contain rows with the same key (e.g., after a failed compaction), the source row could still match in both files.

**Suggested fix**: Track a global per-source-row "already matched" flag that persists across target files, and skip already-matched source rows in subsequent file scans.

---

### R11-C-007: `parse_decimal_string` negation can overflow for `i128::MIN`
**Priority**: P2
**Files**: `src/parse_values.rs:394`
**Description**: The `parse_decimal_string` function computes the unsigned magnitude then negates at the end: `if negative { -unscaled } else { unscaled }`. If the parsed value happens to equal `i128::MIN` in unsigned magnitude (which requires the intermediate `unscaled` to equal `i128::MIN`), the negation would overflow since `i128::MIN` has no positive counterpart. However, since `unscaled` is computed as `scaled_integer + frac` (both non-negative), it will always be non-negative, so `-unscaled` can never underflow past `i128::MIN`. The actual risk is if `unscaled == i128::MAX` and the input is negative — `-i128::MAX` is valid. So this is safe in practice, but the function doesn't document this invariant.

Actually, the true issue is simpler: if the input string is "-0" followed by a fraction that parses to a large positive value, the unsigned magnitude could be very large. Since `unscaled` is always >= 0 (sum of two non-negative values), `-unscaled` ranges from `i128::MIN + 1` to `0`. The value `-i128::MIN` is impossible. So this is safe.

**Re-assessment**: P3 (documentation only, no actual bug).

---

### R11-C-008: Compaction functions use manual SQL escaping instead of parameterized queries
**Priority**: P2
**Files**: `src/compaction_functions.rs:92-96, 664-674, 714-716, 755-757`
**Description**: All compaction functions (merge_adjacent_files, rewrite_data_files, expire_snapshots, add_data_files, set_option, set_commit_message) construct DuckDB SQL by string interpolation with single-quote escaping (`value.replace('\'', "''")`). While this is the standard SQL escaping and is safe for DuckDB's default settings, it creates a maintenance burden and deviation from the parameterized query pattern used throughout the rest of the codebase. DuckDB's `duckdb` Rust crate supports parameterized queries via `execute` with params.

The `catalog_path` at line 94 is user-provided (the connection string for the catalog database), so a malicious catalog path containing `''` followed by SQL could theoretically escape, though the double-single-quote escaping prevents this in standard SQL mode.

**Suggested fix**: Consider using parameterized DuckDB queries where possible: `conn.execute("ATTACH ducklake:? AS __compaction", [&catalog_path])`. If DuckDB's ATTACH doesn't support parameters, the current escaping is acceptable but should be documented as intentional.

---

### R11-C-009: `Date32` partition value computation could silently produce None for valid dates
**Priority**: P3
**Files**: `src/insert_exec.rs:362-365, 628-629`
**Description**: The `from_num_days_from_ce_opt(days + 719_163)` conversion can fail (returning `None`) if `days + 719_163` overflows `i32`. Since Arrow Date32 values can theoretically be any `i32`, dates very far in the future (year ~5.8M) could silently become null partition values (mapped to `__HIVE_DEFAULT_PARTITION__`) instead of producing an error. This is extremely unlikely to occur in practice but could cause data to be routed to the wrong partition silently.

**Suggested fix**: Use `i32::checked_add(days, 719_163)` and return an error if it overflows, rather than silently producing a null partition value.

---

### R11-C-010: `DeleteFilterStream` row_offset overflow not detected
**Priority**: P3
**Files**: `src/delete_filter.rs:160`
**Description**: The `row_offset` field is `i64` and is incremented by `num_rows` (also `i64`) on each batch via `self.row_offset += num_rows`. While an overflow check exists for the batch-to-i64 conversion (line 151), the addition itself is unchecked. For a file with more than `i64::MAX` total rows across all batches (impossible in practice since Parquet limits file size), this could silently wrap. The i64 range accommodates ~9.2 quintillion rows, so this is purely theoretical.

**Suggested fix**: Use `checked_add` for consistency with the overflow-checked patterns elsewhere: `self.row_offset = self.row_offset.checked_add(num_rows).ok_or_else(|| ...)?;`. This would require changing the return type of `poll_next`.

---

### R11-C-011: `write_parquet_with_setup` builds table_key from names, not stored table path
**Priority**: P3
**Files**: `src/table_writer.rs:512-515`
**Description**: The `write_parquet_with_setup` method constructs the table_key from `schema_name` and `table_name` arguments with a trailing `/`:
```rust
let table_key = join_paths(
    &join_paths(&self.base_key_path, schema_name)?,
    &format!("{}/", table_name),
);
```
The comment at update_exec.rs:434 notes: "Use the catalog's stored table_path instead of deriving from names, so writes go to the correct location even after table rename." The `write_parquet_with_setup` is called from the inlining flush path, which may not have access to the stored table_path. If a table was renamed after inlined data was stored, the flush could write the Parquet file to the old (name-derived) path instead of the catalog-stored path. This is an edge case that requires table rename + inlined data flush to co-occur.

**Suggested fix**: Pass the catalog-stored table_path into `write_parquet_with_setup` instead of deriving it from names, matching the pattern in `update_exec.rs` and `merge_exec.rs`.
