# R5 — Correctness Review

**Reviewer**: correctness-review agent
**Date**: 2026-03-03
**Scope**: All source files in `src/` — logic errors, boundary conditions, overflow, NULL handling, race conditions, error propagation, data integrity, SQL injection
**Branch**: `ducklake-features/integration`

---

## Findings

### R5-S-001 — Column stats MIN/MAX uses lexicographic comparison on VARCHAR [HIGH]

**File**: `src/metadata_writer_sqlite.rs` (table-level stats aggregation SQL)
**Issue**: The `register_column_stats` method aggregates per-file column statistics into table-level stats using SQL `MIN(fcs.min_value)` and `MAX(fcs.max_value)`. These values are stored as `VARCHAR` in the `ducklake_file_column_stats` and `ducklake_table_column_stats` tables. SQL `MIN`/`MAX` on `VARCHAR` performs *lexicographic* comparison, not numeric comparison.

For numeric columns, this produces wrong results: `MIN('9', '10')` = `'10'` (because `'1' < '9'`), and `MAX('2', '100')` = `'2'`.

**Impact**: Incorrect table-level min/max statistics for numeric columns. These stats may be used for query planning optimizations (e.g., predicate pushdown, row group pruning via statistics). Wrong bounds could cause row groups to be incorrectly pruned, silently dropping matching rows.

**Fix**: Cast to the column's native type before aggregating, or perform the comparison in application code with type-aware parsing. Alternatively, store stats as the column's native type or use a numeric sort key.

---

### R5-S-002 — `saturating_add` silently clips row count to i64::MAX [MEDIUM]

**File**: `src/table_writer.rs:275`
**Issue**: When accumulating the total row count across written batches, the code uses `i64::saturating_add()`. If the total row count exceeds `i64::MAX` (unlikely but theoretically possible with massive datasets), the count silently clips to `i64::MAX` instead of returning an error.

This clipped value is then stored in catalog metadata as the file's `record_count`, which affects:
- Row count queries (`SQL_GET_TABLE_ROW_COUNT`)
- Delete file position calculations
- Virtual column `rowid` computation

**Impact**: Silent metadata corruption for extremely large tables. In practice unlikely to hit for a single Parquet file, but the defensive approach would be to use `checked_add` and return an error.

**Fix**: Replace `saturating_add` with `checked_add` and propagate an error on overflow.

---

### R5-S-003 — `unwrap_or(i64::MAX)` silently clips rowid overflow [MEDIUM]

**File**: `src/virtual_column_exec.rs:236`
**Issue**: When computing `rowid` values for the virtual column, the code uses:
```rust
.map(|offset| row_id_start.checked_add(offset).unwrap_or(i64::MAX))
```
If `row_id_start + offset` overflows, the rowid is silently set to `i64::MAX` for all overflowing rows, producing duplicate rowids. The `rowid` virtual column is meant to provide globally unique row identifiers.

**Impact**: Duplicate rowid values when `row_id_start` is near `i64::MAX`. Users relying on `rowid` for uniqueness would get incorrect results. In practice unlikely given typical row counts.

**Fix**: Return a `DataFusionError::Execution` on overflow instead of silently clipping.

---

### R5-S-004 — `replace_table_files` does not update table_stats [MEDIUM]

**File**: `src/metadata_writer_sqlite.rs` — `replace_table_files` method
**Issue**: The `replace_table_files` method (used during compaction) replaces all data files for a table with new files, but does not update the `ducklake_table_stats` row. After compaction:
- `record_count` in `ducklake_table_stats` still reflects the pre-compaction value
- `next_row_id` is not adjusted for the new file layout
- `file_size_bytes` is stale

**Impact**: Stale table-level statistics after compaction. `COUNT(*)` optimization may return incorrect counts if the compaction changed the total row count (e.g., by merging delete files into compacted data). The `next_row_id` staleness could cause ID collisions in subsequent inserts if compaction resets row IDs.

**Fix**: Recalculate and update `ducklake_table_stats` after replacing files. At minimum, update `file_size_bytes` from the new files and verify `record_count`.

---

### R5-S-005 — `values_equal` in merge_exec doesn't handle NaN for floating-point types [LOW]

**File**: `src/merge_exec.rs` — `values_equal` function
**Issue**: The `values_equal` function compares join key values using direct equality (`left_val == right_val`). For `Float32` and `Float64` types, IEEE 754 specifies that `NaN != NaN`. If a join key column contains NaN values, a source row with NaN will never match a target row with NaN, even though SQL semantics typically treat NaN = NaN as true in join predicates (following DuckDB and many SQL engines).

**Impact**: MERGE operations on floating-point join keys may fail to match rows that have NaN values. This is an edge case but could lead to incorrect results — matched rows being treated as unmatched, causing duplicate inserts instead of updates.

**Fix**: Add explicit NaN handling in the `Float32Array`/`Float64Array` match arms: if both values are NaN, return `true`.

---

### R5-S-006 — `rewrite_duckdb_view_sql` operates on byte indices of char-collected vector [LOW]

**File**: `src/schema.rs:166-192` — `rewrite_duckdb_view_sql`
**Issue**: The method collects `sql.chars()` into a `Vec<char>` and iterates by index, but uses `"count_star()".len()` (byte length = 12) to advance the index. This works correctly only because `"count_star()"` is pure ASCII and its byte length equals its char count. However, if a future rewrite pattern involves non-ASCII characters, this would break.

Additionally, the method creates a full `remaining` string on every iteration for the `starts_with` check, which is O(n²) overall.

**Impact**: No current correctness issue since `count_star()` is ASCII. Performance is suboptimal for very large view SQL strings.

**Fix**: Use byte-level operations directly (operating on `&str` slices) instead of `Vec<char>` for both correctness and performance.

---

### R5-S-007 — `delete_filter.rs` uses `u32` indices limiting batch size [LOW]

**File**: `src/delete_filter.rs` — `DeleteFilterStream::poll_next`
**Issue**: The stream converts row counts to `u32` for Arrow's `take` kernel indices:
```rust
let num_rows = batch.num_rows() as u32;
```
Arrow's `take` kernel requires `UInt32Array` indices. If a single batch contains more than `u32::MAX` (~4.3 billion) rows, this cast will silently truncate, producing wrong filter results.

**Impact**: In practice, Parquet row groups are much smaller than 4B rows (typically 1M or less), so this is theoretical. DataFusion's batch size defaults also prevent this. However, the code lacks a guard or explicit check.

**Fix**: Add an explicit bounds check and return an error if `num_rows > u32::MAX`.

---

### R5-S-008 — `table_deletions.rs` also uses u32 indices without bounds check [LOW]

**File**: `src/table_deletions.rs:670-675` — `filter_batch`
**Issue**: Same pattern as R5-S-007. The `filter_batch` method in `DeletedRowsStream` checks `u32::try_from(num_rows)` and does return an error on overflow, which is correct. However, the indices vector uses `u32` throughout, which is the correct pattern.

**Impact**: Actually properly handled here — this is correct. The `try_from` with error propagation is the right approach. (Self-correcting: not a finding.)

---

### R5-S-009 — `extract_update_info` skips extra projection columns silently [LOW]

**File**: `src/query_planner.rs:202-205` — `extract_update_info`
**Issue**: The loop iterating over `projection_exprs` breaks when `i >= schema.fields().len()`. If the SQL planner produces more projection expressions than the table has columns (e.g., due to a computed column or future DataFusion change), the extra expressions are silently ignored rather than flagged as an error.

**Impact**: If DataFusion ever changes its UPDATE plan shape to include extra expressions beyond the table columns, assignments could be silently missed.

**Fix**: Consider returning an error if `projection_exprs.len() != schema.fields().len()` to catch unexpected plan shapes early.

---

### R5-S-010 — `parse_table_name` in table_functions.rs allows empty schema or table [LOW]

**File**: `src/table_functions.rs:344-352` — `parse_table_name`
**Issue**: The `parse_table_name` function splits on the first `.` character but doesn't validate that neither the schema nor table name is empty. Input like `.foo` produces `("", "foo")` and `foo.` produces `("foo", "")`. An empty schema or table name will cause the subsequent metadata lookup to fail (schema/table not found), which is safe but produces a confusing error message.

**Impact**: Poor user experience with confusing error messages. No data integrity risk since the lookup will fail.

**Fix**: Add validation that both parts are non-empty after splitting, with a clear error message.

---

### R5-S-011 — Snapshot ID comparison uses `>=` for begin_snapshot but `<` for end_snapshot [INFO]

**File**: `src/metadata_provider.rs` — all SQL constants
**Issue**: The snapshot visibility predicate consistently uses:
```sql
WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)
```
This means an entity is visible when `snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot`. This is a half-open interval `[begin_snapshot, end_snapshot)`. This is consistent across all queries and matches DuckLake's semantics. Noting for completeness — this is correct as designed.

**Impact**: None. This is the correct DuckLake snapshot isolation semantics.

---

### R5-S-012 — `catalog.rs` `schema_names()` swallows errors with `unwrap_or_default` [LOW]

**File**: `src/catalog.rs:344-354`
**Issue**: If `list_schemas()` fails, the error is logged with `inspect_err` but then `unwrap_or_default()` returns an empty list. This means schema listing failures are silently hidden from users — they just see `information_schema` as the only schema. DataFusion's `CatalogProvider::schema_names()` returns `Vec<String>` (not `Result`), so there's no way to propagate the error.

**Impact**: Transient metadata connectivity issues are silently hidden. Users may think their catalog is empty when the metadata database is temporarily unavailable.

**Fix**: This is a limitation of DataFusion's `CatalogProvider` trait. Consider logging at `error!` level (already done) and potentially adding a health check method. No code change needed — this is correctly handling the trait constraint.

---

### R5-S-013 — `deregister_schema` drops tables but doesn't update snapshot atomically [LOW]

**File**: `src/catalog.rs:242-258`
**Issue**: When cascade-dropping a schema, the code drops each table individually in a loop, then drops the schema. Each `drop_table` call may create its own snapshot. Only the final `drop_schema` call's snapshot is stored via `self.snapshot_id.store()`. The intermediate snapshots from table drops are not visible through the catalog's snapshot_id.

However, each `drop_table` and `drop_schema` is individually transactional in the metadata writer, so this is safe from a data integrity perspective — it just means multiple snapshots are created for a cascade drop.

**Impact**: Minor — multiple snapshots created for a single logical operation. Not a correctness bug.

---

### R5-S-014 — `build_inlined_data_exec` column lookup is O(n²) per row [LOW]

**File**: `src/table.rs` (persisted output lines 341-351)
**Issue**: For each field in the schema, for each inlined row, the code does a linear search through `column_names` to find the position of the column:
```rust
row.column_names.iter().position(|n| n == col_name)
```
This is O(columns × rows × columns_per_row). For tables with many columns and many inlined rows, this could be slow.

**Impact**: Performance issue only, not a correctness bug. Inlined data is typically small (controlled by `data_inlining_row_limit`), so this is unlikely to be noticeable in practice.

---

### R5-S-015 — `compaction_functions.rs` SQL injection via string interpolation [MEDIUM]

**File**: `src/compaction_functions.rs`
**Issue**: The compaction functions build SQL strings using string interpolation with single-quote escaping:
```rust
let sql = format!("SELECT * FROM ducklake_compact('{}', '{}')",
    catalog_path.replace('\'', "''"),
    table_name.replace('\'', "''"));
```
Single-quote escaping is the standard SQL literal escaping approach and is correct for string values. However, this pattern is inherently fragile — if any code path passes a value through without the `replace()` call, it becomes injectable.

**Impact**: The current code correctly applies escaping. This is a defense-in-depth note rather than a current vulnerability.

---

### R5-S-016 — `delete_exec.rs` empty table deletion returns 0 without creating snapshot [INFO]

**File**: `src/delete_exec.rs` — `execute_inner`
**Issue**: When deleting from a table with no data files (`table_files.is_empty()`), the code returns immediately with count 0 without creating a snapshot. This is correct behavior — `DELETE FROM empty_table` should be a no-op and shouldn't create unnecessary snapshots.

**Impact**: None. Correct behavior.

---

### R5-S-017 — `metadata_writer_sqlite.rs` DDL uses TEXT for `min_value`/`max_value` [INFO]

**File**: `src/metadata_writer_sqlite.rs` — DDL schema
**Issue**: Related to R5-S-001. The DDL creates `ducklake_file_column_stats` with `min_value TEXT` and `max_value TEXT`. This design decision means all statistical values are stored as strings, which is consistent with DuckDB's DuckLake implementation but leads to the lexicographic comparison issue in R5-S-001.

**Impact**: Design limitation shared with upstream DuckDB DuckLake. The column stats SQL aggregation in R5-S-001 is the real issue.

---

### R5-S-018 — `schema.rs` `plan_view` creates temporary SessionContext without write support [LOW]

**File**: `src/schema.rs:140-162` — `plan_view`
**Issue**: When planning a view, `plan_view` creates a temporary `SessionContext` with a read-only `DuckLakeCatalog` (using `with_snapshot`). If the view SQL references other views or tables that require write capabilities, this would fail. However, views are read-only constructs so this is correct — view planning should never need write access.

**Impact**: None. Correct design.

---

## Summary

| Severity | Count | IDs |
|----------|-------|-----|
| HIGH     | 1     | R5-S-001 |
| MEDIUM   | 3     | R5-S-002, R5-S-003, R5-S-004, R5-S-015 |
| LOW      | 7     | R5-S-005, R5-S-006, R5-S-007, R5-S-009, R5-S-010, R5-S-012, R5-S-014 |
| INFO     | 3     | R5-S-011, R5-S-016, R5-S-017 |

**Total actionable findings**: 11 (HIGH + MEDIUM + LOW)
**Informational**: 3

### Key Themes

1. **Statistics integrity (R5-S-001)**: The most impactful finding — lexicographic MIN/MAX on VARCHAR-stored numeric stats can produce incorrect bounds, potentially causing incorrect query results via statistics-based pruning.

2. **Silent overflow clipping (R5-S-002, R5-S-003)**: `saturating_add` and `unwrap_or(i64::MAX)` patterns silently clip overflows instead of erroring. While unlikely to hit in practice, these violate the principle of failing loudly on unexpected conditions.

3. **Compaction metadata staleness (R5-S-004)**: `replace_table_files` doesn't update `ducklake_table_stats`, leaving stale metadata after compaction.

4. **Input validation gaps (R5-S-010)**: Empty schema/table name parts from string splitting are not validated, leading to confusing error messages.
