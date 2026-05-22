# R7 Correctness Review — Post-R6 Regression Analysis

**Reviewer**: correctness-review agent
**Date**: 2026-03-04
**Branch**: `ducklake-features/integration`
**Focus**: Regressions and new bugs introduced by R6 fix agents

---

## Summary

Reviewed all files modified by R6 fix agents. Found **2 high-severity issues**, **3 medium-severity issues**, and **2 low-severity observations**. The R6 changes are generally well-implemented with good atomicity, error handling, and cleanup patterns. The most critical finding is a snapshot rollback race condition that was made more impactful by R6's snapshot propagation changes.

---

## HIGH — Requires Fix

### R7-C-001: OnceLock INSTALL Failure Permanently Breaks Compaction

**File**: `src/compaction_functions.rs:81-83`
**Introduced by**: R6-S-026

```rust
DUCKLAKE_INSTALLED.get_or_init(|| {
    let _ = conn.execute("INSTALL ducklake;", []);
});
```

**Bug**: `OnceLock::get_or_init` executes the closure exactly once and records it as initialized regardless of whether the inner operation succeeded. The `let _ =` silently discards any error from `INSTALL ducklake`. If INSTALL fails due to a transient error (network timeout, disk full, permission denied), the OnceLock records it as "done" and **never retries** for the remainder of the process.

**Impact**: All subsequent compaction function calls will fail on `LOAD ducklake` (line 84) with a confusing error about loading rather than installing. Restarting the process is the only recovery.

**Fix**: Use `OnceLock` only on success, or use a different pattern:
```rust
static DUCKLAKE_INSTALLED: std::sync::Mutex<bool> = std::sync::Mutex::new(false);
// ...
let mut installed = DUCKLAKE_INSTALLED.lock().unwrap();
if !*installed {
    conn.execute("INSTALL ducklake;", [])
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    *installed = true;
}
```

Or use `OnceLock<Result<()>>` and check the result.

---

### R7-C-002: Snapshot ID Rollback Race in Concurrent DDL

**Files**: `src/schema.rs:379,444,464` and `src/catalog.rs:258,315`
**Made more impactful by**: R6 snapshot propagation via `AtomicI64`

```rust
// schema.rs:379 (deregister_table)
catalog_sid.store(new_snapshot, Ordering::Release);

// schema.rs:444 (register_table - empty table)
catalog_sid.store(result.snapshot_id, Ordering::Release);

// catalog.rs:258 (deregister_schema)
self.snapshot_id.store(new_snapshot, Ordering::Release);
```

**Bug**: Plain `store()` can overwrite a newer snapshot_id with an older one if DDL operations complete out of order. Scenario:
1. Thread A starts DDL → database assigns snapshot_id = 100
2. Thread B starts DDL → database assigns snapshot_id = 101
3. Thread B finishes first → stores 101 to AtomicI64
4. Thread A finishes → stores 100, **overwriting 101**
5. Subsequent queries use snapshot 100, missing Thread B's changes

This was a pre-existing design limitation documented in R5-S-029, but R6's snapshot propagation changes (adding `catalog_snapshot_id` to schemas and propagating after DDL) make it more likely to trigger since more code paths now call `store()`.

**Impact**: After concurrent DDL, the catalog can silently regress to an older snapshot, causing:
- Tables created by one DDL operation to be invisible
- Dropped tables to "reappear"
- Data inconsistency between metadata and object store

**Fix**: Replace all `store()` calls with `fetch_max()`:
```rust
catalog_sid.fetch_max(new_snapshot, Ordering::Release);
```
`fetch_max` atomically computes `max(current, new_value)` and stores it, ensuring the snapshot_id can only advance forward. Available since Rust 1.45.

---

## MEDIUM — Should Fix

### R7-C-003: PartitionTransform Silent Fallback for Unknown Transforms

**File**: `src/insert_exec.rs:62-63`
**Introduced by**: R6 partition transform enum

```rust
Some(_) => Self::Identity,  // Silent fallback
```

**Bug**: Unknown partition transform strings (e.g., `"bucket"`, `"truncate"`, `"yer"` typo) silently map to `Identity` instead of returning an error. This means:
- A misspelled transform produces incorrect partition layouts
- New transform types added upstream would silently degrade to Identity
- Data would be written to wrong partition directories

**Impact**: Silent data corruption in partition layouts. Queries filtering on partition columns could return incorrect results or miss data entirely.

**Fix**: Return an error for unrecognized transforms:
```rust
Some(other) => return Err(DuckLakeError::InvalidConfig(
    format!("Unknown partition transform: '{}'", other)
)),
```

---

### R7-C-004: record_count Can Go Negative

**Files**: `src/metadata_writer_sqlite.rs:1628-1638`, `src/metadata_writer_postgres.rs:1227-1235` (equivalent), `src/metadata_writer_mysql.rs:1362-1370` (equivalent)

```rust
// SQLite register_dml_files:
if total_net_new_deletions > 0 {
    sqlx::query(
        "UPDATE ducklake_table_stats
         SET record_count = COALESCE(record_count, 0) - ?
         WHERE table_id = ?",
    )
    .bind(total_net_new_deletions)
    .bind(table_id)
    .execute(&mut *tx)
    .await?;
}
```

**Bug**: No guard against `record_count` going negative. If `total_net_new_deletions` exceeds the current `record_count` (possible if stats are out of sync, or if concurrent operations both decrement), the result is a negative record count.

**Impact**: Negative record_count could confuse query planners, statistics-based optimizations, or downstream consumers that expect non-negative row counts. Unlikely in normal operation but possible after crash recovery or concurrent DML.

**Fix**: Use `MAX(0, ...)`:
```sql
SET record_count = MAX(0, COALESCE(record_count, 0) - ?)
```

---

### R7-C-005: PostgreSQL/MySQL `register_dml_files` Missing `recompute_table_column_stats`

**Files**: `src/metadata_writer_postgres.rs:1163-1315`, `src/metadata_writer_mysql.rs:1298-1450`
**Parity issue introduced during**: R6 cross-engine parity fixes

The SQLite implementation of `register_dml_files` calls `recompute_table_column_stats` after registering files with column stats (lines 1723-1726):

```rust
// SQLite:
if has_column_stats {
    Self::recompute_table_column_stats(&mut tx, table_id).await?;
}
```

Neither the PostgreSQL nor MySQL implementations call an equivalent method after `register_dml_files`. This means table-level column statistics (`ducklake_table_column_stats`) are not updated after DML operations on PostgreSQL/MySQL, potentially causing stale min/max values used for query optimization.

**Impact**: Suboptimal query plans on PostgreSQL/MySQL backends after UPDATE/MERGE operations that change column value ranges. Correctness is preserved (statistics are advisory), but performance may degrade.

---

## LOW — Acceptable / Monitor

### R7-C-006: MERGE Source Candidates with Duplicate Keys — First-Match Wins

**File**: `src/merge_exec.rs:493-513`

When multiple source rows have the same join key, only the first source row in the hash index's candidate list is processed per target row (`break` at line 512). Unmatched source rows remain in the "unmatched" pool and would be inserted via WHEN NOT MATCHED INSERT.

This is technically correct per SQL standard semantics (each source row matches at most one target row), but the choice of WHICH source row wins depends on hash map iteration order, which is non-deterministic. Users with duplicate source keys may get unpredictable results.

**Assessment**: Not a bug — matches standard SQL MERGE behavior. The R3F-033 check correctly errors when a single source row matches multiple target rows. However, consider adding documentation about this behavior.

---

### R7-C-007: DeferredCompactionProvider Ignores Projection/Filters/Limit

**File**: `src/compaction_functions.rs:156-181`

```rust
async fn scan(
    &self,
    state: &dyn datafusion::catalog::Session,
    projection: Option<&Vec<usize>>,
    filters: &[datafusion::prelude::Expr],
    limit: Option<usize>,
) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    // ... executes query, creates MemTable ...
    mem.scan(state, projection, filters, limit).await  // Delegates to MemTable
}
```

The DuckDB query is executed in full regardless of projection/filters/limit. These parameters are then applied by MemTable, which is correct but inefficient. Since compaction functions are maintenance operations called infrequently, this is acceptable.

**Assessment**: Not a bug. The delegation to MemTable handles projection/filters/limit correctly. No regression from R6.

---

## Verification Checklist

| Area | Status | Notes |
|------|--------|-------|
| Deferred compaction (R6-S-002) | Correct | SQL built at `call()` time, executed at `scan()` time. EXPLAIN is safe. |
| Upload cleanup (R6-S-037/038) | Correct | All exec plans (DELETE/UPDATE/MERGE) properly clean up on failure at each stage. |
| Atomic single-file finish (R6-S-039) | Correct | `table_writer.rs` uses `replace_table_files` for Replace mode, `register_data_file` for Append. Error paths clean up files. |
| PG/MySQL parity | **Mostly correct** | `end_table_files`, `replace_table_files` match SQLite. Missing `recompute_table_column_stats` in `register_dml_files` (R7-C-005). |
| Partition transform enum | **Silent fallback** | Unknown transforms default to Identity without error (R7-C-003). |
| Downcast error handling | Correct | All downcast sites in merge_exec.rs and insert_exec.rs use `ok_or_else` with descriptive errors. |
| Snapshot propagation | **Race condition** | `store()` should be `fetch_max()` (R7-C-002). |
| Transaction safety | Correct | All metadata writer methods use single transactions for atomicity. PG/MySQL use `FOR UPDATE` where needed. |
| SQL injection | Correct | Compaction functions escape single quotes. Metadata writers use parameterized queries. SQLite `validate_ducklake_type_for_ddl` rejects injection characters. |
| Error propagation | Correct | New R6 error paths (cleanup on upload failure, cleanup on snapshot creation failure) all propagate the original error. |
| Overflow checks | Correct | `checked_add` used for row_id arithmetic. `i64::try_from` for row indices and counts. |
| Empty input handling | Correct | All DML exec plans handle 0 affected rows (skip snapshot creation). `register_dml_files` has early exit for empty inputs. |

---

## Files Reviewed

| File | Lines | R6 Changes |
|------|-------|------------|
| `src/compaction_functions.rs` | 802 | Deferred execution, OnceLock INSTALL |
| `src/delete_exec.rs` | 431 | Upload cleanup on failure |
| `src/update_exec.rs` | 592 | Upload cleanup on failure |
| `src/merge_exec.rs` | 910 | Upload cleanup, downcast error handling |
| `src/table_writer.rs` | ~1850 | Atomic single-file finish (replace_table_files) |
| `src/insert_exec.rs` | 1081 | Partition transform enum |
| `src/schema.rs` | 566 | AtomicI64 snapshot propagation |
| `src/catalog.rs` | ~500 | AtomicI64 snapshot management |
| `src/metadata_writer_sqlite.rs` | ~1750 | register_dml_files, end_table_files, replace_table_files |
| `src/metadata_writer_postgres.rs` | ~1350 | register_dml_files, end_table_files, replace_table_files, FOR UPDATE |
| `src/metadata_writer_mysql.rs` | ~1500 | register_dml_files, end_table_files, replace_table_files, sequence IDs |
| `src/metadata_writer.rs` | ~700 | Trait default implementations |
