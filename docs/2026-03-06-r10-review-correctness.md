# R10 Correctness Review — 2026-03-06

**Branch:** `ducklake-features/integration`
**Reviewer:** Claude Opus 4.6
**Scope:** Full codebase correctness review — logic bugs, edge cases, data integrity, security
**Prior reviews:** R1–R9 (~360 findings fixed)

---

## Findings Summary

| # | Severity | File | Line(s) | Description |
|---|----------|------|---------|-------------|
| R10-S-001 | Medium | `delete_exec.rs` | 308 | Unchecked u64 `+=` accumulation for `total_deleted` |
| R10-S-002 | Medium | `update_exec.rs` | 356 | Unchecked u64 `+=` accumulation for `total_updated` |
| R10-S-003 | Medium | `merge_exec.rs` | 529, 584 | Unchecked u64 `+=` accumulation for `total_affected` |
| R10-S-004 | Medium | `table_writer.rs` | 669 | Unchecked i64 `+=` accumulation for `total_rows` in `commit_uploaded_files` |
| R10-S-005 | Medium | `table_writer.rs` | 509, 795 | Unchecked i64 `+=` accumulation for `row_count` in `write_parquet_with_setup` and `write_batch` |
| R10-S-006 | Low | `metadata_writer_mysql.rs` | 367 | `next_sequence_ids` returns wrong ID when `count=0` |
| R10-S-007 | Low | `merge_exec.rs` | 618 | `total_records += i64::try_from(...)` unchecked accumulation |
| R10-S-008 | Low | `update_exec.rs` | 450 | `total_records += i64::try_from(...)` unchecked accumulation |
| R10-S-009 | Info | `metadata_writer_sqlite.rs` | 602, 900 | `(order + 1) as i64` unchecked cast from usize |

**Total:** 9 findings (0 Critical, 0 High, 5 Medium, 3 Low, 1 Info)

---

## Detailed Findings

### R10-S-001 — Unchecked u64 accumulation in DELETE row count [Medium]

**File:** `src/delete_exec.rs:308`

```rust
total_deleted += new_delete_count;
```

Individual file counts are checked via `u64::try_from(positions_to_delete.len())`, but the cross-file accumulation uses plain `+=`. In Rust release builds, u64 overflow wraps silently, reporting an incorrect (much smaller) count to the user. While reaching 2^64 rows is impractical, this is **inconsistent** with the project's careful overflow checking elsewhere (e.g., `table_writer.rs:279` uses `checked_add`).

**Fix:** Use `total_deleted = total_deleted.checked_add(new_delete_count).ok_or_else(...)`.

---

### R10-S-002 — Unchecked u64 accumulation in UPDATE row count [Medium]

**File:** `src/update_exec.rs:356`

Same pattern as R10-S-001: `total_updated += new_update_count;` without overflow check.

---

### R10-S-003 — Unchecked u64 accumulation in MERGE row count [Medium]

**File:** `src/merge_exec.rs:529, 584`

Two accumulation sites:
- Line 529: `total_affected += new_match_count;` (matched rows)
- Line 584: `total_affected += u64::try_from(filtered.num_rows())...;` (unmatched inserts)

Both use unchecked `+=`.

---

### R10-S-004 — Unchecked i64 accumulation in commit_uploaded_files [Medium]

**File:** `src/table_writer.rs:669`

```rust
total_rows += upload.row_count;
```

`total_rows` is `i64`. Accumulating across many partitioned uploads could overflow. Used directly as `records_written` in the returned `WriteResult`.

**Fix:** Use `total_rows = total_rows.checked_add(upload.row_count).ok_or_else(...)`.

---

### R10-S-005 — Unchecked i64 accumulation in write paths [Medium]

**File:** `src/table_writer.rs:509, 795`

Two sites:
- Line 509 in `write_parquet_with_setup`: `row_count += i64::try_from(batch.num_rows())...`
- Line 795 in `write_batch`: `self.row_count += i64::try_from(batch.num_rows())...`

The `try_from` conversion is checked, but the `+=` accumulation is not. Writing many batches could overflow silently in release builds, resulting in a negative `row_count` being registered in catalog metadata.

---

### R10-S-006 — MySQL next_sequence_ids returns wrong ID for count=0 [Low]

**File:** `src/metadata_writer_mysql.rs:367`

```rust
Ok(end_value - count + 1)
```

If `count=0` (e.g., table with 0 columns), the sequence is incremented by 0 (`seq_value = seq_value + 0`), so `end_value` is unchanged. The function returns `end_value + 1`, which is one past the last allocated ID — a phantom ID that was never allocated and may collide with the next real allocation.

**Mitigating factor:** All callers pass `columns.len()` which is validated to be non-empty before reaching this point. Severity is Low because the path is currently unreachable.

---

### R10-S-007 — Unchecked i64 accumulation in MERGE data file write [Low]

**File:** `src/merge_exec.rs:618`

```rust
total_records += i64::try_from(batch_with_ids.num_rows())...;
```

Same pattern: individual conversion checked, accumulation unchecked.

---

### R10-S-008 — Unchecked i64 accumulation in UPDATE data file write [Low]

**File:** `src/update_exec.rs:450`

```rust
total_records += i64::try_from(batch_with_ids.num_rows())...;
```

Same pattern.

---

### R10-S-009 — Unchecked usize-to-i64 cast in column order [Info]

**File:** `src/metadata_writer_sqlite.rs:602, 900`

```rust
.bind((order + 1) as i64)
```

If `order` equals `usize::MAX` (impossible in practice since it's bounded by `columns.len()`), this would overflow. Same pattern in MySQL and PostgreSQL backends.

**Mitigating factor:** `columns` is a `&[ColumnDef]` slice, so `order` < `columns.len()` which is bounded by available memory. Not a practical concern.

---

## Areas Reviewed (No Issues Found)

### SQL Injection
All SQL queries across all metadata writer backends use parameterized queries (`?` for SQLite/MySQL, `$N` for PostgreSQL). User-provided values (schema names, table names, column names, data values) are always bound as parameters, never interpolated into SQL strings.

The only `format!`-based SQL constructions use `quote_id()` for table/schema identifiers (e.g., inlined data table names), which properly escapes double quotes.

### Transaction Safety
- **SQLite:** `begin_write_transaction` wraps snapshot creation, schema/table/column setup in a single transaction. `block_on_with_retry` handles SQLITE_BUSY correctly.
- **PostgreSQL/MySQL:** Same pattern with proper transaction boundaries.
- **DML operations:** All use `UploadCleanupGuard` to clean up orphaned files on error. Delete files and data files are committed atomically via `register_dml_files`.
- **Replace mode:** Uses `replace_table_files` for atomic end-old + register-new.

### Delete Filter Correctness
- `DeleteFilterExec` correctly tracks `row_offset` across batches.
- `CoalescePartitionsExec` is used when input has multiple partitions (R8-S-014).
- NULL handling in filter evaluation: `mask.is_valid(i) && mask.value(i)` correctly treats NULL predicates as non-matching.

### Path Resolution Security
- Path traversal protection via `validate_no_path_traversal` with percent-decoding.
- Null byte injection protection via `validate_no_null_bytes`.
- Partition values URL-encoded in `url_encode_partition_value`.

### MERGE Correctness
- R3F-033 multi-target detection correctly prevents source row from matching multiple targets.
- Hash-based join index with proper NULL key handling (NULL keys never match, per SQL semantics).
- NaN equality handled correctly via bit-level comparison.

### Snapshot Isolation
- Tables/schemas filtered by snapshot validity ranges (`begin_snapshot`/`end_snapshot`).
- Write operations create snapshots before any metadata changes.
- Conflict detection in `begin_checked_write_transaction` runs within the same transaction as the write.

---

## Observations (Not Bugs)

1. **DML does not use column rename layer:** DELETE/UPDATE/MERGE read Parquet files directly without `ColumnRenameExec`. This works correctly because physical expressions use column indices (not names), so column renames don't affect filter evaluation. Column additions/drops change the table's file list, so old files with different schemas are naturally excluded.

2. **10M row safety limit in UPDATE:** `update_exec.rs:238` has a `MAX_BUFFERED_ROWS = 10_000_000` guard. This is appropriate for the copy-on-write pattern but means large UPDATE operations will fail rather than stream.

3. **Inlined data flush race:** `write_or_inline` reads inlined data count, then conditionally flushes. Between these operations, another writer could add more inlined data. This is mitigated by the write transaction that pins the snapshot, but the inlined row count read is outside the transaction.
