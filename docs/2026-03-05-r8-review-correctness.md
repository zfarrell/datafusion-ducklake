# R8 Correctness Review

**Reviewer**: r8-correctness-review agent
**Date**: 2026-03-05
**Branch**: `ducklake-features/integration`
**Scope**: Full codebase — all 35 source files in `src/`

---

## Summary

Reviewed all source files with emphasis on write path, metadata writers, and read path correctness. Found **73 raw findings** across 6 parallel review passes. After deduplication and withdrawal of false positives, **55 distinct findings** remain.

| Severity | Count |
|----------|-------|
| P0       | 0     |
| P1       | 10    |
| P2       | 25    |
| P3       | 20    |

Prior R7 fixes (OnceLock, fetch_max, PartitionTransform, decimal parsing, checked arithmetic) remain intact. New findings focus on cross-backend parity gaps, compaction delete-file leaks, footer size calculation, and concurrency issues in PG/MySQL.

---

## P1 — Must Fix (10)

### R8-C-001: `replace_table_files` does not end active delete files (all backends)

**Files**: `metadata_writer_sqlite.rs:1394`, `metadata_writer_postgres.rs:1169`, `metadata_writer_mysql.rs:1298`

`end_table_files` correctly ends both data files AND delete files (SQLite line 1358–1366). `replace_table_files` (compaction path) only ends data files. After compaction, orphaned delete files referencing ended data file IDs remain active. The MOR read path (`DeleteFilterExec`) may apply stale deletes to wrong data, causing phantom row deletions.

**Fix**: Add `UPDATE ducklake_delete_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL` to `replace_table_files` in all three backends.

---

### R8-C-002: `recompute_table_column_stats` column join missing filters (all backends)

**Files**: `metadata_writer_sqlite.rs:937`, `metadata_writer_postgres.rs:766`, `metadata_writer_mysql.rs:884`

```sql
INNER JOIN ducklake_column c ON fcs.column_id = c.column_id
```

Missing `AND c.end_snapshot IS NULL` and `AND c.table_id = fcs.table_id`. After `ALTER TABLE RENAME COLUMN`, two column rows share the same `column_id` — the ended one and the active one. The join matches both, using the wrong `column_type` for `is_numeric_type()`, silently corrupting min/max aggregation in `ducklake_table_column_stats`.

**Fix**: Add `AND c.end_snapshot IS NULL AND c.table_id = fcs.table_id` to the join.

---

### R8-C-003: `calculate_footer_size_from_bytes` underreports by 8 bytes

**File**: `table_writer.rs:1493–1516`

Returns `metadata_len` but the Parquet footer is `[ThriftMetadata(metadata_len)][4-byte LE len][4-byte PAR1]`. Actual footer size = `metadata_len + 8`. DuckDB uses `footer_size` to seek from end-of-file; with the underreported value, it misses the Thrift blob start, failing to parse files written by this extension.

**Fix**: Return `i64::from(metadata_len) + 8`.

---

### R8-C-004: Timestamp inline serialization drops sub-second precision

**File**: `table_writer.rs:1170`

```rust
Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string())
```

Microseconds are correctly computed but the format string truncates them. Timestamps like `2024-01-01 12:00:00.123456` become `2024-01-01 12:00:00`. When read back from catalog, microseconds are permanently lost — **data corruption** for tables using inlined data with timestamp columns.

**Fix**: Use `"%Y-%m-%d %H:%M:%S%.6f"`.

---

### R8-C-005: PostgreSQL `schema_version` counter not concurrency-safe

**File**: `metadata_writer_postgres.rs:344–374` (and 10+ other DDL sites)

```rust
let prev = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
    .fetch_one(&mut *tx).await?;
let new_schema_version = prev + 1;
```

Under READ COMMITTED, two concurrent DDL ops both read the same MAX, both compute the same `new_schema_version`, producing duplicate schema versions. This breaks DuckDB's version-based catalog invalidation and snapshot isolation.

**Fix**: Use a dedicated PostgreSQL sequence (`CREATE SEQUENCE ducklake_schema_version_seq`) or upgrade DDL transactions to SERIALIZABLE.

---

### R8-C-006: PostgreSQL/MySQL TOCTOU in `get_or_create_schema` / `get_or_create_table`

**File**: `metadata_writer_postgres.rs:864–960`, `metadata_writer_mysql.rs:988–1034`

Under READ COMMITTED (PG) / REPEATABLE READ (MySQL), two concurrent callers with the same name both SELECT → find nothing → INSERT, creating duplicate active schemas/tables. No unique partial index or advisory lock prevents this.

**Fix**: Add `CREATE UNIQUE INDEX ON ducklake_schema (schema_name) WHERE end_snapshot IS NULL` (PG) or use `INSERT ... ON CONFLICT` / advisory locks.

---

### R8-C-007: MySQL `register_data_file` never updates `next_file_id` on snapshot

**File**: `metadata_writer_mysql.rs:1174–1250`

SQLite updates `ducklake_snapshot.next_file_id` in `register_data_file` (line 656–672). MySQL only updates it inside `register_dml_files`, not the standalone `register_data_file`. DuckDB reads `next_file_id` for catalog tracking; a stale 0 causes interop failures.

**Fix**: Add snapshot `next_file_id` update to MySQL's `register_data_file`, matching SQLite.

---

### R8-C-008: MySQL/PG missing `register_file_partition_value` / `get_active_partition_columns`

**Files**: `metadata_writer_mysql.rs` (not overridden), `metadata_writer_postgres.rs` (not overridden)

Trait defaults are no-ops. Partitioned tables written via MySQL/PG silently lose all partition metadata. Partition-based file pruning never works. The tables (`ducklake_file_partition_value`, `ducklake_partition_column`) are created but never populated through these write paths.

**Fix**: Implement both methods for MySQL and PostgreSQL, mirroring SQLite (lines 3252–3300).

---

### R8-C-009: MySQL/PG missing `record_snapshot_changes` for DML

**Files**: `metadata_writer_mysql.rs` (not overridden), `metadata_writer_postgres.rs` (not overridden)

Trait default is `Ok(())`. DELETE/UPDATE/MERGE operations produce no `ducklake_snapshot_changes` entries, breaking DuckDB cross-engine change detection (CDC, time travel). SQLite implements this at line 1726.

**Fix**: Override `record_snapshot_changes` in both backends.

---

### R8-C-010: DeleteFilterExec `row_offset` wrong for multi-partition scans

**File**: `delete_filter.rs:119`

```rust
row_offset: 0,  // Always starts at 0 regardless of partition
```

Delete file positions are **global** row positions within a file. If DataFusion splits a file into multiple partitions (row group splitting), partition N's stream checks row `local_i` against position `0 + local_i` instead of `partition_offset + local_i`. Deletes would be misapplied across partitions. Currently safe because ParquetExec defaults to 1 partition per file, but latent under row-group splitting configurations.

**Fix**: Either enforce single-partition execution (via `CoalescePartitionsExec` wrapper as VirtualColumnExec does), or propagate partition-aware row offsets.

---

## P2 — Should Fix (25)

### R8-C-011: `total_file_size` overflow in `replace_table_files` (all backends)

**Files**: `metadata_writer_sqlite.rs:1472`, `metadata_writer_postgres.rs:1245`, `metadata_writer_mysql.rs:1380`

Uses `.sum()` (unchecked, wraps in release) while `total_record_count` on the adjacent line uses `checked_add`. Inconsistent — corrupts `file_size_bytes` stats on overflow.

**Fix**: Use `try_fold` with `checked_add`, matching `total_record_count`.

---

### R8-C-012: `total_net_new_deletions` unchecked arithmetic in `register_dml_files` (all backends)

**Files**: `metadata_writer_sqlite.rs:1564`, `metadata_writer_postgres.rs:1336`, `metadata_writer_mysql.rs:1471`

```rust
total_net_new_deletions += file.delete_count - old_delete_count;
```

Both subtraction and addition are unchecked. Corrupted catalog values can cause underflow/overflow, panicking in debug or wrapping silently.

**Fix**: Use `checked_sub` and `checked_add`.

---

### R8-C-013: SQLite `rename_view`, `rename_table`, `set_table_comment`, `set_column_comment` lack retry

**File**: `metadata_writer_sqlite.rs:2232, 2653, 2769, 2843`

These DDL write operations use `block_on` instead of `block_on_with_retry`. Under concurrent load, SQLITE_BUSY causes immediate failure instead of retry.

**Fix**: Change to `block_on_with_retry`.

---

### R8-C-014: `replace_table_files` never calls `recompute_table_column_stats` (all backends)

**Files**: `metadata_writer_sqlite.rs:1383–1503`, `metadata_writer_postgres.rs:1169–1310`, `metadata_writer_mysql.rs:1298–1408`

After compaction, per-file stats are registered but table-level column stats (`ducklake_table_column_stats`) remain stale from pre-compaction data. Affects query planning.

**Fix**: Call `recompute_table_column_stats` at end of `replace_table_files`.

---

### R8-C-015: `record_snapshot_changes` (SQLite) no retry, no transaction

**File**: `metadata_writer_sqlite.rs:1726–1738`

Uses `block_on` (not `block_on_with_retry`) and executes directly on pool (no transaction). SQLITE_BUSY silently loses the change record. Plus `ON CONFLICT DO UPDATE` overwrites previous change description for the same snapshot.

**Fix**: Use `block_on_with_retry` and consider appending to `changes_made` rather than replacing.

---

### R8-C-016: Nanosecond-to-microsecond conversion uses truncating `/` for negative timestamps

**Files**: `table_writer.rs:1158`, `insert_exec.rs:445`

```rust
a.value(idx) / 1_000
```

Should use `div_euclid(1_000)` for pre-epoch (negative) timestamps. Truncating division produces off-by-one errors for timestamps like -1.5µs.

**Fix**: Use `.div_euclid(1_000)`.

---

### R8-C-017: `percent_decode_path` corrupts non-ASCII UTF-8 paths

**File**: `path_resolver.rs:29–46`

`bytes[i] as char` treats raw bytes as Unicode scalar values. For multi-byte UTF-8 (e.g., `/home/用户/data`), the decoded string is garbage, causing incorrect traversal detection or false positive rejections.

**Fix**: Use `String::from_utf8_lossy` or `percent_encoding::percent_decode_str` for the decoded form.

---

### R8-C-018: Path traversal guard skips decoded check when `decoded == path`

**File**: `path_resolver.rs:68–74`

```rust
if decoded != path && has_dotdot_component(&decoded) { ... }
```

If `percent_decode_path` produces output equal to input (e.g., non-hex `%` sequences), traversal check on decoded form is skipped. The guard should always run `has_dotdot_component` on the decoded path.

**Fix**: Remove `decoded != path &&` condition.

---

### R8-C-019: `partition_values_equal` uses `f64` comparison for integer partition keys

**File**: `table.rs:1304–1313`

Integers above 2^53 lose precision when parsed as `f64`, causing `9007199254740993 == 9007199254740992` via float comparison, which produces incorrect partition pruning.

**Fix**: Try `i64` parse first, then fall back to `f64`.

---

### R8-C-020: Schema mapping cache assumes first-file schema is representative

**File**: `table.rs:401–474`

If the first file in `table_files` lacks Parquet field IDs (external file), the cache is populated with empty rename mapping. All subsequent DuckLake-written files with field IDs and renamed columns use the empty mapping, silently skipping renames.

**Fix**: Skip files without field IDs when building the cache, or iterate until a file with field IDs is found.

---

### R8-C-021: Date statistics parsing uses integer format, not ISO

**File**: `table.rs:1827–1828`

```rust
DataType::Date32 => s.parse::<i32>().ok().map(|v| ScalarValue::Date32(Some(v))),
```

DuckDB stores date stats as ISO strings (`"2024-01-15"`), not epoch-day integers. `parse::<i32>()` always fails on ISO strings, so stats-based file pruning **never works for date columns**.

**Fix**: Parse ISO date strings to epoch-day values.

---

### R8-C-022: `catalog.rs:schema()` swallows metadata provider errors

**File**: `catalog.rs:382–427`

```rust
match self.provider.get_schema_by_name(name, snapshot_id) {
    Ok(Some(meta)) => { ... },
    _ => None,  // Err(...) also returns None
}
```

Transient DB connection errors appear as "schema not found" instead of internal errors, making diagnostics very difficult.

**Fix**: Log errors and/or return `Err` for provider failures.

---

### R8-C-023: `count_inlined_rows` missing `schema_version` filter (all backends)

**Files**: `metadata_provider_sqlite.rs:1013`, `metadata_provider_postgres.rs:1044`, `metadata_provider_mysql.rs:1020`

Queries `ducklake_inlined_data_tables WHERE table_id = ?` without `schema_version` filter. May count rows from a wrong-version inlined table after schema evolution, returning incorrect row counts.

**Fix**: Add the same `schema_version` filter used by `get_inlined_data`.

---

### R8-C-024: MySQL `LAST_INSERT_ID()` type mismatch — 3 sites lack `CAST AS SIGNED`

**File**: `metadata_writer_mysql.rs:1212, 1329, 1517`

MySQL returns `BIGINT UNSIGNED` for `LAST_INSERT_ID()`. `try_get::<i64>()` on `u64` causes `ColumnDecodeError`. The helper functions at lines 378/386 correctly use `CAST(LAST_INSERT_ID() AS SIGNED)`, but these 3 sites don't.

**Fix**: Use `CAST(LAST_INSERT_ID() AS SIGNED)` consistently.

---

### R8-C-025: `validate_schema_evolution` type comparison is case-sensitive

**File**: `metadata_writer_validation.rs:106`

`*existing_type != new_col.ducklake_type` rejects `"VARCHAR"` vs `"varchar"` as a type change. Different backends may normalize differently, causing spurious append failures.

**Fix**: Use case-insensitive comparison.

---

### R8-C-026: Cascade `drop_schema` snapshot chain inconsistency

**File**: `catalog.rs:241–255`

Each `drop_table` in cascade creates a new snapshot, but intermediate snapshot IDs are not propagated to the catalog's `AtomicI64`. Only the final `drop_schema` snapshot is stored via `fetch_max`. Intermediate table-drop snapshots are skipped in the catalog's view, affecting time-travel consistency.

**Fix**: Call `fetch_max` after each `drop_table` in the cascade loop.

---

### R8-C-027: `rewrite_duckdb_view_sql` is O(n²) — allocates `String` every iteration

**File**: `schema.rs:194–216`

```rust
let remaining: String = lower_chars[i..].iter().collect(); // O(n) per iteration
```

For large SQL view definitions, O(n²) character copies can cause performance issues.

**Fix**: Use a single-pass approach with `str::find` or `str::match_indices`.

---

### R8-C-028: PG/MySQL missing `find_table_id` — inlined data flush can't locate table

**Files**: `metadata_writer_postgres.rs` (not overridden), `metadata_writer_mysql.rs` (not overridden)

Trait default returns `Ok(None)`. The flush path (`table_writer.rs:400`) silently fails to find the table. Currently consistent since inlining is disabled on PG/MySQL, but will break if ever enabled.

**Fix**: Implement `find_table_id` in both backends, or return an explicit error.

---

### R8-C-029: `begin_checked_write_transaction` conflict check not re-verified after the check (PG/MySQL)

**Files**: `metadata_writer_postgres.rs:1772–1850`, `metadata_writer_mysql.rs:1841–1946`

Under READ COMMITTED (PG) / REPEATABLE READ (MySQL), between the conflict check and the actual write, a concurrent DROP can commit without being visible, leaving a write targeting a dropped table.

**Fix**: Re-verify table liveness inside `write_transaction_inner`, or use `SELECT ... FOR UPDATE`.

---

### R8-C-030: `write_parquet_with_setup` orphans file on `register_data_file` failure

**File**: `table_writer.rs:526–534`

If `object_store.put()` succeeds but `register_data_file()` fails, the uploaded Parquet file is never deleted. Unlike `finish()` which has cleanup, this path silently leaks files.

**Fix**: Add cleanup (`object_store.delete`) in the error path.

---

### R8-C-031: DuckDB `count_inlined_rows` uses `information_schema.tables` without schema filter

**File**: `metadata_provider_duckdb.rs:100–106`

`WHERE table_name = ?` without `table_schema` filter. Could match tables in other DuckDB schemas with the same name.

**Fix**: Add `AND table_schema = 'main'`.

---

### R8-C-032: PostgreSQL hardcodes `table_schema = 'public'` for inlined data

**File**: `metadata_provider_postgres.rs:926–944, 1057–1061`

Fails for non-default PostgreSQL schemas.

**Fix**: Use `current_schema()` or a configurable schema parameter.

---

### R8-C-033: PG/MySQL `snapshot_time` read as `NaiveDateTime` — timezone lost

**Files**: `metadata_provider_postgres.rs:87–89`, `metadata_provider_mysql.rs:89–91`

`TIMESTAMPTZ` read as `NaiveDateTime` discards timezone. SQLite correctly reads as `String`. Inconsistent cross-engine behavior.

**Fix**: Read as `Option<String>` like SQLite, or use timezone-aware type.

---

### R8-C-034: `parse_decimal_string` unchecked arithmetic overflow

**File**: `parse_values.rs:349–372`

`integer * 10i128.pow(scale_u)`, `frac_val * 10i128.pow(...)`, and their addition are all unchecked. Large integers with large scales overflow silently in release mode, producing corrupt decimal values.

**Fix**: Use `checked_mul` and `checked_add` with error propagation.

---

### R8-C-035: MERGE source-match counting broken by early `break`

**File**: `merge_exec.rs:496–512`

The `break` after the first candidate prevents counting matches for subsequent source rows with the same join key. The R3F-033 cardinality violation check (`source_match_count > 1`) only fires for the first candidate. Duplicate source keys bypass the check.

**Fix**: Count all candidates before breaking, or iterate all candidates for the counting check.

---

## P3 — Minor / Latent (20)

### R8-C-036: `extract_type_params` panics on non-ASCII type strings

**File**: `types.rs:386–391` — Byte-index slice `type_str[prefix_len..]` panics if prefix crosses a multi-byte UTF-8 boundary. Extremely unlikely with DuckLake type names.

### R8-C-037: `"-"` parsed as valid decimal 0 in parse_decimal_string

**File**: `parse_values.rs:325–330` — Input `"-"` silently succeeds as `0` in both Lenient and Strict modes.

### R8-C-038: `null_count` accumulation can overflow i64

**File**: `table.rs:1355–1410` — `null_counts[col_idx] += nc` unchecked i64 addition across many files.

### R8-C-039: Row count clamped to `usize::MAX` instead of `Precision::Absent`

**File**: `table.rs:1414–1419` — `unwrap_or(usize::MAX)` as `Precision::Exact` gives wildly wrong count on 32-bit or corrupt data.

### R8-C-040: `validate_name` overly broad `".."` check

**File**: `schema.rs:38` — Rejects valid names like `"foo..bar"` (no path separators present).

### R8-C-041: `table_names()` not deduplicated

**File**: `schema.rs:226–251` — Tables and views with same name appear twice.

### R8-C-042: `block_on_with_retry` deterministic jitter

**File**: `metadata_writer_sqlite.rs:73–84` — Same thread+attempt always produces same delay, potential thundering-herd under high contention.

### R8-C-043: `store_inlined_data` uses `CREATE TABLE` not `CREATE TABLE IF NOT EXISTS`

**File**: `metadata_writer_sqlite.rs:3083` — Concurrent/retried creation fails.

### R8-C-044: `get_or_create_schema` `changes_made` overwrites earlier entry for same snapshot

**File**: `metadata_writer_sqlite.rs:1104–1113` — `ON CONFLICT DO UPDATE SET changes_made = excluded.changes_made` overwrites, not appends.

### R8-C-045: `clear_inlined_data` no retry, no transaction (SQLite)

**File**: `metadata_writer_sqlite.rs:3223–3249` — SQLITE_BUSY fails silently; partial clear possible.

### R8-C-046: `sqlite_master` deprecated alias

**File**: `metadata_writer_sqlite.rs:2974, 3159` — Should use `sqlite_schema`.

### R8-C-047: Standalone `register_delete_file` doesn't update `record_count`

**File**: `metadata_writer_sqlite.rs:1801–1841` — Inconsistent with `register_dml_files`.

### R8-C-048: MySQL `write_transaction_inner` missing `created_schema` change record

**File**: `metadata_writer_mysql.rs:615–629` — Only records `created_table`, not `created_schema`.

### R8-C-049: MySQL standalone `create_snapshot` omits `schema_version` and `next_file_id`

**File**: `metadata_writer_mysql.rs:978–986` — Uses defaults instead of computed values.

### R8-C-050: `Option<T>` comparison in `should_replace_min/max`

**File**: `table_writer.rs:1432–1468` — `None < Some(x)` is `true` in Rust, so parse failures could incorrectly replace stats.

### R8-C-051: Partitioned append non-atomic across partition files

**File**: `table_writer.rs:664–695` — Mid-loop metadata failure orphans uploaded files with no cleanup.

### R8-C-052: `total_rows` unchecked in `commit_uploaded_files`

**File**: `table_writer.rs:653` — `total_rows += upload.row_count` unchecked i64 addition.

### R8-C-053: `compaction_functions.rs` ATTACH escaping limited to single-quotes

**File**: `compaction_functions.rs:94–99` — `catalog_path.replace('\'', "''")` doesn't sanitize other special chars (`;`, `\n`, null bytes). Low risk with filesystem paths.

### R8-C-054: `table_functions.rs` allows `start_snapshot == end_snapshot`

**File**: `table_functions.rs:455–461` — Produces empty result with no warning due to exclusive lower bound.

### R8-C-055: Delete file re-read on every `scan()` call

**File**: `table.rs:889–900` — Redundant work; also a minor TOCTOU risk between plan and execute phases.

---

## Verification Checklist

| Area | Status | Notes |
|------|--------|-------|
| R7 OnceLock fix (S-001) | Intact | Uses `Mutex<bool>` pattern |
| R7 fetch_max (S-002) | Intact | All snapshot stores use `fetch_max` |
| R7 PartitionTransform (S-003) | Intact | Returns error for unknown transforms |
| R7 decimal parse (S-006) | Intact | Lenient mode null-on-error |
| R7 checked arithmetic | Mostly intact | New overflow sites found in `total_file_size` and `total_net_new_deletions` |
| replace_table_files | **Bug** | Delete files not ended (R8-C-001) |
| recompute_table_column_stats | **Bug** | Column join missing filters (R8-C-002) |
| Footer size | **Bug** | Off by 8 bytes (R8-C-003) |
| Timestamp inline | **Bug** | Sub-second truncation (R8-C-004) |
| PG/MySQL parity | **Gaps** | 6 trait methods not overridden (R8-C-007–009, R8-C-028) |
| Concurrency (PG) | **Races** | schema_version dup, TOCTOU in get_or_create (R8-C-005–006) |
| SQL injection | OK | All parameterized; compaction ATTACH has limited escaping (P3) |
| Error propagation | **Swallowed** | catalog.rs:schema() (R8-C-022), record_snapshot_changes (R8-C-015) |

---

## Files Reviewed

All 35 source files in `src/` were reviewed across 6 parallel review agents:

| Agent | Files | Focus |
|-------|-------|-------|
| SQLite writer | metadata_writer_sqlite.rs, metadata_writer.rs | BUSY retry, transactions, data integrity |
| Postgres writer | metadata_writer_postgres.rs, metadata_writer.rs | Concurrency, isolation, trait parity |
| MySQL writer | metadata_writer_mysql.rs, metadata_writer.rs | Sequences, LAST_INSERT_ID, trait parity |
| Write path | delete/update/merge/insert_exec.rs, table_writer.rs | Atomicity, overflow, correctness |
| Read path | delete_filter.rs, table.rs, catalog.rs, schema.rs, path_resolver.rs, parse_values.rs, types.rs, virtual_column_exec.rs, column_rename.rs | MOR, pruning, path safety |
| Metadata providers | All 4 providers, query_planner.rs, table_functions.rs, table_changes.rs, table_deletions.rs, table_insertions.rs, information_schema.rs, validation.rs, compaction.rs, cdc_common.rs, error.rs | SQL queries, snapshot isolation, CDC |
