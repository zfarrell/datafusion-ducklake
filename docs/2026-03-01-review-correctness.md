# Correctness Review: Tier 1 Sprint (2026-03-01)

## Executive Summary

This review covers the write-side partitioning, data inlining, SLT view rewriting, and cross-engine test infrastructure added in the Tier 1 sprint. **Five critical issues** and several major/minor issues were identified across atomicity gaps, silent data misrouting, and data loss scenarios. The most severe involve non-atomic partitioned writes (each partition creates an independent snapshot and column set), inline data loss on flush failure, and Replace-mode atomicity gaps.

---

## Findings by Severity

### Critical

#### C1. Partitioned writes create independent snapshots per partition, reassigning column IDs each time

**Files:** `insert_exec.rs:610-636`, `table_writer.rs:101-124,149-191`

**Bug:** `write_partitioned()` iterates over each partition and calls `table_writer.begin_write_partitioned()` per partition. Each call invokes `begin_write_internal()` which calls `metadata.begin_write_transaction()`. In the SQLite implementation (`metadata_writer_sqlite.rs:341-498`), `write_transaction_inner` creates a new snapshot, ends ALL existing columns (`UPDATE ducklake_column SET end_snapshot = ?`), and creates new columns with new IDs. This means:

- Partition A: creates snapshot S1, assigns column IDs [C1, C2], writes Parquet with field_ids [C1, C2]
- Partition B: creates snapshot S2, ENDS columns [C1, C2], assigns [C3, C4], writes Parquet with field_ids [C3, C4]
- Partition C: creates snapshot S3, ENDS columns [C3, C4], assigns [C5, C6], writes Parquet with field_ids [C5, C6]

Only the last partition's column IDs are active. Parquet files from earlier partitions embed stale field IDs that don't match any active column definition. DuckDB reads by field_id, so cross-engine reads will fail or produce incorrect column mapping.

**Reproduction:** Insert data into a table with 2+ partition values.

**Suggested fix:** Perform a single `begin_write_transaction` for the entire partitioned write, then use the returned setup (snapshot_id, column_ids) for all partition files. Register each data file separately under the same snapshot.

---

#### C2. Replace-mode metadata commit occurs before Parquet upload

**Files:** `table_writer.rs:149-191`, `metadata_writer_sqlite.rs:479-490`

**Bug:** `begin_write_internal` calls `metadata.begin_write_transaction(mode=Replace)` which commits immediately (the SQLite transaction is committed at line 490). This ends all existing data files. But the actual Parquet upload doesn't happen until `session.finish()` (line 607). If the upload fails, old files are already ended and the new file is never registered—the table appears empty.

**Reproduction:** Trigger an object store upload failure (e.g., disk full, S3 permission error) during a Replace-mode write.

**Suggested fix:** Defer ending old files until after the upload succeeds—either by splitting `begin_write_transaction` to not end files until a separate `commit_write` call, or by moving the file-end into `commit_metadata()`.

---

#### C3. Inline data lost when flush-to-Parquet fails after `clear_inlined_data`

**Files:** `table_writer.rs:313-328` (threshold exceeded path), `table_writer.rs:407-413` (manual flush path)

**Bug:** Both paths call `clear_inlined_data()` BEFORE writing the Parquet file:

```
// Threshold path (line 313-315):
self.metadata.clear_inlined_data(setup.table_id, setup.snapshot_id)?;
// ... then Parquet write (line 321-328)

// Flush path (line 407-409):
self.metadata.clear_inlined_data(setup.table_id, setup.snapshot_id)?;
// ... then Parquet write (line 412)
```

If the Parquet write or object store upload fails, the inlined rows are already marked with `end_snapshot` and are invisible to future reads. The data is permanently lost.

**Reproduction:** Call `flush_inlined_data` with a read-only or full object store.

**Suggested fix:** Move `clear_inlined_data` to AFTER successful Parquet upload and metadata commit.

---

#### C4. Partitioned writes can partially commit, leaving table in inconsistent state

**Files:** `insert_exec.rs:610-652`

**Bug:** Each partition in the loop independently creates a session, writes, and commits. If partition 3 of 5 fails, partitions 1-2 are committed while 3-5 are not. With `WriteMode::Replace`, the first partition already ended all existing files, so the table contains only data from partitions 1-2—a partial overwrite.

**Reproduction:** Write 5 partitions where the 3rd partition's object store upload fails.

**Suggested fix:** All partitions should share a single snapshot and write transaction. Files should only be registered in metadata after ALL uploads succeed, or a rollback mechanism should undo partial commits.

---

#### C5. Inline data read failure silently swallowed during threshold-exceeded flush

**File:** `table_writer.rs:306-310`

**Bug:**
```rust
if let Ok(inline_rows) = self.get_inlined_data_as_batch(...) {
    all_batches.push(inline_rows);
}
```

If reading inlined data fails, the error is silently swallowed. The code proceeds to clear the inlined data (line 314) and write only the new batches to Parquet. The existing inlined rows are lost.

**Reproduction:** Corrupt the inlined data table or cause a schema mismatch during read.

**Suggested fix:** Propagate the error: `let inline_rows = self.get_inlined_data_as_batch(...)?;`

---

### Major

#### M1. Timestamp partition transforms only support microsecond resolution

**File:** `insert_exec.rs:464-478`

**Bug:** `extract_temporal_component` for `DataType::Timestamp(_, _)` only attempts `downcast_ref::<TimestampMicrosecondArray>()`. Timestamps with Second, Millisecond, or Nanosecond units silently return `None`, routing all rows to `__HIVE_DEFAULT_PARTITION__`.

**Reproduction:** Create a table with a `Timestamp(Millisecond, None)` column, set partition by `year(ts_col)`, insert data. All rows end up in the default partition regardless of timestamp value.

**Suggested fix:** Handle all four timestamp units (`TimestampSecondArray`, `TimestampMillisecondArray`, `TimestampMicrosecondArray`, `TimestampNanosecondArray`) by converting to a common representation (e.g., `DateTime` via the appropriate constructor).

---

#### M2. Unknown partition transforms silently produce NULL partition values

**File:** `insert_exec.rs:425`

**Bug:** `compute_partition_value` returns `None` for unrecognized transform strings (e.g., a typo like `"yer"` instead of `"year"`). This maps to `__HIVE_DEFAULT_PARTITION__` via `build_hive_dir`. No error or warning is produced.

**Reproduction:** Configure a partition column with `transform: Some("yer".to_string())`.

**Suggested fix:** Return an error for unknown transforms rather than silently producing a NULL value.

---

#### M3. Inline flush path writes to wrong directory (`t{table_id}/` vs `table_name/`)

**File:** `table_writer.rs:439-443, 472-473`

**Bug:** `write_parquet_with_setup` constructs the object path as `<data_path>/<schema_name>/t<table_id>/<uuid>.parquet` but registers only `<file_name>` as a relative path in the catalog. The read path resolves relative file paths against `<data_path>/<schema_name>/<table_name>/`, so the file won't be found.

**Reproduction:** Insert data that exceeds the inlining threshold, triggering a flush via `write_parquet_with_setup`. Then query the table—file not found.

**Suggested fix:** Either pass the table_name to `write_parquet_with_setup` and use it for path construction, or register the file with an absolute path or the correct relative path.

---

#### M4. Hive partition values not URL-encoded or sanitized

**File:** `insert_exec.rs:494-496`

**Bug:** `build_hive_dir` uses raw string interpolation: `format!("{}={}", name, v)`. If a partition value contains `/`, `..`, `=`, or other special characters, the resulting path is malformed or could traverse directories.

**Reproduction:** Insert a row where the partition column value is `"foo/bar"`. The Hive path becomes `col=foo/bar` which creates an unexpected directory structure.

**Suggested fix:** URL-encode partition values per the Hive partition naming convention (e.g., `%2F` for `/`).

---

#### M5. `rewrite_duckdb_view_sql` can match `count_star()` as substring

**File:** `schema.rs:149-156`

**Bug:** The `find("count_star()")` search is not word-boundary-aware. It would incorrectly rewrite SQL like `SELECT discount_star() FROM ...` to `SELECT disCOUNT(*) FROM ...`.

**Reproduction:** Create a DuckDB view whose SQL contains a function or identifier ending in `count_star()`.

**Suggested fix:** Use regex or check for word boundaries (non-alphanumeric character before `count`).

---

### Minor

#### m1. `row_idx as u32` truncation in `extract_rows`

**File:** `insert_exec.rs:564`

**Bug:** `row_idx as u32` silently truncates row indices > 2^32. While impractical for single batches, the type system doesn't prevent it.

**Suggested fix:** Use `u32::try_from(row_idx).map_err(...)` for safety.

---

#### m2. `arrow_array_value_to_string` for Date32/Date64 produces raw integers

**File:** `table_writer.rs:758-764`

**Bug:** Date32 and Date64 arrays' `.value(idx).to_string()` produces the raw integer representation (days/ms since epoch), not a human-readable date string. The round-trip works within DataFusion (since `parse_string_to_array` parses the integer back), but cross-engine reads from DuckDB (which expects formatted date strings) may fail.

**Suggested fix:** Format dates as ISO-8601 strings for cross-engine compatibility. Ensure `parse_string_to_array` handles both integer and date-string formats.

---

#### m3. `parse_string_to_array` silently converts unparseable values to NULL

**File:** `table_writer.rs:831-833`

**Bug:** The `parse_primitive!` macro calls `s.parse()` and appends NULL on parse failure. This silently loses data rather than surfacing an error.

**Suggested fix:** Return an error on parse failure, or at minimum log a warning.

---

#### m4. `format_float` in `hybrid_asyncdb.rs` may not match DuckDB precision exactly

**File:** `tests/hybrid_asyncdb.rs:479-496`

**Bug:** `v.to_string()` uses Rust's default float formatting which may differ from DuckDB's display precision (e.g., `1.0000000000000002` vs `1.0`). This could cause spurious SLT test failures.

**Suggested fix:** Use DuckDB-compatible float formatting (e.g., round to significant digits).

---

#### m5. `rewrite_duckdb_view_sql` position bug with non-ASCII SQL

**File:** `schema.rs:152-154`

**Bug:** `result.to_lowercase().find(...)` returns a byte position in the lowercased string, but `result.replace_range(pos..pos+12, ...)` operates on the original string. For non-ASCII characters that change length during case conversion, the position is incorrect. In practice, SQL is typically ASCII, so risk is low.

---

#### m6. SQL interpolation of inlined data table names

**Files:** `metadata_writer_sqlite.rs:1826-1830, 1861-1870, 1883-1884, 1896-1898, 1955, 1976-1982, 2020-2023`

**Bug:** Dynamic SQL is built via `format!("... FROM \"{}\" ...", inlined_table_name)`. The table name is always `ducklake_inlined_data_{table_id}` (safe), but the pattern is fragile—a `"` in the name would break quoting.

**Suggested fix:** Validate or sanitize the table name, or use a different quoting approach.

---

## Codex CLI Findings

Codex (gpt-5.3-codex) independently reviewed `src/table_writer.rs`, `src/insert_exec.rs`, and `src/metadata_writer_sqlite.rs` and identified the following issues (verbatim summary):

1. **Replace writes are not atomic** — metadata setup (ending old files) is committed before Parquet upload; upload failure leaves table empty. (Matches C2 above.)

2. **Partitioned inserts can partially commit** — each partition is an independent session/snapshot; failure mid-way leaves table in mixed state. (Matches C4 above.)

3. **Partition metadata registration non-atomic with file registration** — `register_file_partition_value` runs after `session.finish()`; failure leaves data file without partition values. (New finding — partition values are orphaned.)

4. **Inline flush path writes to wrong directory** — physical path uses `t{table_id}` but catalog registers only filename relative to table path. (Matches M3 above.)

5. **Inline data lost on flush failure** — `clear_inlined_data` called before Parquet write; upload failure permanently loses data. (Matches C3 above.)

6. **Temporal partition transforms fail for non-microsecond timestamps** — only `TimestampMicrosecondArray` handled. (Matches M1 above.)

7. **Unknown partition transform treated as NULL** — typo in transform silently routes to default partition. (Matches M2 above.)

8. **Hive path components not escaped** — partition values with `/`, `..`, or `=` corrupt path structure. (Matches M4 above.)

9. **MAX()+1 ID allocation without uniqueness constraints** — concurrent writers could allocate duplicate IDs. (New finding — SQLite's single-writer model prevents this in practice, but no UNIQUE constraint guards against it for other backends.)

**Codex-exclusive findings (not in primary review):**
- Finding #3 (partition value registration non-atomicity) and #9 (MAX+1 ID allocation) were not independently identified in the primary review at the same severity level. Both are valid concerns, particularly for the Postgres/MySQL backends where concurrent writers are more realistic.

---

## Testing Gaps Identified

- No fault-injection tests for object store failures during partitioned writes
- No test verifies all-or-nothing semantics for multi-partition inserts
- No test for inline flush failure recovery (data preservation)
- No test for Replace-mode failure after metadata commit but before upload
- No test for non-microsecond timestamp partition transforms
- No test for special characters in partition values
