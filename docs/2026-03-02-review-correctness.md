# Correctness Review — 2026-03-02

**Scope**: All Rust source files under `src/` on branch `ducklake-features/integration`.
**Focus areas**: Logic bugs, NULL handling, boundary values, race conditions, resource leaks, error propagation, data integrity, SQL injection, snapshot isolation, transaction safety.

---

## P0 — Critical (data loss / security)

### P0-1: SQL injection in inlined data queries (all providers)

**Files**: `metadata_provider_sqlite.rs:870-898`, `metadata_provider_postgres.rs:962-967`, `metadata_provider_mysql.rs:941-949`

When reading inlined data from catalog-managed tables, all three metadata providers construct SQL queries using `format!()` with table names and column names interpolated directly into the query string:

```rust
// SQLite — metadata_provider_sqlite.rs:870
format!("PRAGMA table_info('{}')", inlined_table_name)

// SQLite — metadata_provider_sqlite.rs:893-898
let col_exprs: Vec<String> = columns.iter().map(|c| format!("CAST(\"{}\" AS TEXT)", c)).collect();
let sql = format!("SELECT {} FROM \"{}\" ...", col_exprs.join(", "), inlined_table_name);
```

The `inlined_table_name` value comes from a database lookup (`ducklake_inlined_data_tables`), so an attacker who can write to the catalog database can inject arbitrary SQL. While `quote_identifier()` exists in `metadata_writer_validation.rs`, it is not used in any of these read-path queries.

**Impact**: A compromised or maliciously crafted catalog database could execute arbitrary SQL when DataFusion reads inlined data.

**Fix**: Use `quote_identifier()` for all interpolated identifiers, or restructure to avoid dynamic SQL for column/table references.

---

### P0-2: Partial metadata commit in DELETE / UPDATE / MERGE execution

**Files**: `delete_exec.rs:275-330`, `update_exec.rs:380-450`, `merge_exec.rs` (same pattern)

Delete, update, and merge execution plans create a single snapshot upfront, then process files in a loop. Within the loop, each iteration registers delete files (and data files for update/merge) individually via `register_delete_file()` / `register_data_file()`. If any registration fails mid-loop, previously committed metadata remains in the catalog while later files are not registered.

```rust
// delete_exec.rs — simplified
let snapshot_id = writer.create_snapshot()?;
for file in files {
    // ... process file, upload delete file ...
    writer.register_delete_file(table_id, snapshot_id, &info)?;  // committed individually
    // If next iteration fails, this registration persists
}
```

The cleanup code removes uploaded object store files on failure, but does NOT roll back already-committed metadata rows (registered delete files / data files).

**Impact**: A failure mid-loop leaves the catalog in an inconsistent state where some files have delete markers but others don't, causing data integrity issues. Queries reading the catalog at the partially-committed snapshot will see incorrect results.

**Fix**: Either (a) batch all metadata registrations into a single transaction that commits atomically at the end, or (b) use the snapshot creation as the commit boundary by only making the snapshot "visible" after all registrations succeed.

---

## P1 — High (race condition / correctness)

### P1-1: TOCTOU race in `get_or_create_schema()` (all providers)

**Files**: `metadata_writer_sqlite.rs:632-669`, `metadata_writer_postgres.rs:577-613`, `metadata_writer_mysql.rs:665-704`

All three implementations of `get_or_create_schema()` perform a SELECT to check for an existing schema, then an INSERT to create a new one. In SQLite and MySQL, these operations use a bare connection (not a transaction), creating a classic TOCTOU race:

```rust
// SQLite — metadata_writer_sqlite.rs:632+
let existing = sqlx::query("SELECT schema_id ... WHERE schema_name = ? AND end_snapshot IS NULL")
    .fetch_optional(&self.pool)  // pool, not transaction
    .await?;
if let Some(row) = existing { return Ok((row.try_get(0)?, false)); }
// Another connection can INSERT between the SELECT and INSERT
sqlx::query("INSERT INTO ducklake_schema ...")
    .execute(&self.pool)
    .await?;
```

PostgreSQL has the same pattern at line 584-610, also using `&self.pool` directly.

**Impact**: Under concurrent writes, two connections can both see "no existing schema" and both INSERT, creating duplicate schemas. This violates the DuckLake invariant that schema names are unique within a snapshot.

**Fix**: Wrap SELECT + INSERT in a transaction, or use INSERT ... ON CONFLICT / INSERT IGNORE.

---

### P1-2: `register_column_stats()` not transactional (all providers)

**Files**: `metadata_writer_sqlite.rs:782-809`, `metadata_writer_postgres.rs:719-746`, `metadata_writer_mysql.rs:816-843`

All three implementations insert column statistics one row at a time using `&self.pool` (not a transaction). If the process fails mid-way through the stats loop, some column stats are persisted and others are not.

```rust
// Postgres — metadata_writer_postgres.rs:729-743
for stat in stats {
    sqlx::query("INSERT INTO ducklake_file_column_stats ...")
        .execute(&self.pool)  // pool, not transaction
        .await?;
}
```

**Impact**: Partial column stats can cause incorrect file pruning decisions (e.g., if min but not max is recorded, or stats for some columns are present but others are missing for the same file).

**Fix**: Wrap the stats insertion loop in a transaction.

---

### P1-3: `end_table_files()` not transactional (SQLite)

**File**: `metadata_writer_sqlite.rs:835-848`

The SQLite implementation of `end_table_files()` runs a single UPDATE using `&self.pool` directly, not wrapped in a transaction. While this is a single statement, it runs outside the caller's logical transaction boundary.

In the Postgres and MySQL implementations, `end_table_files()` also uses the pool directly (lines 772-785 and 870-883 respectively).

**Impact**: If `end_table_files()` succeeds but the subsequent `register_data_file()` fails, the old files are marked as ended but no new file replaces them, leaving the table with no active data files.

**Fix**: The entire Replace-mode sequence (end old files + register new file) should be in one transaction.

---

### P1-4: `write_or_inline()` silently swallows inline data retrieval error

**File**: `table_writer.rs:307-313`

When the inlining threshold is exceeded and existing inline data needs to be flushed, the code uses `if let Ok(...)` to silently ignore failures when reading back the existing inline data:

```rust
if current_inline > 0 {
    if let Ok(inline_rows) = self.get_inlined_data_as_batch(
        setup.table_id, setup.snapshot_id, arrow_schema,
    ) {
        all_batches.push(inline_rows);
    }
    // Clear the inlined data
    self.metadata.clear_inlined_data(setup.table_id, setup.snapshot_id)?;
}
```

If `get_inlined_data_as_batch` fails, the existing inline data is silently dropped (because `clear_inlined_data` runs unconditionally after), and only the new data is written to Parquet.

**Impact**: Silent data loss of previously inlined rows during threshold-exceeded flush.

**Fix**: Propagate the error from `get_inlined_data_as_batch()` instead of swallowing it.

---

### P1-5: `num_rows() as i64` overflow in write path

**Files**: `table_writer.rs:262`, `table_writer.rs:459`

```rust
let total_new_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
```

`num_rows()` returns `usize`. On 64-bit platforms this is safe for practical row counts, but `as i64` silently wraps values exceeding `i64::MAX`. In `write_batch()` (line 676), `i64::try_from()` is correctly used — the `as` cast pattern is inconsistent.

Note: `table_writer.rs:676` correctly uses `i64::try_from(batch.num_rows())` with proper error handling. The inconsistency means some paths are protected and others are not.

**Impact**: Theoretical integer overflow for extremely large batches (>2^63 rows). Low probability but the inconsistency signals incomplete hardening.

**Fix**: Use `i64::try_from()` consistently throughout.

---

## P2 — Medium (edge cases / silent failures)

### P2-1: `footer_size as usize` unchecked cast

**File**: `table.rs:758`

In `build_exec_for_single_file()`, footer_size is cast without `try_from`:
```rust
pf = pf.with_metadata_size_hint(footer_size as usize);
```

Elsewhere in the same file (lines 515-518), the safe pattern is used:
```rust
if let Ok(hint) = usize::try_from(footer_size) { ... }
```

**Impact**: A negative footer_size from corrupt metadata would silently wrap to a large value, likely causing an out-of-memory error or incorrect Parquet reading.

**Fix**: Use `usize::try_from(footer_size)` consistently.

---

### P2-2: `extract_column_stats` null_count cast

**File**: `table_writer.rs:1168`

```rust
null_counts[col_idx] += nc as i64;
```

`null_count_opt()` returns `Option<u64>`. If the null count exceeds `i64::MAX`, `as i64` wraps to a negative value. The accumulation across row groups could also overflow i64.

**Impact**: Incorrect null count statistics stored in metadata for files with extremely high null counts.

**Fix**: Use `i64::try_from(nc).unwrap_or(i64::MAX)` or saturating addition.

---

### P2-3: `parse_string_to_array` silently converts parse failures to NULL

**File**: `table_writer.rs:1040-1044`

When flushing inlined data back to Parquet, unparseable string values are silently converted to NULL:

```rust
Some(s) => match s.parse() {
    Ok(v) => builder.append_value(v),
    Err(_) => builder.append_null(),
},
```

**Impact**: Data that was correctly stored as inlined strings but fails to round-trip through string parsing is silently converted to NULL, causing data loss. For example, a Float32 value stored as "NaN" or "Infinity" may or may not parse correctly depending on the Rust stdlib.

**Fix**: Return an error on parse failure, or at minimum log a warning.

---

### P2-4: Silent error swallowing in `DuckLakeTable::new()`

**File**: `table.rs:193-201`

```rust
let cached_row_count = provider.get_table_row_count(table_id, snapshot_id)
    .ok().flatten();
let partition_columns = provider.get_partition_columns(table_id, snapshot_id)
    .unwrap_or_default();
```

Errors from `get_table_row_count()` and `get_partition_columns()` are silently ignored. While this prevents table creation from failing on optional metadata, it makes debugging difficult when these features silently stop working.

**Impact**: Partition pruning and COUNT(*) optimization silently degrade without any indication to the user or operator.

**Fix**: Log warnings (via `tracing::warn!`) when these optional metadata lookups fail, similar to the pattern used in `schema.rs` for `table_names()` and `table_exist()`.

---

### P2-5: Float NaN handling in stats comparison

**Files**: `table_writer.rs:1254-1283`

The `should_replace_min()` / `should_replace_max()` functions use `<` and `>` comparisons on parsed floats. In Rust, `NaN < x` is always `false` and `NaN > x` is always `false`, so:
- A NaN min will never be replaced
- A NaN max will never be replaced
- A non-NaN value will never replace NaN

This can lead to NaN being incorrectly stored as the min or max, preventing correct file pruning.

**Impact**: NaN values in float columns can cause stats-based file pruning to either prune too aggressively (missing rows) or not prune at all.

**Fix**: Handle NaN explicitly: ignore NaN values when computing min/max, or use `f32::total_cmp()` / `f64::total_cmp()` which provides a total ordering.

---

### P2-6: `values_equal()` in merge_exec falls through to `false` for unsupported types

**File**: `merge_exec.rs:183-200+`

The `values_equal()` function handles a hardcoded set of Arrow data types (Int8, Int16, Int32, Int64, Utf8, etc.) and returns `false` for any unrecognized type. This means MERGE operations on tables with unsupported column types in the join key will silently fail to match any rows.

**Impact**: MERGE INTO with join keys of unsupported types (e.g., Decimal, Timestamp) will silently treat all rows as unmatched, inserting duplicates instead of updating.

**Fix**: Either return an error for unsupported key types, or use Arrow's generic comparison kernels.

---

### P2-7: `schema.rs` `register_table` always uses `LocalFileSystem`

**File**: `schema.rs:395-396`

```rust
let object_store: Arc<dyn object_store::ObjectStore> =
    Arc::new(object_store::local::LocalFileSystem::new());
```

When CREATE TABLE AS SELECT (CTAS) writes data, it always uses `LocalFileSystem` regardless of the configured data_path. If the catalog is configured with an S3 data_path, CTAS will fail or write to the wrong location.

**Impact**: CTAS is broken for non-local-filesystem deployments (S3, MinIO, etc.).

**Fix**: Obtain the object store from the session's RuntimeEnv using the catalog's ObjectStoreUrl.

---

## P3 — Low (style / minor)

### P3-1: `schema_names()` and `table_names()` silently return partial results on error

**Files**: `catalog.rs:295-306`, `schema.rs:183-196`

Both `schema_names()` and `table_names()` use `.unwrap_or_default()` on metadata query results. While they do log errors via `tracing::error!`, the DataFusion API contract for these methods doesn't support returning errors (they return `Vec<String>`). This is acceptable given the API constraint but callers should be aware that an empty list might indicate a metadata error rather than an empty catalog.

---

### P3-2: `DuckLakeSchema::rewrite_duckdb_view_sql` byte-level string manipulation

**File**: `schema.rs:149-172`

The `rewrite_duckdb_view_sql()` function processes SQL at the byte level, using `bytes[i] as char` which is only correct for ASCII. The function only rewrites `count_star()` which is ASCII, so this is functionally correct for the current use case, but the pattern is fragile.

---

### P3-3: `AtomicI64` ordering in `DuckLakeCatalog`

**File**: `catalog.rs`

The snapshot_id uses `Ordering::Acquire` for loads and `Ordering::Release` for stores. This is correct for ensuring visibility of the snapshot_id change, but there's no corresponding synchronization of the metadata that the snapshot_id refers to. The metadata provider is queried through separate function calls, so the Acquire/Release semantics don't actually guarantee that a thread reading a new snapshot_id will see the corresponding metadata. In practice this is fine because the metadata provider uses its own synchronization (Mutex in DuckDB, connection pool in SQLx).

---

### P3-4: `table_writer.rs:443` uses table_id for path instead of table_name

**File**: `table_writer.rs:443`

```rust
&format!("t{}/", setup.table_id),
```

When flushing inlined data to Parquet via `write_parquet_with_setup()`, the file is written to a path like `<data_path>/<schema>/t42/` instead of `<data_path>/<schema>/<table_name>/`. This creates an inconsistent path structure compared to normal writes which use the table name.

**Impact**: Minor path inconsistency. The Parquet file is correctly registered in metadata with its actual path, so queries work correctly. But the directory structure is non-standard.

---

## Summary

| Severity | Count | Categories |
|----------|-------|-----------|
| P0       | 2     | SQL injection, partial metadata commit |
| P1       | 5     | Race conditions, silent data loss, overflow |
| P2       | 7     | Edge cases, silent failures, type gaps |
| P3       | 4     | Style, minor inconsistencies |
| **Total**| **18**|           |

### Priority recommendations

1. **Immediate**: Fix P0-1 (SQL injection) by applying `quote_identifier()` to all dynamic identifiers in inlined data queries.
2. **Immediate**: Fix P0-2 (partial commit) by wrapping multi-file metadata registrations in a single transaction.
3. **Soon**: Fix P1-1 (TOCTOU) by using transactions or upsert patterns in `get_or_create_schema()`.
4. **Soon**: Fix P1-4 (silent data loss) by propagating errors from inline data retrieval.
5. **Soon**: Fix P2-7 (CTAS hardcodes LocalFileSystem) to support S3/MinIO deployments.
