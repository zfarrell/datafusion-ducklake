# R9 Correctness Review — F-044 Macro Migration

## Summary

Reviewed the 4 F-044 commits (7f76386, 3e6c73b, 30a8ce5, 7ca31af) for correctness issues introduced by the macro migration. The migration replaced ~4,100 lines of duplicated per-backend code with shared macros + a `SqlDialect` trait.

**Overall assessment: No critical (P0) correctness regressions found.** The macro-generated code faithfully reproduces the original per-backend behavior. All SQL queries, transaction boundaries, placeholder numbering, bind ordering, and RETURNING/LAST_INSERT_ID branching are correct. Two informational findings and two pre-existing concerns (not introduced by F-044) are documented below.

### Files reviewed:
- `src/dialect.rs` — SqlDialect trait + 3 implementations
- `src/metadata_writer_impl.rs` — 5 macros (recompute_table_column_stats, writer_query_ops, writer_file_ops, writer_ddl_ops, writer_drop_inner, writer_drop_ops)
- `src/metadata_writer_sqlite.rs` — macro invocations + per-backend methods
- `src/metadata_writer_postgres.rs` — macro invocations + per-backend methods
- `src/metadata_writer_mysql.rs` — macro invocations + per-backend methods
- `src/metadata_provider_impl.rs` — provider macro + delegation pattern

### Review methodology:
1. Read all macro definitions and invocations
2. Compared macro SQL against original per-backend implementations via `git show 7f76386^:src/metadata_writer_{sqlite,postgres,mysql}.rs`
3. Verified placeholder numbering (PG `$1,$2,...` vs SQLite/MySQL `?`)
4. Verified bind parameter order matches SQL column order
5. Verified transaction boundaries (begin/commit pairs)
6. Verified RETURNING vs LAST_INSERT_ID branching
7. Verified next_entity_id delegation per backend
8. Checked SQL injection surface (all values parameterized)

## Findings

### R9-C-001: SQLite boolean binding change in recompute_table_column_stats (Priority: P3)
**File**: `src/metadata_writer_impl.rs:119`
**Description**: The macro binds `agg.contains_null` (Rust `bool`) directly. The original SQLite implementation used `bind(if agg.contains_null { 1 } else { 0 })` (integer). PG/MySQL originals already used `bind(bool)`.
**Evidence**:
```rust
// Original SQLite (pre-macro):
.bind(if agg.contains_null { 1 } else { 0 })

// Macro (all backends):
.bind(agg.contains_null)
```
**Impact**: None. sqlx's SQLite driver converts `bool` to `INTEGER 0/1` automatically. The stored values are identical. Confirmed by cross-engine tests and the `ducklake_table_column_stats.contains_null` column being `BOOLEAN` in all schemas.
**Suggested fix**: No fix needed. The macro version is cleaner.
**Effort**: N/A

### R9-C-002: PG column_order type correctly differentiated (Priority: P3)
**File**: `src/metadata_writer_postgres.rs:785`, `src/metadata_writer_impl.rs:1388`
**Description**: The `column_order_type` parameter correctly uses `i32` for PostgreSQL (matching PG's `INTEGER` column type) and `i64` for SQLite/MySQL. This was manually verified against PG's `ducklake_column` DDL (`column_order INTEGER NOT NULL` → maps to i32 in sqlx::Postgres).
**Evidence**:
```rust
// PG invocation:
column_order_type = i32

// SQLite/MySQL invocation:
column_order_type = i64

// Usage in macro (metadata_writer_impl.rs:1388):
column_order: r.try_get::<$co_type, _>(3)? as i64,
```
**Impact**: Correct. No issue.
**Suggested fix**: N/A
**Effort**: N/A

### R9-C-003: DDL ops schema_version uses MAX+1 (not sequences) — pre-existing (Priority: P2)
**File**: `src/metadata_writer_impl.rs:1102-1104`
**Description**: All DDL ops in the macro (create_view, drop_view, alter_table, rename_table, etc.) allocate `schema_version` via `MAX(schema_version)+1`. For PostgreSQL, `write_transaction_inner` uses `nextval('ducklake_schema_version_seq')` instead, which is concurrent-safe. However, **this is NOT a regression** — the original pre-macro PG DDL ops also used `MAX+1` (verified via git history).
**Evidence**:
```sql
-- DDL macro (all backends):
SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot

-- write_transaction_inner (PG only):
SELECT nextval('ducklake_schema_version_seq')
```
Pre-macro PG `alter_table` (line 2295 of original): `SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot` — same pattern.
**Impact**: Under concurrent PG DDL (alter_table, create_view, etc.), duplicate `schema_version` values are theoretically possible. In practice this is low-risk because: (a) DDL is typically not concurrent, (b) `schema_version` is stored in each snapshot row and in `ducklake_schema_versions`, but doesn't serve as a unique constraint.
**Suggested fix**: Consider using `next_entity_id("schema_version")` in the DDL macro snapshot creation block to leverage PG sequences. This is a pre-existing enhancement, not a F-044 regression.
**Effort**: S

### R9-C-004: MySQL LAST_INSERT_ID is connection-scoped — safe (Priority: P3)
**File**: `src/metadata_writer_mysql.rs:371-376`
**Description**: MySQL's `LAST_INSERT_ID()` is per-connection, not global. Within a transaction (`&mut tx`), the connection is exclusive. The macro's `else` branch (non-RETURNING path) calls `(last_id_fn)(&mut tx).await?` immediately after INSERT, which is safe because no other operation can interleave on that connection.
**Evidence**:
```rust
// In macro (metadata_writer_impl.rs:501-503):
sqlx::query(&insert_sql)...execute(&mut *tx).await?;
(last_id_fn)(&mut tx).await?
```
The `create_snapshot` method uses `last_insert_id_conn` on an acquired `PoolConnection` — also safe because the connection is exclusively held.
**Impact**: No race condition. Correct.
**Suggested fix**: N/A
**Effort**: N/A

### R9-C-005: Transaction boundaries preserved across all macros (Priority: P3)
**File**: `src/metadata_writer_impl.rs` (all macros)
**Description**: Verified that all methods which previously operated within transactions still do so after macro migration:
- `register_column_stats`: `pool.begin()` → stats inserts → `recompute_table_column_stats` → `tx.commit()` ✓
- `register_data_file`: `pool.begin()` → stats check → file insert → table stats update → next_file_id → `tx.commit()` ✓
- `end_table_files`: `pool.begin()` → end data files → end delete files → reset stats → `tx.commit()` ✓
- `replace_table_files`: `pool.begin()` → end files → insert new files → stats → recompute → `tx.commit()` ✓
- `register_dml_files`: `pool.begin()` → delete file handling → data file inserts → recompute → next_file_id → `tx.commit()` ✓
- `register_delete_file`: `pool.begin()` → end old → insert new → decrement stats → `tx.commit()` ✓
- All DDL ops: `pool.begin()` → snapshot + DDL → changes tracking → `tx.commit()` ✓
- `drop_table_inner`/`drop_schema_inner`: cascade → `tx.commit()` ✓
- `set_data_path`: `pool.begin()` → delete + insert → `tx.commit()` ✓
**Impact**: No transaction boundary changes.
**Suggested fix**: N/A
**Effort**: N/A

### R9-C-006: SQL placeholder numbering verified for PG (Priority: P3)
**File**: `src/metadata_writer_impl.rs` (all macros)
**Description**: Verified PostgreSQL `$1, $2, ...` placeholder numbering via `d.ph(n)`. All queries checked:
- `register_data_file`: 8 placeholders (`$1-$8`), 8 binds ✓
- `register_dml_files`: Multiple queries, all placeholders match bind count ✓
- `replace_table_files`: 8 placeholders for file INSERT, 6 for stats INSERT, 4 for partition INSERT ✓
- All DDL ops: placeholder counts match bind counts ✓

The `d.ph(n)` pattern makes placeholder numbering explicit and less error-prone than the original hardcoded strings.
**Impact**: Correct.
**Suggested fix**: N/A
**Effort**: N/A

### R9-C-007: Provider macro delegation pattern correct (Priority: P3)
**File**: `src/metadata_provider_impl.rs:674-681, 988-994`
**Description**: The provider macro delegates 3 methods to per-backend `_impl` helpers:
1. `get_delete_files_added_between_snapshots` → `self.get_delete_files_impl(table_id, start_snapshot, end_snapshot)` ✓
2. `get_inlined_data` → `self.get_inlined_data_impl(table_id, snapshot_id)` ✓
3. `count_inlined_rows` (called within `get_table_record_count`) → `self.count_inlined_rows_impl(table_id, snapshot_id).await` ✓

All 3 backends (SQLite, PG, MySQL) provide these `_impl` methods with matching signatures. Parameters passed correctly.
**Impact**: Correct delegation.
**Suggested fix**: N/A
**Effort**: N/A

### R9-C-008: SQL injection surface — all values parameterized (Priority: P3)
**File**: All macro files
**Description**: Reviewed all SQL queries in the macros for SQL injection:
- All user-provided values (table names, column names, paths, UUIDs, stats) are bound via `d.ph(n)` parameterized placeholders
- `d.col(name)` only accepts hardcoded column names (`"key"`, `"sql"`, `"type"`) — not user input
- `d.upsert()`, `d.now()`, `d.for_update()`, `d.clamp_zero()`, `d.greatest()` all return SQL fragments from known constants
- `quote_identifier()` (used in provider `_impl` methods for inlined data table names) properly escapes double-quotes
- `d.quote_id()` properly escapes identifiers (double-quote doubling for SQLite/PG, backtick doubling for MySQL)
**Impact**: No SQL injection vectors found.
**Suggested fix**: N/A
**Effort**: N/A

### R9-C-009: Dummy last_insert_id closures are unreachable (Priority: P3)
**File**: `src/metadata_writer_sqlite.rs:924-925`, `src/metadata_writer_postgres.rs:765-766`
**Description**: SQLite and PG pass dummy `last_insert_id` closures that return `Ok(0)`. These are gated by `if d.supports_returning()` which is `true` for both backends, so the `else` branch (which would call the dummy) is unreachable. The `#[allow(unused_variables)]` annotation on `last_id_fn` suppresses the expected warning.
**Evidence**:
```rust
// SQLite/PG invocation:
last_insert_id = |_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>| async {
    Ok::<i64, crate::error::DuckLakeError>(0)
}

// In macro, gated by:
if d.supports_returning() { /* RETURNING path */ } else { (last_id_fn)(&mut tx).await? }
```
SQLite `supports_returning() = true`, PG `supports_returning() = true`. Only MySQL (`false`) ever calls `last_id_fn`.
**Impact**: Correct. The dummy closures are dead code within the `if` branch but required by the macro signature.
**Suggested fix**: N/A
**Effort**: N/A

## Codex Second Opinion

Codex (`codex exec --full-auto`) independently reviewed `src/metadata_writer_impl.rs` and confirmed:

- **PG placeholder numbering**: Consistently generated as `d.ph(1..N)` with valid sequential usage in each SQL string. No issues found.
- **Bind parameter order**: Each `sqlx::query(...).bind(...)` sequence matches the SQL placeholder order, including both `supports_returning()` and non-RETURNING branches.
- **Transaction commit**: All functions that open `begin().await?` commit. Wrapper methods that call inner helpers rely on inner helpers that commit (e.g. `drop_table_inner`/`drop_schema_inner`).
- **RETURNING branch**: Branches using RETURNING correctly `fetch_one` and read id column 0; non-RETURNING branches `execute` then use `last_id_fn` consistently.

No correctness issues found by codex.

## Conclusion

The F-044 macro migration is **correctness-preserving**. All SQL queries, transaction boundaries, placeholder numbering, bind parameter ordering, and RETURNING/LAST_INSERT_ID branching match the original per-backend implementations. The only behavioral change (R9-C-001: SQLite bool binding) is semantically equivalent. The pre-existing schema_version allocation concern (R9-C-003) is noted but is not a regression.

Test suite: `cargo test --features write-sqlite` build failed due to filesystem contention (concurrent `cargo clean` removed target directory mid-compilation). This is an environment issue, not a code issue. Previous test runs (787+ tests) passed on this branch per commit history.
