# R9 Codex Review — F-044 Changes

## Summary
Total codex findings: 13
P0 claims: 3 (confirmed: 0, false positive: 3)
P1 claims: 4 (confirmed: 1, false positive: 3)
P2/P3 findings: 6

## Validated Findings

### R9-CX-001: `replace_table_files` does not update `next_file_id` (Priority: P2)
**Source**: Codex review 3
**Codex claimed priority**: High (P0/P1)
**Validated priority**: P2
**Validation evidence**: Confirmed that `replace_table_files` (line 578-751) does not call the `next_file_id_sql` UPDATE, while `register_data_file` (line 528-532) and `register_dml_files` (line 984-988) both do. However, `next_file_id` is a metadata hint for DuckDB cross-engine interop and is recalculated by subsequent snapshot creation calls (e.g., `write_transaction_inner` in all backends). Data integrity is not affected — only metadata staleness for that specific compaction snapshot.
**File**: src/metadata_writer_impl.rs:578-751
**Description**: When `replace_table_files` creates new data files during compaction, it does not update `ducklake_snapshot.next_file_id` like the other file registration methods do. This leaves the snapshot's `next_file_id` stale.
**Suggested fix**: Add `next_file_id_sql` execution before `tx.commit()` in `replace_table_files`, matching the pattern in `register_data_file` and `register_dml_files`.
**Effort**: S

### R9-CX-002: `recompute_table_column_stats` drops `contains_nan` and `extra_stats` (Priority: P3)
**Source**: Codex review 3
**Codex claimed priority**: Medium (P1)
**Validated priority**: P3
**Validation evidence**: The function at line 28-127 reads `null_count`, `min_value`, `max_value` from per-file stats and writes `contains_null`, `min_value`, `max_value` to table-level stats. The `contains_nan` and `extra_stats` columns exist in the schema but are never populated by DataFusion. DuckDB may populate these fields, but DataFusion's recomputation overwrites them. This is a known limitation matching the project's current scope — NaN tracking is not implemented.
**File**: src/metadata_writer_impl.rs:28-127
**Description**: Table-level column stats recomputation ignores `contains_nan` and `extra_stats`, writing NULL to those columns. If DuckDB had previously written NaN stats, they'd be lost on recomputation.
**Suggested fix**: Low priority. Could preserve existing `contains_nan` by OR-aggregating, but this is outside current scope.
**Effort**: M

### R9-CX-003: `MySqlDialect::next_id_sql` returns `SELECT 0` (Priority: P3)
**Source**: Codex review 1
**Codex claimed priority**: High (P0)
**Validated priority**: P3 (latent, unreachable)
**Validation evidence**: MySQL's `next_entity_id` at `src/metadata_writer_mysql.rs:657-663` calls `next_sequence_id(tx, entity)` directly, completely bypassing `next_id_sql()`. The trait method exists only for completeness and is never called in the MySQL path. The `#[allow(dead_code)]` on the trait confirms this is expected.
**File**: src/dialect.rs:381-386
**Description**: `MySqlDialect::next_id_sql` returns a dummy `SELECT 0` SQL, but this method is never called for MySQL — the MySQL backend uses its own `next_sequence_id()` function instead.
**Suggested fix**: Add a doc comment explaining this is intentional, or `panic!("MySQL uses next_sequence_id instead")`.
**Effort**: S

### R9-CX-004: `quote_id()` is unused across the codebase (Priority: P3)
**Source**: Codex review 1
**Codex claimed priority**: Medium
**Validated priority**: P3
**Validation evidence**: Grep confirms no call sites for `quote_id()` outside the trait definition and implementations. The codebase uses `quote_identifier()` from `metadata_provider.rs:803` and `quote_mysql_identifier()` for MySQL instead. The `#[allow(dead_code)]` on `SqlDialect` acknowledges this.
**File**: src/dialect.rs:9
**Description**: `SqlDialect::quote_id()` is defined but never called. Identifier quoting is done through standalone `quote_identifier` and `quote_mysql_identifier` functions instead, fragmenting the quoting logic.
**Suggested fix**: Consider migrating callers to use `quote_id()` via the dialect, or document why standalone functions are preferred.
**Effort**: M

### R9-CX-005: `upsert()` produces invalid SQL on empty `set_cols` (Priority: P3)
**Source**: Codex review 1
**Codex claimed priority**: Medium
**Validated priority**: P3 (theoretical)
**Validation evidence**: All call sites pass `&["changes_made"]` — never empty. The method is `pub(crate)` and used only in macro-generated code with hardcoded arguments. No code path can reach this with empty `set_cols`.
**File**: src/dialect.rs:119-128
**Description**: If `upsert()` were called with an empty `set_cols` slice, it would produce `ON CONFLICT(...) DO UPDATE SET ` which is invalid SQL. This is unreachable in current code.
**Suggested fix**: Add `debug_assert!(!set_cols.is_empty())` as a safety net.
**Effort**: S

### R9-CX-006: `get_file_column_stats` join lacks `c.table_id` filter (Priority: FALSE POSITIVE)
**Source**: Codex review 2
**Codex claimed priority**: High (P0)
**Validated priority**: FALSE POSITIVE
**Validation evidence**: The SQL at line 690-701 matches the DuckDB reference implementation exactly (`SQL_GET_FILE_COLUMN_STATS` at `src/metadata_provider.rs:89-96`). The `s.table_id = ?` WHERE clause scopes the stats to a single table. The `column_id` values stored in `ducklake_file_column_stats` are always associated with the correct table since they're inserted with `table_id` scoping. Cross-table collision is not possible because `s.table_id = ?` filters first, then `s.column_id = c.column_id` resolves correctly within that table's column space. Multiple historical rows for the same `column_id` could theoretically cause duplicates, but this matches DuckDB's own behavior.
**File**: src/metadata_provider_impl.rs:690-701

### R9-CX-007: `get_partition_columns` join lacks `c.table_id` filter (Priority: FALSE POSITIVE)
**Source**: Codex review 2
**Codex claimed priority**: High (P0)
**Validated priority**: FALSE POSITIVE
**Validation evidence**: Same pattern as R9-CX-006. The SQL matches `SQL_GET_PARTITION_COLUMNS` at `src/metadata_provider.rs:116-125`, which is the DuckDB reference. The `pi.table_id = ?` WHERE clause scopes correctly.
**File**: src/metadata_provider_impl.rs:785-798

### R9-CX-008: `register_delete_file` does not update table stats (Priority: FALSE POSITIVE)
**Source**: Codex review 3
**Codex claimed priority**: Medium (P1)
**Validated priority**: FALSE POSITIVE
**Validation evidence**: `register_delete_file` is a low-level method. The trait's default `register_dml_files` at `src/metadata_writer.rs:474` calls it individually, but this default is overridden by the macro-generated `register_dml_files` which handles stats updates in a single transaction. The standalone `register_delete_file` is not called directly by any DML execution path — `delete_exec.rs:346` calls `register_dml_files`. The method exists for the trait contract but is always called through the stats-aware wrapper.
**File**: src/metadata_writer_impl.rs:995-1067

### R9-CX-009: SQLite `next_entity_id` uses MAX+1 pattern (Priority: FALSE POSITIVE)
**Source**: Codex review 3
**Codex claimed priority**: Medium (P1)
**Validated priority**: FALSE POSITIVE
**Validation evidence**: As documented in `src/metadata_writer_sqlite.rs:377-379`: "SQLite uses a single-writer model (WAL mode allows concurrent readers but only one writer at a time). All MAX+1 ID queries run inside transactions, which is safe because SQLite serializes writes." The `block_on_with_retry` wrapper with exponential backoff handles SQLITE_BUSY, ensuring no concurrent writers. This is correct by design.
**File**: src/metadata_writer_sqlite.rs:697-710

### R9-CX-010: `col()` not safe for dynamic input (Priority: P3)
**Source**: Codex review 1
**Codex claimed priority**: Medium
**Validated priority**: P3 (design note)
**Validation evidence**: All call sites pass string literals: `"key"`, `"sql"`, `"type"`. The method is `pub(crate)` and used only in macro-generated code. No user-controlled input reaches `col()`.
**File**: src/dialect.rs:13
**Description**: `col()` returns the input unchanged for SQLite/Postgres and only quotes specific MySQL reserved words. Safe as used, but the API doesn't prevent misuse with dynamic input.
**Suggested fix**: Document that this method is for known catalog column names only.
**Effort**: S

### R9-CX-011: `register_dml_files` skips stats recompute when no column stats provided (Priority: FALSE POSITIVE)
**Source**: Codex review 3
**Codex claimed priority**: High (P1)
**Validated priority**: FALSE POSITIVE
**Validation evidence**: When data files are added without column stats (`column_stats.is_empty()`), the `has_column_stats` flag stays false and table-level stats are not recomputed. This is correct — if no per-file stats were computed, there's nothing to aggregate. The existing table-level stats remain valid because they still bound the data from files that DID have stats. Files without stats simply aren't included in the statistical bounds, which is the expected behavior (conservative stats).
**File**: src/metadata_writer_impl.rs:963-981

### R9-CX-012: Placeholder numbering drift risk with manual `ph(n)` (Priority: P3)
**Source**: Codex review 1
**Codex claimed priority**: Medium
**Validated priority**: P3 (design note)
**Validation evidence**: All current `ph()` calls have correct numbering — I verified several complex queries with 6-8 placeholders. The risk is maintenance-related: future edits could desync placeholder indices. SQLite/MySQL use `?` and ignore the index, so only Postgres is affected. No current bugs found.
**File**: src/dialect.rs:6
**Description**: Manual placeholder numbering in format strings is error-prone for maintenance but currently correct everywhere.
**Suggested fix**: Consider a builder pattern or placeholder counter, but low priority given current correctness.
**Effort**: L

### R9-CX-013: `upsert()`/`insert_or_ignore()` accept raw SQL fragments (Priority: P3)
**Source**: Codex review 1
**Codex claimed priority**: Medium
**Validated priority**: P3 (design note)
**Validation evidence**: All arguments are hardcoded string literals in macro-generated code. No dynamic input reaches these methods. Safe as used.
**File**: src/dialect.rs:35, 44
**Description**: These methods interpolate arguments directly into SQL without escaping. Safe because all inputs are compile-time constants, but the API is "trusted input only."
**Suggested fix**: Document the trusted-input contract on the trait methods.
**Effort**: S
