# R10 Codex Review

**Date**: 2026-03-06
**Branch**: `ducklake-features/integration`
**Reviewer**: Claude Opus 4.6 (codex-driven with manual validation)

## Summary

- Total codex findings: 25
- P0 claims: 0
- P1 claims: 15 (confirmed: 4, downgraded: 9, false positive: 2)
- P2 claims: 10 (confirmed: 7, downgraded: 1, false positive: 2)
- **Final validated findings**: 20 (4 P1, 9 P2, 7 P3)

### Historical false positive context
Codex P0/P1 false positive rate in prior rounds: 46-86%. This round: 11/15 P1 claims were downgraded or false positive (73%).

---

## Validated Findings

### R10-CX-001: Missing `table_id` join predicate in `get_file_column_stats` (Priority: P1)
**Source**: Codex review 3
**Codex claimed**: P1 → **Validated**: P1 CONFIRMED
**Location**: `src/metadata_provider_impl.rs:693`
**Evidence**: The query joins `ducklake_file_column_stats s` to `ducklake_column c` on `s.column_id = c.column_id` without adding `AND c.table_id = s.table_id`. In SQLite, `column_id` is allocated per-table (`MAX(column_id) + 1 WHERE table_id = ?`), so the same `column_id` value can exist in different tables. The `s.table_id` filter constrains the stats rows but not the column name lookup. With two tables having columns with the same `column_id`, this query returns the wrong column name. Postgres uses a global sequence so it's safe there, but SQLite cross-table collision is real.

### R10-CX-002: Missing `table_id` join predicate in partition column lookups (Priority: P1)
**Source**: Codex review 3
**Codex claimed**: P1 → **Validated**: P1 CONFIRMED
**Location**: `src/metadata_provider_impl.rs:789`
**Evidence**: Same root cause as R10-CX-001. The partition column query joins `ducklake_column c ON pc.column_id = c.column_id` without `c.table_id` constraint. Could return wrong partition column names on SQLite when `column_id` values collide across tables. The `metadata_writer_impl.rs:333` path adds `c.end_snapshot IS NULL` which narrows it but still doesn't include `table_id`.

### R10-CX-003: DDL schema-version race on Postgres (Priority: P1)
**Source**: Codex review 3
**Codex claimed**: P1 → **Validated**: P1 CONFIRMED
**Location**: `src/metadata_writer_impl.rs:1092`
**Evidence**: `create_ddl_snapshot!` computes `MAX(schema_version) + 1` inside a transaction. Postgres uses `READ COMMITTED` isolation by default (`pool.begin().await?`), so two concurrent DDL transactions can read the same max and assign duplicate schema versions. SQLite is safe due to serialized writes with WAL mode. Impact is limited to DDL concurrency scenarios but can produce non-monotonic schema version chains.

### R10-CX-004: Append-mode partitioned file commit is non-atomic (Priority: P1)
**Source**: Codex review 1
**Codex claimed**: P1 → **Validated**: P1 CONFIRMED
**Location**: `src/table_writer.rs:680-712`
**Evidence**: In append mode, each file is registered individually via `register_data_file`, `register_column_stats`, and `register_file_partition_value` as separate operations. If `register_column_stats` fails after `register_data_file` succeeds, the file is committed without stats, and retrying would create a duplicate file entry. The code comment at line 682 acknowledges this is "acceptable for append mode" but doesn't address the stats/partition partial failure within a single file registration. Replace mode uses `replace_table_files` which is atomic.

### R10-CX-005: MERGE multi-source-match semantics (Priority: P2)
**Source**: Codex review 1
**Codex claimed**: P1 → **Validated**: P2 (downgraded)
**Location**: `src/merge_exec.rs:492-513`
**Evidence**: The code `break`s on the first candidate match at line 511. Codex claimed this allows duplicate source rows to become "unmatched" and get inserted. However, the code at line 494-503 increments `source_match_count` for every candidate in the loop BEFORE the break, and errors if any source row matches multiple targets. The `break` after the first candidate is correct because the hash index groups by key — all candidates with the same key are equivalent. The target-row-matches-multiple-source-rows case is NOT checked (only source-matches-multiple-targets is), which diverges from strict SQL MERGE semantics but is a common implementation choice. Downgraded because it only matters when multiple source rows have identical join keys matching the same target, and the existing check catches the most dangerous case.

### R10-CX-006: DML uses pre-collected table_files without commit-time validation (Priority: P2)
**Source**: Codex review 1
**Codex claimed**: P1 → **Validated**: P2 (downgraded)
**Location**: `src/delete_exec.rs:176-223`, `src/update_exec.rs:199-262`, `src/merge_exec.rs:369-435`
**Evidence**: DELETE/UPDATE/MERGE read `table_files` at plan creation time and use them at execution time without re-validating. Concurrent compaction could invalidate file IDs. However, this is mitigated by: (1) DuckLake's snapshot isolation — each session pins to a snapshot ID, (2) the metadata layer's `commit_delete_files` and similar endpoints should fail if referencing invalid file IDs, and (3) compaction is expected to be coordinated with writes. The risk is real but the architecture's snapshot model provides inherent protection.

### R10-CX-007: Orphaned files from partitioned write commit failure (Priority: P2)
**Source**: Codex review 1
**Codex claimed**: P2 → **Validated**: P2 CONFIRMED
**Location**: `src/insert_exec.rs:759-815`, `src/table_writer.rs:630-712`
**Evidence**: Partitioned writes upload N files, then call `commit_uploaded_files`. If commit fails, `cleanup_uploaded_files` is called (line 725-735), but individual upload failures mid-batch leave earlier uploads without cleanup. The `UploadCleanupGuard` is used in delete/update/merge paths but not in the `commit_uploaded_files` path of `insert_exec`. Storage leak risk, not data loss.

### R10-CX-008: `validate_batch_schema` doesn't check nullability (Priority: P2)
**Source**: Codex review 1
**Codex claimed**: P2 → **Validated**: P2 CONFIRMED
**Location**: `src/table_writer.rs:197-243`
**Evidence**: `validate_batch_schema` checks field names and types but not nullability. All DML callers (INSERT, UPDATE, MERGE) call `validate_not_null_constraints` separately, so the public API is safe. But `DuckLakeTableWriter` is `pub` and direct callers could bypass the check. Low risk since no current code path bypasses it.

### R10-CX-009: Filter pushdown + DeleteFilterExec row offset interaction (Priority: P2)
**Source**: Codex review 2
**Codex claimed**: P1 → **Validated**: P2 (downgraded — likely safe)
**Location**: `src/table.rs:1446`, `src/delete_filter.rs:187`
**Evidence**: `supports_filters_pushdown` returns `Inexact` for all filters, which codex claimed would cause Parquet row-group pruning to shift row offsets. However, `build_exec_for_file_with_deletes` does NOT pass filter predicates to the `FileScanConfigBuilder` or `ParquetSource`. Filters are only applied by DataFusion's optimizer as a `FilterExec` node ABOVE `DeleteFilterExec`. The `DeleteFilterExec` wraps the raw Parquet scan which reads all row groups. The `CoalescePartitionsExec` guard at line 42 ensures single-partition ordering. **Row offsets are correct.** The theoretical risk is if a future DataFusion optimizer learns to push predicates through `DeleteFilterExec` into the Parquet source, but that would require explicit support.

### R10-CX-010: `file_index` virtual column depends on pruning (Priority: P2)
**Source**: Codex review 2
**Codex claimed**: P2 → **Validated**: P2 CONFIRMED
**Location**: `src/table.rs:1601-1608`
**Evidence**: `file_index` uses `enumerate()` on `active_files` after stat/partition pruning. If pruning removes files, the indices shift. The `file_index` column is documented as "ordinal position" but its value depends on which files pass pruning. This is a semantic issue — the value is stable for a given query but not across queries with different filters.

### R10-CX-011: `rowid`/`file_row_number` accuracy with deletes (Priority: P2)
**Source**: Codex review 2
**Codex claimed**: P2 → **Validated**: P2 CONFIRMED
**Location**: `src/virtual_column_exec.rs:203`
**Evidence**: Virtual column `row_offset` tracks rows that flow through the stream post-delete-filtering. After `DeleteFilterExec` removes rows, `file_row_number` no longer corresponds to physical Parquet row positions. This is correct behavior for the "row number in output" semantic but may confuse users expecting physical positions.

### R10-CX-012: Negative-scale decimal parsing (Priority: P2)
**Source**: Codex review 2
**Codex claimed**: P2 → **Validated**: P2 CONFIRMED
**Location**: `src/parse_values.rs:349`
**Evidence**: `parse_decimal_string` uses `scale.max(0)` which treats negative scales as zero. DuckLake's type system maps some DuckDB types with negative scales. The parsed value would have incorrect precision. Edge case for inlined data only.

### R10-CX-013: Lenient parse fallback returns StringArray for non-string types (Priority: P3)
**Source**: Codex review 2
**Codex claimed**: P2 → **Validated**: P3 (downgraded)
**Location**: `src/parse_values.rs:258`
**Evidence**: Lenient fallback for unsupported types returns `StringArray`. This would fail `RecordBatch::try_new` if the target field isn't Utf8. However, lenient mode is only used for specific known-safe paths and unsupported types are extremely rare in practice.

### R10-CX-014: Struct field parsing with escaped quotes (Priority: P3)
**Source**: Codex review 2
**Codex claimed**: P2 → **Validated**: P3 (downgraded)
**Location**: `src/types.rs:457`
**Evidence**: `parse_struct_fields` doesn't handle escaped quotes (`""`) in field names. Valid edge case but DuckDB-generated struct type strings don't use this pattern in practice.

### R10-CX-015: DROP SCHEMA CASCADE non-atomic (Priority: P2)
**Source**: Codex review 4
**Codex claimed**: P1 → **Validated**: P2 (downgraded)
**Location**: `src/catalog.rs:241-259`
**Evidence**: Each `drop_table` creates a separate snapshot. If one table drop fails mid-cascade, some tables are dropped and some aren't. The state is consistent (each individual operation is complete) but partial. Recovery is straightforward: re-run the CASCADE. Downgraded because partial state is recoverable and consistent.

### R10-CX-016: `register_schema` ignores existing schema and constructs wrong path (Priority: P2)
**Source**: Codex review 4
**Codex claimed**: P1 → **Validated**: P2 (downgraded)
**Location**: `src/catalog.rs:313-323`
**Evidence**: `register_schema` uses `get_or_create_schema` and constructs `schema_path` from `name` (line 322: `resolve_path(&self.catalog_path, name, true)`). If the schema already exists with a custom path, this returns the wrong path. However, `get_or_create_schema` returns the existing schema_id, and the path is only used for the returned `DuckLakeSchema` object — not persisted again. The schema path in metadata remains correct. Downgraded because the impact is limited to the returned object's path, not stored metadata.

### R10-CX-017: CREATE TABLE uses Replace mode unconditionally (Priority: P3)
**Source**: Codex review 4
**Codex claimed**: P1 → **Validated**: P3 (downgraded — by design)
**Location**: `src/schema.rs:475`
**Evidence**: `begin_write_transaction` with `Replace` mode for CREATE TABLE. The `write_transaction_inner` checks for existing tables and validates schema evolution. If the table exists with the same schema, it reuses the table_id. If the schema differs, `validate_schema_evolution` enforces compatibility. DataFusion's catalog layer handles `IF NOT EXISTS` / `OR REPLACE` semantics before calling `register_table`. This is by design.

### R10-CX-018: UPDATE projection-order assumption (Priority: P3)
**Source**: Codex review 4
**Codex claimed**: P1 → **Validated**: P3 (false positive)
**Location**: `src/query_planner.rs:211-232`
**Evidence**: DataFusion's SQL planner produces UPDATE projections in table schema order — one expression per column. The code correctly compares `col.name == *field.name()` to detect unchanged columns. The optimizer does not reorder projections in UPDATE plans. The `is_unchanged` check by column name provides a safety net. No corruption risk.

### R10-CX-019: Table function 3-part identifier parsing (Priority: P2)
**Source**: Codex review 4
**Codex claimed**: P2 → **Validated**: P2 CONFIRMED
**Location**: `src/table_functions.rs:355-370`
**Evidence**: `parse_table_name` splits on the first unquoted dot. Input `catalog.schema.table` becomes `(schema="catalog", table="schema.table")`. Fully qualified 3-part names are not supported. This matches DuckLake's design (catalog is implicit) but could confuse users.

### R10-CX-020: Absolute path escape without base-path enforcement (Priority: P3)
**Source**: Codex review 5
**Codex claimed**: P1 → **Validated**: P3 (downgraded)
**Location**: `src/path_resolver.rs:220-226`
**Evidence**: For non-relative paths, `resolve_path` validates against `..` and null bytes but allows any absolute path. However, paths come from trusted catalog metadata, and object stores enforce their own access boundaries. The only attack vector is a compromised catalog database, which is already a trust boundary violation. Defense-in-depth only.

### R10-CX-021: Full-file delete delta O(record_count) memory (Priority: P2)
**Source**: Codex review 5
**Codex claimed**: P1 → **Validated**: P2 (downgraded)
**Location**: `src/table_deletions.rs:710`
**Evidence**: `compute_deleted_positions` builds `(0..record_count)` into a Vec for full-file deletes with prior partial deletes. For a file with 100M rows, this allocates ~800MB. However, this is a CDC-specific path (not normal reads) and full-file deletes with prior partial deletes are rare. The `DeltaPositions::All` fast path handles the common case at line 708.

### R10-CX-022: Compaction default `older_than = 2099-01-01` (Priority: P3)
**Source**: Codex review 5
**Codex claimed**: P1 → **Validated**: P3 (downgraded — by design)
**Location**: `src/compaction_functions.rs:33, 517`
**Evidence**: The `DEFAULT_OLDER_THAN` constant is `2099-01-01`, making all files eligible for cleanup. This is intentional — the function delegates to DuckDB's `ducklake_delete_orphaned_files` which has its own safety logic. The DataFusion wrapper is a thin pass-through.

### R10-CX-023: Encryption keys not zeroized (Priority: P3)
**Source**: Codex review 5
**Codex claimed**: P2 → **Validated**: P3 (downgraded)
**Location**: `src/encryption.rs:82, 100, 186`
**Evidence**: Keys remain in heap `String`/`HashMap`. Standard Rust practice for non-HSM key handling. Zeroization would require `zeroize` crate and careful lifetime management. Low practical risk — memory exposure requires process memory access which is already a full compromise.

### R10-CX-024: Key lookup normalization collision (Priority: P2)
**Source**: Codex review 5
**Codex claimed**: P2 → **Validated**: P2 CONFIRMED
**Location**: `src/encryption.rs:249-251`
**Evidence**: Leading-slash stripping causes `/a/b.parquet` and `a/b.parquet` to resolve identically. Fallback iteration depends on HashMap order. Nondeterministic key selection can cause sporadic decryption failures.

### R10-CX-025: Timezone inconsistency in compaction functions (Priority: P3)
**Source**: Codex review 5
**Codex claimed**: P2 → **Validated**: P3 (downgraded)
**Location**: `src/compaction_functions.rs:433, 465, 517`
**Evidence**: `expire_snapshots` uses `TIMESTAMPTZ` while cleanup functions use `TIMESTAMP`. These are all passed through to DuckDB which handles timezone conversion internally. The inconsistency is cosmetic since DuckDB normalizes the comparison.

---

## Priority Summary

| Priority | Count | Findings |
|----------|-------|----------|
| P1 (High) | 4 | R10-CX-001, R10-CX-002, R10-CX-003, R10-CX-004 |
| P2 (Medium) | 9 | R10-CX-005, R10-CX-006, R10-CX-007, R10-CX-008, R10-CX-009, R10-CX-010, R10-CX-011, R10-CX-012, R10-CX-024 |
| P3 (Low) | 7 | R10-CX-013, R10-CX-014, R10-CX-017, R10-CX-020, R10-CX-021, R10-CX-022, R10-CX-023 |
| FP | 2 | R10-CX-018, R10-CX-025 (eliminated) |

### Actionable P1 fixes:
1. **R10-CX-001/002**: Add `AND c.table_id = <table_id>` to column joins in `get_file_column_stats` and partition column lookups
2. **R10-CX-003**: Use `SERIALIZABLE` isolation or `SELECT ... FOR UPDATE` for DDL snapshot creation on Postgres
3. **R10-CX-004**: Wrap per-file append registration (data_file + stats + partition values) in a single transaction
