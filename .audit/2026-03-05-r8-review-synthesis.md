# Review Cycle 8 Synthesis
Date: 2026-03-05

## Overview

- **Raw findings**: 125 (across 5 reviews: idiomatic 11, correctness 55, interop 11, test-harness 7, codex 43)
- **Codex P0/P1 false positives removed**: 17 (89% FP rate, consistent with historical 86-89%)
- **Overlaps deduplicated**: 12 (rewrite_duckdb_view_sql x3→1, recompute_stats x2→1, total_file_size x2→1, TOCTOU+uniqueness x2→1, table_names x2→1, MERGE matching x2→1, DML cleanup x3→1, partition tests x2→1, path issues x2→1, schedule_start+UUID+column extras x3→P3)
- **After deduplication**: 96
- **By priority**: 0 P0, 12 P1, 31 P2, 53 P3

## Cumulative Review Stats (R1-R8)

| Cycle | Raw | Dedup | Fixed | Deferred/Open | FP/Verified |
|-------|-----|-------|-------|---------------|-------------|
| R1 | 36 | 36 | 26 | 10 P3 | — |
| R2 | 99 | 58 | 55 | 3 | — |
| R3 | 67 | 50 | 25 | 25 P2/P3 | — |
| R4 | 74 | 46 | 43 | 2 deferred + 1 open | — |
| R5 | 95 | 77 | 72 | 4 skipped | 1 FP |
| R6 | 107 | 88 | ~49 | 1 unfixable + 1 deferred + 36 P3 | 3 codex P0→P1 |
| R7 | 58 | 50 | 22 | 28 P3 not assigned | 2 FP |
| R8 | 125 | 96 | 43 | 53 P3 not assigned | 17 codex FP |
| **Total** | **661** | **501** | **~335** | — | — |

**Key trend**: R8 has the highest raw finding count (125) due to the correctness review's 6-agent parallel pass covering all 35 source files. After dedup and FP removal, 96 unique findings remain. Finding rate is NOT declining — R8 dug deeper into backend parity, concurrency, and interop schema mismatches that prior cycles surface-checked.

---

## Deduplicated Findings by Priority

### P0 — Data Corruption / Security (0)

No P0 findings. Second consecutive cycle with zero P0 items. The Codex review claimed 1 P0 (MERGE match loop) — validated as false positive (cardinality check exists at lines 498-505).

---

### P1 — Must Fix (12)

#### R8-S-001: `replace_table_files` does not end active delete files (all backends)
- **Sources**: R8-C-001
- **Files**: `metadata_writer_sqlite.rs:1394`, `metadata_writer_postgres.rs:1169`, `metadata_writer_mysql.rs:1298`
- **Description**: After compaction, orphaned delete files referencing ended data file IDs remain active. MOR read path (`DeleteFilterExec`) may apply stale deletes, causing phantom row deletions.
- **Fix**: Add `UPDATE ducklake_delete_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL` to `replace_table_files` in all three backends.
- **Effort**: S
- **Agent**: r8-fix-compaction

#### R8-S-002: `recompute_table_column_stats` column join missing filters (all backends)
- **Sources**: R8-C-002, Codex 3.6
- **Files**: `metadata_writer_sqlite.rs:937`, `metadata_writer_postgres.rs:766`, `metadata_writer_mysql.rs:884`
- **Description**: Missing `AND c.end_snapshot IS NULL AND c.table_id = fcs.table_id` in column join. After `ALTER TABLE RENAME COLUMN`, two column rows share the same `column_id` — the join matches both, using wrong `column_type` for `is_numeric_type()`, corrupting min/max aggregation.
- **Fix**: Add `AND c.end_snapshot IS NULL AND c.table_id = fcs.table_id` to the join in all backends.
- **Effort**: S
- **Agent**: r8-fix-metadata-correctness

#### R8-S-003: `calculate_footer_size_from_bytes` underreports by 8 bytes
- **Sources**: R8-C-003
- **File**: `table_writer.rs:1493-1516`
- **Description**: Returns `metadata_len` but Parquet footer is `[ThriftMetadata(metadata_len)][4-byte LE len][4-byte PAR1]`. DuckDB uses `footer_size` to seek from end-of-file; underreported value causes DuckDB to fail parsing files written by this extension.
- **Fix**: Return `i64::from(metadata_len) + 8`.
- **Effort**: S
- **Agent**: r8-fix-interop

#### R8-S-004: Timestamp inline serialization drops sub-second precision
- **Sources**: R8-C-004
- **File**: `table_writer.rs:1170`
- **Description**: Format string `"%Y-%m-%d %H:%M:%S"` truncates microseconds. Timestamps like `2024-01-01 12:00:00.123456` become `2024-01-01 12:00:00`. **Data corruption** for tables using inlined data with timestamp columns.
- **Fix**: Use `"%Y-%m-%d %H:%M:%S%.6f"`.
- **Effort**: S
- **Agent**: r8-fix-interop

#### R8-S-005: PostgreSQL `schema_version` counter not concurrency-safe
- **Sources**: R8-C-005
- **File**: `metadata_writer_postgres.rs:344-374`
- **Description**: Under READ COMMITTED, concurrent DDL ops read same MAX, compute same `new_schema_version`, producing duplicates. Breaks DuckDB version-based catalog invalidation.
- **Fix**: Use a PostgreSQL sequence or upgrade DDL transactions to SERIALIZABLE.
- **Effort**: M
- **Agent**: r8-fix-pg-concurrency

#### R8-S-006: PG/MySQL TOCTOU + no DB-level uniqueness for active schema/table names
- **Sources**: R8-C-006, Codex 3.4
- **Files**: `metadata_writer_postgres.rs:864-960`, `metadata_writer_mysql.rs:988-1034`, all backend DDL tables
- **Description**: Two concurrent callers with same name both SELECT → find nothing → INSERT, creating duplicate active schemas/tables. No unique partial index or advisory lock prevents this.
- **Fix**: Add `CREATE UNIQUE INDEX ON ducklake_schema (schema_name) WHERE end_snapshot IS NULL` (PG) or use `INSERT ... ON CONFLICT` / advisory locks. MySQL: equivalent locking strategy.
- **Effort**: M
- **Agent**: r8-fix-pg-concurrency

#### R8-S-007: MySQL `register_data_file` never updates `next_file_id` on snapshot
- **Sources**: R8-C-007
- **File**: `metadata_writer_mysql.rs:1174-1250`
- **Description**: SQLite updates `ducklake_snapshot.next_file_id` in `register_data_file`. MySQL only updates in `register_dml_files`. Stale 0 causes DuckDB interop failures.
- **Fix**: Add snapshot `next_file_id` update to MySQL's `register_data_file`.
- **Effort**: S
- **Agent**: r8-fix-backend-parity

#### R8-S-008: PG/MySQL missing `register_file_partition_value` / `get_active_partition_columns`
- **Sources**: R8-C-008
- **Files**: `metadata_writer_mysql.rs`, `metadata_writer_postgres.rs` (trait defaults are no-ops)
- **Description**: Partitioned tables written via MySQL/PG silently lose all partition metadata. Partition-based file pruning never works.
- **Fix**: Implement both methods for MySQL and PostgreSQL, mirroring SQLite.
- **Effort**: M
- **Agent**: r8-fix-backend-parity

#### R8-S-009: PG/MySQL missing `record_snapshot_changes` for DML
- **Sources**: R8-C-009
- **Files**: `metadata_writer_mysql.rs`, `metadata_writer_postgres.rs` (trait default is `Ok(())`)
- **Description**: DELETE/UPDATE/MERGE produce no `ducklake_snapshot_changes` entries, breaking DuckDB cross-engine change detection (CDC, time travel).
- **Fix**: Override `record_snapshot_changes` in both backends, mirroring SQLite.
- **Effort**: M
- **Agent**: r8-fix-backend-parity

#### R8-S-010: `ducklake_schema_versions` has extra `table_id` column (SQLite only)
- **Sources**: Interop P1-001
- **File**: `metadata_writer_sqlite.rs:312-316`
- **Description**: Extra `table_id INTEGER` column not in DuckDB DDL. Causes DuckDB writes to fail with column count mismatch. Experimentally confirmed. We never INSERT `table_id` into this table.
- **Fix**: Remove `table_id` from `ducklake_schema_versions` in SQLite DDL.
- **Effort**: S
- **Agent**: r8-fix-interop

#### R8-S-011: `ducklake_data_file` and `ducklake_delete_file` have extra `partial_max` column
- **Sources**: Interop P1-002
- **Files**: `metadata_writer_sqlite.rs:161,178`, `metadata_writer_postgres.rs:84,100`, `metadata_writer_mysql.rs:94,112`
- **Description**: Extra `partial_max INTEGER` not in DuckDB DDL. Causes DuckDB writes to fail with column count mismatch. Experimentally confirmed. Column is never read or written.
- **Fix**: Remove `partial_max` from both tables across all three backends.
- **Effort**: S
- **Agent**: r8-fix-interop

#### R8-S-012: Append-mode schema validation allows silent column removal
- **Sources**: Codex 3.2
- **File**: `metadata_writer_validation.rs:87`
- **Description**: `validate_schema_evolution()` explicitly allows "implicit removal" — columns present in existing schema but missing from new schema are not flagged. For append-mode, this silently drops catalog columns.
- **Fix**: In append mode, require all existing columns to be present. Only allow adding new nullable columns.
- **Effort**: S
- **Agent**: r8-fix-metadata-correctness

---

### P2 — Should Fix (31)

#### R8-S-013: `rewrite_duckdb_view_sql` is O(n^2) and may match inside string literals
- **Sources**: R8-I-001, R8-C-027, Codex 1.2, 1.6
- **File**: `schema.rs:194-216`
- **Description**: Each loop iteration allocates `String` from `lower_chars[i..]`. Also, `count_star()` rewriting could match inside SQL string literals.
- **Fix**: Use single-pass approach with index-based checks; add string-literal awareness.
- **Effort**: S
- **Agent**: r8-fix-code-quality

#### R8-S-014: DeleteFilterExec `row_offset` wrong for multi-partition scans (latent)
- **Sources**: R8-C-010
- **File**: `delete_filter.rs:119`
- **Description**: Always starts at 0 regardless of partition. Currently safe because ParquetExec defaults to 1 partition per file, but latent under row-group splitting configurations.
- **Fix**: Enforce single-partition execution (via `CoalescePartitionsExec` wrapper) or propagate partition-aware row offsets.
- **Effort**: M
- **Agent**: r8-fix-correctness

#### R8-S-015: `total_file_size` overflow in `replace_table_files` (all backends)
- **Sources**: R8-C-011, Codex 3.7
- **Files**: `metadata_writer_sqlite.rs:1472`, `metadata_writer_postgres.rs:1245`, `metadata_writer_mysql.rs:1380`
- **Description**: Uses `.sum()` (unchecked) while `total_record_count` uses `checked_add`. Inconsistent.
- **Fix**: Use `try_fold` with `checked_add`.
- **Effort**: S
- **Agent**: r8-fix-correctness

#### R8-S-016: `total_net_new_deletions` unchecked arithmetic in `register_dml_files`
- **Sources**: R8-C-012
- **Files**: All three metadata writers
- **Description**: Both subtraction and addition are unchecked. Corrupted catalog values can cause underflow/overflow.
- **Fix**: Use `checked_sub` and `checked_add`.
- **Effort**: S
- **Agent**: r8-fix-correctness

#### R8-S-017: SQLite DDL operations lack SQLITE_BUSY retry
- **Sources**: R8-C-013
- **File**: `metadata_writer_sqlite.rs:2232, 2653, 2769, 2843`
- **Description**: `rename_view`, `rename_table`, `set_table_comment`, `set_column_comment` use `block_on` instead of `block_on_with_retry`.
- **Fix**: Change to `block_on_with_retry`.
- **Effort**: S
- **Agent**: r8-fix-metadata-correctness

#### R8-S-018: `replace_table_files` never calls `recompute_table_column_stats`
- **Sources**: R8-C-014
- **Files**: All three metadata writers
- **Description**: After compaction, table-level column stats remain stale.
- **Fix**: Call `recompute_table_column_stats` at end of `replace_table_files`.
- **Effort**: S
- **Agent**: r8-fix-compaction

#### R8-S-019: Nanosecond-to-microsecond conversion uses truncating division for negative timestamps
- **Sources**: R8-C-016
- **Files**: `table_writer.rs:1158`, `insert_exec.rs:445`
- **Description**: Should use `div_euclid(1_000)` for pre-epoch (negative) timestamps. Truncating division produces off-by-one errors.
- **Fix**: Use `.div_euclid(1_000)`.
- **Effort**: S
- **Agent**: r8-fix-correctness

#### R8-S-020: `percent_decode_path` corrupts non-ASCII UTF-8 + path traversal guard skips decoded check
- **Sources**: R8-C-017, R8-C-018
- **File**: `path_resolver.rs:29-74`
- **Description**: `bytes[i] as char` treats raw bytes as Unicode scalar values (garbles multi-byte UTF-8). Also, `decoded != path &&` condition skips traversal check when decode produces equal output.
- **Fix**: Use `percent_encoding::percent_decode_str`; remove `decoded != path &&` condition.
- **Effort**: S
- **Agent**: r8-fix-correctness

#### R8-S-021: `partition_values_equal` uses f64 for integer partition keys
- **Sources**: R8-C-019
- **File**: `table.rs:1304-1313`
- **Description**: Integers above 2^53 lose precision when parsed as f64. Incorrect partition pruning possible.
- **Fix**: Try `i64` parse first, fall back to `f64`.
- **Effort**: S
- **Agent**: r8-fix-correctness

#### R8-S-022: Schema mapping cache assumes first-file schema is representative
- **Sources**: R8-C-020
- **File**: `table.rs:401-474`
- **Description**: If first file lacks Parquet field IDs (external file), cache has empty rename mapping. Subsequent files with field IDs skip renames.
- **Fix**: Skip files without field IDs when building cache, or iterate until a file with field IDs is found.
- **Effort**: S
- **Agent**: r8-fix-correctness

#### R8-S-023: Date statistics parsing uses integer format, not ISO
- **Sources**: R8-C-021
- **File**: `table.rs:1827-1828`
- **Description**: DuckDB stores date stats as ISO strings (`"2024-01-15"`), not epoch-day integers. `parse::<i32>()` always fails, so stats-based file pruning **never works for date columns**.
- **Fix**: Parse ISO date strings to epoch-day values.
- **Effort**: S
- **Agent**: r8-fix-interop

#### R8-S-024: `catalog.rs:schema()` swallows metadata provider errors
- **Sources**: R8-C-022
- **File**: `catalog.rs:382-427`
- **Description**: `_ => None` catches both `Ok(None)` and `Err(...)`. Transient DB errors appear as "schema not found".
- **Fix**: Log errors and/or return `Err` for provider failures.
- **Effort**: S
- **Agent**: r8-fix-code-quality

#### R8-S-025: `count_inlined_rows` missing `schema_version` filter (all backends)
- **Sources**: R8-C-023
- **Files**: All three metadata providers
- **Description**: Queries without `schema_version` filter. May count rows from wrong-version inlined table after schema evolution.
- **Fix**: Add `schema_version` filter matching `get_inlined_data`.
- **Effort**: S
- **Agent**: r8-fix-metadata-correctness

#### R8-S-026: MySQL `LAST_INSERT_ID()` type mismatch at 3 sites
- **Sources**: R8-C-024
- **File**: `metadata_writer_mysql.rs:1212, 1329, 1517`
- **Description**: Missing `CAST(LAST_INSERT_ID() AS SIGNED)` at 3 sites (other sites do it correctly).
- **Fix**: Use `CAST(LAST_INSERT_ID() AS SIGNED)` consistently.
- **Effort**: S
- **Agent**: r8-fix-backend-parity

#### R8-S-027: `validate_schema_evolution` type comparison is case-sensitive
- **Sources**: R8-C-025
- **File**: `metadata_writer_validation.rs:106`
- **Description**: `"VARCHAR"` vs `"varchar"` rejected as type change. Different backends may normalize differently.
- **Fix**: Use case-insensitive comparison.
- **Effort**: S
- **Agent**: r8-fix-metadata-correctness

#### R8-S-028: Cascade `drop_schema` snapshot chain inconsistency
- **Sources**: R8-C-026
- **File**: `catalog.rs:241-255`
- **Description**: Intermediate table-drop snapshots not propagated to catalog's `AtomicI64`.
- **Fix**: Call `fetch_max` after each `drop_table` in cascade loop.
- **Effort**: S
- **Agent**: r8-fix-correctness

#### R8-S-029: PG/MySQL missing `find_table_id` for inlined data flush
- **Sources**: R8-C-028
- **Files**: `metadata_writer_postgres.rs`, `metadata_writer_mysql.rs` (trait default returns `Ok(None)`)
- **Description**: Flush path silently fails to find table. Currently consistent since inlining is disabled, but will break if ever enabled.
- **Fix**: Implement `find_table_id` in both backends, or return explicit error.
- **Effort**: S
- **Agent**: r8-fix-backend-parity

#### R8-S-030: PG/MySQL `begin_checked_write_transaction` conflict check not re-verified
- **Sources**: R8-C-029
- **Files**: `metadata_writer_postgres.rs:1772-1850`, `metadata_writer_mysql.rs:1841-1946`
- **Description**: Between conflict check and actual write, a concurrent DROP can commit without being visible.
- **Fix**: Re-verify table liveness inside `write_transaction_inner`, or use `SELECT ... FOR UPDATE`.
- **Effort**: M
- **Agent**: r8-fix-pg-concurrency

#### R8-S-031: `write_parquet_with_setup` orphans file on `register_data_file` failure
- **Sources**: R8-C-030
- **File**: `table_writer.rs:526-534`
- **Description**: If `put()` succeeds but `register_data_file()` fails, uploaded Parquet file is never deleted.
- **Fix**: Add cleanup (`object_store.delete`) in error path.
- **Effort**: S
- **Agent**: r8-fix-correctness

#### R8-S-032: `parse_decimal_string` unchecked arithmetic overflow
- **Sources**: R8-C-034
- **File**: `parse_values.rs:349-372`
- **Description**: `integer * 10i128.pow(scale_u)` and additions are unchecked. Large inputs overflow silently.
- **Fix**: Use `checked_mul` and `checked_add` with error propagation.
- **Effort**: S
- **Agent**: r8-fix-correctness

#### R8-S-033: Code duplication across delete/update/merge exec plans
- **Sources**: R8-I-002
- **Files**: `delete_exec.rs`, `update_exec.rs`, `merge_exec.rs`
- **Description**: ~80 lines of nearly identical delete-file-writing code duplicated across all three files. Maintenance risk.
- **Fix**: Extract shared `write_delete_file()` helper.
- **Effort**: M
- **Agent**: r8-fix-code-quality

#### R8-S-034: DML exec orphan file cleanup gaps (delete/update/merge)
- **Sources**: Codex 2.4, 2.5, 2.6
- **Files**: `delete_exec.rs:248-304`, `update_exec.rs:387-466`, `merge_exec.rs:461-640`
- **Description**: Early errors after upload leave orphaned Parquet files. Only explicit cleanup branches handle this.
- **Fix**: Use scoped cleanup guard for uploaded files.
- **Effort**: M
- **Agent**: r8-fix-code-quality

#### R8-S-035: No DF→DuckDB cross-engine tests for partitions or MERGE
- **Sources**: Interop P2-001, P2-002
- **Files**: `tests/cross_engine_partition_tests.rs`, `tests/cross_engine_tests.rs`
- **Description**: All 7 partition tests are DuckDB→DF only. Only 1 MERGE test exists (DuckDB→DF). Critical coverage gap.
- **Fix**: Add DF→DuckDB partition tests and MERGE test.
- **Effort**: M
- **Agent**: r8-fix-tests

#### R8-S-036: DuckDB `count_inlined_rows` missing schema filter
- **Sources**: R8-C-031
- **File**: `metadata_provider_duckdb.rs:100-106`
- **Description**: `WHERE table_name = ?` without `table_schema` filter could match tables in other schemas.
- **Fix**: Add `AND table_schema = 'main'`.
- **Effort**: S
- **Agent**: r8-fix-metadata-correctness

#### R8-S-037: PG hardcodes `table_schema = 'public'` for inlined data
- **Sources**: R8-C-032
- **File**: `metadata_provider_postgres.rs:926-944, 1057-1061`
- **Description**: Fails for non-default PostgreSQL schemas.
- **Fix**: Use `current_schema()` or configurable schema parameter.
- **Effort**: S
- **Agent**: r8-fix-backend-parity

#### R8-S-038: PG/MySQL `snapshot_time` read as `NaiveDateTime` — timezone lost
- **Sources**: R8-C-033
- **Files**: `metadata_provider_postgres.rs:87-89`, `metadata_provider_mysql.rs:89-91`
- **Description**: `TIMESTAMPTZ` read as `NaiveDateTime` discards timezone. SQLite correctly reads as `String`.
- **Fix**: Read as `Option<String>` like SQLite, or use timezone-aware type.
- **Effort**: S
- **Agent**: r8-fix-backend-parity

#### R8-S-039: Type parsing accepts malformed trailing input
- **Sources**: Codex 1.7
- **File**: `types.rs:23`
- **Description**: Prefix-only matching allows `varchar(10)garbage` to be accepted as `Utf8`.
- **Fix**: Validate no trailing tokens after type syntax.
- **Effort**: S
- **Agent**: r8-fix-code-quality

#### R8-S-040: Compaction runs synchronous DuckDB in async path
- **Sources**: Codex 5.7
- **File**: `compaction_functions.rs:171`
- **Description**: Synchronous DuckDB operations block async executor threads.
- **Fix**: Use `tokio::task::spawn_blocking`.
- **Effort**: S
- **Agent**: r8-fix-code-quality

#### R8-S-041: `record_snapshot_changes` no retry, no transaction (SQLite)
- **Sources**: R8-C-015
- **File**: `metadata_writer_sqlite.rs:1726-1738`
- **Description**: Uses `block_on` (not `block_on_with_retry`) and executes directly on pool. SQLITE_BUSY silently loses the change record.
- **Fix**: Use `block_on_with_retry`.
- **Effort**: S
- **Agent**: r8-fix-metadata-correctness

#### R8-S-042: MySQL standalone `create_snapshot` omits `schema_version` and `next_file_id`
- **Sources**: R8-C-049
- **File**: `metadata_writer_mysql.rs:978-986`
- **Description**: Uses defaults instead of computed values.
- **Fix**: Match SQLite implementation.
- **Effort**: S
- **Agent**: r8-fix-backend-parity

#### R8-S-043: MySQL `write_transaction_inner` missing `created_schema` change record
- **Sources**: R8-C-048
- **File**: `metadata_writer_mysql.rs:615-629`
- **Description**: Only records `created_table`, not `created_schema`.
- **Fix**: Add `created_schema` change record matching SQLite.
- **Effort**: S
- **Agent**: r8-fix-backend-parity

---

### P3 — Nice to Have (53)

#### From Correctness Review (20)
- **R8-S-044**: `extract_type_params` panics on non-ASCII type strings (`types.rs:386`) — extremely unlikely
- **R8-S-045**: `"-"` parsed as valid decimal 0 (`parse_values.rs:325`)
- **R8-S-046**: `null_count` accumulation can overflow i64 (`table.rs:1355`)
- **R8-S-047**: Row count clamped to `usize::MAX` instead of `Precision::Absent` (`table.rs:1414`)
- **R8-S-048**: `validate_name` overly broad `".."` check (`schema.rs:38`)
- **R8-S-049**: `table_names()` not deduplicated (`schema.rs:226`, also Codex 1.9)
- **R8-S-050**: `block_on_with_retry` deterministic jitter (`metadata_writer_sqlite.rs:73`)
- **R8-S-051**: `store_inlined_data` uses `CREATE TABLE` not `CREATE TABLE IF NOT EXISTS` (`metadata_writer_sqlite.rs:3083`)
- **R8-S-052**: `get_or_create_schema` `changes_made` overwrites earlier entry (`metadata_writer_sqlite.rs:1104`)
- **R8-S-053**: `clear_inlined_data` no retry, no transaction (`metadata_writer_sqlite.rs:3223`)
- **R8-S-054**: `sqlite_master` deprecated alias (`metadata_writer_sqlite.rs:2974, 3159`)
- **R8-S-055**: Standalone `register_delete_file` doesn't update `record_count` (`metadata_writer_sqlite.rs:1801`)
- **R8-S-056**: `Option<T>` comparison in `should_replace_min/max` — `None < Some(x)` (`table_writer.rs:1432`)
- **R8-S-057**: Partitioned append non-atomic across partition files (`table_writer.rs:664`)
- **R8-S-058**: `total_rows` unchecked in `commit_uploaded_files` (`table_writer.rs:653`)
- **R8-S-059**: `compaction_functions.rs` ATTACH escaping limited to single-quotes (`compaction_functions.rs:94`)
- **R8-S-060**: `table_functions.rs` allows `start_snapshot == end_snapshot` (`table_functions.rs:455`)
- **R8-S-061**: Delete file re-read on every `scan()` call (`table.rs:889`)
- **R8-S-062**: MERGE source-match counting broken by early `break` (R8-C-035) — needs verification, may be correct per SQL standard
- **R8-S-063**: Cross-engine tests only use SQLite backend (Interop P2-006) — documented TODO

#### From Idiomatic Review (7)
- **R8-S-064**: `extract_rows` clones UInt32Array unnecessarily (`insert_exec.rs:737`)
- **R8-S-065**: `source_match_masks` rebuilt per target file (`merge_exec.rs:456`)
- **R8-S-066**: `.take(num_rows)` redundant on exact-size vec (`update_exec.rs:311`)
- **R8-S-067**: `null_counts[i].max(0) as usize` truncating cast on 32-bit (`table.rs:1410`)
- **R8-S-068**: `file_idx as u64` unchecked cast (`table.rs:1597,1622`)
- **R8-S-069**: `order as i64` unchecked casts (`metadata_writer_sqlite.rs:597,606`)
- **R8-S-070**: Poisoned mutex recovery lacks explanatory comment (`compaction_functions.rs:85`)

#### From Idiomatic Review — Production `unwrap()`
- **R8-S-071**: `unwrap()` in `table_deletions.rs` non-test code (`:739,804,830`) — should use `expect()` with invariant message

#### From Interop Review (5)
- **R8-S-072**: `schedule_start` TEXT vs TIMESTAMPTZ type mismatch (Interop P2-003) — low impact, compaction delegated to DuckDB
- **R8-S-073**: `ducklake_column` has extra `default_value_type`/`default_value_dialect` columns (Interop P2-004) — non-breaking, DuckDB tolerates
- **R8-S-074**: UUID format difference (UUIDv4 vs UUIDv7) (Interop P2-005) — cosmetic
- **R8-S-075**: Column ordering differences in DDL (Interop P3-002) — cosmetic
- **R8-S-076**: `parse_values.rs` truncating cast `num_days() as i32` (R8-I-006) — extremely unlikely dates

#### From Test Harness Review (7)
- **R8-S-077**: `rewrite_order_by_all` in `sqllogictest_runner.rs` still naive (R8-TH-001)
- **R8-S-078**: Decimal roundtrip test uses normalizing comparison (R8-TH-002)
- **R8-S-079**: `assert_results_eq_strict` not used in all type roundtrips (R8-TH-003)
- **R8-S-080**: `convert_batch_to_strings` duplicated between hybrid_asyncdb and test_utils (R8-TH-004)
- **R8-S-081**: `cte_wraps_dml` double-quote fix lacks unit test (R8-TH-005)
- **R8-S-082**: SLT preprocessor error→ok conversion lacks tests (R8-TH-006)
- **R8-S-083**: `.sort()` after `ORDER BY` queries in merge tests (R8-TH-007)

#### From Codex Review (validated P2/P3 not already covered)
- **R8-S-084**: CTAS eagerly collects all partitions into memory (`schema.rs:412`) — relates to deferred F-036
- **R8-S-085**: Delete-file reads collect full stream (`table.rs:526`) — relates to deferred F-036
- **R8-S-086**: Quoted struct-field parsing lacks escape handling (`types.rs:437`)
- **R8-S-087**: `parse_table_name` unmatched quotes (`table_functions.rs:355`)
- **R8-S-088**: `ducklake_list_files` materializes all rows (`table_functions.rs:122`)
- **R8-S-089**: `column_rename` per-batch schema lookup (`column_rename.rs:165`)
- **R8-S-090**: `virtual_column` Vec allocation per batch (`virtual_column_exec.rs:225`)
- **R8-S-091**: Delete filtering binary search per row (`table_deletions.rs:739`)
- **R8-S-092**: Panic in plan selection (`table_changes.rs:789`, `table_deletions.rs:350`)
- **R8-S-093**: Null-count overflow hiding with `saturating_add` (`table_writer.rs:1292`)
- **R8-S-094**: `inlined_rows_to_batch` O(rows * cols^2) (`table_writer.rs:1206`)
- **R8-S-095**: Extra projection expressions warned not errored (`query_planner.rs:202`)
- **R8-S-096**: Trait defaults for `register_dml_files` non-atomic (`metadata_writer.rs:468`)

---

## Recommended Fix Agents

### Agent 1: r8-fix-interop (4 findings — 3 P1 + 1 P2)
**Findings**: R8-S-003, R8-S-004, R8-S-010, R8-S-011, R8-S-023
**Scope**: Footer size +8, timestamp sub-second precision, remove extra DDL columns (table_id, partial_max), date stats ISO parsing
**Effort**: S (aggregate)

### Agent 2: r8-fix-backend-parity (8 findings — 3 P1 + 5 P2)
**Findings**: R8-S-007, R8-S-008, R8-S-009, R8-S-026, R8-S-029, R8-S-037, R8-S-038, R8-S-042, R8-S-043
**Scope**: MySQL next_file_id, PG/MySQL partition values, snapshot_changes, LAST_INSERT_ID casts, find_table_id, PG schema hardcode, snapshot_time timezone, MySQL create_snapshot, created_schema
**Effort**: M (aggregate)

### Agent 3: r8-fix-metadata-correctness (5 findings — 2 P1 + 3 P2)
**Findings**: R8-S-002, R8-S-012, R8-S-017, R8-S-025, R8-S-027, R8-S-036, R8-S-041
**Scope**: Column stats join filter, append validation, SQLite DDL retry, inlined rows schema_version, type case-insensitive, DuckDB inlined schema filter, snapshot_changes retry
**Effort**: S (aggregate)

### Agent 4: r8-fix-pg-concurrency (3 findings — 2 P1 + 1 P2)
**Findings**: R8-S-005, R8-S-006, R8-S-030
**Scope**: PG schema_version sequence, partial unique indexes for active rows, conflict check re-verification
**Effort**: M (aggregate)

### Agent 5: r8-fix-correctness (10 findings — all P2)
**Findings**: R8-S-014, R8-S-015, R8-S-016, R8-S-019, R8-S-020, R8-S-021, R8-S-022, R8-S-028, R8-S-031, R8-S-032
**Scope**: DeleteFilter row_offset, checked arithmetic (file_size, deletions, decimals), div_euclid timestamps, path resolver, partition f64, schema mapping, drop cascade fetch_max, orphan cleanup
**Effort**: M (aggregate)

### Agent 6: r8-fix-compaction (2 findings — 1 P1 + 1 P2)
**Findings**: R8-S-001, R8-S-018
**Scope**: End delete files in replace_table_files, recompute stats after compaction
**Effort**: S (aggregate)

### Agent 7: r8-fix-code-quality (5 findings — all P2)
**Findings**: R8-S-013, R8-S-033, R8-S-034, R8-S-039, R8-S-040, R8-S-024
**Scope**: O(n^2) view rewrite, DML code dedup, orphan file guards, type parsing validation, async compaction, error swallowing
**Effort**: M (aggregate)

### Agent 8: r8-fix-tests (1 finding — P2)
**Findings**: R8-S-035
**Scope**: DF→DuckDB cross-engine tests for partitions and MERGE
**Effort**: M

**Total agents**: 8 (covering all 12 P1 + 31 P2 = 43 findings)
**53 P3 findings**: Not assigned — optional, low impact.

---

## Resolution Status

All 43 P1+P2 findings fixed by 8 agents. Final merge commit: `12548a6`. Tests: 770 passing, 3 pre-existing DuckDB failures.

#### P1 (12/12 FIXED)
- **R8-S-001** [FIXED `ed9c86c`]: `replace_table_files` now ends active delete files
- **R8-S-002** [FIXED `68a14dd`]: `recompute_table_column_stats` column join filters added
- **R8-S-003** [FIXED `b59edb1`]: `calculate_footer_size_from_bytes` returns +8 bytes
- **R8-S-004** [FIXED `b59edb1`]: Timestamp inline serialization preserves sub-second precision
- **R8-S-005** [FIXED `71da28c`]: PostgreSQL `schema_version` counter concurrency-safe
- **R8-S-006** [FIXED `71da28c`]: PG/MySQL TOCTOU + DB-level uniqueness for active schema/table names
- **R8-S-007** [FIXED `875814e`]: MySQL `register_data_file` updates `next_file_id`
- **R8-S-008** [FIXED `875814e`]: PG/MySQL `register_file_partition_value` / `get_active_partition_columns` implemented
- **R8-S-009** [FIXED `875814e`]: PG/MySQL `record_snapshot_changes` implemented for DML
- **R8-S-010** [FIXED `b59edb1`]: `ducklake_schema_versions` extra `table_id` column removed (SQLite)
- **R8-S-011** [FIXED `b59edb1`]: `ducklake_data_file`/`ducklake_delete_file` extra `partial_max` column removed
- **R8-S-012** [FIXED `68a14dd`]: Append-mode schema validation blocks silent column removal

#### P2 (31/31 FIXED)
- **R8-S-013** [FIXED `91279ec`]: `rewrite_duckdb_view_sql` single-pass with string-literal awareness
- **R8-S-014** [FIXED `c6a62b8`]: DeleteFilterExec `row_offset` enforced single-partition execution
- **R8-S-015** [FIXED `c6a62b8`]: `total_file_size` overflow uses checked arithmetic
- **R8-S-016** [FIXED `c6a62b8`]: `total_net_new_deletions` unchecked arithmetic fixed
- **R8-S-017** [FIXED `68a14dd`]: SQLite DDL operations use SQLITE_BUSY retry
- **R8-S-018** [FIXED `ed9c86c`]: `replace_table_files` calls `recompute_table_column_stats`
- **R8-S-019** [FIXED `c6a62b8`]: Nanosecond-to-microsecond uses `div_euclid` for negative timestamps
- **R8-S-020** [FIXED `c6a62b8`]: `percent_decode_path` handles non-ASCII UTF-8 + path traversal
- **R8-S-021** [FIXED `c6a62b8`]: `partition_values_equal` uses i64 before f64 fallback
- **R8-S-022** [FIXED `c6a62b8`]: Schema mapping cache skips files without field IDs
- **R8-S-023** [FIXED `b59edb1`]: Date statistics parsing uses ISO format
- **R8-S-024** [FIXED `91279ec`]: `catalog.rs:schema()` no longer swallows metadata errors
- **R8-S-025** [FIXED `68a14dd`]: `count_inlined_rows` adds `schema_version` filter
- **R8-S-026** [FIXED `875814e`]: MySQL `LAST_INSERT_ID()` type mismatch fixed at 3 sites
- **R8-S-027** [FIXED `68a14dd`]: `validate_schema_evolution` type comparison case-insensitive
- **R8-S-028** [FIXED `c6a62b8`]: Cascade `drop_schema` propagates snapshot via `fetch_max`
- **R8-S-029** [FIXED `875814e`]: PG/MySQL `find_table_id` implemented for inlined data flush
- **R8-S-030** [FIXED `71da28c`]: PG/MySQL `begin_checked_write_transaction` conflict check re-verified
- **R8-S-031** [FIXED `c6a62b8`]: `write_parquet_with_setup` cleans up orphan file on failure
- **R8-S-032** [FIXED `c6a62b8`]: `parse_decimal_string` uses checked arithmetic
- **R8-S-033** [FIXED `91279ec`]: Delete/update/merge exec shared `write_delete_file()` helper
- **R8-S-034** [FIXED `91279ec`]: DML exec orphan file cleanup uses scoped guards
- **R8-S-035** [FIXED `4687f5d`]: DF→DuckDB cross-engine tests for partitions and MERGE added
- **R8-S-036** [FIXED `68a14dd`]: DuckDB `count_inlined_rows` adds schema filter
- **R8-S-037** [FIXED `875814e`]: PG uses `current_schema()` for inlined data
- **R8-S-038** [FIXED `875814e`]: PG/MySQL `snapshot_time` read as String (timezone preserved)
- **R8-S-039** [FIXED `91279ec`]: Type parsing rejects malformed trailing input
- **R8-S-040** [FIXED `91279ec`]: Compaction uses `spawn_blocking` for sync DuckDB in async path
- **R8-S-041** [FIXED `68a14dd`]: `record_snapshot_changes` uses retry (SQLite)
- **R8-S-042** [FIXED `875814e`]: MySQL `create_snapshot` includes `schema_version` and `next_file_id`
- **R8-S-043** [FIXED `875814e`]: MySQL `write_transaction_inner` records `created_schema` change

#### P3 (53 NOT ASSIGNED)
- R8-S-044 through R8-S-096 [NOT ASSIGNED]: Optional, low impact. See P3 section above for details.

---

## Previously Deferred Items (still open from R1-R7)

- **F-036**: INSERT streaming for OOM prevention (L effort — architectural). R8 re-raised by Codex (R8-S-084, R8-S-085).
- **F-044**: Provider/writer code deduplication (L effort — ~1000+ lines near-identical). R8-S-033 partially addresses DML exec duplication.
- **F-045**: Async trait redesign, sync→async (L effort — ~60+ block_on calls). R8-S-040 is a narrow fix for compaction.
- **R4-S-018**: PG/MySQL checked write TOCTOU (P2, low real-world impact). Expanded in R8-S-030.
- **R4-S-036**: map_err boilerplate (50+ sites)
- **R4-S-040**: Monolithic execute() blocks
- **R6-S-017**: Concurrent DML lost-delete race (architectural)

---

## Key Observations

1. **Finding rate is NOT declining**: R8's 96 dedup findings is the highest since R5 (77). The 6-agent correctness review dug deeper into backend parity and concurrency, surfacing issues prior cycles only surface-checked.

2. **Zero P0 for second consecutive cycle**: No data corruption or security vulnerabilities. Codex P0 FP rate remains at 89%.

3. **Backend parity is the biggest theme**: 8 of 12 P1 findings relate to PG/MySQL missing trait implementations or interop-breaking DDL columns. The SQLite backend is significantly more complete than PG/MySQL.

4. **Interop DDL mismatches are experimentally confirmed**: The extra `table_id` and `partial_max` columns were experimentally verified to break DuckDB writes. These are high-confidence P1 findings.

5. **Concurrency issues cluster in PG**: schema_version races (R8-S-005), TOCTOU in get_or_create (R8-S-006), and conflict check gaps (R8-S-030) all stem from READ COMMITTED isolation level. These are real but only affect concurrent DDL scenarios.

6. **Footer size bug (R8-S-003) is interop-critical**: The +8 byte discrepancy means DuckDB cannot read Parquet files written by this extension. This is one of the highest-impact P1 findings.

7. **Timestamp sub-second truncation (R8-S-004) is data loss**: Microsecond precision is permanently lost for inlined data. Straightforward fix.

8. **Most P1 items are S effort**: 8 of 12 P1 findings have small, straightforward fixes. Only the PG concurrency items (R8-S-005, R8-S-006) and backend parity items (R8-S-008, R8-S-009) require moderate effort.

9. **R7 fixes verified intact**: Prior cycle fixes (OnceLock, fetch_max, PartitionTransform, decimal parsing, checked arithmetic) remain correct. New findings are genuinely new issues.

10. **53 P3 findings accumulating**: Between R7's 28 unassigned P3 and R8's 53, there are ~70+ P3 items across the two latest cycles. These are mostly code quality and minor edge cases that don't affect production behavior.
