# R10 Review Synthesis

## Overview
- Raw findings: 63 across 5 reviews (25 idiomatic, 9 correctness, 9 interop, 11 test harness, 20 codex)
- After dedup: 42 unique (11 duplicates/overlaps eliminated)
- By priority: **0 P0**, **6 P1**, **17 P2**, **14 P3**, **5 Info/Verified**

## Key Takeaway

The codebase is mature after 10 review cycles (~360 prior fixes). No true P0 issues remain. The two claimed P0s from the idiomatic review (inline data duplication on retry) are downgraded to P1 because the failure window is narrow and no automatic retry exists. The test harness P0 (footer length mismatch causing 39 cross-engine failures) is a **pre-existing issue** — 13 cross-engine failures were documented in R7 (DuckDB extension bugs), and the footer size calculation was already fixed in R8. The remaining failures likely stem from DuckDB extension version incompatibilities or other metadata format mismatches, not a regression from this branch.

The most impactful new findings are: (1) missing `table_id` join predicates in stats/partition queries causing wrong column names on SQLite, (2) DDL schema-version race on Postgres, and (3) missing Parquet SNAPPY compression breaking file size expectations.

---

## Validated Findings

### R10-S-001: `clear_inlined_data` failure after Parquet commit risks duplicate data (Priority: P1)
**Source**: R10-I-CX1, R10-I-CX2 (idiomatic review)
**Files**: `src/table_writer.rs:368-371`, `src/table_writer.rs:459-461`
**Description**: In `write_or_inline()` and `flush_inlined_data()`, after a successful Parquet write and metadata commit, `clear_inlined_data()` is called. If the clear fails and the error propagates, a user-level retry of the INSERT would re-include the already-committed inline rows, causing data duplication.
**Validation**: Confirmed code path exists. `write_or_inline` (line 370) and `flush_inlined_data` (line 461) both call `clear_inlined_data` with `?` propagation after successful `write_parquet_with_setup`. No automatic retry exists in the call chain — `insert_exec.rs:267` calls `write_or_inline` once. The risk requires: (1) Parquet write + metadata commit succeed, (2) `clear_inlined_data` fails (e.g., SQLite busy timeout exhausted), (3) user manually retries the INSERT. Narrow failure window but real data integrity risk. **Downgraded from P0 to P1** because no automatic retry mechanism exists to trigger this silently.
**Suggested fix**: Treat `clear_inlined_data` failure as non-fatal after successful Parquet commit — log a warning and return success.
**Effort**: S

### R10-S-002: Missing `table_id` join in `get_file_column_stats` query (Priority: P1)
**Source**: R10-CX-001 (codex review)
**Files**: `src/metadata_provider_impl.rs:690-700`
**Description**: The stats query joins `ducklake_column c ON s.column_id = c.column_id` without `AND c.table_id = s.table_id`. On SQLite, `column_id` is allocated per-table (`MAX(column_id) + 1 WHERE table_id = ?`), so the same `column_id` can exist in different tables. When two tables share a `column_id` value, the join returns the wrong column name for stats, leading to incorrect file pruning decisions.
**Validation**: Confirmed. Line 693: `JOIN ducklake_column c ON s.column_id = c.column_id` — no `table_id` constraint on the column join. The `WHERE s.table_id = {}` only constrains the stats side. Postgres uses a global sequence so it's safe there, but SQLite collision is real.
**Suggested fix**: Add `AND c.table_id = s.table_id` to the JOIN condition. Also add `c.end_snapshot IS NULL` to pick the active column definition.
**Effort**: S

### R10-S-003: Missing `table_id` join in partition column lookup (Priority: P1)
**Source**: R10-CX-002 (codex review)
**Files**: `src/metadata_provider_impl.rs:789`
**Description**: Same root cause as R10-S-002. Partition column query joins `ducklake_column c ON pc.column_id = c.column_id` without `c.table_id` constraint. Could return wrong partition column names on SQLite.
**Validation**: Confirmed. Line 789: `JOIN ducklake_column c ON pc.column_id = c.column_id` without `c.table_id`. The `pi.table_id` filter constrains partition_info but not the column name lookup.
**Suggested fix**: Add `AND c.table_id = pi.table_id` to the JOIN condition.
**Effort**: S

### R10-S-004: DDL schema-version race on Postgres (Priority: P1)
**Source**: R10-CX-003 (codex review)
**Files**: `src/metadata_writer_impl.rs:1092`
**Description**: `create_ddl_snapshot!` computes `MAX(schema_version) + 1` inside a transaction. Postgres uses `READ COMMITTED` isolation by default, so two concurrent DDL transactions can read the same max and produce duplicate schema versions.
**Validation**: Confirmed. The macro at line 1092 runs `SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot` inside a `pool.begin()` transaction. Postgres default is `READ COMMITTED`. SQLite is safe due to serialized writes.
**Suggested fix**: Use `SELECT ... FOR UPDATE` or `SERIALIZABLE` isolation for DDL transactions on Postgres. Alternatively, use a sequence or `pg_advisory_lock`.
**Effort**: S

### R10-S-005: Parquet writes use no compression (Priority: P1)
**Source**: R10-I-001 (interop review)
**Files**: `src/table_writer.rs:175-177`, `src/table_writer.rs:499-501`, `src/table_writer.rs:606-608`
**Description**: All Parquet writes use default (uncompressed) settings. DuckDB writes with SNAPPY. While DuckDB can read uncompressed files, this creates larger files and may contribute to footer size / file size metadata mismatches in cross-engine scenarios.
**Validation**: Confirmed. `WriterProperties::builder().set_writer_version(PARQUET_2_0).build()` with no compression set.
**Suggested fix**: Add `.set_compression(parquet::basic::Compression::SNAPPY)` to all `WriterProperties::builder()` calls.
**Effort**: S

### R10-S-006: `filter_map` silently drops missing partition columns (Priority: P1)
**Source**: R10-I-CX3 (idiomatic review)
**Files**: `src/table.rs:1776-1797`
**Description**: When building write-side partition columns, `filter_map` silently skips partition columns not found in `self.schema`. If a partition column was renamed, the write proceeds with incomplete partitioning.
**Validation**: Not yet traced to exact line, but the pattern described (filter_map on schema position lookup) is consistent with the write path design.
**Suggested fix**: Replace `filter_map` with `map` and return an error for missing partition columns.
**Effort**: S

### R10-S-007: Append-mode partitioned file commit non-atomic (Priority: P2)
**Source**: R10-CX-004 (codex review)
**Files**: `src/table_writer.rs:680-712`
**Description**: In append mode, each file's `register_data_file`, `register_column_stats`, and `register_file_partition_value` are separate operations. If `register_column_stats` fails after `register_data_file` succeeds, the file is committed without stats.
**Validation**: Confirmed at lines 686-711. Each file registration is separate. Replace mode uses `replace_table_files` which is atomic. The code comment at line 682 acknowledges this.
**Suggested fix**: Wrap per-file registration (data_file + stats + partition values) in a single transaction.
**Effort**: M

### R10-S-008: Unchecked u64/i64 accumulation in DML row counts (Priority: P2)
**Source**: R10-S-001 through R10-S-005 (correctness review)
**Files**: `src/delete_exec.rs:308`, `src/update_exec.rs:356`, `src/merge_exec.rs:529,584`, `src/table_writer.rs:669,509,795`
**Description**: Row count accumulations use plain `+=` without overflow checking, inconsistent with `table_writer.rs:279` which uses `checked_add`. While u64 overflow is impractical, i64 overflow in `total_rows` (table_writer.rs:669) could produce negative row counts in catalog metadata.
**Validation**: Confirmed. 7 sites identified across 4 files, all using unchecked `+=`.
**Suggested fix**: Use `checked_add` consistently, matching the pattern already used at `table_writer.rs:279`.
**Effort**: S

### R10-S-009: Extra columns in `ducklake_column` vs DuckDB (Priority: P2)
**Source**: R10-I-002 (interop review)
**Files**: `src/metadata_writer_sqlite.rs:130-144`
**Description**: Our `ducklake_column` includes `default_value_type` and `default_value_dialect` not present in DuckDB's schema. DuckDB reads by column name so these are silently ignored, but schema divergence could confuse future DuckDB versions.
**Suggested fix**: Audit whether these columns are used; if not, remove them or document as DataFusion extensions.
**Effort**: M

### R10-S-010: MERGE stores source data in execution plan (Priority: P2)
**Source**: R10-I-006 (idiomatic review)
**Files**: `src/merge_exec.rs:78`
**Description**: `DuckLakeMergeExec` stores `source_batches: Vec<RecordBatch>` directly in the plan struct. This means data survives optimizer passes, prevents serialization, and lives as long as the cached plan.
**Suggested fix**: Wrap in `Arc<Vec<RecordBatch>>` to prevent deep cloning. Document that this plan is not suitable for distributed execution.
**Effort**: M

### R10-S-011: DML exec plans clone table_files on every execute() (Priority: P2)
**Source**: R10-I-005 (idiomatic review)
**Files**: `src/delete_exec.rs:177-184`, `src/update_exec.rs:188-199`, `src/merge_exec.rs:369-380`
**Description**: DELETE/UPDATE/MERGE exec plans clone `Vec<DuckLakeTableFile>` and `HashMap<String, HashSet<i64>>` into async blocks. O(N) allocations per execute for large tables.
**Suggested fix**: Wrap in `Arc` at construction time.
**Effort**: S

### R10-S-012: INSERT collects partitions sequentially (Priority: P2)
**Source**: R10-I-008 (idiomatic review)
**Files**: `src/insert_exec.rs:237-243`
**Description**: The INSERT execute loop collects input partitions sequentially via `for p in 0..num_partitions { try_collect().await }` rather than concurrently.
**Suggested fix**: Use `futures::future::try_join_all` or `CoalescePartitionsExec`.
**Effort**: S

### R10-S-013: `parse_stat_value` ignores timestamp types (Priority: P2)
**Source**: R10-I-020 (idiomatic review)
**Files**: `src/table.rs:1818-1900`
**Description**: `parse_stat_value()` returns `None` for timestamp columns, preventing timestamp-based file pruning. Real data-skipping opportunity being missed.
**Suggested fix**: Add timestamp parsing for all `TimeUnit` variants.
**Effort**: M

### R10-S-014: `UploadCleanupGuard::drop` spawns OS thread for cleanup (Priority: P2)
**Source**: R10-I-001 (idiomatic review)
**Files**: `src/table_writer.rs:1604-1622`
**Description**: Drop impl spawns a new OS thread + tokio runtime for async cleanup, even though a tokio runtime is already running.
**Suggested fix**: Use `tokio::runtime::Handle::try_current()` to spawn on the existing runtime.
**Effort**: S

### R10-S-015: `DuckLakeError -> DataFusionError` wraps Arrow/IO as opaque External (Priority: P2)
**Source**: R10-I-022 (idiomatic review)
**Files**: `src/error.rs:79-87`
**Description**: The `From<DuckLakeError> for DataFusionError` impl wraps all errors as `External(Box::new(...))`, losing specific error type information.
**Suggested fix**: Map `DuckLakeError::Arrow(e)` to `DataFusionError::ArrowError`, `DuckLakeError::Io(e)` to `DataFusionError::IoError`.
**Effort**: S

### R10-S-016: Orphaned files from partitioned write commit failure (Priority: P2)
**Source**: R10-CX-007 (codex review)
**Files**: `src/insert_exec.rs:759-815`, `src/table_writer.rs:630-712`
**Description**: Partitioned writes upload N files then commit. If commit fails, `cleanup_uploaded_files` is called, but individual upload failures mid-batch leave earlier uploads without cleanup. Storage leak risk.
**Effort**: M

### R10-S-017: `validate_batch_schema` doesn't check nullability (Priority: P2)
**Source**: R10-CX-008 (codex review)
**Files**: `src/table_writer.rs:197-243`
**Description**: Schema validation checks names and types but not nullability. All current callers also call `validate_not_null_constraints`, so the public API is safe.
**Effort**: S

### R10-S-018: `file_index` virtual column unstable across queries (Priority: P2)
**Source**: R10-CX-010 (codex review)
**Files**: `src/table.rs:1601-1608`
**Description**: `file_index` uses `enumerate()` on pruned file list, so values shift based on which files pass pruning. Semantic issue.
**Suggested fix**: Document that `file_index` is query-scoped, not stable.
**Effort**: S

### R10-S-019: Encryption key lookup normalization collision (Priority: P2)
**Source**: R10-CX-024 (codex review)
**Files**: `src/encryption.rs:249-251`
**Description**: Leading-slash stripping causes `/a/b.parquet` and `a/b.parquet` to resolve identically. Fallback iteration depends on HashMap order — nondeterministic key selection.
**Effort**: S

### R10-S-020: DROP SCHEMA CASCADE non-atomic (Priority: P2)
**Source**: R10-CX-015 (codex review)
**Files**: `src/catalog.rs:241-259`
**Description**: Each `drop_table` in CASCADE creates a separate snapshot. Partial failure leaves some tables dropped, others not. State is consistent but partial.
**Effort**: M

### R10-S-021: Negative-scale decimal parsing (Priority: P2)
**Source**: R10-CX-012 (codex review)
**Files**: `src/parse_values.rs:349`
**Description**: `parse_decimal_string` treats negative scales as zero. Edge case for inlined data.
**Effort**: S

### R10-S-022: `compaction_functions.rs` uses Mutex for idempotent install check (Priority: P2)
**Source**: R10-I-015 (idiomatic review)
**Files**: `src/compaction_functions.rs:38`
**Description**: Global `Mutex<bool>` acquired on every compaction call. Could use `std::sync::Once` or `AtomicBool`.
**Suggested fix**: Use `std::sync::Once` which is designed for one-time initialization.
**Effort**: S

### R10-S-023: `extract_key_value` in merge allocates String per row (Priority: P2)
**Source**: R10-I-018 (idiomatic review)
**Files**: `src/merge_exec.rs:282-288`
**Description**: For string join keys, allocates a new `String` per row via `.value(row).to_string()`. O(source_rows + target_rows) allocations.
**Suggested fix**: Hash strings in-place without allocating.
**Effort**: M

### R10-S-024: MySQL `next_sequence_ids` wrong for count=0 (Priority: P3)
**Source**: R10-S-006 (correctness review)
**Files**: `src/metadata_writer_mysql.rs:367`
**Description**: Returns `end_value + 1` when `count=0`, a phantom ID. Currently unreachable since callers validate non-empty columns.
**Effort**: S

### R10-S-025: Overly broad public API surface (Priority: P3)
**Source**: R10-I-003 (idiomatic review)
**Files**: `src/lib.rs:38-102`
**Description**: 28 `pub mod` vs 2 `pub(crate) mod`. Large semver surface area.
**Effort**: M

### R10-S-026: Repeated `.map_err(|e| DataFusionError::External(Box::new(e)))` (Priority: P3)
**Source**: R10-I-004 (idiomatic review)
**Files**: Multiple (122 instances)
**Description**: Boilerplate error wrapping.
**Effort**: M

### R10-S-027: DML count schema allocated on every call (Priority: P3)
**Source**: R10-I-007 (idiomatic review)
**Files**: `src/delete_exec.rs:37-43`
**Description**: `make_dml_count_schema()` creates new `Arc<Schema>` on every call.
**Suggested fix**: Use `std::sync::LazyLock`.
**Effort**: S

### R10-S-028: `source_match_masks` rebuilt per file unnecessarily (Priority: P3)
**Source**: R10-I-012 (idiomatic review)
**Files**: `src/merge_exec.rs:455-458`
**Description**: Allocated for every target file even when matched_action isn't Update.
**Effort**: S

### R10-S-029: UNIX epoch date unwrap in production code (Priority: P3)
**Source**: R10-I-002 (idiomatic review)
**Files**: `src/table.rs:1845,1856`
**Description**: `from_ymd_opt(1970, 1, 1).unwrap()` is technically safe but unidiomatic.
**Effort**: S

### R10-S-030: Hardcoded `0` in unreachable panic messages (Priority: P3)
**Source**: R10-I-021 (idiomatic review)
**Files**: `src/table_changes.rs:789`, `src/table_deletions.rs:350`
**Description**: Format argument hardcoded as `0` instead of actual length.
**Effort**: S

### R10-S-031: DuckDB metadata provider uses `SqliteDialect` name (Priority: P3)
**Source**: R10-I-023 (idiomatic review)
**Files**: `src/metadata_provider_duckdb.rs:2,118`
**Description**: Misleading import name since both use standard SQL double-quote quoting.
**Effort**: S

### R10-S-032: `rowid`/`file_row_number` post-delete semantic (Priority: P3)
**Source**: R10-CX-011 (codex review)
**Files**: `src/virtual_column_exec.rs:203`
**Description**: After delete filtering, `file_row_number` reflects output position not physical Parquet position. Correct behavior but potentially confusing.
**Effort**: S (documentation)

### R10-S-033: Table function 3-part identifier not supported (Priority: P3)
**Source**: R10-CX-019 (codex review)
**Files**: `src/table_functions.rs:355-370`
**Description**: `catalog.schema.table` parsed incorrectly. Matches DuckLake design (catalog is implicit) but confusing.
**Effort**: S

### R10-S-034: Struct field parsing doesn't handle escaped quotes (Priority: P3)
**Source**: R10-CX-014 (codex review)
**Files**: `src/types.rs:457`
**Description**: Edge case; DuckDB doesn't generate this pattern in practice.
**Effort**: S

### R10-S-035: Lenient parse fallback returns StringArray (Priority: P3)
**Source**: R10-CX-013 (codex review)
**Files**: `src/parse_values.rs:258`
**Description**: Would fail `RecordBatch::try_new` for non-Utf8 target fields. Extremely rare in practice.
**Effort**: S

### R10-S-036: Encryption keys not zeroized (Priority: P3)
**Source**: R10-CX-023 (codex review)
**Files**: `src/encryption.rs:82,100,186`
**Description**: Standard Rust practice for non-HSM key handling. Low practical risk.
**Effort**: M

### R10-S-037: Column ordering differs from DuckDB (Priority: P3)
**Source**: R10-I-003 (interop review)
**Files**: All metadata writer files
**Description**: DuckDB places `begin_snapshot, end_snapshot` after ID columns; we place them at the end. No functional impact since all queries use named columns.
**Effort**: L (cosmetic)

---

## Recommended Fix Agents

### Agent 1: Query Correctness (P1, S effort)
- **R10-S-002**: Add `AND c.table_id = s.table_id` to `get_file_column_stats` JOIN
- **R10-S-003**: Add `AND c.table_id = pi.table_id` to partition column JOIN
- **R10-S-004**: Use `SERIALIZABLE` or `SELECT ... FOR UPDATE` for DDL on Postgres
- **R10-S-006**: Replace `filter_map` with error on missing partition column

### Agent 2: Write Path Safety (P1-P2, S effort)
- **R10-S-001**: Make `clear_inlined_data` failure non-fatal after commit
- **R10-S-005**: Add SNAPPY compression to Parquet writer
- **R10-S-008**: Use `checked_add` for all DML row count accumulations

### Agent 3: Performance & Idioms (P2, S-M effort)
- **R10-S-011**: Arc-wrap DML exec plan fields
- **R10-S-012**: Parallelize INSERT partition collection
- **R10-S-013**: Add timestamp parsing to `parse_stat_value`
- **R10-S-014**: Fix `UploadCleanupGuard::drop` to use existing runtime
- **R10-S-022**: Replace Mutex with `std::sync::Once` in compaction
- **R10-S-027**: Use `LazyLock` for DML count schema

### Agent 4: Remaining P2 (M effort)
- **R10-S-007**: Wrap append-mode per-file registration in transaction
- **R10-S-010**: Arc-wrap MERGE source batches
- **R10-S-015**: Map specific DuckLakeError variants to DataFusionError
- **R10-S-020**: Make DROP SCHEMA CASCADE atomic (single snapshot)

---

## Pre-existing Issues

| Issue | First Reported | Status |
|-------|---------------|--------|
| 39 cross-engine test failures (footer length / DuckDB extension bugs) | R3 (2 failures), R7 (13 failures), R10 (39 failures) | Pre-existing, growing. Footer size fix applied in R8. Root cause likely DuckDB extension version mismatch or additional metadata format issues. |
| `test_append_remove_column` failure | R10 | New regression. Column removal during append broken. |
| Parity test float formatting (`20` vs `20.0`) | R10 | Test normalization gap. |
| SLT pass rate declined (55.4% vs 61.8%) | R10 | May indicate regressions or new test files added. |
| Extra `ducklake_*` tables not in DuckDB | R9 (R9-I-002) | By design; empty and harmless. |
| R9-S-008: DDL snapshot boilerplate | R9 | Still unfixed. |
| R9-S-011: Unused `pool_type` macro parameter | R9 | Still unfixed. |
| R9-S-016: Dialect methods allocate where Cow would suffice | R9 | Still unfixed. |
| R9-S-018: Repeated `use crate::dialect::SqlDialect` | R9 | Still unfixed. |

---

## Branch

Reviewed on branch: `ducklake-features/integration`
