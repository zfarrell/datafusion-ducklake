# Code Review Synthesis — 2026-03-03 R4

## Overview
- Reviews: 5 (idiomatic, correctness, interop, test-harness, codex)
- Raw findings: 74
- After deduplication: 46
- Already tracked from R3: 2 (R3F-046, R3F-035)
- Related to R2 deferred (F-044): 2 (not counted as new)
- By priority: **1 P0, 12 P1, 20 P2, 13 P3**
- **Resolution: 44 of 46 FIXED, 2 DEFERRED**
  - Fixed by 8 agents across 8 commits
  - Deferred: R4-S-036 (map_err boilerplate, relates to R2 F-044), R4-S-040 (monolithic execute blocks, relates to R2 F-044)

## Deduplication Notes

### Key Merges (22 raw findings collapsed into 10)
- **C-002 + IO-003** → R4-S-005 (DML column stats — found by correctness + interop)
- **IO-001 + IO-002** → R4-S-008 (non-standard snapshot_changes tokens — UPDATE + MERGE same pattern)
- **I-001 + I-012 + C-007 + I-007** → R4-S-021 (residual `as` casts in table_deletions, delete_filter, table_functions)
- **C-006 + CX-006** → R4-S-016 (record_snapshot_changes failure handling)
- **IO-005 + IO-006** → R4-S-023 (inlined data table type + naming convention)
- **IO-007 + IO-008 + IO-010** → R4-S-024 (data/delete file naming + field_ids)
- **CX-042 + I-003** → R4-S-020 (silent error swallowing in providers/table init)
- **TH-002 + TH-003** → R4-S-028 (virtual column filtering in test helpers)
- **TH-006 + TH-007 + TH-008** → R4-S-029 (Decimal128/float formatting divergence)
- **TH-010 + CX-080 + CX-081 + CX-082** → R4-S-031 (test assertion gaps)

### P0 Claims Assessed (3 claimed → 1 validated)
- **R4-CX-001** (inline data loss on flush failure): **VALIDATED P0.** Confirmed `clear_inlined_data()` called at `table_writer.rs:329-330` and `:422-424` BEFORE `write_parquet_with_setup`. If write/upload fails, inlined rows are permanently lost.
- **R4-CX-002** (write/read path directory mismatch): **DOWNGRADED to P1.** Confirmed `write_parquet_with_setup` uses `t{table_id}/` (line 457) while `create_table` stores `table_name/` (metadata_writer_sqlite.rs:444). However, this ONLY affects the inline data flush paths — the normal streaming write path (`begin_write`, line 83) correctly uses `table_name`. Tests pass because inline data is read from catalog DB, not Parquet. Real bug but narrower scope than claimed.
- **R4-CX-003** (non-atomic partitioned commit): **DOWNGRADED to P1.** `commit_uploaded_files` calls `end_table_files` then iterates `register_data_file` calls without a wrapping transaction. Valid concern, but requires a mid-loop failure to a local database (SQLite), which is extremely unlikely after the first `register_data_file` succeeds. Not a P0 data corruption scenario.

### R3 Duplicates (already tracked)
- **R4-TH-009** (WITH...INSERT routing) = R3F-046 — still open from R3
- **R4-CX-063** (with_new_children extra children) = R3F-035 — still open from R3
- **R4-I-014** (four-way MetadataProvider duplication) = R2 deferred F-044 theme
- **R4-I-015** (verbose error wrapping) = related to I-004, folds into F-044 theme

### Informational (not counted)
- **R4-IO-011** (DDL column ordering differs) — no functional impact
- **R4-TH-004** (df_query_all documentation) — needs doc comment, not a bug
- **R4-CX-021** (create_view schema_id validation) — folded into R4-S-014

---

## Deduplicated Findings

### P0 — Critical (1)

#### R4-S-001: Inline data cleared before durable Parquet replacement **[FIXED]** (`54d3739`, fix-dml-metadata)
- **Source**: codex (CX-001)
- **Files**: `table_writer.rs:329-330` (threshold path), `table_writer.rs:422-424` (flush path)
- **Description**: Both `flush_inlined_data` and the inline-threshold overflow path call `clear_inlined_data()` BEFORE `write_parquet_with_setup()`. If the Parquet upload or metadata registration fails after clearing, the inlined rows are permanently lost — the old data is gone and no replacement was committed.
- **Impact**: Data loss on any write failure in the inline flush path.
- **Fix**: Move `clear_inlined_data()` to after successful `write_parquet_with_setup` return, or wrap clear+write in compensating logic that restores inlined data on failure.
- **Effort**: S

---

### P1 — High (12)

#### R4-S-002: Inline flush writes to `t{table_id}/` but table path is `table_name/` **[FIXED]** (`54d3739`, fix-dml-metadata)
- **Source**: codex (CX-002, downgraded from P0)
- **Files**: `table_writer.rs:454-457` (write path), `metadata_writer_sqlite.rs:444` (table path)
- **Description**: `write_parquet_with_setup` uploads files to `<base>/<schema>/t<table_id>/<uuid>.parquet` and registers only `<uuid>.parquet` as relative. But the stored table path is `<table_name>/`. Readers resolve relative to `table_name/`, so flushed inline Parquet files are unreachable.
- **Impact**: Inline data flushed to Parquet becomes invisible to readers.
- **Fix**: Upload to `<schema_name>/<table_name>/` (matching stored table path) or store `t{table_id}/` as the table path.
- **Effort**: S

#### R4-S-003: Partitioned commit non-atomic **[FIXED]** (fix-atomicity)
- **Source**: codex (CX-003, downgraded from P0)
- **Files**: `table_writer.rs:570-622`
- **Description**: `commit_uploaded_files` in Replace mode calls `end_table_files()` first, then iterates `register_data_file` per file without a wrapping transaction. Mid-loop failure leaves old files ended but not all new files registered.
- **Impact**: Partial metadata state on failure (unlikely with local DB but possible with PG/MySQL).
- **Fix**: Wrap end+register loop in single metadata transaction, or implement compensating rollback.
- **Effort**: M

#### R4-S-004: `register_dml_files` doesn't update snapshot `next_file_id` **[FIXED]** (`54d3739`, fix-dml-metadata)
- **Source**: correctness (C-001)
- **Files**: `metadata_writer_sqlite.rs:1151-1254`, `metadata_writer_postgres.rs:997-1058`, `metadata_writer_mysql.rs:1099-1160`
- **Description**: DML paths insert files but never update the snapshot's `next_file_id`. The INSERT path (`write_setup()`) does this correctly. Stale `next_file_id` causes ID collisions when DuckDB subsequently creates files.
- **Impact**: ID collisions in mixed DF+DuckDB write catalogs.
- **Fix**: Add `UPDATE ducklake_snapshot SET next_file_id = ...` at end of `register_dml_files` in all backends.
- **Effort**: S

#### R4-S-005: DML data files missing `register_column_stats` **[FIXED]** (`54d3739`, fix-dml-metadata)
- **Source**: correctness (C-002), interop (IO-003)
- **Files**: `metadata_writer_sqlite.rs:1193-1249`, `update_exec.rs:504-513`, `merge_exec.rs:608-617`
- **Description**: UPDATE/MERGE create new data files via `register_dml_files` but never call `register_column_stats`. The INSERT path (`table_writer.rs:502`) does this correctly. Missing per-file stats in `ducklake_file_column_stats` and stale aggregate stats in `ducklake_table_column_stats`.
- **Impact**: DuckDB row-group pruning and predicate pushdown degraded for DML-created files.
- **Fix**: Compute column stats in DML exec plans and pass to `register_dml_files`, then call `register_column_stats` per file.
- **Effort**: M

#### R4-S-006: PG/MySQL `register_dml_files` missing `row_id_start` and `table_stats` **[FIXED]** (`2a51319`, fix-pg-mysql)
- **Source**: correctness (C-003)
- **Files**: `metadata_writer_postgres.rs:1039-1053`, `metadata_writer_mysql.rs:1141-1155`
- **Description**: R3F-002 fixed this in SQLite but the fix was not ported to Postgres/MySQL. Those backends just `INSERT INTO ducklake_data_file` without `row_id_start` or `table_stats` updates.
- **Impact**: NULL `row_id_start` and stale stats for DML files in PG/MySQL catalogs.
- **Fix**: Port the R3F-002 fix from SQLite `register_dml_files` to PG and MySQL.
- **Effort**: S

#### R4-S-007: `end_table_files` (Replace mode) doesn't reset `table_stats` **[FIXED]** (`54d3739`, fix-dml-metadata)
- **Source**: correctness (C-005)
- **Files**: `metadata_writer_sqlite.rs:1134-1149`
- **Description**: Replace mode ends all existing data files but doesn't reset `ducklake_table_stats`. The INSERT path additively updates `record_count`, `next_row_id`, `file_size_bytes` on top of stale values. Also doesn't end associated delete files or clean up orphaned column stats.
- **Impact**: Replace-mode INSERT produces inflated record_count (e.g., 1000+500=1500 instead of 500).
- **Fix**: Reset `table_stats` to 0, end active delete files, optionally clean up file column stats.
- **Effort**: S

#### R4-S-008: UPDATE/MERGE `snapshot_changes` use non-standard tokens **[FIXED]** (`d567931`, fix-interop-format)
- **Source**: interop (IO-001, IO-002)
- **Files**: `update_exec.rs:517`, `merge_exec.rs:621`
- **Description**: UPDATE records `updated_table:{id}`, MERGE records `merged_into_table:{id}`. DuckDB uses `inserted_into_table:{id},deleted_from_table:{id}` for both — UPDATE/MERGE are inferred from the combination. Our non-standard tokens break DuckDB's `ducklake_table_changes()`.
- **Impact**: CDC tracking broken for DF-written UPDATE/MERGE snapshots when read by DuckDB.
- **Fix**: Use `inserted_into_table:{id},deleted_from_table:{id}` for UPDATE. For MERGE, use the applicable combination based on operations performed.
- **Effort**: S

#### R4-S-009: Delete file `file_path` column uses relative catalog path **[FIXED]** (`d567931`, fix-interop-format)
- **Source**: interop (IO-004)
- **Files**: `delete_exec.rs:336-337`, `update_exec.rs:400`, `merge_exec.rs:477`
- **Description**: DuckDB populates the delete file's `file_path` with the fully resolved path from `data_path` root. Our code uses the raw catalog-relative path (just `uuid.parquet`).
- **Impact**: Tools reading delete files by `file_path` will fail to match. Breaks format contract for Iceberg compatibility layer.
- **Fix**: Resolve full path (data_path + schema_path + table_path + file.path) before writing to delete file.
- **Effort**: S

#### R4-S-010: NULL filter predicate incorrectly treated as match in DELETE/UPDATE **[FIXED]** (`39fea14`, fix-dml-correctness)
- **Source**: codex (CX-005)
- **Files**: `delete_exec.rs:290-293`, `update_exec.rs:327-330`
- **Description**: `mask.value(i)` reads the value buffer ignoring the null bitmap. For SQL WHERE semantics, NULL should be treated as non-match (false). A null filter result can match rows for deletion/update.
- **Impact**: Rows matching NULL conditions incorrectly deleted/updated.
- **Fix**: Replace `mask.value(i)` with `mask.value(i) && !mask.is_null(i)`.
- **Effort**: S

#### R4-S-011: UPDATE/MERGE skip NOT NULL constraint validation **[FIXED]** (`39fea14`, fix-dml-correctness)
- **Source**: codex (CX-007)
- **Files**: `update_exec.rs:359-376`, `merge_exec.rs:518-538,543-574`
- **Description**: INSERT validates non-nullable columns before writing. UPDATE and MERGE write transformed/source rows without equivalent checks.
- **Impact**: NULLs can be written into non-nullable columns via UPDATE/MERGE.
- **Fix**: Extract NOT NULL validation from INSERT into shared helper; call before writing in UPDATE/MERGE.
- **Effort**: S

#### R4-S-012: LIMIT pushed into Parquet scan before DeleteFilterExec **[FIXED]** (`39fea14`, fix-dml-correctness)
- **Source**: codex (CX-041)
- **Files**: `table.rs:830,1435`
- **Description**: `scan()` passes `limit` into `FileScanConfigBuilder::with_limit()` for files that have delete files. Parquet reader stops after N rows, then `DeleteFilterExec` filters some, yielding fewer than N.
- **Impact**: `SELECT ... LIMIT N` on tables with deletes returns fewer rows than requested.
- **Fix**: Only push `limit` into Parquet scan for files WITHOUT delete files; omit limit for files with deletes.
- **Effort**: S

#### R4-S-013: `record_count` never decremented after DELETE **[FIXED]** (`54d3739`, fix-dml-metadata)
- **Source**: correctness (C-004)
- **Files**: `metadata_writer_sqlite.rs:1164-1191`, `delete_exec.rs:386-401`
- **Description**: `register_dml_files` only increments `record_count` when data files are added. DELETE operations never adjust it. After DELETE, `record_count` overstates live rows.
- **Impact**: Inflated cardinality estimates → suboptimal query plans (wrong join strategies, overallocated memory).
- **Fix**: When processing delete files, compute net new deletions and subtract from `record_count`.
- **Effort**: S

---

### P2 — Medium (20)

#### R4-S-014: Drop/create operations missing existence validation **[FIXED]** (fix-atomicity)
- **Source**: codex (CX-020, CX-021)
- **Files**: All `metadata_writer_*.rs` — `drop_view`, `drop_table_inner`, `drop_schema_inner`, `create_view`
- **Description**: Drop operations create snapshots without checking the target is active. `create_view` doesn't validate schema_id is active. Dropping non-existent/already-dropped objects creates spurious snapshots.
- **Fix**: Add existence/active checks before creating drop snapshots.
- **Effort**: S

#### R4-S-015: `rename_table`/`rename_view` allow duplicate active names **[FIXED]** (fix-atomicity)
- **Source**: codex (CX-022)
- **Files**: All `metadata_writer_*.rs` — rename methods
- **Description**: No check for existing active object with same `(schema_id, new_name)`. Creates ambiguous lookups in `find_table_id`.
- **Fix**: Check name collision before rename; return error if exists.
- **Effort**: S

#### R4-S-016: `record_snapshot_changes` failure after successful DML commit **[FIXED]** (fix-atomicity)
- **Source**: correctness (C-006), codex (CX-006)
- **Files**: `delete_exec.rs:396-401`, `update_exec.rs:515-518`, `merge_exec.rs:619-622`
- **Description**: DML returns error if `record_snapshot_changes` fails even though metadata is committed. User sees failure but data was persisted. Retry could duplicate effects.
- **Fix**: Make `record_snapshot_changes` non-fatal (log warning) or move into `register_dml_files` transaction.
- **Effort**: S

#### R4-S-017: `finish()` cleanup deletes already-committed metadata file **[FIXED]** (fix-atomicity)
- **Source**: codex (CX-004)
- **Files**: `table_writer.rs:831-843,870-877`
- **Description**: If `register_column_stats` fails after `register_data_file` succeeds, the error path deletes the uploaded Parquet file. But metadata already references it.
- **Fix**: Track commit stage; skip file deletion if metadata already references the file.
- **Effort**: S

#### R4-S-018: Checked write transactions TOCTOU in PG/MySQL **[FIXED]** (fix-atomicity)
- **Source**: codex (CX-023)
- **Files**: `metadata_writer_postgres.rs:1259-1375`, `metadata_writer_mysql.rs:1379-1495`
- **Description**: Conflict detection uses `SELECT COUNT(*)` without `FOR UPDATE` or serializable isolation.
- **Fix**: Use `SELECT ... FOR UPDATE` or serializable isolation for conflict checks.
- **Effort**: S

#### R4-S-019: `list_all_columns` violates snapshot isolation **[FIXED]** (`d294651`, fix-quality)
- **Source**: codex (CX-040)
- **Files**: All `metadata_provider_*.rs` — `list_all_columns` query
- **Description**: Filters with `c.end_snapshot IS NULL` instead of proper snapshot-window predicates.
- **Fix**: Use `begin_snapshot <= snapshot_id AND (end_snapshot > snapshot_id OR end_snapshot IS NULL)`.
- **Effort**: S

#### R4-S-020: Provider errors silently swallowed **[FIXED]** (`d294651`, fix-quality)
- **Source**: codex (CX-042), idiomatic (I-003)
- **Files**: `table.rs:220,235`, `metadata_provider_duckdb.rs:598-600`
- **Description**: `unwrap_or_default()` on partition/inlined data loading; `.filter_map(|r| r.ok())` drops deserialization errors silently.
- **Fix**: Propagate errors with `?` or at minimum log warnings.
- **Effort**: S

#### R4-S-021: Residual unsafe `as` casts in non-DML files **[FIXED]** (`d294651`, fix-quality)
- **Source**: idiomatic (I-001, I-012), correctness (C-007), idiomatic (I-007)
- **Files**: `table_deletions.rs:762-784`, `delete_filter.rs:170-189`, `table_functions.rs:377-390`
- **Description**: R3F-010 fixed `as` casts in DML execs, but table_deletions, delete_filter, and table_functions still use bare `as` casts. Inconsistent with project pattern of `try_from`/`i64::from`.
- **Fix**: Replace with `i64::from()` for widening, `try_from().unwrap()` for narrowing after guards.
- **Effort**: S

#### R4-S-022: Struct field names not escaped in `arrow_to_ducklake_type` **[FIXED]** (`d294651`, fix-quality)
- **Source**: codex (CX-061)
- **Files**: `types.rs:185-188`
- **Description**: Struct field names emitted unquoted. Names with spaces, commas, or colons produce invalid type strings.
- **Fix**: Quote/escape field names containing special characters.
- **Effort**: S

#### R4-S-023: Inlined data table uses TEXT for all columns and non-standard naming **[FIXED]** (`fbeef2e`, fix-interop-conventions)
- **Source**: interop (IO-005, IO-006)
- **Files**: `metadata_writer_sqlite.rs:2432,2450-2452`
- **Description**: Our inlined data tables use `TEXT` for all user columns (DuckDB uses original types) and name them `ducklake_inlined_data_{table_id}` (DuckDB uses `_{table_id}_{schema_version}`).
- **Fix**: Use `arrow_to_ducklake_type()` for column types; append `_{schema_version}` to table name.
- **Effort**: S

#### R4-S-024: Data/delete file naming and field_id divergence from DuckDB **[FIXED]** (`fbeef2e`, fix-interop-conventions)
- **Source**: interop (IO-007, IO-008, IO-010)
- **Files**: `table_writer.rs:84`, `table.rs:82-86`, DML execs
- **Description**: Data files use `{uuid}.parquet` (DuckDB: `ducklake-{uuid}.parquet`). Delete file schema has no Parquet field_ids (DuckDB uses sentinel values 0x7FFFFFFE/0x7FFFFFFD).
- **Fix**: Add `ducklake-` prefix to data files; add field_id metadata to delete file schema fields.
- **Effort**: S

#### R4-S-025: `parse_decimal` accepts malformed input **[FIXED]** (`d294651`, fix-quality)
- **Source**: codex (CX-060)
- **Files**: `types.rs:264-267`
- **Description**: `decimal(10` or `numeric(12,2` (missing closing paren) silently falls back to Decimal128(18,0).
- **Fix**: Require closing parenthesis; return error for malformed decimal type strings.
- **Effort**: S

#### R4-S-026: Projection index bounds not checked in CDC functions **[FIXED]** (`d294651`, fix-quality)
- **Source**: codex (CX-062)
- **Files**: `table_changes.rs:417-421`, `table_deletions.rs:140-143`
- **Description**: Projection indices index into `self.output_schema.field(idx)` without bounds checking. Invalid index causes panic.
- **Fix**: Use `get()` or bounds-check; return `DataFusionError::Internal` for out-of-range.
- **Effort**: S

#### R4-S-027: CDC projection analysis duplicated **[FIXED]** (`fbeef2e`, fix-interop-conventions)
- **Source**: idiomatic (I-005)
- **Files**: `table_changes.rs:385-468`, `table_deletions.rs:111-188`
- **Description**: Nearly identical ~80-line `analyze_projection` methods. Bug fix in one may not be applied to the other.
- **Fix**: Extract shared `CdcProjectionAnalysis` into a common module.
- **Effort**: S

#### R4-S-028: Virtual column filtering inconsistency in test helpers **[FIXED]** (`11e4084`, fix-tests)
- **Source**: test-harness (TH-002, TH-003)
- **Files**: `tests/merge_tests.rs:100-103`, `tests/cross_engine_alter_tests.rs:214`
- **Description**: `merge_tests` uses `batches_to_strings` (unfiltered); `alter_tests` has local `df_query` that doesn't strip virtual columns. Both risk false passes or hidden failures.
- **Fix**: Use shared `batches_to_strings_filtered` / `df_query` from `test_utils`.
- **Effort**: S

#### R4-S-029: Decimal128/float formatting divergence between test paths **[FIXED]** (`11e4084`, fix-tests)
- **Source**: test-harness (TH-006, TH-007, TH-008)
- **Files**: `tests/hybrid_asyncdb.rs:479-498,599-607`, `tests/common/test_utils.rs:24-25,167-180`
- **Description**: Two Decimal128 formatters (f64 division vs integer arithmetic) and inconsistent float `.0` suffix handling. Can cause spurious SLT failures or false passes.
- **Fix**: Unify on integer-arithmetic Decimal128 and consistent float formatting.
- **Effort**: S

#### R4-S-030: `DuckDbConn` struct duplicated across 10 test files **[FIXED]** (`11e4084`, fix-tests)
- **Source**: test-harness (TH-001)
- **Files**: 10 test files (cross_engine_*.rs, merge_tests.rs, etc.)
- **Description**: Identical wrapper struct copy-pasted. Changes must be replicated 10 times.
- **Fix**: Move to `tests/common/mod.rs` and import.
- **Effort**: S

#### R4-S-031: Test assertion gaps **[FIXED]** (`11e4084`, fix-tests)
- **Source**: test-harness (TH-010), codex (CX-080, CX-081, CX-082)
- **Files**: `tests/adversarial_catalog_tests.rs`, `tests/hybrid_asyncdb.rs:420`, `tests/common/test_utils.rs:279`, `tests/sqllogictest_runner.rs:84`
- **Description**: Adversarial tests discard results (tautological); transaction-path decoding swallows errors as NULL; `normalize_value` collapses distinct numbers; SLT tests can pass with zero statements.
- **Fix**: Add proper assertions, error propagation, minimum statement counts.
- **Effort**: M

#### R4-S-032: No unit tests for `is_write_statement` routing function **[FIXED]** (`11e4084`, fix-tests)
- **Source**: test-harness (TH-012)
- **Files**: `tests/hybrid_asyncdb.rs:117`
- **Description**: Critical routing function with no unit tests. Edge cases untested.
- **Fix**: Add `#[cfg(test)]` module with cases for mixed case, leading whitespace, comments, CTEs.
- **Effort**: S

#### R4-S-033: `convert_batch_to_strings` missing types fall through silently **[FIXED]** (`11e4084`, fix-tests)
- **Source**: test-harness (TH-013)
- **Files**: `tests/hybrid_asyncdb.rs:612-615`
- **Description**: Unrecognized types produce `?...?` placeholder output instead of an error.
- **Fix**: Replace catch-all with panic or error for unsupported types.
- **Effort**: S

---

### P3 — Low (13)

#### R4-S-034: Lossy i64→f64 cast in compaction_functions **[FIXED]** (`d294651`, fix-quality)
- **Source**: idiomatic (I-002)
- **Files**: `compaction_functions.rs:482`
- **Fix**: Add range guard for values above 2^53.

#### R4-S-035: Unchecked i64 overflow in virtual_column_exec **[FIXED]** (`d294651`, fix-quality)
- **Source**: codex (CX-064)
- **Files**: `virtual_column_exec.rs:230-232,260-261`
- **Fix**: Use `checked_add()`.

#### R4-S-036: Pervasive `.map_err(DataFusionError::External(Box::new(e)))` boilerplate **[DEFERRED]** (50+ sites, relates to R2 F-044 provider/writer dedup)
- **Source**: idiomatic (I-004)
- **Files**: 50+ occurrences across DML execs, table_writer, compaction, etc.
- **Fix**: Add `IntoDataFusionExternal` helper trait in `error.rs`.

#### R4-S-037: Fragile `bind_repeat!` macro **[FIXED]** (`d294651`, fix-quality)
- **Source**: idiomatic (I-006)
- **Files**: `metadata_provider_postgres.rs:16-49`
- **Fix**: Replace with a simple loop function.

#### R4-S-038: Row collection boilerplate in compaction_functions **[FIXED]** (`11e4084`, fix-tests)
- **Source**: idiomatic (I-008)
- **Files**: `compaction_functions.rs:296-343,549-579`
- **Fix**: Use struct-based row collection.

#### R4-S-039: Path normalization duplication in encryption.rs **[FIXED]** (`d294651`, fix-quality)
- **Source**: idiomatic (I-009)
- **Files**: `encryption.rs:253-270`
- **Fix**: Use path_resolver for canonical normalization.

#### R4-S-040: Monolithic 200+ line execute() async blocks **[DEFERRED]** (relates to R2 F-044 provider/writer dedup — L effort architectural refactor)
- **Source**: idiomatic (I-010)
- **Files**: `delete_exec.rs:193-406`, `insert_exec.rs`, `merge_exec.rs`, `update_exec.rs`
- **Fix**: Extract logical phases into helper functions.

#### R4-S-041: Inconsistent `Arc::clone` vs `.clone()` usage **[FIXED]** (`d294651`, fix-quality)
- **Source**: idiomatic (I-011)
- **Files**: Various — `delete_exec.rs`, `table.rs`, `schema.rs`, etc.
- **Fix**: Standardize on `Arc::clone(&x)`.

#### R4-S-042: Missing `#[must_use]` on builder patterns **[FIXED]** (`d294651`, fix-quality)
- **Source**: idiomatic (I-013)
- **Files**: `metadata_writer.rs` (builder structs), `table.rs`
- **Fix**: Add `#[must_use]` to builder methods returning Self.

#### R4-S-043: SQLite `schedule_start` uses TEXT type **[FIXED]** (`fbeef2e`, fix-interop-conventions)
- **Source**: interop (IO-009)
- **Files**: `metadata_writer_sqlite.rs:213`
- **Fix**: Use consistent ISO 8601 timestamp format.

#### R4-S-044: SLT infrastructure fragilities **[FIXED]** (`11e4084`, fix-tests)
- **Source**: test-harness (TH-005, TH-011, TH-014, TH-015, TH-016)
- **Files**: `tests/hybrid_asyncdb.rs`, `tests/sqllogictest_runner.rs`
- **Description**: Grouped: unwrap on downcast_ref (TH-005), prefix-only error matching (TH-011), fragile ORDER BY ALL rewriting (TH-014), regex over-match in string literals (TH-015), no negative tests for loop expansion (TH-016).
- **Fix**: Improve error messages, document limitations, add bounds checking.

#### R4-S-045: `assert_results_eq` doesn't show differing rows on count mismatch **[FIXED]** (`11e4084`, fix-tests)
- **Source**: test-harness (TH-017)
- **Files**: `tests/common/test_utils.rs`
- **Fix**: Print first few rows on row-count mismatch.

#### R4-S-046: Edge case handling in DuckDB provider **[FIXED]** (`d294651`, fix-quality)
- **Source**: codex (CX-043, CX-044)
- **Files**: `metadata_provider_duckdb.rs:100,591,620`, `table.rs:1298,1341`
- **Description**: Grouped: DuckDB inlined-data read doesn't check table existence (CX-043); negative `null_count` wraps to huge usize (CX-044).
- **Fix**: Check table existence; clamp negative values to 0.

---

## Fix Agents — Resolution

All 8 recommended agents were executed. **44 of 46 findings fixed.**

### Agent 1: fix-dml-metadata — DML Metadata Integrity (P0 + P1) ✓
- **Commit**: `54d3739`
- **Findings fixed**: R4-S-001, R4-S-002, R4-S-004, R4-S-005, R4-S-007, R4-S-013

### Agent 2: fix-dml-correctness — DML Correctness (P1) ✓
- **Commit**: `39fea14`
- **Findings fixed**: R4-S-010, R4-S-011, R4-S-012

### Agent 3: fix-interop-format — Interop Format Fixes (P1) ✓
- **Commit**: `d567931`
- **Findings fixed**: R4-S-008, R4-S-009

### Agent 4: fix-pg-mysql — PG/MySQL Fix Parity (P1) ✓
- **Commit**: `2a51319`
- **Findings fixed**: R4-S-006

### Agent 5: fix-atomicity — Atomicity + Validation (P1 + P2) ✓
- **Commit**: committed to worktree
- **Findings fixed**: R4-S-003, R4-S-014, R4-S-015, R4-S-016, R4-S-017, R4-S-018

### Agent 6: fix-quality — Code Quality + Safety (P2 + P3) ✓
- **Commit**: `d294651`
- **Findings fixed**: R4-S-019, R4-S-020, R4-S-021, R4-S-022, R4-S-025, R4-S-026, R4-S-034, R4-S-035, R4-S-037, R4-S-039, R4-S-041, R4-S-042, R4-S-046

### Agent 7: fix-interop-conventions — Interop Convention Alignment (P2 + P3) ✓
- **Commit**: `fbeef2e`
- **Findings fixed**: R4-S-023, R4-S-024, R4-S-027, R4-S-043

### Agent 8: fix-tests — Test Infrastructure (P2 + P3) ✓
- **Commit**: `11e4084`
- **Findings fixed**: R4-S-028, R4-S-029, R4-S-030, R4-S-031, R4-S-032, R4-S-033, R4-S-038, R4-S-044, R4-S-045

### Deferred (2 findings)
- **R4-S-036**: map_err boilerplate (50+ sites) — relates to R2 F-044 provider/writer dedup
- **R4-S-040**: Monolithic execute() blocks — relates to R2 F-044 architectural refactor

---

## Priority Summary

| Priority | Count | Fixed | Deferred | Key Themes |
|----------|-------|-------|----------|------------|
| P0 | 1 | 1 | 0 | Inline data loss on flush failure |
| P1 | 12 | 12 | 0 | DML metadata gaps (stats, IDs, counts), interop format divergence, NULL filter, LIMIT+delete, NOT NULL constraint |
| P2 | 20 | 20 | 0 | Atomicity gaps, validation gaps, snapshot isolation, error swallowing, type/naming convention, test infrastructure |
| P3 | 13 | 11 | 2 | Code quality (boilerplate, casts, style), SLT fragilities, edge cases |
| **Total** | **46** | **44** | **2** | |

## Cross-Cutting Observations

### 1. DML Metadata Path Continues to Diverge from INSERT
R3 fixed several INSERT/DML parity issues (row_id_start, snapshot_changes, MERGE cleanup), but the DML path still misses: column stats (R4-S-005), next_file_id (R4-S-004), record_count decrements (R4-S-013), Replace-mode table_stats reset (R4-S-007). The root cause is that `register_dml_files` is a simpler code path than the INSERT's `register_data_file` + `register_column_stats` + `write_setup` pipeline.

### 2. Inline Data Flush Path Has Multiple Issues
Three findings (R4-S-001, R4-S-002, and part of R4-S-005) affect the inline data flush path. The P0 (clearing before commit) and the path mismatch (t{table_id} vs table_name) are both in `write_parquet_with_setup`. These should be fixed together.

### 3. R3F-013 Fix Introduced New Interop Problems
The R3F-013 fix added snapshot_changes recording for DML, but used non-standard tokens (`updated_table`, `merged_into_table`). The correct format uses only `inserted_into_table` and `deleted_from_table` tokens (R4-S-008).

### 4. PG/MySQL Parity Gap Persists
R3F-006 ported R2 fixes to PG/MySQL, but the R3F-002 fix (row_id_start in DML) was not ported (R4-S-006). This is the same pattern as previous cycles — SQLite fixes not propagated to other backends.

### 5. Test Infrastructure Debt Growing
12 of 46 findings (26%) are test infrastructure issues. The DuckDbConn duplication, formatting divergence, and assertion gaps create maintenance risk and reduce confidence in test results.
