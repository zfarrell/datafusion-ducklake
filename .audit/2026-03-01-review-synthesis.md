# Review Synthesis: Consolidated Action Plan

**Date**: 2026-03-01
**Source reviews**: Idiomatic, Correctness, Interop, Test Harness
**Branch**: `ducklake-features/integration`

---

## Resolution Status (2026-03-02)

All P0 and P1 findings have been resolved. P2 items partially addressed. P3 items remain as backlog.

| Priority | Total | Fixed | Remaining |
|----------|-------|-------|-----------|
| P0 | 6 | 6 | 0 |
| P1 | 11 | 11 | 0 |
| P2 | 13 | 9 | 4 |
| P3 | 13 | 0 | 13 |

### P0 Fixes (all resolved)
- **P0-1, P0-2, P0-4** (Agent 1): Single `begin_write_transaction` for all partitions. Replace-mode defers old file ending until after upload. All-or-nothing commit with `cleanup_uploaded_files()` on failure.
- **P0-3, P0-5** (Agent 2): `clear_inlined_data` moved after successful Parquet write. Error propagation for inline reads.
- **P0-6** (Agent 3): `quote_identifier()` for SQL injection prevention.

### P1 Fixes (all resolved)
- **P1-1** (Agent 1): BTreeMap for deterministic partition ordering.
- **P1-2** (Agent 3): All 4 Arrow timestamp precisions supported.
- **P1-3** (Agent 3): Unknown transforms return error.
- **P1-4** (Agent 2): Flush path fixed to use `table_name/` not `t{table_id}/`.
- **P1-5** (Agent 3): Hive partition values URL-encoded.
- **P1-6** (Agent 3): Unsupported partition types return error.
- **P1-7** (Agent 1): Partition values registered atomically with file registration.
- **P1-8** (Agent 2): `InlinedDataRow.column_names` uses `Arc<Vec<String>>`.
- **P1-9** (Agent 4): `assert_results_eq` checks column counts before zip.
- **P1-10** (Agent 4): New `test_df_write_partitioned_duckdb_read()` test.
- **P1-11** (Agent 4): New `test_df_write_inlined_duckdb_read()` test.

### P2 Fixes (9 of 13 resolved)
- **P2-2** (Agent 4): Shared `tests/common/test_utils.rs` module.
- **P2-3** (Agent 4): Dead `rewrite_unqualified_tables()` removed.
- **P2-4** (Agent 4): Word-boundary matching for virtual column stripping.
- **P2-5** (Agent 4): SLT skip-count logging.
- **P2-6** (Agent 4): Sort masking fixed.
- **P2-7** (Agent 4): DuckDB skip warnings added.
- **P2-9** (Agent 3): `count_star()` rewriting uses word boundaries.
- **P2-11** (Agent 4): New `test_duckdb_partitioned_inlined_data()` combo test.
- **P2-12** (Agent 3): Date32/Date64 use ISO-8601 formatting.

### P2 Remaining (4 items)
- **P2-1**: Vectorized partition routing (performance optimization).
- **P2-8**: PG/MySQL inlining fallback.
- **P2-10**: Snapshot field population (`schema_version`, `next_catalog_id`, `next_file_id`).
- **P2-13**: UNIQUE constraints for PG/MySQL ID allocation.

### P3 Remaining (13 items — all backlog nits/style)
All P3 items (P3-1 through P3-13) remain unaddressed. See P3 table below for details.

---

## Executive Summary

Four independent reviews of the Tier 1 sprint code examined idiomatic Rust/DataFusion patterns, correctness/bugs, DuckLake interop compliance, and test infrastructure. A total of **57 raw findings** were identified across the four reviews. After deduplication (multiple reviews flagged the same underlying issues), **36 unique findings** remain.

**Key themes:**
1. **Write atomicity is broken**: Partitioned writes create independent snapshots per partition, leading to stale field IDs and partial commit risk. Replace-mode writes end old files before upload succeeds.
2. **Data loss on failure**: Inline data is cleared before Parquet write completes; errors during inline reads are silently swallowed.
3. **SQL injection surface**: Column names from `ColumnDef` are interpolated into SQL without sanitization.
4. **Test infrastructure gaps**: Duplicated helpers across 6+ files, `assert_results_eq` uses `zip` (false pass risk), no DF-write partition/inline interop tests.
5. **Silent data misrouting**: Unknown partition transforms and unsupported timestamp precisions silently produce NULL partitions.

---

## Deduplicated Findings

### P0: Fix Immediately (data corruption, data loss, security)

| ID | Finding | Source | Files | Suggested Fix | Effort |
|----|---------|--------|-------|---------------|--------|
| P0-1 | **Partitioned writes create independent snapshots, reassigning column IDs each time.** Each partition call to `begin_write_transaction` ends all existing columns and creates new ones. Only the last partition's column IDs are active. Parquet files from earlier partitions embed stale field IDs. Cross-engine reads produce incorrect column mapping. | Correctness C1 | `insert_exec.rs:610-636`, `table_writer.rs:101-124,149-191`, `metadata_writer_sqlite.rs:341-498` | Perform a single `begin_write_transaction` for the entire partitioned write. Share the returned setup (snapshot_id, column_ids) across all partition files. | L |
| P0-2 | **Replace-mode metadata commit before Parquet upload.** `begin_write_transaction(Replace)` commits immediately (ends all existing data files). If upload fails, old files are ended and new file never registered — table appears empty. | Correctness C2 | `table_writer.rs:149-191`, `metadata_writer_sqlite.rs:479-490` | Defer ending old files until after upload succeeds. Split `begin_write_transaction` or move file-end into `commit_metadata()`. | M |
| P0-3 | **Inline data lost when flush-to-Parquet fails.** Both threshold-exceeded and manual flush paths call `clear_inlined_data()` BEFORE writing Parquet. Upload failure permanently loses the inlined rows. | Correctness C3 | `table_writer.rs:313-328, 407-413` | Move `clear_inlined_data` to AFTER successful Parquet upload and metadata commit. | S |
| P0-4 | **Partitioned writes partially commit on failure.** Each partition independently creates a session, writes, and commits. Failure mid-way leaves committed partitions plus missing ones. With Replace mode, first partition already ended all old files. | Correctness C4 | `insert_exec.rs:610-652` | All partitions share a single snapshot/transaction. Register files only after ALL uploads succeed. | L |
| P0-5 | **Inline data read failure silently swallowed.** `if let Ok(inline_rows) = ...` discards errors, then proceeds to clear and write — existing inlined rows are lost. | Correctness C5 | `table_writer.rs:306-310` | Propagate the error: `let inline_rows = self.get_inlined_data_as_batch(...)?;` | S |
| P0-6 | **SQL injection via column name interpolation in SQLite inlining.** Column names from `ColumnDef` are interpolated directly into CREATE TABLE and INSERT SQL via `format!()`. A column name containing quotes or semicolons would break quoting or enable injection. | Idiomatic C-1, Correctness m6 | `metadata_writer_sqlite.rs:1826-1830, 1861-1870, 1896-1901, 1979-1983, 2020-2023` | Add `validate_sql_identifier()` helper. Validate column names before interpolation. | S |

### P1: Fix Soon (correctness bugs affecting users, interop breakage)

| ID | Finding | Source | Files | Suggested Fix | Effort |
|----|---------|--------|-------|---------------|--------|
| P1-1 | **Replace-mode with partitioned writes uses non-deterministic HashMap order.** The first partition gets `Replace` semantics (ending old files) while others get `Append`. Which partition is "first" depends on HashMap iteration order. | Idiomatic M-4 | `insert_exec.rs:595-653` | Sort partition keys or use `BTreeMap`. Better: decouple Replace (end old files) from the partition loop entirely. | S |
| P1-2 | **Timestamp partition transforms only support microsecond resolution.** `extract_temporal_component` only handles `TimestampMicrosecondArray`. Second/Millisecond/Nanosecond timestamps silently return `None`, routing all rows to `__HIVE_DEFAULT_PARTITION__`. | Idiomatic m-3, Correctness M1 | `insert_exec.rs:436-482` | Handle all four Arrow timestamp precisions. | S |
| P1-3 | **Unknown partition transforms silently produce NULL partition values.** Typo in transform (e.g., `"yer"` instead of `"year"`) routes all rows to default partition without error. | Correctness M2 | `insert_exec.rs:425` | Return an error for unrecognized transform strings. | S |
| P1-4 | **Inline flush writes to wrong directory (`t{table_id}/` vs `table_name/`).** `write_parquet_with_setup` constructs path as `<data_path>/<schema_name>/t<table_id>/<uuid>.parquet` but catalog registers only filename. Read path resolves against `<table_name>/` — file not found. | Idiomatic m-7, Correctness M3 | `table_writer.rs:439-443, 472-473` | Pass `table_name` to `write_parquet_with_setup` and use it for path construction. | S |
| P1-5 | **Hive partition values not URL-encoded.** Raw string interpolation for partition values. Values with `/`, `..`, `=` create malformed or directory-traversal paths. | Correctness M4 | `insert_exec.rs:494-496` | URL-encode partition values per Hive convention. | S |
| P1-6 | **`compute_partition_value` returns `None` for unsupported types (silent data misrouting).** Binary, Decimal128, etc. silently group all rows into `__HIVE_DEFAULT_PARTITION__`. | Idiomatic m-2 | `insert_exec.rs:293-427` | Return `Result<Option<String>>` and error for unsupported partition column types. | S |
| P1-7 | **Partition value registration non-atomic with file registration.** `register_file_partition_value` runs after `session.finish()`; failure leaves data file without partition values. | Correctness (Codex #3) | `insert_exec.rs` (post-finish path) | Move partition value registration inside the write transaction. | M |
| P1-8 | **`InlinedDataRow` clones `column_names` per row.** 10,000-row inline insert creates 10,000 identical `Vec<String>` allocations. | Idiomatic M-3 | `table_writer.rs:663-690` | Change to `Arc<Vec<String>>` or store column names once per batch. | S |
| P1-9 | **`assert_results_eq` uses `zip` without column-count check (false pass risk).** If one engine returns 3 columns and the other 4, `zip` silently truncates. Tests can pass despite missing columns. | Test Harness (Codex #1), Risk 6 | `cross_engine_postgres_tests.rs:289-290`, `cross_engine_mysql_tests.rs` | Assert `row_a.len() == row_b.len()` before zipping. | S |
| P1-10 | **No DF-write partitioned data interop test.** All 7 partition cross-engine tests only verify DuckDB-write, DF-read direction. DF-write partition metadata/paths may differ from DuckDB expectations. | Interop M1, Test Harness gap | `cross_engine_partition_tests.rs` | Add `test_df_write_partitioned_duckdb_read()`. | M |
| P1-11 | **No DF-write inlined data interop test.** DF creates SQLite inlined tables with TEXT-only columns. DuckDB read-back untested. | Interop M2 | `cross_engine_inline_tests.rs` | Add `test_df_write_inlined_duckdb_read()`. Verify TEXT-only columns work. | M |

### P2: Fix Next Sprint (performance, code quality, test coverage gaps)

| ID | Finding | Source | Files | Suggested Fix | Effort |
|----|---------|--------|-------|---------------|--------|
| P2-1 | **Row-by-row partition routing is O(rows x partitions), non-columnar.** `route_batches_to_partitions` iterates row-by-row calling `compute_partition_value()` per row per partition column. ~10x slower than vectorized approach for large inserts. | Idiomatic M-1 | `insert_exec.rs:506-531` | Use Arrow columnar operations (e.g., `arrow::compute::partition` or hash-group). | M |
| P2-2 | **Duplicated test helpers across 6+ files.** 5 near-identical "RecordBatch to Vec<Vec<String>>" implementations. PG and MySQL test files are ~95% identical. `duckdb_value_to_string()` varies — DML variant missing Date32, Timestamp, Decimal128 (uses Debug formatting). | Idiomatic M-2, Test Harness (duplication) | `write_partition_tests.rs`, `write_inline_tests.rs`, `cross_engine_postgres_tests.rs`, `cross_engine_mysql_tests.rs`, `hybrid_asyncdb.rs`, `cross_engine_dml_tests.rs` | Extract to `tests/common/test_utils.rs`. Use most comprehensive `duckdb_value_to_string()`. | M |
| P2-3 | **`rewrite_unqualified_tables()` is dead code (no-op).** Returns input unchanged. Creates false sense of having a safety net for table reference rewriting. | Test Harness (runner #1) | `sqllogictest_runner.rs:604-612` | Remove function and call sites. | S |
| P2-4 | **Virtual column stripping uses substring matching (false positive risk).** `sql_upper.contains(&name.to_uppercase())` can match partial words. E.g., SQL with `"filename"` in a string literal keeps the virtual column. | Test Harness #1 | `hybrid_asyncdb.rs:282-301` | Use word-boundary regex `\bFILENAME\b`. | S |
| P2-5 | **SLT runner silently skips tests without accounting.** No counter or summary for skipped statements. Impossible to track coverage improvement/regression. | Test Harness (runner #3) | `sqllogictest_runner.rs:647` | Add skip-count logging per test file. | S |
| P2-6 | **Sorting helpers mask ORDER BY regressions.** Tests apply `rows.sort()` after `ORDER BY` queries, hiding broken ordering. | Test Harness (Codex #6) | `write_partition_tests.rs:93, 182` | Remove sort or add separate ORDER BY assertion. | S |
| P2-7 | **`try_open()` silent degradation in PG/MySQL tests.** When DuckDB can't connect, tests pass but only verify DataFusion side, not cross-engine correctness. | Test Harness Risk 3 | `cross_engine_postgres_tests.rs`, `cross_engine_mysql_tests.rs` | Log warning or use `#[should_panic]` when DuckDB side is skipped. Ideally, fail test if DuckDB verification is skipped. | S |
| P2-8 | **Postgres/MySQL inlining not implemented.** `store_inlined_data()` returns `Unsupported`. Users who set `data_inlining_row_limit` with PG/MySQL get runtime errors. | Interop M3 | `metadata_writer.rs:581-591` | Either implement or fall back to Parquet gracefully (catch error, skip inlining). | M |
| P2-9 | **`rewrite_duckdb_view_sql` substring match on `count_star()`.** Not word-boundary-aware — could incorrectly rewrite `discount_star()`. | Correctness M5 | `schema.rs:149-156` | Use regex or check for non-alphanumeric boundary before `count`. | S |
| P2-10 | **Snapshot INSERT doesn't populate `schema_version`, `next_catalog_id`, `next_file_id`.** Defaults to 1/0/0. If DuckDB writes to same catalog afterward, sequence values may conflict. | Interop m5 | `metadata_writer_sqlite.rs` (create_snapshot) | Populate these fields properly. | S |
| P2-11 | **No test for inlining + partitioning combination.** What happens when a partitioned table has data below the inlining threshold? | Test Harness gap | N/A | Add combo test. | S |
| P2-12 | **Date32/Date64 inline values produce raw integers.** `arrow_array_value_to_string` for dates produces day/ms counts, not ISO-8601. Cross-engine reads may fail. | Correctness m2 | `table_writer.rs:758-764` | Format as ISO-8601 strings. | S |
| P2-13 | **MAX()+1 ID allocation without uniqueness constraints.** Concurrent writers could allocate duplicate IDs. SQLite single-writer prevents this, but PG/MySQL are vulnerable. | Correctness (Codex #9) | `metadata_writer_sqlite.rs` | Add UNIQUE constraints or use sequences for PG/MySQL. | M |

### P3: Backlog (nits, style, nice-to-haves)

| ID | Finding | Source | Files | Suggested Fix | Effort |
|----|---------|--------|-------|---------------|--------|
| P3-1 | **Inconsistent `map_err` patterns.** Repeated `.map_err(\|e\| DataFusionError::External(Box::new(e)))`. | Idiomatic N-1 | `insert_exec.rs` | Add `to_df_err()` helper. | S |
| P3-2 | **`pre-commit` hook uses hardcoded absolute path.** `/home/zac/.cargo/bin/cargo fmt` won't work for other contributors. | Idiomatic m-5 | `.githooks/pre-commit` | Use `cargo fmt` (rely on PATH). | S |
| P3-3 | **DuckDbPgConn/MySqlConn::query uses infinite column iteration with error-based break.** Relies on DuckDB returning error for out-of-bounds column access. | Idiomatic m-6 | `cross_engine_postgres_tests.rs:162-168`, `cross_engine_mysql_tests.rs:162-168` | Get column count from statement metadata first. | S |
| P3-4 | **`row_idx as u32` truncation in `extract_rows`.** Silent truncation for indices > 2^32. | Correctness m1 | `insert_exec.rs:564` | Use `u32::try_from(row_idx).map_err(...)`. | S |
| P3-5 | **`parse_string_to_array` silently converts unparseable values to NULL.** Silent data loss for parse failures. | Correctness m3 | `table_writer.rs:831-833` | Return error on parse failure. | S |
| P3-6 | **Unused `_snapshot_id` parameter in `get_inlined_data_as_batch`.** Underscore prefix suggests incomplete implementation. | Idiomatic m-4 | `table_writer.rs:421` | Use for snapshot filtering or remove. | S |
| P3-7 | **`rewrite_duckdb_view_sql` byte-offset bug with non-ASCII SQL.** `to_lowercase().find()` returns byte position that may not match original string for non-ASCII. In practice, DuckDB SQL is ASCII. | Idiomatic m-1, Correctness m5 | `schema.rs:149-156` | Add comment documenting ASCII assumption, or use regex. | S |
| P3-8 | **Test assertions use index-based access without bounds checking.** `rows[0]` panics with unhelpful message if empty. | Idiomatic N-5 | `write_partition_tests.rs`, `write_inline_tests.rs` | Add `assert!(rows.len() >= N)` before indexing. | S |
| P3-9 | **`normalize_value` doesn't handle NaN/Inf edge cases.** `s.parse::<f64>()` normalizes these, which may not be desired. | Idiomatic N-6 | `cross_engine_postgres_tests.rs:271`, `cross_engine_mysql_tests.rs:275` | Guard against NaN/Inf normalization. | S |
| P3-10 | **`batches_to_inlined_rows` could use `ArrayFormatter` for all types.** Large match statement has fallback to `ArrayFormatter` — could simplify. | Idiomatic N-4 | `table_writer.rs:696-778` | Use `ArrayFormatter` uniformly. | S |
| P3-11 | **Partial Hive directory verification in tests.** Only `region=US` parquet presence is verified; `region=EU` existence checked without confirming parquet files. | Test Harness (Codex #7) | `write_partition_tests.rs:324, 340` | Verify parquet files exist in all partition directories. | S |
| P3-12 | **Inline/flush tests assert counts not content.** Some tests only verify row counts, leaving room for corruption to go undetected. | Test Harness (Codex #8) | `write_inline_tests.rs:382, 421, 509` | Assert full row content. | S |
| P3-13 | **`is_hybrid_incompatible_error()` converts `statement error` to `statement ok`.** Reduces test coverage — regressions that make statements succeed silently won't be caught. | Test Harness (runner #4) | `sqllogictest_runner.rs:711` | Add counter, log converted statements. | S |

---

## Cross-Reference Resolution

These items were flagged for cross-referencing in the task prompt:

| Cross-Reference Item | Resolution |
|----------------------|------------|
| SQL injection in inlining code | **P0-6** — consolidated from Idiomatic C-1 + Correctness m6. Same root cause: `format!()` with column names. |
| Partition snapshot/field-ID issue | **P0-1** — from Correctness C1. The interop review didn't independently flag this but it's the root cause of potential interop breakage. |
| Atomicity gaps in write paths | **P0-2, P0-3, P0-4, P0-5** — four distinct atomicity bugs. P0-1 and P0-4 are closely related (both involve partitioned writes). |
| Test false-positive risks | **P1-9** (`zip` truncation), **P2-4** (virtual column substring), **P2-6** (sort masking), **P2-7** (silent degradation) — four distinct false-positive mechanisms. |
| Missing DF-write partition/inline tests | **P1-10, P1-11** — consolidated from Interop M1/M2 + Test Harness gap. |

---

## Recommended Agent Assignments

### Agent 1: Write Atomicity Fix (P0-1, P0-2, P0-4, P1-1, P1-7) — Effort: L

The core atomicity issues are deeply interrelated. A single agent should refactor the write transaction model:
- Single `begin_write_transaction` for all partitions (P0-1, P0-4)
- Defer Replace-mode file ending until after upload (P0-2)
- Deterministic partition ordering (P1-1)
- Atomic partition value registration (P1-7)

**Files**: `insert_exec.rs`, `table_writer.rs`, `metadata_writer_sqlite.rs`, `metadata_writer.rs`

### Agent 2: Inline Data Safety Fix (P0-3, P0-5, P1-4, P1-8) — Effort: M

All inline data issues:
- Move `clear_inlined_data` after Parquet write (P0-3)
- Propagate inline read errors (P0-5)
- Fix flush path directory (P1-4)
- `Arc<Vec<String>>` for column names (P1-8)

**Files**: `table_writer.rs`

### Agent 3: Input Validation & Partition Safety (P0-6, P1-2, P1-3, P1-5, P1-6, P2-9, P2-12) — Effort: M

All input validation and silent-failure fixes:
- SQL identifier validation (P0-6)
- All timestamp precisions (P1-2)
- Error for unknown transforms (P1-3)
- URL-encode partition values (P1-5)
- Error for unsupported partition types (P1-6)
- `count_star()` word boundary (P2-9)
- Date formatting for inlined values (P2-12)

**Files**: `metadata_writer_sqlite.rs`, `insert_exec.rs`, `schema.rs`, `table_writer.rs`

### Agent 4: Test Infrastructure Cleanup (P1-9, P1-10, P1-11, P2-2, P2-3, P2-4, P2-5, P2-6, P2-7, P2-11) — Effort: M

All test harness fixes:
- Fix `assert_results_eq` zip truncation (P1-9)
- Add DF-write partition interop test (P1-10)
- Add DF-write inline interop test (P1-11)
- Extract shared test helpers (P2-2)
- Remove dead `rewrite_unqualified_tables` (P2-3)
- Fix virtual column substring matching (P2-4)
- Add SLT skip-count logging (P2-5)
- Fix sort masking (P2-6)
- Fix `try_open` silent degradation (P2-7)
- Add inlining+partitioning combo test (P2-11)

**Files**: `tests/common/`, `tests/cross_engine_*.rs`, `tests/write_*.rs`, `hybrid_asyncdb.rs`, `sqllogictest_runner.rs`

### Remaining Items (no dedicated agent needed)

P2-1 (vectorized partition routing), P2-8 (PG/MySQL inlining fallback), P2-10 (snapshot fields), P2-13 (UNIQUE constraints), and all P3 items can be addressed individually in future sprints or as cleanup tasks.

---

## Summary Statistics

| Priority | Count | Estimated Total Effort |
|----------|-------|----------------------|
| P0 (fix immediately) | 6 | 2L + 2S + 1M = ~2 sprints |
| P1 (fix soon) | 11 | 5S + 4M + 1L = ~1.5 sprints |
| P2 (next sprint) | 13 | 8S + 4M = ~1 sprint |
| P3 (backlog) | 13 | 13S = ~0.5 sprint |
| **Total** | **36** (deduplicated from 57 raw) | |

**Recommended agents**: 4 (as described above)
**Estimated total effort**: 3-4 agent sessions (the 4 agents can run concurrently since they touch mostly non-overlapping files)
