# Review Cycle Trend Analysis (R1–R5)

**Date**: 2026-03-03
**Analyst**: trend-analyst
**Scope**: All P0 and P1 findings across 5 review cycles

---

## Executive Summary

1. **Most P0/P1 findings are genuinely new issues in previously unreviewed code, not regressions from fix agents.** Of the 73 validated P0/P1 findings across all cycles, only 3 are confirmed fix-agent-introduced defects. The primary driver of new findings is expanded review scope (new agents like Codex, deeper DML coverage).

2. **The PG/MySQL parity gap is a systemic, recurring pattern.** In every cycle where SQLite receives a fix, the corresponding PG/MySQL backends are missed. This produced 3 separate P1 findings (R3F-006, R4-S-006, R5-S-003) across 3 consecutive cycles — the single most predictable failure mode.

3. **The Codex review agent has an 86% P0 false positive rate (6/7 claims downgraded).** All other review agents have a 0% P0 false positive rate (14/14 validated). Codex adds valuable breadth but over-classifies severity.

4. **DML metadata path divergence from INSERT is the highest-yield recurring theme.** The `register_dml_files` code path lacks features present in `register_data_file`: row_id_start, column stats, next_file_id, record_count decrements, table_stats reset. This produced 8+ P1 findings across R3–R5.

5. **Two R1 P0 fixes were lost (worktree cleanup before merge), persisting undetected for 3 review cycles.** R1 P0-3 (inline data lost on flush) and R1 P1-4 (flush path mismatch) were marked as fixed but have no commits in git. They reappeared as R4-S-001 (P0) and R4-S-002 (P1), finally fixed in commit `54d3739`.

---

## 1. Complete P0/P1 Inventory

### P0 Findings

| Cycle | ID | Description | Validated? | Fixed In | Origin |
|-------|-----|-------------|-----------|----------|--------|
| R1 | P0-1 | Partitioned writes create independent snapshots, reassigning column IDs | Yes | R1 (`5b0ff8c`) | Original code |
| R1 | P0-2 | Replace-mode metadata commit before Parquet upload | Yes | R1 (`5b0ff8c`) | Original code |
| R1 | P0-3 | Inline data lost when flush-to-Parquet fails (clear before write) | Yes | **R4** (`54d3739`) | Original code; R1 fix lost |
| R1 | P0-4 | Partitioned writes partially commit on failure | Yes | R1 (`5b0ff8c`) | Original code |
| R1 | P0-5 | Inline data read failure silently swallowed | Yes | **R4** (`54d3739`) | Original code; R1 fix lost |
| R1 | P0-6 | SQL injection via column name interpolation (WRITE path) | Yes | R1 (`d65bf08`) | Original code |
| R2 | F-001 | SQL injection in inlined data queries (READ path) | Yes | R3 (`065c411`) | Original code (different path than R1 P0-6) |
| R2 | F-002 | Non-atomic DELETE/UPDATE/MERGE metadata commit | Yes | R2 (Round 1) | Original code (DML path, not INSERT) |
| R2 | F-003 | CTAS hard-wired to LocalFileSystem | Yes | R2 (Round 1) | Original code |
| R2 | F-004 | sql_write_tests.rs silently passes on errors | Yes | R2 (`f76fc88`) | Original code (test infra) |
| R2 | F-005 | Roundtrip interop tests silently skip without `#[ignore]` | Yes | R2 (`f76fc88`) | Original code (test infra) |
| R3 | R3F-001 | Missing `ducklake_table_column_stats` causes DuckDB crash | Yes | R3 (`3203d33`) | Original code (table never populated) |
| R3 | R3F-002 | `register_dml_files` omits `row_id_start` and `table_stats` | Yes | R3 (`3203d33`) | Original code (DML path) |
| R3 | R3F-003 | MERGE has no orphaned file cleanup on failure | Yes | R3 (`3203d33`) | Original code |
| R4 | R4-S-001 | Inline data cleared before durable Parquet replacement | Yes (P0) | R4 (`54d3739`) | **Same as R1 P0-3** — fix lost to worktree cleanup |
| R5 | CX-001 | Replace mode drops data if register fails after end | Downgraded→P2 | R5 (verified safe) | N/A — within transaction |
| R5 | CX-002 | Inlining flush swallows error + clears data | Downgraded→P1 | R5 (fix-write-safety) | Original code |
| R5 | CX-003 | Date32 partition pruning epoch-days vs ISO strings | Downgraded→P1 | R5 (fix-write-safety) | Original code |
| R5 | CX-004 | normalize_value false positives | Downgraded→P2 | R5 (fix-test-infra) | Original code (test) |
| R4 | CX-002 | Inline flush writes to wrong directory | Downgraded→P1 | R4 (`54d3739`) | **Same as R1 P1-4** — fix lost |
| R4 | CX-003 | Non-atomic partitioned commit | Downgraded→P1 | R4 (fix-atomicity) | Original code |

**P0 validation rate by cycle:**

| Cycle | Claimed | Validated | Rate | Source of false positives |
|-------|---------|-----------|------|--------------------------|
| R1 | 6 | 6 | 100% | N/A |
| R2 | 5 | 5 | 100% | N/A |
| R3 | 3 | 3 | 100% | N/A |
| R4 | 3 | 1 | 33% | Codex (2 downgraded) |
| R5 | 4 | 0 | 0% | Codex (4 downgraded) |
| **Total** | **21** | **15** | **71%** | |

### P1 Findings

| Cycle | ID | Description | Fixed In | Origin |
|-------|-----|-------------|----------|--------|
| R1 | P1-1 | Replace-mode HashMap non-deterministic order | R1 (`5b0ff8c`) | Original code |
| R1 | P1-2 | Timestamp partitions only support microsecond | R1 (`d65bf08`) | Original code |
| R1 | P1-3 | Unknown partition transforms silently produce NULL | R1 (`d65bf08`) | Original code |
| R1 | P1-4 | Inline flush writes to wrong directory | **R4** (`54d3739`) | Original code; R1 fix lost |
| R1 | P1-5 | Hive partition values not URL-encoded | R1 (`d65bf08`) | Original code |
| R1 | P1-6 | compute_partition_value returns None for unsupported types | R1 (`d65bf08`) | Original code |
| R1 | P1-7 | Partition value registration non-atomic | R1 (`5b0ff8c`) | Original code |
| R1 | P1-8 | InlinedDataRow clones column_names per row | R1 fix (lost) | Original code |
| R1 | P1-9 | assert_results_eq uses zip without column-count check | R1 fix (lost) | Original code (test) |
| R1 | P1-10 | No DF-write partitioned data interop test | R1 fix (lost) | Test coverage gap |
| R1 | P1-11 | No DF-write inlined data interop test | R1 fix (lost) | Test coverage gap |
| R2 | F-006 | TOCTOU race in get_or_create_schema() (all sqlx) | R2 (Round 1) | Original code |
| R2 | F-007 | register_column_stats() not transactional | R2 (Round 1) | Original code |
| R2 | F-008 | end_table_files() not transactional | R2 (Round 1) | Original code |
| R2 | F-009 | MERGE panics on unsupported key types | R2 (Round 1) | Original code |
| R2 | F-010 | Delete file format default mismatch | R2 (Round 1) | Original code |
| R2 | F-011 | Missing row_id_start in data file registration | R2 (`b680790`) | Original code |
| R2 | F-012 | Missing ducklake_schema_versions and table_stats | R2 (Round 1) | Original code |
| R2 | F-013 | Column IDs regenerated every write transaction | R2 (Round 1) | Original code |
| R2 | F-014 | Write paths derived from names instead of catalog paths | R2 (Round 1) | Original code |
| R2 | F-015 | MySQL/PostgreSQL ID allocation race (MAX+1) | R2 (Round 1) | Original code (upgraded from R1 P2-13) |
| R2 | F-016 | table_changes() wrong column order for projections | R2 (Round 1) | Original code |
| R2 | F-017 | table_deletions() ignores projection | R2 (Round 1) | Original code |
| R2 | F-018 | UDTFs resolve at latest snapshot, not session | R2 (Round 1) | Original code |
| R2 | F-019 | SLT runner never fails CI | R2 (`f76fc88`) | Original code (test) |
| R2 | F-020 | drop_schema orphans child tables and files | R2 (Round 1) | Original code |
| R3 | R3F-004 | Date32/Date64 inlined data roundtrip broken | R3 (Agent 2) | Original code |
| R3 | R3F-005 | Timestamp inlined data roundtrip broken | R3 (Agent 2) | Original code |
| R3 | R3F-006 | PG/MySQL writers missing R2 fixes (SQLite-only) | R3 (`d9d54ce`) | **Fix parity gap** from R2 |
| R3 | R3F-007 | create_snapshot() doesn't inherit schema_version | R3 (`3203d33`) | Original code |
| R3 | R3F-008 | quote_identifier not applied consistently | R3 (`065c411`) | Partial fix gap from R2 F-001 |
| R3 | R3F-009 | .unwrap() on downcasts in production code | R3 (`888705e`) | Original code |
| R3 | R3F-010 | Unchecked `as` casts across DML execs | R3 (`888705e`) | Original code |
| R3 | R3F-011 | next_catalog_id and next_file_id never populated | R3 (`3203d33`) | Original code |
| R3 | R3F-012 | Timestamp non-UTC timezone silently replaced | R3 (Agent 2) | Original code |
| R4 | R4-S-002 | Inline flush writes to t{table_id}/ (P0→P1) | R4 (`54d3739`) | **Same as R1 P1-4** — fix lost |
| R4 | R4-S-003 | Partitioned commit non-atomic (P0→P1) | R4 (fix-atomicity) | Original code |
| R4 | R4-S-004 | register_dml_files doesn't update next_file_id | R4 (`54d3739`) | Original code (DML path) |
| R4 | R4-S-005 | DML data files missing register_column_stats | R4 (`54d3739`) | Original code (DML path) |
| R4 | R4-S-006 | PG/MySQL register_dml_files missing row_id_start | R4 (`2a51319`) | **Fix parity gap** from R3F-002 |
| R4 | R4-S-007 | end_table_files (Replace mode) doesn't reset table_stats | R4 (`54d3739`) | Original code |
| R4 | R4-S-008 | UPDATE/MERGE snapshot_changes non-standard tokens | R4 (`d567931`) | **Fix-introduced** by R3 (`3203d33`) |
| R4 | R4-S-009 | Delete file file_path uses relative path | R4 (`d567931`) | Original code |
| R4 | R4-S-010 | NULL filter treated as match in DELETE/UPDATE | R4 (`39fea14`) | Original code |
| R4 | R4-S-011 | UPDATE/MERGE skip NOT NULL validation | R4 (`39fea14`) | Original code |
| R4 | R4-S-012 | LIMIT pushed into Parquet scan before DeleteFilter | R4 (`39fea14`) | Original code |
| R4 | R4-S-013 | record_count never decremented after DELETE | R4 (`54d3739`) | Original code |
| R5 | R5-S-001 | Lexicographic MIN/MAX on VARCHAR column stats | R5 (`15b746f`) | **Fix-introduced** by R3 (`3203d33`, R3F-001) |
| R5 | R5-S-002 | replace_table_files doesn't update table_stats after compaction | R5 (fix-metadata) | Original code |
| R5 | R5-S-003 | contains_null inconsistency in PG/MySQL ALTER | R5 (fix-write-safety) | **Fix parity gap** from R3F-001 |
| R5 | R5-S-004 | Inlining flush silently loses existing inline rows | R5 (fix-write-safety) | Original code (variant of R1 P0-5) |
| R5 | R5-S-005 | Date32 partition pruning epoch-days vs ISO strings | R5 (fix-write-safety) | Original code |
| R5 | R5-S-006 | UPDATE panic on invalid assignment column index | R5 (`fc2f5da`) | Original code |
| R5 | R5-S-007 | Delete-delta boundary BETWEEN vs strict lower bound | R5 (fix-metadata) | Original code |
| R5 | R5-S-008 | statistics() sized to base columns, schema() full_schema | R5 (fix-metadata) | Original code |
| R5 | R5-S-009 | strip_prefix mis-trims paths in list_files | R5 (`15b746f`) | Original code |
| R5 | R5-S-010 | Decimal128 negative sign loss in test formatter | R5 (`08f55a9`) | Original code (test) |
| R5 | R5-S-011 | Missing cross-engine tests for DML, ALTER, partitions | R5 (fix-cross-engine) | Test coverage gap |

---

## 2. P0 Validation Rate Analysis

### Overall P0 Statistics

| Metric | Value |
|--------|-------|
| Total P0 claims across R1–R5 | 21 |
| Validated at P0 | 15 |
| Downgraded (false positives) | 6 |
| Overall validation rate | 71% |

### P0 False Positive Rate by Review Agent

| Review Agent | P0 Claims | Validated | False Positives | FP Rate |
|-------------|-----------|-----------|-----------------|---------|
| Correctness | 5 (R1) + 5 (R2) + 3 (R3) = 13 | 13 | 0 | **0%** |
| Interop | 0 P0s across all cycles | — | — | **0%** |
| Idiomatic | 0 P0s across all cycles | — | — | **0%** |
| Test Harness | 1 (R2 F-004) | 1 | 0 | **0%** |
| Codex | 3 (R4) + 4 (R5) = 7 | 1 | 6 | **86%** |

**Analysis**: The Codex review agent produces the vast majority of P0 false positives. Its failure mode is consistent: it flags code patterns as data-loss risks without verifying whether the code executes within a database transaction that would rollback on failure. In R4, it flagged Replace-mode and non-atomic partition commit without checking transaction boundaries. In R5, all 4 P0 claims were downgraded — 2 were within transactions, 1 was a cross-engine-only issue, and 1 was test code.

**Evidence**:
- R4 CX-001 (downgraded): Claimed data loss in Replace mode. Actual code runs `end_table_files` and `register_data_file` within the same metadata writer transaction. Rollback protects against data loss.
- R5 CX-001 (downgraded): Same pattern — claimed Replace mode drops data, but both calls are within a single transaction.
- R5 CX-004 (downgraded): Flagged `normalize_value` as P0 "data loss" — it's test infrastructure code, not production.

---

## 3. Recurring Theme Analysis

### Theme 1: DML Metadata Path Divergence from INSERT (8+ findings, R3–R5)

The `register_dml_files` code path (DELETE/UPDATE/MERGE) is separate from the INSERT path (`register_data_file`). Every time a new metadata requirement is added to INSERT, the DML path falls behind.

| Cycle | Finding | Missing in DML path |
|-------|---------|-------------------|
| R3 | R3F-002 | `row_id_start`, `table_stats` |
| R3 | R3F-003 | MERGE orphan cleanup |
| R4 | R4-S-004 | `next_file_id` |
| R4 | R4-S-005 | `register_column_stats` |
| R4 | R4-S-007 | Replace-mode `table_stats` reset |
| R4 | R4-S-013 | `record_count` decrement for DELETE |
| R5 | R5-S-001 | Type-aware stats aggregation |
| R5 | R5-S-002 | `table_stats` after compaction |

**Root cause**: `register_dml_files` in `metadata_writer_sqlite.rs` was written as a simplified version of `register_data_file` and has never been refactored to share code. Each fix to the INSERT path must be manually replicated in the DML path.

### Theme 2: PG/MySQL Parity Gap (3 P1 findings, R3–R5)

Every fix applied to the SQLite backend is at risk of not being ported to PostgreSQL and MySQL backends.

| Cycle | Finding | SQLite Fix | PG/MySQL Gap |
|-------|---------|-----------|--------------|
| R3 | R3F-006 | R2 F-012/F-013/F-026/F-027 fixed for SQLite | PG/MySQL missing schema_versions, column ID preservation, UUIDs, changes_made |
| R4 | R4-S-006 | R3F-002 fixed for SQLite | PG/MySQL missing row_id_start + table_stats in DML |
| R5 | R5-S-003 | R3F-001 fixed for SQLite | PG/MySQL missing contains_null=1 for ALTER ADD COLUMN |

**Root cause**: The 3 metadata writer backends (`metadata_writer_sqlite.rs`, `metadata_writer_postgres.rs`, `metadata_writer_mysql.rs`) share ~80% identical logic but are maintained as separate files with no shared abstraction. This is tracked as R2 deferred F-044 (provider/writer code deduplication, L effort).

### Theme 3: Inline Data Path Fragility (6+ findings, R1–R5)

The inline data write/flush/read path has produced findings in every cycle:

| Cycle | Findings | Issue |
|-------|----------|-------|
| R1 | P0-3, P0-5 | clear_inlined_data before write; error swallowed |
| R1 | P1-4, P1-8 | Wrong directory; per-row column name clones |
| R3 | R3F-004, R3F-005 | Date32/Timestamp roundtrip broken |
| R4 | R4-S-001 (P0), R4-S-002 | Same as R1 P0-3 and P1-4 (fix lost) |
| R5 | R5-S-004, R5-S-014, R5-S-015 | Error swallowing variant; epoch serialization; Decimal flush |

**Root cause**: The inline data subsystem has complex serialization/deserialization logic with separate code paths for writing, flushing (inline→Parquet), and reading. The clear-before-write antipattern in particular persisted for 3 cycles (R1→R4) due to a lost fix commit.

### Theme 4: Codex P0 Over-Reporting (7 findings, R4–R5)

The Codex review agent consistently flags potential data-loss scenarios without verifying transactional safety:

| Cycle | Claims | Validated | Pattern |
|-------|--------|-----------|---------|
| R4 | 3 P0 | 1 | 2 downgraded — both within transactions |
| R5 | 4 P0 | 0 | All downgraded — transaction safety, test code, cross-engine only |

### Theme 5: Test Infrastructure Masking Issues (12+ findings, R1–R5)

Test infrastructure issues appear in every cycle, with test code that can produce false passes:

| Cycle | Key Findings |
|-------|-------------|
| R1 | P1-9 (zip truncation), P2-4 (substring match), P2-6 (sort masking) |
| R2 | F-004 (silent test pass), F-005 (silent skip), F-019 (SLT never fails) |
| R3 | R3F-015 (helper divergence), R3F-016 (read-only test silent pass) |
| R4 | R4-S-028–R4-S-033 (6 test infrastructure findings) |
| R5 | R5-S-010 (Decimal sign), R5-S-023 (normalize_value), R5-S-031–033, R5-S-040–043 |

---

## 4. Fix-Introduced Regressions

### Confirmed Fix-Introduced Defects (3)

#### 1. R4-S-008: Non-standard snapshot_changes tokens — introduced by R3 fix

- **Introduced by**: Commit `3203d33` (R3 fix agent, R3F-013 "Record snapshot_changes for DML")
- **What agent was fixing**: R3F-013 required adding `snapshot_changes` records for DML operations. The fix agent used `updated_table:{id}` and `merged_into_table:{id}` tokens.
- **What went wrong**: DuckDB uses only `inserted_into_table:{id}` and `deleted_from_table:{id}` tokens for all DML, including UPDATE and MERGE. The agent invented non-standard tokens.
- **Impact**: CDC tracking broken for DF-written UPDATE/MERGE snapshots when read by DuckDB.
- **Fixed in**: R4 commit `d567931` — replaced with standard `inserted_into_table:{id},deleted_from_table:{id}`.
- **Evidence**: `git show d567931 -- src/update_exec.rs src/merge_exec.rs` shows the R3F-013 comment being updated with R4-S-008 correction.

#### 2. R5-S-001: Lexicographic MIN/MAX on VARCHAR column stats — introduced by R3 fix

- **Introduced by**: Commit `3203d33` (R3 fix agent, R3F-001 "Populate ducklake_table_column_stats")
- **What agent was fixing**: R3F-001 was a P0 — DuckDB crashed reading catalogs without `ducklake_table_column_stats`. The fix populated this table using SQL `MIN()`/`MAX()` aggregation.
- **What went wrong**: `min_value` and `max_value` are stored as VARCHAR. SQL MIN/MAX on VARCHAR uses lexicographic ordering, which is wrong for numeric types (e.g., `MIN('9','10') = '10'`).
- **Impact**: DuckDB row-group pruning could incorrectly skip matching data for numeric columns.
- **Fixed in**: R5 commit `15b746f` — replaced with type-aware application-level comparison (`recompute_table_column_stats` at `metadata_writer_sqlite.rs:835`).
- **Evidence**: `git blame src/metadata_writer_sqlite.rs` line 836–838 shows R5-S-001 comment referencing the new type-aware approach.

#### 3. R3F-006: PG/MySQL missing R2 fixes (parity gap from R2 fix agents)

- **Introduced by**: R2 fix agents (fix-interop, fix-atomicity, etc.) that applied fixes to SQLite only
- **What agents were fixing**: F-012 (schema_versions), F-013 (column ID preservation), F-026 (UUID generation), F-027 (changes_made format)
- **What went wrong**: Fix agents modified `metadata_writer_sqlite.rs` but did not propagate changes to `metadata_writer_postgres.rs` or `metadata_writer_mysql.rs`.
- **Impact**: PG/MySQL-backed catalogs had 4 interop regressions relative to SQLite.
- **Fixed in**: R3 commit `d9d54ce` (ported fixes to PG/MySQL).
- **Note**: This pattern repeated in R4-S-006 (R3 fix not ported) and R5-S-003 (another R3 fix not ported). The parity gap is a systemic issue, not an isolated incident.

### Lost Fix Commits (2 findings persisting across cycles)

#### R1 P0-3 → R4-S-001: Inline data cleared before durable write

- **First found**: R1 (P0-3), marked as "Fixed" by "Agent 2"
- **R1 fix commit**: **Not found in git history.** Only 2 of 4 claimed R1 fix commits exist (`5b0ff8c`, `d65bf08`). Agent 2 and Agent 4 commits are missing — likely lost to worktree cleanup before merge.
- **R2 status**: Listed under "Previously Fixed" (inherited R1 claim)
- **R3 status**: Not re-raised (reviewers didn't catch it)
- **R4 status**: Rediscovered as R4-S-001 (validated P0)
- **Actually fixed**: R4 commit `54d3739`
- **Gap**: 3 review cycles (R1→R4) before the bug was actually fixed.

#### R1 P1-4 → R4-S-002: Inline flush writes to wrong directory

- **First found**: R1 (P1-4), marked as "Fixed" by "Agent 2"
- **Same lost commit as above** (Agent 2 worktree)
- **R2 status**: Listed under "Previously Fixed"
- **R4 status**: Rediscovered as R4-S-002 (downgraded from Codex P0 to P1)
- **Actually fixed**: R4 commit `54d3739`

---

## 5. Review Coverage and Diminishing Returns Analysis

### New vs Recurring Findings Per Cycle

| Cycle | Total P0+P1 | Genuinely New | Recurrences | Fix-Introduced | FP/Downgraded |
|-------|------------|---------------|-------------|----------------|---------------|
| R1 | 17 | 17 | 0 | 0 | 0 |
| R2 | 20 | 20 | 0 | 0 | 0 |
| R3 | 12 | 10 | 1 (R3F-008) | 1 (R3F-006 parity) | 0 |
| R4 | 13 | 8 | 2 (R4-S-001/002) | 1 (R4-S-008) | 2 (P0→P1) |
| R5 | 11 | 7 | 0 | 2 (R5-S-001, R5-S-003) | 6 (P0→P1/P2) |

### Are Later Cycles Finding Genuinely New Issues?

**Yes.** Each cycle expands into new code areas:

- **R1**: Write atomicity, partition handling, SQL injection, inline data (focus: INSERT path, write infrastructure)
- **R2**: DML atomicity (DELETE/UPDATE/MERGE), table functions, drop cascade, interop alignment (focus: new Codex agent added, DML path, table functions)
- **R3**: DML metadata completeness, PG/MySQL parity, inlined data roundtrip, numeric safety (focus: cross-engine interop gaps revealed by integration testing)
- **R4**: DML metadata integrity (stats, counts, IDs), interop format correctness, DML correctness (NULL filters, NOT NULL, LIMIT), test infrastructure (focus: deeper DML metadata, runtime correctness)
- **R5**: Stats correctness, partition pruning, test formatters, table function edge cases (focus: edge cases in previously-fixed areas, test infrastructure masking)

### Diminishing Returns Evidence

| Metric | R1 | R2 | R3 | R4 | R5 |
|--------|----|----|----|----|-----|
| P0 validated | 6 | 5 | 3 | 1 | 0 |
| P1 validated | 11 | 15 | 9 | 12 | 11 |
| P0+P1 total | 17 | 20 | 12 | 13 | 11 |
| % findings that are P0 | 17% | 9% | 6% | 2% | 0% |
| P0 FP rate | 0% | 0% | 0% | 67% | 100% |
| Fix-introduced | 0 | 0 | 1 | 1 | 2 |

**Observations**:
1. **P0 findings are exhausted by R4.** R5 had zero validated P0s. The codebase's critical safety issues have been addressed.
2. **P1 count is relatively stable** (9–15 per cycle), suggesting each review cycle continues to find real, impactful issues in newly-examined or newly-written code.
3. **Fix-introduced regressions are increasing** (0, 0, 1, 1, 2) as the fix surface area grows. This is expected but warrants mitigation.
4. **The P0 false positive rate is increasing** (0%, 0%, 0%, 67%, 100%) as fewer real P0s exist and the Codex agent continues flagging at the same threshold.

### Files Most Frequently Flagged

| File | R1 | R2 | R3 | R4 | R5 | Total P0+P1 |
|------|----|----|----|----|-----|-------------|
| `metadata_writer_sqlite.rs` | 3 | 5 | 4 | 4 | 3 | 19 |
| `table_writer.rs` | 4 | 2 | 3 | 2 | 3 | 14 |
| `insert_exec.rs` | 5 | 1 | 1 | 0 | 0 | 7 |
| `metadata_writer_postgres.rs` | 0 | 2 | 1 | 2 | 2 | 7 |
| `metadata_writer_mysql.rs` | 0 | 2 | 1 | 2 | 2 | 7 |
| `merge_exec.rs` | 0 | 1 | 1 | 0 | 2 | 4 |
| `update_exec.rs` | 0 | 1 | 1 | 2 | 1 | 5 |
| `delete_exec.rs` | 0 | 1 | 1 | 1 | 0 | 3 |
| Test files | 3 | 4 | 2 | 6+ | 4+ | 19+ |

`metadata_writer_sqlite.rs` and `table_writer.rs` together account for ~45% of all P0/P1 findings.

---

## 6. Recommendations (Evidence-Backed)

### R1: Mandate cross-backend verification for all SQLite fixes

**Evidence**: PG/MySQL parity gap produced P1 findings in 3 consecutive cycles (R3F-006, R4-S-006, R5-S-003). Each time, the review-fix cycle costs resources to discover and fix the same class of gap.

**Action**: Fix agent prompts should include: "For any change to `metadata_writer_sqlite.rs`, verify the corresponding change exists in `metadata_writer_postgres.rs` and `metadata_writer_mysql.rs`."

### R2: Unify DML and INSERT metadata paths

**Evidence**: `register_dml_files` divergence from `register_data_file` produced 8+ P1 findings across R3–R5 (R3F-002, R4-S-004/005/007/013, R5-S-001/002).

**Action**: Refactor `register_dml_files` to call `register_data_file` internally, or extract shared logic into a common function. This is part of the deferred F-044 scope.

### R3: Validate fix agent commits before marking findings as resolved

**Evidence**: R1 P0-3 and R1 P1-4 were marked as "Fixed" but their commits were lost to worktree cleanup. This persisted undetected for 3 cycles until R4 rediscovered them.

**Action**: After each fix cycle, run `git log --oneline` and verify each claimed fix has a corresponding commit on the integration branch. Do not rely on synthesis document status alone.

### R4: Downgrade Codex P0 claims to P1 by default; validate before accepting

**Evidence**: Codex P0 false positive rate is 86% (6/7). All other agents have 0% P0 FP rate.

**Action**: Codex findings labeled P0 should be treated as P1-candidates requiring source code validation before being classified as P0. The synthesis step already does this (R4 and R5 correctly downgraded), so this is more of a process note for efficiency.

### R5: Add regression tests for fix-introduced patterns

**Evidence**: 3 fix-introduced defects found (R4-S-008, R5-S-001, R3F-006). Two of these involve new code that was never tested (snapshot_changes tokens, stats aggregation logic).

**Action**: Fix agents should add or extend tests covering the new code they introduce, particularly for interop-sensitive changes (snapshot_changes format, stats aggregation, metadata field population).

---

## Appendix: Git History Reference

### Review Cycle Boundaries

| Cycle | Review Start | Fix Commits | Review End |
|-------|-------------|-------------|------------|
| R1 | `a25f84f` (2026-03-01) | `d65bf08`, `5b0ff8c` (+ 2 lost) | `2c5e9ff` |
| R2 | `8484233`–`ca762f0` (2026-03-02) | `b680790`, `f76fc88`, `8e36f3b`, `30cb263` (+ 6 missing) | `f271b15` |
| R3 | `e2207b2`–`46e00a3` (2026-03-02) | `065c411`, `d9d54ce`, `888705e`, `3203d33`, `3930b56`, `0aa590c`, `69388dd` | `7d0180b` |
| R4 | `906d3e2`–`d75c4a8` (2026-03-03) | `d567931`, `2a51319`, `39fea14`, `d294651`, `54d3739`, `11e4084`, `fbeef2e` | `133d7d9` |
| R5 | `3ba00a8`–`49ed49b` (2026-03-03) | `15b746f`, `fc2f5da`, `7c685bd`, `84d51ff`, `08f55a9` | `49501c9` |

### Fix Agent Commit Count by Cycle

| Cycle | Planned Agents | Commits Found | Commits Missing |
|-------|---------------|---------------|-----------------|
| R1 | 4 | 2 | 2 (Agent 2, Agent 4) |
| R2 | 10 | 4 | 6 (fix-security, fix-atomicity, fix-interop, fix-dml, fix-providers, fix-quality) |
| R3 | 8 | 7 | 0 (all accounted) |
| R4 | 8 | 7 | 1 (fix-atomicity partial: R4-S-018 lost) |
| R5 | 8 | 5 | 0 (all accounted) |
