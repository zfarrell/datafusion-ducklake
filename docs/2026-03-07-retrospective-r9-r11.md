# Retrospective: Review Cycles R9-R11

**Date**: 2026-03-07
**Scope**: Review cycles R9, R10, R11 (2026-03-06 to 2026-03-07)
**Prior retrospectives**: `docs/2026-03-03-retrospective-r1-r5.md` (R1-R5), `docs/2026-03-05-retrospective.md` (R1-R8)

---

## Executive Summary

R9-R11 represent the final phase of a review campaign that has now spanned 11 cycles. The codebase is mature: zero validated P0 findings across all three cycles (extending the P0-free streak to 6 consecutive cycles since R6). However, finding counts are NOT declining -- R11 found 45 unique issues, the second-highest since R8 (96). The review process continues to find real bugs, but the severity profile has shifted decisively from "data corruption" to "metadata edge cases" and "defensive coding gaps."

The most significant development in R9-R11 was the implementation and review of F-044 (backend code deduplication), which was the #1 recommendation from both prior retrospectives. F-044 eliminated the PG/MySQL parity gap as a systemic theme -- R10 and R11 found zero parity gap findings, ending a pattern that appeared in 6 of 8 prior cycles.

The most concerning development is the R10-to-R11 regression: R10-S-004 introduced a broken `FOR UPDATE` fix that R11-S-004 identified as invalid PostgreSQL SQL. This is the clearest example yet of fix agents introducing bugs that require another review cycle to catch.

### Key Numbers

| Metric | R9 | R10 | R11 | R1-R8 avg |
|--------|----|----|-----|-----------|
| Raw findings | 55 | 63 | 97 | ~82 |
| After dedup | 25 | 42 | 45 | ~56 |
| Validated P0 | 0 | 0 | 0 | 1.9 |
| Validated P1 | 2 | 6 | 11 | 11.5 |
| P2 | 12 | 17 | 22 | ~22 |
| P3 | 11 | 14 | 9 | ~25 |
| Codex FP rate | 46% | low | 6% (3/48 raw) | ~60% |
| Fix-introduced defects found | 0 | 0 | 1 confirmed | ~0.9/cycle |

---

## 1. Trend Analysis

### 1.1 Finding Counts Per Cycle

| Cycle | Raw | Dedup | P0 | P1 | P2 | P3 | Total P0+P1 |
|-------|-----|-------|----|----|----|----|-------------|
| R1 | ~36 | 36 | 6 | 11 | 13 | 13 | 17 |
| R2 | ~99 | 58 | 5 | 15 | 21 | 17 | 20 |
| R3 | ~67 | 50 | 3 | 9 | 16 | 22 | 12 |
| R4 | ~74 | 46 | 1 | 12 | 20 | 13 | 13 |
| R5 | ~95 | 77 | 0 | 11 | 28 | 38 | 11 |
| R6 | ~107 | 88 | 0 | 14 | 38 | 36 | 14 |
| R7 | ~58 | 50 | 0 | 8 | 14 | 28 | 8 |
| R8 | ~125 | 96 | 0 | 12 | 31 | 53 | 12 |
| R9 | 55 | 25 | 0 | 2 | 12 | 11 | 2 |
| R10 | 63 | 42 | 0 | 6 | 17 | 14 | 6 |
| R11 | 97 | 45 | 0 | 11 | 22 | 9 | 11 |

**Observations**:

1. **P0 findings remain exhausted.** The P0-free streak is now 6 cycles (R6-R11). The last validated P0 was R4-S-001 in early R4. The codebase has no critical safety issues remaining.

2. **P1 findings dipped in R9-R10 but rebounded in R11.** R9 had only 2 P1s, which is expected because R9 was a focused review of F-044 refactoring (not a full-scope review). R10 found 6 P1s, and R11 found 11 -- back to the R1-R8 average of 11.5. P1s are not declining.

3. **Total dedup finding count is declining for R9-R10 but not R11.** R9 (25) was the lowest ever, but it was scope-limited. R10 (42) and R11 (45) are below the R1-R8 average (56) but not dramatically so. The review process continues to find substantial issues.

4. **R9 was an outlier.** Its narrow scope (F-044 refactoring only) makes it non-comparable to full-scope reviews. R10 and R11 are better comparators to R1-R8.

### 1.2 P0/P1 Severity Trajectory

| Period | Avg P0/cycle | Avg P1/cycle | P0+P1/cycle |
|--------|-------------|-------------|-------------|
| R1-R3 (foundational) | 4.7 | 11.7 | 16.3 |
| R4-R6 (deepening) | 0.3 | 12.3 | 12.7 |
| R7-R9 (maturation) | 0 | 3.3 | 3.3 |
| R10-R11 (late) | 0 | 8.5 | 8.5 |

The R7-R9 dip is misleading -- R7 and R9 were both constrained-scope reviews. R10-R11 returned to full-scope and found 8.5 P0+P1 per cycle. The honest read is: **full-scope reviews continue to find ~8-12 P1s per cycle, and there is no evidence of convergence**.

### 1.3 False Positive Rates

**Codex P0 false positive rate** has been 100% since R6 -- every P0 claim from Codex in R6-R11 was downgraded. This is expected: there are no real P0s left, but Codex's severity calibration has not adapted.

**Codex overall FP rate** has improved significantly:

| Cycle | Codex Raw | Codex FPs | FP Rate |
|-------|-----------|-----------|---------|
| R4 | ~12 | 3 | ~25% |
| R5 | ~20 | 5 | ~25% |
| R8 | 43 | 17 | ~40% |
| R9 | 13 | 6 | 46% |
| R10 | 20 | ~5 | ~25% |
| R11 | 48 | 3 | 6% |

R11 Codex had a remarkably low 6% false positive rate (3 FPs out of 48 raw findings). This is the best Codex cycle ever. If this holds, it represents a major improvement in review quality.

**Non-Codex agents** continue to have near-zero P1 false positive rates across all cycles. The 5 review agents (correctness, idiomatic, interop, test-harness, codex) have complementary strengths with minimal overlap.

### 1.4 Are We Fixing Root Causes or Playing Whack-a-Mole?

Mixed. Two root causes have been addressed:

1. **F-044 (backend deduplication)**: Implemented in R9. This was the #1 recommendation from both prior retrospectives. The PG/MySQL parity gap theme -- which appeared in 6 of 8 cycles (R3-R8) -- has zero findings in R10 and R11. This is a genuine root-cause fix.

2. **Lost commits**: Fixed by the merge agent workflow in R4. Zero recurrences in R5-R11.

But several themes persist:

1. **Test infrastructure issues**: Findings in every single cycle R1-R11 (11/11). R11 found 9 test-related issues (R11-S-023, S-024, S-026, S-032, S-039, S-040, S-041, S-045, plus test coverage gaps in S-011, S-031). The test harness has never been systematically addressed.

2. **Unchecked arithmetic/casts**: Findings in 8+ cycles. R11 found 6 instances (S-013, S-014, S-018, S-019, S-020, S-021). Despite repeated fixes, new code and newly-reviewed code continue to have this pattern.

3. **Inline data fragility**: R10-S-001 (clear_inlined_data failure risks duplication) is a variant of the original R1 P0-3 that has resurfaced in different forms across 5 cycles (R1, R4, R5, R6, R10).

---

## 2. Recurring Themes: Comparison to Prior Retrospectives

### 2.1 R1-R5 Retrospective Recommendations -- Status After R11

| # | Recommendation | Adopted? | Result After R11 |
|---|----------------|----------|------------------|
| R1 | Cross-backend verification in fix prompts | Yes (R6+) | **Partially effective.** Reduced parity gaps but did not eliminate them until F-044 (R9) addressed the root cause structurally. |
| R2 | Unify DML and INSERT metadata paths | **Yes (F-044, R9)** | **Effective.** Macro unification in `metadata_writer_impl.rs` shares code across paths. R10-R11 found zero DML/INSERT divergence issues. |
| R3 | Validate fix commits before marking resolved | Yes (R4+) | **Fully effective.** Zero lost commits in R5-R11. |
| R4 | Downgrade Codex P0 claims by default | Yes (R4+) | **Fully effective.** Synthesis step routinely downgrades. All Codex P0s in R6-R11 correctly downgraded. |
| R5 | Add regression tests for fix-introduced patterns | Partially | **Insufficient.** R11-S-024 explicitly flags that R10 fixes (checked_add, Arc, transaction wrapping) have no regression tests. |

### 2.2 R6-R8 Retrospective Recommendations -- Status After R11

| # | Recommendation | Adopted? | Result After R11 |
|---|----------------|----------|------------------|
| 8.1.1 | Implement F-044 instead of another full review | **Yes (R9)** | **Highly effective.** ~4,137 lines removed. Parity gap theme eliminated. Best structural improvement of the project. |
| 8.1.2 | Narrow review scope to PG/MySQL, compaction, inlined data | Partially | R9 was scoped to F-044. R10-R11 returned to full-scope. |
| 8.1.3 | Assign top-20 P3 items | No | P3 backlog continues to grow. |
| 8.1.4 | Add backend-parity CI check | No | Moot after F-044 -- shared macros prevent most parity drift. |
| 8.2 | Graduate to PR-based reviews | No | Still doing full-scope integration-branch reviews. |

### 2.3 New Themes in R9-R11

**Theme A: Snapshot-Awareness Gaps in Queries (R11)**

R11-S-005 and R11-S-006 found that `get_file_column_stats` and `get_partition_columns` join `ducklake_column` with `c.end_snapshot IS NULL` only, ignoring the target snapshot for time-travel queries. This is a new category: metadata queries that are correct for current-snapshot reads but incorrect for historical snapshots. R10-S-002 and R10-S-003 found a related but different issue (missing `table_id` join). Together, these represent a systematic audit gap in metadata query correctness.

**Theme B: `append_table_files` Incompleteness (R11)**

R11-S-001 found that `append_table_files` (the partitioned INSERT commit path) initializes `cumulative_row_id = 0` instead of reading the current `next_row_id`, and never updates `ducklake_table_stats`. This is the most impactful finding since R4 -- it affects every partitioned INSERT to a non-empty table. This path was presumably created during F-044 macro migration or existed prior but was never tested with multi-insert scenarios.

**Theme C: Fix-Introduced Regressions from R10 (R11)**

R11-S-004 identified that R10-S-004's fix (adding `FOR UPDATE` to DDL snapshot creation) produces invalid PostgreSQL SQL: `SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot FOR UPDATE`. PostgreSQL rejects `FOR UPDATE` on queries with aggregates. This means the R10 fix for the DDL schema-version race is itself broken.

---

## 3. Process Assessment

### 3.1 Agent Workflow

**Worktree isolation**: Continues to work well. No coordination conflicts, no lost commits. The merge agent workflow is the single most successful process innovation of the project.

**Agent sizing**: R9 used 5 recommended fix agents (scoped well). R10 recommended 4 agents. R11 recommended 4 agents. The 4-5 agent model with 5-7 findings per agent remains the optimal configuration, as identified in the R6-R8 retrospective.

**Fix agent prompts**: Cross-backend verification instructions in agent prompts are now less critical thanks to F-044. The shared macro approach prevents most parity drift at the code level.

### 3.2 Review Quality

The 5-agent review structure (correctness, idiomatic, interop, test-harness, codex) continues to find real bugs. Each agent has a distinct value proposition:

| Agent | R9-R11 Unique Contribution |
|-------|--------------------------|
| Correctness | R11-S-001 (append_table_files row_id), R11-S-003 (MERGE multi-match) -- highest-impact P1s |
| Idiomatic | Performance issues, code quality, allocation patterns |
| Interop | R10-S-005 (SNAPPY compression), R11-S-011/S-031 (cross-engine test gaps) |
| Test Harness | Test duplication, false-pass risk, coverage gaps |
| Codex | Broadest coverage; R11 had exceptional 94% precision. Uniquely found R11-S-004 (FOR UPDATE aggregate) and R10-S-002/S-003 (table_id join) |

**Codex has earned its keep.** After persistent ~40-60% FP rates in R4-R9, R11 Codex achieved 94% precision (3 FPs out of 48 raw). If this is not a one-cycle anomaly, Codex has crossed a quality threshold. Even at historical FP rates, Codex uniquely identifies issues no other agent catches.

### 3.3 Fix Quality: R10-to-R11 Regression Analysis

R11-S-004 is a confirmed fix-introduced regression from R10:

- **R10-S-004** identified: DDL `schema_version` race on Postgres (concurrent DDL produces duplicate schema versions because `MAX(schema_version) + 1` inside READ COMMITTED transaction is not serializable).
- **R10 fix**: Add `FOR UPDATE` to the query.
- **R11-S-004 discovery**: `SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot FOR UPDATE` is invalid PostgreSQL -- you cannot use `FOR UPDATE` with aggregate functions.
- **Root cause of the bad fix**: The fix agent applied `FOR UPDATE` mechanically without testing on PostgreSQL. The macro generates the same SQL for all backends, and SQLite/MySQL either ignore or tolerate `FOR UPDATE` with aggregates, so tests pass on the non-Docker backends.

**Historical fix-introduced regression rate:**

| Cycle | Fix-Introduced Found | Details |
|-------|---------------------|---------|
| R3 | 1 | R3F-006 (parity gap from R2 SQLite-only fix) |
| R4 | 1 | R4-S-008 (non-standard snapshot_changes tokens from R3) |
| R5 | 2 | R5-S-001 (lexicographic stats from R3), R5-S-003 (parity gap from R3) |
| R6 | 0 | -- |
| R7 | 3 | R7-S-001, R7-S-002, R7-S-008 (all from R6 fixes) |
| R8 | 0 | -- |
| R9 | 0 | -- |
| R10 | 0 | -- |
| R11 | 1 | R11-S-004 (broken FOR UPDATE from R10-S-004) |
| **Total** | **8** | |

Fix-introduced regressions average 0.73/cycle. They cluster after large fix batches (R7 had 3 after R6's 10-agent fix cycle). The R10-to-R11 regression is notable because it is a fix that is provably broken (PostgreSQL will reject the query at runtime), not a subtle edge case.

**Are there other R10 fix regressions in R11?** Scanning R11 findings:
- R11-S-024 notes R10 fixes lack regression tests (checked_add, Arc wrapping), but does not claim they are incorrect -- just untested.
- No other R11 finding explicitly references an R10 fix as broken.

So R11-S-004 appears to be the only R10-introduced regression, but the lack of regression tests for R10 fixes (R11-S-024) means we cannot be confident.

### 3.4 Merge Process

No merge issues reported in R9-R11. The merge agent workflow established in R4 continues to function reliably.

### 3.5 Codex Value Assessment

| Metric | R4-R8 | R9-R11 |
|--------|-------|--------|
| P0 claims | 11 | ~2 |
| P0 FP rate | 91% | 100% |
| Overall FP rate | ~40-60% | ~25% (improving) |
| Unique high-value finds | Yes | Yes (R10-S-002/003 table_id join, R11-S-004 FOR UPDATE) |

**Verdict**: Codex is worth the effort. Its FP rate has improved markedly, and it continues to find issues that other agents miss. The R10-S-002/R10-S-003 table_id join bugs (P1) were Codex-unique discoveries that would have caused incorrect query results on SQLite. The synthesis step's downgrade protocol handles the remaining P0 FPs efficiently.

---

## 4. R10-to-R11 Regression Deep Dive

### R11-S-004: FOR UPDATE with Aggregate Fails on PostgreSQL

**Timeline**:
1. R8-S-005 first identified: PostgreSQL `schema_version` counter not concurrency-safe.
2. R8 fix: Added `pg_advisory_xact_lock` approach (commit `71da28c`).
3. R10-S-004 re-identified: DDL schema-version race on Postgres (possibly the R8 fix was incomplete or the macro migration changed the code path).
4. R10 fix: Added `FOR UPDATE` to the `MAX(schema_version)` query in `create_ddl_snapshot!` macro.
5. R11-S-004 discovered: The `FOR UPDATE` is invalid PostgreSQL SQL when combined with an aggregate.

**Why this happened**:
1. The fix agent applied a textbook concurrency pattern (`FOR UPDATE`) without considering that PostgreSQL disallows it with aggregates.
2. The macro generates the same SQL for all backends. SQLite doesn't support `FOR UPDATE` at all (ignores it or doesn't parse it in this context). MySQL may tolerate it differently.
3. No PostgreSQL-specific test was run to validate the fix. Docker-dependent PG tests are commonly skipped.
4. The R10 synthesis document suggested `FOR UPDATE` as the fix approach, so the fix agent was following synthesis guidance that was itself incorrect.

**How often does this pattern recur?** Fix-introduced regressions have occurred in 6 of 11 cycles (R3, R4, R5, R7, R11, plus R3F-006 which was a parity gap). However, a fix that generates invalid SQL on a specific backend is a first. Prior regressions were subtle (wrong token names, wrong comparison semantics). R11-S-004 would cause a hard runtime error on PostgreSQL DDL operations.

**Systemic implications**: The R10 synthesis document -- which is supposed to be the validated, expert-reviewed fix plan -- suggested an approach that was itself wrong. This means the synthesis step has a blind spot for backend-specific SQL validity.

---

## 5. Recommendations

### 5.1 Stop Full-Scope Reviews -- Graduate to Targeted Reviews

**Evidence**: 11 cycles of full-scope review have found zero P0s since R4 (7 cycles). P1 counts are stable at ~8-12 per full-scope cycle with no convergence trend. Each cycle costs 9-13 agents (5 review + 4-8 fix). The cumulative agent count is ~140+.

**Recommendation**: Stop full-scope reviews. They are no longer cost-effective for finding critical bugs. Instead:

1. **PR-based reviews** for new code changes (targeted, fast feedback).
2. **Focused deep dives** for known risk areas (e.g., "audit all metadata queries for snapshot-awareness," "audit all SQL generation for PostgreSQL compatibility").
3. **Post-fix reviews** only when a structural change is made (like F-044).

### 5.2 Require PostgreSQL Integration Tests for SQL-Generation Fixes

**Evidence**: R11-S-004. The R10 fix generated invalid PostgreSQL SQL because no PG test was run.

**Recommendation**: Any fix that changes SQL generation in the macro layer (`metadata_writer_impl.rs`, `metadata_provider_impl.rs`) MUST be validated against PostgreSQL via Docker tests before merge. Add to fix agent prompts: "If you changed SQL queries in macro code, run `cargo test --features write-postgres` to validate PostgreSQL compatibility."

### 5.3 Add Regression Tests for Every Fix

**Evidence**: R11-S-024 explicitly flags R10 fixes as lacking regression tests. Historical pattern: fix agents add code fixes but not tests for those fixes, creating the conditions for undetected regressions.

**Recommendation**: Make fix agent prompts require: "For each finding you fix, add or extend a test that would fail without your fix." Track compliance in the synthesis document.

### 5.4 Address the Snapshot-Awareness Audit Gap

**Evidence**: R10-S-002, R10-S-003, R11-S-005, R11-S-006 -- four findings across two cycles about metadata queries that are correct for current snapshots but incorrect for time-travel queries.

**Recommendation**: Conduct a targeted audit of all metadata queries in `metadata_provider_impl.rs` for snapshot-awareness. This is a bounded scope (~900 lines) that a single focused agent could complete. It would catch the remaining instances of this pattern rather than discovering them one per review cycle.

### 5.5 Systematically Address Test Infrastructure

**Evidence**: Test infrastructure findings appear in every cycle (11/11). The test harness has never been systematically addressed -- fixes are incremental and per-finding.

**Recommendation**: Dedicate one focused effort to:
1. Centralize test helpers (R11-S-023 -- duplicated across 17+ files)
2. Add regression tests for prior fix cycles (R11-S-024)
3. Fix permanently ignored tests (R11-S-040)
4. Tighten SLT patterns (R11-S-026)

This is a one-time investment that would reduce the per-cycle test infrastructure findings.

### 5.6 Fix the Known R10 Regression Before Next Review

**Evidence**: R11-S-004 is a confirmed broken fix. The FOR UPDATE + aggregate query will fail at runtime on PostgreSQL.

**Recommendation**: Fix R11-S-004 immediately using one of:
- `SELECT schema_version FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1 FOR UPDATE`
- `pg_advisory_xact_lock` (used in R8-S-005 fix)
- Remove `FOR UPDATE` and accept the narrow race window (it was only a P1 originally)

### 5.7 Cost/Benefit: Continuing Reviews vs. Shipping

**Cost of one more full-scope review cycle**: ~13 agents (5 review + 8 fix), finding ~0 P0 + ~10 P1 + ~20 P2.

**Value of those findings**: P1s are increasingly about edge cases (NaN in compaction threshold, unchecked i32 truncation for extreme dates, case-sensitive type comparison). These are real bugs but low real-world probability. No single P1 from R10-R11 would cause data corruption for a normal user.

**Alternative uses of the same effort**:
- Ship the current state and fix issues as they arise from real usage
- Improve SLT pass rate (currently ~55%, was ~62% before regressions)
- Implement deferred F-036 (INSERT streaming for OOM prevention -- an actual production risk)
- Create comprehensive documentation and examples

**Verdict**: We are past diminishing returns for review cycles. The project should ship. Fix the known R11-S-004 regression, apply the high-priority P1 fixes from R10-R11 (particularly R11-S-001 append_table_files and R11-S-003 MERGE multi-match), and move to maintenance mode with PR-based reviews.

---

## 6. Cumulative Statistics (R1-R11)

| Metric | Value |
|--------|-------|
| Total review cycles | 11 |
| Total raw findings | ~900+ |
| Total deduplicated findings | ~600+ |
| Total fixed | ~420+ |
| Validated P0 total | 15 (all in R1-R4) |
| Validated P1 total | ~110 |
| P0-free streak | 6 cycles (R6-R11) |
| Fix-introduced regressions | 8 total (R3, R4, R5, R7, R11) |
| Review agents spawned | ~55 |
| Fix agents spawned | ~80 |
| Total agents | ~135 |
| Deferred architectural items | 4 (F-036, F-045, R4-S-018, R6-S-017) |
| F-044 status | COMPLETE (R9) -- ~4,137 lines removed |
| P3 backlog (unaddressed) | ~150+ |
| Test count | ~820+ |
| Cross-engine tests | 72+ |
| SLT pass rate | ~55% (declined from 62% -- needs investigation) |

---

## Appendix A: Theme Resolution Status (R1-R11)

| # | Theme | First Found | Status After R11 | Cycles Active |
|---|-------|-------------|------------------|---------------|
| 1 | PG/MySQL backend parity gap | R2 | **RESOLVED** (F-044 in R9) | 6 of 8 (R3-R8), 0 of 3 (R9-R11) |
| 2 | DML metadata path divergence | R3 | **RESOLVED** (F-044 macros) | 5 of 8, 0 of 3 |
| 3 | Inline data subsystem fragility | R1 | **Mostly resolved** -- R10-S-001 is last variant | 6 of 11 |
| 4 | Test infrastructure issues | R1 | **UNRESOLVED** -- findings every cycle | 11 of 11 |
| 5 | Codex P0 over-reporting | R4 | **Managed** via synthesis downgrade protocol | 8 of 8 (all Codex cycles) |
| 6 | Unchecked arithmetic/casts | R3 | **UNRESOLVED** -- new instances each cycle | 8+ of 11 |
| 7 | Interop DDL schema mismatches | R8 | **Mostly resolved** (R8 fixes) | 2 of 4 |
| 8 | PG concurrency races | R7 | **Worsened** (R10 fix broke, R11 caught) | 3 of 5 |
| 9 | Snapshot-awareness gaps in queries | R10 | **NEW** -- 4 findings across R10-R11 | 2 of 2 |
| 10 | Fix-introduced regressions | R3 | **Persistent** -- 8 total, no process fix | 6 of 11 |
| 11 | Lost commits | R1 | **RESOLVED** (merge agent, R4+) | 1 of 11 |

## Appendix B: Comparison of Retrospective Recommendations

| Retro | Key Recommendation | Acted On? | Outcome |
|-------|-------------------|-----------|---------|
| R1-R5 | Implement F-044 | Yes (R9) | Best structural improvement; eliminated parity theme |
| R1-R5 | Validate fix commits | Yes (R4+) | Zero lost commits since |
| R1-R5 | Downgrade Codex P0 | Yes (R4+) | Prevents wasted fix agent effort |
| R1-R5 | Cross-backend verification | Yes (R6+) | Partially effective, superseded by F-044 |
| R1-R5 | Regression tests for fixes | Partial | Still flagged in R11-S-024 |
| R6-R8 | Graduate to PR-based reviews | No | Still doing full-scope reviews |
| R6-R8 | Assign P3 backlog | No | ~150+ P3 items unaddressed |
| R6-R8 | Backend-parity CI check | No | Moot after F-044 |
| **R9-R11** | **Stop full-scope reviews** | Pending | -- |
| **R9-R11** | **Require PG integration tests for SQL fixes** | Pending | -- |
| **R9-R11** | **Snapshot-awareness audit** | Pending | -- |
| **R9-R11** | **Systematic test infrastructure fix** | Pending | -- |
