# Comprehensive Review Cycle Retrospective (R1-R8)

**Date**: 2026-03-05
**Scope**: All 8 review/fix cycles (2026-03-01 to 2026-03-05)
**Prior analysis**: `docs/2026-03-03-retrospective-r1-r5.md` (R1-R5 baseline)

---

## Executive Summary

Over 5 days and 8 review/fix cycles, the DataFusion-DuckLake project underwent systematic code review that identified **501 deduplicated findings** from **661 raw findings** and fixed **~335** of them. The project evolved from 6 validated P0 findings (data corruption/security) to three consecutive zero-P0 cycles (R6-R8).

### Key Findings

1. **P0 findings are exhausted.** Zero validated P0 findings in R6, R7, and R8. Codex continues claiming P0s at a 91%+ false positive rate. The codebase's critical safety posture is sound.

2. **P1 counts are stable but shifting in nature.** R6-R8 averaged 11.3 P1/cycle (vs 11.6 in R1-R5). Early P1s were fundamental safety bugs; late P1s are backend parity gaps and interop schema mismatches.

3. **PG/MySQL backend parity remains the #1 systemic issue** (6 of 8 cycles). R8 found 3 new P1 parity gaps (next_file_id, partition values, snapshot_changes) despite R6-R7 fixes. Root cause (F-044, three separate ~3000-line files) has been deferred since R2.

4. **R6-R8 fix agents achieved 98%+ assigned fix rates**, with R7 and R8 at 100%. Process maturation (unique agent names, worktree isolation, merge workflow, Codex validation) has eliminated infrastructure-class failures.

5. **Finding count is NOT declining.** R8 (96 dedup) was the second-highest cycle ever. Deeper correctness reviews (R8 used 6 parallel agents covering all 35 source files) keep surfacing genuine issues in under-reviewed areas.

6. **The R1-R5 analysis's recommendations were partially adopted** with measurable results. Cross-backend verification in fix agent prompts reduced but did not eliminate parity gaps. Codex severity validation eliminated ~30 false positives. Commit verification prevented lost-fix recurrences. But the structural recommendation (F-044 deduplication) was never implemented, so the parity gap pattern persists.

---

## 1. P0/P1 Inventory for R6-R8

### P0 Findings (R6-R8)

| Cycle | Claimed | Validated | Source | Disposition |
|-------|---------|-----------|--------|-------------|
| R6 | 3 (Codex) | 0 | CX-W-001, CX-M-001, CX-TF-001 | All downgraded to P2 (transaction safety, documented workarounds) |
| R7 | 0 | 0 | — | First cycle with zero P0 claims from any agent |
| R8 | 1 (Codex) | 0 | MERGE match loop | FP: cardinality check exists at lines 498-505 |
| **Total** | **4** | **0** | | |

**R6-R8 validated P0 count: zero.** This continues the trend from R5. The last validated P0 was R4-S-001 (inline data cleared before durable write, a rediscovery of R1 P0-3).

### P1 Findings (R6-R8): Complete Inventory

| Cycle | ID | Description | Validated? | Fixed In | Origin |
|-------|-----|-------------|-----------|----------|--------|
| R6 | R6-S-001 | replace_table_files missing table_id in column stats | Yes | `d3aa034` | Original code |
| R6 | R6-S-002 | Compaction UDTFs execute side effects at planning time | Yes | `f93444c` | Original code |
| R6 | R6-S-003 | PG/MySQL missing record_count decrement on DELETE | Yes | `75ad2e1` | **Backend parity gap** from R4 fix |
| R6 | R6-S-004 | end_table_files backend drift (PG/MySQL vs SQLite) | Yes | `75ad2e1` | **Backend parity gap** from R4 fix |
| R6 | R6-S-005 | unwrap() on downcasts in merge_exec extract_key_value | Yes | `aaf5a4f` | Original code |
| R6 | R6-S-006 | Silent downcast failures as NULL in partition routing | Yes | `aaf5a4f` | Original code |
| R6 | R6-S-007 | Inconsistent inlined value parse policy | Yes | `c9c761b` | Original code |
| R6 | R6-S-008 | arrow_array_value_to_string returns Ok("") on failure | Yes | `aaf5a4f` | Original code |
| R6 | R6-S-009 | Hardcoded schema_version=1 in inlined data naming | Yes | `b8a4476` | Original code |
| R6 | R6-S-010 | SET NOT NULL without data validation | Yes (documented) | `f4c0f58` | Original code (limitation) |
| R6 | R6-S-011 | parse_table_name doesn't unescape quoted identifiers | Yes | `f93444c` | Original code |
| R6 | R6-S-012 | CDC paths missing encryption factory | Yes | `b8a4476` | Original code |
| R6 | R6-S-013 | Transaction state tracking test false positive | Yes | `03f9cb3` | Original code (test) |
| R6 | R6-S-014 | Duplicated type-to-string conversion | Unfixable | — | Module structure constraint |
| R7 | R7-S-001 | OnceLock silently swallows INSTALL ducklake failure | Yes | `843096a` | **Fix-introduced** by R6 (OnceLock added in R6-S-026) |
| R7 | R7-S-002 | Snapshot ID rollback race in concurrent DDL | Yes | `843096a` | **Fix-introduced** by R6 (snapshot propagation R6-S-040) |
| R7 | R7-S-003 | PartitionTransform silent fallback for unknown transforms | Yes | `843096a` | Original code (pre-existing) |
| R7 | R7-S-004 | register_schema missing with_catalog_snapshot_id | Yes | `843096a` | Original code |
| R7 | R7-S-005 | Partition pruning uses string comparison for all types | Yes | `843096a` | Original code |
| R7 | R7-S-006 | parse_values.rs decimal errors propagate in Lenient mode | Yes | `843096a` | Original code |
| R7 | R7-S-007 | decode_decimal_bytes panics for >16 byte input | Yes | `843096a` | Original code |
| R7 | R7-S-008 | parse_values.rs module is dead code — not wired in | Yes | `f5f0a2d` | **Fix-introduced** by R6 (parse_values created in R6-S-022) |
| R8 | R8-S-001 | replace_table_files doesn't end active delete files | Yes | `ed9c86c` | Original code |
| R8 | R8-S-002 | recompute_table_column_stats column join missing filters | Yes | `68a14dd` | Original code |
| R8 | R8-S-003 | calculate_footer_size_from_bytes underreports by 8 | Yes | `b59edb1` | Original code (interop-critical) |
| R8 | R8-S-004 | Timestamp inline serialization drops sub-second precision | Yes | `b59edb1` | Original code (data loss) |
| R8 | R8-S-005 | PostgreSQL schema_version counter not concurrency-safe | Yes | `71da28c` | Original code |
| R8 | R8-S-006 | PG/MySQL TOCTOU + no uniqueness for active names | Yes | `71da28c` | Original code |
| R8 | R8-S-007 | MySQL register_data_file never updates next_file_id | Yes | `875814e` | **Backend parity gap** from original code |
| R8 | R8-S-008 | PG/MySQL missing partition value registration | Yes | `875814e` | **Backend parity gap** — never implemented |
| R8 | R8-S-009 | PG/MySQL missing record_snapshot_changes for DML | Yes | `875814e` | **Backend parity gap** — never implemented |
| R8 | R8-S-010 | ducklake_schema_versions has extra table_id column | Yes | `b59edb1` | Original code (interop-breaking) |
| R8 | R8-S-011 | ducklake_data_file/delete_file have extra partial_max | Yes | `b59edb1` | Original code (interop-breaking) |
| R8 | R8-S-012 | Append-mode schema validation allows silent column removal | Yes | `68a14dd` | Original code |

**R6-R8 P1 summary**: 34 P1 findings across 3 cycles. 33 validated and fixed, 1 unfixable (R6-S-014). Zero false positives among P1 claims.

### P1 Origin Classification (R6-R8)

| Origin | Count | % |
|--------|-------|---|
| Original code (never reviewed before) | 25 | 74% |
| Backend parity gap (fix not ported) | 5 | 15% |
| Fix-introduced defect | 3 | 9% |
| Unfixable (structural) | 1 | 3% |

---

## 2. Trend Comparison: R1-R5 vs R6-R8

### 2.1 Did the R1-R5 Recommendations Get Adopted?

| Recommendation | Adopted? | Result |
|----------------|----------|--------|
| **R1: Cross-backend verification in fix agent prompts** | Partially | Fix agents now check PG/MySQL, but parity gaps still found in R6 (2), R7 (3), R8 (3). Reduces incidence but doesn't eliminate root cause. |
| **R2: Unify DML and INSERT metadata paths** | No | F-044 still deferred. R8 found 3 P1 DML/INSERT parity gaps in PG/MySQL. |
| **R3: Validate fix agent commits before marking resolved** | Yes | No lost commits in R5-R8. The R1 P0-3 / R1 P1-4 "fix lost to worktree" problem has not recurred. Merge agent workflow enforces this. |
| **R4: Downgrade Codex P0 claims by default** | Yes | Synthesis step routinely downgrades Codex P0s. ~30 false positives prevented from consuming fix capacity across R4-R8. |
| **R5: Add regression tests for fix-introduced patterns** | Partially | Fix agents add tests for new code, but R7 found 3 fix-introduced defects (R7-S-001, R7-S-002, R7-S-008), suggesting coverage is incomplete. |

### 2.2 P0/P1 Trajectory

| Metric | R1-R5 | R6-R8 | Trend |
|--------|-------|-------|-------|
| Validated P0 total | 15 | 0 | P0s exhausted |
| Validated P1 total | 58 | 34 | Stable per-cycle (~11.3 vs 11.6) |
| P0 FP rate (Codex) | 86% (6/7) | 100% (4/4) | Worsening (no real P0s to find) |
| P1 FP rate (all) | ~0% | 0% (0/34) | Stable |

**Assessment**: The P0 decline is real, not an artifact of scope narrowing. R8 had the deepest correctness review (6 parallel agents, all 35 source files) and still found zero P0s. The codebase's critical safety issues are genuinely resolved.

P1 stability is also real — each cycle expands into new areas. R6 found error handling and compaction issues. R7 found concurrency races and dead code. R8 found backend parity gaps and interop DDL mismatches.

### 2.3 Fix-Introduced Defects

| Cycle | Fix-Introduced | Details |
|-------|---------------|---------|
| R1-R3 | 1 | R3F-006 (PG/MySQL parity gap from R2 fix agents) |
| R4 | 1 | R4-S-008 (non-standard snapshot_changes tokens from R3) |
| R5 | 2 | R5-S-001 (lexicographic stats from R3), R5-S-003 (PG parity gap from R3) |
| R6 | 0 | — |
| R7 | 3 | R7-S-001 (OnceLock from R6-S-026), R7-S-002 (snapshot race from R6-S-040), R7-S-008 (dead code from R6-S-022) |
| R8 | 0 | — |
| **Total** | **7** | |

**Trajectory**: Fix-introduced defects peaked in R7 (3) due to R6's large fix surface (10 agents, 49 fixes). R8 found zero fix-introduced defects, suggesting R7's fixes were well-tested. The R7 peak is a one-cycle anomaly, not a trend.

**Pattern**: Fix-introduced defects cluster in two categories:
1. **Parity gaps** (3 of 7): Fix agent changes SQLite but not PG/MySQL. Structural problem (F-044).
2. **Incomplete new code** (4 of 7): New code added by fix agents (OnceLock, snapshot propagation, parse_values module, stats aggregation) has edge cases the fix agent didn't test for.

### 2.4 Codex FP Rate

| Cycle | Codex P0 Claims | P0 FP Rate | Total Codex FPs |
|-------|-----------------|------------|-----------------|
| R4 | 3 | 67% | 3 |
| R5 | 4 | 100% | 5 |
| R6 | 3 | 100% | 3 |
| R7 | 0 | — | 2 |
| R8 | 1 | 100% | 17 |
| **Cumulative** | **11** | **91%** | **30** |

**R6-R8 specifically**: Codex P0 FP rate remained at 100% (4/4 claims downgraded). However, R7 showed marked improvement in overall FP rate (16.7% vs 90%+ in R4-R6), suggesting the validation protocol in synthesis is filtering effectively at P1 level too. R8's 17 total FPs were driven by a 43-finding Codex review — the largest ever — with an 89% raw-to-validated FP rate.

**Codex's value proposition**: Despite high FP rates, Codex uniquely identifies issues no other agent catches. R8 Codex uniquely found: MERGE match loop analysis, DML cleanup gaps, async compaction blocking, type parsing trailing input. The breadth is worth the validation cost.

### 2.5 PG/MySQL Parity Gap

| Cycle | Parity Findings | Fix? | Recurred Next Cycle? |
|-------|----------------|------|---------------------|
| R2 | F-015 | R2 | — |
| R3 | R3F-006 (4 gaps) | R3 `d9d54ce` | Yes (R4) |
| R4 | R4-S-006 | R4 `2a51319` | Yes (R5) |
| R5 | R5-S-003 | R5 | Yes (R6) |
| R6 | R6-S-003, R6-S-004 | R6 `75ad2e1` | Yes (R7) |
| R7 | R7-S-009, R7-S-010, R7-S-011 | R7 `8500ddf` | Yes (R8) |
| R8 | R8-S-007, R8-S-008, R8-S-009 | R8 `875814e` | ? |

**This is the most predictable failure mode in the project.** Every cycle that touches the SQLite writer produces 2-3 parity gap findings in PG/MySQL. Cumulative cost: ~18 P1/P2 findings across 6 cycles, each consuming fix agent capacity.

**Status**: NOT resolved. Despite cross-backend verification in fix agent prompts (adopted in R6+), new gaps still emerge because:
1. Fix agents can verify existing methods but can't detect missing trait overrides (R8-S-008, R8-S-009)
2. The three backends have subtly different SQL dialects, making 1:1 porting non-trivial
3. Some features were never implemented in PG/MySQL from the start (partition values, snapshot changes)

### 2.6 DML Metadata Divergence

**Status**: Substantially resolved for SQLite by R5. R6-R8 DML findings are primarily PG/MySQL parity gaps (overlapping with Theme 1), not new INSERT/DML divergence in SQLite. The R1-R5 recommendation to unify the paths was never implemented, but manual fixes have closed most gaps in the SQLite backend.

### 2.7 Lost Commits

**Status**: Fully resolved. No lost commits in R5-R8. The merge agent workflow (introduced R4+) and commit verification step prevent this class of failure. The R4 incident (worktree cleaned before merge) was the last occurrence.

---

## 3. New Themes in R6-R8

### 3.1 Interop DDL Schema Mismatches (R8 — NEW)

R8 discovered a class of issues not present in R1-R7: extra columns in DDL tables that break DuckDB writes.

| Finding | Table | Extra Column | Impact |
|---------|-------|-------------|--------|
| R8-S-010 | ducklake_schema_versions | `table_id INTEGER` | DuckDB writes fail with column count mismatch |
| R8-S-011 | ducklake_data_file, ducklake_delete_file | `partial_max INTEGER` | DuckDB writes fail with column count mismatch |

These were **experimentally confirmed** — not theoretical. They represent a new finding category: our DDL schema diverges from DuckDB's expected schema in ways that break cross-engine writes. Prior cycles focused on metadata content correctness, not schema shape.

### 3.2 Concurrency Races in PostgreSQL (R7-R8 — NEW)

R7 and R8 surfaced concurrency issues specific to PostgreSQL's READ COMMITTED isolation:

| Finding | Issue |
|---------|-------|
| R7-S-002 | Snapshot ID rollback race via `store()` instead of `fetch_max()` |
| R8-S-005 | schema_version counter race (duplicate versions under concurrent DDL) |
| R8-S-006 | TOCTOU in get_or_create_schema (duplicate active schemas) |
| R8-S-030 | Conflict check gap between verification and write |

These are genuine issues but low real-world impact (concurrent DDL is rare). They represent a maturation of the review process — R1-R5 focused on single-writer correctness; R6-R8 began stress-testing concurrent scenarios.

### 3.3 Dead Code / Incomplete Wiring (R7 — NEW)

R7-S-008 revealed that R6's `parse_values.rs` module was created but never wired into production paths. The legacy `parse_string_to_array` in `table_writer.rs` was still the active code path. This is a new failure mode: fix agents create new infrastructure but leave old code in place.

### 3.4 Compaction Path Gaps (R6-R8 — NEW)

The compaction path (delegated to DuckDB) received its first deep review in R6-R8:

| Finding | Issue |
|---------|-------|
| R6-S-002 | Compaction executes at planning time, not scan time |
| R6-S-026 | INSTALL ducklake on every call |
| R8-S-001 | replace_table_files doesn't end delete files |
| R8-S-018 | replace_table_files doesn't recompute column stats |
| R8-S-040 | Synchronous DuckDB blocks async executor |

This area was effectively unreviewed in R1-R5. The R6-R8 findings are all original-code issues, not regressions.

### 3.5 Timestamp/Date Precision Issues (R8 — NEW)

R8 found multiple precision and format issues that were previously masked:

| Finding | Issue |
|---------|-------|
| R8-S-004 | Timestamp inline serialization truncates microseconds |
| R8-S-019 | Nanosecond-to-microsecond truncating division for negative timestamps |
| R8-S-023 | Date statistics stored as integers, DuckDB expects ISO strings |

These represent a subtle class of data fidelity issues that only become visible with cross-engine testing or edge-case timestamp values.

---

## 4. Process & Infrastructure

### 4.1 Agent Naming (Fixed in R7)

**Problem**: R6 agents launched with stale tmux panes from R5, causing instant context exhaustion.
**Fix**: Per-cycle naming convention (`r7-fix-correctness`, not `fix-correctness`).
**Result**: Zero recurrences in R7 or R8. This was a complete fix.

### 4.2 Worktree Isolation (Fixed in R2)

**Status**: Fully resolved since R2. The R4 worktree-cleanup incident (R4-S-018 lost) was operator error, not a worktree isolation failure. No issues in R5-R8.

### 4.3 Merge Workflow

**Status**: Working well. Key milestones:
- R6: 21 conflicts resolved across 3 branches (10 agents, largest fix surface)
- R7: Fast-forward merge (6 agents, clean)
- R8: Multi-branch merge, final commit `12548a6`

No lost commits since the merge agent was introduced in R4.

### 4.4 Agent Context Exhaustion

**Frequency**: Occasional, not tracked formally.
**Impact**: Agents with >7 findings or working on `metadata_writer_sqlite.rs` (3000+ lines) are at highest risk. The optimal range is 5-7 findings per agent.
**R6-R8 data**: R5 pushed to 9 findings/agent (highest) without failure. R7 used 3.7/agent (lowest, cleanest). R8 used 5.4/agent. No formal context exhaustion incidents in R6-R8.

### 4.5 P3 Backlog Accumulation

| Cycle | P3 Not Assigned | Cumulative (R6-R8) |
|-------|----------------|---------------------|
| R6 | 36 | 36 |
| R7 | 7 (17 fixed, 4 already resolved) | 43 |
| R8 | 53 | 96 |

**~96 P3 items from R6-R8 remain unaddressed** (plus ~25 from R3-R5). R7 was an outlier — its P3 batch was mostly handled because it had only 28 total and 17 were S-effort. Most P3 items are code quality nits, edge-case bounds checks, and test harness improvements.

### 4.6 Review Agent Counts and Costs

| Cycle | Review Agents | Fix Agents | Total Agents | P1+P2 Fixed |
|-------|--------------|------------|--------------|-------------|
| R1 | 4 | 4 | 8 | 17 |
| R2 | 5 | 10 | 15 | 40 |
| R3 | 5 | 8 | 13 | 22 |
| R4 | 5 | 8 | 13 | 33 |
| R5 | 5 | 8 | 13 | 39 |
| R6 | 5 | 10 | 15 | 52 |
| R7 | 5 | 6 | 11 | 22 |
| R8 | 5 | 8 | 13 | 43 |
| **Total** | **39** | **62** | **101** | **268** |

Average cost: **~2.7 findings fixed per agent** (including review agents in denominator), or **~4.3 P1+P2 fixed per fix agent**.

---

## 5. ROI Analysis

### 5.1 Yield per Cycle

| Metric | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 |
|--------|----|----|----|----|----|----|----|----|
| Validated P0 | 6 | 5 | 3 | 1 | 0 | 0 | 0 | 0 |
| Validated P1 | 11 | 15 | 9 | 12 | 11 | 13 | 8 | 12 |
| P1+P2 fixed | 17 | 40 | 22 | 33 | 39 | 52 | 22 | 43 |
| Fix agents used | 4 | 10 | 8 | 8 | 8 | 10 | 6 | 8 |
| **P1+P2 per fix agent** | **4.3** | **4.0** | **2.8** | **4.1** | **4.9** | **5.2** | **3.7** | **5.4** |

### 5.2 Diminishing Returns?

**P0 yield**: Clearly diminishing. P0s peaked at R1 (6) and are exhausted.

**P1 yield**: NOT diminishing. R6-R8 averaged 11.0 P1/cycle vs R1-R5's 11.6. The nature shifted from safety bugs to completeness bugs, but the yield is stable.

**P2 yield**: Actually increasing. R6-R8 averaged 27.7 P2/cycle vs R1-R5's 19.6. Deeper reviews find more moderate-impact issues.

**Overall assessment**: Full-scope reviews still produce significant yield at P1+P2 level. However, the ROI of P1 findings is declining — backend parity gaps (the dominant P1 theme) would be better addressed by implementing F-044 than by continuing to find and fix them one at a time.

### 5.3 Cost-Benefit Breakdown

| Category | Estimated Agent Cost (agents * ~1 turn) | Bugs Prevented |
|----------|----------------------------------------|----------------|
| R1-R3 (foundational) | ~36 agents | 15 P0, 35 P1 — high value |
| R4-R5 (deepening) | ~26 agents | 1 P0, 23 P1 — medium-high value |
| R6-R8 (maturation) | ~39 agents | 0 P0, 34 P1 — medium value, P1s increasingly about completeness not safety |

The highest-value reviews were R1-R3, which caught SQL injection, data loss, and atomicity violations. R6-R8 still find real bugs but the severity profile has shifted from "data corruption" to "feature gap" and "edge case handling."

---

## 6. Recurring Theme Resolution Status

| # | Theme | First Found | Status After R8 | Cycles Active |
|---|-------|-------------|-----------------|---------------|
| 1 | PG/MySQL backend parity gap | R2 | **UNRESOLVED** — root cause (F-044) still deferred | 6 of 8 |
| 2 | DML metadata path divergence | R3 | **Mostly resolved** in SQLite; PG/MySQL gaps remain | 5 of 8 |
| 3 | Inline data subsystem fragility | R1 | **Mostly resolved** — R7-R8 found last remaining issues | 5 of 8 |
| 4 | Test infrastructure masking | R1 | **Ongoing** — incremental improvements each cycle | 8 of 8 |
| 5 | Codex P0 over-reporting | R4 | **Managed** — validation protocol prevents waste | 5 of 5 |
| 6 | Checked arithmetic / defensive coding | R3 | **Improving** — diminishing count per cycle | 6 of 8 |
| 7 | Interop DDL schema mismatches | R8 | **NEW** — fixed in R8, may recur | 1 of 8 |
| 8 | PG concurrency races | R7 | **NEW** — fixed in R7-R8 | 2 of 8 |
| 9 | Compaction path gaps | R6 | **NEW** — fixed in R6-R8 | 3 of 8 |
| 10 | Timestamp/date precision | R8 | **NEW** — fixed in R8 | 1 of 8 |

---

## 7. Files Most Frequently Flagged (R1-R8)

| File | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 | Total |
|------|----|----|----|----|----|----|----|----|-------|
| `metadata_writer_sqlite.rs` | 3 | 5 | 4 | 4 | 3 | 5 | 3 | 5 | 32 |
| `table_writer.rs` | 4 | 2 | 3 | 2 | 3 | 3 | 4 | 3 | 24 |
| `metadata_writer_postgres.rs` | 0 | 2 | 1 | 2 | 2 | 3 | 2 | 6 | 18 |
| `metadata_writer_mysql.rs` | 0 | 2 | 1 | 2 | 2 | 3 | 2 | 7 | 19 |
| `insert_exec.rs` | 5 | 1 | 1 | 0 | 0 | 3 | 1 | 1 | 12 |
| `table.rs` | 0 | 0 | 0 | 1 | 2 | 2 | 1 | 5 | 11 |
| Test files | 3 | 4 | 2 | 6+ | 4+ | 10 | 4 | 7 | 40+ |

**Notable R6-R8 shift**: `metadata_writer_postgres.rs` and `metadata_writer_mysql.rs` rose significantly as the review focus expanded to backend parity. `table.rs` also emerged as a hot spot in R8 (schema mapping, partition pruning, date statistics).

---

## 8. Concrete Recommendations

### 8.1 For R9 (If Proceeding)

1. **Implement F-044 (backend deduplication) INSTEAD of another full review.** Estimated cost: 1-2 agents, L effort. Expected savings: eliminates the 2-3 parity gap findings that appear in every cycle. The cumulative cost of NOT doing F-044 (~18 parity findings across 6 cycles, each requiring fix agent work) exceeds the one-time cost of the refactor.

2. **If reviewing, narrow scope to:**
   - PG/MySQL backends only (highest remaining yield)
   - Compaction path (under-reviewed, 3 new findings in R6-R8)
   - Inlined data path on PG/MySQL (disabled but has dead code that will break when enabled)

3. **Assign top-20 P3 items.** Prioritize S-effort items from the ~96 unassigned P3 backlog. Good candidates:
   - All checked arithmetic items (R8-S-046, R8-S-058, R8-S-068, R8-S-069) — S effort, real if unlikely
   - Test duplication cleanup (R8-S-080, R6-S-074, R6-S-076) — reduces maintenance burden
   - `unwrap()` → `expect()` in production code (R8-S-071) — trivial, improves debug-ability

4. **Add a backend-parity CI check.** A script comparing trait method overrides across the three writer backends would catch missing implementations before review. Estimated cost: 1 agent, S effort.

### 8.2 If Pausing Reviews

1. **Address F-044 (provider/writer deduplication).** This is the single highest-impact structural improvement remaining. Extract shared logic into a common implementation with backend-specific SQL dialect adapters.

2. **Address F-036 (INSERT streaming).** Currently all partitions are materialized in memory. For large tables this is an OOM risk.

3. **Improve SLT pass rate.** Currently 157/254 (61.8%). With R1-R8 fixes, many previously failing tests may now pass. A focused SLT sprint could push this to 70-80%.

4. **Create a "known limitations" document** consolidating:
   - 7 deferred architectural items
   - ~120 unassigned P3 items
   - Blocked Tier 2/3 work (MERGE SQL, time travel SQL, struct field evolution)

### 8.3 Review Model Graduation

With 3 consecutive zero-P0 cycles and 100% fix rates on assigned findings, the project should **graduate from full-scope integration-branch reviews** to:

1. **PR-based reviews**: Review changed files only, triggered by PR creation. Lower cost, faster feedback loop.
2. **Targeted deep dives**: Focused reviews of specific subsystems (e.g., "review PG/MySQL concurrency" or "review compaction path") rather than whole-codebase sweeps.
3. **Regression-only reviews**: After implementing F-044 or F-036, run a focused review on the changed code.

---

## Appendix A: Deferred Items Master List

| ID | Description | Since | Effort | Why Deferred | Impact |
|----|-------------|-------|--------|--------------|--------|
| F-036 | INSERT streaming for OOM prevention | R2 | L | Requires streaming write redesign | High for large tables |
| F-044 | Provider/writer code deduplication | R2 | L | ~3000+ lines near-identical across 3 backends | Highest — eliminates parity gap theme |
| F-045 | Async trait redesign (sync→async) | R2 | L | ~60+ block_on() calls | Medium — performance |
| R4-S-018 | PG/MySQL checked write TOCTOU | R4 | S | Low real-world impact | Low — concurrent DDL rare |
| R4-S-036 | map_err boilerplate (50+ sites) | R4 | M | Relates to F-044 | Low — code quality |
| R4-S-040 | Monolithic execute() blocks | R4 | L | Relates to F-044 | Low — code quality |
| R6-S-017 | Concurrent DML lost-delete race | R6 | M | Architectural — per-file locking | Medium — concurrent DML |

## Appendix B: Cumulative Statistics

| Metric | Value |
|--------|-------|
| Total raw findings | 661 |
| Total deduplicated | 501 |
| Total fixed | ~335 (67% overall, 98%+ of assigned P0-P2) |
| Total P3 unassigned (R6-R8) | ~96 |
| Total deferred (architectural) | 7 |
| Codex false positives identified | ~30 |
| Review agents spawned | ~39 (5 per cycle x ~8 cycles) |
| Fix agents spawned | ~62 |
| Total agents | ~101 |
| Fix-introduced defects | 7 (across all cycles) |
| Lost commits | 2 (R1 only, resolved by R4) |
| Test count | ~725 → 770+ |
| Cross-engine tests | 72+ |
| SLT pass rate | 157/254 (61.8%, stable) |
| P0-free streak | 3 cycles (R6-R8) |

## Appendix C: Review Cycle Boundaries (R6-R8)

| Cycle | Review Date | Fix Agents | Fix Commits | Final Merge |
|-------|------------|------------|-------------|-------------|
| R6 | 2026-03-04 | 10 | `d3aa034`, `75ad2e1`, `aaf5a4f`, `07cd101`, `f93444c`, `b8a4476`, `f4c0f58`, `03f9cb3`, `5666cf5`, `08ff2f7`, `c9c761b`, `d6a5104` | `4f9cc49` |
| R7 | 2026-03-04 | 6 | `843096a`, `f5f0a2d`, `8500ddf`, `6f611c5`, `448845f`, `9735a62`, `1d7c9d2`, `368ec69` | `6f611c5` |
| R8 | 2026-03-05 | 8 | `ed9c86c`, `68a14dd`, `b59edb1`, `71da28c`, `875814e`, `c6a62b8`, `91279ec`, `4687f5d` | `12548a6` |
