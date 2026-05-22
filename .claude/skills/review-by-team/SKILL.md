---
name: review-by-team
description: Spawn a team of agents to perform a deep code review of the integration branch
argument-hint: "[focus-area]"
---

# Deep Code Review

Perform a thorough, multi-agent code review of the current  branch. The user may optionally specify a focus area as `$ARGUMENTS` (e.g., "write path", "partitioning", "inlining"). If no focus is given, review all recent work.

## Critical Rules

1. **ALL work must be done by team agents.** Use `TeamCreate` first, then spawn agents with `Task` using `team_name`. NEVER use plain subagents.
2. **No AI attribution** in any commits.
3. **Each agent must run `/home/zac/.cargo/bin/cargo clean` before finishing.**
4. All review findings should be documented (e.g. docs/review/2026-03-01-performance.md or docs/review/2026-03-01-gap-analysis.md, etc). 

## Step 1: Create a team

```
TeamCreate: team_name="code-review-YYYY-MM-DD", description="Deep code review"
```

## Step 2: Spawn 5 review agents in parallel

Each agent works in `isolation: "worktree"`, runs in background, and checks out the current branch.

**IMPORTANT — Unique agent names per cycle**: Prefix all agent names with the cycle identifier (e.g., `r7-idiomatic-review`, `r7-correctness-review`). Reusing generic names like `idiomatic-review` across cycles causes tmux pane reuse, where the new agent inherits a stale session with exhausted context. This leads to agents launching at ~10% context remaining.

Each agent should also use the `codex` CLI (at `/usr/local/bin/codex`) to get a second opinion on key files:
```
codex --quiet --approval-mode full-auto "<review prompt>" -f <file1> -f <file2>
```

### Agent 1: `idiomatic-review`
**Focus**: Rust idioms, DataFusion API usage, DuckLake patterns, code organization.
- Error handling (Result/Option, ? operator, no unwrap in non-test code)
- Ownership/borrowing, lifetime usage, trait design
- DataFusion APIs (SessionContext, TableProvider, ExecutionPlan, Arrow arrays)
- Consistency with existing codebase patterns
- Performance (unnecessary allocations, clone abuse)
- Use codex on the most critical source files

**Output**: `docs/YYYY-MM-DD-review-idiomatic.md`

### Agent 2: `correctness-review`
**Focus**: Logic bugs, edge cases, data integrity, security.
- Off-by-one errors, incorrect conditionals, missing match arms
- NULL handling, empty inputs, boundary values, overflow
- Race conditions, deadlocks, resource leaks
- Error propagation (swallowed errors, wrong error types)
- Data integrity (silent corruption, lost writes, double-counting)
- SQL injection (parameterized vs interpolated queries)
- Snapshot isolation correctness
- Use codex on write path and metadata writer files

**Output**: `docs/YYYY-MM-DD-review-correctness.md`

### Agent 3: `interop-review`
**Focus**: DuckLake model compliance, cross-engine compatibility.
- Schema compatibility: do our catalog tables match DuckDB's DuckLake schema?
- No DF-specific schema extensions that DuckDB can't read
- Hive directory layouts match DuckDB expectations
- Inline data format compatible with DuckDB
- Cross-engine test coverage: DF->DuckDB and DuckDB->DF both tested?
- Compare our DDL against a real DuckDB DuckLake catalog (use `duckdb` CLI)
- Use codex on metadata writer files

**Output**: `docs/YYYY-MM-DD-review-interop.md`

### Agent 4: `test-harness-review`
**Focus**: Test infrastructure correctness, false positives, coverage gaps.
- SLT adapter routing logic (hybrid_asyncdb.rs)
- Cross-engine test assertions (are they strong enough?)
- False positive risks (zip truncation, substring matching, sort masking)
- Coverage gaps (missing test scenarios)
- Test helper duplication
- Silent skip/degradation issues
- Use codex on test files

**Output**: `docs/YYYY-MM-DD-review-test-harness.md`

### Agent 5: `codex-review`
**Focus**: General purpose review done entirely by codex. 
Codex P0 false positive rate is 86% (6/7). After codex completes it's review the agent must validate any p0 or p1 claims and adjust the report with that evidence. 

**Output**: `docs/YYYY-MM-DD-codex-review.md`

## Step 3: Wait for all review agents to complete

Shut down each agent as it finishes. Track progress.

## Step 4: Spawn synthesis agent

After all reviews complete, spawn a `synthesis` agent that:
1. Reads all review documents
2. Deduplicates findings (same issue in multiple reviews = 1 finding)
3. Prioritizes into tiers:
   - **P0**: Data corruption, data loss, security vulnerabilities
   - **P1**: Correctness bugs affecting users, interop breakage
   - **P2**: Performance, code quality, test coverage gaps
   - **P3**: Nits, style, nice-to-haves
   - **Codex P0 validation**: Codex review agent P0 claims must be validated against source code before accepting. Codex has an 86% P0 false positive rate (6/7 across R4-R5) — it consistently flags potential data-loss scenarios without verifying transactional safety. Default Codex P0 claims to P1-candidate; only upgrade to P0 after confirming the code is NOT within a transaction boundary.
4. For each finding: unique ID, source review(s), description, affected files, suggested fix, estimated effort (S/M/L)
5. Groups related fixes into agent-sized chunks with recommended agent assignments
6. Updates `docs/remaining-work-audit.md` with new findings
7. Updates `docs/handoff-prompt.md` (both worktree and main repo copies)

**Output**: `docs/YYYY-MM-DD-review-synthesis.md`

## Step 5: Report to user

Present the synthesis results:
- Total findings by priority
- Recommended fix agents (count and descriptions)
- Ask user if they want to spin up a fix team

## Step 6: If user approves, spin up fix team

Create a new team and spawn fix agents as recommended by the synthesis. Typical groupings:
- **Write atomicity/safety** — transaction model, commit ordering
- **Input validation** — SQL injection, encoding, error propagation
- **Inline data safety** — data loss prevention, path correctness
- **Test infrastructure** — false positives, coverage gaps, helper dedup

Each fix agent should:
- Read the synthesis doc for its assigned findings
- **Reproduce first**: Before fixing a bug, write a test that reproduces it (when applicable). For bugs relevant to multiple metadata backends, write tests for all engines (SQLite, Postgres, MySQL). Verify the test fails, then fix the bug and verify the test passes.
- Run full test suite
- Commit (no AI attribution)
- **Cross-backend verification**: For any change to `metadata_writer_sqlite.rs`, verify the corresponding change exists in `metadata_writer_postgres.rs` and `metadata_writer_mysql.rs`. This is the single most predictable failure mode across review cycles — SQLite-only fixes have produced P1 findings in 3 consecutive cycles.
- **Regression tests required**: Add or extend tests covering any new code introduced by fixes, particularly for interop-sensitive changes (snapshot_changes format, stats aggregation, metadata field population). Fix-introduced regressions have been increasing across cycles.
- Run `cargo clean` before finishing
- **Report worktree branch name and commit hashes** to team lead when done. Do NOT merge into integration — the merge agent handles this.

## Step 7: Merge agent

After all fix agents complete, spawn a **merge agent** that is NOT in a worktree (it works directly on the integration branch in the main repo).

The merge agent:
1. Collects all worktree branch names reported by fix agents
2. Merges each branch into `ducklake-features/integration` one at a time
3. Resolves merge conflicts (or reports back to team lead if non-trivial)
4. After all merges, runs `cargo build --features write-sqlite && cargo test --features write-sqlite` to verify nothing broke
5. Verifies every claimed fix commit is reachable from integration HEAD via `git log`
6. Reports to team lead: branches merged, final commit hash on integration, any issues

**Important**: Do NOT clean up worktrees or the team until the merge agent confirms all branches have been successfully merged and verified.

## Step 8: Final docs update

After the merge agent confirms success, spawn one more agent to:
- Update synthesis doc with resolution status
- Update `remaining-work-audit.md`
- Update `handoff-prompt.md`
- Update `project-status.md` with new test counts

## Step 9: Cleanup

Only after the merge agent has confirmed success and docs are updated:
- Clean up worktrees
- `TeamDelete`
