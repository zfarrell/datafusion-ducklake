# PR Strategy: Ship the Integration Branch

## The Situation

- **264 commits** on `ducklake-features/integration` ahead of `main`
- **84,450 lines added**, 4,176 removed across **217 files**
- **37 source files** (15 new, 22 modified), **86 test files** (47 new), **57 docs** (all new)
- **11 review cycles** with ~500+ findings fixed
- **811 tests passing**, 3 known failures (pre-existing DuckDB bug)
- Main is fully contained in integration (no divergence)
- Code has been through more review than most open-source projects ever get

## Recommendation: Two-PR Approach

### Why Not Many Small PRs?

The previous PR strategy (`ducklake-pr-strategy.md`) proposed re-implementing fixes one at a time against main. That worked for small bug fixes (#78, #79), but the integration branch isn't a collection of independent fixes — it's a **cohesive rewrite**. The code is deeply interconnected:

- `dialect.rs` + `metadata_provider_impl.rs` + `metadata_writer_impl.rs` are a deduplication refactor (F-044) that touches ALL metadata providers/writers simultaneously
- The write path (`delete_exec.rs`, `update_exec.rs`, `merge_exec.rs`, `query_planner.rs`) depends on `table.rs`, `table_writer.rs`, `metadata_writer.rs` changes
- Virtual columns (`virtual_column_exec.rs`) thread through `table.rs`, `delete_filter.rs`, `table_functions.rs`
- Every existing file has been modified to integrate with new features

**Splitting into 10+ PRs would mean:**
- Each PR would break compilation until subsequent PRs land
- Re-implementing on branches off main would lose 11 review cycles of battle-tested fixes
- Cherry-picking produces conflicts (already proven in `ducklake-pr-strategy.md`)
- Enormous coordination overhead for marginal review benefit

### Why Two PRs?

**PR 1: Core implementation** — all `src/`, `tests/`, `Cargo.toml`, `build.rs`, `CLAUDE.md`, `.gitignore`, `examples/`
**PR 2: Documentation** — all `docs/`

This split is natural because:
1. **Docs are 100% independent** — zero impact on compilation or tests
2. **The code is a single coherent unit** — you can't meaningfully use `delete_exec.rs` without `query_planner.rs` without `table.rs` changes
3. **The code has already been reviewed** — 11 cycles, 500+ findings fixed, 4-agent review teams with correctness/interop/idiomatic/test-harness specialists
4. **Feature flags gate everything** — the read path works without `write`, write needs `write-sqlite`/`write-postgres`/`write-mysql`. Existing users see zero behavior change unless they opt in.

---

## Concrete Plan

### PR 1: Full Read/Write DuckLake Implementation (the big one)

**Title:** `feat: complete DuckLake read/write implementation with multi-backend support`

**What's in it:**
- 37 source files (23.3k lines of src changes)
- 86 test files (41.2k lines of test changes)
- `Cargo.toml`, `Cargo.lock`, `build.rs`, `.gitignore`, `.githooks/`, `examples/`, `benchmark/`
- `CLAUDE.md` (project documentation for contributors)

**What's NOT in it:**
- Review cycle docs (`docs/2026-03-*`)
- Process docs (`docs/handoff-prompt.md`, `docs/project-status.md`, etc.)
- Issue tracker database (`.ducklake-issue-tracker.db`)

**PR description structure:**
1. Executive summary (what DuckLake is, what this adds)
2. Feature matrix table (read/write/DDL/DML/virtual columns/etc.)
3. Architecture overview (one paragraph per layer)
4. How feature flags work (existing users unaffected)
5. Test coverage summary (811 tests, 72+ cross-engine, 158/254 SLT)
6. Review history note (11 cycles, 500+ findings fixed)
7. How to test locally

**Review approach:**
- The PR body IS the review guide — structured to let reviewers understand the architecture quickly
- Point reviewers to key files: `dialect.rs` (pattern), `table.rs` (integration point), `query_planner.rs` (routing)
- Tests ARE the specification — 811 tests document expected behavior better than any description
- Offer to walk through any section on request

### PR 2: Project Documentation

**Title:** `docs: add implementation guide, review reports, and project documentation`

**What's in it:**
- `docs/implementation-guide.md` — developer guide
- `docs/testing-strategy.md` — testing approach
- `docs/EXCLUDED_TESTS.md` — why certain tests are skipped
- `docs/duckdb-behavior-reference.md` — DuckDB parity notes
- Review cycle reports (historical record)
- Other process docs

**This can land before, after, or simultaneously with PR 1.**

---

## Why This Is Actually Fine

### "But big PRs are bad!"

The usual argument against big PRs:
1. **Hard to review** — True for unreviewed code. This code has had 11 review cycles with specialized agents examining correctness, idiomatic Rust, cross-engine interop, and test coverage. The review has already happened.
2. **Hard to revert** — Feature flags solve this. If write support has a bug, users simply don't enable `--features write-sqlite`. The read path is backward-compatible.
3. **Hard to bisect** — The 264 commits are preserved in the branch. `git bisect` works within the merge.
4. **Risky** — 811 tests, 72+ cross-engine interop tests, and 158 SLT tests mitigate this. The code is better tested than most projects' entire codebases.

### "Shouldn't you squash?"

**No.** The 264 commits contain useful history: feature additions, bug fixes, review cycle responses. A merge commit (not squash) preserves this for bisection and blame. The merge commit message can summarize everything.

### "What about the 3 open PRs (#80, #81, #82)?"

- **#82** (fix: type normalization) — Already on integration branch. Close it after PR 1 merges.
- **#81** (fix: name validation) — Already on integration branch. Close it after PR 1 merges.
- **#80** (fix: validate record_count) — Already on integration branch. Close it after PR 1 merges.
- **#83** (Discord README) — Independent, can merge whenever.

---

## Execution Steps

1. **Prepare PR 1:**
   - Exclude docs/, `.ducklake-issue-tracker.db`, and `.claude/` from the diff
   - Write a thorough PR description (the description IS the review)
   - Create the PR: `ducklake-features/integration` → `main`

2. **Prepare PR 2:**
   - Branch from PR 1's result
   - Add docs/ directory
   - Minimal PR description

3. **Review process:**
   - Self-review the PR description for accuracy
   - Run full test suite one final time and paste results in PR
   - If maintainers want deeper review of specific areas, point them to the relevant review cycle docs

4. **Merge:**
   - Merge commit (not squash) to preserve history
   - Tag a release (v0.1.0?) since this is a major capability addition

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Reviewer overwhelmed by diff size | Medium | Excellent PR description + review already done + feature flags |
| CI fails on main | Low | 811 tests pass locally, compilation verified |
| Hidden regression | Low | 11 review cycles, 72+ cross-engine tests |
| Merge conflicts with #83 | Very Low | #83 only touches README |
| Feature-flagged code has dead code warnings | Low | Already handled with `#[cfg(...)]` and `#[allow(dead_code)]` |

## Alternative Considered: Three PRs (Read / Write / Tests+Docs)

I considered splitting into read-path and write-path PRs. The problem:
- `table.rs` has both read and write changes interleaved (1,293 lines changed)
- `metadata_writer_sqlite.rs` is 2,425 lines changed — it's the write path's backbone and can't be split from the write executors
- The F-044 deduplication (`dialect.rs`, `*_impl.rs`) touches both read and write metadata layers simultaneously
- You'd end up with one PR that doesn't compile until the other lands

**Bottom line: the code is one unit. Ship it as one unit.**

---

## Summary

| Approach | PRs | Risk | Overhead | Review burden |
|----------|-----|------|----------|---------------|
| Many small PRs (re-implement) | 10-15 | HIGH (bugs, lost fixes) | Very High | Redundant (already reviewed) |
| Split by layer | 3-5 | HIGH (won't compile independently) | High | Moderate |
| **Two PRs (code + docs)** | **2** | **Low** | **Low** | **Low (already reviewed)** |
| Single mega-PR | 1 | Low | Lowest | Low (already reviewed) |

**The two-PR approach balances pragmatism with organization. The code is one coherent unit that's been reviewed 11 times. Ship it.**
