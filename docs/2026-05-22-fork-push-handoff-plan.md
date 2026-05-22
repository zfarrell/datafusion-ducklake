# Fork Push + Context Handoff Plan (Evidence-Based)

Date: 2026-05-22
Repo: `datafusion-ducklake`
Audited branch: `ducklake-features/integration`

## Scope

This document reconciles:
- high-signal docs (`remaining-work-audit`, `R9-R11 retrospective`, `snapshot-awareness audit`, `project-status`)
- actual branch graph and commit ancestry
- direct code evidence for key risk claims

Goal: define exactly what to push to `https://github.com/zfarrell/datafusion-ducklake` and how to hand off context safely.

## Evidence Summary

### 1) Documentation quality and freshness

- `docs/INDEX.md` is stale as an index and should not be treated as source-of-truth.
  - It claims Feb 28 updates while key docs were updated through Mar 7.
- `docs/project-status.md` is broad and useful, but dated Mar 1; it predates late-cycle fixes.
- `docs/remaining-work-audit.md` and `docs/2026-03-07-retrospective-r9-r11.md` capture late-cycle history and are better planning inputs.
- `docs/2026-03-07-snapshot-awareness-audit.md` is high-signal and directly tied to concrete code changes.

### 2) Branch state (actual Git evidence)

- Current branch: `ducklake-features/integration`
- HEAD: `8f38748`
- Local branches: 47
- Branches not merged into `ducklake-features/integration`: 13

`git cherry` vs `ducklake-features/integration` shows:
- Already represented in integration (`-`):  
  `ducklake-features/all-work-backup`, `ducklake-features/postgres-writer`, `fix/validate-column-def-types`, `fix/zero-column-tables`
- Unique relative to integration (`+`):  
  `ducklake-features/phase1-docs`, `ducklake-features/virtual-columns`, `fix/missing-delete-file-error`, `fix/name-validation`, `fix/type-normalization-promotion`, `fix/type-roundtrip-precision`, `fix/validate-record-count`, `fix/validate-type-strings`, `fixup/review-52`

### 3) Code-level verification of high-risk claims

- Snapshot-awareness fixes are present in provider/query paths:
  - `src/metadata_provider_impl.rs` includes temporal predicates for column/table lookups and `list_all_columns`.
  - `src/table.rs` and `src/table_functions.rs` pass `snapshot_id` through metadata lookups.
- Append/write atomicity paths are present:
  - `src/table_writer.rs` uses `begin_write_transaction`, `commit_uploaded_files`, `cleanup_uploaded_files`.
  - `src/metadata_writer_impl.rs` implements `append_table_files` with overflow checks.
- PostgreSQL DDL `FOR UPDATE` regression appears addressed:
  - `src/metadata_writer_impl.rs` macro now locks via:
    `SELECT schema_version FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1{for_update}`
    (no invalid aggregate + `FOR UPDATE` combination).

### 4) Test inventory signal

- Current tree contains 817 `#[test]` / `#[tokio::test]` markers (`src/` + `tests/` scan).
- `tests/` directory contains 68 files.
- This is broadly consistent with docs claiming ~811+ tests and late-cycle additions.

## Recommended Push Plan

Use a two-tier push: `core` first, then `optional exploration branches`.

### Tier 1 (must push)

Push only the integration branch as primary exploration baseline:
- `ducklake-features/integration`

Reason:
- Contains the merged R9-R11 body of work and snapshot audit follow-ups.
- Minimizes noise and branch sprawl while preserving most meaningful history.

### Tier 2 (optional, if you want extra historical threads)

Push unique-but-not-merged branches that may contain exploratory or superseded variants:
- `ducklake-features/phase1-docs`
- `ducklake-features/virtual-columns`
- `fix/missing-delete-file-error`
- `fix/name-validation`
- `fix/type-normalization-promotion`
- `fix/type-roundtrip-precision`
- `fix/validate-record-count`
- `fix/validate-type-strings`
- `fixup/review-52`

Notes:
- Some of these overlap in intent with integration, but not as exact cherry-equivalent commits.
- Push them only if you want forensic/history preservation in your fork.

## Exact Commands

Run from repo root (`/home/zac/datafusion-ducklake`) in a network-enabled shell.

```bash
# 0) sanity
git status --short --branch
git fetch origin --prune

# 1) ensure fork remote exists
git remote get-url fork >/dev/null 2>&1 || \
  git remote add fork https://github.com/zfarrell/datafusion-ducklake.git

# 2) push core branch
git push -u fork ducklake-features/integration

# 3) optional: push additional unique branches
for b in \
  ducklake-features/phase1-docs \
  ducklake-features/virtual-columns \
  fix/missing-delete-file-error \
  fix/name-validation \
  fix/type-normalization-promotion \
  fix/type-roundtrip-precision \
  fix/validate-record-count \
  fix/validate-type-strings \
  fixup/review-52
do
  git push -u fork "$b"
done
```

## Context Handoff Package

For a new collaborator/agent, point them to these files in this order:

1. `docs/2026-05-22-fork-push-handoff-plan.md` (this file)
2. `docs/remaining-work-audit.md`
3. `docs/2026-03-07-retrospective-r9-r11.md`
4. `docs/2026-03-07-snapshot-awareness-audit.md`
5. `docs/project-status.md`
6. `docs/pr-strategy-final.md` (historical strategy context, not authoritative execution state)

Suggested handoff note:
- Primary working branch in fork: `ducklake-features/integration`
- Other pushed branches are archival/exploratory only
- Treat `docs/INDEX.md` as stale

## Risk Notes

- Untracked local files currently exist under `.claude/worktrees/` and many dated docs; these are not pushed unless committed.
- This environment cannot access GitHub, so push commands must be run in your network-enabled shell/session.
- No destructive history rewrite is recommended; preserve branch history for bisectability.
