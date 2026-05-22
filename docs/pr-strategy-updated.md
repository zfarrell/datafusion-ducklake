# PR Strategy: Integration Branch -> Main (Updated 2026-03-07)

## What Changed From the Original Plan

The original `ducklake-pr-strategy.md` covered only the **security review fixes** (issues #52-#64). All 9 PRs from that plan (#65-#75, #78) are **MERGED** or **CLOSED**. That strategy is fully complete.

This document addresses the **real remaining challenge**: merging the `ducklake-features/integration` branch (264 commits, 217 files, +84k/-4k lines) into `main`.

### Key Differences From Original Strategy

| Aspect | Original Strategy | Updated Reality |
|--------|-------------------|-----------------|
| Scope | 13 security issues | Full feature branch (write support, DDL, multi-backend, F-044 dedup) |
| Approach | Re-implement each fix against main | Cannot re-implement — too interconnected, too large |
| Cherry-pick feasibility | Tested and rejected | Same — features are deeply coupled |
| PR count | 9 small PRs | 5-6 larger PRs (see below) |
| Review confidence | Low (new code) | High (11 review cycles, 335+ findings fixed, 811 tests) |

### Why the Original "Re-implement" Approach Won't Work Here

The security fixes were 5-50 line changes in isolated functions. The feature branch has:
- 15 new source files (entire subsystems: write path, query planner, compaction, CDC)
- 37 source files modified (every major module touched)
- 73 test files (67 new)
- Deep cross-cutting concerns (F-044 dedup restructured all 4 providers + all 3 writers)

Re-implementing would mean rewriting the entire project. Instead, we split the integration branch into ordered PRs.

---

## Prerequisites

### Step 0: Merge Open PRs Against Main

Three PRs from earlier bug-fix work are already open against `main`:

| PR | Title | Status | Lines |
|----|-------|--------|-------|
| #80 | fix: validate record_count metadata | OPEN | +87/-2 |
| #81 | fix: validate catalog entity names | OPEN | +224/-11 |
| #82 | fix: normalize type aliases and promotion rules | OPEN | +359/-2 |
| #83 | Add Discord link to README | OPEN (external) | +2/-0 |

**Action**: Merge #80, #81, #82 (and #83 if desired), then rebase `ducklake-features/integration` onto new `main`. This avoids conflicts since integration already has equivalent fixes.

After rebase, the integration diff should be ~80k+ lines (minus the ~670 lines from those 3 PRs that are already implemented differently on integration).

---

## PR Splitting Strategy

Since `main` has not diverged (merge-base = main HEAD), we can create PRs by branching from main and progressively merging subsets. Each PR builds on the prior one.

**Approach**: Create stacked PRs where each PR's branch is based on the prior PR's branch. Merge in order.

### PR A: Read Path Enhancements (~8k lines, 12 src files)

**Title**: `feat: enhanced read path — virtual columns, complex types, parse values, encryption`

**Scope**: Read-only improvements that don't require write support.

**Source files (new)**:
- `src/virtual_column_exec.rs` — Virtual column execution (filename, file_row_number, rowid)
- `src/parse_values.rs` — String-to-Arrow parsing for inlined data

**Source files (modified)**:
- `src/types.rs` — Complex type parsing (List/Struct/Map), decimal improvements
- `src/path_resolver.rs` — Path resolution improvements
- `src/delete_filter.rs` — MOR delete filtering improvements
- `src/encryption.rs` — PME read enhancements
- `src/table.rs` — Virtual column support, filter pushdown, scan improvements (READ-ONLY changes)
- `src/catalog.rs` — Dynamic lookup improvements (READ-ONLY changes)
- `src/metadata_provider.rs` — New trait methods for read path
- `src/metadata_provider_duckdb.rs` — New read-path methods
- `src/information_schema.rs` — Extended queryable catalog
- `src/table_changes.rs` — CDC read improvements
- `src/table_deletions.rs` — Deletion tracking improvements
- `src/lib.rs` — Module declarations, re-exports

**Infrastructure**:
- `Cargo.toml` — Add `chrono` dependency
- `.gitignore`, `.githooks/pre-commit`

**Test files**: ~15 files covering read-path features (virtual_column_tests, delete_filter_tests changes, encryption tests, type tests, information_schema_test changes, file_pruning_tests, deep_edge_case_tests, adversarial tests related to read path)

**Dependencies**: None (first PR)

---

### PR B: Write Foundation — INSERT + DDL (~18k lines, 10 src files)

**Title**: `feat: write support — INSERT, CREATE/DROP TABLE/SCHEMA, ALTER TABLE`

**Scope**: The write path foundation: table writer, INSERT execution, DDL operations, query planner.

**Source files (new)**:
- `src/insert_exec.rs` — DuckLakeInsertExec with partition routing
- `src/table_writer.rs` — High-level write API with atomicity
- `src/query_planner.rs` — DuckLakeQueryPlanner (routes DML)
- `src/metadata_writer.rs` — MetadataWriter trait (expanded)
- `src/metadata_writer_validation.rs` — DDL/DML validation helpers
- `src/table_insertions.rs` — Insertion tracking

**Source files (modified)**:
- `src/schema.rs` — DDL: CREATE TABLE, ALTER TABLE, DROP TABLE/SCHEMA
- `src/table.rs` — DML routing (insert/delete/update methods)
- `src/table_functions.rs` — New table functions (snapshots, flush, etc.)
- `Cargo.toml` — `write` feature flag, `uuid` dependency, `write-sqlite` feature

**Test files**: ~15 files (write_tests, alter_table_tests, create_schema_tests, drop_and_constraints_tests, write_partition_tests, conflict_detection_tests, stats_tests, table_function_tests, time_travel_tests)

**Dependencies**: PR A

---

### PR C: SQLite MetadataWriter (~6k lines, 1 src file)

**Title**: `feat: SQLite metadata writer for write operations`

**Scope**: The SQLite backend implementation of MetadataWriter.

**Source files**:
- `src/metadata_writer_sqlite.rs` — Full SQLite write backend with SQLITE_BUSY retry

**Test files**: Most write tests use SQLite backend so they'd already be in PR B. Additional edge case tests here.

**Dependencies**: PR B

**Note**: This is the primary/default write backend. Without this, write tests can't run. Consider merging B+C together if the combined size is manageable (~24k lines). Alternatively, PR B could include just enough of the SQLite writer to make tests pass.

**Recommendation**: **Merge PR B and PR C together** as a single PR. The write foundation without a backend is untestable. Combined: ~24k lines.

---

### PR D: DELETE, UPDATE, MERGE (~5k lines, 4 src files)

**Title**: `feat: DELETE, UPDATE, and MERGE DML operations`

**Scope**: The remaining DML operations beyond INSERT.

**Source files (new)**:
- `src/delete_exec.rs` — DuckLakeDeleteExec (MOR pattern)
- `src/update_exec.rs` — DuckLakeUpdateExec (copy-on-write)
- `src/merge_exec.rs` — DuckLakeMergeExec (MATCHED/NOT MATCHED)
- `src/cdc_common.rs` — Shared CDC utilities

**Test files**: ~8 files (delete_tests, update_tests, merge_tests, table_changes_tests changes, cross_engine_dml_tests)

**Dependencies**: PR B+C (needs write foundation + SQLite writer)

---

### PR E: Multi-Backend Writers + F-044 Code Dedup (~10k lines, 7 src files)

**Title**: `feat: PostgreSQL and MySQL write backends with provider deduplication`

**Scope**: PG/MySQL writers + the F-044 deduplication refactor (dialect trait + shared impls).

**Source files (new)**:
- `src/metadata_writer_postgres.rs` — PostgreSQL write backend
- `src/metadata_writer_mysql.rs` — MySQL write backend
- `src/dialect.rs` — SQL dialect trait (F-044)
- `src/metadata_provider_impl.rs` — Shared provider implementation (F-044)
- `src/metadata_writer_impl.rs` — Shared writer implementation (F-044)

**Source files (modified)**:
- `src/metadata_provider_sqlite.rs` — Refactored to use shared impl
- `src/metadata_provider_postgres.rs` — Refactored to use shared impl
- `src/metadata_provider_mysql.rs` — Refactored to use shared impl
- `Cargo.toml` — `write-postgres`, `write-mysql` feature flags

**Test files**: ~8 files (cross_engine_postgres_tests, cross_engine_mysql_tests, hybrid_asyncdb changes, cross_engine tests using PG/MySQL)

**Dependencies**: PR D (needs full DML support for cross-engine testing)

---

### PR F: Remaining Tests, SLT, Compaction, Cross-Engine (~20k lines, ~30 test files)

**Title**: `feat: compaction, expanded test suite, and SLT improvements`

**Scope**: Compaction functions, remaining cross-engine tests, adversarial tests, issue reproduction tests, SLT test files, view tests.

**Source files (new)**:
- `src/compaction_functions.rs` — merge_adjacent_files, rewrite_data_files, expire_snapshots

**Test files**: All remaining test files not covered above (~30 files including adversarial_*, issue_repro_*, cross_engine_*, edge_case_*, view_tests, compaction_tests, interop_type_tests, SLT test files)

**Test data**: All Parquet test fixtures (`${DATA_PATH}/*.parquet`)

**Dependencies**: PR E (needs all backends for cross-engine tests)

---

## Summary Table

| PR | Title | Est. Lines | Src Files | Test Files | Depends On |
|----|-------|-----------|-----------|------------|------------|
| 0 | Merge #80, #81, #82 + rebase | — | — | — | — |
| A | Read path enhancements | ~8k | 12 | ~15 | — |
| B+C | Write foundation + SQLite writer | ~24k | 11 | ~15 | A |
| D | DELETE, UPDATE, MERGE | ~5k | 4 | ~8 | B+C |
| E | Multi-backend writers + F-044 dedup | ~10k | 8 | ~8 | D |
| F | Compaction, tests, SLT | ~20k | 1 | ~30 | E |

**Total**: ~67k lines across 5 PRs (some overlap with line counts due to shared modifications).

---

## Alternative: 2-PR Approach (Recommended if Team Has Limited Review Bandwidth)

Given that the code has been through **11 review cycles** with **335+ findings fixed** and has **811 passing tests**, the incremental review value of splitting into 5 PRs is lower than for fresh code. A simpler split:

### Alt-PR 1: Core Features (~45k lines)
All source files, core test files. Everything needed for a working system.

### Alt-PR 2: Extended Test Suite (~25k lines)
Adversarial tests, issue reproduction tests, deep edge case tests, remaining SLT improvements, compaction tests.

This reduces coordination overhead while still keeping the test suite separate for easier review.

---

## Alternative: Single PR (Pragmatic)

If the team is comfortable with large PRs (this code has been extensively reviewed):

**Title**: `feat: full DuckLake write support, multi-backend, DDL, DML, 800+ tests`

**Justification**:
- 11 review cycles completed (R1-R11), 460+ findings fixed
- 811 tests passing, 158/254 SLT passing
- F-044 dedup removed 4,137 lines of duplication
- Snapshot-awareness audit completed
- The interdependencies make clean splitting expensive

**Risk**: Hard to review 84k lines in one PR. But the review has already happened across 11 cycles.

---

## Lessons Learned From Merged PRs

### Security Fix PRs (#65-#78): What Worked
1. **Re-implement against main** was the right call for small, isolated fixes
2. **Grouping by logical unit** (e.g., #54+#55 path traversal + null bytes) reduced PR count without losing clarity
3. **5 review waves** caught real issues (UTF-8 regression, cast inconsistency) — don't skip review
4. **CI flakiness** (DuckDB autoloading race) was a recurring blocker — plan for re-runs

### What Won't Work For the Feature Branch
1. **Cherry-picking** was already proven infeasible for the security fixes — it's even more infeasible at 264 commits
2. **Re-implementing** 84k lines against main would be rewriting the entire project
3. **Splitting by commit** doesn't work because commits aren't cleanly grouped by feature — review fixes are interleaved with feature work

### Recommended Next Steps
1. Decide on 5-PR, 2-PR, or 1-PR approach based on team's review bandwidth
2. Merge open PRs #80, #81, #82 against main
3. Rebase integration onto updated main
4. Create first PR (or single PR) and request review
5. For stacked PRs: use `gh pr create --base <prior-branch>` pattern

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Rebase conflicts after merging #80-#82 | Medium | Integration has equivalent fixes; resolve manually |
| CI failures on large PR | Medium | Tests pass locally (811/811); CI may need multiple runs for DuckDB flakiness |
| Reviewer fatigue on large PR | High | Provide detailed PR description; point to review cycle docs |
| Stacked PR merge conflicts | Medium | Merge in strict order; rebase each onto prior |
| Split introduces broken intermediate state | Low | Each PR should compile and pass its own tests |
