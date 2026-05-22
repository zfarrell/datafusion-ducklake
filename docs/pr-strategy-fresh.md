# PR Strategy: ducklake-features/integration -> main

## Overview

The `ducklake-features/integration` branch contains **264 commits** with **84,450 insertions** across **217 files** (37 src files, 86 test files, 57 docs, misc). This represents a massive feature development effort that added write support, multiple metadata backends, query planning, DDL, DML, and extensive testing.

### Approach: Squash-merge themed PRs from cherry-picked/rebased topic branches

**Why not cherry-pick individual commits?** The 264 commits are interleaved (fixes reference earlier features, refactors touch everything). Individual commits won't compile independently.

**Why not one giant PR?** 84k lines is unreviewable.

**Recommended approach:**
1. Create topic branches off `main` for each PR
2. For each PR, use `git checkout main && git checkout -b pr/topic` then selectively apply the relevant subset of files from the integration branch
3. Use `git checkout ducklake-features/integration -- <files>` to pull specific files, then adjust for compilation
4. Each PR must compile and pass tests independently
5. Squash-merge each PR into main

### Key insight: Feature flags make this possible

The codebase is heavily feature-gated. Write support (`write`), metadata backends (`metadata-sqlite`, `metadata-postgres`, `metadata-mysql`), and encryption are all behind `cfg(feature = ...)`. This means we can merge read-path improvements first without needing write modules to compile.

---

## PR Sequence

### PR 1: Read-path foundations and type system improvements
**Theme:** Improve the existing read path that's already on main
**Size:** ~3,000 lines src + ~2,000 lines tests
**Dependencies:** None (base PR)

**Source files (modified):**
- `src/types.rs` — Extended type mapping (lists, structs, maps, decimals, chrono)
- `src/metadata_provider.rs` — New trait methods, SQL constants, shared types
- `src/metadata_provider_duckdb.rs` — New provider methods (partitions, inlined data, snapshots)
- `src/path_resolver.rs` — Hierarchical path resolution improvements
- `src/delete_filter.rs` — Minor filter improvements
- `src/error.rs` — Error type additions
- `src/encryption.rs` — PME read improvements

**Source files (new):**
- `src/parse_values.rs` — String-to-Arrow parsing for inlined data

**Cargo.toml:**
- Add `chrono` dependency

**Tests:** Type system tests, parse_values tests, path resolver tests

**Notes:** This PR adds no new feature flags. It extends the `MetadataProvider` trait with new methods. The DuckDB provider implements them; the sqlx providers (sqlite/pg/mysql) will get stub/todo implementations temporarily. This needs careful handling — either add default impls on the trait, or split the trait extension to PR2.

---

### PR 2: Catalog, schema, and table provider enhancements
**Theme:** Catalog/schema DDL support + table scan improvements
**Size:** ~3,500 lines src + ~3,000 lines tests
**Dependencies:** PR 1

**Source files (modified):**
- `src/catalog.rs` — Snapshot-aware catalog, dynamic lookup improvements
- `src/schema.rs` — DDL support (CREATE TABLE, ALTER TABLE, DROP), schema creation
- `src/table.rs` — Read-path portions only: filter pushdown, scan improvements, virtual column integration
- `src/information_schema.rs` — SQL-queryable catalog improvements
- `src/lib.rs` — New module declarations (read-path only)

**Source files (new):**
- `src/virtual_column_exec.rs` — Virtual columns (filename, file_row_number, rowid, snapshot_id, file_index)

**Tests:** Virtual column tests, catalog tests, schema DDL tests, information_schema tests

---

### PR 3: Table functions and CDC
**Theme:** Table functions and change tracking
**Size:** ~2,000 lines src + ~2,000 lines tests
**Dependencies:** PR 2

**Source files (modified):**
- `src/table_functions.rs` — ducklake_snapshots(), ducklake_table_changes(), etc.
- `src/table_changes.rs` — CDC implementation improvements
- `src/table_deletions.rs` — Deletion tracking enhancements

**Source files (new):**
- `src/table_insertions.rs` — Insertion tracking
- `src/cdc_common.rs` — Shared CDC utilities

**Tests:** Table function tests, table_changes tests, time travel tests

---

### PR 4: F-044 Code deduplication (dialect + provider/writer macros)
**Theme:** Infrastructure for multi-backend support
**Size:** ~4,000 lines src (new), ~1,500 lines reduced from existing files
**Dependencies:** PR 1 (needs MetadataProvider trait)

**Source files (new):**
- `src/dialect.rs` — SQL dialect abstraction (673 lines)
- `src/metadata_provider_impl.rs` — Macro-based provider implementation (1,010 lines)

**Source files (modified):**
- `src/metadata_provider_sqlite.rs` — Reduced via macro dedup
- `src/metadata_provider_postgres.rs` — Reduced via macro dedup
- `src/metadata_provider_mysql.rs` — Reduced via macro dedup

**Notes:** This is a refactor PR. It replaces duplicated code across the 3 sqlx-based metadata providers with a macro + dialect trait approach. The DuckDB provider is NOT affected (it uses a different API). This PR makes PR 5+ much simpler.

---

### PR 5: Write support core (INSERT + table writer)
**Theme:** Core write infrastructure
**Size:** ~5,000 lines src + ~2,000 lines tests
**Dependencies:** PR 2, PR 4
**Feature flag:** `write`

**Source files (modified):**
- `src/insert_exec.rs` — Full INSERT implementation with partition routing
- `src/table_writer.rs` — High-level write API with atomicity
- `src/metadata_writer.rs` — MetadataWriter trait expansion
- `src/table.rs` — Write-path portions (insert method, DML routing)

**Source files (new):**
- `src/metadata_writer_validation.rs` — DDL/DML validation helpers (1,211 lines)
- `src/metadata_writer_impl.rs` — Shared writer implementation via macros (2,262 lines)

**Cargo.toml:**
- Add `write-mysql` and `write-postgres` feature flags

**Tests:** Write tests, write partition tests, conflict detection tests

---

### PR 6: SQLite metadata writer improvements
**Theme:** SQLite writer hardening
**Size:** ~2,700 lines (mostly modifications to existing file)
**Dependencies:** PR 5

**Source files (modified):**
- `src/metadata_writer_sqlite.rs` — Major expansion with SQLITE_BUSY retry, WAL mode, transactions

**Tests:** Concurrent write tests, SQLite-specific tests

---

### PR 7: PostgreSQL and MySQL metadata writers
**Theme:** Additional metadata backends for writes
**Size:** ~2,200 lines src + ~500 lines tests
**Dependencies:** PR 5, PR 6 (for shared writer patterns)
**Feature flags:** `write-postgres`, `write-mysql`

**Source files (new):**
- `src/metadata_writer_postgres.rs` (1,042 lines)
- `src/metadata_writer_mysql.rs` (1,173 lines)

**Tests:** Postgres writer tests, MySQL writer tests (Docker-dependent)

---

### PR 8: DELETE, UPDATE, MERGE execution
**Theme:** DML beyond INSERT
**Size:** ~2,000 lines src + ~2,000 lines tests
**Dependencies:** PR 5
**Feature flag:** `write`

**Source files (new):**
- `src/delete_exec.rs` (383 lines) — MOR delete files
- `src/update_exec.rs` (485 lines) — Copy-on-write update
- `src/merge_exec.rs` (829 lines) — MERGE INTO

**Source files (new):**
- `src/query_planner.rs` (315 lines) — Routes DELETE/UPDATE/MERGE

**Source files (modified):**
- `src/table.rs` — DML method implementations (delete, update, merge)

**Tests:** Delete tests, update tests, merge tests

---

### PR 9: Compaction functions
**Theme:** Catalog maintenance operations
**Size:** ~850 lines src + ~500 lines tests
**Dependencies:** PR 5

**Source files (new):**
- `src/compaction_functions.rs` (824 lines)

**Tests:** Compaction tests

---

### PR 10: Cross-engine interop tests
**Theme:** DuckDB <-> DataFusion interoperability validation
**Size:** ~5,000 lines tests
**Dependencies:** PR 8 (needs DML support)

**Test files (all new):**
- `tests/cross_engine_tests.rs`
- `tests/cross_engine_alter_tests.rs`
- `tests/cross_engine_ddl_tests.rs`
- `tests/cross_engine_dml_tests.rs`
- `tests/cross_engine_feature_tests.rs`
- `tests/cross_engine_inline_tests.rs`
- `tests/cross_engine_insert_tests.rs`
- `tests/cross_engine_partition_tests.rs`
- `tests/cross_engine_mysql_tests.rs`
- `tests/cross_engine_postgres_tests.rs`

---

### PR 11: Edge case, adversarial, and stress tests
**Theme:** Robustness test suite
**Size:** ~8,000 lines tests
**Dependencies:** PR 8

**Test files (all new):**
- `tests/adversarial_*` (6 files)
- `tests/deep_edge_case_tests.rs`
- `tests/edge_case_tests.rs`
- `tests/issue_repro_*` (6 files)
- `tests/roundtrip_interop_tests.rs`
- `tests/parity_tests.rs`

---

### PR 12: SQLLogicTest improvements + view tests
**Theme:** SLT harness and test coverage
**Size:** ~1,500 lines
**Dependencies:** PR 8

**Files:**
- `tests/sqllogictests/` — Modified SLT files + new view tests
- `tests/view_tests.rs`
- `tests/common/test_utils.rs`

---

### PR 13: Documentation
**Theme:** Project docs, review reports, audit reports
**Size:** ~15,000 lines docs
**Dependencies:** PR 12 (last, or can go earlier)

**Files:**
- `docs/*` — All documentation files
- `CLAUDE.md` updates
- `README.md` updates (if any)

**Notes:** Review reports (docs/2026-03-*) are internal development artifacts. Consider whether these belong in the public repo. A cleaner approach might be to only include `docs/handoff-prompt.md`, `docs/project-status.md`, and similar reference docs, excluding the per-review-cycle reports.

---

## Summary Table

| PR | Title | ~Lines | Feature Flag | Depends On |
|----|-------|--------|-------------|------------|
| 1 | Read-path foundations & type system | 5,000 | — | — |
| 2 | Catalog, schema, table provider | 6,500 | — | 1 |
| 3 | Table functions & CDC | 4,000 | — | 2 |
| 4 | F-044 code deduplication | 4,000 | — | 1 |
| 5 | Write support core (INSERT) | 7,000 | `write` | 2, 4 |
| 6 | SQLite writer improvements | 3,000 | `write-sqlite` | 5 |
| 7 | PostgreSQL & MySQL writers | 2,700 | `write-pg/mysql` | 5 |
| 8 | DELETE, UPDATE, MERGE | 4,000 | `write` | 5 |
| 9 | Compaction functions | 1,350 | `write` | 5 |
| 10 | Cross-engine interop tests | 5,000 | — | 8 |
| 11 | Adversarial & stress tests | 8,000 | — | 8 |
| 12 | SLT + view tests | 1,500 | — | 8 |
| 13 | Documentation | 15,000 | — | Any |

**Total: ~62,000 lines** (remaining ~22k is in docs and test infrastructure overlap)

## Dependency Graph

```
PR 1 (types, providers, parse)
├── PR 2 (catalog, schema, table, virtual cols)
│   ├── PR 3 (table functions, CDC)
│   └── PR 5 (write core) ← also depends on PR 4
│       ├── PR 6 (SQLite writer)
│       ├── PR 7 (PG/MySQL writers)
│       ├── PR 8 (DELETE/UPDATE/MERGE)
│       │   ├── PR 10 (cross-engine tests)
│       │   ├── PR 11 (adversarial tests)
│       │   └── PR 12 (SLT + view tests)
│       └── PR 9 (compaction)
├── PR 4 (F-044 dedup)
└── PR 13 (docs) — can go anywhere
```

## Risks and Mitigations

### Risk 1: Entangled changes in `src/table.rs`
`table.rs` has +1,391/-103 lines touching both read and write paths. Splitting cleanly requires careful extraction.

**Mitigation:** In PR 2, include only the read-path changes (virtual columns, filter pushdown, scan improvements). Use `#[cfg(feature = "write")]` blocks to defer DML methods to PR 5/8. The existing feature gating in the code helps here.

### Risk 2: MetadataProvider trait changes affect all backends
PR 1 extends the trait, but the sqlx providers get refactored in PR 4. Between PR 1 and PR 4, the sqlx providers need to compile with new trait methods.

**Mitigation:** Add `default` implementations for new trait methods (returning `unimplemented!()` or empty results) in PR 1. PR 4 replaces them with real implementations.

### Risk 3: Test infrastructure changes span multiple PRs
`tests/common/` module is shared by many test files added in different PRs.

**Mitigation:** Include the common test module in whichever PR first needs it (likely PR 1 or PR 2). Extend it in later PRs as needed.

### Risk 4: 264-commit history makes selective extraction fragile
The integration branch has many fix-on-fix commits. File-level checkout is more reliable than commit-level cherry-pick.

**Mitigation:** Use `git checkout integration -- <file>` for whole-file extraction. For files that need partial changes (like `table.rs`), manually extract the relevant portions. Always verify compilation after each extraction.

### Risk 5: Merge conflicts between concurrent PRs
If multiple PRs are open simultaneously, they may conflict on shared files (`lib.rs`, `Cargo.toml`, `table.rs`).

**Mitigation:** Merge PRs sequentially. Rebase subsequent PR branches onto main after each merge. The dependency graph above defines the safe ordering.

## Execution Recommendations

1. **Start with PR 1 + PR 4 in parallel** — they're independent and unblock everything else
2. **PR 2 and PR 3 can follow quickly** — read-path only, lower risk
3. **PR 5 is the critical path** — write core unlocks 5 downstream PRs
4. **PRs 6-9 can be prepared in parallel** once PR 5 lands
5. **PRs 10-12 are test-only** — lowest risk, can be batched
6. **PR 13 (docs)** — submit whenever, consider trimming review artifacts
7. **Exclude untracked files** (`.claude/`, review docs not yet committed) from all PRs
8. **Each PR should update `CLAUDE.md`** as appropriate (especially the test count, SLT pass rate, feature list)
