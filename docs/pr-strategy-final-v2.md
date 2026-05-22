# PR Strategy v2: Multi-PR Feature Split

## Approach

**Stacked PRs** — each PR branch is based on the previous PR's branch. Merge in order. The final PR's branch equals the integration branch HEAD.

**Method for each PR:**
1. Create branch from previous PR branch (or `main` for PR 1)
2. `git checkout ducklake-features/integration -- <files>` to pull whole files
3. For `lib.rs`, manually edit to only declare modules present in that PR
4. Verify compilation: `cargo build` (and `cargo build --features write-sqlite` for write PRs)
5. Run relevant tests

**Why whole-file checkout works:** The codebase is heavily feature-gated. Write-path code in shared files (`table.rs`, `schema.rs`, `catalog.rs`) is behind `#[cfg(feature = "write")]`. Read-path PRs can include the full final version of these files — write code simply won't compile in without the `write` feature enabled. New write-only source files (e.g., `delete_exec.rs`) are only declared in `lib.rs` under `#[cfg(feature = "write")]`, so they don't need to exist until the write PR.

---

## Prerequisites

Merge open PRs #80, #81, #82 against `main` first, then rebase integration. The integration branch already has equivalent fixes, so conflicts will be minor.

---

## PR 1: Read Path Enhancements + Type System

**Title:** `feat: enhanced read path — virtual columns, complex types, path resolution, encryption`
**Est. size:** ~8k lines src + ~4k lines tests

This PR upgrades every read-path component: types, parsing, path resolution, virtual columns, delete filtering, encryption, catalog/schema dynamic lookup, information schema, table functions, and CDC tracking. No write support.

### Source files (NEW — 6 files)
| File | Lines | Purpose |
|------|-------|---------|
| `src/virtual_column_exec.rs` | 350 | Virtual columns: filename, file_row_number, rowid, snapshot_id, file_index |
| `src/parse_values.rs` | 509 | String-to-Arrow parsing for inlined data (lenient/strict modes) |
| `src/table_insertions.rs` | 199 | Insertion tracking for CDC |
| `src/cdc_common.rs` | 196 | Shared CDC utilities |
| `src/dialect.rs` | 673 | SQL dialect abstraction (needed by DuckDB provider) |
| `src/compaction_functions.rs` | 824 | DuckDB compaction function wrappers |

### Source files (MODIFIED — take full integration version, 14 files)
| File | Main lines | Integration lines | Key changes |
|------|-----------|-------------------|-------------|
| `src/types.rs` | 221 | 1184 | Lists, structs, maps, decimals, chrono types |
| `src/metadata_provider.rs` | 536 | 802 | New trait methods, SQL constants, shared types |
| `src/metadata_provider_duckdb.rs` | 397 | 786 | New provider methods (partitions, inlined data, snapshots) |
| `src/path_resolver.rs` | 170 | 536 | Hierarchical path resolution, S3/MinIO support |
| `src/delete_filter.rs` | 200 | 243 | Filter improvements |
| `src/encryption.rs` | 128 | 231 | PME read enhancements |
| `src/error.rs` | 30 | 40 | Error type additions |
| `src/catalog.rs` | 217 | 427 | Snapshot-aware dynamic lookup (write sections gated) |
| `src/schema.rs` | 273 | 599 | DDL support, snapshot filtering (write sections gated) |
| `src/table.rs` | 734 | 2022 | Virtual columns, filter pushdown, scans (write sections gated) |
| `src/information_schema.rs` | 235 | 439 | Extended queryable catalog |
| `src/table_functions.rs` | 394 | 809 | ducklake_snapshots(), ducklake_table_changes(), etc. |
| `src/table_changes.rs` | 145 | 485 | CDC implementation improvements |
| `src/table_deletions.rs` | 165 | 520 | Deletion tracking enhancements |

### Infrastructure
| File | Changes |
|------|---------|
| `src/lib.rs` | Add module declarations for new read-path modules only. Keep write modules commented/gated. |
| `Cargo.toml` | Add `chrono` dependency |
| `examples/basic_query.rs` | Updated example |

### Compilation strategy
- `cargo build` — default features (metadata-duckdb, no write). All `#[cfg(feature = "write")]` blocks in table.rs/schema.rs/catalog.rs are dead code.
- `cargo build --features metadata-sqlite` — will FAIL because sqlx providers now depend on `metadata_provider_impl.rs` (F-044). **Solution:** include `metadata_provider_impl.rs` in this PR too. It's a macro file with no write dependencies.
- **Required addition:** `src/metadata_provider_impl.rs` (1,010 lines) + modified sqlx providers (sqlite/pg/mysql) to use it. This keeps all 4 metadata providers compiling.

### Updated source files (add to handle sqlx provider compilation)
| File | Main lines | Integration lines | Notes |
|------|-----------|-------------------|-------|
| `src/metadata_provider_impl.rs` | NEW | 1010 | F-044 shared provider macros |
| `src/metadata_provider_sqlite.rs` | 679 | 336 | Refactored to use shared impl |
| `src/metadata_provider_postgres.rs` | 700 | 329 | Refactored to use shared impl |
| `src/metadata_provider_mysql.rs` | 675 | 333 | Refactored to use shared impl |

### Test files (~15 files)
**New:**
- `tests/virtual_column_tests.rs`
- `tests/virtual_column_extended_tests.rs`
- `tests/file_pruning_tests.rs`
- `tests/table_function_tests.rs`
- `tests/time_travel_tests.rs`
- `tests/stats_tests.rs`
- `tests/view_tests.rs`
- `tests/negative_footer_size_test.rs`
- `tests/parity_tests.rs`
- `tests/common/test_utils.rs`
- `tests/sqllogictests/sql/view/*.test` (6 files)

**Modified:**
- `tests/common/mod.rs` — test utilities
- `tests/delete_filter_tests.rs`
- `tests/information_schema_test.rs`
- `tests/table_changes_tests.rs`
- `tests/sqlite_metadata_provider_test.rs`
- `tests/postgres_metadata_provider_test.rs`
- `tests/mysql_metadata_provider_test.rs`
- `tests/hybrid_asyncdb.rs`
- `tests/sqllogictest_runner.rs`
- Modified SLT files (read-path related)

### Verification
```bash
cargo build
cargo build --features metadata-sqlite
cargo build --features encryption
cargo test --features metadata-duckdb  # read-only tests
```

---

## PR 2: Write Foundation — INSERT + DDL + SQLite Writer

**Title:** `feat: write support — INSERT, DDL, table writer, SQLite metadata writer`
**Est. size:** ~12k lines src + ~8k lines tests

Adds the complete write infrastructure: INSERT execution, table writer with atomicity, query planner for DML routing, metadata writer trait + SQLite implementation, DDL operations (CREATE/DROP TABLE/SCHEMA, ALTER TABLE). The SQLite writer is bundled here because write tests are untestable without a backend.

### Source files (NEW — 5 files)
| File | Lines | Purpose |
|------|-------|---------|
| `src/insert_exec.rs` | 1118 | DuckLakeInsertExec with partition routing and transforms |
| `src/query_planner.rs` | 315 | DuckLakeQueryPlanner routes DELETE/UPDATE/MERGE to table methods |
| `src/metadata_writer_validation.rs` | 1211 | DDL/DML validation helpers |
| `src/metadata_writer_impl.rs` | 2262 | F-044 shared writer macros (used by all 3 writer backends) |

### Source files (MODIFIED — already present from PR 1, take updated version)
| File | Key additions |
|------|---------------|
| `src/metadata_writer.rs` | MetadataWriter trait expanded (303→838 lines) |
| `src/metadata_writer_sqlite.rs` | Full SQLite write backend with SQLITE_BUSY retry (663→3098 lines) |
| `src/table_writer.rs` | High-level write API with atomicity (450→2275 lines) |
| `src/lib.rs` | Add write module declarations |

### Compilation strategy
- `cargo build --features write-sqlite` — full write path compilation
- Feature flag `write` gates all new modules in `lib.rs`
- `metadata_writer_impl.rs` is gated under `#[cfg(feature = "write")]`

### Test files (~18 files)
**New:**
- `tests/write_partition_tests.rs`
- `tests/alter_table_tests.rs`
- `tests/create_schema_tests.rs`
- `tests/drop_and_constraints_tests.rs`
- `tests/conflict_detection_tests.rs`
- `tests/sql_dml_tests.rs`
- `tests/edge_case_tests.rs`
- `tests/deep_edge_case_tests.rs`
- `tests/adversarial_catalog_tests.rs`
- `tests/adversarial_edge_tests.rs`
- `tests/adversarial_pattern_tests_1.rs`
- `tests/adversarial_pattern_tests_2.rs`
- `tests/adversarial_storage_tests.rs`
- `tests/adversarial_type_schema_tests.rs`

**Modified:**
- `tests/write_tests.rs`
- `tests/concurrent_tests.rs`
- `tests/concurrent_write_tests.rs`
- `tests/sql_write_tests.rs`
- `tests/renamed_columns_tests.rs`
- Modified SLT files (insert/alter/catalog related)

### Verification
```bash
cargo build --features write-sqlite
cargo test --features write-sqlite
```

---

## PR 3: DELETE, UPDATE, MERGE

**Title:** `feat: DELETE, UPDATE, and MERGE DML operations`
**Est. size:** ~3.5k lines src + ~3k lines tests

Adds the remaining DML operations beyond INSERT: DELETE (MOR pattern), UPDATE (copy-on-write via delete + insert), and MERGE (MATCHED/NOT MATCHED branches).

### Source files (NEW — 3 files)
| File | Lines | Purpose |
|------|-------|---------|
| `src/delete_exec.rs` | 383 | DuckLakeDeleteExec writes delete files (MOR pattern) |
| `src/update_exec.rs` | 485 | DuckLakeUpdateExec (copy-on-write) |
| `src/merge_exec.rs` | 829 | DuckLakeMergeExec (MATCHED/NOT MATCHED branches) |

### Source files (MODIFIED)
| File | Changes |
|------|---------|
| `src/lib.rs` | Add `delete_exec`, `update_exec`, `merge_exec` module declarations (already gated under `write`) |

### Compilation strategy
All 3 files are gated under `#[cfg(feature = "write")]` in `lib.rs`. No changes to shared files needed — `table.rs` already has the DML routing methods from PR 1 (gated behind `write`).

### Test files (~5 files)
**New:**
- `tests/delete_tests.rs`
- `tests/update_tests.rs`
- `tests/merge_tests.rs`

**Modified:**
- Modified SLT files (delete/update related)

### Verification
```bash
cargo build --features write-sqlite
cargo test --features write-sqlite delete
cargo test --features write-sqlite update
cargo test --features write-sqlite merge
```

---

## PR 4: PostgreSQL + MySQL Write Backends

**Title:** `feat: PostgreSQL and MySQL metadata writers`
**Est. size:** ~4k lines src + ~2k lines tests

Adds write support for PostgreSQL and MySQL metadata backends, completing multi-backend support.

### Source files (NEW — 2 files)
| File | Lines | Purpose |
|------|-------|---------|
| `src/metadata_writer_postgres.rs` | 1042 | PostgreSQL write backend |
| `src/metadata_writer_mysql.rs` | 1173 | MySQL write backend |

### Infrastructure
| File | Changes |
|------|---------|
| `src/lib.rs` | Add `metadata_writer_postgres`, `metadata_writer_mysql` module declarations |
| `Cargo.toml` | Add `write-postgres`, `write-mysql` feature flags |

### Test files (~4 files)
**New:**
- `tests/postgres_metadata_writer_test.rs`
- `tests/mysql_metadata_writer_test.rs`
- `tests/issue_repro_backend_tests.rs`

### Verification
```bash
cargo build --features write-postgres
cargo build --features write-mysql
cargo test --all-features --features skip-tests-with-docker  # without Docker
cargo test --all-features  # with Docker
```

---

## PR 5: Cross-Engine Tests + Remaining Test Suite

**Title:** `feat: cross-engine interoperability tests and expanded test suite`
**Est. size:** ~12k lines tests

Adds the comprehensive cross-engine test suite (DataFusion <-> DuckDB interop), issue reproduction tests, and remaining edge case coverage.

### Test files (~15 files)
**New:**
- `tests/cross_engine_tests.rs`
- `tests/cross_engine_alter_tests.rs`
- `tests/cross_engine_ddl_tests.rs`
- `tests/cross_engine_dml_tests.rs`
- `tests/cross_engine_feature_tests.rs`
- `tests/cross_engine_inline_tests.rs`
- `tests/cross_engine_insert_tests.rs`
- `tests/cross_engine_partition_tests.rs`
- `tests/cross_engine_postgres_tests.rs`
- `tests/cross_engine_mysql_tests.rs`
- `tests/compaction_tests.rs`
- `tests/interop_type_tests.rs`
- `tests/roundtrip_interop_tests.rs`
- `tests/issue_repro_misc_tests.rs`
- `tests/issue_repro_schema_tests.rs`
- `tests/issue_repro_storage_tests.rs`
- `tests/issue_repro_stress_tests.rs`
- `tests/issue_repro_type_tests.rs`

### Verification
```bash
cargo test --features write-sqlite cross_engine
cargo test --all-features
```

---

## Summary

| PR | Title | New src files | Modified src files | Test files | Est. lines | Depends on |
|----|-------|--------------|-------------------|------------|-----------|------------|
| 1 | Read path + types + infrastructure | 7 | 18 | ~25 | ~12k | — |
| 2 | Write + INSERT + DDL + SQLite writer | 4 | 4 | ~20 | ~20k | PR 1 |
| 3 | DELETE, UPDATE, MERGE | 3 | 1 | ~5 | ~4k | PR 2 |
| 4 | PostgreSQL + MySQL writers | 2 | 2 | ~4 | ~5k | PR 3 |
| 5 | Cross-engine + remaining tests | 0 | 0 | ~18 | ~12k | PR 4 |

**Total: 5 PRs, ~53k lines** (some overlap with shared file modifications across PRs)

---

## Execution Script

For each PR, the creation process is:

```bash
# PR 1 example
git checkout main
git checkout -b pr/1-read-path
git checkout ducklake-features/integration -- \
  src/virtual_column_exec.rs \
  src/parse_values.rs \
  src/table_insertions.rs \
  src/cdc_common.rs \
  src/dialect.rs \
  src/compaction_functions.rs \
  src/metadata_provider_impl.rs \
  src/types.rs \
  src/metadata_provider.rs \
  src/metadata_provider_duckdb.rs \
  src/metadata_provider_sqlite.rs \
  src/metadata_provider_postgres.rs \
  src/metadata_provider_mysql.rs \
  src/path_resolver.rs \
  src/delete_filter.rs \
  src/encryption.rs \
  src/error.rs \
  src/catalog.rs \
  src/schema.rs \
  src/table.rs \
  src/information_schema.rs \
  src/table_functions.rs \
  src/table_changes.rs \
  src/table_deletions.rs \
  Cargo.toml \
  examples/basic_query.rs

# Manually edit src/lib.rs to declare only the modules present
# (remove write-gated module declarations for modules not yet added)

# Verify
cargo build
cargo build --features metadata-sqlite
cargo build --features encryption

# Add tests
git checkout ducklake-features/integration -- tests/virtual_column_tests.rs tests/...
cargo test
```

For subsequent PRs:
```bash
git checkout pr/1-read-path
git checkout -b pr/2-write-foundation
git checkout ducklake-features/integration -- <new files for PR 2>
# Edit lib.rs to add new module declarations
cargo build --features write-sqlite
cargo test --features write-sqlite
```

---

## Handling the Tricky Shared Files

### `src/lib.rs`
Each PR manually edits `lib.rs` to declare only the modules present so far. This is the only file that needs manual per-PR editing rather than whole-file checkout.

### `src/table.rs`, `src/schema.rs`, `src/catalog.rs`
Take the FULL integration version in PR 1. Write-path code is behind `#[cfg(feature = "write")]` and references modules (insert_exec, delete_exec, etc.) that are also behind `#[cfg(feature = "write")]` in lib.rs. Without the `write` feature enabled, these sections don't compile, so missing modules are not an error.

### `src/metadata_writer_sqlite.rs`
On `main` this file already exists (663 lines, read-path only). In PR 2, replace it with the full 3098-line integration version that adds the MetadataWriter implementation.

### `src/metadata_writer_impl.rs` (F-044)
This is gated under `#[cfg(feature = "write")]`. Include in PR 2 since writers need it. It's referenced by `metadata_writer_sqlite.rs` via `crate::metadata_writer_impl::impl_writer_*!()` macros.

### sqlx providers (`metadata_provider_{sqlite,postgres,mysql}.rs`)
These shrank from ~680 lines to ~330 lines due to F-044. They now depend on `metadata_provider_impl.rs` and `dialect.rs`. Both of these must be in PR 1 alongside the updated providers.

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| PR 1 is large (~12k) | Accepted | Unavoidable — read path touches every file. Well-structured with clear theme. |
| lib.rs manual editing per PR | Low risk | Simple — just adding/removing `mod` declarations behind feature gates |
| Compilation failure from missing modules | Low | Feature gates prevent compilation of gated code. Verify each PR with `cargo build`. |
| Test assignment ambiguity | Medium | Some tests span features. Assign to earliest PR where they can compile. |
| Merge conflicts between stacked PRs | Low | Merge in strict order. Only lib.rs needs per-PR adjustment. |
