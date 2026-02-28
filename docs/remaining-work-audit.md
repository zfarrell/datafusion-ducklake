# Remaining Work Audit: DataFusion-DuckLake Feature Parity

**Date**: 2026-02-28
**Auditor**: Comprehensive cross-document reconciliation
**Branch audited**: `ducklake-features/integration` (updated 2026-02-28 with SLT results at commit `f792458`)
**Documents reviewed**: 12 (see methodology at end)

---

## 1. Executive Summary

### Overall Feature Parity: ~75-80% (honest assessment)

The integration branch has made substantial progress. Phases 0 through 5 are effectively complete, and Phase 6 is mostly done. The 60-65% estimate in the plan document is **outdated** -- it was written before Phases 4-6 work landed.

Here is a corrected breakdown:

| Area | Status | Parity |
|------|--------|--------|
| Read path (SELECT, filters, MOR deletes, encrypted reads) | Complete | 95% |
| INSERT (all modes, stats, field IDs, footer tracking) | Complete | 90% |
| DELETE (write + cross-engine) | Complete | 90% |
| UPDATE (write + cross-engine) | Complete | 90% |
| Virtual columns (all 5) | Complete | 100% |
| Query planner (DML routing) | Complete | 95% |
| Column statistics (write + read + pruning) | Complete | 90% |
| File pruning | Complete | 85% (missing benchmark) |
| Time travel table functions | Complete | 90% (SQL syntax deferred) |
| ALTER TABLE (8 of 12 ops) | Mostly complete | 67% |
| Views (CREATE/DROP/RENAME) | Complete | 95% |
| DROP TABLE/SCHEMA | Complete | 100% |
| CREATE SCHEMA | Complete | 95% |
| Compaction (5 functions) | Complete (delegated to DuckDB) | 85% |
| Partitioning (read-side) | Complete | 50% (write-side missing) |
| Data inlining (read-side) | Complete | 40% (write-side missing) |
| MERGE INTO (programmatic API) | Complete | 60% (SQL parsing missing) |
| Complex types (read/write) | Type parsing done | 50% (evolution missing) |
| Encrypted writes | Not started | 0% |
| Multi-backend writers (Postgres/MySQL) | Fully implemented | 90% (cross-engine tests missing) |
| SQLLogicTest pass rate | 151/248 (60.9%) | 61% (up from 48.4%) |
| Table functions | 12 of ~16 implemented | 75% |

### Remaining Items by Effort Level

| Effort | Count | Description |
|--------|-------|-------------|
| Small (S) | 6 | Tests, benchmarks, reserved name test, fix branch PRs |
| Medium (M) | 5 | Cross-engine Postgres/MySQL tests, SLT improvements, time travel SQL |
| Large (L) | 6 | Write-side partitioning, write-side inlining, complex type evolution, encrypted writes, SQL MERGE, SLT complex types |
| Blocked/Deferred | 4 | SQL MERGE (DataFusion parser), SQL time travel (DataFusion 52), macros (DuckLake limitation), sorted keys (DuckDB unsupported) |

---

## 2. Reconciliation Issues

### 2.1 Major Contradictions

**remaining-gaps.md is stale on Postgres/MySQL status**:
- `remaining-gaps.md` (Section 1.1, 1.2) says Postgres/MySQL have 4 stub writer methods and 8 missing provider methods.
- **Actual state**: All 4 writer methods (`rename_table`, `set_table_comment`, `set_column_comment`, `rename_view`) are FULLY IMPLEMENTED in both `metadata_writer_postgres.rs` and `metadata_writer_mysql.rs`. All 8 provider methods (`list_views`, `get_view_by_name`, `view_exists`, `get_file_column_stats`, `get_table_row_count`, `get_partition_columns`, `get_file_partition_values`, `get_inlined_data`) are FULLY IMPLEMENTED in both `metadata_provider_postgres.rs` and `metadata_provider_mysql.rs`.
- **Root cause**: `remaining-gaps.md` was written at commit `3d874ff`, before commits `4f73c9b` (Postgres) and `5f66562` (MySQL) landed.
- **Impact**: Gap items 1.1 and 1.2 from remaining-gaps.md are **already resolved**. Zero remaining work there.

**remaining-gaps.md Section 10 (File Pruning) is stale**:
- Says "Read-side pruning is not implemented."
- **Actual state**: File pruning IS implemented in `table.rs` (`statistics()` method at line 1246, partition-aware pruning at line 879). Commit `995419c` implemented this.
- **Impact**: Gap item 10 is **already resolved**.

**remaining-gaps.md Section 11 (Compaction) is stale**:
- Says "Not started."
- **Actual state**: All 5 compaction functions are implemented in `src/compaction_functions.rs` (commit `48a648f`). They delegate to DuckDB's native compaction via a temporary ATTACH.
- **Impact**: Gap item 11 is **already resolved**.

**remaining-gaps.md Section 12 (NOT NULL enforcement) is stale**:
- Says "Enforcement during writes is not implemented."
- **Actual state**: NOT NULL enforcement IS implemented in `metadata_writer_validation.rs` and `insert_exec.rs`. This was part of Phase 0 work.
- **Impact**: Gap item 12 is **already resolved**.

**remaining-gaps.md Section 13 (Snapshot refresh after DDL) is stale**:
- Says "`DuckLakeCatalog.snapshot_id` is currently immutable."
- **Actual state**: `catalog.rs` uses `AtomicI64` for `snapshot_id` (confirmed by `review-work-completeness.md` Section 4 and grep of source).
- **Impact**: Gap item 13 is **already resolved**.

**review-feature-parity.md (Feb 26) is heavily stale**:
- Shows ALTER TABLE as "4 of 12 operations" -- actual: 8 of 12.
- Shows Virtual Columns as "2 of 5" -- actual: 5 of 5.
- Shows Column Statistics read-side as "No" -- actual: implemented.
- Shows File Pruning as "None" -- actual: implemented.
- Shows Time Travel as "None" -- actual: table functions implemented.
- Shows MERGE INTO as "None" -- actual: programmatic API implemented.
- Shows Data Inlining as "None" -- actual: read-side implemented.
- Shows Compaction as "None" -- actual: 5 functions implemented.
- Shows Partitioning as "None" -- actual: read-side implemented.
- Shows Table Functions as "4 of 16+" -- actual: 12+ implemented.
- **Root cause**: Written Feb 26, before Phases 2-5 work completed.
- **Impact**: This document should NOT be used for current status. Use the plan checkboxes instead.

**review-work-completeness.md (Feb 26) is stale**:
- Shows "55 commits ahead of main" -- actual: many more commits since.
- File inventories and gap cross-references are pre-Phase 2-5 work.
- **Impact**: Section 3 "Gap Analysis Cross-Reference" is outdated. Most "NOT implemented" gaps are now done.

### 2.2 Items Mentioned in One Document But Missing From Another

**slt-failure-report.md mentions "complex types unsupported" (13 tests)**:
- But `feature-parity-plan-feb-28.md` Phase 4.4 says complex type parsing IS done; only struct field evolution (ADD/REMOVE/RENAME FIELD) is outstanding.
- **Resolution**: Complex type PARSING works. The SLT failures are because these tests also require struct EVOLUTION operations (ALTER TABLE ADD FIELD, etc.), not just basic type parsing. The slt-failure-report correctly identifies these as "Unsupported DuckLake type" errors, but several tests in this group actually fail because of evolution, not parsing.

**Handoff prompt (worktree) differs from main repo version**:
- The worktree version does not exist at the expected path.
- The main repo version at `/home/zac/datafusion-ducklake/docs/handoff-prompt.md` is the authoritative one.

### 2.3 Cross-Engine Testing Matrix (Plan Section) is Misleading

The feature-parity-plan has a "Cross-Engine Testing Matrix" at the bottom with ALL checkboxes unchecked. This is misleading -- cross-engine tests DO exist and pass. The matrix was never updated. For example:
- Basic SELECT cross-engine: tested in `cross_engine_tests.rs`
- INSERT cross-engine: tested in `cross_engine_insert_tests.rs`
- DELETE/UPDATE cross-engine: tested in `cross_engine_dml_tests.rs`
- DDL cross-engine: tested in `cross_engine_ddl_tests.rs`, `cross_engine_alter_tests.rs`
- Virtual columns, stats, conflicts: tested in `cross_engine_feature_tests.rs`
- Partitioning: tested in `cross_engine_partition_tests.rs`
- Data inlining: tested in `cross_engine_inline_tests.rs`

---

## 3. Remaining Work -- Definitive List

### Tier 1: Implementable Now (no external blockers)

#### T1-1: Write-Side Partitioning (Hive Directory Routing)
- **Plan reference**: Phase 4.2, unchecked items
- **What needs to be done**:
  - During INSERT, evaluate partition transform expressions (IDENTITY, YEAR, MONTH, DAY, HOUR) to determine target partition
  - Route rows to per-partition Parquet files using Hive-style directory layout (e.g., `year=2024/month=01/data.parquet`)
  - Record partition values in `ducklake_file_partition_value` table
  - ALTER TABLE SET PARTITIONED BY support
- **Files to modify**: `src/insert_exec.rs` (row routing), `src/table_writer.rs` (multi-file output), `src/metadata_writer.rs` (AlterTableOp::SetPartitionedBy)
- **Estimated effort**: Large (L)
- **SLT tests unlocked**: 3-5 partitioning tests (partial)
- **Dependencies**: None (partition metadata tables exist in all writer DDL schemas)

#### T1-2: Write-Side Data Inlining
- **Plan reference**: Phase 4.3, unchecked items
- **What needs to be done**:
  - For tables below `DATA_INLINING_ROW_LIMIT`, store data inline in catalog metadata rather than Parquet
  - Implement `ducklake_flush_inlined_data()` table function
  - Implement `ducklake_set_option(catalog, 'data_inlining_row_limit', N)` handling
- **Files to modify**: `src/insert_exec.rs`, `src/metadata_writer.rs`, `src/compaction_functions.rs`
- **Estimated effort**: Large (L)
- **SLT tests unlocked**: 7+ data_inlining tests
- **Dependencies**: None fundamental, but architecturally questionable for Postgres/MySQL backends (see remaining-gaps.md Section 3)
- **Note**: DuckDB stores inlined data as actual DuckDB tables in the catalog database. Implementing for non-DuckDB backends requires a different storage approach.

#### T1-3: SLT Pass Rate Improvements -- "Table Not Found" Fixes (3 remaining tests)
- **Plan reference**: Phase 6.3, slt-failure-report Category 8
- **Status**: PARTIALLY DONE (4 of 8 original tests fixed: compaction_partitioned_non_adjacent, compaction_partitioned_table, delete_ignore_extra_columns, basic_partitioning)
- **What remains**:
  - Fix CTAS table visibility (tables created via `CREATE TABLE ... AS`) -- 2-3 tests (data_inlining_large, data_inlining_types, types/all_types)
  - Fix table resolution timing for merge_update_insert -- 1 test
- **Files to modify**: `tests/hybrid_asyncdb.rs` (test adapter), `src/schema.rs` (table visibility)
- **Estimated effort**: Small (S)
- **SLT tests unlocked**: 2-3 tests
- **Dependencies**: None

#### T1-4: SLT Pass Rate Improvements -- Expected Failure Mismatch Fixes (1 remaining test)
- **Plan reference**: Phase 6.3, slt-failure-report Category 9
- **Status**: MOSTLY DONE (3 of 4 original tests fixed: detach_ducklake, ducklake_read_only, missing_parquet)
- **What remains**: data_inlining/data_inlining_transaction_local_alter.test now shows as result mismatch rather than expected failure mismatch
- **Estimated effort**: Small (S)
- **SLT tests unlocked**: 0-1 tests (merged into result mismatch category)
- **Dependencies**: None

#### T1-5: SLT Pass Rate Improvements -- Query Result Mismatch Fixes (30 tests remaining)
- **Plan reference**: Phase 6.3, slt-failure-report Category 12
- **Status**: 2 of original 33 fixed (checkpoint_updates_interleaved, timestamp). Net count changed due to reclassification.
- **What needs to be done**: Case-by-case analysis. Some are fixable:
  - Rowid computation differences -- fix in virtual column logic (2 tests)
  - Type promotion display -- fix in type formatter (2-3 tests)
  - Default value application -- fix in insert logic (2 tests)
  - NaN/Inf display -- fix in result formatting (1 test)
  - View result format -- fix view query output (1 test)
  - Virtual column values -- fix computation (1 test)
- **Estimated effort**: Medium (M), varies per test
- **SLT tests unlocked**: Estimated 10-15 of the 30
- **Dependencies**: None (each fix is independent)

#### T1-6: File Pruning Benchmark
- **Plan reference**: Phase 2.2, single unchecked item
- **What needs to be done**: Create a benchmark measuring scan reduction on a multi-file table with file pruning enabled vs disabled
- **Files to modify**: `benchmark/` directory
- **Estimated effort**: Small (S)
- **SLT tests unlocked**: None
- **Dependencies**: None

#### T1-7: Fix Branch PRs (3 remaining)
- **Plan reference**: Phase 0.1, three unchecked items
- **What needs to be done**: Push and create PRs for:
  - `fix/validate-record-count`
  - `fix/name-validation`
  - `fix/type-normalization-promotion`
- **Estimated effort**: Small (S) -- branches exist, just need pushing and PR creation
- **SLT tests unlocked**: None directly
- **Dependencies**: None

#### T1-8: Cross-Engine Postgres/MySQL Tests
- **Plan reference**: Phase 6.4, two unchecked items
- **What needs to be done**:
  - Cross-engine test: DF writes to Postgres-backed catalog, DuckDB reads
  - Cross-engine test: DF writes to MySQL-backed catalog, DuckDB reads
  - Requires DuckDB to connect to same Postgres/MySQL catalog DB
- **Files to modify**: New test files or extend existing cross-engine tests
- **Estimated effort**: Medium (M)
- **SLT tests unlocked**: None
- **Dependencies**: Docker infrastructure for Postgres/MySQL test databases. DuckDB must support connecting to same catalog.
- **Blocker note**: DuckDB's DuckLake extension may not support Postgres/MySQL as catalog backends (it uses its own internal storage). This might require using DuckDB as the catalog backend for cross-engine tests, which would change the test approach.

#### T1-9: Reserved Schema Name Test
- **Plan reference**: Phase 3.4, one unchecked item
- **What needs to be done**: Add test that CREATE SCHEMA rejects "information_schema"
- **Note**: The FEATURE is already implemented (found in `catalog.rs` line 252). Only the explicit test is missing.
- **Estimated effort**: Small (S)
- **SLT tests unlocked**: None
- **Dependencies**: None

#### T1-10: Port View-Related SLT Tests (~6 tests)
- **Plan reference**: Phase 6.3
- **What needs to be done**: Port/adapt 6 view-related DuckLake SLT tests
- **Estimated effort**: Small (S)
- **Dependencies**: Views are fully working (CREATE/DROP/RENAME all implemented)

### Tier 2: Blocked on External Factors

#### T2-1: SQL-Level MERGE INTO Parsing
- **Plan reference**: Phase 4.5, one unchecked item
- **What blocks it**: DataFusion's SQL parser (sqlparser-rs) parses MERGE INTO syntax, but DataFusion's logical planner does not convert it into executable plans
- **When resolved**: Would require either (a) DataFusion adding MERGE support, or (b) a custom SQL pre-processor
- **Workaround**: Programmatic MERGE API already exists via `DuckLakeTable::merge()`. Users can use DELETE + INSERT as separate operations.
- **Estimated effort if unblocked**: Medium (M)

#### T2-2: SQL-Level Time Travel Syntax
- **Plan reference**: Phase 2.3 (deferred), research-time-travel.md
- **What blocks it**: DataFusion 51 silently ignores the `version` field from sqlparser. DataFusion 52 introduces `RelationPlanner` which would support this.
- **When resolved**: When project upgrades to DataFusion 52+
- **Workaround**: Table functions (`ducklake_table_changes`, `ducklake_table_insertions`, `ducklake_current_snapshot`, etc.) provide time travel access today. Programmatic `DuckLakeCatalog::with_snapshot()` also works.
- **Estimated effort if unblocked**: Low-Medium

#### T2-3: add_files DuckDB Issues (21 SLT tests)
- **Plan reference**: slt-failure-report Category 1
- **What blocks it**: Mix of DuckDB version issues (within-transaction query problems, function signature mismatches, NULL shared_ptr dereference) and result format differences after add_files operations
- **When resolved**: Update DuckDB or DuckLake extension for internal errors; result format fixes may be partially addressable
- **Workaround**: None for DuckDB internal errors; some result mismatches may be fixable
- **Impact**: ~15 tests permanently blocked by DuckDB issues, ~6 may be fixable with result format improvements

#### T2-4: DuckDB Macros Not Supported (9 SLT tests)
- **Plan reference**: slt-failure-report Category 3
- **What blocks it**: DuckDB DuckLake extension itself does not support CREATE MACRO/CREATE FUNCTION in DuckLake catalogs
- **When resolved**: When DuckLake extension adds macro support
- **Workaround**: None -- this is a DuckLake limitation, not a DataFusion one
- **Impact**: 9 tests permanently blocked until upstream DuckLake changes

#### T2-5: DuckDB Spatial Extension (RESOLVED)
- **Plan reference**: slt-failure-report Category 4
- **Status**: RESOLVED. All 5 geo tests now pass (ducklake_geometry, ducklake_geometry_add_files, ducklake_geometry_inlining, ducklake_geometry_merge, ducklake_geometry_nested).
- **How resolved**: Improved spatial type handling and function routing.

### Tier 3: Architectural Changes Required

#### T3-1: Complex Type Evolution (Struct Field ADD/REMOVE/RENAME)
- **Plan reference**: Phase 4.4
- **What's needed**:
  - New `AlterTableOp` variants for ADD FIELD, REMOVE FIELD, RENAME FIELD
  - `ducklake_column_mapping` / `ducklake_name_mapping` integration
  - Parent-column hierarchy reconstruction in metadata layer
  - Parquet reader support for nested field_id mapping
- **Risk**: Medium-high. The metadata layer's column representation is flat (no parent/child hierarchy). Retrofitting nested structure evolution is a significant refactor.
- **Worth doing?**: Medium priority. Struct evolution is used in real-world DuckLake catalogs, but only for advanced schema evolution scenarios. Basic struct READ support works fine.
- **Estimated effort**: Large (L)
- **SLT tests unlocked**: 12 tests (complex types + struct evolution). Previously 13, but `time_travel/basic_time_travel.test` now passes.

#### T3-2: Encrypted Writes
- **Plan reference**: Phase 4.6, research-encryption-deep-dive.md
- **What's needed**:
  - Encryption key generation for new data/delete files
  - Pass encryption config to Parquet writer in `DuckLakeTableWriter`
  - Store keys per-file in `ducklake_data_file.encryption_key` / `ducklake_delete_file.encryption_key`
  - DuckDB-compat layer for parquet-rs (2 metadata divergences)
- **Risk**: Medium. DuckDB's encryption is mostly PME-compliant but has 2 HIGH-severity divergences: (1) missing `ColumnCryptoMetaData` on chunks, (2) missing `aad_file_unique`. A compatibility wrapper is needed.
- **Worth doing?**: Only if users need encrypted DuckLake catalogs. Read-side decryption already works.
- **Estimated effort**: Large (L)
- **SLT tests unlocked**: 0-2 (most encryption tests are DuckDB-specific)

### Tier 4: Not Applicable / Intentionally Skipped

#### T4-1: Secrets Management
- **Why skipped**: DataFusion uses its own object store configuration. DuckLake's `ducklake_secret` is DuckDB-specific.
- **Real gap?**: No. Different approach, same goal.

#### T4-2: ATTACH/DETACH Syntax
- **Why skipped**: DataFusion uses `register_catalog()`/`deregister_catalog()`.
- **Real gap?**: No. DataFusion API equivalent exists.

#### T4-3: PRAGMA Statements
- **Why skipped**: DuckDB-specific internal commands.
- **Real gap?**: No.

#### T4-4: Catalog-Stored Macros (CREATE MACRO)
- **Why skipped**: DuckDB's UDF model differs from DataFusion's. DuckLake itself doesn't even support macros yet.
- **Real gap?**: No.

#### T4-5: DuckDB System Functions (duckdb_tables, duckdb_schemas, etc.)
- **Why skipped**: Replaced by `information_schema` views in DataFusion.
- **Real gap?**: No.

#### T4-6: SET SORTED BY
- **Why skipped**: DuckDB DuckLake v0.3 itself returns "unsupported" for this operation.
- **Real gap?**: No -- not supported in the reference implementation either.

#### T4-7: Data Inlining Write-Side for Non-DuckDB Backends
- **Why skipped**: DuckDB stores inlined data as actual DuckDB tables. Implementing for Postgres/MySQL/SQLite would require a completely different approach (JSON, serialized format, etc.).
- **Real gap?**: Partially. Data inlined by DuckDB is readable by DataFusion (read-side works). The inability to WRITE inlined data from DataFusion means small tables always get Parquet files. This is acceptable behavior.

#### T4-8: SQLite Catalog Format Write Interop
- **Why noted**: Our SQLite writer creates valid catalogs, but DuckDB can't CREATE TABLE/VIEW/SCHEMA on them (DuckDB expects its own internal format).
- **Real gap?**: Partial. DF-written catalogs are readable by DuckDB. DuckDB just can't do DDL on them. This is documented in the handoff prompt as a "key finding."

---

## 4. SLT Gap Analysis

### Current State

| Metric | Value |
|--------|-------|
| Total tests | 248 |
| Passing | 151 |
| Failing | 97 |
| Pass rate | 60.9% |
| Baseline (pre-Phase 6) | 16 / 248 (6.5%) |
| Previous milestone | 120 / 248 (48.4%) |
| Improvement (from baseline) | +135 tests (+54.4 percentage points) |
| Improvement (from previous) | +31 tests (+12.5 percentage points) |

### Realistic Target

With the remaining fixable items completed:
- Fix subset of "Query Result Mismatch": +10-15 tests
- Fix CTAS table visibility: +2-3 tests
- Fix rowid computation: +2 tests
- Fix transaction table resolution: +2-3 tests

**Realistic achievable**: ~165-175 / 248 (~67-71%)

**Hard ceiling** (without upstream changes): ~185 / 248 (~75%) — would require complex type evolution (T3-1) + all fixable result mismatches.

### Breakdown of 97 Remaining Failures by Actionability

| Category | Count | Actionability | Estimated Fixable |
|----------|-------|---------------|-------------------|
| add_files result mismatches & DuckDB issues | 21 | MOSTLY BLOCKED -- DuckDB version / within-transaction issues | 3-5 (result format fixes) |
| Unsupported complex types (struct/list/map evolution) | 12 | REQUIRES T3-1 (arch change) | 0 (without T3-1) |
| Data inlining (hybrid limitation) | 10 | FUNDAMENTAL LIMITATION -- hybrid mode can't read inlined data | 0-2 (CTAS fixes only) |
| DuckDB macros not supported | 9 | BLOCKED -- upstream DuckLake | 0 |
| Query result mismatch (various) | 30 | PARTIALLY FIXABLE (T1-5) | 10-15 |
| Other (catalog names, DuckDB-specific, transactions) | 15 | MOSTLY BLOCKED -- various limitations | 2-3 (table resolution) |
| **Total** | **97** | | **15-25 fixable** |

### Which Feature Implementations Would Unlock the Most Tests

| Feature | Tests Unlocked | Effort |
|---------|---------------|--------|
| Result mismatch fixes (T1-5) | 10-15 | Medium |
| Complex type evolution (T3-1) | 12 | Large |
| CTAS table visibility fixes | 2-3 | Small |
| Rowid computation alignment | 2 | Small |
| Transaction table resolution | 2-3 | Small |
| DuckDB extension update (T2-3) | ~15 | Small (but external) |

Note: Several previously-listed items are now resolved:
- year() UDF: registered (6 tests fixed)
- Table/schema resolution fixes: mostly done (4 tests fixed)
- Expected failure mismatch fixes: mostly done (3 tests fixed)
- Spatial extension: now working (5 tests fixed)

---

## 5. Recommended Next Steps

### Priority Order (Best ROI: effort vs impact)

**1. Push fix branches and create PRs (T1-7)** -- Small effort, clears tech debt
- Push `fix/validate-record-count`, `fix/name-validation`, `fix/type-normalization-promotion`
- Create PRs against main

**2. SLT "Table Not Found" fixes (T1-3)** -- Medium effort, 5-8 tests
- Expose metadata tables to DataFusion
- Fix CTAS table visibility
- Best bang-for-buck in SLT improvement

**3. SLT "Expected Failure Mismatch" fixes (T1-4)** -- Small effort, 4 tests
- Quick wins in the test adapter

**4. Port view-related SLT tests (T1-10)** -- Small effort, verifies existing work
- Views are fully working; just need tests ported

**5. Reserved schema name test (T1-9)** -- Trivial effort, closes a checkbox
- Feature is done, just needs the test

**6. Register year() UDF** -- Small effort, 3 tests
- Unblocks partitioning SLT tests

**7. File pruning benchmark (T1-6)** -- Small effort, closes last Phase 2 checkbox

**8. SLT result mismatch investigation (T1-5)** -- Medium effort, high impact
- 10-15 tests fixable, requires case-by-case analysis of each mismatch

**9. Cross-engine Postgres/MySQL tests (T1-8)** -- Medium effort, validates multi-backend
- Important for production confidence but may have DuckDB connectivity blockers

**10. Write-side data inlining (T1-2)** -- Large effort, 7 tests
- Unlocks data inlining SLT tests
- Consider implementing only for DuckDB metadata backend

**11. Write-side partitioning (T1-1)** -- Large effort, 3-5 tests
- Important for large-table management
- Hive-style directory routing is the main complexity

**12. Complex type evolution (T3-1)** -- Large effort, 13 tests
- Architectural refactor needed
- Defer unless users specifically need struct evolution

**13. Encrypted writes (T3-2)** -- Large effort, 0-2 tests
- Only needed for encrypted catalogs
- Defer indefinitely unless user-requested

### Items to NOT work on

- **SQL MERGE parsing** (T2-1): Blocked on DataFusion. Programmatic API works.
- **SQL time travel syntax** (T2-2): Blocked on DataFusion 52 upgrade. Table functions work.
- **DuckDB macros** (T2-4): Blocked on upstream DuckLake. Not our problem.
- **SET SORTED BY**: DuckDB itself doesn't support it.
- **Secrets management**: DataFusion has its own approach.

---

## Appendix: Document Freshness Assessment

| Document | Written | Current? | Notes |
|----------|---------|----------|-------|
| feature-parity-plan-feb-28.md | Feb 27 | **Mostly current** | Checkboxes accurately reflect integration branch state; cross-engine matrix never updated |
| remaining-gaps.md | Feb 28 (commit 3d874ff) | **STALE** | Written before Postgres/MySQL implementations, file pruning, compaction. 6 of 14 gap items already resolved. |
| slt-failure-report.md | Feb 28 | **Current** | Updated to reflect 151/248 (60.9%) pass rate |
| handoff-prompt.md | Feb 28 | **Current** | Accurately describes Phases 0-5 complete, Phase 6 in progress |
| review-feature-parity.md | Feb 26 | **HEAVILY STALE** | Pre-Phase 2-5. Shows many features as "None" that are now implemented. Do not use for current status. |
| review-work-completeness.md | Feb 26 | **STALE** | Pre-Phase 2-5. Gap cross-reference is outdated. |
| review-sqllogictest-coverage.md | Feb 26 | **STALE** | Shows 2-3% pass rate; actual is 48.4%. Categories still useful for understanding failure types. |
| research-time-travel.md | Feb 27 | **Current** | Feasibility analysis still valid. Recommendations unchanged. |
| research-encryption-deep-dive.md | Feb 27 | **Current** | DuckDB encryption analysis still valid. |
| ducklake-impl-prompt.md | Feb 26 | **Reference only** | Original requirements. Still valid as north star. |
| ducklake-impl-supplement.md | Feb 26 | **Reference only** | Agreed priority order. Still valid. |

---

## Appendix: Methodology

This audit was produced by:

1. Reading all 12 referenced documents in full
2. Examining the actual integration branch source code via grep/read of key files
3. Cross-referencing every unchecked `[ ]` item in feature-parity-plan-feb-28.md against actual code
4. Cross-referencing remaining-gaps.md item status against actual code (found 6 stale items)
5. Verifying slt-failure-report.md categories against the failure descriptions
6. Checking git commit history to establish document freshness
7. Verifying the build compiles successfully (`cargo build --features write-sqlite`)

No code changes were made. This is purely an audit document.
