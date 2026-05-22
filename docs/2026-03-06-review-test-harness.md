# R9 Test Harness Review -- F-044 Test Coverage

## Summary

F-044 consolidated ~27 writer methods and ~20 provider methods into 6 macros (`impl_metadata_provider!`, `impl_writer_query_ops!`, `impl_writer_file_ops!`, `impl_writer_ddl_ops!`, `impl_writer_drop_inner!`, `impl_writer_drop_ops!`) plus a `SqlDialect` trait with 3 implementations. The existing test suite provides good integration coverage of the macro-generated code through end-to-end tests, but has specific gaps in unit testing certain macro groups, backend parity, and dialect trait verification. 11 findings identified (0 P0, 2 P1, 5 P2, 4 P3).

**Test counts**: 31 unit tests in `metadata_writer_sqlite.rs`, 30 tests in `postgres_metadata_writer_test.rs`, 29 tests in `mysql_metadata_writer_test.rs`, plus ~787+ integration tests across 67 test files. All 3 backends exercise the macro-generated code, but SQLite is the only backend tested in CI without Docker.

## Findings

### R9-T-001: No unit tests for SqlDialect trait methods (Priority: P2)
**File**: src/dialect.rs:1-387
**Description**: The `SqlDialect` trait (18 methods, 3 implementations) has zero unit tests. Each dialect produces different SQL fragments -- `ph()`, `upsert()`, `insert_or_ignore()`, `bool_lit()`, `cast_text()`, `cast_int()`, `clamp_zero()`, `for_update()`, `existence_check_is_count()`, `next_id_sql()` all vary by backend. The trait is critical infrastructure that all macro-generated code depends on, yet correctness is only validated indirectly through integration tests. A typo in a dialect method (e.g., MySQL's `upsert()` using `VALUES({c})` instead of `VALUES({c})`) would only be caught by Docker-dependent MySQL tests.
**Suggested fix**: Add a `#[cfg(test)] mod tests` in `dialect.rs` with unit tests for each dialect implementation. Key tests: `ph()` returns `?`/$N/`?`, `upsert()` generates correct ON CONFLICT/ON DUPLICATE KEY syntax, `insert_or_ignore()` generates correct INSERT OR IGNORE/INSERT IGNORE/ON CONFLICT DO NOTHING, `bool_lit()` returns 1/0 vs TRUE/FALSE, `next_id_sql()` returns correct SQL per entity.
**Effort**: S

### R9-T-002: SQLite writer unit tests missing coverage for 7 macro-generated methods (Priority: P2)
**File**: src/metadata_writer_sqlite.rs:1457-2596
**Description**: The SQLite `#[cfg(test)]` module (31 tests) covers the per-backend override methods well but is missing direct unit tests for these macro-generated methods:
- `record_snapshot_changes` (impl_writer_query_ops)
- `find_table_id` (impl_writer_query_ops)
- `list_active_table_ids` (impl_writer_query_ops) -- tested in PG/MySQL test files but not SQLite
- `register_column_stats` (impl_writer_file_ops) -- tested in PG/MySQL but not SQLite
- `register_delete_file` (impl_writer_file_ops) -- tested in PG/MySQL but not SQLite
- `register_dml_files` (impl_writer_file_ops) -- not directly unit-tested in any backend
- `create_view` / `drop_view` (impl_writer_ddl_ops) -- tested in PG/MySQL but not SQLite unit tests (covered by cross-engine integration tests)
- `drop_table` / `drop_schema` (impl_writer_drop_ops) -- tested in PG/MySQL but not SQLite unit tests

Since SQLite is the only backend routinely tested in CI (no Docker), missing SQLite unit tests means these macro methods only get validated when Docker tests run.
**Suggested fix**: Add SQLite unit tests for `register_delete_file`, `register_column_stats`, `create_view`/`drop_view`, `drop_table`/`drop_schema`, `list_active_table_ids`, `find_table_id`, and `record_snapshot_changes`. Many can mirror the existing PG/MySQL test patterns.
**Effort**: M

### R9-T-003: MySQL LAST_INSERT_ID path has no specific unit test (Priority: P1)
**File**: src/metadata_writer_impl.rs:1036-1062 (register_delete_file), src/metadata_writer_impl.rs:478-503 (register_data_file)
**Description**: The macro generates a `supports_returning()` branch: SQLite/PG use `RETURNING data_file_id`, MySQL uses `(last_id_fn)(&mut tx).await?`. The `last_insert_id` function (src/metadata_writer_mysql.rs:371) calls `SELECT LAST_INSERT_ID()`. While MySQL integration tests exercise this path, there is no test that specifically verifies the LAST_INSERT_ID fallback produces the correct file ID. If the `last_insert_id` function returned the wrong value (e.g., from a different table's auto-increment), it would silently corrupt metadata.
**Suggested fix**: Add a MySQL-specific test that calls `register_data_file` twice and verifies the returned IDs are sequential and correct. Also add a test for `register_delete_file` verifying the returned ID. These exist as Docker-dependent tests but should verify the actual ID values, not just "no error".
**Effort**: S

### R9-T-004: `register_dml_files` has no direct unit test in any backend (Priority: P1)
**File**: src/metadata_writer_impl.rs:753-994
**Description**: `register_dml_files` is the most complex macro-generated method (~240 lines). It handles INSERT+DELETE file pairs with optional column stats and partition values. It is exercised indirectly by DELETE/UPDATE/MERGE integration tests, but has no direct unit test that verifies its internal logic (e.g., correct linking of delete files to data files, partition value registration, column stats registration, table stats updates). This is the highest-risk method for a macro migration bug because its internal branching (RETURNING vs LAST_INSERT_ID, partition values, column stats) interacts with multiple dialect methods.
**Suggested fix**: Add a direct unit test for `register_dml_files` in at least the SQLite writer that:
1. Registers a DML operation with both insert and delete files
2. Verifies the returned file IDs
3. Verifies the delete file's `data_file_id` linkage
4. Verifies partition values and column stats are recorded
**Effort**: M

### R9-T-005: `#[allow(dead_code)]` on dialect structs may mask unused method issues (Priority: P3)
**File**: src/dialect.rs:4, 74, 192, 293
**Description**: Both the `SqlDialect` trait and all three struct implementations (`SqliteDialect`, `PostgresDialect`, `MySqlDialect`) are annotated with `#[allow(dead_code)]`. This suppresses compiler warnings that would otherwise flag methods that are defined but never called from macro-generated code. If a macro is refactored to stop using a dialect method, the dead code won't be detected.
**Suggested fix**: Remove `#[allow(dead_code)]` from the structs and trait, and instead annotate only the specific methods that are legitimately unused (e.g., MySQL's `next_id_sql` which is a trait-completeness stub). Alternatively, add `#[cfg(test)]` tests that exercise each method to eliminate the dead_code warnings naturally.
**Effort**: S

### R9-T-006: Cross-engine tests only cover SQLite backend without Docker (Priority: P2)
**File**: tests/cross_engine_tests.rs, tests/cross_engine_alter_tests.rs, tests/cross_engine_ddl_tests.rs
**Description**: The primary cross-engine test suite (tests/cross_engine_*.rs excluding MySQL/PG-specific files) requires `write-sqlite` + `metadata-duckdb` features and uses SQLite as the catalog backend. MySQL and PostgreSQL cross-engine tests exist (tests/cross_engine_mysql_tests.rs with 8 tests, tests/cross_engine_postgres_tests.rs with 5 tests) but are `#[ignore]`-gated behind Docker. This means CI only validates the macro-generated SQL against SQLite -- if a dialect method generates incorrect SQL for PG or MySQL, it won't be caught without Docker.
**Suggested fix**: This is a structural limitation. Consider adding a "dry-run" mode that validates the generated SQL strings without executing them against a database, allowing dialect-specific SQL verification without Docker.
**Effort**: L

### R9-T-007: 3 pre-existing DuckDB assertion crashes in cross_engine_alter_tests (Priority: P3)
**File**: tests/cross_engine_alter_tests.rs
**Description**: Three cross-engine ALTER TABLE tests crash with DuckDB internal assertions. These are pre-existing upstream DuckDB bugs (documented since R7) unrelated to F-044. The specific tests are: `test_df_rename_table_duckdb_reads`, `test_duckdb_rename_table_df_reads`, and one other ALTER TABLE test where DuckDB crashes reading DF-altered metadata. These are not `#[ignore]`-annotated, meaning they produce test noise.
**Suggested fix**: Add `#[ignore = "pre-existing DuckDB assertion crash - upstream bug"]` annotations to the 3 affected tests to reduce noise. Track the upstream DuckDB issue.
**Effort**: S

### R9-T-008: `recompute_table_column_stats` macro has no direct test (Priority: P2)
**File**: src/metadata_writer_impl.rs:5-131
**Description**: The `impl_recompute_table_column_stats!` macro generates a complex function that deletes existing table-level column stats and re-aggregates from per-file stats using type-aware comparison (`stat_value_less_than`). While the comparison helpers are well-tested (4 unit tests), the macro-generated aggregation function itself is only tested indirectly through `replace_table_files` tests. A bug in the aggregation logic (e.g., incorrect MIN/MAX computation across files) would not be caught by existing tests.
**Suggested fix**: Add a test that creates multiple data files with known per-file column stats, calls `replace_table_files` (which triggers recomputation), and verifies the aggregated table-level stats have correct MIN, MAX, and contains_null values.
**Effort**: S

### R9-T-009: Some integration tests only verify "no error" without checking values (Priority: P2)
**File**: Various test files
**Description**: Several integration tests follow a pattern of calling a writer method and only checking `.unwrap()` (no error) or checking row count without verifying actual data values. Examples:
- Some `register_data_file` tests only check the returned ID is non-zero
- Some `end_table_files` tests only check count but not which files were ended
- Cross-engine tests that verify DuckDB can read data but don't compare specific row values

This creates false positive risk: the macro-generated SQL could produce subtly wrong results (e.g., incorrect row_id_start, wrong snapshot linkage) that wouldn't be caught.
**Suggested fix**: Audit key integration tests and add value-level assertions. Priority targets: `register_data_file` should verify `row_id_start` and `file_size_bytes` in the database, `end_table_files` should verify the correct `end_snapshot` was set.
**Effort**: M

### R9-T-010: PG/MySQL writer test files have no `#[cfg(test)]` in-source tests (Priority: P3)
**File**: src/metadata_writer_postgres.rs, src/metadata_writer_mysql.rs
**Description**: Unlike `metadata_writer_sqlite.rs` (31 in-source tests), the PG and MySQL writer files have zero `#[cfg(test)]` modules. Their tests are in separate `tests/postgres_metadata_writer_test.rs` (30 tests) and `tests/mysql_metadata_writer_test.rs` (29 tests). This is not a bug per se, but the external test files cannot access `pub(crate)` internals. This means backend-specific internal behavior (e.g., `next_sequence_id`, `last_insert_id`, `next_sequence_ids`) is only tested through the public MetadataWriter trait.
**Suggested fix**: Consider adding `#[cfg(test)]` modules to PG/MySQL writers for testing internal functions like `last_insert_id` and `next_sequence_id` directly. Low priority since these are exercised through integration tests.
**Effort**: S

### R9-T-011: 1 flaky view test and SQL write tests remain `#[ignore]`-annotated (Priority: P3)
**File**: tests/sql_write_tests.rs:67, :286, :366, :432, :522
**Description**: 5 SQL write tests are `#[ignore]` with reasons like "CTAS not yet supported", "INSERT OVERWRITE: column count mismatch with virtual columns", etc. These represent known limitations of the SQL path. The view test flakiness (mentioned in previous reviews) stems from view registration timing. These are not F-044 regressions but pre-existing limitations that should be tracked.
**Suggested fix**: Create tracking issues for the 5 ignored SQL write tests and the flaky view behavior. These represent real feature gaps.
**Effort**: S

## Test Coverage Matrix

| Macro / Method Group | SQLite Unit | PG Unit | MySQL Unit | Integration |
|---|---|---|---|---|
| **impl_writer_query_ops** (8 methods) | | | | |
| get_data_path | Yes | Yes | Yes | Yes |
| set_data_path | Yes | Yes | Yes | Yes |
| record_snapshot_changes | -- | -- | -- | Indirect |
| list_active_table_ids | -- | Yes | Yes | Yes |
| get_active_columns | -- | Yes | Yes | Yes |
| find_table_id | -- | -- | -- | Indirect |
| register_file_partition_value | -- | -- | -- | Indirect |
| get_active_partition_columns | -- | -- | -- | Indirect |
| **impl_writer_file_ops** (6 methods) | | | | |
| register_column_stats | -- | Yes | -- | Yes |
| register_data_file | Yes | Yes | Yes | Yes |
| end_table_files | Yes | Yes | Yes | Yes |
| replace_table_files | Yes | -- | -- | Indirect |
| register_dml_files | -- | -- | -- | Indirect only |
| register_delete_file | -- | Yes | Yes | Yes |
| **impl_writer_ddl_ops** (7 methods) | | | | |
| create_view | -- | Yes | Yes | Yes |
| drop_view | -- | Yes | Yes | Yes |
| rename_view | Yes | Yes | Yes | Yes |
| alter_table | Yes | Yes | Yes | Yes |
| rename_table | Yes | Yes | Yes | Yes |
| set_table_comment | Yes | Yes | Yes | Yes |
| set_column_comment | Yes | Yes | Yes | Yes |
| **impl_writer_drop_ops** (4+ methods) | | | | |
| drop_table | -- | Yes | Yes | Yes |
| drop_schema | -- | Yes | Yes | Yes |
| drop_table_checked | -- | Yes | Yes | Yes |
| drop_schema_checked | -- | -- | Yes | Yes |
| **impl_metadata_provider** (20 methods) | N/A | N/A | N/A | Yes (all) |
| **SqlDialect** (18 methods x 3 impls) | -- | -- | -- | Indirect |
| **Per-backend overrides** | | | | |
| create_snapshot | Yes | Yes | Yes | Yes |
| get_or_create_schema | Yes | Yes | Yes | Yes |
| get_or_create_table | Yes | Yes | Yes | Yes |
| set_columns | Yes | Yes | Yes | Yes |
| write_transaction_inner | Indirect | Indirect | Indirect | Yes |
| begin_checked_write_transaction | -- | Yes | -- | Yes |

Legend: "Yes" = direct unit test, "--" = no direct test, "Indirect" = tested through higher-level operations, "N/A" = not applicable
