# R5 Codex Review — 2026-03-03

## Summary
- Codex runs: 6
- Total findings: 39 (4 P0, 16 P1, 15 P2, 4 P3)
- Known deferred (excluded): F-036, F-044, F-045, R4-S-018, R4-S-036, R4-S-040
- False positive excluded: MERGE duplicate-source-row detection (already implemented at merge_exec.rs:408-417)

## Findings

### CX-001: WriteMode::Replace can permanently drop table data
- **Severity**: P0
- **Source**: Run 1
- **Files**: `src/table_writer.rs:872-897`
- **Description**: In Replace mode, `end_table_files()` is called first (ending old files), then `register_data_file()` is called separately. If `register_data_file()` fails after `end_table_files()` succeeds, the snapshot has no active data files — all data is lost.
- **Suggested fix**: Make replace atomic in metadata with a single transaction API (`replace_table_files`), or register the new file first and only then end old files within one transaction.

### CX-002: Inlining flush path can silently lose existing inlined rows
- **Severity**: P0
- **Source**: Run 1
- **Files**: `src/table_writer.rs:321-351`
- **Description**: `get_inlined_data_as_batch` errors are silently swallowed (`if let Ok(...)`), but `had_inline` still causes `clear_inlined_data` after the Parquet write. This clears rows that were never included in the Parquet file.
- **Suggested fix**: Do not swallow read/convert errors; fail the operation before writing/clearing, or only clear when inline rows were successfully materialized and included.

### CX-003: Date32/Date64 partition pruning produces wrong comparisons
- **Severity**: P0
- **Source**: Run 4
- **Files**: `src/table.rs:1269-1270`
- **Description**: `scalar_value_to_partition_string()` converts `Date32`/`Date64` filter literals to raw epoch integers (e.g. `19737`), but partition values are stored as formatted dates (e.g. `2024-01-15`). Equality pruning can wrongly exclude matching files, producing incorrect query results.
- **Suggested fix**: Format date literals to the same canonical string used for partition value materialization (`YYYY-MM-DD` via chrono), not `to_string()` on raw day/ms counts.

### CX-004: normalize_value creates false positives in test assertions
- **Severity**: P0
- **Source**: Run 6
- **Files**: `tests/common/test_utils.rs:284`
- **Description**: `normalize_value` parses every numeric string as `f64` and rounds to 6 decimals (`format!("{:.6}", f)`), which can make different values compare equal (large integers beyond 2^53, close floats). This creates real false positives in `assert_results_eq`.
- **Suggested fix**: Only apply tolerance-based comparison to explicitly float-typed columns, and compare integers/decimals exactly.

### CX-005: commit_uploaded_files non-atomic in append mode
- **Severity**: P1
- **Source**: Run 1
- **Files**: `src/table_writer.rs:637-653`
- **Description**: In append mode, files are registered one at a time. A mid-loop failure can leave partially committed files (data file row exists but partition values/stats missing).
- **Suggested fix**: Add a transactional metadata API for append batch registration, or compensate on failure.

### CX-006: column_stats failure propagates after data committed
- **Severity**: P1
- **Source**: Run 1
- **Files**: `src/table_writer.rs:516-519`
- **Description**: `write_parquet_with_setup` reports failure after data is already committed if `register_column_stats` fails (`?` propagation), causing retry-driven duplicate writes.
- **Suggested fix**: Treat column-stats registration as non-fatal warning once `register_data_file` succeeds (match `finish()` behavior).

### CX-007: Insert/append path does not enforce NOT NULL constraints
- **Severity**: P1
- **Source**: Run 1
- **Files**: `src/table_writer.rs:214`, `src/table_writer.rs:732`
- **Description**: Unlike UPDATE/MERGE, the regular insert/append path does not call `validate_not_null_constraints` before writing Parquet. Invalid nulls can be persisted and break DuckDB interop.
- **Suggested fix**: Call `validate_not_null_constraints` in insert/append/write paths prior to writing.

### CX-008: UPDATE can panic on invalid assignment column index
- **Severity**: P1
- **Source**: Run 1
- **Files**: `src/update_exec.rs:374`
- **Description**: `columns[*col_idx] = ...` without bounds check turns planner/input issues into runtime panic.
- **Suggested fix**: Validate `column_index < table_schema.fields().len()` during plan construction and return `DataFusionError::Plan`.

### CX-009: SQLite INSERT OR REPLACE erases snapshot_changes columns
- **Severity**: P1
- **Source**: Run 2
- **Files**: `src/metadata_writer_sqlite.rs:559`, `:704`, `:907`, `:1517`, `:1846`, `:1915`, `:2032`, `:2290`, `:2448`, `:2521`, `:2614`
- **Description**: `INSERT OR REPLACE INTO ducklake_snapshot_changes (snapshot_id, changes_made)` deletes then re-inserts, silently erasing `author`, `commit_message`, and `commit_extra_info` columns.
- **Suggested fix**: Use SQLite UPSERT: `INSERT ... ON CONFLICT(snapshot_id) DO UPDATE SET changes_made=excluded.changes_made`.

### CX-010: set_data_path non-atomic in Postgres/MySQL
- **Severity**: P1
- **Source**: Run 2
- **Files**: `src/metadata_writer_postgres.rs:1160-1172`, `src/metadata_writer_mysql.rs:1263-1275`
- **Description**: `DELETE` then `INSERT` without a transaction can leave the catalog with missing or duplicate `data_path` under concurrent writers or failures.
- **Suggested fix**: Wrap in a transaction and enforce uniqueness with backend-appropriate unique index/upsert.

### CX-011: MySQL ID allocation race-prone with MAX+1
- **Severity**: P1
- **Source**: Run 2
- **Files**: `src/metadata_writer_mysql.rs:470`, `:541`, `:923`, `:968`, `:1622`, `:1925`, `:2051`
- **Description**: `MAX(...)+1 ... FOR UPDATE` on non-unique columns is race-prone (especially empty-table/bootstrap) and can generate duplicate logical IDs for `table_id`, `column_id`, `view_id`, `partition_id`.
- **Suggested fix**: Move to `AUTO_INCREMENT`/sequence-backed columns or dedicated sequence table with proper locking, and enforce unique constraints.

### CX-012: Inlined-table resolution ignores snapshot/version semantics
- **Severity**: P1
- **Source**: Run 3
- **Files**: `src/metadata_provider_sqlite.rs:853`, `src/metadata_provider_postgres.rs:879`, `src/metadata_provider_mysql.rs:897`, `src/metadata_provider_duckdb.rs:85`
- **Description**: `SELECT ... FROM ducklake_inlined_data_tables WHERE table_id = ?` has no snapshot/schema-version filter and no `ORDER BY`, so historical reads can pick the wrong inlined table.
- **Suggested fix**: Resolve `schema_version` for `snapshot_id`, then select with `schema_version <= snapshot_schema_version ORDER BY schema_version DESC LIMIT 1`.

### CX-013: DuckDB delete-delta boundary inclusive vs exclusive mismatch
- **Severity**: P1
- **Source**: Run 3
- **Files**: `src/metadata_provider_duckdb.rs:666`
- **Description**: `get_delete_files_added_between_snapshots` uses inclusive lower bound (`BETWEEN start AND end`), unlike other providers' `> start AND <= end`. This can double-report boundary snapshot changes.
- **Suggested fix**: Change DuckDB delete-delta SQL to strict lower bound (`> start_snapshot`).

### CX-014: DuckLakeSchema snapshot not refreshed after DDL writes
- **Severity**: P1
- **Source**: Run 4
- **Files**: `src/schema.rs:75`, `:314`, `:361`
- **Description**: `DuckLakeSchema` pins `snapshot_id` as immutable and does not refresh after `register_table`/`deregister_table`, so subsequent lookups can read stale metadata.
- **Suggested fix**: Store schema snapshot as `AtomicI64` (shared with catalog) and update after write operations.

### CX-015: schema() returns full_schema but statistics() sized to base only
- **Severity**: P1
- **Source**: Run 4
- **Files**: `src/table.rs:1281`, `:1289-1310`
- **Description**: `schema()` returns `full_schema` (base + virtual columns), but `statistics()` builds column_statistics only for base columns, violating DataFusion's expected alignment.
- **Suggested fix**: Return stats vector sized to `full_schema` (append `unknown` stats for virtual columns).

### CX-016: ducklake_list_files strip_prefix can mis-trim paths
- **Severity**: P1
- **Source**: Run 5
- **Files**: `src/table_functions.rs:150-153`, `:167-169`
- **Description**: Raw `strip_prefix(&data_path)` on full paths can mis-trim when `data_path` is only a string prefix (e.g. `/data` vs `/database`), returning incorrect file paths.
- **Suggested fix**: Only strip when path starts with `data_path + "/"` after canonicalization.

### CX-017: Table function dot-splitting breaks quoted identifiers
- **Severity**: P1
- **Source**: Run 5
- **Files**: `src/table_functions.rs:344`
- **Description**: Parsing `schema.table` by splitting on first dot breaks for valid quoted identifiers containing dots and 3-part names (`catalog.schema.table`).
- **Suggested fix**: Implement SQL-identifier-aware parsing or support explicit separate arguments.

### CX-018: DuckDB decode error silently converted to "NULL" in tests
- **Severity**: P1
- **Source**: Run 6
- **Files**: `tests/hybrid_asyncdb.rs:420`
- **Description**: In transaction-mode reads, any DuckDB decode error is silently converted to `"NULL"`, masking real conversion/type bugs and potentially letting tests pass incorrectly.
- **Suggested fix**: Return an error instead of substituting `"NULL"`.

### CX-019: Zero-row DML returns StatementComplete instead of count
- **Severity**: P1
- **Source**: Run 6
- **Files**: `tests/hybrid_asyncdb.rs:366`
- **Description**: DML rowcount output only returned when `changed_rows > 0`; zero-row DML returns `StatementComplete`, breaking `query I` semantics for zero-count assertions.
- **Suggested fix**: Always return `DBOutput::Rows` with one integer row for DML, including `"0"`.

### CX-020: rewrite_table_references does raw substring replacement
- **Severity**: P1
- **Source**: Run 6
- **Files**: `tests/hybrid_asyncdb.rs:152`
- **Description**: Raw substring rewriting of `ducklake.` without SQL parsing can rewrite inside string literals/comments and corrupt queries.
- **Suggested fix**: Rewrite qualified table refs via parsed SQL AST or token-aware logic.

### CX-021: UPDATE buffers all matched rows in memory
- **Severity**: P2
- **Source**: Run 1
- **Files**: `src/update_exec.rs:244`, `:447-471`
- **Description**: All matched rows are accumulated in `updated_batches` before writing a single Parquet file. Large updates can cause OOM.
- **Suggested fix**: Stream updated batches directly to Parquet writer or chunked temp files.

### CX-022: MERGE uses O(N*M) nested loop join
- **Severity**: P2
- **Source**: Run 1
- **Files**: `src/merge_exec.rs:374-428`
- **Description**: Nested target-row x source-row scans are quadratic and degrade severely on non-trivial datasets.
- **Suggested fix**: Build hash index on source join keys to avoid quadratic scans.

### CX-023: flush_inlined_data maps unknown types to Utf8
- **Severity**: P2
- **Source**: Run 1
- **Files**: `src/table_writer.rs:411`
- **Description**: `unwrap_or(Utf8)` silently writes wrong physical schema for unknown DuckLake types, breaking DuckDB interoperability.
- **Suggested fix**: Fail on unknown type mapping instead of defaulting.

### CX-024: MySQL VARCHAR length limits create cross-backend incompatibility
- **Severity**: P2
- **Source**: Run 2
- **Files**: `src/metadata_writer_mysql.rs:23-304`
- **Description**: Many MySQL fields are bounded to `VARCHAR(255/1024)` where SQLite/Postgres are unbounded, causing truncation/error risk for long paths/stats.
- **Suggested fix**: Align MySQL to `TEXT` for variable-length metadata fields.

### CX-025: initialize_schema not atomic across backends
- **Severity**: P2
- **Source**: Run 2
- **Files**: `src/metadata_writer_sqlite.rs:1528`, `src/metadata_writer_postgres.rs:1178`, `src/metadata_writer_mysql.rs:1283`
- **Description**: Schema initialization is not a single atomic unit; failures can leave partially initialized catalog state.
- **Suggested fix**: Use one transaction where backend permits (Postgres/SQLite); add idempotent retry for MySQL.

### CX-026: DuckDB table-existence uses unwrap_or(false) suppressing errors
- **Severity**: P2
- **Source**: Run 3
- **Files**: `src/metadata_provider_duckdb.rs:100`, `:602`
- **Description**: `.unwrap_or(false)` suppresses DB errors and silently returns empty/0, masking real catalog/query failures.
- **Suggested fix**: Handle `QueryReturnedNoRows` explicitly but propagate all other errors.

### CX-027: get_table_row_count non-atomic across separate statements
- **Severity**: P2
- **Source**: Run 3
- **Files**: `src/metadata_provider_sqlite.rs:938-968`, `src/metadata_provider_postgres.rs:969-998`, `src/metadata_provider_mysql.rs:739-770`, `src/metadata_provider_duckdb.rs:499-508`
- **Description**: File count and inlined count computed via separate statements without transaction/snapshot pin; concurrent changes can produce mixed-state totals.
- **Suggested fix**: Run both counts in one SQL statement (CTE/scalar subquery) or wrap in single transaction.

### CX-028: Filters advertised as Inexact but not pushed to Parquet
- **Severity**: P2
- **Source**: Run 4
- **Files**: `src/table.rs:1387`, `:571`, `:797`, `:853`
- **Description**: `supports_filters_pushdown()` reports all filters as `Inexact`, but scan planning never passes filters into Parquet/DataSource scan config, so pushdown is advertised but not implemented.
- **Suggested fix**: Wire filters into scan config or return `Unsupported` for filters not actually pushed.

### CX-029: join_paths globally normalizes interior double slashes
- **Severity**: P2
- **Source**: Run 4
- **Files**: `src/path_resolver.rs:277-279`
- **Description**: `join_paths()` globally normalizes repeated `/`, which can rewrite valid object-store keys containing `//`.
- **Suggested fix**: Only normalize the join boundary (avoid accidental double separator at concat point) and preserve interior path bytes.

### CX-030: CDC projection HashMap dedup breaks duplicate column projections
- **Severity**: P2
- **Source**: Run 5
- **Files**: `src/cdc_common.rs:88`
- **Description**: `table_idx_pos` built with `HashMap<idx, pos>` collapses duplicate projected table columns, producing incorrect reorder map in `SELECT col, col, ...` cases.
- **Suggested fix**: Build reorder indices position-by-position without deduplicating.

### CX-031: Delete file size unwrap_or(0) hides metadata errors
- **Severity**: P2
- **Source**: Run 5
- **Files**: `src/table_deletions.rs:119-132`
- **Description**: `unwrap_or(0)` for delete file size/footer causes silent failure (opaque file-read behavior) instead of a clear metadata error.
- **Suggested fix**: Treat missing size for present delete files as a hard planning error.

### CX-032: Snapshot bounds only accept Int32/Int64 literals
- **Severity**: P2
- **Source**: Run 5
- **Files**: `src/table_functions.rs:377-388`
- **Description**: Table change functions only accept `Int32`/`Int64` literals for snapshot bounds. DuckDB workflows use wider numeric types, causing avoidable interop failures.
- **Suggested fix**: Accept wider scalar numeric variants (`UInt*`, `Decimal*`) and/or timestamp-to-snapshot resolution.

### CX-033: table_changes only emits Insert, never Delete
- **Severity**: P2
- **Source**: Run 5
- **Files**: `src/table_changes.rs:549`, `:45`
- **Description**: `ducklake_table_changes()` only reads added data files (always `ChangeType::Insert`). `ChangeType::Delete` exists but is unused. DuckDB DuckLake `table_changes` is expected to represent full CDC (insert + delete).
- **Suggested fix**: Union insert plans with deletion plans so `ducklake_table_changes()` returns both change types.

### CX-034: cross_engine_df_write_duckdb_read doesn't check float column
- **Severity**: P2
- **Source**: Run 6
- **Files**: `tests/cross_engine_tests.rs:213`
- **Description**: Asserts `id` and `name` but never validates `score`, so float serialization/precision regressions slip through.
- **Suggested fix**: Assert full row equality including `score`.

### CX-035: Timestamp conversion pre-1970 can panic
- **Severity**: P2
- **Source**: Run 6
- **Files**: `tests/common/test_utils.rs:36`, `:131`
- **Description**: Timestamp conversion uses `%` on potentially negative epoch values and casts to `u32`, which can panic for pre-1970 timestamps.
- **Suggested fix**: Use `div_euclid`/`rem_euclid` before constructing timestamps.

### CX-036: MySQL snapshot change format inconsistent
- **Severity**: P3
- **Source**: Run 2
- **Files**: `src/metadata_writer_mysql.rs:2410`
- **Description**: MySQL uses `"Altered table (id={})"` format versus other backends' `"altered_table:{}"`, breaking downstream parsers expecting uniform DuckLake change tokens.
- **Suggested fix**: Normalize MySQL to the same canonical token format.

### CX-037: nulls_allowed NULL→true coercion hides catalog corruption
- **Severity**: P3
- **Source**: Run 3
- **Files**: `src/metadata_provider_sqlite.rs:173`, `src/metadata_provider_postgres.rs:168`, `src/metadata_provider_mysql.rs:171`, `src/metadata_provider_duckdb.rs:219`
- **Description**: `nulls_allowed` read as `Option<bool>` with `unwrap_or(true)` silently widens nullability when catalog is corrupted.
- **Suggested fix**: Treat NULL as an error or gate behind compatibility flag with warning.

### CX-038: test_table_rewrite uses contains() instead of exact match
- **Severity**: P3
- **Source**: Run 6
- **Files**: `tests/hybrid_asyncdb.rs:758`
- **Description**: `contains("ducklake.main.test")` instead of exact SQL equality allows malformed rewrites to pass.
- **Suggested fix**: Assert exact expected SQL strings.

### CX-039: cross_engine_null_handling test coverage gap
- **Severity**: P3
- **Source**: Run 6
- **Files**: `tests/cross_engine_tests.rs:457`
- **Description**: Only checks two NULL cells plus row count, leaving most fields unverified.
- **Suggested fix**: Compare full expected result sets for both engines with `assert_results_eq`.
