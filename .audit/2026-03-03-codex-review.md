# R4 Codex Review — 2026-03-03

## Overview
- **Review tool**: OpenAI Codex CLI v0.105.0 (gpt-5.3-codex)
- **Review rounds**: 5 (core write path, metadata writers, read path/providers, type system/supporting code, test infrastructure)
- **Raw findings**: 24
- **By priority**: 3 P0, 12 P1, 7 P2, 2 P3

---

## Round 1: Core Write Path

Source files: `src/table_writer.rs`, `src/insert_exec.rs`, `src/delete_exec.rs`, `src/update_exec.rs`, `src/merge_exec.rs`

### R4-CX-001 — P0: Inline data deleted before durable replacement exists
- **File(s)**: `src/table_writer.rs:422-427`, `src/table_writer.rs:319-331`
- **Description**: Both `flush_inlined_data` and the inline-threshold path call `clear_inlined_data()` *before* Parquet upload and metadata registration. Any upload or metadata failure after clearing permanently loses table data — the old inlined rows are gone and the new Parquet file was never committed.
- **Assessment**: Valid. The clear should happen *after* successful Parquet commit, or within the same transaction that registers the new file.
- **Suggested fix**: Move `clear_inlined_data()` to after successful `write_parquet_with_setup()` return, or wrap clear+write in compensating logic.

### R4-CX-002 — P0: Write path / table path mismatch causes missing data files
- **File(s)**: `src/table_writer.rs:454-458`, `src/table_writer.rs:494-498`, `src/metadata_writer_sqlite.rs:444`
- **Description**: `write_parquet_with_setup` uploads files to `<base>/<schema_name>/t<table_id>/<uuid>.parquet` and registers only `<uuid>.parquet` as a relative path. But table creation sets the table path to `<table_name>/` (relative to schema). Readers resolve files as `<base>/<schema_name>/<table_name>/<uuid>.parquet`, which is a different directory from where the file was actually written (unless `table_name == "t<table_id>"`).
- **Assessment**: Valid. The upload path uses `t{table_id}/` while the registered table path uses `table_name/`. This means the registered relative path resolves to a non-existent location on read.
- **Suggested fix**: Either upload to `<schema_name>/<table_name>/` (matching the stored table path), or store the table path as `t{table_id}/` to match the upload path.

### R4-CX-003 — P0: Partitioned commit non-atomic — partial metadata on failure
- **File(s)**: `src/table_writer.rs:570-622`, `src/insert_exec.rs:823-825`
- **Description**: In Replace mode, `commit_uploaded_files` calls `end_table_files()` first, then iterates through files calling `register_data_file`, `register_column_stats`, and `register_file_partition_value`. If any registration fails mid-loop, old files are already ended but not all new files are registered. No rollback or compensating cleanup occurs.
- **Assessment**: Valid. The end+register sequence should be within a single metadata transaction. Currently each `register_data_file` is its own transaction.
- **Suggested fix**: Wrap the entire end+register loop in a single metadata transaction, or implement compensating rollback on partial failure.

### R4-CX-004 — P1: `finish()` cleanup deletes already-committed metadata file
- **File(s)**: `src/table_writer.rs:831-843`, `src/table_writer.rs:870-877`
- **Description**: If `register_data_file` succeeds but `register_column_stats` fails, the `finish()` error path deletes the uploaded Parquet file. But metadata already references that file, creating a dangling reference.
- **Assessment**: Valid but narrow — requires `register_column_stats` to fail after `register_data_file` succeeds, which is an unusual failure mode. Still a real data integrity risk.
- **Suggested fix**: Track commit stage; skip file deletion if metadata already references the file.

### R4-CX-005 — P1: NULL filter predicate incorrectly treated as match in DELETE/UPDATE
- **File(s)**: `src/delete_exec.rs:290-293`, `src/update_exec.rs:327-330`
- **Description**: Filter results use `mask.value(i)` without checking null validity. Arrow's `BooleanArray::value()` reads the value buffer directly, ignoring the null bitmap. For SQL WHERE semantics, NULL should be treated as non-match (false). A null filter result can be interpreted as true, incorrectly matching rows for deletion/update.
- **Assessment**: Valid. Should use `mask.value(i) && mask.is_valid(i)` or the equivalent `mask.is_valid(i) && mask.value(i)` check.
- **Suggested fix**: Replace `mask.value(i)` with a null-aware check, e.g., `mask.value(i) && !mask.is_null(i)`.

### R4-CX-006 — P2: DML returns error after successful metadata commit on `record_snapshot_changes` failure
- **File(s)**: `src/delete_exec.rs:387-401`, `src/update_exec.rs:505-518`, `src/merge_exec.rs:609-622`
- **Description**: `register_dml_files` commits data/delete files first, then `record_snapshot_changes` is called separately. If the change-tracking write fails, the operation returns an error even though the DML metadata is already committed. Retrying could duplicate effects.
- **Assessment**: Valid but low severity — `record_snapshot_changes` is metadata bookkeeping (CDC). The DML itself is committed. Downgraded from codex's P1 to P2.
- **Suggested fix**: Either include `record_snapshot_changes` in the `register_dml_files` transaction, or demote the failure to a warning.

### R4-CX-007 — P1: UPDATE/MERGE skip NOT NULL constraint validation
- **File(s)**: `src/update_exec.rs:359-376`, `src/merge_exec.rs:518-538`, `src/merge_exec.rs:543-574`
- **Description**: INSERT explicitly validates non-nullable columns before writing. UPDATE and MERGE write transformed/source rows directly without equivalent NOT NULL checks, allowing NULLs into non-nullable table columns.
- **Assessment**: Valid. The constraint check in INSERT should be shared or replicated in UPDATE/MERGE write paths.
- **Suggested fix**: Extract the NOT NULL validation from INSERT into a shared helper and call it before writing in UPDATE/MERGE paths.

---

## Round 2: Metadata Writers

Source files: `src/metadata_writer_sqlite.rs`, `src/metadata_writer_postgres.rs`, `src/metadata_writer_mysql.rs`, `src/metadata_writer.rs`

### R4-CX-020 — P1: Drop operations create empty snapshots for already-dropped objects
- **File(s)**:
  - `src/metadata_writer_sqlite.rs:611-700,703-805,1645-1693`
  - `src/metadata_writer_postgres.rs:524-610,613-715,1466-1515`
  - `src/metadata_writer_mysql.rs:606-696,699-802,1589-1638`
- **Description**: `drop_view`, `drop_table_inner`, and `drop_schema_inner` create a new snapshot and record `dropped_*` metadata without first checking that the target object is actually active. Dropping an already-dropped or non-existent ID commits a spurious snapshot with misleading change history.
- **Assessment**: Valid. Should verify the target exists and is active before creating a drop snapshot.
- **Suggested fix**: Add an existence/active check at the start of each drop method; return error or no-op for missing/already-dropped targets.

### R4-CX-021 — P2: `create_view` doesn't validate schema_id is active
- **File(s)**:
  - `src/metadata_writer_sqlite.rs:1603-1625`
  - `src/metadata_writer_postgres.rs:1425-1445`
  - `src/metadata_writer_mysql.rs:1547-1568`
- **Description**: `create_view` inserts the view row before validating that `schema_id` references an active schema. The schema name lookup (for `changes_made`) defaults to empty string on failure. This can create orphaned views in dropped schemas with malformed change tracking.
- **Assessment**: Valid but low practical risk — callers typically resolve schema_id from active schemas. Downgraded from codex's P1 to P2.
- **Suggested fix**: Validate `schema_id` is active before inserting the view row.

### R4-CX-022 — P1: `rename_table`/`rename_view` allow duplicate active names
- **File(s)**:
  - `src/metadata_writer_sqlite.rs:2103-2170,1701-1770`
  - `src/metadata_writer_postgres.rs:1898-1965,1523-1592`
  - `src/metadata_writer_mysql.rs:2027-2094,1646-1715`
- **Description**: `rename_table` and `rename_view` do not check for an existing active object with the same `(schema_id, new_name)`. The old row is ended and a new row inserted, potentially creating duplicate active names. This causes ambiguous lookups in `find_table_id` and name resolution.
- **Assessment**: Valid. This is a constraint violation that should be caught before the rename.
- **Suggested fix**: Check for name collision before rename; return error if an active object already exists with the target name.

### R4-CX-023 — P2: Checked write transactions have TOCTOU in PG/MySQL
- **File(s)**:
  - `src/metadata_writer_postgres.rs:1259-1329,1332-1375`
  - `src/metadata_writer_mysql.rs:1379-1449,1452-1495`
- **Description**: `begin_checked_write_transaction`, `drop_table_checked`, and `drop_schema_checked` use `SELECT COUNT(*)` for conflict detection without `FOR UPDATE` or serializable isolation. Concurrent transactions can slip between check and write.
- **Assessment**: Valid in principle, but the practical severity depends on concurrency patterns. SQLite is inherently serialized. PG/MySQL could hit this under concurrent writes. Downgraded from codex's P1 to P2.
- **Suggested fix**: Use `SELECT ... FOR UPDATE` on the relevant rows during conflict checks, or run at serializable isolation level.

---

## Round 3: Read Path and Providers

Source files: `src/metadata_provider_duckdb.rs`, `src/metadata_provider_sqlite.rs`, `src/metadata_provider_postgres.rs`, `src/metadata_provider_mysql.rs`, `src/table.rs`, `src/delete_filter.rs`

### R4-CX-040 — P1: `list_all_columns` violates snapshot isolation
- **File(s)**:
  - `src/metadata_provider_sqlite.rs:393-401`
  - `src/metadata_provider_postgres.rs:423-431`
  - `src/metadata_provider_mysql.rs:392-400`
  - `src/metadata_provider_duckdb.rs:372`
- **Description**: The `list_all_columns` query (used by information_schema) filters columns with `c.end_snapshot IS NULL` instead of proper snapshot-window predicates (`begin_snapshot <= snapshot_id AND (end_snapshot > snapshot_id OR end_snapshot IS NULL)`). This returns current columns rather than point-in-time columns, violating snapshot isolation for historical reads.
- **Assessment**: Valid. The column filter should match the pattern used for schemas and tables elsewhere.
- **Suggested fix**: Replace `c.end_snapshot IS NULL` with snapshot-aware predicates matching the table/schema filters.

### R4-CX-041 — P1: LIMIT pushed into Parquet scan before DeleteFilterExec
- **File(s)**: `src/table.rs:830,1435`
- **Description**: The `limit` parameter from `scan()` is passed into `FileScanConfigBuilder::with_limit()` for files that have delete files. The Parquet reader stops after `limit` rows, but `DeleteFilterExec` then filters some of those rows, yielding fewer rows than the requested limit.
- **Assessment**: Valid. This is a correctness bug for `SELECT ... LIMIT N` on tables with delete files — queries can return fewer rows than expected.
- **Suggested fix**: Do not push `limit` into Parquet scans for files that have delete files (i.e., only push limit for the `files_without_deletes` path).

### R4-CX-042 — P2: Provider errors silently swallowed during table initialization
- **File(s)**: `src/table.rs:220,235`
- **Description**: Partition value and inlined data loading use `unwrap_or_default()`, silently converting metadata query failures into empty results. This can mask real errors and cause silent data loss (missing inlined rows) or incorrect partition pruning.
- **Assessment**: Valid. At minimum, failures should be logged as warnings. Ideally they should propagate as errors.
- **Suggested fix**: Replace `unwrap_or_default()` with `?` propagation or at least `unwrap_or_else(|e| { log::warn!(...); default })`.

### R4-CX-043 — P3: DuckDB inlined-data read fails if table doesn't exist
- **File(s)**: `src/metadata_provider_duckdb.rs:100,591,620`
- **Description**: DuckDB provider's inlined-data and row-count methods don't check whether the inlined table exists before querying/PRAGMA. If the table is missing, a DuckDB error propagates. Other backends gracefully return empty/0.
- **Assessment**: Valid but edge case — only occurs with stale metadata pointing to dropped inlined tables. Downgraded from codex's P2 to P3.
- **Suggested fix**: Check table existence before querying; return empty/0 on missing table.

### R4-CX-044 — P3: Negative `null_count` wraps to huge usize in statistics
- **File(s)**: `src/table.rs:1298,1341`
- **Description**: `null_count` is accumulated as `i64` and cast to `usize`. Corrupt metadata with negative values wraps to a huge positive number, polluting planner statistics.
- **Assessment**: Valid but requires corrupt metadata. Downgraded from codex's P2 to P3.
- **Suggested fix**: Clamp negative values to 0 or validate non-negativity.

---

## Round 4: Type System and Supporting Code

Source files: `src/types.rs`, `src/path_resolver.rs`, `src/encryption.rs`, `src/table_changes.rs`, `src/table_deletions.rs`, `src/virtual_column_exec.rs`

### R4-CX-060 — P2: `parse_decimal()` accepts malformed input (missing closing paren)
- **File(s)**: `src/types.rs:264-267`
- **Description**: Inputs like `decimal(10` or `numeric(12,2` (missing closing parenthesis) are treated as valid and silently fall back to `Decimal128(18,0)` instead of returning an error. This can change column schema silently.
- **Assessment**: Valid. Downgraded from codex's P1 to P2 — requires malformed catalog data.
- **Suggested fix**: Require closing parenthesis in the parsing logic; return error for malformed decimal type strings.

### R4-CX-061 — P1: `arrow_to_ducklake_type()` struct field names not escaped
- **File(s)**: `src/types.rs:185-188`
- **Description**: Struct field names are emitted as unquoted `"{name} {type}"`. Field names containing spaces, commas, colons, or quotes produce invalid/unparseable DuckLake type strings, breaking Arrow-to-DuckLake-to-Arrow roundtrips.
- **Assessment**: Valid. Struct types with special characters in field names will produce corrupt type strings.
- **Suggested fix**: Quote or escape field names containing special characters in the struct type string serialization.

### R4-CX-062 — P2: Projection index bounds not checked in table_changes/table_deletions
- **File(s)**: `src/table_changes.rs:417-421`, `src/table_deletions.rs:140-143`
- **Description**: Projection indices index into `self.output_schema.field(idx)` without bounds checking. An invalid projection index causes a panic instead of a DataFusionError.
- **Assessment**: Valid. Should use `get()` or bounds-check before indexing.
- **Suggested fix**: Add bounds checks; return `DataFusionError::Internal` for out-of-range indices.

### R4-CX-063 — P3: `DeletedRowsExec::with_new_children()` silently ignores extra children
- **File(s)**: `src/table_deletions.rs:490-523`
- **Description**: `with_new_children()` only reads the expected children by index; extra children are silently dropped. This could hide optimizer/planner bugs that produce unexpected child counts.
- **Assessment**: Valid but very low risk — standard DataFusion behavior for execution plans.
- **Suggested fix**: Add assertion on expected child count in `with_new_children()`.

### R4-CX-064 — P2: Unchecked i64 addition overflow in virtual_column_exec
- **File(s)**: `src/virtual_column_exec.rs:230-232,260-261`
- **Description**: `rowid` and `row_offset` arithmetic use unchecked `i64` addition. Extremely large offsets can overflow (panic in debug, wrap in release).
- **Assessment**: Valid but extreme edge case — requires tables with >9.2 quintillion cumulative rows. Kept at P2 for consistency with the R3 approach to numeric safety.
- **Suggested fix**: Use `checked_add()` with error propagation.

---

## Round 5: Test Infrastructure

Source files: `tests/common/test_utils.rs`, `tests/sqllogictest_runner.rs`, `tests/hybrid_asyncdb.rs`

### R4-CX-080 — P2: Transaction-path row decoding swallows errors as "NULL"
- **File(s)**: `tests/hybrid_asyncdb.rs:420`
- **Description**: Transaction-path row decoding coerces `Err(_)` from `row.get(...)` to `"NULL"`. Real adapter/type-conversion defects are masked as passing result comparisons, creating false positives in BEGIN...SELECT test blocks.
- **Assessment**: Valid. Downgraded from codex's P1 to P2 — this is test infrastructure, not production code.
- **Suggested fix**: Log or fail on `row.get()` errors instead of silently returning "NULL".

### R4-CX-081 — P2: `normalize_value` collapses distinct numeric values
- **File(s)**: `tests/common/test_utils.rs:279-280`
- **Description**: `normalize_value` parses any numeric-looking string as `f64` and rounds to 6 decimal places. Large integers/decimals or high-precision floats can be collapsed into the same normalized string, letting mismatches pass assertions.
- **Assessment**: Valid. Downgraded from codex's P1 to P2 — affects test accuracy, not production code.
- **Suggested fix**: Only apply f64 normalization to known float columns, or use higher precision for large numbers.

### R4-CX-082 — P2: SLT tests can pass with zero executed statements
- **File(s)**: `tests/sqllogictest_runner.rs:84,112,771`
- **Description**: Preprocessing can emit `halt` for several directives, and `run_hybrid_test` doesn't assert that any statements were executed. A generated test can report success while running zero checks.
- **Assessment**: Valid. Zero-check tests provide false confidence.
- **Suggested fix**: Assert minimum statement count at the end of `run_hybrid_test`.

### R4-CX-083 — P3: Weak `contains()` assertions in `test_table_rewrite`
- **File(s)**: `tests/hybrid_asyncdb.rs:756,764`
- **Description**: Uses `contains("ducklake.main.test")` instead of exact expected SQL. Overly permissive matching can pass even with malformed rewrite output.
- **Assessment**: Valid but low impact — a cosmetic test weakness.
- **Suggested fix**: Use exact string matching or a more specific pattern.

---

## Priority Summary

| Priority | Count | Key Themes |
|----------|-------|------------|
| P0 | 3 | Inline data loss on flush failure, write/read path mismatch, non-atomic partitioned commit |
| P1 | 8 | NULL filter semantics, NOT NULL constraint gaps, snapshot isolation in list_all_columns, LIMIT+delete interaction, rename collision, drop validation, struct field escaping |
| P2 | 9 | TOCTOU in checked transactions, error swallowing, malformed decimal parsing, projection bounds, overflow, test infrastructure false positives |
| P3 | 4 | DuckDB inlined table existence, negative null_count, extra children ignored, weak test assertion |
| **Total** | **24** | |

## Cross-Cutting Observations

### 1. Write Path / Metadata Path Divergence (R4-CX-002)
The most critical new finding. The table writer uploads files to a `t{table_id}/` directory but the metadata stores the table path as `table_name/`. Every file written by `write_parquet_with_setup` is unreachable by readers.

### 2. Inline Data Clearing Before Commit (R4-CX-001)
A data-loss window exists in the flush path: inlined rows are deleted from the catalog before the replacement Parquet file is committed. Any failure between clear and commit loses data permanently.

### 3. LIMIT Correctness With Deletes (R4-CX-041)
Pushing `limit` into Parquet scans for files with delete files yields incorrect results. This affects any `SELECT ... LIMIT N` on tables that have had rows deleted.

### 4. Constraint Validation Gap (R4-CX-007)
INSERT validates NOT NULL constraints but UPDATE/MERGE do not, creating an inconsistent constraint enforcement surface.
