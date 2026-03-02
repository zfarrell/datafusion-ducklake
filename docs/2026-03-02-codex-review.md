# Codex General Review — 2026-03-02

## Summary

7 batches reviewed covering all key source files across the write path, metadata providers,
DataFusion integration layer, data operations, supporting modules, backend-specific writers,
and test files. **36 total findings** identified, ranging from potential data-loss bugs (P0)
to minor test coverage gaps (P3).

---

## Batch 1: Core Write Path

**Files**: `src/metadata_writer.rs`, `src/metadata_writer_sqlite.rs`, `src/insert_exec.rs`, `src/delete_exec.rs`, `src/update_exec.rs`, `src/table_writer.rs`

1. **Replace writes can permanently drop all active files if new-file registration fails (data loss).**
   In replace mode, old files are ended before new file metadata is guaranteed to be committed. If `register_data_file` fails after `end_table_files`, the table can be left empty.
   - `src/table_writer.rs:831`, `src/table_writer.rs:843`, `src/table_writer.rs:554`, `src/table_writer.rs:572`

2. **DELETE/UPDATE are not atomic across files; partial metadata commits can occur (corruption risk).**
   Both executors register files incrementally in a loop. On mid-loop failure they clean up uploaded objects, but already-committed metadata for earlier files is not rolled back, leaving partial DML effects in one snapshot.
   - `src/delete_exec.rs:367`, `src/update_exec.rs:435`, `src/update_exec.rs:499`

3. **`write_parquet_with_setup` writes to `.../t{table_id}/...` but registers only filename relative to table path (file lookup mismatch).**
   Object is uploaded under a table-id path, but catalog path is stored as just `file_name` relative to the table's metadata path (usually name-based). This can make files unreadable/orphaned.
   - `src/table_writer.rs:440`, `src/table_writer.rs:447`, `src/table_writer.rs:473`

4. **Write paths are derived from `schema_name/table_name` instead of catalog table path; breaks renamed/custom-path tables.**
   After rename (path intentionally unchanged) or custom path usage, new writes can go to a different physical location than metadata resolution expects.
   - `src/table_writer.rs:83`, `src/table_writer.rs:109`, `src/update_exec.rs:455`, `src/update_exec.rs:496`

5. **Inline-data flush path can silently discard existing inlined rows on conversion/read failure.**
   If `get_inlined_data_as_batch` fails, code ignores the error and still clears inline rows, then writes only new batches.
   - `src/table_writer.rs:307`, `src/table_writer.rs:316`

6. **Column IDs are regenerated on every write transaction, not preserved (field-id instability).**
   `begin_write_transaction` ends all active columns and inserts new rows with fresh IDs even for append-compatible schema, which can destabilize Parquet field-id mapping across snapshots/files.
   - `src/metadata_writer_sqlite.rs:440`, `src/metadata_writer_sqlite.rs:449`, `src/metadata_writer_sqlite.rs:459`

7. **`drop_schema` ends only schema row, not contained tables/files (orphaned metadata/data risk).**
   Dropping a schema does not end child table metadata in this implementation, creating inconsistent catalog state.
   - `src/metadata_writer_sqlite.rs:585`, `src/metadata_writer_sqlite.rs:593`

8. **High memory/perf risk in INSERT path: all partitions are fully materialized before writing.**
   Large inserts can OOM because all input batches from all partitions are collected in memory first.
   - `src/insert_exec.rs:211`, `src/insert_exec.rs:217`

9. **Potential panic from unchecked partition column index in partition routing.**
   `batch.column(pc.column_index)` is used without bounds validation; bad planner/config input can panic the executor instead of returning an error.
   - `src/insert_exec.rs:252`

10. **Schema truncation risk: `build_schema_with_field_ids` silently zips to shorter side.**
    If `column_ids.len() != schema.fields().len()`, fields are silently dropped/ignored instead of failing fast, which can cause incorrect Parquet schemas.
    - `src/table_writer.rs:1122`, `src/table_writer.rs:1126`

---

## Batch 2: Metadata Providers

**Files**: `src/metadata_provider.rs`, `src/metadata_provider_sqlite.rs`, `src/metadata_provider_postgres.rs`, `src/metadata_provider_mysql.rs`, `src/metadata_provider_duckdb.rs`

1. **Dynamic SQL identifier injection in inlined-data queries (all providers).**
   Unescaped table/column names are interpolated into SQL with `format!`, so a crafted catalog entry (or corrupted metadata DB) can break query structure or inject SQL.
   - SQLite: `src/metadata_provider_sqlite.rs:870`, `:893`, `:895`, `:993`
   - PostgreSQL: `src/metadata_provider_postgres.rs:964`, `:966`, `:1065`
   - MySQL: `src/metadata_provider_mysql.rs:943`, `:945`, `:1004`
   - DuckDB: `src/metadata_provider_duckdb.rs:542`, `:557`, `:559`

2. **DuckDB row count omits inlined rows (behavioral inconsistency).**
   `get_table_row_count` in DuckDB only aggregates `ducklake_data_file` and does not add `ducklake_inlined_data` rows, unlike SQLite/Postgres/MySQL providers.
   - DuckDB: `src/metadata_provider_duckdb.rs:442`
   - Others include inlined rows: `src/metadata_provider_sqlite.rs:956`, `src/metadata_provider_postgres.rs:1026`, `src/metadata_provider_mysql.rs:762`

3. **`DeleteFileChange` assumes non-null `data.footer_size`; decode can fail if nullable.**
   `DeleteFileChange.data_file_footer_size` is `i64`, but SQL selects `data.footer_size`, which is modeled as optional elsewhere (`DuckLakeFileData.footer_size: Option<i64>`). If nulls exist, `row.get/try_get` will error.
   - Struct: `src/metadata_provider.rs:581`
   - Parsing: `src/metadata_provider_sqlite.rs:685`, `src/metadata_provider_postgres.rs:704`, `src/metadata_provider_mysql.rs:680`, `src/metadata_provider_duckdb.rs:601`

4. **Inconsistent missing-`data_path` error handling.**
   SQLite/Postgres/MySQL map missing metadata to `InvalidConfig`, but DuckDB uses `query_row` directly and returns a backend error for missing rows.
   - DuckDB: `src/metadata_provider_duckdb.rs:82`
   - Typed handling: `src/metadata_provider_sqlite.rs:61`, `src/metadata_provider_postgres.rs:93`, `src/metadata_provider_mysql.rs:60`

---

## Batch 3: Table/Catalog Layer

**Files**: `src/table.rs`, `src/catalog.rs`, `src/schema.rs`, `src/query_planner.rs`, `src/table_functions.rs`, `src/information_schema.rs`

1. **CTAS writes are hard-wired to local filesystem object store, breaking non-local backends.**
   `register_table` constructs `DuckLakeTableWriter` with `LocalFileSystem` unconditionally (`Arc::new(object_store::local::LocalFileSystem::new())`).
   In S3/GCS/Azure setups, `CREATE TABLE AS SELECT` can write to local disk instead of the configured object store.
   - `src/schema.rs:395`

2. **`register_table` does synchronous `block_on` bridging inside provider trait path.**
   It blocks to run async scan/write work from a sync trait method. This is fragile in async runtimes (potential blocking contention / runtime-context sensitivity).
   - `src/schema.rs:346`, `src/schema.rs:362`, `src/schema.rs:403`

3. **UDTFs resolve metadata at "latest snapshot" instead of session/catalog snapshot.**
   Multiple functions call `get_current_snapshot()` at invocation time. Results from `ducklake_*` table functions can become inconsistent with table scans planned against a pinned snapshot.
   - `src/table_functions.rs:288`, `src/table_functions.rs:63`, `src/table_functions.rs:508`

4. **DELETE/UPDATE planner is tightly coupled to `DefaultTableSource` concrete type.**
   `downcast_ducklake_table` rejects any `TableSource` wrapper that is not exactly `DefaultTableSource`. Valid DuckLake targets can fail planning if DataFusion introduces alternative wrappers.
   - `src/query_planner.rs:92`

---

## Batch 4: Data Operations

**Files**: `src/table_insertions.rs`, `src/table_deletions.rs`, `src/table_changes.rs`, `src/virtual_column_exec.rs`, `src/merge_exec.rs`, `src/compaction_functions.rs`

1. **`ducklake_table_changes()` can return columns in the wrong order (and wrong multiplicity) for projected CDC queries.**
   In projection analysis, requested order is captured, but execution schema/output are rebuilt in fixed `table_cols + snapshot_id + change_type` order.
   - `src/table_changes.rs:361`, `:385`, `:488`, `:506`, `:243`

2. **`ducklake_table_deletions()` ignores projection for non-empty results.**
   `scan()` accepts `projection`, but only applies it in the empty fast path. For actual data, it always returns full `output_schema` via `DeletedRowsExec`.
   - `src/table_deletions.rs:221`, `:236`, `:254`, `:435`

3. **MERGE join key comparison silently mis-handles unsupported Arrow types and can panic on type mismatch.**
   `values_equal` only supports a subset of datatypes and returns `false` for others, causing valid matches to be dropped. It also `unwrap()`s downcasts based on target type, so mismatched key types can panic.
   - `src/merge_exec.rs:239`, `src/merge_exec.rs:195`

4. **Virtual column null semantics are incorrect when metadata is absent.**
   Missing `row_id_start`/`snapshot_id` are coerced to `0`. That turns "unknown/missing metadata" into a real value and can corrupt downstream logic expecting nulls.
   - `src/virtual_column_exec.rs:207`, `src/virtual_column_exec.rs:215`

5. **Virtual row-number/rowid generation is partition-local, not file-global.**
   `row_offset` starts at `0` for each `execute(partition, ...)` stream. If an input file is emitted across multiple partitions, `file_row_number`/`rowid` sequences restart per partition, producing duplicates.
   - `src/virtual_column_exec.rs:154`, `:165`, `:200`, `:208`

---

## Batch 5: Supporting Modules

**Files**: `src/types.rs`, `src/path_resolver.rs`, `src/encryption.rs`, `src/error.rs`, `src/delete_filter.rs`, `src/lib.rs`

1. **Ambiguous key decoding can silently use the wrong bytes for valid hex keys.**
   In `decode_key`, base64 decoding is attempted before hex decoding. A 32-char hex key (common AES-128 representation) is also valid base64 text and decodes to 24 bytes, which passes AES length checks, so it is accepted as the wrong key material.
   - `src/encryption.rs:133`, `src/encryption.rs:143`

2. **Temporal type mapping is lossy (timezone and time-unit semantics can change).**
   `arrow_to_ducklake_type` maps any `Timestamp(_, Some(_))` to `timestamptz`, and `ducklake_to_arrow_type("timestamptz")` always maps back to UTC, losing original timezone. Also `Time32(_) | Time64(_)` both map to `"time"`, which maps back to `Time64(Microsecond)`, changing unit/type on roundtrip.
   - `src/types.rs:121`, `src/types.rs:57`, `src/types.rs:116`, `src/types.rs:54`

3. **Decimal parser matches non-decimal type prefixes.**
   `parse_decimal` uses `starts_with("decimal") || starts_with("numeric")`, so strings like `decimalx(10,2)` are treated as decimal instead of unsupported type.
   - `src/types.rs:217`

4. **`join_paths` can emit doubled separators when relative path starts with slash and base has no trailing slash.**
   When `base_path` does not end with `/` or `\`, leading separators in `relative_path` are not trimmed, producing outputs like `"/base//child"`.
   - `src/path_resolver.rs:263`, `src/path_resolver.rs:272`

5. **Base path is not validated in relative resolution path.**
   `join_paths` validates only `relative_path`; `resolve_path(..., is_relative=true)` does not validate `base_path`. If `PathResolver::new` is called with untrusted base path, traversal/null checks are bypassed for that side.
   - `src/path_resolver.rs:233`, `src/path_resolver.rs:264`

---

## Batch 6: Write Path — Other Backends

**Files**: `src/metadata_writer_postgres.rs`, `src/metadata_writer_mysql.rs`, `src/metadata_writer_validation.rs`

1. **MySQL ID allocation is race-prone under concurrent writers (`MAX(...) + 1`).**
   `FOR UPDATE` on aggregate `MAX()` does not guarantee safe monotonic allocation across all concurrency cases (especially with no uniqueness constraint on these logical IDs), so duplicate IDs are possible.
   - `src/metadata_writer_mysql.rs:429`, `:485`, `:738`, `:780`, `:1222`, `:1404`

2. **`partition_id` generation is `MAX(...) + 1` without sequence/locking in both Postgres and MySQL.**
   Concurrent `ALTER TABLE ... SET PARTITIONED BY` operations can allocate the same `partition_id`.
   - `src/metadata_writer_postgres.rs:1413`, `src/metadata_writer_mysql.rs:1530`

3. **MySQL `initialize_schema` can leak modified session `sql_mode` back to pool if an error occurs before restore.**
   If `INSERT IGNORE ... snapshot_id=0` fails, restore step is skipped and the pooled connection may keep `NO_AUTO_VALUE_ON_ZERO`, affecting later operations.
   - `src/metadata_writer_mysql.rs:984`, `:988`, `:993`, `:998`

No SQL-injection issues found; runtime values are consistently passed via `.bind(...)`.

---

## Batch 7: Key Test Files

**Files**: `tests/roundtrip_interop_tests.rs`, `tests/sqllogictest_runner.rs`, `tests/sql_dml_tests.rs`, `tests/write_partition_tests.rs`

1. **sqllogictest runner never fails the test when cases fail — suite can report failures but still pass CI.**
   Only prints failures; there is no `assert_eq!(failed, 0)` / panic.
   - `tests/sqllogictest_runner.rs:870-880`

2. **Schema-evolution interop test is explicitly non-failing on critical failure paths, creating false positives.**
   Returns early when DuckDB cannot read the evolved catalog; logs missing legacy rows instead of asserting.
   - `tests/roundtrip_interop_tests.rs:389-403`, `tests/roundtrip_interop_tests.rs:415-423`

3. **Weak assertion for count check can pass on unrelated output.**
   Uses `stdout.contains('3')` instead of parsing a scalar result.
   - `tests/roundtrip_interop_tests.rs:217-222`

4. **One test may panic non-deterministically due to batch assumptions.**
   Assumes all 3 rows are in `batches[0]`; DataFusion can split rows across batches.
   - `tests/roundtrip_interop_tests.rs:277-284`

5. **"partition pruning" test validates result correctness but not pruning behavior.**
   Checks returned rows only; no plan/file-scan assertions.
   - `tests/write_partition_tests.rs:191-223`

6. **Hive directory test only verifies parquet-file existence for one partition.**
   Asserts files for `region=US` but not for `region=EU`, allowing partial write bugs to pass.
   - `tests/write_partition_tests.rs:324-342`

7. **Sorting helper masks ordering regressions in tests that already use `ORDER BY`.**
   Sorts all rows before assertions, so broken ordering could still pass.
   - `tests/write_partition_tests.rs:93-110`

8. **Coverage gap: test file claims metadata registration validation but tests do not assert metadata tables/partition values.**
   - `tests/write_partition_tests.rs:3-4`

---

## Consolidated Findings

### CX-01: Replace Mode Data Loss on Registration Failure (Severity: P0)
- **Source**: Batch 1
- **File(s)**: `src/table_writer.rs:831`, `:843`, `:554`, `:572`
- **Description**: In replace mode, old files are ended before new file metadata is committed. If `register_data_file` fails after `end_table_files`, the table is left empty with no recovery path.
- **Suggestion**: Register new files first, then end old files, or wrap both operations in a metadata transaction.
- **Effort**: M

### CX-02: Non-Atomic DELETE/UPDATE Across Files (Severity: P0)
- **Source**: Batch 1
- **File(s)**: `src/delete_exec.rs:367`, `src/update_exec.rs:435`, `:499`
- **Description**: Multi-file DML registers files incrementally. Mid-loop failure leaves partial metadata committed in the snapshot with no rollback.
- **Suggestion**: Batch all file registrations and commit atomically, or implement metadata transaction rollback.
- **Effort**: L

### CX-03: SQL Identifier Injection in Inlined-Data Queries (Severity: P0)
- **Source**: Batch 2
- **File(s)**: All 4 metadata providers (SQLite `:870`, Postgres `:964`, MySQL `:943`, DuckDB `:542`)
- **Description**: Unescaped table/column names are interpolated into SQL with `format!`. Crafted catalog entries can inject SQL.
- **Suggestion**: Quote identifiers using dialect-appropriate escaping (double-quote for Postgres/DuckDB/SQLite, backtick for MySQL).
- **Effort**: M

### CX-04: CTAS Hard-Wired to LocalFileSystem (Severity: P0)
- **Source**: Batch 3
- **File(s)**: `src/schema.rs:395`
- **Description**: `register_table` constructs `DuckLakeTableWriter` with `LocalFileSystem` unconditionally. S3/GCS/Azure CTAS writes go to local disk.
- **Suggestion**: Pass the session's registered object store through to the table writer.
- **Effort**: M

### CX-05: File Path Mismatch Between Write Location and Catalog Registration (Severity: P1)
- **Source**: Batch 1
- **File(s)**: `src/table_writer.rs:440`, `:447`, `:473`
- **Description**: Parquet files are written to `.../t{table_id}/...` but registered with just `file_name` relative to table's metadata path (name-based). Files become unreadable/orphaned.
- **Suggestion**: Ensure registration path matches actual write path, or resolve both through the same path hierarchy.
- **Effort**: M

### CX-06: Write Paths Derived from Names Instead of Catalog Paths (Severity: P1)
- **Source**: Batch 1
- **File(s)**: `src/table_writer.rs:83`, `:109`, `src/update_exec.rs:455`, `:496`
- **Description**: Write paths use `schema_name/table_name` instead of catalog table path. Breaks after renames or custom path usage.
- **Suggestion**: Read and use the table's stored path from catalog metadata.
- **Effort**: M

### CX-07: Encryption Key Decoding Ambiguity (Severity: P1)
- **Source**: Batch 5
- **File(s)**: `src/encryption.rs:133`, `:143`
- **Description**: Base64 is tried before hex. A 32-char hex key is valid base64, decodes to 24 bytes (valid AES-192), and is silently accepted as wrong key material.
- **Suggestion**: Require explicit key encoding prefix (e.g., `hex:` / `base64:`) or try hex first for common key lengths.
- **Effort**: S

### CX-08: Column IDs Regenerated Every Write Transaction (Severity: P1)
- **Source**: Batch 1
- **File(s)**: `src/metadata_writer_sqlite.rs:440`, `:449`, `:459`
- **Description**: `begin_write_transaction` ends all active columns and inserts new rows with fresh IDs even for schema-compatible appends. Destabilizes Parquet field-id mapping.
- **Suggestion**: Preserve existing column IDs when schema hasn't changed; only allocate new IDs for added columns.
- **Effort**: M

### CX-09: MERGE Panics on Unsupported/Mismatched Key Types (Severity: P1)
- **Source**: Batch 4
- **File(s)**: `src/merge_exec.rs:195`, `:239`
- **Description**: `values_equal` returns `false` for unsupported types (timestamps, decimals, binary) and `unwrap()`s downcasts, causing silent data loss or panics.
- **Suggestion**: Return `DataFusionError` for unsupported types; add safe downcast with error propagation.
- **Effort**: M

### CX-10: MySQL ID Allocation Race Condition (Severity: P1)
- **Source**: Batch 6
- **File(s)**: `src/metadata_writer_mysql.rs:429`, `:485`, `:738`, `:780`, `:1222`, `:1404`
- **Description**: `MAX(...) + 1` ID allocation with `FOR UPDATE` is not safe for concurrent MySQL writers. Duplicate IDs possible.
- **Suggestion**: Use MySQL `AUTO_INCREMENT` columns or application-level locking.
- **Effort**: M

### CX-11: DuckDB Row Count Omits Inlined Rows (Severity: P1)
- **Source**: Batch 2
- **File(s)**: `src/metadata_provider_duckdb.rs:442`
- **Description**: `get_table_row_count` in DuckDB doesn't add inlined data rows, unlike all other providers.
- **Suggestion**: Add inlined row count query matching other providers.
- **Effort**: S

### CX-12: Table Changes Returns Wrong Column Order for Projections (Severity: P1)
- **Source**: Batch 4
- **File(s)**: `src/table_changes.rs:361`, `:385`, `:488`, `:506`
- **Description**: Projection is captured but execution schema is rebuilt in fixed order (`table_cols + snapshot_id + change_type`). Breaks projected CDC queries.
- **Suggestion**: Apply projection indices to output schema and batch construction.
- **Effort**: M

### CX-13: Table Deletions Ignores Projection for Non-Empty Results (Severity: P1)
- **Source**: Batch 4
- **File(s)**: `src/table_deletions.rs:221`, `:236`, `:254`, `:435`
- **Description**: Projection only applied in empty fast path; actual data returns full schema.
- **Suggestion**: Apply projection in `DeletedRowsExec` output.
- **Effort**: M

### CX-14: UDTF Snapshot Inconsistency (Severity: P1)
- **Source**: Batch 3
- **File(s)**: `src/table_functions.rs:288`, `:63`, `:508`
- **Description**: Table functions resolve metadata at latest snapshot instead of session/catalog pinned snapshot, causing inconsistent views.
- **Suggestion**: Pass pinned snapshot ID through to UDTF metadata queries.
- **Effort**: M

### CX-15: Inline Data Flush Silently Discards Rows on Read Failure (Severity: P1)
- **Source**: Batch 1
- **File(s)**: `src/table_writer.rs:307`, `:316`
- **Description**: If `get_inlined_data_as_batch` fails, the error is ignored, inline rows are cleared, and only new batches are written.
- **Suggestion**: Propagate the error and abort the flush operation.
- **Effort**: S

### CX-16: sqllogictest Runner Never Fails CI (Severity: P1)
- **Source**: Batch 7
- **File(s)**: `tests/sqllogictest_runner.rs:870-880`
- **Description**: Runner prints failures but has no assertion to fail the test, so CI always passes.
- **Suggestion**: Add `assert_eq!(failures, 0)` or panic on any failure.
- **Effort**: S

### CX-17: `drop_schema` Orphans Child Tables (Severity: P1)
- **Source**: Batch 1
- **File(s)**: `src/metadata_writer_sqlite.rs:585`, `:593`
- **Description**: Dropping a schema ends only the schema row, not child table/file metadata.
- **Suggestion**: Cascade end operations to child tables and their files.
- **Effort**: M

### CX-18: Virtual Column Row IDs Are Partition-Local (Severity: P2)
- **Source**: Batch 4
- **File(s)**: `src/virtual_column_exec.rs:154`, `:165`, `:200`, `:208`
- **Description**: `row_offset` restarts at 0 per partition. Multi-partition files get duplicate rowids.
- **Suggestion**: Use file-level row offset from metadata or coordinate across partitions.
- **Effort**: M

### CX-19: Virtual Column Null Coercion to Zero (Severity: P2)
- **Source**: Batch 4
- **File(s)**: `src/virtual_column_exec.rs:207`, `:215`
- **Description**: Missing `row_id_start`/`snapshot_id` are coerced to `0` instead of null, corrupting downstream logic.
- **Suggestion**: Use nullable columns and emit null when metadata is absent.
- **Effort**: S

### CX-20: Schema Truncation on Field ID Mismatch (Severity: P2)
- **Source**: Batch 1
- **File(s)**: `src/table_writer.rs:1122`, `:1126`
- **Description**: `build_schema_with_field_ids` silently zips to shorter side when column ID count doesn't match field count.
- **Suggestion**: Return an error if lengths differ.
- **Effort**: S

### CX-21: INSERT OOM Risk — Full Partition Materialization (Severity: P2)
- **Source**: Batch 1
- **File(s)**: `src/insert_exec.rs:211`, `:217`
- **Description**: All partitions are fully materialized in memory before writing. Large inserts can OOM.
- **Suggestion**: Stream partitions to disk incrementally.
- **Effort**: L

### CX-22: `block_on` in Sync Trait Method (Severity: P2)
- **Source**: Batch 3
- **File(s)**: `src/schema.rs:346`, `:362`, `:403`
- **Description**: Synchronous `block_on` from sync trait method is fragile with async runtimes.
- **Suggestion**: Use `spawn_blocking` or restructure to use async trait methods.
- **Effort**: M

### CX-23: Temporal Type Mapping Is Lossy (Severity: P2)
- **Source**: Batch 5
- **File(s)**: `src/types.rs:121`, `:57`, `:116`, `:54`
- **Description**: Timezone is lost on roundtrip (all `timestamptz` maps back to UTC). `Time32`/`Time64` both map to `"time"` which maps back to `Time64(Microsecond)`.
- **Suggestion**: Preserve timezone in type string; distinguish time unit variants.
- **Effort**: M

### CX-24: `DeleteFileChange` Assumes Non-Null Footer Size (Severity: P2)
- **Source**: Batch 2
- **File(s)**: `src/metadata_provider.rs:581`, parsing in all 4 providers
- **Description**: `data_file_footer_size: i64` but source column is nullable. Will error if null.
- **Suggestion**: Make field `Option<i64>` to match source nullability.
- **Effort**: S

### CX-25: Partition ID Race in Postgres and MySQL (Severity: P2)
- **Source**: Batch 6
- **File(s)**: `src/metadata_writer_postgres.rs:1413`, `src/metadata_writer_mysql.rs:1530`
- **Description**: `MAX(...) + 1` partition ID allocation without proper locking. Concurrent schema alterations can duplicate IDs.
- **Suggestion**: Use database sequences (Postgres) or `AUTO_INCREMENT` (MySQL).
- **Effort**: M

### CX-26: MySQL sql_mode Leak on Error (Severity: P2)
- **Source**: Batch 6
- **File(s)**: `src/metadata_writer_mysql.rs:984-998`
- **Description**: `initialize_schema` modifies `sql_mode` to `NO_AUTO_VALUE_ON_ZERO`; if subsequent operations fail before restore, pooled connection retains modified mode.
- **Suggestion**: Use `DROP TEMPORARY TABLE` / `RAII` pattern, or always restore in a `finally`-equivalent.
- **Effort**: S

### CX-27: Unchecked Partition Column Index (Severity: P2)
- **Source**: Batch 1
- **File(s)**: `src/insert_exec.rs:252`
- **Description**: `batch.column(pc.column_index)` without bounds check. Bad config panics instead of returning error.
- **Suggestion**: Add bounds check with descriptive error.
- **Effort**: S

### CX-28: Schema Evolution Test Creates False Positives (Severity: P2)
- **Source**: Batch 7
- **File(s)**: `tests/roundtrip_interop_tests.rs:389-403`, `:415-423`
- **Description**: Returns early on DuckDB read failure; logs missing rows instead of asserting. Critical failures pass silently.
- **Suggestion**: Assert on all expected outcomes or mark test as `#[ignore]` with diagnostic label.
- **Effort**: S

### CX-29: Decimal Parser Overly Permissive Prefix Match (Severity: P2)
- **Source**: Batch 5
- **File(s)**: `src/types.rs:217`
- **Description**: `starts_with("decimal")` matches `decimalx(10,2)` etc.
- **Suggestion**: Use exact match or regex with word boundary.
- **Effort**: S

### CX-30: Weak Count Assertion (`stdout.contains('3')`) (Severity: P2)
- **Source**: Batch 7
- **File(s)**: `tests/roundtrip_interop_tests.rs:217-222`
- **Description**: Can pass on any output containing character '3'.
- **Suggestion**: Parse scalar result and compare numerically.
- **Effort**: S

### CX-31: DELETE/UPDATE Planner Coupled to DefaultTableSource (Severity: P2)
- **Source**: Batch 3
- **File(s)**: `src/query_planner.rs:92`
- **Description**: Rejects non-`DefaultTableSource` wrappers, reducing forward compatibility.
- **Suggestion**: Support additional DataFusion table source wrappers or use trait-based detection.
- **Effort**: S

### CX-32: Inconsistent `data_path` Error Handling Across Providers (Severity: P3)
- **Source**: Batch 2
- **File(s)**: `src/metadata_provider_duckdb.rs:82` vs others
- **Description**: DuckDB returns backend error for missing metadata; others return `InvalidConfig`.
- **Suggestion**: Standardize error type across providers.
- **Effort**: S

### CX-33: Double Path Separators in `join_paths` (Severity: P3)
- **Source**: Batch 5
- **File(s)**: `src/path_resolver.rs:263`, `:272`
- **Description**: Can produce `"/base//child"` when relative path starts with slash.
- **Suggestion**: Trim leading separators from relative path before joining.
- **Effort**: S

### CX-34: Test Batch Assumption (Non-Deterministic Panic) (Severity: P3)
- **Source**: Batch 7
- **File(s)**: `tests/roundtrip_interop_tests.rs:277-284`
- **Description**: Assumes all rows are in `batches[0]`; DataFusion can split across batches.
- **Suggestion**: Concatenate all batches before asserting.
- **Effort**: S

### CX-35: Partition Pruning Test Doesn't Assert Pruning (Severity: P3)
- **Source**: Batch 7
- **File(s)**: `tests/write_partition_tests.rs:191-223`
- **Description**: Only checks returned rows, not whether files were actually pruned.
- **Suggestion**: Add plan/metrics assertions to verify file pruning.
- **Effort**: S

### CX-36: Base Path Not Validated in Relative Resolution (Severity: P3)
- **Source**: Batch 5
- **File(s)**: `src/path_resolver.rs:233`, `:264`
- **Description**: Only `relative_path` is validated; untrusted `base_path` bypasses traversal/null checks.
- **Suggestion**: Apply same validation to base path.
- **Effort**: S
