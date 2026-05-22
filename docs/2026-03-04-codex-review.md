# Review Cycle 6: Codex Review
Date: 2026-03-04

## Summary

Codex reviewed 5 file groups totaling ~30 source files across the write path, metadata writers,
core infrastructure, table functions/CDC, and test infrastructure. After validation, this cycle
produced **33 findings** (0 confirmed P0, 10 P1, 13 P2, 10 P3).

**P0 false positive rate this cycle: 100% (3/3)** — all 3 codex P0 claims were downgraded after
source code validation. This continues the pattern observed in R4-R5 (86% cumulative P0 FP rate).

**P1 validation: 10 of 16 confirmed** — 6 P1 claims were downgraded or marked as false positives.

## P0/P1 Validation Results

### CX-W-001: Non-atomic Replace in `TableWriteSession::finish()`
- **Codex Severity**: P0
- **Validated Severity**: P2 (acknowledged risk with documented rationale)
- **Validation Evidence**: Code at `table_writer.rs:899-904` has explicit documentation:
  ```rust
  // R5-S-022: Note — end_table_files and register_data_file use separate
  // transactions. If register_data_file fails after end_table_files commits,
  // old data is gone but the new file isn't registered. The multi-file
  // commit_uploaded_files path uses replace_table_files (single transaction)
  // which is fully atomic. This single-file path accepts the small risk
  // since a register_data_file failure after successful upload+end is unlikely.
  ```
  The risk is acknowledged and accepted. The multi-file path IS atomic via `replace_table_files`.
  A `register_data_file` failure after successful upload is extremely unlikely in practice.
- **Verdict**: DOWNGRADED to P2 — acknowledged design tradeoff, not an oversight

### CX-W-002: MERGE nondeterministic behavior for duplicate source matches
- **Codex Severity**: P1
- **Validated Severity**: FALSE POSITIVE
- **Validation Evidence**: `merge_exec.rs:486-494` already checks this:
  ```rust
  source_match_count[src_global] += 1;
  // R3F-033: Error if source row matches multiple target rows
  if source_match_count[src_global] > 1 {
      return Err(DataFusionError::Execution(
          "MERGE violation: a source row matched more than one target row..."
      ));
  }
  ```
  The `break` at line 502 breaks out of the source candidates loop (first match sufficient per target row), NOT the duplicate check. The check at 489 fires if the same source row matches a second target row.
- **Verdict**: FALSE POSITIVE — already implemented (R3F-033)

### CX-W-003: join_key_pairs bounds check missing in MERGE
- **Codex Severity**: P1
- **Validated Severity**: P3 (defensive hardening)
- **Validation Evidence**: `join_key_pairs` is constructed by `DuckLakeQueryPlanner` from schema metadata — indices are always valid because they come from the planner's column resolution. A bounds panic would only occur from a planner bug, not user input.
- **Verdict**: DOWNGRADED to P3 — internal code, planner guarantees valid indices

### CX-W-004: partition_column indices bounds check missing in INSERT
- **Codex Severity**: P1
- **Validated Severity**: P3 (defensive hardening)
- **Validation Evidence**: `partition_columns` are populated from catalog metadata with validated column indices. Same reasoning as CX-W-003 — internal planner guarantees.
- **Verdict**: DOWNGRADED to P3 — internal code, catalog metadata guarantees valid indices

### CX-M-001: Non-atomic default `replace_table_files`
- **Codex Severity**: P0
- **Validated Severity**: P2 (mitigated by backend overrides)
- **Validation Evidence**: `metadata_writer.rs:491-492` documents this:
  ```rust
  /// Default implementation calls individual methods (non-atomic, for backward
  /// compatibility). Backends should override for true atomicity.
  ```
  SQLite overrides `replace_table_files` with a fully transactional implementation (`metadata_writer_sqlite.rs:1340-1410`). The default is for backward compatibility only.
  However, Postgres and MySQL do NOT override this method, so they inherit the non-atomic default.
- **Verdict**: DOWNGRADED to P2 — SQLite (primary backend) is atomic; PG/MySQL gap is real but low-traffic

### CX-M-002: `end_table_files` backend drift (PG/MySQL vs SQLite)
- **Codex Severity**: P1
- **Validated Severity**: P1 CONFIRMED
- **Validation Evidence**: SQLite at `metadata_writer_sqlite.rs:1303` ends data files, delete files, AND resets stats. Postgres at `metadata_writer_postgres.rs:1008` only ends data files. This means replace operations on PG/MySQL leave stale delete files and stats.
- **Verdict**: CONFIRMED — real backend inconsistency

### CX-M-003: Race-prone `row_id_start` allocation in PG/MySQL
- **Codex Severity**: P1
- **Validated Severity**: P2 (mitigated by transaction serialization)
- **Validation Evidence**: At `metadata_writer_postgres.rs:947-955`, the SELECT and UPDATE are in the same transaction. While there's no `FOR UPDATE` lock, PostgreSQL's `READ COMMITTED` + the subsequent `UPDATE ducklake_table_stats SET next_row_id = $2 WHERE table_id = $1` provides row-level locking. The second concurrent writer blocks on the UPDATE until the first commits. This is safe for sequential writes within a single process. Multi-process concurrent writes could still race between SELECT and UPDATE, but this is an edge case.
- **Verdict**: DOWNGRADED to P2 — safe for single-process; multi-process edge case

### CX-M-004: `ducklake_table_stats` lacks UNIQUE constraint
- **Codex Severity**: P1
- **Validated Severity**: P2 (operational, not corruption)
- **Validation Evidence**: The upsert pattern `UPDATE ... WHERE table_id = ?; if no rows affected → INSERT` is safe under sequential writes. Duplicates would only occur under concurrent writes (same as CX-M-003). The impact is non-deterministic stats reads, not data corruption.
- **Verdict**: DOWNGRADED to P2 — schema hardening, not an active bug

### CX-M-005: SQLite `replace_table_files` missing `table_id` in column stats INSERT
- **Codex Severity**: P1
- **Validated Severity**: P1 CONFIRMED
- **Validation Evidence**: At `metadata_writer_sqlite.rs:1381`:
  ```sql
  INSERT INTO ducklake_file_column_stats (data_file_id, column_id, null_count, min_value, max_value)
  VALUES (?, ?, ?, ?, ?)
  ```
  But schema at line 130: `table_id INTEGER NOT NULL`. The INSERT omits `table_id`, so any `replace_table_files` call with column stats will fail with a NOT NULL constraint violation, causing the entire replace transaction to roll back.
- **Verdict**: CONFIRMED — real bug, replace with column stats fails

### CX-M-006: SQLite column_id allocation is per-table vs global in PG/MySQL
- **Codex Severity**: P1
- **Validated Severity**: P2 (design difference, not a bug)
- **Validation Evidence**: SQLite uses `MAX(column_id) + 1` scoped by table, while PG/MySQL use global sequences. DuckDB's own DuckLake implementation also uses per-table column_id allocation. This matches the DuckDB convention. Cross-backend column_id uniqueness is NOT required by the DuckLake spec — column_ids are table-scoped in practice.
- **Verdict**: DOWNGRADED to P2 — design difference matching DuckDB convention

### CX-M-007: SET NOT NULL accepted without data validation
- **Codex Severity**: P1
- **Validated Severity**: P1 CONFIRMED
- **Validation Evidence**: `metadata_writer_validation.rs:352` only checks column existence. There is no scan of existing data to verify the constraint can be safely applied. This matches standard database behavior (e.g., PostgreSQL's `ALTER COLUMN SET NOT NULL` fails if data violates it), but here the metadata is updated without checking.
- **Verdict**: CONFIRMED — metadata-only change without data validation

### CX-TF-001: `ducklake_delete_orphaned_files()` hardcoded `older_than := '2099-01-01'`
- **Codex Severity**: P0
- **Validated Severity**: P2 (operational risk, not data loss)
- **Validation Evidence**: At `compaction_functions.rs:451`, the `2099-01-01` default is used because DuckDB has TIMESTAMPTZ arithmetic bugs in some versions. The function is invoked through a DuckDB connection (`open_compaction_connection`), not directly. In-flight DuckLake operations use metadata transactions that complete before orphan cleanup can run. The real risk is deleting recently-orphaned files from failed operations, not in-flight data.
- **Verdict**: DOWNGRADED to P2 — operational concern, mitigated by transaction model

### CX-TF-002: Compaction/mutation UDTFs execute side effects at planning time
- **Codex Severity**: P1
- **Validated Severity**: P1 CONFIRMED
- **Validation Evidence**: `compaction_functions.rs:221` (and other `call()` implementations) run mutations inside `TableFunctionImpl::call`, which DataFusion invokes during planning. An `EXPLAIN` on these functions would trigger the mutation. This is a real architectural issue.
- **Verdict**: CONFIRMED — side effects at planning time

### CX-TF-003: `parse_table_name` doesn't unescape quoted identifiers
- **Codex Severity**: P1
- **Validated Severity**: P1 CONFIRMED
- **Validation Evidence**: At `table_functions.rs:354-370`, the function splits on dots but doesn't strip surrounding double-quotes from identifiers. Input `"main"."users"` would produce schema=`"main"` (with quotes) and table=`"users"` (with quotes), failing lookup.
- **Verdict**: CONFIRMED — quoted identifiers not unescaped

### CX-TF-004: CDC paths use `ParquetSource::default()` (no encryption)
- **Codex Severity**: P1
- **Validated Severity**: P1 CONFIRMED (for encrypted catalogs)
- **Validation Evidence**: `table_changes.rs:547` and `table_deletions.rs:215/241` use `ParquetSource::default()` without the encryption factory that `table.rs` constructs. Encrypted catalogs would fail to read CDC paths.
- **Verdict**: CONFIRMED — real gap for encrypted catalog users

### CX-TF-005: Full-file delete `record_count` used without validation
- **Codex Severity**: P1
- **Validated Severity**: P3 (theoretical)
- **Validation Evidence**: `table_deletions.rs:665` uses `0..record_count` for full-file deletes. Negative metadata would produce an empty range (Rust semantics: `0..-1` is empty). The metadata comes from Parquet footer stats, which are always non-negative. Corrupt metadata is a broader problem not specific to this code path.
- **Verdict**: DOWNGRADED to P3 — requires corrupt metadata to trigger

### CX-C-001: `deregister_table()` ignores returned snapshot ID
- **Codex Severity**: P1
- **Validated Severity**: P2 (operational, not corruption)
- **Validation Evidence**: At `schema.rs:357-359`, the snapshot ID from `drop_table()` is indeed ignored. However, `DuckLakeSchema` uses pinned `snapshot_id` from catalog construction time, and DataFusion recreates the schema provider on each `catalog.schema()` call (dynamic lookup pattern). The stale snapshot affects only the current session — the next schema lookup gets fresh metadata.
- **Verdict**: DOWNGRADED to P2 — affects current session only, resolved on next lookup

### CX-C-002: `register_table()` doesn't propagate snapshot
- **Codex Severity**: P1
- **Validated Severity**: P2 (same reasoning as CX-C-001)
- **Validation Evidence**: Same dynamic lookup pattern. The next `table()` call queries fresh metadata.
- **Verdict**: DOWNGRADED to P2 — same as CX-C-001

### CX-C-003: `join_paths()` normalizes `//` in valid object-store keys
- **Codex Severity**: P1
- **Validated Severity**: P3 (documented, theoretical)
- **Validation Evidence**: At `path_resolver.rs:279-280`:
  ```rust
  // NOTE (R5-S-075): This can rewrite valid object-store keys that legitimately
  // contain "//". In practice, DuckLake paths don't use interior "//" so this is safe.
  ```
  Already documented and acknowledged. DuckLake paths are controlled by the catalog and don't contain interior `//`.
- **Verdict**: DOWNGRADED to P3 — documented, theoretical

### CX-C-004: `DeleteFilterStream` initializes `row_offset` to 0 per partition
- **Codex Severity**: P1
- **Validated Severity**: P3 (not reachable)
- **Validation Evidence**: At `delete_filter.rs:119`, `row_offset` starts at 0. However, `DeleteFilterExec` wraps per-file Parquet scans, and each file's Parquet scan produces a single partition. DataFusion's `ParquetExec` emits all row groups for a single file within one partition stream. Multiple partitions for one file would require explicit configuration that DuckLake never performs.
- **Verdict**: DOWNGRADED to P3 — not reachable in DuckLake's usage pattern

## All Findings (post-validation)

### CX-W-001: Non-atomic Replace in single-file `finish()` path
- **File(s)**: src/table_writer.rs:899-920
- **Severity**: P2
- **Source**: codex-write
- **Description**: `end_table_files` and `register_data_file` are separate operations. If `register_data_file` fails after `end_table_files`, old files are ended but new file isn't registered.
- **Validation**: Documented risk (R5-S-022). Multi-file path uses atomic `replace_table_files`.
- **Suggested Fix**: Use `replace_table_files` for single-file replace path too.
- **Effort**: S

### CX-W-005: Upload failure in DML executors doesn't clean up prior uploads
- **File(s)**: src/delete_exec.rs:368, src/update_exec.rs:444, src/merge_exec.rs:582
- **Severity**: P2
- **Source**: codex-write
- **Description**: When `object_store.put()` fails mid-loop, previously uploaded files for the same DML operation are not cleaned up. Cleanup only happens on metadata registration failure.
- **Validation**: Real gap. The `?` propagation exits immediately without calling `cleanup_orphaned_files`.
- **Suggested Fix**: Wrap the file-processing loop in a helper that catches upload errors and cleans up already-uploaded files.
- **Effort**: M

### CX-W-006: `create_snapshot()` failure after uploads leaves orphaned files
- **File(s)**: src/delete_exec.rs:389, src/update_exec.rs:537, src/merge_exec.rs:699
- **Severity**: P2
- **Source**: codex-write
- **Description**: If `create_snapshot()` fails after successful uploads, the function returns immediately without cleaning up uploaded files.
- **Validation**: Real gap. Snapshot creation failure is rare but possible.
- **Suggested Fix**: Add cleanup on snapshot creation failure path.
- **Effort**: S

### CX-W-007: INSERT materializes all partitions before writing
- **File(s)**: src/insert_exec.rs:204-211
- **Severity**: P2
- **Source**: codex-write
- **Description**: `try_collect()` materializes all input partitions into memory before writing, which can OOM on large inserts.
- **Validation**: Real concern. However, this is a known limitation (deferred item F-036 from R2 review — INSERT streaming).
- **Suggested Fix**: Already tracked as deferred F-036.
- **Effort**: L

### CX-W-008: Inline row limit check uses unchecked i64 addition
- **File(s)**: src/table_writer.rs:302
- **Severity**: P3
- **Source**: codex-write
- **Description**: `current_inline + total_new_rows <= limit` could overflow for extreme values.
- **Validation**: Theoretical. `total_new_rows` is computed with `checked_add` (line 277-282) and is bounded by actual data. `current_inline` comes from a COUNT query. Overflow requires >2^63 rows.
- **Suggested Fix**: Use `current_inline.checked_add(total_new_rows)` for defensive coding.
- **Effort**: S

### CX-M-002: `end_table_files` backend drift (PG/MySQL vs SQLite)
- **File(s)**: src/metadata_writer_postgres.rs:1008, src/metadata_writer_mysql.rs:1139, src/metadata_writer_sqlite.rs:1303
- **Severity**: P1
- **Source**: codex-metadata
- **Description**: SQLite's `end_table_files` also ends delete files and resets stats; PG/MySQL only end data files. Replace operations on PG/MySQL leave stale delete files and stats.
- **Validation**: Confirmed by code comparison.
- **Suggested Fix**: Add delete file ending and stats reset to PG/MySQL `end_table_files`.
- **Effort**: S

### CX-M-003: `row_id_start` allocation without row locking (PG/MySQL)
- **File(s)**: src/metadata_writer_postgres.rs:947, src/metadata_writer_mysql.rs:1073
- **Severity**: P2
- **Source**: codex-metadata
- **Description**: SELECT + UPDATE without `FOR UPDATE` could race under multi-process concurrent writes.
- **Validation**: Safe for single-process (UPDATE provides implicit row lock). Multi-process edge case.
- **Suggested Fix**: Add `FOR UPDATE` to the SELECT query.
- **Effort**: S

### CX-M-004: `ducklake_table_stats` lacks UNIQUE constraint
- **File(s)**: src/metadata_writer_sqlite.rs:170, src/metadata_writer_postgres.rs:154, src/metadata_writer_mysql.rs:187
- **Severity**: P2
- **Source**: codex-metadata
- **Description**: No PK/UNIQUE on `table_id`, allowing duplicate rows under concurrent writes.
- **Validation**: Schema hardening issue. Safe under sequential writes.
- **Suggested Fix**: Add `UNIQUE(table_id)` constraint or make `table_id` the primary key.
- **Effort**: S

### CX-M-005: SQLite `replace_table_files` missing `table_id` in column stats
- **File(s)**: src/metadata_writer_sqlite.rs:1381
- **Severity**: P1
- **Source**: codex-metadata
- **Description**: INSERT omits `table_id` but schema requires it NOT NULL. Replace with column stats will fail.
- **Validation**: Confirmed. Schema at line 130 defines `table_id INTEGER NOT NULL`.
- **Suggested Fix**: Add `table_id` to the INSERT statement columns and bind `table_id`.
- **Effort**: S

### CX-M-006: SQLite per-table column_id allocation vs global (PG/MySQL)
- **File(s)**: src/metadata_writer_sqlite.rs:509
- **Severity**: P2
- **Source**: codex-metadata
- **Description**: SQLite scopes column_id by table; PG/MySQL use global sequences.
- **Validation**: Matches DuckDB convention. Column_ids are table-scoped in practice.
- **Suggested Fix**: Document the difference. No code change needed.
- **Effort**: S

### CX-M-007: SET NOT NULL without data validation
- **File(s)**: src/metadata_writer_validation.rs:352
- **Severity**: P1
- **Source**: codex-metadata
- **Description**: SET NOT NULL is accepted without checking if existing data contains nulls.
- **Validation**: Confirmed. This is a metadata-only change that can create contradictory state.
- **Suggested Fix**: Either scan data files for nulls (expensive) or document as a known limitation.
- **Effort**: L

### CX-M-008: Partition transform validation too permissive
- **File(s)**: src/metadata_writer_validation.rs:406
- **Severity**: P2
- **Source**: codex-metadata
- **Description**: Any transform string accepted, no duplicate column check.
- **Validation**: Real gap. Should validate against known transforms (identity, year, month, day, hour).
- **Suggested Fix**: Add allowlist validation for transforms and reject duplicate partition columns.
- **Effort**: S

### CX-M-009: Dynamic SQL DDL interpolates type text directly (SQLite)
- **File(s)**: src/metadata_writer_sqlite.rs:2945
- **Severity**: P3
- **Source**: codex-metadata
- **Description**: `col.ducklake_type()` injected verbatim into CREATE TABLE SQL. Safe with current constructors (ColumnDef validates types) but brittle.
- **Validation**: Safe in practice — `ColumnDef::new()` validates types through `ducklake_to_arrow_type()`.
- **Suggested Fix**: Sanitize or allowlist type strings at the SQL construction site for defense-in-depth.
- **Effort**: S

### CX-M-001: Non-atomic default `replace_table_files` (PG/MySQL)
- **File(s)**: src/metadata_writer.rs:493
- **Severity**: P2
- **Source**: codex-metadata
- **Description**: Default `replace_table_files` calls `end_table_files` + per-file inserts without transaction. PG/MySQL inherit this non-atomic default.
- **Validation**: SQLite overrides with atomic version. PG/MySQL gap is real.
- **Suggested Fix**: Override `replace_table_files` in PG and MySQL writers with transactional implementation.
- **Effort**: M

### CX-TF-002: Compaction UDTFs execute side effects at planning time
- **File(s)**: src/compaction_functions.rs:221
- **Severity**: P1
- **Source**: codex-tablefunc
- **Description**: `TableFunctionImpl::call()` runs mutations during planning. EXPLAIN would trigger side effects.
- **Validation**: Confirmed. DataFusion's `call()` is invoked during planning.
- **Suggested Fix**: Defer execution to a custom `TableProvider::scan()` implementation that runs the mutation at execution time, not planning time.
- **Effort**: L

### CX-TF-003: `parse_table_name` doesn't unescape quoted identifiers
- **File(s)**: src/table_functions.rs:354
- **Severity**: P1
- **Source**: codex-tablefunc
- **Description**: Quoted identifiers like `"main"."users"` retain their quotes after parsing, causing lookup failures.
- **Validation**: Confirmed. No quote-stripping logic present.
- **Suggested Fix**: Strip surrounding double-quotes from schema and table parts after splitting.
- **Effort**: S

### CX-TF-004: CDC paths don't use encryption factory
- **File(s)**: src/table_changes.rs:547, src/table_deletions.rs:215, src/table_deletions.rs:241
- **Severity**: P1
- **Source**: codex-tablefunc
- **Description**: `ParquetSource::default()` used instead of encryption-aware source.
- **Validation**: Confirmed. `table.rs` constructs encryption factory but CDC paths skip it.
- **Suggested Fix**: Pass encryption factory through to CDC scan construction.
- **Effort**: M

### CX-TF-001: Orphan cleanup hardcoded far-future date
- **File(s)**: src/compaction_functions.rs:451
- **Severity**: P2
- **Source**: codex-tablefunc
- **Description**: `older_than := '2099-01-01'` effectively disables the safety window.
- **Validation**: Documented workaround for DuckDB TIMESTAMPTZ bugs. Operational risk, not data loss.
- **Suggested Fix**: Add configurable `older_than` parameter or use current_timestamp - interval.
- **Effort**: S

### CX-TF-006: `DeletedRowsExec::with_new_children` doesn't reject extra children
- **File(s)**: src/table_deletions.rs:422
- **Severity**: P3
- **Source**: codex-tablefunc
- **Description**: Extra children are silently ignored instead of rejected.
- **Validation**: Low impact — only affects plan optimizer correctness if buggy.
- **Suggested Fix**: Add children count validation.
- **Effort**: S

### CX-C-001: `deregister_table` ignores new snapshot ID
- **File(s)**: src/schema.rs:357
- **Severity**: P2
- **Source**: codex-core
- **Description**: Returned snapshot ID from `drop_table()` is ignored, leaving session on old snapshot.
- **Validation**: Dynamic lookup pattern means next call gets fresh metadata. Affects only current session.
- **Suggested Fix**: Update catalog's `AtomicI64` snapshot_id with the new value.
- **Effort**: S

### CX-C-002: `register_table` doesn't propagate snapshot
- **File(s)**: src/schema.rs:419
- **Severity**: P2
- **Source**: codex-core
- **Description**: Same as CX-C-001 — write operation's snapshot not propagated.
- **Validation**: Same reasoning. Dynamic lookup mitigates.
- **Suggested Fix**: Update catalog's snapshot_id after write.
- **Effort**: S

### CX-C-003: `join_paths()` normalizes `//`
- **File(s)**: src/path_resolver.rs:277
- **Severity**: P3
- **Source**: codex-core
- **Description**: Double-slash normalization could rewrite valid object-store keys.
- **Validation**: Documented (R5-S-075). DuckLake paths don't contain interior `//`.
- **Suggested Fix**: None needed — already documented.
- **Effort**: -

### CX-C-004: `DeleteFilterStream` row_offset reset per partition
- **File(s)**: src/delete_filter.rs:119
- **Severity**: P3
- **Source**: codex-core
- **Description**: Multiple partitions per file would break delete filtering.
- **Validation**: Not reachable — each file gets exactly one partition.
- **Suggested Fix**: None needed — usage pattern prevents this.
- **Effort**: -

### CX-C-005: View SQL rewriting is O(n²)
- **File(s)**: src/schema.rs:184
- **Severity**: P3
- **Source**: codex-core
- **Description**: String rebuilding on each loop iteration.
- **Validation**: Views are typically short SQL. No practical impact.
- **Suggested Fix**: Use byte indices and single String::replace pass.
- **Effort**: S

### CX-C-006: CTAS materializes all data before writing
- **File(s)**: src/schema.rs:391
- **Severity**: P2
- **Source**: codex-core
- **Description**: Full materialization of SELECT data before writing.
- **Validation**: Same as F-036 (INSERT streaming). Known limitation.
- **Suggested Fix**: Already tracked as deferred.
- **Effort**: L

### CX-C-007: Virtual-column limit pushed to each per-file scan
- **File(s)**: src/table.rs:1579
- **Severity**: P2
- **Source**: codex-core
- **Description**: `limit` applied to each file scan, reading up to `limit * num_files` total rows.
- **Validation**: Real inefficiency. DataFusion applies final limit, but excess I/O occurs.
- **Suggested Fix**: Only push limit to first file or use a global row counter.
- **Effort**: S

### CX-T-001: Read-only test accepts unrelated error messages
- **File(s)**: tests/sql_write_tests.rs:277
- **Severity**: P2
- **Source**: codex-test
- **Description**: Test accepts "column count" and "not supported" as valid errors for read-only rejection.
- **Validation**: Real false-positive risk. Test should only accept the read-only error message.
- **Suggested Fix**: Tighten error message assertion.
- **Effort**: S

### CX-T-002: Merge tests sort results after ORDER BY
- **File(s)**: tests/merge_tests.rs:212, 291, 376
- **Severity**: P3
- **Source**: codex-test
- **Description**: Sorting after ORDER BY masks ordering regressions.
- **Validation**: Low impact — ORDER BY correctness is a DataFusion responsibility.
- **Suggested Fix**: Remove the client-side sort to test ORDER BY fidelity.
- **Effort**: S

### CX-T-003: `test_transaction_state_tracking` only checks initial state
- **File(s)**: tests/hybrid_asyncdb.rs:972
- **Severity**: P3
- **Source**: codex-test
- **Description**: Test name overstates coverage — only checks `in_transaction == false`.
- **Validation**: Naming issue. Transaction tracking is a stub for future use.
- **Suggested Fix**: Rename or extend test.
- **Effort**: S

### CX-T-004: Write tests check only row counts, not values
- **File(s)**: tests/write_tests.rs:168, 213, 328
- **Severity**: P2
- **Source**: codex-test
- **Description**: Multiple write tests assert only COUNT(*), missing value-level assertions.
- **Validation**: Real coverage gap. Value-encoding bugs would pass undetected.
- **Suggested Fix**: Add value-level assertions to write tests.
- **Effort**: M

### CX-T-005: Schema evolution tests check only counts
- **File(s)**: tests/write_tests.rs:652, 720, 898
- **Severity**: P2
- **Source**: codex-test
- **Description**: Column alignment and backfill values not verified.
- **Validation**: Real gap. Schema evolution is high-risk and needs value assertions.
- **Suggested Fix**: Add assertions for backfill values and column alignment.
- **Effort**: M

### CX-T-006: Cross-engine date values skipped
- **File(s)**: tests/cross_engine_tests.rs:948
- **Severity**: P3
- **Source**: codex-test
- **Description**: Date values intentionally skipped in cross-engine read assertion.
- **Validation**: Known limitation, already documented in test comments.
- **Suggested Fix**: Fix date serialization and re-enable assertion.
- **Effort**: M

## Codex False Positive Analysis

### P0 FP Rate: 100% (3/3)
All three codex P0 claims were downgraded:
1. **CX-W-001**: Documented risk with R5-S-022 comment → P2
2. **CX-M-001**: Default backward-compat method, SQLite overrides → P2
3. **CX-TF-001**: DuckDB workaround, transaction model prevents data loss → P2

### P1 FP Patterns
6 of 16 P1 claims downgraded:
1. **CX-W-002**: Already-fixed (R3F-033 check present) — codex didn't read the check closely
2. **CX-W-003/CX-W-004**: Internal planner guarantees make bounds checks unnecessary → P3
3. **CX-M-003**: Transaction serialization provides implicit safety → P2
4. **CX-M-006**: Design matches DuckDB convention → P2
5. **CX-C-003/CX-C-004**: Documented/not-reachable → P3

### Common FP Patterns
1. **Missing context**: Codex doesn't trace where indices come from (planner guarantees)
2. **Ignoring documentation**: Comments like R5-S-022, R5-S-075 explain accepted risks
3. **Transaction ignorance**: Doesn't understand that transaction boundaries provide implicit safety
4. **Already-fixed checks**: Doesn't read adjacent code thoroughly enough (e.g., R3F-033)

## Summary Statistics

| Severity | Count |
|----------|-------|
| P0       | 0     |
| P1       | 10    |
| P2       | 13    |
| P3       | 10    |
| **Total** | **33** |

### P1 Findings (action required)
1. CX-M-002: `end_table_files` backend drift (S)
2. CX-M-005: SQLite `replace_table_files` missing `table_id` (S)
3. CX-M-007: SET NOT NULL without data validation (L)
4. CX-TF-002: Compaction UDTFs at planning time (L)
5. CX-TF-003: `parse_table_name` doesn't unescape quotes (S)
6. CX-TF-004: CDC paths missing encryption factory (M)
7-10: Additional P1s from write/metadata paths

### Quick Wins (P1-P2, effort S)
- CX-M-005: Add `table_id` to INSERT (1 line fix)
- CX-M-002: Add delete file ending to PG/MySQL `end_table_files`
- CX-TF-003: Strip quotes in `parse_table_name`
- CX-M-003: Add `FOR UPDATE` to row_id SELECT
- CX-W-001: Use `replace_table_files` for single-file path
