# R11 Codex Review

## Summary
- Total findings: 48
- By priority: P0: 7 (2 validated, 3 downgraded to P1, 2 false positive), P1: 24 (16 validated, 4 downgraded to P2, 4 false positive), P2: 17
- Codex P0 false positive rate: 71% (5/7 not P0)
- Codex P1 false positive rate: 33% (8/24 not P1)
- Files reviewed: all 38 source files across 10 codex batches

## Validated P0 Findings

### R11-CX-001: append_table_files starts cumulative_row_id at 0
**Priority**: P0 (codex claimed: P0)
**Validation**: validated — `cumulative_row_id` is initialized to `0` without reading current `next_row_id` from `ducklake_table_stats`. Appending to non-empty tables reuses row ID ranges.
**Files**: `src/metadata_writer_impl.rs:812`
**Description**: `append_table_files` initializes `cumulative_row_id = 0` and uses it as `row_id_start` for appended files. When appending to a table that already has data, row IDs will overlap with existing rows, corrupting delete-vector semantics and row identity. The function also never updates `ducklake_table_stats.next_row_id`.
**Suggested fix**: Read current `next_row_id` from `ducklake_table_stats` (locked) at transaction start, use it as the starting offset, and update stats at commit.

### R11-CX-002: replace_table_files / register_dml_files default impls are non-atomic
**Priority**: P1 (codex claimed: P0)
**Validation**: downgraded to P1 — The code explicitly documents this as "default implementation is NOT atomic" and warns backends to override. All actual backends (SQLite, Postgres, MySQL) DO override with transactional implementations. The defaults exist only for backward compatibility / trait completeness.
**Files**: `src/metadata_writer.rs:468, 499`
**Description**: Default trait implementations for `register_dml_files` and `replace_table_files` perform per-file writes without a transaction. However, all backends override these with atomic implementations.
**Suggested fix**: Consider making these return `Err(Unsupported)` in the default to prevent accidental use without override.

## Validated P1 Findings

### R11-CX-003: Date32 num_days() as i32 truncation
**Priority**: P1 (codex claimed: P0)
**Validation**: downgraded to P1 — Affects only inlined data parsing for extreme dates. In practice, Date32 valid range is limited, but the truncation is technically unsafe.
**Files**: `src/parse_values.rs:130`
**Description**: `date.signed_duration_since(UNIX_EPOCH_DATE).num_days() as i32` performs unchecked narrowing. Out-of-range parsed dates can wrap/truncate into incorrect day offsets.
**Suggested fix**: Use `i32::try_from(num_days)` and handle overflow per ParseMode (null in Lenient, error in Strict).

### R11-CX-004: Decimal pow(scale_u) can overflow before checked_mul
**Priority**: P1 (codex claimed: P0)
**Validation**: downgraded to P1 — `10i128.pow(scale_u)` where `scale_u` comes from `Decimal128(_, scale)`. Max Decimal128 scale is 38, and `10i128.pow(38)` fits in i128 (< 2^127). So in practice this doesn't overflow for valid decimal types. However, if invalid scale metadata is encountered, it could overflow.
**Files**: `src/parse_values.rs:362, 382`
**Description**: `10i128.pow(scale_u)` is computed before being used in `checked_mul`. While valid Decimal128 scales (0-38) fit, invalid metadata could cause overflow.
**Suggested fix**: Replace with iterative checked power helper for defense-in-depth.

### R11-CX-005: create_ddl_snapshot FOR UPDATE with aggregate fails on Postgres
**Priority**: P1 (codex claimed: P1)
**Validation**: validated — PostgreSQL does not allow `FOR UPDATE` with aggregate queries. `SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot FOR UPDATE` will fail at runtime on Postgres.
**Files**: `src/metadata_writer_impl.rs:1213-1216`
**Description**: The DDL snapshot macro builds `SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot FOR UPDATE`. PostgreSQL rejects this.
**Suggested fix**: Use `SELECT schema_version FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1 FOR UPDATE` or a subquery approach.

### R11-CX-006: get_file_column_stats ignores snapshot_id for column join
**Priority**: P1 (codex claimed: P1)
**Validation**: validated — The query joins `ducklake_column` with `c.end_snapshot IS NULL` only, not filtering by the requested `snapshot_id`. For historical snapshot queries, this returns current column names instead of names valid at that snapshot.
**Files**: `src/metadata_provider_impl.rs:694`
**Description**: Column stats query uses `c.end_snapshot IS NULL` instead of temporal snapshot predicates, returning current column definitions for historical snapshots.
**Suggested fix**: Add `c.begin_snapshot <= snapshot_id AND (c.end_snapshot IS NULL OR c.end_snapshot > snapshot_id)`.

### R11-CX-007: get_partition_columns ignores snapshot_id for column join
**Priority**: P1 (codex claimed: P1)
**Validation**: validated — Same pattern as R11-CX-006.
**Files**: `src/metadata_provider_impl.rs:791`
**Description**: Partition column query joins columns with `c.end_snapshot IS NULL` only, ignoring the target snapshot.
**Suggested fix**: Apply snapshot-window predicates to `ducklake_column`.

### R11-CX-008: register_delete_file doesn't update table stats or next_file_id
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — `register_delete_file` is the low-level primitive. The high-level `register_dml_files` (which all DML paths use) handles stats and next_file_id. Direct callers would need to handle this themselves.
**Files**: `src/metadata_writer_impl.rs:1128`
**Description**: `register_delete_file` doesn't update `ducklake_table_stats.record_count` or `ducklake_snapshot.next_file_id`. However, it's wrapped by `register_dml_files` which does.
**Suggested fix**: Document that callers must use `register_dml_files` for correct accounting, or add stats update to standalone path.

### R11-CX-009: delete_filter.rs unchecked row_offset arithmetic
**Priority**: P1 (codex claimed: P1)
**Validation**: validated — `self.row_offset += num_rows` at line 160 and `self.row_offset + i64::from(i)` at line 188 are both unchecked. While overflow requires >9.2 quintillion rows (unlikely), this is a correctness gap.
**Files**: `src/delete_filter.rs:160, 188`
**Description**: Row offset accumulation and global position computation use unchecked i64 addition. Near i64::MAX, this wraps and produces incorrect delete filtering.
**Suggested fix**: Use `checked_add` and return `DataFusionError` on overflow.

### R11-CX-010: Timestamp partition transform unchecked multiplication
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — Timestamp values are bounded by valid datetime range; extreme values would need timestamps far outside the valid datetime range. Low practical risk.
**Files**: `src/insert_exec.rs:433`, `src/table_writer.rs:1149`
**Description**: Timestamp unit conversions multiply raw values (`* 1_000_000`, `* 1_000`) without overflow checks.
**Suggested fix**: Use `checked_mul` for defense-in-depth.

### R11-CX-011: MERGE multi-match detection is correct but break is misleading
**Priority**: P2 (codex claimed: P1)
**Validation**: false positive — Code at line 493-512 correctly tracks `source_match_count` per source row across ALL target rows. The `break` at line 512 exits the inner candidates loop (per target row), not the outer loop. If the same source matches multiple targets, `source_match_count` increments and errors at line 499-504. The multi-target-per-source check IS implemented correctly.
**Files**: `src/merge_exec.rs:493`
**Description**: Codex claimed multi-match silently uses first candidate. In reality, the code correctly detects when a source row matches multiple target rows and returns an execution error.
**Suggested fix**: None needed.

### R11-CX-012: merge_exec unchecked column index access
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — Column indices come from query planner which validates against schema. Runtime panic from invalid indices would indicate a planner bug, which is an internal error. Arrow itself would also panic on out-of-range access.
**Files**: `src/merge_exec.rs:320, 481`
**Description**: `batch.column(col_idx)` used without bounds checking. Indices are planner-validated.
**Suggested fix**: Add debug-mode assertions or use `get()` for clearer error messages.

### R11-CX-013: UPDATE assignment positional matching
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — DataFusion's SQL planner guarantees that UPDATE projection expressions are in table column order (one per column). The optimizer does not reorder projections. This is by design.
**Files**: `src/query_planner.rs:211`
**Description**: UPDATE assignment detection relies on positional index matching. This is correct given DataFusion's planner guarantees.
**Suggested fix**: Consider adding a defensive assertion that projection aliases match schema field names.

### R11-CX-014: UPDATE extra projections logged as warning, not error
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — This handles edge cases from planner (e.g., extra computed columns). The warning is intentional.
**Files**: `src/query_planner.rs:202`
**Description**: Extra projection expressions beyond schema fields are warned and ignored.
**Suggested fix**: Consider upgrading to error if this indicates a planner contract violation.

### R11-CX-015: DuckDB delete_file_id silent error swallowing
**Priority**: P1 (codex claimed: P1)
**Validation**: validated — `if let Ok(Some(_)) = row.get::<_, Option<i64>>(6)` silently treats type-conversion errors as "no delete file". A corrupt/mistyped column could be silently ignored.
**Files**: `src/metadata_provider_duckdb.rs:269, 462`
**Description**: Delete file ID parsing swallows errors, treating decode failures as absent delete files.
**Suggested fix**: Use `row.get::<_, Option<i64>>(6)?` with `?` propagation, then match on the Option.

### R11-CX-016: DuckDB get_inlined_data missing schema qualification
**Priority**: P1 (codex claimed: P1)
**Validation**: validated — The `information_schema.tables` query at line 629 checks only `table_name` without `table_schema`, unlike `count_inlined_rows` which uses `main` schema. Could resolve wrong table when names collide across schemas.
**Files**: `src/metadata_provider_duckdb.rs:629`
**Description**: Table existence check lacks schema predicate, potentially matching wrong table.
**Suggested fix**: Add `AND table_schema = 'main'` to the query.

### R11-CX-017: DuckDB fc + ic unchecked addition in get_table_row_count
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — Would require >9.2 quintillion rows. Theoretical but practically impossible.
**Files**: `src/metadata_provider_duckdb.rs:532`
**Description**: `fc + ic` uses unchecked i64 addition.
**Suggested fix**: Use `checked_add`.

### R11-CX-018: column_rename.rs reverse mapping collision
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — HashMap::collect() silently drops duplicates. However, the name_mapping is constructed from schema evolution which guarantees unique new names (validated by `validate_no_duplicate_columns`). Theoretical only.
**Files**: `src/column_rename.rs:56`
**Description**: Reverse mapping HashMap silently overwrites on duplicate new-name keys.
**Suggested fix**: Add debug assertion for uniqueness.

### R11-CX-019: parse_values Decimal256 routed through i128
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — DuckLake doesn't currently use Decimal256. This is a limitation, not a bug.
**Files**: `src/parse_values.rs:278`
**Description**: Decimal256 parsing uses i128 intermediate, truncating values outside i128 range.
**Suggested fix**: Implement i256-based parser when Decimal256 support is needed.

### R11-CX-020: parse_values timestamp unit interpretation
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — Inlined data timestamps are stored as strings, not raw integers. The integer path is a fallback that's rarely used.
**Files**: `src/parse_values.rs:198`
**Description**: Integer timestamp input always interpreted as microseconds regardless of target TimeUnit.
**Suggested fix**: Parse according to target TimeUnit or document the assumption.

### R11-CX-021: Missing delete-file size defaults to 0
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — Delete file size metadata is always populated by the write path. Missing size only occurs for corrupt/external catalogs. The warning + default-to-0 is a reasonable degradation.
**Files**: `src/table_changes.rs:603`, `src/table_deletions.rs:127`
**Description**: Missing delete-file size metadata defaults to 0 with warning.
**Suggested fix**: Consider failing fast instead of defaulting.

### R11-CX-022: Delete position validation
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — Delete positions come from our own delete file writer which produces valid positions. Corrupt files are the only risk vector.
**Files**: `src/table_deletions.rs:684`
**Description**: Delete positions are accepted without range validation.
**Suggested fix**: Add validation for defense-in-depth.

### R11-CX-023: Type promotion validation is case-sensitive
**Priority**: P1 (codex claimed: P1)
**Validation**: validated — `is_type_promotion_allowed` uses lowercase string literals. DuckLake type names are stored lowercase by convention, but external catalogs or DuckDB may use different casing.
**Files**: `src/metadata_writer_validation.rs:286`
**Description**: Type promotion checks are case-sensitive. Mixed-case type names from external sources would be incorrectly rejected.
**Suggested fix**: Normalize both types to lowercase before comparison.

### R11-CX-024: compaction delete_threshold NaN passthrough
**Priority**: P1 (codex claimed: P1)
**Validation**: validated — `NaN < 0.0` and `NaN > 1.0` are both false, so NaN passes the range check and gets interpolated into SQL.
**Files**: `src/compaction_functions.rs:364`
**Description**: `delete_threshold` range check doesn't reject NaN/infinity.
**Suggested fix**: Add `threshold.is_finite()` check.

### R11-CX-025: Schema stale snapshot_id after DDL
**Priority**: P2 (codex claimed: P1)
**Validation**: downgraded to P2 — DuckLakeSchema instances are short-lived (created per query via dynamic lookup). The `catalog_snapshot_id` AtomicI64 propagates new snapshots to the catalog level. New queries get fresh schema instances with updated snapshot_ids.
**Files**: `src/schema.rs:85`
**Description**: `DuckLakeSchema` has immutable `snapshot_id`. After DDL, the same instance has stale snapshot.
**Suggested fix**: Not needed — dynamic lookup ensures fresh instances per query.

### R11-CX-026: Nullable-ignoring schema equality checks
**Priority**: P1 (codex claimed: P1)
**Validation**: validated — Schema match checks in all three backends (SQLite, Postgres, MySQL) compare only name/type, ignoring nullability. This means nullability changes during INSERT schema evolution go undetected.
**Files**: `src/metadata_writer_sqlite.rs:559`, `src/metadata_writer_postgres.rs:442`, `src/metadata_writer_mysql.rs:559`
**Description**: Schema equality ignores nullable state, allowing nullability drift to go unrecorded.
**Suggested fix**: Include nullability in schema comparison.

## Validated P2 Findings

### R11-CX-027: Partitioned INSERT missing cleanup on commit failure
**Priority**: P2 (codex claimed: P2)
**Files**: `src/insert_exec.rs:816`
**Description**: If `commit_uploaded_files` fails after uploads succeed, already-uploaded Parquet files are not cleaned up.
**Suggested fix**: Add cleanup on commit failure.

### R11-CX-028: INSERT try_collect() full materialization
**Priority**: P2 (codex claimed: P2)
**Files**: `src/insert_exec.rs:246`
**Description**: `try_collect()` materializes entire input stream. Known limitation (F-036 deferred).
**Suggested fix**: Deferred to F-036 streaming INSERT.

### R11-CX-029: inlined_rows_to_batch O(n*cols^2) position lookup
**Priority**: P2 (codex claimed: P2)
**Files**: `src/table_writer.rs:1241`
**Description**: Per-row per-column linear name lookup via `position()`.
**Suggested fix**: Precompute HashMap for O(1) lookups.

### R11-CX-030: MERGE memory accumulation
**Priority**: P2 (codex claimed: P2)
**Files**: `src/merge_exec.rs:392`
**Description**: `new_data_batches` and source hash index accumulated fully in memory.
**Suggested fix**: Deferred — streaming MERGE is complex.

### R11-CX-031: read_delete_file_positions full batch collection
**Priority**: P2 (codex claimed: P2)
**Files**: `src/table.rs:533`
**Description**: Delete file batches collected into Vec before extracting positions.
**Suggested fix**: Stream incrementally.

### R11-CX-032: Null-count aggregation unchecked i64 addition
**Priority**: P2 (codex claimed: P2)
**Files**: `src/table.rs:1379`
**Description**: `null_counts[col_idx] += nc` is unchecked.
**Suggested fix**: Use saturating_add.

### R11-CX-033: DUCKLAKE_INSTALLED check-then-set race
**Priority**: P2 (codex claimed: P2)
**Files**: `src/compaction_functions.rs:85`
**Description**: Concurrent calls can execute `INSTALL ducklake` simultaneously.
**Suggested fix**: Use OnceLock or similar.

### R11-CX-034: information_schema unchecked i64 aggregation
**Priority**: P2 (codex claimed: P2)
**Files**: `src/information_schema.rs:469`
**Description**: File count/size aggregation uses unchecked arithmetic.
**Suggested fix**: Use saturating arithmetic.

### R11-CX-035: cdc_common O(n^2) reorder mapping
**Priority**: P2 (codex claimed: P2)
**Files**: `src/cdc_common.rs:98`
**Description**: Reorder mapping uses `position()` per projected index.
**Suggested fix**: Precompute positions in single pass.

### R11-CX-036: block_on panics outside Tokio runtime
**Priority**: P2 (codex claimed: P2)
**Files**: `src/metadata_provider.rs:784`
**Description**: `Handle::current()` panics if no Tokio runtime is active.
**Suggested fix**: Use `Handle::try_current()` with fallback.

### R11-CX-037: Encryption key lookup O(n) fallback
**Priority**: P2 (codex claimed: P2)
**Files**: `src/encryption.rs:251`
**Description**: Linear scan over all keys on normalized-path mismatch.
**Suggested fix**: Precompute normalized-path HashMap.

### R11-CX-038: store_inlined_data O(rows*cols^2) in SQLite writer
**Priority**: P2 (codex claimed: P2)
**Files**: `src/metadata_writer_sqlite.rs:1328`
**Description**: Per-cell linear name lookup in nested loops.
**Suggested fix**: Precompute HashMap.

### R11-CX-039: next_entity_id table_id unwrap
**Priority**: P2 (codex claimed: P2)
**Files**: `src/metadata_writer_sqlite.rs:706`
**Description**: `table_id.unwrap()` can panic on contract violation.
**Suggested fix**: Use `ok_or_else(...)`.

### R11-CX-040: table_deletions full position Vec materialization
**Priority**: P2 (codex claimed: P2)
**Files**: `src/table_deletions.rs:710`
**Description**: `(0..record_count)` collected into Vec for full-file deletes.
**Suggested fix**: Use lazy range check.

### R11-CX-041: with_new_children lax child count validation
**Priority**: P3 (codex claimed: P2)
**Files**: `src/table_deletions.rs:467`
**Description**: `with_new_children` doesn't validate exact child count.
**Suggested fix**: Add validation.

### R11-CX-042: TOCTOU in inlined data reads (all backends)
**Priority**: P2 (codex claimed: P2)
**Files**: `src/metadata_provider_sqlite.rs:222`, `src/metadata_provider_postgres.rs:208`, `src/metadata_provider_mysql.rs:216`
**Description**: Separate existence/columns/data queries vulnerable to concurrent DDL.
**Suggested fix**: Execute within single transaction.

### R11-CX-043: SQLite delete_files correlated subquery performance
**Priority**: P2 (codex claimed: P2)
**Files**: `src/metadata_provider_sqlite.rs:76`
**Description**: Four correlated subqueries per row in delete file queries.
**Suggested fix**: Use CTE/window-function strategy.

## False Positive Findings

### R11-CX-044: path_resolver normalize_path_separators
**Priority**: false positive (codex claimed: P0)
**Validation**: false positive — The code has an explicit comment (R5-S-075) acknowledging this behavior and noting "In practice, DuckLake paths don't use interior '//' so this is safe." DuckLake controls all path generation.
**Files**: `src/path_resolver.rs:264`

### R11-CX-045: Missing active-name uniqueness constraints
**Priority**: false positive (codex claimed: P0)
**Validation**: false positive — DuckLake follows DuckDB's catalog design which uses application-level uniqueness via snapshot-based visibility. The write paths use `get_or_create_*` patterns with checked writes that detect conflicts. Adding partial unique indexes would conflict with the soft-delete (end_snapshot) pattern.
**Files**: `src/metadata_writer_mysql.rs:36`, `src/metadata_writer_sqlite.rs:109`

### R11-CX-046: types.rs split_top_level quoted string handling
**Priority**: P3 (codex claimed: P1)
**Validation**: downgraded to P3 — Commas in quoted struct field names are extremely rare in practice. DuckLake type strings come from controlled catalog metadata.
**Files**: `src/types.rs:520`

### R11-CX-047: types.rs escaped quote handling
**Priority**: P3 (codex claimed: P1)
**Validation**: downgraded to P3 — Same reasoning as R11-CX-046.
**Files**: `src/types.rs:459`

### R11-CX-048: path_resolver drops query/fragment from URLs
**Priority**: P3 (codex claimed: P1)
**Validation**: downgraded to P3 — DuckLake paths are catalog-controlled paths, not user-provided URLs with query parameters. S3 versionId is not a supported feature.
**Files**: `src/path_resolver.rs:133`

## Priority Summary (Post-Validation)

| Priority | Count | Findings |
|----------|-------|----------|
| P0 | 1 | R11-CX-001 |
| P1 | 10 | R11-CX-002, 003, 005, 006, 007, 009, 015, 016, 023, 024, 026 |
| P2 | 31 | R11-CX-004, 008, 010-014, 017-022, 025, 027-043 |
| P3 | 4 | R11-CX-041, 046, 047, 048 |
| False positive | 2 | R11-CX-044, 045 |
