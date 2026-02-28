# DataFusion-DuckLake: Remaining Gaps

Comprehensive inventory of every remaining gap between DataFusion-DuckLake and
DuckDB's DuckLake extension, as of 2026-02-28 (commit `8c39e7e`).

---

## 1. Multi-Backend Writer Parity Gaps

### 1.1 MetadataWriter: Postgres/MySQL Missing Methods

All three writers (SQLite, Postgres, MySQL) implement the core MetadataWriter trait
methods. The following four methods are **stub-only** in Postgres and MySQL
(they return `Err("not yet implemented")`):

| Method | SQLite | Postgres | MySQL |
|--------|--------|----------|-------|
| `rename_table` | Full | Stub | Stub |
| `set_table_comment` | Full | Stub | Stub |
| `set_column_comment` | Full | Stub | Stub |
| `rename_view` | Full | Stub | Stub |

**What's needed**: Port the SQLite implementations to Postgres/MySQL. The logic
is identical — only SQL placeholder syntax differs (`?` vs `$N` vs `?`, and
`INSERT OR REPLACE` vs `ON CONFLICT DO UPDATE` vs `ON DUPLICATE KEY UPDATE`).

**Estimated effort**: Small (each method is 20-40 lines of straightforward SQL).

**Suggested approach**: Copy the SQLite implementations, adjust placeholder
syntax and upsert patterns.

### 1.2 MetadataProvider: Postgres/MySQL Missing Methods

The MetadataProvider trait has default implementations (return empty/not-found)
for several methods. SQLite overrides all of them; Postgres and MySQL use defaults:

| Method | SQLite | Postgres | MySQL | DuckDB |
|--------|--------|----------|-------|--------|
| `list_views` | Full | Default (empty) | Default (empty) | Default (empty) |
| `get_view_by_name` | Full | Default (None) | Default (None) | Default (None) |
| `view_exists` | Full | Default (false) | Default (false) | Default (false) |
| `get_file_column_stats` | Full | Default (empty) | Default (empty) | Default (empty) |
| `get_table_row_count` | Full | Default (None) | Default (None) | Default (None) |
| `get_partition_columns` | Full | Default (empty) | Default (empty) | Default (empty) |
| `get_file_partition_values` | Full | Default (empty) | Default (empty) | Default (empty) |
| `get_inlined_data` | Full | Default (empty) | Default (empty) | Default (empty) |

**What's needed**: Implement these methods in Postgres and MySQL providers.
The SQL queries are already defined as shared constants in `metadata_provider.rs`
for DuckDB-compatible dialects; Postgres/MySQL just need parameterized equivalents.

**What's blocking**: Nothing — these are straightforward SQL translations.

**Estimated effort**: Medium (each method is 10-30 lines, but there are 8 of them
across 2 backends = ~16 implementations).

**Suggested approach**: Start with `list_views`/`get_view_by_name`/`view_exists`
since views are already supported on the write side.

### 1.3 H-3 Bug Fix: `list_all_columns` Missing Column Filter

**Status**: Fixed in this commit.

The `list_all_columns()` method in ALL providers (Postgres, MySQL, SQLite, and
the shared `SQL_LIST_ALL_COLUMNS` constant used by DuckDB) was missing
`AND c.end_snapshot IS NULL` in the WHERE clause. This could return superseded
(dropped/renamed) columns alongside current columns.

Note: `get_table_structure()` already had the correct filter in all backends.

### 1.4 ID Generation Strategy Differences

| Backend | Strategy | Concurrent-Safe? |
|---------|----------|-----------------|
| SQLite | `MAX(id) + 1` | Yes (single-writer WAL mode) |
| Postgres | `nextval('seq')` | Yes (sequences are concurrent-safe) |
| MySQL | `MAX(id) + 1 FOR UPDATE` or `AUTO_INCREMENT` | Mostly (FOR UPDATE provides row locking) |

These differences are acceptable — each backend uses the idiomatic approach for
its database engine.

### 1.5 Schema DDL Differences

All three backends create identical catalog tables. Minor dialect differences:

- **Postgres**: Uses `BIGINT GENERATED ALWAYS AS IDENTITY`, `UUID` type, `BOOLEAN` defaults as `TRUE/FALSE`
- **MySQL**: Uses `BIGINT AUTO_INCREMENT`, `VARCHAR(255)` for UUIDs, backtick-quoted reserved words (`key`, `sql`, `type`)
- **SQLite**: Uses `INTEGER PRIMARY KEY` (implicit autoincrement), `TEXT` for timestamps

These are correct for each database's dialect.

---

## 2. Write-Side Partitioning

**Current status**: Not started

**What's needed**:
1. During INSERT, evaluate partition transform expressions (IDENTITY, YEAR, MONTH, DAY, HOUR)
   on each row to determine the target partition
2. Route rows to per-partition Parquet files using Hive-style directory layout
   (e.g., `year=2024/month=01/data.parquet`)
3. Record partition values in `ducklake_file_partition_value` table
4. Create/update `ducklake_partition_info` and `ducklake_partition_column` entries

**What's blocking**: Nothing fundamental. The partition metadata tables already
exist in all writer DDL schemas. The main complexity is the row routing logic
in `DuckLakeInsertExec`.

**Estimated effort**: Large

**Suggested approach**:
- Add `PartitionConfig` to `DuckLakeTable` (read from `ducklake_partition_info`/`ducklake_partition_column`)
- In `DuckLakeInsertExec`, partition incoming batches by computing transform values
- Write separate Parquet files per partition group
- Register each file with its partition values in metadata

---

## 3. Write-Side Data Inlining + `ducklake_flush_inlined_data()`

**Current status**: Not started (read-side partial: `get_inlined_data()` exists in SQLite provider)

**What's needed**:
1. Configurable row-count threshold below which data is stored inline in the
   metadata catalog (in `ducklake_inlined_data_tables`) rather than as Parquet files
2. `ducklake_flush_inlined_data()` table function to materialize inlined data to Parquet
3. Read-side integration to merge inlined data with Parquet data during scans
4. Inlined deletion tracking (deletes against inlined data stored in metadata)

**What's blocking**: This is a DuckDB-specific optimization. DuckDB stores
inlined data as actual DuckDB tables in the catalog database. Implementing this
with sqlx/Postgres/MySQL would require a completely different storage approach
(e.g., storing data as JSON or a serialized format in the catalog).

**Estimated effort**: Large (and architecturally questionable for non-DuckDB backends)

**Suggested approach**: Consider implementing only for the DuckDB metadata backend
(where it's natural) and skip for Postgres/MySQL/SQLite. Document that small
tables created by DuckDB with inlining are not readable from Postgres/MySQL/SQLite
providers until flushed.

---

## 4. Complex Type Evolution (Struct Field ADD/REMOVE/RENAME)

**Current status**: Partial — basic LIST, STRUCT, MAP type parsing is implemented
in `types.rs`. Schema evolution for complex types is not implemented.

**What's needed**:
1. `ADD_FIELD`, `REMOVE_FIELD`, `RENAME_FIELD` ALTER TABLE operations for struct columns
2. Recursive field_id tracking for nested types
3. Schema version tracking when nested structure changes
4. Column mapping updates for nested field renames

**What's blocking**: The `MetadataWriter` trait's `AlterTableOp` enum currently
covers flat column operations only. Adding nested field operations requires:
- New `AlterTableOp` variants
- `ducklake_column_mapping` / `ducklake_name_mapping` integration
- Parquet reader support for nested field_id mapping

**Estimated effort**: Large

**Suggested approach**: Start with read-side support (parsing complex type strings,
mapping nested field_ids). Write-side evolution can follow.

---

## 5. Encrypted Writes

**Current status**: Decrypt-only (`src/encryption.rs` reads encrypted Parquet files)

**What's needed**:
1. Encryption key generation for new data files and delete files
2. Pass encryption configuration to the Parquet writer in `DuckLakeTableWriter`
3. Store encryption keys per-file in `ducklake_data_file.encryption_key` and
   `ducklake_delete_file.encryption_key`
4. Support the DuckDB-specific encryption format (AES-256-GCM with custom key wrapping)

**What's blocking**: DuckDB uses a non-standard Parquet encryption scheme
(not Apache Parquet Modular Encryption / PME). The `parquet-rs` crate supports
PME but not DuckDB's custom format. A compatibility wrapper is needed.

**Estimated effort**: Large

**Suggested approach**: As documented in `docs/gap-analysis.md`, the DuckDB
encryption format uses AES-256-GCM with a custom key derivation scheme.
Options:
- Implement a custom `EncryptionFactory` for writes that matches DuckDB's format
- Or use standard PME and accept that files won't be readable by DuckDB's
  native encryption reader (and vice versa)

The first option maintains interop but requires reverse-engineering DuckDB's
exact key derivation. The second is easier but breaks cross-tool compatibility.

---

## 6. SQL-Level MERGE INTO

**Current status**: Not started

**What's needed**:
1. SQL parser support for `MERGE INTO ... USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED ...`
2. Logical plan conversion to a combination of DELETE + INSERT + UPDATE operations
3. Physical execution plan that orchestrates the three operations atomically

**What's blocking**: DataFusion's SQL parser (based on sqlparser-rs) does parse
MERGE INTO syntax, but DataFusion's logical planner does not convert it into
executable plans. This requires either:
- Extending DataFusion's planner to support MERGE INTO natively
- Implementing a custom SQL pre-processor that decomposes MERGE INTO into
  multiple statements

Also requires DELETE and UPDATE support to be complete first.

**Estimated effort**: Large

**Suggested approach**: Implement as a custom `QueryPlanner` extension that
intercepts MERGE INTO logical plans and decomposes them into separate
DELETE + INSERT operations. This avoids needing to modify DataFusion core.

---

## 7. SQL-Level Time Travel Syntax

**Current status**: Not started (catalog pins to latest snapshot at creation time)

**What's needed**:
1. Support for `SELECT * FROM table AT SNAPSHOT <id>` syntax
2. Support for `SELECT * FROM table AS OF TIMESTAMP '<ts>'` syntax
3. Per-query snapshot override without recreating the catalog
4. Snapshot-to-timestamp resolution via `ducklake_snapshot` table

**What's blocking**: DataFusion's SQL parser does not support `AT SNAPSHOT` or
`AS OF` syntax. Options:
- Custom SQL pre-processor to rewrite time travel queries
- DataFusion 52+ may add table versioning support (blocked on upgrade)
- Table function approach: `ducklake_table_at_snapshot('table', snapshot_id)`

**Estimated effort**: Medium (table function approach) to Large (parser extension)

**Suggested approach**: Implement as a table function first:
```sql
SELECT * FROM ducklake_table_at_snapshot('schema.table', 42)
```
This avoids SQL parser changes entirely. The table function creates a
`DuckLakeTable` pinned to the specified snapshot. Later, a full `AT SNAPSHOT`
syntax extension can be added.

---

## 8. SQLite Catalog Format Write Interop

**Current status**: Partial — our SQLite writer creates valid catalogs, but with
minor differences from DuckDB-created catalogs.

**What's needed**:
1. Verify that catalogs created by our SQLite writer are readable by DuckDB's
   DuckLake extension (bidirectional compatibility)
2. Ensure version string (`ducklake_metadata.version`) matches expected values
3. Test migration paths: our v0.3 catalogs should be readable by DuckDB 1.4.x

**What's blocking**: Testing requires DuckDB CLI with DuckLake extension installed.

**Estimated effort**: Small (mostly testing and minor fixes)

**Suggested approach**: Create a CI test that:
1. Creates a catalog with our SQLite writer
2. Opens it with DuckDB CLI + DuckLake extension
3. Verifies tables/data are readable
4. And vice versa: create with DuckDB, read with our provider

---

## 9. Remaining SLT (SqlLogicTest) Failures

As of commit `8c39e7e`, 75 of 248 SLT tests pass (30%). The remaining 173
failures fall into these categories:

### 9.1 DELETE Statement Support
**Tests affected**: ~20-30 tests
**Status**: DELETE execution plan exists but many edge cases fail
**What's needed**: Fix remaining DELETE edge cases, handle DELETE with WHERE clauses,
multi-table deletes, DELETE RETURNING

### 9.2 UPDATE Statement Support
**Tests affected**: ~15-20 tests
**Status**: UPDATE execution plan exists but many edge cases fail
**What's needed**: Fix remaining UPDATE edge cases, UPDATE with subqueries,
UPDATE with JOINs, computed SET expressions

### 9.3 Complex Types (LIST, STRUCT, MAP)
**Tests affected**: ~10-15 tests
**Status**: Type parsing implemented, but schema evolution and nested operations fail
**What's needed**: Full complex type support in reads and writes

### 9.4 Partitioning Tests
**Tests affected**: ~10 tests
**Status**: Not started
**What's needed**: Write-side partitioning (Gap #2 above) plus partition pruning on reads

### 9.5 Data Inlining Tests
**Tests affected**: ~5-8 tests
**Status**: Not started
**What's needed**: Data inlining support (Gap #3 above)

### 9.6 Compaction Tests
**Tests affected**: ~5 tests
**Status**: Not started
**What's needed**: `ducklake_merge_adjacent_files()` and `ducklake_rewrite_data_files()` table functions

### 9.7 MERGE INTO Tests
**Tests affected**: ~3-5 tests
**Status**: Not started
**What's needed**: MERGE INTO support (Gap #6 above)

### 9.8 Time Travel Tests
**Tests affected**: ~3-5 tests
**Status**: Not started
**What's needed**: AT SNAPSHOT syntax (Gap #7 above)

### 9.9 Macro Tests
**Tests affected**: ~3-5 tests
**Status**: Not started
**What's needed**: CREATE MACRO / table macro support in catalog

### 9.10 Sort Key / Configuration Tests
**Tests affected**: ~3-5 tests
**Status**: Not started
**What's needed**: `SET_SORTED_BY` ALTER TABLE support, sort-preserving writes

### 9.11 Various SQL Compatibility Issues
**Tests affected**: ~20-30 tests
**Status**: Mixed — some are DataFusion SQL dialect differences
**What's needed**: Case-by-case analysis. Some may need DataFusion SQL compatibility
improvements, others may need DuckLake-specific SQL rewriting.

---

## 10. File Pruning via Column Statistics

**Current status**: Statistics are written to `ducklake_file_column_stats` during
INSERT (all three writers support `register_column_stats`). Read-side pruning
is not implemented.

**What's needed**:
1. Read per-file column statistics from `ducklake_file_column_stats`
2. During scan planning, evaluate filter predicates against per-file min/max
3. Exclude files where filters provably eliminate all rows
4. Implement `statistics()` method on `DuckLakeTable` for DataFusion optimizer

**What's blocking**: Nothing — the data is already stored.

**Estimated effort**: Medium

**Suggested approach**: Implement file pruning in `DuckLakeTable::scan()` by:
1. Loading per-file column stats alongside file metadata
2. For each filter predicate, check if the file's min/max range overlaps
3. Exclude non-overlapping files from the scan plan

---

## 11. Compaction Functions

**Current status**: Not started

**What's needed**:
1. `ducklake_merge_adjacent_files(catalog, table)` — merges small files to target size
2. `ducklake_rewrite_data_files(catalog, table)` — rewrites files with high delete ratios
3. Configurable thresholds: target_file_size, min_file_size, max_file_size, delete_threshold
4. Sort-preserving merge (read sort settings, apply ORDER BY during rewrite)

**What's blocking**: Nothing fundamental. Requires combining existing read + write infrastructure.

**Estimated effort**: Large

**Suggested approach**: Implement as table functions that:
1. Query file metadata to identify candidates
2. Read candidate files via existing ParquetExec
3. Write merged output via existing DuckLakeTableWriter
4. Update metadata (end old files, register new files) atomically

---

## 12. NOT NULL Constraint Enforcement

**Current status**: NOT NULL metadata is read from `ducklake_column.nulls_allowed`
and surfaced in Arrow schema. Enforcement during writes is not implemented.

**What's needed**:
1. During INSERT, validate that non-nullable columns have no null values
2. During `ALTER TABLE SET NOT NULL`, verify existing data has no nulls (using column stats)
3. Return descriptive errors on constraint violation

**What's blocking**: Nothing.

**Estimated effort**: Small

**Suggested approach**: Add a validation step in `DuckLakeInsertExec` that checks
each batch for null values in non-nullable columns before writing.

---

## 13. Snapshot Refresh After DDL

**Current status**: Bug documented in `edge-case-findings.md` — after DDL operations
(CREATE TABLE, CREATE SCHEMA, ALTER TABLE), the catalog's pinned snapshot_id is stale.

**What's needed**: After any write operation that creates a new snapshot, update the
catalog's snapshot_id to the new value.

**What's blocking**: `DuckLakeCatalog.snapshot_id` is currently immutable. Needs to
be changed to `AtomicI64` or similar.

**Estimated effort**: Small

**Suggested approach**: Change `snapshot_id: i64` to `snapshot_id: AtomicI64` in
`DuckLakeCatalog`. After each write operation, store the new snapshot_id.

---

## 14. Multi-Backend Testing

**Current status**: Postgres and MySQL writers/providers exist but are not tested
in CI (require running database instances).

**What's needed**:
1. Docker-based CI setup for Postgres and MySQL testing
2. Integration tests that exercise all writer/provider methods against real databases
3. Cross-backend roundtrip tests (write with backend A, read with backend B)

**What's blocking**: CI infrastructure decision — Docker Compose in CI,
or testcontainers-rs for programmatic container management.

**Estimated effort**: Medium

**Suggested approach**: Use `testcontainers-rs` crate to programmatically start
Postgres and MySQL containers in integration tests. Gate behind a feature flag
(e.g., `test-docker`) so they don't run on every commit.

---

## Summary Table

| # | Gap | Status | Effort | Priority |
|---|-----|--------|--------|----------|
| 1.1 | Postgres/MySQL writer stubs (4 methods) | Not started | Small | High |
| 1.2 | Postgres/MySQL provider gaps (8 methods) | Not started | Medium | Medium |
| 1.3 | `list_all_columns` column filter bug | **Fixed** | — | — |
| 2 | Write-side partitioning | Not started | Large | Medium |
| 3 | Data inlining + flush | Not started | Large | Low |
| 4 | Complex type evolution | Partial | Large | Medium |
| 5 | Encrypted writes | Not started | Large | Low |
| 6 | MERGE INTO | Not started | Large | Low |
| 7 | Time travel syntax | Not started | Medium | Medium |
| 8 | SQLite catalog interop testing | Partial | Small | High |
| 9 | SLT failures (173 remaining) | In progress | Large | High |
| 10 | File pruning via column stats | Not started | Medium | High |
| 11 | Compaction functions | Not started | Large | Low |
| 12 | NOT NULL enforcement | Not started | Small | Medium |
| 13 | Snapshot refresh after DDL | Not started | Small | High |
| 14 | Multi-backend testing (Docker) | Not started | Medium | Medium |
