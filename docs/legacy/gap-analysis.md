# DataFusion-DuckLake Gap Analysis

This document provides a comprehensive analysis of the current `datafusion-ducklake` implementation,
identifies gaps relative to the DuckLake C++ reference implementation and DataFusion's contracts,
and recommends implementation priorities.

---

## Part 1: Current Implementation Inventory

### 1.1 Module Structure

| Module | File | Feature Gate | Purpose |
|--------|------|-------------|---------|
| `catalog` | `src/catalog.rs` | none | `DuckLakeCatalog` implementing `CatalogProvider` |
| `schema` | `src/schema.rs` | none | `DuckLakeSchema` implementing `SchemaProvider` |
| `table` | `src/table.rs` | none | `DuckLakeTable` implementing `TableProvider` |
| `metadata_provider` | `src/metadata_provider.rs` | none | `MetadataProvider` trait + SQL queries |
| `metadata_provider_duckdb` | `src/metadata_provider_duckdb.rs` | `metadata-duckdb` | DuckDB-backed metadata reader |
| `types` | `src/types.rs` | none | Type mapping (DuckLake <-> Arrow) |
| `delete_filter` | `src/delete_filter.rs` | none | `DeleteFilterExec` for MOR deletes |
| `column_rename` | `src/column_rename.rs` | none | `ColumnRenameExec` for renamed columns |
| `path_resolver` | `src/path_resolver.rs` | none | URL parsing and hierarchical path resolution |
| `error` | `src/error.rs` | none | `DuckLakeError` enum |
| `information_schema` | `src/information_schema.rs` | none | Virtual tables for catalog introspection |
| `table_functions` | `src/table_functions.rs` | none | UDTFs (snapshots, table_info, list_files, etc.) |
| `table_changes` | `src/table_changes.rs` | none | CDC inserts TableProvider |
| `table_deletions` | `src/table_deletions.rs` | none | CDC deletes TableProvider |
| `insert_exec` | `src/insert_exec.rs` | `write` | `DuckLakeInsertExec` execution plan |
| `table_writer` | `src/table_writer.rs` | `write` | Parquet file writer with field_ids |
| `metadata_writer` | `src/metadata_writer.rs` | `write` | `MetadataWriter` trait |
| `metadata_writer_sqlite` | `src/metadata_writer_sqlite.rs` | `write-sqlite` | SQLite-backed metadata writer |
| `encryption` | `src/encryption.rs` | `encryption` | `DuckLakeEncryptionFactory` (decrypt-only) |

### 1.2 Catalog Layer

**`DuckLakeCatalog`** (`src/catalog.rs`)
- Implements `CatalogProvider` trait
- Binds to a snapshot_id at creation time
- Dynamic lookup: `schema()` calls `get_schema_by_name()` per invocation
- `schema_names()` calls `list_schemas()` per invocation
- Exposes `information_schema` as a special virtual schema
- `with_writer()` enables write support (feature-gated)
- Does NOT implement `register_schema()` (CREATE SCHEMA)
- Does NOT implement `deregister_schema()` (DROP SCHEMA)

**`DuckLakeSchema`** (`src/schema.rs`)
- Implements `SchemaProvider` trait
- Dynamic lookup: `table()` calls `get_table_by_name()` per invocation
- `table_names()` calls `list_tables()` per invocation
- `table_exist()` calls `table_exists()` per invocation
- `register_table()` implemented under `write` feature (for CTAS)
- `validate_table_name()` prevents path traversal attacks
- Does NOT implement `deregister_table()` (DROP TABLE)
- Does NOT support views

**`DuckLakeTable`** (`src/table.rs`)
- Implements `TableProvider` trait
- Caches schema and file list at creation time
- `scan()` separates files into with-deletes and without-deletes groups
- Files without deletes grouped into single `ParquetExec`
- Files with deletes get individual `ParquetExec` wrapped in `DeleteFilterExec`
- Multiple plans combined with `UnionExec`
- `supports_filters_pushdown()` returns `Inexact` for all filters
- `insert_into()` implemented under `write` feature
- Does NOT implement `update()` method
- Does NOT implement `delete()` method (future DataFusion extension)
- No partition-based file pruning using column statistics

### 1.3 Metadata Provider Layer

**`MetadataProvider` trait** (`src/metadata_provider.rs`)
- 14 methods covering reads:
  - `list_schemas()`, `list_tables()`, `list_columns()`
  - `get_schema_by_name()`, `get_table_by_name()`, `table_exists()`
  - `get_table_files_for_select()` (data files + delete files)
  - `get_snapshot_id()`, `list_snapshots()`, `get_data_path()`
  - `get_data_file_changes()`, `get_delete_file_changes()`
  - `get_table_files_by_snapshot()`, `get_table_total_deletions()`
- SQL constants for all catalog queries
- Key structs: `SchemaMetadata`, `TableMetadata`, `DuckLakeTableColumn`, `DuckLakeFileData`, `DuckLakeTableFile`, `DataFileChange`, `DeleteFileChange`
- Comment: `todo: support select with file pruning` (line 477)

**`DuckdbMetadataProvider`** (`src/metadata_provider_duckdb.rs`, feature `metadata-duckdb`)
- Uses a single DuckDB connection protected by `Mutex`
- Implements all 14 MetadataProvider methods
- Thread-safe: connection is shared across queries

**`MetadataWriter` trait** (`src/metadata_writer.rs`, feature `write`)
- Methods: `create_snapshot()`, `get_or_create_schema()`, `get_or_create_table()`, `set_columns()`, `register_data_file()`, `end_table_files()`, `get_data_path()`, `set_data_path()`, `initialize_schema()`, `begin_write_transaction()`
- `WriteMode` enum: Replace, Append
- Key structs: `ColumnDef`, `DataFileInfo`, `WriteSetupResult`, `WriteResult`

**`SqliteMetadataWriter`** (`src/metadata_writer_sqlite.rs`, feature `write-sqlite`)
- Full SQLite implementation using `sqlx`
- Contains DDL for all catalog tables
- Transactional writes with schema evolution validation for appends

### 1.4 Type System

**`types.rs`**
- `ducklake_to_arrow_type()`: Converts DuckLake type strings to Arrow `DataType`
- `arrow_to_ducklake_type()`: Reverse mapping for writes
- Supports: integers, floats, booleans, strings, dates, timestamps, decimals, geometry (Binary/WKB)
- `build_arrow_schema()`: Constructs Arrow schemas from column metadata
- `extract_parquet_field_ids()`: Reads Parquet field_ids for column rename detection
- `build_read_schema_with_field_id_mapping()`: Maps Parquet columns to current DuckLake names
- LIST, STRUCT, MAP types return `UnsupportedType` errors

### 1.5 Execution Plans

| Plan | File | Purpose |
|------|------|---------|
| `DeleteFilterExec` | `src/delete_filter.rs` | Wraps ParquetExec to filter deleted rows by position |
| `ColumnRenameExec` | `src/column_rename.rs` | Renames Parquet columns to current DuckLake names |
| `DuckLakeInsertExec` | `src/insert_exec.rs` | Collects batches and writes Parquet files |
| `AppendCDCColumnsExec` | `src/table_changes.rs` | Wraps ParquetExec adding snapshot_id + change_type CDC columns |
| `DeletedRowsExec` | `src/table_deletions.rs` | 3-phase state machine for extracting deleted rows |

### 1.6 Write Path

- `DuckLakeInsertExec` (`src/insert_exec.rs`): Single-partition, collects all batches in memory before writing
- `DuckLakeTableWriter` (`src/table_writer.rs`): Writes Parquet with field_ids, calculates footer size
- `TableWriteSession`: Manages schema validation, Parquet writing, object store interaction
- `begin_write()` / `write_table()` / `append_table()`: Core write operations
- Supports `WriteMode::Replace` (truncate + insert) and `WriteMode::Append`

### 1.7 Encryption

- `DuckLakeEncryptionFactory` (`src/encryption.rs`, feature `encryption`)
- Implements DataFusion's `EncryptionFactory` trait
- Supports base64, hex, and raw key decoding
- **Decrypt-only**: does not write encrypted files
- Notes: DuckDB encryption is non-PME-compliant

### 1.8 Test Coverage

| Test File | Coverage Area |
|-----------|--------------|
| `tests/delete_filter_tests.rs` | End-to-end delete filtering |
| `tests/concurrent_tests.rs` | Thread-safety, concurrent reads |
| `tests/concurrent_write_tests.rs` | Concurrent write operations |
| `tests/write_tests.rs` | Basic write operations |
| `tests/sql_write_tests.rs` | SQL-based write tests |
| `tests/encryption_tests.rs` | Encrypted Parquet reading |
| `tests/information_schema_test.rs` | Information schema virtual tables |
| `tests/renamed_columns_tests.rs` | Column rename via field_id mapping |
| `tests/table_changes_tests.rs` | CDC table changes |
| `tests/table_tests.rs` | Basic table reading |
| `tests/object_store_integration_test.rs` | S3/MinIO integration |
| `tests/sqlite_metadata_provider_test.rs` | SQLite metadata provider |
| `tests/postgres_metadata_provider_test.rs` | PostgreSQL metadata provider |
| `tests/mysql_metadata_provider_test.rs` | MySQL metadata provider |
| `tests/hybrid_asyncdb.rs` | Hybrid async database tests |
| `tests/sqllogictest_runner.rs` | sqllogictest framework |
| `tests/common/mod.rs` | Test data generation helpers |

---

## Part 2: Gap Analysis

### Gap 1: DELETE Support (Write Delete Files)

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_delete.cpp`):
- `DuckLakeDelete` is a physical operator that collects row IDs from a scan (filename, file_index, file_row_number)
- Sorts and deduplicates delete positions per file
- Writes Parquet delete files with schema `(file_path: VARCHAR, pos: BIGINT)`
- Supports snapshot-embedded deletes when existing delete files are present (adds `_ducklake_internal_snapshot_id: BIGINT` column)
- Handles fully-deleted files by dropping the data file entirely
- Handles inlined data deletions (pushed to metadata manager, not written as files)
- Supports inlined file deletions (small deletes stored in metadata instead of files)
- Transaction-local delete tracking with merge on commit
- `PlanDelete()` connects the scan source to the delete operator, wiring up the `DuckLakeDeleteMap`

**Current DataFusion implementation**:
- `DeleteFilterExec` handles READ-side filtering of deleted rows (MOR pattern) -- this works
- No write-side delete file generation
- `DuckLakeTable` does not implement any `delete()` method
- DataFusion does not currently have a standard `TableProvider::delete()` method in its trait

**Gap details**:
- No physical operator for writing delete files
- No API to generate delete files from a set of row positions
- No snapshot-embedded delete file support
- No transaction-local delete accumulation
- No fully-deleted file detection/dropping
- No inlined data or inlined file deletion paths

**Affected sqllogictest areas**: Any test using `DELETE FROM` statements

**Recommendation**: Implement as a custom execution plan similar to `DuckLakeInsertExec`. Requires extending `MetadataWriter` with `register_delete_file()`. This is the highest-priority write gap since DELETE is foundational for UPDATE and MERGE.

---

### Gap 2: UPDATE Support

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_update.cpp`):
- UPDATE is implemented as DELETE + INSERT (copy-on-write pattern)
- `DuckLakeUpdate` orchestrates three child operators: copy (writes new data), delete (writes delete files for old rows), insert (registers new data files)
- Duplicate row detection using `(file_index, row_number)` pairs with `seen_rows` set
- Expression executor evaluates SET expressions
- Handles type casting for unsupported Parquet types
- Partition expression re-evaluation for updated rows
- `BindUpdateConstraints()` ensures all columns are projected (not just updated ones)
- Does NOT support RETURNING clause
- Does NOT support SET DEFAULT

**Current DataFusion implementation**:
- `DuckLakeTable` does not implement `update()`
- No equivalent of the delete+insert orchestration pattern
- Write infrastructure (InsertExec, TableWriter) exists but would need adaptation

**Gap details**:
- No update execution plan
- No delete+insert orchestration
- No duplicate row detection
- No partition re-evaluation for updated rows
- Requires Gap 1 (DELETE support) to be implemented first

**Affected sqllogictest areas**: Any test using `UPDATE` statements

**Recommendation**: Implement after DELETE support. Compose existing InsertExec with a new DeleteExec. Medium-high priority as many real workloads use UPDATE.

---

### Gap 3: DROP TABLE / DROP SCHEMA

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_schema_entry.cpp`):
- `DropEntry()` removes tables, views, and macros from the catalog
- Tables are dropped by recording the table_id in `dropped_tables` set
- Schema drop (`TryDropSchema()`) supports CASCADE (drops all contained entries)
- All drops are transaction-local until commit
- On commit, metadata manager records the drop in `ducklake_snapshot`

**Current DataFusion implementation**:
- `DuckLakeSchema::deregister_table()` returns `Err(not implemented)` (DataFusion trait default)
- `DuckLakeCatalog` does not implement schema deregistration
- `MetadataWriter` has no `drop_table()` or `drop_schema()` methods

**Gap details**:
- No `deregister_table()` implementation
- No DROP SCHEMA support
- No CASCADE behavior
- No metadata recording of drops

**Affected sqllogictest areas**: Tests with `DROP TABLE`, `DROP SCHEMA`, cleanup sequences

**Recommendation**: Medium priority. Implement `deregister_table()` on `DuckLakeSchema` and add `drop_table()` to `MetadataWriter`. Schema drop can follow.

---

### Gap 4: CREATE/DROP VIEW

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_view_entry.cpp`, `ducklake_schema_entry.cpp`):
- Views are stored in the catalog with `view_id`, `view_uuid`, and `query_sql`
- `DuckLakeViewEntry` extends `ViewCatalogEntry`
- Supports ALTER VIEW (RENAME_VIEW, SET_COMMENT)
- Views are parsed lazily (query string stored, parsed on demand)
- Create/drop tracked in transaction with commit to metadata

**Current DataFusion implementation**:
- No view support anywhere in the codebase
- `DuckLakeSchema` only handles tables, not views
- `MetadataProvider` has no view-related methods
- No `ducklake_view` table queries

**Gap details**:
- No view metadata reading
- No view creation/registration
- No view querying (DataFusion supports views natively if registered)
- No ALTER VIEW support

**Affected sqllogictest areas**: Tests with `CREATE VIEW`, `SELECT * FROM view_name`

**Recommendation**: Medium priority. Read-side is straightforward: query the `ducklake_view` table and register views with DataFusion's `SessionContext`. Write-side (CREATE VIEW) requires MetadataWriter extension.

---

### Gap 5: ALTER TABLE Operations

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_table_entry.cpp`):
- Comprehensive ALTER TABLE support including:
  - `RENAME_TABLE`, `RENAME_COLUMN`, `ADD_COLUMN`, `REMOVE_COLUMN`
  - `ALTER_COLUMN_TYPE` with type promotion rules (widening only: tinyint->smallint->int->bigint, float->double, timestamp->timestamptz)
  - `SET_PARTITIONED_BY` with partition transforms (IDENTITY, YEAR, MONTH, DAY, HOUR)
  - `SET_SORTED_BY` with sort expressions
  - `SET_NOT_NULL` / `DROP_NOT_NULL` with stats validation
  - `SET_DEFAULT` for column defaults
  - `SET_COMMENT` / `SET_COLUMN_COMMENT`
  - `ADD_FIELD` / `REMOVE_FIELD` / `RENAME_FIELD` for nested struct fields
- Each alter creates a new `DuckLakeTableEntry` with the change recorded in `LocalChange`
- Schema version tracking (new version on structural changes)

**Current DataFusion implementation**:
- No ALTER TABLE support
- Column rename detection exists for READ (via field_id mapping in `column_rename.rs` and `types.rs`)
- No schema evolution during writes beyond basic append validation

**Gap details**:
- No ALTER TABLE execution
- No schema version creation in metadata
- No type promotion validation
- No partition/sort key management
- No NOT NULL constraint support
- No nested type evolution

**Affected sqllogictest areas**: Tests with `ALTER TABLE` statements

**Recommendation**: Lower priority for initial release. ADD_COLUMN and RENAME_COLUMN are most impactful. Type promotion can be deferred.

---

### Gap 6: Column Statistics

**Status**: NOT IMPLEMENTED (read-side; partially implemented on write-side)

**Reference behavior** (`ducklake_stats.cpp`, `ducklake_scan.cpp`):
- `DuckLakeColumnStats` tracks per-column min, max, null_count, num_values, column_size_bytes, contains_nan
- `DuckLakeTableStats` aggregates column stats across all files via `MergeStats()`
- Stats are stored as JSON in the catalog (`ducklake_column_stats` table)
- `DuckLakeStatistics()` function provides stats to the query planner
- Supports numeric stats, string stats, variant stats
- Stats used for:
  - Query optimization (cardinality estimation)
  - NOT NULL constraint validation (checking if column has nulls)
  - `GetPartitionStats()` for exact COUNT(*) optimization
- Special handling: float/double stats only used if no NaN values

**Current DataFusion implementation**:
- `TableMetadata` struct has a `record_count` field
- `DuckLakeTable` does not implement `statistics()` method on `TableProvider`
- Write path (`table_writer.rs`) does not collect or store column statistics
- No JSON stats parsing
- No `GetPartitionStats()` equivalent

**Gap details**:
- No `statistics()` implementation on `DuckLakeTable`
- No column-level stats reading from metadata
- No stats merging across files
- No exact COUNT(*) optimization via partition stats
- Write path does not generate stats for new files
- No stats JSON format parsing

**Affected sqllogictest areas**: Query optimization quality, COUNT(*) performance

**Recommendation**: Medium priority. Reading stats improves query planning significantly. The exact COUNT(*) optimization (partition stats) is valuable for dashboards and monitoring queries.

---

### Gap 7: File Pruning (Filter Pushdown to File List)

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_multi_file_list.cpp`):
- `DynamicFilterPushdown()` prunes files based on column statistics before scanning
- Supports filter types: constant comparisons, conjunctions, null filters, IN filters
- Uses per-file column stats (min/max) to skip entire files
- Operates on `DuckLakeMultiFileList` level, reducing the file list before ParquetExec
- Partition-based file pruning using partition values

**Current DataFusion implementation**:
- `supports_filters_pushdown()` returns `Inexact` for all filters -- this pushes filters INTO Parquet scanning (row group/page pruning) but does NOT prune the file list
- Comment in `metadata_provider.rs`: `todo: support select with file pruning`
- No per-file statistics stored or used for file selection
- No partition-based file pruning

**Gap details**:
- No file-level pruning based on column statistics
- No partition-based file selection
- All files are always scanned (with Parquet-level pruning only)
- `MetadataProvider` does not return per-file stats
- No `DynamicFilterPushdown` equivalent

**Affected sqllogictest areas**: Performance tests, large table queries

**Recommendation**: Medium-high priority for production workloads. Requires extending `MetadataProvider` to return per-file column stats, then filtering the file list before constructing scan plans.

---

### Gap 8: Time Travel / AT SNAPSHOT Queries

**Status**: PARTIALLY IMPLEMENTED

**Reference behavior** (`ducklake_scan.cpp`, `ducklake_transaction.cpp`):
- Tables can be queried at historical snapshots via `AT` clause
- `DuckLakeFunctionInfo` stores the target snapshot
- File lists are filtered to only include files valid at the target snapshot
- `GetPartitionStats()` detects time travel queries and falls back to full scan for correctness
- Catalog can be attached at a historical snapshot (`CatalogSnapshot()`)

**Current DataFusion implementation**:
- Catalog binds to a snapshot_id at creation time via `get_snapshot_id()`
- Tables are read at this fixed snapshot (snapshot filtering in SQL queries)
- CDC functions (`ducklake_table_changes`, `ducklake_table_deletions`) support snapshot ranges
- No per-query snapshot override (AT clause)
- No way to change the snapshot after catalog creation

**Gap details**:
- No `AT SNAPSHOT <id>` query syntax support
- Snapshot is fixed at catalog creation time
- No ability to query historical data on a per-query basis
- CDC functions provide some historical access but not full time travel

**Affected sqllogictest areas**: Tests using `AT SNAPSHOT` syntax, time travel queries

**Recommendation**: Lower priority initially. The fixed-snapshot approach works for most read-only use cases. Per-query time travel requires DataFusion SQL extension or custom table function.

---

### Gap 9: MERGE INTO

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_merge_into.cpp`):
- Full MERGE INTO support with WHEN MATCHED UPDATE, WHEN MATCHED DELETE, WHEN NOT MATCHED INSERT actions
- `DuckLakeMergeInsert` operator handles the insert path of merge
- Reuses existing DELETE and UPDATE operators for matched actions
- Single UPDATE/DELETE action limitation (one per MERGE)
- Does NOT support RETURNING clause
- `PhysicalMergeInto` orchestrates all merge actions

**Current DataFusion implementation**:
- No MERGE INTO support
- DataFusion does not have built-in MERGE INTO support in its SQL parser/planner

**Gap details**:
- No MERGE INTO operator
- No SQL syntax support
- Requires DELETE and UPDATE support first (Gaps 1 and 2)

**Affected sqllogictest areas**: Tests using `MERGE INTO` statements

**Recommendation**: Low priority. Requires Gaps 1 and 2 first. MERGE INTO is important for ETL workloads but can be approximated with separate DELETE + INSERT.

---

### Gap 10: Data Inlining

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_inline_data.hpp`, `ducklake_inlined_data_reader.cpp`, `ducklake_insert.cpp`):
- Small tables can store data directly in the metadata catalog instead of Parquet files
- `DuckLakeInlineData` operator routes data to either inline storage or Parquet files based on a configurable row limit
- Inlined data stored in `ducklake_inlined_data` table as a DuckDB table
- Separate inlined data reader for scanning
- Inlined deletions tracked in metadata (not as delete files)
- Flush mechanism converts inlined data to Parquet when threshold exceeded

**Current DataFusion implementation**:
- No data inlining support
- All data goes through Parquet files
- No `ducklake_inlined_data` table interaction

**Gap details**:
- No inline data reading from metadata catalog
- No inline data writing
- No threshold-based routing (inline vs. Parquet)
- No inline deletion tracking
- No flush mechanism

**Affected sqllogictest areas**: Tests with very small tables, inlined data configuration

**Recommendation**: Low priority. Most production workloads use Parquet files. Inlining is an optimization for very small tables.

---

### Gap 11: Compaction (merge_adjacent_files, rewrite_data_files)

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_compaction_functions.cpp`):
- Two compaction types: `MERGE_ADJACENT_TABLES` and `REWRITE_DELETES`
- `ducklake_merge_adjacent_files()`: Merges small files within partition groups to target file size
- `ducklake_rewrite_data_files()`: Rewrites files with high delete ratios
- `DuckLakeCompactor` generates compaction commands as logical plans
- Supports sort-preserving compaction (reads sort settings and applies ORDER BY)
- Configurable: `target_file_size`, `max_compacted_files`, `min_file_size`, `max_file_size`, `delete_threshold`, `auto_compact`
- Row ID and snapshot ID tracking for compacted files
- Can operate on a single table or entire catalog

**Current DataFusion implementation**:
- No compaction functions
- No table functions for merge_adjacent_files or rewrite_data_files
- No file size tracking or targeting

**Gap details**:
- No `ducklake_merge_adjacent_files()` function
- No `ducklake_rewrite_data_files()` function
- No compaction plan generation
- No sort-preserving merge
- No configurable thresholds

**Affected sqllogictest areas**: Tests using compaction functions

**Recommendation**: Low-medium priority. Important for long-running production catalogs but not needed for initial correctness.

---

### Gap 12: Complex Type Support (LIST, STRUCT, MAP)

**Status**: NOT IMPLEMENTED (returns errors)

**Reference behavior** (`ducklake_table_entry.cpp`):
- Full support for nested types: LIST, STRUCT, MAP
- `ADD_FIELD` / `REMOVE_FIELD` / `RENAME_FIELD` for struct field evolution
- Nested type evolution preserves field_ids
- Stats support for nested types (via extra_stats)

**Current DataFusion implementation**:
- `ducklake_to_arrow_type()` returns `UnsupportedType` error for LIST, STRUCT, MAP
- `arrow_to_ducklake_type()` returns error for complex types
- No nested field_id tracking
- Arrow and DataFusion fully support these types

**Gap details**:
- Type parsing for LIST, STRUCT, MAP not implemented
- No nested field_id mapping
- No schema evolution for nested fields
- Arrow types `List`, `Struct`, `Map` fully supported by DataFusion

**Affected sqllogictest areas**: Tests with complex typed columns

**Recommendation**: Medium priority. Many real-world datasets use LIST and STRUCT. The type mapping is straightforward; the challenge is recursive field_id handling.

---

### Gap 13: Partition Support (Write Path)

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_insert.cpp`, `ducklake_table_entry.cpp`):
- Tables can have partition columns with transforms: IDENTITY, YEAR, MONTH, DAY, HOUR
- `SET_PARTITIONED_BY` ALTER TABLE command
- Insert path computes partition values for each row and routes to per-partition files
- Partition values stored in `ducklake_file_partition` table
- Hive-style directory layout for partitioned data
- File pruning uses partition values

**Current DataFusion implementation**:
- No partition configuration on tables
- No partition transform computation during writes
- No per-partition file routing
- No `ducklake_file_partition` table interaction
- Read path does not leverage partition metadata for pruning

**Gap details**:
- No partition-aware writes
- No partition transform functions
- No hive-style directory layout generation
- No partition metadata storage
- No partition pruning on reads

**Affected sqllogictest areas**: Tests with partitioned tables

**Recommendation**: Medium priority for production workloads. Required for efficient large-table management.

---

### Gap 14: Transaction Conflict Detection

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_transaction.cpp`, `ducklake_transaction_manager.cpp`):
- Transaction tracking with `DuckLakeTransaction` managing all local changes
- `FlushChanges()` commits all transaction-local changes atomically
- Conflict detection during commit: validates that concurrent transactions haven't modified the same data
- `ChangesMade()` tracks schema changes, data changes, dropped files, name maps
- Rollback support: cleans up written files on transaction failure
- `DuckLakeTransactionManager` manages transaction lifecycle

**Current DataFusion implementation**:
- `MetadataWriter::begin_write_transaction()` starts a transaction
- `SqliteMetadataWriter` has basic transactional writes
- No conflict detection between concurrent writers
- No rollback of written Parquet files on failure
- No comprehensive transaction-local change tracking

**Gap details**:
- No concurrent writer conflict detection
- No file cleanup on transaction failure
- No snapshot conflict validation
- No transaction-local change accumulation across multiple operations

**Affected sqllogictest areas**: Concurrent write tests, multi-statement transactions

**Recommendation**: Medium-high priority for write correctness. Without conflict detection, concurrent writes can corrupt the catalog.

---

### Gap 15: Column Statistics on Write

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_insert.cpp`, `ducklake_stats.cpp`):
- During INSERT, column statistics are extracted from Parquet file metadata
- Stats include: min, max, null_count, num_values, column_size_bytes, contains_nan
- Stats stored per-file, per-column in the catalog
- Stats parsed from Parquet footer metadata after file write
- Stats used for file pruning and query optimization

**Current DataFusion implementation**:
- `DuckLakeTableWriter` writes Parquet files but does not extract/store column statistics
- `DataFileInfo` in `MetadataWriter` does not include per-column stats
- `register_data_file()` only records file path, size, row count, footer size, encryption key

**Gap details**:
- No column statistics extraction from written Parquet files
- No stats storage in metadata catalog
- No `DataFileStats` structure in MetadataWriter
- Parquet footer contains this information but it's not extracted

**Affected sqllogictest areas**: Statistics-dependent optimizations, NOT NULL validation

**Recommendation**: Medium priority. Pair with Gap 6 (reading stats) for full stats lifecycle.

---

### Gap 16: Macros (Scalar and Table)

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_schema_entry.cpp`, `ducklake_transaction.cpp`):
- DuckLake stores scalar macros and table macros in the catalog
- `CreateFunction()` handles both `MACRO_ENTRY` and `TABLE_MACRO_ENTRY` types
- Macros stored with schema association
- Drop support for macros
- Transaction-local macro tracking

**Current DataFusion implementation**:
- No macro support
- No `ducklake_macro` table interaction
- DataFusion supports UDFs but not catalog-stored macros

**Gap details**:
- No macro reading from catalog
- No macro registration with DataFusion
- No macro creation/drop support
- Different semantics: DuckLake macros vs DataFusion UDFs

**Affected sqllogictest areas**: Tests using `CREATE MACRO` or macro invocations

**Recommendation**: Low priority. Macros are a DuckDB-specific feature that doesn't map cleanly to DataFusion.

---

### Gap 17: Virtual Columns

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_table_entry.cpp`):
- Tables expose virtual columns: `filename`, `file_row_number`, `file_index`, `rowid`, `snapshot_id`
- `GetVirtualColumns()` returns the map of available virtual columns
- Virtual columns are used by DELETE and UPDATE to identify rows
- `snapshot_id` column tracks when each row was inserted (for time travel in compacted files)

**Current DataFusion implementation**:
- No virtual column exposure
- `DuckLakeTable` does not report virtual columns in its schema
- Delete/update operations (not implemented) would need these for row identification

**Gap details**:
- No virtual column registration with DataFusion
- No filename/row_number/file_index column generation during scans
- No snapshot_id embedding in compacted files
- Required for implementing DELETE and UPDATE

**Affected sqllogictest areas**: Tests querying virtual columns, DELETE/UPDATE internals

**Recommendation**: Medium priority (required for DELETE/UPDATE implementation).

---

### Gap 18: NOT NULL Constraints

**Status**: NOT IMPLEMENTED

**Reference behavior** (`ducklake_table_entry.cpp`, `ducklake_insert.cpp`):
- NOT NULL is the only constraint type supported by DuckLake
- `SET_NOT_NULL` validates that existing data has no nulls (using column stats)
- `DROP_NOT_NULL` removes the constraint
- INSERT enforces NOT NULL during data writing
- Constraint information stored in `ducklake_column` table

**Current DataFusion implementation**:
- Column metadata (`DuckLakeTableColumn`) has a `not_null` field but it is not used
- No constraint enforcement during writes
- No NOT NULL validation on ALTER TABLE

**Gap details**:
- NOT NULL metadata is read but not enforced
- No constraint checking during INSERT
- No ALTER TABLE SET NOT NULL / DROP NOT NULL

**Affected sqllogictest areas**: Tests with NOT NULL constraints, constraint violation tests

**Recommendation**: Medium priority for write correctness. Reading the flag is already done; enforcement during writes is the gap.

---

### Gap 19: Encrypted Write Support

**Status**: NOT IMPLEMENTED (read-only encryption exists)

**Reference behavior** (`ducklake_insert.cpp`, `ducklake_delete.cpp`):
- Encryption key generation: `GenerateEncryptionKey()` creates keys for new files
- Both data files and delete files can be encrypted
- Key stored per-file in metadata
- `encryption_config` passed to Parquet writer

**Current DataFusion implementation**:
- `DuckLakeEncryptionFactory` supports READING encrypted files
- No encryption key generation for new files
- No encrypted write in `DuckLakeTableWriter`

**Gap details**:
- No key generation during writes
- No `encryption_config` passed to Parquet writer
- No encrypted delete file writing
- Read-side encryption works

**Affected sqllogictest areas**: Tests writing to encrypted tables

**Recommendation**: Low-medium priority. Only needed when encryption is enabled.

---

### Gap 20: MetadataProvider Implementations (PostgreSQL, MySQL Write)

**Status**: PARTIAL (read-only for Postgres/MySQL; write for SQLite only)

**Reference behavior**: DuckLake supports PostgreSQL, MySQL, SQLite, and DuckDB as metadata backends for both reads and writes.

**Current DataFusion implementation**:
- `DuckdbMetadataProvider`: Read-only (feature `metadata-duckdb`)
- PostgreSQL and MySQL metadata providers: Read-only via sqlx (features `metadata-postgres`, `metadata-mysql`)
- `SqliteMetadataWriter`: Write support (feature `write-sqlite`)
- No DuckDB MetadataWriter
- No PostgreSQL MetadataWriter
- No MySQL MetadataWriter

**Gap details**:
- No DuckDB-backed MetadataWriter
- No PostgreSQL-backed MetadataWriter
- No MySQL-backed MetadataWriter
- SQLite write support exists and can serve as template

**Affected sqllogictest areas**: Tests targeting specific backends

**Recommendation**: Medium priority for DuckDB writer (most common development backend). PostgreSQL and MySQL writers can follow the SQLite pattern.

---

## Part 3: Priority Recommendations

### Tier 1: Correctness-Critical (Required for Write Correctness)

These gaps must be addressed for write operations to be correct and safe.

| Priority | Gap | Rationale |
|----------|-----|-----------|
| P0 | **Gap 14: Transaction Conflict Detection** | Without this, concurrent writes can corrupt the catalog. Required for any production write workload. |
| P0 | **Gap 18: NOT NULL Constraints** | Data integrity requires enforcing constraints during writes. The metadata is already read. |

### Tier 2: Foundational Write Operations

These enable the most-requested DML operations and unblock many sqllogictest tests.

| Priority | Gap | Rationale |
|----------|-----|-----------|
| P1 | **Gap 1: DELETE Support** | Foundation for UPDATE and MERGE. Enables most DML tests. |
| P1 | **Gap 2: UPDATE Support** | Second most common DML after INSERT. Depends on Gap 1. |
| P1 | **Gap 3: DROP TABLE / DROP SCHEMA** | Required for test cleanup and schema management. |
| P1 | **Gap 17: Virtual Columns** | Required by DELETE/UPDATE to identify rows. |

### Tier 3: Query Correctness and Performance

These improve query results and performance for read workloads.

| Priority | Gap | Rationale |
|----------|-----|-----------|
| P2 | **Gap 6: Column Statistics (Read)** | Improves query planning; enables COUNT(*) optimization. |
| P2 | **Gap 7: File Pruning** | Major performance improvement for large tables. |
| P2 | **Gap 12: Complex Type Support** | Many real-world datasets use LIST/STRUCT. |
| P2 | **Gap 15: Column Statistics (Write)** | Needed for file pruning and read-side stats. |
| P2 | **Gap 4: CREATE/DROP VIEW** | Views are common in SQL workloads. |

### Tier 4: Production Features

These are needed for production-quality deployments but not for initial correctness.

| Priority | Gap | Rationale |
|----------|-----|-----------|
| P3 | **Gap 13: Partition Support (Write)** | Required for large table management. |
| P3 | **Gap 20: MetadataProvider Implementations** | DuckDB writer most needed; others follow same pattern. |
| P3 | **Gap 5: ALTER TABLE** | ADD_COLUMN and RENAME_COLUMN most impactful. |
| P3 | **Gap 11: Compaction** | Important for long-running catalogs. |
| P3 | **Gap 19: Encrypted Write Support** | Only needed with encryption enabled. |

### Tier 5: Advanced Features

These can be deferred to later releases.

| Priority | Gap | Rationale |
|----------|-----|-----------|
| P4 | **Gap 8: Time Travel** | Fixed-snapshot approach works for most use cases. |
| P4 | **Gap 9: MERGE INTO** | Can be approximated with DELETE + INSERT. |
| P4 | **Gap 10: Data Inlining** | Optimization for very small tables. |
| P4 | **Gap 16: Macros** | DuckDB-specific; doesn't map cleanly to DataFusion. |

### Recommended Implementation Order

```
Phase 1: Write Foundation
  1. Gap 17 (Virtual Columns) -- enables row identification
  2. Gap 1  (DELETE)          -- write delete files
  3. Gap 2  (UPDATE)          -- delete + insert pattern
  4. Gap 14 (Transaction Conflict Detection) -- write safety
  5. Gap 18 (NOT NULL Constraints) -- data integrity
  6. Gap 3  (DROP TABLE/SCHEMA) -- cleanup operations

Phase 2: Read Quality
  7. Gap 15 (Column Stats Write) -- produce stats during writes
  8. Gap 6  (Column Stats Read)  -- consume stats for optimization
  9. Gap 7  (File Pruning)       -- skip files using stats
  10. Gap 12 (Complex Types)     -- LIST, STRUCT, MAP support

Phase 3: SQL Completeness
  11. Gap 4  (Views)            -- CREATE/DROP VIEW
  12. Gap 5  (ALTER TABLE)      -- schema evolution
  13. Gap 13 (Partitions Write) -- partition-aware writes
  14. Gap 20 (Metadata Writers) -- additional backends

Phase 4: Advanced
  15. Gap 11 (Compaction)       -- file maintenance
  16. Gap 8  (Time Travel)      -- per-query snapshots
  17. Gap 19 (Encrypted Writes) -- write encryption
  18. Gap 9  (MERGE INTO)       -- combined DML
  19. Gap 10 (Data Inlining)    -- small table optimization
  20. Gap 16 (Macros)           -- catalog-stored macros
```

### sqllogictest Enablement Impact

| Phase | Estimated Tests Enabled | Key Test Categories |
|-------|------------------------|---------------------|
| Phase 1 | ~40-50% | DELETE, UPDATE, DROP, basic DML workflows |
| Phase 2 | ~60-70% | Complex types, performance, optimization |
| Phase 3 | ~85-90% | Views, ALTER TABLE, partitioned tables |
| Phase 4 | ~95-100% | Compaction, time travel, edge cases |
