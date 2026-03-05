# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataFusion-DuckLake is a DataFusion extension providing full read and write access to DuckLake catalogs. DuckLake is an integrated data lake and catalog format that stores:
- **Metadata**: In SQL databases (DuckDB, SQLite, PostgreSQL, MySQL) as structured catalog tables
- **Data**: As Apache Parquet files on disk or object storage (S3, MinIO)

The extension integrates DuckLake with Apache DataFusion by implementing catalog, table provider, and query planner interfaces for SELECT, INSERT, DELETE, UPDATE, MERGE, and DDL operations.

## Commands

### Build and Test
```bash
# Build (default: DuckDB metadata provider, read-only)
cargo build

# Build with write support (SQLite backend)
cargo build --features write-sqlite

# Build all features
cargo build --all-features

# Run all tests
cargo test --features write-sqlite

# Run all tests including Docker-dependent (needs Postgres/MySQL running)
cargo test --all-features

# Skip Docker tests
cargo test --all-features --features skip-tests-with-docker

# Run specific test categories
cargo test delete_filter        # Delete file tests
cargo test concurrent           # Concurrency tests
cargo test cross_engine         # Cross-engine interop
cargo test alter_table          # ALTER TABLE tests
cargo test merge                # MERGE INTO tests

# Run the basic query example
cargo run --example basic_query -- <catalog.db> <sql>
```

## Feature Flags

| Feature | Purpose | Default |
|---------|---------|---------|
| `metadata-duckdb` | DuckDB catalog backend | Yes |
| `metadata-sqlite` | SQLite catalog backend (via sqlx) | |
| `metadata-postgres` | PostgreSQL catalog backend (via sqlx) | |
| `metadata-mysql` | MySQL catalog backend (via sqlx) | |
| `write` | Write support (INSERT/DELETE/UPDATE/MERGE) | |
| `write-sqlite` | Write + SQLite metadata | |
| `write-postgres` | Write + PostgreSQL metadata | |
| `write-mysql` | Write + MySQL metadata | |
| `encryption` | Parquet Modular Encryption (PME) reads | |
| `skip-tests-with-docker` | Skip Docker-dependent tests | |

## Architecture

### Module Overview

The codebase follows a layered architecture (~35k lines across 35 source files):

#### Catalog Integration (Read Path Foundation)
- **`catalog.rs`** — `DuckLakeCatalog` implements `CatalogProvider` with dynamic on-demand schema lookup
- **`schema.rs`** — `DuckLakeSchema` implements `SchemaProvider` with dynamic on-demand table lookup; handles DDL (CREATE TABLE, ALTER TABLE, DROP)
- **`table.rs`** — `DuckLakeTable` implements `TableProvider`; handles scanning, filter pushdown, virtual columns, and routes DML operations
- **`path_resolver.rs`** — Hierarchical path resolution (catalog -> schema -> table -> file) for S3, MinIO, and local filesystem

#### Metadata Providers (Feature-Gated)
- **`metadata_provider.rs`** — `MetadataProvider` trait, SQL constants, shared types
- **`metadata_provider_duckdb.rs`** — DuckDB backend (default)
- **`metadata_provider_sqlite.rs`** — SQLite via sqlx
- **`metadata_provider_postgres.rs`** — PostgreSQL via sqlx
- **`metadata_provider_mysql.rs`** — MySQL via sqlx

#### Read Path
- **`delete_filter.rs`** — `DeleteFilterExec` wraps Parquet scans for MOR (Merge-On-Read) row filtering
- **`column_rename.rs`** — `ColumnRenameExec` handles field_id-based column renames after DDL evolution
- **`virtual_column_exec.rs`** — Virtual columns: `filename`, `file_row_number`, `rowid`, `snapshot_id`, `file_index`
- **`types.rs`** — DuckLake-to-Arrow type mapping (lists, structs, maps, decimals)
- **`encryption.rs`** — Parquet Modular Encryption (PME) read support
- **`parse_values.rs`** — String-to-Arrow parsing for inlined data (lenient/strict modes)

#### Write Path (Feature-Gated: `write`)
- **`insert_exec.rs`** — `DuckLakeInsertExec` with partition routing and transforms
- **`delete_exec.rs`** — `DuckLakeDeleteExec` writes delete files (MOR pattern)
- **`update_exec.rs`** — `DuckLakeUpdateExec` (copy-on-write via delete + insert)
- **`merge_exec.rs`** — `DuckLakeMergeExec` (MATCHED/NOT MATCHED branches)
- **`table_writer.rs`** — `DuckLakeTableWriter` high-level write API with atomicity guarantees
- **`metadata_writer.rs`** — `MetadataWriter` trait for catalog metadata commits
- **`metadata_writer_sqlite.rs`** — SQLite implementation with SQLITE_BUSY retry logic
- **`metadata_writer_postgres.rs`** — PostgreSQL implementation
- **`metadata_writer_mysql.rs`** — MySQL implementation
- **`metadata_writer_validation.rs`** — DDL/DML validation helpers

#### Query Planning & Table Functions
- **`query_planner.rs`** — `DuckLakeQueryPlanner` routes DELETE/UPDATE/MERGE to table methods
- **`table_functions.rs`** — `ducklake_snapshots()`, `ducklake_table_changes()`, etc.
- **`table_changes.rs`** — CDC (Change Data Capture) implementation
- **`table_deletions.rs`** — Deletion tracking via delete files
- **`table_insertions.rs`** — Insertion tracking
- **`information_schema.rs`** — SQL-queryable catalog: snapshots, schemata, tables, columns, files
- **`compaction_functions.rs`** — Delegates to DuckDB: `merge_adjacent_files()`, `rewrite_data_files()`, `expire_snapshots()`

### Dynamic Metadata Lookup

The catalog uses a **pure dynamic lookup** approach with no caching at the catalog/schema level:

- **DuckLakeCatalog**: `schema_names()` and `schema()` query metadata on every call. `new()` is O(1).
- **DuckLakeSchema**: `table_names()`, `table()`, and `table_exist()` query metadata on every call. `new()` is O(1).
- **DuckLakeTable**: Caches table structure and file lists at creation time (necessary for query planning).

### Write Atomicity

Write operations follow a write-then-commit pattern:
1. Parquet file uploaded to object store
2. Metadata committed to catalog database
3. On failure: best-effort cleanup of uploaded file

### Key Implementation Patterns

- **MOR (Merge-On-Read)**: DELETE/UPDATE write delete files with `(file_path, pos)` rather than rewriting data. Delete files are joined with data files during reads.
- **Field IDs**: Parquet field IDs track columns across renames and DDL evolution. Delete files use sentinel values (`0x7FFFFFFE`, `0x7FFFFFFD`).
- **Partition Transforms**: Write-side partitioning with year/month/day/hour transforms (identity for simple partitioning).
- **SQLite Concurrency**: Retry-on-SQLITE_BUSY with exponential backoff + jitter (handles `SQLITE_BUSY_SNAPSHOT` in WAL mode).
- **Filter Pushdown**: Returns `Inexact` for all filters, allowing Parquet row group/page pruning. Filters reapplied after `DeleteFilterExec`.
- **Inlined Data**: Small datasets stored directly in catalog metadata; auto-flushed to Parquet files on threshold.
- **Snapshot Isolation**: Queries pinned to snapshot ID determined at catalog creation. Tables/schemas filtered by snapshot validity ranges.

### Path Resolution Hierarchy

DuckLake resolves paths hierarchically with relative and absolute path support:
- **data_path** (from `ducklake_metadata`): Root path for all data
- **schema.path**: Relative to `data_path` or absolute
- **table.path**: Relative to schema path or absolute
- **file.path**: Relative to table path or absolute

### Object Store Registration

Object stores must be registered with DataFusion's `RuntimeEnv` before querying:
- **Local filesystem**: Automatically available
- **S3/MinIO**: Register via `AmazonS3Builder` and `RuntimeEnv::register_object_store()`
- See `examples/basic_query.rs` for configuration examples

## Current Limitations

- Complex types (nested lists, structs, maps) have limited schema evolution support
- Parquet Modular Encryption writes not implemented (reads work)
- No `AT SNAPSHOT` SQL syntax for time travel (programmatic API works)
- No MERGE SQL syntax (programmatic API works)
- Streaming INSERT deferred (inlined data flush edge cases)
- No optional metadata caching layer (all lookups are dynamic)

## Testing

The project includes 787+ tests across 67 test files:
- **Cross-engine**: 72+ interoperability tests (DataFusion <-> DuckDB)
- **DML**: INSERT, DELETE, UPDATE, MERGE tests
- **DDL**: ALTER TABLE, CREATE SCHEMA, DROP tests
- **Features**: virtual columns, encryption, time travel, CDC, delete filtering
- **Concurrency**: thread-safety, concurrent writes, SQLite busy handling
- **Edge cases**: adversarial inputs, deep edge cases, conflict detection
- **SQLLogicTest**: 157/254 passing (61.8%)
- **Test data**: Generated in Rust (no external scripts), temporary directories for isolation

```bash
cargo test --features write-sqlite          # All tests (recommended)
cargo test cross_engine                     # Cross-engine interop
cargo test --all-features                   # Including Docker tests
cargo test --ignored                        # Performance benchmarks
```
