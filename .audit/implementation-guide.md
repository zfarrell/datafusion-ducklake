# DataFusion-DuckLake Implementation Guide

A practical guide for developers working on this codebase. All information verified against
the actual source code on the `ducklake-features/integration` branch.

## Build & Test

### Dependencies
```toml
# Cargo.toml key versions
datafusion = "51.0"
arrow = "57"
parquet = "57"
duckdb = "1.4.1"       # optional, feature-gated
sqlx = "0.8"            # optional, feature-gated
sqllogictest = "0.23"   # dev-dependency
```

### Feature Flags (from Cargo.toml lines 53-72)

| Feature | Dependencies | Purpose |
|---------|-------------|---------|
| `default` | `metadata-duckdb` | Default build includes DuckDB provider |
| `metadata-duckdb` | `dep:duckdb` | DuckDB metadata provider |
| `metadata-postgres` | `dep:sqlx` + postgres | PostgreSQL metadata provider |
| `metadata-mysql` | `dep:sqlx` + mysql | MySQL metadata provider |
| `metadata-sqlite` | `dep:sqlx` + sqlite | SQLite metadata provider |
| `encryption` | `parquet/encryption`, `datafusion/parquet_encryption`, `base64`, `hex` | Parquet Modular Encryption |
| `write` | `dep:uuid` | Write support (INSERT, DELETE, UPDATE, MERGE) |
| `write-sqlite` | `write` + `metadata-sqlite` | Write + SQLite backend |
| `write-mysql` | `write` + `metadata-mysql` | Write + MySQL backend |
| `write-postgres` | `write` + `metadata-postgres` | Write + PostgreSQL backend |
| `skip-tests-with-docker` | (none) | Skip tests requiring Docker containers |

### Build Commands
```bash
# Default build (DuckDB metadata provider only)
cargo build

# All features
cargo build --all-features

# Write support with SQLite backend
cargo build --features write-sqlite,metadata-duckdb

# Encryption support
cargo build --features encryption

# Full read+write+encryption
cargo build --features write-sqlite,metadata-duckdb,encryption
```

### Test Commands
```bash
# Default tests (DuckDB provider tests)
cargo test

# All tests (needs Docker for Postgres/MySQL)
cargo test --all-features

# All tests skipping Docker
cargo test --all-features --features skip-tests-with-docker

# Specific test categories
cargo test delete_filter        # Delete file tests
cargo test concurrent           # Concurrency tests
cargo test cross_engine          # Cross-engine interop tests
cargo test table_changes         # CDC/time travel tests
cargo test encryption            # Encryption tests
cargo test sqllogictest          # SLT runner tests
cargo test alter_table           # ALTER TABLE tests
cargo test merge                 # MERGE INTO tests
cargo test compaction            # Compaction function tests
cargo test virtual_column        # Virtual column tests
cargo test view                  # View tests

# Tests requiring Docker (Postgres, MySQL, MinIO)
cargo test postgres              # PostgreSQL provider/writer
cargo test mysql                 # MySQL provider/writer
cargo test object_store          # S3/MinIO integration
```

### Running the Example
```bash
cargo run --example basic_query -- <catalog.ducklake> "SELECT * FROM main.table_name"
```

## Architecture Overview

### Module Structure (from `src/lib.rs`)

```
src/
├── lib.rs                      # Module declarations, feature gates, re-exports
├── catalog.rs                  # DuckLakeCatalog (CatalogProvider)
├── schema.rs                   # DuckLakeSchema (SchemaProvider)
├── table.rs                    # DuckLakeTable (TableProvider) — largest file
├── metadata_provider.rs        # MetadataProvider trait + SQL constants + data types
├── metadata_provider_duckdb.rs # DuckDB implementation
├── metadata_provider_sqlite.rs # SQLite implementation
├── metadata_provider_postgres.rs # PostgreSQL implementation
├── metadata_provider_mysql.rs  # MySQL implementation
├── metadata_writer.rs          # MetadataWriter trait + AlterTableOp + ColumnDef + types
├── metadata_writer_sqlite.rs   # SQLite writer implementation
├── metadata_writer_postgres.rs # PostgreSQL writer implementation
├── metadata_writer_mysql.rs    # MySQL writer implementation
├── metadata_writer_validation.rs # Shared validation for writer implementations
├── path_resolver.rs            # URL parsing and hierarchical path resolution
├── types.rs                    # DuckLake ↔ Arrow type mapping
├── error.rs                    # DuckLakeError enum
├── encryption.rs               # Parquet Modular Encryption support
├── delete_filter.rs            # DeleteFilterExec (MOR delete filtering)
├── column_rename.rs            # ColumnRenameExec (handles column renames across versions)
├── virtual_column_exec.rs      # VirtualColumnExec (filename, rowid, snapshot_id, etc.)
├── information_schema.rs       # InformationSchemaProvider
├── table_functions.rs          # UDTF registration (ducklake_snapshots, table_changes, etc.)
├── table_changes.rs            # ducklake_table_changes() implementation
├── table_insertions.rs         # ducklake_table_insertions() implementation
├── table_deletions.rs          # ducklake_table_deletions() implementation
├── compaction_functions.rs     # DuckDB-delegated compaction functions
├── query_planner.rs            # DuckLakeQueryPlanner (DELETE/UPDATE routing) [write]
├── insert_exec.rs              # DuckLakeInsertExec [write]
├── delete_exec.rs              # DuckLakeDeleteExec [write]
├── update_exec.rs              # DuckLakeUpdateExec [write]
├── merge_exec.rs               # DuckLakeMergeExec [write]
└── table_writer.rs             # DuckLakeTableWriter (Parquet file writing) [write]
```

### Request Flow: Query Execution

```
User: SELECT * FROM ducklake.main.users WHERE id > 5
  │
  ├─1─► DataFusion resolves "ducklake" → DuckLakeCatalog
  │     catalog.schema("main")
  │     └── Queries MetadataProvider.get_schema_by_name("main", snapshot_id)
  │         Returns SchemaMetadata { schema_id, path, path_is_relative }
  │
  ├─2─► DuckLakeSchema created with resolved schema_path
  │     schema.table("users")
  │     └── Queries MetadataProvider.get_table_by_name(schema_id, "users", snapshot_id)
  │         Returns TableMetadata { table_id, path, path_is_relative }
  │
  ├─3─► DuckLakeTable created — caches structure + files at creation time
  │     ├── MetadataProvider.get_table_structure(table_id)  → columns
  │     ├── MetadataProvider.get_table_files_for_select(table_id, snapshot_id) → files
  │     ├── MetadataProvider.get_partition_columns(table_id, snapshot_id) → partitions
  │     └── MetadataProvider.get_inlined_data(table_id, snapshot_id) → inline data
  │
  ├─4─► table.scan() builds execution plan:
  │     ├── Files WITHOUT delete files → grouped into single ParquetExec
  │     ├── Files WITH delete files → individual ParquetExec + DeleteFilterExec each
  │     ├── Virtual columns requested? → wrap in VirtualColumnExec
  │     ├── Column renames detected? → wrap in ColumnRenameExec
  │     ├── Encryption keys present? → attach EncryptionFactory to ParquetSource
  │     └── Multiple plans → combine with UnionExec
  │
  └─5─► DataFusion executes the physical plan
        ├── Parquet files scanned with filter pushdown (row group pruning, page filtering)
        ├── DeleteFilterExec removes deleted rows by position
        └── Results streamed to user
```

### Request Flow: Write Operation (INSERT)

```
User: INSERT INTO ducklake.main.users VALUES (6, 'Eve', 'eve@example.com')
  │
  ├─1─► DuckLakeQueryPlanner (or default planner) creates DuckLakeInsertExec
  │     Reads input batches → collects all rows
  │
  ├─2─► DuckLakeTableWriter.write_table_batches()
  │     ├── MetadataWriter.begin_write_transaction() — creates snapshot, gets table/schema IDs
  │     ├── Writes Parquet file to object store (UUID-named file)
  │     └── MetadataWriter.register_data_file() — registers file + column stats in catalog
  │
  └─3─► Returns row count
```

### Metadata Provider Layer

The `MetadataProvider` trait (`src/metadata_provider.rs:600-763`) defines **read-only** catalog access:

**Core methods** (required):
- `get_current_snapshot()` → `i64`
- `get_data_path()` → `String`
- `list_snapshots()` → `Vec<SnapshotMetadata>`
- `list_schemas(snapshot_id)` → `Vec<SchemaMetadata>`
- `list_tables(schema_id, snapshot_id)` → `Vec<TableMetadata>`
- `get_table_structure(table_id)` → `Vec<DuckLakeTableColumn>`
- `get_table_files_for_select(table_id, snapshot_id)` → `Vec<DuckLakeTableFile>`
- `get_schema_by_name(name, snapshot_id)` → `Option<SchemaMetadata>`
- `get_table_by_name(schema_id, name, snapshot_id)` → `Option<TableMetadata>`
- `table_exists(schema_id, name, snapshot_id)` → `bool`
- `list_all_tables(snapshot_id)` → `Vec<TableWithSchema>` (bulk)
- `list_all_columns(snapshot_id)` → `Vec<ColumnWithTable>` (bulk)
- `list_all_files(snapshot_id)` → `Vec<FileWithTable>` (bulk)
- `get_data_files_added_between_snapshots(table_id, start, end)` → `Vec<DataFileChange>`
- `get_delete_files_added_between_snapshots(table_id, start, end)` → `Vec<DeleteFileChange>`

**Optional methods** (have default implementations):
- `get_file_column_stats()` → file-level column statistics
- `get_table_row_count()` → exact row count optimization
- `get_partition_columns()` → partition key columns
- `get_file_partition_values()` → per-file partition values
- `list_views()` / `get_view_by_name()` / `view_exists()` → view support
- `get_inlined_data()` → data stored directly in catalog DB

### Metadata Writer Layer

The `MetadataWriter` trait (`src/metadata_writer.rs:319-515`) defines **write** catalog access:

**Core methods** (required):
- `create_snapshot()` → `i64`
- `get_or_create_schema(name, path, snapshot_id)` → `(schema_id, was_created)`
- `get_or_create_table(schema_id, name, path, snapshot_id)` → `(table_id, was_created)`
- `set_columns(table_id, columns, snapshot_id)` → `Vec<column_ids>`
- `register_data_file(table_id, snapshot_id, file)` → `data_file_id`
- `end_table_files(table_id, snapshot_id)` → count
- `get_data_path()` / `set_data_path(path)` — catalog data path
- `initialize_schema()` — create DuckLake catalog tables
- `begin_write_transaction(schema, table, columns, mode)` → `WriteSetupResult`
- `register_delete_file(table_id, snapshot_id, file)` → `delete_file_id`
- `drop_table(table_id)` / `drop_schema(schema_id)` → `snapshot_id`
- `list_active_table_ids(schema_id)` → `Vec<i64>`
- `alter_table(table_id, op)` → `snapshot_id`
- `get_active_columns(table_id)` → `Vec<(name, type, nullable)>`
- `rename_table(table_id, new_name)` → `snapshot_id`
- `set_table_comment()` / `set_column_comment()` → `snapshot_id`
- `create_view()` / `drop_view()` / `rename_view()` → view management

**AlterTableOp variants** (`metadata_writer.rs:21-54`):
- `AddColumn { column }` — add nullable column
- `DropColumn { column_name }` — soft delete via end_snapshot
- `RenameColumn { old_name, new_name }`
- `AlterColumnType(AlterColumnTypeOp)` — widening only (see `is_type_promotion_allowed`)
- `SetColumnDefault` / `DropColumnDefault`
- `SetNotNull` / `DropNotNull`
- `SetPartitionedBy { partition_columns }` — define partition columns with transform expressions

## How Cross-Engine Tests Work

### Test Infrastructure (`tests/cross_engine_tests.rs`)

Cross-engine tests verify that data written by one engine can be read by another.
Requires features: `write-sqlite`, `metadata-duckdb`, `metadata-sqlite`.

**Three patterns**:
1. **df_write_df_read**: DataFusion writes (SQLite catalog) → DataFusion reads
2. **df_write_duckdb_read**: DataFusion writes → DuckDB reads and verifies
3. **duckdb_write_df_read**: DuckDB writes → DataFusion reads and verifies

**Setup helper** (`setup_ducklake_catalog()`):
```rust
// Creates temp dir, initializes SQLite catalog, sets data_path
let env = setup_ducklake_catalog().await;
// env.catalog_db_path → path to SQLite DB
// env.data_path → path to Parquet file directory
```

**Opening in DataFusion** (two variants):
```rust
// Via DuckDB provider (read-only)
fn open_in_datafusion_duckdb(catalog_path: &Path) -> SessionContext

// Via SQLite provider (read-only or writable)
async fn open_in_datafusion_sqlite(catalog_path: &Path) -> SessionContext
async fn open_in_datafusion_writable(catalog_path: &Path) -> SessionContext
```

**Opening in DuckDB** (`DuckDbConn` wrapper):
```rust
DuckDbConn::open(catalog_path)                    // SQLite-backed catalog
DuckDbConn::open_native(catalog_path)             // Native DuckDB catalog
DuckDbConn::open_with_data_path(catalog, data)    // With explicit DATA_PATH
```

**Test lifecycle**:
1. `setup_ducklake_catalog()` — creates fresh SQLite catalog
2. Writer engine creates tables and inserts data
3. Reader engine opens the same catalog file
4. Assertions verify data matches

### Additional cross-engine test files
- `cross_engine_dml_tests.rs` — DELETE, UPDATE operations
- `cross_engine_insert_tests.rs` — INSERT variations
- `cross_engine_alter_tests.rs` — ALTER TABLE operations
- `cross_engine_ddl_tests.rs` — CREATE/DROP TABLE/SCHEMA
- `cross_engine_partition_tests.rs` — Partitioned tables
- `cross_engine_inline_tests.rs` — Inlined data
- `cross_engine_feature_tests.rs` — Feature-specific tests

## How the SLT Runner Works

### Hybrid DuckDB+DataFusion Adapter

The SLT (SQL Logic Test) runner uses DuckDB's own test suite to validate DataFusion's read path.

**Files**:
- `tests/sqllogictest_runner.rs` — Test runner + preprocessor
- `tests/hybrid_asyncdb.rs` — `HybridDuckLakeDB` adapter

**`HybridDuckLakeDB`** (`hybrid_asyncdb.rs`):
- Implements `sqllogictest::AsyncDB` trait
- Maintains two connections:
  - `duckdb_conn: Arc<Mutex<Connection>>` — DuckDB for writes
  - `datafusion_ctx: Arc<Mutex<SessionContext>>` — DataFusion for reads
- **Routing logic** (`is_write_statement()`):
  - WRITE → DuckDB: CREATE, INSERT, UPDATE, DELETE, DROP, ALTER, MERGE, USE, SHOW, BEGIN/COMMIT/ROLLBACK
  - READ → DataFusion: SELECT, WITH...SELECT
- After each write (outside transactions): `refresh_catalog()` creates a fresh
  `DuckdbMetadataProvider` and `DuckLakeCatalog`
- **Table reference rewriting**: `ducklake.table` → `ducklake.main.table` (3-part names)
- **Virtual column stripping**: Removes extension-specific columns (filename, file_row_number,
  file_index, rowid, snapshot_id) from SELECT * results unless explicitly referenced

**Preprocessor** (`preprocess_test_file()` in `sqllogictest_runner.rs`):
Transforms DuckDB test files to work with the hybrid adapter:

| Transformation | Why |
|---------------|-----|
| Remove `require`, `test-env` directives | Not supported by sqllogictest crate |
| Skip `ATTACH`/`DETACH` statements | Handled in Rust |
| Skip `EXPLAIN`, `DESCRIBE` | Different output format |
| Skip `COMMENT ON`, `PRAGMA` | DuckDB-specific |
| Expand `loop`/`foreach`/`endloop` | Not supported by sqllogictest crate |
| `statement maybe` → `statement ok` | Conservative: assume success |
| Skip multi-connection (`con1`, `con2`) | Not supported |
| Strip `statement error` expected text | Accept any error message |
| Rewrite `ORDER BY ALL` → remove + add `rowsort` | Not in DataFusion's SQL dialect |
| Skip DuckDB-specific functions | `GLOB()`, `TYPEOF()`, `STATS()`, etc. |
| Skip named parameter syntax (`=>`) | DuckDB-specific |

**Test discovery**: `run_all_sqllogictests()` recursively finds `.test` files under
`tests/sqllogictests/sql/`, runs each through the preprocessor, then executes with the
hybrid adapter.

## How to Add New Features

### Adding a New Table Function

1. **Define the function struct** in `src/table_functions.rs`:
```rust
#[derive(Debug)]
pub struct DucklakeMyFunction {
    provider: Arc<dyn MetadataProvider>,
}

impl DucklakeMyFunction {
    pub fn new(provider: Arc<dyn MetadataProvider>) -> Self {
        Self { provider }
    }
}

impl TableFunctionImpl for DucklakeMyFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        // Parse args from exprs (see parse_change_function_args for pattern)
        // Query metadata provider
        // Build RecordBatch with results
        // Return Arc::new(MemTable::try_new(schema, vec![vec![batch]])?)
    }
}
```

2. **Register in `register_ducklake_functions()`** (line 535-571):
```rust
ctx.register_udtf(
    "ducklake_my_function",
    Arc::new(DucklakeMyFunction::new(provider.clone())),
);
```

3. If the function needs a streaming TableProvider (not MemTable), create a new module
   like `src/table_changes.rs` with a struct implementing `TableProvider` + custom `ExecutionPlan`.

### Adding a New ALTER TABLE Operation

1. **Add variant to `AlterTableOp`** in `src/metadata_writer.rs`:
```rust
pub enum AlterTableOp {
    // ... existing variants ...
    MyNewOp { param: String },
}
```

2. **Implement in all 3 writer backends**:
   - `src/metadata_writer_sqlite.rs` → `fn alter_table()`
   - `src/metadata_writer_postgres.rs` → `fn alter_table()`
   - `src/metadata_writer_mysql.rs` → `fn alter_table()`

   Each `alter_table()` method pattern-matches on `AlterTableOp` variants. Example from SQLite:
   ```rust
   fn alter_table(&self, table_id: i64, op: &AlterTableOp) -> Result<i64> {
       match op {
           AlterTableOp::MyNewOp { param } => {
               // 1. Create snapshot
               // 2. Apply the operation (modify ducklake_column rows, etc.)
               // 3. Return snapshot_id
           }
           // ... other variants
       }
   }
   ```

3. **Add test** in `tests/alter_table_tests.rs` or `tests/cross_engine_alter_tests.rs`.

### Adding a New MetadataWriter Method

1. **Add to trait** in `src/metadata_writer.rs`:
```rust
pub trait MetadataWriter: Send + Sync + std::fmt::Debug {
    // ... existing methods ...

    /// Describe what this does.
    fn my_new_method(&self, param: i64) -> Result<Something>;
}
```

2. **Implement in all 3 backends**:
   - `src/metadata_writer_sqlite.rs`
   - `src/metadata_writer_postgres.rs`
   - `src/metadata_writer_mysql.rs`

   Each backend uses its own SQL dialect:
   - SQLite/Postgres: `sqlx::query!()` or raw SQL via `sqlx::query()`
   - Note: SQLite uses `block_on()` wrapper from `metadata_provider.rs` to bridge
     async sqlx to sync trait methods

3. **Optionally add default implementation** if backward compatibility is needed:
```rust
fn my_new_method(&self, _param: i64) -> Result<Something> {
    // Sensible default for backward compatibility
    Ok(default_value)
}
```

### Adding a New Virtual Column

Virtual columns are defined in `src/virtual_column_exec.rs`.

1. **Add constant** (alongside existing ones at lines 25-33):
```rust
pub const VIRTUAL_COL_MY_COL: &str = "my_column";
```

2. **Add field to `VirtualColumnSet`** (line 49-56):
```rust
pub struct VirtualColumnSet {
    pub filename: bool,
    pub file_row_number: bool,
    pub rowid: bool,
    pub snapshot_id: bool,
    pub file_index: bool,
    pub my_column: bool,  // NEW
}
```

3. **Update `VirtualColumnSet::any()`** and detection logic.

4. **Update `VirtualColumnExec`'s stream** to generate the column data.

5. **Update `DuckLakeTable::scan()`** in `src/table.rs` to:
   - Detect the virtual column in projection
   - Populate `VirtualColumnFileInfo` with the data
   - Set the flag in `VirtualColumnSet`

6. **Update SLT preprocessor** (`tests/sqllogictest_runner.rs`):
   Add the new column to `EXTENSION_VIRTUAL_COLS` or `DUCKLAKE_VIRTUAL_COLS` in
   `hybrid_asyncdb.rs` depending on whether DuckDB also has this column.

### Adding a New MetadataProvider Method

1. **Add to trait** in `src/metadata_provider.rs`:
```rust
pub trait MetadataProvider: Send + Sync + std::fmt::Debug {
    /// Description of method.
    fn my_method(&self, table_id: i64, snapshot_id: i64) -> Result<Vec<Something>> {
        Ok(Vec::new())  // Default: empty
    }
}
```

2. **Add SQL constant** if needed (same file, top section):
```rust
pub const SQL_MY_QUERY: &str = "SELECT ... FROM ducklake_... WHERE ...";
```

3. **Implement in all backends**:
   - `metadata_provider_duckdb.rs` — uses `duckdb::Connection`
   - `metadata_provider_sqlite.rs` — uses `sqlx::SqlitePool`
   - `metadata_provider_postgres.rs` — uses `sqlx::PgPool`
   - `metadata_provider_mysql.rs` — uses `sqlx::MySqlPool`

## Key Patterns and Conventions

### Error Handling
- `DuckLakeError` in `src/error.rs` — crate-level error type
- Convert to `DataFusionError::External(Box::new(e))` at DataFusion integration boundaries
- Use `map_err(|e| DataFusionError::External(Box::new(e)))?` pattern

### Thread Safety
- `MetadataProvider`: `Send + Sync + Debug` (trait bound)
- `MetadataWriter`: `Send + Sync + Debug` (trait bound)
- DuckDB provider: single connection behind `Mutex`
- SQLite/Postgres/MySQL providers: connection pool via sqlx

### Path Resolution
All path operations go through `src/path_resolver.rs`:
- `parse_object_store_url(url)` → `(ObjectStoreUrl, key_path)`
- `resolve_path(base, relative, is_relative)` → resolved path
- `PathResolver` struct for hierarchical resolution
- Supports: `file:///`, `s3://bucket/`, local paths

### Snapshot Consistency
- Catalog-level: `AtomicI64` for snapshot_id (updated on writes)
- Schema/Table-level: snapshot_id received from parent and stored as `i64`
- All metadata queries include snapshot filtering

### Feature Gating Pattern
```rust
// In lib.rs: conditionally include module
#[cfg(feature = "write")]
pub mod insert_exec;

// In struct: conditional field
#[cfg(feature = "write")]
writer: Option<Arc<dyn MetadataWriter>>,

// In impl: conditional method
#[cfg(feature = "write")]
pub fn with_writer(mut self, writer: Arc<dyn MetadataWriter>) -> Self { ... }
```

### Test Data Generation
Tests use DuckDB to generate catalogs on-the-fly (`tests/common/mod.rs`):
```rust
let conn = duckdb::Connection::open_in_memory()?;
conn.execute("LOAD ducklake;", [])?;
conn.execute("ATTACH 'ducklake:path.ducklake' AS test_catalog;", [])?;
conn.execute("CREATE TABLE test_catalog.users (...);", [])?;
conn.execute("INSERT INTO test_catalog.users VALUES ...;", [])?;
```
No external scripts or pre-generated test data required.

### Parquet File Writing (Write Feature)
`DuckLakeTableWriter` in `src/table_writer.rs`:
- Writes Parquet files with UUID-based filenames
- Calculates footer sizes for read optimization
- Registers files + column stats in catalog metadata
- Best-effort cleanup of orphaned files on failure
- Builds schemas with Parquet field IDs for DuckDB compatibility

### Write-Side Partitioning
`DuckLakeInsertExec` in `src/insert_exec.rs` supports partitioned writes:
- Evaluates partition transform expressions (IDENTITY, YEAR, MONTH, DAY, HOUR) per row
- Routes rows to per-partition Parquet files using Hive-style directory layout
  (e.g., `year=2024/month=01/<uuid>.parquet`)
- Records partition values in `ducklake_file_partition_value` metadata table
- `AlterTableOp::SetPartitionedBy` defines partition columns with transform expressions
- Partition columns are defined via `PartitionColumnDef { column_name, transform }` structs

### Write-Side Data Inlining
The MetadataWriter trait includes 6 inlining lifecycle methods:
- `create_inline_table()` — create per-table inline storage
- `insert_inline_data()` — store rows directly in catalog metadata
- `get_inline_row_count()` — check if threshold exceeded
- `flush_inline_to_parquet()` — convert inlined data to Parquet file
- `drop_inline_table()` — clean up inline storage
- `get_inlining_threshold()` — get configured row limit

SQLite backend implementation (`src/metadata_writer_sqlite.rs`):
- Uses per-table SQLite tables for inline storage
- Auto-flush to Parquet when `DATA_INLINING_ROW_LIMIT` exceeded
- `ducklake_flush_inlined_data()` table function for manual flush

### Write Atomicity Model (updated 2026-03-02)

Partitioned writes use a **single-transaction, all-or-nothing** model:

1. **Single `begin_write_transaction`**: Called once for the entire write, not per partition. Returns a shared `WriteSetupResult` with snapshot_id and column_ids used by all partitions.
2. **Upload-then-commit**: Files are uploaded to object storage first via `upload()`, returning `UploadedFile` structs. Only after ALL uploads succeed are files registered in the catalog via `commit_uploaded_files()`.
3. **Deferred Replace-mode**: In Replace mode, old files are ended AFTER upload succeeds (not before). This prevents an empty table if upload fails.
4. **Cleanup on failure**: If any upload or commit fails, `cleanup_uploaded_files()` removes already-uploaded files from object storage.
5. **Deterministic ordering**: Partitions use `BTreeMap` (not `HashMap`) for deterministic iteration order.
6. **Atomic partition values**: Partition values are registered inside the write transaction, not after `session.finish()`.

Key types: `UploadedFile`, `begin_write_partitioned_with_setup()`, `commit_uploaded_files()`, `cleanup_uploaded_files()`.

### SQL Identifier Quoting

All SQL identifiers (column names, table names) interpolated into dynamic SQL **must** be sanitized using `quote_identifier()`. This prevents SQL injection when column names contain special characters (quotes, semicolons, etc.).

```rust
// WRONG: SQL injection risk
format!("CREATE TABLE inline_{} ({})", table_id, col_name);

// CORRECT: Use quote_identifier()
format!("CREATE TABLE inline_{} ({})", table_id, quote_identifier(col_name));
```

This applies to all metadata writer backends (SQLite, PostgreSQL, MySQL), especially in the inlining code paths.

### Partition Value URL-Encoding

Hive partition values are URL-encoded per Hive convention before being used in directory paths. Values containing `/`, `..`, `=`, or other special characters are encoded to prevent malformed or directory-traversal paths.

```rust
// Partition value "hello/world" becomes "hello%2Fworld" in the path:
// year=2024/region=hello%2Fworld/data.parquet
```

### Pre-commit Hook
A pre-commit hook is installed at `.githooks/pre-commit`:
- Automatically runs `cargo fmt` on staged Rust files before committing
- Configure with: `git config core.hooksPath .githooks`
- Ensures consistent formatting across the integration branch
