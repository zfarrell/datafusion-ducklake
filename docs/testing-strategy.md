# Testing Strategy for DataFusion DuckLake Extension

## Overview

This document defines the testing strategy for the DataFusion DuckLake extension. Testing is the primary quality gate for all feature work. The goal is near-100% compliance with DuckDB's DuckLake sqllogic tests that do not exercise DuckDB-specific behavior.

Testing follows a four-tier model. Each tier addresses a distinct category of correctness, and features must satisfy all applicable tiers before they are considered complete.

---

## Tier 1: DuckLake SQLLogic Compatibility Tests

### Purpose

Verify that the DataFusion extension produces the same query results as DuckDB + DuckLake for all portable test scenarios. This is the **primary acceptance criterion** for the extension.

### Infrastructure

The extension already has a hybrid test runner that executes DuckDB DuckLake tests against DataFusion's read path:

- **Test runner**: `tests/sqllogictest_runner.rs`
  - Auto-discovers all `.test` files under `tests/sqllogictests/sql/`
  - Preprocesses test files to remove DuckDB-specific directives
  - Reports pass/fail counts with per-test diagnostics

- **Hybrid adapter**: `tests/hybrid_asyncdb.rs`
  - Routes WRITE operations (CREATE, INSERT, UPDATE, DELETE, DROP, ALTER, CALL, USE, SHOW, BEGIN, COMMIT, ROLLBACK) to DuckDB
  - Routes READ operations (SELECT, WITH) to DataFusion
  - Refreshes the DataFusion catalog snapshot after each write
  - Rewrites 2-part table references (`ducklake.table`) to 3-part (`ducklake.main.table`)
  - Converts Arrow RecordBatches to sqllogictest string output

- **Test files**: 248 `.test` files currently copied to `tests/sqllogictests/sql/` from the DuckLake reference suite at `ducklake/test/sql/`

### Preprocessing

The preprocessor in `sqllogictest_runner.rs` (`preprocess_test_file()`) already handles:

| Construct | Action |
|-----------|--------|
| `require ducklake` / `require parquet` | Stripped |
| `test-env` directives | Stripped |
| `# name:` / `# description:` / `# group:` | Stripped |
| `ATTACH` / `DETACH` statements | Stripped (connection managed in Rust) |
| `EXPLAIN` statements and query blocks | Stripped (no consistent output format) |

### Porting Strategy

The portability survey (`docs/test-portability-survey.md`) categorizes all 342 test files into:

- **Category A (Directly Portable)**: ~20 files. Standard SQL requiring minimal or no changes.
- **Category B (Needs Adaptation)**: ~165 files. Relevant features with DuckDB-specific syntax to replace.
- **Category C (Not Portable)**: ~157 files. DuckDB-internal features with no DataFusion equivalent.

**Porting priority** (from the survey):

1. **Phase 1 -- Core Read Path**: `insert/insert_column_list.test`, `insert/insert_into_self.test`, `delete/basic_delete.test`, `delete/empty_delete.test`, `update/basic_update.test`, `update/test_update_expression.test`, `ducklake_basic.test`
2. **Phase 2 -- DDL Operations**: `alter/add_column.test`, `alter/drop_column.test`, `alter/rename_column.test`, `alter/rename_table.test`, `catalog/schema.test`, `catalog/drop_table.test`, `catalog/quoted_identifiers.test`
3. **Phase 3 -- Type Coverage**: `types/floats.test`, `types/timestamp.test`, `types/null_byte.test`
4. **Phase 4 -- Query Features**: `stats/filter_pushdown.test`, `stats/count_star_optimization_basic.test`, `view/ducklake_view.test`, `constraints/not_null.test`
5. **Phase 5 -- Advanced Operations**: `alter/mixed_alter.test`, `transaction/basic_transaction.test`, `delete/truncate_table.test`

### Adaptation Patterns

When porting Category B tests, apply these transformations:

| Pattern | DuckDB | DataFusion | Frequency |
|---------|--------|------------|-----------|
| Bare FROM | `FROM ducklake.test` | `SELECT * FROM ducklake.test` | Very common |
| INSERT FROM | `INSERT INTO t FROM range(100)` | `INSERT INTO t SELECT * FROM generate_series(0, 99)` | Common |
| USE catalog | `USE ducklake; SELECT * FROM t` | `SELECT * FROM ducklake.main.t` | ~110 files |
| CALL statements | `CALL ducklake_flush_inlined_data(...)` | Remove (write-side operation) | ~149 files |
| Time travel | `SELECT * FROM t AT (VERSION => 2)` | Remove query block | ~28 files |
| DuckDB catalog functions | `duckdb_tables()`, `duckdb_schemas()` | Remove or replace with `information_schema` | ~30 files |
| DuckLake functions | `ducklake_snapshots()`, `ducklake_table_info()` | Remove query block | ~20 files |
| Internal metadata | `__ducklake_metadata_*` | Remove query block | ~64 files |
| `stats()` | `stats('column')` | Remove verification query | ~12 files |
| `glob()` | `glob('path/*')` | Remove file system verification | ~19 files |
| `foreach`/`endloop` | Parameterized test loops | Manually unroll into separate test blocks | ~41 files |
| Multi-connection | `con1`/`con2` concurrent tests | Skip entire test file | ~15 files |
| DESCRIBE format | 6-column DuckDB format | May differ; update expected output | ~5 files |
| SHOW TABLES | `SHOW TABLES` | `SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'` | ~5 files |

### Preprocessor Enhancements Needed

The current preprocessor handles the basics. Additional preprocessing may be needed for high-volume patterns:

1. **Strip CALL statements**: Detect `statement ok` followed by `CALL ducklake_*` and skip both lines. This alone unlocks ~94 additional test files.
2. **Strip USE statements**: Detect `statement ok` followed by `USE ...` and skip both lines. The hybrid adapter already rewrites table references, so this may already work since `USE` is routed to DuckDB.
3. **Strip time travel queries**: Detect `query` blocks where the SQL contains `AT (VERSION` or `AT (TIMESTAMP` and skip the entire query block (directive + SQL + separator + results).

### Excluded Test Documentation

Every test file that is **not** ported must be documented with an exclusion reason. Maintain a file at `tests/sqllogictests/EXCLUDED_TESTS.md` with entries in this format:

```
## <directory>/<filename>.test
- **Reason**: <specific reason>
- **DuckDB constructs**: <list of unsupported constructs used>
- **Category**: C (Not Portable)
```

Valid exclusion reasons:
- Uses DuckDB-specific `CALL ducklake_*` maintenance functions with no portable equivalent
- Uses multi-connection concurrency (`con1`/`con2`) not supported by hybrid runner
- Tests DuckDB-internal metadata (`__ducklake_metadata_*` tables)
- Tests DuckDB-specific catalog functions (`duckdb_tables()`, `ducklake_snapshots()`)
- Tests DuckLake write-side features (compaction, data inlining, sorted tables, etc.)
- Tests time travel (`AT (VERSION => ...)`) not supported by DataFusion
- Requires DuckDB extensions (`require spatial`, `require icu`, `require httpfs`)
- Tests DuckDB-specific types (`VARIANT`, `JSON`, `GEOMETRY`) not supported by DataFusion

### Running Tier 1 Tests

```bash
# Run all sqllogic tests (auto-discovers .test files)
cargo test --features metadata-duckdb run_all_sqllogictests -- --nocapture

# Run and see pass/fail summary
cargo test --features metadata-duckdb run_all_sqllogictests -- --nocapture 2>&1 | tail -20
```

---

## Tier 2: DataFusion Contract Tests

### Purpose

Verify that the extension correctly implements DataFusion's `TableProvider`, `CatalogProvider`, and `SchemaProvider` trait contracts. These tests ensure the extension integrates cleanly with DataFusion's `SessionContext` and query engine.

### Test Location

Existing Rust integration tests in `tests/`:
- `tests/table_tests.rs` -- `DuckLakeTable` as `TableProvider`
- `tests/delete_filter_tests.rs` -- Delete filtering with `DeleteFilterExec`
- `tests/concurrent_tests.rs` -- Thread-safety of catalog/schema providers
- `tests/information_schema_test.rs` -- `information_schema` integration

### Test Categories

**CatalogProvider contract** (`src/catalog.rs`):
- `schema_names()` returns all schemas in the catalog
- `schema()` returns `Some(schema)` for existing schemas, `None` for non-existent
- Schemas are queryable immediately after creation (dynamic lookup)
- Catalog registration with `SessionContext::register_catalog()` works
- Multiple catalogs can coexist in a single session

**SchemaProvider contract** (`src/schema.rs`):
- `table_names()` returns all tables in the schema
- `table()` returns `Some(table)` for existing tables, `None` for non-existent
- `table_exist()` returns correct boolean
- Tables are queryable immediately after metadata changes (dynamic lookup)

**TableProvider contract** (`src/table.rs`):
- `schema()` returns correct Arrow schema with proper types
- `scan()` produces correct results for:
  - Full table scans (`SELECT *`)
  - Column projections (`SELECT a, c`)
  - Reordered projections (`SELECT c, a`)
  - Empty tables (zero data files)
  - Tables with delete files
  - Tables with multiple data files
- `supports_filters_pushdown()` returns `Inexact` for all filters
- Filter pushdown produces correct results (not just no errors)
- `table_type()` returns `TableType::Base`

**Type mapping** (`src/types.rs`):
- Integer types: TINYINT, SMALLINT, INTEGER, BIGINT mapped correctly
- Float types: FLOAT, DOUBLE mapped correctly
- String types: VARCHAR, TEXT mapped to Utf8
- Temporal types: DATE, TIMESTAMP, TIMESTAMPTZ mapped correctly
- Decimal types: DECIMAL(p,s) with correct precision and scale
- Boolean: BOOLEAN mapped correctly
- Binary: BLOB mapped correctly
- Geometry: mapped to Binary (WKB format)
- Complex types: LIST, STRUCT, MAP return descriptive errors
- SQL type aliases handled: "int" = Int32, "text" = Utf8, "bigint" = Int64, etc.

**Delete filtering** (`src/delete_filter.rs`):
- `DeleteFilterExec` correctly excludes deleted rows
- Handles multiple delete files per data file
- Handles COUNT(*) optimization (zero-column batches)
- Handles edge case: all rows in a batch deleted
- Handles edge case: no rows deleted (passthrough)

### Test Patterns

Tier 2 tests use DuckDB to set up test catalogs, then query through DataFusion:

```rust
// Setup: create DuckLake catalog via DuckDB
let temp_dir = TempDir::new()?;
let catalog_path = temp_dir.path().join("test.ducklake");
create_catalog_no_deletes(&catalog_path)?;

// Test: query through DataFusion
let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())?;
let catalog = Arc::new(DuckLakeCatalog::new(provider)?);
let ctx = SessionContext::new();
ctx.register_catalog("ducklake", catalog);

let df = ctx.sql("SELECT * FROM ducklake.main.users").await?;
let batches = df.collect().await?;
assert_eq!(total_rows(&batches), 4);
```

Test helper functions in `tests/common/mod.rs` provide reusable catalog creation:
- `create_catalog_no_deletes()` -- 4-row users table
- `create_catalog_with_deletes()` -- 5 rows, 2 deleted
- `create_catalog_with_updates()` -- 3 rows, 2 updated (MOR pattern)
- `create_catalog_filter_pushdown()` -- 5 rows, 1 deleted (filter ordering test)
- `create_catalog_empty_table()` -- Empty table (insert + delete all)
- `create_catalog_basic_test()` -- Two tables for general queries
- `create_catalog_complex_deletions()` -- Multi-round insert/delete scenario
- `create_catalog_multiple_snapshots()` -- Multi-snapshot table for change tracking

### Naming Convention

```
tests/<feature>_tests.rs       -- Integration test files
tests/common/mod.rs            -- Shared test helpers
```

Test function names: `test_<feature>_<scenario>`, for example:
- `test_empty_table_basic_scan`
- `test_deleted_rows_excluded`
- `test_count_with_deletes`

### Running Tier 2 Tests

```bash
# Run all Rust integration tests
cargo test --features metadata-duckdb

# Run specific test file
cargo test --features metadata-duckdb --test table_tests
cargo test --features metadata-duckdb --test delete_filter_tests

# Run specific test
cargo test --features metadata-duckdb test_empty_table_basic_scan
```

---

## Tier 3: Behavioral Verification Against Live DuckDB

### Purpose

For complex or ambiguous behaviors, verify that the DataFusion extension produces identical results to DuckDB + DuckLake by executing the same operation against both engines and comparing results.

### When to Use Tier 3

Tier 3 tests are appropriate when:
- The expected behavior is not obvious from the DuckLake specification
- NULL handling or type coercion behavior is ambiguous
- Edge cases in filter pushdown, aggregation, or sorting
- New type mappings where Arrow representation choices matter
- Delete file interaction with specific query patterns (e.g., GROUP BY with deletes)

Tier 3 is **not** a substitute for Tier 1 or Tier 2. Use it for targeted verification of specific behaviors, not for broad coverage.

### Test Pattern

```rust
#[tokio::test]
async fn test_behavior_matches_duckdb() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("verify.ducklake");

    // Step 1: Setup via DuckDB
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;
    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])?;
    conn.execute("CREATE TABLE test_catalog.t (id INT, val DOUBLE);", [])?;
    conn.execute("INSERT INTO test_catalog.t VALUES (1, 1.5), (2, NULL), (3, 'NaN'::DOUBLE);", [])?;

    // Step 2: Query via DuckDB
    let mut stmt = conn.prepare("SELECT SUM(val) FROM test_catalog.t")?;
    let duckdb_result: f64 = stmt.query_row([], |row| row.get(0))?;

    // Step 3: Query via DataFusion
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())?;
    let catalog = Arc::new(DuckLakeCatalog::new(provider)?);
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", catalog);
    let df = ctx.sql("SELECT SUM(val) FROM ducklake.main.t").await?;
    let batches = df.collect().await?;
    let datafusion_result = extract_f64(&batches[0], 0, 0);

    // Step 4: Compare
    assert_eq!(duckdb_result, datafusion_result,
        "DuckDB and DataFusion should produce identical SUM results with NULL/NaN");

    Ok(())
}
```

### Key Behaviors to Verify

These behaviors are documented in `docs/duckdb-behavior-reference.md` and warrant dual-execution testing:

| Behavior | Reference |
|----------|-----------|
| NULL handling in aggregates (SUM, AVG, COUNT) | duckdb-behavior-reference.md: "Type System" |
| TIMESTAMP vs TIMESTAMPTZ normalization | duckdb-behavior-reference.md: "TIMESTAMPTZ values normalized to UTC" |
| DECIMAL precision and scale after arithmetic | duckdb-behavior-reference.md: "Type Promotions" |
| BOOLEAN representation in output | duckdb-behavior-reference.md: "Type System" |
| Empty table scan results (schema preservation) | duckdb-behavior-reference.md: "DDL" |
| DELETE + re-INSERT row ordering | duckdb-behavior-reference.md: "Delete Operations" |
| Type promotion after ALTER TABLE ALTER COLUMN | duckdb-behavior-reference.md: "Schema Evolution" |
| Column ordering after ADD/DROP COLUMN | duckdb-behavior-reference.md: "Schema Evolution" |

### Running Tier 3 Tests

Tier 3 tests live alongside Tier 2 tests in the same test files, distinguished by naming:

```bash
# Run behavioral verification tests
cargo test --features metadata-duckdb test_behavior_
```

---

## Tier 4: Unit Tests

### Purpose

Test internal logic in isolation where it provides real value. Unit tests are **not** a substitute for integration tests -- use them sparingly and only where they add clarity.

### What to Unit Test

- **Type parsing** (`src/types.rs`): Parsing DuckLake type strings into Arrow DataTypes. Many edge cases (aliases, precision/scale, nested types).
- **Path resolution** (`src/path_resolver.rs`): URL parsing, relative/absolute path resolution, hierarchical path construction.
- **Delete position extraction** (`src/delete_filter.rs`): Extracting row positions from delete file batches.
- **SQL generation** (`src/metadata_provider_duckdb.rs`): If SQL query construction becomes complex, verify generated SQL strings.

### What NOT to Unit Test

- Catalog/schema/table provider methods (use Tier 2 integration tests instead)
- Anything that requires a DuckDB connection (use Tier 2 or Tier 3)
- DataFusion execution plan behavior (use Tier 1 or Tier 2)

### Existing Unit Tests

- `src/delete_filter.rs`: `test_extract_deleted_positions_simple`, `test_extract_deleted_positions_multiple_files`, `test_extract_deleted_positions_with_nulls`
- `tests/hybrid_asyncdb.rs`: `test_write_detection`, `test_table_rewrite`

### Running Unit Tests

```bash
# Run all unit tests (no feature flag needed for pure unit tests)
cargo test --lib

# Run unit tests in a specific module
cargo test --lib types::
cargo test --lib delete_filter::
```

---

## Test Infrastructure

### Feature Gating

All tests that require DuckDB are gated behind the `metadata-duckdb` feature flag:

```rust
#![cfg(feature = "metadata-duckdb")]
```

This allows `cargo test` without DuckDB to still run pure unit tests.

### Temporary Directory Management

Tests create isolated temporary directories for catalog files:

```rust
let temp_dir = TempDir::new()?;
let catalog_path = temp_dir.path().join("test.ducklake");
```

The `TempDir` is automatically cleaned up when dropped. For tests that need the directory to outlive setup functions, use `std::mem::forget(temp_dir)` -- the OS will clean up on process exit.

### DuckDB Connection Pattern

All test setup uses in-memory DuckDB connections to avoid file locking issues:

```rust
let conn = duckdb::Connection::open_in_memory()?;
conn.execute("INSTALL ducklake;", [])?;
conn.execute("LOAD ducklake;", [])?;
let ducklake_path = format!("ducklake:{}", catalog_path.display());
conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])?;
```

The DuckLake catalog file is stored on disk (for DataFusion to read), but the DuckDB connection itself is in-memory.

### Hybrid Test Runner Type Conversion

The hybrid adapter (`tests/hybrid_asyncdb.rs`) converts Arrow types to sqllogictest string output. Currently supported types:

| Arrow Type | Output Format |
|-----------|---------------|
| Int8, Int16, Int32, Int64 | Integer string (`"42"`) |
| Float32, Float64 | Float string (`"3.14"`) |
| Utf8 | Raw string |
| Boolean | `"true"` / `"false"` |
| Date32 | ISO date (`"2024-01-15"`) |
| Timestamp (microsecond) | Datetime string (`"2024-01-15 10:30:00"`) |
| Decimal128 | Fixed-point string (`"99.99"`) |
| NULL | `"NULL"` |

New types must be added to `convert_batch_to_strings()` in `hybrid_asyncdb.rs` as features are implemented. Missing type conversions will produce debug-format output (e.g., `"{:?}"`) which will cause sqllogictest comparison failures.

---

## Naming Conventions

### Test Files

| Location | Pattern | Example |
|----------|---------|---------|
| Tier 1 (sqllogic) | `tests/sqllogictests/sql/<category>/<name>.test` | `tests/sqllogictests/sql/insert/insert_column_list.test` |
| Tier 2 (contract) | `tests/<feature>_tests.rs` | `tests/table_tests.rs` |
| Tier 3 (behavioral) | Inside Tier 2 files, prefixed `test_behavior_` | `test_behavior_null_aggregation` |
| Tier 4 (unit) | Inside `src/*.rs` as `#[cfg(test)] mod tests` | `src/types.rs::tests::test_parse_decimal` |

### Test Functions

- Use `test_` prefix (required by Rust)
- Use descriptive names: `test_<what>_<scenario>`
- Ignored tests use `#[ignore]` with a comment explaining why

```rust
#[tokio::test]
#[ignore] // TODO: Requires struct type support (not yet implemented)
async fn test_struct_field_projection() -> DataFusionResult<()> {
    // ...
}
```

---

## Tests-First Principle

Every identified gap in the gap analysis (`docs/gap-analysis.md`) must have at least one test **before** implementation begins. The workflow for each feature is:

1. **Write the test**: Create a failing test that exercises the expected behavior
2. **Verify it fails**: Run the test and confirm it fails for the right reason
3. **Implement**: Write the minimal code to make the test pass
4. **Verify it passes**: Run the test suite and confirm no regressions
5. **Document**: Note any tradeoffs, limitations, or follow-up work

If a feature cannot be fully implemented, leave:
- A `#[ignore]` test with a `TODO` comment explaining what remains
- No partial implementation presented as complete

```rust
#[tokio::test]
#[ignore] // TODO: Implement LIST type mapping in types.rs
async fn test_list_column_roundtrip() -> DataFusionResult<()> {
    // Setup: create table with LIST column via DuckDB
    // Query: SELECT list_col FROM ducklake.main.t
    // Assert: correct Arrow List type and values
    todo!("Blocked on complex type support")
}
```

---

## Running All Tests

```bash
# Full test suite (all tiers)
cargo test --features metadata-duckdb

# Tier 1 only (sqllogic compatibility)
cargo test --features metadata-duckdb run_all_sqllogictests -- --nocapture

# Tier 2 only (contract tests)
cargo test --features metadata-duckdb --test table_tests --test delete_filter_tests --test concurrent_tests --test information_schema_test

# Tier 4 only (unit tests, no DuckDB needed)
cargo test --lib

# Single test by name
cargo test --features metadata-duckdb test_empty_table_basic_scan
```
