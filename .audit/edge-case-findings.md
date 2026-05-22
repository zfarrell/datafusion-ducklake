# Deep Edge Case Testing Findings

## Summary

Ran 33 edge case tests against the DataFusion-DuckLake implementation. **32 passed, 1 failed.** Four notable findings were discovered.

## Bug: Empty Schema Not Visible After Creation

**Severity: Medium**
**Test: `test_empty_schema_list_tables` (FAILED)**
**File: `src/catalog.rs`**

When a schema is created via `CREATE SCHEMA`, the catalog cannot see it because `DuckLakeCatalog::schema()` uses the snapshot_id that was pinned when the catalog was created. Schema creation produces a new snapshot, but the catalog still queries with the old snapshot_id.

**Root cause:** `DuckLakeCatalog::schema()` calls `self.metadata_provider.get_schema_by_name()` with the pinned `self.snapshot_id`. The newly created schema has a `begin_snapshot` greater than the pinned snapshot, so the query filters it out.

**Impact:** After `CREATE SCHEMA`, the same session cannot access the new schema without re-creating the catalog (or refreshing the snapshot). This affects any workflow where a schema is created and immediately used.

**Suggested fix:** After `register_schema()` creates a new snapshot, update `self.snapshot_id` to the new snapshot value. This requires making `snapshot_id` mutable (e.g., `AtomicI64` or `Mutex<i64>`).

## Finding: VARCHAR(N) With Length Specifier Not Handled

**Severity: Low**
**Test: `test_varchar_with_length` (PASSED - test verified the gap exists)**
**File: `src/types.rs`**

The type parser `ducklake_to_arrow_type()` does not handle `VARCHAR(N)` format (e.g., `"varchar(255)"`). It only matches the exact string `"varchar"`. Types with length specifiers fall through to `UnsupportedType` error.

**Impact:** If DuckLake catalogs contain columns typed as `VARCHAR(255)` or similar, they cannot be read. Currently DuckDB normalizes these to plain `VARCHAR`, so this is not an immediate issue, but could surface with other metadata backends.

**Suggested fix:** Add a check in the type parser: if the type starts with `"varchar("`, map it to `DataType::Utf8` (ignoring the length, since Arrow strings are variable-length).

## Finding: Struct Field Names With Spaces Not Supported

**Severity: Low**
**Test: `test_struct_field_names_with_spaces` (PASSED - test verified the gap exists)**
**File: `src/types.rs`**

The `parse_complex_type()` function cannot parse struct types where field names contain spaces (e.g., `STRUCT("first name" VARCHAR, "last name" VARCHAR)`). The parser doesn't handle quoted identifiers.

**Impact:** Tables with struct columns using quoted field names cannot be read. This is an uncommon pattern but is valid in DuckDB.

## Finding: Duplicate Column Names Accepted in Write

**Severity: Low**
**Test: `test_duplicate_column_names_write` (PASSED - test verified duplicate names are accepted)**
**File: `src/table_writer.rs`**

Writing a RecordBatch with duplicate column names (e.g., two columns both named `"x"`) succeeds without error. The data is written and can be read back, but having duplicate column names can cause ambiguity in queries.

**Impact:** Could lead to confusing query results if a user accidentally creates a table with duplicate column names. Arrow schemas technically allow duplicate names, but SQL semantics expect unique column names.

**Suggested fix:** Add validation in `DuckLakeTableWriter` or `DuckLakeSchema::register_table()` to reject schemas with duplicate column names.

## Tests That Passed (Confirming Correct Behavior)

The following edge cases were tested and confirmed to work correctly:

- **Rename column to same name**: Correctly returns an error
- **Drop + re-add same column name**: Works correctly across snapshots
- **Alter column type to same type**: Correctly returns an error
- **Decimal edge cases**: Max precision (38,10), zero-scale (10,0), NUMERIC alias all work
- **HUGEINT type**: Correctly mapped to Decimal128(38,0)
- **View on dropped table**: Correctly returns an error when querying
- **Multiple appends then delete**: Data integrity maintained
- **Write then replace**: CTAS replacement works correctly
- **Write zero-row batch**: Handled correctly (no data file created)
- **Column with very long name (200 chars)**: Works correctly
- **Table name with hyphens**: Works correctly
- **Snapshot ID monotonicity**: Verified across operations
- **Append with schema evolution (new nullable column)**: Works correctly
- **Append with type mismatch**: Correctly returns an error
- **Append with non-nullable new column**: Correctly returns an error
- **Type roundtrip (INTERVAL, UUID, JSON)**: All map and roundtrip correctly
- **DELETE/UPDATE on empty table**: Both succeed without error
- **Malformed complex types**: All return errors without panicking
- **Write with no batches**: Handled correctly
- **get_data_path before setting**: Returns appropriate error
- **Negative decimal scale**: Correctly returns an error
- **Decimal without parens**: Correctly returns an error

## Test File

All tests are in `tests/deep_edge_case_tests.rs`. Run with:

```bash
cargo test --test deep_edge_case_tests --features write-sqlite,skip-tests-with-docker
```
