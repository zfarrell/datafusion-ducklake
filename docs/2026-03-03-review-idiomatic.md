# R4 Idiomatic Rust Review

**Date**: 2026-03-03
**Reviewer**: idiomatic-review agent
**Scope**: All 33 source files in `src/`
**Focus**: Error handling, ownership/borrowing, DataFusion API usage, consistency, performance, code organization
**Prior cycles**: R1 (36 findings), R2 (55 findings), R3 (38 findings) — this review targets NEW issues only

---

## P1 — Important

### R4-I-001: Unchecked `as` casts in table_deletions.rs filter_batch()

**File(s)**: `src/table_deletions.rs:762-784`
**Description**: The `filter_batch()` method guards against `num_rows > u32::MAX` but then uses bare `as` casts for the actual conversions (`0..num_rows as u32`, `i as i64`, `i as u32`). While the guard makes these safe at runtime, `as` casts bypass the compiler's narrowing checks and are inconsistent with the project's move toward `try_from`/`i64::from` elsewhere.
**Suggested fix**: Replace `as` casts with `u32::try_from(num_rows).unwrap()` (since the guard already validates the range) or use `i64::from(i)` for widening casts. This matches the pattern used in `delete_exec.rs:278-301`.

### R4-I-002: Lossy `i64 as f64` cast in compaction_functions.rs

**File(s)**: `src/compaction_functions.rs:482`
**Description**: `*v as f64` converts an `i64` to `f64`. For values above 2^53 (~9 quadrillion), this silently loses precision. While unlikely in practice for compaction thresholds, it's a latent correctness issue.
**Suggested fix**: Add a range guard:
```rust
let fv = *v as f64;
if fv as i64 != *v {
    return Err(DataFusionError::Plan(format!(
        "Integer value {} cannot be represented exactly as f64", v
    )));
}
Ok(fv)
```

### R4-I-003: Silent error swallowing in metadata_provider_duckdb.rs

**File(s)**: `src/metadata_provider_duckdb.rs:598-600`
**Description**: When reading PRAGMA table_info for inlined data columns, `.filter_map(|r| r.ok())` silently drops any rows that fail to deserialize. If the PRAGMA returns unexpected data, this could cause incorrect column filtering, leading to wrong query results with no error or warning.
**Suggested fix**: Replace with `.collect::<Result<Vec<_>, _>>()?` to propagate errors, or at minimum log dropped errors at warn level.

### R4-I-004: Pervasive `.map_err(|e| DataFusionError::External(Box::new(e)))` boilerplate

**File(s)**: 50+ occurrences across `src/delete_exec.rs`, `src/insert_exec.rs`, `src/merge_exec.rs`, `src/update_exec.rs`, `src/table_writer.rs`, `src/compaction_functions.rs`, `src/table_changes.rs`, `src/table_deletions.rs`, `src/encryption.rs`
**Description**: The same error-wrapping pattern is repeated verbatim dozens of times. This adds visual noise, increases the chance of inconsistent error wrapping, and makes it harder to add context to errors later.
**Suggested fix**: Add a helper trait in `src/error.rs`:
```rust
pub(crate) trait IntoDataFusionExternal<T> {
    fn into_df_external(self) -> DataFusionResult<T>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> IntoDataFusionExternal<T> for Result<T, E> {
    fn into_df_external(self) -> DataFusionResult<T> {
        self.map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
```
Then call sites become: `arrow_writer.write(&batch).into_df_external()?;`

---

## P2 — Moderate

### R4-I-005: CDC projection analysis duplicated between table_changes.rs and table_deletions.rs

**File(s)**: `src/table_changes.rs:385-468`, `src/table_deletions.rs:111-188`
**Description**: Both files implement nearly identical `analyze_projection` methods that split projection indices into table columns and CDC virtual columns, compute reorder mappings, and handle the "no projection = all columns" case. The logic is complex enough (~80 lines each) that a bug fix in one may not be applied to the other.
**Suggested fix**: Extract a shared `CdcProjectionAnalysis` struct and `analyze_cdc_projection()` function into a common module (e.g., `src/cdc_common.rs` or a shared section of `src/table_functions.rs`).

### R4-I-006: Fragile `bind_repeat!` macro in metadata_provider_postgres.rs

**File(s)**: `src/metadata_provider_postgres.rs:16-49`
**Description**: The `bind_repeat!` macro manually implements repetition counts 1 through 8 via copy-paste arms. It's used exactly once (for binding `snapshot_id` multiple times in a query). This is fragile — adding a 9th binding requires adding a new arm — and obfuscates what's happening.
**Suggested fix**: Replace with a simple loop or fold:
```rust
fn bind_repeated<'a>(
    mut query: sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: i64,
    count: usize,
) -> sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for _ in 0..count {
        query = query.bind(value);
    }
    query
}
```

### R4-I-007: `*v as i64` widening casts in table_functions.rs

**File(s)**: `src/table_functions.rs:377-390`
**Description**: Several `ScalarValue` extraction arms use `*v as i64` for widening conversions from Int8/Int16/Int32 to i64. While these are lossless, `i64::from(*v)` is the idiomatic Rust way to express guaranteed-lossless widening and makes the safety self-documenting.
**Suggested fix**: Replace `*v as i64` with `i64::from(*v)` for Int8, Int16, and Int32 arms. Keep `as` only where `From` is not implemented (e.g., unsigned-to-signed where overflow is possible).

### R4-I-008: Row collection boilerplate in compaction_functions.rs

**File(s)**: `src/compaction_functions.rs:296-343`, `549-579`
**Description**: Multiple functions manually push values into 5-7 parallel `Vec<Option<T>>` arrays column by column when extracting rows from query results. This pattern is repeated for both `get_data_files_for_compaction` and `get_compaction_file_details`, with slight column variations.
**Suggested fix**: Consider a struct-based approach where rows are collected into a `Vec<FileRow>` struct first, then converted to columnar arrays in a single pass. This improves readability and reduces the risk of column count mismatches.

### R4-I-009: Path normalization duplication in encryption.rs

**File(s)**: `src/encryption.rs:253-270`
**Description**: The encryption key lookup tries four path format variations (as-is, with leading `/`, without leading `/`, basename-only) to find a matching key. This duplicates path normalization logic that `path_resolver.rs` already handles, and the four-variant approach is fragile.
**Suggested fix**: Use `path_resolver::resolve_path()` or a canonical-path helper to normalize both the lookup key and the file path before comparison, reducing the match to a single normalized comparison.

### R4-I-010: Monolithic 200+ line execute() async blocks

**File(s)**: `src/delete_exec.rs:193-406`, `src/insert_exec.rs` (similar), `src/merge_exec.rs` (similar), `src/update_exec.rs` (similar)
**Description**: The `execute()` methods in DML exec plans contain 200+ line async blocks that handle file scanning, filter evaluation, file writing, metadata registration, and cleanup in a single scope. This makes the control flow hard to follow and difficult to unit test individual phases.
**Suggested fix**: Extract logical phases into helper async functions:
- `scan_and_collect_positions()` — scan files and collect matching row positions
- `write_delete_files()` — write Parquet delete files to object store
- `commit_metadata()` — register files in catalog with cleanup on failure

---

## P3 — Minor / Style

### R4-I-011: Inconsistent `Arc::clone` vs `.clone()` usage

**File(s)**: Various — `src/delete_exec.rs`, `src/table.rs`, `src/schema.rs`, `src/catalog.rs`, `src/table_changes.rs`
**Description**: Some files use the Clippy-recommended `Arc::clone(&x)` pattern while others use `x.clone()` for `Arc` values. The project has no consistent convention. `Arc::clone` makes it clear the operation is cheap (reference count increment), while `.clone()` on an `Arc` can look like a deep clone.
**Suggested fix**: Standardize on `Arc::clone(&x)` project-wide, matching the existing usage in `delete_exec.rs:184-188`. Consider enabling `clippy::clone_on_ref_ptr` lint.

### R4-I-012: Residual `as` casts in delete_filter.rs

**File(s)**: `src/delete_filter.rs:170-189`
**Description**: Same pattern as R4-I-001 — `filter_batch()` has a guard at line 170 (`num_rows > u32::MAX as usize`) followed by `i as i64` (line 175) and `i as u32` (line 178) casts. These are safe due to the guard but inconsistent with the `try_from` pattern used elsewhere in the project.
**Suggested fix**: Same as R4-I-001 — use `i64::from` for widening and `u32::try_from().unwrap()` for narrowing (or restructure the loop to avoid the cast entirely).

### R4-I-013: Missing `#[must_use]` on builder patterns

**File(s)**: `src/metadata_writer.rs` (DeleteFileInfo, DataFileInfo builders), `src/table.rs` (DuckLakeTable builder methods)
**Description**: Builder methods like `DeleteFileInfo::new().with_footer_size()` return `Self` but are not marked `#[must_use]`. Forgetting to capture the return value silently discards the configuration.
**Suggested fix**: Add `#[must_use]` to builder methods that return `Self`, or add `#[must_use]` to the struct itself.

### R4-I-014: Four-way MetadataProvider implementation duplication

**File(s)**: `src/metadata_provider_duckdb.rs`, `src/metadata_provider_sqlite.rs`, `src/metadata_provider_postgres.rs`, `src/metadata_provider_mysql.rs`
**Description**: The four MetadataProvider implementations (700-1000 lines each) share ~80% identical logic, differing mainly in SQL parameter binding syntax (`?` vs `$1` vs named), connection management (rusqlite vs sqlx), and a few database-specific queries (PRAGMA vs information_schema). This makes it easy for bug fixes to be applied to one backend but missed in others.
**Suggested fix**: Long-term, consider a template/macro approach or a shared base implementation with database-specific adapters for binding and connection. Short-term, ensure test coverage is symmetric across all backends.

### R4-I-015: Verbose error wrapping in compaction_functions.rs

**File(s)**: `src/compaction_functions.rs` (throughout)
**Description**: Many error conversions use multi-line closures like:
```rust
.map_err(|e| DataFusionError::Execution(format!("Failed to parse compaction result: {}", e)))?
```
While more descriptive than `External(Box::new(e))`, the verbosity adds noise. Some messages are redundant with the underlying error.
**Suggested fix**: If R4-I-004's helper trait is adopted, extend it with a contextual variant:
```rust
fn into_df_execution(self, context: &str) -> DataFusionResult<T>;
```

---

## Summary

| Priority | Count | Description |
|----------|-------|-------------|
| P0       | 0     | —           |
| P1       | 4     | Silent error swallowing, unsafe casts, pervasive boilerplate |
| P2       | 6     | Code duplication, fragile macros, monolithic functions |
| P3       | 5     | Style inconsistencies, missing attributes |
| **Total**| **15**| |

No P0 (critical/blocking) issues found. The codebase is generally well-structured with good error handling. The most impactful improvements would be R4-I-004 (error helper trait, reduces 50+ repetitive sites) and R4-I-003 (silent error swallowing, potential correctness issue).
