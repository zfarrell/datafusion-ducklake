# R7 Idiomatic Review – 2026-03-04

**Reviewer:** idiomatic-review agent (Claude Opus 4.6)
**Branch:** `ducklake-features/integration`
**Scope:** All `src/**/*.rs` files; special focus on R6-introduced code
**Focus:** Rust idioms, DataFusion API usage, patterns, code organization

---

## Summary

Reviewed 34 Rust source files for idiomatic patterns, error handling, ownership, DataFusion API usage, and code consistency. Ran Codex second opinion on five key R6 files (`compaction_functions.rs`, `parse_values.rs`, `insert_exec.rs`, `metadata_writer_sqlite.rs`, `table_writer.rs`). Found 6 P1/P2 issues and 8 P3 issues.

Overall the codebase follows good Rust practices — all `.unwrap()` calls except two are in `#[cfg(test)]` code, error propagation with `?` is consistent, and the new R6 modules (`parse_values.rs`, `metadata_writer_validation.rs`, `compaction_functions.rs`) are well-structured. The main issues are: (1) a dead-code module that was never wired in, (2) redundant pattern matching, (3) silent error swallowing in `OnceLock`, and (4) cross-backend validation gaps.

---

## P1 – Must Fix

### P1-IDM-001: `OnceLock` silently swallows `INSTALL ducklake` failure
- **File:** `src/compaction_functions.rs:81-83`
- **Issue:** `DUCKLAKE_INSTALLED.get_or_init(|| { let _ = conn.execute("INSTALL ducklake;", []); })` ignores the result. If the first `INSTALL` call fails (transient I/O error, network issue), the `OnceLock` is permanently set and subsequent calls will never retry — silently leaving DuckLake uninstalled for the rest of the process lifetime.
- **Fix:** Either use a custom try-init pattern that doesn't cache failures, or check the result and propagate the error. Example:
  ```rust
  static DUCKLAKE_INSTALLED: OnceLock<Result<(), String>> = OnceLock::new();
  let install_result = DUCKLAKE_INSTALLED.get_or_init(|| {
      conn.execute("INSTALL ducklake;", [])
          .map_err(|e| e.to_string())
  });
  if let Err(e) = install_result {
      return Err(DataFusionError::External(Box::new(
          std::io::Error::new(std::io::ErrorKind::Other, e.clone())
      )));
  }
  ```

### P1-IDM-002: `parse_values.rs` module is dead code — not wired into any path
- **File:** `src/parse_values.rs` (entire module), `src/table_writer.rs:1206,1274`
- **Issue:** The R6-introduced `parse_string_values_to_array` (with `ParseMode::Lenient`/`Strict`) is only used in its own `#[cfg(test)]` module. The actual read path (`table.rs`) and write path (`table_writer.rs:1206`) still use the legacy `parse_string_to_array` from `table_writer.rs:1274`. This means:
  - The shared parsing module documented as used by "both the read path and write path" is dead code
  - The `ParseMode` abstraction is untested in production
  - Bug fixes to one parser won't propagate to the other
- **Fix:** Replace `table_writer::parse_string_to_array` calls with `parse_values::parse_string_values_to_array(values, data_type, ParseMode::Strict)`. Replace the read-path parser in `table.rs` (if it has one) with `ParseMode::Lenient`. Delete the legacy `parse_string_to_array` from `table_writer.rs`.

---

## P2 – Should Fix

### P2-IDM-003: `is_sqlite_busy` has redundant pattern destructuring
- **File:** `src/metadata_writer_sqlite.rs:35-46`
- **Issue:** The function destructures `DuckLakeError::Sqlx(sqlx::Error::Database(db_err))` twice in sequence — once to check `db_err.code()`, and again to check `db_err.message()`. The second `if let` always matches when the first does, creating dead-code duplication.
- **Fix:** Combine into a single match:
  ```rust
  fn is_sqlite_busy(err: &DuckLakeError) -> bool {
      if let DuckLakeError::Sqlx(sqlx::Error::Database(db_err)) = err {
          if let Some(code) = db_err.code() {
              if code.as_ref() == "5" || code.as_ref() == "517" {
                  return true;
              }
          }
          return db_err.message().contains("database is locked");
      }
      false
  }
  ```

### P2-IDM-004: `validate_ducklake_type_for_ddl` only exists in SQLite writer
- **File:** `src/metadata_writer_sqlite.rs` (present), `src/metadata_writer_postgres.rs` (absent), `src/metadata_writer_mysql.rs` (absent)
- **Issue:** The SQL injection validation function `validate_ducklake_type_for_ddl` (R6-S-029) that rejects type strings with SQL-special characters is only implemented in the SQLite metadata writer. The PostgreSQL and MySQL writers perform DDL with string-interpolated type names but have no equivalent validation, creating an inconsistent security posture.
- **Fix:** Move `validate_ducklake_type_for_ddl` to `metadata_writer_validation.rs` (where other shared validation already lives) and call it from all three backend writers.

### P2-IDM-005: `PartitionTransform::from_str_opt` silently defaults unknown transforms to Identity
- **File:** `src/insert_exec.rs:54-62`
- **Issue:** The catch-all `Some(_) => Self::Identity` means typos like `"yer"` (for `"year"`) or completely invalid transforms silently become Identity partitioning. The validation module (`metadata_writer_validation.rs`) has an `ALLOWED_PARTITION_TRANSFORMS` allowlist, but `from_str_opt` doesn't use it.
- **Fix:** Return `Result<Self, DuckLakeError>` instead of defaulting, or log a warning. At minimum, use the allowlist from validation to reject unknown transforms at parse time.

### P2-IDM-006: `.unwrap()` in non-test code for epoch date
- **File:** `src/table_writer.rs:1362`, `src/table_writer.rs:1387`
- **Issue:** `chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()` appears twice in the production `parse_string_to_array` function. This is technically safe (1970-01-01 is always valid) but violates the project convention against `unwrap()` in non-test code. The newer `parse_values.rs` already has a `const UNIX_EPOCH_DATE` that handles this at compile time.
- **Fix:** If P1-IDM-002 is resolved (wiring in `parse_values.rs`), this code is deleted. Otherwise, replace with a const:
  ```rust
  const UNIX_EPOCH_DATE: chrono::NaiveDate = match chrono::NaiveDate::from_ymd_opt(1970, 1, 1) {
      Some(d) => d,
      None => panic!("1970-01-01 is a valid date"),
  };
  ```

---

## P3 – Nice to Have

### P3-IDM-007: Unchecked `as i32` cast for Date64 epoch days
- **File:** `src/table_writer.rs:1091` (approximate)
- **Issue:** `(epoch_ms / 86_400_000) as i32` can overflow for extreme `Date64` values (though unlikely in practice). Arrow's `Date32` range is ~±5.8M years, so this is safe for real dates but not checked.
- **Fix:** Use `i32::try_from(epoch_ms / 86_400_000).map_err(...)` for defensive correctness.

### P3-IDM-008: `to_lowercase()` allocates per boolean parse
- **File:** `src/parse_values.rs:80`, `src/table_writer.rs:1308`
- **Issue:** `s.to_lowercase()` allocates a new `String` for every boolean value parsed. For large inlined datasets this is wasteful.
- **Fix:** Use `eq_ignore_ascii_case` for comparison:
  ```rust
  if s.eq_ignore_ascii_case("true") || s == "1" || s.eq_ignore_ascii_case("t") {
      builder.append_value(true)
  } else if s.eq_ignore_ascii_case("false") || s == "0" || s.eq_ignore_ascii_case("f") {
      builder.append_value(false)
  } else { ... }
  ```

### P3-IDM-009: Per-row `Vec<Option<String>>` cloning in partition routing
- **File:** `src/insert_exec.rs:543`
- **Issue:** `route_batches_identity` clones `Option<String>` per row per partition column via `col[row_idx].clone()`. For tables with many rows, this creates significant allocation pressure.
- **Fix:** Build the partition key from string references and only clone values on first insertion into the map entry.

### P3-IDM-010: `unwrap_or(i64::MAX)` silently saturates file count
- **File:** `src/table_writer.rs:1902`
- **Issue:** `i64::try_from(result.files_written).unwrap_or(i64::MAX)` silently returns `i64::MAX` if the count overflows. While practically impossible (writing >9.2 quintillion files), this masks a potential logic error.
- **Fix:** Return an error or use `.expect("files_written within i64 range")` to fail fast in debug builds.

### P3-IDM-011: `inlined_rows_to_batch` has O(cols × rows) linear lookups
- **File:** `src/table_writer.rs:1197` (approximate)
- **Issue:** For each cell, the function does `row.columns.iter().position(|c| c.name == field.name())` — an O(N) scan per column per row. For tables with many columns, this is quadratic.
- **Fix:** Precompute a `HashMap<&str, usize>` from column names to row-value indices once, then index into it per field.

### P3-IDM-012: `to_lowercase()` allocates in `PartitionTransform::from_str_opt`
- **File:** `src/insert_exec.rs:55`
- **Issue:** `s.map(|t| t.to_lowercase())` allocates a new `String` per partition column per call. Since this is called during planning (not per-row), the impact is minimal, but `eq_ignore_ascii_case` would be more idiomatic.
- **Fix:** Use match with `eq_ignore_ascii_case` instead of `to_lowercase()`.

### P3-IDM-013: `columns[0]` direct indexing without bounds check
- **File:** `src/compaction_functions.rs:221`
- **Issue:** Direct `columns[0]` access assumes non-empty schema. If this helper is ever called with a zero-column schema, it will panic.
- **Fix:** Guard with `columns.first().ok_or_else(|| ...)?`.

### P3-IDM-014: `expect()` on `len == 1` guarded vectors
- **File:** `src/table_deletions.rs:347`, `src/table_changes.rs:786`
- **Issue:** These use `.expect("...")` after checking `len == 1`. While logically safe, the `expect` message would be more useful if it included the actual length for debugging.
- **Fix:** Use `.unwrap_or_else(|| panic!("expected 1 element, got {}", vec.len()))` or just keep as-is (minor).

---

## Codex Second Opinion

Ran `codex exec --full-auto` on the five key R6 files. Codex independently confirmed 8 of the 14 findings above and identified the same patterns (unwrap in non-test, duplicate parsing, redundant pattern match, silent OnceLock failure, per-row cloning). No additional critical findings beyond what the manual review found.

### Codex-specific notes:
- Codex flagged `to_lowercase()` allocation in both boolean parsing and partition transform parsing (P3-IDM-008, P3-IDM-012)
- Codex flagged `idxs.clone()` in `insert_exec.rs:721` before `UInt32Array::from(...)` as an unnecessary copy — vectors could be consumed instead of cloned (minor, folded into P3-IDM-009 scope)
- Codex confirmed no other non-test `unwrap()`/`expect()` in the reviewed files

---

## Positive Observations

1. **Error handling is excellent overall** — 99%+ of non-test code uses `?` and `Result` properly
2. **`metadata_writer_validation.rs`** is a well-designed shared validation module (R6 improvement)
3. **`DeferredCompactionProvider`** pattern is idiomatic DataFusion — defers DuckDB execution to scan time correctly
4. **Delete/Update/Merge execution plans** consistently implement cleanup-on-failure (R6-S-037/S-038)
5. **`parse_values.rs`** itself is well-written (const epoch, ParseMode enum, comprehensive type coverage) — it just needs to be wired in
6. **Snapshot propagation via `AtomicI64`** in `schema.rs` is correct and avoids cache invalidation complexity
7. **`downcast_key!` macro** in `merge_exec.rs` properly replaces unwrap-heavy downcasting with proper error returns
8. **SQLite busy retry** has good exponential backoff with jitter — production-grade pattern

---

## Statistics

| Metric | Count |
|--------|-------|
| Files reviewed | 34 |
| P1 findings | 2 |
| P2 findings | 4 |
| P3 findings | 8 |
| Total findings | 14 |
| `.unwrap()` in non-test code | 2 (both in `table_writer.rs`) |
| `.expect()` in non-test code | 2 (both guarded by length checks) |
| Codex findings (unique) | 0 (all overlapped with manual review) |
