# R7 Codex Review — 2026-03-04

## Overview

Four codex reviews were run against the `ducklake-features/integration` branch using `codex exec --full-auto`. Every P0 and P1 finding was validated against actual source code.

**Historical context**: Codex P0 false-positive rate across R4–R6 was ~90%+. All codex P0 findings were defaulted to P1-candidate and only upgraded after confirming the code is NOT within a transaction.

---

## Review 1: Write Path

**Files**: `src/insert_exec.rs`, `src/delete_exec.rs`, `src/update_exec.rs`, `src/merge_exec.rs`, `src/table_writer.rs`, `src/table_insertions.rs`

### P1 Findings

| # | File:Line | Codex Claim | Validated? | Notes |
|---|-----------|-------------|------------|-------|
| 1 | merge_exec.rs:493-513 | MERGE silently picks one source row when multiple share a join key | **FALSE POSITIVE** | Line 499 has explicit cardinality check: `source_match_count[src_global] > 1` returns error. The `break` at 512 is after first candidate match per target row — correct behavior. |
| 2 | table_writer.rs:1692 | `decode_decimal_bytes` panics for >16 byte decimals | **VALID P1** | `16usize.saturating_sub(bytes.len())` yields start=0 when bytes>16; `copy_from_slice` panics on size mismatch. Very unlikely in practice (Decimal128 stats ≤16 bytes) but Decimal256 path converts to i128 and could trigger. |
| 3 | table_writer.rs:302 | `current_inline + total_new_rows` i64 overflow | **VALID P1** | Unchecked addition of two i64 values. Both need to be very large (~4.6×10¹⁸) to overflow. Low probability but real. |

### P2 Findings

| # | File:Line | Finding |
|---|-----------|---------|
| 1 | insert_exec.rs:537,659 | Partition column index used without bounds check |
| 2 | table_insertions.rs:104-107 | Projection index panic on out-of-bounds |

---

## Review 2: Metadata Writers

**Files**: `src/metadata_writer.rs`, `src/metadata_writer_sqlite.rs`, `src/metadata_writer_postgres.rs`, `src/metadata_writer_mysql.rs`, `src/metadata_writer_validation.rs`

### P1 Findings

| # | File:Line | Codex Claim | Validated? | Notes |
|---|-----------|-------------|------------|-------|
| 1 | sqlite/pg/mysql register_dml_files | `record_count = COALESCE(record_count, 0) - ?` can go negative | **VALID P1** | SQLite line ~1629. No lower-bound check. If metadata is inconsistent the count goes negative. Low probability in normal operation. |
| 2 | sqlite/pg/mysql replace_table_files | `files.iter().map(\|f\| f.file_info.record_count).sum()` unchecked overflow | **VALID P1** | SQLite line ~1532. Pure `sum::<i64>()` with no checked arithmetic. Would require astronomical file counts. |
| 3 | metadata_writer_validation.rs:352 | SET NOT NULL without data validation | **FALSE POSITIVE** | Comment explicitly says "Known limitation" matching DuckDB behavior. By design, not a bug. |

### P2 Findings

| # | File:Line | Finding |
|---|-----------|---------|
| 1 | Cross-backend | Column stats and table column stats have consistency gaps across SQLite/PG/MySQL |
| 2 | SQLite | `initialize_schema()` DDL not in explicit transaction |
| 3 | validation.rs | DDL type validation is character-based, not grammar-based |

---

## Review 3: New/Changed Files

**Files**: `src/compaction_functions.rs`, `src/parse_values.rs`, `src/table_functions.rs`, `src/schema.rs`, `src/table_writer.rs`

### P1 Findings

| # | File:Line | Codex Claim | Validated? | Notes |
|---|-----------|-------------|------------|-------|
| 1 | table_writer.rs:1863 | `flush_inlined_data` executes in `call()` not `scan()` | **VALID P1** | Side-effectful operation runs during planning phase. Design choice but violates DataFusion execution model expectations. |
| 2 | parse_values.rs:244,267 | Decimal parse failures propagate error even in Lenient mode | **VALID P1** | `parse_decimal_string(s, *scale)?` uses `?` to propagate — not wrapped in lenient handling like other types. Lenient mode should produce nulls, not errors. |
| 3 | compaction_functions.rs:81 | `INSTALL ducklake` error swallowed in OnceLock | **VALID P1** | `let _ = conn.execute("INSTALL ducklake;", []);` — error discarded. Inside `OnceLock::get_or_init` so subsequent calls won't retry. |
| 4 | parse_values.rs:240 | Timestamp nanosecond `us * 1_000` overflow | **VALID P1** | No checked multiplication. Timestamps near i64::MAX/1000 would overflow. Realistic for extreme but valid timestamps. |

### P2 Findings

| # | File:Line | Finding |
|---|-----------|---------|
| 1 | table_writer.rs:1580 | `unwrap_or(i64::MAX)` silent saturation for column stats |

---

## Review 4: Core + Tests

**Files**: `src/catalog.rs`, `src/table.rs`, `src/delete_filter.rs`, `tests/cross_engine_tests.rs`, `tests/write_tests.rs`

### P1 Findings

| # | File:Line | Codex Claim | Validated? | Notes |
|---|-----------|-------------|------------|-------|
| 1 | catalog.rs:320 | `register_schema()` missing `with_catalog_snapshot_id()` | **VALID P1** | Line 328 calls `.with_writer()` but NOT `.with_catalog_snapshot_id()`. Compare with `schema()` at line 411-412 which correctly calls both. Newly registered schemas won't have snapshot context. |
| 2 | table.rs:957 | Partition pruning uses string comparison | **VALID P1** | `actual_value.as_deref() != Some(expected_value.as_str())` — string equality for all types. Numeric "1" vs "01" or "1.0" vs "1.00" would fail to match. |

### P2 Findings

| # | File:Line | Finding |
|---|-----------|---------|
| 1 | write_tests.rs | Tests assert only row counts, not column value correctness |
| 2 | cross_engine_tests.rs:948 | Skipped date verification comment |

---

## P0/P1 Validation Summary

### False Positives (2)

1. **merge_exec.rs:493-513** — Code has explicit cardinality check at line 499. The `break` at 512 is correct control flow after matching. Codex misread the logic.
2. **metadata_writer_validation.rs:352** — SET NOT NULL without data scan is a documented known limitation matching DuckDB's behavior. By design.

### Validated P1 Findings (10)

| Priority | Finding | Severity Rationale |
|----------|---------|-------------------|
| P1 | table_writer.rs:1692 `decode_decimal_bytes` panic on >16 bytes | Panic in production path; unlikely but not impossible with Decimal256 |
| P1 | table_writer.rs:302 unchecked i64 addition | Overflow requires extreme values; low probability |
| P1 | record_count can go negative | Metadata inconsistency propagation; low probability |
| P1 | replace_table_files unchecked sum | i64 overflow; astronomical file counts needed |
| P1 | table_writer.rs:1863 side effect in `call()` | Violates DataFusion execution model; functional but architecturally wrong |
| P1 | parse_values.rs:244,267 decimal errors in Lenient mode | Breaks Lenient contract — should null, not error |
| P1 | compaction_functions.rs:81 swallowed error in OnceLock | Silent failure on extension install; no retry possible |
| P1 | parse_values.rs:240 nanosecond overflow | Unchecked `us * 1_000` multiplication |
| P1 | catalog.rs:320 missing snapshot ID | register_schema lacks `with_catalog_snapshot_id()` |
| P1 | table.rs:957 string partition pruning | Type-insensitive comparison causes missed matches |

### Highest-Impact Items for R8

1. **parse_values.rs decimal Lenient mode** — Breaks expected contract; users on read path will get unexpected errors
2. **catalog.rs register_schema snapshot ID** — Functional gap vs `schema()` path
3. **table.rs partition pruning** — Correctness issue for non-string partition columns
4. **compaction_functions.rs OnceLock error** — Silent failure with no recovery path

---

## Codex P0 → P1 Downgrade Notes

No codex findings were rated P0 in R7. All severity-1 findings were already at P1 level. This is consistent with the codex tool's improved calibration in recent runs.

## Statistics

- **Total findings**: 4 reviews, 21 findings total
- **P1 findings**: 12 claimed → 10 validated (2 false positives)
- **P2 findings**: 9 (not individually validated per protocol)
- **False positive rate (P1)**: 16.7% (2/12) — significant improvement over R4-R6 ~90%+ rate
