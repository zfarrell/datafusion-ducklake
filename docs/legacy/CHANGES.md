# Extract Duplicated Writer Logic + Fix Catalog Schema Gaps

## Workstream 1: Catalog Schema Gap Fixes

### 1.1 Type promotion: `timestamp -> timestamptz`
**File**: `src/metadata_writer.rs`

Added `("timestamp", "timestamptz")` to `is_type_promotion_allowed()`.

### 1.2 Expanded `ColumnDef` struct
**File**: `src/metadata_writer.rs`

Added 5 `Option` fields for DuckLake forward compatibility:
- `initial_default: Option<String>`
- `default_value: Option<String>`
- `parent_column: Option<i64>`
- `default_value_type: Option<String>`
- `default_value_dialect: Option<String>`

Updated `new()` constructor to initialize all to `None`. `from_arrow()` inherits via `new()`.

### 1.3 DDL updates — all 3 writers
**Files**: `src/metadata_writer_sqlite.rs`, `src/metadata_writer_postgres.rs`, `src/metadata_writer_mysql.rs`

Added missing columns (all nullable or with defaults — backward compatible):

| Table | New Columns |
|-------|-------------|
| `ducklake_snapshot` | `schema_version DEFAULT 1`, `next_catalog_id DEFAULT 0`, `next_file_id DEFAULT 0` |
| `ducklake_column` | `initial_default`, `default_value`, `parent_column`, `default_value_type`, `default_value_dialect` |
| `ducklake_data_file` | `file_order`, `file_format DEFAULT 'PARQUET'`, `partition_id` |
| `ducklake_delete_file` | `format DEFAULT 'POSITION_DELETES'` |
| `ducklake_view` | `dialect DEFAULT 'SQL'` |

DB-specific type mappings applied per backend (e.g. `INTEGER` for SQLite, `BIGINT` for Postgres/MySQL, `VARCHAR(255)`/`VARCHAR(1024)` for MySQL).

### 1.4 Updated `ducklake_column` INSERT statements — all 3 writers
12 SQL changes (4 per writer) to bind the new `ColumnDef` fields:
- `write_transaction_inner` (column insert loop)
- `set_columns` (column insert loop)
- `alter_table` AddColumn, RenameColumn, AlterColumnType branches

For alter_table branches where no `ColumnDef` is available (RenameColumn, AlterColumnType), all 5 new fields are bound as `None`.

### 1.5 Fixed MySQL `sql_text` to `` `sql` ``
**File**: `src/metadata_writer_mysql.rs`

- DDL: `sql_text TEXT` changed to `` `sql` TEXT ``
- INSERT: `sql_text` changed to `` `sql` ``

This matches SQLite/Postgres and the shared `SQL_LIST_VIEWS`/`SQL_GET_VIEW_BY_NAME` constants. Backtick-quoting avoids MySQL reserved word conflict.

### 1.6 Refactored `ducklake_snapshot_changes` to match reference schema
**Files**: All 3 writers

**Problem**: Our `ducklake_snapshot_changes` had a completely different schema than the DuckLake reference. It stored machine-readable per-entity change tracking (`change_type`, `table_id`, `schema_id`) for optimistic concurrency control, while the reference stores human-readable audit info (`changes_made`, `author`, `commit_message`, `commit_extra_info`).

**Fix**: Two changes:

**A. Replaced `ducklake_snapshot_changes` with the reference schema:**
```sql
CREATE TABLE IF NOT EXISTS ducklake_snapshot_changes (
    snapshot_id BIGINT PRIMARY KEY,
    changes_made VARCHAR,
    author VARCHAR,
    commit_message VARCHAR,
    commit_extra_info VARCHAR
);
```

**B. Moved conflict detection to `_df_change_tracking`:**
```sql
CREATE TABLE IF NOT EXISTS _df_change_tracking (
    id INTEGER PRIMARY KEY,
    snapshot_id INTEGER NOT NULL,
    change_type TEXT NOT NULL,
    table_id INTEGER,
    schema_id INTEGER
);
```

In all 3 writers:
- DDL: Creates both tables
- Conflict detection queries (7 per writer): Changed from `ducklake_snapshot_changes` to `_df_change_tracking`
- Write operations (`drop_table_inner`, `drop_schema_inner`, `alter_table`): INSERT into both tables — `_df_change_tracking` gets machine-readable tracking, `ducklake_snapshot_changes` gets human-readable description

---

## Workstream 2: Extract Duplicated Validation Logic

### 2.1 New file: `src/metadata_writer_validation.rs`
Feature-gated: `#[cfg(feature = "write")]`, visibility: `pub(crate)`

**`ActiveColumnInfo`** struct — DB-independent parsed column row:
```rust
pub(crate) struct ActiveColumnInfo {
    pub column_id: i64,
    pub column_name: String,
    pub column_type: String,
    pub column_order: i64,
    pub is_nullable: bool,
}
```

**`AlterTableAction`** enum — validation result telling caller what SQL to execute:
- `InsertColumn { column_name, column_type, column_order, is_nullable }`
- `EndColumn { column_id }`
- `ReplaceColumn { end_column_id, column_name, column_type, column_order, is_nullable }`

**Extracted functions:**
- `validate_schema_evolution()` — replaces ~30 identical lines per writer in `write_transaction_inner`
- `validate_table_has_columns()`
- `validate_alter_table()` — dispatches to 4 private validators:
  - `validate_add_column` — nullable check, duplicate name check, next order calc
  - `validate_drop_column` — last column check, find target
  - `validate_rename_column` — find target, new name conflict check
  - `validate_alter_column_type` — find target, type promotion check

**Unit tests** (15 tests): All validation paths covered without any DB backend.

### 2.2 Registered module
**File**: `src/lib.rs`

Added `#[cfg(feature = "write")] pub(crate) mod metadata_writer_validation;`

### 2.3 Refactored all 3 writers

**`write_transaction_inner()`**: Replaced inline schema evolution validation (~30 lines) with one `validate_schema_evolution()` call.

**`alter_table()`**: Each writer's ~100-line 4-arm match (interleaved validation + SQL) became:
1. Parse `col_rows` into `Vec<ActiveColumnInfo>` (5-7 lines, stays per-backend)
2. `validate_table_has_columns(&columns)?;`
3. `let action = validate_alter_table(&columns, op)?;`
4. `match action { InsertColumn => ..., EndColumn => ..., ReplaceColumn => ... }` — pure SQL execution

**What stays per-backend (NOT extracted):**
- SQL parameter binding (`?` vs `$1/$2`)
- ID retrieval (`RETURNING` vs `LAST_INSERT_ID`)
- Conflict detection queries (DB-specific parameters)
- Row parsing into `ActiveColumnInfo` (trivial, 5 lines)

---

## Verification

- `cargo test --features write-sqlite,skip-tests-with-docker` — all tests pass
- `cargo check --features write-postgres` — clean
- `cargo check --features write-mysql` — clean
- `cargo clippy --features write-sqlite` — clean
- `cargo fmt --check` — clean

## Files Changed

| File | Changes |
|------|---------|
| `src/metadata_writer.rs` | Type promotion, ColumnDef expansion |
| `src/metadata_writer_sqlite.rs` | DDL + INSERTs + change tracking refactor + alter_table dedup |
| `src/metadata_writer_postgres.rs` | DDL + INSERTs + change tracking refactor + alter_table dedup |
| `src/metadata_writer_mysql.rs` | DDL + INSERTs + sql fix + change tracking refactor + alter_table dedup |
| `src/metadata_writer_validation.rs` | New file with all extracted validators + 15 unit tests |
| `src/lib.rs` | Module registration |
