# DuckDB + DuckLake Behavior Reference

Tested with DuckDB v1.4.4 and DuckLake extension v0.3 on Linux (Ubuntu).

## Setup

```sql
INSTALL ducklake;
LOAD ducklake;
-- ATTACH syntax: direct path with TYPE ducklake
ATTACH '/tmp/test.ducklake' AS my_catalog (TYPE ducklake);
USE my_catalog;
```

**Important**: The `ducklake:///path` URI syntax does NOT work. The correct form is a bare file path with `(TYPE ducklake)`.

The metadata is stored in the `.ducklake` file (a regular DuckDB database). Data files (Parquet) are stored in a sibling directory named `<file>.files/`.

---

## DDL Operations

### 1. CREATE SCHEMA

```sql
CREATE SCHEMA test_schema;
```

**Result**: Schema created successfully. Visible in `information_schema.schemata`:
```
schema_name: main, test_schema
```

A `main` schema is automatically created when attaching a new DuckLake catalog.

### 2. CREATE TABLE

Supported column types tested:

```sql
CREATE TABLE test_schema.type_test (
    id INTEGER,
    name VARCHAR,
    active BOOLEAN,
    price DECIMAL(10, 2),
    birth_date DATE,
    created_at TIMESTAMP,
    created_at_tz TIMESTAMPTZ,
    data BLOB,
    uid UUID,
    tags VARCHAR[],
    metadata STRUCT(key VARCHAR, val VARCHAR)
);
```

**Result**: All types created successfully. The `information_schema.columns` view reports:
| column_name | data_type |
|---|---|
| id | INTEGER |
| name | VARCHAR |
| active | BOOLEAN |
| price | DECIMAL(10,2) |
| birth_date | DATE |
| created_at | TIMESTAMP |
| created_at_tz | TIMESTAMP WITH TIME ZONE |
| data | BLOB |
| uid | UUID |
| tags | VARCHAR[] |
| metadata | STRUCT("key" VARCHAR, val VARCHAR) |

**Note for DataFusion**: The `key` column name in STRUCT is quoted because it is a reserved word. DataFusion must handle STRUCT field name quoting.

### 3. CREATE TABLE AS SELECT (CTAS)

```sql
CREATE TABLE test_schema.numbers AS SELECT i AS num, i * 2 AS doubled FROM range(1, 6) t(i);
```

**Result**: Table created with inferred types (both columns BIGINT from range function). Data immediately queryable.

### 4. ALTER TABLE ADD COLUMN

```sql
ALTER TABLE test_schema.numbers ADD COLUMN tripled INTEGER;
```

**Result**: Column added. Existing rows have NULL for the new column.

### 5. ALTER TABLE DROP COLUMN

```sql
ALTER TABLE test_schema.numbers DROP COLUMN tripled;
```

**Result**: Column removed. Only remaining columns visible.

### 6. ALTER TABLE RENAME COLUMN

```sql
ALTER TABLE test_schema.numbers RENAME COLUMN doubled TO times_two;
```

**Result**: Column renamed. Data access works with new name.

### 7. ALTER TABLE ALTER COLUMN TYPE

```sql
-- Starting with INTEGER column
ALTER TABLE test ALTER COLUMN x TYPE BIGINT;
```

**Result**: Column type changed from INTEGER to BIGINT. Existing data is preserved and readable. The column metadata is updated; existing Parquet files are read with type casting.

### 8. DROP TABLE

```sql
DROP TABLE test_schema.alter_type_test;
```

**Result**: Table removed from `information_schema.tables`.

### 9. DROP SCHEMA

```sql
-- Empty schema: succeeds
DROP SCHEMA drop_me;

-- Non-empty schema without CASCADE: FAILS
DROP SCHEMA test_schema;
-- Error: Cannot drop schema "test_schema" because there are entries that depend on it

-- Non-empty schema with CASCADE: succeeds
DROP SCHEMA test_schema CASCADE;
```

**Result**: Empty schema drops succeed. Non-empty schema requires `CASCADE`. All dependent tables are dropped.

### 10. CREATE VIEW

```sql
CREATE VIEW main.v1 AS SELECT a, b FROM main.t1 WHERE a > 1;
```

**Result**: View created. Appears in `information_schema.tables` with `table_type = 'VIEW'`. Queryable like a regular table.

### 11. ALTER TABLE RENAME

```sql
ALTER TABLE rename_me RENAME TO renamed_table;
```

**Result**: Table renamed. Old name no longer exists.

### 12. TRUNCATE TABLE

```sql
TRUNCATE TABLE trunc_test;
```

**Result**: All rows deleted. `COUNT(*)` returns 0.

### 13. CREATE TABLE IF NOT EXISTS / DROP TABLE IF EXISTS

```sql
CREATE TABLE IF NOT EXISTS tx_test (id INTEGER, val VARCHAR);  -- no error if exists
DROP TABLE IF EXISTS nonexistent_table;                        -- no error if missing
```

**Result**: Both work without errors.

---

## DML Operations

### 1. INSERT INTO

```sql
-- Single row
INSERT INTO employees VALUES (1, 'Alice', 75000.00, 'Engineering');

-- Multi-row
INSERT INTO employees VALUES
    (2, 'Bob', 80000.50, 'Engineering'),
    (3, 'Carol', 65000.00, 'Marketing');

-- INSERT ... SELECT
INSERT INTO eng_employees SELECT id, name, salary FROM employees WHERE dept = 'Engineering';
```

**Result**: All forms work. Each INSERT creates a new Parquet data file. Multi-row inserts go into a single file.

### 2. UPDATE

```sql
-- Simple WHERE
UPDATE employees SET salary = 85000.00 WHERE name = 'Alice';

-- Multiple columns
UPDATE employees SET salary = 95000.00, dept = 'Management' WHERE name = 'Dave';

-- Expression-based
UPDATE employees SET salary = salary * 1.10 WHERE dept = 'Engineering';
```

**Result**: All work. Updates create new data files with postimages and delete files with preimages. The change tracking shows `update_preimage` and `update_postimage` entries.

### 3. DELETE

```sql
-- With WHERE
DELETE FROM employees WHERE name = 'Carol';

-- All rows
DELETE FROM eng_employees;
```

**Result**: Both work. Deletes create delete files in the DuckLake metadata.

### 4. MERGE INTO

```sql
MERGE INTO target AS t
USING source AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET val = s.val, updated = true
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, true);
```

**Result**: Works correctly. Matched rows updated, unmatched rows inserted.

**Note for DataFusion**: MERGE INTO support is important for DuckLake interoperability.

---

## Time Travel

### Query at Version

```sql
-- Query historical data by snapshot version
SELECT * FROM main.employees AT (VERSION => 20) ORDER BY id;
```

**Result**: Returns data as it existed at snapshot 20.

### Query at Timestamp

```sql
SELECT * FROM main.employees AT (TIMESTAMP => TIMESTAMPTZ '2026-02-21 05:19:33.20609+00') ORDER BY id;
```

**Result**: Returns data as it existed at the given timestamp. The timestamp is matched to the closest snapshot.

**Note for DataFusion**: The `AT (VERSION => N)` and `AT (TIMESTAMP => ts)` syntax must be supported. The VERSION is a DuckLake snapshot ID (integer), and TIMESTAMP must be TIMESTAMPTZ.

---

## Metadata Functions

### ducklake_snapshots(catalog)

```sql
SELECT * FROM ducklake_snapshots('my_catalog');
```

**Result columns**: `snapshot_id`, `snapshot_time`, `schema_version`, `changes` (MAP), `author`, `commit_message`, `commit_extra_info`

The `changes` column is a `MAP(VARCHAR, VARCHAR[])` with keys like:
- `schemas_created`, `schemas_dropped`
- `tables_created`, `tables_dropped`, `tables_altered`
- `tables_inserted_into`, `tables_deleted_from`
- `views_created`

### ducklake_table_info(catalog)

```sql
SELECT * FROM ducklake_table_info('my_catalog');
```

**Result columns**: `table_name`, `schema_id`, `table_id`, `table_uuid`, `file_count`, `file_size_bytes`, `delete_file_count`, `delete_file_size_bytes`

**Note**: Takes only ONE argument (catalog name), not two.

### ducklake_list_files(catalog, table_name)

```sql
SELECT * FROM ducklake_list_files('my_catalog', 'employees');
```

**Result columns**: `data_file`, `data_file_size_bytes`, `data_file_footer_size`, `data_file_encryption_key`, `delete_file`, `delete_file_size_bytes`, `delete_file_footer_size`, `delete_file_encryption_key`

Data files are Parquet files stored at paths like:
`/tmp/test.ducklake.files/main/<table_name>/ducklake-<uuid>.parquet`

### ducklake_table_changes(catalog, schema, table, start_snapshot, end_snapshot)

```sql
SELECT * FROM ducklake_table_changes('my_catalog', 'main', 'employees', 19, 26);
```

**Result columns**: `snapshot_id`, `rowid`, `change_type`, plus all table columns

Change types:
- `insert` -- new row added
- `delete` -- row removed
- `update_preimage` -- row state before update
- `update_postimage` -- row state after update

**Note**: Requires 5 arguments: catalog, schema_name, table_name, start_snapshot (BIGINT), end_snapshot (BIGINT).

### ducklake_table_insertions(catalog, schema, table, start_snapshot, end_snapshot)

```sql
SELECT * FROM ducklake_table_insertions('my_catalog', 'main', 'tx_test', 0, 24);
```

Returns only the inserted rows between the snapshot range.

### ducklake_table_deletions(catalog, schema, table, start_snapshot, end_snapshot)

```sql
SELECT * FROM ducklake_table_deletions('my_catalog', 'main', 'tx_test', 0, 24);
```

Returns only the deleted rows between the snapshot range.

### ducklake_current_snapshot(catalog) / ducklake_last_committed_snapshot(catalog)

```sql
-- Both are TABLE functions, not scalar
SELECT * FROM ducklake_current_snapshot('my_catalog');
SELECT * FROM ducklake_last_committed_snapshot('my_catalog');
```

Returns `id` (UINT64) of the current/last committed snapshot.

### ducklake_set_commit_message(catalog, author, message)

```sql
-- Must be set INSIDE a transaction BEFORE committing
BEGIN;
CALL ducklake_set_commit_message('my_catalog', 'author_name', 'commit message');
INSERT INTO my_table VALUES (...);
COMMIT;
```

**Result**: The snapshot created by the COMMIT will have the author and message fields populated.

### ducklake_options(catalog)

```sql
SELECT * FROM ducklake_options('my_catalog');
```

Returns global and scoped options:
| option_name | value | scope |
|---|---|---|
| created_by | DuckDB 6ddac802ff | GLOBAL |
| data_path | /path/to/files/ | GLOBAL |
| encrypted | false | GLOBAL |
| version | 0.3 | GLOBAL |

---

## Maintenance Functions

### ducklake_merge_adjacent_files(catalog, table_name)

```sql
CALL ducklake_merge_adjacent_files('my_catalog', 'tx_test');
```

**Result**: Merges small Parquet files into larger ones. In testing, 3 files were merged into 2. Data remains intact and queryable.

Optional named parameters: `max_compacted_files`, `schema`, `max_file_size`, `min_file_size`.

### ducklake_expire_snapshots(catalog, older_than)

```sql
CALL ducklake_expire_snapshots('my_catalog', older_than := TIMESTAMPTZ '2026-02-21T05:21:00Z');
```

**Result**: Reduced snapshot count from 45 to 33 by expiring old snapshots. Uses named parameter `older_than`.

Optional: `dry_run` (BOOLEAN), `versions` (UBIGINT[]).

### ducklake_cleanup_old_files(catalog)

```sql
CALL ducklake_cleanup_old_files('my_catalog');
```

**Result**: Deletes orphaned data files from disk that are no longer referenced by any snapshot.

### ducklake_flush_inlined_data(catalog)

```sql
CALL ducklake_flush_inlined_data('my_catalog');
```

**Result**: Forces inlined data to be written to Parquet files. (Small inserts may be inlined in metadata.)

---

## Type System Details

### Supported Types

| DuckDB Type | Parquet Roundtrip | Notes |
|---|---|---|
| TINYINT | Yes | 8-bit signed integer |
| SMALLINT | Yes | 16-bit signed integer |
| INTEGER | Yes | 32-bit signed integer |
| BIGINT | Yes | 64-bit signed integer |
| HUGEINT | **LOSSY** | Stored as DOUBLE in Parquet; large values lose precision. Value `123456789012345678` read back as `123456789012345680`. Max HUGEINT causes read error. |
| FLOAT | Yes | 32-bit IEEE 754 |
| DOUBLE | Yes | 64-bit IEEE 754 |
| DECIMAL(p,s) | Yes | Tested up to DECIMAL(38,10). Full precision preserved. |
| BOOLEAN | Yes | true/false/NULL |
| VARCHAR | Yes | Tested with 10,000-character strings |
| BLOB | Yes | Binary data stored correctly |
| DATE | Yes | |
| TIMESTAMP | Yes | No timezone info stored |
| TIMESTAMPTZ | Yes | Stored as UTC. Input `10:30:00+05:00` becomes `05:30:00+00` |
| INTERVAL | Yes | `1 year 2 months 3 days` roundtrips correctly |
| UUID | Yes | |
| VARCHAR[] | Yes | Lists/arrays supported |
| INTEGER[] | Yes | |
| STRUCT(...) | Yes | Nested field access works: `info.name` |
| MAP(K,V) | Yes | `MAP {'key': 'value'}` syntax |

### Unsupported Types

| Type | Error |
|---|---|
| ENUM (user-defined types) | `Not implemented Error: DuckLake does not support user-defined types` |

### NULL Handling

- NULLs work in all column types
- `IS NULL` / `IS NOT NULL` filters work correctly
- NULL primary key columns are allowed (since PK constraints not supported)
- After ALTER TABLE ADD COLUMN, existing rows have NULL for the new column

### TIMESTAMPTZ Behavior

Timestamps with time zones are normalized to UTC on storage:
```sql
INSERT INTO ts_test VALUES ('2024-01-15 10:30:00+05:00');
-- Stored and returned as: 2024-01-15 05:30:00+00
```

**Note for DataFusion**: DataFusion's TIMESTAMP WITH TIME ZONE must normalize to UTC when writing Parquet for DuckLake compatibility.

---

## Constraints

### Supported

| Constraint | Status |
|---|---|
| NOT NULL | Supported. Enforced on INSERT/UPDATE. Error: `Constraint Error: NOT NULL constraint failed: table.column` |
| DEFAULT | Supported. Default values are applied on INSERT when column is omitted. Visible in `information_schema.columns.column_default`. |

### Not Supported

| Constraint | Error |
|---|---|
| PRIMARY KEY | `Not implemented Error: PRIMARY KEY/UNIQUE constraints are not supported in DuckLake` |
| UNIQUE | `Not implemented Error: PRIMARY KEY/UNIQUE constraints are not supported in DuckLake` |
| CHECK | `Not implemented Error: CHECK constraints are not supported in DuckLake` |
| FOREIGN KEY | Fails because it requires PRIMARY KEY/UNIQUE on referenced table |

**Note for DataFusion**: Only NOT NULL and DEFAULT constraints need to be supported for DuckLake compatibility. PK/UNIQUE/CHECK/FK are not available.

---

## Transaction Semantics

### BEGIN / COMMIT / ROLLBACK

```sql
BEGIN;
INSERT INTO tx_test VALUES (1, 'committed');
COMMIT;  -- Data visible after commit

BEGIN;
INSERT INTO tx_test VALUES (2, 'will_rollback');
-- Data visible WITHIN the transaction
ROLLBACK;  -- Data is NOT visible after rollback
```

**Result**: Full transaction support. Each committed transaction creates a new snapshot. Rolled-back transactions leave no trace.

**Note for DataFusion**: Transaction support is critical. Each DML operation within a committed transaction creates a snapshot entry.

---

## Schema Evolution

### Add Column

```sql
CREATE TABLE evolution_test (id INTEGER, name VARCHAR);
INSERT INTO evolution_test VALUES (1, 'Alice'), (2, 'Bob');
ALTER TABLE evolution_test ADD COLUMN age INTEGER;
INSERT INTO evolution_test VALUES (3, 'Carol', 30);
SELECT * FROM evolution_test;
```

**Result**:
```
id | name  | age
1  | Alice | NULL
2  | Bob   | NULL
3  | Carol | 30
```

Old Parquet files are read with the new schema; missing columns filled with NULL.

### Rename Column

```sql
ALTER TABLE evolution_test RENAME COLUMN name TO full_name;
SELECT * FROM evolution_test;
```

**Result**: Column renamed. Old data accessible via new name. DuckLake uses column IDs internally, not names.

### Change Column Type

```sql
ALTER TABLE evolution_test ALTER COLUMN id TYPE BIGINT;
```

**Result**: Type changed from INTEGER to BIGINT. Data remains readable. The metadata tracks the new type, and Parquet files are read with type casting.

**Note for DataFusion**: Schema evolution is a core DuckLake feature. The metadata DB tracks column IDs and type history. DataFusion must handle reading Parquet files with different schemas than the current table definition.

---

## Partitioning

**Status**: Partitioning via SQL `PARTITION BY` syntax is NOT supported in DuckLake as of v0.3.

```sql
CREATE TABLE t (...) PARTITION BY (col);
-- Parser Error: syntax error at or near "PARTITION"
```

The `ducklake_set_option` function also does not support a `partition_columns` option:
```
Not implemented Error: Unsupported option partition_columns
```

However, the internal metadata DB has `ducklake_partition_column` and `ducklake_partition_info` tables, suggesting this feature is planned.

---

## Information Schema

### Schemata

```sql
SELECT * FROM information_schema.schemata WHERE catalog_name = 'my_catalog';
```

Returns: `catalog_name`, `schema_name`, `schema_owner` (always "duckdb")

### Tables

```sql
SELECT table_catalog, table_schema, table_name, table_type
FROM information_schema.tables WHERE table_catalog = 'my_catalog';
```

`table_type` values: `BASE TABLE`, `VIEW`

### Columns

```sql
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns WHERE table_catalog = 'my_catalog' AND table_name = 'my_table';
```

- `is_nullable`: `YES` or `NO`
- `column_default`: String representation (e.g., `'unknown'`) or `NULL`

---

## Internal Metadata Structure

The `.ducklake` file is a regular DuckDB database containing these tables:

| Table | Purpose |
|---|---|
| `ducklake_schema` | Schema definitions with `begin_snapshot`/`end_snapshot` |
| `ducklake_table` | Table definitions with `table_uuid`, paths, snapshot ranges |
| `ducklake_column` | Column definitions with types, defaults, nullability, `parent_column` for nested types |
| `ducklake_view` | View definitions |
| `ducklake_snapshot` | Snapshot metadata: `snapshot_id`, `snapshot_time`, `schema_version`, `next_catalog_id`, `next_file_id` |
| `ducklake_snapshot_changes` | Change tracking per snapshot |
| `ducklake_data_file` | Data file references (Parquet paths) |
| `ducklake_delete_file` | Delete file references |
| `ducklake_file_column_stats` | Per-file column statistics (min/max) |
| `ducklake_table_column_stats` | Aggregated table column statistics |
| `ducklake_table_stats` | Table-level statistics |
| `ducklake_column_mapping` | Maps column IDs across schema versions |
| `ducklake_name_mapping` | Name-to-ID mapping for schema evolution |
| `ducklake_metadata` | Global options (version, data_path, encrypted, created_by) |
| `ducklake_inlined_data_tables` | Tables with data inlined in metadata |
| `ducklake_files_scheduled_for_deletion` | Garbage collection tracking |
| `ducklake_partition_column` | Partition column definitions (future use) |
| `ducklake_partition_info` | Partition metadata (future use) |
| `ducklake_file_partition_value` | Per-file partition values (future use) |
| `ducklake_tag` | Tag definitions |
| `ducklake_column_tag` | Column-level tags |
| `ducklake_schema_versions` | Schema version history |

### Key Design Points

1. **Column IDs**: Columns are identified by `column_id` (not name), enabling rename without data rewrite.
2. **Snapshot Ranges**: Schemas, tables, and columns have `begin_snapshot`/`end_snapshot` to track when they were created/dropped.
3. **Relative Paths**: File paths use `path_is_relative = true` so the data directory can be relocated.
4. **UUIDs**: Tables have UUIDs (`table_uuid`) for cross-system identification.
5. **File Layout**: Data files are stored as `<data_path>/<schema>/<table>/<filename>.parquet` with UUID-based filenames.

---

## Data File Structure

- **Location**: `<ducklake_file>.files/<schema>/<table>/ducklake-<uuid>.parquet`
- **Format**: Standard Parquet files readable by any Parquet-compatible tool
- **New files per operation**: Each INSERT/UPDATE/DELETE creates new Parquet file(s)
- **Delete tracking**: Updates create both new data files (postimages) and delete files (preimages)
- **Footer size**: Tracked separately in metadata for efficient reads
- **Encryption**: Optional (controlled via `encrypted` option); encryption key tracked per file

---

## Key Findings for DataFusion Implementation

### Critical Behaviors to Replicate

1. **ATTACH syntax**: `ATTACH 'path.ducklake' AS name (TYPE ducklake)` -- not URI-based
2. **Automatic Parquet file management**: Each DML creates new files; never modifies existing ones
3. **Schema evolution via column IDs**: Renamed/retyped columns still read old Parquet files correctly
4. **Snapshot-per-commit**: Every committed transaction creates exactly one snapshot
5. **Time travel**: `AT (VERSION => N)` and `AT (TIMESTAMP => ts)` must be supported
6. **NOT NULL as only constraint**: PK/UNIQUE/CHECK/FK are all unsupported
7. **TIMESTAMPTZ normalization**: Always stored/returned as UTC

### Surprising Behaviors / Edge Cases

1. **HUGEINT precision loss**: Large HUGEINT values are stored as DOUBLE in Parquet, causing precision loss. Values exceeding DOUBLE range cause read errors.
2. **No partitioning yet**: Despite metadata tables existing for it, partitioning is not implemented in v0.3.
3. **No ENUM/user-defined types**: Custom types are explicitly not supported.
4. **commit_message requires transaction**: `ducklake_set_commit_message` must be called inside a `BEGIN`/`COMMIT` block to take effect.
5. **ducklake_table_info takes 1 arg**: Only catalog name, not catalog + table.
6. **Table functions, not scalars**: `ducklake_current_snapshot` and `ducklake_last_committed_snapshot` are table functions (used in FROM clause).
7. **COPY TO works**: Data can be exported via COPY TO from DuckLake tables.
8. **DEFAULT values work**: Column defaults are supported and applied correctly.
9. **Data inlining**: Small data may be inlined in the metadata DB and flushed to Parquet via `ducklake_flush_inlined_data`.
10. **File compaction**: `ducklake_merge_adjacent_files` compacts small files; crucial for write-heavy workloads.
