//! SQLite implementation of [`MetadataWriter`].
//!
//! Requires multi-threaded Tokio runtime (`#[tokio::test(flavor = "multi_thread")]`).

use std::sync::Arc;

use crate::Result;
use crate::dialect::{SqlDialect, SqliteDialect};
use crate::error::DuckLakeError;
use crate::metadata_provider::{InlinedDataRow, block_on};
use crate::metadata_writer::{
    AlterTableOp, ColumnDef, MetadataWriter, WriteMode, WriteSetupResult,
};
use crate::metadata_writer_validation::{
    ActiveColumnInfo, AlterTableAction, validate_alter_table, validate_ducklake_type_for_ddl,
    validate_no_duplicate_columns, validate_schema_evolution, validate_table_has_columns,
};
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use std::str::FromStr;

const DEFAULT_MAX_CONNECTIONS: u32 = 5;

/// Maximum number of retries for SQLITE_BUSY errors.
const BUSY_RETRY_MAX_ATTEMPTS: u32 = 20;

/// Initial backoff delay for SQLITE_BUSY retries (in milliseconds).
const BUSY_RETRY_INITIAL_DELAY_MS: u64 = 10;

/// Check whether a [`DuckLakeError`] wraps an SQLITE_BUSY error.
///
/// SQLite uses error code 5 for SQLITE_BUSY and 517 for SQLITE_BUSY_SNAPSHOT
/// (which occurs in WAL mode when a transaction can't commit because the
/// database was modified after the transaction started).
fn is_sqlite_busy(err: &DuckLakeError) -> bool {
    if let DuckLakeError::Sqlx(sqlx::Error::Database(db_err)) = err {
        if let Some(code) = db_err.code() {
            if code.as_ref() == "5" || code.as_ref() == "517" {
                return true;
            }
        }
        // Fallback to error message check
        return db_err.message().contains("database is locked");
    }
    false
}

/// Execute an async closure via [`block_on`], retrying with exponential backoff
/// and jitter when the operation fails with SQLITE_BUSY.
///
/// This is necessary because SQLite only supports a single concurrent writer.
/// Under concurrent load, transactions may fail with "database is locked"
/// even when `busy_timeout` is set, particularly due to SQLITE_BUSY_SNAPSHOT
/// errors in WAL mode that bypass the busy handler.
///
/// The jitter prevents thundering-herd collisions when many writers retry
/// at the same backoff intervals.
fn block_on_with_retry<F, Fut, T>(mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0u32;
    loop {
        match block_on(f()) {
            Ok(val) => return Ok(val),
            Err(e) if is_sqlite_busy(&e) && attempt < BUSY_RETRY_MAX_ATTEMPTS => {
                attempt += 1;
                // Exponential backoff: 20, 40, 80, ..., capped at 640ms base
                let base_ms = BUSY_RETRY_INITIAL_DELAY_MS * (1u64 << attempt.min(6));
                // Add random jitter (0 to base_ms) to avoid thundering herd
                let jitter_ms = {
                    // Simple per-thread jitter using thread id + attempt as seed
                    let tid = std::thread::current().id();
                    let hash = {
                        use std::hash::{Hash, Hasher};
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        tid.hash(&mut h);
                        attempt.hash(&mut h);
                        h.finish()
                    };
                    hash % base_ms.max(1)
                };
                let delay = std::time::Duration::from_millis(base_ms + jitter_ms);
                // Use std::thread::sleep to avoid blocking the tokio runtime
                std::thread::sleep(delay);
            },
            Err(e) => return Err(e),
        }
    }
}

const SQL_CREATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_metadata (
    key VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    scope VARCHAR,
    scope_id INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_snapshot (
    snapshot_id INTEGER PRIMARY KEY,
    snapshot_time TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f+00:00', 'now')),
    schema_version INTEGER DEFAULT 1,
    next_catalog_id INTEGER DEFAULT 0,
    next_file_id INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ducklake_schema (
    schema_id INTEGER PRIMARY KEY,
    schema_uuid VARCHAR,
    schema_name VARCHAR NOT NULL,
    path VARCHAR NOT NULL DEFAULT '',
    path_is_relative BOOLEAN NOT NULL DEFAULT 1,
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_table (
    table_id INTEGER NOT NULL,
    table_uuid VARCHAR,
    schema_id INTEGER NOT NULL,
    table_name VARCHAR NOT NULL,
    path VARCHAR NOT NULL DEFAULT '',
    path_is_relative BOOLEAN NOT NULL DEFAULT 1,
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_column (
    column_id INTEGER NOT NULL,
    table_id INTEGER NOT NULL,
    column_name VARCHAR NOT NULL,
    column_type VARCHAR NOT NULL,
    column_order INTEGER NOT NULL,
    nulls_allowed BOOLEAN DEFAULT 1,
    initial_default VARCHAR,
    default_value VARCHAR,
    parent_column INTEGER,
    default_value_type VARCHAR,
    default_value_dialect VARCHAR,
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_data_file (
    data_file_id INTEGER PRIMARY KEY,
    table_id INTEGER NOT NULL,
    path VARCHAR NOT NULL,
    path_is_relative BOOLEAN NOT NULL DEFAULT 1,
    file_size_bytes INTEGER NOT NULL,
    footer_size INTEGER,
    encryption_key VARCHAR,
    record_count INTEGER,
    row_id_start INTEGER,
    mapping_id INTEGER,
    file_order INTEGER,
    file_format VARCHAR DEFAULT 'parquet',
    partition_id INTEGER,
    partial_file_info VARCHAR,
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_delete_file (
    delete_file_id INTEGER PRIMARY KEY,
    data_file_id INTEGER NOT NULL,
    table_id INTEGER NOT NULL,
    path VARCHAR NOT NULL,
    path_is_relative BOOLEAN NOT NULL DEFAULT 1,
    file_size_bytes INTEGER NOT NULL,
    footer_size INTEGER,
    encryption_key VARCHAR,
    delete_count INTEGER,
    format VARCHAR DEFAULT 'parquet',
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_snapshot_changes (
    snapshot_id INTEGER PRIMARY KEY,
    changes_made VARCHAR,
    author VARCHAR,
    commit_message VARCHAR,
    commit_extra_info VARCHAR
);

-- R6-S-030: DataFusion-specific tables use _df_ prefix to avoid conflicts with DuckLake catalog tables.
CREATE TABLE IF NOT EXISTS _df_change_tracking (
    id INTEGER PRIMARY KEY,
    snapshot_id INTEGER NOT NULL,
    change_type TEXT NOT NULL,
    table_id INTEGER,
    schema_id INTEGER
);

-- R6-S-030: The ducklake_ prefix on the following tables matches DuckDB's catalog schema exactly.
-- This ensures cross-engine interoperability: DuckDB expects these standard DuckLake table names.
CREATE TABLE IF NOT EXISTS ducklake_file_column_stats (
    data_file_id INTEGER NOT NULL,
    table_id INTEGER NOT NULL,
    column_id INTEGER NOT NULL,
    column_size_bytes INTEGER,
    value_count INTEGER,
    null_count INTEGER,
    min_value VARCHAR,
    max_value VARCHAR,
    contains_nan BOOLEAN,
    extra_stats VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_view (
    view_id INTEGER NOT NULL,
    view_uuid VARCHAR,
    schema_id INTEGER NOT NULL,
    view_name VARCHAR NOT NULL,
    dialect VARCHAR,
    sql VARCHAR NOT NULL,
    column_aliases VARCHAR,
    begin_snapshot INTEGER NOT NULL,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_tag (
    object_id INTEGER,
    begin_snapshot INTEGER,
    end_snapshot INTEGER,
    key VARCHAR,
    value VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_column_tag (
    table_id INTEGER,
    column_id INTEGER,
    begin_snapshot INTEGER,
    end_snapshot INTEGER,
    key VARCHAR,
    value VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_table_stats (
    table_id INTEGER PRIMARY KEY,
    record_count INTEGER,
    next_row_id INTEGER,
    file_size_bytes INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_table_column_stats (
    table_id INTEGER,
    column_id INTEGER,
    contains_null BOOLEAN,
    contains_nan BOOLEAN,
    min_value VARCHAR,
    max_value VARCHAR,
    extra_stats VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_partition_info (
    partition_id INTEGER,
    table_id INTEGER,
    begin_snapshot INTEGER,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_partition_column (
    partition_id INTEGER,
    table_id INTEGER,
    partition_key_index INTEGER,
    column_id INTEGER,
    transform VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_file_partition_value (
    data_file_id INTEGER,
    table_id INTEGER,
    partition_key_index INTEGER,
    partition_value VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_files_scheduled_for_deletion (
    data_file_id INTEGER,
    path VARCHAR,
    path_is_relative BOOLEAN,
    schedule_start TEXT  -- R4-S-043/R6-S-031: TEXT for cross-engine compat; ISO 8601 UTC (e.g. '2024-01-15T10:30:00Z'). DuckDB uses TIMESTAMPTZ.
);

-- R6-S-030: ducklake_ prefix matches DuckDB's catalog naming convention for interoperability.
CREATE TABLE IF NOT EXISTS ducklake_inlined_data_tables (
    table_id INTEGER,
    table_name VARCHAR,
    schema_version INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_column_mapping (
    mapping_id INTEGER,
    table_id INTEGER,
    type VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_name_mapping (
    mapping_id INTEGER,
    column_id INTEGER,
    source_name VARCHAR,
    target_field_id INTEGER,
    parent_column INTEGER,
    is_partition BOOLEAN
);

CREATE TABLE IF NOT EXISTS ducklake_schema_versions (
    begin_snapshot INTEGER,
    schema_version INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_macro (
    schema_id INTEGER,
    macro_id INTEGER,
    macro_name VARCHAR,
    begin_snapshot INTEGER,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_macro_impl (
    macro_id INTEGER,
    impl_id INTEGER,
    dialect VARCHAR,
    sql VARCHAR,
    type VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_macro_parameters (
    macro_id INTEGER,
    impl_id INTEGER,
    column_id INTEGER,
    parameter_name VARCHAR,
    parameter_type VARCHAR,
    default_value VARCHAR,
    default_value_type VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_sort_info (
    sort_id INTEGER,
    table_id INTEGER,
    begin_snapshot INTEGER,
    end_snapshot INTEGER
);

CREATE TABLE IF NOT EXISTS ducklake_sort_expression (
    sort_id INTEGER,
    table_id INTEGER,
    sort_key_index INTEGER,
    expression VARCHAR,
    dialect VARCHAR,
    sort_direction VARCHAR,
    null_order VARCHAR
);

CREATE TABLE IF NOT EXISTS ducklake_file_variant_stats (
    data_file_id INTEGER,
    table_id INTEGER,
    column_id INTEGER,
    variant_path VARCHAR,
    shredded_type VARCHAR,
    column_size_bytes INTEGER,
    value_count INTEGER,
    null_count INTEGER,
    min_value VARCHAR,
    max_value VARCHAR,
    contains_nan BOOLEAN,
    extra_stats VARCHAR
);
"#;

/// SQLite-based metadata writer for DuckLake catalogs.
///
/// **Concurrency note:** SQLite uses a single-writer model (WAL mode allows
/// concurrent readers but only one writer at a time). All MAX+1 ID queries
/// run inside transactions, which is safe because SQLite serializes writes.
/// Concurrent writers to the same SQLite database file from separate processes
/// are NOT supported and may produce duplicate IDs or lock errors.
#[derive(Debug, Clone)]
pub struct SqliteMetadataWriter {
    pool: SqlitePool,
}

impl SqliteMetadataWriter {
    pub async fn new(connection_string: &str) -> Result<Self> {
        Self::with_max_connections(connection_string, DEFAULT_MAX_CONNECTIONS).await
    }

    pub async fn with_max_connections(
        connection_string: &str,
        max_connections: u32,
    ) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(connection_string)?
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(30));
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await?;
        Ok(Self {
            pool,
        })
    }

    pub async fn new_with_init(connection_string: &str) -> Result<Self> {
        let writer = Self::new(connection_string).await?;
        writer.initialize_schema()?;
        Ok(writer)
    }

    /// Inner write transaction logic, usable with an existing transaction.
    /// Creates snapshot, schema, table, columns and commits.
    async fn write_transaction_inner(
        mut tx: sqlx::Transaction<'_, sqlx::Sqlite>,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
    ) -> Result<WriteSetupResult> {
        // Check if schema exists
        let existing_schema = sqlx::query(
            "SELECT schema_id FROM ducklake_schema
             WHERE schema_name = ? AND end_snapshot IS NULL",
        )
        .bind(schema_name)
        .fetch_optional(&mut *tx)
        .await?;

        let schema_exists = existing_schema.is_some();
        let mut table_exists = false;

        // Check if table exists (if schema exists)
        let existing_table = if let Some(ref s_row) = existing_schema {
            let sid: i64 = s_row.try_get(0)?;
            let t = sqlx::query(
                "SELECT table_id FROM ducklake_table
                 WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL",
            )
            .bind(sid)
            .bind(table_name)
            .fetch_optional(&mut *tx)
            .await?;
            table_exists = t.is_some();
            t
        } else {
            None
        };

        let is_ddl = !schema_exists || !table_exists;

        // Get current schema_version for the new snapshot (F-012)
        let prev_sv_row =
            sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                .fetch_one(&mut *tx)
                .await?;
        let prev_schema_version: i64 = prev_sv_row.try_get(0)?;

        let new_schema_version = if is_ddl {
            prev_schema_version + 1
        } else {
            prev_schema_version
        };

        // Create snapshot with correct schema_version
        let row = sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (strftime('%Y-%m-%d %H:%M:%f+00:00', 'now'), ?) RETURNING snapshot_id",
        )
        .bind(new_schema_version)
        .fetch_one(&mut *tx)
        .await?;
        let snapshot_id: i64 = row.try_get(0)?;

        // Record schema_version change if DDL (F-012)
        if is_ddl {
            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version)
                 VALUES (?, ?)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;
        }

        // Create schema if needed (F-026: generate UUID)
        let schema_id: i64 = if let Some(s_row) = existing_schema {
            s_row.try_get(0)?
        } else {
            let schema_path = format!("{}/", schema_name);
            let schema_uuid = uuid::Uuid::new_v4().to_string();
            let row = sqlx::query(
                "INSERT INTO ducklake_schema (schema_uuid, schema_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, 1, ?) RETURNING schema_id",
            )
            .bind(&schema_uuid)
            .bind(schema_name)
            .bind(&schema_path)
            .bind(snapshot_id)
            .fetch_one(&mut *tx)
            .await?;
            row.try_get(0)?
        };

        // Create table if needed (F-026: generate UUID)
        let table_id: i64 = if let Some(t_row) = existing_table {
            t_row.try_get(0)?
        } else {
            let next_tid_row =
                sqlx::query("SELECT COALESCE(MAX(table_id), 0) + 1 FROM ducklake_table")
                    .fetch_one(&mut *tx)
                    .await?;
            let next_table_id: i64 = next_tid_row.try_get(0)?;

            let table_path = format!("{}/", table_name);
            let table_uuid = uuid::Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, 1, ?)",
            )
            .bind(next_table_id)
            .bind(&table_uuid)
            .bind(schema_id)
            .bind(table_name)
            .bind(&table_path)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;
            next_table_id
        };

        // Get existing columns with IDs for schema comparison (F-013)
        let col_rows = sqlx::query(
            "SELECT column_id, column_name, column_type, nulls_allowed
             FROM ducklake_column
             WHERE table_id = ? AND end_snapshot IS NULL
             ORDER BY column_order",
        )
        .bind(table_id)
        .fetch_all(&mut *tx)
        .await?;

        let mut existing_columns: Vec<(String, String, bool)> = Vec::with_capacity(col_rows.len());
        let mut existing_column_ids: Vec<i64> = Vec::with_capacity(col_rows.len());
        for row in &col_rows {
            let col_id: i64 = row.try_get(0)?;
            let name: String = row.try_get(1)?;
            let col_type: String = row.try_get(2)?;
            let nullable: bool = row.try_get::<Option<bool>, _>(3)?.unwrap_or(true);
            existing_column_ids.push(col_id);
            existing_columns.push((name, col_type, nullable));
        }

        validate_no_duplicate_columns(columns)?;
        validate_schema_evolution(&existing_columns, columns, mode)?;

        // F-013: Check if schema is identical — if so, preserve column IDs
        let schema_matches = existing_columns.len() == columns.len()
            && existing_columns.iter().zip(columns.iter()).all(
                |((name, col_type, nullable), col)| {
                    name == &col.name
                        && col_type == &col.ducklake_type
                        && *nullable == col.is_nullable
                },
            );

        let column_ids = if schema_matches && !existing_columns.is_empty() {
            // Schema unchanged — reuse existing column IDs
            existing_column_ids
        } else {
            // Schema changed or new table — end existing columns and create new ones
            sqlx::query(
                "UPDATE ducklake_column SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Column IDs are allocated per-table in SQLite, matching DuckDB's DuckLake
            // convention. This differs from PostgreSQL and MySQL backends which use
            // globally unique column IDs (via sequences/auto-increment). The per-table
            // approach is safe because column_id is always scoped by table_id in queries.
            let next_cid_row = sqlx::query(
                "SELECT COALESCE(MAX(column_id), 0) + 1 FROM ducklake_column WHERE table_id = ?",
            )
            .bind(table_id)
            .fetch_one(&mut *tx)
            .await?;
            let next_column_id: i64 = next_cid_row.try_get(0)?;

            let mut new_ids = Vec::with_capacity(columns.len());
            for (order, col) in columns.iter().enumerate() {
                let column_id = next_column_id + order as i64;
                sqlx::query(
                    "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, initial_default, default_value, parent_column, default_value_type, default_value_dialect, begin_snapshot)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(column_id)
                .bind(table_id)
                .bind(&col.name)
                .bind(&col.ducklake_type)
                .bind((order + 1) as i64)
                .bind(col.is_nullable)
                .bind(&col.initial_default)
                .bind(&col.default_value)
                .bind(col.parent_column)
                .bind(&col.default_value_type)
                .bind(&col.default_value_dialect)
                .bind(snapshot_id)
                .execute(&mut *tx)
                .await?;
                new_ids.push(column_id);
            }
            new_ids
        };

        // Record in snapshot_changes with DuckDB-compatible format (F-027)
        if !table_exists {
            // R3F-014: Include created_schema if schema was also new
            let esc_schema = schema_name.replace('"', "\"\"");
            let esc_table = table_name.replace('"', "\"\"");
            let changes = if !schema_exists {
                format!(
                    "created_schema:\"{}\",created_table:\"{}\".\"{}\"",
                    esc_schema, esc_schema, esc_table
                )
            } else {
                format!("created_table:\"{}\".\"{}\"", esc_schema, esc_table)
            };
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON CONFLICT(snapshot_id) DO UPDATE SET changes_made = excluded.changes_made",
            )
            .bind(snapshot_id)
            .bind(&changes)
            .execute(&mut *tx)
            .await?;
        } else {
            // R3F-013: Record inserted_into_table for append to existing table
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON CONFLICT(snapshot_id) DO UPDATE SET changes_made = excluded.changes_made",
            )
            .bind(snapshot_id)
            .bind(format!("inserted_into_table:{}", table_id))
            .execute(&mut *tx)
            .await?;
        }

        // R3F-011: Update snapshot with next_catalog_id and next_file_id
        sqlx::query(
            "UPDATE ducklake_snapshot
             SET next_catalog_id = COALESCE((SELECT MAX(v) + 1 FROM (
                     SELECT COALESCE(MAX(schema_id), 0) AS v FROM ducklake_schema
                     UNION ALL SELECT COALESCE(MAX(table_id), 0) FROM ducklake_table
                     UNION ALL SELECT COALESCE(MAX(view_id), 0) FROM ducklake_view
                 )), 0),
                 next_file_id = COALESCE((SELECT MAX(v) + 1 FROM (
                     SELECT COALESCE(MAX(data_file_id), 0) AS v FROM ducklake_data_file
                     UNION ALL SELECT COALESCE(MAX(delete_file_id), 0) FROM ducklake_delete_file
                 )), 0)
             WHERE snapshot_id = ?",
        )
        .bind(snapshot_id)
        .execute(&mut *tx)
        .await?;

        // Note: Replace-mode file ending is NOT done here. The caller is
        // responsible for calling end_table_files() after the Parquet upload
        // succeeds. This prevents data loss if the upload fails.

        tx.commit().await?;

        Ok(WriteSetupResult {
            snapshot_id,
            schema_id,
            table_id,
            column_ids,
        })
    }

    crate::metadata_writer_impl::impl_writer_drop_inner!(
        sqlx::Transaction<'_, sqlx::Sqlite>,
        dialect = crate::dialect::SqliteDialect,
        last_insert_id = |_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>| async {
            Ok::<i64, crate::error::DuckLakeError>(0)
        }
    );

    crate::metadata_writer_impl::impl_recompute_table_column_stats!(
        sqlx::Transaction<'_, sqlx::Sqlite>,
        crate::dialect::SqliteDialect
    );

    async fn next_entity_id(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        entity: &str,
        table_id: Option<i64>,
    ) -> Result<i64> {
        use crate::dialect::SqlDialect;
        let (sql, needs_table_id) = crate::dialect::SqliteDialect.next_id_sql(entity);
        let row = if needs_table_id {
            sqlx::query(&sql)
                .bind(table_id.ok_or_else(|| {
                    crate::error::DuckLakeError::Internal(
                        "table_id required for entity ID generation".to_string(),
                    )
                })?)
                .fetch_one(&mut **tx)
                .await?
        } else {
            sqlx::query(&sql).fetch_one(&mut **tx).await?
        };
        Ok(row.try_get(0)?)
    }
}

impl MetadataWriter for SqliteMetadataWriter {
    fn create_snapshot(&self) -> Result<i64> {
        let pool = &self.pool;
        block_on_with_retry(|| async {
            // R3F-007: Inherit schema_version from previous snapshot
            // R3F-011: Compute next_catalog_id and next_file_id
            // Use a single INSERT with subqueries to minimize lock duration
            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version, next_catalog_id, next_file_id)
                 VALUES (
                     strftime('%Y-%m-%d %H:%M:%f+00:00', 'now'),
                     COALESCE((SELECT MAX(schema_version) FROM ducklake_snapshot), 1),
                     COALESCE((SELECT MAX(v) + 1 FROM (
                         SELECT COALESCE(MAX(schema_id), 0) AS v FROM ducklake_schema
                         UNION ALL SELECT COALESCE(MAX(table_id), 0) FROM ducklake_table
                         UNION ALL SELECT COALESCE(MAX(view_id), 0) FROM ducklake_view
                     )), 0),
                     COALESCE((SELECT MAX(v) + 1 FROM (
                         SELECT COALESCE(MAX(data_file_id), 0) AS v FROM ducklake_data_file
                         UNION ALL SELECT COALESCE(MAX(delete_file_id), 0) FROM ducklake_delete_file
                     )), 0)
                 ) RETURNING snapshot_id",
            )
            .fetch_one(pool)
            .await?;
            Ok(row.try_get(0)?)
        })
    }

    fn get_or_create_schema(
        &self,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        let pool = &self.pool;
        block_on_with_retry(|| async {
            // Use a transaction to prevent TOCTOU race between SELECT and INSERT
            let mut tx = pool.begin().await?;

            let existing = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = ? AND end_snapshot IS NULL",
            )
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = existing {
                tx.commit().await?;
                return Ok((row.try_get(0)?, false));
            }

            let base_path = path.unwrap_or(name);
            let schema_path = if base_path.ends_with('/') {
                base_path.to_string()
            } else {
                format!("{}/", base_path)
            };
            let schema_uuid = uuid::Uuid::new_v4().to_string();
            let row = sqlx::query(
                "INSERT INTO ducklake_schema (schema_uuid, schema_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, 1, ?) RETURNING schema_id",
            )
            .bind(&schema_uuid)
            .bind(name)
            .bind(&schema_path)
            .bind(snapshot_id)
            .fetch_one(&mut *tx)
            .await?;

            // R3F-014: Record created_schema change tracking
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON CONFLICT(snapshot_id) DO UPDATE SET changes_made = excluded.changes_made",
            )
            .bind(snapshot_id)
            .bind(format!("created_schema:\"{}\"", name.replace('"', "\"\"")))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok((row.try_get(0)?, true))
        })
    }

    fn get_or_create_table(
        &self,
        schema_id: i64,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        let pool = &self.pool;
        block_on_with_retry(|| async {
            let mut tx = pool.begin().await?;

            let existing = sqlx::query(
                "SELECT table_id FROM ducklake_table
                 WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = existing {
                tx.commit().await?;
                return Ok((row.try_get(0)?, false));
            }

            let base_path = path.unwrap_or(name);
            let table_path = if base_path.ends_with('/') {
                base_path.to_string()
            } else {
                format!("{}/", base_path)
            };
            let next_tid_row =
                sqlx::query("SELECT COALESCE(MAX(table_id), 0) + 1 FROM ducklake_table")
                    .fetch_one(&mut *tx)
                    .await?;
            let next_table_id: i64 = next_tid_row.try_get(0)?;

            let table_uuid = uuid::Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, 1, ?)",
            )
            .bind(next_table_id)
            .bind(&table_uuid)
            .bind(schema_id)
            .bind(name)
            .bind(&table_path)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok((next_table_id, true))
        })
    }

    fn set_columns(
        &self,
        table_id: i64,
        columns: &[ColumnDef],
        snapshot_id: i64,
    ) -> Result<Vec<i64>> {
        validate_no_duplicate_columns(columns)?;
        let pool = &self.pool;
        block_on_with_retry(|| async {
            // Use a transaction to ensure atomicity: if column insertion fails,
            // we don't leave existing columns marked as ended
            let mut tx = pool.begin().await?;

            sqlx::query(
                "UPDATE ducklake_column SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            let next_cid_row = sqlx::query(
                "SELECT COALESCE(MAX(column_id), 0) + 1 FROM ducklake_column WHERE table_id = ?",
            )
            .bind(table_id)
            .fetch_one(&mut *tx)
            .await?;
            let next_column_id: i64 = next_cid_row.try_get(0)?;

            let mut column_ids = Vec::with_capacity(columns.len());
            for (order, col) in columns.iter().enumerate() {
                let column_id = next_column_id + order as i64;
                sqlx::query(
                    "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, initial_default, default_value, parent_column, default_value_type, default_value_dialect, begin_snapshot)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(column_id)
                .bind(table_id)
                .bind(&col.name)
                .bind(&col.ducklake_type)
                .bind((order + 1) as i64)
                .bind(col.is_nullable)
                .bind(&col.initial_default)
                .bind(&col.default_value)
                .bind(col.parent_column)
                .bind(&col.default_value_type)
                .bind(&col.default_value_dialect)
                .bind(snapshot_id)
                .execute(&mut *tx)
                .await?;
                column_ids.push(column_id);
            }

            tx.commit().await?;
            Ok(column_ids)
        })
    }

    // --- file_ops and query_ops generated by macros ---
    crate::metadata_writer_impl::impl_writer_file_ops!(
        SqliteMetadataWriter,
        dialect = crate::dialect::SqliteDialect,
        block_on = block_on_with_retry,
        last_insert_id = |_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>| async {
            Ok::<i64, crate::error::DuckLakeError>(0)
        }
    );

    crate::metadata_writer_impl::impl_writer_query_ops!(
        SqliteMetadataWriter,
        dialect = crate::dialect::SqliteDialect,
        block_on = block_on_with_retry
    );

    crate::metadata_writer_impl::impl_writer_ddl_ops!(
        SqliteMetadataWriter,
        dialect = crate::dialect::SqliteDialect,
        block_on = block_on_with_retry,
        last_insert_id = |_tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>| async {
            Ok::<i64, crate::error::DuckLakeError>(0)
        }
    );

    // --- remaining per-backend methods below ---

    fn initialize_schema(&self) -> Result<()> {
        let pool = &self.pool;
        block_on(async {
            // The DDL (CREATE TABLE IF NOT EXISTS) must run outside the
            // transaction because SQLite does not support transactional DDL
            // via sqlx's raw `execute` on a multi-statement string.
            sqlx::query(SQL_CREATE_SCHEMA).execute(pool).await?;

            // Wrap the seed DML in an explicit transaction so that either all
            // metadata rows are inserted or none are (R7-S-047).
            let mut tx = pool.begin().await?;

            // Insert DuckLake version metadata if not already present.
            // DuckLake uses this for migration checks; v0.3 is compatible with DuckDB v1.4.x.
            sqlx::query(
                "INSERT INTO ducklake_metadata (key, value)
                 SELECT 'version', '0.3'
                 WHERE NOT EXISTS (SELECT 1 FROM ducklake_metadata WHERE key = 'version' AND scope IS NULL)",
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO ducklake_metadata (key, value)
                 SELECT 'created_by', 'DataFusion-DuckLake'
                 WHERE NOT EXISTS (SELECT 1 FROM ducklake_metadata WHERE key = 'created_by' AND scope IS NULL)",
            )
            .execute(&mut *tx)
            .await?;

            // DuckDB sets `encrypted=false` in metadata; match for interop (F-047)
            sqlx::query(
                "INSERT INTO ducklake_metadata (key, value)
                 SELECT 'encrypted', 'false'
                 WHERE NOT EXISTS (SELECT 1 FROM ducklake_metadata WHERE key = 'encrypted' AND scope IS NULL)",
            )
            .execute(&mut *tx)
            .await?;

            // Insert initial snapshot 0 (DuckDB expects this as the "empty catalog" snapshot)
            sqlx::query(
                "INSERT OR IGNORE INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
                 VALUES (0, strftime('%Y-%m-%d %H:%M:%f+00:00', 'now'), 0, 0, 0)",
            )
            .execute(&mut *tx)
            .await?;

            // Insert initial schema_version entry (F-012)
            sqlx::query(
                "INSERT OR IGNORE INTO ducklake_schema_versions (begin_snapshot, schema_version)
                 VALUES (0, 0)",
            )
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;

            Ok(())
        })
    }

    crate::metadata_writer_impl::impl_writer_drop_ops!(
        SqliteMetadataWriter,
        dialect = crate::dialect::SqliteDialect,
        block_on = block_on_with_retry
    );

    fn begin_checked_write_transaction(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
        since_snapshot: i64,
    ) -> Result<WriteSetupResult> {
        let pool = &self.pool;
        block_on_with_retry(|| async {
            let mut tx = pool.begin().await?;

            // Conflict check: look for DROP operations since our snapshot.
            // This runs in the SAME transaction as the write to prevent TOCTOU races.
            let schema_row = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = ?
                 ORDER BY schema_id DESC LIMIT 1",
            )
            .bind(schema_name)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(schema_row) = schema_row {
                let schema_id: i64 = schema_row.try_get(0)?;

                // Check DF-originated drops via change tracking
                let schema_drop = sqlx::query(
                    "SELECT COUNT(*) FROM _df_change_tracking
                     WHERE snapshot_id > ? AND schema_id = ? AND change_type = 'DROP_SCHEMA'",
                )
                .bind(since_snapshot)
                .bind(schema_id)
                .fetch_one(&mut *tx)
                .await?;
                if schema_drop.try_get::<i64, _>(0)? > 0 {
                    return Err(crate::error::DuckLakeError::TransactionConflict(format!(
                        "Transaction conflict: schema '{}' was dropped by another transaction since snapshot {}",
                        schema_name, since_snapshot
                    )));
                }

                // Check DuckDB-originated drops via catalog metadata (R5-S-018)
                let schema_ended = sqlx::query(
                    "SELECT COUNT(*) FROM ducklake_schema
                     WHERE schema_id = ? AND end_snapshot IS NOT NULL AND end_snapshot > ?",
                )
                .bind(schema_id)
                .bind(since_snapshot)
                .fetch_one(&mut *tx)
                .await?;
                if schema_ended.try_get::<i64, _>(0)? > 0 {
                    return Err(crate::error::DuckLakeError::TransactionConflict(format!(
                        "Transaction conflict: schema '{}' was dropped (possibly by DuckDB) since snapshot {}",
                        schema_name, since_snapshot
                    )));
                }

                let table_row = sqlx::query(
                    "SELECT table_id FROM ducklake_table
                     WHERE schema_id = ? AND table_name = ?
                     ORDER BY table_id DESC LIMIT 1",
                )
                .bind(schema_id)
                .bind(table_name)
                .fetch_optional(&mut *tx)
                .await?;

                if let Some(table_row) = table_row {
                    let table_id: i64 = table_row.try_get(0)?;

                    // Check DF-originated drops via change tracking
                    let table_drop = sqlx::query(
                        "SELECT COUNT(*) FROM _df_change_tracking
                         WHERE snapshot_id > ? AND table_id = ? AND change_type = 'DROP_TABLE'",
                    )
                    .bind(since_snapshot)
                    .bind(table_id)
                    .fetch_one(&mut *tx)
                    .await?;
                    if table_drop.try_get::<i64, _>(0)? > 0 {
                        return Err(crate::error::DuckLakeError::TransactionConflict(format!(
                            "Transaction conflict: table '{}.{}' was dropped by another transaction since snapshot {}",
                            schema_name, table_name, since_snapshot
                        )));
                    }

                    // Check DuckDB-originated drops via catalog metadata (R5-S-018)
                    let table_ended = sqlx::query(
                        "SELECT COUNT(*) FROM ducklake_table
                         WHERE table_id = ? AND end_snapshot IS NOT NULL AND end_snapshot > ?",
                    )
                    .bind(table_id)
                    .bind(since_snapshot)
                    .fetch_one(&mut *tx)
                    .await?;
                    if table_ended.try_get::<i64, _>(0)? > 0 {
                        return Err(crate::error::DuckLakeError::TransactionConflict(format!(
                            "Transaction conflict: table '{}.{}' was dropped (possibly by DuckDB) since snapshot {}",
                            schema_name, table_name, since_snapshot
                        )));
                    }
                }
            }

            // No conflict — proceed with write in the same transaction.
            Self::write_transaction_inner(tx, schema_name, table_name, columns, mode).await
        })
    }

    fn begin_write_transaction(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
    ) -> Result<WriteSetupResult> {
        let pool = &self.pool;
        block_on_with_retry(|| async {
            let tx = pool.begin().await?;
            Self::write_transaction_inner(tx, schema_name, table_name, columns, mode).await
        })
    }

    fn get_data_inlining_row_limit(&self) -> Result<Option<i64>> {
        block_on(async {
            let row = sqlx::query(
                "SELECT value FROM ducklake_metadata WHERE key = 'data_inlining_row_limit' AND scope IS NULL",
            )
            .fetch_optional(&self.pool)
            .await?;

            match row {
                Some(r) => {
                    let val: String = r.try_get(0)?;
                    match val.parse::<i64>() {
                        Ok(limit) if limit > 0 => Ok(Some(limit)),
                        _ => Ok(None),
                    }
                },
                None => Ok(None),
            }
        })
    }

    fn get_inlined_row_count(&self, table_id: i64) -> Result<i64> {
        block_on(async {
            let table_info = sqlx::query(
                "SELECT table_name FROM ducklake_inlined_data_tables WHERE table_id = ?",
            )
            .bind(table_id)
            .fetch_optional(&self.pool)
            .await?;

            let Some(info_row) = table_info else {
                return Ok(0);
            };

            let inlined_table_name: String = info_row.try_get(0)?;

            // Check if table exists
            let exists =
                sqlx::query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?")
                    .bind(&inlined_table_name)
                    .fetch_one(&self.pool)
                    .await?;
            let count: i64 = exists.try_get(0)?;
            if count == 0 {
                return Ok(0);
            }

            let count_sql = format!(
                "SELECT COUNT(*) FROM {} WHERE end_snapshot IS NULL",
                SqliteDialect.quote_id(&inlined_table_name)
            );
            let row = sqlx::query(&count_sql).fetch_one(&self.pool).await?;
            Ok(row.try_get(0)?)
        })
    }

    fn store_inlined_data(
        &self,
        table_id: i64,
        snapshot_id: i64,
        columns: &[ColumnDef],
        rows: &[InlinedDataRow],
    ) -> Result<i64> {
        if rows.is_empty() {
            return Ok(0);
        }
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // R4-S-023 + R6-S-009: Use DuckDB-compatible naming with actual schema_version
            // First check if an inlined data table already exists for this table_id
            let existing = sqlx::query(
                "SELECT table_name, schema_version FROM ducklake_inlined_data_tables WHERE table_id = ?",
            )
            .bind(table_id)
            .fetch_optional(&mut *tx)
            .await?;

            // R7-S-012: Look up current schema_version to detect schema evolution
            let sv_row =
                sqlx::query("SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = ?")
                    .bind(snapshot_id)
                    .fetch_one(&mut *tx)
                    .await?;
            let current_sv: i64 = sv_row.try_get(0)?;

            let inlined_table_name = if let Some(ref row) = existing {
                let old_name: String = row.try_get(0)?;
                let old_sv: i64 = row.try_get(1)?;
                if old_sv < current_sv {
                    // Schema evolved (e.g. ALTER TABLE ADD COLUMN).
                    // Expire old inlined rows and recreate table with updated columns.
                    let old_exists = sqlx::query(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?",
                    )
                    .bind(&old_name)
                    .fetch_one(&mut *tx)
                    .await?;
                    if old_exists.try_get::<i64, _>(0)? > 0 {
                        let expire_sql = format!(
                            "UPDATE {} SET end_snapshot = ? WHERE end_snapshot IS NULL",
                            SqliteDialect.quote_id(&old_name)
                        );
                        sqlx::query(&expire_sql)
                            .bind(snapshot_id)
                            .execute(&mut *tx)
                            .await?;
                    }
                    let new_name = format!("ducklake_inlined_data_{}_{}", table_id, current_sv);
                    sqlx::query(
                        "UPDATE ducklake_inlined_data_tables SET table_name = ?, schema_version = ? WHERE table_id = ?",
                    )
                    .bind(&new_name)
                    .bind(current_sv)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;
                    new_name
                } else {
                    old_name
                }
            } else {
                format!("ducklake_inlined_data_{}_{}", table_id, current_sv)
            };

            // Check if inline data table exists in SQLite; create if not
            let exists =
                sqlx::query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?")
                    .bind(&inlined_table_name)
                    .fetch_one(&mut *tx)
                    .await?;
            let table_exists: i64 = exists.try_get(0)?;

            if table_exists == 0 {
                // Create the inline data table matching DuckDB's layout:
                // row_id, begin_snapshot, end_snapshot, then user columns
                let mut create_sql = format!(
                    "CREATE TABLE {} (row_id INTEGER, begin_snapshot INTEGER, end_snapshot INTEGER",
                    SqliteDialect.quote_id(&inlined_table_name)
                );
                for col in columns {
                    // R6-S-029: Validate type string before DDL interpolation
                    let dl_type = col.ducklake_type();
                    validate_ducklake_type_for_ddl(dl_type)?;
                    create_sql.push_str(&format!(
                        ", {} {}",
                        SqliteDialect.quote_id(col.name()),
                        dl_type
                    ));
                }
                create_sql.push(')');
                sqlx::query(&create_sql).execute(&mut *tx).await?;

                // Register in ducklake_inlined_data_tables (only for new tables)
                if existing.is_none() {
                    sqlx::query(
                        "INSERT INTO ducklake_inlined_data_tables (table_id, table_name, schema_version) VALUES (?, ?, ?)",
                    )
                    .bind(table_id)
                    .bind(&inlined_table_name)
                    .bind(current_sv)
                    .execute(&mut *tx)
                    .await?;
                }
            }

            // Get next row_id
            let next_row_id_sql = format!(
                "SELECT COALESCE(MAX(row_id), -1) + 1 FROM {}",
                SqliteDialect.quote_id(&inlined_table_name)
            );
            let next_row_id_row = sqlx::query(&next_row_id_sql).fetch_one(&mut *tx).await?;
            let mut row_id: i64 = next_row_id_row.try_get(0)?;

            // Build column names for the INSERT (properly quoted to prevent injection)
            let col_names: Vec<String> = columns
                .iter()
                .map(|c| SqliteDialect.quote_id(c.name()))
                .collect();
            let placeholders: Vec<&str> = (0..columns.len()).map(|_| "?").collect();
            let insert_sql = format!(
                "INSERT INTO {} (row_id, begin_snapshot, {}) VALUES (?, ?, {})",
                SqliteDialect.quote_id(&inlined_table_name),
                col_names.join(", "),
                placeholders.join(", ")
            );

            let mut total_rows: i64 = 0;
            for inlined_row in rows {
                let mut query = sqlx::query(&insert_sql).bind(row_id).bind(snapshot_id);

                // Bind each column value
                for col in columns {
                    let value = inlined_row
                        .column_names
                        .iter()
                        .position(|n| n == col.name())
                        .and_then(|pos| inlined_row.values.get(pos))
                        .and_then(|v| v.clone());
                    query = query.bind(value);
                }

                query.execute(&mut *tx).await?;
                row_id += 1;
                total_rows += 1;
            }

            tx.commit().await?;
            Ok(total_rows)
        })
    }

    fn read_inlined_data(&self, table_id: i64) -> Result<Vec<InlinedDataRow>> {
        block_on(async {
            let table_info = sqlx::query(
                "SELECT table_name FROM ducklake_inlined_data_tables WHERE table_id = ?",
            )
            .bind(table_id)
            .fetch_optional(&self.pool)
            .await?;

            let Some(info_row) = table_info else {
                return Ok(Vec::new());
            };

            let inlined_table_name: String = info_row.try_get(0)?;

            // Check if table exists
            let exists =
                sqlx::query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?")
                    .bind(&inlined_table_name)
                    .fetch_one(&self.pool)
                    .await?;
            let count: i64 = exists.try_get(0)?;
            if count == 0 {
                return Ok(Vec::new());
            }

            // Get column names
            let pragma_query = format!(
                "PRAGMA table_info({})",
                SqliteDialect.quote_id(&inlined_table_name)
            );
            let columns = sqlx::query(&pragma_query).fetch_all(&self.pool).await?;

            let user_columns: Vec<String> = columns
                .iter()
                .filter_map(|row| {
                    let name: String = row.try_get::<String, _>(1).ok()?;
                    if name == "row_id" || name == "begin_snapshot" || name == "end_snapshot" {
                        None
                    } else {
                        Some(name)
                    }
                })
                .collect();

            if user_columns.is_empty() {
                return Ok(Vec::new());
            }

            // Query active rows (properly quoted to prevent injection)
            let col_list: Vec<String> = user_columns
                .iter()
                .map(|c| format!("CAST({} AS TEXT)", SqliteDialect.quote_id(c)))
                .collect();
            let select_sql = format!(
                "SELECT {} FROM {} WHERE end_snapshot IS NULL",
                col_list.join(", "),
                SqliteDialect.quote_id(&inlined_table_name),
            );

            let rows = sqlx::query(&select_sql).fetch_all(&self.pool).await?;

            let num_columns = user_columns.len();
            let user_columns = Arc::new(user_columns);
            let mut result = Vec::new();
            for row in &rows {
                let mut values = Vec::new();
                for i in 0..num_columns {
                    let val: Option<String> = row.try_get(i)?;
                    values.push(val);
                }
                result.push(InlinedDataRow {
                    column_names: Arc::clone(&user_columns),
                    values,
                });
            }

            Ok(result)
        })
    }

    fn clear_inlined_data(&self, table_id: i64, snapshot_id: i64) -> Result<()> {
        block_on(async {
            let table_info = sqlx::query(
                "SELECT table_name FROM ducklake_inlined_data_tables WHERE table_id = ?",
            )
            .bind(table_id)
            .fetch_optional(&self.pool)
            .await?;

            let Some(info_row) = table_info else {
                return Ok(());
            };

            let inlined_table_name: String = info_row.try_get(0)?;

            // Set end_snapshot on all active rows
            let update_sql = format!(
                "UPDATE {} SET end_snapshot = ? WHERE end_snapshot IS NULL",
                SqliteDialect.quote_id(&inlined_table_name)
            );
            sqlx::query(&update_sql)
                .bind(snapshot_id)
                .execute(&self.pool)
                .await?;

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_writer::{ColumnStatInfo, DataFileInfo, DeleteFileInfo, ReplaceFileEntry};
    use crate::metadata_writer_validation::{
        cmp_decimal_strings, is_numeric_type, stat_value_less_than,
    };
    use tempfile::TempDir;

    async fn create_test_writer() -> (SqliteMetadataWriter, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
        let writer = SqliteMetadataWriter::new_with_init(&conn_str)
            .await
            .unwrap();
        (writer, temp_dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_snapshot() {
        let (writer, _temp) = create_test_writer().await;

        let snap1 = writer.create_snapshot().unwrap();
        assert_eq!(snap1, 1);

        let snap2 = writer.create_snapshot().unwrap();
        assert_eq!(snap2, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_or_create_schema() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();

        // Create new schema
        let (schema_id, created) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        assert!(created);
        assert_eq!(schema_id, 1);

        // Get existing schema
        let (schema_id2, created2) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        assert!(!created2);
        assert_eq!(schema_id2, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_or_create_table() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();

        // Create new table
        let (table_id, created) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();
        assert!(created);
        assert_eq!(table_id, 1);

        // Get existing table
        let (table_id2, created2) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();
        assert!(!created2);
        assert_eq!(table_id2, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_columns() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];

        let column_ids = writer.set_columns(table_id, &columns, snapshot_id).unwrap();
        assert_eq!(column_ids.len(), 2);
        assert_eq!(column_ids[0], 1);
        assert_eq!(column_ids[1], 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_data_file() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        let file = DataFileInfo::new("data.parquet", 1024, 100).with_footer_size(256);

        let file_id = writer
            .register_data_file(table_id, snapshot_id, &file)
            .unwrap();
        assert_eq!(file_id, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_end_table_files() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot1 = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot1)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot1)
            .unwrap();

        // Register a file
        let file = DataFileInfo::new("data1.parquet", 1024, 100);
        writer
            .register_data_file(table_id, snapshot1, &file)
            .unwrap();

        // End files at new snapshot
        let snapshot2 = writer.create_snapshot().unwrap();
        let ended = writer.end_table_files(table_id, snapshot2).unwrap();
        assert_eq!(ended, 1);

        // End again should affect 0 files
        let ended2 = writer.end_table_files(table_id, snapshot2).unwrap();
        assert_eq!(ended2, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_data_path() {
        let (writer, _temp) = create_test_writer().await;

        // Set data path
        writer.set_data_path("/data/path").unwrap();

        // Get data path
        let path = writer.get_data_path().unwrap();
        assert_eq!(path, "/data/path");

        // Update data path
        writer.set_data_path("/new/path").unwrap();
        let path2 = writer.get_data_path().unwrap();
        assert_eq!(path2, "/new/path");
    }

    /// Verifies that column_id is stable (reused) after a rename operation.
    /// This is critical for Parquet field_id mapping (types.rs maps column_id → field_id).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_column_id_stable_after_rename() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        // Create columns: id (column_id=1), name (column_id=2)
        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        let column_ids = writer.set_columns(table_id, &columns, snapshot_id).unwrap();
        assert_eq!(column_ids, vec![1, 2]);

        // Rename "name" to "full_name"
        let op = AlterTableOp::RenameColumn {
            old_name: "name".to_string(),
            new_name: "full_name".to_string(),
        };
        writer.alter_table(table_id, &op).unwrap();

        // Verify the renamed column still has the same column_id (2)
        let rows = block_on(async {
            sqlx::query(
                "SELECT column_id, column_name FROM ducklake_column
                 WHERE table_id = ? AND end_snapshot IS NULL
                 ORDER BY column_order",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });

        assert_eq!(rows.len(), 2);
        // "id" column should still have column_id = 1
        let col0_id: i64 = rows[0].try_get(0).unwrap();
        let col0_name: String = rows[0].try_get(1).unwrap();
        assert_eq!(col0_id, 1);
        assert_eq!(col0_name, "id");
        // "full_name" column should still have column_id = 2 (reused from "name")
        let col1_id: i64 = rows[1].try_get(0).unwrap();
        let col1_name: String = rows[1].try_get(1).unwrap();
        assert_eq!(col1_id, 2);
        assert_eq!(col1_name, "full_name");

        // Also verify there are ended rows with the same column_ids
        let all_rows = block_on(async {
            sqlx::query(
                "SELECT column_id, column_name, end_snapshot FROM ducklake_column
                 WHERE table_id = ?
                 ORDER BY column_id, end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });

        // Should have 3 rows: id(active), name(ended), full_name(active)
        assert_eq!(all_rows.len(), 3);

        // Verify column_id=2 appears twice (once ended, once active)
        let col2_rows: Vec<_> = all_rows
            .iter()
            .filter(|r| r.try_get::<i64, _>(0).unwrap() == 2)
            .collect();
        assert_eq!(col2_rows.len(), 2);
        // One should be ended (name), one active (full_name)
        let ended: String = col2_rows[0].try_get(1).unwrap();
        let active: String = col2_rows[1].try_get(1).unwrap();
        assert_eq!(ended, "name");
        assert_eq!(active, "full_name");
    }

    /// Verifies that adding a new column gets the next sequential column_id,
    /// not conflicting with existing or ended column_ids.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_column_gets_next_id() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        // Create columns: id (1), name (2)
        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        // Add a new column
        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("email", "varchar", true).unwrap(),
        };
        writer.alter_table(table_id, &op).unwrap();

        // Verify new column got column_id = 3
        let rows = block_on(async {
            sqlx::query(
                "SELECT column_id, column_name FROM ducklake_column
                 WHERE table_id = ? AND end_snapshot IS NULL
                 ORDER BY column_order",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });

        assert_eq!(rows.len(), 3);
        let email_id: i64 = rows[2].try_get(0).unwrap();
        let email_name: String = rows[2].try_get(1).unwrap();
        assert_eq!(email_id, 3);
        assert_eq!(email_name, "email");
    }

    /// Verifies that default values are preserved after a rename operation.
    /// This tests the M4 fix: ReplaceColumn must carry forward all default-related fields.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_defaults_preserved_after_rename() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        // Create a column with default values set
        let mut col = ColumnDef::new("status", "varchar", true).unwrap();
        col.initial_default = Some("active".to_string());
        col.default_value = Some("active".to_string());
        col.default_value_type = Some("VARCHAR".to_string());
        col.default_value_dialect = Some("SQL".to_string());
        let columns = vec![ColumnDef::new("id", "int64", false).unwrap(), col];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        // Rename "status" to "user_status"
        let op = AlterTableOp::RenameColumn {
            old_name: "status".to_string(),
            new_name: "user_status".to_string(),
        };
        writer.alter_table(table_id, &op).unwrap();

        // Verify defaults are preserved on the renamed column
        let rows = block_on(async {
            sqlx::query(
                "SELECT column_name, initial_default, default_value, default_value_type, default_value_dialect
                 FROM ducklake_column
                 WHERE table_id = ? AND end_snapshot IS NULL AND column_name = 'user_status'",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });

        assert_eq!(rows.len(), 1);
        let name: String = rows[0].try_get(0).unwrap();
        let init_def: Option<String> = rows[0].try_get(1).unwrap();
        let def_val: Option<String> = rows[0].try_get(2).unwrap();
        let def_type: Option<String> = rows[0].try_get(3).unwrap();
        let def_dialect: Option<String> = rows[0].try_get(4).unwrap();
        assert_eq!(name, "user_status");
        assert_eq!(init_def.as_deref(), Some("active"));
        assert_eq!(def_val.as_deref(), Some("active"));
        assert_eq!(def_type.as_deref(), Some("VARCHAR"));
        assert_eq!(def_dialect.as_deref(), Some("SQL"));
    }

    /// P2-5: Duplicate column names in write_transaction_inner should be rejected.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_duplicate_column_names_rejected() {
        let (writer, _temp) = create_test_writer().await;

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("id", "int64", false).unwrap(), // duplicate
        ];

        let result =
            writer.begin_write_transaction("main", "dup_test", &columns, WriteMode::Replace);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Duplicate column name"),
            "Expected duplicate column error, got: {err}"
        );
    }

    /// P2-5: Duplicate columns in set_columns should also be rejected.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_columns_duplicate_rejected() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        let columns = vec![
            ColumnDef::new("x", "int64", false).unwrap(),
            ColumnDef::new("x", "varchar", true).unwrap(),
        ];

        let result = writer.set_columns(table_id, &columns, snapshot_id);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Duplicate column name")
        );
    }

    /// P1-3: Column stats row should be initialized after ALTER TABLE AddColumn.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_column_initializes_table_column_stats() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        // Add a new column
        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("email", "varchar", true).unwrap(),
        };
        writer.alter_table(table_id, &op).unwrap();

        // Verify that ducklake_table_column_stats has a row for the new column
        let stats_rows = block_on(async {
            sqlx::query(
                "SELECT table_id, column_id, contains_null FROM ducklake_table_column_stats
                 WHERE table_id = ?
                 ORDER BY column_id",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });

        // Should have at least the new column's stats row
        assert!(
            !stats_rows.is_empty(),
            "Expected ducklake_table_column_stats to have a row for the new column"
        );

        // Find the row for the email column (column_id = 3)
        let new_col_stat = stats_rows
            .iter()
            .find(|r| r.try_get::<i64, _>(1).unwrap() == 3);
        assert!(
            new_col_stat.is_some(),
            "Expected a stats row for the newly added column (column_id=3)"
        );

        // R5-S-003: Verify contains_null is set to true (1), not NULL.
        // Existing rows have NULL for the new column, so contains_null must be true.
        // NULL contains_null causes DuckDB to crash when reading from the catalog.
        let contains_null: Option<bool> = new_col_stat.unwrap().try_get(2).unwrap();
        assert_eq!(
            contains_null,
            Some(true),
            "contains_null must be TRUE (not NULL) for newly added columns"
        );
    }

    /// P1-4: Compound ALTER TABLE operations (ADD then RENAME) should not corrupt metadata.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_compound_alter_add_then_rename() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "users", None, snapshot_id)
            .unwrap();

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        let col_ids = writer.set_columns(table_id, &columns, snapshot_id).unwrap();
        assert_eq!(col_ids, vec![1, 2]);

        // Step 1: ADD COLUMN email
        let snap1 = writer
            .alter_table(
                table_id,
                &AlterTableOp::AddColumn {
                    column: ColumnDef::new("email", "varchar", true).unwrap(),
                },
            )
            .unwrap();

        // Step 2: RENAME the newly added column
        let snap2 = writer
            .alter_table(
                table_id,
                &AlterTableOp::RenameColumn {
                    old_name: "email".to_string(),
                    new_name: "contact_email".to_string(),
                },
            )
            .unwrap();

        assert!(snap2 > snap1, "Each alter should create a new snapshot");

        // Verify final column state
        let columns = writer.get_active_columns(table_id).unwrap();
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].0, "id");
        assert_eq!(columns[1].0, "name");
        assert_eq!(columns[2].0, "contact_email");
        assert_eq!(columns[2].1, "varchar");
        assert!(columns[2].2); // nullable

        // Verify no duplicate active column names
        let active_rows = block_on(async {
            sqlx::query(
                "SELECT column_name FROM ducklake_column
                 WHERE table_id = ? AND end_snapshot IS NULL
                 ORDER BY column_order",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        let names: Vec<String> = active_rows.iter().map(|r| r.try_get(0).unwrap()).collect();
        assert_eq!(names, vec!["id", "name", "contact_email"]);

        // Verify column_id stability: email was added as id=3, renamed column should reuse id=3
        let id_rows = block_on(async {
            sqlx::query(
                "SELECT column_id, column_name FROM ducklake_column
                 WHERE table_id = ? AND end_snapshot IS NULL
                 ORDER BY column_order",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        let renamed_col_id: i64 = id_rows[2].try_get(0).unwrap();
        let renamed_col_name: String = id_rows[2].try_get(1).unwrap();
        assert_eq!(renamed_col_name, "contact_email");
        // The rename reuses the same column_id
        assert_eq!(renamed_col_id, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rename_table() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "old_name", None, snapshot_id)
            .unwrap();
        let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        // Rename the table
        let rename_snap = writer.rename_table(table_id, "new_name").unwrap();
        assert!(rename_snap > snapshot_id);

        // Verify: old row is ended, new row has new name
        let rows = block_on(async {
            sqlx::query(
                "SELECT table_name, end_snapshot FROM ducklake_table
                 WHERE table_id = ? ORDER BY begin_snapshot",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(rows.len(), 2);
        let old_name: String = rows[0].try_get(0).unwrap();
        let old_end: Option<i64> = rows[0].try_get(1).unwrap();
        let new_name: String = rows[1].try_get(0).unwrap();
        let new_end: Option<i64> = rows[1].try_get(1).unwrap();
        assert_eq!(old_name, "old_name");
        assert!(old_end.is_some());
        assert_eq!(new_name, "new_name");
        assert!(new_end.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rename_nonexistent_table_fails() {
        let (writer, _temp) = create_test_writer().await;
        let result = writer.rename_table(999, "new_name");
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_table_comment() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "t1", None, snapshot_id)
            .unwrap();
        let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        // Set a comment
        let snap1 = writer.set_table_comment(table_id, "First comment").unwrap();
        assert!(snap1 > snapshot_id);

        // Verify
        let rows = block_on(async {
            sqlx::query(
                "SELECT value FROM ducklake_tag
                 WHERE object_id = ? AND key = 'comment' AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(rows.len(), 1);
        let comment: String = rows[0].try_get(0).unwrap();
        assert_eq!(comment, "First comment");

        // Update the comment
        let snap2 = writer
            .set_table_comment(table_id, "Updated comment")
            .unwrap();
        assert!(snap2 > snap1);

        // Verify old comment is ended, new one is active
        let all_rows = block_on(async {
            sqlx::query(
                "SELECT value, end_snapshot FROM ducklake_tag
                 WHERE object_id = ? AND key = 'comment'
                 ORDER BY begin_snapshot",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(all_rows.len(), 2);
        let first_end: Option<i64> = all_rows[0].try_get(1).unwrap();
        let second_val: String = all_rows[1].try_get(0).unwrap();
        let second_end: Option<i64> = all_rows[1].try_get(1).unwrap();
        assert!(first_end.is_some()); // old comment ended
        assert_eq!(second_val, "Updated comment");
        assert!(second_end.is_none()); // new comment active
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_column_comment() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "t1", None, snapshot_id)
            .unwrap();
        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        // Set a column comment
        let snap1 = writer
            .set_column_comment(table_id, "name", "The user name")
            .unwrap();
        assert!(snap1 > snapshot_id);

        // Verify
        let rows = block_on(async {
            sqlx::query(
                "SELECT value FROM ducklake_column_tag
                 WHERE table_id = ? AND key = 'comment' AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(rows.len(), 1);
        let comment: String = rows[0].try_get(0).unwrap();
        assert_eq!(comment, "The user name");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_set_column_comment_nonexistent_column_fails() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "t1", None, snapshot_id)
            .unwrap();
        let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        let result = writer.set_column_comment(table_id, "missing", "comment");
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_alter_table_set_column_default() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "t1", None, snapshot_id)
            .unwrap();
        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("age", "int32", true).unwrap(),
        ];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        // SET DEFAULT
        let op = AlterTableOp::SetColumnDefault {
            column_name: "age".into(),
            default_value: "0".into(),
        };
        writer.alter_table(table_id, &op).unwrap();

        // Verify default_value is set
        let rows = block_on(async {
            sqlx::query(
                "SELECT default_value FROM ducklake_column
                 WHERE table_id = ? AND column_name = 'age' AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(rows.len(), 1);
        let default_val: Option<String> = rows[0].try_get(0).unwrap();
        assert_eq!(default_val.as_deref(), Some("0"));

        // DROP DEFAULT
        let op2 = AlterTableOp::DropColumnDefault {
            column_name: "age".into(),
        };
        writer.alter_table(table_id, &op2).unwrap();

        let rows2 = block_on(async {
            sqlx::query(
                "SELECT default_value FROM ducklake_column
                 WHERE table_id = ? AND column_name = 'age' AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(rows2.len(), 1);
        let default_val2: Option<String> = rows2[0].try_get(0).unwrap();
        assert!(default_val2.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_alter_table_set_not_null() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "t1", None, snapshot_id)
            .unwrap();
        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        // SET NOT NULL
        let op = AlterTableOp::SetNotNull {
            column_name: "name".into(),
        };
        writer.alter_table(table_id, &op).unwrap();

        let columns = writer.get_active_columns(table_id).unwrap();
        assert_eq!(columns[1].0, "name");
        assert!(!columns[1].2); // not nullable

        // DROP NOT NULL
        let op2 = AlterTableOp::DropNotNull {
            column_name: "name".into(),
        };
        writer.alter_table(table_id, &op2).unwrap();

        let columns2 = writer.get_active_columns(table_id).unwrap();
        assert_eq!(columns2[1].0, "name");
        assert!(columns2[1].2); // nullable again
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rename_view() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();

        // Create a view
        let (view_id, create_snap) = writer
            .create_view(schema_id, "old_view", "SELECT 1 AS x")
            .unwrap();

        // Rename the view
        let rename_snap = writer.rename_view(view_id, "new_view").unwrap();
        assert!(rename_snap > create_snap);

        // Verify: old row is ended, new row has new name
        let rows = block_on(async {
            sqlx::query(
                "SELECT view_name, end_snapshot FROM ducklake_view
                 WHERE view_id = ? ORDER BY begin_snapshot",
            )
            .bind(view_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(rows.len(), 2);
        let old_name: String = rows[0].try_get(0).unwrap();
        let old_end: Option<i64> = rows[0].try_get(1).unwrap();
        let new_name: String = rows[1].try_get(0).unwrap();
        let new_end: Option<i64> = rows[1].try_get(1).unwrap();
        assert_eq!(old_name, "old_view");
        assert!(old_end.is_some());
        assert_eq!(new_name, "new_view");
        assert!(new_end.is_none());

        // Verify SQL is preserved
        let sql_row = block_on(async {
            sqlx::query(
                "SELECT sql FROM ducklake_view
                 WHERE view_id = ? AND end_snapshot IS NULL",
            )
            .bind(view_id)
            .fetch_one(&writer.pool)
            .await
            .unwrap()
        });
        let sql: String = sql_row.try_get(0).unwrap();
        assert_eq!(sql, "SELECT 1 AS x");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rename_nonexistent_view_fails() {
        let (writer, _temp) = create_test_writer().await;
        let result = writer.rename_view(999, "new_name");
        assert!(result.is_err());
    }

    // R5-S-001: Unit tests for type-aware stat comparison

    #[test]
    fn test_is_numeric_type() {
        assert!(is_numeric_type("INTEGER"));
        assert!(is_numeric_type("BIGINT"));
        assert!(is_numeric_type("FLOAT"));
        assert!(is_numeric_type("DOUBLE"));
        assert!(is_numeric_type("DECIMAL(10,2)"));
        assert!(is_numeric_type("int"));
        assert!(is_numeric_type("SMALLINT"));
        assert!(!is_numeric_type("VARCHAR"));
        assert!(!is_numeric_type("TEXT"));
        assert!(!is_numeric_type("DATE"));
        assert!(!is_numeric_type("TIMESTAMP"));
        assert!(!is_numeric_type("BOOLEAN"));
    }

    #[test]
    fn test_stat_value_less_than_numeric() {
        // Numeric comparison: "9" < "10" numerically
        assert!(stat_value_less_than("9", "10", true));
        assert!(!stat_value_less_than("10", "9", true));

        // But lexicographically "10" < "9"
        assert!(!stat_value_less_than("9", "10", false));
        assert!(stat_value_less_than("10", "9", false));

        // Negative numbers
        assert!(stat_value_less_than("-5", "3", true));
        assert!(!stat_value_less_than("3", "-5", true));

        // Floating point
        assert!(stat_value_less_than("1.5", "2.5", true));
        assert!(!stat_value_less_than("2.5", "1.5", true));

        // Equal values
        assert!(!stat_value_less_than("42", "42", true));
    }

    #[test]
    fn test_stat_value_less_than_string() {
        // String comparison uses lexicographic ordering
        assert!(stat_value_less_than("apple", "banana", false));
        assert!(!stat_value_less_than("banana", "apple", false));
        assert!(!stat_value_less_than("banana", "banana", false));
    }

    #[test]
    fn test_cmp_decimal_strings() {
        use std::cmp::Ordering;
        assert_eq!(cmp_decimal_strings("1.1", "1.2"), Some(Ordering::Less));
        assert_eq!(cmp_decimal_strings("1.2", "1.1"), Some(Ordering::Greater));
        assert_eq!(cmp_decimal_strings("1.1", "1.1"), Some(Ordering::Equal));
        assert_eq!(
            cmp_decimal_strings("99999999999999999.1", "99999999999999999.2"),
            Some(Ordering::Less)
        );
        assert_eq!(cmp_decimal_strings("-10.5", "-10.3"), Some(Ordering::Less));
        assert_eq!(cmp_decimal_strings("-0.0", "0.0"), Some(Ordering::Equal));
        assert_eq!(cmp_decimal_strings("5", "5.0"), Some(Ordering::Equal));
        assert_eq!(cmp_decimal_strings("abc", "def"), None);
    }

    #[test]
    fn test_stat_value_less_than_decimal_precision() {
        assert!(stat_value_less_than(
            "99999999999999999.1",
            "99999999999999999.2",
            true
        ));
        assert!(!stat_value_less_than(
            "99999999999999999.2",
            "99999999999999999.1",
            true
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replace_table_files_column_stats_include_table_id() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "t", None, snapshot_id)
            .unwrap();
        let cols = vec![ColumnDef::new("id", "int64", false).unwrap()];
        writer.set_columns(table_id, &cols, snapshot_id).unwrap();
        let col_row =
            sqlx::query("SELECT column_id FROM ducklake_column WHERE table_id = ? LIMIT 1")
                .bind(table_id)
                .fetch_one(&writer.pool)
                .await
                .unwrap();
        let col_id: i64 = col_row.try_get(0).unwrap();
        let file = DataFileInfo::new("f1.parquet", 1000, 100);
        writer
            .register_data_file(table_id, snapshot_id, &file)
            .unwrap();
        let stats = vec![ColumnStatInfo {
            column_id: col_id,
            null_count: Some(0),
            min_value: Some("1".into()),
            max_value: Some("100".into()),
        }];
        let entry = ReplaceFileEntry {
            file_info: DataFileInfo::new("c.parquet", 2000, 200).with_column_stats(stats),
            partition_values: vec![],
        };
        let snap2 = writer.create_snapshot().unwrap();
        let ids = writer
            .replace_table_files(table_id, snap2, &[entry])
            .unwrap();
        assert_eq!(ids.len(), 1);
        let row =
            sqlx::query("SELECT table_id FROM ducklake_file_column_stats WHERE data_file_id = ?")
                .bind(ids[0])
                .fetch_one(&writer.pool)
                .await
                .unwrap();
        let tid: i64 = row.try_get(0).unwrap();
        assert_eq!(tid, table_id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_replace_table_files_row_id_start() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "t", None, snapshot_id)
            .unwrap();
        let cols = vec![ColumnDef::new("id", "int64", false).unwrap()];
        writer.set_columns(table_id, &cols, snapshot_id).unwrap();
        let file = DataFileInfo::new("f1.parquet", 1000, 100);
        writer
            .register_data_file(table_id, snapshot_id, &file)
            .unwrap();
        let entries = vec![
            ReplaceFileEntry {
                file_info: DataFileInfo::new("a.parquet", 500, 50),
                partition_values: vec![],
            },
            ReplaceFileEntry {
                file_info: DataFileInfo::new("b.parquet", 600, 70),
                partition_values: vec![],
            },
        ];
        let snap2 = writer.create_snapshot().unwrap();
        let ids = writer
            .replace_table_files(table_id, snap2, &entries)
            .unwrap();
        assert_eq!(ids.len(), 2);
        let r1 = sqlx::query("SELECT row_id_start FROM ducklake_data_file WHERE data_file_id = ?")
            .bind(ids[0])
            .fetch_one(&writer.pool)
            .await
            .unwrap();
        assert_eq!(r1.try_get::<i64, _>(0).unwrap(), 0);
        let r2 = sqlx::query("SELECT row_id_start FROM ducklake_data_file WHERE data_file_id = ?")
            .bind(ids[1])
            .fetch_one(&writer.pool)
            .await
            .unwrap();
        assert_eq!(r2.try_get::<i64, _>(0).unwrap(), 50);
    }

    // R7-S-009: validate_ducklake_type_for_ddl test moved to metadata_writer_validation

    // --- R9-S-001: Direct unit test for register_dml_files ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_dml_files_with_insert_and_delete() {
        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "t", None, snapshot_id)
            .unwrap();
        let cols = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("val", "varchar", true).unwrap(),
        ];
        let col_ids = writer.set_columns(table_id, &cols, snapshot_id).unwrap();

        // Register an initial data file so we have a data_file_id to reference
        let initial_file = DataFileInfo::new("initial.parquet", 2000, 200);
        let data_file_id = writer
            .register_data_file(table_id, snapshot_id, &initial_file)
            .unwrap();
        assert_eq!(data_file_id, 1);

        // Now register DML files: a delete file (referencing the initial data file)
        // and a new insert data file with column stats
        let snap2 = writer.create_snapshot().unwrap();

        let delete_file = DeleteFileInfo {
            data_file_id,
            path: "delete1.parquet".to_string(),
            path_is_relative: true,
            file_size_bytes: 500,
            footer_size: Some(50),
            delete_count: 10,
        };

        let insert_file = DataFileInfo::new("insert1.parquet", 1500, 150).with_column_stats(vec![
            ColumnStatInfo {
                column_id: col_ids[0],
                null_count: Some(0),
                min_value: Some("100".into()),
                max_value: Some("200".into()),
            },
            ColumnStatInfo {
                column_id: col_ids[1],
                null_count: Some(5),
                min_value: Some("aaa".into()),
                max_value: Some("zzz".into()),
            },
        ]);

        writer
            .register_dml_files(table_id, snap2, &[delete_file], &[insert_file])
            .unwrap();

        // Verify delete file was created with correct data_file_id linkage
        let del_rows = block_on(async {
            sqlx::query(
                "SELECT data_file_id, delete_count, path FROM ducklake_delete_file
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(del_rows.len(), 1);
        let linked_data_id: i64 = del_rows[0].try_get(0).unwrap();
        let del_count: i64 = del_rows[0].try_get(1).unwrap();
        let del_path: String = del_rows[0].try_get(2).unwrap();
        assert_eq!(linked_data_id, data_file_id);
        assert_eq!(del_count, 10);
        assert_eq!(del_path, "delete1.parquet");

        // Verify new data file was inserted
        let data_rows = block_on(async {
            sqlx::query(
                "SELECT data_file_id, path, record_count FROM ducklake_data_file
                 WHERE table_id = ? AND end_snapshot IS NULL AND path = 'insert1.parquet'",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(data_rows.len(), 1);
        let new_file_id: i64 = data_rows[0].try_get(0).unwrap();
        assert!(new_file_id > data_file_id);

        // Verify per-file column stats were recorded for the new data file
        let stats_rows = block_on(async {
            sqlx::query(
                "SELECT column_id, null_count, min_value, max_value
                 FROM ducklake_file_column_stats
                 WHERE data_file_id = ? AND table_id = ?
                 ORDER BY column_id",
            )
            .bind(new_file_id)
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(stats_rows.len(), 2);
        let stat_col1: i64 = stats_rows[0].try_get(0).unwrap();
        let stat_null1: Option<i64> = stats_rows[0].try_get(1).unwrap();
        let stat_min1: Option<String> = stats_rows[0].try_get(2).unwrap();
        let stat_max1: Option<String> = stats_rows[0].try_get(3).unwrap();
        assert_eq!(stat_col1, col_ids[0]);
        assert_eq!(stat_null1, Some(0));
        assert_eq!(stat_min1.as_deref(), Some("100"));
        assert_eq!(stat_max1.as_deref(), Some("200"));

        // Verify table-level column stats were recomputed
        let table_stats = block_on(async {
            sqlx::query(
                "SELECT column_id, contains_null, min_value, max_value
                 FROM ducklake_table_column_stats WHERE table_id = ?
                 ORDER BY column_id",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert!(!table_stats.is_empty());
    }

    // --- R9-S-007: Tests for macro-generated methods ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_record_snapshot_changes() {
        let (writer, _temp) = create_test_writer().await;
        let snap = writer.create_snapshot().unwrap();

        writer.record_snapshot_changes(snap, "test_change").unwrap();

        // Verify
        let rows = block_on(async {
            sqlx::query("SELECT changes_made FROM ducklake_snapshot_changes WHERE snapshot_id = ?")
                .bind(snap)
                .fetch_all(&writer.pool)
                .await
                .unwrap()
        });
        assert_eq!(rows.len(), 1);
        let changes: String = rows[0].try_get(0).unwrap();
        assert_eq!(changes, "test_change");

        // Upsert should overwrite
        writer
            .record_snapshot_changes(snap, "updated_change")
            .unwrap();

        let rows2 = block_on(async {
            sqlx::query("SELECT changes_made FROM ducklake_snapshot_changes WHERE snapshot_id = ?")
                .bind(snap)
                .fetch_all(&writer.pool)
                .await
                .unwrap()
        });
        assert_eq!(rows2.len(), 1);
        let changes2: String = rows2[0].try_get(0).unwrap();
        assert_eq!(changes2, "updated_change");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_find_table_id() {
        let (writer, _temp) = create_test_writer().await;
        let snap = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer.get_or_create_schema("myschema", None, snap).unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "mytable", None, snap)
            .unwrap();

        // Found
        let found = writer.find_table_id("myschema", "mytable").unwrap();
        assert_eq!(found, Some(table_id));

        // Not found - wrong table name
        let not_found = writer.find_table_id("myschema", "nonexistent").unwrap();
        assert_eq!(not_found, None);

        // Not found - wrong schema name
        let not_found2 = writer.find_table_id("other", "mytable").unwrap();
        assert_eq!(not_found2, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_delete_file() {
        let (writer, _temp) = create_test_writer().await;
        let snap = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer.get_or_create_schema("main", None, snap).unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "t", None, snap)
            .unwrap();

        // Register a data file first
        let data_file = DataFileInfo::new("data.parquet", 1000, 100);
        let data_file_id = writer
            .register_data_file(table_id, snap, &data_file)
            .unwrap();

        // Register delete file referencing the data file
        let del_file = DeleteFileInfo {
            data_file_id,
            path: "del.parquet".to_string(),
            path_is_relative: true,
            file_size_bytes: 200,
            footer_size: Some(20),
            delete_count: 5,
        };
        let del_file_id = writer
            .register_delete_file(table_id, snap, &del_file)
            .unwrap();
        assert!(del_file_id > 0);

        // Verify
        let rows = block_on(async {
            sqlx::query(
                "SELECT data_file_id, path, delete_count FROM ducklake_delete_file
                 WHERE delete_file_id = ?",
            )
            .bind(del_file_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].try_get::<i64, _>(0).unwrap(), data_file_id);
        assert_eq!(rows[0].try_get::<String, _>(1).unwrap(), "del.parquet");
        assert_eq!(rows[0].try_get::<i64, _>(2).unwrap(), 5);

        // Register a second delete file for the same data file - should end the first
        let snap2 = writer.create_snapshot().unwrap();
        let del_file2 = DeleteFileInfo {
            data_file_id,
            path: "del2.parquet".to_string(),
            path_is_relative: true,
            file_size_bytes: 300,
            footer_size: Some(30),
            delete_count: 8,
        };
        let del_file_id2 = writer
            .register_delete_file(table_id, snap2, &del_file2)
            .unwrap();
        assert!(del_file_id2 > del_file_id);

        // First delete file should now be ended
        let ended = block_on(async {
            sqlx::query("SELECT end_snapshot FROM ducklake_delete_file WHERE delete_file_id = ?")
                .bind(del_file_id)
                .fetch_one(&writer.pool)
                .await
                .unwrap()
        });
        let end_snap: Option<i64> = ended.try_get(0).unwrap();
        assert!(end_snap.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_and_drop_view() {
        let (writer, _temp) = create_test_writer().await;
        let snap = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer.get_or_create_schema("main", None, snap).unwrap();

        // Create view
        let (view_id, create_snap) = writer
            .create_view(schema_id, "v1", "SELECT 42 AS answer")
            .unwrap();
        assert!(view_id > 0);
        assert!(create_snap > snap);

        // Verify view exists
        let rows = block_on(async {
            sqlx::query(
                "SELECT view_name, sql FROM ducklake_view
                 WHERE view_id = ? AND end_snapshot IS NULL",
            )
            .bind(view_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].try_get::<String, _>(0).unwrap(), "v1");
        assert_eq!(
            rows[0].try_get::<String, _>(1).unwrap(),
            "SELECT 42 AS answer"
        );

        // Drop view
        let drop_snap = writer.drop_view(view_id).unwrap();
        assert!(drop_snap > create_snap);

        // Verify view is ended
        let active = block_on(async {
            sqlx::query(
                "SELECT COUNT(*) FROM ducklake_view WHERE view_id = ? AND end_snapshot IS NULL",
            )
            .bind(view_id)
            .fetch_one(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(active.try_get::<i64, _>(0).unwrap(), 0);

        // Drop again should fail
        let result = writer.drop_view(view_id);
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_drop_table() {
        let (writer, _temp) = create_test_writer().await;
        let snap = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer.get_or_create_schema("main", None, snap).unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "to_drop", None, snap)
            .unwrap();
        let cols = vec![ColumnDef::new("id", "int64", false).unwrap()];
        writer.set_columns(table_id, &cols, snap).unwrap();

        // Register a file so drop also ends files
        let file = DataFileInfo::new("f.parquet", 100, 10);
        writer.register_data_file(table_id, snap, &file).unwrap();

        let drop_snap = writer.drop_table(table_id).unwrap();
        assert!(drop_snap > snap);

        // Verify table is ended
        let active_tables = block_on(async {
            sqlx::query(
                "SELECT COUNT(*) FROM ducklake_table WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_one(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(active_tables.try_get::<i64, _>(0).unwrap(), 0);

        // Verify columns are ended
        let active_cols = block_on(async {
            sqlx::query(
                "SELECT COUNT(*) FROM ducklake_column WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_one(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(active_cols.try_get::<i64, _>(0).unwrap(), 0);

        // Verify data files are ended
        let active_files = block_on(async {
            sqlx::query(
                "SELECT COUNT(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_one(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(active_files.try_get::<i64, _>(0).unwrap(), 0);

        // Drop again should fail
        assert!(writer.drop_table(table_id).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_drop_schema() {
        let (writer, _temp) = create_test_writer().await;
        let snap = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer.get_or_create_schema("to_drop", None, snap).unwrap();

        let drop_snap = writer.drop_schema(schema_id).unwrap();
        assert!(drop_snap > snap);

        // Verify schema is ended
        let active = block_on(async {
            sqlx::query(
                "SELECT COUNT(*) FROM ducklake_schema WHERE schema_id = ? AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .fetch_one(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(active.try_get::<i64, _>(0).unwrap(), 0);

        // Drop again should fail
        assert!(writer.drop_schema(schema_id).is_err());
    }

    // --- R9-S-012: Test recompute_table_column_stats ---

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recompute_table_column_stats_aggregation() {
        let (writer, _temp) = create_test_writer().await;
        let snap = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer.get_or_create_schema("main", None, snap).unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "stats_test", None, snap)
            .unwrap();
        let cols = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        let col_ids = writer.set_columns(table_id, &cols, snap).unwrap();

        // Register two data files with different stats
        let file1 = DataFileInfo::new("f1.parquet", 1000, 100);
        let file1_id = writer.register_data_file(table_id, snap, &file1).unwrap();

        let file2 = DataFileInfo::new("f2.parquet", 2000, 200);
        let file2_id = writer.register_data_file(table_id, snap, &file2).unwrap();

        // Register column stats for file1: id=[10,50], name=["alice","charlie"], no nulls
        let stats1 = vec![
            ColumnStatInfo {
                column_id: col_ids[0],
                null_count: Some(0),
                min_value: Some("10".into()),
                max_value: Some("50".into()),
            },
            ColumnStatInfo {
                column_id: col_ids[1],
                null_count: Some(0),
                min_value: Some("alice".into()),
                max_value: Some("charlie".into()),
            },
        ];
        writer
            .register_column_stats(file1_id, table_id, &stats1)
            .unwrap();

        // Register column stats for file2: id=[5,100], name=["bob","zebra"], has nulls
        let stats2 = vec![
            ColumnStatInfo {
                column_id: col_ids[0],
                null_count: Some(0),
                min_value: Some("5".into()),
                max_value: Some("100".into()),
            },
            ColumnStatInfo {
                column_id: col_ids[1],
                null_count: Some(3),
                min_value: Some("bob".into()),
                max_value: Some("zebra".into()),
            },
        ];
        writer
            .register_column_stats(file2_id, table_id, &stats2)
            .unwrap();

        // Verify aggregated table-level column stats
        let table_stats = block_on(async {
            sqlx::query(
                "SELECT column_id, contains_null, min_value, max_value
                 FROM ducklake_table_column_stats WHERE table_id = ?
                 ORDER BY column_id",
            )
            .bind(table_id)
            .fetch_all(&writer.pool)
            .await
            .unwrap()
        });
        assert_eq!(table_stats.len(), 2);

        // id column: min=5 (numeric), max=100 (numeric), no nulls
        let id_null: bool = table_stats[0].try_get(1).unwrap();
        let id_min: Option<String> = table_stats[0].try_get(2).unwrap();
        let id_max: Option<String> = table_stats[0].try_get(3).unwrap();
        assert!(!id_null);
        assert_eq!(id_min.as_deref(), Some("5"));
        assert_eq!(id_max.as_deref(), Some("100"));

        // name column: min="alice" (string), max="zebra" (string), has nulls
        let name_null: bool = table_stats[1].try_get(1).unwrap();
        let name_min: Option<String> = table_stats[1].try_get(2).unwrap();
        let name_max: Option<String> = table_stats[1].try_get(3).unwrap();
        assert!(name_null);
        assert_eq!(name_min.as_deref(), Some("alice"));
        assert_eq!(name_max.as_deref(), Some("zebra"));
    }

    /// R7-S-012: After ALTER TABLE ADD COLUMN, store_inlined_data detects
    /// schema mismatch and recreates the inlined data table with updated columns.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_inlined_data_schema_evolution_after_alter() {
        use crate::metadata_provider::InlinedDataRow;

        let (writer, _temp) = create_test_writer().await;
        let snapshot_id = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("main", None, snapshot_id)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "items", None, snapshot_id)
            .unwrap();

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        writer.set_columns(table_id, &columns, snapshot_id).unwrap();

        // Enable inlining
        block_on(async {
            sqlx::query("INSERT INTO ducklake_metadata (key, value) VALUES ('data_inlining_row_limit', '100')")
                .execute(&writer.pool)
                .await
                .unwrap();
        });

        // Store inlined data with original 2-column schema
        let col_names = Arc::new(vec!["id".to_string(), "name".to_string()]);
        let rows = vec![InlinedDataRow {
            column_names: col_names.clone(),
            values: vec![Some("1".to_string()), Some("alice".to_string())],
        }];
        let stored = writer
            .store_inlined_data(table_id, snapshot_id, &columns, &rows)
            .unwrap();
        assert_eq!(stored, 1);

        // Check original schema_version in registry
        let sv_row = block_on(async {
            sqlx::query(
                "SELECT schema_version FROM ducklake_inlined_data_tables WHERE table_id = ?",
            )
            .bind(table_id)
            .fetch_one(&writer.pool)
            .await
            .unwrap()
        });
        let original_sv: i64 = sv_row.try_get(0).unwrap();

        // ALTER TABLE ADD COLUMN (bumps schema_version)
        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("email", "varchar", true).unwrap(),
        };
        let alter_snapshot = writer.alter_table(table_id, &op).unwrap();

        // Store inlined data with updated 3-column schema
        let new_columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
            ColumnDef::new("email", "varchar", true).unwrap(),
        ];
        let col_names3 = Arc::new(vec![
            "id".to_string(),
            "name".to_string(),
            "email".to_string(),
        ]);
        let rows2 = vec![InlinedDataRow {
            column_names: col_names3,
            values: vec![
                Some("2".to_string()),
                Some("bob".to_string()),
                Some("bob@test.com".to_string()),
            ],
        }];
        let stored2 = writer
            .store_inlined_data(table_id, alter_snapshot, &new_columns, &rows2)
            .unwrap();
        assert_eq!(stored2, 1);

        // Verify schema_version was bumped in registry
        let new_sv_row = block_on(async {
            sqlx::query(
                "SELECT schema_version, table_name FROM ducklake_inlined_data_tables WHERE table_id = ?",
            )
            .bind(table_id)
            .fetch_one(&writer.pool)
            .await
            .unwrap()
        });
        let new_sv: i64 = new_sv_row.try_get(0).unwrap();
        let new_tbl: String = new_sv_row.try_get(1).unwrap();
        assert!(
            new_sv > original_sv,
            "schema_version should have been bumped"
        );

        // Verify new table has the email column
        let cols = block_on(async {
            let sql = format!("PRAGMA table_info({})", SqliteDialect.quote_id(&new_tbl));
            sqlx::query(&sql).fetch_all(&writer.pool).await.unwrap()
        });
        let col_names_in_table: Vec<String> = cols
            .iter()
            .map(|r| r.try_get::<String, _>(1).unwrap())
            .collect();
        assert!(
            col_names_in_table.contains(&"email".to_string()),
            "New table should have email column, got: {:?}",
            col_names_in_table
        );
    }
}
