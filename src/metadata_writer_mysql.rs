//! MySQL implementation of [`MetadataWriter`].
//!
//! Requires multi-threaded Tokio runtime (`#[tokio::test(flavor = "multi_thread")]`).

use crate::Result;
use crate::error::DuckLakeError;
use crate::metadata_provider::block_on;
use crate::metadata_writer::{
    AlterTableOp, ColumnDef, ColumnStatInfo, DataFileInfo, DeleteFileInfo, MetadataWriter,
    ReplaceFileEntry, WriteMode, WriteSetupResult,
};
use crate::metadata_writer_validation::{
    ActiveColumnInfo, AlterTableAction, is_numeric_type, stat_value_less_than,
    validate_alter_table, validate_no_duplicate_columns, validate_schema_evolution,
    validate_table_has_columns,
};
use sqlx::Row;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

const DEFAULT_MAX_CONNECTIONS: u32 = 5;

const SQL_CREATE_METADATA: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_metadata (
    `key` VARCHAR(255) NOT NULL,
    value TEXT NOT NULL,
    scope VARCHAR(255),
    scope_id BIGINT
)"#;

const SQL_CREATE_SNAPSHOT: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_snapshot (
    snapshot_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_time DATETIME(6) DEFAULT NOW(6),
    schema_version BIGINT DEFAULT 1,
    next_catalog_id BIGINT DEFAULT 0,
    next_file_id BIGINT DEFAULT 0
)"#;

const SQL_CREATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_schema (
    schema_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    schema_uuid VARCHAR(255),
    schema_name VARCHAR(255) NOT NULL,
    path VARCHAR(4096) NOT NULL DEFAULT '',
    path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_table (
    table_id BIGINT NOT NULL,
    table_uuid VARCHAR(255),
    schema_id BIGINT NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    path VARCHAR(4096) NOT NULL DEFAULT '',
    path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_COLUMN: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_column (
    column_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    column_type VARCHAR(255) NOT NULL,
    column_order INTEGER NOT NULL,
    nulls_allowed BOOLEAN DEFAULT TRUE,
    initial_default TEXT,
    default_value TEXT,
    parent_column BIGINT,
    default_value_type VARCHAR(255),
    default_value_dialect VARCHAR(255),
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_DATA_FILE: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_data_file (
    data_file_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
    file_size_bytes BIGINT NOT NULL,
    footer_size BIGINT,
    encryption_key VARCHAR(255),
    record_count BIGINT,
    row_id_start BIGINT,
    mapping_id BIGINT,
    file_order INTEGER,
    file_format VARCHAR(255) DEFAULT 'parquet',
    partition_id BIGINT,
    partial_max BIGINT,
    partial_file_info TEXT,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_DELETE_FILE: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_delete_file (
    delete_file_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    data_file_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
    file_size_bytes BIGINT NOT NULL,
    footer_size BIGINT,
    encryption_key VARCHAR(255),
    delete_count BIGINT,
    format VARCHAR(255) DEFAULT 'parquet',
    partial_max BIGINT,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_SNAPSHOT_CHANGES: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_snapshot_changes (
    snapshot_id BIGINT PRIMARY KEY,
    changes_made TEXT,
    author VARCHAR(255),
    commit_message TEXT,
    commit_extra_info TEXT
)"#;

const SQL_CREATE_CHANGE_TRACKING: &str = r#"
CREATE TABLE IF NOT EXISTS _df_change_tracking (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_id BIGINT NOT NULL,
    change_type VARCHAR(255) NOT NULL,
    table_id BIGINT,
    schema_id BIGINT
)"#;

/// Sequence table for concurrent-safe ID generation in MySQL (R5-S-027).
/// Replaces the race-prone `MAX(id)+1 FOR UPDATE` pattern.
const SQL_CREATE_SEQUENCES: &str = r#"
CREATE TABLE IF NOT EXISTS _df_sequences (
    seq_name VARCHAR(255) PRIMARY KEY,
    seq_value BIGINT NOT NULL DEFAULT 0
)"#;

const SQL_CREATE_FILE_COLUMN_STATS: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_file_column_stats (
    data_file_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    column_size_bytes BIGINT,
    value_count BIGINT,
    null_count BIGINT,
    min_value VARCHAR(1024),
    max_value VARCHAR(1024),
    contains_nan BOOLEAN,
    extra_stats VARCHAR(1024)
)"#;

const SQL_CREATE_VIEW: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_view (
    view_id BIGINT NOT NULL,
    view_uuid VARCHAR(255),
    schema_id BIGINT NOT NULL,
    view_name VARCHAR(255) NOT NULL,
    dialect VARCHAR(255),
    `sql` TEXT NOT NULL,
    column_aliases TEXT,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_TAG: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_tag (
    object_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    `key` VARCHAR(255),
    value VARCHAR(1024)
)"#;

const SQL_CREATE_COLUMN_TAG: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_column_tag (
    table_id BIGINT,
    column_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    `key` VARCHAR(255),
    value VARCHAR(1024)
)"#;

const SQL_CREATE_TABLE_STATS: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_table_stats (
    table_id BIGINT PRIMARY KEY,
    record_count BIGINT,
    next_row_id BIGINT,
    file_size_bytes BIGINT
)"#;

const SQL_CREATE_TABLE_COLUMN_STATS: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_table_column_stats (
    table_id BIGINT,
    column_id BIGINT,
    contains_null BOOLEAN,
    contains_nan BOOLEAN,
    min_value VARCHAR(1024),
    max_value VARCHAR(1024),
    extra_stats VARCHAR(1024)
)"#;

const SQL_CREATE_PARTITION_INFO: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_partition_info (
    partition_id BIGINT,
    table_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_PARTITION_COLUMN: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_partition_column (
    partition_id BIGINT,
    table_id BIGINT,
    partition_key_index BIGINT,
    column_id BIGINT,
    transform VARCHAR(255)
)"#;

const SQL_CREATE_FILE_PARTITION_VALUE: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_file_partition_value (
    data_file_id BIGINT,
    table_id BIGINT,
    partition_key_index BIGINT,
    partition_value VARCHAR(1024)
)"#;

const SQL_CREATE_FILES_SCHEDULED_FOR_DELETION: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_files_scheduled_for_deletion (
    data_file_id BIGINT,
    path TEXT,
    path_is_relative BOOLEAN,
    schedule_start DATETIME(6)
)"#;

const SQL_CREATE_INLINED_DATA_TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_inlined_data_tables (
    table_id BIGINT,
    table_name VARCHAR(255),
    schema_version BIGINT
)"#;

const SQL_CREATE_COLUMN_MAPPING: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_column_mapping (
    mapping_id BIGINT,
    table_id BIGINT,
    `type` VARCHAR(255)
)"#;

const SQL_CREATE_NAME_MAPPING: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_name_mapping (
    mapping_id BIGINT,
    column_id BIGINT,
    source_name VARCHAR(255),
    target_field_id BIGINT,
    parent_column BIGINT,
    is_partition BOOLEAN
)"#;

const SQL_CREATE_SCHEMA_VERSIONS: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_schema_versions (
    begin_snapshot BIGINT,
    schema_version BIGINT,
    table_id BIGINT
)"#;

const SQL_CREATE_MACRO: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_macro (
    schema_id BIGINT,
    macro_id BIGINT,
    macro_name VARCHAR(255),
    begin_snapshot BIGINT,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_MACRO_IMPL: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_macro_impl (
    macro_id BIGINT,
    impl_id BIGINT,
    dialect VARCHAR(255),
    `sql` TEXT,
    `type` VARCHAR(255)
)"#;

const SQL_CREATE_MACRO_PARAMETERS: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_macro_parameters (
    macro_id BIGINT,
    impl_id BIGINT,
    column_id BIGINT,
    parameter_name VARCHAR(255),
    parameter_type VARCHAR(255),
    default_value VARCHAR(1024),
    default_value_type VARCHAR(255)
)"#;

const SQL_CREATE_SORT_INFO: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_sort_info (
    sort_id BIGINT,
    table_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_SORT_EXPRESSION: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_sort_expression (
    sort_id BIGINT,
    table_id BIGINT,
    sort_key_index BIGINT,
    expression VARCHAR(1024),
    dialect VARCHAR(255),
    sort_direction VARCHAR(255),
    null_order VARCHAR(255)
)"#;

const SQL_CREATE_FILE_VARIANT_STATS: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_file_variant_stats (
    data_file_id BIGINT,
    table_id BIGINT,
    column_id BIGINT,
    variant_path VARCHAR(1024),
    shredded_type VARCHAR(255),
    column_size_bytes BIGINT,
    value_count BIGINT,
    null_count BIGINT,
    min_value VARCHAR(1024),
    max_value VARCHAR(1024),
    contains_nan BOOLEAN,
    extra_stats VARCHAR(1024)
)"#;

/// Atomically allocate the next ID from a named sequence (R5-S-027).
/// Uses `UPDATE ... SET seq_value = seq_value + 1` with row-level locking
/// for concurrent-safe ID generation without the MAX+1 race.
async fn next_sequence_id(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    seq_name: &str,
) -> Result<i64> {
    next_sequence_ids(tx, seq_name, 1).await
}

/// Atomically allocate `count` sequential IDs from a named sequence (R5-S-027).
/// Returns the first ID in the allocated range. The allocated IDs are
/// `[first_id, first_id+1, ..., first_id+count-1]`.
async fn next_sequence_ids(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    seq_name: &str,
    count: i64,
) -> Result<i64> {
    // Ensure the sequence row exists
    sqlx::query("INSERT IGNORE INTO _df_sequences (seq_name, seq_value) VALUES (?, 0)")
        .bind(seq_name)
        .execute(&mut **tx)
        .await?;

    // Atomically increment by count and return the new value
    sqlx::query("UPDATE _df_sequences SET seq_value = seq_value + ? WHERE seq_name = ?")
        .bind(count)
        .bind(seq_name)
        .execute(&mut **tx)
        .await?;

    let row = sqlx::query("SELECT seq_value FROM _df_sequences WHERE seq_name = ?")
        .bind(seq_name)
        .fetch_one(&mut **tx)
        .await?;
    let end_value: i64 = row.try_get(0)?;
    // Return the first ID in the range (end - count + 1)
    Ok(end_value - count + 1)
}

/// Helper to get the last inserted auto-increment ID from a MySQL transaction.
async fn last_insert_id(tx: &mut sqlx::Transaction<'_, sqlx::MySql>) -> Result<i64> {
    let row = sqlx::query("SELECT CAST(LAST_INSERT_ID() AS SIGNED) as id")
        .fetch_one(&mut **tx)
        .await?;
    Ok(row.try_get(0)?)
}

/// Helper to get the last inserted auto-increment ID from a MySQL connection.
async fn last_insert_id_conn(conn: &mut sqlx::pool::PoolConnection<sqlx::MySql>) -> Result<i64> {
    let row = sqlx::query("SELECT CAST(LAST_INSERT_ID() AS SIGNED) as id")
        .fetch_one(&mut **conn)
        .await?;
    Ok(row.try_get(0)?)
}

/// MySQL-based metadata writer for DuckLake catalogs.
#[derive(Debug, Clone)]
pub struct MySqlMetadataWriter {
    pool: MySqlPool,
}

impl MySqlMetadataWriter {
    pub async fn new(connection_string: &str) -> Result<Self> {
        Self::with_max_connections(connection_string, DEFAULT_MAX_CONNECTIONS).await
    }

    pub async fn with_max_connections(
        connection_string: &str,
        max_connections: u32,
    ) -> Result<Self> {
        let pool = MySqlPoolOptions::new()
            .max_connections(max_connections)
            .connect(connection_string)
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
        mut tx: sqlx::Transaction<'_, sqlx::MySql>,
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

        // Get schema_version for the new snapshot (F-012, R8-S-005).
        // For DDL, use the _df_sequences table to atomically allocate the next version,
        // preventing concurrent DDL from producing duplicate schema_versions.
        let new_schema_version: i64 = if is_ddl {
            next_sequence_value(&mut tx, "schema_version").await?
        } else {
            let prev_sv_row =
                sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                    .fetch_one(&mut *tx)
                    .await?;
            prev_sv_row.try_get(0)?
        };

        // Create snapshot with correct schema_version
        sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
        )
        .bind(new_schema_version)
        .execute(&mut *tx)
        .await?;
        let snapshot_id = last_insert_id(&mut tx).await?;

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
            sqlx::query(
                "INSERT INTO ducklake_schema (schema_uuid, schema_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, TRUE, ?)",
            )
            .bind(&schema_uuid)
            .bind(schema_name)
            .bind(&schema_path)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;
            last_insert_id(&mut tx).await?
        };

        // Create table if needed (F-026: generate UUID)
        let table_id: i64 = if let Some(t_row) = existing_table {
            t_row.try_get(0)?
        } else {
            let next_table_id = next_sequence_id(&mut tx, "table_id").await?;

            let table_path = format!("{}/", table_name);
            let table_uuid = uuid::Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, TRUE, ?)",
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
                |((name, col_type, _nullable), col)| {
                    name == &col.name && col_type == &col.ducklake_type
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

            let first_column_id =
                next_sequence_ids(&mut tx, "column_id", columns.len() as i64).await?;

            let mut new_ids = Vec::with_capacity(columns.len());
            for (order, col) in columns.iter().enumerate() {
                let column_id = first_column_id + order as i64;
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
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!(
                "created_table:\"{}\".\"{}\"",
                schema_name.replace('"', "\"\""),
                table_name.replace('"', "\"\"")
            ))
            .execute(&mut *tx)
            .await?;
        }

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

    /// Inner drop table logic, usable with an existing transaction.
    async fn drop_table_inner(
        mut tx: sqlx::Transaction<'_, sqlx::MySql>,
        table_id: i64,
    ) -> Result<i64> {
        // R4-S-014: Validate table exists and is active before creating snapshot
        let exists = sqlx::query(
            "SELECT COUNT(*) FROM ducklake_table WHERE table_id = ? AND end_snapshot IS NULL",
        )
        .bind(table_id)
        .fetch_one(&mut *tx)
        .await?;
        if exists.try_get::<i64, _>(0)? == 0 {
            return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                "Table with id {} not found or already dropped",
                table_id
            )));
        }

        // Increment schema_version for DDL (F-012)
        let prev_sv_row =
            sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                .fetch_one(&mut *tx)
                .await?;
        let new_schema_version: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

        sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
        )
        .bind(new_schema_version)
        .execute(&mut *tx)
        .await?;
        let snapshot_id = last_insert_id(&mut tx).await?;

        sqlx::query(
            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)",
        )
        .bind(snapshot_id)
        .bind(new_schema_version)
        .execute(&mut *tx)
        .await?;

        // Mark the table as dropped
        sqlx::query(
            "UPDATE ducklake_table SET end_snapshot = ?
             WHERE table_id = ? AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        // End all active columns
        sqlx::query(
            "UPDATE ducklake_column SET end_snapshot = ?
             WHERE table_id = ? AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        // End all active data files
        sqlx::query(
            "UPDATE ducklake_data_file SET end_snapshot = ?
             WHERE table_id = ? AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        // End all active delete files
        sqlx::query(
            "UPDATE ducklake_delete_file SET end_snapshot = ?
             WHERE table_id = ? AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        // Record the change for conflict detection
        sqlx::query(
            "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id)
             VALUES (?, 'DROP_TABLE', ?)",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        // Record in spec-compliant snapshot changes
        sqlx::query(
            "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
             VALUES (?, ?)
             ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
        )
        .bind(snapshot_id)
        .bind(format!("dropped_table:{}", table_id))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(snapshot_id)
    }

    /// Inner drop schema logic, usable with an existing transaction.
    async fn drop_schema_inner(
        mut tx: sqlx::Transaction<'_, sqlx::MySql>,
        schema_id: i64,
    ) -> Result<i64> {
        // R4-S-014: Validate schema exists and is active before creating snapshot
        let exists = sqlx::query(
            "SELECT COUNT(*) FROM ducklake_schema WHERE schema_id = ? AND end_snapshot IS NULL",
        )
        .bind(schema_id)
        .fetch_one(&mut *tx)
        .await?;
        if exists.try_get::<i64, _>(0)? == 0 {
            return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                "Schema with id {} not found or already dropped",
                schema_id
            )));
        }

        // Increment schema_version for DDL (F-012)
        let prev_sv_row =
            sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                .fetch_one(&mut *tx)
                .await?;
        let new_schema_version: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

        sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
        )
        .bind(new_schema_version)
        .execute(&mut *tx)
        .await?;
        let snapshot_id = last_insert_id(&mut tx).await?;

        sqlx::query(
            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)",
        )
        .bind(snapshot_id)
        .bind(new_schema_version)
        .execute(&mut *tx)
        .await?;

        // Cascade: end columns for all active tables in this schema
        sqlx::query(
            "UPDATE ducklake_column SET end_snapshot = ?
             WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE schema_id = ? AND end_snapshot IS NULL)
             AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // Cascade: end data files for all active tables in this schema
        sqlx::query(
            "UPDATE ducklake_data_file SET end_snapshot = ?
             WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE schema_id = ? AND end_snapshot IS NULL)
             AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // Cascade: end delete files for all active tables in this schema
        sqlx::query(
            "UPDATE ducklake_delete_file SET end_snapshot = ?
             WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE schema_id = ? AND end_snapshot IS NULL)
             AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // End all active tables in this schema
        sqlx::query(
            "UPDATE ducklake_table SET end_snapshot = ?
             WHERE schema_id = ? AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // Mark the schema as dropped
        sqlx::query(
            "UPDATE ducklake_schema SET end_snapshot = ?
             WHERE schema_id = ? AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // Record the change for conflict detection
        sqlx::query(
            "INSERT INTO _df_change_tracking (snapshot_id, change_type, schema_id)
             VALUES (?, 'DROP_SCHEMA', ?)",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // Record in spec-compliant snapshot changes
        sqlx::query(
            "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
             VALUES (?, ?)
             ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
        )
        .bind(snapshot_id)
        .bind(format!("dropped_schema:{}", schema_id))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(snapshot_id)
    }

    /// R7-S-011/R7-S-022: Recompute table-level column stats (parity with SQLite).
    async fn recompute_table_column_stats(
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        table_id: i64,
    ) -> Result<()> {
        use std::collections::HashMap;

        sqlx::query("DELETE FROM ducklake_table_column_stats WHERE table_id = ?")
            .bind(table_id)
            .execute(&mut **tx)
            .await?;

        let rows = sqlx::query(
            "SELECT fcs.column_id, fcs.null_count, fcs.min_value, fcs.max_value, c.column_type
             FROM ducklake_file_column_stats fcs
             INNER JOIN ducklake_data_file df
                 ON fcs.data_file_id = df.data_file_id
                 AND df.table_id = fcs.table_id
                 AND df.end_snapshot IS NULL
             INNER JOIN ducklake_column c ON fcs.column_id = c.column_id
             WHERE fcs.table_id = ?",
        )
        .bind(table_id)
        .fetch_all(&mut **tx)
        .await?;

        if rows.is_empty() {
            return Ok(());
        }

        struct ColumnAgg {
            contains_null: bool,
            min_value: Option<String>,
            max_value: Option<String>,
            is_numeric: bool,
        }

        let mut aggs: HashMap<i64, ColumnAgg> = HashMap::new();

        for row in &rows {
            let column_id: i64 = row.try_get(0)?;
            let null_count: Option<i64> = row.try_get(1)?;
            let min_value: Option<String> = row.try_get(2)?;
            let max_value: Option<String> = row.try_get(3)?;
            let column_type: String = row.try_get(4)?;

            let is_numeric = is_numeric_type(&column_type);

            let entry = aggs.entry(column_id).or_insert(ColumnAgg {
                contains_null: false,
                min_value: None,
                max_value: None,
                is_numeric,
            });

            if null_count.unwrap_or(0) > 0 {
                entry.contains_null = true;
            }

            if let Some(ref new_min) = min_value {
                entry.min_value = Some(match &entry.min_value {
                    None => new_min.clone(),
                    Some(current) => {
                        if stat_value_less_than(new_min, current, entry.is_numeric) {
                            new_min.clone()
                        } else {
                            current.clone()
                        }
                    },
                });
            }

            if let Some(ref new_max) = max_value {
                entry.max_value = Some(match &entry.max_value {
                    None => new_max.clone(),
                    Some(current) => {
                        if stat_value_less_than(current, new_max, entry.is_numeric) {
                            new_max.clone()
                        } else {
                            current.clone()
                        }
                    },
                });
            }
        }

        for (column_id, agg) in &aggs {
            sqlx::query(
                "INSERT INTO ducklake_table_column_stats
                 (table_id, column_id, contains_null, min_value, max_value)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(table_id)
            .bind(column_id)
            .bind(agg.contains_null)
            .bind(&agg.min_value)
            .bind(&agg.max_value)
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }
}

impl MetadataWriter for MySqlMetadataWriter {
    fn create_snapshot(&self) -> Result<i64> {
        block_on(async {
            let mut conn = self.pool.acquire().await?;
            sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
                .execute(&mut *conn)
                .await?;
            last_insert_id_conn(&mut conn).await
        })
    }

    fn get_or_create_schema(
        &self,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        block_on(async {
            // Use a transaction to prevent TOCTOU race between SELECT and INSERT
            let mut tx = self.pool.begin().await?;

            // R8-S-006: Use FOR UPDATE to acquire a gap lock under REPEATABLE READ,
            // preventing concurrent transactions from inserting a duplicate active schema.
            let existing = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = ? AND end_snapshot IS NULL
                 FOR UPDATE",
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
            // F-026: generate UUID
            let schema_uuid = uuid::Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO ducklake_schema (schema_uuid, schema_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, TRUE, ?)",
            )
            .bind(&schema_uuid)
            .bind(name)
            .bind(&schema_path)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;
            let id = last_insert_id(&mut tx).await?;

            tx.commit().await?;
            Ok((id, true))
        })
    }

    fn get_or_create_table(
        &self,
        schema_id: i64,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // R8-S-006: Use FOR UPDATE to acquire a gap lock under REPEATABLE READ,
            // preventing concurrent transactions from inserting a duplicate active table.
            let existing = sqlx::query(
                "SELECT table_id FROM ducklake_table
                 WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL
                 FOR UPDATE",
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
            let next_table_id = next_sequence_id(&mut tx, "table_id").await?;

            // F-026: generate UUID
            let table_uuid = uuid::Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, TRUE, ?)",
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
        block_on(async {
            let mut tx = self.pool.begin().await?;

            sqlx::query(
                "UPDATE ducklake_column SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            let next_column_id =
                next_sequence_ids(&mut tx, "column_id", columns.len() as i64).await?;

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

    fn register_column_stats(
        &self,
        data_file_id: i64,
        table_id: i64,
        stats: &[ColumnStatInfo],
    ) -> Result<()> {
        if stats.is_empty() {
            return Ok(());
        }
        block_on(async {
            let mut tx = self.pool.begin().await?;
            for stat in stats {
                sqlx::query(
                    "INSERT INTO ducklake_file_column_stats
                     (data_file_id, table_id, column_id, null_count, min_value, max_value)
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(data_file_id)
                .bind(table_id)
                .bind(stat.column_id)
                .bind(stat.null_count)
                .bind(&stat.min_value)
                .bind(&stat.max_value)
                .execute(&mut *tx)
                .await?;
            }

            // R7-S-011: Recompute table-level column stats (parity with SQLite)
            Self::recompute_table_column_stats(&mut tx, table_id).await?;

            tx.commit().await?;
            Ok(())
        })
    }

    fn register_data_file(
        &self,
        table_id: i64,
        snapshot_id: i64,
        file: &DataFileInfo,
    ) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Get current next_row_id from table_stats (F-011: row_id_start)
            // R6-S-033: Use FOR UPDATE to prevent concurrent row_id allocation races
            let stats_row = sqlx::query(
                "SELECT next_row_id FROM ducklake_table_stats WHERE table_id = ? FOR UPDATE",
            )
            .bind(table_id)
            .fetch_optional(&mut *tx)
            .await?;
            let row_id_start: i64 = match stats_row {
                Some(r) => r.try_get::<Option<i64>, _>(0)?.unwrap_or(0),
                None => 0,
            };

            sqlx::query(
                "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, row_id_start, file_format, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, ?, ?, 'parquet', ?)",
            )
            .bind(table_id)
            .bind(&file.path)
            .bind(file.path_is_relative)
            .bind(file.file_size_bytes)
            .bind(file.footer_size)
            .bind(file.record_count)
            .bind(row_id_start)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;

            // Get the auto-generated ID
            let id_row = sqlx::query("SELECT LAST_INSERT_ID()")
                .fetch_one(&mut *tx)
                .await?;
            let data_file_id: i64 = id_row.try_get(0)?;

            // Update ducklake_table_stats (F-012: table_stats population)
            let new_next_row_id = row_id_start
                .checked_add(file.record_count)
                .ok_or_else(|| DuckLakeError::Internal("row_id overflow".into()))?;
            let updated = sqlx::query(
                "UPDATE ducklake_table_stats
                 SET record_count = COALESCE(record_count, 0) + ?,
                     next_row_id = ?,
                     file_size_bytes = COALESCE(file_size_bytes, 0) + ?
                 WHERE table_id = ?",
            )
            .bind(file.record_count)
            .bind(new_next_row_id)
            .bind(file.file_size_bytes)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            if updated.rows_affected() == 0 {
                sqlx::query(
                    "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
                     VALUES (?, ?, ?, ?)",
                )
                .bind(table_id)
                .bind(file.record_count)
                .bind(new_next_row_id)
                .bind(file.file_size_bytes)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            Ok(data_file_id)
        })
    }

    fn end_table_files(&self, table_id: i64, snapshot_id: i64) -> Result<u64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;
            let result = sqlx::query(
                "UPDATE ducklake_data_file SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // R6-S-004: End active delete files in Replace mode (parity with SQLite)
            sqlx::query(
                "UPDATE ducklake_delete_file SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // R6-S-004: Reset table_stats so subsequent INSERTs start from 0
            sqlx::query(
                "UPDATE ducklake_table_stats
                 SET record_count = 0, next_row_id = 0, file_size_bytes = 0
                 WHERE table_id = ?",
            )
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(result.rows_affected())
        })
    }

    // R6-S-018: Atomic replace_table_files override (parity with SQLite)
    fn replace_table_files(
        &self,
        table_id: i64,
        snapshot_id: i64,
        files: &[ReplaceFileEntry],
    ) -> Result<Vec<i64>> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // End all existing data files
            sqlx::query(
                "UPDATE ducklake_data_file SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            let mut ids = Vec::with_capacity(files.len());
            let mut cumulative_row_id: i64 = 0;
            for entry in files {
                let path_is_relative = entry.file_info.path_is_relative;
                sqlx::query(
                    "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, row_id_start, begin_snapshot)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(table_id)
                .bind(&entry.file_info.path)
                .bind(path_is_relative)
                .bind(entry.file_info.file_size_bytes)
                .bind(entry.file_info.footer_size)
                .bind(entry.file_info.record_count)
                .bind(cumulative_row_id)
                .bind(snapshot_id)
                .execute(&mut *tx)
                .await?;

                let id_row = sqlx::query("SELECT LAST_INSERT_ID()")
                    .fetch_one(&mut *tx)
                    .await?;
                let data_file_id: i64 = id_row.try_get(0)?;

                // Register column stats
                for stat in &entry.file_info.column_stats {
                    sqlx::query(
                        "INSERT INTO ducklake_file_column_stats (data_file_id, table_id, column_id, null_count, min_value, max_value)
                         VALUES (?, ?, ?, ?, ?, ?)",
                    )
                    .bind(data_file_id)
                    .bind(table_id)
                    .bind(stat.column_id)
                    .bind(stat.null_count)
                    .bind(&stat.min_value)
                    .bind(&stat.max_value)
                    .execute(&mut *tx)
                    .await?;
                }

                // Register partition values
                for (key_index, val) in &entry.partition_values {
                    sqlx::query(
                        "INSERT INTO ducklake_file_partition_value (data_file_id, table_id, partition_key_index, partition_value)
                         VALUES (?, ?, ?, ?)",
                    )
                    .bind(data_file_id)
                    .bind(table_id)
                    .bind(key_index)
                    .bind(val.as_deref())
                    .execute(&mut *tx)
                    .await?;
                }

                cumulative_row_id = cumulative_row_id
                    .checked_add(entry.file_info.record_count)
                    .ok_or_else(|| {
                        DuckLakeError::Internal("row_id overflow during compaction".into())
                    })?;
                ids.push(data_file_id);
            }

            // Recalculate ducklake_table_stats from new files
            let total_record_count: i64 = files.iter().try_fold(0i64, |acc, f| {
                acc.checked_add(f.file_info.record_count).ok_or_else(|| {
                    DuckLakeError::Internal(
                        "record_count sum overflow in replace_table_files".into(),
                    )
                })
            })?;
            let total_file_size: i64 = files.iter().map(|f| f.file_info.file_size_bytes).sum();
            let updated = sqlx::query(
                "UPDATE ducklake_table_stats
                 SET record_count = ?,
                     next_row_id = ?,
                     file_size_bytes = ?
                 WHERE table_id = ?",
            )
            .bind(total_record_count)
            .bind(total_record_count)
            .bind(total_file_size)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            if updated.rows_affected() == 0 {
                sqlx::query(
                    "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
                     VALUES (?, ?, ?, ?)",
                )
                .bind(table_id)
                .bind(total_record_count)
                .bind(total_record_count)
                .bind(total_file_size)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            Ok(ids)
        })
    }

    fn register_dml_files(
        &self,
        table_id: i64,
        snapshot_id: i64,
        delete_files: &[DeleteFileInfo],
        data_files: &[DataFileInfo],
    ) -> Result<()> {
        if delete_files.is_empty() && data_files.is_empty() {
            return Ok(());
        }
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // R6-S-003: Track net new deletions to decrement record_count (parity with SQLite)
            let mut total_net_new_deletions: i64 = 0;

            for file in delete_files {
                // R6-S-003: Get the old delete_count before ending the existing delete file
                let old_row = sqlx::query(
                    "SELECT COALESCE(delete_count, 0) FROM ducklake_delete_file
                     WHERE data_file_id = ? AND table_id = ? AND end_snapshot IS NULL",
                )
                .bind(file.data_file_id)
                .bind(table_id)
                .fetch_optional(&mut *tx)
                .await?;
                let old_delete_count: i64 = match old_row {
                    Some(r) => r.try_get(0)?,
                    None => 0,
                };

                // End any existing active delete file for this data file
                sqlx::query(
                    "UPDATE ducklake_delete_file SET end_snapshot = ?
                     WHERE data_file_id = ? AND table_id = ? AND end_snapshot IS NULL",
                )
                .bind(snapshot_id)
                .bind(file.data_file_id)
                .bind(table_id)
                .execute(&mut *tx)
                .await?;

                // Insert the new delete file
                sqlx::query(
                    "INSERT INTO ducklake_delete_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, delete_count, format, begin_snapshot)
                     VALUES (?, ?, ?, ?, ?, ?, ?, 'parquet', ?)",
                )
                .bind(file.data_file_id)
                .bind(table_id)
                .bind(&file.path)
                .bind(file.path_is_relative)
                .bind(file.file_size_bytes)
                .bind(file.footer_size)
                .bind(file.delete_count)
                .bind(snapshot_id)
                .execute(&mut *tx)
                .await?;

                total_net_new_deletions += file.delete_count - old_delete_count;
            }

            // R6-S-003 + R7-S-010: Decrement record_count (clamped to 0)
            if total_net_new_deletions > 0 {
                sqlx::query(
                    "UPDATE ducklake_table_stats
                     SET record_count = GREATEST(0, COALESCE(record_count, 0) - ?)
                     WHERE table_id = ?",
                )
                .bind(total_net_new_deletions)
                .bind(table_id)
                .execute(&mut *tx)
                .await?;
            }

            // R3F-002: For each new data file, set row_id_start and update table_stats
            let mut has_column_stats = false;
            for file in data_files {
                let stats_row = sqlx::query(
                    "SELECT next_row_id FROM ducklake_table_stats WHERE table_id = ? FOR UPDATE",
                )
                .bind(table_id)
                .fetch_optional(&mut *tx)
                .await?;
                let row_id_start: i64 = match stats_row {
                    Some(r) => r.try_get::<Option<i64>, _>(0)?.unwrap_or(0),
                    None => 0,
                };

                sqlx::query(
                    "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, row_id_start, begin_snapshot)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(table_id)
                .bind(&file.path)
                .bind(file.path_is_relative)
                .bind(file.file_size_bytes)
                .bind(file.footer_size)
                .bind(file.record_count)
                .bind(row_id_start)
                .bind(snapshot_id)
                .execute(&mut *tx)
                .await?;

                // R7-S-011: Get data_file_id for column stats
                let id_row = sqlx::query("SELECT LAST_INSERT_ID()")
                    .fetch_one(&mut *tx)
                    .await?;
                let data_file_id: i64 = id_row.try_get(0)?;

                let new_next_row_id = row_id_start
                    .checked_add(file.record_count)
                    .ok_or_else(|| DuckLakeError::Internal("row_id overflow".into()))?;
                let updated = sqlx::query(
                    "UPDATE ducklake_table_stats
                     SET record_count = COALESCE(record_count, 0) + ?,
                         next_row_id = ?,
                         file_size_bytes = COALESCE(file_size_bytes, 0) + ?
                     WHERE table_id = ?",
                )
                .bind(file.record_count)
                .bind(new_next_row_id)
                .bind(file.file_size_bytes)
                .bind(table_id)
                .execute(&mut *tx)
                .await?;

                if updated.rows_affected() == 0 {
                    sqlx::query(
                        "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
                         VALUES (?, ?, ?, ?)",
                    )
                    .bind(table_id)
                    .bind(file.record_count)
                    .bind(new_next_row_id)
                    .bind(file.file_size_bytes)
                    .execute(&mut *tx)
                    .await?;
                }

                // R7-S-011: Register per-file column stats (parity with SQLite)
                if !file.column_stats.is_empty() {
                    has_column_stats = true;
                    for stat in &file.column_stats {
                        sqlx::query(
                            "INSERT INTO ducklake_file_column_stats
                             (data_file_id, table_id, column_id, null_count, min_value, max_value)
                             VALUES (?, ?, ?, ?, ?, ?)",
                        )
                        .bind(data_file_id)
                        .bind(table_id)
                        .bind(stat.column_id)
                        .bind(stat.null_count)
                        .bind(&stat.min_value)
                        .bind(&stat.max_value)
                        .execute(&mut *tx)
                        .await?;
                    }
                }
            }

            // R7-S-011: Recompute table-level column stats
            if has_column_stats {
                Self::recompute_table_column_stats(&mut tx, table_id).await?;
            }

            // R4-S-004: Update snapshot's next_file_id
            sqlx::query(
                "UPDATE ducklake_snapshot
                 SET next_file_id = COALESCE(GREATEST(
                     (SELECT COALESCE(MAX(data_file_id), 0) + 1 FROM ducklake_data_file),
                     (SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM ducklake_delete_file)
                 ), 0)
                 WHERE snapshot_id = ?",
            )
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(())
        })
    }

    fn get_data_path(&self) -> Result<String> {
        block_on(async {
            let row = sqlx::query(
                "SELECT value FROM ducklake_metadata WHERE `key` = ? AND scope IS NULL",
            )
            .bind("data_path")
            .fetch_optional(&self.pool)
            .await?;

            match row {
                Some(r) => Ok(r.try_get(0)?),
                None => Err(crate::error::DuckLakeError::InvalidConfig(
                    "Missing required catalog metadata: 'data_path' not configured.".to_string(),
                )),
            }
        })
    }

    fn set_data_path(&self, path: &str) -> Result<()> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            sqlx::query(
                "DELETE FROM ducklake_metadata WHERE `key` = 'data_path' AND scope IS NULL",
            )
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "INSERT INTO ducklake_metadata (`key`, value, scope)
                 VALUES ('data_path', ?, NULL)",
            )
            .bind(path)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(())
        })
    }

    fn initialize_schema(&self) -> Result<()> {
        block_on(async {
            // Note: MySQL DDL (CREATE TABLE) causes implicit commits, so these
            // cannot be wrapped in a single transaction. If initialization fails
            // partway through, re-running is safe due to IF NOT EXISTS clauses (R5-S-072).
            let ddl_statements = [
                SQL_CREATE_METADATA,
                SQL_CREATE_SNAPSHOT,
                SQL_CREATE_SCHEMA,
                SQL_CREATE_TABLE,
                SQL_CREATE_COLUMN,
                SQL_CREATE_DATA_FILE,
                SQL_CREATE_DELETE_FILE,
                SQL_CREATE_SNAPSHOT_CHANGES,
                SQL_CREATE_CHANGE_TRACKING,
                SQL_CREATE_SEQUENCES,
                SQL_CREATE_FILE_COLUMN_STATS,
                SQL_CREATE_VIEW,
                SQL_CREATE_TAG,
                SQL_CREATE_COLUMN_TAG,
                SQL_CREATE_TABLE_STATS,
                SQL_CREATE_TABLE_COLUMN_STATS,
                SQL_CREATE_PARTITION_INFO,
                SQL_CREATE_PARTITION_COLUMN,
                SQL_CREATE_FILE_PARTITION_VALUE,
                SQL_CREATE_FILES_SCHEDULED_FOR_DELETION,
                SQL_CREATE_INLINED_DATA_TABLES,
                SQL_CREATE_COLUMN_MAPPING,
                SQL_CREATE_NAME_MAPPING,
                SQL_CREATE_SCHEMA_VERSIONS,
                SQL_CREATE_MACRO,
                SQL_CREATE_MACRO_IMPL,
                SQL_CREATE_MACRO_PARAMETERS,
                SQL_CREATE_SORT_INFO,
                SQL_CREATE_SORT_EXPRESSION,
                SQL_CREATE_FILE_VARIANT_STATS,
            ];

            for ddl in ddl_statements {
                sqlx::query(ddl).execute(&self.pool).await?;
            }

            // Insert DuckLake version metadata if not already present.
            // DuckLake uses this for migration checks; v0.3 is compatible with DuckDB v1.4.x.
            sqlx::query(
                "INSERT INTO ducklake_metadata (`key`, value)
                 SELECT 'version', '0.3' FROM DUAL
                 WHERE NOT EXISTS (SELECT 1 FROM ducklake_metadata WHERE `key` = 'version' AND scope IS NULL)",
            )
            .execute(&self.pool)
            .await?;
            sqlx::query(
                "INSERT INTO ducklake_metadata (`key`, value)
                 SELECT 'created_by', 'DataFusion-DuckLake' FROM DUAL
                 WHERE NOT EXISTS (SELECT 1 FROM ducklake_metadata WHERE `key` = 'created_by' AND scope IS NULL)",
            )
            .execute(&self.pool)
            .await?;

            // DuckDB sets `encrypted=false` in metadata; match for interop (F-047)
            sqlx::query(
                "INSERT INTO ducklake_metadata (`key`, value)
                 SELECT 'encrypted', 'false' FROM DUAL
                 WHERE NOT EXISTS (SELECT 1 FROM ducklake_metadata WHERE `key` = 'encrypted' AND scope IS NULL)",
            )
            .execute(&self.pool)
            .await?;

            // Insert initial snapshot 0 (DuckDB expects this as the "empty catalog" snapshot).
            // MySQL treats INSERT of 0 into AUTO_INCREMENT as a new auto-value unless
            // NO_AUTO_VALUE_ON_ZERO is set, so we temporarily enable that mode.
            // We must use a single connection for session variable save/restore,
            // since pool connections don't share user variables.
            // IMPORTANT: Always restore sql_mode even if the INSERT fails, to avoid
            // leaking modified sql_mode on a pooled connection.
            let mut conn = self.pool.acquire().await?;
            sqlx::query("SET @old_sql_mode = @@SESSION.sql_mode")
                .execute(&mut *conn)
                .await?;
            sqlx::query(
                "SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',NO_AUTO_VALUE_ON_ZERO')",
            )
            .execute(&mut *conn)
            .await?;
            let insert_result = sqlx::query(
                "INSERT IGNORE INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
                 VALUES (0, NOW(6), 0, 0, 0)",
            )
            .execute(&mut *conn)
            .await;
            // Always restore sql_mode before checking the insert result
            sqlx::query("SET SESSION sql_mode = @old_sql_mode")
                .execute(&mut *conn)
                .await?;
            insert_result?;

            // Insert initial schema_version entry (F-012)
            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version)
                 SELECT 0, 0 FROM DUAL
                 WHERE NOT EXISTS (SELECT 1 FROM ducklake_schema_versions WHERE begin_snapshot = 0)",
            )
            .execute(&self.pool)
            .await?;

            // Sync sequences with existing data (handles migration from MAX+1 pattern, R5-S-027).
            // Uses INSERT ... ON DUPLICATE KEY UPDATE to atomically set each sequence
            // to the max existing value if it's higher than the current sequence value.
            for (seq_name, table_name, col_name) in [
                ("table_id", "ducklake_table", "table_id"),
                ("column_id", "ducklake_column", "column_id"),
                ("view_id", "ducklake_view", "view_id"),
                ("partition_id", "ducklake_partition_info", "partition_id"),
                // R8-S-005: Sync schema_version sequence for concurrent DDL safety
                ("schema_version", "ducklake_snapshot", "schema_version"),
            ] {
                let sync_sql = format!(
                    "INSERT INTO _df_sequences (seq_name, seq_value)
                     SELECT ?, COALESCE(MAX({}), 0) FROM {}
                     ON DUPLICATE KEY UPDATE seq_value = GREATEST(seq_value, VALUES(seq_value))",
                    col_name, table_name
                );
                sqlx::query(&sync_sql)
                    .bind(seq_name)
                    .execute(&self.pool)
                    .await?;
            }

            Ok(())
        })
    }

    fn register_delete_file(
        &self,
        table_id: i64,
        snapshot_id: i64,
        file: &DeleteFileInfo,
    ) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // End any existing active delete file for this data file
            sqlx::query(
                "UPDATE ducklake_delete_file SET end_snapshot = ?
                 WHERE data_file_id = ? AND table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(file.data_file_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Insert the new delete file
            sqlx::query(
                "INSERT INTO ducklake_delete_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, delete_count, format, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, ?, ?, 'parquet', ?)",
            )
            .bind(file.data_file_id)
            .bind(table_id)
            .bind(&file.path)
            .bind(file.path_is_relative)
            .bind(file.file_size_bytes)
            .bind(file.footer_size)
            .bind(file.delete_count)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;
            let id = last_insert_id(&mut tx).await?;

            tx.commit().await?;
            Ok(id)
        })
    }

    fn drop_table(&self, table_id: i64) -> Result<i64> {
        block_on(async {
            let tx = self.pool.begin().await?;
            Self::drop_table_inner(tx, table_id).await
        })
    }

    fn drop_schema(&self, schema_id: i64) -> Result<i64> {
        block_on(async {
            let tx = self.pool.begin().await?;
            Self::drop_schema_inner(tx, schema_id).await
        })
    }

    fn list_active_table_ids(&self, schema_id: i64) -> Result<Vec<i64>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT table_id FROM ducklake_table
                 WHERE schema_id = ? AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .fetch_all(&self.pool)
            .await?;

            let mut ids = Vec::with_capacity(rows.len());
            for row in rows {
                ids.push(row.try_get(0)?);
            }
            Ok(ids)
        })
    }

    fn begin_checked_write_transaction(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
        since_snapshot: i64,
    ) -> Result<WriteSetupResult> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // R8-S-030: Use FOR UPDATE to lock schema/table rows during conflict check.
            // This prevents a concurrent DROP from committing between our check and the
            // actual write in write_transaction_inner.
            let schema_row = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = ?
                 ORDER BY schema_id DESC LIMIT 1
                 FOR UPDATE",
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
                     ORDER BY table_id DESC LIMIT 1
                     FOR UPDATE",
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

    fn drop_table_checked(&self, table_id: i64, since_snapshot: i64) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Check DF-originated drops
            let drop_check = sqlx::query(
                "SELECT COUNT(*) FROM _df_change_tracking
                 WHERE snapshot_id > ? AND table_id = ? AND change_type = 'DROP_TABLE'",
            )
            .bind(since_snapshot)
            .bind(table_id)
            .fetch_one(&mut *tx)
            .await?;
            if drop_check.try_get::<i64, _>(0)? > 0 {
                return Err(crate::error::DuckLakeError::TransactionConflict(format!(
                    "Transaction conflict: table (id={}) was already dropped by another transaction since snapshot {}",
                    table_id, since_snapshot
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
                    "Transaction conflict: table (id={}) was already dropped (possibly by DuckDB) since snapshot {}",
                    table_id, since_snapshot
                )));
            }

            Self::drop_table_inner(tx, table_id).await
        })
    }

    fn drop_schema_checked(&self, schema_id: i64, since_snapshot: i64) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Check DF-originated drops
            let drop_check = sqlx::query(
                "SELECT COUNT(*) FROM _df_change_tracking
                 WHERE snapshot_id > ? AND schema_id = ? AND change_type = 'DROP_SCHEMA'",
            )
            .bind(since_snapshot)
            .bind(schema_id)
            .fetch_one(&mut *tx)
            .await?;
            if drop_check.try_get::<i64, _>(0)? > 0 {
                return Err(crate::error::DuckLakeError::TransactionConflict(format!(
                    "Transaction conflict: schema (id={}) was already dropped by another transaction since snapshot {}",
                    schema_id, since_snapshot
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
                    "Transaction conflict: schema (id={}) was already dropped (possibly by DuckDB) since snapshot {}",
                    schema_id, since_snapshot
                )));
            }

            Self::drop_schema_inner(tx, schema_id).await
        })
    }

    fn begin_write_transaction(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
    ) -> Result<WriteSetupResult> {
        block_on(async {
            let tx = self.pool.begin().await?;
            Self::write_transaction_inner(tx, schema_name, table_name, columns, mode).await
        })
    }

    fn create_view(&self, schema_id: i64, view_name: &str, sql: &str) -> Result<(i64, i64)> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Increment schema_version for DDL (F-012)
            let prev_sv_row =
                sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                    .fetch_one(&mut *tx)
                    .await?;
            let new_schema_version: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

            sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
            )
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            let view_id = next_sequence_id(&mut tx, "view_id").await?;

            // F-026: generate UUID for view
            let view_uuid = uuid::Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO ducklake_view (view_id, view_uuid, schema_id, view_name, `sql`, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(view_id)
            .bind(&view_uuid)
            .bind(schema_id)
            .bind(view_name)
            .bind(sql)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;

            // Record changes_made in DuckDB format (F-027)
            let schema_row = sqlx::query(
                "SELECT schema_name FROM ducklake_schema WHERE schema_id = ? AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .fetch_optional(&mut *tx)
            .await?;
            let schema_name = schema_row
                .map(|r| r.try_get::<String, _>(0).unwrap_or_default())
                .unwrap_or_default();

            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!(
                "created_view:\"{}\".\"{}\"",
                schema_name.replace('"', "\"\""),
                view_name.replace('"', "\"\"")
            ))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok((view_id, snapshot_id))
        })
    }

    fn drop_view(&self, view_id: i64) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // R4-S-014: Validate view exists and is active before creating snapshot
            let exists = sqlx::query(
                "SELECT COUNT(*) FROM ducklake_view WHERE view_id = ? AND end_snapshot IS NULL",
            )
            .bind(view_id)
            .fetch_one(&mut *tx)
            .await?;
            if exists.try_get::<i64, _>(0)? == 0 {
                return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                    "View with id {} not found or already dropped",
                    view_id
                )));
            }

            // Increment schema_version for DDL (F-012)
            let prev_sv_row =
                sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                    .fetch_one(&mut *tx)
                    .await?;
            let new_schema_version: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

            sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
            )
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "UPDATE ducklake_view SET end_snapshot = ?
                 WHERE view_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(view_id)
            .execute(&mut *tx)
            .await?;

            // Record changes_made in DuckDB format (F-027)
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!("dropped_view:{}", view_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    fn rename_view(&self, view_id: i64, new_name: &str) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Fetch the current active view row
            let view_row = sqlx::query(
                "SELECT schema_id, view_uuid, `sql`, dialect, column_aliases
                 FROM ducklake_view
                 WHERE view_id = ? AND end_snapshot IS NULL",
            )
            .bind(view_id)
            .fetch_optional(&mut *tx)
            .await?;

            let Some(view_row) = view_row else {
                return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                    "View with id {} not found or already dropped",
                    view_id
                )));
            };

            let schema_id: i64 = view_row.try_get(0)?;
            let view_uuid: Option<String> = view_row.try_get(1)?;
            let sql: String = view_row.try_get(2)?;
            let dialect: Option<String> = view_row.try_get(3)?;
            let column_aliases: Option<String> = view_row.try_get(4)?;

            // R4-S-015: Check for duplicate active view name in same schema
            let dup = sqlx::query(
                "SELECT COUNT(*) FROM ducklake_view
                 WHERE schema_id = ? AND view_name = ? AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .bind(new_name)
            .fetch_one(&mut *tx)
            .await?;
            if dup.try_get::<i64, _>(0)? > 0 {
                return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                    "A view named '{}' already exists in schema {}",
                    new_name, schema_id
                )));
            }

            // Increment schema_version for DDL (F-012)
            let prev_sv_row =
                sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                    .fetch_one(&mut *tx)
                    .await?;
            let new_schema_version: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

            sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
            )
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // End the existing view row
            sqlx::query(
                "UPDATE ducklake_view SET end_snapshot = ?
                 WHERE view_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(view_id)
            .execute(&mut *tx)
            .await?;

            // Insert new view row with updated name (same view_id, same SQL)
            sqlx::query(
                "INSERT INTO ducklake_view (view_id, view_uuid, schema_id, view_name, dialect, `sql`, column_aliases, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(view_id)
            .bind(&view_uuid)
            .bind(schema_id)
            .bind(new_name)
            .bind(&dialect)
            .bind(&sql)
            .bind(&column_aliases)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;

            // Record change for conflict detection
            sqlx::query(
                "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id)
                 VALUES (?, 'ALTER_VIEW', ?)",
            )
            .bind(snapshot_id)
            .bind(view_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes (F-027)
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!("altered_view:{}", view_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    fn alter_table(&self, table_id: i64, op: &AlterTableOp) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Get active columns for validation (including default value fields)
            let col_rows = sqlx::query(
                "SELECT column_id, column_name, column_type, column_order, nulls_allowed,
                        initial_default, default_value, parent_column, default_value_type, default_value_dialect
                 FROM ducklake_column
                 WHERE table_id = ? AND end_snapshot IS NULL
                 ORDER BY column_order",
            )
            .bind(table_id)
            .fetch_all(&mut *tx)
            .await?;

            let columns: Vec<ActiveColumnInfo> = col_rows
                .iter()
                .map(|r| {
                    Ok(ActiveColumnInfo {
                        column_id: r.try_get(0)?,
                        column_name: r.try_get(1)?,
                        column_type: r.try_get(2)?,
                        column_order: r.try_get(3)?,
                        is_nullable: r.try_get::<Option<bool>, _>(4)?.unwrap_or(true),
                        initial_default: r.try_get(5)?,
                        default_value: r.try_get(6)?,
                        parent_column: r.try_get(7)?,
                        default_value_type: r.try_get(8)?,
                        default_value_dialect: r.try_get(9)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            validate_table_has_columns(&columns)?;
            let action = validate_alter_table(&columns, op)?;

            // Increment schema_version for DDL (F-012)
            let prev_sv_row =
                sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                    .fetch_one(&mut *tx)
                    .await?;
            let new_schema_version: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

            sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
            )
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            match action {
                AlterTableAction::InsertColumn {
                    column_name,
                    column_type,
                    column_order,
                    is_nullable,
                } => {
                    let next_column_id = next_sequence_id(&mut tx, "column_id").await?;

                    let (
                        initial_default,
                        default_value,
                        parent_column,
                        default_value_type,
                        default_value_dialect,
                    ) = if let AlterTableOp::AddColumn {
                        column,
                    } = op
                    {
                        (
                            &column.initial_default,
                            &column.default_value,
                            column.parent_column,
                            &column.default_value_type,
                            &column.default_value_dialect,
                        )
                    } else {
                        (&None, &None, None, &None, &None)
                    };
                    sqlx::query(
                        "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, initial_default, default_value, parent_column, default_value_type, default_value_dialect, begin_snapshot)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    )
                    .bind(next_column_id)
                    .bind(table_id)
                    .bind(&column_name)
                    .bind(&column_type)
                    .bind(column_order)
                    .bind(is_nullable)
                    .bind(initial_default)
                    .bind(default_value)
                    .bind(parent_column)
                    .bind(default_value_type)
                    .bind(default_value_dialect)
                    .bind(snapshot_id)
                    .execute(&mut *tx)
                    .await?;

                    // Initialize table-level column stats for the new column (upstream bug #625)
                    // R5-S-003: contains_null must be 1 because existing rows have NULL for the new column.
                    // NULL here causes DuckDB to crash when reading from the catalog.
                    sqlx::query(
                        "INSERT INTO ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan)
                         VALUES (?, ?, 1, NULL)",
                    )
                    .bind(table_id)
                    .bind(next_column_id)
                    .execute(&mut *tx)
                    .await?;
                },
                AlterTableAction::EndColumn {
                    column_id,
                } => {
                    sqlx::query(
                        "UPDATE ducklake_column SET end_snapshot = ?
                         WHERE column_id = ? AND end_snapshot IS NULL",
                    )
                    .bind(snapshot_id)
                    .bind(column_id)
                    .execute(&mut *tx)
                    .await?;
                },
                AlterTableAction::ReplaceColumn {
                    end_column_id,
                    column_name,
                    column_type,
                    column_order,
                    is_nullable,
                    initial_default,
                    default_value,
                    parent_column,
                    default_value_type,
                    default_value_dialect,
                } => {
                    // End the existing column row
                    sqlx::query(
                        "UPDATE ducklake_column SET end_snapshot = ?
                         WHERE column_id = ? AND end_snapshot IS NULL",
                    )
                    .bind(snapshot_id)
                    .bind(end_column_id)
                    .execute(&mut *tx)
                    .await?;

                    // Reuse the same column_id for the replacement row
                    // (critical for Parquet field_id mapping)
                    sqlx::query(
                        "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, initial_default, default_value, parent_column, default_value_type, default_value_dialect, begin_snapshot)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    )
                    .bind(end_column_id)
                    .bind(table_id)
                    .bind(&column_name)
                    .bind(&column_type)
                    .bind(column_order)
                    .bind(is_nullable)
                    .bind(&initial_default)
                    .bind(&default_value)
                    .bind(parent_column)
                    .bind(&default_value_type)
                    .bind(&default_value_dialect)
                    .bind(snapshot_id)
                    .execute(&mut *tx)
                    .await?;
                },
                AlterTableAction::SetPartitionedBy {
                    partition_columns,
                } => {
                    // End any existing partition info
                    sqlx::query(
                        "UPDATE ducklake_partition_info SET end_snapshot = ?
                         WHERE table_id = ? AND end_snapshot IS NULL",
                    )
                    .bind(snapshot_id)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                    // Create new partition_info entry
                    let partition_id = next_sequence_id(&mut tx, "partition_id").await?;

                    sqlx::query(
                        "INSERT INTO ducklake_partition_info (partition_id, table_id, begin_snapshot)
                         VALUES (?, ?, ?)",
                    )
                    .bind(partition_id)
                    .bind(table_id)
                    .bind(snapshot_id)
                    .execute(&mut *tx)
                    .await?;

                    // Create partition_column entries
                    for (key_index, (column_id, _column_name, transform)) in
                        partition_columns.iter().enumerate()
                    {
                        sqlx::query(
                            "INSERT INTO ducklake_partition_column (partition_id, table_id, partition_key_index, column_id, transform)
                             VALUES (?, ?, ?, ?, ?)",
                        )
                        .bind(partition_id)
                        .bind(table_id)
                        .bind(key_index as i64)
                        .bind(column_id)
                        .bind(transform.as_deref().unwrap_or("identity"))
                        .execute(&mut *tx)
                        .await?;
                    }
                },
            }

            // Record change for conflict detection
            sqlx::query(
                "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id)
                 VALUES (?, 'ALTER_TABLE', ?)",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!("altered_table:{}", table_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    fn get_active_columns(&self, table_id: i64) -> Result<Vec<(String, String, bool)>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT column_name, column_type, nulls_allowed
                 FROM ducklake_column
                 WHERE table_id = ? AND end_snapshot IS NULL
                 ORDER BY column_order",
            )
            .bind(table_id)
            .fetch_all(&self.pool)
            .await?;

            let mut columns = Vec::with_capacity(rows.len());
            for row in rows {
                let name: String = row.try_get(0)?;
                let col_type: String = row.try_get(1)?;
                let nullable: bool = row.try_get::<Option<bool>, _>(2)?.unwrap_or(true);
                columns.push((name, col_type, nullable));
            }
            Ok(columns)
        })
    }

    fn rename_table(&self, table_id: i64, new_name: &str) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Fetch the current active table row
            let table_row = sqlx::query(
                "SELECT schema_id, table_uuid, path, path_is_relative
                 FROM ducklake_table
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .fetch_optional(&mut *tx)
            .await?;

            let Some(table_row) = table_row else {
                return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                    "Table with id {} not found or already dropped",
                    table_id
                )));
            };

            let schema_id: i64 = table_row.try_get(0)?;
            let table_uuid: Option<String> = table_row.try_get(1)?;
            let path: String = table_row.try_get(2)?;
            let path_is_relative: bool = table_row.try_get(3)?;

            // R4-S-015: Check for duplicate active table name in same schema
            let dup = sqlx::query(
                "SELECT COUNT(*) FROM ducklake_table
                 WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .bind(new_name)
            .fetch_one(&mut *tx)
            .await?;
            if dup.try_get::<i64, _>(0)? > 0 {
                return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                    "A table named '{}' already exists in schema {}",
                    new_name, schema_id
                )));
            }

            // Increment schema_version for DDL (F-012)
            let prev_sv_row =
                sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                    .fetch_one(&mut *tx)
                    .await?;
            let new_schema_version: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

            sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
            )
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // End the existing table row
            sqlx::query(
                "UPDATE ducklake_table SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Insert new table row with updated name (same table_id, same path)
            sqlx::query(
                "INSERT INTO ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(table_id)
            .bind(&table_uuid)
            .bind(schema_id)
            .bind(new_name)
            .bind(&path)
            .bind(path_is_relative)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;

            // Record change for conflict detection
            sqlx::query(
                "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id)
                 VALUES (?, 'ALTER_TABLE', ?)",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes (F-027)
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!("altered_table:{}", table_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    fn set_table_comment(&self, table_id: i64, comment: &str) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Increment schema_version for DDL (F-012)
            let prev_sv_row =
                sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                    .fetch_one(&mut *tx)
                    .await?;
            let new_schema_version: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

            sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
            )
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // End any existing comment tag for this table
            sqlx::query(
                "UPDATE ducklake_tag SET end_snapshot = ?
                 WHERE object_id = ? AND `key` = 'comment' AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Insert new comment tag
            sqlx::query(
                "INSERT INTO ducklake_tag (object_id, begin_snapshot, `key`, value)
                 VALUES (?, ?, 'comment', ?)",
            )
            .bind(table_id)
            .bind(snapshot_id)
            .bind(comment)
            .execute(&mut *tx)
            .await?;

            // Record change for conflict detection
            sqlx::query(
                "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id)
                 VALUES (?, 'ALTER_TABLE', ?)",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!("altered_table:{}", table_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    fn set_column_comment(&self, table_id: i64, column_name: &str, comment: &str) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Look up the column_id for the named column
            let col_row = sqlx::query(
                "SELECT column_id FROM ducklake_column
                 WHERE table_id = ? AND column_name = ? AND end_snapshot IS NULL",
            )
            .bind(table_id)
            .bind(column_name)
            .fetch_optional(&mut *tx)
            .await?;

            let Some(col_row) = col_row else {
                return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                    "Column '{}' not found in table",
                    column_name
                )));
            };
            let column_id: i64 = col_row.try_get(0)?;

            // Increment schema_version for DDL (F-012)
            let prev_sv_row =
                sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                    .fetch_one(&mut *tx)
                    .await?;
            let new_schema_version: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

            sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (NOW(6), ?)",
            )
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES (?, ?)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // End any existing comment tag for this column
            sqlx::query(
                "UPDATE ducklake_column_tag SET end_snapshot = ?
                 WHERE table_id = ? AND column_id = ? AND `key` = 'comment' AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .bind(column_id)
            .execute(&mut *tx)
            .await?;

            // Insert new comment tag
            sqlx::query(
                "INSERT INTO ducklake_column_tag (table_id, column_id, begin_snapshot, `key`, value)
                 VALUES (?, ?, ?, 'comment', ?)",
            )
            .bind(table_id)
            .bind(column_id)
            .bind(snapshot_id)
            .bind(comment)
            .execute(&mut *tx)
            .await?;

            // Record change for conflict detection
            sqlx::query(
                "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id)
                 VALUES (?, 'ALTER_TABLE', ?)",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!("altered_table:{}", table_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }
}
