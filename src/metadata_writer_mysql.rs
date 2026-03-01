//! MySQL implementation of [`MetadataWriter`].
//!
//! Requires multi-threaded Tokio runtime (`#[tokio::test(flavor = "multi_thread")]`).

use crate::Result;
use crate::metadata_provider::block_on;
use crate::metadata_writer::{
    AlterTableOp, ColumnDef, ColumnStatInfo, DataFileInfo, DeleteFileInfo, MetadataWriter,
    WriteMode, WriteSetupResult,
};
use crate::metadata_writer_validation::{
    ActiveColumnInfo, AlterTableAction, validate_alter_table, validate_no_duplicate_columns,
    validate_schema_evolution, validate_table_has_columns,
};
use sqlx::Row;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

const DEFAULT_MAX_CONNECTIONS: u32 = 5;

const SQL_CREATE_METADATA: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_metadata (
    `key` VARCHAR(255) NOT NULL,
    value VARCHAR(1024) NOT NULL,
    scope VARCHAR(255),
    scope_id BIGINT
)"#;

const SQL_CREATE_SNAPSHOT: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_snapshot (
    snapshot_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_time TIMESTAMP(6) DEFAULT NOW(6),
    schema_version INTEGER DEFAULT 1,
    next_catalog_id BIGINT DEFAULT 0,
    next_file_id BIGINT DEFAULT 0
)"#;

const SQL_CREATE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_schema (
    schema_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    schema_uuid VARCHAR(255),
    schema_name VARCHAR(255) NOT NULL,
    path VARCHAR(1024) NOT NULL DEFAULT '',
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
    path VARCHAR(1024) NOT NULL DEFAULT '',
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
    initial_default VARCHAR(1024),
    default_value VARCHAR(1024),
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
    path VARCHAR(1024) NOT NULL,
    path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
    file_size_bytes BIGINT NOT NULL,
    footer_size BIGINT,
    encryption_key VARCHAR(255),
    record_count BIGINT,
    row_id_start BIGINT,
    mapping_id BIGINT,
    file_order INTEGER,
    file_format VARCHAR(255) DEFAULT 'PARQUET',
    partition_id BIGINT,
    partial_max BIGINT,
    partial_file_info VARCHAR(1024),
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_DELETE_FILE: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_delete_file (
    delete_file_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    data_file_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    path VARCHAR(1024) NOT NULL,
    path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
    file_size_bytes BIGINT NOT NULL,
    footer_size BIGINT,
    encryption_key VARCHAR(255),
    delete_count BIGINT,
    format VARCHAR(255) DEFAULT 'POSITION_DELETES',
    partial_max BIGINT,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
)"#;

const SQL_CREATE_SNAPSHOT_CHANGES: &str = r#"
CREATE TABLE IF NOT EXISTS ducklake_snapshot_changes (
    snapshot_id BIGINT PRIMARY KEY,
    changes_made VARCHAR(1024),
    author VARCHAR(255),
    commit_message VARCHAR(1024),
    commit_extra_info VARCHAR(1024)
)"#;

const SQL_CREATE_CHANGE_TRACKING: &str = r#"
CREATE TABLE IF NOT EXISTS _df_change_tracking (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_id BIGINT NOT NULL,
    change_type VARCHAR(255) NOT NULL,
    table_id BIGINT,
    schema_id BIGINT
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
    table_id BIGINT,
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
    path VARCHAR(1024),
    path_is_relative BOOLEAN,
    schedule_start TIMESTAMP(6)
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
        // Create snapshot
        sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
            .execute(&mut *tx)
            .await?;
        let snapshot_id = last_insert_id(&mut tx).await?;

        // Get or create schema
        let schema_id: i64 = {
            let existing = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = ? AND end_snapshot IS NULL",
            )
            .bind(schema_name)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = existing {
                row.try_get(0)?
            } else {
                let schema_path = format!("{}/", schema_name);
                sqlx::query(
                    "INSERT INTO ducklake_schema (schema_name, path, path_is_relative, begin_snapshot)
                     VALUES (?, ?, TRUE, ?)",
                )
                .bind(schema_name)
                .bind(&schema_path)
                .bind(snapshot_id)
                .execute(&mut *tx)
                .await?;
                last_insert_id(&mut tx).await?
            }
        };

        // Get or create table
        let table_id: i64 = {
            let existing = sqlx::query(
                "SELECT table_id FROM ducklake_table
                 WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .bind(table_name)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = existing {
                row.try_get(0)?
            } else {
                let next_tid_row = sqlx::query(
                    "SELECT COALESCE(MAX(table_id), 0) + 1 FROM ducklake_table FOR UPDATE",
                )
                .fetch_one(&mut *tx)
                .await?;
                let next_table_id: i64 = next_tid_row.try_get(0)?;

                let table_path = format!("{}/", table_name);
                sqlx::query(
                    "INSERT INTO ducklake_table (table_id, schema_id, table_name, path, path_is_relative, begin_snapshot)
                     VALUES (?, ?, ?, ?, TRUE, ?)",
                )
                .bind(next_table_id)
                .bind(schema_id)
                .bind(table_name)
                .bind(&table_path)
                .bind(snapshot_id)
                .execute(&mut *tx)
                .await?;
                next_table_id
            }
        };

        // Get existing columns to check schema compatibility for appends
        let rows = sqlx::query(
            "SELECT column_name, column_type, nulls_allowed
             FROM ducklake_column
             WHERE table_id = ? AND end_snapshot IS NULL
             ORDER BY column_order",
        )
        .bind(table_id)
        .fetch_all(&mut *tx)
        .await?;

        let mut existing_columns: Vec<(String, String, bool)> = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.try_get(0)?;
            let col_type: String = row.try_get(1)?;
            let nullable: bool = row.try_get::<Option<bool>, _>(2)?.unwrap_or(true);
            existing_columns.push((name, col_type, nullable));
        }

        validate_no_duplicate_columns(columns)?;
        validate_schema_evolution(&existing_columns, columns, mode)?;

        // End existing columns
        sqlx::query(
            "UPDATE ducklake_column SET end_snapshot = ?
             WHERE table_id = ? AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        // Compute next column_id explicitly
        let next_cid_row = sqlx::query(
            "SELECT COALESCE(MAX(column_id), 0) + 1 FROM ducklake_column WHERE table_id = ? FOR UPDATE",
        )
        .bind(table_id)
        .fetch_one(&mut *tx)
        .await?;
        let next_column_id: i64 = next_cid_row.try_get(0)?;

        // Insert new columns
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

        // For Replace mode, end existing data files
        if mode == WriteMode::Replace {
            sqlx::query(
                "UPDATE ducklake_data_file SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;
        }

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
        // Create a new snapshot for the drop
        sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
            .execute(&mut *tx)
            .await?;
        let snapshot_id = last_insert_id(&mut tx).await?;

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
        .bind(format!("Dropped table (id={})", table_id))
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
        // Create a new snapshot for the drop
        sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
            .execute(&mut *tx)
            .await?;
        let snapshot_id = last_insert_id(&mut tx).await?;

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
        .bind(format!("Dropped schema (id={})", schema_id))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(snapshot_id)
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
            let mut conn = self.pool.acquire().await?;

            let existing = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = ? AND end_snapshot IS NULL",
            )
            .bind(name)
            .fetch_optional(&mut *conn)
            .await?;

            if let Some(row) = existing {
                return Ok((row.try_get(0)?, false));
            }

            let base_path = path.unwrap_or(name);
            let schema_path = if base_path.ends_with('/') {
                base_path.to_string()
            } else {
                format!("{}/", base_path)
            };
            sqlx::query(
                "INSERT INTO ducklake_schema (schema_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, TRUE, ?)",
            )
            .bind(name)
            .bind(&schema_path)
            .bind(snapshot_id)
            .execute(&mut *conn)
            .await?;
            let id = last_insert_id_conn(&mut conn).await?;

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
                sqlx::query("SELECT COALESCE(MAX(table_id), 0) + 1 FROM ducklake_table FOR UPDATE")
                    .fetch_one(&mut *tx)
                    .await?;
            let next_table_id: i64 = next_tid_row.try_get(0)?;

            sqlx::query(
                "INSERT INTO ducklake_table (table_id, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES (?, ?, ?, ?, TRUE, ?)",
            )
            .bind(next_table_id)
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

            let next_cid_row = sqlx::query(
                "SELECT COALESCE(MAX(column_id), 0) + 1 FROM ducklake_column WHERE table_id = ? FOR UPDATE",
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
                .execute(&self.pool)
                .await?;
            }
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
            let mut conn = self.pool.acquire().await?;
            sqlx::query(
                "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(table_id)
            .bind(&file.path)
            .bind(file.path_is_relative)
            .bind(file.file_size_bytes)
            .bind(file.footer_size)
            .bind(file.record_count)
            .bind(snapshot_id)
            .execute(&mut *conn)
            .await?;
            last_insert_id_conn(&mut conn).await
        })
    }

    fn end_table_files(&self, table_id: i64, snapshot_id: i64) -> Result<u64> {
        block_on(async {
            let result = sqlx::query(
                "UPDATE ducklake_data_file SET end_snapshot = ?
                 WHERE table_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&self.pool)
            .await?;

            Ok(result.rows_affected())
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
            sqlx::query(
                "DELETE FROM ducklake_metadata WHERE `key` = 'data_path' AND scope IS NULL",
            )
            .execute(&self.pool)
            .await?;

            sqlx::query(
                "INSERT INTO ducklake_metadata (`key`, value, scope)
                 VALUES ('data_path', ?, NULL)",
            )
            .bind(path)
            .execute(&self.pool)
            .await?;

            Ok(())
        })
    }

    fn initialize_schema(&self) -> Result<()> {
        block_on(async {
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

            // Insert initial snapshot 0 (DuckDB expects this as the "empty catalog" snapshot).
            // MySQL treats INSERT of 0 into AUTO_INCREMENT as a new auto-value unless
            // NO_AUTO_VALUE_ON_ZERO is set, so we temporarily enable that mode.
            // We must use a single connection for session variable save/restore,
            // since pool connections don't share user variables.
            let mut conn = self.pool.acquire().await?;
            sqlx::query("SET @old_sql_mode = @@SESSION.sql_mode")
                .execute(&mut *conn)
                .await?;
            sqlx::query(
                "SET SESSION sql_mode = CONCAT(@@SESSION.sql_mode, ',NO_AUTO_VALUE_ON_ZERO')",
            )
            .execute(&mut *conn)
            .await?;
            sqlx::query(
                "INSERT IGNORE INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
                 VALUES (0, NOW(6), 0, 0, 0)",
            )
            .execute(&mut *conn)
            .await?;
            sqlx::query("SET SESSION sql_mode = @old_sql_mode")
                .execute(&mut *conn)
                .await?;

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
                "INSERT INTO ducklake_delete_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, delete_count, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
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

            // Conflict check: look for DROP operations since our snapshot.
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
                }
            }

            // No conflict — proceed with write in the same transaction.
            Self::write_transaction_inner(tx, schema_name, table_name, columns, mode).await
        })
    }

    fn drop_table_checked(&self, table_id: i64, since_snapshot: i64) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

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

            Self::drop_table_inner(tx, table_id).await
        })
    }

    fn drop_schema_checked(&self, schema_id: i64, since_snapshot: i64) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

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

            sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
                .execute(&mut *tx)
                .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            let vid_row =
                sqlx::query("SELECT COALESCE(MAX(view_id), 0) + 1 FROM ducklake_view FOR UPDATE")
                    .fetch_one(&mut *tx)
                    .await?;
            let view_id: i64 = vid_row.try_get(0)?;

            sqlx::query(
                "INSERT INTO ducklake_view (view_id, schema_id, view_name, `sql`, begin_snapshot)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(view_id)
            .bind(schema_id)
            .bind(view_name)
            .bind(sql)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok((view_id, snapshot_id))
        })
    }

    fn drop_view(&self, view_id: i64) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
                .execute(&mut *tx)
                .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            sqlx::query(
                "UPDATE ducklake_view SET end_snapshot = ?
                 WHERE view_id = ? AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(view_id)
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

            // Create a new snapshot
            sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
                .execute(&mut *tx)
                .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

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

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!("Renamed view (id={})", view_id))
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

            // Create a new snapshot
            sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
                .execute(&mut *tx)
                .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

            match action {
                AlterTableAction::InsertColumn {
                    column_name,
                    column_type,
                    column_order,
                    is_nullable,
                } => {
                    // Compute next column_id explicitly
                    let next_cid_row = sqlx::query(
                        "SELECT COALESCE(MAX(column_id), 0) + 1 FROM ducklake_column WHERE table_id = ? FOR UPDATE",
                    )
                    .bind(table_id)
                    .fetch_one(&mut *tx)
                    .await?;
                    let next_column_id: i64 = next_cid_row.try_get(0)?;

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
                    sqlx::query(
                        "INSERT INTO ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan)
                         VALUES (?, ?, NULL, NULL)",
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
            .bind(format!("Altered table (id={})", table_id))
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

            // Create a new snapshot
            sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
                .execute(&mut *tx)
                .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

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

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES (?, ?)
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(format!("Renamed table (id={})", table_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    fn set_table_comment(&self, table_id: i64, comment: &str) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Create a new snapshot
            sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
                .execute(&mut *tx)
                .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

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
            .bind(format!("Altered table (id={})", table_id))
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

            // Create a new snapshot
            sqlx::query("INSERT INTO ducklake_snapshot (snapshot_time) VALUES (NOW(6))")
                .execute(&mut *tx)
                .await?;
            let snapshot_id = last_insert_id(&mut tx).await?;

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
            .bind(format!("Altered table (id={})", table_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }
}
