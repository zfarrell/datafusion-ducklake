//! MySQL implementation of [`MetadataWriter`].
//!
//! Requires multi-threaded Tokio runtime (`#[tokio::test(flavor = "multi_thread")]`).

use crate::Result;
use crate::metadata_provider::block_on;
use crate::metadata_writer::{
    AlterTableOp, ColumnDef, MetadataWriter, WriteMode, WriteSetupResult,
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
    schema_version BIGINT
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
            next_sequence_id(&mut tx, "schema_version").await?
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
            // R8-S-043: Include created_schema if schema was also new (parity with SQLite)
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
                 ON DUPLICATE KEY UPDATE changes_made = VALUES(changes_made)",
            )
            .bind(snapshot_id)
            .bind(changes)
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

    crate::metadata_writer_impl::impl_writer_drop_inner!(
        sqlx::Transaction<'_, sqlx::MySql>,
        dialect = crate::dialect::MySqlDialect,
        last_insert_id = last_insert_id
    );

    crate::metadata_writer_impl::impl_recompute_table_column_stats!(
        sqlx::Transaction<'_, sqlx::MySql>,
        crate::dialect::MySqlDialect
    );

    async fn next_entity_id(
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        entity: &str,
        _table_id: Option<i64>,
    ) -> crate::Result<i64> {
        next_sequence_id(tx, entity).await
    }
}

impl MetadataWriter for MySqlMetadataWriter {
    // R8-S-042: Include schema_version and next_file_id (parity with SQLite)
    fn create_snapshot(&self) -> Result<i64> {
        block_on(async {
            let mut conn = self.pool.acquire().await?;
            sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version, next_file_id)
                 VALUES (
                     NOW(6),
                     COALESCE((SELECT MAX(schema_version) FROM ducklake_snapshot), 1),
                     COALESCE(GREATEST(
                         (SELECT COALESCE(MAX(data_file_id), 0) + 1 FROM ducklake_data_file),
                         (SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM ducklake_delete_file)
                     ), 0)
                 )",
            )
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

    // --- file_ops and query_ops generated by macros ---
    crate::metadata_writer_impl::impl_writer_file_ops!(
        MySqlMetadataWriter,
        pool_type = MySqlPool,
        dialect = crate::dialect::MySqlDialect,
        block_on = crate::metadata_provider::block_on_once,
        last_insert_id = last_insert_id
    );

    crate::metadata_writer_impl::impl_writer_query_ops!(
        MySqlMetadataWriter,
        pool_type = MySqlPool,
        dialect = crate::dialect::MySqlDialect,
        block_on = crate::metadata_provider::block_on_once
    );

    crate::metadata_writer_impl::impl_writer_ddl_ops!(
        MySqlMetadataWriter,
        pool_type = MySqlPool,
        dialect = crate::dialect::MySqlDialect,
        block_on = crate::metadata_provider::block_on_once,
        last_insert_id = last_insert_id,
        column_order_type = i64
    );

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

    crate::metadata_writer_impl::impl_writer_drop_ops!(
        MySqlMetadataWriter,
        pool_type = MySqlPool,
        dialect = crate::dialect::MySqlDialect,
        block_on = crate::metadata_provider::block_on_once
    );

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

    // R8-S-008: Implement register_file_partition_value (parity with SQLite)
}

#[cfg(test)]
#[cfg(all(feature = "metadata-mysql", not(feature = "skip-tests-with-docker")))]
mod tests {
    use super::*;
    use crate::metadata_writer::DataFileInfo;

    /// R9-S-002: Verify MySQL LAST_INSERT_ID path returns correct sequential IDs.
    #[tokio::test]
    async fn test_mysql_register_data_file_sequential_ids() {
        // Connect to Docker MySQL (standard test credentials)
        let conn_str = std::env::var("MYSQL_CONNECTION_STRING")
            .unwrap_or_else(|_| "mysql://root:ducklake@localhost:3306/ducklake_test".to_string());

        let writer = match MySqlMetadataWriter::new_with_init(&conn_str).await {
            Ok(w) => w,
            Err(e) => {
                eprintln!("Skipping MySQL test (connection failed): {e}");
                return;
            },
        };

        let snap = writer.create_snapshot().unwrap();
        let (schema_id, _) = writer
            .get_or_create_schema("test_schema", None, snap)
            .unwrap();
        let (table_id, _) = writer
            .get_or_create_table(schema_id, "test_seq_ids", None, snap)
            .unwrap();

        // Register two data files and verify IDs are sequential
        let file1 = DataFileInfo::new("file1.parquet", 1000, 100);
        let id1 = writer.register_data_file(table_id, snap, &file1).unwrap();

        let file2 = DataFileInfo::new("file2.parquet", 2000, 200);
        let id2 = writer.register_data_file(table_id, snap, &file2).unwrap();

        assert!(id1 > 0, "First file ID should be positive");
        assert_eq!(id2, id1 + 1, "Second file ID should be sequential");
    }
}
