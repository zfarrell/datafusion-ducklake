//! PostgreSQL implementation of [`MetadataWriter`].
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
    ActiveColumnInfo, AlterTableAction, validate_alter_table, validate_no_duplicate_columns,
    validate_schema_evolution, validate_table_has_columns,
};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const DEFAULT_MAX_CONNECTIONS: u32 = 5;

const SQL_CREATE_TABLES: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS ducklake_metadata (
        key VARCHAR NOT NULL,
        value VARCHAR NOT NULL,
        scope VARCHAR,
        scope_id BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_snapshot (
        snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        snapshot_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        schema_version BIGINT DEFAULT 1,
        next_catalog_id BIGINT DEFAULT 0,
        next_file_id BIGINT DEFAULT 0
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_schema (
        schema_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        schema_uuid UUID,
        schema_name VARCHAR NOT NULL,
        path VARCHAR NOT NULL DEFAULT '',
        path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_table (
        table_id BIGINT NOT NULL,
        table_uuid UUID,
        schema_id BIGINT NOT NULL,
        table_name VARCHAR NOT NULL,
        path VARCHAR NOT NULL DEFAULT '',
        path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_column (
        column_id BIGINT NOT NULL,
        table_id BIGINT NOT NULL,
        column_name VARCHAR NOT NULL,
        column_type VARCHAR NOT NULL,
        column_order INTEGER NOT NULL,
        nulls_allowed BOOLEAN DEFAULT TRUE,
        initial_default VARCHAR,
        default_value VARCHAR,
        parent_column BIGINT,
        default_value_type VARCHAR,
        default_value_dialect VARCHAR,
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_data_file (
        data_file_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        table_id BIGINT NOT NULL,
        path VARCHAR NOT NULL,
        path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
        file_size_bytes BIGINT NOT NULL,
        footer_size BIGINT,
        encryption_key VARCHAR,
        record_count BIGINT,
        row_id_start BIGINT,
        mapping_id BIGINT,
        file_order INTEGER,
        file_format VARCHAR DEFAULT 'parquet',
        partition_id BIGINT,
        partial_file_info VARCHAR,
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_delete_file (
        delete_file_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        data_file_id BIGINT NOT NULL,
        table_id BIGINT NOT NULL,
        path VARCHAR NOT NULL,
        path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
        file_size_bytes BIGINT NOT NULL,
        footer_size BIGINT,
        encryption_key VARCHAR,
        delete_count BIGINT,
        format VARCHAR DEFAULT 'parquet',
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_snapshot_changes (
        snapshot_id BIGINT PRIMARY KEY,
        changes_made VARCHAR,
        author VARCHAR,
        commit_message VARCHAR,
        commit_extra_info VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS _df_change_tracking (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        snapshot_id BIGINT NOT NULL,
        change_type TEXT NOT NULL,
        table_id BIGINT,
        schema_id BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_file_column_stats (
        data_file_id BIGINT NOT NULL,
        table_id BIGINT NOT NULL,
        column_id BIGINT NOT NULL,
        column_size_bytes BIGINT,
        value_count BIGINT,
        null_count BIGINT,
        min_value VARCHAR,
        max_value VARCHAR,
        contains_nan BOOLEAN,
        extra_stats VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_view (
        view_id BIGINT NOT NULL,
        view_uuid UUID,
        schema_id BIGINT NOT NULL,
        view_name VARCHAR NOT NULL,
        dialect VARCHAR,
        sql VARCHAR NOT NULL,
        column_aliases VARCHAR,
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_tag (
        object_id BIGINT,
        begin_snapshot BIGINT,
        end_snapshot BIGINT,
        key VARCHAR,
        value VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_column_tag (
        table_id BIGINT,
        column_id BIGINT,
        begin_snapshot BIGINT,
        end_snapshot BIGINT,
        key VARCHAR,
        value VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_table_stats (
        table_id BIGINT PRIMARY KEY,
        record_count BIGINT,
        next_row_id BIGINT,
        file_size_bytes BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_table_column_stats (
        table_id BIGINT,
        column_id BIGINT,
        contains_null BOOLEAN,
        contains_nan BOOLEAN,
        min_value VARCHAR,
        max_value VARCHAR,
        extra_stats VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_partition_info (
        partition_id BIGINT,
        table_id BIGINT,
        begin_snapshot BIGINT,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_partition_column (
        partition_id BIGINT,
        table_id BIGINT,
        partition_key_index BIGINT,
        column_id BIGINT,
        transform VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_file_partition_value (
        data_file_id BIGINT,
        table_id BIGINT,
        partition_key_index BIGINT,
        partition_value VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_files_scheduled_for_deletion (
        data_file_id BIGINT,
        path VARCHAR,
        path_is_relative BOOLEAN,
        schedule_start TIMESTAMP WITH TIME ZONE
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_inlined_data_tables (
        table_id BIGINT,
        table_name VARCHAR,
        schema_version BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_column_mapping (
        mapping_id BIGINT,
        table_id BIGINT,
        type VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_name_mapping (
        mapping_id BIGINT,
        column_id BIGINT,
        source_name VARCHAR,
        target_field_id BIGINT,
        parent_column BIGINT,
        is_partition BOOLEAN
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_schema_versions (
        begin_snapshot BIGINT,
        schema_version BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_macro (
        schema_id BIGINT,
        macro_id BIGINT,
        macro_name VARCHAR,
        begin_snapshot BIGINT,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_macro_impl (
        macro_id BIGINT,
        impl_id BIGINT,
        dialect VARCHAR,
        sql VARCHAR,
        type VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_macro_parameters (
        macro_id BIGINT,
        impl_id BIGINT,
        column_id BIGINT,
        parameter_name VARCHAR,
        parameter_type VARCHAR,
        default_value VARCHAR,
        default_value_type VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_sort_info (
        sort_id BIGINT,
        table_id BIGINT,
        begin_snapshot BIGINT,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_sort_expression (
        sort_id BIGINT,
        table_id BIGINT,
        sort_key_index BIGINT,
        expression VARCHAR,
        dialect VARCHAR,
        sort_direction VARCHAR,
        null_order VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_file_variant_stats (
        data_file_id BIGINT,
        table_id BIGINT,
        column_id BIGINT,
        variant_path VARCHAR,
        shredded_type VARCHAR,
        column_size_bytes BIGINT,
        value_count BIGINT,
        null_count BIGINT,
        min_value VARCHAR,
        max_value VARCHAR,
        contains_nan BOOLEAN,
        extra_stats VARCHAR
    )",
];

/// PostgreSQL-based metadata writer for DuckLake catalogs.
#[derive(Debug, Clone)]
pub struct PostgresMetadataWriter {
    pool: PgPool,
}

impl PostgresMetadataWriter {
    pub async fn new(connection_string: &str) -> Result<Self> {
        Self::with_max_connections(connection_string, DEFAULT_MAX_CONNECTIONS).await
    }

    pub async fn with_max_connections(
        connection_string: &str,
        max_connections: u32,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
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
        mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
    ) -> Result<WriteSetupResult> {
        // Check if schema exists
        let existing_schema = sqlx::query(
            "SELECT schema_id FROM ducklake_schema
             WHERE schema_name = $1 AND end_snapshot IS NULL",
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
                 WHERE schema_id = $1 AND table_name = $2 AND end_snapshot IS NULL",
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
        // For DDL, use a PG sequence to atomically allocate the next version,
        // preventing concurrent DDL from producing duplicate schema_versions.
        let new_schema_version: i64 = if is_ddl {
            let sv_row = sqlx::query("SELECT nextval('ducklake_schema_version_seq')")
                .fetch_one(&mut *tx)
                .await?;
            sv_row.try_get(0)?
        } else {
            let prev_sv_row =
                sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                    .fetch_one(&mut *tx)
                    .await?;
            prev_sv_row.try_get(0)?
        };

        // Create snapshot with correct schema_version
        let row = sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
        )
        .bind(new_schema_version)
        .fetch_one(&mut *tx)
        .await?;
        let snapshot_id: i64 = row.try_get(0)?;

        // Record schema_version change if DDL (F-012)
        if is_ddl {
            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version)
                 VALUES ($1, $2)",
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
            let row = sqlx::query(
                "INSERT INTO ducklake_schema (schema_uuid, schema_name, path, path_is_relative, begin_snapshot)
                 VALUES (gen_random_uuid(), $1, $2, TRUE, $3) RETURNING schema_id",
            )
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
            let next_tid_row = sqlx::query("SELECT nextval('ducklake_table_id_seq')")
                .fetch_one(&mut *tx)
                .await?;
            let next_table_id: i64 = next_tid_row.try_get(0)?;

            let table_path = format!("{}/", table_name);
            sqlx::query(
                "INSERT INTO ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES ($1, gen_random_uuid(), $2, $3, $4, TRUE, $5)",
            )
            .bind(next_table_id)
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
             WHERE table_id = $1 AND end_snapshot IS NULL
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
                "UPDATE ducklake_column SET end_snapshot = $1
                 WHERE table_id = $2 AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            let mut new_ids = Vec::with_capacity(columns.len());
            for (order, col) in columns.iter().enumerate() {
                let cid_row = sqlx::query("SELECT nextval('ducklake_column_id_seq')")
                    .fetch_one(&mut *tx)
                    .await?;
                let column_id: i64 = cid_row.try_get(0)?;
                sqlx::query(
                    "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, initial_default, default_value, parent_column, default_value_type, default_value_dialect, begin_snapshot)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
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
                 VALUES ($1, $2)
                 ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
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

    /// Inner drop table logic, usable with an existing transaction.
    async fn drop_table_inner(
        mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
        table_id: i64,
    ) -> Result<i64> {
        // R4-S-014: Validate table exists and is active before creating snapshot
        let exists = sqlx::query(
            "SELECT COUNT(*) FROM ducklake_table WHERE table_id = $1 AND end_snapshot IS NULL",
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

        let row = sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
        )
        .bind(new_schema_version)
        .fetch_one(&mut *tx)
        .await?;
        let snapshot_id: i64 = row.try_get(0)?;

        sqlx::query(
            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ($1, $2)",
        )
        .bind(snapshot_id)
        .bind(new_schema_version)
        .execute(&mut *tx)
        .await?;

        // Mark the table as dropped by setting end_snapshot
        sqlx::query(
            "UPDATE ducklake_table SET end_snapshot = $1
             WHERE table_id = $2 AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE ducklake_column SET end_snapshot = $1
             WHERE table_id = $2 AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE ducklake_data_file SET end_snapshot = $1
             WHERE table_id = $2 AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE ducklake_delete_file SET end_snapshot = $1
             WHERE table_id = $2 AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id)
             VALUES ($1, 'DROP_TABLE', $2)",
        )
        .bind(snapshot_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await?;

        // Record in spec-compliant snapshot changes (F-027)
        sqlx::query(
            "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
             VALUES ($1, $2)
             ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
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
        mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
        schema_id: i64,
    ) -> Result<i64> {
        // R4-S-014: Validate schema exists and is active before creating snapshot
        let exists = sqlx::query(
            "SELECT COUNT(*) FROM ducklake_schema WHERE schema_id = $1 AND end_snapshot IS NULL",
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

        let row = sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
        )
        .bind(new_schema_version)
        .fetch_one(&mut *tx)
        .await?;
        let snapshot_id: i64 = row.try_get(0)?;

        sqlx::query(
            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ($1, $2)",
        )
        .bind(snapshot_id)
        .bind(new_schema_version)
        .execute(&mut *tx)
        .await?;

        // Cascade: end columns for all active tables in this schema
        sqlx::query(
            "UPDATE ducklake_column SET end_snapshot = $1
             WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE schema_id = $2 AND end_snapshot IS NULL)
             AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // Cascade: end data files for all active tables in this schema
        sqlx::query(
            "UPDATE ducklake_data_file SET end_snapshot = $1
             WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE schema_id = $2 AND end_snapshot IS NULL)
             AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // Cascade: end delete files for all active tables in this schema
        sqlx::query(
            "UPDATE ducklake_delete_file SET end_snapshot = $1
             WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE schema_id = $2 AND end_snapshot IS NULL)
             AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // End all active tables in this schema
        sqlx::query(
            "UPDATE ducklake_table SET end_snapshot = $1
             WHERE schema_id = $2 AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // Mark the schema as dropped
        sqlx::query(
            "UPDATE ducklake_schema SET end_snapshot = $1
             WHERE schema_id = $2 AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO _df_change_tracking (snapshot_id, change_type, schema_id)
             VALUES ($1, 'DROP_SCHEMA', $2)",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        // Record in spec-compliant snapshot changes (F-027)
        sqlx::query(
            "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
             VALUES ($1, $2)
             ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
        )
        .bind(snapshot_id)
        .bind(format!("dropped_schema:{}", schema_id))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(snapshot_id)
    }

    crate::metadata_writer_impl::impl_recompute_table_column_stats!(
        sqlx::Transaction<'_, sqlx::Postgres>,
        crate::dialect::PostgresDialect
    );
}

impl MetadataWriter for PostgresMetadataWriter {
    // R8-S-042: Include schema_version and next_file_id (parity with SQLite)
    fn create_snapshot(&self) -> Result<i64> {
        block_on(async {
            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version, next_file_id)
                 VALUES (
                     CURRENT_TIMESTAMP,
                     COALESCE((SELECT MAX(schema_version) FROM ducklake_snapshot), 1),
                     COALESCE(GREATEST(
                         (SELECT COALESCE(MAX(data_file_id), 0) + 1 FROM ducklake_data_file),
                         (SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM ducklake_delete_file)
                     ), 0)
                 ) RETURNING snapshot_id",
            )
            .fetch_one(&self.pool)
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
        block_on(async {
            // Use a transaction to prevent TOCTOU race between SELECT and INSERT
            let mut tx = self.pool.begin().await?;

            let existing = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = $1 AND end_snapshot IS NULL",
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
            // R8-S-006: Use ON CONFLICT DO NOTHING to handle concurrent INSERT race.
            // The partial unique index ducklake_schema_active_name prevents duplicates.
            let result = sqlx::query(
                "INSERT INTO ducklake_schema (schema_uuid, schema_name, path, path_is_relative, begin_snapshot)
                 VALUES (gen_random_uuid(), $1, $2, TRUE, $3)
                 ON CONFLICT (schema_name) WHERE end_snapshot IS NULL DO NOTHING
                 RETURNING schema_id",
            )
            .bind(name)
            .bind(&schema_path)
            .bind(snapshot_id)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = result {
                tx.commit().await?;
                Ok((row.try_get(0)?, true))
            } else {
                // Concurrent transaction inserted first — fetch the existing row
                let row = sqlx::query(
                    "SELECT schema_id FROM ducklake_schema
                     WHERE schema_name = $1 AND end_snapshot IS NULL",
                )
                .bind(name)
                .fetch_one(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok((row.try_get(0)?, false))
            }
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
                 WHERE schema_id = $1 AND table_name = $2 AND end_snapshot IS NULL",
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
            let next_tid_row = sqlx::query("SELECT nextval('ducklake_table_id_seq')")
                .fetch_one(&mut *tx)
                .await?;
            let next_table_id: i64 = next_tid_row.try_get(0)?;

            // F-026: generate UUID
            // R8-S-006: Use ON CONFLICT DO NOTHING to handle concurrent INSERT race.
            // The partial unique index ducklake_table_active_name prevents duplicates.
            let result = sqlx::query(
                "INSERT INTO ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES ($1, gen_random_uuid(), $2, $3, $4, TRUE, $5)
                 ON CONFLICT (schema_id, table_name) WHERE end_snapshot IS NULL DO NOTHING
                 RETURNING table_id",
            )
            .bind(next_table_id)
            .bind(schema_id)
            .bind(name)
            .bind(&table_path)
            .bind(snapshot_id)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = result {
                tx.commit().await?;
                Ok((row.try_get(0)?, true))
            } else {
                // Concurrent transaction inserted first — fetch the existing row
                let row = sqlx::query(
                    "SELECT table_id FROM ducklake_table
                     WHERE schema_id = $1 AND table_name = $2 AND end_snapshot IS NULL",
                )
                .bind(schema_id)
                .bind(name)
                .fetch_one(&mut *tx)
                .await?;
                tx.commit().await?;
                Ok((row.try_get(0)?, false))
            }
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
                "UPDATE ducklake_column SET end_snapshot = $1
                 WHERE table_id = $2 AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            let mut column_ids = Vec::with_capacity(columns.len());
            for (order, col) in columns.iter().enumerate() {
                let cid_row = sqlx::query("SELECT nextval('ducklake_column_id_seq')")
                    .fetch_one(&mut *tx)
                    .await?;
                let column_id: i64 = cid_row.try_get(0)?;
                sqlx::query(
                    "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, initial_default, default_value, parent_column, default_value_type, default_value_dialect, begin_snapshot)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
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
        PostgresMetadataWriter,
        pool_type = PgPool,
        dialect = crate::dialect::PostgresDialect,
        block_on = crate::metadata_provider::block_on_once,
        last_insert_id = |_tx: &mut sqlx::Transaction<'_, sqlx::Postgres>| async {
            Ok::<i64, crate::error::DuckLakeError>(0)
        }
    );

    crate::metadata_writer_impl::impl_writer_query_ops!(
        PostgresMetadataWriter,
        pool_type = PgPool,
        dialect = crate::dialect::PostgresDialect,
        block_on = crate::metadata_provider::block_on_once
    );
    fn initialize_schema(&self) -> Result<()> {
        block_on(async {
            // Wrap in a transaction for atomicity — partial catalog state on failure
            // would leave an unusable catalog (R5-S-072).
            let mut tx = self.pool.begin().await?;

            for ddl in SQL_CREATE_TABLES {
                sqlx::query(ddl).execute(&mut *tx).await?;
            }

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
                "INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
                 OVERRIDING SYSTEM VALUE VALUES (0, NOW(), 0, 0, 0)
                 ON CONFLICT (snapshot_id) DO NOTHING",
            )
            .execute(&mut *tx)
            .await?;

            // Create sequences for concurrent-safe ID generation.
            sqlx::query("CREATE SEQUENCE IF NOT EXISTS ducklake_table_id_seq")
                .execute(&mut *tx)
                .await?;
            sqlx::query("CREATE SEQUENCE IF NOT EXISTS ducklake_column_id_seq")
                .execute(&mut *tx)
                .await?;
            sqlx::query("CREATE SEQUENCE IF NOT EXISTS ducklake_view_id_seq")
                .execute(&mut *tx)
                .await?;
            sqlx::query("CREATE SEQUENCE IF NOT EXISTS ducklake_partition_id_seq")
                .execute(&mut *tx)
                .await?;

            // R8-S-005: Sequence for concurrency-safe schema_version generation.
            // Prevents concurrent DDL from reading the same MAX and producing duplicates.
            sqlx::query("CREATE SEQUENCE IF NOT EXISTS ducklake_schema_version_seq")
                .execute(&mut *tx)
                .await?;

            // R8-S-006: Partial unique indexes to prevent duplicate active schema/table names.
            // Under READ COMMITTED, two concurrent transactions can both SELECT → find nothing → INSERT.
            // These indexes enforce uniqueness at the DB level for rows with end_snapshot IS NULL.
            sqlx::query(
                "CREATE UNIQUE INDEX IF NOT EXISTS ducklake_schema_active_name
                 ON ducklake_schema (schema_name) WHERE end_snapshot IS NULL",
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "CREATE UNIQUE INDEX IF NOT EXISTS ducklake_table_active_name
                 ON ducklake_table (schema_id, table_name) WHERE end_snapshot IS NULL",
            )
            .execute(&mut *tx)
            .await?;

            // Insert initial schema_version entry (F-012)
            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version)
                 SELECT 0, 0
                 WHERE NOT EXISTS (SELECT 1 FROM ducklake_schema_versions WHERE begin_snapshot = 0)",
            )
            .execute(&mut *tx)
            .await?;

            // Sync sequences with existing data (handles migration from MAX+1 pattern).
            // Only advances the sequence if there's existing data; otherwise leaves at default start=1.
            sqlx::query(
                "SELECT setval('ducklake_table_id_seq', MAX(table_id)) FROM ducklake_table HAVING MAX(table_id) IS NOT NULL",
            )
            .fetch_optional(&mut *tx)
            .await?;
            sqlx::query(
                "SELECT setval('ducklake_column_id_seq', MAX(column_id)) FROM ducklake_column HAVING MAX(column_id) IS NOT NULL",
            )
            .fetch_optional(&mut *tx)
            .await?;
            sqlx::query(
                "SELECT setval('ducklake_view_id_seq', MAX(view_id)) FROM ducklake_view HAVING MAX(view_id) IS NOT NULL",
            )
            .fetch_optional(&mut *tx)
            .await?;
            sqlx::query(
                "SELECT setval('ducklake_partition_id_seq', MAX(partition_id)) FROM ducklake_partition_info HAVING MAX(partition_id) IS NOT NULL",
            )
            .fetch_optional(&mut *tx)
            .await?;
            // R8-S-005: Sync schema_version sequence with existing data
            sqlx::query(
                "SELECT setval('ducklake_schema_version_seq', MAX(schema_version)) FROM ducklake_snapshot HAVING MAX(schema_version) IS NOT NULL",
            )
            .fetch_optional(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(())
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
                 WHERE schema_name = $1
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
                     WHERE snapshot_id > $1 AND schema_id = $2 AND change_type = 'DROP_SCHEMA'",
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
                     WHERE schema_id = $1 AND end_snapshot IS NOT NULL AND end_snapshot > $2",
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
                     WHERE schema_id = $1 AND table_name = $2
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
                         WHERE snapshot_id > $1 AND table_id = $2 AND change_type = 'DROP_TABLE'",
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
                         WHERE table_id = $1 AND end_snapshot IS NOT NULL AND end_snapshot > $2",
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
                 WHERE snapshot_id > $1 AND table_id = $2 AND change_type = 'DROP_TABLE'",
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
                 WHERE table_id = $1 AND end_snapshot IS NOT NULL AND end_snapshot > $2",
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
                 WHERE snapshot_id > $1 AND schema_id = $2 AND change_type = 'DROP_SCHEMA'",
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
                 WHERE schema_id = $1 AND end_snapshot IS NOT NULL AND end_snapshot > $2",
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

            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
            )
            .bind(new_schema_version)
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ($1, $2)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            let vid_row = sqlx::query("SELECT nextval('ducklake_view_id_seq')")
                .fetch_one(&mut *tx)
                .await?;
            let view_id: i64 = vid_row.try_get(0)?;

            // F-026: generate UUID for view
            sqlx::query(
                "INSERT INTO ducklake_view (view_id, view_uuid, schema_id, view_name, sql, begin_snapshot)
                 VALUES ($1, gen_random_uuid(), $2, $3, $4, $5)",
            )
            .bind(view_id)
            .bind(schema_id)
            .bind(view_name)
            .bind(sql)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await?;

            // Record changes_made in DuckDB format (F-027)
            let schema_row = sqlx::query(
                "SELECT schema_name FROM ducklake_schema WHERE schema_id = $1 AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .fetch_optional(&mut *tx)
            .await?;
            let schema_name = schema_row
                .map(|r| r.try_get::<String, _>(0).unwrap_or_default())
                .unwrap_or_default();

            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES ($1, $2)
                 ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
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
                "SELECT COUNT(*) FROM ducklake_view WHERE view_id = $1 AND end_snapshot IS NULL",
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

            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
            )
            .bind(new_schema_version)
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ($1, $2)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "UPDATE ducklake_view SET end_snapshot = $1
                 WHERE view_id = $2 AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(view_id)
            .execute(&mut *tx)
            .await?;

            // Record changes_made in DuckDB format (F-027)
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES ($1, $2)
                 ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
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
                "SELECT schema_id, CAST(view_uuid AS VARCHAR), sql, dialect, column_aliases
                 FROM ducklake_view
                 WHERE view_id = $1 AND end_snapshot IS NULL",
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
                 WHERE schema_id = $1 AND view_name = $2 AND end_snapshot IS NULL",
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

            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
            )
            .bind(new_schema_version)
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ($1, $2)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // End the existing view row
            sqlx::query(
                "UPDATE ducklake_view SET end_snapshot = $1
                 WHERE view_id = $2 AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(view_id)
            .execute(&mut *tx)
            .await?;

            // Insert new view row with updated name (same view_id, same SQL)
            sqlx::query(
                "INSERT INTO ducklake_view (view_id, view_uuid, schema_id, view_name, dialect, sql, column_aliases, begin_snapshot)
                 VALUES ($1, $2::UUID, $3, $4, $5, $6, $7, $8)",
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
                 VALUES ($1, 'ALTER_VIEW', $2)",
            )
            .bind(snapshot_id)
            .bind(view_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES ($1, $2)
                 ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
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
                 WHERE table_id = $1 AND end_snapshot IS NULL
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
                        column_order: r.try_get::<i32, _>(3)? as i64,
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

            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
            )
            .bind(new_schema_version)
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ($1, $2)",
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
                    let next_cid_row = sqlx::query("SELECT nextval('ducklake_column_id_seq')")
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
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
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
                    // R5-S-003: contains_null must be TRUE because existing rows have NULL for the new column.
                    // NULL here causes DuckDB to crash when reading from the catalog.
                    sqlx::query(
                        "INSERT INTO ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan)
                         VALUES ($1, $2, TRUE, NULL)",
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
                        "UPDATE ducklake_column SET end_snapshot = $1
                         WHERE column_id = $2 AND end_snapshot IS NULL",
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
                        "UPDATE ducklake_column SET end_snapshot = $1
                         WHERE column_id = $2 AND end_snapshot IS NULL",
                    )
                    .bind(snapshot_id)
                    .bind(end_column_id)
                    .execute(&mut *tx)
                    .await?;

                    // Reuse the same column_id for the replacement row
                    // (critical for Parquet field_id mapping)
                    sqlx::query(
                        "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, initial_default, default_value, parent_column, default_value_type, default_value_dialect, begin_snapshot)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
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
                        "UPDATE ducklake_partition_info SET end_snapshot = $1
                         WHERE table_id = $2 AND end_snapshot IS NULL",
                    )
                    .bind(snapshot_id)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                    // Create new partition_info entry
                    let pid_row = sqlx::query("SELECT nextval('ducklake_partition_id_seq')")
                        .fetch_one(&mut *tx)
                        .await?;
                    let partition_id: i64 = pid_row.try_get(0)?;

                    sqlx::query(
                        "INSERT INTO ducklake_partition_info (partition_id, table_id, begin_snapshot)
                         VALUES ($1, $2, $3)",
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
                             VALUES ($1, $2, $3, $4, $5)",
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
                 VALUES ($1, 'ALTER_TABLE', $2)",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES ($1, $2)
                 ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
            )
            .bind(snapshot_id)
            .bind(format!("altered_table:{}", table_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    fn rename_table(&self, table_id: i64, new_name: &str) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Fetch the current active table row
            let table_row = sqlx::query(
                "SELECT schema_id, CAST(table_uuid AS VARCHAR), path, path_is_relative
                 FROM ducklake_table
                 WHERE table_id = $1 AND end_snapshot IS NULL",
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
                 WHERE schema_id = $1 AND table_name = $2 AND end_snapshot IS NULL",
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

            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
            )
            .bind(new_schema_version)
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ($1, $2)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // End the existing table row
            sqlx::query(
                "UPDATE ducklake_table SET end_snapshot = $1
                 WHERE table_id = $2 AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Insert new table row with updated name (same table_id, same path)
            sqlx::query(
                "INSERT INTO ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES ($1, $2::UUID, $3, $4, $5, $6, $7)",
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
                 VALUES ($1, 'ALTER_TABLE', $2)",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES ($1, $2)
                 ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
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

            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
            )
            .bind(new_schema_version)
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ($1, $2)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // End any existing comment tag for this table
            sqlx::query(
                "UPDATE ducklake_tag SET end_snapshot = $1
                 WHERE object_id = $2 AND key = 'comment' AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Insert new comment tag
            sqlx::query(
                "INSERT INTO ducklake_tag (object_id, begin_snapshot, key, value)
                 VALUES ($1, $2, 'comment', $3)",
            )
            .bind(table_id)
            .bind(snapshot_id)
            .bind(comment)
            .execute(&mut *tx)
            .await?;

            // Record change for conflict detection
            sqlx::query(
                "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id)
                 VALUES ($1, 'ALTER_TABLE', $2)",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES ($1, $2)
                 ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
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
                 WHERE table_id = $1 AND column_name = $2 AND end_snapshot IS NULL",
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

            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES (CURRENT_TIMESTAMP, $1) RETURNING snapshot_id",
            )
            .bind(new_schema_version)
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            sqlx::query(
                "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ($1, $2)",
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // End any existing comment tag for this column
            sqlx::query(
                "UPDATE ducklake_column_tag SET end_snapshot = $1
                 WHERE table_id = $2 AND column_id = $3 AND key = 'comment' AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .bind(column_id)
            .execute(&mut *tx)
            .await?;

            // Insert new comment tag
            sqlx::query(
                "INSERT INTO ducklake_column_tag (table_id, column_id, begin_snapshot, key, value)
                 VALUES ($1, $2, $3, 'comment', $4)",
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
                 VALUES ($1, 'ALTER_TABLE', $2)",
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made)
                 VALUES ($1, $2)
                 ON CONFLICT (snapshot_id) DO UPDATE SET changes_made = EXCLUDED.changes_made",
            )
            .bind(snapshot_id)
            .bind(format!("altered_table:{}", table_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    // R8-S-008: Implement register_file_partition_value (parity with SQLite)
}
