//! PostgreSQL implementation of [`MetadataWriter`].
//!
//! Requires multi-threaded Tokio runtime (`#[tokio::test(flavor = "multi_thread")]`).

use crate::Result;
use crate::metadata_provider::block_on;
use crate::metadata_writer::{
    AlterTableOp, ColumnDef, ColumnStatInfo, DataFileInfo, DeleteFileInfo, MetadataWriter,
    WriteMode, WriteSetupResult,
};
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

const DEFAULT_MAX_CONNECTIONS: u32 = 5;

const SQL_CREATE_TABLES: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS ducklake_metadata (
        key VARCHAR NOT NULL,
        value VARCHAR NOT NULL,
        scope VARCHAR
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_snapshot (
        snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        snapshot_time TIMESTAMP DEFAULT NOW()
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_schema (
        schema_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        schema_name VARCHAR NOT NULL,
        path VARCHAR NOT NULL DEFAULT '',
        path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_table (
        table_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        schema_id BIGINT NOT NULL,
        table_name VARCHAR NOT NULL,
        path VARCHAR NOT NULL DEFAULT '',
        path_is_relative BOOLEAN NOT NULL DEFAULT TRUE,
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_column (
        column_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        table_id BIGINT NOT NULL,
        column_name VARCHAR NOT NULL,
        column_type VARCHAR NOT NULL,
        column_order INTEGER NOT NULL,
        nulls_allowed BOOLEAN DEFAULT TRUE,
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
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_snapshot_changes (
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
        null_count BIGINT,
        min_value VARCHAR,
        max_value VARCHAR,
        PRIMARY KEY (data_file_id, column_id)
    )",
    "CREATE TABLE IF NOT EXISTS ducklake_view (
        view_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        schema_id BIGINT NOT NULL,
        view_name VARCHAR NOT NULL,
        sql VARCHAR NOT NULL,
        begin_snapshot BIGINT NOT NULL,
        end_snapshot BIGINT
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
        let row = sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time) VALUES (CURRENT_TIMESTAMP) RETURNING snapshot_id",
        )
        .fetch_one(&mut *tx)
        .await?;
        let snapshot_id: i64 = row.try_get(0)?;

        let schema_id: i64 = {
            let existing = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = $1 AND end_snapshot IS NULL",
            )
            .bind(schema_name)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = existing {
                row.try_get(0)?
            } else {
                let row = sqlx::query(
                    "INSERT INTO ducklake_schema (schema_name, path, path_is_relative, begin_snapshot)
                     VALUES ($1, $2, TRUE, $3) RETURNING schema_id",
                )
                .bind(schema_name)
                .bind(schema_name)
                .bind(snapshot_id)
                .fetch_one(&mut *tx)
                .await?;
                row.try_get(0)?
            }
        };

        let table_id: i64 = {
            let existing = sqlx::query(
                "SELECT table_id FROM ducklake_table
                 WHERE schema_id = $1 AND table_name = $2 AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .bind(table_name)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = existing {
                row.try_get(0)?
            } else {
                let row = sqlx::query(
                    "INSERT INTO ducklake_table (schema_id, table_name, path, path_is_relative, begin_snapshot)
                     VALUES ($1, $2, $3, TRUE, $4) RETURNING table_id",
                )
                .bind(schema_id)
                .bind(table_name)
                .bind(table_name)
                .bind(snapshot_id)
                .fetch_one(&mut *tx)
                .await?;
                row.try_get(0)?
            }
        };

        // Get existing columns to check schema compatibility for appends
        let rows = sqlx::query(
            "SELECT column_name, column_type, nulls_allowed
             FROM ducklake_column
             WHERE table_id = $1 AND end_snapshot IS NULL
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

        // For append mode, validate schema compatibility with evolution rules:
        // - Allowed: add nullable columns, remove columns, reorder columns
        // - Disallowed: add non-nullable columns, type changes for existing columns
        if mode == WriteMode::Append && !existing_columns.is_empty() {
            use std::collections::HashMap;

            // Build map of existing columns: name -> (type, nullable)
            let existing_map: HashMap<&str, (&str, bool)> = existing_columns
                .iter()
                .map(|(name, col_type, nullable)| (name.as_str(), (col_type.as_str(), *nullable)))
                .collect();

            for new_col in columns.iter() {
                if let Some((existing_type, _existing_nullable)) =
                    existing_map.get(new_col.name.as_str())
                {
                    // Column exists - check type matches
                    if *existing_type != new_col.ducklake_type {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "Schema evolution error: column '{}' has type '{}' in existing table but '{}' in new schema. Type changes are not allowed.",
                            new_col.name, existing_type, new_col.ducklake_type
                        )));
                    }
                } else {
                    // New column - must be nullable
                    if !new_col.is_nullable {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "Schema evolution error: new column '{}' must be nullable. Adding non-nullable columns is not allowed.",
                            new_col.name
                        )));
                    }
                }
            }
        }

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
            let row = sqlx::query(
                "INSERT INTO ducklake_column (table_id, column_name, column_type, column_order, nulls_allowed, begin_snapshot)
                 VALUES ($1, $2, $3, $4, $5, $6) RETURNING column_id",
            )
            .bind(table_id)
            .bind(&col.name)
            .bind(&col.ducklake_type)
            .bind(order as i64)
            .bind(col.is_nullable)
            .bind(snapshot_id)
            .fetch_one(&mut *tx)
            .await?;
            column_ids.push(row.try_get(0)?);
        }

        if mode == WriteMode::Replace {
            sqlx::query(
                "UPDATE ducklake_data_file SET end_snapshot = $1
                 WHERE table_id = $2 AND end_snapshot IS NULL",
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
        mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
        table_id: i64,
    ) -> Result<i64> {
        let row = sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time) VALUES (CURRENT_TIMESTAMP) RETURNING snapshot_id",
        )
        .fetch_one(&mut *tx)
        .await?;
        let snapshot_id: i64 = row.try_get(0)?;

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
            "INSERT INTO ducklake_snapshot_changes (snapshot_id, change_type, table_id)
             VALUES ($1, 'DROP_TABLE', $2)",
        )
        .bind(snapshot_id)
        .bind(table_id)
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
        let row = sqlx::query(
            "INSERT INTO ducklake_snapshot (snapshot_time) VALUES (CURRENT_TIMESTAMP) RETURNING snapshot_id",
        )
        .fetch_one(&mut *tx)
        .await?;
        let snapshot_id: i64 = row.try_get(0)?;

        sqlx::query(
            "UPDATE ducklake_schema SET end_snapshot = $1
             WHERE schema_id = $2 AND end_snapshot IS NULL",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "INSERT INTO ducklake_snapshot_changes (snapshot_id, change_type, schema_id)
             VALUES ($1, 'DROP_SCHEMA', $2)",
        )
        .bind(snapshot_id)
        .bind(schema_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(snapshot_id)
    }
}

impl MetadataWriter for PostgresMetadataWriter {
    fn create_snapshot(&self) -> Result<i64> {
        block_on(async {
            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time) VALUES (CURRENT_TIMESTAMP) RETURNING snapshot_id",
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
            let existing = sqlx::query(
                "SELECT schema_id FROM ducklake_schema
                 WHERE schema_name = $1 AND end_snapshot IS NULL",
            )
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(row) = existing {
                return Ok((row.try_get(0)?, false));
            }

            let schema_path = path.unwrap_or(name);
            let row = sqlx::query(
                "INSERT INTO ducklake_schema (schema_name, path, path_is_relative, begin_snapshot)
                 VALUES ($1, $2, TRUE, $3) RETURNING schema_id",
            )
            .bind(name)
            .bind(schema_path)
            .bind(snapshot_id)
            .fetch_one(&self.pool)
            .await?;

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
        block_on(async {
            let existing = sqlx::query(
                "SELECT table_id FROM ducklake_table
                 WHERE schema_id = $1 AND table_name = $2 AND end_snapshot IS NULL",
            )
            .bind(schema_id)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

            if let Some(row) = existing {
                return Ok((row.try_get(0)?, false));
            }

            let table_path = path.unwrap_or(name);
            let row = sqlx::query(
                "INSERT INTO ducklake_table (schema_id, table_name, path, path_is_relative, begin_snapshot)
                 VALUES ($1, $2, $3, TRUE, $4) RETURNING table_id",
            )
            .bind(schema_id)
            .bind(name)
            .bind(table_path)
            .bind(snapshot_id)
            .fetch_one(&self.pool)
            .await?;

            Ok((row.try_get(0)?, true))
        })
    }

    fn set_columns(
        &self,
        table_id: i64,
        columns: &[ColumnDef],
        snapshot_id: i64,
    ) -> Result<Vec<i64>> {
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
                let row = sqlx::query(
                    "INSERT INTO ducklake_column (table_id, column_name, column_type, column_order, nulls_allowed, begin_snapshot)
                     VALUES ($1, $2, $3, $4, $5, $6) RETURNING column_id",
                )
                .bind(table_id)
                .bind(&col.name)
                .bind(&col.ducklake_type)
                .bind(order as i64)
                .bind(col.is_nullable)
                .bind(snapshot_id)
                .fetch_one(&mut *tx)
                .await?;
                column_ids.push(row.try_get(0)?);
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
                     VALUES ($1, $2, $3, $4, $5, $6)
                     ON CONFLICT (data_file_id, column_id) DO UPDATE SET
                         null_count = EXCLUDED.null_count,
                         min_value = EXCLUDED.min_value,
                         max_value = EXCLUDED.max_value",
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
            let row = sqlx::query(
                "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, begin_snapshot)
                 VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING data_file_id",
            )
            .bind(table_id)
            .bind(&file.path)
            .bind(file.path_is_relative)
            .bind(file.file_size_bytes)
            .bind(file.footer_size)
            .bind(file.record_count)
            .bind(snapshot_id)
            .fetch_one(&self.pool)
            .await?;
            Ok(row.try_get(0)?)
        })
    }

    fn end_table_files(&self, table_id: i64, snapshot_id: i64) -> Result<u64> {
        block_on(async {
            let result = sqlx::query(
                "UPDATE ducklake_data_file SET end_snapshot = $1
                 WHERE table_id = $2 AND end_snapshot IS NULL",
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
            let row =
                sqlx::query("SELECT value FROM ducklake_metadata WHERE key = $1 AND scope IS NULL")
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
            sqlx::query("DELETE FROM ducklake_metadata WHERE key = 'data_path' AND scope IS NULL")
                .execute(&self.pool)
                .await?;

            sqlx::query(
                "INSERT INTO ducklake_metadata (key, value, scope)
                 VALUES ('data_path', $1, NULL)",
            )
            .bind(path)
            .execute(&self.pool)
            .await?;

            Ok(())
        })
    }

    fn initialize_schema(&self) -> Result<()> {
        block_on(async {
            for ddl in SQL_CREATE_TABLES {
                sqlx::query(ddl).execute(&self.pool).await?;
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
                "UPDATE ducklake_delete_file SET end_snapshot = $1
                 WHERE data_file_id = $2 AND table_id = $3 AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(file.data_file_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Insert the new delete file
            let row = sqlx::query(
                "INSERT INTO ducklake_delete_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, delete_count, begin_snapshot)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING delete_file_id",
            )
            .bind(file.data_file_id)
            .bind(table_id)
            .bind(&file.path)
            .bind(file.path_is_relative)
            .bind(file.file_size_bytes)
            .bind(file.footer_size)
            .bind(file.delete_count)
            .bind(snapshot_id)
            .fetch_one(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(row.try_get(0)?)
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
                 WHERE schema_id = $1 AND end_snapshot IS NULL",
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
                 WHERE schema_name = $1
                 ORDER BY schema_id DESC LIMIT 1",
            )
            .bind(schema_name)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(schema_row) = schema_row {
                let schema_id: i64 = schema_row.try_get(0)?;

                let schema_drop = sqlx::query(
                    "SELECT COUNT(*) FROM ducklake_snapshot_changes
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

                let table_row = sqlx::query(
                    "SELECT table_id FROM ducklake_table
                     WHERE schema_id = $1 AND table_name = $2
                     ORDER BY table_id DESC LIMIT 1",
                )
                .bind(schema_id)
                .bind(table_name)
                .fetch_optional(&mut *tx)
                .await?;

                if let Some(table_row) = table_row {
                    let table_id: i64 = table_row.try_get(0)?;
                    let table_drop = sqlx::query(
                        "SELECT COUNT(*) FROM ducklake_snapshot_changes
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
                "SELECT COUNT(*) FROM ducklake_snapshot_changes
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

            Self::drop_table_inner(tx, table_id).await
        })
    }

    fn drop_schema_checked(&self, schema_id: i64, since_snapshot: i64) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            let drop_check = sqlx::query(
                "SELECT COUNT(*) FROM ducklake_snapshot_changes
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

            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time) VALUES (CURRENT_TIMESTAMP) RETURNING snapshot_id",
            )
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            let row = sqlx::query(
                "INSERT INTO ducklake_view (schema_id, view_name, sql, begin_snapshot)
                 VALUES ($1, $2, $3, $4) RETURNING view_id",
            )
            .bind(schema_id)
            .bind(view_name)
            .bind(sql)
            .bind(snapshot_id)
            .fetch_one(&mut *tx)
            .await?;
            let view_id: i64 = row.try_get(0)?;

            tx.commit().await?;
            Ok((view_id, snapshot_id))
        })
    }

    fn drop_view(&self, view_id: i64) -> Result<i64> {
        block_on(async {
            let mut tx = self.pool.begin().await?;

            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time) VALUES (CURRENT_TIMESTAMP) RETURNING snapshot_id",
            )
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            sqlx::query(
                "UPDATE ducklake_view SET end_snapshot = $1
                 WHERE view_id = $2 AND end_snapshot IS NULL",
            )
            .bind(snapshot_id)
            .bind(view_id)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        })
    }

    fn alter_table(&self, table_id: i64, op: &AlterTableOp) -> Result<i64> {
        use crate::metadata_writer::is_type_promotion_allowed;

        block_on(async {
            let mut tx = self.pool.begin().await?;

            // Get active columns for validation
            let col_rows = sqlx::query(
                "SELECT column_id, column_name, column_type, column_order, nulls_allowed
                 FROM ducklake_column
                 WHERE table_id = $1 AND end_snapshot IS NULL
                 ORDER BY column_order",
            )
            .bind(table_id)
            .fetch_all(&mut *tx)
            .await?;

            if col_rows.is_empty() {
                return Err(crate::error::DuckLakeError::Internal(
                    "Cannot alter table: no active columns found (table may be dropped)"
                        .to_string(),
                ));
            }

            // Create a new snapshot
            let row = sqlx::query(
                "INSERT INTO ducklake_snapshot (snapshot_time) VALUES (CURRENT_TIMESTAMP) RETURNING snapshot_id",
            )
            .fetch_one(&mut *tx)
            .await?;
            let snapshot_id: i64 = row.try_get(0)?;

            match op {
                AlterTableOp::AddColumn {
                    column,
                } => {
                    if !column.is_nullable {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "Cannot add non-nullable column '{}': new columns must be nullable since existing rows have no value",
                            column.name
                        )));
                    }

                    for row in &col_rows {
                        let name: String = row.try_get(1)?;
                        if name == column.name {
                            return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                                "Column '{}' already exists in table",
                                column.name
                            )));
                        }
                    }

                    let max_order: i64 = col_rows
                        .iter()
                        .map(|r| r.try_get::<i64, _>(3).unwrap_or(0))
                        .max()
                        .unwrap_or(-1);

                    sqlx::query(
                        "INSERT INTO ducklake_column (table_id, column_name, column_type, column_order, nulls_allowed, begin_snapshot)
                         VALUES ($1, $2, $3, $4, $5, $6)",
                    )
                    .bind(table_id)
                    .bind(&column.name)
                    .bind(&column.ducklake_type)
                    .bind(max_order + 1)
                    .bind(column.is_nullable)
                    .bind(snapshot_id)
                    .execute(&mut *tx)
                    .await?;
                },

                AlterTableOp::DropColumn {
                    column_name,
                } => {
                    if col_rows.len() == 1 {
                        return Err(crate::error::DuckLakeError::InvalidConfig(
                            "Cannot drop column: table only has one column remaining".to_string(),
                        ));
                    }

                    let target = col_rows
                        .iter()
                        .find(|r| r.try_get::<String, _>(1).unwrap_or_default() == *column_name);

                    let Some(target_row) = target else {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "Column '{}' not found in table",
                            column_name
                        )));
                    };

                    let column_id: i64 = target_row.try_get(0)?;

                    sqlx::query(
                        "UPDATE ducklake_column SET end_snapshot = $1
                         WHERE column_id = $2",
                    )
                    .bind(snapshot_id)
                    .bind(column_id)
                    .execute(&mut *tx)
                    .await?;
                },

                AlterTableOp::RenameColumn {
                    old_name,
                    new_name,
                } => {
                    let target = col_rows
                        .iter()
                        .find(|r| r.try_get::<String, _>(1).unwrap_or_default() == *old_name);

                    let Some(target_row) = target else {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "Column '{}' not found in table",
                            old_name
                        )));
                    };

                    for row in &col_rows {
                        let name: String = row.try_get(1)?;
                        if name == *new_name {
                            return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                                "Column '{}' already exists in table",
                                new_name
                            )));
                        }
                    }

                    let column_id: i64 = target_row.try_get(0)?;
                    let col_type: String = target_row.try_get(2)?;
                    let col_order: i64 = target_row.try_get(3)?;
                    let nullable: bool = target_row.try_get::<Option<bool>, _>(4)?.unwrap_or(true);

                    sqlx::query(
                        "UPDATE ducklake_column SET end_snapshot = $1
                         WHERE column_id = $2",
                    )
                    .bind(snapshot_id)
                    .bind(column_id)
                    .execute(&mut *tx)
                    .await?;

                    sqlx::query(
                        "INSERT INTO ducklake_column (table_id, column_name, column_type, column_order, nulls_allowed, begin_snapshot)
                         VALUES ($1, $2, $3, $4, $5, $6)",
                    )
                    .bind(table_id)
                    .bind(new_name)
                    .bind(&col_type)
                    .bind(col_order)
                    .bind(nullable)
                    .bind(snapshot_id)
                    .execute(&mut *tx)
                    .await?;
                },

                AlterTableOp::AlterColumnType(alter_type) => {
                    let target = col_rows.iter().find(|r| {
                        r.try_get::<String, _>(1).unwrap_or_default() == alter_type.column_name
                    });

                    let Some(target_row) = target else {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "Column '{}' not found in table",
                            alter_type.column_name
                        )));
                    };

                    let column_id: i64 = target_row.try_get(0)?;
                    let current_type: String = target_row.try_get(2)?;
                    let col_order: i64 = target_row.try_get(3)?;
                    let nullable: bool = target_row.try_get::<Option<bool>, _>(4)?.unwrap_or(true);

                    if !is_type_promotion_allowed(&current_type, &alter_type.new_type) {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "Cannot change type of column '{}' from '{}' to '{}': only widening type promotions are allowed",
                            alter_type.column_name, current_type, alter_type.new_type
                        )));
                    }

                    sqlx::query(
                        "UPDATE ducklake_column SET end_snapshot = $1
                         WHERE column_id = $2",
                    )
                    .bind(snapshot_id)
                    .bind(column_id)
                    .execute(&mut *tx)
                    .await?;

                    sqlx::query(
                        "INSERT INTO ducklake_column (table_id, column_name, column_type, column_order, nulls_allowed, begin_snapshot)
                         VALUES ($1, $2, $3, $4, $5, $6)",
                    )
                    .bind(table_id)
                    .bind(&alter_type.column_name)
                    .bind(&alter_type.new_type)
                    .bind(col_order)
                    .bind(nullable)
                    .bind(snapshot_id)
                    .execute(&mut *tx)
                    .await?;
                },
            }

            // Record change for conflict detection
            sqlx::query(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, change_type, table_id)
                 VALUES ($1, 'ALTER_TABLE', $2)",
            )
            .bind(snapshot_id)
            .bind(table_id)
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
                 WHERE table_id = $1 AND end_snapshot IS NULL
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
}
