//! SQLite metadata provider for DuckLake catalogs.

use std::sync::Arc;

use crate::Result;
use crate::metadata_provider::{
    ColumnWithTable, DataFileChange, DeleteFileChange, DuckLakeFileData, DuckLakeTableColumn,
    DuckLakeTableFile, FileColumnStats, FilePartitionValue, FileWithTable, InlinedDataRow,
    MetadataProvider, PartitionColumn, SQL_GET_FILE_COLUMN_STATS, SQL_GET_FILE_PARTITION_VALUES,
    SQL_GET_PARTITION_COLUMNS, SQL_GET_VIEW_BY_NAME, SQL_LIST_VIEWS, SQL_VIEW_EXISTS,
    SchemaMetadata, SnapshotMetadata, TableMetadata, TableWithSchema, ViewMetadata, block_on,
    quote_identifier,
};
use sqlx::Row;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
// R6-S-031: Removed NaiveDateTime — snapshot_time read as String for cross-engine compat

/// Note: This provider requires a multi-threaded Tokio runtime
/// (`tokio::runtime::Builder::new_multi_thread()`) because it uses
/// `tokio::task::block_in_place()` to bridge async sqlx operations.
#[derive(Debug, Clone)]
pub struct SqliteMetadataProvider {
    pub(crate) pool: SqlitePool,
}

impl SqliteMetadataProvider {
    /// Creates a new provider for an existing DuckLake catalog.
    ///
    /// Connection string format: `sqlite:///path/to/catalog.db` or `sqlite::memory:`
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await?;

        Ok(Self {
            pool,
        })
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

impl MetadataProvider for SqliteMetadataProvider {
    fn get_current_snapshot(&self) -> Result<i64> {
        block_on(async {
            let row = sqlx::query("SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot")
                .fetch_one(&self.pool)
                .await?;
            Ok(row.try_get(0)?)
        })
    }

    fn get_data_path(&self) -> Result<String> {
        block_on(async {
            let row =
                sqlx::query("SELECT value FROM ducklake_metadata WHERE key = ? AND scope IS NULL")
                    .bind("data_path")
                    .fetch_optional(&self.pool)
                    .await?;

            match row {
                Some(r) => Ok(r.try_get(0)?),
                None => Err(crate::error::DuckLakeError::InvalidConfig(
                    "Missing required catalog metadata: 'data_path' not configured. \
                     The catalog may be uninitialized or corrupted."
                        .to_string(),
                )),
            }
        })
    }

    fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT s.snapshot_id, s.snapshot_time, s.schema_version,
                        c.changes_made, c.author, c.commit_message, c.commit_extra_info
                 FROM ducklake_snapshot s
                 LEFT JOIN ducklake_snapshot_changes c ON s.snapshot_id = c.snapshot_id
                 ORDER BY s.snapshot_id",
            )
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let snapshot_id: i64 = row.try_get(0)?;
                    // R6-S-031: Read snapshot_time as String to handle both TEXT and TIMESTAMPTZ
                    let snapshot_time: Option<String> = row.try_get(1)?;

                    Ok(SnapshotMetadata {
                        snapshot_id,
                        snapshot_time,
                        schema_version: row.try_get(2)?,
                        changes: row.try_get(3)?,
                        author: row.try_get(4)?,
                        commit_message: row.try_get(5)?,
                        commit_extra_info: row.try_get(6)?,
                    })
                })
                .collect()
        })
    }

    fn list_schemas(&self, snapshot_id: i64) -> Result<Vec<SchemaMetadata>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema
                 WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
            )
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(SchemaMetadata {
                        schema_id: row.try_get(0)?,
                        schema_name: row.try_get(1)?,
                        path: row.try_get(2)?,
                        path_is_relative: row.try_get(3)?,
                    })
                })
                .collect()
        })
    }

    fn list_tables(&self, schema_id: i64, snapshot_id: i64) -> Result<Vec<TableMetadata>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT table_id, table_name, path, path_is_relative FROM ducklake_table
                 WHERE schema_id = ?
                   AND ? >= begin_snapshot
                   AND (? < end_snapshot OR end_snapshot IS NULL)",
            )
            .bind(schema_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(TableMetadata {
                        table_id: row.try_get(0)?,
                        table_name: row.try_get(1)?,
                        path: row.try_get(2)?,
                        path_is_relative: row.try_get(3)?,
                    })
                })
                .collect()
        })
    }

    fn get_table_structure(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeTableColumn>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT column_id, column_name, column_type, nulls_allowed
                 FROM ducklake_column
                 WHERE table_id = ?
                   AND ? >= begin_snapshot
                   AND (? < end_snapshot OR end_snapshot IS NULL)
                 ORDER BY column_order",
            )
            .bind(table_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let nulls_allowed: Option<bool> = row.try_get(3)?;
                    let col_name: String = row.try_get(1)?;
                    if nulls_allowed.is_none() {
                        tracing::warn!(
                            column_name = %col_name,
                            "nulls_allowed is NULL in catalog — defaulting to true; this may indicate catalog corruption"
                        );
                    }
                    Ok(DuckLakeTableColumn {
                        column_id: row.try_get(0)?,
                        column_name: col_name,
                        column_type: row.try_get(2)?,
                        is_nullable: nulls_allowed.unwrap_or(true),
                    })
                })
                .collect()
        })
    }

    fn get_table_files_for_select(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeTableFile>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT
                    data.data_file_id,
                    data.path AS data_file_path,
                    data.path_is_relative AS data_path_is_relative,
                    data.file_size_bytes AS data_file_size,
                    data.footer_size AS data_footer_size,
                    data.encryption_key AS data_encryption_key,
                    del.delete_file_id,
                    del.path AS delete_file_path,
                    del.path_is_relative AS delete_path_is_relative,
                    del.file_size_bytes AS delete_file_size,
                    del.footer_size AS delete_footer_size,
                    del.encryption_key AS delete_encryption_key,
                    del.delete_count,
                    data.begin_snapshot,
                    data.row_id_start,
                    data.record_count
                FROM ducklake_data_file AS data
                LEFT JOIN ducklake_delete_file AS del
                    ON data.data_file_id = del.data_file_id
                    AND del.table_id = ?
                    AND ? >= del.begin_snapshot
                    AND (? < del.end_snapshot OR del.end_snapshot IS NULL)
                WHERE data.table_id = ?
                  AND ? >= data.begin_snapshot
                  AND (? < data.end_snapshot OR data.end_snapshot IS NULL)",
            )
            .bind(table_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(table_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let data_file_id: Option<i64> = row.try_get(0)?;
                    let data_file = DuckLakeFileData {
                        path: row.try_get(1)?,
                        path_is_relative: row.try_get(2)?,
                        file_size_bytes: row.try_get(3)?,
                        footer_size: row.try_get(4)?,
                        encryption_key: row.try_get(5)?,
                    };

                    let delete_file = if row.try_get::<Option<i64>, _>(6)?.is_some() {
                        Some(DuckLakeFileData {
                            path: row.try_get(7)?,
                            path_is_relative: row.try_get(8)?,
                            file_size_bytes: row.try_get(9)?,
                            footer_size: row.try_get(10)?,
                            encryption_key: row.try_get(11)?,
                        })
                    } else {
                        None
                    };

                    let begin_snapshot: Option<i64> = row.try_get(13)?;
                    let row_id_start: Option<i64> = row.try_get(14)?;
                    let record_count: Option<i64> = row.try_get(15)?;

                    Ok(DuckLakeTableFile {
                        data_file_id,
                        file: data_file,
                        delete_file,
                        row_id_start,
                        snapshot_id: begin_snapshot,
                        max_row_count: record_count,
                    })
                })
                .collect()
        })
    }

    fn get_schema_by_name(&self, name: &str, snapshot_id: i64) -> Result<Option<SchemaMetadata>> {
        block_on(async {
            let row = sqlx::query(
                "SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema
                 WHERE schema_name = ?
                   AND ? >= begin_snapshot
                   AND (? < end_snapshot OR end_snapshot IS NULL)",
            )
            .bind(name)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_optional(&self.pool)
            .await?;

            match row {
                Some(r) => Ok(Some(SchemaMetadata {
                    schema_id: r.try_get(0)?,
                    schema_name: r.try_get(1)?,
                    path: r.try_get(2)?,
                    path_is_relative: r.try_get(3)?,
                })),
                None => Ok(None),
            }
        })
    }

    fn get_table_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> Result<Option<TableMetadata>> {
        block_on(async {
            let row = sqlx::query(
                "SELECT table_id, table_name, path, path_is_relative FROM ducklake_table
                 WHERE schema_id = ?
                   AND table_name = ?
                   AND ? >= begin_snapshot
                   AND (? < end_snapshot OR end_snapshot IS NULL)",
            )
            .bind(schema_id)
            .bind(name)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_optional(&self.pool)
            .await?;

            match row {
                Some(r) => Ok(Some(TableMetadata {
                    table_id: r.try_get(0)?,
                    table_name: r.try_get(1)?,
                    path: r.try_get(2)?,
                    path_is_relative: r.try_get(3)?,
                })),
                None => Ok(None),
            }
        })
    }

    fn table_exists(&self, schema_id: i64, name: &str, snapshot_id: i64) -> Result<bool> {
        block_on(async {
            let row = sqlx::query(
                "SELECT COUNT(*) FROM ducklake_table
                 WHERE schema_id = ?
                   AND table_name = ?
                   AND ? >= begin_snapshot
                   AND (? < end_snapshot OR end_snapshot IS NULL)",
            )
            .bind(schema_id)
            .bind(name)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_one(&self.pool)
            .await?;

            let count: i64 = row.try_get(0)?;
            Ok(count > 0)
        })
    }

    fn list_all_tables(&self, snapshot_id: i64) -> Result<Vec<TableWithSchema>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT s.schema_name, s.schema_id, t.table_id, t.table_name,
                        CAST(t.table_uuid AS TEXT) AS table_uuid, t.path, t.path_is_relative
                 FROM ducklake_schema s
                 JOIN ducklake_table t ON s.schema_id = t.schema_id
                 WHERE ? >= s.begin_snapshot
                   AND (? < s.end_snapshot OR s.end_snapshot IS NULL)
                   AND ? >= t.begin_snapshot
                   AND (? < t.end_snapshot OR t.end_snapshot IS NULL)
                 ORDER BY s.schema_name, t.table_name",
            )
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let schema_name: String = row.try_get(0)?;
                    let schema_id: i64 = row.try_get(1)?;
                    let table = TableMetadata {
                        table_id: row.try_get(2)?,
                        table_name: row.try_get(3)?,
                        path: row.try_get(5)?,
                        path_is_relative: row.try_get(6)?,
                    };
                    let table_uuid: Option<String> = row.try_get(4)?;
                    Ok(TableWithSchema {
                        schema_name,
                        schema_id,
                        table_uuid,
                        table,
                    })
                })
                .collect()
        })
    }

    fn list_all_columns(&self, snapshot_id: i64) -> Result<Vec<ColumnWithTable>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT s.schema_name, t.table_name, c.column_id, c.column_name, c.column_type, c.nulls_allowed
                 FROM ducklake_schema s
                 JOIN ducklake_table t ON s.schema_id = t.schema_id
                 JOIN ducklake_column c ON t.table_id = c.table_id
                 WHERE ? >= s.begin_snapshot
                   AND (? < s.end_snapshot OR s.end_snapshot IS NULL)
                   AND ? >= t.begin_snapshot
                   AND (? < t.end_snapshot OR t.end_snapshot IS NULL)
                   AND ? >= c.begin_snapshot
                   AND (? < c.end_snapshot OR c.end_snapshot IS NULL)
                 ORDER BY s.schema_name, t.table_name, c.column_order",
            )
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let schema_name: String = row.try_get(0)?;
                    let table_name: String = row.try_get(1)?;
                    let nulls_allowed: Option<bool> = row.try_get(5)?;
                    let col_name: String = row.try_get(3)?;
                    if nulls_allowed.is_none() {
                        tracing::warn!(
                            column_name = %col_name,
                            "nulls_allowed is NULL in catalog — defaulting to true; this may indicate catalog corruption"
                        );
                    }
                    let column = DuckLakeTableColumn {
                        column_id: row.try_get(2)?,
                        column_name: col_name,
                        column_type: row.try_get(4)?,
                        is_nullable: nulls_allowed.unwrap_or(true),
                    };
                    Ok(ColumnWithTable {
                        schema_name,
                        table_name,
                        column,
                    })
                })
                .collect()
        })
    }

    fn list_all_files(&self, snapshot_id: i64) -> Result<Vec<FileWithTable>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT
                    s.schema_name,
                    t.table_name,
                    data.data_file_id,
                    data.path AS data_file_path,
                    data.path_is_relative AS data_path_is_relative,
                    data.file_size_bytes AS data_file_size,
                    data.footer_size AS data_footer_size,
                    data.encryption_key AS data_encryption_key,
                    del.delete_file_id,
                    del.path AS delete_file_path,
                    del.path_is_relative AS delete_path_is_relative,
                    del.file_size_bytes AS delete_file_size,
                    del.footer_size AS delete_footer_size,
                    del.encryption_key AS delete_encryption_key,
                    data.record_count
                FROM ducklake_schema s
                JOIN ducklake_table t ON s.schema_id = t.schema_id
                JOIN ducklake_data_file data ON t.table_id = data.table_id
                LEFT JOIN ducklake_delete_file del
                    ON data.data_file_id = del.data_file_id
                    AND del.table_id = t.table_id
                    AND ? >= del.begin_snapshot
                    AND (? < del.end_snapshot OR del.end_snapshot IS NULL)
                WHERE ? >= s.begin_snapshot
                  AND (? < s.end_snapshot OR s.end_snapshot IS NULL)
                  AND ? >= t.begin_snapshot
                  AND (? < t.end_snapshot OR t.end_snapshot IS NULL)
                  AND ? >= data.begin_snapshot
                  AND (? < data.end_snapshot OR data.end_snapshot IS NULL)
                ORDER BY s.schema_name, t.table_name, data.path",
            )
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let data_file = DuckLakeFileData {
                        path: row.try_get(3)?,
                        path_is_relative: row.try_get(4)?,
                        file_size_bytes: row.try_get(5)?,
                        footer_size: row.try_get(6)?,
                        encryption_key: row.try_get(7)?,
                    };

                    let delete_file = if row.try_get::<Option<i64>, _>(8)?.is_some() {
                        Some(DuckLakeFileData {
                            path: row.try_get(9)?,
                            path_is_relative: row.try_get(10)?,
                            file_size_bytes: row.try_get(11)?,
                            footer_size: row.try_get(12)?,
                            encryption_key: row.try_get(13)?,
                        })
                    } else {
                        None
                    };

                    Ok(FileWithTable {
                        schema_name: row.try_get(0)?,
                        table_name: row.try_get(1)?,
                        file: DuckLakeTableFile {
                            data_file_id: row.try_get(2)?,
                            file: data_file,
                            delete_file,
                            row_id_start: None,
                            snapshot_id: None,
                            max_row_count: row.try_get(14)?,
                        },
                    })
                })
                .collect()
        })
    }

    fn get_data_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> Result<Vec<DataFileChange>> {
        block_on(async {
            let rows = sqlx::query(
                "SELECT
                    data.begin_snapshot,
                    data.path,
                    data.path_is_relative,
                    data.file_size_bytes,
                    data.footer_size,
                    data.encryption_key
                FROM ducklake_data_file AS data
                WHERE data.table_id = ?
                  AND data.begin_snapshot > ?
                  AND data.begin_snapshot <= ?
                ORDER BY data.begin_snapshot",
            )
            .bind(table_id)
            .bind(start_snapshot)
            .bind(end_snapshot)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(DataFileChange {
                        begin_snapshot: row.try_get(0)?,
                        path: row.try_get(1)?,
                        path_is_relative: row.try_get(2)?,
                        file_size_bytes: row.try_get(3)?,
                        footer_size: row.try_get(4)?,
                        encryption_key: row.try_get(5)?,
                    })
                })
                .collect()
        })
    }

    fn get_delete_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> Result<Vec<DeleteFileChange>> {
        block_on(async {
            // SQLite doesn't support LATERAL JOIN, so we use correlated subqueries instead
            // This query has two parts:
            // 1. Incremental deletes: delete files added in the snapshot range
            // 2. Full file deletes: data files that were completely removed in the snapshot range
            let rows = sqlx::query(
                r#"
-- Part 1: Incremental deletes (delete file added)
SELECT
    data.path AS data_path,
    data.path_is_relative AS data_path_is_relative,
    data.file_size_bytes AS data_file_size,
    data.footer_size AS data_footer_size,
    data.row_id_start,
    data.record_count,
    data.mapping_id,

    cd.path AS current_delete_path,
    cd.path_is_relative AS current_delete_path_is_relative,
    cd.file_size_bytes AS current_delete_file_size,
    cd.footer_size AS current_delete_footer_size,

    -- Previous delete file (correlated subquery instead of LATERAL)
    (SELECT path FROM ducklake_delete_file pd
     WHERE pd.table_id = ?
       AND pd.data_file_id = cd.data_file_id
       AND pd.begin_snapshot < cd.begin_snapshot
     ORDER BY pd.begin_snapshot DESC LIMIT 1) AS prev_delete_path,
    (SELECT path_is_relative FROM ducklake_delete_file pd
     WHERE pd.table_id = ?
       AND pd.data_file_id = cd.data_file_id
       AND pd.begin_snapshot < cd.begin_snapshot
     ORDER BY pd.begin_snapshot DESC LIMIT 1) AS prev_delete_path_is_relative,
    (SELECT file_size_bytes FROM ducklake_delete_file pd
     WHERE pd.table_id = ?
       AND pd.data_file_id = cd.data_file_id
       AND pd.begin_snapshot < cd.begin_snapshot
     ORDER BY pd.begin_snapshot DESC LIMIT 1) AS prev_delete_file_size,
    (SELECT footer_size FROM ducklake_delete_file pd
     WHERE pd.table_id = ?
       AND pd.data_file_id = cd.data_file_id
       AND pd.begin_snapshot < cd.begin_snapshot
     ORDER BY pd.begin_snapshot DESC LIMIT 1) AS prev_delete_footer_size,

    data.encryption_key AS data_encryption_key,
    cd.begin_snapshot AS snapshot_id
FROM ducklake_delete_file cd
JOIN ducklake_data_file data ON data.data_file_id = cd.data_file_id
WHERE cd.table_id = ?
  AND cd.begin_snapshot > ?
  AND cd.begin_snapshot <= ?
  AND data.table_id = ?

UNION ALL

-- Part 2: Full file deletes (data file removed entirely)
SELECT
    data.path AS data_path,
    data.path_is_relative AS data_path_is_relative,
    data.file_size_bytes AS data_file_size,
    data.footer_size AS data_footer_size,
    data.row_id_start,
    data.record_count,
    data.mapping_id,

    NULL AS current_delete_path,
    NULL AS current_delete_path_is_relative,
    NULL AS current_delete_file_size,
    NULL AS current_delete_footer_size,

    -- Previous delete file
    (SELECT path FROM ducklake_delete_file pd
     WHERE pd.table_id = ?
       AND pd.data_file_id = data.data_file_id
       AND pd.begin_snapshot < data.end_snapshot
     ORDER BY pd.begin_snapshot DESC LIMIT 1) AS prev_delete_path,
    (SELECT path_is_relative FROM ducklake_delete_file pd
     WHERE pd.table_id = ?
       AND pd.data_file_id = data.data_file_id
       AND pd.begin_snapshot < data.end_snapshot
     ORDER BY pd.begin_snapshot DESC LIMIT 1) AS prev_delete_path_is_relative,
    (SELECT file_size_bytes FROM ducklake_delete_file pd
     WHERE pd.table_id = ?
       AND pd.data_file_id = data.data_file_id
       AND pd.begin_snapshot < data.end_snapshot
     ORDER BY pd.begin_snapshot DESC LIMIT 1) AS prev_delete_file_size,
    (SELECT footer_size FROM ducklake_delete_file pd
     WHERE pd.table_id = ?
       AND pd.data_file_id = data.data_file_id
       AND pd.begin_snapshot < data.end_snapshot
     ORDER BY pd.begin_snapshot DESC LIMIT 1) AS prev_delete_footer_size,

    data.encryption_key AS data_encryption_key,
    data.end_snapshot AS snapshot_id
FROM ducklake_data_file data
WHERE data.table_id = ?
  AND data.end_snapshot > ?
  AND data.end_snapshot <= ?
"#,
            )
            // Part 1 bindings: 4x table_id for prev subqueries, table_id for cd, start, end, table_id for data
            .bind(table_id)
            .bind(table_id)
            .bind(table_id)
            .bind(table_id)
            .bind(table_id)
            .bind(start_snapshot)
            .bind(end_snapshot)
            .bind(table_id)
            // Part 2 bindings: 4x table_id for prev subqueries, table_id for data, start, end
            .bind(table_id)
            .bind(table_id)
            .bind(table_id)
            .bind(table_id)
            .bind(table_id)
            .bind(start_snapshot)
            .bind(end_snapshot)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(DeleteFileChange {
                        // data file
                        data_file_path: row.try_get(0)?,
                        data_file_path_is_relative: row.try_get(1)?,
                        data_file_size_bytes: row.try_get(2)?,
                        data_file_footer_size: row.try_get(3)?,
                        data_row_id_start: row.try_get(4)?,
                        data_record_count: row.try_get(5)?,
                        data_mapping_id: row.try_get(6)?,

                        // current delete
                        current_delete_path: row.try_get(7)?,
                        current_delete_path_is_relative: row.try_get(8)?,
                        current_delete_file_size_bytes: row.try_get(9)?,
                        current_delete_footer_size: row.try_get(10)?,

                        // previous delete
                        previous_delete_path: row.try_get(11)?,
                        previous_delete_path_is_relative: row.try_get(12)?,
                        previous_delete_file_size_bytes: row.try_get(13)?,
                        previous_delete_footer_size: row.try_get(14)?,

                        // data file encryption key (R6-S-012)
                        data_encryption_key: row.try_get(15)?,

                        // snapshot
                        snapshot_id: row.try_get(16)?,
                    })
                })
                .collect()
        })
    }

    fn get_file_column_stats(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<FileColumnStats>> {
        block_on(async {
            sqlx::query(SQL_GET_FILE_COLUMN_STATS)
                .bind(table_id)
                .bind(snapshot_id)
                .bind(snapshot_id)
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|row| {
                    Ok(FileColumnStats {
                        data_file_id: row.try_get(0)?,
                        column_name: row.try_get(1)?,
                        null_count: row.try_get(2)?,
                        min_value: row.try_get(3)?,
                        max_value: row.try_get(4)?,
                    })
                })
                .collect()
        })
    }

    fn list_views(&self, schema_id: i64, snapshot_id: i64) -> Result<Vec<ViewMetadata>> {
        block_on(async {
            sqlx::query(SQL_LIST_VIEWS)
                .bind(schema_id)
                .bind(snapshot_id)
                .bind(snapshot_id)
                .fetch_all(&self.pool)
                .await?
                .iter()
                .map(|row| {
                    Ok(ViewMetadata {
                        view_id: row.try_get(0)?,
                        view_name: row.try_get(1)?,
                        sql: row.try_get(2)?,
                    })
                })
                .collect()
        })
    }

    fn get_view_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> Result<Option<ViewMetadata>> {
        block_on(async {
            let row = sqlx::query(SQL_GET_VIEW_BY_NAME)
                .bind(schema_id)
                .bind(name)
                .bind(snapshot_id)
                .bind(snapshot_id)
                .fetch_optional(&self.pool)
                .await?;
            match row {
                Some(row) => Ok(Some(ViewMetadata {
                    view_id: row.try_get(0)?,
                    view_name: row.try_get(1)?,
                    sql: row.try_get(2)?,
                })),
                None => Ok(None),
            }
        })
    }

    fn view_exists(&self, schema_id: i64, name: &str, snapshot_id: i64) -> Result<bool> {
        block_on(async {
            let row = sqlx::query(SQL_VIEW_EXISTS)
                .bind(schema_id)
                .bind(name)
                .bind(snapshot_id)
                .bind(snapshot_id)
                .fetch_one(&self.pool)
                .await?;
            Ok(row.try_get::<bool, _>(0)?)
        })
    }

    fn get_partition_columns(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<PartitionColumn>> {
        block_on(async {
            sqlx::query(SQL_GET_PARTITION_COLUMNS)
                .bind(table_id)
                .bind(snapshot_id)
                .bind(snapshot_id)
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|row| {
                    Ok(PartitionColumn {
                        partition_key_index: row.try_get(0)?,
                        column_name: row.try_get(1)?,
                        transform: row.try_get(2)?,
                    })
                })
                .collect()
        })
    }

    fn get_file_partition_values(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<FilePartitionValue>> {
        block_on(async {
            sqlx::query(SQL_GET_FILE_PARTITION_VALUES)
                .bind(table_id)
                .bind(snapshot_id)
                .bind(snapshot_id)
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .map(|row| {
                    Ok(FilePartitionValue {
                        data_file_id: row.try_get(0)?,
                        partition_key_index: row.try_get(1)?,
                        partition_value: row.try_get(2)?,
                    })
                })
                .collect()
        })
    }

    fn get_inlined_data(&self, table_id: i64, snapshot_id: i64) -> Result<Vec<InlinedDataRow>> {
        block_on(async {
            // Look up the inlined data table name, filtered by snapshot's schema_version (R5-S-028).
            // Pick the latest schema_version that doesn't exceed the snapshot's version.
            let table_info = sqlx::query(
                "SELECT table_name, schema_version FROM ducklake_inlined_data_tables \
                 WHERE table_id = ? \
                   AND schema_version <= (SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = ?) \
                 ORDER BY schema_version DESC LIMIT 1",
            )
            .bind(table_id)
            .bind(snapshot_id)
            .fetch_optional(&self.pool)
            .await?;

            let Some(info_row) = table_info else {
                return Ok(Vec::new());
            };

            let inlined_table_name: String = info_row.try_get(0)?;

            // Check if the inlined data table exists
            let exists =
                sqlx::query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?")
                    .bind(&inlined_table_name)
                    .fetch_one(&self.pool)
                    .await?;
            let count: i64 = exists.try_get(0)?;
            if count == 0 {
                return Ok(Vec::new());
            }

            // Query the inlined data table - get column names dynamically
            let pragma_query = format!(
                "PRAGMA table_info({})",
                quote_identifier(&inlined_table_name)
            );
            let columns = sqlx::query(&pragma_query).fetch_all(&self.pool).await?;

            // Column layout: row_id, begin_snapshot, end_snapshot, then user columns
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

            // Build select query with quoted identifiers to prevent SQL injection
            let col_list: Vec<String> = user_columns
                .iter()
                .map(|c| format!("CAST({} AS TEXT)", quote_identifier(c)))
                .collect();
            let select_sql = format!(
                "SELECT {} FROM {} WHERE begin_snapshot <= ? AND (end_snapshot IS NULL OR ? < end_snapshot)",
                col_list.join(", "),
                quote_identifier(&inlined_table_name),
            );

            let rows = sqlx::query(&select_sql)
                .bind(snapshot_id)
                .bind(snapshot_id)
                .fetch_all(&self.pool)
                .await?;

            let num_columns = user_columns.len();
            let user_columns = Arc::new(user_columns);
            let mut result = Vec::with_capacity(rows.len());
            for row in &rows {
                let mut values = Vec::with_capacity(num_columns);
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

    fn get_table_row_count(&self, table_id: i64, snapshot_id: i64) -> Result<Option<i64>> {
        block_on(async {
            // First get row count from data files
            let row = sqlx::query(
                "SELECT
                    CASE WHEN COUNT(*) = COUNT(data.record_count)
                        THEN COALESCE(SUM(data.record_count), 0) - COALESCE(SUM(del.delete_count), 0)
                        ELSE NULL
                    END as row_count
                FROM ducklake_data_file data
                LEFT JOIN ducklake_delete_file del
                    ON data.data_file_id = del.data_file_id
                    AND del.table_id = ?
                    AND ? >= del.begin_snapshot
                    AND (? < del.end_snapshot OR del.end_snapshot IS NULL)
                WHERE data.table_id = ?
                  AND ? >= data.begin_snapshot
                  AND (? < data.end_snapshot OR data.end_snapshot IS NULL)",
            )
            .bind(table_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .bind(table_id)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_one(&self.pool)
            .await?;

            let file_count: Option<i64> = row.try_get(0)?;

            // Also count inlined data rows
            let inlined_count = self.count_inlined_rows(table_id, snapshot_id).await?;

            match (file_count, inlined_count) {
                (Some(fc), ic) => Ok(Some(fc + ic)),
                (None, _) => Ok(None),
            }
        })
    }
}

impl SqliteMetadataProvider {
    /// Count inlined rows for a table at a given snapshot.
    async fn count_inlined_rows(&self, table_id: i64, snapshot_id: i64) -> Result<i64> {
        let table_info = sqlx::query(
            "SELECT table_name FROM ducklake_inlined_data_tables \
             WHERE table_id = ? \
               AND schema_version <= (SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = ?) \
             ORDER BY schema_version DESC LIMIT 1",
        )
        .bind(table_id)
        .bind(snapshot_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(info_row) = table_info else {
            return Ok(0);
        };

        let inlined_table_name: String = info_row.try_get(0)?;

        // Check if the inlined data table exists
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
            "SELECT COUNT(*) FROM {} WHERE begin_snapshot <= ? AND (end_snapshot IS NULL OR ? < end_snapshot)",
            quote_identifier(&inlined_table_name),
        );

        let row = sqlx::query(&count_sql)
            .bind(snapshot_id)
            .bind(snapshot_id)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.try_get(0)?)
    }
}
