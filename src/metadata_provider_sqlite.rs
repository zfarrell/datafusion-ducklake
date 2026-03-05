//! SQLite metadata provider for DuckLake catalogs.

use std::sync::Arc;

use crate::Result;
use crate::dialect::SqliteDialect;
use crate::metadata_provider::{
    ColumnWithTable, DataFileChange, DeleteFileChange, DuckLakeFileData, DuckLakeTableColumn,
    DuckLakeTableFile, FileColumnStats, FilePartitionValue, FileWithTable, InlinedDataRow,
    MetadataProvider, PartitionColumn, SchemaMetadata, SnapshotMetadata, TableMetadata,
    TableWithSchema, ViewMetadata, block_on, quote_identifier,
};
use crate::metadata_provider_impl::impl_metadata_provider;
use sqlx::Row;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

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

impl_metadata_provider!(
    SqliteMetadataProvider,
    pool_type = SqlitePool,
    dialect = SqliteDialect
);

// Override methods that differ structurally from other backends
impl SqliteMetadataProvider {
    pub(crate) fn get_delete_files_impl(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> Result<Vec<DeleteFileChange>> {
        block_on(async {
            // SQLite doesn't support LATERAL JOIN, so we use correlated subqueries instead
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
            // Part 1 bindings
            .bind(table_id)
            .bind(table_id)
            .bind(table_id)
            .bind(table_id)
            .bind(table_id)
            .bind(start_snapshot)
            .bind(end_snapshot)
            .bind(table_id)
            // Part 2 bindings
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
                        data_file_path: row.try_get(0)?,
                        data_file_path_is_relative: row.try_get(1)?,
                        data_file_size_bytes: row.try_get(2)?,
                        data_file_footer_size: row.try_get(3)?,
                        data_row_id_start: row.try_get(4)?,
                        data_record_count: row.try_get(5)?,
                        data_mapping_id: row.try_get(6)?,
                        current_delete_path: row.try_get(7)?,
                        current_delete_path_is_relative: row.try_get(8)?,
                        current_delete_file_size_bytes: row.try_get(9)?,
                        current_delete_footer_size: row.try_get(10)?,
                        previous_delete_path: row.try_get(11)?,
                        previous_delete_path_is_relative: row.try_get(12)?,
                        previous_delete_file_size_bytes: row.try_get(13)?,
                        previous_delete_footer_size: row.try_get(14)?,
                        data_encryption_key: row.try_get(15)?,
                        snapshot_id: row.try_get(16)?,
                    })
                })
                .collect()
        })
    }

    pub(crate) fn get_inlined_data_impl(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<InlinedDataRow>> {
        block_on(async {
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

            let exists =
                sqlx::query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?")
                    .bind(&inlined_table_name)
                    .fetch_one(&self.pool)
                    .await?;
            let count: i64 = exists.try_get(0)?;
            if count == 0 {
                return Ok(Vec::new());
            }

            let pragma_query = format!(
                "PRAGMA table_info({})",
                quote_identifier(&inlined_table_name)
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

    pub(crate) async fn count_inlined_rows_impl(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<i64> {
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
