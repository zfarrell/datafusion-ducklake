/// Generates a `recompute_table_column_stats` async function for a given
/// sqlx transaction type and dialect. The function deletes existing table-level
/// column stats, re-aggregates from per-file stats using type-aware comparison,
/// and inserts the new aggregates.
macro_rules! impl_recompute_table_column_stats {
    ($tx_type:ty, $dialect:expr) => {
        async fn recompute_table_column_stats(
            tx: &mut $tx_type,
            table_id: i64,
        ) -> crate::Result<()> {
            use std::collections::HashMap;
            use crate::dialect::SqlDialect;
            use crate::metadata_writer_validation::{is_numeric_type, stat_value_less_than};
            use sqlx::Row;

            let d = $dialect;

            let delete_sql = format!(
                "DELETE FROM ducklake_table_column_stats WHERE table_id = {}",
                d.ph(1),
            );
            sqlx::query(&delete_sql)
                .bind(table_id)
                .execute(&mut **tx)
                .await?;

            let select_sql = format!(
                "SELECT fcs.column_id, fcs.null_count, fcs.min_value, fcs.max_value, c.column_type
                 FROM ducklake_file_column_stats fcs
                 INNER JOIN ducklake_data_file df
                     ON fcs.data_file_id = df.data_file_id
                     AND df.table_id = fcs.table_id
                     AND df.end_snapshot IS NULL
                 INNER JOIN ducklake_column c ON fcs.column_id = c.column_id
                     AND c.end_snapshot IS NULL AND c.table_id = fcs.table_id
                 WHERE fcs.table_id = {}",
                d.ph(1),
            );
            let rows = sqlx::query(&select_sql)
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
                        }
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
                        }
                    });
                }
            }

            let insert_sql = format!(
                "INSERT INTO ducklake_table_column_stats
                 (table_id, column_id, contains_null, min_value, max_value)
                 VALUES ({}, {}, {}, {}, {})",
                d.ph(1),
                d.ph(2),
                d.ph(3),
                d.ph(4),
                d.ph(5),
            );

            for (column_id, agg) in &aggs {
                sqlx::query(&insert_sql)
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
    };
}

pub(crate) use impl_recompute_table_column_stats;

/// Generates MetadataWriter query/metadata operation implementations.
///
/// Methods: get_data_path, set_data_path, record_snapshot_changes,
/// list_active_table_ids, get_active_columns, find_table_id,
/// register_file_partition_value, get_active_partition_columns.
macro_rules! impl_writer_query_ops {
    (
        $struct_name:ty,
        pool_type = $pool_type:ty,
        dialect = $dialect:expr,
        block_on = $block_on:path
    ) => {
        fn get_data_path(&self) -> crate::Result<String> {
            use crate::dialect::SqlDialect;
            use sqlx::Row;
            let d = $dialect;
            let sql = format!(
                "SELECT value FROM ducklake_metadata WHERE {} = {} AND scope IS NULL",
                d.col("key"),
                d.ph(1),
            );
            $block_on(|| async {
                let row = sqlx::query(&sql)
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

        fn set_data_path(&self, path: &str) -> crate::Result<()> {
            use crate::dialect::SqlDialect;
            let d = $dialect;
            let delete_sql = format!(
                "DELETE FROM ducklake_metadata WHERE {} = 'data_path' AND scope IS NULL",
                d.col("key"),
            );
            let insert_sql = format!(
                "INSERT INTO ducklake_metadata ({}, value, scope) VALUES ('data_path', {}, NULL)",
                d.col("key"),
                d.ph(1),
            );
            $block_on(|| async {
                let mut tx = self.pool.begin().await?;

                sqlx::query(&delete_sql)
                    .execute(&mut *tx)
                    .await?;

                sqlx::query(&insert_sql)
                    .bind(path)
                    .execute(&mut *tx)
                    .await?;

                tx.commit().await?;
                Ok(())
            })
        }

        fn record_snapshot_changes(&self, snapshot_id: i64, changes_made: &str) -> crate::Result<()> {
            use crate::dialect::SqlDialect;
            let d = $dialect;
            let sql = format!(
                "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                d.ph(1),
                d.ph(2),
                d.upsert("snapshot_id", &["changes_made"]),
            );
            $block_on(|| async {
                sqlx::query(&sql)
                    .bind(snapshot_id)
                    .bind(changes_made)
                    .execute(&self.pool)
                    .await?;
                Ok(())
            })
        }

        fn list_active_table_ids(&self, schema_id: i64) -> crate::Result<Vec<i64>> {
            use crate::dialect::SqlDialect;
            use sqlx::Row;
            let d = $dialect;
            let sql = format!(
                "SELECT table_id FROM ducklake_table WHERE schema_id = {} AND end_snapshot IS NULL",
                d.ph(1),
            );
            $block_on(|| async {
                let rows = sqlx::query(&sql)
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

        fn get_active_columns(&self, table_id: i64) -> crate::Result<Vec<(String, String, bool)>> {
            use crate::dialect::SqlDialect;
            use sqlx::Row;
            let d = $dialect;
            let sql = format!(
                "SELECT column_name, column_type, nulls_allowed
                 FROM ducklake_column
                 WHERE table_id = {} AND end_snapshot IS NULL
                 ORDER BY column_order",
                d.ph(1),
            );
            $block_on(|| async {
                let rows = sqlx::query(&sql)
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

        fn find_table_id(&self, schema_name: &str, table_name: &str) -> crate::Result<Option<i64>> {
            use crate::dialect::SqlDialect;
            use sqlx::Row;
            let d = $dialect;
            let sql = format!(
                "SELECT t.table_id FROM ducklake_table t
                 JOIN ducklake_schema s ON t.schema_id = s.schema_id
                 WHERE s.schema_name = {} AND s.end_snapshot IS NULL
                   AND t.table_name = {} AND t.end_snapshot IS NULL",
                d.ph(1),
                d.ph(2),
            );
            $block_on(|| async {
                let row = sqlx::query(&sql)
                    .bind(schema_name)
                    .bind(table_name)
                    .fetch_optional(&self.pool)
                    .await?;

                match row {
                    Some(r) => Ok(Some(r.try_get(0)?)),
                    None => Ok(None),
                }
            })
        }

        fn register_file_partition_value(
            &self,
            data_file_id: i64,
            table_id: i64,
            partition_key_index: i32,
            partition_value: Option<&str>,
        ) -> crate::Result<()> {
            use crate::dialect::SqlDialect;
            let d = $dialect;
            let sql = format!(
                "INSERT INTO ducklake_file_partition_value (data_file_id, table_id, partition_key_index, partition_value)
                 VALUES ({}, {}, {}, {})",
                d.ph(1),
                d.ph(2),
                d.ph(3),
                d.ph(4),
            );
            $block_on(|| async {
                sqlx::query(&sql)
                    .bind(data_file_id)
                    .bind(table_id)
                    .bind(partition_key_index as i64)
                    .bind(partition_value)
                    .execute(&self.pool)
                    .await?;
                Ok(())
            })
        }

        fn get_active_partition_columns(
            &self,
            table_id: i64,
        ) -> crate::Result<Vec<(String, i64, Option<String>)>> {
            use crate::dialect::SqlDialect;
            use sqlx::Row;
            let d = $dialect;
            let sql = format!(
                "SELECT c.column_name, pc.column_id, pc.transform
                 FROM ducklake_partition_info pi
                 JOIN ducklake_partition_column pc
                     ON pi.partition_id = pc.partition_id AND pi.table_id = pc.table_id
                 JOIN ducklake_column c ON pc.column_id = c.column_id AND c.end_snapshot IS NULL
                 WHERE pi.table_id = {} AND pi.end_snapshot IS NULL
                 ORDER BY pc.partition_key_index",
                d.ph(1),
            );
            $block_on(|| async {
                let rows = sqlx::query(&sql)
                    .bind(table_id)
                    .fetch_all(&self.pool)
                    .await?;

                let mut result = Vec::with_capacity(rows.len());
                for row in rows {
                    let name: String = row.try_get(0)?;
                    let col_id: i64 = row.try_get(1)?;
                    let transform: Option<String> = row.try_get(2)?;
                    result.push((name, col_id, transform));
                }
                Ok(result)
            })
        }
    };
}

pub(crate) use impl_writer_query_ops;

/// Generates MetadataWriter file operation implementations.
///
/// Methods: register_column_stats, register_data_file, end_table_files,
/// replace_table_files, register_dml_files, register_delete_file.
///
/// The `last_insert_id` parameter is an async closure `|tx: &mut Transaction| -> Result<i64>`
/// used for MySQL which doesn't support RETURNING. For SQLite/PG, pass a dummy.
macro_rules! impl_writer_file_ops {
    (
        $struct_name:ty,
        pool_type = $pool_type:ty,
        dialect = $dialect:expr,
        block_on = $block_on:path,
        last_insert_id = $last_id:expr
    ) => {
        fn register_column_stats(
            &self,
            data_file_id: i64,
            table_id: i64,
            stats: &[crate::metadata_writer::ColumnStatInfo],
        ) -> crate::Result<()> {
            use crate::dialect::SqlDialect;
            if stats.is_empty() {
                return Ok(());
            }
            let d = $dialect;
            let sql = format!(
                "INSERT INTO ducklake_file_column_stats
                 (data_file_id, table_id, column_id, null_count, min_value, max_value)
                 VALUES ({}, {}, {}, {}, {}, {})",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6),
            );
            $block_on(|| async {
                let mut tx = self.pool.begin().await?;
                for stat in stats {
                    sqlx::query(&sql)
                        .bind(data_file_id)
                        .bind(table_id)
                        .bind(stat.column_id)
                        .bind(stat.null_count)
                        .bind(&stat.min_value)
                        .bind(&stat.max_value)
                        .execute(&mut *tx)
                        .await?;
                }

                Self::recompute_table_column_stats(&mut tx, table_id).await?;

                tx.commit().await?;
                Ok(())
            })
        }

        fn register_data_file(
            &self,
            table_id: i64,
            snapshot_id: i64,
            file: &crate::metadata_writer::DataFileInfo,
        ) -> crate::Result<i64> {
            use crate::dialect::SqlDialect;
            use crate::error::DuckLakeError;
            use sqlx::Row;
            let d = $dialect;
            let stats_sql = format!(
                "SELECT next_row_id FROM ducklake_table_stats WHERE table_id = {}{}",
                d.ph(1),
                d.for_update(),
            );
            let insert_sql = if d.supports_returning() {
                format!(
                    "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, row_id_start, file_format, begin_snapshot)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, 'parquet', {}) RETURNING data_file_id",
                    d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8),
                )
            } else {
                format!(
                    "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, row_id_start, file_format, begin_snapshot)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, 'parquet', {})",
                    d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8),
                )
            };
            let update_stats_sql = format!(
                "UPDATE ducklake_table_stats
                 SET record_count = COALESCE(record_count, 0) + {},
                     next_row_id = {},
                     file_size_bytes = COALESCE(file_size_bytes, 0) + {}
                 WHERE table_id = {}",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4),
            );
            let insert_stats_sql = format!(
                "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
                 VALUES ({}, {}, {}, {})",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4),
            );
            let next_file_id_sql = format!(
                "UPDATE ducklake_snapshot SET next_file_id = COALESCE({}, 0) WHERE snapshot_id = {}",
                d.greatest(
                    &format!("(SELECT COALESCE(MAX(data_file_id), 0) + 1 FROM ducklake_data_file)"),
                    &format!("(SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM ducklake_delete_file)"),
                ),
                d.ph(1),
            );
            #[allow(unused_variables)]
            let last_id_fn = $last_id;
            $block_on(|| async {
                let mut tx = self.pool.begin().await?;

                // Get current next_row_id from table_stats
                let stats_row = sqlx::query(&stats_sql)
                    .bind(table_id)
                    .fetch_optional(&mut *tx)
                    .await?;
                let row_id_start: i64 = match stats_row {
                    Some(r) => r.try_get::<Option<i64>, _>(0)?.unwrap_or(0),
                    None => 0,
                };

                // Insert data file
                let data_file_id: i64 = if d.supports_returning() {
                    let row = sqlx::query(&insert_sql)
                        .bind(table_id)
                        .bind(&file.path)
                        .bind(file.path_is_relative)
                        .bind(file.file_size_bytes)
                        .bind(file.footer_size)
                        .bind(file.record_count)
                        .bind(row_id_start)
                        .bind(snapshot_id)
                        .fetch_one(&mut *tx)
                        .await?;
                    row.try_get(0)?
                } else {
                    sqlx::query(&insert_sql)
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
                    (last_id_fn)(&mut tx).await?
                };

                // Update ducklake_table_stats
                let new_next_row_id = row_id_start
                    .checked_add(file.record_count)
                    .ok_or_else(|| DuckLakeError::Internal("row_id overflow".into()))?;
                let updated = sqlx::query(&update_stats_sql)
                    .bind(file.record_count)
                    .bind(new_next_row_id)
                    .bind(file.file_size_bytes)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                if updated.rows_affected() == 0 {
                    sqlx::query(&insert_stats_sql)
                        .bind(table_id)
                        .bind(file.record_count)
                        .bind(new_next_row_id)
                        .bind(file.file_size_bytes)
                        .execute(&mut *tx)
                        .await?;
                }

                // Update snapshot's next_file_id
                sqlx::query(&next_file_id_sql)
                    .bind(snapshot_id)
                    .execute(&mut *tx)
                    .await?;

                tx.commit().await?;
                Ok(data_file_id)
            })
        }

        fn end_table_files(&self, table_id: i64, snapshot_id: i64) -> crate::Result<u64> {
            use crate::dialect::SqlDialect;
            let d = $dialect;
            let end_data_sql = format!(
                "UPDATE ducklake_data_file SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                d.ph(1), d.ph(2),
            );
            let end_delete_sql = format!(
                "UPDATE ducklake_delete_file SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                d.ph(1), d.ph(2),
            );
            let reset_stats_sql = format!(
                "UPDATE ducklake_table_stats SET record_count = 0, next_row_id = 0, file_size_bytes = 0 WHERE table_id = {}",
                d.ph(1),
            );
            $block_on(|| async {
                let mut tx = self.pool.begin().await?;
                let result = sqlx::query(&end_data_sql)
                    .bind(snapshot_id)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                sqlx::query(&end_delete_sql)
                    .bind(snapshot_id)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                sqlx::query(&reset_stats_sql)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                tx.commit().await?;
                Ok(result.rows_affected())
            })
        }

        fn replace_table_files(
            &self,
            table_id: i64,
            snapshot_id: i64,
            files: &[crate::metadata_writer::ReplaceFileEntry],
        ) -> crate::Result<Vec<i64>> {
            use crate::dialect::SqlDialect;
            use crate::error::DuckLakeError;
            use sqlx::Row;
            let d = $dialect;
            let end_data_sql = format!(
                "UPDATE ducklake_data_file SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                d.ph(1), d.ph(2),
            );
            let end_delete_sql = format!(
                "UPDATE ducklake_delete_file SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                d.ph(1), d.ph(2),
            );
            let insert_file_sql = if d.supports_returning() {
                format!(
                    "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, row_id_start, begin_snapshot)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, {}) RETURNING data_file_id",
                    d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8),
                )
            } else {
                format!(
                    "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, row_id_start, begin_snapshot)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
                    d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8),
                )
            };
            let insert_stats_query = format!(
                "INSERT INTO ducklake_file_column_stats (data_file_id, table_id, column_id, null_count, min_value, max_value)
                 VALUES ({}, {}, {}, {}, {}, {})",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6),
            );
            let insert_partition_sql = format!(
                "INSERT INTO ducklake_file_partition_value (data_file_id, table_id, partition_key_index, partition_value)
                 VALUES ({}, {}, {}, {})",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4),
            );
            let update_table_stats_sql = format!(
                "UPDATE ducklake_table_stats SET record_count = {}, next_row_id = {}, file_size_bytes = {} WHERE table_id = {}",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4),
            );
            let insert_table_stats_sql = format!(
                "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
                 VALUES ({}, {}, {}, {})",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4),
            );
            #[allow(unused_variables)]
            let last_id_fn = $last_id;
            $block_on(|| async {
                let mut tx = self.pool.begin().await?;

                // End all existing data files
                sqlx::query(&end_data_sql)
                    .bind(snapshot_id)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                // End all active delete files
                sqlx::query(&end_delete_sql)
                    .bind(snapshot_id)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                let mut ids = Vec::with_capacity(files.len());
                let mut cumulative_row_id: i64 = 0;
                for entry in files {
                    let path_is_relative = entry.file_info.path_is_relative;
                    // Register data file
                    let data_file_id: i64 = if d.supports_returning() {
                        let row = sqlx::query(&insert_file_sql)
                            .bind(table_id)
                            .bind(&entry.file_info.path)
                            .bind(path_is_relative)
                            .bind(entry.file_info.file_size_bytes)
                            .bind(entry.file_info.footer_size)
                            .bind(entry.file_info.record_count)
                            .bind(cumulative_row_id)
                            .bind(snapshot_id)
                            .fetch_one(&mut *tx)
                            .await?;
                        row.try_get(0)?
                    } else {
                        sqlx::query(&insert_file_sql)
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
                        (last_id_fn)(&mut tx).await?
                    };

                    // Register column stats
                    for stat in &entry.file_info.column_stats {
                        sqlx::query(&insert_stats_query)
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
                        sqlx::query(&insert_partition_sql)
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
                let total_file_size: i64 = files.iter().try_fold(0i64, |acc, f| {
                    acc.checked_add(f.file_info.file_size_bytes).ok_or_else(|| {
                        DuckLakeError::Internal(
                            "file_size_bytes sum overflow in replace_table_files".into(),
                        )
                    })
                })?;
                let updated = sqlx::query(&update_table_stats_sql)
                    .bind(total_record_count)
                    .bind(total_record_count)
                    .bind(total_file_size)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                if updated.rows_affected() == 0 {
                    sqlx::query(&insert_table_stats_sql)
                        .bind(table_id)
                        .bind(total_record_count)
                        .bind(total_record_count)
                        .bind(total_file_size)
                        .execute(&mut *tx)
                        .await?;
                }

                // Recompute table column stats from new compacted files
                Self::recompute_table_column_stats(&mut tx, table_id).await?;

                tx.commit().await?;
                Ok(ids)
            })
        }

        fn register_dml_files(
            &self,
            table_id: i64,
            snapshot_id: i64,
            delete_files: &[crate::metadata_writer::DeleteFileInfo],
            data_files: &[crate::metadata_writer::DataFileInfo],
        ) -> crate::Result<()> {
            use crate::dialect::SqlDialect;
            use crate::error::DuckLakeError;
            use sqlx::Row;
            if delete_files.is_empty() && data_files.is_empty() {
                return Ok(());
            }
            let d = $dialect;
            let old_delete_sql = format!(
                "SELECT COALESCE(delete_count, 0) FROM ducklake_delete_file
                 WHERE data_file_id = {} AND table_id = {} AND end_snapshot IS NULL",
                d.ph(1), d.ph(2),
            );
            let end_delete_sql = format!(
                "UPDATE ducklake_delete_file SET end_snapshot = {}
                 WHERE data_file_id = {} AND table_id = {} AND end_snapshot IS NULL",
                d.ph(1), d.ph(2), d.ph(3),
            );
            let insert_delete_sql = format!(
                "INSERT INTO ducklake_delete_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, delete_count, format, begin_snapshot)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, 'parquet', {})",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8),
            );
            let decrement_sql = format!(
                "UPDATE ducklake_table_stats SET record_count = {} WHERE table_id = {}",
                d.clamp_zero(&format!("COALESCE(record_count, 0) - {}", d.ph(1))),
                d.ph(2),
            );
            let stats_sql = format!(
                "SELECT next_row_id FROM ducklake_table_stats WHERE table_id = {}{}",
                d.ph(1),
                d.for_update(),
            );
            let insert_data_file_sql = if d.supports_returning() {
                format!(
                    "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, row_id_start, begin_snapshot)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, {}) RETURNING data_file_id",
                    d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8),
                )
            } else {
                format!(
                    "INSERT INTO ducklake_data_file (table_id, path, path_is_relative, file_size_bytes, footer_size, record_count, row_id_start, begin_snapshot)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
                    d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8),
                )
            };
            let update_stats_sql = format!(
                "UPDATE ducklake_table_stats
                 SET record_count = COALESCE(record_count, 0) + {},
                     next_row_id = {},
                     file_size_bytes = COALESCE(file_size_bytes, 0) + {}
                 WHERE table_id = {}",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4),
            );
            let insert_stats_sql = format!(
                "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
                 VALUES ({}, {}, {}, {})",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4),
            );
            let insert_col_stats_sql = format!(
                "INSERT INTO ducklake_file_column_stats
                 (data_file_id, table_id, column_id, null_count, min_value, max_value)
                 VALUES ({}, {}, {}, {}, {}, {})",
                d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6),
            );
            let next_file_id_sql = format!(
                "UPDATE ducklake_snapshot SET next_file_id = COALESCE({}, 0) WHERE snapshot_id = {}",
                d.greatest(
                    &format!("(SELECT COALESCE(MAX(data_file_id), 0) + 1 FROM ducklake_data_file)"),
                    &format!("(SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM ducklake_delete_file)"),
                ),
                d.ph(1),
            );
            #[allow(unused_variables)]
            let last_id_fn = $last_id;
            $block_on(|| async {
                let mut tx = self.pool.begin().await?;

                // Track net new deletions to decrement record_count
                let mut total_net_new_deletions: i64 = 0;

                for file in delete_files {
                    // Get the old delete_count before ending the existing delete file
                    let old_row = sqlx::query(&old_delete_sql)
                        .bind(file.data_file_id)
                        .bind(table_id)
                        .fetch_optional(&mut *tx)
                        .await?;
                    let old_delete_count: i64 = match old_row {
                        Some(r) => r.try_get(0)?,
                        None => 0,
                    };

                    // End any existing active delete file for this data file
                    sqlx::query(&end_delete_sql)
                        .bind(snapshot_id)
                        .bind(file.data_file_id)
                        .bind(table_id)
                        .execute(&mut *tx)
                        .await?;

                    // Insert the new delete file
                    sqlx::query(&insert_delete_sql)
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

                    let net_delta =
                        file.delete_count
                            .checked_sub(old_delete_count)
                            .ok_or_else(|| {
                                DuckLakeError::Internal(format!(
                                    "delete_count underflow: new {} < old {}",
                                    file.delete_count, old_delete_count
                                ))
                            })?;
                    total_net_new_deletions = total_net_new_deletions
                        .checked_add(net_delta)
                        .ok_or_else(|| {
                            DuckLakeError::Internal(
                                "total_net_new_deletions overflow in register_dml_files".into(),
                            )
                        })?;
                }

                // Decrement record_count (clamped to 0)
                if total_net_new_deletions > 0 {
                    sqlx::query(&decrement_sql)
                        .bind(total_net_new_deletions)
                        .bind(table_id)
                        .execute(&mut *tx)
                        .await?;
                }

                // For each new data file, set row_id_start and update table_stats
                let mut has_column_stats = false;
                for file in data_files {
                    let stats_row = sqlx::query(&stats_sql)
                        .bind(table_id)
                        .fetch_optional(&mut *tx)
                        .await?;
                    let row_id_start: i64 = match stats_row {
                        Some(r) => r.try_get::<Option<i64>, _>(0)?.unwrap_or(0),
                        None => 0,
                    };

                    let data_file_id: i64 = if d.supports_returning() {
                        let row = sqlx::query(&insert_data_file_sql)
                            .bind(table_id)
                            .bind(&file.path)
                            .bind(file.path_is_relative)
                            .bind(file.file_size_bytes)
                            .bind(file.footer_size)
                            .bind(file.record_count)
                            .bind(row_id_start)
                            .bind(snapshot_id)
                            .fetch_one(&mut *tx)
                            .await?;
                        row.try_get(0)?
                    } else {
                        sqlx::query(&insert_data_file_sql)
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
                        (last_id_fn)(&mut tx).await?
                    };

                    // Update ducklake_table_stats
                    let new_next_row_id = row_id_start
                        .checked_add(file.record_count)
                        .ok_or_else(|| DuckLakeError::Internal("row_id overflow".into()))?;
                    let updated = sqlx::query(&update_stats_sql)
                        .bind(file.record_count)
                        .bind(new_next_row_id)
                        .bind(file.file_size_bytes)
                        .bind(table_id)
                        .execute(&mut *tx)
                        .await?;

                    if updated.rows_affected() == 0 {
                        sqlx::query(&insert_stats_sql)
                            .bind(table_id)
                            .bind(file.record_count)
                            .bind(new_next_row_id)
                            .bind(file.file_size_bytes)
                            .execute(&mut *tx)
                            .await?;
                    }

                    // Register per-file column stats
                    if !file.column_stats.is_empty() {
                        has_column_stats = true;
                        for stat in &file.column_stats {
                            sqlx::query(&insert_col_stats_sql)
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

                // Recompute table-level column stats
                if has_column_stats {
                    Self::recompute_table_column_stats(&mut tx, table_id).await?;
                }

                // Update snapshot's next_file_id
                sqlx::query(&next_file_id_sql)
                    .bind(snapshot_id)
                    .execute(&mut *tx)
                    .await?;

                tx.commit().await?;
                Ok(())
            })
        }

        fn register_delete_file(
            &self,
            table_id: i64,
            snapshot_id: i64,
            file: &crate::metadata_writer::DeleteFileInfo,
        ) -> crate::Result<i64> {
            use crate::dialect::SqlDialect;
            use sqlx::Row;
            let d = $dialect;
            let end_sql = format!(
                "UPDATE ducklake_delete_file SET end_snapshot = {}
                 WHERE data_file_id = {} AND table_id = {} AND end_snapshot IS NULL",
                d.ph(1), d.ph(2), d.ph(3),
            );
            let insert_sql = if d.supports_returning() {
                format!(
                    "INSERT INTO ducklake_delete_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, delete_count, format, begin_snapshot)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, 'parquet', {}) RETURNING delete_file_id",
                    d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8),
                )
            } else {
                format!(
                    "INSERT INTO ducklake_delete_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, delete_count, format, begin_snapshot)
                     VALUES ({}, {}, {}, {}, {}, {}, {}, 'parquet', {})",
                    d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8),
                )
            };
            #[allow(unused_variables)]
            let last_id_fn = $last_id;
            $block_on(|| async {
                let mut tx = self.pool.begin().await?;

                // End any existing active delete file for this data file
                sqlx::query(&end_sql)
                    .bind(snapshot_id)
                    .bind(file.data_file_id)
                    .bind(table_id)
                    .execute(&mut *tx)
                    .await?;

                // Insert the new delete file
                let delete_file_id: i64 = if d.supports_returning() {
                    let row = sqlx::query(&insert_sql)
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
                    row.try_get(0)?
                } else {
                    sqlx::query(&insert_sql)
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
                    (last_id_fn)(&mut tx).await?
                };

                tx.commit().await?;
                Ok(delete_file_id)
            })
        }
    };
}

pub(crate) use impl_writer_file_ops;

/// Generates DDL-related `MetadataWriter` methods (create_view, drop_view, rename_view,
/// alter_table, rename_table, set_table_comment, set_column_comment).
///
/// Parameters:
/// - `$struct_name`: The metadata writer struct type
/// - `pool_type`: The sqlx pool type (SqlitePool, PgPool, MySqlPool)
/// - `dialect`: SqlDialect implementation expression
/// - `block_on`: blocking executor (block_on_with_retry or block_on_no_retry)
/// - `last_insert_id`: async closure to get last inserted ID (MySQL)
/// - `column_order_type`: Rust type for column_order column (i64 for SQLite/MySQL, i32 for PG)
macro_rules! impl_writer_ddl_ops {
    (
        $struct_name:ty,
        pool_type = $pool_type:ty,
        dialect = $dialect:expr,
        block_on = $block_on:path,
        last_insert_id = $last_id:expr,
        column_order_type = $co_type:ty
    ) => {
            fn create_view(&self, schema_id: i64, view_name: &str, sql: &str) -> Result<(i64, i64)> {
                let pool = &self.pool;
                $block_on(|| async {
                    use crate::dialect::SqlDialect;
                    use sqlx::Row;
                    let d = $dialect;
                    let mut tx = pool.begin().await?;

                    // Create DDL snapshot with schema_version increment (F-012)
                    let snapshot_id: i64 = {
                        let prev_sv_row = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                            .fetch_one(&mut *tx).await?;
                        let new_sv: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

                        let sid: i64 = if d.supports_returning() {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {}) RETURNING snapshot_id",
                                d.now(), d.ph(1)
                            );
                            let row = sqlx::query(&ins).bind(new_sv).fetch_one(&mut *tx).await?;
                            row.try_get(0)?
                        } else {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {})",
                                d.now(), d.ph(1)
                            );
                            sqlx::query(&ins).bind(new_sv).execute(&mut *tx).await?;
                            let last_id_fn = $last_id;
                            (last_id_fn)(&mut tx).await?
                        };

                        let sv_sql = format!(
                            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ({}, {})",
                            d.ph(1), d.ph(2)
                        );
                        sqlx::query(&sv_sql).bind(sid).bind(new_sv).execute(&mut *tx).await?;
                        sid
                    };

                    // Get next view_id
                    let view_id = <$struct_name>::next_entity_id(&mut tx, "view_id", None).await?;

                    // Generate UUID (F-026) and insert view row
                    let view_uuid = uuid::Uuid::new_v4().to_string();
                    let view_ins = format!(
                        "INSERT INTO ducklake_view (view_id, view_uuid, schema_id, view_name, {}, begin_snapshot) VALUES ({}, {}, {}, {}, {}, {})",
                        d.col("sql"),
                        d.ph(1), d.uuid_ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6)
                    );
                    sqlx::query(&view_ins)
                        .bind(view_id).bind(&view_uuid).bind(schema_id)
                        .bind(view_name).bind(sql).bind(snapshot_id)
                        .execute(&mut *tx).await?;

                    // Record changes_made (F-027)
                    let schema_row = sqlx::query(&format!(
                        "SELECT schema_name FROM ducklake_schema WHERE schema_id = {} AND end_snapshot IS NULL",
                        d.ph(1)
                    ))
                    .bind(schema_id)
                    .fetch_optional(&mut *tx).await?;
                    let schema_name = schema_row
                        .map(|r| r.try_get::<String, _>(0).unwrap_or_default())
                        .unwrap_or_default();

                    let changes_sql = format!(
                        "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                        d.ph(1), d.ph(2), d.upsert("snapshot_id", &["changes_made"])
                    );
                    sqlx::query(&changes_sql)
                        .bind(snapshot_id)
                        .bind(format!("created_view:\"{}\".\"{}\"",
                            schema_name.replace('"', "\"\""),
                            view_name.replace('"', "\"\"")))
                        .execute(&mut *tx).await?;

                    tx.commit().await?;
                    Ok((view_id, snapshot_id))
                })
            }

            fn drop_view(&self, view_id: i64) -> Result<i64> {
                let pool = &self.pool;
                $block_on(|| async {
                    use crate::dialect::SqlDialect;
                    use sqlx::Row;
                    let d = $dialect;
                    let mut tx = pool.begin().await?;

                    // R4-S-014: Validate view exists and is active
                    let exists = sqlx::query(&format!(
                        "SELECT COUNT(*) FROM ducklake_view WHERE view_id = {} AND end_snapshot IS NULL",
                        d.ph(1)
                    ))
                    .bind(view_id)
                    .fetch_one(&mut *tx).await?;
                    if exists.try_get::<i64, _>(0)? == 0 {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "View with id {} not found or already dropped", view_id
                        )));
                    }

                    // Create DDL snapshot
                    let snapshot_id: i64 = {
                        let prev_sv_row = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                            .fetch_one(&mut *tx).await?;
                        let new_sv: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

                        let sid: i64 = if d.supports_returning() {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {}) RETURNING snapshot_id",
                                d.now(), d.ph(1)
                            );
                            let row = sqlx::query(&ins).bind(new_sv).fetch_one(&mut *tx).await?;
                            row.try_get(0)?
                        } else {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {})",
                                d.now(), d.ph(1)
                            );
                            sqlx::query(&ins).bind(new_sv).execute(&mut *tx).await?;
                            let last_id_fn = $last_id;
                            (last_id_fn)(&mut tx).await?
                        };

                        let sv_sql = format!(
                            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ({}, {})",
                            d.ph(1), d.ph(2)
                        );
                        sqlx::query(&sv_sql).bind(sid).bind(new_sv).execute(&mut *tx).await?;
                        sid
                    };

                    // End the view
                    let end_sql = format!(
                        "UPDATE ducklake_view SET end_snapshot = {} WHERE view_id = {} AND end_snapshot IS NULL",
                        d.ph(1), d.ph(2)
                    );
                    sqlx::query(&end_sql).bind(snapshot_id).bind(view_id)
                        .execute(&mut *tx).await?;

                    // Record changes
                    let changes_sql = format!(
                        "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                        d.ph(1), d.ph(2), d.upsert("snapshot_id", &["changes_made"])
                    );
                    sqlx::query(&changes_sql)
                        .bind(snapshot_id)
                        .bind(format!("dropped_view:{}", view_id))
                        .execute(&mut *tx).await?;

                    tx.commit().await?;
                    Ok(snapshot_id)
                })
            }

            fn rename_view(&self, view_id: i64, new_name: &str) -> Result<i64> {
                let pool = &self.pool;
                $block_on(|| async {
                    use crate::dialect::SqlDialect;
                    use sqlx::Row;
                    let d = $dialect;
                    let mut tx = pool.begin().await?;

                    // Fetch current active view row
                    let view_row = sqlx::query(&format!(
                        "SELECT schema_id, {}, {}, dialect, column_aliases FROM ducklake_view WHERE view_id = {} AND end_snapshot IS NULL",
                        d.read_uuid("view_uuid"), d.col("sql"), d.ph(1)
                    ))
                    .bind(view_id)
                    .fetch_optional(&mut *tx).await?;

                    let Some(view_row) = view_row else {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "View with id {} not found or already dropped", view_id
                        )));
                    };

                    let schema_id: i64 = view_row.try_get(0)?;
                    let view_uuid: Option<String> = view_row.try_get(1)?;
                    let sql_text: String = view_row.try_get(2)?;
                    let dialect_val: Option<String> = view_row.try_get(3)?;
                    let column_aliases: Option<String> = view_row.try_get(4)?;

                    // R4-S-015: Check for duplicate active view name in same schema
                    let dup = sqlx::query(&format!(
                        "SELECT COUNT(*) FROM ducklake_view WHERE schema_id = {} AND view_name = {} AND end_snapshot IS NULL",
                        d.ph(1), d.ph(2)
                    ))
                    .bind(schema_id).bind(new_name)
                    .fetch_one(&mut *tx).await?;
                    if dup.try_get::<i64, _>(0)? > 0 {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "A view named '{}' already exists in schema {}", new_name, schema_id
                        )));
                    }

                    // Create DDL snapshot
                    let snapshot_id: i64 = {
                        let prev_sv_row = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                            .fetch_one(&mut *tx).await?;
                        let new_sv: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

                        let sid: i64 = if d.supports_returning() {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {}) RETURNING snapshot_id",
                                d.now(), d.ph(1)
                            );
                            let row = sqlx::query(&ins).bind(new_sv).fetch_one(&mut *tx).await?;
                            row.try_get(0)?
                        } else {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {})",
                                d.now(), d.ph(1)
                            );
                            sqlx::query(&ins).bind(new_sv).execute(&mut *tx).await?;
                            let last_id_fn = $last_id;
                            (last_id_fn)(&mut tx).await?
                        };

                        let sv_sql = format!(
                            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ({}, {})",
                            d.ph(1), d.ph(2)
                        );
                        sqlx::query(&sv_sql).bind(sid).bind(new_sv).execute(&mut *tx).await?;
                        sid
                    };

                    // End existing view row
                    let end_sql = format!(
                        "UPDATE ducklake_view SET end_snapshot = {} WHERE view_id = {} AND end_snapshot IS NULL",
                        d.ph(1), d.ph(2)
                    );
                    sqlx::query(&end_sql).bind(snapshot_id).bind(view_id)
                        .execute(&mut *tx).await?;

                    // Insert new view row with updated name
                    let view_ins = format!(
                        "INSERT INTO ducklake_view (view_id, view_uuid, schema_id, view_name, dialect, {}, column_aliases, begin_snapshot) VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
                        d.col("sql"),
                        d.ph(1), d.uuid_ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7), d.ph(8)
                    );
                    sqlx::query(&view_ins)
                        .bind(view_id).bind(&view_uuid).bind(schema_id)
                        .bind(new_name).bind(&dialect_val).bind(&sql_text)
                        .bind(&column_aliases).bind(snapshot_id)
                        .execute(&mut *tx).await?;

                    // Record change for conflict detection
                    let ct_sql = format!(
                        "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id) VALUES ({}, 'ALTER_VIEW', {})",
                        d.ph(1), d.ph(2)
                    );
                    sqlx::query(&ct_sql).bind(snapshot_id).bind(view_id)
                        .execute(&mut *tx).await?;

                    // Record snapshot changes
                    let changes_sql = format!(
                        "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                        d.ph(1), d.ph(2), d.upsert("snapshot_id", &["changes_made"])
                    );
                    sqlx::query(&changes_sql)
                        .bind(snapshot_id)
                        .bind(format!("altered_view:{}", view_id))
                        .execute(&mut *tx).await?;

                    tx.commit().await?;
                    Ok(snapshot_id)
                })
            }

            fn alter_table(&self, table_id: i64, op: &AlterTableOp) -> Result<i64> {
                let pool = &self.pool;
                $block_on(|| async {
                    use crate::dialect::SqlDialect;
                    use sqlx::Row;
                    let d = $dialect;
                    let mut tx = pool.begin().await?;

                    // Get active columns for validation
                    let col_rows = sqlx::query(&format!(
                        "SELECT column_id, column_name, column_type, column_order, nulls_allowed, \
                                initial_default, default_value, parent_column, default_value_type, default_value_dialect \
                         FROM ducklake_column WHERE table_id = {} AND end_snapshot IS NULL ORDER BY column_order",
                        d.ph(1)
                    ))
                    .bind(table_id)
                    .fetch_all(&mut *tx).await?;

                    let columns: Vec<ActiveColumnInfo> = col_rows
                        .iter()
                        .map(|r| {
                            Ok(ActiveColumnInfo {
                                column_id: r.try_get(0)?,
                                column_name: r.try_get(1)?,
                                column_type: r.try_get(2)?,
                                column_order: r.try_get::<$co_type, _>(3)? as i64,
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

                    // Create DDL snapshot
                    let snapshot_id: i64 = {
                        let prev_sv_row = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                            .fetch_one(&mut *tx).await?;
                        let new_sv: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

                        let sid: i64 = if d.supports_returning() {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {}) RETURNING snapshot_id",
                                d.now(), d.ph(1)
                            );
                            let row = sqlx::query(&ins).bind(new_sv).fetch_one(&mut *tx).await?;
                            row.try_get(0)?
                        } else {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {})",
                                d.now(), d.ph(1)
                            );
                            sqlx::query(&ins).bind(new_sv).execute(&mut *tx).await?;
                            let last_id_fn = $last_id;
                            (last_id_fn)(&mut tx).await?
                        };

                        let sv_sql = format!(
                            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ({}, {})",
                            d.ph(1), d.ph(2)
                        );
                        sqlx::query(&sv_sql).bind(sid).bind(new_sv).execute(&mut *tx).await?;
                        sid
                    };

                    match action {
                        AlterTableAction::InsertColumn {
                            column_name,
                            column_type,
                            column_order,
                            is_nullable,
                        } => {
                            let next_column_id = <$struct_name>::next_entity_id(&mut tx, "column_id", Some(table_id)).await?;

                            // Extract ColumnDef fields from AddColumn op
                            let (initial_default, default_value, parent_column, default_value_type, default_value_dialect) =
                                if let AlterTableOp::AddColumn { column } = op {
                                    (&column.initial_default, &column.default_value, column.parent_column,
                                     &column.default_value_type, &column.default_value_dialect)
                                } else {
                                    (&None, &None, None, &None, &None)
                                };

                            let col_ins = format!(
                                "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, \
                                 nulls_allowed, initial_default, default_value, parent_column, default_value_type, \
                                 default_value_dialect, begin_snapshot) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                                d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6),
                                d.ph(7), d.ph(8), d.ph(9), d.ph(10), d.ph(11), d.ph(12)
                            );
                            sqlx::query(&col_ins)
                                .bind(next_column_id).bind(table_id).bind(&column_name)
                                .bind(&column_type).bind(column_order).bind(is_nullable)
                                .bind(initial_default).bind(default_value).bind(parent_column)
                                .bind(default_value_type).bind(default_value_dialect).bind(snapshot_id)
                                .execute(&mut *tx).await?;

                            // Initialize table-level column stats (R3F-001)
                            let stats_sql = format!(
                                "INSERT INTO ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan) \
                                 VALUES ({}, {}, {}, NULL)",
                                d.ph(1), d.ph(2), d.bool_lit(true)
                            );
                            sqlx::query(&stats_sql)
                                .bind(table_id).bind(next_column_id)
                                .execute(&mut *tx).await?;
                        },
                        AlterTableAction::EndColumn { column_id } => {
                            let end_sql = format!(
                                "UPDATE ducklake_column SET end_snapshot = {} WHERE column_id = {} AND end_snapshot IS NULL",
                                d.ph(1), d.ph(2)
                            );
                            sqlx::query(&end_sql).bind(snapshot_id).bind(column_id)
                                .execute(&mut *tx).await?;
                        },
                        AlterTableAction::ReplaceColumn {
                            end_column_id, column_name, column_type, column_order,
                            is_nullable, initial_default, default_value, parent_column,
                            default_value_type, default_value_dialect,
                        } => {
                            // End existing column row
                            let end_sql = format!(
                                "UPDATE ducklake_column SET end_snapshot = {} WHERE column_id = {} AND end_snapshot IS NULL",
                                d.ph(1), d.ph(2)
                            );
                            sqlx::query(&end_sql).bind(snapshot_id).bind(end_column_id)
                                .execute(&mut *tx).await?;

                            // Reuse same column_id (critical for Parquet field_id mapping)
                            let col_ins = format!(
                                "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, \
                                 nulls_allowed, initial_default, default_value, parent_column, default_value_type, \
                                 default_value_dialect, begin_snapshot) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                                d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6),
                                d.ph(7), d.ph(8), d.ph(9), d.ph(10), d.ph(11), d.ph(12)
                            );
                            sqlx::query(&col_ins)
                                .bind(end_column_id).bind(table_id).bind(&column_name)
                                .bind(&column_type).bind(column_order).bind(is_nullable)
                                .bind(&initial_default).bind(&default_value).bind(parent_column)
                                .bind(&default_value_type).bind(&default_value_dialect).bind(snapshot_id)
                                .execute(&mut *tx).await?;
                        },
                        AlterTableAction::SetPartitionedBy { partition_columns } => {
                            // End any existing partition info
                            let end_part = format!(
                                "UPDATE ducklake_partition_info SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                                d.ph(1), d.ph(2)
                            );
                            sqlx::query(&end_part).bind(snapshot_id).bind(table_id)
                                .execute(&mut *tx).await?;

                            // Create new partition_info entry
                            let partition_id = <$struct_name>::next_entity_id(&mut tx, "partition_id", None).await?;

                            let part_ins = format!(
                                "INSERT INTO ducklake_partition_info (partition_id, table_id, begin_snapshot) VALUES ({}, {}, {})",
                                d.ph(1), d.ph(2), d.ph(3)
                            );
                            sqlx::query(&part_ins).bind(partition_id).bind(table_id).bind(snapshot_id)
                                .execute(&mut *tx).await?;

                            // Create partition_column entries
                            for (key_index, (column_id, _column_name, transform)) in partition_columns.iter().enumerate() {
                                let pc_ins = format!(
                                    "INSERT INTO ducklake_partition_column (partition_id, table_id, partition_key_index, column_id, transform) \
                                     VALUES ({}, {}, {}, {}, {})",
                                    d.ph(1), d.ph(2), d.ph(3), d.ph(4), d.ph(5)
                                );
                                sqlx::query(&pc_ins)
                                    .bind(partition_id).bind(table_id).bind(key_index as i64)
                                    .bind(column_id).bind(transform.as_deref().unwrap_or("identity"))
                                    .execute(&mut *tx).await?;
                            }
                        },
                    }

                    // Record change for conflict detection
                    let ct_sql = format!(
                        "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id) VALUES ({}, 'ALTER_TABLE', {})",
                        d.ph(1), d.ph(2)
                    );
                    sqlx::query(&ct_sql).bind(snapshot_id).bind(table_id)
                        .execute(&mut *tx).await?;

                    // Record snapshot changes
                    let changes_sql = format!(
                        "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                        d.ph(1), d.ph(2), d.upsert("snapshot_id", &["changes_made"])
                    );
                    sqlx::query(&changes_sql)
                        .bind(snapshot_id)
                        .bind(format!("altered_table:{}", table_id))
                        .execute(&mut *tx).await?;

                    tx.commit().await?;
                    Ok(snapshot_id)
                })
            }

            fn rename_table(&self, table_id: i64, new_name: &str) -> Result<i64> {
                let pool = &self.pool;
                $block_on(|| async {
                    use crate::dialect::SqlDialect;
                    use sqlx::Row;
                    let d = $dialect;
                    let mut tx = pool.begin().await?;

                    // Fetch current active table row
                    let table_row = sqlx::query(&format!(
                        "SELECT schema_id, {}, path, path_is_relative FROM ducklake_table WHERE table_id = {} AND end_snapshot IS NULL",
                        d.read_uuid("table_uuid"), d.ph(1)
                    ))
                    .bind(table_id)
                    .fetch_optional(&mut *tx).await?;

                    let Some(table_row) = table_row else {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "Table with id {} not found or already dropped", table_id
                        )));
                    };

                    let schema_id: i64 = table_row.try_get(0)?;
                    let table_uuid: Option<String> = table_row.try_get(1)?;
                    let path: String = table_row.try_get(2)?;
                    let path_is_relative: bool = table_row.try_get(3)?;

                    // R4-S-015: Check for duplicate active table name in same schema
                    let dup = sqlx::query(&format!(
                        "SELECT COUNT(*) FROM ducklake_table WHERE schema_id = {} AND table_name = {} AND end_snapshot IS NULL",
                        d.ph(1), d.ph(2)
                    ))
                    .bind(schema_id).bind(new_name)
                    .fetch_one(&mut *tx).await?;
                    if dup.try_get::<i64, _>(0)? > 0 {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "A table named '{}' already exists in schema {}", new_name, schema_id
                        )));
                    }

                    // Create DDL snapshot
                    let snapshot_id: i64 = {
                        let prev_sv_row = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                            .fetch_one(&mut *tx).await?;
                        let new_sv: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

                        let sid: i64 = if d.supports_returning() {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {}) RETURNING snapshot_id",
                                d.now(), d.ph(1)
                            );
                            let row = sqlx::query(&ins).bind(new_sv).fetch_one(&mut *tx).await?;
                            row.try_get(0)?
                        } else {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {})",
                                d.now(), d.ph(1)
                            );
                            sqlx::query(&ins).bind(new_sv).execute(&mut *tx).await?;
                            let last_id_fn = $last_id;
                            (last_id_fn)(&mut tx).await?
                        };

                        let sv_sql = format!(
                            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ({}, {})",
                            d.ph(1), d.ph(2)
                        );
                        sqlx::query(&sv_sql).bind(sid).bind(new_sv).execute(&mut *tx).await?;
                        sid
                    };

                    // End existing table row
                    let end_sql = format!(
                        "UPDATE ducklake_table SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                        d.ph(1), d.ph(2)
                    );
                    sqlx::query(&end_sql).bind(snapshot_id).bind(table_id)
                        .execute(&mut *tx).await?;

                    // Insert new table row with updated name
                    let table_ins = format!(
                        "INSERT INTO ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot) \
                         VALUES ({}, {}, {}, {}, {}, {}, {})",
                        d.ph(1), d.uuid_ph(2), d.ph(3), d.ph(4), d.ph(5), d.ph(6), d.ph(7)
                    );
                    sqlx::query(&table_ins)
                        .bind(table_id).bind(&table_uuid).bind(schema_id)
                        .bind(new_name).bind(&path).bind(path_is_relative).bind(snapshot_id)
                        .execute(&mut *tx).await?;

                    // Record change for conflict detection
                    let ct_sql = format!(
                        "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id) VALUES ({}, 'ALTER_TABLE', {})",
                        d.ph(1), d.ph(2)
                    );
                    sqlx::query(&ct_sql).bind(snapshot_id).bind(table_id)
                        .execute(&mut *tx).await?;

                    // Record snapshot changes
                    let changes_sql = format!(
                        "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                        d.ph(1), d.ph(2), d.upsert("snapshot_id", &["changes_made"])
                    );
                    sqlx::query(&changes_sql)
                        .bind(snapshot_id)
                        .bind(format!("altered_table:{}", table_id))
                        .execute(&mut *tx).await?;

                    tx.commit().await?;
                    Ok(snapshot_id)
                })
            }

            fn set_table_comment(&self, table_id: i64, comment: &str) -> Result<i64> {
                let pool = &self.pool;
                $block_on(|| async {
                    use crate::dialect::SqlDialect;
                    use sqlx::Row;
                    let d = $dialect;
                    let mut tx = pool.begin().await?;

                    // Create DDL snapshot
                    let snapshot_id: i64 = {
                        let prev_sv_row = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                            .fetch_one(&mut *tx).await?;
                        let new_sv: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

                        let sid: i64 = if d.supports_returning() {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {}) RETURNING snapshot_id",
                                d.now(), d.ph(1)
                            );
                            let row = sqlx::query(&ins).bind(new_sv).fetch_one(&mut *tx).await?;
                            row.try_get(0)?
                        } else {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {})",
                                d.now(), d.ph(1)
                            );
                            sqlx::query(&ins).bind(new_sv).execute(&mut *tx).await?;
                            let last_id_fn = $last_id;
                            (last_id_fn)(&mut tx).await?
                        };

                        let sv_sql = format!(
                            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ({}, {})",
                            d.ph(1), d.ph(2)
                        );
                        sqlx::query(&sv_sql).bind(sid).bind(new_sv).execute(&mut *tx).await?;
                        sid
                    };

                    // End any existing comment tag for this table
                    let end_tag = format!(
                        "UPDATE ducklake_tag SET end_snapshot = {} WHERE object_id = {} AND {} = 'comment' AND end_snapshot IS NULL",
                        d.ph(1), d.ph(2), d.col("key")
                    );
                    sqlx::query(&end_tag).bind(snapshot_id).bind(table_id)
                        .execute(&mut *tx).await?;

                    // Insert new comment tag
                    let ins_tag = format!(
                        "INSERT INTO ducklake_tag (object_id, begin_snapshot, {}, value) VALUES ({}, {}, 'comment', {})",
                        d.col("key"), d.ph(1), d.ph(2), d.ph(3)
                    );
                    sqlx::query(&ins_tag).bind(table_id).bind(snapshot_id).bind(comment)
                        .execute(&mut *tx).await?;

                    // Record change for conflict detection
                    let ct_sql = format!(
                        "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id) VALUES ({}, 'ALTER_TABLE', {})",
                        d.ph(1), d.ph(2)
                    );
                    sqlx::query(&ct_sql).bind(snapshot_id).bind(table_id)
                        .execute(&mut *tx).await?;

                    // Record snapshot changes
                    let changes_sql = format!(
                        "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                        d.ph(1), d.ph(2), d.upsert("snapshot_id", &["changes_made"])
                    );
                    sqlx::query(&changes_sql)
                        .bind(snapshot_id)
                        .bind(format!("altered_table:{}", table_id))
                        .execute(&mut *tx).await?;

                    tx.commit().await?;
                    Ok(snapshot_id)
                })
            }

            fn set_column_comment(&self, table_id: i64, column_name: &str, comment: &str) -> Result<i64> {
                let pool = &self.pool;
                $block_on(|| async {
                    use crate::dialect::SqlDialect;
                    use sqlx::Row;
                    let d = $dialect;
                    let mut tx = pool.begin().await?;

                    // Look up the column_id for the named column
                    let col_row = sqlx::query(&format!(
                        "SELECT column_id FROM ducklake_column WHERE table_id = {} AND column_name = {} AND end_snapshot IS NULL",
                        d.ph(1), d.ph(2)
                    ))
                    .bind(table_id).bind(column_name)
                    .fetch_optional(&mut *tx).await?;

                    let Some(col_row) = col_row else {
                        return Err(crate::error::DuckLakeError::InvalidConfig(format!(
                            "Column '{}' not found in table", column_name
                        )));
                    };
                    let column_id: i64 = col_row.try_get(0)?;

                    // Create DDL snapshot
                    let snapshot_id: i64 = {
                        let prev_sv_row = sqlx::query("SELECT COALESCE(MAX(schema_version), 0) FROM ducklake_snapshot")
                            .fetch_one(&mut *tx).await?;
                        let new_sv: i64 = prev_sv_row.try_get::<i64, _>(0)? + 1;

                        let sid: i64 = if d.supports_returning() {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {}) RETURNING snapshot_id",
                                d.now(), d.ph(1)
                            );
                            let row = sqlx::query(&ins).bind(new_sv).fetch_one(&mut *tx).await?;
                            row.try_get(0)?
                        } else {
                            let ins = format!(
                                "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {})",
                                d.now(), d.ph(1)
                            );
                            sqlx::query(&ins).bind(new_sv).execute(&mut *tx).await?;
                            let last_id_fn = $last_id;
                            (last_id_fn)(&mut tx).await?
                        };

                        let sv_sql = format!(
                            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ({}, {})",
                            d.ph(1), d.ph(2)
                        );
                        sqlx::query(&sv_sql).bind(sid).bind(new_sv).execute(&mut *tx).await?;
                        sid
                    };

                    // End any existing comment tag for this column
                    let end_tag = format!(
                        "UPDATE ducklake_column_tag SET end_snapshot = {} WHERE table_id = {} AND column_id = {} AND {} = 'comment' AND end_snapshot IS NULL",
                        d.ph(1), d.ph(2), d.ph(3), d.col("key")
                    );
                    sqlx::query(&end_tag).bind(snapshot_id).bind(table_id).bind(column_id)
                        .execute(&mut *tx).await?;

                    // Insert new comment tag
                    let ins_tag = format!(
                        "INSERT INTO ducklake_column_tag (table_id, column_id, begin_snapshot, {}, value) VALUES ({}, {}, {}, 'comment', {})",
                        d.col("key"), d.ph(1), d.ph(2), d.ph(3), d.ph(4)
                    );
                    sqlx::query(&ins_tag)
                        .bind(table_id).bind(column_id).bind(snapshot_id).bind(comment)
                        .execute(&mut *tx).await?;

                    // Record change for conflict detection
                    let ct_sql = format!(
                        "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id) VALUES ({}, 'ALTER_TABLE', {})",
                        d.ph(1), d.ph(2)
                    );
                    sqlx::query(&ct_sql).bind(snapshot_id).bind(table_id)
                        .execute(&mut *tx).await?;

                    // Record snapshot changes
                    let changes_sql = format!(
                        "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                        d.ph(1), d.ph(2), d.upsert("snapshot_id", &["changes_made"])
                    );
                    sqlx::query(&changes_sql)
                        .bind(snapshot_id)
                        .bind(format!("altered_table:{}", table_id))
                        .execute(&mut *tx).await?;

                    tx.commit().await?;
                    Ok(snapshot_id)
                })
            }
    };
}

pub(crate) use impl_writer_ddl_ops;

/// Generates `drop_table_inner` and `drop_schema_inner` async helper methods.
/// These are invoked inside `impl $struct_name` blocks (not trait impls).
macro_rules! impl_writer_drop_inner {
    (
        $tx_type:ty,
        dialect = $dialect:expr,
        last_insert_id = $last_id:expr
    ) => {
        async fn drop_table_inner(
            mut tx: $tx_type,
            table_id: i64,
        ) -> Result<i64> {
            use crate::dialect::SqlDialect;
            let d = $dialect;

            // R4-S-014: Validate table exists and is active before creating snapshot
            let exists = sqlx::query(
                &format!("SELECT COUNT(*) FROM ducklake_table WHERE table_id = {} AND end_snapshot IS NULL", d.ph(1)),
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

            let snapshot_id: i64 = if d.supports_returning() {
                let row = sqlx::query(
                    &format!(
                        "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {}) RETURNING snapshot_id",
                        d.now(), d.ph(1)
                    ),
                )
                .bind(new_schema_version)
                .fetch_one(&mut *tx)
                .await?;
                row.try_get(0)?
            } else {
                sqlx::query(
                    &format!(
                        "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {})",
                        d.now(), d.ph(1)
                    ),
                )
                .bind(new_schema_version)
                .execute(&mut *tx)
                .await?;
                ($last_id)(&mut tx).await?
            };

            sqlx::query(
                &format!(
                    "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ({}, {})",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // Mark the table as dropped by setting end_snapshot
            sqlx::query(
                &format!(
                    "UPDATE ducklake_table SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // End all active columns for this table
            sqlx::query(
                &format!(
                    "UPDATE ducklake_column SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // End all active data files for this table
            sqlx::query(
                &format!(
                    "UPDATE ducklake_data_file SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // End all active delete files for this table
            sqlx::query(
                &format!(
                    "UPDATE ducklake_delete_file SET end_snapshot = {} WHERE table_id = {} AND end_snapshot IS NULL",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record the change for conflict detection
            sqlx::query(
                &format!(
                    "INSERT INTO _df_change_tracking (snapshot_id, change_type, table_id) VALUES ({}, 'DROP_TABLE', {})",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(table_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            let upsert = d.upsert("snapshot_id", &["changes_made"]);
            sqlx::query(
                &format!(
                    "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                    d.ph(1), d.ph(2), upsert
                ),
            )
            .bind(snapshot_id)
            .bind(format!("dropped_table:{}", table_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        }

        async fn drop_schema_inner(
            mut tx: $tx_type,
            schema_id: i64,
        ) -> Result<i64> {
            use crate::dialect::SqlDialect;
            let d = $dialect;

            // R4-S-014: Validate schema exists and is active before creating snapshot
            let exists = sqlx::query(
                &format!("SELECT COUNT(*) FROM ducklake_schema WHERE schema_id = {} AND end_snapshot IS NULL", d.ph(1)),
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

            let snapshot_id: i64 = if d.supports_returning() {
                let row = sqlx::query(
                    &format!(
                        "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {}) RETURNING snapshot_id",
                        d.now(), d.ph(1)
                    ),
                )
                .bind(new_schema_version)
                .fetch_one(&mut *tx)
                .await?;
                row.try_get(0)?
            } else {
                sqlx::query(
                    &format!(
                        "INSERT INTO ducklake_snapshot (snapshot_time, schema_version) VALUES ({}, {})",
                        d.now(), d.ph(1)
                    ),
                )
                .bind(new_schema_version)
                .execute(&mut *tx)
                .await?;
                ($last_id)(&mut tx).await?
            };

            sqlx::query(
                &format!(
                    "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version) VALUES ({}, {})",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(new_schema_version)
            .execute(&mut *tx)
            .await?;

            // Cascade: end columns for all active tables in this schema
            sqlx::query(
                &format!(
                    "UPDATE ducklake_column SET end_snapshot = {} \
                     WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE schema_id = {} AND end_snapshot IS NULL) \
                     AND end_snapshot IS NULL",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(schema_id)
            .execute(&mut *tx)
            .await?;

            // Cascade: end data files for all active tables in this schema
            sqlx::query(
                &format!(
                    "UPDATE ducklake_data_file SET end_snapshot = {} \
                     WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE schema_id = {} AND end_snapshot IS NULL) \
                     AND end_snapshot IS NULL",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(schema_id)
            .execute(&mut *tx)
            .await?;

            // Cascade: end delete files for all active tables in this schema
            sqlx::query(
                &format!(
                    "UPDATE ducklake_delete_file SET end_snapshot = {} \
                     WHERE table_id IN (SELECT table_id FROM ducklake_table WHERE schema_id = {} AND end_snapshot IS NULL) \
                     AND end_snapshot IS NULL",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(schema_id)
            .execute(&mut *tx)
            .await?;

            // End all active tables in this schema
            sqlx::query(
                &format!(
                    "UPDATE ducklake_table SET end_snapshot = {} WHERE schema_id = {} AND end_snapshot IS NULL",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(schema_id)
            .execute(&mut *tx)
            .await?;

            // Mark the schema as dropped
            sqlx::query(
                &format!(
                    "UPDATE ducklake_schema SET end_snapshot = {} WHERE schema_id = {} AND end_snapshot IS NULL",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(schema_id)
            .execute(&mut *tx)
            .await?;

            // Record the change for conflict detection
            sqlx::query(
                &format!(
                    "INSERT INTO _df_change_tracking (snapshot_id, change_type, schema_id) VALUES ({}, 'DROP_SCHEMA', {})",
                    d.ph(1), d.ph(2)
                ),
            )
            .bind(snapshot_id)
            .bind(schema_id)
            .execute(&mut *tx)
            .await?;

            // Record in spec-compliant snapshot changes
            let upsert = d.upsert("snapshot_id", &["changes_made"]);
            sqlx::query(
                &format!(
                    "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES ({}, {}) {}",
                    d.ph(1), d.ph(2), upsert
                ),
            )
            .bind(snapshot_id)
            .bind(format!("dropped_schema:{}", schema_id))
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok(snapshot_id)
        }
    };
}

pub(crate) use impl_writer_drop_inner;

/// Generates `drop_table`, `drop_schema`, `drop_table_checked`, `drop_schema_checked`
/// trait methods. Invoked inside `impl MetadataWriter for $struct_name` blocks.
macro_rules! impl_writer_drop_ops {
    (
        $struct_name:ty,
        pool_type = $pool_type:ty,
        dialect = $dialect:expr,
        block_on = $block_on:path
    ) => {
            fn drop_table(&self, table_id: i64) -> Result<i64> {
                let pool = &self.pool;
                $block_on(|| async {
                    let tx = pool.begin().await?;
                    <$struct_name>::drop_table_inner(tx, table_id).await
                })
            }

            fn drop_schema(&self, schema_id: i64) -> Result<i64> {
                let pool = &self.pool;
                $block_on(|| async {
                    let tx = pool.begin().await?;
                    <$struct_name>::drop_schema_inner(tx, schema_id).await
                })
            }

            fn drop_table_checked(&self, table_id: i64, since_snapshot: i64) -> Result<i64> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let pool = &self.pool;
                $block_on(|| async {
                    let mut tx = pool.begin().await?;

                    // Check DF-originated drops
                    let drop_check = sqlx::query(
                        &format!(
                            "SELECT COUNT(*) FROM _df_change_tracking \
                             WHERE snapshot_id > {} AND table_id = {} AND change_type = 'DROP_TABLE'",
                            d.ph(1), d.ph(2)
                        ),
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
                        &format!(
                            "SELECT COUNT(*) FROM ducklake_table \
                             WHERE table_id = {} AND end_snapshot IS NOT NULL AND end_snapshot > {}",
                            d.ph(1), d.ph(2)
                        ),
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

                    // No conflict — perform drop in the same transaction.
                    <$struct_name>::drop_table_inner(tx, table_id).await
                })
            }

            fn drop_schema_checked(&self, schema_id: i64, since_snapshot: i64) -> Result<i64> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let pool = &self.pool;
                $block_on(|| async {
                    let mut tx = pool.begin().await?;

                    // Check DF-originated drops
                    let drop_check = sqlx::query(
                        &format!(
                            "SELECT COUNT(*) FROM _df_change_tracking \
                             WHERE snapshot_id > {} AND schema_id = {} AND change_type = 'DROP_SCHEMA'",
                            d.ph(1), d.ph(2)
                        ),
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
                        &format!(
                            "SELECT COUNT(*) FROM ducklake_schema \
                             WHERE schema_id = {} AND end_snapshot IS NOT NULL AND end_snapshot > {}",
                            d.ph(1), d.ph(2)
                        ),
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

                    // No conflict — perform drop in the same transaction.
                    <$struct_name>::drop_schema_inner(tx, schema_id).await
                })
            }
    };
}

pub(crate) use impl_writer_drop_ops;
