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
