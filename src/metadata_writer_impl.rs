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
