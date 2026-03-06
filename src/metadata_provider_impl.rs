/// Shared implementation of MetadataProvider for sqlx-based backends (SQLite, PostgreSQL, MySQL).
///
/// The macro generates all trait methods. For methods that differ structurally between backends
/// (get_delete_files_added_between_snapshots, get_inlined_data, count_inlined_rows),
/// the macro delegates to `self.get_delete_files_impl(...)`, `self.get_inlined_data_impl(...)`,
/// and `self.count_inlined_rows_impl(...)` which each backend provides on its struct.
macro_rules! impl_metadata_provider {
    (
        $struct_name:ty,
        pool_type = $pool_type:ty,
        dialect = $dialect:expr
    ) => {
        impl MetadataProvider for $struct_name {
            fn get_current_snapshot(&self) -> Result<i64> {
                block_on(async {
                    let row =
                        sqlx::query("SELECT COALESCE(MAX(snapshot_id), 0) FROM ducklake_snapshot")
                            .fetch_one(&self.pool)
                            .await?;
                    Ok(row.try_get(0)?)
                })
            }

            fn get_data_path(&self) -> Result<String> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT value FROM ducklake_metadata WHERE {} = {} AND scope IS NULL",
                    d.col("key"),
                    d.ph(1),
                );
                block_on(async {
                    let row = sqlx::query(&sql)
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT s.snapshot_id, {}, s.schema_version,
                            c.changes_made, c.author, c.commit_message, c.commit_extra_info
                     FROM ducklake_snapshot s
                     LEFT JOIN ducklake_snapshot_changes c ON s.snapshot_id = c.snapshot_id
                     ORDER BY s.snapshot_id",
                    d.cast_text("s.snapshot_time"),
                );
                block_on(async {
                    let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;

                    rows.into_iter()
                        .map(|row| {
                            let snapshot_id: i64 = row.try_get(0)?;
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema
                     WHERE {} >= begin_snapshot AND ({} < end_snapshot OR end_snapshot IS NULL)",
                    d.ph(1),
                    d.ph(2),
                );
                block_on(async {
                    let rows = sqlx::query(&sql)
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

            fn list_tables(
                &self,
                schema_id: i64,
                snapshot_id: i64,
            ) -> Result<Vec<TableMetadata>> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT table_id, table_name, path, path_is_relative FROM ducklake_table
                     WHERE schema_id = {}
                       AND {} >= begin_snapshot
                       AND ({} < end_snapshot OR end_snapshot IS NULL)",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                );
                block_on(async {
                    let rows = sqlx::query(&sql)
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT column_id, column_name, column_type, nulls_allowed
                     FROM ducklake_column
                     WHERE table_id = {}
                       AND {} >= begin_snapshot
                       AND ({} < end_snapshot OR end_snapshot IS NULL)
                     ORDER BY column_order",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                );
                block_on(async {
                    let rows = sqlx::query(&sql)
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
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
                        AND del.table_id = {}
                        AND {} >= del.begin_snapshot
                        AND ({} < del.end_snapshot OR del.end_snapshot IS NULL)
                    WHERE data.table_id = {}
                      AND {} >= data.begin_snapshot
                      AND ({} < data.end_snapshot OR data.end_snapshot IS NULL)",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                    d.ph(4),
                    d.ph(5),
                    d.ph(6),
                );
                block_on(async {
                    let rows = sqlx::query(&sql)
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

            fn get_schema_by_name(
                &self,
                name: &str,
                snapshot_id: i64,
            ) -> Result<Option<SchemaMetadata>> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema
                     WHERE schema_name = {}
                       AND {} >= begin_snapshot
                       AND ({} < end_snapshot OR end_snapshot IS NULL)",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                );
                block_on(async {
                    let row = sqlx::query(&sql)
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT table_id, table_name, path, path_is_relative FROM ducklake_table
                     WHERE schema_id = {}
                       AND table_name = {}
                       AND {} >= begin_snapshot
                       AND ({} < end_snapshot OR end_snapshot IS NULL)",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                    d.ph(4),
                );
                block_on(async {
                    let row = sqlx::query(&sql)
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

            fn table_exists(
                &self,
                schema_id: i64,
                name: &str,
                snapshot_id: i64,
            ) -> Result<bool> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                block_on(async {
                    if d.existence_check_is_count() {
                        let sql = format!(
                            "SELECT COUNT(*) FROM ducklake_table
                             WHERE schema_id = {}
                               AND table_name = {}
                               AND {} >= begin_snapshot
                               AND ({} < end_snapshot OR end_snapshot IS NULL)",
                            d.ph(1),
                            d.ph(2),
                            d.ph(3),
                            d.ph(4),
                        );
                        let row = sqlx::query(&sql)
                            .bind(schema_id)
                            .bind(name)
                            .bind(snapshot_id)
                            .bind(snapshot_id)
                            .fetch_one(&self.pool)
                            .await?;
                        let count: i64 = row.try_get(0)?;
                        Ok(count > 0)
                    } else {
                        let sql = format!(
                            "SELECT EXISTS(
                                SELECT 1 FROM ducklake_table
                                WHERE schema_id = {}
                                  AND table_name = {}
                                  AND {} >= begin_snapshot
                                  AND ({} < end_snapshot OR end_snapshot IS NULL)
                            )",
                            d.ph(1),
                            d.ph(2),
                            d.ph(3),
                            d.ph(4),
                        );
                        let row = sqlx::query(&sql)
                            .bind(schema_id)
                            .bind(name)
                            .bind(snapshot_id)
                            .bind(snapshot_id)
                            .fetch_one(&self.pool)
                            .await?;
                        Ok(row.try_get::<bool, _>(0)?)
                    }
                })
            }

            fn list_all_tables(&self, snapshot_id: i64) -> Result<Vec<TableWithSchema>> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT s.schema_name, s.schema_id, t.table_id, t.table_name,
                            {} AS table_uuid, t.path, t.path_is_relative
                     FROM ducklake_schema s
                     JOIN ducklake_table t ON s.schema_id = t.schema_id
                     WHERE {} >= s.begin_snapshot
                       AND ({} < s.end_snapshot OR s.end_snapshot IS NULL)
                       AND {} >= t.begin_snapshot
                       AND ({} < t.end_snapshot OR t.end_snapshot IS NULL)
                     ORDER BY s.schema_name, t.table_name",
                    d.cast_text("t.table_uuid"),
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                    d.ph(4),
                );
                block_on(async {
                    let rows = sqlx::query(&sql)
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT s.schema_name, t.table_name, c.column_id, c.column_name, c.column_type, c.nulls_allowed
                     FROM ducklake_schema s
                     JOIN ducklake_table t ON s.schema_id = t.schema_id
                     JOIN ducklake_column c ON t.table_id = c.table_id
                     WHERE {} >= s.begin_snapshot
                       AND ({} < s.end_snapshot OR s.end_snapshot IS NULL)
                       AND {} >= t.begin_snapshot
                       AND ({} < t.end_snapshot OR t.end_snapshot IS NULL)
                       AND {} >= c.begin_snapshot
                       AND ({} < c.end_snapshot OR c.end_snapshot IS NULL)
                     ORDER BY s.schema_name, t.table_name, c.column_order",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                    d.ph(4),
                    d.ph(5),
                    d.ph(6),
                );
                block_on(async {
                    let rows = sqlx::query(&sql)
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
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
                        AND {} >= del.begin_snapshot
                        AND ({} < del.end_snapshot OR del.end_snapshot IS NULL)
                    WHERE {} >= s.begin_snapshot
                      AND ({} < s.end_snapshot OR s.end_snapshot IS NULL)
                      AND {} >= t.begin_snapshot
                      AND ({} < t.end_snapshot OR t.end_snapshot IS NULL)
                      AND {} >= data.begin_snapshot
                      AND ({} < data.end_snapshot OR data.end_snapshot IS NULL)
                    ORDER BY s.schema_name, t.table_name, data.path",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                    d.ph(4),
                    d.ph(5),
                    d.ph(6),
                    d.ph(7),
                    d.ph(8),
                );
                block_on(async {
                    let rows = sqlx::query(&sql)
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT
                        data.begin_snapshot,
                        data.path,
                        data.path_is_relative,
                        data.file_size_bytes,
                        data.footer_size,
                        data.encryption_key
                    FROM ducklake_data_file AS data
                    WHERE data.table_id = {}
                      AND data.begin_snapshot > {}
                      AND data.begin_snapshot <= {}
                    ORDER BY data.begin_snapshot",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                );
                block_on(async {
                    let rows = sqlx::query(&sql)
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
                self.get_delete_files_impl(table_id, start_snapshot, end_snapshot)
            }

            fn get_file_column_stats(
                &self,
                table_id: i64,
                snapshot_id: i64,
            ) -> Result<Vec<FileColumnStats>> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT s.data_file_id, c.column_name, s.null_count, s.min_value, s.max_value
                     FROM ducklake_file_column_stats s
                     JOIN ducklake_data_file f ON s.data_file_id = f.data_file_id
                     JOIN ducklake_column c ON s.column_id = c.column_id
                     WHERE s.table_id = {}
                       AND {} >= f.begin_snapshot
                       AND ({} < f.end_snapshot OR f.end_snapshot IS NULL)",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                );
                block_on(async {
                    sqlx::query(&sql)
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

            fn get_table_row_count(
                &self,
                table_id: i64,
                snapshot_id: i64,
            ) -> Result<Option<i64>> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let row_count_expr = d.cast_int(
                    "COALESCE(SUM(data.record_count), 0) - COALESCE(SUM(del.delete_count), 0)",
                );
                let sql = format!(
                    "SELECT
                        CASE WHEN COUNT(*) = COUNT(data.record_count)
                            THEN {row_count_expr}
                            ELSE NULL
                        END as row_count
                    FROM ducklake_data_file data
                    LEFT JOIN ducklake_delete_file del
                        ON data.data_file_id = del.data_file_id
                        AND del.table_id = {}
                        AND {} >= del.begin_snapshot
                        AND ({} < del.end_snapshot OR del.end_snapshot IS NULL)
                    WHERE data.table_id = {}
                      AND {} >= data.begin_snapshot
                      AND ({} < data.end_snapshot OR data.end_snapshot IS NULL)",
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                    d.ph(4),
                    d.ph(5),
                    d.ph(6),
                );
                block_on(async {
                    let row = sqlx::query(&sql)
                        .bind(table_id)
                        .bind(snapshot_id)
                        .bind(snapshot_id)
                        .bind(table_id)
                        .bind(snapshot_id)
                        .bind(snapshot_id)
                        .fetch_one(&self.pool)
                        .await?;

                    let file_count: Option<i64> = row.try_get(0)?;

                    let inlined_count =
                        self.count_inlined_rows_impl(table_id, snapshot_id).await?;

                    match (file_count, inlined_count) {
                        (Some(fc), ic) => Ok(Some(fc + ic)),
                        (None, _) => Ok(None),
                    }
                })
            }

            fn get_partition_columns(
                &self,
                table_id: i64,
                snapshot_id: i64,
            ) -> Result<Vec<PartitionColumn>> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT {} AS partition_key_index, c.column_name, pc.transform
                     FROM ducklake_partition_info pi
                     JOIN ducklake_partition_column pc
                         ON pi.partition_id = pc.partition_id AND pi.table_id = pc.table_id
                     JOIN ducklake_column c ON pc.column_id = c.column_id
                     WHERE pi.table_id = {}
                       AND {} >= pi.begin_snapshot
                       AND ({} < pi.end_snapshot OR pi.end_snapshot IS NULL)
                     ORDER BY pc.partition_key_index",
                    d.cast_int("pc.partition_key_index"),
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                );
                block_on(async {
                    sqlx::query(&sql)
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT fpv.data_file_id, {} AS partition_key_index, fpv.partition_value
                     FROM ducklake_file_partition_value fpv
                     JOIN ducklake_data_file df ON fpv.data_file_id = df.data_file_id
                     WHERE fpv.table_id = {}
                       AND {} >= df.begin_snapshot
                       AND ({} < df.end_snapshot OR df.end_snapshot IS NULL)",
                    d.cast_int("fpv.partition_key_index"),
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                );
                block_on(async {
                    sqlx::query(&sql)
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

            fn list_views(
                &self,
                schema_id: i64,
                snapshot_id: i64,
            ) -> Result<Vec<ViewMetadata>> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT view_id, view_name, {} FROM ducklake_view
                     WHERE schema_id = {}
                       AND {} >= begin_snapshot
                       AND ({} < end_snapshot OR end_snapshot IS NULL)",
                    d.col("sql"),
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                );
                block_on(async {
                    sqlx::query(&sql)
                        .bind(schema_id)
                        .bind(snapshot_id)
                        .bind(snapshot_id)
                        .fetch_all(&self.pool)
                        .await?
                        .into_iter()
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
                use crate::dialect::SqlDialect;
                let d = $dialect;
                let sql = format!(
                    "SELECT view_id, view_name, {} FROM ducklake_view
                     WHERE schema_id = {}
                       AND view_name = {}
                       AND {} >= begin_snapshot
                       AND ({} < end_snapshot OR end_snapshot IS NULL)",
                    d.col("sql"),
                    d.ph(1),
                    d.ph(2),
                    d.ph(3),
                    d.ph(4),
                );
                block_on(async {
                    let row = sqlx::query(&sql)
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

            fn view_exists(
                &self,
                schema_id: i64,
                name: &str,
                snapshot_id: i64,
            ) -> Result<bool> {
                use crate::dialect::SqlDialect;
                let d = $dialect;
                block_on(async {
                    if d.existence_check_is_count() {
                        let sql = format!(
                            "SELECT COUNT(*) FROM ducklake_view
                             WHERE schema_id = {}
                               AND view_name = {}
                               AND {} >= begin_snapshot
                               AND ({} < end_snapshot OR end_snapshot IS NULL)",
                            d.ph(1),
                            d.ph(2),
                            d.ph(3),
                            d.ph(4),
                        );
                        let row = sqlx::query(&sql)
                            .bind(schema_id)
                            .bind(name)
                            .bind(snapshot_id)
                            .bind(snapshot_id)
                            .fetch_one(&self.pool)
                            .await?;
                        let count: i64 = row.try_get(0)?;
                        Ok(count > 0)
                    } else {
                        let sql = format!(
                            "SELECT EXISTS(
                                SELECT 1 FROM ducklake_view
                                WHERE schema_id = {}
                                  AND view_name = {}
                                  AND {} >= begin_snapshot
                                  AND ({} < end_snapshot OR end_snapshot IS NULL)
                            )",
                            d.ph(1),
                            d.ph(2),
                            d.ph(3),
                            d.ph(4),
                        );
                        let row = sqlx::query(&sql)
                            .bind(schema_id)
                            .bind(name)
                            .bind(snapshot_id)
                            .bind(snapshot_id)
                            .fetch_one(&self.pool)
                            .await?;
                        Ok(row.try_get::<bool, _>(0)?)
                    }
                })
            }

            fn get_inlined_data(
                &self,
                table_id: i64,
                snapshot_id: i64,
            ) -> Result<Vec<InlinedDataRow>> {
                self.get_inlined_data_impl(table_id, snapshot_id)
            }
        }
    };
}

pub(crate) use impl_metadata_provider;
