use crate::DuckLakeError;
use crate::metadata_provider::{
    ColumnWithTable, DataFileChange, DeleteFileChange, DuckLakeFileData, DuckLakeTableColumn,
    DuckLakeTableFile, FileColumnStats, FilePartitionValue, FileWithTable, InlinedDataRow,
    MetadataProvider, PartitionColumn, SQL_GET_DATA_FILES,
    SQL_GET_DATA_FILES_ADDED_BETWEEN_SNAPSHOTS, SQL_GET_DATA_PATH,
    SQL_GET_DELETE_FILES_ADDED_BETWEEN_SNAPSHOTS, SQL_GET_FILE_COLUMN_STATS,
    SQL_GET_FILE_PARTITION_VALUES, SQL_GET_LATEST_SNAPSHOT, SQL_GET_PARTITION_COLUMNS,
    SQL_GET_SCHEMA_BY_NAME, SQL_GET_TABLE_BY_NAME, SQL_GET_TABLE_COLUMNS, SQL_GET_TABLE_ROW_COUNT,
    SQL_GET_VIEW_BY_NAME, SQL_LIST_ALL_COLUMNS, SQL_LIST_ALL_FILES, SQL_LIST_ALL_TABLES,
    SQL_LIST_SCHEMAS, SQL_LIST_SNAPSHOTS, SQL_LIST_TABLES, SQL_LIST_VIEWS, SQL_TABLE_EXISTS,
    SQL_VIEW_EXISTS, SchemaMetadata, SnapshotMetadata, TableMetadata, TableWithSchema,
    ViewMetadata, quote_identifier,
};
use duckdb::AccessMode::ReadOnly;
use duckdb::{Config, Connection, params};
use std::sync::{Arc, Mutex, MutexGuard};

/// DuckDB metadata provider
///
/// Uses a single shared connection protected by a Mutex to avoid
/// the overhead of creating a new connection for each metadata query.
/// This is safe for read-only operations.
#[derive(Debug, Clone)]
pub struct DuckdbMetadataProvider {
    conn: Arc<Mutex<Connection>>,
    /// Path to the catalog database, used in error messages and tracing
    catalog_path: String,
}

impl DuckdbMetadataProvider {
    /// Create a new DuckDB metadata provider
    pub fn new(catalog_path: impl Into<String>) -> crate::Result<Self> {
        let catalog_path = catalog_path.into();
        let conn = Self::create_connection(&catalog_path)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            catalog_path,
        })
    }

    /// Get a reference to the shared connection
    fn connection(&self) -> crate::Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|e| {
            DuckLakeError::Internal(format!(
                "DuckDB connection mutex poisoned for catalog '{}': {}",
                self.catalog_path, e
            ))
        })
    }

    /// Create a new read-only connection to the catalog database
    fn create_connection(catalog_path: &str) -> crate::Result<Connection> {
        let config = Config::default().access_mode(ReadOnly)?;
        match Connection::open_with_flags(catalog_path, config) {
            Ok(con) => Ok(con),
            Err(msg)
                if msg
                    .to_string()
                    .starts_with("IO Error: Could not set lock on file") =>
            {
                tracing::warn!(
                    error = %msg,
                    "DuckDB file likely already open in write mode. Cannot connect"
                );
                Err(DuckLakeError::DuckDb(msg))
            },
            Err(msg) => {
                tracing::error!(error = %msg, "Failed to open DuckDB catalog");
                Err(DuckLakeError::DuckDb(msg))
            },
        }
    }

    /// Count inlined rows for a table at a given snapshot.
    fn count_inlined_rows(
        &self,
        conn: &Connection,
        table_id: i64,
        snapshot_id: i64,
    ) -> crate::Result<i64> {
        // Look up the inlined data table name
        let result = conn.query_row(
            "SELECT table_name FROM ducklake_inlined_data_tables WHERE table_id = ?",
            [table_id],
            |row| {
                let table_name: String = row.get(0)?;
                Ok(table_name)
            },
        );

        let inlined_table_name = match result {
            Ok(name) => name,
            Err(duckdb::Error::QueryReturnedNoRows) => return Ok(0),
            Err(e) => return Err(DuckLakeError::DuckDb(e)),
        };

        // Verify the table actually exists
        let table_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = ?",
                [&inlined_table_name],
                |row| row.get(0),
            )
            .map_err(DuckLakeError::DuckDb)?;
        if !table_exists {
            return Ok(0);
        }

        // Count active inlined rows at this snapshot
        let count_sql = format!(
            "SELECT COUNT(*) FROM {} WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
            quote_identifier(&inlined_table_name)
        );
        let count: i64 = conn.query_row(&count_sql, params![snapshot_id, snapshot_id], |row| {
            row.get(0)
        })?;

        Ok(count)
    }
}

impl MetadataProvider for DuckdbMetadataProvider {
    fn get_current_snapshot(&self) -> crate::Result<i64> {
        let conn = self.connection()?;
        let snapshot_id: i64 = conn.query_row(SQL_GET_LATEST_SNAPSHOT, [], |row| row.get(0))?;
        Ok(snapshot_id)
    }

    fn get_data_path(&self) -> crate::Result<String> {
        let conn = self.connection()?;
        let result = conn.query_row(SQL_GET_DATA_PATH, [], |row| row.get(0));
        match result {
            Ok(data_path) => Ok(data_path),
            Err(duckdb::Error::QueryReturnedNoRows) => Err(DuckLakeError::InvalidConfig(
                "No data_path found in ducklake_metadata table. \
                     Is this a valid DuckLake catalog?"
                    .to_string(),
            )),
            Err(e) => Err(DuckLakeError::DuckDb(e)),
        }
    }

    fn list_snapshots(&self) -> crate::Result<Vec<SnapshotMetadata>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_LIST_SNAPSHOTS)?;

        let snapshots = stmt
            .query_map([], |row| {
                Ok(SnapshotMetadata {
                    snapshot_id: row.get(0)?,
                    snapshot_time: row.get(1)?,
                    schema_version: row.get(2)?,
                    changes: row.get(3)?,
                    author: row.get(4)?,
                    commit_message: row.get(5)?,
                    commit_extra_info: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(snapshots)
    }

    fn list_schemas(&self, snapshot_id: i64) -> crate::Result<Vec<SchemaMetadata>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_LIST_SCHEMAS)?;

        let schemas = stmt
            .query_map([snapshot_id, snapshot_id], |row| {
                let schema_id: i64 = row.get(0)?;
                let schema_name: String = row.get(1)?;
                let path: String = row.get(2)?;
                let path_is_relative: bool = row.get(3)?;
                Ok(SchemaMetadata {
                    schema_id,
                    schema_name,
                    path,
                    path_is_relative,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(schemas)
    }

    fn list_tables(&self, schema_id: i64, snapshot_id: i64) -> crate::Result<Vec<TableMetadata>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_LIST_TABLES)?;

        let tables = stmt
            .query_map([schema_id, snapshot_id, snapshot_id], |row| {
                let table_id: i64 = row.get(0)?;
                let table_name: String = row.get(1)?;
                let path: String = row.get(2)?;
                let path_is_relative: bool = row.get(3)?;
                Ok(TableMetadata {
                    table_id,
                    table_name,
                    path,
                    path_is_relative,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    fn get_table_structure(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> crate::Result<Vec<DuckLakeTableColumn>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_GET_TABLE_COLUMNS)?;

        let columns = stmt
            .query_map([table_id, snapshot_id, snapshot_id], |row| {
                let column_id: i64 = row.get(0)?;
                let column_name: String = row.get(1)?;
                let column_type: String = row.get(2)?;
                let nulls_allowed: Option<bool> = row.get(3)?;
                if nulls_allowed.is_none() {
                    tracing::warn!(
                        column_name = %column_name,
                        "nulls_allowed is NULL in catalog — defaulting to true; this may indicate catalog corruption"
                    );
                }
                Ok(DuckLakeTableColumn::new(
                    column_id,
                    column_name,
                    column_type,
                    nulls_allowed.unwrap_or(true),
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(columns)
    }

    fn get_table_files_for_select(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> crate::Result<Vec<DuckLakeTableFile>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_GET_DATA_FILES)?;

        let files = stmt
            .query_map(
                [table_id, snapshot_id, snapshot_id, table_id, snapshot_id, snapshot_id],
                |row| {
                    // Parse data file (columns 0-5)
                    let data_file_id: i64 = row.get(0)?;
                    let data_file = DuckLakeFileData {
                        path: row.get(1)?,
                        path_is_relative: row.get(2)?,
                        file_size_bytes: row.get(3)?,
                        footer_size: row.get(4)?,
                        encryption_key: row.get(5)?,
                    };

                    // Parse delete file (columns 6-12) if exists
                    let delete_file = if let Ok(Some(_)) = row.get::<_, Option<i64>>(6) {
                        Some(DuckLakeFileData {
                            path: row.get(7)?,
                            path_is_relative: row.get(8)?,
                            file_size_bytes: row.get(9)?,
                            footer_size: row.get(10)?,
                            encryption_key: row.get(11)?,
                        })
                    } else {
                        None
                    };

                    // Column 12 (_delete_count) intentionally skipped — unused
                    let begin_snapshot: Option<i64> = row.get(13)?;
                    let row_id_start: Option<i64> = row.get(14)?;
                    let record_count: Option<i64> = row.get(15)?;

                    Ok(DuckLakeTableFile {
                        data_file_id: Some(data_file_id),
                        file: data_file,
                        delete_file,
                        row_id_start,
                        snapshot_id: begin_snapshot,
                        max_row_count: record_count,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    fn get_schema_by_name(
        &self,
        name: &str,
        snapshot_id: i64,
    ) -> crate::Result<Option<SchemaMetadata>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_GET_SCHEMA_BY_NAME)?;

        let mut rows = stmt.query(params![name, snapshot_id, snapshot_id])?;

        if let Some(row) = rows.next()? {
            let schema_id: i64 = row.get(0)?;
            let schema_name: String = row.get(1)?;
            let path: String = row.get(2)?;
            let path_is_relative: bool = row.get(3)?;
            Ok(Some(SchemaMetadata {
                schema_id,
                schema_name,
                path,
                path_is_relative,
            }))
        } else {
            Ok(None)
        }
    }

    fn get_table_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> crate::Result<Option<TableMetadata>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_GET_TABLE_BY_NAME)?;

        let mut rows = stmt.query(params![&schema_id, &name, &snapshot_id, &snapshot_id])?;

        if let Some(row) = rows.next()? {
            let table_id: i64 = row.get(0)?;
            let table_name: String = row.get(1)?;
            let path: String = row.get(2)?;
            let path_is_relative: bool = row.get(3)?;
            Ok(Some(TableMetadata {
                table_id,
                table_name,
                path,
                path_is_relative,
            }))
        } else {
            Ok(None)
        }
    }

    fn table_exists(&self, schema_id: i64, name: &str, snapshot_id: i64) -> crate::Result<bool> {
        let conn = self.connection()?;
        let exists: bool = conn.query_row(
            SQL_TABLE_EXISTS,
            params![schema_id, &name, &snapshot_id, &snapshot_id],
            |row| row.get(0),
        )?;
        Ok(exists)
    }

    fn list_all_tables(&self, snapshot_id: i64) -> crate::Result<Vec<TableWithSchema>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_LIST_ALL_TABLES)?;

        let tables = stmt
            .query_map(
                params![snapshot_id, snapshot_id, snapshot_id, snapshot_id],
                |row| {
                    let schema_name: String = row.get(0)?;
                    let schema_id: i64 = row.get(1)?;
                    let table = TableMetadata {
                        table_id: row.get(2)?,
                        table_name: row.get(3)?,
                        path: row.get(5)?,
                        path_is_relative: row.get(6)?,
                    };
                    let table_uuid: Option<String> = row.get(4)?;
                    Ok(TableWithSchema {
                        schema_name,
                        schema_id,
                        table_uuid,
                        table,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    fn list_all_columns(&self, snapshot_id: i64) -> crate::Result<Vec<ColumnWithTable>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_LIST_ALL_COLUMNS)?;

        let columns = stmt
            .query_map(
                params![snapshot_id, snapshot_id, snapshot_id, snapshot_id, snapshot_id, snapshot_id],
                |row| {
                    let schema_name: String = row.get(0)?;
                    let table_name: String = row.get(1)?;
                    let nulls_allowed: Option<bool> = row.get(5)?;
                    let col_name: String = row.get(3)?;
                    if nulls_allowed.is_none() {
                        tracing::warn!(
                            column_name = %col_name,
                            "nulls_allowed is NULL in catalog — defaulting to true; this may indicate catalog corruption"
                        );
                    }
                    let column = DuckLakeTableColumn {
                        column_id: row.get(2)?,
                        column_name: col_name,
                        column_type: row.get(4)?,
                        is_nullable: nulls_allowed.unwrap_or(true),
                    };
                    Ok(ColumnWithTable {
                        schema_name,
                        table_name,
                        column,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(columns)
    }

    fn list_all_files(&self, snapshot_id: i64) -> crate::Result<Vec<FileWithTable>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_LIST_ALL_FILES)?;

        let files = stmt
            .query_map(
                params![
                    snapshot_id,
                    snapshot_id,
                    snapshot_id,
                    snapshot_id,
                    snapshot_id,
                    snapshot_id,
                    snapshot_id,
                    snapshot_id
                ],
                |row| {
                    let schema_name: String = row.get(0)?;
                    let table_name: String = row.get(1)?;

                    // Parse data file (column 2: data_file_id)
                    let data_file_id: i64 = row.get(2)?;
                    let data_file = DuckLakeFileData {
                        path: row.get(3)?,
                        path_is_relative: row.get(4)?,
                        file_size_bytes: row.get(5)?,
                        footer_size: row.get(6)?,
                        encryption_key: row.get(7)?,
                    };

                    // Parse optional delete file (column 8: delete_file_id, check if exists but don't store)
                    let delete_file =
                        if let Ok(Some(_delete_file_id)) = row.get::<_, Option<i64>>(8) {
                            Some(DuckLakeFileData {
                                path: row.get(9)?,
                                path_is_relative: row.get(10)?,
                                file_size_bytes: row.get(11)?,
                                footer_size: row.get(12)?,
                                encryption_key: row.get(13)?,
                            })
                        } else {
                            None
                        };

                    let max_row_count = row.get::<_, Option<i64>>(14)?;

                    Ok(FileWithTable {
                        schema_name,
                        table_name,
                        file: DuckLakeTableFile {
                            data_file_id: Some(data_file_id),
                            file: data_file,
                            delete_file,
                            row_id_start: None,
                            snapshot_id: None,
                            max_row_count,
                        },
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    fn get_data_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> crate::Result<Vec<DataFileChange>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_GET_DATA_FILES_ADDED_BETWEEN_SNAPSHOTS)?;

        let files = stmt
            .query_map(params![table_id, start_snapshot, end_snapshot], |row| {
                Ok(DataFileChange {
                    begin_snapshot: row.get(0)?,
                    path: row.get(1)?,
                    path_is_relative: row.get(2)?,
                    file_size_bytes: row.get(3)?,
                    footer_size: row.get(4)?,
                    encryption_key: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    fn get_table_row_count(&self, table_id: i64, snapshot_id: i64) -> crate::Result<Option<i64>> {
        let conn = self.connection()?;
        let file_count: Option<i64> = conn.query_row(
            SQL_GET_TABLE_ROW_COUNT,
            params![table_id, snapshot_id, snapshot_id, table_id, snapshot_id, snapshot_id],
            |row| row.get(0),
        )?;

        // Also count inlined data rows (matching SQLite/Postgres/MySQL providers)
        let inlined_count = self.count_inlined_rows(&conn, table_id, snapshot_id)?;

        match (file_count, inlined_count) {
            (Some(fc), ic) => Ok(Some(fc + ic)),
            (None, _) => Ok(None),
        }
    }

    fn get_file_column_stats(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> crate::Result<Vec<FileColumnStats>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_GET_FILE_COLUMN_STATS)?;

        let stats = stmt
            .query_map(params![table_id, snapshot_id, snapshot_id], |row| {
                Ok(FileColumnStats {
                    data_file_id: row.get(0)?,
                    column_name: row.get(1)?,
                    null_count: row.get(2)?,
                    min_value: row.get(3)?,
                    max_value: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(stats)
    }

    fn get_partition_columns(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> crate::Result<Vec<PartitionColumn>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_GET_PARTITION_COLUMNS)?;

        let columns = stmt
            .query_map(params![table_id, snapshot_id, snapshot_id], |row| {
                Ok(PartitionColumn {
                    partition_key_index: row.get(0)?,
                    column_name: row.get(1)?,
                    transform: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(columns)
    }

    fn get_file_partition_values(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> crate::Result<Vec<FilePartitionValue>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_GET_FILE_PARTITION_VALUES)?;

        let values = stmt
            .query_map(params![table_id, snapshot_id, snapshot_id], |row| {
                Ok(FilePartitionValue {
                    data_file_id: row.get(0)?,
                    partition_key_index: row.get(1)?,
                    partition_value: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(values)
    }

    fn get_inlined_data(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> crate::Result<Vec<InlinedDataRow>> {
        let conn = self.connection()?;

        // Look up the inlined data table name, filtered by snapshot's schema_version (R5-S-028).
        // Pick the latest schema_version that doesn't exceed the snapshot's version.
        let result = conn.query_row(
            "SELECT table_name FROM ducklake_inlined_data_tables \
             WHERE table_id = ? \
               AND schema_version <= (SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = ?) \
             ORDER BY schema_version DESC LIMIT 1",
            [table_id, snapshot_id],
            |row| row.get::<_, String>(0),
        );

        let inlined_table_name: String = match result {
            Ok(name) => name,
            Err(duckdb::Error::QueryReturnedNoRows) => return Ok(Vec::new()),
            Err(e) => return Err(DuckLakeError::DuckDb(e)),
        };

        // Verify the inlined data table actually exists before querying it
        let table_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = ?",
                [&inlined_table_name],
                |row| row.get(0),
            )
            .map_err(DuckLakeError::DuckDb)?;
        if !table_exists {
            return Ok(Vec::new());
        }

        // Get column names from the inlined data table (skip row_id, begin_snapshot, end_snapshot)
        let pragma_sql = format!(
            "PRAGMA table_info({})",
            quote_identifier(&inlined_table_name)
        );
        let mut pragma_stmt = conn.prepare(&pragma_sql)?;
        let user_columns: Vec<String> = pragma_stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|name| name != "row_id" && name != "begin_snapshot" && name != "end_snapshot")
            .collect();

        if user_columns.is_empty() {
            return Ok(Vec::new());
        }

        // Build select query with quoted identifiers to prevent SQL injection
        let col_list: Vec<String> = user_columns
            .iter()
            .map(|c| format!("CAST({} AS VARCHAR)", quote_identifier(c)))
            .collect();
        let select_sql = format!(
            "SELECT {} FROM {} WHERE begin_snapshot <= ? AND (end_snapshot IS NULL OR ? < end_snapshot)",
            col_list.join(", "),
            quote_identifier(&inlined_table_name),
        );

        let num_columns = user_columns.len();
        let user_columns = Arc::new(user_columns);
        let mut stmt = conn.prepare(&select_sql)?;
        let rows = stmt
            .query_map([snapshot_id, snapshot_id], |row| {
                let mut values = Vec::new();
                for i in 0..num_columns {
                    let val: Option<String> = row.get(i)?;
                    values.push(val);
                }
                Ok(InlinedDataRow {
                    column_names: Arc::clone(&user_columns),
                    values,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    fn get_delete_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> crate::Result<Vec<DeleteFileChange>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_GET_DELETE_FILES_ADDED_BETWEEN_SNAPSHOTS)?;

        let files = stmt
            .query_map(params![table_id, start_snapshot, end_snapshot], |row| {
                Ok(DeleteFileChange {
                    // data file
                    data_file_path: row.get(0)?,
                    data_file_path_is_relative: row.get(1)?,
                    data_file_size_bytes: row.get(2)?,
                    data_file_footer_size: row.get(3)?,
                    data_row_id_start: row.get(4)?,
                    data_record_count: row.get(5)?,
                    data_mapping_id: row.get(6)?,

                    // current delete
                    current_delete_path: row.get(7)?,
                    current_delete_path_is_relative: row.get(8)?,
                    current_delete_file_size_bytes: row.get(9)?,
                    current_delete_footer_size: row.get(10)?,

                    // previous delete
                    previous_delete_path: row.get(11)?,
                    previous_delete_path_is_relative: row.get(12)?,
                    previous_delete_file_size_bytes: row.get(13)?,
                    previous_delete_footer_size: row.get(14)?,

                    // data file encryption key (R6-S-012)
                    data_encryption_key: row.get(15)?,

                    // snapshot
                    snapshot_id: row.get(16)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(files)
    }

    fn list_views(&self, schema_id: i64, snapshot_id: i64) -> crate::Result<Vec<ViewMetadata>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(SQL_LIST_VIEWS)?;
        let views = stmt
            .query_map(params![schema_id, snapshot_id, snapshot_id], |row| {
                Ok(ViewMetadata {
                    view_id: row.get(0)?,
                    view_name: row.get(1)?,
                    sql: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(views)
    }

    fn get_view_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> crate::Result<Option<ViewMetadata>> {
        let conn = self.connection()?;
        let result = conn.query_row(
            SQL_GET_VIEW_BY_NAME,
            params![schema_id, name, snapshot_id, snapshot_id],
            |row| {
                Ok(ViewMetadata {
                    view_id: row.get(0)?,
                    view_name: row.get(1)?,
                    sql: row.get(2)?,
                })
            },
        );
        match result {
            Ok(view) => Ok(Some(view)),
            Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(DuckLakeError::DuckDb(e)),
        }
    }

    fn view_exists(&self, schema_id: i64, name: &str, snapshot_id: i64) -> crate::Result<bool> {
        let conn = self.connection()?;
        let exists: bool = conn.query_row(
            SQL_VIEW_EXISTS,
            params![schema_id, name, snapshot_id, snapshot_id],
            |row| row.get(0),
        )?;
        Ok(exists)
    }
}
