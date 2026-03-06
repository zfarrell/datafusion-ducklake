//! Metadata writer trait and common types for DuckLake catalog writes.
//!
//! This module provides the `MetadataWriter` trait for writing metadata to DuckLake catalogs,
//! along with helper types for column definitions and data file registration.

use crate::Result;
use crate::metadata_provider::InlinedDataRow;

/// Write mode for table operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Drop existing data and replace with new data
    Replace,
    /// Keep existing data and append new records
    Append,
}
use crate::types::{arrow_to_ducklake_type, ducklake_to_arrow_type};
use arrow::datatypes::DataType;

/// ALTER TABLE operation types.
#[derive(Debug, Clone)]
pub enum AlterTableOp {
    /// Add a new column. Must be nullable (existing rows have no value).
    AddColumn {
        column: ColumnDef,
    },
    /// Drop a column (soft delete via end_snapshot).
    DropColumn {
        column_name: String,
    },
    /// Rename a column.
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    /// Change a column's type (widening only).
    AlterColumnType(AlterColumnTypeOp),
    /// Set a column's default value.
    SetColumnDefault {
        column_name: String,
        default_value: String,
    },
    /// Drop a column's default value.
    DropColumnDefault {
        column_name: String,
    },
    /// Set a column as NOT NULL.
    SetNotNull {
        column_name: String,
    },
    /// Drop a column's NOT NULL constraint (allow NULLs).
    DropNotNull {
        column_name: String,
    },
    /// Set partition columns for a table.
    SetPartitionedBy {
        /// Column names and optional transforms
        partition_columns: Vec<PartitionColumnDef>,
    },
}

/// Definition of a partition column for SET PARTITIONED BY.
#[derive(Debug, Clone)]
pub struct PartitionColumnDef {
    /// Name of the source column
    pub column_name: String,
    /// Optional transform (e.g., "year", "month", "day", "hour", "identity")
    pub transform: Option<String>,
}

/// Parameters for ALTER COLUMN TYPE operation.
#[derive(Debug, Clone)]
pub struct AlterColumnTypeOp {
    /// Name of the column to alter
    pub column_name: String,
    /// New DuckLake type string (must be a valid widening of the current type)
    pub new_type: String,
}

/// Check if a type promotion is allowed (widening only).
///
/// Matches the DuckLake C++ type promotion rules:
/// - int8 → int16 → int32 → int64
/// - uint8 → uint16 → uint32 → uint64
/// - float → double
/// - Unsigned integers can widen to larger signed integers
pub fn is_type_promotion_allowed(source: &str, target: &str) -> bool {
    matches!(
        (source, target),
        // Signed integer widening chain
        ("int8", "int16" | "int32" | "int64")
            | ("int16", "int32" | "int64")
            | ("int32", "int64")
            // Unsigned integer widening chain
            | ("uint8", "uint16" | "uint32" | "uint64")
            | ("uint16", "uint32" | "uint64")
            | ("uint32", "uint64")
            // Unsigned → signed cross-promotion
            | ("uint8", "int16" | "int32" | "int64")
            | ("uint16", "int32" | "int64")
            | ("uint32", "int64")
            // Float widening
            | ("float", "double")
            // Timestamp widening
            | ("timestamp", "timestamptz")
    )
}

/// Column definition for creating or updating a table's schema.
///
/// Unlike `DuckLakeTableColumn` (used for reading), this struct doesn't have a `column_id`
/// field since IDs are assigned by the catalog during write operations.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name
    pub(crate) name: String,
    /// DuckLake type string (e.g., "varchar", "int64", "decimal(10,2)")
    pub(crate) ducklake_type: String,
    /// Whether this column allows NULL values
    pub is_nullable: bool,
    /// Initial default value expression (DuckLake forward compatibility)
    pub initial_default: Option<String>,
    /// Default value expression (DuckLake forward compatibility)
    pub default_value: Option<String>,
    /// Parent column ID for nested columns (DuckLake forward compatibility)
    pub parent_column: Option<i64>,
    /// Type of the default value (DuckLake forward compatibility)
    pub default_value_type: Option<String>,
    /// SQL dialect for the default value (DuckLake forward compatibility)
    pub default_value_dialect: Option<String>,
}

impl ColumnDef {
    /// Returns the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the DuckLake type string.
    pub fn ducklake_type(&self) -> &str {
        &self.ducklake_type
    }

    /// Returns whether this column allows NULL values.
    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    /// Create a new column definition.
    ///
    /// Validates that `ducklake_type` is a recognized DuckLake type string by converting
    /// it to an Arrow DataType. Returns an error if the type is invalid or unsupported.
    pub fn new(
        name: impl Into<String>,
        ducklake_type: impl Into<String>,
        is_nullable: bool,
    ) -> Result<Self> {
        let ducklake_type = ducklake_type.into();
        // Validate the type string by attempting to convert it to an Arrow type.
        // We discard the result; we only care that the conversion succeeds.
        ducklake_to_arrow_type(&ducklake_type)?;
        Ok(Self {
            name: name.into(),
            ducklake_type,
            is_nullable,
            initial_default: None,
            default_value: None,
            parent_column: None,
            default_value_type: None,
            default_value_dialect: None,
        })
    }

    /// Create a column definition from an Arrow DataType.
    ///
    /// This is a convenience constructor that converts the Arrow type to a DuckLake type string.
    /// The resulting DuckLake type is guaranteed to be valid since it was derived from a known
    /// Arrow type.
    pub fn from_arrow(
        name: impl Into<String>,
        data_type: &DataType,
        is_nullable: bool,
    ) -> Result<Self> {
        let ducklake_type = arrow_to_ducklake_type(data_type)?;
        // We use direct struct construction here since the ducklake_type was just
        // produced by arrow_to_ducklake_type, so it is guaranteed to be valid.
        Ok(Self {
            name: name.into(),
            ducklake_type,
            is_nullable,
            initial_default: None,
            default_value: None,
            parent_column: None,
            default_value_type: None,
            default_value_dialect: None,
        })
    }
}

/// Information about a data file to register in the catalog.
///
/// This struct contains the metadata needed to register a Parquet file in the DuckLake catalog.
#[derive(Debug, Clone)]
pub struct DataFileInfo {
    /// Path to the file (relative to table path or absolute)
    pub path: String,
    /// Whether the path is relative to the table's path
    pub path_is_relative: bool,
    /// Size of the file in bytes
    pub file_size_bytes: i64,
    /// Size of the Parquet footer in bytes (optimization hint for reads)
    pub footer_size: Option<i64>,
    /// Number of records in the file
    pub record_count: i64,
    /// Per-column statistics for query optimization (R4-S-005)
    pub column_stats: Vec<ColumnStatInfo>,
}

impl DataFileInfo {
    /// Create a new data file info with relative path.
    pub fn new(path: impl Into<String>, file_size_bytes: i64, record_count: i64) -> Self {
        Self {
            path: path.into(),
            path_is_relative: true,
            file_size_bytes,
            footer_size: None,
            record_count,
            column_stats: Vec::new(),
        }
    }

    /// Set the footer size for read optimization.
    #[must_use]
    pub fn with_footer_size(mut self, footer_size: i64) -> Self {
        self.footer_size = Some(footer_size);
        self
    }

    /// Mark this file as having an absolute path.
    #[must_use]
    pub fn with_absolute_path(mut self) -> Self {
        self.path_is_relative = false;
        self
    }

    /// Attach per-column statistics for this data file.
    #[must_use]
    pub fn with_column_stats(mut self, stats: Vec<ColumnStatInfo>) -> Self {
        self.column_stats = stats;
        self
    }
}

/// Per-column statistics for a data file.
///
/// Stores min/max values (as strings) and null count for a single column
/// within a data file. Used for query optimization (file pruning).
#[derive(Debug, Clone)]
pub struct ColumnStatInfo {
    /// Column ID in the catalog
    pub column_id: i64,
    /// Number of null values in this column for this file
    pub null_count: Option<i64>,
    /// Minimum value as a string (type-specific serialization)
    pub min_value: Option<String>,
    /// Maximum value as a string (type-specific serialization)
    pub max_value: Option<String>,
}

/// Information about a delete file to register in the catalog.
///
/// Delete files contain (file_path: VARCHAR, pos: INT64) records that
/// identify which rows in a data file have been deleted.
#[derive(Debug, Clone)]
pub struct DeleteFileInfo {
    /// ID of the data file this delete file applies to
    pub data_file_id: i64,
    /// Path to the delete file (relative to table path or absolute)
    pub path: String,
    /// Whether the path is relative to the table's path
    pub path_is_relative: bool,
    /// Size of the delete file in bytes
    pub file_size_bytes: i64,
    /// Size of the Parquet footer in bytes (optimization hint for reads)
    pub footer_size: Option<i64>,
    /// Number of deleted rows recorded in this file
    pub delete_count: i64,
}

impl DeleteFileInfo {
    /// Create a new delete file info with relative path.
    pub fn new(
        data_file_id: i64,
        path: impl Into<String>,
        file_size_bytes: i64,
        delete_count: i64,
    ) -> Self {
        Self {
            data_file_id,
            path: path.into(),
            path_is_relative: true,
            file_size_bytes,
            footer_size: None,
            delete_count,
        }
    }

    /// Set the footer size for read optimization.
    #[must_use]
    pub fn with_footer_size(mut self, footer_size: i64) -> Self {
        self.footer_size = Some(footer_size);
        self
    }
}

/// Entry for atomically replacing table files (end old + register new in one transaction).
///
/// Used by `MetadataWriter::replace_table_files` to batch all file registrations
/// (data file, column stats, partition values) into a single atomic operation.
/// Column stats are carried in `file_info.column_stats`.
#[derive(Debug)]
pub struct ReplaceFileEntry {
    /// Data file metadata (includes column_stats)
    pub file_info: DataFileInfo,
    /// Partition values as (key_index, value) pairs
    pub partition_values: Vec<(i32, Option<String>)>,
}

/// Result of a write operation.
#[derive(Debug)]
pub struct WriteResult {
    /// Snapshot ID of the write operation
    pub snapshot_id: i64,
    /// Table ID (may be newly created)
    pub table_id: i64,
    /// Schema ID (may be newly created)
    pub schema_id: i64,
    /// Number of files written
    pub files_written: usize,
    /// Total records written
    pub records_written: i64,
    /// ID of the last data file registered (used for partition value registration)
    pub last_data_file_id: i64,
}

/// Result of a transactional write setup operation.
#[derive(Debug)]
pub struct WriteSetupResult {
    /// Snapshot ID created for this write
    pub snapshot_id: i64,
    /// Schema ID (may be newly created)
    pub schema_id: i64,
    /// Table ID (may be newly created)
    pub table_id: i64,
    /// Column IDs in order
    pub column_ids: Vec<i64>,
}

/// Trait for writing metadata to DuckLake catalogs.
///
/// Implementations must be thread-safe (`Send + Sync`).
pub trait MetadataWriter: Send + Sync + std::fmt::Debug {
    /// Create a new snapshot and return its ID.
    fn create_snapshot(&self) -> Result<i64>;

    /// Get or create a schema, returning `(schema_id, was_created)`.
    fn get_or_create_schema(
        &self,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)>;

    /// Get or create a table, returning `(table_id, was_created)`.
    fn get_or_create_table(
        &self,
        schema_id: i64,
        name: &str,
        path: Option<&str>,
        snapshot_id: i64,
    ) -> Result<(i64, bool)>;

    /// Set columns for a table, returning assigned column IDs.
    /// Ends existing columns using end_snapshot pattern for time travel.
    fn set_columns(
        &self,
        table_id: i64,
        columns: &[ColumnDef],
        snapshot_id: i64,
    ) -> Result<Vec<i64>>;

    /// Register column-level statistics for a data file.
    ///
    /// Stores per-column min/max values and null counts, used for
    /// file-level pruning during query planning.
    ///
    /// Default implementation is a no-op for backward compatibility.
    fn register_column_stats(
        &self,
        _data_file_id: i64,
        _table_id: i64,
        _stats: &[ColumnStatInfo],
    ) -> Result<()> {
        Ok(())
    }

    /// Register a new data file. Returns the assigned data_file_id.
    fn register_data_file(
        &self,
        table_id: i64,
        snapshot_id: i64,
        file: &DataFileInfo,
    ) -> Result<i64>;

    /// End all existing data files for a table. Returns count of files ended.
    fn end_table_files(&self, table_id: i64, snapshot_id: i64) -> Result<u64>;

    /// Get the data path from catalog metadata.
    fn get_data_path(&self) -> Result<String>;

    /// Set the data path in catalog metadata.
    fn set_data_path(&self, path: &str) -> Result<()>;

    /// Initialize DuckLake schema tables if they don't exist.
    fn initialize_schema(&self) -> Result<()>;

    /// Record changes_made for a snapshot in ducklake_snapshot_changes.
    ///
    /// Used by DML operations to record what changed (e.g., "deleted_from_table:1").
    /// Default implementation is a no-op for backward compatibility.
    fn record_snapshot_changes(&self, _snapshot_id: i64, _changes_made: &str) -> Result<()> {
        Ok(())
    }

    /// Atomically set up catalog metadata for a write operation.
    /// Creates snapshot, schema, table, columns in a single transaction.
    ///
    /// Does NOT end existing data files for Replace mode. The caller must
    /// call `end_table_files()` separately after the Parquet upload succeeds
    /// to prevent data loss on upload failure.
    fn begin_write_transaction(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
    ) -> Result<WriteSetupResult>;

    /// Register a new delete file for a data file. Returns the assigned delete_file_id.
    ///
    /// If the data file already has an active delete file, the existing one should
    /// be ended (set end_snapshot) before registering the new one.
    fn register_delete_file(
        &self,
        table_id: i64,
        snapshot_id: i64,
        file: &DeleteFileInfo,
    ) -> Result<i64>;

    /// Atomically register multiple delete files and data files for DML operations.
    ///
    /// Ensures that all metadata registrations for a single DML operation
    /// (DELETE, UPDATE, MERGE) are committed atomically. If any registration
    /// fails, none of them take effect.
    ///
    /// Default implementation calls individual methods (non-atomic, for backward
    /// compatibility). Backends should override for true atomicity.
    fn register_dml_files(
        &self,
        table_id: i64,
        snapshot_id: i64,
        delete_files: &[DeleteFileInfo],
        data_files: &[DataFileInfo],
    ) -> Result<()> {
        for file in delete_files {
            self.register_delete_file(table_id, snapshot_id, file)?;
        }
        for file in data_files {
            self.register_data_file(table_id, snapshot_id, file)?;
        }
        Ok(())
    }

    /// Atomically replace table files: end existing files and register new ones.
    ///
    /// Ends all existing data files for the table, then registers each new file
    /// with its column stats and partition values, all within a single transaction.
    ///
    /// Returns the data_file_id for each registered file.
    ///
    /// # Warning: Default implementation is NOT atomic
    ///
    /// The default implementation calls `end_table_files` followed by individual
    /// `register_data_file` / `register_column_stats` / `register_file_partition_value`
    /// calls **without** a wrapping transaction.  If a failure occurs mid-way, the
    /// table metadata will be left in an inconsistent state (some files ended, some
    /// new files registered).  Backends **should** override this method to wrap the
    /// entire operation in a single transaction for true atomicity.
    fn replace_table_files(
        &self,
        table_id: i64,
        snapshot_id: i64,
        files: &[ReplaceFileEntry],
    ) -> Result<Vec<i64>> {
        self.end_table_files(table_id, snapshot_id)?;
        let mut ids = Vec::with_capacity(files.len());
        for entry in files {
            let data_file_id = self.register_data_file(table_id, snapshot_id, &entry.file_info)?;
            if !entry.file_info.column_stats.is_empty() {
                self.register_column_stats(data_file_id, table_id, &entry.file_info.column_stats)?;
            }
            for (key_index, val) in &entry.partition_values {
                self.register_file_partition_value(
                    data_file_id,
                    table_id,
                    *key_index,
                    val.as_deref(),
                )?;
            }
            ids.push(data_file_id);
        }
        Ok(ids)
    }

    /// Atomically append new files to a table (without ending existing files).
    ///
    /// Registers each new file with its column stats and partition values,
    /// all within a single transaction. Unlike `replace_table_files`, this does
    /// NOT end existing data files — it only adds new ones.
    ///
    /// Returns the data_file_id for each registered file.
    ///
    /// # Warning: Default implementation is NOT atomic
    ///
    /// The default implementation calls individual `register_data_file` /
    /// `register_column_stats` / `register_file_partition_value` calls
    /// **without** a wrapping transaction. Backends **should** override this
    /// method to wrap the entire operation in a single transaction.
    fn append_table_files(
        &self,
        table_id: i64,
        snapshot_id: i64,
        files: &[ReplaceFileEntry],
    ) -> Result<Vec<i64>> {
        let mut ids = Vec::with_capacity(files.len());
        for entry in files {
            let data_file_id = self.register_data_file(table_id, snapshot_id, &entry.file_info)?;
            if !entry.file_info.column_stats.is_empty() {
                self.register_column_stats(data_file_id, table_id, &entry.file_info.column_stats)?;
            }
            for (key_index, val) in &entry.partition_values {
                self.register_file_partition_value(
                    data_file_id,
                    table_id,
                    *key_index,
                    val.as_deref(),
                )?;
            }
            ids.push(data_file_id);
        }
        Ok(ids)
    }

    /// Drop a table by setting its end_snapshot.
    /// Creates a new snapshot and marks the table as dropped.
    /// Data files are NOT deleted (preserved for time travel).
    /// Also ends all active columns for this table.
    fn drop_table(&self, table_id: i64) -> Result<i64>;

    /// Drop a schema by setting its end_snapshot.
    /// Creates a new snapshot and marks the schema as dropped.
    /// Returns the snapshot_id created for the drop.
    fn drop_schema(&self, schema_id: i64) -> Result<i64>;

    /// List active table IDs in a schema (tables with no end_snapshot).
    fn list_active_table_ids(&self, schema_id: i64) -> Result<Vec<i64>>;

    /// Begin a write transaction with conflict detection.
    ///
    /// Like `begin_write_transaction`, but checks for conflicting changes
    /// (e.g., table drops) that occurred after `since_snapshot`. If a conflict
    /// is detected, returns `Err(TransactionConflict)`.
    ///
    /// Default implementation delegates to `begin_write_transaction` without
    /// conflict checking.
    fn begin_checked_write_transaction(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        mode: WriteMode,
        since_snapshot: i64,
    ) -> Result<WriteSetupResult> {
        let _ = since_snapshot;
        self.begin_write_transaction(schema_name, table_name, columns, mode)
    }

    /// Drop a table with conflict detection.
    ///
    /// Like `drop_table`, but checks that the table hasn't been dropped by
    /// another transaction since `since_snapshot`.
    ///
    /// Default implementation delegates to `drop_table` without conflict checking.
    fn drop_table_checked(&self, table_id: i64, since_snapshot: i64) -> Result<i64> {
        let _ = since_snapshot;
        self.drop_table(table_id)
    }

    /// Drop a schema with conflict detection.
    ///
    /// Like `drop_schema`, but checks that the schema hasn't been dropped by
    /// another transaction since `since_snapshot`.
    ///
    /// Default implementation delegates to `drop_schema` without conflict checking.
    fn drop_schema_checked(&self, schema_id: i64, since_snapshot: i64) -> Result<i64> {
        let _ = since_snapshot;
        self.drop_schema(schema_id)
    }

    /// Apply an ALTER TABLE operation to a table.
    ///
    /// Creates a new snapshot and applies the column change.
    /// Returns the snapshot_id created for the alter.
    fn alter_table(&self, table_id: i64, op: &AlterTableOp) -> Result<i64>;

    /// Get active columns for a table as (name, type, nullable) tuples.
    ///
    /// Returns columns ordered by column_order. Only returns columns
    /// that have no end_snapshot (i.e., currently active).
    fn get_active_columns(&self, table_id: i64) -> Result<Vec<(String, String, bool)>>;

    /// Rename a table in the catalog.
    ///
    /// Creates a new snapshot, ends the existing table row, and inserts a new row
    /// with the updated name. The table's physical path does NOT change.
    /// Returns the snapshot_id created for the rename.
    fn rename_table(&self, table_id: i64, new_name: &str) -> Result<i64>;

    /// Set or update a comment on a table.
    ///
    /// Stores the comment in `ducklake_tag` with key='comment'.
    /// If a comment already exists, it is ended and replaced.
    /// Returns the snapshot_id created for the comment.
    fn set_table_comment(&self, table_id: i64, comment: &str) -> Result<i64>;

    /// Set or update a comment on a column.
    ///
    /// Stores the comment in `ducklake_column_tag` with key='comment'.
    /// If a comment already exists, it is ended and replaced.
    /// Returns the snapshot_id created for the comment.
    fn set_column_comment(&self, table_id: i64, column_name: &str, comment: &str) -> Result<i64>;

    /// Create a view in the catalog.
    /// Creates a new snapshot, stores the view SQL definition, and returns (view_id, snapshot_id).
    fn create_view(&self, schema_id: i64, view_name: &str, sql: &str) -> Result<(i64, i64)>;

    /// Drop a view by setting its end_snapshot.
    /// Creates a new snapshot and marks the view as dropped.
    /// Returns the snapshot_id created for the drop.
    fn drop_view(&self, view_id: i64) -> Result<i64>;

    /// Rename a view in the catalog.
    ///
    /// Creates a new snapshot, ends the existing view row, and inserts a new row
    /// with the updated name. The view's SQL definition does NOT change.
    /// Returns the snapshot_id created for the rename.
    fn rename_view(&self, view_id: i64, new_name: &str) -> Result<i64>;

    /// Register a partition value for a data file.
    ///
    /// Records that a specific data file has a given partition value at the
    /// specified partition key index. Called after writing each partitioned file.
    fn register_file_partition_value(
        &self,
        data_file_id: i64,
        table_id: i64,
        partition_key_index: i32,
        partition_value: Option<&str>,
    ) -> Result<()> {
        let _ = (data_file_id, table_id, partition_key_index, partition_value);
        Ok(())
    }

    /// Get the active partition columns for a table.
    ///
    /// Returns (column_name, column_id, transform) tuples for all active
    /// partition columns, ordered by partition_key_index.
    fn get_active_partition_columns(
        &self,
        _table_id: i64,
    ) -> Result<Vec<(String, i64, Option<String>)>> {
        Ok(Vec::new())
    }

    /// Look up a table by schema and table name, returning its table_id.
    ///
    /// Returns `None` if the table does not exist.
    fn find_table_id(&self, _schema_name: &str, _table_name: &str) -> Result<Option<i64>> {
        Ok(None)
    }

    // ==================== Data inlining methods ====================

    /// Get the data inlining row limit from catalog metadata.
    ///
    /// Returns `Some(limit)` if inlining is enabled (limit > 0),
    /// `None` if inlining is not configured.
    fn get_data_inlining_row_limit(&self) -> Result<Option<i64>> {
        Ok(None)
    }

    /// Get the current number of inlined rows for a table.
    fn get_inlined_row_count(&self, _table_id: i64) -> Result<i64> {
        Ok(0)
    }

    /// Store data inline in the catalog database.
    ///
    /// Creates the inlined data table if needed, registers it in
    /// `ducklake_inlined_data_tables`, and inserts the rows.
    /// Returns the number of rows stored.
    fn store_inlined_data(
        &self,
        _table_id: i64,
        _snapshot_id: i64,
        _columns: &[ColumnDef],
        _rows: &[InlinedDataRow],
    ) -> Result<i64> {
        Err(crate::DuckLakeError::Unsupported(
            "Inlined data storage not supported by this backend".into(),
        ))
    }

    /// Read all active inlined data rows for a table.
    ///
    /// Returns the same format as `MetadataProvider::get_inlined_data`.
    /// Used when flushing inlined data to Parquet.
    fn read_inlined_data(&self, _table_id: i64) -> Result<Vec<InlinedDataRow>> {
        Err(crate::DuckLakeError::Unsupported(
            "Inlined data read not supported by this backend".into(),
        ))
    }

    /// Remove all active inlined data for a table (set end_snapshot).
    ///
    /// Called after flushing inlined data to Parquet.
    fn clear_inlined_data(&self, _table_id: i64, _snapshot_id: i64) -> Result<()> {
        Err(crate::DuckLakeError::Unsupported(
            "Inlined data clear not supported by this backend".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DuckLakeError;

    #[test]
    fn test_column_def_new() {
        let col = ColumnDef::new("test_col", "int32", true).unwrap();
        assert_eq!(col.name, "test_col");
        assert_eq!(col.ducklake_type, "int32");
        assert!(col.is_nullable);
    }

    #[test]
    fn test_column_def_new_valid_types() {
        // Various valid type strings should be accepted
        assert!(ColumnDef::new("a", "int32", true).is_ok());
        assert!(ColumnDef::new("b", "varchar", false).is_ok());
        assert!(ColumnDef::new("c", "boolean", true).is_ok());
        assert!(ColumnDef::new("d", "float64", true).is_ok());
        assert!(ColumnDef::new("e", "decimal(10,2)", true).is_ok());
        assert!(ColumnDef::new("f", "timestamp", true).is_ok());
        assert!(ColumnDef::new("g", "date", true).is_ok());
        assert!(ColumnDef::new("h", "bigint", true).is_ok());
        assert!(ColumnDef::new("i", "text", true).is_ok());
    }

    #[test]
    fn test_column_def_new_invalid_type_rejected() {
        let result = ColumnDef::new("col", "not_a_type", true);
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert_eq!(msg, "not_a_type");
            },
            other => panic!("Expected UnsupportedType error, got {:?}", other),
        }
    }

    #[test]
    fn test_column_def_new_empty_type_rejected() {
        let result = ColumnDef::new("col", "", true);
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(_)) => {},
            other => panic!("Expected UnsupportedType error, got {:?}", other),
        }
    }

    #[test]
    fn test_column_def_from_arrow() {
        let col = ColumnDef::from_arrow("id", &DataType::Int64, false).unwrap();
        assert_eq!(col.name, "id");
        assert_eq!(col.ducklake_type, "int64");
        assert!(!col.is_nullable);
    }

    #[test]
    fn test_data_file_info_new() {
        let file = DataFileInfo::new("test.parquet", 1024, 100);
        assert_eq!(file.path, "test.parquet");
        assert!(file.path_is_relative);
        assert_eq!(file.file_size_bytes, 1024);
        assert_eq!(file.record_count, 100);
        assert!(file.footer_size.is_none());
    }

    #[test]
    fn test_data_file_info_with_footer_size() {
        let file = DataFileInfo::new("test.parquet", 1024, 100).with_footer_size(256);
        assert_eq!(file.footer_size, Some(256));
    }

    #[test]
    fn test_data_file_info_with_absolute_path() {
        let file = DataFileInfo::new("/absolute/path.parquet", 1024, 100).with_absolute_path();
        assert!(!file.path_is_relative);
    }

    #[test]
    fn test_type_promotion_allowed() {
        // Signed integer widening
        assert!(is_type_promotion_allowed("int8", "int16"));
        assert!(is_type_promotion_allowed("int8", "int32"));
        assert!(is_type_promotion_allowed("int8", "int64"));
        assert!(is_type_promotion_allowed("int16", "int32"));
        assert!(is_type_promotion_allowed("int16", "int64"));
        assert!(is_type_promotion_allowed("int32", "int64"));

        // Unsigned integer widening
        assert!(is_type_promotion_allowed("uint8", "uint16"));
        assert!(is_type_promotion_allowed("uint16", "uint32"));
        assert!(is_type_promotion_allowed("uint32", "uint64"));

        // Unsigned → signed cross-promotion
        assert!(is_type_promotion_allowed("uint8", "int16"));
        assert!(is_type_promotion_allowed("uint16", "int32"));
        assert!(is_type_promotion_allowed("uint32", "int64"));

        // Float widening
        assert!(is_type_promotion_allowed("float", "double"));
    }

    #[test]
    fn test_type_promotion_not_allowed() {
        // Narrowing
        assert!(!is_type_promotion_allowed("int64", "int32"));
        assert!(!is_type_promotion_allowed("int32", "int16"));
        assert!(!is_type_promotion_allowed("double", "float"));

        // Same type
        assert!(!is_type_promotion_allowed("int32", "int32"));

        // Incompatible types
        assert!(!is_type_promotion_allowed("varchar", "int32"));
        assert!(!is_type_promotion_allowed("int32", "varchar"));
        assert!(!is_type_promotion_allowed("float", "int32"));
    }
}
