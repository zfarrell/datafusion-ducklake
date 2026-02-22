//! Metadata writer trait and common types for DuckLake catalog writes.
//!
//! This module provides the `MetadataWriter` trait for writing metadata to DuckLake catalogs,
//! along with helper types for column definitions and data file registration.

use crate::Result;

/// Write mode for table operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Drop existing data and replace with new data
    Replace,
    /// Keep existing data and append new records
    Append,
}
use crate::types::arrow_to_ducklake_type;
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
    )
}

/// Column definition for creating or updating a table's schema.
///
/// Unlike `DuckLakeTableColumn` (used for reading), this struct doesn't have a `column_id`
/// field since IDs are assigned by the catalog during write operations.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// DuckLake type string (e.g., "varchar", "int64", "decimal(10,2)")
    pub ducklake_type: String,
    /// Whether this column allows NULL values
    pub is_nullable: bool,
}

impl ColumnDef {
    /// Create a new column definition.
    pub fn new(
        name: impl Into<String>,
        ducklake_type: impl Into<String>,
        is_nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            ducklake_type: ducklake_type.into(),
            is_nullable,
        }
    }

    /// Create a column definition from an Arrow DataType.
    ///
    /// This is a convenience constructor that converts the Arrow type to a DuckLake type string.
    pub fn from_arrow(
        name: impl Into<String>,
        data_type: &DataType,
        is_nullable: bool,
    ) -> Result<Self> {
        let ducklake_type = arrow_to_ducklake_type(data_type)?;
        Ok(Self::new(name, ducklake_type, is_nullable))
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
        }
    }

    /// Set the footer size for read optimization.
    pub fn with_footer_size(mut self, footer_size: i64) -> Self {
        self.footer_size = Some(footer_size);
        self
    }

    /// Mark this file as having an absolute path.
    pub fn with_absolute_path(mut self) -> Self {
        self.path_is_relative = false;
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
    pub fn with_footer_size(mut self, footer_size: i64) -> Self {
        self.footer_size = Some(footer_size);
        self
    }
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

    /// Atomically set up catalog metadata for a write operation.
    /// Creates snapshot, schema, table, columns in a single transaction.
    /// If mode is `WriteMode::Replace`, ends existing data files.
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

    /// Create a view in the catalog.
    /// Creates a new snapshot, stores the view SQL definition, and returns (view_id, snapshot_id).
    fn create_view(&self, schema_id: i64, view_name: &str, sql: &str) -> Result<(i64, i64)>;

    /// Drop a view by setting its end_snapshot.
    /// Creates a new snapshot and marks the view as dropped.
    /// Returns the snapshot_id created for the drop.
    fn drop_view(&self, view_id: i64) -> Result<i64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_def_new() {
        let col = ColumnDef::new("test_col", "int32", true);
        assert_eq!(col.name, "test_col");
        assert_eq!(col.ducklake_type, "int32");
        assert!(col.is_nullable);
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
