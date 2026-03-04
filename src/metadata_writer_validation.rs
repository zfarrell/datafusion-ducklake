//! Shared validation logic for MetadataWriter implementations.
//!
//! Extracts duplicated validation code from SQLite, Postgres, and MySQL writers
//! into a single source of truth. Backend-specific SQL execution remains in each writer.

use crate::Result;
use crate::error::DuckLakeError;
use crate::metadata_writer::{
    AlterTableOp, ColumnDef, PartitionColumnDef, WriteMode, is_type_promotion_allowed,
};

/// DB-independent parsed column row for validation.
#[derive(Debug, Clone)]
pub(crate) struct ActiveColumnInfo {
    pub column_id: i64,
    pub column_name: String,
    pub column_type: String,
    pub column_order: i64,
    pub is_nullable: bool,
    pub initial_default: Option<String>,
    pub default_value: Option<String>,
    pub parent_column: Option<i64>,
    pub default_value_type: Option<String>,
    pub default_value_dialect: Option<String>,
}

/// Validation result telling the caller what SQL to execute after validation.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum AlterTableAction {
    /// Insert a new column at the given order.
    InsertColumn {
        column_name: String,
        column_type: String,
        column_order: i64,
        is_nullable: bool,
    },
    /// End (soft-delete) an existing column.
    EndColumn {
        column_id: i64,
    },
    /// End an existing column and replace it with new values.
    ReplaceColumn {
        end_column_id: i64,
        column_name: String,
        column_type: String,
        column_order: i64,
        is_nullable: bool,
        initial_default: Option<String>,
        default_value: Option<String>,
        parent_column: Option<i64>,
        default_value_type: Option<String>,
        default_value_dialect: Option<String>,
    },
    /// Set partition columns (validated column references + column_ids).
    SetPartitionedBy {
        /// (column_id, column_name, transform) for each partition column
        partition_columns: Vec<(i64, String, Option<String>)>,
    },
}

// Re-export from metadata_provider to avoid duplication (R5-S-044)
pub(crate) use crate::metadata_provider::quote_identifier;

/// Validate that column names are unique within the provided column list.
///
/// Returns an error if any two columns share the same name (case-sensitive).
/// Should be called before inserting columns into the catalog.
pub(crate) fn validate_no_duplicate_columns(columns: &[ColumnDef]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for col in columns {
        if !seen.insert(&col.name) {
            return Err(DuckLakeError::InvalidConfig(format!(
                "Duplicate column name '{}' in table definition",
                col.name
            )));
        }
    }
    Ok(())
}

/// Validate schema evolution rules for append mode.
///
/// Checks that:
/// - Existing columns have matching types in the new schema
/// - New columns (not in existing schema) are nullable
/// - Columns removed from the new schema are allowed (implicit removal)
pub(crate) fn validate_schema_evolution(
    existing: &[(String, String, bool)],
    new: &[ColumnDef],
    mode: WriteMode,
) -> Result<()> {
    if mode != WriteMode::Append || existing.is_empty() {
        return Ok(());
    }

    use std::collections::HashMap;

    let existing_map: HashMap<&str, (&str, bool)> = existing
        .iter()
        .map(|(name, col_type, nullable)| (name.as_str(), (col_type.as_str(), *nullable)))
        .collect();

    for new_col in new.iter() {
        if let Some((existing_type, _existing_nullable)) = existing_map.get(new_col.name.as_str()) {
            if *existing_type != new_col.ducklake_type {
                return Err(DuckLakeError::InvalidConfig(format!(
                    "Schema evolution error: column '{}' has type '{}' in existing table but '{}' in new schema. Type changes are not allowed.",
                    new_col.name, existing_type, new_col.ducklake_type
                )));
            }
        } else if !new_col.is_nullable {
            return Err(DuckLakeError::InvalidConfig(format!(
                "Schema evolution error: new column '{}' must be nullable. Adding non-nullable columns is not allowed.",
                new_col.name
            )));
        }
    }

    Ok(())
}

/// Validate that a table has at least one active column.
pub(crate) fn validate_table_has_columns(columns: &[ActiveColumnInfo]) -> Result<()> {
    if columns.is_empty() {
        return Err(DuckLakeError::Internal(
            "Cannot alter table: no active columns found (table may be dropped)".to_string(),
        ));
    }
    Ok(())
}

/// Validate an ALTER TABLE operation and return the action to execute.
pub(crate) fn validate_alter_table(
    columns: &[ActiveColumnInfo],
    op: &AlterTableOp,
) -> Result<AlterTableAction> {
    match op {
        AlterTableOp::AddColumn {
            column,
        } => validate_add_column(columns, column),
        AlterTableOp::DropColumn {
            column_name,
        } => validate_drop_column(columns, column_name),
        AlterTableOp::RenameColumn {
            old_name,
            new_name,
        } => validate_rename_column(columns, old_name, new_name),
        AlterTableOp::AlterColumnType(alter_type) => {
            validate_alter_column_type(columns, &alter_type.column_name, &alter_type.new_type)
        },
        AlterTableOp::SetColumnDefault {
            column_name,
            default_value,
        } => validate_set_column_default(columns, column_name, default_value),
        AlterTableOp::DropColumnDefault {
            column_name,
        } => validate_drop_column_default(columns, column_name),
        AlterTableOp::SetNotNull {
            column_name,
        } => validate_set_not_null(columns, column_name),
        AlterTableOp::DropNotNull {
            column_name,
        } => validate_drop_not_null(columns, column_name),
        AlterTableOp::SetPartitionedBy {
            partition_columns,
        } => validate_set_partitioned_by(columns, partition_columns),
    }
}

fn validate_add_column(
    columns: &[ActiveColumnInfo],
    column: &ColumnDef,
) -> Result<AlterTableAction> {
    if !column.is_nullable {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Cannot add non-nullable column '{}': new columns must be nullable since existing rows have no value",
            column.name
        )));
    }

    for col in columns {
        if col.column_name == column.name {
            return Err(DuckLakeError::InvalidConfig(format!(
                "Column '{}' already exists in table",
                column.name
            )));
        }
    }

    let max_order = columns.iter().map(|c| c.column_order).max().unwrap_or(-1);

    Ok(AlterTableAction::InsertColumn {
        column_name: column.name.clone(),
        column_type: column.ducklake_type.clone(),
        column_order: max_order + 1,
        is_nullable: column.is_nullable,
    })
}

fn validate_drop_column(
    columns: &[ActiveColumnInfo],
    column_name: &str,
) -> Result<AlterTableAction> {
    if columns.len() == 1 {
        return Err(DuckLakeError::InvalidConfig(
            "Cannot drop column: table only has one column remaining".to_string(),
        ));
    }

    let target = columns.iter().find(|c| c.column_name == column_name);

    let Some(target_col) = target else {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Column '{}' not found in table",
            column_name
        )));
    };

    Ok(AlterTableAction::EndColumn {
        column_id: target_col.column_id,
    })
}

fn validate_rename_column(
    columns: &[ActiveColumnInfo],
    old_name: &str,
    new_name: &str,
) -> Result<AlterTableAction> {
    let target = columns.iter().find(|c| c.column_name == old_name);

    let Some(target_col) = target else {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Column '{}' not found in table",
            old_name
        )));
    };

    for col in columns {
        if col.column_name == new_name {
            return Err(DuckLakeError::InvalidConfig(format!(
                "Column '{}' already exists in table",
                new_name
            )));
        }
    }

    Ok(AlterTableAction::ReplaceColumn {
        end_column_id: target_col.column_id,
        column_name: new_name.to_string(),
        column_type: target_col.column_type.clone(),
        column_order: target_col.column_order,
        is_nullable: target_col.is_nullable,
        initial_default: target_col.initial_default.clone(),
        default_value: target_col.default_value.clone(),
        parent_column: target_col.parent_column,
        default_value_type: target_col.default_value_type.clone(),
        default_value_dialect: target_col.default_value_dialect.clone(),
    })
}

fn validate_alter_column_type(
    columns: &[ActiveColumnInfo],
    column_name: &str,
    new_type: &str,
) -> Result<AlterTableAction> {
    let target = columns.iter().find(|c| c.column_name == column_name);

    let Some(target_col) = target else {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Column '{}' not found in table",
            column_name
        )));
    };

    if !is_type_promotion_allowed(&target_col.column_type, new_type) {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Cannot change type of column '{}' from '{}' to '{}': only widening type promotions are allowed",
            column_name, target_col.column_type, new_type
        )));
    }

    Ok(AlterTableAction::ReplaceColumn {
        end_column_id: target_col.column_id,
        column_name: column_name.to_string(),
        column_type: new_type.to_string(),
        column_order: target_col.column_order,
        is_nullable: target_col.is_nullable,
        initial_default: target_col.initial_default.clone(),
        default_value: target_col.default_value.clone(),
        parent_column: target_col.parent_column,
        default_value_type: target_col.default_value_type.clone(),
        default_value_dialect: target_col.default_value_dialect.clone(),
    })
}

fn validate_set_column_default(
    columns: &[ActiveColumnInfo],
    column_name: &str,
    default_value: &str,
) -> Result<AlterTableAction> {
    let target = columns.iter().find(|c| c.column_name == column_name);

    let Some(target_col) = target else {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Column '{}' not found in table",
            column_name
        )));
    };

    Ok(AlterTableAction::ReplaceColumn {
        end_column_id: target_col.column_id,
        column_name: target_col.column_name.clone(),
        column_type: target_col.column_type.clone(),
        column_order: target_col.column_order,
        is_nullable: target_col.is_nullable,
        initial_default: target_col.initial_default.clone(),
        default_value: Some(default_value.to_string()),
        parent_column: target_col.parent_column,
        default_value_type: target_col.default_value_type.clone(),
        default_value_dialect: target_col.default_value_dialect.clone(),
    })
}

fn validate_drop_column_default(
    columns: &[ActiveColumnInfo],
    column_name: &str,
) -> Result<AlterTableAction> {
    let target = columns.iter().find(|c| c.column_name == column_name);

    let Some(target_col) = target else {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Column '{}' not found in table",
            column_name
        )));
    };

    Ok(AlterTableAction::ReplaceColumn {
        end_column_id: target_col.column_id,
        column_name: target_col.column_name.clone(),
        column_type: target_col.column_type.clone(),
        column_order: target_col.column_order,
        is_nullable: target_col.is_nullable,
        initial_default: target_col.initial_default.clone(),
        default_value: None,
        parent_column: target_col.parent_column,
        default_value_type: target_col.default_value_type.clone(),
        default_value_dialect: target_col.default_value_dialect.clone(),
    })
}

// Known limitation: SET NOT NULL only updates catalog metadata without scanning
// existing data for nulls. If the column already contains NULL values, the
// constraint will be recorded but not enforced retroactively. This matches
// DuckDB's DuckLake behavior where the catalog is the source of truth for
// constraints, but a full table scan would be needed to truly validate.
fn validate_set_not_null(
    columns: &[ActiveColumnInfo],
    column_name: &str,
) -> Result<AlterTableAction> {
    let target = columns.iter().find(|c| c.column_name == column_name);

    let Some(target_col) = target else {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Column '{}' not found in table",
            column_name
        )));
    };

    tracing::warn!(
        column = %column_name,
        "SET NOT NULL does not validate existing data — constraint may be violated if column already contains nulls"
    );

    Ok(AlterTableAction::ReplaceColumn {
        end_column_id: target_col.column_id,
        column_name: target_col.column_name.clone(),
        column_type: target_col.column_type.clone(),
        column_order: target_col.column_order,
        is_nullable: false,
        initial_default: target_col.initial_default.clone(),
        default_value: target_col.default_value.clone(),
        parent_column: target_col.parent_column,
        default_value_type: target_col.default_value_type.clone(),
        default_value_dialect: target_col.default_value_dialect.clone(),
    })
}

fn validate_drop_not_null(
    columns: &[ActiveColumnInfo],
    column_name: &str,
) -> Result<AlterTableAction> {
    let target = columns.iter().find(|c| c.column_name == column_name);

    let Some(target_col) = target else {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Column '{}' not found in table",
            column_name
        )));
    };

    Ok(AlterTableAction::ReplaceColumn {
        end_column_id: target_col.column_id,
        column_name: target_col.column_name.clone(),
        column_type: target_col.column_type.clone(),
        column_order: target_col.column_order,
        is_nullable: true,
        initial_default: target_col.initial_default.clone(),
        default_value: target_col.default_value.clone(),
        parent_column: target_col.parent_column,
        default_value_type: target_col.default_value_type.clone(),
        default_value_dialect: target_col.default_value_dialect.clone(),
    })
}

/// Allowed partition transform values. Empty/None maps to identity.
const ALLOWED_PARTITION_TRANSFORMS: &[&str] = &["identity", "year", "month", "day", "hour"];

fn validate_set_partitioned_by(
    columns: &[ActiveColumnInfo],
    partition_columns: &[PartitionColumnDef],
) -> Result<AlterTableAction> {
    if partition_columns.is_empty() {
        return Err(DuckLakeError::InvalidConfig(
            "SET PARTITIONED BY requires at least one column".to_string(),
        ));
    }

    // Check for duplicate partition columns
    let mut seen_columns = std::collections::HashSet::new();
    for pc in partition_columns {
        if !seen_columns.insert(&pc.column_name) {
            return Err(DuckLakeError::InvalidConfig(format!(
                "Duplicate partition column '{}'",
                pc.column_name
            )));
        }
    }

    let mut validated = Vec::with_capacity(partition_columns.len());
    for pc in partition_columns {
        // Validate transform value against allowlist
        if let Some(ref transform) = pc.transform {
            let t = transform.to_lowercase();
            if !t.is_empty() && !ALLOWED_PARTITION_TRANSFORMS.contains(&t.as_str()) {
                return Err(DuckLakeError::InvalidConfig(format!(
                    "Invalid partition transform '{}' for column '{}'. Allowed transforms: {}",
                    transform,
                    pc.column_name,
                    ALLOWED_PARTITION_TRANSFORMS.join(", ")
                )));
            }
        }

        let target = columns.iter().find(|c| c.column_name == pc.column_name);
        let Some(target_col) = target else {
            return Err(DuckLakeError::InvalidConfig(format!(
                "Partition column '{}' not found in table",
                pc.column_name
            )));
        };
        validated.push((
            target_col.column_id,
            target_col.column_name.clone(),
            pc.transform.clone(),
        ));
    }

    Ok(AlterTableAction::SetPartitionedBy {
        partition_columns: validated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_columns(cols: &[(&str, &str, i64, bool)]) -> Vec<ActiveColumnInfo> {
        cols.iter()
            .enumerate()
            .map(|(i, (name, typ, order, nullable))| ActiveColumnInfo {
                column_id: i as i64 + 1,
                column_name: name.to_string(),
                column_type: typ.to_string(),
                column_order: *order,
                is_nullable: *nullable,
                initial_default: None,
                default_value: None,
                parent_column: None,
                default_value_type: None,
                default_value_dialect: None,
            })
            .collect()
    }

    // --- validate_schema_evolution tests ---

    #[test]
    fn test_schema_evolution_replace_mode_skips_validation() {
        let existing = vec![("id".into(), "int64".into(), false)];
        let new = vec![ColumnDef::new("id", "varchar", false).unwrap()]; // type mismatch
        assert!(validate_schema_evolution(&existing, &new, WriteMode::Replace).is_ok());
    }

    #[test]
    fn test_schema_evolution_empty_existing_skips_validation() {
        let existing: Vec<(String, String, bool)> = vec![];
        let new = vec![ColumnDef::new("id", "int64", false).unwrap()];
        assert!(validate_schema_evolution(&existing, &new, WriteMode::Append).is_ok());
    }

    #[test]
    fn test_schema_evolution_matching_types_ok() {
        let existing = vec![("id".into(), "int64".into(), false)];
        let new = vec![ColumnDef::new("id", "int64", false).unwrap()];
        assert!(validate_schema_evolution(&existing, &new, WriteMode::Append).is_ok());
    }

    #[test]
    fn test_schema_evolution_type_mismatch_fails() {
        let existing = vec![("id".into(), "int64".into(), false)];
        let new = vec![ColumnDef::new("id", "varchar", false).unwrap()];
        let err = validate_schema_evolution(&existing, &new, WriteMode::Append).unwrap_err();
        assert!(err.to_string().contains("Type changes are not allowed"));
    }

    #[test]
    fn test_schema_evolution_new_nullable_column_ok() {
        let existing = vec![("id".into(), "int64".into(), false)];
        let new = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        assert!(validate_schema_evolution(&existing, &new, WriteMode::Append).is_ok());
    }

    #[test]
    fn test_schema_evolution_new_non_nullable_column_fails() {
        let existing = vec![("id".into(), "int64".into(), false)];
        let new = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", false).unwrap(),
        ];
        let err = validate_schema_evolution(&existing, &new, WriteMode::Append).unwrap_err();
        assert!(err.to_string().contains("must be nullable"));
    }

    // --- validate_table_has_columns tests ---

    #[test]
    fn test_table_has_columns_ok() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        assert!(validate_table_has_columns(&columns).is_ok());
    }

    #[test]
    fn test_table_has_no_columns_fails() {
        assert!(validate_table_has_columns(&[]).is_err());
    }

    // --- validate_add_column tests ---

    #[test]
    fn test_add_nullable_column_ok() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("name", "varchar", true).unwrap(),
        };
        let action = validate_alter_table(&columns, &op).unwrap();
        match action {
            AlterTableAction::InsertColumn {
                column_order,
                ..
            } => assert_eq!(column_order, 1),
            _ => panic!("Expected InsertColumn"),
        }
    }

    #[test]
    fn test_add_non_nullable_column_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("name", "varchar", false).unwrap(),
        };
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    #[test]
    fn test_add_duplicate_column_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("id", "varchar", true).unwrap(),
        };
        let err = validate_alter_table(&columns, &op).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    // --- validate_drop_column tests ---

    #[test]
    fn test_drop_column_ok() {
        let columns = make_columns(&[("id", "int64", 0, false), ("name", "varchar", 1, true)]);
        let op = AlterTableOp::DropColumn {
            column_name: "name".into(),
        };
        let action = validate_alter_table(&columns, &op).unwrap();
        match action {
            AlterTableAction::EndColumn {
                column_id,
            } => assert_eq!(column_id, 2),
            _ => panic!("Expected EndColumn"),
        }
    }

    #[test]
    fn test_drop_last_column_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::DropColumn {
            column_name: "id".into(),
        };
        let err = validate_alter_table(&columns, &op).unwrap_err();
        assert!(err.to_string().contains("one column remaining"));
    }

    #[test]
    fn test_drop_nonexistent_column_fails() {
        let columns = make_columns(&[("id", "int64", 0, false), ("name", "varchar", 1, true)]);
        let op = AlterTableOp::DropColumn {
            column_name: "missing".into(),
        };
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    // --- validate_rename_column tests ---

    #[test]
    fn test_rename_column_ok() {
        let columns = make_columns(&[("id", "int64", 0, false), ("name", "varchar", 1, true)]);
        let op = AlterTableOp::RenameColumn {
            old_name: "name".into(),
            new_name: "full_name".into(),
        };
        let action = validate_alter_table(&columns, &op).unwrap();
        match action {
            AlterTableAction::ReplaceColumn {
                end_column_id,
                column_name,
                column_type,
                column_order,
                is_nullable,
                ..
            } => {
                assert_eq!(end_column_id, 2);
                assert_eq!(column_name, "full_name");
                assert_eq!(column_type, "varchar");
                assert_eq!(column_order, 1);
                assert!(is_nullable);
            },
            _ => panic!("Expected ReplaceColumn"),
        }
    }

    #[test]
    fn test_rename_to_existing_name_fails() {
        let columns = make_columns(&[("id", "int64", 0, false), ("name", "varchar", 1, true)]);
        let op = AlterTableOp::RenameColumn {
            old_name: "name".into(),
            new_name: "id".into(),
        };
        let err = validate_alter_table(&columns, &op).unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn test_rename_nonexistent_column_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::RenameColumn {
            old_name: "missing".into(),
            new_name: "new_name".into(),
        };
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    // --- validate_alter_column_type tests ---

    #[test]
    fn test_alter_column_type_widening_ok() {
        let columns = make_columns(&[("id", "int32", 0, false)]);
        let op = AlterTableOp::AlterColumnType(crate::metadata_writer::AlterColumnTypeOp {
            column_name: "id".into(),
            new_type: "int64".into(),
        });
        let action = validate_alter_table(&columns, &op).unwrap();
        match action {
            AlterTableAction::ReplaceColumn {
                column_type,
                ..
            } => assert_eq!(column_type, "int64"),
            _ => panic!("Expected ReplaceColumn"),
        }
    }

    #[test]
    fn test_alter_column_type_narrowing_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::AlterColumnType(crate::metadata_writer::AlterColumnTypeOp {
            column_name: "id".into(),
            new_type: "int32".into(),
        });
        let err = validate_alter_table(&columns, &op).unwrap_err();
        assert!(err.to_string().contains("widening type promotions"));
    }

    #[test]
    fn test_alter_nonexistent_column_type_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::AlterColumnType(crate::metadata_writer::AlterColumnTypeOp {
            column_name: "missing".into(),
            new_type: "int64".into(),
        });
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    // --- validate_no_duplicate_columns tests ---

    #[test]
    fn test_no_duplicate_columns_ok() {
        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        assert!(validate_no_duplicate_columns(&columns).is_ok());
    }

    #[test]
    fn test_duplicate_columns_fails() {
        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("id", "int64", false).unwrap(),
        ];
        let err = validate_no_duplicate_columns(&columns).unwrap_err();
        assert!(err.to_string().contains("Duplicate column name"));
    }

    #[test]
    fn test_duplicate_columns_different_types_fails() {
        let columns = vec![
            ColumnDef::new("x", "int64", false).unwrap(),
            ColumnDef::new("x", "varchar", true).unwrap(),
        ];
        let err = validate_no_duplicate_columns(&columns).unwrap_err();
        assert!(err.to_string().contains("Duplicate column name 'x'"));
    }

    // --- validate_set_column_default tests ---

    #[test]
    fn test_set_column_default_ok() {
        let columns = make_columns(&[("id", "int64", 0, false), ("name", "varchar", 1, true)]);
        let op = AlterTableOp::SetColumnDefault {
            column_name: "name".into(),
            default_value: "'unknown'".into(),
        };
        let action = validate_alter_table(&columns, &op).unwrap();
        match action {
            AlterTableAction::ReplaceColumn {
                default_value,
                column_name,
                ..
            } => {
                assert_eq!(default_value, Some("'unknown'".to_string()));
                assert_eq!(column_name, "name");
            },
            _ => panic!("Expected ReplaceColumn"),
        }
    }

    #[test]
    fn test_set_column_default_nonexistent_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::SetColumnDefault {
            column_name: "missing".into(),
            default_value: "0".into(),
        };
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    // --- validate_drop_column_default tests ---

    #[test]
    fn test_drop_column_default_ok() {
        let columns = make_columns(&[("id", "int64", 0, false), ("name", "varchar", 1, true)]);
        let op = AlterTableOp::DropColumnDefault {
            column_name: "name".into(),
        };
        let action = validate_alter_table(&columns, &op).unwrap();
        match action {
            AlterTableAction::ReplaceColumn {
                default_value,
                column_name,
                ..
            } => {
                assert!(default_value.is_none());
                assert_eq!(column_name, "name");
            },
            _ => panic!("Expected ReplaceColumn"),
        }
    }

    #[test]
    fn test_drop_column_default_nonexistent_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::DropColumnDefault {
            column_name: "missing".into(),
        };
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    // --- validate_set_not_null tests ---

    #[test]
    fn test_set_not_null_ok() {
        let columns = make_columns(&[("id", "int64", 0, false), ("name", "varchar", 1, true)]);
        let op = AlterTableOp::SetNotNull {
            column_name: "name".into(),
        };
        let action = validate_alter_table(&columns, &op).unwrap();
        match action {
            AlterTableAction::ReplaceColumn {
                is_nullable,
                column_name,
                ..
            } => {
                assert!(!is_nullable);
                assert_eq!(column_name, "name");
            },
            _ => panic!("Expected ReplaceColumn"),
        }
    }

    #[test]
    fn test_set_not_null_nonexistent_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::SetNotNull {
            column_name: "missing".into(),
        };
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    // --- validate_drop_not_null tests ---

    #[test]
    fn test_drop_not_null_ok() {
        let columns = make_columns(&[("id", "int64", 0, false), ("name", "varchar", 1, false)]);
        let op = AlterTableOp::DropNotNull {
            column_name: "name".into(),
        };
        let action = validate_alter_table(&columns, &op).unwrap();
        match action {
            AlterTableAction::ReplaceColumn {
                is_nullable,
                column_name,
                ..
            } => {
                assert!(is_nullable);
                assert_eq!(column_name, "name");
            },
            _ => panic!("Expected ReplaceColumn"),
        }
    }

    #[test]
    fn test_drop_not_null_nonexistent_fails() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::DropNotNull {
            column_name: "missing".into(),
        };
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    // --- validate_set_partitioned_by tests ---

    #[test]
    fn test_partition_valid_transforms() {
        let columns = make_columns(&[("id", "int64", 0, false), ("ts", "timestamp", 1, true)]);
        for transform in &["identity", "year", "month", "day", "hour"] {
            let op = AlterTableOp::SetPartitionedBy {
                partition_columns: vec![PartitionColumnDef {
                    column_name: "ts".into(),
                    transform: Some(transform.to_string()),
                }],
            };
            assert!(
                validate_alter_table(&columns, &op).is_ok(),
                "transform '{}' should be allowed",
                transform
            );
        }
    }

    #[test]
    fn test_partition_none_transform_ok() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::SetPartitionedBy {
            partition_columns: vec![PartitionColumnDef {
                column_name: "id".into(),
                transform: None,
            }],
        };
        assert!(validate_alter_table(&columns, &op).is_ok());
    }

    #[test]
    fn test_partition_empty_transform_ok() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::SetPartitionedBy {
            partition_columns: vec![PartitionColumnDef {
                column_name: "id".into(),
                transform: Some("".into()),
            }],
        };
        assert!(validate_alter_table(&columns, &op).is_ok());
    }

    #[test]
    fn test_partition_invalid_transform_rejected() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::SetPartitionedBy {
            partition_columns: vec![PartitionColumnDef {
                column_name: "id".into(),
                transform: Some("bucket".into()),
            }],
        };
        let err = validate_alter_table(&columns, &op).unwrap_err();
        assert!(err.to_string().contains("Invalid partition transform"));
    }

    #[test]
    fn test_partition_duplicate_column_rejected() {
        let columns = make_columns(&[("id", "int64", 0, false), ("ts", "timestamp", 1, true)]);
        let op = AlterTableOp::SetPartitionedBy {
            partition_columns: vec![
                PartitionColumnDef {
                    column_name: "id".into(),
                    transform: None,
                },
                PartitionColumnDef {
                    column_name: "id".into(),
                    transform: Some("year".into()),
                },
            ],
        };
        let err = validate_alter_table(&columns, &op).unwrap_err();
        assert!(err.to_string().contains("Duplicate partition column"));
    }

    #[test]
    fn test_partition_nonexistent_column_rejected() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::SetPartitionedBy {
            partition_columns: vec![PartitionColumnDef {
                column_name: "missing".into(),
                transform: None,
            }],
        };
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    #[test]
    fn test_partition_empty_list_rejected() {
        let columns = make_columns(&[("id", "int64", 0, false)]);
        let op = AlterTableOp::SetPartitionedBy {
            partition_columns: vec![],
        };
        assert!(validate_alter_table(&columns, &op).is_err());
    }

    // --- quote_identifier tests ---

    #[test]
    fn test_quote_identifier_simple() {
        assert_eq!(quote_identifier("name"), "\"name\"");
    }

    #[test]
    fn test_quote_identifier_with_double_quote() {
        assert_eq!(quote_identifier(r#"my"col"#), r#""my""col""#);
    }

    #[test]
    fn test_quote_identifier_with_semicolon() {
        assert_eq!(
            quote_identifier("col; DROP TABLE users"),
            "\"col; DROP TABLE users\""
        );
    }

    #[test]
    fn test_quote_identifier_injection_attempt() {
        let malicious = r#"x" TEXT); DROP TABLE foo; --"#;
        let quoted = quote_identifier(malicious);
        assert_eq!(quoted, r#""x"" TEXT); DROP TABLE foo; --""#);
    }
}
