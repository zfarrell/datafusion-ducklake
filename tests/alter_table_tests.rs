//! Tests for ALTER TABLE operations (Gap 5).
//!
//! Tests ADD COLUMN, DROP COLUMN, RENAME COLUMN, and ALTER COLUMN TYPE
//! at the MetadataWriter level. DataFusion v51 does not support ALTER TABLE
//! at the SQL planner level, so these operations are exposed programmatically.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use datafusion_ducklake::metadata_writer::{
    AlterColumnTypeOp, AlterTableOp, ColumnDef, MetadataWriter, WriteMode,
};
use datafusion_ducklake::metadata_writer_sqlite::SqliteMetadataWriter;
use tempfile::TempDir;

async fn create_test_writer() -> (SqliteMetadataWriter, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer
        .set_data_path(temp_dir.path().to_str().unwrap())
        .unwrap();
    (writer, temp_dir)
}

fn test_columns() -> Vec<ColumnDef> {
    vec![ColumnDef::new("id", "int32", false).unwrap(), ColumnDef::new("name", "varchar", true).unwrap()]
}

/// Helper: create a table and return (table_id, schema_id, snapshot_id)
fn setup_table(writer: &SqliteMetadataWriter) -> (i64, i64, i64) {
    let setup = writer
        .begin_write_transaction("main", "users", &test_columns(), WriteMode::Replace)
        .unwrap();
    (setup.table_id, setup.schema_id, setup.snapshot_id)
}

// ==================== ADD COLUMN ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_add_column_basic() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    let snapshot = writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();

    assert!(snapshot > 0);

    // Verify column was added by checking active columns
    let columns = writer.get_active_columns(table_id).unwrap();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[1].0, "name");
    assert_eq!(columns[2].0, "email");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_column_non_nullable_fails() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    // Adding a non-nullable column should fail (existing rows would have no value)
    let result = writer.alter_table(
        table_id,
        &AlterTableOp::AddColumn {
            column: ColumnDef::new("age", "int32", false).unwrap(),
        },
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("nullable"),
        "Error should mention nullable: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_column_duplicate_name_fails() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    // Adding a column with a name that already exists should fail
    let result = writer.alter_table(
        table_id,
        &AlterTableOp::AddColumn {
            column: ColumnDef::new("name", "varchar", true).unwrap(),
        },
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("already exists"),
        "Error should mention 'already exists': {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_add_multiple_columns() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();

    writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("age", "int32", true).unwrap(),
            },
        )
        .unwrap();

    let columns = writer.get_active_columns(table_id).unwrap();
    assert_eq!(columns.len(), 4);
    assert_eq!(columns[2].0, "email");
    assert_eq!(columns[3].0, "age");
}

// ==================== DROP COLUMN ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_column_basic() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    let snapshot = writer
        .alter_table(
            table_id,
            &AlterTableOp::DropColumn {
                column_name: "name".to_string(),
            },
        )
        .unwrap();

    assert!(snapshot > 0);

    let columns = writer.get_active_columns(table_id).unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].0, "id");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_column_nonexistent_fails() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    let result = writer.alter_table(
        table_id,
        &AlterTableOp::DropColumn {
            column_name: "nonexistent".to_string(),
        },
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("not found"),
        "Error should mention 'not found': {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_last_column_fails() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    // Drop first column
    writer
        .alter_table(
            table_id,
            &AlterTableOp::DropColumn {
                column_name: "name".to_string(),
            },
        )
        .unwrap();

    // Try to drop the last remaining column
    let result = writer.alter_table(
        table_id,
        &AlterTableOp::DropColumn {
            column_name: "id".to_string(),
        },
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("last column") || err.to_string().contains("only has one column"),
        "Error should mention last column: {err}"
    );
}

// ==================== RENAME COLUMN ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_rename_column_basic() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    let snapshot = writer
        .alter_table(
            table_id,
            &AlterTableOp::RenameColumn {
                old_name: "name".to_string(),
                new_name: "full_name".to_string(),
            },
        )
        .unwrap();

    assert!(snapshot > 0);

    let columns = writer.get_active_columns(table_id).unwrap();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[1].0, "full_name");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rename_column_nonexistent_fails() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    let result = writer.alter_table(
        table_id,
        &AlterTableOp::RenameColumn {
            old_name: "nonexistent".to_string(),
            new_name: "something".to_string(),
        },
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("not found"),
        "Error should mention 'not found': {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rename_column_to_existing_name_fails() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    let result = writer.alter_table(
        table_id,
        &AlterTableOp::RenameColumn {
            old_name: "name".to_string(),
            new_name: "id".to_string(), // "id" already exists
        },
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("already exists"),
        "Error should mention 'already exists': {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rename_column_preserves_type_and_nullable() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    writer
        .alter_table(
            table_id,
            &AlterTableOp::RenameColumn {
                old_name: "id".to_string(),
                new_name: "user_id".to_string(),
            },
        )
        .unwrap();

    let columns = writer.get_active_columns(table_id).unwrap();
    // (name, type, nullable)
    assert_eq!(columns[0].0, "user_id");
    assert_eq!(columns[0].1, "int32");
    assert!(!columns[0].2); // NOT NULL preserved
}

// ==================== ALTER COLUMN TYPE ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_column_type_widening() {
    let (writer, _temp) = create_test_writer().await;

    // Create table with int32 column
    let columns =
        vec![ColumnDef::new("id", "int32", false).unwrap(), ColumnDef::new("value", "int32", true).unwrap()];
    let setup = writer
        .begin_write_transaction("main", "data", &columns, WriteMode::Replace)
        .unwrap();

    // Widen int32 → int64 (allowed)
    let snapshot = writer
        .alter_table(
            setup.table_id,
            &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
                column_name: "value".to_string(),
                new_type: "int64".to_string(),
            }),
        )
        .unwrap();

    assert!(snapshot > 0);

    let columns = writer.get_active_columns(setup.table_id).unwrap();
    assert_eq!(columns[1].0, "value");
    assert_eq!(columns[1].1, "int64");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_column_type_narrowing_fails() {
    let (writer, _temp) = create_test_writer().await;

    let columns = vec![ColumnDef::new("value", "int64", true).unwrap()];
    let setup = writer
        .begin_write_transaction("main", "data", &columns, WriteMode::Replace)
        .unwrap();

    // Narrow int64 → int32 (not allowed)
    let result = writer.alter_table(
        setup.table_id,
        &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "value".to_string(),
            new_type: "int32".to_string(),
        }),
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("widening")
            || err.to_string().contains("not allowed")
            || err.to_string().contains("Cannot change type"),
        "Error should mention type restriction: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_column_type_float_to_double() {
    let (writer, _temp) = create_test_writer().await;

    let columns = vec![ColumnDef::new("value", "float", true).unwrap()];
    let setup = writer
        .begin_write_transaction("main", "data", &columns, WriteMode::Replace)
        .unwrap();

    // float → double (allowed)
    let result = writer.alter_table(
        setup.table_id,
        &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "value".to_string(),
            new_type: "double".to_string(),
        }),
    );

    assert!(result.is_ok());
    let columns = writer.get_active_columns(setup.table_id).unwrap();
    assert_eq!(columns[0].1, "double");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_column_type_incompatible_fails() {
    let (writer, _temp) = create_test_writer().await;

    let columns = vec![ColumnDef::new("value", "varchar", true).unwrap()];
    let setup = writer
        .begin_write_transaction("main", "data", &columns, WriteMode::Replace)
        .unwrap();

    // varchar → int32 (not allowed - incompatible types)
    let result = writer.alter_table(
        setup.table_id,
        &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "value".to_string(),
            new_type: "int32".to_string(),
        }),
    );

    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_column_type_preserves_nullable() {
    let (writer, _temp) = create_test_writer().await;

    let columns = vec![ColumnDef::new("value", "int32", false).unwrap()];
    let setup = writer
        .begin_write_transaction("main", "data", &columns, WriteMode::Replace)
        .unwrap();

    writer
        .alter_table(
            setup.table_id,
            &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
                column_name: "value".to_string(),
                new_type: "int64".to_string(),
            }),
        )
        .unwrap();

    let columns = writer.get_active_columns(setup.table_id).unwrap();
    assert_eq!(columns[0].1, "int64");
    assert!(!columns[0].2); // NOT NULL preserved
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_column_type_nonexistent_fails() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    let result = writer.alter_table(
        table_id,
        &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "nonexistent".to_string(),
            new_type: "int64".to_string(),
        }),
    );

    assert!(result.is_err());
}

// ==================== Type promotion matrix ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_type_promotion_int_widening_chain() {
    let (writer, _temp) = create_test_writer().await;

    // int16 → int32 → int64
    let columns = vec![ColumnDef::new("v", "int16", true).unwrap()];
    let setup = writer
        .begin_write_transaction("main", "chain", &columns, WriteMode::Replace)
        .unwrap();

    // int16 → int32
    writer
        .alter_table(
            setup.table_id,
            &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
                column_name: "v".to_string(),
                new_type: "int32".to_string(),
            }),
        )
        .unwrap();

    // int32 → int64
    writer
        .alter_table(
            setup.table_id,
            &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
                column_name: "v".to_string(),
                new_type: "int64".to_string(),
            }),
        )
        .unwrap();

    let columns = writer.get_active_columns(setup.table_id).unwrap();
    assert_eq!(columns[0].1, "int64");
}

// ==================== Combined operations ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_combined_add_rename_drop() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    // Add email column
    writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();

    // Rename name → full_name
    writer
        .alter_table(
            table_id,
            &AlterTableOp::RenameColumn {
                old_name: "name".to_string(),
                new_name: "full_name".to_string(),
            },
        )
        .unwrap();

    // Drop email
    writer
        .alter_table(
            table_id,
            &AlterTableOp::DropColumn {
                column_name: "email".to_string(),
            },
        )
        .unwrap();

    let columns = writer.get_active_columns(table_id).unwrap();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[1].0, "full_name");
}

// ==================== Snapshot tracking ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_creates_new_snapshot() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, initial_snapshot) = setup_table(&writer);

    let snap1 = writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();

    assert!(snap1 > initial_snapshot);

    let snap2 = writer
        .alter_table(
            table_id,
            &AlterTableOp::RenameColumn {
                old_name: "name".to_string(),
                new_name: "full_name".to_string(),
            },
        )
        .unwrap();

    assert!(snap2 > snap1);
}

// ==================== Conflict detection integration ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_on_dropped_table_fails() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    // Drop the table
    writer.drop_table(table_id).unwrap();

    // Try to alter the dropped table
    let result = writer.alter_table(
        table_id,
        &AlterTableOp::AddColumn {
            column: ColumnDef::new("email", "varchar", true).unwrap(),
        },
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("no active columns")
            || err.to_string().contains("not found")
            || err.to_string().contains("dropped"),
        "Error should indicate table issue: {err}"
    );
}

// ==================== Compound ALTER TABLE ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_compound_alter_add_then_rename() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    // Step 1: Add a column
    writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();

    // Step 2: Rename the newly added column
    writer
        .alter_table(
            table_id,
            &AlterTableOp::RenameColumn {
                old_name: "email".to_string(),
                new_name: "contact_email".to_string(),
            },
        )
        .unwrap();

    let columns = writer.get_active_columns(table_id).unwrap();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[1].0, "name");
    assert_eq!(columns[2].0, "contact_email");
    assert_eq!(columns[2].1, "varchar");
    assert!(columns[2].2); // nullable
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compound_alter_add_rename_drop() {
    let (writer, _temp) = create_test_writer().await;
    let (table_id, _, _) = setup_table(&writer);

    // Add two columns
    writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();
    writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("age", "int32", true).unwrap(),
            },
        )
        .unwrap();

    // Rename one
    writer
        .alter_table(
            table_id,
            &AlterTableOp::RenameColumn {
                old_name: "name".to_string(),
                new_name: "full_name".to_string(),
            },
        )
        .unwrap();

    // Drop one
    writer
        .alter_table(
            table_id,
            &AlterTableOp::DropColumn {
                column_name: "age".to_string(),
            },
        )
        .unwrap();

    // Widen type
    writer
        .alter_table(
            table_id,
            &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
                column_name: "id".to_string(),
                new_type: "int64".to_string(),
            }),
        )
        .unwrap();

    let columns = writer.get_active_columns(table_id).unwrap();
    assert_eq!(columns.len(), 3, "Expected 3 columns after compound ALTER: {columns:?}");
    assert_eq!(columns[0].0, "id");
    assert_eq!(columns[0].1, "int64"); // widened
    assert_eq!(columns[1].0, "full_name"); // renamed
    assert_eq!(columns[2].0, "email");
}

// ==================== Duplicate column validation ====================

#[tokio::test(flavor = "multi_thread")]
async fn test_write_with_duplicate_columns_fails() {
    let (writer, _temp) = create_test_writer().await;

    let dup_columns = vec![
        ColumnDef::new("id", "int32", false).unwrap(),
        ColumnDef::new("name", "varchar", true).unwrap(),
        ColumnDef::new("id", "int32", false).unwrap(), // duplicate
    ];

    let result = writer.begin_write_transaction("main", "bad_table", &dup_columns, WriteMode::Replace);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("Duplicate column name"),
        "Error should mention duplicate column: {err}"
    );
}
