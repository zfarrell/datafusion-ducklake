//! Tests for transaction conflict detection (Gap 14).
//!
//! These tests verify that concurrent write operations properly detect conflicts
//! and return appropriate errors instead of silently corrupting data.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use datafusion_ducklake::error::DuckLakeError;
use datafusion_ducklake::metadata_writer::{ColumnDef, MetadataWriter, WriteMode};
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
    vec![
        ColumnDef::new("id", "int32", false).unwrap(),
        ColumnDef::new("name", "varchar", true).unwrap(),
    ]
}

// ==================== INSERT after DROP conflicts ====================

/// Inserting into a table that was dropped since our snapshot should fail.
#[tokio::test(flavor = "multi_thread")]
async fn test_conflict_insert_after_table_drop() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create table via write transaction (snapshot 1)
    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;

    // Another writer drops the table (snapshot 2)
    writer.drop_table(setup.table_id).unwrap();

    // Try to write with stale snapshot → should conflict
    let result = writer.begin_checked_write_transaction(
        "main",
        "users",
        &columns,
        WriteMode::Append,
        stale_snapshot,
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, DuckLakeError::TransactionConflict(_)),
        "Expected TransactionConflict, got: {err}"
    );
    assert!(
        err.to_string().contains("dropped"),
        "Error should mention 'dropped': {err}"
    );
}

/// Inserting into a table in a dropped schema should fail.
#[tokio::test(flavor = "multi_thread")]
async fn test_conflict_insert_after_schema_drop() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create table in schema "analytics" (snapshot 1)
    let setup = writer
        .begin_write_transaction("analytics", "events", &columns, WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;

    // Drop the schema (drops all tables first, then schema)
    writer.drop_table(setup.table_id).unwrap();
    writer.drop_schema(setup.schema_id).unwrap();

    // Try to write with stale snapshot → should conflict
    let result = writer.begin_checked_write_transaction(
        "analytics",
        "events",
        &columns,
        WriteMode::Append,
        stale_snapshot,
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, DuckLakeError::TransactionConflict(_)),
        "Expected TransactionConflict, got: {err}"
    );
}

// ==================== DROP after DROP conflicts ====================

/// Dropping a table that was already dropped should fail.
#[tokio::test(flavor = "multi_thread")]
async fn test_conflict_concurrent_table_drops() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create table (snapshot 1)
    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;

    // Writer A drops the table (snapshot 2)
    writer.drop_table(setup.table_id).unwrap();

    // Writer B also tries to drop the same table with stale snapshot → conflict
    let result = writer.drop_table_checked(setup.table_id, stale_snapshot);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, DuckLakeError::TransactionConflict(_)),
        "Expected TransactionConflict, got: {err}"
    );
    assert!(
        err.to_string().contains("dropped"),
        "Error should mention 'dropped': {err}"
    );
}

/// Dropping a schema that was already dropped should fail.
#[tokio::test(flavor = "multi_thread")]
async fn test_conflict_concurrent_schema_drops() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create table in schema (snapshot 1)
    let setup = writer
        .begin_write_transaction("myschema", "data", &columns, WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;

    // Writer A drops the schema (snapshots 2, 3)
    writer.drop_table(setup.table_id).unwrap();
    writer.drop_schema(setup.schema_id).unwrap();

    // Writer B also tries to drop the schema with stale snapshot → conflict
    let result = writer.drop_schema_checked(setup.schema_id, stale_snapshot);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, DuckLakeError::TransactionConflict(_)),
        "Expected TransactionConflict, got: {err}"
    );
}

// ==================== No-conflict scenarios ====================

/// Writing to different tables should not conflict.
#[tokio::test(flavor = "multi_thread")]
async fn test_no_conflict_independent_tables() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create table A (snapshot 1)
    let setup_a = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();
    let snapshot_after_a = setup_a.snapshot_id;

    // Create table B (snapshot 2)
    let _setup_b = writer
        .begin_write_transaction("main", "orders", &columns, WriteMode::Replace)
        .unwrap();

    // Drop table B (snapshot 3)
    writer.drop_table(_setup_b.table_id).unwrap();

    // Writing to table A with stale snapshot should still succeed
    // because table A was not modified since snapshot_after_a
    let result = writer.begin_checked_write_transaction(
        "main",
        "users",
        &columns,
        WriteMode::Append,
        snapshot_after_a,
    );

    assert!(result.is_ok(), "Should not conflict: {result:?}");
}

/// Sequential writes with fresh snapshots should not conflict.
#[tokio::test(flavor = "multi_thread")]
async fn test_no_conflict_sequential_with_fresh_snapshot() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // First write (snapshot 1)
    let setup1 = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    // Second write using the LATEST snapshot (not stale) → no conflict
    let result = writer.begin_checked_write_transaction(
        "main",
        "users",
        &columns,
        WriteMode::Append,
        setup1.snapshot_id, // This IS the latest snapshot, so no conflict
    );

    assert!(result.is_ok(), "Should not conflict: {result:?}");
}

/// Writing to a NEW table (that doesn't exist yet) with fresh snapshot should succeed.
#[tokio::test(flavor = "multi_thread")]
async fn test_no_conflict_new_table_creation() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create one table (snapshot 1)
    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    // Create a NEW different table with checked write → should succeed
    let result = writer.begin_checked_write_transaction(
        "main",
        "orders",
        &columns,
        WriteMode::Replace,
        setup.snapshot_id,
    );

    assert!(result.is_ok(), "Should not conflict: {result:?}");
}

/// Appending to the same table (no destructive changes) should not conflict.
#[tokio::test(flavor = "multi_thread")]
async fn test_no_conflict_concurrent_appends() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create table (snapshot 1)
    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();
    let start_snapshot = setup.snapshot_id;

    // Another append (snapshot 2) - not a destructive change
    let _setup2 = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Append)
        .unwrap();

    // Third append with original snapshot → should NOT conflict
    // (concurrent inserts are safe, only drops/alters conflict)
    let result = writer.begin_checked_write_transaction(
        "main",
        "users",
        &columns,
        WriteMode::Append,
        start_snapshot,
    );

    assert!(
        result.is_ok(),
        "Concurrent appends should not conflict: {result:?}"
    );
}

// ==================== Error message quality ====================

/// Conflict error messages should be descriptive.
#[tokio::test(flavor = "multi_thread")]
async fn test_conflict_error_message_contains_table_info() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create and drop table
    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;
    writer.drop_table(setup.table_id).unwrap();

    // Trigger conflict
    let err = writer
        .begin_checked_write_transaction(
            "main",
            "users",
            &columns,
            WriteMode::Append,
            stale_snapshot,
        )
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("conflict"),
        "Error should contain 'conflict': {msg}"
    );
}

/// Drop conflict error should mention the table was already dropped.
#[tokio::test(flavor = "multi_thread")]
async fn test_drop_conflict_error_message() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;
    writer.drop_table(setup.table_id).unwrap();

    let err = writer
        .drop_table_checked(setup.table_id, stale_snapshot)
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("conflict"),
        "Error should contain 'conflict': {msg}"
    );
    assert!(
        msg.to_lowercase().contains("drop"),
        "Error should mention 'drop': {msg}"
    );
}

// ==================== Edge cases ====================

/// Conflict detection with snapshot 0 (before any snapshots exist) should detect all changes.
#[tokio::test(flavor = "multi_thread")]
async fn test_conflict_detection_from_snapshot_zero() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create table (snapshot 1)
    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    // Drop it (snapshot 2)
    writer.drop_table(setup.table_id).unwrap();

    // Check from snapshot 0 → should detect the drop
    let result = writer.begin_checked_write_transaction(
        "main",
        "users",
        &columns,
        WriteMode::Append,
        0, // Before any snapshots
    );

    assert!(result.is_err(), "Should detect conflict from snapshot 0");
}

/// Replace mode on a dropped table should still conflict.
#[tokio::test(flavor = "multi_thread")]
async fn test_conflict_replace_after_drop() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;

    writer.drop_table(setup.table_id).unwrap();

    // Even Replace mode should conflict when the table was dropped
    let result = writer.begin_checked_write_transaction(
        "main",
        "users",
        &columns,
        WriteMode::Replace,
        stale_snapshot,
    );

    assert!(
        result.is_err(),
        "Replace on dropped table should still conflict"
    );
}

/// Multiple drops and recreates - conflict should only check since our snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn test_conflict_only_checks_since_snapshot() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create table (snapshot 1)
    let setup1 = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    // Drop it (snapshot 2)
    writer.drop_table(setup1.table_id).unwrap();

    // Recreate it (snapshot 3) - this is after the drop
    let setup3 = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    // Writing with snapshot from AFTER the recreate should succeed
    let result = writer.begin_checked_write_transaction(
        "main",
        "users",
        &columns,
        WriteMode::Append,
        setup3.snapshot_id,
    );

    assert!(
        result.is_ok(),
        "Should not conflict when using snapshot after recreate: {result:?}"
    );
}

/// The unchecked begin_write_transaction should still work without conflict detection.
#[tokio::test(flavor = "multi_thread")]
async fn test_unchecked_write_still_works() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create and drop table
    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();
    writer.drop_table(setup.table_id).unwrap();

    // Unchecked write should succeed (no conflict detection)
    let result = writer.begin_write_transaction("main", "users", &columns, WriteMode::Replace);
    assert!(
        result.is_ok(),
        "Unchecked write should not perform conflict detection"
    );
}

/// The unchecked drop_table should still work without conflict detection.
#[tokio::test(flavor = "multi_thread")]
async fn test_unchecked_drop_still_works() {
    let (writer, _temp) = create_test_writer().await;
    let columns = test_columns();

    // Create table
    let setup = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    // Unchecked drop should work fine
    let result = writer.drop_table(setup.table_id);
    assert!(result.is_ok());
}
