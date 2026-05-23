//! Ticket #20 acceptance tests: snapshot-bounded DDL visibility (time travel).
//!
//! These tests verify the snapshot-bounded visibility invariant for every
//! DDL operation in scope of #20:
//!
//! - ALTER TABLE ADD COLUMN: new column is hidden in pre-change snapshots,
//!   visible in post-change snapshots.
//! - ALTER TABLE DROP COLUMN: dropped column remains visible in pre-change
//!   snapshots, hidden in post-change snapshots.
//! - ALTER TABLE RENAME COLUMN: old name resolves in pre-change snapshots,
//!   new name in post-change snapshots, and column_id is preserved.
//! - DROP TABLE: table is hidden in post-change snapshots, visible in
//!   pre-change snapshots.
//! - DROP SCHEMA CASCADE: schema + all tables are atomically closed
//!   (all-or-none); historical snapshots still resolve everything.
//! - CREATE SCHEMA: new schema visible at post-change snapshot only.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use datafusion_ducklake::metadata_provider::MetadataProvider;
use datafusion_ducklake::metadata_writer::{
    AlterTableOp, ColumnDef, MetadataWriter, WriteMode,
};
use datafusion_ducklake::metadata_writer_sqlite::SqliteMetadataWriter;
use datafusion_ducklake::SqliteMetadataProvider;
use tempfile::TempDir;

async fn create_writer_and_provider() -> (
    SqliteMetadataWriter,
    Arc<dyn MetadataProvider>,
    TempDir,
) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer
        .set_data_path(temp_dir.path().to_str().unwrap())
        .unwrap();
    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap())
        as Arc<dyn MetadataProvider>;
    (writer, provider, temp_dir)
}

fn base_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef::new("id", "int32", false).unwrap(),
        ColumnDef::new("name", "varchar", true).unwrap(),
    ]
}

// ==================== ALTER TABLE ADD COLUMN ====================

#[tokio::test(flavor = "multi_thread")]
async fn add_column_time_travel_visibility() {
    let (writer, provider, _t) = create_writer_and_provider().await;

    let setup = writer
        .begin_write_transaction("main", "users", &base_columns(), WriteMode::Replace)
        .unwrap();
    let pre_alter_snapshot = setup.snapshot_id;

    let post_alter_snapshot = writer
        .alter_table(
            setup.table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();

    assert!(post_alter_snapshot > pre_alter_snapshot);

    // Pre-alter snapshot: only id, name visible.
    let pre_cols = provider
        .get_table_structure(setup.table_id, pre_alter_snapshot)
        .unwrap();
    let pre_names: Vec<&str> = pre_cols.iter().map(|c| c.column_name.as_str()).collect();
    assert_eq!(pre_names, vec!["id", "name"], "pre-alter columns must not include the added column");

    // Post-alter snapshot: id, name, email all visible.
    let post_cols = provider
        .get_table_structure(setup.table_id, post_alter_snapshot)
        .unwrap();
    let post_names: Vec<&str> = post_cols.iter().map(|c| c.column_name.as_str()).collect();
    assert_eq!(post_names, vec!["id", "name", "email"], "post-alter columns must include the added column");
}

// ==================== ALTER TABLE DROP COLUMN ====================

#[tokio::test(flavor = "multi_thread")]
async fn drop_column_time_travel_visibility() {
    let (writer, provider, _t) = create_writer_and_provider().await;

    let setup = writer
        .begin_write_transaction("main", "users", &base_columns(), WriteMode::Replace)
        .unwrap();
    let pre_drop_snapshot = setup.snapshot_id;

    let post_drop_snapshot = writer
        .alter_table(
            setup.table_id,
            &AlterTableOp::DropColumn {
                column_name: "name".to_string(),
            },
        )
        .unwrap();

    // Pre-drop snapshot: dropped column still visible.
    let pre_cols = provider
        .get_table_structure(setup.table_id, pre_drop_snapshot)
        .unwrap();
    let pre_names: Vec<&str> = pre_cols.iter().map(|c| c.column_name.as_str()).collect();
    assert!(pre_names.contains(&"name"), "dropped column must remain visible in pre-drop snapshot");

    // Post-drop snapshot: dropped column gone.
    let post_cols = provider
        .get_table_structure(setup.table_id, post_drop_snapshot)
        .unwrap();
    let post_names: Vec<&str> = post_cols.iter().map(|c| c.column_name.as_str()).collect();
    assert!(!post_names.contains(&"name"), "dropped column must be hidden in post-drop snapshot");
    assert!(post_names.contains(&"id"), "non-dropped columns must remain");
}

// ==================== ALTER TABLE RENAME COLUMN ====================

#[tokio::test(flavor = "multi_thread")]
async fn rename_column_time_travel_visibility() {
    let (writer, provider, _t) = create_writer_and_provider().await;

    let setup = writer
        .begin_write_transaction("main", "users", &base_columns(), WriteMode::Replace)
        .unwrap();
    let pre_rename_snapshot = setup.snapshot_id;

    let pre_cols = provider
        .get_table_structure(setup.table_id, pre_rename_snapshot)
        .unwrap();
    let name_column_id = pre_cols
        .iter()
        .find(|c| c.column_name == "name")
        .expect("must find 'name' column")
        .column_id;

    let post_rename_snapshot = writer
        .alter_table(
            setup.table_id,
            &AlterTableOp::RenameColumn {
                old_name: "name".to_string(),
                new_name: "full_name".to_string(),
            },
        )
        .unwrap();

    // Pre-rename snapshot: old name visible, new name not visible.
    let pre = provider
        .get_table_structure(setup.table_id, pre_rename_snapshot)
        .unwrap();
    let pre_names: Vec<&str> = pre.iter().map(|c| c.column_name.as_str()).collect();
    assert!(pre_names.contains(&"name"));
    assert!(!pre_names.contains(&"full_name"));

    // Post-rename snapshot: new name visible, old name not visible.
    let post = provider
        .get_table_structure(setup.table_id, post_rename_snapshot)
        .unwrap();
    let post_names: Vec<&str> = post.iter().map(|c| c.column_name.as_str()).collect();
    assert!(post_names.contains(&"full_name"));
    assert!(!post_names.contains(&"name"));

    // Column ID is preserved across rename (critical for Parquet field_id mapping).
    let renamed = post
        .iter()
        .find(|c| c.column_name == "full_name")
        .expect("must find renamed column");
    assert_eq!(
        renamed.column_id, name_column_id,
        "column_id must be preserved across RENAME COLUMN"
    );
}

// ==================== DROP TABLE ====================

#[tokio::test(flavor = "multi_thread")]
async fn drop_table_time_travel_visibility() {
    let (writer, provider, _t) = create_writer_and_provider().await;

    let setup = writer
        .begin_write_transaction("main", "users", &base_columns(), WriteMode::Replace)
        .unwrap();
    let pre_drop_snapshot = setup.snapshot_id;

    let post_drop_snapshot = writer.drop_table(setup.table_id).unwrap();
    assert!(post_drop_snapshot > pre_drop_snapshot);

    // Get schema id for lookup.
    let schema = provider
        .get_schema_by_name("main", post_drop_snapshot)
        .unwrap()
        .unwrap();

    // Pre-drop snapshot: table resolves.
    let pre_table = provider
        .get_table_by_name(schema.schema_id, "users", pre_drop_snapshot)
        .unwrap();
    assert!(pre_table.is_some(), "table must resolve at pre-drop snapshot");

    // Post-drop snapshot: table gone.
    let post_table = provider
        .get_table_by_name(schema.schema_id, "users", post_drop_snapshot)
        .unwrap();
    assert!(post_table.is_none(), "table must not resolve at post-drop snapshot");

    // table_names equivalent
    let post_tables = provider
        .list_tables(schema.schema_id, post_drop_snapshot)
        .unwrap();
    assert!(post_tables.iter().all(|t| t.table_name != "users"));
}

// ==================== CREATE SCHEMA ====================

#[tokio::test(flavor = "multi_thread")]
async fn create_schema_immediate_visibility() {
    let (writer, provider, _t) = create_writer_and_provider().await;

    // Snapshot 0 should have no schemas yet.
    let pre_snapshot = writer.create_snapshot().unwrap();
    let pre_schemas = provider.list_schemas(pre_snapshot).unwrap();
    assert!(
        pre_schemas.iter().all(|s| s.schema_name != "analytics"),
        "analytics schema must not exist before creation"
    );

    let (schema_id, was_created) = writer
        .get_or_create_schema("analytics", None, writer.create_snapshot().unwrap())
        .unwrap();
    assert!(was_created);
    assert!(schema_id > 0);

    let post_snapshot = writer.create_snapshot().unwrap();
    let post_schemas = provider.list_schemas(post_snapshot).unwrap();
    let post_names: Vec<&str> = post_schemas.iter().map(|s| s.schema_name.as_str()).collect();
    assert!(
        post_names.contains(&"analytics"),
        "analytics schema must be visible immediately after creation: {:?}",
        post_names
    );
}

// ==================== DROP SCHEMA CASCADE — R10-S-020 atomicity ====================

/// Verifies the all-or-none invariant of DROP SCHEMA CASCADE.
///
/// After CASCADE drop, every table, column, and data-file row that belonged
/// to the schema MUST be closed at the same end_snapshot — there must be no
/// "half-dropped" state where the schema is closed but child tables remain
/// active (or vice versa). This is the R10-S-020 fix that #20 must protect.
#[tokio::test(flavor = "multi_thread")]
async fn drop_schema_cascade_atomic_all_or_none() {
    let (writer, provider, _t) = create_writer_and_provider().await;

    // Build a schema with multiple tables.
    let setup_a = writer
        .begin_write_transaction("analytics", "users", &base_columns(), WriteMode::Replace)
        .unwrap();
    let _setup_b = writer
        .begin_write_transaction("analytics", "events", &base_columns(), WriteMode::Replace)
        .unwrap();
    let _setup_c = writer
        .begin_write_transaction(
            "analytics",
            "sessions",
            &base_columns(),
            WriteMode::Replace,
        )
        .unwrap();

    let pre_snapshot = writer.create_snapshot().unwrap();
    let schema = provider
        .get_schema_by_name("analytics", pre_snapshot)
        .unwrap()
        .unwrap();
    let pre_tables = provider
        .list_tables(schema.schema_id, pre_snapshot)
        .unwrap();
    assert_eq!(pre_tables.len(), 3, "schema must have 3 active tables before cascade");

    // CASCADE drop — should atomically close schema + all tables.
    let post_snapshot = writer.drop_schema(schema.schema_id).unwrap();

    // INVARIANT 1: schema gone in post-drop snapshot.
    let schema_after = provider
        .get_schema_by_name("analytics", post_snapshot)
        .unwrap();
    assert!(
        schema_after.is_none(),
        "schema must be closed after cascade drop"
    );

    // INVARIANT 2: every child table closed in the SAME post-drop snapshot.
    // Since list_tables filters by `end_snapshot IS NULL OR end_snapshot > snapshot_id`,
    // the tables must NOT appear at post_snapshot.
    let active_after = provider
        .list_tables(schema.schema_id, post_snapshot)
        .unwrap();
    assert!(
        active_after.is_empty(),
        "no tables in dropped schema must remain active: found {:?}",
        active_after.iter().map(|t| &t.table_name).collect::<Vec<_>>()
    );

    // INVARIANT 3: all tables AT pre-drop snapshot still resolve (time travel).
    let still_visible = provider
        .list_tables(schema.schema_id, pre_snapshot)
        .unwrap();
    assert_eq!(
        still_visible.len(),
        3,
        "historical snapshot must still see all 3 tables"
    );

    // INVARIANT 4: columns for the first table closed at the SAME snapshot as the schema.
    // Reading the table structure at post_snapshot must return empty (since the
    // columns share end_snapshot with the schema closure).
    let cols_after = provider
        .get_table_structure(setup_a.table_id, post_snapshot)
        .unwrap();
    assert!(
        cols_after.is_empty(),
        "columns of dropped table must not be active at post-drop snapshot"
    );

    // INVARIANT 5: pre-drop snapshot still resolves table structure.
    let cols_before = provider
        .get_table_structure(setup_a.table_id, pre_snapshot)
        .unwrap();
    assert_eq!(cols_before.len(), 2, "historical column lookup must still succeed");
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_schema_cascade_empty_schema_succeeds() {
    let (writer, provider, _t) = create_writer_and_provider().await;

    let snapshot = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("empty_schema", None, snapshot)
        .unwrap();
    let pre = writer.create_snapshot().unwrap();
    assert!(
        provider.get_schema_by_name("empty_schema", pre).unwrap().is_some()
    );

    let post = writer.drop_schema(schema_id).unwrap();
    assert!(
        provider.get_schema_by_name("empty_schema", post).unwrap().is_none()
    );
}

// ==================== Concurrent ALTER conflict detection ====================

/// Two transactions both attempt to ALTER the same table starting from the
/// same `since_snapshot`. One commits, the other gets a `TransactionConflict`.
///
/// This exercises the snapshot-aware conflict detection introduced by the
/// integration tree's R11-S-001/004/005/006/018/022/025 work, specifically
/// for DDL on the same table-id.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_alter_via_drop_table_conflict() {
    let (writer, _provider, _t) = create_writer_and_provider().await;

    let setup = writer
        .begin_write_transaction("main", "users", &base_columns(), WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;

    // Tx A: drop the table (commits).
    writer.drop_table(setup.table_id).unwrap();

    // Tx B: with the stale snapshot, attempt another drop on the same table
    // — must be detected as a conflict via the snapshot-bounded check.
    let result = writer.drop_table_checked(setup.table_id, stale_snapshot);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(
            &err,
            datafusion_ducklake::error::DuckLakeError::TransactionConflict(_)
        ),
        "expected TransactionConflict, got: {err}"
    );
}

/// Two threads concurrently issue ALTER TABLE ADD COLUMN on the same table.
/// Both should succeed (each acquires the SQLite write lock in turn), and the
/// final column list must reflect both adds without corruption.
///
/// Note: SQLite serializes writes at the connection-pool level, so this test
/// verifies "no corruption under concurrent ALTER" rather than the conflict
/// path (which is the `_checked` variants). The `concurrent_alter_via_drop_table_conflict`
/// test above exercises the explicit conflict path.
/// A `_checked` write that races a DROP_SCHEMA must conflict.
/// This is the cross-DDL-vs-DML invariant: an ALTER or write that started
/// at `stale_snapshot` must fail if the schema was dropped concurrently.
#[tokio::test(flavor = "multi_thread")]
async fn checked_write_after_concurrent_schema_drop_conflicts() {
    let (writer, _provider, _t) = create_writer_and_provider().await;

    let setup = writer
        .begin_write_transaction("analytics", "events", &base_columns(), WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;

    // Drop the schema cascade-style (which closes all its tables atomically).
    writer.drop_schema(setup.schema_id).unwrap();

    // Stale-snapshot checked write into the same schema must conflict.
    let result = writer.begin_checked_write_transaction(
        "analytics",
        "events",
        &base_columns(),
        WriteMode::Append,
        stale_snapshot,
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(
            &err,
            datafusion_ducklake::error::DuckLakeError::TransactionConflict(_)
        ),
        "expected TransactionConflict on stale write after schema drop, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_alter_add_column_no_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = Arc::new(
        SqliteMetadataWriter::new_with_init(&conn_str)
            .await
            .unwrap(),
    );
    writer
        .set_data_path(temp_dir.path().to_str().unwrap())
        .unwrap();

    let setup = writer
        .begin_write_transaction("main", "users", &base_columns(), WriteMode::Replace)
        .unwrap();
    let table_id = setup.table_id;

    let writer_a = Arc::clone(&writer);
    let writer_b = Arc::clone(&writer);

    let handle_a = tokio::task::spawn_blocking(move || {
        writer_a.alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("col_a", "varchar", true).unwrap(),
            },
        )
    });
    let handle_b = tokio::task::spawn_blocking(move || {
        writer_b.alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("col_b", "int64", true).unwrap(),
            },
        )
    });

    let res_a = handle_a.await.unwrap();
    let res_b = handle_b.await.unwrap();

    // Both ALTERs must succeed (SQLite serializes them; no conflict expected
    // for non-overlapping ADD COLUMN ops). The point is no corruption.
    assert!(res_a.is_ok(), "concurrent ALTER A failed: {:?}", res_a);
    assert!(res_b.is_ok(), "concurrent ALTER B failed: {:?}", res_b);

    let cols = writer.get_active_columns(table_id).unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c.0.as_str()).collect();
    assert_eq!(names.len(), 4, "must have 4 columns after both ALTERs: {:?}", names);
    assert!(names.contains(&"id"));
    assert!(names.contains(&"name"));
    assert!(names.contains(&"col_a"));
    assert!(names.contains(&"col_b"));
}
