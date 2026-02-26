#![cfg(feature = "write-postgres")]
//! PostgreSQL MetadataWriter tests.
//!
//! Uses testcontainers to spin up a temporary PostgreSQL instance.

use datafusion_ducklake::PostgresMetadataWriter;
use datafusion_ducklake::metadata_writer::{
    AlterColumnTypeOp, AlterTableOp, ColumnDef, ColumnStatInfo, DataFileInfo, DeleteFileInfo,
    MetadataWriter, WriteMode,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// Helper to create a PostgreSQL writer with initialized schema
async fn create_writer() -> (
    PostgresMetadataWriter,
    testcontainers::ContainerAsync<Postgres>,
) {
    let container = Postgres::default().start().await.unwrap();
    let host = "127.0.0.1";
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let conn_str = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    let writer = PostgresMetadataWriter::new_with_init(&conn_str)
        .await
        .expect("Failed to create writer");

    (writer, container)
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_create_snapshot() {
    let (writer, _container) = create_writer().await;

    let snap1 = writer.create_snapshot().unwrap();
    assert!(snap1 > 0);

    let snap2 = writer.create_snapshot().unwrap();
    assert!(snap2 > snap1);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_or_create_schema() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();

    // Create new schema
    let (schema_id, created) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    assert!(created);
    assert!(schema_id > 0);

    // Get existing schema
    let (schema_id2, created2) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    assert!(!created2);
    assert_eq!(schema_id2, schema_id);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_or_create_table() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();

    // Create new table
    let (table_id, created) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();
    assert!(created);
    assert!(table_id > 0);

    // Get existing table
    let (table_id2, created2) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();
    assert!(!created2);
    assert_eq!(table_id2, table_id);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_set_columns() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns =
        vec![ColumnDef::new("id", "int64", false).unwrap(), ColumnDef::new("name", "varchar", true).unwrap()];

    let column_ids = writer.set_columns(table_id, &columns, snapshot_id).unwrap();
    assert_eq!(column_ids.len(), 2);
    assert!(column_ids[0] > 0);
    assert!(column_ids[1] > column_ids[0]);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_register_data_file() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let file = DataFileInfo::new("data.parquet", 1024, 100).with_footer_size(256);

    let file_id = writer
        .register_data_file(table_id, snapshot_id, &file)
        .unwrap();
    assert!(file_id > 0);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_end_table_files() {
    let (writer, _container) = create_writer().await;
    let snapshot1 = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot1)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot1)
        .unwrap();

    let file = DataFileInfo::new("data1.parquet", 1024, 100);
    writer
        .register_data_file(table_id, snapshot1, &file)
        .unwrap();

    let snapshot2 = writer.create_snapshot().unwrap();
    let ended = writer.end_table_files(table_id, snapshot2).unwrap();
    assert_eq!(ended, 1);

    // End again should affect 0 files
    let ended2 = writer.end_table_files(table_id, snapshot2).unwrap();
    assert_eq!(ended2, 0);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_data_path() {
    let (writer, _container) = create_writer().await;

    writer.set_data_path("/data/path").unwrap();
    let path = writer.get_data_path().unwrap();
    assert_eq!(path, "/data/path");

    writer.set_data_path("/new/path").unwrap();
    let path2 = writer.get_data_path().unwrap();
    assert_eq!(path2, "/new/path");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_begin_write_transaction() {
    let (writer, _container) = create_writer().await;

    let columns =
        vec![ColumnDef::new("id", "int64", false).unwrap(), ColumnDef::new("name", "varchar", true).unwrap()];

    let result = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    assert!(result.snapshot_id > 0);
    assert!(result.schema_id > 0);
    assert!(result.table_id > 0);
    assert_eq!(result.column_ids.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_register_delete_file() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let data_file = DataFileInfo::new("data.parquet", 1024, 100);
    let data_file_id = writer
        .register_data_file(table_id, snapshot_id, &data_file)
        .unwrap();

    let delete_file = DeleteFileInfo::new(data_file_id, "data.delete.parquet", 512, 5);
    let delete_file_id = writer
        .register_delete_file(table_id, snapshot_id, &delete_file)
        .unwrap();
    assert!(delete_file_id > 0);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_drop_table() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
    writer.set_columns(table_id, &columns, snapshot_id).unwrap();

    let drop_snapshot = writer.drop_table(table_id).unwrap();
    assert!(drop_snapshot > snapshot_id);

    // Table should no longer appear in active tables
    let active = writer.list_active_table_ids(schema_id).unwrap();
    assert!(active.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_drop_schema() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("test_schema", None, snapshot_id)
        .unwrap();

    let drop_snapshot = writer.drop_schema(schema_id).unwrap();
    assert!(drop_snapshot > snapshot_id);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_list_active_table_ids() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();

    // No tables yet
    let ids = writer.list_active_table_ids(schema_id).unwrap();
    assert!(ids.is_empty());

    // Add a table
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let ids = writer.list_active_table_ids(schema_id).unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0], table_id);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_register_column_stats() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
    let column_ids = writer.set_columns(table_id, &columns, snapshot_id).unwrap();

    let file = DataFileInfo::new("data.parquet", 1024, 100);
    let data_file_id = writer
        .register_data_file(table_id, snapshot_id, &file)
        .unwrap();

    let stats = vec![ColumnStatInfo {
        column_id: column_ids[0],
        null_count: Some(0),
        min_value: Some("1".to_string()),
        max_value: Some("100".to_string()),
    }];

    writer
        .register_column_stats(data_file_id, table_id, &stats)
        .unwrap();

    // Upsert should also work
    let stats2 = vec![ColumnStatInfo {
        column_id: column_ids[0],
        null_count: Some(5),
        min_value: Some("1".to_string()),
        max_value: Some("200".to_string()),
    }];

    writer
        .register_column_stats(data_file_id, table_id, &stats2)
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_initialize_schema_idempotent() {
    let (writer, _container) = create_writer().await;

    // Should be safe to call again
    writer.initialize_schema().unwrap();
    writer.initialize_schema().unwrap();

    // Verify it still works
    let snap = writer.create_snapshot().unwrap();
    assert!(snap > 0);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_active_columns() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns =
        vec![ColumnDef::new("id", "int64", false).unwrap(), ColumnDef::new("name", "varchar", true).unwrap()];
    writer.set_columns(table_id, &columns, snapshot_id).unwrap();

    let active = writer.get_active_columns(table_id).unwrap();
    assert_eq!(active.len(), 2);
    assert_eq!(active[0], ("id".to_string(), "int64".to_string(), false));
    assert_eq!(active[1], ("name".to_string(), "varchar".to_string(), true));
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_create_and_drop_view() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();

    let (view_id, view_snapshot) = writer
        .create_view(schema_id, "my_view", "SELECT 1")
        .unwrap();
    assert!(view_id > 0);
    assert!(view_snapshot > snapshot_id);

    let drop_snapshot = writer.drop_view(view_id).unwrap();
    assert!(drop_snapshot > view_snapshot);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_alter_table_add_column() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
    writer.set_columns(table_id, &columns, snapshot_id).unwrap();

    let alter_snap = writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();
    assert!(alter_snap > snapshot_id);

    let active = writer.get_active_columns(table_id).unwrap();
    assert_eq!(active.len(), 2);
    assert_eq!(active[1].0, "email");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_alter_table_drop_column() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns =
        vec![ColumnDef::new("id", "int64", false).unwrap(), ColumnDef::new("name", "varchar", true).unwrap()];
    writer.set_columns(table_id, &columns, snapshot_id).unwrap();

    writer
        .alter_table(
            table_id,
            &AlterTableOp::DropColumn {
                column_name: "name".to_string(),
            },
        )
        .unwrap();

    let active = writer.get_active_columns(table_id).unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].0, "id");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_alter_table_rename_column() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
    writer.set_columns(table_id, &columns, snapshot_id).unwrap();

    writer
        .alter_table(
            table_id,
            &AlterTableOp::RenameColumn {
                old_name: "id".to_string(),
                new_name: "user_id".to_string(),
            },
        )
        .unwrap();

    let active = writer.get_active_columns(table_id).unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].0, "user_id");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_alter_table_change_type() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns = vec![ColumnDef::new("id", "int32", false).unwrap()];
    writer.set_columns(table_id, &columns, snapshot_id).unwrap();

    writer
        .alter_table(
            table_id,
            &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
                column_name: "id".to_string(),
                new_type: "int64".to_string(),
            }),
        )
        .unwrap();

    let active = writer.get_active_columns(table_id).unwrap();
    assert_eq!(active[0].1, "int64");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_begin_checked_write_transaction() {
    let (writer, _container) = create_writer().await;

    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];

    // First write
    let result = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();
    let first_snapshot = result.snapshot_id;

    // Checked write should succeed (no conflicts)
    let result2 = writer
        .begin_checked_write_transaction(
            "main",
            "users",
            &columns,
            WriteMode::Append,
            first_snapshot,
        )
        .unwrap();
    assert!(result2.snapshot_id > first_snapshot);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_drop_table_checked_conflict() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
    writer.set_columns(table_id, &columns, snapshot_id).unwrap();

    // Drop table
    writer.drop_table(table_id).unwrap();

    // Checked drop should fail (already dropped)
    let result = writer.drop_table_checked(table_id, snapshot_id);
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_drop_schema_checked_conflict() {
    let (writer, _container) = create_writer().await;
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("test_schema", None, snapshot_id)
        .unwrap();

    // Drop schema
    writer.drop_schema(schema_id).unwrap();

    // Checked drop should fail (already dropped)
    let result = writer.drop_schema_checked(schema_id, snapshot_id);
    assert!(result.is_err());
}
