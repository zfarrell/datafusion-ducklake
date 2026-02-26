#![cfg(feature = "write-mysql")]
//! MySQL metadata writer tests
//!
//! Tests the MySqlMetadataWriter implementation against a real MySQL instance
//! using testcontainers.

use datafusion_ducklake::{
    ColumnDef, DataFileInfo, DeleteFileInfo, MetadataWriter, MySqlMetadataWriter, WriteMode,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;

/// Helper to create a MySQL writer with initialized schema
async fn create_mysql_writer()
-> anyhow::Result<(MySqlMetadataWriter, testcontainers::ContainerAsync<Mysql>)> {
    let container = Mysql::default().start().await?;

    let host = "127.0.0.1";
    let port = container.get_host_port_ipv4(3306).await?;
    let conn_str = format!("mysql://root@{}:{}/test", host, port);

    let writer = MySqlMetadataWriter::new_with_init(&conn_str)
        .await
        .expect("Failed to create writer");

    Ok((writer, container))
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_create_snapshot() {
    let (writer, _container) = create_mysql_writer().await.unwrap();

    let snap1 = writer.create_snapshot().unwrap();
    assert!(snap1 >= 1);

    let snap2 = writer.create_snapshot().unwrap();
    assert!(snap2 > snap1);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_or_create_schema() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
    let snapshot_id = writer.create_snapshot().unwrap();

    // Create new schema
    let (schema_id, created) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    assert!(created);
    assert!(schema_id >= 1);

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
    let (writer, _container) = create_mysql_writer().await.unwrap();
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();

    // Create new table
    let (table_id, created) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();
    assert!(created);
    assert!(table_id >= 1);

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
    let (writer, _container) = create_mysql_writer().await.unwrap();
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
    // MySQL auto-increment IDs are always increasing
    assert!(column_ids[0] >= 1);
    assert!(column_ids[1] > column_ids[0]);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_register_data_file() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
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
    assert!(file_id >= 1);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_end_table_files() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
    let snapshot1 = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot1)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot1)
        .unwrap();

    // Register a file
    let file = DataFileInfo::new("data1.parquet", 1024, 100);
    writer
        .register_data_file(table_id, snapshot1, &file)
        .unwrap();

    // End files at new snapshot
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
    let (writer, _container) = create_mysql_writer().await.unwrap();

    // Set data path
    writer.set_data_path("/data/path").unwrap();

    // Get data path
    let path = writer.get_data_path().unwrap();
    assert_eq!(path, "/data/path");

    // Update data path
    writer.set_data_path("/new/path").unwrap();
    let path2 = writer.get_data_path().unwrap();
    assert_eq!(path2, "/new/path");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_begin_write_transaction() {
    let (writer, _container) = create_mysql_writer().await.unwrap();

    let columns =
        vec![ColumnDef::new("id", "int64", false).unwrap(), ColumnDef::new("name", "varchar", true).unwrap()];

    let result = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    assert!(result.snapshot_id >= 1);
    assert!(result.schema_id >= 1);
    assert!(result.table_id >= 1);
    assert_eq!(result.column_ids.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_register_delete_file() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
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

    let delete_file = DeleteFileInfo::new(data_file_id, "delete.parquet", 512, 5);
    let delete_file_id = writer
        .register_delete_file(table_id, snapshot_id, &delete_file)
        .unwrap();
    assert!(delete_file_id >= 1);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_drop_table() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
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
    let (writer, _container) = create_mysql_writer().await.unwrap();
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();

    let drop_snapshot = writer.drop_schema(schema_id).unwrap();
    assert!(drop_snapshot > snapshot_id);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_list_active_table_ids() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();

    // Initially no tables
    let active = writer.list_active_table_ids(schema_id).unwrap();
    assert!(active.is_empty());

    // Create two tables
    let (table_id1, _) = writer
        .get_or_create_table(schema_id, "t1", None, snapshot_id)
        .unwrap();
    let (_table_id2, _) = writer
        .get_or_create_table(schema_id, "t2", None, snapshot_id)
        .unwrap();

    let active = writer.list_active_table_ids(schema_id).unwrap();
    assert_eq!(active.len(), 2);

    // Drop one table
    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
    writer
        .set_columns(table_id1, &columns, snapshot_id)
        .unwrap();
    writer.drop_table(table_id1).unwrap();

    let active = writer.list_active_table_ids(schema_id).unwrap();
    assert_eq!(active.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_create_and_drop_view() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();

    let (view_id, create_snapshot) = writer
        .create_view(schema_id, "my_view", "SELECT 1")
        .unwrap();
    assert!(view_id >= 1);
    assert!(create_snapshot > snapshot_id);

    let drop_snapshot = writer.drop_view(view_id).unwrap();
    assert!(drop_snapshot > create_snapshot);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_active_columns() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
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
    assert_eq!(active[0].0, "id");
    assert_eq!(active[0].1, "int64");
    assert!(!active[0].2);
    assert_eq!(active[1].0, "name");
    assert_eq!(active[1].1, "varchar");
    assert!(active[1].2);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_alter_table_add_column() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "users", None, snapshot_id)
        .unwrap();

    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
    writer.set_columns(table_id, &columns, snapshot_id).unwrap();

    use datafusion_ducklake::metadata_writer::AlterTableOp;
    let alter_snapshot = writer
        .alter_table(
            table_id,
            &AlterTableOp::AddColumn {
                column: ColumnDef::new("email", "varchar", true).unwrap(),
            },
        )
        .unwrap();
    assert!(alter_snapshot > snapshot_id);

    let active = writer.get_active_columns(table_id).unwrap();
    assert_eq!(active.len(), 2);
    assert_eq!(active[1].0, "email");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_alter_table_drop_column() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
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

    use datafusion_ducklake::metadata_writer::AlterTableOp;
    let alter_snapshot = writer
        .alter_table(
            table_id,
            &AlterTableOp::DropColumn {
                column_name: "name".to_string(),
            },
        )
        .unwrap();
    assert!(alter_snapshot > snapshot_id);

    let active = writer.get_active_columns(table_id).unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].0, "id");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_alter_table_rename_column() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
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

    use datafusion_ducklake::metadata_writer::AlterTableOp;
    writer
        .alter_table(
            table_id,
            &AlterTableOp::RenameColumn {
                old_name: "name".to_string(),
                new_name: "full_name".to_string(),
            },
        )
        .unwrap();

    let active = writer.get_active_columns(table_id).unwrap();
    assert_eq!(active.len(), 2);
    assert_eq!(active[0].0, "id");
    assert_eq!(active[1].0, "full_name");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_initialize_schema_idempotent() {
    let (writer, _container) = create_mysql_writer().await.unwrap();

    // Initialize again - should be idempotent
    writer
        .initialize_schema()
        .expect("Second init should succeed");

    // Should still work
    let snap = writer.create_snapshot().unwrap();
    assert!(snap >= 1);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_begin_checked_write_transaction() {
    let (writer, _container) = create_mysql_writer().await.unwrap();

    let columns =
        vec![ColumnDef::new("id", "int64", false).unwrap(), ColumnDef::new("name", "varchar", true).unwrap()];

    // First write
    let result1 = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    // Should succeed without conflict
    let result2 = writer
        .begin_checked_write_transaction(
            "main",
            "users",
            &columns,
            WriteMode::Append,
            result1.snapshot_id,
        )
        .unwrap();
    assert!(result2.snapshot_id > result1.snapshot_id);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_conflict_detection_drop_table() {
    let (writer, _container) = create_mysql_writer().await.unwrap();

    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
    let result = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    let before_drop = result.snapshot_id;

    // Drop the table
    writer.drop_table(result.table_id).unwrap();

    // Checked write should detect the conflict
    let conflict = writer.begin_checked_write_transaction(
        "main",
        "users",
        &columns,
        WriteMode::Append,
        before_drop,
    );
    assert!(conflict.is_err());
    let err_msg = format!("{}", conflict.unwrap_err());
    assert!(err_msg.contains("conflict"));
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_drop_table_checked_conflict() {
    let (writer, _container) = create_mysql_writer().await.unwrap();

    let columns = vec![ColumnDef::new("id", "int64", false).unwrap()];
    let result = writer
        .begin_write_transaction("main", "users", &columns, WriteMode::Replace)
        .unwrap();

    let before_drop = result.snapshot_id;

    // Drop the table
    writer.drop_table(result.table_id).unwrap();

    // Checked drop should detect the conflict
    let conflict = writer.drop_table_checked(result.table_id, before_drop);
    assert!(conflict.is_err());
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_register_column_stats() {
    let (writer, _container) = create_mysql_writer().await.unwrap();
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
    let file_id = writer
        .register_data_file(table_id, snapshot_id, &file)
        .unwrap();

    use datafusion_ducklake::ColumnStatInfo;
    let stats = vec![ColumnStatInfo {
        column_id: column_ids[0],
        null_count: Some(0),
        min_value: Some("1".to_string()),
        max_value: Some("100".to_string()),
    }];

    // Should succeed
    writer
        .register_column_stats(file_id, table_id, &stats)
        .unwrap();

    // Registering again should use ON DUPLICATE KEY UPDATE
    writer
        .register_column_stats(file_id, table_id, &stats)
        .unwrap();
}
