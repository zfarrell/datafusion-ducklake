#![cfg(feature = "metadata-mysql")]
//! MySQL metadata provider tests
//!
//! This test suite verifies the MySQL metadata provider implementation,
//! including all MetadataProvider trait methods, schema initialization,
//! concurrent access, and error handling.
//!
//! ## Test Setup
//!
//! Tests use testcontainers to spin up a temporary MySQL instance.
//! Each test creates its own database with test data to ensure isolation.
//!
//! ## Coverage
//!
//! - Schema initialization (idempotent)
//! - All MetadataProvider trait methods
//! - Snapshot isolation and temporal queries
//! - Concurrent access and thread safety
//! - Error handling and edge cases

mod common;

use datafusion::prelude::*;
use datafusion_ducklake::{
    DuckLakeCatalog, DuckdbMetadataProvider, MySqlMetadataProvider,
    metadata_provider::MetadataProvider,
};
use sqlx::MySqlPool;
use std::sync::Arc;
use tempfile::TempDir;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;

/// Initialize DuckLake catalog schema in MySQL (for tests only)
async fn init_schema(pool: &MySqlPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_snapshot (
            snapshot_id BIGINT PRIMARY KEY,
            snapshot_time DATETIME(6)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_schema (
            schema_id BIGINT PRIMARY KEY,
            schema_name VARCHAR(255) NOT NULL,
            path VARCHAR(1024) NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            begin_snapshot BIGINT NOT NULL,
            end_snapshot BIGINT
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_table (
            table_id BIGINT PRIMARY KEY,
            schema_id BIGINT NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            path VARCHAR(1024) NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            begin_snapshot BIGINT NOT NULL,
            end_snapshot BIGINT,
            FOREIGN KEY (schema_id) REFERENCES ducklake_schema(schema_id)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_column (
            column_id BIGINT PRIMARY KEY,
            table_id BIGINT NOT NULL,
            column_name VARCHAR(255) NOT NULL,
            column_type VARCHAR(255) NOT NULL,
            column_order INTEGER NOT NULL,
            nulls_allowed BOOLEAN,
            FOREIGN KEY (table_id) REFERENCES ducklake_table(table_id)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_data_file (
            data_file_id BIGINT PRIMARY KEY,
            table_id BIGINT NOT NULL,
            path VARCHAR(1024) NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            footer_size BIGINT,
            encryption_key VARCHAR(255),
            record_count BIGINT,
            begin_snapshot BIGINT NOT NULL DEFAULT 1,
            end_snapshot BIGINT,
            FOREIGN KEY (table_id) REFERENCES ducklake_table(table_id)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_delete_file (
            delete_file_id BIGINT PRIMARY KEY,
            data_file_id BIGINT NOT NULL,
            table_id BIGINT NOT NULL,
            path VARCHAR(1024) NOT NULL,
            path_is_relative BOOLEAN NOT NULL,
            file_size_bytes BIGINT NOT NULL,
            footer_size BIGINT,
            encryption_key VARCHAR(255),
            delete_count BIGINT,
            begin_snapshot BIGINT NOT NULL,
            end_snapshot BIGINT,
            FOREIGN KEY (data_file_id) REFERENCES ducklake_data_file(data_file_id),
            FOREIGN KEY (table_id) REFERENCES ducklake_table(table_id)
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_metadata (
            `key` VARCHAR(255) NOT NULL,
            value VARCHAR(1024) NOT NULL,
            scope VARCHAR(255),
            scope_id BIGINT,
            PRIMARY KEY (`key`)
        )",
    )
    .execute(pool)
    .await?;

    // MySQL indexes
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_schema_snapshot ON ducklake_schema(begin_snapshot, end_snapshot)",
    )
    .execute(pool)
    .await
    .ok(); // Ignore if already exists

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_table_schema ON ducklake_table(schema_id)")
        .execute(pool)
        .await
        .ok();

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_table_snapshot ON ducklake_table(begin_snapshot, end_snapshot)",
    )
    .execute(pool)
    .await
    .ok();

    Ok(())
}

/// Helper to create a MySQL provider with initialized schema
async fn create_mysql_provider()
-> anyhow::Result<(MySqlMetadataProvider, testcontainers::ContainerAsync<Mysql>)> {
    let container = Mysql::default().start().await?;

    let host = "127.0.0.1";
    let port = container.get_host_port_ipv4(3306).await?;
    let conn_str = format!("mysql://root@{}:{}/test", host, port);

    let provider = MySqlMetadataProvider::new(&conn_str)
        .await
        .expect("Failed to create provider");
    init_schema(&provider.pool).await?;

    Ok((provider, container))
}

/// Helper to populate test data in MySQL
async fn populate_test_data(provider: &MySqlMetadataProvider) -> anyhow::Result<()> {
    let pool = &provider.pool;

    // Insert snapshots
    sqlx::query("INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time) VALUES (?, NOW())")
        .bind(1i64)
        .execute(pool)
        .await?;

    sqlx::query("INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time) VALUES (?, NOW())")
        .bind(2i64)
        .execute(pool)
        .await?;

    // Insert metadata (data_path)
    sqlx::query(
        "INSERT INTO ducklake_metadata (`key`, value, scope, scope_id) VALUES (?, ?, NULL, NULL)",
    )
    .bind("data_path")
    .bind("file:///tmp/ducklake_data/")
    .execute(pool)
    .await?;

    // Insert schema
    sqlx::query(
        "INSERT INTO ducklake_schema (schema_id, schema_name, path, path_is_relative, begin_snapshot, end_snapshot)
         VALUES (?, ?, ?, ?, ?, ?)"
    )
    .bind(1i64)
    .bind("test_schema")
    .bind("test_schema/")
    .bind(true)
    .bind(1i64)
    .bind(None::<i64>)
    .execute(pool)
    .await?;

    // Insert another schema (only in snapshot 2)
    sqlx::query(
        "INSERT INTO ducklake_schema (schema_id, schema_name, path, path_is_relative, begin_snapshot, end_snapshot)
         VALUES (?, ?, ?, ?, ?, ?)"
    )
    .bind(2i64)
    .bind("schema2")
    .bind("schema2/")
    .bind(true)
    .bind(2i64)
    .bind(None::<i64>)
    .execute(pool)
    .await?;

    // Insert table
    sqlx::query(
        "INSERT INTO ducklake_table (table_id, schema_id, table_name, path, path_is_relative, begin_snapshot, end_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(1i64)
    .bind(1i64)
    .bind("users")
    .bind("users/")
    .bind(true)
    .bind(1i64)
    .bind(None::<i64>)
    .execute(pool)
    .await?;

    // Insert another table (only in snapshot 2)
    sqlx::query(
        "INSERT INTO ducklake_table (table_id, schema_id, table_name, path, path_is_relative, begin_snapshot, end_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(2i64)
    .bind(1i64)
    .bind("products")
    .bind("products/")
    .bind(true)
    .bind(2i64)
    .bind(None::<i64>)
    .execute(pool)
    .await?;

    // Insert columns for users table
    sqlx::query(
        "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind("id")
    .bind("INT")
    .bind(0i32)
    .bind(false)
    .bind(1i64)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(2i64)
    .bind(1i64)
    .bind("name")
    .bind("VARCHAR")
    .bind(1i32)
    .bind(true)
    .bind(1i64)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(3i64)
    .bind(1i64)
    .bind("email")
    .bind("VARCHAR")
    .bind(2i32)
    .bind(true)
    .bind(1i64)
    .execute(pool)
    .await?;

    // Insert data file
    sqlx::query(
        "INSERT INTO ducklake_data_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(1i64)
    .bind(1i64)
    .bind("data_001.parquet")
    .bind(true)
    .bind(1024i64)
    .bind(Some(128i64))
    .bind(1i64)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO ducklake_data_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(2i64)
    .bind(1i64)
    .bind("data_002.parquet")
    .bind(true)
    .bind(2048i64)
    .bind(Some(256i64))
    .bind(1i64)
    .execute(pool)
    .await?;

    // Insert delete file for first data file
    sqlx::query(
        "INSERT INTO ducklake_delete_file (delete_file_id, data_file_id, table_id, path, path_is_relative,
                                           file_size_bytes, footer_size, delete_count, begin_snapshot, end_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(1i64)
    .bind(1i64)
    .bind(1i64)
    .bind("data_001.delete.parquet")
    .bind(true)
    .bind(512i64)
    .bind(Some(64i64))
    .bind(Some(5i64))
    .bind(1i64)
    .bind(None::<i64>)
    .execute(pool)
    .await?;

    Ok(())
}

/// Helper to populate MySQL with metadata from a DuckDB-created catalog
async fn populate_from_duckdb_catalog(
    provider: &MySqlMetadataProvider,
) -> anyhow::Result<(String, TempDir)> {
    // Step 1: Create temporary directory and DuckDB catalog with real Parquet files
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("source.ducklake");
    common::create_catalog_no_deletes(&catalog_path)?;

    // Step 2: Read metadata from DuckDB catalog
    let duckdb_provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())?;

    let data_path = duckdb_provider.get_data_path()?;
    let snapshots = duckdb_provider.list_snapshots()?;
    let current_snapshot = snapshots
        .last()
        .ok_or_else(|| anyhow::anyhow!("No snapshots found"))?;

    let schemas = duckdb_provider.list_schemas(current_snapshot.snapshot_id)?;

    // Step 3: Populate MySQL with metadata from DuckDB
    let pool = &provider.pool;

    // Insert snapshots
    for snapshot in &snapshots {
        let timestamp_value: Option<sqlx::types::chrono::NaiveDateTime> =
            snapshot.snapshot_time.as_ref().and_then(|ts_str| {
                sqlx::types::chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%d %H:%M:%S%.6f")
                    .ok()
            });

        sqlx::query("INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time) VALUES (?, ?)")
            .bind(snapshot.snapshot_id)
            .bind(timestamp_value)
            .execute(pool)
            .await?;
    }

    // Insert data_path metadata
    sqlx::query(
        "INSERT INTO ducklake_metadata (`key`, value, scope, scope_id) VALUES (?, ?, NULL, NULL)",
    )
    .bind("data_path")
    .bind(&data_path)
    .execute(pool)
    .await?;

    // Insert schemas, tables, columns, and files
    for schema in &schemas {
        sqlx::query(
            "INSERT INTO ducklake_schema (schema_id, schema_name, path, path_is_relative, begin_snapshot, end_snapshot)
             VALUES (?, ?, ?, ?, ?, ?)"
        )
        .bind(schema.schema_id)
        .bind(&schema.schema_name)
        .bind(&schema.path)
        .bind(schema.path_is_relative)
        .bind(1i64)
        .bind(None::<i64>)
        .execute(pool)
        .await?;

        let tables = duckdb_provider.list_tables(schema.schema_id, current_snapshot.snapshot_id)?;

        for table in &tables {
            sqlx::query(
                "INSERT INTO ducklake_table (table_id, schema_id, table_name, path, path_is_relative, begin_snapshot, end_snapshot)
                 VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(table.table_id)
            .bind(schema.schema_id)
            .bind(&table.table_name)
            .bind(&table.path)
            .bind(table.path_is_relative)
            .bind(1i64)
            .bind(None::<i64>)
            .execute(pool)
            .await?;

            let columns = duckdb_provider.get_table_structure(table.table_id, current_snapshot.snapshot_id)?;

            for (order, column) in columns.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO ducklake_column (column_id, table_id, column_name, column_type, column_order, nulls_allowed)
                     VALUES (?, ?, ?, ?, ?, ?)"
                )
                .bind(column.column_id)
                .bind(table.table_id)
                .bind(&column.column_name)
                .bind(&column.column_type)
                .bind(order as i32)
                .bind(column.is_nullable)
                .execute(pool)
                .await?;
            }

            let files = duckdb_provider
                .get_table_files_for_select(table.table_id, current_snapshot.snapshot_id)?;

            for (file_idx, file) in files.iter().enumerate() {
                let data_file_id = table.table_id * 1000 + file_idx as i64 + 1;

                sqlx::query(
                    "INSERT INTO ducklake_data_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, footer_size, begin_snapshot)
                     VALUES (?, ?, ?, ?, ?, ?, ?)"
                )
                .bind(data_file_id)
                .bind(table.table_id)
                .bind(&file.file.path)
                .bind(file.file.path_is_relative)
                .bind(file.file.file_size_bytes)
                .bind(file.file.footer_size)
                .bind(1i64)
                .execute(pool)
                .await?;

                if let Some(delete_file) = &file.delete_file {
                    let delete_file_id = data_file_id;

                    sqlx::query(
                        "INSERT INTO ducklake_delete_file (delete_file_id, data_file_id, table_id, path, path_is_relative,
                                                           file_size_bytes, footer_size, delete_count, begin_snapshot, end_snapshot)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    )
                    .bind(delete_file_id)
                    .bind(data_file_id)
                    .bind(table.table_id)
                    .bind(&delete_file.path)
                    .bind(delete_file.path_is_relative)
                    .bind(delete_file.file_size_bytes)
                    .bind(delete_file.footer_size)
                    .bind(None::<i64>)
                    .bind(1i64)
                    .bind(None::<i64>)
                    .execute(pool)
                    .await?;
                }
            }
        }
    }

    Ok((data_path, temp_dir))
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_schema_initialization_idempotent() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    // Initialize schema again - should be idempotent
    init_schema(&provider.pool)
        .await
        .expect("Schema initialization should be idempotent");

    // Verify tables exist by querying them
    let result = provider.get_current_snapshot();
    assert!(result.is_ok(), "Should be able to query after init");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_current_snapshot() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    // Initially should be 0 (no snapshots)
    let snapshot_id = provider
        .get_current_snapshot()
        .expect("Should get current snapshot");
    assert_eq!(snapshot_id, 0, "Should be 0 when no snapshots exist");

    // Populate test data
    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    // Should now return 2 (max snapshot_id)
    let snapshot_id = provider
        .get_current_snapshot()
        .expect("Should get current snapshot");
    assert_eq!(snapshot_id, 2, "Should return max snapshot_id");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_data_path() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    let data_path = provider.get_data_path().expect("Should get data path");

    assert_eq!(data_path, "file:///tmp/ducklake_data/");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_list_snapshots() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    let snapshots = provider.list_snapshots().expect("Should list snapshots");

    assert_eq!(snapshots.len(), 2, "Should have 2 snapshots");
    assert_eq!(snapshots[0].snapshot_id, 1);
    assert_eq!(snapshots[1].snapshot_id, 2);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_list_schemas_snapshot_isolation() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    // Snapshot 1 should only see test_schema
    let schemas = provider
        .list_schemas(1)
        .expect("Should list schemas for snapshot 1");

    assert_eq!(schemas.len(), 1, "Snapshot 1 should have 1 schema");
    assert_eq!(schemas[0].schema_name, "test_schema");

    // Snapshot 2 should see both schemas
    let schemas = provider
        .list_schemas(2)
        .expect("Should list schemas for snapshot 2");

    assert_eq!(schemas.len(), 2, "Snapshot 2 should have 2 schemas");

    let schema_names: Vec<_> = schemas.iter().map(|s| s.schema_name.as_str()).collect();
    assert!(schema_names.contains(&"test_schema"));
    assert!(schema_names.contains(&"schema2"));
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_schema_by_name() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    // Should find test_schema
    let schema = provider
        .get_schema_by_name("test_schema", 1)
        .expect("Should get schema by name");

    assert!(schema.is_some(), "Should find test_schema");
    let schema = schema.unwrap();
    assert_eq!(schema.schema_name, "test_schema");
    assert_eq!(schema.schema_id, 1);

    // Should not find non-existent schema
    let schema = provider
        .get_schema_by_name("nonexistent", 1)
        .expect("Should handle non-existent schema");

    assert!(schema.is_none(), "Should not find nonexistent schema");

    // schema2 should not be visible in snapshot 1
    let schema = provider
        .get_schema_by_name("schema2", 1)
        .expect("Should handle schema not in snapshot");

    assert!(
        schema.is_none(),
        "schema2 should not be visible in snapshot 1"
    );

    // schema2 should be visible in snapshot 2
    let schema = provider
        .get_schema_by_name("schema2", 2)
        .expect("Should get schema by name");

    assert!(schema.is_some(), "schema2 should be visible in snapshot 2");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_list_tables() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    // Snapshot 1 should only see users table
    let tables = provider.list_tables(1, 1).expect("Should list tables");

    assert_eq!(tables.len(), 1, "Snapshot 1 should have 1 table");
    assert_eq!(tables[0].table_name, "users");

    // Snapshot 2 should see both tables
    let tables = provider.list_tables(1, 2).expect("Should list tables");

    assert_eq!(tables.len(), 2, "Snapshot 2 should have 2 tables");

    let table_names: Vec<_> = tables.iter().map(|t| t.table_name.as_str()).collect();
    assert!(table_names.contains(&"users"));
    assert!(table_names.contains(&"products"));
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_table_by_name() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    // Should find users table
    let table = provider
        .get_table_by_name(1, "users", 1)
        .expect("Should get table by name");

    assert!(table.is_some(), "Should find users table");
    let table = table.unwrap();
    assert_eq!(table.table_name, "users");
    assert_eq!(table.table_id, 1);

    // Should not find non-existent table
    let table = provider
        .get_table_by_name(1, "nonexistent", 1)
        .expect("Should handle non-existent table");

    assert!(table.is_none(), "Should not find nonexistent table");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_table_exists() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    // users table should exist
    let exists = provider
        .table_exists(1, "users", 1)
        .expect("Should check if table exists");

    assert!(exists, "users table should exist");

    // nonexistent table should not exist
    let exists = provider
        .table_exists(1, "nonexistent", 1)
        .expect("Should check if table exists");

    assert!(!exists, "nonexistent table should not exist");

    // products table should not exist in snapshot 1
    let exists = provider
        .table_exists(1, "products", 1)
        .expect("Should check if table exists");

    assert!(!exists, "products table should not exist in snapshot 1");

    // products table should exist in snapshot 2
    let exists = provider
        .table_exists(1, "products", 2)
        .expect("Should check if table exists");

    assert!(exists, "products table should exist in snapshot 2");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_table_structure() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    let columns = provider
        .get_table_structure(1, 1)
        .expect("Should get table structure");

    assert_eq!(columns.len(), 3, "users table should have 3 columns");

    assert_eq!(columns[0].column_name, "id");
    assert_eq!(columns[0].column_type, "INT");

    assert_eq!(columns[1].column_name, "name");
    assert_eq!(columns[1].column_type, "VARCHAR");

    assert_eq!(columns[2].column_name, "email");
    assert_eq!(columns[2].column_type, "VARCHAR");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_table_files_for_select() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    let files = provider
        .get_table_files_for_select(1, 1)
        .expect("Should get table files");

    assert_eq!(files.len(), 2, "Should have 2 data files");

    // First file should have a delete file
    assert_eq!(files[0].file.path, "data_001.parquet");
    assert_eq!(files[0].file.file_size_bytes, 1024);
    assert_eq!(files[0].file.footer_size, Some(128));
    assert!(
        files[0].delete_file.is_some(),
        "First file should have delete file"
    );

    let delete_file = files[0].delete_file.as_ref().unwrap();
    assert_eq!(delete_file.path, "data_001.delete.parquet");
    assert_eq!(delete_file.file_size_bytes, 512);

    // Second file should not have a delete file
    assert_eq!(files[1].file.path, "data_002.parquet");
    assert_eq!(files[1].file.file_size_bytes, 2048);
    assert_eq!(files[1].file.footer_size, Some(256));
    assert!(
        files[1].delete_file.is_none(),
        "Second file should not have delete file"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_list_all_tables() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    // Snapshot 1 should only see 1 table
    let tables = provider.list_all_tables(1).expect("Should list all tables");

    assert_eq!(tables.len(), 1, "Snapshot 1 should have 1 table");
    assert_eq!(tables[0].schema_name, "test_schema");
    assert_eq!(tables[0].table.table_name, "users");

    // Snapshot 2 should see 2 tables
    let tables = provider.list_all_tables(2).expect("Should list all tables");

    assert_eq!(tables.len(), 2, "Snapshot 2 should have 2 tables");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_list_all_columns() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    let columns = provider
        .list_all_columns(1)
        .expect("Should list all columns");

    assert_eq!(columns.len(), 3, "Should have 3 columns from users table");

    assert_eq!(columns[0].schema_name, "test_schema");
    assert_eq!(columns[0].table_name, "users");
    assert_eq!(columns[0].column.column_name, "id");

    assert_eq!(columns[1].column.column_name, "name");
    assert_eq!(columns[2].column.column_name, "email");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_list_all_files() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    let files = provider.list_all_files(1).expect("Should list all files");

    assert_eq!(files.len(), 2, "Should have 2 files");

    assert_eq!(files[0].schema_name, "test_schema");
    assert_eq!(files[0].table_name, "users");
    assert_eq!(files[0].file.file.path, "data_001.parquet");
    assert!(files[0].file.delete_file.is_some());

    assert_eq!(files[1].file.file.path, "data_002.parquet");
    assert!(files[1].file.delete_file.is_none());
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_concurrent_access() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    let provider = Arc::new(provider);

    // Spawn 10 concurrent tasks
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let provider = provider.clone();
        let task = tokio::spawn(async move {
            let _snapshot = provider
                .get_current_snapshot()
                .expect("Should get snapshot");
            let _schemas = provider.list_schemas(1).expect("Should list schemas");
            let _tables = provider.list_tables(1, 1).expect("Should list tables");
            let _columns = provider
                .get_table_structure(1, 1)
                .expect("Should get structure");
        });
        tasks.push(task);
    }

    for task in tasks {
        task.await.expect("Task should complete successfully");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_datafusion_integration() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    populate_test_data(&provider)
        .await
        .expect("Failed to populate test data");

    let catalog = DuckLakeCatalog::new(provider).expect("Should create catalog");

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Query information_schema
    let df = ctx
        .sql("SELECT schema_name FROM ducklake.information_schema.schemata")
        .await
        .expect("Should query information_schema");

    let results = df.collect().await.expect("Should collect results");
    assert!(!results.is_empty(), "Should have schema results");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_error_invalid_connection_string() {
    let result = MySqlMetadataProvider::new("invalid://connection:string").await;
    assert!(
        result.is_err(),
        "Should fail with invalid connection string"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_error_connection_refused() {
    let result = MySqlMetadataProvider::new("mysql://root@localhost:9999/db").await;
    assert!(result.is_err(), "Should fail when connection is refused");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_query_real_parquet_files() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    let (_data_path, _temp_dir) = populate_from_duckdb_catalog(&provider)
        .await
        .expect("Failed to populate from DuckDB catalog");

    let catalog = DuckLakeCatalog::new(provider).expect("Should create catalog");

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Query actual table data
    let df = ctx
        .sql("SELECT * FROM ducklake.main.users ORDER BY id")
        .await
        .expect("Should query table data");

    let results = df.collect().await.expect("Should collect results");

    assert_eq!(results.len(), 1, "Should have one batch");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 4, "Should have 4 rows");

    // Verify schema
    // 3 real columns + 2 virtual columns (filename, file_row_number)
    assert_eq!(
        batch.num_columns(),
        5,
        "Should have 5 columns (3 real + 2 virtual)"
    );
    let schema = batch.schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
    assert_eq!(schema.field(2).name(), "email");

    // Verify first row data
    use datafusion::arrow::array::{Int32Array, StringArray};
    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let email_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(id_col.value(0), 1);
    assert_eq!(name_col.value(0), "Alice");
    assert_eq!(email_col.value(0), "alice@example.com");

    assert_eq!(id_col.value(1), 2);
    assert_eq!(name_col.value(1), "Bob");
    assert_eq!(email_col.value(1), "bob@example.com");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_query_with_filter() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    let (_data_path, _temp_dir) = populate_from_duckdb_catalog(&provider)
        .await
        .expect("Failed to populate from DuckDB catalog");

    let catalog = DuckLakeCatalog::new(provider).expect("Should create catalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Query with WHERE filter
    let df = ctx
        .sql("SELECT name, email FROM ducklake.main.users WHERE id > 2 ORDER BY id")
        .await
        .expect("Should query with filter");

    let results = df.collect().await.expect("Should collect results");

    assert_eq!(results.len(), 1, "Should have one batch");
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 2, "Should have 2 rows (Charlie, Diana)");

    use datafusion::arrow::array::StringArray;
    let name_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(name_col.value(0), "Charlie");
    assert_eq!(name_col.value(1), "Diana");
}

// --- View methods ---

async fn create_view_tables(pool: &MySqlPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_view (
            view_id BIGINT NOT NULL,
            view_uuid VARCHAR(255),
            schema_id BIGINT NOT NULL,
            view_name VARCHAR(255) NOT NULL,
            dialect VARCHAR(255),
            `sql` TEXT NOT NULL,
            column_aliases TEXT,
            begin_snapshot BIGINT NOT NULL,
            end_snapshot BIGINT
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn populate_view_data(pool: &MySqlPool) -> anyhow::Result<()> {
    // Insert a view visible from snapshot 1
    sqlx::query(
        "INSERT INTO ducklake_view (view_id, schema_id, view_name, `sql`, begin_snapshot, end_snapshot)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind("user_emails")
    .bind("SELECT id, email FROM users")
    .bind(1i64)
    .bind(None::<i64>)
    .execute(pool)
    .await?;

    // Insert another view visible from snapshot 2
    sqlx::query(
        "INSERT INTO ducklake_view (view_id, schema_id, view_name, `sql`, begin_snapshot, end_snapshot)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(2i64)
    .bind(1i64)
    .bind("user_names")
    .bind("SELECT id, name FROM users")
    .bind(2i64)
    .bind(None::<i64>)
    .execute(pool)
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_list_views() {
    let (provider, _container) = create_mysql_provider().await.unwrap();
    populate_test_data(&provider).await.unwrap();
    create_view_tables(&provider.pool).await.unwrap();
    populate_view_data(&provider.pool).await.unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    // Snapshot 1 should see 1 view
    let views = provider.list_views(1, 1).unwrap();
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].view_name, "user_emails");
    assert_eq!(views[0].sql, "SELECT id, email FROM users");

    // Snapshot 2 should see 2 views
    let views = provider.list_views(1, 2).unwrap();
    assert_eq!(views.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_view_by_name() {
    let (provider, _container) = create_mysql_provider().await.unwrap();
    populate_test_data(&provider).await.unwrap();
    create_view_tables(&provider.pool).await.unwrap();
    populate_view_data(&provider.pool).await.unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    // Should find user_emails view
    let view = provider.get_view_by_name(1, "user_emails", 1).unwrap();
    assert!(view.is_some());
    let view = view.unwrap();
    assert_eq!(view.view_name, "user_emails");
    assert_eq!(view.view_id, 1);

    // Should not find non-existent view
    let view = provider.get_view_by_name(1, "nonexistent", 1).unwrap();
    assert!(view.is_none());

    // user_names should not be visible in snapshot 1
    let view = provider.get_view_by_name(1, "user_names", 1).unwrap();
    assert!(view.is_none());

    // user_names should be visible in snapshot 2
    let view = provider.get_view_by_name(1, "user_names", 2).unwrap();
    assert!(view.is_some());
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_view_exists() {
    let (provider, _container) = create_mysql_provider().await.unwrap();
    populate_test_data(&provider).await.unwrap();
    create_view_tables(&provider.pool).await.unwrap();
    populate_view_data(&provider.pool).await.unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    assert!(provider.view_exists(1, "user_emails", 1).unwrap());
    assert!(!provider.view_exists(1, "nonexistent", 1).unwrap());
    assert!(!provider.view_exists(1, "user_names", 1).unwrap());
    assert!(provider.view_exists(1, "user_names", 2).unwrap());
}

// --- File column stats ---

async fn create_stats_tables(pool: &MySqlPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_file_column_stats (
            data_file_id BIGINT NOT NULL,
            table_id BIGINT NOT NULL,
            column_id BIGINT NOT NULL,
            column_size_bytes BIGINT,
            value_count BIGINT,
            null_count BIGINT,
            min_value VARCHAR(1024),
            max_value VARCHAR(1024),
            contains_nan BOOLEAN,
            extra_stats VARCHAR(1024)
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_file_column_stats() {
    let (provider, _container) = create_mysql_provider().await.unwrap();
    populate_test_data(&provider).await.unwrap();
    create_stats_tables(&provider.pool).await.unwrap();

    let pool = &provider.pool;

    // Insert column stats for file 1, column 1
    sqlx::query(
        "INSERT INTO ducklake_file_column_stats (data_file_id, table_id, column_id, null_count, min_value, max_value)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind(1i64)
    .bind(0i64)
    .bind("1")
    .bind("100")
    .execute(pool)
    .await
    .unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    let stats = provider.get_file_column_stats(1, 1).unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].data_file_id, 1);
    assert_eq!(stats[0].column_name, "id");
    assert_eq!(stats[0].null_count, Some(0));
    assert_eq!(stats[0].min_value, Some("1".to_string()));
    assert_eq!(stats[0].max_value, Some("100".to_string()));
}

// --- Row count ---

async fn create_inlined_data_tables(pool: &MySqlPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_inlined_data_tables (
            table_id BIGINT,
            table_name VARCHAR(255),
            schema_version BIGINT
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_table_row_count() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    let pool = &provider.pool;

    create_inlined_data_tables(pool).await.unwrap();

    // Set up minimal data with record_count
    sqlx::query("INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time) VALUES (?, NOW())")
        .bind(1i64)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO ducklake_metadata (`key`, value, scope, scope_id) VALUES (?, ?, NULL, NULL)",
    )
    .bind("data_path")
    .bind("/tmp/test/")
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO ducklake_schema (schema_id, schema_name, path, path_is_relative, begin_snapshot)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind("main")
    .bind("main/")
    .bind(true)
    .bind(1i64)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO ducklake_table (table_id, schema_id, table_name, path, path_is_relative, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind("t1")
    .bind("t1/")
    .bind(true)
    .bind(1i64)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO ducklake_data_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, record_count, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind("f1.parquet")
    .bind(true)
    .bind(1024i64)
    .bind(50i64)
    .bind(1i64)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO ducklake_data_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, record_count, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(2i64)
    .bind(1i64)
    .bind("f2.parquet")
    .bind(true)
    .bind(2048i64)
    .bind(30i64)
    .bind(1i64)
    .execute(pool)
    .await
    .unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    let count = provider.get_table_row_count(1, 1).unwrap();
    assert_eq!(count, Some(80)); // 50 + 30
}

// --- Partition columns ---

async fn create_partition_tables(pool: &MySqlPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_partition_info (
            partition_id BIGINT,
            table_id BIGINT,
            begin_snapshot BIGINT,
            end_snapshot BIGINT
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_partition_column (
            partition_id BIGINT,
            table_id BIGINT,
            partition_key_index BIGINT,
            column_id BIGINT,
            transform VARCHAR(255)
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_file_partition_value (
            data_file_id BIGINT,
            table_id BIGINT,
            partition_key_index BIGINT,
            partition_value VARCHAR(1024)
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_partition_columns() {
    let (provider, _container) = create_mysql_provider().await.unwrap();
    populate_test_data(&provider).await.unwrap();
    create_partition_tables(&provider.pool).await.unwrap();

    let pool = &provider.pool;

    // Insert partition info
    sqlx::query(
        "INSERT INTO ducklake_partition_info (partition_id, table_id, begin_snapshot, end_snapshot)
         VALUES (?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind(1i64)
    .bind(None::<i64>)
    .execute(pool)
    .await
    .unwrap();

    // Insert partition column
    sqlx::query(
        "INSERT INTO ducklake_partition_column (partition_id, table_id, partition_key_index, column_id, transform)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind(0i64)
    .bind(1i64) // column_id for 'id'
    .bind(None::<String>)
    .execute(pool)
    .await
    .unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    let partition_cols = provider.get_partition_columns(1, 1).unwrap();
    assert_eq!(partition_cols.len(), 1);
    assert_eq!(partition_cols[0].partition_key_index, 0);
    assert_eq!(partition_cols[0].column_name, "id");

    // Table with no partitions should return empty
    let partition_cols = provider.get_partition_columns(2, 2).unwrap();
    assert!(partition_cols.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_file_partition_values() {
    let (provider, _container) = create_mysql_provider().await.unwrap();
    populate_test_data(&provider).await.unwrap();
    create_partition_tables(&provider.pool).await.unwrap();

    let pool = &provider.pool;

    // Insert partition values for data file 1
    sqlx::query(
        "INSERT INTO ducklake_file_partition_value (data_file_id, table_id, partition_key_index, partition_value)
         VALUES (?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind(0i64)
    .bind("value_a")
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO ducklake_file_partition_value (data_file_id, table_id, partition_key_index, partition_value)
         VALUES (?, ?, ?, ?)",
    )
    .bind(2i64)
    .bind(1i64)
    .bind(0i64)
    .bind("value_b")
    .execute(pool)
    .await
    .unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    let values = provider.get_file_partition_values(1, 1).unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].data_file_id, 1);
    assert_eq!(values[0].partition_value, Some("value_a".to_string()));
    assert_eq!(values[1].data_file_id, 2);
    assert_eq!(values[1].partition_value, Some("value_b".to_string()));
}

// --- Inlined data ---

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_inlined_data_no_table() {
    let (provider, _container) = create_mysql_provider().await.unwrap();
    populate_test_data(&provider).await.unwrap();
    create_inlined_data_tables(&provider.pool).await.unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    // No inlined data table registered - should return empty
    let data = provider.get_inlined_data(1, 1).unwrap();
    assert!(data.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_inlined_data_with_data() {
    let (provider, _container) = create_mysql_provider().await.unwrap();
    populate_test_data(&provider).await.unwrap();

    let pool = &provider.pool;

    create_inlined_data_tables(pool).await.unwrap();

    // Create an actual inlined data table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ducklake_inlined_users (
            row_id BIGINT,
            begin_snapshot BIGINT,
            end_snapshot BIGINT,
            name VARCHAR(255),
            age VARCHAR(255)
        )",
    )
    .execute(pool)
    .await
    .unwrap();

    // Register the inlined table
    sqlx::query(
        "INSERT INTO ducklake_inlined_data_tables (table_id, table_name, schema_version)
         VALUES (?, ?, ?)",
    )
    .bind(1i64)
    .bind("ducklake_inlined_users")
    .bind(1i64)
    .execute(pool)
    .await
    .unwrap();

    // Insert rows
    sqlx::query(
        "INSERT INTO ducklake_inlined_users (row_id, begin_snapshot, end_snapshot, name, age)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind(None::<i64>)
    .bind("Alice")
    .bind("30")
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO ducklake_inlined_users (row_id, begin_snapshot, end_snapshot, name, age)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(2i64)
    .bind(1i64)
    .bind(None::<i64>)
    .bind("Bob")
    .bind("25")
    .execute(pool)
    .await
    .unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    let data = provider.get_inlined_data(1, 1).unwrap();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0].column_names, vec!["name", "age"]);
    assert_eq!(
        data[0].values,
        vec![Some("Alice".to_string()), Some("30".to_string())]
    );
    assert_eq!(
        data[1].values,
        vec![Some("Bob".to_string()), Some("25".to_string())]
    );
}

// --- Row count with inlined data ---

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
async fn test_get_table_row_count_no_record_count() {
    let (provider, _container) = create_mysql_provider().await.unwrap();

    let pool = &provider.pool;

    create_inlined_data_tables(pool).await.unwrap();

    sqlx::query("INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time) VALUES (?, NOW())")
        .bind(1i64)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO ducklake_metadata (`key`, value, scope, scope_id) VALUES (?, ?, NULL, NULL)",
    )
    .bind("data_path")
    .bind("/tmp/test/")
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO ducklake_schema (schema_id, schema_name, path, path_is_relative, begin_snapshot)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind("main")
    .bind("main/")
    .bind(true)
    .bind(1i64)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO ducklake_table (table_id, schema_id, table_name, path, path_is_relative, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind("t1")
    .bind("t1/")
    .bind(true)
    .bind(1i64)
    .execute(pool)
    .await
    .unwrap();

    // Insert file WITHOUT record_count
    sqlx::query(
        "INSERT INTO ducklake_data_file (data_file_id, table_id, path, path_is_relative, file_size_bytes, begin_snapshot)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(1i64)
    .bind(1i64)
    .bind("f1.parquet")
    .bind(true)
    .bind(1024i64)
    .bind(1i64)
    .execute(pool)
    .await
    .unwrap();

    use datafusion_ducklake::metadata_provider::MetadataProvider;

    // Should return None when record_count is missing
    let count = provider.get_table_row_count(1, 1).unwrap();
    assert_eq!(count, None);
}
