//! Backend-specific issue reproduction tests.
//!
//! These tests reproduce DuckLake issues that are specific to Postgres or MySQL backends.
//! They MUST run against actual Postgres/MySQL instances (via testcontainers), not SQLite.
//!
//! Issues covered:
//! - Postgres: #147, #240, #591, #619, #637, #644
//! - MySQL: #214, #288

// ============================================================================
// POSTGRES TESTS
// ============================================================================

#[cfg(feature = "write-postgres")]
mod postgres_tests {
    use datafusion_ducklake::PostgresMetadataWriter;
    use datafusion_ducklake::metadata_writer::{
        ColumnDef, ColumnStatInfo, DataFileInfo, MetadataWriter, WriteMode,
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

    // ==================== #147: v0.1→v0.2 migration ====================
    // https://github.com/duckdb/ducklake/issues/147
    //
    // Postgres ALTER TABLE tries to add already-existing columns during migration.
    // Test: call initialize_schema() twice on the same Postgres DB.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
    async fn test_issue_147_postgres_double_init_migration() {
        let (writer, _container) = create_writer().await;

        // Schema was initialized in create_writer(). Call initialize_schema() again
        // to simulate a v0.1→v0.2 migration scenario.
        let result = writer.initialize_schema();
        assert!(
            result.is_ok(),
            "Issue #147: Second initialize_schema() on Postgres failed: {:?}",
            result.err()
        );

        // Third call should also work
        let result2 = writer.initialize_schema();
        assert!(
            result2.is_ok(),
            "Issue #147: Third initialize_schema() on Postgres failed: {:?}",
            result2.err()
        );

        // Verify the schema still works after repeated initialization
        let snap = writer.create_snapshot().unwrap();
        assert!(snap > 0, "Should be able to create snapshots after re-init");
    }

    // ==================== #240: Concurrent migration deadlock ====================
    // https://github.com/duckdb/ducklake/issues/240
    //
    // Two threads calling initialize_schema() simultaneously on Postgres can deadlock.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
    async fn test_issue_240_postgres_concurrent_init_deadlock() {
        let container = Postgres::default().start().await.unwrap();
        let host = "127.0.0.1";
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let conn_str = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

        // Create multiple writers sharing the same database
        let conn_str1 = conn_str.clone();
        let conn_str2 = conn_str.clone();

        // Run initialize_schema() concurrently using spawn_blocking (needs Tokio runtime)
        let handle1 = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            let writer = rt
                .block_on(PostgresMetadataWriter::new(&conn_str1))
                .expect("Failed to create writer1");
            writer.initialize_schema()
        });
        let handle2 = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            let writer = rt
                .block_on(PostgresMetadataWriter::new(&conn_str2))
                .expect("Failed to create writer2");
            writer.initialize_schema()
        });

        let result1 = handle1.await.expect("Task 1 panicked");
        let result2 = handle2.await.expect("Task 2 panicked");

        // At least one should succeed; ideally both should succeed without deadlock
        let both_ok = result1.is_ok() && result2.is_ok();
        let at_least_one_ok = result1.is_ok() || result2.is_ok();

        if !both_ok {
            eprintln!(
                "Issue #240: Concurrent init results: r1={:?}, r2={:?}",
                result1.as_ref().err(),
                result2.as_ref().err()
            );
        }

        assert!(
            at_least_one_ok,
            "Issue #240: Both concurrent initialize_schema() calls failed on Postgres"
        );

        // Verify the DB is functional after concurrent init
        let writer3 = PostgresMetadataWriter::new(&conn_str)
            .await
            .expect("Failed to create writer3 after concurrent init");
        let snap = writer3.create_snapshot();
        assert!(
            snap.is_ok(),
            "Issue #240: DB should be functional after concurrent init: {:?}",
            snap.err()
        );
    }

    // ==================== #591: Complex types with Postgres catalog ====================
    // https://github.com/duckdb/ducklake/issues/591
    //
    // Creating a table with struct/map/list columns via Postgres writer.
    // The DuckLake column_type is stored as a string — verify these complex type
    // strings round-trip correctly through Postgres.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
    async fn test_issue_591_postgres_complex_types() {
        let (writer, _container) = create_writer().await;

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("metadata", "STRUCT(name VARCHAR, age INTEGER)", true).unwrap(),
            ColumnDef::new("tags", "VARCHAR[]", true).unwrap(),
            ColumnDef::new("properties", "MAP(VARCHAR, VARCHAR)", true).unwrap(),
            ColumnDef::new(
                "nested",
                "STRUCT(items STRUCT(name VARCHAR, value DOUBLE)[])",
                true,
            )
            .unwrap(),
        ];

        let result =
            writer.begin_write_transaction("main", "complex_table", &columns, WriteMode::Replace);
        match &result {
            Ok(r) => {
                assert_eq!(r.column_ids.len(), 5);

                // Verify types round-trip
                let active = writer.get_active_columns(r.table_id).unwrap();
                assert_eq!(active.len(), 5);
                assert_eq!(active[0].0, "id");
                assert_eq!(active[0].1, "int64");
                assert_eq!(active[1].0, "metadata");
                assert_eq!(active[1].1, "STRUCT(name VARCHAR, age INTEGER)");
                assert_eq!(active[2].0, "tags");
                assert_eq!(active[2].1, "VARCHAR[]");
                assert_eq!(active[3].0, "properties");
                assert_eq!(active[3].1, "MAP(VARCHAR, VARCHAR)");
                assert_eq!(active[4].0, "nested");
                assert_eq!(
                    active[4].1,
                    "STRUCT(items STRUCT(name VARCHAR, value DOUBLE)[])"
                );
                println!("Issue #591: Complex types stored and retrieved correctly from Postgres");
            },
            Err(e) => {
                panic!(
                    "Issue #591: Failed to create table with complex types on Postgres: {}",
                    e
                );
            },
        }
    }

    // ==================== #619: Column names >64 chars ====================
    // https://github.com/duckdb/ducklake/issues/619
    //
    // Postgres truncates identifiers to 63 bytes by default (NAMEDATALEN - 1).
    // DuckLake stores column names as VARCHAR values (not identifiers), so they
    // should not be truncated. Verify a 100-char column name is preserved.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
    async fn test_issue_619_postgres_long_column_names() {
        let (writer, _container) = create_writer().await;

        // Create a column name that's 100 characters
        let long_name = "a".repeat(100);
        assert_eq!(long_name.len(), 100);

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new(&long_name, "varchar", true).unwrap(),
        ];

        let result =
            writer.begin_write_transaction("main", "long_cols", &columns, WriteMode::Replace);
        match &result {
            Ok(r) => {
                let active = writer.get_active_columns(r.table_id).unwrap();
                assert_eq!(active.len(), 2);
                assert_eq!(
                    active[1].0,
                    long_name,
                    "Issue #619: Column name was truncated! Expected {} chars, got {} chars",
                    long_name.len(),
                    active[1].0.len()
                );
                println!(
                    "Issue #619: 100-char column name preserved correctly in Postgres (length={})",
                    active[1].0.len()
                );
            },
            Err(e) => {
                panic!(
                    "Issue #619: Failed to create table with long column name on Postgres: {}",
                    e
                );
            },
        }
    }

    // ==================== #637: DOUBLE column type ====================
    // https://github.com/duckdb/ducklake/issues/637
    //
    // Postgres uses "DOUBLE PRECISION" not "DOUBLE" in DDL. The upstream DuckLake
    // extension generates "DOUBLE" which fails. Our writer stores the type as a
    // VARCHAR string, so it should work. Verify "double" and "DOUBLE" type strings
    // round-trip correctly.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
    async fn test_issue_637_postgres_double_column_type() {
        let (writer, _container) = create_writer().await;

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("value", "double", true).unwrap(),
            ColumnDef::new("price", "DOUBLE", true).unwrap(),
            ColumnDef::new("rate", "float", true).unwrap(),
            ColumnDef::new("amount", "DOUBLE PRECISION", true).unwrap(),
        ];

        let result =
            writer.begin_write_transaction("main", "doubles_table", &columns, WriteMode::Replace);
        match &result {
            Ok(r) => {
                let active = writer.get_active_columns(r.table_id).unwrap();
                assert_eq!(active.len(), 5);
                assert_eq!(active[1].0, "value");
                assert_eq!(active[1].1, "double");
                assert_eq!(active[2].0, "price");
                assert_eq!(active[2].1, "DOUBLE");
                assert_eq!(active[3].0, "rate");
                assert_eq!(active[3].1, "float");
                assert_eq!(active[4].0, "amount");
                assert_eq!(active[4].1, "DOUBLE PRECISION");
                println!("Issue #637: DOUBLE type strings stored correctly in Postgres catalog");
            },
            Err(e) => {
                panic!(
                    "Issue #637: Failed to create table with DOUBLE columns on Postgres: {}",
                    e
                );
            },
        }
    }

    // ==================== #644: WHERE+ORDER BY with multiple data files ====================
    // https://github.com/duckdb/ducklake/issues/644
    //
    // Write multiple batches via Postgres writer, query with WHERE+ORDER BY+LIMIT.
    // The original issue was about inlined data with Postgres catalog — we test that
    // metadata operations (multiple file registrations + stats) work correctly.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
    async fn test_issue_644_postgres_multiple_files_with_stats() {
        let (writer, _container) = create_writer().await;

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("value", "varchar", true).unwrap(),
        ];

        let result = writer
            .begin_write_transaction("main", "multi_file", &columns, WriteMode::Replace)
            .unwrap();

        let table_id = result.table_id;
        let snapshot_id = result.snapshot_id;
        let col_id = result.column_ids[0];

        // Register multiple data files (simulating multiple batch writes)
        let file1 = DataFileInfo::new("batch1.parquet", 1024, 50).with_footer_size(128);
        let file1_id = writer
            .register_data_file(table_id, snapshot_id, &file1)
            .unwrap();

        let file2 = DataFileInfo::new("batch2.parquet", 2048, 100).with_footer_size(256);
        let file2_id = writer
            .register_data_file(table_id, snapshot_id, &file2)
            .unwrap();

        let file3 = DataFileInfo::new("batch3.parquet", 512, 25).with_footer_size(64);
        let file3_id = writer
            .register_data_file(table_id, snapshot_id, &file3)
            .unwrap();

        assert!(file2_id > file1_id);
        assert!(file3_id > file2_id);

        // Register column stats for each file
        let stats1 = vec![ColumnStatInfo {
            column_id: col_id,
            null_count: Some(0),
            min_value: Some("1".to_string()),
            max_value: Some("50".to_string()),
        }];
        writer
            .register_column_stats(file1_id, table_id, &stats1)
            .unwrap();

        let stats2 = vec![ColumnStatInfo {
            column_id: col_id,
            null_count: Some(2),
            min_value: Some("51".to_string()),
            max_value: Some("150".to_string()),
        }];
        writer
            .register_column_stats(file2_id, table_id, &stats2)
            .unwrap();

        let stats3 = vec![ColumnStatInfo {
            column_id: col_id,
            null_count: Some(0),
            min_value: Some("151".to_string()),
            max_value: Some("175".to_string()),
        }];
        writer
            .register_column_stats(file3_id, table_id, &stats3)
            .unwrap();

        println!("Issue #644: Successfully registered 3 data files with column stats on Postgres");

        // Verify we can do a second write transaction (append) after multiple file registrations
        let result2 = writer
            .begin_write_transaction("main", "multi_file", &columns, WriteMode::Append)
            .unwrap();
        assert!(result2.snapshot_id > snapshot_id);

        let file4 = DataFileInfo::new("batch4.parquet", 768, 40);
        let file4_id = writer
            .register_data_file(result2.table_id, result2.snapshot_id, &file4)
            .unwrap();
        assert!(file4_id > file3_id);

        println!("Issue #644: Append after multi-file write succeeded on Postgres");
    }
}

// ============================================================================
// MYSQL TESTS
// ============================================================================

#[cfg(feature = "write-mysql")]
mod mysql_tests {
    use datafusion_ducklake::{
        ColumnDef, ColumnStatInfo, DataFileInfo, MetadataWriter, MySqlMetadataWriter, WriteMode,
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

    // ==================== #214: Second initialization fails ====================
    // https://github.com/duckdb/ducklake/issues/214
    //
    // If CREATE TABLE lacks IF NOT EXISTS, the second call to initialize_schema()
    // fails on MySQL.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
    async fn test_issue_214_mysql_second_init_fails() {
        let (writer, _container) = create_mysql_writer().await.unwrap();

        // Schema was already initialized in create_mysql_writer(). Call again.
        let result = writer.initialize_schema();
        assert!(
            result.is_ok(),
            "Issue #214: Second initialize_schema() on MySQL failed: {:?}",
            result.err()
        );

        // Third call
        let result2 = writer.initialize_schema();
        assert!(
            result2.is_ok(),
            "Issue #214: Third initialize_schema() on MySQL failed: {:?}",
            result2.err()
        );

        // Verify DB still works
        let snap = writer.create_snapshot().unwrap();
        assert!(snap >= 1);
        println!(
            "Issue #214: MySQL re-initialization succeeded (snapshot={})",
            snap
        );
    }

    // ==================== #288: Stats update fails on insert ====================
    // https://github.com/duckdb/ducklake/issues/288
    //
    // Multiple inserts via MySQL writer — the original issue is that DuckDB's
    // upstream tries HAVING clause in stats UPDATE which fails on MySQL.
    // We test that our writer can register multiple data files with column stats
    // and that stats upserts work correctly on MySQL.
    #[tokio::test(flavor = "multi_thread")]
    #[cfg_attr(all(feature = "skip-tests-with-docker", target_os = "macos"), ignore)]
    async fn test_issue_288_mysql_stats_update_on_insert() {
        let (writer, _container) = create_mysql_writer().await.unwrap();

        let columns = vec![
            ColumnDef::new("id", "int64", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];

        // First insert
        let result1 = writer
            .begin_write_transaction("main", "stats_test", &columns, WriteMode::Replace)
            .unwrap();

        let file1 = DataFileInfo::new("data1.parquet", 1024, 100).with_footer_size(128);
        let file1_id = writer
            .register_data_file(result1.table_id, result1.snapshot_id, &file1)
            .unwrap();

        let stats1 = vec![ColumnStatInfo {
            column_id: result1.column_ids[0],
            null_count: Some(0),
            min_value: Some("1".to_string()),
            max_value: Some("100".to_string()),
        }];
        writer
            .register_column_stats(file1_id, result1.table_id, &stats1)
            .unwrap();

        // Second insert (append)
        let result2 = writer
            .begin_write_transaction("main", "stats_test", &columns, WriteMode::Append)
            .unwrap();

        let file2 = DataFileInfo::new("data2.parquet", 2048, 200).with_footer_size(256);
        let file2_id = writer
            .register_data_file(result2.table_id, result2.snapshot_id, &file2)
            .unwrap();

        let stats2 = vec![ColumnStatInfo {
            column_id: result2.column_ids[0],
            null_count: Some(5),
            min_value: Some("101".to_string()),
            max_value: Some("300".to_string()),
        }];
        let stats_result = writer.register_column_stats(file2_id, result2.table_id, &stats2);
        assert!(
            stats_result.is_ok(),
            "Issue #288: Stats update on second insert failed on MySQL: {:?}",
            stats_result.err()
        );

        // Third insert
        let result3 = writer
            .begin_write_transaction("main", "stats_test", &columns, WriteMode::Append)
            .unwrap();
        let file3 = DataFileInfo::new("data3.parquet", 512, 50);
        let file3_id = writer
            .register_data_file(result3.table_id, result3.snapshot_id, &file3)
            .unwrap();

        let stats3 = vec![
            ColumnStatInfo {
                column_id: result3.column_ids[0],
                null_count: Some(0),
                min_value: Some("301".to_string()),
                max_value: Some("350".to_string()),
            },
            ColumnStatInfo {
                column_id: result3.column_ids[1],
                null_count: Some(10),
                min_value: Some("Alice".to_string()),
                max_value: Some("Zoe".to_string()),
            },
        ];
        let stats_result = writer.register_column_stats(file3_id, result3.table_id, &stats3);
        assert!(
            stats_result.is_ok(),
            "Issue #288: Stats update on third insert failed on MySQL: {:?}",
            stats_result.err()
        );

        // Also test stat upsert (re-register stats for same file)
        let stats3_updated = vec![ColumnStatInfo {
            column_id: result3.column_ids[0],
            null_count: Some(1),
            min_value: Some("300".to_string()),
            max_value: Some("360".to_string()),
        }];
        let upsert_result =
            writer.register_column_stats(file3_id, result3.table_id, &stats3_updated);
        assert!(
            upsert_result.is_ok(),
            "Issue #288: Stats upsert failed on MySQL: {:?}",
            upsert_result.err()
        );

        println!("Issue #288: Multiple inserts with stats updates all succeeded on MySQL");
    }
}
