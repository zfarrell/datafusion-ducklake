//! Compaction table function tests.
//!
//! Tests for ducklake_merge_adjacent_files, ducklake_rewrite_data_files,
//! ducklake_expire_snapshots, ducklake_cleanup_old_files, and
//! ducklake_delete_orphaned_files.
//!
//! Each test creates a fresh DuckLake catalog using DuckDB's native format,
//! then exercises compaction via DataFusion table functions and verifies results.

#![cfg(feature = "metadata-duckdb")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::prelude::*;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckdbMetadataProvider, register_ducklake_compaction_functions,
};

// ==================== Setup helpers ====================

/// DuckDB connection helper for creating test catalogs.
struct DuckDbHelper {
    conn: duckdb::Connection,
}

impl DuckDbHelper {
    fn open_with_data_path(catalog_path: &Path, data_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", [])
            .expect("load ducklake");
        let attach_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(
            &format!(
                "ATTACH '{}' AS dl (DATA_PATH '{}');",
                attach_path,
                data_path.display()
            ),
            [],
        )
        .expect("attach ducklake catalog with data path");
        DuckDbHelper { conn }
    }

    fn open(catalog_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", [])
            .expect("load ducklake");
        let attach_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(
            &format!("ATTACH '{}' AS dl;", attach_path),
            [],
        )
        .expect("attach ducklake catalog");
        DuckDbHelper { conn }
    }

    fn execute(&self, sql: &str) {
        self.conn
            .execute(sql, [])
            .unwrap_or_else(|e| panic!("DuckDB execute failed: {e}\nSQL: {sql}"));
    }

    fn query_count(&self, sql: &str) -> i64 {
        self.conn
            .query_row(sql, [], |row| row.get::<_, i64>(0))
            .unwrap_or_else(|e| panic!("DuckDB query_row failed: {e}\nSQL: {sql}"))
    }
}

/// Test environment.
struct CompactionEnv {
    _temp_dir: TempDir,
    catalog_path: PathBuf,
    data_path: PathBuf,
}

fn setup_env() -> CompactionEnv {
    let temp_dir = TempDir::new().expect("create temp dir");
    let catalog_path = temp_dir.path().join("compact.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("create data dir");
    CompactionEnv {
        _temp_dir: temp_dir,
        catalog_path,
        data_path,
    }
}

/// Open catalog in DataFusion with compaction functions registered.
fn open_df_with_compaction(catalog_path: &Path) -> SessionContext {
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())
        .expect("create DuckdbMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    register_ducklake_compaction_functions(&ctx, catalog_path.to_str().unwrap());
    ctx
}

/// Run a SQL query in DataFusion and return row count.
async fn df_row_count(ctx: &SessionContext, sql: &str) -> usize {
    let df = ctx.sql(sql).await.expect("DataFusion SQL failed");
    let batches = df.collect().await.expect("DataFusion collect failed");
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Run a SQL query in DataFusion and return results as string rows.
async fn df_query(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let df = ctx.sql(sql).await.expect("DataFusion SQL failed");
    let batches = df.collect().await.expect("DataFusion collect failed");
    let mut rows = Vec::new();
    for batch in &batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    row.push("NULL".to_string());
                } else {
                    row.push(arrow::util::display::array_value_to_string(col, row_idx).unwrap());
                }
            }
            rows.push(row);
        }
    }
    rows
}

// ==================== Tests ====================

/// Test merging small files: insert data in multiple batches, then merge.
/// Verifies data is preserved and file count is reduced.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_adjacent_files() {
    let env = setup_env();

    // Create catalog with multiple small files (one INSERT per file)
    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.t1 (id INTEGER, name VARCHAR)");
        db.execute("INSERT INTO dl.main.t1 VALUES (1, 'a')");
        db.execute("INSERT INTO dl.main.t1 VALUES (2, 'b')");
        db.execute("INSERT INTO dl.main.t1 VALUES (3, 'c')");
        db.execute("INSERT INTO dl.main.t1 VALUES (4, 'd')");
        db.execute("INSERT INTO dl.main.t1 VALUES (5, 'e')");

        // Verify 5+ files exist (one per INSERT)
        let file_count = db.query_count(
            "SELECT COUNT(*) FROM ducklake_list_files('dl', 't1')",
        );
        assert!(file_count >= 5, "Expected at least 5 files before merge, got {file_count}");
    }

    // Run merge via DataFusion
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let _result = df_query(&ctx, "SELECT * FROM ducklake_merge_adjacent_files()").await;
    }

    // Verify: files are merged and data is preserved
    {
        let db = DuckDbHelper::open(&env.catalog_path);
        let file_count = db.query_count(
            "SELECT COUNT(*) FROM ducklake_list_files('dl', 't1')",
        );
        assert!(file_count < 5, "Expected fewer files after merge, got {file_count}");

        let row_count = db.query_count("SELECT COUNT(*) FROM dl.main.t1");
        assert_eq!(row_count, 5, "Data should be preserved after merge");
    }

    // Also verify DataFusion can read the merged data
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let rows = df_query(
            &ctx,
            "SELECT id, name FROM ducklake.main.t1 ORDER BY id",
        )
        .await;
        assert_eq!(rows.len(), 5, "DataFusion should see 5 rows after merge");
        assert_eq!(rows[0], vec!["1", "a"]);
        assert_eq!(rows[4], vec!["5", "e"]);
    }
}

/// Test merging files for a specific table only.
#[tokio::test(flavor = "multi_thread")]
async fn test_merge_adjacent_files_specific_table() {
    let env = setup_env();

    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.t1 (id INTEGER)");
        db.execute("INSERT INTO dl.main.t1 VALUES (1)");
        db.execute("INSERT INTO dl.main.t1 VALUES (2)");
        db.execute("INSERT INTO dl.main.t1 VALUES (3)");

        db.execute("CREATE TABLE dl.main.t2 (id INTEGER)");
        db.execute("INSERT INTO dl.main.t2 VALUES (10)");
        db.execute("INSERT INTO dl.main.t2 VALUES (20)");
        db.execute("INSERT INTO dl.main.t2 VALUES (30)");
    }

    // Merge only t1
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        df_query(&ctx, "SELECT * FROM ducklake_merge_adjacent_files('t1')").await;
    }

    // Verify: t1 merged, t2 untouched
    {
        let db = DuckDbHelper::open(&env.catalog_path);
        let t1_files = db.query_count("SELECT COUNT(*) FROM ducklake_list_files('dl', 't1')");
        let t2_files = db.query_count("SELECT COUNT(*) FROM ducklake_list_files('dl', 't2')");
        assert!(t1_files < 3, "t1 should have fewer files after merge");
        assert!(t2_files >= 3, "t2 should still have all original files");
    }
}

/// Test rewriting files with deletes.
/// Insert data, delete some rows (creating delete files), then rewrite.
#[tokio::test(flavor = "multi_thread")]
async fn test_rewrite_data_files() {
    let env = setup_env();

    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.t1 (id INTEGER, name VARCHAR)");
        db.execute("INSERT INTO dl.main.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        db.execute("INSERT INTO dl.main.t1 VALUES (4, 'd'), (5, 'e')");
        db.execute("DELETE FROM dl.main.t1 WHERE id = 2");
        db.execute("DELETE FROM dl.main.t1 WHERE id = 4");

        // Verify delete files exist
        let delete_count = db.query_count(
            "SELECT COUNT(*) FROM ducklake_list_files('dl', 't1') WHERE delete_file IS NOT NULL",
        );
        assert!(delete_count > 0, "Should have delete files before rewrite");
    }

    // Rewrite via DataFusion with threshold 0 (rewrite any file with deletes)
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        df_query(
            &ctx,
            "SELECT * FROM ducklake_rewrite_data_files('t1', 0.0)",
        )
        .await;
    }

    // Verify: no more delete files, correct data
    {
        let db = DuckDbHelper::open(&env.catalog_path);
        let delete_count = db.query_count(
            "SELECT COUNT(*) FROM ducklake_list_files('dl', 't1') WHERE delete_file IS NOT NULL",
        );
        assert_eq!(
            delete_count, 0,
            "Should have no delete files after rewrite"
        );

        let row_count = db.query_count("SELECT COUNT(*) FROM dl.main.t1");
        assert_eq!(row_count, 3, "Should have 3 rows (1, 3, 5) after rewrite");
    }

    // DataFusion reads the rewritten data correctly
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let rows = df_query(
            &ctx,
            "SELECT id, name FROM ducklake.main.t1 ORDER BY id",
        )
        .await;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec!["1", "a"]);
        assert_eq!(rows[1], vec!["3", "c"]);
        assert_eq!(rows[2], vec!["5", "e"]);
    }
}

/// Test expire_snapshots: expire old snapshots, verify current data unaffected.
#[tokio::test(flavor = "multi_thread")]
async fn test_expire_snapshots() {
    let env = setup_env();

    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.t1 (id INTEGER, name VARCHAR)");
        db.execute("INSERT INTO dl.main.t1 VALUES (1, 'a'), (2, 'b')");
        db.execute("INSERT INTO dl.main.t1 VALUES (3, 'c')");
        db.execute("INSERT INTO dl.main.t1 VALUES (4, 'd')");
    }

    // Expire all snapshots older than now via DataFusion
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let expired_rows = df_query(
            &ctx,
            "SELECT * FROM ducklake_expire_snapshots('2099-01-01T00:00:00Z')",
        )
        .await;
        // Should have expired some snapshots (at least the initial create + inserts)
        assert!(
            !expired_rows.is_empty(),
            "Should have expired at least one snapshot"
        );
    }

    // Verify current data is still intact
    {
        let db = DuckDbHelper::open(&env.catalog_path);
        let row_count = db.query_count("SELECT COUNT(*) FROM dl.main.t1");
        assert_eq!(
            row_count, 4,
            "Current data should be unaffected by expire_snapshots"
        );
    }

    // DataFusion reads correctly after expire
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let rows = df_query(
            &ctx,
            "SELECT id FROM ducklake.main.t1 ORDER BY id",
        )
        .await;
        assert_eq!(rows.len(), 4);
    }
}

/// Test cleanup_old_files: expire snapshots then clean up orphaned files.
#[tokio::test(flavor = "multi_thread")]
async fn test_cleanup_old_files() {
    let env = setup_env();

    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.t1 (id INTEGER, name VARCHAR)");
        db.execute("INSERT INTO dl.main.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        db.execute("INSERT INTO dl.main.t1 VALUES (4, 'd'), (5, 'e')");
        db.execute("DELETE FROM dl.main.t1 WHERE id = 2");

        // Rewrite to create orphaned old files
        db.execute("SELECT * FROM ducklake_rewrite_data_files('dl', 't1', delete_threshold := 0.0)");
        // Expire all snapshots
        db.execute("SELECT * FROM ducklake_expire_snapshots('dl', older_than := '2099-01-01'::TIMESTAMPTZ)");
    }

    // Run cleanup via DataFusion
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let _cleaned = df_query(&ctx, "SELECT * FROM ducklake_cleanup_old_files()").await;
        // Note: cleanup may or may not find files to clean depending on implementation
    }

    // Verify data is still accessible
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let rows = df_query(
            &ctx,
            "SELECT id FROM ducklake.main.t1 ORDER BY id",
        )
        .await;
        assert_eq!(rows.len(), 4, "Data should be intact after cleanup");
    }
}

/// Test delete_orphaned_files with dry_run mode.
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_orphaned_files_dry_run() {
    let env = setup_env();

    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.t1 (id INTEGER)");
        db.execute("INSERT INTO dl.main.t1 VALUES (1), (2), (3)");
    }

    // Run orphan check with dry_run=true via DataFusion
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let result = df_query(
            &ctx,
            "SELECT * FROM ducklake_delete_orphaned_files(true)",
        )
        .await;
        // In a clean catalog, there should be no orphans
        assert!(
            result.is_empty(),
            "Clean catalog should have no orphaned files"
        );
    }

    // Verify data is still fine
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let count = df_row_count(
            &ctx,
            "SELECT * FROM ducklake.main.t1",
        )
        .await;
        assert_eq!(count, 3);
    }
}

/// Cross-engine test: DataFusion compaction produces catalog readable by DuckDB.
#[tokio::test(flavor = "multi_thread")]
async fn test_cross_engine_df_compaction_duckdb_read() {
    let env = setup_env();

    // DuckDB creates catalog with small files
    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.t1 (id INTEGER, name VARCHAR)");
        db.execute("INSERT INTO dl.main.t1 VALUES (1, 'alpha')");
        db.execute("INSERT INTO dl.main.t1 VALUES (2, 'beta')");
        db.execute("INSERT INTO dl.main.t1 VALUES (3, 'gamma')");
        db.execute("INSERT INTO dl.main.t1 VALUES (4, 'delta')");
        db.execute("INSERT INTO dl.main.t1 VALUES (5, 'epsilon')");
    }

    // DataFusion runs merge
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        df_query(&ctx, "SELECT * FROM ducklake_merge_adjacent_files()").await;
    }

    // DuckDB reads the merged catalog
    {
        let db = DuckDbHelper::open(&env.catalog_path);
        let row_count = db.query_count("SELECT COUNT(*) FROM dl.main.t1");
        assert_eq!(row_count, 5, "DuckDB should see all 5 rows after DF merge");

        let file_count = db.query_count(
            "SELECT COUNT(*) FROM ducklake_list_files('dl', 't1')",
        );
        assert!(file_count < 5, "DuckDB should see fewer files after merge");
    }
}

/// Cross-engine test: DuckDB compaction produces catalog readable by DataFusion.
#[tokio::test(flavor = "multi_thread")]
async fn test_cross_engine_duckdb_compaction_df_read() {
    let env = setup_env();

    // DuckDB creates catalog with deletes and runs rewrite
    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.t1 (id INTEGER, name VARCHAR)");
        db.execute("INSERT INTO dl.main.t1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
        db.execute("DELETE FROM dl.main.t1 WHERE id IN (2, 4)");
        db.execute("SELECT * FROM ducklake_rewrite_data_files('dl', 't1', delete_threshold := 0.0)");
    }

    // DataFusion reads the compacted catalog
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let rows = df_query(
            &ctx,
            "SELECT id, name FROM ducklake.main.t1 ORDER BY id",
        )
        .await;
        assert_eq!(rows.len(), 2, "DataFusion should see 2 rows after DuckDB rewrite");
        assert_eq!(rows[0], vec!["1", "a"]);
        assert_eq!(rows[1], vec!["3", "c"]);
    }
}

/// Cross-engine test: Full compaction lifecycle.
/// DuckDB creates data → DataFusion runs all compaction steps → both engines verify.
#[tokio::test(flavor = "multi_thread")]
async fn test_cross_engine_full_compaction_lifecycle() {
    let env = setup_env();

    // Step 1: DuckDB creates catalog with multiple tables and operations
    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.users (id INTEGER, name VARCHAR)");
        db.execute("INSERT INTO dl.main.users VALUES (1, 'Alice')");
        db.execute("INSERT INTO dl.main.users VALUES (2, 'Bob')");
        db.execute("INSERT INTO dl.main.users VALUES (3, 'Charlie')");
        db.execute("INSERT INTO dl.main.users VALUES (4, 'Diana')");
        db.execute("INSERT INTO dl.main.users VALUES (5, 'Eve')");
        db.execute("DELETE FROM dl.main.users WHERE id = 3");
    }

    // Step 2: DataFusion runs compaction
    {
        let ctx = open_df_with_compaction(&env.catalog_path);

        // Merge small files
        df_query(&ctx, "SELECT * FROM ducklake_merge_adjacent_files()").await;
    }

    // Need a fresh connection for rewrite (merge changed catalog state)
    {
        let ctx = open_df_with_compaction(&env.catalog_path);

        // Rewrite files with deletes
        df_query(
            &ctx,
            "SELECT * FROM ducklake_rewrite_data_files('users', 0.0)",
        )
        .await;
    }

    {
        let ctx = open_df_with_compaction(&env.catalog_path);

        // Expire old snapshots
        df_query(
            &ctx,
            "SELECT * FROM ducklake_expire_snapshots('2099-01-01T00:00:00Z')",
        )
        .await;
    }

    {
        let ctx = open_df_with_compaction(&env.catalog_path);

        // Cleanup old files
        df_query(&ctx, "SELECT * FROM ducklake_cleanup_old_files()").await;
    }

    // Step 3: Both engines verify final state
    let expected_data: Vec<Vec<&str>> = vec![
        vec!["1", "Alice"],
        vec!["2", "Bob"],
        vec!["4", "Diana"],
        vec!["5", "Eve"],
    ];

    // DuckDB verifies
    {
        let db = DuckDbHelper::open(&env.catalog_path);
        let row_count = db.query_count("SELECT COUNT(*) FROM dl.main.users");
        assert_eq!(
            row_count, 4,
            "DuckDB should see 4 rows after full compaction lifecycle"
        );

        // No delete files should remain
        let delete_count = db.query_count(
            "SELECT COUNT(*) FROM ducklake_list_files('dl', 'users') WHERE delete_file IS NOT NULL",
        );
        assert_eq!(
            delete_count, 0,
            "No delete files should remain after rewrite"
        );
    }

    // DataFusion verifies
    {
        let ctx = open_df_with_compaction(&env.catalog_path);
        let rows = df_query(
            &ctx,
            "SELECT id, name FROM ducklake.main.users ORDER BY id",
        )
        .await;
        assert_eq!(rows.len(), expected_data.len());
        for (i, expected) in expected_data.iter().enumerate() {
            assert_eq!(
                rows[i],
                expected.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "Row {} mismatch",
                i
            );
        }
    }
}

/// Test error handling: invalid arguments.
#[tokio::test(flavor = "multi_thread")]
async fn test_compaction_invalid_arguments() {
    let env = setup_env();

    {
        let db = DuckDbHelper::open_with_data_path(&env.catalog_path, &env.data_path);
        db.execute("CREATE TABLE dl.main.t1 (id INTEGER)");
        db.execute("INSERT INTO dl.main.t1 VALUES (1)");
    }

    let ctx = open_df_with_compaction(&env.catalog_path);

    // expire_snapshots with no args should fail
    let result = ctx
        .sql("SELECT * FROM ducklake_expire_snapshots()")
        .await;
    assert!(result.is_err(), "expire_snapshots() with no args should fail");

    // rewrite_data_files with no args should fail
    let result = ctx
        .sql("SELECT * FROM ducklake_rewrite_data_files()")
        .await;
    assert!(result.is_err(), "rewrite_data_files() with no args should fail");
}
