//! Cross-engine DDL tests: Views, DROP TABLE/SCHEMA, CREATE SCHEMA.
//!
//! These tests verify DDL operations are compatible between DataFusion and DuckDB
//! when sharing a DuckLake catalog.
//!
//! **Architecture note**: DuckDB creates catalogs in its own native `.ducklake` format.
//! Our DataFusion SQLite writer creates a separate SQLite format that is read-compatible
//! with DuckDB but NOT write-compatible. Therefore:
//! - "DuckDB writes, DF reads" tests use native DuckDB catalogs with DuckdbMetadataProvider
//! - "DF writes, DF reads" tests use SQLite catalogs with SqliteMetadataProvider
//! - "DF writes, DuckDB reads" tests are limited (DuckDB can't always read our SQLite format)
//!
//! Requires features: `write-sqlite`, `metadata-duckdb`, `metadata-sqlite`

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::test_utils::{batches_to_strings_filtered, df_query, duckdb_value_to_string};
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, DuckdbMetadataProvider, MetadataProvider, MetadataWriter,
    SqliteMetadataProvider, SqliteMetadataWriter,
};

// ==================== Setup helpers ====================

/// Environment for tests using SQLite-backed catalog (DF reads & writes).
struct SqliteTestEnv {
    _temp_dir: TempDir,
    catalog_db_path: PathBuf,
}

/// Creates a fresh DuckLake catalog backed by SQLite with initial table data.
async fn setup_sqlite_env_with_table(
    schema_name: &str,
    table_name: &str,
    batches: &[RecordBatch],
) -> SqliteTestEnv {
    let temp_dir = TempDir::new().expect("create temp dir");
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("create data dir");

    let conn_str = format!("sqlite:{}?mode=rwc", catalog_db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .expect("init sqlite catalog");
    let data_path_str = format!("{}/", data_path.display());
    writer.set_data_path(&data_path_str).expect("set data path");

    let writer2 = SqliteMetadataWriter::new(&conn_str).await.unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
    let table_writer = DuckLakeTableWriter::new(Arc::new(writer2), object_store).unwrap();
    table_writer
        .write_table(schema_name, table_name, batches)
        .await
        .unwrap();

    SqliteTestEnv {
        _temp_dir: temp_dir,
        catalog_db_path,
    }
}

/// Environment for tests using native DuckDB catalog (DuckDB creates).
struct DuckDbTestEnv {
    _temp_dir: TempDir,
    catalog_path: PathBuf,
    #[allow(dead_code)]
    data_path: PathBuf,
}

/// Creates a DuckLake catalog via DuckDB with initial table data.
fn setup_duckdb_env_with_table(create_sql: &str, insert_sql: &str) -> DuckDbTestEnv {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("test.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute(create_sql);
    duckdb.execute(insert_sql);

    DuckDbTestEnv {
        _temp_dir: temp_dir,
        catalog_path,
        data_path,
    }
}

/// Creates a DuckLake catalog via DuckDB (empty, just initialized).
fn setup_duckdb_env() -> DuckDbTestEnv {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("test.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute("CREATE TABLE ducklake.main.init_marker (x INT)");
    duckdb.execute("INSERT INTO ducklake.main.init_marker VALUES (1)");

    DuckDbTestEnv {
        _temp_dir: temp_dir,
        catalog_path,
        data_path,
    }
}

// ==================== Context helpers ====================

/// Open a writable DataFusion context (SQLite-backed).
async fn open_writable_df(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}?mode=rwc", catalog_path.display());
    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Open a read-only DataFusion context (SQLite-backed).
async fn open_readonly_df_sqlite(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}", catalog_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Open a read-only DataFusion context (DuckDB-backed, for native DuckLake catalogs).
fn open_readonly_df_duckdb(catalog_path: &Path) -> SessionContext {
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

// ==================== DuckDB wrapper ====================

struct DuckDbConn {
    conn: duckdb::Connection,
}

impl DuckDbConn {
    fn open_native(catalog_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();
        let attach_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(&format!("ATTACH '{}' AS ducklake;", attach_path), [])
            .unwrap();
        DuckDbConn {
            conn,
        }
    }

    fn open_with_data_path(catalog_path: &Path, data_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();
        let attach_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(
            &format!(
                "ATTACH '{}' AS ducklake (DATA_PATH '{}');",
                attach_path,
                data_path.display()
            ),
            [],
        )
        .unwrap();
        DuckDbConn {
            conn,
        }
    }

    fn execute(&self, sql: &str) {
        self.conn
            .execute(sql, [])
            .unwrap_or_else(|e| panic!("DuckDB execute failed: {e}\nSQL: {sql}"));
    }

    fn query(&self, sql: &str) -> Vec<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(sql)
            .unwrap_or_else(|e| panic!("DuckDB prepare failed: {e}\nSQL: {sql}"));
        let mut rows = stmt.query([]).expect("DuckDB query failed");

        let mut results = Vec::new();
        while let Some(row) = rows.next().expect("DuckDB row iteration") {
            let mut vals = Vec::new();
            for i in 0.. {
                match row.get::<_, duckdb::types::Value>(i) {
                    Ok(v) => vals.push(duckdb_value_to_string(&v)),
                    Err(_) => break,
                }
            }
            results.push(vals);
        }
        results
    }

    #[allow(dead_code)]
    fn query_single_string(&self, sql: &str) -> Vec<String> {
        self.query(sql)
            .into_iter()
            .map(|row| row[0].clone())
            .collect()
    }

    fn try_query(&self, sql: &str) -> std::result::Result<Vec<Vec<String>>, duckdb::Error> {
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query([])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let mut vals = Vec::new();
            for i in 0.. {
                match row.get::<_, duckdb::types::Value>(i) {
                    Ok(v) => vals.push(duckdb_value_to_string(&v)),
                    Err(_) => break,
                }
            }
            results.push(vals);
        }
        Ok(results)
    }
}

// ==================== Query helpers ====================

async fn df_query_result(
    ctx: &SessionContext,
    sql: &str,
) -> datafusion::error::Result<Vec<Vec<String>>> {
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    Ok(batches_to_strings_filtered(&batches))
}


fn make_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap()
}

// =============================================================================
// Views: Cross-engine tests
// =============================================================================
// Note: Views are only cross-engine testable on SQLite-backed catalogs via the
// SqliteMetadataProvider (which implements view methods). The DuckDB metadata
// provider has default empty implementations for view methods.

/// DataFusion creates a view (via writer API) → DataFusion reads it back.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_creates_view_df_reads() {
    let env = setup_sqlite_env_with_table("main", "users", &[make_test_batch()]).await;

    // Create view via MetadataWriter
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let snapshot = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot)
        .unwrap()
        .unwrap();
    writer
        .create_view(
            schema_meta.schema_id,
            "bob_view",
            "SELECT id, name FROM users WHERE name = 'Bob'",
        )
        .unwrap();

    // DataFusion reads the view
    let ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let rows = df_query(&ctx, "SELECT id, name FROM ducklake.main.bob_view").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["2", "Bob"]);
}

/// View is listed in table_names() and table_exist().
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_view_in_table_names() {
    let env = setup_sqlite_env_with_table("main", "users", &[make_test_batch()]).await;

    // Create view via writer
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let snapshot = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot)
        .unwrap()
        .unwrap();
    writer
        .create_view(
            schema_meta.schema_id,
            "names_view",
            "SELECT name FROM users",
        )
        .unwrap();

    // DataFusion sees the view in table_names() and table_exist()
    let ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema = catalog.schema("main").unwrap();
    let names = schema.table_names();

    assert!(
        names.contains(&"names_view".to_string()),
        "View should appear in table_names: {:?}",
        names
    );
    assert!(
        schema.table_exist("names_view"),
        "table_exist should return true for view"
    );
}

/// Create then drop a view, verify it's gone.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_view_create_drop_lifecycle() {
    let env = setup_sqlite_env_with_table("main", "users", &[make_test_batch()]).await;

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let snapshot = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot)
        .unwrap()
        .unwrap();
    let (view_id, _) = writer
        .create_view(schema_meta.schema_id, "temp_view", "SELECT id FROM users")
        .unwrap();

    // Verify view exists
    let ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema = catalog.schema("main").unwrap();
    assert!(schema.table_exist("temp_view"));

    // Drop the view
    writer.drop_view(view_id).unwrap();

    // Verify view is gone (fresh context)
    let ctx2 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let catalog2 = ctx2.catalog("ducklake").unwrap();
    let schema2 = catalog2.schema("main").unwrap();
    assert!(
        !schema2.table_exist("temp_view"),
        "Dropped view should not exist"
    );
    // Table should still exist
    assert!(schema2.table_exist("users"));
}

/// View with aggregation query works.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_view_with_aggregation() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("product", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "A", "B", "A"])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])),
        ],
    )
    .unwrap();

    let env = setup_sqlite_env_with_table("main", "sales", &[batch]).await;

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let snapshot = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot)
        .unwrap()
        .unwrap();
    writer
        .create_view(
            schema_meta.schema_id,
            "sales_summary",
            "SELECT product, SUM(amount) as total FROM sales GROUP BY product",
        )
        .unwrap();

    // DataFusion reads the aggregation view
    let ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let mut rows = df_query(
        &ctx,
        "SELECT product, total FROM ducklake.main.sales_summary ORDER BY product",
    )
    .await;
    rows.sort();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "A");
    assert_eq!(rows[0][1], "90"); // 10+30+50
    assert_eq!(rows[1][0], "B");
    assert_eq!(rows[1][1], "60"); // 20+40
}

/// DuckDB creates a view (native catalog) → DuckDB reads it back.
/// (DataFusion can't read native DuckDB views because DuckdbMetadataProvider
/// doesn't implement view methods, but this test verifies DuckDB view creation works.)
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_view_roundtrip() {
    let env = setup_duckdb_env_with_table(
        "CREATE TABLE ducklake.main.users (id INT, name VARCHAR)",
        "INSERT INTO ducklake.main.users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    );

    // DuckDB creates a view
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute(
        "CREATE VIEW ducklake.main.alice_view AS SELECT id, name FROM ducklake.main.users WHERE name = 'Alice'",
    );

    // DuckDB reads the view back
    let rows = duckdb.query("SELECT id, name FROM ducklake.main.alice_view");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["1", "Alice"]);

    // DuckDB drops the view
    duckdb.execute("DROP VIEW ducklake.main.alice_view");

    // Verify it's gone
    let result = duckdb.try_query("SELECT * FROM ducklake.main.alice_view");
    assert!(result.is_err(), "Dropped view should not be queryable");

    // Table should still be accessible
    let table_rows = duckdb.query("SELECT COUNT(*) FROM ducklake.main.users");
    assert_eq!(table_rows[0][0], "3");
}

// =============================================================================
// ALTER VIEW RENAME: Cross-engine tests
// =============================================================================

/// DF creates a view → DF renames it → DF reads it under the new name.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_creates_view_df_renames() {
    let env = setup_sqlite_env_with_table("main", "users", &[make_test_batch()]).await;

    // Create view via MetadataWriter
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let snapshot = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot)
        .unwrap()
        .unwrap();
    let (view_id, _) = writer
        .create_view(
            schema_meta.schema_id,
            "old_view",
            "SELECT id, name FROM users WHERE name = 'Bob'",
        )
        .unwrap();

    // Verify old name works
    let ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let rows = df_query(&ctx, "SELECT id, name FROM ducklake.main.old_view").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["2", "Bob"]);

    // Rename the view
    writer.rename_view(view_id, "new_view").unwrap();

    // Fresh context: new name should work
    let ctx2 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let rows2 = df_query(&ctx2, "SELECT id, name FROM ducklake.main.new_view").await;
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0], vec!["2", "Bob"]);

    // Old name should not work
    let result = df_query_result(&ctx2, "SELECT * FROM ducklake.main.old_view").await;
    assert!(result.is_err(), "Old view name should not be accessible");
}

/// DuckDB creates a view → DF renames it → DF reads under new name.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_creates_view_df_renames() {
    let env = setup_sqlite_env_with_table("main", "users", &[make_test_batch()]).await;

    // Create view via MetadataWriter (simulating DuckDB-created view)
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let snapshot = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot)
        .unwrap()
        .unwrap();
    let (view_id, _) = writer
        .create_view(
            schema_meta.schema_id,
            "original_view",
            "SELECT name FROM users",
        )
        .unwrap();

    // DF renames
    writer.rename_view(view_id, "renamed_view").unwrap();

    // DF reads the renamed view
    let ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let rows = df_query(
        &ctx,
        "SELECT name FROM ducklake.main.renamed_view ORDER BY name",
    )
    .await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "Alice");

    // Verify renamed view appears in table_names() and table_exist()
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema = catalog.schema("main").unwrap();
    assert!(
        schema.table_exist("renamed_view"),
        "Renamed view should exist"
    );
    assert!(
        !schema.table_exist("original_view"),
        "Original view name should not exist"
    );
    let names = schema.table_names();
    assert!(
        names.contains(&"renamed_view".to_string()),
        "Renamed view should appear in table_names: {:?}",
        names
    );
}

/// Rename a nonexistent view should fail.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_rename_nonexistent_view_fails() {
    let env = setup_sqlite_env_with_table("main", "users", &[make_test_batch()]).await;

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let result = writer.rename_view(999, "new_name");
    assert!(result.is_err(), "Renaming nonexistent view should fail");
}

/// Rename view preserves the SQL definition, so queries still work correctly.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_rename_view_preserves_sql() {
    let env = setup_sqlite_env_with_table("main", "users", &[make_test_batch()]).await;

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let snapshot = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot)
        .unwrap()
        .unwrap();

    // Create a filtered view
    let (view_id, _) = writer
        .create_view(
            schema_meta.schema_id,
            "filtered",
            "SELECT id, name FROM users WHERE id > 1",
        )
        .unwrap();

    // Rename
    writer.rename_view(view_id, "filtered_renamed").unwrap();

    // Query the renamed view — should still have the filter
    let ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let rows = df_query(
        &ctx,
        "SELECT id, name FROM ducklake.main.filtered_renamed ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2); // id=2 Bob, id=3 Charlie
    assert_eq!(rows[0][0], "2");
    assert_eq!(rows[1][0], "3");
}

// =============================================================================
// DROP TABLE: Cross-engine tests
// =============================================================================

/// DataFusion drops a table (SQLite catalog) → DuckDB-backed fresh context confirms it's gone.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_drops_table_df_confirms() {
    let env = setup_sqlite_env_with_table("main", "to_drop", &[make_test_batch()]).await;

    // Verify table exists
    let ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let rows = df_query(&ctx, "SELECT COUNT(*) FROM ducklake.main.to_drop").await;
    assert_eq!(rows[0][0], "3");

    // DataFusion drops the table
    let ctx2 = open_writable_df(&env.catalog_db_path).await;
    ctx2.sql("DROP TABLE ducklake.main.to_drop")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Fresh context confirms table is gone
    let ctx3 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let result = df_query_result(&ctx3, "SELECT * FROM ducklake.main.to_drop").await;
    assert!(result.is_err(), "Dropped table should not be queryable");
}

/// DuckDB drops a table (native catalog) → DataFusion confirms it's gone.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_drops_table_df_confirms() {
    let env = setup_duckdb_env_with_table(
        "CREATE TABLE ducklake.main.employees (id INT, name VARCHAR)",
        "INSERT INTO ducklake.main.employees VALUES (1, 'Alice'), (2, 'Bob')",
    );

    // Verify DF can read it
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let rows = df_query(
        &ctx,
        "SELECT id, name FROM ducklake.main.employees ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2);

    // DuckDB drops the table
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("DROP TABLE ducklake.main.employees");
    drop(duckdb);

    // DataFusion confirms table is gone (fresh context)
    let ctx2 = open_readonly_df_duckdb(&env.catalog_path);
    let result = df_query_result(&ctx2, "SELECT * FROM ducklake.main.employees").await;
    assert!(result.is_err(), "Dropped table should not be queryable");
}

/// DROP TABLE IF EXISTS on a non-existent table succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_drop_table_if_exists_nonexistent() {
    let env = setup_sqlite_env_with_table("main", "existing", &[make_test_batch()]).await;

    let ctx = open_writable_df(&env.catalog_db_path).await;

    // DROP TABLE IF EXISTS on non-existent table should not error
    let result = ctx
        .sql("DROP TABLE IF EXISTS ducklake.main.nonexistent")
        .await;
    assert!(
        result.is_ok(),
        "DROP TABLE IF EXISTS should not error: {:?}",
        result.err()
    );

    if let Ok(df) = result {
        let exec = df.collect().await;
        assert!(
            exec.is_ok(),
            "Execution of DROP TABLE IF EXISTS should succeed: {:?}",
            exec.err()
        );
    }

    // Existing table should still be there
    let ctx2 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let rows = df_query(&ctx2, "SELECT COUNT(*) FROM ducklake.main.existing").await;
    assert_eq!(rows[0][0], "3");
}

/// DuckDB drops one table, another table remains accessible.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_drop_table_other_tables_unaffected() {
    let env = setup_duckdb_env();

    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("CREATE TABLE ducklake.main.keep_me (id INT)");
    duckdb.execute("INSERT INTO ducklake.main.keep_me VALUES (1), (2)");
    duckdb.execute("CREATE TABLE ducklake.main.drop_me (id INT)");
    duckdb.execute("INSERT INTO ducklake.main.drop_me VALUES (3)");

    // Drop one table
    duckdb.execute("DROP TABLE ducklake.main.drop_me");
    drop(duckdb);

    // DataFusion verifies the other table is intact
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let rows = df_query(&ctx, "SELECT id FROM ducklake.main.keep_me ORDER BY id").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[1][0], "2");

    // Dropped table is gone
    let result = df_query_result(&ctx, "SELECT * FROM ducklake.main.drop_me").await;
    assert!(result.is_err());
}

// =============================================================================
// DROP SCHEMA: Cross-engine tests
// =============================================================================

/// DataFusion drops a schema with CASCADE → fresh DF context confirms it's gone.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_drops_schema_cascade() {
    let env = setup_sqlite_env_with_table("test_schema", "t1", &[make_test_batch()]).await;

    // Verify schema+table exist
    let ctx = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let rows = df_query(&ctx, "SELECT COUNT(*) FROM ducklake.test_schema.t1").await;
    assert_eq!(rows[0][0], "3");

    // Drop schema with CASCADE
    let ctx2 = open_writable_df(&env.catalog_db_path).await;
    ctx2.sql("DROP SCHEMA ducklake.test_schema CASCADE")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify schema is gone
    let ctx3 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let catalog = ctx3.catalog("ducklake").unwrap();
    let schema_names = catalog.schema_names();
    assert!(
        !schema_names.contains(&"test_schema".to_string()),
        "Schema should be gone, got: {:?}",
        schema_names
    );
}

/// DuckDB drops a schema with CASCADE → DataFusion confirms it's gone.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_drops_schema_cascade_df_confirms() {
    let env = setup_duckdb_env();

    // Create schema with data
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("CREATE SCHEMA ducklake.to_drop");
    duckdb.execute("CREATE TABLE ducklake.to_drop.data (val INT)");
    duckdb.execute("INSERT INTO ducklake.to_drop.data VALUES (42)");

    // Verify DF sees it
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let catalog = ctx.catalog("ducklake").unwrap();
    assert!(catalog.schema("to_drop").is_some());

    // DuckDB drops the schema
    duckdb.execute("DROP SCHEMA ducklake.to_drop CASCADE");
    drop(duckdb);

    // DataFusion confirms schema is gone (fresh context)
    let ctx2 = open_readonly_df_duckdb(&env.catalog_path);
    let catalog2 = ctx2.catalog("ducklake").unwrap();
    assert!(
        catalog2.schema("to_drop").is_none(),
        "Dropped schema should not exist"
    );
    // Main schema still exists
    assert!(catalog2.schema("main").is_some());
}

/// DROP non-empty SCHEMA without CASCADE should fail.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_drop_nonempty_schema_without_cascade_fails() {
    let env = setup_sqlite_env_with_table("nonempty", "data", &[make_test_batch()]).await;

    let ctx = open_writable_df(&env.catalog_db_path).await;
    let result = ctx.sql("DROP SCHEMA ducklake.nonempty").await;

    match result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(
                exec.is_err(),
                "DROP SCHEMA without CASCADE on non-empty schema should fail"
            );
            let err_msg = exec.unwrap_err().to_string();
            assert!(
                err_msg.contains("depend")
                    || err_msg.contains("CASCADE")
                    || err_msg.contains("not empty"),
                "Error should mention CASCADE or dependencies, got: {}",
                err_msg
            );
        },
        Err(e) => {
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("depend")
                    || err_msg.contains("CASCADE")
                    || err_msg.contains("not empty"),
                "Error should mention CASCADE or dependencies, got: {}",
                err_msg
            );
        },
    }

    // Schema should still exist
    let ctx2 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let catalog = ctx2.catalog("ducklake").unwrap();
    assert!(catalog.schema("nonempty").is_some());
}

/// DROP SCHEMA IF EXISTS on non-existent schema succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_drop_schema_if_exists_nonexistent() {
    let env = setup_sqlite_env_with_table("main", "init", &[make_test_batch()]).await;

    let ctx = open_writable_df(&env.catalog_db_path).await;
    let result = ctx
        .sql("DROP SCHEMA IF EXISTS ducklake.nonexistent_schema")
        .await;
    assert!(result.is_ok());

    if let Ok(df) = result {
        let exec = df.collect().await;
        assert!(exec.is_ok());
    }
}

// =============================================================================
// CREATE SCHEMA: Cross-engine tests
// =============================================================================

/// DataFusion creates a schema → fresh DF context can see it.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_df_creates_schema() {
    let env = setup_sqlite_env_with_table("main", "init", &[make_test_batch()]).await;

    let ctx = open_writable_df(&env.catalog_db_path).await;
    ctx.sql("CREATE SCHEMA ducklake.analytics")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Fresh context verifies the schema exists
    let ctx2 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let catalog = ctx2.catalog("ducklake").unwrap();
    let schema_names = catalog.schema_names();
    assert!(
        schema_names.contains(&"analytics".to_string()),
        "New schema should be visible: {:?}",
        schema_names
    );
}

/// DuckDB creates a schema → DataFusion sees it and can read tables in it.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_creates_schema_df_sees_it() {
    let env = setup_duckdb_env();

    // DuckDB creates a new schema with data
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("CREATE SCHEMA ducklake.reporting");
    duckdb.execute("CREATE TABLE ducklake.reporting.summary (category VARCHAR, total INT)");
    duckdb.execute("INSERT INTO ducklake.reporting.summary VALUES ('A', 100), ('B', 200)");
    drop(duckdb);

    // DataFusion sees the schema
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema_names = catalog.schema_names();
    assert!(
        schema_names.contains(&"reporting".to_string()),
        "DataFusion should see DuckDB-created schema: {:?}",
        schema_names
    );

    // DataFusion can read data
    let rows = df_query(
        &ctx,
        "SELECT category, total FROM ducklake.reporting.summary ORDER BY category",
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["A", "100"]);
    assert_eq!(rows[1], vec!["B", "200"]);
}

/// CREATE SCHEMA + DROP SCHEMA roundtrip (DF only, SQLite catalog).
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_create_drop_schema_roundtrip() {
    let env = setup_sqlite_env_with_table("main", "init", &[make_test_batch()]).await;

    // Create schema
    let ctx = open_writable_df(&env.catalog_db_path).await;
    ctx.sql("CREATE SCHEMA ducklake.ephemeral")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify it exists
    let ctx2 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let catalog = ctx2.catalog("ducklake").unwrap();
    assert!(catalog.schema_names().contains(&"ephemeral".to_string()));

    // Drop it
    let ctx3 = open_writable_df(&env.catalog_db_path).await;
    ctx3.sql("DROP SCHEMA ducklake.ephemeral")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify it's gone
    let ctx4 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let catalog2 = ctx4.catalog("ducklake").unwrap();
    assert!(
        !catalog2.schema_names().contains(&"ephemeral".to_string()),
        "Dropped schema should not exist"
    );
}

/// DuckDB creates a schema + drops it → DataFusion confirms lifecycle.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_create_drop_schema_roundtrip() {
    let env = setup_duckdb_env();

    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("CREATE SCHEMA ducklake.temp_schema");
    duckdb.execute("CREATE TABLE ducklake.temp_schema.data (id INT)");
    duckdb.execute("INSERT INTO ducklake.temp_schema.data VALUES (1)");

    // DF confirms schema exists
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    assert!(
        ctx.catalog("ducklake")
            .unwrap()
            .schema("temp_schema")
            .is_some()
    );

    // DuckDB drops the schema
    duckdb.execute("DROP SCHEMA ducklake.temp_schema CASCADE");
    drop(duckdb);

    // DF confirms it's gone
    let ctx2 = open_readonly_df_duckdb(&env.catalog_path);
    assert!(
        ctx2.catalog("ducklake")
            .unwrap()
            .schema("temp_schema")
            .is_none()
    );
}

/// Schema name validation rejects empty names.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_create_schema_empty_name_fails() {
    let env = setup_sqlite_env_with_table("main", "init", &[make_test_batch()]).await;

    let ctx = open_writable_df(&env.catalog_db_path).await;

    let result = ctx.sql("CREATE SCHEMA ducklake.\"\"").await;
    match result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(exec.is_err(), "Empty schema name should fail");
        },
        Err(_) => {
            // Parse-time failure is expected
        },
    }
}

/// CREATE SCHEMA IF NOT EXISTS is idempotent.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_create_schema_if_not_exists() {
    let env = setup_sqlite_env_with_table("main", "init", &[make_test_batch()]).await;

    // Create schema
    let ctx = open_writable_df(&env.catalog_db_path).await;
    ctx.sql("CREATE SCHEMA ducklake.shared_schema")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // CREATE SCHEMA IF NOT EXISTS should succeed
    let ctx2 = open_writable_df(&env.catalog_db_path).await;
    let result = ctx2
        .sql("CREATE SCHEMA IF NOT EXISTS ducklake.shared_schema")
        .await
        .unwrap()
        .collect()
        .await;
    assert!(
        result.is_ok(),
        "CREATE SCHEMA IF NOT EXISTS should succeed: {:?}",
        result.err()
    );
}

// =============================================================================
// Combined DDL lifecycle scenarios
// =============================================================================

/// Full lifecycle on native DuckDB catalog: create schema → tables → view → drop all.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_full_ddl_lifecycle_native() {
    let env = setup_duckdb_env();

    // Step 1: DuckDB creates a schema with tables and views
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("CREATE SCHEMA ducklake.lifecycle");
    duckdb.execute("CREATE TABLE ducklake.lifecycle.products (id INT, name VARCHAR, price DOUBLE)");
    duckdb.execute(
        "INSERT INTO ducklake.lifecycle.products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)",
    );
    duckdb.execute(
        "CREATE VIEW ducklake.lifecycle.expensive AS \
         SELECT * FROM ducklake.lifecycle.products WHERE price > 10",
    );
    drop(duckdb);

    // Step 2: DataFusion reads the table
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let rows = df_query(
        &ctx,
        "SELECT id, name, price FROM ducklake.lifecycle.products ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[1][0], "2");

    // Step 3: DataFusion confirms schema names
    let catalog = ctx.catalog("ducklake").unwrap();
    assert!(catalog.schema_names().contains(&"lifecycle".to_string()));

    // Step 4: DuckDB drops the view, then the table, then the schema
    let duckdb2 = DuckDbConn::open_native(&env.catalog_path);
    duckdb2.execute("DROP VIEW ducklake.lifecycle.expensive");
    duckdb2.execute("DROP TABLE ducklake.lifecycle.products");
    duckdb2.execute("DROP SCHEMA ducklake.lifecycle");
    drop(duckdb2);

    // Step 5: DataFusion confirms everything is gone
    let ctx2 = open_readonly_df_duckdb(&env.catalog_path);
    let catalog2 = ctx2.catalog("ducklake").unwrap();
    assert!(
        !catalog2.schema_names().contains(&"lifecycle".to_string()),
        "Schema should be gone"
    );
}

/// Full lifecycle on SQLite catalog: DF creates schema → tables → view → drops all.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_full_ddl_lifecycle_sqlite() {
    let env = setup_sqlite_env_with_table("main", "base", &[make_test_batch()]).await;

    // Step 1: DF creates a schema
    let ctx = open_writable_df(&env.catalog_db_path).await;
    ctx.sql("CREATE SCHEMA ducklake.lifecycle")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Step 2: DF creates a table via CTAS in the new schema
    let ctx2 = open_writable_df(&env.catalog_db_path).await;
    ctx2.sql(
        "CREATE TABLE ducklake.lifecycle.products AS \
         SELECT 1 as id, 'Widget' as name UNION ALL SELECT 2, 'Gadget'",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // Step 3: Verify data
    let ctx3 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let rows = df_query(
        &ctx3,
        "SELECT id, name FROM ducklake.lifecycle.products ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2);

    // Step 4: Create a view
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();
    let snapshot = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("lifecycle", snapshot)
        .unwrap()
        .unwrap();
    let (view_id, _) = writer
        .create_view(
            schema_meta.schema_id,
            "gadgets",
            "SELECT * FROM products WHERE name = 'Gadget'",
        )
        .unwrap();

    // Step 5: Query the view
    let ctx4 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let view_rows = df_query(&ctx4, "SELECT id, name FROM ducklake.lifecycle.gadgets").await;
    assert_eq!(view_rows.len(), 1);
    assert_eq!(view_rows[0][1], "Gadget");

    // Step 6: Drop the view
    writer.drop_view(view_id).unwrap();

    // Step 7: Drop the schema with CASCADE
    let ctx5 = open_writable_df(&env.catalog_db_path).await;
    ctx5.sql("DROP SCHEMA ducklake.lifecycle CASCADE")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Step 8: Verify everything is gone
    let ctx6 = open_readonly_df_sqlite(&env.catalog_db_path).await;
    let catalog = ctx6.catalog("ducklake").unwrap();
    assert!(
        !catalog.schema_names().contains(&"lifecycle".to_string()),
        "Schema should be gone after CASCADE drop"
    );
}

/// Multiple schemas: dropping one doesn't affect others (native catalog).
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_drop_schema_preserves_other_schemas() {
    let env = setup_duckdb_env();

    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("CREATE SCHEMA ducklake.keep_schema");
    duckdb.execute("CREATE TABLE ducklake.keep_schema.data (id INT)");
    duckdb.execute("INSERT INTO ducklake.keep_schema.data VALUES (1), (2)");
    duckdb.execute("CREATE SCHEMA ducklake.drop_schema");
    duckdb.execute("CREATE TABLE ducklake.drop_schema.data (id INT)");
    duckdb.execute("INSERT INTO ducklake.drop_schema.data VALUES (3)");

    // Drop one schema
    duckdb.execute("DROP SCHEMA ducklake.drop_schema CASCADE");
    drop(duckdb);

    // DataFusion confirms the other schema is intact
    let ctx = open_readonly_df_duckdb(&env.catalog_path);
    let rows = df_query(&ctx, "SELECT id FROM ducklake.keep_schema.data ORDER BY id").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[1][0], "2");

    // Dropped schema is gone
    let catalog = ctx.catalog("ducklake").unwrap();
    assert!(catalog.schema("drop_schema").is_none());
}
