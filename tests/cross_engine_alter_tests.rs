//! Cross-engine ALTER TABLE tests.
//!
//! These tests verify that ALTER TABLE operations produce metadata compatible
//! between DataFusion and DuckDB when sharing a DuckLake catalog.
//!
//! Test patterns:
//! - DataFusion alters → DuckDB reads (SQLite-backed catalog, DF writes only)
//! - DuckDB alters → DataFusion reads (DuckDB-native catalog, DuckDB writes)
//!
//! Requires features: `write-sqlite`, `metadata-duckdb`, `metadata-sqlite`

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

mod common;

use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::metadata_writer::AlterTableOp;
use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, DuckdbMetadataProvider, MetadataWriter,
    SqliteMetadataProvider, SqliteMetadataWriter,
};

// ==================== Setup helpers (SQLite-backed, DF writes) ====================

struct SqliteTestEnv {
    _temp_dir: TempDir,
    catalog_db_path: PathBuf,
}

/// Creates a fresh DuckLake catalog backed by SQLite with a table containing data.
async fn setup_sqlite_with_table() -> SqliteTestEnv {
    let temp_dir = TempDir::new().unwrap();
    let catalog_db_path = temp_dir.path().join("catalog.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", catalog_db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    let data_path_str = format!("{}/", data_path.display());
    writer.set_data_path(&data_path_str).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![30, 25, 35])),
        ],
    )
    .unwrap();

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "people", &[batch])
        .await
        .unwrap();

    SqliteTestEnv {
        _temp_dir: temp_dir,
        catalog_db_path,
    }
}

// ==================== Setup helpers (DuckDB-native, DuckDB writes) ====================

struct DuckDbTestEnv {
    _temp_dir: TempDir,
    catalog_path: PathBuf,
}

/// Creates a DuckLake catalog via DuckDB with initial data.
fn setup_duckdb_with_table() -> DuckDbTestEnv {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("test.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
    duckdb.execute(
        "CREATE TABLE ducklake.main.people (id INTEGER NOT NULL, name VARCHAR, age INTEGER)",
    );
    duckdb.execute(
        "INSERT INTO ducklake.main.people VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
    );

    DuckDbTestEnv {
        _temp_dir: temp_dir,
        catalog_path,
    }
}

// ==================== Context helpers ====================

async fn get_sqlite_writer(catalog_path: &Path) -> SqliteMetadataWriter {
    let conn_str = format!("sqlite:{}?mode=rwc", catalog_path.display());
    SqliteMetadataWriter::new(&conn_str).await.unwrap()
}

fn open_df_duckdb(catalog_path: &Path) -> SessionContext {
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

async fn open_df_sqlite(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}", catalog_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
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
    fn open(catalog_db_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();
        let attach_path = format!("ducklake:sqlite:{}", catalog_db_path.display());
        conn.execute(&format!("ATTACH '{}' AS ducklake;", attach_path), [])
            .unwrap();
        DuckDbConn {
            conn,
        }
    }

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
}

use common::test_utils::duckdb_value_to_string;

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
                    row.push(
                        arrow::util::display::ArrayFormatter::try_new(col, &Default::default())
                            .unwrap()
                            .value(row_idx)
                            .to_string(),
                    );
                }
            }
            rows.push(row);
        }
    }
    rows
}

/// Get the table_id for a table from the SQLite catalog.
async fn get_table_id(catalog_path: &Path, table_name: &str) -> i64 {
    let conn_str = format!("sqlite:{}", catalog_path.display());
    let pool = sqlx::sqlite::SqlitePool::connect(&conn_str).await.unwrap();
    let row = sqlx::query(
        "SELECT table_id FROM ducklake_table WHERE table_name = ? AND end_snapshot IS NULL",
    )
    .bind(table_name)
    .fetch_one(&pool)
    .await
    .unwrap();
    use sqlx::Row;
    row.try_get::<i64, _>(0).unwrap()
}

// ==================== RENAME TABLE tests ====================

/// DataFusion renames table → DuckDB sees new name (read-only)
#[tokio::test(flavor = "multi_thread")]
async fn test_df_rename_table_duckdb_reads() {
    let env = setup_sqlite_with_table().await;
    let writer = get_sqlite_writer(&env.catalog_db_path).await;
    let table_id = get_table_id(&env.catalog_db_path, "people").await;

    // DataFusion renames the table
    writer.rename_table(table_id, "persons").unwrap();

    // DuckDB should see the new name (read-only)
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT * FROM ducklake.main.persons ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");

    // Old name should not work
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        duckdb.query("SELECT * FROM ducklake.main.people");
    }));
    assert!(result.is_err(), "Old table name should not exist");
}

/// DuckDB renames table → DataFusion sees new name
#[tokio::test(flavor = "multi_thread")]
async fn test_duckdb_rename_table_df_reads() {
    let env = setup_duckdb_with_table();

    // DuckDB renames the table
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("ALTER TABLE ducklake.main.people RENAME TO persons");
    // Close DuckDB connection so DF can read
    drop(duckdb);

    // DataFusion should see the new name
    let ctx = open_df_duckdb(&env.catalog_path);
    let rows = df_query(&ctx, "SELECT * FROM ducklake.main.persons ORDER BY id").await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");
}

// ==================== SET DEFAULT / DROP DEFAULT tests ====================

/// DataFusion sets column default → verify metadata in catalog
#[tokio::test(flavor = "multi_thread")]
async fn test_df_set_column_default_metadata() {
    let env = setup_sqlite_with_table().await;
    let writer = get_sqlite_writer(&env.catalog_db_path).await;
    let table_id = get_table_id(&env.catalog_db_path, "people").await;

    // Set default on age column
    let op = AlterTableOp::SetColumnDefault {
        column_name: "age".into(),
        default_value: "0".into(),
    };
    writer.alter_table(table_id, &op).unwrap();

    // Verify via DuckDB read-only that the default is visible
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows =
        duckdb.query("SELECT column_default FROM duckdb_columns() WHERE table_name = 'people' AND column_name = 'age'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "0");
}

/// DuckDB sets column default → DataFusion can still read data
#[tokio::test(flavor = "multi_thread")]
async fn test_duckdb_set_column_default_df_reads() {
    let env = setup_duckdb_with_table();

    // DuckDB sets default and inserts a row
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("ALTER TABLE ducklake.main.people ALTER COLUMN age SET DEFAULT 99");
    duckdb.execute("INSERT INTO ducklake.main.people (id, name) VALUES (4, 'Dave')");
    drop(duckdb);

    // DataFusion should see the inserted row with default value
    let ctx = open_df_duckdb(&env.catalog_path);
    let rows = df_query(
        &ctx,
        "SELECT id, name, age FROM ducklake.main.people WHERE id = 4",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][2], "99");
}

/// DataFusion drops column default → verify metadata cleared
#[tokio::test(flavor = "multi_thread")]
async fn test_df_drop_column_default_metadata() {
    let env = setup_sqlite_with_table().await;
    let writer = get_sqlite_writer(&env.catalog_db_path).await;
    let table_id = get_table_id(&env.catalog_db_path, "people").await;

    // Set then drop default
    writer
        .alter_table(
            table_id,
            &AlterTableOp::SetColumnDefault {
                column_name: "age".into(),
                default_value: "0".into(),
            },
        )
        .unwrap();
    writer
        .alter_table(
            table_id,
            &AlterTableOp::DropColumnDefault {
                column_name: "age".into(),
            },
        )
        .unwrap();

    // DuckDB should see no default
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows =
        duckdb.query("SELECT column_default FROM duckdb_columns() WHERE table_name = 'people' AND column_name = 'age'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "NULL");
}

// ==================== SET NOT NULL / DROP NOT NULL tests ====================

/// DataFusion sets NOT NULL → DuckDB sees non-nullable schema
#[tokio::test(flavor = "multi_thread")]
async fn test_df_set_not_null_duckdb_sees_schema() {
    let env = setup_sqlite_with_table().await;
    let writer = get_sqlite_writer(&env.catalog_db_path).await;
    let table_id = get_table_id(&env.catalog_db_path, "people").await;

    // Set NOT NULL on name column
    let op = AlterTableOp::SetNotNull {
        column_name: "name".into(),
    };
    writer.alter_table(table_id, &op).unwrap();

    // DuckDB should see non-nullable schema
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query(
        "SELECT is_nullable FROM duckdb_columns() WHERE table_name = 'people' AND column_name = 'name'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "false");
}

/// DuckDB sets NOT NULL → DataFusion reads correct schema
#[tokio::test(flavor = "multi_thread")]
async fn test_duckdb_set_not_null_df_reads() {
    let env = setup_duckdb_with_table();

    // DuckDB sets NOT NULL on name
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("ALTER TABLE ducklake.main.people ALTER COLUMN name SET NOT NULL");
    drop(duckdb);

    // DataFusion should see the column as non-nullable and still read data
    let ctx = open_df_duckdb(&env.catalog_path);
    let rows = df_query(&ctx, "SELECT * FROM ducklake.main.people ORDER BY id").await;
    assert_eq!(rows.len(), 3);

    // Verify schema shows non-nullable
    let df = ctx
        .sql("SELECT * FROM ducklake.main.people LIMIT 0")
        .await
        .unwrap();
    let schema = df.schema();
    let name_field = schema.field_with_unqualified_name("name").unwrap();
    assert!(
        !name_field.is_nullable(),
        "name column should be non-nullable after SET NOT NULL"
    );
}

/// DataFusion drops NOT NULL → DuckDB sees nullable schema
#[tokio::test(flavor = "multi_thread")]
async fn test_df_drop_not_null_duckdb_sees_schema() {
    let env = setup_sqlite_with_table().await;
    let writer = get_sqlite_writer(&env.catalog_db_path).await;
    let table_id = get_table_id(&env.catalog_db_path, "people").await;

    // First set NOT NULL, then drop it
    writer
        .alter_table(
            table_id,
            &AlterTableOp::SetNotNull {
                column_name: "name".into(),
            },
        )
        .unwrap();
    writer
        .alter_table(
            table_id,
            &AlterTableOp::DropNotNull {
                column_name: "name".into(),
            },
        )
        .unwrap();

    // DuckDB should see nullable again
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query(
        "SELECT is_nullable FROM duckdb_columns() WHERE table_name = 'people' AND column_name = 'name'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "true");
}

/// DuckDB drops NOT NULL → DataFusion reads nullable column
#[tokio::test(flavor = "multi_thread")]
async fn test_duckdb_drop_not_null_df_reads() {
    let env = setup_duckdb_with_table();

    // DuckDB sets then drops NOT NULL, inserts NULL
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("ALTER TABLE ducklake.main.people ALTER COLUMN name SET NOT NULL");
    duckdb.execute("ALTER TABLE ducklake.main.people ALTER COLUMN name DROP NOT NULL");
    duckdb.execute("INSERT INTO ducklake.main.people (id, name, age) VALUES (4, NULL, 20)");
    drop(duckdb);

    // DataFusion should see the NULL row
    let ctx = open_df_duckdb(&env.catalog_path);
    let rows = df_query(&ctx, "SELECT name FROM ducklake.main.people WHERE id = 4").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "NULL");
}

// ==================== COMMENT tests ====================

/// DataFusion sets table comment → DuckDB sees it (read-only)
#[tokio::test(flavor = "multi_thread")]
async fn test_df_set_table_comment_duckdb_reads() {
    let env = setup_sqlite_with_table().await;
    let writer = get_sqlite_writer(&env.catalog_db_path).await;
    let table_id = get_table_id(&env.catalog_db_path, "people").await;

    writer
        .set_table_comment(table_id, "A table of people")
        .unwrap();

    // DuckDB should see the comment
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT comment FROM duckdb_tables() WHERE table_name = 'people'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "A table of people");
}

/// DuckDB sets table comment → verify in catalog
#[tokio::test(flavor = "multi_thread")]
async fn test_duckdb_set_table_comment_df_reads() {
    let env = setup_duckdb_with_table();

    // DuckDB sets a comment
    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("COMMENT ON TABLE ducklake.main.people IS 'People directory'");

    // Re-read from DuckDB to confirm
    let rows = duckdb.query("SELECT comment FROM duckdb_tables() WHERE table_name = 'people'");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "People directory");
}

/// DataFusion sets column comment → DuckDB sees it (read-only)
#[tokio::test(flavor = "multi_thread")]
async fn test_df_set_column_comment_duckdb_reads() {
    let env = setup_sqlite_with_table().await;
    let writer = get_sqlite_writer(&env.catalog_db_path).await;
    let table_id = get_table_id(&env.catalog_db_path, "people").await;

    writer
        .set_column_comment(table_id, "name", "Full name of the person")
        .unwrap();

    // DuckDB should see the column comment
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query(
        "SELECT comment FROM duckdb_columns() WHERE table_name = 'people' AND column_name = 'name'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "Full name of the person");
}

/// DuckDB sets column comment → verify in catalog
#[tokio::test(flavor = "multi_thread")]
async fn test_duckdb_set_column_comment_verify() {
    let env = setup_duckdb_with_table();

    let duckdb = DuckDbConn::open_native(&env.catalog_path);
    duckdb.execute("COMMENT ON COLUMN ducklake.main.people.name IS 'Person name'");
    duckdb.execute("COMMENT ON COLUMN ducklake.main.people.age IS 'Age in years'");

    // Verify via DuckDB
    let rows = duckdb.query(
        "SELECT column_name, comment FROM duckdb_columns() WHERE table_name = 'people' AND comment IS NOT NULL ORDER BY column_name",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "age");
    assert_eq!(rows[0][1], "Age in years");
    assert_eq!(rows[1][0], "name");
    assert_eq!(rows[1][1], "Person name");
}

// ==================== Combined operation tests ====================

/// Verify data is still readable after multiple ALTER TABLE operations
#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_alter_operations_data_intact() {
    let env = setup_sqlite_with_table().await;
    let writer = get_sqlite_writer(&env.catalog_db_path).await;
    let table_id = get_table_id(&env.catalog_db_path, "people").await;

    // Apply multiple operations
    writer
        .alter_table(
            table_id,
            &AlterTableOp::SetColumnDefault {
                column_name: "age".into(),
                default_value: "0".into(),
            },
        )
        .unwrap();
    writer
        .alter_table(
            table_id,
            &AlterTableOp::SetNotNull {
                column_name: "name".into(),
            },
        )
        .unwrap();
    writer.set_table_comment(table_id, "People table").unwrap();
    writer
        .set_column_comment(table_id, "id", "Primary key")
        .unwrap();
    writer.rename_table(table_id, "contacts").unwrap();

    // Data should still be readable via DF
    let ctx = open_df_sqlite(&env.catalog_db_path).await;
    let rows = df_query(&ctx, "SELECT * FROM ducklake.main.contacts ORDER BY id").await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");
    assert_eq!(rows[0][2], "30");

    // Data should still be readable via DuckDB (read-only)
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let duckdb_rows = duckdb.query("SELECT * FROM ducklake.main.contacts ORDER BY id");
    assert_eq!(duckdb_rows.len(), 3);
    assert_eq!(duckdb_rows[0][0], "1");

    // Verify DuckDB sees comment
    let comment_rows =
        duckdb.query("SELECT comment FROM duckdb_tables() WHERE table_name = 'contacts'");
    assert_eq!(comment_rows.len(), 1);
    assert_eq!(comment_rows[0][0], "People table");
}
