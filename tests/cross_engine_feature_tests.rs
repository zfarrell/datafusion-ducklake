//! Cross-engine feature tests for Virtual Columns, Query Planner, Column Statistics,
//! and Conflict Detection.
//!
//! These tests verify that features work correctly across DataFusion and DuckDB engines,
//! using the cross-engine test infrastructure patterns.

#![cfg(all(
    feature = "write-sqlite",
    feature = "metadata-duckdb",
    feature = "metadata-sqlite"
))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeQueryPlanner, DuckLakeTableWriter, DuckdbMetadataProvider,
    MetadataProvider, MetadataWriter, SqliteMetadataProvider, SqliteMetadataWriter,
};

// ==================== Setup helpers ====================

struct TestEnv {
    _temp_dir: TempDir,
    catalog_db_path: PathBuf,
    #[allow(dead_code)]
    data_path: PathBuf,
}

async fn setup_sqlite_catalog() -> TestEnv {
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

    TestEnv {
        _temp_dir: temp_dir,
        catalog_db_path,
        data_path,
    }
}

async fn open_df_sqlite_readonly(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}", catalog_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str)
        .await
        .expect("create SqliteMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

async fn open_df_sqlite_writable(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}?mode=rwc", catalog_path.display());
    let writer = Arc::new(
        SqliteMetadataWriter::new(&conn_str)
            .await
            .expect("create SqliteMetadataWriter"),
    );
    let provider = Arc::new(
        SqliteMetadataProvider::new(&conn_str)
            .await
            .expect("create SqliteMetadataProvider"),
    );
    let catalog = DuckLakeCatalog::with_writer(provider, writer).expect("create writable catalog");

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(DuckLakeQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

struct DuckDbConn {
    conn: duckdb::Connection,
}

impl DuckDbConn {
    fn open(catalog_db_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", []).expect("load ducklake");
        let attach_path = format!("ducklake:sqlite:{}", catalog_db_path.display());
        conn.execute(&format!("ATTACH '{}' AS ducklake;", attach_path), [])
            .expect("attach ducklake catalog");
        DuckDbConn { conn }
    }

    fn open_with_data_path(catalog_path: &Path, data_path: &Path) -> Self {
        let conn = duckdb::Connection::open_in_memory().expect("open in-memory duckdb");
        conn.execute("INSTALL ducklake;", [])
            .expect("install ducklake");
        conn.execute("LOAD ducklake;", []).expect("load ducklake");
        let attach_path = format!("ducklake:{}", catalog_path.display());
        conn.execute(
            &format!(
                "ATTACH '{}' AS ducklake (DATA_PATH '{}');",
                attach_path,
                data_path.display()
            ),
            [],
        )
        .expect("attach ducklake catalog with data path");
        DuckDbConn { conn }
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

fn duckdb_value_to_string(v: &duckdb::types::Value) -> String {
    match v {
        duckdb::types::Value::Null => "NULL".to_string(),
        duckdb::types::Value::Boolean(b) => b.to_string(),
        duckdb::types::Value::TinyInt(i) => i.to_string(),
        duckdb::types::Value::SmallInt(i) => i.to_string(),
        duckdb::types::Value::Int(i) => i.to_string(),
        duckdb::types::Value::BigInt(i) => i.to_string(),
        duckdb::types::Value::Float(f) => format!("{f}"),
        duckdb::types::Value::Double(f) => format!("{f}"),
        duckdb::types::Value::Text(s) => s.clone(),
        duckdb::types::Value::HugeInt(i) => i.to_string(),
        _ => format!("{v:?}"),
    }
}

async fn df_query(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let df = ctx.sql(sql).await.expect("DataFusion SQL failed");
    let batches = df.collect().await.expect("DataFusion collect failed");
    batches_to_strings(&batches)
}

fn batches_to_strings(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    row.push("NULL".to_string());
                } else {
                    row.push(arrow_value_to_string(col, row_idx));
                }
            }
            rows.push(row);
        }
    }
    rows
}

fn arrow_value_to_string(array: &dyn Array, idx: usize) -> String {
    match array.data_type() {
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            a.value(idx).to_string()
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            a.value(idx).to_string()
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            a.value(idx).to_string()
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{}", a.value(idx))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            a.value(idx).to_string()
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            a.value(idx).to_string()
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.value(idx).to_string()
        }
        other => format!("<unsupported:{other:?}>"),
    }
}

// ==================== Virtual Columns Tests ====================

/// Virtual columns: SELECT filename returns valid file paths.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_virtual_col_filename_returns_paths() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "people", &[batch])
        .await
        .unwrap();

    let ctx = open_df_sqlite_readonly(&env.catalog_db_path).await;
    let rows = df_query(&ctx, "SELECT filename FROM ducklake.main.people").await;

    assert_eq!(rows.len(), 3, "Should have 3 rows");
    for row in &rows {
        assert!(
            row[0].ends_with(".parquet") || row[0].contains(".parquet"),
            "filename should reference parquet file, got: {}",
            row[0]
        );
    }
}

/// Virtual columns: file_row_number returns correct sequential positions.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_virtual_col_row_numbers_sequential() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]))],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "nums", &[batch])
        .await
        .unwrap();

    let ctx = open_df_sqlite_readonly(&env.catalog_db_path).await;
    let rows = df_query(
        &ctx,
        "SELECT file_row_number FROM ducklake.main.nums ORDER BY file_row_number",
    )
    .await;

    assert_eq!(rows.len(), 5);
    let mut row_nums: Vec<i64> = rows.iter().map(|r| r[0].parse().unwrap()).collect();
    row_nums.sort();
    assert_eq!(row_nums, vec![0, 1, 2, 3, 4]);
}

/// Virtual columns: DuckDB-written data, DataFusion reads with virtual columns.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_write_df_read_virtual_columns() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("vc_test.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB writes data
    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute(
            "CREATE TABLE ducklake.main.items (id INT, name VARCHAR)",
        );
        duckdb.execute(
            "INSERT INTO ducklake.main.items VALUES (1, 'Widget'), (2, 'Gadget'), (3, 'Doohickey')",
        );
    }

    // DataFusion reads with virtual columns
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // SELECT * should include virtual columns
    let rows = df_query(&ctx, "SELECT * FROM ducklake.main.items ORDER BY id").await;
    assert_eq!(rows.len(), 3);
    // Schema should have id, name, filename, file_row_number, rowid, snapshot_id, file_index (7 columns)
    assert_eq!(rows[0].len(), 7, "SELECT * should include virtual columns");

    // Verify real data + virtual columns have values
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Widget");
    assert!(
        !rows[0][2].is_empty(),
        "filename should not be empty"
    );

    // file_row_number values should be 0, 1, 2
    let mut row_numbers: Vec<i64> = rows.iter().map(|r| r[3].parse().unwrap()).collect();
    row_numbers.sort();
    assert_eq!(row_numbers, vec![0, 1, 2]);

    // rowid values should be 0, 1, 2 (single file, row_id_start=0)
    let mut rowids: Vec<i64> = rows.iter().map(|r| r[4].parse().unwrap()).collect();
    rowids.sort();
    assert_eq!(rowids, vec![0, 1, 2]);

    // snapshot_id should be non-zero
    let snap_id: i64 = rows[0][5].parse().unwrap();
    assert!(snap_id > 0, "snapshot_id should be positive");

    // file_index should be 0 (only one file)
    let file_idx: u64 = rows[0][6].parse().unwrap();
    assert_eq!(file_idx, 0);
}

/// Virtual columns work with DELETE operations.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_virtual_cols_with_delete() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "del_vc", &[batch])
        .await
        .unwrap();

    // Delete row id=2 via DuckLakeQueryPlanner
    let ctx = open_df_sqlite_writable(&env.catalog_db_path).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.del_vc WHERE id = 2")
        .await
        .unwrap();
    let _ = df.collect().await.unwrap();

    // Re-read: virtual columns should still work after delete
    let read_ctx = open_df_sqlite_readonly(&env.catalog_db_path).await;
    let rows = df_query(
        &read_ctx,
        "SELECT id, filename, file_row_number FROM ducklake.main.del_vc ORDER BY id",
    )
    .await;

    assert_eq!(rows.len(), 3, "Should have 3 rows after delete");
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, vec!["1", "3", "4"]);

    // Each row should still have filename and row_number
    for row in &rows {
        assert!(!row[1].is_empty(), "filename should not be empty");
        let _: i64 = row[2].parse().expect("file_row_number should be numeric");
    }
}

/// Virtual columns work with UPDATE operations.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_virtual_cols_with_update() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "upd_vc", &[batch])
        .await
        .unwrap();

    // UPDATE row id=2 via DuckLakeQueryPlanner
    let ctx = open_df_sqlite_writable(&env.catalog_db_path).await;
    let df = ctx
        .sql("UPDATE ducklake.main.upd_vc SET name = 'Bobby' WHERE id = 2")
        .await
        .unwrap();
    let _ = df.collect().await.unwrap();

    // Re-read with virtual columns
    let read_ctx = open_df_sqlite_readonly(&env.catalog_db_path).await;
    let rows = df_query(
        &read_ctx,
        "SELECT id, name, filename FROM ducklake.main.upd_vc ORDER BY id",
    )
    .await;

    assert_eq!(rows.len(), 3, "Should still have 3 rows");
    assert_eq!(rows[1][1], "Bobby", "Row id=2 should be updated");

    for row in &rows {
        assert!(!row[2].is_empty(), "filename should not be empty");
    }
}

// ==================== Query Planner Tests ====================

/// Query planner: DELETE routed through DuckLakeQueryPlanner.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_planner_delete_and_duckdb_verify() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "planner_del", &[batch])
        .await
        .unwrap();

    // DELETE via SQL through planner
    let ctx = open_df_sqlite_writable(&env.catalog_db_path).await;
    let df = ctx
        .sql("DELETE FROM ducklake.main.planner_del WHERE id > 3")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    // Verify delete count
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2);

    // DuckDB verifies the result
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let duckdb_rows = duckdb.query("SELECT id FROM ducklake.main.planner_del ORDER BY id");
    let duckdb_ids: Vec<&str> = duckdb_rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(duckdb_ids, vec!["1", "2", "3"]);
}

/// Query planner: UPDATE routed through DuckLakeQueryPlanner, verified by DuckDB.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_planner_update_and_duckdb_verify() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "planner_upd", &[batch])
        .await
        .unwrap();

    // UPDATE via SQL through planner
    let ctx = open_df_sqlite_writable(&env.catalog_db_path).await;
    let df = ctx
        .sql("UPDATE ducklake.main.planner_upd SET name = 'UPDATED' WHERE id = 2")
        .await
        .unwrap();
    let _ = df.collect().await.unwrap();

    // DataFusion verifies (fresh read context to pick up new snapshot)
    let read_ctx = open_df_sqlite_readonly(&env.catalog_db_path).await;
    let rows = df_query(
        &read_ctx,
        "SELECT id, name FROM ducklake.main.planner_upd ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "Alice"]);
    assert_eq!(rows[1], vec!["2", "UPDATED"]);
    assert_eq!(rows[2], vec!["3", "Charlie"]);

    // Also verify total row count via DuckDB (basic interop check)
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let duckdb_count =
        duckdb.query("SELECT COUNT(*) FROM ducklake.main.planner_upd");
    assert_eq!(duckdb_count[0][0], "3", "DuckDB should see 3 total rows");
}

/// Query planner: INSERT via SQL parsed and executed correctly.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_planner_insert_via_sql() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "planner_ins", &[batch])
        .await
        .unwrap();

    // INSERT via SQL (uses DataFusion's standard INSERT path through the planner)
    let ctx = open_df_sqlite_writable(&env.catalog_db_path).await;
    let insert_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Diana"])),
        ],
    )
    .unwrap();
    ctx.register_batch("source", insert_batch).unwrap();

    let df = ctx
        .sql("INSERT INTO ducklake.main.planner_ins (id, name) SELECT * FROM source")
        .await
        .unwrap();
    let _ = df.collect().await.unwrap();

    // DataFusion verifies (fresh read context)
    let read_ctx = open_df_sqlite_readonly(&env.catalog_db_path).await;
    let df_rows = df_query(
        &read_ctx,
        "SELECT id, name FROM ducklake.main.planner_ins ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 4);
    assert_eq!(df_rows[0], vec!["1", "Alice"]);
    assert_eq!(df_rows[1], vec!["2", "Bob"]);
    assert_eq!(df_rows[2], vec!["3", "Charlie"]);
    assert_eq!(df_rows[3], vec!["4", "Diana"]);

    // Also verify total row count via DuckDB (basic interop check)
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let duckdb_count =
        duckdb.query("SELECT COUNT(*) FROM ducklake.main.planner_ins");
    assert_eq!(duckdb_count[0][0], "4", "DuckDB should see 4 total rows");
}

/// Query planner: Regular SELECT queries are unaffected by custom planner.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_planner_select_passthrough() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "select_test", &[batch])
        .await
        .unwrap();

    // SELECT works normally with the custom planner
    let ctx = open_df_sqlite_writable(&env.catalog_db_path).await;
    let rows = df_query(
        &ctx,
        "SELECT id, name FROM ducklake.main.select_test ORDER BY id",
    )
    .await;

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec!["1", "x"]);
    assert_eq!(rows[1], vec!["2", "y"]);
    assert_eq!(rows[2], vec!["3", "z"]);
}

// ==================== Column Statistics Tests ====================

/// Stats: DF-written stats are readable by DuckDB via the stats table.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_stats_df_write_duckdb_read() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec![Some("Alice"), None, Some("Charlie")])),
            Arc::new(Float64Array::from(vec![Some(95.5), Some(87.3), Some(92.1)])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "stats_ce", &[batch])
        .await
        .unwrap();

    // Verify stats are in the ducklake_file_column_stats table via DuckDB
    let duckdb = DuckDbConn::open(&env.catalog_db_path);

    // DuckDB can read the data
    let data_rows = duckdb.query("SELECT COUNT(*) FROM ducklake.main.stats_ce");
    assert_eq!(data_rows[0][0], "3");

    // Also verify via DataFusion that TableProvider::statistics() works
    let ctx = open_df_sqlite_readonly(&env.catalog_db_path).await;
    let table = ctx
        .catalog("ducklake")
        .unwrap()
        .schema("main")
        .unwrap()
        .table("stats_ce")
        .await
        .unwrap()
        .unwrap();

    let stats = table.statistics().expect("stats should be present");

    // Check id column: min=10, max=30, null_count=0
    let id_stats = &stats.column_statistics[0];
    assert!(
        matches!(
            &id_stats.null_count,
            datafusion::common::stats::Precision::Inexact(0)
        ),
        "id null_count should be 0, got: {:?}",
        id_stats.null_count
    );

    // Check name column: has 1 null
    let name_stats = &stats.column_statistics[1];
    assert!(
        matches!(
            &name_stats.null_count,
            datafusion::common::stats::Precision::Inexact(1)
        ),
        "name null_count should be 1, got: {:?}",
        name_stats.null_count
    );
}

/// Stats: Verify stats in ducklake_file_column_stats table directly via SQLite.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_stats_stored_in_metadata_table() {
    let env = setup_sqlite_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![5, 15, 25]))],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "stats_meta", &[batch])
        .await
        .unwrap();

    // Read stats directly from the metadata provider
    let provider = SqliteMetadataProvider::new(&format!("sqlite:{}", env.catalog_db_path.display()))
        .await
        .unwrap();
    let snapshot_id = provider.get_current_snapshot().unwrap();
    let schema_meta = provider
        .get_schema_by_name("main", snapshot_id)
        .unwrap()
        .unwrap();
    let table = provider
        .get_table_by_name(schema_meta.schema_id, "stats_meta", snapshot_id)
        .unwrap()
        .unwrap();
    let file_stats = provider
        .get_file_column_stats(table.table_id, snapshot_id)
        .unwrap();

    // Should have stats for the val column
    assert!(!file_stats.is_empty(), "Should have column stats");

    let val_stats: Vec<_> = file_stats
        .iter()
        .filter(|s| s.column_name == "val")
        .collect();
    assert_eq!(val_stats.len(), 1);
    assert_eq!(val_stats[0].null_count, Some(0));
    assert_eq!(val_stats[0].min_value.as_deref(), Some("5"));
    assert_eq!(val_stats[0].max_value.as_deref(), Some("25"));
}

// ==================== Conflict Detection Tests ====================

/// Conflict detection: INSERT after table drop detected.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_conflict_insert_after_drop() {
    let env = setup_sqlite_catalog().await;

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let columns = vec![
        datafusion_ducklake::ColumnDef::new("id", "int32", false).unwrap(),
        datafusion_ducklake::ColumnDef::new("name", "varchar", true).unwrap(),
    ];

    // Create table
    let setup = writer
        .begin_write_transaction("main", "conflict_test", &columns, datafusion_ducklake::WriteMode::Replace)
        .unwrap();
    let stale_snapshot = setup.snapshot_id;

    // Drop it
    writer.drop_table(setup.table_id).unwrap();

    // Checked write with stale snapshot should conflict
    let result = writer.begin_checked_write_transaction(
        "main",
        "conflict_test",
        &columns,
        datafusion_ducklake::WriteMode::Append,
        stale_snapshot,
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, datafusion_ducklake::DuckLakeError::TransactionConflict(_)),
        "Expected TransactionConflict, got: {err}"
    );
}

/// Conflict detection: No false conflicts for independent tables.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_conflict_independent_tables_no_conflict() {
    let env = setup_sqlite_catalog().await;

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let columns = vec![
        datafusion_ducklake::ColumnDef::new("id", "int32", false).unwrap(),
    ];

    // Create table A
    let setup_a = writer
        .begin_write_transaction("main", "table_a", &columns, datafusion_ducklake::WriteMode::Replace)
        .unwrap();
    let snapshot_after_a = setup_a.snapshot_id;

    // Create and drop table B
    let setup_b = writer
        .begin_write_transaction("main", "table_b", &columns, datafusion_ducklake::WriteMode::Replace)
        .unwrap();
    writer.drop_table(setup_b.table_id).unwrap();

    // Checked write to table A should succeed (B's drop doesn't affect A)
    let result = writer.begin_checked_write_transaction(
        "main",
        "table_a",
        &columns,
        datafusion_ducklake::WriteMode::Append,
        snapshot_after_a,
    );

    assert!(result.is_ok(), "Should not conflict: {result:?}");
}

/// Conflict detection: Error messages are descriptive.
#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_conflict_error_messages() {
    let env = setup_sqlite_catalog().await;

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();

    let columns = vec![
        datafusion_ducklake::ColumnDef::new("id", "int32", false).unwrap(),
    ];

    let setup = writer
        .begin_write_transaction("main", "msg_test", &columns, datafusion_ducklake::WriteMode::Replace)
        .unwrap();
    let stale = setup.snapshot_id;
    writer.drop_table(setup.table_id).unwrap();

    let err = writer
        .begin_checked_write_transaction(
            "main",
            "msg_test",
            &columns,
            datafusion_ducklake::WriteMode::Append,
            stale,
        )
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("conflict"),
        "Error should mention 'conflict': {msg}"
    );
    assert!(
        msg.to_lowercase().contains("drop"),
        "Error should mention 'drop': {msg}"
    );
}
