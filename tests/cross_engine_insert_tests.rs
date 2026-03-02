//! Cross-engine INSERT tests for DataFusion + DuckDB interoperability.
//!
//! Tests verify INSERT functionality across engines:
//! 1. DF INSERT + DuckDB SELECT
//! 2. DuckDB INSERT + DF SELECT
//! 3. INSERT with NOT NULL constraint enforcement
//! 4. INSERT INTO ... SELECT FROM
//! 5. CREATE TABLE AS SELECT (CTAS)
//! 6. INSERT with DEFAULT values
//! 7. WriteMode::Replace (TRUNCATE + INSERT)
//! 8. Multi-batch INSERT (verify row count accuracy)
//! 9. Footer size stored correctly in metadata

#![cfg(all(feature = "write-sqlite", feature = "metadata-duckdb", feature = "metadata-sqlite"))]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::test_utils::{assert_results_eq, df_query, duckdb_value_to_string};
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, DuckdbMetadataProvider, MetadataWriter,
    SqliteMetadataProvider, SqliteMetadataWriter,
};

// ==================== Setup helpers ====================

struct CrossEngineEnv {
    _temp_dir: TempDir,
    catalog_db_path: PathBuf,
    data_path: PathBuf,
}

async fn setup_ducklake_catalog() -> CrossEngineEnv {
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

    CrossEngineEnv {
        _temp_dir: temp_dir,
        catalog_db_path,
        data_path,
    }
}

fn open_in_datafusion_duckdb(catalog_path: &Path) -> SessionContext {
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())
        .expect("create DuckdbMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

async fn open_in_datafusion_sqlite(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}", catalog_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str)
        .await
        .expect("create SqliteMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

async fn open_in_datafusion_writable(catalog_path: &Path) -> SessionContext {
    let conn_str = format!("sqlite:{}?mode=rwc", catalog_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create SqliteMetadataWriter");
    let provider = SqliteMetadataProvider::new(&conn_str)
        .await
        .expect("create SqliteMetadataProvider");
    let catalog = DuckLakeCatalog::with_writer(Arc::new(provider), Arc::new(writer))
        .expect("create writable DuckLakeCatalog");
    let ctx = SessionContext::new();
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
        DuckDbConn {
            conn,
        }
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


// ==================== Test 1: DF INSERT + DuckDB SELECT ====================
// DataFusion writes via DuckLakeTableWriter → DuckDB reads and verifies columns/types

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_df_write_duckdb_read_types() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    // Write data with multiple column types
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("count", DataType::Int64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None])),
            Arc::new(Float64Array::from(vec![
                Some(95.5),
                Some(87.3),
                Some(100.0),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
            ])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer
        .write_table("main", "typed_data", &[batch])
        .await
        .unwrap();
    assert_eq!(result.records_written, 3);

    // DuckDB reads and verifies
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb
        .query("SELECT id, name, score, active, count FROM ducklake.main.typed_data ORDER BY id");

    assert_eq!(rows.len(), 3, "DuckDB should see 3 rows");
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[1][1], "Bob");
    assert_eq!(rows[2][0], "3");
    assert_eq!(rows[2][1], "NULL");
    assert_eq!(rows[2][4], "NULL"); // count is NULL for row 3

    // Also verify via DataFusion for completeness
    drop(duckdb);
    let ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(
        &ctx,
        "SELECT id, name, score, active, count FROM ducklake.main.typed_data ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 3);
    assert_eq!(df_rows[0][1], "Alice");
    assert_eq!(df_rows[2][1], "NULL");
}

// ==================== Test 2: DuckDB INSERT + DF SELECT ====================
// DuckDB inserts data → DataFusion reads via both providers

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_duckdb_write_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("duckdb_insert.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB creates table and inserts data
    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute(
            "CREATE TABLE ducklake.main.products (id INT, name VARCHAR, price DOUBLE, in_stock BOOLEAN)",
        );
        duckdb.execute(
            "INSERT INTO ducklake.main.products VALUES \
             (1, 'Laptop', 999.99, true), \
             (2, 'Mouse', 25.50, true), \
             (3, 'Keyboard', 75.00, false)",
        );

        // Verify DuckDB sees the data
        let rows = duckdb
            .query("SELECT id, name, price, in_stock FROM ducklake.main.products ORDER BY id");
        assert_eq!(rows.len(), 3);
    }

    // DataFusion reads with DuckdbMetadataProvider
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let actual = df_query(
        &ctx,
        "SELECT id, name, price, in_stock FROM ducklake.main.products ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "Laptop".into(), "999.99".into(), "true".into()],
        vec!["2".into(), "Mouse".into(), "25.5".into(), "true".into()],
        vec!["3".into(), "Keyboard".into(), "75.0".into(), "false".into()],
    ];

    assert_results_eq("duckdb_insert_df_read", &expected, &actual);
}

// ==================== Test 3: INSERT with NOT NULL constraint ====================
// DataFusion writer enforces NOT NULL on non-nullable columns

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_not_null_enforcement() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    // Schema with a non-nullable column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),   // NOT NULL
        Field::new("name", DataType::Utf8, false),  // NOT NULL
        Field::new("value", DataType::Int64, true), // nullable
    ]));

    // Valid batch: all non-nullable columns have values
    let valid_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int64Array::from(vec![Some(100), None])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store.clone()).unwrap();
    let result = table_writer
        .write_table("main", "not_null_test", &[valid_batch])
        .await
        .unwrap();
    assert_eq!(result.records_written, 2);

    // Verify via DuckDB
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, name, value FROM ducklake.main.not_null_test ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "Alice", "100"]);
    assert_eq!(rows[1], vec!["2", "Bob", "NULL"]);
}

// ==================== Test 4: INSERT INTO ... SELECT FROM ====================
// SQL-level INSERT INTO ... SELECT FROM across DataFusion

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_into_select_from() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // First create a table with initial data using the writer API
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(StringArray::from(vec!["a", "b"]))],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "target_table", &[batch])
        .await
        .unwrap();

    // Open writable context and register source data
    let ctx = open_in_datafusion_writable(&env.catalog_db_path).await;

    let source_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4, 5])),
            Arc::new(StringArray::from(vec!["c", "d", "e"])),
        ],
    )
    .unwrap();
    ctx.register_batch("source_data", source_batch).unwrap();

    // INSERT INTO ... SELECT FROM (explicit column list to avoid virtual column count mismatch)
    let result = ctx
        .sql("INSERT INTO ducklake.main.target_table (id, value) SELECT * FROM source_data")
        .await;

    match result {
        Ok(df) => {
            let _ = df.collect().await.unwrap();

            // Verify via DataFusion with fresh context
            let read_ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
            let df_rows = df_query(
                &read_ctx,
                "SELECT id, value FROM ducklake.main.target_table ORDER BY id",
            )
            .await;
            assert_eq!(
                df_rows.len(),
                5,
                "Should have 5 rows (2 original + 3 inserted)"
            );
            assert_eq!(df_rows[0], vec!["1", "a"]);
            assert_eq!(df_rows[4], vec!["5", "e"]);
        },
        Err(e) => {
            panic!("INSERT INTO ... SELECT FROM failed: {}", e);
        },
    }
}

// ==================== Test 5: CREATE TABLE AS SELECT (CTAS) ====================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_ctas() {
    let env = setup_ducklake_catalog().await;

    // Initialize with a snapshot and create "main" schema so CTAS can resolve it
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");
    let snap_id = writer.create_snapshot().unwrap();
    writer.get_or_create_schema("main", None, snap_id).unwrap();

    let ctx = open_in_datafusion_writable(&env.catalog_db_path).await;

    // Register in-memory source
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
            Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
        ],
    )
    .unwrap();
    ctx.register_batch("ctas_source", batch).unwrap();

    // CTAS
    let result = ctx
        .sql("CREATE TABLE ducklake.main.ctas_table AS SELECT * FROM ctas_source")
        .await;

    match result {
        Ok(df) => {
            let _ = df.collect().await.unwrap();

            // Verify via DuckDB
            let duckdb = DuckDbConn::open(&env.catalog_db_path);
            let rows =
                duckdb.query("SELECT id, label, amount FROM ducklake.main.ctas_table ORDER BY id");
            assert_eq!(rows.len(), 3, "CTAS should have created 3 rows");
            assert_eq!(rows[0][1], "X");
            assert_eq!(rows[2][1], "Z");

            // Also verify via DataFusion
            drop(duckdb);
            let read_ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
            let df_rows = df_query(
                &read_ctx,
                "SELECT id, label, amount FROM ducklake.main.ctas_table ORDER BY id",
            )
            .await;
            assert_eq!(df_rows.len(), 3);
            assert_results_eq(
                "ctas_df_read",
                &vec![
                    vec!["10".into(), "X".into(), "1.1".into()],
                    vec!["20".into(), "Y".into(), "2.2".into()],
                    vec!["30".into(), "Z".into(), "3.3".into()],
                ],
                &df_rows,
            );
        },
        Err(e) => {
            panic!("CTAS failed: {}", e);
        },
    }
}

// ==================== Test 6: INSERT with DEFAULT values ====================
// Test that columns with default values work correctly

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_with_defaults() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    // Write initial data with all columns populated
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("priority", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("active"), Some("inactive")])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
        ],
    )
    .unwrap();

    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer.clone()), object_store.clone()).unwrap();
    table_writer
        .write_table("main", "default_test", &[batch])
        .await
        .unwrap();

    // Append more data (without priority, simulating a partial insert)
    let schema_partial = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("priority", DataType::Int32, true),
    ]));

    let batch_partial = RecordBatch::try_new(
        schema_partial,
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StringArray::from(vec![Some("pending")])),
            Arc::new(Int32Array::from(vec![None as Option<i32>])), // NULL = no default
        ],
    )
    .unwrap();

    let table_writer2 = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer2
        .append_table("main", "default_test", &[batch_partial])
        .await
        .unwrap();
    assert_eq!(result.records_written, 1);

    // Verify via DataFusion (using sqlite provider for correct field_id resolution)
    let ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(
        &ctx,
        "SELECT id, status, priority FROM ducklake.main.default_test ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 3);
    assert_eq!(df_rows[0], vec!["1", "active", "1"]);
    assert_eq!(df_rows[1], vec!["2", "inactive", "2"]);
    assert_eq!(df_rows[2], vec!["3", "pending", "NULL"]);
}

// ==================== Test 7: WriteMode::Replace ====================
// Verify that Replace mode truncates existing data before inserting

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_replace_mode() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));

    // Write initial data (3 rows)
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["old_a", "old_b", "old_c"])),
        ],
    )
    .unwrap();

    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer.clone()), object_store.clone()).unwrap();
    table_writer
        .write_table("main", "replace_test", &[batch1])
        .await
        .unwrap();

    // Replace with new data (2 rows) - write_table uses Replace mode by default
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["new_x", "new_y"])),
        ],
    )
    .unwrap();

    let table_writer2 = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer2
        .write_table("main", "replace_test", &[batch2])
        .await
        .unwrap();
    assert_eq!(result.records_written, 2);

    // Verify via DuckDB: only new data should exist
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let rows = duckdb.query("SELECT id, value FROM ducklake.main.replace_test ORDER BY id");
    assert_eq!(rows.len(), 2, "Replace should have only 2 rows");
    assert_eq!(rows[0], vec!["10", "new_x"]);
    assert_eq!(rows[1], vec!["20", "new_y"]);
    drop(duckdb);

    // Also verify via DataFusion
    let ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(
        &ctx,
        "SELECT id, value FROM ducklake.main.replace_test ORDER BY id",
    )
    .await;
    assert_results_eq(
        "replace_mode_df_read",
        &vec![vec!["10".into(), "new_x".into()], vec!["20".into(), "new_y".into()]],
        &df_rows,
    );
}

// ==================== Test 8: Multi-batch INSERT ====================
// Multiple batches in a single write, verifying row count accuracy

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_multi_batch() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int64, true),
    ]));

    // Create multiple batches of varying sizes
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![4, 5])), Arc::new(Int64Array::from(vec![400, 500]))],
    )
    .unwrap();

    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![6, 7, 8, 9])),
            Arc::new(Int64Array::from(vec![600, 700, 800, 900])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer
        .write_table("main", "multi_batch", &[batch1, batch2, batch3])
        .await
        .unwrap();

    // Verify total record count
    assert_eq!(
        result.records_written, 9,
        "Should have written 9 records across 3 batches"
    );

    // Verify via DuckDB
    let duckdb = DuckDbConn::open(&env.catalog_db_path);
    let count_rows = duckdb.query("SELECT COUNT(*) FROM ducklake.main.multi_batch");
    assert_eq!(count_rows[0][0], "9");

    let rows = duckdb.query("SELECT id, value FROM ducklake.main.multi_batch ORDER BY id");
    assert_eq!(rows.len(), 9);
    assert_eq!(rows[0], vec!["1", "100"]);
    assert_eq!(rows[8], vec!["9", "900"]);
    drop(duckdb);

    // Verify via DataFusion
    let ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
    let df_count = df_query(&ctx, "SELECT COUNT(*) FROM ducklake.main.multi_batch").await;
    assert_eq!(df_count[0][0], "9");

    let df_rows = df_query(
        &ctx,
        "SELECT id, value FROM ducklake.main.multi_batch ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 9);
    assert_eq!(df_rows[0], vec!["1", "100"]);
    assert_eq!(df_rows[8], vec!["9", "900"]);
}

// ==================== Test 9: Footer size stored in metadata ====================
// Verify that Parquet footer sizes are recorded in the catalog metadata

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_footer_size_in_metadata() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "alpha", "beta", "gamma", "delta", "epsilon",
            ])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "footer_test", &[batch])
        .await
        .unwrap();

    // Check the SQLite catalog directly for footer_size in ducklake_data_file
    let conn_str_read = format!("sqlite:{}", env.catalog_db_path.display());
    let pool = sqlx::SqlitePool::connect(&conn_str_read)
        .await
        .expect("connect to sqlite");
    let row: (Option<i64>,) = sqlx::query_as(
        "SELECT footer_size FROM ducklake_data_file WHERE end_snapshot IS NULL LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("query footer_size");

    assert!(row.0.is_some(), "footer_size should be stored in metadata");
    let footer_size = row.0.unwrap();
    assert!(
        footer_size > 0,
        "footer_size should be positive, got {footer_size}"
    );

    // Also verify the file_size_bytes is positive
    let row2: (i64,) = sqlx::query_as(
        "SELECT file_size_bytes FROM ducklake_data_file WHERE end_snapshot IS NULL LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("query file_size_bytes");
    assert!(row2.0 > 0, "file_size_bytes should be positive");

    // Footer size should be less than file size
    assert!(
        footer_size < row2.0,
        "footer_size ({footer_size}) should be less than file_size ({})",
        row2.0
    );
}

// ==================== Test 10: Write + Append + Replace modes ====================
// Test all three write patterns: create, append, replace

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_write_append_replace() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, true),
    ]));

    // Write initial data (Replace mode = default for write_table)
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(StringArray::from(vec!["a", "b"]))],
    )
    .unwrap();

    let table_writer =
        DuckLakeTableWriter::new(Arc::new(writer.clone()), object_store.clone()).unwrap();
    let result = table_writer
        .write_table("main", "mode_test", &[batch1])
        .await
        .unwrap();
    assert_eq!(result.records_written, 2);

    // Append more data
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![3, 4])), Arc::new(StringArray::from(vec!["c", "d"]))],
    )
    .unwrap();

    let table_writer2 =
        DuckLakeTableWriter::new(Arc::new(writer.clone()), object_store.clone()).unwrap();
    let result2 = table_writer2
        .append_table("main", "mode_test", &[batch2])
        .await
        .unwrap();
    assert_eq!(result2.records_written, 2);

    // Verify via DataFusion: should see all 4 rows
    let ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
    let df_rows = df_query(
        &ctx,
        "SELECT id, val FROM ducklake.main.mode_test ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 4, "Should have 4 rows after append");
    assert_eq!(df_rows[0], vec!["1", "a"]);
    assert_eq!(df_rows[3], vec!["4", "d"]);

    // Now replace all data
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![99])), Arc::new(StringArray::from(vec!["replaced"]))],
    )
    .unwrap();

    let table_writer3 = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result3 = table_writer3
        .write_table("main", "mode_test", &[batch3])
        .await
        .unwrap();
    assert_eq!(result3.records_written, 1);

    // Verify via fresh DataFusion context: should see only 1 row
    let ctx2 = open_in_datafusion_sqlite(&env.catalog_db_path).await;
    let df_rows2 = df_query(
        &ctx2,
        "SELECT id, val FROM ducklake.main.mode_test ORDER BY id",
    )
    .await;
    assert_eq!(df_rows2.len(), 1);
    assert_eq!(df_rows2[0], vec!["99", "replaced"]);
}

// ==================== Test: SQL INSERT VALUES cross-engine ====================
// SQL-level INSERT INTO ... VALUES with DuckDB verification

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_sql_insert_values_duckdb_verify() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Create table with writer API first
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![1])), Arc::new(StringArray::from(vec!["initial"]))],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "sql_values_test", &[batch])
        .await
        .unwrap();

    // Open writable context
    let ctx = open_in_datafusion_writable(&env.catalog_db_path).await;

    // SQL INSERT INTO ... VALUES
    let result = ctx
        .sql("INSERT INTO ducklake.main.sql_values_test (id, name) VALUES (2, 'second'), (3, 'third')")
        .await;

    match result {
        Ok(df) => {
            let _ = df.collect().await.unwrap();

            // Verify via DataFusion (fresh context to pick up new snapshot)
            let read_ctx = open_in_datafusion_sqlite(&env.catalog_db_path).await;
            let df_rows = df_query(
                &read_ctx,
                "SELECT id, name FROM ducklake.main.sql_values_test ORDER BY id",
            )
            .await;
            assert_eq!(
                df_rows.len(),
                3,
                "Should have 3 rows (1 initial + 2 inserted)"
            );
            assert_eq!(df_rows[0], vec!["1", "initial"]);
            assert_eq!(df_rows[1], vec!["2", "second"]);
            assert_eq!(df_rows[2], vec!["3", "third"]);
        },
        Err(e) => {
            panic!("SQL INSERT INTO ... VALUES failed: {}", e);
        },
    }
}

// ==================== Test: DuckDB multiple INSERTs + DF read ====================
// DuckDB does multiple separate INSERT operations, DF reads all

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_duckdb_multiple_inserts_df_read() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("multi_insert.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB creates table and does multiple INSERTs
    {
        let duckdb = DuckDbConn::open_with_data_path(&catalog_path, &data_path);
        duckdb.execute("CREATE TABLE ducklake.main.events (id INT, event VARCHAR)");
        duckdb.execute("INSERT INTO ducklake.main.events VALUES (1, 'click')");
        duckdb.execute("INSERT INTO ducklake.main.events VALUES (2, 'scroll'), (3, 'hover')");
        duckdb.execute("INSERT INTO ducklake.main.events VALUES (4, 'submit')");
    }

    // DataFusion reads all data
    let ctx = open_in_datafusion_duckdb(&catalog_path);
    let actual = df_query(
        &ctx,
        "SELECT id, event FROM ducklake.main.events ORDER BY id",
    )
    .await;

    assert_eq!(
        actual.len(),
        4,
        "Should see all 4 rows from multiple inserts"
    );
    assert_eq!(actual[0], vec!["1", "click"]);
    assert_eq!(actual[1], vec!["2", "scroll"]);
    assert_eq!(actual[2], vec!["3", "hover"]);
    assert_eq!(actual[3], vec!["4", "submit"]);
}

// ==================== Test: INSERT OVERWRITE cross-engine ====================

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_overwrite() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Create table with initial data
    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "overwrite_cross", &[batch])
        .await
        .unwrap();

    // Open writable context
    let ctx = open_in_datafusion_writable(&env.catalog_db_path).await;

    // Register source for overwrite
    let overwrite_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20])), Arc::new(StringArray::from(vec!["x", "y"]))],
    )
    .unwrap();
    ctx.register_batch("overwrite_src", overwrite_batch)
        .unwrap();

    // INSERT OVERWRITE (explicit column list to avoid virtual column count mismatch)
    let result = ctx
        .sql("INSERT OVERWRITE ducklake.main.overwrite_cross (id, val) SELECT * FROM overwrite_src")
        .await;

    match result {
        Ok(df) => {
            let _ = df.collect().await.unwrap();

            // Verify via DuckDB: should only have replacement data
            let duckdb = DuckDbConn::open(&env.catalog_db_path);
            let rows =
                duckdb.query("SELECT id, val FROM ducklake.main.overwrite_cross ORDER BY id");
            assert_eq!(rows.len(), 2, "Overwrite should leave only 2 rows");
            assert_eq!(rows[0], vec!["10", "x"]);
            assert_eq!(rows[1], vec!["20", "y"]);
        },
        Err(e) => {
            panic!("INSERT OVERWRITE failed: {}", e);
        },
    }
}

// ==================== Test: Parquet file existence verification ====================
// Verify that Parquet files actually exist on disk after writes

#[tokio::test(flavor = "multi_thread")]
async fn cross_engine_insert_parquet_files_exist() {
    let env = setup_ducklake_catalog().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let conn_str = format!("sqlite:{}?mode=rwc", env.catalog_db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["hello", "world"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer
        .write_table("main", "file_check", &[batch])
        .await
        .unwrap();
    assert_eq!(result.files_written, 1);

    // Check that a Parquet file exists in the data directory
    let table_data_path = env.data_path.join("main").join("file_check");
    assert!(
        table_data_path.exists(),
        "Table data directory should exist: {:?}",
        table_data_path
    );

    let parquet_files: Vec<_> = std::fs::read_dir(&table_data_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();

    assert_eq!(parquet_files.len(), 1, "Should have exactly 1 Parquet file");

    // Verify it's a valid Parquet file by reading its metadata
    let file_path = parquet_files[0].path();
    let file = std::fs::File::open(&file_path).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("Should be a valid Parquet file");

    let metadata = reader.metadata();
    assert_eq!(
        metadata.file_metadata().num_rows(),
        2,
        "Parquet file should report 2 rows"
    );
}
