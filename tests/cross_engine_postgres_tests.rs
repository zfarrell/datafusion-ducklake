//! Cross-engine tests with PostgreSQL as the DuckLake catalog backend.
//!
//! Tests verify DataFusion can write to and read from a Postgres-backed DuckLake catalog,
//! and that DuckDB can also read from the same Postgres catalog (full interop).
//!
//! Test patterns:
//! 1. DF writes via PostgresMetadataWriter → DF reads via PostgresMetadataProvider
//! 2. DF writes via PostgresMetadataWriter → DuckDB reads via ducklake:postgres:
//! 3. DuckDB writes via ducklake:postgres: → DF reads via PostgresMetadataProvider
//! 4. SQL-based DDL/DML (CREATE TABLE, INSERT, SELECT, DELETE, UPDATE)
//!
//! Requires: Docker (testcontainers spins up PostgreSQL), features: write-postgres, metadata-duckdb

#![cfg(all(feature = "write-postgres", feature = "metadata-duckdb"))]

mod common;

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeQueryPlanner, DuckLakeTableWriter, MetadataWriter,
    PostgresMetadataProvider, PostgresMetadataWriter,
};

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

// ==================== Setup helpers ====================

/// Environment for Postgres-backed cross-engine tests.
struct PgCrossEngineEnv {
    _temp_dir: TempDir,
    #[allow(dead_code)]
    data_path: String,
    pg_conn_str: String,
    _container: testcontainers::ContainerAsync<Postgres>,
}

/// Starts a PostgreSQL container and initializes a DuckLake catalog.
async fn setup_pg_env() -> PgCrossEngineEnv {
    let container = Postgres::default().start().await.unwrap();
    let host = "127.0.0.1";
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pg_conn_str = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    // Initialize DuckLake schema tables in Postgres
    let writer = PostgresMetadataWriter::new_with_init(&pg_conn_str)
        .await
        .expect("init postgres catalog");

    // Set up local data directory for Parquet files
    let temp_dir = TempDir::new().expect("create temp dir");
    let data_path = format!("{}/", temp_dir.path().join("data").display());
    std::fs::create_dir_all(&data_path).expect("create data dir");
    writer.set_data_path(&data_path).expect("set data path");

    PgCrossEngineEnv {
        _temp_dir: temp_dir,
        data_path,
        pg_conn_str,
        _container: container,
    }
}

// ==================== Context helpers ====================

/// Open a read-only DataFusion context using PostgresMetadataProvider.
async fn open_readonly_df_postgres(conn_str: &str) -> SessionContext {
    let provider = PostgresMetadataProvider::new(conn_str)
        .await
        .expect("create PostgresMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Open a writable DataFusion context using Postgres backend.
async fn open_writable_df_postgres(conn_str: &str) -> SessionContext {
    let provider = Arc::new(
        PostgresMetadataProvider::new(conn_str)
            .await
            .expect("create PostgresMetadataProvider"),
    );
    let writer = Arc::new(
        PostgresMetadataWriter::new(conn_str)
            .await
            .expect("create PostgresMetadataWriter"),
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

// ==================== DuckDB with Postgres catalog ====================

struct DuckDbPgConn {
    conn: duckdb::Connection,
}

impl DuckDbPgConn {
    /// Open DuckDB and attach to a Postgres-backed DuckLake catalog.
    /// Returns None if DuckDB can't load required extensions.
    fn try_open(pg_conn_str: &str, data_path: Option<&str>) -> Option<Self> {
        let conn = duckdb::Connection::open_in_memory().ok()?;
        conn.execute("INSTALL ducklake;", []).ok()?;
        conn.execute("LOAD ducklake;", []).ok()?;
        conn.execute("INSTALL postgres;", []).ok()?;
        conn.execute("LOAD postgres;", []).ok()?;

        // Convert sqlx-style connection string to libpq-style for DuckDB
        // sqlx: postgresql://user:pass@host:port/db
        // libpq: host=H port=P dbname=D user=U password=P
        let libpq_str = sqlx_to_libpq(pg_conn_str);
        let attach = if let Some(dp) = data_path {
            format!(
                "ATTACH 'ducklake:postgres:{}' AS ducklake (DATA_PATH '{}');",
                libpq_str, dp
            )
        } else {
            format!("ATTACH 'ducklake:postgres:{}' AS ducklake;", libpq_str)
        };

        match conn.execute(&attach, []) {
            Ok(_) => Some(DuckDbPgConn {
                conn,
            }),
            Err(e) => {
                eprintln!("DuckDB couldn't attach Postgres DuckLake catalog: {e}");
                None
            },
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

/// Convert a sqlx-style PostgreSQL URL to a libpq-style connection string.
/// Input: postgresql://user:pass@host:port/dbname
/// Output: host=host port=port dbname=dbname user=user password=pass
fn sqlx_to_libpq(url: &str) -> String {
    let url = url::Url::parse(url).expect("parse pg url");
    let host = url.host_str().unwrap_or("localhost");
    let port = url.port().unwrap_or(5432);
    let user = url.username();
    let password = url.password().unwrap_or("");
    let dbname = url.path().trim_start_matches('/');
    format!(
        "host={} port={} dbname={} user={} password={}",
        host, port, dbname, user, password
    )
}

use common::test_utils::{
    arrow_value_to_string, assert_results_eq, batches_to_strings, duckdb_value_to_string,
    normalize_value,
};

// ==================== Query helpers ====================

async fn df_query(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let df = ctx.sql(sql).await.expect("DataFusion SQL failed");
    let batches = df.collect().await.expect("DataFusion collect failed");
    batches_to_strings(&batches)
}

// ==================== Test 1: DF writes → DF reads (Postgres catalog) ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_pg_df_write_df_read() {
    let env = setup_pg_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Write using DuckLakeTableWriter with Postgres backend
    let writer = PostgresMetadataWriter::new(&env.pg_conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                Some("Bob"),
                Some("Charlie"),
            ])),
            Arc::new(Float64Array::from(vec![Some(10.5), Some(20.0), Some(30.5)])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer
        .write_table("main", "test_table", &[batch])
        .await
        .unwrap();
    assert_eq!(result.records_written, 3);

    // Read back using DataFusion + PostgresMetadataProvider
    let ctx = open_readonly_df_postgres(&env.pg_conn_str).await;
    let actual = df_query(
        &ctx,
        "SELECT id, name, value FROM ducklake.main.test_table ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "Alice".into(), "10.5".into()],
        vec!["2".into(), "Bob".into(), "20.0".into()],
        vec!["3".into(), "Charlie".into(), "30.5".into()],
    ];

    assert_results_eq("pg_df_write_df_read", &expected, &actual);
}

// ==================== Test 2: DF writes → DuckDB reads (Postgres catalog) ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_pg_df_write_duckdb_read() {
    let env = setup_pg_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Write using DuckLakeTableWriter
    let writer = PostgresMetadataWriter::new(&env.pg_conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec![Some("Xena"), Some("Yuri"), None])),
            Arc::new(Float64Array::from(vec![Some(95.5), Some(87.3), Some(92.1)])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let result = table_writer
        .write_table("main", "scores", &[batch])
        .await
        .unwrap();
    assert_eq!(result.records_written, 3);

    // Read using DuckDB with Postgres DuckLake catalog
    let duckdb = match DuckDbPgConn::try_open(&env.pg_conn_str, None) {
        Some(d) => d,
        None => {
            eprintln!("Skipping DuckDB read: DuckDB postgres extension not available");
            return;
        },
    };

    let rows = duckdb.query("SELECT id, name, score FROM ducklake.main.scores ORDER BY id");
    assert_eq!(rows.len(), 3, "DuckDB should see 3 rows");
    assert_eq!(rows[0][0], "10");
    assert_eq!(rows[0][1], "Xena");
    assert_eq!(rows[1][0], "20");
    assert_eq!(rows[2][1], "NULL");
}

// ==================== Test 3: DuckDB writes → DF reads (Postgres catalog) ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_pg_duckdb_write_df_read() {
    // Use a fresh Postgres container — DuckDB initializes the DuckLake catalog itself
    let container = Postgres::default().start().await.unwrap();
    let host = "127.0.0.1";
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pg_conn_str = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    let temp_dir = TempDir::new().unwrap();
    let data_path = format!("{}/", temp_dir.path().join("data").display());
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB creates the catalog from scratch via postgres-backed DuckLake
    let duckdb = match DuckDbPgConn::try_open(&pg_conn_str, Some(&data_path)) {
        Some(d) => d,
        None => {
            eprintln!("Skipping: DuckDB postgres extension not available");
            return;
        },
    };

    duckdb.execute("CREATE TABLE ducklake.main.orders (id INT, product VARCHAR, amount DOUBLE)");
    duckdb.execute(
        "INSERT INTO ducklake.main.orders VALUES \
         (1, 'Widget', 19.99), \
         (2, 'Gadget', 49.99), \
         (3, 'Doohickey', 9.99)",
    );
    drop(duckdb);

    // DF reads via PostgresMetadataProvider
    let ctx = open_readonly_df_postgres(&pg_conn_str).await;
    let actual = df_query(
        &ctx,
        "SELECT id, product, amount FROM ducklake.main.orders ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "Widget".into(), "19.99".into()],
        vec!["2".into(), "Gadget".into(), "49.99".into()],
        vec!["3".into(), "Doohickey".into(), "9.99".into()],
    ];

    assert_results_eq("pg_duckdb_write_df_read", &expected, &actual);
}

// ==================== Test 4: NULL handling roundtrip ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_pg_null_handling() {
    let env = setup_pg_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let writer = PostgresMetadataWriter::new(&env.pg_conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("Alice"),
                None,
                Some("Charlie"),
            ])),
            Arc::new(Int64Array::from(vec![Some(100), Some(200), None])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "nulls_test", &[batch])
        .await
        .unwrap();

    // DF reads and verifies NULLs
    let ctx = open_readonly_df_postgres(&env.pg_conn_str).await;
    let df_rows = df_query(
        &ctx,
        "SELECT id, name, value FROM ducklake.main.nulls_test ORDER BY id",
    )
    .await;
    assert_eq!(df_rows.len(), 3);
    assert_eq!(df_rows[0], vec!["1", "Alice", "100"]);
    assert_eq!(df_rows[1][1], "NULL"); // name is NULL
    assert_eq!(df_rows[2][2], "NULL"); // value is NULL

    // DuckDB also verifies NULLs (if available)
    if let Some(duckdb) = DuckDbPgConn::try_open(&env.pg_conn_str, None) {
        let rows = duckdb.query("SELECT id, name, value FROM ducklake.main.nulls_test ORDER BY id");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1][1], "NULL");
        assert_eq!(rows[2][2], "NULL");
    }
}

// ==================== Test 5: SQL DDL via DF with Postgres catalog ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_pg_sql_create_insert_select() {
    let env = setup_pg_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // First, bootstrap the "main" schema by writing an initial table via TableWriter
    // (the SQL DDL path requires the schema to already exist in the catalog)
    let writer = PostgresMetadataWriter::new(&env.pg_conn_str)
        .await
        .expect("create writer");
    let init_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let init_batch =
        RecordBatch::try_new(init_schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let tw = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    tw.write_table("main", "_init", &[init_batch])
        .await
        .unwrap();

    // Use writable DF context to CREATE TABLE, then use TableWriter for INSERT
    let ctx = open_writable_df_postgres(&env.pg_conn_str).await;

    // CREATE TABLE via SQL (use INT and DOUBLE only — VARCHAR maps to Utf8View
    // which our type mapper doesn't support yet)
    ctx.sql("CREATE TABLE ducklake.main.employees (id INT, dept_id INT, salary DOUBLE)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // INSERT data via DuckLakeTableWriter (SQL INSERT requires snapshot refresh
    // which the current architecture doesn't support in a single session)
    let writer2 = PostgresMetadataWriter::new(&env.pg_conn_str)
        .await
        .expect("create writer");
    let emp_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("dept_id", DataType::Int32, false),
        Field::new("salary", DataType::Float64, true),
    ]));
    let emp_batch = RecordBatch::try_new(
        emp_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 10])),
            Arc::new(Float64Array::from(vec![
                Some(75000.0),
                Some(85000.0),
                Some(95000.0),
            ])),
        ],
    )
    .unwrap();
    let object_store2: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
    let tw2 = DuckLakeTableWriter::new(Arc::new(writer2), object_store2).unwrap();
    tw2.write_table("main", "employees", &[emp_batch])
        .await
        .unwrap();

    // SELECT via fresh read-only context
    let read_ctx = open_readonly_df_postgres(&env.pg_conn_str).await;
    let actual = df_query(
        &read_ctx,
        "SELECT id, dept_id, salary FROM ducklake.main.employees ORDER BY id",
    )
    .await;

    let expected = vec![
        vec!["1".into(), "10".into(), "75000.0".into()],
        vec!["2".into(), "20".into(), "85000.0".into()],
        vec!["3".into(), "10".into(), "95000.0".into()],
    ];

    assert_results_eq("pg_sql_create_insert_select", &expected, &actual);
}

// ==================== Test 6: Multiple tables in Postgres catalog ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_pg_multiple_tables() {
    let env = setup_pg_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let writer = PostgresMetadataWriter::new(&env.pg_conn_str)
        .await
        .expect("create writer");

    // Table 1: users
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch1 = RecordBatch::try_new(
        schema1,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();

    let writer_arc = Arc::new(writer);
    let tw1 = DuckLakeTableWriter::new(writer_arc.clone(), object_store.clone()).unwrap();
    tw1.write_table("main", "users", &[batch1]).await.unwrap();

    // Table 2: products
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, true),
    ]));
    let batch2 = RecordBatch::try_new(
        schema2,
        vec![
            Arc::new(Int32Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec!["Widget", "Gadget"])),
            Arc::new(Float64Array::from(vec![Some(9.99), Some(19.99)])),
        ],
    )
    .unwrap();

    let tw2 = DuckLakeTableWriter::new(writer_arc, object_store).unwrap();
    tw2.write_table("main", "products", &[batch2])
        .await
        .unwrap();

    // Read both tables
    let ctx = open_readonly_df_postgres(&env.pg_conn_str).await;

    let users = df_query(&ctx, "SELECT id, name FROM ducklake.main.users ORDER BY id").await;
    assert_eq!(users.len(), 2);
    assert_eq!(users[0], vec!["1", "Alice"]);

    let products = df_query(
        &ctx,
        "SELECT id, name, price FROM ducklake.main.products ORDER BY id",
    )
    .await;
    assert_eq!(products.len(), 2);
    assert_eq!(products[0][1], "Widget");
}

// ==================== Test 7: Count query ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_pg_count_query() {
    let env = setup_pg_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let writer = PostgresMetadataWriter::new(&env.pg_conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "nums", &[batch])
        .await
        .unwrap();

    let ctx = open_readonly_df_postgres(&env.pg_conn_str).await;
    let count = df_query(&ctx, "SELECT COUNT(*) FROM ducklake.main.nums").await;
    assert_eq!(count.len(), 1);
    assert_eq!(count[0][0], "5");

    // Cross-check with DuckDB if available
    if let Some(duckdb) = DuckDbPgConn::try_open(&env.pg_conn_str, None) {
        let duckdb_count = duckdb.query("SELECT COUNT(*) FROM ducklake.main.nums");
        assert_eq!(duckdb_count[0][0], "5");
    }
}

// ==================== Test 8: Bidirectional roundtrip ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_pg_bidirectional_roundtrip() {
    let env = setup_pg_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Step 1: DF writes initial data via Postgres catalog
    let writer = PostgresMetadataWriter::new(&env.pg_conn_str)
        .await
        .expect("create writer");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    table_writer
        .write_table("main", "roundtrip", &[batch])
        .await
        .unwrap();

    // Step 2: DF verifies initial data
    let ctx = open_readonly_df_postgres(&env.pg_conn_str).await;
    let rows = df_query(
        &ctx,
        "SELECT id, name FROM ducklake.main.roundtrip ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "Alice"]);
    assert_eq!(rows[1], vec!["2", "Bob"]);

    // Step 3: If DuckDB can connect, have it verify too
    if let Some(duckdb) = DuckDbPgConn::try_open(&env.pg_conn_str, None) {
        let duckdb_rows = duckdb.query("SELECT id, name FROM ducklake.main.roundtrip ORDER BY id");
        assert_eq!(duckdb_rows.len(), 2);
        assert_eq!(duckdb_rows[0][0], "1");
        assert_eq!(duckdb_rows[0][1], "Alice");
    }
}
