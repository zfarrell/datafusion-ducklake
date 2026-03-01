//! Cross-engine tests with MySQL as the DuckLake catalog backend.
//!
//! Tests verify DataFusion can write to and read from a MySQL-backed DuckLake catalog,
//! and that DuckDB can also read from the same MySQL catalog (full interop).
//!
//! Test patterns:
//! 1. DF writes via MySqlMetadataWriter → DF reads via MySqlMetadataProvider
//! 2. DF writes via MySqlMetadataWriter → DuckDB reads via ducklake:mysql:
//! 3. DuckDB writes via ducklake:mysql: → DF reads via MySqlMetadataProvider
//! 4. SQL-based DDL/DML (CREATE TABLE, INSERT, SELECT)
//!
//! Requires: Docker (testcontainers spins up MySQL), features: write-mysql, metadata-duckdb

#![cfg(all(feature = "write-mysql", feature = "metadata-duckdb"))]

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
    MySqlMetadataProvider, MySqlMetadataWriter,
};

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mysql::Mysql;

// ==================== Setup helpers ====================

/// Environment for MySQL-backed cross-engine tests.
struct MysqlCrossEngineEnv {
    _temp_dir: TempDir,
    #[allow(dead_code)]
    data_path: String,
    mysql_conn_str: String,
    _container: testcontainers::ContainerAsync<Mysql>,
}

/// Starts a MySQL container and initializes a DuckLake catalog.
async fn setup_mysql_env() -> MysqlCrossEngineEnv {
    let container = Mysql::default().start().await.unwrap();
    let host = "127.0.0.1";
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let mysql_conn_str = format!("mysql://root@{}:{}/test", host, port);

    // Initialize DuckLake schema tables in MySQL
    let writer = MySqlMetadataWriter::new_with_init(&mysql_conn_str)
        .await
        .expect("init mysql catalog");

    // Set up local data directory for Parquet files
    let temp_dir = TempDir::new().expect("create temp dir");
    let data_path = format!("{}/", temp_dir.path().join("data").display());
    std::fs::create_dir_all(&data_path).expect("create data dir");
    writer.set_data_path(&data_path).expect("set data path");

    MysqlCrossEngineEnv {
        _temp_dir: temp_dir,
        data_path,
        mysql_conn_str,
        _container: container,
    }
}

// ==================== Context helpers ====================

/// Open a read-only DataFusion context using MySqlMetadataProvider.
async fn open_readonly_df_mysql(conn_str: &str) -> SessionContext {
    let provider = MySqlMetadataProvider::new(conn_str)
        .await
        .expect("create MySqlMetadataProvider");
    let catalog = DuckLakeCatalog::new(provider).expect("create DuckLakeCatalog");
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Open a writable DataFusion context using MySQL backend.
async fn open_writable_df_mysql(conn_str: &str) -> SessionContext {
    let provider = Arc::new(
        MySqlMetadataProvider::new(conn_str)
            .await
            .expect("create MySqlMetadataProvider"),
    );
    let writer = Arc::new(
        MySqlMetadataWriter::new(conn_str)
            .await
            .expect("create MySqlMetadataWriter"),
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

// ==================== DuckDB with MySQL catalog ====================

struct DuckDbMysqlConn {
    conn: duckdb::Connection,
}

impl DuckDbMysqlConn {
    /// Open DuckDB and attach to a MySQL-backed DuckLake catalog.
    /// Returns None if DuckDB can't load required extensions.
    fn try_open(mysql_conn_str: &str, data_path: Option<&str>) -> Option<Self> {
        let conn = duckdb::Connection::open_in_memory().ok()?;
        conn.execute("INSTALL ducklake;", []).ok()?;
        conn.execute("LOAD ducklake;", []).ok()?;
        conn.execute("INSTALL mysql;", []).ok()?;
        conn.execute("LOAD mysql;", []).ok()?;

        // Convert sqlx-style to DuckDB mysql connection string
        // sqlx: mysql://user@host:port/db or mysql://user:pass@host:port/db
        // DuckDB mysql: host=H port=P database=D user=U password=P
        let duckdb_str = sqlx_mysql_to_duckdb(mysql_conn_str);
        let attach = if let Some(dp) = data_path {
            format!(
                "ATTACH 'ducklake:mysql:{}' AS ducklake (DATA_PATH '{}');",
                duckdb_str, dp
            )
        } else {
            format!("ATTACH 'ducklake:mysql:{}' AS ducklake;", duckdb_str)
        };

        match conn.execute(&attach, []) {
            Ok(_) => Some(DuckDbMysqlConn {
                conn,
            }),
            Err(e) => {
                eprintln!("DuckDB couldn't attach MySQL DuckLake catalog: {e}");
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

/// Convert sqlx-style MySQL URL to DuckDB-style connection string.
/// Input: mysql://user@host:port/db or mysql://user:pass@host:port/db
/// Output: host=host port=port database=db user=user password=pass
fn sqlx_mysql_to_duckdb(url: &str) -> String {
    let url = url::Url::parse(url).expect("parse mysql url");
    let host = url.host_str().unwrap_or("localhost");
    let port = url.port().unwrap_or(3306);
    let user = url.username();
    let password = url.password().unwrap_or("");
    let db = url.path().trim_start_matches('/');
    if password.is_empty() {
        format!("host={} port={} database={} user={}", host, port, db, user)
    } else {
        format!(
            "host={} port={} database={} user={} password={}",
            host, port, db, user, password
        )
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

// ==================== Query helpers ====================

async fn df_query(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let df = ctx.sql(sql).await.expect("DataFusion SQL failed");
    let batches = df.collect().await.expect("DataFusion collect failed");
    batches_to_strings(&batches)
}

fn batches_to_strings(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let col_indices: Vec<usize> = (0..batch.num_columns())
            .filter(|&i| {
                let name = schema.field(i).name();
                name != "filename" && name != "file_row_number"
            })
            .collect();
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for &col_idx in &col_indices {
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
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            a.value(idx).to_string()
        },
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            format!("{}", a.value(idx))
        },
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            a.value(idx).to_string()
        },
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            a.value(idx).to_string()
        },
        other => format!("<unsupported:{other:?}>"),
    }
}

fn normalize_value(s: &str) -> String {
    if s == "NULL" {
        return s.to_string();
    }
    if let Ok(f) = s.parse::<f64>() {
        return format!("{:.6}", f);
    }
    s.to_string()
}

fn assert_results_eq(scenario: &str, expected: &[Vec<String>], actual: &[Vec<String>]) {
    assert_eq!(
        expected.len(),
        actual.len(),
        "[{scenario}] Row count mismatch: expected {}, got {}.\n  Expected: {expected:?}\n  Actual: {actual:?}",
        expected.len(),
        actual.len()
    );
    for (i, (exp, act)) in expected.iter().zip(actual.iter()).enumerate() {
        for (j, (ev, av)) in exp.iter().zip(act.iter()).enumerate() {
            assert_eq!(
                normalize_value(ev),
                normalize_value(av),
                "[{scenario}] Mismatch at row {i}, col {j}: expected '{ev}', got '{av}'"
            );
        }
    }
}

// ==================== Test 1: DF writes → DF reads (MySQL catalog) ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_mysql_df_write_df_read() {
    let env = setup_mysql_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Write using DuckLakeTableWriter with MySQL backend
    let writer = MySqlMetadataWriter::new(&env.mysql_conn_str)
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

    // Read back using DataFusion + MySqlMetadataProvider
    let ctx = open_readonly_df_mysql(&env.mysql_conn_str).await;
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

    assert_results_eq("mysql_df_write_df_read", &expected, &actual);
}

// ==================== Test 2: DF writes → DuckDB reads (MySQL catalog) ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_mysql_df_write_duckdb_read() {
    let env = setup_mysql_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Write using DuckLakeTableWriter
    let writer = MySqlMetadataWriter::new(&env.mysql_conn_str)
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

    // Read using DuckDB with MySQL DuckLake catalog
    let duckdb = match DuckDbMysqlConn::try_open(&env.mysql_conn_str, None) {
        Some(d) => d,
        None => {
            eprintln!("Skipping DuckDB read: DuckDB mysql extension not available");
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

// ==================== Test 3: DuckDB writes → DF reads (MySQL catalog) ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_mysql_duckdb_write_df_read() {
    // Use a fresh MySQL container — DuckDB initializes the DuckLake catalog itself
    let container = Mysql::default().start().await.unwrap();
    let host = "127.0.0.1";
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let mysql_conn_str = format!("mysql://root@{}:{}/test", host, port);

    let temp_dir = TempDir::new().unwrap();
    let data_path = format!("{}/", temp_dir.path().join("data").display());
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB creates the catalog from scratch via MySQL-backed DuckLake
    let duckdb = match DuckDbMysqlConn::try_open(&mysql_conn_str, Some(&data_path)) {
        Some(d) => d,
        None => {
            eprintln!("Skipping: DuckDB mysql extension not available");
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

    // DF reads via MySqlMetadataProvider
    let ctx = open_readonly_df_mysql(&mysql_conn_str).await;
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

    assert_results_eq("mysql_duckdb_write_df_read", &expected, &actual);
}

// ==================== Test 4: NULL handling roundtrip ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_mysql_null_handling() {
    let env = setup_mysql_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let writer = MySqlMetadataWriter::new(&env.mysql_conn_str)
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
    let ctx = open_readonly_df_mysql(&env.mysql_conn_str).await;
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
    if let Some(duckdb) = DuckDbMysqlConn::try_open(&env.mysql_conn_str, None) {
        let rows = duckdb.query("SELECT id, name, value FROM ducklake.main.nulls_test ORDER BY id");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1][1], "NULL");
        assert_eq!(rows[2][2], "NULL");
    }
}

// ==================== Test 5: SQL DDL via DF with MySQL catalog ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_mysql_sql_create_insert_select() {
    let env = setup_mysql_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // First, bootstrap the "main" schema by writing an initial table via TableWriter
    // (the SQL DDL path requires the schema to already exist in the catalog)
    let writer = MySqlMetadataWriter::new(&env.mysql_conn_str)
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
    let ctx = open_writable_df_mysql(&env.mysql_conn_str).await;

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
    let writer2 = MySqlMetadataWriter::new(&env.mysql_conn_str)
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
    let read_ctx = open_readonly_df_mysql(&env.mysql_conn_str).await;
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

    assert_results_eq("mysql_sql_create_insert_select", &expected, &actual);
}

// ==================== Test 6: Multiple tables in MySQL catalog ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_mysql_multiple_tables() {
    let env = setup_mysql_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let writer = MySqlMetadataWriter::new(&env.mysql_conn_str)
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
    let ctx = open_readonly_df_mysql(&env.mysql_conn_str).await;

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
async fn cross_engine_mysql_count_query() {
    let env = setup_mysql_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    let writer = MySqlMetadataWriter::new(&env.mysql_conn_str)
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

    let ctx = open_readonly_df_mysql(&env.mysql_conn_str).await;
    let count = df_query(&ctx, "SELECT COUNT(*) FROM ducklake.main.nums").await;
    assert_eq!(count.len(), 1);
    assert_eq!(count[0][0], "5");

    // Cross-check with DuckDB if available
    if let Some(duckdb) = DuckDbMysqlConn::try_open(&env.mysql_conn_str, None) {
        let duckdb_count = duckdb.query("SELECT COUNT(*) FROM ducklake.main.nums");
        assert_eq!(duckdb_count[0][0], "5");
    }
}

// ==================== Test 8: Bidirectional roundtrip ====================

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires Docker
async fn cross_engine_mysql_bidirectional_roundtrip() {
    let env = setup_mysql_env().await;
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());

    // Step 1: DF writes initial data via MySQL catalog
    let writer = MySqlMetadataWriter::new(&env.mysql_conn_str)
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
    let ctx = open_readonly_df_mysql(&env.mysql_conn_str).await;
    let rows = df_query(
        &ctx,
        "SELECT id, name FROM ducklake.main.roundtrip ORDER BY id",
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "Alice"]);
    assert_eq!(rows[1], vec!["2", "Bob"]);

    // Step 3: If DuckDB can connect, have it verify too
    if let Some(duckdb) = DuckDbMysqlConn::try_open(&env.mysql_conn_str, None) {
        let duckdb_rows = duckdb.query("SELECT id, name FROM ducklake.main.roundtrip ORDER BY id");
        assert_eq!(duckdb_rows.len(), 2);
        assert_eq!(duckdb_rows[0][0], "1");
        assert_eq!(duckdb_rows[0][1], "Alice");
    }
}
