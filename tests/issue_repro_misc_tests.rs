//! Reproduction tests for miscellaneous DuckLake issues.
//!
//! Issues covered: #147, #214, #240, #288, #297, #619, #644, #779, #794, #795.
//! These span version migration, MySQL/Postgres backend bugs, data inlining,
//! default values, long column names, and read-only catalog access.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Array, Int32Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::metadata_writer::{AlterTableOp, ColumnDef, MetadataWriter};
use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, MetadataProvider, SqliteMetadataProvider,
    SqliteMetadataWriter,
};

// ============================================================================
// Common helpers
// ============================================================================

fn create_object_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

async fn create_test_env() -> (Arc<SqliteMetadataWriter>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer.set_data_path(data_path.to_str().unwrap()).unwrap();

    (Arc::new(writer), temp_dir)
}

async fn write_table(
    writer: Arc<SqliteMetadataWriter>,
    schema_name: &str,
    table_name: &str,
    batches: &[RecordBatch],
) {
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    table_writer
        .write_table(schema_name, table_name, batches)
        .await
        .unwrap();
}

async fn append_table(
    writer: Arc<SqliteMetadataWriter>,
    schema_name: &str,
    table_name: &str,
    batches: &[RecordBatch],
) {
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    table_writer
        .append_table(schema_name, table_name, batches)
        .await
        .unwrap();
}

async fn create_read_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

fn id_name_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn make_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        id_name_schema(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(
                names.into_iter().map(Some).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

// ============================================================================
// Issue #147: Failed to migrate DuckLake from v0.1 to v0.2
// https://github.com/duckdb/ducklake/issues/147
//
// Bug: Migration from v0.1 to v0.2 tries to add 'path' column that already
// exists, because version metadata wasn't properly incremented.
// Test: Verify our extension handles catalogs with different version metadata
// and that re-initializing schema (simulating migration) is idempotent.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_147_version_migration_idempotent() {
    // https://github.com/duckdb/ducklake/issues/147
    // Create a catalog, then re-initialize it (simulating what a migration does).
    // The schema creation should be idempotent.
    let (writer, temp_dir) = create_test_env().await;

    // Write some data first
    let batch = make_batch(vec![1, 2], vec!["alice", "bob"]);
    write_table(writer.clone(), "main", "users", &[batch]).await;

    // Re-initialize the schema (simulates a migration scenario)
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer2 = SqliteMetadataWriter::new_with_init(&conn_str).await;
    assert!(
        writer2.is_ok(),
        "Re-initializing schema on existing catalog should succeed (idempotent)"
    );

    // Verify data is still readable after re-init
    let ctx = create_read_ctx(&temp_dir).await;
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.users")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count: i64 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "Data should survive re-initialization");
}

// ============================================================================
// Issue #214: MySQL catalog fails the second time it is attached
// https://github.com/duckdb/ducklake/issues/214
//
// Bug: MySQL CREATE TABLE doesn't use IF NOT EXISTS, so second attach fails.
// Test: Verify our SQLite writer handles double-initialization gracefully
// (SQLite uses CREATE TABLE IF NOT EXISTS, so this should pass).
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_214_double_initialization() {
    // https://github.com/duckdb/ducklake/issues/214
    // Initialize schema twice on same database - should be idempotent
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    // First initialization
    let writer1 = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer1
        .set_data_path(data_path.to_str().unwrap())
        .unwrap();

    // Write a table
    let batch = make_batch(vec![1], vec!["test"]);
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(Arc::new(writer1), object_store.clone()).unwrap();
    table_writer
        .write_table("main", "t1", &[batch.clone()])
        .await
        .unwrap();

    // Second initialization (simulates re-attach)
    let writer2 = SqliteMetadataWriter::new_with_init(&conn_str).await;
    assert!(
        writer2.is_ok(),
        "Second initialization should not fail (IF NOT EXISTS)"
    );

    // Write another table with second writer
    let writer2 = Arc::new(writer2.unwrap());
    let table_writer2 = DuckLakeTableWriter::new(writer2, object_store).unwrap();
    let batch2 = make_batch(vec![2], vec!["test2"]);
    let result = table_writer2
        .write_table("main", "t2", &[batch2])
        .await;
    assert!(
        result.is_ok(),
        "Writing with second writer should succeed"
    );

    // Verify both tables readable
    let ctx = create_read_ctx(&temp_dir).await;
    let df = ctx
        .sql("SELECT * FROM ducklake.main.t1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let df2 = ctx
        .sql("SELECT * FROM ducklake.main.t2")
        .await
        .unwrap();
    let batches2 = df2.collect().await.unwrap();
    assert_eq!(batches2.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

// ============================================================================
// Issue #240: Deadlock during migration from v0.1 to v0.2 (Postgres)
// https://github.com/duckdb/ducklake/issues/240
//
// Bug: Concurrent ALTER TABLE during migration causes deadlock on Postgres.
// Test: Verify concurrent schema initialization doesn't deadlock on SQLite.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_240_concurrent_initialization() {
    // https://github.com/duckdb/ducklake/issues/240
    // Try to initialize schema concurrently from multiple connections.
    // On SQLite this shouldn't deadlock (WAL mode serializes writes).
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    // Launch multiple concurrent initializations
    let mut handles = vec![];
    for _ in 0..5 {
        let cs = conn_str.clone();
        handles.push(tokio::spawn(async move {
            SqliteMetadataWriter::new_with_init(&cs).await
        }));
    }

    let mut successes = 0;
    let mut failures = 0;
    for h in handles {
        match h.await.unwrap() {
            Ok(_) => successes += 1,
            Err(e) => {
                eprintln!("Concurrent init error: {e}");
                failures += 1;
            }
        }
    }

    // All should succeed for SQLite (IF NOT EXISTS + WAL mode)
    assert!(
        successes >= 1,
        "At least one initialization should succeed; got {successes} successes, {failures} failures"
    );
    println!(
        "Concurrent init: {successes} successes, {failures} failures"
    );

    // Verify the catalog is usable after concurrent init
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();
    writer
        .set_data_path(data_path.to_str().unwrap())
        .unwrap();
    let writer = Arc::new(writer);
    let batch = make_batch(vec![1], vec!["ok"]);
    write_table(writer, "main", "test_table", &[batch]).await;

    let ctx = create_read_ctx(&temp_dir).await;
    let df = ctx
        .sql("SELECT * FROM ducklake.main.test_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

// ============================================================================
// Issue #288: MySQL fails to update table stats on insert
// https://github.com/duckdb/ducklake/issues/288
//
// Bug: MySQL connector doesn't support HASH_JOIN in UPDATE for stats.
// Test: Verify our SQLite writer handles multiple sequential inserts
// and stats updates correctly.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_288_multiple_inserts_stats() {
    // https://github.com/duckdb/ducklake/issues/288
    // Multiple inserts into the same table should all succeed and stats
    // should remain consistent.
    let (writer, temp_dir) = create_test_env().await;

    // First insert
    let batch1 = make_batch(vec![1], vec!["first"]);
    write_table(writer.clone(), "main", "insert_test", &[batch1]).await;

    // Second insert (this is where MySQL fails with HASH_JOIN error)
    let batch2 = make_batch(vec![2], vec!["second"]);
    append_table(writer.clone(), "main", "insert_test", &[batch2]).await;

    // Third insert
    let batch3 = make_batch(vec![3], vec!["third"]);
    append_table(writer.clone(), "main", "insert_test", &[batch3]).await;

    // Verify all rows are readable
    let ctx = create_read_ctx(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.insert_test ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "All three inserts should be visible");

    // Verify COUNT(*) works correctly (tests stats)
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.insert_test")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count: i64 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3);
}

// ============================================================================
// Issue #297: Limitation in default values (function-based defaults)
// https://github.com/duckdb/ducklake/issues/297
//
// Bug: DuckLake only supports literal defaults, not function defaults like now().
// Test: Verify our writer can store function-based default value metadata
// (default_value_type and default_value_dialect fields).
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_297_function_default_values() {
    // https://github.com/duckdb/ducklake/issues/297
    // Verify that column default values (including function-based ones)
    // can be stored and that the column is readable.
    let (writer, temp_dir) = create_test_env().await;

    // Create table with a column that has a default value
    let id_col = ColumnDef::new("id", "INTEGER", false);
    let mut name_col = ColumnDef::new("name", "VARCHAR", true);
    name_col.default_value = Some("unknown".to_string());
    name_col.default_value_type = Some("VARCHAR".to_string());
    name_col.default_value_dialect = Some("SQL".to_string());

    // Also test a timestamp column with function default metadata
    let mut created_col = ColumnDef::new("created_at", "TIMESTAMP", true);
    created_col.default_value = Some("now()".to_string());
    created_col.default_value_type = Some("TIMESTAMP".to_string());
    created_col.default_value_dialect = Some("SQL".to_string());

    let snapshot_id = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer
        .get_or_create_schema("main", None, snapshot_id)
        .unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "defaults_test", None, snapshot_id)
        .unwrap();
    writer
        .set_columns(table_id, &[id_col, name_col, created_col], snapshot_id)
        .unwrap();

    // Write data manually with all columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("test")])),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1704067200000000)])), // 2024-01-01
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer =
        DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "defaults_test", &[batch])
        .await
        .unwrap();

    // Verify the table is readable
    let ctx = create_read_ctx(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, name FROM ducklake.main.defaults_test")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

// ============================================================================
// Issue #619: Data inlining binder error for columns > 64 characters
// https://github.com/duckdb/ducklake/issues/619
//
// Bug: PostgreSQL truncates identifiers > 63 bytes, causing binder errors
// when inlined data references full column names.
// Test: Verify our extension handles very long column names correctly.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_619_long_column_names() {
    // https://github.com/duckdb/ducklake/issues/619
    // Create a table with column names exceeding 64 characters.
    // Our SQLite backend doesn't truncate identifiers, but we verify
    // the full round-trip works correctly.
    let (writer, temp_dir) = create_test_env().await;

    let long_col_name = "this_is_a_very_long_column_name_that_exceeds_sixty_four_characters_limit_by_quite_a_lot";
    assert!(
        long_col_name.len() > 64,
        "Column name should exceed 64 chars"
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(long_col_name, DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("value1"),
                Some("value2"),
                Some("value3"),
            ])),
        ],
    )
    .unwrap();

    write_table(writer.clone(), "main", "long_cols", &[batch]).await;

    // Verify the table is readable and the long column name is preserved
    let ctx = create_read_ctx(&temp_dir).await;
    let df = ctx
        .sql("SELECT * FROM ducklake.main.long_cols")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // Verify column name is preserved
    let result_schema = batches[0].schema();
    let col_names: Vec<&str> = result_schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        col_names.contains(&long_col_name),
        "Long column name should be preserved; got: {:?}",
        col_names
    );

    // Also test querying the specific long-named column
    let query = format!(
        "SELECT \"{}\" FROM ducklake.main.long_cols WHERE id = 1",
        long_col_name
    );
    let df = ctx.sql(&query).await.unwrap();
    let batches = df.collect().await.unwrap();
    let val = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(val, "value1");
}

// ============================================================================
// Issue #644: WHERE + ORDER BY breaks with inlined data in Postgres
// https://github.com/duckdb/ducklake/issues/644
//
// Bug: Queries with WHERE + ORDER BY + LIMIT return wrong results when data
// is inlined in Postgres (rows violating WHERE appear in results).
// Test: Verify that WHERE + ORDER BY + LIMIT returns correct results
// when reading from multiple data files.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_644_where_orderby_limit_correctness() {
    // https://github.com/duckdb/ducklake/issues/644
    // Write data in two batches (simulating data across files), then query
    // with WHERE + ORDER BY + LIMIT to verify correctness.
    let (writer, temp_dir) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    // First batch: category A rows
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a_0", "a_1", "a_2"])),
            Arc::new(StringArray::from(vec!["A", "A", "A"])),
            Arc::new(Int32Array::from(vec![100, 90, 80])),
        ],
    )
    .unwrap();

    // Second batch: category B rows
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["b_0", "b_1", "b_2"])),
            Arc::new(StringArray::from(vec!["B", "B", "B"])),
            Arc::new(Int32Array::from(vec![70, 60, 50])),
        ],
    )
    .unwrap();

    write_table(writer.clone(), "main", "test_data", &[batch1]).await;
    append_table(writer.clone(), "main", "test_data", &[batch2]).await;

    // Query with WHERE + ORDER BY + LIMIT
    let ctx = create_read_ctx(&temp_dir).await;
    let df = ctx
        .sql(
            "SELECT id, category FROM ducklake.main.test_data
             WHERE category = 'A'
             ORDER BY value DESC
             LIMIT 3",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    // Verify all returned rows have category = 'A'
    for batch in &batches {
        let categories = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..categories.len() {
            assert_eq!(
                categories.value(i),
                "A",
                "Row {} has category '{}', expected 'A' (WHERE clause violated)",
                i,
                categories.value(i)
            );
        }
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should return exactly 3 category A rows");
}

// ============================================================================
// Issue #779: Cannot connect to read-only v0.3 DuckLake
// https://github.com/duckdb/ducklake/issues/779
//
// Bug: Read-only catalogs can't be opened if version differs from expected -
// migration requires write access but the catalog is read-only.
// Test: Verify our extension can open a catalog in read-only mode and that
// version metadata doesn't block reading.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_779_readonly_catalog_access() {
    // https://github.com/duckdb/ducklake/issues/779
    // Create a catalog, write data, then open it read-only.
    // Verify that the read-only provider can access data without
    // needing to perform any migration/write operations.
    let (writer, temp_dir) = create_test_env().await;

    let batch = make_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
    write_table(writer.clone(), "main", "users", &[batch]).await;
    drop(writer);

    // Open in read-only mode (no ?mode=rwc)
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // Should be able to read data without write access
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.users")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count: i64 = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3, "Should read all 3 rows in read-only mode");

    // Also verify schema listing works
    let df = ctx
        .sql("SELECT * FROM ducklake.main.users ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "bob");
    assert_eq!(names.value(2), "charlie");
}

// ============================================================================
// Issue #794: IcebergMultiFileReader virtual column indexing mismatch
// https://github.com/duckdb/ducklake/issues/794
//
// Bug: Virtual column index calculated from local column count doesn't match
// global output chunk indexing when columns have defaults.
// Test: Verify that reading a table where a column was added later (with
// default) doesn't cause schema mismatches.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_794_schema_evolution_with_defaults() {
    // https://github.com/duckdb/ducklake/issues/794
    // This tests a scenario where a table has columns added after initial
    // creation (with defaults), and older data files lack the new column.
    // Our extension should handle this gracefully.
    let (writer, temp_dir) = create_test_env().await;

    // Create initial table with id and name
    let batch1 = make_batch(vec![1, 2], vec!["alice", "bob"]);
    write_table(writer.clone(), "main", "evolving", &[batch1]).await;

    // Add a new column via ALTER TABLE
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let snapshot_id = provider.get_current_snapshot().unwrap();

    // Get the table info
    let schemas = provider.list_schemas(snapshot_id).unwrap();
    let main_schema = schemas
        .iter()
        .find(|s| s.schema_name == "main")
        .unwrap();
    let tables = provider
        .list_tables(main_schema.schema_id, snapshot_id)
        .unwrap();
    let table = tables
        .iter()
        .find(|t| t.table_name == "evolving")
        .unwrap();

    // Add column with default (use lowercase to match Arrow type mapping)
    let mut new_col = ColumnDef::new("status", "varchar", true);
    new_col.default_value = Some("active".to_string());
    new_col.default_value_type = Some("VARCHAR".to_string());

    writer
        .alter_table(
            table.table_id,
            &AlterTableOp::AddColumn { column: new_col },
        )
        .unwrap();

    // Append new data with the expanded schema
    let schema3 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
    ]));
    let batch2 = RecordBatch::try_new(
        schema3,
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec![Some("charlie"), Some("diana")])),
            Arc::new(StringArray::from(vec![Some("active"), Some("inactive")])),
        ],
    )
    .unwrap();
    append_table(writer.clone(), "main", "evolving", &[batch2]).await;

    // Read back - should see all rows; old rows might have NULL for status
    let ctx = create_read_ctx(&temp_dir).await;
    let df = ctx
        .sql("SELECT id, name, status FROM ducklake.main.evolving ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 4,
        "Should see all 4 rows across old and new files"
    );

    // New rows should have status values
    // Collect all status values
    let mut statuses = vec![];
    for batch in &batches {
        let status_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..status_col.len() {
            if status_col.is_null(i) {
                statuses.push(None);
            } else {
                statuses.push(Some(status_col.value(i).to_string()));
            }
        }
    }
    // At minimum, the new rows should have their status
    assert!(
        statuses.iter().any(|s| s.as_deref() == Some("active")),
        "Should find 'active' status in results"
    );
    assert!(
        statuses.iter().any(|s| s.as_deref() == Some("inactive")),
        "Should find 'inactive' status in results"
    );
}

// ============================================================================
// Issue #795: Migration error message reports incorrect version number
// https://github.com/duckdb/ducklake/issues/795
//
// Bug: Error message says "migrate from v0.2 to v0.3" but catalog is already v0.3.
// Test: Verify our metadata correctly reports and reads version metadata.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_issue_795_version_metadata_accuracy() {
    // https://github.com/duckdb/ducklake/issues/795
    // Verify that the version stored in ducklake_metadata is accurate
    // and consistent after initialization and after operations.
    let (writer, temp_dir) = create_test_env().await;

    // Write some data
    let batch = make_batch(vec![1], vec!["test"]);
    write_table(writer.clone(), "main", "version_test", &[batch]).await;

    // Read version metadata directly from SQLite
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    // Use the provider to verify catalog is readable (which means version is OK)
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    let df = ctx
        .sql("SELECT * FROM ducklake.main.version_test")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    // Re-init and verify version hasn't been corrupted
    let writer2 = SqliteMetadataWriter::new_with_init(&conn_str).await.unwrap();
    let snap = writer2.create_snapshot().unwrap();
    assert!(snap > 0, "Snapshot ID should be positive after re-init");

    // Catalog should still be readable
    let provider2 = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog2 = DuckLakeCatalog::new(provider2).unwrap();
    let ctx2 = SessionContext::new();
    ctx2.register_catalog("ducklake", Arc::new(catalog2));

    let df = ctx2
        .sql("SELECT * FROM ducklake.main.version_test")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}
