//! Integration tests for DROP TABLE, DROP SCHEMA, and NOT NULL constraints.
//!
//! These tests verify the DDL operations and constraint enforcement
//! implemented in the DuckLake catalog extension.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter, WriteMode,
};

/// Helper to create a test environment with writer and data directory.
async fn create_test_env() -> (SqliteMetadataWriter, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer.set_data_path(data_path.to_str().unwrap()).unwrap();

    (writer, temp_dir)
}

/// Helper to create a fresh writable context for an existing temp_dir.
async fn create_writable_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::with_writer(Arc::new(provider), Arc::new(writer)).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Helper to create a read-only context for verification.
async fn create_read_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Helper to create a table with initial data using the lower-level writer API.
async fn create_table_with_data(
    temp_dir: &TempDir,
    schema_name: &str,
    table_name: &str,
    schema: &Schema,
    batches: Vec<RecordBatch>,
) {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new(&conn_str).await.unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());

    let table_writer = DuckLakeTableWriter::new(Arc::new(writer), object_store).unwrap();
    let mut session = table_writer
        .begin_write(schema_name, table_name, schema, WriteMode::Replace)
        .unwrap();

    for batch in &batches {
        session.write_batch(batch).unwrap();
    }
    session.finish().await.unwrap();
}

/// Helper schema with id (non-nullable) and name (nullable).
fn test_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

/// Helper to create a simple test batch.
fn test_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(test_schema()),
        vec![Arc::new(Int32Array::from(ids)), Arc::new(StringArray::from(names))],
    )
    .unwrap()
}

// =============================================================================
// DROP TABLE tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_basic() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a table using low-level API
    create_table_with_data(
        &temp_dir,
        "main",
        "users",
        &test_schema(),
        vec![test_batch(vec![1, 2, 3], vec!["Alice", "Bob", "Charlie"])],
    )
    .await;

    // Verify table exists
    let read_ctx = create_read_ctx(&temp_dir).await;
    let result = read_ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.users")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 3);

    // Drop the table
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("DROP TABLE ducklake.main.users")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify table is gone (fresh context to get new snapshot)
    let read_ctx2 = create_read_ctx(&temp_dir).await;
    let result = read_ctx2
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.users")
        .await;
    assert!(
        result.is_err(),
        "Querying dropped table should fail, but got Ok"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_not_in_table_names() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a table
    create_table_with_data(
        &temp_dir,
        "main",
        "to_drop",
        &test_schema(),
        vec![test_batch(vec![1], vec!["test"])],
    )
    .await;

    // Verify table shows up in table_names
    let ctx = create_writable_ctx(&temp_dir).await;
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema = catalog.schema("main").unwrap();
    assert!(
        schema.table_names().contains(&"to_drop".to_string()),
        "Table should appear in table_names before drop"
    );

    // Drop the table
    ctx.sql("DROP TABLE ducklake.main.to_drop")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify table_names no longer includes the dropped table (fresh context)
    let ctx2 = create_writable_ctx(&temp_dir).await;
    let catalog2 = ctx2.catalog("ducklake").unwrap();
    let schema2 = catalog2.schema("main").unwrap();
    assert!(
        !schema2.table_names().contains(&"to_drop".to_string()),
        "Dropped table should NOT appear in table_names"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_recreate_after_drop() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create table with data
    create_table_with_data(
        &temp_dir,
        "main",
        "recyclable",
        &test_schema(),
        vec![test_batch(vec![1, 2], vec!["old1", "old2"])],
    )
    .await;

    // Drop the table
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("DROP TABLE ducklake.main.recyclable")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Re-create with different data
    create_table_with_data(
        &temp_dir,
        "main",
        "recyclable",
        &test_schema(),
        vec![test_batch(vec![10, 20, 30], vec!["new1", "new2", "new3"])],
    )
    .await;

    // Verify only the new data is visible
    let read_ctx = create_read_ctx(&temp_dir).await;
    let result = read_ctx
        .sql("SELECT id, name FROM ducklake.main.recyclable ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Re-created table should have 3 rows");

    let ids = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[10, 20, 30]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_if_exists_nonexistent() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create the "main" schema by writing a table first (so the schema exists)
    create_table_with_data(
        &temp_dir,
        "main",
        "dummy",
        &test_schema(),
        vec![test_batch(vec![1], vec!["x"])],
    )
    .await;

    let ctx = create_writable_ctx(&temp_dir).await;

    // DROP TABLE IF EXISTS on a non-existent table should succeed silently
    let result = ctx
        .sql("DROP TABLE IF EXISTS ducklake.main.nonexistent")
        .await;
    assert!(
        result.is_ok(),
        "DROP TABLE IF EXISTS on non-existent table should not error: {:?}",
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
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_without_if_exists_nonexistent() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create the "main" schema
    create_table_with_data(
        &temp_dir,
        "main",
        "dummy",
        &test_schema(),
        vec![test_batch(vec![1], vec!["x"])],
    )
    .await;

    let ctx = create_writable_ctx(&temp_dir).await;

    // DROP TABLE without IF EXISTS on non-existent table should error
    let result = ctx.sql("DROP TABLE ducklake.main.nonexistent").await;
    match result {
        Ok(df) => {
            let exec = df.collect().await;
            // Either planning or execution should fail
            assert!(
                exec.is_err(),
                "DROP TABLE on non-existent table should fail"
            );
        },
        Err(_) => {
            // Planning-time failure is also acceptable
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_table_exist_returns_false() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a table
    create_table_with_data(
        &temp_dir,
        "main",
        "check_exist",
        &test_schema(),
        vec![test_batch(vec![1], vec!["test"])],
    )
    .await;

    // Verify table_exist returns true
    let ctx = create_writable_ctx(&temp_dir).await;
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema = catalog.schema("main").unwrap();
    assert!(
        schema.table_exist("check_exist"),
        "table_exist should return true for existing table"
    );

    // Drop the table
    ctx.sql("DROP TABLE ducklake.main.check_exist")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify table_exist returns false (fresh context)
    let ctx2 = create_writable_ctx(&temp_dir).await;
    let catalog2 = ctx2.catalog("ducklake").unwrap();
    let schema2 = catalog2.schema("main").unwrap();
    assert!(
        !schema2.table_exist("check_exist"),
        "table_exist should return false for dropped table"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_other_tables_unaffected() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create two tables
    create_table_with_data(
        &temp_dir,
        "main",
        "keep_me",
        &test_schema(),
        vec![test_batch(vec![1], vec!["keep"])],
    )
    .await;
    create_table_with_data(
        &temp_dir,
        "main",
        "drop_me",
        &test_schema(),
        vec![test_batch(vec![2], vec!["drop"])],
    )
    .await;

    // Drop one table
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("DROP TABLE ducklake.main.drop_me")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify the other table is still accessible
    let read_ctx = create_read_ctx(&temp_dir).await;
    let result = read_ctx
        .sql("SELECT name FROM ducklake.main.keep_me")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let names = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "keep");
}

// =============================================================================
// DROP SCHEMA tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_schema_empty() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a schema by creating and dropping a table in it
    create_table_with_data(
        &temp_dir,
        "empty_schema",
        "temp_table",
        &test_schema(),
        vec![test_batch(vec![1], vec!["temp"])],
    )
    .await;

    // Drop the table to make the schema empty
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("DROP TABLE ducklake.empty_schema.temp_table")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Now drop the empty schema
    let ctx2 = create_writable_ctx(&temp_dir).await;
    let result = ctx2.sql("DROP SCHEMA ducklake.empty_schema").await;

    match result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(
                exec.is_ok(),
                "DROP SCHEMA on empty schema should succeed: {:?}",
                exec.err()
            );
        },
        Err(e) => {
            panic!(
                "DROP SCHEMA on empty schema should not fail during planning: {}",
                e
            );
        },
    }

    // Verify schema is gone
    let ctx3 = create_writable_ctx(&temp_dir).await;
    let catalog = ctx3.catalog("ducklake").unwrap();
    let schema_names = catalog.schema_names();
    assert!(
        !schema_names.contains(&"empty_schema".to_string()),
        "Dropped schema should not appear in schema_names, got: {:?}",
        schema_names
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_schema_cascade() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a schema with tables
    create_table_with_data(
        &temp_dir,
        "cascade_schema",
        "table1",
        &test_schema(),
        vec![test_batch(vec![1], vec!["a"])],
    )
    .await;
    create_table_with_data(
        &temp_dir,
        "cascade_schema",
        "table2",
        &test_schema(),
        vec![test_batch(vec![2], vec!["b"])],
    )
    .await;

    // Drop schema with CASCADE
    let ctx = create_writable_ctx(&temp_dir).await;
    let result = ctx.sql("DROP SCHEMA ducklake.cascade_schema CASCADE").await;

    match result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(
                exec.is_ok(),
                "DROP SCHEMA CASCADE should succeed: {:?}",
                exec.err()
            );
        },
        Err(e) => {
            panic!("DROP SCHEMA CASCADE should not fail: {}", e);
        },
    }

    // Verify schema and tables are gone
    let ctx2 = create_writable_ctx(&temp_dir).await;
    let catalog = ctx2.catalog("ducklake").unwrap();
    let schema_names = catalog.schema_names();
    assert!(
        !schema_names.contains(&"cascade_schema".to_string()),
        "Cascaded schema should not appear in schema_names"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_schema_nonempty_without_cascade_fails() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a schema with a table
    create_table_with_data(
        &temp_dir,
        "nonempty_schema",
        "my_table",
        &test_schema(),
        vec![test_batch(vec![1], vec!["data"])],
    )
    .await;

    // Try to drop the non-empty schema without CASCADE
    let ctx = create_writable_ctx(&temp_dir).await;
    let result = ctx.sql("DROP SCHEMA ducklake.nonempty_schema").await;

    match result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(
                exec.is_err(),
                "DROP SCHEMA on non-empty schema without CASCADE should fail"
            );
            let err_msg = exec.unwrap_err().to_string();
            assert!(
                err_msg.contains("depend")
                    || err_msg.contains("CASCADE")
                    || err_msg.contains("not empty"),
                "Error should mention dependencies or CASCADE, got: {}",
                err_msg
            );
        },
        Err(e) => {
            // Planning-time failure is also acceptable
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("depend")
                    || err_msg.contains("CASCADE")
                    || err_msg.contains("not empty"),
                "Error should mention dependencies or CASCADE, got: {}",
                err_msg
            );
        },
    }

    // Verify schema is still there
    let ctx2 = create_writable_ctx(&temp_dir).await;
    let catalog = ctx2.catalog("ducklake").unwrap();
    let schema_names = catalog.schema_names();
    assert!(
        schema_names.contains(&"nonempty_schema".to_string()),
        "Schema should still exist after failed drop"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_schema_not_in_schema_names() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a schema with a table, then cascade-drop it
    create_table_with_data(
        &temp_dir,
        "vanishing_schema",
        "tbl",
        &test_schema(),
        vec![test_batch(vec![1], vec!["x"])],
    )
    .await;

    let ctx = create_writable_ctx(&temp_dir).await;

    // Verify schema is in schema_names before drop
    let catalog = ctx.catalog("ducklake").unwrap();
    assert!(
        catalog
            .schema_names()
            .contains(&"vanishing_schema".to_string()),
        "Schema should be in schema_names before drop"
    );

    ctx.sql("DROP SCHEMA ducklake.vanishing_schema CASCADE")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify with fresh context
    let ctx2 = create_writable_ctx(&temp_dir).await;
    let catalog2 = ctx2.catalog("ducklake").unwrap();
    assert!(
        !catalog2
            .schema_names()
            .contains(&"vanishing_schema".to_string()),
        "Schema should NOT be in schema_names after drop"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_schema_if_exists_nonexistent() {
    let (_writer, temp_dir) = create_test_env().await;

    let ctx = create_writable_ctx(&temp_dir).await;

    // DROP SCHEMA IF EXISTS on non-existent schema should succeed
    let result = ctx
        .sql("DROP SCHEMA IF EXISTS ducklake.nonexistent_schema")
        .await;

    assert!(
        result.is_ok(),
        "DROP SCHEMA IF EXISTS on non-existent schema should not error: {:?}",
        result.err()
    );
    if let Ok(df) = result {
        let exec = df.collect().await;
        assert!(exec.is_ok(), "Execution should succeed: {:?}", exec.err());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_schema_information_schema_fails() {
    let (_writer, temp_dir) = create_test_env().await;

    let ctx = create_writable_ctx(&temp_dir).await;

    // Attempting to drop information_schema should fail
    let result = ctx
        .sql("DROP SCHEMA ducklake.information_schema CASCADE")
        .await;

    match result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(exec.is_err(), "Dropping information_schema should fail");
            let err_msg = exec.unwrap_err().to_string();
            assert!(
                err_msg.contains("information_schema"),
                "Error should mention information_schema, got: {}",
                err_msg
            );
        },
        Err(e) => {
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("information_schema"),
                "Error should mention information_schema, got: {}",
                err_msg
            );
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_schema_cascade_tables_not_queryable() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create schema with multiple tables
    create_table_with_data(
        &temp_dir,
        "full_schema",
        "orders",
        &test_schema(),
        vec![test_batch(vec![1, 2], vec!["order1", "order2"])],
    )
    .await;
    create_table_with_data(
        &temp_dir,
        "full_schema",
        "customers",
        &test_schema(),
        vec![test_batch(vec![10, 20], vec!["cust1", "cust2"])],
    )
    .await;

    // Cascade-drop the schema
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("DROP SCHEMA ducklake.full_schema CASCADE")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify neither table is queryable
    let read_ctx = create_read_ctx(&temp_dir).await;
    let r1 = read_ctx
        .sql("SELECT * FROM ducklake.full_schema.orders")
        .await;
    assert!(
        r1.is_err(),
        "Orders table should not be queryable after cascade drop"
    );

    let r2 = read_ctx
        .sql("SELECT * FROM ducklake.full_schema.customers")
        .await;
    assert!(
        r2.is_err(),
        "Customers table should not be queryable after cascade drop"
    );
}

// =============================================================================
// NOT NULL constraint tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_not_null_insert_with_null_fails() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a table with NOT NULL columns using the writer API
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),  // NOT NULL
        Field::new("name", DataType::Utf8, false), // NOT NULL
        Field::new("email", DataType::Utf8, true), // nullable
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice"])),
            Arc::new(StringArray::from(vec![Some("alice@test.com")])),
        ],
    )
    .unwrap();

    create_table_with_data(&temp_dir, "main", "strict_table", &schema, vec![batch]).await;

    // Try to INSERT a row with NULL in the NOT NULL 'name' column
    let ctx = create_writable_ctx(&temp_dir).await;

    // Create source with NULL in the 'name' column
    let null_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true), // nullable in source
        Field::new("email", DataType::Utf8, true),
    ]));

    let null_batch = RecordBatch::try_new(
        null_schema,
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec![None::<&str>])), // NULL value
            Arc::new(StringArray::from(vec![Some("bob@test.com")])),
        ],
    )
    .unwrap();

    ctx.register_batch("null_source", null_batch).unwrap();

    let result = ctx
        .sql("INSERT INTO ducklake.main.strict_table (id, name, email) SELECT * FROM null_source")
        .await;

    match result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(
                exec.is_err(),
                "INSERT with NULL in NOT NULL column should fail"
            );
            let err_msg = exec.unwrap_err().to_string();
            assert!(
                err_msg.contains("NOT NULL") || err_msg.contains("null"),
                "Error should mention NOT NULL constraint, got: {}",
                err_msg
            );
        },
        Err(e) => {
            // Planning-time failure is also acceptable
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("NOT NULL") || err_msg.contains("null"),
                "Error should mention NOT NULL constraint, got: {}",
                err_msg
            );
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_not_null_insert_valid_succeeds() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a table with NOT NULL columns
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),  // NOT NULL
        Field::new("name", DataType::Utf8, false), // NOT NULL
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1])), Arc::new(StringArray::from(vec!["Alice"]))],
    )
    .unwrap();

    create_table_with_data(
        &temp_dir,
        "main",
        "valid_insert_table",
        &schema,
        vec![batch],
    )
    .await;

    // INSERT with valid (non-NULL) values should succeed
    let ctx = create_writable_ctx(&temp_dir).await;
    let valid_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["Bob", "Charlie"])),
        ],
    )
    .unwrap();
    ctx.register_batch("valid_source", valid_batch).unwrap();

    let result = ctx
        .sql("INSERT INTO ducklake.main.valid_insert_table (id, name) SELECT * FROM valid_source")
        .await
        .unwrap()
        .collect()
        .await;

    assert!(
        result.is_ok(),
        "INSERT with valid non-NULL values should succeed: {:?}",
        result.err()
    );

    // Verify the data was inserted
    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.valid_insert_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        count, 3,
        "Table should have 3 rows (1 original + 2 inserted)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_not_null_nullable_column_accepts_null() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a table where 'email' is nullable
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("email", DataType::Utf8, true), // nullable
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("a@b.com")])),
        ],
    )
    .unwrap();

    create_table_with_data(
        &temp_dir,
        "main",
        "nullable_col_table",
        &schema,
        vec![batch],
    )
    .await;

    // INSERT with NULL in the nullable column should succeed
    let ctx = create_writable_ctx(&temp_dir).await;
    let null_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int32Array::from(vec![2])), Arc::new(StringArray::from(vec![None::<&str>]))],
    )
    .unwrap();
    ctx.register_batch("null_email_source", null_batch).unwrap();

    let result = ctx
        .sql("INSERT INTO ducklake.main.nullable_col_table (id, email) SELECT * FROM null_email_source")
        .await
        .unwrap()
        .collect()
        .await;

    assert!(
        result.is_ok(),
        "INSERT with NULL in nullable column should succeed: {:?}",
        result.err()
    );

    // Verify the NULL was stored
    let read_ctx = create_read_ctx(&temp_dir).await;
    let df = read_ctx
        .sql("SELECT id, email FROM ducklake.main.nullable_col_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 2);

    let emails = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(emails.is_null(1), "Second row's email should be NULL");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_not_null_mixed_null_batch_fails() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a table with a NOT NULL column
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),   // NOT NULL
        Field::new("value", DataType::Utf8, false), // NOT NULL
    ]);

    let initial_batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1])), Arc::new(StringArray::from(vec!["init"]))],
    )
    .unwrap();

    create_table_with_data(
        &temp_dir,
        "main",
        "mixed_null_table",
        &schema,
        vec![initial_batch],
    )
    .await;

    // Try to INSERT a batch where some rows have NULL and some don't
    let ctx = create_writable_ctx(&temp_dir).await;

    let mixed_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true), // allow nulls in source
    ]));

    let mixed_batch = RecordBatch::try_new(
        mixed_schema,
        vec![
            Arc::new(Int32Array::from(vec![2, 3, 4])),
            Arc::new(StringArray::from(vec![
                Some("valid"),
                None, // NULL in NOT NULL column
                Some("also_valid"),
            ])),
        ],
    )
    .unwrap();

    ctx.register_batch("mixed_source", mixed_batch).unwrap();

    let result = ctx
        .sql("INSERT INTO ducklake.main.mixed_null_table (id, value) SELECT * FROM mixed_source")
        .await;

    match result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(
                exec.is_err(),
                "INSERT with mix of NULL and non-NULL in NOT NULL column should fail"
            );
            let err_msg = exec.unwrap_err().to_string();
            assert!(
                err_msg.contains("NOT NULL") || err_msg.contains("null"),
                "Error should mention NOT NULL constraint, got: {}",
                err_msg
            );
        },
        Err(e) => {
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("NOT NULL") || err_msg.contains("null"),
                "Error should mention NOT NULL constraint, got: {}",
                err_msg
            );
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_not_null_error_mentions_column_name() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create table with specific NOT NULL column names
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("required_field", DataType::Utf8, false), // NOT NULL
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![1])), Arc::new(StringArray::from(vec!["present"]))],
    )
    .unwrap();

    create_table_with_data(
        &temp_dir,
        "main",
        "named_constraint_table",
        &schema,
        vec![batch],
    )
    .await;

    // Insert with NULL in 'required_field'
    let ctx = create_writable_ctx(&temp_dir).await;

    let insert_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("required_field", DataType::Utf8, true),
    ]));

    let null_batch = RecordBatch::try_new(
        insert_schema,
        vec![Arc::new(Int32Array::from(vec![2])), Arc::new(StringArray::from(vec![None::<&str>]))],
    )
    .unwrap();

    ctx.register_batch("named_source", null_batch).unwrap();

    let result = ctx
        .sql("INSERT INTO ducklake.main.named_constraint_table (id, required_field) SELECT * FROM named_source")
        .await
        .unwrap()
        .collect()
        .await;

    assert!(result.is_err(), "Should fail on NOT NULL violation");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("required_field"),
        "Error should mention the specific column name 'required_field', got: {}",
        err_msg
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_not_null_constraint_preserved_after_insert() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a table with NOT NULL columns using the writer API
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),    // NOT NULL
        Field::new("status", DataType::Utf8, false), // NOT NULL
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["active", "inactive"])),
        ],
    )
    .unwrap();

    create_table_with_data(&temp_dir, "main", "status_table", &schema, vec![batch]).await;

    // First, do a valid INSERT to evolve the schema
    let ctx = create_writable_ctx(&temp_dir).await;
    let valid_batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(vec![3])), Arc::new(StringArray::from(vec!["pending"]))],
    )
    .unwrap();
    ctx.register_batch("valid_insert", valid_batch).unwrap();
    ctx.sql("INSERT INTO ducklake.main.status_table (id, status) SELECT * FROM valid_insert")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Now try to insert with NULL in the NOT NULL column
    let ctx2 = create_writable_ctx(&temp_dir).await;

    let null_source_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("status", DataType::Utf8, true), // nullable in source
    ]));

    let null_source = RecordBatch::try_new(
        null_source_schema,
        vec![Arc::new(Int32Array::from(vec![4])), Arc::new(StringArray::from(vec![None::<&str>]))],
    )
    .unwrap();

    ctx2.register_batch("null_status", null_source).unwrap();

    let result = ctx2
        .sql("INSERT INTO ducklake.main.status_table (id, status) SELECT * FROM null_status")
        .await;

    match result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(
                exec.is_err(),
                "INSERT with NULL in NOT NULL column should fail even after prior inserts"
            );
        },
        Err(_) => {
            // Planning-time failure is acceptable
        },
    }
}

// =============================================================================
// Combined scenario tests
// =============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_then_query_schema_still_works() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create two tables in "combo_schema"
    create_table_with_data(
        &temp_dir,
        "combo_schema",
        "table_a",
        &test_schema(),
        vec![test_batch(vec![1], vec!["a"])],
    )
    .await;
    create_table_with_data(
        &temp_dir,
        "combo_schema",
        "table_b",
        &test_schema(),
        vec![test_batch(vec![2], vec!["b"])],
    )
    .await;

    // Drop only table_a
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("DROP TABLE ducklake.combo_schema.table_a")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Schema should still work, table_b should be queryable
    let read_ctx = create_read_ctx(&temp_dir).await;
    let catalog = read_ctx.catalog("ducklake").unwrap();
    let schema = catalog.schema("combo_schema");
    assert!(
        schema.is_some(),
        "Schema should still exist after dropping one table"
    );

    let result = read_ctx
        .sql("SELECT name FROM ducklake.combo_schema.table_b")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let names = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "b");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_table_data_files_preserved() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create a table with data
    create_table_with_data(
        &temp_dir,
        "main",
        "preserved_data",
        &test_schema(),
        vec![test_batch(vec![1, 2], vec!["keep1", "keep2"])],
    )
    .await;

    // Find data files before drop
    let data_dir = temp_dir
        .path()
        .join("data")
        .join("main")
        .join("preserved_data");
    let files_before: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    assert!(
        !files_before.is_empty(),
        "Should have data files before drop"
    );

    // Drop the table
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("DROP TABLE ducklake.main.preserved_data")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify data files still exist on disk (preserved for time travel)
    let files_after: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    assert_eq!(
        files_before.len(),
        files_after.len(),
        "Data files should be preserved after DROP TABLE (time travel)"
    );
}
