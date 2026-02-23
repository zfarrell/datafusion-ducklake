//! Deep edge case tests to find additional bugs.
//!
//! Probes areas NOT covered by existing tests: rename-to-same-name,
//! drop+re-add column, view on dropped table, decimal edge cases,
//! empty schema queries, type mapping gaps, etc.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::metadata_writer::{
    AlterColumnTypeOp, AlterTableOp, ColumnDef, MetadataWriter, WriteMode,
};
use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeQueryPlanner, DuckLakeTableWriter, MetadataProvider,
    SqliteMetadataProvider, SqliteMetadataWriter,
};

// ============================================================================
// Common helpers (copied pattern from existing tests)
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

async fn create_dml_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(DuckLakeQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

async fn create_writable_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());

    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
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
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

async fn query_count(ctx: &SessionContext, table: &str) -> i64 {
    let df = ctx
        .sql(&format!("SELECT COUNT(*) FROM ducklake.main.{}", table))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn query_ids(ctx: &SessionContext, table: &str) -> Vec<i32> {
    let df = ctx
        .sql(&format!(
            "SELECT id FROM ducklake.main.{} ORDER BY id",
            table
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    ids
}

async fn get_writer(temp_dir: &TempDir) -> SqliteMetadataWriter {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    SqliteMetadataWriter::new(&conn_str).await.unwrap()
}

fn get_table_id(writer: &SqliteMetadataWriter) -> i64 {
    let setup = writer
        .begin_write_transaction(
            "main",
            "test_tbl",
            &[
                ColumnDef::new("id", "int32", false),
                ColumnDef::new("name", "varchar", true),
            ],
            WriteMode::Replace,
        )
        .unwrap();
    setup.table_id
}

// ============================================================================
// 1. ALTER TABLE: Rename column to same name (should it succeed or fail?)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_rename_column_to_same_name() {
    // BUG PROBE: Renaming a column to its own name should be rejected
    // because validate_rename_column checks if new_name already exists
    // in columns - and old_name IS in columns.
    let writer = {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
        let w = SqliteMetadataWriter::new_with_init(&conn_str).await.unwrap();
        w.set_data_path(temp_dir.path().to_str().unwrap()).unwrap();

        let columns = vec![
            ColumnDef::new("id", "int32", false),
            ColumnDef::new("name", "varchar", true),
        ];
        let setup = w
            .begin_write_transaction("main", "t", &columns, WriteMode::Replace)
            .unwrap();

        let result = w.alter_table(
            setup.table_id,
            &AlterTableOp::RenameColumn {
                old_name: "name".to_string(),
                new_name: "name".to_string(), // Same name!
            },
        );

        // This should fail with "already exists" because "name" is in the column list.
        // If it silently succeeds, that's a semantic no-op but creates unnecessary
        // snapshot churn. Verify the behavior.
        assert!(
            result.is_err(),
            "BUG: Renaming a column to its own name should fail (it creates pointless snapshot churn), but it succeeded"
        );
    };
}

// ============================================================================
// 2. ALTER TABLE: Drop + re-add same column name
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_and_readd_same_column_name() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let w = SqliteMetadataWriter::new_with_init(&conn_str).await.unwrap();
    w.set_data_path(temp_dir.path().to_str().unwrap()).unwrap();

    let columns = vec![
        ColumnDef::new("id", "int32", false),
        ColumnDef::new("name", "varchar", true),
        ColumnDef::new("email", "varchar", true),
    ];
    let setup = w
        .begin_write_transaction("main", "t", &columns, WriteMode::Replace)
        .unwrap();

    // Drop "email"
    w.alter_table(
        setup.table_id,
        &AlterTableOp::DropColumn {
            column_name: "email".to_string(),
        },
    )
    .unwrap();

    // Re-add "email" with different type
    let result = w.alter_table(
        setup.table_id,
        &AlterTableOp::AddColumn {
            column: ColumnDef::new("email", "int64", true),
        },
    );

    // This should succeed - the old "email" is ended, so new one should be fine
    assert!(
        result.is_ok(),
        "BUG: Dropping and re-adding a column with the same name should work, but failed: {:?}",
        result.err()
    );

    let columns = w.get_active_columns(setup.table_id).unwrap();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[2].0, "email");
    assert_eq!(columns[2].1, "int64"); // New type
}

// ============================================================================
// 3. ALTER TABLE: Alter type to same type (no-op)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_column_type_to_same_type() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let w = SqliteMetadataWriter::new_with_init(&conn_str).await.unwrap();
    w.set_data_path(temp_dir.path().to_str().unwrap()).unwrap();

    let columns = vec![ColumnDef::new("value", "int32", true)];
    let setup = w
        .begin_write_transaction("main", "t", &columns, WriteMode::Replace)
        .unwrap();

    // int32 → int32 (same type)
    let result = w.alter_table(
        setup.table_id,
        &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "value".to_string(),
            new_type: "int32".to_string(),
        }),
    );

    // is_type_promotion_allowed("int32", "int32") returns false,
    // so this should fail. Document the behavior.
    assert!(
        result.is_err(),
        "Altering column type to the same type should be rejected by is_type_promotion_allowed"
    );
}

// ============================================================================
// 4. Type mapping edge cases
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_type_decimal_max_precision() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // Test Decimal128 max precision (38)
    let result = ducklake_to_arrow_type("decimal(38, 18)");
    assert!(result.is_ok(), "decimal(38,18) should parse: {:?}", result);
    assert_eq!(result.unwrap(), DataType::Decimal128(38, 18));

    // Test Decimal256 (precision > 38)
    let result = ducklake_to_arrow_type("decimal(39, 10)");
    assert!(result.is_ok(), "decimal(39,10) should use Decimal256: {:?}", result);
    assert_eq!(result.unwrap(), DataType::Decimal256(39, 10));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_decimal_zero_precision() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // decimal(0, 0) - zero precision
    let result = ducklake_to_arrow_type("decimal(0, 0)");
    // This should parse but may be invalid for Arrow (precision must be > 0 for Decimal128)
    // Let's just verify it doesn't panic
    if result.is_ok() {
        let dt = result.unwrap();
        // Decimal128 with precision 0 is technically invalid in Arrow
        match dt {
            DataType::Decimal128(p, _) => {
                // Arrow will likely reject this later, but we at least don't panic
                assert_eq!(p, 0);
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_decimal_negative_scale() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // Negative scale - e.g., decimal(5, -2) means "round to nearest 100"
    let result = ducklake_to_arrow_type("decimal(5, -2)");
    // parse_decimal uses i8 for scale, so negative should parse
    if result.is_ok() {
        let dt = result.unwrap();
        match dt {
            DataType::Decimal128(5, -2) => {} // Good
            other => panic!("Expected Decimal128(5, -2), got {:?}", other),
        }
    }
    // Either way: no panic
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_decimal_without_parens() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // "decimal" without parentheses - should NOT match parse_decimal
    // and should fall through to the _ match arm
    let result = ducklake_to_arrow_type("decimal");
    // This is unparameterized decimal - should it default to something or error?
    assert!(
        result.is_err(),
        "BUG? 'decimal' without parameters should error but got: {:?}",
        result
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_numeric_alias() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // "numeric" is an alias for "decimal" in many SQL databases
    let result = ducklake_to_arrow_type("numeric(10, 2)");
    assert!(result.is_ok(), "numeric(10,2) should parse like decimal: {:?}", result);
    assert_eq!(result.unwrap(), DataType::Decimal128(10, 2));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_hugeint() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // DuckDB has HUGEINT (128-bit integer) - is it mapped?
    let result = ducklake_to_arrow_type("hugeint");
    // This is probably UnsupportedType
    if result.is_err() {
        // Expected - document that hugeint is not supported
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_varchar_with_length() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // VARCHAR(255) - DuckDB ignores the length but it appears in type strings
    let result = ducklake_to_arrow_type("varchar(255)");
    // This may fail because parse_decimal is tried first (starts with "v", so no),
    // and then "varchar(255)" won't match "varchar" exactly.
    // It may fall through to parse_complex_type which won't match either.
    if result.is_err() {
        // BUG: VARCHAR(N) from DuckDB catalogs will fail type parsing!
        // DuckDB stores "VARCHAR" but some catalogs store "VARCHAR(255)"
        eprintln!("FINDING: VARCHAR(N) with length specifier is not handled: {:?}", result.err());
    }
}

// ============================================================================
// 5. View referencing dropped table
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_view_on_dropped_table() {
    let (writer, temp_dir) = create_test_env().await;

    // Create table with data
    let batch = make_batch(vec![1, 2], vec!["a", "b"]);
    write_table(writer.clone(), "main", "base_tbl", &[batch]).await;

    // Create view referencing base_tbl
    let schema_id = {
        let db_path = temp_dir.path().join("test.db");
        let conn_str = format!("sqlite:{}", db_path.display());
        let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
        let snapshot = provider.get_current_snapshot().unwrap();
        provider
            .get_schema_by_name("main", snapshot)
            .unwrap()
            .unwrap()
            .schema_id
    };
    {
        let w = get_writer(&temp_dir).await;
        w.create_view(schema_id, "my_view", "SELECT id, name FROM base_tbl")
            .unwrap();
    }

    // Verify view works
    let ctx1 = create_read_ctx(&temp_dir).await;
    let count = query_count(&ctx1, "my_view").await;
    assert_eq!(count, 2);

    // Drop the underlying table
    {
        let ctx = create_writable_ctx(&temp_dir).await;
        ctx.sql("DROP TABLE ducklake.main.base_tbl")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    // Now query the view - it should fail gracefully, not panic
    let ctx2 = create_read_ctx(&temp_dir).await;
    let result = ctx2.sql("SELECT * FROM ducklake.main.my_view").await;

    // The view should either fail at plan time or execution time, but not panic
    match result {
        Ok(df) => {
            let exec_result = df.collect().await;
            // If planning succeeded, execution should fail
            if exec_result.is_err() {
                // Good - graceful failure
            } else {
                // If it returns data, that's unexpected (zombie view)
                let batches = exec_result.unwrap();
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                // This would be a bug if the dropped table's data is still visible
                eprintln!(
                    "FINDING: View on dropped table returned {} rows (expected error)",
                    total_rows
                );
            }
        }
        Err(_) => {
            // Good - planning correctly failed
        }
    }
}

// ============================================================================
// 6. Empty schema operations
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_schema_list_tables() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create an empty schema
    let ctx = create_writable_ctx(&temp_dir).await;
    ctx.sql("CREATE SCHEMA ducklake.empty_schema")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // List tables in empty schema
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema = catalog.schema("empty_schema").unwrap();
    let table_names = schema.table_names();
    assert!(table_names.is_empty(), "Empty schema should have no tables");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_all_tables_in_schema() {
    let (writer, temp_dir) = create_test_env().await;

    // Create two tables
    let batch1 = make_batch(vec![1], vec!["a"]);
    let batch2 = make_batch(vec![2], vec!["b"]);
    write_table(writer.clone(), "main", "t1", &[batch1]).await;
    {
        let w = Arc::new(get_writer(&temp_dir).await);
        write_table(w, "main", "t2", &[batch2]).await;
    }

    // Verify both exist
    let ctx1 = create_read_ctx(&temp_dir).await;
    let catalog = ctx1.catalog("ducklake").unwrap();
    let schema = catalog.schema("main").unwrap();
    assert!(schema.table_names().len() >= 2);

    // Drop both tables
    let ctx2 = create_writable_ctx(&temp_dir).await;
    ctx2.sql("DROP TABLE ducklake.main.t1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let ctx3 = create_writable_ctx(&temp_dir).await;
    ctx3.sql("DROP TABLE ducklake.main.t2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Schema should still exist but with no tables
    let ctx4 = create_read_ctx(&temp_dir).await;
    let catalog4 = ctx4.catalog("ducklake").unwrap();
    let schema4 = catalog4.schema("main").unwrap();
    let names = schema4.table_names();
    assert!(
        names.is_empty(),
        "Schema should have no tables after dropping all, got: {:?}",
        names
    );
}

// ============================================================================
// 7. Schema name edge cases
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_schema_name_with_spaces() {
    let (_writer, temp_dir) = create_test_env().await;

    // Create schema with spaces in name
    let ctx = create_writable_ctx(&temp_dir).await;
    let result = ctx
        .sql("CREATE SCHEMA ducklake.\"my schema\"")
        .await;

    // DataFusion may or may not support quoted identifiers here
    // Just verify no panic
    match result {
        Ok(df) => {
            let _ = df.collect().await;
        }
        Err(e) => {
            // Expected if DataFusion doesn't support this syntax
            eprintln!("Schema with spaces not supported: {}", e);
        }
    }
}

// ============================================================================
// 8. Multiple appends then delete
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_appends_then_delete_from_first_file() {
    let (writer, temp_dir) = create_test_env().await;

    // Write initial data (file 1)
    let batch1 = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_table(writer.clone(), "main", "multi_append", &[batch1]).await;

    // Append more data (file 2)
    {
        let w = Arc::new(get_writer(&temp_dir).await);
        let batch2 = make_batch(vec![4, 5, 6], vec!["d", "e", "f"]);
        append_table(w, "main", "multi_append", &[batch2]).await;
    }

    // Append even more (file 3)
    {
        let w = Arc::new(get_writer(&temp_dir).await);
        let batch3 = make_batch(vec![7, 8, 9], vec!["g", "h", "i"]);
        append_table(w, "main", "multi_append", &[batch3]).await;
    }

    // Verify all 9 rows
    let ctx = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&ctx, "multi_append").await, 9);

    // Delete from the FIRST file (id=2)
    let dml_ctx = create_dml_ctx(&temp_dir).await;
    let df = dml_ctx
        .sql("DELETE FROM ducklake.main.multi_append WHERE id = 2")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 1);

    // Verify 8 rows remain
    let read_ctx = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&read_ctx, "multi_append").await;
    assert_eq!(ids, vec![1, 3, 4, 5, 6, 7, 8, 9]);
}

// ============================================================================
// 9. Write then overwrite (Replace mode)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_write_then_replace() {
    let (writer, temp_dir) = create_test_env().await;

    // Write initial data
    let batch1 = make_batch(vec![1, 2, 3], vec!["old1", "old2", "old3"]);
    write_table(writer.clone(), "main", "replace_tbl", &[batch1]).await;

    // Verify initial data
    let ctx1 = create_read_ctx(&temp_dir).await;
    assert_eq!(query_count(&ctx1, "replace_tbl").await, 3);

    // Replace with new data (fewer rows, different values)
    {
        let w = Arc::new(get_writer(&temp_dir).await);
        let batch2 = make_batch(vec![10, 20], vec!["new1", "new2"]);
        write_table(w, "main", "replace_tbl", &[batch2]).await;
    }

    // Verify only new data
    let ctx2 = create_read_ctx(&temp_dir).await;
    let ids = query_ids(&ctx2, "replace_tbl").await;
    assert_eq!(ids, vec![10, 20]);
    assert_eq!(query_count(&ctx2, "replace_tbl").await, 2);
}

// ============================================================================
// 10. Write empty batch (0 rows)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_write_zero_row_batch() {
    let (writer, temp_dir) = create_test_env().await;

    // Create a batch with 0 rows
    let schema = id_name_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .unwrap();

    assert_eq!(batch.num_rows(), 0);

    // Write it - should this succeed or fail?
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    let result = table_writer
        .write_table("main", "empty_batch_tbl", &[batch])
        .await;

    // It will write a 0-row parquet file. That should work fine.
    if result.is_ok() {
        let ctx = create_read_ctx(&temp_dir).await;
        let count = query_count(&ctx, "empty_batch_tbl").await;
        assert_eq!(count, 0, "0-row batch should result in 0 rows");
    }
    // If it errors, that's also acceptable behavior
}

// ============================================================================
// 11. Column with very long name
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_column_with_very_long_name() {
    let (writer, temp_dir) = create_test_env().await;

    let long_name = "x".repeat(500);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(&long_name, DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("value")])),
        ],
    )
    .unwrap();

    write_table(writer.clone(), "main", "long_col", &[batch]).await;

    // Read back
    let ctx = create_read_ctx(&temp_dir).await;
    let count = query_count(&ctx, "long_col").await;
    assert_eq!(count, 1);

    // Verify we can select the long-named column
    let df = ctx
        .sql(&format!(
            "SELECT \"{}\" FROM ducklake.main.long_col",
            long_name
        ))
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

// ============================================================================
// 12. Table name with special characters (quoted identifiers)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_table_name_with_hyphens() {
    let (writer, temp_dir) = create_test_env().await;

    // Table name with hyphens (needs quoting)
    let batch = make_batch(vec![1], vec!["a"]);
    write_table(writer.clone(), "main", "my-table", &[batch]).await;

    let ctx = create_read_ctx(&temp_dir).await;
    // Need to use quoted identifier
    let df = ctx
        .sql("SELECT id FROM ducklake.main.\"my-table\"")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
}

// ============================================================================
// 13. Concurrent snapshot creation
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_ids_always_increase() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let w = SqliteMetadataWriter::new_with_init(&conn_str).await.unwrap();
    w.set_data_path(temp_dir.path().to_str().unwrap()).unwrap();

    let mut prev_snapshot = 0i64;
    for _ in 0..10 {
        let snapshot = w.create_snapshot().unwrap();
        assert!(
            snapshot > prev_snapshot,
            "Snapshot IDs must be strictly increasing: got {} after {}",
            snapshot,
            prev_snapshot
        );
        prev_snapshot = snapshot;
    }
}

// ============================================================================
// 14. Append with schema evolution: add new nullable column
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_append_with_extra_nullable_column() {
    let (writer, temp_dir) = create_test_env().await;

    // Write initial data with 2 columns
    let batch1 = make_batch(vec![1, 2], vec!["a", "b"]);
    write_table(writer.clone(), "main", "evolve_tbl", &[batch1]).await;

    // Append with 3 columns (new nullable column)
    let schema3 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
    ]));
    let batch2 = RecordBatch::try_new(
        schema3,
        vec![
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(StringArray::from(vec![Some("c")])),
            Arc::new(StringArray::from(vec![Some("c@test.com")])),
        ],
    )
    .unwrap();

    {
        let w = Arc::new(get_writer(&temp_dir).await);
        append_table(w, "main", "evolve_tbl", &[batch2]).await;
    }

    // Read back - should have 3 rows, with NULLs for old rows' email
    let ctx = create_read_ctx(&temp_dir).await;
    let count = query_count(&ctx, "evolve_tbl").await;
    assert_eq!(count, 3, "Should have 3 rows after append");
}

// ============================================================================
// 15. Append with type mismatch (should fail)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_append_with_type_mismatch_fails() {
    let (writer, temp_dir) = create_test_env().await;

    // Write initial data: id (Int32), name (Utf8)
    let batch1 = make_batch(vec![1], vec!["a"]);
    write_table(writer.clone(), "main", "mismatch_tbl", &[batch1]).await;

    // Try to append with id as Int64 instead of Int32
    let schema_wrong = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false), // Wrong type!
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch2 = RecordBatch::try_new(
        schema_wrong,
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec![Some("b")])),
        ],
    )
    .unwrap();

    let w = Arc::new(get_writer(&temp_dir).await);
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(w, object_store).unwrap();
    let result = table_writer
        .append_table("main", "mismatch_tbl", &[batch2])
        .await;

    assert!(
        result.is_err(),
        "BUG: Appending with mismatched column types should fail, but succeeded"
    );
}

// ============================================================================
// 16. Append with non-nullable new column (should fail validation)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_append_with_non_nullable_new_column_fails() {
    let (writer, temp_dir) = create_test_env().await;

    // Write initial data: id (Int32), name (Utf8)
    let batch1 = make_batch(vec![1], vec!["a"]);
    write_table(writer.clone(), "main", "nonnull_tbl", &[batch1]).await;

    // Try to append with a NEW NON-NULLABLE column
    let schema_bad = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("required_col", DataType::Utf8, false), // Non-nullable new column!
    ]));
    let batch2 = RecordBatch::try_new(
        schema_bad,
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec![Some("b")])),
            Arc::new(StringArray::from(vec![Some("required")])),
        ],
    )
    .unwrap();

    let w = Arc::new(get_writer(&temp_dir).await);
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(w, object_store).unwrap();
    let result = table_writer
        .append_table("main", "nonnull_tbl", &[batch2])
        .await;

    assert!(
        result.is_err(),
        "Appending with non-nullable new column should fail validation"
    );
}

// ============================================================================
// 17. Type roundtrip edge cases
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_type_roundtrip_interval() {
    use datafusion_ducklake::types::{arrow_to_ducklake_type, ducklake_to_arrow_type};
    use arrow::datatypes::IntervalUnit;

    let arrow_type = DataType::Interval(IntervalUnit::MonthDayNano);
    let ducklake = arrow_to_ducklake_type(&arrow_type).unwrap();
    assert_eq!(ducklake, "interval");
    let back = ducklake_to_arrow_type(&ducklake).unwrap();
    assert_eq!(back, arrow_type, "Interval roundtrip failed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_roundtrip_uuid() {
    use datafusion_ducklake::types::{arrow_to_ducklake_type, ducklake_to_arrow_type};

    let arrow_type = DataType::FixedSizeBinary(16);
    let ducklake = arrow_to_ducklake_type(&arrow_type).unwrap();
    assert_eq!(ducklake, "uuid");
    let back = ducklake_to_arrow_type(&ducklake).unwrap();
    assert_eq!(back, arrow_type, "UUID roundtrip failed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_type_json_roundtrip_lossy() {
    use datafusion_ducklake::types::{arrow_to_ducklake_type, ducklake_to_arrow_type};

    // JSON maps to Utf8 going in, but Utf8 maps to "varchar" going out
    // So json → Utf8 → varchar → Utf8 (lossy: we lose the "json" type info)
    let json_arrow = ducklake_to_arrow_type("json").unwrap();
    assert_eq!(json_arrow, DataType::Utf8);

    let back = arrow_to_ducklake_type(&json_arrow).unwrap();
    assert_eq!(back, "varchar", "JSON type is lost in roundtrip (expected behavior)");
}

// ============================================================================
// 18. DELETE from already-empty table (no data files)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_from_table_with_no_data_files() {
    let (writer, temp_dir) = create_test_env().await;

    // Create table with data then delete all
    let batch = make_batch(vec![1], vec!["a"]);
    write_table(writer.clone(), "main", "empty_del", &[batch]).await;

    let ctx1 = create_dml_ctx(&temp_dir).await;
    let df = ctx1
        .sql("DELETE FROM ducklake.main.empty_del")
        .await
        .unwrap();
    df.collect().await.unwrap();

    // Now try to delete again from the "empty" table
    let ctx2 = create_dml_ctx(&temp_dir).await;
    let result = ctx2
        .sql("DELETE FROM ducklake.main.empty_del WHERE id = 999")
        .await;

    match result {
        Ok(df) => {
            let batches = df.collect().await.unwrap();
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap()
                .value(0);
            assert_eq!(count, 0, "Deleting from empty table should delete 0 rows");
        }
        Err(e) => {
            eprintln!("FINDING: DELETE from already-empty table failed: {}", e);
        }
    }
}

// ============================================================================
// 19. UPDATE on already-empty table
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_update_on_empty_table() {
    let (writer, temp_dir) = create_test_env().await;

    // Create table with data then delete all
    let batch = make_batch(vec![1], vec!["a"]);
    write_table(writer.clone(), "main", "empty_upd", &[batch]).await;

    let ctx1 = create_dml_ctx(&temp_dir).await;
    ctx1.sql("DELETE FROM ducklake.main.empty_upd")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Try to update the empty table
    let ctx2 = create_dml_ctx(&temp_dir).await;
    let result = ctx2
        .sql("UPDATE ducklake.main.empty_upd SET name = 'x'")
        .await;

    match result {
        Ok(df) => {
            let batches = df.collect().await.unwrap();
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap()
                .value(0);
            assert_eq!(count, 0, "Updating empty table should update 0 rows");
        }
        Err(e) => {
            eprintln!("FINDING: UPDATE on empty table failed: {}", e);
        }
    }
}

// ============================================================================
// 20. Struct field names with spaces
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_struct_field_names_with_spaces() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // Struct field name with space - "first name" not "first_name"
    let result = ducklake_to_arrow_type("struct(\"first name\" varchar, age int32)");
    // The parser splits on space after removing quotes... this may fail
    if result.is_err() {
        eprintln!("FINDING: Struct field names with spaces are not supported: {:?}", result.err());
    }
}

// ============================================================================
// 21. parse_complex_type with malformed input
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_malformed_complex_types_no_panic() {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // These should all error gracefully, never panic
    let malformed = vec![
        "list(",          // unclosed paren
        "list<",          // unclosed angle bracket
        "struct()",       // empty struct
        "struct(,)",      // empty field
        "map(varchar)",   // only one type param
        "map(,,)",        // too many params
        "list<>",         // empty list type
        "struct(a)",      // field without type
        "list(list(list(list(list(list(list(int32)))))))",  // deeply nested
    ];

    for input in malformed {
        let result = ducklake_to_arrow_type(input);
        // Just verify it doesn't panic - error is expected
        match result {
            Ok(dt) => {
                // Some of these may actually parse (like deeply nested)
                eprintln!("Unexpectedly parsed '{}': {:?}", input, dt);
            }
            Err(_) => {
                // Expected
            }
        }
    }
}

// ============================================================================
// 22. Table with duplicate column names in write
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_with_duplicate_column_names() {
    let (writer, temp_dir) = create_test_env().await;

    // Arrow schema with duplicate column names
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("id", DataType::Int32, false), // Duplicate!
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![2])),
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    let result = table_writer
        .write_table("main", "dup_cols", &[batch])
        .await;

    // This should ideally fail, but may silently create ambiguous schema
    match result {
        Ok(_) => {
            eprintln!("FINDING: Duplicate column names in write succeeded - may cause ambiguity");
            // Try to read it back
            let ctx = create_read_ctx(&temp_dir).await;
            let result = ctx.sql("SELECT * FROM ducklake.main.dup_cols").await;
            match result {
                Ok(df) => {
                    let exec_result = df.collect().await;
                    match exec_result {
                        Ok(batches) => {
                            eprintln!("  Read back {} rows with duplicate column names",
                                batches.iter().map(|b| b.num_rows()).sum::<usize>());
                        }
                        Err(e) => {
                            eprintln!("  Read-back execution failed: {}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  Read-back planning failed: {}", e);
                }
            }
        }
        Err(_) => {
            // Good - rejected duplicate columns
        }
    }
}

// ============================================================================
// 23. Write with no batches
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_write_table_no_batches() {
    let (writer, _temp_dir) = create_test_env().await;

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    let result = table_writer.write_table("main", "no_data", &[]).await;

    assert!(
        result.is_err(),
        "Writing with no batches should fail"
    );
}

// ============================================================================
// 24. Metadata: get_data_path without setting it
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_get_data_path_before_setting() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let w = SqliteMetadataWriter::new_with_init(&conn_str).await.unwrap();
    // Don't call set_data_path

    let result = w.get_data_path();
    assert!(
        result.is_err(),
        "get_data_path without setting should fail with clear error"
    );
    if let Err(e) = result {
        assert!(
            e.to_string().contains("data_path"),
            "Error should mention 'data_path': {}",
            e
        );
    }
}
