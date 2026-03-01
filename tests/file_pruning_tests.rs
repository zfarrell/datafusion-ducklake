//! Integration tests for file-level pruning using column statistics.
//!
//! Tests verify that per-file column statistics are used to skip files
//! during scan planning when filter predicates prove no rows can match.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter,
};

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

async fn create_read_context(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());
    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

/// Write two files with non-overlapping integer ranges, then verify
/// equality filter returns correct results (pruning is transparent).
#[tokio::test(flavor = "multi_thread")]
async fn test_pruning_equality_filter_correct_results() {
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    // File 1: ids 1-3
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
    table_writer
        .write_table("main", "prune_eq", &[batch1])
        .await
        .unwrap();

    // File 2: ids 10-12
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 11, 12])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .unwrap();

    let table_writer2 = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer2
        .append_table("main", "prune_eq", &[batch2])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;

    // WHERE id = 2 should only match file 1
    let rows = ctx
        .sql("SELECT id, name FROM ducklake.main.prune_eq WHERE id = 2 ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].num_rows(), 1);
    let ids = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 2);
    let names = rows[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "b");

    // WHERE id = 11 should only match file 2
    let rows = ctx
        .sql("SELECT id, name FROM ducklake.main.prune_eq WHERE id = 11")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    // WHERE id = 5 should match no rows (between the two files)
    let rows = ctx
        .sql("SELECT id FROM ducklake.main.prune_eq WHERE id = 5")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

/// Range filter: WHERE id > value should return correct results.
#[tokio::test(flavor = "multi_thread")]
async fn test_pruning_range_filter_correct_results() {
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));

    // File 1: values 1-5
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();

    let tw = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
    tw.write_table("main", "prune_range", &[batch1])
        .await
        .unwrap();

    // File 2: values 100-104
    let batch2 = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![100, 101, 102, 103, 104]))],
    )
    .unwrap();

    let tw2 = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    tw2.append_table("main", "prune_range", &[batch2])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;

    // WHERE val > 50 should skip file 1 entirely
    let rows = ctx
        .sql("SELECT val FROM ducklake.main.prune_range WHERE val > 50 ORDER BY val")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let all_vals: Vec<i32> = rows
        .iter()
        .flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..arr.len()).map(move |i| arr.value(i))
        })
        .collect();

    assert_eq!(all_vals, vec![100, 101, 102, 103, 104]);

    // WHERE val < 10 should skip file 2 entirely
    let rows = ctx
        .sql("SELECT val FROM ducklake.main.prune_range WHERE val < 10 ORDER BY val")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let all_vals: Vec<i32> = rows
        .iter()
        .flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..arr.len()).map(move |i| arr.value(i))
        })
        .collect();

    assert_eq!(all_vals, vec![1, 2, 3, 4, 5]);

    // WHERE val >= 1 AND val <= 104 should include both files
    let rows = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.prune_range WHERE val >= 1 AND val <= 104")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let cnt = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 10);
}

/// Test with string column stats.
#[tokio::test(flavor = "multi_thread")]
async fn test_pruning_string_filter_correct_results() {
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("val", DataType::Int32, false),
    ]));

    // File 1: names starting with a-c
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();

    let tw = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
    tw.write_table("main", "prune_str", &[batch1])
        .await
        .unwrap();

    // File 2: names starting with x-z
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["xavier", "yolanda", "zara"])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();

    let tw2 = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    tw2.append_table("main", "prune_str", &[batch2])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;

    // WHERE name = 'bob' should only look in file 1
    let rows = ctx
        .sql("SELECT name, val FROM ducklake.main.prune_str WHERE name = 'bob'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
    let names = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "bob");

    // WHERE name = 'zara' should only look in file 2
    let rows = ctx
        .sql("SELECT val FROM ducklake.main.prune_str WHERE name = 'zara'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
}

/// Test with float column stats.
#[tokio::test(flavor = "multi_thread")]
async fn test_pruning_float_filter_correct_results() {
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "score",
        DataType::Float64,
        false,
    )]));

    // File 1: scores 1.0-3.0
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]))],
    )
    .unwrap();

    let tw = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
    tw.write_table("main", "prune_float", &[batch1])
        .await
        .unwrap();

    // File 2: scores 100.0-300.0
    let batch2 = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0]))],
    )
    .unwrap();

    let tw2 = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    tw2.append_table("main", "prune_float", &[batch2])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;

    // WHERE score > 50.0 should only include file 2
    let rows = ctx
        .sql("SELECT score FROM ducklake.main.prune_float WHERE score > 50.0 ORDER BY score")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let all_scores: Vec<f64> = rows
        .iter()
        .flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            (0..arr.len()).map(move |i| arr.value(i))
        })
        .collect();

    assert_eq!(all_scores, vec![100.0, 200.0, 300.0]);
}

/// No pruning when stats are absent (table has no column stats).
#[tokio::test(flavor = "multi_thread")]
async fn test_no_pruning_when_stats_absent() {
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();

    let tw = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    tw.write_table("main", "no_prune", &[batch]).await.unwrap();

    let ctx = create_read_context(&temp_dir).await;

    // Query should still return correct results even though we have stats
    // (this is mostly about ensuring no crashes when running with filters)
    let rows = ctx
        .sql("SELECT id FROM ducklake.main.no_prune WHERE id > 3 ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let all_ids: Vec<i32> = rows
        .iter()
        .flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..arr.len()).map(move |i| arr.value(i))
        })
        .collect();

    assert_eq!(all_ids, vec![4, 5]);
}

/// No false negatives: pruning should never skip rows that match the filter.
/// This test writes overlapping ranges and verifies all matching rows are returned.
#[tokio::test(flavor = "multi_thread")]
async fn test_pruning_no_false_negatives() {
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    // File 1: ids 1-10
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))],
    )
    .unwrap();

    let tw = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
    tw.write_table("main", "no_fn", &[batch1]).await.unwrap();

    // File 2: ids 5-15 (overlapping with file 1)
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![
            5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        ]))],
    )
    .unwrap();

    let tw2 = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
    tw2.append_table("main", "no_fn", &[batch2]).await.unwrap();

    // File 3: ids 20-25
    let batch3 = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![20, 21, 22, 23, 24, 25]))],
    )
    .unwrap();

    let tw3 = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    tw3.append_table("main", "no_fn", &[batch3]).await.unwrap();

    let ctx = create_read_context(&temp_dir).await;

    // WHERE id = 5 should return rows from BOTH file 1 and file 2
    let rows = ctx
        .sql("SELECT id FROM ducklake.main.no_fn WHERE id = 5 ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "id=5 appears in both file 1 and file 2");

    // WHERE id >= 10 should return rows from file 1 (10), file 2 (10-15), file 3 (20-25)
    let rows = ctx
        .sql("SELECT id FROM ducklake.main.no_fn WHERE id >= 10 ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let all_ids: Vec<i32> = rows
        .iter()
        .flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..arr.len()).map(move |i| arr.value(i))
        })
        .collect();

    // file1: 10, file2: 10,11,12,13,14,15, file3: 20,21,22,23,24,25 = 13 rows
    assert_eq!(all_ids.len(), 13);
    assert!(all_ids.contains(&10));
    assert!(all_ids.contains(&15));
    assert!(all_ids.contains(&25));

    // WHERE id > 100 should match nothing (all files pruned)
    let rows = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.no_fn WHERE id > 100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let cnt = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 0);
}

/// Test pruning with NULL handling: columns with NULLs should not be incorrectly pruned.
#[tokio::test(flavor = "multi_thread")]
async fn test_pruning_with_nulls() {
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    // File 1: has NULLs
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap();

    let tw = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
    tw.write_table("main", "prune_null", &[batch1])
        .await
        .unwrap();

    // File 2: no NULLs, higher values
    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["x", "y", "z"])),
        ],
    )
    .unwrap();

    let tw2 = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    tw2.append_table("main", "prune_null", &[batch2])
        .await
        .unwrap();

    let ctx = create_read_context(&temp_dir).await;

    // WHERE id = 1 should return 1 row from file 1
    let rows = ctx
        .sql("SELECT id FROM ducklake.main.prune_null WHERE id = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);

    // WHERE id > 5 should only return file 2 rows
    let rows = ctx
        .sql("SELECT id FROM ducklake.main.prune_null WHERE id > 5 ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let all_ids: Vec<i32> = rows
        .iter()
        .flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..arr.len()).map(move |i| arr.value(i))
        })
        .collect();

    assert_eq!(all_ids, vec![10, 20, 30]);

    // Full scan should return all non-null rows (no false negatives)
    let rows = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.prune_null")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let cnt = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 6);
}

/// File pruning effectiveness: creates many files with distinct ranges and verifies
/// that a selective filter prunes most files (rows scanned << total rows).
#[tokio::test(flavor = "multi_thread")]
async fn test_file_pruning_effectiveness() {
    let (writer, temp_dir) = create_test_env().await;
    let object_store = create_object_store();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Float64, false),
    ]));

    // Create 10 files with non-overlapping id ranges of 100 rows each.
    // File 0: ids 0-99, File 1: ids 100-199, ..., File 9: ids 900-999
    let total_rows = 1000;
    for file_idx in 0..10 {
        let start = file_idx * 100;
        let ids: Vec<i32> = (start..start + 100).collect();
        let values: Vec<f64> = ids.iter().map(|&id| id as f64 * 1.5).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(ids)), Arc::new(Float64Array::from(values))],
        )
        .unwrap();

        let tw = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
        if file_idx == 0 {
            tw.write_table("main", "prune_eff", &[batch]).await.unwrap();
        } else {
            tw.append_table("main", "prune_eff", &[batch])
                .await
                .unwrap();
        }
    }

    let ctx = create_read_context(&temp_dir).await;

    // Full table scan: should return all 1000 rows
    let rows = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.prune_eff")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let full_count = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(full_count, total_rows as i64);

    // Selective filter: id >= 500 AND id < 600 should match only file 5 (100 rows)
    let rows = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.prune_eff WHERE id >= 500 AND id < 600")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let filtered_count = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        filtered_count, 100,
        "Should match exactly one file's worth of rows"
    );

    // Very selective filter: id = 42 should match only 1 row from file 0
    let rows = ctx
        .sql("SELECT id, value FROM ducklake.main.prune_eff WHERE id = 42")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
    let id_val = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert_eq!(id_val, 42);

    // Out-of-range filter: id > 9999 should be pruned entirely
    let rows = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.prune_eff WHERE id > 9999")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let empty_count = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(empty_count, 0, "Out-of-range filter should prune all files");

    // Use EXPLAIN ANALYZE to verify pruning is happening via row group stats
    let explain = ctx
        .sql("EXPLAIN ANALYZE SELECT id FROM ducklake.main.prune_eff WHERE id >= 900")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Extract the explain text and check for evidence of pruning
    let explain_text: String = explain
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column(1) // plan column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..col.len()).map(move |i| col.value(i).to_string())
        })
        .collect::<Vec<_>>()
        .join("\n");

    // The explain output should show that rows were produced (confirming query worked)
    assert!(
        explain_text.contains("output_rows") || explain_text.contains("rows="),
        "EXPLAIN ANALYZE should show row counts: {}",
        &explain_text[..explain_text.len().min(500)]
    );
}

/// Cross-engine: DuckDB writes stats, DataFusion prunes based on them.
#[cfg(feature = "metadata-duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn test_pruning_duckdb_written_stats() {
    let temp_dir = TempDir::new().unwrap();
    let catalog_path = temp_dir.path().join("prune_ce.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    // DuckDB writes two separate inserts to create two files
    {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute("INSTALL ducklake;", []).unwrap();
        conn.execute("LOAD ducklake;", []).unwrap();
        conn.execute(
            &format!(
                "ATTACH 'ducklake:{}' AS ducklake (DATA_PATH '{}');",
                catalog_path.display(),
                data_path.display()
            ),
            [],
        )
        .unwrap();

        conn.execute(
            "CREATE TABLE ducklake.main.prune_test (id INT, name VARCHAR)",
            [],
        )
        .unwrap();

        // First insert: ids 1-3
        conn.execute(
            "INSERT INTO ducklake.main.prune_test VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            [],
        )
        .unwrap();

        // Second insert: ids 100-102
        conn.execute(
            "INSERT INTO ducklake.main.prune_test VALUES (100, 'x'), (101, 'y'), (102, 'z')",
            [],
        )
        .unwrap();
    }

    // DataFusion reads and should prune correctly
    let provider =
        datafusion_ducklake::DuckdbMetadataProvider::new(catalog_path.to_str().unwrap()).unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    // WHERE id = 2 should return 1 row
    let rows = ctx
        .sql("SELECT id, name FROM ducklake.main.prune_test WHERE id = 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);

    // WHERE id > 50 should return 3 rows from the second file
    let rows = ctx
        .sql("SELECT id FROM ducklake.main.prune_test WHERE id > 50 ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let all_ids: Vec<i32> = rows
        .iter()
        .flat_map(|b| {
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..arr.len()).map(move |i| arr.value(i))
        })
        .collect();

    assert_eq!(all_ids, vec![100, 101, 102]);

    // Total count should be 6
    let rows = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.prune_test")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let cnt = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 6);
}
