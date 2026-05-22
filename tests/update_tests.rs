//! Integration tests for UPDATE support.
//!
//! Tests verify that `DuckLakeTable::update()` correctly writes delete files
//! for old rows and new data files with updated values, and that subsequent
//! reads reflect the updates.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTable, DuckLakeTableWriter, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter, UpdateAssignment,
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

async fn write_test_data(writer: Arc<SqliteMetadataWriter>, batches: &[RecordBatch]) {
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    table_writer
        .write_table("main", "test_table", batches)
        .await
        .unwrap();
}

async fn create_writable_context(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("test", Arc::new(catalog));
    ctx
}

async fn create_read_context(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("test", Arc::new(catalog));
    ctx
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn make_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![Arc::new(Int32Array::from(ids)), Arc::new(StringArray::from(names))],
    )
    .unwrap()
}

/// Execute an update on the table and return the count of updated rows.
async fn execute_update(
    ctx: &SessionContext,
    assignments: Vec<UpdateAssignment>,
    filters: &[Expr],
) -> u64 {
    use datafusion::execution::SendableRecordBatchStream;
    use futures::StreamExt;

    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();
    let table = schema.table("test_table").await.unwrap().unwrap();

    let ducklake_table = table
        .as_any()
        .downcast_ref::<DuckLakeTable>()
        .expect("Expected DuckLakeTable");

    let state = ctx.state();
    let plan = ducklake_table
        .update(&state, assignments, filters)
        .await
        .unwrap();

    let task_ctx = ctx.task_ctx();
    let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx).unwrap();

    let mut total_updated = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        total_updated += count_col.value(0);
    }
    total_updated
}

async fn query_count(ctx: &SessionContext) -> i64 {
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM test.main.test_table")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0)
}

async fn query_rows(ctx: &SessionContext) -> Vec<(i32, String)> {
    let df = ctx
        .sql("SELECT id, name FROM test.main.test_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    rows
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_with_literal_value() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["alice", "bob", "charlie"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE test_table SET name = 'updated' WHERE id = 2
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1, // name column
            expr: lit("updated"),
        }],
        &[col("id").eq(lit(2))],
    )
    .await;
    assert_eq!(updated, 1);

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "alice".to_string()));
    assert_eq!(rows[1], (2, "updated".to_string()));
    assert_eq!(rows[2], (3, "charlie".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_multiple_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE test_table SET name = 'X' WHERE id > 3
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("X"),
        }],
        &[col("id").gt(lit(3))],
    )
    .await;
    assert_eq!(updated, 2);

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[1], (2, "b".to_string()));
    assert_eq!(rows[2], (3, "c".to_string()));
    assert_eq!(rows[3], (4, "X".to_string()));
    assert_eq!(rows[4], (5, "X".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_all_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE test_table SET name = 'all' (no WHERE)
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("all"),
        }],
        &[],
    )
    .await;
    assert_eq!(updated, 3);

    // Verify - all names changed but ids preserved
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "all".to_string()));
    assert_eq!(rows[1], (2, "all".to_string()));
    assert_eq!(rows[2], (3, "all".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_no_matching_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE test_table SET name = 'X' WHERE id > 100
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("X"),
        }],
        &[col("id").gt(lit(100))],
    )
    .await;
    assert_eq!(updated, 0);

    // Verify - nothing changed
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 3);
    let rows = query_rows(&ctx).await;
    assert_eq!(rows[0], (1, "a".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_preserves_row_count() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4], vec!["a", "b", "c", "d"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE should not change row count
    let ctx = create_writable_context(&temp_dir).await;
    execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("new"),
        }],
        &[col("id").lt_eq(lit(2))],
    )
    .await;

    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 4);
}

/// Like [`execute_update`], but returns the underlying `Result` so tests can
/// assert error paths (NOT NULL violations, concurrent conflicts, OOM cap).
async fn try_execute_update(
    ctx: &SessionContext,
    assignments: Vec<UpdateAssignment>,
    filters: &[Expr],
) -> datafusion::error::Result<u64> {
    use datafusion::execution::SendableRecordBatchStream;
    use futures::StreamExt;

    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();
    let table = schema.table("test_table").await.unwrap().unwrap();
    let ducklake_table = table
        .as_any()
        .downcast_ref::<DuckLakeTable>()
        .expect("Expected DuckLakeTable");

    let state = ctx.state();
    let plan = ducklake_table.update(&state, assignments, filters).await?;

    let task_ctx = ctx.task_ctx();
    let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx)?;

    let mut total = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        total += count_col.value(0);
    }
    Ok(total)
}

/// Build an UPDATE plan against `ctx` without executing it. The returned plan
/// captures the table's snapshot at plan-time, so running it later (after a
/// concurrent commit) exercises the conflict-detection path.
async fn build_update_plan(
    ctx: &SessionContext,
    assignments: Vec<UpdateAssignment>,
    filters: &[Expr],
) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();
    let table = schema.table("test_table").await.unwrap().unwrap();
    let ducklake_table = table
        .as_any()
        .downcast_ref::<DuckLakeTable>()
        .expect("Expected DuckLakeTable");
    let state = ctx.state();
    ducklake_table.update(&state, assignments, filters).await.unwrap()
}

/// Run an already-built UPDATE plan, returning the row count or the failure.
async fn run_update_plan(
    ctx: &SessionContext,
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> datafusion::error::Result<u64> {
    use datafusion::execution::SendableRecordBatchStream;
    use futures::StreamExt;

    let task_ctx = ctx.task_ctx();
    let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx)?;
    let mut total = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        total += count_col.value(0);
    }
    Ok(total)
}

/// Latest snapshot id in the catalog.
async fn current_snapshot_id(temp_dir: &TempDir) -> i64 {
    use sqlx::Row;
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());
    let pool = sqlx::sqlite::SqlitePool::connect(&conn_str).await.unwrap();
    let row = sqlx::query("SELECT COALESCE(MAX(snapshot_id), -1) FROM ducklake_snapshot")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    row.try_get(0).unwrap()
}

/// All Parquet files currently on disk under the test data dir
/// (data files + delete files). Used to assert no orphans escape on error.
fn list_parquet_files_on_disk(temp_dir: &TempDir) -> Vec<std::path::PathBuf> {
    let data_path = temp_dir.path().join("data");
    let mut out = Vec::new();
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p
                    .extension()
                    .and_then(|x| x.to_str())
                    .is_some_and(|x| x == "parquet")
                {
                    out.push(p);
                }
            }
        }
    }
    walk(&data_path, &mut out);
    out
}

/// Writable context backed by a custom `SessionConfig` so tests can dial
/// `ducklake.max_buffered_rows_per_dml` to exercise the safety valve.
async fn create_writable_context_with_max_buffered_rows(
    temp_dir: &TempDir,
    max_buffered_rows_per_dml: usize,
) -> SessionContext {
    use datafusion::common::config::ConfigOptions;
    use datafusion::execution::config::SessionConfig;
    use datafusion_ducklake::config::DuckLakeConfig;

    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = Arc::new(SqliteMetadataProvider::new(&conn_str).await.unwrap());
    let writer = Arc::new(SqliteMetadataWriter::new(&conn_str).await.unwrap());
    let catalog = DuckLakeCatalog::with_writer(provider, writer).unwrap();

    let mut options = ConfigOptions::default();
    options.extensions.insert(DuckLakeConfig {
        max_buffered_rows_per_dml,
    });
    let session_config = SessionConfig::from(options);
    let ctx = SessionContext::new_with_config(session_config);
    ctx.register_catalog("test", Arc::new(catalog));
    ctx
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_with_existing_deletes() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    // First: delete id=3
    {
        use datafusion::execution::SendableRecordBatchStream;
        use futures::StreamExt;

        let ctx = create_writable_context(&temp_dir).await;
        let catalog = ctx.catalog("test").unwrap();
        let schema = catalog.schema("main").unwrap();
        let table = schema.table("test_table").await.unwrap().unwrap();
        let ducklake_table = table.as_any().downcast_ref::<DuckLakeTable>().unwrap();
        let state = ctx.state();
        let plan = ducklake_table
            .delete(&state, &[col("id").eq(lit(3))])
            .await
            .unwrap();
        let task_ctx = ctx.task_ctx();
        let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx).unwrap();
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
    }

    // Now update: SET name = 'X' WHERE id >= 2
    // id=3 is deleted, so only ids 2, 4, 5 should be updated
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("X"),
        }],
        &[col("id").gt_eq(lit(2))],
    )
    .await;
    assert_eq!(
        updated, 3,
        "Should update ids 2, 4, 5 (not 3, which is deleted)"
    );

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 4); // 1, 2, 4, 5 (3 was deleted)
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[1], (2, "X".to_string()));
    assert_eq!(rows[2], (4, "X".to_string()));
    assert_eq!(rows[3], (5, "X".to_string()));
}

// ---------------------------------------------------------------------------
// Acceptance-criteria coverage for #18
// ---------------------------------------------------------------------------

/// A successful UPDATE must advance the snapshot id.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_advances_snapshot() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    let before = current_snapshot_id(&temp_dir).await;

    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("z"),
        }],
        &[col("id").eq(lit(2))],
    )
    .await;
    assert_eq!(updated, 1);

    let after = current_snapshot_id(&temp_dir).await;
    assert!(
        after > before,
        "UPDATE must advance the snapshot: before={before} after={after}"
    );
}

/// Setting a NOT NULL column to NULL must return an error and must NOT leave
/// any orphan files on disk — the validation has to run before any write.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_not_null_pre_write_validation() {
    use datafusion::scalar::ScalarValue;

    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    let before_disk: std::collections::BTreeSet<_> = list_parquet_files_on_disk(&temp_dir)
        .into_iter()
        .collect();
    let before_snap = current_snapshot_id(&temp_dir).await;

    // UPDATE test_table SET id = NULL WHERE id = 2 — `id` is NOT NULL.
    let ctx = create_writable_context(&temp_dir).await;
    let err = try_execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 0, // id (NOT NULL)
            expr: Expr::Literal(ScalarValue::Int32(None), None),
        }],
        &[col("id").eq(lit(2))],
    )
    .await
    .expect_err("UPDATE setting NOT NULL column to NULL must error");
    // The error may come from either our explicit
    // `validate_not_null_constraints` check or, even earlier, from
    // `RecordBatch::try_new` itself rejecting nulls on a non-nullable
    // field. Either path runs BEFORE any disk write, which is the
    // acceptance criterion this test enforces.
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not null")
            || msg.contains("not_null")
            || msg.contains("non-nullable")
            || msg.contains("constraint")
            || msg.contains("contains null values"),
        "expected a NOT NULL / nullability error, got: {err}"
    );

    // Give any spawned cleanup task a brief window.
    let mut after_disk: std::collections::BTreeSet<_> = list_parquet_files_on_disk(&temp_dir)
        .into_iter()
        .collect();
    for _ in 0..50 {
        if after_disk == before_disk {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        after_disk = list_parquet_files_on_disk(&temp_dir)
            .into_iter()
            .collect();
    }

    let orphans: Vec<_> = after_disk.difference(&before_disk).collect();
    assert!(
        orphans.is_empty(),
        "NOT NULL pre-write validation must not leave orphan files; found {} orphan(s): {:?}",
        orphans.len(),
        orphans
    );

    // Snapshot must NOT have advanced.
    assert_eq!(
        current_snapshot_id(&temp_dir).await,
        before_snap,
        "failed UPDATE must not advance snapshot"
    );

    // Read-back: data is unchanged.
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "a".to_string()));
    assert_eq!(rows[1], (2, "b".to_string()));
    assert_eq!(rows[2], (3, "c".to_string()));
}

/// `SET x = x + 1` must be evaluated against the PRE-update value of `x`,
/// not after — i.e. the assignment is computed against the matched row, not
/// the in-progress updated row.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_self_referencing_set_uses_pre_update_value() {
    let (writer, temp_dir) = create_test_env().await;
    // ids: 10, 20, 30 — pick values >9 so post-update we can distinguish
    // between "added 1" (11/21/31) and "added 1 twice" or other oddities.
    let batch = make_batch(vec![10, 20, 30], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // UPDATE test_table SET id = id + 1
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 0, // id
            expr: col("id") + lit(1_i32),
        }],
        &[],
    )
    .await;
    assert_eq!(updated, 3);

    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 3);
    let mut ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![11, 21, 31],
        "self-referencing SET must see pre-update value"
    );
}

/// Two UPDATEs planned against the same snapshot of the table that touch
/// OVERLAPPING rows of the SAME data file: the first wins, the second must
/// fail with `TransactionConflict` and not leave orphan files on disk.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_update_overlapping_conflicts() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    let ctx_a = create_writable_context(&temp_dir).await;
    let ctx_b = create_writable_context(&temp_dir).await;

    // Both UPDATEs target id <= 3 — overlapping in the same file.
    let plan_a = build_update_plan(
        &ctx_a,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("A"),
        }],
        &[col("id").lt_eq(lit(3))],
    )
    .await;
    let plan_b = build_update_plan(
        &ctx_b,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("B"),
        }],
        &[col("id").lt_eq(lit(3))],
    )
    .await;

    let before_disk = list_parquet_files_on_disk(&temp_dir).len();

    // A commits first; wins.
    let a_count = run_update_plan(&ctx_a, plan_a).await.expect("A should win");
    assert_eq!(a_count, 3);

    // B is still planned against the pre-A snapshot; must conflict.
    let b_err = run_update_plan(&ctx_b, plan_b)
        .await
        .expect_err("B should be rejected by conflict detection");
    assert!(
        b_err.to_string().to_lowercase().contains("conflict"),
        "expected a transaction-conflict, got: {b_err}"
    );

    // A's commit added one delete file + one data file. B added nothing
    // visible (its uploads must be cleaned up by `UploadCleanupGuard`).
    let expected = before_disk + 2;
    let mut after_disk = list_parquet_files_on_disk(&temp_dir).len();
    for _ in 0..50 {
        if after_disk == expected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        after_disk = list_parquet_files_on_disk(&temp_dir).len();
    }
    assert_eq!(
        after_disk, expected,
        "conflict failure must not leave orphan files (before={before_disk}, after={after_disk})"
    );

    // Data reflects only A's commit.
    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    let names: Vec<&str> = rows.iter().map(|(_, n)| n.as_str()).collect();
    assert_eq!(names, vec!["A", "A", "A", "d", "e"]);
}

/// Two UPDATEs planned against the same snapshot but targeting rows in
/// DIFFERENT data files: both should commit. This exercises the file-level
/// granularity of the conflict check — disjoint data_file_ids do not
/// conflict, so disjoint-by-construction workloads run free.
///
/// **Trade-off:** the writer's conflict check is keyed on `data_file_id`.
/// Two UPDATEs that touch disjoint rows in the *same* file would still
/// conflict; that's the conservative behaviour documented at the call site
/// in `update_exec.rs`. Two UPDATEs that touch disjoint rows in
/// disjoint files commit cleanly, which is the case this test covers.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_update_disjoint_files_both_commit() {
    let (writer, temp_dir) = create_test_env().await;

    // Two separate write calls produce two separate data files.
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    let batch_a = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    let batch_b = make_batch(vec![100, 200, 300], vec!["x", "y", "z"]);
    table_writer
        .write_table("main", "test_table", &[batch_a])
        .await
        .unwrap();
    table_writer
        .append_table("main", "test_table", &[batch_b])
        .await
        .unwrap();

    // Sanity: there should be two data files in the catalog now.
    {
        use sqlx::Row;
        let db_path = temp_dir.path().join("test.db");
        let conn_str = format!("sqlite:{}", db_path.display());
        let pool = sqlx::sqlite::SqlitePool::connect(&conn_str).await.unwrap();
        let row = sqlx::query(
            "SELECT COUNT(*) FROM ducklake_data_file WHERE end_snapshot IS NULL",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let active_data_files: i64 = row.try_get(0).unwrap();
        pool.close().await;
        assert_eq!(
            active_data_files, 2,
            "test precondition: two separate active data files"
        );
    }

    let ctx_a = create_writable_context(&temp_dir).await;
    let ctx_b = create_writable_context(&temp_dir).await;

    // A touches only the small-ids file; B touches only the large-ids file.
    let plan_a = build_update_plan(
        &ctx_a,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("A"),
        }],
        &[col("id").lt(lit(50))],
    )
    .await;
    let plan_b = build_update_plan(
        &ctx_b,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("B"),
        }],
        &[col("id").gt(lit(50))],
    )
    .await;

    // Both must succeed.
    let a_count = run_update_plan(&ctx_a, plan_a).await.expect("A should commit");
    assert_eq!(a_count, 3);
    let b_count = run_update_plan(&ctx_b, plan_b).await.expect("B should also commit");
    assert_eq!(b_count, 3);

    let ctx = create_read_context(&temp_dir).await;
    let rows = query_rows(&ctx).await;
    assert_eq!(rows.len(), 6);
    let names: Vec<&str> = rows.iter().map(|(_, n)| n.as_str()).collect();
    // ids 1,2,3 -> "A"; ids 100,200,300 -> "B"
    assert_eq!(names, vec!["A", "A", "A", "B", "B", "B"]);
}

/// The `ducklake.max_buffered_rows_per_dml` config knob bounds the UPDATE's
/// in-memory buffer. Dial it low and update more rows than fit: the exec
/// must error with `ResourcesExhausted` and leave no orphan files.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_max_buffered_rows_boundary_errors() {
    let (writer, temp_dir) = create_test_env().await;
    let ids: Vec<i32> = (0..1000).collect();
    let names: Vec<&str> = (0..1000).map(|_| "x").collect();
    let batch = make_batch(ids, names);
    write_test_data(writer, &[batch]).await;

    let before_disk: std::collections::BTreeSet<_> = list_parquet_files_on_disk(&temp_dir)
        .into_iter()
        .collect();

    // Cap at 100 rows; the unconditional UPDATE buffers all 1000.
    let ctx = create_writable_context_with_max_buffered_rows(&temp_dir, 100).await;
    let err = try_execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("y"),
        }],
        &[],
    )
    .await
    .expect_err("UPDATE exceeding the cap must error");

    let msg = err.to_string();
    let msg_lc = msg.to_lowercase();
    assert!(
        msg_lc.contains("too many rows") || msg_lc.contains("resourcesexhausted") || msg_lc.contains("buffered"),
        "expected a ResourcesExhausted-style error, got: {msg}"
    );

    // No orphan parquet files on disk (give async cleanup a window).
    let mut after_disk: std::collections::BTreeSet<_> = list_parquet_files_on_disk(&temp_dir)
        .into_iter()
        .collect();
    for _ in 0..50 {
        if after_disk == before_disk {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        after_disk = list_parquet_files_on_disk(&temp_dir)
            .into_iter()
            .collect();
    }
    let orphans: Vec<_> = after_disk.difference(&before_disk).collect();
    assert!(
        orphans.is_empty(),
        "max_buffered_rows trip must not leave orphans; got {} orphan(s)",
        orphans.len()
    );
}

/// The same workload that errors at a low cap succeeds when the cap is
/// raised high enough — confirms the knob is read at execute time.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_max_buffered_rows_configurable_raise() {
    let (writer, temp_dir) = create_test_env().await;
    let ids: Vec<i32> = (0..1000).collect();
    let names: Vec<&str> = (0..1000).map(|_| "x").collect();
    let batch = make_batch(ids, names);
    write_test_data(writer, &[batch]).await;

    // Cap at 10_000 — comfortably more than the 1000 rows we update.
    let ctx = create_writable_context_with_max_buffered_rows(&temp_dir, 10_000).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("y"),
        }],
        &[],
    )
    .await;
    assert_eq!(updated, 1000);

    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 1000);
}

/// UPDATE whose predicate matches only rows that were already deleted is a
/// no-op: zero rows updated, no new snapshot, no new files. (R3F-032 short-
/// circuit, mirrored from DELETE.)
#[tokio::test(flavor = "multi_thread")]
async fn test_update_already_deleted_rows_is_no_op() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // Delete id=2 first.
    {
        let ctx = create_writable_context(&temp_dir).await;
        let catalog = ctx.catalog("test").unwrap();
        let schema = catalog.schema("main").unwrap();
        let table = schema.table("test_table").await.unwrap().unwrap();
        let dlt = table.as_any().downcast_ref::<DuckLakeTable>().unwrap();
        let state = ctx.state();
        let plan = dlt.delete(&state, &[col("id").eq(lit(2))]).await.unwrap();
        let task_ctx = ctx.task_ctx();
        use datafusion::execution::SendableRecordBatchStream;
        use futures::StreamExt;
        let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx).unwrap();
        while let Some(b) = stream.next().await {
            let _ = b.unwrap();
        }
    }

    let before_snap = current_snapshot_id(&temp_dir).await;
    let before_disk = list_parquet_files_on_disk(&temp_dir).len();

    // Now UPDATE only id=2 — which is already deleted.
    let ctx = create_writable_context(&temp_dir).await;
    let updated = execute_update(
        &ctx,
        vec![UpdateAssignment {
            column_index: 1,
            expr: lit("Z"),
        }],
        &[col("id").eq(lit(2))],
    )
    .await;
    assert_eq!(updated, 0, "UPDATE over already-deleted rows must be a no-op");

    assert_eq!(
        current_snapshot_id(&temp_dir).await,
        before_snap,
        "no-op UPDATE must not advance the snapshot"
    );
    assert_eq!(
        list_parquet_files_on_disk(&temp_dir).len(),
        before_disk,
        "no-op UPDATE must not write any new files"
    );
}
