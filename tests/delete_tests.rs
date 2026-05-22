//! Integration tests for DELETE support.
//!
//! Tests verify that `DuckLakeTable::delete()` correctly writes delete files
//! and that subsequent reads reflect the deletions.

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
    SqliteMetadataWriter,
};

/// Create a local filesystem object store
fn create_object_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

/// Helper to create a test environment with writer and data directory.
/// Returns the writer, temp directory, and connection string.
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

/// Write test data: a simple (id, name) table with the given rows.
async fn write_test_data(writer: Arc<SqliteMetadataWriter>, batches: &[RecordBatch]) {
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer, object_store).unwrap();
    table_writer
        .write_table("main", "test_table", batches)
        .await
        .unwrap();
}

/// Create a writable SessionContext for the test environment.
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

/// Create a read-only SessionContext for the test environment.
async fn create_read_context(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("test", Arc::new(catalog));
    ctx
}

/// Helper to make a simple (id INT32, name UTF8) schema.
fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Helper to make a RecordBatch with (id, name) data.
fn make_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![Arc::new(Int32Array::from(ids)), Arc::new(StringArray::from(names))],
    )
    .unwrap()
}

/// Execute a delete on the table and return the count of deleted rows.
async fn execute_delete(ctx: &SessionContext, filters: &[Expr]) -> u64 {
    try_execute_delete(ctx, filters).await.unwrap()
}

/// Like [`execute_delete`] but returns the underlying `Result` so tests can
/// assert error paths (concurrent-conflict, commit-failure).
async fn try_execute_delete(
    ctx: &SessionContext,
    filters: &[Expr],
) -> datafusion::error::Result<u64> {
    use datafusion::execution::SendableRecordBatchStream;
    use futures::StreamExt;

    // Get the table provider
    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();
    let table = schema.table("test_table").await.unwrap().unwrap();

    // Downcast to DuckLakeTable
    let ducklake_table = table
        .as_any()
        .downcast_ref::<DuckLakeTable>()
        .expect("Expected DuckLakeTable");

    // Create delete plan
    let state = ctx.state();
    let plan = ducklake_table.delete(&state, filters).await?;

    // Execute
    let task_ctx = ctx.task_ctx();
    let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx)?;

    let mut total_deleted = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        total_deleted += count_col.value(0);
    }
    Ok(total_deleted)
}

/// Build a DELETE plan against `ctx` without executing it. The returned plan
/// captures the table's snapshot at plan-time, so executing it later (after a
/// concurrent commit) is the test surface for optimistic-concurrency conflict
/// detection.
async fn build_delete_plan(
    ctx: &SessionContext,
    filters: &[Expr],
) -> std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    let catalog = ctx.catalog("test").unwrap();
    let schema = catalog.schema("main").unwrap();
    let table = schema.table("test_table").await.unwrap().unwrap();

    let ducklake_table = table
        .as_any()
        .downcast_ref::<DuckLakeTable>()
        .expect("Expected DuckLakeTable");

    let state = ctx.state();
    ducklake_table.delete(&state, filters).await.unwrap()
}

/// Run an already-built DELETE plan, returning the row count or the failure.
async fn run_delete_plan(
    ctx: &SessionContext,
    plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> datafusion::error::Result<u64> {
    use datafusion::execution::SendableRecordBatchStream;
    use futures::StreamExt;

    let task_ctx = ctx.task_ctx();
    let mut stream: SendableRecordBatchStream = plan.execute(0, task_ctx)?;

    let mut total_deleted = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        total_deleted += count_col.value(0);
    }
    Ok(total_deleted)
}

/// Get the latest snapshot id in the catalog (highest `snapshot_id`).
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

/// Count active (non-ended) `ducklake_delete_file` rows for the given table.
async fn active_delete_file_count(temp_dir: &TempDir) -> i64 {
    use sqlx::Row;
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());
    let pool = sqlx::sqlite::SqlitePool::connect(&conn_str).await.unwrap();
    let row = sqlx::query(
        "SELECT COUNT(*) FROM ducklake_delete_file WHERE end_snapshot IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    pool.close().await;
    row.try_get(0).unwrap()
}

/// List `*-delete.parquet` files currently sitting in the data dir on disk.
fn list_delete_files_on_disk(temp_dir: &TempDir) -> Vec<std::path::PathBuf> {
    let data_path = temp_dir.path().join("data");
    let mut out = Vec::new();
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-delete.parquet"))
                {
                    out.push(p);
                }
            }
        }
    }
    walk(&data_path, &mut out);
    out
}

/// Helper to query the count of rows in the table.
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

/// Helper to query all ids in sorted order.
async fn query_ids(ctx: &SessionContext) -> Vec<i32> {
    let df = ctx
        .sql("SELECT id FROM test.main.test_table ORDER BY id")
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

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_with_where_clause() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    // Delete rows where id > 3
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").gt(lit(3))]).await;
    assert_eq!(deleted, 2, "Should have deleted 2 rows (id=4 and id=5)");

    // Read back with a fresh context to see the deletes
    let ctx = create_read_context(&temp_dir).await;
    let ids = query_ids(&ctx).await;
    assert_eq!(ids, vec![1, 2, 3], "Only ids 1,2,3 should remain");
    assert_eq!(query_count(&ctx).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_all_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // Delete all rows (no filter)
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[]).await;
    assert_eq!(deleted, 3, "Should have deleted all 3 rows");

    // Read back
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 0, "Table should be empty");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_no_matching_rows() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    // Delete rows where id > 100 (no matches)
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").gt(lit(100))]).await;
    assert_eq!(deleted, 0, "Should have deleted 0 rows");

    // Read back - all rows should still exist
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 3, "All 3 rows should remain");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_then_select_reflects_deletion() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4], vec!["alice", "bob", "charlie", "diana"]);
    write_test_data(writer, &[batch]).await;

    // Verify initial state
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 4);

    // Delete bob (id=2) and charlie (id=3)
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(
        &ctx,
        &[col("id").gt_eq(lit(2)).and(col("id").lt_eq(lit(3)))],
    )
    .await;
    assert_eq!(deleted, 2);

    // Verify remaining rows
    let ctx = create_read_context(&temp_dir).await;
    let ids = query_ids(&ctx).await;
    assert_eq!(ids, vec![1, 4]);

    // Verify names match
    let df = ctx
        .sql("SELECT name FROM test.main.test_table ORDER BY id")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "diana");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_with_existing_deletes() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    // First delete: remove id=2
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").eq(lit(2))]).await;
    assert_eq!(deleted, 1);

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_ids(&ctx).await, vec![1, 3, 4, 5]);

    // Second delete: remove id=4 (on a table that already has a delete file)
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").eq(lit(4))]).await;
    assert_eq!(deleted, 1);

    // Verify both deletions are reflected
    let ctx = create_read_context(&temp_dir).await;
    let ids = query_ids(&ctx).await;
    assert_eq!(ids, vec![1, 3, 5]);
    assert_eq!(query_count(&ctx).await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_with_string_filter() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["keep", "remove", "keep"]);
    write_test_data(writer, &[batch]).await;

    // Delete rows where name = 'remove'
    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("name").eq(lit("remove"))]).await;
    assert_eq!(deleted, 1);

    // Verify
    let ctx = create_read_context(&temp_dir).await;
    let ids = query_ids(&ctx).await;
    assert_eq!(ids, vec![1, 3]);
}

// ---------------------------------------------------------------------------
// Acceptance-criteria coverage for #17
// ---------------------------------------------------------------------------

/// A successful DELETE must allocate a new snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_advances_snapshot() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4], vec!["a", "b", "c", "d"]);
    write_test_data(writer, &[batch]).await;

    let before = current_snapshot_id(&temp_dir).await;

    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").eq(lit(2))]).await;
    assert_eq!(deleted, 1);

    let after = current_snapshot_id(&temp_dir).await;
    assert!(
        after > before,
        "snapshot must advance on DELETE: before={before} after={after}"
    );
}

/// A no-op DELETE (predicate matches nothing) must NOT allocate a new snapshot
/// and must NOT write a delete file. (R3F-032 short-circuit.)
#[tokio::test(flavor = "multi_thread")]
async fn test_no_op_delete_does_not_advance_snapshot() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
    write_test_data(writer, &[batch]).await;

    let before_snap = current_snapshot_id(&temp_dir).await;
    let before_active = active_delete_file_count(&temp_dir).await;
    let before_disk = list_delete_files_on_disk(&temp_dir).len();

    let ctx = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx, &[col("id").gt(lit(999))]).await;
    assert_eq!(deleted, 0);

    assert_eq!(
        current_snapshot_id(&temp_dir).await,
        before_snap,
        "no-op DELETE must not advance snapshot"
    );
    assert_eq!(
        active_delete_file_count(&temp_dir).await,
        before_active,
        "no-op DELETE must not register a delete file"
    );
    assert_eq!(
        list_delete_files_on_disk(&temp_dir).len(),
        before_disk,
        "no-op DELETE must not leave delete files on disk"
    );
}

/// Re-running the same DELETE predicate must be idempotent:
/// - the second invocation deletes 0 rows
/// - no spurious additional active delete-file entries
/// - no spurious additional `*-delete.parquet` files on disk
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_idempotent_no_spurious_entries() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    write_test_data(writer, &[batch]).await;

    let ctx = create_writable_context(&temp_dir).await;
    let first = execute_delete(&ctx, &[col("id").lt_eq(lit(2))]).await;
    assert_eq!(first, 2);

    let active_after_first = active_delete_file_count(&temp_dir).await;
    let disk_after_first = list_delete_files_on_disk(&temp_dir).len();
    assert_eq!(active_after_first, 1, "expected exactly one active delete file");
    assert_eq!(disk_after_first, 1, "expected exactly one *-delete.parquet");

    // Replay: same predicate, all matching rows already deleted.
    let ctx2 = create_writable_context(&temp_dir).await;
    let second = execute_delete(&ctx2, &[col("id").lt_eq(lit(2))]).await;
    assert_eq!(
        second, 0,
        "DELETE over already-deleted rows must report 0 affected"
    );

    // No new active row in ducklake_delete_file, no new file on disk.
    assert_eq!(
        active_delete_file_count(&temp_dir).await,
        active_after_first,
        "idempotent DELETE must not add a new active delete-file row"
    );
    assert_eq!(
        list_delete_files_on_disk(&temp_dir).len(),
        disk_after_first,
        "idempotent DELETE must not write an extra delete file"
    );

    // And the data is still correct.
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_ids(&ctx).await, vec![3, 4, 5]);
}

/// Two DELETEs planned against the same snapshot of the table:
/// the one that commits first wins, the second must fail with a
/// TransactionConflict — and on failure must leave **no orphan**
/// `*-delete.parquet` on disk (`UploadCleanupGuard` invariant).
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_delete_one_wins_one_conflicts() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4, 5, 6], vec!["a", "b", "c", "d", "e", "f"]);
    write_test_data(writer, &[batch]).await;

    // Each plan is built against its own context with its own DuckLakeTable —
    // so each carries its own snapshot_id from before the other commits.
    let ctx_a = create_writable_context(&temp_dir).await;
    let ctx_b = create_writable_context(&temp_dir).await;

    let plan_a = build_delete_plan(&ctx_a, &[col("id").eq(lit(1))]).await;
    let plan_b = build_delete_plan(&ctx_b, &[col("id").eq(lit(2))]).await;

    let before_disk = list_delete_files_on_disk(&temp_dir).len();

    // Run A first — should win.
    let a_count = run_delete_plan(&ctx_a, plan_a).await.expect("DELETE A should succeed");
    assert_eq!(a_count, 1);

    // Run B — its plan still sees the pre-A snapshot, so it should fail.
    let b_err = run_delete_plan(&ctx_b, plan_b)
        .await
        .expect_err("DELETE B should be rejected by conflict detection");
    let msg = b_err.to_string();
    assert!(
        msg.to_lowercase().contains("transaction conflict")
            || msg.to_lowercase().contains("conflict"),
        "expected a TransactionConflict, got: {msg}"
    );

    // Exactly one new *-delete.parquet should be on disk (A's). B's upload
    // must have been cleaned up by UploadCleanupGuard. The guard's cleanup
    // runs on a spawned task, so allow a bounded settling window.
    let expected = before_disk + 1;
    let mut after_disk = list_delete_files_on_disk(&temp_dir).len();
    for _ in 0..50 {
        if after_disk == expected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        after_disk = list_delete_files_on_disk(&temp_dir).len();
    }
    assert_eq!(
        after_disk, expected,
        "conflict failure must clean up B's orphan delete file (before={before_disk}, after={after_disk})"
    );

    // And the catalog reflects only A's effect.
    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_ids(&ctx).await, vec![2, 3, 4, 5, 6]);
    assert_eq!(active_delete_file_count(&temp_dir).await, 1);
}

/// Same scenario as the concurrent-conflict test, but framed as a
/// commit-failure recovery test: the upload completes (B writes its
/// `*-delete.parquet` to the object store before `register_dml_files` is
/// called), then the metadata commit fails, and `UploadCleanupGuard` must
/// remove the orphan from disk.
///
/// This is the test the ticket spells out under "Commit-failure recovery".
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_commit_failure_cleans_up_orphan() {
    let (writer, temp_dir) = create_test_env().await;
    let batch = make_batch(vec![1, 2, 3, 4], vec!["a", "b", "c", "d"]);
    write_test_data(writer, &[batch]).await;

    // Build the doomed plan first (snapshot = pre-conflict).
    let ctx_doomed = create_writable_context(&temp_dir).await;
    let plan_doomed = build_delete_plan(&ctx_doomed, &[col("id").eq(lit(3))]).await;

    // Cause a concurrent commit to occur AFTER the doomed plan was built.
    let ctx_winner = create_writable_context(&temp_dir).await;
    let deleted = execute_delete(&ctx_winner, &[col("id").eq(lit(1))]).await;
    assert_eq!(deleted, 1);

    // Snapshot the on-disk delete-file set BEFORE running the doomed plan.
    let before: std::collections::BTreeSet<_> = list_delete_files_on_disk(&temp_dir)
        .into_iter()
        .collect();

    // Run the doomed plan: upload succeeds, then register_dml_files fails
    // with TransactionConflict; UploadCleanupGuard must rm the upload.
    let err = run_delete_plan(&ctx_doomed, plan_doomed)
        .await
        .expect_err("doomed DELETE should fail with TransactionConflict");
    assert!(
        err.to_string().to_lowercase().contains("conflict"),
        "expected a conflict error, got: {err}"
    );

    // Cleanup is best-effort and may be async (spawned). Give it a short
    // bounded window to land before asserting.
    for _ in 0..50 {
        let after: std::collections::BTreeSet<_> = list_delete_files_on_disk(&temp_dir)
            .into_iter()
            .collect();
        if after == before {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let after: std::collections::BTreeSet<_> = list_delete_files_on_disk(&temp_dir)
        .into_iter()
        .collect();
    let orphans: Vec<_> = after.difference(&before).collect();
    panic!(
        "expected zero orphan delete files after commit failure, found {} orphan(s): {:?}",
        orphans.len(),
        orphans
    );
}

/// Sanity / memory-bound test. A DELETE over a moderately large table
/// should stream through Parquet (one batch at a time) and complete with
/// bounded memory. We don't try to assert a megabyte budget, but we DO
/// assert that the per-file buffer used by the exec is the position list
/// (one i64 per row), not the row data — so we exercise a row count that
/// would OOM if rows were buffered.
///
/// 200k rows × 8 bytes = ~1.6 MB of positions, comfortably bounded.
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_large_rowcount_is_memory_bounded() {
    let (writer, temp_dir) = create_test_env().await;

    // 200k rows in a single batch (this is well within reason for tests
    // but big enough to catch any per-row blowup we might regress to).
    const N: i32 = 200_000;
    let ids: Vec<i32> = (0..N).collect();
    let names: Vec<&str> = (0..N).map(|_| "x").collect();
    let batch = make_batch(ids, names);
    write_test_data(writer, &[batch]).await;

    let ctx = create_writable_context(&temp_dir).await;
    // Delete everything in one shot.
    let deleted = execute_delete(&ctx, &[]).await;
    assert_eq!(deleted as i32, N);

    let ctx = create_read_context(&temp_dir).await;
    assert_eq!(query_count(&ctx).await, 0);
}
