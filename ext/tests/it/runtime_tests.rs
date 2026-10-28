//! An extension's queries share the catalog's PostgreSQL pool with core's
//! blocking catalog calls, so they must not leave a connection bound to the
//! embedder's runtime. Docker-gated (testcontainers Postgres).

#![cfg(not(feature = "skip-tests-with-docker"))]

use std::time::Duration;

use datafusion_ducklake::extension::CatalogPool;
use datafusion_ducklake::{MetadataProvider, PostgresMetadataProvider};
use sqlx::postgres::PgPoolOptions;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// On a single-threaded runtime, an extension query opens the pool's only
/// connection, then a blocking catalog call on that same thread takes it. A
/// connection the extension opened on this runtime would wait on the driver
/// of the runtime the call has blocked.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_catalog_call_after_an_extension_query_on_a_current_thread_runtime() {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");

    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let outcome = runtime.block_on(async move {
            // Lazy: no connection until the extension's query.
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(3))
                .connect_lazy(&url)
                .unwrap();
            let provider = PostgresMetadataProvider::from_pool(pool);
            let db = provider.catalog_db().unwrap();
            let one: i32 = db
                .run(|pool| async move {
                    let CatalogPool::Postgres(pool) = pool else {
                        unreachable!()
                    };
                    sqlx::query_scalar("SELECT 1").fetch_one(&pool).await
                })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(one, 1);
            // No DuckLake tables here, so the call fails, but only once
            // PostgreSQL has answered on the connection.
            provider
                .get_current_snapshot()
                .map_err(|error| error.to_string())
        });
        let _ = done.send(outcome);
    });

    let outcome = finished
        .recv_timeout(Duration::from_secs(30))
        .expect("the catalog call hung");
    let error = outcome.expect_err("the catalog has no DuckLake tables");
    assert!(
        error.contains("ducklake_snapshot"),
        "the call must reach PostgreSQL, not time out waiting on the connection: {error}"
    );
}

/// One batch can hold two rows for one file (two verify passes of a table
/// stored together); PostgreSQL's multi-row upsert refuses a key twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_with_two_rows_for_one_file_records_the_later() {
    use datafusion_ducklake_ext::file_order::table::{self as order_table, OrderRow};

    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = PgPoolOptions::new()
        .connect(&format!(
            "postgresql://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
    let db = crate::postgres_db(&pool);
    let row = |sort_key: &str| OrderRow {
        path: "a.parquet".to_string(),
        file_size_bytes: 10,
        sort_key: sort_key.to_string(),
    };
    order_table::record(
        &db,
        1,
        vec![(row("1:asc:nulls_last"), "verify"), (row("1:desc:nulls_first"), "verify")],
    )
    .await
    .unwrap();
    assert_eq!(
        order_table::read(&db, 1, vec!["a.parquet".to_string()])
            .await
            .unwrap(),
        vec![row("1:desc:nulls_first")]
    );
}

/// The same for the index: one batch can hold two rows for one file and
/// column (a write and a backfill stored together).
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_with_two_index_rows_for_one_file_records_the_later() {
    use datafusion_ducklake_ext::file_index::FileIndexRow;
    use datafusion_ducklake_ext::file_index::format::{FileIndexMeta, IndexValueType};
    use datafusion_ducklake_ext::file_index::table as index_table;

    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let pool = PgPoolOptions::new()
        .connect(&format!(
            "postgresql://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .unwrap();
    let db = crate::postgres_db(&pool);
    let row = |index_path: &str, built_by: &str| FileIndexRow {
        meta: FileIndexMeta {
            path: "a.parquet".to_string(),
            file_size_bytes: 10,
            column_id: 1,
            index_path: index_path.to_string(),
            index_size_bytes: 1,
            format_version: 1,
            value_type: IndexValueType::Int64,
            row_count: 1,
            row_group_rows: vec![1],
            heavy_hitters: Vec::new(),
            bucket_counts: None,
        },
        index_key: index_path.to_string(),
        built_by: built_by.to_string(),
    };
    index_table::record(
        &db,
        1,
        vec![row("first.pidx", "write"), row("second.pidx", "backfill")],
    )
    .await
    .unwrap();
    let read = index_table::read(&db, 1, vec!["a.parquet".to_string()], vec![1])
        .await
        .unwrap();
    let read: Vec<_> = read
        .iter()
        .map(|row| (row.meta.index_path.as_str(), row.built_by.as_str()))
        .collect();
    assert_eq!(read, vec![("second.pidx", "backfill")]);
}
