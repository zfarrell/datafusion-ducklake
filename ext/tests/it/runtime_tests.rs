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
