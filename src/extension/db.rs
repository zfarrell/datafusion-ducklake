//! The catalog's database, as an extension reaches it.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::DuckLakeError;
use crate::metadata_provider::{block_on, catalog_runtime};

/// The pool [`CatalogDb::run`] hands an extension's work.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum CatalogPool {
    #[cfg(feature = "metadata-sqlite")]
    Sqlite(sqlx::SqlitePool),
    #[cfg(feature = "metadata-postgres")]
    Postgres(sqlx::PgPool),
}

/// The catalog's database, for an extension to keep its own tables in. A hook
/// reaches it through its context, other code through
/// [`MetadataProvider::catalog_db`](crate::MetadataProvider::catalog_db), so
/// an extension opens no connection of its own and cannot be handed another
/// catalog's.
///
/// Every query an extension runs goes through [`run`](Self::run), which runs
/// it on the runtime this crate drives its own catalog queries on. That
/// matters because a pooled connection belongs to the runtime it was opened
/// on: one an extension opened on the embedder's runtime and returned to the
/// pool would leave a later blocking catalog call waiting on a runtime that
/// call has itself blocked. Keep the [`CatalogPool`] inside the work; a query
/// on it from anywhere else reopens that hole.
///
/// Name an extension's tables outside the spec's `ducklake_` prefix. Other
/// DuckLake clients only touch the tables they name.
#[derive(Clone)]
pub struct CatalogDb {
    pool: CatalogPool,
}

impl fmt::Debug for CatalogDb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogDb")
            .field("backend", &self.backend())
            .finish()
    }
}

impl CatalogDb {
    #[cfg(feature = "metadata-sqlite")]
    pub(crate) fn sqlite(pool: &sqlx::SqlitePool) -> Self {
        Self {
            pool: CatalogPool::Sqlite(pool.clone()),
        }
    }

    #[cfg(feature = "metadata-postgres")]
    pub(crate) fn postgres(pool: &sqlx::PgPool) -> Self {
        Self {
            pool: CatalogPool::Postgres(pool.clone()),
        }
    }

    /// `"sqlite"` or `"postgres"`.
    pub fn backend(&self) -> &'static str {
        match &self.pool {
            #[cfg(feature = "metadata-sqlite")]
            CatalogPool::Sqlite(_) => "sqlite",
            #[cfg(feature = "metadata-postgres")]
            CatalogPool::Postgres(_) => "postgres",
        }
    }

    /// Names the database: the same for every handle on it, whichever pool
    /// (a provider and a writer opened separately share it), and different
    /// for another database. For keying what an extension remembers per
    /// catalog. The SQLite file, or PostgreSQL's host, port and database, so
    /// two in-memory SQLite databases share one.
    pub fn id(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match &self.pool {
            #[cfg(feature = "metadata-sqlite")]
            CatalogPool::Sqlite(pool) => {
                let file = pool.connect_options().get_filename().to_path_buf();
                ("sqlite", std::fs::canonicalize(&file).unwrap_or(file)).hash(&mut hasher);
            },
            #[cfg(feature = "metadata-postgres")]
            CatalogPool::Postgres(pool) => {
                let options = pool.connect_options();
                (
                    "postgres",
                    options.get_host(),
                    options.get_port(),
                    options.get_database(),
                )
                    .hash(&mut hasher);
            },
        }
        hasher.finish()
    }

    /// Run `work` on the catalog runtime and return its output. Run a
    /// transaction whole inside one `work`. Dropping the returned future
    /// stops `work`; a panic in it resumes here.
    pub async fn run<F, Fut, T>(&self, work: F) -> crate::Result<T>
    where
        F: FnOnce(CatalogPool) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        AbortOnDrop(catalog_runtime().spawn(work(self.pool.clone()))).await
    }

    /// [`run`](Self::run) from synchronous code, on any runtime or none. It
    /// blocks the calling thread until `work` is done.
    pub fn run_blocking<F, Fut, T>(&self, work: F) -> crate::Result<T>
    where
        F: FnOnce(CatalogPool) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        block_on(self.run(work))
    }
}

/// A spawned task, aborted when its handle is dropped.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = crate::Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|joined| match joined {
            Ok(value) => Ok(value),
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => Err(DuckLakeError::Internal(format!(
                "the catalog runtime dropped an extension's work: {error}"
            ))),
        })
    }
}

#[cfg(all(test, feature = "metadata-sqlite"))]
mod tests {
    use super::*;

    async fn pool() -> sqlx::SqlitePool {
        sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn pools_on_one_database_share_an_id() {
        let dir = tempfile::tempdir().unwrap();
        let open = |name: &str| {
            let url = format!("sqlite://{}?mode=rwc", dir.path().join(name).display());
            async move {
                sqlx::sqlite::SqlitePoolOptions::new()
                    .connect(&url)
                    .await
                    .unwrap()
            }
        };
        let (a, also_a, b) = (open("a.db").await, open("a.db").await, open("b.db").await);
        assert_eq!(CatalogDb::sqlite(&a).id(), CatalogDb::sqlite(&also_a).id());
        assert_ne!(CatalogDb::sqlite(&a).id(), CatalogDb::sqlite(&b).id());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn work_runs_on_the_catalog_runtime() {
        let db = CatalogDb::sqlite(&pool().await);
        let name = db
            .run(|_| async { std::thread::current().name().map(str::to_string) })
            .await
            .unwrap();
        assert_eq!(name.as_deref(), Some("ducklake-catalog"));
    }

    #[test]
    fn run_blocking_works_without_a_runtime() {
        let pool = catalog_runtime().block_on(pool());
        let db = CatalogDb::sqlite(&pool);
        let one: i64 = db
            .run_blocking(|pool| async move {
                #[allow(irrefutable_let_patterns)]
                let CatalogPool::Sqlite(pool) = pool else {
                    unreachable!()
                };
                sqlx::query_scalar("SELECT 1").fetch_one(&pool).await
            })
            .unwrap()
            .unwrap();
        assert_eq!(one, 1);
    }

    #[tokio::test]
    async fn dropping_the_future_stops_the_work() {
        let db = CatalogDb::sqlite(&pool().await);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let run = db.run(|_| async move {
            let _tx = tx;
            std::future::pending::<()>().await;
        });
        // Start it, then give up on it.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), run)
                .await
                .is_err()
        );
        // The sender is dropped only when the task is.
        assert!(rx.await.is_err());
    }
}
