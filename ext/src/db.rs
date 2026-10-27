//! The catalog database, as the extensions of this crate reach it.
//!
//! Core stores nothing for an extension (see `datafusion_ducklake::extension`).
//! Each extension here keeps its own table in the catalog's database, outside
//! the spec's `ducklake_*` namespace: DuckLake clients (the DuckDB extension
//! included) only touch the tables they name, so they neither read nor
//! disturb it. A table is created when a row is first recorded, and a missing
//! table reads as no rows. Prune joins `ducklake_data_file`, which is why it
//! must be the catalog's database.
//!
//! The caller hands each extension an [`ExtensionDb`]: the pool it already
//! gave core, so an extension opens no connection of its own.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion_ducklake::DuckLakeError;

/// A catalog database pool, shared with core: cloning a pool clones a handle,
/// not its connections.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ExtensionDb {
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
}

impl ExtensionDb {
    /// The SQLite catalog's pool.
    #[cfg(feature = "sqlite")]
    pub fn sqlite(pool: sqlx::SqlitePool) -> Self {
        Self::Sqlite(pool)
    }

    /// The PostgreSQL catalog's pool (single or multi-catalog layout).
    #[cfg(feature = "postgres")]
    pub fn postgres(pool: sqlx::PgPool) -> Self {
        Self::Postgres(pool)
    }
}

pub(crate) fn sql_error(error: sqlx::Error) -> DuckLakeError {
    DuckLakeError::Internal(format!("extension table: {error}"))
}

/// Whether `error` says the table does not exist (nothing was ever recorded
/// in this catalog), which reads as "no rows".
pub(crate) fn is_missing_table(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(db) => {
            db.code().as_deref() == Some("42P01") || db.message().contains("no such table")
        },
        _ => false,
    }
}

/// Run `upsert`, creating the table with `ddl` first if it is missing. On
/// PostgreSQL the table is created only when missing: Postgres checks CREATE
/// privilege before IF NOT EXISTS, so a role without DDL rights can still
/// record once an administrator has created it.
pub(crate) async fn upsert_creating<DB, F, Fut>(
    pool: &sqlx::Pool<DB>,
    ddl: &'static str,
    upsert: F,
) -> Result<(), sqlx::Error>
where
    DB: sqlx::Database,
    for<'c> &'c sqlx::Pool<DB>: sqlx::Executor<'c, Database = DB>,
    <DB as sqlx::Database>::Arguments: sqlx::IntoArguments<DB>,
    F: Fn() -> Fut,
    Fut: Future<Output = Result<(), sqlx::Error>>,
{
    match upsert().await {
        Err(e) if is_missing_table(&e) => {
            match sqlx::query(ddl).execute(pool).await {
                Ok(_) => {},
                // A concurrent first creation can lose the race on the
                // type name (23505) even with IF NOT EXISTS.
                Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("23505") => {},
                Err(e) => return Err(e),
            }
            upsert().await
        },
        done => done,
    }
}

/// The time `min_age` ago in SQLite's `CURRENT_TIMESTAMP` format, so the
/// comparison is textual and exact. Second resolution: compare with `<=`, so
/// a zero `min_age` takes this second's rows.
#[cfg(feature = "sqlite")]
pub(crate) fn sqlite_cutoff(min_age: Duration) -> String {
    chrono::Duration::from_std(min_age)
        .ok()
        .and_then(|age| chrono::Utc::now().checked_sub_signed(age))
        .map_or_else(
            || "0000-01-01 00:00:00".to_string(),
            |at| at.format("%Y-%m-%d %H:%M:%S").to_string(),
        )
}

/// Drive `future` (a query on a caller's pool) from synchronous code. Only a
/// multi-threaded runtime can block a worker and keep its I/O moving; from
/// anywhere else, `None`, and the caller does without the read.
pub(crate) fn block_on<F: Future>(future: F) -> Option<F::Output> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            Some(tokio::task::block_in_place(|| handle.block_on(future)))
        },
        _ => None,
    }
}

/// How long a row read is used before it is read again. Another process (a
/// backfill elsewhere) may have replaced it.
const KNOWN_TTL: Duration = Duration::from_secs(600);

/// How long "no row" is believed, so a backfill elsewhere is picked up.
const ABSENT_TTL: Duration = Duration::from_secs(60);

/// A `(path, key)` and what was read for it.
pub(crate) type Found<V> = Vec<((String, i64), V)>;

/// What one extension instance has read from its table, parsed, by
/// `(table_id, path, key)`. Bounded: the least useful entries go first. Rows
/// this process records are put here as they are recorded.
pub(crate) struct RowMemo<V> {
    extension: &'static str,
    entries: moka::sync::Cache<(i64, String, i64), Entry<V>>,
}

enum Entry<V> {
    Known(Arc<V>, Instant),
    Absent(Instant),
}

impl<V> Clone for Entry<V> {
    fn clone(&self) -> Self {
        match self {
            Entry::Known(value, at) => Entry::Known(Arc::clone(value), *at),
            Entry::Absent(at) => Entry::Absent(*at),
        }
    }
}

impl<V> Entry<V> {
    fn fresh(&self) -> bool {
        match self {
            Entry::Known(_, at) => at.elapsed() < KNOWN_TTL,
            Entry::Absent(at) => at.elapsed() < ABSENT_TTL,
        }
    }
}

impl<V> fmt::Debug for RowMemo<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowMemo")
            .field("extension", &self.extension)
            .field("entries", &self.entries.entry_count())
            .finish()
    }
}

impl<V: Send + Sync + 'static> RowMemo<V> {
    pub(crate) fn new(extension: &'static str, max_entries: u64) -> Self {
        Self {
            extension,
            entries: moka::sync::Cache::new(max_entries),
        }
    }

    /// The rows for `paths` x `keys`, by `(path, key)`: `read` is called once
    /// with the paths not known fresh, not at all once all are. A failed read
    /// is not remembered, so the next lookup asks again.
    pub(crate) async fn get<Fut>(
        &self,
        table_id: i64,
        paths: &[&str],
        keys: &[i64],
        read: impl FnOnce(Vec<String>) -> Fut,
    ) -> HashMap<(String, i64), Arc<V>>
    where
        Fut: Future<Output = datafusion_ducklake::Result<Found<V>>>,
    {
        let missing = self.missing(table_id, paths, keys);
        if !missing.is_empty() {
            let found = read(missing.clone()).await;
            self.fill(table_id, missing, keys, found);
        }
        self.collect(table_id, paths, keys)
    }

    /// [`Self::get`] from synchronous code: see [`block_on`]. Where it cannot
    /// block, only what is already known.
    pub(crate) fn get_blocking<Fut>(
        &self,
        table_id: i64,
        paths: &[&str],
        keys: &[i64],
        read: impl FnOnce(Vec<String>) -> Fut,
    ) -> HashMap<(String, i64), Arc<V>>
    where
        Fut: Future<Output = datafusion_ducklake::Result<Found<V>>>,
    {
        let missing = self.missing(table_id, paths, keys);
        if !missing.is_empty() {
            match block_on(read(missing.clone())) {
                Some(found) => self.fill(table_id, missing, keys, found),
                None => tracing::debug!(
                    extension = self.extension,
                    "not on a multi-threaded runtime; rows not read"
                ),
            }
        }
        self.collect(table_id, paths, keys)
    }

    /// Keep `value`, just recorded for `(path, key)`.
    pub(crate) fn put(&self, table_id: i64, path: &str, key: i64, value: V) {
        self.entries.insert(
            (table_id, path.to_string(), key),
            Entry::Known(Arc::new(value), Instant::now()),
        );
    }

    fn missing(&self, table_id: i64, paths: &[&str], keys: &[i64]) -> Vec<String> {
        paths
            .iter()
            .filter(|path| {
                keys.iter().any(|&key| {
                    !self
                        .entries
                        .get(&(table_id, path.to_string(), key))
                        .is_some_and(|entry| entry.fresh())
                })
            })
            .map(|path| path.to_string())
            .collect()
    }

    fn fill(
        &self,
        table_id: i64,
        missing: Vec<String>,
        keys: &[i64],
        found: datafusion_ducklake::Result<Found<V>>,
    ) {
        let found = match found {
            Ok(found) => found,
            Err(error) => {
                tracing::debug!(%error, extension = self.extension, "row lookup failed");
                return;
            },
        };
        let now = Instant::now();
        let mut found: HashMap<(String, i64), Arc<V>> = found
            .into_iter()
            .map(|(key, value)| (key, Arc::new(value)))
            .collect();
        for path in missing {
            for &key in keys {
                let entry = match found.remove(&(path.clone(), key)) {
                    Some(value) => Entry::Known(value, now),
                    None => Entry::Absent(now),
                };
                self.entries.insert((table_id, path.clone(), key), entry);
            }
        }
    }

    fn collect(
        &self,
        table_id: i64,
        paths: &[&str],
        keys: &[i64],
    ) -> HashMap<(String, i64), Arc<V>> {
        let mut out = HashMap::new();
        for &path in paths {
            for &key in keys {
                if let Some(Entry::Known(value, _)) =
                    self.entries.get(&(table_id, path.to_string(), key))
                {
                    out.insert((path.to_string(), key), value);
                }
            }
        }
        out
    }
}
