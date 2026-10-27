//! The catalog database, as the extensions of this crate use it.
//!
//! Core stores nothing for an extension (see `datafusion_ducklake::extension`).
//! Each extension here keeps its own table in the catalog's database, outside
//! the spec's `ducklake_*` namespace: DuckLake clients (the DuckDB extension
//! included) only touch the tables they name, so they neither read nor
//! disturb it. A table is created when a row is first recorded, and a missing
//! table reads as no rows. Prune joins `ducklake_data_file`, which is why it
//! must be the catalog's database.
//!
//! Core hands the database to a hook as a [`CatalogDb`], which runs every
//! query on core's catalog runtime: an extension opens no connection of its
//! own, and is never given another catalog's. SQLite and PostgreSQL catalogs
//! offer one; on DuckDB and MySQL these extensions stay off.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion_ducklake::extension::{CatalogDb, CatalogPool};
use datafusion_ducklake::{DuckLakeError, Result};

pub(crate) fn sql_error(error: sqlx::Error) -> DuckLakeError {
    DuckLakeError::Internal(format!("extension table: {error}"))
}

/// The error for a catalog backend these extensions do not support.
pub(crate) fn unsupported(db: &CatalogDb) -> DuckLakeError {
    DuckLakeError::Unsupported(format!(
        "extension tables on a {} catalog: this build supports SQLite and PostgreSQL",
        db.backend()
    ))
}

/// Run `work` on the catalog's runtime (see [`CatalogDb::run`]), flattening
/// its database error.
pub(crate) async fn run<F, Fut, T>(db: &CatalogDb, work: F) -> Result<T>
where
    F: FnOnce(CatalogPool) -> Fut,
    Fut: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    db.run(work).await?
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
) -> std::result::Result<(), sqlx::Error>
where
    DB: sqlx::Database,
    for<'c> &'c sqlx::Pool<DB>: sqlx::Executor<'c, Database = DB>,
    <DB as sqlx::Database>::Arguments: sqlx::IntoArguments<DB>,
    F: Fn() -> Fut,
    Fut: Future<Output = std::result::Result<(), sqlx::Error>>,
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

// ---------------------------------------------------------------------------
// Recording
// ---------------------------------------------------------------------------

/// Records rows for an extension, writing together the rows of files stored
/// while an earlier write to the same table is still running: under
/// concurrent uploads, one transaction serves many files rather than each
/// taking a connection and, on SQLite, the write lock.
pub(crate) struct GroupCommit<R> {
    queues: Mutex<HashMap<(u64, i64), Queue<R>>>,
}

struct Queue<R> {
    rows: Vec<R>,
    waiting: Vec<tokio::sync::oneshot::Sender<std::result::Result<(), String>>>,
}

impl<R> fmt::Debug for GroupCommit<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupCommit").finish_non_exhaustive()
    }
}

impl<R> Default for GroupCommit<R> {
    fn default() -> Self {
        Self {
            queues: Mutex::new(HashMap::new()),
        }
    }
}

impl<R: Send + 'static> GroupCommit<R> {
    /// Record `rows` of `table_id` with `write`, and return once they are
    /// recorded. The call that finds no write running writes, its own rows
    /// and then whatever arrived meanwhile, until none are left; the others
    /// wait for it.
    pub(crate) async fn record<W, Fut>(
        &self,
        db: &CatalogDb,
        table_id: i64,
        rows: Vec<R>,
        write: W,
    ) -> Result<()>
    where
        W: Fn(CatalogDb, i64, Vec<R>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let key = (db.id(), table_id);
        let (done, recorded) = tokio::sync::oneshot::channel();
        let leads = {
            let mut queues = self.lock();
            let leads = !queues.contains_key(&key);
            let queue = queues.entry(key).or_insert_with(|| Queue {
                rows: Vec::new(),
                waiting: Vec::new(),
            });
            queue.rows.extend(rows);
            queue.waiting.push(done);
            leads
        };
        if leads {
            // Dropped mid-write, the guard fails every waiter, so none hangs.
            let lead = Lead {
                commit: self,
                key,
            };
            loop {
                let (rows, waiting) = {
                    let mut queues = self.lock();
                    let Some(queue) = queues.get_mut(&key) else {
                        break;
                    };
                    if queue.waiting.is_empty() {
                        queues.remove(&key);
                        break;
                    }
                    (
                        std::mem::take(&mut queue.rows),
                        std::mem::take(&mut queue.waiting),
                    )
                };
                let written = write(db.clone(), table_id, rows)
                    .await
                    .map_err(|error| error.to_string());
                for waiter in waiting {
                    let _ = waiter.send(written.clone());
                }
            }
            std::mem::forget(lead);
        }
        match recorded.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(DuckLakeError::Internal(error)),
            Err(_) => Err(DuckLakeError::Internal(
                "extension rows not recorded: the write recording them was dropped".into(),
            )),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(u64, i64), Queue<R>>> {
        self.queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Held by the call writing for a key. Dropped before it finishes (its
/// future cancelled), it removes the key's queue, which fails its waiters.
struct Lead<'a, R> {
    commit: &'a GroupCommit<R>,
    key: (u64, i64),
}

impl<R> Drop for Lead<'_, R> {
    fn drop(&mut self) {
        self.commit
            .queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.key);
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// How long a row read is used before it is read again. Another process (a
/// backfill elsewhere) may have replaced it.
const KNOWN_TTL: Duration = Duration::from_secs(600);

/// How long "no row" is believed, so a backfill elsewhere is picked up.
const ABSENT_TTL: Duration = Duration::from_secs(60);

/// A `(path, key)` and what was read for it.
pub(crate) type Found<V> = Vec<((String, i64), V)>;

/// What one extension instance has read from its tables, parsed, by
/// `(catalog, table_id, path, key)`. Bounded by the bytes its entries hold:
/// the least useful go first. Rows this process records are put here as they
/// are recorded.
pub(crate) struct RowMemo<V> {
    extension: &'static str,
    entries: moka::sync::Cache<MemoKey, Entry<V>>,
}

type MemoKey = (u64, i64, String, i64);

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
            .field("bytes", &self.entries.weighted_size())
            .finish()
    }
}

impl<V: Send + Sync + 'static> RowMemo<V> {
    /// A memo of at most about `max_bytes`, with `bytes` estimating what one
    /// value holds.
    pub(crate) fn new(extension: &'static str, max_bytes: u64, bytes: fn(&V) -> usize) -> Self {
        Self {
            extension,
            entries: moka::sync::Cache::builder()
                .max_capacity(max_bytes)
                .weigher(move |key: &MemoKey, entry: &Entry<V>| {
                    let value = match entry {
                        Entry::Known(value, _) => bytes(value),
                        Entry::Absent(_) => 0,
                    };
                    u32::try_from(64 + key.2.len() + value).unwrap_or(u32::MAX)
                })
                .build(),
        }
    }

    /// The rows for `paths` x `keys` of `db`, by `(path, key)`: `read` is
    /// called once with the paths not known fresh, not at all once all are.
    /// A failed read is not remembered, so the next lookup asks again.
    pub(crate) async fn get<Fut>(
        &self,
        db: &CatalogDb,
        table_id: i64,
        paths: &[&str],
        keys: &[i64],
        read: impl FnOnce(Vec<String>) -> Fut,
    ) -> HashMap<(String, i64), Arc<V>>
    where
        Fut: Future<Output = Result<Found<V>>>,
    {
        let at = (db.id(), table_id);
        let missing = self.missing(at, paths, keys);
        if !missing.is_empty() {
            let found = read(missing.clone()).await;
            self.fill(at, missing, keys, found);
        }
        self.collect(at, paths, keys)
    }

    /// [`Self::get`] from synchronous code, with `read` blocking.
    pub(crate) fn get_blocking(
        &self,
        db: &CatalogDb,
        table_id: i64,
        paths: &[&str],
        keys: &[i64],
        read: impl FnOnce(Vec<String>) -> Result<Found<V>>,
    ) -> HashMap<(String, i64), Arc<V>> {
        let at = (db.id(), table_id);
        let missing = self.missing(at, paths, keys);
        if !missing.is_empty() {
            let found = read(missing.clone());
            self.fill(at, missing, keys, found);
        }
        self.collect(at, paths, keys)
    }

    /// Keep `value`, just recorded in `db` for `(path, key)`.
    pub(crate) fn put(&self, db: &CatalogDb, table_id: i64, path: &str, key: i64, value: V) {
        self.entries.insert(
            (db.id(), table_id, path.to_string(), key),
            Entry::Known(Arc::new(value), Instant::now()),
        );
    }

    fn missing(&self, (db, table_id): (u64, i64), paths: &[&str], keys: &[i64]) -> Vec<String> {
        paths
            .iter()
            .filter(|path| {
                keys.iter().any(|&key| {
                    !self
                        .entries
                        .get(&(db, table_id, path.to_string(), key))
                        .is_some_and(|entry| entry.fresh())
                })
            })
            .map(|path| path.to_string())
            .collect()
    }

    fn fill(
        &self,
        (db, table_id): (u64, i64),
        missing: Vec<String>,
        keys: &[i64],
        found: Result<Found<V>>,
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
                self.entries
                    .insert((db, table_id, path.clone(), key), entry);
            }
        }
    }

    fn collect(
        &self,
        (db, table_id): (u64, i64),
        paths: &[&str],
        keys: &[i64],
    ) -> HashMap<(String, i64), Arc<V>> {
        let mut out = HashMap::new();
        for &path in paths {
            for &key in keys {
                if let Some(Entry::Known(value, _)) =
                    self.entries.get(&(db, table_id, path.to_string(), key))
                {
                    out.insert((path.to_string(), key), value);
                }
            }
        }
        out
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion_ducklake::MetadataProvider;

    use super::*;

    async fn db() -> CatalogDb {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        datafusion_ducklake::SqliteMetadataProvider::from_pool(pool)
            .catalog_db()
            .unwrap()
    }

    #[tokio::test]
    async fn rows_stored_while_a_write_runs_are_written_together() {
        let db = db().await;
        let commit = Arc::new(GroupCommit::<i32>::default());
        let writes = Arc::new(AtomicUsize::new(0));
        let (started, wait_started) = tokio::sync::oneshot::channel::<()>();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let gate = Arc::new(Mutex::new(Some((started, released))));
        let written = Arc::new(Mutex::new(Vec::<Vec<i32>>::new()));
        let write = {
            let (writes, gate, written) = (writes.clone(), gate.clone(), written.clone());
            move |_db: CatalogDb, _table: i64, rows: Vec<i32>| {
                let (writes, gate, written) = (writes.clone(), gate.clone(), written.clone());
                async move {
                    writes.fetch_add(1, Ordering::SeqCst);
                    // The first write holds until the others have queued.
                    let first = gate.lock().unwrap().take();
                    if let Some((started, released)) = first {
                        started.send(()).unwrap();
                        released.await.unwrap();
                    }
                    written.lock().unwrap().push(rows);
                    Ok(())
                }
            }
        };
        let first = tokio::spawn({
            let (commit, db, write) = (commit.clone(), db.clone(), write.clone());
            async move { commit.record(&db, 1, vec![1], write).await }
        });
        wait_started.await.unwrap();
        let others: Vec<_> = (2..=4)
            .map(|n| {
                let (commit, db, write) = (commit.clone(), db.clone(), write.clone());
                tokio::spawn(async move { commit.record(&db, 1, vec![n], write).await })
            })
            .collect();
        // Let them queue behind the running write.
        tokio::time::sleep(Duration::from_millis(50)).await;
        release.send(()).unwrap();
        first.await.unwrap().unwrap();
        for other in others {
            other.await.unwrap().unwrap();
        }
        assert_eq!(writes.load(Ordering::SeqCst), 2);
        let mut second = written.lock().unwrap()[1].clone();
        second.sort();
        assert_eq!(written.lock().unwrap()[0], vec![1]);
        assert_eq!(second, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn a_failed_write_fails_every_row_in_it_and_the_next_call_writes_again() {
        let db = db().await;
        let commit = GroupCommit::<i32>::default();
        let failing = |_: CatalogDb, _: i64, _: Vec<i32>| async {
            Err(DuckLakeError::Internal("boom".into()))
        };
        assert!(commit.record(&db, 1, vec![1], failing).await.is_err());
        let ok = |_: CatalogDb, _: i64, _: Vec<i32>| async { Ok(()) };
        commit.record(&db, 1, vec![2], ok).await.unwrap();
    }

    #[tokio::test]
    async fn a_dropped_leader_fails_its_waiters_instead_of_leaving_them() {
        let db = db().await;
        let commit = Arc::new(GroupCommit::<i32>::default());
        let (started, wait_started) = tokio::sync::oneshot::channel::<()>();
        let started = Arc::new(Mutex::new(Some(started)));
        let hang = move |_: CatalogDb, _: i64, _: Vec<i32>| {
            let started = started.clone();
            async move {
                if let Some(started) = started.lock().unwrap().take() {
                    let _ = started.send(());
                }
                std::future::pending::<Result<()>>().await
            }
        };
        let leader = tokio::spawn({
            let (commit, db, hang) = (commit.clone(), db.clone(), hang.clone());
            async move { commit.record(&db, 1, vec![1], hang).await }
        });
        wait_started.await.unwrap();
        let waiter = tokio::spawn({
            let (commit, db, hang) = (commit.clone(), db.clone(), hang.clone());
            async move { commit.record(&db, 1, vec![2], hang).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        leader.abort();
        let waited = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("the waiter must not hang")
            .unwrap();
        assert!(waited.is_err());
        // The table can be recorded again.
        let ok = |_: CatalogDb, _: i64, _: Vec<i32>| async { Ok(()) };
        commit.record(&db, 1, vec![3], ok).await.unwrap();
    }

    #[tokio::test]
    async fn the_memo_is_bounded_by_bytes() {
        let db = db().await;
        let memo = RowMemo::<Vec<u8>>::new("test", 10_000, Vec::len);
        for i in 0..100 {
            memo.put(&db, 1, &format!("f{i}"), 0, vec![0; 1_000]);
        }
        memo.entries.run_pending_tasks();
        assert!(memo.entries.weighted_size() <= 10_000);
        assert!(memo.entries.entry_count() <= 10);
    }
}
