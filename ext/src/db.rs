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

// ---------------------------------------------------------------------------
// Recording
// ---------------------------------------------------------------------------

/// Records rows for an extension, writing together the rows of files stored
/// while an earlier write to the same table is still running: under
/// concurrent uploads, one transaction serves many files rather than each
/// taking a connection and, on SQLite, the write lock.
///
/// A task of its own on the catalog runtime writes, not any caller: a caller
/// returns once its rows
/// are written, and one that is dropped (its query cancelled) loses no
/// other caller's rows. Rows are grouped per pool, never per database: two
/// pools on one database can see different tables (a role's `search_path`).
pub(crate) struct GroupCommit<R> {
    queues: Arc<Mutex<Queues<R>>>,
}

type Queues<R> = HashMap<(usize, i64), Queue<R>>;

type Waiter = tokio::sync::oneshot::Sender<std::result::Result<(), String>>;

struct Queue<R> {
    rows: Vec<R>,
    waiting: Vec<Waiter>,
}

impl<R> fmt::Debug for GroupCommit<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupCommit").finish_non_exhaustive()
    }
}

impl<R> Default for GroupCommit<R> {
    fn default() -> Self {
        Self {
            queues: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<R: Send + 'static> GroupCommit<R> {
    /// Record `rows` of `table_id` through `db` with `write`, and return once
    /// they are recorded.
    pub(crate) async fn record<W, Fut>(
        &self,
        db: &CatalogDb,
        table_id: i64,
        rows: Vec<R>,
        write: W,
    ) -> Result<()>
    where
        W: Fn(CatalogDb, i64, Vec<R>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let key = (db.pool_id(), table_id);
        let (done, recorded) = tokio::sync::oneshot::channel();
        let starts = {
            let mut queues = lock(&self.queues);
            let starts = !queues.contains_key(&key);
            let queue = queues.entry(key).or_insert_with(|| Queue {
                rows: Vec::new(),
                waiting: Vec::new(),
            });
            queue.rows.extend(rows);
            queue.waiting.push(done);
            starts
        };
        if starts {
            // On the catalog runtime, never the caller's, which the caller
            // may be blocking while it waits.
            let queues = Arc::clone(&self.queues);
            let flushed_by = db.clone();
            db.spawn(move |_| flush(queues, key, flushed_by, table_id, write));
        }
        match recorded.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(DuckLakeError::Internal(error)),
            Err(_) => Err(DuckLakeError::Internal(
                "extension rows not recorded: the task writing them stopped".into(),
            )),
        }
    }
}

/// Write the queue at `key` batch by batch until it is empty.
async fn flush<R, W, Fut>(
    queues: Arc<Mutex<Queues<R>>>,
    key: (usize, i64),
    db: CatalogDb,
    table_id: i64,
    write: W,
) where
    W: Fn(CatalogDb, i64, Vec<R>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    // Stopped early (a panic, its runtime shutting down), the guard drops
    // the queue, which fails its callers rather than leaving them waiting.
    let mut guard = Flushing {
        queues: Arc::clone(&queues),
        key,
        finished: false,
    };
    loop {
        let (rows, waiting) = {
            let mut queues = lock(&queues);
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
    guard.finished = true;
}

fn lock<R>(queues: &Mutex<Queues<R>>) -> std::sync::MutexGuard<'_, Queues<R>> {
    queues
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Flushing<R> {
    queues: Arc<Mutex<Queues<R>>>,
    key: (usize, i64),
    finished: bool,
}

impl<R> Drop for Flushing<R> {
    fn drop(&mut self) {
        if !self.finished {
            lock(&self.queues).remove(&self.key);
        }
    }
}

/// `rows` with only the last of those sharing a key: one statement may not
/// upsert a key twice (PostgreSQL), and the later row is the newer record.
pub(crate) fn last_per_key<R, K: std::hash::Hash + Eq>(
    mut rows: Vec<R>,
    key: impl Fn(&R) -> K,
) -> Vec<R> {
    let mut seen = std::collections::HashSet::new();
    rows.reverse();
    rows.retain(|row| seen.insert(key(row)));
    rows.reverse();
    rows
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

    /// A write whose calls each wait for a gate the test opens, and that
    /// keeps what each call was given.
    #[derive(Clone, Default)]
    struct Gated {
        gates: Arc<Mutex<Vec<tokio::sync::oneshot::Receiver<()>>>>,
        started: Arc<tokio::sync::Notify>,
        written: Arc<Mutex<Vec<Batch>>>,
    }

    /// A row and the pool it came from.
    type Row = (usize, i32);

    /// The pool a batch was written through, and its rows.
    type Batch = (usize, Vec<Row>);

    type Written = std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>;

    impl Gated {
        /// Gates for the next `n` calls, in call order.
        fn gates(&self, n: usize) -> Vec<tokio::sync::oneshot::Sender<()>> {
            let (open, gates): (Vec<_>, Vec<_>) =
                (0..n).map(|_| tokio::sync::oneshot::channel()).unzip();
            *self.gates.lock().unwrap() = gates;
            open
        }

        fn write(&self) -> impl Fn(CatalogDb, i64, Vec<Row>) -> Written + Send + Sync + 'static {
            let this = self.clone();
            move |db, _, rows| {
                let this = this.clone();
                Box::pin(async move {
                    let gate = {
                        let mut gates = this.gates.lock().unwrap();
                        (!gates.is_empty()).then(|| gates.remove(0))
                    };
                    this.started.notify_one();
                    if let Some(gate) = gate {
                        let _ = gate.await;
                    }
                    this.written.lock().unwrap().push((db.pool_id(), rows));
                    Ok(())
                })
            }
        }
    }

    async fn record(
        commit: &Arc<GroupCommit<(usize, i32)>>,
        db: &CatalogDb,
        gated: &Gated,
        row: i32,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let (commit, db, write) = (commit.clone(), db.clone(), gated.write());
        tokio::spawn(async move {
            let rows = vec![(db.pool_id(), row)];
            commit.record(&db, 1, rows, write).await
        })
    }

    #[tokio::test]
    async fn rows_through_another_pool_are_never_written_through_this_one() {
        // Two pools on one database: the same `id`, as two tenants' pools on
        // one PostgreSQL database with different search paths would have.
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("c.db").display());
        let open = || async {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .connect(&url)
                .await
                .unwrap();
            datafusion_ducklake::SqliteMetadataProvider::from_pool(pool)
                .catalog_db()
                .unwrap()
        };
        let (a, b) = (open().await, open().await);
        assert_eq!(a.id(), b.id());
        let commit = Arc::new(GroupCommit::default());
        let gated = Gated::default();
        let open = gated.gates(1);
        let first = record(&commit, &a, &gated, 1).await;
        gated.started.notified().await;
        let others = [record(&commit, &a, &gated, 2).await, record(&commit, &b, &gated, 3).await];
        tokio::time::sleep(Duration::from_millis(50)).await;
        open.into_iter().for_each(|gate| gate.send(()).unwrap());
        first.await.unwrap().unwrap();
        for other in others {
            other.await.unwrap().unwrap();
        }
        for (through, rows) in gated.written.lock().unwrap().iter() {
            assert!(
                rows.iter().all(|(from, _)| from == through),
                "rows written through a pool they did not come from: {rows:?}"
            );
        }
    }

    /// A caller that blocks its single-threaded runtime while it waits must
    /// not be waiting on a write that runtime would have to run.
    #[test]
    fn a_caller_blocking_its_current_thread_runtime_still_gets_its_rows_written() {
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let recorded = runtime.block_on(async {
                let db = db().await;
                let commit = GroupCommit::<i32>::default();
                let ok = |_: CatalogDb, _: i64, _: Vec<i32>| async { Ok(()) };
                futures::executor::block_on(commit.record(&db, 1, vec![1], ok))
            });
            let _ = done.send(recorded.is_ok());
        });
        assert!(
            finished
                .recv_timeout(Duration::from_secs(10))
                .expect("the caller hung"),
            "the rows were not recorded"
        );
    }

    #[tokio::test]
    async fn a_call_returns_once_its_rows_are_written_while_others_keep_coming() {
        let db = db().await;
        let commit = Arc::new(GroupCommit::default());
        let gated = Gated::default();
        let mut open = gated.gates(2).into_iter();
        let first = record(&commit, &db, &gated, 1).await;
        gated.started.notified().await;
        let second = record(&commit, &db, &gated, 2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        // The first write finishes; the second, holding row 2, does not.
        open.next().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("the first call must not wait for rows it did not bring")
            .unwrap()
            .unwrap();
        open.next().unwrap().send(()).unwrap();
        second.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn rows_still_queued_are_written_when_the_call_writing_drops() {
        let db = db().await;
        let commit = Arc::new(GroupCommit::default());
        let gated = Gated::default();
        let open = gated.gates(1);
        let first = record(&commit, &db, &gated, 1).await;
        gated.started.notified().await;
        let second = record(&commit, &db, &gated, 2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        first.abort();
        let _ = first.await;
        open.into_iter().for_each(|gate| {
            let _ = gate.send(());
        });
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("the waiter must not hang")
            .unwrap()
            .expect("another query's cancel must not lose this call's rows");
        let written = gated.written.lock().unwrap();
        assert!(
            written
                .iter()
                .any(|(_, rows)| rows.iter().any(|(_, row)| *row == 2)),
            "{written:?}"
        );
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
