//! `ext_ducklake_file_order`: one row per data file whose rows were verified
//! to follow a sort key.
//!
//! ```sql
//! CREATE TABLE ext_ducklake_file_order (
//!     table_id        BIGINT  NOT NULL,
//!     path            VARCHAR NOT NULL,  -- ducklake_data_file.path
//!     file_size_bytes BIGINT  NOT NULL,  -- must equal the listed file's size
//!     sort_key        VARCHAR NOT NULL,  -- `field_id:asc|desc:nulls_first|nulls_last`, ...
//!     verified_by     VARCHAR NOT NULL,  -- write | verify
//!     verified_at     TIMESTAMP NOT NULL,
//!     PRIMARY KEY (table_id, path)
//! )
//! ```
//!
//! It is keyed by path, not by `data_file_id`: an id names a catalog row, and
//! a writer that reuses ids would hand a stale row to an unrelated file. A
//! later verification of the same object replaces the row: same bytes, so
//! only the key (a changed sort spec) can differ.

use datafusion_ducklake::Result;
use datafusion_ducklake::extension::{CatalogDb, CatalogPool};

use crate::db::{is_missing_table, run, sql_error, unsupported, upsert_creating};

/// The table's name.
pub const TABLE: &str = "ext_ducklake_file_order";

#[cfg(feature = "sqlite")]
const SQLITE_DDL: &str = "CREATE TABLE IF NOT EXISTS ext_ducklake_file_order (
    table_id BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    sort_key VARCHAR NOT NULL,
    verified_by VARCHAR NOT NULL,
    verified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_id, path)
)";

#[cfg(feature = "postgres")]
const POSTGRES_DDL: &str = "CREATE TABLE IF NOT EXISTS ext_ducklake_file_order (
    table_id BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    sort_key VARCHAR NOT NULL,
    verified_by VARCHAR NOT NULL,
    verified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, path)
)";

/// One row: the verified sort key of the file at `path`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderRow {
    /// The data file's catalog path (`ducklake_data_file.path`).
    pub path: String,
    /// The data file's size; a file of another size at `path` is not this one.
    pub file_size_bytes: i64,
    /// The verified key, primary key first.
    pub sort_key: String,
}

/// Record that the files `rows` name follow their keys, each as checked by
/// its `verified_by`, in one transaction (SQLite) or one statement
/// (PostgreSQL).
pub async fn record(
    db: &CatalogDb,
    table_id: i64,
    rows: Vec<(OrderRow, &'static str)>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let unsupported = unsupported(db);
    run(db, move |pool| async move {
        match pool {
            #[cfg(feature = "sqlite")]
            CatalogPool::Sqlite(pool) => upsert_creating(&pool, SQLITE_DDL, || async {
                let mut tx = pool.begin().await?;
                for (row, verified_by) in &rows {
                    sqlx::query(
                        "INSERT INTO ext_ducklake_file_order
                             (table_id, path, file_size_bytes, sort_key, verified_by)
                         VALUES (?, ?, ?, ?, ?)
                         ON CONFLICT (table_id, path) DO UPDATE SET
                             file_size_bytes = excluded.file_size_bytes,
                             sort_key = excluded.sort_key,
                             verified_by = excluded.verified_by,
                             verified_at = CURRENT_TIMESTAMP",
                    )
                    .bind(table_id)
                    .bind(&row.path)
                    .bind(row.file_size_bytes)
                    .bind(&row.sort_key)
                    .bind(*verified_by)
                    .execute(&mut *tx)
                    .await?;
                }
                tx.commit().await
            })
            .await
            .map_err(sql_error),
            #[cfg(feature = "postgres")]
            CatalogPool::Postgres(pool) => {
                let paths: Vec<&str> = rows.iter().map(|(r, _)| r.path.as_str()).collect();
                let sizes: Vec<i64> = rows.iter().map(|(r, _)| r.file_size_bytes).collect();
                let keys: Vec<&str> = rows.iter().map(|(r, _)| r.sort_key.as_str()).collect();
                let by: Vec<&str> = rows.iter().map(|(_, by)| *by).collect();
                upsert_creating(&pool, POSTGRES_DDL, || async {
                    sqlx::query(
                        "INSERT INTO ext_ducklake_file_order
                             (table_id, path, file_size_bytes, sort_key, verified_by)
                         SELECT $1, u.path, u.size, u.key, u.by
                         FROM UNNEST($2::text[], $3::bigint[], $4::text[], $5::text[])
                              AS u(path, size, key, by)
                         ON CONFLICT (table_id, path) DO UPDATE SET
                             file_size_bytes = EXCLUDED.file_size_bytes,
                             sort_key = EXCLUDED.sort_key,
                             verified_by = EXCLUDED.verified_by,
                             verified_at = now()",
                    )
                    .bind(table_id)
                    .bind(&paths)
                    .bind(&sizes)
                    .bind(&keys)
                    .bind(&by)
                    .execute(&pool)
                    .await
                    .map(|_| ())
                })
                .await
                .map_err(sql_error)
            },
            #[allow(unreachable_patterns)]
            _ => Err(unsupported),
        }
    })
    .await
}

/// The rows of `table_id`'s files at `paths`.
pub async fn read(db: &CatalogDb, table_id: i64, paths: Vec<String>) -> Result<Vec<OrderRow>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let unsupported = unsupported(db);
    run(db, move |pool| async move {
        let rows: std::result::Result<Vec<(String, i64, String)>, sqlx::Error> = match pool {
            #[cfg(feature = "sqlite")]
            CatalogPool::Sqlite(pool) => {
                let mut out = Vec::new();
                let mut failed = None;
                // Bounded binds per statement (SQLite's limit is 32766).
                for chunk in paths.chunks(900) {
                    let sql = format!(
                        "SELECT path, file_size_bytes, sort_key FROM ext_ducklake_file_order \
                         WHERE table_id = ? AND path IN ({})",
                        vec!["?"; chunk.len()].join(",")
                    );
                    let mut query = sqlx::query_as(sqlx::AssertSqlSafe(sql)).bind(table_id);
                    for path in chunk {
                        query = query.bind(path);
                    }
                    match query.fetch_all(&pool).await {
                        Ok(rows) => out.extend(rows),
                        Err(e) => {
                            failed = Some(e);
                            break;
                        },
                    }
                }
                failed.map_or(Ok(out), Err)
            },
            #[cfg(feature = "postgres")]
            CatalogPool::Postgres(pool) => {
                sqlx::query_as(
                    "SELECT path, file_size_bytes, sort_key FROM ext_ducklake_file_order
                     WHERE table_id = $1 AND path = ANY($2)",
                )
                .bind(table_id)
                .bind(&paths)
                .fetch_all(&pool)
                .await
            },
            #[allow(unreachable_patterns)]
            _ => return Err(unsupported),
        };
        match rows {
            Ok(rows) => Ok(rows
                .into_iter()
                .map(|(path, file_size_bytes, sort_key)| OrderRow {
                    path,
                    file_size_bytes,
                    sort_key,
                })
                .collect()),
            Err(e) if is_missing_table(&e) => Ok(Vec::new()),
            Err(e) => Err(sql_error(e)),
        }
    })
    .await
}

/// Drop the rows whose file no catalog row of their table lists any more;
/// returns how many.
pub(crate) async fn prune(db: &CatalogDb) -> Result<u64> {
    let unsupported = unsupported(db);
    run(db, move |pool| async move {
        // Any catalog row for the path keeps it: a retired file is still read
        // by time travel until expiry removes its row.
        let done = match pool {
            #[cfg(feature = "sqlite")]
            CatalogPool::Sqlite(pool) => sqlx::query(
                "DELETE FROM ext_ducklake_file_order
                 WHERE NOT EXISTS (
                     SELECT 1 FROM ducklake_data_file df
                     WHERE df.table_id = ext_ducklake_file_order.table_id
                       AND df.path = ext_ducklake_file_order.path)",
            )
            .execute(&pool)
            .await
            .map(|done| done.rows_affected()),
            #[cfg(feature = "postgres")]
            CatalogPool::Postgres(pool) => sqlx::query(
                "DELETE FROM ext_ducklake_file_order o
                 WHERE NOT EXISTS (SELECT 1 FROM ducklake_data_file df
                                   WHERE df.table_id = o.table_id AND df.path = o.path)",
            )
            .execute(&pool)
            .await
            .map(|done| done.rows_affected()),
            #[allow(unreachable_patterns)]
            _ => return Err(unsupported),
        };
        match done {
            Ok(n) => Ok(n),
            Err(e) if is_missing_table(&e) => Ok(0),
            Err(e) => Err(sql_error(e)),
        }
    })
    .await
}
