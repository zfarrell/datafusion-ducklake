//! `ext_ducklake_file_index`: one row per (data file, indexed column).
//!
//! ```sql
//! CREATE TABLE ext_ducklake_file_index (
//!     table_id         BIGINT  NOT NULL,
//!     path             VARCHAR NOT NULL,  -- ducklake_data_file.path
//!     file_size_bytes  BIGINT  NOT NULL,  -- must equal the listed file's size
//!     column_id        BIGINT  NOT NULL,
//!     index_path       VARCHAR NOT NULL,  -- relative to the data file's directory
//!     index_key        VARCHAR NOT NULL,  -- the object's full store key
//!     index_size_bytes BIGINT  NOT NULL,
//!     format_version   INTEGER NOT NULL,
//!     value_type       VARCHAR NOT NULL,
//!     row_count        BIGINT  NOT NULL,
//!     row_group_rows   VARCHAR NOT NULL,  -- JSON
//!     heavy_hitters    VARCHAR NOT NULL,  -- JSON
//!     built_by         VARCHAR NOT NULL,  -- write | compact | backfill
//!     built_at         TIMESTAMP NOT NULL,
//!     bucket_counts    VARCHAR,           -- JSON, from the backfill
//!     PRIMARY KEY (table_id, path, column_id)
//! )
//! ```
//!
//! `index_key` is kept because prune runs after expiry, when the table's path
//! chain may no longer resolve. A later row for the same (path, column)
//! replaces the earlier one: same bytes, so only what was derived from them
//! (counts, a newer format) can differ.

use std::time::Duration;

use datafusion_ducklake::extension::{CatalogDb, CatalogPool};
use datafusion_ducklake::{DuckLakeError, Result};

use super::FileIndexRow;
use super::format::{
    FileIndexMeta, IndexValueType, heavy_hitters_from_json, heavy_hitters_to_json,
    row_group_rows_from_json, row_group_rows_to_json,
};
use crate::db::{is_missing_table, run, sql_error, unsupported, upsert_creating};

/// The table's name.
pub const TABLE: &str = "ext_ducklake_file_index";

#[cfg(feature = "sqlite")]
const SQLITE_DDL: &str = "CREATE TABLE IF NOT EXISTS ext_ducklake_file_index (
    table_id BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    index_path VARCHAR NOT NULL,
    index_key VARCHAR NOT NULL,
    index_size_bytes BIGINT NOT NULL,
    format_version INTEGER NOT NULL,
    value_type VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    row_group_rows VARCHAR NOT NULL,
    heavy_hitters VARCHAR NOT NULL,
    built_by VARCHAR NOT NULL,
    built_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    bucket_counts VARCHAR,
    PRIMARY KEY (table_id, path, column_id)
)";

#[cfg(feature = "postgres")]
const POSTGRES_DDL: &str = "CREATE TABLE IF NOT EXISTS ext_ducklake_file_index (
    table_id BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    index_path VARCHAR NOT NULL,
    index_key VARCHAR NOT NULL,
    index_size_bytes BIGINT NOT NULL,
    format_version INTEGER NOT NULL,
    value_type VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    row_group_rows VARCHAR NOT NULL,
    heavy_hitters VARCHAR NOT NULL,
    built_by VARCHAR NOT NULL,
    built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    bucket_counts VARCHAR,
    PRIMARY KEY (table_id, path, column_id)
)";

const SELECT_COLUMNS: &str = "path, file_size_bytes, column_id, index_path, index_key, \
     index_size_bytes, format_version, value_type, row_count, row_group_rows, heavy_hitters, \
     built_by, bucket_counts";

/// The parts of one row as read, in [`SELECT_COLUMNS`] order.
struct Parts {
    path: String,
    file_size_bytes: i64,
    column_id: i64,
    index_path: String,
    index_key: String,
    index_size_bytes: i64,
    format_version: i64,
    value_type: String,
    row_count: i64,
    row_group_rows: String,
    heavy_hitters: String,
    built_by: String,
    bucket_counts: Option<String>,
}

impl Parts {
    fn read<R: sqlx::Row>(row: &R) -> std::result::Result<Self, sqlx::Error>
    where
        for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
        for<'r> Option<String>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
        for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
        for<'r> i32: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
        usize: sqlx::ColumnIndex<R>,
    {
        Ok(Self {
            path: row.try_get(0)?,
            file_size_bytes: row.try_get(1)?,
            column_id: row.try_get(2)?,
            index_path: row.try_get(3)?,
            index_key: row.try_get(4)?,
            index_size_bytes: row.try_get(5)?,
            format_version: row
                .try_get::<i32, _>(6)
                .map(i64::from)
                .or_else(|_| row.try_get::<i64, _>(6))?,
            value_type: row.try_get(7)?,
            row_count: row.try_get(8)?,
            row_group_rows: row.try_get(9)?,
            heavy_hitters: row.try_get(10)?,
            built_by: row.try_get(11)?,
            bucket_counts: row.try_get(12)?,
        })
    }

    /// `None` for a row this build cannot interpret (unknown value type,
    /// malformed JSON), which a reader treats like a missing row.
    fn row(self) -> Option<FileIndexRow> {
        let value_type = IndexValueType::parse(&self.value_type)?;
        Some(FileIndexRow {
            meta: FileIndexMeta {
                path: self.path,
                file_size_bytes: self.file_size_bytes,
                column_id: self.column_id,
                index_path: self.index_path,
                index_size_bytes: self.index_size_bytes,
                format_version: i32::try_from(self.format_version).ok()?,
                value_type,
                row_count: self.row_count,
                row_group_rows: row_group_rows_from_json(&self.row_group_rows).ok()?,
                heavy_hitters: heavy_hitters_from_json(&self.heavy_hitters, value_type).ok()?,
                // Malformed counts lose only the counts, never the index.
                bucket_counts: self
                    .bucket_counts
                    .as_deref()
                    .and_then(|counts| super::counts::bucket_counts_from_json(counts, value_type)),
            },
            index_key: self.index_key,
            built_by: self.built_by,
        })
    }
}

fn counts_json(row: &FileIndexRow) -> Option<String> {
    row.meta
        .bucket_counts
        .as_ref()
        .map(super::counts::bucket_counts_to_json)
}

/// Record `rows` of `table_id`'s files, in one transaction (SQLite) or one
/// statement (PostgreSQL).
pub async fn record(db: &CatalogDb, table_id: i64, rows: Vec<FileIndexRow>) -> Result<()> {
    let rows = crate::db::last_per_key(rows, |row| (row.meta.path.clone(), row.meta.column_id));
    if rows.is_empty() {
        return Ok(());
    }
    let unsupported = unsupported(db);
    run(db, move |pool| async move {
        match pool {
            #[cfg(feature = "sqlite")]
            CatalogPool::Sqlite(pool) => record_sqlite(&pool, table_id, &rows).await,
            #[cfg(feature = "postgres")]
            CatalogPool::Postgres(pool) => record_postgres(&pool, table_id, &rows).await,
            #[allow(unreachable_patterns)]
            _ => Err(unsupported),
        }
    })
    .await
}

#[cfg(feature = "sqlite")]
async fn record_sqlite(
    pool: &sqlx::SqlitePool,
    table_id: i64,
    rows: &[FileIndexRow],
) -> Result<()> {
    {
        upsert_creating(pool, SQLITE_DDL, || async {
            let mut tx = pool.begin().await?;
            for row in rows {
                let meta = &row.meta;
                sqlx::query(
                    "INSERT INTO ext_ducklake_file_index
                         (table_id, path, file_size_bytes, column_id, index_path, index_key,
                          index_size_bytes, format_version, value_type, row_count,
                          row_group_rows, heavy_hitters, built_by, bucket_counts)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (table_id, path, column_id) DO UPDATE SET
                         file_size_bytes = excluded.file_size_bytes,
                         index_path = excluded.index_path,
                         index_key = excluded.index_key,
                         index_size_bytes = excluded.index_size_bytes,
                         format_version = excluded.format_version,
                         value_type = excluded.value_type,
                         row_count = excluded.row_count,
                         row_group_rows = excluded.row_group_rows,
                         heavy_hitters = excluded.heavy_hitters,
                         built_by = excluded.built_by,
                         bucket_counts = excluded.bucket_counts,
                         built_at = CURRENT_TIMESTAMP",
                )
                .bind(table_id)
                .bind(&meta.path)
                .bind(meta.file_size_bytes)
                .bind(meta.column_id)
                .bind(&meta.index_path)
                .bind(&row.index_key)
                .bind(meta.index_size_bytes)
                .bind(meta.format_version)
                .bind(meta.value_type.as_str())
                .bind(meta.row_count)
                .bind(row_group_rows_to_json(&meta.row_group_rows))
                .bind(heavy_hitters_to_json(&meta.heavy_hitters))
                .bind(&row.built_by)
                .bind(counts_json(row))
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await
        })
        .await
        .map_err(sql_error)
    }
}

#[cfg(feature = "postgres")]
async fn record_postgres(pool: &sqlx::PgPool, table_id: i64, rows: &[FileIndexRow]) -> Result<()> {
    {
        {
            let paths: Vec<&str> = rows.iter().map(|r| r.meta.path.as_str()).collect();
            let sizes: Vec<i64> = rows.iter().map(|r| r.meta.file_size_bytes).collect();
            let columns: Vec<i64> = rows.iter().map(|r| r.meta.column_id).collect();
            let index_paths: Vec<&str> = rows.iter().map(|r| r.meta.index_path.as_str()).collect();
            let index_keys: Vec<&str> = rows.iter().map(|r| r.index_key.as_str()).collect();
            let index_sizes: Vec<i64> = rows.iter().map(|r| r.meta.index_size_bytes).collect();
            let versions: Vec<i32> = rows.iter().map(|r| r.meta.format_version).collect();
            let types: Vec<&str> = rows.iter().map(|r| r.meta.value_type.as_str()).collect();
            let row_counts: Vec<i64> = rows.iter().map(|r| r.meta.row_count).collect();
            let groups: Vec<String> = rows
                .iter()
                .map(|r| row_group_rows_to_json(&r.meta.row_group_rows))
                .collect();
            let hitters: Vec<String> = rows
                .iter()
                .map(|r| heavy_hitters_to_json(&r.meta.heavy_hitters))
                .collect();
            let built_by: Vec<&str> = rows.iter().map(|r| r.built_by.as_str()).collect();
            let counts: Vec<Option<String>> = rows.iter().map(counts_json).collect();
            upsert_creating(pool, POSTGRES_DDL, || async {
                sqlx::query(
                    "INSERT INTO ext_ducklake_file_index
                         (table_id, path, file_size_bytes, column_id, index_path, index_key,
                          index_size_bytes, format_version, value_type, row_count,
                          row_group_rows, heavy_hitters, built_by, bucket_counts)
                     SELECT $1, u.path, u.size, u.col, u.ipath, u.ikey, u.isize, u.ver, u.vt,
                            u.rows, u.groups, u.hh, u.by, u.bc
                     FROM UNNEST($2::text[], $3::bigint[], $4::bigint[], $5::text[], $6::text[],
                                 $7::bigint[], $8::int[], $9::text[], $10::bigint[], $11::text[],
                                 $12::text[], $13::text[], $14::text[])
                          AS u(path, size, col, ipath, ikey, isize, ver, vt, rows, groups, hh,
                               by, bc)
                     ON CONFLICT (table_id, path, column_id) DO UPDATE SET
                         file_size_bytes = EXCLUDED.file_size_bytes,
                         index_path = EXCLUDED.index_path,
                         index_key = EXCLUDED.index_key,
                         index_size_bytes = EXCLUDED.index_size_bytes,
                         format_version = EXCLUDED.format_version,
                         value_type = EXCLUDED.value_type,
                         row_count = EXCLUDED.row_count,
                         row_group_rows = EXCLUDED.row_group_rows,
                         heavy_hitters = EXCLUDED.heavy_hitters,
                         built_by = EXCLUDED.built_by,
                         bucket_counts = EXCLUDED.bucket_counts,
                         built_at = now()",
                )
                .bind(table_id)
                .bind(&paths)
                .bind(&sizes)
                .bind(&columns)
                .bind(&index_paths)
                .bind(&index_keys)
                .bind(&index_sizes)
                .bind(&versions)
                .bind(&types)
                .bind(&row_counts)
                .bind(&groups)
                .bind(&hitters)
                .bind(&built_by)
                .bind(&counts)
                .execute(pool)
                .await
                .map(|_| ())
            })
            .await
            .map_err(sql_error)
        }
    }
}

/// The rows of `table_id`'s files at `paths` for `column_ids`. A row this
/// build cannot interpret is left out.
pub async fn read(
    db: &CatalogDb,
    table_id: i64,
    paths: Vec<String>,
    column_ids: Vec<i64>,
) -> Result<Vec<FileIndexRow>> {
    if paths.is_empty() || column_ids.is_empty() {
        return Ok(Vec::new());
    }
    run(db, move |pool| read_on(pool, table_id, paths, column_ids)).await
}

/// [`read`], on `pool`: the work [`CatalogDb::run`] runs.
pub(crate) async fn read_on(
    pool: CatalogPool,
    table_id: i64,
    paths: Vec<String>,
    column_ids: Vec<i64>,
) -> Result<Vec<FileIndexRow>> {
    if paths.is_empty() || column_ids.is_empty() {
        return Ok(Vec::new());
    }
    let parts: std::result::Result<Vec<Parts>, sqlx::Error> = match pool {
        #[cfg(feature = "sqlite")]
        CatalogPool::Sqlite(pool) => {
            let columns = vec!["?"; column_ids.len()].join(",");
            let mut out = Vec::new();
            let mut failed = None;
            // Bounded binds per statement (SQLite's limit is 32766).
            for chunk in paths.chunks(900) {
                let sql = format!(
                    "SELECT {SELECT_COLUMNS} FROM ext_ducklake_file_index \
                     WHERE table_id = ? AND path IN ({}) AND column_id IN ({columns})",
                    vec!["?"; chunk.len()].join(",")
                );
                let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(table_id);
                for path in chunk {
                    query = query.bind(path);
                }
                for column_id in &column_ids {
                    query = query.bind(*column_id);
                }
                match query.fetch_all(&pool).await {
                    Ok(rows) => {
                        for row in &rows {
                            out.push(Parts::read(row));
                        }
                    },
                    Err(e) => {
                        failed = Some(e);
                        break;
                    },
                }
            }
            match failed {
                Some(e) => Err(e),
                None => out.into_iter().collect(),
            }
        },
        #[cfg(feature = "postgres")]
        CatalogPool::Postgres(pool) => sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {SELECT_COLUMNS} FROM ext_ducklake_file_index
             WHERE table_id = $1 AND path = ANY($2) AND column_id = ANY($3)"
        )))
        .bind(table_id)
        .bind(&paths)
        .bind(&column_ids)
        .fetch_all(&pool)
        .await
        .and_then(|rows| rows.iter().map(Parts::read).collect()),
        #[allow(unreachable_patterns)]
        _ => {
            return Err(DuckLakeError::Unsupported(
                "extension tables on this catalog's backend".into(),
            ));
        },
    };
    match parts {
        Ok(parts) => Ok(parts.into_iter().filter_map(Parts::row).collect()),
        Err(e) if is_missing_table(&e) => Ok(Vec::new()),
        Err(e) => Err(sql_error(e)),
    }
}

/// Drop the rows recorded at least `min_age` ago whose data file no catalog
/// row lists any more; returns their index objects' store keys.
pub(crate) async fn prune(db: &CatalogDb, min_age: Duration) -> Result<Vec<String>> {
    let unsupported = unsupported(db);
    run(db, move |pool| prune_on(pool, min_age, unsupported)).await
}

async fn prune_on(
    pool: CatalogPool,
    min_age: Duration,
    unsupported: DuckLakeError,
) -> Result<Vec<String>> {
    // Any catalog row for the path keeps it: a retired file is still read by
    // time travel until expiry removes its row.
    let keys = match pool {
        #[cfg(feature = "sqlite")]
        CatalogPool::Sqlite(pool) => {
            sqlx::query_scalar::<_, String>(
                "DELETE FROM ext_ducklake_file_index
                 WHERE built_at <= ?
                   AND NOT EXISTS (
                     SELECT 1 FROM ducklake_data_file df
                     WHERE df.table_id = ext_ducklake_file_index.table_id
                       AND df.path = ext_ducklake_file_index.path)
                 RETURNING index_key",
            )
            .bind(crate::db::sqlite_cutoff(min_age))
            .fetch_all(&pool)
            .await
        },
        #[cfg(feature = "postgres")]
        CatalogPool::Postgres(pool) => {
            sqlx::query_scalar::<_, String>(
                "DELETE FROM ext_ducklake_file_index i
                 WHERE i.built_at <= now() - make_interval(secs => $1::double precision / 1e6)
                   AND NOT EXISTS (SELECT 1 FROM ducklake_data_file df
                                   WHERE df.table_id = i.table_id AND df.path = i.path)
                 RETURNING i.index_key",
            )
            .bind(i64::try_from(min_age.as_micros()).unwrap_or(i64::MAX))
            .fetch_all(&pool)
            .await
        },
        #[allow(unreachable_patterns)]
        _ => return Err(unsupported),
    };
    match keys {
        Ok(keys) => Ok(keys),
        Err(e) if is_missing_table(&e) => Ok(Vec::new()),
        Err(e) => Err(sql_error(e)),
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::file_index::format::IndexKey;

    async fn db() -> (CatalogDb, sqlx::SqlitePool) {
        use datafusion_ducklake::MetadataProvider;
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let db = datafusion_ducklake::SqliteMetadataProvider::from_pool(pool.clone())
            .catalog_db()
            .unwrap();
        (db, pool)
    }

    fn row(path: &str, column_id: i64) -> FileIndexRow {
        FileIndexRow {
            meta: FileIndexMeta {
                path: path.into(),
                file_size_bytes: 42,
                column_id,
                index_path: format!("{path}.c{column_id}.v1.pidx"),
                index_size_bytes: 9,
                format_version: 1,
                value_type: IndexValueType::Utf8,
                row_count: 3,
                row_group_rows: vec![2, 1],
                heavy_hitters: vec![(IndexKey::Utf8(b"x".to_vec().into()), 2)],
                bucket_counts: None,
            },
            index_key: format!("t/{path}.c{column_id}.v1.pidx"),
            built_by: "write".into(),
        }
    }

    #[tokio::test]
    async fn a_row_reads_back_as_recorded_and_a_later_one_replaces_it() {
        let (db, _pool) = db().await;
        let paths = vec!["a.parquet".to_string()];
        assert!(
            read(&db, 1, paths.clone(), vec![7])
                .await
                .unwrap()
                .is_empty(),
            "no table yet"
        );
        record(&db, 1, vec![row("a.parquet", 7), row("a.parquet", 8)])
            .await
            .unwrap();
        assert_eq!(
            read(&db, 1, paths.clone(), vec![7]).await.unwrap(),
            vec![row("a.parquet", 7)]
        );
        assert!(
            read(&db, 2, paths.clone(), vec![7])
                .await
                .unwrap()
                .is_empty(),
            "another table"
        );

        let mut rebuilt = row("a.parquet", 7);
        rebuilt.built_by = "backfill".into();
        record(&db, 1, vec![rebuilt.clone()]).await.unwrap();
        assert_eq!(
            read(&db, 1, paths.clone(), vec![7]).await.unwrap(),
            vec![rebuilt]
        );
    }

    #[tokio::test]
    async fn malformed_counts_lose_only_the_counts() {
        let (db, pool) = db().await;
        record(&db, 1, vec![row("a.parquet", 7)]).await.unwrap();
        sqlx::query("UPDATE ext_ducklake_file_index SET bucket_counts = '{\"v\":1'")
            .execute(&pool)
            .await
            .unwrap();
        let paths = vec!["a.parquet".to_string()];
        assert_eq!(
            read(&db, 1, paths.clone(), vec![7]).await.unwrap(),
            vec![row("a.parquet", 7)]
        );
        sqlx::query("UPDATE ext_ducklake_file_index SET value_type = 'nope'")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            read(&db, 1, paths.clone(), vec![7])
                .await
                .unwrap()
                .is_empty()
        );
    }
}
