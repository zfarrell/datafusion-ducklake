//! Integration tests of the extensions, through `datafusion-ducklake`'s
//! public API only. One binary, as in the parent crate.

mod file_ordering_postgres_tests;
mod file_ordering_tests;
mod runtime_tests;

use datafusion_ducklake::MetadataProvider;
use datafusion_ducklake::extension::CatalogDb;

/// The database of the SQLite catalog `pool` opens, as core hands it out.
pub fn sqlite_db(pool: &sqlx::SqlitePool) -> CatalogDb {
    datafusion_ducklake::SqliteMetadataProvider::from_pool(pool.clone())
        .catalog_db()
        .unwrap()
}

/// The database of the PostgreSQL catalog `pool` opens, as core hands it out.
pub fn postgres_db(pool: &sqlx::PgPool) -> CatalogDb {
    datafusion_ducklake::PostgresMetadataProvider::from_pool(pool.clone())
        .catalog_db()
        .unwrap()
}
