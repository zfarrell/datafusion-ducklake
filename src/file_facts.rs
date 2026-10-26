//! Storage for [`FileFact`](crate::extension::FileFact)s: one catalog table
//! shared by every extension.
//!
//! ```sql
//! CREATE TABLE ext_ducklake_file_fact (
//!     extension       VARCHAR NOT NULL,  -- DuckLakeExtension::name
//!     table_id        BIGINT  NOT NULL,
//!     path            VARCHAR NOT NULL,  -- ducklake_data_file.path
//!     fact_key        BIGINT  NOT NULL,  -- FileFact::key
//!     file_size_bytes BIGINT  NOT NULL,  -- must equal the listed file's size
//!     value           VARCHAR NOT NULL,  -- the extension's encoding
//!     recorded_at     TIMESTAMP NOT NULL,
//!     PRIMARY KEY (extension, table_id, path, fact_key)
//! )
//! ```
//!
//! It is outside the spec's `ducklake_*` namespace on purpose: DuckLake clients
//! (the DuckDB extension included) only touch the tables they name, so they
//! neither read nor disturb it, and no future spec table can collide with it.
//! It is created when a fact is first recorded, so a catalog that never records
//! one never gets it. Readers treat a missing table as no facts.

/// The table's name.
pub const TABLE: &str = "ext_ducklake_file_fact";

#[cfg(feature = "write-sqlite")]
pub(crate) const SQLITE_DDL: &str = "CREATE TABLE IF NOT EXISTS ext_ducklake_file_fact (
    extension VARCHAR NOT NULL,
    table_id BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    fact_key BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    value VARCHAR NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (extension, table_id, path, fact_key)
)";

#[cfg(feature = "write-postgres")]
pub(crate) const POSTGRES_DDL: &str = "CREATE TABLE IF NOT EXISTS ext_ducklake_file_fact (
    extension VARCHAR NOT NULL,
    table_id BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    fact_key BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    value VARCHAR NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (extension, table_id, path, fact_key)
)";

/// Whether `error` says the table does not exist (no fact was ever recorded in
/// this catalog), which reads as "no facts".
#[cfg(any(feature = "metadata-sqlite", feature = "multicatalog-postgres"))]
pub(crate) fn is_missing_table(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(db) => {
            db.code().as_deref() == Some("42P01") || db.message().contains("no such table")
        },
        _ => false,
    }
}
