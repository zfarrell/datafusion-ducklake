//! Experimental extensions for `datafusion-ducklake`, built only on its
//! unstable extension API ([`datafusion_ducklake::extension`]).
//!
//! - [`file_order`]: verifies the sort order of each written file and declares
//!   it to the planner, so `ORDER BY <sort key> LIMIT n` can stop early.
//! - [`file_index`]: a per-file secondary index for `col = lit` and
//!   `col IN (...)` filters, and hourly counts served from it.
//!
//! Each keeps its own table in the catalog's database, which core hands it
//! (`datafusion_ducklake::extension::CatalogDb`): SQLite and PostgreSQL
//! catalogs only. Neither is part of the DuckLake spec. Other DuckLake clients
//! ignore what they record, and a file they write simply has no record.

#[cfg(not(any(feature = "sqlite", feature = "postgres")))]
compile_error!("datafusion-ducklake-ext needs the `sqlite` or `postgres` feature");

pub(crate) mod db;
pub mod file_index;
pub mod file_order;
