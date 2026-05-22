//! # DataFusion-DuckLake
//!
//! A DataFusion extension that adds support for DuckLake, an integrated data lake and catalog format.
//!
//! ## Overview
//!
//! DuckLake uses:
//! - **Catalog Database**: SQL database (DuckDB, SQLite, PostgreSQL, MySQL) storing metadata as SQL tables
//! - **Data Storage**: Apache Parquet files stored on disk/object storage
//!
//! This extension provides read-only access to DuckLake catalogs through DataFusion's
//! catalog and table provider interfaces.
//!
//! ## Example: Read-only access
//!
//! ```no_run
//! # async fn example() -> datafusion_ducklake::Result<()> {
//! use datafusion::prelude::*;
//! use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
//!
//! // Create a DataFusion session context
//! let ctx = SessionContext::new();
//!
//! // Create a DuckDB metadata provider
//! let provider = DuckdbMetadataProvider::new("path/to/catalog.ducklake")?;
//!
//! // Register a DuckLake catalog with the provider
//! let catalog = DuckLakeCatalog::new(provider)?;
//! ctx.register_catalog("ducklake", std::sync::Arc::new(catalog));
//!
//! // Query tables from the catalog
//! let df = ctx.sql("SELECT * FROM ducklake.main.my_table").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Example: SQL `DELETE` / `UPDATE`
//!
//! DataFusion natively plans `INSERT` against a `TableProvider`, but it does
//! **not** natively plan `DELETE` or `UPDATE`. To enable those statements,
//! install [`DuckLakeQueryPlanner`] on the [`SessionStateBuilder`]:
//!
//! ```no_run
//! # #[cfg(all(feature = "write", feature = "write-sqlite", feature = "metadata-sqlite"))]
//! # async fn example() -> datafusion_ducklake::Result<()> {
//! use std::sync::Arc;
//! use datafusion::prelude::*;
//! use datafusion::execution::session_state::SessionStateBuilder;
//! use datafusion_ducklake::{
//!     DuckLakeCatalog, DuckLakeQueryPlanner, SqliteMetadataProvider, SqliteMetadataWriter,
//! };
//!
//! let provider = Arc::new(SqliteMetadataProvider::new("sqlite:catalog.db").await?);
//! let writer = Arc::new(SqliteMetadataWriter::new("sqlite:catalog.db?mode=rwc").await?);
//! let catalog = DuckLakeCatalog::with_writer(provider, writer)?;
//!
//! // Install the DuckLake query planner so DELETE/UPDATE are routed to DuckLake execs.
//! let state = DuckLakeQueryPlanner::register_into(
//!     SessionStateBuilder::new().with_default_features(),
//! )
//! .build();
//! let ctx = SessionContext::new_with_state(state);
//! ctx.register_catalog("ducklake", Arc::new(catalog));
//!
//! ctx.sql("DELETE FROM ducklake.main.my_table WHERE id = 1").await?.collect().await?;
//! ctx.sql("UPDATE ducklake.main.my_table SET name = 'x' WHERE id = 2").await?.collect().await?;
//! # Ok(())
//! # }
//! ```

pub mod catalog;
pub mod cdc_common;
pub mod column_rename;
pub mod delete_filter;
pub(crate) mod dialect;
pub mod encryption;
pub mod error;
pub mod information_schema;
pub mod metadata_provider;
pub mod parse_values;
pub mod path_resolver;
pub mod row_id;
pub mod schema;
pub mod table;
pub mod table_changes;
pub mod table_deletions;
pub mod table_functions;
pub mod table_insertions;
pub mod types;
pub mod virtual_column_exec;

// Shared provider macro (used by SQLite, PostgreSQL, MySQL providers)
#[cfg(any(feature = "metadata-sqlite", feature = "metadata-postgres", feature = "metadata-mysql"))]
pub(crate) mod metadata_provider_impl;

// Metadata providers (feature-gated)
#[cfg(feature = "metadata-duckdb")]
pub mod metadata_provider_duckdb;
#[cfg(feature = "metadata-mysql")]
pub mod metadata_provider_mysql;
#[cfg(feature = "metadata-postgres")]
pub mod metadata_provider_postgres;
#[cfg(feature = "metadata-sqlite")]
pub mod metadata_provider_sqlite;

// Shared writer macros (used by SQLite, PostgreSQL, MySQL writers)
#[cfg(any(feature = "write-sqlite", feature = "write-postgres", feature = "write-mysql"))]
pub(crate) mod metadata_writer_impl;

// Write support (feature-gated)
#[cfg(feature = "write")]
pub mod delete_exec;
#[cfg(feature = "write")]
pub mod insert_exec;
#[cfg(feature = "write")]
pub mod merge_exec;
#[cfg(feature = "write")]
pub mod metadata_writer;
#[cfg(feature = "write-mysql")]
pub mod metadata_writer_mysql;
#[cfg(feature = "write-postgres")]
pub mod metadata_writer_postgres;
#[cfg(feature = "write-sqlite")]
pub mod metadata_writer_sqlite;
#[cfg(feature = "write")]
pub(crate) mod metadata_writer_validation;
#[cfg(feature = "write")]
pub mod query_planner;
#[cfg(feature = "write")]
pub mod table_writer;
#[cfg(feature = "write")]
pub mod update_exec;

// Result type for DuckLake operations
pub type Result<T> = std::result::Result<T, DuckLakeError>;

// Re-export main types for convenience
pub use catalog::DuckLakeCatalog;
pub use error::DuckLakeError;
pub use metadata_provider::MetadataProvider;
pub use schema::DuckLakeSchema;
pub use table::DuckLakeTable;
pub use table_functions::register_ducklake_functions;
pub use virtual_column_exec::{
    VIRTUAL_COL_FILE_INDEX, VIRTUAL_COL_FILE_ROW_NUMBER, VIRTUAL_COL_FILENAME, VIRTUAL_COL_ROWID,
    VIRTUAL_COL_SNAPSHOT_ID, VirtualColumnExec, VirtualColumnFileInfo, VirtualColumnSet,
};

// Re-export metadata providers (feature-gated)
#[cfg(feature = "metadata-duckdb")]
pub use metadata_provider_duckdb::DuckdbMetadataProvider;
#[cfg(feature = "metadata-mysql")]
pub use metadata_provider_mysql::MySqlMetadataProvider;
#[cfg(feature = "metadata-postgres")]
pub use metadata_provider_postgres::PostgresMetadataProvider;
#[cfg(feature = "metadata-sqlite")]
pub use metadata_provider_sqlite::SqliteMetadataProvider;

// Re-export write types (feature-gated)
#[cfg(feature = "write")]
pub use delete_exec::DuckLakeDeleteExec;
#[cfg(feature = "write")]
pub use insert_exec::{DuckLakeInsertExec, PartitionTransform};
#[cfg(feature = "write")]
pub use merge_exec::{DuckLakeMergeExec, MergeMatchedAction};
#[cfg(feature = "write")]
pub use metadata_writer::{
    ColumnDef, ColumnStatInfo, DataFileInfo, DeleteFileInfo, MetadataWriter, WriteMode,
    WriteResult, WriteSetupResult,
};
#[cfg(feature = "write-mysql")]
pub use metadata_writer_mysql::MySqlMetadataWriter;
#[cfg(feature = "write-postgres")]
pub use metadata_writer_postgres::PostgresMetadataWriter;
#[cfg(feature = "write-sqlite")]
pub use metadata_writer_sqlite::SqliteMetadataWriter;
#[cfg(feature = "write")]
pub use query_planner::DuckLakeQueryPlanner;
#[cfg(feature = "write")]
pub use table_writer::{
    DuckLakeTableWriter, DucklakeFlushInlinedDataFunction, TableWriteSession,
    cleanup_orphaned_files,
};
#[cfg(feature = "write")]
pub use update_exec::{DuckLakeUpdateExec, UpdateAssignment};
