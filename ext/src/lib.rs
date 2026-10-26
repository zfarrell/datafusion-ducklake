//! Experimental extensions for `datafusion-ducklake`, built only on its public
//! extension API ([`datafusion_ducklake::extension`]).
//!
//! - [`file_order`]: verifies the sort order of each written file and declares
//!   it to the planner, so `ORDER BY <sort key> LIMIT n` can stop early.
//! - [`file_index`]: a per-file secondary index for `col = lit` and
//!   `col IN (...)` filters, and hourly counts served from it.
//!
//! Neither is part of the DuckLake spec. Other DuckLake clients ignore what
//! they record, and a file they write simply has no record.

pub mod file_index;
pub mod file_order;
