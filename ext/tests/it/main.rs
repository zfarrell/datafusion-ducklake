//! Integration tests of the extensions, through `datafusion-ducklake`'s
//! public API only. One binary, as in the parent crate.

mod counting;
mod file_index_counts_tests;
mod file_index_read_tests;
mod file_indexing_postgres_tests;
mod file_indexing_tests;
mod file_ordering_postgres_tests;
mod file_ordering_tests;
