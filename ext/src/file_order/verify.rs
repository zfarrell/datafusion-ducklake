//! Record the order of files written without the extension.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::compute::SortOptions;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion_ducklake::{BackfillOptions, DuckLakeError, DuckLakeTable, Result};

use super::check::{AttestedKey, encode};
use super::{VerifyOrder, recorded_orders};

/// Options for [`FileOrderTableExt::verify_file_orders`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct VerifyOrderOptions {
    /// Only these data files; `None` = every live file without a record.
    pub data_file_ids: Option<Vec<i64>>,
    /// Stop after reading this many files (bounds one pass); `None` = all.
    pub max_files: Option<usize>,
}

impl VerifyOrderOptions {
    /// Check only these data files.
    #[must_use]
    pub fn with_data_file_ids(mut self, data_file_ids: Vec<i64>) -> Self {
        self.data_file_ids = Some(data_file_ids);
        self
    }

    /// Stop after reading `max_files` files.
    #[must_use]
    pub fn with_max_files(mut self, max_files: usize) -> Self {
        self.max_files = Some(max_files);
        self
    }
}

/// What [`FileOrderTableExt::verify_file_orders`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct VerifyOrderResult {
    /// Files whose key columns were read and checked.
    pub files_verified: usize,
    /// Of those, files recorded as ordered.
    pub files_recorded: u64,
    /// Of those, files whose rows are NOT in sort order in the catalog's
    /// column types, or whose key values do not cast to them (nothing
    /// recorded).
    pub files_unsorted: usize,
    /// Files not read: already recorded under the current key, name-mapped,
    /// encrypted, a footer without the key columns, or a key column whose
    /// type does not cast to the catalog's.
    pub files_skipped: usize,
    /// Compressed bytes of the key column chunks read.
    pub bytes_read: u64,
}

/// [`DuckLakeTable`] methods of the file order extension.
#[async_trait]
pub trait FileOrderTableExt {
    /// Record the verified sort order of live data files that have no record
    /// under the table's current sort key: files written before the extension
    /// was on or under an older key, by another DuckLake client, uploaded
    /// parquet registered as is, or `ducklake_add_data_files`. The files are
    /// not rewritten and no snapshot is taken.
    ///
    /// Runs [`DuckLakeTable::backfill`]: per file, read only the sort-key
    /// columns (located by field id), in
    /// file order, through the same check the writer runs; if every row is in
    /// order, record the file's path, size and key. A record describes the
    /// object at that path, so it needs no snapshot and is true at every
    /// snapshot that lists the file. Concurrent maintenance is harmless: a
    /// record for a file compacted away meanwhile matches no live file and is
    /// dropped by [`super::prune_file_orders`]. Idempotent. Records are made
    /// once all candidates are read, so an error part way (a file vacuumed
    /// after listing, say) records nothing for the pass.
    async fn verify_file_orders(
        &self,
        state: &dyn Session,
        opts: VerifyOrderOptions,
    ) -> Result<VerifyOrderResult>;
}

#[async_trait]
impl FileOrderTableExt for DuckLakeTable {
    async fn verify_file_orders(
        &self,
        state: &dyn Session,
        opts: VerifyOrderOptions,
    ) -> Result<VerifyOrderResult> {
        if self.writer().is_none() {
            return Err(DuckLakeError::InvalidConfig(
                "verify_file_orders: table is read-only; open the catalog with a writer"
                    .to_string(),
            ));
        }
        let Some(spec) = self.live_sort_spec()? else {
            return Ok(VerifyOrderResult::default());
        };
        // Expression keys cannot be evaluated here, so they cannot be verified.
        let Some(keys) = spec.producible_columns() else {
            return Ok(VerifyOrderResult::default());
        };
        // Files already recorded under the current key are passed over; one
        // recorded under an older key is checked again under this one.
        let physical_schema = self.physical_schema();
        let column_ids = self.top_level_column_ids();
        let current_key = keys
            .iter()
            .map(|(name, direction, nulls)| {
                let index = physical_schema.index_of(name).ok()?;
                Some(AttestedKey {
                    field_id: i32::try_from(column_ids[index]).ok()?,
                    options: SortOptions {
                        descending: *direction == datafusion_ducklake::SortDirection::Desc,
                        nulls_first: *nulls == datafusion_ducklake::NullOrder::NullsFirst,
                    },
                })
            })
            .collect::<Option<Vec<_>>>()
            .map(|keys| encode(&keys));
        let files = self.data_files()?;
        let paths: Vec<&str> = files.iter().map(|tf| tf.file.path.as_str()).collect();
        let recorded: HashSet<(String, i64)> =
            recorded_orders(self.metadata_provider().as_ref(), self.table_id(), &paths)?
                .into_iter()
                .filter(|(_, _, sort_key)| current_key.as_ref() == Some(sort_key))
                .map(|(path, size, _)| (path, size))
                .collect();

        let mut backfill = BackfillOptions::default();
        if let Some(ids) = opts.data_file_ids {
            backfill = backfill.with_data_file_ids(ids);
        }
        if let Some(max) = opts.max_files {
            backfill = backfill.with_max_files(max);
        }
        let done = self
            .backfill(
                state,
                Arc::new(VerifyOrder {
                    recorded: Arc::new(recorded),
                }),
                backfill,
            )
            .await?;
        // A file whose key values do not cast to the catalog's types fails
        // the reader too: it was read, and is not in order.
        let files_verified = (done.files_read + done.files_failed) as usize;
        let files_recorded = done.facts.len() as u64;
        Ok(VerifyOrderResult {
            files_verified,
            files_recorded,
            files_unsorted: files_verified.saturating_sub(files_recorded as usize),
            files_skipped: done.files_skipped as usize,
            bytes_read: done.bytes_read,
        })
    }
}
