//! Record the order of files written without the extension.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::compute::SortOptions;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion_ducklake::{BackfillOptions, DuckLakeError, DuckLakeTable, Result};

use super::check::{AttestedKey, encode};
use super::{FileOrder, VerifyOrder, VerifyPass, table};

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
    /// Files whose key columns were read and checked to the end.
    pub files_verified: usize,
    /// Of those, files recorded as ordered.
    pub files_recorded: u64,
    /// Of those, files whose rows are NOT in sort order in the catalog's
    /// column types (nothing recorded).
    pub files_unsorted: usize,
    /// Files not read: already recorded under the current key, name-mapped,
    /// encrypted, a footer without the key columns, or a key column whose
    /// type does not cast to the catalog's.
    pub files_skipped: usize,
    /// Files whose read failed, whose key values did not cast to the
    /// catalog's types, or whose record could not be kept (warned; a later
    /// pass retries them).
    pub files_failed: usize,
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
    /// dropped by [`super::prune_file_orders`]. Idempotent. Each file is
    /// recorded as soon as it is checked, so an interrupted pass keeps its
    /// progress.
    ///
    /// Records go to the [`FileOrder`] registered in the table's read
    /// options (`DuckLakeCatalog::with_extension` registers it there).
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
        let Some(order) = self.read_options().extensions.get::<FileOrder>() else {
            return Err(DuckLakeError::InvalidConfig(
                "verify_file_orders: no FileOrder registered on the table's read options"
                    .to_string(),
            ));
        };
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
        let paths: Vec<String> = self
            .data_files()?
            .into_iter()
            .map(|tf| tf.file.path)
            .collect();
        let recorded: HashSet<(String, i64)> = table::read(order.db(), self.table_id(), &paths)
            .await?
            .into_iter()
            .filter(|row| current_key.as_ref() == Some(&row.sort_key))
            .map(|row| (row.path, row.file_size_bytes))
            .collect();

        let mut backfill = BackfillOptions::default();
        if let Some(ids) = opts.data_file_ids {
            backfill = backfill.with_data_file_ids(ids);
        }
        if let Some(max) = opts.max_files {
            backfill = backfill.with_max_files(max);
        }
        let pass = Arc::new(VerifyPass {
            recorded,
            ..Default::default()
        });
        let done = self
            .backfill(
                state,
                Arc::new(VerifyOrder {
                    shared: Arc::clone(&order.0),
                    pass: Arc::clone(&pass),
                }),
                backfill,
            )
            .await?;
        let files_verified = done.files_read as usize;
        let files_recorded = pass
            .files_recorded
            .load(std::sync::atomic::Ordering::Relaxed);
        Ok(VerifyOrderResult {
            files_verified,
            files_recorded,
            files_unsorted: files_verified.saturating_sub(files_recorded as usize),
            files_skipped: done.files_skipped as usize,
            files_failed: done.files_failed as usize,
            bytes_read: done.bytes_read,
        })
    }
}
