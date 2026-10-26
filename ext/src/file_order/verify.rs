//! Record the order of files written without the extension.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_ducklake::{DuckLakeError, DuckLakeTable, Result};
use futures::StreamExt;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::SortingColumn;

use super::check::{AttestedKey, SortCheck, encode};
use super::{BY_VERIFY, NAME, order_fact, recorded_orders};

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
    /// Per file: read only the sort-key columns (located by field id), in
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
        let mut result = VerifyOrderResult::default();
        let writer = self.writer().ok_or_else(|| {
            DuckLakeError::InvalidConfig(
                "verify_file_orders: table is read-only; open the catalog with a writer"
                    .to_string(),
            )
        })?;
        let Some(spec) = self.live_sort_spec()? else {
            return Ok(result);
        };
        // Expression keys cannot be evaluated here, so they cannot be verified.
        let Some(keys) = spec.producible_columns() else {
            return Ok(result);
        };
        let physical_schema = self.physical_schema();
        let column_ids = self.top_level_column_ids();
        let mut key_ids: Vec<(i64, SortOptions)> = Vec::with_capacity(keys.len());
        // Each key's catalog type: the reader casts the file's column to it.
        let mut key_types: Vec<DataType> = Vec::with_capacity(keys.len());
        for (name, direction, nulls) in &keys {
            let Ok(index) = physical_schema.index_of(name) else {
                return Ok(result);
            };
            key_types.push(physical_schema.field(index).data_type().clone());
            key_ids.push((
                column_ids[index],
                SortOptions {
                    descending: *direction == datafusion_ducklake::SortDirection::Desc,
                    nulls_first: *nulls == datafusion_ducklake::NullOrder::NullsFirst,
                },
            ));
        }

        let object_store = state
            .runtime_env()
            .object_store(self.object_store_url().as_ref())?;
        let readers = DefaultParquetFileReaderFactory::new(Arc::clone(&object_store));
        let reader_metrics = ExecutionPlanMetricsSet::new();
        let selected = opts
            .data_file_ids
            .map(|ids| ids.into_iter().collect::<HashSet<_>>());
        let candidates: Vec<_> = self
            .data_files()?
            .into_iter()
            .filter(|tf| {
                selected
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&tf.data_file_id))
            })
            .collect();
        // Files already recorded under the current key are skipped; one
        // recorded under an older key is checked again under this one.
        let current_key = key_ids
            .iter()
            .map(|(id, options)| {
                Some(AttestedKey {
                    field_id: i32::try_from(*id).ok()?,
                    options: *options,
                })
            })
            .collect::<Option<Vec<_>>>()
            .map(|keys| encode(&keys));
        let paths: Vec<&str> = candidates.iter().map(|tf| tf.file.path.as_str()).collect();
        let recorded: HashSet<(String, i64)> =
            recorded_orders(self.metadata_provider().as_ref(), self.table_id(), &paths)?
                .into_iter()
                .filter(|(_, _, sort_key)| current_key.as_ref() == Some(sort_key))
                .map(|(path, size, _)| (path, size))
                .collect();

        let mut facts = Vec::new();
        for tf in candidates {
            if opts
                .max_files
                .is_some_and(|max| result.files_verified >= max)
            {
                break;
            }
            if recorded.contains(&(tf.file.path.clone(), tf.file.file_size_bytes))
                || tf.file.mapping_id.is_some()
                || tf
                    .file
                    .encryption_key
                    .as_deref()
                    .is_some_and(|k| !k.is_empty())
            {
                result.files_skipped += 1;
                continue;
            }
            let source = self.data_file_location(&tf.file)?;
            let size = u64::try_from(tf.file.file_size_bytes).map_err(|_| {
                DuckLakeError::Internal(format!("negative file size for {}", tf.file.path))
            })?;
            let open = || {
                readers.create_reader(
                    0,
                    PartitionedFile::new(source.to_string(), size),
                    datafusion_ducklake::table::metadata_size_hint(tf.file.footer_size),
                    &reader_metrics,
                )
            };
            let footer = open()?.get_metadata(None).await?;
            let reader_metadata =
                ArrowReaderMetadata::try_new(Arc::clone(&footer), Default::default())?;
            let file_schema = reader_metadata.schema().clone();
            let field_id = |field: &Field| {
                field
                    .metadata()
                    .get("PARQUET:field_id")
                    .and_then(|id| id.parse::<i64>().ok())
            };
            // By field id; a file with no field ids at all (an external or
            // pre-DuckLake upload) by name, as the reader resolves it.
            let by_name = file_schema.fields().iter().all(|f| field_id(f).is_none());
            let roots: Option<Vec<usize>> = key_ids
                .iter()
                .map(|(id, _)| {
                    if by_name {
                        let index = column_ids.iter().position(|c| c == id)?;
                        file_schema
                            .index_of(physical_schema.field(index).name())
                            .ok()
                    } else {
                        file_schema
                            .fields()
                            .iter()
                            .position(|f| field_id(f) == Some(*id))
                    }
                })
                .collect();
            let Some(roots) = roots else {
                result.files_skipped += 1;
                continue;
            };
            let mut sorted_roots = roots.clone();
            sorted_roots.sort_unstable();
            sorted_roots.dedup();
            let descr = footer.file_metadata().schema_descr();
            result.bytes_read += footer
                .row_groups()
                .iter()
                .flat_map(|rg| rg.columns().iter().enumerate())
                .filter(|(leaf, _)| sorted_roots.contains(&descr.get_column_root_idx(*leaf)))
                .map(|(_, column)| column.compressed_size().max(0) as u64)
                .sum::<u64>();
            let mask = ProjectionMask::roots(descr, sorted_roots.clone());
            let key_reader: Box<dyn AsyncFileReader> = open()?;
            let mut stream =
                ParquetRecordBatchStreamBuilder::new_with_metadata(key_reader, reader_metadata)
                    .with_projection(mask)
                    .build()?;
            let requested: Vec<SortingColumn> = roots
                .iter()
                .zip(&key_ids)
                .map(|(root, (_, options))| SortingColumn {
                    column_idx: sorted_roots.binary_search(root).unwrap() as i32,
                    descending: options.descending,
                    nulls_first: options.nulls_first,
                })
                .collect();
            // Check the rows as the reader sees them, in the catalog's type.
            // An order in the file's own type need not survive the cast: ISO
            // text with mixed UTC offsets read as a TIMESTAMP, for one.
            let file_schema = stream.schema();
            let catalog_typed: Option<Vec<Field>> = sorted_roots
                .iter()
                .zip(file_schema.fields())
                .map(|(root, field)| {
                    let key = roots.iter().position(|r| r == root)?;
                    let target = &key_types[key];
                    arrow::compute::can_cast_types(field.data_type(), target)
                        .then(|| field.as_ref().clone().with_data_type(target.clone()))
                })
                .collect();
            let catalog_ids: Option<Vec<i32>> = key_ids
                .iter()
                .map(|(id, _)| i32::try_from(*id).ok())
                .collect();
            let checked = catalog_typed.and_then(|fields| {
                let schema = Arc::new(Schema::new(fields));
                let check = SortCheck::with_field_ids(schema.as_ref(), &requested, &catalog_ids?)?;
                Some((schema, check))
            });
            let Some((catalog_schema, mut check)) = checked else {
                result.files_skipped += 1;
                continue;
            };
            result.files_verified += 1;
            let mut castable = true;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let columns = batch
                    .columns()
                    .iter()
                    .zip(catalog_schema.fields())
                    .map(|(column, field)| {
                        arrow::compute::cast_with_options(
                            column,
                            field.data_type(),
                            &datafusion::common::format::DEFAULT_CAST_OPTIONS,
                        )
                    })
                    .collect::<std::result::Result<Vec<_>, _>>();
                // A value the cast refuses fails the reader too: not verified.
                let Ok(columns) = columns else {
                    castable = false;
                    break;
                };
                check.observe(&RecordBatch::try_new(Arc::clone(&catalog_schema), columns)?)?;
            }
            if !castable {
                result.files_unsorted += 1;
                continue;
            }
            match check.attestation() {
                Some(sort_key) => facts.push(order_fact(
                    &tf.file.path,
                    tf.file.file_size_bytes,
                    &sort_key,
                    BY_VERIFY,
                )),
                None => result.files_unsorted += 1,
            }
        }
        result.files_recorded = writer.record_file_facts(NAME, self.table_id(), &facts)?;
        Ok(result)
    }
}
