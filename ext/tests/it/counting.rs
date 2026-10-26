//! A provider that forwards every `MetadataProvider` method and counts the
//! calls: a copy of the parent crate's, in `catalog_read_memo_tests`.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use arrow::record_batch::RecordBatch;
use std::sync::{Arc, Mutex};
use datafusion_ducklake::inlined_filter::{InlinedDataScan, InlinedFilter};
use datafusion_ducklake::metadata_provider::{
    ColumnWithTable, DataFileChange, DeleteFileChange, DuckLakeFileMetadata, DuckLakeInlinedData,
    DuckLakeInlinedDelete, DuckLakeInlinedRows, DuckLakeNameMapping, DuckLakeStatistics,
    DuckLakeTableColumn, DuckLakeTableField, DuckLakeTableFile,
    FileWithTable, InlinedFileDeleteChange, InlinedRowSelection, SchemaMetadata, SnapshotMetadata,
    TableMetadata, TableWithSchema, ViewMetadata, ViewWithSchema,
};
use datafusion_ducklake::stats_filter::StatsFilter;
use datafusion_ducklake::{
    ColumnTag, DuckLakeInlinedFileDeleteForFlush,
    DuckLakeInlinedFlushData, DuckLakeTag, MetadataProvider, ObjectTag, Result, SnapshotChangeMetadata, TagTarget,
};


/// Something to run once, just before the next call of a provider method.
type Hook = (&'static str, Box<dyn FnOnce() + Send>);

/// Forwards every `MetadataProvider` method to `inner` and counts the calls.
#[derive(Clone)]
pub struct CountingProvider {
    inner: Arc<dyn MetadataProvider>,
    calls: Arc<Mutex<BTreeMap<&'static str, usize>>>,
    hook: Arc<Mutex<Option<Hook>>>,
}

impl std::fmt::Debug for CountingProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountingProvider")
            .field("calls", &self.calls)
            .finish_non_exhaustive()
    }
}

impl CountingProvider {
    pub fn new(inner: Arc<dyn MetadataProvider>) -> Self {
        Self {
            inner,
            calls: Arc::new(Mutex::new(BTreeMap::new())),
            hook: Arc::new(Mutex::new(None)),
        }
    }

    fn count(&self, method: &'static str) {
        *self.calls.lock().unwrap().entry(method).or_default() += 1;
        let mut hook = self.hook.lock().unwrap();
        if hook.as_ref().is_some_and(|(before, _)| *before == method) {
            let (_, run) = hook.take().unwrap();
            drop(hook);
            run();
        }
    }

    /// Run `run` once, just before the next call of `method`.
    fn before(&self, method: &'static str, run: impl FnOnce() + Send + 'static) {
        *self.hook.lock().unwrap() = Some((method, Box::new(run)));
    }

    /// The calls made since the last `take`, cleared.
    pub fn take(&self) -> BTreeMap<&'static str, usize> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }
}

impl MetadataProvider for CountingProvider {
    fn get_current_snapshot(&self) -> Result<i64> {
        self.count("get_current_snapshot");
        self.inner.get_current_snapshot()
    }
    fn get_data_path(&self) -> Result<String> {
        self.count("get_data_path");
        self.inner.get_data_path()
    }
    fn get_metadata_settings(
        &self,
        schema_id: Option<i64>,
        table_id: Option<i64>,
    ) -> Result<HashMap<String, String>> {
        self.count("get_metadata_settings");
        self.inner.get_metadata_settings(schema_id, table_id)
    }
    fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>> {
        self.count("list_snapshots");
        self.inner.list_snapshots()
    }
    fn list_snapshot_changes(&self) -> Result<Vec<SnapshotChangeMetadata>> {
        self.count("list_snapshot_changes");
        self.inner.list_snapshot_changes()
    }
    fn find_snapshot_by_commit_extra_info(&self, needle: &str) -> Result<Option<i64>> {
        self.count("find_snapshot_by_commit_extra_info");
        self.inner.find_snapshot_by_commit_extra_info(needle)
    }
    fn list_schemas(&self, snapshot_id: i64) -> Result<Vec<SchemaMetadata>> {
        self.count("list_schemas");
        self.inner.list_schemas(snapshot_id)
    }
    fn list_tables(&self, schema_id: i64, snapshot_id: i64) -> Result<Vec<TableMetadata>> {
        self.count("list_tables");
        self.inner.list_tables(schema_id, snapshot_id)
    }
    fn list_views(&self, schema_id: i64, snapshot_id: i64) -> Result<Vec<ViewMetadata>> {
        self.count("list_views");
        self.inner.list_views(schema_id, snapshot_id)
    }
    fn get_table_structure(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeTableColumn>> {
        self.count("get_table_structure");
        self.inner.get_table_structure(table_id, snapshot_id)
    }
    fn get_table_fields(&self, table_id: i64, snapshot_id: i64) -> Result<Vec<DuckLakeTableField>> {
        self.count("get_table_fields");
        self.inner.get_table_fields(table_id, snapshot_id)
    }
    fn get_name_mapping(&self, mapping_id: i64) -> Result<DuckLakeNameMapping> {
        self.count("get_name_mapping");
        self.inner.get_name_mapping(mapping_id)
    }
    fn get_table_files_for_select(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeTableFile>> {
        self.count("get_table_files_for_select");
        self.inner.get_table_files_for_select(table_id, snapshot_id)
    }
    fn get_partition_spec(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Option<datafusion_ducklake::PartitionSpec>> {
        self.count("get_partition_spec");
        self.inner.get_partition_spec(table_id, snapshot_id)
    }
    fn get_sort_spec(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Option<datafusion_ducklake::SortSpec>> {
        self.count("get_sort_spec");
        self.inner.get_sort_spec(table_id, snapshot_id)
    }
    fn get_table_statistics(&self, table_id: i64, snapshot_id: i64) -> Result<DuckLakeStatistics> {
        self.count("get_table_statistics");
        self.inner.get_table_statistics(table_id, snapshot_id)
    }
    fn get_table_summary_statistics(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<DuckLakeStatistics> {
        self.count("get_table_summary_statistics");
        self.inner
            .get_table_summary_statistics(table_id, snapshot_id)
    }
    fn file_facts(
        &self,
        extension: &str,
        table_id: i64,
        paths: &[&str],
        keys: Option<&[i64]>,
    ) -> Result<Vec<datafusion_ducklake::FileFact>> {
        self.count("file_facts");
        self.inner.file_facts(extension, table_id, paths, keys)
    }
    fn get_table_file_metadata_page(
        &self,
        table_id: i64,
        snapshot_id: i64,
        after_data_file_id: Option<i64>,
        limit: usize,
    ) -> Result<Vec<DuckLakeFileMetadata>> {
        self.count("get_table_file_metadata_page");
        self.inner
            .get_table_file_metadata_page(table_id, snapshot_id, after_data_file_id, limit)
    }
    fn get_table_file_metadata_page_filtered(
        &self,
        table_id: i64,
        snapshot_id: i64,
        after_data_file_id: Option<i64>,
        limit: usize,
        filter: Option<&StatsFilter>,
    ) -> Result<Vec<DuckLakeFileMetadata>> {
        // Counted apart when the catalog-side statistics filter is passed.
        self.count(if filter.is_some() {
            "get_table_file_metadata_page_filtered(filter)"
        } else {
            "get_table_file_metadata_page_filtered"
        });
        self.inner.get_table_file_metadata_page_filtered(
            table_id,
            snapshot_id,
            after_data_file_id,
            limit,
            filter,
        )
    }
    fn get_inlined_data(
        &self,
        table_id: i64,
        snapshot_id: i64,
        columns: &[DuckLakeTableColumn],
    ) -> Result<Vec<RecordBatch>> {
        self.count("get_inlined_data");
        self.inner.get_inlined_data(table_id, snapshot_id, columns)
    }
    fn scan_inlined_data(
        &self,
        table_id: i64,
        snapshot_id: i64,
        columns: &[DuckLakeTableColumn],
        filter: Option<&InlinedFilter>,
    ) -> Result<InlinedDataScan> {
        self.count("scan_inlined_data");
        self.inner
            .scan_inlined_data(table_id, snapshot_id, columns, filter)
    }
    fn scan_inlined_data_noting_absence(
        &self,
        table_id: i64,
        snapshot_id: i64,
        columns: &[DuckLakeTableColumn],
        filter: Option<&InlinedFilter>,
    ) -> Result<(InlinedDataScan, Option<i64>)> {
        self.count("scan_inlined_data_noting_absence");
        self.inner
            .scan_inlined_data_noting_absence(table_id, snapshot_id, columns, filter)
    }
    fn get_inlined_deletes(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeInlinedDelete>> {
        self.count("get_inlined_deletes");
        self.inner.get_inlined_deletes(table_id, snapshot_id)
    }
    fn get_delete_file_begin_snapshot(&self, delete_file_id: i64) -> Result<Option<i64>> {
        self.count("get_delete_file_begin_snapshot");
        self.inner.get_delete_file_begin_snapshot(delete_file_id)
    }
    fn get_inlined_data_with_row_ids(
        &self,
        table_id: i64,
        snapshot_id: i64,
        columns: &[DuckLakeTableColumn],
    ) -> Result<Vec<DuckLakeInlinedData>> {
        self.count("get_inlined_data_with_row_ids");
        self.inner
            .get_inlined_data_with_row_ids(table_id, snapshot_id, columns)
    }
    fn get_inlined_data_for_flush(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeInlinedFlushData>> {
        self.count("get_inlined_data_for_flush");
        self.inner.get_inlined_data_for_flush(table_id, snapshot_id)
    }
    fn get_inlined_file_deletes_for_flush(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DuckLakeInlinedFileDeleteForFlush>> {
        self.count("get_inlined_file_deletes_for_flush");
        self.inner
            .get_inlined_file_deletes_for_flush(table_id, snapshot_id)
    }
    fn read_inlined_rows(
        &self,
        table_id: i64,
        columns: &[DuckLakeTableColumn],
        selection: InlinedRowSelection,
        filter: Option<&InlinedFilter>,
    ) -> Result<Vec<DuckLakeInlinedRows>> {
        self.count("read_inlined_rows");
        self.inner
            .read_inlined_rows(table_id, columns, selection, filter)
    }
    fn get_inlined_file_deletes_between(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> Result<Vec<InlinedFileDeleteChange>> {
        self.count("get_inlined_file_deletes_between");
        self.inner
            .get_inlined_file_deletes_between(table_id, start_snapshot, end_snapshot)
    }
    fn get_table_row_count(&self, table_id: i64, snapshot_id: i64) -> Result<u64> {
        self.count("get_table_row_count");
        self.inner.get_table_row_count(table_id, snapshot_id)
    }
    fn get_schema_by_name(&self, name: &str, snapshot_id: i64) -> Result<Option<SchemaMetadata>> {
        self.count("get_schema_by_name");
        self.inner.get_schema_by_name(name, snapshot_id)
    }
    fn get_table_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> Result<Option<TableMetadata>> {
        self.count("get_table_by_name");
        self.inner.get_table_by_name(schema_id, name, snapshot_id)
    }
    fn get_view_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> Result<Option<ViewMetadata>> {
        self.count("get_view_by_name");
        self.inner.get_view_by_name(schema_id, name, snapshot_id)
    }
    fn table_exists(&self, schema_id: i64, name: &str, snapshot_id: i64) -> Result<bool> {
        self.count("table_exists");
        self.inner.table_exists(schema_id, name, snapshot_id)
    }
    fn get_tags(&self, target: TagTarget, snapshot_id: i64) -> Result<Vec<DuckLakeTag>> {
        self.count("get_tags");
        self.inner.get_tags(target, snapshot_id)
    }
    fn get_view_id_by_name(
        &self,
        schema_id: i64,
        name: &str,
        snapshot_id: i64,
    ) -> Result<Option<i64>> {
        self.count("get_view_id_by_name");
        self.inner.get_view_id_by_name(schema_id, name, snapshot_id)
    }
    fn list_all_tables(&self, snapshot_id: i64) -> Result<Vec<TableWithSchema>> {
        self.count("list_all_tables");
        self.inner.list_all_tables(snapshot_id)
    }
    fn list_all_views(&self, snapshot_id: i64) -> Result<Vec<ViewWithSchema>> {
        self.count("list_all_views");
        self.inner.list_all_views(snapshot_id)
    }
    fn list_all_columns(&self, snapshot_id: i64) -> Result<Vec<ColumnWithTable>> {
        self.count("list_all_columns");
        self.inner.list_all_columns(snapshot_id)
    }
    fn list_all_object_tags(&self, snapshot_id: i64) -> Result<Vec<ObjectTag>> {
        self.count("list_all_object_tags");
        self.inner.list_all_object_tags(snapshot_id)
    }
    fn list_all_column_tags(&self, snapshot_id: i64) -> Result<Vec<ColumnTag>> {
        self.count("list_all_column_tags");
        self.inner.list_all_column_tags(snapshot_id)
    }
    fn list_all_files(&self, snapshot_id: i64) -> Result<Vec<FileWithTable>> {
        self.count("list_all_files");
        self.inner.list_all_files(snapshot_id)
    }
    fn get_data_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> Result<Vec<DataFileChange>> {
        self.count("get_data_files_added_between_snapshots");
        self.inner
            .get_data_files_added_between_snapshots(table_id, start_snapshot, end_snapshot)
    }
    fn get_delete_files_added_between_snapshots(
        &self,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> Result<Vec<DeleteFileChange>> {
        self.count("get_delete_files_added_between_snapshots");
        self.inner
            .get_delete_files_added_between_snapshots(table_id, start_snapshot, end_snapshot)
    }
}
