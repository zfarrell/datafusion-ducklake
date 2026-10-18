//! Postgres multicatalog counterpart of `file_indexing_tests.rs`: the
//! `ext_ducklake_file_index` sidecar through `PostgresMetadataWriter` and
//! `MulticatalogProvider`. Docker-gated (testcontainers Postgres).

#![cfg(all(feature = "write-postgres", not(feature = "skip-tests-with-docker")))]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::*;
use datafusion_ducklake::ext_file_index::{FileIndexMeta, IndexKey, IndexValueType};
use datafusion_ducklake::file_indexing::FileIndexRow;
use datafusion_ducklake::metadata_provider::MetadataProvider;
use datafusion_ducklake::{
    BuildIndexOptions, DuckLakeCatalog, DuckLakeTable, DuckLakeTableWriter, DuckLakeWriteOptions,
    MetadataWriter, MulticatalogManager, MulticatalogProvider, PostgresMetadataWriter,
};
use object_store::local::LocalFileSystem;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tempfile::TempDir;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("user_id", DataType::Utf8, true),
    ]))
}

fn batch(range: std::ops::Range<i64>) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(range.clone())),
            Arc::new(StringArray::from_iter_values(
                range.map(|i| format!("u{}", i % 17)),
            )),
        ],
    )
    .unwrap()
}

fn options() -> DuckLakeWriteOptions {
    let mut options = DuckLakeWriteOptions::default().with_data_inlining_row_limit(0);
    options.max_row_group_rows = Some(100);
    options
}

struct Env {
    pool: PgPool,
    catalog_id: i64,
    table_id: i64,
    _data: TempDir,
    _container: ContainerAsync<Postgres>,
}

impl Env {
    async fn new() -> Self {
        let container = Postgres::default().start().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgresql://postgres:postgres@127.0.0.1:{port}/postgres"
            ))
            .await
            .unwrap();
        datafusion_ducklake::initialize_multicatalog_schema(&pool)
            .await
            .unwrap();
        let catalog_id = MulticatalogManager::new(pool.clone())
            .create_catalog("cat")
            .await
            .unwrap();
        let data = TempDir::new().unwrap();
        let mut env = Self {
            pool,
            catalog_id,
            table_id: 0,
            _data: data,
            _container: container,
        };
        let writer = env.writer().await;
        writer
            .set_data_path(env._data.path().to_str().unwrap())
            .unwrap();
        let created = DuckLakeTableWriter::new(writer.clone(), Arc::new(LocalFileSystem::new()))
            .unwrap()
            .with_options(&options())
            .write_table("public", "logs", &[batch(0..1)])
            .await
            .unwrap();
        env.table_id = created.table_id;
        writer
            .set_table_setting(env.table_id, "ext_file_index_columns", "user_id")
            .unwrap();
        env
    }

    async fn writer(&self) -> Arc<PostgresMetadataWriter> {
        Arc::new(
            PostgresMetadataWriter::with_pool(self.pool.clone(), self.catalog_id)
                .await
                .unwrap()
                .with_data_inlining_row_limit(0),
        )
    }

    async fn provider(&self) -> MulticatalogProvider {
        MulticatalogProvider::with_pool(self.pool.clone(), "cat")
            .await
            .unwrap()
    }

    async fn append(&self, rows: RecordBatch) {
        DuckLakeTableWriter::new(self.writer().await, Arc::new(LocalFileSystem::new()))
            .unwrap()
            .with_options(&options(index))
            .append_table("public", "logs", &[rows])
            .await
            .unwrap();
    }

    async fn table(&self) -> (SessionContext, DuckLakeTable) {
        let catalog =
            DuckLakeCatalog::with_writer(Arc::new(self.provider().await), self.writer().await)
                .unwrap()
                .with_write_options(options());
        let ctx = SessionContext::new();
        ctx.register_catalog("cat", Arc::new(catalog));
        let table = ctx
            .catalog("cat")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("logs")
            .await
            .unwrap()
            .unwrap();
        let table = (table.as_ref() as &dyn std::any::Any)
            .downcast_ref::<DuckLakeTable>()
            .unwrap()
            .clone();
        (ctx, table)
    }

    async fn backfill(&self) -> datafusion_ducklake::BuildIndexResult {
        let (ctx, table) = self.table().await;
        table
            .build_file_indexes(&ctx.state(), BuildIndexOptions::default())
            .await
            .unwrap()
    }

    /// Live `(path, size)` of the table.
    async fn live(&self) -> Vec<(String, i64)> {
        sqlx::query_as(
            "SELECT path, file_size_bytes FROM ducklake_data_file
             WHERE table_id = $1 AND end_snapshot IS NULL ORDER BY path",
        )
        .bind(self.table_id)
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    async fn user_id_column(&self) -> i64 {
        sqlx::query_scalar(
            "SELECT column_id FROM ducklake_column
             WHERE table_id = $1 AND column_name = 'user_id' AND end_snapshot IS NULL",
        )
        .bind(self.table_id)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn metas(&self) -> Vec<FileIndexMeta> {
        let live = self.live().await;
        let paths: Vec<&str> = live.iter().map(|(p, _)| p.as_str()).collect();
        let column = self.user_id_column().await;
        let provider = self.provider().await;
        let mut metas = provider
            .file_indexes(self.table_id, &paths, &[column])
            .unwrap();
        metas.sort_by(|a, b| a.path.cmp(&b.path));
        metas
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn backfilled_files_are_recorded_and_read_back() {
    let env = Env::new().await;
    // Before anything is recorded, the sidecar does not exist: no rows.
    assert!(env.metas().await.is_empty());
    env.append(batch(0..350)).await;
    env.backfill().await;
    let live = env.live().await;
    let metas = env.metas().await;
    // The creating write (one row) and the append.
    assert_eq!(metas.len(), 2, "{metas:?}");
    let meta = metas.iter().find(|m| m.row_count == 350).unwrap();
    let (path, size) = live
        .iter()
        .find(|(p, _)| *p == meta.path)
        .expect("row of a live file");
    assert_eq!(meta.file_size_bytes, *size);
    assert_eq!(meta.column_id, env.user_id_column().await);
    assert_eq!(meta.value_type, IndexValueType::Utf8);
    assert_eq!(meta.row_count, 350);
    assert_eq!(meta.row_group_rows, vec![100, 100, 100, 50]);
    assert_eq!(meta.format_version, 1);
    let name = path.rsplit('/').next().unwrap();
    assert_eq!(
        meta.index_path,
        format!("{name}.c{}.v1.pidx", meta.column_id)
    );
    // 17 users over 350 rows: each is above row_count / 4096.
    assert_eq!(meta.heavy_hitters.len(), 17);
    assert!(
        meta.heavy_hitters
            .contains(&(IndexKey::Utf8(b"u0".to_vec().into()), 21))
    );
    let key: String =
        sqlx::query_scalar("SELECT index_key FROM ext_ducklake_file_index WHERE path = $1")
            .bind(&meta.path)
            .fetch_one(&env.pool)
            .await
            .unwrap();
    let bytes = std::fs::read(format!("/{key}")).unwrap();
    assert_eq!(meta.index_size_bytes, bytes.len() as i64);
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_then_prune() {
    let env = Env::new().await;
    env.append(batch(0..200)).await;
    let (ctx, table) = env.table().await;
    let built = table
        .build_file_indexes(&ctx.state(), BuildIndexOptions::default())
        .await
        .unwrap();
    // The creating write (one row) and the append.
    assert_eq!(built.files_indexed, 2, "{built:?}");
    assert_eq!(env.metas().await.len(), 2);
    let again = table
        .build_file_indexes(&ctx.state(), BuildIndexOptions::default())
        .await
        .unwrap();
    assert_eq!(again.files_indexed, 0);
    assert_eq!(again.files_skipped, 2);

    // A row for a path no data file has: pruned only past the grace period.
    let writer = env.writer().await;
    let mut ghost = env.metas().await[0].clone();
    ghost.path = "never-committed.parquet".to_string();
    writer
        .record_file_indexes(
            env.table_id,
            "write",
            &[FileIndexRow {
                meta: ghost,
                index_key: "ghost/key.pidx".to_string(),
            }],
        )
        .unwrap();
    assert!(
        writer
            .prune_file_indexes(Duration::from_secs(3_600))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        writer.prune_file_indexes(Duration::ZERO).unwrap(),
        vec!["ghost/key.pidx".to_string()]
    );
    assert_eq!(env.metas().await.len(), 2, "live files keep their rows");
}
