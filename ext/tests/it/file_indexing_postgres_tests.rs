//! Postgres multicatalog counterpart of `file_indexing_tests.rs`: the
//! `ext_ducklake_file_index` sidecar through `PostgresMetadataWriter` and
//! `MulticatalogProvider`. Docker-gated (testcontainers Postgres).

#![cfg(not(feature = "skip-tests-with-docker"))]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::*;
use datafusion_ducklake::extension::CatalogDb;
use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeReadOptions, DuckLakeTable, DuckLakeTableWriter, DuckLakeWriteOptions,
    MetadataWriter, MulticatalogManager, MulticatalogProvider, PostgresMetadataWriter,
};
use datafusion_ducklake_ext::file_index::format::{FileIndexMeta, IndexKey, IndexValueType};
use datafusion_ducklake_ext::file_index::table as index_table;
use datafusion_ducklake_ext::file_index::{
    BuildIndexOptions, BuildIndexResult, FileIndex, FileIndexRow, FileIndexTableExt,
    file_index_rows, prune_file_indexes,
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

    fn db(&self) -> CatalogDb {
        crate::postgres_db(&self.pool)
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
            .with_options(&options())
            .append_table("public", "logs", &[rows])
            .await
            .unwrap();
    }

    async fn table(&self) -> (SessionContext, DuckLakeTable) {
        let catalog =
            DuckLakeCatalog::with_writer(Arc::new(self.provider().await), self.writer().await)
                .unwrap()
                .with_read_options(
                    DuckLakeReadOptions::default().with_extension(Arc::new(FileIndex::new())),
                )
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

    async fn backfill(&self) -> BuildIndexResult {
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
        let mut metas: Vec<FileIndexMeta> =
            file_index_rows(&self.db(), self.table_id, &paths, &[column])
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.meta)
                .collect();
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
    // Two hot users over 3,000 rows: above the 1,024-row heavy-hitter floor.
    env.append(
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..3_000)),
                Arc::new(StringArray::from_iter_values((0..3_000).map(|i| {
                    if i % 2 == 0 {
                        "hot"
                    } else {
                        "warm"
                    }
                }))),
            ],
        )
        .unwrap(),
    )
    .await;
    env.backfill().await;
    let live = env.live().await;
    let metas = env.metas().await;
    // The creating write (one row) and the two appends.
    assert_eq!(metas.len(), 3, "{metas:?}");
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
    // 17 users of about 21 rows each: none is heavy.
    assert!(meta.heavy_hitters.is_empty());
    let hot = metas.iter().find(|m| m.row_count == 3_000).unwrap();
    assert_eq!(
        hot.heavy_hitters,
        vec![
            (IndexKey::Utf8(b"hot".to_vec().into()), 1_500),
            (IndexKey::Utf8(b"warm".to_vec().into()), 1_500),
        ],
        "heavy hitters round-trip through the catalog"
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
    let mut ghost = env.metas().await[0].clone();
    ghost.path = "never-committed.parquet".to_string();
    index_table::record(
        &env.db(),
        env.table_id,
        vec![FileIndexRow {
            meta: ghost,
            index_key: "ghost/key.pidx".to_string(),
            built_by: "write".to_string(),
        }],
    )
    .await
    .unwrap();
    let store = Arc::new(LocalFileSystem::new());
    assert!(
        prune_file_indexes(&env.db(), store.clone(), Duration::from_secs(3_600))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        prune_file_indexes(&env.db(), store, Duration::ZERO)
            .await
            .unwrap(),
        vec!["ghost/key.pidx".to_string()]
    );
    assert_eq!(env.metas().await.len(), 2, "live files keep their rows");
}

/// `bucket_counts` (W7) round-trips through the Postgres writer and the
/// multicatalog reader, and a row without counts reads back without them.
#[tokio::test(flavor = "multi_thread")]
async fn bucket_counts_round_trip() {
    use datafusion_ducklake_ext::file_index::format::{BUCKET_GRAIN_HOUR_US, BucketCounts};
    let env = Env::new().await;
    env.append(batch(0..350)).await;
    env.backfill().await;
    let mut metas = env.metas().await;
    assert!(metas.iter().all(|m| m.bucket_counts.is_none()));
    let h = BUCKET_GRAIN_HOUR_US;
    let counts = BucketCounts {
        ts_column_id: 99,
        grain_us: h,
        keys: vec![
            (
                IndexKey::Utf8(b"u0".to_vec().into()),
                vec![(-h, 3), (0, 18)],
            ),
            (IndexKey::Utf8(b"u1".to_vec().into()), vec![(5 * h, 21)]),
        ],
    };
    metas[0].bucket_counts = Some(counts.clone());
    let path = metas[0].path.clone();
    index_table::record(
        &env.db(),
        env.table_id,
        vec![FileIndexRow {
            meta: metas[0].clone(),
            index_key: "unused/key.pidx".to_string(),
            built_by: "backfill".to_string(),
        }],
    )
    .await
    .unwrap();
    let back = env.metas().await;
    let row = back.iter().find(|m| m.path == path).unwrap();
    assert_eq!(row.bucket_counts.as_ref(), Some(&counts));
    assert!(
        back.iter()
            .filter(|m| m.path != path)
            .all(|m| m.bucket_counts.is_none())
    );
}
