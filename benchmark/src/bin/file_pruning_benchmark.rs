//! Benchmark measuring scan reduction with file pruning on multi-file tables.
//!
//! Creates a DuckLake table with 100 files (each containing non-overlapping value
//! ranges), then measures query times with filters that prune most files vs.
//! full-table scans. Run with:
//!
//!   cargo run -p ducklake-benchmark --bin file-pruning-benchmark --release

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use tempfile::TempDir;

const NUM_FILES: i32 = 100;
const ROWS_PER_FILE: i32 = 1000;
const WARMUP_ITERS: usize = 2;
const BENCH_ITERS: usize = 10;

fn setup_catalog(temp_dir: &TempDir) -> Result<()> {
    let catalog_path = temp_dir.path().join("bench.ducklake");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;
    conn.execute(
        &format!(
            "ATTACH 'ducklake:{}' AS ducklake (DATA_PATH '{}');",
            catalog_path.display(),
            data_path.display()
        ),
        [],
    )?;

    conn.execute(
        "CREATE TABLE ducklake.main.bench_table (id INT, value DOUBLE, label VARCHAR)",
        [],
    )?;

    // Insert data in NUM_FILES separate batches to create separate files.
    // File i contains ids [i*ROWS_PER_FILE .. (i+1)*ROWS_PER_FILE).
    for file_idx in 0..NUM_FILES {
        let start = file_idx * ROWS_PER_FILE;
        let end = start + ROWS_PER_FILE;
        conn.execute(
            &format!(
                "INSERT INTO ducklake.main.bench_table \
                 SELECT i, i * 1.5, 'label_' || (i % 10)::VARCHAR \
                 FROM generate_series({}, {}) t(i)",
                start,
                end - 1
            ),
            [],
        )?;
    }

    Ok(())
}

async fn create_context(temp_dir: &TempDir) -> Result<SessionContext> {
    let catalog_path = temp_dir.path().join("bench.ducklake");
    let provider = DuckdbMetadataProvider::new(catalog_path.to_str().unwrap())?;
    let catalog = DuckLakeCatalog::new(provider)?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    Ok(ctx)
}

async fn bench_query(ctx: &SessionContext, sql: &str, label: &str) -> Result<()> {
    // Warmup
    for _ in 0..WARMUP_ITERS {
        ctx.sql(sql).await?.collect().await?;
    }

    let mut durations = Vec::with_capacity(BENCH_ITERS);
    let mut total_rows = 0u64;

    for _ in 0..BENCH_ITERS {
        let start = Instant::now();
        let batches = ctx.sql(sql).await?.collect().await?;
        let elapsed = start.elapsed();
        durations.push(elapsed);
        total_rows = batches.iter().map(|b| b.num_rows() as u64).sum();
    }

    durations.sort();
    let median = durations[BENCH_ITERS / 2];
    let min = durations[0];
    let max = durations[BENCH_ITERS - 1];
    let mean: f64 = durations.iter().map(|d| d.as_secs_f64()).sum::<f64>() / BENCH_ITERS as f64;

    println!(
        "  {:<45} rows={:<8} min={:>8.2}ms  median={:>8.2}ms  mean={:>8.2}ms  max={:>8.2}ms",
        label,
        total_rows,
        min.as_secs_f64() * 1000.0,
        median.as_secs_f64() * 1000.0,
        mean * 1000.0,
        max.as_secs_f64() * 1000.0,
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let total_rows = (NUM_FILES * ROWS_PER_FILE) as u64;
    println!("File Pruning Benchmark");
    println!("======================");
    println!(
        "Setup: {} files, {} rows/file, {} total rows\n",
        NUM_FILES, ROWS_PER_FILE, total_rows
    );

    println!("Creating catalog with {} files...", NUM_FILES);
    let temp_dir = TempDir::new()?;
    setup_catalog(&temp_dir)?;
    println!("Setup complete.\n");

    let ctx = create_context(&temp_dir).await?;

    println!(
        "Benchmarks ({} iterations, {} warmup):",
        BENCH_ITERS, WARMUP_ITERS
    );

    // 1. Full table scan (no pruning)
    bench_query(
        &ctx,
        "SELECT COUNT(*) FROM ducklake.main.bench_table",
        "Full scan (COUNT *)",
    )
    .await?;

    // 2. Selective filter hitting 1 file out of 100
    bench_query(
        &ctx,
        &format!(
            "SELECT COUNT(*) FROM ducklake.main.bench_table WHERE id >= {} AND id < {}",
            50 * ROWS_PER_FILE,
            51 * ROWS_PER_FILE
        ),
        "1-file filter (1% selectivity)",
    )
    .await?;

    // 3. Equality filter hitting 1 row
    bench_query(
        &ctx,
        "SELECT id, value FROM ducklake.main.bench_table WHERE id = 42000",
        "Point lookup (id = 42000)",
    )
    .await?;

    // 4. Range filter hitting ~10 files
    bench_query(
        &ctx,
        &format!(
            "SELECT COUNT(*) FROM ducklake.main.bench_table WHERE id >= {} AND id < {}",
            20 * ROWS_PER_FILE,
            30 * ROWS_PER_FILE
        ),
        "10-file filter (10% selectivity)",
    )
    .await?;

    // 5. Out-of-range filter (all files pruned)
    bench_query(
        &ctx,
        &format!(
            "SELECT COUNT(*) FROM ducklake.main.bench_table WHERE id > {}",
            total_rows + 1000
        ),
        "Out-of-range (all files pruned)",
    )
    .await?;

    // 6. Aggregation with selective filter
    bench_query(
        &ctx,
        &format!(
            "SELECT AVG(value), MIN(id), MAX(id) FROM ducklake.main.bench_table WHERE id >= {} AND id < {}",
            50 * ROWS_PER_FILE,
            51 * ROWS_PER_FILE
        ),
        "Aggregation on 1 file",
    )
    .await?;

    // 7. Full table aggregation (no pruning)
    bench_query(
        &ctx,
        "SELECT AVG(value), MIN(id), MAX(id) FROM ducklake.main.bench_table",
        "Full table aggregation",
    )
    .await?;

    println!("\nDone.");
    Ok(())
}
