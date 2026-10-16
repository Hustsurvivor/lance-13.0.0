// SPDX-License-Identifier: Apache-2.0

mod lineitem_common;

use std::path::PathBuf;
use std::time::Instant;

use clap::Parser;
use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance_core::metadata_metrics::collect;
use lineitem_common::{Result, export, metrics_enabled};
use serde_json::json;

#[derive(Parser)]
struct Args {
    #[arg(
        long,
        default_value = "/workspace/source/damlake_test/data/lineitem.lance"
    )]
    dataset: String,
    #[arg(
        long,
        default_value = "/workspace/source/damlake_test/results/scan_lineitem.jsonl"
    )]
    output: PathBuf,
    #[arg(long)]
    version: Option<u64>,
    #[arg(long)]
    filter: Option<String>,
    #[arg(long, default_value_t = 5)]
    iterations: usize,
    /// Also stream the data outside metadata collection and report its row count.
    #[arg(long)]
    execute: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.iterations == 0 {
        return Err("iterations must be > 0".into());
    }
    let enabled = metrics_enabled()?;
    for iteration in 0..args.iterations {
        let (result, record) = collect(enabled, "scan_lineitem", &args.dataset, async {
            let mut builder = DatasetBuilder::from_uri(&args.dataset);
            if let Some(version) = args.version {
                builder = builder.with_version(version);
            }
            let dataset = builder.load().await?;
            let mut scanner = dataset.scan();
            scanner.use_scalar_index(false);
            if let Some(filter) = &args.filter {
                scanner.filter(filter)?;
            }
            let _plan = scanner.create_plan().await?;
            Ok::<_, lance::Error>((dataset, scanner))
        })
        .await;
        let mut extra = json!({"iteration": iteration, "requested_version": args.version,
            "filter": args.filter, "cache_policy": "fresh_session_per_read_os_cache_uncontrolled",
            "scope": "dataset_open_and_scan_planning_only"});
        // Export metadata even if optional data validation subsequently fails.
        if let Err(error) = &result {
            extra["error"] = json!(error.to_string());
        }
        export(
            &args.output,
            record,
            result.as_ref().ok().map(|(dataset, _)| dataset),
            extra,
        )?;
        let (dataset, scanner) = result?;
        if args.execute {
            let started = Instant::now();
            let mut stream = scanner.try_into_stream().await?;
            let mut rows = 0_u64;
            while let Some(batch) = stream.try_next().await? {
                rows += batch.num_rows() as u64;
            }
            eprintln!(
                "iteration={iteration} version={} rows={rows} scan_execution_us={:.3} (outside metadata timing)",
                dataset.version_id(),
                started.elapsed().as_secs_f64() * 1e6
            );
        }
    }
    Ok(())
}
