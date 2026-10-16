// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Opt-in, operation-local table metadata measurements for experiments.
//!
//! Use [`collect`] around one complete operation and export its returned JSON
//! after timing ends. Ordinary calls without a collection scope do not allocate
//! timers or write output. Spawned tasks do not inherit Tokio task locals: use
//! [`inherit`] explicitly when moving measured work into another task.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::{Value, json};

#[derive(Default)]
struct Measurements {
    intervals: Vec<(&'static str, u64, u64)>,
}

struct Context {
    epoch: Instant,
    measurements: Mutex<Measurements>,
}

tokio::task_local! {
    static CONTEXT: Arc<Context>;
}

/// RAII interval, recorded even when its scope returns an error.
#[must_use]
pub struct Phase(Option<(Arc<Context>, &'static str, u64)>);

/// Start a table-metadata-only interval. Nested intervals are allowed.
///
/// ```
/// let timer = lance_core::metadata_metrics::phase("schema_metadata_processing");
/// // Perform table schema work here, without data-file IO.
/// drop(timer);
/// ```
pub fn phase(name: &'static str) -> Phase {
    Phase(
        CONTEXT
            .try_with(|context| {
                let context = Arc::clone(context);
                let start = context.epoch.elapsed().as_nanos() as u64;
                (context, name, start)
            })
            .ok(),
    )
}

impl Drop for Phase {
    fn drop(&mut self) {
        if let Some((context, name, start)) = self.0.take() {
            let end = context.epoch.elapsed().as_nanos() as u64;
            if let Ok(mut measurements) = context.measurements.lock() {
                measurements.intervals.push((name, start, end));
            }
        }
    }
}

/// Capture the current context before spawning a measured child future.
///
/// Await all children before finishing [`collect`]. Unscoped tasks are excluded.
/// ```
/// # async fn example() {
/// let child = lance_core::metadata_metrics::inherit(async {});
/// tokio::spawn(child).await.unwrap();
/// # }
/// ```
pub fn inherit<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let context = CONTEXT.try_with(Arc::clone).ok();
    async move {
        match context {
            Some(context) => CONTEXT.scope(context, future).await,
            None => future.await,
        }
    }
}

/// Measure one operation without performing output IO or changing its result.
///
/// `enabled = false` returns no record. Each enabled call creates a separate
/// context, including nested calls. Phase durations are inclusive; their sum is
/// not a total. `metadata_total_us` is the union of all marked intervals, not
/// CPU time and not a claim to cover uninstrumented work. Output is returned on
/// both success and error; cancellation drops timers but cannot return a record.
///
/// ```
/// # async fn example() {
/// let (result, record) = lance_core::metadata_metrics::collect(
///     true, "read_latest", "dataset", async { Ok::<_, ()>(()) }
/// ).await;
/// assert!(result.is_ok());
/// assert!(record.is_some());
/// # }
/// ```
pub async fn collect<T, E>(
    enabled: bool,
    operation: &str,
    dataset: &str,
    future: impl Future<Output = std::result::Result<T, E>>,
) -> (std::result::Result<T, E>, Option<Value>) {
    if !enabled {
        return (future.await, None);
    }
    let context = Arc::new(Context {
        epoch: Instant::now(),
        measurements: Mutex::new(Measurements::default()),
    });
    let result = CONTEXT.scope(Arc::clone(&context), future).await;
    let elapsed = context.epoch.elapsed();
    let Ok(measurements) = context.measurements.lock() else {
        tracing::warn!("Metadata measurements unavailable: poisoned collector");
        return (result, None);
    };
    let mut phases = BTreeMap::<&str, (u64, u64)>::new();
    let mut intervals = Vec::with_capacity(measurements.intervals.len());
    for &(name, start, end) in &measurements.intervals {
        let entry = phases.entry(name).or_default();
        entry.0 += end - start;
        entry.1 += 1;
        intervals.push((start, end));
    }
    intervals.sort_unstable();
    let mut end = 0;
    let mut total = 0;
    for (start, next_end) in intervals {
        total += next_end.saturating_sub(start.max(end));
        end = end.max(next_end);
    }
    let phases: BTreeMap<_, _> = phases
        .into_iter()
        .map(|(name, (ns, calls))| {
            (
                name,
                json!({"total_us": ns as f64 / 1000.0, "calls": calls}),
            )
        })
        .collect();
    let record = json!({
        "schema_version": 1,
        "operation": operation,
        "dataset": dataset,
        "success": result.is_ok(),
        "end_to_end_us": elapsed.as_secs_f64() * 1_000_000.0,
        "metadata_total_us": total as f64 / 1000.0,
        "total_definition": "union_of_instrumented_metadata_intervals",
        "phases": phases,
    });
    (result, Some(record))
}
