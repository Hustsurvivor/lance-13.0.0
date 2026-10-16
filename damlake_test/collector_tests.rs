// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::metadata_metrics::{collect, inherit, phase};

#[tokio::test]
async fn nested_intervals_are_not_added_twice_and_errors_are_recorded() {
    let (result, record) = collect(true, "error", "dataset", async {
        let _outer = phase("outer");
        {
            let _inner = phase("inner");
            tokio::task::yield_now().await;
        }
        Err::<(), _>("expected")
    })
    .await;
    assert_eq!(result, Err("expected"));
    let record = record.unwrap();
    assert_eq!(record["success"], false);
    assert_eq!(record["phases"]["inner"]["calls"], 1);
    assert_eq!(
        record["metadata_total_us"],
        record["phases"]["outer"]["total_us"]
    );
    assert!(
        record["end_to_end_us"].as_f64().unwrap() >= record["metadata_total_us"].as_f64().unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_requests_and_explicit_children_are_isolated() {
    let first = collect(true, "first", "a", async {
        let child = inherit(async {
            let _timer = phase("child");
            tokio::task::yield_now().await;
        });
        tokio::spawn(child).await.unwrap();
        Ok::<_, ()>(())
    });
    let second = collect(true, "second", "b", async {
        let _timer = phase("second_only");
        tokio::task::yield_now().await;
        Ok::<_, ()>(())
    });
    let ((_, first), (_, second)) = tokio::join!(first, second);
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first["phases"]["child"]["calls"], 1);
    assert!(first["phases"].get("second_only").is_none());
    assert!(second["phases"].get("child").is_none());
}

#[tokio::test]
async fn disabled_collection_preserves_result_without_output() {
    let (result, record) = collect(false, "disabled", "dataset", async {
        let _timer = phase("not_recorded");
        Ok::<_, ()>(42)
    })
    .await;
    assert_eq!(result, Ok(42));
    assert!(record.is_none());
}

#[tokio::test]
async fn overlapping_children_use_interval_union() {
    let (_, record) = collect(true, "parallel", "dataset", async {
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let spawn_child = |name| {
            let barrier = barrier.clone();
            tokio::spawn(inherit(async move {
                let _timer = phase(name);
                barrier.wait().await;
                tokio::task::yield_now().await;
            }))
        };
        let a = spawn_child("a");
        let b = spawn_child("b");
        a.await.unwrap();
        b.await.unwrap();
        Ok::<_, ()>(())
    })
    .await;
    let record = record.unwrap();
    let total = record["metadata_total_us"].as_f64().unwrap();
    let a = record["phases"]["a"]["total_us"].as_f64().unwrap();
    let b = record["phases"]["b"]["total_us"].as_f64().unwrap();
    assert!(total >= a.max(b));
    assert!(total <= a + b);
}
