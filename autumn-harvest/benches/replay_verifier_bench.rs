//! Criterion benchmark: `ReplayVerifier` batch throughput.
//!
//! AC from issue #251: verifying 1,000 fixtures averaging 1k events each
//! completes in under 30 seconds on a 4-core laptop (in-memory user code, no DB).
//!
//! Run with:
//!   cargo bench -p autumn-harvest --features testing --no-default-features \
//!     --bench `replay_verifier_bench`

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{HistorySnapshot, ReplayVerifier};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use serde_json::Value;

/// Workflow that executes N sequential activities.
fn sequential_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = usize::try_from(input.as_u64().unwrap_or(0)).unwrap_or(0);
        for i in 0..n {
            ctx.execute_activity_raw(&format!("activity_{i}"), Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(Value::Null)
    })
}

/// Build a `HistorySnapshot` JSON string with `activity_count` completed activities.
fn build_fixture_json(activity_count: usize) -> String {
    let exec_id = ExecutionId::new();
    let mut events = Vec::with_capacity(activity_count * 2 + 1);
    events.push(WorkflowEvent::WorkflowStarted {
        input: Value::from(activity_count as u64),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
    });
    for i in 0..activity_count {
        let activity_id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name: format!("activity_{i}"),
            input: Value::Null,
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: Value::Null,
        });
    }
    let snapshot = HistorySnapshot {
        workflow_name: "sequential".to_string(),
        execution_id: exec_id,
        events,
        context_headers: None,
    };
    serde_json::to_string(&snapshot).unwrap()
}

/// Benchmark: 1,000 fixtures × ~500 activities each (≈ 1k events per fixture).
///
/// Budget: < 30 seconds on a 4-core laptop.
fn bench_1000_fixtures(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("verify_1000_fixtures_1k_events", |b| {
        b.iter_batched(
            || {
                // Create a temp dir with 1000 fixture files.
                let dir = tempfile::tempdir().unwrap();
                let fixture_json = build_fixture_json(500); // 500 activities = 1001 events
                for i in 0..1000 {
                    std::fs::write(
                        dir.path().join(format!("fixture_{i:04}.json")),
                        &fixture_json,
                    )
                    .unwrap();
                }
                dir
            },
            |dir| {
                rt.block_on(async {
                    ReplayVerifier::new()
                        .register_fn("sequential", sequential_workflow)
                        .verify_dir(dir.path())
                        .await
                })
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group!(benches, bench_1000_fixtures);
criterion_main!(benches);
