//! Criterion benchmark: replay throughput for large event histories.
//!
//! Verifies the requirement from issue #135:
//! "Replaying a 10,000-event history completes in under 200ms on a
//! laptop-class machine for a workflow whose user code is in-memory only."
//!
//! Run with:
//!   cargo bench -p autumn-harvest --features testing --no-default-features --bench `replay_bench`

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::WorkflowReplayer;
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
        let n: usize = usize::try_from(input.as_u64().unwrap_or(0)).unwrap_or(0);
        let mut last = Value::Null;
        for i in 0..n {
            let name = format!("activity_{i}");
            last = ctx
                .execute_activity_raw(&name, Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(last)
    })
}

/// Build a synthetic event history with `n` completed activities.
fn build_history(n: usize) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let mut events = Vec::with_capacity(n * 2 + 1);

    events.push(WorkflowEvent::WorkflowStarted {
        input: Value::from(n as u64),
        timestamp: Utc::now(),
    });

    for i in 0..n {
        let activity_id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name: format!("activity_{i}"),
            input: Value::Null,
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: Value::from(i as u64),
        });
    }

    (exec_id, events)
}

fn bench_replay_10k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let replayer = WorkflowReplayer::new().register_fn("sequential", sequential_workflow);

    c.bench_function("replay_10k_events", |b| {
        b.iter_batched(
            || build_history(5_000), // 5_000 activities = 10_001 events
            |(_exec_id, events)| rt.block_on(replayer.replay_from_events(events)),
            BatchSize::SmallInput,
        );
    });
}

fn bench_replay_1k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let replayer = WorkflowReplayer::new().register_fn("sequential", sequential_workflow);

    c.bench_function("replay_1k_events", |b| {
        b.iter_batched(
            || build_history(500), // 500 activities = 1_001 events
            |(_exec_id, events)| rt.block_on(replayer.replay_from_events(events)),
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_replay_1k, bench_replay_10k);
criterion_main!(benches);
