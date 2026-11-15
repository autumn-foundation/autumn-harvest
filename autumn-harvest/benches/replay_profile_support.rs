//! Shared workload builder for the `replay_profile` instruction-count harness.
//!
//! Mirrors the `sequential_workflow` / `build_history` shape of
//! `replay_bench.rs`: the same activities per event and the same
//! `execute_activity_raw` call site. So this harness measures the documented
//! issue #135 workload, not a shape that flatters one change.
//!
//! The one addition is a realistic per-activity JSON payload
//! (`activity_payload`) in place of `Value::Null`. A payload-free history
//! under-counts costs that scale with payload size: `Value` clone and
//! `serde_json` (de)serialization at the event-history boundary.
//!
//! Both history construction and the workflow's `execute_activity_raw` call
//! use `activity_payload(i)`. So the recorded `ActivityScheduled.input` matches
//! what the replaying code supplies, as the strict-mode input comparison of
//! `WorkflowReplayer` requires.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::{Value, json};

/// A representative per-activity input record: an order line item.
///
/// About 230 bytes serialized. That is typical of a real activity call, and
/// large enough to exercise `Value` clone and serde cost, not only the
/// `Value::Null` fast path.
#[must_use]
pub fn activity_payload(i: usize) -> Value {
    json!({
        "order_id": format!("order-{i:08}"),
        "customer_id": i % 4096,
        "amount_cents": (i as u64 * 137) % 1_000_000,
        "currency": "USD",
        "items": [
            {"sku": format!("SKU-{}", i % 512), "qty": 1 + (i % 5), "unit_cents": 999},
            {"sku": format!("SKU-{}", (i + 7) % 512), "qty": 1 + ((i + 3) % 5), "unit_cents": 1499},
        ],
        "status": "processing",
    })
}

/// Workflow that executes `n` sequential activities, each carrying a
/// realistic JSON payload. Structurally identical to `replay_bench.rs`'s
/// `sequential_workflow`.
pub fn sequential_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n: usize = usize::try_from(input.as_u64().unwrap_or(0)).unwrap_or(0);
        let mut last = Value::Null;
        for i in 0..n {
            let name = format!("activity_{i}");
            last = ctx
                .execute_activity_raw(&name, activity_payload(i), "default")
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(last)
    })
}

/// Build a synthetic event history with `n` completed activities, each
/// carrying `activity_payload(i)` as both its scheduled input and its
/// completed output.
#[must_use]
pub fn build_history(n: usize) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let mut events = Vec::with_capacity(n * 2 + 1);

    events.push(WorkflowEvent::WorkflowStarted {
        input: Value::from(n as u64),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    });

    for i in 0..n {
        let activity_id = ActivityExecId::new();
        let payload = activity_payload(i);
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name: format!("activity_{i}"),
            input: payload.clone(),
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: payload,
        });
    }

    (exec_id, events)
}
