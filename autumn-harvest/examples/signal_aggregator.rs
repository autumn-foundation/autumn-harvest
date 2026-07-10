#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Non-blocking signal drain for the aggregator / entity-workflow pattern —
//! issue #775.
//!
//! ## The problem
//!
//! The canonical aggregator collects N events delivered as signals, batch-
//! processes them, and `continue_as_new`s to keep its history bounded. Written
//! with the only tool available before this feature — `receive_signal`, which
//! blocks for exactly one signal per call — collecting a batch of N signals
//! costs O(N) workflow tasks and O(N²) cumulative replay work, because each of
//! the N tasks replays the whole growing history.
//!
//! ## The solution: non-blocking drain
//!
//! Block once for the first event, then drain every already-buffered signal of
//! the same name in the *same* task execution — no extra tasks, no per-signal
//! replay amplification:
//!
//! ```rust
//! use autumn_harvest::prelude::*;
//!
//! #[workflow]
//! async fn aggregate(ctx: &WorkflowContext, _input: ()) -> Result<usize, String> {
//!     // Block until at least one "line_item" signal has arrived.
//!     let first: LineItem = ctx.receive_signal("line_item").await.map_err(|e| e.to_string())?;
//!     let mut batch = vec![first];
//!     // Non-blocking: sweep up every other "line_item" already buffered.
//!     batch.extend(ctx.drain_signals::<LineItem>("line_item").map_err(|e| e.to_string())?);
//!     // ... process `batch`, then continue_as_new ...
//!     Ok(batch.len())
//! }
//! ```
//!
//! ## The four drain methods
//!
//! | Method | Returns | Blocks? |
//! |--------|---------|---------|
//! | `try_receive_signal::<O>(name)`  | `Option<O>`  (oldest buffered) | never |
//! | `drain_signals::<O>(name)`       | `Vec<O>`     (all buffered)    | never |
//! | `try_wait_for_signal(name)`      | `Option<Value>` (oldest)      | never |
//! | `drain_signals_raw(name)`        | `Vec<Value>` (all)            | never |
//!
//! All four consume already-recorded `SignalReceived` events in FIFO
//! recorded-history order and NEVER suspend. They never double-deliver an event
//! already consumed by `receive_signal`/`wait_for_signal` or a push handler, and
//! they never resolve a signal reserved for an open `receive_signal_timeout`
//! race (issue #476).

use autumn_harvest::prelude::*;

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
struct LineItem {
    sku: String,
    qty: u32,
}

/// Collect a batch of `line_item` signals: block for the first, then drain the
/// rest without suspending. Returns the total quantity across the batch.
#[workflow]
async fn aggregate_order(ctx: &WorkflowContext, _input: ()) -> Result<u32, String> {
    // Block until the first line item arrives.
    let first: LineItem = ctx
        .receive_signal("line_item")
        .await
        .map_err(|e| e.to_string())?;

    let mut batch = vec![first];
    // Non-blocking sweep of every other line_item already buffered in history.
    batch.extend(
        ctx.drain_signals::<LineItem>("line_item")
            .map_err(|e| e.to_string())?,
    );

    let total_qty: u32 = batch.iter().map(|i| i.qty).sum();
    Ok(total_qty)
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _wfs = workflows![aggregate_order];
    println!("signal_aggregator example compiled successfully");
    println!();
    println!("Deliver a batch of signals, then let the workflow drain them:");
    println!(
        "  curl -X POST http://localhost:8080/api/harvest/workflows/{{exec_id}}/signal/line_item \\"
    );
    println!("    -H 'Content-Type: application/json' -d '{{\"sku\":\"A\",\"qty\":2}}'");
}

// Gated on the `testing` feature as well as `test`: the example itself must
// keep building under `--no-default-features` (which CI exercises), while
// `autumn_harvest::testing` only exists for external consumers when the
// `testing` feature is enabled.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use serde_json::json;

    #[tokio::test]
    async fn drains_all_buffered_line_items_in_one_task() {
        let outcome = WorkflowTestEnv::new()
            .queue_signal("line_item", json!({"sku": "A", "qty": 2}))
            .queue_signal("line_item", json!({"sku": "B", "qty": 3}))
            .queue_signal("line_item", json!({"sku": "C", "qty": 5}))
            .run(aggregate_order_info().handler, json!(null))
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
        // 2 + 3 + 5 = 10 — all three same-name signals collected in one task.
        assert_eq!(outcome.result.as_ref().unwrap(), &json!(10));
    }

    #[tokio::test]
    async fn replay_self_check_succeeds() {
        // The falsifiable correctness bar: a replay of a history whose workflow
        // drains signals must report ReplaySucceeded, never NonDeterministic.
        let outcome = WorkflowTestEnv::new()
            .queue_signal("line_item", json!({"sku": "A", "qty": 2}))
            .queue_signal("line_item", json!({"sku": "B", "qty": 3}))
            .run(aggregate_order_info().handler, json!(null))
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let report = outcome.replay_check(aggregate_order_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "signal drain must be replay-safe:\n{report}"
        );
    }
}
