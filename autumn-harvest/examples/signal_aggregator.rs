#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Non-blocking signal drain for the aggregator / entity-workflow pattern —
//! issue #775.
//!
//! ## The problem
//!
//! The canonical aggregator collects N events delivered as signals, batch-
//! processes them, and `continue_as_new`s to keep its history bounded. Before
//! this feature, the only tool was `receive_signal`, which blocks for exactly
//! one signal per call. A batch of N signals then costs O(N) workflow tasks.
//! It also costs O(N²) cumulative replay work, because each of the N tasks
//! replays the whole growing history.
//!
//! ## The solution: non-blocking drain
//!
//! Block once for the first event. Then drain every already-buffered signal of
//! the same name in the *same* task execution. There are no extra tasks and no
//! per-signal replay amplification:
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
//! ## The drain methods
//!
//! | Method | Returns | Blocks? |
//! |--------|---------|---------|
//! | `try_receive_signal::<O>(name)`  | `Option<O>`  (oldest buffered) | never |
//! | `drain_signals::<O>(name)`       | `Vec<O>`     (all, fail-fast)  | never |
//! | `drain_signals_collect::<O>(name)` | `Vec<Result<O, String>>` (all, per-slot) | never |
//! | `try_wait_for_signal(name)`      | `Option<Value>` (oldest)      | never |
//! | `drain_signals_raw(name)`        | `Vec<Value>` (all)            | never |
//!
//! `drain_signals` is fail-fast. One undeserializable payload aborts the whole
//! batch and discards its good siblings. `drain_signals_collect` returns a
//! per-slot `Result`, so a poison payload never loses the good ones. The pair
//! mirrors the fan-out fail-fast/collect pair.
//!
//! All consume already-recorded `SignalReceived` events in FIFO
//! recorded-history order and NEVER suspend. They never double-deliver an event
//! already consumed by `receive_signal`/`wait_for_signal` or a push handler.
//! They never resolve a signal reserved for an open `receive_signal_timeout`
//! race (issue #476).

use autumn_harvest::prelude::*;

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
struct LineItem {
    sku: String,
    qty: u32,
}

/// Collect a batch of `line_item` signals: block for the first, then drain the
/// rest without suspending. Returns the total quantity across the batch.
///
/// This runnable version is deliberately **single-shot**: it collects one batch
/// and returns, so the embedded `WorkflowTestEnv` tests terminate. A real
/// long-lived aggregator loops. After it processes `batch`, it calls
/// `ctx.continue_as_new(...)` in place of the `Ok(total_qty)` return below.
/// That forks a fresh execution with bounded history, which blocks on the next
/// `receive_signal("line_item")`.
///
/// Aggregator race caveat: a `line_item` signal may arrive *after* this drain
/// but *before* a `continue_as_new` commit. It belongs to the **old**
/// execution's signal stream. `continue_as_new` mints a new `exec_id`, and a new
/// exec_id is a new signal stream. A final drain on the current execution, right
/// before continuing, still sees such a straggler. It is not carried forward
/// into the successor.
#[workflow]
async fn aggregate_order(ctx: &WorkflowContext, _input: ()) -> Result<u32, String> {
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
    // A long-lived aggregator would `ctx.continue_as_new(...)` here instead of
    // returning, to keep history bounded across batches.
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

// Gated on the `testing` feature as well as `test`. The example must keep
// building under `--no-default-features`, which CI exercises.
// `autumn_harvest::testing` exists for an external consumer only when the
// `testing` feature is on.
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
