#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Declarative DAG signal / approval gate — issue #746.
//!
//! ## The problem
//!
//! A `#[dag]` pipeline can wire activities into a graph, but the moment one
//! step needs to *pause and wait for a human approval* (or any external
//! event), authors previously had to abandon the DAG surface and hand-write a
//! full `#[workflow]` — losing the declarative graph, the DAG trigger
//! contract, and the graph-view tooling.
//!
//! ## The solution: `signal_gate` / `signal_gate_with_timeout`
//!
//! A **gate node** is a graph node with no activity dispatch: it pauses the
//! run until a named signal arrives, then makes the signal payload its output
//! so downstream nodes can consume it. It composes exactly like any other
//! node (`.upstream(&gate)`, `.map_activity(f).over(&gate)`, `.condition(...)`).
//!
//! ```text
//!   extract  ─▶  [approval gate]  ─▶  load
//!                (waits for the
//!                 "approval" signal)
//! ```
//!
//! Deliver the signal that unblocks the gate with the ordinary standalone
//! signal route (AC3) — the gate consumes it just like a `wait_for_signal`:
//!
//! ```bash
//! curl -X POST \
//!   http://localhost:8080/api/harvest/workflows/{exec_id}/signal/approval \
//!   -H 'Content-Type: application/json' \
//!   -d '{"approved": true, "reviewer": "alice"}'
//! ```
//!
//! ## Timeout behaviour — fail vs continue
//!
//! `signal_gate_with_timeout(name, timeout, on_timeout)` arms a durable
//! deadline. If the deadline fires before the signal arrives:
//!
//! | `on_timeout`                | gate output    | run outcome                          |
//! |-----------------------------|----------------|--------------------------------------|
//! | `GateTimeoutAction::FailRun`  | —              | the DAG run **fails**                |
//! | `GateTimeoutAction::Continue` | `Value::Null`  | the run **continues** with a null gate output |
//!
//! ### "Continue to a named branch" is declarative
//!
//! There is no bespoke branch-target mechanism. Under `Continue`, the gate's
//! null-vs-payload output *is* the branch selector: attach a
//! `.condition(...)` to each downstream node. The `order_approval_with_fallback`
//! DAG below routes to `fulfill_order` when the signal arrived (payload present)
//! and to `escalate_to_manager` when the deadline fired first (payload null) —
//! two branches, expressed purely with conditions over the gate output.
//!
//! ## Invariants
//!
//! - **Unified-DAG only.** Gates lower onto the unified workflow-execution
//!   path; a classic DAG containing a gate is rejected at build time with a
//!   `DagSignalGateRequiresUnifiedExecution` error naming the DAG and signal.
//!   Build with `--features unified-dag-execution`.
//! - **No new `WorkflowEvent` variant, no migration.** Gates reuse the #476
//!   `TimerStarted`/`TimerFired`/`SignalReceived` machinery, so a gated run
//!   is durable and replays deterministically (see the embedded tests).
//!
//! Run with:
//!   cargo run --example dag_approval_gate --features unified-dag-execution

use autumn_harvest::prelude::*;
use serde_json::{json, Value};
use std::time::Duration;

// DAG activities take only `&ActivityContext`; the unified walker feeds each
// node its shaped input internally.

/// Pull the order the pipeline operates on.
#[activity(start_to_close = "10s")]
async fn extract_order(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "order_id": "ord_42", "amount_cents": 250_000 }))
}

/// Fulfil the order once approval has been granted.
#[activity(start_to_close = "10s")]
async fn load_order(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("fulfilled"))
}

/// Alternative branch: escalate to a manager if approval timed out.
#[activity(start_to_close = "10s")]
async fn escalate_to_manager(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("escalated"))
}

/// **Primary teaching shape.** `extract → approval gate → load`, where the gate
/// fails the whole run if no approval signal arrives within the SLA. This is
/// the classic human-in-the-loop pipeline: do the work only after a human
/// approves, and treat a missed SLA as a hard failure worth paging on.
#[dag(default_queue = "approvals")]
fn order_approval_pipeline(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_order);
    let gate = dag
        .signal_gate_with_timeout(
            "approval",
            Duration::from_secs(24 * 60 * 60), // 24h SLA
            GateTimeoutAction::FailRun,
        )
        .upstream(&extract);
    let _load = dag.activity(load_order).upstream(&gate);
}

/// **Declarative named-branch shape (AC2).** Same gate, but `Continue` on
/// timeout: instead of failing, the run proceeds with a `Value::Null` gate
/// output, and two downstream `.condition` branches select what happens next.
///
/// - `load_order` runs only when the gate output is a real payload (approved).
/// - `escalate_to_manager` runs only when the gate output is null (timed out).
#[dag(default_queue = "approvals")]
fn order_approval_with_fallback(dag: &mut DagBuilder) {
    let extract = dag.activity(extract_order);
    let gate = dag
        .signal_gate_with_timeout(
            "approval",
            Duration::from_secs(24 * 60 * 60),
            GateTimeoutAction::Continue,
        )
        .upstream(&extract);
    // Approved branch: gate output present (non-null).
    let _fulfil = dag
        .activity(load_order)
        .upstream(&gate)
        .condition(|ups| !ups[0].is_null());
    // Timed-out branch: gate output is the null sentinel.
    let _escalate = dag
        .activity(escalate_to_manager)
        .upstream(&gate)
        .condition(|ups| ups[0].is_null());
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _dags = dags![order_approval_pipeline, order_approval_with_fallback];
    let _acts = activities![extract_order, load_order, escalate_to_manager];
    println!("dag_approval_gate example compiled successfully");
    println!();
    println!("Unblock an approval gate by delivering its signal:");
    println!(
        "  curl -X POST \
         http://localhost:8080/api/harvest/workflows/{{exec_id}}/signal/approval \\"
    );
    println!("    -H 'Content-Type: application/json' -d '{{\"approved\": true}}'");
}

// Gated on `testing` + `unified-dag-execution`: the example binary builds under
// `--features unified-dag-execution` alone, and the embedded replay/harness
// tests appear when `testing` is also enabled. Run with:
//   cargo test -p autumn-harvest --features testing,unified-dag-execution \
//     --example dag_approval_gate
#[cfg(all(test, feature = "testing", feature = "unified-dag-execution"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};

    fn scheduled(name: &str, events: &[WorkflowEvent]) -> bool {
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { name: n, .. } if n == name))
    }

    /// AC1 + AC3 + AC5 (signal branch): a queued `approval` signal unblocks the
    /// gate, `load_order` runs, and the captured history replays deterministically.
    #[tokio::test]
    async fn approval_signal_unblocks_load_and_replays() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_order", |_| {
                Ok(json!({ "order_id": "ord_42", "amount_cents": 250_000 }))
            })
            .mock_activity("load_order", |_| Ok(json!("fulfilled")))
            .queue_signal("approval", json!({ "approved": true, "reviewer": "alice" }))
            .run(__autumn_workflow_info_order_approval_pipeline().handler, Value::Null)
            .await;

        assert!(
            outcome.result.is_ok(),
            "an approved gate DAG must complete, got: {:?}",
            outcome.result
        );
        let events = outcome.events();
        assert!(
            scheduled("load_order", &events),
            "load_order must run once the approval gate unblocks"
        );

        let report = WorkflowReplayer::new()
            .register_fn(
                "order_approval_pipeline",
                __autumn_workflow_info_order_approval_pipeline().handler,
            )
            .replay_from_events(events.to_vec())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "AC5: signal-branch history must replay deterministically, got: {report}"
        );
    }

    /// AC2a: no approval arrives, the deadline fires, and a `FailRun` gate fails
    /// the whole DAG run — and the timer-first history replays deterministically
    /// to that same FAILED outcome (never a non-determinism divergence).
    #[tokio::test]
    async fn missing_approval_fails_the_run() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_order", |_| {
                Ok(json!({ "order_id": "ord_42", "amount_cents": 250_000 }))
            })
            .run(__autumn_workflow_info_order_approval_pipeline().handler, Value::Null)
            .await;

        assert!(
            outcome.result.is_err(),
            "a FailRun gate must fail the run when the SLA deadline fires first, got: {:?}",
            outcome.result
        );

        // The FailRun timeout is a deterministic outcome, not a bug: the
        // timer-first history replays to the same clean WorkflowFailed on every
        // pass, never a NonDeterminismDetected.
        let report = WorkflowReplayer::new()
            .register_fn(
                "order_approval_pipeline",
                __autumn_workflow_info_order_approval_pipeline().handler,
            )
            .replay_from_events(outcome.events().to_vec())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::WorkflowFailed { .. }),
            "AC5: a FailRun timeout must replay to the same deterministic FAILED \
             outcome, got: {report}"
        );
        assert!(
            !matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
            "the FailRun timeout must be a clean deterministic failure, got: {report}"
        );
    }

    /// AC2b + AC5 (timer branch): under `Continue`, a missed SLA routes to the
    /// declarative `escalate_to_manager` branch (fulfil is skipped by its
    /// condition), and the timer-first history replays deterministically.
    #[tokio::test]
    async fn timeout_routes_to_escalation_branch_and_replays() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_order", |_| {
                Ok(json!({ "order_id": "ord_42", "amount_cents": 250_000 }))
            })
            .mock_activity("load_order", |_| Ok(json!("fulfilled")))
            .mock_activity("escalate_to_manager", |_| Ok(json!("escalated")))
            .run(
                __autumn_workflow_info_order_approval_with_fallback().handler,
                Value::Null,
            )
            .await;

        assert!(
            outcome.result.is_ok(),
            "Continue-on-timeout must let the run complete via the escalation branch, got: {:?}",
            outcome.result
        );
        let events = outcome.events();
        assert!(
            scheduled("escalate_to_manager", &events),
            "the timed-out branch (gate output null) must run escalate_to_manager"
        );
        assert!(
            !scheduled("load_order", &events),
            "the approved branch must be skipped by its condition when the gate timed out"
        );

        let report = WorkflowReplayer::new()
            .register_fn(
                "order_approval_with_fallback",
                __autumn_workflow_info_order_approval_with_fallback().handler,
            )
            .replay_from_events(events.to_vec())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "AC5: timer-first history must replay deterministically, got: {report}"
        );
    }

    /// The approved branch of the fallback DAG: a queued signal routes to
    /// `load_order` and skips escalation.
    #[tokio::test]
    async fn approval_signal_routes_to_fulfil_branch() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("extract_order", |_| {
                Ok(json!({ "order_id": "ord_42", "amount_cents": 250_000 }))
            })
            .mock_activity("load_order", |_| Ok(json!("fulfilled")))
            .mock_activity("escalate_to_manager", |_| Ok(json!("escalated")))
            .queue_signal("approval", json!({ "approved": true }))
            .run(
                __autumn_workflow_info_order_approval_with_fallback().handler,
                Value::Null,
            )
            .await;

        assert!(
            outcome.result.is_ok(),
            "an approved fallback DAG must complete, got: {:?}",
            outcome.result
        );
        let events = outcome.events();
        assert!(
            scheduled("load_order", &events),
            "an approved gate (non-null payload) must run the fulfil branch"
        );
        assert!(
            !scheduled("escalate_to_manager", &events),
            "the escalation branch must be skipped by its condition when approved"
        );
    }
}
