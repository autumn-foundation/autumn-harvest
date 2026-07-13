#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Workflow run metadata via `ctx.info()` — issue #698.
//!
//! ## The problem
//!
//! A `#[workflow]` body can read its author-supplied business key
//! (`ctx.workflow_id()`) and type (`ctx.workflow_type()`), but it could not read
//! its system-generated `execution_id` — the exact identifier every operator
//! tool keys on: the management-API path (`/api/harvest/workflows/{exec_id}/...`),
//! the DLQ, reset/pause/cancel, and the ADR-0001 `execution.id` span attribute.
//! So a workflow could log `workflow_id = "cart-42"`, but an operator paging from
//! that log line had no run UUID to open the execution or build a correlated
//! trace query, and authors couldn't mint run-scoped idempotency keys.
//!
//! ## The solution: `ctx.info()`
//!
//! One replay-safe, zero-footprint accessor bundles the run's system metadata:
//!
//! ```rust
//! use autumn_harvest::prelude::*;
//!
//! #[workflow]
//! async fn checkout(ctx: &WorkflowContext, _cart: serde_json::Value) -> Result<(), String> {
//!     let info = ctx.info();
//!     tracing::info!(execution_id = %info.execution_id, "starting checkout");
//!     // Run-scoped idempotency key — stable across retries and replays:
//!     let _charge_key = format!("charge-{}", info.execution_id);
//!     if let Some(parent) = info.parent_execution_id {
//!         tracing::info!(%parent, "spawned by parent workflow");
//!     }
//!     Ok(())
//! }
//! ```
//!
//! ## Guarantees
//!
//! - **Zero footprint** — calling `ctx.info()` appends no `harvest_events` and
//!   emits no command (same leave-no-trace property as a query handler).
//! - **Replay-safe** — every field is derived from already-recorded state (the
//!   `WorkflowStarted` event and the execution row), so it returns byte-identical
//!   values on every worker and every replay pass.
//! - **No feature flag** — usable from any `#[workflow]` body.
//! - **No new `WorkflowEvent` variant, no migration.**
//!
//! Fields: `execution_id`, `workflow_id`, `workflow_type`, `start_time`,
//! `history_event_count`, `is_replaying`, and `parent_execution_id`
//! (`None` for a top-level run).

use autumn_harvest::prelude::*;

/// An order workflow that stamps its own run id into an activity's idempotency
/// key and records the spawning parent (if any) for correlation.
#[workflow]
async fn place_order(ctx: &WorkflowContext, order: serde_json::Value) -> Result<String, String> {
    let info = ctx.info();

    // One-hop correlation: this is the exact id an operator opens in the
    // management API / Vantage — no directory lookup needed.
    tracing::info!(
        execution_id = %info.execution_id,
        workflow_id = %info.workflow_id,
        workflow_type = %info.workflow_type,
        parent = ?info.parent_execution_id,
        "placing order"
    );

    // Mint a run-scoped idempotency key from the execution id so a charge is
    // never double-applied across worker-level retries of this run.
    let charge_key = format!("charge-{}", info.execution_id);

    let receipt = ctx
        .execute_activity_raw(
            "charge_payment",
            serde_json::json!({
                "order": order,
                "idempotency_key": charge_key,
                // Record the spawning parent (if any) for downstream correlation.
                "parent": info.parent_execution_id.map(|p| p.to_string()),
            }),
            "default",
        )
        .await
        .map_err(|e| e.to_string())?;

    Ok(receipt
        .get("receipt")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string())
}

/// Charges the order once, keyed by the caller-supplied idempotency key.
#[activity(start_to_close = "10s")]
async fn charge_payment(
    _ctx: &ActivityContext,
    _req: serde_json::Value,
) -> Result<serde_json::Value, String> {
    Ok(serde_json::json!({ "receipt": "rcpt_123" }))
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _wfs = workflows![place_order];
    let _acts = activities![charge_payment];
    println!("ctx_info example compiled successfully");
    println!();
    println!("Inside a #[workflow], `ctx.info().execution_id` is the same id you open with:");
    println!("  curl http://localhost:8080/api/harvest/workflows/{{exec_id}}");
}

// Gated on the `testing` feature as well as `test`: the example itself must keep
// building under `--no-default-features` (which CI exercises), while
// `autumn_harvest::testing` only exists for external consumers when the
// `testing` feature is enabled.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::ExecutionId;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use serde_json::json;

    /// The run reads its own `execution_id` into a charge idempotency key,
    /// completes, and — because `ctx.info()` leaves no footprint — records only
    /// the single activity's events; the recorded history replays deterministically.
    #[tokio::test]
    async fn place_order_uses_run_scoped_key_and_replays_deterministically() {
        let captured: std::sync::Arc<std::sync::Mutex<Option<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let captured_clone = captured.clone();

        let outcome = WorkflowTestEnv::new()
            .with_workflow_name("place_order")
            .mock_activity("charge_payment", move |input| {
                // The idempotency key threaded from ctx.info().execution_id.
                let key = input
                    .get("idempotency_key")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                *captured_clone.lock().unwrap() = Some(key);
                Ok(json!({ "receipt": "rcpt_123" }))
            })
            .run(place_order_info().handler, json!({ "sku": "abc" }))
            .await;

        assert_eq!(outcome.result.clone().unwrap(), json!("rcpt_123"));

        // The key is run-scoped: "charge-<execution_id>".
        let key = captured.lock().unwrap().clone().expect("charge ran");
        assert!(
            key.starts_with("charge-"),
            "expected a run-scoped charge key, got {key}"
        );

        // Zero-footprint: only the activity's events are recorded (no info() events).
        let events = outcome.events();
        assert!(
            !events.iter().any(|e| matches!(
                e,
                autumn_harvest::event::WorkflowEvent::MarkerRecorded { .. }
            )),
            "ctx.info() must not append any marker/side-effect events"
        );

        let report = outcome.replay_check(place_order_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "info()-reading workflow must replay deterministically:\n{report}"
        );
    }

    /// A run configured with a spawning parent surfaces it via
    /// `ctx.info().parent_execution_id`; the workflow threads it into the
    /// activity input for correlation, and it replays deterministically (the
    /// `WorkflowTestEnv::with_parent_execution_id` value is carried through
    /// `replay_check`).
    #[tokio::test]
    async fn parent_execution_id_is_visible_when_spawned_as_a_child() {
        let parent = ExecutionId::new();
        let seen_parent: std::sync::Arc<std::sync::Mutex<Option<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let seen_clone = seen_parent.clone();

        let outcome = WorkflowTestEnv::new()
            .with_parent_execution_id(Some(parent))
            .mock_activity("charge_payment", move |input| {
                *seen_clone.lock().unwrap() = input
                    .get("parent")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                Ok(json!({ "receipt": "rcpt_123" }))
            })
            .run(place_order_info().handler, json!({ "sku": "abc" }))
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        // The spawning parent is surfaced via ctx.info().parent_execution_id and
        // threaded into the activity input — exactly the configured id.
        assert_eq!(
            seen_parent.lock().unwrap().clone(),
            Some(parent.to_string()),
            "child run must observe its spawning parent's execution id"
        );

        // And a run of the same workflow with the parent threaded replays clean.
        let report = outcome.replay_check(place_order_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "parent-aware run must replay deterministically:\n{report}"
        );
    }

    /// A top-level run reports no parent (`None`).
    #[tokio::test]
    async fn top_level_run_reports_no_parent() {
        let seen_parent: std::sync::Arc<std::sync::Mutex<Option<serde_json::Value>>> =
            std::sync::Arc::new(std::sync::Mutex::new(None));
        let seen_clone = seen_parent.clone();

        let outcome = WorkflowTestEnv::new()
            .mock_activity("charge_payment", move |input| {
                *seen_clone.lock().unwrap() = input.get("parent").cloned();
                Ok(json!({ "receipt": "rcpt_123" }))
            })
            .run(place_order_info().handler, json!({ "sku": "abc" }))
            .await;
        assert!(outcome.result.is_ok());

        // No parent configured → the correlation field is JSON null.
        assert_eq!(
            seen_parent.lock().unwrap().clone(),
            Some(serde_json::Value::Null),
            "a top-level run must report no parent"
        );
    }
}
