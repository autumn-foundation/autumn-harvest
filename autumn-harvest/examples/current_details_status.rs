#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Operator status breadcrumb via `ctx.set_current_details` — issue #593.
//!
//! ## The problem
//!
//! `GET /workflows/{id}` reports `RUNNING`. `/stack` reports what event the
//! run is blocked on. Neither answers the question that actually shortens an
//! incident: *what is this run trying to do right now, in plain English?*
//! Reconstructing that today means spelunking `/history/export`, or the
//! workflow author registering a `#[query]` handler the operator has to know
//! the name of and call by hand.
//!
//! ## The solution: `ctx.set_current_details(...)`
//!
//! One call per phase. No handler registration, no query name to remember:
//!
//! ```rust
//! use autumn_harvest::prelude::*;
//!
//! #[workflow]
//! async fn fulfill_order(ctx: &WorkflowContext, order_id: String) -> Result<(), String> {
//!     ctx.set_current_details("step 1/2: charging card");
//!     // ... ctx.execute_activity(&charge_payment_info(), ...).await?;
//!     ctx.set_current_details("step 2/2: awaiting carrier pickup confirmation");
//!     // ... ctx.execute_activity(&ship_order_info(), ...).await?;
//!     ctx.set_current_details(""); // clear -- the run is about to complete
//!     Ok(())
//! }
//! ```
//!
//! An operator reads it back with the existing describe call — no new
//! endpoint, no fan-out query, no live worker required:
//!
//! ```bash
//! curl http://localhost:8080/api/harvest/workflows/{exec_id} \
//!   | jq -r '.execution.current_details'
//! # "step 2/2: awaiting carrier pickup confirmation"
//! ```
//!
//! ## Semantics
//!
//! | Property | Behaviour |
//! |---|---|
//! | **Last-write-wins** | Each call overwrites the previous value. |
//! | **Empty string clears** | `set_current_details("")` persists `NULL`, not `""`. |
//! | **Durable** | Persisted to a column on `harvest_workflow_executions` -- survives worker restart and LRU cache eviction. Not held only in process memory. |
//! | **Replay-safe** | Suppressed while `ctx.is_replaying()`; zero `WorkflowCommand`s, zero `WorkflowEvent`s during a replay pass. See `tests::replay_self_check_succeeds` below. |
//! | **No new event variant** | The value rides an internal, replay-suppressed `WorkflowCommand` that is never appended to `harvest_events` -- the append-only event-JSON contract is untouched. |
//! | **Bounded** | Capped at [`autumn_harvest::context::DEFAULT_CURRENT_DETAILS_CAP_BYTES`] (1 KiB by default; configurable via `HarvestBuilder::with_current_details_cap`). Oversized input is truncated on a UTF-8 character boundary, never rejected -- a status breadcrumb can never wedge a workflow. |
//!
//! ## Out of scope (per issue #593)
//!
//! - A static, immutable run summary set once at start time (fast-follow).
//! - A dedicated query handler or SSE push for the field (SSE already covers
//!   live updates via #324; this is a pull field on describe).
//! - Indexing/filtering executions *by* `current_details` (that's the
//!   search-attributes surface, #506/#159).
//!
//! Run with:
//!   cargo run --example current_details_status

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderRequest {
    order_id: String,
    amount_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderResult {
    order_id: String,
    tracking_number: String,
}

/// Order-fulfillment workflow that keeps `current_details` up to date at
/// every phase so an operator can see progress with a single describe call.
#[workflow]
async fn fulfill_order(ctx: &WorkflowContext, order: OrderRequest) -> Result<OrderResult, String> {
    ctx.set_current_details(format!("step 1/4: validating order {}", order.order_id));
    let _: () = ctx
        .execute_activity(&validate_order_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;

    ctx.set_current_details(format!(
        "step 2/4: charging ${:.2}",
        order.amount_cents as f64 / 100.0
    ));
    let _: () = ctx
        .execute_activity(&charge_payment_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;

    ctx.set_current_details("step 3/4: awaiting carrier pickup confirmation");
    let tracking_number: String = ctx
        .execute_activity(&ship_order_info(), order.clone())
        .await
        .map_err(|e| e.to_string())?;

    ctx.set_current_details("step 4/4: sending confirmation email");
    let _: () = ctx
        .execute_activity(&send_confirmation_info(), order.order_id.clone())
        .await
        .map_err(|e| e.to_string())?;

    // Clear the breadcrumb -- the run is about to complete, there is no more
    // "current" activity to report.
    ctx.set_current_details("");

    Ok(OrderResult {
        order_id: order.order_id,
        tracking_number,
    })
}

#[activity(start_to_close = "10s")]
async fn validate_order(_ctx: &ActivityContext, _order: OrderRequest) -> Result<(), String> {
    Ok(())
}

#[activity(start_to_close = "10s")]
async fn charge_payment(_ctx: &ActivityContext, _order: OrderRequest) -> Result<(), String> {
    Ok(())
}

#[activity(start_to_close = "10s")]
async fn ship_order(_ctx: &ActivityContext, _order: OrderRequest) -> Result<String, String> {
    Ok("1Z999AA10123456784".to_string())
}

#[activity(start_to_close = "10s")]
async fn send_confirmation(_ctx: &ActivityContext, _order_id: String) -> Result<(), String> {
    Ok(())
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _wfs = workflows![fulfill_order];
    let _acts = activities![
        validate_order,
        charge_payment,
        ship_order,
        send_confirmation
    ];
    println!("current_details_status example compiled successfully");
    println!();
    println!("Read the live status back from the describe endpoint:");
    println!(
        "  curl http://localhost:8080/api/harvest/workflows/{{exec_id}} | jq -r '.execution.current_details'"
    );
}

// Gated on the `testing` feature as well as `test`: the example itself must
// keep building under `--no-default-features` (which CI exercises), while
// `autumn_harvest::testing` only exists for external consumers when the
// `testing` feature is enabled.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use serde_json::json;

    #[tokio::test]
    async fn fulfillment_completes_and_leaves_no_current_details_event_footprint() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("validate_order", |_| Ok(json!(null)))
            .mock_activity("charge_payment", |_| Ok(json!(null)))
            .mock_activity("ship_order", |_| Ok(json!("1Z999AA10123456784")))
            .mock_activity("send_confirmation", |_| Ok(json!(null)))
            .run(
                fulfill_order_info().handler,
                json!({"order_id": "ord_1", "amount_cents": 4999}),
            )
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
        assert_eq!(
            outcome.result.as_ref().unwrap(),
            &json!({"order_id": "ord_1", "tracking_number": "1Z999AA10123456784"})
        );

        // Four set_current_details calls (three sets + one empty-string clear)
        // must leave zero footprint in harvest_events -- only the four
        // activities' events are recorded.
        let events = outcome.events();
        let scheduled = events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
            .count();
        let completed = events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
            .count();
        assert_eq!(
            scheduled, 4,
            "expected exactly four ActivityScheduled events"
        );
        assert_eq!(
            completed, 4,
            "expected exactly four ActivityCompleted events"
        );
    }

    #[tokio::test]
    async fn replay_self_check_succeeds() {
        // The falsifiable correctness bar from issue #593: a replay of any
        // history whose workflow calls set_current_details must report
        // ReplaySucceeded, never NonDeterministic.
        let outcome = WorkflowTestEnv::new()
            .mock_activity("validate_order", |_| Ok(json!(null)))
            .mock_activity("charge_payment", |_| Ok(json!(null)))
            .mock_activity("ship_order", |_| Ok(json!("1Z999AA10123456784")))
            .mock_activity("send_confirmation", |_| Ok(json!(null)))
            .run(
                fulfill_order_info().handler,
                json!({"order_id": "ord_1", "amount_cents": 4999}),
            )
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let report = outcome.replay_check(fulfill_order_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "set_current_details must be replay-safe:\n{report}"
        );
    }
}
