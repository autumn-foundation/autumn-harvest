#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Typed workflow failures — classify child failures by a stable `error_type`,
//! not by scraping the message string — issue #767.
//!
//! ## The problem
//!
//! A child workflow that fails hands the parent only a `String`. To decide
//! whether to compensate, retry, or escalate, the parent has to *substring-match*
//! the message text — brittle, and it breaks the instant the child reworders its
//! error.
//!
//! ## The solution: `WorkflowFailure`
//!
//! A workflow can fail with a typed [`WorkflowFailure`] carrying a stable,
//! low-cardinality `error_type` class (plus optional structured `details` and a
//! `non_retryable` flag). Opt in simply by returning
//! `Result<T, WorkflowFailure>` from a `#[workflow]` — the macro serialises it
//! onto the engine's wire format automatically. The parent then branches on the
//! typed class via [`HarvestError::workflow_error_type`] — **no substring
//! matching**:
//!
//! ```rust,ignore
//! match ctx.spawn_child_workflow(&charge_card_info(), order).await {
//!     Ok(receipt) => Ok(receipt),
//!     Err(e) => match e.workflow_error_type() {
//!         Some("ValidationRejected")  => compensate(...),   // permanent — refund & notify
//!         Some("BudgetExceeded")      => escalate(...),      // hand off to finance
//!         Some("UpstreamUnavailable") => reschedule(...),    // transient — try later
//!         _                           => Err(e.to_string()), // untyped fallback
//!     },
//! }
//! ```
//!
//! An embedder awaiting the child directly gets the same typed surface through
//! [`TypedWorkflowHandle::result`] / `result_snapshot()`
//! (`snap.error_type` / `snap.error_details` / `snap.non_retryable`).
//!
//! ## Guarantees
//!
//! | Property | Behaviour |
//! |---|---|
//! | **No new event variant** | Typed fields ride *additive* optional columns on the existing `WorkflowFailed` / `ChildWorkflowFailed` events. |
//! | **No migration** | The fields are opaque JSON inside `harvest_events`. |
//! | **Backward compatible** | A pre-#767 (or `Err(String)`) failure decodes to `error_type = None` — it routes to the untyped fallback, never a synthetic class. |
//! | **Replay-deterministic** | The parent's branch is a pure function of the recorded typed `ChildWorkflowFailed`, so replay always takes the same branch. |
//! | **Human message preserved** | The `execution.error` column keeps the plain message, never the wire envelope. |
//!
//! Run with:
//!   cargo run --example typed_workflow_failure

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    order_id: String,
    /// Drives which typed failure the child raises in this example.
    failure_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Receipt {
    order_id: String,
    authorized: bool,
}

/// A child workflow that fails with a **typed** [`WorkflowFailure`]. Opting into
/// typed failures is just the return type: `Result<Receipt, WorkflowFailure>`.
#[workflow]
async fn charge_card(_ctx: &WorkflowContext, order: Order) -> Result<Receipt, WorkflowFailure> {
    match order.failure_mode.as_str() {
        // Permanent, caller's fault — do not retry.
        "declined" => Err(WorkflowFailure::new(
            "ValidationRejected",
            format!("issuer declined the charge for order {}", order.order_id),
        )
        .with_details(serde_json::json!({ "decline_code": "do_not_honor" }))
        .non_retryable()),
        // Policy limit — hand off to a human.
        "over_budget" => Err(WorkflowFailure::new(
            "BudgetExceeded",
            format!("order {} exceeds the tenant spend cap", order.order_id),
        )
        .non_retryable()),
        // Transient downstream problem — safe to try again later.
        "gateway_down" => Err(WorkflowFailure::new(
            "UpstreamUnavailable",
            format!("payment gateway timed out for order {}", order.order_id),
        )),
        _ => Ok(Receipt {
            order_id: order.order_id,
            authorized: true,
        }),
    }
}

/// The parent decides what to do purely from the child's typed `error_type`.
#[workflow]
async fn checkout(ctx: &WorkflowContext, order: Order) -> Result<String, String> {
    match ctx.spawn_child_workflow(&charge_card_info(), order).await {
        Ok(receipt) => {
            let receipt: Receipt = receipt;
            Ok(format!("charged:{}", receipt.order_id))
        }
        Err(e) => {
            // Route on the stable class — never on the message text.
            let action = match e.workflow_error_type() {
                Some("ValidationRejected") => "compensate:refund_and_notify_customer",
                Some("BudgetExceeded") => "escalate:route_to_finance_review",
                Some("UpstreamUnavailable") => "reschedule:retry_in_1h",
                Some(_) => "generic:open_ticket",
                None => "untyped:log_and_fail",
            };
            // `non_retryable` and `details` are available too, for richer policy.
            Ok(format!(
                "{action}|non_retryable={}",
                e.is_workflow_non_retryable()
            ))
        }
    }
}

fn main() {
    let _wfs = workflows![checkout, charge_card];
    println!("typed_workflow_failure example compiled successfully");
    println!();
    println!(
        "A child fails with WorkflowFailure::new(\"ValidationRejected\", ...).non_retryable();"
    );
    println!("the parent branches on err.workflow_error_type() instead of scraping the message.");
}

// Gated on `testing` as well as `test` so the example still builds under
// `--no-default-features` (which CI exercises), while `autumn_harvest::testing`
// is only available to external consumers when the `testing` feature is on.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use serde_json::json;

    /// Drive the parent with the child mocked to fail with a typed envelope,
    /// and assert the parent branched on the typed class.
    async fn run_checkout_with_typed_child_failure(
        error_type: &str,
        message: &str,
        non_retryable: bool,
    ) -> String {
        let mut failure = WorkflowFailure::new(error_type, message);
        if non_retryable {
            failure = failure.non_retryable();
        }
        let envelope = failure.into_workflow_error_payload();
        let outcome = WorkflowTestEnv::new()
            .mock_child_workflow("charge_card", move |_input| Err(envelope.clone()))
            .run(
                checkout_info().handler,
                json!({ "order_id": "ord_1", "failure_mode": "n/a" }),
            )
            .await;
        outcome
            .result
            .expect("checkout should complete on the compensation branch")
            .as_str()
            .expect("string output")
            .to_string()
    }

    #[tokio::test]
    async fn parent_routes_on_typed_error_type_across_categories() {
        assert_eq!(
            run_checkout_with_typed_child_failure("ValidationRejected", "issuer declined", true)
                .await,
            "compensate:refund_and_notify_customer|non_retryable=true"
        );
        assert_eq!(
            run_checkout_with_typed_child_failure("BudgetExceeded", "over the cap", true).await,
            "escalate:route_to_finance_review|non_retryable=true"
        );
        assert_eq!(
            run_checkout_with_typed_child_failure(
                "UpstreamUnavailable",
                "gateway timed out",
                false
            )
            .await,
            "reschedule:retry_in_1h|non_retryable=false"
        );
    }

    #[tokio::test]
    async fn reworded_message_takes_the_same_branch() {
        // Same class, different wording → identical branch (the whole point).
        let a = run_checkout_with_typed_child_failure(
            "ValidationRejected",
            "issuer declined the charge",
            true,
        )
        .await;
        let b = run_checkout_with_typed_child_failure(
            "ValidationRejected",
            "card refused: do_not_honor",
            true,
        )
        .await;
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn legacy_untyped_child_failure_hits_the_untyped_fallback() {
        // A plain string (pre-#767 / Err(String)) has no error_type.
        let outcome = WorkflowTestEnv::new()
            .mock_child_workflow("charge_card", |_input| Err("boom".to_string()))
            .run(
                checkout_info().handler,
                json!({ "order_id": "ord_1", "failure_mode": "n/a" }),
            )
            .await;
        assert_eq!(
            outcome.result.unwrap().as_str().unwrap(),
            "untyped:log_and_fail|non_retryable=false"
        );
    }

    #[tokio::test]
    async fn checkout_replays_deterministically_over_a_typed_child_failure() {
        // Record a run's history, then replay the parent handler against it and
        // confirm the typed branch reproduces without divergence.
        let envelope = WorkflowFailure::new("ValidationRejected", "issuer declined")
            .non_retryable()
            .into_workflow_error_payload();
        let outcome = WorkflowTestEnv::new()
            .mock_child_workflow("charge_card", move |_input| Err(envelope.clone()))
            .run(
                checkout_info().handler,
                json!({ "order_id": "ord_1", "failure_mode": "n/a" }),
            )
            .await;
        let report = outcome.replay_check(checkout_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "typed child failure must replay deterministically: {report}"
        );
    }
}
