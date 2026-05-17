#![allow(clippy::doc_markdown, clippy::needless_raw_string_hashes)]
//! Example: Atomic start-or-attach + signal for webhook receivers (issue #244).
//!
//! Demonstrates how a webhook receiver collapses the racy
//! *fetch-then-start-then-signal* trio into a single `signal_with_start`
//! call.  The signal lands deterministically in the workflow's first tick on
//! a fresh start, and goes to the existing live execution on a duplicate.
//!
//! The before/after below is the *Success Metric* for the issue: the
//! canonical "idempotent webhook → workflow" snippet shrinks from ≥3 calls
//! and ≥30 lines of error-handling glue to a single call and ≤5 lines of
//! logic.
//!
//! Run with:
//!   cargo run --example signal_with_start_webhook

use autumn_harvest::prelude::*;
use autumn_harvest::{
    ExecutionId, ShardRouter, SignalWithStartParams, WorkflowIdReusePolicy,
    signal_with_start_workflow_execution,
};
use serde_json::json;

/// The author's workflow registers a signal handler the same way it would
/// for any other signal source.  The signal-with-start primitive does not
/// change the workflow side of the contract.
#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: String) -> Result<(), String> {
    // Wait for the *stripe.subscription_created* signal to arrive.  When the
    // workflow is started via `signal_with_start`, this signal is staged in
    // the same Postgres transaction as the WorkflowStarted event, so the
    // very first dispatch already sees it.
    let _payload = ctx
        .wait_for_signal("stripe.subscription_created")
        .await
        .map_err(|e| e.to_string())?;

    // ... grant entitlements, send welcome email, etc. ...
    let _ = user_id;
    Ok(())
}

/// HOW THE WEBHOOK HANDLER USED TO LOOK — three calls, racy across replicas.
///
/// ```ignore
/// async fn handle_webhook_before(payload: StripeWebhook, pool: &Pool) -> Result<()> {
///     let mut conn = pool.get().await?;
///     // 1. fetch — does this workflow already exist?
///     let existing = load_workflow_by_key(&mut conn, "onboarding", &payload.customer_id).await?;
///     match existing {
///         Some(execution) if execution.state == "RUNNING" => {
///             // 2a. signal the existing run.
///             send_signal(&mut conn, execution.id, "stripe.subscription_created", payload.to_json()).await?;
///         }
///         _ => {
///             // 2b. start a new run …
///             let exec_id = ExecutionId::new();
///             start_or_load_workflow_execution(&mut conn, /* params */).await?;
///             // 3. … and *separately* signal it.  Race window if a second
///             //    replica calls start_or_load at the same time: the second
///             //    caller's signal may land on the *prior* run.
///             send_signal(&mut conn, exec_id, "stripe.subscription_created", payload.to_json()).await?;
///         }
///     }
///     Ok(())
/// }
/// ```
///
/// HOW THE WEBHOOK HANDLER LOOKS NOW — one atomic call, race-free.
async fn handle_webhook_after(
    conn: &mut diesel_async::AsyncPgConnection,
    customer_id: String,
    stripe_event_id: String,
    payload: serde_json::Value,
    router: &ShardRouter,
) -> Result<ExecutionId, autumn_harvest::HarvestError> {
    let shard = router.pick_for_new_workflow("onboarding", &customer_id);
    let exec_id = ExecutionId::new_for_shard(shard);

    let outcome = signal_with_start_workflow_execution(
        conn,
        SignalWithStartParams {
            workflow_name: "onboarding",
            workflow_id: &customer_id,
            exec_id,
            input: json!(customer_id.clone()),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            // Idempotent attach to an existing live run; reject if duplicate
            // starts must never happen (e.g., financial onboarding flows).
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            signal_name: "stripe.subscription_created",
            signal_payload: payload,
            // Stripe-Idempotency-Key dedupes upstream retries: two webhook
            // deliveries with the same event id yield exactly one
            // SignalReceived event in the workflow's history.
            idempotency_key: Some(stripe_event_id),
        },
    )
    .await?;

    Ok(outcome.exec_id)
}

fn main() {
    println!("signal_with_start_webhook example — see source for usage");
    println!();
    println!("# Equivalent management API call (no Rust glue required):");
    println!();
    println!(
        r#"  curl -X POST http://localhost:8080/api/harvest/workflows/onboarding/signal-with-start \"#
    );
    println!(r#"       -H 'Content-Type: application/json' \"#);
    println!(r#"       -d '{{"#);
    println!(r#"             "workflow_id": "cus_NXabcd12","#);
    println!(r#"             "start_input": "cus_NXabcd12","#);
    println!(r#"             "signal_name": "stripe.subscription_created","#);
    println!(r#"             "signal_payload": {{ "event_id": "evt_1NX...", "amount": 9900 }},"#);
    println!(r#"             "idempotency_key": "evt_1NX..."  // Stripe's event id"#);
    println!(r#"           }}'"#);
    println!();
    println!("Response (201 on fresh start, 200 on attach):");
    println!(r#"  {{"#);
    println!(r#"    "execution_id": "01900000-0000-7000-...","#);
    println!(r#"    "workflow_name": "onboarding","#);
    println!(r#"    "workflow_id": "cus_NXabcd12","#);
    println!(r#"    "state": "RUNNING","#);
    println!(r#"    "started_fresh": true,"#);
    println!(r#"    "signal_delivered": true"#);
    println!(r#"  }}"#);

    let _ = handle_webhook_after;
}
