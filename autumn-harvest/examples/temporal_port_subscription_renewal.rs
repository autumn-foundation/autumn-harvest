#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Worked example for the Temporal → Harvest migration guide (issue #947).
//!
//! This is a direct Rust port of a small Temporal TypeScript workflow. Both
//! versions are shown side by side in
//! [`docs/migrating-from-temporal.md`](../../docs/migrating-from-temporal.md#worked-example).
//! Read that page first — this file is the compiling, tested half of the
//! pair.
//!
//! ## What it does
//!
//! `subscription_renewal` models one subscription entity: charge the
//! customer, wait 30 days for the next billing cycle, then loop forever via
//! `continue_as_new` — unless a `cancel` signal has arrived, in which case
//! the loop stops.
//!
//! ## Dispatch timing (read this before copying the pattern)
//!
//! `ctx.register_signal_handler_raw` (issue #546) is push-based: it stores
//! the handler, but does **not** fire it inline. The handler dispatches on
//! the *next* history-consulting call the workflow body makes. A `cancel`
//! signal that arrives:
//!
//! - **before this run even starts** (delivered via `signal-with-start`, or
//!   staged before the first workflow task) needs a flush point before the
//!   first `cancelled.load(...)` check, since nothing else has run between
//!   registration and that check. `ctx.system_now()` is that flush point.
//!   It is a cheap deterministic primitive call, not an activity or a
//!   timer, so it costs nothing. Without it, the first check always reads
//!   `false`, even for a signal already recorded in history;
//! - **while the workflow is waiting on the timer** is observed the moment
//!   the timer resolves — the `ctx.timer(...)` call itself is the flush
//!   point, so the second `cancelled.load(...)` check needs no extra call.
//!
//! See `autumn-harvest/examples/signal_handlers_subscription.rs` for the
//! full dispatch-timing contract and its own `system_now()` flush-point
//! idiom, which this example follows.

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubscriptionState {
    pub cycles: u32,
}

#[activity(start_to_close = "30s")]
async fn charge_card(_ctx: &ActivityContext, cycles: u32) -> Result<(), String> {
    // ... real billing call goes here ...
    println!("charged card for cycle {cycles}");
    Ok(())
}

#[workflow]
async fn subscription_renewal(
    ctx: &WorkflowContext,
    state: SubscriptionState,
) -> Result<(), String> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let handler_flag = cancelled.clone();
    ctx.register_signal_handler_raw("cancel", move |_payload| {
        handler_flag.store(true, Ordering::SeqCst);
    });

    // Registration only stores the handler -- it does not dispatch inline.
    // This cheap deterministic primitive call flushes it before the check
    // below, since nothing else runs first. See the module docs above.
    let _ = ctx.system_now();

    // A cancellation already recorded before this run started stops the
    // loop immediately.
    if cancelled.load(Ordering::SeqCst) {
        return Ok(());
    }

    let _: () = ctx
        .execute_activity(&charge_card_info(), state.cycles)
        .await
        .map_err(|e| e.to_string())?;

    // Wait for the next billing cycle. A cancel signal delivered during
    // this wait is dispatched to the handler above as soon as the wait
    // resolves, before the check below runs.
    ctx.timer("next-billing-cycle", 30 * 24 * 60 * 60)
        .await
        .map_err(|e| e.to_string())?;

    if cancelled.load(Ordering::SeqCst) {
        return Ok(());
    }

    let next = SubscriptionState {
        cycles: state.cycles + 1,
    };
    ctx.continue_as_new(serde_json::to_value(next).map_err(|e| e.to_string())?)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

fn main() {
    println!("temporal_port_subscription_renewal example loaded successfully!");
    println!();
    println!("This is a direct port of a Temporal TypeScript workflow. See:");
    println!("  docs/migrating-from-temporal.md#worked-example");
    println!();
    println!("Deliver the cancel signal over the management API:");
    println!("  curl -X POST /api/harvest/workflows/{{exec_id}}/signal \\");
    println!("       -H 'Content-Type: application/json' \\");
    println!("       -d '{{\"signal_name\": \"cancel\", \"payload\": null}}'");
    println!();
    println!("Register on a HarvestBuilder:");
    println!("  .workflows(workflows![subscription_renewal])");
    println!("  .activities(activities![charge_card])");
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::context::WorkflowCommand;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use autumn_harvest::types::ExecutionId;
    use serde_json::json;

    fn started_event(input: serde_json::Value) -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input,
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    /// The non-cancelled path: the activity runs, the timer resolves, and
    /// the loop continues via `continue_as_new` with `cycles` incremented.
    #[tokio::test]
    async fn no_cancellation_charges_and_continues() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("charge_card", |_| Ok(json!(null)))
            .run(
                subscription_renewal_info().handler,
                json!(SubscriptionState { cycles: 0 }),
            )
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let charged = outcome.events().iter().any(
            |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "charge_card"),
        );
        assert!(charged, "charge_card must be scheduled when not cancelled");

        let continued = outcome.events().iter().find_map(|e| match e {
            WorkflowEvent::WorkflowContinuedAsNew {
                input,
                new_workflow_type,
                ..
            } => Some((input.clone(), new_workflow_type.clone())),
            _ => None,
        });
        let (input, new_workflow_type) =
            continued.expect("workflow must continue-as-new when not cancelled");
        assert_eq!(
            new_workflow_type, None,
            "this is a same-type checkpoint, not a phase transition (issue #803)"
        );
        let next: SubscriptionState = serde_json::from_value(input).unwrap();
        assert_eq!(
            next.cycles, 1,
            "cycles must be incremented across the checkpoint"
        );
    }

    /// The cancelled path: a `cancel` signal recorded before the run started
    /// stops the loop before `charge_card` is ever scheduled. This is the
    /// direct port of `signal_handlers_subscription.rs`'s
    /// `cancel_signal_recorded_before_start_skips_billing_charge` test.
    #[tokio::test]
    async fn cancellation_recorded_before_start_skips_the_charge() {
        let events = vec![
            started_event(json!(SubscriptionState { cycles: 3 })),
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::Value::Null,
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let result = subscription_renewal(&ctx, SubscriptionState { cycles: 3 }).await;
        assert!(
            result.is_ok(),
            "a pre-recorded cancellation must complete the run cleanly: {result:?}"
        );

        let commands = ctx.drain_commands();
        assert!(
            !commands.iter().any(
                |c| matches!(c, WorkflowCommand::ScheduleActivity { name, .. } if name == "charge_card")
            ),
            "must not charge the card after observing a pre-recorded cancellation: {commands:?}"
        );
    }

    /// The recorded, non-cancelled history replays deterministically — the
    /// same guarantee `WorkflowReplayer` gives Temporal's `WorkflowReplayer`
    /// (same name, different SDK) does.
    #[tokio::test]
    async fn non_cancelled_history_replays_deterministically() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("charge_card", |_| Ok(json!(null)))
            .run(
                subscription_renewal_info().handler,
                json!(SubscriptionState { cycles: 0 }),
            )
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let report = outcome
            .replay_check(subscription_renewal_info().handler)
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "recorded history must replay deterministically:\n{report}"
        );
    }
}
