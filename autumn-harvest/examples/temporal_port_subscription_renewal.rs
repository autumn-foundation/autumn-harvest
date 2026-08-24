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
    pub subscription_id: String,
    pub cycles: u32,
}

/// A downstream idempotency key, plus the cycle number it applies to. See
/// playbook step 4 in the migration guide: every side-effecting activity
/// call needs its own caller-supplied key, derived from state already in
/// history, so a retried attempt cannot charge the card twice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChargeRequest {
    pub idempotency_key: String,
    pub cycles: u32,
}

#[activity(start_to_close = "30s")]
async fn charge_card(_ctx: &ActivityContext, req: ChargeRequest) -> Result<(), String> {
    // A real billing call goes here, passing req.idempotency_key through to
    // the payment provider's own idempotency-key parameter.
    println!(
        "charged card for cycle {} (idempotency key: {})",
        req.cycles, req.idempotency_key
    );
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

    // Derive the key from `subscription_id`, not from `ctx.workflow_id()` or
    // the execution id. Playbook step 4 in the migration guide needs a key
    // the caller supplies before routing the request to either engine.
    // `subscription_id` is that key: it names the same subscription on both
    // sides of a dual-run cutover, no matter which engine's own internal id
    // the request happens to carry.
    let idempotency_key = format!("{}-cycle-{}", state.subscription_id, state.cycles);
    let _: () = ctx
        .execute_activity(
            &charge_card_info(),
            ChargeRequest {
                idempotency_key,
                cycles: state.cycles,
            },
        )
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
        subscription_id: state.subscription_id.clone(),
        cycles: state.cycles + 1,
    };
    ctx.continue_as_new(serde_json::to_value(next).map_err(|e| e.to_string())?)
        .await
        .map_err(|e| e.to_string())?;

    unreachable!("continue_as_new suspends the run and never resolves")
}

fn main() {
    println!("temporal_port_subscription_renewal example loaded successfully!");
    println!();
    println!("This is a direct port of a Temporal TypeScript workflow. See:");
    println!("  docs/migrating-from-temporal.md#worked-example");
    println!();
    println!("Deliver the cancel signal over the management API:");
    println!("  curl -X POST /api/harvest/workflows/{{exec_id}}/signal/cancel \\");
    println!("       -H 'Content-Type: application/json' \\");
    println!("       -d 'null'");
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
                json!(SubscriptionState {
                    subscription_id: "sub-42".to_string(),
                    cycles: 0,
                }),
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
        let subscription_id = "sub-42".to_string();
        let events = vec![
            started_event(json!(SubscriptionState {
                subscription_id: subscription_id.clone(),
                cycles: 3,
            })),
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::Value::Null,
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // The cancellation check must precede the only suspending call in
        // this path (`execute_activity`). If a regression removes that
        // ordering, the workflow blocks forever on an oneshot channel with
        // no executor to resolve it. Bound the await so such a regression
        // fails fast with a clear message, instead of hanging the test.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            subscription_renewal(
                &ctx,
                SubscriptionState {
                    subscription_id,
                    cycles: 3,
                },
            ),
        )
        .await
        .expect("workflow must not suspend when a cancellation was already recorded");
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

    /// The mid-wait cancelled path: `charge_card` already ran, and a
    /// `cancel` signal arrived while the workflow was suspended on the
    /// `next-billing-cycle` timer. The module docs above claim the
    /// `ctx.timer(...)` call itself is the flush point for a signal
    /// recorded between `TimerStarted` and `TimerFired`. This test proves
    /// it: the loop must stop, and must not push a `ContinueAsNew`
    /// command.
    #[tokio::test]
    async fn cancellation_recorded_mid_wait_stops_the_loop_before_continuing() {
        let activity_id = ActivityExecId::new();
        let subscription_id = "sub-42".to_string();
        let events = vec![
            started_event(json!(SubscriptionState {
                subscription_id: subscription_id.clone(),
                cycles: 3,
            })),
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".into(),
                input: json!(3),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: serde_json::Value::Null,
            },
            WorkflowEvent::TimerStarted {
                timer_id: TimerId::new("next-billing-cycle"),
                duration_secs: 30 * 24 * 60 * 60,
            },
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::Value::Null,
            },
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("next-billing-cycle"),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        // Every suspending call in this path (the activity, then the
        // timer) is already resolved by the recorded history above, so
        // this replay never touches a live oneshot channel. Bound it
        // anyway, for the same defense-in-depth reason as the pre-start
        // test above.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            subscription_renewal(
                &ctx,
                SubscriptionState {
                    subscription_id,
                    cycles: 3,
                },
            ),
        )
        .await
        .expect("workflow must not suspend once the full history has been replayed");
        assert!(
            result.is_ok(),
            "a mid-wait cancellation must complete the run cleanly: {result:?}"
        );

        let commands = ctx.drain_commands();
        assert!(
            !commands
                .iter()
                .any(|c| matches!(c, WorkflowCommand::ContinueAsNew { .. })),
            "must not continue the loop after observing a mid-wait cancellation: {commands:?}"
        );
    }

    /// The recorded, non-cancelled history replays deterministically — the
    /// same guarantee `WorkflowReplayer` gives Temporal's own
    /// `WorkflowReplayer` (same name, in the Temporal .NET and PHP SDKs)
    /// does.
    #[tokio::test]
    async fn non_cancelled_history_replays_deterministically() {
        let outcome = WorkflowTestEnv::new()
            .mock_activity("charge_card", |_| Ok(json!(null)))
            .run(
                subscription_renewal_info().handler,
                json!(SubscriptionState {
                    subscription_id: "sub-42".to_string(),
                    cycles: 0,
                }),
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

    /// The idempotency key must come from `subscription_id`, an identity
    /// the caller supplies before routing the request to either engine.
    /// It must never come from `ctx.workflow_id()` or the execution id.
    ///
    /// Playbook step 4 in the migration guide states this rule. A key
    /// minted from harvest-internal identity would mint a *different* key
    /// after a re-route between engines during a dual-run cutover. A
    /// retried charge would then slip through under the fresh key and
    /// double-charge the customer. `WorkflowTestEnv` leaves
    /// `ctx.workflow_id()` as an empty string by default, so a regression
    /// to that derivation would produce `"-cycle-0"`, not the asserted
    /// `"sub-42-cycle-0"`, and this test would catch it.
    #[tokio::test]
    async fn idempotency_key_is_derived_from_subscription_id_not_workflow_identity() {
        let seen_key: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        let seen_key_handle = seen_key.clone();
        let outcome = WorkflowTestEnv::new()
            .mock_activity("charge_card", move |input| {
                let req: ChargeRequest = serde_json::from_value(input)
                    .expect("charge_card input must deserialize as ChargeRequest");
                *seen_key_handle.lock().unwrap() = Some(req.idempotency_key);
                Ok(json!(null))
            })
            .run(
                subscription_renewal_info().handler,
                json!(SubscriptionState {
                    subscription_id: "sub-42".to_string(),
                    cycles: 0,
                }),
            )
            .await;
        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        let key = seen_key
            .lock()
            .unwrap()
            .clone()
            .expect("charge_card must have been called");
        assert_eq!(
            key, "sub-42-cycle-0",
            "the idempotency key must come from subscription_id, not from \
             ctx.workflow_id() or the execution id"
        );
    }
}
