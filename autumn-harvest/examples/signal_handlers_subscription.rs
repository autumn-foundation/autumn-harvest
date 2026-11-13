#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Push-based signal handlers (issue #546).
//!
//! Demonstrates `ctx.register_signal_handler` / `register_signal_handler_raw`:
//! a long-running "subscription" workflow reacts to `cancel`/`pause`/`upgrade`
//! signals that can arrive at *any* point, with **zero** hand-coded
//! `select!`-style interleaving. Compare this to the pull-based
//! `wait_for_signal`/`receive_signal` primitive, which blocks one specific
//! point in the workflow body waiting for the next matching signal.
//!
//! ## Pull vs push — when to reach for which
//!
//! | | Pull (`wait_for_signal` / `receive_signal`) | Push (`register_signal_handler`) |
//! |---|---|---|
//! | Shape | One `.await` at one code point | One registration call, fires whenever |
//! | Fits | "Block here until X happens" (approval gate, checkout) | "React to X at any time while doing other things" (cancel/pause/upgrade mid-run) |
//! | Mixing concerns | Awkward with more than one concurrent signal | Natural — register one handler per signal name |
//! | Return value | Caller gets the payload directly | Fire-and-forget; handler mutates captured state |
//!
//! Buffered signals are never dropped. A signal can arrive before its handler
//! is registered, for example on an earlier workflow-task cycle. It is still
//! dispatched once the handler exists, in the same workflow task. The pull and
//! push styles coexist for different signal names with no special
//! coordination. Even for the *same* name, at most one style receives a given
//! recorded `SignalReceived` event, so there is no double delivery.
//!
//! **Dispatch timing.** `register_signal_handler*` only stores the handler. It
//! does not fire inline. Dispatch happens on the *next* history-consulting call
//! of the workflow body: an activity, timer, another signal wait, or a
//! deterministic primitive like `system_now()`. At the latest, it happens once
//! more when the cycle ends. So read handler-mutated state only after at least
//! one such call. A read on the next line, with nothing in between, sees
//! pre-dispatch state. The `system_now()` call below is the minimal way to
//! force dispatch when nothing else in the body would.
//!
//! ## Handlers must be idempotent, replay-safe state mutation only
//!
//! A signal handler has no persisted "already delivered" marker, unlike the
//! `UpdateCompleted` event of the `update` primitive. So a handler fires once
//! per recorded `SignalReceived` event **every time the workflow replays from
//! the top of its function**. In this example that is after every activity
//! completes, and after a worker restart. It is not once over the execution's
//! lifetime.
//!
//! That is safe for pure state reconstruction, exactly as shown below. `state`
//! is a fresh `Arc<Mutex<..>>`, created at the top of the function on every
//! cycle. So a replay of the same recorded signals always rebuilds the
//! identical final value.
//!
//! It is **not** safe for a handler with a non-idempotent side effect. Examples
//! are an external HTTP call, a log line meant to fire once, or an increment
//! sent to another system. That side effect repeats on every later replay of
//! this history. Keep signal handlers to mutating captured in-memory state.
//! Route anything with an external side effect through a regular activity.
//!
//! Deliver signals over the existing management route:
//!   POST /api/harvest/workflows/{exec_id}/signal
//!   { "signal_name": "cancel", "payload": { "reason": "customer_requested" } }

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use autumn_harvest::prelude::*;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct SubscriptionState {
    pub cancelled: bool,
    pub cancel_reason: Option<String>,
    pub paused: bool,
    pub tier: String,
}

#[derive(serde::Deserialize)]
struct CancelRequest {
    reason: String,
}

#[derive(serde::Deserialize)]
struct UpgradeRequest {
    tier: String,
}

#[activity(start_to_close = "30s")]
async fn charge_billing_cycle(_ctx: &ActivityContext, tier: String) -> Result<(), String> {
    println!("charging billing cycle for tier: {tier}");
    Ok(())
}

/// Lock the subscription state, recovering from poisoning instead of
/// panicking.
///
/// A push signal handler runs on the workflow's own replay/live cycle. A panic
/// inside one is caught and logged, not propagated (fire-and-forget delivery).
/// But a panic *while it holds this lock* still poisons the `std::sync::Mutex`,
/// per ordinary Rust semantics. Catching the panic does not "un-poison" a mutex
/// that the unwinding code held. With `PoisonError::into_inner`, a later
/// `.lock()` in the same cycle, such as the final read below, does not panic
/// on that unrelated poisoned lock.
fn lock_state(state: &Mutex<SubscriptionState>) -> MutexGuard<'_, SubscriptionState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A long-running subscription workflow.
///
/// Three signal handlers, registered up front, react to `cancel` / `pause` /
/// `upgrade` at any point while the workflow runs its billing loop. It needs no
/// `tokio::select!`, no `await_condition` polling loop, and no hand-rolled
/// interleaving.
#[workflow]
async fn subscription(
    ctx: &WorkflowContext,
    initial_tier: String,
) -> Result<SubscriptionState, String> {
    let state = Arc::new(Mutex::new(SubscriptionState {
        tier: initial_tier,
        ..Default::default()
    }));

    // React to a cancellation at any point in the subscription lifecycle.
    // Pure state mutation only -- no logging/external calls here, since a
    // cold replay redelivers this same recorded signal (see the module docs).
    let cancel_state = state.clone();
    ctx.register_signal_handler("cancel", move |req: CancelRequest| {
        let mut s = lock_state(&cancel_state);
        s.cancelled = true;
        s.cancel_reason = Some(req.reason);
    });

    // React to a pause at any point (billing loop below checks the flag).
    let pause_state = state.clone();
    ctx.register_signal_handler_raw("pause", move |_payload| {
        lock_state(&pause_state).paused = true;
    });

    // React to a tier upgrade/downgrade at any point.
    let upgrade_state = state.clone();
    ctx.register_signal_handler("upgrade", move |req: UpgradeRequest| {
        lock_state(&upgrade_state).tier = req.tier;
    });

    // Registration only stores a handler. It does not dispatch inline.
    // Dispatch happens on the next history-consulting call of the workflow
    // body: an activity, a timer, or a cheap deterministic primitive like
    // `system_now()` below. A signal recorded at the start of this cycle is
    // visible to the reads below only *after* such a call. A real
    // activity/timer/loop provides that for free. This example checks the
    // flags first, so `system_now()` forces dispatch here.
    let _ = ctx.system_now();

    // The handlers above replay deterministically at the top of every cycle,
    // wherever the rest of the body has reached.
    let tier_to_charge = lock_state(&state).tier.clone();
    if !lock_state(&state).cancelled && !lock_state(&state).paused {
        let _: () = ctx
            .execute_activity(&charge_billing_cycle_info(), tier_to_charge)
            .await
            .map_err(|e| e.to_string())?;
    }

    let final_state = lock_state(&state).clone();
    Ok(final_state)
}

fn main() {
    println!("signal_handlers_subscription example loaded successfully!");
    println!();
    println!("Register three push-based handlers up front:");
    println!("  ctx.register_signal_handler(\"cancel\", |req: CancelRequest| {{ .. }});");
    println!("  ctx.register_signal_handler_raw(\"pause\", |payload| {{ .. }});");
    println!("  ctx.register_signal_handler(\"upgrade\", |req: UpgradeRequest| {{ .. }});");
    println!();
    println!("Deliver a signal over the management API:");
    println!("  curl -X POST /api/harvest/workflows/{{exec_id}}/signal \\");
    println!("       -H 'Content-Type: application/json' \\");
    println!(
        "       -d '{{\"signal_name\": \"cancel\", \"payload\": {{\"reason\": \"customer_requested\"}}}}'"
    );
    println!();
    println!("Register on a HarvestBuilder:");
    println!("  .workflows(workflows![subscription])");
    println!("  .activities(activities![charge_billing_cycle])");
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::context::WorkflowCommand;
    use autumn_harvest::types::ExecutionId;

    fn started_event(input: &str) -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!(input),
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    #[tokio::test]
    async fn cancel_signal_recorded_before_start_skips_billing_charge() {
        // Regression (PR #890 review): the billing decision below must observe
        // a signal recorded before this cycle starts. The only command between
        // the three `register_signal_handler*` calls and this cancellation
        // check is the `system_now()` flush point.
        let events = vec![
            started_event("gold"),
            WorkflowEvent::SignalReceived {
                signal_name: "cancel".into(),
                payload: serde_json::json!({"reason": "customer_requested"}),
            },
        ];
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

        let result = subscription(&ctx, "gold".to_string()).await;
        let state = result.expect("workflow should complete without charging");
        assert!(
            state.cancelled,
            "a cancellation recorded before the cycle started must be observed"
        );
        assert_eq!(state.cancel_reason.as_deref(), Some("customer_requested"));

        let commands = ctx.drain_commands();
        assert!(
            !commands
                .iter()
                .any(|c| matches!(c, WorkflowCommand::ScheduleActivity { name, .. } if name == "charge_billing_cycle")),
            "must not charge the billing cycle after observing a pre-recorded \
             cancellation: {commands:?}"
        );
    }

    #[tokio::test]
    async fn no_signals_recorded_charges_billing_cycle() {
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![started_event("gold")]);

        // The activity never completes in this test, so race the workflow
        // future against a short sleep and inspect what it queued.
        let fut = subscription(&ctx, "gold".to_string());
        tokio::pin!(fut);
        tokio::select! {
            _ = &mut fut => panic!("workflow must suspend waiting on the activity"),
            () = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }

        let commands = ctx.drain_commands();
        assert!(
            commands
                .iter()
                .any(|c| matches!(c, WorkflowCommand::ScheduleActivity { name, .. } if name == "charge_billing_cycle")),
            "must charge the billing cycle when no cancellation/pause was recorded: {commands:?}"
        );
    }
}
