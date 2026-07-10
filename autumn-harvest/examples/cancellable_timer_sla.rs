#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Cancellable / renewable durable timers — issue #768.
//!
//! ## The problem
//!
//! The overwhelmingly common "wait for N seconds" primitive ([`WorkflowContext::timer`])
//! is *fire-once*: once armed it always fires. But real orchestrations need to
//! **cancel** a pending timer (the work finished early, so the SLA timer must
//! not fire) and **reset** it (a sliding-window / idle-session timeout that
//! renews on every event). Hand-rolling that with plain timers accumulates
//! *orphaned firings* — a stale timer that fires late into a workflow that has
//! already moved on.
//!
//! ## The solution: `ctx.start_timer(id, secs)` + `TimerHandle`
//!
//! `start_timer` arms a durable timer **without suspending** and returns a
//! [`TimerHandle`]. The handle can `cancel()`, `reset(secs)` (re-arm to a fresh
//! duration), or `await_fire()` (suspend until it fires or is cancelled). Each
//! `reset` cancels the old arming and starts a fresh one — intentionally O(1)
//! history per reset with **zero** orphaned firings.
//!
//! ```rust,ignore
//! use autumn_harvest::prelude::*;
//!
//! #[workflow]
//! async fn fulfill(ctx: &WorkflowContext) -> Result<(), String> {
//!     let mut sla = ctx.start_timer("fulfillment-sla", 3600); // non-suspending
//!     // ... renew the window when the customer adds items ...
//!     sla.reset(3600).map_err(|e| e.to_string())?;
//!     // ... shipped early? cancel so the SLA timer never fires ...
//!     sla.cancel().map_err(|e| e.to_string())?;
//!     Ok(())
//! }
//! ```
//!
//! ## Determinism & the fire-vs-cancel contract
//!
//! Arming records a single `TimerStarted`; cancel records a `TimerCancelled`
//! (an event added for this feature) so no `TimerFired` is ever produced for a
//! cancelled timer. A genuine fire-vs-cancel *race* is resolved deterministically
//! by **recorded-history order** — whichever of `TimerFired` / `TimerCancelled`
//! for the id is recorded first wins on every replay, regardless of wall-clock
//! timing on the replaying worker.
//!
//! ## Deadline is measured from `await_fire`, not from `start_timer`
//!
//! Arming records only the `TimerStarted` event — the durable `harvest_timers`
//! row (which makes the timer fire-eligible) is inserted when the timer is
//! **awaited**, with `fires_at = db_now + duration` at that instant. An
//! armed-but-unawaited timer therefore never fires while the workflow is parked
//! on some other wait, matching the "observed only at `await_fire()`" contract.
//! This suits idle-timeout / debounce / lease patterns (which always await or
//! reset). Fires are anchored to the Postgres clock, so absolute honoring is
//! subject to worker↔database skew — this is not a skew-proof absolute-time
//! guarantee. (This example arms and awaits in the same cycle, so its SLA
//! deadline coincides with `start_timer` time.)
//!
//! ## Composition note
//!
//! This slice ships the core primitive. Composing an armed handle with a signal
//! wait in one `select!`-style call (an idle-timeout that resets on the next
//! event *signal*) is a natural follow-up: use `ctx.receive_signal_timeout`
//! (issue #476) for the two-branch signal-or-deadline shape today, or drive the
//! reset from workflow logic as below.
//!
//! Run the embedded tests with:
//! ```bash
//! cargo test -p autumn-harvest --no-default-features --features testing \
//!     --example cancellable_timer_sla
//! ```

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

/// A deterministic fulfillment plan (drives the example without external
/// signals so it is fully replay-deterministic).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FulfillmentPlan {
    /// How many times the customer extended the order (each renews the SLA).
    pub extensions: u32,
    /// Whether the order shipped before the SLA deadline (cancel) or the SLA
    /// window elapsed (await the fire).
    pub ships_before_deadline: bool,
}

/// Arms a fulfillment-SLA timer, renews it on each extension, then either
/// cancels it (shipped early) or awaits the breach.
#[workflow]
async fn fulfillment_sla(ctx: &WorkflowContext, plan: FulfillmentPlan) -> Result<String, String> {
    // Non-suspending: arm the SLA timer and keep running.
    let mut sla = ctx.start_timer("fulfillment-sla", 3600);

    // Each extension resets (renews) the SLA window. Because reset cancels the
    // prior arming, there is never an orphaned timer left to fire late.
    for _ in 0..plan.extensions {
        sla.reset(3600).map_err(|e| e.to_string())?;
    }

    if plan.ships_before_deadline {
        // Shipped before the deadline — cancel so the SLA timer never fires.
        sla.cancel().map_err(|e| e.to_string())?;
        Ok("shipped".to_string())
    } else {
        // Wait for the SLA deadline (or a cancellation) to resolve.
        match sla.await_fire().await.map_err(|e| e.to_string())? {
            TimerOutcome::Fired => Ok("sla_breached".to_string()),
            TimerOutcome::Cancelled => Ok("cancelled".to_string()),
        }
    }
}

fn main() {
    let _wfs = workflows![fulfillment_sla];
    println!("cancellable_timer_sla example compiled successfully");
    println!();
    println!("ctx.start_timer(id, secs) -> TimerHandle:");
    println!("  - non-suspending arm; handle.cancel() / handle.reset(secs) / handle.await_fire()");
    println!("  - cancel deletes the durable row + records TimerCancelled — no orphaned fire");
    println!("  - reset = cancel + re-arm (O(1) history/reset, zero orphaned firings)");
    println!("  - fire-vs-cancel decided by recorded-history order (replay-safe)");
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use serde_json::json;

    fn count_timer_cancels(events: &[WorkflowEvent]) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::TimerCancelled { .. }))
            .count()
    }
    fn has_timer_fired(events: &[WorkflowEvent]) -> bool {
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. }))
    }

    #[tokio::test]
    async fn ships_early_cancels_the_sla_timer() {
        let plan = FulfillmentPlan {
            extensions: 2,
            ships_before_deadline: true,
        };
        let outcome = WorkflowTestEnv::new()
            .run(fulfillment_sla_info().handler, json!(plan))
            .await;
        assert_eq!(outcome.result, Ok(json!("shipped")));

        let events = outcome.events();
        // Two resets + one final cancel = three TimerCancelled; the SLA never fires.
        assert_eq!(count_timer_cancels(&events), 3, "events: {events:?}");
        assert!(
            !has_timer_fired(&events),
            "a cancelled SLA timer must never fire"
        );

        let report = outcome.replay_check(fulfillment_sla_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay self-check failed:\n{report}"
        );
    }

    #[tokio::test]
    async fn sla_breach_awaits_the_fire() {
        let plan = FulfillmentPlan {
            extensions: 0,
            ships_before_deadline: false,
        };
        let outcome = WorkflowTestEnv::new()
            .run(fulfillment_sla_info().handler, json!(plan))
            .await;
        assert_eq!(outcome.result, Ok(json!("sla_breached")));

        let events = outcome.events();
        assert!(
            has_timer_fired(&events),
            "the SLA timer must fire: {events:?}"
        );
        assert_eq!(count_timer_cancels(&events), 0);

        let report = outcome.replay_check(fulfillment_sla_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay self-check failed:\n{report}"
        );
    }

    #[tokio::test]
    async fn renewed_then_shipped_leaves_no_orphaned_fire() {
        let plan = FulfillmentPlan {
            extensions: 3,
            ships_before_deadline: true,
        };
        let outcome = WorkflowTestEnv::new()
            .run(fulfillment_sla_info().handler, json!(plan))
            .await;
        assert_eq!(outcome.result, Ok(json!("shipped")));

        let events = outcome.events();
        // Three resets + one ship cancel; never fires.
        assert_eq!(count_timer_cancels(&events), 4, "events: {events:?}");
        assert!(!has_timer_fired(&events));

        let report = outcome.replay_check(fulfillment_sla_info().handler).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay self-check failed:\n{report}"
        );
    }
}
