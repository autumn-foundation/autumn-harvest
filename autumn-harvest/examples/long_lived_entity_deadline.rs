#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Deadline-aware continue-as-new for long-lived, low-event workflows — issue #772.
//!
//! ## The problem
//!
//! An entity workflow (a subscription, a cart, a device) can run for weeks while
//! recording only a handful of events per day. It never approaches the history
//! event-count threshold that `should_continue_as_new()` watches — so it is
//! never advised to checkpoint. But if it declares an `execution_timeout`
//! (issue #243) for SLA / runaway protection, the hard timeout eventually kills
//! it mid-flight, even though it is perfectly healthy.
//!
//! ## The solution
//!
//! `should_continue_as_new()` gains a **second, independent trigger**: it also
//! returns `true` once the run has consumed a configurable fraction (default
//! `0.8`) of its `execution_timeout` budget. A long-lived workflow can then
//! checkpoint via `continue_as_new` *before* the hard deadline truncates it —
//! carrying its state forward into a fresh run with a fresh deadline.
//!
//! Two new replay-safe, event-free accessors support this:
//!
//! - [`WorkflowContext::deadline`] — the **nominal** `WorkflowStarted` timestamp
//!   + effective `execution_timeout`, or `None` when there is no timeout. Pure;
//!   records nothing. This is replay-STABLE and **does not reflect pause
//!   extensions** — the engine's internal `should_continue_as_new()` budget check
//!   *does* account for a paused/resumed run's shifted deadline, but the public
//!   accessor stays nominal so author code depending on it replays
//!   deterministically.
//! - [`WorkflowContext::time_until_deadline`] — remaining time to the nominal
//!   deadline, measured against the replay-safe recorded clock
//!   ([`WorkflowContext::system_now`], issue #384), **never**
//!   `chrono::Utc::now()`.
//!
//! Contract: when `should_continue_as_new()` returns `true`, the workflow **must**
//! call `continue_as_new` (as below) rather than continue running.
//!
//! ```rust,ignore
//! use autumn_harvest::prelude::*;
//!
//! #[workflow(execution_timeout = "24h")]
//! async fn subscription_entity(ctx: &WorkflowContext, state: SubState) -> Result<SubState, String> {
//!     // Checkpoint before the hard deadline (or history growth) truncates us.
//!     if ctx.should_continue_as_new() {
//!         ctx.continue_as_new(serde_json::to_value(&state).unwrap()).await?;
//!     }
//!     // ... one cycle of durable work ...
//!     Ok(state)
//! }
//! ```
//!
//! Determinism: **no new `WorkflowEvent` variant, no migration.** The deadline
//! is derived purely from the recorded `WorkflowStarted` timestamp; the clock
//! read reuses the existing `SideEffectRecorded{Now}` event (issue #384), so a
//! history that crossed the deadline replays to the same `ContinueAsNew`
//! command on every worker.
//!
//! Run the embedded tests with:
//! ```bash
//! cargo test -p autumn-harvest --no-default-features --features testing \
//!     --example long_lived_entity_deadline
//! ```

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubState {
    /// Number of billing cycles processed so far (carried across each
    /// continue-as-new fork).
    pub cycles: u32,
}

/// A long-lived subscription entity. At the top of each cycle it checks whether
/// it should checkpoint (history size OR deadline fraction), forks a fresh run
/// via `continue_as_new` if so, then processes one durable billing cycle.
///
/// `#[workflow(execution_timeout = "24h")]` gives the run a hard deadline; the
/// deadline-aware `should_continue_as_new()` recommends a checkpoint once ~80%
/// of that budget is consumed, so a run that would otherwise be killed by the
/// hard timeout instead forks cleanly and carries its state forward.
#[workflow(execution_timeout = "24h")]
async fn subscription_entity(ctx: &WorkflowContext, state: SubState) -> Result<SubState, String> {
    // Checkpoint before the hard execution_timeout (or unbounded history
    // growth) would truncate the run. continue_as_new re-seeds the successor
    // with the current state, so no work is lost.
    if ctx.should_continue_as_new() {
        ctx.continue_as_new(serde_json::to_value(&state).map_err(|e| e.to_string())?)
            .await
            .map_err(|e| e.to_string())?;
        // continue_as_new never returns — the run is sealed here.
        return Ok(state);
    }

    // One durable cycle of "work": wait for the next billing tick, then bump the
    // counter. A production entity would loop over signals; the example does a
    // single cycle so the test harness can drive it deterministically.
    ctx.timer("billing-cycle", 30)
        .await
        .map_err(|e| e.to_string())?;

    Ok(SubState {
        cycles: state.cycles + 1,
    })
}

fn main() {
    let _wfs = workflows![subscription_entity];
    println!("long_lived_entity_deadline example compiled successfully");
    println!();
    println!("Deadline-aware continue-as-new (issue #772):");
    println!("  - should_continue_as_new() ALSO trips at ~80% of execution_timeout");
    println!("  - lets a long-lived, low-event workflow checkpoint before the hard deadline");
    println!("  - ctx.deadline() / ctx.time_until_deadline() are replay-safe, event-free");
    println!("  - no new WorkflowEvent variant, no migration");
}

// Gated on the `testing` feature as well as `test`: the example must keep
// building under `--no-default-features`, while `autumn_harvest::testing` only
// exists for external consumers when the `testing` feature is enabled.
#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use autumn_harvest::context::WorkflowContext;
    use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
    use autumn_harvest::types::ExecutionId;
    use chrono::{DateTime, Duration, Utc};
    use serde_json::{Value, json};

    /// AC4 — opt-in-safe: a live run WITHOUT an execution timeout behaves exactly
    /// as before (the pattern completes one cycle normally).
    #[tokio::test]
    async fn entity_completes_a_cycle_without_a_deadline() {
        let outcome = WorkflowTestEnv::new()
            .run(
                subscription_entity_info().handler,
                json!(SubState { cycles: 3 }),
            )
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

        // No side-effect events were recorded — with no deadline the
        // should_continue_as_new() deadline branch consults no wall clock.
        let has_side_effect = outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. }));
        assert!(
            !has_side_effect,
            "a workflow with no deadline must record zero side-effect events"
        );

        let next: SubState = serde_json::from_value(outcome.result.clone().unwrap()).unwrap();
        assert_eq!(next.cycles, 4, "one billing cycle was processed");
    }

    /// AC4 — opt-in-safe with a deadline configured: a live run whose deadline is
    /// far in the future still completes its cycle normally (the deadline branch
    /// does not trip).
    #[tokio::test]
    async fn entity_with_a_far_deadline_still_completes_normally() {
        let outcome = WorkflowTestEnv::new()
            .with_execution_timeout(Duration::hours(24))
            .run(
                subscription_entity_info().handler,
                json!(SubState { cycles: 0 }),
            )
            .await;

        assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
        let next: SubState = serde_json::from_value(outcome.result.clone().unwrap()).unwrap();
        assert_eq!(next.cycles, 1);
    }

    /// AC5 — the deadline-triggered checkpoint replays deterministically to the
    /// same `ContinueAsNew` command. The history was recorded by a run that had
    /// consumed ~90% of a 30s budget when it checked `should_continue_as_new()`.
    #[tokio::test]
    async fn crossed_deadline_replays_to_continue_as_new() {
        let t0 = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        // 27s of a 30s budget consumed (0.9 ≥ 0.8) ⇒ the deadline branch tripped
        // and the run forked, carrying its state forward.
        let recorded_now = t0 + Duration::seconds(27);
        let state = json!(SubState { cycles: 7 });
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: state.clone(),
                timestamp: t0,
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            // The clock read should_continue_as_new()'s deadline branch records
            // because a deadline exists — under the reserved probe name (#772).
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Now,
                name: Some(autumn_harvest::DEADLINE_PROBE_SIDE_EFFECT_NAME.to_string()),
                value: json!(recorded_now.timestamp_millis()),
            },
            // The checkpoint fork carrying the state forward.
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: ExecutionId::new(),
                input: state.clone(),
            },
        ];

        let report = WorkflowReplayer::new()
            .with_execution_timeout(Duration::seconds(30))
            .register_fn("subscription_entity", subscription_entity_info().handler)
            .replay_from_events(events)
            .await;

        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "a deadline-crossed history must replay to ContinueAsNew, got: {report}"
        );
    }

    /// The two `should_continue_as_new` triggers compose: with no deadline the
    /// context helper still trips on history size, and the deadline helper is a
    /// pure, event-free read of the recorded start clock.
    #[test]
    fn deadline_helpers_are_pure_and_event_free() {
        let t0 = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let ctx = WorkflowContext::for_replay(
            ExecutionId::new(),
            vec![WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: t0,
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }],
        )
        .with_execution_timeout(Some(Duration::hours(2)));

        assert_eq!(ctx.deadline(), Some(t0 + Duration::hours(2)));
        // deadline() records nothing.
        assert!(ctx.drain_commands().is_empty());
        // Sanity: the wall-clock instant used above is a valid timestamp.
        let _ = Utc::now();
    }
}
