//! Tests for `WorkflowTestEnv` — the in-process workflow unit-test harness (issue #250).
//!
//! Three required example tests per AC:
//! - (a) Happy-path workflow with one activity.
//! - (b) Workflow whose first attempt fails and retry succeeds.
//! - (c) Workflow that races a timer against a signal.
//!
//! Additional tests cover per-attempt mocks, child-workflow stubbing,
//! local-activity mocking, event-log inspection, cancellation, and the
//! replay-mode self-check.
//!
//! Run with:
//!   cargo test -p autumn-harvest --test `workflow_test_env_tests` \
//!     --features testing --no-default-features

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::ExecutionId;
use autumn_harvest::context::{SessionOptions, WorkflowContext};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
use autumn_harvest::types::ParentClosePolicy;
use serde_json::{Value, json};

// ──────────────────────────── workflow helpers ────────────────────────────────

/// (a) One activity, returns its output wrapped.
fn one_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .execute_activity_raw("send_email", json!({"to": "user@example.com"}), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"sent": result}))
    })
}

/// (b) Explicitly retries a failing activity by calling it again on error.
fn retry_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = match ctx
            .execute_activity_raw("charge_card", json!({"amount": 100}), "payments")
            .await
        {
            Ok(v) => v,
            Err(_first_err) => {
                // Explicit retry: call the activity again after first failure.
                ctx.execute_activity_raw("charge_card", json!({"amount": 100}), "payments")
                    .await
                    .map_err(|e| e.to_string())?
            }
        };
        Ok(result)
    })
}

/// Publishes an operator status breadcrumb at each phase via
/// `ctx.set_current_details`, overwriting it between activities and clearing
/// it (empty string) before completing (issue #593).
fn current_details_status_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.set_current_details("step 1/2: charging card");
        let charge = ctx
            .execute_activity_raw("charge_card", json!({"amount": 100}), "payments")
            .await
            .map_err(|e| e.to_string())?;
        ctx.set_current_details("step 2/2: sending receipt");
        let receipt = ctx
            .execute_activity_raw("send_email", json!({"to": "user@example.com"}), "default")
            .await
            .map_err(|e| e.to_string())?;
        // Clear the breadcrumb on completion.
        ctx.set_current_details("");
        Ok(json!({"charge": charge, "receipt": receipt}))
    })
}

/// (c) Races a timer against a signal; returns which path fired first.
fn race_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::select! {
            biased;
            r = ctx.timer("race", 60) => {
                r.map_err(|e| e.to_string())?;
                Ok(json!("timer"))
            }
            payload = ctx.wait_for_signal("approve") => {
                Ok(json!({"signal": payload.map_err(|e| e.to_string())?}))
            }
        }
    })
}

/// (issue #768) Arm a cancellable timer and await its outcome.
fn cancellable_await_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(json!(format!("{outcome:?}")))
    })
}

/// (Codex P2 round 5, issue #768 — FIX 1) Arm `idle` at 300s, then call
/// `start_timer("idle", 600)` again with NO cancel/reset between. The second call
/// is a duration-preserving idempotent no-op: the returned handle MUST carry the
/// ORIGINAL 300s, not 600, so the durable row / virtual clock stay consistent with
/// the single recorded `TimerStarted(idle, 300)`. Emits the armed duration so the
/// test can assert 300 (before the fix the handle carried 600).
fn duplicate_active_start_timer_diff_duration_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _first = ctx.start_timer("idle", 300);
        let handle = ctx.start_timer("idle", 600); // duplicate active — MUST preserve 300
        let armed = handle.duration_secs();
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(json!({ "armed": armed, "outcome": format!("{outcome:?}") }))
    })
}

/// (issue #768) Arm a cancellable timer then cancel it (fire-and-forget) — the
/// timer must never fire.
fn cancellable_cancel_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        handle.cancel().map_err(|e| e.to_string())?;
        Ok(json!("cancelled"))
    })
}

/// (issue #768) Arm, reset once, then await — the reset re-arms with zero
/// orphaned firings.
fn cancellable_reset_once_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mut handle = ctx.start_timer("idle", 300);
        handle.reset(300).map_err(|e| e.to_string())?;
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(json!(format!("{outcome:?}")))
    })
}

/// (Codex P2, issue #768) Arm a cancellable timer, then suspend on an activity
/// in the SAME cycle. The armed timer must NOT fire before the activity
/// completes — mirrors the real worker, which records the arm and leaves the
/// workflow parked on the activity.
fn arm_timer_then_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _handle = ctx.start_timer("idle", 300);
        let result = ctx
            .execute_activity_raw("work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"work": result}))
    })
}

/// (Codex P2, issue #768) Arm a cancellable timer, suspend on an activity in the
/// SAME cycle, then `await_fire()` the timer AFTER the activity completes. The
/// timer's `TimerStarted` is recorded in the mixed batch (competing suspension,
/// so no fire yet); the later bookkeeping-only `await_fire` reschedules the
/// already-armed row and it fires — never interleaved before `ActivityCompleted`.
fn arm_timer_activity_then_await_fire_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        let result = ctx
            .execute_activity_raw("work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(json!({"work": result, "timer": format!("{outcome:?}")}))
    })
}

/// (issue #768, matrix #6) Arm a cancellable timer, suspend on an activity, then
/// `cancel()` and `await_fire()` AFTER the activity completes. The armed timer
/// must resolve `Cancelled` and never fire.
fn arm_timer_activity_cancel_then_await_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        let result = ctx
            .execute_activity_raw("work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        handle.cancel().map_err(|e| e.to_string())?;
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(json!({"work": result, "timer": format!("{outcome:?}")}))
    })
}

/// (Codex P2 round 4, issue #768 — FIX A primary flow) Arm a cancellable timer,
/// suspend on an activity, then `reset()` and `await_fire()` AFTER the activity
/// completes. Under the round-4 model the armed timer's `harvest_timers` row is
/// inserted only at `await_fire`, so it can NEVER fire while the workflow is
/// parked on the activity — no spurious `TimerFired` that would break the
/// subsequent `reset()` (an unconsumed fire → non-determinism) or diverge replay.
fn arm_timer_activity_reset_then_await_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mut handle = ctx.start_timer("idle", 300);
        let result = ctx
            .execute_activity_raw("work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        handle.reset(300).map_err(|e| e.to_string())?;
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(json!({"work": result, "timer": format!("{outcome:?}")}))
    })
}

/// (Codex P2 round 15, issue #768) Arm two cancellable timers with DIFFERENT
/// durations and `await_fire()` both concurrently via `tokio::join!`. Production
/// reschedules the parked task to the MINIMUM `fires_at` and ingests due timers in
/// deadline order, so the FAST timer (dur 1) must fire before the SLOW one (dur
/// 10) regardless of `join!` poll order (which polls `slow` first).
fn concurrent_await_two_timers_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Capture the virtual-clock start so the test can assert the elapsed after
        // both concurrent timers fire is the MAX deadline (10s), not the SUM (11s)
        // — issue #768, Codex P2 round 16.
        let start = ctx.now();
        let slow = ctx.start_timer("slow", 10);
        let fast = ctx.start_timer("fast", 1);
        let (s, f) = tokio::join!(slow.await_fire(), fast.await_fire());
        let now_elapsed_secs = (ctx.now() - start).num_seconds();
        Ok(json!({
            "slow": format!("{:?}", s.map_err(|e| e.to_string())?),
            "fast": format!("{:?}", f.map_err(|e| e.to_string())?),
            "now_elapsed_secs": now_elapsed_secs,
        }))
    })
}

/// (Codex P2 round 16, issue #768 — Finding 2) A sibling/handler-style
/// `cancel_timer("deadline")` targeting an id the main body uses as a CLASSIC
/// `ctx.timer` must be a NO-OP: it must NOT emit a `CancelTimer` (whose
/// worker-side `delete_pending_timer` would delete the classic timer's durable
/// row and hang its waiter). The classic timer therefore still fires and the run
/// replays deterministically.
fn cancel_timer_does_not_kill_a_classic_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // `join!` polls the classic timer FIRST — so `ctx.timer("deadline")` records
        // "deadline" as a classic id and parks — THEN the sibling branch calls
        // `cancel_timer("deadline")` on that now-classic id, exactly the Finding-2
        // "cancel while the classic timer is pending" race. The cancel must be a
        // no-op (no `CancelTimer`, no row delete), so the classic timer still fires.
        let (t, _c) = tokio::join!(ctx.timer("deadline", 5), async {
            ctx.cancel_timer("deadline")
        });
        t.map_err(|e| e.to_string())?;
        Ok(json!("classic_fired"))
    })
}

/// (Codex P2 round 4, issue #768 — FIX B) Arm a cancellable timer, cancel it, and
/// then start a CLASSIC `ctx.timer` with the SAME id in one task. The classic
/// timer must arm cleanly and fire — the same-batch `CancelTimer` must not leave
/// the classic timer rescheduled to a deleted row (no hang), and the run must
/// replay deterministically.
fn cancel_then_classic_timer_same_id_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        handle.cancel().map_err(|e| e.to_string())?;
        // Classic (suspending) timer, SAME id — replaces the cancelled one.
        ctx.timer("idle", 60).await.map_err(|e| e.to_string())?;
        Ok(json!("classic_fired"))
    })
}

/// (Codex P2 round 8, issue #768 — FIX A) Arm `idle` at 300s via a handle, then
/// reset it to 600s through `ctx.reset_timer` (NOT the handle), and await the
/// original handle. `await_fire()` must use the CURRENT armed duration (600) —
/// read from the logical state map — for the live deadline AND the virtual
/// clock, NOT the handle's stale cached 300. Before the fix the handle's cached
/// 300 armed the live deadline / advanced the clock even though the recorded
/// `TimerStarted` was 600.
fn reset_via_ctx_then_await_handle_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        // Reset through the CONTEXT, not the handle — the handle's cached
        // duration stays 300 while the authoritative armed duration becomes 600.
        ctx.reset_timer("idle", 600).map_err(|e| e.to_string())?;
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        let now = ctx.now().timestamp();
        Ok(json!({ "outcome": format!("{outcome:?}"), "now": now }))
    })
}

/// Captures a deterministic side-effect (`system_now`) BEFORE suspending on an
/// activity. Exercises that the test harness persists the pre-suspension
/// `SideEffectRecorded` event so the next replay iteration does not see drift.
fn side_effect_then_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let captured_at = ctx.system_now().timestamp_millis();
        let result = ctx
            .execute_activity_raw("send_email", json!({"to": "user@example.com"}), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"sent": result, "captured_at": captured_at}))
    })
}

/// Two sequential activities, used for event-log ordering assertions.
fn two_step_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let a = ctx
            .execute_activity_raw("step_a", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let b = ctx
            .execute_activity_raw("step_b", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!([a, b]))
    })
}

/// Spawns a child workflow and returns its output.
fn parent_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .spawn_child_workflow_raw("child_processing", json!({"id": 42}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"child": result}))
    })
}

/// Uses a local activity.
fn local_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .execute_local_activity_raw("compute_hash", json!([1, 2, 3]), None, None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"hash": result}))
    })
}

/// Checks for cancellation and bails out early.
fn cancellable_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.check_cancellation().map_err(|e| e.to_string())?;
        let result = ctx
            .execute_activity_raw("long_op", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

fn terminal_detached_spawn_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_id = ctx
            .spawn_child_workflow_detached_raw(
                "terminal_detached_child",
                json!({"mode": "audit"}),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;
        Ok(json!({ "child_id": child_id.to_string() }))
    })
}

fn terminal_policy_detached_spawns_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let request_cancel_child = ctx
            .spawn_child_workflow_detached_raw(
                "terminal_request_cancel_child",
                json!({"mode": "request_cancel"}),
                ParentClosePolicy::RequestCancel,
            )
            .map_err(|e| e.to_string())?;
        let terminate_child = ctx
            .spawn_child_workflow_detached_raw(
                "terminal_terminate_child",
                json!({"mode": "terminate"}),
                ParentClosePolicy::Terminate,
            )
            .map_err(|e| e.to_string())?;
        let abandoned_child = ctx
            .spawn_child_workflow_detached_raw(
                "terminal_abandoned_child",
                json!({"mode": "abandon"}),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;

        Ok(json!({
            "request_cancel_child": request_cancel_child.to_string(),
            "terminate_child": terminate_child.to_string(),
            "abandoned_child": abandoned_child.to_string(),
        }))
    })
}

fn activity_and_detached_spawn_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity = ctx.execute_activity_raw("batched_work", Value::Null, "default");
        let detached = async {
            ctx.spawn_child_workflow_detached_raw(
                "batched_detached_child",
                json!({"mode": "audit"}),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())
        };
        let (activity, child_id) = tokio::join!(activity, detached);
        let activity = activity.map_err(|e| e.to_string())?;
        let child_id = child_id?;

        Ok(json!({
            "activity": activity,
            "child_id": child_id.to_string(),
        }))
    })
}

fn signal_and_detached_spawn_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000123")
                .map_err(|e| e.to_string())?,
        );
        let signal = ctx.signal_external_workflow(target, "refresh", json!({"ok": true}));
        let detached = async {
            ctx.spawn_child_workflow_detached_raw(
                "signal_detached_child",
                json!({"mode": "audit"}),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())
        };
        let (signal, child_id) = tokio::join!(signal, detached);
        signal.map_err(|e| e.to_string())?;
        let child_id = child_id?;

        Ok(json!({ "child_id": child_id.to_string() }))
    })
}

// ─────────────────────── (a) Happy-path test ─────────────────────────────────

#[tokio::test]
async fn test_happy_path_one_activity() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("send_email", |_| Ok(json!("delivered")))
        .run(one_activity_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"sent": "delivered"})));

    let events = outcome.events();
    assert!(
        events.iter().any(|e| matches!(e,
            WorkflowEvent::ActivityScheduled { name, .. } if name == "send_email"
        )),
        "expected ActivityScheduled for send_email"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "expected ActivityCompleted"
    );
}

#[tokio::test]
async fn test_side_effect_captured_before_suspension_is_persisted() {
    // Regression (issue #384): a deterministic primitive emitted before parking
    // on an activity must be persisted to history by the harness. If dropped,
    // the next iteration would replay system_now() against ActivityScheduled and
    // record spurious side-effect drift, failing the run.
    let outcome = WorkflowTestEnv::new()
        .mock_activity("send_email", |_| Ok(json!("delivered")))
        .run(side_effect_then_activity_workflow, json!(null))
        .await;

    assert!(
        outcome.result.is_ok(),
        "workflow must not fail with spurious side-effect drift: {:?}",
        outcome.result
    );
    assert_eq!(outcome.result.as_ref().unwrap()["sent"], json!("delivered"));

    let events = outcome.events();
    // The SideEffectRecorded event must be persisted, and ahead of the activity.
    let se_idx = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. }))
        .expect("SideEffectRecorded must be persisted in history");
    let act_idx = events
        .iter()
        .position(
            |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "send_email"),
        )
        .expect("ActivityScheduled must be present");
    assert!(
        se_idx < act_idx,
        "side effect must be recorded before the activity it precedes"
    );
    // Exactly one capture — not duplicated across replay iterations.
    let se_count = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. }))
        .count();
    assert_eq!(se_count, 1, "side effect must be recorded exactly once");
}

// ─────────────────────── (b) Retry test ──────────────────────────────────────

#[tokio::test]
async fn test_first_attempt_fails_second_succeeds() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_attempt("charge_card", 1, Err("transient gateway error".into()))
        .mock_activity_attempt("charge_card", 2, Ok(json!({"status": "charged"})))
        .run(retry_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"status": "charged"})));

    let events = outcome.events();
    let scheduled_count = events
        .iter()
        .filter(
            |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "charge_card"),
        )
        .count();
    assert_eq!(
        scheduled_count, 2,
        "expected two ActivityScheduled events (one failed, one succeeded)"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "expected an ActivityFailed event for the first attempt"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "expected an ActivityCompleted event for the second attempt"
    );
}

// ─────────────────────── (c) Timer vs signal race ────────────────────────────

#[tokio::test]
async fn test_timer_wins_when_no_signal_queued() {
    let outcome = WorkflowTestEnv::new().run(race_workflow, json!(null)).await;

    assert_eq!(outcome.result, Ok(json!("timer")));

    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "expected TimerStarted"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "expected TimerFired"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SignalReceived { .. })),
        "signal must not be in events when timer wins"
    );
}

#[tokio::test]
async fn test_signal_wins_when_queued() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("approve", json!("green-light"))
        .run(race_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"signal": "green-light"})));

    let events = outcome.events();
    assert!(
        events.iter().any(|e| matches!(e, WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == "approve")),
        "expected SignalReceived(approve)"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "timer must not fire when signal wins"
    );
}

// ──────────── Cancellable / renewable durable timers (issue #768) ────────────

#[tokio::test]
async fn test_cancellable_timer_fires_when_not_cancelled() {
    let outcome = WorkflowTestEnv::new()
        .run(cancellable_await_workflow, json!(null))
        .await;
    assert_eq!(outcome.result, Ok(json!("Fired")));

    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "expected TimerStarted"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "expected TimerFired"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerCancelled { .. })),
        "an uncancelled timer must not record TimerCancelled"
    );

    let report = outcome.replay_check(cancellable_await_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

/// (Codex P2 round 15, issue #768) Concurrent awaited cancellable timers fire in
/// DEADLINE order (min `fires_at` first), matching production's min-deadline
/// reschedule + deadline-ordered ingest — never in `join!` poll order.
#[tokio::test]
async fn test_concurrent_awaited_timers_fire_in_deadline_order() {
    let outcome = WorkflowTestEnv::new()
        .run(concurrent_await_two_timers_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

    let fired_order: Vec<String> = outcome
        .events()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::TimerFired { timer_id } => Some(timer_id.as_str().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        fired_order,
        vec!["fast".to_string(), "slow".to_string()],
        "concurrent awaited timers must fire in deadline order (fast dur=1 before \
         slow dur=10), never in join! poll order: {fired_order:?}"
    );

    // Issue #768, Codex P2 round 16: the two timers are armed concurrently (one
    // `await_fire` batch), so production starts both deadlines at the same instant
    // and the workflow-observed elapsed after both fire is the MAX deadline (10s),
    // NOT the sum (11s). The live `ctx.now()` read inside the workflow, and
    // `final_now()`/`elapsed()` reconstructed from history, must all agree at 10.
    let result = outcome.result.as_ref().expect("run ok");
    assert_eq!(
        result.get("now_elapsed_secs").and_then(Value::as_i64),
        Some(10),
        "ctx.now() after both concurrent timers fire must advance by MAX(10,1)=10, not 11"
    );
    assert_eq!(
        outcome.elapsed().num_seconds(),
        10,
        "final_now/elapsed must reconstruct the MAX deadline (10s), not the sum (11s)"
    );

    let report = outcome
        .replay_check(concurrent_await_two_timers_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

/// FIX 1 (Codex P2 round 5, issue #768): a duplicate active `start_timer` with a
/// DIFFERENT duration preserves the RECORDED duration. The handle carries 300
/// (not the second call's 600), exactly one `TimerStarted(idle, 300)` is
/// recorded, the timer fires at the 300s deadline (the virtual clock advances by
/// 300, not 600 — asserted via the armed duration the handle used), and the run
/// replays cleanly. Before the fix the handle carried 600.
#[tokio::test]
async fn test_duplicate_active_start_timer_preserves_recorded_duration() {
    let outcome = WorkflowTestEnv::new()
        .run(
            duplicate_active_start_timer_diff_duration_workflow,
            json!(null),
        )
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({ "armed": 300, "outcome": "Fired" })),
        "duplicate active start_timer must preserve the recorded 300s duration, \
         not the second call's 600s"
    );

    // Exactly ONE TimerStarted, recorded with duration 300 (the second
    // start_timer emits no event).
    let timer_starteds: Vec<u64> = outcome
        .events()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::TimerStarted { duration_secs, .. } => Some(*duration_secs),
            _ => None,
        })
        .collect();
    assert_eq!(
        timer_starteds,
        vec![300],
        "exactly one TimerStarted(idle, 300) must be recorded: {:?}",
        outcome.events()
    );

    let report = outcome
        .replay_check(duplicate_active_start_timer_diff_duration_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

#[tokio::test]
async fn test_cancellable_timer_does_not_fire_when_cancelled() {
    let outcome = WorkflowTestEnv::new()
        .run(cancellable_cancel_workflow, json!(null))
        .await;
    assert_eq!(outcome.result, Ok(json!("cancelled")));

    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerCancelled { .. })),
        "a cancelled timer must record TimerCancelled"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "a cancelled timer must never fire"
    );
}

#[tokio::test]
async fn test_reset_then_fire() {
    let outcome = WorkflowTestEnv::new()
        .run(cancellable_reset_once_workflow, json!(null))
        .await;
    assert_eq!(outcome.result, Ok(json!("Fired")));

    // History: TimerStarted, TimerCancelled, TimerStarted, TimerFired.
    let all_events = outcome.events();
    let timer_events: Vec<&WorkflowEvent> = all_events
        .iter()
        .filter(|e| {
            matches!(
                e,
                WorkflowEvent::TimerStarted { .. }
                    | WorkflowEvent::TimerCancelled { .. }
                    | WorkflowEvent::TimerFired { .. }
            )
        })
        .collect();
    assert_eq!(
        timer_events.len(),
        4,
        "expected 4 timer events: {timer_events:?}"
    );
    assert!(matches!(
        timer_events[0],
        WorkflowEvent::TimerStarted { .. }
    ));
    assert!(matches!(
        timer_events[1],
        WorkflowEvent::TimerCancelled { .. }
    ));
    assert!(matches!(
        timer_events[2],
        WorkflowEvent::TimerStarted { .. }
    ));
    assert!(matches!(timer_events[3], WorkflowEvent::TimerFired { .. }));

    let report = outcome.replay_check(cancellable_reset_once_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

/// (Codex P2 round 8, issue #768 — FIX A) A reset performed through
/// `ctx.reset_timer` (or another handle) must be honoured by a subsequent
/// `await_fire()` on the ORIGINAL handle: the live deadline and the virtual
/// clock must use the CURRENT armed duration (600) from the logical state map,
/// not the awaiting handle's stale cached 300. Before the fix the clock
/// advanced by 300 and disagreed with the recorded `TimerStarted(idle, 600)`.
#[tokio::test]
async fn test_await_fire_uses_current_reset_duration_not_stale_handle() {
    let env = WorkflowTestEnv::new();
    let start = env.now();
    let outcome = env
        .run(reset_via_ctx_then_await_handle_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

    // The final recorded TimerStarted(idle) is the reset's 600.
    let last_ts = outcome.events().iter().rev().find_map(|e| match e {
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs,
        } if timer_id.as_str() == "idle" => Some(*duration_secs),
        _ => None,
    });
    assert_eq!(
        last_ts,
        Some(600),
        "final TimerStarted(idle) must be the reset's 600: {:?}",
        outcome.events()
    );

    // The virtual clock (ctx.now()) advanced by the CURRENT armed 600, not the
    // stale handle's 300. This is the FIX A behaviour.
    let v = outcome.result.as_ref().unwrap();
    let now = v["now"].as_i64().unwrap();
    assert_eq!(
        now - start.timestamp(),
        600,
        "ctx.now() must advance by the current armed 600, not the stale handle 300"
    );

    // final_now()/elapsed() (the harness virtual clock) must AGREE with the
    // workflow-observed ctx.now() and count only the fired 600 — not 300+600.
    assert_eq!(
        outcome.final_now().timestamp(),
        now,
        "final_now() must match the workflow-observed ctx.now()"
    );
    assert_eq!(
        outcome.elapsed(),
        chrono::Duration::seconds(600),
        "elapsed() must be the fired 600, not the cancelled+fired sum (900)"
    );

    let report = outcome
        .replay_check(reset_via_ctx_then_await_handle_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

/// (Codex P2 round 8, issue #768 — FIX B) The harness virtual clock
/// (`final_now`/`elapsed`) must advance ONLY for timers that actually fired.
/// A reset records `[TimerStarted(300), TimerCancelled, TimerStarted(300),
/// TimerFired]`; only the second (fired) 300 counts — NOT the cancelled first
/// arm. Before the fix `final_now` summed every `TimerStarted` → 600.
#[tokio::test]
async fn test_final_now_counts_only_fired_arm_not_cancelled_reset_arm() {
    let env = WorkflowTestEnv::new();
    let start = env.now();
    let outcome = env.run(cancellable_reset_once_workflow, json!(null)).await;
    assert_eq!(outcome.result, Ok(json!("Fired")));
    assert_eq!(
        outcome.elapsed(),
        chrono::Duration::seconds(300),
        "virtual clock must advance only for the FIRED arm (300), not the \
         cancelled-arm + fired-arm sum (600)"
    );
    assert_eq!(outcome.final_now(), start + chrono::Duration::seconds(300));
}

/// (Codex P2 round 8, issue #768 — FIX B) A cancelled timer never fired, so the
/// harness virtual clock must not advance for its `TimerStarted` at all. Before
/// the fix `final_now` counted the cancelled arm's 300.
#[tokio::test]
async fn test_final_now_does_not_advance_for_a_cancelled_never_fired_timer() {
    let env = WorkflowTestEnv::new();
    let start = env.now();
    let outcome = env.run(cancellable_cancel_workflow, json!(null)).await;
    assert_eq!(outcome.result, Ok(json!("cancelled")));
    assert_eq!(
        outcome.elapsed(),
        chrono::Duration::zero(),
        "a cancelled (never-fired) timer must not advance the virtual clock"
    );
    assert_eq!(outcome.final_now(), start);
}

#[tokio::test]
async fn test_armed_timer_does_not_fire_during_a_mixed_activity_suspension() {
    // Codex P2 (issue #768): arming a cancellable timer in the same cycle as an
    // activity suspension must NOT interleave a TimerFired before the activity
    // completes. The real worker records the arm and parks on the activity;
    // firing the timer here would synthesise impossible history such as
    // `[TimerStarted, ActivityScheduled, TimerFired, ActivityCompleted]`.
    let outcome = WorkflowTestEnv::new()
        .mock_activity("work", |_| Ok(json!("done")))
        .run(arm_timer_then_activity_workflow, json!(null))
        .await;
    assert_eq!(outcome.result, Ok(json!({"work": "done"})));

    let events = outcome.events();
    // The timer is armed...
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "expected the arm to record TimerStarted: {events:?}"
    );
    // ...but never fires (the workflow parked on the activity, then completed).
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "an armed-but-unawaited timer must not fire during a mixed activity \
         suspension (impossible history): {events:?}"
    );

    // No TimerFired ever precedes ActivityCompleted — assert the exact ordering
    // invariant Codex flagged.
    let ts = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }));
    let tf = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerFired { .. }));
    assert!(
        tf.is_none() || tf > ts,
        "TimerFired must never precede ActivityCompleted: {events:?}"
    );

    let report = outcome.replay_check(arm_timer_then_activity_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

#[tokio::test]
async fn start_timer_then_activity_then_await_fire_fires() {
    // Codex P2 (issue #768): an armed timer whose `TimerStarted` was recorded in
    // an EARLIER mixed (activity) suspension batch must still fire on a later
    // bookkeeping-only `await_fire` cycle — production reschedules the existing
    // durable row. Without the fix the harness returns a no-op for the re-arm and
    // the run stalls ("suspended with no resolvable commands").
    let outcome = WorkflowTestEnv::new()
        .mock_activity("work", |_| Ok(json!("done")))
        .run(arm_timer_activity_then_await_fire_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"work": "done", "timer": "Fired"})),
        "the armed timer must fire after the activity completes: {:?}",
        outcome.result
    );

    let events = outcome.events();
    // Exactly one arm, one fire, no cancel.
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
            .count(),
        1,
        "exactly one TimerStarted: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::TimerFired { .. }))
            .count(),
        1,
        "exactly one TimerFired: {events:?}"
    );

    // Production-shaped ordering: TimerStarted precedes ActivityCompleted, and
    // TimerFired comes AFTER ActivityCompleted (never interleaved before it).
    let ts = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
        .expect("TimerStarted present");
    let ac = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        .expect("ActivityCompleted present");
    let tf = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerFired { .. }))
        .expect("TimerFired present");
    assert!(
        ts < ac,
        "TimerStarted must precede ActivityCompleted: {events:?}"
    );
    assert!(
        tf > ac,
        "TimerFired must come AFTER ActivityCompleted, never interleaved before: {events:?}"
    );

    let report = outcome
        .replay_check(arm_timer_activity_then_await_fire_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

#[tokio::test]
async fn start_timer_then_activity_then_cancel_then_await_cancelled() {
    // Matrix #6 (issue #768): arm → activity → cancel → await must resolve
    // Cancelled and never fire, even though the arm was recorded in an earlier
    // mixed-suspension batch.
    let outcome = WorkflowTestEnv::new()
        .mock_activity("work", |_| Ok(json!("done")))
        .run(arm_timer_activity_cancel_then_await_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"work": "done", "timer": "Cancelled"})),
        "the armed timer must resolve Cancelled: {:?}",
        outcome.result
    );

    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerCancelled { .. })),
        "a cancelled timer records TimerCancelled: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "a cancelled timer must never fire: {events:?}"
    );

    let report = outcome
        .replay_check(arm_timer_activity_cancel_then_await_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

#[tokio::test]
async fn start_timer_activity_reset_then_await_fires_without_spurious_fire() {
    // FIX A primary flow (Codex P2 round 4, issue #768): arm → activity →
    // reset → await. The armed timer must NOT fire while parked on the
    // activity — under the round-4 model its durable row is inserted only at
    // `await_fire`, so a `reset()` after the activity is never confronted with an
    // already-fired timer (which would leave an unconsumed TimerFired → non-
    // determinism), and the run replays deterministically to a final `Fired`.
    let outcome = WorkflowTestEnv::new()
        .mock_activity("work", |_| Ok(json!("done")))
        .run(arm_timer_activity_reset_then_await_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"work": "done", "timer": "Fired"})),
        "the reset timer must fire after the activity completes: {:?}",
        outcome.result
    );

    let events = outcome.events();
    // No TimerFired may precede ActivityCompleted (the arm-during-activity window
    // is exactly what round 4 makes non-fire-eligible).
    let ac = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        .expect("ActivityCompleted present");
    let first_tf = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerFired { .. }));
    assert!(
        first_tf.is_none_or(|tf| tf > ac),
        "no TimerFired may precede ActivityCompleted: {events:?}"
    );
    // Exactly one fire (the awaited one), and the reset recorded a cancel + a
    // fresh arm.
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::TimerFired { .. }))
            .count(),
        1,
        "exactly one TimerFired: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerCancelled { .. })),
        "the reset records a TimerCancelled: {events:?}"
    );

    let report = outcome
        .replay_check(arm_timer_activity_reset_then_await_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

#[tokio::test]
async fn cancel_then_classic_timer_same_id_arms_and_fires() {
    // FIX B (Codex P2 round 4, issue #768): a same-task `start_timer("idle");
    // cancel(); ctx.timer("idle", n)` must arm the CLASSIC timer cleanly and
    // fire — the same-batch cancel must not leave the classic timer wedged (no
    // hang), and the run must replay deterministically.
    let outcome = WorkflowTestEnv::new()
        .run(cancel_then_classic_timer_same_id_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!("classic_fired")),
        "the classic timer must fire after a same-id cancel: {:?}",
        outcome.result
    );

    let events = outcome.events();
    // A cancel is recorded, and the classic timer both starts and fires.
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerCancelled { .. })),
        "the cancel records TimerCancelled: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "the classic timer must fire: {events:?}"
    );

    let report = outcome
        .replay_check(cancel_then_classic_timer_same_id_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

#[tokio::test]
async fn cancel_timer_on_a_classic_id_does_not_kill_the_classic_timer() {
    // Issue #768, Codex P2 round 16 — Finding 2. A `cancel_timer("deadline")`
    // targeting the classic timer's id must be a pure no-op: it must NOT record a
    // `TimerCancelled` (whose worker-side `delete_pending_timer` would delete the
    // classic timer's durable row), so the classic timer still fires — no hang —
    // and the run replays deterministically.
    let outcome = WorkflowTestEnv::new()
        .run(
            cancel_timer_does_not_kill_a_classic_timer_workflow,
            json!(null),
        )
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!("classic_fired")),
        "the classic timer must fire despite a same-id cancel_timer: {:?}",
        outcome.result
    );

    let events = outcome.events();
    // The cancel is a no-op: NO TimerCancelled corrupts the classic timer.
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerCancelled { .. })),
        "cancel_timer on a classic id must record no TimerCancelled: {events:?}"
    );
    // The classic timer both starts and fires.
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "the classic timer must fire: {events:?}"
    );

    let report = outcome
        .replay_check(cancel_timer_does_not_kill_a_classic_timer_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

// ──────────────────── Child-workflow stubbing ─────────────────────────────────

#[tokio::test]
async fn test_child_workflow_stub() {
    let outcome = WorkflowTestEnv::new()
        .mock_child_workflow("child_processing", |input| {
            let id = input["id"].as_i64().unwrap_or(0);
            Ok(json!({"processed": id * 2}))
        })
        .run(parent_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"child": {"processed": 84}})));

    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. })),
        "expected ChildWorkflowStarted"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. })),
        "expected ChildWorkflowCompleted"
    );
}

#[tokio::test]
async fn test_terminal_detached_spawn_records_history_event() {
    let outcome = WorkflowTestEnv::new()
        .run(terminal_detached_spawn_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok());
    let events = outcome.events();
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                WorkflowEvent::ChildWorkflowSpawnedDetached {
                    workflow_name,
                    input,
                    parent_close_policy,
                    ..
                } if workflow_name == "terminal_detached_child"
                    && input == &json!({"mode": "audit"})
                    && *parent_close_policy == ParentClosePolicy::Abandon
            )
        }),
        "terminal detached spawns must be recorded before WorkflowCompleted: {events:?}"
    );
    let spawn_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::ChildWorkflowSpawnedDetached { .. }))
        .expect("spawn event should exist");
    let completed_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::WorkflowCompleted { .. }))
        .expect("workflow completed event should exist");
    assert!(
        spawn_pos < completed_pos,
        "detached spawn should precede terminal event: {events:?}"
    );
}

#[tokio::test]
async fn test_terminal_detached_spawn_records_parent_close_cascades() {
    let outcome = WorkflowTestEnv::new()
        .run(terminal_policy_detached_spawns_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok());
    let events = outcome.events();
    let completed_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::WorkflowCompleted { .. }))
        .expect("workflow completed event should exist");
    let request_cancel_child = events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name,
                ..
            } if workflow_name == "terminal_request_cancel_child" => Some(*child_id),
            _ => None,
        })
        .expect("request-cancel detached spawn should exist");
    let terminate_child = events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name,
                ..
            } if workflow_name == "terminal_terminate_child" => Some(*child_id),
            _ => None,
        })
        .expect("terminate detached spawn should exist");
    let abandoned_child = events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name,
                ..
            } if workflow_name == "terminal_abandoned_child" => Some(*child_id),
            _ => None,
        })
        .expect("abandoned detached spawn should exist");

    let cascades = events
        .iter()
        .enumerate()
        .filter_map(|(pos, event)| match event {
            WorkflowEvent::ChildWorkflowCascadeApplied {
                child_id,
                policy,
                action,
            } => Some((pos, *child_id, *policy, action.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        cascades.len(),
        2,
        "only non-Abandon detached children should cascade: {events:?}"
    );
    assert!(cascades.iter().all(|(pos, ..)| *pos > completed_pos));
    assert!(cascades.iter().any(|(_, child_id, policy, action)| {
        *child_id == request_cancel_child
            && *policy == ParentClosePolicy::RequestCancel
            && *action == "request_cancel"
    }));
    assert!(cascades.iter().any(|(_, child_id, policy, action)| {
        *child_id == terminate_child
            && *policy == ParentClosePolicy::Terminate
            && *action == "terminate"
    }));
    assert!(
        !cascades
            .iter()
            .any(|(_, child_id, _, _)| *child_id == abandoned_child),
        "Abandon detached child should not receive a cascade event: {events:?}"
    );
}

#[tokio::test]
async fn test_detached_spawn_is_recorded_before_activity_terminal_in_batch() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("batched_work", |_| Ok(json!({"ok": true})))
        .run(activity_and_detached_spawn_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok());
    let events = outcome.events();
    let scheduled_pos = events
        .iter()
        .position(|event| {
            matches!(
                event,
                WorkflowEvent::ActivityScheduled { name, .. } if name == "batched_work"
            )
        })
        .expect("activity should be scheduled");
    let detached_pos = events
        .iter()
        .position(|event| {
            matches!(
                event,
                WorkflowEvent::ChildWorkflowSpawnedDetached { workflow_name, .. }
                    if workflow_name == "batched_detached_child"
            )
        })
        .expect("detached child spawn should be recorded");
    let completed_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::ActivityCompleted { .. }))
        .expect("activity should complete");

    assert!(
        scheduled_pos < detached_pos && detached_pos < completed_pos,
        "detached spawn should be recorded between activity schedule and terminal: {events:?}"
    );
}

#[tokio::test]
async fn test_signal_terminal_is_recorded_before_detached_spawn_in_batch() {
    let outcome = WorkflowTestEnv::new()
        .run(signal_and_detached_spawn_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok());
    let events = outcome.events();
    let requested_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::ExternalSignalRequested { .. }))
        .expect("external signal request should be recorded");
    let delivered_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::ExternalSignalDelivered { .. }))
        .expect("external signal delivery should be recorded");
    let detached_pos = events
        .iter()
        .position(|event| {
            matches!(
                event,
                WorkflowEvent::ChildWorkflowSpawnedDetached { workflow_name, .. }
                    if workflow_name == "signal_detached_child"
            )
        })
        .expect("detached child spawn should be recorded");

    assert!(
        requested_pos < delivered_pos && delivered_pos < detached_pos,
        "test history must mirror production mixed-signal order: {events:?}"
    );
}

// ──────────────────── Event-log inspection ───────────────────────────────────

#[tokio::test]
async fn test_event_log_ordering() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("step_a", |_| Ok(json!("a")))
        .mock_activity("step_b", |_| Ok(json!("b")))
        .run(two_step_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!(["a", "b"])));

    let activity_names: Vec<&str> = outcome
        .events()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityScheduled { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        activity_names,
        ["step_a", "step_b"],
        "activities must appear in order"
    );
}

// ──────────────────── Local-activity mocking ─────────────────────────────────

#[tokio::test]
async fn test_local_activity_mock() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("compute_hash", |input| {
            let sum: i64 = input
                .as_array()
                .map_or(&[][..], Vec::as_slice)
                .iter()
                .filter_map(|v: &Value| v.as_i64())
                .sum();
            Ok(json!(sum))
        })
        .run(local_activity_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"hash": 6})));

    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityScheduled { .. })),
        "expected LocalActivityScheduled"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityCompleted { .. })),
        "expected LocalActivityCompleted"
    );
}

// ──────────────────── Missing stub → loud failure ────────────────────────────

#[tokio::test]
async fn test_missing_stub_fails_loudly() {
    let outcome = WorkflowTestEnv::new()
        // Intentionally omit the mock for "send_email"
        .run(one_activity_workflow, json!(null))
        .await;

    // The workflow should fail because no mock is registered.
    assert!(
        outcome.result.is_err(),
        "missing stub must cause an error result, not a panic"
    );
    let err = outcome.result.unwrap_err();
    assert!(
        err.contains("send_email"),
        "error should name the unregistered activity; got: {err}"
    );
}

// ──────────────────── Replay self-check ──────────────────────────────────────

#[tokio::test]
async fn test_replay_self_check_succeeds_for_deterministic_workflow() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("send_email", |_| Ok(json!("delivered")))
        .run(one_activity_workflow, json!(null))
        .await;

    // The test env records a real history; replaying through WorkflowReplayer
    // must succeed, proving the workflow is deterministic.
    let report = outcome.replay_check(one_activity_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

// ──────────────── set_current_details status breadcrumb (issue #593) ─────────

#[tokio::test]
async fn test_current_details_set_overwrite_clear_leaves_no_event_footprint() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("charge_card", |_| Ok(json!({"status": "charged"})))
        .mock_activity("send_email", |_| Ok(json!("delivered")))
        .run(current_details_status_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

    // set_current_details (initial set, overwrite, and a trailing empty-string
    // clear) must leave zero footprint in harvest_events -- only the two
    // activities' events are recorded, exactly as if the calls were never made.
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
        scheduled, 2,
        "expected exactly two ActivityScheduled events"
    );
    assert_eq!(
        completed, 2,
        "expected exactly two ActivityCompleted events"
    );

    let report = outcome.replay_check(current_details_status_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "set_current_details calls (set, overwrite, and empty-string clear) \
         must replay deterministically:\n{report}"
    );
}

// ──────────────────── Cancellation injection ─────────────────────────────────

#[tokio::test]
async fn test_cancellation_injection() {
    let outcome = WorkflowTestEnv::new()
        .with_cancellation("operator requested")
        .run(cancellable_workflow, json!(null))
        .await;

    // Workflow bails out at check_cancellation() before reaching the activity.
    assert!(
        outcome.result.is_err(),
        "cancelled workflow must return Err"
    );
    assert!(
        outcome.result.as_ref().unwrap_err().contains("cancelled"),
        "error should indicate cancellation"
    );
    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowCancelled { .. })),
        "expected WorkflowCancelled in the event log"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. })),
        "activity must not be scheduled after cancellation"
    );
}

// ──────────────────── env.now() ─────────────────────────────────────────────

#[tokio::test]
async fn test_simulated_time_is_stable() {
    let env = WorkflowTestEnv::new();
    let t1 = env.now();
    let t2 = env.now();
    assert_eq!(t1, t2, "simulated time must be deterministic");
}

// ──────────────────── with_state ────────────────────────────────────────────

struct CounterConfig {
    multiplier: i64,
}

fn stateful_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mult = ctx.state::<CounterConfig>().map_or(1, |c| c.multiplier);
        let v = input.as_i64().unwrap_or(0);
        Ok(json!(v * mult))
    })
}

#[tokio::test]
async fn test_shared_state_injection() {
    let outcome = WorkflowTestEnv::new()
        .with_state(CounterConfig { multiplier: 7 })
        .run(stateful_workflow, json!(3))
        .await;

    assert_eq!(outcome.result, Ok(json!(21)));
}

// ──────────────────── Parameterised retry-policy edge cases ─────────────────

/// A workflow that keeps retrying until success or exhaustion.
fn max_retries_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        const MAX: usize = 3;
        for attempt in 1..=MAX {
            match ctx
                .execute_activity_raw("flaky_op", json!({"attempt": attempt}), "q")
                .await
            {
                Ok(v) => return Ok(v),
                Err(_) if attempt < MAX => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        unreachable!()
    })
}

#[tokio::test]
async fn test_all_attempts_fail_returns_error() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("flaky_op", |_| Err("always broken".into()))
        .run(max_retries_workflow, json!(null))
        .await;

    assert!(
        outcome.result.is_err(),
        "exhausted retries should return Err"
    );
}

#[tokio::test]
async fn test_third_attempt_succeeds() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_attempt("flaky_op", 1, Err("fail1".into()))
        .mock_activity_attempt("flaky_op", 2, Err("fail2".into()))
        .mock_activity_attempt("flaky_op", 3, Ok(json!("finally")))
        .run(max_retries_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!("finally")));
    let scheduled = outcome
        .events()
        .iter()
        .filter(
            |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "flaky_op"),
        )
        .count();
    assert_eq!(scheduled, 3, "expected exactly 3 scheduled events");
}

// ─────────────── mock_activity_retries (worker-level retry sequences) ─────────

/// Workflow that calls one activity a single time, expecting the worker to
/// handle retries transparently.
fn single_call_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("resilient_op", json!(null), "default")
            .await
            .map_err(|e| e.to_string())
    })
}

#[tokio::test]
async fn test_mock_activity_retries_succeeds_on_third_worker_attempt() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries(
            "resilient_op",
            vec![
                Err("transient_1".into()),
                Err("transient_2".into()),
                Ok(json!({"status": "ok"})),
            ],
        )
        .run(single_call_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"status": "ok"})));
}

#[tokio::test]
async fn test_mock_activity_retries_all_fail_returns_error() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries(
            "resilient_op",
            vec![Err("err1".into()), Err("err2".into()), Err("err3".into())],
        )
        .run(single_call_workflow, json!(null))
        .await;

    assert!(
        outcome.result.is_err(),
        "all-fail retry sequence should propagate error to workflow"
    );
}

#[tokio::test]
async fn test_mock_activity_retries_history_has_one_scheduled_event() {
    // Worker-level retries are transparent: one execute_activity_raw call →
    // one ActivityScheduled, but multiple ActivityFailed + ActivityCompleted.
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries("resilient_op", vec![Err("fail".into()), Ok(json!("done"))])
        .run(single_call_workflow, json!(null))
        .await;

    let scheduled_count = outcome
        .events()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "resilient_op"))
        .count();
    assert_eq!(
        scheduled_count, 1,
        "worker-level retries must share one ActivityScheduled event"
    );
}

#[tokio::test]
async fn test_mock_activity_retries_history_has_no_intermediate_failures() {
    // Non-terminal retries do not produce ActivityFailed events in the real
    // worker (requeue_for_retry is called instead), so the test harness must
    // match: only the terminal outcome writes ActivityFailed.
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries(
            "resilient_op",
            vec![
                Err("transient_1".into()),
                Err("transient_2".into()),
                Ok(json!("done")),
            ],
        )
        .run(single_call_workflow, json!(null))
        .await;

    let events = outcome.events();

    // No ActivityFailed at all — the sequence ends in ActivityCompleted.
    let failed_count = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(
        failed_count, 0,
        "intermediate retries must not write ActivityFailed to history"
    );

    // Terminal ActivityCompleted event is present.
    let completed = events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }));
    assert!(completed, "expected a final ActivityCompleted event");

    // Three ActivityStarted events (one per attempt) are present.
    let started_count = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityStarted { .. }))
        .count();
    assert_eq!(started_count, 3, "expected one ActivityStarted per attempt");
}

#[tokio::test]
async fn test_mock_activity_retries_all_fail_terminal_failure_is_non_retryable() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries(
            "resilient_op",
            vec![
                Err("fail_1".into()),
                Err("fail_2".into()),
                Err("fail_3".into()),
            ],
        )
        .run(single_call_workflow, json!(null))
        .await;

    assert!(
        outcome.result.is_err(),
        "exhausted retry sequence must fail"
    );

    // Exactly one ActivityFailed event — the terminal failure — with
    // non_retryable: false, matching production behaviour for plain
    // Err(String) payloads (exhaustion is determined by the retry policy,
    // not the non_retryable flag on the event).
    let terminal_failure_count = outcome
        .events()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(
        terminal_failure_count, 1,
        "exactly one ActivityFailed event expected when all attempts fail"
    );
}

#[tokio::test]
async fn test_mock_activity_retries_single_ok_returns_success() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries("resilient_op", vec![Ok(json!("immediate"))])
        .run(single_call_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!("immediate")));

    // No intermediate failures when the first attempt succeeds.
    let failed_count = outcome
        .events()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(failed_count, 0);
}

// ─────────── receive_signal_timeout / wait_for_signal_timeout (issue #476) ───────────

/// Awaits an approval signal with a deadline; escalates to auto-reject on timeout.
fn approval_with_timeout_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let decision: Option<Value> = ctx
            .receive_signal_timeout("approval", std::time::Duration::from_secs(3600))
            .await
            .map_err(|e| e.to_string())?;
        Ok(decision.map_or_else(
            || json!({"outcome": "auto_rejected"}),
            |payload| json!({"outcome": "decided", "payload": payload}),
        ))
    })
}

#[tokio::test]
async fn test_receive_signal_timeout_signal_branch() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("approval", json!({"approved": true}))
        .run(approval_with_timeout_workflow, json!(null))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"outcome": "decided", "payload": {"approved": true}}))
    );
    assert!(
        outcome.events().iter().any(
            |e| matches!(e, WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == "approval")
        ),
        "expected SignalReceived(approval)"
    );

    let report = outcome.replay_check(approval_with_timeout_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "signal branch must replay deterministically:\n{report}"
    );
}

#[tokio::test]
async fn test_receive_signal_timeout_timeout_branch() {
    // No signal queued — the stubbed timer fires immediately (no real sleeping)
    // and the workflow takes the timeout/escalation branch.
    let outcome = WorkflowTestEnv::new()
        .run(approval_with_timeout_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"outcome": "auto_rejected"})));
    assert!(
        outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "expected TimerFired for the deadline timer"
    );
    assert!(
        !outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SignalReceived { .. })),
        "no signal must be recorded on the timeout branch"
    );

    let report = outcome.replay_check(approval_with_timeout_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "timeout branch must replay deterministically:\n{report}"
    );
}

// ─────────── execute_child_workflow_timeout — child-or-deadline (issue #779) ───────────

/// Awaits a child workflow with a deadline; on the deadline, escalates to a
/// default outcome without ever consuming the (never-produced) child result.
fn child_with_timeout_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let outcome: Option<Value> = ctx
            .spawn_child_workflow_timeout(
                "child_processing",
                json!({"id": 42}),
                std::time::Duration::from_secs(3600),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(outcome.map_or_else(
            || json!({"outcome": "timed_out"}),
            |output| json!({"outcome": "child_done", "output": output}),
        ))
    })
}

#[tokio::test]
async fn test_child_timeout_child_branch() {
    // A registered child mock resolves before the (stubbed) deadline — the
    // success branch, no real sleeping.
    let outcome = WorkflowTestEnv::new()
        .mock_child_workflow("child_processing", |input| {
            let id = input["id"].as_i64().unwrap_or(0);
            Ok(json!({"processed": id}))
        })
        .run(child_with_timeout_workflow, json!(null))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"outcome": "child_done", "output": {"processed": 42}}))
    );
    assert!(
        outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. })),
        "expected ChildWorkflowStarted"
    );
    assert!(
        outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. })),
        "expected ChildWorkflowCompleted on the child-win branch"
    );

    let report = outcome.replay_check(child_with_timeout_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "child branch must replay deterministically:\n{report}"
    );
}

#[tokio::test]
async fn test_child_timeout_deadline_branch() {
    // No child mock queued — the stubbed deadline timer fires immediately (no
    // real sleeping) and the workflow takes the timeout/escalation branch.
    // Parity with `test_receive_signal_timeout_timeout_branch`.
    let outcome = WorkflowTestEnv::new()
        .run(child_with_timeout_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"outcome": "timed_out"})));
    assert!(
        outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "expected TimerFired for the deadline timer"
    );
    assert!(
        !outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. })),
        "no child completion must be recorded on the deadline branch"
    );

    let report = outcome.replay_check(child_with_timeout_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "deadline branch must replay deterministically:\n{report}"
    );
}

// ── issue #488: last_completion_result / last_error carryover (unit) ─────────

fn carryover_reader_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let cursor: Option<Value> = ctx
            .last_completion_result::<Value>()
            .map_err(|e| e.to_string())?;
        let err: Option<String> = ctx.last_error();
        Ok(json!({"cursor": cursor, "last_error": err}))
    })
}

/// No carryover seeded → both accessors return None.
#[tokio::test]
async fn test_last_completion_result_none_on_first_run() {
    let outcome = WorkflowTestEnv::new()
        .run(carryover_reader_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"cursor": null, "last_error": null}))
    );
}

/// Seeded carryover is returned by `last_completion_result` and `last_error` returns None.
#[tokio::test]
async fn test_last_completion_result_seeded_value() {
    let cursor = json!({"processed_at": "2026-06-14T00:00:00Z", "cursor": 42});
    let outcome = WorkflowTestEnv::new()
        .with_last_completion_result(cursor.clone())
        .run(carryover_reader_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"cursor": cursor, "last_error": null}))
    );
}

/// Seeded `last_error` is returned; `last_completion_result` still shows the prior value.
#[tokio::test]
async fn test_last_error_seeded_value() {
    let prior_result = json!({"cursor": 10});
    let outcome = WorkflowTestEnv::new()
        .with_last_completion_result(prior_result.clone())
        .with_last_error("upstream timed out")
        .run(carryover_reader_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({
            "cursor": prior_result,
            "last_error": "upstream timed out"
        }))
    );
}

/// Replay determinism: the seeded carryover must replay identically.
#[tokio::test]
async fn test_last_completion_result_replays_deterministically() {
    let cursor = json!({"cursor": 99});
    let outcome = WorkflowTestEnv::new()
        .with_last_completion_result(cursor)
        .run(carryover_reader_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

    let report = outcome.replay_check(carryover_reader_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "carryover must replay deterministically:\n{report}"
    );
}

// ── issue #508: scheduled_time accessor (unit) ────────────────────────────────

fn scheduled_time_reader_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let slot: Option<String> = ctx.scheduled_time().map(|t| t.to_rfc3339());
        Ok(json!({ "scheduled_time": slot }))
    })
}

/// No `scheduled_time` seeded → accessor returns None.
#[tokio::test]
async fn test_scheduled_time_none_by_default() {
    let outcome = WorkflowTestEnv::new()
        .run(scheduled_time_reader_workflow, json!(null))
        .await;
    assert_eq!(outcome.result, Ok(json!({ "scheduled_time": null })));
}

/// Seeded `scheduled_time` is returned by the accessor.
#[tokio::test]
async fn test_scheduled_time_seeded_value() {
    use chrono::{TimeZone as _, Utc};
    let slot = Utc.with_ymd_and_hms(2026, 3, 15, 0, 0, 0).unwrap();
    let outcome = WorkflowTestEnv::new()
        .with_scheduled_time(slot)
        .run(scheduled_time_reader_workflow, json!(null))
        .await;
    match outcome.result {
        Ok(v) => {
            let s = v["scheduled_time"].as_str().expect("expected a string");
            assert!(s.contains("2026-03-15"), "slot round-trip, got: {s}");
        }
        Err(e) => panic!("run failed: {e}"),
    }
}

/// Replay determinism: the seeded `scheduled_time` replays identically.
#[tokio::test]
async fn test_scheduled_time_replays_deterministically() {
    use chrono::{TimeZone as _, Utc};
    let slot = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let outcome = WorkflowTestEnv::new()
        .with_scheduled_time(slot)
        .run(scheduled_time_reader_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

    let report = outcome.replay_check(scheduled_time_reader_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "scheduled_time must replay deterministically:\n{report}"
    );
}

// ── issue #526: virtual clock advancement when durable timers fire ─────────────

fn clock_after_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t0 = ctx.now().timestamp();
        ctx.timer("sleep30d", 30 * 24 * 3600)
            .await
            .map_err(|e| e.to_string())?;
        let t1 = ctx.now().timestamp();
        Ok(json!({ "t0": t0, "t1": t1, "diff_secs": t1 - t0 }))
    })
}

#[tokio::test]
async fn test_timer_advances_virtual_clock() {
    let outcome = WorkflowTestEnv::new()
        .run(clock_after_timer_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.unwrap();
    let diff_secs = v["diff_secs"].as_i64().expect("diff_secs must be integer");
    let expected = 30_i64 * 24 * 3600;
    assert_eq!(
        diff_secs, expected,
        "ctx.now() must advance by 30 days after timer fires"
    );
}

// ── issue #749: sleep_until absolute-deadline durable timer ────────────────────

/// A fixed instant firmly in the future (2035-01-01T00:00:00Z), shared between
/// the future-deadline workflow and its test so the test can reconstruct the
/// exact `remaining_secs_until(deadline, frozen_now)` math.
const SLEEP_UNTIL_FUTURE_DEADLINE_TS: i64 = 2_051_222_400;

/// Sleeps until a fixed future instant; reports the virtual-clock delta so the
/// test can check internal consistency against the recorded timer duration.
fn sleep_until_future_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t0 = ctx.now().timestamp();
        let deadline = chrono::DateTime::from_timestamp(SLEEP_UNTIL_FUTURE_DEADLINE_TS, 0).unwrap();
        ctx.sleep_until("wake", deadline)
            .await
            .map_err(|e| e.to_string())?;
        let t1 = ctx.now().timestamp();
        Ok(json!({ "delta": t1 - t0 }))
    })
}

/// Sleeps until an instant firmly in the past — must fire immediately.
fn sleep_until_past_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // 2000-01-01T00:00:00Z — firmly in the past.
        let deadline = chrono::DateTime::from_timestamp(946_684_800, 0).unwrap();
        ctx.sleep_until("wake", deadline)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!("fired"))
    })
}

#[tokio::test]
async fn sleep_until_resolves_in_test_env_without_real_sleep() {
    let outcome = WorkflowTestEnv::new()
        .run(sleep_until_future_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

    // Read both the frozen `system_now()` capture (`SideEffectRecorded { Now }`,
    // epoch-millis) and the recorded timer duration straight from history, then
    // reconstruct the exact remaining-seconds math `sleep_until` performed. This
    // is a genuine `sleep_until` assertion, not the test-env invariant that the
    // virtual clock advances by whatever timer was started. `remaining_secs_until`
    // is `pub(crate)` and unreachable from this external test crate, so the
    // expected value is computed inline with the same round-up rule.
    let frozen_millis = outcome
        .events()
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::SideEffectRecorded {
                kind: autumn_harvest::SideEffectKind::Now,
                value,
                ..
            } => value.as_i64(),
            _ => None,
        })
        .expect("a SideEffectRecorded(Now) event must freeze the wall clock");
    let timer_secs = outcome
        .events()
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::TimerStarted { duration_secs, .. } => Some(*duration_secs),
            _ => None,
        })
        .expect("a TimerStarted event must be recorded");

    let frozen_now = chrono::DateTime::from_timestamp_millis(frozen_millis)
        .expect("frozen millis must be a valid instant");
    let deadline = chrono::DateTime::from_timestamp(SLEEP_UNTIL_FUTURE_DEADLINE_TS, 0).unwrap();
    let delta = deadline.signed_duration_since(frozen_now);
    let expected =
        u64::try_from(delta.num_seconds() + i64::from(delta.subsec_nanos() > 0)).unwrap();
    assert_eq!(
        timer_secs, expected,
        "recorded timer duration must equal remaining_secs_until(deadline, frozen_now)"
    );
    assert!(timer_secs > 0, "a future deadline must be a positive wait");

    // ...and it resolves with no real sleep: the virtual clock advanced by
    // exactly the recorded timer duration.
    let reported_delta = outcome.result.unwrap()["delta"].as_u64().unwrap();
    assert_eq!(
        reported_delta, timer_secs,
        "virtual clock must advance by exactly the recorded timer duration"
    );
}

#[tokio::test]
async fn sleep_until_past_deadline_fires_immediately() {
    let outcome = WorkflowTestEnv::new()
        .run(sleep_until_past_workflow, json!(null))
        .await;
    assert_eq!(outcome.result, Ok(json!("fired")));

    let timer_secs = outcome
        .events()
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::TimerStarted { duration_secs, .. } => Some(*duration_secs),
            _ => None,
        })
        .expect("a TimerStarted event must be recorded even for a past deadline");
    assert_eq!(
        timer_secs, 0,
        "a past deadline must record a zero-duration timer (AC3)"
    );
}

fn two_timers_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t0 = ctx.now().timestamp();
        ctx.timer("hour1", 3600).await.map_err(|e| e.to_string())?;
        let t1 = ctx.now().timestamp();
        ctx.timer("hour2", 7200).await.map_err(|e| e.to_string())?;
        let t2 = ctx.now().timestamp();
        Ok(json!({ "t0": t0, "t1": t1, "t2": t2 }))
    })
}

#[tokio::test]
async fn test_sequential_timers_accumulate() {
    let outcome = WorkflowTestEnv::new()
        .run(two_timers_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.unwrap();
    let t0 = v["t0"].as_i64().unwrap();
    let t1 = v["t1"].as_i64().unwrap();
    let t2 = v["t2"].as_i64().unwrap();
    assert_eq!(t1 - t0, 3600, "first timer must advance clock by 1h");
    assert_eq!(
        t2 - t0,
        3600 + 7200,
        "second timer must accumulate to 3h total"
    );
}

fn activity_then_now_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t0 = ctx.now().timestamp();
        ctx.execute_activity_raw("noop", json!(null), "default")
            .await
            .map_err(|e| e.to_string())?;
        let t1 = ctx.now().timestamp();
        Ok(json!({ "diff_secs": t1 - t0 }))
    })
}

#[tokio::test]
async fn test_activities_do_not_advance_clock() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("noop", |_| Ok(json!(null)))
        .run(activity_then_now_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.unwrap();
    let diff_secs = v["diff_secs"].as_i64().unwrap();
    assert_eq!(
        diff_secs, 0,
        "activities must not advance the virtual clock"
    );
}

fn signal_or_timer_now_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t0 = ctx.now().timestamp();
        let won = match ctx
            .wait_for_signal_timeout("approve", std::time::Duration::from_secs(30 * 24 * 3600))
            .await
        {
            Ok(Some(_)) => "signal",
            Ok(None) => "timer",
            Err(e) => return Err(e.to_string()),
        };
        let t1 = ctx.now().timestamp();
        Ok(json!({ "won": won, "diff_secs": t1 - t0 }))
    })
}

#[tokio::test]
async fn test_signal_preempts_timer_no_advance() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("approve", json!("go"))
        .run(signal_or_timer_now_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.unwrap();
    assert_eq!(v["won"], json!("signal"), "signal must have won");
    let diff_secs = v["diff_secs"].as_i64().unwrap();
    assert_eq!(
        diff_secs, 0,
        "signal-preempted timer must not advance the clock"
    );
}

fn billing_loop_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t_start = ctx.now().timestamp();
        ctx.timer("cycle1", 30 * 24 * 3600)
            .await
            .map_err(|e| e.to_string())?;
        let t_after_first = ctx.now().timestamp();
        ctx.timer("cycle2", 30 * 24 * 3600)
            .await
            .map_err(|e| e.to_string())?;
        let t_after_second = ctx.now().timestamp();
        Ok(json!({
            "t_start": t_start,
            "t_after_first": t_after_first,
            "t_after_second": t_after_second,
        }))
    })
}

#[tokio::test]
async fn test_billing_loop_dates_and_elapsed() {
    let env = WorkflowTestEnv::new();
    let start = env.now();
    let outcome = env.run(billing_loop_workflow, json!(null)).await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.as_ref().unwrap();

    let t_start = v["t_start"].as_i64().unwrap();
    let t_after_first = v["t_after_first"].as_i64().unwrap();
    let t_after_second = v["t_after_second"].as_i64().unwrap();

    assert_eq!(
        t_after_first - t_start,
        30 * 24 * 3600_i64,
        "first 30d advance"
    );
    assert_eq!(
        t_after_second - t_start,
        60 * 24 * 3600_i64,
        "second 30d accumulates to 60d"
    );

    let sixty_days = chrono::Duration::days(60);
    assert_eq!(
        outcome.final_now(),
        start + sixty_days,
        "outcome.final_now() must be start + 60 days"
    );
    assert_eq!(
        outcome.elapsed(),
        sixty_days,
        "outcome.elapsed() must be 60 days"
    );
    assert_eq!(
        outcome.final_now().timestamp(),
        t_after_second,
        "outcome.final_now() must match workflow-observed ctx.now()"
    );
}

fn year_of_timers_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        for i in 0..12_u32 {
            ctx.timer(&format!("month{i}"), 30 * 24 * 3600)
                .await
                .map_err(|e| e.to_string())?;
        }
        ctx.timer("extra5d", 5 * 24 * 3600)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(null))
    })
}

#[tokio::test]
async fn test_365_days_under_50ms() {
    let env = WorkflowTestEnv::new();
    let start = env.now();
    let wall_start = std::time::Instant::now();
    let outcome = env.run(year_of_timers_workflow, json!(null)).await;
    let elapsed_wall = wall_start.elapsed();
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    // 50ms target in release builds; 5000ms budget for unoptimized debug builds.
    let wall_budget_ms = if cfg!(debug_assertions) { 5_000 } else { 50 };
    assert!(
        elapsed_wall.as_millis() < wall_budget_ms,
        "365 days of virtual time must complete in < {wall_budget_ms}ms wall-clock, got {}ms",
        elapsed_wall.as_millis()
    );
    assert_eq!(
        outcome.final_now(),
        start + chrono::Duration::days(365),
        "outcome.final_now() must be start + 365 days"
    );
}

// ──────────────────────── ctx.race() tests (issue #600) ────────────────────────

/// Hedge two providers, cancel the loser, take the winner — the AC4 ≤5-line
/// DX example, exercised end-to-end through the no-DB test harness.
fn race_two_providers_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("fetch_primary", input.clone(), "default")
            .activity_raw("fetch_fallback", input, "default")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"winner_index": winner.index, "value": winner.value}))
    })
}

#[tokio::test]
async fn test_race_two_activities_discards_loser_and_replays_deterministically() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("fetch_primary", |_input| Ok(json!({"provider": "primary"})))
        .mock_activity("fetch_fallback", |_input| {
            Ok(json!({"provider": "fallback"}))
        })
        .run(race_two_providers_workflow, json!({"query": "widgets"}))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"winner_index": 0, "value": {"provider": "primary"}}))
    );

    // The winner is durably recorded via the existing MarkerRecorded event
    // (issue #600 AC2 — no new WorkflowEvent variant).
    assert!(
        outcome.events().iter().any(
            |e| matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name == "race_winner:1")
        ),
        "expected a race_winner marker in history: {:?}",
        outcome.events()
    );
    // WorkflowTestEnv resolves every mocked ScheduleActivity synchronously
    // within the same iteration, so by the time the race is evaluated *both*
    // branches already have a real terminal in history — there is nothing
    // durably "open" left to cancel, and no synthetic "lost race" terminal is
    // (or should be) recorded. Both activities' real completions are visible
    // in history; only the non-winning one's value is discarded. The
    // still-open-loser-gets-cancelled path (a genuinely durable resource) is
    // covered by the `race_activity_replays_winner_and_records_marker_plus_cancel_losers`
    // and `race_activity_winner_is_stable_across_randomized_completion_positions`
    // unit tests in `context.rs`, which construct that history state directly.
    assert!(
        !outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "no branch should be force-failed when both already completed: {:?}",
        outcome.events()
    );
    let fallback_completed = outcome.events().iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityCompleted { output, .. } if output == &json!({"provider": "fallback"})
    ));
    assert!(
        fallback_completed,
        "the losing branch's own real completion is still recorded in history: {:?}",
        outcome.events()
    );

    let report = outcome.replay_check(race_two_providers_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "race must replay deterministically:\n{report}"
    );
}

/// Approval-signal-or-timeout expressed via `ctx.race()` (issue #600's other
/// headline example from the User Story) — a thin wrapper over the
/// already-shipped `receive_signal_timeout` (issue #476). `winner.index` for
/// this shape is a fixed role-based value (timer = 0, signal = 1),
/// independent of the `.signal()`/`.timer()` call order below.
fn race_approval_or_timeout_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .signal("approval")
            .timer(std::time::Duration::from_secs(3600))
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"winner_index": winner.index, "value": winner.value}))
    })
}

#[tokio::test]
async fn test_race_approval_or_timeout_signal_branch() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("approval", json!({"approved": true}))
        .run(race_approval_or_timeout_workflow, json!(null))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"winner_index": 1, "value": {"approved": true}}))
    );

    let report = outcome
        .replay_check(race_approval_or_timeout_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "signal-branch race must replay deterministically:\n{report}"
    );
}

#[tokio::test]
async fn test_race_approval_or_timeout_timer_branch() {
    // No signal queued — the stubbed timer fires immediately.
    let outcome = WorkflowTestEnv::new()
        .run(race_approval_or_timeout_workflow, json!(null))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"winner_index": 0, "value": null}))
    );

    let report = outcome
        .replay_check(race_approval_or_timeout_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "timer-branch race must replay deterministically:\n{report}"
    );
}

// ─────────────────────── Worker sessions (issue #606) ─────────────────────────

/// A 3-step pipeline over a worker session: open, run one member activity
/// through it, then close. Deliberately registers **no** mock for the
/// reserved `__harvest_session_acquire`/`__harvest_session_release`
/// activities -- the harness must auto-resolve them so a session-using
/// workflow needs no special test setup for the happy path.
fn session_pipeline_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let session = ctx
            .create_session(SessionOptions::new("gpu-workers"))
            .await
            .map_err(|e| e.to_string())?;
        let host = session.host_worker_id().to_string();

        let transcoded: Value = session
            .execute_activity_raw("transcode_chunk", json!({"chunk": 1}), "gpu-workers")
            .await
            .map_err(|e| e.to_string())?;

        session.complete().await.map_err(|e| e.to_string())?;

        Ok(json!({"host": host, "transcoded": transcoded}))
    })
}

#[tokio::test]
async fn test_worker_session_pipeline_auto_resolves_reserved_activities() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("transcode_chunk", |_| Ok(json!("transcoded")))
        .run(session_pipeline_workflow, json!(null))
        .await;

    assert!(
        outcome.result.is_ok(),
        "session pipeline should succeed with no mock registered for the reserved \
         acquire/release activities: {:?}",
        outcome.result
    );
    let output = outcome.result.clone().unwrap();
    assert_eq!(output["transcoded"], json!("transcoded"));
    // The auto-resolved host worker id must be a non-empty, stable string.
    assert!(output["host"].as_str().is_some_and(|h| !h.is_empty()));

    let events = outcome.events();
    assert!(
        events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityScheduled { name, .. }
                if name == "__harvest_session_acquire"
        )),
        "expected ActivityScheduled for the internal session-acquire activity"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityScheduled { name, .. }
                if name == "__harvest_session_release"
        )),
        "expected ActivityScheduled for the internal session-release activity"
    );

    let report = outcome.replay_check(session_pipeline_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "session pipeline must replay deterministically:\n{report}"
    );
}

#[tokio::test]
async fn test_worker_session_auto_resolved_host_is_stable_across_replay() {
    // Run once to get recorded history, then replay it directly and confirm
    // the *same* host worker id comes back both times (proving the harness's
    // auto-resolution is deterministic, not re-randomized per call).
    let outcome = WorkflowTestEnv::new()
        .mock_activity("transcode_chunk", |_| Ok(json!("transcoded")))
        .run(session_pipeline_workflow, json!(null))
        .await;
    let first_host = outcome.result.as_ref().unwrap()["host"].clone();

    let report = outcome.replay_check(session_pipeline_workflow).await;
    assert!(matches!(report.status, ReplayStatus::ReplaySucceeded));

    // Re-run against the exact same recorded history via a fresh env to
    // confirm the acquire activity's *recorded* output (not a fresh
    // auto-resolution) is what a real replay would use.
    let events = outcome.events().to_vec();
    let acquired_host = events.iter().find_map(|e| match e {
        WorkflowEvent::ActivityCompleted { output, .. } if output.as_str().is_some() => {
            Some(output.clone())
        }
        _ => None,
    });
    assert_eq!(Some(first_host), acquired_host);
}

// ─────────── saga metric labels through the harness (issue #801) ───────────

/// Saga workflow with an activity-backed forward step (forces a real
/// suspension/iteration boundary) and a manual compensated unwind.
fn compensating_saga_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mut saga = autumn_harvest::Saga::new(ctx);
        saga.step(
            || async {
                ctx.execute_activity_raw("reserve", Value::Null, "default")
                    .await
            },
            |_| async { Ok::<_, autumn_harvest::HarvestError>(()) },
        )
        .await
        .map_err(|e| e.to_string())?;
        saga.compensate_all().await.map_err(|e| e.to_string())?;
        Ok(json!("compensated"))
    })
}

/// Recorder capturing the saga counters with their labels.
#[derive(Default)]
struct SagaLabelRecorder {
    compensated: std::sync::Mutex<Vec<(String, String)>>,
}

impl autumn_harvest::telemetry::MetricsRecorder for SagaLabelRecorder {
    fn record_saga_compensated(&self, workflow_name: &str, queue: &str) {
        self.compensated
            .lock()
            .unwrap()
            .push((workflow_name.to_owned(), queue.to_owned()));
    }
}

/// Issue #801 post-review P3: `with_workflow_name`/`with_queue_name` thread
/// real metric labels into the harness contexts, and the counter still fires
/// exactly once across the multiple iterations the env drives.
#[tokio::test]
async fn test_env_threads_workflow_and_queue_labels_into_saga_metrics() {
    let recorder = std::sync::Arc::new(SagaLabelRecorder::default());
    let env = WorkflowTestEnv::new()
        .with_metrics(recorder.clone())
        .with_workflow_name("book_trip")
        .with_queue_name("payments")
        .mock_activity("reserve", |_| Ok(json!("rsv-1")));

    let outcome = env.run(compensating_saga_workflow, Value::Null).await;
    assert_eq!(outcome.result, Ok(json!("compensated")));

    assert_eq!(
        recorder.compensated.lock().unwrap().as_slice(),
        &[("book_trip".to_owned(), "payments".to_owned())],
        "the harness must thread real workflow/queue labels and the counter \
         must fire exactly once across the env's iterations"
    );
}

// ─────────────── Typed workflow failure (issue #767, Codex P2) ────────────────

/// A root workflow that fails with a typed `WorkflowFailure`. The macro encodes
/// `Err(WorkflowFailure)` into the `harvest_workflow_failure_v1` envelope on the
/// engine's `Result<Value, String>` boundary; this handler reproduces that
/// encoding via `into_workflow_error_payload` so the harness sees exactly what
/// the worker would.
fn typed_failing_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    use autumn_harvest::failure::{IntoWorkflowErrorString, WorkflowFailure};
    Box::pin(async move {
        Err(WorkflowFailure::new("BudgetExceeded", "over")
            .non_retryable()
            .with_details(json!({ "cap": 5 }))
            .into_workflow_error_payload())
    })
}

#[tokio::test]
async fn test_env_root_typed_failure_records_typed_fields_and_human_message() {
    let outcome = WorkflowTestEnv::new()
        .run(typed_failing_workflow, json!(null))
        .await;

    // The outcome error string is the decoded human message, not the raw envelope.
    assert_eq!(outcome.result, Err("over".to_owned()));

    let recorded = outcome
        .events()
        .iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowFailed {
                error,
                error_type,
                details,
                non_retryable,
            } => Some((
                error.clone(),
                error_type.clone(),
                details.clone(),
                *non_retryable,
            )),
            _ => None,
        })
        .expect("a WorkflowFailed event must be recorded");

    let (error, error_type, details, non_retryable) = recorded;
    assert_eq!(error, "over");
    assert_eq!(error_type.as_deref(), Some("BudgetExceeded"));
    assert_eq!(non_retryable, Some(true));
    assert_eq!(details, Some(json!({ "cap": 5 })));
}

// ─────────── Non-blocking signal drain (issue #775) ───────────

/// Aggregator pattern: block for the first "event" signal, then non-blockingly
/// drain every remaining buffered "event" signal in one task execution.
fn drain_aggregator_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Block until the batch of "event" signals is promoted into history.
        let first = ctx
            .wait_for_signal("event")
            .await
            .map_err(|e| e.to_string())?;
        let mut collected = vec![first];
        // Non-blocking: consume the rest already buffered.
        collected.extend(ctx.drain_signals_raw("event").map_err(|e| e.to_string())?);
        Ok(json!({ "collected": collected }))
    })
}

#[tokio::test]
async fn test_drain_signals_returns_all_queued_in_order() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("event", json!({"seq": 1}))
        .queue_signal("event", json!({"seq": 2}))
        .queue_signal("event", json!({"seq": 3}))
        .run(drain_aggregator_workflow, json!(null))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"collected": [{"seq": 1}, {"seq": 2}, {"seq": 3}]})),
        "all queued same-name signals must be returned by the wait+drain in queued order"
    );
}

/// Returns whether a non-blocking `try_receive_signal` saw anything.
fn try_receive_none_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let got: Option<Value> = ctx.try_receive_signal("event").map_err(|e| e.to_string())?;
        Ok(json!({ "got": got }))
    })
}

#[tokio::test]
async fn test_try_receive_signal_none_when_nothing_queued() {
    let outcome = WorkflowTestEnv::new()
        .run(try_receive_none_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"got": Value::Null})),
        "try_receive_signal must return None (and never suspend) when nothing is buffered"
    );
}

/// Aggregator that returns the COUNT of signals collected (block for the first,
/// then drain the rest) — for the N-at-scale success-metric assertion.
fn drain_n_env_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let first = ctx
            .wait_for_signal("event")
            .await
            .map_err(|e| e.to_string())?;
        let mut all = vec![first];
        all.extend(ctx.drain_signals_raw("event").map_err(|e| e.to_string())?);
        Ok(json!({ "count": all.len() }))
    })
}

#[tokio::test]
async fn test_drain_1000_signals_returns_all_in_one_task() {
    // Success-metric (issue #775, AC8): a wait + drain in ONE task execution
    // must RETURN all N buffered same-name signals — not merely consume them.
    // A `.take(k)` truncation regression would fail this returned-count check.
    let mut env = WorkflowTestEnv::new();
    for i in 0..1000 {
        env = env.queue_signal("event", json!({ "seq": i }));
    }
    let outcome = env.run(drain_n_env_workflow, json!(null)).await;
    assert_eq!(
        outcome.result,
        Ok(json!({ "count": 1000 })),
        "all 1000 buffered same-name signals must be returned in a single task"
    );
}

/// Does exactly ONE `wait_for_signal("event")` and returns it WITHOUT draining —
/// proves the test-env batch-promotion of the queued extras did not corrupt the
/// single-wait return path.
fn single_wait_no_drain_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let first = ctx
            .wait_for_signal("event")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "first": first }))
    })
}

#[tokio::test]
async fn test_single_wait_with_extras_queued_returns_first_signal() {
    // Regression guard for the `WaitForSignal` batch-promotion change: with
    // multiple same-name signals queued, a workflow doing a single
    // `wait_for_signal` (no drain) must still return the FIRST queued signal —
    // promoting the extras into history must not corrupt the single-wait return.
    let outcome = WorkflowTestEnv::new()
        .queue_signal("event", json!({"seq": 1}))
        .queue_signal("event", json!({"seq": 2}))
        .queue_signal("event", json!({"seq": 3}))
        .run(single_wait_no_drain_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({ "first": {"seq": 1} })),
        "a single wait_for_signal must return the first queued signal despite batch promotion"
    );
}

/// Drain-first (issue #775, Codex P2): the workflow's FIRST action is a
/// non-blocking `drain_signals_raw`, with NO prior blocking `wait_for_signal`.
/// Production ingests pending signals at task-prep (before the handler runs), so
/// a drain-first workflow must observe every pre-queued signal. Before the
/// task-prep ingest fix — when the harness only promoted signals at a
/// `WaitForSignal` wake — this returned an empty `collected`.
fn drain_first_no_wait_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let collected = ctx.drain_signals_raw("event").map_err(|e| e.to_string())?;
        Ok(json!({ "collected": collected }))
    })
}

#[tokio::test]
async fn test_drain_first_no_prior_wait_returns_all_queued() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("event", json!({"seq": 1}))
        .queue_signal("event", json!({"seq": 2}))
        .queue_signal("event", json!({"seq": 3}))
        .run(drain_first_no_wait_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"collected": [{"seq": 1}, {"seq": 2}, {"seq": 3}]})),
        "a drain with NO prior wait_for_signal must still observe every pre-queued signal \
         (task-prep ingest, mirroring production `ingest_due_timers_and_signals`)"
    );
}

/// try_receive-first (issue #775, Codex P2): FIRST action is a non-blocking
/// `try_receive_signal` with a signal pre-queued and NO prior blocking wait.
fn try_receive_first_no_wait_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let got: Option<Value> = ctx.try_receive_signal("event").map_err(|e| e.to_string())?;
        Ok(json!({ "got": got }))
    })
}

#[tokio::test]
async fn test_try_receive_first_no_prior_wait_returns_oldest() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("event", json!({"seq": 1}))
        .queue_signal("event", json!({"seq": 2}))
        .run(try_receive_first_no_wait_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"got": {"seq": 1}})),
        "a try_receive_signal with NO prior wait must return the oldest pre-queued signal"
    );
}

// ── Deadline-aware continue-as-new replay_check (issue #772) ───────────────────

/// A deadline-aware workflow: it consults `should_continue_as_new()` (which,
/// when an `execution_timeout` is configured, records a `SideEffectRecorded{Now}`
/// deadline probe under the reserved sentinel name — even when the deadline
/// fraction is nowhere near reached, since the clock is read before the fraction
/// is compared), then performs one durable step.
fn deadline_aware_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // With an execution_timeout configured, this call records the reserved
        // `__harvest_deadline_probe` `SideEffectRecorded{Now}` event. For a far
        // deadline the recommendation is false, so the run continues normally.
        if ctx.should_continue_as_new() {
            ctx.continue_as_new(json!({"checkpointed": true}))
                .await
                .map_err(|e| e.to_string())?;
            return Ok(json!({"checkpointed": true}));
        }
        // One durable step after the deadline probe, so an unconsumed probe on
        // replay surfaces as an unambiguous command mismatch.
        ctx.timer("cycle", 30).await.map_err(|e| e.to_string())?;
        Ok(json!({"done": true}))
    })
}

/// Issue #772 (review finding): `TestRunOutcome::replay_check` must carry the
/// `WorkflowTestEnv::with_execution_timeout` budget into the `WorkflowReplayer`
/// it builds. Otherwise the deadline branch is disabled during the self-check,
/// leaving the live run's recorded `SideEffectRecorded{Now}` deadline probe
/// unconsumed — a FALSE non-determinism report for an otherwise valid
/// deadline-aware history.
#[tokio::test]
async fn deadline_aware_run_replay_check_carries_execution_timeout() {
    use autumn_harvest::DEADLINE_PROBE_SIDE_EFFECT_NAME;
    use autumn_harvest::event::SideEffectKind;

    let outcome = WorkflowTestEnv::new()
        .with_execution_timeout(chrono::Duration::hours(24))
        .run(deadline_aware_workflow, json!(null))
        .await;

    // The live run completed normally (far deadline ⇒ no checkpoint).
    assert_eq!(
        outcome.result,
        Ok(json!({"done": true})),
        "run failed: {:?}",
        outcome.result
    );

    // Sanity: the deadline probe was recorded, so the history genuinely
    // exercises the deadline-aware code path this test guards.
    let recorded_probe = outcome.events().iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::SideEffectRecorded { kind: SideEffectKind::Now, name: Some(n), .. }
                if n == DEADLINE_PROBE_SIDE_EFFECT_NAME
        )
    });
    assert!(
        recorded_probe,
        "with_execution_timeout must make should_continue_as_new() record the deadline probe: {:?}",
        outcome.events()
    );

    // The self-check must report ReplaySucceeded. Before the fix, replay_check
    // built the replayer with execution_timeout None, disabling the deadline
    // branch, so the recorded probe was left unconsumed → false non-determinism.
    let report = outcome.replay_check(deadline_aware_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay_check must carry the test env's execution_timeout so the deadline-aware \
         history replays cleanly, got: {report}"
    );
}

/// A workflow that consumes 90s of its deadline budget via a durable timer,
/// then reports whether `should_continue_as_new()` trips.
fn deadline_trip_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Advancing the virtual clock 90s of a 100s budget consumes 0.9 of it,
        // past the default 0.8 deadline fraction.
        ctx.timer("gate", 90).await.map_err(|e| e.to_string())?;
        Ok(json!(ctx.should_continue_as_new()))
    })
}

/// Issue #772 (Codex P2): in a live `WorkflowTestEnv::with_execution_timeout`
/// run, the deadline probe must read the context's ADVANCING virtual clock —
/// not the host `Utc::now()` — so a workflow that consumes its deadline budget
/// via durable timers actually trips the deadline continue-as-new branch under
/// test. Before the fix the probe read `Utc::now()` (≈ `start_time`, ~zero
/// elapsed), so almost none of the budget looked consumed and the branch never
/// tripped in the harness — making the test-env timeout support unable to
/// validate the long-lived low-event workflows the feature was added for.
#[tokio::test]
async fn deadline_probe_reads_advancing_clock_and_trips_in_test_env() {
    let outcome = WorkflowTestEnv::new()
        .with_execution_timeout(chrono::Duration::seconds(100))
        .run(deadline_trip_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!(true)),
        "advancing the virtual clock past the deadline fraction via a durable timer must \
         trip should_continue_as_new() in the test env; result: {:?} events: {:?}",
        outcome.result,
        outcome.events(),
    );
}

/// A workflow that consumes only 10s of its 100s budget.
fn deadline_no_trip_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("gate", 10).await.map_err(|e| e.to_string())?;
        Ok(json!(ctx.should_continue_as_new()))
    })
}

/// Control (issue #772 Codex P2): a run that consumes only a small fraction of
/// its deadline budget must NOT trip — proving the deadline branch reads the
/// advancing clock rather than being unconditionally true. This case passes
/// both before and after the fix, so it guards against an over-eager change.
#[tokio::test]
async fn deadline_probe_does_not_trip_below_fraction_in_test_env() {
    let outcome = WorkflowTestEnv::new()
        .with_execution_timeout(chrono::Duration::seconds(100))
        .run(deadline_no_trip_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!(false)),
        "consuming only 10s of a 100s budget must NOT trip should_continue_as_new(); \
         result: {:?}",
        outcome.result,
    );
}
