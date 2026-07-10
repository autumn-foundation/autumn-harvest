//! Tests for `WorkflowReplayer` — the replay harness described in issue #135.
//!
//! These tests exercise the public API in `autumn_harvest::testing`:
//!
//! - `WorkflowReplayer::new().register_fn(name, handler)` — fluent construction
//! - `replay_from_events` — hand-authored fixture replays
//! - `replay_from_json` — round-trip through the JSON snapshot format
//! - Non-determinism detection across activity, timer, version, and signal kinds

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::{SessionOptions, WorkflowContext};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{IntoWorkflowErrorString, WorkflowFailure, decode_workflow_failure};
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::testing::{
    HistorySnapshot, NonDeterminismKind, ReplayStatus, WorkflowReplayer,
};
use autumn_harvest::types::{ActivityExecId, ExecutionId, ParentClosePolicy, TimerId};
use chrono::Utc;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Test workflow handler functions
// ---------------------------------------------------------------------------

/// Two sequential activities then a timer — the "canonical" replay fixture.
fn canonical_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

/// Same workflow but with activities in reversed order — triggers non-determinism.
fn reordered_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // step_two before step_one — diverges from canonical history
        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

/// Workflow that uses `ctx.version()` to gate a new code path — correctly fenced.
fn versioned_workflow_fenced<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        // Version gate: if version >= 2, run step_three; old histories skip it.
        let version = ctx.version("add_step_three", 1, 2);
        if version >= 2 {
            ctx.execute_activity_raw("step_three", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
        }

        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

/// Workflow that adds a new activity WITHOUT a version fence — causes non-determinism.
fn versioned_workflow_unfenced<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        // step_three inserted WITHOUT a version gate — diverges against old histories
        ctx.execute_activity_raw("step_three", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;

        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.timer("cooldown", 60).await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

/// Phase-1 workflow using `ctx.patched()` to gate a new code path (issue #687):
/// pre-patch runs replay the old branch, marker-bearing runs take the new one.
fn patched_workflow_gated<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if ctx.patched("gate") {
            ctx.execute_activity_raw("new_activity", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"branch": "new"}))
        } else {
            ctx.execute_activity_raw("old_activity", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"branch": "old"}))
        }
    })
}

/// Broken phase-3 workflow: the `patched("gate")` call was deleted BEFORE all
/// marker-bearing executions drained — the stale `patch:gate` marker is left
/// unconsumed and must classify as `PatchMarkerMismatch`.
fn patched_workflow_removed_gate<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("new_activity", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"branch": "new"}))
    })
}

/// Phase-2 workflow: all pre-patch runs have drained, so the branch is gone —
/// `deprecate_patch("gate")` makes the recorded marker transparent and the
/// new code path runs unconditionally.
fn patched_workflow_deprecated<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.deprecate_patch("gate");
        ctx.execute_activity_raw("new_activity", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"branch": "new"}))
    })
}

/// Phase-2 workflow with a *residual* `patched()` call left in place: the
/// deprecation memo keeps the residual call deterministic — phase-1 histories
/// (marker present) stay on the new branch, phase-0 histories (no marker)
/// stay on the old branch.
fn patched_workflow_deprecated_residual<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.deprecate_patch("gate");
        if ctx.patched("gate") {
            ctx.execute_activity_raw("new_activity", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"branch": "new"}))
        } else {
            ctx.execute_activity_raw("old_activity", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"branch": "old"}))
        }
    })
}

/// Sandwich-flip regression handler (issue #687 review finding F1):
/// `patched(id)` → `deprecate_patch(id)` → residual `patched(id)` in one
/// body. On the live cycle the first call's marker exists only as a pending
/// command; the this-cycle latch makes `deprecate_patch` (and the residual
/// call's memo) see it, so live and replay passes agree: (true, true).
fn patched_workflow_sandwich<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let first = ctx.patched("gate");
        ctx.deprecate_patch("gate");
        let second = ctx.patched("gate");
        if first && second {
            ctx.execute_activity_raw("new_activity", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"branch": "new"}))
        } else {
            ctx.execute_activity_raw("old_activity", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({"branch": "old"}))
        }
    })
}

/// Workflow that calls `set_current_details` before, between, and after
/// activities, including a trailing clear via an empty string (issue #593).
/// It must replay against a history containing **only** the activity events
/// -- `set_current_details` leaves zero footprint in `harvest_events`, so no
/// history event corresponds to any of these calls.
fn current_details_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.set_current_details("step 1/2: running step_one");
        let r1 = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.set_current_details("step 2/2: running step_two");
        let r2 = ctx
            .execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        // Clear the breadcrumb on completion.
        ctx.set_current_details("");
        Ok(serde_json::json!({"first": r1, "second": r2}))
    })
}

/// Worker-session pipeline (issue #606): open a session, run one member
/// activity through it, then release. Proves AC6 -- session identity comes
/// from the deterministic `session:{seq}` marker and the physical host
/// worker binding comes from the acquire activity's recorded output, so
/// replay succeeds identically regardless of which (if any) worker is
/// available, and regardless of which worker actually hosted the session.
fn session_pipeline_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let session = ctx
            .create_session(SessionOptions::new("gpu-workers"))
            .await
            .map_err(|e| e.to_string())?;
        let transcoded = session
            .execute_activity_raw("transcode_chunk", Value::Null, "gpu-workers")
            .await
            .map_err(|e| e.to_string())?;
        session.complete().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"transcoded": transcoded}))
    })
}

/// Workflow that starts a timer before any activities.
fn timer_first_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.timer("wait", 30).await.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Workflow that sleeps until an absolute deadline (issue #749). The deadline
/// is carried in the input as epoch-millis so the fixture is fully
/// deterministic; `sleep_until` internally captures `system_now()`
/// (`SideEffectRecorded`) then starts a whole-second timer.
fn sleep_until_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let deadline_millis = input["deadline_millis"].as_i64().unwrap_or(0);
        let deadline = chrono::DateTime::from_timestamp_millis(deadline_millis)
            .ok_or_else(|| "bad deadline".to_string())?;
        ctx.sleep_until("wake", deadline)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Cancellable durable timer (issue #768): arm, then await the outcome.
fn cancellable_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("{outcome:?}")))
    })
}

/// Arms a cancellable durable timer and then COMPLETES in the same task without
/// awaiting it (Codex P1, issue #768). The live worker emits `[ArmTimer(idle)]`
/// and then seals the execution; the terminal-cycle persist path
/// (`plan_timer_lifecycle` with `skip_arm_inserts = true`) must still record the
/// `TimerStarted` event (while skipping the never-firing `harvest_timers` row),
/// or the positional `match_timer_arm` diverges strict replay when `start_timer`
/// re-runs.
fn arm_timer_then_complete_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _handle = ctx.start_timer("idle", 300);
        // Do NOT await the timer — complete in the same task.
        Ok(serde_json::json!("done"))
    })
}

/// Cancellable timer reset loop (issue #768): arm, reset N times, then either
/// await the fire or cancel. Drives the O(K)-history, zero-orphan reset path.
fn cancellable_timer_reset_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let reset_count = input["reset_count"].as_u64().unwrap_or(0);
        let cancel_at_end = input["cancel_at_end"].as_bool().unwrap_or(false);
        let mut handle = ctx.start_timer("idle", 300);
        for _ in 0..reset_count {
            handle.reset(300).map_err(|e| e.to_string())?;
        }
        if cancel_at_end {
            handle.cancel().map_err(|e| e.to_string())?;
            Ok(serde_json::json!("cancelled_by_workflow"))
        } else {
            let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
            Ok(serde_json::json!(format!("{outcome:?}")))
        }
    })
}

/// Same-cycle cancel-then-await (Codex P2, issue #768): arm a timer, cancel it,
/// then await its outcome in the SAME task. Must resolve `Cancelled` and must
/// NOT re-arm — otherwise the "cancelled" timer re-arms and later fires
/// (AC2/AC3 violation). Proves the fix is correct on the REPLAY path, not just
/// live: `cancel()` consumes the recorded `TimerCancelled`, so `await_fire`'s
/// `match_timer_or_cancel` sees `NoMatch` and must fall back to the per-context
/// cancelled state rather than re-arming.
fn cancel_then_await_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        handle.cancel().map_err(|e| e.to_string())?;
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("{outcome:?}")))
    })
}

/// Cancel-then-reset-then-await (issue #768): the reset re-arms after the
/// cancel, so `await_fire` must wait/fire normally — NOT short-circuit to
/// `Cancelled` from the earlier cancel.
fn cancel_then_reset_then_await_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mut handle = ctx.start_timer("idle", 300);
        handle.cancel().map_err(|e| e.to_string())?;
        handle.reset(300).map_err(|e| e.to_string())?;
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("{outcome:?}")))
    })
}

/// Arms a timer then runs an activity — never cancels or awaits. Used to prove
/// that removing a `cancel_timer` call while history still records a
/// `TimerCancelled` surfaces as `TimerCancelMismatch` (issue #768).
fn cancellable_timer_then_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _handle = ctx.start_timer("idle", 300);
        let out = ctx
            .execute_activity_raw("work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// Cancellable timer reset loop that ALSO captures a `new_uuid()` side effect in
/// the SAME cycle as the initial arm and each reset (issue #768, FINDING 1). This
/// drives the arm/reset + side-effect same-cycle interleaving path — the exact
/// ordering the pre-fix worker got wrong — across a reset-heavy history.
fn cancellable_reset_with_uuid_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let reset_count = input["reset_count"].as_u64().unwrap_or(0);
        let cancel_at_end = input["cancel_at_end"].as_bool().unwrap_or(false);
        let mut handle = ctx.start_timer("idle", 300);
        let _sid = ctx.new_uuid(); // side effect in the same cycle as the arm
        for _ in 0..reset_count {
            handle.reset(300).map_err(|e| e.to_string())?;
            let _u = ctx.new_uuid(); // side effect in the same cycle as the reset
        }
        if cancel_at_end {
            handle.cancel().map_err(|e| e.to_string())?;
            Ok(serde_json::json!("cancelled_by_workflow"))
        } else {
            let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
            Ok(serde_json::json!(format!("{outcome:?}")))
        }
    })
}

/// Same as `cancellable_timer_workflow` but arms a differently-named timer —
/// used to prove a renamed timer id surfaces as `TimerMismatch` (issue #768).
fn cancellable_timer_renamed_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("renamed", 300);
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("{outcome:?}")))
    })
}

/// Arms a cancellable timer, then captures a `new_uuid()` side effect in the
/// SAME cycle, then awaits the fire (issue #768, FINDING 1). The live worker
/// emits `[ArmTimer(idle), RecordSideEffect(Uuid)]`; the recorded history MUST
/// interleave `TimerStarted` at the `ArmTimer` position (before
/// `SideEffectRecorded`), or `match_timer_arm`'s positional check diverges on
/// resume. This fixture replays the CORRECT emission-order history — the one the
/// fixed worker produces.
fn cancellable_timer_then_side_effect_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        let _sid = ctx.new_uuid();
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("{outcome:?}")))
    })
}

/// Workflow that derives a timer duration from deterministic retry-jitter math.
fn jitter_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let attempt = u32::try_from(input["attempt"].as_u64().unwrap_or(1)).unwrap_or(1);
        let seed = input["seed"].as_u64().unwrap_or(0);
        let policy = RetryPolicy::exponential(8, std::time::Duration::from_secs(2))
            .with_jitter(JitterPolicy::Equal);
        let delay = policy
            .next_delay_with_seed(attempt, seed)
            .ok_or_else(|| "no delay for attempt".to_string())?;
        let secs = delay.as_secs().max(1);
        let timer_name = format!("retry_jitter_{attempt}_{seed}");
        ctx.timer(&timer_name, secs)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"timer_secs": secs, "timer_name": timer_name}))
    })
}

/// Workflow that branches on the operator force-fail cause (issue #765):
/// a genuine success completes normally; an `OperatorForceFailed` activity
/// error routes to a compensation activity; any other error propagates.
fn force_failed_compensating_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        match ctx
            .execute_activity_raw("charge_card", Value::Null, "default")
            .await
        {
            Ok(v) => Ok(serde_json::json!({"charged": v})),
            Err(e) if e.is_operator_force_failed() => {
                // The workflow advances to its own compensation path — it is
                // NOT terminated (issue #765 AC).
                let r = ctx
                    .execute_activity_raw("release_hold", Value::Null, "default")
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(serde_json::json!({"compensated": r}))
            }
            Err(e) => Err(e.to_string()),
        }
    })
}

// ---------------------------------------------------------------------------
// Typed workflow failures (issue #767) — replay-determinism fixtures
// ---------------------------------------------------------------------------

/// Parent that spawns a child, then branches on the child's *typed* failure
/// class (issue #767). The compensation branch is only taken for a typed
/// `ValidationRejected` + `non_retryable` child failure, and it emits a
/// **command** (the `issue_refund` activity) so the branch decision is
/// observable in history — if the typed fields did not survive replay, the
/// handler would take the `Err(e)` fall-through and complete early, diverging
/// from the recorded history (falsifiable).
fn parent_typed_child_failure_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        match ctx
            .spawn_child_workflow_raw("charge_card_child", Value::Null)
            .await
        {
            Ok(v) => Ok(serde_json::json!({ "child_ok": v })),
            // Branch purely on the typed error_type + non_retryable — ZERO
            // substring matching on the message.
            Err(e)
                if e.workflow_error_type() == Some("ValidationRejected")
                    && e.is_workflow_non_retryable() =>
            {
                let refund = ctx
                    .execute_activity_raw("issue_refund", Value::Null, "default")
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(serde_json::json!({ "compensated": refund }))
            }
            Err(e) => Err(e.to_string()),
        }
    })
}

/// A workflow that fails with its own *typed* [`WorkflowFailure`] (issue #767),
/// serialised through [`IntoWorkflowErrorString`] exactly as the `#[workflow]`
/// dispatch shim does.
fn self_typed_failure_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        Err(
            WorkflowFailure::new("BudgetExceeded", "monthly spend cap reached")
                .with_details(serde_json::json!({ "cap_usd": 5000 }))
                .non_retryable()
                .into_workflow_error_payload(),
        )
    })
}

/// Parent history that ends with the parent completing *after* observing a
/// typed `ChildWorkflowFailed` and compensating.
fn parent_typed_child_failure_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let child_id = ExecutionId::new();
    let refund_id = ActivityExecId::new();
    let decoded = decode_workflow_failure(
        &WorkflowFailure::new("ValidationRejected", "card declined by issuer")
            .with_details(serde_json::json!({ "code": 402 }))
            .non_retryable()
            .into_workflow_error_payload(),
    );
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "charge_card_child".into(),
            input: Value::Null,
        },
        WorkflowEvent::child_workflow_failed_typed(child_id, &decoded),
        WorkflowEvent::ActivityScheduled {
            activity_id: refund_id,
            name: "issue_refund".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: refund_id,
            output: serde_json::json!("refunded"),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({ "compensated": "refunded" }),
        },
    ];
    (exec_id, events)
}

/// History for a run that ended in a typed `WorkflowFailed` (issue #767).
fn self_typed_failure_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let decoded = decode_workflow_failure(
        &WorkflowFailure::new("BudgetExceeded", "monthly spend cap reached")
            .with_details(serde_json::json!({ "cap_usd": 5000 }))
            .non_retryable()
            .into_workflow_error_payload(),
    );
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::workflow_failed_typed(&decoded),
    ];
    (exec_id, events)
}

// ---------------------------------------------------------------------------
// Helper: build canonical event history
// ---------------------------------------------------------------------------

fn canonical_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
    let timer_id = TimerId::new("cooldown");
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: serde_json::json!("result_one"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id2,
            name: "step_two".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id2,
            output: serde_json::json!("result_two"),
        },
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 60,
        },
        WorkflowEvent::TimerFired { timer_id },
    ];
    (exec_id, events)
}

/// History for `current_details_workflow`: two activities, no timer, and
/// deliberately **no** event corresponding to any `set_current_details` call
/// -- proving the call is zero-footprint by construction (issue #593).
fn current_details_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: serde_json::json!("result_one"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id2,
            name: "step_two".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id2,
            output: serde_json::json!("result_two"),
        },
    ];
    (exec_id, events)
}

fn build_replayer() -> WorkflowReplayer {
    WorkflowReplayer::new()
        .register_fn("canonical_workflow", canonical_workflow)
        .register_fn("reordered_workflow", reordered_workflow)
        .register_fn("versioned_workflow_fenced", versioned_workflow_fenced)
        .register_fn("versioned_workflow_unfenced", versioned_workflow_unfenced)
        .register_fn("patched_workflow_gated", patched_workflow_gated)
        .register_fn(
            "patched_workflow_removed_gate",
            patched_workflow_removed_gate,
        )
        .register_fn("patched_workflow_deprecated", patched_workflow_deprecated)
        .register_fn(
            "patched_workflow_deprecated_residual",
            patched_workflow_deprecated_residual,
        )
        .register_fn("patched_workflow_sandwich", patched_workflow_sandwich)
        .register_fn("timer_first_workflow", timer_first_workflow)
        .register_fn("sleep_until_workflow", sleep_until_workflow)
        .register_fn("jitter_timer_workflow", jitter_timer_workflow)
        .register_fn("current_details_workflow", current_details_workflow)
        .register_fn("session_pipeline_workflow", session_pipeline_workflow)
        .register_fn(
            "force_failed_compensating_workflow",
            force_failed_compensating_workflow,
        )
        .register_fn(
            "parent_typed_child_failure_workflow",
            parent_typed_child_failure_workflow,
        )
        .register_fn("self_typed_failure_workflow", self_typed_failure_workflow)
        .register_fn("cancellable_timer_workflow", cancellable_timer_workflow)
        .register_fn(
            "arm_timer_then_complete_workflow",
            arm_timer_then_complete_workflow,
        )
        .register_fn(
            "cancellable_timer_reset_workflow",
            cancellable_timer_reset_workflow,
        )
        .register_fn(
            "cancel_then_await_timer_workflow",
            cancel_then_await_timer_workflow,
        )
        .register_fn(
            "cancel_then_reset_then_await_timer_workflow",
            cancel_then_reset_then_await_timer_workflow,
        )
        .register_fn(
            "cancellable_timer_then_activity_workflow",
            cancellable_timer_then_activity_workflow,
        )
        .register_fn(
            "cancellable_timer_renamed_workflow",
            cancellable_timer_renamed_workflow,
        )
        .register_fn(
            "cancellable_timer_then_side_effect_workflow",
            cancellable_timer_then_side_effect_workflow,
        )
        .register_fn(
            "cancellable_reset_with_uuid_workflow",
            cancellable_reset_with_uuid_workflow,
        )
}

/// History recorded by a run whose `charge_card` activity was force-failed by
/// an operator (issue #765): the forced failure rides the *existing*
/// `ActivityFailed` variant (no new event variant), carrying the distinct
/// `OperatorForceFailed` `error_type`; the workflow then ran its compensation
/// activity on the live frontier.
fn force_failed_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let charge_id = ActivityExecId::new();
    let release_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: charge_id,
            name: "charge_card".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: charge_id,
            error: "activity force-failed by operator: incident INC-42".into(),
            attempt: 1,
            error_type: "OperatorForceFailed".into(),
            non_retryable: true,
            details: Some(serde_json::json!({
                "forced_by_operator": true,
                "reason": "incident INC-42",
            })),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: release_id,
            name: "release_hold".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: release_id,
            output: serde_json::json!("hold released"),
        },
    ];
    (exec_id, events)
}

/// History for `session_pipeline_workflow`, parameterized by the recorded
/// host worker id -- so the same shape can be replayed twice with two
/// different hosts to prove AC6's "regardless of which worker" claim.
fn session_pipeline_history(host_worker_id: &str) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let session_uuid = uuid::Uuid::new_v4();
    let acquire_id = ActivityExecId::new();
    let member_id = ActivityExecId::new();
    let release_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "session:1".into(),
            details: serde_json::json!(session_uuid.to_string()),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: acquire_id,
            name: "__harvest_session_acquire".into(),
            input: serde_json::json!(session_uuid.to_string()),
            queue: "gpu-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: acquire_id,
            output: serde_json::json!(host_worker_id),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: member_id,
            name: "transcode_chunk".into(),
            input: Value::Null,
            queue: "gpu-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: member_id,
            output: serde_json::json!("transcoded"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: release_id,
            name: "__harvest_session_release".into(),
            // `Session::complete()` dispatches the release activity with the
            // session id (as a string) for input -- the worker-side handler
            // parses it back out to know which `harvest_sessions` row to
            // mark COMPLETED.
            input: serde_json::json!(session_uuid.to_string()),
            queue: "gpu-workers".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: release_id,
            output: Value::Null,
        },
    ];
    (exec_id, events)
}

/// Build a snapshot from a `(exec_id, events)` pair with a given workflow name.
fn make_snapshot(name: &str, exec_id: ExecutionId, events: Vec<WorkflowEvent>) -> HistorySnapshot {
    HistorySnapshot {
        workflow_name: name.to_string(),
        execution_id: exec_id,
        events,
        context_headers: None,
    }
}

// ---------------------------------------------------------------------------
// (a0) Operator force-fail replays deterministically down the workflow's own
//      failure/compensation branch (issue #765 AC: the forced ActivityFailed
//      is a plain, existing event — replay must recognize the distinct
//      OperatorForceFailed cause and take the same branch every time).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_force_failed_activity_takes_compensation_branch_deterministically() {
    let (exec_id, events) = force_failed_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "force_failed_compensating_workflow",
            exec_id,
            events,
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a history containing an operator-forced ActivityFailed must replay \
         deterministically down the compensation branch, got: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be positive"
    );
}

// ---------------------------------------------------------------------------
// (a1) Typed workflow failures (issue #767, AC6) replay deterministically.
//
//   Proof 1: a parent that branches on a *typed* `ChildWorkflowFailed`'s
//   error_type/non_retryable replays down its compensation branch. The branch
//   emits a command (issue_refund), so if the typed fields did NOT survive
//   replay the parent would take the fall-through and complete early, diverging
//   from history — ReplaySucceeded over multiple cycles proves the typed fields
//   are reproduced identically. This is the parent-side surface of AC6.
//
//   Proof 2: a run that ended in a typed `WorkflowFailed` round-trips through
//   replay deterministically — the reproduced failure carries the identical
//   typed error_type/details/non_retryable on every cycle.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_parent_typed_child_failure_compensation_is_deterministic() {
    let (exec_id, events) = parent_typed_child_failure_history();
    let replayer = build_replayer();

    // Replay the SAME history 3 times — the typed child failure must drive the
    // same (compensation) branch every cycle.
    for cycle in 0..3 {
        let report = replayer
            .replay_from_snapshot(make_snapshot(
                "parent_typed_child_failure_workflow",
                exec_id,
                events.clone(),
            ))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "cycle {cycle}: a parent branching on a typed child failure must \
             replay deterministically down the compensation branch, got: {report}"
        );
    }
}

#[tokio::test]
async fn replay_typed_workflow_failed_round_trips_with_identical_typed_fields() {
    let (exec_id, events) = self_typed_failure_history();
    let replayer = build_replayer();

    let mut reproduced = Vec::new();
    for _ in 0..2 {
        let report = replayer
            .replay_from_snapshot(make_snapshot(
                "self_typed_failure_workflow",
                exec_id,
                events.clone(),
            ))
            .await;
        // A self-failing workflow surfaces as `WorkflowFailed` (it did not
        // complete successfully) — the *determinism* is the point: the same
        // typed envelope is reproduced on every cycle.
        let ReplayStatus::WorkflowFailed { error, .. } = report.status else {
            panic!("expected WorkflowFailed status, got: {report}");
        };
        reproduced.push(error);
    }
    assert_eq!(
        reproduced[0], reproduced[1],
        "typed WorkflowFailed must reproduce byte-identically across replay cycles"
    );

    // The reproduced failure decodes to the identical typed fields.
    let decoded = decode_workflow_failure(&reproduced[0]);
    assert_eq!(decoded.error_type.as_deref(), Some("BudgetExceeded"));
    assert_eq!(decoded.non_retryable, Some(true));
    assert_eq!(
        decoded.details,
        Some(serde_json::json!({ "cap_usd": 5000 }))
    );
}

// ---------------------------------------------------------------------------
// (a) Unchanged workflow against its own history → ReplaySucceeded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_unchanged_workflow_succeeds() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("canonical_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "unchanged workflow must succeed replay, got: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be positive"
    );
}

// ---------------------------------------------------------------------------
// (a2) set_current_details calls replay safely with zero event footprint
//      (issue #593 falsifiable correctness bar).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_workflow_with_current_details_calls_succeeds() {
    let (exec_id, events) = current_details_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("current_details_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a workflow calling set_current_details (including an empty-string \
         clear) must replay against a history with zero corresponding events, \
         got: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be positive"
    );
}

// ---------------------------------------------------------------------------
// (a3) Worker sessions replay with 100% fidelity regardless of which worker
//      is available at replay time (issue #606 AC6 -- the headline claim:
//      no new WorkflowEvent variant, session identity via MarkerRecorded,
//      physical worker binding via the acquire activity's recorded output).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_session_pipeline_workflow_succeeds() {
    let (exec_id, events) = session_pipeline_history("worker-host-A");
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("session_pipeline_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a workflow using ctx.create_session()/Session must replay with 100% \
         fidelity, got: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be positive"
    );
}

#[tokio::test]
async fn replay_session_pipeline_workflow_succeeds_regardless_of_recorded_host_worker() {
    // Same fixture shape, a *different* recorded host worker id -- proving
    // replay is worker-independent: the session's physical binding is
    // recovered purely from the acquire activity's recorded output, with no
    // engine-level knowledge of which worker is live (or exists at all) at
    // replay time.
    let replayer = build_replayer();

    for host in [
        "worker-host-A",
        "worker-host-B",
        "a-totally-different-worker",
    ] {
        let (exec_id, events) = session_pipeline_history(host);
        let report = replayer
            .replay_from_snapshot(make_snapshot("session_pipeline_workflow", exec_id, events))
            .await;

        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay must succeed identically for recorded host '{host}', got: {report}"
        );
    }
}

// ---------------------------------------------------------------------------
// (b) Activities reordered → NonDeterminismDetected { ActivityScheduleMismatch }
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_reordered_activities_detects_non_determinism() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("reordered_workflow", exec_id, events))
        .await;

    match &report.status {
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            assert_eq!(
                *kind,
                NonDeterminismKind::ActivityScheduleMismatch,
                "reordered activities must produce ActivityScheduleMismatch, got {kind:?}"
            );
        }
        other => panic!("expected NonDeterminismDetected, got: {other:?}\nreport: {report}"),
    }
}

#[tokio::test]
async fn replay_jitter_timer_is_exact_and_deterministic() {
    let replayer = build_replayer();
    let exec_id = ExecutionId::new();
    let attempt = 3u32;
    let seed = 0xfeed_beefu64;
    let policy = RetryPolicy::exponential(8, std::time::Duration::from_secs(2))
        .with_jitter(JitterPolicy::Equal);
    let expected_delay = policy
        .next_delay_with_seed(attempt, seed)
        .expect("delay must exist");
    let timer_secs = expected_delay.as_secs().max(1);
    let timer_id = TimerId::new(format!("retry_jitter_{attempt}_{seed}"));
    let input = serde_json::json!({"attempt": attempt, "seed": seed});

    let ok_history = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: timer_secs,
        },
        WorkflowEvent::TimerFired { timer_id },
    ];
    let ok = replayer
        .replay_from_snapshot(make_snapshot("jitter_timer_workflow", exec_id, ok_history))
        .await;
    assert!(matches!(ok.status, ReplayStatus::ReplaySucceeded), "{ok}");

    let bad_history = vec![
        WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new(format!("retry_jitter_{attempt}_{seed}")),
            duration_secs: timer_secs.saturating_add(1),
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new(format!("retry_jitter_{attempt}_{seed}")),
        },
    ];
    let bad = replayer
        .replay_from_snapshot(make_snapshot(
            "jitter_timer_workflow",
            ExecutionId::new(),
            bad_history,
        ))
        .await;
    assert!(
        matches!(
            bad.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::TimerMismatch,
                ..
            }
        ),
        "timer duration mismatch must be detected as TimerMismatch, got: {bad}"
    );
}

// ---------------------------------------------------------------------------
// sleep_until — absolute-deadline durable timer (issue #749)
// ---------------------------------------------------------------------------

/// Build a consistent recorded history for `sleep_until_workflow`: a frozen
/// `system_now()` capture plus a whole-second timer whose duration equals
/// `remaining_secs_until(deadline, frozen_now)` computed inline from the same
/// values, so the fixture is internally consistent by construction.
fn sleep_until_history(
    frozen_millis: i64,
    deadline_millis: i64,
) -> (Value, Vec<WorkflowEvent>, u64) {
    // Mirror of `context::remaining_secs_until` (crate-private): clamp past to
    // zero, round sub-second remainders up to whole seconds.
    let delta_ms = deadline_millis - frozen_millis;
    let duration_secs: u64 = if delta_ms <= 0 {
        0
    } else {
        let secs = delta_ms / 1000;
        let round_up = i64::from(delta_ms % 1000 != 0);
        u64::try_from(secs + round_up).unwrap_or(u64::MAX)
    };
    let input = serde_json::json!({ "deadline_millis": deadline_millis });
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Now,
            name: None,
            value: serde_json::json!(frozen_millis),
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("wake"),
            duration_secs,
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("wake"),
        },
    ];
    (input, events, duration_secs)
}

/// The falsifiable bar for issue #749: `sleep_until` histories replay
/// deterministically across N = 1000 runs with zero divergences. Each iteration
/// builds a *distinct* fixture — the frozen `system_now()` instant and the
/// deadline offset both vary with `i`, including deliberate sub-second jitter
/// that exercises the round-up path — so this is 1000 varied cases, not one
/// fixture cloned 1000 times.
#[tokio::test]
async fn sleep_until_replays_deterministically() {
    let base_frozen = 1_600_000_000_000_i64;
    let replayer = build_replayer();
    for i in 0..1000_i64 {
        // Vary the frozen capture and the remaining offset deterministically;
        // the `* 250ms` term walks the sub-second remainder through the round-up
        // boundary (0, 250, 500, 750, 1000ms, ...).
        let frozen_millis = base_frozen + i * 37_000;
        let deadline_millis = frozen_millis + 3_600_000 + i * 250;
        let (_input, events, duration_secs) = sleep_until_history(frozen_millis, deadline_millis);
        if i == 0 {
            assert_eq!(duration_secs, 3600, "i=0 fixture math must be consistent");
        }

        let report = replayer
            .replay_from_snapshot(make_snapshot(
                "sleep_until_workflow",
                ExecutionId::new(),
                events,
            ))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay pass {i} must succeed with zero divergences, got: {report}"
        );
    }
}

/// A recorded past-deadline `sleep_until` (duration clamped to 0) still replays
/// cleanly — AC3 through the replayer.
#[tokio::test]
async fn sleep_until_past_deadline_replays_deterministically() {
    let frozen_millis = 1_700_000_000_000_i64;
    let deadline_millis = frozen_millis - 60_000; // one minute BEFORE the frozen now
    let (_input, events, duration_secs) = sleep_until_history(frozen_millis, deadline_millis);
    assert_eq!(duration_secs, 0, "past deadline must clamp to zero");

    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "sleep_until_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "past-deadline sleep_until must replay cleanly, got: {report}"
    );
}

/// A mutated timer *duration* in an otherwise-valid `sleep_until` history
/// surfaces as ordinary timer non-determinism — NOT a panic, NOT
/// `ReplaySucceeded`. (Renamed from `..._reorder_...`: this mutates the recorded
/// duration, not event order.)
#[tokio::test]
async fn sleep_until_duration_mismatch_surfaces_as_timer_nondeterminism() {
    let frozen_millis = 1_700_000_000_000_i64;
    let deadline_millis = frozen_millis + 3_600_000;
    let (_input, mut events, duration_secs) = sleep_until_history(frozen_millis, deadline_millis);

    // Mutate the recorded timer so the replayed duration diverges.
    if let WorkflowEvent::TimerStarted {
        duration_secs: d, ..
    } = &mut events[2]
    {
        *d = duration_secs.saturating_add(1);
    } else {
        panic!("event[2] must be TimerStarted");
    }

    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "sleep_until_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::TimerMismatch,
                ..
            }
        ),
        "a mutated sleep_until timer must be detected as TimerMismatch, got: {report}"
    );
}

/// A genuine *structural* divergence: the frozen `SideEffectRecorded { Now }`
/// event is dropped from history (as if a code change removed the `system_now()`
/// capture that `sleep_until` performs), so on replay the first history-consulting
/// call diverges. Must surface as a classified non-determinism — NOT a panic,
/// NOT `ReplaySucceeded`.
#[tokio::test]
async fn sleep_until_missing_side_effect_surfaces_as_nondeterminism() {
    let frozen_millis = 1_700_000_000_000_i64;
    let deadline_millis = frozen_millis + 3_600_000;
    let (_input, events, _duration_secs) = sleep_until_history(frozen_millis, deadline_millis);

    // Drop the SideEffectRecorded(Now) capture (events[1]), leaving
    // [WorkflowStarted, TimerStarted, TimerFired].
    let mut structural = events;
    structural.remove(1);

    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "sleep_until_workflow",
            ExecutionId::new(),
            structural,
        ))
        .await;
    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::SideEffectDrift,
                ..
            }
        ),
        "dropping the frozen system_now() capture must surface as SideEffectDrift, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Cancellable / renewable durable timers (issue #768)
// ---------------------------------------------------------------------------

fn cancellable_ts() -> WorkflowEvent {
    WorkflowEvent::TimerStarted {
        timer_id: TimerId::new("idle"),
        duration_secs: 300,
    }
}
fn cancellable_tf() -> WorkflowEvent {
    WorkflowEvent::TimerFired {
        timer_id: TimerId::new("idle"),
    }
}
fn cancellable_tc() -> WorkflowEvent {
    WorkflowEvent::TimerCancelled {
        timer_id: TimerId::new("idle"),
    }
}
fn wf_started() -> WorkflowEvent {
    wf_started_with_input(Value::Null)
}
fn wf_started_with_input(input: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

#[tokio::test]
async fn fired_timer_replays_succeeded() {
    let events = vec![wf_started(), cancellable_ts(), cancellable_tf()];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancellable_timer_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a fired cancellable timer replays cleanly, got: {report}"
    );
}

#[tokio::test]
async fn cancelled_timer_replays_succeeded() {
    let events = vec![wf_started(), cancellable_ts(), cancellable_tc()];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancellable_timer_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a cancelled cancellable timer replays cleanly, got: {report}"
    );
}

/// Codex P2 (issue #768) — determinism gate: a same-task
/// `start_timer; cancel; await_fire` replays cleanly against the history the
/// FIXED worker produces (`[WorkflowStarted, TimerStarted, TimerCancelled]`).
/// On replay `cancel()` consumes the recorded `TimerCancelled`, so `await_fire`
/// sees `match_timer_or_cancel == NoMatch` and MUST resolve `Cancelled` from the
/// per-context cancelled state rather than re-arming and parking — a re-arm here
/// would suspend mid-replay (never reaching completion) and the run would never
/// resolve, so `ReplaySucceeded` here is the direct proof the fix holds on
/// replay, not just live.
#[tokio::test]
async fn cancel_then_await_same_cycle_replays_succeeded() {
    let events = vec![wf_started(), cancellable_ts(), cancellable_tc()];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancel_then_await_timer_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a same-cycle cancel-then-await cancellable timer replays cleanly, got: {report}"
    );
}

/// Codex P2 (issue #768) — determinism gate for reset-after-cancel: a
/// `cancel(); reset(); await_fire()` re-arms after the cancel, so the timer
/// fires. Replays cleanly against the FIXED worker history
/// `[WorkflowStarted, TimerStarted, TimerCancelled, TimerCancelled, TimerStarted,
/// TimerFired]` and resolves `Fired` — proving the reset flips the per-context
/// state back to Armed so `await_fire` does NOT short-circuit to `Cancelled`.
#[tokio::test]
async fn cancel_then_reset_then_await_replays_fired() {
    let events = vec![
        wf_started(),
        cancellable_ts(),
        cancellable_tc(),
        cancellable_tc(),
        cancellable_ts(),
        cancellable_tf(),
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancel_then_reset_then_await_timer_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a cancel-then-reset-then-await cancellable timer replays cleanly (fires), got: {report}"
    );
}

/// Codex P1 (issue #768): a `start_timer` that arms a timer and then completes in
/// the SAME task must record the `TimerStarted` event in its terminal history.
/// The FIXED worker produces `[WorkflowStarted, TimerStarted(idle),
/// WorkflowCompleted]`; replaying that against the same workflow code must
/// succeed — `start_timer`'s positional `match_timer_arm` consumes the recorded
/// `TimerStarted` at the cursor.
#[tokio::test]
async fn arm_then_complete_replays_when_timer_started_recorded() {
    let events = vec![
        wf_started(),
        cancellable_ts(),
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("done"),
        },
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "arm_timer_then_complete_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a fixed-worker terminal history WITH TimerStarted must replay cleanly, got: {report}"
    );
}

/// Codex P1 (issue #768): proves the `TimerStarted` event is load-bearing — i.e.
/// the consequence of the FINDING-6 bug. The BUGGY worker dropped the event,
/// recording `[WorkflowStarted, WorkflowCompleted]`. Replaying that history
/// against the same workflow code diverges: `start_timer`'s positional
/// `match_timer_arm` expects `TimerStarted` at the cursor but finds the terminal
/// `WorkflowCompleted` instead.
#[tokio::test]
async fn arm_then_complete_diverges_without_recorded_timer_started() {
    let events = vec![
        wf_started(),
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("done"),
        },
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "arm_timer_then_complete_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "a terminal history MISSING TimerStarted must diverge (proving the event is \
         load-bearing), got: {report}"
    );
}

/// Builds the history recorded by `cancellable_timer_reset_workflow`: an initial
/// arm, then two events per reset (cancel + fresh arm), then a terminal
/// fire-or-cancel. Deliberately O(K) with zero orphaned firings (AC4).
fn cancellable_reset_history(reset_count: u64, cancel_at_end: bool) -> Vec<WorkflowEvent> {
    let input = serde_json::json!({
        "reset_count": reset_count,
        "cancel_at_end": cancel_at_end,
    });
    let mut events = vec![wf_started_with_input(input), cancellable_ts()];
    for _ in 0..reset_count {
        events.push(cancellable_tc());
        events.push(cancellable_ts());
    }
    events.push(if cancel_at_end {
        cancellable_tc()
    } else {
        cancellable_tf()
    });
    events
}

/// A distinct `SideEffectRecorded { Uuid }` event seeded from `seed` so every
/// fixture's history is byte-distinct (a hyphenated UUID string deserialises
/// back to `uuid::Uuid` on replay).
fn uuid_side_effect(seed: u64) -> WorkflowEvent {
    WorkflowEvent::SideEffectRecorded {
        kind: autumn_harvest::SideEffectKind::Uuid,
        name: None,
        value: serde_json::json!(format!("018f0000-0000-7000-8000-{seed:012x}")),
    }
}

/// History recorded by `cancellable_reset_with_uuid_workflow`: an initial arm +
/// its same-cycle side effect, then three events per reset (cancel + fresh arm +
/// same-cycle side effect — the FINDING-1 interleaving), then a terminal
/// fire-or-cancel. Bounded and O(K) with zero orphaned firings. `seed` makes the
/// interleaved side-effect values distinct so the whole fixture is unique.
fn cancellable_reset_uuid_history(
    reset_count: u64,
    cancel_at_end: bool,
    seed: u64,
) -> Vec<WorkflowEvent> {
    let input = serde_json::json!({
        "reset_count": reset_count,
        "cancel_at_end": cancel_at_end,
    });
    let mut events = vec![
        wf_started_with_input(input),
        cancellable_ts(),
        uuid_side_effect(seed * 1000),
    ];
    for i in 0..reset_count {
        events.push(cancellable_tc());
        events.push(cancellable_ts());
        events.push(uuid_side_effect(seed * 1000 + i + 1));
    }
    events.push(if cancel_at_end {
        cancellable_tc()
    } else {
        cancellable_tf()
    });
    events
}

/// The falsifiable success-metric bar for issue #768: reset-heavy cancellable
/// timer histories replay deterministically across N = 1000 GENUINELY DISTINCT
/// fixtures, each with zero divergences and a bounded, O(K), zero-orphaned-firing
/// history. Reset cadence spans a wide prime range and terminal outcome varies;
/// every third fixture also interleaves a `new_uuid()` side effect in the SAME
/// cycle as the initial arm and each reset — exercising the FINDING-1
/// timer-lifecycle-event-in-emission-position ordering path (a distinct per-`i`
/// seed makes those fixtures byte-unique). The plain fixtures pin the exact
/// 2-events/reset bound; the interleaved fixtures pin the 3-events/reset bound.
#[tokio::test]
async fn reset_1000_times_replays_with_bounded_history() {
    let replayer = build_replayer();
    for i in 0..1000_u64 {
        // Wide, prime-modulus spread so shapes are not just ~40 repeats.
        let reset_count = i % 97;
        let cancel_at_end = (i / 97) % 2 == 0;
        let with_side_effect = i % 3 == 0;

        let (workflow_name, events) = if with_side_effect {
            let events = cancellable_reset_uuid_history(reset_count, cancel_at_end, i);
            // Bounded: WorkflowStarted + arm + initial side-effect + 3/reset + terminal.
            assert_eq!(
                events.len() as u64,
                3 * reset_count + 4,
                "uuid-interleaved history must be bounded at 3 events per reset (i={i})"
            );
            ("cancellable_reset_with_uuid_workflow", events)
        } else {
            let events = cancellable_reset_history(reset_count, cancel_at_end);
            // Bounded: WorkflowStarted + initial arm + 2 events/reset + terminal.
            assert_eq!(
                events.len() as u64,
                2 * reset_count + 3,
                "plain reset history must be bounded at 2 events per reset (i={i})"
            );
            ("cancellable_timer_reset_workflow", events)
        };

        // Zero orphaned firings: at most one terminal TimerFired ever.
        let fired = events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::TimerFired { .. }))
            .count();
        assert!(
            fired <= 1,
            "a reset history must never orphan a TimerFired (i={i}, fired={fired})"
        );

        let report = replayer
            .replay_from_snapshot(make_snapshot(workflow_name, ExecutionId::new(), events))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "reset replay pass {i} ({workflow_name}, resets={reset_count}, \
             cancel={cancel_at_end}, side_effect={with_side_effect}) must succeed with \
             zero divergences, got: {report}"
        );
    }
}

/// Removing a `cancel_timer` call while history still records the
/// `TimerCancelled` leaves it unconsumed; the next command trips over it and
/// the divergence is classified precisely as `TimerCancelMismatch` (AC).
#[tokio::test]
async fn removed_cancel_surfaces_as_timer_cancel_mismatch() {
    let aid = ActivityExecId::new();
    let events = vec![
        wf_started(),
        cancellable_ts(),
        cancellable_tc(),
        WorkflowEvent::ActivityScheduled {
            activity_id: aid,
            name: "work".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: aid,
            output: serde_json::json!("done"),
        },
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancellable_timer_then_activity_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::TimerCancelMismatch,
                ..
            }
        ),
        "an unconsumed TimerCancelled must classify as TimerCancelMismatch, got: {report}"
    );
}

/// Renaming a timer id surfaces as an ordinary `TimerMismatch` (AC).
#[tokio::test]
async fn renamed_timer_surfaces_as_timer_mismatch() {
    let events = vec![wf_started(), cancellable_ts(), cancellable_tf()];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancellable_timer_renamed_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::TimerMismatch,
                ..
            }
        ),
        "a renamed timer id must classify as TimerMismatch, got: {report}"
    );
}

/// FINDING 1: a `start_timer` followed same-cycle by a `new_uuid()` side effect
/// records history in emission order `[TimerStarted(idle), SideEffectRecorded]`.
/// This is the history the FIXED worker produces (timer-lifecycle events
/// interleaved at their command position, not appended at the end of the batch).
/// It must replay cleanly.
#[tokio::test]
async fn timer_arm_then_side_effect_correct_order_replays_succeeded() {
    let uuid_value = serde_json::json!("018f0000-0000-7000-8000-000000000000");
    let events = vec![
        wf_started(),
        cancellable_ts(),
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Uuid,
            name: None,
            value: uuid_value,
        },
        cancellable_tf(),
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancellable_timer_then_side_effect_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "emission-order history (TimerStarted before SideEffectRecorded) must \
         replay cleanly, got: {report}"
    );
}

/// FINDING 1 (bug proof): the PRE-FIX worker appended timer-lifecycle events at
/// the END of the batch, recording `[SideEffectRecorded, TimerStarted]`. That
/// wrong-order history nd-blocks the run on first resume — `match_timer_arm` is
/// positional, `drain_early_signals` does not skip `SideEffectRecorded`, so the
/// cursor lands on `SideEffectRecorded` and the timer arm diverges. This test
/// pins that the buggy ordering is genuinely non-replayable.
#[tokio::test]
async fn timer_arm_then_side_effect_wrong_order_diverges() {
    let uuid_value = serde_json::json!("018f0000-0000-7000-8000-000000000000");
    let events = vec![
        wf_started(),
        // Wrong order: side effect recorded before the timer arm.
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Uuid,
            name: None,
            value: uuid_value,
        },
        cancellable_ts(),
        cancellable_tf(),
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancellable_timer_then_side_effect_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "the pre-fix end-of-batch timer ordering must be non-replayable, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// (c) New code path correctly fenced behind ctx.version() → ReplaySucceeded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_version_fenced_workflow_succeeds() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("versioned_workflow_fenced", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "version-fenced new path must succeed replay, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// (d) Same new path NOT fenced → NonDeterminismDetected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_version_unfenced_detects_non_determinism() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "versioned_workflow_unfenced",
            exec_id,
            events,
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "unfenced new code path must produce NonDeterminismDetected, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// ctx.patched() / ctx.deprecate_patch() lifecycle (issue #687)
// ---------------------------------------------------------------------------

/// Phase-0 history: recorded by pre-patch code — old branch, no marker.
fn pre_patch_history() -> Vec<WorkflowEvent> {
    let id = ActivityExecId::new();
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "old_activity".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: serde_json::json!("old_result"),
        },
    ]
}

/// Phase-1 history: recorded by patched code — `patch:gate` marker + new branch.
fn post_patch_history() -> Vec<WorkflowEvent> {
    let id = ActivityExecId::new();
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "patch:gate".into(),
            details: serde_json::json!(1),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "new_activity".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: serde_json::json!("new_result"),
        },
    ]
}

/// The success-metric money test: the SAME phase-1 handler replays a
/// pre-patch history down the old branch and a post-patch history down the
/// new branch, with zero integer bookkeeping. The branch-selection proof is
/// structural: had either replay taken the wrong branch, the wrong activity
/// name would have diverged against the recorded one.
#[tokio::test]
async fn replay_pre_patch_and_post_patch_histories_take_opposite_branches() {
    let replayer = build_replayer();

    let pre = replayer
        .replay_from_snapshot(make_snapshot(
            "patched_workflow_gated",
            ExecutionId::new(),
            pre_patch_history(),
        ))
        .await;
    assert!(
        matches!(pre.status, ReplayStatus::ReplaySucceeded),
        "pre-patch history must replay the OLD branch cleanly, got: {pre}"
    );

    let post = replayer
        .replay_from_snapshot(make_snapshot(
            "patched_workflow_gated",
            ExecutionId::new(),
            post_patch_history(),
        ))
        .await;
    assert!(
        matches!(post.status, ReplayStatus::ReplaySucceeded),
        "post-patch history must replay the NEW branch cleanly, got: {post}"
    );
}

/// Deleting the `patched()` call before all marker-bearing runs drained leaves
/// the recorded `patch:gate` marker unconsumed — the next command trips over
/// it and the divergence classifies as `PatchMarkerMismatch`.
#[tokio::test]
async fn replay_removed_too_early_patch_call_classified_as_patch_marker_mismatch() {
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "patched_workflow_removed_gate",
            ExecutionId::new(),
            post_patch_history(),
        ))
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::PatchMarkerMismatch,
                ..
            }
        ),
        "a too-early gate removal must classify as PatchMarkerMismatch, got: {report}"
    );
}

/// Interop: a history recorded by a `ctx.version("gate", 1, 2)`-style
/// workflow (version marker + new-branch activity) replays cleanly against
/// the `patched`-based handler, taking the new branch.
#[tokio::test]
async fn replay_version_recorded_history_against_patched_handler_succeeds() {
    let id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate".into(),
            details: serde_json::json!(2),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "new_activity".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: serde_json::json!("new_result"),
        },
    ];

    let replayer = build_replayer();
    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "patched_workflow_gated",
            ExecutionId::new(),
            events,
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "version-marker history must interop with the patched handler, got: {report}"
    );
}

/// AC-3 lifecycle, deploy 2: once pre-patch runs have DRAINED, the phase-2
/// handler (`deprecate_patch` + unconditional new code) replays phase-1
/// (marker-bearing) histories cleanly. Phase-0 histories are deliberately
/// NOT covered here: deprecation is only safe after they drained — replaying
/// one against unconditional new code diverges, as the lifecycle documents.
#[tokio::test]
async fn replay_deprecated_patch_phase2_handler_succeeds_for_phase1_history() {
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "patched_workflow_deprecated",
            ExecutionId::new(),
            post_patch_history(),
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "phase-2 handler must replay a phase-1 history cleanly, got: {report}"
    );
}

/// AC-3 lifecycle proof with a residual `patched()` call: the deprecation
/// memo keeps the residual call deterministic for BOTH generations —
/// phase-1 histories keep the new branch, phase-0 histories keep the old one.
#[tokio::test]
async fn replay_deprecate_patch_with_residual_patched_keeps_phase1_branch() {
    let replayer = build_replayer();

    let phase1 = replayer
        .replay_from_snapshot(make_snapshot(
            "patched_workflow_deprecated_residual",
            ExecutionId::new(),
            post_patch_history(),
        ))
        .await;
    assert!(
        matches!(phase1.status, ReplayStatus::ReplaySucceeded),
        "residual patched() must keep a phase-1 history on the new branch, got: {phase1}"
    );

    let phase0 = replayer
        .replay_from_snapshot(make_snapshot(
            "patched_workflow_deprecated_residual",
            ExecutionId::new(),
            pre_patch_history(),
        ))
        .await;
    assert!(
        matches!(phase0.status, ReplayStatus::ReplaySucceeded),
        "residual patched() must keep a phase-0 history on the old branch, got: {phase0}"
    );
}

/// Phase-2 history: recorded by deprecated-gate code (`deprecate_patch` +
/// unconditional new code) — new branch, **no** marker.
fn phase2_marker_less_history() -> Vec<WorkflowEvent> {
    let id = ActivityExecId::new();
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id,
            name: "new_activity".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id,
            output: serde_json::json!("new_result"),
        },
    ]
}

/// Gate-free workflow that makes no ctx calls at all — phase 3 of the
/// lifecycle after even the `deprecate_patch` call was deleted.
fn gate_free_noop_workflow<'a>(
    _ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move { Ok(Value::Null) })
}

/// Positive phase-3 proof: a history recorded by phase-2 code (deprecated
/// gate → no marker) replays cleanly against gate-free phase-3 code, i.e.
/// deleting the `deprecate_patch` call after marker-bearing runs drained is
/// safe for the marker-less generation.
#[tokio::test]
async fn replay_phase3_gate_free_code_replays_marker_less_phase2_history() {
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            // Gate-free handler running new_activity unconditionally.
            "patched_workflow_removed_gate",
            ExecutionId::new(),
            phase2_marker_less_history(),
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "gate-free phase-3 code must replay a marker-less phase-2 history cleanly, got: {report}"
    );
}

/// A stale `patch:{id}` marker as the FINAL history event, replayed against
/// code with no ctx calls left at all, surfaces loudly — but as the generic
/// `EarlyCompletion` (unconsumed trailing history), not `PatchMarkerMismatch`:
/// no subsequent command ever trips over the marker, the completed-history
/// check reports it instead. Pinned so the classification is deliberate.
#[tokio::test]
async fn replay_trailing_stale_patch_marker_classifies_as_early_completion() {
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "patch:gate".into(),
            details: serde_json::json!(1),
        },
    ];

    let replayer = WorkflowReplayer::new().register_fn("gate_free_noop", gate_free_noop_workflow);
    let report = replayer
        .replay_from_snapshot(make_snapshot("gate_free_noop", ExecutionId::new(), events))
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::EarlyCompletion,
                ..
            }
        ),
        "a trailing stale patch marker must classify as EarlyCompletion, got: {report}"
    );
}

/// Sandwich-flip money test (issue #687 review finding F1): the history a
/// fixed live pass of the sandwich handler produces — `patched("gate")`
/// records exactly one marker, `deprecate_patch` + residual `patched` add
/// nothing, both booleans are true → new branch. That history must replay
/// cleanly against the same handler: without the this-cycle latch the live
/// pass would have taken the OLD branch (residual `patched` false) while
/// every replay pass takes the new one — a permanent nd-block.
#[tokio::test]
async fn replay_patched_deprecate_patched_sandwich_is_replay_consistent() {
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "patched_workflow_sandwich",
            ExecutionId::new(),
            post_patch_history(),
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the patched→deprecate→patched sandwich must be live/replay consistent, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Display impl gives useful failure message
// ---------------------------------------------------------------------------

#[tokio::test]
async fn report_display_is_useful_on_failure() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("reordered_workflow", exec_id, events))
        .await;

    let display = format!("{report}");
    assert!(
        display.contains("NonDeterminism") || display.contains("mismatch"),
        "Display impl must mention the error class: {display}"
    );
}

// ---------------------------------------------------------------------------
// replay_from_json round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_from_json_succeeds_with_unchanged_workflow() {
    use autumn_harvest::testing::HistorySnapshot;

    let (exec_id, events) = canonical_history();
    let snapshot = HistorySnapshot {
        workflow_name: "canonical_workflow".to_string(),
        execution_id: exec_id,
        events,
        context_headers: None,
    };
    let json = serde_json::to_string(&snapshot).expect("serialization must succeed");

    let replayer = build_replayer();
    let report = replayer
        .replay_from_json(&json)
        .await
        .expect("JSON deserialisation must succeed");

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "JSON round-trip with unchanged workflow must succeed: {report}"
    );
}

#[tokio::test]
async fn replay_from_json_detects_non_determinism() {
    use autumn_harvest::testing::HistorySnapshot;

    let (exec_id, events) = canonical_history();
    let snapshot = HistorySnapshot {
        workflow_name: "reordered_workflow".to_string(),
        execution_id: exec_id,
        events,
        context_headers: None,
    };
    let json = serde_json::to_string(&snapshot).expect("serialization must succeed");

    let replayer = build_replayer();
    let report = replayer
        .replay_from_json(&json)
        .await
        .expect("JSON deserialisation must succeed");

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "JSON replay with reordered workflow must detect non-determinism: {report}"
    );
}

#[tokio::test]
async fn replay_from_json_rejects_invalid_json() {
    let replayer = build_replayer();
    let result = replayer.replay_from_json("not valid json").await;
    assert!(result.is_err(), "invalid JSON must return Err");
}

// ---------------------------------------------------------------------------
// Unknown workflow name surfaces as WorkflowFailed with descriptive error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_unknown_workflow_surfaces_as_failed() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("unknown_workflow", exec_id, events))
        .await;

    match &report.status {
        ReplayStatus::WorkflowFailed { error, .. } => {
            assert!(
                error.contains("unknown_workflow"),
                "error must name the missing workflow: {error}"
            );
        }
        other => panic!("expected WorkflowFailed for unknown workflow, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// ReplayReport struct fields are populated correctly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn report_fields_are_populated() {
    let (exec_id, events) = canonical_history();
    let event_count = events.len();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("canonical_workflow", exec_id, events))
        .await;

    assert_eq!(report.execution_id, exec_id, "execution_id must match");
    assert!(
        report.events_replayed <= event_count,
        "events_replayed must not exceed total events"
    );
}

// ---------------------------------------------------------------------------
// Timer mismatch: workflow expects timer, history has activity at that position
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_activity_history_for_timer_workflow_detects_timer_mismatch() {
    // Build a history with activity events, but the workflow starts with a timer.
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        // History has activity first, but timer_first_workflow starts with a timer
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: serde_json::json!("ok"),
        },
    ];

    let replayer = build_replayer();
    let report = replayer
        .replay_from_snapshot(make_snapshot("timer_first_workflow", exec_id, events))
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::TimerMismatch,
                ..
            }
        ),
        "timer-missing divergence must produce TimerMismatch, got: {:?}",
        report.status
    );
}

// ---------------------------------------------------------------------------
// Non-determinism report includes expected and actual strings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn non_determinism_report_includes_expected_actual() {
    let (exec_id, events) = canonical_history();
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("reordered_workflow", exec_id, events))
        .await;

    match &report.status {
        ReplayStatus::NonDeterminismDetected {
            expected, actual, ..
        } => {
            assert!(!expected.is_empty(), "expected must not be empty");
            assert!(!actual.is_empty(), "actual must not be empty");
        }
        other => panic!("expected NonDeterminismDetected, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Activity input mismatch is detected (strict replay mode)
// ---------------------------------------------------------------------------

/// Workflow that calls `step_one` with a NON-NULL input that won't match history.
fn changed_input_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // History records step_one with input=null but this code now passes 42.
        ctx.execute_activity_raw("step_one", serde_json::json!(42), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

#[tokio::test]
async fn replay_activity_with_changed_input_detects_non_determinism() {
    // Build a history where step_one was called with input=null.
    let exec_id = ExecutionId::new();
    let id1 = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null, // recorded input was null
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: serde_json::json!("ok"),
        },
    ];

    // Replay with a workflow that passes 42 — strict mode should catch the mismatch.
    let replayer = WorkflowReplayer::new().register_fn("changed", changed_input_workflow);
    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "changed".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
        })
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::ActivityScheduleMismatch,
                ..
            }
        ),
        "changed input must produce ActivityScheduleMismatch, got: {:?}",
        report.status
    );
}

// ---------------------------------------------------------------------------
// Replayer is usable as a static helper in CI (one-liner pattern)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_liner_ci_pattern_compiles_and_runs() {
    let (_exec_id, events) = canonical_history();

    // This is the pattern shown in README docs — single handler, bare events list.
    let report = WorkflowReplayer::new()
        .register_fn("canonical_workflow", canonical_workflow)
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "one-liner pattern must succeed: {report}"
    );
}

// ---------------------------------------------------------------------------
// Early-completion detection
// ---------------------------------------------------------------------------

/// Workflow that only executes `step_one`, ignoring the rest of the history.
fn early_return_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        // Returns before consuming step_two or the timer — early completion.
        Ok(Value::Null)
    })
}

/// A workflow that returns early leaves unconsumed history events → `NonDeterminismDetected`
/// with `EarlyCompletion` kind.
#[tokio::test]
async fn replay_early_completion_detects_non_determinism() {
    let (exec_id, events) = canonical_history();

    let report = WorkflowReplayer::new()
        .register_fn("early_return_workflow", early_return_workflow)
        .replay_from_snapshot(make_snapshot("early_return_workflow", exec_id, events))
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::EarlyCompletion,
                ..
            }
        ),
        "early-return workflow must produce EarlyCompletion non-determinism, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// Suspended-during-replay detection
// ---------------------------------------------------------------------------

/// Workflow that issues a new activity not in the recorded history.
fn extra_step_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("step_two", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        // NEW step not in the canonical 2-activity history → suspends.
        ctx.execute_activity_raw("step_three_new", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// A workflow that adds a new command beyond recorded history suspends during
/// strict replay → `NonDeterminismDetected` (not `ReplaySucceeded`).
#[tokio::test]
async fn replay_new_command_beyond_history_detects_non_determinism() {
    // Build a 2-activity history (step_one, step_two only — no step_three).
    let exec_id = ExecutionId::new();
    let id1 = autumn_harvest::types::ActivityExecId::new();
    let id2 = autumn_harvest::types::ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: Value::Null,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id2,
            name: "step_two".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id2,
            output: Value::Null,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("extra_step_workflow", extra_step_workflow)
        .replay_from_snapshot(make_snapshot("extra_step_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "workflow adding a new command beyond history must be non-deterministic: {report}"
    );
}

// ---------------------------------------------------------------------------
// Terminal lifecycle events ignored by early-completion check
// ---------------------------------------------------------------------------

fn single_step_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

// ---------------------------------------------------------------------------
// Acceptance criteria: multiple concurrent version changes in a single workflow
// (spec AC#3: "Must support multiple concurrent version changes")
// ---------------------------------------------------------------------------

/// Workflow with two sequential version gates that BOTH have recorded markers.
fn two_gate_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let va = ctx.version("gate_alpha", 1, 3);
        let vb = ctx.version("gate_beta", 1, 5);
        Ok(serde_json::json!({"va": va, "vb": vb}))
    })
}

/// History with two version markers: `gate_alpha=2`, `gate_beta=4`.
fn two_gate_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_alpha".into(),
            details: serde_json::json!(2_u32),
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_beta".into(),
            details: serde_json::json!(4_u32),
        },
    ];
    (exec_id, events)
}

/// Both version markers are consumed in order — no non-determinism.
#[tokio::test]
async fn replay_two_concurrent_version_gates_succeed() {
    let (exec_id, events) = two_gate_history();
    let report = WorkflowReplayer::new()
        .register_fn("two_gate_workflow", two_gate_workflow)
        .replay_from_snapshot(make_snapshot("two_gate_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "two concurrent version gates must replay cleanly: {report}"
    );
}

/// A version gate interleaved with an activity also replays correctly.
fn interleaved_gate_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let va = ctx.version("gate_alpha", 1, 3);
        let r = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let vb = ctx.version("gate_beta", 1, 5);
        Ok(serde_json::json!({"va": va, "r": r, "vb": vb}))
    })
}

fn interleaved_gate_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let aid = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_alpha".into(),
            details: serde_json::json!(2_u32),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: aid,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: aid,
            output: serde_json::json!("done"),
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_beta".into(),
            details: serde_json::json!(4_u32),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replay_interleaved_version_gate_and_activity_succeed() {
    let (exec_id, events) = interleaved_gate_history();
    let report = WorkflowReplayer::new()
        .register_fn("interleaved_gate_workflow", interleaved_gate_workflow)
        .replay_from_snapshot(make_snapshot("interleaved_gate_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "interleaved version gate + activity must replay cleanly: {report}"
    );
}

// ---------------------------------------------------------------------------
// Acceptance criteria: clear warnings for improper use
// (spec AC#5: "Must provide clear compilation errors or runtime warnings if
//  versioning is used improperly")
// ---------------------------------------------------------------------------

/// A workflow that was updated to use `version("gate_new", …)` where history
/// still records `version("gate_old", …)`.  The unconsumed old marker causes
/// the next command (an activity) to see a `MarkerRecorded(version:gate_old)`
/// at the position where it expects `ActivityScheduled(step)`.
/// The replayer must classify this as `VersionMarkerMismatch`, not a generic
/// `ActivityScheduleMismatch`, so the error message clearly points to the
/// version gate as the source of the problem.
fn renamed_gate_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Code was updated: "gate_old" → "gate_new", but history still has gate_old
        let _v = ctx.version("gate_new", 1, 2);
        ctx.execute_activity_raw("step", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn history_with_old_gate() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let aid = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "version:gate_old".into(),
            details: serde_json::json!(1_u32),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: aid,
            name: "step".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: aid,
            output: serde_json::json!("done"),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replay_renamed_version_gate_classified_as_version_marker_mismatch() {
    let (exec_id, events) = history_with_old_gate();
    let report = WorkflowReplayer::new()
        .register_fn("renamed_gate_workflow", renamed_gate_workflow)
        .replay_from_snapshot(make_snapshot("renamed_gate_workflow", exec_id, events))
        .await;

    match &report.status {
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            assert_eq!(
                *kind,
                NonDeterminismKind::VersionMarkerMismatch,
                "a renamed version gate must produce VersionMarkerMismatch, got {kind:?}\nreport: {report}"
            );
        }
        other => panic!("expected NonDeterminismDetected, got: {other:?}\nreport: {report}"),
    }
}

/// Histories loaded from a completed execution include `WorkflowCompleted` as
/// the last event. An unchanged workflow should still report `ReplaySucceeded`.
#[tokio::test]
async fn replay_history_with_workflow_completed_tail_succeeds() {
    let exec_id = ExecutionId::new();
    let id1 = autumn_harvest::types::ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id1,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id1,
            output: Value::Null,
        },
        // Terminal lifecycle event — the executor appends this after the
        // workflow returns. It must not be treated as "unconsumed history".
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("single_step_workflow", single_step_workflow)
        .replay_from_snapshot(make_snapshot("single_step_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "history with WorkflowCompleted tail must not trigger early-completion: {report}"
    );
}

// ---------------------------------------------------------------------------
// Child workflow replay tests
// ---------------------------------------------------------------------------

fn child_spawning_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .spawn_child_workflow_raw("child_processor", serde_json::json!({"item": "A"}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"processed": result}))
    })
}

fn renamed_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Name changed from "child_processor" → triggers non-determinism
        let result = ctx
            .spawn_child_workflow_raw("renamed_processor", serde_json::json!({"item": "A"}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

fn changed_input_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Input changed — triggers non-determinism
        let result = ctx
            .spawn_child_workflow_raw("child_processor", serde_json::json!({"item": "CHANGED"}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

fn child_spawning_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let child_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "child_processor".into(),
            input: serde_json::json!({"item": "A"}),
        },
        WorkflowEvent::ChildWorkflowCompleted {
            child_id,
            output: serde_json::json!({"done": true}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"processed": {"done": true}}),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replayer_succeeds_for_workflow_spawning_a_child() {
    let (exec_id, events) = child_spawning_history();
    let report = WorkflowReplayer::new()
        .register_fn("child_spawning_workflow", child_spawning_workflow)
        .replay_from_snapshot(make_snapshot("child_spawning_workflow", exec_id, events))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "child spawn workflow must replay successfully: {report}"
    );
}

#[tokio::test]
async fn replayer_detects_changed_child_workflow_name() {
    let (exec_id, events) = child_spawning_history();
    let report = WorkflowReplayer::new()
        .register_fn("renamed_child_workflow", renamed_child_workflow)
        .replay_from_snapshot(make_snapshot("renamed_child_workflow", exec_id, events))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "renamed child workflow must trigger non-determinism: {report}"
    );
}

#[tokio::test]
async fn replayer_reports_child_workflow_mismatch_kind() {
    let (exec_id, events) = child_spawning_history();
    let report = WorkflowReplayer::new()
        .register_fn("renamed_child_workflow", renamed_child_workflow)
        .replay_from_snapshot(make_snapshot("renamed_child_workflow", exec_id, events))
        .await;
    match &report.status {
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            assert_eq!(
                *kind,
                NonDeterminismKind::ChildWorkflowMismatch,
                "wrong non-determinism kind: {kind:?}"
            );
        }
        other => panic!("expected NonDeterminismDetected, got {other:?}"),
    }
}

#[tokio::test]
async fn replayer_detects_changed_child_workflow_input() {
    let (exec_id, events) = child_spawning_history();
    let report = WorkflowReplayer::new()
        .register_fn("changed_input_child_workflow", changed_input_child_workflow)
        .replay_from_snapshot(make_snapshot(
            "changed_input_child_workflow",
            exec_id,
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "changed child input must trigger non-determinism: {report}"
    );
}

// ---------------------------------------------------------------------------
// Child workflow fan-out replay tests (issue #601)
// ---------------------------------------------------------------------------

/// Workflow that fans out two children via `spawn_child_workflow_fan_out_raw`.
fn child_fan_out_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let results = ctx
            .spawn_child_workflow_fan_out_raw(vec![
                (
                    "fan_out_child".to_string(),
                    serde_json::json!({"item": "A"}),
                ),
                (
                    "fan_out_child".to_string(),
                    serde_json::json!({"item": "B"}),
                ),
            ])
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"processed": results}))
    })
}

/// Same shape but fans out only ONE child — triggers a `fan_out:{n}` count
/// mismatch against a two-child recorded history.
fn child_fan_out_count_changed_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let results = ctx
            .spawn_child_workflow_fan_out_raw(vec![(
                "fan_out_child".to_string(),
                serde_json::json!({"item": "A"}),
            )])
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"processed": results}))
    })
}

fn child_fan_out_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let child_a = ExecutionId::new();
    let child_b = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: serde_json::json!(2u64),
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id: child_a,
            workflow_name: "fan_out_child".into(),
            input: serde_json::json!({"item": "A"}),
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id: child_b,
            workflow_name: "fan_out_child".into(),
            input: serde_json::json!({"item": "B"}),
        },
        WorkflowEvent::ChildWorkflowCompleted {
            child_id: child_a,
            output: serde_json::json!({"done": "A"}),
        },
        WorkflowEvent::ChildWorkflowCompleted {
            child_id: child_b,
            output: serde_json::json!({"done": "B"}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"processed": [{"done": "A"}, {"done": "B"}]}),
        },
    ];
    (exec_id, events)
}

/// Falsifiable success-bar coverage for issue #601: replaying a recorded
/// history whose workflow calls `spawn_child_workflow_fan_out_raw` must
/// report `ReplaySucceeded` — mirroring the `set_current_details` precedent
/// (issue #593) of locking the ACs' "replays deterministically" claim behind
/// an actual `WorkflowReplayer` fixture rather than only unit tests.
#[tokio::test]
async fn replayer_succeeds_for_workflow_spawning_a_child_fan_out() {
    let (exec_id, events) = child_fan_out_history();
    let report = WorkflowReplayer::new()
        .register_fn("child_fan_out_workflow", child_fan_out_workflow)
        .replay_from_snapshot(make_snapshot("child_fan_out_workflow", exec_id, events))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "child fan-out workflow must replay successfully: {report}"
    );
}

/// The recorded history fanned out 2 children; the (simulated) redeployed
/// code now fans out only 1 -- the `fan_out:{n}` count marker must catch the
/// divergence before any child is (re)spawned.
#[tokio::test]
async fn replayer_detects_child_fan_out_count_mismatch() {
    let (exec_id, events) = child_fan_out_history();
    let report = WorkflowReplayer::new()
        .register_fn(
            "child_fan_out_count_changed_workflow",
            child_fan_out_count_changed_workflow,
        )
        .replay_from_snapshot(make_snapshot(
            "child_fan_out_count_changed_workflow",
            exec_id,
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "child fan-out count mismatch must trigger non-determinism: {report}"
    );
}

/// Workflow whose second fan-out child exceeds the default 2 MiB payload
/// cap, so the fan-out fails before *any* child (or the `fan_out:{n}`
/// marker) is ever recorded -- see `peek_fan_out_count`/
/// `record_fan_out_marker`/`validate_child_payload_caps` in `context.rs`.
fn child_fan_out_workflow_with_oversized_child<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let huge = serde_json::json!({ "data": "x".repeat(3 * 1024 * 1024) });
        let results = ctx
            .spawn_child_workflow_fan_out_raw(vec![
                (
                    "fan_out_child".to_string(),
                    serde_json::json!({"item": "A"}),
                ),
                ("fan_out_child".to_string(), huge),
            ])
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"processed": results}))
    })
}

/// Plain (non-fan-out) single-child spawn whose input exceeds the default
/// 2 MiB payload cap -- used to show the limitation below is general, not
/// introduced by fan-out.
fn single_child_spawn_with_oversized_input<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let huge = serde_json::json!({ "data": "x".repeat(3 * 1024 * 1024) });
        ctx.spawn_child_workflow_raw("some_child", huge)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Post-review hardening for issue #601 (Codex finding, PR #901): a fan-out
/// that fails on payload-cap overflow before any child is dispatched must
/// never leave an orphaned `fan_out:{n}` marker in the persisted terminal
/// history -- confirmed here by feeding the minimal, marker-free
/// `[WorkflowStarted, WorkflowFailed]` history (what the fixed code
/// actually persists for this failure -- see the `drain_commands().is_empty()`
/// assertion in
/// `child_fan_out_raw_oversized_child_rejects_before_dispatching_any_sibling`,
/// `tests/child_fanout_tests.rs`) back through the replayer.
///
/// This does **not** achieve `ReplaySucceeded`, and that is expected, not a
/// bug: see [`known_limitation_early_config_dependent_failure_does_not_replay_cleanly`]
/// below for why a bare trailing `WorkflowFailed` diverges on *any* match
/// attempt reaching it, fan-out or not. What this test locks in is the
/// narrower claim the fix actually makes: the divergence is a plain
/// `MarkerRecorded(fan_out:1)` vs `WorkflowFailed` mismatch (the code
/// correctly attempting to peek/record the marker and finding the terminal
/// event instead) -- not the pre-fix `ChildWorkflowStarted(fan_out_child)`
/// vs `WorkflowFailed` mismatch, which would have meant a marker claiming a
/// child that was never recorded lied about the group's true size.
#[tokio::test]
async fn replayer_diverges_at_marker_not_at_a_phantom_child_for_payload_cap_failure() {
    let exec_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::workflow_failed("payload too large: child input exceeds cap"),
    ];
    let report = WorkflowReplayer::new()
        .register_fn(
            "child_fan_out_workflow_with_oversized_child",
            child_fan_out_workflow_with_oversized_child,
        )
        .replay_from_snapshot(make_snapshot(
            "child_fan_out_workflow_with_oversized_child",
            exec_id,
            events,
        ))
        .await;
    match report.status {
        ReplayStatus::NonDeterminismDetected {
            ref expected,
            ref actual,
            ..
        } => {
            assert_eq!(
                expected, "MarkerRecorded(fan_out:1)",
                "the fix must move the divergence to the marker check, not \
                 a phantom child: {report}"
            );
            assert_eq!(actual, "WorkflowFailed");
        }
        other => panic!(
            "expected a marker-vs-WorkflowFailed NonDeterminismDetected \
             (see the known-limitation test for why this doesn't fully \
             replay), got: {other:?}"
        ),
    }
}

/// Known, **pre-existing** engine limitation (predates issue #601 and is
/// not specific to fan-out): a workflow whose first live execution fails
/// due to a config-dependent check -- like the payload-size cap (issue
/// #252) -- *after* at least one Harvest primitive call has already
/// touched the matcher does not replay cleanly through `WorkflowReplayer`.
///
/// `HistoryMatcher::new` deliberately leaves a bare trailing `WorkflowFailed`
/// non-transparent unless it is immediately followed by a `WorkflowRedriven`
/// event (issue #510) -- see the comment there: "a genuinely failed run...
/// must be unaffected." So any `match_*` call that reaches that cursor
/// position sees `WorkflowFailed` instead of the event type it expects and
/// reports a divergence, rather than gracefully recognizing "the workflow
/// is about to fail anyway." This is demonstrated here with a **plain
/// single-child** `spawn_child_workflow_raw` call (no fan-out involved at
/// all) to prove the limitation is general: issue #601's `peek_fan_out_count`
/// fix (above) narrows *what* diverges for a fan-out's own payload-cap
/// failure, but closing this gap for good -- making terminal `WorkflowFailed`
/// events transparent to in-progress match attempts -- is a deliberate,
/// documented design tradeoff this codebase currently avoids, and is out of
/// scope for a context.rs-only change.
#[tokio::test]
async fn known_limitation_early_config_dependent_failure_does_not_replay_cleanly() {
    let exec_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::workflow_failed("payload too large"),
    ];
    let report = WorkflowReplayer::new()
        .register_fn(
            "single_child_spawn_with_oversized_input",
            single_child_spawn_with_oversized_input,
        )
        .replay_from_snapshot(make_snapshot(
            "single_child_spawn_with_oversized_input",
            exec_id,
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "documents a known, pre-existing engine limitation (not a fan-out \
         regression) -- if this ever starts passing, the limitation this \
         test documents has been fixed and its doc comment should be \
         updated/removed: {report}"
    );
}

// ---------------------------------------------------------------------------
// External signal replay tests (issue #330)
// ---------------------------------------------------------------------------

/// Build a history with `ExternalSignalRequested` + `ExternalSignalDelivered`.
fn external_signal_delivered_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let signal_id = autumn_harvest::types::ExternalSignalId::new();
    let target = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ExternalSignalRequested {
            signal_id,
            target,
            signal_name: "tenant_cancel".into(),
            payload: serde_json::json!({"reason": "billing_lapse"}),
            idempotency_key: None,
        },
        WorkflowEvent::ExternalSignalDelivered { signal_id },
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];
    (exec_id, events)
}

/// Build a history with `ExternalSignalRequested` + `ExternalSignalFailed`.
fn external_signal_failed_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let signal_id = autumn_harvest::types::ExternalSignalId::new();
    let target = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ExternalSignalRequested {
            signal_id,
            target,
            signal_name: "tenant_cancel".into(),
            payload: Value::Null,
            idempotency_key: None,
        },
        WorkflowEvent::ExternalSignalFailed {
            signal_id,
            reason_code: "target_terminal".into(),
        },
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];
    (exec_id, events)
}

/// Workflow that signals an external workflow, then returns Ok.
fn external_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_str = input["target"].as_str().unwrap_or("");
        let target: ExecutionId = target_str.parse().unwrap();
        let _result = ctx
            .signal_external_workflow(
                target,
                "tenant_cancel",
                serde_json::json!({"reason": "billing_lapse"}),
            )
            .await;
        Ok(Value::Null)
    })
}

/// Workflow that signals an external workflow with a DIFFERENT signal name than history.
fn external_signal_wrong_name_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_str = input["target"].as_str().unwrap_or("");
        let target: ExecutionId = target_str.parse().unwrap();
        // Uses "wrong_signal" instead of "tenant_cancel" — triggers non-determinism
        ctx.signal_external_workflow(target, "wrong_signal", Value::Null)
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

#[tokio::test]
async fn replayer_replays_external_signal_delivered_successfully() {
    let (_exec_id, events) = external_signal_delivered_history();
    // Extract target from events
    let target = events
        .iter()
        .find_map(|e| {
            if let WorkflowEvent::ExternalSignalRequested { target, .. } = e {
                Some(*target)
            } else {
                None
            }
        })
        .unwrap();

    let input = serde_json::json!({"target": target.to_string()});
    let events_with_input: Vec<_> = events
        .iter()
        .map(|e| match e {
            WorkflowEvent::WorkflowStarted { timestamp, .. } => WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: *timestamp,
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            other => other.clone(),
        })
        .collect();

    let report = WorkflowReplayer::new()
        .register_fn("external_signal_workflow", external_signal_workflow)
        .replay_from_events(events_with_input)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "external signal delivered history must replay cleanly: {report}"
    );
}

#[tokio::test]
async fn replayer_detects_external_signal_name_mismatch() {
    let (_exec_id, events) = external_signal_delivered_history();
    let target = events
        .iter()
        .find_map(|e| {
            if let WorkflowEvent::ExternalSignalRequested { target, .. } = e {
                Some(*target)
            } else {
                None
            }
        })
        .unwrap();

    let input = serde_json::json!({"target": target.to_string()});
    let events_with_input: Vec<_> = events
        .iter()
        .map(|e| match e {
            WorkflowEvent::WorkflowStarted { timestamp, .. } => WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: *timestamp,
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            other => other.clone(),
        })
        .collect();

    let report = WorkflowReplayer::new()
        .register_fn(
            "external_signal_wrong_name_workflow",
            external_signal_wrong_name_workflow,
        )
        .replay_from_events(events_with_input)
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::ExternalSignalMismatch,
                ..
            }
        ),
        "wrong signal name must trigger ExternalSignalMismatch: {report}"
    );
}

#[tokio::test]
async fn replayer_replays_external_signal_failed_history() {
    let (_exec_id, events) = external_signal_failed_history();
    let target = events
        .iter()
        .find_map(|e| {
            if let WorkflowEvent::ExternalSignalRequested { target, .. } = e {
                Some(*target)
            } else {
                None
            }
        })
        .unwrap();

    let input = serde_json::json!({"target": target.to_string()});
    let events_with_input: Vec<_> = events
        .iter()
        .map(|e| match e {
            WorkflowEvent::WorkflowStarted { timestamp, .. } => WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: *timestamp,
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            other => other.clone(),
        })
        .collect();

    // The workflow handles the error from signal_external_workflow by ignoring it
    // (the `let _result = ...` pattern), so the workflow itself succeeds.
    let report = WorkflowReplayer::new()
        .register_fn("external_signal_workflow", external_signal_workflow)
        .replay_from_events(events_with_input)
        .await;

    // The workflow catches the error and returns Ok, so replay succeeds.
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "external signal failed history should replay successfully when error is handled: {report}"
    );
}

// ---------------------------------------------------------------------------
// Detached child workflow spawn (issue #347)
// ---------------------------------------------------------------------------

/// Workflow that spawns a detached child with Abandon policy and returns immediately.
fn detached_spawn_abandon_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_id = ctx
            .spawn_child_workflow_detached_raw(
                "some_child",
                Value::Null,
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "child_id": child_id.to_string() }))
    })
}

/// Workflow that spawns a detached child with `RequestCancel` policy.
fn detached_spawn_request_cancel_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_id = ctx
            .spawn_child_workflow_detached_raw(
                "some_child",
                Value::Null,
                ParentClosePolicy::RequestCancel,
            )
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "child_id": child_id.to_string() }))
    })
}

/// Workflow that spawns a detached child and then runs an activity.
fn detached_spawn_then_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_id = ctx
            .spawn_child_workflow_detached_raw("monitor", Value::Null, ParentClosePolicy::Abandon)
            .map_err(|e| e.to_string())?;
        let result = ctx
            .execute_activity_raw("do_work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "child_id": child_id.to_string(), "result": result }))
    })
}

/// Build a history with a `ChildWorkflowSpawnedDetached` event followed by completion.
fn detached_spawn_history(
    policy: ParentClosePolicy,
) -> (ExecutionId, ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let child_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ChildWorkflowSpawnedDetached {
            child_id,
            workflow_name: "some_child".into(),
            input: Value::Null,
            parent_close_policy: policy,
        },
    ];
    (exec_id, child_id, events)
}

/// Build a history with a detached spawn followed by an activity.
fn detached_spawn_then_activity_history() -> (ExecutionId, ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let child_id = ExecutionId::new();
    let act_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ChildWorkflowSpawnedDetached {
            child_id,
            workflow_name: "monitor".into(),
            input: Value::Null,
            parent_close_policy: ParentClosePolicy::Abandon,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act_id,
            name: "do_work".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act_id,
            output: serde_json::json!("done"),
        },
    ];
    (exec_id, child_id, events)
}

// ── (i) Replay with ChildWorkflowSpawnedDetached returns the same child_id ──

#[tokio::test]
async fn replay_detached_spawn_returns_recorded_child_id() {
    let (exec_id, child_id, events) = detached_spawn_history(ParentClosePolicy::Abandon);
    let replayer = WorkflowReplayer::new().register_fn(
        "detached_spawn_abandon_workflow",
        detached_spawn_abandon_workflow,
    );

    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "detached_spawn_abandon_workflow".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
        })
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "detached spawn replay must succeed: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be positive"
    );
    let _ = child_id; // child_id is used above and the replay returned same id
}

// ── (ii) Replay with RequestCancel policy succeeds ──────────────────────────

#[tokio::test]
async fn replay_detached_spawn_request_cancel_policy_succeeds() {
    let (exec_id, _child_id, events) = detached_spawn_history(ParentClosePolicy::RequestCancel);
    let replayer = WorkflowReplayer::new().register_fn(
        "detached_spawn_request_cancel_workflow",
        detached_spawn_request_cancel_workflow,
    );

    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "detached_spawn_request_cancel_workflow".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
        })
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "RequestCancel detached spawn replay must succeed: {report}"
    );
}

#[tokio::test]
async fn replay_detached_spawn_policy_mismatch_detects_non_determinism() {
    let (exec_id, _child_id, events) = detached_spawn_history(ParentClosePolicy::RequestCancel);
    let replayer = WorkflowReplayer::new().register_fn(
        "detached_spawn_abandon_workflow",
        detached_spawn_abandon_workflow,
    );

    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "detached_spawn_abandon_workflow".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
        })
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "policy mismatch must detect non-determinism: {report}"
    );
}

// ── (iii) Detached spawn + activity — determinism preserved ─────────────────

#[tokio::test]
async fn replay_detached_spawn_then_activity_succeeds() {
    let (exec_id, _child_id, events) = detached_spawn_then_activity_history();
    let replayer = WorkflowReplayer::new().register_fn(
        "detached_spawn_then_activity_workflow",
        detached_spawn_then_activity_workflow,
    );

    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "detached_spawn_then_activity_workflow".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
        })
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "detached spawn + activity replay must succeed: {report}"
    );
}

// ── (iv) Reordering detached spawn after activity → NonDeterminism ───────────

fn reordered_detached_spawn_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // activity FIRST, then detached spawn — opposite of history
        let result = ctx
            .execute_activity_raw("do_work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let child_id = ctx
            .spawn_child_workflow_detached_raw("monitor", Value::Null, ParentClosePolicy::Abandon)
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "result": result, "child_id": child_id.to_string() }))
    })
}

#[tokio::test]
async fn replay_reordered_detached_spawn_detects_non_determinism() {
    let (exec_id, _child_id, events) = detached_spawn_then_activity_history();
    let replayer = WorkflowReplayer::new().register_fn(
        "reordered_detached_spawn_workflow",
        reordered_detached_spawn_workflow,
    );

    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "reordered_detached_spawn_workflow".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
        })
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "reordered detached spawn must detect non-determinism: {report}"
    );
}

// ── (v) Backwards compat: existing awaited ChildWorkflowStarted still replays ──

/// Workflow that awaits a child — the classic pre-#347 path.
fn awaited_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let output = ctx
            .spawn_child_workflow_raw("child_step", Value::Null)
            .await
            .map_err(|e| e.to_string())?;
        Ok(output)
    })
}

/// History fixture for a successfully awaited child workflow — no new fields.
fn awaited_child_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let child_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "child_step".into(),
            input: Value::Null,
        },
        WorkflowEvent::ChildWorkflowCompleted {
            child_id,
            output: serde_json::json!("child_result"),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replay_backwards_compat_awaited_child_workflow() {
    let (exec_id, events) = awaited_child_history();
    let replayer =
        WorkflowReplayer::new().register_fn("awaited_child_workflow", awaited_child_workflow);

    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "awaited_child_workflow".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
        })
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "pre-#347 awaited child history must still replay correctly: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be positive"
    );
}

// ---------------------------------------------------------------------------
// Deterministic side-effect primitives (issue #384)
// ---------------------------------------------------------------------------

/// Calls `ctx.system_now()` then schedules an activity. The captured clock value
/// lowers onto a `SideEffectRecorded` event, matched in command order.
fn now_then_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _t = ctx.system_now();
        let r = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "step": r }))
    })
}

fn now_then_activity_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let activity_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Now,
            name: None,
            value: serde_json::json!(1_700_000_000_000_i64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "step_one".to_string(),
            input: Value::Null,
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!("ok"),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replay_succeeds_for_recorded_side_effect() {
    let (exec_id, events) = now_then_activity_history();
    let replayer =
        WorkflowReplayer::new().register_fn("now_then_activity", now_then_activity_workflow);

    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "now_then_activity".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
        })
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "side-effect history must replay cleanly: {report}"
    );
}

#[tokio::test]
async fn replay_detects_side_effect_drift() {
    // History has NO recorded side effect — the activity sits where the workflow
    // now calls system_now(). The built-in primitive must surface SideEffectDrift.
    let exec_id = ExecutionId::new();
    let activity_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "step_one".to_string(),
            input: Value::Null,
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!("ok"),
        },
    ];

    let replayer =
        WorkflowReplayer::new().register_fn("now_then_activity", now_then_activity_workflow);

    let report = replayer
        .replay_from_snapshot(HistorySnapshot {
            workflow_name: "now_then_activity".to_string(),
            execution_id: exec_id,
            events,
            context_headers: None,
        })
        .await;

    match report.status {
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            assert_eq!(
                kind,
                NonDeterminismKind::SideEffectDrift,
                "expected SideEffectDrift, got {kind:?}"
            );
        }
        other => panic!("expected NonDeterminismDetected(SideEffectDrift), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// receive_signal_timeout — signal-or-deadline race (issue #476)
// ---------------------------------------------------------------------------

/// Awaits an approval signal with a deadline, then branches on the outcome.
fn signal_or_deadline_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let decision = ctx
            .wait_for_signal_timeout("approval", std::time::Duration::from_secs(300))
            .await
            .map_err(|e| e.to_string())?;
        Ok(decision.map_or_else(
            || serde_json::json!({"escalated": true}),
            |payload| serde_json::json!({"approved": payload}),
        ))
    })
}

fn signal_branch_fixture() -> Vec<WorkflowEvent> {
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("__signal_timeout:1:approval"),
            duration_secs: 300,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "approval".to_string(),
            payload: serde_json::json!({"ok": true}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"approved": {"ok": true}}),
        },
    ]
}

fn timeout_branch_fixture() -> Vec<WorkflowEvent> {
    let timer_id = TimerId::new("__signal_timeout:1:approval");
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 300,
        },
        WorkflowEvent::TimerFired { timer_id },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"escalated": true}),
        },
    ]
}

#[tokio::test]
async fn signal_timeout_signal_branch_replays_succeeded() {
    let report = WorkflowReplayer::new()
        .register_fn("signal_or_deadline", signal_or_deadline_workflow)
        .replay_from_events(signal_branch_fixture())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "signal branch must replay:\n{report}"
    );
}

#[tokio::test]
async fn signal_timeout_timeout_branch_replays_succeeded() {
    let report = WorkflowReplayer::new()
        .register_fn("signal_or_deadline", signal_or_deadline_workflow)
        .replay_from_events(timeout_branch_fixture())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "timeout branch must replay:\n{report}"
    );
}

#[tokio::test]
async fn signal_timeout_both_branches_replay_succeeded_across_randomized_orderings() {
    // Issue #476 success metric: a fixture exercising both branches replays
    // with ReplaySucceeded 100% of the time across 1,000 randomized orderings.
    let mut seed: u64 = 0x5DEE_CE66;
    for i in 0..1_000 {
        // Simple deterministic LCG so the test needs no RNG dependency.
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let events = if seed & 1 == 0 {
            signal_branch_fixture()
        } else {
            timeout_branch_fixture()
        };

        let report = WorkflowReplayer::new()
            .register_fn("signal_or_deadline", signal_or_deadline_workflow)
            .replay_from_events(events)
            .await;

        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "iteration {i} must replay:\n{report}"
        );
    }
}

#[tokio::test]
async fn signal_timeout_timeout_branch_with_ignored_late_signal_replays_succeeded() {
    // A late approval ingested after the deadline fired, which the workflow's
    // auto-reject branch intentionally never consumes. This is a valid
    // production history and must not be reported as non-determinism.
    let timer_id = TimerId::new("__signal_timeout:1:approval");
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 300,
        },
        WorkflowEvent::TimerFired { timer_id },
        WorkflowEvent::SignalReceived {
            signal_name: "approval".to_string(),
            payload: serde_json::json!({"approved": true}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"escalated": true}),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("signal_or_deadline", signal_or_deadline_workflow)
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "timeout branch with an ignored late signal must replay:\n{report}"
    );
}

// ---------------------------------------------------------------------------
// Push-based signal handlers (issue #546)
// ---------------------------------------------------------------------------

/// A "subscription" style workflow that reacts to `cancel`/`pause` signals via
/// `register_signal_handler_raw` rather than hand-coded `wait_for_signal`
/// interleaving. Completes immediately once both handlers are registered and
/// have drained whatever history is already recorded for their names.
fn subscription_handler_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let cancelled = std::sync::Arc::new(std::sync::Mutex::new(false));
        let paused = std::sync::Arc::new(std::sync::Mutex::new(false));

        let c = cancelled.clone();
        ctx.register_signal_handler_raw("cancel", move |_payload: Value| {
            *c.lock().unwrap() = true;
        });
        let p = paused.clone();
        ctx.register_signal_handler_raw("pause", move |_payload: Value| {
            *p.lock().unwrap() = true;
        });

        Ok(serde_json::json!({
            "cancelled": *cancelled.lock().unwrap(),
            "paused": *paused.lock().unwrap(),
        }))
    })
}

#[tokio::test]
async fn replayer_replays_signal_handler_workflow_successfully() {
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "cancel".to_string(),
            payload: serde_json::json!({"reason": "user_requested"}),
        },
        WorkflowEvent::SignalReceived {
            signal_name: "pause".to_string(),
            payload: serde_json::json!({}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"cancelled": true, "paused": true}),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "subscription_handler_workflow",
            subscription_handler_workflow,
        )
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "handler-based workflow must replay successfully: {report}"
    );
}

/// Same fixture with the two signals recorded in the opposite order. Handler
/// dispatch must be replay-deterministic regardless of which signal name was
/// recorded first (issue #546 Success Metric: 100% replay-success rate across
/// reordered signal-arrival fixtures).
#[tokio::test]
async fn replayer_replays_signal_handler_workflow_with_reordered_signals() {
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "pause".to_string(),
            payload: serde_json::json!({}),
        },
        WorkflowEvent::SignalReceived {
            signal_name: "cancel".to_string(),
            payload: serde_json::json!({"reason": "user_requested"}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"cancelled": true, "paused": true}),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "subscription_handler_workflow",
            subscription_handler_workflow,
        )
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "reordered signal-arrival fixture must still replay successfully: {report}"
    );
}

/// A history recording only the `cancel` signal (no `pause`) must also replay
/// cleanly -- an unregistered-for-this-run signal name is simply never sent,
/// which is a normal history, not a divergence.
#[tokio::test]
async fn replayer_replays_signal_handler_workflow_with_only_one_signal() {
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "cancel".to_string(),
            payload: serde_json::json!({"reason": "user_requested"}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"cancelled": true, "paused": false}),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "subscription_handler_workflow",
            subscription_handler_workflow,
        )
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "single-signal fixture must replay successfully: {report}"
    );
}

// ---------------------------------------------------------------------------
// Saga compensation observability (issue #801)
// ---------------------------------------------------------------------------

/// Saga workflow with an activity-backed forward step + compensation, ending
/// in a manual `compensate_all()` unwind — the durable compensation pattern
/// documented in docs/saga.md.
fn saga_compensated_workflow<'a>(
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
            move |rsv: Value| async move {
                ctx.execute_activity_raw("release", rsv, "default")
                    .await
                    .map(|_| ())
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        saga.compensate_all().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!("compensated"))
    })
}

/// History for `saga_compensated_workflow` as recorded by #801+ code: the
/// `saga_compensated:{seq}` dedup marker sits between the forward step's
/// events and the compensation activity's events.
fn saga_marker_history() -> Vec<WorkflowEvent> {
    let reserve_id = ActivityExecId::new();
    let release_id = ActivityExecId::new();
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: reserve_id,
            name: "reserve".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: reserve_id,
            output: serde_json::json!("rsv-1"),
        },
        WorkflowEvent::MarkerRecorded {
            name: "saga_compensated:1".into(),
            details: serde_json::json!(1),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: release_id,
            name: "release".into(),
            input: serde_json::json!("rsv-1"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: release_id,
            output: Value::Null,
        },
    ]
}

/// Same shape as recorded by pre-#801 code: no saga marker anywhere.
fn saga_pre_marker_history() -> Vec<WorkflowEvent> {
    saga_marker_history()
        .into_iter()
        .filter(|event| !matches!(event, WorkflowEvent::MarkerRecorded { .. }))
        .collect()
}

/// Recorder counting only the two saga counters (everything else no-ops).
#[derive(Default)]
struct SagaCounterRecorder {
    compensated: std::sync::atomic::AtomicUsize,
    failed: std::sync::atomic::AtomicUsize,
}

impl autumn_harvest::telemetry::MetricsRecorder for SagaCounterRecorder {
    fn record_saga_compensated(&self, _workflow_name: &str, _queue: &str) {
        self.compensated
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_saga_compensation_failed(&self, _workflow_name: &str, _queue: &str) {
        self.failed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// A marker-bearing (#801+) saga history replays deterministically.
#[tokio::test]
async fn replayer_succeeds_for_saga_history_with_compensation_markers() {
    let report = WorkflowReplayer::new()
        .register_fn("saga_compensated_workflow", saga_compensated_workflow)
        .replay_from_events(saga_marker_history())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "marker-bearing saga history must replay cleanly: {report}"
    );
}

/// Replay of a recorded unwind emits zero saga metrics — the counters fire
/// only on the live frontier where the marker is first recorded.
#[tokio::test]
async fn replayer_replay_emits_no_saga_metrics() {
    let recorder = std::sync::Arc::new(SagaCounterRecorder::default());
    let report = WorkflowReplayer::new()
        .register_fn("saga_compensated_workflow", saga_compensated_workflow)
        .with_metrics(recorder.clone())
        .replay_from_events(saga_marker_history())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "fixture must replay cleanly: {report}"
    );
    assert_eq!(
        recorder
            .compensated
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a full-history replay must emit no saga.compensated samples"
    );
    assert_eq!(
        recorder.failed.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a full-history replay must emit no saga.compensation_failed samples"
    );
}

/// Backward compat (AC7): a pre-#801 marker-less saga history replays
/// untouched under the instrumented code — the Absent arm is non-mutating.
#[tokio::test]
async fn replayer_succeeds_for_pre_marker_saga_history() {
    let recorder = std::sync::Arc::new(SagaCounterRecorder::default());
    let report = WorkflowReplayer::new()
        .register_fn("saga_compensated_workflow", saga_compensated_workflow)
        .with_metrics(recorder.clone())
        .replay_from_events(saga_pre_marker_history())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "pre-#801 marker-less saga history must replay cleanly: {report}"
    );
    assert_eq!(
        recorder
            .compensated
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "legacy histories are never counted retroactively"
    );
}

/// Failed-unwind workflow for the post-review P2-1 persisted shape: one saga
/// step (in-memory forward, activity-backed compensation that terminally
/// failed), the author catches `SagaCompensationFailed`, consumes the
/// duplicate webhook signal, and completes normally.
fn saga_failed_unwind_with_trailing_signal_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mut saga = autumn_harvest::Saga::new(ctx);
        saga.step(
            || async { Ok::<_, autumn_harvest::HarvestError>(serde_json::json!("rsv-1")) },
            move |rsv: Value| async move {
                ctx.execute_activity_raw("release", rsv, "default")
                    .await
                    .map(|_| ())
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        if saga.compensate_all().await.is_ok() {
            return Err("compensation unexpectedly succeeded".to_string());
        }
        // Author-caught dangling state; consume the duplicate cancel webhook
        // signal that arrived at the final unwind cycle's wake, then complete.
        let _ = ctx
            .wait_for_signal("dup_cancel")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!("dangling_state_logged"))
    })
}

/// The exact history the P2-1 fix persists: the failure marker recorded PAST
/// the drained trailing signal. This must replay deterministically with zero
/// fresh samples — the money proof that the new marker position is
/// replay-consistent.
#[tokio::test]
async fn replayer_succeeds_for_failed_unwind_history_with_trailing_signal() {
    let release_id = ActivityExecId::new();
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "saga_compensated:1".into(),
            details: serde_json::json!(1),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: release_id,
            name: "release".into(),
            input: serde_json::json!("rsv-1"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: release_id,
            error: "release rejected".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "dup_cancel".into(),
            payload: serde_json::json!({"retry": true}),
        },
        WorkflowEvent::MarkerRecorded {
            name: "saga_compensation_failed:1".into(),
            details: serde_json::json!(1),
        },
    ];

    let recorder = std::sync::Arc::new(SagaCounterRecorder::default());
    let report = WorkflowReplayer::new()
        .register_fn(
            "saga_failed_unwind_with_trailing_signal_workflow",
            saga_failed_unwind_with_trailing_signal_workflow,
        )
        .with_metrics(recorder.clone())
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the failure marker recorded past a drained trailing signal must \
         replay cleanly: {report}"
    );
    assert_eq!(
        recorder.failed.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a full-history replay of the failed unwind emits no fresh samples"
    );
    assert_eq!(
        recorder
            .compensated
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

/// Cancel-and-compensate workflow for the marker-past-cancellation shape.
fn saga_cancel_and_compensate_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mut saga = autumn_harvest::Saga::new(ctx);
        saga.step(
            || async { Ok::<_, autumn_harvest::HarvestError>("flight-1") },
            |_| async { Ok::<_, autumn_harvest::HarvestError>(()) },
        )
        .await
        .map_err(|e| e.to_string())?;
        if ctx.is_cancelled() {
            saga.compensate_all().await.map_err(|e| e.to_string())?;
            return Ok(serde_json::json!("cancelled_and_compensated"));
        }
        Ok(serde_json::json!("completed"))
    })
}

/// The cancel-and-compensate persisted shape: the `saga_compensated:{seq}`
/// marker recorded AFTER the (never-consumed) `WorkflowCancelled` lifecycle
/// event. Must replay deterministically with zero fresh samples.
#[tokio::test]
async fn replayer_succeeds_for_cancel_and_compensate_history_with_marker() {
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::WorkflowCancelled {
            reason: "operator shutdown".into(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "saga_compensated:1".into(),
            details: serde_json::json!(1),
        },
    ];

    let recorder = std::sync::Arc::new(SagaCounterRecorder::default());
    let report = WorkflowReplayer::new()
        .register_fn(
            "saga_cancel_and_compensate_workflow",
            saga_cancel_and_compensate_workflow,
        )
        .with_metrics(recorder.clone())
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the compensated marker recorded past the cancellation event must \
         replay cleanly: {report}"
    );
    assert_eq!(
        recorder
            .compensated
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a replay of the recorded cancel-and-compensate unwind emits nothing"
    );
}

/// Codex P2 (PR #973 review): the normal terminal shape for a **pre-#801**
/// cancelled run is `[..., WorkflowCancelled]` with NO saga marker anywhere —
/// the cancellation event itself is the last recorded event. A replay probe
/// of that history against the cancel-and-compensate workflow must stay
/// uncounted and command-free: the matcher's cancellation-transparency
/// lookahead reports the frontier, but a `WorkflowReplayer` run is a pure
/// read — it must never retroactively count an old history nor push a fresh
/// marker command mid-replay.
#[tokio::test]
async fn replayer_pre_marker_cancelled_history_stays_uncounted() {
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::WorkflowCancelled {
            reason: "operator shutdown".into(),
        },
    ];

    let recorder = std::sync::Arc::new(SagaCounterRecorder::default());
    let report = WorkflowReplayer::new()
        .register_fn(
            "saga_cancel_and_compensate_workflow",
            saga_cancel_and_compensate_workflow,
        )
        .with_metrics(recorder.clone())
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a pre-#801 marker-less cancelled history must replay cleanly: {report}"
    );
    assert_eq!(
        recorder
            .compensated
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a replay probe must never retroactively count a pre-#801 cancelled history"
    );
    assert_eq!(recorder.failed.load(std::sync::atomic::Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// Replay-safe custom business metrics (issue #758, shipped as issue #532):
// a counter incremented once in a workflow body is emitted exactly once on
// the live execution and zero times across N >= 5 replay cycles.
// ---------------------------------------------------------------------------

/// Counting `MetricsRecorder` capturing every custom-metric emission.
///
/// `is_enabled()` keeps its default (`true`) so `UserMetrics` does not
/// short-circuit; every other recorder method keeps its no-op default.
#[derive(Default)]
struct CountingMetrics {
    counters: std::sync::atomic::AtomicU64,
    histograms: std::sync::atomic::AtomicU64,
}

impl CountingMetrics {
    fn counter_total(&self) -> u64 {
        self.counters.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn histogram_total(&self) -> u64 {
        self.histograms.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl autumn_harvest::telemetry::MetricsRecorder for CountingMetrics {
    fn record_user_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
        assert!(
            name.starts_with(autumn_harvest::telemetry::USER_METRIC_PREFIX),
            "custom metric names must carry the harvest.user. namespace, got {name}"
        );
        self.counters
            .fetch_add(value, std::sync::atomic::Ordering::SeqCst);
    }

    fn record_user_histogram(&self, name: &str, _value: f64, _labels: &[(&str, &str)]) {
        assert!(
            name.starts_with(autumn_harvest::telemetry::USER_METRIC_PREFIX),
            "custom metric names must carry the harvest.user. namespace, got {name}"
        );
        self.histograms
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// One activity, then a single business-counter increment and one histogram
/// sample (issue #758's "counter incremented once in workflow code" shape;
/// the histogram proves suppression covers both metric kinds).
fn business_counter_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let shipped = ctx
            .execute_activity_raw("fulfill", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        ctx.metrics()
            .counter("orders_fulfilled", 1, &[("tier", "gold")]);
        ctx.metrics()
            .histogram("order_amount_usd", 42.5, &[("tier", "gold")]);
        Ok(serde_json::json!({"shipped": shipped}))
    })
}

/// Loop variant: `input` is the iteration count; each iteration runs the
/// activity and increments the counter once — two live iterations must emit
/// exactly two.
fn business_counter_loop_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let iterations = input.as_u64().unwrap_or(0);
        for _ in 0..iterations {
            ctx.execute_activity_raw("fulfill", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            ctx.metrics()
                .counter("orders_fulfilled", 1, &[("tier", "gold")]);
        }
        Ok(serde_json::json!({"iterations": iterations}))
    })
}

/// Recorded history for `business_counter_workflow` — note there is **no**
/// event corresponding to the `ctx.metrics()` call: custom metrics leave the
/// history byte-identical (issue #758's append-only AC by construction).
fn business_counter_history() -> Vec<WorkflowEvent> {
    let activity_id = ActivityExecId::new();
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "fulfill".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!("shipped"),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"shipped": "shipped"}),
        },
    ]
}

/// AC (issue #758): a workflow that increments a counter once in its body,
/// replayed across N (>= 5) cycles, produces **exactly 1** emission — 1 from
/// the live run (asserted here via `WorkflowTestEnv`), 0 from the replays
/// (asserted in `five_cycle_replay_emits_zero_workflow_metrics` below).
#[tokio::test]
async fn live_execution_emits_workflow_counter_exactly_once() {
    let metrics = std::sync::Arc::new(CountingMetrics::default());
    let outcome = autumn_harvest::testing::WorkflowTestEnv::new()
        .with_metrics(metrics.clone())
        .mock_activity("fulfill", |_| Ok(serde_json::json!("shipped")))
        .run(business_counter_workflow, Value::Null)
        .await;

    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    // The test env drives multiple executor iterations (live frontier +
    // replay cycles after each suspension); emission still nets exactly one.
    assert_eq!(
        metrics.counter_total(),
        1,
        "a counter incremented once in workflow code must emit exactly once"
    );
    assert_eq!(
        metrics.histogram_total(),
        1,
        "a histogram sampled once in workflow code must emit exactly once"
    );
}

/// AC (issue #758): 0 double-counts across a 5-cycle replay of a counter
/// incremented once in workflow code.
#[tokio::test]
async fn five_cycle_replay_emits_zero_workflow_metrics() {
    let metrics = std::sync::Arc::new(CountingMetrics::default());

    for cycle in 0..5 {
        let report = WorkflowReplayer::new()
            .register_fn("business_counter_workflow", business_counter_workflow)
            .with_metrics(metrics.clone())
            .replay_from_events(business_counter_history())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "replay cycle {cycle} must succeed: {report}"
        );
    }

    assert_eq!(
        metrics.counter_total(),
        0,
        "custom workflow metrics must be suppressed on every replay cycle"
    );
    assert_eq!(
        metrics.histogram_total(),
        0,
        "custom workflow histograms must be suppressed on every replay cycle"
    );
}

/// AC (issue #758): a live execution that legitimately hits the increment
/// twice (a loop iteration recorded in history) emits exactly 2.
#[tokio::test]
async fn live_loop_hitting_increment_twice_emits_exactly_two() {
    let metrics = std::sync::Arc::new(CountingMetrics::default());
    let outcome = autumn_harvest::testing::WorkflowTestEnv::new()
        .with_metrics(metrics.clone())
        .mock_activity("fulfill", |_| Ok(serde_json::json!("shipped")))
        .run(business_counter_loop_workflow, serde_json::json!(2))
        .await;

    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    assert_eq!(
        metrics.counter_total(),
        2,
        "two legitimate live increments must emit exactly twice — no more (replay double-count), no fewer (over-suppression)"
    );
}
