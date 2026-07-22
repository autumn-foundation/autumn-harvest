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
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{IntoWorkflowErrorString, WorkflowFailure, decode_workflow_failure};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::testing::{
    HistorySnapshot, NonDeterminismKind, ReplayStatus, WorkflowReplayer,
};
use autumn_harvest::types::{
    ActivityExecId, ExecutionId, ParentClosePolicy, TimerId, UpdateId, WorkerId,
};
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

// ──────────────────────────── mutex (issue #691) ─────────────────────────────
//
// Durable mutex replay contract: `ctx.mutex(key).acquire()` records a single
// `MutexGranted { key, lock_seq, acquired_at }` anchor when the lock is granted
// (the matcher is strictly positional); release is event-less bookkeeping. So a
// recorded `[WorkflowStarted, MutexGranted]` history replays with 100% fidelity
// regardless of which `lock_seq` was minted at grant time.

/// Acquire a durable mutex, use the guard for a trivial critical section, drop
/// it at scope end, and complete.
fn mutex_grant_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
        // Trivial critical section — read the fencing token so the guard is used.
        let _seq = guard.lock_seq();
        Ok(serde_json::json!({"held": guard.key()}))
    })
}

/// Acquire "k", release explicitly, then run an activity — proves the event-less
/// release replays cleanly *before* a subsequent recorded event (the activity).
fn mutex_release_then_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
        guard.release();
        let r = ctx
            .execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"step": r}))
    })
}

/// Acquire key "a" — used to prove a recorded `MutexGranted { key: "b" }`
/// diverges as a `MutexGrantMismatch` (key-divergence detection).
fn mutex_grant_key_a_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _guard = ctx.mutex("a").acquire().await.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Hold "k", then re-acquire the same key. The self-deadlock guard must fire
/// **synchronously** (before any positional history match), so the caught
/// `MutexSelfDeadlock` branch replays deterministically rather than diverging —
/// the P1 regression this pins (issue #691 review: a self-deadlock checked
/// *after* the positional match returns the typed error live but nd-blocks on
/// replay).
fn mutex_self_deadlock_caught_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let g1 = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
        let result = match ctx.mutex("k").acquire().await {
            Ok(_) => Err("expected a self-deadlock error".to_string()),
            Err(e) if e.is_mutex_self_deadlock() => Ok(serde_json::json!("caught_self_deadlock")),
            Err(e) => Err(e.to_string()),
        };
        drop(g1);
        result
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

/// Deadline-aware continue-as-new (issue #772): at the top of the run, check
/// `should_continue_as_new()`; when it trips (event count OR the deadline
/// fraction), fork a fresh run via `continue_as_new`. Otherwise do a unit of
/// "work" and complete. Registered with a per-run `execution_timeout` on the
/// replayer so the deadline branch is exercised.
fn deadline_can_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if ctx.should_continue_as_new() {
            let prev = input["cycle"].as_i64().unwrap_or(0);
            ctx.continue_as_new(serde_json::json!({ "cycle": prev + 1 }))
                .await
                .map_err(|e| e.to_string())?;
            return Ok(Value::Null);
        }
        Ok(serde_json::json!("done"))
    })
}

/// Deadline-aware decision probe (issue #772) whose `should_continue_as_new()`
/// decision is made **observable through replay determinism**: the trip branch
/// schedules the `checkpoint_now` activity, the no-trip branch schedules
/// `keep_working`. Because the recorded history fixes which activity was
/// scheduled, a *wrong* decision (a broken fraction comparison) schedules the
/// other activity and diverges — so the 1000× bar actually falsifies a broken
/// comparison rather than passing regardless of the decision.
fn deadline_branch_probe_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity = if ctx.should_continue_as_new() {
            "checkpoint_now"
        } else {
            "keep_working"
        };
        ctx.execute_activity_raw(activity, Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Long-lived entity shape (issue #772 Finding 1): `should_continue_as_new()` at
/// the top, then a durable timer + complete. Used to prove a pre-#772 history
/// (recorded with NO `SideEffectRecorded{Now}` at the check site) replays
/// cleanly under the new deadline-aware binary instead of nd-blocking.
fn should_can_then_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if ctx.should_continue_as_new() {
            let prev = input["cycle"].as_i64().unwrap_or(0);
            ctx.continue_as_new(serde_json::json!({ "cycle": prev + 1 }))
                .await
                .map_err(|e| e.to_string())?;
            return Ok(Value::Null);
        }
        ctx.timer("renewal", 3600)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!("done"))
    })
}

/// Migration-path shape (issue #772, deadline-probe naming fix): the deadline
/// check sits immediately before a **user** `ctx.system_now()` call, then a
/// durable timer. In a pre-#772 history there is NO recorded deadline-probe
/// `Now` at the check site — but there IS the user's own `system_now()` Now at
/// the cursor. The deadline probe must NOT consume that user `Now`; it belongs
/// to `ctx.system_now()`. Before the naming fix, the tolerant matcher treated
/// any `SideEffectRecorded{Now, name: None}` at the cursor as its own read,
/// stole the user's `Now`, and the subsequent `ctx.system_now()` then replayed
/// against the following `TimerStarted` and reported false non-determinism.
fn should_can_then_user_now_then_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if ctx.should_continue_as_new() {
            let prev = input["cycle"].as_i64().unwrap_or(0);
            ctx.continue_as_new(serde_json::json!({ "cycle": prev + 1 }))
                .await
                .map_err(|e| e.to_string())?;
            return Ok(Value::Null);
        }
        // A genuine author-side `system_now()` immediately after the deadline
        // check. Its recorded `Now` must be consumed HERE, not by the probe.
        let _captured = ctx.system_now();
        ctx.timer("renewal", 3600)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!("done"))
    })
}

/// Await an external workflow's terminal output (issue #757). Returns the
/// awaited target's output verbatim.
fn await_external_wf<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_str = input["target"].as_str().ok_or("missing target")?;
        let target =
            ExecutionId::from_uuid(uuid::Uuid::parse_str(target_str).map_err(|e| e.to_string())?);
        let out = ctx
            .await_external_workflow_value(target)
            .await
            .map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// Await an external workflow and branch on its typed terminal cause (issue
/// #757): the branch decision is made observable through replay determinism —
/// a `PaymentDeclined` failure schedules `compensate`, any other outcome
/// schedules `finalize`. A wrong branch would schedule the other activity and
/// diverge.
fn await_external_branch_wf<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_str = input["target"].as_str().ok_or("missing target")?;
        let target =
            ExecutionId::from_uuid(uuid::Uuid::parse_str(target_str).map_err(|e| e.to_string())?);
        let activity = match ctx.await_external_workflow_value(target).await {
            Err(e) if e.workflow_error_type() == Some("PaymentDeclined") => "compensate",
            _ => "finalize",
        };
        ctx.execute_activity_raw(activity, Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Await an external workflow concurrently with an activity (issue #757): both
/// run in one `tokio::join!` batch. Regression fixture for the interleaved
/// stash triplets across randomized re-drive orderings.
fn await_external_concurrent_wf<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_str = input["target"].as_str().ok_or("missing target")?;
        let target =
            ExecutionId::from_uuid(uuid::Uuid::parse_str(target_str).map_err(|e| e.to_string())?);
        let (act, awaited) = futures::join!(
            ctx.execute_activity_raw("compute", Value::Null, "default"),
            ctx.await_external_workflow_value(target),
        );
        act.map_err(|e| e.to_string())?;
        let out = awaited.map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// Await an external workflow concurrently with a durable timer (issue #757):
/// both run in one `futures::join!` batch. Regression fixture proving the await
/// stash triplets keep replay deterministic when `ExternalAwaitResolved` and
/// `TimerFired` are recorded in EITHER order (mirrors the #476
/// `receive_signal_timeout` concurrent-with-timer bar).
fn await_external_concurrent_timer_wf<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target_str = input["target"].as_str().ok_or("missing target")?;
        let target =
            ExecutionId::from_uuid(uuid::Uuid::parse_str(target_str).map_err(|e| e.to_string())?);
        let (timer_res, awaited) = futures::join!(
            ctx.timer("wake", 1),
            ctx.await_external_workflow_value(target)
        );
        timer_res.map_err(|e| e.to_string())?;
        let out = awaited.map_err(|e| e.to_string())?;
        Ok(out)
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

/// Duplicate ACTIVE arm (Codex P2, issue #768): calls `start_timer("idle", 300)`
/// twice with no intervening cancel/reset, then awaits the fire. The durable row
/// is deduped, so the recorded history has exactly ONE `TimerStarted("idle")`.
/// On replay the first `start_timer` consumes it; the second must be an
/// idempotent no-op that does NOT re-run the positional `match_timer_arm`
/// (which would diverge against the trailing `TimerFired`).
fn duplicate_active_start_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _first = ctx.start_timer("idle", 300);
        let handle = ctx.start_timer("idle", 300); // duplicate active arm — no-op
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("{outcome:?}")))
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

/// Duplicate ACTIVE arm with a DIFFERENT duration (Codex P2 round 5, issue
/// #768): `start_timer("idle", 300)` then `start_timer("idle", 600)` with no
/// cancel/reset between. The second call is a duration-preserving idempotent
/// no-op — the returned handle MUST carry the ORIGINAL recorded 300s, not 600,
/// so the durable row / virtual clock stay consistent with the single recorded
/// `TimerStarted(idle, 300)`. Emits the armed duration in its result so the test
/// can assert 300 (before the fix the handle carried 600).
fn duplicate_active_start_timer_diff_duration_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _first = ctx.start_timer("idle", 300);
        // Duplicate active arm with a DIFFERENT duration — MUST preserve 300.
        let handle = ctx.start_timer("idle", 600);
        let armed = handle.duration_secs();
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "armed": armed, "outcome": format!("{outcome:?}") }))
    })
}

/// SOUNDNESS gate (Codex P2 round 5, issue #768): the REORDERED code awaits the
/// timer BEFORE the activity. Replayed against a history where the activity was
/// recorded FIRST (`[…, TimerStarted, ActivityScheduled, ActivityCompleted,
/// TimerFired]`), the `await_fire` outcome scan must STOP at the unconsumed
/// `ActivityScheduled` (returning `NoMatch`) so strict replay reports a
/// non-determinism divergence — instead of skipping across the activity to claim
/// the trailing `TimerFired` and passing (the false-negative the fix closes).
fn timer_await_before_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        let out = ctx
            .execute_activity_raw("work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("{outcome:?}:{out}")))
    })
}

/// LEGIT flow (Codex P2 round 5, issue #768): arm the timer, run the activity,
/// THEN await the timer — the order recorded in history. When `await_fire` runs,
/// the activity events are already CONSUMED, so the outcome scan crosses them via
/// `is_consumed` and reaches the `TimerFired`. Proves the soundness stop does NOT
/// break the legitimate `arm→activity→await` flow.
fn activity_then_timer_await_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        let out = ctx
            .execute_activity_raw("work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("{out}:{outcome:?}")))
    })
}

/// FIX 1 SOUNDNESS GATE (Codex P2 round 6, issue #768): the REORDERED code
/// CANCELS the timer BEFORE running the activity. Replayed against a history that
/// recorded the activity FIRST (`[…, TimerStarted, ActivityScheduled,
/// ActivityCompleted, TimerCancelled, WorkflowCompleted]`), `match_timer_cancel`'s
/// scan must STOP at the unconsumed `ActivityScheduled` (returning `NoMatch`) so
/// strict replay reports a non-determinism divergence — instead of skipping across
/// the activity to claim the trailing `TimerCancelled` and passing (the
/// false-negative this fix closes; sibling of the round-5 `match_timer_or_cancel`
/// fix).
fn cancel_timer_before_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        handle.cancel().map_err(|e| e.to_string())?; // reordered: cancel BEFORE the activity
        let out = ctx
            .execute_activity_raw("work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!(format!("cancelled:{out}")))
    })
}

/// FIX 1 LEGIT control (Codex P2 round 6, issue #768): arm the timer, run the
/// activity, THEN cancel — the order recorded in history. When `cancel()` runs,
/// the activity events are already CONSUMED, so `match_timer_cancel`'s scan crosses
/// them via `is_consumed` and reaches the `TimerCancelled` at the cursor. Proves
/// the soundness stop does NOT break the legitimate `arm→activity→cancel` flow.
fn activity_then_cancel_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        let out = ctx
            .execute_activity_raw("work", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        handle.cancel().map_err(|e| e.to_string())?; // cancel AFTER the activity (as recorded)
        Ok(serde_json::json!(format!("{out}:cancelled")))
    })
}

/// FIX 2 (Codex P2 round 6, issue #768): the core sliding-window / idle-session
/// LOOP that reuses the SAME timer id — arm; await→Fired; arm again — N times,
/// with a fresh duration each iteration. Consuming a fire must CLEAR the Armed
/// logical state so each iteration's `start_timer` records a FRESH `TimerStarted`
/// (N `TimerStarted` + N `TimerFired`, balanced). Before the fix the stale Armed
/// state made the second+ `start_timer` a duplicate no-op, leaving the recorded
/// `TimerStarted`s unconsumed → an early-completion divergence.
fn cancellable_timer_loop_reuse_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let iterations = input["iterations"].as_u64().unwrap_or(0);
        let vary = input["vary_duration"].as_bool().unwrap_or(false);
        for i in 0..iterations {
            let dur = if vary { 300 + i } else { 300 };
            let handle = ctx.start_timer("idle", dur);
            let _outcome = handle.await_fire().await.map_err(|e| e.to_string())?;
        }
        Ok(serde_json::json!({ "iterations": iterations }))
    })
}

/// Codex P2 round 10 (issue #768) — CANONICAL ORDER: arm the timer, THEN cancel
/// it (no intervening activity), matching the recorded history
/// `[TimerStarted(idle), TimerCancelled(idle)]`. Must replay cleanly: `start_timer`
/// consumes the `TimerStarted` at the cursor, then `match_timer_cancel` claims the
/// `TimerCancelled` directly at the (now-advanced) cursor.
fn start_then_cancel_no_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        handle.cancel().map_err(|e| e.to_string())?;
        Ok(serde_json::json!("cancelled"))
    })
}

/// Codex P2 round 10 (issue #768) — REORDERED: cancel the timer BEFORE arming it,
/// replayed against the canonical `[TimerStarted(idle), TimerCancelled(idle)]`
/// history. `match_timer_cancel("idle")`'s scan must STOP at the unconsumed
/// SAME-id `TimerStarted` (this id's own command-ordering anchor) rather than
/// crossing it to claim the trailing `TimerCancelled` — so strict replay reports
/// `NonDeterminismDetected`. Before the round-10 fix the shared helper crossed a
/// same-id `TimerStarted` transparently, wrongly passing the reorder.
fn cancel_then_start_no_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.cancel_timer("idle").map_err(|e| e.to_string())?; // reordered: cancel BEFORE the arm
        let _handle = ctx.start_timer("idle", 300);
        Ok(serde_json::json!("cancelled"))
    })
}

/// Non-blocking signal drain (issue #775): run one activity, then drain every
/// buffered "event" signal in the same task execution. `SignalReceived` events
/// interleaved with the activity events in history must replay deterministically.
fn drain_after_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("step_one", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let drained = ctx.drain_signals_raw("event").map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "drained": drained }))
    })
}

/// Drains every buffered "event" signal in ONE task execution and returns the
/// count — the falsifiable success-metric workflow (issue #775). If the drain
/// failed to consume all buffered signals, the leftover unconsumed history
/// would flag the replay as non-deterministic rather than `ReplaySucceeded`.
fn drain_n_signals_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let drained = ctx.drain_signals_raw("event").map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "count": drained.len() }))
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

/// Workflow that branches on a contained **handler-panic** activity cause
/// (issue #782): a panicking activity is contained and, after its retry budget
/// is exhausted, records a terminal `ActivityFailed` carrying the distinct
/// `HandlerPanic` `error_type` on the *existing* event variant (no new variant).
/// The workflow observes that typed cause via `activity_error_type()` and
/// advances to its own compensation activity — proving AC6 "replay contract
/// unchanged": a history containing a `HandlerPanic`-typed `ActivityFailed`
/// replays deterministically down the same branch. If the typed `error_type`
/// did not survive replay the handler would fall through to `Err(e)` and
/// complete early, diverging from the recorded compensation activity — so
/// `ReplaySucceeded` is falsifiable evidence.
fn handler_panic_compensating_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        match ctx
            .execute_activity_raw("risky_step", Value::Null, "default")
            .await
        {
            Ok(v) => Ok(serde_json::json!({"ok": v})),
            // Branch purely on the typed HandlerPanic error_type — no substring
            // matching on the message.
            Err(e) if e.activity_error_type() == Some("HandlerPanic") => {
                let r = ctx
                    .execute_activity_raw("compensate", Value::Null, "default")
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

/// Issue #1126 replay-neutrality fixture: race two activities (branch 0 wins),
/// then run a follow-on activity. Proves the recorded post-fix history — winner
/// marker + follow-on schedule + the synthetic loser `ActivityFailed` — replays
/// with 100% fidelity. Must pass both before AND after the same-cycle fix.
fn race_then_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("fetch_a", Value::Null, "default")
            .activity_raw("fetch_b", Value::Null, "default")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        // Defensive: the recorded history resolved branch 0; a divergent index
        // would surface as a workflow error rather than a silent mismatch. In a
        // faithful replay this guard never fires.
        if winner.index != 0 {
            return Err(format!("expected branch 0 to win, got {}", winner.index));
        }
        let follow = ctx
            .execute_activity_raw("next_step", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"winner": winner.value, "follow": follow}))
    })
}

/// Issue #1126 replay-neutrality fixture: race two activities (branch 0 wins)
/// and end at the race with no follow-on command — the exact history shape old
/// code could record. Must pass both before AND after the same-cycle fix.
fn race_only_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("fetch_a", Value::Null, "default")
            .activity_raw("fetch_b", Value::Null, "default")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(winner.value)
    })
}

#[allow(clippy::too_many_lines)] // merge of trunk + branch register_fn lists
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
        .register_fn(
            "handler_panic_compensating_workflow",
            handler_panic_compensating_workflow,
        )
        .register_fn("cancellable_timer_workflow", cancellable_timer_workflow)
        .register_fn(
            "arm_timer_then_complete_workflow",
            arm_timer_then_complete_workflow,
        )
        .register_fn(
            "duplicate_active_start_timer_workflow",
            duplicate_active_start_timer_workflow,
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
        .register_fn(
            "duplicate_active_start_timer_diff_duration_workflow",
            duplicate_active_start_timer_diff_duration_workflow,
        )
        .register_fn(
            "timer_await_before_activity_workflow",
            timer_await_before_activity_workflow,
        )
        .register_fn(
            "activity_then_timer_await_workflow",
            activity_then_timer_await_workflow,
        )
        .register_fn(
            "cancel_timer_before_activity_workflow",
            cancel_timer_before_activity_workflow,
        )
        .register_fn(
            "activity_then_cancel_timer_workflow",
            activity_then_cancel_timer_workflow,
        )
        .register_fn(
            "cancellable_timer_loop_reuse_workflow",
            cancellable_timer_loop_reuse_workflow,
        )
        .register_fn(
            "start_then_cancel_no_activity_workflow",
            start_then_cancel_no_activity_workflow,
        )
        .register_fn(
            "cancel_then_start_no_activity_workflow",
            cancel_then_start_no_activity_workflow,
        )
        .register_fn(
            "drain_after_activity_workflow",
            drain_after_activity_workflow,
        )
        .register_fn("drain_n_signals_workflow", drain_n_signals_workflow)
        .register_fn("deadline_can_workflow", deadline_can_workflow)
        .register_fn(
            "deadline_branch_probe_workflow",
            deadline_branch_probe_workflow,
        )
        .register_fn(
            "should_can_then_timer_workflow",
            should_can_then_timer_workflow,
        )
        .register_fn(
            "should_can_then_user_now_then_timer_workflow",
            should_can_then_user_now_then_timer_workflow,
        )
        .register_fn("await_external_workflow", await_external_wf)
        .register_fn("await_external_branch_workflow", await_external_branch_wf)
        .register_fn(
            "await_external_concurrent_workflow",
            await_external_concurrent_wf,
        )
        .register_fn(
            "await_external_concurrent_timer_workflow",
            await_external_concurrent_timer_wf,
        )
        .register_fn("mutex_grant_workflow", mutex_grant_workflow)
        .register_fn(
            "mutex_release_then_activity_workflow",
            mutex_release_then_activity_workflow,
        )
        .register_fn("mutex_grant_key_a_workflow", mutex_grant_key_a_workflow)
        .register_fn(
            "mutex_self_deadlock_caught_workflow",
            mutex_self_deadlock_caught_workflow,
        )
        .register_fn("race_then_activity_workflow", race_then_activity_workflow)
        .register_fn("race_only_workflow", race_only_workflow)
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
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
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
// (a0b) A contained activity HANDLER PANIC (issue #782) replays deterministically
//       down the workflow's own compensation branch. The panic is contained by
//       the engine into a terminal `ActivityFailed` carrying the distinct
//       `HandlerPanic` error_type on the EXISTING event variant — AC6 requires
//       the replay contract to be unchanged, i.e. such a history must replay
//       clean and drive the same branch every cycle.
// ---------------------------------------------------------------------------

/// History recorded by a run whose `risky_step` activity panicked: after its
/// retry budget was exhausted the engine recorded a terminal `ActivityFailed`
/// carrying the engine-reserved `HandlerPanic` `error_type` (issue #782, no new
/// event variant); the workflow then ran its compensation activity on the live
/// frontier.
fn handler_panic_activity_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let risky_id = ActivityExecId::new();
    let compensate_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: risky_id,
            name: "risky_step".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: risky_id,
            error: "activity handler panicked: risky boom".into(),
            attempt: 2,
            error_type: "HandlerPanic".into(),
            non_retryable: true,
            details: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: compensate_id,
            name: "compensate".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: compensate_id,
            output: serde_json::json!("compensated"),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replay_handler_panic_activity_takes_compensation_branch_deterministically() {
    let (exec_id, events) = handler_panic_activity_history();
    let replayer = build_replayer();

    // Replay the SAME history multiple times — the contained HandlerPanic cause
    // must drive the same (compensation) branch every cycle.
    for cycle in 0..3 {
        let report = replayer
            .replay_from_snapshot(make_snapshot(
                "handler_panic_compensating_workflow",
                exec_id,
                events.clone(),
            ))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "cycle {cycle}: a history containing a HandlerPanic ActivityFailed must \
             replay deterministically (issue #782 AC6, replay contract unchanged), got: {report}"
        );
        assert!(
            report.events_replayed > 0,
            "cycle {cycle}: events_replayed must be positive"
        );
    }
}

// ---------------------------------------------------------------------------
// (a0.5) Non-blocking signal drain (issue #775) replays deterministically, and
//        the falsifiable success metric: N buffered signals drained in ONE task.
// ---------------------------------------------------------------------------

/// History with `SignalReceived` events interleaved with the activity events —
/// one before the activity is scheduled, one after it completes.
fn drain_interleaved_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let step_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        // Signal ingested before the workflow reaches the activity call.
        WorkflowEvent::SignalReceived {
            signal_name: "event".into(),
            payload: serde_json::json!({"seq": 1}),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: step_id,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: step_id,
            output: serde_json::json!("done"),
        },
        // Signal ingested after the activity completed but before the drain.
        WorkflowEvent::SignalReceived {
            signal_name: "event".into(),
            payload: serde_json::json!({"seq": 2}),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replay_drain_signals_interleaved_with_activity_is_deterministic() {
    let (exec_id, events) = drain_interleaved_history();
    let replayer = build_replayer();

    // Replay the same history multiple times: the interleaved signals must be
    // drained identically every cycle.
    for cycle in 0..3 {
        let report = replayer
            .replay_from_snapshot(make_snapshot(
                "drain_after_activity_workflow",
                exec_id,
                events.clone(),
            ))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "cycle {cycle}: a history with SignalReceived interleaved with activity \
             events plus a drain_signals call must replay deterministically, got: {report}"
        );
    }
}

#[tokio::test]
async fn replay_drain_n_signals_in_one_task_consumes_all() {
    // Falsifiable success metric (issue #775): N buffered signals drained in a
    // SINGLE task execution. If the drain left any of the N unconsumed, the
    // replay would fail the strict-mode unconsumed-history check rather than
    // report ReplaySucceeded.
    let exec_id = ExecutionId::new();
    let n = 1000usize;
    let mut events = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    for i in 0..n {
        events.push(WorkflowEvent::SignalReceived {
            signal_name: "event".into(),
            payload: serde_json::json!({ "seq": i }),
        });
    }

    let replayer = build_replayer();
    let report = replayer
        .replay_from_snapshot(make_snapshot("drain_n_signals_workflow", exec_id, events))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{n} buffered signals must be drained in one task execution (else leftover \
         unconsumed history flags non-determinism), got: {report}"
    );
}

// ---------------------------------------------------------------------------
// (a2) ctx.await_external_workflow (issue #757) replays deterministically.
// ---------------------------------------------------------------------------

/// A recorded history for a workflow that awaited an external target which
/// reached COMPLETED — `ExternalAwaitRequested` + `ExternalAwaitResolved`.
fn await_resolved_history(target: ExecutionId, output: Value) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let await_id = autumn_harvest::ExternalAwaitId::new();
    let events = vec![
        WorkflowEvent::workflow_started(
            serde_json::json!({ "target": target.to_string() }),
            Utc::now(),
        ),
        WorkflowEvent::ExternalAwaitRequested { await_id, target },
        WorkflowEvent::ExternalAwaitResolved { await_id, output },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replayer_succeeds_for_await_external_workflow() {
    let target = ExecutionId::new();
    let (exec_id, events) =
        await_resolved_history(target, serde_json::json!({ "tracking": "abc123" }));
    let replayer = build_replayer();
    let report = replayer
        .replay_from_snapshot(make_snapshot("await_external_workflow", exec_id, events))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a history awaiting an external target that COMPLETED must replay \
         deterministically, got: {report}"
    );
}

/// The falsifiable success metric (issue #757, mirroring the #476 1000-ordering
/// bar): replay one representative await fixture across N = 1000 distinct
/// fixtures (varying target id and output) — every pass must report
/// `ReplaySucceeded` with zero divergences. Each fixture pins the recorded
/// output through the workflow's own return value, so a broken match would
/// diverge rather than pass.
#[tokio::test]
async fn replayer_await_external_workflow_1000_passes_deterministic() {
    let replayer = build_replayer();
    for i in 0..1000_usize {
        let target = ExecutionId::new();
        let output = serde_json::json!({ "seq": i, "target": target.to_string() });
        let (exec_id, events) = await_resolved_history(target, output);
        let report = replayer
            .replay_from_snapshot(make_snapshot("await_external_workflow", exec_id, events))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "iteration {i}: await-external fixture must replay deterministically, got: {report}"
        );
    }
}

/// A concurrent await+activity history whose interleaving is derived from
/// `ordering` (issue #757 review, P3-b): the schedule block (`ActivityScheduled`
/// vs `ExternalAwaitRequested`) and the completion block (`ActivityCompleted` vs
/// `ExternalAwaitResolved`) are each swapped by one bit of `ordering`, giving the
/// four valid interleavings (schedules always precede completions, which the
/// `futures::join!` batch guarantees). The `await_external_concurrent_workflow`
/// handler consumes both via the stash triplets in every scan loop.
fn await_concurrent_history(
    target: ExecutionId,
    output: Value,
    act_output: Value,
    ordering: usize,
) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let await_id = autumn_harvest::ExternalAwaitId::new();
    let act_id = ActivityExecId::new();

    let sched = WorkflowEvent::ActivityScheduled {
        activity_id: act_id,
        name: "compute".into(),
        input: Value::Null,
        queue: "default".into(),
    };
    let requested = WorkflowEvent::ExternalAwaitRequested { await_id, target };
    let completed = WorkflowEvent::ActivityCompleted {
        activity_id: act_id,
        output: act_output,
    };
    let resolved = WorkflowEvent::ExternalAwaitResolved { await_id, output };

    let mut events = vec![WorkflowEvent::workflow_started(
        serde_json::json!({ "target": target.to_string() }),
        Utc::now(),
    )];
    // Schedule block (bit 0).
    if ordering & 1 == 0 {
        events.push(sched);
        events.push(requested);
    } else {
        events.push(requested);
        events.push(sched);
    }
    // Completion block (bit 1).
    if ordering & 2 == 0 {
        events.push(completed);
        events.push(resolved);
    } else {
        events.push(resolved);
        events.push(completed);
    }
    (exec_id, events)
}

/// The falsifiable ORDERING bar (issue #757 review, P3-b, mirroring the #476
/// 1000-ordering precedent): replay the concurrent await+activity fixture across
/// N = 1000 passes, walking a deterministic index-derived permutation of the four
/// valid interleavings of the await triplet relative to the activity's events
/// (with varying data). Every pass must report `ReplaySucceeded` — a stash bug in
/// any scan loop would diverge on the ordering it mishandles.
#[tokio::test]
async fn replayer_await_external_concurrent_1000_orderings_deterministic() {
    let replayer = build_replayer();
    let mut per_ordering = [0usize; 4];
    for i in 0..1000_usize {
        let ordering = i % 4;
        per_ordering[ordering] += 1;
        let target = ExecutionId::new();
        let output = serde_json::json!({ "seq": i, "ok": true });
        let act_output = serde_json::json!(i);
        let (exec_id, events) = await_concurrent_history(target, output, act_output, ordering);
        let report = replayer
            .replay_from_snapshot(make_snapshot(
                "await_external_concurrent_workflow",
                exec_id,
                events,
            ))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "iteration {i} (ordering {ordering}): concurrent await+activity must replay \
             deterministically across randomized interleavings, got: {report}"
        );
    }
    // Every one of the four interleavings was genuinely exercised.
    assert!(
        per_ordering.iter().all(|&c| c > 0),
        "all four interleavings must be covered: {per_ordering:?}"
    );
}

#[tokio::test]
async fn replayer_await_external_failed_target_branches_deterministically() {
    // A target that reached FAILED with a typed PaymentDeclined cause: the
    // awaiter's history recorded `ExternalAwaitFailed` + the compensation
    // activity. Replay must recognize the typed cause and take the same branch.
    let target = ExecutionId::new();
    let exec_id = ExecutionId::new();
    let await_id = autumn_harvest::ExternalAwaitId::new();
    let compensate_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::workflow_started(
            serde_json::json!({ "target": target.to_string() }),
            Utc::now(),
        ),
        WorkflowEvent::ExternalAwaitRequested { await_id, target },
        WorkflowEvent::ExternalAwaitFailed {
            await_id,
            reason_code: "target_failed".into(),
            message: Some("card declined".into()),
            error_type: Some("PaymentDeclined".into()),
            details: None,
            non_retryable: Some(true),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: compensate_id,
            name: "compensate".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: compensate_id,
            output: serde_json::json!("compensated"),
        },
    ];
    let replayer = build_replayer();
    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "await_external_branch_workflow",
            exec_id,
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a history awaiting a FAILED target must replay the typed-cause branch \
         deterministically, got: {report}"
    );
}

#[tokio::test]
async fn replayer_await_external_concurrent_with_activity_deterministic() {
    // Await concurrent with an activity in one tokio::join! batch. Replay across
    // both interleaved orderings of the recorded terminal events must succeed —
    // the stash triplets in every scan loop keep neither from diverging.
    let target = ExecutionId::new();
    let await_id = autumn_harvest::ExternalAwaitId::new();
    let act_id = ActivityExecId::new();
    let output = serde_json::json!({ "ok": true });
    // Ordering A: activity terminal before await terminal.
    let events_a = vec![
        WorkflowEvent::workflow_started(
            serde_json::json!({ "target": target.to_string() }),
            Utc::now(),
        ),
        WorkflowEvent::ActivityScheduled {
            activity_id: act_id,
            name: "compute".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ExternalAwaitRequested { await_id, target },
        WorkflowEvent::ActivityCompleted {
            activity_id: act_id,
            output: serde_json::json!(7),
        },
        WorkflowEvent::ExternalAwaitResolved {
            await_id,
            output: output.clone(),
        },
    ];
    // Ordering B: await terminal before activity terminal.
    let events_b = vec![
        WorkflowEvent::workflow_started(
            serde_json::json!({ "target": target.to_string() }),
            Utc::now(),
        ),
        WorkflowEvent::ActivityScheduled {
            activity_id: act_id,
            name: "compute".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ExternalAwaitRequested { await_id, target },
        WorkflowEvent::ExternalAwaitResolved {
            await_id,
            output: output.clone(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act_id,
            output: serde_json::json!(7),
        },
    ];
    let replayer = build_replayer();
    for (label, events) in [("A", events_a), ("B", events_b)] {
        let exec_id = ExecutionId::new();
        let report = replayer
            .replay_from_snapshot(make_snapshot(
                "await_external_concurrent_workflow",
                exec_id,
                events,
            ))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "ordering {label}: concurrent await+activity must replay deterministically, got: {report}"
        );
    }
}

#[tokio::test]
async fn replayer_await_external_concurrent_with_timer_deterministic() {
    // P2-e (issue #757 review): await concurrent with a durable timer in one
    // `futures::join!` batch. Replay across BOTH interleaved orderings of the
    // recorded terminal events (`TimerFired` vs `ExternalAwaitResolved`) must
    // succeed — the await stash triplets threaded through `match_signal_or_timer`
    // / `match_child_or_timer` keep the timer scan from diverging.
    let target = ExecutionId::new();
    let await_id = autumn_harvest::ExternalAwaitId::new();
    let output = serde_json::json!({ "value": 7 });
    let started = || {
        WorkflowEvent::workflow_started(
            serde_json::json!({ "target": target.to_string() }),
            Utc::now(),
        )
    };
    let timer_started = || WorkflowEvent::TimerStarted {
        timer_id: TimerId::new("wake"),
        duration_secs: 1,
    };
    let requested = || WorkflowEvent::ExternalAwaitRequested { await_id, target };
    let timer_fired = || WorkflowEvent::TimerFired {
        timer_id: TimerId::new("wake"),
    };
    let resolved = || WorkflowEvent::ExternalAwaitResolved {
        await_id,
        output: output.clone(),
    };
    // Ordering A: TimerFired before ExternalAwaitResolved.
    let events_a = vec![
        started(),
        timer_started(),
        requested(),
        timer_fired(),
        resolved(),
    ];
    // Ordering B: ExternalAwaitResolved before TimerFired.
    let events_b = vec![
        started(),
        timer_started(),
        requested(),
        resolved(),
        timer_fired(),
    ];
    let replayer = build_replayer();
    for (label, events) in [("A", events_a), ("B", events_b)] {
        let exec_id = ExecutionId::new();
        let report = replayer
            .replay_from_snapshot(make_snapshot(
                "await_external_concurrent_timer_workflow",
                exec_id,
                events,
            ))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "ordering {label}: concurrent await+timer must replay deterministically, got: {report}"
        );
    }
}

#[tokio::test]
async fn replayer_detects_await_external_target_mismatch() {
    // A history recorded awaiting target X, replayed by a workflow that awaits a
    // DIFFERENT target Y (a non-deterministic code change) must be classified as
    // ExternalAwaitMismatch, not swallowed.
    let recorded_target = ExecutionId::new();
    let (exec_id, events) =
        await_resolved_history(recorded_target, serde_json::json!({ "ok": true }));
    // The workflow input names a DIFFERENT target than what history recorded.
    let different_target = ExecutionId::new();
    let mut snap = make_snapshot("await_external_workflow", exec_id, events);
    // Overwrite the WorkflowStarted input to name the different target.
    if let Some(WorkflowEvent::WorkflowStarted { input, .. }) = snap.events.first_mut() {
        *input = serde_json::json!({ "target": different_target.to_string() });
    }
    let replayer = build_replayer();
    let report = replayer.replay_from_snapshot(snap).await;
    let summary = report.to_string();
    match &report.status {
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            assert_eq!(
                *kind,
                NonDeterminismKind::ExternalAwaitMismatch,
                "a divergent awaited target must classify as ExternalAwaitMismatch, got: {summary}"
            );
        }
        other => panic!("expected NonDeterminismDetected(ExternalAwaitMismatch), got: {other:?}"),
    }
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
// (a4) Durable mutex (issue #691) replays deterministically. A granted lock is
//      anchored by a single `MutexGranted` event; release is event-less
//      bookkeeping. Replay recovers the guard from the recorded grant regardless
//      of which `lock_seq` was minted, and a key divergence / self-deadlock are
//      classified precisely.
// ---------------------------------------------------------------------------

/// History for a granted durable mutex on `key`: `[WorkflowStarted,
/// MutexGranted { key, lock_seq, acquired_at }]`. Release records no event.
fn mutex_grant_history(key: &str, lock_seq: i64) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MutexGranted {
            key: key.to_string(),
            lock_seq,
            acquired_at: Utc::now(),
        },
    ];
    (exec_id, events)
}

#[tokio::test]
async fn replay_workflow_with_mutex_grant_succeeds() {
    let (exec_id, events) = mutex_grant_history("k", 1);
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("mutex_grant_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a workflow that acquires a durable mutex must replay against its \
         recorded MutexGranted history (release is event-less bookkeeping), \
         got: {report}"
    );
    assert!(
        report.events_replayed > 0,
        "events_replayed must be positive"
    );
}

/// AC6 (issue #691): replay is deterministic across unbounded cycles regardless
/// of which fencing token (`lock_seq`) was minted at grant time. Build 1000
/// *distinct* fixtures, each varying the recorded `lock_seq` (and grant instant),
/// and assert every one replays `ReplaySucceeded`.
#[tokio::test]
async fn replay_workflow_with_mutex_grant_succeeds_across_unbounded_cycles() {
    let replayer = build_replayer();

    for i in 0..1000_usize {
        // Vary the recorded fencing token so no two fixtures are identical —
        // the workflow recovers whatever `lock_seq` the grant carried.
        let lock_seq = i64::try_from(i).unwrap_or(i64::MAX).wrapping_add(1);
        let (exec_id, events) = mutex_grant_history("k", lock_seq);

        let report = replayer
            .replay_from_snapshot(make_snapshot("mutex_grant_workflow", exec_id, events))
            .await;

        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "cycle {i} (lock_seq={lock_seq}): a granted mutex must replay \
             deterministically regardless of the recorded fencing token, \
             got: {report}"
        );
    }
}

#[tokio::test]
async fn replay_workflow_mutex_release_before_subsequent_event_succeeds() {
    // [WorkflowStarted, MutexGranted{k}, ActivityScheduled{step_one},
    //  ActivityCompleted{step_one}] — the workflow acquires, releases (no event),
    //  then schedules the activity whose events follow the grant. The event-less
    //  release must not perturb the positional match of the subsequent activity.
    let exec_id = ExecutionId::new();
    let step_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MutexGranted {
            key: "k".to_string(),
            lock_seq: 1,
            acquired_at: Utc::now(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: step_id,
            name: "step_one".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: step_id,
            output: serde_json::json!("done"),
        },
    ];
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "mutex_release_then_activity_workflow",
            exec_id,
            events,
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "an explicit mutex release before a subsequent recorded event (the \
         activity) must replay cleanly — the release is event-less bookkeeping, \
         got: {report}"
    );
}

#[tokio::test]
async fn replay_workflow_mutex_key_divergence_detected() {
    // The workflow acquires key "a", but the recorded history granted "b" — a
    // non-deterministic divergence classified as MutexGrantMismatch.
    let (exec_id, events) = mutex_grant_history("b", 1);
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("mutex_grant_key_a_workflow", exec_id, events))
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::MutexGrantMismatch,
                ..
            }
        ),
        "acquiring a different mutex key than the recorded grant must be \
         detected as MutexGrantMismatch, got: {report}"
    );
}

#[tokio::test]
async fn mutex_self_deadlock_is_checked_before_positional_match() {
    // Only ONE MutexGranted is recorded (a self-deadlock never records a second
    // grant). The workflow acquires "k" (matches the grant), then re-acquires
    // "k" — which must surface `MutexSelfDeadlock` SYNCHRONOUSLY, before the
    // positional history match, so the caught branch replays deterministically
    // rather than nd-blocking (issue #691 review, P1).
    let (exec_id, events) = mutex_grant_history("k", 1);
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "mutex_self_deadlock_caught_workflow",
            exec_id,
            events,
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a self-deadlock caught by the workflow must be raised before the \
         positional history match, so replay succeeds deterministically \
         instead of diverging, got: {report}"
    );
}

// ── ctx.race() replay-neutrality fixtures (issue #1126) ─────────────────────
// These prove the same-cycle fix does NOT change replay of RECORDED histories.
// They must pass BOTH before and after the fix — they are replay-neutrality
// coverage, NOT the RED reproducer (the RED case lives in context.rs and
// exercises the LIVE resolving cycle).

#[tokio::test]
async fn replay_race_with_in_flight_loser_and_follow_on_activity_succeeds() {
    // A recorded post-fix history: an activity race (2 branches, branch 0
    // "fetch_a" wins) followed by a follow-on activity ("next_step"). The loser
    // ("fetch_b") was scheduled AND started but never got a terminal live; its
    // synthetic `ActivityFailed` ("lost race to a sibling branch") is appended
    // by the winner cycle. This full-fidelity history must replay cleanly.
    let exec_id = ExecutionId::new();
    let winner_id = ActivityExecId::new();
    let loser_id = ActivityExecId::new();
    let next_id = ActivityExecId::new();
    let winner_output = serde_json::json!({"p": "a"});
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "race:1".to_string(),
            details: Value::from(2u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: winner_id,
            name: "fetch_a".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: loser_id,
            name: "fetch_b".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityStarted {
            activity_id: loser_id,
            worker_id: WorkerId::new("test-worker"),
        },
        WorkflowEvent::ActivityStarted {
            activity_id: winner_id,
            worker_id: WorkerId::new("test-worker"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: winner_id,
            output: winner_output.clone(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "race_winner:1".to_string(),
            details: Value::from(0u64),
        },
        // The follow-on activity, scheduled by the winner cycle.
        WorkflowEvent::ActivityScheduled {
            activity_id: next_id,
            name: "next_step".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        // The synthetic loser terminal, appended last by the winner cycle
        // (`apply_race_loser_cancellations`), interleaved before the follow-on
        // activity's own terminal.
        WorkflowEvent::ActivityFailed {
            activity_id: loser_id,
            error: "lost race to a sibling branch".to_string(),
            attempt: 1,
            error_type: "Error".to_string(),
            non_retryable: true,
            details: None,
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: next_id,
            output: serde_json::json!("done"),
        },
    ];
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot(
            "race_then_activity_workflow",
            exec_id,
            events,
        ))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1126: a recorded race (in-flight loser) followed by a follow-on \
         activity must replay with 100% fidelity, got: {report}"
    );
}

#[tokio::test]
async fn replay_race_ending_at_race_no_follow_on_succeeds() {
    // A pre-fix-shaped history where the workflow ENDS at the race (no follow-on
    // command). Branch 0 ("fetch_a") wins; the loser's synthetic `ActivityFailed`
    // is recorded BEFORE the winner marker (mirroring the existing
    // `race_activity_verifies_previously_recorded_winner_without_new_commands`
    // ordering) to prove that ordering also replays clean.
    let exec_id = ExecutionId::new();
    let winner_id = ActivityExecId::new();
    let loser_id = ActivityExecId::new();
    let winner_output = serde_json::json!({"p": "a"});
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "race:1".to_string(),
            details: Value::from(2u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: winner_id,
            name: "fetch_a".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: loser_id,
            name: "fetch_b".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityStarted {
            activity_id: loser_id,
            worker_id: WorkerId::new("test-worker"),
        },
        WorkflowEvent::ActivityStarted {
            activity_id: winner_id,
            worker_id: WorkerId::new("test-worker"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: winner_id,
            output: winner_output.clone(),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: loser_id,
            error: "lost race to a sibling branch".to_string(),
            attempt: 1,
            error_type: "Error".to_string(),
            non_retryable: true,
            details: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "race_winner:1".to_string(),
            details: Value::from(0u64),
        },
    ];
    let replayer = build_replayer();

    let report = replayer
        .replay_from_snapshot(make_snapshot("race_only_workflow", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1126: a race with an in-flight loser that ends at the race (no \
         follow-on command) must replay with 100% fidelity, got: {report}"
    );
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
// Deadline-aware continue-as-new (issue #772)
// ---------------------------------------------------------------------------

/// Build a history for `deadline_can_workflow` whose recorded wall clock has
/// consumed `consumed_secs` of the run's execution-timeout budget, then
/// continued-as-new. `should_continue_as_new()` records the `system_now()`
/// read as a `SideEffectRecorded{Now}` (because a deadline exists), and the
/// deadline branch forks a fresh run.
fn deadline_can_history(t0_millis: i64, consumed_secs: i64) -> Vec<WorkflowEvent> {
    let mut events = deadline_probe_history(t0_millis, consumed_secs);
    events.push(WorkflowEvent::WorkflowContinuedAsNew {
        new_exec_id: ExecutionId::new(),
        input: serde_json::json!({ "cycle": 1 }),
    });
    events
}

/// Build the common prefix `WorkflowStarted` + the single `system_now()`
/// capture that `should_continue_as_new()` records when a deadline exists (the
/// recorded wall clock is `t0 + consumed_secs`).
fn deadline_probe_history(t0_millis: i64, consumed_secs: i64) -> Vec<WorkflowEvent> {
    let t0 = chrono::DateTime::from_timestamp_millis(t0_millis).unwrap();
    let recorded_now = t0 + chrono::Duration::seconds(consumed_secs);
    vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({ "cycle": 0 }),
            timestamp: t0,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Now,
            name: Some(autumn_harvest::DEADLINE_PROBE_SIDE_EFFECT_NAME.to_string()),
            value: serde_json::json!(recorded_now.timestamp_millis()),
        },
    ]
}

/// Build a history for `deadline_branch_probe_workflow` whose recorded activity
/// pins the `should_continue_as_new()` decision: `checkpoint_now` when the run
/// was expected to trip, `keep_working` otherwise. A broken fraction comparison
/// schedules the *other* activity on replay and diverges — so a replay that
/// succeeds actually proves the decision matched, not merely that the workflow
/// completed.
fn deadline_branch_history(
    t0_millis: i64,
    consumed_secs: i64,
    expected_trip: bool,
) -> Vec<WorkflowEvent> {
    let mut events = deadline_probe_history(t0_millis, consumed_secs);
    let activity = if expected_trip {
        "checkpoint_now"
    } else {
        "keep_working"
    };
    let activity_id = ActivityExecId::new();
    events.push(WorkflowEvent::ActivityScheduled {
        activity_id,
        name: activity.into(),
        input: Value::Null,
        queue: "default".into(),
    });
    events.push(WorkflowEvent::ActivityCompleted {
        activity_id,
        output: serde_json::json!("done"),
    });
    events
}

/// Build a **pre-#772** history for `should_can_then_timer_workflow`: it fired a
/// durable timer but — recorded under the old binary — carries NO
/// `SideEffectRecorded{Now}` at the `should_continue_as_new()` call site.
fn pre_772_timer_history(t0_millis: i64) -> Vec<WorkflowEvent> {
    let t0 = chrono::DateTime::from_timestamp_millis(t0_millis).unwrap();
    vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({ "cycle": 0 }),
            timestamp: t0,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("renewal"),
            duration_secs: 3600,
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("renewal"),
        },
    ]
}

/// Build a **pre-#772** history for `should_can_then_user_now_then_timer_workflow`:
/// recorded under the old binary (which had no deadline read), it carries the
/// author's own `system_now()` capture (`{Now, name: None}`) at the
/// `should_continue_as_new()` call site, followed by the durable timer. The
/// probe must leave the user's `Now` for `ctx.system_now()`.
fn pre_772_user_now_timer_history(t0_millis: i64) -> Vec<WorkflowEvent> {
    let t0 = chrono::DateTime::from_timestamp_millis(t0_millis).unwrap();
    // The user's recorded wall clock, well within the 30s budget so that even
    // if the (buggy) probe were to consume it, the deadline branch would not
    // trip — isolating the failure to the stolen-`Now` divergence.
    let user_now = t0 + chrono::Duration::seconds(1);
    vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({ "cycle": 0 }),
            timestamp: t0,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Now,
            name: None,
            value: serde_json::json!(user_now.timestamp_millis()),
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("renewal"),
            duration_secs: 3600,
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("renewal"),
        },
    ]
}

/// AC5: a history that crossed the deadline replays to the same
/// `ContinueAsNew` command (`ReplaySucceeded`). The `execution_timeout` is
/// supplied to the replayer, so `should_continue_as_new()`'s deadline branch is
/// exercised deterministically against the recorded `system_now()` capture.
#[tokio::test]
async fn replay_deadline_crossed_history_yields_continue_as_new() {
    // 27s of a 30s budget consumed (0.9 ≥ 0.8) ⇒ deadline branch trips.
    let events = deadline_can_history(1_700_000_000_000, 27);
    let report = build_replayer()
        .with_execution_timeout(chrono::Duration::seconds(30))
        .replay_from_snapshot(make_snapshot(
            "deadline_can_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a deadline-crossed history must replay to ContinueAsNew, got: {report}"
    );
}

/// Issue #772 (Codex P2 — carry deadline metadata in JSON replays): a full
/// history export / `harvest-replay` JSON fixture for a deadline-aware workflow
/// must carry `execution_timeout` on the snapshot so the JSON path threads the
/// deadline budget. The metadata lives ONLY on the snapshot here — the replayer
/// has NO global `with_execution_timeout` — so `replay_from_json` must read it
/// from the JSON. Before the fix, `replay_from_snapshot` supplied only the
/// (absent) global timeout, so the recorded `SideEffectRecorded{Now}` deadline
/// probe was left unconsumed and the deadline-aware history false-reported
/// non-determinism.
#[tokio::test]
async fn json_replay_threads_snapshot_execution_timeout_for_deadline_history() {
    // 27s of a 30s budget consumed (0.9 ≥ 0.8) ⇒ the deadline branch tripped
    // (the history recorded a continue-as-new).
    let events = deadline_can_history(1_700_000_000_000, 27);
    let snapshot = HistorySnapshot {
        workflow_name: "deadline_can_workflow".to_string(),
        execution_id: ExecutionId::new(),
        events,
        context_headers: None,
        execution_timeout: Some(chrono::Duration::seconds(30)),
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
    };
    let json = serde_json::to_string(&snapshot).expect("snapshot serialises");
    // The exported JSON must carry the deadline budget at the top level so it
    // round-trips into a HistorySnapshot (and matches a HistoryExportDocument).
    assert!(
        json.contains("execution_timeout"),
        "snapshot JSON must serialise execution_timeout: {json}"
    );

    // No global with_execution_timeout — the budget must come from the JSON.
    let report = WorkflowReplayer::new()
        .register_fn("deadline_can_workflow", deadline_can_workflow)
        .replay_from_json(&json)
        .await
        .expect("snapshot JSON parses");
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a deadline-aware history whose execution_timeout is carried on the JSON \
         snapshot must replay cleanly via replay_from_json (not false-report \
         non-determinism), got: {report}"
    );
}

/// Issue #772 (Codex P2): backward compatibility. A snapshot JSON WITHOUT the
/// new deadline fields still deserializes (serde `default` ⇒ `None`) — a legacy
/// export produced before this field. Deserialization must succeed, and the
/// replayer's global `with_execution_timeout` still enables the deadline branch
/// (the documented fallback), so a deadline-aware history replays unchanged.
#[tokio::test]
async fn json_replay_legacy_snapshot_without_fields_falls_back_to_global_timeout() {
    let events = deadline_can_history(1_700_000_000_000, 27);
    let snapshot = HistorySnapshot {
        workflow_name: "deadline_can_workflow".to_string(),
        execution_id: ExecutionId::new(),
        events,
        context_headers: None,
        // Legacy snapshot: no deadline metadata. `skip_serializing_if` omits
        // both fields from the JSON, producing a byte-for-byte pre-#772 export.
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
    };
    let json = serde_json::to_string(&snapshot).expect("snapshot serialises");
    assert!(
        !json.contains("execution_timeout") && !json.contains("deadline_at"),
        "a None deadline field must be omitted from the JSON (legacy shape): {json}"
    );

    // Deserialization tolerates the absent fields; the global fallback enables
    // the deadline branch so the deadline-aware history replays cleanly.
    let report = WorkflowReplayer::new()
        .register_fn("deadline_can_workflow", deadline_can_workflow)
        .with_execution_timeout(chrono::Duration::seconds(30))
        .replay_from_json(&json)
        .await
        .expect("legacy snapshot JSON (no deadline fields) parses");
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a legacy snapshot without deadline fields must deserialize and replay \
         via the replayer's global with_execution_timeout fallback, got: {report}"
    );
}

/// The falsifiable bar for issue #772: the deadline-driven
/// `should_continue_as_new()` **decision** replays deterministically across
/// N = 1000 distinct fixtures with zero divergences — and each fixture pins the
/// decision, so a broken fraction comparison is caught rather than passing
/// regardless (Finding 3). Each iteration varies the frozen `system_now()`
/// capture and execution-timeout budget, and **mixes trip (consumed ≥ 0.85 of
/// budget) and not-yet (consumed ≤ 0.40 of budget) cases**, both well clear of
/// the 0.8 boundary so only a genuinely broken comparison flips them. The
/// recorded activity (`checkpoint_now` vs `keep_working`) fixes the expected
/// decision: a wrong decision schedules the other activity and diverges.
#[tokio::test]
async fn deadline_triggered_can_replays_deterministically_1000x() {
    let base = 1_600_000_000_000_i64;
    for i in 0..1000_i64 {
        let budget_secs = 20 + (i % 100);
        // Alternate trip / not-yet fixtures.
        let expected_trip = i % 2 == 0;
        let consumed_secs = if expected_trip {
            // ≥ 0.85 of budget consumed ⇒ trips at fraction 0.8.
            (budget_secs * 85) / 100 + 1
        } else {
            // ≤ 0.40 of budget consumed ⇒ does not trip at fraction 0.8.
            (budget_secs * 40) / 100
        };
        let t0_millis = base + i * 41_000;
        let events = deadline_branch_history(t0_millis, consumed_secs, expected_trip);
        let report = build_replayer()
            .with_execution_timeout(chrono::Duration::seconds(budget_secs))
            .replay_from_snapshot(make_snapshot(
                "deadline_branch_probe_workflow",
                ExecutionId::new(),
                events,
            ))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "deadline replay pass {i} (expected_trip={expected_trip}) must succeed with the \
             recorded decision, got: {report}"
        );
    }
}

/// Finding 1: a pre-#772 in-flight history (a fired durable timer, NO recorded
/// `SideEffectRecorded{Now}` at the `should_continue_as_new()` check) resumed
/// under the new deadline-aware binary must replay cleanly — NOT diverge /
/// nd-block — and must NOT emit a spurious `ContinueAsNew`. This is the
/// canonical `should_continue_as_new()`-then-`ctx.timer()` shape (the shipped
/// example's shape). Before the tolerant-clock-read fix, the deadline branch's
/// strict `system_now` hit the recorded `TimerStarted` at the cursor and
/// recorded a non-determinism error, wedging a healthy run on a routine upgrade.
#[tokio::test]
async fn should_continue_as_new_tolerates_pre_772_history_without_nd_block() {
    let events = pre_772_timer_history(1_700_000_000_000);
    let report = build_replayer()
        .with_execution_timeout(chrono::Duration::seconds(30))
        .replay_from_snapshot(make_snapshot(
            "should_can_then_timer_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a pre-#772 history must replay cleanly under the deadline-aware binary \
         (no divergence, no spurious ContinueAsNew), got: {report}"
    );
}

/// Migration-path bug fix (issue #772): a pre-#772 history where the
/// `should_continue_as_new()` check sits immediately before a **user**
/// `ctx.system_now()` call. The recorded `SideEffectRecorded{Now, name: None}`
/// at the cursor belongs to `ctx.system_now()`, NOT the engine's deadline
/// probe. Before the naming fix the tolerant matcher stole that `Now`,
/// advanced the cursor, and the subsequent `ctx.system_now()` diverged against
/// the following `TimerStarted` — reporting false non-determinism on upgrade.
/// The probe now records/matches under a reserved sentinel name, so the user's
/// `Now` is left untouched and the history replays cleanly.
#[tokio::test]
async fn deadline_probe_does_not_consume_a_user_system_now() {
    let events = pre_772_user_now_timer_history(1_700_000_000_000);
    let report = build_replayer()
        .with_execution_timeout(chrono::Duration::seconds(30))
        .replay_from_snapshot(make_snapshot(
            "should_can_then_user_now_then_timer_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the deadline probe must not consume the user's system_now() Now: a \
         pre-#772 history with a user Now at the check site must replay cleanly, \
         got: {report}"
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

/// Codex P2 (issue #768) — determinism gate for a DUPLICATE ACTIVE arm: calling
/// `start_timer("idle", 300)` twice with no cancel/reset between records exactly
/// ONE `TimerStarted` (the durable row is deduped). Replaying the fixed-worker
/// history `[WorkflowStarted, TimerStarted, TimerFired]` must succeed — the
/// second `start_timer` must be an idempotent no-op that does NOT re-run the
/// positional `match_timer_arm` against the trailing `TimerFired`. Without the
/// fix the second arm diverges (a timer mismatch for history this worker wrote).
#[tokio::test]
async fn duplicate_active_start_timer_replays_succeeded() {
    let events = vec![wf_started(), cancellable_ts(), cancellable_tf()];
    // Exactly ONE TimerStarted despite two start_timer calls (row dedup).
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
            .count(),
        1,
        "the fixed-worker history records exactly one TimerStarted"
    );
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "duplicate_active_start_timer_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a duplicate active start_timer replays cleanly (idempotent no-op), got: {report}"
    );
}

/// FIX 1 (Codex P2 round 5, issue #768) — a duplicate active `start_timer` with a
/// DIFFERENT duration preserves the RECORDED duration. History records exactly one
/// `TimerStarted(idle, 300)` (the second `start_timer(idle, 600)` is a no-op), and
/// on replay the handle must carry 300 — so the timer fires at the 300s deadline
/// (the virtual clock advances by 300, not 600) and the run resolves `Fired`. The
/// fixture emits the armed duration in its result; before the fix the handle
/// carried 600 (the durable row / clock would use 600 while history recorded 300).
#[tokio::test]
async fn duplicate_active_start_timer_diff_duration_preserves_recorded_duration() {
    let events = vec![wf_started(), cancellable_ts(), cancellable_tf()];
    // Exactly ONE TimerStarted, and its recorded duration is 300 (not 600).
    let timer_starteds: Vec<u64> = events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::TimerStarted { duration_secs, .. } => Some(*duration_secs),
            _ => None,
        })
        .collect();
    assert_eq!(
        timer_starteds,
        vec![300],
        "the fixed-worker history records exactly one TimerStarted(idle, 300)"
    );
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "duplicate_active_start_timer_diff_duration_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a duplicate active start_timer with a different duration replays cleanly, got: {report}"
    );
}

/// FIX 2 SOUNDNESS GATE (Codex P2 round 5, issue #768): the reordered code awaits
/// the timer BEFORE the activity, replayed against a history recording the
/// activity FIRST. The `await_fire` outcome scan must STOP at the unconsumed
/// `ActivityScheduled` so strict replay reports `NonDeterminismDetected` — NOT
/// `ReplaySucceeded`. Before the fix the scan skipped across the activity to claim
/// the trailing `TimerFired`, wrongly passing.
#[tokio::test]
async fn await_timer_before_activity_detects_command_reorder() {
    let work_id = ActivityExecId::new();
    let events = vec![
        wf_started(),
        cancellable_ts(),
        WorkflowEvent::ActivityScheduled {
            activity_id: work_id,
            name: "work".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: work_id,
            output: serde_json::json!("done"),
        },
        cancellable_tf(),
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("x"),
        },
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "timer_await_before_activity_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "awaiting the timer before an activity recorded first must be caught as \
         non-determinism (the outcome scan must not claim the trailing TimerFired \
         across the unconsumed ActivityScheduled), got: {report}"
    );
}

/// FIX 2 (Codex P2 round 5, issue #768): the LEGIT `arm→activity→await_fire` flow
/// still replays cleanly against the SAME history — the activity events are
/// consumed before `await_fire` runs, so the outcome scan crosses them via
/// `is_consumed` and reaches the `TimerFired`. Proves the soundness stop does not
/// break the legitimate ordering.
#[tokio::test]
async fn activity_then_await_timer_replays_succeeded() {
    let work_id = ActivityExecId::new();
    let events = vec![
        wf_started(),
        cancellable_ts(),
        WorkflowEvent::ActivityScheduled {
            activity_id: work_id,
            name: "work".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: work_id,
            output: serde_json::json!("done"),
        },
        cancellable_tf(),
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("x"),
        },
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "activity_then_timer_await_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the legitimate arm->activity->await_fire flow must still replay cleanly, got: {report}"
    );
}

/// FIX 1 SOUNDNESS GATE (Codex P2 round 6, issue #768): the reordered code cancels
/// the timer BEFORE the activity, replayed against a history recording the activity
/// FIRST. `match_timer_cancel`'s scan must STOP at the unconsumed
/// `ActivityScheduled` so strict replay reports `NonDeterminismDetected` — NOT
/// `ReplaySucceeded`. Before the fix the scan skipped across the activity to claim
/// the trailing `TimerCancelled`, wrongly passing.
#[tokio::test]
async fn cancel_timer_before_activity_detects_command_reorder() {
    let work_id = ActivityExecId::new();
    let events = vec![
        wf_started(),
        cancellable_ts(),
        WorkflowEvent::ActivityScheduled {
            activity_id: work_id,
            name: "work".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: work_id,
            output: serde_json::json!("done"),
        },
        cancellable_tc(),
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("x"),
        },
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancel_timer_before_activity_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "cancelling the timer before an activity recorded first must be caught as \
         non-determinism (the cancel scan must not claim the trailing TimerCancelled \
         across the unconsumed ActivityScheduled), got: {report}"
    );
}

/// FIX 1 LEGIT control (Codex P2 round 6, issue #768): the `arm→activity→cancel`
/// flow still replays cleanly against the SAME history — the activity events are
/// consumed before `cancel()` runs, so `match_timer_cancel`'s scan crosses them via
/// `is_consumed` and reaches the `TimerCancelled`. Proves the soundness stop does
/// not break the legitimate ordering.
#[tokio::test]
async fn activity_then_cancel_timer_replays_succeeded() {
    let work_id = ActivityExecId::new();
    let events = vec![
        wf_started(),
        cancellable_ts(),
        WorkflowEvent::ActivityScheduled {
            activity_id: work_id,
            name: "work".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: work_id,
            output: serde_json::json!("done"),
        },
        cancellable_tc(),
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("x"),
        },
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "activity_then_cancel_timer_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the legitimate arm->activity->cancel flow must still replay cleanly, got: {report}"
    );
}

/// Codex P2 round 10 SOUNDNESS GATE (issue #768): the reordered code cancels the
/// timer BEFORE arming it, replayed against the canonical
/// `[TimerStarted(idle), TimerCancelled(idle)]` history. `match_timer_cancel`'s
/// scan must STOP at the unconsumed SAME-id `TimerStarted` (its ordering anchor)
/// so strict replay reports `NonDeterminismDetected` — NOT `ReplaySucceeded`.
/// Before the fix the shared helper crossed a same-id `TimerStarted` transparently,
/// letting the cancel claim the trailing `TimerCancelled` and wrongly passing.
#[tokio::test]
async fn cancel_then_start_no_activity_detects_command_reorder() {
    let events = vec![
        wf_started(),
        cancellable_ts(),
        cancellable_tc(),
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("cancelled"),
        },
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancel_then_start_no_activity_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "cancelling a same-id timer before its arm must be caught as non-determinism \
         (the cancel scan must not claim the trailing TimerCancelled across the \
         unconsumed same-id TimerStarted anchor), got: {report}"
    );
}

/// Codex P2 round 10 LEGIT control (issue #768): the canonical `arm→cancel` order
/// (no intervening activity) must still replay cleanly against the same history —
/// `start_timer` consumes the `TimerStarted` at the cursor, then
/// `match_timer_cancel` claims the `TimerCancelled` directly. Proves the same-id
/// anchor stop does not break the legitimate ordering.
#[tokio::test]
async fn start_then_cancel_no_activity_replays_succeeded() {
    let events = vec![
        wf_started(),
        cancellable_ts(),
        cancellable_tc(),
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("cancelled"),
        },
    ];
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "start_then_cancel_no_activity_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the canonical arm->cancel flow (same id, no activity) must still replay \
         cleanly, got: {report}"
    );
}

/// FIX 2 (Codex P2 round 6, issue #768): the sliding-window / idle-session LOOP
/// that reuses the SAME timer id (arm; await→Fired; arm again — with a DIFFERENT
/// duration each iteration) must replay cleanly. Consuming a fire clears the Armed
/// logical state so each iteration records a FRESH `TimerStarted` (N `TimerStarted`
/// + N `TimerFired`, balanced). Before the fix the stale Armed state made the
/// second+ `start_timer` a duplicate no-op, leaving the recorded `TimerStarted`s
/// unconsumed → an early-completion divergence.
#[tokio::test]
async fn cancellable_timer_loop_reuse_replays_succeeded() {
    let iterations = 3u64;
    let input = serde_json::json!({ "iterations": iterations, "vary_duration": true });
    let mut events = vec![wf_started_with_input(input)];
    for i in 0..iterations {
        events.push(WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("idle"),
            duration_secs: 300 + i,
        });
        events.push(WorkflowEvent::TimerFired {
            timer_id: TimerId::new("idle"),
        });
    }
    let report = build_replayer()
        .replay_from_snapshot(make_snapshot(
            "cancellable_timer_loop_reuse_workflow",
            ExecutionId::new(),
            events,
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a loop reusing the same timer id (arm; await->Fired; re-arm with a fresh \
         duration) must record and consume a fresh TimerStarted per iteration, got: {report}"
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
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
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
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
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
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
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

/// A parked (unresolved) signal-or-deadline race: the timer is armed but
/// neither `SignalReceived` nor `TimerFired` is recorded yet. `signal_branch_fixture`
/// truncated to drop the resolution + completion.
fn signal_in_flight_fixture() -> Vec<WorkflowEvent> {
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
    ]
}

/// Lead 2 (issue #476 parity): a healthy in-flight signal-timeout workflow
/// sampled by the deploy replay canary reaches the recorded-history frontier
/// with the race still `InProgress` and then *suspends* — it must report
/// `ReplaySucceeded`, not a false non-determinism. Mirrors the child-timeout
/// twin's `child_timeout_in_flight_canary_replays_succeeded` and
/// `check_strict_replay_no_match`'s canary-at-frontier exception, which the
/// `InProgress` arm previously ignored.
#[tokio::test]
async fn signal_timeout_in_flight_canary_replays_succeeded() {
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn("signal_or_deadline", signal_or_deadline_workflow)
        .replay_canary_snapshot(make_snapshot(
            "signal_or_deadline",
            exec_id,
            signal_in_flight_fixture(),
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "an in-flight signal-timeout race at the history frontier must be a \
         healthy suspend in canary mode, not a false non-determinism:\n{report}"
    );
}

/// Lead 2 update-transparency guard (mirrors the child twin's
/// `child_timeout_in_flight_woken_by_update_canary_replays_succeeded`): an
/// in-flight signal-timeout race whose only trailing history is transparent
/// update events. `match_signal_or_timer`'s `InProgress` scan skips updates via
/// `scan_cursor += 1` WITHOUT consuming them, leaving the matcher cursor BEFORE
/// them, so a raw `position() >= len()` frontier check reads `false` for a
/// perfectly healthy suspend. The `at_frontier` check must use
/// `has_non_lifecycle_unconsumed`, which treats update (and terminal-lifecycle)
/// events as transparent, agreeing with the global unconsumed check.
fn signal_in_flight_woken_by_update_fixture() -> Vec<WorkflowEvent> {
    let update_id = UpdateId::new();
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
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: "poke".to_string(),
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::UpdateCompleted {
            update_id,
            output: Value::Null,
        },
    ]
}

#[tokio::test]
async fn signal_timeout_in_flight_woken_by_update_canary_replays_succeeded() {
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn("signal_or_deadline", signal_or_deadline_workflow)
        .replay_canary_snapshot(make_snapshot(
            "signal_or_deadline",
            exec_id,
            signal_in_flight_woken_by_update_fixture(),
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "an in-flight signal-timeout race whose only trailing history is \
         transparent update events must still be a healthy suspend in canary \
         mode, not a false non-determinism:\n{report}"
    );
}

/// Regression guard: the canary-at-frontier exception must NOT weaken STRICT
/// (non-canary) replay. `WorkflowReplayer::replay_from_events` runs strict
/// replay, where an unresolved race at the end of a fixture is still a fixture
/// problem and must report non-determinism.
#[tokio::test]
async fn signal_timeout_in_flight_strict_still_reports_nondeterminism() {
    let report = WorkflowReplayer::new()
        .register_fn("signal_or_deadline", signal_or_deadline_workflow)
        .replay_from_events(signal_in_flight_fixture())
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "strict (non-canary) replay of an unresolved race must still be a \
         non-determinism error — the canary exception must not weaken it:\n{report}"
    );
}

/// Mandatory guardrail: the canary-at-frontier exception must ONLY cover a
/// genuinely-suspending in-flight race. A real code-vs-history divergence
/// (here the recorded deadline timer has a different duration than the handler
/// arms) resolves as `Diverged`, never `InProgress`, so it must STILL be
/// reported as non-determinism even in canary mode.
#[tokio::test]
async fn signal_timeout_genuine_divergence_still_detected_in_canary() {
    // The handler arms `__signal_timeout:1:approval` with a 300s deadline;
    // history records 60s — a genuine duration divergence.
    let divergent = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("__signal_timeout:1:approval"),
            duration_secs: 60,
        },
    ];
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn("signal_or_deadline", signal_or_deadline_workflow)
        .replay_canary_snapshot(make_snapshot("signal_or_deadline", exec_id, divergent))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "a genuine mid-history divergence must still be detected in canary mode \
         — the frontier exception must not mask it:\n{report}"
    );
}

// ---------------------------------------------------------------------------
// issue #1048 (signal side): the InProgress canary-frontier guard false-fires
// when the parked race has an interleaved *bookkeeping sibling* command.
//
// `match_signal_or_timer` rewinds the matcher cursor to the sibling's
// unconsumed `SideEffectRecorded` (an interleaved-command allowlist member) so
// the sibling's own matcher can consume it next. The inline guard reads
// `has_non_lifecycle_unconsumed()` at that rewound (non-frontier) cursor —
// BEFORE the joined `side_effect` future is polled — so it observes a
// transient "not at frontier" and (in canary) fires a false non-determinism.
// A healthy parked workflow is thus reported `NonDeterminismDetected` instead
// of `ReplaySucceeded`.
// ---------------------------------------------------------------------------

/// Reachable #1048 trigger: `tokio::join!(wait_for_signal_timeout, side_effect)`.
/// `side_effect` lowers to `RecordSideEffect`, so the suspension batch is the
/// allowlisted `[StartTimer, WaitForSignal, RecordSideEffect]` (accepted by
/// `extract_started_timer_for_suspension`) — a genuine parked history a canary
/// samples, unlike the mixed `[StartTimer, WaitForSignal, ScheduleActivity]`
/// batch the worker rejects.
fn signal_or_deadline_with_bookkeeping_sibling_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let (decision, _seed) = tokio::join!(
            ctx.wait_for_signal_timeout("approval", std::time::Duration::from_secs(300)),
            async { ctx.side_effect("nm", || 1_u64).unwrap_or(0) },
        );
        let decision = decision.map_err(|e| e.to_string())?;
        Ok(decision.map_or_else(
            || serde_json::json!({"escalated": true}),
            |payload| serde_json::json!({"approved": payload}),
        ))
    })
}

/// Parked signal-timeout race + a trailing interleaved bookkeeping sibling
/// (`SideEffectRecorded`). The armed timer is at idx 1, the sibling at idx 2;
/// neither `SignalReceived` nor `TimerFired` is recorded. This is exactly the
/// history the `join!` workflow above records when it first parks.
fn signal_in_flight_with_bookkeeping_sibling_fixture() -> Vec<WorkflowEvent> {
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
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Custom,
            name: Some("nm".to_string()),
            value: serde_json::json!(1),
        },
    ]
}

/// AC1 (signal): a canary replay of a producible parked history where the
/// signal-timeout race is in-flight concurrently with an interleaved
/// bookkeeping sibling must report `ReplaySucceeded`, not
/// `NonDeterminismDetected`. RED until the inline `InProgress` guard is removed.
#[tokio::test]
async fn signal_timeout_in_flight_with_bookkeeping_sibling_canary_replays_succeeded() {
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn(
            "signal_or_deadline_sibling",
            signal_or_deadline_with_bookkeeping_sibling_workflow,
        )
        .replay_canary_snapshot(make_snapshot(
            "signal_or_deadline_sibling",
            exec_id,
            signal_in_flight_with_bookkeeping_sibling_fixture(),
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1048: an in-flight signal-timeout race joined with an allowlisted \
         bookkeeping sibling (side_effect) must be a healthy suspend in canary \
         mode, not a false non-determinism from the rewound-cursor guard:\n{report}"
    );
}

/// No-over-suppression guardrail for issue #1048 (the black-hat risk): removing
/// the inline `InProgress` guard must NOT let a GENUINE non-determinism that
/// surfaces as `InProgress`-with-unconsumed-events pass as `ReplaySucceeded`
/// under canary. This is distinct from the `Diverged` case
/// (`signal_timeout_genuine_divergence_still_detected_in_canary`): here the race
/// matcher resolves to `InProgress` and rewinds the cursor to a trailing
/// interleaved-command event, but the workflow (`signal_or_deadline_workflow`,
/// which only does the wait and NEVER schedules an activity) has no sibling that
/// consumes it — so it is a genuine leftover. The executor's end-of-cycle
/// authority (`history_has_unconsumed_events`, executor.rs:1183) must still
/// catch it as `NonDeterminismDetected`, proving the deferral does not blind the
/// canary to genuinely-unconsumed history.
#[tokio::test]
async fn signal_timeout_in_flight_genuine_leftover_event_still_detected_in_canary() {
    let orphan = vec![
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
        // Genuine leftover: an interleaved-command-allowlist event the matcher
        // rewinds to on `InProgress`, but which the plain wait workflow never
        // dispatches — so nothing consumes it and it remains at the frontier.
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "orphaned_activity".into(),
            input: Value::Null,
            queue: "default".into(),
        },
    ];
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn("signal_or_deadline", signal_or_deadline_workflow)
        .replay_canary_snapshot(make_snapshot("signal_or_deadline", exec_id, orphan))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "issue #1048: an in-flight signal-timeout race with a genuinely-unconsumed \
         trailing event must STILL be a non-determinism error under canary — the \
         end-of-cycle authority must catch what the removed inline guard used to:\n{report}"
    );
}

// ---------------------------------------------------------------------------
// execute_child_workflow_timeout — child-or-deadline race (issue #779)
// ---------------------------------------------------------------------------

/// Awaits a child workflow with a deadline, then branches on the outcome. A
/// child failure before the deadline propagates as an Err (mapped to a String).
fn child_or_deadline_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let outcome = ctx
            .spawn_child_workflow_timeout(
                "process_order",
                serde_json::json!({"id": 42}),
                std::time::Duration::from_secs(300),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(outcome.map_or_else(
            || serde_json::json!({"timed_out": true}),
            |output| serde_json::json!({"child": output}),
        ))
    })
}

fn child_timeout_started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

/// Child completed before the deadline fired.
fn child_win_fixture() -> Vec<WorkflowEvent> {
    let child_id = ExecutionId::new();
    let timer_id = TimerId::new("__child_timeout:1:process_order");
    vec![
        child_timeout_started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".to_string(),
            input: serde_json::json!({"id": 42}),
        },
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 300,
        },
        WorkflowEvent::ChildWorkflowCompleted {
            child_id,
            output: serde_json::json!({"ok": true}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"child": {"ok": true}}),
        },
    ]
}

/// Deadline timer fired first; the loser child was then cancelled (a synthetic
/// `ChildWorkflowFailed` terminal recorded after the fire).
fn timer_win_fixture() -> Vec<WorkflowEvent> {
    let child_id = ExecutionId::new();
    let timer_id = TimerId::new("__child_timeout:1:process_order");
    vec![
        child_timeout_started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".to_string(),
            input: serde_json::json!({"id": 42}),
        },
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 300,
        },
        WorkflowEvent::TimerFired { timer_id },
        WorkflowEvent::ChildWorkflowFailed {
            child_id,
            error: "lost race to a sibling branch".to_string(),
            error_type: None,
            details: None,
            non_retryable: None,
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"timed_out": true}),
        },
    ]
}

#[tokio::test]
async fn child_timeout_child_win_replays_succeeded() {
    let report = WorkflowReplayer::new()
        .register_fn("child_or_deadline", child_or_deadline_workflow)
        .replay_from_events(child_win_fixture())
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "child-win branch must replay:\n{report}"
    );
}

#[tokio::test]
async fn child_timeout_timer_win_replays_succeeded() {
    let report = WorkflowReplayer::new()
        .register_fn("child_or_deadline", child_or_deadline_workflow)
        .replay_from_events(timer_win_fixture())
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "timer-win branch (with the sealed loser child terminal) must replay:\n{report}"
    );
}

#[tokio::test]
async fn child_timeout_both_branches_replay_succeeded_across_randomized_orderings() {
    // Issue #779 success metric (parallel to #476): a fixture exercising both
    // branches replays with ReplaySucceeded 100% of the time across 1,000
    // randomized orderings. Winner is decided strictly by recorded history
    // index, never wall-clock — so a deterministic pick per iteration must
    // always replay clean.
    let mut seed: u64 = 0x5DEE_CE66;
    for i in 0..1_000 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let events = if seed & 1 == 0 {
            child_win_fixture()
        } else {
            timer_win_fixture()
        };

        let report = WorkflowReplayer::new()
            .register_fn("child_or_deadline", child_or_deadline_workflow)
            .replay_from_events(events)
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "iteration {i} must replay:\n{report}"
        );
    }
}

/// A child that FAILED before the deadline replays deterministically to the
/// Err branch: the workflow maps the typed failure to a String and returns it
/// via `?`, so — like every self-failing workflow — it surfaces as
/// `ReplayStatus::WorkflowFailed` (it did not complete successfully), mirroring
/// `replay_typed_workflow_failed_round_trips_with_identical_typed_fields`. The
/// *determinism* is the point: the same failure is reproduced on every cycle.
#[tokio::test]
async fn child_timeout_child_fails_before_deadline_replays_deterministically() {
    let child_id = ExecutionId::new();
    let timer_id = TimerId::new("__child_timeout:1:process_order");
    let events = vec![
        child_timeout_started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".to_string(),
            input: serde_json::json!({"id": 42}),
        },
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 300,
        },
        WorkflowEvent::ChildWorkflowFailed {
            child_id,
            error: "child-workflow:process_order: downstream 503".to_string(),
            error_type: Some("UpstreamUnavailable".to_string()),
            details: None,
            non_retryable: Some(true),
        },
        WorkflowEvent::WorkflowFailed {
            error: "child-workflow:process_order: downstream 503".to_string(),
            error_type: None,
            details: None,
            non_retryable: None,
        },
    ];

    let mut reproduced = Vec::new();
    for _ in 0..2 {
        let report = WorkflowReplayer::new()
            .register_fn("child_or_deadline", child_or_deadline_workflow)
            .replay_from_events(events.clone())
            .await;
        let ReplayStatus::WorkflowFailed { error, .. } = report.status else {
            panic!("a child failure before the deadline surfaces as WorkflowFailed:\n{report}");
        };
        reproduced.push(error);
    }
    assert_eq!(
        reproduced[0], reproduced[1],
        "the child failure must reproduce byte-identically across replay cycles"
    );
}

/// A still-running child-timeout workflow: the child started and the deadline
/// timer is armed, but neither has resolved yet. This is the exact shape of a
/// live execution parked on `spawn_child_workflow_timeout` — the history ends
/// at the recorded frontier with the race `InProgress`.
fn child_in_flight_fixture() -> Vec<WorkflowEvent> {
    let child_id = ExecutionId::new();
    let timer_id = TimerId::new("__child_timeout:1:process_order");
    vec![
        child_timeout_started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".to_string(),
            input: serde_json::json!({"id": 42}),
        },
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 300,
        },
    ]
}

/// Codex P2 (issue #779): a healthy in-flight child-timeout workflow sampled by
/// the deploy replay canary reaches the recorded-history frontier with the race
/// still `InProgress` and then *suspends* — it must report `ReplaySucceeded`,
/// not a false non-determinism. Mirrors `check_strict_replay_no_match`'s
/// canary-at-frontier exception, which the `InProgress` arm previously ignored.
#[tokio::test]
async fn child_timeout_in_flight_canary_replays_succeeded() {
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn("child_or_deadline", child_or_deadline_workflow)
        .replay_canary_snapshot(make_snapshot(
            "child_or_deadline",
            exec_id,
            child_in_flight_fixture(),
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "an in-flight child-timeout race at the history frontier must be a \
         healthy suspend in canary mode, not a false non-determinism:\n{report}"
    );
}

/// Codex P2 round 10 (issue #779): an in-flight child-timeout workflow that was
/// woken by an update (`UpdateAdmitted`/`UpdateCompleted` appended AFTER the
/// armed race, at the recorded-history frontier). Update events are transparent
/// to the global unconsumed-history check (`has_non_lifecycle_unconsumed`), but
/// `match_child_or_timer`'s `InProgress` scan skips them via `scan_cursor += 1`
/// WITHOUT consuming them, so the matcher cursor is left positioned BEFORE the
/// trailing updates. A raw `position() >= len()` frontier check therefore reads
/// `false` (cursor < len), defeating the canary-at-frontier suppression and
/// producing a FALSE non-determinism for a perfectly healthy suspend. The
/// frontier check must treat trailing transparent update events the same way the
/// global unconsumed check does.
fn child_in_flight_woken_by_update_fixture() -> Vec<WorkflowEvent> {
    let child_id = ExecutionId::new();
    let timer_id = TimerId::new("__child_timeout:1:process_order");
    let update_id = UpdateId::new();
    vec![
        child_timeout_started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".to_string(),
            input: serde_json::json!({"id": 42}),
        },
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 300,
        },
        // Woken by an update while the race is still pending — transparent to
        // replay, but they sit at the frontier AFTER the armed race.
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: "poke".to_string(),
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::UpdateCompleted {
            update_id,
            output: Value::Null,
        },
    ]
}

#[tokio::test]
async fn child_timeout_in_flight_woken_by_update_canary_replays_succeeded() {
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn("child_or_deadline", child_or_deadline_workflow)
        .replay_canary_snapshot(make_snapshot(
            "child_or_deadline",
            exec_id,
            child_in_flight_woken_by_update_fixture(),
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "an in-flight child-timeout race whose only trailing history is \
         transparent update events must still be a healthy suspend in canary \
         mode, not a false non-determinism:\n{report}"
    );
}

/// Regression guard for the fix above: the canary exception must NOT weaken
/// STRICT (non-canary) replay. `WorkflowReplayer::replay_from_events` runs
/// strict replay, where an unresolved race at the end of a fixture is still a
/// fixture problem and must report non-determinism.
#[tokio::test]
async fn child_timeout_in_flight_strict_still_reports_nondeterminism() {
    let report = WorkflowReplayer::new()
        .register_fn("child_or_deadline", child_or_deadline_workflow)
        .replay_from_events(child_in_flight_fixture())
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "strict (non-canary) replay of an unresolved race must still be a \
         non-determinism error — the canary exception must not weaken it:\n{report}"
    );
}

/// Mandatory guardrail: the canary-at-frontier exception must ONLY cover a
/// genuinely-suspending in-flight race. A real code-vs-history divergence (here
/// the handler passes a different child input than history recorded) resolves
/// as `Diverged`, never `InProgress`, so it must STILL be reported as
/// non-determinism even in canary mode.
#[tokio::test]
async fn child_timeout_genuine_divergence_still_detected_in_canary() {
    let child_id = ExecutionId::new();
    let timer_id = TimerId::new("__child_timeout:1:process_order");
    // Recorded child input `{"id": 7}` diverges from the handler's `{"id": 42}`.
    let divergent = vec![
        child_timeout_started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".to_string(),
            input: serde_json::json!({"id": 7}),
        },
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 300,
        },
    ];
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn("child_or_deadline", child_or_deadline_workflow)
        .replay_canary_snapshot(make_snapshot("child_or_deadline", exec_id, divergent))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "a genuine mid-history divergence must still be detected in canary mode \
         — the frontier exception must not mask it:\n{report}"
    );
}

// ---------------------------------------------------------------------------
// issue #1048 (child side, AC3 twin): the byte-identical InProgress
// canary-frontier guard in `spawn_child_workflow_timeout` false-fires the same
// way when the parked child-timeout race has an interleaved bookkeeping sibling
// (`match_child_or_timer` rewinds the cursor to the sibling's
// `SideEffectRecorded`, and the guard reads `has_non_lifecycle_unconsumed()`
// there before the sibling is polled).
// ---------------------------------------------------------------------------

/// Reachable #1048 trigger for the child twin:
/// `tokio::join!(spawn_child_workflow_timeout, side_effect)`. The suspension
/// batch is the allowlisted `[StartChildWorkflow, StartTimer, RecordSideEffect]`.
fn child_or_deadline_with_bookkeeping_sibling_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let (outcome, _seed) = tokio::join!(
            ctx.spawn_child_workflow_timeout(
                "process_order",
                serde_json::json!({"id": 42}),
                std::time::Duration::from_secs(300),
            ),
            async { ctx.side_effect("nm", || 1_u64).unwrap_or(0) },
        );
        let outcome = outcome.map_err(|e| e.to_string())?;
        Ok(outcome.map_or_else(
            || serde_json::json!({"timed_out": true}),
            |output| serde_json::json!({"child": output}),
        ))
    })
}

/// Parked child-timeout race + a trailing interleaved bookkeeping sibling
/// (`SideEffectRecorded`): child started (idx 1), timer armed (idx 2), sibling
/// recorded (idx 3); neither `ChildWorkflow*` terminal nor `TimerFired` is
/// recorded.
fn child_in_flight_with_bookkeeping_sibling_fixture() -> Vec<WorkflowEvent> {
    let child_id = ExecutionId::new();
    let timer_id = TimerId::new("__child_timeout:1:process_order");
    vec![
        child_timeout_started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".to_string(),
            input: serde_json::json!({"id": 42}),
        },
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 300,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: autumn_harvest::SideEffectKind::Custom,
            name: Some("nm".to_string()),
            value: serde_json::json!(1),
        },
    ]
}

/// AC1 + AC3 (child): a canary replay of a producible parked history where the
/// child-timeout race is in-flight concurrently with an interleaved bookkeeping
/// sibling must report `ReplaySucceeded`. RED until the twin's inline `InProgress`
/// guard is removed symmetrically with the signal side.
#[tokio::test]
async fn child_timeout_in_flight_with_bookkeeping_sibling_canary_replays_succeeded() {
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn(
            "child_or_deadline_sibling",
            child_or_deadline_with_bookkeeping_sibling_workflow,
        )
        .replay_canary_snapshot(make_snapshot(
            "child_or_deadline_sibling",
            exec_id,
            child_in_flight_with_bookkeeping_sibling_fixture(),
        ))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1048: an in-flight child-timeout race joined with an allowlisted \
         bookkeeping sibling (side_effect) must be a healthy suspend in canary \
         mode, not a false non-determinism from the rewound-cursor guard:\n{report}"
    );
}

/// No-over-suppression guardrail for issue #1048 — CHILD twin of
/// `signal_timeout_in_flight_genuine_leftover_event_still_detected_in_canary`
/// (AC3 symmetry). Removing the byte-identical inline `InProgress` canary-frontier
/// guard from `spawn_child_workflow_timeout` must NOT let a GENUINE
/// non-determinism that surfaces as `InProgress`-with-unconsumed-events pass as
/// `ReplaySucceeded` under canary. Distinct from the `Diverged` case
/// (`child_timeout_genuine_divergence_still_detected_in_canary`): here
/// `match_child_or_timer` resolves the race to `InProgress` and rewinds the
/// cursor to a trailing interleaved-command-allowlist event (`ActivityScheduled`,
/// replay.rs:4407), but the plain workflow (`child_or_deadline_workflow`, which
/// only does the child-timeout wait and NEVER schedules an activity) has no
/// sibling that consumes it — so it is a genuine leftover. The executor's
/// end-of-cycle authority (`history_has_unconsumed_events`, executor.rs:1183)
/// must still catch it as `NonDeterminismDetected`, proving the deferral to that
/// authority does not blind the canary to genuinely-unconsumed history on the
/// child matcher's distinct rewind path.
#[tokio::test]
async fn child_timeout_in_flight_genuine_leftover_event_still_detected_in_canary() {
    let child_id = ExecutionId::new();
    let timer_id = TimerId::new("__child_timeout:1:process_order");
    let orphan = vec![
        child_timeout_started(),
        // Input matches the handler's dispatched `{"id": 42}` so the race
        // resolves `InProgress`, NOT `Diverged` — testing the leftover path.
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "process_order".to_string(),
            input: serde_json::json!({"id": 42}),
        },
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 300,
        },
        // Genuine leftover: an interleaved-command-allowlist event the matcher
        // rewinds to on `InProgress`, but which the plain child-timeout workflow
        // never dispatches — so nothing consumes it and it remains at the frontier.
        WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "orphaned_activity".into(),
            input: Value::Null,
            queue: "default".into(),
        },
    ];
    let exec_id = ExecutionId::new();
    let report = WorkflowReplayer::new()
        .register_fn("child_or_deadline", child_or_deadline_workflow)
        .replay_canary_snapshot(make_snapshot("child_or_deadline", exec_id, orphan))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "issue #1048 (child twin): an in-flight child-timeout race with a \
         genuinely-unconsumed trailing event must STILL be a non-determinism \
         error under canary — the end-of-cycle authority must catch what the \
         removed inline guard used to, on the child matcher's rewind path:\n{report}"
    );
}

/// A child-timeout whose serialized input exceeds the workflow-input cap on a
/// FRESH dispatch, PROPAGATING the resulting `PayloadTooLarge` via `?`. The
/// live over-cap run records no child/timer events — the child was never
/// dispatched — so the terminal history is just `WorkflowFailed`.
fn child_timeout_oversized_propagate_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // >2 MiB (DEFAULT_MAX_WORKFLOW_INPUT_BYTES) — exceeds the replay cap.
        let oversized = serde_json::json!({ "data": "x".repeat(3 * 1024 * 1024) });
        let outcome = ctx
            .spawn_child_workflow_timeout(
                "process_order",
                oversized,
                std::time::Duration::from_secs(300),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(outcome.map_or_else(
            || serde_json::json!({"timed_out": true}),
            |output| serde_json::json!({"child": output}),
        ))
    })
}

/// Same fresh over-cap child-timeout, but CATCHES the `PayloadTooLarge` and
/// degrades gracefully, then completes. The live run records no child/timer
/// events, so the terminal history is `WorkflowCompleted` — on replay the
/// cursor sits on that event, NOT a recorded child start.
fn child_timeout_oversized_catch_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let oversized = serde_json::json!({ "data": "x".repeat(3 * 1024 * 1024) });
        let outcome = match ctx
            .spawn_child_workflow_timeout(
                "process_order",
                oversized,
                std::time::Duration::from_secs(300),
            )
            .await
        {
            Ok(o) => o,
            // Degrade gracefully on an oversized sub-orchestration input; any
            // OTHER error (e.g. a genuine non-determinism) still propagates, so
            // a spurious `Diverged` from mis-ordering the cap check would NOT be
            // swallowed and this workflow would fail instead of completing.
            Err(HarvestError::PayloadTooLarge { .. }) => {
                return Ok(serde_json::json!({"skipped_oversized": true}));
            }
            Err(other) => return Err(other.to_string()),
        };
        Ok(outcome.map_or_else(
            || serde_json::json!({"timed_out": true}),
            |output| serde_json::json!({"child": output}),
        ))
    })
}

/// Codex P2 (issue #779): a fresh child-timeout over-cap input records NO
/// child/timer events, so on replay the cursor sits on the next real event
/// (here a caught-and-continued `WorkflowCompleted`). The payload-cap pre-check
/// must run BEFORE `match_child_or_timer`, so replay reproduces the same
/// `PayloadTooLarge` (which the workflow catches and continues) instead of
/// diverging against the non-child event at the cursor. Before the fix this
/// history replayed as a non-determinism / `WorkflowFailed`.
#[tokio::test]
async fn child_timeout_oversized_input_caught_replays_succeeded() {
    let events = vec![
        child_timeout_started(),
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"skipped_oversized": true}),
        },
    ];
    let report = WorkflowReplayer::new()
        .register_fn("child_or_deadline", child_timeout_oversized_catch_workflow)
        .replay_from_events(events)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a caught cap-failure child-timeout history must replay cleanly — the \
         cap pre-check must reproduce PayloadTooLarge before the matcher \
         diverges on the WorkflowCompleted at the cursor:\n{report}"
    );
}

/// Companion to the catch case: a fresh over-cap child-timeout that PROPAGATES
/// the `PayloadTooLarge` records only a terminal `WorkflowFailed`. On replay the
/// cursor sits on that `WorkflowFailed`; the cap pre-check must reproduce the
/// same `PayloadTooLarge` deterministically rather than a spurious
/// non-determinism. Mirrors
/// `child_timeout_child_fails_before_deadline_replays_deterministically`.
#[tokio::test]
async fn child_timeout_oversized_input_propagated_replays_deterministically() {
    let events = vec![
        child_timeout_started(),
        WorkflowEvent::WorkflowFailed {
            error: "child workflow input payload too large".to_string(),
            error_type: None,
            details: None,
            non_retryable: None,
        },
    ];

    let mut reproduced = Vec::new();
    for _ in 0..2 {
        let report = WorkflowReplayer::new()
            .register_fn(
                "child_or_deadline",
                child_timeout_oversized_propagate_workflow,
            )
            .replay_from_events(events.clone())
            .await;
        let ReplayStatus::WorkflowFailed { error, .. } = report.status else {
            panic!(
                "a propagated cap failure must surface as WorkflowFailed, not a \
                 spurious non-determinism:\n{report}"
            );
        };
        assert!(
            error.contains("too large") || error.to_lowercase().contains("payload"),
            "the reproduced error must be the PayloadTooLarge, not a \
             non-determinism message: {error}"
        );
        assert!(
            !error.to_lowercase().contains("non-deterministic")
                && !error.contains("mismatch")
                && !error.contains("ChildWorkflowStarted"),
            "the cap failure must NOT be masked by a history-matcher divergence: \
             {error}"
        );
        reproduced.push(error);
    }
    assert_eq!(
        reproduced[0], reproduced[1],
        "the cap failure must reproduce byte-identically across replay cycles"
    );
}

/// #779 (Codex round-12 P2): a workflow that CATCHES an oversized child-timeout
/// `PayloadTooLarge` (records NOTHING) and then IMMEDIATELY dispatches a SECOND
/// child-timeout (recorded). The first (caught, seq 1) call's fresh-dispatch peek
/// must be fingerprinted by its OWN timer id (`__child_timeout:1:oversized_child`)
/// so it is never miscredited the SECOND (seq 2) call's `ChildWorkflowStarted` at
/// the cursor.
///
/// RED (pre-fix, unfingerprinted `peek_child_start_at_cursor`): the first call's
/// peek matched the second call's `ChildWorkflowStarted`, concluded it was already
/// dispatched, skipped its own cap re-check, then diverged in
/// `match_child_or_timer` (recorded `second_child` != requested `oversized_child`)
/// — a spurious non-determinism instead of reproducing `PayloadTooLarge`, so the
/// workflow failed. GREEN (timer-id fingerprint): the caught call reproduces
/// `PayloadTooLarge`, the second call resolves child-win, the run completes.
fn child_timeout_caught_then_second_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Call 1 (seq 1): oversized input → PayloadTooLarge, CAUGHT, records
        // nothing.
        let oversized = serde_json::json!({ "data": "x".repeat(3 * 1024 * 1024) });
        match ctx
            .spawn_child_workflow_timeout(
                "oversized_child",
                oversized,
                std::time::Duration::from_secs(300),
            )
            .await
        {
            Ok(_) => {
                return Err("expected PayloadTooLarge on the oversized child".to_string());
            }
            Err(HarvestError::PayloadTooLarge { .. }) => { /* degrade gracefully */ }
            // Any OTHER error (e.g. a spurious Diverged from the peek miscrediting
            // the second call's child start) propagates and fails the run — so the
            // RED state is NOT ReplaySucceeded.
            Err(other) => return Err(other.to_string()),
        }
        // Call 2 (seq 2): small input, recorded, child wins before the deadline.
        let outcome = ctx
            .spawn_child_workflow_timeout(
                "second_child",
                serde_json::json!({"id": 7}),
                std::time::Duration::from_secs(300),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(outcome.map_or_else(
            || serde_json::json!({"timed_out": true}),
            |output| serde_json::json!({"second": output}),
        ))
    })
}

#[tokio::test]
async fn child_timeout_caught_oversized_then_second_child_replays_succeeded() {
    // The caught first call (seq 1) burns its sequence number, so the recorded
    // SECOND call is seq 2 — its deadline timer id is `__child_timeout:2:...`.
    let child_id = ExecutionId::new();
    let timer_id = TimerId::new("__child_timeout:2:second_child");
    let events = vec![
        child_timeout_started(),
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "second_child".to_string(),
            input: serde_json::json!({"id": 7}),
        },
        WorkflowEvent::TimerStarted {
            timer_id,
            duration_secs: 300,
        },
        WorkflowEvent::ChildWorkflowCompleted {
            child_id,
            output: serde_json::json!({"ok": true}),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"second": {"ok": true}}),
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn(
            "child_or_deadline",
            child_timeout_caught_then_second_child_workflow,
        )
        .replay_from_events(events)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a caught oversized child-timeout followed by a SECOND recorded \
         child-timeout must replay cleanly — the caught call's fresh-dispatch peek \
         is fingerprinted by its own timer id, so it is never miscredited the \
         second call's ChildWorkflowStarted at the cursor (issue #779 Codex \
         round-12 P2):\n{report}"
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

// ── signal.unhandled is never emitted by the replay path (issue #684, AC2) ──

/// Counts `record_signal_unhandled` calls.
#[derive(Default)]
struct SignalUnhandledCounter {
    unhandled: std::sync::atomic::AtomicU64,
}

impl autumn_harvest::telemetry::MetricsRecorder for SignalUnhandledCounter {
    fn record_signal_unhandled(&self, _workflow_name: &str, _queue: &str) {
        self.unhandled
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// A workflow that consumes a single "go" signal and returns — so the recorded
/// history it produces replays cleanly (the signal is consumed at replay).
fn signal_wait_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _ = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// AC2 (issue #684): a `WorkflowReplayer` replay of an already-terminal history
/// drives the strict/canary path (`run_workflow_strict`/`run_workflow_canary`),
/// NOT the live `drive_workflow` where `harvest.signal.unhandled` is emitted, so
/// re-replaying a terminal history emits ZERO unhandled-signal samples — the
/// counter can never double-count on replay by construction.
#[tokio::test]
async fn replay_of_terminal_history_never_emits_signal_unhandled() {
    let metrics = std::sync::Arc::new(SignalUnhandledCounter::default());
    let replayer = WorkflowReplayer::new()
        .register_fn("signal_wait_workflow", signal_wait_workflow)
        .with_metrics(metrics.clone());

    let exec_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SignalReceived {
            signal_name: "go".into(),
            payload: Value::Null,
        },
    ];

    // Replay several cycles: the signal is consumed each time (ReplaySucceeded),
    // and the unhandled counter never increments on any replay cycle.
    for cycle in 0..5 {
        let report = replayer
            .replay_from_snapshot(make_snapshot(
                "signal_wait_workflow",
                exec_id,
                events.clone(),
            ))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "cycle {cycle}: consuming-signal workflow must replay cleanly, got: {report}"
        );
    }
    assert_eq!(
        metrics.unhandled.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the replay path must never emit harvest.signal.unhandled (AC2: no double-count on replay)"
    );
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

// ---------------------------------------------------------------------------
// (issue #698 FIX 1) parent_execution_id must be threaded into the replay /
//   canary tooling. `parent_execution_id` lives in NO WorkflowEvent (it is
//   sourced only from the harvest_workflow_executions.parent_id column), so a
//   pure-history replay builds the child's context with parent = None UNLESS the
//   replayer is told the parent. A child that branches its COMMAND stream on
//   ctx.info().parent_execution_id (a blessed AC2 "parent-aware child logic"
//   use) then false-reports non-determinism in the deploy canary / its own
//   WorkflowReplayer CI test. WorkflowReplayer::with_parent_execution_id closes
//   that gap for the JSON-fixture path.
// ---------------------------------------------------------------------------

/// A child workflow whose COMMAND stream depends on its spawning parent:
/// schedules `child_of_parent` when it HAS a parent, `orphan_step` when it does
/// not. The scheduled activity NAME is the observable divergence.
fn parent_branching_child<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let step = if ctx.info().parent_execution_id.is_some() {
            "child_of_parent"
        } else {
            "orphan_step"
        };
        let out = ctx
            .execute_activity_raw(step, Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// History recorded by a run of `parent_branching_child` that HAD a parent
/// (`P`): it scheduled and completed the `child_of_parent` activity.
fn parent_taken_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let act_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act_id,
            name: "child_of_parent".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act_id,
            output: serde_json::json!("ok"),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("ok"),
        },
    ];
    (exec_id, events)
}

/// (a) With the spawning parent supplied via `with_parent_execution_id`, the
/// child's parent-taken branch matches the recorded history → `ReplaySucceeded`.
#[tokio::test]
async fn parent_aware_child_replays_clean_when_parent_is_threaded() {
    let parent = ExecutionId::new();
    let (exec_id, events) = parent_taken_history();

    let report = WorkflowReplayer::new()
        .register_fn("parent_branching_child", parent_branching_child)
        .with_parent_execution_id(Some(parent))
        .replay_from_snapshot(make_snapshot("parent_branching_child", exec_id, events))
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a parent-aware child must replay clean when the parent is threaded, got: {report}"
    );
}

/// (b) WITHOUT the parent, the child takes its `orphan_step` branch, diverging
/// from the recorded `child_of_parent` schedule → `NonDeterminismDetected`. This
/// proves the threading is load-bearing: dropping `with_parent_execution_id`
/// would surface exactly this false non-determinism in the canary / CI.
#[tokio::test]
async fn parent_aware_child_diverges_when_parent_is_not_threaded() {
    let (exec_id, events) = parent_taken_history();

    let report = WorkflowReplayer::new()
        .register_fn("parent_branching_child", parent_branching_child)
        // Deliberately DO NOT call with_parent_execution_id — parent = None.
        .replay_from_snapshot(make_snapshot("parent_branching_child", exec_id, events))
        .await;

    match &report.status {
        ReplayStatus::NonDeterminismDetected { .. } => {}
        other => panic!(
            "without a threaded parent the child must diverge (orphan_step vs recorded \
             child_of_parent); got: {other:?}\nreport: {report}"
        ),
    }
}

/// (c) Issue #698 (Codex P2 @ testing.rs:306) — the DB `export_history` ->
/// `HistoryExportDocument` -> JSON -> `replay_from_json` path (retention
/// archives / offline replay). The `HistoryExportRequest` carries the row's
/// `parent_id`, which `export_history` embeds at the TOP LEVEL of the document
/// so it deserialises into `HistorySnapshot.parent_execution_id`. The child
/// then replays clean through `replay_from_json` WITHOUT any manual
/// `with_parent_execution_id` override — proving the parent survives the
/// export-document round-trip, exactly like `execution_timeout`/`deadline_at`.
#[tokio::test]
async fn parent_aware_child_replays_clean_through_export_document_round_trip() {
    use autumn_harvest::history_export::{
        HistoryExportRequest, HistoryPayloadPolicy, export_history,
    };

    let parent = ExecutionId::new();
    let (exec_id, events) = parent_taken_history();

    // Build the DB-export request the way retention / the HTTP export route
    // does, sourcing the parent from the row's `parent_id` column.
    let document = export_history(HistoryExportRequest {
        workflow_name: "parent_branching_child".to_string(),
        execution_id: exec_id,
        shard_id: 0,
        state: "COMPLETED".to_string(),
        events,
        exported_at: Utc::now(),
        payload_policy: HistoryPayloadPolicy::Full,
        max_bytes: Some(64 * 1024),
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: Some(parent),
        workflow_id: None,
    })
    .expect("full export should fit under the limit");
    let json = serde_json::to_string(&document).expect("export serialises");

    // Replay the exported JSON directly — NO with_parent_execution_id override.
    let report = WorkflowReplayer::new()
        .register_fn("parent_branching_child", parent_branching_child)
        .replay_from_json(&json)
        .await
        .expect("exported document must be accepted by replay_from_json");

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a parent-aware child exported through the DB export document must replay \
         clean via replay_from_json with no manual parent override, got: {report}"
    );
}

/// (d) The negative control for (c): when the export request carries no parent
/// (`parent_execution_id: None`), the exported document deserialises to
/// parent = `None`, so the child takes its `orphan_step` branch and diverges
/// from the recorded `child_of_parent` schedule — proving the export document
/// is the load-bearing carrier of the parent through this path.
#[tokio::test]
async fn parent_aware_child_diverges_through_export_document_without_parent() {
    use autumn_harvest::history_export::{
        HistoryExportRequest, HistoryPayloadPolicy, export_history,
    };

    let (exec_id, events) = parent_taken_history();

    let document = export_history(HistoryExportRequest {
        workflow_name: "parent_branching_child".to_string(),
        execution_id: exec_id,
        shard_id: 0,
        state: "COMPLETED".to_string(),
        events,
        exported_at: Utc::now(),
        payload_policy: HistoryPayloadPolicy::Full,
        max_bytes: Some(64 * 1024),
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
    })
    .expect("full export should fit under the limit");
    let json = serde_json::to_string(&document).expect("export serialises");

    let report = WorkflowReplayer::new()
        .register_fn("parent_branching_child", parent_branching_child)
        .replay_from_json(&json)
        .await
        .expect("exported document must be accepted by replay_from_json");

    match &report.status {
        ReplayStatus::NonDeterminismDetected { .. } => {}
        other => panic!(
            "an export document without a parent must replay the orphan branch and diverge; \
             got: {other:?}\nreport: {report}"
        ),
    }
}

// ---------------------------------------------------------------------------
// (issue #698 FIX 2) workflow_id / workflow_type must be threaded into EVERY
//   replay path. `ctx.info().workflow_type` (= the workflow_name column /
//   handler key) and `ctx.info().workflow_id` (the business id column) live in
//   NO WorkflowEvent — the LIVE worker sets them on the context via span_meta,
//   but the strict/canary/export/JSON replay paths build the context without
//   them, so a pure-history replay reports "" for both. A workflow that branches
//   command-affecting logic on either — or, as here, embeds them in an activity
//   INPUT (which the ctx.info() docs promise is replay-safe) — then false-reports
//   non-determinism under export/DB/canary/JSON replay. These tests are the
//   falsifiable bar: pre-fix they RED (divergence: recorded input carries the
//   real identity, replay computes ""), post-fix they GREEN.
//
//   `workflow_type` uses mechanism 1 (apply the already-carried workflow_name);
//   `workflow_id` uses mechanism 2 (an added HistorySnapshot/HistoryExportDocument
//   field). The strict-input round-trip below is simultaneously the DIRECT
//   assertion the census calls for: the activity input IS `ctx.info()`, so a clean
//   strict replay proves the replay-path context reports the real values, not "".
// ---------------------------------------------------------------------------

/// A workflow that embeds its own `ctx.info().workflow_type` and
/// `ctx.info().workflow_id` into the input of a single activity. Under STRICT
/// replay (the `WorkflowReplayer` default) the emitted activity input is compared
/// byte-for-byte against the recorded `ActivityScheduled.input`, so a replay that
/// computed the wrong (empty) identity diverges. This is the strongest, most
/// direct assertion that the replay-path context reports the real identity.
fn identity_into_activity_input<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let info = ctx.info();
        let payload = serde_json::json!({
            "workflow_type": info.workflow_type,
            "workflow_id": info.workflow_id,
        });
        let out = ctx
            .execute_activity_raw("record_identity", payload, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// History recorded by a live run of `identity_into_activity_input`: the
/// `record_identity` activity input carries the REAL `(workflow_type, workflow_id)`
/// the live context observed via `span_meta`.
fn identity_taken_history(wf_type: &str, wf_id: &str) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let act_id = ActivityExecId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act_id,
            name: "record_identity".into(),
            input: serde_json::json!({
                "workflow_type": wf_type,
                "workflow_id": wf_id,
            }),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act_id,
            output: serde_json::json!("ok"),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("ok"),
        },
    ];
    (exec_id, events)
}

/// (e) THE MONEY TEST. A history recorded under a real workflow type
/// (`identity_wf`) and business id (`cart-42`) is exported through the DB export
/// document (the way retention / the HTTP export route does) and replayed via
/// `replay_from_json` with NO manual override. The export document carries
/// `workflow_name` (→ `workflow_type`, mechanism 1) and the new `workflow_id`
/// field (mechanism 2), so both round-trip into the replay context and the strict
/// activity-input comparison passes → `ReplaySucceeded`. Pre-fix the replay
/// computed `("", "")` and diverged.
#[tokio::test]
async fn workflow_type_and_id_replay_clean_through_export_document_round_trip() {
    use autumn_harvest::history_export::{
        HistoryExportRequest, HistoryPayloadPolicy, export_history,
    };

    let (exec_id, events) = identity_taken_history("identity_wf", "cart-42");

    let document = export_history(HistoryExportRequest {
        workflow_name: "identity_wf".to_string(),
        // Issue #698: the business id rides the export document (mechanism 2).
        workflow_id: Some("cart-42".to_string()),
        execution_id: exec_id,
        shard_id: 0,
        state: "COMPLETED".to_string(),
        events,
        exported_at: Utc::now(),
        payload_policy: HistoryPayloadPolicy::Full,
        max_bytes: Some(64 * 1024),
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
    })
    .expect("full export should fit under the limit");
    let json = serde_json::to_string(&document).expect("export serialises");

    // Replay the exported JSON directly — NO manual override of type/id.
    let report = WorkflowReplayer::new()
        .register_fn("identity_wf", identity_into_activity_input)
        .replay_from_json(&json)
        .await
        .expect("exported document must be accepted by replay_from_json");

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a workflow that embeds ctx.info().workflow_type / workflow_id in an \
         activity input must replay clean through the export document round-trip \
         (both identity fields threaded), got: {report}"
    );
}

/// (f) The negative control for `workflow_id` (mechanism 2): when the export
/// request carries no `workflow_id`, the exported document deserialises to
/// `workflow_id = ""`, so the replayed activity input `{"workflow_id": ""}`
/// diverges from the recorded `"cart-42"` — proving the export document's
/// `workflow_id` field is the load-bearing carrier through this path.
#[tokio::test]
async fn workflow_id_diverges_through_export_document_when_id_is_dropped() {
    use autumn_harvest::history_export::{
        HistoryExportRequest, HistoryPayloadPolicy, export_history,
    };

    let (exec_id, events) = identity_taken_history("identity_wf", "cart-42");

    let document = export_history(HistoryExportRequest {
        workflow_name: "identity_wf".to_string(),
        // Deliberately drop the business id: the recorded input still says
        // "cart-42", so replay must diverge.
        workflow_id: None,
        execution_id: exec_id,
        shard_id: 0,
        state: "COMPLETED".to_string(),
        events,
        exported_at: Utc::now(),
        payload_policy: HistoryPayloadPolicy::Full,
        max_bytes: Some(64 * 1024),
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
    })
    .expect("full export should fit under the limit");
    let json = serde_json::to_string(&document).expect("export serialises");

    let report = WorkflowReplayer::new()
        .register_fn("identity_wf", identity_into_activity_input)
        .replay_from_json(&json)
        .await
        .expect("exported document must be accepted by replay_from_json");

    match &report.status {
        ReplayStatus::NonDeterminismDetected { .. } => {}
        other => panic!(
            "an export document without a workflow_id must diverge (recorded \
             \"cart-42\" vs replayed \"\"); got: {other:?}\nreport: {report}"
        ),
    }
}

/// (g) `workflow_type` load-bearing proof for the raw-events path (mechanism 1):
/// `replay_from_events` applies the single registered handler's KEY as the
/// context's `workflow_type`. The recorded activity input carries that same type
/// (and an empty id, since a raw-events fixture has no business id), so a clean
/// strict replay proves the handler key is applied as `ctx.info().workflow_type`
/// rather than left "".
#[tokio::test]
async fn workflow_type_is_applied_from_handler_key_on_raw_events_path() {
    let (_exec_id, events) = identity_taken_history("identity_wf", "");

    let report = WorkflowReplayer::new()
        .register_fn("identity_wf", identity_into_activity_input)
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the handler key must be applied as ctx.info().workflow_type on the \
         raw-events path, got: {report}"
    );
}

/// (h) `workflow_id` global-override proof (mechanism 2, raw-events path):
/// `WorkflowReplayer::with_workflow_id` supplies the business id a raw-events
/// fixture cannot carry, so a workflow that embeds it in an activity input
/// replays clean.
#[tokio::test]
async fn workflow_id_is_applied_from_replayer_global_on_raw_events_path() {
    let (_exec_id, events) = identity_taken_history("identity_wf", "cart-99");

    let report = WorkflowReplayer::new()
        .register_fn("identity_wf", identity_into_activity_input)
        .with_workflow_id("cart-99")
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "with_workflow_id must thread the business id into the raw-events replay \
         context, got: {report}"
    );
}

// ---------------------------------------------------------------------------
// (issue #698, Codex P2 @ testing.rs:903) execution_id must be threadable into
//   the raw `replay_from_events` path. `ctx.info().execution_id` is documented
//   as replay-safe (the production / DB / snapshot / canary replay paths all
//   recover the real id), but a raw-events fixture carries no HistorySnapshot to
//   source it from, so `replay_from_events` mints a FRESH id. A workflow that
//   recorded its own `ctx.info().execution_id` in a command-affecting value
//   (here, an activity INPUT — which the docs promise is replay-safe) then
//   false-reports non-determinism: the random replay id vs the recorded original
//   run id. `WorkflowReplayer::with_execution_id` closes that last raw-path gap.
// ---------------------------------------------------------------------------

/// A workflow that embeds its own `ctx.info().execution_id` into the input of a
/// single activity. Under STRICT replay the emitted input is compared
/// byte-for-byte against the recorded `ActivityScheduled.input`, so a replay that
/// computed a DIFFERENT `execution_id` diverges — the strongest, most direct
/// assertion that the raw-path context reports the supplied id.
fn exec_id_into_activity_input<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let info = ctx.info();
        let payload = serde_json::json!({ "execution_id": info.execution_id.to_string() });
        let out = ctx
            .execute_activity_raw("record_exec_id", payload, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// History recorded by a live run of `exec_id_into_activity_input`: the
/// `record_exec_id` activity input carries the REAL `execution_id` the live
/// context observed.
fn exec_id_taken_history(exec_id: ExecutionId) -> Vec<WorkflowEvent> {
    let act_id = ActivityExecId::new();
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act_id,
            name: "record_exec_id".into(),
            input: serde_json::json!({ "execution_id": exec_id.to_string() }),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act_id,
            output: serde_json::json!("ok"),
        },
        WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!("ok"),
        },
    ]
}

/// (i) THE MONEY TEST for the raw-events `execution_id` gap: with the captured run
/// id supplied via `with_execution_id`, the replayed activity input matches the
/// recorded one → `ReplaySucceeded`. Pre-fix `replay_from_events` minted a fresh
/// random id and this diverged.
#[tokio::test]
async fn execution_id_is_applied_from_replayer_global_on_raw_events_path() {
    let exec_id = ExecutionId::new();
    let events = exec_id_taken_history(exec_id);

    let report = WorkflowReplayer::new()
        .register_fn("exec_id_wf", exec_id_into_activity_input)
        .with_execution_id(exec_id)
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "with_execution_id must thread the captured run id into the raw-events \
         replay context, got: {report}"
    );
}

/// (j) The load-bearing negative control for (i): WITHOUT `with_execution_id`,
/// `replay_from_events` mints a fresh random id, so the replayed activity input
/// carries a DIFFERENT `execution_id` than the recorded one → divergence. This is
/// exactly the false non-determinism the builder closes.
#[tokio::test]
async fn execution_id_diverges_on_raw_events_path_without_the_builder() {
    let exec_id = ExecutionId::new();
    let events = exec_id_taken_history(exec_id);

    let report = WorkflowReplayer::new()
        .register_fn("exec_id_wf", exec_id_into_activity_input)
        // Deliberately DO NOT call with_execution_id — a fresh random id is minted.
        .replay_from_events(events)
        .await;

    match &report.status {
        ReplayStatus::NonDeterminismDetected { .. } => {}
        other => panic!(
            "without a threaded execution_id the raw-events replay mints a fresh id \
             and must diverge from the recorded one; got: {other:?}\nreport: {report}"
        ),
    }
}

// ===========================================================================
// Issue #1071 — interleaved-sibling terminal tolerance (RED phase).
//
// Four forward-scan matchers (`match_timer_strict`, `scan_activity_terminal`,
// `match_signal`) falsely report `NonDeterminismDetected` when a *sibling*
// terminal/command is interleaved at its recorded history position — even for an
// IDEAL (correctly-ordered) history — because each scan `break`s at the sibling
// event instead of tolerating it the way `match_signal_or_timer` (issue #476)
// already does.
//
// Each test below feeds the replayer a correct, producible history and asserts
// `ReplaySucceeded`. They are RED against current code: each currently fails
// (the run wedges → `NonDeterminismDetected` / `EarlyCompletion`). The later
// GREEN phase makes each scan tolerate the interleaved sibling.
// ===========================================================================

/// Manifestation #1 — a mixed suspension batch that emits the **timer command
/// before the activity command** (`tokio::join!(ctx.timer(...), activity)`, the
/// timer polled first). Its natural, ideal history has `TimerStarted` first and
/// the activity draining to `ActivityCompleted` before the timer's `TimerFired`
/// at the tail. `match_timer_strict`'s forward `TimerFired` scan currently
/// **breaks at the unconsumed `ActivityScheduled`** (replay.rs:2966) → wedge.
fn interleaved_timer_first_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // `tokio::join!` polls in listed order — the timer command is emitted
        // FIRST, then the activity. Both must resolve for the workflow to return.
        let (timer_res, activity_res) = tokio::join!(
            ctx.timer("wait", 5),
            ctx.execute_activity_raw("work", Value::Null, "default"),
        );
        timer_res.map_err(|e| e.to_string())?;
        let out = activity_res.map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// Manifestation #2 — an **activity-first** mixed batch
/// (`tokio::join!(activity, ctx.timer(...))`) whose sibling timer fires BEFORE
/// the activity completes, so the recorded terminal order is
/// `…, TimerFired, ActivityCompleted`. `scan_activity_terminal`'s forward scan
/// currently **breaks at the unconsumed `TimerFired`** (replay.rs:1204) → the
/// activity wedges `ActivityInProgress`.
fn interleaved_activity_first_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Activity command emitted FIRST, then the timer.
        let (activity_res, timer_res) = tokio::join!(
            ctx.execute_activity_raw("work", Value::Null, "default"),
            ctx.timer("wait", 5),
        );
        let out = activity_res.map_err(|e| e.to_string())?;
        timer_res.map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// Manifestation #4 — two timers armed in one batch
/// (`tokio::join!(ctx.timer("timer_a", …), ctx.timer("timer_b", …))`, `timer_a`
/// polled/matched first) firing in REVERSED order: the sibling (foreign-id)
/// `TimerFired(timer_b)` is recorded before the polled timer's own
/// `TimerFired(timer_a)`. `match_timer_strict`'s scan for `TimerFired(timer_a)`
/// currently **breaks at the unconsumed foreign `TimerFired(timer_b)`** → wedge.
fn interleaved_two_timers_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let (ra, rb) = tokio::join!(ctx.timer("timer_a", 5), ctx.timer("timer_b", 5));
        ra.map_err(|e| e.to_string())?;
        rb.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

fn interleaved_started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

/// #1 fixture: timer-first schedule order; the activity drains and completes
/// before the timer fires. Ideal, producible, correctly-ordered history.
fn interleaved_timer_first_history() -> Vec<WorkflowEvent> {
    let work_id = ActivityExecId::new();
    let timer_id = TimerId::new("wait");
    vec![
        interleaved_started(),
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 5,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: work_id,
            name: "work".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: work_id,
            output: serde_json::json!("done"),
        },
        WorkflowEvent::TimerFired { timer_id },
    ]
}

/// #2 fixture: activity-first schedule order; the sibling timer FIRES before
/// the activity's terminal (`TimerFired` recorded before `ActivityCompleted`).
fn interleaved_timer_before_activity_terminal_history() -> Vec<WorkflowEvent> {
    let work_id = ActivityExecId::new();
    let timer_id = TimerId::new("wait");
    vec![
        interleaved_started(),
        WorkflowEvent::ActivityScheduled {
            activity_id: work_id,
            name: "work".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 5,
        },
        WorkflowEvent::TimerFired { timer_id },
        WorkflowEvent::ActivityCompleted {
            activity_id: work_id,
            output: serde_json::json!("done"),
        },
    ]
}

/// #4 fixture: two timers armed in arm order (`timer_a` then `timer_b`) but firing
/// in REVERSED order (`TimerFired(timer_b)` before `TimerFired(timer_a)`).
fn interleaved_two_timers_reversed_fire_history() -> Vec<WorkflowEvent> {
    let a = TimerId::new("timer_a");
    let b = TimerId::new("timer_b");
    vec![
        interleaved_started(),
        WorkflowEvent::TimerStarted {
            timer_id: a.clone(),
            duration_secs: 5,
        },
        WorkflowEvent::TimerStarted {
            timer_id: b.clone(),
            duration_secs: 5,
        },
        WorkflowEvent::TimerFired { timer_id: b },
        WorkflowEvent::TimerFired { timer_id: a },
    ]
}

/// #3 fixture: a plain `wait_for_signal("go")` whose recorded history carries a
/// sibling deadline timer that fired before the signal
/// (`TimerStarted(__signal_timeout:0:go), TimerFired(__signal_timeout:0:go),
/// SignalReceived(go)`). `match_signal`'s scan currently **stops at the
/// unconsumed `TimerFired`** → `NonDeterminismDetected(EarlyCompletion,
/// event_index=1)`, though the "go" signal WAS delivered. Empirically-proven
/// case from issue #1071 comment 3.
fn interleaved_signal_after_deadline_history() -> Vec<WorkflowEvent> {
    let timer_id = TimerId::new("__signal_timeout:0:go");
    vec![
        interleaved_started(),
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 300,
        },
        WorkflowEvent::TimerFired { timer_id },
        WorkflowEvent::SignalReceived {
            signal_name: "go".into(),
            payload: serde_json::json!({"ok": true}),
        },
    ]
}

#[tokio::test]
async fn interleaved_sibling_timer_first_mixed_batch_replays_succeeded() {
    let report = WorkflowReplayer::new()
        .register_fn("interleaved_timer_first", interleaved_timer_first_workflow)
        .replay_from_events(interleaved_timer_first_history())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1071 manifestation #1: an IDEAL timer-first mixed batch \
         (TimerStarted, ActivityScheduled, ActivityCompleted, TimerFired) must \
         replay — match_timer_strict's TimerFired scan must tolerate the \
         interleaved ActivityScheduled instead of breaking:\n{report}"
    );
}

#[tokio::test]
async fn interleaved_sibling_timer_fires_before_activity_terminal_replays_succeeded() {
    let report = WorkflowReplayer::new()
        .register_fn(
            "interleaved_activity_first",
            interleaved_activity_first_workflow,
        )
        .replay_from_events(interleaved_timer_before_activity_terminal_history())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1071 manifestation #2: a sibling timer firing before the \
         activity's terminal (…, TimerFired, ActivityCompleted) must replay — \
         scan_activity_terminal must tolerate the interleaved TimerFired \
         instead of breaking:\n{report}"
    );
}

#[tokio::test]
async fn interleaved_sibling_signal_after_deadline_timer_fired_replays_succeeded() {
    // Plain `wait_for_signal("go")` (signal_wait_workflow) with a sibling
    // deadline timer's TimerFired recorded before the delivered signal.
    let report = WorkflowReplayer::new()
        .register_fn("signal_wait_workflow", signal_wait_workflow)
        .replay_from_events(interleaved_signal_after_deadline_history())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1071 manifestation #3: a plain wait_for_signal whose history \
         carries an already-fired sibling deadline timer before the delivered \
         signal must replay — match_signal's SignalReceived scan must cross the \
         interleaved TimerFired instead of stopping:\n{report}"
    );
}

/// Two CONCURRENT, DIFFERENTLY-named signal waits in one mixed batch: a plain
/// `wait_for_signal("a")` (polled FIRST) joined with a
/// `receive_signal_timeout::<Value>("b", 5s)` whose deadline fires BEFORE "a"
/// arrives. `receive_signal_timeout` pre-increments `signal_timeout_seq`, so the
/// race arms `__signal_timeout:1:b`; the recorded history is
/// `[WorkflowStarted, TimerStarted(__signal_timeout:1:b),
/// TimerFired(__signal_timeout:1:b), SignalReceived(a)]` (b times out, then a
/// arrives). This is the Codex P1 composition on PR #1084.
fn interleaved_two_signal_foreign_deadline_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // "a" (plain wait) is polled FIRST, so `match_signal("a")` runs before
        // the sibling race's `match_signal_or_timer("b")` during replay.
        let (a_res, b_res) = tokio::join!(
            ctx.wait_for_signal("a"),
            ctx.receive_signal_timeout::<Value>("b", std::time::Duration::from_secs(5)),
        );
        a_res.map_err(|e| e.to_string())?;
        let b: Option<Value> = b_res.map_err(|e| e.to_string())?;
        // Original run: "a" arrived, "b"'s deadline fired first => None.
        Ok(serde_json::json!({ "b_timed_out": b.is_none() }))
    })
}

fn interleaved_two_signal_foreign_deadline_history() -> Vec<WorkflowEvent> {
    // seq pre-increments (context.rs `wait_for_signal_timeout_with_timer_id`),
    // so the first race id is `__signal_timeout:1:b`.
    let timer_id = TimerId::new("__signal_timeout:1:b");
    vec![
        interleaved_started(),
        WorkflowEvent::TimerStarted {
            timer_id: timer_id.clone(),
            duration_secs: 5,
        },
        WorkflowEvent::TimerFired { timer_id },
        WorkflowEvent::SignalReceived {
            signal_name: "a".into(),
            payload: serde_json::json!({ "ok": true }),
        },
    ]
}

#[tokio::test]
async fn interleaved_two_signal_race_foreign_deadline_timer_replays_succeeded() {
    // Codex P1 (PR #1084 review of issue #1071): `match_signal("a")`'s
    // reserved-`__signal_timeout` cross-without-rewind arm keyed only on the
    // reserved PREFIX, so it crossed a FOREIGN signal's deadline timer
    // (`__signal_timeout:1:b`) without rewinding — advancing the cursor past the
    // sibling race's `TimerStarted`/`TimerFired`, so the "b" branch's
    // `match_signal_or_timer` could no longer positionally re-anchor its own
    // `TimerStarted` and strict replay diverged (`NonDeterminismDetected`,
    // `EarlyCompletion`). The guard is now narrowed to the SAME signal name, so a
    // foreign deadline timer falls through to the generic `TimerStarted` rewind
    // arm and the sibling race re-matches.
    let report = WorkflowReplayer::new()
        .register_fn(
            "interleaved_two_signal_foreign_deadline",
            interleaved_two_signal_foreign_deadline_workflow,
        )
        .replay_from_events(interleaved_two_signal_foreign_deadline_history())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "Codex P1 (#1071/#1084): two concurrent differently-named signal waits \
         where a foreign signal's deadline timer fired before the awaited signal \
         arrived must replay — match_signal must only cross THIS signal's own \
         reserved deadline timer without rewind, leaving a foreign one for its \
         sibling race:\n{report}"
    );
}

#[tokio::test]
async fn interleaved_sibling_multi_timer_reversed_fire_order_replays_succeeded() {
    let report = WorkflowReplayer::new()
        .register_fn("interleaved_two_timers", interleaved_two_timers_workflow)
        .replay_from_events(interleaved_two_timers_reversed_fire_history())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1071 manifestation #4: two timers armed in one batch firing in \
         reversed order (TimerFired(b) before TimerFired(a), matching a first) \
         must replay — match_timer_strict must cross the foreign TimerFired \
         non-consumingly instead of breaking:\n{report}"
    );
}

// ---------------------------------------------------------------------------
// Genuine-divergence regression guards (must PASS on current code; the #1071
// fix must NOT over-swallow these). Paired with the pre-existing
// `replay_activity_history_for_timer_workflow_detects_timer_mismatch`
// (timer positional-anchor mismatch), which likewise stays green.
// ---------------------------------------------------------------------------

/// Signal-site park-forever guard (issue #768 round 13, preserved by #1071):
/// a plain `wait_for_signal("go")` whose history ends in a STRAY unconsumed
/// `TimerStarted` with NO matching `TimerFired` and NO signal must still be a
/// divergence — pushing a `WaitForSignal` command here would park the run
/// forever on a signal that will never arrive. The #1071 TimerFired-tolerance
/// fix must not suppress this (there is no `TimerFired` to cross here).
#[tokio::test]
async fn interleaved_sibling_signal_stray_timer_started_still_diverges() {
    let events = vec![
        interleaved_started(),
        // Stray, unconsumed: no matching TimerFired, no SignalReceived follows.
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("orphan"),
            duration_secs: 5,
        },
    ];
    let report = WorkflowReplayer::new()
        .register_fn("signal_wait_workflow", signal_wait_workflow)
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "a stray unconsumed TimerStarted where a signal was expected must still \
         diverge (park-forever guard); the #1071 fix must not over-swallow \
         it:\n{report}"
    );
}

// ---------------------------------------------------------------------------
// #1071 review-fix coverage (PR follow-up): additional replayer manifestations
// and divergence guards flagged by review. NONE of these change matcher LOGIC —
// they broaden the empirical coverage of the sibling-tolerance fix and pin the
// genuine-divergence boundaries it must NOT over-swallow.
// ---------------------------------------------------------------------------

/// Variant of manifestation #3 that exercises the GENUINE-user-timer path:
/// `tokio::join!(ctx.wait_for_signal("go"), ctx.timer("t", 5))` with the signal
/// wait polled FIRST and a plain user `ctx.timer("t", …)` (NOT the reserved
/// `__signal_timeout` deadline id) as the sibling. This drives `match_signal`'s
/// generic-`TimerStarted` rewind arm (which sets `first_interleaved_command`) +
/// the new foreign-`TimerFired` cross + the sibling `match_timer_strict("t")`
/// re-matching after the rewind — the path the reserved-`__signal_timeout`
/// variant (`interleaved_sibling_signal_after_deadline_...`) does NOT cover.
fn signal_wait_with_user_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // Signal-wait command emitted FIRST (polled first by tokio::join!),
        // then a plain user timer.
        let (sig_res, timer_res) = tokio::join!(ctx.wait_for_signal("go"), ctx.timer("t", 5));
        sig_res.map_err(|e| e.to_string())?;
        timer_res.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Test A (review §3a variant): a plain user `ctx.timer("t")` sibling whose fire
/// is recorded before the awaited signal must replay. Complements the reserved
/// `__signal_timeout` variant already covered by
/// `interleaved_sibling_signal_after_deadline_timer_fired_replays_succeeded`.
#[tokio::test]
async fn interleaved_sibling_signal_after_plain_user_timer_fired_replays_succeeded() {
    let t = TimerId::new("t");
    let events = vec![
        interleaved_started(),
        WorkflowEvent::TimerStarted {
            timer_id: t.clone(),
            duration_secs: 5,
        },
        WorkflowEvent::TimerFired { timer_id: t },
        WorkflowEvent::SignalReceived {
            signal_name: "go".into(),
            payload: serde_json::json!({"ok": true}),
        },
    ];
    let report = WorkflowReplayer::new()
        .register_fn(
            "signal_wait_with_user_timer_workflow",
            signal_wait_with_user_timer_workflow,
        )
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1071: a plain user ctx.timer sibling whose fire is recorded \
         before the awaited signal must replay — match_signal must rewind for \
         the interleaved TimerStarted, cross the foreign TimerFired, find the \
         signal, and let the sibling match_timer_strict re-match after the \
         rewind:\n{report}"
    );
}

/// Test B (review §3b): foreign-`TimerFired` genuine-divergence guard. Two timers
/// are armed in one batch (`timer_a` matched first) but ONLY `timer_b` fires; the
/// history also carries the terminal `WorkflowCompleted` that a fully-resolved
/// run would reach. Crossing `TimerFired(timer_b)` non-consumingly must NOT let
/// `timer_a` falsely resolve — so the workflow must NOT reach `WorkflowCompleted`,
/// leaving that terminal unconsumed at the frontier → the replayer must flag a
/// divergence, NOT `ReplaySucceeded`. This proves the foreign-`TimerFired` cross
/// is transparent-only and never a false resolution of the polled timer.
#[tokio::test]
async fn interleaved_sibling_multi_timer_wrong_one_fires_still_diverges() {
    let a = TimerId::new("timer_a");
    let b = TimerId::new("timer_b");
    let events = vec![
        interleaved_started(),
        WorkflowEvent::TimerStarted {
            timer_id: a,
            duration_secs: 5,
        },
        WorkflowEvent::TimerStarted {
            timer_id: b.clone(),
            duration_secs: 5,
        },
        // Only timer_b fires; timer_a NEVER fires.
        WorkflowEvent::TimerFired { timer_id: b },
        // A fully-resolved run would reach this terminal — it is unreachable
        // unless timer_a is (wrongly) resolved by crossing TimerFired(timer_b).
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];
    let report = WorkflowReplayer::new()
        .register_fn("interleaved_two_timers", interleaved_two_timers_workflow)
        .replay_from_events(events)
        .await;

    assert!(
        !matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1071: crossing a foreign TimerFired(timer_b) must NOT falsely \
         resolve the polled timer_a — the workflow must stay parked on timer_a \
         and never reach the recorded WorkflowCompleted, so the replay must \
         diverge (early-completion) rather than succeed:\n{report}"
    );
}

/// Test C (review §2a, resolved EMPIRICALLY): a stray FOREIGN `TimerFired` where a
/// signal was expected, with NO `TimerStarted` and NO signal
/// (`[WorkflowStarted, TimerFired(orphan)]`), fed to the plain `signal_wait_workflow`.
/// The #1071 fix makes `match_signal` cross a `TimerFired` non-consumingly, so the
/// scan reaches end-of-history and `wait_for_signal` returns `NoMatch` (park) — but
/// the stray, unconsumed `TimerFired` remains at the frontier, so the replayer must
/// still flag a divergence rather than silently swallowing the stray event as
/// `ReplaySucceeded`. (If this ever returns `ReplaySucceeded`, the tolerance is
/// over-swallowing and it is a correctness bug.)
#[tokio::test]
async fn interleaved_sibling_signal_stray_timer_fired_still_diverges() {
    let events = vec![
        interleaved_started(),
        // Stray, unconsumed foreign fire: no TimerStarted, no SignalReceived.
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("orphan"),
        },
    ];
    let report = WorkflowReplayer::new()
        .register_fn("signal_wait_workflow", signal_wait_workflow)
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "a stray unconsumed foreign TimerFired where a signal was expected must \
         still diverge — the #1071 TimerFired-cross must NOT silently swallow a \
         stray fire as a successful replay:\n{report}"
    );
}

/// Composes a plain `wait_for_signal` with a cancellable-timer (`start_timer` /
/// `TimerHandle::await_fire`, issue #768) in one `tokio::join!`. The cancellable
/// timer's `TimerFired` is recorded BEFORE the delivered signal.
fn signal_wait_with_cancellable_await_fire_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        let (sig_res, fire_res) = tokio::join!(ctx.wait_for_signal("go"), handle.await_fire());
        sig_res.map_err(|e| e.to_string())?;
        fire_res.map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// Test D (determinism-review finding 5.1 — KNOWN LIMITATION PIN): composing a
/// plain `wait_for_signal` with a cancellable-timer `await_fire()` in one
/// `tokio::join!` is NOT a supported shape when the history records the
/// cancellable timer's fire BEFORE the signal.
///
/// Issue #768 already documents composing an armed cancellable-timer handle with
/// a signal wait as a follow-up, not a supported composition. The #1071 fix
/// broadens `match_signal`'s foreign-`TimerFired` cross: the plain
/// `wait_for_signal` wins by crossing the cancellable timer's `TimerFired`
/// non-consumingly and advancing the cursor past it, which STRANDS that fire
/// BEHIND the cursor. The cancellable timer's `await_fire` uses
/// `match_timer_or_cancel`, which scans only FORWARD from the cursor, so it
/// misses the behind-cursor fire and cannot resolve — diverging strict replay.
/// On a live worker this self-heals (the arm re-fires), but a `WorkflowReplayer`
/// strict replay surfaces the divergence. Use `ctx.receive_signal_timeout`
/// (issue #476) for a supported signal-or-deadline shape.
///
/// This test PINS the actual strict-replay outcome so a future change to the
/// composition's behavior is noticed.
#[tokio::test]
async fn known_limitation_signal_wait_composed_with_cancellable_await_fire_diverges_on_reversed_order()
 {
    let events = vec![
        interleaved_started(),
        // Cancellable-timer arm (consumed positionally by match_timer_arm).
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("idle"),
            duration_secs: 300,
        },
        // The cancellable timer's fire is recorded BEFORE the signal.
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("idle"),
        },
        WorkflowEvent::SignalReceived {
            signal_name: "go".into(),
            payload: serde_json::json!({"ok": true}),
        },
    ];
    let report = WorkflowReplayer::new()
        .register_fn(
            "signal_wait_with_cancellable_await_fire_workflow",
            signal_wait_with_cancellable_await_fire_workflow,
        )
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "known limitation (issue #768 follow-up / #1071): a plain wait_for_signal \
         composed with a cancellable-timer await_fire whose fire is recorded \
         before the signal is unsupported — the signal-win strands the \
         cancellable fire behind the cursor and match_timer_or_cancel's \
         forward-only scan misses it, diverging strict replay:\n{report}"
    );
}

/// `tokio::join!(ctx.timer("t", 5), ctx.spawn_child_workflow(...))` — a mixed
/// suspension batch of a timer and a child workflow, timer polled/matched first.
fn timer_then_child_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let (timer_res, child_res) = tokio::join!(
            ctx.timer("t", 5),
            ctx.spawn_child_workflow_raw("child_proc", serde_json::json!({"item": "A"})),
        );
        timer_res.map_err(|e| e.to_string())?;
        let out = child_res.map_err(|e| e.to_string())?;
        Ok(out)
    })
}

/// Test F (review §2c): an interleaved CHILD-WORKFLOW terminal must be crossed.
/// `join!(ctx.timer("t"), ctx.spawn_child_workflow(...))` where the child
/// completes BEFORE the timer fires:
/// `[WorkflowStarted, TimerStarted(t), ChildWorkflowStarted, ChildWorkflowCompleted, TimerFired(t)]`.
/// Exercises `match_timer_strict`'s `ChildWorkflowStarted` interleaved-command
/// rewind + the new `ChildWorkflowCompleted` transparent cross, then the sibling
/// `match_child_workflow` re-matching after the rewind.
#[tokio::test]
async fn interleaved_sibling_child_workflow_terminal_before_timer_replays_succeeded() {
    let child_id = ExecutionId::new();
    let t = TimerId::new("t");
    let events = vec![
        interleaved_started(),
        WorkflowEvent::TimerStarted {
            timer_id: t.clone(),
            duration_secs: 5,
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "child_proc".into(),
            input: serde_json::json!({"item": "A"}),
        },
        WorkflowEvent::ChildWorkflowCompleted {
            child_id,
            output: serde_json::json!({"done": true}),
        },
        WorkflowEvent::TimerFired { timer_id: t },
    ];
    let report = WorkflowReplayer::new()
        .register_fn("timer_then_child_workflow", timer_then_child_workflow)
        .replay_from_events(events)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "issue #1071 manifestation #2 (child-workflow variant): a child workflow \
         completing before a sibling timer's fire must replay — \
         match_timer_strict must cross the interleaved ChildWorkflowStarted \
         (rewind) and ChildWorkflowCompleted (transparent), then the sibling \
         match_child_workflow re-matches after the rewind:\n{report}"
    );
}

// ─── Issue #614 (PR #1107 Codex review): STRICT replay honors with_history_policy ───
//
// The merged #1107 fix threaded the runtime's history policy into the CANARY
// replay path only; the STRICT paths (`replay_from_events` / `replay_from_snapshot`
// → `run_workflow_strict` / `_advancing_clock`) still hardcoded
// `WorkflowHistoryPolicy::default()`, so a caller that set `with_history_policy`
// and then used a strict path had the setting silently ignored — a
// `should_continue_as_new`-branching workflow then false-diverged. This block pins
// the fix on the strict path: the same workflow + history that DIVERGES under the
// default policy replays CLEAN once `with_history_policy(threshold = 2)` is set.

/// Schedules `checkpoint` when `should_continue_as_new()` trips (via the
/// history-size threshold), else `keep_going`. The recorded history fixes which
/// activity was scheduled, so a wrong effective policy schedules the other
/// activity and diverges — the decision is observable through replay determinism.
fn history_policy_branch_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity = if ctx.should_continue_as_new() {
            "checkpoint"
        } else {
            "keep_going"
        };
        ctx.execute_activity_raw(activity, Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(Value::Null)
    })
}

/// A 4-event history recorded by the `checkpoint` branch (event count 4 > the
/// tuned threshold of 2): `WorkflowStarted`, `ActivityScheduled(checkpoint)`,
/// `ActivityCompleted`, `WorkflowCompleted`. Under the DEFAULT policy (large
/// threshold) `should_continue_as_new()` never trips for this small history.
fn history_policy_checkpoint_fixture() -> Vec<WorkflowEvent> {
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
            name: "checkpoint".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: Value::Null,
        },
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ]
}

#[tokio::test]
async fn strict_replay_default_policy_diverges_from_low_threshold_history() {
    // Negative control: under the DEFAULT history policy (large threshold) the
    // 4-event history never trips `should_continue_as_new()`, so the workflow
    // schedules `keep_going` and diverges from the recorded `checkpoint`. Proves
    // the fixture is discriminating (and that the strict-path default is
    // unchanged for callers that never set a policy).
    let report = WorkflowReplayer::new()
        .register_fn("history_policy_branch", history_policy_branch_workflow)
        .replay_from_events(history_policy_checkpoint_fixture())
        .await;

    assert!(
        matches!(
            report.status,
            ReplayStatus::NonDeterminismDetected {
                kind: NonDeterminismKind::ActivityScheduleMismatch,
                ..
            }
        ),
        "default policy must diverge (keep_going vs recorded checkpoint):\n{report}"
    );
}

#[tokio::test]
async fn strict_replay_honors_with_history_policy() {
    // Issue #614 (PR #1107 Codex review): the STRICT `replay_from_events` path
    // must honor `with_history_policy`. With threshold = 2 the 4-event history
    // trips `should_continue_as_new()`, so the workflow schedules `checkpoint`,
    // matching the recorded history → ReplaySucceeded. BEFORE the fix the strict
    // path hardcoded the default policy, ignored this setter, scheduled
    // `keep_going`, and reported NonDeterminismDetected (identical to the negative
    // control above) — so this test is the fix's regression guard.
    let policy =
        autumn_harvest::context::WorkflowHistoryPolicy::default().with_continue_as_new_threshold(2);
    let report = WorkflowReplayer::new()
        .with_history_policy(policy)
        .register_fn("history_policy_branch", history_policy_branch_workflow)
        .replay_from_events(history_policy_checkpoint_fixture())
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "strict replay must honor with_history_policy(threshold=2) and replay clean:\n{report}"
    );
}

// ===========================================================================
// Bounded / windowed activity fan-out (issue #750)
// ===========================================================================

/// Windowed fan-out over 4 fixed activities (`task_0..3`, input `0..3`),
/// parameterised only by window. All windows must replay the same recorded
/// history identically — the window governs live dispatch, never history.
fn windowed_replay_w1<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    windowed_replay_body(ctx, 1)
}
fn windowed_replay_w2<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    windowed_replay_body(ctx, 2)
}
fn windowed_replay_w100<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    windowed_replay_body(ctx, 100)
}
/// Unbounded sibling with the identical activity shape.
fn unbounded_replay_4<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activities: Vec<_> = (0..4)
            .map(|i| {
                (
                    format!("task_{i}"),
                    serde_json::json!(i),
                    "default".to_string(),
                )
            })
            .collect();
        let results = ctx
            .execute_activity_fan_out_raw(activities)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "results": results }))
    })
}

fn windowed_replay_body(
    ctx: &WorkflowContext,
    window: usize,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
    Box::pin(async move {
        let activities: Vec<_> = (0..4)
            .map(|i| {
                (
                    format!("task_{i}"),
                    serde_json::json!(i),
                    "default".to_string(),
                )
            })
            .collect();
        let results = ctx
            .execute_activity_fan_out_raw_windowed(activities, window)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "results": results }))
    })
}

/// Build a windowed-fan-out recorded history: `marker(count=4)` + 4
/// `ActivityScheduled` (always input order) + 4 `ActivityCompleted` in the
/// order given by `completion_order` (a permutation of `0..4`).
fn windowed_fan_out_history(completion_order: &[usize]) -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let ids: Vec<ActivityExecId> = (0..4).map(|_| ActivityExecId::new()).collect();
    let mut events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: serde_json::json!(4u64),
        },
    ];
    // Scheduled always in input order (this is what the impl records).
    for (i, id) in ids.iter().enumerate() {
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id: *id,
            name: format!("task_{i}"),
            input: serde_json::json!(i),
            queue: "default".into(),
        });
    }
    // Completions in the requested (possibly randomized) order.
    for &i in completion_order {
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id: ids[i],
            output: serde_json::json!(format!("done_{i}")),
        });
    }
    (exec_id, events)
}

/// Falsifiable success-bar coverage (AC9): replaying a recorded bounded-fan-out
/// history must report `ReplaySucceeded`.
#[tokio::test]
async fn replayer_succeeds_for_windowed_fan_out() {
    let (exec_id, events) = windowed_fan_out_history(&[0, 1, 2, 3]);
    let report = WorkflowReplayer::new()
        .register_fn("windowed_replay_w2", windowed_replay_w2)
        .replay_from_snapshot(make_snapshot("windowed_replay_w2", exec_id, events))
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "windowed fan-out must replay successfully: {report}"
    );
}

/// AC6 (window-independence): the SAME recorded history replays to identical
/// results whether the replaying code is configured with W=2, W=100, or the
/// unbounded path — the window governs live dispatch only.
#[tokio::test]
async fn replayer_windowed_fan_out_is_window_independent() {
    let (exec_id, events) = windowed_fan_out_history(&[2, 0, 3, 1]);

    // (a) all windows replay clean under the strict WorkflowReplayer.
    for (name, handler) in [
        (
            "windowed_replay_w1",
            windowed_replay_w1 as WorkflowHandlerFn,
        ),
        (
            "windowed_replay_w2",
            windowed_replay_w2 as WorkflowHandlerFn,
        ),
        (
            "windowed_replay_w100",
            windowed_replay_w100 as WorkflowHandlerFn,
        ),
        (
            "unbounded_replay_4",
            unbounded_replay_4 as WorkflowHandlerFn,
        ),
    ] {
        let report = WorkflowReplayer::new()
            .register_fn(name, handler)
            .replay_from_snapshot(make_snapshot(name, exec_id, events.clone()))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{name} must replay clean: {report}"
        );
    }

    // (b) all windows produce byte-identical results when driven to completion.
    let mut outputs = Vec::new();
    for handler in [
        windowed_replay_w1 as WorkflowHandlerFn,
        windowed_replay_w2 as WorkflowHandlerFn,
        windowed_replay_w100 as WorkflowHandlerFn,
        unbounded_replay_4 as WorkflowHandlerFn,
    ] {
        let outcome =
            autumn_harvest::executor::run_workflow(exec_id, events.clone(), handler, Value::Null)
                .await;
        match outcome {
            autumn_harvest::executor::WorkflowOutcome::Completed { output, .. } => {
                outputs.push(output);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }
    let expected = serde_json::json!({ "results": ["done_0", "done_1", "done_2", "done_3"] });
    for out in &outputs {
        assert_eq!(
            *out, expected,
            "all windows must produce input-order results"
        );
    }
}

/// AC6 (the falsifiable determinism bar): replay ≥25 histories whose
/// `ActivityCompleted` events appear in randomized (non-input) order relative to
/// the `ActivityScheduled` events. Every one must report `ReplaySucceeded` with
/// input-order results.
#[tokio::test]
async fn replayer_windowed_fan_out_randomized_completion_order() {
    // Deterministic permutation generator seeded by the iteration index.
    fn perm(seed: usize) -> Vec<usize> {
        let mut v = vec![0usize, 1, 2, 3];
        // Fisher–Yates with a small LCG seeded by `seed` — deterministic.
        let mut state = (seed as u64)
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        for i in (1..v.len()).rev() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let j = (state >> 33) as usize % (i + 1);
            v.swap(i, j);
        }
        v
    }

    // Guard against a vacuous pass: if `perm` were ever refactored to the
    // identity, the "randomized order" claim would be silently false. Require at
    // least two DISTINCT non-identity permutations across the seeds.
    let identity = vec![0usize, 1, 2, 3];
    let non_identity: std::collections::BTreeSet<Vec<usize>> =
        (0..30).map(perm).filter(|o| *o != identity).collect();
    assert!(
        non_identity.len() >= 2,
        "the completion-order generator must produce >=2 distinct non-input orderings; \
         got {non_identity:?}"
    );

    for seed in 0..30 {
        let order = perm(seed);
        let (exec_id, events) = windowed_fan_out_history(&order);
        let report = WorkflowReplayer::new()
            .register_fn("windowed_replay_w2", windowed_replay_w2)
            .replay_from_snapshot(make_snapshot("windowed_replay_w2", exec_id, events.clone()))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "seed {seed} order {order:?} must replay clean: {report}"
        );

        let outcome = autumn_harvest::executor::run_workflow(
            exec_id,
            events,
            windowed_replay_w2,
            Value::Null,
        )
        .await;
        match outcome {
            autumn_harvest::executor::WorkflowOutcome::Completed { output, .. } => {
                assert_eq!(
                    output,
                    serde_json::json!({ "results": ["done_0", "done_1", "done_2", "done_3"] }),
                    "seed {seed} order {order:?}: results must be in input order"
                );
            }
            other => panic!("seed {seed}: expected Completed, got {other:?}"),
        }
    }
}

/// Build a genuinely WINDOWED-SHAPED recorded history (the shape a `W=2`
/// fan-out actually records): `marker(count=4)` then, per wave, all of that
/// wave's `ActivityScheduled` followed by that wave's `ActivityCompleted` —
/// `Sched0, Sched1, Comp0, Comp1, Sched2, Sched3, Comp2, Comp3`. This differs
/// from [`windowed_fan_out_history`], which records the unbounded shape (all 4
/// scheduled up front, then all completions).
fn windowed_shaped_fan_out_history() -> (ExecutionId, Vec<WorkflowEvent>) {
    let exec_id = ExecutionId::new();
    let ids: Vec<ActivityExecId> = (0..4).map(|_| ActivityExecId::new()).collect();
    let mut events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: serde_json::json!(4u64),
        },
    ];
    // Two waves of two, each scheduled-then-completed before the next wave.
    for wave in [[0usize, 1], [2, 3]] {
        for &i in &wave {
            events.push(WorkflowEvent::ActivityScheduled {
                activity_id: ids[i],
                name: format!("task_{i}"),
                input: serde_json::json!(i),
                queue: "default".into(),
            });
        }
        for &i in &wave {
            events.push(WorkflowEvent::ActivityCompleted {
                activity_id: ids[i],
                output: serde_json::json!(format!("done_{i}")),
            });
        }
    }
    (exec_id, events)
}

/// MAJOR-2 / AC6: a genuinely windowed-SHAPED recorded history (scheduled +
/// completed wave-by-wave, `W=2`) replays clean under DIFFERENT windows (`W=1`,
/// `W=4`, and the unbounded path) and produces byte-identical input-order
/// results — proving the two-phase resume walks a windowed-shaped history
/// correctly regardless of the replaying code's window. The pre-existing
/// window-independence fixtures only exercise the unbounded-shaped history.
#[tokio::test]
async fn replayer_windowed_shaped_history_replays_under_any_window() {
    let (exec_id, events) = windowed_shaped_fan_out_history();

    for (name, handler) in [
        (
            "windowed_replay_w1",
            windowed_replay_w1 as WorkflowHandlerFn,
        ),
        (
            "windowed_replay_w4",
            windowed_replay_w4 as WorkflowHandlerFn,
        ),
        (
            "unbounded_replay_4",
            unbounded_replay_4 as WorkflowHandlerFn,
        ),
    ] {
        let report = WorkflowReplayer::new()
            .register_fn(name, handler)
            .replay_from_snapshot(make_snapshot(name, exec_id, events.clone()))
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{name} must replay a windowed-shaped history clean: {report}"
        );

        let outcome =
            autumn_harvest::executor::run_workflow(exec_id, events.clone(), handler, Value::Null)
                .await;
        match outcome {
            autumn_harvest::executor::WorkflowOutcome::Completed { output, .. } => {
                assert_eq!(
                    output,
                    serde_json::json!({ "results": ["done_0", "done_1", "done_2", "done_3"] }),
                    "{name}: windowed-shaped history must yield input-order results"
                );
            }
            other => panic!("{name}: expected Completed, got {other:?}"),
        }
    }
}

/// A `W=4`-configured windowed replay handler (sibling of the `w1`/`w2`/`w100`
/// handlers above) for the windowed-shaped-history coverage.
fn windowed_replay_w4<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    windowed_replay_body(ctx, 4)
}

/// MINOR: a fail-fast bounded fan-out history — `marker(count=4)`, a partial
/// wave scheduled (`W=2` → slots 0,1 only), slot 1 `ActivityFailed`, terminal
/// `WorkflowFailed` — replays **cleanly** (deterministically reproduces the
/// failure), i.e. `ReplayStatus::WorkflowFailed`, never
/// `NonDeterminismDetected`.
///
/// Note: for a workflow that legitimately FAILS, "replays cleanly" surfaces as
/// `WorkflowFailed` (the deterministic reproduction of the recorded failure) —
/// `ReplaySucceeded` is reserved for a workflow that COMPLETES. This mirrors
/// `replay_typed_workflow_failed_round_trips_with_identical_typed_fields`.
#[tokio::test]
async fn replayer_windowed_fail_fast_history_reproduces_failure_deterministically() {
    let exec_id = ExecutionId::new();
    let ids: Vec<ActivityExecId> = (0..2).map(|_| ActivityExecId::new()).collect();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        // Marker records the FULL count (4), even though fail-fast only ever
        // scheduled the first wave [0,1] before the failure aborted the fan-out.
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: serde_json::json!(4u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: ids[0],
            name: "task_0".into(),
            input: serde_json::json!(0),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: ids[1],
            name: "task_1".into(),
            input: serde_json::json!(1),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: ids[0],
            output: serde_json::json!("done_0"),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: ids[1],
            error: "task_1_boom".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
        WorkflowEvent::WorkflowFailed {
            error: "task_1_boom".into(),
            error_type: None,
            details: None,
            non_retryable: None,
        },
    ];

    let report = WorkflowReplayer::new()
        .register_fn("windowed_replay_w2", windowed_replay_w2)
        .replay_from_snapshot(make_snapshot("windowed_replay_w2", exec_id, events))
        .await;

    assert!(
        !matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
        "a fail-fast bounded history must NOT surface false non-determinism: {report}"
    );
    match report.status {
        ReplayStatus::WorkflowFailed { ref error, .. } => {
            assert!(
                error.contains("task_1_boom"),
                "reproduced failure must carry the recorded activity error: {report}"
            );
        }
        other => panic!("expected WorkflowFailed (clean reproduction), got {other:?}"),
    }
}
