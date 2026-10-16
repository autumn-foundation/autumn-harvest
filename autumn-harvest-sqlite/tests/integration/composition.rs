//! Issue #2026: child workflows, and signal, cancel and await between runs.
//!
//! Each test drives a real composition on the `SQLite` backend. Each test
//! then replays every history it wrote on the core `WorkflowReplayer`.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]
// Some fixtures return at once and hold no `.await`.
#![allow(clippy::unused_async)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use autumn_harvest::ParentClosePolicy;
use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::failure::ERROR_TYPE_HANDLER_PANIC;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest_sqlite::{
    ActivitySpec, ExecutionOutcome, RunState, SqliteError, SqliteRuntime,
    UNSUPPORTED_FEATURE_ERROR_TYPE,
};
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};

// ── Fixtures ────────────────────────────────────────────────────────────────

/// Runs one activity and reports what the child sees in `ctx.info()`.
#[workflow]
async fn child_work(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    let doubled = ctx
        .execute_activity_raw("double", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?;
    let info = ctx.info();
    Ok(json!({
        "doubled": doubled,
        "parent": info.parent_execution_id.map(|p| p.to_string()),
        "workflow_id": info.workflow_id,
    }))
}

#[workflow]
async fn parent_attached(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    let child = ctx
        .spawn_child_workflow_raw("child_work", json!(n))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "child": child }))
}

#[workflow]
async fn child_typed_failure(ctx: &WorkflowContext, _n: i64) -> Result<i64, WorkflowFailure> {
    let _ = ctx.info();
    Err(WorkflowFailure::new("PaymentDeclined", "card declined")
        .with_details(json!({ "code": 51 })))
}

/// Completes, but its last cycle schedules an external activity. This
/// backend still rejects that, so the child is sealed with a typed failure.
#[workflow]
async fn child_unsupported(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    // Poll the external activity once, so its command joins the last batch,
    // then return without awaiting it.
    let mut external =
        std::pin::pin!(ctx.execute_activity_external("external", json!({}), "default", 60));
    let _ = std::future::Future::poll(
        external.as_mut(),
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
    );
    Ok(n)
}

/// Spawns the named child and reports the typed failure it observes.
#[workflow]
async fn parent_reports_child_failure(
    ctx: &WorkflowContext,
    child: String,
) -> Result<Value, String> {
    match ctx.spawn_child_workflow_raw(&child, json!(0)).await {
        Ok(v) => Err(format!("the child must fail, got {v}")),
        Err(HarvestError::WorkflowFailed {
            name,
            reason,
            error_type,
            details,
            non_retryable,
        }) => Ok(json!({
            "name": name,
            "reason": reason,
            "error_type": error_type,
            "details": details,
            "non_retryable": non_retryable,
        })),
        Err(other) => Err(format!("unexpected error: {other}")),
    }
}

#[workflow]
async fn waits_for_go(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    // A recorded side effect gives a replay of a cancelled run a command to check.
    let _ticket = ctx.new_uuid();
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

#[workflow]
async fn waits_on_timer(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.timer("nap", 3600).await.map_err(|e| e.to_string())
}

/// Spawns a detached copy of itself under `request_cancel` until `depth` is 0,
/// then waits for a signal. Each link waits for `go` after its spawn.
#[workflow]
async fn detached_chain(ctx: &WorkflowContext, depth: i64) -> Result<Value, String> {
    if depth > 0 {
        ctx.spawn_child_workflow_detached_raw(
            "detached_chain",
            json!(depth - 1),
            ParentClosePolicy::RequestCancel,
        )
        .map_err(|e| e.to_string())?;
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Awaits `target` and runs a child in one `join!`, the await first.
#[workflow]
async fn joins_await_then_child(
    ctx: &WorkflowContext,
    target: ExecutionId,
) -> Result<Value, String> {
    let (awaited, child) = tokio::join!(
        ctx.await_external_workflow_value(target),
        ctx.spawn_child_workflow_raw("child_work", json!(2)),
    );
    Ok(json!({
        "awaited": awaited.map_err(|e| e.to_string())?,
        "child": child.map_err(|e| e.to_string())?["doubled"],
    }))
}

/// The same `join!` with the child first.
#[workflow]
async fn joins_child_then_await(
    ctx: &WorkflowContext,
    target: ExecutionId,
) -> Result<Value, String> {
    let (child, awaited) = tokio::join!(
        ctx.spawn_child_workflow_raw("child_work", json!(2)),
        ctx.await_external_workflow_value(target),
    );
    Ok(json!({
        "awaited": awaited.map_err(|e| e.to_string())?,
        "child": child.map_err(|e| e.to_string())?["doubled"],
    }))
}

/// Fails with the child's failure, without catching it.
#[workflow]
async fn parent_propagates(ctx: &WorkflowContext, child: String) -> Result<Value, String> {
    ctx.spawn_child_workflow_raw(&child, json!(0))
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn child_panics(_ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    panic!("kaboom")
}

/// A chain of attached children. The leaf waits for `go`.
#[workflow]
async fn attached_chain(ctx: &WorkflowContext, depth: i64) -> Result<Value, String> {
    if depth == 0 {
        return ctx.wait_for_signal("go").await.map_err(|e| e.to_string());
    }
    ctx.spawn_child_workflow_raw("attached_chain", json!(depth - 1))
        .await
        .map_err(|e| e.to_string())
}

/// Awaits `target` while a child signals it. Neither needs outside input.
#[workflow]
async fn joins_await_and_signaller(
    ctx: &WorkflowContext,
    target: ExecutionId,
) -> Result<Value, String> {
    let (awaited, sent) = tokio::join!(
        ctx.await_external_workflow_value(target),
        ctx.spawn_child_workflow_raw("signal_reports", json!(target)),
    );
    Ok(json!({
        "awaited": awaited.map_err(|e| e.to_string())?,
        "sent": sent.map_err(|e| e.to_string())?,
    }))
}

/// Spawns a detached watchdog that cancels this parent, then waits.
#[workflow]
async fn parent_with_watchdog(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let me = ctx.info().execution_id;
    ctx.spawn_child_workflow_detached_raw("watchdog", json!(me), ParentClosePolicy::RequestCancel)
        .map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Cancels its own parent and runs an activity in the same batch. The
/// cascade seals this run, so the activity must never run.
#[workflow]
async fn watchdog(ctx: &WorkflowContext, parent: ExecutionId) -> Result<(), String> {
    let (cancel, effect) = tokio::join!(
        ctx.request_cancel_external_workflow(parent),
        ctx.execute_activity_raw("watchdog_effect", json!(1), "default"),
    );
    cancel.map_err(|e| e.to_string())?;
    effect.map(drop).map_err(|e| e.to_string())
}

/// Spawns a detached `request_cancel` waiter, then fails in the same cycle.
#[workflow]
async fn spawns_detached_then_fails(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.spawn_child_workflow_detached_raw(
        "waits_for_go",
        json!(0),
        ParentClosePolicy::RequestCancel,
    )
    .map_err(|e| e.to_string())?;
    Err("parent gave up".to_string())
}

/// Sends the same keyed signal by workflow id.
#[workflow]
async fn keyed_signal_by_id(ctx: &WorkflowContext, key: Value) -> Result<String, String> {
    let name = key["name"].as_str().unwrap_or_default().to_string();
    let id = key["id"].as_str().unwrap_or_default().to_string();
    let idem = key["key"].as_str().unwrap_or_default().to_string();
    ctx.signal_external_workflow_by_id_with_idempotency(&name, &id, "go", json!("keyed"), idem)
        .await
        .map(|()| "delivered".to_string())
        .map_err(|e| e.to_string())
}

/// Sends one keyed signal to `target`.
#[workflow]
async fn keyed_signal(ctx: &WorkflowContext, args: Value) -> Result<String, String> {
    let target: ExecutionId =
        serde_json::from_value(args["target"].clone()).map_err(|e| e.to_string())?;
    let idem = args["key"].as_str().unwrap_or_default().to_string();
    match ctx
        .signal_external_workflow_with_idempotency(target, "other", json!(1), idem)
        .await
    {
        Ok(()) => Ok("delivered".to_string()),
        Err(HarvestError::ExternalSignalFailed { reason_code, .. }) => Ok(reason_code),
        Err(other) => Err(other.to_string()),
    }
}

/// Runs a child under a one-second deadline. The child runs `slow`.
#[workflow]
async fn parent_child_timeout_slow(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let out = ctx
        .spawn_child_workflow_timeout("slow_child", json!(0), std::time::Duration::from_secs(1))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "child": out }))
}

#[workflow]
async fn slow_child(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.execute_activity_raw("slow", json!(0), "default")
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn parent_of_waiter(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let out = ctx
        .spawn_child_workflow_raw("waits_for_go", json!(0))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "child": out }))
}

/// Spawns `waits_for_go` and reports how the child ended, as text.
#[workflow]
async fn parent_reports_waiter(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    match ctx.spawn_child_workflow_raw("waits_for_go", json!(0)).await {
        Ok(v) => Ok(format!("ok: {v}")),
        Err(HarvestError::WorkflowFailed { reason, .. }) => Ok(reason),
        Err(other) => Err(other.to_string()),
    }
}

#[workflow]
async fn parent_detached_awaits(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    let child = ctx
        .spawn_child_workflow_detached_raw("child_work", json!(n), ParentClosePolicy::Abandon)
        .map_err(|e| e.to_string())?;
    let out = ctx
        .await_external_workflow_value(child)
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "child_id": child.to_string(), "out": out }))
}

/// Spawns a detached `waits_for_go` under `policy`, then ends. With
/// `then_activity`, the spawn rides a suspending batch. Without it, the spawn
/// rides the terminal cycle.
#[workflow]
async fn parent_spawns_detached(ctx: &WorkflowContext, args: Value) -> Result<String, String> {
    let policy = match args["policy"].as_str().unwrap_or_default() {
        "abandon" => ParentClosePolicy::Abandon,
        "terminate" => ParentClosePolicy::Terminate,
        _ => ParentClosePolicy::RequestCancel,
    };
    let child = ctx
        .spawn_child_workflow_detached_raw("waits_for_go", json!(0), policy)
        .map_err(|e| e.to_string())?;
    if args["then_activity"].as_bool().unwrap_or_default() {
        ctx.execute_activity_raw("double", json!(1), "default")
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(child.to_string())
}

#[workflow]
async fn signal_reports(ctx: &WorkflowContext, target: ExecutionId) -> Result<String, String> {
    match ctx
        .signal_external_workflow(target, "go", json!({ "from": "signaller" }))
        .await
    {
        Ok(()) => Ok("delivered".to_string()),
        Err(HarvestError::ExternalSignalFailed { reason_code, .. }) => Ok(reason_code),
        Err(other) => Err(other.to_string()),
    }
}

#[workflow]
async fn signal_by_id_reports(ctx: &WorkflowContext, key: Value) -> Result<String, String> {
    let name = key["name"].as_str().unwrap_or_default().to_string();
    let id = key["id"].as_str().unwrap_or_default().to_string();
    match ctx
        .signal_external_workflow_by_id(&name, &id, "go", json!("by-id"))
        .await
    {
        Ok(()) => Ok("delivered".to_string()),
        Err(HarvestError::ExternalSignalFailed { reason_code, .. }) => Ok(reason_code),
        Err(other) => Err(other.to_string()),
    }
}

/// Sends the same keyed signal twice. The target must stage it once.
#[workflow]
async fn signal_twice_with_key(ctx: &WorkflowContext, target: ExecutionId) -> Result<(), String> {
    for _ in 0..2 {
        ctx.signal_external_workflow_with_idempotency(target, "go", json!(7), "once".to_string())
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[workflow]
async fn cancel_reports(ctx: &WorkflowContext, target: ExecutionId) -> Result<String, String> {
    match ctx.request_cancel_external_workflow(target).await {
        Ok(()) => Ok("delivered".to_string()),
        Err(HarvestError::ExternalCancelFailed { reason_code, .. }) => Ok(reason_code),
        Err(other) => Err(other.to_string()),
    }
}

#[workflow]
async fn cancel_by_id_reports(ctx: &WorkflowContext, key: Value) -> Result<String, String> {
    let name = key["name"].as_str().unwrap_or_default().to_string();
    let id = key["id"].as_str().unwrap_or_default().to_string();
    match ctx.request_cancel_external_workflow_by_id(&name, &id).await {
        Ok(()) => Ok("delivered".to_string()),
        Err(HarvestError::ExternalCancelFailed { reason_code, .. }) => Ok(reason_code),
        Err(other) => Err(other.to_string()),
    }
}

#[workflow]
async fn await_reports(ctx: &WorkflowContext, target: ExecutionId) -> Result<Value, String> {
    match ctx.await_external_workflow_value(target).await {
        Ok(v) => Ok(json!({ "ok": v })),
        Err(HarvestError::WorkflowFailed {
            reason,
            error_type,
            details,
            ..
        }) => Ok(json!({ "failed": reason, "error_type": error_type, "details": details })),
        Err(HarvestError::ExternalAwaitFailed { reason_code, .. }) => {
            Ok(json!({ "await_failed": reason_code }))
        }
        Err(other) => Err(other.to_string()),
    }
}

#[workflow]
async fn parent_child_timeout(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let out = ctx
        .spawn_child_workflow_timeout("waits_for_go", json!(0), std::time::Duration::from_secs(60))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "child": out }))
}

/// Awaits its own parent. Its parent awaits it, so neither can finish.
#[workflow]
async fn child_awaits_parent(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let parent = ctx.info().parent_execution_id.ok_or("no parent")?;
    ctx.await_external_workflow_value(parent)
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn parent_of_awaiting_child(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_raw("child_awaits_parent", json!(0))
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn parent_of_unregistered(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_raw("registered_late", json!(5))
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn registered_late(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n + 1)
}

/// Fails in the same cycle that dispatches a child. The child never starts.
#[workflow]
async fn fails_while_dispatching(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    let child = async {
        ctx.spawn_child_workflow_raw("child_work", json!(1))
            .await
            .map_err(|e| e.to_string())
    };
    tokio::try_join!(child, async { Err::<(), String>("boom".to_string()) })?;
    Ok(())
}

/// Continues as new once, then returns its input. A child cannot continue as
/// new, so a child run of it fails.
#[workflow]
async fn continues_once(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    if n == 0 {
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(n)
}

/// Registers a `parent` update and a `parent` query. Each returns the parent
/// id that the body saw. The body then waits for `go`.
#[workflow]
async fn reports_parent_on_update(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let parent = ctx.info().parent_execution_id.map(|p| p.to_string());
    let seen = parent.clone();
    ctx.register_query_handler("parent", move |_: &Value| Ok::<Value, String>(json!(seen)));
    ctx.register_update_handler_no_validator("parent", move |_input: Value| {
        let parent = parent.clone();
        async move { Ok(json!(parent)) }
    });
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Starts `reports_parent_on_update` as an attached child.
#[workflow]
async fn parent_of_update_target(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_raw("reports_parent_on_update", json!(0))
        .await
        .map_err(|e| e.to_string())
}

/// The test clock of `a_long_terminal_cycle_loses_to_a_due_deadline`, in epoch
/// milliseconds. Only that test reads it.
static BUSY_CLOCK: AtomicI64 = AtomicI64::new(1_767_225_600_000);

/// Uses 5 s of the test clock in its only cycle, then completes. It stands
/// for a child whose last cycle does heavy work.
#[workflow]
async fn busy_child(_ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    BUSY_CLOCK.fetch_add(5_000, Ordering::SeqCst);
    Ok(json!("busy-done"))
}

#[workflow]
async fn parent_child_timeout_busy(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let out = ctx
        .spawn_child_workflow_timeout("busy_child", json!(0), std::time::Duration::from_secs(1))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "child": out }))
}

/// The test clock of `a_long_cancel_cleanup_loses_to_a_due_deadline`.
static CANCEL_CLOCK: AtomicI64 = AtomicI64::new(1_767_225_600_000);

/// Waits for `go`. Under a cancel request it uses 5 s of the test clock in
/// its cleanup, then returns.
#[workflow]
async fn busy_cancel_cleanup(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    if ctx.cancellation_reason().is_some() {
        CANCEL_CLOCK.fetch_add(5_000, Ordering::SeqCst);
        return Ok(json!("cleaned up"));
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

#[workflow]
async fn parent_child_timeout_cancel(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let out = ctx
        .spawn_child_workflow_timeout(
            "busy_cancel_cleanup",
            json!(0),
            std::time::Duration::from_secs(1),
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "child": out }))
}

/// The test clock of `a_signal_sent_after_a_deadline_loses_to_it`.
static SIGNAL_CLOCK: AtomicI64 = AtomicI64::new(1_767_225_600_000);

/// Waits 1 s for `go`. Reports which branch won.
#[workflow]
async fn waits_for_go_briefly(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let got = ctx
        .wait_for_signal_timeout("go", std::time::Duration::from_secs(1))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!(if got.is_some() { "signal" } else { "timeout" }))
}

/// Uses 5 s of the test clock, then signals `go` to `target`.
#[workflow]
async fn busy_signaller(ctx: &WorkflowContext, target: ExecutionId) -> Result<(), String> {
    SIGNAL_CLOCK.fetch_add(5_000, Ordering::SeqCst);
    ctx.signal_external_workflow(target, "go", json!(1))
        .await
        .map_err(|e| e.to_string())
}

/// The test clock of `a_long_final_panic_loses_to_a_due_deadline`.
static PANIC_CLOCK: AtomicI64 = AtomicI64::new(1_767_225_600_000);
/// How many times `busy_panic` ran.
static PANIC_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Panics on every run. Its third run, the one that uses up the panic
/// budget, first uses 5 s of the test clock.
#[workflow]
async fn busy_panic(_ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    if PANIC_CALLS.fetch_add(1, Ordering::SeqCst) == 2 {
        PANIC_CLOCK.fetch_add(5_000, Ordering::SeqCst);
    }
    panic!("kaboom")
}

#[workflow]
async fn parent_child_timeout_panic(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let out = ctx
        .spawn_child_workflow_timeout("busy_panic", json!(0), std::time::Duration::from_secs(1))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!({ "child": out }))
}

/// Spawns a detached child under `request_cancel`, then waits for `go`. Under
/// a cancel request it records a side effect as cleanup and returns.
#[workflow]
async fn spawns_then_cleans_up(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_detached_raw(
        "waits_for_go",
        json!(0),
        ParentClosePolicy::RequestCancel,
    )
    .map_err(|e| e.to_string())?;
    if ctx.cancellation_reason().is_some() {
        let _ticket = ctx.new_uuid();
        return Ok(json!("cleaned up"));
    }
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Spawns a detached child under `request_cancel`, then waits on an activity
/// that no one registers.
#[workflow]
async fn spawns_then_waits_on_missing(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_detached_raw(
        "waits_for_go",
        json!(0),
        ParentClosePolicy::RequestCancel,
    )
    .map_err(|e| e.to_string())?;
    ctx.execute_activity_raw("missing", json!(0), "default")
        .await
        .map_err(|e| e.to_string())
}

/// Mints a value, then continues as new. A child run of it fails.
#[workflow]
async fn mints_then_continues(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let _ticket = ctx.new_uuid();
    ctx.continue_as_new(json!(1))
        .await
        .map_err(|e| e.to_string())?;
    Ok(0)
}

/// Spawns a detached child under `request_cancel`, then waits past its 30 s
/// execution timeout.
#[workflow(execution_timeout = "30s")]
async fn spawns_then_times_out(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_detached_raw(
        "waits_for_go",
        json!(0),
        ParentClosePolicy::RequestCancel,
    )
    .map_err(|e| e.to_string())?;
    ctx.wait_for_signal("never")
        .await
        .map_err(|e| e.to_string())
}

/// Waits past its 30 s execution timeout.
#[workflow(execution_timeout = "30s")]
async fn timed_child(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.wait_for_signal("never")
        .await
        .map_err(|e| e.to_string())
}

/// Counts the runs of `retried_child`. Only one test starts it.
static RETRIED_CHILD_RUNS: AtomicUsize = AtomicUsize::new(0);

/// Fails on every run. Its type declares two attempts with no backoff.
#[workflow(retry = autumn_harvest::policy::RetryPolicy::fixed(2, std::time::Duration::ZERO))]
async fn retried_child(_ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    RETRIED_CHILD_RUNS.fetch_add(1, Ordering::SeqCst);
    Err("always".to_string())
}

/// Counts the runs of `retry_declared_child`. Only one test starts it.
static RETRY_DECLARED_RUNS: AtomicUsize = AtomicUsize::new(0);

/// Like `retried_child`, for the awaited case.
#[workflow(retry = autumn_harvest::policy::RetryPolicy::fixed(2, std::time::Duration::ZERO))]
async fn retry_declared_child(_ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    RETRY_DECLARED_RUNS.fetch_add(1, Ordering::SeqCst);
    Err("always".to_string())
}

/// Spawns `retried_child` detached under `abandon`, then waits.
#[workflow]
async fn spawns_retried_child(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_detached_raw("retried_child", json!(0), ParentClosePolicy::Abandon)
        .map_err(|e| e.to_string())?;
    ctx.wait_for_signal("never")
        .await
        .map_err(|e| e.to_string())
}

/// Holds the `k` mutex until a signal that never comes.
#[workflow]
async fn holds_the_lock(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let _guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
    ctx.wait_for_signal("never")
        .await
        .map_err(|e| e.to_string())
}

/// Takes the `k` mutex, releases it, and completes.
#[workflow]
async fn takes_the_lock(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    let guard = ctx.mutex("k").acquire().await.map_err(|e| e.to_string())?;
    guard.release();
    Ok(1)
}

/// Spawns a detached child under `request_cancel`, waits for `go`, then
/// panics on every run.
#[workflow]
async fn spawns_then_panics(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_detached_raw(
        "waits_for_go",
        json!(0),
        ParentClosePolicy::RequestCancel,
    )
    .map_err(|e| e.to_string())?;
    let _: Value = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    panic!("kaboom")
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn runtime() -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    register_all(&mut rt);
    rt
}

fn register_all(rt: &mut SqliteRuntime) {
    for info in fixtures() {
        rt.register_workflow(&info);
    }
    rt.register_activity_raw(
        "double",
        ActivitySpec::new(1, |v: Value| Ok(json!(v.as_i64().unwrap_or_default() * 2))),
    );
}

fn fixtures() -> Vec<WorkflowInfo> {
    vec![
        child_work_info(),
        parent_attached_info(),
        child_typed_failure_info(),
        child_unsupported_info(),
        parent_reports_child_failure_info(),
        waits_for_go_info(),
        waits_on_timer_info(),
        detached_chain_info(),
        joins_await_then_child_info(),
        joins_child_then_await_info(),
        parent_propagates_info(),
        child_panics_info(),
        attached_chain_info(),
        joins_await_and_signaller_info(),
        parent_with_watchdog_info(),
        watchdog_info(),
        spawns_detached_then_fails_info(),
        keyed_signal_by_id_info(),
        keyed_signal_info(),
        parent_child_timeout_slow_info(),
        slow_child_info(),
        parent_of_waiter_info(),
        parent_reports_waiter_info(),
        parent_detached_awaits_info(),
        parent_spawns_detached_info(),
        signal_reports_info(),
        signal_by_id_reports_info(),
        signal_twice_with_key_info(),
        cancel_reports_info(),
        cancel_by_id_reports_info(),
        await_reports_info(),
        parent_child_timeout_info(),
        child_awaits_parent_info(),
        parent_of_awaiting_child_info(),
        parent_of_unregistered_info(),
        fails_while_dispatching_info(),
        continues_once_info(),
        reports_parent_on_update_info(),
        parent_of_update_target_info(),
        busy_child_info(),
        parent_child_timeout_busy_info(),
        busy_cancel_cleanup_info(),
        parent_child_timeout_cancel_info(),
        waits_for_go_briefly_info(),
        busy_signaller_info(),
        busy_panic_info(),
        parent_child_timeout_panic_info(),
        spawns_then_cleans_up_info(),
        spawns_then_waits_on_missing_info(),
        mints_then_continues_info(),
        spawns_then_times_out_info(),
        timed_child_info(),
        retried_child_info(),
        retry_declared_child_info(),
        spawns_retried_child_info(),
        holds_the_lock_info(),
        takes_the_lock_info(),
        spawns_then_panics_info(),
    ]
}

fn handler_of(name: &str) -> WorkflowHandlerFn {
    fixtures()
        .into_iter()
        .chain(std::iter::once(registered_late_info()))
        .find(|info| info.name == name)
        .map_or_else(|| panic!("no fixture named {name}"), |info| info.handler)
}

/// Replay the stored history of `exec` on the core replayer, with the run's
/// execution id, workflow id and parent id.
///
/// A cancel is a hard seal. It ends a parked run, and the run's code never
/// sees the `WorkflowCancelled` event. Strict core replay verifies a
/// cancelled history in full, so it reports such a run as still parked. A
/// Postgres history of the same run has the same bytes and the same result.
/// For that history, replay the events the run wrote in frontier-tolerant
/// (canary) mode. That mode accepts a run that parks at the end.
fn assert_replays<'a>(
    rt: &SqliteRuntime,
    exec: ExecutionId,
    workflow: &'a str,
) -> impl std::future::Future<Output = ()> + 'a {
    // Read the run now. The future then holds no runtime borrow.
    let history = rt.load_history(exec).unwrap();
    let parent = rt.parent_execution(exec).unwrap();
    async move { replay_history(history, exec, parent, workflow).await }
}

async fn replay_history(
    history: Vec<WorkflowEvent>,
    exec: ExecutionId,
    parent: Option<ExecutionId>,
    workflow: &str,
) {
    let replayer = WorkflowReplayer::new().register_fn(workflow, handler_of(workflow));
    // Post-terminal bookkeeping, such as a cascade record, can follow the cancel.
    let cancel_at = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::WorkflowCancelled { .. }));
    let events = cancel_at.map_or(&history[..], |at| &history[..at]);
    // Every run in these tests uses its execution id as its workflow id.
    let snapshot = serde_json::from_value(json!({
        "workflow_name": workflow,
        "execution_id": exec,
        "events": events,
        "parent_execution_id": parent,
        "workflow_id": exec.to_string(),
    }))
    .unwrap();
    let report = if cancel_at.is_some() {
        replayer.replay_canary_snapshot(snapshot).await
    } else {
        replayer.replay_from_snapshot(snapshot).await
    };
    let recorded_failure = history.iter().rev().find_map(|e| match e {
        WorkflowEvent::WorkflowFailed { error, .. } => Some(error.clone()),
        _ => None,
    });
    let ok = match (&report.status, &recorded_failure) {
        (ReplayStatus::ReplaySucceeded, _) => true,
        // A failed run must replay to the same failure.
        (ReplayStatus::WorkflowFailed { error, .. }, Some(recorded)) => {
            &autumn_harvest::failure::decode_workflow_failure(error).message == recorded
        }
        _ => false,
    };
    assert!(
        ok,
        "`{workflow}` history must replay on the core:\n{report}\n{history:#?}"
    );
}

fn completed(rt: &SqliteRuntime, exec: ExecutionId) -> Value {
    match rt.outcome(exec).unwrap() {
        ExecutionOutcome::Completed(v) => v,
        other => panic!("expected a completed run, got {other:?}"),
    }
}

fn children_of(rt: &SqliteRuntime, parent: ExecutionId) -> Vec<ExecutionId> {
    rt.load_history(parent)
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ChildWorkflowStarted { child_id, .. }
            | WorkflowEvent::ChildWorkflowSpawnedDetached { child_id, .. } => Some(*child_id),
            _ => None,
        })
        .collect()
}

fn count_rows(path: &std::path::Path, sql: &str, exec: ExecutionId) -> i64 {
    let inspector = rusqlite::Connection::open(path).unwrap();
    inspector
        .query_row(sql, [exec.to_string()], |row| row.get(0))
        .unwrap()
}

fn assert_no_open_rows(path: &std::path::Path, exec: ExecutionId) {
    for sql in [
        "SELECT COUNT(*) FROM harvest_tasks WHERE exec_id = ?1 AND state = 'PENDING'",
        "SELECT COUNT(*) FROM harvest_timers WHERE exec_id = ?1 AND fired = 0",
        "SELECT COUNT(*) FROM harvest_signals WHERE exec_id = ?1 AND delivered = 0",
    ] {
        assert_eq!(count_rows(path, sql, exec), 0, "{sql}");
    }
}

fn temp_db() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

// ── Attached children ───────────────────────────────────────────────────────

#[tokio::test]
async fn an_attached_child_completes_and_reports_back() {
    let mut rt = runtime();
    let parent = rt.start_workflow("parent_attached", json!(3)).unwrap();

    let state = rt.run_until_blocked(parent).await.unwrap();
    let RunState::Completed(output) = state else {
        panic!("one drive must finish the parent and its child, got {state:?}");
    };

    let children = children_of(&rt, parent);
    assert_eq!(children.len(), 1, "exactly one child starts");
    let child = children[0];
    assert_eq!(output["child"]["doubled"], json!(6));
    assert_eq!(output["child"]["parent"], json!(parent.to_string()));
    assert_eq!(output["child"]["workflow_id"], json!(child.to_string()));
    assert_eq!(completed(&rt, child), output["child"]);

    let history = rt.load_history(parent).unwrap();
    let kinds: Vec<&str> = history.iter().map(WorkflowEvent::type_name).collect();
    assert_eq!(
        kinds,
        [
            "WorkflowStarted",
            "ChildWorkflowStarted",
            "ChildWorkflowCompleted",
            "WorkflowCompleted"
        ]
    );
    assert_replays(&rt, parent, "parent_attached").await;
    assert_replays(&rt, child, "child_work").await;
}

#[tokio::test]
async fn the_fleet_drivers_converge_a_parent_and_its_child() {
    let mut rt = runtime();
    let parent = rt.start_workflow("parent_attached", json!(4)).unwrap();

    assert!(
        rt.poll_once().await.unwrap(),
        "the first pass starts the child"
    );
    rt.run_until_idle().await.unwrap();

    assert_eq!(completed(&rt, parent)["child"]["doubled"], json!(8));
    assert!(!rt.poll_once().await.unwrap(), "the fleet is idle");
}

#[tokio::test]
async fn a_parent_waits_on_a_blocked_child_and_starts_it_once() {
    let mut rt = runtime();
    let parent = rt.start_workflow("parent_of_waiter", json!(0)).unwrap();

    for _ in 0..3 {
        let state = rt.run_until_blocked(parent).await.unwrap();
        let children = children_of(&rt, parent);
        assert_eq!(
            children.len(),
            1,
            "a re-drive must not start a second child"
        );
        assert!(
            matches!(state, RunState::WaitingWorkflow(id) if id == children[0]),
            "the parent waits on its child: {state:?}"
        );
    }
    let child = children_of(&rt, parent)[0];
    let state = rt.run_until_blocked(child).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(ref s) if s == "go"));

    rt.send_signal(child, "go", json!("ready")).unwrap();
    let state = rt.run_until_blocked(parent).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v["child"] == json!("ready")),
        "{state:?}"
    );
    assert_replays(&rt, parent, "parent_of_waiter").await;
    assert_replays(&rt, child, "waits_for_go").await;
}

#[tokio::test]
async fn a_typed_child_failure_surfaces_in_the_parent() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_reports_child_failure", json!("child_typed_failure"))
        .unwrap();

    rt.run_until_blocked(parent).await.unwrap();
    let out = completed(&rt, parent);
    assert_eq!(out["name"], json!("child-workflow:child_typed_failure"));
    assert_eq!(out["reason"], json!("card declined"));
    assert_eq!(out["error_type"], json!("PaymentDeclined"));
    assert_eq!(out["details"], json!({ "code": 51 }));

    let failed = rt
        .load_history(parent)
        .unwrap()
        .into_iter()
        .find(|e| matches!(e, WorkflowEvent::ChildWorkflowFailed { .. }));
    assert!(
        matches!(
            failed,
            Some(WorkflowEvent::ChildWorkflowFailed { ref error, ref error_type, ref details, .. })
                if error == "card declined"
                    && error_type.as_deref() == Some("PaymentDeclined")
                    && details.as_ref() == Some(&json!({ "code": 51 }))
        ),
        "{failed:?}"
    );
    let child = children_of(&rt, parent)[0];
    assert!(
        matches!(rt.outcome(child).unwrap(), ExecutionOutcome::Failed(ref m) if m == "card declined")
    );
    assert_replays(&rt, parent, "parent_reports_child_failure").await;
    assert_replays(&rt, child, "child_typed_failure").await;
}

#[tokio::test]
async fn an_unsupported_child_surfaces_as_a_typed_failure() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_reports_child_failure", json!("child_unsupported"))
        .unwrap();

    rt.run_until_blocked(parent).await.unwrap();
    let out = completed(&rt, parent);
    assert_eq!(out["error_type"], json!(UNSUPPORTED_FEATURE_ERROR_TYPE));
    assert_eq!(out["details"]["feature"], json!("ScheduleExternalActivity"));
    assert_eq!(out["non_retryable"], json!(true));
    assert_replays(&rt, parent, "parent_reports_child_failure").await;
}

#[tokio::test]
async fn an_unregistered_child_workflow_leaves_the_parent_running() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_of_unregistered", json!(0))
        .unwrap();

    let err = rt.run_until_blocked(parent).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UnknownWorkflow(ref n) if n == "registered_late"),
        "{err}"
    );
    assert!(matches!(
        rt.outcome(parent).unwrap(),
        ExecutionOutcome::Running
    ));
    assert!(children_of(&rt, parent).is_empty(), "the cycle rolled back");

    rt.register_workflow(&registered_late_info());
    let state = rt.run_until_blocked(parent).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!(6)),
        "{state:?}"
    );
    assert_replays(&rt, parent, "parent_of_unregistered").await;
}

#[tokio::test]
async fn an_abandoned_child_dispatch_is_recorded_and_never_runs() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("fails_while_dispatching", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(parent).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(ref m) if m == "boom"),
        "{state:?}"
    );
    let history = rt.load_history(parent).unwrap();
    let child = children_of(&rt, parent)[0];
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ChildWorkflowFailed { child_id, error, non_retryable: Some(true), .. }
                if *child_id == child && error == autumn_harvest::event::ABANDONED_DISPATCH_REASON
        )),
        "{history:#?}"
    );
    assert!(
        matches!(rt.outcome(child), Err(SqliteError::ExecutionNotFound(_))),
        "an abandoned child gets no row"
    );
    assert_replays(&rt, parent, "fails_while_dispatching").await;
}

#[tokio::test]
async fn a_parent_and_its_child_survive_a_reopen() {
    let (_dir, path) = temp_db();
    let (parent, child) = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        register_all(&mut rt);
        let parent = rt.start_workflow("parent_of_waiter", json!(0)).unwrap();
        rt.run_until_blocked(parent).await.unwrap();
        (parent, children_of(&rt, parent)[0])
    };

    let mut rt = SqliteRuntime::open(&path).unwrap();
    register_all(&mut rt);
    rt.send_signal(child, "go", json!(42)).unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, parent), json!({ "child": 42 }));
    assert_eq!(children_of(&rt, parent).len(), 1);
}

#[tokio::test]
async fn a_child_timeout_race_cancels_the_losing_child() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register_all(&mut rt);
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let parent = rt.start_workflow("parent_child_timeout", json!(0)).unwrap();

    let state = rt.run_until_blocked_as_of(parent, t0).await.unwrap();
    assert!(matches!(state, RunState::WaitingWorkflow(_)), "{state:?}");
    let child = children_of(&rt, parent)[0];

    let later = t0 + chrono::Duration::seconds(61);
    let state = rt.run_until_blocked_as_of(parent, later).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "child": null })),
        "the deadline wins: {state:?}"
    );
    assert!(matches!(
        rt.outcome(child).unwrap(),
        ExecutionOutcome::Terminated(ref s) if s == "CANCELLED"
    ));
    assert!(rt.load_history(parent).unwrap().iter().any(|e| matches!(
        e,
        WorkflowEvent::ChildWorkflowFailed { error, .. }
            if error == "child workflow cancelled: lost race to a sibling branch"
    )));
    assert_no_open_rows(&path, child);
    assert_replays(&rt, parent, "parent_child_timeout").await;
    assert_replays(&rt, child, "waits_for_go").await;
}

#[tokio::test]
async fn mutual_awaits_block_instead_of_spinning() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_of_awaiting_child", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];
    assert!(
        matches!(state, RunState::WaitingWorkflow(id) if id == child),
        "{state:?}"
    );
    assert!(!rt.poll_once().await.unwrap(), "both runs are blocked");
    rt.run_until_idle().await.unwrap();
}

// ── Detached children and the parent-close policy ───────────────────────────

#[tokio::test]
async fn a_detached_child_reports_through_await() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_detached_awaits", json!(5))
        .unwrap();

    let state = rt.run_until_blocked(parent).await.unwrap();
    let RunState::Completed(out) = state else {
        panic!("{state:?}");
    };
    let child = children_of(&rt, parent)[0];
    assert_eq!(out["child_id"], json!(child.to_string()));
    assert_eq!(out["out"]["doubled"], json!(10));
    assert_eq!(out["out"]["parent"], json!(parent.to_string()));

    let kinds: Vec<&str> = rt
        .load_history(parent)
        .unwrap()
        .iter()
        .map(WorkflowEvent::type_name)
        .collect();
    assert_eq!(
        kinds,
        [
            "WorkflowStarted",
            "ChildWorkflowSpawnedDetached",
            "ExternalAwaitRequested",
            "ExternalAwaitResolved",
            "WorkflowCompleted"
        ]
    );
    assert_replays(&rt, parent, "parent_detached_awaits").await;
    assert_replays(&rt, child, "child_work").await;
}

async fn run_detached(
    policy: &str,
    then_activity: bool,
) -> (
    tempfile::TempDir,
    std::path::PathBuf,
    SqliteRuntime,
    ExecutionId,
    ExecutionId,
) {
    let (dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register_all(&mut rt);
    let parent = rt
        .start_workflow(
            "parent_spawns_detached",
            json!({ "policy": policy, "then_activity": then_activity }),
        )
        .unwrap();
    let state = rt.run_until_blocked(parent).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let child = children_of(&rt, parent)[0];
    assert_eq!(completed(&rt, parent), json!(child.to_string()));
    (dir, path, rt, parent, child)
}

fn cascade_actions(rt: &SqliteRuntime, parent: ExecutionId) -> Vec<String> {
    let history = rt.load_history(parent).unwrap();
    let terminal = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::WorkflowCompleted { .. }))
        .expect("the parent completed");
    history[terminal..]
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ChildWorkflowCascadeApplied { action, .. } => Some(action.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn request_cancel_cancels_a_running_detached_child() {
    for then_activity in [false, true] {
        let (_dir, path, rt, parent, child) = run_detached("request_cancel", then_activity).await;
        assert!(
            matches!(rt.outcome(child).unwrap(), ExecutionOutcome::Terminated(ref s) if s == "CANCELLED"),
            "then_activity={then_activity}"
        );
        assert!(matches!(
            rt.load_history(child).unwrap().last(),
            Some(WorkflowEvent::WorkflowCancelled { reason }) if reason == "parent closed"
        ));
        assert_eq!(cascade_actions(&rt, parent), ["request_cancel"]);
        assert_no_open_rows(&path, child);
        assert_replays(&rt, parent, "parent_spawns_detached").await;
        assert_replays(&rt, child, "waits_for_go").await;
    }
}

#[tokio::test]
async fn terminate_fails_a_running_detached_child() {
    let (_dir, path, rt, parent, child) = run_detached("terminate", true).await;
    assert!(
        matches!(rt.outcome(child).unwrap(), ExecutionOutcome::Failed(ref m) if m == "ParentClosed")
    );
    assert!(matches!(
        rt.load_history(child).unwrap().last(),
        Some(WorkflowEvent::WorkflowFailed { error, error_type: None, .. }) if error == "ParentClosed"
    ));
    assert_eq!(cascade_actions(&rt, parent), ["terminate"]);
    assert_no_open_rows(&path, child);
    assert_replays(&rt, parent, "parent_spawns_detached").await;
}

#[tokio::test]
async fn abandon_leaves_a_detached_child_running() {
    let (_dir, _path, mut rt, parent, child) = run_detached("abandon", false).await;
    assert!(matches!(
        rt.outcome(child).unwrap(),
        ExecutionOutcome::Running
    ));
    assert_eq!(cascade_actions(&rt, parent), Vec::<String>::new());

    rt.send_signal(child, "go", json!("later")).unwrap();
    let state = rt.run_until_blocked(child).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v == &json!("later")));
    assert_eq!(
        rt.load_history(parent).unwrap().len(),
        3,
        "a detached child never writes to its parent"
    );
}

// ── External signal ─────────────────────────────────────────────────────────

#[tokio::test]
async fn a_signal_reaches_another_run() {
    let mut rt = runtime();
    let target = rt.start_workflow("waits_for_go", json!(0)).unwrap();
    rt.run_until_blocked(target).await.unwrap();
    let sender = rt.start_workflow("signal_reports", json!(target)).unwrap();

    let state = rt.run_until_blocked(sender).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("delivered")),
        "{state:?}"
    );
    let kinds: Vec<&str> = rt
        .load_history(sender)
        .unwrap()
        .iter()
        .map(WorkflowEvent::type_name)
        .collect();
    assert_eq!(
        kinds,
        [
            "WorkflowStarted",
            "ExternalSignalRequested",
            "ExternalSignalDelivered",
            "WorkflowCompleted"
        ]
    );

    let state = rt.run_until_blocked(target).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "from": "signaller" })),
        "{state:?}"
    );
    assert_replays(&rt, sender, "signal_reports").await;
    assert_replays(&rt, target, "waits_for_go").await;
}

#[tokio::test]
async fn a_signal_reports_unknown_and_terminal_targets() {
    let mut rt = runtime();
    let unknown = rt
        .start_workflow("signal_reports", json!(ExecutionId::new()))
        .unwrap();
    let done = rt.start_workflow("child_typed_failure", json!(0)).unwrap();
    rt.run_until_blocked(done).await.unwrap();
    let terminal = rt.start_workflow("signal_reports", json!(done)).unwrap();

    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, unknown), json!("target_unknown"));
    assert_eq!(completed(&rt, terminal), json!("target_terminal"));
    assert_replays(&rt, unknown, "signal_reports").await;
    assert_replays(&rt, terminal, "signal_reports").await;
}

#[tokio::test]
async fn a_signal_by_workflow_id_reaches_the_current_run() {
    let mut rt = runtime();
    let target = rt
        .start_workflow_with_id("waits_for_go", "order-1", json!(0))
        .unwrap();
    let live = rt
        .start_workflow(
            "signal_by_id_reports",
            json!({ "name": "waits_for_go", "id": "order-1" }),
        )
        .unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, live), json!("delivered"));
    assert_eq!(completed(&rt, target), json!("by-id"));

    let ended = rt
        .start_workflow(
            "signal_by_id_reports",
            json!({ "name": "waits_for_go", "id": "order-1" }),
        )
        .unwrap();
    let missing = rt
        .start_workflow(
            "signal_by_id_reports",
            json!({ "name": "waits_for_go", "id": "nobody" }),
        )
        .unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, ended), json!("not_running"));
    assert_eq!(completed(&rt, missing), json!("target_unknown"));
    assert_replays(&rt, live, "signal_by_id_reports").await;
}

#[tokio::test]
async fn a_keyed_signal_is_staged_once() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register_all(&mut rt);
    let target = rt.start_workflow("waits_for_go", json!(0)).unwrap();
    rt.run_until_blocked(target).await.unwrap();
    let sender = rt
        .start_workflow("signal_twice_with_key", json!(target))
        .unwrap();

    let state = rt.run_until_blocked(sender).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    assert_eq!(
        count_rows(
            &path,
            "SELECT COUNT(*) FROM harvest_signals WHERE exec_id = ?1",
            target
        ),
        1,
        "the second send with the same key is a duplicate"
    );
    let delivered = rt
        .load_history(sender)
        .unwrap()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ExternalSignalDelivered { .. }))
        .count();
    assert_eq!(delivered, 2, "a duplicate still reports delivered");
    assert_replays(&rt, sender, "signal_twice_with_key").await;
}

// ── External cancel ─────────────────────────────────────────────────────────

#[tokio::test]
async fn a_cancel_ends_another_run() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register_all(&mut rt);
    let target = rt.start_workflow("waits_for_go", json!(0)).unwrap();
    rt.run_until_blocked(target).await.unwrap();
    rt.send_signal(target, "other", json!(1)).unwrap();
    let canceller = rt.start_workflow("cancel_reports", json!(target)).unwrap();

    let state = rt.run_until_blocked(canceller).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("delivered")),
        "{state:?}"
    );
    let kinds: Vec<&str> = rt
        .load_history(canceller)
        .unwrap()
        .iter()
        .map(WorkflowEvent::type_name)
        .collect();
    assert_eq!(
        kinds,
        [
            "WorkflowStarted",
            "ExternalCancelRequested",
            "ExternalCancelDelivered",
            "WorkflowCompleted"
        ]
    );
    assert!(matches!(
        rt.outcome(target).unwrap(),
        ExecutionOutcome::Terminated(ref s) if s == "CANCELLED"
    ));
    assert!(matches!(
        rt.load_history(target).unwrap().last(),
        Some(WorkflowEvent::WorkflowCancelled { reason }) if reason == "cancelled by external request"
    ));
    assert_no_open_rows(&path, target);
    assert!(matches!(
        rt.send_signal(target, "go", json!(1)),
        Err(SqliteError::WorkflowNotRunning { .. })
    ));
    assert_replays(&rt, canceller, "cancel_reports").await;
    assert_replays(&rt, target, "waits_for_go").await;
}

#[tokio::test]
async fn a_cancel_reports_unknown_and_terminal_targets() {
    let mut rt = runtime();
    let unknown = rt
        .start_workflow("cancel_reports", json!(ExecutionId::new()))
        .unwrap();
    let done = rt.start_workflow("child_typed_failure", json!(0)).unwrap();
    rt.run_until_blocked(done).await.unwrap();
    let terminal = rt.start_workflow("cancel_reports", json!(done)).unwrap();
    let by_id = rt
        .start_workflow(
            "cancel_by_id_reports",
            json!({ "name": "waits_for_go", "id": "nobody" }),
        )
        .unwrap();

    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, unknown), json!("target_unknown"));
    assert_eq!(completed(&rt, terminal), json!("delivered"));
    assert_eq!(completed(&rt, by_id), json!("target_unknown"));
    assert!(
        matches!(rt.outcome(done).unwrap(), ExecutionOutcome::Failed(_)),
        "a terminal target is unchanged"
    );
    assert_replays(&rt, unknown, "cancel_reports").await;
}

#[tokio::test]
async fn a_cancel_by_workflow_id_ends_the_current_run() {
    let mut rt = runtime();
    let target = rt
        .start_workflow_with_id("waits_for_go", "order-2", json!(0))
        .unwrap();
    let canceller = rt
        .start_workflow(
            "cancel_by_id_reports",
            json!({ "name": "waits_for_go", "id": "order-2" }),
        )
        .unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, canceller), json!("delivered"));
    assert!(matches!(
        rt.outcome(target).unwrap(),
        ExecutionOutcome::Terminated(ref s) if s == "CANCELLED"
    ));
}

#[tokio::test]
async fn cancelling_an_awaited_child_fails_its_parent() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_reports_waiter", json!(0))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];
    let canceller = rt.start_workflow("cancel_reports", json!(child)).unwrap();

    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, canceller), json!("delivered"));
    assert_eq!(
        completed(&rt, parent),
        json!("child workflow cancelled: cancelled by external request")
    );
    assert_replays(&rt, parent, "parent_reports_waiter").await;
}

// ── External await ──────────────────────────────────────────────────────────

#[tokio::test]
async fn an_await_reports_a_typed_target_failure() {
    let mut rt = runtime();
    let target = rt.start_workflow("child_typed_failure", json!(0)).unwrap();
    let awaiter = rt.start_workflow("await_reports", json!(target)).unwrap();

    let state = rt.run_until_blocked(awaiter).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let out = completed(&rt, awaiter);
    assert_eq!(out["failed"], json!("card declined"));
    assert_eq!(out["error_type"], json!("PaymentDeclined"));
    assert_eq!(out["details"], json!({ "code": 51 }));
    assert_replays(&rt, awaiter, "await_reports").await;
}

#[tokio::test]
async fn an_await_of_an_unknown_target_fails_at_once() {
    let mut rt = runtime();
    let awaiter = rt
        .start_workflow("await_reports", json!(ExecutionId::new()))
        .unwrap();
    rt.run_until_blocked(awaiter).await.unwrap();
    assert_eq!(
        completed(&rt, awaiter),
        json!({ "await_failed": "target_unknown" })
    );
    assert_replays(&rt, awaiter, "await_reports").await;
}

#[tokio::test]
async fn an_await_of_a_cancelled_target_reports_it() {
    let mut rt = runtime();
    let target = rt.start_workflow("waits_for_go", json!(0)).unwrap();
    let awaiter = rt.start_workflow("await_reports", json!(target)).unwrap();
    let state = rt.run_until_blocked(awaiter).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingWorkflow(id) if id == target),
        "{state:?}"
    );

    let canceller = rt.start_workflow("cancel_reports", json!(target)).unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, canceller), json!("delivered"));
    let out = completed(&rt, awaiter);
    assert_eq!(out["error_type"], json!("target_cancelled"));
    assert_eq!(out["failed"], json!("cancelled by external request"));
    assert_replays(&rt, awaiter, "await_reports").await;
}

/// A cancel seals a run that is parked on a timer. Its history must replay.
#[tokio::test]
async fn a_cancelled_timer_wait_replays() {
    let mut rt = runtime();
    let target = rt.start_workflow("waits_on_timer", json!(0)).unwrap();
    let state = rt.run_until_blocked(target).await.unwrap();
    assert!(matches!(state, RunState::WaitingTimer), "{state:?}");
    let canceller = rt.start_workflow("cancel_reports", json!(target)).unwrap();
    rt.run_until_blocked(canceller).await.unwrap();
    assert!(matches!(
        rt.outcome(target).unwrap(),
        ExecutionOutcome::Terminated(ref s) if s == "CANCELLED"
    ));
    assert_replays(&rt, target, "waits_on_timer").await;
}

/// Closing the root cascades down a chain of detached children. The cascade
/// uses a worklist, so each level is cancelled and records its own cascade.
#[tokio::test]
async fn a_cascade_reaches_every_level_of_a_detached_chain() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register_all(&mut rt);
    let root = rt.start_workflow("detached_chain", json!(3)).unwrap();
    rt.run_until_idle().await.unwrap();

    let mut chain = vec![root];
    while let Some(next) = children_of(&rt, *chain.last().unwrap()).first().copied() {
        chain.push(next);
    }
    assert_eq!(chain.len(), 4, "the root and three detached levels");

    rt.send_signal(root, "go", json!("done")).unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, root), json!("done"));
    for (level, run) in chain.iter().enumerate().skip(1) {
        assert!(
            matches!(rt.outcome(*run).unwrap(), ExecutionOutcome::Terminated(ref s) if s == "CANCELLED"),
            "level {level} must be cancelled"
        );
        assert_no_open_rows(&path, *run);
    }
    for (level, run) in chain.iter().enumerate().take(3) {
        let cascades = rt
            .load_history(*run)
            .unwrap()
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowCascadeApplied { .. }))
            .count();
        assert_eq!(cascades, 1, "level {level} records one cascade");
    }
    for run in &chain {
        assert_replays(&rt, *run, "detached_chain").await;
    }
}

/// A `join!` of an await and a child completes in either order, and replays.
#[tokio::test]
async fn a_join_of_an_await_and_a_child_completes() {
    for workflow in ["joins_await_then_child", "joins_child_then_await"] {
        let mut rt = runtime();
        let target = rt.start_workflow("waits_for_go", json!(0)).unwrap();
        rt.run_until_blocked(target).await.unwrap();
        let parent = rt.start_workflow(workflow, json!(target)).unwrap();
        let state = rt.run_until_blocked(parent).await;
        let state = state
            .unwrap_or_else(|e| panic!("{workflow}: {e}\n{:#?}", rt.load_history(parent).unwrap()));
        assert!(
            matches!(state, RunState::WaitingWorkflow(id) if id == target),
            "{workflow}: {state:?}"
        );

        rt.send_signal(target, "go", json!("ready")).unwrap();
        let state = rt.run_until_blocked(parent).await;
        let state = state
            .unwrap_or_else(|e| panic!("{workflow}: {e}\n{:#?}", rt.load_history(parent).unwrap()));
        assert!(
            matches!(state, RunState::Completed(ref v) if v == &json!({ "awaited": "ready", "child": 4 })),
            "{workflow}: {state:?}\n{:#?}",
            rt.load_history(parent).unwrap()
        );
        assert_replays(&rt, parent, workflow).await;
    }
}

// ── Review follow-ups ───────────────────────────────────────────────────────

#[tokio::test]
async fn an_uncaught_child_failure_fails_the_parent() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_propagates", json!("child_typed_failure"))
        .unwrap();
    let state = rt.run_until_blocked(parent).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(ref m) if m.contains("card declined")),
        "{state:?}"
    );
    assert_replays(&rt, parent, "parent_propagates").await;
}

#[tokio::test]
async fn a_typed_child_failure_keeps_non_retryable() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_reports_child_failure", json!("child_typed_failure"))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    assert_eq!(completed(&rt, parent)["non_retryable"], json!(false));
}

/// A panicking child keeps the drive's error until its budget runs out. Then
/// it is sealed, and the parent records the typed panic.
#[tokio::test]
async fn a_panicking_child_fails_its_parent_after_the_budget() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_reports_child_failure", json!("child_panics"))
        .unwrap();
    let mut panics = 0;
    let state = loop {
        match rt.run_until_blocked(parent).await {
            Err(SqliteError::WorkflowPanicked { .. }) => panics += 1,
            other => break other.unwrap(),
        }
        assert!(panics < 5, "the panic budget must seal the child");
    };
    assert_eq!(panics, 2, "two strikes stay resumable");
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    assert_eq!(
        completed(&rt, parent)["error_type"],
        json!(ERROR_TYPE_HANDLER_PANIC)
    );
    assert_replays(&rt, parent, "parent_reports_child_failure").await;
}

/// The root names the run it waits on directly, not the leaf that needs input.
#[tokio::test]
async fn a_three_level_chain_names_the_direct_child_and_completes() {
    let mut rt = runtime();
    let root = rt.start_workflow("attached_chain", json!(2)).unwrap();
    let state = rt.run_until_blocked(root).await.unwrap();
    let middle = children_of(&rt, root)[0];
    let leaf = children_of(&rt, middle)[0];
    assert!(
        matches!(state, RunState::WaitingWorkflow(id) if id == middle),
        "{state:?}"
    );
    assert!(!rt.poll_once().await.unwrap(), "every run is blocked");

    rt.send_signal(leaf, "go", json!("deep")).unwrap();
    let state = rt.run_until_blocked(root).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("deep")),
        "{state:?}"
    );
    for run in [root, middle, leaf] {
        assert_replays(&rt, run, "attached_chain").await;
    }
}

/// A blocked waited-on run does not starve a sibling that needs no input.
#[tokio::test]
async fn a_blocked_target_does_not_starve_a_sibling_child() {
    let mut rt = runtime();
    let target = rt.start_workflow("waits_for_go", json!(0)).unwrap();
    let parent = rt
        .start_workflow("joins_await_and_signaller", json!(target))
        .unwrap();
    let state = rt.run_until_blocked(parent).await.unwrap();
    assert!(
        matches!(
            state,
            RunState::Completed(ref v)
                if v == &json!({ "awaited": { "from": "signaller" }, "sent": "delivered" })
        ),
        "one drive must finish the parent: {state:?}"
    );
    assert_replays(&rt, parent, "joins_await_and_signaller").await;
    assert_replays(&rt, target, "waits_for_go").await;
}

static WATCHDOG_EFFECTS: AtomicUsize = AtomicUsize::new(0);

/// A run that the cascade of its own cancel seals applies no more of its batch.
#[tokio::test]
async fn a_run_sealed_by_its_own_cancel_runs_no_more_work() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register_all(&mut rt);
    rt.register_activity_raw(
        "watchdog_effect",
        ActivitySpec::new(1, |v: Value| {
            WATCHDOG_EFFECTS.fetch_add(1, Ordering::SeqCst);
            Ok(v)
        }),
    );
    let parent = rt.start_workflow("parent_with_watchdog", json!(0)).unwrap();
    rt.run_until_idle().await.unwrap();

    let dog = children_of(&rt, parent)[0];
    for run in [parent, dog] {
        assert!(matches!(
            rt.outcome(run).unwrap(),
            ExecutionOutcome::Terminated(ref s) if s == "CANCELLED"
        ));
        assert_no_open_rows(&path, run);
    }
    assert_eq!(
        WATCHDOG_EFFECTS.load(Ordering::SeqCst),
        0,
        "the body must never run"
    );
    let kinds: Vec<&str> = rt
        .load_history(dog)
        .unwrap()
        .iter()
        .map(WorkflowEvent::type_name)
        .collect();
    assert_eq!(
        kinds,
        [
            "WorkflowStarted",
            "ExternalCancelRequested",
            "ExternalCancelDelivered",
            "WorkflowCancelled"
        ]
    );
    assert_replays(&rt, dog, "watchdog").await;
}

#[tokio::test]
async fn a_detached_spawn_in_a_failing_cycle_is_cascaded() {
    let (_dir, path) = temp_db();
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register_all(&mut rt);
    let parent = rt
        .start_workflow("spawns_detached_then_fails", json!(0))
        .unwrap();
    let state = rt.run_until_blocked(parent).await.unwrap();
    assert!(
        matches!(state, RunState::Failed(ref m) if m == "parent gave up"),
        "{state:?}"
    );
    let child = children_of(&rt, parent)[0];
    assert!(matches!(
        rt.outcome(child).unwrap(),
        ExecutionOutcome::Terminated(ref s) if s == "CANCELLED"
    ));
    assert_no_open_rows(&path, child);
    let kinds: Vec<&str> = rt
        .load_history(parent)
        .unwrap()
        .iter()
        .map(WorkflowEvent::type_name)
        .collect();
    assert_eq!(
        kinds,
        [
            "WorkflowStarted",
            "ChildWorkflowSpawnedDetached",
            "WorkflowFailed",
            "ChildWorkflowCascadeApplied"
        ]
    );
    assert_replays(&rt, parent, "spawns_detached_then_fails").await;
}

/// `TerminateIfRunning` supersedes a running child. The parent sees a cancel.
#[tokio::test]
async fn a_reuse_seal_of_a_running_child_fails_its_parent() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_reports_waiter", json!(0))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];
    rt.start_workflow_with_reuse_policy(
        "waits_for_go",
        &child.to_string(),
        json!(0),
        WorkflowIdReusePolicy::TerminateIfRunning,
    )
    .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    assert_eq!(
        completed(&rt, parent),
        json!("child workflow cancelled: superseded by a TerminateIfRunning start (#1068)")
    );
    assert_replays(&rt, parent, "parent_reports_waiter").await;
}

/// An await reports a run that a reuse seal superseded by its last terminal.
#[tokio::test]
async fn an_await_of_a_superseded_target_reports_its_last_terminal() {
    let mut rt = runtime();
    // A completed run that a restart later sealed still resolves the await.
    let done = rt
        .start_workflow_with_id("child_typed_failure", "sealed-1", json!(0))
        .unwrap();
    rt.run_until_blocked(done).await.unwrap();
    let running = rt
        .start_workflow_with_id("waits_for_go", "sealed-2", json!(0))
        .unwrap();
    rt.run_until_blocked(running).await.unwrap();
    for (name, id) in [
        ("child_typed_failure", "sealed-1"),
        ("waits_for_go", "sealed-2"),
    ] {
        rt.start_workflow_with_reuse_policy(
            name,
            id,
            json!(0),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap();
    }
    let on_failed = rt.start_workflow("await_reports", json!(done)).unwrap();
    let on_running = rt.start_workflow("await_reports", json!(running)).unwrap();
    rt.run_until_blocked(on_failed).await.unwrap();
    rt.run_until_blocked(on_running).await.unwrap();

    assert_eq!(
        completed(&rt, on_failed)["error_type"],
        json!("PaymentDeclined")
    );
    let out = completed(&rt, on_running);
    assert_eq!(out["error_type"], json!("target_cancelled"));
    assert_eq!(
        out["failed"],
        json!("superseded by a TerminateIfRunning start (#1068)")
    );
}

/// `AllowDuplicateFailedOnly` replaces a cancelled run, as core does.
#[tokio::test]
async fn failed_only_reuse_replaces_a_cancelled_run() {
    let mut rt = runtime();
    let first = rt
        .start_workflow_with_id("waits_for_go", "reuse-1", json!(0))
        .unwrap();
    let canceller = rt.start_workflow("cancel_reports", json!(first)).unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, canceller), json!("delivered"));

    let outcome = rt
        .start_workflow_with_reuse_policy(
            "waits_for_go",
            "reuse-1",
            json!(0),
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        )
        .unwrap();
    assert!(outcome.created, "a cancelled prior is replaced");
    assert_ne!(outcome.exec_id, first);
}

/// A keyed signal row outlives a cancel of its target, so a duplicate send
/// still dedups, as on Postgres.
#[tokio::test]
async fn a_signal_key_survives_a_cancel_of_its_target() {
    let mut rt = runtime();
    let target = rt.start_workflow("waits_for_go", json!(0)).unwrap();
    rt.run_until_blocked(target).await.unwrap();
    let args = json!({ "target": target, "key": "k" });
    let first = rt.start_workflow("keyed_signal", args.clone()).unwrap();
    rt.run_until_blocked(first).await.unwrap();
    let canceller = rt.start_workflow("cancel_reports", json!(target)).unwrap();
    rt.run_until_blocked(canceller).await.unwrap();

    let again = rt.start_workflow("keyed_signal", args).unwrap();
    rt.run_until_blocked(again).await.unwrap();
    assert_eq!(completed(&rt, first), json!("delivered"));
    assert_eq!(
        completed(&rt, again),
        json!("delivered"),
        "a duplicate key dedups"
    );

    let unkeyed = rt
        .start_workflow("keyed_signal", json!({ "target": target, "key": "" }))
        .unwrap();
    rt.run_until_blocked(unkeyed).await.unwrap();
    assert_eq!(
        completed(&rt, unkeyed),
        json!("target_terminal"),
        "an empty key is no key"
    );
}

/// A by-id key dedups across every run of that key.
#[tokio::test]
async fn a_by_id_signal_key_dedups_across_runs() {
    let mut rt = runtime();
    let first = rt
        .start_workflow_with_id("waits_for_go", "chain-1", json!(0))
        .unwrap();
    let key = json!({ "name": "waits_for_go", "id": "chain-1", "key": "k2" });
    let sender = rt
        .start_workflow("keyed_signal_by_id", key.clone())
        .unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, sender), json!("delivered"));
    assert_eq!(completed(&rt, first), json!("keyed"));

    let second = rt
        .start_workflow_with_reuse_policy(
            "waits_for_go",
            "chain-1",
            json!(0),
            WorkflowIdReusePolicy::TerminateIfRunning,
        )
        .unwrap()
        .exec_id;
    let again = rt.start_workflow("keyed_signal_by_id", key).unwrap();
    rt.run_until_idle().await.unwrap();
    assert_eq!(completed(&rt, again), json!("delivered"));
    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "go"),
        "the new run gets no duplicate: {state:?}"
    );
}

/// A child that ends after its parent's deadline loses to the deadline, as
/// on Postgres (issue #779).
#[tokio::test]
async fn a_late_child_loses_to_a_due_deadline() {
    let mut rt = runtime();
    let clock = Arc::new(AtomicI64::new(1_767_225_600_000));
    let read = Arc::clone(&clock);
    rt.set_clock(move || {
        chrono::DateTime::from_timestamp_millis(read.load(Ordering::SeqCst)).unwrap()
    });
    let bump = Arc::clone(&clock);
    rt.register_activity_raw(
        "slow",
        ActivitySpec::new(1, move |_| {
            bump.fetch_add(5_000, Ordering::SeqCst);
            Ok(json!("slow-done"))
        }),
    );
    let parent = rt
        .start_workflow("parent_child_timeout_slow", json!(0))
        .unwrap();
    let state = rt.run_until_blocked(parent).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "child": null })),
        "the deadline wins: {state:?}"
    );
    let history = rt.load_history(parent).unwrap();
    let fired = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerFired { .. }))
        .expect("the deadline fired");
    let child_done = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. }))
        .expect("the child result is still recorded");
    assert!(fired < child_done, "{history:#?}");
    assert_replays(&rt, parent, "parent_child_timeout_slow").await;
}

// ── Cancel, terminate and continue-as-new (issues #2021, #2023) ─────────────

/// An operator cancel of a child is cooperative. The child ends `CANCELLED`
/// at its next drive, and its parent sees the cancel.
#[tokio::test]
async fn an_operator_cancel_of_a_child_reaches_its_parent() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_reports_waiter", json!(0))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];

    rt.cancel(child, "ops").unwrap();
    rt.run_until_idle().await.unwrap();

    assert!(matches!(
        rt.outcome(child).unwrap(),
        ExecutionOutcome::Terminated(ref s) if s == "CANCELLED"
    ));
    assert_eq!(
        completed(&rt, parent),
        json!("child workflow cancelled: ops")
    );
    assert_replays(&rt, parent, "parent_reports_waiter").await;
}

/// A terminate of a child reaches its parent with the Postgres text.
#[tokio::test]
async fn a_terminate_of_a_child_reaches_its_parent() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_reports_waiter", json!(0))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];

    rt.terminate(child, "ops").unwrap();
    rt.run_until_idle().await.unwrap();

    assert_eq!(
        completed(&rt, parent),
        json!("child workflow terminated: ops")
    );
    assert_replays(&rt, parent, "parent_reports_waiter").await;
}

/// A terminated await target reports `target_terminated`, as on Postgres.
#[tokio::test]
async fn an_await_of_a_terminated_target_reports_it() {
    let mut rt = runtime();
    let target = rt.start_workflow("waits_for_go", json!(0)).unwrap();
    let awaiter = rt.start_workflow("await_reports", json!(target)).unwrap();
    rt.run_until_blocked(awaiter).await.unwrap();

    rt.terminate(target, "ops").unwrap();
    rt.run_until_blocked(awaiter).await.unwrap();

    let out = completed(&rt, awaiter);
    assert_eq!(out["error_type"], json!("target_terminated"));
    assert_eq!(out["failed"], json!("ops"));
    assert_replays(&rt, awaiter, "await_reports").await;
}

/// A child that continues as new fails with the Postgres text. Its parent
/// sees the failure.
#[tokio::test]
async fn a_child_cannot_continue_as_new() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_reports_child_failure", json!("continues_once"))
        .unwrap();

    rt.run_until_blocked(parent).await.unwrap();

    let out = completed(&rt, parent);
    assert_eq!(
        out["reason"],
        json!("continue_as_new is not supported in child workflows in this release")
    );
    let child = children_of(&rt, parent)[0];
    assert!(matches!(
        rt.outcome(child).unwrap(),
        ExecutionOutcome::Failed(_)
    ));
    assert_eq!(rt.successor(child).unwrap(), None);
    assert_replays(&rt, parent, "parent_reports_child_failure").await;
}

/// An await follows its target across a continue-as-new, as on Postgres. It
/// resolves with the output of the last run.
#[tokio::test]
async fn an_await_follows_a_continue_as_new() {
    let mut rt = runtime();
    let target = rt.start_workflow("continues_once", json!(0)).unwrap();
    let awaiter = rt.start_workflow("await_reports", json!(target)).unwrap();

    let state = rt.run_until_blocked(awaiter).await.unwrap();

    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    assert_eq!(completed(&rt, awaiter), json!({ "ok": 1 }));
    assert!(rt.successor(target).unwrap().is_some());
    assert_replays(&rt, awaiter, "await_reports").await;
}

/// The cap check before a terminal event counts the cascade events that the
/// close writes, as on Postgres. A parent one event below the cap fails.
#[tokio::test]
async fn the_terminal_cap_check_counts_cascade_events() {
    let mut rt = runtime();
    // `WorkflowStarted` and `ChildWorkflowSpawnedDetached` come first. The
    // cascade event then reaches the cap.
    rt.set_history_event_hard_cap(Some(3));
    let parent = rt
        .start_workflow(
            "parent_spawns_detached",
            json!({ "policy": "request_cancel" }),
        )
        .unwrap();

    let state = rt.run_until_blocked(parent).await.unwrap();

    assert!(
        matches!(state, RunState::Failed(ref m) if m.contains("HistoryCapExceeded")),
        "{state:?}"
    );
    assert!(children_of(&rt, parent).is_empty(), "the spawn rolls back");
}

/// An `abandon` child writes no cascade event, so the same parent completes.
#[tokio::test]
async fn an_abandon_child_adds_no_cascade_event_to_the_cap_check() {
    let mut rt = runtime();
    rt.set_history_event_hard_cap(Some(3));
    let parent = rt
        .start_workflow("parent_spawns_detached", json!({ "policy": "abandon" }))
        .unwrap();

    let state = rt.run_until_blocked(parent).await.unwrap();

    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
}

/// An update replays a child with its parent link, as a drive does.
#[tokio::test]
async fn an_update_on_a_child_sees_its_parent() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_of_update_target", json!(0))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];

    let out = rt.update(child, "parent", json!(null)).await.unwrap();

    assert_eq!(out, json!(parent.to_string()));
}

/// A query replays a child with its parent link, as a drive does.
#[tokio::test]
async fn a_query_on_a_child_sees_its_parent() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow("parent_of_update_target", json!(0))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];

    let out = rt.query(child, "parent", json!(null)).await.unwrap();

    assert_eq!(out, json!(parent.to_string()));
}

/// The close reads the clock again when the child ends. A terminal cycle that
/// runs past the parent's deadline loses to it, as on Postgres.
#[tokio::test]
async fn a_long_terminal_cycle_loses_to_a_due_deadline() {
    let mut rt = runtime();
    rt.set_clock(|| {
        chrono::DateTime::from_timestamp_millis(BUSY_CLOCK.load(Ordering::SeqCst)).unwrap()
    });
    let parent = rt
        .start_workflow("parent_child_timeout_busy", json!(0))
        .unwrap();

    let state = rt.run_until_blocked(parent).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "child": null })),
        "the deadline wins: {state:?}"
    );
    let history = rt.load_history(parent).unwrap();
    let fired = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerFired { .. }))
        .expect("the deadline fired");
    let child_done = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. }))
        .expect("the child result is still recorded");
    assert!(fired < child_done, "{history:#?}");
}

/// A cancelled child closes at the time its cleanup ends. A cleanup that
/// runs past the parent's deadline loses to it.
#[tokio::test]
async fn a_long_cancel_cleanup_loses_to_a_due_deadline() {
    let mut rt = runtime();
    rt.set_clock(|| {
        chrono::DateTime::from_timestamp_millis(CANCEL_CLOCK.load(Ordering::SeqCst)).unwrap()
    });
    let parent = rt
        .start_workflow("parent_child_timeout_cancel", json!(0))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];

    rt.cancel(child, "ops").unwrap();
    let state = rt.run_until_blocked(parent).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "child": null })),
        "the deadline wins: {state:?}"
    );
    let history = rt.load_history(parent).unwrap();
    let fired = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerFired { .. }))
        .expect("the deadline fired");
    let child_done = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ChildWorkflowFailed { .. }))
        .expect("the child result is still recorded");
    assert!(fired < child_done, "{history:#?}");
}

/// A signal from another run arrives when the sender writes it. A sender
/// cycle that runs past the target's deadline loses to it.
#[tokio::test]
async fn a_signal_sent_after_a_deadline_loses_to_it() {
    let mut rt = runtime();
    rt.set_clock(|| {
        chrono::DateTime::from_timestamp_millis(SIGNAL_CLOCK.load(Ordering::SeqCst)).unwrap()
    });
    let target = rt.start_workflow("waits_for_go_briefly", json!(0)).unwrap();
    rt.run_until_blocked(target).await.unwrap();
    let sender = rt.start_workflow("busy_signaller", json!(target)).unwrap();

    rt.run_until_blocked(sender).await.unwrap();
    let state = rt.run_until_blocked(target).await.unwrap();

    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("timeout")),
        "the deadline wins: {state:?}"
    );
}

/// The panic seal closes the child at the time its last run ends. A last run
/// that goes past the parent's deadline loses to it.
#[tokio::test]
async fn a_long_final_panic_loses_to_a_due_deadline() {
    let mut rt = runtime();
    rt.set_clock(|| {
        chrono::DateTime::from_timestamp_millis(PANIC_CLOCK.load(Ordering::SeqCst)).unwrap()
    });
    let parent = rt
        .start_workflow("parent_child_timeout_panic", json!(0))
        .unwrap();
    let state = loop {
        match rt.run_until_blocked(parent).await {
            Err(SqliteError::WorkflowPanicked { .. }) => {}
            other => break other.unwrap(),
        }
        assert!(
            PANIC_CALLS.load(Ordering::SeqCst) < 5,
            "the budget seals it"
        );
    };

    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!({ "child": null })),
        "the deadline wins: {state:?}"
    );
}

/// The cap check of a cancel's cleanup batch counts the cascade events of the
/// seal. A batch that would reach the cap with them rolls back, and the seal
/// adds only `WorkflowCancelled` and the cascade.
#[tokio::test]
async fn the_cancel_cap_check_counts_cascade_events() {
    let mut rt = runtime();
    // `WorkflowStarted` and `ChildWorkflowSpawnedDetached` come first. The
    // side effect and the cascade event then reach the cap.
    rt.set_history_event_hard_cap(Some(4));
    let parent = rt
        .start_workflow("spawns_then_cleans_up", json!(0))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();

    rt.cancel(parent, "ops").unwrap();
    let state = rt.run_until_blocked(parent).await.unwrap();

    assert!(matches!(state, RunState::Cancelled(_)), "{state:?}");
    let history = rt.load_history(parent).unwrap();
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. })),
        "the cleanup batch rolls back: {history:#?}"
    );
    let cascades = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowCascadeApplied { .. }))
        .count();
    assert_eq!(cascades, 1, "{history:#?}");
}

/// The fallback cancel seal counts the cascade events too. Task failures that
/// would reach the cap with them roll back.
#[tokio::test]
async fn the_fallback_cancel_cap_check_counts_cascade_events() {
    let mut rt = runtime();
    // `WorkflowStarted`, `ChildWorkflowSpawnedDetached` and `ActivityScheduled`
    // come first. The task failure and the cascade event then reach the cap.
    rt.set_history_event_hard_cap(Some(5));
    let parent = rt
        .start_workflow("spawns_then_waits_on_missing", json!(0))
        .unwrap();
    let err = rt.run_until_blocked(parent).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UnregisteredActivity(_)),
        "{err:?}"
    );

    rt.cancel(parent, "ops").unwrap();
    let state = rt.run_until_blocked(parent).await;

    assert!(matches!(state, Ok(RunState::Cancelled(_))), "{state:?}");
    let history = rt.load_history(parent).unwrap();
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "the task failure rolls back: {history:#?}"
    );
    let cascades = history
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ChildWorkflowCascadeApplied { .. }))
        .count();
    assert_eq!(cascades, 1, "{history:#?}");
}

/// A child that continues as new keeps the bookkeeping of its last cycle. The
/// side effect precedes the failure, as for any failing cycle.
#[tokio::test]
async fn a_child_continue_as_new_keeps_its_side_effects() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow(
            "parent_reports_child_failure",
            json!("mints_then_continues"),
        )
        .unwrap();

    rt.run_until_blocked(parent).await.unwrap();

    let child = children_of(&rt, parent)[0];
    let history = rt.load_history(child).unwrap();
    let minted = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. }))
        .expect("the side effect is recorded");
    let failed = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. }))
        .expect("the child failed");
    assert!(minted < failed, "{history:#?}");
}

/// A run that passes its execution timeout closes like any terminal run. Its
/// detached children get their policy, as on Postgres.
#[tokio::test]
async fn a_timed_out_parent_applies_its_close_policy() {
    let mut rt = runtime();
    let clock = Arc::new(AtomicI64::new(1_767_225_600_000));
    let read = Arc::clone(&clock);
    rt.set_clock(move || {
        chrono::DateTime::from_timestamp_millis(read.load(Ordering::SeqCst)).unwrap()
    });
    let parent = rt
        .start_workflow("spawns_then_times_out", json!(0))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];

    clock.fetch_add(31_000, Ordering::SeqCst);
    let state = rt.run_until_blocked(parent).await.unwrap();

    assert!(matches!(state, RunState::TimedOut(_)), "{state:?}");
    let child_state = rt.run_until_blocked(child).await.unwrap();
    assert!(
        matches!(child_state, RunState::Cancelled(_)),
        "{child_state:?}"
    );
    let history = rt.load_history(parent).unwrap();
    let timed_out = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::WorkflowExecutionTimedOut { .. }))
        .expect("the run timed out");
    let cascade = history
        .iter()
        .position(|e| matches!(e, WorkflowEvent::ChildWorkflowCascadeApplied { .. }))
        .expect("the cascade is recorded");
    assert!(timed_out < cascade, "{history:#?}");
}

/// A child keeps the execution timeout of its type, as on Postgres. An
/// awaiting parent records the timeout.
#[tokio::test]
async fn an_awaited_child_times_out_by_its_declared_timeout() {
    let mut rt = runtime();
    let clock = Arc::new(AtomicI64::new(1_767_225_600_000));
    let read = Arc::clone(&clock);
    rt.set_clock(move || {
        chrono::DateTime::from_timestamp_millis(read.load(Ordering::SeqCst)).unwrap()
    });
    let parent = rt
        .start_workflow("parent_reports_child_failure", json!("timed_child"))
        .unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];

    clock.fetch_add(31_000, Ordering::SeqCst);
    let state = rt.run_until_blocked(parent).await.unwrap();

    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    assert!(matches!(
        rt.run_until_blocked(child).await.unwrap(),
        RunState::TimedOut(_)
    ));
    let out = completed(&rt, parent);
    assert_eq!(
        out["reason"],
        json!("timeout: WorkflowExecution for timed_child"),
        "{out}"
    );
    assert_replays(&rt, parent, "parent_reports_child_failure").await;
}

/// A detached child keeps the retry policy of its type, as on Postgres.
#[tokio::test]
async fn a_detached_child_retries_by_its_declared_policy() {
    let mut rt = runtime();
    let parent = rt.start_workflow("spawns_retried_child", json!(0)).unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    let child = children_of(&rt, parent)[0];

    rt.run_until_idle().await.ok();

    assert_eq!(RETRIED_CHILD_RUNS.load(Ordering::SeqCst), 2);
    assert!(
        rt.load_history(child)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowRetryScheduled { .. })),
        "the first attempt schedules a retry"
    );
}

/// An awaited child never retries, as on Postgres. Its parent sees the
/// first failure.
#[tokio::test]
async fn an_awaited_child_does_not_retry() {
    let mut rt = runtime();
    let parent = rt
        .start_workflow(
            "parent_reports_child_failure",
            json!("retry_declared_child"),
        )
        .unwrap();

    let state = rt.run_until_blocked(parent).await.unwrap();

    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    assert_eq!(RETRY_DECLARED_RUNS.load(Ordering::SeqCst), 1);
    assert_eq!(completed(&rt, parent)["reason"], json!("always"));
}

/// A run that another run cancels gives up its mutex and records its close
/// time, as every terminal path does.
#[tokio::test]
async fn a_hard_cancelled_holder_releases_its_mutex() {
    let mut rt = runtime();
    let holder = rt.start_workflow("holds_the_lock", json!(0)).unwrap();
    rt.run_until_blocked(holder).await.unwrap();
    let waiter = rt.start_workflow("takes_the_lock", json!(0)).unwrap();
    assert!(matches!(
        rt.run_until_blocked(waiter).await.unwrap(),
        RunState::WaitingMutex(_)
    ));

    let canceller = rt.start_workflow("cancel_reports", json!(holder)).unwrap();
    rt.run_until_blocked(canceller).await.unwrap();
    let state = rt.run_until_blocked(waiter).await.unwrap();

    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    let row = rt.describe(holder).unwrap();
    assert!(row.summary.closed_at.value().is_some(), "{row:?}");
}

/// The final panic strike counts the cascade events of its close, as the
/// failure arm does. A parent that would pass the cap fails at the cap.
#[tokio::test]
async fn the_final_panic_strike_counts_cascade_events() {
    let mut rt = runtime();
    // `WorkflowStarted`, `ChildWorkflowSpawnedDetached` and `SignalReceived`
    // come first. The cascade event then reaches the cap.
    rt.set_history_event_hard_cap(Some(4));
    let parent = rt.start_workflow("spawns_then_panics", json!(0)).unwrap();
    rt.run_until_blocked(parent).await.unwrap();
    rt.send_signal(parent, "go", json!(1)).unwrap();

    let state = loop {
        match rt.run_until_blocked(parent).await {
            Err(SqliteError::WorkflowPanicked { .. }) => {}
            other => break other.unwrap(),
        }
    };

    assert!(
        matches!(state, RunState::Failed(ref m) if m.contains("HistoryCapExceeded")),
        "{state:?}"
    );
}
