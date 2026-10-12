//! Issue #2026: child workflows, and signal, cancel and await between runs.
//!
//! Each test drives a real composition on the `SQLite` backend. Each test
//! then replays every history it wrote on the core `WorkflowReplayer`.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]
// Some fixtures return at once and hold no `.await`.
#![allow(clippy::unused_async)]

use autumn_harvest::ParentClosePolicy;
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

/// Completes, but its last cycle upserts a search attribute. This backend
/// still rejects that, so the child is sealed with a typed failure.
#[workflow]
async fn child_unsupported(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.upsert_search_attrs([("stage".to_string(), Some(json!("done")))])
        .map_err(|e| e.to_string())?;
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
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

#[workflow]
async fn waits_on_timer(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.timer("nap", 3600).await.map_err(|e| e.to_string())
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
    ]
}

fn handler_of(name: &str) -> WorkflowHandlerFn {
    fixtures()
        .into_iter()
        .chain(std::iter::once(registered_late_info()))
        .find(|info| info.name == name)
        .map_or_else(|| panic!("no fixture named {name}"), |info| info.handler)
}

/// Replay the stored history of `exec` on the core replayer.
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
    // Read the history now. The future then holds no runtime borrow.
    let history = rt.load_history(exec).unwrap();
    async move { replay_history(history, exec, workflow).await }
}

async fn replay_history(history: Vec<WorkflowEvent>, exec: ExecutionId, workflow: &str) {
    let replayer = WorkflowReplayer::new().register_fn(workflow, handler_of(workflow));
    let report = match history.split_last() {
        Some((WorkflowEvent::WorkflowCancelled { .. }, written)) => {
            let snapshot = serde_json::from_value(json!({
                "workflow_name": workflow,
                "execution_id": exec,
                "events": written,
            }))
            .unwrap();
            replayer.replay_canary_snapshot(snapshot).await
        }
        _ => replayer.replay_from_events(history.clone()).await,
    };
    let ok = match &report.status {
        ReplayStatus::ReplaySucceeded => true,
        // A failed run replays to the same failure.
        ReplayStatus::WorkflowFailed { .. } => history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        ReplayStatus::NonDeterminismDetected { .. } => false,
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
    assert_eq!(out["details"]["feature"], json!("UpsertSearchAttributes"));
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
