#![cfg(feature = "db")]
//! End-to-end integration tests for `ctx.execute_child_workflow_timeout` /
//! `ctx.spawn_child_workflow_timeout` — deadline-bounded child-workflow awaits
//! (issue #779).
//!
//! Drives a real worker loop against a throwaway Postgres container (reusing
//! the harness helpers from `integration_e2e`). RED PHASE: the two
//! `WorkflowContext` methods do not exist yet, so this file fails to compile —
//! the accepted red state (per the #543/#593 precedent). It is Docker-run in
//! CI once green.
//!
//! Scenarios:
//! - child completes before the deadline → parent gets `Ok(Some(output))`.
//! - child hangs past the deadline → parent gets `Ok(None)` AND the still-running
//!   child row is sealed `CANCELLED` (R1/R6/R8).
//! - child fails before the deadline → parent gets a typed `Err` (R13).

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::HandlerRegistry;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use serde_json::Value;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_child_executions_from_url, load_history_from_url,
    load_timers_for_execution_from_url, setup_test_database_url, spawn_test_worker,
    wait_for_execution_state,
};

type WfFuture<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;
type WfHandler = for<'a> fn(&'a WorkflowContext, Value) -> WfFuture<'a>;

fn wf_info(name: &'static str, handler: WfHandler) -> WorkflowInfo {
    WorkflowInfo {
        mcp: false,
        name,
        module: "child_timeout_tests",
        handler,
        execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
    }
}

fn registry(parent: WorkflowInfo, child: WorkflowInfo) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(vec![parent, child], vec![]))
}

// ── Workflow fns ───────────────────────────────────────────────────────────

/// Parent awaits a child with a 300s deadline; reports Some/None distinctly.
fn parent_await_child(ctx: &WorkflowContext, input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let outcome: Option<Value> = ctx
            .spawn_child_workflow_timeout(
                "timeout_child",
                input,
                std::time::Duration::from_secs(300),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(outcome.map_or_else(
            || serde_json::json!({"outcome": "timed_out"}),
            |o| serde_json::json!({"outcome": "child_done", "child": o}),
        ))
    })
}

/// Parent awaits a hanging child with a SHORT (2s) deadline so the durable
/// timer fires in bounded wall-clock time and the child is cancelled.
fn parent_short_deadline(ctx: &WorkflowContext, input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let outcome: Option<Value> = ctx
            .spawn_child_workflow_timeout("hanging_child", input, std::time::Duration::from_secs(2))
            .await
            .map_err(|e| e.to_string())?;
        Ok(outcome.map_or_else(
            || serde_json::json!({"outcome": "timed_out"}),
            |o| serde_json::json!({"outcome": "child_done", "child": o}),
        ))
    })
}

fn fast_child(_ctx: &WorkflowContext, input: Value) -> WfFuture<'_> {
    Box::pin(async move { Ok(serde_json::json!({"echo": input})) })
}

/// A child that hangs forever waiting for a signal that never arrives.
fn hanging_child(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let _ = ctx
            .wait_for_signal("never")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"unreachable": true}))
    })
}

fn failing_child(_ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move { Err("downstream 503".to_string()) })
}

/// A child that blocks until a `"go"` signal is delivered, then completes. Used
/// by the self-wake test to keep the child RUNNING under external control while
/// the parent is forced through a re-park cycle.
fn signal_gated_child(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let _ = ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"released": true}))
    })
}

/// Parent runs TWO child-timeouts SEQUENTIALLY in one workflow body: the first
/// child completes (→ `Some`), the second hangs past a SHORT deadline (→ `None`).
fn parent_two_sequential(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let first: Option<Value> = ctx
            .spawn_child_workflow_timeout(
                "seq_fast_child",
                serde_json::json!({"n": 1}),
                std::time::Duration::from_secs(300),
            )
            .await
            .map_err(|e| e.to_string())?;
        let second: Option<Value> = ctx
            .spawn_child_workflow_timeout(
                "seq_hang_child",
                serde_json::json!({"n": 2}),
                std::time::Duration::from_secs(2),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "first": first, "second": second }))
    })
}

/// Poll the parent's children until exactly `expected` exist and the child at
/// `idx` reaches `state`, or panic after ~10s. Returns the children snapshot.
async fn wait_for_child_state(
    database_url: &str,
    parent_exec_id: ExecutionId,
    expected: usize,
    idx: usize,
    state: &str,
) -> Vec<autumn_harvest::models::WorkflowExecution> {
    for _ in 0..100 {
        let children = load_child_executions_from_url(database_url, parent_exec_id).await;
        if children.len() == expected && children.get(idx).is_some_and(|c| c.state == state) {
            return children;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("child[{idx}] did not reach state {state} (expected {expected} children)");
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_completes_before_deadline_parent_gets_some() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({"id": 1})).await;

    let reg = registry(
        wf_info("e2e_test_workflow", parent_await_child),
        wf_info("timeout_child", fast_child),
    );
    let worker = build_runtime_worker("worker-779-child-win", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent must have output");
    assert_eq!(
        output.get("outcome").cloned(),
        Some(serde_json::json!("child_done")),
        "child completed before the deadline → parent must get Some"
    );
    // Assert the actual winning child PAYLOAD flowed through, not just the
    // branch tag: `fast_child` echoes its input `{"id": 1}`.
    assert_eq!(
        output.get("child").cloned(),
        Some(serde_json::json!({"echo": {"id": 1}})),
        "the child's actual output payload must surface to the parent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_hangs_past_deadline_parent_gets_none_and_child_cancelled() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({"id": 2})).await;

    let reg = registry(
        wf_info("e2e_test_workflow", parent_short_deadline),
        wf_info("hanging_child", hanging_child),
    );
    let worker = build_runtime_worker("worker-779-timer-win", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    // The deadline fired first → parent takes the timeout branch.
    assert_eq!(
        parent.output.and_then(|o| o.get("outcome").cloned()),
        Some(serde_json::json!("timed_out")),
        "deadline fired first → parent must get None"
    );

    // The parent's own history must carry the deadline TimerFired.
    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    assert!(
        parent_history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "parent history must record the deadline TimerFired"
    );

    // The still-running loser child must be sealed CANCELLED (no leak).
    let children = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(children.len(), 1, "exactly one child execution");
    assert_eq!(
        children[0].state, "CANCELLED",
        "the losing child must be durably cancelled when the deadline wins"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_fails_before_deadline_parent_gets_typed_err() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({"id": 3})).await;

    let reg = registry(
        wf_info("e2e_test_workflow", parent_await_child),
        wf_info("timeout_child", failing_child),
    );
    let worker = build_runtime_worker("worker-779-child-fail", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // A child failure before the deadline propagates as an Err through the
    // parent (which maps it to a String and returns via `?`), failing the run.
    let parent = wait_for_execution_state(&database_url, parent_exec_id, "FAILED").await;
    worker.shutdown();
    handle.await.expect("join");

    assert!(
        parent.error.unwrap_or_default().contains("downstream 503"),
        "the child's failure must surface to the parent"
    );
    // The deadline was never reached (child failed fast under a 300s timer), so
    // no TimerFired may appear in the parent history on the child-failure path.
    let parent_history = load_history_from_url(&database_url, parent_exec_id).await;
    assert!(
        !parent_history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "no deadline TimerFired must be recorded when the child fails before the deadline"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_sequential_child_timeouts_resolve_independently() {
    // AC: a real workflow runs child-timeouts SEQUENTIALLY (each in its own
    // suspension batch — the join! shape is worker-rejected). The seq counter
    // and distinct `__child_timeout:{seq}` ids must both work end-to-end: the
    // first child completes → Some, the second times out → None.
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({"id": 4})).await;

    let reg = Arc::new(HandlerRegistry::new(
        vec![
            wf_info("e2e_test_workflow", parent_two_sequential),
            wf_info("seq_fast_child", fast_child),
            wf_info("seq_hang_child", hanging_child),
        ],
        vec![],
    ));
    let worker = build_runtime_worker("worker-779-seq", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent must have output");
    // First child completed before its (long) deadline → Some(echoed input).
    assert_eq!(
        output.get("first").cloned(),
        Some(serde_json::json!({"echo": {"n": 1}})),
        "the first sequential child must resolve to Some(its output)"
    );
    // Second child hung past its short deadline → None (JSON null).
    assert_eq!(
        output.get("second").cloned(),
        Some(Value::Null),
        "the second sequential child must time out to None"
    );

    // Exactly two children: the fast one COMPLETED, the hung one CANCELLED.
    let children = load_child_executions_from_url(&database_url, parent_exec_id).await;
    assert_eq!(children.len(), 2, "exactly two child executions");
    let fast = children
        .iter()
        .find(|c| c.workflow_name == "seq_fast_child")
        .expect("fast child row");
    let hung = children
        .iter()
        .find(|c| c.workflow_name == "seq_hang_child")
        .expect("hung child row");
    assert_eq!(fast.state, "COMPLETED", "the fast child must complete");
    assert_eq!(
        hung.state, "CANCELLED",
        "the timed-out child must be sealed CANCELLED"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_completes_in_park_gap_self_wakes_parent() {
    // R3: exercises the RE-PARK path in `persist_child_timeout_race` and its
    // post-commit self-wake re-check. A parent parked on a child-timeout race
    // is woken by an UNRELATED wake while its child is still RUNNING — it
    // reloads history, matches `InProgress`, and RE-PARKS, running the self-wake
    // re-check (which correctly observes the child NOT-yet-terminal and does not
    // double-wake). The child is then released and the parent resolves to Some.
    //
    // HONEST COVERAGE NOTE: this reliably drives the re-park machinery + the
    // self-wake re-check code path, and proves the parent still reaches
    // COMPLETED with Some across a spurious re-park. It does NOT deterministically
    // force the branch where the re-check observes an ALREADY-terminal child (the
    // child-completes-strictly-inside-the-park-gap interleaving): the child-row
    // terminal state and the parent-history `ChildWorkflowCompleted` event are
    // committed in ONE transaction (`persist_child_workflow_completion`), so that
    // branch is only reachable via a genuine timing race against a stale history
    // snapshot, which the current harness cannot pin without mid-transaction
    // instrumentation. The re-park cycle here executes the exact same re-check.
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({"id": 5})).await;

    // 300s deadline so the timer NEVER fires — the only resolution is the child.
    let reg = registry(
        wf_info("e2e_test_workflow", parent_await_child),
        wf_info("timeout_child", signal_gated_child),
    );
    let worker = build_runtime_worker("worker-779-selfwake", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    // Wait until the child is spawned AND RUNNING (parked on its "go" signal) —
    // the parent is now parked on the child-timeout mixed batch.
    let children = wait_for_child_state(&database_url, parent_exec_id, 1, 0, "RUNNING").await;
    let child_exec_id = ExecutionId::from_uuid(children[0].id);

    // Force a genuine re-park: wake the parent by an UNRELATED wake while the
    // child is still RUNNING. The worker re-claims, reloads history, matches
    // InProgress, and re-parks — running the self-wake re-check with the child
    // not-yet-terminal. Repeat to make the re-park cycle unmistakable.
    for _ in 0..3 {
        autumn_harvest::queue::wake_workflow_task(&mut conn, parent_exec_id)
            .await
            .expect("wake parent");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }

    // The parent must NOT have terminated across the spurious re-parks (the
    // self-wake re-check is a no-op while the child runs, and the 300s timer
    // never fires) — proving the re-park cycles were benign.
    let mid_history = load_history_from_url(&database_url, parent_exec_id).await;
    assert!(
        !mid_history.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::WorkflowCompleted { .. }
                | WorkflowEvent::WorkflowFailed { .. }
                | WorkflowEvent::TimerFired { .. }
                | WorkflowEvent::ChildWorkflowCompleted { .. }
        )),
        "parent must not terminate or observe a child terminal across the spurious re-parks"
    );

    // Now release the child: it completes, wakes the parent, and the parent
    // resolves the race to Some.
    autumn_harvest::signal::send_signal(&mut conn, child_exec_id, "go", serde_json::json!({}))
        .await
        .expect("deliver go signal to child");
    autumn_harvest::queue::wake_workflow_task(&mut conn, child_exec_id)
        .await
        .expect("wake child");

    let parent = wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    assert_eq!(
        parent.output.and_then(|o| o.get("outcome").cloned()),
        Some(serde_json::json!("child_done")),
        "after a re-park, the child completion must still resolve the parent to Some"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_win_tears_down_deadline_timer_no_retention_pin() {
    // Codex P2 (#779): on a child-win the still-armed `__child_timeout:{seq}`
    // durable timer must be proactively deleted. An unfired `harvest_timers`
    // row (`fired = false`) is treated as an in-flight dependency by
    // `retention::has_inflight_dependencies`, so leaving it would pin the
    // terminal parent forever. Drive a child-win to COMPLETED and assert NO
    // `__child_timeout:` row survives for the parent execution.
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, parent_exec_id, serde_json::json!({"id": 6})).await;

    let reg = registry(
        wf_info("e2e_test_workflow", parent_await_child),
        wf_info("timeout_child", fast_child),
    );
    let worker = build_runtime_worker("worker-779-timer-cleanup", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, parent_exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    // Sanity: this really was the child-win branch.
    assert_eq!(
        parent.output.and_then(|o| o.get("outcome").cloned()),
        Some(serde_json::json!("child_done")),
        "child completed before the deadline → child-win branch"
    );

    // The deadline timer must have been durably deleted (not merely left
    // unfired): no `__child_timeout:` row may remain for the parent, and in
    // fact no unfired timer at all should linger to pin retention.
    let timers = load_timers_for_execution_from_url(&database_url, parent_exec_id).await;
    assert!(
        !timers
            .iter()
            .any(|t| t.timer_id.starts_with("__child_timeout:")),
        "child-win must delete the deadline timer row; found: {:?}",
        timers.iter().map(|t| &t.timer_id).collect::<Vec<_>>()
    );
    assert!(
        !timers.iter().any(|t| !t.fired),
        "no unfired timer may remain to pin the terminal parent via retention: {:?}",
        timers
            .iter()
            .map(|t| (&t.timer_id, t.fired))
            .collect::<Vec<_>>()
    );
}
