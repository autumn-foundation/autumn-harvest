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
use autumn_harvest::types::{ExecutionId, TimerId};
use autumn_harvest::worker::HandlerRegistry;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
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

// ── #779 Codex P1: over-deadline out-of-band child terminal ordering ─────────

const RACE_TIMER_ID: &str = "__child_timeout:1:timeout_child";

/// Build a parent parked on a child-timeout race whose deadline is ALREADY in
/// the past, WITHOUT the deadline having been ingested yet (the parent was not
/// claimed at its deadline). Returns `(parent_exec_id, child_exec_id)`.
///
/// History prefix: `WorkflowStarted, ChildWorkflowStarted, TimerStarted(deadline)`
/// matching a first `spawn_child_workflow_timeout("timeout_child", .., 2s)` call
/// (seq 1 → `__child_timeout:1:timeout_child`). A matching `harvest_timers` row
/// is inserted OVERDUE (`fires_at = NOW() - 2s`, `fired = false`) — the exact
/// state a child completing/failing after its deadline races against.
/// Seed the parent's parked-on-overdue-child-timeout state: append the
/// `WorkflowStarted, ChildWorkflowStarted(child), TimerStarted(deadline)` prefix
/// and insert a matching OVERDUE `harvest_timers` row (`fires_at = NOW() - 2s`,
/// `fired = false`) for `RACE_TIMER_ID`. Shared by the fake-child and real-child
/// setups so both drive the exact recorded state a late-terminating child races.
async fn seed_overdue_child_race(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    child_exec_id: ExecutionId,
) {
    let started_prefix = vec![
        WorkflowEvent::workflow_started(serde_json::json!({"id": 1}), chrono::Utc::now()),
        WorkflowEvent::ChildWorkflowStarted {
            child_id: child_exec_id,
            workflow_name: "timeout_child".into(),
            input: serde_json::json!({"id": 1}),
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new(RACE_TIMER_ID),
            duration_secs: 2,
        },
    ];
    autumn_harvest::store::append_events(conn, parent_exec_id, &started_prefix, 0)
        .await
        .expect("append parent started prefix");

    let overdue = autumn_harvest::models::NewHarvestTimer {
        workflow_exec_id: parent_exec_id.as_uuid(),
        timer_id: RACE_TIMER_ID,
        fires_at: chrono::Utc::now() - chrono::Duration::seconds(2),
    };
    diesel::insert_into(autumn_harvest::schema::harvest_timers::table)
        .values(&overdue)
        .execute(conn)
        .await
        .expect("insert overdue deadline timer");
}

async fn setup_parent_racing_overdue_child(
    conn: &mut AsyncPgConnection,
) -> (ExecutionId, ExecutionId) {
    let parent_exec_id = insert_workflow_execution(conn).await;
    let child_exec_id = ExecutionId::new();
    seed_overdue_child_race(conn, parent_exec_id, child_exec_id).await;
    (parent_exec_id, child_exec_id)
}

/// Like [`setup_parent_racing_overdue_child`], but the child is a REAL RUNNING
/// `harvest_workflow_executions` row LINKED to the parent as an awaited child
/// (`parent_id` set, `parent_close_policy` NULL) so the out-of-band operator
/// paths (`cancel_workflow_execution` / `terminate_workflow_execution`) and the
/// child's own execution-timeout scanner (`enforce_workflow_execution_timeouts`)
/// can act on it. When `child_deadline_at` is `Some`, the child row's execution
/// deadline is set (in the past) so the timeout scanner picks it up.
async fn setup_parent_racing_overdue_real_child(
    conn: &mut AsyncPgConnection,
    child_deadline_at: Option<chrono::DateTime<chrono::Utc>>,
) -> (ExecutionId, ExecutionId) {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    use diesel::{ExpressionMethods, QueryDsl};

    let parent_exec_id = insert_workflow_execution(conn).await;
    let child_exec_id = insert_workflow_execution(conn).await;

    // Link child → parent as an AWAITED child (parent_close_policy stays NULL,
    // the invariant notify_awaited_parent_of_child_terminal / the execution
    // timeout scanner both gate on) and, for the execution-timeout scenario, set
    // its deadline in the past so enforce_workflow_execution_timeouts selects it.
    diesel::update(dsl::harvest_workflow_executions.find(child_exec_id.as_uuid()))
        .set((
            dsl::parent_id.eq(Some(parent_exec_id.as_uuid())),
            dsl::deadline_at.eq(child_deadline_at),
        ))
        .execute(conn)
        .await
        .expect("link child to parent");

    seed_overdue_child_race(conn, parent_exec_id, child_exec_id).await;
    (parent_exec_id, child_exec_id)
}

/// Replay the child-timeout primitive over a loaded parent history and return
/// its outcome. Uses the exact call shape the parent workflow made: same
/// workflow name, input, and (implied) 2s deadline → `__child_timeout:1:...`.
async fn replay_child_timeout(
    database_url: &str,
    parent_exec_id: ExecutionId,
) -> Result<Option<Value>, String> {
    let history = load_history_from_url(database_url, parent_exec_id).await;
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), history.events);
    ctx.spawn_child_workflow_timeout(
        "timeout_child",
        serde_json::json!({"id": 1}),
        std::time::Duration::from_secs(2),
    )
    .await
    .map_err(|e| e.to_string())
}

/// The money test: a child that COMPLETES after its deadline appends its
/// terminal out-of-band via the REAL `wake_parent_for_child_completion`. The
/// pre-fix bug recorded `[.., ChildWorkflowCompleted]` with the overdue
/// deadline never materialized, so the pure recorded-order matcher returned
/// `Some`. Post-fix, the wake path materializes the due deadline FIRST, so the
/// recorded order is `[.., TimerFired, ChildWorkflowCompleted]` and the
/// primitive resolves to `None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_deadline_child_completion_orders_deadline_first_and_resolves_none() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let (parent_exec_id, child_exec_id) = setup_parent_racing_overdue_child(&mut conn).await;

    // Drive the REAL out-of-band child-completion wake path (byte-for-byte what
    // the worker runs when a child completes and notifies its parent).
    autumn_harvest::worker::wake_parent_for_child_completion(
        &mut conn,
        parent_exec_id,
        child_exec_id,
        serde_json::json!({"processed": true}),
    )
    .await
    .expect("wake parent for child completion");

    // Post-fix recorded order: the overdue deadline TimerFired precedes the
    // out-of-band child terminal.
    let history = load_history_from_url(&database_url, parent_exec_id).await;
    let tail: Vec<&WorkflowEvent> = history.events.iter().rev().take(2).collect();
    assert!(
        matches!(tail.as_slice(), [WorkflowEvent::ChildWorkflowCompleted { .. }, WorkflowEvent::TimerFired { timer_id }] if timer_id.as_str() == RACE_TIMER_ID),
        "history must end with [TimerFired(deadline), ChildWorkflowCompleted]; got: {:?}",
        history
            .events
            .iter()
            .map(WorkflowEvent::type_name)
            .collect::<Vec<_>>()
    );

    // The overdue deadline row must be marked fired (materialized, not re-fired
    // later by the parent-claim ingest).
    let timers = load_timers_for_execution_from_url(&database_url, parent_exec_id).await;
    assert!(
        timers
            .iter()
            .find(|t| t.timer_id == RACE_TIMER_ID)
            .is_some_and(|t| t.fired),
        "the materialized deadline timer must be marked fired: {:?}",
        timers
            .iter()
            .map(|t| (&t.timer_id, t.fired))
            .collect::<Vec<_>>()
    );

    // The primitive replays to None: the deadline (recorded first) won.
    let outcome = replay_child_timeout(&database_url, parent_exec_id).await;
    assert_eq!(
        outcome,
        Ok(None),
        "an over-deadline child COMPLETION must resolve to None (deadline won), not Some"
    );
}

/// Failure twin: a child that FAILS after its deadline appends its terminal via
/// the REAL `wake_parent_for_child_failure`, which received the same
/// deadline-ordering fix. Post-fix the recorded order is
/// `[.., TimerFired, ChildWorkflowFailed]` → the primitive resolves to `None`,
/// NOT the child's `Err`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_deadline_child_failure_orders_deadline_first_and_resolves_none() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let (parent_exec_id, child_exec_id) = setup_parent_racing_overdue_child(&mut conn).await;

    autumn_harvest::worker::wake_parent_for_child_failure(
        &mut conn,
        parent_exec_id,
        child_exec_id,
        "downstream 503",
    )
    .await
    .expect("wake parent for child failure");

    let history = load_history_from_url(&database_url, parent_exec_id).await;
    let tail: Vec<&WorkflowEvent> = history.events.iter().rev().take(2).collect();
    assert!(
        matches!(tail.as_slice(), [WorkflowEvent::ChildWorkflowFailed { .. }, WorkflowEvent::TimerFired { timer_id }] if timer_id.as_str() == RACE_TIMER_ID),
        "history must end with [TimerFired(deadline), ChildWorkflowFailed]; got: {:?}",
        history
            .events
            .iter()
            .map(WorkflowEvent::type_name)
            .collect::<Vec<_>>()
    );

    let outcome = replay_child_timeout(&database_url, parent_exec_id).await;
    assert_eq!(
        outcome,
        Ok(None),
        "an over-deadline child FAILURE must resolve to None (deadline won), not surface Err"
    );
}

// ── #779 Codex P2-D: over-deadline OPERATOR/TIMEOUT out-of-band terminals ─────
//
// Beyond the worker's completion/failure wake paths (fixed in P1), three OTHER
// out-of-band paths append a child terminal to a parent WITHOUT going through
// wake_parent_for_child_completion/_failure: an operator CANCEL, an operator
// TERMINATE (both via execution::notify_awaited_parent_of_child_terminal), and
// the child's OWN execution timeout (via timeout::wake_parent_for_child_timeout).
// Each must also materialize the parent's DUE __child_timeout deadline FIRST so
// an over-deadline child resolves the parent's spawn_child_workflow_timeout to
// None (not Err). These drive the REAL public entry points, mirroring the P1
// tests that drive the real wake functions.

/// Assert the parent's history ends with `[TimerFired(deadline),
/// ChildWorkflowFailed]` — the overdue deadline ordered ahead of the out-of-band
/// child terminal — and that the primitive replays to `None`.
async fn assert_parent_resolves_none_after_overdue_terminal(
    database_url: &str,
    parent_exec_id: ExecutionId,
    context: &str,
) {
    let history = load_history_from_url(database_url, parent_exec_id).await;
    let tail: Vec<&WorkflowEvent> = history.events.iter().rev().take(2).collect();
    assert!(
        matches!(tail.as_slice(), [WorkflowEvent::ChildWorkflowFailed { .. }, WorkflowEvent::TimerFired { timer_id }] if timer_id.as_str() == RACE_TIMER_ID),
        "[{context}] parent history must end with [TimerFired(deadline), ChildWorkflowFailed]; got: {:?}",
        history
            .events
            .iter()
            .map(WorkflowEvent::type_name)
            .collect::<Vec<_>>()
    );

    let outcome = replay_child_timeout(database_url, parent_exec_id).await;
    assert_eq!(
        outcome,
        Ok(None),
        "[{context}] an over-deadline child must resolve to None (deadline won), not Err"
    );
}

/// Operator CANCEL of an over-deadline awaited child appends its terminal via
/// `notify_awaited_parent_of_child_terminal`, which (post-fix) materializes the
/// overdue `__child_timeout` deadline first → the parent resolves `None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_deadline_child_operator_cancel_orders_deadline_first_and_resolves_none() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let (parent_exec_id, child_exec_id) =
        setup_parent_racing_overdue_real_child(&mut conn, None).await;

    // REAL out-of-band operator-cancel path on the awaited child.
    autumn_harvest::execution::cancel_workflow_execution(
        &mut conn,
        child_exec_id,
        "operator abort",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("cancel child");

    assert_parent_resolves_none_after_overdue_terminal(
        &database_url,
        parent_exec_id,
        "operator-cancel",
    )
    .await;
}

/// Operator TERMINATE of an over-deadline awaited child takes the same
/// `notify_awaited_parent_of_child_terminal` path → the parent resolves `None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_deadline_child_operator_terminate_orders_deadline_first_and_resolves_none() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let (parent_exec_id, child_exec_id) =
        setup_parent_racing_overdue_real_child(&mut conn, None).await;

    // REAL out-of-band operator-terminate path on the awaited child.
    autumn_harvest::execution::terminate_workflow_execution(
        &mut conn,
        child_exec_id,
        "operator terminate",
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("terminate child");

    assert_parent_resolves_none_after_overdue_terminal(
        &database_url,
        parent_exec_id,
        "operator-terminate",
    )
    .await;
}

/// The child hitting its OWN execution timeout (issue #243) appends its terminal
/// to the parent via `timeout::wake_parent_for_child_timeout` (post-fix it
/// materializes the overdue `__child_timeout` deadline first) → the parent
/// resolves `None`, not the child-timeout `Err`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_deadline_child_execution_timeout_orders_deadline_first_and_resolves_none() {
    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    // Child's own execution deadline is 5s in the past so the scanner selects it.
    let child_deadline = chrono::Utc::now() - chrono::Duration::seconds(5);
    let (parent_exec_id, _child_exec_id) =
        setup_parent_racing_overdue_real_child(&mut conn, Some(child_deadline)).await;

    // REAL execution-timeout scanner: times out the overdue child and notifies
    // the parent via wake_parent_for_child_timeout.
    let timed_out = autumn_harvest::timeout::enforce_workflow_execution_timeouts(
        &mut conn,
        &autumn_harvest::telemetry::NoOpMetrics,
    )
    .await
    .expect("enforce execution timeouts");
    assert!(
        timed_out >= 1,
        "the overdue child must be timed out (got {timed_out})"
    );

    assert_parent_resolves_none_after_overdue_terminal(
        &database_url,
        parent_exec_id,
        "child-execution-timeout",
    )
    .await;
}

/// Focused helper coverage: `materialize_due_child_timeout_deadlines`
/// (a) fires a DUE `__child_timeout` timer and appends `TimerFired`,
/// (b) leaves a NOT-yet-due `__child_timeout` timer alone, and
/// (c) ignores a DUE NON-`__child_timeout` timer (e.g. a plain `ctx.timer`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialize_due_child_timeout_deadlines_fires_only_due_child_timeout_timers() {
    use autumn_harvest::schema::harvest_timers::dsl;

    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    autumn_harvest::store::append_events(
        &mut conn,
        parent_exec_id,
        &[WorkflowEvent::workflow_started(
            serde_json::json!({}),
            chrono::Utc::now(),
        )],
        0,
    )
    .await
    .expect("seed WorkflowStarted");

    let due_child = "__child_timeout:1:timeout_child"; // (a) due child-timeout
    let future_child = "__child_timeout:2:timeout_child"; // (b) not-yet-due child-timeout
    let due_plain = "user_timer:reminder"; // (c) due, but not a child-timeout
    let rows = vec![
        autumn_harvest::models::NewHarvestTimer {
            workflow_exec_id: parent_exec_id.as_uuid(),
            timer_id: due_child,
            fires_at: chrono::Utc::now() - chrono::Duration::seconds(2),
        },
        autumn_harvest::models::NewHarvestTimer {
            workflow_exec_id: parent_exec_id.as_uuid(),
            timer_id: future_child,
            fires_at: chrono::Utc::now() + chrono::Duration::seconds(300),
        },
        autumn_harvest::models::NewHarvestTimer {
            workflow_exec_id: parent_exec_id.as_uuid(),
            timer_id: due_plain,
            fires_at: chrono::Utc::now() - chrono::Duration::seconds(2),
        },
    ];
    diesel::insert_into(dsl::harvest_timers)
        .values(&rows)
        .execute(&mut conn)
        .await
        .expect("insert timer rows");

    let fired =
        autumn_harvest::worker::materialize_due_child_timeout_deadlines(&mut conn, parent_exec_id)
            .await
            .expect("materialize");
    assert_eq!(
        fired, 1,
        "exactly the one DUE child-timeout deadline is fired"
    );

    // (a) the due child-timeout timer is marked fired AND a matching TimerFired
    //     event was appended.
    let timers = load_timers_for_execution_from_url(&database_url, parent_exec_id).await;
    let fired_map: std::collections::HashMap<&str, bool> = timers
        .iter()
        .map(|t| (t.timer_id.as_str(), t.fired))
        .collect();
    assert_eq!(
        fired_map.get(due_child),
        Some(&true),
        "(a) due child-timeout fired"
    );
    assert_eq!(
        fired_map.get(future_child),
        Some(&false),
        "(b) not-yet-due child-timeout must be left alone"
    );
    assert_eq!(
        fired_map.get(due_plain),
        Some(&false),
        "(c) a due non-child-timeout timer must be ignored"
    );

    let history = load_history_from_url(&database_url, parent_exec_id).await;
    let fired_events: Vec<&TimerId> = history
        .events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::TimerFired { timer_id } => Some(timer_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        fired_events.len(),
        1,
        "exactly one TimerFired appended (the due child-timeout): {fired_events:?}"
    );
    assert_eq!(fired_events[0].as_str(), due_child);
}

/// Exactly-once coordination between the two `__child_timeout` deadline-fire
/// paths (issue #779, Codex P2-A). Since the P1 fix there are two writers that
/// can fire a due deadline: the parent-claim ingest
/// (`ingest_due_timers_and_signals`) and the out-of-band materializer
/// (`materialize_due_child_timeout_deadlines`). Both append `TimerFired` **and**
/// set `fired = true` atomically in one transaction, so a second attempt for the
/// same timer is excluded by the `fired = false` predicate — it never appends a
/// duplicate `TimerFired`. This asserts the exclusion half of that guarantee:
/// re-invoking the materializer over an already-fired timer returns `0` and
/// appends nothing.
///
/// The complementary collision half — when a concurrent writer's `TimerFired`
/// has not yet committed, so the second writer's stale-`start_id` append lands
/// on the same `event_id` — is enforced by the `UNIQUE(workflow_exec_id,
/// event_id)` constraint (`harvest_events`), which aborts the racing append. A
/// durable duplicate `TimerFired` is therefore impossible without any
/// compare-and-set on the shared ingest path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialize_due_child_timeout_deadlines_is_idempotent_no_duplicate_timerfired() {
    use autumn_harvest::schema::harvest_timers::dsl;

    let (database_url, _container) = setup_test_database_url().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let parent_exec_id = insert_workflow_execution(&mut conn).await;
    autumn_harvest::store::append_events(
        &mut conn,
        parent_exec_id,
        &[WorkflowEvent::workflow_started(
            serde_json::json!({}),
            chrono::Utc::now(),
        )],
        0,
    )
    .await
    .expect("seed WorkflowStarted");

    let due_child = "__child_timeout:1:timeout_child";
    diesel::insert_into(dsl::harvest_timers)
        .values(&autumn_harvest::models::NewHarvestTimer {
            workflow_exec_id: parent_exec_id.as_uuid(),
            timer_id: due_child,
            fires_at: chrono::Utc::now() - chrono::Duration::seconds(2),
        })
        .execute(&mut conn)
        .await
        .expect("insert due child-timeout timer");

    // First fire: the due deadline is materialized once.
    let first =
        autumn_harvest::worker::materialize_due_child_timeout_deadlines(&mut conn, parent_exec_id)
            .await
            .expect("first materialize");
    assert_eq!(first, 1, "the due child-timeout deadline fires once");

    // Second fire (models a subsequent ingest or a re-run of the materializer
    // over the same, already-fired timer): the `fired = false` predicate now
    // excludes it, so nothing is fired and nothing is appended.
    let second =
        autumn_harvest::worker::materialize_due_child_timeout_deadlines(&mut conn, parent_exec_id)
            .await
            .expect("second materialize");
    assert_eq!(
        second, 0,
        "an already-fired deadline is excluded — no second fire"
    );

    // Durable state: exactly ONE TimerFired for this timer, timer row fired.
    let history = load_history_from_url(&database_url, parent_exec_id).await;
    let fired_events: Vec<&TimerId> = history
        .events
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::TimerFired { timer_id } => Some(timer_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        fired_events.len(),
        1,
        "exactly one durable TimerFired — no duplicate: {fired_events:?}"
    );
    assert_eq!(fired_events[0].as_str(), due_child);

    let timers = load_timers_for_execution_from_url(&database_url, parent_exec_id).await;
    assert!(
        timers.iter().any(|t| t.timer_id == due_child && t.fired),
        "the child-timeout timer row is marked fired"
    );
}
