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
    insert_workflow_execution, insert_workflow_execution_with_id, load_child_executions_from_url,
    load_history_from_url, load_timers_for_execution_from_url, setup_test_database_url_or_env,
    spawn_test_worker, wait_for_execution_state,
};

type WfFuture<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;
type WfHandler = for<'a> fn(&'a WorkflowContext, Value) -> WfFuture<'a>;

fn wf_info(name: &'static str, handler: WfHandler) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "child_timeout_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
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
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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
    // parent. The parent maps it to a String and returns it via `?`, so the
    // run fails.
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
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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
    // post-commit self-wake re-check. An UNRELATED wake wakes a parent parked on
    // a child-timeout race while its child is still RUNNING. The parent reloads
    // history, matches `InProgress`, and RE-PARKS. That runs the self-wake
    // re-check, which sees the child not yet terminal and does not double-wake.
    // The child is then released and the parent resolves to Some.
    //
    // COVERAGE NOTE: this drives the re-park machinery and the self-wake
    // re-check path. It proves the parent still reaches COMPLETED with Some
    // across a spurious re-park. It does NOT force the branch where the
    // re-check sees an ALREADY-terminal child. That needs the child to complete
    // strictly inside the park gap. One transaction
    // (`persist_child_workflow_completion`) commits both the child-row terminal
    // state and the parent-history `ChildWorkflowCompleted` event. So only a
    // real timing race against a stale history snapshot reaches that branch.
    // The harness cannot pin it without mid-transaction instrumentation. The
    // re-park cycle here runs the exact same re-check.
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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

    // The parent must NOT terminate across the spurious re-parks. The self-wake
    // re-check is a no-op while the child runs, and the 300s timer never fires.
    // This proves the re-park cycles were benign.
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
    // Issue #779: on a child-win the still-armed `__child_timeout:{seq}`
    // durable timer must be proactively deleted. An unfired `harvest_timers`
    // row (`fired = false`) is treated as an in-flight dependency by
    // `retention::has_inflight_dependencies`, so leaving it would pin the
    // terminal parent forever. Drive a child-win to COMPLETED and assert NO
    // `__child_timeout:` row survives for the parent execution.
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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

    // The deadline timer must be durably deleted, not just left unfired. No
    // `__child_timeout:` row may remain for the parent. No unfired timer at all
    // may linger to pin retention.
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

// ── #779: over-deadline out-of-band child terminal ordering ────────────────

const RACE_TIMER_ID: &str = "__child_timeout:1:timeout_child";

/// Seeds a parent parked on a child-timeout race whose deadline is ALREADY in
/// the past.
///
/// The deadline is not ingested yet, because nothing claimed the parent at its
/// deadline.
///
/// History prefix: `WorkflowStarted, ChildWorkflowStarted, TimerStarted(deadline)`.
/// It matches a first `spawn_child_workflow_timeout("timeout_child", .., 2s)`
/// call (seq 1 → `__child_timeout:1:timeout_child`). A matching
/// `harvest_timers` row for `RACE_TIMER_ID` is inserted OVERDUE
/// (`fires_at = NOW() - 2s`, `fired = false`). A child that completes or fails
/// after its deadline races against this state. The fake-child and real-child
/// setups share it.
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

/// Builds a parent racing an overdue child deadline, with a fake child (no
/// execution row). Returns `(parent_exec_id, child_exec_id)`.
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
/// (`parent_id` set, `parent_close_policy` NULL). So the out-of-band operator
/// paths (`cancel_workflow_execution` / `terminate_workflow_execution`) can act
/// on it. So can the child's own scanner (`enforce_workflow_execution_timeouts`).
/// When `child_deadline_at` is `Some`, the child row gets that execution
/// deadline (in the past), so the timeout scanner picks it up.
async fn setup_parent_racing_overdue_real_child(
    conn: &mut AsyncPgConnection,
    child_deadline_at: Option<chrono::DateTime<chrono::Utc>>,
) -> (ExecutionId, ExecutionId) {
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    use diesel::{ExpressionMethods, QueryDsl};

    let parent_exec_id = insert_workflow_execution(conn).await;
    // A `child-*` workflow_id marks this row as the child, for readability.
    let child_exec_id =
        insert_workflow_execution_with_id(conn, &format!("child-{}", uuid::Uuid::new_v4())).await;

    // Link child → parent as an AWAITED child. parent_close_policy stays NULL:
    // notify_awaited_parent_of_child_terminal and the execution timeout scanner
    // both gate on that. For the execution-timeout scenario, also set the
    // deadline in the past, so enforce_workflow_execution_timeouts selects it.
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
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let (parent_exec_id, child_exec_id) = setup_parent_racing_overdue_child(&mut conn).await;

    autumn_harvest::worker::wake_parent_for_child_failure(
        &mut conn,
        parent_exec_id,
        child_exec_id,
        "downstream 503",
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
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

// ── #779: over-deadline OPERATOR/TIMEOUT out-of-band terminals ─────────────
//
// The section above covers the worker's completion/failure wake paths. Three
// OTHER out-of-band paths append a child terminal to a parent WITHOUT
// wake_parent_for_child_completion/_failure. Two are an operator CANCEL and an
// operator TERMINATE, both via execution::notify_awaited_parent_of_child_terminal.
// The third is the child's OWN execution timeout, via
// timeout::wake_parent_for_child_timeout. Each must also materialize the
// parent's DUE __child_timeout deadline FIRST. Then an over-deadline child
// resolves the parent's spawn_child_workflow_timeout to None (not Err). These
// tests drive the REAL public entry points, like the wake-path tests above.

/// Assert the parent's history ends with `[TimerFired(deadline),
/// ChildWorkflowFailed]`, with the overdue deadline ahead of the out-of-band
/// child terminal. Also assert that the primitive replays to `None`.
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
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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

/// A child that hits its OWN execution timeout (issue #243) appends its
/// terminal to the parent via `timeout::wake_parent_for_child_timeout`. After
/// the fix, that materializes the overdue `__child_timeout` deadline first. So
/// the parent resolves `None`, not the child-timeout `Err`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_deadline_child_execution_timeout_orders_deadline_first_and_resolves_none() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
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

/// Focused coverage of `materialize_due_child_timeout_deadlines`.
///
/// - (a) It fires a DUE `__child_timeout` timer and appends `TimerFired`.
/// - (b) It leaves a NOT-yet-due `__child_timeout` timer alone.
/// - (c) It ignores a DUE NON-`__child_timeout` timer (e.g. a plain `ctx.timer`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialize_due_child_timeout_deadlines_fires_only_due_child_timeout_timers() {
    use autumn_harvest::schema::harvest_timers::dsl;

    let (database_url, _guard) = setup_test_database_url_or_env().await;
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
/// paths (issue #779). Two writers can fire a due deadline: the parent-claim
/// ingest
/// (`ingest_due_timers_and_signals`) and the out-of-band materializer
/// (`materialize_due_child_timeout_deadlines`). Each appends `TimerFired`
/// **and** sets `fired = true` atomically in one transaction. So the
/// `fired = false` predicate excludes a second attempt for the same timer. It
/// never appends a duplicate `TimerFired`. This asserts the exclusion half of
/// that guarantee. A re-run of the materializer over a fired timer returns `0`
/// and appends nothing.
///
/// The collision half covers a concurrent writer whose `TimerFired` has not
/// committed yet. The second writer's stale-`start_id` append then lands on
/// the same `event_id`. The `UNIQUE(workflow_exec_id, event_id)` constraint
/// (`harvest_events`) aborts that racing append. So a durable duplicate
/// `TimerFired` is impossible without any compare-and-set on the ingest path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn materialize_due_child_timeout_deadlines_is_idempotent_no_duplicate_timerfired() {
    use autumn_harvest::schema::harvest_timers::dsl;

    let (database_url, _guard) = setup_test_database_url_or_env().await;
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

    // Second fire: models a later ingest, or a materializer re-run over the
    // same fired timer. The `fired = false` predicate now excludes it, so
    // nothing fires and nothing is appended.
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

/// #779: event-id accounting for a race-loser CHILD cancellation that ALSO
/// materializes a due `__child_timeout` deadline.
///
/// When `apply_race_loser_cancellations` cancels a losing child, the REAL
/// `notify_awaited_parent_of_child_terminal` appends onto the PARENT's OWN
/// history via self-computed `MAX(event_id)+1` ids. Since the over-deadline
/// OPERATOR/TIMEOUT fix of #779, that path materializes the deadline first. So
/// the append is `[TimerFired(overdue deadline), ChildWorkflowFailed]`, TWO
/// events, not one.
/// The pre-fix caller advanced its in-memory `next_event_id` cursor by a
/// hardcoded `+1`. That left it short by the materialized-deadline count (N).
/// The winner-persist path then appends the winner terminal
/// (`WorkflowCompleted`/`WorkflowFailed`). That append reused a consumed id
/// and hit `UNIQUE(workflow_exec_id, event_id)`. It rolled back and wrongly
/// failed the healthy race-winning parent.
///
/// Drives the REAL `apply_race_loser_cancellations` against a parent with an
/// overdue deadline and an awaited RUNNING loser child. Then performs the real
/// terminal append at the returned cursor. RED (revert the count re-read to a
/// hardcoded `+1`): the cursor is `4`, still AT the `ChildWorkflowFailed` id,
/// and the terminal append collides. GREEN (re-read the true next id): the
/// cursor is `5` and the append lands gap-free.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn race_loser_child_cancel_materializing_deadline_advances_event_id_past_all_appends() {
    use autumn_harvest::WorkflowCommand;

    let (database_url, _guard) = crate::integration_e2e::setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    // Parent (RUNNING) parked on a child-timeout race whose deadline is already
    // overdue (harvest_timers row `fired = false`), plus a REAL RUNNING awaited
    // loser child linked to it. History prefix seeds ids 0..2:
    //   [WorkflowStarted(0), ChildWorkflowStarted(1), TimerStarted(deadline)(2)]
    let (parent_exec_id, loser_child_exec_id) =
        setup_parent_racing_overdue_real_child(&mut conn, None).await;

    // The cursor the winner-persist path hands to apply_race_loser_cancellations:
    // the true next id after the prefix (== DB max + 1 == 3).
    let entry_next = load_history_from_url(&database_url, parent_exec_id)
        .await
        .next_event_id;
    assert_eq!(entry_next, 3, "prefix seeds event ids 0..2");

    // Resolve the race by cancelling the losing child. It is an awaited child of
    // the parent, so the REAL cancellation notifies the parent inline. That
    // materializes the overdue `__child_timeout` deadline (TimerFired) BEFORE
    // the ChildWorkflowFailed: TWO events onto the parent's own history.
    let commands = vec![WorkflowCommand::CancelRaceLosers {
        reason: autumn_harvest::context::LoserCancelReason::RaceLoser,
        activities: vec![],
        children: vec![loser_child_exec_id],
        timers: vec![],
    }];
    let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
    let mut next_event_id = entry_next;
    autumn_harvest::worker::apply_race_loser_cancellations(
        &mut conn,
        parent_exec_id,
        &commands,
        &mut next_event_id,
        &registry,
    )
    .await
    .expect("apply race loser cancellations");

    // The cursor must advance past BOTH appended events (materialized
    // TimerFired + ChildWorkflowFailed): entry(3) + 2 = 5. The pre-fix value
    // entry(3) + 1 = 4 still points AT the ChildWorkflowFailed id.
    assert_eq!(
        next_event_id, 5,
        "cursor must skip the materialized deadline AND the child terminal that \
         notify_awaited_parent_of_child_terminal appended (accounting fix)"
    );

    // The subsequent winner-terminal append the persist path performs at the
    // returned cursor. RED: at the stale id 4 this collides on
    // UNIQUE(workflow_exec_id, event_id) and this `.expect` fails. GREEN: id 5 is
    // free.
    autumn_harvest::store::append_events(
        &mut conn,
        parent_exec_id,
        &[WorkflowEvent::WorkflowCompleted {
            output: serde_json::json!({"winner": "sibling"}),
        }],
        next_event_id,
    )
    .await
    .expect(
        "winner-terminal append must NOT collide with the materialized deadline / \
         child terminal (event-id accounting fix, #779 Codex P2)",
    );

    // Final parent history: gap-free, distinct, correctly ordered.
    let history = load_history_from_url(&database_url, parent_exec_id).await;
    let names: Vec<&str> = history
        .events
        .iter()
        .map(WorkflowEvent::type_name)
        .collect();
    assert_eq!(
        names,
        vec![
            "WorkflowStarted",
            "ChildWorkflowStarted",
            "TimerStarted",
            "TimerFired",
            "ChildWorkflowFailed",
            "WorkflowCompleted",
        ],
        "parent history must be gap-free and correctly ordered: {names:?}"
    );
    assert_eq!(
        history.next_event_id, 6,
        "six sequential events (ids 0..5) with no gap"
    );
}

/// #779 (HEALTHY-RUN-KILLER): the child-timeout SUSPENSION persist path
/// ([`persist_child_timeout_race`]) must append its parent events with
/// `append_single_event` (parent-row `FOR UPDATE` + `MAX(event_id)` re-read
/// per insert). It matches the plain `persist_all_started_child_workflows`
/// path byte for byte. It must NOT use a batch `append_events` at the stale
/// pre-handler `next_event_id`.
///
/// A resolved `ctx.race()` over children may push a `CancelRaceLosers` for a
/// still-RUNNING loser child and, in the SAME cycle, `spawn_child_workflow_timeout`
/// suspends. `extract_child_timeout_race` deliberately tolerates that
/// `CancelRaceLosers`, so the batch is routed to `persist_child_timeout_race`.
/// While that suspension transaction runs, the loser child can complete on its
/// own worker. It then appends `ChildWorkflowCompleted` onto the PARENT via
/// `wake_parent_for_child_completion`'s own `append_single_event` (parent-row
/// lock + MAX re-read). The two appends target the same `event_id`.
///
/// RED (pre-fix): the suspension path used
/// `store::append_events(parent, &events, stale_next_event_id)` with NO
/// parent-row lock. The concurrent committer had already taken that
/// `event_id`, so the batch insert hit `UNIQUE(workflow_exec_id, event_id)`.
/// That `Err` flowed through `fail_execution_on_error` and terminally FAILED a
/// healthy parent. This test models the concurrent committer as already
/// committed. It asserts that the pre-fix stale-`append_events` discipline
/// conflicts. It also asserts that the conflict is classified as the transient
/// event-id violation, the exact input the pre-fix path failed on.
///
/// GREEN (post-fix): the fixed path appends the same parent events via
/// `append_single_event`. Each re-reads `MAX(event_id)` under the parent-row
/// lock and lands gap-free PAST the committer. No collision, so the healthy
/// parent never fails. This is a store-layer proof of the fix, like the
/// sibling `race_loser_child_cancel_*` test. `persist_child_timeout_race` and
/// its `StartedChildWorkflowCommand` / `StartedTimerCommand` argument types
/// are private to `worker.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_timeout_suspension_append_uses_single_event_discipline_not_stale_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    // Parent RUNNING with WorkflowStarted at event_id 0 → next_event_id = 1.
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

    // A concurrent race-loser child completed first and appended its terminal
    // onto the PARENT via append_single_event (MAX+1 = 1) — exactly
    // wake_parent_for_child_completion. MAX(event_id) is now 1.
    autumn_harvest::store::append_single_event(
        &mut conn,
        parent_exec_id,
        WorkflowEvent::ChildWorkflowCompleted {
            child_id: ExecutionId::new(),
            output: serde_json::json!({"loser": "done"}),
        },
    )
    .await
    .expect("concurrent loser-child terminal takes event_id 1");

    // The suspension batch the child-timeout persist path appends:
    // [ChildWorkflowStarted(new child), TimerStarted(deadline)].
    let suspension_events = vec![
        WorkflowEvent::ChildWorkflowStarted {
            child_id: ExecutionId::new(),
            workflow_name: "timeout_child".to_string(),
            input: serde_json::json!({}),
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("__child_timeout:1:timeout_child"),
            duration_secs: 60,
        },
    ];

    // RED: the pre-fix discipline is a batch append at the stale next_event_id
    // (1, computed before the handler ran). It collides with the committed
    // loser terminal on UNIQUE(workflow_exec_id, event_id). This is the exact
    // Err persist_child_timeout_race fed into fail_execution_on_error, terminally
    // failing the healthy parent. (Rolls back; nothing committed.)
    let stale_next_event_id = 1;
    let red_err = autumn_harvest::store::append_events(
        &mut conn,
        parent_exec_id,
        &suspension_events,
        stale_next_event_id,
    )
    .await
    .expect_err("stale-id batch append must collide on the committed event_id");
    assert!(
        red_err.is_event_id_unique_violation(),
        "the stale-batch collision is the transient event-id violation the pre-fix \
         path terminally failed on; got: {red_err}"
    );

    // GREEN: the fixed discipline is append_single_event per event (parent-row
    // FOR UPDATE + MAX re-read). It serialises against the loser terminal, lands
    // gap-free at ids 2 and 3, and never fails the parent.
    for event in suspension_events {
        autumn_harvest::store::append_single_event(&mut conn, parent_exec_id, event)
            .await
            .expect(
                "append_single_event re-reads MAX and lands past the concurrent loser \
                 terminal — no collision, no terminal failure (issue #779 Codex round-12 P2)",
            );
    }

    // Gap-free, correctly ordered parent history: the healthy parent survives.
    let history = load_history_from_url(&database_url, parent_exec_id).await;
    let names: Vec<&str> = history
        .events
        .iter()
        .map(WorkflowEvent::type_name)
        .collect();
    assert_eq!(
        names,
        vec![
            "WorkflowStarted",
            "ChildWorkflowCompleted",
            "ChildWorkflowStarted",
            "TimerStarted",
        ],
        "the child-timeout suspension append lands gap-free past the concurrent \
         loser terminal — no UNIQUE collision, no terminal failure: {names:?}"
    );
    assert_eq!(history.next_event_id, 4, "four sequential events, ids 0..3");
    assert!(
        !history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "the healthy parent was never terminally failed: {:?}",
        history.events,
    );
}

/// Transient wake-event-ingest event-id conflict → the parent workflow task is
/// re-driven (park + wake), NOT terminally failed (issue #779 shared-path fix).
///
/// Deterministic reproduction of a pre-existing shared-path hazard. #779's
/// child-timeout deadline materialization newly exercises it: a child
/// completes late while the parent is claimed for its due `__child_timeout`
/// deadline. It already affected the `timeout.rs` / `external_task.rs`
/// concurrent-`append_single_event` racers. `ingest_due_timers_and_signals`
/// appends `TimerFired`/`SignalReceived` at a `next_event_id` precomputed from
/// an earlier `load_history`. It takes no exec-row lock and does no
/// `MAX(event_id)` recompute. A concurrent committer that took that same
/// `event_id` first makes the ingest's batch insert hit
/// `UNIQUE(workflow_exec_id, event_id)`.
///
/// RED (pre-fix): that raw `ingest_due_timers_and_signals` error flowed through
/// `fail_execution_on_error` and terminally FAILED a healthy run. This test
/// first asserts that the raw error is classified as the transient conflict,
/// the exact input the pre-fix path failed on. GREEN:
/// `ingest_wake_events_or_requeue` re-drives the task instead (`Ok(None)`). It
/// leaves the run RUNNING and appends no event. A fresh re-claim and re-drive
/// then advances past the winner's committed event, so the ingest completes
/// cleanly. The same conflict provably cannot recur: it converges in one
/// retry, with no hot loop.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_event_id_conflict_requeues_parent_instead_of_failing() {
    use autumn_harvest::queue::{self};
    use autumn_harvest::schema::harvest_task_queue::dsl as task_dsl;
    use autumn_harvest::schema::harvest_workflow_executions::dsl as exec_dsl;
    use diesel::QueryDsl;
    use std::time::Duration;

    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    // Seed a RUNNING execution with WorkflowStarted at event_id 0.
    let exec_id = insert_workflow_execution(&mut conn).await;
    autumn_harvest::store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::workflow_started(
            serde_json::json!({}),
            chrono::Utc::now(),
        )],
        0,
    )
    .await
    .expect("seed WorkflowStarted");

    // A DUE timer gives the ingest real work (a TimerFired to append).
    let due_timer = "user_timer:reminder";
    diesel::insert_into(autumn_harvest::schema::harvest_timers::dsl::harvest_timers)
        .values(&autumn_harvest::models::NewHarvestTimer {
            workflow_exec_id: exec_id.as_uuid(),
            timer_id: due_timer,
            fires_at: chrono::Utc::now() - chrono::Duration::seconds(2),
        })
        .execute(&mut conn)
        .await
        .expect("insert due timer");

    // Enqueue + claim the parent workflow task so it is RUNNING (claimed) — the
    // state `park_workflow_task` requires to release the claim on re-drive.
    let mut params =
        queue::EnqueueParams::new("default", queue::TaskType::Workflow, serde_json::json!({}));
    params.workflow_exec_id = Some(exec_id.as_uuid());
    params.scheduled_at = chrono::Utc::now() - chrono::Duration::seconds(1);
    queue::enqueue(&mut conn, &params).await.expect("enqueue");
    let task = queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "worker-779",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("claim_task")
    .expect("a claimable workflow task");

    // The ingest would compute next_event_id = 1 (after WorkflowStarted at 0).
    let stale_next_event_id = 1;

    // Concurrent committer wins the race: append_single_event takes event_id 1
    // (MAX+1) first — exactly the timeout.rs / external_task.rs / #779
    // child-timeout materializer racer.
    autumn_harvest::store::append_single_event(
        &mut conn,
        exec_id,
        WorkflowEvent::SignalReceived {
            signal_name: "concurrent".to_string(),
            payload: serde_json::json!({}),
        },
    )
    .await
    .expect("concurrent committer appends at event_id 1");

    // RED: the raw ingest at the stale id hits UNIQUE(exec_id, event_id). The
    // pre-fix code fed this exact Err into fail_execution_on_error, terminally
    // FAILING the run. (This call rolls back; nothing is committed.)
    let raw_err = autumn_harvest::worker::ingest_due_timers_and_signals(
        &mut conn,
        exec_id,
        stale_next_event_id,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect_err("stale-id ingest must conflict on the committed event_id");
    assert!(
        raw_err.is_event_id_unique_violation(),
        "the ingest conflict must be classified as the transient event-id conflict \
         (the input the pre-fix fail_execution_on_error terminally failed on); got: {raw_err}"
    );

    // GREEN: the fix converts that same conflict into a re-drive, not a failure.
    let outcome = autumn_harvest::worker::ingest_wake_events_or_requeue(
        &mut conn,
        &task,
        "worker-779",
        Duration::from_secs(5),
        exec_id,
        stale_next_event_id,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("a transient conflict is NOT a genuine error");
    assert!(
        outcome.is_none(),
        "transient event-id conflict → the parent task is re-driven (None), not failed"
    );

    // (a) the run was NOT terminally failed — still RUNNING.
    let exec_state: String = exec_dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(exec_dsl::state)
        .first(&mut conn)
        .await
        .expect("load exec state");
    assert_eq!(exec_state, "RUNNING", "the healthy run must not be failed");

    // (b) no WorkflowFailed event was appended.
    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        !history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowFailed { .. })),
        "no WorkflowFailed on a transient conflict: {:?}",
        history.events,
    );

    // (c) the timer was NOT marked fired (the ingest transaction rolled back).
    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert!(
        timers.iter().all(|t| !t.fired),
        "the ingest rolled back — the timer must stay unfired until a clean re-drive"
    );

    // (d) the parent task was re-pended to PENDING (park + wake) and is
    //     immediately claimable — no retry attempt consumed, no failure.
    let (task_state, task_worker, task_scheduled): (
        String,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
    ) = task_dsl::harvest_task_queue
        .find(task.id)
        .select((task_dsl::state, task_dsl::worker_id, task_dsl::scheduled_at))
        .first(&mut conn)
        .await
        .expect("load task row");
    assert_eq!(
        task_state, "PENDING",
        "the parent task is re-pended for immediate re-claim"
    );
    assert!(task_worker.is_none(), "the claim was released on re-drive");
    assert!(
        task_scheduled <= chrono::Utc::now(),
        "the re-pended task is immediately claimable (scheduled_at in the past)"
    );

    // Re-drive: a fresh re-claim and history load advance next_event_id past
    // the winner's committed event. So the ingest completes cleanly, and the
    // same conflict provably cannot recur.
    let task2 = queue::claim_task(
        &mut conn,
        &["default".to_string()],
        "worker-779",
        "",
        None,
        &[],
        &[],
    )
    .await
    .expect("re-claim")
    .expect("the re-pended parent task is immediately re-claimable");
    let fresh = load_history_from_url(&database_url, exec_id).await;
    let (fired_timers, _signals) = autumn_harvest::worker::ingest_wake_events_or_requeue(
        &mut conn,
        &task2,
        "worker-779",
        Duration::from_secs(5),
        exec_id,
        fresh.next_event_id,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .expect("clean re-drive")
    .expect("the re-drive ingests cleanly (Some), not a second conflict");
    assert_eq!(
        fired_timers.len(),
        1,
        "the due timer fires on the clean re-drive"
    );
    assert_eq!(fired_timers[0].as_str(), due_timer);

    // The timer is now durably fired + a TimerFired event was appended; the run
    // proceeds normally (still RUNNING) — the healthy run was never lost.
    let timers_after = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert!(
        timers_after
            .iter()
            .any(|t| t.timer_id == due_timer && t.fired),
        "the timer is marked fired after the clean re-drive"
    );
    let history_after = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history_after
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "a TimerFired was appended on the clean re-drive"
    );
    let exec_state_after: String = exec_dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(exec_dsl::state)
        .first(&mut conn)
        .await
        .expect("load exec state after re-drive");
    assert_eq!(
        exec_state_after, "RUNNING",
        "the run proceeds normally after the transient-conflict re-drive"
    );
}

/// Lock-order regression (issue #779, ABBA claim):
/// [`materialize_due_child_timeout_deadlines`] MUST acquire the parent
/// EXECUTION row `FOR UPDATE` **before** it takes the due `harvest_timers` rows
/// `FOR UPDATE`.
///
/// The operator cancel/terminate path
/// (`execution::notify_awaited_parent_of_child_terminal`) locks the parent
/// execution row first and then calls the materializer. The worker-wake
/// (`wake_parent_for_child_completion`/`_failure`) and child-execution-timeout
/// (`timeout::wake_parent_for_child_timeout`) callers reach the materializer
/// with **no** outer parent lock. Suppose the materializer took the timer lock
/// first. Then those two orderings would invert (ABBA). Take two concurrent
/// wakes of the same overdue parent. One comes via a normal child terminal
/// (timer first). One comes via an operator cancel/terminate of a sibling
/// child (execution row first). They would deadlock, and Postgres would abort
/// a healthy terminal notification.
///
/// This proves the unified execution-row → timer order **deterministically**,
/// with no timing-dependent deadlock forcing. Hold the parent execution row
/// lock open on a dedicated connection. Invoke the materializer on another.
/// Wait until its backend is blocked on a lock (its FIRST acquisition). While
/// it is blocked, a `SELECT ... FOR UPDATE NOWAIT` probe must succeed, so the
/// timer row is NOT yet locked. Under the pre-fix timer-first order this probe
/// fails with `lock_not_available`. The materializer would already hold the
/// timer `FOR UPDATE` while blocked on the exec-row append. Releasing the held
/// lock lets the materializer complete and fire the due deadline exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[allow(clippy::too_many_lines)]
async fn materializer_locks_execution_row_before_timers_no_abba() {
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::SimpleAsyncConnection;

    #[derive(diesel::QueryableByName)]
    struct Pid {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pid: i32,
    }
    #[derive(diesel::QueryableByName)]
    struct Waiting {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");
    let mut conn_lock = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect lock");
    let mut conn_probe = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect probe");
    let mut conn_mat = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect mat");

    // Parent with a single OVERDUE `__child_timeout` deadline the materializer
    // will want to fire.
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
    let timer_id_str = "__child_timeout:1:timeout_child";
    diesel::insert_into(autumn_harvest::schema::harvest_timers::table)
        .values(&autumn_harvest::models::NewHarvestTimer {
            workflow_exec_id: parent_exec_id.as_uuid(),
            timer_id: timer_id_str,
            fires_at: chrono::Utc::now() - chrono::Duration::seconds(2),
        })
        .execute(&mut conn)
        .await
        .expect("insert overdue deadline timer");

    // Capture the materializer connection's backend PID so we can watch it block.
    let mat_pid: i32 = diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(&mut conn_mat)
        .await
        .expect("materializer backend pid")
        .pid;

    // Hold the parent execution row lock open (BEGIN + FOR UPDATE, no commit).
    conn_lock
        .batch_execute("BEGIN")
        .await
        .expect("begin lock txn");
    diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(parent_exec_id.as_uuid())
        .execute(&mut conn_lock)
        .await
        .expect("lock parent execution row");

    // Run the materializer on its own connection; it must block on the parent
    // execution row lock as its FIRST acquisition (unified exec-row → timer
    // order). Move the owned connection into the task.
    let mat_handle = tokio::spawn(async move {
        let r = autumn_harvest::worker::materialize_due_child_timeout_deadlines(
            &mut conn_mat,
            parent_exec_id,
        )
        .await;
        (conn_mat, r)
    });

    // Wait until the materializer backend is genuinely blocked on a lock.
    let mut blocked = false;
    for _ in 0..200 {
        let waiting: i64 = diesel::sql_query(
            "SELECT COUNT(*)::BIGINT AS n FROM pg_stat_activity \
             WHERE pid = $1 AND wait_event_type = 'Lock'",
        )
        .bind::<diesel::sql_types::Integer, _>(mat_pid)
        .get_result::<Waiting>(&mut conn_probe)
        .await
        .expect("poll pg_stat_activity")
        .n;
        if waiting >= 1 {
            blocked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        blocked,
        "the materializer must block on the parent execution row lock (its first acquisition)"
    );

    // While the materializer is blocked, the timer row must NOT be locked — proof
    // it acquires the execution row FIRST. Under the pre-fix timer-first order it
    // would already hold this row `FOR UPDATE`, so NOWAIT would fail.
    let probe = diesel::sql_query(
        "SELECT id FROM harvest_timers \
         WHERE workflow_exec_id = $1 AND timer_id = $2 FOR UPDATE NOWAIT",
    )
    .bind::<diesel::sql_types::Uuid, _>(parent_exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(timer_id_str)
    .execute(&mut conn_probe)
    .await;
    assert!(
        probe.is_ok(),
        "timer row must be unlocked while the materializer blocks on the exec-row lock \
         (execution-row → timer order); got {probe:?}"
    );

    // Release the execution row lock; the materializer proceeds and fires the
    // due deadline exactly once.
    conn_lock
        .batch_execute("ROLLBACK")
        .await
        .expect("release lock");

    let (mut conn_mat, result) = mat_handle.await.expect("materializer task joins");
    let fired = result.expect("materialize succeeds");
    assert_eq!(fired, 1, "the one overdue child-timeout deadline is fired");

    let timer_fired: bool = autumn_harvest::schema::harvest_timers::dsl::harvest_timers
        .filter(
            autumn_harvest::schema::harvest_timers::dsl::workflow_exec_id
                .eq(parent_exec_id.as_uuid()),
        )
        .filter(autumn_harvest::schema::harvest_timers::dsl::timer_id.eq(timer_id_str))
        .select(autumn_harvest::schema::harvest_timers::dsl::fired)
        .first(&mut conn_mat)
        .await
        .expect("load timer fired flag");
    assert!(
        timer_fired,
        "the deadline timer is marked fired after release"
    );
}
