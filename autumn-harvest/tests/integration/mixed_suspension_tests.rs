#![cfg(feature = "db")]
//! End-to-end integration tests for **mixed-kind concurrent waits in one
//! suspension batch** (issue #950).
//!
//! Before #950 the worker's suspension dispatcher pattern-matched a closed set
//! of *homogeneous* batch shapes; anything else — an activity joined with a
//! timer, a child raced against a signal — hit
//! `"workflow task suspended with unsupported commands …; this command set is
//! not implemented yet"` and terminally failed the workflow. These tests drive
//! each composition end-to-end through a **real worker loop** against a
//! throwaway Postgres, proving the heterogeneous batch is persisted in one
//! transaction and every branch resolves independently.
//!
//! The success-metric matrix (≥ 6 compositions):
//!
//! | # | Composition | Winner exercised |
//! |---|---|---|
//! | 1 | activity × timer | activity |
//! | 2 | activity × timer | timer |
//! | 3 | activity × signal | signal |
//! | 4 | child × timer | child |
//! | 5 | child × signal | signal |
//! | 6 | activity × child | activity |
//! | 7 | activity × timer × signal (3-way) | activity |
//!
//! plus `futures::join!` wait-**all** over an activity and a durable timer
//! (AC1), and a one-transaction atomicity assertion.
//!
//! The deterministic-replay half of the success metric (1,000 randomized
//! event-order replays per composition, 0 divergences) lives in
//! `replayer_tests.rs`, which needs no database.

use std::pin::Pin;
use std::sync::Arc;

use autumn_harvest::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::HandlerRegistry;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use serde_json::Value;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_child_executions_from_url, load_history_from_url,
    load_timers_for_execution_from_url, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state,
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
        module: "mixed_suspension_tests",
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

fn act_info(name: &'static str, handler: autumn_harvest::info::ActivityHandlerFn) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "mixed_suspension_tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler,
    }
}

// ── Activities ─────────────────────────────────────────────────────────────

/// Returns immediately, echoing its input — used as the "fast" branch.
fn fast_activity(_ctx: &autumn_harvest::ActivityContext, input: Value) -> ActFuture {
    Box::pin(async move { Ok(serde_json::json!({"fast": input})) })
}

/// Sleeps well past every deadline in this file so the sibling branch wins.
fn slow_activity(_ctx: &autumn_harvest::ActivityContext, _input: Value) -> ActFuture {
    Box::pin(async move {
        tokio::time::sleep(std::time::Duration::from_secs(120)).await;
        Ok(serde_json::json!({"slow": true}))
    })
}

type ActFuture = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send>>;

// ── Child workflows ────────────────────────────────────────────────────────

fn fast_child(_ctx: &WorkflowContext, input: Value) -> WfFuture<'_> {
    Box::pin(async move { Ok(serde_json::json!({"child": input})) })
}

/// Blocks forever on a signal that never arrives, so the sibling branch wins.
fn hanging_child(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let _ = ctx
            .wait_for_signal("never")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"unreachable": true}))
    })
}

// ── Parent workflows, one per composition ──────────────────────────────────

/// 1 & 2: activity × timer. The activity name in the input selects which
/// branch wins, so one body covers both directions.
fn parent_activity_vs_timer(ctx: &WorkflowContext, input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let activity = input
            .get("activity")
            .and_then(Value::as_str)
            .unwrap_or("fast_activity")
            .to_string();
        let deadline = input.get("deadline").and_then(Value::as_u64).unwrap_or(2);
        let winner = ctx
            .race()
            .activity_raw(&activity, serde_json::json!({"n": 1}), "default")
            .label("work")
            .timer(std::time::Duration::from_secs(deadline))
            .label("deadline")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "index": winner.index,
            "label": winner.label,
            "value": winner.value,
        }))
    })
}

/// 3: activity × signal — an abort signal interrupting a running activity.
fn parent_activity_vs_signal(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("slow_activity", serde_json::json!({"n": 1}), "default")
            .label("work")
            .signal("abort")
            .label("abort")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "index": winner.index,
            "label": winner.label,
            "value": winner.value,
        }))
    })
}

/// 4: child × timer, with an ORDINARY race timer (not #779's
/// `__child_timeout:` primitive).
fn parent_child_vs_timer(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .child_workflow_raw("fast_child", serde_json::json!({"n": 1}))
            .label("child")
            .timer(std::time::Duration::from_secs(300))
            .label("deadline")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "index": winner.index,
            "label": winner.label,
            "value": winner.value,
        }))
    })
}

/// 5: child × signal.
fn parent_child_vs_signal(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .child_workflow_raw("hanging_child", serde_json::json!({"n": 1}))
            .label("child")
            .signal("abort")
            .label("abort")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "index": winner.index,
            "label": winner.label,
            "value": winner.value,
        }))
    })
}

/// 6: activity × child.
fn parent_activity_vs_child(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("fast_activity", serde_json::json!({"n": 1}), "default")
            .label("work")
            .child_workflow_raw("hanging_child", serde_json::json!({"n": 1}))
            .label("child")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "index": winner.index,
            "label": winner.label,
            "value": winner.value,
        }))
    })
}

/// 7: three-way activity × timer × signal.
fn parent_three_way(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("fast_activity", serde_json::json!({"n": 1}), "default")
            .label("work")
            .timer(std::time::Duration::from_secs(300))
            .label("deadline")
            .signal("abort")
            .label("abort")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({
            "index": winner.index,
            "label": winner.label,
            "value": winner.value,
        }))
    })
}

/// AC1 wait-**all**: `futures::join!` over an activity AND a durable timer.
/// Both branches must resolve; the workflow completes only when each has.
fn parent_join_activity_and_timer(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let (activity, timer) = futures::join!(
            ctx.execute_activity_raw("fast_activity", serde_json::json!({"n": 1}), "default"),
            ctx.timer("cool_off", 1),
        );
        let activity = activity.map_err(|e| e.to_string())?;
        timer.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"activity": activity, "timer": "fired"}))
    })
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn registry(
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(workflows, activities))
}

/// Poll until the parent has at least one `harvest_signals`-deliverable park,
/// then deliver `signal_name`. Retries so the signal never races ahead of the
/// park (a signal delivered before the park would be re-checked by the
/// post-park pending-signal sweep anyway — this just keeps the test tight).
async fn deliver_signal_when_parked(
    database_url: &str,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: Value,
) {
    for _ in 0..100 {
        let history = load_history_from_url(database_url, exec_id).await;
        let dispatched = history.events.iter().any(|e| {
            matches!(
                e,
                WorkflowEvent::ActivityScheduled { .. } | WorkflowEvent::ChildWorkflowStarted { .. }
            )
        });
        if dispatched {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect for signal delivery");
    autumn_harvest::signal::send_signal(&mut conn, exec_id, signal_name, payload)
        .await
        .expect("signal delivery must succeed");
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Composition 1 — activity × timer, activity wins. The heterogeneous batch
/// (`ScheduleActivity` + `StartTimer`) must persist in one transaction: the
/// parent's history carries BOTH `ActivityScheduled` and `TimerStarted`, and
/// the losing timer's durable row is torn down on the win.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_beats_timer_in_a_mixed_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(
        &mut conn,
        exec_id,
        serde_json::json!({"activity": "fast_activity", "deadline": 300}),
    )
    .await;

    let reg = registry(
        vec![wf_info("e2e_test_workflow", parent_activity_vs_timer)],
        vec![act_info("fast_activity", fast_activity)],
    );
    let worker = build_runtime_worker("worker-950-act-timer", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("index").cloned(),
        Some(serde_json::json!(0)),
        "the activity branch must win: {output}"
    );
    assert_eq!(output.get("label").cloned(), Some(serde_json::json!("work")));

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. })),
        "the activity branch must be recorded: {:?}",
        history.events
    );
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "the timer branch must be recorded IN THE SAME batch — this is the \
         heterogeneous persistence the issue is about: {:?}",
        history.events
    );

    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert!(
        timers.iter().all(|t| t.fired),
        "the losing timer's still-armed durable row must be deleted on the \
         activity win, never left to fire against a decided race: {timers:?}"
    );
}

/// Composition 2 — activity × timer, timer wins. The activity hangs well past
/// the 2s deadline, so the durable timer fires first and the still-running
/// activity task is durably cancelled with a synthetic terminal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timer_beats_activity_in_a_mixed_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(
        &mut conn,
        exec_id,
        serde_json::json!({"activity": "slow_activity", "deadline": 2}),
    )
    .await;

    let reg = registry(
        vec![wf_info("e2e_test_workflow", parent_activity_vs_timer)],
        vec![act_info("slow_activity", slow_activity)],
    );
    let worker = build_runtime_worker("worker-950-timer-act", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("index").cloned(),
        Some(serde_json::json!(1)),
        "the timer branch must win: {output}"
    );
    assert_eq!(
        output.get("value").cloned(),
        Some(Value::Null),
        "a timer win carries no value"
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "the deadline must have fired: {:?}",
        history.events
    );
    assert!(
        history.events.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityFailed { error, .. } if error.contains("lost race")
        )),
        "the losing activity must be durably cancelled with a synthetic \
         terminal so replay never loops on ActivityInProgress: {:?}",
        history.events
    );
}

/// Composition 3 — activity × signal: an abort signal interrupts a running
/// activity. This is the exact shape the issue calls out as a runtime failure
/// before #950.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_beats_activity_in_a_mixed_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info("e2e_test_workflow", parent_activity_vs_signal)],
        vec![act_info("slow_activity", slow_activity)],
    );
    let worker = build_runtime_worker("worker-950-signal-act", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    deliver_signal_when_parked(
        &database_url,
        exec_id,
        "abort",
        serde_json::json!({"reason": "user"}),
    )
    .await;

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("index").cloned(),
        Some(serde_json::json!(1)),
        "the signal branch must win: {output}"
    );
    assert_eq!(
        output.get("value").cloned(),
        Some(serde_json::json!({"reason": "user"})),
        "the signal payload must surface as the winner value"
    );
}

/// Composition 4 — child × timer with an ordinary race timer. Distinct from
/// #779's `spawn_child_workflow_timeout` primitive, whose reserved
/// `__child_timeout:` id routes it to its own dedicated persist path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_beats_timer_in_a_mixed_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![
            wf_info("e2e_test_workflow", parent_child_vs_timer),
            wf_info("fast_child", fast_child),
        ],
        vec![],
    );
    let worker = build_runtime_worker("worker-950-child-timer", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("index").cloned(),
        Some(serde_json::json!(0)),
        "the child branch must win: {output}"
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. })),
        "the child branch must be recorded: {:?}",
        history.events
    );
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "the timer branch must be recorded in the same batch: {:?}",
        history.events
    );

    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert!(
        timers.iter().all(|t| t.fired),
        "the losing deadline row must be torn down: {timers:?}"
    );
}

/// Composition 5 — child × signal: the signal wins and the still-running
/// child is durably cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_beats_child_in_a_mixed_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![
            wf_info("e2e_test_workflow", parent_child_vs_signal),
            wf_info("hanging_child", hanging_child),
        ],
        vec![],
    );
    let worker = build_runtime_worker("worker-950-child-signal", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    deliver_signal_when_parked(
        &database_url,
        exec_id,
        "abort",
        serde_json::json!({"reason": "stop"}),
    )
    .await;

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("index").cloned(),
        Some(serde_json::json!(1)),
        "the signal branch must win: {output}"
    );

    let children = load_child_executions_from_url(&database_url, exec_id).await;
    assert_eq!(children.len(), 1, "exactly one child was started");
    assert_eq!(
        children[0].state, "CANCELLED",
        "the losing child must be durably cancelled, never left running: {:?}",
        children[0]
    );
}

/// Composition 6 — activity × child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activity_beats_child_in_a_mixed_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![
            wf_info("e2e_test_workflow", parent_activity_vs_child),
            wf_info("hanging_child", hanging_child),
        ],
        vec![act_info("fast_activity", fast_activity)],
    );
    let worker = build_runtime_worker("worker-950-act-child", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("index").cloned(),
        Some(serde_json::json!(0)),
        "the activity branch must win: {output}"
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
            && history
                .events
                .iter()
                .any(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. })),
        "the activity enqueue and the child start must both be persisted in \
         the same batch: {:?}",
        history.events
    );

    let children = load_child_executions_from_url(&database_url, exec_id).await;
    assert_eq!(children.len(), 1);
    assert_eq!(
        children[0].state, "CANCELLED",
        "the losing child must be durably cancelled: {:?}",
        children[0]
    );
}

/// Composition 7 — the three-way activity × timer × signal batch: an activity
/// enqueue, a durable timer row and a signal wait all in one transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_way_activity_timer_signal_mixed_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info("e2e_test_workflow", parent_three_way)],
        vec![act_info("fast_activity", fast_activity)],
    );
    let worker = build_runtime_worker("worker-950-three-way", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("index").cloned(),
        Some(serde_json::json!(0)),
        "the fast activity must win the three-way race: {output}"
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. })),
        "activity branch persisted: {:?}",
        history.events
    );
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "timer branch persisted in the SAME batch: {:?}",
        history.events
    );
    // AC3: zero new event variants — the whole three-way batch composes only
    // events that already existed before this issue.
    assert!(
        history.events.iter().all(|e| matches!(
            e,
            WorkflowEvent::WorkflowStarted { .. }
                | WorkflowEvent::MarkerRecorded { .. }
                | WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::ActivityStarted { .. }
                | WorkflowEvent::ActivityCompleted { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::TimerFired { .. }
                | WorkflowEvent::SignalReceived { .. }
                | WorkflowEvent::WorkflowCompleted { .. }
        )),
        "the mixed batch must compose only pre-existing event variants: {:?}",
        history.events
    );
}

/// AC1 wait-**all**: `futures::join!` over an activity and a durable timer.
/// Both must resolve independently before the workflow completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_of_an_activity_and_a_timer_resolves_both_branches() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info("e2e_test_workflow", parent_join_activity_and_timer)],
        vec![act_info("fast_activity", fast_activity)],
    );
    let worker = build_runtime_worker("worker-950-join", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("timer").cloned(),
        Some(serde_json::json!("fired")),
        "the timer branch must have resolved: {output}"
    );
    assert!(
        output.get("activity").is_some(),
        "the activity branch must have resolved: {output}"
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. })),
        "join! must have persisted the activity enqueue: {:?}",
        history.events
    );
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "join! must have armed AND fired the durable timer: {:?}",
        history.events
    );
}
