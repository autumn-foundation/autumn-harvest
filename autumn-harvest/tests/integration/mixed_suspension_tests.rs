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
//! (AC1), parallel timers parking at the earliest deadline, and the AC8
//! local-activity rejection failing loudly with **no partial durable trace**.
//!
//! The one-transaction property itself is structural — everything durable is
//! inside a single `conn.transaction` in `persist_mixed_suspension_batch` — and
//! what these tests observe of it is that both branches of a batch always land
//! together, and that a rejected batch leaves nothing behind.
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
use chrono::{Duration, Utc};
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use serde_json::Value;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, enqueue_started_workflow_task,
    insert_workflow_execution, load_child_executions_from_url, load_history_from_url,
    load_timers_for_execution_from_url, seed_pending_timer_row, setup_test_database_url_or_env,
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

/// An `ActivityInfo` registered as an INLINE local activity (`is_local: true`).
fn local_act_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
) -> ActivityInfo {
    ActivityInfo {
        is_local: true,
        ..act_info(name, handler)
    }
}

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

/// 4b: child × a SHORT deadline, so the deadline wins over a hanging child.
fn parent_child_vs_short_timer(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .child_workflow_raw("hanging_child", serde_json::json!({"n": 1}))
            .label("child")
            .timer(std::time::Duration::from_secs(2))
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

/// Two durable timers in one batch — the batch must park at the EARLIEST.
fn parent_two_timers(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .timer(std::time::Duration::from_secs(2))
            .label("short")
            .timer(std::time::Duration::from_secs(300))
            .label("long")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"index": winner.index, "label": winner.label}))
    })
}

/// 7b: the three-way race with a SLOW activity and a FAR deadline, so only the
/// signal branch can resolve it.
fn parent_three_way_slow(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("slow_activity", serde_json::json!({"n": 1}), "default")
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

/// AC8: an inline local activity joined with a durable timer — the shape that
/// silently dropped the timer before issue #950.
fn parent_local_activity_and_timer(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let (local, timer) = futures::join!(
            ctx.execute_local_activity_raw(
                "compute_local",
                serde_json::json!({"n": 1}),
                None,
                None
            ),
            ctx.timer("deadline", 30),
        );
        let local = local.map_err(|e| e.to_string())?;
        timer.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"local": local}))
    })
}

/// Issue #1247: arm a cancellable timer, cancel it, then run a local activity
/// — all in one decision cycle. `CancelTimer` is not a durable awaitable, so
/// `local_activity_batch_conflict` (AC8) lets this batch through. Before
/// this fix, `extract_run_local_activity`'s catch-all silently dropped the
/// `CancelTimer`, losing both its `TimerCancelled` event and its
/// `harvest_timers` row delete.
fn parent_cancel_timer_then_local_activity(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let handle = ctx.start_timer("idle", 300);
        handle.cancel().map_err(|e| e.to_string())?;
        let local = ctx
            .execute_local_activity_raw("compute_local", serde_json::json!({"n": 1}), None, None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"local": local}))
    })
}

/// Issue #1247: a `ctx.race()` resolved by its activity branch, followed
/// immediately by a local activity in the SAME decision cycle. The race
/// resolves synchronously inside its own `.await` — the winning activity's
/// terminal is already in history on this replay. So the coroutine keeps
/// running past it without suspending. The timer branch's loser cleanup
/// (`CancelRaceLosers`) and the local activity both land in one batch.
/// Before this fix `extract_run_local_activity` treated `CancelRaceLosers` as
/// unreachable and panicked on it.
fn parent_race_then_local_activity(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("fast_activity", serde_json::json!({"n": 1}), "default")
            .label("work")
            .timer(std::time::Duration::from_secs(300))
            .label("deadline")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        let local = ctx
            .execute_local_activity_raw("compute_local", serde_json::json!({"n": 1}), None, None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"winner": winner.label, "local": local}))
    })
}

/// Issue #1247: the ACTIVITY-loser twin of the test above. A timer loser
/// produces no event at all. So it never exercised whether
/// `apply_race_loser_cancellations`'s synthetic `ActivityFailed` for a
/// cancelled loser activity reaches the in-memory history the co-batched
/// local activity's immediate re-drive replays against.
fn parent_race_activity_loser_then_local_activity(
    ctx: &WorkflowContext,
    _input: Value,
) -> WfFuture<'_> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("fast_activity", serde_json::json!({"n": 1}), "default")
            .label("fast")
            .activity_raw("slow_activity", serde_json::json!({"n": 1}), "default")
            .label("slow")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        let local = ctx
            .execute_local_activity_raw("compute_local", serde_json::json!({"n": 1}), None, None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"winner": winner.label, "local": local}))
    })
}

/// AC1 wait-**all** with a SIGNAL branch: `join!` of a plain
/// `ctx.wait_for_signal` and an activity. `futures::join!` polls in declaration
/// order, so the `WaitForSignal` command is pushed first and the batch records
/// only the sibling's `ActivityScheduled` — the shape Codex round 1 flagged as
/// nd-blocking on its first wake.
fn parent_join_signal_and_activity(ctx: &WorkflowContext, _input: Value) -> WfFuture<'_> {
    Box::pin(async move {
        let (signal, activity) = futures::join!(
            ctx.wait_for_signal("go"),
            ctx.execute_activity_raw("fast_activity", serde_json::json!({"n": 1}), "default"),
        );
        let signal = signal.map_err(|e| e.to_string())?;
        let activity = activity.map_err(|e| e.to_string())?;
        Ok(serde_json::json!({"signal": signal, "activity": activity}))
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

fn registry(workflows: Vec<WorkflowInfo>, activities: Vec<ActivityInfo>) -> Arc<HandlerRegistry> {
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
                WorkflowEvent::ActivityScheduled { .. }
                    | WorkflowEvent::ChildWorkflowStarted { .. }
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
    assert_eq!(
        output.get("label").cloned(),
        Some(serde_json::json!("work"))
    );

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

    // `CancelRaceLosers` -> `queue::delete_pending_timer` DELETES the unfired
    // row, so the correct assertion is that no row survives — `all(|t| t.fired)`
    // would be vacuously true on an empty set and would also pass if the timer
    // had never been armed at all, which is the very thing this batch exists to
    // do. The `TimerStarted` assertion above proves it WAS armed.
    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert!(
        timers.is_empty(),
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

    // Deleted, not merely marked fired — see the note in the activity×timer
    // test above.
    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert!(
        timers.is_empty(),
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

/// Composition 4b — child × timer, **timer wins**: the issue's headline user
/// story ("awaiting a child workflow under a deadline"). The child hangs past
/// the 2s deadline, so the timer branch wins and the still-running child is
/// durably cancelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timer_beats_child_in_a_mixed_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![
            wf_info("e2e_test_workflow", parent_child_vs_short_timer),
            wf_info("hanging_child", hanging_child),
        ],
        vec![],
    );
    let worker = build_runtime_worker("worker-950-timer-child", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("index").cloned(),
        Some(serde_json::json!(1)),
        "the deadline must win over a hanging child: {output}"
    );

    let children = load_child_executions_from_url(&database_url, exec_id).await;
    assert_eq!(children.len(), 1, "exactly one child was started");
    assert_eq!(
        children[0].state, "CANCELLED",
        "the over-deadline child must be durably cancelled, never left running: {:?}",
        children[0]
    );
}

/// Two durable timers in ONE batch park the task at the **earliest** deadline,
/// not the latest. `extract_started_timer_for_suspension` rejects a
/// multi-timer batch, so this shape only exists on the generalized path.
///
/// Load-bearing by construction: the long branch is 300s and
/// `wait_for_execution_state` gives up after 10s, so a park that took the
/// max (or the wrong timer's) deadline fails this test by timing out rather
/// than by a wrong assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_timers_park_at_the_earliest_deadline() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info("e2e_test_workflow", parent_two_timers)],
        vec![],
    );
    let worker = build_runtime_worker("worker-950-parallel-timers", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("index").cloned(),
        Some(serde_json::json!(0)),
        "the 2s branch must win over the 300s branch: {output}"
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    let armed = history
        .events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
        .count();
    assert_eq!(
        armed, 2,
        "BOTH timers must be armed in the one batch: {:?}",
        history.events
    );
}

/// Composition 7b — the three-way batch resolved by its **signal** branch, so
/// the signal wait is load-bearing rather than decoration: the activity is slow
/// and the deadline is far away, leaving the delivered signal as the only way
/// this workflow can complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_way_mixed_batch_resolved_by_its_signal_branch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info("e2e_test_workflow", parent_three_way_slow)],
        vec![act_info("slow_activity", slow_activity)],
    );
    let worker = build_runtime_worker("worker-950-three-way-signal", 4, 2, reg);
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
        Some(serde_json::json!(2)),
        "the signal branch must win the three-way race: {output}"
    );
    assert_eq!(
        output.get("value").cloned(),
        Some(serde_json::json!({"reason": "user"})),
        "the signal payload must surface as the winner value"
    );

    // All three branches were persisted in the one batch: the activity enqueue,
    // the durable deadline row, and the signal park.
    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
            && history
                .events
                .iter()
                .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. }))
            && history
                .events
                .iter()
                .any(|e| matches!(e, WorkflowEvent::SignalReceived { .. })),
        "activity, timer and signal branches must all appear: {:?}",
        history.events
    );
}

/// AC8: a `RunLocalActivity` co-batched with a durable sibling wait terminally
/// fails the execution with a typed error naming the conflict — and leaves
/// **no partial durable trace**: the sibling timer is not armed, the sibling
/// activity is not enqueued, and no branch event is recorded.
///
/// Before this issue the batch "succeeded": `extract_run_local_activity`
/// silently dropped the siblings, so the deadline was armed a whole decision
/// cycle late with no error anywhere. This test pins the behaviour change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_activity_co_batched_with_a_timer_fails_loudly_and_leaves_no_trace() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info(
            "e2e_test_workflow",
            parent_local_activity_and_timer,
        )],
        vec![local_act_info("compute_local", fast_activity)],
    );
    let worker = build_runtime_worker("worker-950-local-conflict", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "FAILED").await;
    worker.shutdown();
    handle.await.expect("join");

    let error = parent.error.unwrap_or_default();
    assert!(
        error.contains("local activity cannot share a suspension batch"),
        "the failure must be the typed AC8 rejection, not a generic \
         'unsupported commands' error: {error}"
    );
    assert!(
        error.contains("StartTimer"),
        "the error must name the sibling command that would have been dropped: {error}"
    );

    // No partial durable trace: the rejection is raised before any of the
    // cycle's persistence steps run.
    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history.events.iter().all(|e| !matches!(
            e,
            WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::LocalActivityScheduled { .. }
        )),
        "a rejected batch must record no branch event at all: {:?}",
        history.events
    );
    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert!(
        timers.is_empty(),
        "a rejected batch must arm no durable timer row: {timers:?}"
    );
}

/// Issue #1247: a `CancelTimer` co-batched with a `RunLocalActivity` must
/// keep its `TimerCancelled` event AND delete the durable `harvest_timers`
/// row it targets. Before this fix, `extract_run_local_activity`'s
/// catch-all silently dropped the command, so neither happened.
///
/// The row is seeded directly rather than armed live via `await_fire`
/// (`seed_pending_timer_row`). It stands in for the row a real
/// `ArmTimer { for_await: true }` cycle from an EARLIER task would have left
/// behind. That lets this test drive the DB-delete path deterministically,
/// instead of racing the scheduler for a live window. The workflow body
/// itself only needs an ordinary same-cycle arm-then-cancel. The batch it
/// produces, `[ArmTimer, CancelTimer, RunLocalActivity]`, is the exact shape
/// the fix must handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_timer_co_batched_with_a_local_activity_deletes_its_row_and_keeps_its_event() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    seed_pending_timer_row(&mut conn, exec_id, "idle", Utc::now() + Duration::hours(1)).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info(
            "e2e_test_workflow",
            parent_cancel_timer_then_local_activity,
        )],
        vec![local_act_info("compute_local", fast_activity)],
    );
    let worker = build_runtime_worker("worker-1247-cancel-timer-local-activity", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("local").cloned(),
        Some(serde_json::json!({"fast": {"n": 1}})),
        "the local activity must still resolve: {output}"
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    let started_at = history
        .events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerStarted { .. }));
    let cancelled_at = history
        .events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::TimerCancelled { .. }));
    let scheduled_at = history
        .events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::LocalActivityScheduled { .. }));
    // `is_some()` on each side is load-bearing, not decoration.
    // `Option<usize>` orders `None` below every `Some`, so a dropped event
    // would pass a bare `<` comparison silently instead of failing loud.
    assert!(
        started_at.is_some() && started_at < cancelled_at,
        "TimerStarted must be recorded before TimerCancelled, not dropped: {:?}",
        history.events
    );
    assert!(
        cancelled_at.is_some() && cancelled_at < scheduled_at,
        "TimerCancelled must be recorded before LocalActivityScheduled, not dropped: {:?}",
        history.events
    );
    assert!(
        !history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "a cancelled timer must never fire, seeded row included: {:?}",
        history.events
    );

    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert!(
        timers.is_empty(),
        "the CancelTimer's harvest_timers row must be deleted, not left for a later \
         claim of this workflow task to ingest as a stale TimerFired: {timers:?}"
    );
}

/// Issue #1247: a `ctx.race()` resolved by its activity branch, immediately
/// followed by a local activity in the same decision cycle. The batch this
/// composition produces is `[RecordMarker, CancelRaceLosers, RunLocalActivity]`.
/// Before this fix, `extract_run_local_activity` treated `CancelRaceLosers` as
/// unreachable and panicked on it. That was a regression introduced, and
/// caught in self-review, while fixing this issue's original silent-drop bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn race_resolved_by_activity_then_local_activity_in_one_batch() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info(
            "e2e_test_workflow",
            parent_race_then_local_activity,
        )],
        vec![
            act_info("fast_activity", fast_activity),
            local_act_info("compute_local", fast_activity),
        ],
    );
    let worker = build_runtime_worker("worker-1247-race-then-local-activity", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("winner").cloned(),
        Some(serde_json::json!("work")),
        "the activity branch must win the race: {output}"
    );
    assert_eq!(
        output.get("local").cloned(),
        Some(serde_json::json!({"fast": {"n": 1}})),
        "the local activity must still resolve after the race: {output}"
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    assert!(
        history
            .events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityScheduled { .. })),
        "the local activity must be scheduled, not lost alongside CancelRaceLosers: {:?}",
        history.events
    );

    // The loser (the 300s deadline timer) must be durably cancelled in the
    // same batch, exactly as the no-local-activity composition above proves.
    // This pins that adding a local activity to the mix does not regress it.
    let timers = load_timers_for_execution_from_url(&database_url, exec_id).await;
    assert!(
        timers.is_empty(),
        "the losing timer's durable row must still be deleted when the winner \
         is followed by a local activity in the same batch: {timers:?}"
    );
}

/// Issue #1247: the ACTIVITY-loser twin. `apply_race_loser_cancellations`
/// durably appends a synthetic `ActivityFailed` for the cancelled loser.
/// Before this fix it never returned that event, so the local activity's
/// immediate in-process re-drive replayed against an in-memory history
/// missing it. A real divergence there would either fail the workflow
/// outright or leave duplicate/missing events behind. This drives the
/// exact composition end to end and checks for both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn race_resolved_by_activity_with_an_open_activity_loser_then_local_activity() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info(
            "e2e_test_workflow",
            parent_race_activity_loser_then_local_activity,
        )],
        vec![
            act_info("fast_activity", fast_activity),
            act_info("slow_activity", slow_activity),
            local_act_info("compute_local", fast_activity),
        ],
    );
    let worker = build_runtime_worker("worker-1247-race-activity-loser-local-activity", 2, 1, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("winner").cloned(),
        Some(serde_json::json!("fast")),
        "the fast activity branch must win the race: {output}"
    );
    assert_eq!(
        output.get("local").cloned(),
        Some(serde_json::json!({"fast": {"n": 1}})),
        "the local activity must still resolve after the race: {output}"
    );

    let history = load_history_from_url(&database_url, exec_id).await;
    let loser_failures = history
        .events
        .iter()
        .filter(|e| {
            matches!(
                e,
                WorkflowEvent::ActivityFailed { error, .. }
                    if error == "lost race to a sibling branch"
            )
        })
        .count();
    assert_eq!(
        loser_failures, 1,
        "the loser activity must get exactly one synthetic ActivityFailed — \
         zero means it was lost, more than one means a divergent re-drive \
         re-ran the cancellation: {:?}",
        history.events
    );
    assert_eq!(
        history
            .events
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::LocalActivityScheduled { .. }))
            .count(),
        1,
        "the local activity must be scheduled exactly once, not re-emitted by \
         a diverging replay: {:?}",
        history.events
    );
}

/// AC1's signal half, end-to-end: `join!(ctx.wait_for_signal(..),
/// ctx.execute_activity(..))` must complete through a real worker — both
/// branches resolve independently and the workflow finishes.
///
/// Codex round 1 (P1): before the `match_signal` mixed-batch fix this shape
/// persisted fine and then nd-blocked on its very first wake, because the plain
/// signal matcher treated the sibling's recorded `ActivityScheduled` as a
/// divergence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_of_a_signal_wait_and_an_activity_resolves_both_branches() {
    let (database_url, _guard) = setup_test_database_url_or_env().await;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("connect");

    let exec_id = insert_workflow_execution(&mut conn).await;
    enqueue_started_workflow_task(&mut conn, exec_id, Value::Null).await;

    let reg = registry(
        vec![wf_info(
            "e2e_test_workflow",
            parent_join_signal_and_activity,
        )],
        vec![act_info("fast_activity", fast_activity)],
    );
    let worker = build_runtime_worker("worker-950-join-signal", 4, 2, reg);
    let pool = build_test_pool(&database_url);
    let handle = spawn_test_worker(Arc::clone(&worker), pool);

    deliver_signal_when_parked(&database_url, exec_id, "go", serde_json::json!({"n": 7})).await;

    let parent = wait_for_execution_state(&database_url, exec_id, "COMPLETED").await;
    worker.shutdown();
    handle.await.expect("join");

    let output = parent.output.expect("completed parent has output");
    assert_eq!(
        output.get("signal").cloned(),
        Some(serde_json::json!({"n": 7})),
        "the signal branch must have resolved with its payload: {output}"
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
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
            && history
                .events
                .iter()
                .any(|e| matches!(e, WorkflowEvent::SignalReceived { .. })),
        "both branches must be recorded: {:?}",
        history.events
    );
}
