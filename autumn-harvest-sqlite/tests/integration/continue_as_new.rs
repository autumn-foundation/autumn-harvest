//! Issue #2023: `continue_as_new` on the `SQLite` backend.
//!
//! The drive seals the run `CONTINUED_AS_NEW` and starts a successor with the
//! same `workflow_id` in one transaction. Undelivered signals move to the
//! successor. Both histories replay on the core `WorkflowReplayer`.

// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{WorkflowIdReusePolicy, WorkflowInfo};
use autumn_harvest_sqlite::{
    ActivitySpec, ExecutionOutcome, RunState, SqliteError, SqliteRuntime,
    UNSUPPORTED_FEATURE_ERROR_TYPE,
};
use serde_json::{Value, json};

/// Counts to 3. Each run adds one, then continues as new until it reaches 3.
#[workflow]
async fn counting_loop(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let next = ctx
        .execute_activity_raw("add_one", json!(n), "default")
        .await
        .map_err(|e| e.to_string())?
        .as_i64()
        .ok_or("add_one returned a non-integer")?;
    if next < 3 {
        ctx.continue_as_new(json!(next))
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(next)
}

/// Phase 0 consumes `first`, then continues. Phase 1 waits for `second`.
#[workflow]
async fn signal_hand_off(ctx: &WorkflowContext, phase: i64) -> Result<Value, String> {
    if phase == 0 {
        ctx.wait_for_signal("first")
            .await
            .map_err(|e| e.to_string())?;
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    ctx.wait_for_signal("second")
        .await
        .map_err(|e| e.to_string())
}

/// Continues as a different type. This backend rejects that by name.
#[workflow]
async fn cross_type(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    ctx.continue_as_new_as_type("counting_loop", json!(0))
        .await
        .map_err(|e| e.to_string())
}

/// Records a side effect and dispatches an activity in the hand-off cycle.
#[workflow]
async fn dispatch_in_hand_off(ctx: &WorkflowContext, phase: i64) -> Result<i64, String> {
    if phase == 0 {
        let _id = ctx.new_uuid();
        // Poll the activity once, so its dispatch rides in the hand-off drain.
        let mut activity =
            std::pin::pin!(ctx.execute_activity_raw("add_one", json!(phase), "default"));
        let mut hand_off = std::pin::pin!(ctx.continue_as_new(json!(1)));
        std::future::poll_fn(|cx| {
            let _ = activity.as_mut().poll(cx);
            hand_off.as_mut().poll(cx)
        })
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(phase)
}

/// Arms a one-hour timer and runs one step, then continues. The timer is
/// still unfired at the hand-off.
#[workflow]
async fn timer_at_hand_off(ctx: &WorkflowContext, phase: i64) -> Result<i64, String> {
    if phase == 0 {
        let mut deadline = std::pin::pin!(ctx.timer("deadline", 3600));
        let mut step = std::pin::pin!(ctx.execute_activity_raw("add_one", json!(0), "default"));
        std::future::poll_fn(|cx| {
            let _ = deadline.as_mut().poll(cx);
            step.as_mut().poll(cx)
        })
        .await
        .map_err(|e| e.to_string())?;
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(phase)
}

fn register(rt: &mut SqliteRuntime) {
    rt.register_workflow(&counting_loop_info());
    rt.register_workflow(&signal_hand_off_info());
    rt.register_workflow(&cross_type_info());
    rt.register_workflow(&dispatch_in_hand_off_info());
    rt.register_workflow(&timer_at_hand_off_info());
    rt.register_activity_raw(
        "add_one",
        ActivitySpec::new(1, |v: Value| Ok(json!(v.as_i64().unwrap_or_default() + 1))),
    );
}

fn runtime() -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    register(&mut rt);
    rt
}

/// Replays `history` of `exec` on the core replayer and asserts success.
async fn assert_replays(exec: ExecutionId, history: Vec<WorkflowEvent>, info: &WorkflowInfo) {
    let report = WorkflowReplayer::new()
        .register_fn(info.name, info.handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "the history of {exec} must replay:\n{report}"
    );
}

/// Counts rows of `table` for `exec` on a raw connection to the file.
fn row_count(path: &std::path::Path, table: &str, filter: &str, exec: ExecutionId) -> i64 {
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.query_row(
        &format!("SELECT COUNT(*) FROM {table} WHERE exec_id = ?1 AND {filter}"),
        rusqlite::params![exec.to_string()],
        |row| row.get(0),
    )
    .unwrap()
}

fn successor_of(state: &RunState) -> ExecutionId {
    match state {
        RunState::ContinuedAsNew(successor) => *successor,
        other => panic!("expected ContinuedAsNew, got {other:?}"),
    }
}

// ── AC1: a loop workflow continues as new and finishes in a successor ─────

#[tokio::test]
async fn a_loop_workflow_continues_as_new_and_finishes_in_a_successor() {
    let mut rt = runtime();
    let first = rt
        .start_workflow_with_id("counting_loop", "loop-1", json!(0))
        .unwrap();

    let second = successor_of(&rt.run_until_blocked(first).await.unwrap());
    let third = successor_of(&rt.run_until_blocked(second).await.unwrap());
    let done = rt.run_until_blocked(third).await.unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v == &json!(3)),
        "{done:?}"
    );

    for sealed in [first, second] {
        assert!(
            matches!(rt.outcome(sealed).unwrap(), ExecutionOutcome::Terminated(ref s) if s == "CONTINUED_AS_NEW"),
            "a continued run is sealed CONTINUED_AS_NEW"
        );
    }

    // The old run ends with the hand-off. The successor starts with its input.
    let old = rt.load_history(first).unwrap();
    assert!(
        matches!(
            old.last(),
            Some(WorkflowEvent::WorkflowContinuedAsNew { new_exec_id, input, new_workflow_type: None })
                if *new_exec_id == second && input == &json!(1)
        ),
        "{old:?}"
    );
    let new = rt.load_history(second).unwrap();
    assert!(
        matches!(new.first(), Some(WorkflowEvent::WorkflowStarted { input, .. }) if input == &json!(1)),
        "{new:?}"
    );

    // Same `workflow_id`: a start with the business id attaches to the last run.
    let attached = rt
        .start_workflow_with_reuse_policy(
            "counting_loop",
            "loop-1",
            json!(0),
            WorkflowIdReusePolicy::AllowDuplicate,
        )
        .unwrap();
    assert_eq!(attached.exec_id, third);
    assert!(!attached.created);
}

#[tokio::test]
async fn the_fleet_driver_follows_the_chain_to_completion() {
    let mut rt = runtime();
    rt.start_workflow_with_id("counting_loop", "loop-2", json!(0))
        .unwrap();

    rt.run_until_idle().await.unwrap();

    let last = rt
        .start_workflow_with_id("counting_loop", "loop-2", json!(0))
        .unwrap();
    assert!(
        matches!(rt.outcome(last).unwrap(), ExecutionOutcome::Completed(ref v) if v == &json!(3))
    );
}

#[tokio::test]
async fn a_redrive_of_a_continued_run_names_the_same_successor() {
    let mut rt = runtime();
    let first = rt.start_workflow("counting_loop", json!(0)).unwrap();
    let second = successor_of(&rt.run_until_blocked(first).await.unwrap());

    let again = successor_of(&rt.run_until_blocked(first).await.unwrap());

    assert_eq!(again, second, "a re-drive starts no second successor");
    assert_eq!(rt.successor(first).unwrap(), Some(second));
    assert_eq!(
        rt.successor(second).unwrap(),
        None,
        "a running run has none"
    );
    let started = rt.load_history(first).unwrap();
    assert_eq!(
        started
            .iter()
            .filter(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn a_reuse_policy_seal_names_no_successor() {
    let mut rt = runtime();
    let prior = rt
        .start_workflow_with_id("signal_hand_off", "key", json!(1))
        .unwrap();
    rt.start_workflow_with_reuse_policy(
        "signal_hand_off",
        "key",
        json!(1),
        WorkflowIdReusePolicy::TerminateIfRunning,
    )
    .unwrap();

    let state = rt.run_until_blocked(prior).await.unwrap();

    assert!(
        matches!(state, RunState::Failed(ref m) if m.contains("not resumable")),
        "{state:?}"
    );
    assert_eq!(rt.successor(prior).unwrap(), None);
}

// ── AC2: signals and the hand-off (issue #2020) ───────────────────────────
//
// The hand-off decision replays every staged signal, as on Postgres. Those
// signals stay with the old run, read or not. A signal for the successor goes
// to the successor after the hand-off.

#[tokio::test]
async fn a_signal_sent_after_the_hand_off_reaches_the_successor() {
    let mut rt = runtime();
    let first = rt.start_workflow("signal_hand_off", json!(0)).unwrap();
    rt.send_signal(first, "first", json!("consumed by run 1"))
        .unwrap();

    let second = successor_of(&rt.run_until_blocked(first).await.unwrap());
    rt.send_signal(second, "second", json!({"for": "run 2"}))
        .unwrap();
    let done = rt.run_until_blocked(second).await.unwrap();

    assert!(
        matches!(done, RunState::Completed(ref v) if v == &json!({"for": "run 2"})),
        "{done:?}"
    );
    let old = rt.load_history(first).unwrap();
    assert!(old.iter().any(
        |e| matches!(e, WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == "first")
    ));
    // The old run is terminal, so a late signal to it is refused.
    assert!(matches!(
        rt.send_signal(first, "second", json!(null)),
        Err(SqliteError::WorkflowNotRunning { .. })
    ));
}

#[tokio::test]
async fn a_signal_staged_before_the_hand_off_stays_on_the_old_run() {
    let mut rt = runtime();
    let first = rt.start_workflow("signal_hand_off", json!(0)).unwrap();
    rt.send_signal(first, "first", json!(1)).unwrap();
    rt.send_signal(first, "second", json!(2)).unwrap();

    let second = successor_of(&rt.run_until_blocked(first).await.unwrap());

    // The hand-off decision replayed `second`, so it is in the old history,
    // as on Postgres. The successor never sees it.
    assert!(rt.load_history(first).unwrap().iter().any(
        |e| matches!(e, WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == "second")
    ));
    let state = rt.run_until_blocked(second).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "second"),
        "{state:?}"
    );
}

/// Phase 0 drains `x`, then continues with the count. Phase 1 drains again.
#[workflow]
async fn drain_then_hand_off(ctx: &WorkflowContext, seen: i64) -> Result<Value, String> {
    let more = ctx.drain_signals_raw("x").map_err(|e| e.to_string())?.len();
    let total = seen + i64::try_from(more).unwrap_or(i64::MAX);
    if seen == 0 {
        ctx.continue_as_new(json!(total))
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(json!(total))
}

/// A drained signal stays with the run that drained it. The successor never
/// gets it a second time (issue #2020).
#[tokio::test]
async fn a_drained_signal_is_not_delivered_again_after_the_hand_off() {
    let mut rt = runtime();
    rt.register_workflow(&drain_then_hand_off_info());
    let first = rt.start_workflow("drain_then_hand_off", json!(0)).unwrap();
    rt.send_signal(first, "x", json!(1)).unwrap();
    rt.send_signal(first, "x", json!(2)).unwrap();

    let second = successor_of(&rt.run_until_blocked(first).await.unwrap());
    let done = rt.run_until_blocked(second).await.unwrap();

    assert!(
        matches!(done, RunState::Completed(ref v) if v == &json!(2)),
        "{done:?}"
    );
    assert!(
        !rt.load_history(second)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SignalReceived { .. }))
    );
}

// ── AC4: both histories replay on the core `WorkflowReplayer` ─────────────

#[tokio::test]
async fn both_histories_replay_on_the_core_replayer() {
    let mut rt = runtime();
    let first = rt.start_workflow("counting_loop", json!(1)).unwrap();
    let second = successor_of(&rt.run_until_blocked(first).await.unwrap());
    let done = rt.run_until_blocked(second).await.unwrap();
    assert!(matches!(done, RunState::Completed(_)), "{done:?}");

    for exec in [first, second] {
        assert_replays(exec, rt.load_history(exec).unwrap(), &counting_loop_info()).await;
    }
}

#[tokio::test]
async fn every_hand_off_shape_replays_on_the_core_replayer() {
    let mut rt = runtime();
    let signal = rt.start_workflow("signal_hand_off", json!(0)).unwrap();
    rt.send_signal(signal, "first", json!(1)).unwrap();
    let dispatch = rt.start_workflow("dispatch_in_hand_off", json!(0)).unwrap();
    let timer = rt.start_workflow("timer_at_hand_off", json!(0)).unwrap();

    for (first, info) in [
        (signal, signal_hand_off_info()),
        (dispatch, dispatch_in_hand_off_info()),
        (timer, timer_at_hand_off_info()),
    ] {
        let second = successor_of(&rt.run_until_blocked(first).await.unwrap());
        if first == signal {
            rt.send_signal(second, "second", json!(2)).unwrap();
        }
        let done = rt.run_until_blocked(second).await.unwrap();
        assert!(matches!(done, RunState::Completed(_)), "{done:?}");
        assert_replays(first, rt.load_history(first).unwrap(), &info).await;
        assert_replays(second, rt.load_history(second).unwrap(), &info).await;
    }
}

// ── Durability ────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_chain_resumes_after_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chain.db");
    let (first, second) = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        register(&mut rt);
        let first = rt
            .start_workflow_with_id("counting_loop", "k", json!(0))
            .unwrap();
        let second = successor_of(&rt.run_until_blocked(first).await.unwrap());
        (first, second)
    };

    let mut rt = SqliteRuntime::open(&path).unwrap();
    register(&mut rt);
    while rt.poll_once().await.unwrap() {}

    assert_eq!(rt.successor(first).unwrap(), Some(second));
    let last = rt
        .start_workflow_with_id("counting_loop", "k", json!(0))
        .unwrap();
    assert!(
        matches!(rt.outcome(last).unwrap(), ExecutionOutcome::Completed(ref v) if v == &json!(3))
    );
    let again = successor_of(&rt.run_until_blocked(first).await.unwrap());
    assert_eq!(again, second, "a reopen starts no second successor");
}

#[tokio::test]
async fn the_hand_off_deletes_the_old_runs_unfired_timer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("timer.db");
    let mut rt = SqliteRuntime::open(&path).unwrap();
    register(&mut rt);
    let first = rt.start_workflow("timer_at_hand_off", json!(0)).unwrap();

    let second = successor_of(&rt.run_until_blocked(first).await.unwrap());

    assert!(
        rt.load_history(first)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "the timer was armed before the hand-off"
    );
    assert_eq!(row_count(&path, "harvest_timers", "fired = 0", first), 0);
    let done = rt.run_until_blocked(second).await.unwrap();
    assert!(matches!(done, RunState::Completed(ref v) if v == &json!(1)));
}

// ── Edges ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_cross_type_continuation_fails_by_name() {
    let mut rt = runtime();
    let exec = rt.start_workflow("cross_type", json!(0)).unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();

    assert!(matches!(err, SqliteError::Unsupported(_)), "{err}");
    match rt.load_history(exec).unwrap().last() {
        Some(WorkflowEvent::WorkflowFailed {
            error_type,
            details,
            ..
        }) => {
            assert_eq!(error_type.as_deref(), Some(UNSUPPORTED_FEATURE_ERROR_TYPE));
            assert_eq!(
                details.as_ref().and_then(|d| d.get("feature")),
                Some(&json!("ContinueAsNew.new_workflow_type"))
            );
        }
        other => panic!("expected a typed WorkflowFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn the_hand_off_keeps_side_effects_and_drops_dispatches() {
    let mut rt = runtime();
    let first = rt.start_workflow("dispatch_in_hand_off", json!(0)).unwrap();

    let second = successor_of(&rt.run_until_blocked(first).await.unwrap());

    let old = rt.load_history(first).unwrap();
    assert!(
        old.iter()
            .any(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. })),
        "the side effect of the hand-off cycle is kept: {old:?}"
    );
    assert!(
        !old.iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. })),
        "the dispatch of the hand-off cycle is dropped: {old:?}"
    );
    let done = rt.run_until_blocked(second).await.unwrap();
    assert!(
        matches!(done, RunState::Completed(ref v) if v == &json!(1)),
        "{done:?}"
    );
}

/// Drains `x`, then continues while it found any. Returns the total.
#[workflow]
async fn drain_until_empty(ctx: &WorkflowContext, seen: i64) -> Result<Value, String> {
    let more = ctx.drain_signals_raw("x").map_err(|e| e.to_string())?.len();
    if more == 0 {
        return Ok(json!(seen));
    }
    let total = seen + i64::try_from(more).unwrap_or(i64::MAX);
    ctx.continue_as_new(json!(total))
        .await
        .map_err(|e| e.to_string())?;
    Ok(json!(total))
}

/// A backlog past the history caps reaches the successors in arrival order.
/// Each run replays only the signals that fit. The rest move on at the
/// hand-off, so each signal arrives exactly once (issue #2020).
#[tokio::test]
async fn a_backlog_past_the_caps_moves_on_at_the_hand_off_exactly_once() {
    let mut rt = runtime();
    rt.register_workflow(&drain_until_empty_info());
    rt.set_history_event_hard_cap(Some(4));
    let mut exec = rt.start_workflow("drain_until_empty", json!(0)).unwrap();
    for i in 0..10 {
        rt.send_signal(exec, "x", json!(i)).unwrap();
    }

    let mut payloads = Vec::new();
    let done = loop {
        let state = rt.run_until_blocked(exec).await.unwrap();
        payloads.extend(
            rt.load_history(exec)
                .unwrap()
                .into_iter()
                .filter_map(|e| match e {
                    WorkflowEvent::SignalReceived { payload, .. } => Some(payload),
                    _ => None,
                }),
        );
        match state {
            RunState::ContinuedAsNew(next) => exec = next,
            other => break other,
        }
    };

    assert!(
        matches!(done, RunState::Completed(ref v) if v == &json!(10)),
        "{done:?}"
    );
    assert_eq!(payloads, (0..10).map(|i| json!(i)).collect::<Vec<_>>());
}
