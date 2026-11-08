//! External (task-token) activities on the single-writer backend.
//!
//! A run that awaits `ctx.execute_activity_external` records the handoff and
//! parks. A dispatcher reads the pending handoffs, and settles each token with
//! `complete_external` or `fail_external`. The deadline fires through the
//! durable timer path. Each test runs on a controlled clock, so no test sleeps.

// The `#[workflow]` macro references its input through the expansion.
#![allow(clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use autumn_harvest::TimeoutType;
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_sqlite::{
    ExternalActivityToken, ExternalHandoffFilter, ExternalTaskState, RunState, SqliteError,
    SqliteRuntime,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

/// The epoch-second start of every clock in this file.
const T0: i64 = 3_000_000;

/// A shared clock in epoch milliseconds. The runtime and the test read it.
#[derive(Clone)]
struct Clock(Arc<AtomicI64>);

impl Clock {
    fn at(secs: i64) -> Self {
        Self(Arc::new(AtomicI64::new(secs * 1000)))
    }

    fn set(&self, secs: i64) {
        self.0.store(secs * 1000, Ordering::SeqCst);
    }

    fn install(&self, rt: &mut SqliteRuntime) {
        let millis = Arc::clone(&self.0);
        rt.set_clock(move || {
            DateTime::from_timestamp_millis(millis.load(Ordering::SeqCst)).unwrap_or_default()
        });
    }
}

const fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

/// Await one external activity. The input is its deadline in seconds.
#[workflow]
async fn bridge(ctx: &WorkflowContext, secs: u64) -> Result<String, String> {
    match ctx
        .execute_activity_external("bridge", json!({ "secs": secs }), "camel", secs)
        .await
    {
        Ok(output) => Ok(format!("ok:{output}")),
        Err(e) => Ok(format!("err:{e}")),
    }
}

/// Await one external activity of another name.
#[workflow]
async fn other_bridge(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.execute_activity_external("other", json!({}), "camel", 60)
        .await
        .map_err(|e| e.to_string())
}

fn runtime(clock: &Clock) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    clock.install(&mut rt);
    rt.register_workflow(&bridge_info());
    rt.register_workflow(&other_bridge_info());
    rt
}

/// Start `bridge` with a deadline of `secs`, drive it to its park, and
/// return the run and its token.
async fn parked(rt: &mut SqliteRuntime, secs: u64) -> (ExecutionId, ExternalActivityToken) {
    let exec = rt.start_workflow("bridge", json!(secs)).unwrap();
    match rt.run_until_blocked(exec).await.unwrap() {
        RunState::WaitingExternal(token) => (exec, token),
        other => panic!("expected WaitingExternal, got {other:?}"),
    }
}

fn awaiting_events(rt: &SqliteRuntime, exec: ExecutionId) -> Vec<WorkflowEvent> {
    rt.load_history(exec)
        .unwrap()
        .into_iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityAwaitingExternal { .. }))
        .collect()
}

async fn assert_replays(history: Vec<WorkflowEvent>) {
    let report = WorkflowReplayer::new()
        .register(vec![bridge_info()])
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "{:?}",
        report.status
    );
}

#[tokio::test]
async fn a_run_records_the_handoff_and_parks() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 60).await;

    let awaiting = awaiting_events(&rt, exec);
    assert_eq!(awaiting.len(), 1);
    let WorkflowEvent::ActivityAwaitingExternal {
        activity_id,
        token: recorded,
        name,
        input,
        queue,
        schedule_to_close_secs,
    } = &awaiting[0]
    else {
        unreachable!()
    };
    assert_eq!(*recorded, token);
    assert_eq!(name, "bridge");
    assert_eq!(input, &json!({ "secs": 60 }));
    assert_eq!(queue, "camel");
    assert_eq!(*schedule_to_close_secs, 60);

    let rows = rt
        .list_external_handoffs(&ExternalHandoffFilter::pending())
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.token, token);
    assert_eq!(row.execution_id, exec);
    assert_eq!(row.activity_id, *activity_id);
    assert_eq!(row.activity_name, "bridge");
    assert_eq!(row.workflow_name, "bridge");
    assert_eq!(row.queue, "camel");
    assert_eq!(row.input, json!({ "secs": 60 }));
    assert_eq!(row.state, ExternalTaskState::Pending);
    assert_eq!(row.created_at, at(T0));
    assert_eq!(row.deadline_at, at(T0 + 60));
}

#[tokio::test]
async fn complete_external_resumes_the_run_once() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 60).await;

    clock.set(T0 + 5);
    assert!(rt.complete_external(token, json!({ "reply": 7 })).unwrap());
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("ok:{\"reply\":7}")),
        "{state:?}"
    );

    // A settled token is terminal. Each later settle is a no-op.
    assert!(!rt.complete_external(token, json!(1)).unwrap());
    assert!(!rt.fail_external(token, "late", false).unwrap());

    let all = rt
        .list_external_handoffs(&ExternalHandoffFilter::default().token(token))
        .unwrap();
    assert_eq!(all[0].state, ExternalTaskState::Completed);
    assert_eq!(all[0].updated_at, at(T0 + 5));
    assert_eq!(
        rt.list_external_handoffs(&ExternalHandoffFilter::pending())
            .unwrap(),
        Vec::new()
    );
    let completions = rt
        .load_history(exec)
        .unwrap()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityCompletedExternally { .. }))
        .count();
    assert_eq!(completions, 1);
    assert_replays(rt.load_history(exec).unwrap()).await;
}

#[tokio::test]
async fn fail_external_fails_the_activity() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 60).await;

    assert!(rt.fail_external(token, "card declined", false).unwrap());
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Completed(Value::String(out)) = state else {
        panic!("expected a completed run, got {state:?}");
    };
    assert!(out.starts_with("err:"), "{out}");
    assert!(out.contains("card declined"), "{out}");

    let row = rt
        .list_external_handoffs(&ExternalHandoffFilter::default().token(token))
        .unwrap();
    assert_eq!(row[0].state, ExternalTaskState::Failed);
    assert!(rt.load_history(exec).unwrap().iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityFailedExternally { error, retryable: false, .. }
            if error == "card declined"
    )));
    assert_replays(rt.load_history(exec).unwrap()).await;
}

#[tokio::test]
async fn an_unknown_token_is_not_found() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let token = ExternalActivityToken::new();
    for result in [
        rt.complete_external(token, json!(1)),
        rt.fail_external(token, "x", true),
    ] {
        assert!(
            matches!(result, Err(SqliteError::ExternalTaskNotFound(t)) if t == token),
            "{result:?}"
        );
    }
    let extend = rt.extend_external_deadline(token, Duration::from_secs(5));
    assert!(
        matches!(extend, Err(SqliteError::ExternalTaskNotFound(t)) if t == token),
        "{extend:?}"
    );
}

#[tokio::test]
async fn the_deadline_fires_through_the_timer_path() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 10).await;

    // Before the deadline, the run stays parked.
    clock.set(T0 + 9);
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::WaitingExternal(t) if t == token
    ));

    clock.set(T0 + 10);
    let state = rt.run_until_blocked(exec).await.unwrap();
    let RunState::Completed(Value::String(out)) = state else {
        panic!("expected a completed run, got {state:?}");
    };
    assert!(out.starts_with("err:"), "{out}");

    let history = rt.load_history(exec).unwrap();
    assert!(history.iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityTimedOut {
            timeout_type: TimeoutType::ScheduleToClose,
            ..
        }
    )));
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "the deadline writes no TimerFired"
    );
    let row = rt
        .list_external_handoffs(&ExternalHandoffFilter::default().token(token))
        .unwrap();
    assert_eq!(row[0].state, ExternalTaskState::TimedOut);
    assert!(!rt.complete_external(token, json!(1)).unwrap());
    assert_replays(rt.load_history(exec).unwrap()).await;
}

#[tokio::test]
async fn a_settle_removes_the_deadline() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 10).await;
    assert!(rt.complete_external(token, json!("done")).unwrap());

    // Past the old deadline, the result stands. No timeout follows.
    clock.set(T0 + 100);
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("ok:\"done\"")),
        "{state:?}"
    );
    assert!(
        !rt.load_history(exec)
            .unwrap()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. }))
    );
}

#[tokio::test]
async fn a_reparked_run_keeps_one_handoff_and_its_deadline() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 60).await;

    clock.set(T0 + 30);
    for _ in 0..3 {
        assert!(matches!(
            rt.run_until_blocked(exec).await.unwrap(),
            RunState::WaitingExternal(t) if t == token
        ));
    }
    assert_eq!(awaiting_events(&rt, exec).len(), 1);
    let rows = rt
        .list_external_handoffs(&ExternalHandoffFilter::pending())
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].deadline_at, at(T0 + 60));
}

#[tokio::test]
async fn pending_handoffs_list_by_name_in_deadline_order() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (_, late) = parked(&mut rt, 300).await;
    let (_, soon) = parked(&mut rt, 30).await;
    let other = rt.start_workflow("other_bridge", json!(0)).unwrap();
    rt.run_until_blocked(other).await.unwrap();

    let bridge = rt
        .list_external_handoffs(&ExternalHandoffFilter::pending().activity_name("bridge"))
        .unwrap();
    let tokens: Vec<ExternalActivityToken> = bridge.iter().map(|r| r.token).collect();
    assert_eq!(tokens, vec![soon, late]);

    let first = rt
        .list_external_handoffs(
            &ExternalHandoffFilter::pending()
                .activity_name("bridge")
                .limit(1),
        )
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].token, soon);

    let by_run = rt
        .list_external_handoffs(&ExternalHandoffFilter::pending().execution_id(other))
        .unwrap();
    assert_eq!(by_run.len(), 1);
    assert_eq!(by_run[0].activity_name, "other");

    // A settled handoff leaves the pending read.
    assert!(rt.complete_external(soon, json!(1)).unwrap());
    let left = rt
        .list_external_handoffs(&ExternalHandoffFilter::pending().activity_name("bridge"))
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].token, late);
}

#[tokio::test]
async fn an_ended_run_refuses_a_settle_and_cancels_its_handoff() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 60).await;

    rt.terminate(exec, "operator stop").unwrap();
    assert!(!rt.complete_external(token, json!(1)).unwrap());
    assert!(!rt.fail_external(token, "late", false).unwrap());

    let row = rt
        .list_external_handoffs(&ExternalHandoffFilter::default().token(token))
        .unwrap();
    assert_eq!(row[0].state, ExternalTaskState::Cancelled);
    assert_eq!(
        rt.list_external_handoffs(&ExternalHandoffFilter::pending())
            .unwrap(),
        Vec::new()
    );
    // The run stays terminal, and no settle event lands after its end.
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Terminated(_)
    ));
    assert!(matches!(
        rt.load_history(exec).unwrap().last(),
        Some(WorkflowEvent::WorkflowCancelled { .. })
    ));
}

#[tokio::test]
async fn a_cancel_ends_a_parked_external_wait() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 60).await;

    rt.cancel(exec, "stop").unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Cancelled(_)), "{state:?}");
    let row = rt
        .list_external_handoffs(&ExternalHandoffFilter::default().token(token))
        .unwrap();
    assert_eq!(row[0].state, ExternalTaskState::Cancelled);
}

#[tokio::test]
async fn the_fleet_drivers_treat_the_park_as_blocked() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 60).await;

    assert!(!rt.poll_once().await.unwrap(), "a park is not progress");
    rt.run_until_idle().await.unwrap();

    assert!(rt.complete_external(token, json!(2)).unwrap());
    rt.run_until_idle().await.unwrap();
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
}

#[tokio::test]
async fn extend_external_deadline_moves_the_deadline() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 10).await;

    clock.set(T0 + 8);
    rt.extend_external_deadline(token, Duration::from_secs(20))
        .unwrap();
    let row = rt
        .list_external_handoffs(&ExternalHandoffFilter::default().token(token))
        .unwrap();
    assert_eq!(row[0].deadline_at, at(T0 + 28));
    assert!(rt.load_history(exec).unwrap().iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityExternalDeadlineExtended { token: t, .. } if *t == token
    )));

    // The old deadline passes, and the run still waits.
    clock.set(T0 + 20);
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::WaitingExternal(_)
    ));
    clock.set(T0 + 28);
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(Value::String(ref s)) if s.starts_with("err:")),
        "{state:?}"
    );
    assert_replays(rt.load_history(exec).unwrap()).await;

    let late = rt.extend_external_deadline(token, Duration::from_secs(5));
    assert!(
        matches!(late, Err(SqliteError::ExternalTaskNotPending { token: t, .. }) if t == token),
        "{late:?}"
    );
}

#[tokio::test]
async fn a_too_deep_output_fails_the_activity() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 60).await;

    let mut deep = json!(0);
    for _ in 0..200 {
        deep = json!([deep]);
    }
    assert!(rt.complete_external(token, deep).unwrap());
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(Value::String(ref s)) if s.starts_with("err:")),
        "{state:?}"
    );
    let row = rt
        .list_external_handoffs(&ExternalHandoffFilter::default().token(token))
        .unwrap();
    assert_eq!(row[0].state, ExternalTaskState::Failed);
}

#[tokio::test]
async fn a_handoff_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let clock = Clock::at(T0);
    let (exec, token) = {
        let mut rt = SqliteRuntime::open(&path).unwrap();
        clock.install(&mut rt);
        rt.register_workflow(&bridge_info());
        parked(&mut rt, 60).await
    };

    let mut rt = SqliteRuntime::open(&path).unwrap();
    clock.install(&mut rt);
    rt.register_workflow(&bridge_info());
    let rows = rt
        .list_external_handoffs(&ExternalHandoffFilter::pending())
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].token, token);
    assert!(rt.complete_external(token, json!("back")).unwrap());
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
}

#[tokio::test]
async fn an_overdue_run_times_out_before_a_settle() {
    #[workflow(execution_timeout = "30s")]
    async fn bounded_bridge(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
        ctx.execute_activity_external("bridge", json!({}), "camel", 600)
            .await
            .map_err(|e| e.to_string())
    }

    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    rt.register_workflow(&bounded_bridge_info());
    let exec = rt.start_workflow("bounded_bridge", json!(0)).unwrap();
    let RunState::WaitingExternal(token) = rt.run_until_blocked(exec).await.unwrap() else {
        panic!("expected a park");
    };

    clock.set(T0 + 31);
    assert!(!rt.complete_external(token, json!(1)).unwrap());
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::TimedOut(_)
    ));
    let row = rt
        .list_external_handoffs(&ExternalHandoffFilter::default().token(token))
        .unwrap();
    assert_eq!(row[0].state, ExternalTaskState::Cancelled);
}

/// Joins two external activities in one batch.
#[workflow]
async fn joins_two_externals(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let (a, b) = tokio::join!(
        ctx.execute_activity_external("a", json!({}), "camel", 60),
        ctx.execute_activity_external("b", json!({}), "camel", 60),
    );
    Ok(json!([
        a.map_err(|e| e.to_string())?,
        b.map_err(|e| e.to_string())?
    ]))
}

/// Joins an external activity and a timer in one batch.
#[workflow]
async fn joins_external_and_timer(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let (reply, nap) = tokio::join!(
        ctx.execute_activity_external("a", json!({}), "camel", 60),
        ctx.timer("nap", 5),
    );
    nap.map_err(|e| e.to_string())?;
    reply.map_err(|e| e.to_string())
}

/// The run of `workflow` ends `FAILED`, typed, with the feature
/// `ScheduleExternalActivity`, and leaves no handoff.
async fn assert_external_batch_rejected(workflow: &str) {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    rt.register_workflow(&joins_two_externals_info());
    rt.register_workflow(&joins_external_and_timer_info());
    let exec = rt.start_workflow(workflow, json!(0)).unwrap();

    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::Unsupported(ref m) if m.starts_with("ScheduleExternalActivity — ")),
        "{err:?}"
    );
    let failed = rt
        .load_history(exec)
        .unwrap()
        .into_iter()
        .find_map(|e| match e {
            WorkflowEvent::WorkflowFailed {
                error_type,
                details,
                ..
            } => Some((error_type, details)),
            _ => None,
        });
    let (error_type, details) = failed.expect("a typed WorkflowFailed");
    assert_eq!(error_type.as_deref(), Some("UnsupportedFeature"));
    assert_eq!(
        details.unwrap()["feature"],
        json!("ScheduleExternalActivity")
    );
    assert_eq!(
        rt.list_external_handoffs(&ExternalHandoffFilter::new())
            .unwrap(),
        Vec::new(),
        "the rolled-back cycle leaves no handoff"
    );
}

#[tokio::test]
async fn two_external_activities_in_one_batch_are_rejected() {
    assert_external_batch_rejected("joins_two_externals").await;
}

#[tokio::test]
async fn an_external_activity_beside_a_timer_is_rejected() {
    assert_external_batch_rejected("joins_external_and_timer").await;
}

#[tokio::test]
async fn a_settle_after_the_deadline_times_out_first() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (exec, token) = parked(&mut rt, 10).await;

    // No drive ran since the deadline passed. The late reply loses.
    clock.set(T0 + 11);
    assert!(!rt.complete_external(token, json!("late")).unwrap());
    let row = rt
        .list_external_handoffs(&ExternalHandoffFilter::new().token(token))
        .unwrap();
    assert_eq!(row[0].state, ExternalTaskState::TimedOut);
    assert_eq!(row[0].updated_at, at(T0 + 10));
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(Value::String(ref s)) if s.starts_with("err:")),
        "{state:?}"
    );
    assert_replays(rt.load_history(exec).unwrap()).await;
}

#[tokio::test]
async fn an_extension_after_the_deadline_times_out_first() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (_exec, token) = parked(&mut rt, 10).await;

    clock.set(T0 + 10);
    let late = rt.extend_external_deadline(token, Duration::from_secs(60));
    assert!(
        matches!(
            late,
            Err(SqliteError::ExternalTaskNotPending {
                state: ExternalTaskState::TimedOut,
                ..
            })
        ),
        "{late:?}"
    );
}

#[tokio::test]
async fn an_unbounded_deadline_stays_readable() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (_exec, token) = parked(&mut rt, u64::MAX).await;
    rt.extend_external_deadline(token, Duration::MAX).unwrap();

    let rows = rt
        .list_external_handoffs(&ExternalHandoffFilter::pending())
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].deadline_at > at(T0 + 1_000_000_000));
}

#[tokio::test]
async fn the_filter_clamps_its_limit_and_filters_by_state() {
    let clock = Clock::at(T0);
    let mut rt = runtime(&clock);
    let (_, first) = parked(&mut rt, 10).await;
    let (_, second) = parked(&mut rt, 20).await;

    let zero = rt
        .list_external_handoffs(&ExternalHandoffFilter::pending().limit(0))
        .unwrap();
    assert_eq!(zero.len(), 1, "a zero limit reads one row");

    assert!(rt.complete_external(first, json!(1)).unwrap());
    let done = rt
        .list_external_handoffs(&ExternalHandoffFilter::new().state(ExternalTaskState::Completed))
        .unwrap();
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].token, first);
    let all = rt
        .list_external_handoffs(&ExternalHandoffFilter::new())
        .unwrap();
    assert_eq!(all.len(), 2);
    assert!(all.iter().any(|r| r.token == second));
}

#[tokio::test]
async fn pending_work_does_not_list_the_deadline_as_a_timer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    let clock = Clock::at(T0);
    let mut rt = SqliteRuntime::open(&path).unwrap();
    clock.install(&mut rt);
    rt.register_workflow(&bridge_info());
    let (exec, _token) = parked(&mut rt, 60).await;

    let reader = autumn_harvest_sqlite::SqliteReader::open(&path).unwrap();
    let work = reader.pending_work(exec).unwrap();
    assert!(work.timers.is_empty(), "{work:?}");
}
