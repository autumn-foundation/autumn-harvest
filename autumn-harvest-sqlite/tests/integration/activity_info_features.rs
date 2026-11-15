//! Registration-time honor or rejection of unsupported `ActivityInfo`-level
//! features: the THIRD author-declared-feature ingress surface (issue #1069).
//!
//! The `ScheduleActivity` **command** fields (`retry_policy_override` /
//! `start_to_close_override`) are honored. The **`WorkflowInfo`** fields are rejected
//! at registration. A `#[activity(...)]` can still declare *dispatch-time defaults*
//! that the Postgres worker enforces from `ActivityInfo`. They are NOT copied into the
//! `ScheduleActivity` command. The main one is `default_schedule_to_close`, the
//! total, cross-retry deadline (issue #378). A name-only body registration never saw
//! them, so it neither honored nor rejected them. An activity with a total deadline
//! and a retry policy could then retry PAST the deadline. It could complete instead
//! of recording `ActivityTimedOut`.
//!
//! The audited, `ActivityInfo`-driven
//! [`SqliteRuntime::register_activity`](autumn_harvest_sqlite::SqliteRuntime::register_activity)
//! closes that gap. It HONORS `default_schedule_to_close`: persisted and enforced. It
//! REJECTS the dispatch-admission and cross-worker fields LOUDLY at registration,
//! with a setup-time panic that names the feature. These tests drive the PUBLIC API.
//! The `unsupported_activity_feature` audit fn has exhaustive in-crate unit coverage.

// The `#[workflow]`/`#[activity]` macros reference their input params, so a
// deliberately-unused param reads as a "used underscore binding" through expansion.
#![allow(clippy::used_underscore_binding)]
// The `#[activity]` fixtures have trivial await-free bodies. Most tests register
// a synchronous closure with `register_activity_with_body` in their place.
#![allow(clippy::unused_async)]

use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest::{HarvestError, TimeoutType};
use autumn_harvest_sqlite::{RunState, SqliteError, SqliteRuntime};
use chrono::{DateTime, Utc};
use serde_json::json;

// ── HONORED: default_schedule_to_close (the finding's headline, issue #378) ────────
//
// The total deadline is 1s, but the retry back-off is 30s. So after the FIRST
// failure, the next attempt would land well past the deadline. The worker seals a
// terminal `ActivityTimedOut { ScheduleToClose }` instead of requeuing. The run
// converges in one `run_until_blocked` call with no clock manipulation.
// The ScheduleActivity command does NOT carry `default_schedule_to_close`. The worker
// resolves it by name from the registry at dispatch. The Postgres worker resolves
// ActivityInfo from its HandlerRegistry the same way.
#[activity(schedule_to_close = "1s", retry = RetryPolicy::fixed(5, Duration::from_secs(30)))]
async fn always_fails(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn schedule_to_close_wf(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx
        .execute_activity::<_, i64>(&always_fails_info(), n)
        .await
    {
        Ok(_) => Ok("completed".to_string()),
        // A schedule-to-close timeout surfaces (like start-to-close) as
        // HarvestError::Timeout — the workflow drives its own timeout branch.
        Err(HarvestError::Timeout { .. }) => Ok("sched_timed_out".to_string()),
        Err(other) => Err(other.to_string()),
    }
}

// Regression: the backend once ignored `default_schedule_to_close`. The body was
// retried (`RetryPolicy::fixed(5, ..)`) and recorded `ActivityFailed` after 5
// attempts, never `ActivityTimedOut`. A slow body that succeeded would have recorded
// `ActivityCompleted` past the declared total deadline.
#[tokio::test]
async fn schedule_to_close_deadline_records_activity_timed_out_not_retry_or_complete() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&schedule_to_close_wf_info());
    // The audited, info-based registration — HONORS the declared schedule_to_close.
    rt.register_activity_with_body(&always_fails_info(), |_input| Err("boom".to_string()));
    let exec = rt.start_workflow("schedule_to_close_wf", json!(7)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    // The workflow drove its timeout branch (proves it observed HarvestError::Timeout).
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "sched_timed_out"),
        "the workflow must observe the schedule-to-close timeout, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::ScheduleToClose,
                ..
            }
        )),
        "history must record a terminal ActivityTimedOut {{ ScheduleToClose }}:\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "an activity past its total deadline must NOT complete (RED pre-fix):\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "a schedule-to-close timeout is a TimedOut terminal, not a retry-exhaustion \
         ActivityFailed (RED pre-fix would retry to exhaustion):\n{history:?}"
    );

    // The timed-out history replays byte-identically on the core engine. The handler
    // re-observes the recorded ActivityTimedOut and drives the same timeout branch.
    let report = WorkflowReplayer::new()
        .register_fn("schedule_to_close_wf", schedule_to_close_wf_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a ScheduleToClose history must replay cleanly on the core:\n{report}"
    );
}

// ── Control: a generous schedule_to_close does NOT false-positive ──────────────────
#[activity(schedule_to_close = "300s", retry = RetryPolicy::fixed(3, Duration::from_secs(1)))]
async fn under_deadline(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn under_deadline_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&under_deadline_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

#[tokio::test]
async fn a_successful_body_under_a_generous_deadline_completes_normally() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&under_deadline_wf_info());
    rt.register_activity_with_body(&under_deadline_info(), Ok);
    let exec = rt.start_workflow("under_deadline_wf", json!(9)).unwrap();

    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(9)),
        "a body well under its 300s total deadline must complete normally, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "an in-deadline body records ActivityCompleted:\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. })),
        "an in-deadline body must NOT be falsely timed out:\n{history:?}"
    );
}

// ── FINALIZE-TIME recheck: a SUCCESSFUL body finalized past the total deadline ──────
//
// The pre-body claim check enforces `schedule_to_close` against the cycle-start clock.
// The total (cross-retry) deadline can still elapse WHILE the body runs. A body that
// starts before its cap and returns `Ok` after it must NOT finalize as
// `ActivityCompleted`. Postgres enforces this with its background timeout scanner. It
// times out a RUNNING activity past its cap, so the late completion loses the race.
// This backend has no scanner, so it rechecks the deadline at finalize (issue #1069).
//
// An injectable clock drives the test without sleeping. The runtime and the body share
// an `AtomicI64`. The body moves it 5s past the 1s deadline BEFORE it returns `Ok`.
// The cycle-start read (t0) lets the body run. The finalize read (t0+5s) is past the cap.
#[activity(schedule_to_close = "1s")]
async fn slow_but_succeeds(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn slow_success_wf(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx
        .execute_activity::<_, i64>(&slow_but_succeeds_info(), n)
        .await
    {
        Ok(_) => Ok("completed".to_string()),
        // A schedule-to-close timeout surfaces (like start-to-close) as
        // HarvestError::Timeout — the workflow drives its own timeout branch.
        Err(HarvestError::Timeout { .. }) => Ok("sched_timed_out".to_string()),
        Err(other) => Err(other.to_string()),
    }
}

// Regression: `drain_ready` had no finalize-time recheck. A body that succeeded past
// its total deadline recorded `ActivityCompleted`, and the workflow completed.
#[tokio::test]
async fn a_successful_body_finalized_past_the_total_deadline_records_timed_out_not_completed() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&slow_success_wf_info());

    // A shared, injectable clock (epoch-millis). The runtime and the body both use it,
    // so the deadline elapses WHILE the body runs, without sleeping.
    let t0: i64 = 5_000_000_000;
    let clock = Arc::new(AtomicI64::new(t0));
    let clock_for_rt = Arc::clone(&clock);
    rt.set_clock(move || {
        DateTime::from_timestamp_millis(clock_for_rt.load(Ordering::SeqCst)).unwrap()
    });

    // The body SUCCEEDS but moves the clock 5s past the 1s deadline BEFORE it returns.
    // The claim check (t0) lets it run. The finalize clock (t0+5s) is past the cap.
    let clock_for_body = Arc::clone(&clock);
    rt.register_activity_with_body(&slow_but_succeeds_info(), move |input| {
        clock_for_body.store(t0 + 5_000, Ordering::SeqCst);
        Ok(input)
    });

    let exec = rt.start_workflow("slow_success_wf", json!(7)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "sched_timed_out"),
        "the workflow must observe the finalize-time schedule-to-close timeout, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::ScheduleToClose,
                ..
            }
        )),
        "a SUCCESSFUL body finalized past its total deadline must record a terminal \
         ActivityTimedOut {{ ScheduleToClose }}:\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "the late success must NOT record ActivityCompleted (RED pre-fix):\n{history:?}"
    );

    // The timed-out history replays byte-identically on the core engine.
    let report = WorkflowReplayer::new()
        .register_fn("slow_success_wf", slow_success_wf_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a finalize-time ScheduleToClose history must replay cleanly on the core:\n{report}"
    );
}

// ── PRE-RUN check reads a FRESH clock (issue #1069) ─────────────────────────────────
//
// `drain_ready` captures the cycle-start `now` ONCE and drains every ready activity in
// one pass. The pre-run `schedule_to_close` check must NOT compare a LATER task's
// deadline against that stale `now`. An EARLIER body in the same pass can use real
// time. So a task queued behind it can cross its total deadline WHILE it waits.
// With the stale `now`, the body runs anyway. The finalize recheck then seals the
// timeout only AFTER the side effect, from an attempt past the cap. A fresh read at
// the pre-run check seals the task through the SAME timeout path BEFORE its body runs.
//
// A shared injectable clock drives the test without sleeping. BOTH activities fan out
// in ONE suspension batch, so ONE pass drains both in seq (input) order.
// `earlier_body` (seq 0) moves the clock 5s past the 1s deadline, then completes.
// `queued_side_effect` (seq 1, drained after) has an observable side-effect counter.

#[activity]
async fn earlier_body(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[activity(schedule_to_close = "1s")]
async fn queued_side_effect(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn drained_together_wf(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    // One suspension batch makes both ready together. ONE `drain_ready` pass drains
    // them, `earlier_body` first (lower seq).
    match ctx
        .execute_activity_fan_out_raw(vec![
            ("earlier_body".to_string(), json!(n), "default".to_string()),
            (
                "queued_side_effect".to_string(),
                json!(n),
                "default".to_string(),
            ),
        ])
        .await
    {
        Ok(_) => Ok("both_completed".to_string()),
        // The fail-fast fan-out surfaces the timeout of `queued_side_effect`. Map it so
        // the run reaches a clean terminal state. The assertions read history.
        Err(_) => Ok("queued_timed_out".to_string()),
    }
}

// Regression: the pre-run check used the STALE cycle-start `now`. So the body of
// `queued_side_effect` RAN (counter == 1) and timed out only at finalize, AFTER its
// side effect. With a fresh clock, the body NEVER runs (counter == 0). The SAME
// terminal `ActivityTimedOut { ScheduleToClose }` is still recorded.
#[tokio::test]
async fn a_queued_activity_whose_deadline_elapsed_while_an_earlier_body_ran_is_timed_out_before_running()
 {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&drained_together_wf_info());

    // A shared, injectable clock (epoch-millis). The deadline elapses WHILE the earlier
    // body runs, without sleeping.
    let t0: i64 = 5_000_000_000;
    let clock = Arc::new(AtomicI64::new(t0));
    let clock_for_rt = Arc::clone(&clock);
    rt.set_clock(move || {
        DateTime::from_timestamp_millis(clock_for_rt.load(Ordering::SeqCst)).unwrap()
    });

    // `earlier_body` (seq 0, drained FIRST) moves the clock to t0+5s, then completes.
    // That is past the deadline of `queued_side_effect` (t0+1000). It has no
    // schedule_to_close of its own, so it never times out.
    let clock_for_body = Arc::clone(&clock);
    rt.register_activity_with_body(&earlier_body_info(), move |input| {
        clock_for_body.store(t0 + 5_000, Ordering::SeqCst);
        Ok(input)
    });

    // `queued_side_effect` (seq 1, drained AFTER) increments an OBSERVABLE counter, so
    // "did its body run?" is falsifiable. Its 1s total deadline has ALREADY elapsed at
    // its claim.
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = Arc::clone(&calls);
    rt.register_activity_with_body(&queued_side_effect_info(), move |input| {
        body_calls.fetch_add(1, Ordering::SeqCst);
        Ok(input)
    });

    let exec = rt.start_workflow("drained_together_wf", json!(7)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "queued_timed_out"),
        "the run must terminate observing the queued activity's schedule-to-close \
         timeout, got {state:?}"
    );

    // GREEN: the queued body was sealed BEFORE it ran.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the queued activity's body must NOT run — its absolute deadline had already \
         elapsed (an earlier body drained in the same pass advanced the clock past it) \
         when it was claimed. RED pre-fix: the stale cycle `now` let the body run \
         (counter == 1), timed out only at finalize AFTER the side effect."
    );

    // The existing timeout path still records the SAME terminal
    // `ActivityTimedOut { ScheduleToClose }`. Only the body never ran.
    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::ScheduleToClose,
                ..
            }
        )),
        "the over-deadline queued activity must seal ActivityTimedOut {{ ScheduleToClose }} \
         via the existing timeout path:\n{history:?}"
    );
    // Control: the fresh-clock pre-run check does not time out the on-time earlier body.
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "the earlier body must complete normally:\n{history:?}"
    );

    // The timed-out history replays byte-identically on the core engine.
    let report = WorkflowReplayer::new()
        .register_fn("drained_together_wf", drained_together_wf_info().handler)
        .replay_from_events(history)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a pre-run ScheduleToClose history must replay cleanly on the core:\n{report}"
    );
}

// ── REJECTED at registration (setup-time panic, naming the feature) ────────────────

#[activity(heartbeat_timeout = "10s")]
async fn heartbeating(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[test]
#[should_panic(expected = "heartbeat_timeout")]
fn registering_an_activity_with_heartbeat_timeout_panics_naming_the_feature() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    // No scanner times out a silent attempt here. So reject at setup, and do not
    // run a body that can never observe the declared heartbeat timeout.
    rt.register_activity_with_body(&heartbeating_info(), Ok);
}

// Issue #2024: the handler path runs the same audit.
#[test]
#[should_panic(expected = "heartbeat_timeout")]
fn registering_the_handler_with_heartbeat_timeout_panics_naming_the_feature() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_activity(&heartbeating_info());
}

#[activity(local = true)]
async fn local_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

// Issue #2029: a local activity registers. `local_activities.rs` covers it.
#[test]
fn registering_a_local_activity_is_accepted() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_activity_with_body(&local_act_info(), Ok);
}

// ── No regression: a plain activity (only body + honored start_to_close/retry) ─────

#[activity(start_to_close = "30s", retry = RetryPolicy::fixed(3, Duration::from_secs(1)))]
async fn plain_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn plain_act_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&plain_act_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

#[tokio::test]
async fn a_plain_activity_registers_and_runs_unchanged() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&plain_act_wf_info());
    // Only honored fields (start_to_close via command, retry): registers + runs.
    rt.register_activity_with_body(&plain_act_info(), Ok);
    let exec = rt.start_workflow("plain_act_wf", json!(21)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(21)),
        "a plain, only-honored-defaults activity must run to completion, got {state:?}"
    );
}

// ── HONORED across LATE registration (issue #1069) ──────────────────────────────────
//
// The ScheduleActivity command does NOT carry the `schedule_to_close` deadline (issue
// #378). The backend resolves it BY NAME from the registered `ActivityInfo`. The crate
// supports late registration: a task waits `PENDING` until its body appears, and a
// re-drive picks it up. A *schedule-time* resolution then finds no spec and persists
// NULL. The later re-drive never backfills it, so the deadline is silently lost.
//
// So `schedule_to_close_at` resolves at CLAIM time, when the spec is always present.
// An unregistered activity is never claimed: it goes back to PENDING. The deadline is
// anchored to the ORIGINAL schedule instant (`scheduled_at`), which retries never move.
// Claim time would extend the budget by the registration wait. A body registered LATE,
// past an elapsed deadline, is sealed as `ActivityTimedOut { ScheduleToClose }`. That is
// the same result as an up-front registration.
//
// The body would SUCCEED, and the deadline passes BEFORE the re-drive. The activity
// still times out. So the test proves the deadline was honored, not the body result.
#[activity(schedule_to_close = "1s")]
async fn late_reg_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn late_reg_wf(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx
        .execute_activity::<_, i64>(&late_reg_act_info(), n)
        .await
    {
        Ok(_) => Ok("completed".to_string()),
        Err(HarvestError::Timeout { .. }) => Ok("sched_timed_out".to_string()),
        Err(other) => Err(other.to_string()),
    }
}

// Regression: `schedule_to_close` resolved at SCHEDULE time, before registration. A
// NULL deadline was persisted and never backfilled. The late body ran and recorded
// `ActivityCompleted`, past its declared 1s total deadline.
#[tokio::test]
async fn schedule_to_close_survives_late_activity_registration() {
    // Deterministic clock (the `_as_of` seam). The activity is scheduled at t0. The
    // re-drive is 10s later, past the 1s deadline.
    let t0: DateTime<Utc> = DateTime::from_timestamp(7_000_000, 0).unwrap();

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&late_reg_wf_info());
    // `late_reg_act` is deliberately NOT registered yet. It is scheduled first, then
    // registered LATE.
    let exec = rt.start_workflow("late_reg_wf", json!(7)).unwrap();

    // The first drive at t0 schedules the activity and claims it. It finds no body,
    // releases the claim and surfaces UnregisteredActivity. The task is now PENDING
    // with scheduled_at = t0.
    let err = rt.run_until_blocked_as_of(exec, t0).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UnregisteredActivity(ref n) if n == "late_reg_act"),
        "expected UnregisteredActivity(late_reg_act), got {err}"
    );

    // Register the body LATE, with the declared 1s deadline. The body would SUCCEED,
    // but it must still time out: the deadline elapsed while it waited.
    rt.register_activity_with_body(&late_reg_act_info(), Ok);

    // Re-drive 10s later, past the 1s total deadline. The claim resolves the deadline
    // from the spec, anchored to scheduled_at (t0). It seals a terminal
    // `ActivityTimedOut { ScheduleToClose }` BEFORE the body runs.
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(10))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v == "sched_timed_out"),
        "the workflow must observe the schedule-to-close timeout even for a \
         LATE-registered activity, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history.iter().any(|e| matches!(
            e,
            WorkflowEvent::ActivityTimedOut {
                timeout_type: TimeoutType::ScheduleToClose,
                ..
            }
        )),
        "a late-registered activity past its total deadline must record a terminal \
         ActivityTimedOut {{ ScheduleToClose }} (RED pre-fix: the deadline was dropped \
         at schedule time and the body simply completed):\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "a body past its (honored) total deadline must NOT complete — even a \
         would-succeed body (RED pre-fix recorded ActivityCompleted):\n{history:?}"
    );
}

// ── Control: a LATE-registered activity WITHIN its deadline still completes ─────────
//
// The mirror of the timeout case. A late registration with a re-drive BEFORE the
// deadline completes normally. The claim-time resolution must not time out a body
// that is within its deadline.
#[activity(schedule_to_close = "300s")]
async fn late_reg_ok_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[workflow]
async fn late_reg_ok_wf(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let out: i64 = ctx
        .execute_activity(&late_reg_ok_act_info(), n)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

#[tokio::test]
async fn late_registered_activity_within_deadline_completes_normally() {
    let t0: DateTime<Utc> = DateTime::from_timestamp(8_000_000, 0).unwrap();

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&late_reg_ok_wf_info());
    let exec = rt.start_workflow("late_reg_ok_wf", json!(9)).unwrap();

    // Schedule at t0 with the body still unregistered.
    let err = rt.run_until_blocked_as_of(exec, t0).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UnregisteredActivity(ref n) if n == "late_reg_ok_act"),
        "expected UnregisteredActivity(late_reg_ok_act), got {err}"
    );

    // Register late and re-drive only 5s later — well within the 300s deadline.
    rt.register_activity_with_body(&late_reg_ok_act_info(), Ok);
    let state = rt
        .run_until_blocked_as_of(exec, t0 + chrono::Duration::seconds(5))
        .await
        .unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(9)),
        "a late-registered, in-deadline body must complete normally, got {state:?}"
    );

    let history = rt.load_history(exec).unwrap();
    assert!(
        history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "an in-deadline late-registered body records ActivityCompleted:\n{history:?}"
    );
    assert!(
        !history
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityTimedOut { .. })),
        "an in-deadline body must NOT be falsely timed out by the claim-time \
         resolution:\n{history:?}"
    );
}
