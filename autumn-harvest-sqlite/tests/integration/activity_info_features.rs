//! Registration-time honor/rejection of unsupported `ActivityInfo`-level features —
//! the THIRD author-declared-feature ingress surface (Codex #1069 P2, `runtime.rs:39`).
//!
//! After the `ScheduleActivity` **command** fields (`retry_policy_override` /
//! `start_to_close_override`, honored) and the **`WorkflowInfo`** fields (rejected at
//! registration), a `#[activity(...)]` can still declare *dispatch-time defaults the
//! Postgres worker enforces from `ActivityInfo` but which are NOT copied into the
//! `ScheduleActivity` command* — most notably `default_schedule_to_close` (the total,
//! cross-retry deadline, issue #378). A name-only body registration never saw them, so
//! they were neither honored nor rejected: an activity with a total deadline and a
//! retry policy could keep backing off/retrying PAST the declared deadline and
//! eventually complete instead of recording `ActivityTimedOut`.
//!
//! The audited, `ActivityInfo`-driven
//! [`SqliteRuntime::register_activity`](autumn_harvest_sqlite::SqliteRuntime::register_activity)
//! closes that gap: it HONORS `default_schedule_to_close` (persisted + enforced) and
//! REJECTS the dispatch-admission / cross-worker fields LOUDLY at registration (a
//! setup-time panic naming the feature). These tests drive the PUBLIC API; the
//! `unsupported_activity_feature` audit fn has exhaustive in-crate unit coverage.

// The `#[workflow]`/`#[activity]` macros reference their input params, so a
// deliberately-unused param reads as a "used underscore binding" through expansion.
#![allow(clippy::used_underscore_binding)]
// The `#[activity]` fixtures have trivial await-free bodies — the crate runs a
// registered SYNCHRONOUS closure, not the macro's async body.
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
// The total deadline is 1s, but the retry back-off is 30s — so on the FIRST failure
// the next attempt would land 30s out, well past the 1s total deadline. The worker
// therefore seals the activity terminal `ActivityTimedOut { ScheduleToClose }` instead
// of requeuing — the run converges in one `run_until_blocked` call with no clock
// manipulation. `default_schedule_to_close` is NOT carried on the ScheduleActivity
// command; it is resolved by name from the registry at dispatch (exactly as the
// Postgres worker resolves ActivityInfo from its HandlerRegistry).
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

// RED pre-fix: `default_schedule_to_close` was never seen by the backend, so the body
// kept being retried (`RetryPolicy::fixed(5, ..)`) and eventually recorded
// `ActivityFailed` after 5 attempts — never a terminal `ActivityTimedOut`, and a
// slow-but-eventually-succeeding body would have recorded `ActivityCompleted` past the
// declared total deadline.
#[tokio::test]
async fn schedule_to_close_deadline_records_activity_timed_out_not_retry_or_complete() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&schedule_to_close_wf_info());
    // The audited, info-based registration — HONORS the declared schedule_to_close.
    rt.register_activity(&always_fails_info(), |_input| Err("boom".to_string()));
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

    // The timed-out history replays byte-identically on the core engine — the handler
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
    rt.register_activity(&under_deadline_info(), Ok);
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
// The pre-body claim check enforces `schedule_to_close` against the cycle-start clock,
// but the total (cross-retry) deadline can elapse WHILE the body runs. A body that
// starts just before its cap and returns `Ok` after it must NOT be finalized as
// `ActivityCompleted` past the declared cap. The Postgres backend enforces this via its
// background timeout scanner (a RUNNING activity past its cap is timed out and its late
// completion loses the race); this scanner-less backend must recheck the deadline inline
// at finalize time. Codex #1069 P2, `worker.rs:211`.
//
// Driven deterministically (no real sleeping) via an injectable clock the body advances:
// the runtime's clock and the activity body share an `AtomicI64`. The body advances it
// 5s past the 1s deadline BEFORE returning `Ok`, so the cycle-start read (t0 < t0+1s)
// lets the body run while the post-body finalize read (t0+5s) is past the total cap.
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

// RED pre-fix: `drain_ready` had no finalize-time recheck, so a body that succeeded past
// its total deadline recorded `ActivityCompleted` and the workflow completed "completed".
#[tokio::test]
async fn a_successful_body_finalized_past_the_total_deadline_records_timed_out_not_completed() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&slow_success_wf_info());

    // A shared, injectable clock (epoch-millis). Both the runtime driver and the
    // activity body read/advance it, so the deadline elapses WHILE the body runs with
    // no real-time sleeping.
    let t0: i64 = 5_000_000_000;
    let clock = Arc::new(AtomicI64::new(t0));
    let clock_for_rt = Arc::clone(&clock);
    rt.set_clock(move || {
        DateTime::from_timestamp_millis(clock_for_rt.load(Ordering::SeqCst)).unwrap()
    });

    // The body SUCCEEDS but advances the shared clock 5s past the 1s deadline BEFORE
    // returning: the pre-body claim check (cycle now = t0 < t0+1s) lets it run, yet the
    // post-body finalize clock (t0+5s) has passed the total cap.
    let clock_for_body = Arc::clone(&clock);
    rt.register_activity(&slow_but_succeeds_info(), move |input| {
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

// ── PRE-RUN check reads a FRESH clock (Codex #1069 P2, `worker.rs:194`) ─────────────
//
// `drain_ready` captures the cycle-start `now` ONCE and drains every ready activity in
// one pass. The pre-run `schedule_to_close` check must NOT compare a LATER task's
// absolute deadline against that stale `now`: an EARLIER body drained in the same pass
// can consume real wall-clock time, so a task queued behind it can cross its total
// deadline WHILE it waited its turn. Comparing against the stale `now` runs the body
// anyway — the finalize-time recheck only seals the timeout AFTER the side effect
// already happened, starting an attempt past the declared cap. Reading current time at
// the pre-run check seals an already over-deadline queued task via the SAME timeout
// path BEFORE its body runs.
//
// Driven deterministically (no sleeping) via a shared injectable clock: BOTH activities
// are fanned out in ONE suspension batch → both are drained in ONE pass, in seq (input)
// order. `earlier_body` (seq 0) advances the shared clock 5s past `queued_side_effect`'s
// 1s deadline, then completes; `queued_side_effect` (seq 1, drained after) has an
// observable side-effect counter.

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
    // Fan out BOTH activities in one suspension batch → they become ready together and
    // are drained in ONE `drain_ready` pass, `earlier_body` first (lower seq).
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
        // `queued_side_effect` times out → the fail-fast fan-out surfaces the failure;
        // map it so the run reaches a clean terminal state regardless of how the timeout
        // is surfaced (the assertions below are on history, not the returned string).
        Err(_) => Ok("queued_timed_out".to_string()),
    }
}

// RED pre-fix: the pre-run check used the STALE cycle-start `now` (< the deadline the
// earlier body pushed us past), so `queued_side_effect`'s body RAN (counter == 1) and
// was timed out only at finalize — AFTER its side effect. GREEN post-fix: the pre-run
// check reads the fresh clock (past the deadline), so the body NEVER runs (counter == 0)
// while the SAME terminal `ActivityTimedOut { ScheduleToClose }` is still recorded.
#[tokio::test]
async fn a_queued_activity_whose_deadline_elapsed_while_an_earlier_body_ran_is_timed_out_before_running()
 {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&drained_together_wf_info());

    // A shared, injectable clock (epoch-millis). The runtime driver and `earlier_body`
    // read/advance it, so the deadline elapses WHILE the earlier body runs — no sleeping.
    let t0: i64 = 5_000_000_000;
    let clock = Arc::new(AtomicI64::new(t0));
    let clock_for_rt = Arc::clone(&clock);
    rt.set_clock(move || {
        DateTime::from_timestamp_millis(clock_for_rt.load(Ordering::SeqCst)).unwrap()
    });

    // `earlier_body` (seq 0, drained FIRST) advances the shared clock to t0+5s — well
    // past `queued_side_effect`'s absolute deadline (scheduled_at t0 + 1s = t0+1000) —
    // then completes. No schedule_to_close of its own, so it never times out.
    let clock_for_body = Arc::clone(&clock);
    rt.register_activity(&earlier_body_info(), move |input| {
        clock_for_body.store(t0 + 5_000, Ordering::SeqCst);
        Ok(input)
    });

    // `queued_side_effect` (seq 1, drained AFTER) increments an OBSERVABLE counter so
    // "did its body run?" is falsifiable. Its 1s total deadline has ALREADY elapsed by
    // the time it is claimed (the earlier body pushed the clock to t0+5s).
    let calls = Arc::new(AtomicUsize::new(0));
    let body_calls = Arc::clone(&calls);
    rt.register_activity(&queued_side_effect_info(), move |input| {
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

    // The SAME terminal `ActivityTimedOut { ScheduleToClose }` is still recorded via the
    // existing timeout path — the body just never ran.
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
    // The earlier body completed normally (control: a fresh-clock pre-run check does not
    // falsely time out an on-time earlier body).
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
    // No heartbeating in the synchronous inline drain — reject at setup rather than
    // silently running a body that can never observe the declared heartbeat timeout.
    rt.register_activity(&heartbeating_info(), Ok);
}

#[activity(local = true)]
async fn local_act(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

#[test]
#[should_panic(expected = "local = true")]
fn registering_a_local_activity_panics_naming_the_feature() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    // Local activities dispatch via RunLocalActivity, a command-layer non-goal here.
    rt.register_activity(&local_act_info(), Ok);
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
    rt.register_activity(&plain_act_info(), Ok);
    let exec = rt.start_workflow("plain_act_wf", json!(21)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(21)),
        "a plain, only-honored-defaults activity must run to completion, got {state:?}"
    );
}

// ── HONORED across LATE registration (Codex P2, runtime.rs:944) ─────────────────────
//
// The `schedule_to_close` deadline (issue #378) is NOT carried on the ScheduleActivity
// command — the backend resolves it BY NAME from the registered `ActivityInfo`. If the
// activity is SCHEDULED before its body/`ActivityInfo` is registered (the crate's
// supported late-registration flow: the task waits `PENDING` until the body appears,
// then a re-drive picks it up), a *schedule-time* resolution finds no spec and persists
// NULL — silently DROPPING the declared deadline forever, because the later re-drive
// never backfills it.
//
// The fix resolves `schedule_to_close_at` at CLAIM time from the guaranteed-present
// registered spec (an unregistered activity can never be claimed — it is released back
// to PENDING), anchored to the task's ORIGINAL schedule instant (`scheduled_at`, never
// mutated by retries), NOT claim time (which would extend the budget by the
// registration wait). So a body registered LATE, past a deadline that already elapsed
// while it waited, is sealed terminal `ActivityTimedOut { ScheduleToClose }` — exactly
// as it would be had the body been registered up front.
//
// A body that would SUCCEED is used, and the deadline is passed BEFORE the re-drive, so
// the assertion is strong: the activity is timed out even though its body would have
// returned `Ok` — proving the deadline was honored, not the (never-run, past-deadline)
// body result.
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

// RED pre-fix: `schedule_to_close` was resolved at SCHEDULE time, when the activity was
// not yet registered, so a NULL deadline was persisted and never backfilled — the
// late-registered body simply ran and recorded `ActivityCompleted`, silently ignoring
// its declared 1s total deadline.
#[tokio::test]
async fn schedule_to_close_survives_late_activity_registration() {
    // Deterministic clock (the `_as_of` seam): the activity is scheduled at t0 and the
    // re-drive is 10s later — well past the 1s deadline — with no wall-clock coupling.
    let t0: DateTime<Utc> = DateTime::from_timestamp(7_000_000, 0).unwrap();

    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    rt.register_workflow(&late_reg_wf_info());
    // NOTE: `late_reg_act` is deliberately NOT registered yet — it is scheduled first,
    // then registered LATE (the crate's supported late-registration flow).
    let exec = rt.start_workflow("late_reg_wf", json!(7)).unwrap();

    // First drive at t0: schedules the activity (scheduled_at = t0), claims it, misses
    // the (unregistered) body → releases the claim → surfaces UnregisteredActivity. The
    // task is now PENDING with a stable scheduled_at = t0.
    let err = rt.run_until_blocked_as_of(exec, t0).await.unwrap_err();
    assert!(
        matches!(err, SqliteError::UnregisteredActivity(ref n) if n == "late_reg_act"),
        "expected UnregisteredActivity(late_reg_act), got {err}"
    );

    // Register the body LATE, carrying the declared 1s deadline. A body that would
    // SUCCEED (`Ok`) — yet it must still be timed out below, because the deadline
    // elapsed while it waited for registration.
    rt.register_activity(&late_reg_act_info(), Ok);

    // Re-drive 10s later — well past the 1s total deadline. The claim resolves the
    // deadline from the now-registered spec anchored to scheduled_at(t0), sees it has
    // passed, and seals a terminal `ActivityTimedOut { ScheduleToClose }` BEFORE running
    // the (would-succeed) body.
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
// Symmetric to the timeout case: registering late, but re-driving BEFORE the deadline,
// must complete normally — the claim-time resolution anchored to the ORIGINAL schedule
// instant must not spuriously time out a body that is genuinely in-deadline.
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
    rt.register_activity(&late_reg_ok_act_info(), Ok);
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
