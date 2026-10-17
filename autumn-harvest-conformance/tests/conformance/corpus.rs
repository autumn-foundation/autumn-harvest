//! The corpus: one constructor per scenario.
//!
//! Each constructor returns a [`Scenario`] with the same name as the function.
//! `main.rs` lists every constructor once. The list makes one test each.
//!
//! A scenario must give one history for every legal timing on Postgres. A
//! step therefore waits on an event, never on a sleep. A timer that a script
//! races is at least one second longer than the script needs.

// The `#[workflow]`, `#[activity]` and `#[update]` macros read their inputs,
// so an unused input is a used underscore binding. Some bodies never await.
#![allow(
    clippy::used_underscore_binding,
    clippy::unused_async,
    clippy::unnecessary_wraps
)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::fan_out::FanOutOptions;
use autumn_harvest::prelude::*;
use autumn_harvest::types::ParentClosePolicy;
use serde_json::{Value, json};

use crate::commands::CommandKind as K;
use crate::scenario::{Scenario, Step};

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

// ── Activities shared by the corpus ──────────────────────────────────────────

/// Doubles its input.
#[activity(retry = RetryPolicy::fixed(3, Duration::ZERO))]
pub async fn double(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n * 2)
}

/// Fails the first two attempts, then returns its attempt number.
#[activity(retry = RetryPolicy::fixed(3, Duration::ZERO))]
pub async fn flaky(ctx: &ActivityContext, _n: i64) -> Result<u32, String> {
    let attempt = ctx.info().attempt;
    if attempt < 3 {
        return Err(format!("attempt {attempt} fails"));
    }
    Ok(attempt)
}

/// Fails on every attempt.
#[activity(retry = RetryPolicy::fixed(2, Duration::ZERO))]
pub async fn broken(_ctx: &ActivityContext, _n: i64) -> Result<i64, String> {
    Err("always broken".to_owned())
}

/// Fails once with a typed, non-retryable failure.
#[activity(retry = RetryPolicy::fixed(3, Duration::ZERO))]
pub async fn declined(_ctx: &ActivityContext, _n: i64) -> Result<i64, String> {
    Err(ActivityFailure::non_retryable("PaymentDeclined", "card declined")
        .with_details(json!({ "code": 51 }))
        .into_error_payload())
}

/// Fails the first attempt. The retry waits two seconds.
#[activity(retry = RetryPolicy::fixed(2, Duration::from_secs(2)))]
pub async fn slow_retry(ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    if ctx.info().attempt == 1 {
        return Err("first attempt fails".to_owned());
    }
    Ok(n)
}

/// A local activity that doubles its input.
#[activity(local = true, retry = RetryPolicy::fixed(2, Duration::ZERO))]
pub async fn local_double(_ctx: &ActivityContext, n: i64) -> Result<i64, String> {
    Ok(n * 2)
}

fn shared_activities() -> Vec<ActivityInfo> {
    vec![
        double_info(),
        flaky_info(),
        broken_info(),
        declined_info(),
        slow_retry_info(),
        local_double_info(),
    ]
}

// ── Activities, retries and failures ─────────────────────────────────────────

#[workflow]
pub async fn runs_one_activity(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_activity(&double_info(), n).await.map_err(err)
}

/// One activity completes on its first attempt.
pub fn activity_completes() -> Scenario {
    Scenario::new(
        "activity_completes",
        "runs_one_activity",
        || vec![runs_one_activity_info()],
        &[K::ScheduleActivity],
    )
    .input(json!(21))
    .activities(shared_activities)
}

#[workflow]
pub async fn runs_flaky(ctx: &WorkflowContext, n: i64) -> Result<u32, String> {
    ctx.execute_activity(&flaky_info(), n).await.map_err(err)
}

/// An activity fails twice, then the third attempt succeeds.
pub fn activity_retries_then_succeeds() -> Scenario {
    Scenario::new(
        "activity_retries_then_succeeds",
        "runs_flaky",
        || vec![runs_flaky_info()],
        &[K::ScheduleActivity],
    )
    .activities(shared_activities)
}

#[workflow]
pub async fn reports_broken(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    match ctx.execute_activity::<_, i64>(&broken_info(), n).await {
        Ok(v) => Ok(format!("unexpected {v}")),
        Err(e) => Ok(format!("caught: {e}")),
    }
}

/// An activity spends its retries. The workflow catches the error.
pub fn activity_exhausts_retries() -> Scenario {
    Scenario::new(
        "activity_exhausts_retries",
        "reports_broken",
        || vec![reports_broken_info()],
        &[K::ScheduleActivity],
    )
    .activities(shared_activities)
}

#[workflow]
pub async fn propagates_declined(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_activity(&declined_info(), n).await.map_err(err)
}

/// A typed non-retryable failure ends the activity and fails the run.
pub fn activity_fails_non_retryable() -> Scenario {
    Scenario::new(
        "activity_fails_non_retryable",
        "propagates_declined",
        || vec![propagates_declined_info()],
        &[K::ScheduleActivity],
    )
    .activities(shared_activities)
}

#[workflow]
pub async fn fails_plainly(_ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    Err("plain failure".to_owned())
}

/// The workflow body returns a plain error.
pub fn workflow_fails() -> Scenario {
    Scenario::new(
        "workflow_fails",
        "fails_plainly",
        || vec![fails_plainly_info()],
        &[],
    )
}

#[workflow]
pub async fn fails_typed(_ctx: &WorkflowContext, _n: i64) -> Result<i64, WorkflowFailure> {
    Err(WorkflowFailure::new("BudgetExceeded", "over budget").with_details(json!({ "limit": 5 })))
}

/// The workflow body returns a typed failure with details.
pub fn workflow_fails_typed() -> Scenario {
    Scenario::new(
        "workflow_fails_typed",
        "fails_typed",
        || vec![fails_typed_info()],
        &[],
    )
}

#[workflow]
pub async fn runs_local(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_local_activity(&local_double_info(), n)
        .await
        .map_err(err)
}

/// A local activity runs inline on the workflow worker.
pub fn local_activity() -> Scenario {
    Scenario::new(
        "local_activity",
        "runs_local",
        || vec![runs_local_info()],
        &[K::RunLocalActivity],
    )
    .input(json!(4))
    .activities(shared_activities)
}

#[workflow]
pub async fn joins_activity_and_signal(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    let info = slow_retry_info();
    let (out, go) = tokio::join!(
        ctx.execute_activity::<_, i64>(&info, n),
        ctx.wait_for_signal("go"),
    );
    Ok(json!({ "out": out.map_err(err)?, "go": go.map_err(err)? }))
}

/// A signal wakes the run while its activity waits out a retry backoff.
///
/// The cycle that the signal starts finds the activity in progress, so it
/// re-parks on it.
pub fn signal_while_activity_retries() -> Scenario {
    Scenario::new(
        "signal_while_activity_retries",
        "joins_activity_and_signal",
        || vec![joins_activity_and_signal_info()],
        &[K::ScheduleActivity, K::WaitForActivity, K::WaitForSignal],
    )
    .input(json!(5))
    .activities(shared_activities)
    .step(Step::AwaitEvent {
        event: "ActivityScheduled",
        count: 1,
    })
    .step(Step::Signals(vec![("go", json!("now"))]))
}

// ── Signals, in every style ──────────────────────────────────────────────────

#[workflow]
pub async fn waits_for_go(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(err)
}

/// A blocking wait receives a signal sent after the run parks.
pub fn signal_wait() -> Scenario {
    Scenario::new(
        "signal_wait",
        "waits_for_go",
        || vec![waits_for_go_info()],
        &[K::WaitForSignal],
    )
    .step(Step::Signals(vec![("go", json!({ "k": 1 }))]))
}

/// A blocking wait receives a signal sent before the first cycle.
pub fn signal_before_first_cycle() -> Scenario {
    Scenario::new(
        "signal_before_first_cycle",
        "waits_for_go",
        || vec![waits_for_go_info()],
        &[K::WaitForSignal],
    )
    .initial_signal("go", json!("early"))
}

#[workflow]
pub async fn drain_then_sleep(ctx: &WorkflowContext, _n: i64) -> Result<usize, String> {
    let mut seen = ctx.drain_signals_raw("x").map_err(err)?.len();
    ctx.timer("tick", 1).await.map_err(err)?;
    seen += ctx.drain_signals_raw("x").map_err(err)?.len();
    Ok(seen)
}

/// Issue #2020, case 1: a drain after a timer sees both signals.
pub fn signal_drain() -> Scenario {
    Scenario::new(
        "signal_drain",
        "drain_then_sleep",
        || vec![drain_then_sleep_info()],
        &[K::StartTimer],
    )
    .step(Step::AwaitEvent {
        event: "TimerStarted",
        count: 1,
    })
    .step(Step::Signals(vec![("x", json!("a")), ("x", json!("b"))]))
}

#[workflow]
pub async fn tries_once(ctx: &WorkflowContext, _n: i64) -> Result<Option<String>, String> {
    ctx.try_receive_signal::<String>("x").map_err(err)
}

/// Issue #2020, case 3: a non-blocking read returns a signal at once.
pub fn signal_try_receive() -> Scenario {
    Scenario::new(
        "signal_try_receive",
        "tries_once",
        || vec![tries_once_info()],
        &[],
    )
    .initial_signal("x", json!("hello"))
}

#[workflow]
pub async fn handler_gate(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let slot: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&slot);
    ctx.register_signal_handler_raw("go", move |payload| {
        if let Ok(mut s) = sink.lock() {
            *s = Some(payload);
        }
    });
    // A handler runs at the next history call after it registers.
    ctx.timer("settle", 1).await.map_err(err)?;
    let probe = Arc::clone(&slot);
    ctx.await_condition(move || probe.lock().is_ok_and(|s| s.is_some()))
        .await
        .map_err(err)?;
    let out = slot.lock().map_err(err)?.take().unwrap_or(Value::Null);
    Ok(out)
}

/// Issue #2020, case 2: a push handler and `await_condition` see the signal.
pub fn signal_handler() -> Scenario {
    Scenario::new(
        "signal_handler",
        "handler_gate",
        || vec![handler_gate_info()],
        &[K::StartTimer],
    )
    .step(Step::AwaitEvent {
        event: "TimerStarted",
        count: 1,
    })
    .step(Step::Signals(vec![("go", json!("ship"))]))
}

#[workflow]
pub async fn waits_with_deadline(ctx: &WorkflowContext, secs: u64) -> Result<Value, String> {
    let got = ctx
        .wait_for_signal_timeout("go", Duration::from_secs(secs))
        .await
        .map_err(err)?;
    Ok(got.unwrap_or_else(|| json!("deadline")))
}

/// The signal wins its race against a long deadline.
pub fn signal_beats_deadline() -> Scenario {
    Scenario::new(
        "signal_beats_deadline",
        "waits_with_deadline",
        || vec![waits_with_deadline_info()],
        &[K::StartTimer, K::WaitForSignal, K::CancelRaceLosers],
    )
    .input(json!(60))
    .step(Step::AwaitEvent {
        event: "TimerStarted",
        count: 1,
    })
    .step(Step::Signals(vec![("go", json!("in time"))]))
}

/// The deadline wins. No signal arrives.
pub fn deadline_beats_signal() -> Scenario {
    Scenario::new(
        "deadline_beats_signal",
        "waits_with_deadline",
        || vec![waits_with_deadline_info()],
        &[K::StartTimer],
    )
    .input(json!(1))
}

#[workflow]
pub async fn waits_for_match(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let hit = ctx
        .wait_for_signal_matching("x", |v| v.as_i64() == Some(2))
        .await
        .map_err(err)?;
    let rest = ctx.drain_signals_raw("x").map_err(err)?;
    Ok(json!({ "hit": hit, "rest": rest }))
}

/// A filtered wait skips a signal that does not match.
pub fn signal_matching() -> Scenario {
    Scenario::new(
        "signal_matching",
        "waits_for_match",
        || vec![waits_for_match_info()],
        &[K::WaitForSignal],
    )
    .step(Step::Signals(vec![("x", json!(1)), ("x", json!(2))]))
}

// ── Timers and races ─────────────────────────────────────────────────────────

#[workflow]
pub async fn cancels_then_waits(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let long = ctx.start_timer("long", 3600);
    long.cancel().map_err(err)?;
    let cancelled = long.await_fire().await.map_err(err)?;
    let short = ctx.start_timer("short", 1);
    let fired = short.await_fire().await.map_err(err)?;
    Ok(format!("{cancelled:?} then {fired:?}"))
}

/// A cancellable timer is cancelled. A second one fires.
pub fn cancellable_timers() -> Scenario {
    Scenario::new(
        "cancellable_timers",
        "cancels_then_waits",
        || vec![cancels_then_waits_info()],
        &[K::ArmTimer, K::CancelTimer],
    )
}

#[workflow]
pub async fn races_activity_and_timer(ctx: &WorkflowContext, n: i64) -> Result<String, String> {
    let winner = ctx
        .race()
        .activity_raw("double", json!(n), "default")
        .timer(Duration::from_secs(60))
        .run()
        .await
        .map_err(err)?;
    Ok(format!("{winner:?}"))
}

/// An activity wins a race against a long timer.
pub fn race_activity_wins() -> Scenario {
    Scenario::new(
        "race_activity_wins",
        "races_activity_and_timer",
        || vec![races_activity_and_timer_info()],
        &[K::ScheduleActivity, K::StartTimer, K::CancelRaceLosers],
    )
    .input(json!(3))
    .activities(shared_activities)
}

// ── Side effects, markers, bookkeeping ──────────────────────────────────────

#[workflow]
pub async fn records_effects(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let custom: i64 = ctx.side_effect("seven", || 7).map_err(err)?;
    let id = ctx.new_uuid().to_string();
    let _roll = ctx.random_u64();
    let version = ctx.version("change", 1, 2);
    let patched = ctx.patched("patch");
    ctx.execute_activity::<_, i64>(&double_info(), custom)
        .await
        .map_err(err)?;
    Ok(json!({
        "custom": custom,
        "id_len": id.len(),
        "version": version,
        "patched": patched,
    }))
}

/// Side effects and markers survive a suspension and replay.
pub fn side_effects_and_markers() -> Scenario {
    Scenario::new(
        "side_effects_and_markers",
        "records_effects",
        || vec![records_effects_info()],
        &[K::RecordSideEffect, K::RecordMarker, K::ScheduleActivity],
    )
    .activities(shared_activities)
}

#[workflow]
pub async fn keeps_books(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.upsert_search_attrs([("stage".to_owned(), Some(json!("start")))])
        .map_err(err)?;
    ctx.set_current_details("working");
    ctx.publish_progress(json!({ "pct": 50 })).map_err(err)?;
    ctx.publish_durable_progress(json!({ "pct": 75 }))
        .map_err(err)?;
    ctx.log_info("halfway");
    let out = ctx.execute_activity(&double_info(), n).await.map_err(err)?;
    Ok(out)
}

/// The bookkeeping commands do not change the outcome or the history.
pub fn bookkeeping() -> Scenario {
    Scenario::new(
        "bookkeeping",
        "keeps_books",
        || vec![keeps_books_info()],
        &[
            K::UpsertSearchAttributes,
            K::SetCurrentDetails,
            K::PublishProgress,
            K::PublishDurableProgress,
            K::RecordLog,
            K::ScheduleActivity,
        ],
    )
    .input(json!(8))
    .activities(shared_activities)
}

#[workflow]
pub async fn locks_once(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let guard = ctx.mutex("k").acquire().await.map_err(err)?;
    let out = ctx.execute_activity(&double_info(), n).await.map_err(err)?;
    guard.release();
    Ok(out)
}

/// A workflow mutex is acquired, held over an activity and released.
pub fn mutex() -> Scenario {
    Scenario::new(
        "mutex",
        "locks_once",
        || vec![locks_once_info()],
        &[K::AcquireMutex, K::ReleaseMutex, K::ScheduleActivity],
    )
    .input(json!(6))
    .activities(shared_activities)
}

// ── Updates ──────────────────────────────────────────────────────────────────

#[update(workflow = "takes_updates")]
pub async fn add_one(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n + 1)
}

#[workflow]
pub async fn takes_updates(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.wait_for_signal("done").await.map_err(err)
}

/// A declarative update runs on a parked run and records its result.
pub fn update() -> Scenario {
    Scenario::new(
        "update",
        "takes_updates",
        || vec![takes_updates_info()],
        &[K::RecordUpdateResult, K::WaitForSignal],
    )
    .updates(|| vec![add_one_info()])
    .step(Step::Update {
        name: "add_one",
        input: json!(41),
    })
    .step(Step::Signals(vec![("done", json!("bye"))]))
}

// ── Composition ──────────────────────────────────────────────────────────────

#[workflow]
pub async fn child_doubles(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.execute_activity(&double_info(), n).await.map_err(err)
}

#[workflow]
pub async fn awaits_child(ctx: &WorkflowContext, n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_raw("child_doubles", json!(n))
        .await
        .map_err(err)
}

/// An awaited child runs an activity. The parent returns its output.
pub fn child_workflow() -> Scenario {
    Scenario::new(
        "child_workflow",
        "awaits_child",
        || vec![awaits_child_info(), child_doubles_info()],
        &[K::StartChildWorkflow, K::ScheduleActivity],
    )
    .input(json!(9))
    .activities(shared_activities)
}

/// Waits for `go`, naps one second, then returns the payload.
#[workflow]
pub async fn naps_after_go(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let go = ctx.wait_for_signal("go").await.map_err(err)?;
    ctx.timer("nap", 1).await.map_err(err)?;
    Ok(go)
}

/// Spawns a detached child, then signals it in a later cycle.
///
/// The timer puts the signal in its own cycle. A signal in the spawn cycle
/// meets a Postgres race, which [`same_cycle_signal_to_new_child`] shows.
#[workflow]
pub async fn signals_detached_child(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let child = ctx
        .spawn_child_workflow_detached_raw("naps_after_go", json!(0), ParentClosePolicy::Abandon)
        .map_err(err)?;
    ctx.timer("settle", 1).await.map_err(err)?;
    ctx.signal_external_workflow(child, "go", json!("from parent"))
        .await
        .map_err(err)?;
    ctx.await_external_workflow_value(child).await.map_err(err)
}

/// A parent spawns a detached child, signals it and awaits its result.
pub fn detached_child_signal_and_await() -> Scenario {
    Scenario::new(
        "detached_child_signal_and_await",
        "signals_detached_child",
        || vec![signals_detached_child_info(), naps_after_go_info()],
        &[
            K::SpawnDetachedChildWorkflow,
            K::SignalExternalWorkflow,
            K::AwaitExternalWorkflow,
            K::StartTimer,
        ],
    )
}

/// Spawns a detached child, then cancels it in a later cycle.
#[workflow]
pub async fn cancels_detached_child(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let child = ctx
        .spawn_child_workflow_detached_raw("waits_for_go", json!(0), ParentClosePolicy::Abandon)
        .map_err(err)?;
    ctx.timer("settle", 1).await.map_err(err)?;
    ctx.request_cancel_external_workflow(child)
        .await
        .map_err(err)?;
    match ctx.await_external_workflow_value(child).await {
        Ok(v) => Ok(format!("child completed: {v}")),
        Err(e) => Ok(format!("child ended: {e}")),
    }
}

/// A parent cancels a detached child and observes its end.
pub fn detached_child_cancel() -> Scenario {
    Scenario::new(
        "detached_child_cancel",
        "cancels_detached_child",
        || vec![cancels_detached_child_info(), waits_for_go_info()],
        &[
            K::SpawnDetachedChildWorkflow,
            K::RequestCancelExternalWorkflow,
            K::AwaitExternalWorkflow,
        ],
    )
}

#[workflow]
pub async fn counts_up(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    if n < 2 {
        ctx.continue_as_new(json!(n + 1)).await.map_err(err)?;
    }
    Ok(n)
}

/// A run continues as new twice. The third run completes.
pub fn continue_as_new() -> Scenario {
    Scenario::new(
        "continue_as_new",
        "counts_up",
        || vec![counts_up_info()],
        &[K::ContinueAsNew],
    )
}

// ── Cancel and terminate from outside ────────────────────────────────────────

/// A cancel ends a run that waits for a signal.
pub fn cancel_parked_run() -> Scenario {
    Scenario::new(
        "cancel_parked_run",
        "waits_for_go",
        || vec![waits_for_go_info()],
        &[],
    )
    .step(Step::AwaitEvent {
        event: "WorkflowStarted",
        count: 1,
    })
    .step(Step::Cancel("operator stop"))
}

/// A terminate ends a run that waits for a signal.
pub fn terminate_parked_run() -> Scenario {
    Scenario::new(
        "terminate_parked_run",
        "waits_for_go",
        || vec![waits_for_go_info()],
        &[],
    )
    .step(Step::AwaitEvent {
        event: "WorkflowStarted",
        count: 1,
    })
    .step(Step::Terminate("operator kill"))
}

// ── Primitives that SQLite rejects ───────────────────────────────────────────

#[workflow]
pub async fn runs_external(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.execute_activity_external("external", json!({}), "default", 60)
        .await
        .map_err(err)
}

/// An external (task-token) activity.
pub fn rejects_external_activity() -> Scenario {
    Scenario::new(
        "rejects_external_activity",
        "runs_external",
        || vec![runs_external_info()],
        &[K::ScheduleExternalActivity],
    )
    .rejected("ScheduleExternalActivity")
}

#[workflow]
pub async fn continues_as_other_type(ctx: &WorkflowContext, n: i64) -> Result<(), String> {
    ctx.continue_as_new_as_type("runs_one_activity", json!(n))
        .await
        .map_err(err)
}

/// A continue-as-new to another workflow type.
pub fn rejects_cross_type_continue_as_new() -> Scenario {
    Scenario::new(
        "rejects_cross_type_continue_as_new",
        "continues_as_other_type",
        || vec![continues_as_other_type_info(), runs_one_activity_info()],
        &[],
    )
    .activities(shared_activities)
    .rejected("ContinueAsNew.new_workflow_type")
}

#[workflow]
pub async fn uses_a_session(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    let session = ctx
        .create_session(SessionOptions::new("gpu"))
        .await
        .map_err(err)?;
    session.complete().await.map_err(err)?;
    Ok(())
}

/// A worker session pins activities to one worker.
pub fn rejects_worker_session() -> Scenario {
    Scenario::new(
        "rejects_worker_session",
        "uses_a_session",
        || vec![uses_a_session_info()],
        &[],
    )
    .rejected("ScheduleActivity.schedule_to_start_override")
}

#[workflow]
pub async fn fans_out_to_store(ctx: &WorkflowContext, n: i64) -> Result<usize, String> {
    let results = ctx
        .execute_activity_fan_out_raw_with(
            vec![("double".to_owned(), json!(n), "default".to_owned())],
            &FanOutOptions::new().with_result_writer(true),
        )
        .await
        .map_err(err)?;
    Ok(results.items().len())
}

/// A fan-out that writes each result through a `PayloadStore`.
///
/// Neither runtime has a store here. The context fails the run with the same
/// configuration error on both, before `SQLite` sees the `result_writer`
/// field.
pub fn fan_out_result_writer_needs_a_store() -> Scenario {
    Scenario::new(
        "fan_out_result_writer_needs_a_store",
        "fans_out_to_store",
        || vec![fans_out_to_store_info()],
        &[],
    )
    .activities(shared_activities)
}


#[workflow]
pub async fn acquires_beside_timer(ctx: &WorkflowContext, _n: i64) -> Result<(), String> {
    let (guard, nap) = tokio::join!(ctx.mutex("k").acquire(), ctx.timer("nap", 1));
    nap.map_err(err)?;
    guard.map_err(err)?.release();
    Ok(())
}

/// A mutex acquire that shares its batch with a timer.
///
/// Postgres accepts it. `SQLite` cannot replay the grant in that position.
pub fn rejects_acquire_beside_timer() -> Scenario {
    Scenario::new(
        "rejects_acquire_beside_timer",
        "acquires_beside_timer",
        || vec![acquires_beside_timer_info()],
        &[],
    )
    .rejected("AcquireMutex")
}

#[workflow]
pub async fn local_beside_timer(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let info = local_double_info();
    let (out, nap) = tokio::join!(
        ctx.execute_local_activity::<_, i64>(&info, n),
        ctx.timer("nap", 1),
    );
    nap.map_err(err)?;
    out.map_err(err)
}

/// A local activity that shares its batch with a timer.
pub fn rejects_local_activity_beside_timer() -> Scenario {
    Scenario::new(
        "rejects_local_activity_beside_timer",
        "local_beside_timer",
        || vec![local_beside_timer_info()],
        &[],
    )
    .activities(shared_activities)
    .rejected("RunLocalActivity")
}

#[workflow]
pub async fn abandons_an_activity(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    // Poll the activity once, so its command joins the last batch.
    let mut pending = std::pin::pin!(ctx.execute_activity_raw("double", json!(n), "default"));
    let _ = std::future::Future::poll(
        pending.as_mut(),
        &mut std::task::Context::from_waker(std::task::Waker::noop()),
    );
    Ok(n)
}

/// A run that returns while an activity it started is not awaited.
pub fn rejects_unawaited_activity_in_last_cycle() -> Scenario {
    Scenario::new(
        "rejects_unawaited_activity_in_last_cycle",
        "abandons_an_activity",
        || vec![abandons_an_activity_info()],
        &[],
    )
    .activities(shared_activities)
    .rejected("ScheduleActivity")
}

// ── Known Postgres defects ───────────────────────────────────────────────────
//
// Each scenario below fails on Postgres in some runs. `SQLite` gives the
// expected result each time. `main.rs` marks each test `#[ignore]` with the
// reason. Run them with `--ignored` to reproduce.

#[workflow]
pub async fn signals_child_in_spawn_cycle(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    let child = ctx
        .spawn_child_workflow_detached_raw("naps_after_go", json!(0), ParentClosePolicy::Abandon)
        .map_err(err)?;
    ctx.signal_external_workflow(child, "go", json!("from parent"))
        .await
        .map_err(err)?;
    ctx.await_external_workflow_value(child).await.map_err(err)
}

/// A signal to a detached child in the cycle that spawns it.
///
/// Postgres can deliver the signal twice. The child then holds two
/// `SignalReceived` events, or the parent fails on a duplicate `event_id`.
pub fn same_cycle_signal_to_new_child() -> Scenario {
    Scenario::new(
        "same_cycle_signal_to_new_child",
        "signals_child_in_spawn_cycle",
        || vec![signals_child_in_spawn_cycle_info(), naps_after_go_info()],
        &[],
    )
}

#[workflow]
pub async fn cancels_child_in_spawn_cycle(ctx: &WorkflowContext, _n: i64) -> Result<String, String> {
    let child = ctx
        .spawn_child_workflow_detached_raw("waits_for_go", json!(0), ParentClosePolicy::Abandon)
        .map_err(err)?;
    ctx.request_cancel_external_workflow(child)
        .await
        .map_err(err)?;
    match ctx.await_external_workflow_value(child).await {
        Ok(v) => Ok(format!("child completed: {v}")),
        Err(e) => Ok(format!("child ended: {e}")),
    }
}

/// A cancel of a detached child in the cycle that spawns it.
///
/// Postgres can append a second `ExternalCancelDelivered` after
/// `WorkflowCompleted`, or fail the parent on a duplicate `event_id`.
pub fn same_cycle_cancel_of_new_child() -> Scenario {
    Scenario::new(
        "same_cycle_cancel_of_new_child",
        "cancels_child_in_spawn_cycle",
        || vec![cancels_child_in_spawn_cycle_info(), waits_for_go_info()],
        &[],
    )
}
