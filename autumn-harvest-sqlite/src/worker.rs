//! The single-writer worker pass.
//!
//! [`drain_ready`] is the embedded analog of the Postgres poll/dispatch loop.
//! It claims each ready activity task, runs its body, and applies the retry
//! policy. It also fires each due timer and appends the terminal
//! [`WorkflowEvent`]s to the canonical history.
//!
//! # Crash model — activity execution is at-least-once (by design)
//!
//! The claim (`BEGIN IMMEDIATE` → `RUNNING`) commits atomically. The finalize
//! in [`finalize_activity_result`] also commits atomically: it records the
//! attempt and the terminal event, and marks the task `DONE` or requeues it.
//! The activity **body runs between them, outside any transaction**. `SQLite`
//! cannot hold its single write lock across arbitrary user I/O.
//! A crash after the body runs but before the finalize commits leaves the task
//! `RUNNING`. The next [`SqliteRuntime::open`](crate::SqliteRuntime::open)
//! calls [`reclaim_orphaned_running`](crate::queue::reclaim_orphaned_running),
//! which flips it back to `PENDING`, and the **body re-runs**. Write activity
//! bodies to be idempotent. A crash *after* the finalize commits sees a `DONE`
//! task and never re-runs the body. The finalize is transactional, so the
//! replayable log never records a half-completed activity.
//!
//! # Retry model
//!
//! A *retryable* activity failure goes to the `harvest_activity_attempts`
//! audit table and the `attempt` counter of the task-queue row. It is **not**
//! appended to the replayable event log. Only the terminal outcome
//! (`ActivityCompleted`, or `ActivityFailed` after the last attempt) reaches
//! `harvest_events`. The Postgres engine does the same:
//! `queue::requeue_for_retry` stores the attempt error on the task row. So each
//! persisted history is a clean, terminal-only, replay-correct log.

use std::collections::HashMap;
use std::task::Poll;

use autumn_harvest::builder::{DEFAULT_MAX_ACTIVITY_RESULT_BYTES, DEFAULT_RETRY_AFTER_CEILING};
use autumn_harvest::context::{ActivityIdentity, SharedState};
use autumn_harvest::llm_ledger::LlmCall;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::{
    ActivityContext, ActivityExecId, ActivityHandlerFn, ExecutionId, TimeoutType, TimerId,
    WorkflowCommand, WorkflowEvent, WorkflowHistoryPolicy,
};
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::error::SqliteResult;
use crate::queue::{self, ClaimedTask};
use crate::runtime::{ActivityRunner, ActivitySpec, LedgerActivityBody};
use crate::store;

/// Run all ready work for `exec_id` at logical time `now`.
///
/// It drains ready activity tasks (bodies and retries) and fires due timers.
///
/// `failure_now` reads the crate clock seam at *finalize* time, when a body
/// returns (#1069). A policy retry's backoff thus anchors to the failure time,
/// not to the cycle-start `now`. With the stale `now`, a slow body would make
/// its retry ready too early, or at once if the body outran the delay. The
/// Postgres path also computes the delay after it handles the result. The loop
/// reads the seam once per activity. In `_as_of` simulation the seam returns
/// the fixed epoch, so `failure_now() == now` and behavior does not change.
///
/// Returns `true` if any terminal event was appended, so the workflow may have
/// made progress and should be re-run.
///
/// # Race settling — drain ONE branch at a time (issue #600, #1069)
///
/// An activity `ctx.race()` schedules all N branches in one suspension batch.
/// A drain-all pass would finish each branch before the workflow re-polls.
/// Then, when `settle_race` picks a winner and emits `CancelRaceLosers`, each
/// loser is already `DONE`. Its body already ran, for example a second
/// provider charge. That breaks the race contract: only the winner runs, and
/// pending losers are cancelled. It also diverges from Postgres. There, each
/// completion wakes the workflow (one NOTIFY, one poll), so the race settles
/// while the siblings are still pending.
///
/// The caller sets `settle_after_first_terminal` when an activity race is in
/// flight (see [`history_has_unsettled_activity_race`] and
/// [`commands_open_activity_race`]). The pass then stops at the first terminal
/// event. The outer `drive_one_cycle` loop re-polls the workflow before the
/// siblings drain. `settle_race` picks that branch as the winner and emits
/// `CancelRaceLosers`. That cancels the `PENDING` losers with a synthetic
/// `ActivityFailed`, and their bodies never run. A retry requeue appends no
/// terminal event, so it does not stop the pass. `false` keeps the drain-all
/// default. Fan-out (wait-all) and single or sequential activities never set
/// the flag, so their behavior does not change.
#[allow(clippy::too_many_arguments)]
pub async fn drain_ready(
    conn: &mut Connection,
    exec_id: ExecutionId,
    owner: &RunOwner<'_>,
    now: i64,
    // `+ Send + Sync` keeps the async `drive_one_cycle` future `Send` (clippy
    // `future_not_send`). The driver closures capture only a `Send + Sync`
    // `NowFn` `Arc` (wall clock) or a `Copy` `i64` (`_as_of`).
    failure_now: &(dyn Fn() -> i64 + Send + Sync),
    activities: &HashMap<String, ActivitySpec>,
    settle_after_first_terminal: bool,
) -> SqliteResult<bool> {
    let mut produced = false;

    // A retry requeues at `run_at = now`, so this loop re-claims it at once.
    // The whole retry sequence thus converges in one drain pass.
    //
    // Exception (issue #600): under an unsettled activity `ctx.race()`, break
    // at the first terminal event (see the fn doc). A retry never breaks.
    loop {
        // Claim the next ready task and freeze its resolved defaults in ONE
        // `BEGIN IMMEDIATE` transaction (issue #1068, #1080). Separate
        // transactions left a committed `RUNNING` row with no frozen defaults.
        // A crash there let `reclaim_orphaned_running` return it to `PENDING`,
        // still unfrozen. A re-registration under a changed spec then froze the
        // NEW defaults, which altered the contract of a claimed task. One
        // transaction makes that state unreachable. Either both commit, or the
        // row stays `PENDING` and unfrozen until its first real claim.
        //
        // The transaction covers only the spec lookup and the freeze, one
        // `UPDATE`. It commits BEFORE the body runs. So the write lock lasts
        // microseconds, and the body stays at-least-once, outside any
        // transaction.
        let (task, spec) = {
            let Some((tx, task)) = queue::claim_next_ready_task_tx(conn, exec_id, now)? else {
                break;
            };
            let mut task = task;
            // With no registered body, drop the transaction to roll back the
            // claim. The row stays `PENDING` and a later registration in the
            // SAME runtime can claim it, with no DB reopen. The error still
            // surfaces from the drive loop, as the non-determinism gate does.
            let Some(spec) = activities.get(&task.name) else {
                drop(tx);
                return Err(crate::error::SqliteError::unregistered(&task.name));
            };
            // Freeze or read the four resolved defaults (issue #1068) in this
            // same transaction. See [`freeze_defaults_at_claim`].
            freeze_defaults_at_claim(&tx, &mut task, spec)?;
            tx.commit()?;
            (task, spec)
        };

        // Enforce the total (cross-retry) schedule-to-close deadline BEFORE
        // the body runs (issue #378, #1069). A task drained after its absolute
        // deadline must not run and then record a result past the cap. The
        // Postgres timeout scanner does the same for a `PENDING` row. Seal it
        // `ActivityTimedOut { ScheduleToClose }` and move on. The retry path
        // in `finalize_within_tx` covers a body that keeps failing past the
        // deadline.
        //
        // Read a fresh clock here, not the cycle-start `now` (#1069). An
        // earlier body in this pass can use real time, so this task's deadline
        // can elapse while it waits. With the stale `now`, the body would run,
        // and the finalize recheck would catch it only after the side effect.
        // In `_as_of` simulation `failure_now() == now`, so nothing changes.
        let claim_now = failure_now();
        if schedule_to_close_exceeded(claim_now, task.schedule_to_close_at) {
            finalize_activity_timeout(conn, exec_id, &task, TimeoutType::ScheduleToClose)?;
            produced = true;
            if settle_after_first_terminal {
                break;
            }
            continue;
        }

        // Honor the declared or per-call retry cap (#1069). The context reports
        // it, so resolve it before the body runs. The task is always frozen
        // here: the freeze-or-read step above sets `defaults_frozen` and
        // `max_attempts`, and a row frozen at schedule time already has them.
        // So a frozen task never falls back to `spec.max_attempts` (#1068). The
        // mutable registry would defeat the freeze if a frozen row held NULL.
        // The spec fallback is a defensive read for the unreachable unfrozen
        // case. Clamp to `>= 1` as `ActivitySpec::new` does: a body runs once.
        debug_assert!(
            task.max_attempts.is_some(),
            "a frozen task must carry a resolved max_attempts (issue #1068 freeze invariant)",
        );
        let max_attempts = if task.defaults_frozen {
            task.max_attempts.unwrap_or(1).max(1)
        } else {
            task.max_attempts.unwrap_or(spec.max_attempts).max(1)
        };

        // The body runs OUTSIDE any transaction, at-least-once (see module docs).
        //
        // Contain a panicking body (issue #782 analog). An unwind would skip the
        // finalize and requeue step and leave the row `RUNNING`. The claim
        // query selects only `PENDING`, and one process has no live-peer
        // poison-pill net, so the run would wedge. A caught panic takes the
        // normal `Err(msg)` path: `finalize_activity_result` records the attempt
        // and follows the retry policy. `AssertUnwindSafe` is sound: the closure
        // only reads `spec.body` and `task.input`, and a caught panic discards
        // its result. The Postgres worker contains handler panics the same way.
        //
        // `Instant` measures the real runtime of the body, not the logical
        // `now`. Start-to-close is a real budget, so an overrun records a
        // timeout below.
        //
        // The LLM calls of the attempt (issue #2275) come from a ledger
        // closure or from the `ActivityContext`. Only a completion writes them.
        let started = std::time::Instant::now();
        let mut llm_calls = Vec::new();
        let result = match &spec.body {
            ActivityRunner::Closure(body) => {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(task.input.clone())))
                    .unwrap_or_else(|payload| {
                        // Encode the panic as the typed `harvest_activity_failure_v1`
                        // envelope with `error_type = "HandlerPanic"` (issue #782,
                        // #1069), not as a plain string. The finalize path reads both
                        // the retry class (`parse_typed_payload`) and the terminal
                        // `ActivityFailed` metadata (`parse_error_payload_full`) from
                        // it. So a policy that marks `HandlerPanic` non-retryable
                        // fails the task after ONE attempt. The terminal event matches
                        // the Postgres caught-panic path byte for byte. Otherwise the
                        // panic follows the retry policy as `Err(String)` does. The
                        // message is the raw panic message, as in the core envelope.
                        Err(handler_panic_activity_envelope(
                            autumn_harvest::error::panic_message(payload),
                        ))
                    })
            }
            ActivityRunner::Ledger(body) => {
                let (result, calls) = run_ledger_body(body, &task.input, claim_now);
                llm_calls = calls;
                result
            }
            ActivityRunner::Handler(handler) => {
                let ctx = activity_context(exec_id, owner, &task, claim_now, max_attempts);
                let (result, calls) = run_handler(conn, *handler, ctx, &task).await;
                llm_calls = calls;
                result
            }
        };
        let elapsed = started.elapsed();
        let llm_calls = crate::llm_ledger::fill_unset_latency(llm_calls, elapsed);

        // Enforce the start-to-close timeout (#1069) when the scheduling command
        // set one (`start_to_close_override`, stored on the task row). A
        // single-writer runtime cannot cancel a synchronous body mid-flight. So
        // an overrun records a TERMINAL `ActivityTimedOut { StartToClose }`
        // after the body, in place of its result. The Postgres timeout scanner
        // (`enforce_activity_timeout`) records the same event and does not
        // requeue. The workflow sees `HistoryMatch::TimedOut` and takes its own
        // timeout branch. The check runs before the normal finalize, so the
        // returned value does not matter. Replay never re-runs the body, so
        // timing that varies across a crash rerun cannot change a committed
        // history.
        if start_to_close_exceeded(elapsed, task.start_to_close_ms) {
            finalize_activity_timeout(conn, exec_id, &task, TimeoutType::StartToClose)?;
            produced = true;
            if settle_after_first_terminal {
                break;
            }
            continue;
        }

        // Read the clock seam after the body returns (#1069). A policy retry's
        // backoff counts from this instant, not from the cycle-start `now`.
        let finalize_now = failure_now();

        // Recheck the total (cross-retry) schedule-to-close deadline at
        // finalize time, for an Ok or an Err result (issue #378, #1069). This
        // decision comes before the branches in `finalize_activity_result`.
        // The pre-body check read the clock at claim time, and the deadline may
        // elapse while the body runs. The Postgres timeout scanner
        // (`find_timed_out_tasks` with `TimeoutReason::ScheduleToClose`) times
        // out a RUNNING row whatever its result. A late result then finds the task no longer
        // RUNNING. This backend has no scanner, so it seals
        // `ActivityTimedOut { ScheduleToClose }` inline. An Ok-only recheck
        // left two holes:
        //   - A non-retryable or final-attempt Err past the deadline became an
        //     ordinary `ActivityFailed`, not a timeout.
        //   - A zero-delay retry whose deadline elapsed mid-body re-ran past the
        //     cap. The `finalize_within_tx` check compares the next `run_at`,
        //     which is the stale cycle `now`, against the deadline.
        // The post-body `finalize_now` catches both. The `finalize_within_tx`
        // check still covers a positive backoff whose `run_at` lands at or after
        // a deadline that has not elapsed yet. The timeout is terminal and not
        // retried. The body already ran, so this only selects the terminal
        // event, as the Postgres scanner does when it wins the race. The
        // `continue` skips `finalize_activity_result`, so nothing records a
        // second terminal event.
        if schedule_to_close_exceeded(finalize_now, task.schedule_to_close_at) {
            finalize_activity_timeout(conn, exec_id, &task, TimeoutType::ScheduleToClose)?;
            produced = true;
            if settle_after_first_terminal {
                break;
            }
            continue;
        }

        if finalize_activity_result(
            conn,
            exec_id,
            &task,
            max_attempts,
            now,
            finalize_now,
            result,
            &llm_calls,
        )? {
            produced = true;
            // Under an unsettled race, stop at a terminal event so the workflow
            // cancels the losers before they drain (issue #600). A retryable
            // failure returns `false` and keeps the pass draining.
            if settle_after_first_terminal {
                break;
            }
        }
    }

    // Each timer append and mark is atomic (see `fire_timer`).
    for timer_id in queue::due_timers(conn, exec_id, now)? {
        fire_timer(conn, exec_id, &timer_id)?;
        produced = true;
    }

    Ok(produced)
}

/// The run that owns a drain (issue #2024). `ActivityContext::info()` reports it.
pub struct RunOwner<'a> {
    /// The run's business `workflow_id`.
    pub workflow_id: &'a str,
    /// The run's workflow type, the `#[workflow]` fn name.
    pub workflow_type: &'a str,
    /// The app state that `ctx.state::<T>()` reads (issue #2467).
    pub state: &'a SharedState,
}

/// The heartbeat channel capacity. The drain empties it on every poll, so a
/// small bound is enough.
const HEARTBEAT_CHANNEL_CAPACITY: usize = 16;

/// Build the `ActivityContext` for one attempt of `task` (issue #2024).
///
/// The values match the Postgres worker for one process. `attempt` is 1-based.
/// `previous_failure` is set on a retry only. The returned receiver gets every
/// heartbeat of the attempt.
fn activity_context(
    exec_id: ExecutionId,
    owner: &RunOwner<'_>,
    task: &ClaimedTask,
    claim_now: i64,
    max_attempts: u32,
) -> (ActivityContext, mpsc::Receiver<Value>) {
    let (tx, rx) = mpsc::channel(HEARTBEAT_CHANNEL_CAPACITY);
    let identity = ActivityIdentity::new(
        exec_id,
        owner.workflow_id,
        owner.workflow_type,
        task.name.clone(),
        task.activity_id,
    );
    let attempt = task.attempt.saturating_add(1);
    let task_id = task.task_id.parse().ok();
    debug_assert!(task_id.is_some(), "a task row id is a UUID");
    let ctx = ActivityContext::new_embedded(
        owner.state.clone(),
        identity,
        tx,
        task.heartbeat_details.clone(),
    )
    .with_queue_name(task.queue.clone())
    .with_task_id(task_id)
    .with_deadline(attempt_deadline(
        claim_now,
        task.start_to_close_ms,
        task.schedule_to_close_at,
    ))
    .with_attempt(attempt)
    .with_max_attempts(max_attempts)
    .with_previous_failure(if attempt > 1 {
        task.last_error.clone()
    } else {
        None
    });
    (ctx, rx)
}

/// The attempt deadline: the earlier of `claim_ms + start_to_close_ms` and
/// `schedule_to_close_at` (issue #2024).
///
/// It is the same rule as the Postgres worker. A non-positive budget is
/// unbounded, as in [`start_to_close_exceeded`]. `claim_ms` can be a simulated
/// clock, but `ActivityContext::time_remaining` reads the real clock. So the
/// result is the remaining span added to `Utc::now()`.
fn attempt_deadline(
    claim_ms: i64,
    start_to_close_ms: Option<i64>,
    schedule_to_close_at: Option<i64>,
) -> Option<DateTime<Utc>> {
    let per_attempt = start_to_close_ms
        .filter(|ms| *ms > 0)
        .map(|ms| claim_ms.saturating_add(ms));
    let at = match (per_attempt, schedule_to_close_at) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(only), None) | (None, Some(only)) => Some(only),
        (None, None) => None,
    }?;
    let remaining = chrono::Duration::try_milliseconds(at.saturating_sub(claim_ms))?;
    Utc::now().checked_add_signed(remaining)
}

/// Await one `#[activity]` handler with its context (issue #2024).
///
/// One `poll_fn` drives the body and the heartbeat channel. Each poll runs the
/// body under `catch_unwind`, then persists the latest heartbeat. So the last
/// heartbeat before a return is on the row before the finalize. A panic, in
/// the handler call or in a poll, becomes the typed `HandlerPanic` envelope,
/// as for a closure body.
///
/// It also returns the LLM calls that the body recorded on its context
/// (issue #2275). The finalize writes them only for a completion.
// The `&mut` is load-bearing: `&mut Connection` is `Send`, `&Connection` is not.
#[allow(clippy::needless_pass_by_ref_mut)]
async fn run_handler(
    conn: &mut Connection,
    handler: ActivityHandlerFn,
    (ctx, mut heartbeats): (ActivityContext, mpsc::Receiver<Value>),
    task: &ClaimedTask,
) -> (Result<Value, String>, Vec<LlmCall>) {
    let panicked = |payload| {
        Err(handler_panic_activity_envelope(
            autumn_harvest::error::panic_message(payload),
        ))
    };
    let call = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        handler(&ctx, task.input.clone())
    }));
    let mut body = match call {
        Ok(body) => body,
        Err(payload) => return (panicked(payload), Vec::new()),
    };
    // `move` takes the `&mut Connection` itself. A shared capture needs
    // `Connection: Sync`, so the drive future would not be `Send`.
    let result = std::future::poll_fn(move |cx| {
        let polled =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body.as_mut().poll(cx)));
        persist_heartbeats(conn, &task.task_id, &mut heartbeats);
        polled.unwrap_or_else(|payload| Poll::Ready(panicked(payload)))
    })
    .await;
    (result, ctx.llm_calls())
}

/// Run a ledger closure (issue #2275). A panic becomes the retryable
/// `HandlerPanic` failure, as for a plain closure. A failure keeps no call.
fn run_ledger_body(
    body: &LedgerActivityBody,
    input: &Value,
    claim_ms: i64,
) -> (Result<Value, String>, Vec<LlmCall>) {
    let at = DateTime::<Utc>::from_timestamp_millis(claim_ms).unwrap_or_else(Utc::now);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(input.clone(), at)))
        .unwrap_or_else(|payload| {
            Err(handler_panic_activity_envelope(
                autumn_harvest::error::panic_message(payload),
            ))
        });
    match result {
        Ok((output, calls)) => (Ok(output), calls),
        Err(error) => (Err(error), Vec::new()),
    }
}

/// Write the latest queued heartbeat of `task_id` to its row (issue #2024).
///
/// Only the last payload of a batch is written, because last write wins. A
/// failed write logs a warning and keeps the attempt alive, as the Postgres
/// flusher does.
///
/// A payload nested past the cap is dropped with a warning (issue #2216). The
/// next claim reads the checkpoint back, and `serde_json` cannot read that
/// payload. So the last good checkpoint stays.
///
/// `try_recv` takes no Tokio coop budget, so the drain always empties the
/// channel. A body that waits for capacity wakes when the drain frees a slot.
fn persist_heartbeats(conn: &Connection, task_id: &str, heartbeats: &mut mpsc::Receiver<Value>) {
    let mut latest = None;
    while let Ok(details) = heartbeats.try_recv() {
        if autumn_harvest::payload_depth::nests_past_cap(&details) {
            tracing::warn!(
                task_id,
                cap_depth = crate::MAX_PAYLOAD_DEPTH,
                "dropped an activity heartbeat that nests past the depth cap"
            );
            continue;
        }
        latest = Some(details);
    }
    if let Some(details) = latest
        && let Err(err) = queue::record_heartbeat(conn, task_id, &details)
    {
        tracing::warn!(task_id, error = %err, "failed to persist an activity heartbeat");
    }
}

/// Freeze or read the four resolved activity defaults on a claimed task row
/// (issue #1068).
///
/// - `task.defaults_frozen == true`: `retry_policy`, `max_attempts`,
///   `start_to_close_ms` and the absolute `schedule_to_close_at` were resolved
///   at schedule time or by a prior claim. They are already loaded from the row
///   (a NULL column reads as `None`). Use them as they are and do NOT consult
///   the mutable registry. That keeps the retry and deadline contract of an
///   in-flight activity immutable. A crash, a reopen or a re-registration
///   under a changed spec cannot alter it. The Postgres engine freezes these on
///   the task-queue row too. Only the activity BODY comes from `spec`.
///
/// - `task.defaults_frozen == false`: the activity was scheduled before it was
///   registered. That late registration is a supported flow: the task waits
///   `PENDING` until the body appears. Resolve the defaults now from `spec`,
///   which is present: the caller releases an unregistered activity first.
///   Persist them and mark the row frozen. A later claim then reads the SAME
///   values, not a re-resolution against a changed registry.
///
///   The resolution mirrors the schedule-time freeze in
///   `apply_schedule_activity` and the pre-#1068 claim-time fallback (#1069).
///   The command override wins. It is stored as `task.retry_policy` and
///   `.start_to_close_ms` on the TYPED path. The RAW path falls back to the
///   spec's full `default_retry_policy` and `default_start_to_close`. The
///   absolute deadline anchors to `scheduled_at`, which a retry requeue never
///   changes. Claim time would extend the budget by the registration wait, and
///   retries move `run_at` forward. For a normally registered activity,
///   `scheduled_at` is the schedule `now`. So the deadline is the pre-#1068
///   value, byte for byte.
///
/// [`drain_ready`] calls this **inside the `BEGIN IMMEDIATE` transaction of the
/// `RUNNING` claim** (issue #1068, #1080). `conn` is that open transaction. So
/// the freeze `UPDATE` and the `RUNNING` flip commit together. Separate commits
/// left a gap: a crash between them left a `RUNNING` unfrozen row. A reopen
/// reclaimed it `PENDING`, and a re-registration could freeze a CHANGED spec.
/// Now either both commit, or the row stays `PENDING` and unfrozen. Then the
/// freeze uses the spec registered at the first successful claim.
fn freeze_defaults_at_claim(
    conn: &Connection,
    task: &mut ClaimedTask,
    spec: &ActivitySpec,
) -> SqliteResult<()> {
    if task.defaults_frozen {
        return Ok(());
    }
    if task.retry_policy.is_none() {
        task.retry_policy.clone_from(&spec.default_retry_policy);
    }
    if task.start_to_close_ms.is_none() {
        task.start_to_close_ms = spec
            .start_to_close
            .map(crate::runtime::duration_to_millis_saturating);
    }
    task.schedule_to_close_at = spec.schedule_to_close.map(|d| {
        task.scheduled_at
            .saturating_add(crate::runtime::duration_to_millis_saturating(d))
    });
    // The command override's cap (stored as `task.max_attempts`) wins over
    // the cap of the registered spec.
    task.max_attempts = Some(task.max_attempts.unwrap_or(spec.max_attempts));

    let retry_json = task
        .retry_policy
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    queue::freeze_task_defaults(
        conn,
        &task.task_id,
        retry_json.as_deref(),
        task.max_attempts,
        task.start_to_close_ms,
        task.schedule_to_close_at,
    )?;
    task.defaults_frozen = true;
    Ok(())
}

/// True iff `history` holds an **unsettled** activity `ctx.race()` (issue #600,
/// #1069).
///
/// An open race marker (`race:{seq}`) is recorded on first dispatch to fix
/// the branch count. `settle_race` records the winner marker
/// (`race_winner:{seq}`). A race is unsettled when it has the first and not
/// the second.
///
/// The core lowers an activity or child race onto these two `MarkerRecorded` events.
/// They come from an inline `format!("race:{seq}")` / `format!("race_winner:{seq}")`
/// in `autumn_harvest::context`. There is no exported constant, so this matches
/// the naming convention, as [`signal_timeout_name`] matches the
/// `__signal_timeout:` timer-id convention. The timer and signal race
/// (`race_timer_signal_impl`) records NO `race:` marker. It has no
/// multi-branch drain to gate, so it is correctly not detected here.
/// `drive_one_cycle` uses this to decide whether [`drain_ready`] settles after
/// the first terminal. The committed open marker stays in history. So this also
/// covers backoff cycles, whose commands carry only `WaitForActivity`.
#[must_use]
pub fn history_has_unsettled_activity_race(history: &[WorkflowEvent]) -> bool {
    let mut open: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut settled: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for event in history {
        if let WorkflowEvent::MarkerRecorded { name, .. } = event {
            // `"race_winner:{seq}"` does not start with `"race:"`, so the two
            // prefixes are disjoint. The winner check comes first for clarity.
            if let Some(seq) = name.strip_prefix("race_winner:") {
                settled.insert(seq);
            } else if let Some(seq) = name.strip_prefix("race:") {
                open.insert(seq);
            }
        }
    }
    open.iter().any(|seq| !settled.contains(seq))
}

/// True iff this cycle's suspension `commands` OPEN an activity `ctx.race()`
/// (issue #600, #1069).
///
/// That is a `RecordMarker` whose name starts with `race:`. On the first
/// dispatch cycle, the open marker is a command and not yet in `history`. On
/// later cycles it is in history (see [`history_has_unsettled_activity_race`]).
/// Checking both covers each cycle of the race. A `race_winner:` marker does
/// not start with `race:`, so this never matches the settle cycle.
#[must_use]
pub fn commands_open_activity_race(commands: &[WorkflowCommand]) -> bool {
    commands.iter().any(|cmd| {
        matches!(cmd, WorkflowCommand::RecordMarker { name, .. } if name.starts_with("race:"))
    })
}

/// True iff an activity `ctx.race()` is in flight on this decision cycle
/// (issue #600, #1069).
///
/// Either `history` has an unsettled open marker
/// ([`history_has_unsettled_activity_race`]), or this cycle's `commands` open
/// one ([`commands_open_activity_race`]) before it reaches history. This gates
/// `settle_after_first_terminal` of [`drain_ready`].
#[must_use]
pub fn activity_race_in_flight(history: &[WorkflowEvent], commands: &[WorkflowCommand]) -> bool {
    history_has_unsettled_activity_race(history) || commands_open_activity_race(commands)
}

/// Encode a contained **activity** handler-panic message as the typed
/// `harvest_activity_failure_v1` envelope carrying the engine-reserved
/// [`ERROR_TYPE_HANDLER_PANIC`](autumn_harvest::failure) class (issue #782).
///
/// Mirrors the private `handler_panic_activity_envelope` of the Postgres
/// worker. It uses the same `pub` `autumn_harvest::failure` building blocks
/// ([`ActivityFailure::retryable`](autumn_harvest::failure::ActivityFailure::retryable)
/// and
/// [`IntoActivityErrorString::into_error_payload`](autumn_harvest::failure::IntoActivityErrorString)).
/// So a caught panic on this backend produces a byte-identical failure payload.
/// The failure is **retryable**. A caught panic follows the retry policy, and
/// its `HandlerPanic` class, as an ordinary `Err(String)` does.
fn handler_panic_activity_envelope(message: String) -> String {
    use autumn_harvest::failure::{ERROR_TYPE_HANDLER_PANIC, IntoActivityErrorString as _};
    autumn_harvest::failure::ActivityFailure::retryable(ERROR_TYPE_HANDLER_PANIC, message)
        .into_error_payload()
}

/// Encode an activity-result cap violation as the typed
/// `harvest_activity_failure_v1` envelope with `error_type = "PayloadTooLarge"`
/// and `non_retryable = true` (issue #252, #1069).
///
/// This mirrors `handle_activity_result` of the Postgres worker. An oversized
/// SUCCESSFUL result is refused, and a non-retryable `ActivityFailed` takes the
/// place of `ActivityCompleted`. No unbounded result row is stored. The
/// envelope takes the same terminal path as the `Err` arm of
/// [`finalize_within_tx`]. So `parse_error_payload_full` derives the typed
/// metadata, as Postgres `finalize_activity_failure` does, and the histories
/// match byte for byte. The `pub` `autumn_harvest::failure` primitives build
/// it, so no core export is needed.
fn payload_too_large_activity_envelope(activity_name: &str, observed: u64, cap: u64) -> String {
    use autumn_harvest::failure::IntoActivityErrorString as _;
    autumn_harvest::failure::ActivityFailure::non_retryable(
        "PayloadTooLarge",
        format!(
            "activity '{activity_name}' result exceeds cap: {observed} bytes (cap {cap} bytes)"
        ),
    )
    .into_error_payload()
}

/// The signal name of a `wait_for_signal_timeout` / `receive_signal_timeout`
/// race deadline timer, or `None` if `timer_id` is not one.
///
/// The core arms that deadline timer with the id `__signal_timeout:{seq}:{name}`
/// (an inline `format!` in `autumn_harvest::context`, issue #476). There is no
/// exported constant, so this matches the prefix convention. Only THAT timer
/// may be recorded ahead of the signal in [`SignalIngest::plan`]. An unrelated
/// durable timer armed in the same batch must not.
///
/// It strips the prefix and splits on the colon after the seq, not with a
/// `LIKE` pattern. So a signal name that contains `:`, `%` or `_` still
/// matches exactly.
fn signal_timeout_name(timer_id: &str) -> Option<&str> {
    timer_id
        .strip_prefix("__signal_timeout:")
        .and_then(|rest| rest.split_once(':'))
        .map(|(_seq, name)| name)
}

/// The row that one planned ingest event consumes.
enum IngestRow {
    /// A `harvest_timers` row, by timer id.
    Timer(String),
    /// A `harvest_signals` row, by `signal_seq`.
    Signal(i64),
}

/// Every staged signal of a run, planned as events for one decision cycle
/// (issue #2020).
///
/// The plan reads storage but writes nothing. Replay sees [`Self::events`] in
/// memory. The transaction that persists the cycle outcome then calls
/// [`Self::apply`] first. A discarded cycle thus leaves every signal staged.
///
/// This mirrors the Postgres task-preparation ingest. Every signal reaches
/// history before the workflow runs, so `drain_signals`, `try_receive_signal`
/// and push handlers see it.
#[must_use]
pub struct SignalIngest {
    planned: Vec<(WorkflowEvent, IngestRow)>,
    /// The stored JSON bytes of the planned events.
    bytes: u64,
}

/// The room that the history caps leave for one plan (issue #2023).
///
/// `None` is unlimited. The plan checks the room before each event and stops
/// once the room is full. It then holds at most one event past the room,
/// which makes the cap check fail the run. So a signal flood never loads past
/// the caps into memory.
#[derive(Debug, Clone, Copy, Default)]
pub struct IngestBudget {
    events: Option<u64>,
    bytes: Option<u64>,
}

impl IngestBudget {
    /// The room left under `policy` by a history of `count` events and
    /// `bytes` stored bytes.
    #[must_use]
    pub fn remaining(policy: WorkflowHistoryPolicy, count: u64, bytes: u64) -> Self {
        Self {
            events: policy.event_hard_cap().map(|cap| cap.saturating_sub(count)),
            bytes: policy.byte_hard_cap().map(|cap| cap.saturating_sub(bytes)),
        }
    }

    /// True once `events` planned events and `bytes` planned bytes fill it.
    fn is_full(self, events: usize, bytes: u64) -> bool {
        let events = u64::try_from(events).unwrap_or(u64::MAX);
        self.events.is_some_and(|room| events >= room)
            || self.bytes.is_some_and(|room| bytes >= room)
    }
}

impl SignalIngest {
    /// Plan every undelivered signal of `exec_id`, in arrival order.
    ///
    /// Before each signal, the plan fires each due deadline timer of that
    /// signal name whose `fire_at` is at or before the signal arrival (issue
    /// #476). So a late signal cannot win a signal-or-deadline race, also when
    /// several races share the name. Each deadline fires at most once.
    ///
    /// An unrelated due timer stays for [`drain_ready`]. A `TimerFired` ahead
    /// of a `SignalReceived` that it does not race wedges replay (issue #1071).
    ///
    /// The plan stops when it fills `budget`. Later signals stay staged.
    pub fn plan(
        conn: &Connection,
        exec_id: ExecutionId,
        now: i64,
        budget: IngestBudget,
    ) -> SqliteResult<Self> {
        // One read of the due deadlines, grouped by signal name in `fire_at`
        // order. Each name keeps a cursor, so the plan is O(signals + timers).
        let mut deadlines: HashMap<String, std::collections::VecDeque<(String, i64)>> =
            HashMap::new();
        for (timer_id, fire_at) in queue::due_timers_with_deadlines(conn, exec_id, now)? {
            if let Some(name) = signal_timeout_name(&timer_id) {
                deadlines
                    .entry(name.to_string())
                    .or_default()
                    .push_back((timer_id, fire_at));
            }
        }
        let mut plan = Self {
            planned: Vec::new(),
            bytes: 0,
        };
        store::for_each_pending_signal(conn, exec_id, |signal| {
            // The budget holds per event. A deadline fired without its signal
            // is still in order, because it came due before the signal arrived.
            if let Some(due) = deadlines.get_mut(&signal.name) {
                while due
                    .front()
                    .is_some_and(|(_, fire_at)| *fire_at <= signal.received_at)
                {
                    if budget.is_full(plan.planned.len(), plan.bytes) {
                        return Ok(false);
                    }
                    if let Some((timer_id, _)) = due.pop_front() {
                        let event = WorkflowEvent::TimerFired {
                            timer_id: TimerId::new(timer_id.clone()),
                        };
                        plan.push(event, IngestRow::Timer(timer_id))?;
                    }
                }
            }
            if budget.is_full(plan.planned.len(), plan.bytes) {
                return Ok(false);
            }
            let event = WorkflowEvent::SignalReceived {
                signal_name: signal.name,
                payload: signal.payload,
            };
            plan.push(event, IngestRow::Signal(signal.seq))?;
            Ok(true)
        })?;
        Ok(plan)
    }

    /// Add one event and count its stored bytes.
    fn push(&mut self, event: WorkflowEvent, row: IngestRow) -> SqliteResult<()> {
        let bytes = serde_json::to_string(&event)?.len() as u64;
        self.bytes = self.bytes.saturating_add(bytes);
        self.planned.push((event, row));
        Ok(())
    }

    /// The stored JSON bytes of the planned events. The byte cap counts them
    /// as stored history, as Postgres does after its own ingest.
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The planned events, in history order.
    pub fn events(&self) -> impl Iterator<Item = &WorkflowEvent> {
        self.planned.iter().map(|(event, _)| event)
    }

    /// Append the planned events and consume their rows.
    ///
    /// Call it inside the cycle transaction, before any other append. Returns
    /// `true` if it appended an event.
    pub fn apply(&self, conn: &Connection, exec_id: ExecutionId) -> SqliteResult<bool> {
        for (event, row) in &self.planned {
            store::append_event(conn, exec_id, event)?;
            match row {
                IngestRow::Timer(timer_id) => queue::mark_timer_fired(conn, exec_id, timer_id)?,
                IngestRow::Signal(seq) => store::mark_signal_delivered(conn, *seq)?,
            }
        }
        Ok(!self.planned.is_empty())
    }
}

/// Finalize one activity attempt in a **single transaction** (AC7).
///
/// The audit record, the terminal event (on success or the last retry) and the
/// task-row move (`DONE` or requeue) commit together or not at all.
///
/// Returns `true` if a terminal event was appended, so the run may progress.
/// A crash before the commit leaves the task `RUNNING` for orphan reclaim. A
/// crash after it leaves a clean `DONE` row and a terminal-only history.
///
/// A completion also writes `llm_calls` to the cost ledger in the same
/// transaction (issue #2275). Any other outcome drops them.
#[allow(clippy::too_many_arguments)]
pub fn finalize_activity_result(
    conn: &mut Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    max_attempts: u32,
    now: i64,
    failure_now: i64,
    result: Result<serde_json::Value, String>,
    llm_calls: &[LlmCall],
) -> SqliteResult<bool> {
    let tx = conn.transaction()?;
    let produced = finalize_within_tx(
        &tx,
        exec_id,
        task,
        max_attempts,
        now,
        failure_now,
        result,
        llm_calls,
    )?;
    tx.commit()?;
    Ok(produced)
}

/// The transactional body of [`finalize_activity_result`].
///
/// A caller can run it inside its own transaction. The finalize atomicity tests
/// call it and then *drop the transaction without a commit*. That proves the
/// three writes roll back together.
#[allow(clippy::too_many_arguments)]
pub fn finalize_within_tx(
    conn: &Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    max_attempts: u32,
    now: i64,
    failure_now: i64,
    result: Result<serde_json::Value, String>,
    llm_calls: &[LlmCall],
) -> SqliteResult<bool> {
    // Enforce the activity-result cap (issue #252, #1069), as
    // `handle_activity_result` of the Postgres worker does. An oversized
    // SUCCESSFUL result is NOT stored as `ActivityCompleted`. That would bloat
    // history and show a success that other backends refuse. It becomes a
    // non-retryable `PayloadTooLarge` envelope on the terminal-failure path
    // below, so it fails on the first attempt. This runs BEFORE
    // `record_attempt`, so the audit row, the terminal decision and the event
    // all show the same outcome. The core context enforces the INPUT cap at
    // schedule time. The executor leaves the result cap to the worker
    // (`with_payload_caps(.., 0 /*result*/, ..)`). This backend has no
    // per-activity result-cap override, so the global
    // `DEFAULT_MAX_ACTIVITY_RESULT_BYTES` (2 MiB) applies. The core resolves
    // each activity cap against the same constant. A zero cap means no cap.
    //
    // A result nested past the depth cap fails the same way (issue #2216).
    // Check the depth first, because the size check serializes the whole value.
    let result = match autumn_harvest::payload_depth::refuse_deep_result(&task.name, result) {
        Ok(output) if DEFAULT_MAX_ACTIVITY_RESULT_BYTES > 0 => {
            let observed = serde_json::to_string(&output).map_or(0, |s| s.len() as u64);
            if observed > DEFAULT_MAX_ACTIVITY_RESULT_BYTES {
                Err(payload_too_large_activity_envelope(
                    &task.name,
                    observed,
                    DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
                ))
            } else {
                Ok(output)
            }
        }
        other => other,
    };

    let attempt_num = task.attempt + 1;
    store::record_attempt(conn, exec_id, &task.name, attempt_num, &result)?;

    match result {
        Ok(output) => {
            let seq = store::append_event_seq(
                conn,
                exec_id,
                &WorkflowEvent::ActivityCompleted {
                    activity_id: task.activity_id,
                    output,
                },
            )?;
            // The ledger rows commit with the completion event, as on
            // Postgres (issue #2275). The key is the `seq` of that event.
            crate::llm_ledger::insert_rows(conn, exec_id, seq, &task.name, llm_calls, failure_now)?;
            queue::finish_task(conn, &task.task_id)?;
            Ok(true)
        }
        Err(error) => {
            // Classify the failure as the core worker does
            // (`failure::classify_activity_error`, issue #227). A typed payload
            // with `non_retryable: true`, or a match on the policy's
            // `non_retryable_errors`, fails at once with no more retries. Else
            // this worker would retry such an error up to `max_attempts` and
            // could repeat side effects (#1069). `parse_typed_payload` returns
            // `Some` only for the typed envelope. So a plain `Err(String)`
            // never gets an `"Error"` class that a policy could match (#227).
            let typed = autumn_harvest::failure::parse_typed_payload(&error);
            let payload_non_retryable = typed.as_ref().is_some_and(|f| f.non_retryable);
            let typed_error_type = typed.as_ref().map(|f| f.error_type.as_str());
            let policy_non_retryable = task
                .retry_policy
                .as_ref()
                .is_some_and(|p| p.is_non_retryable(typed_error_type, &error));
            let non_retryable = payload_non_retryable || policy_non_retryable;

            if attempt_num < max_attempts && !non_retryable {
                // Retryable: bump the attempt counter and requeue with the
                // backoff of the whole stored retry policy, through
                // `policy::compute_retry_delay` (#1069). `attempt_num` is 1-based,
                // as that helper expects, so the first retry waits
                // `initial_interval`. The replayable log does not record the
                // retry (see module docs).
                //
                // The backoff counts from `failure_now`, when the body failed,
                // not from the cycle-start `now` (#1069). Postgres also computes
                // the delay after it handles the result. The cycle `now` made a
                // retry ready too early, or at once after a long body. A delayed
                // `run_at` is in the future, so `claim_next_ready_task_tx`
                // (`run_at <= now`) skips it until the driver advances the clock.
                // The workflow blocks on the backoff (see `classify_block`) and
                // does not busy-retry.
                //
                // A ZERO delay is an immediate retry and MUST requeue at the
                // cycle-start `now`. Then this drain pass re-claims it, and the
                // retry sequence converges in one call. That covers no policy and
                // a zero-delay policy such as `RetryPolicy::fixed(n, 0ms)`. Under
                // the wall clock `failure_now >= now`, so it would push `run_at`
                // past `now`. Then `classify_block` would report `WaitingTimer`,
                // and `run_until_blocked` would return early. So `failure_now`
                // anchors only a positive backoff.
                let run_at = task.retry_policy.as_ref().map_or(now, |p| {
                    let delay = autumn_harvest::policy::compute_retry_delay(
                        p.initial_interval,
                        p.backoff_coefficient,
                        p.max_interval,
                        attempt_num,
                    );
                    let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
                    if delay_ms == 0 {
                        now
                    } else {
                        failure_now.saturating_add(delay_ms)
                    }
                });
                // Enforce the total (cross-retry) schedule-to-close deadline
                // before a requeue (issue #378, #1069). A retrying activity with
                // a total deadline must not back off past it and then complete.
                // If the next attempt lands at or after the deadline, seal it
                // `ActivityTimedOut { ScheduleToClose }`. The Postgres
                // `record_schedule_to_close_activity_timeout` path does the same
                // once `now + retry_delay >= deadline`. The failed attempt is
                // already recorded. The workflow sees `HistoryMatch::TimedOut`.
                if schedule_to_close_exceeded(run_at, task.schedule_to_close_at) {
                    store::append_event(
                        conn,
                        exec_id,
                        &WorkflowEvent::ActivityTimedOut {
                            activity_id: task.activity_id,
                            timeout_type: TimeoutType::ScheduleToClose,
                        },
                    )?;
                    queue::finish_task(conn, &task.task_id)?;
                    return Ok(true);
                }
                // The requeue order depends on the kind of retry (issue #1068):
                //
                // - IMMEDIATE (`run_at <= now`, zero delay or no policy): this
                //   pass could re-claim the task. Move it to the BACK of the
                //   ready queue (a new highest `seq`). Then
                //   `claim_next_ready_task_tx` (`ORDER BY seq`) serves each ready
                //   sibling first. Else a low-`seq` race branch that keeps
                //   failing re-selects itself on each retry. It can exhaust its
                //   attempts and settle the race before a faster sibling runs.
                //   That biases the winner to the lowest index. The loop stays
                //   bounded, because each branch has a finite `max_attempts`.
                //   Outside a race there are no siblings, so the move is harmless.
                // - BACKOFF (`run_at > now`): the future `run_at` already keeps
                //   the task out of this pass, so its `seq` does not change.
                // Store the message the way Postgres does: a typed payload
                // keeps only its message (issue #2024).
                let previous = autumn_harvest::failure::parse_error_payload_full(&error).message;
                if run_at <= now {
                    queue::requeue_task_to_back(
                        conn,
                        &task.task_id,
                        attempt_num,
                        run_at,
                        &previous,
                    )?;
                } else {
                    queue::requeue_task(conn, &task.task_id, attempt_num, run_at, &previous)?;
                }
                Ok(false)
            } else {
                // Terminal (attempts exhausted or non-retryable): the failure
                // goes to the event log. Decode the envelope with
                // `parse_error_payload_full` (#1069). Then `ActivityFailed` has
                // the same typed metadata as Postgres `finalize_activity_failure`:
                // `error_type`, `details`, `non_retryable` and the human
                // `message`, never the raw envelope. Typed-failure histories thus
                // match across backends (AC5). A legacy `Err(String)` decodes to
                // `error_type = "Error"` and `message = error`, so its stored
                // form does not change.
                let failure = autumn_harvest::failure::parse_error_payload_full(&error);
                store::append_event(
                    conn,
                    exec_id,
                    &WorkflowEvent::ActivityFailed {
                        activity_id: task.activity_id,
                        error: failure.message,
                        attempt: attempt_num,
                        error_type: failure.error_type,
                        non_retryable: failure.non_retryable,
                        details: failure.details,
                    },
                )?;
                queue::finish_task(conn, &task.task_id)?;
                Ok(true)
            }
        }
    }
}

/// True iff a body's real wall-clock `elapsed` exceeded its start-to-close
/// `budget_ms` (#1069).
///
/// A `None` budget means no start-to-close timeout. A non-positive budget is
/// also unbounded. The core never emits one, and a `0` budget would time out
/// every body. The comparison is strict (`>`), so a body that ends exactly at
/// its budget completes normally.
#[must_use]
pub fn start_to_close_exceeded(elapsed: std::time::Duration, budget_ms: Option<i64>) -> bool {
    match budget_ms {
        Some(ms) if ms > 0 => elapsed.as_millis() > u128::try_from(ms).unwrap_or(u128::MAX),
        _ => false,
    }
}

/// True iff the epoch-millisecond instant `at_ms` has reached the absolute
/// total (cross-retry) `schedule_to_close` deadline (issue #378, #1069).
///
/// A `None` deadline means no total cap. A non-positive deadline also means no
/// cap. The core never emits one, and a real epoch-ms deadline is far above
/// zero.
///
/// The comparison is `>=`, as in the Postgres
/// `schedule_to_close_deadline_exceeded` contract (`now + retry_delay >= deadline`):
/// - The **pre-run** check in [`drain_ready`] passes the claim-time clock. A
///   task drained at or after its deadline times out before its body runs. The
///   Postgres scanner catches a `PENDING` row past its deadline the same way.
/// - The **retry** check in [`finalize_within_tx`] passes the `run_at` of the
///   next attempt. A retry that would land at or after the deadline is sealed,
///   not requeued. So an activity cannot keep backing off past its deadline.
#[must_use]
pub const fn schedule_to_close_exceeded(at_ms: i64, deadline_ms: Option<i64>) -> bool {
    match deadline_ms {
        Some(deadline) if deadline > 0 => at_ms >= deadline,
        _ => false,
    }
}

/// Finalize an activity that exceeded a timeout in a **single transaction**
/// (AC7).
///
/// It records the timed-out attempt in the audit log, appends the terminal
/// `ActivityTimedOut` event and marks the task `DONE`, all or nothing.
///
/// **Terminal — no requeue.** The Postgres `enforce_activity_timeout` also
/// appends `ActivityTimedOut` and calls `fail_task` without a requeue. The
/// timeout is workflow-visible (`HistoryMatch::TimedOut`), not an attempt-level
/// retry. The workflow drives its own timeout branch.
pub fn finalize_activity_timeout(
    conn: &mut Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    timeout_type: TimeoutType,
) -> SqliteResult<()> {
    let tx = conn.transaction()?;
    finalize_timeout_within_tx(&tx, exec_id, task, timeout_type)?;
    tx.commit()?;
    Ok(())
}

/// The transactional body of [`finalize_activity_timeout`].
///
/// A caller can run it inside its own transaction to assert that the audit
/// attempt, the `ActivityTimedOut` event and the `DONE` move roll back together.
pub fn finalize_timeout_within_tx(
    conn: &Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    timeout_type: TimeoutType,
) -> SqliteResult<()> {
    let attempt_num = task.attempt + 1;
    // A timed-out attempt is an attempt outcome, so the audit log records it,
    // as in `finalize_within_tx`. The message names the timeout kind, so a
    // `ScheduleToClose` attempt (issue #378) is not labelled `StartToClose`.
    let timeout_reason = format!("activity exceeded its {timeout_type} timeout");
    store::record_attempt(conn, exec_id, &task.name, attempt_num, &Err(timeout_reason))?;
    store::append_event(
        conn,
        exec_id,
        &WorkflowEvent::ActivityTimedOut {
            activity_id: task.activity_id,
            timeout_type,
        },
    )?;
    queue::finish_task(conn, &task.task_id)?;
    Ok(())
}

/// The per-attempt budget cap of a local activity (issue #2029). It matches
/// the default `max_local_activity_start_to_close` of the Postgres worker.
pub const LOCAL_ACTIVITY_START_TO_CLOSE_CAP: std::time::Duration =
    std::time::Duration::from_secs(60);

/// One `RunLocalActivity` command that the drive runs after the cycle commits
/// (issue #2029).
pub struct LocalRun {
    /// The id of the `LocalActivityScheduled` event.
    pub activity_id: ActivityExecId,
    /// The registered activity name.
    pub name: String,
    /// The activity input.
    pub input: Value,
    /// The per-attempt budget of the call. The cap above bounds it.
    pub start_to_close: Option<std::time::Duration>,
    /// The retry policy of the call. `None` = one attempt.
    pub retry_policy: Option<RetryPolicy>,
    /// The failed attempts that history already records.
    pub failed_attempts: u32,
    /// The error of the last recorded failed attempt.
    pub last_error: Option<String>,
}

impl LocalRun {
    /// The attempt cap: the policy's `max_attempts`, or 1.
    pub fn max_attempts(&self) -> u32 {
        self.retry_policy
            .as_ref()
            .map_or(1, |p| p.max_attempts.max(1))
    }

    /// The per-attempt budget: the declared one, capped at
    /// [`LOCAL_ACTIVITY_START_TO_CLOSE_CAP`].
    fn budget(&self) -> std::time::Duration {
        self.start_to_close
            .map_or(LOCAL_ACTIVITY_START_TO_CLOSE_CAP, |d| {
                d.min(LOCAL_ACTIVITY_START_TO_CLOSE_CAP)
            })
    }
}

/// Run attempt `attempt` of a local activity, inline (issue #2029).
///
/// A closure body gets no context. An `#[activity]` body gets the local
/// context that the Postgres worker builds. A panic becomes the retryable
/// `HandlerPanic` failure. An `#[activity]` body that runs past its budget is
/// dropped under a Tokio runtime. A closure body cannot be stopped, so its
/// budget is checked after it returns. As on Postgres, the result has no
/// size cap here. The history byte cap still bounds it.
///
/// It also returns the LLM calls of the attempt (issue #2275). A failure
/// returns none. `now_ms` is the clock of the runtime, which a ledger body
/// gets (issue #2272).
#[allow(clippy::too_many_arguments)]
pub async fn run_local_attempt(
    exec_id: ExecutionId,
    owner: &RunOwner<'_>,
    spec: &ActivitySpec,
    run: &LocalRun,
    attempt: u32,
    previous_failure: Option<String>,
    deadline_left: Option<std::time::Duration>,
    now_ms: i64,
) -> (Result<Value, String>, Vec<LlmCall>) {
    // The workflow deadline bounds the attempt too (issue #2029).
    let budget = deadline_left.map_or_else(|| run.budget(), |left| run.budget().min(left));
    let started = std::time::Instant::now();
    let mut llm_calls = Vec::new();
    let result = match &spec.body {
        ActivityRunner::Closure(body) => {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(run.input.clone())))
                .unwrap_or_else(|payload| {
                    Err(handler_panic_activity_envelope(
                        autumn_harvest::error::panic_message(payload),
                    ))
                })
        }
        ActivityRunner::Ledger(body) => {
            let (result, calls) = run_ledger_body(body, &run.input, now_ms);
            llm_calls = calls;
            result
        }
        ActivityRunner::Handler(handler) => {
            let identity = ActivityIdentity::new(
                exec_id,
                owner.workflow_id,
                owner.workflow_type,
                run.name.clone(),
                run.activity_id,
            );
            let deadline = chrono::Duration::from_std(budget)
                .ok()
                .and_then(|span| Utc::now().checked_add_signed(span));
            let ctx = ActivityContext::new_embedded_local(owner.state.clone(), identity)
                .with_attempt(attempt)
                .with_max_attempts(run.max_attempts())
                .with_deadline(deadline)
                .with_previous_failure(previous_failure);
            let body = run_local_handler(*handler, &ctx, run.input.clone());
            let Some(result) = within_budget(body, budget).await else {
                return (Err(local_timeout_error(run, budget)), Vec::new());
            };
            llm_calls = ctx.llm_calls();
            result
        }
    };
    let elapsed = started.elapsed();
    if elapsed > budget {
        return (Err(local_timeout_error(run, budget)), Vec::new());
    }
    if result.is_err() {
        llm_calls.clear();
    }
    (
        result,
        crate::llm_ledger::fill_unset_latency(llm_calls, elapsed),
    )
}

/// Append the events of one local attempt (issue #2275).
///
/// A `LocalActivityCompleted` event also writes `llm_calls` to the cost
/// ledger, keyed by its `seq`. Call it in the transaction that holds the
/// attempt, so the rows commit with the event.
///
/// # Errors
///
/// Returns a persistence error if an append or an insert fails.
pub fn append_local_events(
    conn: &Connection,
    exec_id: ExecutionId,
    events: &[WorkflowEvent],
    activity_name: &str,
    llm_calls: &[LlmCall],
    recorded_at_ms: i64,
) -> SqliteResult<()> {
    for event in events {
        let seq = store::append_event_seq(conn, exec_id, event)?;
        if matches!(event, WorkflowEvent::LocalActivityCompleted { .. }) {
            crate::llm_ledger::insert_rows(
                conn,
                exec_id,
                seq,
                activity_name,
                llm_calls,
                recorded_at_ms,
            )?;
        }
    }
    Ok(())
}

/// The Postgres error text of a local attempt that ran past `budget`.
fn local_timeout_error(run: &LocalRun, budget: std::time::Duration) -> String {
    format!("local activity '{}' timed out after {budget:?}", run.name)
}

/// Await `body` for at most `budget`. Returns `None` at the budget, and the
/// body is dropped, as on Postgres (issue #2029).
///
/// The timer is Tokio's. The drive may run on another runtime, or on a Tokio
/// runtime with no timer. The body then runs to its end, and the caller
/// checks the budget after it returns.
pub async fn within_budget<F>(body: F, budget: std::time::Duration) -> Option<F::Output>
where
    F: std::future::Future,
{
    let sleep = tokio::runtime::Handle::try_current()
        .ok()
        .and_then(|_| std::panic::catch_unwind(|| tokio::time::sleep(budget)).ok());
    let Some(sleep) = sleep else {
        return Some(body.await);
    };
    let mut body = std::pin::pin!(body);
    let mut sleep = std::pin::pin!(sleep);
    std::future::poll_fn(|cx| {
        if let Poll::Ready(output) = body.as_mut().poll(cx) {
            return Poll::Ready(Some(output));
        }
        sleep.as_mut().poll(cx).map(|()| None)
    })
    .await
}

/// Await one local `#[activity]` handler, and contain a panic in the call
/// or in a poll (issue #2029).
async fn run_local_handler(
    handler: ActivityHandlerFn,
    ctx: &ActivityContext,
    input: Value,
) -> Result<Value, String> {
    let panicked = |payload| {
        Err(handler_panic_activity_envelope(
            autumn_harvest::error::panic_message(payload),
        ))
    };
    let call = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(ctx, input)));
    let mut body = match call {
        Ok(body) => body,
        Err(payload) => return panicked(payload),
    };
    std::future::poll_fn(move |cx| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body.as_mut().poll(cx)))
            .unwrap_or_else(|payload| Poll::Ready(panicked(payload)))
    })
    .await
}

/// The events of one local attempt, and the backoff before the next one
/// (issue #2029). `None` means the activity is done.
///
/// A failure appends `LocalActivityFailed`. A terminal failure also appends
/// `LocalActivityExhausted`, the marker that ends the activity on replay.
/// The stored error is the typed message, or the raw string, as on Postgres.
pub fn local_attempt_events(
    exec_id: ExecutionId,
    run: &LocalRun,
    attempt: u32,
    result: Result<Value, String>,
) -> (Vec<WorkflowEvent>, Option<std::time::Duration>) {
    let activity_id = run.activity_id;
    let error = match autumn_harvest::payload_depth::refuse_deep_result(&run.name, result) {
        Ok(output) => {
            return (
                vec![WorkflowEvent::LocalActivityCompleted {
                    activity_id,
                    output,
                }],
                None,
            );
        }
        Err(error) => error,
    };
    let typed = autumn_harvest::failure::parse_typed_payload(&error);
    let non_retryable = typed.as_ref().is_some_and(|f| f.non_retryable)
        || run.retry_policy.as_ref().is_some_and(|p| {
            p.is_non_retryable(typed.as_ref().map(|f| f.error_type.as_str()), &error)
        });
    // A positive `retry_after` hint wins over the policy, within the Postgres
    // ceiling.
    let hint = autumn_harvest::policy::resolve_retry_after_hint(
        typed.as_ref().and_then(|f| f.retry_after),
        DEFAULT_RETRY_AFTER_CEILING,
    );
    let message = typed.map_or(error, |f| f.message);
    let failed = WorkflowEvent::LocalActivityFailed {
        activity_id,
        error: message.clone(),
        attempt,
    };
    if non_retryable || attempt >= run.max_attempts() {
        let exhausted = WorkflowEvent::LocalActivityExhausted {
            activity_id,
            error: message,
            attempt,
        };
        return (vec![failed, exhausted], None);
    }
    let seed = local_retry_seed(exec_id, activity_id);
    let delay = hint
        .or_else(|| {
            run.retry_policy
                .as_ref()
                .and_then(|p| p.next_delay_with_seed(attempt, seed))
        })
        .unwrap_or_default();
    (vec![failed], Some(delay))
}

/// The jitter seed of a local retry. It differs per activity of one run.
///
/// It is the FNV-1a mix of the Postgres worker, so both backends pick the
/// same backoff.
fn local_retry_seed(exec_id: ExecutionId, activity_id: ActivityExecId) -> u64 {
    const PRIME: u64 = 0x1000_0000_01b3;
    let mut seed = 0xcbf2_9ce4_8422_2325_u64;
    for id in [exec_id.as_uuid().as_u128(), activity_id.as_uuid().as_u128()] {
        let raw = id.to_le_bytes();
        for half in [&raw[..8], &raw[8..]] {
            seed ^= u64::from_le_bytes(half.try_into().unwrap_or([0; 8]));
            seed = seed.wrapping_mul(PRIME);
        }
    }
    seed
}

/// Fire one due timer atomically.
///
/// It appends `TimerFired` and marks the timer row `fired` in one transaction.
/// A crash between the two would re-fire the timer on restart and leave a
/// stray non-lifecycle `TimerFired` in history.
pub fn fire_timer(conn: &mut Connection, exec_id: ExecutionId, timer_id: &str) -> SqliteResult<()> {
    let tx = conn.transaction()?;
    fire_timer_within_tx(&tx, exec_id, timer_id)?;
    tx.commit()?;
    Ok(())
}

/// The transactional body of [`fire_timer`].
///
/// The timer-fire atomicity test calls it, then *drops the transaction*. That
/// proves both writes roll back together, so a crash cannot double-fire.
pub fn fire_timer_within_tx(
    conn: &Connection,
    exec_id: ExecutionId,
    timer_id: &str,
) -> SqliteResult<()> {
    // An external activity deadline writes its timeout, not `TimerFired`.
    if timer_id.starts_with(crate::external_task::DEADLINE_TIMER_PREFIX) {
        return crate::external_task::fire_deadline(conn, exec_id, timer_id);
    }
    store::append_event(
        conn,
        exec_id,
        &WorkflowEvent::TimerFired {
            timer_id: TimerId::new(timer_id.to_string()),
        },
    )?;
    queue::mark_timer_fired(conn, exec_id, timer_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use autumn_harvest::ActivityExecId;
    use autumn_harvest::policy::RetryPolicy;
    use rusqlite::Connection;

    use autumn_harvest::ExecutionId;

    use std::time::Duration;

    use super::{
        IngestBudget, LocalRun, SignalIngest, attempt_deadline, commands_open_activity_race,
        drain_ready, finalize_within_tx, fire_timer, fire_timer_within_tx,
        handler_panic_activity_envelope, history_has_unsettled_activity_race, local_attempt_events,
        payload_too_large_activity_envelope, schedule_to_close_exceeded, signal_timeout_name,
        start_to_close_exceeded,
    };
    use crate::queue::{self, ClaimedTask};
    use crate::runtime::ActivitySpec;
    use crate::{schema, store};

    /// A `PENDING`, claimable activity task with `scheduled_at == run_at == 0`,
    /// the raw seed for [`drain_ready`] tests. [`seed_running_task`] instead
    /// flips the row to `RUNNING` and builds a `ClaimedTask` by hand.
    fn seed_pending_activity(conn: &Connection, exec: ExecutionId, name: &str) {
        // The task is UNFROZEN. `drain_ready` claims it, freezes the defaults
        // (with `schedule_to_close_at`) from the registered spec, runs the body
        // and finalizes. So a finalize-time recheck runs the real path end to end.
        queue::enqueue_activity(
            conn,
            exec,
            ActivityExecId::new(),
            name,
            &serde_json::json!({}),
            "default",
            0,
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();
    }

    /// An `ActivitySpec` with a total (cross-retry) `schedule_to_close` of
    /// `deadline_ms`. It sets the `pub(crate)` field that `register_activity`
    /// fills from the info, because [`ActivitySpec::new`] leaves it `None`.
    fn spec_with_schedule_to_close(
        deadline_ms: u64,
        body: impl Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync + 'static,
    ) -> ActivitySpec {
        let mut spec = ActivitySpec::new(1, body);
        spec.schedule_to_close = Some(Duration::from_millis(deadline_ms));
        spec
    }

    /// A clock seam that returns `before` on the FIRST read and `after` on
    /// each later read.
    ///
    /// The pre-run `schedule_to_close` check also reads the clock (#1069). So
    /// one constant past the deadline would seal the activity before its body
    /// runs, which the pre-run test covers. These backstop tests model a
    /// deadline that elapses WHILE the body runs. The pre-run read sees
    /// `before`, below the deadline, so the body runs. The finalize recheck
    /// sees `after`, at or past the deadline, and seals it.
    fn advancing_finalize_clock(before: i64, after: i64) -> impl Fn() -> i64 + Send + Sync {
        let reads = std::sync::atomic::AtomicUsize::new(0);
        move || {
            if reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                before
            } else {
                after
            }
        }
    }

    /// The requeue `run_at` (epoch-ms) of one task after one retryable failure.
    /// The finalize gets a distinct cycle-start `now`, a `failure_now` and the
    /// retry `policy`. Asserts that the failure was a retry, not a terminal.
    fn requeued_run_at(policy: Option<RetryPolicy>, now: i64, failure_now: i64) -> i64 {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let mut task = seed_running_task(&conn, exec, "act");
        task.retry_policy = policy;
        let tx = conn.transaction().unwrap();
        let produced = finalize_within_tx(
            &tx,
            exec,
            &task,
            3,
            now,
            failure_now,
            Err("boom".into()),
            &[],
        )
        .unwrap();
        tx.commit().unwrap();
        assert!(!produced, "a retryable failure requeues (not terminal)");
        conn.query_row(
            "SELECT run_at FROM harvest_tasks WHERE task_id = ?1",
            [&task.task_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn open() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();
        conn
    }

    /// The state the drain tests pass. It is empty.
    static TEST_STATE: std::sync::LazyLock<autumn_harvest::context::SharedState> =
        std::sync::LazyLock::new(autumn_harvest::context::empty_shared_state);

    /// The owner the drain tests pass. Their closure bodies never read it.
    fn test_owner() -> super::RunOwner<'static> {
        super::RunOwner {
            workflow_id: "wf-test",
            workflow_type: "test_workflow",
            state: &TEST_STATE,
        }
    }

    fn seed_running_task(conn: &Connection, exec: ExecutionId, name: &str) -> ClaimedTask {
        let act = ActivityExecId::new();
        queue::enqueue_activity(
            conn,
            exec,
            act,
            name,
            &serde_json::json!({}),
            "default",
            0,
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();
        conn.execute("UPDATE harvest_tasks SET state = 'RUNNING'", [])
            .unwrap();
        let task_id: String = conn
            .query_row("SELECT task_id FROM harvest_tasks", [], |r| r.get(0))
            .unwrap();
        // These tests call the finalize directly and skip the claim SELECT.
        // The freeze-or-read step lives in `drain_ready`, so `defaults_frozen`
        // does not matter here and stays `false`.
        ClaimedTask {
            task_id,
            activity_id: act,
            name: name.to_string(),
            input: serde_json::json!({}),
            attempt: 0,
            max_attempts: None,
            retry_policy: None,
            start_to_close_ms: None,
            scheduled_at: 0,
            schedule_to_close_at: None,
            defaults_frozen: false,
            queue: "default".to_string(),
            heartbeat_details: None,
            last_error: None,
        }
    }

    // AC7: the terminal finalize is atomic. A rollback keeps NEITHER the
    // ActivityCompleted event nor the DONE move, so the task stays RUNNING
    // and orphan reclaim can re-run it.
    #[test]
    fn finalize_rolls_back_terminal_event_and_task_transition_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        {
            let tx = conn.transaction().unwrap();
            finalize_within_tx(
                &tx,
                exec,
                &task,
                1,
                0,
                0,
                Ok(serde_json::json!("done")),
                &[],
            )
            .unwrap();
            // Drop `tx` WITHOUT commit → rollback.
        }

        assert!(
            store::load_history(&conn, exec).unwrap().is_empty(),
            "no ActivityCompleted must survive a rolled-back finalize"
        );
        assert_eq!(
            queue::task_state(&conn, &task.task_id).unwrap().as_deref(),
            Some("RUNNING"),
            "task must stay RUNNING after a rolled-back finalize"
        );
    }

    // AC7: a commit keeps both, a terminal-only history and a DONE task row.
    #[test]
    fn finalize_commits_terminal_event_and_done_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        {
            let tx = conn.transaction().unwrap();
            finalize_within_tx(
                &tx,
                exec,
                &task,
                1,
                0,
                0,
                Ok(serde_json::json!("done")),
                &[],
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1, "exactly one terminal event");
        assert!(matches!(
            history[0],
            autumn_harvest::WorkflowEvent::ActivityCompleted { .. }
        ));
        assert_eq!(
            queue::task_state(&conn, &task.task_id).unwrap().as_deref(),
            Some("DONE")
        );
    }

    // A retryable failure requeues with no terminal event and records the
    // attempt, in the same finalize transaction.
    #[test]
    fn finalize_retryable_failure_requeues_without_terminal_event() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        let produced = super::finalize_activity_result(
            &mut conn,
            exec,
            &task,
            3,
            0,
            0,
            Err("boom".into()),
            &[],
        )
        .unwrap();

        assert!(!produced, "a retry is not workflow-visible progress");
        assert!(
            store::load_history(&conn, exec).unwrap().is_empty(),
            "a retryable failure must NOT reach the replayable log"
        );
        assert_eq!(
            queue::task_state(&conn, &task.task_id).unwrap().as_deref(),
            Some("PENDING"),
            "a retryable failure requeues the task"
        );
        assert_eq!(store::load_attempts(&conn, exec, "act").unwrap().len(), 1);
    }

    fn two_calls() -> Vec<autumn_harvest::llm_ledger::LlmCall> {
        vec![
            autumn_harvest::llm_ledger::LlmCall::new("m-1", 10, 2).with_cost_usd_micros(30),
            autumn_harvest::llm_ledger::LlmCall::new("m-1", 12, 4),
        ]
    }

    fn seeded(conn: &Connection) -> (ExecutionId, ClaimedTask) {
        let exec = ExecutionId::new();
        store::insert_execution(
            conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(conn, exec, "think");
        (exec, task)
    }

    // Issue #2275: the ledger rows commit with the completion event, keyed by
    // its `seq`. A rolled-back finalize leaves no row, so a re-run counts once.
    #[test]
    fn a_completion_writes_its_ledger_rows_in_the_finalize_transaction() {
        let mut conn = open();
        let (exec, task) = seeded(&conn);
        {
            let tx = conn.transaction().unwrap();
            let done = Ok(serde_json::json!("done"));
            finalize_within_tx(&tx, exec, &task, 1, 0, 7, done, &two_calls()).unwrap();
        }
        assert_eq!(
            crate::llm_ledger::rows_of(&conn, exec).unwrap(),
            Vec::new(),
            "a rolled-back finalize writes no ledger row"
        );

        let done = Ok(serde_json::json!("done"));
        assert!(
            super::finalize_activity_result(&mut conn, exec, &task, 1, 0, 7, done, &two_calls())
                .unwrap()
        );
        let rows = crate::llm_ledger::rows_of(&conn, exec).unwrap();
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!((rows[0].event_seq, rows[0].call_index), (0, 0));
        assert_eq!((rows[1].event_seq, rows[1].call_index), (0, 1));
        assert_eq!(rows[0].activity_name, "think");
        assert_eq!(rows[0].cost_usd_micros, Some(30));
        assert_eq!(rows[1].input_tokens, 12);
        assert_eq!(rows[0].recorded_at.timestamp_millis(), 7);
        assert!(matches!(
            store::load_history(&conn, exec).unwrap()[..],
            [autumn_harvest::WorkflowEvent::ActivityCompleted { .. }]
        ));
    }

    // Issue #2275: only a completion writes rows, as on Postgres. A retryable
    // failure, a terminal failure and an oversized result write none.
    #[test]
    fn a_failed_attempt_writes_no_ledger_row() {
        let mut conn = open();
        let (exec, task) = seeded(&conn);
        let retried = super::finalize_activity_result(
            &mut conn,
            exec,
            &task,
            3,
            0,
            0,
            Err("boom".into()),
            &two_calls(),
        )
        .unwrap();
        assert!(!retried, "the first failure retries");
        let terminal = super::finalize_activity_result(
            &mut conn,
            exec,
            &task,
            1,
            0,
            0,
            Err("boom".into()),
            &two_calls(),
        )
        .unwrap();
        assert!(terminal, "the last failure ends the activity");
        assert_eq!(crate::llm_ledger::rows_of(&conn, exec).unwrap(), Vec::new());

        // A fresh file: the seed claims the first task row of the file.
        let mut conn = open();
        let (exec, task) = seeded(&conn);
        let cap =
            usize::try_from(autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES).unwrap();
        let oversized = Ok(serde_json::json!("x".repeat(cap + 1)));
        super::finalize_activity_result(&mut conn, exec, &task, 1, 0, 0, oversized, &two_calls())
            .unwrap();
        assert!(matches!(
            store::load_history(&conn, exec).unwrap()[..],
            [autumn_harvest::WorkflowEvent::ActivityFailed { .. }]
        ));
        assert_eq!(crate::llm_ledger::rows_of(&conn, exec).unwrap(), Vec::new());
    }

    // A due timer fire is atomic on commit: the TimerFired event and the `fired`
    // flag land together.
    #[test]
    fn timer_fire_commits_event_and_flag_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        queue::enqueue_timer(&conn, exec, "t1", 0).unwrap();

        fire_timer(&mut conn, exec, "t1").unwrap();

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1);
        assert!(matches!(
            history[0],
            autumn_harvest::WorkflowEvent::TimerFired { .. }
        ));
        assert!(
            !queue::has_unfired_timer(&conn, exec).unwrap(),
            "the fired timer must be marked"
        );
    }

    // Timer-fire atomicity: a crash between the TimerFired append and the
    // `fired` flag rolls BOTH back. The timer stays unfired, so it cannot
    // double-fire, and no stray non-lifecycle TimerFired event survives.
    #[test]
    fn timer_fire_rolls_back_event_and_flag_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        queue::enqueue_timer(&conn, exec, "t1", 0).unwrap();

        {
            let tx = conn.transaction().unwrap();
            fire_timer_within_tx(&tx, exec, "t1").unwrap();
            // Drop `tx` WITHOUT commit → rollback (a simulated crash).
        }

        assert!(
            store::load_history(&conn, exec).unwrap().is_empty(),
            "no stray TimerFired event may survive a rolled-back fire"
        );
        assert!(
            queue::has_unfired_timer(&conn, exec).unwrap(),
            "the timer must remain unfired (no double-fire)"
        );
    }

    // The start-to-close budget predicate (#1069). A `None` or non-positive
    // budget is unbounded. Only a body that STRICTLY exceeds a positive budget
    // is over, so a body that ends exactly at its budget completes normally.
    #[test]
    fn start_to_close_exceeded_truth_table() {
        // No budget → never exceeded.
        assert!(!start_to_close_exceeded(Duration::from_secs(10), None));
        assert!(!start_to_close_exceeded(Duration::from_millis(5), Some(10)));
        // Exactly at budget → NOT exceeded (strict `>`).
        assert!(!start_to_close_exceeded(
            Duration::from_millis(10),
            Some(10)
        ));
        assert!(start_to_close_exceeded(Duration::from_millis(11), Some(10)));
        assert!(start_to_close_exceeded(Duration::from_secs(40), Some(1)));
        // A non-positive budget is unbounded. The core never emits `<= 0`.
        assert!(!start_to_close_exceeded(Duration::from_secs(10), Some(0)));
        assert!(!start_to_close_exceeded(Duration::from_secs(10), Some(-5)));
    }

    // Issue #2024: the deadline is the earlier clock, a non-positive budget is
    // unbounded, and the span is anchored on the real clock.
    #[test]
    fn attempt_deadline_takes_the_earlier_clock_on_the_real_clock() {
        // The deadline lies `want` seconds after the real clock, give or take
        // the time the test itself takes.
        let near = |at: Option<chrono::DateTime<chrono::Utc>>, want: i64| {
            let secs = (at.expect("a bounded attempt") - chrono::Utc::now()).num_seconds();
            assert!((want - 2..=want).contains(&secs), "{secs}s, want {want}s");
        };
        // A simulated claim at epoch 0 still gives a deadline about 60s away.
        near(attempt_deadline(0, Some(60_000), None), 60);
        near(attempt_deadline(0, Some(60_000), Some(30_000)), 30);
        near(attempt_deadline(0, Some(60_000), Some(90_000)), 60);
        near(attempt_deadline(0, None, Some(90_000)), 90);
        assert_eq!(attempt_deadline(0, Some(0), None), None);
        assert_eq!(attempt_deadline(0, Some(-5), None), None);
        assert_eq!(attempt_deadline(0, None, None), None);
    }

    // The total (cross-retry) schedule-to-close deadline predicate (issue #378,
    // #1069). A `None` or non-positive deadline means no total cap. The
    // comparison is `>=`, as in the Postgres `schedule_to_close_deadline_exceeded`
    // contract. Both the pre-run check and the retry check use it.
    #[test]
    fn schedule_to_close_exceeded_truth_table() {
        // No total deadline → never exceeded.
        assert!(!schedule_to_close_exceeded(1_000, None));
        assert!(!schedule_to_close_exceeded(999, Some(1_000)));
        // Exactly AT the deadline → exceeded (`>=`, matching Postgres
        // `now + retry_delay >= deadline`).
        assert!(schedule_to_close_exceeded(1_000, Some(1_000)));
        // Past the deadline (e.g. a retry whose next attempt lands 30s out).
        assert!(schedule_to_close_exceeded(31_000, Some(1_000)));
        // A non-positive deadline means no cap. A real epoch-ms deadline is far
        // above zero.
        assert!(!schedule_to_close_exceeded(1_000, Some(0)));
        assert!(!schedule_to_close_exceeded(1_000, Some(-5)));
    }

    // The parser reads the signal name only from a
    // `__signal_timeout:{seq}:{name}` race timer (#1069). A name can contain a colon.
    // An unrelated durable timer has no signal name.
    #[test]
    fn signal_timeout_name_matches_only_a_race_deadline_timer() {
        assert_eq!(signal_timeout_name("__signal_timeout:0:go"), Some("go"));
        assert_eq!(
            signal_timeout_name("__signal_timeout:7:approval"),
            Some("approval")
        );
        // A signal name that contains ':' still parses (`split_once`, not a
        // suffix or `LIKE` match).
        assert_eq!(
            signal_timeout_name("__signal_timeout:3:ns:go"),
            Some("ns:go")
        );
        // A deadline timer for a DIFFERENT name never matches THIS signal.
        assert_ne!(signal_timeout_name("__signal_timeout:0:other"), Some("go"));
        // An UNRELATED durable timer (`join!(wait_for_signal("go"), timer("t", 5))`)
        // is never a race deadline timer.
        assert_eq!(signal_timeout_name("t"), None);
        assert_eq!(signal_timeout_name("heartbeat"), None);
        // A look-alike of the prefix does not match.
        assert_eq!(signal_timeout_name("__signal_timeout_go"), None);
    }

    // The signal ingest moves ONLY the raced `__signal_timeout:{seq}:{name}`
    // deadline timer ahead of the signal (#1069). An UNRELATED due timer, such
    // as a `join!`ed `ctx.timer("t", 5)`, stays unfired here. A `TimerFired(t)`
    // before `SignalReceived(go)` would wedge the core `match_signal` replay,
    // which does not skip an interleaved `TimerFired`. The ordinary
    // `drain_ready` pass fires `t` AFTER the signal.
    //
    // The test seeds a raced deadline timer and an unrelated timer, both due
    // by the signal arrival. It asserts that only the deadline timer fires
    // ahead of the signal. Firing each due timer would also record
    // `TimerFired(t)` before the signal and fail this test.
    #[test]
    fn ingest_reorders_only_the_raced_deadline_timer_ahead_of_the_signal() {
        let conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();

        store::stage_signal(&conn, exec, "go", &serde_json::json!("v"), 100, None).unwrap();
        // The raced deadline timer of THIS wait, due before the signal.
        queue::enqueue_timer(&conn, exec, "__signal_timeout:0:go", 60).unwrap();
        // An UNRELATED durable timer, also due before the signal. It must NOT
        // fire ahead of the signal.
        queue::enqueue_timer(&conn, exec, "t", 50).unwrap();

        // Commit so the assertions read the persisted state.
        let tx = conn.unchecked_transaction().unwrap();
        let produced = SignalIngest::plan(&tx, exec, 200, IngestBudget::default())
            .unwrap()
            .apply(&tx, exec)
            .unwrap();
        tx.commit().unwrap();
        assert!(
            produced,
            "a signal was consumed (and/or the deadline timer fired)"
        );

        let history = store::load_history(&conn, exec).unwrap();
        let fired: Vec<&str> = history
            .iter()
            .filter_map(|e| match e {
                autumn_harvest::WorkflowEvent::TimerFired { timer_id } => Some(timer_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            fired,
            vec!["__signal_timeout:0:go"],
            "ONLY the raced deadline timer may fire ahead of the signal; the \
             unrelated timer `t` must NOT (pre-fix this also fired `t`):\n{history:?}"
        );

        // The deadline TimerFired comes BEFORE the SignalReceived, so the timeout
        // wins against a late signal. The signal is still recorded.
        let deadline_pos = history
            .iter()
            .position(|e| matches!(e, autumn_harvest::WorkflowEvent::TimerFired { .. }))
            .expect("the deadline timer fired");
        let signal_pos = history
            .iter()
            .position(|e| matches!(e, autumn_harvest::WorkflowEvent::SignalReceived { .. }))
            .expect("the signal was consumed");
        assert!(
            deadline_pos < signal_pos,
            "the raced deadline TimerFired must precede the SignalReceived:\n{history:?}"
        );

        // The unrelated timer `t` is still armed. The ordinary drain pass fires
        // it, so the workflow's own timer await consumes it AFTER the signal.
        let t_fired: i64 = conn
            .query_row(
                "SELECT fired FROM harvest_timers WHERE exec_id = ?1 AND timer_id = 't'",
                rusqlite::params![exec.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            t_fired, 0,
            "the unrelated timer `t` must remain unfired after ingest"
        );
    }

    // Issue #2020: a deadline goes ahead of the first signal that arrives after
    // it, and fires only once. Signals before it stay ahead of it.
    #[test]
    fn ingest_fires_each_deadline_once_in_arrival_order() {
        let conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        store::stage_signal(&conn, exec, "go", &serde_json::json!("on_time"), 10, None).unwrap();
        store::stage_signal(&conn, exec, "go", &serde_json::json!("late"), 90, None).unwrap();
        store::stage_signal(&conn, exec, "go", &serde_json::json!("later"), 100, None).unwrap();
        queue::enqueue_timer(&conn, exec, "__signal_timeout:0:go", 60).unwrap();

        let ingest = SignalIngest::plan(&conn, exec, 120, IngestBudget::default()).unwrap();
        let order: Vec<String> = ingest
            .events()
            .map(|e| match e {
                autumn_harvest::WorkflowEvent::SignalReceived { payload, .. } => {
                    payload.as_str().unwrap_or_default().to_string()
                }
                autumn_harvest::WorkflowEvent::TimerFired { .. } => "deadline".to_string(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(order, ["on_time", "deadline", "late", "later"]);
    }

    // Issue #2020: a plan stops when it fills its budget. Later rows stay staged.
    #[test]
    fn a_plan_stops_at_its_budget() {
        let conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        for i in 0..5 {
            store::stage_signal(&conn, exec, "go", &serde_json::json!(i), i, None).unwrap();
        }
        let policy = autumn_harvest::WorkflowHistoryPolicy::default().with_event_hard_cap(3);

        let ingest =
            SignalIngest::plan(&conn, exec, 10, IngestBudget::remaining(policy, 1, 0)).unwrap();
        assert_eq!(ingest.events().count(), 2, "the room under the cap");
        assert!(ingest.bytes() > 0);
    }

    // Issue #2020: the budget holds per event, also inside one signal's group
    // of due deadlines.
    #[test]
    fn a_plan_checks_its_budget_before_each_deadline_event() {
        let conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        for seq in 0..3 {
            queue::enqueue_timer(&conn, exec, &format!("__signal_timeout:{seq}:go"), 10).unwrap();
        }
        store::stage_signal(&conn, exec, "go", &serde_json::json!(1), 50, None).unwrap();
        let policy = autumn_harvest::WorkflowHistoryPolicy::default().with_event_hard_cap(3);

        let ingest =
            SignalIngest::plan(&conn, exec, 100, IngestBudget::remaining(policy, 1, 0)).unwrap();
        assert_eq!(ingest.events().count(), 2, "the room under the cap");
    }

    // Issue #2020: a plan writes nothing. A discarded cycle leaves every signal
    // staged and every timer armed.
    #[test]
    fn a_signal_ingest_plan_writes_nothing() {
        let conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        store::stage_signal(&conn, exec, "go", &serde_json::json!(1), 100, None).unwrap();
        queue::enqueue_timer(&conn, exec, "__signal_timeout:0:go", 60).unwrap();

        let ingest = SignalIngest::plan(&conn, exec, 200, IngestBudget::default()).unwrap();
        assert_eq!(ingest.events().count(), 2, "a deadline and a signal");
        drop(ingest);

        assert!(store::load_history(&conn, exec).unwrap().is_empty());
        let mut staged = 0;
        store::for_each_pending_signal(&conn, exec, |_| {
            staged += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(staged, 1);
        assert!(queue::has_unfired_timer(&conn, exec).unwrap());
    }

    // A POLICY retry's backoff anchors to the failure time (`failure_now`), not
    // to the cycle-start `now` (#1069). A body that used real time before it
    // failed must schedule its next attempt `delay` after the failure. Postgres
    // also computes the delay after it handles the result.
    //
    // The bug: `run_at = now + delay` used the cycle-start `now`. So the retry
    // was ready `delay` after cycle start, however long the body ran.
    #[test]
    fn retry_backoff_anchors_to_failure_time_not_cycle_start() {
        // The cycle starts at 1s, and the body fails at 46s. `fixed(60s)` arms
        // the retry at 106s, not at 61s.
        let run_at = requeued_run_at(
            Some(RetryPolicy::fixed(3, Duration::from_secs(60))),
            1_000,
            46_000,
        );
        assert_eq!(
            run_at,
            46_000 + 60_000,
            "backoff must anchor to failure_now (46s) + 60s, not cycle-start (1s) + 60s"
        );
        assert_ne!(
            run_at,
            1_000 + 60_000,
            "must NOT anchor to the cycle-start now"
        );

        // A body that ran LONGER than the delay must NOT be ready at once.
        // `run_at = failure_now + 10s` = 56s, after the failure at 46s. The bug
        // gave 1s + 10s = 11s, ready the moment the body failed. Postgres never
        // does that.
        let run_at = requeued_run_at(
            Some(RetryPolicy::fixed(3, Duration::from_secs(10))),
            1_000,
            46_000,
        );
        assert_eq!(run_at, 46_000 + 10_000);
        assert!(
            run_at > 46_000,
            "a retry whose delay is shorter than the body runtime must still be in the \
             future relative to the failure instant, not immediately ready"
        );
    }

    // The no-policy path still requeues at the cycle-start `now` and ignores
    // `failure_now`. So `claim_next_ready_task_tx` re-claims it in the SAME
    // drain pass, and the retry sequence converges in one cycle. Under the wall
    // clock `failure_now >= now`, so it would push `run_at` past `now`. Then
    // `run_until_blocked` would return `WaitingTimer` early.
    #[test]
    fn raw_path_retry_requeues_at_cycle_now_ignoring_failure_now() {
        let run_at = requeued_run_at(None, 1_000, 99_999);
        assert_eq!(
            run_at, 1_000,
            "raw/no-policy path requeues at cycle-start now, never at failure_now"
        );
    }

    // A ZERO-delay policy (`fixed(n, 0ms)`) is an immediate retry. It must
    // requeue at the cycle-start `now`, not at `failure_now`. Under the wall
    // clock `failure_now > now`, so the retry would miss the drain pass. Then
    // `run_until_blocked` would return `WaitingTimer` and not converge in one
    // call. The wall-clock test
    // `declared_retry_policy_raises_attempts_over_registered_spec` catches that
    // end to end.
    #[test]
    fn zero_delay_policy_retry_requeues_at_cycle_now_not_failure_now() {
        let run_at = requeued_run_at(
            Some(RetryPolicy::fixed(4, Duration::from_millis(0))),
            1_000,
            99_999,
        );
        assert_eq!(
            run_at, 1_000,
            "a zero-delay policy retry is immediate: requeue at cycle-start now so it \
             re-claims this drain pass, never at failure_now"
        );
    }

    // A caught activity panic is encoded as the TYPED
    // `harvest_activity_failure_v1` envelope with `error_type = "HandlerPanic"`,
    // not as a plain string (#1069). The finalize path then treats a panic as
    // the Postgres worker does. A policy that marks `HandlerPanic`
    // non-retryable matches, and the terminal `ActivityFailed` records
    // `error_type = "HandlerPanic"`. Mirrors the core test
    // `handler_panic_activity_envelope_is_retryable_handler_panic`.
    #[test]
    fn handler_panic_activity_envelope_encodes_retryable_handler_panic() {
        let payload = handler_panic_activity_envelope("activity boom".to_string());
        // The retry classifier needs a typed envelope, so that
        // `RetryPolicy::is_non_retryable` can read the `error_type`.
        assert!(
            !payload.starts_with("activity boom"),
            "must be a versioned envelope, not the bare panic message"
        );
        let typed = autumn_harvest::failure::parse_typed_payload(&payload)
            .expect("a caught panic encodes a TYPED envelope, not a plain string");
        assert_eq!(
            typed.error_type,
            autumn_harvest::failure::ERROR_TYPE_HANDLER_PANIC,
        );
        assert!(
            !typed.non_retryable,
            "a caught panic is retryable (follows the activity's retry policy)"
        );
        // The terminal decode keeps the HandlerPanic class and the human message.
        let full = autumn_harvest::failure::parse_error_payload_full(&payload);
        assert_eq!(
            full.error_type,
            autumn_harvest::failure::ERROR_TYPE_HANDLER_PANIC,
        );
        assert_eq!(full.message, "activity boom", "raw panic message preserved");
        assert!(!full.non_retryable);
    }

    // Issue #252, #1069: a result-cap violation is encoded as the TYPED
    // `harvest_activity_failure_v1` envelope with `error_type =
    // "PayloadTooLarge"` and `non_retryable = true`. So the finalize path makes
    // it terminal and records the same typed metadata as the Postgres worker.
    #[test]
    fn payload_too_large_activity_envelope_encodes_non_retryable() {
        let payload = payload_too_large_activity_envelope("act", 4_000, 2_000);
        let typed = autumn_harvest::failure::parse_typed_payload(&payload)
            .expect("a cap violation encodes a TYPED envelope, not a plain string");
        assert_eq!(typed.error_type, "PayloadTooLarge");
        assert!(
            typed.non_retryable,
            "a result-cap violation is non-retryable (terminal on first attempt)"
        );
        let full = autumn_harvest::failure::parse_error_payload_full(&payload);
        assert_eq!(full.error_type, "PayloadTooLarge");
        assert!(full.non_retryable);
        assert!(
            full.message.contains("result exceeds cap"),
            "human message names the boundary: {}",
            full.message
        );
    }

    // Issue #252: an oversized SUCCESSFUL result becomes a non-retryable
    // `PayloadTooLarge` terminal `ActivityFailed`. It is NOT stored as an
    // unbounded `ActivityCompleted`, and NOT retried despite `max_attempts = 3`.
    // Mirrors `handle_activity_result` of the Postgres worker. The bug:
    // `finalize_within_tx` appended `ActivityCompleted` with the full payload.
    #[test]
    fn finalize_oversized_result_records_non_retryable_payload_too_large_terminal() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        // The SERIALIZED length of this JSON string exceeds the 2 MiB cap.
        let cap = usize::try_from(autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES)
            .expect("cap fits usize");
        let oversized = serde_json::json!("x".repeat(cap + 1));

        // `max_attempts = 3`, yet the non-retryable failure ends the activity
        // on the FIRST attempt.
        let produced =
            super::finalize_activity_result(&mut conn, exec, &task, 3, 0, 0, Ok(oversized), &[])
                .unwrap();
        assert!(
            produced,
            "an oversized result is a terminal event, not a retry"
        );

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1, "exactly one terminal event");
        match &history[0] {
            autumn_harvest::WorkflowEvent::ActivityFailed {
                error_type,
                non_retryable,
                error,
                ..
            } => {
                assert_eq!(error_type, "PayloadTooLarge");
                assert!(*non_retryable, "must be non-retryable");
                assert!(
                    error.contains("result exceeds cap"),
                    "human message names the boundary: {error}"
                );
                assert!(
                    !error.contains("harvest_activity_failure_v1"),
                    "the raw wire envelope must not leak into the event message"
                );
            }
            other => panic!("expected a PayloadTooLarge ActivityFailed, got {other:?}"),
        }
        assert_eq!(
            queue::task_state(&conn, &task.task_id).unwrap().as_deref(),
            Some("DONE"),
            "the task is terminal (not requeued for retry)"
        );
    }

    // Issue #252: a result UNDER the cap still completes normally.
    #[test]
    fn finalize_under_cap_result_completes_normally() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        let produced = super::finalize_activity_result(
            &mut conn,
            exec,
            &task,
            3,
            0,
            0,
            Ok(serde_json::json!({"ok": true})),
            &[],
        )
        .unwrap();
        assert!(produced);

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1);
        assert!(
            matches!(
                history[0],
                autumn_harvest::WorkflowEvent::ActivityCompleted { .. }
            ),
            "an under-cap result completes normally"
        );
    }

    /// The failure message of a deep result of the activity `act`.
    const DEEP_ACT_MESSAGE: &str = "activity 'act' result nests more than 100 levels";

    /// A value of `depth` nested one-element arrays.
    fn nested(depth: usize) -> serde_json::Value {
        let mut value = serde_json::json!(1);
        for _ in 0..depth {
            value = serde_json::Value::Array(vec![value]);
        }
        value
    }

    // Issue #2216: `serde_json` cannot read back a result past its read limit.
    // So a result nested past the cap becomes a non-retryable `PayloadTooDeep`
    // failure, as an oversized result does. The audit row holds the failure too.
    #[test]
    fn finalize_deep_result_records_non_retryable_payload_too_deep_terminal() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");
        let deep = nested(crate::MAX_PAYLOAD_DEPTH + 1);

        let produced =
            super::finalize_activity_result(&mut conn, exec, &task, 3, 0, 0, Ok(deep), &[])
                .unwrap();
        assert!(produced, "a deep result is a terminal event, not a retry");

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1, "exactly one terminal event");
        match &history[0] {
            autumn_harvest::WorkflowEvent::ActivityFailed {
                error_type,
                non_retryable,
                error,
                ..
            } => {
                assert_eq!(error_type, crate::PAYLOAD_TOO_DEEP_ERROR_TYPE);
                assert!(*non_retryable, "must be non-retryable");
                assert_eq!(error, DEEP_ACT_MESSAGE);
            }
            other => panic!("expected a PayloadTooDeep ActivityFailed, got {other:?}"),
        }
        assert_eq!(
            queue::task_state(&conn, &task.task_id).unwrap().as_deref(),
            Some("DONE"),
            "the task is terminal, not requeued"
        );
        let attempts = store::load_attempts(&conn, exec, "act").unwrap();
        assert_eq!(attempts.len(), 1);
        assert!(
            attempts[0].result.is_err(),
            "the audit row holds the failure"
        );
    }

    /// A local run of `act` that allows `max_attempts` attempts.
    fn local_run(max_attempts: u32) -> LocalRun {
        LocalRun {
            activity_id: ActivityExecId::new(),
            name: "act".to_string(),
            input: serde_json::json!(null),
            start_to_close: None,
            retry_policy: Some(RetryPolicy::fixed(max_attempts, Duration::ZERO)),
            failed_attempts: 0,
            last_error: None,
        }
    }

    // Issue #2216: a local result nested past the cap ends the activity on
    // the first attempt. It is not stored as `LocalActivityCompleted`.
    #[test]
    fn a_deep_local_result_ends_the_activity_on_the_first_attempt() {
        let run = local_run(3);
        for depth in [crate::MAX_PAYLOAD_DEPTH + 1, 1_000] {
            let (events, backoff) =
                local_attempt_events(ExecutionId::new(), &run, 1, Ok(nested(depth)));
            assert!(backoff.is_none(), "no retry follows");
            match events.as_slice() {
                [
                    autumn_harvest::WorkflowEvent::LocalActivityFailed { error, .. },
                    autumn_harvest::WorkflowEvent::LocalActivityExhausted { attempt: 1, .. },
                ] => assert_eq!(error, DEEP_ACT_MESSAGE),
                other => panic!("expected a failed and exhausted pair, got {other:?}"),
            }
        }
    }

    // Issue #2216: the depth check runs before the size check. So a result
    // that is too deep and too large fails as `PayloadTooDeep`.
    #[test]
    fn a_deep_and_oversized_result_fails_as_too_deep() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");
        let cap = usize::try_from(autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES)
            .expect("cap fits usize");
        let mut value = serde_json::json!("x".repeat(cap + 1));
        for _ in 0..=crate::MAX_PAYLOAD_DEPTH {
            value = serde_json::Value::Array(vec![value]);
        }

        super::finalize_activity_result(&mut conn, exec, &task, 1, 0, 0, Ok(value), &[]).unwrap();
        match &store::load_history(&conn, exec).unwrap()[..] {
            [autumn_harvest::WorkflowEvent::ActivityFailed { error_type, .. }] => {
                assert_eq!(error_type, crate::PAYLOAD_TOO_DEEP_ERROR_TYPE);
            }
            other => panic!("expected one ActivityFailed, got {other:?}"),
        }
    }

    // Issue #2216: a local result at the cap completes unchanged.
    #[test]
    fn a_local_result_at_the_cap_completes() {
        let capped = nested(crate::MAX_PAYLOAD_DEPTH);
        let (events, backoff) =
            local_attempt_events(ExecutionId::new(), &local_run(1), 1, Ok(capped.clone()));
        assert!(backoff.is_none());
        assert!(matches!(
            events.as_slice(),
            [autumn_harvest::WorkflowEvent::LocalActivityCompleted { output, .. }] if *output == capped
        ));
    }

    // A body that SUCCEEDS after its finalize clock reaches the total
    // schedule-to-close deadline is sealed `ActivityTimedOut { ScheduleToClose }`,
    // not `ActivityCompleted` (#1069). The pre-body clock (0) is under the
    // deadline (1000), so the body runs. The post-body clock (1500) is past it.
    // This is the inline analog of the Postgres timeout scanner. There, the
    // late completion loses the race: `complete` finds the row no longer
    // RUNNING. The two-valued clock models a deadline that elapses WHILE the
    // body runs.
    //
    // The bug: `drain_ready` had no finalize-time recheck, so the result was
    // recorded as `ActivityCompleted` past the total cap.
    #[tokio::test]
    async fn drain_ready_seals_schedule_to_close_when_deadline_elapses_during_a_successful_body() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // `scheduled_at == run_at == 0`, so the deadline is 0 + 1000ms = 1000.
        seed_pending_activity(&conn, exec, "act");

        let mut activities = std::collections::HashMap::new();
        activities.insert("act".to_string(), spec_with_schedule_to_close(1_000, Ok));

        // The pre-body clock 0 passes the check, so the body runs. The
        // post-body clock 1500 is past the deadline, so the recheck seals it.
        let failure_now = advancing_finalize_clock(0, 1_500);
        let produced = drain_ready(
            &mut conn,
            exec,
            &test_owner(),
            0,
            &failure_now,
            &activities,
            false,
        )
        .await
        .unwrap();
        assert!(produced, "a terminal timeout event was appended");

        let history = store::load_history(&conn, exec).unwrap();
        assert!(
            history.iter().any(|e| matches!(
                e,
                autumn_harvest::WorkflowEvent::ActivityTimedOut {
                    timeout_type: autumn_harvest::TimeoutType::ScheduleToClose,
                    ..
                }
            )),
            "a body finalized past its total deadline must seal ActivityTimedOut \
             {{ ScheduleToClose }}:\n{history:?}"
        );
        assert!(
            !history
                .iter()
                .any(|e| matches!(e, autumn_harvest::WorkflowEvent::ActivityCompleted { .. })),
            "the late success must NOT record ActivityCompleted (RED pre-fix):\n{history:?}"
        );
        let task_state: String = conn
            .query_row("SELECT state FROM harvest_tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            task_state, "DONE",
            "the timed-out task is terminal (not requeued)"
        );
    }

    // Control: a successful body whose finalize clock is still BEFORE the total
    // deadline completes normally. The recheck diverts only a LATE success.
    #[tokio::test]
    async fn drain_ready_completes_a_successful_body_finalized_before_the_deadline() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        seed_pending_activity(&conn, exec, "act");

        let mut activities = std::collections::HashMap::new();
        activities.insert("act".to_string(), spec_with_schedule_to_close(1_000, Ok));

        // The finalize clock 500 is under the deadline 1000.
        let failure_now = || 500_i64;
        let produced = drain_ready(
            &mut conn,
            exec,
            &test_owner(),
            0,
            &failure_now,
            &activities,
            false,
        )
        .await
        .unwrap();
        assert!(produced);

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1, "exactly one terminal event");
        assert!(
            matches!(
                history[0],
                autumn_harvest::WorkflowEvent::ActivityCompleted { .. }
            ),
            "an under-deadline success completes normally:\n{history:?}"
        );
    }

    // The finalize-time schedule-to-close recheck runs for a FAILING body too
    // (#1069). A body that starts before its total deadline may return `Err`
    // after it. The ordinary `ActivityFailed` branch would then report a
    // past-cap timeout as an ordinary failure. The Postgres scanner times out
    // ANY running activity past `schedule_to_close`, whatever its result. So
    // this backend seals `ActivityTimedOut { ScheduleToClose }` for any result.
    // `max_attempts = 1` sends the failure to the terminal branch, not to a
    // retry. The pre-body clock is 0 and the post-body clock is 1500.
    //
    // The bug: the recheck ran only for `result.is_ok()`, so an Err body past
    // the deadline recorded `ActivityFailed`, not `ActivityTimedOut`.
    #[tokio::test]
    async fn drain_ready_seals_schedule_to_close_for_a_failing_body_finalized_past_the_deadline() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // `scheduled_at == run_at == 0`, so the deadline is 0 + 1000ms = 1000.
        seed_pending_activity(&conn, exec, "act");

        let mut activities = std::collections::HashMap::new();
        activities.insert(
            "act".to_string(),
            spec_with_schedule_to_close(1_000, |_| Err("boom".to_string())),
        );

        // The pre-body clock 0 lets the body run. The post-body clock 1500 is
        // past the deadline, so the recheck seals the timeout despite the Err.
        let failure_now = advancing_finalize_clock(0, 1_500);
        let produced = drain_ready(
            &mut conn,
            exec,
            &test_owner(),
            0,
            &failure_now,
            &activities,
            false,
        )
        .await
        .unwrap();
        assert!(produced, "a terminal timeout event was appended");

        let history = store::load_history(&conn, exec).unwrap();
        assert!(
            history.iter().any(|e| matches!(
                e,
                autumn_harvest::WorkflowEvent::ActivityTimedOut {
                    timeout_type: autumn_harvest::TimeoutType::ScheduleToClose,
                    ..
                }
            )),
            "a FAILING body finalized past its total deadline must seal ActivityTimedOut \
             {{ ScheduleToClose }} (RED pre-fix: recorded ActivityFailed):\n{history:?}"
        );
        assert!(
            !history
                .iter()
                .any(|e| matches!(e, autumn_harvest::WorkflowEvent::ActivityFailed { .. })),
            "the past-deadline failure must NOT record ActivityFailed (RED pre-fix):\n{history:?}"
        );
        let task_state: String = conn
            .query_row("SELECT state FROM harvest_tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            task_state, "DONE",
            "the timed-out task is terminal (not requeued)"
        );
    }

    // A ZERO-DELAY retry whose deadline elapsed WHILE the body ran seals
    // `ActivityTimedOut { ScheduleToClose }` and is NOT requeued (#1069). The
    // pre-requeue check in `finalize_within_tx` compares the next `run_at`
    // against the deadline. For a zero-delay retry that is the stale
    // cycle-start `now`, still under the deadline. The finalize-time recheck
    // uses the post-body clock, so it seals the timeout BEFORE the retry
    // branch. `max_attempts = 2` and no retry policy take the immediate
    // requeue path (`run_at = now`).
    //
    // The bug: the recheck ran only for `result.is_ok()`. So the Err body
    // requeued at the stale `now` (0 < 1000) and ran a second attempt past the
    // cap.
    #[tokio::test]
    async fn drain_ready_seals_schedule_to_close_for_a_zero_delay_retry_finalized_past_the_deadline()
     {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // `scheduled_at == run_at == 0`, so the deadline is 0 + 1000ms = 1000.
        seed_pending_activity(&conn, exec, "act");

        // The body counts its calls: the bug ran it twice, the fix runs it once.
        let calls = Arc::new(AtomicUsize::new(0));
        let body_calls = Arc::clone(&calls);
        let spec = {
            let mut s = ActivitySpec::new(2, move |_| {
                body_calls.fetch_add(1, Ordering::SeqCst);
                Err("boom".to_string())
            });
            s.schedule_to_close = Some(Duration::from_secs(1));
            s
        };
        let mut activities = std::collections::HashMap::new();
        activities.insert("act".to_string(), spec);

        // At the pre-body clock 0, the stale zero-delay `run_at = 0` passes
        // the retry-path check. The post-body clock 1500 is past the deadline,
        // so the finalize recheck seals the timeout.
        let failure_now = advancing_finalize_clock(0, 1_500);
        let produced = drain_ready(
            &mut conn,
            exec,
            &test_owner(),
            0,
            &failure_now,
            &activities,
            false,
        )
        .await
        .unwrap();
        assert!(produced);

        let history = store::load_history(&conn, exec).unwrap();
        assert!(
            history.iter().any(|e| matches!(
                e,
                autumn_harvest::WorkflowEvent::ActivityTimedOut {
                    timeout_type: autumn_harvest::TimeoutType::ScheduleToClose,
                    ..
                }
            )),
            "a zero-delay retry past the total deadline must seal ActivityTimedOut \
             {{ ScheduleToClose }} (RED pre-fix: requeued at the stale cycle now and \
             retried past the cap):\n{history:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the body must run ONCE then be sealed — NOT retried past the cap \
             (RED pre-fix ran it twice)"
        );
        let task_state: String = conn
            .query_row("SELECT state FROM harvest_tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            task_state, "DONE",
            "the timed-out task is terminal, not requeued PENDING"
        );
    }

    // A `MarkerRecorded` event for the race-in-flight helpers that gate
    // settle-after-first-terminal in `drain_ready` (issue #600).
    fn marker(name: &str) -> autumn_harvest::WorkflowEvent {
        autumn_harvest::WorkflowEvent::MarkerRecorded {
            name: name.to_string(),
            details: serde_json::json!(2),
        }
    }

    #[test]
    fn history_unsettled_race_detects_open_marker_without_a_winner() {
        assert!(history_has_unsettled_activity_race(&[marker("race:0")]));
        // An open marker and the winner of the same seq → settled.
        assert!(!history_has_unsettled_activity_race(&[
            marker("race:0"),
            marker("race_winner:0"),
        ]));
        // A winner for a DIFFERENT seq does not settle the open one.
        assert!(history_has_unsettled_activity_race(&[
            marker("race:0"),
            marker("race_winner:1"),
        ]));
        // Two races, one still open.
        assert!(history_has_unsettled_activity_race(&[
            marker("race:0"),
            marker("race_winner:0"),
            marker("race:1"),
        ]));
        assert!(!history_has_unsettled_activity_race(&[
            marker("fan_out:3"),
            marker("side_effect:0"),
        ]));
        assert!(!history_has_unsettled_activity_race(&[]));
    }

    #[test]
    fn commands_open_race_matches_only_the_open_marker() {
        use autumn_harvest::WorkflowCommand;
        let open = WorkflowCommand::RecordMarker {
            name: "race:0".to_string(),
            details: serde_json::json!(2),
        };
        assert!(commands_open_activity_race(std::slice::from_ref(&open)));
        // The winner marker does not start with `race:`, so it does not open a
        // race.
        let winner = WorkflowCommand::RecordMarker {
            name: "race_winner:0".to_string(),
            details: serde_json::json!(0),
        };
        assert!(!commands_open_activity_race(std::slice::from_ref(&winner)));
        // A fan-out marker is not a race.
        let fan_out = WorkflowCommand::RecordMarker {
            name: "fan_out:3".to_string(),
            details: serde_json::json!(3),
        };
        assert!(!commands_open_activity_race(std::slice::from_ref(&fan_out)));
        assert!(!commands_open_activity_race(&[]));
    }

    // ── Claim and late-registration freeze are atomic (#1080) ─────
    //
    // The `RUNNING` claim and the first-claim freeze commit in ONE transaction.
    // So a crash between them cannot leave a committed `RUNNING` unfrozen row.
    // A reopen would reclaim such a row as `PENDING`, still unfrozen. A
    // re-registration under a CHANGED spec would then freeze the NEW defaults
    // and alter the contract of a claimed task.

    /// `(state, defaults_frozen)` of the single task row for `exec`.
    fn state_and_frozen(conn: &Connection, exec: ExecutionId) -> (String, i64) {
        conn.query_row(
            "SELECT state, defaults_frozen FROM harvest_tasks WHERE exec_id = ?1",
            [exec.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    }

    fn seed_exec_with_pending_activity(conn: &Connection, exec: ExecutionId, name: &str) {
        store::insert_execution(
            conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // Enqueued UNFROZEN, the late-registration shape.
        seed_pending_activity(conn, exec, name);
    }

    /// The claim (`RUNNING` flip) and the freeze of a late-registered task commit
    /// ATOMICALLY. After the commit the row is `RUNNING` **and** frozen. The bug
    /// froze in a SEPARATE transaction after the `RUNNING` commit, so a committed
    /// `RUNNING` unfrozen state existed in the gap.
    #[test]
    fn late_registration_claim_and_freeze_are_atomic() {
        let mut conn = open();
        let exec = ExecutionId::new();
        seed_exec_with_pending_activity(&conn, exec, "act");
        let spec = ActivitySpec::new(3, |_| Ok(serde_json::json!(null)));

        // One transaction for the claim and the freeze, as in `drain_ready`.
        let (tx, mut task) = queue::claim_next_ready_task_tx(&mut conn, exec, 0)
            .unwrap()
            .expect("a ready task");
        assert!(!task.defaults_frozen, "the seeded task is unfrozen");
        super::freeze_defaults_at_claim(&tx, &mut task, &spec).unwrap();
        tx.commit().unwrap();

        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(state, "RUNNING", "the claim committed RUNNING");
        assert_eq!(frozen, 1, "...and the freeze committed atomically with it");
        assert!(
            !(state == "RUNNING" && frozen == 0),
            "invariant: no committed RUNNING+unfrozen row for a late-registered task",
        );
    }

    /// A crash after the `RUNNING` flip but before the freeze commits rolls back
    /// the WHOLE claim, because both share one transaction. The row returns to
    /// `PENDING` and unfrozen. No committed `RUNNING` unfrozen row is left for a
    /// reopen and re-registration to freeze against a changed spec. The bug
    /// committed `RUNNING` at once, so the same crash left exactly that row.
    #[test]
    fn late_registration_frozen_defaults_survive_a_crash_in_the_claim_gap() {
        let mut conn = open();
        let exec = ExecutionId::new();
        seed_exec_with_pending_activity(&conn, exec, "act");

        // Model the crash: open the claim, which flips the row to RUNNING inside
        // the transaction, then drop it before the freeze. The drop rolls back
        // the RUNNING flip too.
        {
            let (tx, task) = queue::claim_next_ready_task_tx(&mut conn, exec, 0)
                .unwrap()
                .expect("a ready task");
            assert!(!task.defaults_frozen);
            // [crash — the freeze never runs and nothing commits]
            drop(tx);
        }

        // The claim rolled back: the row is PENDING and still unfrozen. No
        // committed RUNNING unfrozen row was left for a reopen
        // (`reclaim_orphaned_running`) and a re-registration to freeze.
        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(
            state, "PENDING",
            "the uncommitted claim rolled back — no orphaned RUNNING row",
        );
        assert_eq!(frozen, 0, "still unfrozen — the freeze never committed");
        assert!(
            !(state == "RUNNING" && frozen == 0),
            "a crash in the claim→freeze gap must never leave a committed RUNNING+unfrozen row",
        );

        // The row is re-claimable and freezes against the spec registered at
        // the first successful claim.
        let spec = ActivitySpec::new(2, |_| Ok(serde_json::json!(null)));
        let (tx, mut task) = queue::claim_next_ready_task_tx(&mut conn, exec, 0)
            .unwrap()
            .expect("re-claimable after the rolled-back gap");
        super::freeze_defaults_at_claim(&tx, &mut task, &spec).unwrap();
        tx.commit().unwrap();
        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(
            (state.as_str(), frozen),
            ("RUNNING", 1),
            "the re-claim freezes atomically",
        );
        assert_eq!(
            task.max_attempts,
            Some(2),
            "frozen from the spec registered at the real first claim",
        );
    }

    /// `reclaim_orphaned_running` flips a stranded RUNNING row to PENDING but must
    /// NOT reset `defaults_frozen`. So a crash and reopen cannot re-resolve the
    /// retry and deadline contract against a changed registry.
    #[test]
    fn reclaim_orphaned_running_preserves_defaults_frozen() {
        let conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // A FROZEN, stranded-RUNNING task.
        queue::enqueue_activity(
            &conn,
            exec,
            ActivityExecId::new(),
            "act",
            &serde_json::json!({}),
            "default",
            0,
            Some(3),
            None,
            None,
            None,
            true,
        )
        .unwrap();
        conn.execute("UPDATE harvest_tasks SET state = 'RUNNING'", [])
            .unwrap();

        let n = queue::reclaim_orphaned_running(&conn).unwrap();
        assert_eq!(n, 1, "the stranded RUNNING row is reclaimed");
        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(state, "PENDING", "reclaim flips RUNNING → PENDING");
        assert_eq!(
            frozen, 1,
            "reclaim must NOT reset defaults_frozen (the frozen contract survives)",
        );
    }

    /// The claim transaction RELEASES the `BEGIN IMMEDIATE` write lock and rolls
    /// back the `RUNNING` flip when the freeze fails mid-claim (#1080).
    ///
    /// A `BEFORE UPDATE OF defaults_frozen` trigger aborts the freeze `UPDATE`,
    /// which sets `defaults_frozen = 1`. The state-only claim `UPDATE` does not
    /// touch that column. So the claim flips the row, and then the freeze
    /// fails. On the error the test drops the transaction, as the `?` in
    /// `drain_ready` does. Both the freeze and the `RUNNING` flip roll back.
    ///
    /// Asserts:
    /// - (a) The row stays `PENDING` and unfrozen, with no `RUNNING` orphan.
    /// - (b) A SECOND connection with `busy_timeout = 0` takes the write lock at
    ///   once. A held transaction would fail with `SQLITE_BUSY`.
    ///
    /// The test uses a temp FILE database, because an in-memory database is
    /// private to one connection.
    #[test]
    fn claim_tx_error_path_releases_the_write_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("claim.sqlite3");
        let mut conn = Connection::open(&path).unwrap();
        // `busy_timeout = 0` makes a held lock fail fast with SQLITE_BUSY. WAL
        // mirrors the runtime.
        conn.execute_batch("PRAGMA busy_timeout = 0;\nPRAGMA journal_mode = WAL;")
            .unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();

        let exec = ExecutionId::new();
        seed_exec_with_pending_activity(&conn, exec, "act");

        // Abort each UPDATE that sets `defaults_frozen = 1`. The state-only
        // claim UPDATE is not affected.
        conn.execute_batch(
            "CREATE TRIGGER block_freeze BEFORE UPDATE OF defaults_frozen ON harvest_tasks \
             WHEN NEW.defaults_frozen = 1 \
             BEGIN SELECT RAISE(ABORT, 'injected freeze failure'); END;",
        )
        .unwrap();

        let spec = ActivitySpec::new(3, |_| Ok(serde_json::json!(null)));

        // The claim flips RUNNING inside the transaction, then the trigger
        // aborts the freeze.
        {
            let (tx, mut task) = queue::claim_next_ready_task_tx(&mut conn, exec, 0)
                .unwrap()
                .expect("a ready task");
            let err = super::freeze_defaults_at_claim(&tx, &mut task, &spec).unwrap_err();
            assert!(
                matches!(err, crate::error::SqliteError::Sqlite(_)),
                "the injected freeze failure surfaces as a Sqlite error, got {err:?}",
            );
            drop(tx); // rollback (mirrors drain_ready dropping the tx on `?`)
        }

        // (a) The RUNNING flip rolled back with the failed freeze.
        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(
            state, "PENDING",
            "the RUNNING flip rolled back with the failed freeze",
        );
        assert_eq!(frozen, 0, "still unfrozen — the freeze never committed");

        // (b) A second connection takes the write lock at once, so the
        //     transaction released it.
        let other = Connection::open(&path).unwrap();
        other.execute_batch("PRAGMA busy_timeout = 0;").unwrap();
        other.execute_batch("BEGIN IMMEDIATE; COMMIT;").expect(
            "a competing connection must acquire the BEGIN IMMEDIATE write lock — no SQLITE_BUSY",
        );

        // With the trigger gone, the PENDING row re-claims and freezes.
        conn.execute_batch("DROP TRIGGER block_freeze").unwrap();
        let (tx, mut task) = queue::claim_next_ready_task_tx(&mut conn, exec, 0)
            .unwrap()
            .expect("re-claimable after the rolled-back error path");
        super::freeze_defaults_at_claim(&tx, &mut task, &spec).unwrap();
        tx.commit().unwrap();
        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(
            (state.as_str(), frozen),
            ("RUNNING", 1),
            "clean recovery: the row claims + freezes atomically",
        );
    }
}
