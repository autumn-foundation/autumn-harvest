//! The single-writer worker pass.
//!
//! [`drain_ready`] is the analog of the Postgres poll/dispatch loop, reduced to
//! what a synchronous, embedded runtime needs: it claims every ready activity
//! task, runs its registered body, applies the retry policy, fires every due
//! timer, and appends the resulting terminal [`WorkflowEvent`]s to the canonical
//! history.
//!
//! # Crash model — activity execution is at-least-once (by design)
//!
//! A claim (`BEGIN IMMEDIATE` → `RUNNING`, committed) and the terminal finalize
//! (record-attempt + terminal-event + `DONE`/requeue, committed together in
//! [`finalize_activity_result`]) are each atomic, but the activity **body runs
//! between them, outside any transaction** — you cannot hold `SQLite`'s single
//! write lock across arbitrary user I/O. So a crash after the body runs but
//! before the finalize commits leaves the task `RUNNING`; on the next
//! [`SqliteRuntime::open`](crate::SqliteRuntime::open),
//! [`reclaim_orphaned_running`](crate::queue::reclaim_orphaned_running) flips it
//! back to `PENDING` and the **body re-runs** — hence *at-least-once*. Write
//! activity bodies to be idempotent. A crash *after* the finalize commits sees a
//! `DONE` task and never re-runs the body. The finalize is transactional so the
//! replayable log never records a half-completed activity.
//!
//! # Retry model
//!
//! A *retryable* activity failure is recorded in the `harvest_activity_attempts`
//! audit table and the task-queue row's `attempt` counter — it is **not** appended
//! to the replayable event log. Only the terminal outcome (`ActivityCompleted`, or
//! `ActivityFailed` after exhausting attempts) reaches `harvest_events`. This
//! matches the Postgres engine (`queue::requeue_for_retry` stores the attempt
//! error on the task row, never in `harvest_events`), keeping every persisted
//! history a clean, terminal-only, replay-correct log.

use std::collections::HashMap;

use autumn_harvest::{ExecutionId, TimeoutType, TimerId, WorkflowEvent};
use rusqlite::Connection;

use crate::error::SqliteResult;
use crate::queue::{self, ClaimedTask};
use crate::runtime::ActivitySpec;
use crate::store;

/// Run all currently-ready work for `exec_id` at logical time `now`: drain ready
/// activity tasks (running bodies + honouring retries) and fire due timers.
///
/// Returns `true` if any terminal event was appended (i.e. the workflow may have
/// made progress and should be re-run).
pub fn drain_ready(
    conn: &mut Connection,
    exec_id: ExecutionId,
    now: i64,
    activities: &HashMap<String, ActivitySpec>,
) -> SqliteResult<bool> {
    let mut produced = false;

    // Drain activity tasks. A retry requeues at `run_at = now`, so it becomes
    // immediately ready again and this loop re-claims it — the whole retry
    // sequence converges in one drain pass under the polling model.
    while let Some(task) = queue::claim_next_ready_task(conn, exec_id, now)? {
        // `claim_next_ready_task` has already committed this row to `RUNNING`. If
        // the activity name has no registered body, RELEASE the claim back to
        // `PENDING` before surfacing the error (Codex #1069 P2) — otherwise the
        // row is stranded `RUNNING`, invisible to later drains (which only
        // re-claim `PENDING`), until a full DB close+reopen runs the orphan
        // reclaim. Releasing it lets a later drain re-claim it once the caller
        // registers the activity in the SAME runtime, with no reopen. The error
        // is still returned loudly; the release only leaves the task
        // re-claimable. The error propagates out of the drive loop, so the
        // caller is not spun (mirrors the non-determinism gate).
        let Some(spec) = activities.get(&task.name) else {
            queue::release_claim(conn, &task.task_id)?;
            return Err(crate::error::SqliteError::unregistered(&task.name));
        };

        // Body runs OUTSIDE any transaction — at-least-once (see module docs).
        //
        // Contain a PANICKING body (issue #782 analog; Codex round 8). A body
        // that `panic!()`s (rather than returning `Err`) would otherwise unwind
        // past the finalize/requeue branch AFTER the row is already `RUNNING`, so
        // no failure/requeue ever commits and the run wedges permanently (the
        // claim query only re-selects `PENDING`, and a single-process design has
        // no live-peer poison-pill net). Catch the unwind and convert it to the
        // crate's NORMAL retryable-activity-failure path — treat it exactly like
        // the body returned `Err(msg)`: `finalize_activity_result` records the
        // attempt and follows the retry policy (requeue if attempts remain, else
        // terminal `ActivityFailed`). `AssertUnwindSafe` is sound: `spec.body`
        // and `task.input` are only read, and on a caught panic the closure's
        // result is discarded (no observable state was left half-mutated — the
        // body ran outside any transaction). Mirrors the Postgres worker's
        // handler-panic containment (`catch_unwind` → retryable typed failure).
        //
        // Measure the body's REAL wall-clock runtime (`Instant`, NOT the logical
        // `now` — start-to-close is a real budget for real execution) so a body
        // that overran its start-to-close budget records a timeout below.
        let started = std::time::Instant::now();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (spec.body)(task.input.clone())
        }))
        .unwrap_or_else(|payload| {
            Err(format!(
                "activity panicked: {}",
                autumn_harvest::error::panic_message(payload)
            ))
        });
        let elapsed = started.elapsed();

        // Enforce the activity's start-to-close timeout (issue #1069 P2), when the
        // scheduling command carried one (`start_to_close_override`, persisted on the
        // task row). A synchronous body cannot be cancelled mid-flight in a
        // single-writer runtime, so this is a POST-EXECUTION outcome: if the body's
        // real runtime exceeded its budget, record a TERMINAL
        // `ActivityTimedOut { StartToClose }` — byte-equivalent to the durable event
        // the Postgres timeout scanner (`enforce_activity_timeout`) records — instead
        // of the body's actual result. Terminal, NOT retried (the Postgres scanner
        // appends `ActivityTimedOut` + `fail_task` without requeuing; the workflow
        // observes `HistoryMatch::TimedOut` and drives its own timeout branch).
        // Checked BEFORE the normal finalize so an over-budget body's outcome is a
        // timeout regardless of what it eventually returned. Replay-safe: the outcome
        // is recorded once, and replay never re-runs the body, so a body whose timing
        // varies across a crash-rerun (at-least-once) cannot diverge a committed
        // history.
        if start_to_close_exceeded(elapsed, task.start_to_close_ms) {
            finalize_activity_timeout(conn, exec_id, &task, TimeoutType::StartToClose)?;
            produced = true;
            continue;
        }

        // Honor the workflow's declared/per-call retry policy when the scheduling
        // command carried one (issue #1069 P2): the task row's persisted
        // `max_attempts` (from `retry_policy_override`) wins over the registered
        // `ActivitySpec` default; a raw-path task (`None`) falls back to the spec.
        // Clamp to `>= 1` to mirror `ActivitySpec::new` (a body always runs once).
        let max_attempts = task.max_attempts.unwrap_or(spec.max_attempts).max(1);
        if finalize_activity_result(conn, exec_id, &task, max_attempts, now, result)? {
            produced = true;
        }
    }

    // Fire due timers (each append + mark is atomic — see `fire_timer`).
    for timer_id in queue::due_timers(conn, exec_id, now)? {
        fire_timer(conn, exec_id, &timer_id)?;
        produced = true;
    }

    Ok(produced)
}

/// True iff `timer_id` is the deadline timer of a `wait_for_signal_timeout` /
/// `receive_signal_timeout` race for the signal named `signal_name`.
///
/// The core arms that deadline timer with the id `__signal_timeout:{seq}:{name}`
/// (an inline `format!` in `autumn_harvest::context`, issue #476 — there is no
/// exported constant, so the prefix convention is matched here). Only THAT timer
/// may be recorded ahead of the signal in [`ingest_awaited_signal`]; an unrelated
/// durable timer armed in the same batch must not (see the call site).
///
/// Parsed structurally — strip the `__signal_timeout:` prefix, then `split_once`
/// on the seq's colon so the remainder is the signal name — rather than a `LIKE`
/// pattern, so a signal name that itself contains `:` (or a `LIKE` metacharacter
/// like `%`/`_`) still matches exactly.
fn is_signal_timeout_deadline_timer(timer_id: &str, signal_name: &str) -> bool {
    timer_id
        .strip_prefix("__signal_timeout:")
        .and_then(|rest| rest.split_once(':'))
        .is_some_and(|(_seq, name)| name == signal_name)
}

/// Ingest one awaited signal chronologically against the execution's due timers
/// (issue #476), mirroring the Postgres `merge_wake_events`.
///
/// Consumes the oldest undelivered signal named `signal_name` (if any) and
/// appends its `SignalReceived` — but the **race deadline timer of THIS wait**
/// (`__signal_timeout:{seq}:{signal_name}`) is fired (`TimerFired`) **first** when
/// its `fire_at` is at or before the signal's `received_at`, so a signal delivered
/// *after* an expired `wait_for_signal_timeout` deadline can never retroactively
/// win the signal-or-deadline race. Two kinds of timer are deliberately NOT fired
/// ahead of the signal: (1) the race deadline timer whose deadline is *after* the
/// signal arrived (left for the ordinary [`drain_ready`] pass so an on-time signal
/// still wins even when the run is only driven past the deadline), and (2) any
/// **unrelated** durable timer (e.g. a `join!`ed `ctx.timer("t", 5)`) — firing it
/// before the signal would wedge replay (see the scoping note at the call site).
/// Runs entirely inside the caller's cycle transaction (`conn` is a `&tx`).
///
/// Returns `true` iff any event was appended (a signal was consumed and/or a due
/// deadline timer fired).
pub fn ingest_awaited_signal(
    conn: &Connection,
    exec_id: ExecutionId,
    signal_name: &str,
    now: i64,
) -> SqliteResult<bool> {
    // Peek (and eagerly deserialize) BEFORE mutating: a corrupt payload rolls the
    // whole cycle transaction back with the signal still undelivered (no timer
    // fired, nothing marked) rather than silently dropped.
    let Some(signal) = store::peek_pending_signal(conn, exec_id, signal_name)? else {
        return Ok(false);
    };
    // Fire the RACED signal-timeout deadline timer for THIS wait — but ONLY it —
    // ahead of the signal when its deadline expired at or before the signal
    // arrived (issue #476; Codex #1069 P2, runtime.rs:699). `due_timers_before_signal`
    // returns *every* due timer whose deadline is `<= received_at`, but only the
    // `__signal_timeout:{seq}:{signal_name}` deadline timer of this
    // `wait_for_signal_timeout` race may be recorded before the `SignalReceived`.
    // An UNRELATED durable timer armed in the same batch — e.g.
    // `join!(ctx.wait_for_signal("go"), ctx.timer("t", 5))` — must NOT fire first:
    // the core `match_signal` path does not skip an interleaved `TimerFired`, so a
    // `TimerFired(t)` recorded before `SignalReceived(go)` wedges replay (the run
    // stays parked waiting even though the signal row was delivered). Leave any
    // such unrelated due timer to the ordinary [`drain_ready`] pass, which fires
    // it *after* this signal at a history position the workflow's own timer-await
    // consumes.
    for timer_id in queue::due_timers_before_signal(conn, exec_id, now, signal.received_at)? {
        if is_signal_timeout_deadline_timer(&timer_id, signal_name) {
            fire_timer_within_tx(conn, exec_id, &timer_id)?;
        }
    }
    store::append_event(
        conn,
        exec_id,
        &WorkflowEvent::SignalReceived {
            signal_name: signal_name.to_string(),
            payload: signal.payload,
        },
    )?;
    store::mark_signal_delivered(conn, signal.seq)?;
    Ok(true)
}

/// Finalize one activity attempt in a **single transaction** (AC7): the audit
/// record, the terminal event append (on success or retry-exhaustion), and the
/// task-row transition (`DONE` or requeue) commit together or not at all.
///
/// Returns `true` if a terminal event was appended (i.e. the run may progress).
/// A crash before this commits leaves the task `RUNNING` for orphan reclaim; a
/// crash after leaves a clean `DONE` row and a terminal-only history.
pub fn finalize_activity_result(
    conn: &mut Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    max_attempts: u32,
    now: i64,
    result: Result<serde_json::Value, String>,
) -> SqliteResult<bool> {
    let tx = conn.transaction()?;
    let produced = finalize_within_tx(&tx, exec_id, task, max_attempts, now, result)?;
    tx.commit()?;
    Ok(produced)
}

/// The transactional body of [`finalize_activity_result`], factored out so the
/// atomic unit (record-attempt + terminal-event + task-transition) can be driven
/// directly inside a caller-owned transaction — the finalize atomicity tests
/// invoke this and then *drop the transaction without committing* to prove the
/// three writes roll back together.
pub fn finalize_within_tx(
    conn: &Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    max_attempts: u32,
    now: i64,
    result: Result<serde_json::Value, String>,
) -> SqliteResult<bool> {
    let attempt_num = task.attempt + 1;
    store::record_attempt(conn, exec_id, &task.name, attempt_num, &result)?;

    match result {
        Ok(output) => {
            store::append_event(
                conn,
                exec_id,
                &WorkflowEvent::ActivityCompleted {
                    activity_id: task.activity_id,
                    output,
                },
            )?;
            queue::finish_task(conn, &task.task_id)?;
            Ok(true)
        }
        Err(error) => {
            // Classify the failure exactly as the core worker does
            // (`failure::classify_activity_error`, issue #227): a typed
            // `ActivityFailure` payload carrying `non_retryable: true`, OR a
            // failure matching the resolved retry policy's `non_retryable_errors`
            // list (incl. legacy `Err(String)` values), must skip remaining
            // retries and fail terminally. Without this the SQLite worker would
            // retry a non-retryable error up to `max_attempts` — diverging from
            // Postgres semantics and potentially repeating side effects (issue
            // #1069 P2, Codex `runtime.rs:985`). `parse_typed_payload` returns
            // `Some` only for the typed envelope, so a plain `Err(String)` never
            // synthesizes an `"Error"` class the policy could accidentally match
            // (the back-compat guarantee from #227).
            let typed = autumn_harvest::failure::parse_typed_payload(&error);
            let payload_non_retryable = typed.as_ref().is_some_and(|f| f.non_retryable);
            let typed_error_type = typed.as_ref().map(|f| f.error_type.as_str());
            let policy_non_retryable = task
                .retry_policy
                .as_ref()
                .is_some_and(|p| p.is_non_retryable(typed_error_type, &error));
            let non_retryable = payload_non_retryable || policy_non_retryable;

            if attempt_num < max_attempts && !non_retryable {
                // Retryable: bump the attempt counter and requeue at
                // `now + backoff_delay`, honoring the WHOLE persisted retry policy
                // (`initial_interval`, `backoff_coefficient`, `max_interval`) via
                // the shared core helper `policy::compute_retry_delay` — NOT an
                // immediate requeue at `now` (issue #1069 P2, Codex
                // `runtime.rs:985`). A raw-path task (`retry_policy == None`) keeps
                // the immediate-requeue behavior (`delay = 0`), so its whole retry
                // sequence still converges in one drain pass. A delayed requeue
                // sets `run_at` in the future, so `claim_next_ready_task`
                // (`run_at <= now`) will NOT re-claim it until the driver advances
                // the clock past the deadline — the workflow blocks on the
                // backing-off activity (see `classify_block`) rather than
                // busy-retrying. `attempt_num` is 1-based (1 after the first
                // failure), matching `compute_retry_delay`'s `attempt` (exp =
                // attempt - 1), so the first retry waits `initial_interval`.
                // NOT recorded in the replayable event log (see module docs).
                let delay_ms = task.retry_policy.as_ref().map_or(0, |p| {
                    let delay = autumn_harvest::policy::compute_retry_delay(
                        p.initial_interval,
                        p.backoff_coefficient,
                        p.max_interval,
                        attempt_num,
                    );
                    i64::try_from(delay.as_millis()).unwrap_or(i64::MAX)
                });
                let run_at = now.saturating_add(delay_ms);
                queue::requeue_task(conn, &task.task_id, attempt_num, run_at)?;
                Ok(false)
            } else {
                // Terminal (attempts exhausted OR non-retryable): the failure is
                // workflow-visible, so it goes into the event log. DECODE the
                // failure envelope (`parse_error_payload_full`, issue #1069 P2,
                // Codex `worker.rs:299`) so the terminal `ActivityFailed` carries
                // the SAME typed metadata the Postgres worker's
                // `finalize_activity_failure` records — `error_type` / `details` /
                // `non_retryable` plus the human `message` (never the raw
                // `harvest_activity_failure_v1` envelope) — keeping typed-failure
                // histories byte-equivalent across backends (AC5). A legacy
                // `Err(String)` decodes to `error_type = "Error"`,
                // `non_retryable = false`, `details = None`, `message = error`, so
                // its stored form is unchanged from before this fix.
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
/// `budget_ms` (issue #1069 P2).
///
/// `None` budget = no start-to-close timeout (unbounded — never exceeded, the
/// prior behavior for every activity without a declared/per-call timeout). A
/// non-positive budget is also treated as unbounded (defensive: the core never
/// emits a `<= 0` start-to-close, and a `0` budget would otherwise time out every
/// body). The comparison is strict (`>`), so a body that finishes exactly at its
/// budget completes normally rather than timing out.
#[must_use]
pub fn start_to_close_exceeded(elapsed: std::time::Duration, budget_ms: Option<i64>) -> bool {
    match budget_ms {
        Some(ms) if ms > 0 => elapsed.as_millis() > u128::try_from(ms).unwrap_or(u128::MAX),
        _ => false,
    }
}

/// Finalize an activity that exceeded its start-to-close budget (issue #1069 P2)
/// in a **single transaction** (AC7): record the timed-out attempt in the audit
/// log, append the terminal `ActivityTimedOut` event, and mark the task `DONE`,
/// together or not at all.
///
/// **Terminal — no requeue.** This mirrors the Postgres `enforce_activity_timeout`,
/// which appends `ActivityTimedOut` and `fail_task`s without requeuing: a
/// start-to-close timeout is workflow-visible (`HistoryMatch::TimedOut`), not an
/// attempt-level retry. The workflow drives its own timeout branch.
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

/// The transactional body of [`finalize_activity_timeout`], factored out so the
/// atomic unit (audit-attempt + terminal `ActivityTimedOut` + `DONE`) can be
/// driven inside a caller-owned transaction and its roll-back-together atomicity
/// asserted directly.
pub fn finalize_timeout_within_tx(
    conn: &Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    timeout_type: TimeoutType,
) -> SqliteResult<()> {
    let attempt_num = task.attempt + 1;
    // A timed-out attempt is an attempt outcome — record it in the per-attempt
    // audit log (mirroring `finalize_within_tx`, which always records the attempt).
    store::record_attempt(
        conn,
        exec_id,
        &task.name,
        attempt_num,
        &Err("activity exceeded its start-to-close timeout".to_string()),
    )?;
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

/// Fire one due timer atomically: append `TimerFired` and mark the timer row
/// `fired` in a single transaction, so a crash never records the fired event
/// without flipping the row — which would both re-fire the timer on restart and
/// leave a stray non-lifecycle `TimerFired` in history.
pub fn fire_timer(conn: &mut Connection, exec_id: ExecutionId, timer_id: &str) -> SqliteResult<()> {
    let tx = conn.transaction()?;
    fire_timer_within_tx(&tx, exec_id, timer_id)?;
    tx.commit()?;
    Ok(())
}

/// The transactional body of [`fire_timer`], factored out so the append-vs-flag
/// atomicity can be driven inside a caller-owned transaction (the timer-fire
/// atomicity test invokes this then *drops the transaction* to prove both writes
/// roll back together — a crash-between-append-and-flag double-fire is
/// impossible).
pub fn fire_timer_within_tx(
    conn: &Connection,
    exec_id: ExecutionId,
    timer_id: &str,
) -> SqliteResult<()> {
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
    use rusqlite::Connection;

    use autumn_harvest::ExecutionId;

    use std::time::Duration;

    use super::{
        finalize_within_tx, fire_timer, fire_timer_within_tx, ingest_awaited_signal,
        is_signal_timeout_deadline_timer, start_to_close_exceeded,
    };
    use crate::queue::{self, ClaimedTask};
    use crate::{schema, store};

    fn open() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();
        conn
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
        )
        .unwrap();
        conn.execute("UPDATE harvest_tasks SET state = 'RUNNING'", [])
            .unwrap();
        let task_id: String = conn
            .query_row("SELECT task_id FROM harvest_tasks", [], |r| r.get(0))
            .unwrap();
        ClaimedTask {
            task_id,
            activity_id: act,
            name: name.to_string(),
            input: serde_json::json!({}),
            attempt: 0,
            max_attempts: None,
            retry_policy: None,
            start_to_close_ms: None,
        }
    }

    // AC7: the terminal finalize is atomic — on rollback NEITHER the
    // ActivityCompleted event nor the DONE transition persists, leaving the task
    // RUNNING (re-runnable after orphan reclaim).
    #[test]
    fn finalize_rolls_back_terminal_event_and_task_transition_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(&conn, exec, "wf", &serde_json::json!(null)).unwrap();
        let task = seed_running_task(&conn, exec, "act");

        {
            let tx = conn.transaction().unwrap();
            finalize_within_tx(&tx, exec, &task, 1, 0, Ok(serde_json::json!("done"))).unwrap();
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

    // AC7: on commit BOTH land together — a clean, terminal-only history and a
    // DONE task row.
    #[test]
    fn finalize_commits_terminal_event_and_done_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(&conn, exec, "wf", &serde_json::json!(null)).unwrap();
        let task = seed_running_task(&conn, exec, "act");

        {
            let tx = conn.transaction().unwrap();
            finalize_within_tx(&tx, exec, &task, 1, 0, Ok(serde_json::json!("done"))).unwrap();
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

    // A retryable failure requeues (no terminal event) and records the attempt —
    // both inside the same finalize transaction.
    #[test]
    fn finalize_retryable_failure_requeues_without_terminal_event() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(&conn, exec, "wf", &serde_json::json!(null)).unwrap();
        let task = seed_running_task(&conn, exec, "act");

        let produced =
            super::finalize_activity_result(&mut conn, exec, &task, 3, 0, Err("boom".into()))
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

    // A due timer fire is atomic on commit: the TimerFired event and the `fired`
    // flag land together.
    #[test]
    fn timer_fire_commits_event_and_flag_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(&conn, exec, "wf", &serde_json::json!(null)).unwrap();
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

    // Timer-fire atomicity (correction #1): a crash between the TimerFired append
    // and the `fired` flag update rolls BOTH back — the timer stays unfired (no
    // double-fire) and no stray non-lifecycle TimerFired event survives.
    #[test]
    fn timer_fire_rolls_back_event_and_flag_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(&conn, exec, "wf", &serde_json::json!(null)).unwrap();
        queue::enqueue_timer(&conn, exec, "t1", 0).unwrap();

        {
            let tx = conn.transaction().unwrap();
            fire_timer_within_tx(&tx, exec, "t1").unwrap();
            // Drop `tx` WITHOUT commit → rollback (simulated crash-between).
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

    // FIX 2 (Codex #1069 P2, runtime.rs:715): the start-to-close budget predicate.
    // `None` (and a non-positive) budget is unbounded (never exceeded); a body is
    // over-budget only when it STRICTLY exceeds a positive budget, so a body that
    // finishes exactly at its budget completes normally.
    #[test]
    fn start_to_close_exceeded_truth_table() {
        // No budget → never exceeded (prior behavior for every un-timed activity).
        assert!(!start_to_close_exceeded(Duration::from_secs(10), None));
        // Under budget.
        assert!(!start_to_close_exceeded(Duration::from_millis(5), Some(10)));
        // Exactly at budget → NOT exceeded (strict `>`).
        assert!(!start_to_close_exceeded(
            Duration::from_millis(10),
            Some(10)
        ));
        // Over budget → exceeded.
        assert!(start_to_close_exceeded(Duration::from_millis(11), Some(10)));
        assert!(start_to_close_exceeded(Duration::from_secs(40), Some(1)));
        // A non-positive budget is treated as unbounded (defensive; core never
        // emits `<= 0`), so a `0`/negative budget never times a body out.
        assert!(!start_to_close_exceeded(Duration::from_secs(10), Some(0)));
        assert!(!start_to_close_exceeded(Duration::from_secs(10), Some(-5)));
    }

    // FIX 1 (Codex #1069 P2, runtime.rs:699): the deadline-timer-name predicate
    // matches ONLY a `__signal_timeout:{seq}:{name}` race timer for the exact
    // signal name — including a name that itself contains a colon — and never an
    // unrelated durable timer.
    #[test]
    fn signal_timeout_deadline_timer_predicate_matches_only_the_raced_timer() {
        assert!(is_signal_timeout_deadline_timer(
            "__signal_timeout:0:go",
            "go"
        ));
        assert!(is_signal_timeout_deadline_timer(
            "__signal_timeout:7:approval",
            "approval"
        ));
        // A signal name that itself contains ':' still matches (structural
        // `split_once`, not a fragile suffix/`LIKE` match).
        assert!(is_signal_timeout_deadline_timer(
            "__signal_timeout:3:ns:go",
            "ns:go"
        ));
        // Wrong signal name → no match (a deadline timer for a DIFFERENT wait must
        // not be reordered ahead of THIS signal).
        assert!(!is_signal_timeout_deadline_timer(
            "__signal_timeout:0:other",
            "go"
        ));
        // An UNRELATED durable timer (`join!(wait_for_signal("go"), timer("t", 5))`)
        // is never a race deadline timer.
        assert!(!is_signal_timeout_deadline_timer("t", "go"));
        assert!(!is_signal_timeout_deadline_timer("heartbeat", "go"));
        // A prefix look-alike that is not the real convention → no match.
        assert!(!is_signal_timeout_deadline_timer(
            "__signal_timeout_go",
            "go"
        ));
    }

    // FIX 1 (Codex #1069 P2, runtime.rs:699): `ingest_awaited_signal` reorders ONLY
    // the raced `__signal_timeout:{seq}:{name}` deadline timer ahead of the signal.
    // An UNRELATED due timer (e.g. a `join!`ed `ctx.timer("t", 5)`) is deliberately
    // LEFT UNFIRED here — firing a `TimerFired(t)` before `SignalReceived(go)` would
    // wedge the core `match_signal` replay (which does not skip an interleaved
    // `TimerFired`). The unrelated timer is instead left for the ordinary
    // `drain_ready` pass to fire AFTER the signal.
    //
    // Seed BOTH a raced deadline timer and an unrelated timer, both due at or
    // before the signal's arrival, then ingest the signal and assert only the
    // deadline timer fired ahead of it. Pre-fix (fire every due-before-signal
    // timer) this test is RED: it also records `TimerFired(t)` before the signal
    // and leaves `t` fired.
    #[test]
    fn ingest_reorders_only_the_raced_deadline_timer_ahead_of_the_signal() {
        let conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(&conn, exec, "wf", &serde_json::json!(null)).unwrap();

        // Signal arrives at t=100.
        store::stage_signal(&conn, exec, "go", &serde_json::json!("v"), 100).unwrap();
        // The raced deadline timer of THIS wait, expired at t=60 (<= 100).
        queue::enqueue_timer(&conn, exec, "__signal_timeout:0:go", 60).unwrap();
        // An UNRELATED durable timer, also due at t=50 (<= 100) — must NOT fire
        // ahead of the signal.
        queue::enqueue_timer(&conn, exec, "t", 50).unwrap();

        // Ingest inside a transaction (the real caller passes a `&tx`); commit so
        // the assertions read the persisted state.
        let tx = conn.unchecked_transaction().unwrap();
        let produced = ingest_awaited_signal(&tx, exec, "go", 200).unwrap();
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

        // The deadline TimerFired is recorded BEFORE the SignalReceived (the timeout
        // wins the race against a late signal); the signal is still recorded.
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

        // The unrelated timer `t` is still armed (unfired) — left for the ordinary
        // drain pass, which fires it at a position the workflow's own timer-await
        // consumes AFTER the signal.
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
}
