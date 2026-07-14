//! The single-writer worker pass for the spike.
//!
//! [`drain_ready`] is the analog of the `db`-gated [`worker`](crate::worker)
//! poll/dispatch loop, reduced to what a synchronous, embedded runtime needs: it
//! runs every ready activity task's registered body, applies the retry policy,
//! and fires every due timer, appending the resulting terminal
//! [`WorkflowEvent`]s to the canonical history.
//!
//! **Atomic post-body persistence.** The activity body runs *outside* any
//! transaction (a `SQLite` write lock cannot be held across arbitrary user
//! work), but everything *after* it — the attempt audit row plus the terminal
//! `ActivityCompleted`/`ActivityFailed` event and the task-state flip, or the
//! retry requeue — commits in a **single** transaction, mirroring the PG
//! engine's `finalize_activity_*` / `requeue_for_retry` transactions. So the
//! task can never be left in a half-state (terminal event appended but task
//! still `RUNNING`).
//!
//! **Requeue on any post-claim error (no stranded `RUNNING` row).** The claim
//! commits the task `RUNNING` in its *own* transaction (the body runs between the
//! claim and the post-body persistence, so the two cannot share one — see above).
//! Any fallible step *after* that claim therefore risks stranding the task
//! `RUNNING` un-reclaimable, because later claims select only `PENDING`. Two such
//! steps exist: the registered-handler lookup (an *unregistered* activity — a
//! recoverable application condition: register the handler and re-run) and the
//! post-body persistence transaction (a *transient* `SQLite`/JSON error). On
//! **either**, [`drain_ready`] requeues the task to `PENDING` (attempt unchanged)
//! via [`queue::mark_pending`] *before* surfacing the error, so a subsequent run —
//! after the handler is registered or the fault clears — re-drains it rather than
//! wedging forever at [`SpikeError::Stuck`]. The **one** residual window is a
//! *hard process crash* between the claim commit and that requeue (power loss, a
//! `SIGKILL` — no in-process error handler runs, so no requeue happens): a
//! single-process spike has no heartbeat-timeout/poison-pill reclaimer to recover
//! it, exactly as the PG engine does for the same class (§5.1).
//!
//! **Atomic timer firing.** Likewise, each due timer's `TimerFired` append and
//! its `fired = 1` flag commit in one transaction — so a crash can never leave a
//! `TimerFired` event durable with `fired = 0`, which a reload would re-fire into
//! a stray duplicate `TimerFired`.
//!
//! **Retry model (a genuine spike finding).** A *retryable* activity failure is
//! recorded in the [`spike_activity_attempts`](super::schema) audit table and the
//! task-queue row's `attempt` counter — it is **not** appended to the replayable
//! event log. Only the terminal outcome (`ActivityCompleted`, or `ActivityFailed`
//! after exhausting attempts) reaches `spike_events`. On this specific
//! point — keeping retryable failures out of the replayable log — the spike
//! matches the Postgres engine (`queue::requeue_for_retry` stores the attempt
//! error on the task row, never in `harvest_events`), which is what keeps every
//! persisted history a clean, terminal-only, replay-correct log. It is **not** a
//! byte-identical event stream, though: the PG engine additionally appends an
//! `ActivityStarted` on claim that this spike never writes, so the two histories
//! are **replay-equivalent** (the matcher skips `ActivityStarted`), not
//! identical. That replay-equivalence is the property AC4 (cross-backend replay)
//! depends on.

use std::collections::HashMap;

use rusqlite::Connection;

use super::{ActivitySpec, SpikeError, queue, store};
use crate::event::WorkflowEvent;
use crate::types::ExecutionId;

/// Run all currently-ready work for `exec_id` at logical time `now`: drain ready
/// activity tasks (running bodies + honouring retries) and fire due timers.
///
/// Returns `true` if any terminal event was appended (i.e. the workflow may have
/// made progress and should be re-run).
pub(super) fn drain_ready(
    conn: &mut Connection,
    exec_id: ExecutionId,
    now: i64,
    activities: &HashMap<String, ActivitySpec>,
) -> Result<bool, SpikeError> {
    let mut produced = false;

    // Drain activity tasks. A retry requeues at `run_at = now`, so it becomes
    // immediately ready again and this loop re-claims it — the whole retry
    // sequence converges in one drain pass under the polling model.
    while let Some(task) = queue::claim_next_ready_task(conn, exec_id, now)? {
        // The claim committed `task` `RUNNING` in its own transaction. Everything
        // from here (the handler lookup, the body, the post-body persistence) can
        // fail; on **any** error we must return the task to `PENDING` first, or a
        // re-run can never drain it (later claims select only `PENDING`) — the
        // exact wedge Codex flagged for an unregistered activity, and the same
        // hazard for a transient post-body persistence error. `mark_pending` is
        // best-effort (attempt/`run_at` unchanged); we then surface the ORIGINAL
        // error, which is more informative than the later classify-as-`Stuck`. The
        // sole residual is a hard process crash between the claim and this requeue
        // (no handler runs) — the §5.1 window a single-writer spike has no
        // reclaimer for.
        match run_claimed_task(conn, exec_id, now, &task, activities) {
            Ok(made_terminal) => produced |= made_terminal,
            Err(e) => {
                let _ = queue::mark_pending(conn, &task.task_id);
                return Err(e);
            }
        }
    }

    // Fire due timers. Each timer's `TimerFired` append + `fired = 1` flag commit
    // in ONE transaction (`due_timers` already collected the ids into an owned
    // Vec, releasing its borrow, so the loop can take `conn` mutably per timer).
    // Without this atomicity a crash between the two autocommits would leave the
    // `TimerFired` event durable with `fired = 0`, so a reload's `due_timers` scan
    // re-fires the timer and appends a stray SECOND `TimerFired` — corrupting the
    // replayable history with a non-lifecycle event the workflow consumed only
    // once.
    for timer_id in queue::due_timers(conn, exec_id, now)? {
        let tx = conn.transaction()?;
        store::append_event(
            &tx,
            exec_id,
            &WorkflowEvent::TimerFired {
                timer_id: crate::types::TimerId::new(timer_id.clone()),
            },
        )?;
        queue::mark_timer_fired(&tx, exec_id, &timer_id)?;
        tx.commit()?;
        produced = true;
    }

    Ok(produced)
}

/// Run one already-claimed (`RUNNING`) task to its durable terminal — success or
/// retry-exhausted failure — or requeue it for another attempt. Returns `true`
/// iff a terminal event was appended.
///
/// The two post-claim failure modes both surface as an `Err` here, which the
/// caller ([`drain_ready`]) turns into a `mark_pending` requeue:
///
/// 1. The activity `name` has **no registered handler** — a recoverable
///    application condition (register it and re-run). Detected *before* the body
///    runs, so no body executes and the attempt counter is untouched.
/// 2. The **post-body persistence transaction fails** (a transient `SQLite`/JSON
///    error). The transaction is rolled back, so the attempt audit row, the
///    terminal event, and the task-state flip all revert atomically — a re-run
///    re-executes the body (at-least-once, consistent with the crash model) with
///    the same attempt number.
///
/// The body runs *outside* any transaction: a `SQLite` write lock cannot be held
/// across arbitrary user work, and the PG engine does not hold one across the
/// activity body either — the atomic unit is (run body) THEN (persist result),
/// not (body + result) together.
fn run_claimed_task(
    conn: &mut Connection,
    exec_id: ExecutionId,
    now: i64,
    task: &queue::ClaimedTask,
    activities: &HashMap<String, ActivitySpec>,
) -> Result<bool, SpikeError> {
    let spec = activities
        .get(&task.name)
        .ok_or_else(|| SpikeError::unregistered(&task.name))?;

    let result = (spec.body)(task.input.clone());
    let attempt_num = task.attempt + 1;

    // Persist everything AFTER the body — the attempt audit row plus the terminal
    // event + task-state flip (success/exhausted), or the retry requeue — in ONE
    // transaction, mirroring the PG engine's `finalize_activity_*` /
    // `requeue_for_retry` transactions. A failure rolls the whole post-body
    // persistence back, so the task is never left in the half-state "terminal
    // event appended but task still RUNNING" (nor "attempt recorded but no
    // requeue"); the caller's `mark_pending` then returns it to `PENDING`.
    let tx = conn.transaction()?;
    store::record_attempt(&tx, exec_id, &task.name, attempt_num, &result)?;

    let mut produced = false;
    match result {
        Ok(output) => {
            store::append_event(
                &tx,
                exec_id,
                &WorkflowEvent::ActivityCompleted {
                    activity_id: task.activity_id,
                    output,
                },
            )?;
            queue::finish_task(&tx, &task.task_id)?;
            produced = true;
        }
        Err(error) => {
            if attempt_num < spec.max_attempts {
                // Retryable: bump the attempt counter and requeue. NOT recorded
                // in the replayable event log (see module docs).
                queue::requeue_task(&tx, &task.task_id, attempt_num, now)?;
            } else {
                // Exhausted: the terminal failure is the workflow-visible
                // outcome, so it goes into the event log.
                store::append_event(
                    &tx,
                    exec_id,
                    &WorkflowEvent::ActivityFailed {
                        activity_id: task.activity_id,
                        error,
                        attempt: attempt_num,
                        error_type: "Error".to_string(),
                        non_retryable: false,
                        details: None,
                    },
                )?;
                queue::finish_task(&tx, &task.task_id)?;
                produced = true;
            }
        }
    }
    tx.commit()?;
    Ok(produced)
}
