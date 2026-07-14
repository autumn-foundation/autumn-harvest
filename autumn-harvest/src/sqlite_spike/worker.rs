//! The single-writer worker pass for the spike.
//!
//! [`drain_ready`] is the analog of the `db`-gated [`worker`](crate::worker)
//! poll/dispatch loop, reduced to what a synchronous, embedded runtime needs: it
//! runs every ready activity task's registered body, applies the retry policy,
//! and fires every due timer, appending the resulting terminal
//! [`WorkflowEvent`]s to the canonical history.
//!
//! **Retry model (a genuine spike finding).** A *retryable* activity failure is
//! recorded in the [`spike_activity_attempts`](super::schema) audit table and the
//! task-queue row's `attempt` counter — it is **not** appended to the replayable
//! event log. Only the terminal outcome (`ActivityCompleted`, or `ActivityFailed`
//! after exhausting attempts) reaches `spike_events`. This mirrors the Postgres
//! engine exactly (`queue::requeue_for_retry` stores the attempt error on the
//! task row, never in `harvest_events`) and is what keeps every persisted history
//! a clean, terminal-only, replay-correct log — the property AC4 (cross-backend
//! replay) depends on.

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
        let spec = activities
            .get(&task.name)
            .ok_or_else(|| SpikeError::unregistered(&task.name))?;

        let result = (spec.body)(task.input.clone());
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
                produced = true;
            }
            Err(error) => {
                if attempt_num < spec.max_attempts {
                    // Retryable: bump the attempt counter and requeue. NOT
                    // recorded in the replayable event log (see module docs).
                    queue::requeue_task(conn, &task.task_id, attempt_num, now)?;
                } else {
                    // Exhausted: the terminal failure is the workflow-visible
                    // outcome, so it goes into the event log.
                    store::append_event(
                        conn,
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
                    queue::finish_task(conn, &task.task_id)?;
                    produced = true;
                }
            }
        }
    }

    // Fire due timers.
    for timer_id in queue::due_timers(conn, exec_id, now)? {
        store::append_event(
            conn,
            exec_id,
            &WorkflowEvent::TimerFired {
                timer_id: crate::types::TimerId::new(timer_id.clone()),
            },
        )?;
        queue::mark_timer_fired(conn, exec_id, &timer_id)?;
        produced = true;
    }

    Ok(produced)
}
