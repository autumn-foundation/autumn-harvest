//! Timeout enforcement for tasks in the work queue.
//!
//! This module provides a background scanner that periodically checks for tasks
//! that have exceeded their timeout limits:
//!
//! - **Heartbeat timeout**: RUNNING tasks whose `last_heartbeat_at` is older than
//!   their `heartbeat_timeout` interval.
//! - **Start-to-close timeout**: RUNNING tasks whose `started_at` plus
//!   `start_to_close` interval has elapsed.
//! - **Schedule-to-start timeout**: PENDING tasks whose `scheduled_at` plus
//!   `schedule_to_start` interval has elapsed.

use std::collections::HashSet;
use std::time::Duration;

use chrono::Utc;
use diesel::{
    BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use scoped_futures::ScopedFutureExt;
use tokio_util::sync::CancellationToken;

use crate::error::{HarvestError, HarvestResult, TimeoutType};
use crate::event::WorkflowEvent;
use crate::execution::{
    apply_parent_close_cascade, cancel_workflow_execution, cancel_workflow_execution_collect,
    check_and_report_unfinished_handlers,
};
use crate::models::{ExternalTask, TaskQueueItem, WorkflowExecution};
use crate::schema::{harvest_external_tasks, harvest_task_queue, harvest_workflow_executions};
use crate::telemetry::MetricsRecorder;
use crate::types::{ActivityExecId, ExecutionId};
use crate::{queue, store};

/// The reason a task was identified as timed out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeoutReason {
    /// Activity stopped sending heartbeats within the configured interval.
    Heartbeat,
    /// Task has been RUNNING longer than its `start_to_close` limit.
    StartToClose,
    /// Task has been PENDING longer than its `schedule_to_start` limit.
    ScheduleToStart,
    /// The cross-retry wall-clock deadline (`schedule_to_close_at`) has elapsed.
    ///
    /// Fires for both RUNNING tasks (mid-execution) and PENDING tasks (queued
    /// but deadline already expired before a worker could claim the task).
    ScheduleToClose,
}

impl std::fmt::Display for TimeoutReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Heartbeat => write!(f, "Heartbeat"),
            Self::StartToClose => write!(f, "StartToClose"),
            Self::ScheduleToStart => write!(f, "ScheduleToStart"),
            Self::ScheduleToClose => write!(f, "ScheduleToClose"),
        }
    }
}

impl TimeoutReason {
    const fn timeout_type(&self) -> TimeoutType {
        match self {
            Self::Heartbeat => TimeoutType::Heartbeat,
            Self::StartToClose => TimeoutType::StartToClose,
            Self::ScheduleToStart => TimeoutType::ScheduleToStart,
            Self::ScheduleToClose => TimeoutType::ScheduleToClose,
        }
    }
}

// ---------------------------------------------------------------------------
// SQL query builders
// ---------------------------------------------------------------------------

/// SQL query to find RUNNING tasks with expired heartbeat timeout.
///
/// A task is considered heartbeat-timed-out when:
/// - `state = 'RUNNING'`
/// - `heartbeat_timeout IS NOT NULL`
/// - `last_heartbeat_at + heartbeat_timeout < NOW()` (or `started_at` if no heartbeat yet)
#[must_use]
pub const fn heartbeat_timeout_query() -> &'static str {
    "SELECT * FROM harvest_task_queue \
     WHERE state = 'RUNNING' \
     AND heartbeat_timeout IS NOT NULL \
     AND COALESCE(last_heartbeat_at, started_at) + heartbeat_timeout < NOW()"
}

/// SQL query to find RUNNING tasks that exceeded their start-to-close timeout.
///
/// A task is considered start-to-close-timed-out when:
/// - `state = 'RUNNING'`
/// - `start_to_close IS NOT NULL`
/// - `started_at + start_to_close < NOW()`
#[must_use]
pub const fn start_to_close_timeout_query() -> &'static str {
    "SELECT * FROM harvest_task_queue \
     WHERE state = 'RUNNING' \
     AND start_to_close IS NOT NULL \
     AND started_at + start_to_close < NOW()"
}

/// SQL query to find PENDING tasks that exceeded their schedule-to-start timeout.
///
/// A task is considered schedule-to-start-timed-out when:
/// - `state = 'PENDING'`
/// - `schedule_to_start IS NOT NULL`
/// - `scheduled_at + schedule_to_start < NOW()`
#[must_use]
pub const fn schedule_to_start_timeout_query() -> &'static str {
    "SELECT * FROM harvest_task_queue \
     WHERE state = 'PENDING' \
     AND schedule_to_start IS NOT NULL \
     AND scheduled_at + schedule_to_start < NOW()"
}

/// SQL query to find RUNNING or PENDING tasks that exceeded their total
/// schedule-to-close wall-clock deadline (issue #378).
///
/// A task is considered schedule-to-close-timed-out when:
/// - `state IN ('RUNNING', 'PENDING')`
/// - `schedule_to_close_at IS NOT NULL`
/// - `schedule_to_close_at < NOW()`
///
/// Fires for in-flight executions (RUNNING) and tasks queued past their
/// deadline (PENDING). The partial index on `schedule_to_close_at` makes
/// this scan cheap.
#[must_use]
pub const fn schedule_to_close_timeout_query() -> &'static str {
    "SELECT * FROM harvest_task_queue \
     WHERE state IN ('RUNNING', 'PENDING') \
     AND schedule_to_close_at IS NOT NULL \
     AND schedule_to_close_at < NOW()"
}

/// SQL query to find RUNNING workflow executions that have exceeded their
/// `execution_timeout` wall-clock deadline (issue #243).
///
/// A workflow execution is considered timed out when:
/// - `state = 'RUNNING'`
/// - `deadline_at IS NOT NULL`
/// - `deadline_at < NOW()`
///
/// `deadline_at` is computed and persisted at start time as
/// `started_at + execution_timeout`, so this is a simple indexed range scan.
#[must_use]
pub const fn workflow_execution_timeout_query() -> &'static str {
    "SELECT * FROM harvest_workflow_executions \
     WHERE state = 'RUNNING' \
     AND deadline_at IS NOT NULL \
     AND deadline_at < NOW()"
}

/// Find all tasks that have exceeded their timeout limits.
///
/// Runs all three timeout queries and returns the matched tasks along with
/// their timeout reason.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
pub async fn find_timed_out_tasks(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Vec<(TaskQueueItem, TimeoutReason)>> {
    let mut results = Vec::new();
    let mut seen = HashSet::new();

    // Heartbeat timeouts
    let heartbeat_tasks: Vec<TaskQueueItem> = diesel::sql_query(heartbeat_timeout_query())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    for task in heartbeat_tasks {
        if seen.insert(task.id) {
            results.push((task, TimeoutReason::Heartbeat));
        }
    }

    // Start-to-close timeouts
    let start_close_tasks: Vec<TaskQueueItem> = diesel::sql_query(start_to_close_timeout_query())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    for task in start_close_tasks {
        if seen.insert(task.id) {
            results.push((task, TimeoutReason::StartToClose));
        }
    }

    // Schedule-to-start timeouts
    let sched_start_tasks: Vec<TaskQueueItem> =
        diesel::sql_query(schedule_to_start_timeout_query())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;
    for task in sched_start_tasks {
        if seen.insert(task.id) {
            results.push((task, TimeoutReason::ScheduleToStart));
        }
    }

    // Schedule-to-close timeouts (cross-retry wall-clock deadline, issue #378)
    let sched_close_tasks: Vec<TaskQueueItem> =
        diesel::sql_query(schedule_to_close_timeout_query())
            .load(conn)
            .await
            .map_err(crate::error::database_error)?;
    for task in sched_close_tasks {
        if seen.insert(task.id) {
            results.push((task, TimeoutReason::ScheduleToClose));
        }
    }

    Ok(results)
}

fn execution_id_from_uuid(id: uuid::Uuid) -> crate::types::ExecutionId {
    id.to_string()
        .parse()
        .expect("database UUIDs must round-trip into ExecutionId")
}

fn timeout_error(task_name: &str, reason: &TimeoutReason) -> String {
    HarvestError::Timeout {
        timeout_type: reason.timeout_type(),
        task_name: task_name.to_string(),
    }
    .to_string()
}

fn find_pending_scheduled_activity(
    history: &[WorkflowEvent],
    activity_name: &str,
) -> HarvestResult<crate::types::ActivityExecId> {
    let terminal_ids = history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. } => Some(*activity_id),
            _ => None,
        })
        .collect::<HashSet<_>>();

    let mut pending = None;
    for event in history {
        if let WorkflowEvent::ActivityScheduled {
            activity_id, name, ..
        } = event
            && name == activity_name
            && !terminal_ids.contains(activity_id)
        {
            if pending.is_some() {
                return Err(HarvestError::non_deterministic_simple(format!(
                    "multiple pending scheduled activities named '{activity_name}' found in history"
                )));
            }
            pending = Some(*activity_id);
        }
    }

    pending.ok_or_else(|| {
        HarvestError::NotFound(format!(
            "no pending scheduled activity '{activity_name}' in workflow history"
        ))
    })
}

fn find_pending_scheduled_activity_by_id(
    history: &[WorkflowEvent],
    requested_activity_id: crate::types::ActivityExecId,
    activity_name: &str,
) -> HarvestResult<crate::types::ActivityExecId> {
    let mut scheduled = false;
    let mut terminal = false;

    for event in history {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if *activity_id == requested_activity_id => {
                if name != activity_name {
                    return Err(HarvestError::non_deterministic_simple(format!(
                        "activity task id '{}' was scheduled for '{name}', not '{activity_name}'",
                        requested_activity_id.as_uuid()
                    )));
                }
                scheduled = true;
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. }
                if *activity_id == requested_activity_id =>
            {
                terminal = true;
            }
            _ => {}
        }
    }

    if scheduled && !terminal {
        Ok(requested_activity_id)
    } else if terminal {
        Err(HarvestError::NotFound(format!(
            "activity '{activity_name}' with id '{}' already has a terminal event",
            requested_activity_id.as_uuid()
        )))
    } else {
        Err(HarvestError::NotFound(format!(
            "no scheduled activity '{activity_name}' with id '{}' in workflow history",
            requested_activity_id.as_uuid()
        )))
    }
}

fn has_activity_terminal_event(
    history: &[WorkflowEvent],
    activity_id: crate::types::ActivityExecId,
) -> bool {
    history.iter().any(|event| {
        matches!(
            event,
            WorkflowEvent::ActivityCompleted { activity_id: id, .. }
                | WorkflowEvent::ActivityFailed { activity_id: id, .. }
                | WorkflowEvent::ActivityTimedOut { activity_id: id, .. }
                if *id == activity_id
        )
    })
}

fn pending_activity_id_for_task(
    history: &[WorkflowEvent],
    task: &TaskQueueItem,
    activity_name: &str,
) -> HarvestResult<Option<crate::types::ActivityExecId>> {
    if let Some(activity_id) = task.activity_id {
        let activity_id = crate::types::ActivityExecId::from_uuid(activity_id);
        if has_activity_terminal_event(history, activity_id) {
            return Ok(None);
        }
        return find_pending_scheduled_activity_by_id(history, activity_id, activity_name)
            .map(Some);
    }

    find_pending_scheduled_activity(history, activity_name).map(Some)
}

async fn lock_workflow_execution_and_load_history(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
) -> HarvestResult<store::EventHistory> {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .for_update()
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

    store::load_history(conn, exec_id).await
}

async fn task_state_for_update(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
) -> HarvestResult<Option<String>> {
    use crate::schema::harvest_task_queue::dsl;

    dsl::harvest_task_queue
        .find(task_id)
        .for_update()
        .select(dsl::state)
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)
}

/// Returns the set of task states that are valid for a given timeout reason.
///
/// `enforce_activity_timeout` skips rows whose current state is not in this
/// set, guarding against double-enforcement races (e.g. two scanner ticks
/// racing on the same row, or a worker completing a task between the scan
/// and enforcement).
const fn expected_task_states_for_timeout(reason: &TimeoutReason) -> &'static [&'static str] {
    match reason {
        TimeoutReason::Heartbeat | TimeoutReason::StartToClose => &["RUNNING"],
        TimeoutReason::ScheduleToStart => &["PENDING"],
        // ScheduleToClose fires for both: mid-execution (RUNNING) and queued
        // past the deadline before any worker claimed it (PENDING).
        TimeoutReason::ScheduleToClose => &["RUNNING", "PENDING"],
    }
}

async fn load_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
) -> HarvestResult<WorkflowExecution> {
    use crate::schema::harvest_workflow_executions::dsl;

    dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
}

async fn update_workflow_execution_timed_out(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    error: &str,
) -> HarvestResult<()> {
    use crate::schema::harvest_workflow_executions::dsl;

    // Code-review fix (issue #603): see
    // `worker::update_workflow_execution_completed` for the rationale --
    // read the pre-update block state so the search_attrs clear below can be
    // gated on it instead of running unconditionally on every timeout.
    let was_nd_blocked = dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(dsl::nd_blocked_at.is_not_null())
        .first::<bool>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .unwrap_or(false);

    let updated = diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
        .set((
            dsl::state.eq("TIMED_OUT"),
            dsl::output.eq(None::<serde_json::Value>),
            dsl::error.eq(Some(error.to_string())),
            dsl::completed_at.eq(Some(Utc::now())),
            // Belt-and-braces ND-block reset (code-review fix, issue #603):
            // a TIMED_OUT execution closes out permanently — a stale block
            // marker must not survive on a terminal row, matching the
            // precedent already applied to the two worker.rs terminal
            // writers.
            dsl::nd_blocked_at.eq(None::<chrono::DateTime<Utc>>),
            dsl::nd_block_reason.eq(None::<String>),
            dsl::nd_block_count.eq(0),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(HarvestError::NotFound(format!(
            "workflow execution {exec_id}"
        )));
    }

    // Gated on `was_nd_blocked` (PR review fix): an unconditional clear here
    // would silently delete pre-existing user search_attrs of the same name
    // on rows created before these keys became reserved.
    if was_nd_blocked {
        crate::store::update_search_attrs(
            conn,
            exec_id,
            &crate::worker::nd_search_attrs_clear_patch(),
        )
        .await?;
    }

    Ok(())
}

async fn wake_parent_for_child_timeout(
    conn: &mut AsyncPgConnection,
    parent_exec_id: crate::types::ExecutionId,
    child_exec_id: crate::types::ExecutionId,
    error: &str,
) -> HarvestResult<()> {
    // Use append_single_event so concurrent sibling timeout/completion paths
    // serialise around the parent execution row and cannot collide on the
    // (workflow_exec_id, event_id) unique constraint.
    let event = WorkflowEvent::ChildWorkflowFailed {
        child_id: child_exec_id,
        error: error.to_string(),
    };
    store::append_single_event(conn, parent_exec_id, event).await?;
    queue::wake_workflow_task(conn, parent_exec_id).await
}

async fn commit_workflow_execution_timeout(
    conn: &mut AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    parent_uuid: Option<uuid::Uuid>,
    timeout_event: &WorkflowEvent,
    error_msg: &str,
    metrics: Option<&(dyn MetricsRecorder + Send + Sync)>,
) -> HarvestResult<(
    bool,
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(ExecutionId, String)>,
)> {
    conn.transaction::<(
        bool,
        Vec<crate::completion_trigger::DeferredTriggerStart>,
        Vec<(ExecutionId, String)>,
    ), HarvestError, _>(|conn| {
        let timeout_event = timeout_event.clone();
        let error_msg = error_msg.to_owned();
        async move {
            // Re-check state under lock to guard against concurrent completion.
            let current_state: Option<String> = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .for_update()
                .select(harvest_workflow_executions::state)
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            match current_state.as_deref() {
                Some("RUNNING") => {}
                _ => return Ok((false, Vec::new(), Vec::new())),
            }

            store::append_single_event(conn, exec_id, timeout_event).await?;
            update_workflow_execution_timed_out(conn, exec_id, &error_msg).await?;

            let _rows = diesel::update(
                harvest_task_queue::table
                    .filter(harvest_task_queue::workflow_exec_id.eq(exec_id.as_uuid()))
                    .filter(
                        harvest_task_queue::state
                            .eq("PENDING")
                            .or(harvest_task_queue::state.eq("RUNNING")),
                    ),
            )
            .set((
                harvest_task_queue::state.eq("FAILED"),
                harvest_task_queue::error.eq(Some(&error_msg)),
                harvest_task_queue::completed_at.eq(Some(Utc::now())),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

            if let Some(parent_uuid) = parent_uuid {
                wake_parent_for_child_timeout(
                    conn,
                    execution_id_from_uuid(parent_uuid),
                    exec_id,
                    &error_msg,
                )
                .await?;
            }

            let (mut deferred, closed_children) = apply_parent_close_cascade(conn, exec_id).await?;
            let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                conn,
                exec_id,
                crate::completion_trigger::TerminalState::TimedOut,
                metrics,
            )
            .await?;
            deferred.extend(triggers);
            Ok((true, deferred, closed_children))
        }
        .scope_boxed()
    })
    .await
}

async fn enforce_activity_timeout(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: crate::types::ExecutionId,
    reason: &TimeoutReason,
    circuit_breakers: Option<&crate::circuit_breaker::CircuitBreakerRegistry>,
    metrics: &(dyn MetricsRecorder + Send + Sync),
) -> HarvestResult<()> {
    let Some(activity_name) = task.activity_name.as_deref() else {
        return queue::fail_task(conn, task.id, &timeout_error("activity", reason)).await;
    };
    let error = timeout_error(activity_name, reason);

    // Did we actually append a timeout (vs. a no-op because the task already
    // moved on)? Only a real enforcement should count toward the breaker.
    let enforced = conn
        .transaction::<bool, HarvestError, _>(|conn| {
            let error = error.clone();
            async move {
                let history = lock_workflow_execution_and_load_history(conn, exec_id).await?;
                let Some(state) = task_state_for_update(conn, task.id).await? else {
                    return Ok(false);
                };
                if !expected_task_states_for_timeout(reason).contains(&state.as_str()) {
                    return Ok(false);
                }
                let activity_id =
                    match pending_activity_id_for_task(&history.events, task, activity_name) {
                        Ok(Some(activity_id)) => activity_id,
                        Ok(None) => return Ok(false),
                        Err(missing_error) => {
                            let fallback = missing_error.to_string();
                            queue::fail_task(conn, task.id, &fallback).await?;
                            return Ok(false);
                        }
                    };
                let timeout_event = WorkflowEvent::ActivityTimedOut {
                    activity_id,
                    timeout_type: reason.timeout_type(),
                };
                store::append_events(conn, exec_id, &[timeout_event], history.next_event_id)
                    .await?;
                queue::fail_task(conn, task.id, &error).await?;
                queue::wake_workflow_task(conn, exec_id).await?;
                Ok(true)
            }
            .scope_boxed()
        })
        .await?;

    // Circuit breaker (issue #369): a start-to-close / heartbeat timeout against
    // a protected downstream is a retryable, downstream-style failure that the
    // handler-result path never sees (the worker may be gone). Record it
    // out-of-band so a hanging downstream trips the breaker just like an
    // explicit error would.
    //
    // ScheduleToClose on a PENDING task means the activity sat in the queue
    // until its total deadline elapsed without any handler being dispatched.
    // No downstream call was made, so the breaker must NOT be fed — a backlog
    // spike would otherwise incorrectly open the circuit.
    // ScheduleToStart: task sat in the queue until the schedule-to-start window
    // expired without ever being dispatched — no downstream call was made.
    // ScheduleToClose on a PENDING task: total deadline expired before dispatch.
    // Neither case represents a downstream failure; suppress the breaker so a
    // queue backlog does not incorrectly open the circuit.
    let downstream_call_made = matches!(reason, TimeoutReason::ScheduleToStart)
        || (matches!(reason, TimeoutReason::ScheduleToClose) && task.state == "PENDING");
    if enforced
        && !downstream_call_made
        && let Some(breakers) = circuit_breakers
        && breakers.on_external_failure(activity_name, std::time::Instant::now())
            == Some(crate::circuit_breaker::CircuitTransition::Tripped)
    {
        metrics.record_circuit_tripped(activity_name);
    }
    Ok(())
}

async fn enforce_workflow_timeout(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: crate::types::ExecutionId,
    reason: &TimeoutReason,
    metrics: &(dyn MetricsRecorder + Send + Sync),
) -> HarvestResult<()> {
    let execution = load_workflow_execution(conn, exec_id).await?;
    let error = timeout_error(&execution.workflow_name, reason);
    let history = store::load_history(conn, exec_id).await?;
    let workflow_event = WorkflowEvent::WorkflowFailed {
        error: error.clone(),
    };

    let (deferred_starts, closed_children) = conn
        .transaction::<_, HarvestError, _>(|conn| {
            let error = error.clone();
            async move {
                store::append_events(conn, exec_id, &[workflow_event], history.next_event_id)
                    .await?;
                update_workflow_execution_timed_out(conn, exec_id, &error).await?;
                queue::fail_task(conn, task.id, &error).await?;
                let (mut deferred, closed_children) =
                    apply_parent_close_cascade(conn, exec_id).await?;
                let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                    conn,
                    exec_id,
                    crate::completion_trigger::TerminalState::TimedOut,
                    Some(metrics),
                )
                .await?;
                deferred.extend(triggers);
                if execution.parent_close_policy.is_none()
                    && let Some(parent_uuid) = execution.parent_id
                {
                    wake_parent_for_child_timeout(
                        conn,
                        execution_id_from_uuid(parent_uuid),
                        exec_id,
                        &error,
                    )
                    .await?;
                }
                Ok((deferred, closed_children))
            }
            .scope_boxed()
        })
        .await?;

    for start in deferred_starts {
        start.spawn();
    }

    for (child_id, child_name) in closed_children {
        if let Err(e) =
            check_and_report_unfinished_handlers(conn, child_id, &child_name, Some(metrics)).await
        {
            tracing::error!(
                child_id = %child_id,
                err = %e,
                "Failed to check and report unfinished handlers on cascaded child in workflow timeout"
            );
        }
    }

    if let Err(e) =
        check_and_report_unfinished_handlers(conn, exec_id, &execution.workflow_name, Some(metrics))
            .await
    {
        tracing::error!(
            exec_id = %exec_id,
            err = %e,
            "Failed to check and report unfinished handlers on workflow timeout"
        );
    }

    // Best-effort: count task-level timeouts toward the schedule auto-pause threshold.
    // Called AFTER the transaction commits to avoid aborting the transition on a
    // counter query failure.
    crate::scheduler::maybe_increment_schedule_failure_counter(
        conn,
        &execution.workflow_id,
        &execution.workflow_name,
        execution.schedule_id,
        execution.origin.as_deref(),
        metrics,
    )
    .await;

    Ok(())
}

/// Enforce schedule-to-close timeouts for pending external activity tasks.
///
/// Scans `harvest_external_tasks` for rows that are still `PENDING` but whose
/// `schedule_to_close_at` has elapsed.  For each expired row the function:
///
/// 1. Marks the row `TIMED_OUT`.
/// 2. Appends `ActivityTimedOut { timeout_type: ScheduleToClose }` to the
///    owning workflow's event history.
/// 3. Wakes the parked workflow task so it can process the timeout.
///
/// Returns the number of external tasks that were timed out.
///
/// # Errors
///
/// Returns the first database or persistence error encountered.
pub async fn enforce_external_task_timeouts(conn: &mut AsyncPgConnection) -> HarvestResult<usize> {
    let expired: Vec<ExternalTask> = harvest_external_tasks::table
        .filter(harvest_external_tasks::state.eq("PENDING"))
        .filter(harvest_external_tasks::schedule_to_close_at.lt(Utc::now()))
        .select(ExternalTask::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let count = expired.len();

    for task in &expired {
        let exec_id = execution_id_from_uuid(task.workflow_exec_id);
        let activity_id = ActivityExecId::from_uuid(task.activity_id);
        let task_id = task.id;

        let timeout_event = WorkflowEvent::ActivityTimedOut {
            activity_id,
            timeout_type: TimeoutType::ScheduleToClose,
        };

        let result = conn
            .transaction::<(), HarvestError, _>(|conn| {
                async move {
                    // Guard against two races:
                    // 1. complete/fail landed after our scan → state != PENDING
                    // 2. heartbeat extended the deadline after our scan →
                    //    schedule_to_close_at is now in the future
                    // Either way, 0 rows updated means we skip the timeout event.
                    let rows = diesel::update(
                        harvest_external_tasks::table
                            .find(task_id)
                            .filter(harvest_external_tasks::state.eq("PENDING"))
                            .filter(harvest_external_tasks::schedule_to_close_at.lt(Utc::now())),
                    )
                    .set((
                        harvest_external_tasks::state.eq("TIMED_OUT"),
                        harvest_external_tasks::updated_at.eq(Utc::now()),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                    if rows == 0 {
                        return Ok(());
                    }

                    store::append_single_event(conn, exec_id, timeout_event).await?;
                    queue::wake_workflow_task(conn, exec_id).await
                }
                .scope_boxed()
            })
            .await;

        if let Err(error) = result {
            tracing::error!(
                task_id = %task.id,
                exec_id = %exec_id,
                error = %error,
                "failed to enforce external task schedule-to-close timeout"
            );
            return Err(error);
        }
    }

    Ok(count)
}

/// Enforce execution-level timeouts for RUNNING workflow executions whose
/// `deadline_at` has elapsed (issue #243).
///
/// For each expired execution this function:
/// 1. Appends `WorkflowEvent::WorkflowExecutionTimedOut` to the execution history.
/// 2. Transitions the execution row to `TIMED_OUT` state.
/// 3. Cancels/fails the outstanding workflow task in `harvest_task_queue`.
/// 4. Notifies the parent workflow (for awaited children) via `ChildWorkflowFailed`.
/// 5. Applies parent-close policy to any running detached children.
///
/// Returns the number of executions that were timed out.
///
/// # Errors
///
/// Returns the first database or persistence error encountered.
#[allow(clippy::too_many_lines)]
pub async fn enforce_workflow_execution_timeouts(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    let now = Utc::now();
    let expired: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .filter(harvest_workflow_executions::deadline_at.is_not_null())
        .filter(harvest_workflow_executions::deadline_at.lt(Some(now)))
        .select(WorkflowExecution::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let count = expired.len();

    for execution in &expired {
        let exec_id = execution_id_from_uuid(execution.id);
        let deadline = execution
            .deadline_at
            .expect("workflow_execution_timeout_query guarantees deadline_at IS NOT NULL");
        let timed_out_at = Utc::now();

        let timeout_event = WorkflowEvent::WorkflowExecutionTimedOut {
            deadline,
            timed_out_at,
        };
        let error_msg = HarvestError::Timeout {
            timeout_type: TimeoutType::WorkflowExecution,
            task_name: execution.workflow_name.clone(),
        }
        .to_string();

        let parent_uuid = if execution.parent_close_policy.is_none() {
            execution.parent_id
        } else {
            None
        };
        let workflow_name = execution.workflow_name.clone();

        // true  = timeout transition was committed
        // false = row was already non-RUNNING; cascade must not run
        let result = commit_workflow_execution_timeout(
            conn,
            exec_id,
            parent_uuid,
            &timeout_event,
            &error_msg,
            Some(metrics),
        )
        .await;

        let (timed_out_applied, deferred_starts, closed_children) = match result {
            Ok((applied, deferred, closed)) => (applied, deferred, closed),
            Err(error) => {
                tracing::error!(
                    exec_id = %exec_id,
                    workflow_name = %workflow_name,
                    error = %error,
                    "failed to enforce workflow execution timeout"
                );
                return Err(error);
            }
        };

        if !timed_out_applied {
            // Row was already non-RUNNING; nothing to do.
            continue;
        }

        for start in deferred_starts {
            start.spawn();
        }

        for (child_id, child_name) in closed_children {
            if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
                conn,
                child_id,
                &child_name,
                Some(metrics),
            )
            .await
            {
                tracing::error!(
                    child_id = %child_id,
                    err = %e,
                    "Failed to check and report unfinished handlers on cascaded child in timeout"
                );
            }
        }

        if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
            conn,
            exec_id,
            &workflow_name,
            Some(metrics),
        )
        .await
        {
            tracing::error!(exec_id = %exec_id, err = %e, "Failed to check and report unfinished handlers");
        }

        tracing::warn!(
            exec_id = %exec_id,
            workflow_name = %workflow_name,
            deadline = %deadline,
            "workflow execution timed out"
        );

        metrics.record_workflow_timeout(&workflow_name, &execution.queue_name);
        metrics.record_workflow_terminal(
            &workflow_name,
            &execution.queue_name,
            crate::telemetry::WorkflowStatus::TimedOut,
        );

        // Best-effort: count execution timeouts toward the auto-pause threshold.
        // `workflow_id` encodes the schedule UUID so the update is scoped to the
        // triggering schedule and does not cross-contaminate sibling schedules.
        crate::scheduler::maybe_increment_schedule_failure_counter(
            conn,
            &execution.workflow_id,
            &workflow_name,
            execution.schedule_id,
            execution.origin.as_deref(),
            metrics,
        )
        .await;
    }

    Ok(count)
}

/// Detect and mark workflow executions that have exceeded their declared soft
/// SLA budget (issue #487).
///
/// The scanner atomically flips `sla_breached = true` and sets `sla_breached_at`
/// for every non-PAUSED execution whose `sla_deadline_at` has elapsed — measured
/// against `COALESCE(completed_at, NOW())` — and that has not yet been marked.
///
/// - RUNNING rows (no `completed_at`) are compared against the current time, as
///   before.
/// - Already-terminal rows (`COMPLETED` / `FAILED` / `CANCELLED` / `TIMED_OUT` /
///   `TERMINATED` / `CONTINUED_AS_NEW`) are compared against their **actual
///   terminal timestamp** (`completed_at`). This means a run that finished
///   *before* its
///   deadline never breaches (no false positive), while a run that crossed the
///   deadline and then went terminal within one scan interval is still caught
///   after the fact — covering completion, failure, cancel, terminate, timeout,
///   and continue-as-new uniformly without a separate per-path marker.
/// - PAUSED rows are excluded: pause suspends the SLA clock.
///
/// It emits `harvest.workflow.sla_breached{workflow, queue}` **exactly once per
/// run**.
///
/// **This function never terminates, cancels, fails, or otherwise alters the
/// lifecycle of the run.**  A breaching run that later completes still reaches
/// COMPLETED with its normal result.  No `WorkflowEvent` is appended and
/// `harvest_events` is left untouched (zero replay footprint, same posture as
/// query handlers).
///
/// Idempotency is guaranteed by the `WHERE sla_breached = false` guard combined
/// with the `RETURNING` clause: repeated scans and concurrent workers on the
/// same shard cannot double-count a breach.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn enforce_workflow_sla_breaches(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    use crate::schema::harvest_workflow_executions;
    use diesel::dsl::sql;
    use diesel::sql_types::Bool;

    let now = Utc::now();
    let breached: Vec<(uuid::Uuid, String, String)> =
        diesel::update(harvest_workflow_executions::table)
            // Exclude only PAUSED (pause suspends the SLA clock); RUNNING and all
            // terminal states are eligible.
            .filter(harvest_workflow_executions::state.ne("PAUSED"))
            .filter(harvest_workflow_executions::sla_deadline_at.is_not_null())
            .filter(harvest_workflow_executions::sla_breached.eq(false))
            // RUNNING rows compare against NOW(); terminal rows against their
            // actual completion instant, so finishing before the deadline never
            // counts as a breach.
            .filter(sql::<Bool>(
                "sla_deadline_at < COALESCE(completed_at, NOW())",
            ))
            .set((
                harvest_workflow_executions::sla_breached.eq(true),
                harvest_workflow_executions::sla_breached_at.eq(Some(now)),
            ))
            .returning((
                harvest_workflow_executions::id,
                harvest_workflow_executions::workflow_name,
                harvest_workflow_executions::queue_name,
            ))
            .get_results(conn)
            .await
            .map_err(crate::error::database_error)?;

    let count = breached.len();

    for (exec_uuid, workflow_name, queue_name) in &breached {
        tracing::warn!(
            exec_id = %exec_uuid,
            workflow_name = %workflow_name,
            queue = %queue_name,
            "soft SLA deadline exceeded — run continues to completion"
        );
        metrics.record_workflow_sla_breach(workflow_name, queue_name);
    }

    Ok(count)
}

/// Enforce all currently expired task timeouts against the database state.
///
/// This mutates queue rows and workflow history so timed-out tasks are not
/// retried indefinitely in the logs while the rest of the runtime remains
/// oblivious.
///
/// # Errors
///
/// Background outbox scanner that polls pending external signal requests,
/// attempts same-shard and cross-shard delivery via `GLOBAL_SHARDED_POOL`,
/// fails with `"target_unknown"` after `unknown_target_grace_window` has elapsed,
/// and wakes up the caller workflow.
#[allow(clippy::too_many_lines)]
pub async fn enforce_external_signals_outbox(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    unknown_target_grace_window: Duration,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
) -> HarvestResult<usize> {
    let mut count = 0;
    let codecs = crate::payload_codec::PayloadCodecs::default();

    let shards: Vec<i32> = if shard_assignments.is_empty() {
        vec![0]
    } else {
        shard_assignments.iter().map(|s| s.as_i32()).collect()
    };

    let mut excluded_event_ids: Vec<i64> = Vec::new();

    loop {
        let shards_clone = shards.clone();
        let codecs_clone = codecs.clone();
        let excluded_clone = excluded_event_ids.clone();

        let step_res: Result<Option<(bool, Option<i64>)>, HarvestError> = conn
            .transaction::<Option<(bool, Option<i64>)>, HarvestError, _>(|conn| {
                let shards = shards_clone;
                let codecs = codecs_clone;
                let excluded = excluded_clone;
                async move {
                    let sql = "SELECT e.* FROM harvest_events e \
                               INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id \
                               WHERE e.event_type = 'ExternalSignalRequested' \
                                 AND execs.state = 'RUNNING' \
                                 AND execs.shard_id = ANY($1) \
                                 AND (e.event_data->'data'->>'signal_id') IS NOT NULL \
                                 AND NOT (e.id = ANY($2)) \
                                 AND NOT EXISTS ( \
                                     SELECT 1 FROM harvest_events res \
                                     WHERE res.workflow_exec_id = e.workflow_exec_id \
                                       AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed') \
                                       AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id' \
                                 ) \
                               LIMIT 1 \
                               FOR UPDATE OF e SKIP LOCKED";

                    let row_opt: Option<crate::models::HarvestEvent> = diesel::sql_query(sql)
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Integer>, _>(&shards)
                        .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&excluded)
                        .get_result(conn)
                        .await
                        .optional()
                        .map_err(crate::error::database_error)?;

                    let Some(row) = row_opt else {
                        return Ok(None);
                    };

                    let caller_exec_id = crate::types::ExecutionId::from_uuid(row.workflow_exec_id);

                    let event = match codecs.decode_event(row.event_data.clone()) {
                        Ok(WorkflowEvent::ExternalSignalRequested {
                            signal_id,
                            target,
                            signal_name,
                            payload,
                            idempotency_key,
                        }) => (signal_id, target, signal_name, payload, idempotency_key),
                        Ok(other) => {
                            tracing::error!(event = ?other, "outbox sweep: query returned non-ExternalSignalRequested event");
                            return Ok(Some((false, Some(row.id))));
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "outbox sweep: failed to decode event_data");
                            return Ok(Some((false, Some(row.id))));
                        }
                    };

                    let (signal_id, target, signal_name, payload, idempotency_key) = event;

                    let age = Utc::now() - row.timestamp;
                    let grace_chrono = chrono::Duration::from_std(unknown_target_grace_window)
                        .map_or(chrono::Duration::MAX, |d| d);
                    let grace_expired = age > grace_chrono;

                    // A NotFound delivery attempt only becomes a permanent
                    // `target_unknown` failure once the grace window has elapsed.
                    // Within the window we leave the row pending (retried next
                    // sweep) so a target that starts slightly after the request —
                    // or that the outbox first sees after worker downtime/backlog —
                    // is still signalled rather than wrongly reported unknown.
                    let not_found_terminal = || {
                        grace_expired.then(|| WorkflowEvent::ExternalSignalFailed {
                            signal_id,
                            reason_code: "target_unknown".to_string(),
                        })
                    };

                    // Try to route target using the config's sharded pool if configured
                    let active_sharded_pool = sharded_pool
                        .clone()
                        .or_else(|| {
                            crate::shard::GLOBAL_SHARDED_POOL.read().ok()
                                .and_then(|lock| lock.clone())
                        });

                    let same_pool = active_sharded_pool.as_ref().is_none_or(|pool| {
                        if let (Some(t_pool), Some(c_pool)) = (
                            pool.exact_pool_for_execution(target),
                            pool.exact_pool_for_execution(caller_exec_id),
                        ) {
                            std::ptr::eq(t_pool, c_pool)
                        } else {
                            false
                        }
                    });

                    let terminal_opt = if same_pool {
                        match crate::signal::send_signal_idempotent(
                            conn,
                            target,
                            &signal_name,
                            payload.clone(),
                            idempotency_key.as_deref(),
                        )
                        .await
                        {
                            // Delivered or deduped (idempotency-key collision):
                            // both mean the signal landed exactly once.
                            Ok(_delivered_or_deduped) => {
                                Some(WorkflowEvent::ExternalSignalDelivered { signal_id })
                            }
                            Err(HarvestError::NotFound(_)) => not_found_terminal(),
                            Err(HarvestError::Database(e)) => {
                                tracing::error!(error = %e, "outbox sweep: db error during local signal delivery");
                                None
                            }
                            Err(_) => Some(WorkflowEvent::ExternalSignalFailed {
                                signal_id,
                                reason_code: "target_terminal".to_string(),
                            }),
                        }
                    } else {
                        // Different pools, so we must have a target_pool resolved
                        let Some(pool) = active_sharded_pool
                            .as_ref()
                            .and_then(|p| p.exact_pool_for_execution(target))
                        else {
                            tracing::warn!(
                                target_shard = %target.shard(),
                                "outbox sweep: target shard is not configured locally; leaving row locked/pending for other workers"
                            );
                            return Ok(Some((false, Some(row.id))));
                        };

                        let mut target_conn = match pool.get().await {
                            Ok(c) => c,
                            Err(e) => {
                                tracing::error!(error = %e, "outbox sweep: failed to acquire target connection");
                                return Ok(Some((false, Some(row.id))));
                            }
                        };

                        match crate::signal::send_signal_idempotent(
                            &mut target_conn,
                            target,
                            &signal_name,
                            payload.clone(),
                            idempotency_key.as_deref(),
                        )
                        .await
                        {
                            // Delivered or deduped (idempotency-key collision):
                            // both mean the signal landed exactly once.
                            Ok(_delivered_or_deduped) => {
                                Some(WorkflowEvent::ExternalSignalDelivered { signal_id })
                            }
                            Err(HarvestError::NotFound(_)) => not_found_terminal(),
                            Err(HarvestError::Database(e)) => {
                                tracing::error!(error = %e, "outbox sweep: db error during remote signal delivery");
                                None
                            }
                            Err(_) => Some(WorkflowEvent::ExternalSignalFailed {
                                signal_id,
                                reason_code: "target_terminal".to_string(),
                            }),
                        }
                    };

                    if let Some(terminal_event) = terminal_opt {
                        let outcome = match &terminal_event {
                            WorkflowEvent::ExternalSignalDelivered { .. } => "delivered",
                            _ => "failed",
                        };
                        let reason_code = match &terminal_event {
                            WorkflowEvent::ExternalSignalFailed {
                                reason_code, ..
                            } => Some(reason_code.clone()),
                            _ => None,
                        };

                        let history = lock_workflow_execution_and_load_history(conn, caller_exec_id).await?;
                        store::append_events(
                            conn,
                            caller_exec_id,
                            &[terminal_event],
                            history.next_event_id,
                        )
                        .await?;
                        queue::wake_workflow_task(conn, caller_exec_id).await?;

                        metrics.record_external_signal_sent(outcome, reason_code.as_deref());
                        Ok(Some((true, None)))
                    } else {
                        Ok(Some((false, Some(row.id))))
                    }
                }
                .scope_boxed()
            })
            .await;

        match step_res {
            Ok(Some((processed, skipped_id))) => {
                if processed {
                    count += 1;
                }
                if let Some(id) = skipped_id {
                    excluded_event_ids.push(id);
                }
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                tracing::error!(error = %e, "outbox sweep error in transaction step");
                return Err(e);
            }
        }
    }

    Ok(count)
}

/// Scan for `ExternalCancelRequested` events without a matching terminal event
/// and attempt cancel delivery (issue #492).
///
/// Mirrors `enforce_external_signals_outbox` but calls `cancel_workflow_execution`
/// instead of `signal::send_signal`. Already-terminal targets resolve as
/// `ExternalCancelDelivered` (no-op success); missing targets after the grace
/// window resolve as `ExternalCancelFailed { reason_code: "target_unknown" }`.
///
/// Per-step outcome: (processed, `skipped_event_id`, deferred trigger starts,
/// (`workflow_name`, `queue_name`) of targets newly cancelled). The deferred
/// starts and terminal metrics are spawned/recorded only after the step
/// transaction commits so trigger workflows never start for a cancellation that
/// later rolls back (issue #492).
type CancelStepOutcome = (
    bool,
    Option<i64>,
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(String, String)>,
    Vec<(ExecutionId, String)>,
);

#[allow(clippy::too_many_lines)]
pub async fn enforce_external_cancels_outbox(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    unknown_target_grace_window: Duration,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
) -> HarvestResult<usize> {
    let mut count = 0;
    let codecs = crate::payload_codec::PayloadCodecs::default();

    let shards: Vec<i32> = if shard_assignments.is_empty() {
        vec![0]
    } else {
        shard_assignments.iter().map(|s| s.as_i32()).collect()
    };

    let mut excluded_event_ids: Vec<i64> = Vec::new();

    loop {
        let shards_clone = shards.clone();
        let codecs_clone = codecs.clone();
        let excluded_clone = excluded_event_ids.clone();

        let step_res: Result<Option<CancelStepOutcome>, HarvestError> = conn
            .transaction::<Option<CancelStepOutcome>, HarvestError, _>(|conn| {
                let shards = shards_clone;
                let codecs = codecs_clone;
                let excluded = excluded_clone;
                async move {
                    let sql = "SELECT e.* FROM harvest_events e \
                               INNER JOIN harvest_workflow_executions execs ON e.workflow_exec_id = execs.id \
                               WHERE e.event_type = 'ExternalCancelRequested' \
                                 AND execs.state = 'RUNNING' \
                                 AND execs.shard_id = ANY($1) \
                                 AND (e.event_data->'data'->>'cancel_id') IS NOT NULL \
                                 AND NOT (e.id = ANY($2)) \
                                 AND NOT EXISTS ( \
                                     SELECT 1 FROM harvest_events res \
                                     WHERE res.workflow_exec_id = e.workflow_exec_id \
                                       AND res.event_type IN ('ExternalCancelDelivered', 'ExternalCancelFailed') \
                                       AND res.event_data->'data'->>'cancel_id' = e.event_data->'data'->>'cancel_id' \
                                 ) \
                               LIMIT 1 \
                               FOR UPDATE OF e SKIP LOCKED";

                    let row_opt: Option<crate::models::HarvestEvent> = diesel::sql_query(sql)
                        .bind::<diesel::sql_types::Array<diesel::sql_types::Integer>, _>(&shards)
                        .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&excluded)
                        .get_result(conn)
                        .await
                        .optional()
                        .map_err(crate::error::database_error)?;

                    let Some(row) = row_opt else {
                        return Ok(None);
                    };

                    let caller_exec_id = crate::types::ExecutionId::from_uuid(row.workflow_exec_id);

                    let (cancel_id, target) = match codecs.decode_event(row.event_data.clone()) {
                        Ok(WorkflowEvent::ExternalCancelRequested { cancel_id, target }) => {
                            (cancel_id, target)
                        }
                        Ok(other) => {
                            tracing::error!(event = ?other, "cancel outbox sweep: query returned non-ExternalCancelRequested event");
                            return Ok(Some((false, Some(row.id), Vec::new(), Vec::new(), Vec::new())));
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "cancel outbox sweep: failed to decode event_data");
                            return Ok(Some((false, Some(row.id), Vec::new(), Vec::new(), Vec::new())));
                        }
                    };

                    let age = Utc::now() - row.timestamp;
                    let grace_chrono = chrono::Duration::from_std(unknown_target_grace_window)
                        .map_or(chrono::Duration::MAX, |d| d);
                    let grace_expired = age > grace_chrono;

                    // A NotFound delivery attempt only becomes a permanent
                    // `target_unknown` failure once the grace window has elapsed.
                    // Within the window we leave the row pending (retried next
                    // sweep) so a target that starts slightly after the request —
                    // or that the outbox first sees after worker downtime/backlog —
                    // is still cancelled rather than wrongly reported unknown.
                    // (issue #492)
                    let not_found_terminal = || {
                        grace_expired.then(|| WorkflowEvent::ExternalCancelFailed {
                            cancel_id,
                            reason_code: "target_unknown".to_string(),
                        })
                    };

                    let active_sharded_pool = sharded_pool
                        .clone()
                        .or_else(|| {
                            crate::shard::GLOBAL_SHARDED_POOL.read().ok()
                                .and_then(|lock| lock.clone())
                        });

                    let same_pool = active_sharded_pool.as_ref().is_none_or(|pool| {
                        if let (Some(t_pool), Some(c_pool)) = (
                            pool.exact_pool_for_execution(target),
                            pool.exact_pool_for_execution(caller_exec_id),
                        ) {
                            std::ptr::eq(t_pool, c_pool)
                        } else {
                            false
                        }
                    });

                    // Completion-trigger / cascade follow-up starts + terminal
                    // metrics from a same-pool cancellation, spawned/recorded only
                    // after this step transaction commits (issue #492). The
                    // cross-shard branch cancels on an independent target
                    // connection, so its triggers spawn correctly against that
                    // connection's own committed transaction and need no deferral.
                    let mut deferred_starts: Vec<
                        crate::completion_trigger::DeferredTriggerStart,
                    > = Vec::new();
                    let mut cancel_metrics: Vec<(String, String)> = Vec::new();
                    let mut deferred_checks: Vec<(ExecutionId, String)> = Vec::new();

                    let terminal_opt = if same_pool {
                        match cancel_workflow_execution_collect(
                            conn,
                            target,
                            "cancelled by external request",
                        )
                        .await
                        {
                            Err(HarvestError::NotFound(_)) => not_found_terminal(),
                            Err(HarvestError::Database(e)) => {
                                tracing::error!(error = %e, "cancel outbox sweep: db error");
                                None
                            }
                            Ok((_cancelled, deferred, checks, metrics_opt)) => {
                                deferred_starts.extend(deferred);
                                deferred_checks.extend(checks);
                                if let Some(m) = metrics_opt {
                                    cancel_metrics.push(m);
                                }
                                Some(WorkflowEvent::ExternalCancelDelivered { cancel_id })
                            }
                            // Other Err (already terminal) = no-op success.
                            Err(_) => {
                                Some(WorkflowEvent::ExternalCancelDelivered { cancel_id })
                            }
                        }
                    } else {
                        let Some(pool) = active_sharded_pool
                            .as_ref()
                            .and_then(|p| p.exact_pool_for_execution(target))
                        else {
                            tracing::warn!(
                                target_shard = %target.shard(),
                                "cancel outbox sweep: target shard not configured locally; skipping"
                            );
                            return Ok(Some((false, Some(row.id), Vec::new(), Vec::new(), Vec::new())));
                        };

                        let mut target_conn = match pool.get().await {
                            Ok(c) => c,
                            Err(e) => {
                                tracing::error!(error = %e, "cancel outbox sweep: failed to acquire target connection");
                                return Ok(Some((false, Some(row.id), Vec::new(), Vec::new(), Vec::new())));
                            }
                        };

                        match cancel_workflow_execution(
                            &mut target_conn,
                            target,
                            "cancelled by external request",
                            metrics,
                        )
                        .await
                        {
                            Err(HarvestError::NotFound(_)) => not_found_terminal(),
                            Err(HarvestError::Database(e)) => {
                                tracing::error!(
                                    error = %e,
                                    "cancel outbox sweep: db error (remote shard)"
                                );
                                None
                            }
                            // Ok or other Err (already terminal) = no-op success.
                            Ok(_) | Err(_) => {
                                Some(WorkflowEvent::ExternalCancelDelivered { cancel_id })
                            }
                        }
                    };

                    if let Some(terminal_event) = terminal_opt {
                        let outcome = match &terminal_event {
                            WorkflowEvent::ExternalCancelDelivered { .. } => "delivered",
                            _ => "failed",
                        };
                        let reason_code = match &terminal_event {
                            WorkflowEvent::ExternalCancelFailed { reason_code, .. } => {
                                Some(reason_code.clone())
                            }
                            _ => None,
                        };

                        let history = lock_workflow_execution_and_load_history(conn, caller_exec_id).await?;
                        store::append_events(
                            conn,
                            caller_exec_id,
                            &[terminal_event],
                            history.next_event_id,
                        )
                        .await?;
                        queue::wake_workflow_task(conn, caller_exec_id).await?;
                        metrics.record_external_cancel_sent(outcome, reason_code.as_deref());
                        Ok(Some((true, None, deferred_starts, cancel_metrics, deferred_checks)))
                    } else {
                        Ok(Some((false, Some(row.id), deferred_starts, cancel_metrics, deferred_checks)))
                    }
                }
                .scope_boxed()
            })
            .await;

        match step_res {
            Ok(Some((processed, skipped_id, deferred_starts, cancel_metrics, deferred_checks))) => {
                // The step transaction has committed: now spawn trigger/cascade
                // follow-up starts and record terminal metrics for same-pool
                // cancellations (issue #492).
                for start in deferred_starts {
                    start.spawn();
                }
                for check in deferred_checks {
                    let _ = check_and_report_unfinished_handlers(
                        conn,
                        check.0,
                        &check.1,
                        Some(metrics),
                    )
                    .await;
                }
                for (workflow_name, queue_name) in cancel_metrics {
                    metrics.record_workflow_terminal(
                        &workflow_name,
                        &queue_name,
                        crate::telemetry::WorkflowStatus::Cancelled,
                    );
                }
                if processed {
                    count += 1;
                }
                if let Some(id) = skipped_id {
                    excluded_event_ids.push(id);
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!(error = %e, "cancel outbox sweep error in transaction step");
                return Err(e);
            }
        }
    }

    Ok(count)
}

/// Returns the first database or persistence error encountered.
pub async fn enforce_timeouts_once(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    unknown_target_grace_window: Duration,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    circuit_breakers: Option<&crate::circuit_breaker::CircuitBreakerRegistry>,
    max_workflow_history_events: Option<u64>,
) -> HarvestResult<usize> {
    let timed_out = find_timed_out_tasks(conn).await?;
    let mut count = timed_out.len();

    for (task, reason) in timed_out {
        let result = match (task.task_type.as_str(), task.workflow_exec_id) {
            ("activity", Some(exec_uuid)) => {
                enforce_activity_timeout(
                    conn,
                    &task,
                    execution_id_from_uuid(exec_uuid),
                    &reason,
                    circuit_breakers,
                    metrics,
                )
                .await
            }
            ("workflow", Some(exec_uuid)) => {
                enforce_workflow_timeout(
                    conn,
                    &task,
                    execution_id_from_uuid(exec_uuid),
                    &reason,
                    metrics,
                )
                .await
            }
            _ => queue::fail_task(conn, task.id, &timeout_error(&task.task_type, &reason)).await,
        };

        if let Err(error) = result {
            tracing::error!(
                task_id = %task.id,
                queue = %task.queue_name,
                reason = %reason,
                error = %error,
                "failed to enforce timed-out task"
            );
            return Err(error);
        }
    }

    count += enforce_external_task_timeouts(conn).await?;
    // Run the soft-SLA scan *before* the hard execution-timeout pass: if a run
    // crosses both its SLA and hard deadline within one tick, the breach must be
    // recorded while the row is still RUNNING — the hard-timeout pass then
    // transitions it to TIMED_OUT and the SLA scan would otherwise skip it.
    count += enforce_workflow_sla_breaches(conn, metrics).await?;
    count += enforce_workflow_execution_timeouts(conn, metrics).await?;
    count += enforce_external_signals_outbox(
        conn,
        metrics,
        unknown_target_grace_window,
        sharded_pool,
        shard_assignments,
    )
    .await?;
    count += enforce_external_cancels_outbox(
        conn,
        metrics,
        unknown_target_grace_window,
        sharded_pool,
        shard_assignments,
    )
    .await?;
    count += crate::completion_trigger::enforce_completion_triggers_outbox(
        conn,
        sharded_pool,
        shard_assignments,
    )
    .await?;
    count +=
        crate::debounce::fire_due_debounced_starts(conn, sharded_pool, shard_assignments, metrics)
            .await?;
    count +=
        crate::event_batch::fire_due_event_batches(conn, sharded_pool, shard_assignments, metrics)
            .await?;
    count += crate::completion_callback::fire_due_completion_deliveries(
        conn,
        sharded_pool,
        shard_assignments,
    )
    .await?;
    if let Some(ceiling) = max_workflow_history_events {
        count += enforce_workflow_history_ceiling(conn, ceiling, metrics).await?;
    }
    Ok(count)
}

/// Spawn a background task that periodically checks for timed-out tasks.
///
/// The checker runs every `interval` duration and enforces any timed-out tasks
/// it finds by mutating queue state and workflow history.
///
/// Stops when the cancellation token is triggered.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn spawn_timeout_checker(
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
    interval: Duration,
    telemetry: std::sync::Arc<crate::telemetry::TelemetryConfig>,
    unknown_target_grace_window: Duration,
    sharded_pool: Option<crate::shard::ShardedDbPool>,
    shard_assignments: Vec<crate::types::ShardId>,
    circuit_breakers: std::sync::Arc<crate::circuit_breaker::CircuitBreakerRegistry>,
    max_workflow_history_events: Option<u64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    tracing::debug!("timeout checker cancelled");
                    break;
                }
                () = tokio::time::sleep(interval) => {
                    // Check for timed out tasks
                }
            }

            match pool.get().await {
                Ok(mut conn) => match enforce_timeouts_once(
                    &mut conn,
                    &*telemetry.metrics,
                    unknown_target_grace_window,
                    &sharded_pool,
                    &shard_assignments,
                    Some(&circuit_breakers),
                    max_workflow_history_events,
                )
                .await
                {
                    Ok(enforced_count) if enforced_count > 0 => {
                        tracing::warn!(enforced_count, "enforced timed-out tasks");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "failed to enforce timed-out tasks");
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "failed to acquire DB connection for timeout check");
                }
            }

            if cancel.is_cancelled() {
                break;
            }
        }
    })
}

/// Terminate RUNNING workflow executions whose durable event count has reached
/// or exceeded the operator-configured hard ceiling (issue #493).
///
/// The ceiling is set via `HarvestBuilder::max_workflow_history_events`.  No
/// new `WorkflowEvent` variant is introduced: each affected execution receives
/// an ordinary `WorkflowFailed` event with a machine-readable error string of
/// the form `"history_ceiling_exceeded: event count {n} >= ceiling {c}"`, and
/// its state transitions to `FAILED`.  Outstanding task-queue rows are
/// cancelled and any awaiting parent is notified.
///
/// Idempotency: the inner transaction re-checks `state = 'RUNNING'` under a
/// `FOR UPDATE` lock, so a concurrent completion or a duplicate scanner tick
/// can never double-append the failure event.
///
/// Returns the number of executions that were terminated.
///
/// # Errors
///
/// Returns the first database or persistence error encountered.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
pub async fn enforce_workflow_history_ceiling(
    conn: &mut AsyncPgConnection,
    ceiling: u64,
    metrics: &(dyn MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    use diesel::sql_types::{BigInt, Nullable, Text, Uuid as SqlUuid};

    // Find RUNNING executions whose recorded event count >= ceiling.
    // The event_count is selected inline to avoid an N+1 query pattern.
    #[derive(diesel::QueryableByName)]
    struct OversizedRow {
        #[diesel(sql_type = SqlUuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = Text)]
        workflow_id: String,
        #[diesel(sql_type = Text)]
        workflow_name: String,
        #[diesel(sql_type = Text)]
        queue_name: String,
        #[diesel(sql_type = Nullable<SqlUuid>)]
        parent_id: Option<uuid::Uuid>,
        #[diesel(sql_type = Nullable<Text>)]
        parent_close_policy: Option<String>,
        #[diesel(sql_type = BigInt)]
        event_count: i64,
    }

    let ceiling_i64 = i64::try_from(ceiling).unwrap_or(i64::MAX);
    let oversized: Vec<OversizedRow> = diesel::sql_query(
        "SELECT id, workflow_id, workflow_name, queue_name, parent_id, parent_close_policy, \
         (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id)::bigint AS event_count \
         FROM harvest_workflow_executions \
         WHERE state = 'RUNNING' \
         AND (SELECT COUNT(*) FROM harvest_events WHERE workflow_exec_id = harvest_workflow_executions.id) >= $1",
    )
    .bind::<BigInt, _>(ceiling_i64)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    let count = oversized.len();

    for row in &oversized {
        let exec_id = execution_id_from_uuid(row.id);
        let event_count = row.event_count;

        let error_msg =
            format!("history_ceiling_exceeded: event count {event_count} >= ceiling {ceiling}");
        let fail_event = WorkflowEvent::WorkflowFailed {
            error: error_msg.clone(),
        };

        let parent_uuid = if row.parent_close_policy.is_none() {
            row.parent_id
        } else {
            None
        };
        let workflow_name = row.workflow_name.clone();
        let queue_name = row.queue_name.clone();

        let (applied, deferred_starts, closed_children) = conn
            .transaction::<(
                bool,
                Vec<crate::completion_trigger::DeferredTriggerStart>,
                Vec<(ExecutionId, String)>,
            ), HarvestError, _>(|conn| {
                let fail_event = fail_event.clone();
                let error_msg = error_msg.clone();
                async move {
                    // Re-check state under FOR UPDATE to guard against concurrent
                    // completion or a duplicate scanner tick.
                    let current_state: Option<String> = harvest_workflow_executions::table
                        .find(row.id)
                        .for_update()
                        .select(harvest_workflow_executions::state)
                        .first(conn)
                        .await
                        .optional()
                        .map_err(crate::error::database_error)?;

                    if current_state.as_deref() != Some("RUNNING") {
                        return Ok((false, Vec::new(), Vec::new()));
                    }

                    store::append_single_event(conn, exec_id, fail_event).await?;

                    // Transition to FAILED state.
                    diesel::update(harvest_workflow_executions::table.find(row.id))
                        .set((
                            harvest_workflow_executions::state.eq("FAILED"),
                            harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                            harvest_workflow_executions::error.eq(Some(error_msg.clone())),
                            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
                        ))
                        .execute(conn)
                        .await
                        .map_err(crate::error::database_error)?;

                    // Cancel outstanding task queue rows.
                    diesel::update(
                        harvest_task_queue::table
                            .filter(harvest_task_queue::workflow_exec_id.eq(row.id))
                            .filter(
                                harvest_task_queue::state
                                    .eq("PENDING")
                                    .or(harvest_task_queue::state.eq("RUNNING")),
                            ),
                    )
                    .set((
                        harvest_task_queue::state.eq("FAILED"),
                        harvest_task_queue::error.eq(Some(&error_msg)),
                        harvest_task_queue::completed_at.eq(Some(Utc::now())),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                    if let Some(parent_uuid) = parent_uuid {
                        wake_parent_for_child_timeout(
                            conn,
                            execution_id_from_uuid(parent_uuid),
                            exec_id,
                            &error_msg,
                        )
                        .await?;
                    }

                    let (mut deferred, closed_children) =
                        apply_parent_close_cascade(conn, exec_id).await?;
                    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                        conn,
                        exec_id,
                        crate::completion_trigger::TerminalState::Failed,
                        Some(metrics),
                    )
                    .await?;
                    deferred.extend(triggers);
                    Ok((true, deferred, closed_children))
                }
                .scope_boxed()
            })
            .await?;

        if !applied {
            continue;
        }

        for start in deferred_starts {
            start.spawn();
        }

        tracing::warn!(
            exec_id = %exec_id,
            workflow_name = %workflow_name,
            ceiling = ceiling,
            "workflow execution terminated: history ceiling exceeded"
        );

        metrics.record_workflow_terminal(
            &workflow_name,
            &queue_name,
            crate::telemetry::WorkflowStatus::Failed,
        );

        if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
            conn,
            exec_id,
            &workflow_name,
            Some(metrics),
        )
        .await
        {
            tracing::error!(
                exec_id = %exec_id,
                err = %e,
                "Failed to check and report unfinished handlers on history ceiling enforcement"
            );
        }

        for (child_id, child_name) in closed_children {
            if let Err(e) = crate::execution::check_and_report_unfinished_handlers(
                conn,
                child_id,
                &child_name,
                Some(metrics),
            )
            .await
            {
                tracing::error!(
                    child_id = %child_id,
                    err = %e,
                    "Failed to check and report unfinished handlers on cascaded child execution in history ceiling"
                );
            }
        }

        // Best-effort: count ceiling failures toward the schedule auto-pause threshold.
        // Called after the transaction commits so a counter query failure cannot
        // roll back the terminal transition.
        crate::scheduler::maybe_increment_schedule_failure_counter(
            conn,
            &row.workflow_id,
            &workflow_name,
            None, // schedule_id not available in OversizedRow
            None, // origin not available in OversizedRow; NULL treated as 'scheduled' (backward-compat)
            metrics,
        )
        .await;
    }

    Ok(count)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_timeout_query_references_correct_table() {
        let sql = heartbeat_timeout_query();
        assert!(
            sql.contains("harvest_task_queue"),
            "should query harvest_task_queue"
        );
        assert!(sql.contains("RUNNING"), "should filter for RUNNING state");
        assert!(
            sql.contains("heartbeat_timeout"),
            "should reference heartbeat_timeout column"
        );
        assert!(
            sql.contains("last_heartbeat_at"),
            "should reference last_heartbeat_at column"
        );
    }

    #[test]
    fn start_to_close_timeout_query_references_correct_columns() {
        let sql = start_to_close_timeout_query();
        assert!(
            sql.contains("harvest_task_queue"),
            "should query harvest_task_queue"
        );
        assert!(sql.contains("RUNNING"), "should filter for RUNNING state");
        assert!(
            sql.contains("start_to_close"),
            "should reference start_to_close column"
        );
        assert!(
            sql.contains("started_at"),
            "should reference started_at column"
        );
    }

    #[test]
    fn schedule_to_start_timeout_query_references_correct_columns() {
        let sql = schedule_to_start_timeout_query();
        assert!(
            sql.contains("harvest_task_queue"),
            "should query harvest_task_queue"
        );
        assert!(sql.contains("PENDING"), "should filter for PENDING state");
        assert!(
            sql.contains("schedule_to_start"),
            "should reference schedule_to_start column"
        );
        assert!(
            sql.contains("scheduled_at"),
            "should reference scheduled_at column"
        );
    }

    #[test]
    fn schedule_to_close_timeout_query_references_correct_columns() {
        let sql = schedule_to_close_timeout_query();
        assert!(
            sql.contains("harvest_task_queue"),
            "should query harvest_task_queue"
        );
        assert!(
            sql.contains("RUNNING") && sql.contains("PENDING"),
            "should filter for both RUNNING and PENDING states"
        );
        assert!(
            sql.contains("schedule_to_close_at"),
            "should reference schedule_to_close_at column"
        );
        assert!(sql.contains("NOW()"), "should compare against NOW()");
    }

    #[test]
    fn timeout_reason_schedule_to_close_maps_correctly() {
        assert_eq!(
            TimeoutReason::ScheduleToClose.to_string(),
            "ScheduleToClose"
        );
        assert_eq!(
            TimeoutReason::ScheduleToClose.timeout_type(),
            TimeoutType::ScheduleToClose
        );
        // Both RUNNING and PENDING are valid states for ScheduleToClose
        let states = expected_task_states_for_timeout(&TimeoutReason::ScheduleToClose);
        assert!(states.contains(&"RUNNING"), "RUNNING must be a valid state");
        assert!(states.contains(&"PENDING"), "PENDING must be a valid state");
        // Other reasons must NOT include both states
        let heartbeat_states = expected_task_states_for_timeout(&TimeoutReason::Heartbeat);
        assert_eq!(heartbeat_states, &["RUNNING"]);
        let sched_start_states = expected_task_states_for_timeout(&TimeoutReason::ScheduleToStart);
        assert_eq!(sched_start_states, &["PENDING"]);
    }

    #[test]
    fn timeout_reason_display() {
        assert_eq!(TimeoutReason::Heartbeat.to_string(), "Heartbeat");
        assert_eq!(TimeoutReason::StartToClose.to_string(), "StartToClose");
        assert_eq!(
            TimeoutReason::ScheduleToStart.to_string(),
            "ScheduleToStart"
        );
    }

    #[test]
    fn timeout_reason_equality() {
        assert_eq!(TimeoutReason::Heartbeat, TimeoutReason::Heartbeat);
        assert_ne!(TimeoutReason::Heartbeat, TimeoutReason::StartToClose);
    }

    // ── Workflow execution timeout query tests (issue #243) ──────────────────

    #[test]
    fn workflow_execution_timeout_query_references_correct_table_and_columns() {
        let sql = workflow_execution_timeout_query();
        assert!(
            sql.contains("harvest_workflow_executions"),
            "must query harvest_workflow_executions"
        );
        assert!(sql.contains("RUNNING"), "must filter for RUNNING state");
        assert!(
            sql.contains("deadline_at"),
            "must reference deadline_at column"
        );
        assert!(sql.contains("NOW()"), "must compare against NOW()");
    }

    // ── Soft SLA breach scanner tests (issue #487) ────────────────────────────

    #[test]
    fn sla_breached_metric_has_correct_name() {
        assert_eq!(
            crate::telemetry::METRIC_WORKFLOW_SLA_BREACHED,
            "harvest.workflow.sla_breached"
        );
    }

    #[test]
    fn record_workflow_sla_breach_is_callable_on_no_op_recorder() {
        use crate::telemetry::MetricsRecorder;
        struct NoOp;
        impl MetricsRecorder for NoOp {}
        // Must not panic; default no-op implementation.
        NoOp.record_workflow_sla_breach("my_workflow", "default");
    }

    #[test]
    fn sla_breach_spy_records_one_call_per_breach() {
        use std::sync::Mutex;

        #[derive(Default)]
        struct SpyRecorder {
            breaches: Mutex<Vec<(String, String)>>,
        }
        impl crate::telemetry::MetricsRecorder for SpyRecorder {
            fn record_workflow_sla_breach(&self, workflow_name: &str, queue: &str) {
                self.breaches
                    .lock()
                    .unwrap()
                    .push((workflow_name.to_owned(), queue.to_owned()));
            }
        }

        let spy = SpyRecorder::default();
        spy.record_workflow_sla_breach("slow_workflow", "priority-queue");
        spy.record_workflow_sla_breach("slow_workflow", "priority-queue");

        let b = spy.breaches.lock().unwrap().clone();
        assert_eq!(b.len(), 2, "spy records every call; idempotency is DB-side");
        assert_eq!(
            b[0],
            ("slow_workflow".to_owned(), "priority-queue".to_owned())
        );
    }
}
