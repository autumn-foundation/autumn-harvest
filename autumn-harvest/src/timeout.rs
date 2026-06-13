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
use crate::execution::apply_parent_close_cascade;
use crate::models::{ExternalTask, TaskQueueItem, WorkflowExecution};
use crate::schema::{harvest_external_tasks, harvest_task_queue, harvest_workflow_executions};
use crate::telemetry::MetricsRecorder;
use crate::types::ActivityExecId;
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

    let updated = diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
        .set((
            dsl::state.eq("TIMED_OUT"),
            dsl::output.eq(None::<serde_json::Value>),
            dsl::error.eq(Some(error.to_string())),
            dsl::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(HarvestError::NotFound(format!(
            "workflow execution {exec_id}"
        )));
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
) -> HarvestResult<(bool, Vec<crate::completion_trigger::DeferredTriggerStart>)> {
    conn.transaction::<(bool, Vec<crate::completion_trigger::DeferredTriggerStart>), HarvestError, _>(|conn| {
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
                _ => return Ok((false, Vec::new())),
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

            let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
            let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                conn,
                exec_id,
                crate::completion_trigger::TerminalState::TimedOut,
                metrics,
            )
            .await?;
            deferred.extend(triggers);
            Ok((true, deferred))
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
    let downstream_call_made =
        matches!(reason, TimeoutReason::ScheduleToClose) && task.state == "PENDING";
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

    let deferred_starts = conn
        .transaction::<Vec<crate::completion_trigger::DeferredTriggerStart>, HarvestError, _>(
            |conn| {
                let error = error.clone();
                async move {
                    store::append_events(conn, exec_id, &[workflow_event], history.next_event_id)
                        .await?;
                    update_workflow_execution_timed_out(conn, exec_id, &error).await?;
                    queue::fail_task(conn, task.id, &error).await?;
                    let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
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
                    Ok(deferred)
                }
                .scope_boxed()
            },
        )
        .await?;

    for start in deferred_starts {
        start.spawn();
    }

    // Best-effort: count task-level timeouts toward the schedule auto-pause threshold.
    // Called AFTER the transaction commits to avoid aborting the transition on a
    // counter query failure.
    crate::scheduler::maybe_increment_schedule_failure_counter(
        conn,
        &execution.workflow_id,
        &execution.workflow_name,
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

        let (timed_out_applied, deferred_starts) = match result {
            Ok((applied, deferred)) => (applied, deferred),
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
/// for every RUNNING execution whose `sla_deadline_at` has elapsed and that has
/// not yet been marked.  (Only RUNNING is scanned — mirroring the #243
/// execution-timeout scanner: a PAUSED run must not breach mid-pause, and
/// SUSPENDED is not a persisted state.)  It emits
/// `harvest.workflow.sla_breached{workflow, queue}` **exactly once per run**.
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

    let now = Utc::now();
    let breached: Vec<(uuid::Uuid, String, String)> =
        diesel::update(harvest_workflow_executions::table)
            .filter(harvest_workflow_executions::state.eq("RUNNING"))
            .filter(harvest_workflow_executions::sla_deadline_at.is_not_null())
            .filter(harvest_workflow_executions::sla_deadline_at.lt(Some(now)))
            .filter(harvest_workflow_executions::sla_breached.eq(false))
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
                        }) => (signal_id, target, signal_name, payload),
                        Ok(other) => {
                            tracing::error!(event = ?other, "outbox sweep: query returned non-ExternalSignalRequested event");
                            return Ok(Some((false, Some(row.id))));
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "outbox sweep: failed to decode event_data");
                            return Ok(Some((false, Some(row.id))));
                        }
                    };

                    let (signal_id, target, signal_name, payload) = event;

                    let age = Utc::now() - row.timestamp;
                    let grace_chrono = chrono::Duration::from_std(unknown_target_grace_window)
                        .map_or(chrono::Duration::MAX, |d| d);

                    if age > grace_chrono {
                        // Grace window expired!
                        let failed_event = WorkflowEvent::ExternalSignalFailed {
                            signal_id,
                            reason_code: "target_unknown".to_string(),
                        };

                        let history = lock_workflow_execution_and_load_history(conn, caller_exec_id).await?;
                        store::append_events(
                            conn,
                            caller_exec_id,
                            &[failed_event],
                            history.next_event_id,
                        )
                        .await?;
                        queue::wake_workflow_task(conn, caller_exec_id).await?;

                        metrics.record_external_signal_sent("failed", Some("target_unknown"));
                        return Ok(Some((true, None)));
                    }

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
                        match crate::signal::send_signal(
                            conn,
                            target,
                            &signal_name,
                            payload.clone(),
                        )
                        .await
                        {
                            Ok(()) => {
                                Some(WorkflowEvent::ExternalSignalDelivered { signal_id })
                            }
                            Err(HarvestError::NotFound(_)) => {
                                None
                            }
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

                        match crate::signal::send_signal(
                            &mut target_conn,
                            target,
                            &signal_name,
                            payload.clone(),
                        )
                        .await
                        {
                            Ok(()) => {
                                Some(WorkflowEvent::ExternalSignalDelivered { signal_id })
                            }
                            Err(HarvestError::NotFound(_)) => {
                                None
                            }
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

/// Returns the first database or persistence error encountered.
pub async fn enforce_timeouts_once(
    conn: &mut AsyncPgConnection,
    metrics: &(dyn MetricsRecorder + Send + Sync),
    unknown_target_grace_window: Duration,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    circuit_breakers: Option<&crate::circuit_breaker::CircuitBreakerRegistry>,
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
    count += crate::completion_trigger::enforce_completion_triggers_outbox(
        conn,
        sharded_pool,
        shard_assignments,
    )
    .await?;
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
