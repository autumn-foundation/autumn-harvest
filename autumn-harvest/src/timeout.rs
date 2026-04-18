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
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use scoped_futures::ScopedFutureExt;
use tokio_util::sync::CancellationToken;

use crate::error::{HarvestError, HarvestResult, TimeoutType};
use crate::event::WorkflowEvent;
use crate::models::{TaskQueueItem, WorkflowExecution};
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
}

impl std::fmt::Display for TimeoutReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Heartbeat => write!(f, "Heartbeat"),
            Self::StartToClose => write!(f, "StartToClose"),
            Self::ScheduleToStart => write!(f, "ScheduleToStart"),
        }
    }
}

impl TimeoutReason {
    const fn timeout_type(&self) -> TimeoutType {
        match self {
            Self::Heartbeat => TimeoutType::Heartbeat,
            Self::StartToClose => TimeoutType::StartToClose,
            Self::ScheduleToStart => TimeoutType::ScheduleToStart,
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

    let pending = history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if name == activity_name && !terminal_ids.contains(activity_id) => Some(*activity_id),
            _ => None,
        })
        .collect::<Vec<_>>();

    match pending.as_slice() {
        [activity_id] => Ok(*activity_id),
        [] => Err(HarvestError::NotFound(format!(
            "no pending scheduled activity '{activity_name}' in workflow history"
        ))),
        _ => Err(HarvestError::NonDeterministic(format!(
            "multiple pending scheduled activities named '{activity_name}' found in history"
        ))),
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
    let parent_history = store::load_history(conn, parent_exec_id).await?;
    let event = WorkflowEvent::ChildWorkflowFailed {
        child_id: child_exec_id,
        error: error.to_string(),
    };
    store::append_events(conn, parent_exec_id, &[event], parent_history.next_event_id).await?;
    queue::wake_workflow_task(conn, parent_exec_id).await
}

async fn enforce_activity_timeout(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: crate::types::ExecutionId,
    reason: &TimeoutReason,
) -> HarvestResult<()> {
    let Some(activity_name) = task.activity_name.as_deref() else {
        return queue::fail_task(conn, task.id, &timeout_error("activity", reason)).await;
    };
    let error = timeout_error(activity_name, reason);
    let history = store::load_history(conn, exec_id).await?;

    let activity_id = match find_pending_scheduled_activity(&history.events, activity_name) {
        Ok(activity_id) => activity_id,
        Err(missing_error) => {
            let fallback = missing_error.to_string();
            queue::fail_task(conn, task.id, &fallback).await?;
            return Ok(());
        }
    };

    let timeout_event = WorkflowEvent::ActivityTimedOut {
        activity_id,
        timeout_type: reason.timeout_type(),
    };

    conn.transaction::<(), HarvestError, _>(|conn| {
        let error = error.clone();
        async move {
            store::append_events(conn, exec_id, &[timeout_event], history.next_event_id).await?;
            queue::fail_task(conn, task.id, &error).await?;
            queue::wake_workflow_task(conn, exec_id).await
        }
        .scope_boxed()
    })
    .await
}

async fn enforce_workflow_timeout(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: crate::types::ExecutionId,
    reason: &TimeoutReason,
) -> HarvestResult<()> {
    let execution = load_workflow_execution(conn, exec_id).await?;
    let error = timeout_error(&execution.workflow_name, reason);
    let history = store::load_history(conn, exec_id).await?;
    let workflow_event = WorkflowEvent::WorkflowFailed {
        error: error.clone(),
    };

    conn.transaction::<(), HarvestError, _>(|conn| {
        let error = error.clone();
        async move {
            store::append_events(conn, exec_id, &[workflow_event], history.next_event_id).await?;
            update_workflow_execution_timed_out(conn, exec_id, &error).await?;
            queue::fail_task(conn, task.id, &error).await?;
            if let Some(parent_uuid) = execution.parent_id {
                wake_parent_for_child_timeout(
                    conn,
                    execution_id_from_uuid(parent_uuid),
                    exec_id,
                    &error,
                )
                .await?;
            }
            Ok(())
        }
        .scope_boxed()
    })
    .await
}

/// Enforce all currently expired task timeouts against the database state.
///
/// This mutates queue rows and workflow history so timed-out tasks are not
/// retried indefinitely in the logs while the rest of the runtime remains
/// oblivious.
///
/// # Errors
///
/// Returns the first database or persistence error encountered.
pub async fn enforce_timeouts_once(conn: &mut AsyncPgConnection) -> HarvestResult<usize> {
    let timed_out = find_timed_out_tasks(conn).await?;
    let count = timed_out.len();

    for (task, reason) in timed_out {
        let result = match (task.task_type.as_str(), task.workflow_exec_id) {
            ("activity", Some(exec_uuid)) => {
                enforce_activity_timeout(conn, &task, execution_id_from_uuid(exec_uuid), &reason)
                    .await
            }
            ("workflow", Some(exec_uuid)) => {
                enforce_workflow_timeout(conn, &task, execution_id_from_uuid(exec_uuid), &reason)
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

    Ok(count)
}

/// Spawn a background task that periodically checks for timed-out tasks.
///
/// The checker runs every `interval` duration and enforces any timed-out tasks
/// it finds by mutating queue state and workflow history.
///
/// Stops when the cancellation token is triggered.
#[must_use]
pub fn spawn_timeout_checker(
    pool: Pool<AsyncPgConnection>,
    cancel: CancellationToken,
    interval: Duration,
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
                Ok(mut conn) => match enforce_timeouts_once(&mut conn).await {
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
}
