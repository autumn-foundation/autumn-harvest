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

use std::time::Duration;

use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::deadpool::Pool;
use tokio_util::sync::CancellationToken;

use crate::error::{HarvestError, HarvestResult};
use crate::models::TaskQueueItem;

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

    // Heartbeat timeouts
    let heartbeat_tasks: Vec<TaskQueueItem> = diesel::sql_query(heartbeat_timeout_query())
        .load(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;
    for task in heartbeat_tasks {
        results.push((task, TimeoutReason::Heartbeat));
    }

    // Start-to-close timeouts
    let start_close_tasks: Vec<TaskQueueItem> = diesel::sql_query(start_to_close_timeout_query())
        .load(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;
    for task in start_close_tasks {
        results.push((task, TimeoutReason::StartToClose));
    }

    // Schedule-to-start timeouts
    let sched_start_tasks: Vec<TaskQueueItem> =
        diesel::sql_query(schedule_to_start_timeout_query())
            .load(conn)
            .await
            .map_err(|e| HarvestError::Database(e.to_string()))?;
    for task in sched_start_tasks {
        results.push((task, TimeoutReason::ScheduleToStart));
    }

    Ok(results)
}

/// Spawn a background task that periodically checks for timed-out tasks.
///
/// The checker runs every `interval` duration and logs any timed-out tasks
/// it finds. The caller is responsible for handling the timed-out tasks
/// (e.g., failing them, requeueing for retry).
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
                Ok(mut conn) => match find_timed_out_tasks(&mut conn).await {
                    Ok(timed_out) => {
                        for (task, reason) in &timed_out {
                            tracing::warn!(
                                task_id = %task.id,
                                queue = %task.queue_name,
                                reason = %reason,
                                "task timed out"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to check for timed-out tasks");
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
