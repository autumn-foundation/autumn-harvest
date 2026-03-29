//! Postgres-backed task queue with `SKIP LOCKED` claiming.
//!
//! Workers poll their assigned queues via [`claim_task()`] which atomically
//! moves a `PENDING` row to `RUNNING` using `FOR UPDATE SKIP LOCKED` --
//! no two workers will ever claim the same task.

use chrono::{Duration, Utc};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::error::{HarvestError, HarvestResult};
use crate::models::{NewTaskQueueItem, TaskQueueItem};

// ---------------------------------------------------------------------------
// TaskType
// ---------------------------------------------------------------------------

/// Discriminator for the kind of task enqueued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    /// A top-level workflow execution.
    Workflow,
    /// A single activity invocation within a workflow.
    Activity,
}

impl TaskType {
    /// Returns the string representation stored in the `task_type` column.
    ///
    /// Must match the DB CHECK constraint: `('workflow','activity')`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Workflow => "workflow",
            Self::Activity => "activity",
        }
    }
}

impl std::fmt::Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// EnqueueParams
// ---------------------------------------------------------------------------

/// Parameters for enqueuing a new task onto the work queue.
#[derive(Debug, Clone)]
pub struct EnqueueParams {
    pub queue_name: String,
    pub task_type: TaskType,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<String>,
    pub input: serde_json::Value,
    pub priority: i32,
    pub max_attempts: i32,
    pub scheduled_at: chrono::DateTime<Utc>,
    pub heartbeat_timeout: Option<Duration>,
    pub start_to_close: Option<Duration>,
    pub schedule_to_start: Option<Duration>,
    pub retry_policy: Option<serde_json::Value>,
}

impl EnqueueParams {
    /// Create minimal enqueue params with sensible defaults.
    #[must_use]
    pub fn new(
        queue_name: impl Into<String>,
        task_type: TaskType,
        input: serde_json::Value,
    ) -> Self {
        Self {
            queue_name: queue_name.into(),
            task_type,
            workflow_exec_id: None,
            activity_name: None,
            input,
            priority: 0,
            max_attempts: 3,
            scheduled_at: Utc::now(),
            heartbeat_timeout: None,
            start_to_close: None,
            schedule_to_start: None,
            retry_policy: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Queue operations
// ---------------------------------------------------------------------------

/// Insert a new task into the work queue and return its ID.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on insert failure.
pub async fn enqueue(conn: &mut AsyncPgConnection, params: &EnqueueParams) -> HarvestResult<Uuid> {
    use crate::schema::harvest_task_queue;

    let task_id = Uuid::new_v4();

    let row = NewTaskQueueItem {
        id: task_id,
        queue_name: &params.queue_name,
        task_type: params.task_type.as_str(),
        workflow_exec_id: params.workflow_exec_id,
        activity_name: params.activity_name.as_deref(),
        input: params.input.clone(),
        priority: params.priority,
        max_attempts: params.max_attempts,
        scheduled_at: params.scheduled_at,
        heartbeat_timeout: params.heartbeat_timeout,
        start_to_close: params.start_to_close,
        schedule_to_start: params.schedule_to_start,
        retry_policy: params.retry_policy.clone(),
    };

    diesel::insert_into(harvest_task_queue::table)
        .values(&row)
        .execute(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;

    Ok(task_id)
}

/// Atomically claim the highest-priority pending task from the given queues.
///
/// Uses `FOR UPDATE SKIP LOCKED` so concurrent workers never contend on the
/// same row. Returns `None` if no eligible task is available.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
pub async fn claim_task(
    conn: &mut AsyncPgConnection,
    queues: &[String],
    worker_id: &str,
) -> HarvestResult<Option<TaskQueueItem>> {
    let result: Vec<TaskQueueItem> = diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET state = 'RUNNING', worker_id = $1, started_at = NOW(), attempt = attempt + 1 \
         WHERE id = ( \
             SELECT id FROM harvest_task_queue \
             WHERE queue_name = ANY($2) AND state = 'PENDING' AND scheduled_at <= NOW() \
             ORDER BY priority DESC, scheduled_at ASC \
             LIMIT 1 FOR UPDATE SKIP LOCKED \
         ) RETURNING *",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .load(conn)
    .await
    .map_err(|e| HarvestError::Database(e.to_string()))?;

    Ok(result.into_iter().next())
}

/// Mark a task as completed with the given output.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on update failure.
pub async fn complete_task(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    output: serde_json::Value,
) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    diesel::update(dsl::harvest_task_queue.find(task_id))
        .set((
            dsl::state.eq("COMPLETED"),
            dsl::output.eq(Some(output)),
            dsl::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;

    Ok(())
}

/// Mark a task as failed with the given error message.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on update failure.
pub async fn fail_task(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    error: &str,
) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    diesel::update(dsl::harvest_task_queue.find(task_id))
        .set((
            dsl::state.eq("FAILED"),
            dsl::error.eq(Some(error)),
            dsl::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;

    Ok(())
}

/// Update the `last_heartbeat_at` timestamp for a running task.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on update failure.
pub async fn record_heartbeat(conn: &mut AsyncPgConnection, task_id: Uuid) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    diesel::update(dsl::harvest_task_queue.find(task_id))
        .set(dsl::last_heartbeat_at.eq(Some(Utc::now())))
        .execute(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;

    Ok(())
}

/// Reset a task to `PENDING` with a future `scheduled_at` for retry.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on update failure.
pub async fn requeue_for_retry(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    delay: Duration,
) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    let next_run = Utc::now() + delay;

    diesel::update(dsl::harvest_task_queue.find(task_id))
        .set((
            dsl::state.eq("PENDING"),
            dsl::worker_id.eq(None::<String>),
            dsl::started_at.eq(None::<chrono::DateTime<Utc>>),
            dsl::scheduled_at.eq(next_run),
        ))
        .execute(conn)
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_params_builds_correctly() {
        let params = EnqueueParams::new(
            "email-queue",
            TaskType::Activity,
            serde_json::json!({"to": "alice"}),
        );

        assert_eq!(params.queue_name, "email-queue");
        assert_eq!(params.task_type, TaskType::Activity);
        assert_eq!(params.input, serde_json::json!({"to": "alice"}));
        assert_eq!(params.priority, 0);
        assert_eq!(params.max_attempts, 3);
        assert!(params.workflow_exec_id.is_none());
        assert!(params.activity_name.is_none());
        assert!(params.heartbeat_timeout.is_none());
        assert!(params.start_to_close.is_none());
        assert!(params.schedule_to_start.is_none());
        assert!(params.retry_policy.is_none());
    }

    #[test]
    fn task_type_display() {
        assert_eq!(TaskType::Workflow.as_str(), "workflow");
        assert_eq!(TaskType::Activity.as_str(), "activity");
        assert_eq!(format!("{}", TaskType::Workflow), "workflow");
        assert_eq!(format!("{}", TaskType::Activity), "activity");
    }

    #[test]
    fn enqueue_params_with_overrides() {
        let mut params = EnqueueParams::new("billing", TaskType::Workflow, serde_json::json!(null));
        params.priority = 10;
        params.max_attempts = 5;
        params.workflow_exec_id = Some(Uuid::new_v4());

        assert_eq!(params.priority, 10);
        assert_eq!(params.max_attempts, 5);
        assert!(params.workflow_exec_id.is_some());
    }
}
