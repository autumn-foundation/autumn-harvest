//! Postgres-backed task queue with `SKIP LOCKED` claiming.
//!
//! Workers poll their assigned queues via [`claim_task()`] which atomically
//! moves a `PENDING` row to `RUNNING` using `FOR UPDATE SKIP LOCKED` --
//! no two workers will ever claim the same task.

use std::time::Duration as StdDuration;

use chrono::{Duration, Utc};
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use crate::error::HarvestResult;
use crate::models::{NewTaskQueueItem, TaskQueueItem};
use crate::telemetry::TraceContextCarrier;
use crate::types::ExecutionId;

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

const IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE: Duration = Duration::seconds(5);

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
    /// Pin this task to a specific worker for best-effort cache locality.
    ///
    /// When set together with [`Self::sticky_timeout`], the task is offered to
    /// this worker preferentially for `sticky_timeout` after it becomes
    /// claimable. Once the window expires, any worker may claim it.
    pub sticky_worker_id: Option<String>,
    /// Duration of the sticky preference window. Stored on the row so that
    /// [`wake_workflow_task()`] can refresh `sticky_until` to the same value
    /// on each transition back to PENDING without needing external config.
    pub sticky_timeout: Option<StdDuration>,
    /// W3C tracecontext carrier propagated across the queue boundary so the
    /// worker can resume the enqueuing process's trace.
    pub trace_context: Option<TraceContextCarrier>,
    /// Cluster-wide concurrency group key. When set together with
    /// [`Self::max_concurrent`], the claim query enforces that at most
    /// `max_concurrent` tasks with this key are `RUNNING` at any instant.
    /// `None` = no per-key cap; only the worker-level semaphore applies.
    pub concurrency_key: Option<String>,
    /// Maximum number of concurrent RUNNING tasks for the `concurrency_key`.
    /// Stored on each row so the claim query can enforce the cap without
    /// application-layer input per poll.
    pub max_concurrent: Option<u32>,
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
            // Default immediate tasks slightly into the past to tolerate small
            // host/Postgres clock skew when workers claim with `scheduled_at <= NOW()`.
            scheduled_at: Utc::now() - IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE,
            heartbeat_timeout: None,
            start_to_close: None,
            schedule_to_start: None,
            retry_policy: None,
            sticky_worker_id: None,
            sticky_timeout: None,
            trace_context: None,
            concurrency_key: None,
            max_concurrent: None,
        }
    }

    /// Pin this task to the given worker for the duration of `timeout`.
    ///
    /// Both fields are required together -- passing either alone is a no-op.
    #[must_use]
    pub fn with_sticky(mut self, worker_id: impl Into<String>, timeout: StdDuration) -> Self {
        self.sticky_worker_id = Some(worker_id.into());
        self.sticky_timeout = Some(timeout);
        self
    }

    /// Attach a trace context carrier so the worker can resume the trace
    /// started by whoever enqueued this task.
    #[must_use]
    pub fn with_trace_context(mut self, carrier: TraceContextCarrier) -> Self {
        self.trace_context = Some(carrier);
        self
    }
}

// ---------------------------------------------------------------------------
// Queue operations
// ---------------------------------------------------------------------------

/// Insert a new task into the work queue and return its ID.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on insert failure.
pub async fn enqueue(conn: &mut AsyncPgConnection, params: &EnqueueParams) -> HarvestResult<Uuid> {
    use crate::schema::harvest_task_queue;

    let task_id = Uuid::new_v4();

    // Sticky pin: only valid when both worker_id and timeout are present so the
    // check constraint `sticky_worker_id IS NULL <=> sticky_until IS NULL` holds.
    // sticky_until is computed below via DB NOW() in a follow-up UPDATE so it
    // agrees with the clock used by `claim_task` and `wake_workflow_task`.
    let sticky = match (params.sticky_worker_id.as_deref(), params.sticky_timeout) {
        (Some(worker), Some(timeout)) => {
            let chrono_timeout = chrono::Duration::from_std(timeout).map_err(|_| {
                crate::error::HarvestError::Config(
                    "sticky_timeout exceeds chrono duration range".to_string(),
                )
            })?;
            Some((worker, chrono_timeout))
        }
        _ => None,
    };

    let concurrency_cap = params
        .max_concurrent
        .map(|n| i32::try_from(n).unwrap_or(i32::MAX));

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
        sticky_worker_id: None,
        sticky_until: None,
        sticky_timeout: None,
        trace_context: params
            .trace_context
            .as_ref()
            .and_then(TraceContextCarrier::to_json),
        concurrency_key: params.concurrency_key.as_deref(),
        concurrency_cap,
    };

    diesel::insert_into(harvest_task_queue::table)
        .values(&row)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    if let Some((worker_id, timeout)) = sticky {
        diesel::sql_query(
            "UPDATE harvest_task_queue \
             SET sticky_worker_id = $2, \
                 sticky_until = NOW() + $3, \
                 sticky_timeout = $3 \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .bind::<diesel::sql_types::Interval, _>(timeout)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    }

    crate::notify::notify_task_enqueued(conn, &params.queue_name, task_id).await?;

    Ok(task_id)
}

/// Atomically claim the highest-priority pending task from the given queues.
///
/// Uses `FOR UPDATE SKIP LOCKED` so concurrent workers never contend on the
/// same row. Returns `None` if no eligible task is available.
///
/// # Sticky routing
///
/// When a row has `sticky_worker_id` set and `sticky_until > NOW()`, only that
/// worker may claim it. Once `sticky_until` elapses, any worker becomes
/// eligible, so a crashed or slow sticky worker never blocks progress.
/// Within the eligible set, rows pinned to the caller are sorted ahead of
/// unpinned rows to maximize cache locality.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn claim_task(
    conn: &mut AsyncPgConnection,
    queues: &[String],
    worker_id: &str,
) -> HarvestResult<Option<TaskQueueItem>> {
    // The concurrency-cap predicate is a correlated subquery that counts how
    // many tasks with the same concurrency_key are currently RUNNING. When
    // the count >= concurrency_cap the row is skipped (NOT EXISTS is false),
    // deferring it to the next poll cycle.
    //
    // Race-condition protection: pg_try_advisory_xact_lock serializes claim
    // attempts for a given key across concurrent workers. Without it, two
    // workers that both observe N < cap in the same window could each claim a
    // task and momentarily exceed the cap. The advisory lock is transaction-
    // scoped (auto-released on commit/rollback), so the serialization window
    // is only as wide as the claim transaction itself — typically sub-ms.
    // Different keys use independent lock slots; uncapped (NULL-key) tasks are
    // completely unaffected (short-circuit branch).
    //
    // The partial index harvest_task_queue_concurrency_key_running makes the
    // inner SELECT fast: it only scans RUNNING rows with a non-NULL key.
    let result: Vec<TaskQueueItem> = diesel::sql_query(
        "UPDATE harvest_task_queue \
         SET state = 'RUNNING', worker_id = $1, started_at = NOW(), attempt = attempt + 1 \
         WHERE id = ( \
             SELECT id FROM harvest_task_queue \
             WHERE queue_name = ANY($2) \
               AND state = 'PENDING' \
               AND scheduled_at <= NOW() \
               AND ( \
                   sticky_worker_id IS NULL \
                   OR sticky_worker_id = $1 \
                   OR sticky_until IS NULL \
                   OR sticky_until <= NOW() \
               ) \
               AND ( \
                   concurrency_key IS NULL \
                   OR ( \
                       pg_try_advisory_xact_lock(hashtext(concurrency_key)::bigint) \
                       AND NOT EXISTS ( \
                           SELECT 1 FROM harvest_task_queue inner_q \
                           WHERE inner_q.concurrency_key = harvest_task_queue.concurrency_key \
                             AND inner_q.state = 'RUNNING' \
                           HAVING COUNT(*) >= harvest_task_queue.concurrency_cap \
                       ) \
                   ) \
               ) \
             ORDER BY \
                 CASE \
                     WHEN sticky_worker_id = $1 AND sticky_until > NOW() THEN 1 \
                     ELSE 0 \
                 END DESC, \
                 priority DESC, \
                 scheduled_at ASC \
             LIMIT 1 FOR UPDATE SKIP LOCKED \
         ) RETURNING *",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(result.into_iter().next())
}

// ---------------------------------------------------------------------------
// Concurrency-key stats
// ---------------------------------------------------------------------------

/// Live stats for a single concurrency group key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConcurrencyKeyStats {
    /// The concurrency group key.
    pub key: String,
    /// Declared maximum concurrent tasks for this key.
    pub max_concurrent: i32,
    /// Number of tasks currently in `RUNNING` state for this key.
    pub in_flight: i64,
    /// Number of tasks in `PENDING` state for this key (may be deferred by
    /// the cap if `in_flight >= max_concurrent`).
    pub pending: i64,
}

/// Return live concurrency stats for all keys visible in the given queues.
///
/// Only rows where `concurrency_key IS NOT NULL` contribute. Results are
/// aggregated per key across all matching queues on this shard.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn concurrency_key_stats(
    conn: &mut AsyncPgConnection,
    queues: &[String],
) -> HarvestResult<Vec<ConcurrencyKeyStats>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        key: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        max_concurrent: i32,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        in_flight: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        pending: i64,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT \
             concurrency_key AS key, \
             MAX(concurrency_cap)::INT4 AS max_concurrent, \
             COUNT(*) FILTER (WHERE state = 'RUNNING') AS in_flight, \
             COUNT(*) FILTER (WHERE state = 'PENDING') AS pending \
         FROM harvest_task_queue \
         WHERE concurrency_key IS NOT NULL \
           AND concurrency_cap IS NOT NULL \
           AND queue_name = ANY($1) \
           AND state IN ('RUNNING', 'PENDING') \
         GROUP BY concurrency_key",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| ConcurrencyKeyStats {
            key: r.key,
            max_concurrent: r.max_concurrent,
            in_flight: r.in_flight,
            pending: r.pending,
        })
        .collect())
}

/// Mark a task as completed with the given output.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn complete_task(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    output: serde_json::Value,
) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    let updated = diesel::update(
        dsl::harvest_task_queue
            .find(task_id)
            .filter(dsl::state.eq("RUNNING")),
    )
    .set((
        dsl::state.eq("COMPLETED"),
        dsl::output.eq(Some(output)),
        dsl::completed_at.eq(Some(Utc::now())),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(crate::error::HarvestError::NotFound(format!(
            "task queue item {task_id} is not running"
        )));
    }

    Ok(())
}

/// Mark a task as failed with the given error message.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn fail_task(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    error: &str,
) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    let updated = diesel::update(
        dsl::harvest_task_queue
            .find(task_id)
            .filter(dsl::state.eq_any(["PENDING", "RUNNING"])),
    )
    .set((
        dsl::state.eq("FAILED"),
        dsl::error.eq(Some(error)),
        dsl::completed_at.eq(Some(Utc::now())),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(crate::error::HarvestError::NotFound(format!(
            "task queue item {task_id} is not pending or running"
        )));
    }

    Ok(())
}

/// Mark all pending or running tasks for a workflow execution as failed.
///
/// This is used by workflow cancellation to drain both queued and currently
/// claimed work. Late-running workers may still finish their local future, but
/// their completion writes will fail because the queue row is no longer open.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) on
/// update failure.
pub async fn fail_open_tasks_for_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    error: &str,
) -> HarvestResult<usize> {
    use crate::schema::harvest_task_queue::dsl;

    diesel::update(
        dsl::harvest_task_queue
            .filter(dsl::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .filter(dsl::state.eq_any(["PENDING", "RUNNING"])),
    )
    .set((
        dsl::state.eq("FAILED"),
        dsl::error.eq(Some(error.to_string())),
        dsl::completed_at.eq(Some(Utc::now())),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)
}

/// Count pending tasks per queue for observability / metrics.
///
/// Returns one row per queue name (unseen queues are omitted). Only tasks in
/// state `PENDING` that are already eligible (`scheduled_at <= NOW()`) are
/// counted — this matches the slice that [`claim_task`] competes over.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn queue_depths(
    conn: &mut AsyncPgConnection,
    queues: &[String],
) -> HarvestResult<Vec<(String, i64)>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        depth: i64,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT queue_name, COUNT(*)::BIGINT AS depth \
         FROM harvest_task_queue \
         WHERE queue_name = ANY($1) \
           AND state = 'PENDING' \
           AND scheduled_at <= NOW() \
         GROUP BY queue_name",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows.into_iter().map(|r| (r.queue_name, r.depth)).collect())
}

/// Update the `last_heartbeat_at` timestamp for a running task.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn record_heartbeat(conn: &mut AsyncPgConnection, task_id: Uuid) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    let updated = diesel::update(
        dsl::harvest_task_queue
            .find(task_id)
            .filter(dsl::state.eq("RUNNING")),
    )
    .set(dsl::last_heartbeat_at.eq(Some(Utc::now())))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(crate::error::HarvestError::NotFound(format!(
            "task queue item {task_id} is not running"
        )));
    }

    Ok(())
}

/// Reset a task to `PENDING` with a future `scheduled_at` for retry.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn requeue_for_retry(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    delay: Duration,
) -> HarvestResult<()> {
    let next_run = Utc::now() + delay;
    reschedule_task(conn, task_id, next_run).await
}

/// Reset a task to `PENDING` at an explicit timestamp.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn reschedule_task(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    scheduled_at: chrono::DateTime<Utc>,
) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    let queue_name = diesel::update(
        dsl::harvest_task_queue
            .find(task_id)
            .filter(dsl::state.eq("RUNNING")),
    )
    .set((
        dsl::state.eq("PENDING"),
        dsl::worker_id.eq(None::<String>),
        dsl::started_at.eq(None::<chrono::DateTime<Utc>>),
        dsl::last_heartbeat_at.eq(None::<chrono::DateTime<Utc>>),
        dsl::scheduled_at.eq(scheduled_at),
    ))
    .returning(dsl::queue_name)
    .get_result::<String>(conn)
    .await
    .optional()
    .map_err(crate::error::database_error)?
    .ok_or_else(|| {
        crate::error::HarvestError::NotFound(format!("task queue item {task_id} is not running"))
    })?;

    crate::notify::notify_task_enqueued(conn, &queue_name, task_id).await?;

    Ok(())
}

/// Hint for sticky cross-worker routing when parking or enqueueing a task.
///
/// When both fields are set, the task is pinned to `worker_id` for `timeout`
/// so the worker's in-process LRU cache can service follow-up replays without
/// reloading history from Postgres. A `None` hint leaves the task unpinned.
#[derive(Debug, Clone, Copy)]
pub struct StickyHint<'a> {
    pub worker_id: &'a str,
    pub timeout: StdDuration,
}

impl<'a> StickyHint<'a> {
    /// Create a new sticky hint.
    #[must_use]
    pub const fn new(worker_id: &'a str, timeout: StdDuration) -> Self {
        Self { worker_id, timeout }
    }

    fn chrono_timeout(self) -> HarvestResult<chrono::Duration> {
        chrono::Duration::from_std(self.timeout).map_err(|_| {
            crate::error::HarvestError::Config(
                "sticky_timeout exceeds chrono duration range".to_string(),
            )
        })
    }
}

/// Pin a task row to a specific worker for best-effort sticky routing.
///
/// Overwrites any existing sticky affinity on the row with the new worker
/// and a fresh `sticky_until = NOW() + hint.timeout`. Use this after calls
/// like [`reschedule_task()`] or [`enqueue()`] when the caller wants the
/// target worker to preferentially claim the task once it becomes due.
///
/// Passing `None` clears any existing pin.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure or
/// [`crate::error::HarvestError::NotFound`] if the row does not exist.
pub async fn set_task_sticky_affinity(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    sticky: Option<StickyHint<'_>>,
) -> HarvestResult<()> {
    // Use DB NOW() for sticky_until so all rows agree on a single clock with
    // `claim_task` and `wake_workflow_task`. See `park_workflow_task` for the
    // rationale.
    let updated = if let Some(hint) = sticky {
        let timeout = hint.chrono_timeout()?;
        diesel::sql_query(
            "UPDATE harvest_task_queue \
             SET sticky_worker_id = $2, \
                 sticky_until = NOW() + $3, \
                 sticky_timeout = $3 \
             WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(hint.worker_id)
        .bind::<diesel::sql_types::Interval, _>(timeout)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?
    } else {
        use crate::schema::harvest_task_queue::dsl;
        diesel::update(dsl::harvest_task_queue.find(task_id))
            .set((
                dsl::sticky_worker_id.eq(None::<String>),
                dsl::sticky_until.eq(None::<chrono::DateTime<Utc>>),
                dsl::sticky_timeout.eq(None::<chrono::Duration>),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?
    };

    if updated == 0 {
        return Err(crate::error::HarvestError::NotFound(format!(
            "task queue item {task_id} does not exist"
        )));
    }

    Ok(())
}

/// Mark a running workflow task as parked while it waits on an external event.
///
/// Parked tasks stay in `RUNNING` state so they remain attached to the same
/// workflow execution, but their worker ownership and start timestamp are cleared
/// so wake-up paths can distinguish them from actively executing workflow tasks.
///
/// If `sticky` is supplied, the row is pinned to the given worker for the
/// configured duration. This lets the next wake-up preferentially land back on
/// the worker that just produced the park -- the same worker whose in-process
/// LRU cache holds the workflow state.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn park_workflow_task(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    sticky: Option<StickyHint<'_>>,
) -> HarvestResult<()> {
    // Use raw SQL when applying a sticky hint so `sticky_until` is computed
    // from Postgres `NOW()` -- the same clock used by `wake_workflow_task` and
    // `claim_task`. Mixing host-clock and DB-clock timestamps on the same row
    // breaks comparisons under testcontainers / cross-host clock skew.
    let updated = if let Some(hint) = sticky {
        let timeout = hint.chrono_timeout()?;
        diesel::sql_query(
            "UPDATE harvest_task_queue \
             SET worker_id = NULL, \
                 started_at = NULL, \
                 sticky_worker_id = $2, \
                 sticky_until = NOW() + $3, \
                 sticky_timeout = $3 \
             WHERE id = $1 \
               AND task_type = 'workflow' \
               AND state = 'RUNNING'",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(hint.worker_id)
        .bind::<diesel::sql_types::Interval, _>(timeout)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?
    } else {
        use crate::schema::harvest_task_queue::dsl;
        diesel::update(
            dsl::harvest_task_queue
                .find(task_id)
                .filter(dsl::task_type.eq(TaskType::Workflow.as_str()))
                .filter(dsl::state.eq("RUNNING")),
        )
        .set((
            dsl::worker_id.eq(None::<String>),
            dsl::started_at.eq(None::<chrono::DateTime<Utc>>),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?
    };

    if updated == 0 {
        return Err(crate::error::HarvestError::NotFound(format!(
            "workflow task queue item {task_id} is not running"
        )));
    }

    Ok(())
}

/// Wake a parked workflow task for the given execution so replay can continue.
///
/// This resets any parked workflow task row for `exec_id` back to `PENDING`
/// and schedules it immediately. Only parked `RUNNING` rows with no worker
/// ownership are eligible. Actively executing `RUNNING` rows and `PENDING`
/// rows (e.g. timer-scheduled tasks) are intentionally excluded. If no parked
/// workflow task exists, this is a no-op.
///
/// If the parked row carries a `sticky_timeout`, `sticky_until` is refreshed
/// to `NOW() + sticky_timeout` so the pinned worker gets a fresh grace period
/// each time the task becomes claimable. Rows without a stored timeout keep
/// their existing `sticky_until` value (which will simply expire if stale).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn wake_workflow_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<()> {
    // Use raw SQL so we can refresh `sticky_until` from the row's own
    // `sticky_timeout` column atomically in a single UPDATE. Doing this in
    // two trips (SELECT then UPDATE) would race with other wake paths.
    let queue_names: Vec<String> = {
        use diesel::deserialize::QueryableByName;
        use diesel::sql_types::Text;

        #[derive(QueryableByName)]
        struct QueueNameRow {
            #[diesel(sql_type = Text)]
            queue_name: String,
        }

        let rows: Vec<QueueNameRow> = diesel::sql_query(
            "UPDATE harvest_task_queue \
             SET state = 'PENDING', \
                 worker_id = NULL, \
                 started_at = NULL, \
                 scheduled_at = $2, \
                 sticky_until = CASE \
                     WHEN sticky_worker_id IS NOT NULL AND sticky_timeout IS NOT NULL \
                     THEN NOW() + sticky_timeout \
                     ELSE sticky_until \
                 END \
             WHERE workflow_exec_id = $1 \
               AND task_type = 'workflow' \
               AND state = 'RUNNING' \
               AND worker_id IS NULL \
               AND started_at IS NULL \
             RETURNING queue_name",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Timestamptz, _>(Utc::now() - IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

        rows.into_iter().map(|r| r.queue_name).collect()
    };

    let mut queue_names = queue_names;
    queue_names.sort();
    queue_names.dedup();

    crate::notify::notify_tasks_enqueued(conn, &queue_names, Uuid::nil()).await?;

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
        assert!(params.trace_context.is_none());
    }

    #[test]
    fn enqueue_params_with_trace_context_attaches_carrier() {
        let carrier = TraceContextCarrier::from_traceparent("00-abcd-ef01-01");
        let params = EnqueueParams::new("billing", TaskType::Workflow, serde_json::json!(null))
            .with_trace_context(carrier.clone());

        assert_eq!(params.trace_context, Some(carrier));
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

    #[test]
    fn enqueue_params_defaults_have_no_sticky_pin() {
        let params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null));
        assert!(params.sticky_worker_id.is_none());
        assert!(params.sticky_timeout.is_none());
    }

    #[test]
    fn enqueue_params_with_sticky_sets_both_fields() {
        let params = EnqueueParams::new("default", TaskType::Workflow, serde_json::json!(null))
            .with_sticky("worker-42", StdDuration::from_secs(7));
        assert_eq!(params.sticky_worker_id.as_deref(), Some("worker-42"));
        assert_eq!(params.sticky_timeout, Some(StdDuration::from_secs(7)));
    }

    #[test]
    fn sticky_hint_constructs_with_fields() {
        let hint = StickyHint::new("w1", StdDuration::from_secs(3));
        assert_eq!(hint.worker_id, "w1");
        assert_eq!(hint.timeout, StdDuration::from_secs(3));
    }

    #[test]
    fn sticky_hint_rejects_out_of_range_duration() {
        let hint = StickyHint::new("w1", StdDuration::from_secs(u64::MAX));
        assert!(hint.chrono_timeout().is_err());
    }

    #[test]
    fn enqueue_params_concurrency_fields_default_to_none() {
        let params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!(null));
        assert!(params.concurrency_key.is_none());
        assert!(params.max_concurrent.is_none());
    }

    #[test]
    fn enqueue_params_concurrency_fields_set_manually() {
        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!(null));
        params.concurrency_key = Some("stripe".to_string());
        params.max_concurrent = Some(5);
        assert_eq!(params.concurrency_key.as_deref(), Some("stripe"));
        assert_eq!(params.max_concurrent, Some(5));
    }
}
