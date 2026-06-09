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
use crate::types::{ExecutionId, Priority};

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
    pub activity_id: Option<Uuid>,
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
    /// Build ID required to claim this task (issue #171).
    ///
    /// Workers whose `build_id` does not match (or is not declared compatible)
    /// will skip this task. `None` = any worker may claim (pre-policy / legacy
    /// executions).
    pub required_build_id: Option<String>,
    /// Optional rate limit key to throttle execution throughput.
    pub rate_limit_key: Option<String>,
    /// Absolute UTC deadline for the entire activity lifetime across all retry
    /// attempts (issue #378). Computed once at initial enqueue as
    /// `NOW() + schedule_to_close`. NULL = no total deadline.
    pub schedule_to_close_at: Option<chrono::DateTime<Utc>>,
    /// Structured capability requirements JSONB payload (issue #382).
    pub required_capabilities: Option<serde_json::Value>,
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
            activity_id: None,
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
            required_build_id: None,
            rate_limit_key: None,
            schedule_to_close_at: None,
            required_capabilities: None,
        }
    }

    /// Set the task priority, overriding the `Normal` default.
    ///
    /// The claim query orders candidates by `priority DESC, available_at ASC`
    /// so tasks with higher priority are always claimed before lower-priority
    /// tasks that arrived earlier on the same queue.
    #[must_use]
    pub const fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority.as_i32();
        self
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
        activity_id: params.activity_id,
        input: params.input.clone(),
        priority: params.priority,
        max_attempts: params.max_attempts,
        scheduled_at: params.scheduled_at,
        heartbeat_timeout: params.heartbeat_timeout,
        start_to_close: params.start_to_close,
        schedule_to_start: params.schedule_to_start,
        retry_policy: params.retry_policy.clone(),
        heartbeat_details: None,
        sticky_worker_id: None,
        sticky_until: None,
        sticky_timeout: None,
        trace_context: params
            .trace_context
            .as_ref()
            .and_then(TraceContextCarrier::to_json),
        concurrency_key: params.concurrency_key.as_deref(),
        concurrency_cap,
        required_build_id: params.required_build_id.as_deref(),
        rate_limit_key: params.rate_limit_key.as_deref(),
        schedule_to_close_at: params.schedule_to_close_at,
        required_capabilities: params.required_capabilities.clone(),
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
/// # Priority and anti-starvation
///
/// Tasks are ordered `priority DESC, available_at ASC` so higher-priority work
/// is claimed first.  When `priority_aging_secs` is `Some(K)`, each task's
/// effective priority is boosted by `+1` for every `K` seconds it has been
/// waiting in `PENDING` state.  This bounds the maximum starvation time for
/// `Low` priority tasks even under sustained high-priority load.
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
#[allow(clippy::too_many_lines)]
pub async fn claim_task(
    conn: &mut AsyncPgConnection,
    queues: &[String],
    worker_id: &str,
    worker_build_id: &str,
    priority_aging_secs: Option<u32>,
    circuit_breaker_activities: &[String],
    ineligible_activities: &[String],
) -> HarvestResult<Option<TaskQueueItem>> {
    // Two-phase claim using a CTE to avoid holding advisory locks during
    // broad WHERE filtering.
    //
    // Phase 1 (CTE): select the best PENDING candidate using the cap check
    // alone (no advisory lock). FOR UPDATE SKIP LOCKED prevents two workers
    // from picking the same row.
    //
    // Phase 2 (UPDATE): for capped keys, acquire pg_try_advisory_xact_lock
    // only for the single selected candidate and re-verify the cap. This
    // closes the race window where two workers could both pass the cap check
    // in the same poll cycle before either commits. If the advisory lock fails
    // (another worker holds it) or the re-check shows the cap is now
    // saturated, the UPDATE matches 0 rows and the transaction commits with no
    // change; the PENDING row is immediately available for the next poll.
    //
    // Acquiring the lock only for the final candidate (not during broad
    // filtering) means a worker never transiently holds locks for keys it will
    // not actually claim, keeping throughput high under contention.
    //
    // The partial index harvest_task_queue_concurrency_key_running makes the
    // scalar subquery fast: it only scans RUNNING rows with a non-NULL key.
    //
    // Build routing filter (issue #171): a task with required_build_id can only
    // be claimed by a worker whose build_id matches, is declared compatible, OR
    // the worker has an empty build_id (legacy worker — can claim anything).
    // When priority_aging_secs is Some(K), each task's effective priority is
    // boosted by floor(wait_seconds / K) to prevent indefinite starvation.
    // A NULL value (or 0, which the builder normalizes to None) disables aging.
    //
    // Circuit-breaker rate limiting (issue #369, $5 = the static set of activity
    // names that have a circuit-breaker policy): for these activities the
    // rate-limit *gate* and token *debit* are BOTH skipped at claim time. Rate
    // limiting is instead enforced authoritatively at dispatch, gated on the real
    // `on_dispatch` decision in process_activity_task: a genuine downstream call
    // atomically consumes a token (`try_consume_rate_limit_token`, rescheduling
    // if none is available) while a `CircuitOpen` short-circuit consumes nothing.
    //
    // This avoids the claim-vs-dispatch staleness race: the breaker state is
    // in-process and can change between claim and dispatch, so any claim-time
    // rate-limit decision keyed on breaker phase is necessarily approximate.
    // Moving it to dispatch lets short-circuits stay claimable at full speed
    // during an outage (no gate) while guaranteeing a real call never runs
    // without a token (authoritative debit). Non-circuit rate-limited activities
    // are unaffected — they gate and debit at claim as before.
    //
    // The concurrency cap is never bypassed: a real call must always respect
    // `max_concurrent`.
    //
    // Pause gating (issue #383): workflow tasks whose execution is in the
    // `PAUSED` state are never claimed. They stay PENDING (or parked) until the
    // execution is resumed, at which point they become claimable again. This is
    // the single executor-layer chokepoint that defers timer fires, signal
    // deliveries, and activity-completion wakes uniformly while paused — no
    // workflow-author cooperation required. In-flight activity tasks are not
    // `task_type = 'workflow'` and so continue to run to completion.
    let aging_secs_i64: Option<i64> = priority_aging_secs.map(i64::from);

    let result: Vec<TaskQueueItem> = diesel::sql_query(
        "WITH worker_info AS ( \
             SELECT COALESCE((SELECT labels FROM harvest_workers WHERE worker_id = $1), '{}'::jsonb) AS labels \
         ), \
         candidate AS ( \
             SELECT id, task_type, concurrency_key, concurrency_cap, rate_limit_key \
             FROM harvest_task_queue \
             CROSS JOIN worker_info \
             WHERE queue_name = ANY($2) \
               AND state = 'PENDING' \
               AND scheduled_at <= NOW() \
               AND ( \
                   schedule_to_close_at IS NULL \
                   OR schedule_to_close_at > NOW() \
               ) \
               AND ( \
                   sticky_worker_id IS NULL \
                   OR sticky_worker_id = $1 \
                   OR sticky_until IS NULL \
                   OR sticky_until <= NOW() \
               ) \
               AND ( \
                   concurrency_key IS NULL \
                   OR concurrency_cap IS NULL \
                   OR ( \
                       SELECT COUNT(*) FROM harvest_task_queue inner_q \
                       WHERE inner_q.concurrency_key = harvest_task_queue.concurrency_key \
                         AND inner_q.task_type = harvest_task_queue.task_type \
                         AND inner_q.state = 'RUNNING' \
                         AND inner_q.worker_id IS NOT NULL \
                   ) < harvest_task_queue.concurrency_cap \
               ) \
               AND ( \
                   required_build_id IS NULL \
                   OR $3 = '' \
                   OR required_build_id = $3 \
                   OR EXISTS ( \
                       SELECT 1 FROM harvest_build_compat \
                       WHERE build_id = $3 \
                         AND compatible_with = harvest_task_queue.required_build_id \
                   ) \
               ) \
               AND ( \
                   task_type <> 'workflow' \
                   OR workflow_exec_id IS NULL \
                   OR NOT EXISTS ( \
                       SELECT 1 FROM harvest_workflow_executions e \
                       WHERE e.id = harvest_task_queue.workflow_exec_id \
                         AND e.state = 'PAUSED' \
                   ) \
               ) \
               AND ( \
                   task_type != 'activity' \
                   OR activity_name IS NULL \
                   OR required_capabilities IS NOT NULL \
                   OR NOT (activity_name = ANY($6)) \
               ) \
               AND ( \
                   required_capabilities IS NULL \
                   OR NOT EXISTS ( \
                       SELECT 1 \
                       FROM jsonb_array_elements(required_capabilities) AS r(value) \
                       WHERE ( \
                           r.value ? 'Exact' AND ( \
                               worker_info.labels->>(r.value->'Exact'->>'key') IS NULL \
                               OR worker_info.labels->>(r.value->'Exact'->>'key') != (r.value->'Exact'->>'value') \
                           ) \
                       ) OR ( \
                           r.value ? 'In' AND ( \
                               worker_info.labels->>(r.value->'In'->>'key') IS NULL \
                               OR NOT ( \
                                   (r.value->'In'->'values') @> jsonb_build_array(worker_info.labels->>(r.value->'In'->>'key')) \
                               ) \
                           ) \
                       ) \
                   ) \
               ) \
               AND ( \
                   rate_limit_key IS NULL \
                   OR harvest_task_queue.activity_name = ANY($5) \
                   OR EXISTS ( \
                       SELECT 1 FROM harvest_rate_limit_buckets b \
                       WHERE b.key = harvest_task_queue.rate_limit_key \
                         AND LEAST(b.burst, b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * b.refill_rate) >= 1.0 \
                   ) \
               ) \
             ORDER BY \
                 CASE \
                     WHEN sticky_worker_id = $1 AND sticky_until > NOW() THEN 1 \
                     ELSE 0 \
                 END DESC, \
                 CASE \
                     WHEN $4::BIGINT IS NOT NULL AND $4::BIGINT > 0 \
                     THEN priority + FLOOR(EXTRACT(EPOCH FROM (NOW() - scheduled_at)) / $4::BIGINT)::INT \
                     ELSE priority \
                 END DESC, \
                 scheduled_at ASC \
             LIMIT 1 FOR UPDATE SKIP LOCKED \
        ), \
        claimed AS ( \
            UPDATE harvest_task_queue \
            SET state = 'RUNNING', worker_id = $1, started_at = NOW(), attempt = attempt + 1 \
            FROM candidate \
            WHERE harvest_task_queue.id = candidate.id \
              AND ( \
                  candidate.concurrency_key IS NULL \
                  OR ( \
                      pg_try_advisory_xact_lock(hashtext(candidate.concurrency_key)::bigint) \
                      AND ( \
                          candidate.concurrency_cap IS NULL \
                          OR ( \
                              SELECT COUNT(*) FROM harvest_task_queue recheck \
                              WHERE recheck.concurrency_key = candidate.concurrency_key \
                                AND recheck.task_type = candidate.task_type \
                                AND recheck.state = 'RUNNING' \
                                AND recheck.worker_id IS NOT NULL \
                          ) < candidate.concurrency_cap \
                      ) \
                  ) \
              ) \
            RETURNING harvest_task_queue.* \
        ), \
        decrement_bucket AS ( \
            UPDATE harvest_rate_limit_buckets b \
            SET tokens = LEAST(b.burst, b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * b.refill_rate) - 1.0, \
                last_refilled_at = NOW() \
            FROM claimed \
            WHERE b.key = claimed.rate_limit_key \
              AND NOT (claimed.activity_name = ANY($5)) \
            RETURNING b.key \
        ) \
        SELECT * FROM claimed",
    )
    .bind::<diesel::sql_types::Text, _>(worker_id)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .bind::<diesel::sql_types::Text, _>(worker_build_id)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>, _>(aging_secs_i64)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(circuit_breaker_activities)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(ineligible_activities)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(result.into_iter().next())
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Worker-pool scaling signals
// ---------------------------------------------------------------------------

/// Live scaling signals for a task queue.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct QueueScalingSignal {
    /// The task queue name.
    pub queue: String,
    /// Number of tasks in `PENDING` state with `scheduled_at <= NOW()`.
    pub backlog: i64,
    /// Number of tasks in `RUNNING` state.
    pub in_flight: i64,
    /// Number of tasks in `PENDING` state with `scheduled_at > NOW()`.
    pub scheduled: i64,
    /// Number of active (healthy, non-draining) workers currently polling this queue.
    pub active_workers: i64,
}

/// Helper struct for queue task counts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct QueueTaskCounts {
    /// The task queue name.
    pub queue: String,
    /// Number of tasks in `PENDING` state with `scheduled_at <= NOW()`.
    pub backlog: i64,
    /// Number of tasks in `RUNNING` state.
    pub in_flight: i64,
    /// Number of tasks in `PENDING` state with `scheduled_at > NOW()`.
    pub scheduled: i64,
}

/// Return backlog, in-flight, and scheduled task counts per queue on this shard.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn queue_task_counts(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Vec<QueueTaskCounts>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        backlog: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        in_flight: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        scheduled: i64,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT \
             queue_name AS queue, \
             COUNT(*) FILTER (WHERE state = 'PENDING' AND scheduled_at <= $1) AS backlog, \
             COUNT(*) FILTER (WHERE state = 'RUNNING') AS in_flight, \
             COUNT(*) FILTER (WHERE state = 'PENDING' AND scheduled_at > $1) AS scheduled \
         FROM harvest_task_queue \
         GROUP BY queue_name",
    )
    .bind::<diesel::sql_types::Timestamptz, _>(Utc::now())
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| QueueTaskCounts {
            queue: r.queue,
            backlog: r.backlog,
            in_flight: r.in_flight,
            scheduled: r.scheduled,
        })
        .collect())
}

// Concurrency-key stats
// ---------------------------------------------------------------------------

/// Live stats for a single `(concurrency_key, task_type)` pair.
///
/// The claim query enforces concurrency caps independently per key+type,
/// so stats are also reported at that granularity.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConcurrencyKeyStats {
    /// The concurrency group key.
    pub key: String,
    /// Task type this row covers (`"workflow"` or `"activity"`).
    pub task_type: String,
    /// Declared maximum concurrent tasks for this key+type.
    pub max_concurrent: i32,
    /// Number of tasks currently in `RUNNING` state for this key+type.
    pub in_flight: i64,
    /// Number of tasks in `PENDING` state for this key+type (may be deferred
    /// by the cap if `in_flight >= max_concurrent`).
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
        #[diesel(sql_type = diesel::sql_types::Text)]
        task_type: String,
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
             task_type, \
             MAX(concurrency_cap)::INT4 AS max_concurrent, \
             COUNT(*) FILTER (WHERE state = 'RUNNING' AND worker_id IS NOT NULL) AS in_flight, \
             COUNT(*) FILTER (WHERE state = 'PENDING') AS pending \
         FROM harvest_task_queue \
         WHERE concurrency_key IS NOT NULL \
           AND concurrency_cap IS NOT NULL \
           AND queue_name = ANY($1) \
           AND (state = 'PENDING' OR (state = 'RUNNING' AND worker_id IS NOT NULL)) \
         GROUP BY concurrency_key, task_type",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| ConcurrencyKeyStats {
            key: r.key,
            task_type: r.task_type,
            max_concurrent: r.max_concurrent,
            in_flight: r.in_flight,
            pending: r.pending,
        })
        .collect())
}

/// Mark a task as completed with the given output.
///
/// Terminal completion clears any heartbeat checkpoint payload so it cannot be
/// observed after the activity has successfully finished.
///
/// # Errors
/// Lock the task queue row `FOR UPDATE` and return its current `state`.
///
/// Used by [`crate::context::ActivityContext::run_transactional`] to verify
/// the task is still `RUNNING` before committing the transactional activity
/// result.  Returns `None` when the row no longer exists.
pub(crate) async fn task_state_for_update(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
) -> HarvestResult<Option<String>> {
    use crate::schema::harvest_task_queue::dsl;

    dsl::harvest_task_queue
        .find(task_id)
        .for_update()
        .select(dsl::state)
        .first::<String>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)
}

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
        dsl::heartbeat_details.eq(None::<serde_json::Value>),
        dsl::error.eq(None::<String>),
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
/// This is a terminal transition, so heartbeat checkpoint payloads are cleared.
/// Retry rescheduling uses [`requeue_for_retry`] instead and preserves the
/// payload for the next attempt.
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
        dsl::heartbeat_details.eq(None::<serde_json::Value>),
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
        dsl::heartbeat_details.eq(None::<serde_json::Value>),
        dsl::completed_at.eq(Some(Utc::now())),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)
}

/// Mark all pending or running task rows for a workflow execution as cancelled.
///
/// Reset uses this instead of failure so operators can distinguish work torn
/// down by a fork from work that exhausted retries or crashed.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) on
/// update failure.
pub async fn cancel_open_tasks_for_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<usize> {
    use crate::schema::harvest_task_queue::dsl;

    diesel::update(
        dsl::harvest_task_queue
            .filter(dsl::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .filter(dsl::state.eq_any(["PENDING", "RUNNING"])),
    )
    .set((
        dsl::state.eq("CANCELLED"),
        dsl::worker_id.eq(None::<String>),
        dsl::error.eq(Some(reason.to_string())),
        dsl::heartbeat_details.eq(None::<serde_json::Value>),
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

/// Update the `last_heartbeat_at` timestamp and checkpoint payload for a running task.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn record_heartbeat(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    details: serde_json::Value,
) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    let updated = diesel::update(
        dsl::harvest_task_queue
            .find(task_id)
            .filter(dsl::state.eq("RUNNING")),
    )
    .set((
        dsl::last_heartbeat_at.eq(Some(Utc::now())),
        dsl::heartbeat_details.eq(Some(details)),
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

/// Reset a task to `PENDING` with a future `scheduled_at` for retry.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
/// Reschedule a `RUNNING` task back to `PENDING` after a retryable failure.
///
/// Stores `previous_error` in the task row's `error` column so the next
/// dispatch can surface it via `ActivityContext::previous_failure()`.
/// The heartbeat details payload is preserved so the retry attempt can resume
/// from the last flushed checkpoint.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn requeue_for_retry(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    delay: Duration,
    previous_error: &str,
) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    let next_run = Utc::now() + delay;

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
        dsl::crash_strikes.eq(0),
        dsl::scheduled_at.eq(next_run),
        dsl::error.eq(Some(previous_error)),
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

/// Reset a task to `PENDING` at an explicit timestamp.
///
/// Clears only the liveness timestamp for the failed attempt. The heartbeat
/// details payload is intentionally preserved so the retry attempt can resume
/// from the last flushed checkpoint.
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
        // Clean continuation (suspension or retryable-error reschedule) means
        // the task made progress without crashing a worker, so the poison-pill
        // crash streak resets — the threshold measures *consecutive* crashes
        // (issue #367).
        dsl::crash_strikes.eq(0),
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

/// Defer a `RUNNING` task back to `PENDING` for a rate-limit retry **without
/// counting it as an attempt** (issue #369).
///
/// Used by the dispatch-time rate-limit gate for circuit-breaker activities:
/// when no token is available the handler never runs, so [`claim_task`]'s
/// `attempt + 1` increment must be undone — otherwise repeated deferrals would
/// silently drain the retry budget and DLQ the task before it ever executed.
/// Otherwise mirrors [`reschedule_task`] (clean continuation: resets the
/// poison-pill crash streak and re-notifies the queue).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn defer_rate_limited_task(
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
        dsl::crash_strikes.eq(0),
        // Undo the claim-time attempt increment: a rate-limit deferral is not an
        // execution, so it must not consume the retry budget.
        dsl::attempt.eq(diesel::dsl::sql::<diesel::sql_types::Integer>(
            "GREATEST(attempt - 1, 0)",
        )),
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
        // No sticky hint: clear any stale affinity left by a previous worker
        // that ran with sticky routing enabled. Without this, wake_workflow_task
        // would refresh sticky_until from the stored sticky_timeout column and
        // re-pin the execution to the old worker even though the current worker
        // is running with sticky routing disabled.
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
                 activity_name = NULL, \
                 sticky_until = CASE \
                     WHEN sticky_worker_id IS NOT NULL AND sticky_timeout IS NOT NULL \
                     THEN NOW() + sticky_timeout \
                     ELSE sticky_until \
                 END \
             WHERE workflow_exec_id = $1 \
               AND task_type = 'workflow' \
               AND ( \
                   (state = 'RUNNING' AND worker_id IS NULL AND started_at IS NULL) \
                   OR (state = 'PENDING' AND scheduled_at > $2 AND activity_name = 'mixed_signal_suspension') \
               ) \
             RETURNING queue_name",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Timestamptz, _>(Utc::now() - IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

        rows.into_iter().map(|r| r.queue_name).collect()
    };

    // A workflow task may already be PENDING with an elapsed `scheduled_at` —
    // e.g. a timer fired while the execution was PAUSED (issue #383), so the
    // task was enqueued but never re-pended by the UPDATE above. Such a task is
    // immediately claimable once the execution is RUNNING again, but no fresh
    // NOTIFY was emitted for it, so a LISTEN-based worker would sleep until the
    // next poll interval. Notify those queues too so resume re-arms promptly.
    let already_due_queue_names: Vec<String> = {
        use diesel::deserialize::QueryableByName;
        use diesel::sql_types::Text;

        #[derive(QueryableByName)]
        struct QueueNameRow {
            #[diesel(sql_type = Text)]
            queue_name: String,
        }

        let rows: Vec<QueueNameRow> = diesel::sql_query(
            "SELECT DISTINCT queue_name FROM harvest_task_queue \
             WHERE workflow_exec_id = $1 \
               AND task_type = 'workflow' \
               AND state = 'PENDING' \
               AND scheduled_at <= $2",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Timestamptz, _>(Utc::now())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

        rows.into_iter().map(|r| r.queue_name).collect()
    };

    let mut queue_names = queue_names;
    queue_names.extend(already_due_queue_names);
    queue_names.sort();
    queue_names.dedup();

    crate::notify::notify_tasks_enqueued(conn, &queue_names, Uuid::nil()).await?;

    Ok(())
}

/// Update the priority of a pending task via the management API.
///
/// Only tasks in `PENDING` state are eligible; already-running tasks ignore
/// the change (the running attempt keeps its original priority). The next retry
/// attempt will use the new value because it will be re-claimed using the
/// updated row. Terminal tasks (`COMPLETED`, `FAILED`, `CANCELLED`) are not
/// found by this filter and the function returns `false`.
///
/// Returns `true` when the update was applied, `false` when the task was not
/// found in an updatable state (terminal tasks return `false`).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn update_task_priority(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    priority: Priority,
) -> HarvestResult<bool> {
    use crate::schema::harvest_task_queue::dsl;

    let updated = diesel::update(
        dsl::harvest_task_queue
            .find(task_id)
            .filter(dsl::state.eq_any(["PENDING", "RUNNING"])),
    )
    .set(dsl::priority.eq(priority.as_i32()))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(updated > 0)
}

/// Returns `true` if a task with the given ID exists in the queue (regardless of state).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn task_exists(conn: &mut AsyncPgConnection, task_id: Uuid) -> HarvestResult<bool> {
    use crate::schema::harvest_task_queue::dsl;

    let found: Option<Uuid> = dsl::harvest_task_queue
        .filter(dsl::id.eq(task_id))
        .select(dsl::id)
        .first::<Uuid>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    Ok(found.is_some())
}

/// Check if any pending tasks in the specified queues are throttled due to rate limits.
///
/// Returns the rate limit keys that are currently saturated (have < 1.0 tokens).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn check_throttled_keys(
    conn: &mut AsyncPgConnection,
    queues: &[String],
) -> HarvestResult<Vec<String>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        rate_limit_key: String,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT DISTINCT q.rate_limit_key \
         FROM harvest_task_queue q \
         JOIN harvest_rate_limit_buckets b ON b.key = q.rate_limit_key \
         WHERE q.queue_name = ANY($1) \
           AND q.state = 'PENDING' \
           AND q.scheduled_at <= NOW() \
           AND LEAST(b.burst, b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * b.refill_rate) < 1.0"
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows.into_iter().map(|r| r.rate_limit_key).collect())
}

/// Atomically consume one rate-limit token from `key`'s bucket at dispatch time.
///
/// Returns `true` if a token was available and debited (the caller may proceed
/// with the real downstream call), or `false` if no token could be reserved and
/// the caller must defer (e.g. reschedule the task) rather than run the call.
///
/// Used by the circuit breaker (issue #369): activities with a breaker skip the
/// claim-time rate-limit gate and debit entirely (see [`claim_task`]); their rate
/// limiting is enforced *here*, gated on the authoritative `on_dispatch`
/// decision, so a `CircuitOpen` short-circuit consumes no token while a genuine
/// call atomically reserves one. The check-and-debit is a single UPDATE so two
/// concurrent dispatches cannot both reserve the last token.
///
/// **Fails closed:** returns `false` both when the bucket is empty *and* when the
/// bucket row is missing. Because the claim-time gate is skipped for these
/// activities, this is the sole rate-limit enforcement point; treating a missing
/// bucket as "allow" would let a configured limit run unthrottled if bucket
/// auto-registration failed or the row was deleted. A `rate_limit_key` is only
/// set when a limit is configured (so a bucket should exist), and deferring until
/// it does matches the old claim-time gate, which also would not admit the task
/// without a bucket row.
///
/// Mirrors the claim-time debit math (apply pending refill, then `-1.0`).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn try_consume_rate_limit_token(
    conn: &mut AsyncPgConnection,
    key: &str,
) -> HarvestResult<bool> {
    #[derive(diesel::QueryableByName)]
    struct Outcome {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        debited: bool,
    }

    // A single conditional UPDATE: the row is debited only if a token is
    // available. `RETURNING`/`EXISTS` reports whether the debit happened; a
    // missing bucket row or an empty bucket both yield `debited = false`.
    let outcome: Option<Outcome> = diesel::sql_query(
        "WITH debited AS ( \
             UPDATE harvest_rate_limit_buckets \
             SET tokens = LEAST(burst, tokens + EXTRACT(EPOCH FROM (NOW() - last_refilled_at)) * refill_rate) - 1.0, \
                 last_refilled_at = NOW() \
             WHERE key = $1 \
               AND LEAST(burst, tokens + EXTRACT(EPOCH FROM (NOW() - last_refilled_at)) * refill_rate) >= 1.0 \
             RETURNING key \
        ) \
        SELECT EXISTS (SELECT 1 FROM debited) AS debited",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .get_result(conn)
    .await
    .optional()
    .map_err(crate::error::database_error)?;

    // Fail closed: only proceed when a token was actually reserved.
    Ok(outcome.is_some_and(|o| o.debited))
}

/// Return one rate-limit token previously reserved by
/// [`try_consume_rate_limit_token`] (capped at burst).
///
/// Used by the circuit breaker (issue #369) on the rare path where a token was
/// reserved for a genuine call that then turns out not to run — e.g. the activity
/// already has a terminal event, or the task row stopped being `RUNNING`
/// (cancelled/timed out concurrently) between the reservation and appending
/// `ActivityStarted`. Refunding keeps the bucket accurate (a call that never
/// happened consumes no token), symmetric with a short-circuit reserving nothing.
/// Mirrors the debit math (apply pending refill, then `+1.0`). A missing bucket
/// row is a no-op.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn refund_rate_limit_token(conn: &mut AsyncPgConnection, key: &str) -> HarvestResult<()> {
    diesel::sql_query(
        "UPDATE harvest_rate_limit_buckets \
         SET tokens = LEAST(burst, tokens + EXTRACT(EPOCH FROM (NOW() - last_refilled_at)) * refill_rate + 1.0), \
             last_refilled_at = NOW() \
         WHERE key = $1",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
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
    fn enqueue_params_schedule_to_close_at_defaults_to_none() {
        let params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!(null));
        assert!(
            params.schedule_to_close_at.is_none(),
            "schedule_to_close_at must default to None (unbounded)"
        );
    }

    #[test]
    fn enqueue_params_schedule_to_close_at_can_be_set() {
        let deadline = Utc::now() + Duration::seconds(300);
        let mut params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!(null));
        params.schedule_to_close_at = Some(deadline);
        assert_eq!(params.schedule_to_close_at, Some(deadline));
    }

    #[test]
    fn enqueue_params_concurrency_fields_set_manually() {
        let mut params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!(null));
        params.concurrency_key = Some("stripe".to_string());
        params.max_concurrent = Some(5);
        assert_eq!(params.concurrency_key.as_deref(), Some("stripe"));
        assert_eq!(params.max_concurrent, Some(5));
    }
}
