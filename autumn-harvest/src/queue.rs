//! Postgres-backed task queue with `SKIP LOCKED` claiming.
//!
//! Workers poll their assigned queues via [`claim_task()`] which atomically
//! moves a `PENDING` row to `RUNNING` using `FOR UPDATE SKIP LOCKED` --
//! no two workers will ever claim the same task.

use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};
use diesel::AsChangeset;
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

const IMMEDIATE_SCHEDULE_SKEW_SECS: i32 = 5;
const IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE: Duration =
    Duration::seconds(IMMEDIATE_SCHEDULE_SKEW_SECS as i64);

/// Compute the **DB-clock** portion of schedule-to-start latency in seconds: the
/// wait from a task's *true* eligibility to when it was claimed (`claimed_at`),
/// clamped to `0`.
///
/// True eligibility is `GREATEST(scheduled_at, created_at)`:
///
/// * `EnqueueParams::new` backdates an immediate task's `scheduled_at` to
///   `NOW() - IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE` so it stays claimable under the
///   `scheduled_at <= NOW()` predicate even when host and Postgres clocks differ
///   slightly. Its real eligibility is the insert time `created_at` (the later of
///   the two), so a promptly-served immediate task reports ~0 — the backdating is
///   dropped without a fixed-allowance subtraction.
/// * A genuinely *delayed* or *retried* task sets `scheduled_at` to an explicit
///   future instant (`>= created_at`, e.g. `requeue_for_retry` uses `NOW() + delay`),
///   so `GREATEST` selects `scheduled_at` and the full wait is reported with no
///   discount. The prior fixed-allowance subtraction under-reported these by up to
///   the allowance and could hide work genuinely over the SLO (issue #501 review).
///
/// `created_at` is `None` only for rows enqueued before the column was added; those
/// fall back to `scheduled_at` (best available) until they drain.
///
/// **Clock discipline:** `claimed_at` must be a **Postgres** timestamp (e.g. the
/// task's `started_at`, stamped by `claim_task`), so this whole expression is
/// computed in one clock and a host/Postgres skew cannot leak in. The worker then
/// adds the host-**monotonic** local wait (permit acquisition + setup, measured
/// with `Instant::elapsed`) to this value — never `Utc::now()`, which would mix the
/// host wall clock with the Postgres eligibility timestamps (issue #501 review).
#[must_use]
pub fn schedule_to_start_secs(
    scheduled_at: DateTime<Utc>,
    created_at: Option<DateTime<Utc>>,
    claimed_at: DateTime<Utc>,
) -> f64 {
    let eligible = created_at.map_or(scheduled_at, |c| scheduled_at.max(c));
    (claimed_at - eligible)
        .max(Duration::zero())
        .to_std()
        .unwrap_or_default()
        .as_secs_f64()
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
    /// Ambient context headers propagated from the parent workflow (issue #481).
    pub context_headers: Option<serde_json::Value>,
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
            context_headers: None,
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
        context_headers: params.context_headers.clone(),
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
             SELECT id, task_type, concurrency_key, concurrency_cap, rate_limit_key, activity_name \
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
        rate_limit_debit AS ( \
            UPDATE harvest_rate_limit_buckets b \
            SET tokens = LEAST(b.burst, b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * b.refill_rate) - 1.0, \
                last_refilled_at = NOW() \
            FROM candidate \
            WHERE b.key = candidate.rate_limit_key \
              AND NOT (candidate.activity_name = ANY($5)) \
              AND LEAST(b.burst, b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * b.refill_rate) >= 1.0 \
            RETURNING b.key AS debited_key \
        ), \
        claimed AS ( \
            UPDATE harvest_task_queue \
            SET state = 'RUNNING', worker_id = $1, started_at = NOW(), attempt = attempt + 1, \
                wake_requested = FALSE \
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
              AND ( \
                  candidate.rate_limit_key IS NULL \
                  OR candidate.activity_name = ANY($5) \
                  OR EXISTS (SELECT 1 FROM rate_limit_debit WHERE debited_key = candidate.rate_limit_key) \
              ) \
            RETURNING harvest_task_queue.* \
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

/// Atomically cancel a single activity task row by its `activity_id`, iff it
/// is still open (`PENDING`/`RUNNING`) — the loser-cancellation primitive for
/// `ctx.race()` (issue #600).
///
/// Returns `Some((activity_name, queue_name))` if a still-open row was
/// transitioned to `CANCELLED` (the caller should then record a synthetic
/// terminal event for it so replay never re-observes it as in-progress, and
/// can use the returned name/queue to record activity-outcome metrics);
/// returns `None` if the row was already terminal by the time this ran (a
/// genuine completion raced the cancellation) — the caller must **not**
/// record a synthetic terminal in that case, since a real one already exists
/// (or is about to be appended by the in-flight completion write).
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) on
/// update failure.
pub async fn cancel_activity_task(
    conn: &mut AsyncPgConnection,
    activity_id: crate::types::ActivityExecId,
) -> HarvestResult<Option<(String, String)>> {
    use crate::schema::harvest_task_queue::dsl;

    let cancelled = diesel::update(
        dsl::harvest_task_queue
            .filter(dsl::activity_id.eq(Some(activity_id.as_uuid())))
            .filter(dsl::state.eq_any(["PENDING", "RUNNING"])),
    )
    .set((
        dsl::state.eq("CANCELLED"),
        dsl::worker_id.eq(None::<String>),
        dsl::error.eq(Some("lost race to a sibling branch".to_string())),
        dsl::heartbeat_details.eq(None::<serde_json::Value>),
        dsl::completed_at.eq(Some(Utc::now())),
    ))
    .returning((dsl::activity_name, dsl::queue_name))
    .get_result::<(Option<String>, String)>(conn)
    .await
    .optional()
    .map_err(crate::error::database_error)?;

    Ok(cancelled.map(|(name, queue_name)| (name.unwrap_or_default(), queue_name)))
}

/// Delete a single still-pending durable timer row by its `timer_id`.
///
/// The loser-cancellation primitive for a losing timer branch of `ctx.race()`
/// (issue #600). A no-op (returns `Ok(())`) if the timer has already fired
/// (`fired = true`) or does not exist.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) on
/// delete failure.
pub async fn delete_pending_timer(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    timer_id: &crate::types::TimerId,
) -> HarvestResult<()> {
    use crate::schema::harvest_timers::dsl;

    diesel::delete(
        dsl::harvest_timers
            .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(dsl::timer_id.eq(timer_id.as_str()))
            .filter(dsl::fired.eq(false)),
    )
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(())
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

/// Returns the age in seconds of the oldest currently-*claimable* eligible task
/// per queue.
///
/// Mirrors the part of [`claim_task`]'s eligibility predicate that determines
/// whether a worker is *supposed* to pick a task up: `state = 'PENDING' AND
/// scheduled_at <= NOW()`, **excluding** (a) tasks whose `schedule_to_close_at`
/// deadline has already elapsed (issue #378) — `claim_task` skips them too, so a
/// past-deadline row awaiting the timeout scanner is not claimable and counting
/// its age would page for work no worker may start; (b) workflow tasks whose
/// execution is `PAUSED` (issue #383) — a paused execution's parked task is
/// intentionally not claimable, so counting its age would inflate the saturation
/// signal and fire false alerts until the workflow is resumed; (c) rate-limited
/// activity tasks whose token bucket is currently below one token (issue #369) —
/// `claim_task` skips these until tokens refill, so counting their age would page
/// for *intended* throttling on a deliberately rate-limited queue (circuit-breaker
/// activities in `circuit_breaker_activities` skip the claim-time rate-limit gate
/// entirely, so they are exempt from this exclusion exactly as in `claim_task`); and
/// (d) tasks behind a saturated per-key concurrency cap (issue #247) — `claim_task`
/// refuses every worker while `COUNT(RUNNING) >= concurrency_cap`, so a capped hot
/// tenant's deferred rows are not claimable and counting their age would page for
/// *intended* fair-share capping until an in-flight task for that key finishes.
///
/// The age is measured from each task's *true* eligibility,
/// `GREATEST(scheduled_at, COALESCE(created_at, scheduled_at))` (see
/// [`schedule_to_start_secs`]): an immediate task's backdated `scheduled_at` is
/// corrected to its `created_at` insert time so it reports ~0, while a delayed/retried
/// task's explicit future `scheduled_at` is used verbatim (no fixed discount that
/// would under-report a real over-SLO wait). Pre-upgrade rows with NULL `created_at`
/// fall back to `scheduled_at`. Clamped to `0`.
///
/// (The finer-grained claim filters — build-id, sticky routing, capabilities — are
/// deliberately *not* mirrored here: those gate *which worker* may claim, not whether
/// the task is work no worker at all may start, so they belong to worker-coverage
/// signals rather than the queue-age gauge.)
///
/// Only queues that have at least one eligible task appear in the result; the
/// sampler is responsible for resetting the gauge to `0` for queues with no
/// eligible tasks.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn oldest_pending_ages(
    conn: &mut AsyncPgConnection,
    queues: &[String],
    circuit_breaker_activities: &[String],
) -> HarvestResult<Vec<(String, f64)>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Double)]
        age_secs: f64,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT queue_name, \
                GREATEST( \
                    EXTRACT(EPOCH FROM (NOW() - MIN(GREATEST(scheduled_at, COALESCE(created_at, scheduled_at))))), \
                    0 \
                )::DOUBLE PRECISION AS age_secs \
         FROM harvest_task_queue \
         WHERE queue_name = ANY($1) \
           AND state = 'PENDING' \
           AND scheduled_at <= NOW() \
           AND ( \
               schedule_to_close_at IS NULL \
               OR schedule_to_close_at > NOW() \
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
               rate_limit_key IS NULL \
               OR activity_name = ANY($2) \
               OR EXISTS ( \
                   SELECT 1 FROM harvest_rate_limit_buckets b \
                   WHERE b.key = harvest_task_queue.rate_limit_key \
                     AND LEAST(b.burst, b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * b.refill_rate) >= 1.0 \
               ) \
           ) \
         GROUP BY queue_name",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(circuit_breaker_activities)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| (r.queue_name, r.age_secs))
        .collect())
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

/// Shared "reset a claimed task back to `PENDING` with a future
/// `scheduled_at`" changeset (code-review cleanup, issue #603): the 7 fields
/// common to both [`requeue_for_retry`] (activity retry) and
/// [`requeue_workflow_task_nd_blocked`] (ND-block backoff), previously
/// duplicated verbatim in both functions.
#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::harvest_task_queue)]
struct PendingRequeueChangeset {
    state: &'static str,
    worker_id: Option<String>,
    started_at: Option<chrono::DateTime<Utc>>,
    last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    crash_strikes: i32,
    scheduled_at: chrono::DateTime<Utc>,
    error: Option<String>,
}

impl PendingRequeueChangeset {
    const fn new(next_run: chrono::DateTime<Utc>, previous_error: String) -> Self {
        Self {
            state: "PENDING",
            worker_id: None,
            started_at: None,
            last_heartbeat_at: None,
            crash_strikes: 0,
            scheduled_at: next_run,
            error: Some(previous_error),
        }
    }
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
    let changeset = PendingRequeueChangeset::new(next_run, previous_error.to_string());

    let queue_name = diesel::update(
        dsl::harvest_task_queue
            .find(task_id)
            .filter(dsl::state.eq("RUNNING")),
    )
    .set(&changeset)
    .returning(dsl::queue_name)
    .get_result::<String>(conn)
    .await
    .optional()
    .map_err(crate::error::database_error)?
    .ok_or_else(|| {
        crate::error::HarvestError::NotFound(format!("task queue item {task_id} is not running"))
    })?;

    // Notify is best-effort: the task is already durably PENDING after the
    // UPDATE above and will be claimed on the next poll cycle even if
    // pg_notify is unavailable. Callers that count retries should key on
    // Ok(()) meaning "state update succeeded", not "notify succeeded".
    if let Err(e) = crate::notify::notify_task_enqueued(conn, &queue_name, task_id).await {
        tracing::warn!(
            task_id = %task_id,
            queue = %queue_name,
            error = %e,
            "pg_notify failed after retry requeue; task is PENDING and will be claimed on next poll"
        );
    }

    Ok(())
}

/// Re-pend an ND-blocked workflow task with a future `scheduled_at` (issue
/// #603).
///
/// Mirrors [`requeue_for_retry`] with four deliberate differences for the
/// replay-non-determinism block path:
/// - restricted to `task_type = 'workflow'` rows (defensive — the block path
///   only ever holds a claimed workflow task);
/// - clears the sticky affinity columns: the pinned worker is running the
///   divergent build, so the re-dispatch must be claimable by any worker
///   (e.g. one already running the rolled-back build);
/// - clears `wake_requested`: a wake captured mid-cycle must not short-circuit
///   the backoff — the row is durably `PENDING` and deferred purely by
///   `scheduled_at` (`claim_task` enforces `scheduled_at <= NOW()`), so signals
///   arriving while blocked are processed on the next backoff dispatch;
/// - clears `activity_name`: a stale `'mixed_signal_suspension'` sentinel
///   (issue #476/#600 timer+signal races) left on the row would otherwise let
///   `primary_repend_workflow_task_query`'s wake fallback match this
///   `PENDING`/future-`scheduled_at` row and reset `scheduled_at` to now on
///   any unrelated wake, silently bypassing the backoff (issue #603 fix).
///
/// No `pg_notify`: the task is deliberately not claimable until `scheduled_at`,
/// so waking pollers early would be pure noise.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::NotFound`] when the task is not a
/// claimed (`RUNNING`) workflow task, and
/// [`crate::error::HarvestError::Database`] on update failure.
pub async fn requeue_workflow_task_nd_blocked(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    delay: Duration,
    reason: &str,
) -> HarvestResult<()> {
    use crate::schema::harvest_task_queue::dsl;

    let next_run = Utc::now() + delay;
    let changeset = PendingRequeueChangeset::new(next_run, reason.to_string());

    let updated = diesel::update(
        dsl::harvest_task_queue
            .find(task_id)
            .filter(dsl::state.eq("RUNNING"))
            .filter(dsl::task_type.eq("workflow")),
    )
    .set((
        changeset,
        dsl::sticky_worker_id.eq(None::<String>),
        dsl::sticky_until.eq(None::<chrono::DateTime<Utc>>),
        dsl::sticky_timeout.eq(None::<chrono::Duration>),
        dsl::wake_requested.eq(false),
        dsl::activity_name.eq(None::<String>),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(crate::error::HarvestError::NotFound(format!(
            "task queue item {task_id} is not a running workflow task"
        )));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// force_retry_activity_now (issue #516)
// ---------------------------------------------------------------------------

/// Outcome of a [`force_retry_activity_now`] call.
#[derive(Debug, Clone)]
pub struct RetryActivityOutcome {
    /// The task-queue row PK that was (or would have been) advanced.
    pub task_id: Uuid,
    /// The queue this task belongs to.
    pub queue_name: String,
    /// The effective `scheduled_at` after the operation (backdated by
    /// `IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE` when `advanced` is `true` so it
    /// passes the `scheduled_at <= NOW()` predicate in `claim_task` even under
    /// host/Postgres clock skew; unchanged otherwise).
    pub scheduled_at: DateTime<Utc>,
    /// `true` when the task's eligibility was advanced (it was backing off);
    /// `false` when the task required no change (see `already_eligible`).
    pub advanced: bool,
    /// Only meaningful when `advanced` is `false`. `true` means the task was
    /// already claimable before this call (genuine idempotent no-op). `false`
    /// means a concurrent worker claimed the task between the SELECT and the
    /// UPDATE — the task is now `RUNNING`, so the caller's goal (retry it) is
    /// already achieved.
    pub already_eligible: bool,
}

/// Advance a backing-off activity task to be immediately eligible for claim
/// (issue #516).
///
/// A backing-off activity is a `PENDING` `harvest_task_queue` row whose
/// `scheduled_at` is in the future (set by [`requeue_for_retry`]). This
/// function advances that timestamp to `NOW()` and wakes an idle worker via
/// `pg_notify` so dispatch happens within one poll interval.
///
/// # Semantics
///
/// - Only `task_type = 'activity'` rows owned by `workflow_exec_id` are
///   matched; a task belonging to a different workflow returns `NotFound`.
/// - If the row's `state != 'PENDING'` (e.g. `RUNNING`, completed, or already
///   in the DLQ), the call returns `HarvestError::Config` (→ 409 via
///   `conflict_from`).
/// - If `scheduled_at <= NOW()` the task is already eligible; the function
///   returns `Ok(RetryActivityOutcome { advanced: false, already_eligible: true })`
///   without touching the database (idempotent no-op).
/// - **The `attempt`/`max_attempts`/`error`/`crash_strikes` columns are never
///   modified.** Only `scheduled_at` is updated, so the attempt counter is
///   neither reset nor incremented — the forced run is the attempt that was
///   already scheduled.
/// - No new `WorkflowEvent` is appended; the forced attempt produces the same
///   `ActivityScheduled`/`ActivityStarted`/… events it would on natural retry.
///
/// # Caveat — rate-limit and concurrency caps
///
/// `advanced: true` means `scheduled_at` was moved to immediate eligibility; it
/// does **not** guarantee the task will be claimed on the very next poll. A
/// per-queue rate-limit bucket (`harvest_rate_limit_buckets`) or a per-key
/// concurrency cap (`concurrency_key`/`concurrency_cap`) in `claim_task` can
/// still defer the actual dispatch until capacity is available. Both of those
/// gates enforce independent admission policies that this function does not
/// bypass or inspect.
///
/// # Errors
///
/// - [`crate::error::HarvestError::NotFound`] — no activity-type task with
///   `id = task_id` and `workflow_exec_id = workflow_exec_id`.
/// - [`crate::error::HarvestError::Config`] — task exists but is not in
///   a retryable (`PENDING`) state.
/// - [`crate::error::HarvestError::Database`] — Postgres error.
pub async fn force_retry_activity_now(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Uuid,
    task_id: Uuid,
) -> HarvestResult<RetryActivityOutcome> {
    use crate::schema::harvest_task_queue::dsl;
    use diesel::SelectableHelper;

    // Load the row — must belong to this workflow and be an activity task.
    let row = dsl::harvest_task_queue
        .filter(dsl::id.eq(task_id))
        .filter(dsl::workflow_exec_id.eq(Some(workflow_exec_id)))
        .filter(dsl::task_type.eq("activity"))
        .select(crate::models::TaskQueueItem::as_select())
        .first::<crate::models::TaskQueueItem>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| {
            crate::error::HarvestError::NotFound(format!(
                "activity task {task_id} not found for workflow {workflow_exec_id}"
            ))
        })?;

    // Only PENDING tasks are retryable; RUNNING/completed/DLQ'd → conflict.
    if row.state != "PENDING" {
        return Err(crate::error::HarvestError::Config(format!(
            "activity task {task_id} is in state '{}', not PENDING — \
             only backing-off (PENDING) tasks can be force-retried",
            row.state
        )));
    }

    let now = Utc::now();

    // Already eligible — idempotent no-op, nothing to advance.
    if row.scheduled_at <= now {
        return Ok(RetryActivityOutcome {
            task_id,
            queue_name: row.queue_name,
            scheduled_at: row.scheduled_at,
            advanced: false,
            already_eligible: true,
        });
    }

    // Backdate by the skew allowance so the row passes `scheduled_at <= NOW()`
    // in claim_task's Postgres-side predicate even when the host clock is
    // slightly ahead of the Postgres server clock. This mirrors what
    // EnqueueParams::new does for immediately-runnable tasks.
    let claim_ready_at = now - IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE;

    // Advance scheduled_at. Only update if still PENDING (guards a concurrent
    // claim race — a worker that claimed the row between our SELECT and this
    // UPDATE would have set state='RUNNING'; the WHERE clause then matches 0
    // rows and we return advanced=false rather than silently succeeding).
    let updated_queue_name = diesel::update(
        dsl::harvest_task_queue
            .filter(dsl::id.eq(task_id))
            .filter(dsl::workflow_exec_id.eq(Some(workflow_exec_id)))
            .filter(dsl::state.eq("PENDING")),
    )
    .set(dsl::scheduled_at.eq(claim_ready_at))
    .returning(dsl::queue_name)
    .get_result::<String>(conn)
    .await
    .optional()
    .map_err(crate::error::database_error)?;

    // If 0 rows were updated a concurrent claim raced us; the task is now
    // RUNNING and the caller's goal (retry it now) is effectively achieved.
    // already_eligible=false distinguishes this from the genuine no-op above.
    let (actual_queue, actual_scheduled_at, actually_advanced) = match updated_queue_name {
        Some(q) => (q, claim_ready_at, true),
        None => (row.queue_name, row.scheduled_at, false),
    };

    if actually_advanced {
        crate::notify::notify_task_enqueued(conn, &actual_queue, task_id).await?;
    }

    Ok(RetryActivityOutcome {
        task_id,
        queue_name: actual_queue,
        scheduled_at: actual_scheduled_at,
        advanced: actually_advanced,
        already_eligible: false,
    })
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
/// Returns `true` when a wake was requested for this row (via
/// `wake_workflow_task`'s dropped-wake fallback) while it was still claimed
/// and mid-processing -- i.e. a wake raced this park. The `wake_requested`
/// flag is atomically read and cleared as part of the same UPDATE that parks
/// the row, so this can never miss a wake that lands in the gap between a
/// caller's own terminal-state check and this call. Callers that care about
/// dropped wakes should treat `true` the same as an already-observed terminal
/// sibling and immediately re-wake via [`wake_workflow_task`] rather than
/// leaving the row parked to wait for a wake that already happened.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn park_workflow_task(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    sticky: Option<StickyHint<'_>>,
) -> HarvestResult<bool> {
    use diesel::deserialize::QueryableByName;
    use diesel::sql_types::Bool;

    #[derive(QueryableByName)]
    struct WakeRequestedRow {
        #[diesel(sql_type = Bool)]
        had_wake_requested: bool,
    }

    // Use raw SQL so `sticky_until` is computed from Postgres `NOW()` -- the
    // same clock used by `wake_workflow_task` and `claim_task` -- and so the
    // pre-update `wake_requested` value can be captured atomically in the same
    // statement that clears it. A `candidate` CTE locks the row with
    // `FOR UPDATE` and reads its current `wake_requested` before the `updated`
    // CTE clears it; doing this as a SELECT-then-UPDATE in two round trips
    // would race with `wake_workflow_task`'s fallback UPDATE in exactly the
    // gap this mechanism exists to close.
    let rows: Vec<WakeRequestedRow> = if let Some(hint) = sticky {
        let timeout = hint.chrono_timeout()?;
        diesel::sql_query(park_workflow_task_sticky_query())
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .bind::<diesel::sql_types::Text, _>(hint.worker_id)
            .bind::<diesel::sql_types::Interval, _>(timeout)
            .load(conn)
            .await
            .map_err(crate::error::database_error)?
    } else {
        // No sticky hint: clear any stale affinity left by a previous worker
        // that ran with sticky routing enabled. Without this, wake_workflow_task
        // would refresh sticky_until from the stored sticky_timeout column and
        // re-pin the execution to the old worker even though the current worker
        // is running with sticky routing disabled.
        diesel::sql_query(park_workflow_task_query())
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .load(conn)
            .await
            .map_err(crate::error::database_error)?
    };

    let Some(row) = rows.into_iter().next() else {
        return Err(crate::error::HarvestError::NotFound(format!(
            "workflow task queue item {task_id} is not running"
        )));
    };

    Ok(row.had_wake_requested)
}

/// SQL for [`park_workflow_task`] when a sticky hint is supplied. Extracted as
/// a `const fn` so its shape (the `candidate`/`updated` CTE split that captures
/// `wake_requested` before clearing it) is unit-testable without a database.
const fn park_workflow_task_sticky_query() -> &'static str {
    "WITH candidate AS ( \
         SELECT id, wake_requested FROM harvest_task_queue \
         WHERE id = $1 AND task_type = 'workflow' AND state = 'RUNNING' \
         FOR UPDATE \
     ), \
     updated AS ( \
         UPDATE harvest_task_queue t \
         SET worker_id = NULL, \
             started_at = NULL, \
             sticky_worker_id = $2, \
             sticky_until = NOW() + $3, \
             sticky_timeout = $3, \
             wake_requested = FALSE \
         FROM candidate \
         WHERE t.id = candidate.id \
         RETURNING candidate.wake_requested AS had_wake_requested \
     ) \
     SELECT had_wake_requested FROM updated"
}

/// SQL for [`park_workflow_task`] when no sticky hint is supplied.
const fn park_workflow_task_query() -> &'static str {
    "WITH candidate AS ( \
         SELECT id, wake_requested FROM harvest_task_queue \
         WHERE id = $1 AND task_type = 'workflow' AND state = 'RUNNING' \
         FOR UPDATE \
     ), \
     updated AS ( \
         UPDATE harvest_task_queue t \
         SET worker_id = NULL, \
             started_at = NULL, \
             sticky_worker_id = NULL, \
             sticky_until = NULL, \
             sticky_timeout = NULL, \
             wake_requested = FALSE \
         FROM candidate \
         WHERE t.id = candidate.id \
         RETURNING candidate.wake_requested AS had_wake_requested \
     ) \
     SELECT had_wake_requested FROM updated"
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
    let mut queue_names = primary_repend_workflow_task(conn, exec_id).await?;

    // Dropped-wake fix: if no row was parked and re-pended above, the target
    // workflow task may currently be claimed and mid-processing (state =
    // 'RUNNING', worker_id IS NOT NULL) rather than parked -- e.g. an
    // in-flight decision cycle that dispatched a fan-out and is still
    // executing when a sibling child completes moments later. The UPDATE
    // above cannot re-pend such a row because it does not match the "parked"
    // WHERE clause yet, so this wake would otherwise be silently dropped with
    // no durable trace, forcing the in-flight cycle to park and wait for a
    // wake that already happened and is gone (recovered only by a later,
    // unrelated wake or the next poll-interval sweep). Fall back to marking
    // the row `wake_requested = TRUE`; `park_workflow_task` atomically reads
    // and clears this flag when the in-flight cycle later parks, and re-pends
    // immediately instead of actually parking if it was set -- closing the
    // race without this call ever blocking or retrying.
    if queue_names.is_empty() {
        let fallback_updated = diesel::sql_query(wake_requested_fallback_query())
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

        if fallback_updated == 0 {
            // Closes a residual race (PR #901 review): if `park_workflow_task`'s
            // `candidate SELECT ... FOR UPDATE` already locked this row before the
            // fallback UPDATE above ran, the fallback blocks on that row lock
            // rather than skipping it outright (its WHERE clause matched the row's
            // pre-park, still-owned snapshot). Once the park commits, Postgres
            // re-checks the fallback's WHERE clause against the just-committed row
            // -- which now has `worker_id = NULL` -- so it no longer matches and
            // `wake_requested` is silently never set, even though a park the wake
            // raced against just committed a parked row that is this exact wake's
            // target. Retry the primary re-pend query once: if the row is now
            // genuinely parked (the common outcome of that exact race), this
            // catches it directly instead of depending on `wake_requested`.
            queue_names = primary_repend_workflow_task(conn, exec_id).await?;
        }
    }

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

    queue_names.extend(already_due_queue_names);
    queue_names.sort();
    queue_names.dedup();

    crate::notify::notify_tasks_enqueued(conn, &queue_names, Uuid::nil()).await?;

    Ok(())
}

/// Runs [`wake_workflow_task`]'s primary re-pend `UPDATE`: resets a parked
/// (or elapsed mixed-signal-suspension) workflow task row for `exec_id` back
/// to `PENDING` and returns the queue names of any rows it touched. Extracted
/// so the dropped-wake fallback below can retry it after losing the
/// `park_workflow_task` row-lock race.
///
/// `created_at` is refreshed to `clock_timestamp()` (the wake instant): this
/// re-pends an *old* parked workflow task with a freshly backdated
/// `scheduled_at` (= now - skew), so without refreshing `created_at` the
/// schedule-to-start eligibility floor `GREATEST(scheduled_at, created_at)`
/// would pick the backdated wake timestamp (the stale insert-time `created_at`
/// is older) and report ~skew seconds of phantom latency for an immediately
/// claimed follow-up task (issue #501 review). The wake instant is this cycle's
/// true eligibility, so an immediately-served wake correctly reports ~0.
async fn primary_repend_workflow_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<Vec<String>> {
    use diesel::deserialize::QueryableByName;
    use diesel::sql_types::Text;

    #[derive(QueryableByName)]
    struct QueueNameRow {
        #[diesel(sql_type = Text)]
        queue_name: String,
    }

    let rows: Vec<QueueNameRow> = diesel::sql_query(primary_repend_workflow_task_query())
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Timestamptz, _>(Utc::now() - IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(rows.into_iter().map(|r| r.queue_name).collect())
}

/// SQL for [`primary_repend_workflow_task`]. Extracted as a `const fn` so its
/// WHERE clause is unit-testable without a database.
const fn primary_repend_workflow_task_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET state = 'PENDING', \
         worker_id = NULL, \
         started_at = NULL, \
         scheduled_at = $2, \
         created_at = clock_timestamp(), \
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
     RETURNING queue_name"
}

/// SQL for [`wake_workflow_task`]'s dropped-wake fallback: marks a still-claimed
/// `RUNNING` row (`worker_id IS NOT NULL`) as wake-requested when the primary
/// re-pend UPDATE above matched no parked row. Extracted as a `const fn` so its
/// WHERE clause is unit-testable without a database.
const fn wake_requested_fallback_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET wake_requested = TRUE \
     WHERE workflow_exec_id = $1 \
       AND task_type = 'workflow' \
       AND state = 'RUNNING' \
       AND worker_id IS NOT NULL"
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
/// A distinct claimable-pending demand: how many claimable pending tasks share a
/// `(queue_name, required_capabilities, required_build_id)` triple.
///
/// `required_capabilities` is `None` for tasks with no label requirements and
/// `Some(json)` for tasks that `claim_task` only lets a capability-matching
/// worker take. `required_build_id` is `None` for build-unrouted tasks and
/// `Some(id)` for tasks that `claim_task` only lets a build-matching (or
/// compatible/legacy) worker take. The stranded-work sampler uses this to
/// detect a queue that looks covered (a worker polls it) but carries a task no
/// covering worker can actually claim — because of its labels OR its build.
///
/// `sticky_owner` is `Some(worker_id)` when the row is currently held by an
/// **unexpired** sticky lease (issue #235): `claim_task` lets only that worker
/// claim it until `sticky_until` passes, so coverage must check that specific
/// owner's liveness rather than any worker on the queue. `None` means the row is
/// freely claimable by the general pool (no lease, or the lease has expired).
///
/// `activity_name` carries the row's activity (NULL for workflow tasks). It lets
/// [`apply_activity_requirements`] back-fill `required_capabilities` for activity
/// rows that did not snapshot them (legacy/manual enqueues), mirroring
/// `claim_task`'s ineligible-activities gate (`$6`).
#[derive(Debug, Clone)]
pub struct ClaimablePendingDemand {
    pub queue_name: String,
    pub required_capabilities: Option<serde_json::Value>,
    pub required_build_id: Option<String>,
    pub sticky_owner: Option<String>,
    pub activity_name: Option<String>,
    pub count: i64,
}

/// Per-queue, per-constraint claimable-demand breakdown for the stranded-work
/// sampler and the shard-health coverage gate (issue #522).
///
/// Returns one [`ClaimablePendingDemand`] per distinct
/// `(queue_name, required_capabilities, required_build_id)` triple among
/// claimable pending tasks. Mirrors **every** claim-time gate in `claim_task` so
/// the counts never include a row a worker would refuse:
///   - expired `schedule_to_close_at` (issue #378, failed by the timeout scanner);
///   - PAUSED *workflow* tasks (task_type-scoped — a paused execution's already
///     scheduled activity tasks stay claimable, so they are still counted);
///   - concurrency-cap saturation (the per-key `RUNNING` count is already at the
///     cap, so the row is throttled — the same recheck subquery `claim_task`
///     uses); and
///   - rate-limit exhaustion (the non-circuit bucket has no token), with the
///     **circuit-breaker exemption**: activities in `circuit_breaker_activities`
///     skip the rate-limit gate at claim, so they remain claimable even with an
///     empty bucket and are still counted.
///
/// An unexpired sticky lease (`sticky_worker_id` set, `sticky_until > NOW()`) is
/// surfaced as `ClaimablePendingDemand::sticky_owner` rather than excluded, so
/// the coverage check can require that specific owner to be live (a row leased to
/// a stale worker is not claimable by anyone else until the lease expires).
///
/// `circuit_breaker_activities` is the static set of activity names with a
/// circuit-breaker policy (`CircuitBreakerRegistry::tracked_activity_names`),
/// matching `claim_task`'s `$5` parameter. Pass an empty slice when no breakers
/// are configured.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn claimable_pending_demand_by_queue(
    conn: &mut AsyncPgConnection,
    circuit_breaker_activities: &[String],
) -> HarvestResult<Vec<ClaimablePendingDemand>> {
    #[derive(diesel::QueryableByName)]
    struct DemandRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
        required_capabilities: Option<serde_json::Value>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        required_build_id: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        sticky_owner: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        activity_name: Option<String>,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        cnt: i64,
    }

    // `sticky_owner` is NULL unless an unexpired lease binds the row to a
    // specific worker; the CASE mirrors claim_task's sticky predicate (a row
    // with sticky_until <= NOW() or NULL is freely claimable). It is both
    // selected and repeated in GROUP BY.
    let sticky_owner_expr = "CASE WHEN tq.sticky_worker_id IS NOT NULL \
                                  AND tq.sticky_until IS NOT NULL \
                                  AND tq.sticky_until > NOW() \
                             THEN tq.sticky_worker_id ELSE NULL END";
    let rows: Vec<DemandRow> = diesel::sql_query(format!(
        "SELECT tq.queue_name, tq.required_capabilities, tq.required_build_id, \
                {sticky_owner_expr} AS sticky_owner, tq.activity_name, \
                COUNT(*)::BIGINT AS cnt \
         FROM harvest_task_queue tq \
         LEFT JOIN harvest_workflow_executions e ON e.id = tq.workflow_exec_id \
         WHERE tq.state = 'PENDING' \
           AND tq.scheduled_at <= NOW() \
           AND ( \
               tq.schedule_to_close_at IS NULL \
               OR tq.schedule_to_close_at > NOW() \
           ) \
           AND ( \
               tq.task_type <> 'workflow' \
               OR tq.workflow_exec_id IS NULL \
               OR e.id IS NULL \
               OR e.state <> 'PAUSED' \
           ) \
           AND ( \
               tq.concurrency_key IS NULL \
               OR tq.concurrency_cap IS NULL \
               OR ( \
                   SELECT COUNT(*) FROM harvest_task_queue inner_q \
                   WHERE inner_q.concurrency_key = tq.concurrency_key \
                     AND inner_q.task_type = tq.task_type \
                     AND inner_q.state = 'RUNNING' \
                     AND inner_q.worker_id IS NOT NULL \
               ) < tq.concurrency_cap \
           ) \
           AND ( \
               tq.rate_limit_key IS NULL \
               OR tq.activity_name = ANY($1) \
               OR EXISTS ( \
                   SELECT 1 FROM harvest_rate_limit_buckets b \
                   WHERE b.key = tq.rate_limit_key \
                     AND LEAST(b.burst, b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * b.refill_rate) >= 1.0 \
               ) \
           ) \
         GROUP BY tq.queue_name, tq.required_capabilities, tq.required_build_id, \
                  {sticky_owner_expr}, tq.activity_name"
    ))
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(circuit_breaker_activities)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| ClaimablePendingDemand {
            queue_name: r.queue_name,
            required_capabilities: r.required_capabilities,
            required_build_id: r.required_build_id,
            sticky_owner: r.sticky_owner,
            activity_name: r.activity_name,
            count: r.cnt,
        })
        .collect())
}

/// Back-fill effective capability requirements for un-snapshotted activity rows.
///
/// `claim_task` skips an activity row with NULL `required_capabilities` for any
/// worker that does not satisfy the *registered* `requires` of that activity
/// (its ineligible-activities gate, `$6`). Queue rows from legacy or manual
/// enqueues may carry NULL `required_capabilities` even when the activity has
/// requirements. To reproduce the gate in coverage, for each demand whose
/// `required_capabilities` is `None` and whose `activity_name` has an entry in
/// `activity_requirements`, the activity's requirements are written into
/// `required_capabilities` so the downstream label-match coverage check applies
/// the same gate. Demands that already carry a snapshot, or whose activity
/// declares no `requires`, are left unchanged.
///
/// `activity_requirements` maps `activity_name` → the JSON encoding of its
/// `Vec<Requirement>` (see `HandlerRegistry::activity_requirements_json`).
pub fn apply_activity_requirements<S: std::hash::BuildHasher>(
    demands: &mut [ClaimablePendingDemand],
    activity_requirements: &std::collections::HashMap<String, serde_json::Value, S>,
) {
    if activity_requirements.is_empty() {
        return;
    }
    for demand in demands.iter_mut() {
        if demand.required_capabilities.is_some() {
            continue;
        }
        if let Some(caps) = demand
            .activity_name
            .as_ref()
            .and_then(|name| activity_requirements.get(name))
        {
            demand.required_capabilities = Some(caps.clone());
        }
    }
}

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

    // ── Dropped-wake fix (issue #601 CI hardening) ──────────────────────────

    #[test]
    fn primary_repend_workflow_task_query_targets_parked_and_elapsed_mixed_signal_rows() {
        let sql = primary_repend_workflow_task_query();
        assert!(sql.contains("SET state = 'PENDING'"));
        assert!(sql.contains("worker_id = NULL"));
        assert!(sql.contains("started_at = NULL"));
        assert!(
            sql.contains("state = 'RUNNING' AND worker_id IS NULL AND started_at IS NULL"),
            "must target genuinely parked rows",
        );
        assert!(
            sql.contains("activity_name = 'mixed_signal_suspension'"),
            "must also target an elapsed mixed-signal PENDING row (issue #383)",
        );
    }

    #[test]
    fn wake_requested_fallback_query_targets_only_claimed_running_rows() {
        let sql = wake_requested_fallback_query();
        assert!(sql.contains("SET wake_requested = TRUE"));
        assert!(sql.contains("task_type = 'workflow'"));
        assert!(sql.contains("state = 'RUNNING'"));
        assert!(
            sql.contains("worker_id IS NOT NULL"),
            "fallback must only mark a row that is currently claimed (mid-processing), \
             never a genuinely parked or PENDING row",
        );
    }

    #[test]
    fn park_workflow_task_queries_capture_wake_requested_before_clearing_it() {
        for sql in [
            park_workflow_task_query(),
            park_workflow_task_sticky_query(),
        ] {
            assert!(
                sql.contains("FOR UPDATE"),
                "must lock the row before reading wake_requested, closing the gap \
                 with wake_workflow_task's fallback UPDATE",
            );
            assert!(sql.contains("SELECT id, wake_requested FROM harvest_task_queue"));
            assert!(
                sql.contains("wake_requested = FALSE"),
                "must clear the flag as part of the same statement that reads it",
            );
            assert!(
                sql.contains("RETURNING candidate.wake_requested AS had_wake_requested"),
                "must return the PRE-update value (from the candidate CTE), not the \
                 just-cleared post-update value",
            );
            assert!(sql.contains("state = 'RUNNING'"));
        }
    }

    #[test]
    fn park_workflow_task_sticky_query_sets_sticky_columns() {
        let sql = park_workflow_task_sticky_query();
        assert!(sql.contains("sticky_worker_id = $2"));
        assert!(sql.contains("sticky_until = NOW() + $3"));
        assert!(sql.contains("sticky_timeout = $3"));
    }

    #[test]
    fn park_workflow_task_query_clears_sticky_columns() {
        let sql = park_workflow_task_query();
        assert!(sql.contains("sticky_worker_id = NULL"));
        assert!(sql.contains("sticky_until = NULL"));
        assert!(sql.contains("sticky_timeout = NULL"));
    }

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
    fn schedule_to_start_uses_eligibility_floor() {
        let now = Utc::now();

        // Immediate task: scheduled_at backdated by the skew allowance, created_at is
        // the real insert-time eligibility. Claimed instantly it must report ~0, not
        // the backdating (floor = created_at).
        let created = now;
        let immediate_scheduled = now - IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE;
        assert!(
            schedule_to_start_secs(immediate_scheduled, Some(created), now) < 0.001,
            "promptly-served immediate task must not report the skew backdating"
        );

        // Immediate task that genuinely waited 10s: enqueued (created) 10s ago, with
        // scheduled_at backdated a further skew. Reports the full 10s (floor = created).
        let created_10s_ago = now - Duration::seconds(10);
        let scheduled_backdated = created_10s_ago - IMMEDIATE_SCHEDULE_SKEW_ALLOWANCE;
        let secs = schedule_to_start_secs(scheduled_backdated, Some(created_10s_ago), now);
        assert!((secs - 10.0).abs() < 0.05, "expected ~10s wait, got {secs}");

        // Delayed/retried task (issue #501 review): scheduled_at set to an explicit
        // instant 10s ago with NO backdating, created_at long before. The full 10s
        // must be reported — the prior fixed discount would have under-reported it.
        let scheduled_explicit = now - Duration::seconds(10);
        let created_long_ago = now - Duration::seconds(300);
        let delayed = schedule_to_start_secs(scheduled_explicit, Some(created_long_ago), now);
        assert!(
            (delayed - 10.0).abs() < 0.05,
            "delayed/retried task must report its full wait with no discount, got {delayed}"
        );

        // Pre-upgrade row (created_at = None) falls back to scheduled_at.
        let legacy = schedule_to_start_secs(now - Duration::seconds(7), None, now);
        assert!(
            (legacy - 7.0).abs() < 0.05,
            "None created_at must use scheduled_at, got {legacy}"
        );

        // Never negative, even if start precedes the eligibility floor.
        assert!(
            schedule_to_start_secs(
                now + Duration::seconds(1),
                Some(now - Duration::seconds(300)),
                now
            ) < f64::EPSILON
        );
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

    fn demand(
        queue: &str,
        caps: Option<serde_json::Value>,
        activity: Option<&str>,
    ) -> ClaimablePendingDemand {
        ClaimablePendingDemand {
            queue_name: queue.to_string(),
            required_capabilities: caps,
            required_build_id: None,
            sticky_owner: None,
            activity_name: activity.map(str::to_string),
            count: 1,
        }
    }

    #[test]
    fn apply_activity_requirements_backfills_unsnapshotted_rows() {
        let gpu = serde_json::json!([{"Exact": {"key": "gpu", "value": "true"}}]);
        let mut reqs = std::collections::HashMap::new();
        reqs.insert("render".to_string(), gpu.clone());

        let mut demands = vec![
            // NULL caps + activity has requires → back-filled.
            demand("default", None, Some("render")),
            // NULL caps + activity has no requires → untouched.
            demand("default", None, Some("plain")),
            // NULL caps + no activity (workflow task) → untouched.
            demand("default", None, None),
        ];
        apply_activity_requirements(&mut demands, &reqs);

        assert_eq!(demands[0].required_capabilities, Some(gpu));
        assert_eq!(demands[1].required_capabilities, None);
        assert_eq!(demands[2].required_capabilities, None);
    }

    #[test]
    fn apply_activity_requirements_keeps_existing_snapshot() {
        // A row that already snapshotted its capabilities must not be overwritten
        // by the registered requires (the snapshot is authoritative).
        let snapshot = serde_json::json!([{"Exact": {"key": "zone", "value": "eu"}}]);
        let registered = serde_json::json!([{"Exact": {"key": "gpu", "value": "true"}}]);
        let mut reqs = std::collections::HashMap::new();
        reqs.insert("render".to_string(), registered);

        let mut demands = vec![demand("default", Some(snapshot.clone()), Some("render"))];
        apply_activity_requirements(&mut demands, &reqs);

        assert_eq!(demands[0].required_capabilities, Some(snapshot));
    }

    #[test]
    fn apply_activity_requirements_empty_map_is_noop() {
        let mut demands = vec![demand("default", None, Some("render"))];
        apply_activity_requirements(&mut demands, &std::collections::HashMap::new());
        assert_eq!(demands[0].required_capabilities, None);
    }
}
