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
// Dynamic per-key rate-limit bucket keys (issue #699)
// ---------------------------------------------------------------------------

/// Namespace prefix for a *dynamic* per-key activity rate-limit bucket stored in
/// `harvest_rate_limit_buckets`.
///
/// Distinct from a *static* [`EnqueueParams::rate_limit_key`] (a bare activity
/// name or an author-supplied `rate_limit_key` string) and from the throttle
/// bucket prefix (`start-throttle:`), so a dynamic per-tenant bucket can never
/// collide with either.
pub const DYNAMIC_RATE_PREFIX: &str = "dyn-rate";

/// Maximum length (bytes) of any single *component* (the normalized expression
/// or the resolved value) of a dynamic per-key bucket key before it is replaced
/// with a stable hash (issue #699, btree-PK safety).
///
/// The composite key is the PRIMARY KEY of `harvest_rate_limit_buckets`. A
/// pathologically large component — a giant tenant id or non-scalar field
/// serialized to JSON in `resolved`, or a very long dot-path / hand-built
/// `ActivityInfo.rate_limit_key` in the expression — would exceed the Postgres
/// btree PK size limit (~2704 bytes) and abort the enqueue transaction in a
/// retry loop, wedging the workflow. Bounding *both* components keeps the whole
/// composite key provably well under that limit (≤ ~530 bytes) while leaving
/// ordinary short values human-readable.
const MAX_KEY_COMPONENT_LEN: usize = 256;

/// Bound a single dynamic-key component to at most [`MAX_KEY_COMPONENT_LEN`]
/// bytes (issue #699, btree-PK safety) with **structurally disjoint** literal
/// and hash encodings so a literal value can never coincide with a hash
/// encoding (issue #699 review, Codex P2):
///
/// - A value within the bound is emitted as a **length-tagged literal**:
///   `L{byte_len}:{value}` (starts with `L`, self-delimiting).
/// - A longer value is replaced with a deterministic, **collision-resistant
///   SHA-256** digest: `H{64hex}` (starts with `H`, fixed 65 bytes).
///
/// The digest is the full 256-bit SHA-256 of the component's UTF-8 bytes,
/// hex-encoded. This is deliberately a *cryptographic* digest rather than the
/// 64-bit `seahash` this originally used: the component is tenant-influenced
/// input (a resolved per-execution value, or a hand-built
/// `ActivityInfo.rate_limit_key`), so two distinct oversized values sharing a
/// 64-bit digest would silently share a `dyn-rate` bucket — the `L`/`H` tags
/// fix *format* collisions, not *digest* collisions. SHA-256 makes a digest
/// collision computationally infeasible, so distinct oversized components get
/// distinct buckets in practice.
///
/// The `L`/`H` first-byte tags make the two forms provably disjoint: a short
/// literal whose value happens to be a 65-byte string beginning with `H`
/// followed by 64 hex characters encodes to `L65:H…`, which can never equal a
/// hash encoding `H…`. Without the tag, `h:{digest}` was itself a valid short
/// literal, so a long value hashing to digest `D` and a short literal value
/// equal to the string `h:{D}` produced the same component — cross-tenant
/// bucket sharing without a hash collision. Applied to both the expression and
/// the resolved value so neither can blow the composite PK size, and so the
/// whole composite key is injective (each component is self-delimiting: an
/// `L{len}:{len-bytes}` literal or a fixed-width `H{64hex}` hash).
fn bound_key_component(component: &str) -> String {
    if component.len() > MAX_KEY_COMPONENT_LEN {
        use sha2::{Digest, Sha256};
        use std::fmt::Write as _;
        let digest = Sha256::digest(component.as_bytes());
        let mut out = String::with_capacity(1 + 64);
        out.push('H');
        for b in digest {
            let _ = write!(out, "{b:02x}");
        }
        out
    } else {
        format!("L{}:{}", component.len(), component)
    }
}

/// Marker for the *resolved-value* component of a dynamic per-key bucket key
/// when the key expression could **not** be resolved from the workflow input
/// (missing field / null / non-object input), i.e. `resolve_concurrency_key`
/// returned `None`.
///
/// Structurally disjoint from every [`bound_key_component`] output (which always
/// begins with `L` or `H`), so the *unresolved* fallback bucket
/// `dyn-rate:{expr}:U` can never coincide with a *resolved* bucket — in
/// particular with `dyn-rate:{expr}:L0:`, which is a legitimately-resolved
/// **empty-string** tenant (`Some("")`). The previous `.unwrap_or_default()` at
/// the call site collapsed both `None` (unresolved / missing / malformed input)
/// and `Some("")` (a real empty-string tenant) onto the same `L0:` bucket,
/// cross-throttling malformed/missing executions against a legitimate empty
/// tenant (issue #699 review, Codex round-5 P2, worker.rs:4720).
const UNRESOLVED_RESOLVED_MARKER: &str = "U";

/// Build the token-bucket key for a *dynamic* per-key activity rate limit
/// (issue #699).
///
/// The key is namespaced by both the dot-path *expression* and the resolved
/// per-execution *value* — `dyn-rate:{expr_component}:{resolved_component}` — so
/// that activities declaring the same expression share one bucket per resolved
/// value (matching the static limiter's "same key → shared bucket → must agree
/// on rps" rule), while distinct tenants get independent buckets. `resolved` is
/// `None` for an execution whose key expression could not be resolved (missing /
/// null / non-object input); it encodes as the distinct
/// [`UNRESOLVED_RESOLVED_MARKER`] (`U`), giving one shared fallback bucket per
/// expression (`dyn-rate:{expr}:U`) so an unkeyed execution is still bounded
/// rather than unbounded. Crucially, that unresolved bucket is **distinct** from
/// `Some("")` — a legitimately-resolved empty-string tenant — which buckets
/// under `L0:` (`dyn-rate:{expr}:L0:`); the `U`/`L`-tag disjointness stops a
/// malformed/missing input from cross-throttling a real empty-string tenant
/// (issue #699 review, Codex round-5 P2).
///
/// `key_expr` is normalized by stripping a leading `input.` (issue #699 review):
/// `resolve_concurrency_key` treats `"input.tenant_id"` and `"tenant_id"` as the
/// same field, so the two spellings must resolve to the same bucket — the strip
/// here mirrors the strip in [`crate::builder`]'s dynamic-key validation.
///
/// *Both* the normalized `expr` and the `resolved` value are byte-length-bounded
/// ([`MAX_KEY_COMPONENT_LEN`], via [`bound_key_component`]): a component whose
/// UTF-8 byte length exceeds the bound is replaced with a stable, collision-
/// resistant SHA-256 digest so a pathological value — an oversized resolved
/// tenant id *or* a very
/// long dot-path / hand-built `ActivityInfo.rate_limit_key` expression — can
/// never blow the btree PK size limit and wedge the enqueue transaction. The
/// byte-length check matches the Postgres btree PK limit (which is byte-based)
/// and is O(1) on the hot enqueue path. With both components bounded the whole
/// composite key is provably ≤ ~530 bytes regardless of the macro or a
/// hand-built `ActivityInfo`.
///
/// The key is **injective by construction**: the `expr` component is emitted by
/// [`bound_key_component`] as either a length-tagged literal `L{len}:{bytes}`
/// (self-delimiting: after the `L`, the decimal `len` up to the next `:` says
/// exactly how many bytes follow) or a fixed-width hash `H{64hex}`, and the
/// `resolved` component is either the same (`Some`) or the single-byte
/// [`UNRESOLVED_RESOLVED_MARKER`] (`U`, for `None`). The `L`/`H`/`U` first-byte
/// tags are structurally disjoint, so a literal value equal to a hash's
/// encoding never collides with that hash, an unresolved `U` bucket never
/// collides with any resolved `L`/`H` bucket (in particular `Some("")`'s `L0:`),
/// and a `:` inside `expr` (a dot-path can address a JSON key containing `:`) or
/// inside `resolved` can never split ambiguously: the pair `(expr, resolved)`
/// maps to exactly one key and vice-versa, whether either component passed
/// through unchanged or was hashed. Without this self-delimiting encoding,
/// `key="a"` + resolved `"b:c"` and `key="a:b"` + resolved `"c"` would both
/// flatten to `dyn-rate:a:b:c` — two distinct exprs sharing one bucket, so their
/// (independently validated, possibly different) rps configs would collide
/// first-writer-wins.
#[must_use]
pub fn dynamic_rate_bucket_key(key_expr: &str, resolved: Option<&str>) -> String {
    let expr = key_expr.strip_prefix("input.").unwrap_or(key_expr);
    // The expr component is self-delimiting (`L{len}:{bytes}` or `H{16hex}`);
    // the resolved component is either the same (`Some`) or the disjoint
    // single-byte `U` marker (`None`, unresolved). `None` (missing/null/
    // non-object input) is kept DISTINCT from `Some("")` (a legitimately-
    // resolved empty-string tenant, which encodes as `L0:`) so the two never
    // share a bucket -- issue #699 review, Codex round-5 P2. The whole composite
    // key stays injective (the `L`/`H`/`U` first-byte tags are disjoint).
    let resolved_component = resolved.map_or_else(
        || UNRESOLVED_RESOLVED_MARKER.to_string(),
        bound_key_component,
    );
    format!(
        "{DYNAMIC_RATE_PREFIX}:{}:{}",
        bound_key_component(expr),
        resolved_component,
    )
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
    /// Worker session this activity belongs to (issue #606). When `Some`,
    /// combined with [`Self::sticky_worker_id`] the claim query **hard-pins**
    /// this row to that worker -- unlike ordinary sticky routing, it never
    /// fails over to a different worker even after the sticky lease expires,
    /// since the session's local state only exists on that one worker.
    /// `None` for an ordinary (non-session) activity.
    pub session_id: Option<Uuid>,
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
            session_id: None,
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

    /// Mark this task as belonging to worker session `session_id` (issue
    /// #606). Combine with [`Self::with_sticky`] to hard-pin the row to the
    /// session's host worker -- `claim_task`'s session gate ignores
    /// `sticky_until` entirely for a session-tagged row.
    #[must_use]
    pub const fn with_session_id(mut self, session_id: Uuid) -> Self {
        self.session_id = Some(session_id);
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
        session_id: params.session_id,
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
// NOTE (issue #619 / Ledger perf pass, buffers -98% @10k): the queue-pause
// exclusion used to be a per-row correlated `NOT EXISTS`, embedded verbatim
// and drift-locked literally to
// `queue_pause::queue_pause_anti_join("harvest_task_queue")`. Measurement
// (docs/performance.md) showed the planner re-probing `harvest_queue_pauses`
// once for every *candidate row* it walked while sorting toward `LIMIT 1` —
// not once per claim as the module doc for issue #619 assumes — accounted for
// ~99% of this query's buffer touches at a 10k-row backlog. `paused_queues`
// below pre-filters the (already `$2`-bounded) pause table into one small
// array in a `MATERIALIZED` CTE — evaluated once, not once per row — and the
// per-row check becomes a plain array-membership test with identical
// semantics: a queue is excluded from `candidate` iff it has an active pause
// row, exactly as before. This is deliberately NOT textually drift-locked to
// `queue_pause::queue_pause_anti_join` any more; see
// `claim_query_excludes_every_paused_queue_via_a_prefiltered_array` below for
// the shape this is held to instead, and
// `tests/queue_pause_tests.rs::claim_query_excludes_paused_queues_end_to_end`
// for the DB-backed equivalence proof.
/// The task-claim query — extracted as a `const fn` (mirroring the
/// `timeout.rs` `*_query()` convention) so its eligibility predicate is
/// shape-testable without a database.
///
/// Binds: `$1` worker id, `$2` queue names, `$3` worker build id,
/// `$4` priority-aging seconds, `$5` circuit-breaker-tracked activities,
/// `$6` ineligible activities. The queue-pause exclusion (issue #619) needs no
/// new bind — it reuses `$2`, pre-filtering `harvest_queue_pauses` to the
/// worker's own polled queues once via `paused_queues` rather than probing it
/// once per candidate row (see the `NOTE` above `claim_task_query` for why).
///
/// The per-activity-type pause (issue #807) needs no bind either: unlike
/// `paused_queues` there is no natural bound to pre-filter by — a worker's
/// polled queues say nothing about which activity types are held — so
/// `paused_activities` reads the whole table. That is deliberate and cheap: the
/// table is keyed by activity name and holds one row per *currently paused*
/// activity, so it is empty in steady state and a handful of rows during an
/// incident. It is `MATERIALIZED` for the same reason `paused_queues` is —
/// evaluated once per claim, never once per candidate row.
///
/// **Both activity-name gates (`$6` and `paused_activities`) are guarded by
/// `task_type != 'activity' OR activity_name IS NULL` and this is load-bearing,
/// not defensive noise.** `activity_name` is `NULL` for workflow tasks, and in
/// SQL `NULL = ANY(array)` is `NULL` — so `NOT (activity_name = ANY(...))` is
/// `NULL`, which is not `TRUE`, which excludes the row. Without the guard a
/// single paused activity would silently stop every *workflow* task in the
/// fleet from being claimed: a total orchestration outage produced by a
/// surgical containment control. Pinned by
/// `claim_query_activity_pause_gate_never_holds_workflow_tasks`.
// The body is one SQL string literal; the line count is the query's, not
// control flow's. `claim_task` carried the same allow before this query was
// extracted for shape-testing.
#[allow(clippy::too_many_lines)]
#[must_use]
pub const fn claim_task_query() -> &'static str {
    "WITH worker_info AS ( \
             SELECT COALESCE((SELECT labels FROM harvest_workers WHERE worker_id = $1), '{}'::jsonb) AS labels \
         ), \
         paused_queues AS MATERIALIZED ( \
             SELECT COALESCE(array_agg(queue_name), ARRAY[]::text[]) AS names \
             FROM harvest_queue_pauses \
             WHERE queue_name = ANY($2) \
         ), \
         paused_activities AS MATERIALIZED ( \
             SELECT COALESCE(array_agg(activity_name), ARRAY[]::text[]) AS names \
             FROM harvest_activity_pauses \
         ), \
         candidate AS ( \
             SELECT id, task_type, concurrency_key, concurrency_cap, rate_limit_key, activity_name \
             FROM harvest_task_queue \
             CROSS JOIN worker_info \
             CROSS JOIN paused_queues \
             CROSS JOIN paused_activities \
             WHERE queue_name = ANY($2) \
               AND state = 'PENDING' \
               AND scheduled_at <= NOW() \
               AND NOT (harvest_task_queue.queue_name = ANY(paused_queues.names)) \
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
                   session_id IS NULL \
                   OR sticky_worker_id = $1 \
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
                   task_type != 'activity' \
                   OR activity_name IS NULL \
                   OR NOT (activity_name = ANY(paused_activities.names)) \
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
                         AND LEAST(COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_burst ELSE NULL END, b.burst), b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_refill_rate ELSE NULL END, b.refill_rate)) >= 1.0 \
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
            SET tokens = LEAST(COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_burst ELSE NULL END, b.burst), b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_refill_rate ELSE NULL END, b.refill_rate)) - 1.0, \
                last_refilled_at = NOW() \
            FROM candidate \
            WHERE b.key = candidate.rate_limit_key \
              AND NOT (candidate.activity_name = ANY($5)) \
              AND LEAST(COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_burst ELSE NULL END, b.burst), b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_refill_rate ELSE NULL END, b.refill_rate)) >= 1.0 \
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
        SELECT * FROM claimed"
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
    // Known limitation — safe-side rate-limit token leak under concurrency
    // contention (pre-existing, shared with the static-rate path; issue #699
    // review, #9): the `rate_limit_debit` CTE debits a token whenever the
    // candidate's bucket has one, but the `claimed` UPDATE can still match 0 rows
    // if a per-key concurrency (`concurrency_key`) advisory-lock race is lost or
    // the re-checked cap is now saturated. In that case the token is spent but
    // the task is NOT claimed. This is SAFE-SIDE (it over-throttles: the
    // protective invariant "a real call always holds a token" is never violated —
    // the leak only ever *reduces* dispatch below budget, never allows a claim
    // without a token), bounded (at most one token per lost claim attempt), and
    // self-healing (the bucket refills at `refill_rate`, so a leaked token is
    // recovered within one refill interval). Fixing it would require moving the
    // debit inside the `claimed` UPDATE's success condition — a change to this
    // hot-path CTE for a pre-existing, harmless-direction edge — so it is
    // deliberately left as-is; a scoped follow-up may revisit it. It applies
    // identically to a task carrying both a per-key concurrency cap and any rate
    // limit (static #332 or dynamic per-key #699).
    //
    // Pause gating (issue #383): workflow tasks whose execution is in the
    // `PAUSED` state are never claimed. They stay PENDING (or parked) until the
    // execution is resumed, at which point they become claimable again. This is
    // the single executor-layer chokepoint that defers timer fires, signal
    // deliveries, and activity-completion wakes uniformly while paused — no
    // workflow-author cooperation required. In-flight activity tasks are not
    // `task_type = 'workflow'` and so continue to run to completion.
    //
    // Worker-session hard pin (issue #606): a row with a non-NULL `session_id`
    // is claimable *only* by its `sticky_worker_id` -- unlike the ordinary
    // sticky gate above, this condition ignores `sticky_until` entirely, so a
    // session member activity never fails over to a different worker even
    // after the (purely bookkeeping) sticky lease expires. The session's
    // local state (a downloaded file, GPU memory, ...) only exists on the
    // host worker, so failover would silently produce wrong results rather
    // than a safe-but-slow retry.
    let aging_secs_i64: Option<i64> = priority_aging_secs.map(i64::from);

    // The claim and the authoritative queue-pause re-check run in ONE
    // explicitly-`READ COMMITTED` transaction (issue #619 round-17 review).
    //
    // # Why one transaction
    //
    // As two autocommit statements, the claim commits on its own. If the
    // re-check then fails — a transient connection error, a statement
    // cancellation, a pool timeout — the `?` propagates while the task is
    // already `RUNNING` with its `attempt` consumed and **no** worker holding
    // it. Recovery is worse than it first looks: the poison-pill reclaimer
    // (issue #367) only reclaims `RUNNING` rows whose `worker_id` has no live
    // `harvest_workers` heartbeat, and this worker is alive and still
    // heartbeating, so it never qualifies. An activity configured with neither
    // `start_to_close` nor `heartbeat_timeout` would therefore stay stranded
    // for as long as the worker lives. Inside a transaction, a re-check error
    // rolls the claim back: the row returns to `PENDING` with its `attempt`
    // intact and the next poll re-claims it.
    //
    // # Why the isolation level is pinned rather than inherited
    //
    // The round-2 contract — *a pause committed before the re-check begins
    // always wins* — depends entirely on the re-check getting a **fresh**
    // snapshot. Under `READ COMMITTED` each statement takes its own snapshot,
    // so that holds inside a transaction exactly as it did across two
    // autocommit statements. Under `REPEATABLE READ` it does **not**: both
    // statements would share the transaction's single snapshot and the
    // re-check could never observe a pause committed after the claim began,
    // silently reinstating the very P1 this re-check exists to close.
    //
    // Two autocommit statements were immune to that by construction (each is
    // its own single-statement transaction, snapshotted at statement start
    // whatever the isolation level), so wrapping them without pinning would
    // hand an operator a way to disable the guarantee from outside the code —
    // a `default_transaction_isolation = repeatable read` set on the database
    // or the role. `build_transaction().read_committed()` emits the level on
    // the `BEGIN` itself, so the guarantee travels with the query.
    //
    // # Cost
    //
    // The claim's `FOR UPDATE SKIP LOCKED` row locks (and its rate-limit
    // bucket lock) are now held for one extra indexed primary-key `UPDATE`
    // against the row this transaction already locked. Competing claimers
    // `SKIP LOCKED` past that row regardless — it is `RUNNING` either way — so
    // the added contention is the duration of a single PK probe.
    let mut tx = conn.build_transaction().read_committed();
    let claimed: Option<TaskQueueItem> = tx
        .run(
            async |conn: &mut AsyncPgConnection| -> HarvestResult<Option<TaskQueueItem>> {
                let result: Vec<TaskQueueItem> = diesel::sql_query(claim_task_query())
                    .bind::<diesel::sql_types::Text, _>(worker_id)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
                    .bind::<diesel::sql_types::Text, _>(worker_build_id)
                    .bind::<diesel::sql_types::Nullable<diesel::sql_types::BigInt>, _>(
                        aging_secs_i64,
                    )
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                        circuit_breaker_activities,
                    )
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(
                        ineligible_activities,
                    )
                    .load(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                let Some(task) = result.into_iter().next() else {
                    return Ok(None);
                };

                // Commit-order barrier (issue #619 round-18 review). The
                // re-check below reads a snapshot taken *before* this
                // transaction commits, so on its own it cannot stop a pause
                // from committing — and being acknowledged to the operator —
                // in the window between that snapshot and our COMMIT. Taking
                // the *shared* mode of the key `pause_queue` takes exclusively
                // makes that impossible: a pause cannot commit while any claim
                // holds it, so a claim can only ever commit *before* the hold
                // is acknowledged.
                //
                // It must be the `try` variant and it must be here rather than
                // before the claim: the queue is not known until a task is in
                // hand, so this path holds task rows and then wants the lock,
                // the inverse of `resume_queue`'s advisory-then-rows order. A
                // blocking acquire would be an ABBA deadlock; a failed `try`
                // simply means a pause or resume is committing right now, so we
                // give the claim back and let the next poll re-decide against
                // committed state. See `queue_pause::try_lock_queue_for_claim`.
                if !crate::queue_pause::try_lock_queue_for_claim(conn, &task.queue_name).await? {
                    crate::queue_pause::release_claim(conn, task.id, worker_id).await?;
                    return Ok(None);
                }

                // Authoritative queue-pause re-check (issue #619). The anti-join in
                // the claim above is evaluated against that statement's snapshot,
                // so a pause committing while the claim was in flight is invisible
                // to it and the task would be dispatched into the very outage the
                // hold exists to ride out. This is a *separate statement* and so
                // takes a fresh snapshot (guaranteed by the pinned `READ COMMITTED`
                // above), releasing the claim back to `PENDING` if the queue is now
                // held. Holding the shared queue lock above makes its verdict
                // authoritative *through commit* rather than only as of its own
                // snapshot. See `queue_pause::release_claim_if_queue_paused` for
                // why an exclusive lock inside the claim itself was rejected: it
                // would serialize all claims for a queue and defeat `SKIP LOCKED`.
                if crate::queue_pause::release_claim_if_queue_paused(conn, task.id, worker_id)
                    .await?
                {
                    return Ok(None);
                }

                // Authoritative activity-pause re-check (issue #807). Same
                // reasoning as the queue-pause re-check directly above: the
                // `paused_activities` pre-filter in the claim is evaluated
                // against that statement's snapshot, so a pause committing
                // while the claim was in flight is invisible to it. This is a
                // separate statement and therefore takes a fresh snapshot.
                //
                // Gated on `task_type` so a workflow task -- which can never be
                // held by an activity pause -- pays no extra round trip on the
                // hot claim path. The guard is not merely an optimization: it
                // makes the added cost fall only on the rows the feature can
                // actually hold.
                //
                // Unlike the queue-pause path there is no advisory-lock barrier
                // here, so this verdict is authoritative as of its own snapshot
                // rather than through commit. See the "Residual window" section
                // on `activity_pause::release_claim_if_activity_paused` for the
                // precise bound and why it is accepted.
                if task.task_type == crate::activity_pause::ACTIVITY_TASK_TYPE
                    && crate::activity_pause::release_claim_if_activity_paused(
                        conn, task.id, worker_id,
                    )
                    .await?
                {
                    return Ok(None);
                }

                Ok(Some(task))
            },
        )
        .await?;

    Ok(claimed)
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
    /// Workflow types observed on this key, each with the overflow strategy
    /// their registered [`crate::concurrency::ConcurrencyPolicy`] declares
    /// (issue #811).
    ///
    /// **Populated only by the management API** (`GET /admin/concurrency`),
    /// which has the handler registry needed to resolve a declared strategy.
    /// The worker's in-process concurrency sampler leaves this empty — it reads
    /// only the counters above, so the extra join is never on its hot path.
    /// `skip_serializing_if` keeps the sampler-shaped value byte-identical to
    /// the pre-#811 wire form.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workflows: Vec<ConcurrencyWorkflowStrategy>,
}

/// A workflow type observed on a concurrency key, with the overflow strategy
/// its registered policy declares (issue #811).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConcurrencyWorkflowStrategy {
    /// The workflow type name.
    pub workflow_name: String,
    /// Effective `on_conflict` strategy for new starts of this workflow type:
    /// `"defer"` (wait for a slot) or `"cancel_running"` (latest-wins — cancel
    /// the oldest running run(s) and admit the newcomer).
    ///
    /// A workflow whose registered `WorkflowInfo` declares no concurrency
    /// policy reports `defer`, matching what the start path resolves.
    pub on_conflict: crate::concurrency::ConcurrencyOnConflict,
}

/// A `(concurrency_key, task_type)` group and one workflow type observed on it.
///
/// Returned by [`concurrency_key_workflows`]; the management API joins these
/// against the handler registry to resolve each workflow's declared
/// `on_conflict` strategy (issue #811).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcurrencyKeyWorkflow {
    /// The concurrency group key.
    pub key: String,
    /// Task type this row covers (`"workflow"` or `"activity"`).
    pub task_type: String,
    /// Workflow type owning the task(s) in this group.
    pub workflow_name: String,
}

/// SQL for [`concurrency_key_stats`].
///
/// Named (rather than inlined) so the drift guard can read BOTH this and
/// [`CONCURRENCY_ATTRIBUTION_SQL`] and assert they share the same row filter —
/// checking only one side could not detect drift in the other.
const CONCURRENCY_STATS_SQL: &str = "SELECT \
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
         GROUP BY concurrency_key, task_type";

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

    let rows: Vec<Row> = diesel::sql_query(CONCURRENCY_STATS_SQL)
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
            workflows: Vec::new(),
        })
        .collect())
}

/// Return the workflow types observed on each live `(concurrency_key,
/// task_type)` group in the given queues (issue #811).
///
/// Mirrors [`concurrency_key_stats`]'s row filter exactly, joined to
/// `harvest_workflow_executions` so each group can be attributed to the
/// workflow type(s) that produced it. The management API joins the result
/// against the handler registry to report each workflow's declared
/// `on_conflict` strategy; the worker's sampler never calls this.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn concurrency_key_workflows(
    conn: &mut AsyncPgConnection,
    queues: &[String],
) -> HarvestResult<Vec<ConcurrencyKeyWorkflow>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        key: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        task_type: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
    }

    let rows: Vec<Row> = diesel::sql_query(CONCURRENCY_ATTRIBUTION_SQL)
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| ConcurrencyKeyWorkflow {
            key: r.key,
            task_type: r.task_type,
            workflow_name: r.workflow_name,
        })
        .collect())
}

/// SQL for [`concurrency_key_workflows`].
///
/// Named (rather than inlined) so a unit test can assert it shares
/// [`concurrency_key_stats`]'s row filter — the two are separate statements
/// and would otherwise be free to drift.
const CONCURRENCY_ATTRIBUTION_SQL: &str = "SELECT \
             t.concurrency_key AS key, \
             t.task_type, \
             e.workflow_name \
         FROM harvest_task_queue t \
         JOIN harvest_workflow_executions e ON e.id = t.workflow_exec_id \
         WHERE t.concurrency_key IS NOT NULL \
           AND t.concurrency_cap IS NOT NULL \
           AND t.queue_name = ANY($1) \
           AND (t.state = 'PENDING' OR (t.state = 'RUNNING' AND t.worker_id IS NOT NULL)) \
         GROUP BY t.concurrency_key, t.task_type, e.workflow_name";

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
/// The `oldest_pending_ages` gauge query — extracted so its claim-gate mirror
/// (including the issue #619 queue-pause anti-join) is shape-testable.
///
/// Binds: `$1` queue names, `$2` circuit-breaker-tracked activities.
#[must_use]
pub const fn oldest_pending_ages_query() -> &'static str {
    "SELECT queue_name, \
                GREATEST( \
                    EXTRACT(EPOCH FROM (NOW() - MIN(GREATEST(scheduled_at, COALESCE(created_at, scheduled_at))))), \
                    0 \
                )::DOUBLE PRECISION AS age_secs \
         FROM harvest_task_queue \
         WHERE queue_name = ANY($1) \
           AND state = 'PENDING' \
           AND scheduled_at <= NOW() \
           AND NOT EXISTS (SELECT 1 FROM harvest_queue_pauses qp \
         WHERE qp.queue_name = harvest_task_queue.queue_name) \
           AND NOT EXISTS (SELECT 1 FROM harvest_activity_pauses ap \
         WHERE ap.activity_name = harvest_task_queue.activity_name \
           AND harvest_task_queue.task_type = 'activity') \
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
                     AND LEAST(COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_burst ELSE NULL END, b.burst), b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at)) * COALESCE(CASE WHEN b.override_expires_at > NOW() THEN b.override_refill_rate ELSE NULL END, b.refill_rate)) >= 1.0 \
               ) \
           ) \
         GROUP BY queue_name"
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

    let rows: Vec<Row> = diesel::sql_query(oldest_pending_ages_query())
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
///
/// `treat_none_as_null = true` is required: Diesel's default `AsChangeset`
/// behavior treats a `None` field as "omit this column from `SET`" rather
/// than "set it to `NULL`", which would silently stop `worker_id`,
/// `started_at`, and `last_heartbeat_at` from ever being cleared on requeue
/// (the pre-refactor code used explicit `.eq(None::<T>)` per column, which
/// is unaffected by this default and was correct).
#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::harvest_task_queue, treat_none_as_null = true)]
struct PendingRequeueChangeset {
    state: &'static str,
    worker_id: Option<String>,
    started_at: Option<chrono::DateTime<Utc>>,
    last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    crash_strikes: i32,
    /// Issue #804: reset on every shared pending-requeue, because reaching this
    /// path PROVES the claiming worker was capable (it resolved a handler and
    /// ran it). Keeps the counter measuring *consecutive* capability misses,
    /// the same semantics `crash_strikes` above has.
    capability_misses: i32,
    /// Issue #804 (round 6): cleared alongside `capability_misses` so the
    /// distinct-worker set and the total counter can never disagree about
    /// whether the streak was broken.
    capability_miss_workers: Vec<String>,
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
            capability_misses: 0,
            capability_miss_workers: Vec::new(),
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

/// Build the `SET` clause used by [`requeue_workflow_task_after_panic`] so a
/// no-DB unit test can assert the generated SQL shape (issue #782). Mirrors the
/// `park_workflow_task_query`/`PendingRequeueChangeset` shape-test precedent.
///
/// Takes the changeset by value so the returned query owns it (the caller only
/// needs the SQL text, never to execute it).
#[cfg(test)]
fn requeue_after_panic_query(changeset: PendingRequeueChangeset) -> String {
    use crate::schema::harvest_task_queue::dsl;
    use diesel::debug_query;
    use diesel::pg::Pg;

    let query = diesel::update(
        dsl::harvest_task_queue
            .find(Uuid::nil())
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
    ));
    debug_query::<Pg, _>(&query).to_string()
}

/// Re-pend a workflow task after a **contained handler panic** with a future
/// `scheduled_at` (issue #782).
///
/// Behaviourally identical to [`requeue_workflow_task_nd_blocked`] — it reuses
/// the shared [`PendingRequeueChangeset`] (task → `PENDING`, `crash_strikes =
/// 0` so the poison-pill reclaimer never trips, `worker_id`/`started_at`/
/// `last_heartbeat_at` nulled), plus clears the sticky affinity columns,
/// `wake_requested`, and any stale `activity_name` sentinel, and appends **no**
/// event — but is a distinct, named entry point so the panic-retry path is
/// self-documenting and separately testable.
///
/// Unlike the ND-block path this stamps **no** execution-row diagnostic columns
/// and needs **no** `FOR UPDATE` pause-guarded transaction: the panic re-pend
/// touches only the task row, and the claim-layer `PAUSED` gate defers a
/// re-pended task on a paused execution exactly like any pending workflow task.
///
/// The owning execution row (`harvest_workflow_executions`) is never touched, so
/// its state stays `RUNNING` throughout the panic-retry loop; the task is
/// deferred purely by `scheduled_at` (`claim_task` enforces `scheduled_at <=
/// NOW()`), so a signal/timer arriving mid-backoff is processed on the next
/// dispatch. No `pg_notify`: the row is deliberately not claimable until
/// `scheduled_at`.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::NotFound`] when the task is not a
/// claimed (`RUNNING`) workflow task, and
/// [`crate::error::HarvestError::Database`] on update failure.
pub async fn requeue_workflow_task_after_panic(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    delay: chrono::Duration,
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

/// Shared `SET` clause for the **clean-continuation** re-pend paths
/// ([`reschedule_task`] and [`defer_rate_limited_task`]).
///
/// "Clean continuation" means the claiming worker resolved a handler and either
/// suspended cleanly, hit a retryable error, or reached the dispatch-time
/// rate-limit gate — i.e. it made progress without crashing. Both consecutive-
/// failure streak counters therefore reset here:
///
/// * `crash_strikes` — the poison-pill quarantine threshold measures
///   *consecutive* worker crashes (issue #367).
/// * `capability_misses` — the capability-miss redelivery budget measures
///   *consecutive* claims by workers with no handler registered (issue #804).
///   Reaching this path PROVES capability, so the streak must reset or an
///   interleaving of incapable and capable claims would falsely escalate the
///   task with `no_capable_worker:`.
///
/// `treat_none_as_null = true` for the same reason as
/// [`PendingRequeueChangeset`]: a `None` field must bind SQL `NULL` rather than
/// be silently omitted from the `SET` clause.
#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::harvest_task_queue, treat_none_as_null = true)]
struct CleanContinuationChangeset {
    state: &'static str,
    worker_id: Option<String>,
    started_at: Option<chrono::DateTime<Utc>>,
    last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    crash_strikes: i32,
    capability_misses: i32,
    /// Issue #804 (round 6): cleared alongside `capability_misses` so the
    /// distinct-worker set and the total counter can never disagree about
    /// whether the streak was broken.
    capability_miss_workers: Vec<String>,
    scheduled_at: chrono::DateTime<Utc>,
}

impl CleanContinuationChangeset {
    const fn new(scheduled_at: chrono::DateTime<Utc>) -> Self {
        Self {
            state: "PENDING",
            worker_id: None,
            started_at: None,
            last_heartbeat_at: None,
            crash_strikes: 0,
            capability_misses: 0,
            capability_miss_workers: Vec::new(),
            scheduled_at,
        }
    }
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
    .set(CleanContinuationChangeset::new(scheduled_at))
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
        CleanContinuationChangeset::new(scheduled_at),
        // Undo the claim-time attempt increment: a rate-limit deferral is not an
        // execution, so it must not consume the retry budget.
        dsl::attempt.eq(diesel::dsl::sql::<diesel::sql_types::Integer>(
            "GREATEST(attempt - 1, 0)",
        )),
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

/// SQL for [`release_task_for_capability_miss`], exposed for no-DB shape tests
/// (issue #804).
///
/// `$1` = task id, `$2` = the releasing worker's id, `$3` = the backoff in
/// seconds.
///
/// The `phase` selects between three literal statements rather than binding
/// flags, mirroring [`park_workflow_task_query`]. Taking the phase itself —
/// rather than the two booleans it implies — keeps
/// [`crate::error::CapabilityMissPhase`] the single source of truth and makes
/// the one incoherent combination (clear the crash strikes *and* restore the
/// attempt) unrepresentable. The two clauses it governs:
///
/// - **`crash_strikes = 0`** only when the miss was detected *after* the
///   workflow body ran to a conclusion
///   ([`crate::error::CapabilityMissPhase::clears_crash_strikes`]): issue
///   #367's counter counts the worker processes a task has killed, so a
///   pre-/mid-handler miss ran nothing and is not evidence about it. Zeroing
///   unconditionally would let an incapable claim landing between two capable
///   crashes reset the streak every time, so `poison_pill_threshold` is never
///   reached in a heterogeneous fleet (Codex round-12 P1). No variant ever
///   *increments* it, which is what AC4 actually requires.
/// - **`attempt = GREATEST(attempt - 1, 0)`** only when the handler was never
///   reached ([`crate::error::CapabilityMissPhase::restores_dispatch_attempt`]):
///   `attempt` is both the retry budget and the "first dispatch?" signal
///   `record_workflow_started` reads, and once the handler has begun, the start
///   metric has already fired. See that predicate for the full argument.
///
/// One statement, guarded on `state = 'RUNNING' AND worker_id = $2` so it can
/// only ever undo *this* worker's own claim — a concurrent poison-pill reclaim
/// that already took the row simply matches 0 rows here (mirrors
/// [`crate::queue_pause::release_claim`]).
///
/// Two details that are load-bearing rather than incidental:
///
/// - **The backoff is computed from the DB clock** (`NOW() + make_interval`),
///   not the host clock. `claim_task` compares `scheduled_at` against Postgres
///   `NOW()`, and this module already tolerates up to
///   [`IMMEDIATE_SCHEDULE_SKEW_SECS`] of host/DB skew — which would swallow the
///   first three backoffs whole. Unlike an ordinary retry (where firing early
///   is harmless), the backoff is the only thing spacing redeliveries out
///   across a rolling deploy, so it must not be defeatable by clock skew.
/// - **`error` is left entirely untouched.** A capability miss is not a task
///   failure — the handler never ran — so the column reserved for real failure
///   reporting must not carry an infrastructure diagnostic. Two surfaces read
///   it and would both be misled: `ActivityContext::previous_failure()` (which
///   an author branches on) and, more sharply, the issue #773 `/stack`
///   endpoint, which renders any non-null `error` on a pending activity as a
///   `last_failure`. Because the release also restores `attempt` to `0` (an
///   activity-task miss is always [`crate::error::CapabilityMissPhase::BeforeHandler`],
///   so the round-17 phase gate never withholds the restoration here), that
///   would report a failure at an otherwise-unreachable `attempt: 0` for an
///   activity that never executed — violating #773 AC3 ("a never-failed
///   pending activity omits `last_failure`") and making a blameless deploy
///   skew look like an application bug on the one surface whose runbook
///   question is "why is this activity retrying?". The diagnostic still
///   reaches the operator through the release's `tracing::info!` and the
///   `harvest.task.capability_miss` counter.
#[must_use]
pub const fn release_task_for_capability_miss_query(
    phase: crate::error::CapabilityMissPhase,
) -> &'static str {
    // Both clauses derive from `phase`, so the arms below cover exactly the
    // three reachable combinations. `AfterHandler` is the only phase that
    // clears the crash strikes, and `BeforeHandler` the only one that restores
    // the attempt, so "clear AND restore" cannot be constructed.
    if phase.clears_crash_strikes() {
        // AfterHandler: the body reached a conclusion on this worker, so the
        // #367 crash streak is genuinely broken -- and the start metric has
        // already fired, so `attempt` stays put.
        "UPDATE harvest_task_queue \
         SET state = 'PENDING', \
             worker_id = NULL, \
             started_at = NULL, \
             last_heartbeat_at = NULL, \
             crash_strikes = 0, \
             capability_misses = CASE \
                 WHEN capability_miss_handler IS DISTINCT FROM $5 THEN 1 \
                 ELSE LEAST(capability_misses, 2147483646) + 1 \
             END, \
             capability_miss_workers = CASE \
                 WHEN capability_miss_handler IS DISTINCT FROM $5 THEN ARRAY[$2] \
                 WHEN $2 = ANY(capability_miss_workers) THEN capability_miss_workers \
                 ELSE array_append(capability_miss_workers, $2) \
             END, \
             capability_miss_handler = $5, \
             scheduled_at = NOW() + make_interval(secs => $3), \
             sticky_worker_id = NULL, \
             sticky_until = NULL, \
             sticky_timeout = NULL, \
             wake_requested = FALSE, \
             activity_name = CASE WHEN task_type = 'workflow' THEN NULL ELSE activity_name END \
         WHERE id = $1 \
           AND state = 'RUNNING' \
           AND worker_id = $2 \
           AND crash_strikes = $4 \
         RETURNING COALESCE(array_length(capability_miss_workers, 1), 0) \
             AS distinct_miss_workers"
    } else if phase.restores_dispatch_attempt() {
        // BeforeHandler: nothing ran at all. Restore the claim-time increment
        // so the miss neither drains an activity's retry budget nor blocks a
        // workflow's first-dispatch start metric.
        "UPDATE harvest_task_queue \
         SET state = 'PENDING', \
             worker_id = NULL, \
             started_at = NULL, \
             last_heartbeat_at = NULL, \
             attempt = GREATEST(attempt - 1, 0), \
             capability_misses = CASE \
                 WHEN capability_miss_handler IS DISTINCT FROM $5 THEN 1 \
                 ELSE LEAST(capability_misses, 2147483646) + 1 \
             END, \
             capability_miss_workers = CASE \
                 WHEN capability_miss_handler IS DISTINCT FROM $5 THEN ARRAY[$2] \
                 WHEN $2 = ANY(capability_miss_workers) THEN capability_miss_workers \
                 ELSE array_append(capability_miss_workers, $2) \
             END, \
             capability_miss_handler = $5, \
             scheduled_at = NOW() + make_interval(secs => $3), \
             sticky_worker_id = NULL, \
             sticky_until = NULL, \
             sticky_timeout = NULL, \
             wake_requested = FALSE, \
             activity_name = CASE WHEN task_type = 'workflow' THEN NULL ELSE activity_name END \
         WHERE id = $1 \
           AND state = 'RUNNING' \
           AND worker_id = $2 \
           AND crash_strikes = $4 \
         RETURNING COALESCE(array_length(capability_miss_workers, 1), 0) \
             AS distinct_miss_workers"
    } else {
        // DuringHandler: the body BEGAN (so the start metric fired and
        // `attempt` must stay put) but did not finish (so the #367 streak is
        // not evidence-broken). The one phase where the two clauses disagree.
        "UPDATE harvest_task_queue \
         SET state = 'PENDING', \
             worker_id = NULL, \
             started_at = NULL, \
             last_heartbeat_at = NULL, \
             capability_misses = CASE \
                 WHEN capability_miss_handler IS DISTINCT FROM $5 THEN 1 \
                 ELSE LEAST(capability_misses, 2147483646) + 1 \
             END, \
             capability_miss_workers = CASE \
                 WHEN capability_miss_handler IS DISTINCT FROM $5 THEN ARRAY[$2] \
                 WHEN $2 = ANY(capability_miss_workers) THEN capability_miss_workers \
                 ELSE array_append(capability_miss_workers, $2) \
             END, \
             capability_miss_handler = $5, \
             scheduled_at = NOW() + make_interval(secs => $3), \
             sticky_worker_id = NULL, \
             sticky_until = NULL, \
             sticky_timeout = NULL, \
             wake_requested = FALSE, \
             activity_name = CASE WHEN task_type = 'workflow' THEN NULL ELSE activity_name END \
         WHERE id = $1 \
           AND state = 'RUNNING' \
           AND worker_id = $2 \
           AND crash_strikes = $4 \
         RETURNING COALESCE(array_length(capability_miss_workers, 1), 0) \
             AS distinct_miss_workers"
    }
}

/// Release a claimed task back to `PENDING` because this worker has **no
/// handler registered** for its workflow/activity type, so a capable peer can
/// claim it (issue #804).
///
/// This is the always-on capability floor underneath build-id routing (#171):
/// SKIP LOCKED has no capability filter, so any worker polling a queue can
/// claim any task on it. Terminally failing the run because the *wrong* worker
/// picked it up turns a routine rolling deploy or a heterogeneous worker pool
/// into lost executions; releasing it costs the task one backoff interval.
///
/// Semantics, and why each differs from a plain reschedule:
///
/// - **`attempt` is restored only when the handler was never reached**
///   (`GREATEST(attempt - 1, 0)`, gated by
///   [`crate::error::CapabilityMissPhase::restores_dispatch_attempt`]) —
///   [`claim_task`] increments it on claim, and a pre-handler miss ran nothing,
///   so it must not drain the retry budget (the exact bug issue #369 fixed for
///   rate-limit deferrals). Once the handler has *begun*, `record_workflow_started`
///   has already fired for this execution, and restoring `attempt` would make
///   the next capable claim look like a first dispatch and emit that counter a
///   second time (Codex round-17 P2). Every activity-task miss is
///   pre-handler, so the budget is still always restored where it is a budget.
/// - **`crash_strikes` is never incremented, and is reset only when the handler
///   ran** (`clear_crash_strikes`) — the poison-pill threshold (#367) measures
///   *consecutive* crashes, so only a dispatch that watched the body reach a
///   conclusion without dying is evidence the streak is broken. A pre-/mid-handler
///   miss ran nothing and leaves the column alone: zeroing it would let an
///   incapable claim landing between two capable crashes launder the task's
///   history forever (Codex round-12 P1). Either way the release takes the row
///   out of `RUNNING` with `worker_id NULL`, so the orphan reclaimer — the only
///   writer that *increments* the column — cannot see it at all.
/// - **`capability_misses` is incremented** — the only counter a capability
///   miss advances. It bounds the bounce; the caller escalates at
///   `WorkerConfig::capability_miss_max_redeliveries`.
/// - **Sticky affinity is cleared** — the pinned worker is the one that just
///   proved it cannot run this task. Leaving the pin would make the release a
///   no-op (only that worker could re-claim it) and burn the redelivery budget
///   on a single incapable worker.
/// - **`activity_name` is cleared on workflow rows only** — a stale
///   `mixed_signal_suspension` sentinel would let an unrelated wake reset
///   `scheduled_at` and bypass the backoff (issue #603). On an activity row
///   `activity_name` is load-bearing and is preserved.
///
/// No `pg_notify`: the row is deliberately not claimable until `scheduled_at`,
/// so waking pollers early would be pure noise (mirrors
/// [`requeue_workflow_task_nd_blocked`]).
///
/// `phase` is the miss's [`crate::error::CapabilityMissPhase`], which governs
/// both conditional clauses — see [`release_task_for_capability_miss_query`]
/// for why a pre-/mid-handler miss must leave issue #367's counter alone and
/// why a post-handler miss must leave `attempt` alone.
///
/// Returns `Some(distinct_miss_workers)` when this worker's claim was actually
/// released, carrying the **post-update** cardinality of
/// `capability_miss_workers` as this statement committed it. `None` means the
/// row was already taken by something else (e.g. an orphan reclaim won the
/// race) and the caller must treat the task as no longer its own.
///
/// The count is returned by the `UPDATE` rather than derived from the caller's
/// earlier read because the two can genuinely disagree (Codex round-36 P2).
/// `observe_miss_and_fleet` snapshots the array before the release; a peer
/// restarting onto a capable build then runs
/// [`invalidate_capability_miss_evidence_for_worker`], which `array_remove`s
/// its own id from exactly these `RUNNING` rows. This statement appends the
/// claimant to the *post-invalidation* array, so a snapshot-derived number
/// over-reports: `{A,B}` + claimant `C` reads as 3 while the row actually holds
/// `{B,C}`. Reporting what the write produced makes the rollout diagnostic true
/// by construction instead of by a timing assumption.
///
/// `claim_crash_strikes` is the `crash_strikes` value the dispatcher observed
/// when it claimed the row, and is what makes this release apply to **that**
/// claim rather than merely to that *worker* (Codex round-37 P1).
/// [`crate::poison_pill::requeue_orphan`] hands an orphaned row back as
/// `PENDING` / `worker_id = NULL` with `crash_strikes + 1`, and nothing stops
/// the same worker from winning it again — so a `(state, worker_id)` guard also
/// matches that *new* claim. Releasing it would re-`PENDING` a row whose
/// replacement handler is already running, inviting a second concurrent claim
/// of the same task and rolling back an `attempt` that belongs to the new
/// dispatch. `crash_strikes` is the right discriminator because the requeue
/// that creates the race is what bumps it; the terminal escalation guard
/// ([`claim_still_held_for_update_query`]) already keys on it.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn release_task_for_capability_miss(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    worker_id: &str,
    backoff: StdDuration,
    phase: crate::error::CapabilityMissPhase,
    claim_crash_strikes: i32,
    frontier: &str,
) -> HarvestResult<Option<i32>> {
    // Bounded by `capability_miss_backoff`'s 30s cap; the clamp is defensive.
    let backoff_secs = f64::min(backoff.as_secs_f64(), 3600.0);
    let released = diesel::sql_query(release_task_for_capability_miss_query(phase))
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .bind::<diesel::sql_types::Double, _>(backoff_secs)
        .bind::<diesel::sql_types::Integer, _>(claim_crash_strikes)
        .bind::<diesel::sql_types::Text, _>(frontier)
        .get_results::<DistinctMissWorkersRow>(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(released
        .into_iter()
        .next()
        .map(|row| row.distinct_miss_workers))
}

/// The post-update `capability_miss_workers` cardinality returned by
/// [`release_task_for_capability_miss_query`].
#[derive(diesel::QueryableByName)]
struct DistinctMissWorkersRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    distinct_miss_workers: i32,
}

/// SQL for [`reset_capability_misses_after_inline_progress`]. Extracted as a
/// `const fn` so its shape is unit-testable without a database.
///
/// Guarded on the claim (`state = 'RUNNING' AND worker_id = $2`) so a row a
/// concurrent poison-pill reclaim or operator action already took out from
/// under this dispatch is never rewritten.
#[must_use]
pub const fn reset_capability_misses_after_inline_progress_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET capability_misses = 0, \
         capability_miss_workers = '{}' \
     WHERE id = $1 \
       AND state = 'RUNNING' \
       AND worker_id = $2"
}

/// Start the capability-miss budget clean because this dispatch made durable
/// inline progress (issue #804, Codex round-20 P1).
///
/// `capability_misses` / `capability_miss_workers` describe **one frontier** —
/// the position the workflow is currently stuck at, and therefore the single
/// handler a claiming worker must register to move it. The park-time reset
/// ([`park_workflow_task_sticky_query`]) already encodes that: a park is proof a
/// capable worker handled the row, so the next deploy starts with a full budget.
///
/// A workflow task can advance through several local-activity frontiers in ONE
/// dispatch, though: `process_workflow_task` runs them inline in a loop and only
/// parks on a *non*-local suspension. Each completion is appended durably, so
/// the frontier moves permanently — but nothing parked, so the counters kept
/// describing the frontier that is now behind us. Worker A missing local
/// activity X and worker B later missing local activity Y then read as "two
/// distinct workers missed this task", the registry confirms both are live, and
/// the run is failed terminally even though A may register Y. That conclusion is
/// the exact opposite of the truth, which is the failure mode issue #804 exists
/// to prevent.
///
/// Resetting here cannot be used to release forever: a *completed* local
/// activity resolves from history on the next replay
/// (`HistoryMatch::Matched`) and emits no command, so a given frontier can make
/// inline progress at most once, and a workflow has a bounded number of
/// local-activity call sites (itself bounded by the history hard cap). The
/// budget therefore becomes "per frontier" rather than "per task", which is what
/// it always meant.
///
/// This reset covers the frontier moves a *dispatch* makes. A third mover needs
/// no reset at all: newly INGESTED history (a due timer fire, a pending signal,
/// an external delta appended by `prepare_workflow_task_with_cache` before the
/// capability gate) can move the frontier with no park and no inline progress
/// in between. That one is handled by keying the evidence to the missed handler
/// (`capability_miss_handler`, Codex round-46 P1) rather than by resetting: the
/// counters simply do not apply to a handler they were not recorded against, so
/// a stale key needs no cleanup. It also makes THIS reset belt-and-braces for
/// the inline case — a row left keyed to a retired frontier reads as a fresh
/// budget either way — while the reset keeps its independent job of clearing
/// the absolute-ceiling count, which the key alone does not touch.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn reset_capability_misses_after_inline_progress(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    worker_id: &str,
) -> HarvestResult<bool> {
    let updated = diesel::sql_query(reset_capability_misses_after_inline_progress_query())
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(updated > 0)
}

/// SQL for [`invalidate_capability_miss_evidence_for_worker`]. Extracted as a
/// `const fn` so its shape is unit-testable without a database.
///
/// Deliberately touches **only** `capability_miss_workers`. `capability_misses`
/// is a true historical count of how many times this frontier was released, and
/// it backs the ungated absolute ceiling
/// (`CAPABILITY_MISS_TOTAL_BUDGET_MULTIPLIER`) that bounds the release loop even
/// when no fleet evidence is available. Decrementing it here would remove that
/// bound; removing one stale ID only ever weakens the *fleet* evidence, which
/// biases toward releasing rather than terminally failing.
/// **Actionable rows only.** `complete_task` does not clear
/// `capability_miss_workers`, so a queue with real history accumulates terminal
/// rows still naming past missers. Rewriting those is pure cost: a terminal task
/// is never redelivered and never escalated, so its array cannot influence any
/// decision. Worse, it is cost paid on the *startup* path — this runs in the
/// same transaction as `register_worker`, so the worker is not published to the
/// fleet until it finishes, and the `queue_name` indexes are partial to pending
/// rows (Codex round-35 P2). The `state` filter bounds the scan to the rows the
/// evidence can actually affect.
///
/// # The empty-array guard is load-bearing
///
/// `capability_miss_workers <> '{}'` reads as redundant next to the membership
/// test, and is not: it is the predicate that
/// `idx_harvest_tq_capability_miss_workers` is partial on, and Postgres's
/// implication prover cannot derive it from `$1 = ANY(...)`. Drop the conjunct
/// and the index stops matching, silently returning this statement to the full
/// backlog scan it exists to avoid — measured on a 200k-row backlog as
/// `Index Scan` / 9 buffers / 1.7 ms versus `Seq Scan` / 3848 buffers / 83 ms
/// (Codex round-39 P2). Pinned by
/// `dropping_the_empty_array_guard_returns_the_invalidation_to_a_full_scan`.
///
/// It is correctness-neutral in its own right: a row that has recorded no miss
/// cannot name `$1`, so the guard never changes which rows are updated.
#[must_use]
pub const fn invalidate_capability_miss_evidence_for_worker_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET capability_miss_workers = array_remove(capability_miss_workers, $1) \
     WHERE queue_name = ANY($2) \
       AND state IN ('PENDING', 'RUNNING') \
       AND capability_miss_workers <> '{}' \
       AND $1 = ANY(capability_miss_workers)"
}

/// Drop a worker ID's stale capability-miss evidence because that ID has just
/// (re-)registered (issue #804, Codex round-26 P1).
///
/// `capability_miss_workers` stores **bare worker IDs**, and
/// [`crate::workers::register_worker`] upserts on `worker_id` — refreshing
/// `started_at` and `build_id` — so a worker that restarts with the *same*
/// configured `worker_id` but a *new* build carrying the previously-missing
/// handler is indistinguishable, by ID alone, from the old incapable instance.
///
/// The harm is a false terminal failure at exactly the wrong moment. Once the
/// persisted release budget is spent, another incapable claimant reads the live
/// fleet, sees the restarted worker's ID already covered by the stale evidence,
/// derives [`crate::worker::FleetCapabilityEvidence::AllLiveWorkersMissed`], and
/// escalates — terminally failing the execution just as the capable build
/// arrives, which is the precise outcome issue #804 exists to prevent.
///
/// Clearing the ID's evidence on re-registration restores the truth: the
/// restarted instance has *not* been observed to miss this frontier, so the
/// fleet is no longer "all missed" and the task is released for it to claim.
///
/// # Trade-off
///
/// A worker crash-looping on the same *incapable* build also clears its own
/// evidence on each restart, so the `AllLiveWorkersMissed` total bound stops
/// applying to it. That case is still bounded — by the ungated absolute ceiling
/// on `capability_misses`, which this function never decrements — and the
/// degradation is deliberately in the safe direction: prefer holding a task over
/// terminally failing an execution a deploy could still rescue (AC1).
///
/// Scoped by `queue_name` so a worker only invalidates evidence on queues it
/// actually polls.
///
/// # Deliberately not ownership-guarded
///
/// Unlike every other writer of these columns, this statement has no
/// `state = 'RUNNING' AND worker_id = $n` clause: it must reach a row **another
/// worker is currently processing**, because that is exactly the row whose
/// decision the stale evidence would poison. A worker holding a claim taken
/// before this restart would otherwise still weigh the old instance as
/// incapable and terminally fail the run (issue #804, Codex round-27 P1).
///
/// That makes this the one writer that can change the columns between a claim
/// and its miss, which is why the decision path re-reads them through
/// [`read_capability_miss_state`] rather than trusting the claim-time snapshot.
/// The re-read is safe in one direction by construction: this statement only
/// ever *removes* entries, so a fresher read can only weaken the fleet evidence
/// and therefore only bias toward releasing.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure.
pub async fn invalidate_capability_miss_evidence_for_worker(
    conn: &mut AsyncPgConnection,
    worker_id: &str,
    queues: &[String],
) -> HarvestResult<usize> {
    if queues.is_empty() {
        return Ok(0);
    }
    let cleared = diesel::sql_query(invalidate_capability_miss_evidence_for_worker_query())
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(cleared)
}

/// SQL for [`read_capability_miss_state`]. Extracted as a `const fn` so its
/// shape is unit-testable without a database.
///
/// Ownership-guarded (`state = 'RUNNING' AND worker_id = $2`) for the same
/// reason the release statement is: a row a concurrent poison-pill reclaim or
/// operator action already took out from under this dispatch is not ours to
/// decide about, and returning its counters would let this worker escalate a
/// claim it no longer holds.
#[must_use]
pub const fn read_capability_miss_state_query() -> &'static str {
    // `$3` is the frontier this dispatch is missing (`{kind}:{name}`, from
    // `MissingHandler::frontier_key`). When it differs from the one the row
    // records, the stored counters are evidence about a frontier that is now
    // behind us, and charging their spend to a frontier no peer has ever been
    // offered is what terminally fails a run the fleet can still finish
    // (issue #804, Codex round-46 P1). Zeroing here rather than in the caller
    // keeps the comparison ATOMIC with the read: the row cannot change frontier
    // between deciding which counters apply and reading them.
    //
    // `IS DISTINCT FROM` (not `<>`) so a `NULL` handler — no frontier recorded
    // yet — reads as a mismatch and therefore as a full budget.
    "SELECT \
         CASE WHEN capability_miss_handler IS DISTINCT FROM $3 \
             THEN 0 ELSE capability_misses END AS capability_misses, \
         CASE WHEN capability_miss_handler IS DISTINCT FROM $3 \
             THEN '{}'::text[] ELSE capability_miss_workers END AS capability_miss_workers \
     FROM harvest_task_queue \
     WHERE id = $1 \
       AND state = 'RUNNING' \
       AND worker_id = $2"
}

/// SQL for [`claim_still_held_for_update`]. Extracted as a `const fn` so its
/// shape is unit-testable without a database.
#[must_use]
pub const fn claim_still_held_for_update_query() -> &'static str {
    "SELECT id \
     FROM harvest_task_queue \
     WHERE id = $1 \
       AND state = 'RUNNING' \
       AND worker_id = $2 \
       AND crash_strikes = $3 \
     FOR UPDATE SKIP LOCKED"
}

/// The task's capability-miss counters **as they stand now**, for the
/// release-vs-escalate decision (issue #804, Codex round-27 P1).
///
/// Returns `None` when the row is no longer claimed by `worker_id` — a
/// concurrent reclaim or operator action took it — which the caller treats as
/// "decide on what we last saw", since the release that follows is itself
/// ownership-guarded and will no-op.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the query fails.
pub async fn read_capability_miss_state(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    worker_id: &str,
    frontier: &str,
) -> HarvestResult<Option<(i32, Vec<String>)>> {
    #[derive(diesel::QueryableByName)]
    struct MissStateRow {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        capability_misses: i32,
        #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Text>)]
        capability_miss_workers: Vec<String>,
    }

    let rows: Vec<MissStateRow> = diesel::sql_query(read_capability_miss_state_query())
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .bind::<diesel::sql_types::Text, _>(frontier)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(rows
        .into_iter()
        .next()
        .map(|r| (r.capability_misses, r.capability_miss_workers)))
}

/// Whether `task_id` is still `RUNNING` under **this exact claim**, taking the
/// row's lock so the answer stays true until the caller's transaction commits
/// (issue #804, Codex round-31 P1).
///
/// # Why the claim, not just the worker
///
/// A poison-pill requeue (`poison_pill::requeue_orphan`) sets the row back to
/// `PENDING` with `worker_id = NULL` and a bumped `crash_strikes`, and dispatch
/// is concurrent — so the **same** worker can re-claim the row while its
/// escalation coroutine for the *previous* attempt is still in flight. A guard
/// on `(state, worker_id)` alone passes in that case and terminally fails the
/// NEW attempt's task on the OLD attempt's evidence. `crash_strikes` is the
/// discriminator `poison_pill::quarantine_orphan` itself uses for exactly this,
/// so the guard is a claim token rather than a worker token.
///
/// # Why `SKIP LOCKED` rather than a blocking wait
///
/// Deadlock avoidance, not throughput. This crate's `harvest_task_queue` lock
/// order is **execution row → task row**, but `poison_pill::quarantine_orphan`
/// takes the task row first and then reaches the execution row (through the
/// dead-letter FK and `fail_owning_workflow`). A blocking `FOR UPDATE` here
/// would let an escalating dispatcher hold the execution row while waiting for
/// the task row that a quarantine already holds while waiting for the execution
/// row — a genuine ABBA cycle, on precisely the pair of paths that race (a
/// worker whose heartbeat has gone stale escalating a task the reclaimer is
/// quarantining).
///
/// `SKIP LOCKED` removes the waiting edge entirely: this transaction never
/// blocks on the task row, so it can never be part of a lock cycle. A row
/// another transaction holds simply reads as "not ours right now", which the
/// caller treats exactly like a lost claim — it withdraws and releases, and the
/// escalation is re-decided on the next redelivery. Withdrawing is always the
/// safe direction.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the query fails.
pub async fn claim_still_held_for_update(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    worker_id: &str,
    crash_strikes: i32,
) -> HarvestResult<bool> {
    // Only the row's *existence* matters -- the id is bound, not read back.
    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[allow(dead_code)]
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let rows: Vec<IdRow> = diesel::sql_query(claim_still_held_for_update_query())
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .bind::<diesel::sql_types::Integer, _>(crash_strikes)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(!rows.is_empty())
}

/// SQL for [`release_suspended_workflow_claim`]. Extracted as a `const fn` so
/// its `WHERE` clause is unit-testable without a database.
///
/// Deliberately mirrors [`primary_repend_workflow_task_query`]'s column list
/// (refresh `created_at` for the schedule-to-start latency metric, preserve
/// rather than clear sticky affinity, clear the `activity_name` sentinel) --
/// this is "the task made no progress, try it again", not "this worker is
/// incapable" (contrast [`release_task_for_capability_miss_query`], which
/// deliberately clears sticky affinity because the worker it releases FROM
/// was just found unable to run the handler at all).
///
/// Resets `capability_misses = 0, capability_miss_workers = '{}'` (Codex
/// review round 2 of #1182), matching [`park_workflow_task_sticky_query`]'s
/// established `reset_capability_misses = true` precedent rather than
/// [`release_task_for_capability_miss_query`]'s (which always *increments*
/// it, since that release specifically records THIS worker's own
/// incapability). This function's caller reaches it only after
/// `handle_suspended_workflow`'s handler dispatch has already run -- the
/// same "reached only after the handler lookup succeeded" condition that
/// makes every park caller except [`crate::worker::requeue_parent_on_
/// transient_ingest_conflict`] a proof of capability -- so a release here is
/// unconditionally proof-of-capability too, and stale evidence from a prior,
/// genuinely incapable worker must not survive it: leaving it would let the
/// next dispatcher's capability-miss decision undercount the live,
/// unaccounted fleet by one, based on a peer this exact release just proved
/// wrong.
///
/// Also resets `crash_strikes = 0` (Codex review round 4 of #1182): the
/// `WHERE ... AND crash_strikes = $3` guard above still pins the update to
/// the exact claim-time snapshot (so a concurrent poison-pill reclaim that
/// bumped the strike count in the meantime is correctly treated as "the
/// claim moved" rather than silently overwritten), but the value the row is
/// set *to* must not carry that snapshot forward. This dispatch attempt ran
/// the handler to a conclusion on this worker -- it just couldn't confirm
/// ownership afterward, not that the process crashed -- so the issue #367
/// crash streak this row was accumulating is genuinely broken, exactly the
/// same "the body reached a conclusion on this worker" justification
/// [`release_task_for_capability_miss_query`]'s `AfterHandler` phase already
/// uses to reset the same column for the same reason. Leaving a stale count
/// in place would let an unrelated, already-resolved crash history count
/// against a task that just proved itself dispatchable.
const fn release_suspended_workflow_claim_query() -> &'static str {
    "UPDATE harvest_task_queue \
     SET state = 'PENDING', \
         worker_id = NULL, \
         started_at = NULL, \
         last_heartbeat_at = NULL, \
         scheduled_at = NOW(), \
         created_at = clock_timestamp(), \
         wake_requested = FALSE, \
         activity_name = NULL, \
         capability_misses = 0, \
         capability_miss_workers = '{}', \
         crash_strikes = 0, \
         sticky_until = CASE \
             WHEN sticky_worker_id IS NOT NULL AND sticky_timeout IS NOT NULL \
             THEN NOW() + sticky_timeout \
             ELSE sticky_until \
         END \
     WHERE id = $1 \
       AND state = 'RUNNING' \
       AND worker_id = $2 \
       AND crash_strikes = $3 \
     RETURNING id"
}

/// Release a still-claimed, but replay-suspended-empty-handed, workflow task
/// back to `PENDING` for a fresh dispatch attempt (issue #1182 follow-up,
/// Codex review).
///
/// [`claim_still_held_for_update`]'s `SKIP LOCKED` guard cannot distinguish a
/// genuine ownership transfer from a concurrent, *unrelated* lock holder
/// (e.g. [`wake_workflow_task`]'s `wake_requested` fallback UPDATE, which
/// touches this exact row for a moment while a sibling event wakes the same
/// execution) merely contending for the row at that instant -- both read back
/// as "not ours right now". Treating the second case identically to the first
/// -- doing nothing -- strands the row: it stays `RUNNING` under a worker who
/// has already moved on (this dispatch already returned, having made its
/// "no terminal decision" call), and nothing else ever re-claims a `RUNNING`
/// row, so it sits forever until an unrelated poison-pill/heartbeat sweep
/// eventually notices.
///
/// This is called only *after* the entire enclosing persistence transaction
/// -- the one that holds the execution row's `FOR UPDATE` lock for its whole
/// duration -- has rolled back, in response to
/// [`crate::error::HarvestError::SuspendedClaimAmbiguous`] propagating out of
/// [`crate::worker::fail_suspended_workflow_if_still_claimed`] with `?`
/// (Codex review round 2 of #1182). A nested `SAVEPOINT` release inside that
/// transaction does not release the execution row, so this release has to
/// wait for the *outermost* transaction to end, not merely
/// [`crate::worker::commit_terminal_failure_if_still_claimed`]'s own internal
/// one. Once it has, this call therefore never holds the execution row while
/// waiting for the task row: it is a single, standalone task-row lock
/// acquisition, so it cannot participate in the execution-row/task-row ABBA
/// cycle `SKIP LOCKED` exists to avoid there (see
/// [`claim_still_held_for_update`]'s doc comment) -- there is no *second*
/// lock for it to be the other half of.
///
/// Deliberately **not** `FOR UPDATE SKIP LOCKED`: a plain `UPDATE` blocks
/// behind whatever transiently holds the row instead of skipping it, then
/// re-evaluates its `WHERE` clause against the row's *post-commit* state. If
/// ownership genuinely moved in the interim, the guard (`worker_id` +
/// `crash_strikes`, the same claim token [`claim_still_held_for_update`]
/// checks) no longer matches and this updates nothing -- the new owner keeps
/// the row, exactly as if this call were never made. If it did not move, the
/// row is released, and `wake_requested` is cleared in the very same write so
/// a wake that landed in the contention window is reconciled rather than
/// silently lost. This mirrors the established, doubly-reviewed
/// [`release_task_for_capability_miss`] fallback -- the pattern this crate
/// already relies on whenever a `SKIP LOCKED` guard's ambiguous "not ours"
/// answer needs an authoritative, blocking follow-up -- but touches none of
/// that function's capability-miss-specific bookkeeping columns, since the
/// handler here DID run; it simply made no replay-significant progress.
///
/// Returns `true` when a row was actually released, `false` when the claim
/// had genuinely moved by the time this ran (a legitimate outcome, not an
/// error -- the new owner is responsible for the row).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] if the query fails.
pub async fn release_suspended_workflow_claim(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    worker_id: &str,
    crash_strikes: i32,
) -> HarvestResult<bool> {
    // Only the row's *existence* matters -- the id is bound, not read back.
    #[derive(diesel::QueryableByName)]
    struct IdRow {
        #[allow(dead_code)]
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
    }

    let rows: Vec<IdRow> = diesel::sql_query(release_suspended_workflow_claim_query())
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .bind::<diesel::sql_types::Text, _>(worker_id)
        .bind::<diesel::sql_types::Integer, _>(crash_strikes)
        .get_results(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(!rows.is_empty())
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
    park_workflow_task_inner(conn, task_id, sticky, true).await
}

/// [`park_workflow_task`] for the one caller whose park is **not** proof that
/// the parking worker can run this task (issue #804).
///
/// [`crate::worker::requeue_parent_on_transient_ingest_conflict`] (issue #779)
/// parks from inside the wake-event ingest, which runs *before* the handler
/// lookup in `process_workflow_task`. A worker with no handler for the type
/// reaches it too, so zeroing the capability-miss accounting there would
/// restart the redelivery budget on a path that proves nothing about
/// capability — weakening the escalation bound the release contract depends on.
///
/// The preservation is expressed as the *absence* of the two SET assignments in
/// the same statement that parks the row, so the row is never observably
/// zeroed. Restoring the values in a follow-up UPDATE would be wrong: the park
/// commits in autocommit mode, so a peer can claim the freshly-parked row and
/// record its own miss (or a capable peer can park and legitimately reset it)
/// in the gap, and no single-statement merge of two whole snapshots is monotone
/// against both of those.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on update failure, and
/// [`crate::error::HarvestError::NotFound`] when the task is not `RUNNING`.
pub async fn park_workflow_task_preserving_capability_misses(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    sticky: Option<StickyHint<'_>>,
) -> HarvestResult<bool> {
    park_workflow_task_inner(conn, task_id, sticky, false).await
}

async fn park_workflow_task_inner(
    conn: &mut AsyncPgConnection,
    task_id: Uuid,
    sticky: Option<StickyHint<'_>>,
    reset_capability_misses: bool,
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
    //
    // Chaos: kill/delay between the pre-park liveness check and the park's
    // atomic UPDATE — the #601 lost-wake window a concurrent wake can land in
    // (AC4). A kill here (owned conn in the reproducer's spawned task) rolls the
    // park back, leaving the task RUNNING with a dead worker.
    crate::chaos_point!(QUEUE_PARK_BEFORE_UPDATE);
    let rows: Vec<WakeRequestedRow> = if let Some(hint) = sticky {
        let timeout = hint.chrono_timeout()?;
        diesel::sql_query(park_workflow_task_sticky_query(reset_capability_misses))
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
        diesel::sql_query(park_workflow_task_query(reset_capability_misses))
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
///
/// Parking resets `capability_misses` (issue #804). A workflow task row is
/// long-lived — it is reused for the entire execution — and parking is the
/// dominant suspension path (activity, signal, child workflow, mutex; only a
/// timer suspension goes through [`reschedule_task`]). Nearly every caller is
/// reached only AFTER the handler lookup in `process_workflow_task` succeeded,
/// so a park is proof that a capable worker handled this row and the
/// consecutive-miss budget must start clean. Without this the counter would be
/// cumulative over the execution's whole life, so a long-lived entity workflow
/// would escalate during a later, unrelated deploy while a capable worker is
/// demonstrably live. `primary_repend_workflow_task_query` needs no equivalent
/// reset: the parked state it matches is produced only here.
///
/// **One caller is *not* proof of capability** and therefore passes
/// `reset_capability_misses = false`:
/// [`crate::worker::requeue_parent_on_transient_ingest_conflict`] (issue #779)
/// parks from inside the wake-event ingest, which runs *before* the handler
/// lookup. A worker with no handler for the type can reach it, so letting it
/// zero the counter would restart the consecutive-miss budget on a path that
/// proves nothing — weakening the escalation bound the release contract depends
/// on. Preserving is expressed as the *absence* of the two SET assignments in
/// this same statement rather than a follow-up restore UPDATE, because the park
/// commits in autocommit mode: a peer can claim the freshly-parked row and
/// record its own miss — or a capable peer can park and legitimately reset it —
/// before a second statement lands, and no single-statement merge of two whole
/// snapshots is monotone against both.
const fn park_workflow_task_sticky_query(reset_capability_misses: bool) -> &'static str {
    if reset_capability_misses {
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
                 capability_misses = 0, \
                 capability_miss_workers = '{}', \
                 wake_requested = FALSE \
             FROM candidate \
             WHERE t.id = candidate.id \
             RETURNING candidate.wake_requested AS had_wake_requested \
         ) \
         SELECT had_wake_requested FROM updated"
    } else {
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
}

/// SQL for [`park_workflow_task`] when no sticky hint is supplied. See
/// [`park_workflow_task_sticky_query`] for the `reset_capability_misses`
/// contract.
const fn park_workflow_task_query(reset_capability_misses: bool) -> &'static str {
    if reset_capability_misses {
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
                 capability_misses = 0, \
                 capability_miss_workers = '{}', \
                 wake_requested = FALSE \
             FROM candidate \
             WHERE t.id = candidate.id \
             RETURNING candidate.wake_requested AS had_wake_requested \
         ) \
         SELECT had_wake_requested FROM updated"
    } else {
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

// ---------------------------------------------------------------------------
// TTL'd rate-limit / start-throttle runtime overrides (issue #945)
// ---------------------------------------------------------------------------

/// Server-side cap on how long an operator-declared TTL'd rate-limit/throttle
/// override may live before it is force-expired, regardless of the caller's
/// requested `ttl_secs` (issue #945).
///
/// A ceiling, not a floor: a request with `ttl_secs` above this is rejected
/// `400` rather than silently clamped down, so an operator's intended expiry
/// is never silently shortened without their knowledge. 24 hours is long
/// enough to bridge a shift change but short enough that a forgotten override
/// cannot silently outlive an incident it was meant to mitigate.
pub const MAX_PACING_OVERRIDE_TTL_SECS: i64 = 24 * 60 * 60;

/// Renders the effective per-second token refill rate expression for a
/// `harvest_rate_limit_buckets` row, honoring a live TTL'd operator override
/// (issue #945).
///
/// Renders `COALESCE(CASE WHEN {alias}.override_expires_at > NOW() THEN
/// {alias}.override_refill_rate ELSE NULL END, {alias}.refill_rate)`.
/// `alias` qualifies every column reference, so this composes into any query
/// aliasing the bucket table (`"b"` in the claim-time gates) or the bare table
/// name itself (`"harvest_rate_limit_buckets"`, for the single-table
/// statements that reference no alias). An expired or absent override
/// (`override_expires_at` is `NULL` or not strictly in the future) falls
/// through to the declared baseline `refill_rate` -- a plain `> NOW()`
/// comparison evaluated fresh on every access, so reverting needs no
/// background sweeper and no operator action.
#[must_use]
pub fn effective_refill_rate_expr(alias: &str) -> String {
    format!(
        "COALESCE(CASE WHEN {alias}.override_expires_at > NOW() THEN \
         {alias}.override_refill_rate ELSE NULL END, {alias}.refill_rate)"
    )
}

/// The burst-capacity sibling of [`effective_refill_rate_expr`] (issue #945).
#[must_use]
pub fn effective_burst_expr(alias: &str) -> String {
    format!(
        "COALESCE(CASE WHEN {alias}.override_expires_at > NOW() THEN \
         {alias}.override_burst ELSE NULL END, {alias}.burst)"
    )
}

/// Renders the full available-tokens expression for a
/// `harvest_rate_limit_buckets` row (issue #945).
///
/// `LEAST(effective_burst, tokens + elapsed * effective_refill_rate)`,
/// composing [`effective_burst_expr`] and [`effective_refill_rate_expr`] so
/// every call site -- the claim-time gate, the dispatch-time debit, the
/// refund, the throttled-key check, and the operator gauge -- derives the
/// identical formula and can never drift.
#[must_use]
pub fn effective_available_tokens_expr(alias: &str) -> String {
    format!(
        "LEAST({burst}, {alias}.tokens + EXTRACT(EPOCH FROM (NOW() - {alias}.last_refilled_at)) * {rate})",
        burst = effective_burst_expr(alias),
        rate = effective_refill_rate_expr(alias),
    )
}

/// The resolved effective rate-limit parameters for one bucket at a point in
/// time (issue #945).
///
/// The *pure* (no-DB) counterpart to [`effective_available_tokens_expr`],
/// used by the override-management HTTP handlers to report `override_active`
/// and the effective values without a second query round-trip.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EffectiveRateLimit {
    /// The refill rate actually in effect: the override's, if active, else
    /// the declared baseline.
    pub refill_rate: f64,
    /// The burst actually in effect: the override's, if active, else the
    /// declared baseline.
    pub burst: f64,
    /// Whether a TTL'd override is currently in effect (`override_expires_at`
    /// is set and strictly after `now`). An override expiring at exactly
    /// `now` is NOT active -- matches the SQL `> NOW()` boundary rendered by
    /// [`effective_refill_rate_expr`]/[`effective_burst_expr`].
    pub override_active: bool,
}

/// Resolve the effective refill rate/burst for a bucket, mirroring the SQL
/// `COALESCE(CASE WHEN override_expires_at > NOW() THEN override_x ELSE NULL
/// END, x)` formula in pure Rust (issue #945).
///
/// Each field falls back to the declared baseline independently: an operator
/// may override only `refill_rate` (or only `burst`) and the other continues
/// to read from the baseline even while the override is active.
#[must_use]
pub fn resolve_effective_rate_limit(
    refill_rate: f64,
    burst: f64,
    override_refill_rate: Option<f64>,
    override_burst: Option<f64>,
    override_expires_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> EffectiveRateLimit {
    let override_active = override_expires_at.is_some_and(|expires| expires > now);
    EffectiveRateLimit {
        refill_rate: if override_active {
            override_refill_rate.unwrap_or(refill_rate)
        } else {
            refill_rate
        },
        burst: if override_active {
            override_burst.unwrap_or(burst)
        } else {
            burst
        },
        override_active,
    }
}

/// Check which activities in the specified queues have pending tasks currently
/// throttled by rate limits.
///
/// Returns the **bounded activity names** whose saturated (`< 1.0` token) buckets
/// are blocking claimable pending tasks — deliberately *not* the raw
/// `rate_limit_key`. **Cardinality contract (team convention, ADR-0001 §7):** the
/// caller ([`crate::worker::Worker::emit_throttle_metrics`]) feeds each returned
/// value into [`crate::telemetry::MetricsRecorder::record_rate_limit_throttled`],
/// which uses it as a metric label. Since a dynamic per-key bucket key
/// (`dyn-rate:{expr}:{tenant}`, issue #699) embeds unbounded tenant input, the
/// resolved key must never become a label; the activity name is bounded by the
/// registered activity set, so it is what we return and label by.
///
/// Honors a live TTL'd rate-limit override (issue #945) via
/// [`effective_available_tokens_expr`), so raising a limit's effective rate
/// stops it reporting "throttled" here even though the declared baseline row is
/// unchanged.
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
        activity_name: String,
    }

    let effective_tokens = effective_available_tokens_expr("b");
    let rows: Vec<Row> = diesel::sql_query(format!(
        "SELECT DISTINCT q.activity_name \
         FROM harvest_task_queue q \
         JOIN harvest_rate_limit_buckets b ON b.key = q.rate_limit_key \
         WHERE q.queue_name = ANY($1) \
           AND q.state = 'PENDING' \
           AND q.activity_name IS NOT NULL \
           AND q.scheduled_at <= NOW() \
           AND {effective_tokens} < 1.0"
    ))
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(queues)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows.into_iter().map(|r| r.activity_name).collect())
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
    //
    // Honors a live TTL'd override (issue #945) via
    // [`effective_available_tokens_expr`] -- the sole rate-limit enforcement
    // point for circuit-breaker activities, so an operator override must take
    // effect here with no worker restart.
    let effective_tokens = effective_available_tokens_expr("harvest_rate_limit_buckets");
    let outcome: Option<Outcome> = diesel::sql_query(format!(
        "WITH debited AS ( \
             UPDATE harvest_rate_limit_buckets \
             SET tokens = {effective_tokens} - 1.0, \
                 last_refilled_at = NOW() \
             WHERE key = $1 \
               AND {effective_tokens} >= 1.0 \
             RETURNING key \
        ) \
        SELECT EXISTS (SELECT 1 FROM debited) AS debited"
    ))
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
/// The `claimable_pending_demand_by_queue` query — extracted so the real call
/// site and the drift test share one source of truth.
///
/// Mirrors **every** claim-time gate in [`claim_task_query`], including the
/// issue #619 queue-pause and issue #807 activity-pause anti-joins. Bind: `$1`
/// circuit-breaker-tracked activities.
#[must_use]
pub fn claimable_pending_demand_query() -> String {
    // `sticky_owner` is NULL unless an unexpired lease binds the row to a
    // specific worker; the CASE mirrors claim_task's sticky predicate (a row
    // with sticky_until <= NOW() or NULL is freely claimable). It is both
    // selected and repeated in GROUP BY.
    let sticky_owner_expr = "CASE WHEN tq.sticky_worker_id IS NOT NULL \
                                  AND tq.sticky_until IS NOT NULL \
                                  AND tq.sticky_until > NOW() \
                             THEN tq.sticky_worker_id ELSE NULL END";

    // Issue #619: a paused queue's backlog is deliberately held, not stalled,
    // so it must not surface as unmet demand (which would drive a false
    // worker-capacity alert during the hold). Generated from the shared
    // renderer, correlated against this query's `tq` alias.
    let queue_pause_gate = crate::queue_pause::queue_pause_anti_join("tq");
    // Issue #807: identically, a paused activity type's backlog is held rather
    // than stalled. The rendered anti-join carries `task_type = 'activity'`, and
    // that predicate is REQUIRED, not redundant: a workflow row's
    // `activity_name` is not reliably NULL -- the engine stamps the
    // `'mixed_signal_suspension'` sentinel into that very column -- so
    // NULL-safety alone would leave this query silently under-reporting demand
    // whenever an activity happened to share that sentinel's name. See
    // `activity_pause::activity_pause_anti_join`, and the shape assertion in
    // `claim_gate_mirrors_carry_the_activity_pause_predicate` below.
    let activity_pause_gate = crate::activity_pause::activity_pause_anti_join("tq");
    // Issue #945: honors a live TTL'd override so this coverage/demand read
    // never disagrees with what `claim_task` (below) will actually admit.
    let effective_tokens_b = effective_available_tokens_expr("b");
    format!(
        "SELECT tq.queue_name, tq.required_capabilities, tq.required_build_id, \
                {sticky_owner_expr} AS sticky_owner, tq.activity_name, \
                COUNT(*)::BIGINT AS cnt \
         FROM harvest_task_queue tq \
         LEFT JOIN harvest_workflow_executions e ON e.id = tq.workflow_exec_id \
         WHERE tq.state = 'PENDING' \
           AND tq.scheduled_at <= NOW() \
           AND {queue_pause_gate} \
           AND {activity_pause_gate} \
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
                     AND {effective_tokens_b} >= 1.0 \
               ) \
           ) \
         GROUP BY tq.queue_name, tq.required_capabilities, tq.required_build_id, \
                  {sticky_owner_expr}, tq.activity_name"
    )
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

    let rows: Vec<DemandRow> = diesel::sql_query(claimable_pending_demand_query())
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
    // Honors a live TTL'd override (issue #945): the refund caps at the
    // *effective* burst (and re-applies pending refill at the *effective*
    // rate) so a refund can never push tokens above what the override
    // currently allows.
    let effective_burst = effective_burst_expr("harvest_rate_limit_buckets");
    let effective_refill_rate = effective_refill_rate_expr("harvest_rate_limit_buckets");
    diesel::sql_query(format!(
        "UPDATE harvest_rate_limit_buckets \
         SET tokens = LEAST({effective_burst}, tokens + EXTRACT(EPOCH FROM (NOW() - last_refilled_at)) * {effective_refill_rate} + 1.0), \
             last_refilled_at = NOW() \
         WHERE key = $1"
    ))
    .bind::<diesel::sql_types::Text, _>(key)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Ensure a token bucket exists for `key`, preserving any operator override.
///
/// `INSERT … ON CONFLICT (key) DO NOTHING` with the initial `tokens = burst`, so
/// a rate change across a deploy never silently resets a live bucket. Shared by
/// the static activity-limiter startup registration and the dynamic per-key
/// enqueue path (issue #699) so the fail-closed `EXISTS` gate in [`claim_task`]
/// (and the dispatch-time [`try_consume_rate_limit_token`]) always has a bucket
/// row to read, matching [`crate::throttle`]'s bucket-ensure exactly.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn ensure_rate_limit_bucket(
    conn: &mut AsyncPgConnection,
    key: &str,
    refill_rate: f64,
    burst: f64,
) -> HarvestResult<()> {
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, $2, $3, $3, NOW()) \
         ON CONFLICT (key) DO NOTHING",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Double, _>(refill_rate)
    .bind::<diesel::sql_types::Double, _>(burst)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// `WHERE` clause that excludes *unbounded* rate-limit key families from the
/// per-key gauge sampler (issue #699 review, #1).
///
/// **Cardinality rule (ADR-0001 §7):** the per-key token/refill GAUGES are
/// emitted with the bucket `key` as a metric LABEL. A caller-controlled /
/// per-execution-resolved key family — dynamic per-key limits
/// (`dyn-rate:{expr}:{tenant}`, #699) and start throttles
/// (`start-throttle:{workflow}:{tenant}`, #607) — embeds unbounded tenant input
/// and buckets are never GC'd, so labelling by it would create one time-series
/// per tenant forever. Those families are excluded here; their per-tenant bucket
/// state is observable via `GET /admin/rate-limits`, not metrics. Bounded static
/// keys (bare activity names / author strings) keep their per-key gauges.
pub const RATE_LIMIT_GAUGE_SAMPLER_FILTER: &str =
    "WHERE key NOT LIKE 'dyn-rate:%' AND key NOT LIKE 'start-throttle:%'";

/// One sampled *bounded-key* rate-limit bucket, for the per-key gauge sampler.
#[derive(Debug, Clone, diesel::QueryableByName)]
pub struct RateLimitBucketSample {
    /// The bounded bucket key (safe to use as a metric label).
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub key: String,
    /// The bucket's refill rate (tokens/sec).
    #[diesel(sql_type = diesel::sql_types::Double)]
    pub refill_rate: f64,
    /// Estimated currently-available tokens (`LEAST(burst, tokens + elapsed*rate)`).
    #[diesel(sql_type = diesel::sql_types::Double)]
    pub estimated_tokens: f64,
}

/// Sample all *bounded-key* rate-limit buckets on one shard for the per-key gauge
/// sampler (issue #699 review, #1).
///
/// Deliberately excludes the unbounded per-tenant key families
/// ([`RATE_LIMIT_GAUGE_SAMPLER_FILTER`]) so a resolved per-tenant key can never
/// become an unbounded metric label. The caller
/// ([`crate::worker`]'s rate-limit sampler) aggregates across shards and forwards
/// each row's `key` to the token/refill gauges.
///
/// Projects the *effective* refill rate/tokens (issue #945) -- a bucket under a
/// live TTL'd override reports the override's values here, not the declared
/// baseline, so this gauge reflects what dispatch is actually enforcing.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn sample_rate_limit_buckets(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Vec<RateLimitBucketSample>> {
    let effective_refill_rate = effective_refill_rate_expr("harvest_rate_limit_buckets");
    let effective_tokens = effective_available_tokens_expr("harvest_rate_limit_buckets");
    diesel::sql_query(format!(
        "SELECT \
             key, \
             {effective_refill_rate} AS refill_rate, \
             {effective_tokens} AS estimated_tokens \
         FROM harvest_rate_limit_buckets {RATE_LIMIT_GAUGE_SAMPLER_FILTER}"
    ))
    .load(conn)
    .await
    .map_err(crate::error::database_error)
}

/// Bound on representative task/execution ids returned per queue.
///
/// Used by [`pending_queue_demand_by_queue_name`] (issue #774 AC3), mirroring
/// [`crate::execution::REACHABILITY_SAMPLE_CAP`]'s pattern with a *separate*
/// constant deliberately not shared across the two unrelated domains — a
/// future change to the workflow-reachability cap must not silently also
/// change the queue-coverage cap.
pub const QUEUE_COVERAGE_SAMPLE_CAP: usize = 5;

/// One queue's fleet-wide `PENDING` demand on a single shard: how many tasks
/// are queued, plus bounded, oldest-first representative ids for drill-down
/// (issue #774).
///
/// `sample_task_ids` chains into the per-task eligibility explainer
/// (`GET /admin/tasks/{id}/eligibility`, issues #380/#611);
/// `sample_execution_ids` chains directly into `GET /workflows/{id}`. Both are
/// **representative and bounded** — not a guaranteed global-oldest set — per
/// the same shard-then-cross-shard capping convention documented on
/// [`crate::execution::WorkflowTypeNonTerminalCount`].
#[derive(Debug, Clone)]
pub struct PendingQueueDemand {
    pub queue_name: String,
    pub pending_count: i64,
    pub sample_task_ids: Vec<Uuid>,
    pub sample_execution_ids: Vec<Uuid>,
}

/// The `pending_queue_demand_by_queue_name` query — extracted so the real
/// call site and the sample-cap drift guard test share one source of truth.
///
/// Deliberately **not** [`claimable_pending_demand_query`]: this is the
/// literal, unfiltered "does anything have PENDING work on this queue" signal
/// issue #774 asks for (queue coverage), not the claim-eligible-right-now
/// signal `claimable_pending_demand_by_queue` computes for worker-capacity
/// concerns (issue #522/#531/#742, explicitly out of scope here). Filtering
/// by concurrency cap / rate-limit / `schedule_to_close` expiry would hide a
/// genuinely-uncovered queue behind an unrelated, separately-alerted
/// condition.
///
/// Samples are drawn via a `LEFT JOIN LATERAL ... LIMIT N`, **not** a full
/// `ARRAY_AGG(...)[1:N]` slice (issue #774 review): the `[1:N]` form only
/// bounds the *returned* array — Postgres must still materialize a
/// transition array covering every `PENDING` row in the queue before
/// slicing it, so a heavily stranded queue (exactly the failure mode this
/// operational gate exists to surface) would make the query's memory use
/// scale with the full backlog. A `LIMIT`-bounded lateral subquery lets the
/// planner use a bounded top-N heap sort instead, so per-queue sampling
/// work stays proportional to `QUEUE_COVERAGE_SAMPLE_CAP`, not to backlog
/// size. Each lateral subquery mirrors the original ordering/filtering
/// exactly: `sample_task_ids` takes the first `QUEUE_COVERAGE_SAMPLE_CAP`
/// pending rows by `(scheduled_at, id)`; `sample_execution_ids` takes the
/// first `QUEUE_COVERAGE_SAMPLE_CAP` *non-null* `workflow_exec_id`s in that
/// same order (a task row can have a `NULL` `workflow_exec_id`, so this is
/// filtered independently rather than derived from the task sample).
#[must_use]
pub const fn pending_queue_demand_query() -> &'static str {
    "SELECT q.queue_name, \
            q.pending_count, \
            COALESCE(t.sample_task_ids, ARRAY[]::UUID[]) AS sample_task_ids, \
            COALESCE(e.sample_execution_ids, ARRAY[]::UUID[]) AS sample_execution_ids \
     FROM ( \
         SELECT queue_name::TEXT AS queue_name, COUNT(*)::BIGINT AS pending_count \
         FROM harvest_task_queue \
         WHERE state = 'PENDING' \
           AND ($1::TEXT IS NULL OR queue_name = $1::TEXT) \
         GROUP BY queue_name \
     ) q \
     LEFT JOIN LATERAL ( \
         SELECT ARRAY_AGG(id ORDER BY scheduled_at ASC, id ASC) AS sample_task_ids \
         FROM ( \
             SELECT id, scheduled_at \
             FROM harvest_task_queue \
             WHERE state = 'PENDING' AND queue_name = q.queue_name \
             ORDER BY scheduled_at ASC, id ASC \
             LIMIT 5 \
         ) top_tasks \
     ) t ON TRUE \
     LEFT JOIN LATERAL ( \
         SELECT ARRAY_AGG(workflow_exec_id ORDER BY scheduled_at ASC, id ASC) AS sample_execution_ids \
         FROM ( \
             SELECT workflow_exec_id, scheduled_at, id \
             FROM harvest_task_queue \
             WHERE state = 'PENDING' \
               AND queue_name = q.queue_name \
               AND workflow_exec_id IS NOT NULL \
             ORDER BY scheduled_at ASC, id ASC \
             LIMIT 5 \
         ) top_execs \
     ) e ON TRUE \
     ORDER BY q.queue_name"
}

/// Fleet-visibility read for issue #774: every queue with `PENDING` work on
/// this shard, with bounded representative sample ids.
///
/// This is deliberately the *simple*, unfiltered PENDING count — see
/// [`pending_queue_demand_query`]'s doc comment for why it does not reuse
/// [`claimable_pending_demand_by_queue`]'s constraint-aware filtering.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
pub async fn pending_queue_demand_by_queue_name(
    conn: &mut AsyncPgConnection,
    queue_name_filter: Option<&str>,
) -> HarvestResult<Vec<PendingQueueDemand>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        pending_count: i64,
        #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Uuid>)]
        sample_task_ids: Vec<Uuid>,
        #[diesel(sql_type = diesel::sql_types::Array<diesel::sql_types::Uuid>)]
        sample_execution_ids: Vec<Uuid>,
    }

    let rows: Vec<Row> = diesel::sql_query(pending_queue_demand_query())
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(queue_name_filter)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| PendingQueueDemand {
            queue_name: r.queue_name,
            pending_count: r.pending_count,
            sample_task_ids: r.sample_task_ids,
            sample_execution_ids: r.sample_execution_ids,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CapabilityMissPhase;

    // ── Issue #811: latest-wins strategy on the concurrency admin read ──────

    /// `GET /admin/concurrency` is a `free_form` contract route, so the
    /// `contract_regression` cross-check cannot catch a drift in this shape.
    /// Pin it here: a sampler-shaped row (no registry, so no attribution) must
    /// serialize byte-identically to the pre-#811 wire form, and an
    /// admin-shaped row must carry the strategy an operator reads.
    #[test]
    fn concurrency_key_stats_serializes_workflows_only_when_populated() {
        let sampler_shaped = ConcurrencyKeyStats {
            key: "tenant-42".to_string(),
            task_type: "workflow".to_string(),
            max_concurrent: 1,
            in_flight: 1,
            pending: 3,
            workflows: Vec::new(),
        };
        let json = serde_json::to_value(&sampler_shaped).expect("serialize");
        assert_eq!(
            json,
            serde_json::json!({
                "key": "tenant-42",
                "task_type": "workflow",
                "max_concurrent": 1,
                "in_flight": 1,
                "pending": 3,
            }),
            "an empty attribution list must be omitted entirely, so the \
             worker-sampler-shaped value is byte-identical to pre-#811"
        );

        let admin_shaped = ConcurrencyKeyStats {
            workflows: vec![
                ConcurrencyWorkflowStrategy {
                    workflow_name: "doc_index".to_string(),
                    on_conflict: crate::concurrency::ConcurrencyOnConflict::CancelRunning,
                },
                ConcurrencyWorkflowStrategy {
                    workflow_name: "doc_report".to_string(),
                    on_conflict: crate::concurrency::ConcurrencyOnConflict::Defer,
                },
            ],
            ..sampler_shaped
        };
        let json = serde_json::to_value(&admin_shaped).expect("serialize");
        assert_eq!(
            json["workflows"],
            serde_json::json!([
                {"workflow_name": "doc_index", "on_conflict": "cancel_running"},
                {"workflow_name": "doc_report", "on_conflict": "defer"},
            ]),
            "the admin read must surface each workflow's effective strategy \
             using the same snake_case wire values the macro attribute accepts"
        );

        // Round-trips, so a client deserializing the admin shape (and an older
        // client deserializing the sampler shape) both work.
        let back: ConcurrencyKeyStats =
            serde_json::from_value(json).expect("admin shape must round-trip");
        assert_eq!(back.workflows.len(), 2);
        let legacy: ConcurrencyKeyStats = serde_json::from_value(serde_json::json!({
            "key": "k",
            "task_type": "activity",
            "max_concurrent": 2,
            "in_flight": 0,
            "pending": 0,
        }))
        .expect("a pre-#811 payload must still deserialize");
        assert!(legacy.workflows.is_empty());
    }

    /// The attribution query must select exactly the same live rows the stats
    /// query counts, or a key could report a workflow it has no live task for
    /// (or omit the one it does). Pin the shared predicate textually — the two
    /// are separate `sql_query` strings that can silently drift.
    #[test]
    fn concurrency_attribution_query_matches_the_stats_row_filter() {
        // The two are separate `sql_query` string literals, so they are free to
        // drift. Assert the shared predicate fragments are present in BOTH --
        // checking only the attribution side would pass even if the STATS side
        // changed, which is exactly the drift this guard exists to catch.
        //
        // The attribution query aliases `harvest_task_queue` as `t` (it joins
        // the executions table), so strip that alias before comparing; the
        // predicates are otherwise character-identical. Fragment-level rather
        // than whole-statement so reformatting is not brittle.
        let stats = CONCURRENCY_STATS_SQL.replace("t.", "");
        let attribution = CONCURRENCY_ATTRIBUTION_SQL.replace("t.", "");
        for fragment in [
            "concurrency_key IS NOT NULL",
            "concurrency_cap IS NOT NULL",
            "queue_name = ANY($1)",
            "(state = 'PENDING' OR (state = 'RUNNING' AND worker_id IS NOT NULL))",
        ] {
            assert!(
                stats.contains(fragment),
                "stats query must contain the shared row-filter fragment `{fragment}`"
            );
            assert!(
                attribution.contains(fragment),
                "attribution query must share the stats row filter fragment `{fragment}`"
            );
        }

        // Both must group by the same key dimensions, or a key could report a
        // workflow it has no live task for (or omit the one it does). The
        // attribution query adds `workflow_name` (that is the whole point), so
        // assert the shared prefix rather than equality.
        assert!(stats.contains("GROUP BY concurrency_key, task_type"));
        assert!(
            attribution.contains("GROUP BY concurrency_key, task_type, e.workflow_name"),
            "attribution groups by the same (key, task_type) dimensions, plus workflow_name"
        );
    }

    // ── Issue #774: queue coverage — pending-demand query ───────────────────

    /// The hot query embeds two `LEFT JOIN LATERAL ... LIMIT N` sample
    /// subqueries (task ids, execution ids); both must stay bound to
    /// `QUEUE_COVERAGE_SAMPLE_CAP` since Diesel `sql_query` cannot
    /// interpolate the const directly.
    #[test]
    fn pending_queue_demand_sql_sample_limits_match_the_cap() {
        let sql = pending_queue_demand_query();
        let needle = format!("LIMIT {QUEUE_COVERAGE_SAMPLE_CAP}");
        assert_eq!(
            sql.matches(&needle).count(),
            2,
            "pending_queue_demand_query() must bound both sample subqueries \
             to exactly QUEUE_COVERAGE_SAMPLE_CAP ({QUEUE_COVERAGE_SAMPLE_CAP}) \
             rows via LIMIT; found {} occurrences of '{needle}' in:\n{sql}",
            sql.matches(&needle).count()
        );
    }

    /// Regression guard for the review-flagged unbounded
    /// `ARRAY_AGG(...)[1:N]` pattern (issue #774 review): a `[1:N]` slice
    /// only bounds the *returned* array, not the transition-array memory
    /// Postgres must first build over every `PENDING` row in the queue --
    /// exactly the failure mode (a queue stranded with thousands of
    /// unclaimed tasks) this operational gate exists to surface. Each
    /// sample must come from its own `LIMIT`-bounded lateral subquery
    /// instead, so a `LEFT JOIN LATERAL` (not a bare `ARRAY_AGG` slice) is
    /// what actually appears in the query.
    #[test]
    fn pending_queue_demand_sql_bounds_samples_via_lateral_limit_not_full_array_agg() {
        let sql = pending_queue_demand_query();
        assert!(
            !sql.contains("[1:"),
            "must not slice a full ARRAY_AGG -- use a LIMIT-bounded lateral \
             subquery instead:\n{sql}"
        );
        assert!(
            sql.to_uppercase().contains("LATERAL"),
            "expected LEFT JOIN LATERAL sample subqueries in:\n{sql}"
        );
    }

    /// The query must filter to `state = 'PENDING'` only — no `claim_task`-style
    /// constraint gating (concurrency cap, rate limit, `schedule_to_close`,
    /// PAUSED-workflow exclusion). That is deliberate scope: #774 answers "is
    /// anyone polling this queue at all", not "is this specific row claimable
    /// right now" (owned by #531/#742/#171).
    #[test]
    fn pending_queue_demand_sql_has_no_claim_eligibility_filtering() {
        let sql = pending_queue_demand_query();
        assert!(sql.contains("state = 'PENDING'"));
        assert!(!sql.contains("schedule_to_close_at"));
        assert!(!sql.contains("concurrency_cap"));
        assert!(!sql.contains("rate_limit"));
        assert!(!sql.contains("PAUSED"));
    }

    // ── Queue pause: claim gate + its mirrors (issue #619) ──────────────────

    /// The hot claim path pre-filters `harvest_queue_pauses` into one small
    /// array (`paused_queues`, evaluated once via `MATERIALIZED`) instead of
    /// embedding the shared `queue_pause_anti_join` literal, which the
    /// planner was re-probing once per *candidate row* rather than once per
    /// claim — see the `NOTE` above `claim_task_query` and
    /// `docs/performance.md` for the measured buffer cost this replaced.
    ///
    /// This is therefore a deliberate, evidence-backed exception to "every
    /// mirror embeds the shared predicate verbatim" — pinned to the new shape
    /// (not the old literal) so a future refactor can't silently regress back
    /// to the per-row form without this test catching the shape change, and
    /// so a reviewer sees exactly what invariant replaces the drift-lock.
    #[test]
    fn claim_query_excludes_every_paused_queue_via_a_prefiltered_array() {
        let sql = claim_task_query();
        assert!(
            sql.contains("paused_queues AS MATERIALIZED"),
            "the pause pre-filter must be forced to materialize once, not be \
             inlined back into a per-row correlated subquery; got:\n{sql}"
        );
        assert!(
            sql.contains(
                "FROM harvest_queue_pauses \
             WHERE queue_name = ANY($2)"
            ),
            "paused_queues must pre-filter to the worker's own polled queues \
             (bounded by $2), not scan the whole pause table; got:\n{sql}"
        );
        assert!(
            sql.contains("NOT (harvest_task_queue.queue_name = ANY(paused_queues.names))"),
            "the per-row check must be a plain array-membership test against \
             the pre-filtered array, not a correlated NOT EXISTS; got:\n{sql}"
        );
        assert!(
            !sql.contains("NOT EXISTS (SELECT 1 FROM harvest_queue_pauses"),
            "the per-row correlated anti-join must be gone entirely; got:\n{sql}"
        );
    }

    /// Both queries that document "mirrors every claim-time gate" must mirror
    /// the pause gate too — otherwise a deliberately-held backlog surfaces as
    /// unmet demand / a stale oldest-pending age and drives a false alert.
    #[test]
    fn claim_gate_mirrors_carry_the_queue_pause_predicate() {
        assert!(
            oldest_pending_ages_query().contains(&crate::queue_pause::queue_pause_anti_join(
                "harvest_task_queue"
            )),
            "oldest_pending_ages must mirror the queue-pause gate"
        );
        assert!(
            claimable_pending_demand_query()
                .contains(&crate::queue_pause::queue_pause_anti_join("tq")),
            "claimable_pending_demand_by_queue must mirror the queue-pause gate"
        );
    }

    /// A pause holds *dispatch*: the gate lives in the PENDING-selection
    /// predicate, never in the terminal/RUNNING paths, so in-flight work is
    /// untouched (AC2).
    #[test]
    fn queue_pause_gate_appears_once_in_the_claim_candidate_scan() {
        assert_eq!(
            claim_task_query().matches("harvest_queue_pauses").count(),
            1,
            "the pause gate belongs only in the candidate (PENDING) scan"
        );
    }

    // ── Activity pause: claim gate + its mirrors (issue #807) ───────────────

    /// The per-activity pause gate must use the same pre-filtered-array shape
    /// the queue gate was rewritten to, never a per-row correlated subquery.
    ///
    /// The `NOTE` above [`claim_task_query`] records that the correlated form
    /// accounted for ~99% of this query's buffer touches at a 10k-row backlog,
    /// because the planner re-probes the pause table once per *candidate row*
    /// walked while sorting toward `LIMIT 1`. An activity pause is enforced on
    /// the same scan, so it inherits the same cost model — and the same
    /// prohibition.
    #[test]
    fn claim_query_excludes_paused_activities_via_a_prefiltered_array() {
        let sql = claim_task_query();
        assert!(
            sql.contains("paused_activities AS MATERIALIZED"),
            "the activity-pause pre-filter must be forced to materialize once, \
             not be inlined back into a per-row correlated subquery; got:\n{sql}"
        );
        assert!(
            sql.contains("FROM harvest_activity_pauses"),
            "paused_activities must read the durable pause table; got:\n{sql}"
        );
        assert!(
            !sql.contains("NOT EXISTS (SELECT 1 FROM harvest_activity_pauses"),
            "the per-row correlated anti-join must never appear on the hot \
             claim path; got:\n{sql}"
        );
    }

    /// **A paused activity must never hold a workflow task.**
    ///
    /// `harvest_task_queue.activity_name` is `Nullable<Text>`: workflow tasks
    /// carry `NULL`. In SQL, `NULL = ANY(array)` is `NULL`, and `NOT NULL` is
    /// `NULL` — which is not `TRUE`, so a bare
    /// `NOT (activity_name = ANY(paused))` silently filters out **every
    /// workflow task in the fleet** the instant any one activity is paused.
    /// That is a total orchestration outage produced by a surgical containment
    /// control — the exact inverse of this feature's purpose.
    ///
    /// The `$6` capability-miss gate already defends against this with a
    /// `task_type != 'activity' OR activity_name IS NULL` prefix; this test
    /// pins the same defence onto the pause gate so it can never be dropped.
    #[test]
    fn claim_query_activity_pause_gate_never_holds_workflow_tasks() {
        let sql = claim_task_query();
        let gate = "task_type != 'activity' \
                   OR activity_name IS NULL \
                   OR NOT (activity_name = ANY(paused_activities.names))";
        let normalized: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let expected: String = gate.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            normalized.contains(&expected),
            "the activity-pause gate must short-circuit on non-activity rows \
             and on a NULL activity_name before testing array membership, or a \
             single paused activity halts every workflow task in the fleet. \
             expected to find:\n  {expected}\nin:\n{sql}"
        );
    }

    /// A pause holds *dispatch*: the gate lives in the PENDING-selection
    /// predicate, never in the terminal/RUNNING paths, so in-flight work is
    /// untouched (issue #807 AC4). Mirrors
    /// `queue_pause_gate_appears_once_in_the_claim_candidate_scan`.
    #[test]
    fn activity_pause_gate_appears_once_in_the_claim_candidate_scan() {
        assert_eq!(
            claim_task_query()
                .matches("harvest_activity_pauses")
                .count(),
            1,
            "the activity-pause gate belongs only in the candidate (PENDING) \
             scan, so an already-RUNNING instance runs to completion"
        );
    }

    /// Both queries that document "mirrors every claim-time gate" must mirror
    /// the activity-pause gate too, or a deliberately-held backlog surfaces as
    /// unmet demand / a stale oldest-pending age and drives a false capacity
    /// alert during exactly the incident the hold exists for.
    #[test]
    fn claim_gate_mirrors_carry_the_activity_pause_predicate() {
        assert!(
            oldest_pending_ages_query().contains(&crate::activity_pause::activity_pause_anti_join(
                "harvest_task_queue"
            )),
            "oldest_pending_ages must mirror the activity-pause gate"
        );
        assert!(
            claimable_pending_demand_query()
                .contains(&crate::activity_pause::activity_pause_anti_join("tq")),
            "claimable_pending_demand_by_queue must mirror the activity-pause gate"
        );
        // Pin the `task_type` scoping explicitly, not just via the renderer:
        // these are read paths with no behavioural test asserting their
        // predicate shape, so a contributor "simplifying" the guard away (on
        // the mistaken belief that a workflow row's `activity_name` is always
        // NULL) would otherwise reintroduce the `mixed_signal_suspension`
        // sentinel hazard here with CI still green.
        let scoping = format!(
            "task_type = '{}'",
            crate::activity_pause::ACTIVITY_TASK_TYPE
        );
        assert!(
            oldest_pending_ages_query().contains(&scoping),
            "oldest_pending_ages must scope the activity-pause gate by task_type"
        );
        assert!(
            claimable_pending_demand_query().contains(&scoping),
            "claimable_pending_demand_by_queue must scope the activity-pause gate by task_type"
        );
    }

    // ── TTL'd rate-limit override drift locks (issue #945) ──────────────────
    //
    // These sites are hand-copied literal SQL (2 of the 3 live inside
    // `pub const fn` bodies, which cannot call runtime helpers), so nothing
    // stops them drifting from `effective_available_tokens_expr` except a
    // test asserting textual containment -- mirroring the queue-pause /
    // activity-pause drift locks immediately above.

    #[test]
    fn claim_task_query_honors_the_effective_rate_limit_override() {
        let sql = claim_task_query();
        let effective = effective_available_tokens_expr("b");
        // Three occurrences: the candidate EXISTS gate, and the
        // rate_limit_debit CTE's own SET (the debit) and WHERE (the re-check
        // immediately before debiting).
        assert_eq!(
            sql.matches(&effective).count(),
            3,
            "claim_task_query must apply the effective-rate-limit formula at \
             the EXISTS gate and both halves of the rate_limit_debit CTE; \
             got:\n{sql}"
        );
    }

    #[test]
    fn oldest_pending_ages_query_honors_the_effective_rate_limit_override() {
        assert!(
            oldest_pending_ages_query().contains(&effective_available_tokens_expr("b")),
            "oldest_pending_ages must mirror the effective-rate-limit override, \
             or the gauge reads stale post-override numbers"
        );
    }

    #[test]
    fn claimable_pending_demand_query_honors_the_effective_rate_limit_override() {
        assert!(
            claimable_pending_demand_query().contains(&effective_available_tokens_expr("b")),
            "claimable_pending_demand_by_queue must mirror the effective-rate-limit \
             override, or the stranded-work sampler misreports coverage while an \
             override is active"
        );
    }

    // ── Dynamic per-key rate-limit bucket keys (issue #699) ─────────────────

    #[test]
    fn dynamic_rate_bucket_key_is_prefixed_and_namespaced_by_expr_and_value() {
        // The `input.` prefix is normalized away (issue #699 review, #6).
        let k = dynamic_rate_bucket_key("input.tenant_id", Some("acme"));
        // Each component is a self-delimiting length-tagged literal
        // (`L{len}:{value}`); `tenant_id` is 9 bytes, `acme` is 4.
        assert_eq!(k, "dyn-rate:L9:tenant_id:L4:acme");
        assert!(k.starts_with(DYNAMIC_RATE_PREFIX));
    }

    #[test]
    fn dynamic_rate_bucket_key_normalizes_input_prefix() {
        // Both spellings resolve the same field (via `resolve_concurrency_key`),
        // so they must share one bucket.
        assert_eq!(
            dynamic_rate_bucket_key("input.tenant_id", Some("acme")),
            dynamic_rate_bucket_key("tenant_id", Some("acme")),
        );
        assert_eq!(
            dynamic_rate_bucket_key("tenant_id", Some("acme")),
            "dyn-rate:L9:tenant_id:L4:acme"
        );
    }

    #[test]
    fn dynamic_rate_bucket_key_same_expr_and_value_is_stable() {
        assert_eq!(
            dynamic_rate_bucket_key("input.tenant_id", Some("acme")),
            dynamic_rate_bucket_key("input.tenant_id", Some("acme")),
        );
    }

    #[test]
    fn dynamic_rate_bucket_key_different_resolved_values_differ() {
        assert_ne!(
            dynamic_rate_bucket_key("input.tenant_id", Some("acme")),
            dynamic_rate_bucket_key("input.tenant_id", Some("globex")),
        );
    }

    #[test]
    fn dynamic_rate_bucket_key_empty_resolved_fallback_is_distinct() {
        // A legitimately-resolved empty-string tenant (`Some("")`) encodes as
        // the length-tagged literal `L0:`, distinct from a real tenant.
        let empty_tenant = dynamic_rate_bucket_key("input.tenant_id", Some(""));
        assert_eq!(empty_tenant, "dyn-rate:L9:tenant_id:L0:");
        assert_ne!(
            empty_tenant,
            dynamic_rate_bucket_key("input.tenant_id", Some("acme"))
        );
    }

    #[test]
    fn dynamic_rate_bucket_key_none_unresolved_is_distinct_from_empty_string_tenant() {
        // Regression (issue #699 review, Codex round-5 P2): `None` (the key
        // expression could NOT be resolved -- missing / null / non-object input)
        // must NOT share a bucket with `Some("")` (a legitimately-resolved
        // empty-string tenant). The previous `.unwrap_or_default()` at the call
        // site collapsed both onto the same `L0:` bucket, cross-throttling
        // malformed/missing executions against a real empty tenant.
        let unresolved = dynamic_rate_bucket_key("input.tenant_id", None);
        // The unresolved fallback encodes with the distinct `U` marker.
        assert_eq!(unresolved, "dyn-rate:L9:tenant_id:U");

        let empty_tenant = dynamic_rate_bucket_key("input.tenant_id", Some(""));
        assert_eq!(empty_tenant, "dyn-rate:L9:tenant_id:L0:");

        // The whole point: the two are DIFFERENT bucket keys.
        assert_ne!(
            unresolved, empty_tenant,
            "an unresolved key (None) must never share a bucket with an \
             empty-string tenant (Some(\"\"))"
        );
        // And the unresolved fallback is still distinct from a real tenant, and
        // stable across calls (one shared fallback bucket per expression).
        assert_ne!(
            unresolved,
            dynamic_rate_bucket_key("input.tenant_id", Some("acme"))
        );
        assert_eq!(unresolved, dynamic_rate_bucket_key("input.tenant_id", None));
    }

    #[test]
    fn dynamic_rate_bucket_key_is_injective_across_colon_containing_components() {
        // Regression (issue #699 review, Codex P2): before the self-delimiting
        // length-tagged component encoding, a `:` inside `key_expr` (a dot-path
        // can address a JSON key containing `:`) or inside `resolved` could
        // collide two genuinely distinct `(expr, resolved)` pairs onto one
        // bucket string. Both of these used to flatten to `dyn-rate:a:b:c`.
        assert_eq!(
            dynamic_rate_bucket_key("a", Some("b:c")),
            "dyn-rate:L1:a:L3:b:c"
        );
        assert_eq!(
            dynamic_rate_bucket_key("a:b", Some("c")),
            "dyn-rate:L3:a:b:L1:c"
        );
        assert_ne!(
            dynamic_rate_bucket_key("a", Some("b:c")),
            dynamic_rate_bucket_key("a:b", Some("c")),
        );
    }

    #[test]
    fn bound_key_component_literal_never_collides_with_a_hash_encoding() {
        // Regression (issue #699 review, Codex P2): the `L`/`H` first-byte tags
        // make the literal and hash encodings structurally disjoint. A short
        // literal whose *value* is exactly a hash's string encoding must NOT
        // collide with that hash — the pre-fix `h:{digest}` hash form was itself
        // a valid short literal, so a long value hashing to digest `D` and a
        // short literal value equal to `h:{D}` produced the same component.
        let long = "x".repeat(300);
        let hash_form = bound_key_component(&long); // `H{64hex}` (long > 256)
        assert!(
            hash_form.starts_with('H'),
            "long value must hash, got {hash_form}"
        );
        assert_eq!(
            hash_form.len(),
            65,
            "SHA-256 hash form is `H` + 64 hex chars, got {hash_form}"
        );
        // A literal whose value is exactly the hash's string encoding.
        let literal_that_equals_the_hash = hash_form.clone();
        let literal_form = bound_key_component(&literal_that_equals_the_hash);
        assert!(
            literal_form.starts_with('L'),
            "short literal must be length-tagged, got {literal_form}"
        );
        assert_ne!(
            hash_form, literal_form,
            "a literal equal to a hash's encoding must not collide with that hash"
        );
        // And the same disjointness holds end-to-end through the composite key:
        // a long resolved value (hashed) vs a short literal equal to that hash.
        let hashed = dynamic_rate_bucket_key("tenant_id", Some(&long));
        let literal = dynamic_rate_bucket_key("tenant_id", Some(&literal_that_equals_the_hash));
        assert_ne!(hashed, literal);
    }

    #[test]
    fn rate_limit_gauge_sampler_filter_excludes_unbounded_key_families() {
        // Cardinality guard (issue #699 review, #1): the per-key gauge sampler
        // must never emit an unbounded per-tenant key as a metric label.
        assert!(RATE_LIMIT_GAUGE_SAMPLER_FILTER.contains("NOT LIKE 'dyn-rate:%'"));
        assert!(RATE_LIMIT_GAUGE_SAMPLER_FILTER.contains("NOT LIKE 'start-throttle:%'"));
    }

    // -----------------------------------------------------------------------
    // TTL'd rate-limit / start-throttle overrides (issue #945)
    // -----------------------------------------------------------------------

    #[test]
    fn max_pacing_override_ttl_secs_is_24_hours() {
        assert_eq!(MAX_PACING_OVERRIDE_TTL_SECS, 86_400);
    }

    #[test]
    fn effective_refill_rate_expr_renders_a_coalesced_case_expression() {
        let rendered = effective_refill_rate_expr("b");
        assert!(rendered.starts_with("COALESCE(CASE WHEN b.override_expires_at > NOW() THEN"));
        assert!(rendered.contains("b.override_refill_rate ELSE NULL END"));
        assert!(rendered.ends_with("b.refill_rate)"));
    }

    #[test]
    fn effective_burst_expr_renders_a_coalesced_case_expression() {
        let rendered = effective_burst_expr("b");
        assert!(rendered.starts_with("COALESCE(CASE WHEN b.override_expires_at > NOW() THEN"));
        assert!(rendered.contains("b.override_burst ELSE NULL END"));
        assert!(rendered.ends_with("b.burst)"));
    }

    #[test]
    fn effective_available_tokens_expr_composes_burst_and_refill_rate() {
        let rendered = effective_available_tokens_expr("b");
        assert!(rendered.starts_with("LEAST("));
        // Contains both sub-expressions verbatim -- this is the drift-lock
        // property every claim-gate/debit/refund/gauge SQL site relies on:
        // any hand-copied occurrence of the burst or refill-rate formula must
        // be an exact substring of what these two helpers independently
        // render, or the containment assertions below (which mirror the
        // actual SQL sites) would fail.
        assert!(rendered.contains(&effective_burst_expr("b")));
        assert!(rendered.contains(&effective_refill_rate_expr("b")));
        assert!(rendered.contains("b.tokens + EXTRACT(EPOCH FROM (NOW() - b.last_refilled_at))"));
    }

    #[test]
    fn effective_expr_helpers_qualify_by_the_given_alias() {
        // The un-aliased single-table call sites (`try_consume_rate_limit_token`,
        // `refund_rate_limit_token`, `sample_rate_limit_buckets`) qualify every
        // column with the bare table name -- Postgres permits qualifying an
        // UPDATE/SELECT target by its own name even without an explicit alias.
        let rendered = effective_available_tokens_expr("harvest_rate_limit_buckets");
        assert!(rendered.contains("harvest_rate_limit_buckets.override_expires_at"));
        assert!(rendered.contains("harvest_rate_limit_buckets.tokens"));
        assert!(rendered.contains("harvest_rate_limit_buckets.last_refilled_at"));
    }

    #[test]
    fn resolve_effective_rate_limit_with_no_override_returns_baseline() {
        let now = Utc::now();
        let effective = resolve_effective_rate_limit(5.0, 10.0, None, None, None, now);
        assert_eq!(
            effective,
            EffectiveRateLimit {
                refill_rate: 5.0,
                burst: 10.0,
                override_active: false,
            }
        );
    }

    #[test]
    fn resolve_effective_rate_limit_with_active_override_both_fields() {
        let now = Utc::now();
        let expires = now + Duration::seconds(60);
        let effective =
            resolve_effective_rate_limit(5.0, 10.0, Some(50.0), Some(100.0), Some(expires), now);
        assert_eq!(
            effective,
            EffectiveRateLimit {
                refill_rate: 50.0,
                burst: 100.0,
                override_active: true,
            }
        );
    }

    #[test]
    fn resolve_effective_rate_limit_only_refill_rate_overridden_burst_falls_back() {
        let now = Utc::now();
        let expires = now + Duration::seconds(60);
        let effective =
            resolve_effective_rate_limit(5.0, 10.0, Some(50.0), None, Some(expires), now);
        assert_eq!(
            effective,
            EffectiveRateLimit {
                refill_rate: 50.0,
                burst: 10.0, // COALESCE falls through to the declared baseline
                override_active: true,
            }
        );
    }

    #[test]
    fn resolve_effective_rate_limit_only_burst_overridden_refill_rate_falls_back() {
        let now = Utc::now();
        let expires = now + Duration::seconds(60);
        let effective =
            resolve_effective_rate_limit(5.0, 10.0, None, Some(100.0), Some(expires), now);
        assert_eq!(
            effective,
            EffectiveRateLimit {
                refill_rate: 5.0, // COALESCE falls through to the declared baseline
                burst: 100.0,
                override_active: true,
            }
        );
    }

    #[test]
    fn resolve_effective_rate_limit_expired_override_falls_back_to_baseline() {
        let now = Utc::now();
        let expired = now - Duration::seconds(1);
        let effective =
            resolve_effective_rate_limit(5.0, 10.0, Some(50.0), Some(100.0), Some(expired), now);
        // Reverting needs no operator action and no background sweeper: a
        // plain `> NOW()` comparison, evaluated fresh, is enough.
        assert_eq!(
            effective,
            EffectiveRateLimit {
                refill_rate: 5.0,
                burst: 10.0,
                override_active: false,
            }
        );
    }

    #[test]
    fn resolve_effective_rate_limit_exact_now_boundary_is_not_active() {
        // `override_expires_at == now` must NOT be considered active -- the
        // SQL renders a strict `>`, not `>=`, and this pure-Rust mirror must
        // agree exactly at the boundary.
        let now = Utc::now();
        let effective =
            resolve_effective_rate_limit(5.0, 10.0, Some(50.0), Some(100.0), Some(now), now);
        assert_eq!(
            effective,
            EffectiveRateLimit {
                refill_rate: 5.0,
                burst: 10.0,
                override_active: false,
            }
        );
    }

    #[test]
    fn dynamic_rate_bucket_key_cannot_collide_with_static_or_throttle_keys() {
        // Static keys are bare activity names / user strings; throttle keys use
        // the `start-throttle:` prefix. The `dyn-rate:` prefix keeps dynamic
        // per-key buckets disjoint from both.
        let dynamic = dynamic_rate_bucket_key("input.tenant_id", Some("acme"));
        assert!(!dynamic.starts_with(crate::throttle::THROTTLE_BUCKET_PREFIX));
        assert_ne!(dynamic, "send_email"); // a bare static activity-name key
    }

    #[test]
    fn dynamic_rate_bucket_key_bounds_oversized_resolved_value() {
        // A short value is a human-readable length-tagged literal.
        let short = dynamic_rate_bucket_key("tenant_id", Some("acme"));
        assert_eq!(short, "dyn-rate:L9:tenant_id:L4:acme");

        // A pathologically large resolved value (would blow the btree PK limit)
        // is replaced with a bounded, stable hash.
        let big = "x".repeat(5000);
        let bounded = dynamic_rate_bucket_key("tenant_id", Some(&big));
        // `dyn-rate:L9:tenant_id:H{64hex}` = 87 bytes regardless of the 5000-char
        // input — bounded far below the ~2704-byte btree PK limit.
        assert!(
            bounded.chars().count() < 128,
            "oversized resolved value must be bounded, got len {}",
            bounded.chars().count()
        );
        // The resolved component is the fixed-width hash `H{64hex}`.
        assert!(bounded.starts_with("dyn-rate:L9:tenant_id:H"));
        // Deterministic: same long value → same key.
        assert_eq!(bounded, dynamic_rate_bucket_key("tenant_id", Some(&big)));
        // Injective in practice: two distinct long values → distinct keys.
        let big2 = "y".repeat(5000);
        assert_ne!(bounded, dynamic_rate_bucket_key("tenant_id", Some(&big2)));
    }

    #[test]
    fn dynamic_rate_bucket_key_bounds_oversized_expr_component() {
        // Regression (issue #699 review, Codex P2): an unbounded expression (a
        // very long dot-path, or a hand-built `ActivityInfo.rate_limit_key`)
        // must not be able to overflow the composite btree PRIMARY KEY and abort
        // the enqueue transaction. The expr component is now bounded/hashed too.
        let big_expr = "a".repeat(5000);
        let bounded = dynamic_rate_bucket_key(&big_expr, Some("acme"));
        // The expr component is the fixed-width hash `H{64hex}` (collision-
        // resistant SHA-256); the resolved component is the length-tagged
        // literal `L4:acme`. Recompute the expected digest via the same SHA-256.
        let expected_hex = {
            use sha2::{Digest, Sha256};
            use std::fmt::Write as _;
            let mut s = String::new();
            for b in Sha256::digest(big_expr.as_bytes()) {
                let _ = write!(s, "{b:02x}");
            }
            s
        };
        assert_eq!(bounded, format!("dyn-rate:H{expected_hex}:L4:acme"));
        assert!(bounded.starts_with("dyn-rate:H"));
        // The whole composite key is provably well under the ~2704 btree limit.
        assert!(
            bounded.len() < 2704,
            "bounded key must fit the btree PK limit, got len {}",
            bounded.len()
        );
        // Deterministic: same long expr → same key.
        assert_eq!(bounded, dynamic_rate_bucket_key(&big_expr, Some("acme")));
    }

    #[test]
    fn dynamic_rate_bucket_key_distinct_oversized_exprs_get_distinct_keys() {
        // Two DISTINCT oversized expressions must not collapse onto one bucket
        // (they are independently validated and may declare different rps).
        let big_a = "a".repeat(5000);
        let big_b = "b".repeat(5000);
        assert_ne!(
            dynamic_rate_bucket_key(&big_a, Some("acme")),
            dynamic_rate_bucket_key(&big_b, Some("acme")),
        );
    }

    #[test]
    fn dynamic_rate_bucket_key_is_provably_bounded_for_two_oversized_components() {
        // Both components oversized → both hashed; the whole key stays tiny and
        // well under the Postgres btree PRIMARY KEY size limit (~2704 bytes).
        let big_expr = "a".repeat(5000);
        let big_val = "x".repeat(5000);
        let key = dynamic_rate_bucket_key(&big_expr, Some(&big_val));
        assert!(
            key.len() < 530,
            "both-oversized key must be ≤ ~530 bytes, got len {}",
            key.len()
        );
    }

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
    fn release_suspended_workflow_claim_query_is_ownership_guarded_and_not_skip_locked() {
        let sql = release_suspended_workflow_claim_query();
        assert!(sql.contains("SET state = 'PENDING'"));
        assert!(sql.contains("worker_id = NULL"));
        assert!(sql.contains("started_at = NULL"));
        assert!(
            sql.contains("wake_requested = FALSE"),
            "a wake that landed in the SKIP LOCKED contention window must be \
             reconciled by this release, not left set with nothing to consume it",
        );
        assert!(
            sql.contains("id = $1")
                && sql.contains("state = 'RUNNING'")
                && sql.contains("worker_id = $2")
                && sql.contains("crash_strikes = $3"),
            "must be guarded on the exact claim token (worker_id AND crash_strikes), \
             the same pair claim_still_held_for_update checks",
        );
        assert!(
            !sql.contains("SKIP LOCKED"),
            "must block behind a transient lock holder and reconcile against the \
             post-commit row, not skip it and report a false ownership transfer",
        );
        assert!(
            sql.contains("sticky_until = CASE"),
            "must preserve/refresh sticky affinity like a normal re-pend, not clear \
             it like the capability-miss release (this worker was never found \
             incapable, so there is no reason to stop routing to it)",
        );
        assert!(
            sql.contains("capability_misses = 0") && sql.contains("capability_miss_workers = '{}'"),
            "a release here is reached only after the handler dispatch already ran, \
             so it is unconditionally proof of capability -- like every park caller \
             except the pre-handler-lookup ingest one -- and must reset stale \
             evidence from a prior incapable worker, matching \
             park_workflow_task_sticky_query(true)'s established precedent",
        );
        assert!(
            sql.contains("crash_strikes = 0"),
            "the handler ran to a conclusion on this worker (it made no \
             replay-significant progress, but the process did not crash), so the \
             issue #367 crash streak is genuinely broken -- matching \
             release_task_for_capability_miss_query's AfterHandler phase, which \
             resets crash_strikes for the identical 'the body reached a conclusion \
             on this worker' reason (Codex review round 4 of #1182)",
        );
    }

    #[test]
    fn park_workflow_task_queries_capture_wake_requested_before_clearing_it() {
        for sql in [
            park_workflow_task_query(true),
            park_workflow_task_sticky_query(true),
            park_workflow_task_query(false),
            park_workflow_task_sticky_query(false),
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
        for sql in [
            park_workflow_task_sticky_query(true),
            park_workflow_task_sticky_query(false),
        ] {
            assert!(sql.contains("sticky_worker_id = $2"));
            assert!(sql.contains("sticky_until = NOW() + $3"));
            assert!(sql.contains("sticky_timeout = $3"));
        }
    }

    #[test]
    fn park_workflow_task_query_clears_sticky_columns() {
        for sql in [
            park_workflow_task_query(true),
            park_workflow_task_query(false),
        ] {
            assert!(sql.contains("sticky_worker_id = NULL"));
            assert!(sql.contains("sticky_until = NULL"));
            assert!(sql.contains("sticky_timeout = NULL"));
        }
    }

    // -----------------------------------------------------------------------
    // Capability-miss release (issue #804)
    // -----------------------------------------------------------------------

    /// Durable inline progress moves the frontier, so the budget it carried
    /// belongs to a handler the run no longer needs. Same reset the park path
    /// performs, at the other point a workflow task demonstrably advances
    /// (issue #804, Codex round-20 P1).
    #[test]
    fn inline_progress_reset_clears_both_capability_miss_columns() {
        let sql = reset_capability_misses_after_inline_progress_query();
        assert!(
            sql.contains("capability_misses = 0"),
            "inline progress must zero the count: {sql}"
        );
        assert!(
            sql.contains("capability_miss_workers = '{}'"),
            "clearing the count alone leaves the DISTINCT set describing the \
             frontier that is now behind us, and that set is what the primary \
             bound escalates on: {sql}"
        );
    }

    /// The reset may only ever rewrite the row this dispatch still holds. A
    /// concurrent poison-pill reclaim (or an operator action) can take the claim
    /// mid-dispatch; refunding the budget on a row now owned by someone else
    /// would hand a fresh budget to a frontier that never advanced.
    #[test]
    fn inline_progress_reset_is_guarded_on_this_workers_claim() {
        let sql = reset_capability_misses_after_inline_progress_query();
        assert!(
            sql.contains("state = 'RUNNING'"),
            "must not resurrect a budget on a row that left RUNNING: {sql}"
        );
        assert!(
            sql.contains("worker_id = $2"),
            "must not rewrite a row another worker now holds: {sql}"
        );
    }

    /// Retiring a frontier must NOT clear the consecutive-failure strike
    /// counters (issue #804, Codex round-24 P2 — declined, and pinned here so
    /// the decline is enforced rather than only argued).
    ///
    /// Every dispatch of a task that completes one local activity and then
    /// kills its worker reaches this statement first, so a `crash_strikes = 0`
    /// here would land on the crashing dispatch too.
    /// `poison_pill::reclaim_orphaned_tasks` increments from the row's *current*
    /// value, so the counter could never exceed `1` and the default
    /// `poison_pill_threshold` of 3 would be unreachable — for the entire class
    /// of poison that crashes after making some progress. The same holds for
    /// #494's timeout strike and #782's panic strike, which live off-row but
    /// are gated by the same `CapabilityMissPhase` rule.
    ///
    /// The counters are only ever preserved here, never incremented, so AC4
    /// holds; genuine progress clears them by the intended route, the clean
    /// continuation, which fires when the body carries forward to a suspension
    /// instead of stalling at a later frontier.
    #[test]
    fn inline_progress_reset_never_launders_a_crash_strike() {
        let sql = reset_capability_misses_after_inline_progress_query();
        assert!(
            !sql.contains("crash_strikes"),
            "retiring a frontier is not evidence about worker crashes -- the \
             crash happens AFTER it, so zeroing here caps the counter at 1 for \
             any task that crashes post-progress: {sql}"
        );
        // Belt and braces: the statement's whole SET clause is the two
        // capability columns, so no other streak state can be swept in by a
        // future edit either.
        let set = sql
            .split("SET ")
            .nth(1)
            .and_then(|s| s.split(" WHERE").next())
            .expect("statement has a SET ... WHERE shape");
        assert_eq!(
            set.matches('=').count(),
            2,
            "the reset must assign exactly the two capability columns: {set}"
        );
    }

    /// A same-ID restart is explicitly supported: `workers::register_worker`
    /// upserts on `worker_id` and refreshes `started_at`/`build_id`. But
    /// `capability_miss_workers` stores bare IDs, so the new — possibly capable
    /// — instance is indistinguishable from the old incapable one. Removing
    /// exactly that ID on re-registration restores the truth (issue #804, Codex
    /// round-26 P1).
    #[test]
    fn registration_invalidates_only_that_workers_miss_evidence() {
        let sql = invalidate_capability_miss_evidence_for_worker_query();
        assert!(
            sql.contains("array_remove(capability_miss_workers, $1)"),
            "must drop exactly the re-registering ID, not clear the set -- other \
             workers' evidence is still true: {sql}"
        );
        assert!(
            sql.contains("$1 = ANY(capability_miss_workers)"),
            "must only touch rows that actually carry this ID's evidence: {sql}"
        );
        assert!(
            sql.contains("state IN ('PENDING', 'RUNNING')"),
            "must not rewrite terminal history on the startup path -- a completed \
             task is never redelivered, so its evidence cannot affect any decision, \
             and this runs inside register_worker's transaction (Codex round-35 P2): {sql}"
        );
    }

    /// `capability_misses` is a true historical count of releases and backs the
    /// **ungated** absolute ceiling that bounds the release loop when no fleet
    /// evidence is available. Decrementing it here would remove the only bound
    /// that still applies to a worker crash-looping on the same incapable build
    /// (which now clears its own evidence on every restart), turning a bounded
    /// degradation into an unbounded release loop.
    #[test]
    fn registration_invalidation_never_refunds_the_release_budget() {
        let sql = invalidate_capability_miss_evidence_for_worker_query();
        assert!(
            !sql.contains("capability_misses"),
            "the count is historical and backs the ungated absolute ceiling; \
             only the DISTINCT-worker evidence may be invalidated: {sql}"
        );
        let set = sql
            .split("SET ")
            .nth(1)
            .and_then(|s| s.split(" WHERE").next())
            .expect("statement has a SET ... WHERE shape");
        assert_eq!(
            set.matches('=').count(),
            1,
            "invalidation must assign exactly the one evidence column: {set}"
        );
    }

    /// A worker only observes — and therefore may only invalidate evidence on —
    /// the queues it actually polls. An unscoped statement would let a worker on
    /// one queue hand a fresh `AllLiveWorkersMissed` reprieve to a task on a
    /// queue it will never claim from.
    #[test]
    fn registration_invalidation_is_scoped_to_the_workers_queues() {
        let sql = invalidate_capability_miss_evidence_for_worker_query();
        assert!(
            sql.contains("queue_name = ANY($2)"),
            "evidence invalidation must be scoped to this worker's queues: {sql}"
        );
    }

    /// The one writer of these columns that is deliberately NOT ownership-
    /// guarded, and must stay that way (issue #804, Codex round-27 P1).
    ///
    /// The row whose decision stale evidence poisons is precisely the one
    /// another worker is *already processing*: it claimed before this restart,
    /// so its snapshot still lists the old incapable instance. Adding a
    /// `state = 'RUNNING' AND worker_id = ...` guard here — the reflex, since
    /// every sibling writer has one — would skip exactly that row and leave the
    /// round-26 terminal failure intact. The claim-vs-write race it creates is
    /// answered on the read side instead, by `read_capability_miss_state`.
    #[test]
    fn registration_invalidation_must_reach_rows_under_an_active_claim() {
        let sql = invalidate_capability_miss_evidence_for_worker_query();
        assert!(
            !sql.contains("state = 'RUNNING'") && !sql.contains("worker_id"),
            "an ownership guard here would skip the claimed row whose decision \
             the stale evidence actually poisons; the race is answered by the \
             decision-time re-read instead: {sql}"
        );
    }

    /// The decision-time re-read must only ever return counters for a row this
    /// worker still holds. Returning a foreign owner's counters would let this
    /// dispatch escalate a claim it no longer has.
    #[test]
    fn miss_state_read_is_ownership_guarded() {
        let sql = read_capability_miss_state_query();
        assert!(
            sql.contains("state = 'RUNNING'") && sql.contains("worker_id = $2"),
            "the re-read must be scoped to this worker's own claim: {sql}"
        );
        assert!(
            sql.contains("capability_misses") && sql.contains("capability_miss_workers"),
            "both counters must be refreshed together, or the pair could \
             disagree about the same frontier: {sql}"
        );
    }

    /// The re-read must apply the row's counters only when they are evidence
    /// about the handler this dispatch is actually stuck on (issue #804, Codex
    /// round-46 P1).
    ///
    /// Doing the comparison in SQL rather than in the caller keeps it ATOMIC
    /// with the read: the row cannot change frontier between deciding which
    /// counters apply and reading them.
    #[test]
    fn miss_state_read_is_keyed_to_the_frontier() {
        let sql = read_capability_miss_state_query();
        assert!(
            sql.contains("capability_miss_handler IS DISTINCT FROM $3"),
            "the read must compare the row's recorded frontier against the one \
             this dispatch is missing: {sql}"
        );
        // `IS DISTINCT FROM`, never `<>`: a NULL handler must read as a
        // mismatch (a fresh budget), and `<> $3` is NULL -- neither branch --
        // for a NULL left side.
        assert!(
            !sql.contains("capability_miss_handler <> $3")
                && !sql.contains("capability_miss_handler != $3"),
            "a plain inequality yields NULL for an unrecorded frontier, which \
             is neither arm of the CASE: {sql}"
        );
        assert!(
            sql.contains("THEN 0 ELSE capability_misses END")
                && sql.contains("THEN '{}'::text[] ELSE capability_miss_workers END"),
            "BOTH counters must be zeroed on a mismatch, or the pair would \
             disagree about which frontier it describes: {sql}"
        );
    }

    /// The commit-boundary guard must hold the row's lock (an unlocked check
    /// merely narrows the window before an unguarded `fail_task`), must key on
    /// the CLAIM rather than the worker, and must never WAIT for the lock.
    #[test]
    fn commit_boundary_claim_guard_locks_without_waiting_and_keys_on_the_claim() {
        let sql = claim_still_held_for_update_query();
        assert!(
            sql.contains("FOR UPDATE"),
            "the guard must hold the lock through the caller's transaction, not \
             just read: {sql}"
        );
        assert!(
            sql.contains("SKIP LOCKED"),
            "the guard must never WAIT on the task row: `poison_pill` takes task \
             -> execution while this path takes execution -> task, so a blocking \
             wait here closes an ABBA cycle: {sql}"
        );
        assert!(
            sql.contains("state = 'RUNNING'") && sql.contains("worker_id = $2"),
            "the guard must still be scoped to this worker's own claim: {sql}"
        );
        assert!(
            sql.contains("crash_strikes = $3"),
            "a poison-pill requeue lets the SAME worker re-claim the row, so \
             (state, worker_id) alone does not identify this attempt: {sql}"
        );
    }

    #[test]
    fn park_queries_reset_the_capability_miss_counter() {
        // `capability_misses` counts CONSECUTIVE misses: a task a capable
        // worker has actually processed must start its next deploy with a full
        // budget. Parking is the dominant success path for a workflow task
        // (claim -> run a decision cycle -> suspend on an activity/timer/
        // signal), and it is only ever reached AFTER the handler lookup in
        // `process_workflow_task` succeeded — so a park is proof a capable
        // worker handled this row.
        //
        // Without this, a long-lived execution that absorbed k misses during
        // one deploy carries them forever and escalates after only `budget - k`
        // misses during the next — failing a healthy run during a routine
        // deploy, which is the exact outcome issue #804 exists to prevent.
        for sql in [
            park_workflow_task_query(true),
            park_workflow_task_sticky_query(true),
        ] {
            assert!(
                sql.contains("capability_misses = 0"),
                "parking must reset the capability-miss budget: {sql}"
            );
            assert!(
                sql.contains("capability_miss_workers = '{}'"),
                "parking must clear the DISTINCT-worker set as well, or a task \
                 a capable worker handled would keep every prior misser and \
                 escalate early on the next deploy: {sql}"
            );
        }
    }

    #[test]
    fn capability_miss_release_query_is_ownership_guarded() {
        // Asserted for BOTH literal variants: the crash-strike flag selects
        // between two statements, so every shared clause must be pinned in
        // both or they can silently drift apart.
        for sql in [
            release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler),
            release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler),
        ] {
            assert!(
                sql.contains("state = 'RUNNING'"),
                "compare-and-swap: only a still-claimed row may be released",
            );
            assert!(
                sql.contains("worker_id = $2"),
                "a worker may only ever undo its OWN claim -- a concurrent \
                 poison-pill reclaim must not be clobbered",
            );
            assert!(sql.contains("id = $1"));
        }
    }

    #[test]
    fn capability_miss_release_query_restores_the_attempt_only_before_the_handler() {
        // Issue #804 Codex round-17 P2. `attempt` does double duty on a workflow
        // row: it is the retry budget AND the "is this the first dispatch?"
        // signal `record_workflow_started` reads. Restoring it is required when
        // nothing ran, and wrong once the start metric has already fired.
        assert!(
            release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler)
                .contains("attempt = GREATEST(attempt - 1, 0)"),
            "claim_task does `attempt + 1`; a pre-handler miss never ran the \
             handler, so it must not consume the retry budget (AC4)",
        );
        for phase in [
            CapabilityMissPhase::DuringHandler,
            CapabilityMissPhase::AfterHandler,
        ] {
            let sql = release_task_for_capability_miss_query(phase);
            assert!(
                !sql.contains("GREATEST(attempt - 1, 0)"),
                "{phase:?}: the start metric already fired for this execution, \
                 so rolling `attempt` back to 0 would make the next capable \
                 claim re-emit `harvest.workflow.started`: {sql}",
            );
            assert!(
                !sql.contains("attempt ="),
                "{phase:?}: a post-handler release must leave `attempt` alone \
                 entirely, not rewrite it to some other value: {sql}",
            );
        }
    }

    /// The release must report the cardinality **its own `UPDATE` produced**,
    /// not the one the caller read beforehand (issue #804, Codex round-36 P2).
    ///
    /// `observe_miss_and_fleet` reads `capability_miss_workers` and derives
    /// `distinct_after` from that snapshot. Between that read and this `UPDATE`,
    /// a peer restarting onto a capable build runs
    /// `invalidate_capability_miss_evidence_for_worker`, which `array_remove`s
    /// its own id from exactly these `RUNNING` rows. The `UPDATE` then appends
    /// the claimant to the *post-invalidation* array, so the durable set can be
    /// smaller than the snapshot predicted — `{A,B}` + claimant `C` is logged as
    /// 3 even though `A` was invalidated first and the row actually holds
    /// `{B,C}`.
    ///
    /// Returning the count from the statement that wrote it is the only way the
    /// number is true by construction rather than by a timing assumption.
    #[test]
    fn capability_miss_release_returns_the_cardinality_it_committed() {
        for phase in [
            CapabilityMissPhase::BeforeHandler,
            CapabilityMissPhase::DuringHandler,
            CapabilityMissPhase::AfterHandler,
        ] {
            let sql = release_task_for_capability_miss_query(phase);
            assert!(
                sql.contains(
                    "RETURNING COALESCE(array_length(capability_miss_workers, 1), 0) \
                     AS distinct_miss_workers"
                ),
                "{phase:?}: the release must return its own post-update \
                 cardinality -- a value derived from the caller's earlier \
                 snapshot silently over-reports when a peer's re-registration \
                 invalidated an entry in between: {sql}",
            );
        }
    }

    /// The release must be guarded on the **claim epoch**, not just the claim's
    /// worker id (issue #804, Codex round-37 P1).
    ///
    /// `poison_pill::requeue_orphan` hands an orphaned row back to the pool as
    /// `PENDING` / `worker_id = NULL` with `crash_strikes + 1`, and nothing
    /// stops the *same* worker from winning it again. A guard keyed on
    /// `(state, worker_id)` alone therefore matches that **new** claim, so a
    /// stale dispatcher's release would re-`PENDING` a row whose replacement
    /// handler is already running — inviting a second concurrent claim and
    /// duplicate side effects, and decrementing an `attempt` that belongs to
    /// the new dispatch.
    ///
    /// `crash_strikes` is the discriminator because the requeue that creates
    /// this race is the thing that bumps it. The terminal escalation guard
    /// ([`claim_still_held_for_update_query`]) already keys on it for exactly
    /// this reason; the release is the far more common path and must match.
    #[test]
    fn capability_miss_release_is_guarded_on_the_claim_epoch() {
        for phase in [
            CapabilityMissPhase::BeforeHandler,
            CapabilityMissPhase::DuringHandler,
            CapabilityMissPhase::AfterHandler,
        ] {
            let sql = release_task_for_capability_miss_query(phase);
            assert!(
                sql.contains("AND crash_strikes = $4"),
                "{phase:?}: a poison-pill requeue lets the SAME worker re-claim \
                 the row, so `worker_id` alone does not identify the claim this \
                 dispatcher holds -- releasing a live replacement claim risks a \
                 concurrent second dispatch: {sql}",
            );
        }
    }

    /// Every phase-selected literal must keep the clauses that are not about
    /// `attempt` or `crash_strikes` — three statements can drift where two
    /// could.
    #[test]
    fn capability_miss_release_query_variants_share_every_common_clause() {
        for phase in [
            CapabilityMissPhase::BeforeHandler,
            CapabilityMissPhase::DuringHandler,
            CapabilityMissPhase::AfterHandler,
        ] {
            let sql = release_task_for_capability_miss_query(phase);
            for clause in [
                "state = 'PENDING'",
                "worker_id = NULL",
                "started_at = NULL",
                "last_heartbeat_at = NULL",
                "ELSE LEAST(capability_misses, 2147483646) + 1",
                "capability_misses = CASE",
                "capability_miss_workers = CASE",
                "capability_miss_handler = $5",
                "scheduled_at = NOW() + make_interval(secs => $3)",
                "sticky_worker_id = NULL",
                "sticky_until = NULL",
                "sticky_timeout = NULL",
                "wake_requested = FALSE",
                "activity_name = CASE WHEN task_type = 'workflow' THEN NULL ELSE activity_name END",
            ] {
                assert!(
                    sql.contains(clause),
                    "{phase:?} lost the shared clause `{clause}`: {sql}",
                );
            }
        }
    }

    #[test]
    fn capability_miss_release_query_never_increments_crash_strikes() {
        for phase in [
            CapabilityMissPhase::BeforeHandler,
            CapabilityMissPhase::DuringHandler,
            CapabilityMissPhase::AfterHandler,
        ] {
            let sql = release_task_for_capability_miss_query(phase);
            assert!(
                !sql.contains("crash_strikes + 1") && !sql.contains("crash_strikes+1"),
                "AC4: crash_strikes must never be incremented by a capability \
                 miss, whichever phase detected it: {sql}",
            );
        }
    }

    /// The release may only clear `crash_strikes` when the handler actually ran
    /// (issue #804 x #367, Codex round-12 P1).
    ///
    /// `crash_strikes` is issue #367's evidence that a task keeps killing the
    /// worker process that runs it. A pre-/mid-handler capability miss ran
    /// nothing, so it is not evidence of anything -- and zeroing on it lets an
    /// incapable claim landing between two capable crashes reset the streak
    /// every time, so `poison_pill_threshold` is never reached in a
    /// heterogeneous fleet and the poison task crashes replacement workers
    /// indefinitely.
    ///
    /// Scoped to the `SET` clause: since round 37 the `WHERE` clause *reads*
    /// `crash_strikes` as the claim-epoch discriminator
    /// ([`capability_miss_release_is_guarded_on_the_claim_epoch`]), which is a
    /// different concern and does not weaken this one. The invariant here is
    /// that the release must not **write** the column.
    #[test]
    fn capability_miss_release_query_preserves_crash_strikes_for_a_pre_handler_miss() {
        let preserving = release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler);
        let (preserving_set, _) = preserving
            .split_once(" WHERE ")
            .expect("the release statement must be guarded by a WHERE clause");
        assert!(
            !preserving_set.contains("crash_strikes"),
            "a pre-/mid-handler miss must not write the column AT ALL -- the \
             handler never ran, so this dispatch is not evidence about whether \
             the task still crashes workers: {preserving}",
        );

        let clearing = release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler);
        assert!(
            clearing.contains("crash_strikes = 0"),
            "a post-handler miss DID watch the body run to a conclusion without \
             killing this worker, so the consecutive-crash streak is genuinely \
             broken (mirrors every sibling release path): {clearing}",
        );
    }

    #[test]
    fn capability_miss_release_query_increments_the_capability_counter() {
        // Asserted for BOTH literal variants: the crash-strike flag selects
        // between two statements, so every shared clause must be pinned in
        // both or they can silently drift apart.
        for sql in [
            release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler),
            release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler),
        ] {
            assert!(
                sql.contains("ELSE LEAST(capability_misses, 2147483646) + 1"),
                "the bounded-redelivery counter is the only counter a capability \
                 miss advances (AC3/AC4), and it must SATURATE rather than raise: \
                 a bare `+ 1` at i32::MAX aborts the statement, leaving the row \
                 RUNNING under a live worker that orphan reclamation skips \
                 (issue #804, Codex round-22 P2)",
            );
            // The saturating increment is the SAME-frontier arm: a release that
            // lands on a different handler than the row records restarts at 1
            // rather than inheriting a prior frontier's spend (Codex round-46
            // P1). Both arms are pinned so neither can be dropped alone.
            assert!(
                sql.contains("WHEN capability_miss_handler IS DISTINCT FROM $5 THEN 1"),
                "a miss on a NEW frontier must start its budget at 1, not \
                 continue the previous frontier's count: {sql}"
            );
            assert!(
                sql.contains("WHEN capability_miss_handler IS DISTINCT FROM $5 THEN ARRAY[$2]"),
                "a miss on a NEW frontier must start a fresh distinct-worker \
                 set, not inherit workers that missed a different handler: {sql}"
            );
            assert!(
                sql.contains("capability_miss_handler = $5"),
                "the release must RECORD the frontier its counters are evidence \
                 about, or the next dispatch cannot tell whose they are: {sql}"
            );
        }
    }

    /// The distinct-worker set must grow **idempotently** (issue #804 round 6).
    ///
    /// The budget is consumed per distinct worker precisely so that one
    /// incapable worker cannot exhaust it by repeatedly winning the claim race
    /// on its own released row. A blind `array_append` would defeat that: the
    /// same worker would add an entry per bounce and the set would grow past
    /// the budget just as fast as the plain counter did.
    #[test]
    fn capability_miss_release_query_appends_the_worker_only_once() {
        // Asserted for BOTH literal variants: the crash-strike flag selects
        // between two statements, so every shared clause must be pinned in
        // both or they can silently drift apart.
        for sql in [
            release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler),
            release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler),
        ] {
            assert!(
                sql.contains("$2 = ANY(capability_miss_workers)"),
                "a repeat miss by the same worker must be recognised, not appended \
                 again: {sql}"
            );
            assert!(
                sql.contains("array_append(capability_miss_workers, $2)"),
                "a NEW worker must be recorded so it consumes one budget unit: {sql}"
            );
            // $2 is the claiming worker id, already bound for the ownership guard.
            assert!(
                sql.contains("AND worker_id = $2"),
                "the appended id must be the SAME id the ownership guard checks, \
                 or the set would record a worker that never held the claim: {sql}"
            );
        }
    }

    /// The release must leave the task row's `error` column **untouched**.
    ///
    /// `error` is the failure-reporting channel for a task that genuinely ran
    /// and failed. The issue #773 `/stack` surface renders it verbatim as a
    /// `last_failure` on any pending activity, with `error_type: "Error"` and
    /// the row's raw `attempt`. A capability miss never ran the handler AND
    /// restores `attempt` to `0`, so writing the diagnostic there fabricates a
    /// failure that did not happen, at an otherwise-unreachable `attempt: 0`,
    /// and makes a blameless deploy skew indistinguishable from a real
    /// application failure on the exact surface whose runbook question is "why
    /// is this activity retrying?".
    ///
    /// Issue #773 AC3 states the invariant directly: a never-failed pending
    /// activity omits `last_failure` entirely. The diagnostic reaches the
    /// operator through the `tracing::info!` on release and the
    /// `harvest.task.capability_miss` counter, so nothing is lost by keeping
    /// it out of a column reserved for real failures.
    /// The pre-lookup park must preserve the miss accounting *atomically*.
    ///
    /// `park_workflow_task` zeroes `capability_misses`, which is proof-backed
    /// for every caller except the issue #779 transient-ingest-conflict
    /// re-drive, which runs before the handler lookup. That caller must keep the
    /// accounting — and it must do so by simply not writing those columns in the
    /// park statement itself, never by restoring them afterwards. The park
    /// commits in autocommit mode (`process_workflow_task` opens its transaction
    /// much later), so between a zeroing park and any follow-up restore a peer
    /// can claim the freshly-parked row and record its own miss, or a capable
    /// peer can park and legitimately reset it — and no single-statement merge of
    /// two whole snapshots is monotone against both of those. Omitting the SET
    /// assignments closes the window by construction: the row is never
    /// observably zeroed at all.
    #[test]
    fn park_preserving_capability_misses_never_writes_the_miss_columns() {
        for sql in [
            park_workflow_task_query(false),
            park_workflow_task_sticky_query(false),
        ] {
            assert!(
                !sql.contains("capability_misses"),
                "the pre-lookup park must leave the counter untouched in the \
                 same statement, not zero it and restore it in a second one: {sql}"
            );
            assert!(
                !sql.contains("capability_miss_workers"),
                "the pre-lookup park must leave the distinct-misser set \
                 untouched in the same statement: {sql}"
            );
            // Everything else a park does must still happen.
            assert!(sql.contains("worker_id = NULL"), "{sql}");
            assert!(sql.contains("started_at = NULL"), "{sql}");
            assert!(sql.contains("wake_requested = FALSE"), "{sql}");
        }
    }

    #[test]
    fn capability_miss_release_query_never_writes_the_error_column() {
        // Asserted for BOTH literal variants: the crash-strike flag selects
        // between two statements, so every shared clause must be pinned in
        // both or they can silently drift apart.
        for sql in [
            release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler),
            release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler),
        ] {
            assert!(
                !sql.contains("error ="),
                "a capability miss is not a task failure; writing `error` makes \
                 /stack fabricate a last_failure at attempt 0 (issue #773 AC3): {sql}"
            );
        }
    }

    #[test]
    fn capability_miss_release_query_unpins_sticky_so_a_peer_can_claim() {
        // Asserted for BOTH literal variants: the crash-strike flag selects
        // between two statements, so every shared clause must be pinned in
        // both or they can silently drift apart.
        for sql in [
            release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler),
            release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler),
        ] {
            // Without this the row stays pinned to the very worker that just
            // proved it cannot run it, so no peer can claim it and the task
            // bounces on one worker straight to escalation.
            assert!(sql.contains("sticky_worker_id = NULL"));
            assert!(sql.contains("sticky_until = NULL"));
            assert!(sql.contains("sticky_timeout = NULL"));
        }
    }

    #[test]
    fn capability_miss_release_query_clears_the_claim_columns() {
        // Asserted for BOTH literal variants: the crash-strike flag selects
        // between two statements, so every shared clause must be pinned in
        // both or they can silently drift apart.
        for sql in [
            release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler),
            release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler),
        ] {
            assert!(sql.contains("state = 'PENDING'"));
            assert!(sql.contains("worker_id = NULL"));
            assert!(sql.contains("started_at = NULL"));
            assert!(sql.contains("last_heartbeat_at = NULL"));
            assert!(
                sql.contains("wake_requested = FALSE"),
                "a wake captured mid-cycle must not short-circuit the backoff",
            );
        }
    }

    #[test]
    fn capability_miss_release_query_clears_the_sentinel_on_workflow_rows_only() {
        // Asserted for BOTH literal variants: the crash-strike flag selects
        // between two statements, so every shared clause must be pinned in
        // both or they can silently drift apart.
        for sql in [
            release_task_for_capability_miss_query(CapabilityMissPhase::AfterHandler),
            release_task_for_capability_miss_query(CapabilityMissPhase::BeforeHandler),
        ] {
            // A stale `mixed_signal_suspension` sentinel left in `activity_name` on
            // a WORKFLOW row lets an unrelated wake reset `scheduled_at` to now and
            // bypass the backoff (the issue #603 fix). On an ACTIVITY row
            // `activity_name` is load-bearing and must survive the release.
            assert!(
                sql.contains(
                    "activity_name = CASE WHEN task_type = 'workflow' THEN NULL ELSE activity_name END"
                ),
                "activity_name must be cleared for workflow rows and preserved for \
                 activity rows: {sql}",
            );
        }
    }

    /// PR-review regression test (Gemini finding): `PendingRequeueChangeset`
    /// must actually null out `worker_id`/`started_at`/`last_heartbeat_at` in
    /// the generated SQL, not silently omit them from the `SET` clause.
    /// Diesel's default `AsChangeset` treats a `None` field as "no update";
    /// without `treat_none_as_null = true` this test fails with the columns
    /// missing from the query entirely.
    #[test]
    fn pending_requeue_changeset_nulls_out_none_fields_in_generated_sql() {
        use crate::schema::harvest_task_queue::dsl;
        use diesel::debug_query;
        use diesel::pg::Pg;

        let changeset =
            PendingRequeueChangeset::new(chrono::Utc::now(), "some retryable error".to_string());
        let query = diesel::update(dsl::harvest_task_queue.filter(dsl::state.eq("RUNNING")))
            .set(&changeset);
        let debug = debug_query::<Pg, _>(&query).to_string();

        // With `treat_none_as_null = true`, a `None` field is bound as a SQL
        // NULL parameter rather than silently omitted from the `SET` clause
        // (Diesel's default behavior). Assert both: the column is present in
        // the generated SQL text, and its bound value is `None` (SQL NULL).
        for column in ["worker_id", "started_at", "last_heartbeat_at"] {
            assert!(
                debug.contains(&format!("\"{column}\" = $")),
                "{column} must appear as a bound column in the SET clause \
                 (not omitted): {debug}"
            );
        }
        assert!(
            debug.matches("None").count() >= 3,
            "worker_id/started_at/last_heartbeat_at must all bind to None (SQL NULL), \
             not be silently dropped from the query: {debug}"
        );
        assert!(debug.contains("\"state\" = "));
        assert!(debug.contains("\"crash_strikes\" = "));
        assert!(debug.contains("\"scheduled_at\" = "));
        assert!(debug.contains("\"error\" = "));
    }

    /// Issue #804: reaching the shared pending-requeue path PROVES the claiming
    /// worker was capable (it resolved a handler and ran it), so the
    /// consecutive-capability-miss counter must reset to 0 there — exactly the
    /// "consecutive" semantics `crash_strikes` already has on the same
    /// changeset.
    ///
    /// Without the reset the counter would accumulate across unrelated,
    /// perfectly healthy retries and eventually escalate a task with
    /// `no_capable_worker:` even though every claim after the first found a
    /// handler — the false-escalation failure mode.
    #[test]
    fn pending_requeue_changeset_resets_the_capability_miss_counter() {
        use crate::schema::harvest_task_queue::dsl;
        use diesel::debug_query;
        use diesel::pg::Pg;

        let changeset =
            PendingRequeueChangeset::new(chrono::Utc::now(), "some retryable error".to_string());

        // The VALUE is the whole point: a non-zero reset would silently make the
        // counter cumulative and escalate healthy runs on a later deploy. A
        // column-presence assertion alone survives a `0 -> 7` mutation.
        assert_eq!(
            changeset.capability_misses, 0,
            "the shared pending-requeue changeset must reset capability_misses to 0 \
             (a capable worker ran the handler)"
        );
        assert_eq!(changeset.crash_strikes, 0);

        let query = diesel::update(dsl::harvest_task_queue.filter(dsl::state.eq("RUNNING")))
            .set(&changeset);
        let debug = debug_query::<Pg, _>(&query).to_string();

        assert!(
            debug.contains("\"capability_misses\" = "),
            "the reset must reach the generated SQL, not just the struct: {debug}"
        );
    }

    /// Issue #804: the clean-continuation path (`reschedule_task`, and
    /// `defer_rate_limited_task` layered on top of it) also proves capability —
    /// the worker resolved a handler and either suspended cleanly or reached
    /// the dispatch-time rate-limit gate. Both must reset the counter.
    ///
    /// The false-escalation this guards: worker A (no handler) releases
    /// (misses = 1), worker B (capable) claims but defers on a rate limit, A
    /// claims again (misses = 2)… After `capability_miss_max_redeliveries`
    /// such interleavings the task escalates with `no_capable_worker:` even
    /// though a capable worker claimed it on every other poll.
    #[test]
    fn clean_continuation_changeset_resets_crash_and_capability_counters() {
        use crate::schema::harvest_task_queue::dsl;
        use diesel::debug_query;
        use diesel::pg::Pg;

        let changeset = CleanContinuationChangeset::new(chrono::Utc::now());

        // Assert the VALUES, not just that the columns appear: a `0 -> n`
        // mutation is invisible to a column-presence check but silently makes
        // both counters cumulative rather than consecutive.
        assert_eq!(
            changeset.capability_misses, 0,
            "capability-miss streak must reset to 0 on a clean continuation (issue #804)"
        );
        assert_eq!(
            changeset.crash_strikes, 0,
            "poison-pill streak must reset to 0 on a clean continuation (issue #367)"
        );

        let query = diesel::update(dsl::harvest_task_queue.filter(dsl::state.eq("RUNNING")))
            .set(&changeset);
        let debug = debug_query::<Pg, _>(&query).to_string();

        for column in ["worker_id", "started_at", "last_heartbeat_at"] {
            assert!(
                debug.contains(&format!("\"{column}\" = $")),
                "{column} must be nulled on a clean continuation: {debug}"
            );
        }
        assert!(debug.contains("\"state\" = "), "{debug}");
        assert!(debug.contains("\"scheduled_at\" = "), "{debug}");
        assert!(
            debug.contains("\"crash_strikes\" = "),
            "poison-pill streak must reset on a clean continuation (issue #367): {debug}"
        );
        assert!(
            debug.contains("\"capability_misses\" = "),
            "capability-miss streak must reset on a clean continuation \
             (issue #804): {debug}"
        );
    }

    /// Issue #782: `requeue_workflow_task_after_panic` must generate a `SET`
    /// clause that (a) resets the shared pending-requeue columns (`state` →
    /// PENDING, `crash_strikes` bound so the poison-pill reclaimer never trips,
    /// `worker_id`/`started_at`/`last_heartbeat_at` nulled) and (b) clears the
    /// sticky affinity, `wake_requested`, and stale `activity_name` columns —
    /// mirroring the ND-block re-pend so a panicking workflow task is deferred
    /// purely by `scheduled_at` and re-claimable by any worker.
    #[test]
    fn requeue_after_panic_query_resets_and_unpins_the_task_row() {
        let changeset =
            PendingRequeueChangeset::new(chrono::Utc::now(), "handler panic: boom".to_string());
        let sql = requeue_after_panic_query(changeset);

        // Every column is emitted as a bound parameter (`= $N`) by
        // `debug_query`, mirroring the sibling `pending_requeue_changeset`
        // shape test — the sticky/activity_name columns bind `None` (SQL NULL)
        // rather than rendering a literal `NULL`.
        for column in [
            "state",
            "crash_strikes",
            "scheduled_at",
            "worker_id",
            "started_at",
            "last_heartbeat_at",
            "sticky_worker_id",
            "sticky_until",
            "sticky_timeout",
            "wake_requested",
            "activity_name",
            "error",
        ] {
            assert!(
                sql.contains(&format!("\"{column}\" = $")),
                "{column} must appear as a bound column in the SET clause: {sql}"
            );
        }
        // The null-ing columns (worker_id/started_at/last_heartbeat_at +
        // sticky_worker_id/sticky_until/sticky_timeout + activity_name) all bind
        // `None` (SQL NULL), and wake_requested binds `false`.
        assert!(
            sql.matches("None").count() >= 7,
            "the seven null-ing columns must all bind to None (SQL NULL): {sql}"
        );
        assert!(
            sql.contains("false"),
            "wake_requested must bind to false: {sql}"
        );
        // Restricted to claimed (RUNNING) workflow rows.
        assert!(sql.contains("\"task_type\""), "{sql}");
        assert!(sql.contains("\"state\""), "{sql}");
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
