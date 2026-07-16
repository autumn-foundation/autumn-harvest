//! Diesel model structs mapping to harvest_* tables.
//!
//! Each table has two variants:
//! - Full struct (`Queryable` + `Selectable`) for reads
//! - `New*` struct (`Insertable`) for inserts — only the fields we supply,
//!   letting Postgres fill in defaults.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use uuid::Uuid;

use crate::schema::{
    harvest_admission_gates, harvest_audit_log, harvest_backfill_log, harvest_batch_jobs,
    harvest_build_compat, harvest_build_policies, harvest_calendar_exclusions, harvest_calendars,
    harvest_completion_deliveries, harvest_completion_trigger_fires,
    harvest_completion_trigger_outbox, harvest_completion_triggers, harvest_dead_letters,
    harvest_events, harvest_execution_summaries, harvest_external_tasks, harvest_payload_refs,
    harvest_rate_limit_buckets, harvest_schedule_decisions, harvest_schedules, harvest_sessions,
    harvest_signals, harvest_task_queue, harvest_timers, harvest_wasm_modules, harvest_workers,
    harvest_workflow_executions,
};

// ── Calendar ──────────────────────────────────────────────────────────────────

/// A named calendar row from `harvest_calendars`.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_calendars)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestCalendar {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    /// `true` for built-in calendars that operators cannot delete.
    pub built_in: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Insert struct for creating a new calendar.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_calendars)]
pub struct NewHarvestCalendar<'a> {
    pub id: Uuid,
    pub name: &'a str,
    pub description: Option<&'a str>,
    pub built_in: bool,
}

/// A single date exclusion row from `harvest_calendar_exclusions`.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_calendar_exclusions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestCalendarExclusion {
    pub id: Uuid,
    pub calendar_name: String,
    pub excluded_date: chrono::NaiveDate,
    pub created_at: DateTime<Utc>,
}

/// Insert struct for adding a date to a calendar's exclusion set.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_calendar_exclusions)]
pub struct NewHarvestCalendarExclusion<'a> {
    pub calendar_name: &'a str,
    pub excluded_date: chrono::NaiveDate,
}

// ── WorkflowExecution ─────────────────────────────────────────────────────────

/// A single workflow execution instance (one row per run).
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_workflow_executions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct WorkflowExecution {
    pub id: Uuid,
    pub workflow_name: String,
    pub workflow_id: String,
    pub run_id: Uuid,
    pub shard_id: i32,
    pub state: String,
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub parent_id: Option<Uuid>,
    pub sticky_worker_id: Option<String>,
    pub queue_name: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub execution_timeout: Option<chrono::Duration>,
    /// Absolute UTC deadline for execution-level timeout (issue #243).
    /// Computed at start as `started_at + execution_timeout`. NULL = no deadline.
    pub deadline_at: Option<DateTime<Utc>>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    /// Build ID assigned at workflow start time (issue #171). `None` = pre-policy.
    pub assigned_build_id: Option<String>,
    /// Parent-close policy for detached children (issue #347). `None` = awaited.
    pub parent_close_policy: Option<String>,
    pub owner: Option<String>,
    pub runbook_url: Option<String>,
    pub severity: Option<String>,
    /// Wall-clock instant the active pause was applied (issue #383). `None` = not paused.
    pub paused_at: Option<DateTime<Utc>>,
    /// Optional operator-supplied pause reason (issue #383).
    pub pause_reason: Option<String>,
    /// Identity that requested the active pause (issue #383).
    pub pause_actor: Option<String>,
    /// Last-write-wins human-readable status string set by the workflow author
    /// via `ctx.set_current_details(...)` (issue #473). `None` = never set.
    pub current_details: Option<String>,
    /// Ambient string key-value context attached at start and propagated to all
    /// activities and child workflows (issue #481). `None` = empty map.
    /// Skipped in serde output: raw header values may contain auth tokens or
    /// tenant secrets and must not be exposed via management API responses.
    #[serde(skip)]
    pub context_headers: Option<serde_json::Value>,
    /// Optional declared SLA budget for soft breach signal (issue #487).
    /// Stored so continue-as-new / reset can re-anchor per run. `None` = no SLA.
    pub sla: Option<chrono::Duration>,
    /// Absolute UTC deadline for soft SLA enforcement (issue #487).
    /// Computed at start as `started_at + sla`. `None` = no SLA declared.
    pub sla_deadline_at: Option<DateTime<Utc>>,
    /// Whether the soft SLA deadline has been breached (issue #487).
    /// Set exactly once by the scanner; never by the workflow engine.
    pub sla_breached: bool,
    /// Wall-clock instant the SLA was first detected as breached (issue #487).
    /// `None` when `sla_breached = false`.
    pub sla_breached_at: Option<DateTime<Utc>>,
    /// Schedule that fired this execution (issue #488). `None` for manual starts.
    pub schedule_id: Option<Uuid>,
    /// Logical schedule slot this run fires for (issue #488); carryover is ordered by
    /// this, not `completed_at`. `None` for manual starts.
    pub scheduled_for: Option<DateTime<Utc>>,
    /// Attempt number of this execution in its retry chain (issue #523). 1 = first attempt.
    pub workflow_attempt: i32,
    /// Frozen effective retry policy for this execution (issue #523). NULL = no auto-retry.
    pub workflow_retry_policy: Option<serde_json::Value>,
    /// ID of the previous failed execution in this retry chain (issue #523). NULL for first attempt.
    pub retry_of_exec_id: Option<Uuid>,
    /// Dispatch origin (issue #534): `scheduled` / `backfill` / `manual_trigger` for
    /// schedule-attributed runs, `None` for all non-scheduled runs. Metadata only.
    pub origin: Option<String>,
    /// Wall-clock of the most recent replay-non-determinism block observation
    /// (issue #603). `None` = not blocked; state stays RUNNING while blocked.
    pub nd_blocked_at: Option<DateTime<Utc>>,
    /// Divergence error string from the most recent blocked cycle (issue #603).
    pub nd_block_reason: Option<String>,
    /// Consecutive block entries for the current divergence incident (issue
    /// #603). Drives the re-dispatch backoff; reset to 0 on a clean cycle.
    pub nd_block_count: i32,
    /// Per-execution completion-callback targets (issue #605): a JSON array
    /// of `{url, filter}` objects. `None` = no per-execution targets; the
    /// effective set is still the union with any builder-wide defaults.
    pub completion_callbacks: Option<serde_json::Value>,
    /// Predecessor execution in a continue-as-new chain (issue #701). `None` for
    /// the first run in a chain and for all non-continued runs.
    pub continued_from_exec_id: Option<Uuid>,
    /// The first (origin) execution of this continue-as-new chain (issue #701).
    /// Set on every successor to the chain head. `None` for the origin run and
    /// all non-continued runs.
    pub first_exec_id: Option<Uuid>,
    /// Wall-clock the per-execution legal hold was placed (issue #747).
    /// `None` = no hold. A hold is ACTIVE when this is non-`None` and
    /// `legal_hold_until` is `None` or in the future; an active hold exempts
    /// this execution's history from retention deletion and PII erasure.
    pub legal_hold_set_at: Option<DateTime<Utc>>,
    /// Optional auto-expiry for the legal hold (issue #747). `None` =
    /// indefinite; a past value means the hold is inactive again.
    pub legal_hold_until: Option<DateTime<Utc>>,
    /// Operator-supplied justification for the legal hold (issue #747).
    pub legal_hold_reason: Option<String>,
    /// Principal that placed the legal hold (issue #747, audit trail).
    pub legal_hold_actor: Option<String>,
    /// Workflow-start provenance classifier (issue #740): a bounded snake_case
    /// source string. `None` for pre-upgrade rows, reported as "unknown" in the
    /// serialized form. Distinct from `origin` (issue #534); never read on replay.
    #[serde(serialize_with = "serialize_start_source_or_unknown")]
    pub start_source: Option<String>,
    /// Optional correlation reference for the start source (issue #740), e.g. the
    /// triggering execution id or schedule id. `None` when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_source_ref: Option<String>,
    /// Optional human/operator attribution for the start (issue #740). `None`
    /// when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_by: Option<String>,
}

/// Serialize a nullable `start_source` column, reporting a `None` (pre-upgrade /
/// unclassified) row as the literal `"unknown"` string (issue #740, AC4).
fn serialize_start_source_or_unknown<S>(
    value: &Option<String>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(value.as_deref().unwrap_or("unknown"))
}

/// Insert struct for creating a new workflow execution.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_workflow_executions)]
pub struct NewWorkflowExecution<'a> {
    pub id: Uuid,
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub run_id: Uuid,
    pub shard_id: i32,
    pub input: serde_json::Value,
    pub parent_id: Option<Uuid>,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    /// Absolute UTC deadline for execution-level timeout (issue #243).
    /// NULL = no deadline enforced.
    pub deadline_at: Option<DateTime<Utc>>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    /// Build ID from the active build policy for this queue at start time.
    pub assigned_build_id: Option<String>,
    /// Parent-close policy for detached children (issue #347). `None` = awaited.
    pub parent_close_policy: Option<String>,
    pub owner: Option<&'a str>,
    pub runbook_url: Option<&'a str>,
    pub severity: Option<&'a str>,
    /// Ambient context headers (issue #481). `None` = no headers.
    pub context_headers: Option<serde_json::Value>,
    /// Optional declared SLA budget (issue #487). NULL = no SLA.
    pub sla: Option<chrono::Duration>,
    /// Absolute UTC SLA deadline (issue #487). NULL = no SLA.
    pub sla_deadline_at: Option<DateTime<Utc>>,
    /// Schedule that fired this execution (issue #488). `None` for manual starts.
    pub schedule_id: Option<Uuid>,
    /// Logical schedule slot this run fires for (issue #488); carryover is ordered by
    /// this, not `completed_at`. `None` for manual starts.
    pub scheduled_for: Option<DateTime<Utc>>,
    /// Attempt number of this execution in its retry chain (issue #523). 1 = first attempt.
    pub workflow_attempt: i32,
    /// Frozen effective retry policy for this execution (issue #523). NULL = no auto-retry.
    pub workflow_retry_policy: Option<serde_json::Value>,
    /// ID of the previous failed execution in this retry chain (issue #523). NULL for first attempt.
    pub retry_of_exec_id: Option<Uuid>,
    /// Dispatch origin (issue #534): `scheduled` / `backfill` / `manual_trigger` for
    /// schedule-attributed runs, `None` for all non-scheduled runs. Metadata only.
    pub origin: Option<&'a str>,
    /// Per-execution completion-callback targets (issue #605). `None` = none configured.
    pub completion_callbacks: Option<serde_json::Value>,
    /// Predecessor execution in a continue-as-new chain (issue #701). `None`
    /// except when inserting a continue-as-new successor.
    pub continued_from_exec_id: Option<Uuid>,
    /// First (origin) execution of this continue-as-new chain (issue #701).
    /// `None` except when inserting a continue-as-new successor.
    pub first_exec_id: Option<Uuid>,
    /// Workflow-start provenance classifier (issue #740): a bounded snake_case
    /// source string. `None` = unclassified. Distinct from `origin` (issue #534).
    pub start_source: Option<&'a str>,
    /// Optional correlation reference for the start source (issue #740). `None`
    /// when absent.
    pub start_source_ref: Option<&'a str>,
    /// Optional human/operator attribution for the start (issue #740). `None`
    /// when absent.
    pub started_by: Option<&'a str>,
}

// ── HarvestEvent ──────────────────────────────────────────────────────────────

/// A single event in the workflow execution history (append-only).
#[derive(
    Debug,
    Clone,
    Queryable,
    QueryableByName,
    Selectable,
    Identifiable,
    serde::Serialize,
    serde::Deserialize,
)]
#[diesel(table_name = harvest_events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestEvent {
    pub id: i64,
    pub workflow_exec_id: Uuid,
    pub event_id: i32,
    pub event_type: String,
    pub event_data: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

/// Insert struct for appending a new event to a workflow's history.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_events)]
pub struct NewHarvestEvent<'a> {
    pub workflow_exec_id: Uuid,
    pub event_id: i32,
    pub event_type: &'a str,
    pub event_data: serde_json::Value,
}

// ── PayloadRef ────────────────────────────────────────────────────────────────

/// A reference from a workflow execution to an offloaded payload blob (issue #524).
#[derive(Debug, Clone, Queryable, Selectable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_payload_refs)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestPayloadRef {
    pub blob_key: String,
    pub workflow_exec_id: Uuid,
    pub store_id: String,
    pub byte_len: i64,
    pub created_at: DateTime<Utc>,
}

/// Insert struct for recording a new offloaded-payload reference.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_payload_refs)]
pub struct NewHarvestPayloadRef {
    pub blob_key: String,
    pub workflow_exec_id: Uuid,
    pub store_id: String,
    pub byte_len: i64,
}

// ── TaskQueueItem ─────────────────────────────────────────────────────────────

/// A pending or in-progress task in the work queue.
#[derive(
    Debug,
    Clone,
    Queryable,
    QueryableByName,
    Selectable,
    Identifiable,
    serde::Serialize,
    serde::Deserialize,
)]
#[diesel(table_name = harvest_task_queue)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskQueueItem {
    pub id: Uuid,
    pub queue_name: String,
    pub task_type: String,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<String>,
    pub activity_id: Option<Uuid>,
    pub input: serde_json::Value,
    pub state: String,
    pub priority: i32,
    pub worker_id: Option<String>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub heartbeat_details: Option<serde_json::Value>,
    pub heartbeat_timeout: Option<chrono::Duration>,
    pub start_to_close: Option<chrono::Duration>,
    pub schedule_to_start: Option<chrono::Duration>,
    pub retry_policy: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub sticky_worker_id: Option<String>,
    pub sticky_until: Option<DateTime<Utc>>,
    pub sticky_timeout: Option<chrono::Duration>,
    pub trace_context: Option<serde_json::Value>,
    /// Cluster-wide concurrency group key. NULL = no cap enforced.
    pub concurrency_key: Option<String>,
    /// Maximum concurrent RUNNING tasks allowed for this `concurrency_key`.
    pub concurrency_cap: Option<i32>,
    /// Build ID required to claim this task (issue #171). NULL = any worker.
    pub required_build_id: Option<String>,
    /// Optional rate limit key to throttle execution throughput.
    pub rate_limit_key: Option<String>,
    /// Consecutive worker crashes attributed to this task (issue #367).
    ///
    /// Incremented each time the orphan-reclaim scanner reclaims this row from
    /// a dead worker without it having completed. When it reaches the
    /// configured poison-pill threshold the task is quarantined to the DLQ.
    pub crash_strikes: i32,
    /// Absolute UTC deadline for the total activity lifetime across all retry
    /// attempts (issue #378). Set once at initial enqueue; never refreshed on
    /// retry. NULL = no total deadline (unbounded retries).
    pub schedule_to_close_at: Option<DateTime<Utc>>,
    /// Structured capability requirements JSONB payload (issue #382).
    pub required_capabilities: Option<serde_json::Value>,
    /// Ambient context headers propagated from the parent workflow (issue #481).
    #[serde(skip)]
    pub context_headers: Option<serde_json::Value>,
    /// Row insert time (issue #501 review). `None` for pre-upgrade rows; the
    /// schedule-to-start SLI uses `GREATEST(scheduled_at, created_at)` as the true
    /// eligibility (an immediate task's backdated `scheduled_at` is corrected to its
    /// insert time, a delayed/retried task keeps its explicit future `scheduled_at`)
    /// and falls back to `scheduled_at` when this is `None`.
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    /// Durable "please wake this task" flag (dropped-wake fix, issue #601 CI
    /// hardening). See `wake_workflow_task`/`park_workflow_task` in `queue.rs`.
    #[serde(default)]
    pub wake_requested: bool,
    /// Worker session this activity belongs to (issue #606). `None` for an
    /// ordinary activity dispatch.
    #[serde(default)]
    pub session_id: Option<Uuid>,
}

/// Insert struct for enqueuing a new task.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_task_queue)]
pub struct NewTaskQueueItem<'a> {
    pub id: Uuid,
    pub queue_name: &'a str,
    pub task_type: &'a str,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<&'a str>,
    pub activity_id: Option<Uuid>,
    pub input: serde_json::Value,
    pub priority: i32,
    pub max_attempts: i32,
    pub scheduled_at: DateTime<Utc>,
    pub heartbeat_timeout: Option<chrono::Duration>,
    pub start_to_close: Option<chrono::Duration>,
    pub schedule_to_start: Option<chrono::Duration>,
    pub retry_policy: Option<serde_json::Value>,
    pub heartbeat_details: Option<serde_json::Value>,
    pub sticky_worker_id: Option<&'a str>,
    pub sticky_until: Option<DateTime<Utc>>,
    pub sticky_timeout: Option<chrono::Duration>,
    pub trace_context: Option<serde_json::Value>,
    pub concurrency_key: Option<&'a str>,
    pub concurrency_cap: Option<i32>,
    /// Build ID required to claim this task. `None` = any worker may claim.
    pub required_build_id: Option<&'a str>,
    /// Optional rate limit key to throttle execution throughput.
    pub rate_limit_key: Option<&'a str>,
    /// Absolute UTC deadline for the total activity lifetime (issue #378).
    /// NULL = no total deadline enforced.
    pub schedule_to_close_at: Option<DateTime<Utc>>,
    /// Structured capability requirements JSONB payload (issue #382).
    pub required_capabilities: Option<serde_json::Value>,
    /// Ambient context headers propagated from the parent workflow (issue #481).
    pub context_headers: Option<serde_json::Value>,
    /// Worker session this activity belongs to (issue #606). `None` for an
    /// ordinary activity dispatch.
    pub session_id: Option<Uuid>,
}

/// Database representation of a rate limit bucket.
#[derive(
    Debug,
    Clone,
    Queryable,
    Selectable,
    Identifiable,
    Insertable,
    AsChangeset,
    serde::Serialize,
    serde::Deserialize,
)]
#[diesel(table_name = harvest_rate_limit_buckets)]
#[diesel(primary_key(key))]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct RateLimitBucket {
    pub key: String,
    pub refill_rate: f64,
    pub burst: f64,
    pub tokens: f64,
    pub last_refilled_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Insert struct for a rate limit bucket.
#[derive(Debug, Insertable, AsChangeset, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_rate_limit_buckets)]
pub struct NewRateLimitBucket<'a> {
    pub key: &'a str,
    pub refill_rate: f64,
    pub burst: f64,
    pub tokens: f64,
    pub last_refilled_at: DateTime<Utc>,
}

// ── Schedule ──────────────────────────────────────────────────────────────────

/// A DAG or workflow schedule configuration.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_schedules)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestSchedule {
    pub id: Uuid,
    /// Set for DAG schedules, NULL for workflow-only schedules.
    pub dag_name: Option<String>,
    pub schedule_expr: Option<String>,
    pub timezone: String,
    pub catchup: bool,
    pub max_active_runs: i32,
    pub is_paused: bool,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Set for workflow-only schedules, NULL for DAG schedules.
    pub workflow_name: Option<String>,
    /// Input JSON passed to each scheduled workflow run.
    pub workflow_input: Option<serde_json::Value>,
    /// Task queue for workflow dispatches. NULL for DAG schedule rows.
    pub queue_name: Option<String>,
    /// When the schedule was paused. NULL when the schedule is active.
    pub paused_at: Option<DateTime<Utc>>,
    /// Actor identity that issued the most recent pause. NULL when active.
    pub paused_by: Option<String>,
    /// Free-text reason recorded with the most recent pause. NULL when active or no reason given.
    pub pause_reason: Option<String>,
    /// Maximum jitter window in seconds. 0 = no jitter (default).
    pub jitter_secs: i64,
    /// Overlap policy variant stored as a `snake_case` string (issue #241).
    pub overlap_policy: String,
    /// Durable buffered fire times for `BufferOne`/`BufferAll` (issue #241).
    pub buffered_runs: serde_json::Value,
    /// Maximum buffered slots under `BufferAll` (issue #241). Default 100.
    pub buffer_all_max: i32,
    /// Optional named calendar for this schedule (issue #337). NULL = no filtering.
    pub calendar_name: Option<String>,
    /// What to do when the fire date is calendar-excluded (issue #337).
    pub skip_policy: String,
    /// Token set by the replica that atomically claimed this slot for firing (issue #350).
    /// NULL = no claim held. Cleared when `next_run_at` is advanced after a successful fire.
    pub fire_claim_token: Option<Uuid>,
    /// UTC expiry for the current claim (issue #350). When past, any replica may re-claim
    /// (crash-recovery path). NULL when `fire_claim_token` is NULL.
    pub fire_claimed_until: Option<DateTime<Utc>>,
    /// Auto-pause threshold (issue #360). NULL = auto-pause disabled.
    pub consecutive_failure_limit: Option<i32>,
    /// Running count of consecutive schedule-triggered execution failures (issue #360).
    pub consecutive_failure_count: i32,
    /// Set to the timestamp when the schedule was auto-paused (issue #360). NULL = not auto-paused.
    pub auto_paused_at: Option<DateTime<Utc>>,
    /// Absolute UTC cutoff for this schedule (issue #478). NULL = no cutoff.
    pub end_at: Option<DateTime<Utc>>,
    /// Total run budget (issue #478). NULL = unlimited.
    pub max_runs: Option<i32>,
    /// Count of executions actually started by this schedule (issue #478).
    pub runs_started: i32,
    /// Set when the schedule transitions to the exhausted state (issue #478). NULL = still active.
    pub exhausted_at: Option<DateTime<Utc>>,
    /// Machine-readable reason for exhaustion: `"end_at_reached"` or `"max_runs_exhausted"` (issue #478).
    pub exhausted_reason: Option<String>,
    /// Catchup policy discriminator for missed fire slots (issue #484).
    /// One of `"skip_all"`, `"most_recent"`, `"window"`, `"unbounded"`. NULL = legacy bool fallback.
    pub catchup_policy: Option<String>,
    /// Window duration in seconds for the `"window"` catchup policy (issue #484). NULL = unused.
    pub catchup_window_secs: Option<i64>,
    /// Number of missed slots dropped on the most recent recovery tick (issue #484).
    pub last_catchup_dropped: i32,
    /// Timestamp of the most recent recovery tick that produced drops (issue #484). NULL = none yet.
    pub last_catchup_at: Option<DateTime<Utc>>,
    /// Default retry policy for runs started by this schedule (issue #523). NULL = no retry.
    pub retry_policy: Option<serde_json::Value>,
}

/// Insert struct for registering a new schedule (DAG or workflow).
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_schedules)]
pub struct NewHarvestSchedule<'a> {
    pub id: Uuid,
    /// Set for DAG schedules, None for workflow-only schedules.
    pub dag_name: Option<&'a str>,
    pub schedule_expr: Option<&'a str>,
    pub timezone: &'a str,
    pub catchup: bool,
    pub max_active_runs: i32,
    pub is_paused: bool,
    /// Set for workflow-only schedules, None for DAG schedules.
    pub workflow_name: Option<&'a str>,
    pub workflow_input: Option<serde_json::Value>,
    /// Task queue for workflow dispatches. None for DAG schedule rows.
    pub queue_name: Option<&'a str>,
    /// Maximum jitter window in seconds. 0 = no jitter.
    pub jitter_secs: i64,
    /// Overlap policy for this schedule (issue #241).
    pub overlap_policy: &'a str,
    /// Durable buffered fire times JSON array (issue #241).
    pub buffered_runs: serde_json::Value,
    /// Maximum buffered slots under `BufferAll` (issue #241).
    pub buffer_all_max: i32,
    /// Optional named calendar for this schedule (issue #337).
    pub calendar_name: Option<&'a str>,
    /// What to do when the fire date is calendar-excluded (issue #337).
    pub skip_policy: &'a str,
}

// ── Signal ────────────────────────────────────────────────────────────────────

/// A pending signal queued for delivery to a workflow execution.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_signals)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestSignal {
    pub id: Uuid,
    pub workflow_exec_id: Uuid,
    pub signal_name: String,
    pub payload: serde_json::Value,
    pub received_at: DateTime<Utc>,
    pub consumed: bool,
    pub idempotency_key: Option<String>,
}

/// Insert struct for queuing a new signal.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_signals)]
pub struct NewHarvestSignal<'a> {
    pub workflow_exec_id: Uuid,
    pub signal_name: &'a str,
    pub payload: serde_json::Value,
    pub idempotency_key: Option<&'a str>,
}

// ── Timer ─────────────────────────────────────────────────────────────────────

/// A durable timer registered by a workflow execution.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_timers)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestTimer {
    pub id: Uuid,
    pub workflow_exec_id: Uuid,
    pub timer_id: String,
    pub fires_at: DateTime<Utc>,
    pub fired: bool,
}

/// Insert struct for registering a new timer.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_timers)]
pub struct NewHarvestTimer<'a> {
    pub workflow_exec_id: Uuid,
    pub timer_id: &'a str,
    pub fires_at: DateTime<Utc>,
}

// ── DeadLetter ────────────────────────────────────────────────────────────────

/// A task that exhausted all retry attempts and was moved to the dead-letter queue.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_dead_letters)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DeadLetter {
    pub id: Uuid,
    pub original_task_id: Uuid,
    pub queue_name: String,
    pub task_type: String,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<String>,
    pub input: serde_json::Value,
    pub error: String,
    pub attempts: i32,
    pub failed_at: DateTime<Utc>,
    pub owner: Option<String>,
    pub severity: Option<String>,
}

/// Insert struct for moving a failed task to the dead-letter queue.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_dead_letters)]
pub struct NewDeadLetter<'a> {
    pub original_task_id: Uuid,
    pub queue_name: &'a str,
    pub task_type: &'a str,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<&'a str>,
    pub input: serde_json::Value,
    pub error: &'a str,
    pub attempts: i32,
    pub owner: Option<&'a str>,
    pub severity: Option<&'a str>,
}

// ── ExternalTask ──────────────────────────────────────────────────────────────

/// A pending or resolved external activity task, keyed by its opaque token.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_external_tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ExternalTask {
    pub id: Uuid,
    pub token: Uuid,
    pub workflow_exec_id: Uuid,
    pub activity_id: Uuid,
    pub name: String,
    pub queue: String,
    pub state: String,
    pub schedule_to_close_at: DateTime<Utc>,
    pub schedule_to_close_secs: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Insert struct for registering a new external activity task.
#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_external_tasks)]
pub struct NewExternalTask<'a> {
    pub token: Uuid,
    pub workflow_exec_id: Uuid,
    pub activity_id: Uuid,
    pub name: &'a str,
    pub queue: &'a str,
    pub schedule_to_close_at: DateTime<Utc>,
    pub schedule_to_close_secs: i64,
}

// ── HarvestWorker ─────────────────────────────────────────────────────────────

/// A live or recently-stopped worker process registered in the fleet table.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_workers)]
#[diesel(primary_key(worker_id))]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestWorker {
    pub worker_id: String,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat_at: DateTime<Utc>,
    /// JSON array of queue names polled by this worker.
    pub queues: serde_json::Value,
    /// JSON array of `ShardId` integers assigned to this worker.
    pub shard_assignments: serde_json::Value,
    /// `max_concurrent_workflows + max_concurrent_activities`.
    pub max_concurrency: i32,
    /// Currently executing tasks (refreshed on every heartbeat).
    pub in_flight_count: i32,
    pub host: String,
    pub version: Option<String>,
    /// Lifecycle status: `Active`, `Draining`, or `Stopped`.
    pub status: String,
    /// When set, the deadline by which this worker must have finished draining.
    pub drain_deadline_at: Option<DateTime<Utc>>,
    /// Immutable build identifier advertised by this worker (issue #171).
    pub build_id: String,
    /// Optional human-readable deployment name (issue #171).
    pub deployment_name: Option<String>,
    /// Capability labels for hardware-aware and regional routing (issue #382).
    pub labels: serde_json::Value,
    /// Advertised worker-session capacity (issue #606). `0` = sessions
    /// disabled on this worker (the default).
    pub max_concurrent_sessions: i32,
    /// Sessions currently `ACTIVE` on this worker (issue #606).
    pub in_use_sessions: i32,
}

/// Insert struct for registering a new worker process.
#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_workers)]
pub struct NewHarvestWorker<'a> {
    pub worker_id: &'a str,
    pub queues: serde_json::Value,
    pub shard_assignments: serde_json::Value,
    pub max_concurrency: i32,
    pub host: &'a str,
    pub version: Option<&'a str>,
    /// Build ID advertised by this worker (empty string = legacy/unset).
    pub build_id: &'a str,
    /// Optional deployment name for operator observability.
    pub deployment_name: Option<&'a str>,
    /// Capability labels for hardware-aware and regional routing (issue #382).
    pub labels: serde_json::Value,
    /// Advertised worker-session capacity (issue #606). `0` = sessions
    /// disabled on this worker (the default).
    pub max_concurrent_sessions: i32,
}

// ── BatchJob ──────────────────────────────────────────────────────────────────

/// A submitted batch operation row from `harvest_batch_jobs` (issue #102).
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_batch_jobs)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BatchJob {
    pub id: Uuid,
    pub action: String,
    pub filter: serde_json::Value,
    pub signal_name: Option<String>,
    pub signal_payload: Option<serde_json::Value>,
    pub status: String,
    pub total: i64,
    pub completed: i64,
    pub failed: i64,
    pub errors: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
    pub created_by: Option<String>,
    pub processed_ids: serde_json::Value,
}

/// Insert struct for submitting a new batch job.
#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_batch_jobs)]
pub struct NewBatchJob<'a> {
    pub id: Uuid,
    pub action: &'a str,
    pub filter: serde_json::Value,
    pub signal_name: Option<&'a str>,
    pub signal_payload: Option<serde_json::Value>,
    pub status: &'a str,
    pub idempotency_key: Option<&'a str>,
    pub created_by: Option<&'a str>,
}

// ── AuditRecord ───────────────────────────────────────────────────────────────

/// A single management API audit record (issue #158).
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_audit_log)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct AuditRecord {
    pub id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub actor: String,
    pub operation: String,
    pub target_type: String,
    pub target_id: Option<String>,
    pub route_or_command: String,
    pub request_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub status: String,
    pub error_summary: Option<String>,
    pub shard_id: Option<i32>,
    pub source: String,
}

/// Insert struct for recording a new audit event.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_audit_log)]
pub struct NewAuditRecord<'a> {
    pub actor: &'a str,
    pub operation: &'a str,
    pub target_type: &'a str,
    pub target_id: Option<&'a str>,
    pub route_or_command: &'a str,
    pub request_id: Option<&'a str>,
    pub idempotency_key: Option<&'a str>,
    pub status: &'a str,
    pub error_summary: Option<&'a str>,
    pub shard_id: Option<i32>,
    pub source: &'a str,
}

// ── BuildPolicy ───────────────────────────────────────────────────────────────

/// Active build policy for a task queue (issue #171).
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_build_policies)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestBuildPolicy {
    pub id: Uuid,
    pub queue_name: String,
    pub build_id: String,
    pub deployment_name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Ramp target build (issue #604). `None` = no ramp configured.
    pub target_build_id: Option<String>,
    /// Ramp percentage 0..=100 (issue #604). `None` = no ramp configured.
    pub ramp_percent: Option<i32>,
}

/// Insert struct for a new build policy.
#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_build_policies)]
pub struct NewHarvestBuildPolicy<'a> {
    pub id: Uuid,
    pub queue_name: &'a str,
    pub build_id: &'a str,
    pub deployment_name: Option<&'a str>,
}

// ── BuildCompatEntry ──────────────────────────────────────────────────────────

/// A build compatibility declaration (issue #171).
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_build_compat)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestBuildCompat {
    pub id: Uuid,
    pub build_id: String,
    pub compatible_with: String,
    pub declared_at: DateTime<Utc>,
}

/// Insert struct for a new compatibility declaration.
#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_build_compat)]
pub struct NewHarvestBuildCompat<'a> {
    pub id: Uuid,
    pub build_id: &'a str,
    pub compatible_with: &'a str,
}

// ── BackfillLog ───────────────────────────────────────────────────────────────

/// One row in `harvest_backfill_log`: durable record of a single backfill request.
#[derive(Debug, Clone, Queryable, Selectable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_backfill_log)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BackfillLogRow {
    pub id: Uuid,
    pub schedule_id: Uuid,
    pub actor: String,
    pub source: String,
    pub from_ts: DateTime<Utc>,
    pub to_ts: DateTime<Utc>,
    pub dry_run: bool,
    pub total: i32,
    pub dispatched: i32,
    pub skipped: i32,
    pub failed: i32,
    pub status: String,
    pub error_summary: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Insert struct for a new backfill log entry.
#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_backfill_log)]
pub struct NewBackfillLogRow {
    pub id: Uuid,
    pub schedule_id: Uuid,
    pub actor: String,
    pub source: String,
    pub from_ts: DateTime<Utc>,
    pub to_ts: DateTime<Utc>,
    pub dry_run: bool,
    pub total: i32,
    pub dispatched: i32,
    pub skipped: i32,
    pub failed: i32,
    pub status: String,
    pub error_summary: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

// ── Schedule Decisions ────────────────────────────────────────────────────────

/// A queryable decision record for a scheduler tick (issue #325).
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_schedule_decisions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ScheduleDecision {
    pub id: Uuid,
    pub schedule_id: Option<Uuid>,
    pub schedule_name: String,
    pub target_kind: String,
    pub decision: String,
    pub reason_code: String,
    pub detail: Option<serde_json::Value>,
    pub occurred_at: DateTime<Utc>,
    pub next_fire_at: DateTime<Utc>,
    pub shard_id: i16,
}

/// Insertable model for creating a schedule decision record.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_schedule_decisions)]
pub struct NewScheduleDecision {
    pub id: Uuid,
    pub schedule_id: Option<Uuid>,
    pub schedule_name: String,
    pub target_kind: String,
    pub decision: String,
    pub reason_code: String,
    pub detail: Option<serde_json::Value>,
    pub occurred_at: DateTime<Utc>,
    pub next_fire_at: DateTime<Utc>,
    pub shard_id: i16,
}

// ── Completion Triggers ────────────────────────────────────────────────────────

/// Queryable model representing a completion trigger.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_completion_triggers)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct CompletionTriggerDb {
    pub id: Uuid,
    pub source_workflow_name: String,
    pub terminal_states: serde_json::Value,
    pub target_workflow_name: String,
    pub input_mapping: serde_json::Value,
    pub queue_name: Option<String>,
    pub is_static: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Optional output guard AST (issue #810). NULL = unconditional.
    pub condition: Option<serde_json::Value>,
}

/// Insertable model for registering/creating a completion trigger.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_completion_triggers)]
pub struct NewCompletionTriggerDb {
    pub id: Uuid,
    pub source_workflow_name: String,
    pub terminal_states: serde_json::Value,
    pub target_workflow_name: String,
    pub input_mapping: serde_json::Value,
    pub queue_name: Option<String>,
    pub is_static: bool,
    /// Optional output guard AST (issue #810). NULL = unconditional.
    pub condition: Option<serde_json::Value>,
}

/// Queryable model representing a fired completion trigger event instance.
#[derive(Debug, Clone, Queryable, Selectable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_completion_trigger_fires)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct CompletionTriggerFireDb {
    pub source_exec_id: Uuid,
    pub trigger_id: Uuid,
    pub fired_at: DateTime<Utc>,
    /// NULL = fired; `condition_unmet` / `condition_invalid` =
    /// resolved-skipped by the output guard (issue #810).
    pub outcome: Option<String>,
}

/// Insertable model for registering a fired completion trigger.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_completion_trigger_fires)]
pub struct NewCompletionTriggerFireDb {
    pub source_exec_id: Uuid,
    pub trigger_id: Uuid,
    /// NULL = fired; Some(reason) = resolved-skipped (issue #810).
    pub outcome: Option<String>,
}

/// Queryable model representing a deferred completion trigger outbox task.
#[derive(Debug, Clone, Queryable, Selectable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_completion_trigger_outbox)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct CompletionTriggerOutboxDb {
    pub id: Uuid,
    pub source_exec_id: Uuid,
    pub trigger_id: Uuid,
    pub target_shard: i32,
    pub target_workflow_name: String,
    pub target_workflow_id: String,
    pub target_input: serde_json::Value,
    pub queue_name: Option<String>,
    pub concurrency_key: Option<String>,
    pub concurrency_limit: Option<i32>,
    pub priority: serde_json::Value,
    pub max_workflow_input_bytes: i64,
    pub created_at: DateTime<Utc>,
}

/// Insertable model for registering a deferred completion trigger outbox task.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_completion_trigger_outbox)]
pub struct NewCompletionTriggerOutboxDb {
    pub source_exec_id: Uuid,
    pub trigger_id: Uuid,
    pub target_shard: i32,
    pub target_workflow_name: String,
    pub target_workflow_id: String,
    pub target_input: serde_json::Value,
    pub queue_name: Option<String>,
    pub concurrency_key: Option<String>,
    pub concurrency_limit: Option<i32>,
    pub priority: serde_json::Value,
    pub max_workflow_input_bytes: i64,
}

// ── AdmissionGate ─────────────────────────────────────────────────────────────

/// A persisted admission gate row from `harvest_admission_gates` (issue #377).
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_admission_gates)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct AdmissionGateRow {
    pub id: Uuid,
    /// Scope kind discriminator: `"fleet"`, `"workflow_name"`, `"queue"`,
    /// `"shard_id"`, or `"owner"`.
    pub scope_kind: String,
    /// Scope value: `None` for `"fleet"`, the specific string for all others.
    pub scope_value: Option<String>,
    pub reason: String,
    pub message: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    /// `None` = no expiry; auto-ignored at/after this time.
    pub expires_at: Option<DateTime<Utc>>,
    /// `None` = still active; set on lift.
    pub lifted_at: Option<DateTime<Utc>>,
    pub lifted_by: Option<String>,
}

/// Insert struct for creating a new admission gate.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_admission_gates)]
pub struct NewAdmissionGateRow<'a> {
    pub id: Uuid,
    pub scope_kind: &'a str,
    pub scope_value: Option<&'a str>,
    pub reason: &'a str,
    pub message: Option<&'a str>,
    pub created_by: &'a str,
    pub expires_at: Option<DateTime<Utc>>,
}

// ── CompletionDelivery ───────────────────────────────────────────────────────

/// A durable completion-callback delivery row (issue #605).
///
/// One row per `(workflow_exec_id, callback_index)`, enqueued inside the
/// terminal transaction and drained by the `fire_due_completion_deliveries`
/// scanner.
#[derive(
    Debug, Clone, Queryable, Selectable, Identifiable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_completion_deliveries)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct CompletionDelivery {
    pub id: Uuid,
    pub workflow_exec_id: Uuid,
    pub shard_id: i32,
    pub callback_index: i32,
    pub workflow_name: String,
    pub workflow_id: String,
    pub target_url: String,
    pub event_filter: serde_json::Value,
    pub terminal_state: String,
    /// Frozen `CompletionEnvelope` JSON, signed and `POSTed` verbatim on every
    /// attempt, including redeliveries.
    pub payload: serde_json::Value,
    /// `PENDING` | `INFLIGHT` | `DELIVERED` | `FAILED`.
    pub state: String,
    pub attempt: i32,
    pub max_attempts: i32,
    /// Frozen `RetryPolicy` JSON at enqueue time.
    pub retry_policy: serde_json::Value,
    pub next_attempt_at: DateTime<Utc>,
    pub last_status: Option<i32>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}

/// Insert struct for enqueuing a new completion-callback delivery.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_completion_deliveries)]
pub struct NewCompletionDelivery<'a> {
    pub id: Uuid,
    pub workflow_exec_id: Uuid,
    pub shard_id: i32,
    pub callback_index: i32,
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub target_url: &'a str,
    pub event_filter: serde_json::Value,
    pub terminal_state: &'a str,
    pub payload: serde_json::Value,
    pub max_attempts: i32,
    pub retry_policy: serde_json::Value,
    pub next_attempt_at: DateTime<Utc>,
}

/// Changeset applied by the scanner after claiming a batch of due rows
/// (transitions `PENDING`/`INFLIGHT` -> `INFLIGHT`, bumps `attempt`, and
/// sets a short in-flight lease on `next_attempt_at`).
#[derive(Debug, AsChangeset)]
#[diesel(table_name = harvest_completion_deliveries)]
pub struct CompletionDeliveryClaim {
    pub state: String,
    pub attempt: i32,
    pub next_attempt_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Changeset applied after a delivery attempt completes, recording the
/// outcome ([`crate::completion_callback::OutcomeAction`]).
#[derive(Debug, AsChangeset)]
#[diesel(table_name = harvest_completion_deliveries)]
pub struct CompletionDeliveryOutcomeUpdate {
    pub state: String,
    pub next_attempt_at: DateTime<Utc>,
    pub last_status: Option<i32>,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}

// ── Worker sessions (issue #606) ───────────────────────────────────────────

/// A worker session row from `harvest_sessions`.
///
/// Derives `QueryableByName` (alongside the ORM-path `Queryable`) so the
/// broken-session scanner's raw `LEFT JOIN` candidate scan
/// ([`crate::sessions::broken_session_candidates_query`]) can load rows
/// directly, mirroring `TaskQueueItem`'s identical dual-path precedent.
#[derive(
    Debug,
    Clone,
    Queryable,
    QueryableByName,
    Selectable,
    Identifiable,
    serde::Serialize,
    serde::Deserialize,
)]
#[diesel(table_name = harvest_sessions)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestSession {
    pub id: Uuid,
    pub workflow_exec_id: Uuid,
    pub host_worker_id: String,
    pub queue_name: String,
    /// `ACTIVE`, `BROKEN`, or `COMPLETED`.
    pub state: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub broken_at: Option<DateTime<Utc>>,
    pub broken_reason: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Insert struct for a newly acquired worker session.
#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_sessions)]
pub struct NewHarvestSession<'a> {
    pub id: Uuid,
    pub workflow_exec_id: Uuid,
    pub host_worker_id: &'a str,
    pub queue_name: &'a str,
    pub expires_at: DateTime<Utc>,
}

/// Changeset for refreshing a session's lease (issue #606).
///
/// Applied after each member-activity completion so a long-running,
/// legitimate session isn't reaped by the broken-session scanner's
/// lease-expiry check.
#[derive(Debug, AsChangeset)]
#[diesel(table_name = harvest_sessions)]
pub struct SessionLeaseRefresh {
    pub expires_at: DateTime<Utc>,
}

/// Changeset applied by the broken-session scanner (issue #606).
#[derive(Debug, AsChangeset)]
#[diesel(table_name = harvest_sessions)]
pub struct SessionBrokenUpdate {
    pub state: String,
    pub broken_at: DateTime<Utc>,
    pub broken_reason: String,
}

/// Changeset applied by `Session::complete()`'s release activity.
#[derive(Debug, AsChangeset)]
#[diesel(table_name = harvest_sessions)]
pub struct SessionCompletedUpdate {
    pub state: String,
    pub completed_at: DateTime<Utc>,
}

// ── Execution summary (issue #752) ──────────────────────────────────────────────

/// A compact summary of a terminal execution demoted by the retention janitor
/// (issue #752), read from `harvest_execution_summaries`.
#[derive(
    Debug,
    Clone,
    Queryable,
    QueryableByName,
    Selectable,
    Identifiable,
    serde::Serialize,
    serde::Deserialize,
)]
#[diesel(table_name = harvest_execution_summaries)]
#[diesel(primary_key(execution_id))]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ExecutionSummary {
    pub execution_id: Uuid,
    pub workflow_name: String,
    pub workflow_id: String,
    pub state: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub duration_ms: Option<i64>,
    pub shard_id: i32,
    pub search_attrs: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    /// Parent execution UUID for a child-workflow summary, else `None`
    /// (issue #752). Links a demoted child to its parent for the #495 PII-erase
    /// cascade.
    pub parent_id: Option<Uuid>,
    pub summarized_at: DateTime<Utc>,
}

/// Insert struct for a newly written execution summary (issue #752).
///
/// `summarized_at` is omitted so Postgres fills its `DEFAULT NOW()`.
#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_execution_summaries)]
pub struct NewExecutionSummary {
    pub execution_id: Uuid,
    pub workflow_name: String,
    pub workflow_id: String,
    pub state: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub duration_ms: Option<i64>,
    pub shard_id: i32,
    pub search_attrs: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    /// Parent execution UUID when demoting a child workflow, else `None`
    /// (issue #752).
    pub parent_id: Option<Uuid>,
}

// ── WASM activity module storage (issue #965) ───────────────────────────────

/// A content-addressed WASM activity module row from `harvest_wasm_modules`
/// (issue #965).
///
/// Reads carry the full `wasm_bytes` blob; callers that only need to know which
/// version is active should prefer the hash-only resolution helpers in
/// [`crate::wasm_store`] to avoid loading the bytes.
#[derive(
    Debug, Clone, Queryable, QueryableByName, Selectable, serde::Serialize, serde::Deserialize,
)]
#[diesel(table_name = harvest_wasm_modules)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct HarvestWasmModule {
    pub hash: String,
    pub activity_name: String,
    pub wasm_bytes: Vec<u8>,
    pub active: bool,
    pub published_at: DateTime<Utc>,
}

/// Insert struct for publishing a WASM activity module version (issue #965).
///
/// `published_at` is omitted so Postgres fills its `DEFAULT now()`.
#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_wasm_modules)]
pub struct NewHarvestWasmModule<'a> {
    pub hash: &'a str,
    pub activity_name: &'a str,
    pub wasm_bytes: &'a [u8],
    pub active: bool,
}
