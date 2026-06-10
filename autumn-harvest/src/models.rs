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
    harvest_completion_trigger_fires, harvest_completion_trigger_outbox,
    harvest_completion_triggers, harvest_dead_letters, harvest_events, harvest_external_tasks,
    harvest_rate_limit_buckets, harvest_schedule_decisions, harvest_schedules, harvest_signals,
    harvest_task_queue, harvest_timers, harvest_workers, harvest_workflow_executions,
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
}

/// Queryable model representing a fired completion trigger event instance.
#[derive(Debug, Clone, Queryable, Selectable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_completion_trigger_fires)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct CompletionTriggerFireDb {
    pub source_exec_id: Uuid,
    pub trigger_id: Uuid,
    pub fired_at: DateTime<Utc>,
}

/// Insertable model for registering a fired completion trigger.
#[derive(Debug, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = harvest_completion_trigger_fires)]
pub struct NewCompletionTriggerFireDb {
    pub source_exec_id: Uuid,
    pub trigger_id: Uuid,
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
