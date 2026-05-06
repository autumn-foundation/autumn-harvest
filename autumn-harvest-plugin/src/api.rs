//! Axum management routes for Harvest workflows and DAGs.
#![allow(clippy::literal_string_with_formatting_args)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use axum::Extension;
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query};
use axum::routing::{delete, get, patch, post};
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use autumn_harvest::audit::{
    self, AuditFilters, HEADER_ACTOR, HEADER_REQUEST_ID, HEADER_SOURCE, OP_BATCH_SUBMIT,
    OP_DAG_PATCH, OP_DAG_TRIGGER, OP_DLQ_DISCARD_BULK, OP_DLQ_REPLAY, OP_DLQ_REPLAY_BULK,
    OP_EXTERNAL_ACTIVITY_COMPLETE, OP_EXTERNAL_ACTIVITY_FAIL, OP_RETENTION_RUN_NOW,
    OP_SCHEDULE_CREATE, OP_SCHEDULE_DELETE, OP_SCHEDULE_PAUSE, OP_SCHEDULE_RESUME,
    OP_WORKFLOW_CANCEL, OP_WORKFLOW_RESET, OP_WORKFLOW_SIGNAL, OP_WORKFLOW_START, SOURCE_API,
    STATUS_FAILED, STATUS_SUCCEEDED, TARGET_BATCH, TARGET_DAG, TARGET_DEAD_LETTER,
    TARGET_EXTERNAL_ACTIVITY, TARGET_RETENTION, TARGET_SCHEDULE, TARGET_WORKFLOW,
};
use autumn_harvest::batch::{
    self, BatchAction, BatchExecutorConfig, BatchFilter, BatchJobStatus, BatchJobView,
    BatchSubmission,
};
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::dlq;
use autumn_harvest::error::{HarvestError, HarvestResult, database_error};
use autumn_harvest::external_task;
use autumn_harvest::models::{
    AuditRecord, DagRun, DeadLetter, HarvestSchedule, NewAuditRecord, WorkflowExecution,
};
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::queue::{self, ConcurrencyKeyStats};
use autumn_harvest::reset::{
    ResetInvalidPoint, ResetResult, WorkflowResetError, WorkflowResetRequest,
    preview_workflow_reset, reset_workflow_execution,
};
use autumn_harvest::retention::{RetentionConfig, RetentionMonitor, RetentionStatus};
use autumn_harvest::scheduler::{
    DagCatalog, RegisteredDag, SchedulerMonitor, SchedulerSnapshot, trigger_dag,
};
use autumn_harvest::schema::{
    harvest_dag_runs, harvest_events, harvest_external_tasks, harvest_schedules, harvest_signals,
    harvest_task_queue, harvest_timers, harvest_workflow_executions,
};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::signal;
use autumn_harvest::store;
use autumn_harvest::telemetry::{ATTR_EXECUTION_ID, ATTR_QUEUE, ATTR_SHARD_ID, ATTR_WORKFLOW_ID};
use autumn_harvest::types::{
    ExecutionId, ExternalActivityToken, ShardId, UpdateId, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::workers::{
    FleetHealth, WorkerFilters, WorkerRow, fleet_health, get_worker, list_workers,
    parse_worker_filters,
};
use autumn_harvest::{HistoryMatch, HistoryMatcher, WorkflowEvent};
use autumn_harvest::{
    StartWorkflowParams, cancel_workflow_execution, start_or_load_workflow_execution,
};

use crate::state::HarvestDbPool;

#[derive(Clone)]
pub struct HarvestRetentionRuntime {
    config: RetentionConfig,
    monitor: Option<RetentionMonitor>,
    trigger: Option<tokio::sync::mpsc::Sender<()>>,
}

impl HarvestRetentionRuntime {
    #[must_use]
    pub const fn new(
        config: RetentionConfig,
        monitor: Option<RetentionMonitor>,
        trigger: Option<tokio::sync::mpsc::Sender<()>>,
    ) -> Self {
        Self {
            config,
            monitor,
            trigger,
        }
    }

    #[must_use]
    pub const fn disabled(config: RetentionConfig) -> Self {
        Self::new(config, None, None)
    }
}

#[derive(Clone)]
pub struct HarvestApiRuntime {
    registry: Arc<HandlerRegistry>,
    dags: Arc<DagCatalog>,
    workflow_schedules: Arc<Vec<WorkflowSchedule>>,
    worker_id: Option<String>,
    queues: Vec<String>,
    scheduler: SchedulerMonitor,
    retention: HarvestRetentionRuntime,
    router: ShardRouter,
}

impl HarvestApiRuntime {
    /// Build an API runtime snapshot from the available Harvest registrations
    /// and any locally owned worker/scheduler state.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        registry: Arc<HandlerRegistry>,
        dags: Arc<DagCatalog>,
        workflow_schedules: Arc<Vec<WorkflowSchedule>>,
        worker_id: Option<String>,
        queues: Vec<String>,
        scheduler: SchedulerMonitor,
        retention: HarvestRetentionRuntime,
        router: ShardRouter,
    ) -> Self {
        Self {
            registry,
            dags,
            workflow_schedules,
            worker_id,
            queues,
            scheduler,
            retention,
            router,
        }
    }

    /// Shard router used to pick a destination for new workflows.
    #[must_use]
    pub const fn router(&self) -> &ShardRouter {
        &self.router
    }

    /// In-process workflow schedule registrations known to this runtime.
    ///
    /// Returns the schedules that were registered via
    /// [`autumn_harvest::builder::HarvestBuilder::workflow_schedule`] at startup. Schedules added
    /// dynamically via the management API are stored only in the database and
    /// will not appear here.
    #[must_use]
    pub fn workflow_schedules(&self) -> &[WorkflowSchedule] {
        &self.workflow_schedules
    }
}

/// A function that extracts an actor identity string from request headers.
///
/// Implement this to integrate with the application's authentication layer.
/// The returned string should identify the operator performing the action
/// (e.g. a username, API key owner, or service account name).
///
/// If no extractor is configured, the default header-based fallback is used:
/// read `X-Harvest-Actor`, otherwise return `"anonymous"`. Using `"anonymous"`
/// is only acceptable for local or dev deployments.
pub type ActorExtractorFn = Arc<dyn Fn(&axum::http::HeaderMap) -> String + Send + Sync + 'static>;

#[derive(Clone)]
pub struct HarvestApiState {
    runtime: Arc<Mutex<Option<HarvestApiRuntime>>>,
    storage_pool: Arc<Mutex<Option<HarvestDbPool>>>,
    /// `2 × worker_heartbeat_interval`; derived from `WorkerConfig` at startup.
    worker_stale_threshold: Arc<Mutex<std::time::Duration>>,
    /// Optional actor extractor injected by the plugin embedder.
    actor_extractor: Arc<Mutex<Option<ActorExtractorFn>>>,
    /// Audit log retention in days (default: 90).
    audit_retention_days: Arc<Mutex<Option<i64>>>,
}

impl Default for HarvestApiState {
    fn default() -> Self {
        Self {
            runtime: Arc::default(),
            storage_pool: Arc::default(),
            worker_stale_threshold: Arc::new(Mutex::new(std::time::Duration::from_secs(10))),
            actor_extractor: Arc::default(),
            audit_retention_days: Arc::new(Mutex::new(None)),
        }
    }
}

impl HarvestApiState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the worker stale threshold used by `/workers` routes.
    ///
    /// Call this during startup with `2 × WorkerConfig::worker_heartbeat_interval`
    /// so the API correctly reflects the configured heartbeat cadence.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_worker_stale_threshold(&self, threshold: std::time::Duration) {
        *self
            .worker_stale_threshold
            .lock()
            .expect("harvest api state lock poisoned") = threshold;
    }

    /// Install a custom actor extractor used to derive the `actor` field of
    /// audit records from incoming request headers.
    ///
    /// The closure receives the full `HeaderMap` of the mutating request and
    /// must return a non-empty string identifying the caller. Common patterns:
    /// - Read a `X-User-ID` header set by an upstream auth gateway.
    /// - Decode a JWT claim from `Authorization`.
    /// - Fall back to `"anonymous"` for unauthenticated dev routes.
    ///
    /// When no extractor is installed the default behaviour reads the
    /// `X-Harvest-Actor` header and falls back to `"anonymous"`.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_actor_extractor<F>(&self, f: F)
    where
        F: Fn(&axum::http::HeaderMap) -> String + Send + Sync + 'static,
    {
        *self
            .actor_extractor
            .lock()
            .expect("harvest api state lock poisoned") = Some(Arc::new(f));
    }

    /// Set the audit log retention period in days.
    ///
    /// Audit records older than this threshold will be deleted on each
    /// retention sweep. Default: 90 days.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_audit_retention_days(&self, days: i64) {
        *self
            .audit_retention_days
            .lock()
            .expect("harvest api state lock poisoned") = Some(days);
    }

    /// Returns `Some(days)` only when explicitly set via [`set_audit_retention_days`];
    /// `None` means "use the builder's retention config unchanged".
    pub(crate) fn audit_retention_days(&self) -> Option<i64> {
        *self
            .audit_retention_days
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Extract the actor identity from request headers using the configured
    /// extractor, or fall back to reading `X-Harvest-Actor` / `"anonymous"`.
    pub(crate) fn extract_actor(&self, headers: &axum::http::HeaderMap) -> String {
        let extractor = self
            .actor_extractor
            .lock()
            .expect("harvest api state lock poisoned")
            .clone();
        if let Some(f) = extractor {
            return f(headers);
        }
        // Default: read X-Harvest-Actor header.
        headers
            .get(HEADER_ACTOR)
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
            .unwrap_or("anonymous")
            .to_string()
    }

    pub(crate) fn worker_stale_threshold(&self) -> std::time::Duration {
        *self
            .worker_stale_threshold
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Install the currently running Harvest runtime snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the internal API-state mutex is poisoned.
    pub fn install(&self, runtime: HarvestApiRuntime) {
        *self
            .runtime
            .lock()
            .expect("harvest api state lock poisoned") = Some(runtime);
    }

    /// Install the Harvest storage pool used by management routes.
    ///
    /// # Panics
    ///
    /// Panics if the internal API-state mutex is poisoned.
    pub fn install_storage_pool(&self, pool: HarvestDbPool) {
        *self
            .storage_pool
            .lock()
            .expect("harvest api state lock poisoned") = Some(pool);
    }

    /// Clear the currently running Harvest runtime snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the internal API-state mutex is poisoned.
    pub fn clear(&self) {
        *self
            .runtime
            .lock()
            .expect("harvest api state lock poisoned") = None;
        *self
            .storage_pool
            .lock()
            .expect("harvest api state lock poisoned") = None;
    }

    fn runtime(&self) -> HarvestResult<HarvestApiRuntime> {
        self.runtime
            .lock()
            .expect("harvest api state lock poisoned")
            .clone()
            .ok_or_else(|| HarvestError::Config("harvest runtime is not started".to_string()))
    }

    pub(crate) fn storage_pool(&self) -> HarvestResult<HarvestDbPool> {
        self.storage_pool
            .lock()
            .expect("harvest api state lock poisoned")
            .clone()
            .ok_or_else(|| {
                HarvestError::Config("harvest storage pool is not configured".to_string())
            })
    }
}

#[derive(Debug, Serialize)]
struct WorkflowDetailsResponse {
    parent_id: Option<uuid::Uuid>,
    execution: WorkflowExecution,
    history: Vec<Value>,
}

#[derive(Debug, Serialize)]
struct WorkflowStackResponse {
    exec_id: String,
    workflow_id: String,
    workflow_name: String,
    state: String,
    is_terminal: bool,
    pending_activities: Vec<PendingActivity>,
    pending_local_activities: Vec<PendingLocalActivity>,
    pending_timers: Vec<PendingTimer>,
    pending_signals: Vec<PendingSignal>,
    buffered_signals: Vec<BufferedSignal>,
    pending_child_workflows: Vec<PendingChildWorkflow>,
    last_event_id: i64,
}

#[derive(Debug, Serialize)]
struct PendingActivity {
    activity_exec_id: String,
    activity_name: String,
    queue: String,
    scheduled_at: chrono::DateTime<chrono::Utc>,
    attempt: i32,
    max_attempts: i32,
    task_status: String,
    claimed_by_worker_id: Option<String>,
    last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    next_retry_at: Option<chrono::DateTime<chrono::Utc>>,
    schedule_to_start_deadline: Option<chrono::DateTime<chrono::Utc>>,
    start_to_close_deadline: Option<chrono::DateTime<chrono::Utc>>,
    heartbeat_deadline: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize)]
struct PendingLocalActivity {
    activity_exec_id: String,
    activity_name: String,
    scheduled_at: chrono::DateTime<chrono::Utc>,
    attempt: i32,
    max_attempts: i32,
    task_status: String,
    last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    next_retry_at: Option<chrono::DateTime<chrono::Utc>>,
    start_to_close_deadline: Option<chrono::DateTime<chrono::Utc>>,
    heartbeat_deadline: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize)]
struct PendingTimer {
    timer_id: String,
    name: Option<String>,
    fires_at: chrono::DateTime<chrono::Utc>,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize)]
struct PendingSignal {
    signal_name: String,
    waiters: i64,
    oldest_waiter_since: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
struct BufferedSignal {
    signal_name: String,
    buffered: i64,
    oldest_received_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
struct PendingChildWorkflow {
    child_exec_id: String,
    child_workflow_name: String,
    state: String,
}

fn is_terminal_state(state: &str) -> bool {
    matches!(
        state,
        "COMPLETED" | "FAILED" | "CANCELLED" | "TIMED_OUT" | "CONTINUED_AS_NEW" | "TERMINATED"
    )
}

#[derive(Debug, Serialize)]
struct StartWorkflowResponse {
    execution_id: String,
    workflow_name: String,
    workflow_id: String,
    state: String,
}

#[derive(Debug, Serialize)]
struct BasicAck {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct DagSummary {
    name: String,
    schedule_expr: Option<String>,
    is_paused: bool,
    next_run_at: Option<chrono::DateTime<chrono::Utc>>,
    max_active_runs: i32,
    catchup: bool,
    task_count: usize,
}

#[derive(Debug, Serialize)]
struct HarvestHealth {
    runtime_ready: bool,
    worker_id: Option<String>,
    queues: Vec<String>,
    dag_count: usize,
    scheduler: SchedulerSnapshot,
}

#[derive(Debug, Serialize)]
struct ReplayDeadLetterResponse {
    ok: bool,
    dead_letter_id: String,
    task_id: String,
}

#[derive(Debug, Serialize)]
struct CancelWorkflowResponse {
    ok: bool,
    execution_id: String,
    state: String,
    reason: String,
    newly_cancelled: bool,
    failed_task_count: usize,
}

#[derive(Debug, Serialize)]
struct ResetWorkflowResponse {
    new_exec_id: String,
    reset_from_exec_id: String,
    reset_to_event_id: i64,
    events_carried_over: usize,
    source_tasks_cancelled: usize,
    source_timers_removed: usize,
    source_signals_dropped: usize,
    source_signals_buffered: usize,
}

impl From<ResetResult> for ResetWorkflowResponse {
    fn from(result: ResetResult) -> Self {
        Self {
            new_exec_id: result.new_exec_id.to_string(),
            reset_from_exec_id: result.reset_from_exec_id.to_string(),
            reset_to_event_id: result.reset_to_event_id,
            events_carried_over: result.events_carried_over,
            source_tasks_cancelled: result.source_tasks_cancelled,
            source_timers_removed: result.source_timers_removed,
            source_signals_dropped: result.source_signals_dropped,
            source_signals_buffered: result.source_signals_buffered,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ResetWorkflowQuery {
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug, Serialize)]
struct ResetErrorResponse {
    message: String,
}

#[derive(Debug, Deserialize)]
struct StartWorkflowRequest {
    workflow_id: Option<String>,
    input: Option<Value>,
    queue: Option<String>,
    memo: Option<Value>,
    search_attrs: Option<Value>,
    execution_timeout_secs: Option<i64>,
    /// How to handle a duplicate `(workflow_name, workflow_id)` collision.
    /// Omitted or `null` → `AllowDuplicate` (preserves existing wire behaviour).
    /// An unknown string value returns `400 Bad Request` with the offending value
    /// echoed in the response body.
    reuse_policy: Option<String>,
}

/// Response body for a 409 Conflict returned by `RejectDuplicate` policy.
#[derive(Debug, Serialize)]
struct AlreadyExistsResponse {
    existing_execution_id: String,
    existing_state: String,
}

#[derive(Debug, Deserialize)]
struct DagTriggerRequest {
    conf: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct CancelWorkflowRequest {
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DagPauseRequest {
    paused: bool,
}

/// Kind tag on a schedule entry returned by `GET /admin/schedules`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum ScheduleKind {
    Dag,
    Workflow,
}

/// A single schedule entry in the `GET /admin/schedules` list.
#[derive(Debug, Serialize)]
struct ScheduleEntry {
    id: uuid::Uuid,
    kind: ScheduleKind,
    name: String,
    schedule_expr: Option<String>,
    is_paused: bool,
    next_run_at: Option<chrono::DateTime<chrono::Utc>>,
    last_run_at: Option<chrono::DateTime<chrono::Utc>>,
    max_active_runs: i32,
    catchup: bool,
}

/// Request body for `POST /admin/schedules/workflow`.
#[derive(Debug, Deserialize)]
struct CreateWorkflowScheduleRequest {
    workflow_name: String,
    schedule_expr: String,
    #[serde(default)]
    input: serde_json::Value,
    #[serde(default)]
    catchup: bool,
    #[serde(default = "default_max_active_runs")]
    max_active_runs: u32,
    #[serde(default)]
    paused: bool,
    #[serde(default = "default_queue_name")]
    queue_name: String,
}

fn default_queue_name() -> String {
    "default".to_string()
}

const fn default_max_active_runs() -> u32 {
    1
}

/// Workflow execution states that the management API recognises in `state=`
/// filters. Anything outside this list is rejected with `400 Bad Request`.
pub(crate) const KNOWN_WORKFLOW_STATES: &[&str] = &[
    "RUNNING",
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMED_OUT",
    "CONTINUED_AS_NEW",
    "TERMINATED",
];

const DEFAULT_WORKFLOW_LIMIT: i64 = 50;
const MAX_WORKFLOW_LIMIT: i64 = 200;
const DEFAULT_WORKFLOW_CHILDREN_LIMIT: usize = 50;
const MAX_WORKFLOW_CHILDREN_LIMIT: usize = 500;
const MAX_WORKFLOW_CHILDREN_DEPTH: u8 = 5;

#[derive(Debug, Default, Clone)]
pub(crate) struct WorkflowFilters {
    pub(crate) limit: i64,
    pub(crate) states: Vec<String>,
    pub(crate) workflow_name: Option<String>,
    pub(crate) search_attrs: Vec<Value>,
}

impl WorkflowFilters {
    pub(crate) const fn with_limit(mut self, limit: i64) -> Self {
        self.limit = limit;
        self
    }
}

#[derive(Debug, Clone)]
struct WorkflowChildrenFilters {
    limit: usize,
    statuses: Vec<String>,
    workflow_name: Option<String>,
    cursor: Option<WorkflowChildrenCursor>,
    max_depth: u8,
}

impl Default for WorkflowChildrenFilters {
    fn default() -> Self {
        Self {
            limit: DEFAULT_WORKFLOW_CHILDREN_LIMIT,
            statuses: Vec::new(),
            workflow_name: None,
            cursor: None,
            max_depth: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct WorkflowChildrenCursor {
    started_at: chrono::DateTime<chrono::Utc>,
    exec_id: uuid::Uuid,
}

#[derive(Debug, Serialize)]
struct WorkflowChildrenResponse {
    items: Vec<WorkflowChildResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct WorkflowChildResponse {
    exec_id: String,
    workflow_name: String,
    status: String,
    started_at: chrono::DateTime<chrono::Utc>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
    error_summary: Option<String>,
    shard_id: i32,
    depth: u8,
}

#[derive(Debug, Deserialize)]
struct DeadLetterListQuery {
    limit: Option<i64>,
}

pub fn harvest_api_router(api_state: HarvestApiState) -> Router<AppState> {
    Router::new()
        .route("/workflows", get(list_workflows))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{id}/children", get(list_workflow_children))
        .route("/workflows/{id}/stack", get(get_workflow_stack))
        .route("/workflows/{workflow_name}/start", post(start_workflow))
        .route("/workflows/{id}/cancel", post(cancel_workflow))
        .route("/workflows/{id}/reset", post(reset_workflow))
        .route(
            "/workflows/{id}/signal/{signal_name}",
            post(signal_workflow),
        )
        .route("/workflows/{id}/query/{query_name}", get(query_workflow))
        // Update primitive (issue #140): synchronous request/response into a running workflow.
        .route("/workflows/{id}/update/{update_name}", post(admit_update))
        .route(
            "/workflows/{id}/update/{update_id}/result",
            get(get_update_result),
        )
        .route("/dags", get(list_dags))
        .route("/dags/{dag_name}/runs", get(list_dag_runs))
        .route("/dags/{dag_name}/trigger", post(trigger_dag_run))
        .route("/dags/{dag_name}", patch(patch_dag))
        .route("/dead-letters", get(list_dead_letters))
        .route(
            "/dead-letters/replay",
            post(bulk_replay_dead_letters_handler),
        )
        .route(
            "/dead-letters/discard",
            post(bulk_discard_dead_letters_handler),
        )
        .route("/dead-letters/{id}/replay", post(replay_dead_letter))
        .route("/health", get(health))
        .route("/admin/retention", get(retention_status))
        .route("/admin/retention/run-now", post(retention_run_now))
        .route("/admin/concurrency", get(concurrency_status))
        // Schedule management (issue #91): unified list + workflow-schedule CRUD.
        .route("/admin/schedules", get(list_schedules))
        .route("/admin/schedules/workflow", post(create_workflow_schedule))
        .route("/admin/schedules/{id}/pause", post(pause_schedule))
        .route("/admin/schedules/{id}/resume", post(resume_schedule))
        .route("/admin/schedules/{id}", delete(delete_schedule))
        // External activity completion (issue #92): async task-token API.
        .route(
            "/activities/external/{token}/complete",
            post(complete_external_activity),
        )
        .route(
            "/activities/external/{token}/fail",
            post(fail_external_activity),
        )
        .route(
            "/activities/external/{token}/heartbeat",
            post(heartbeat_external_activity),
        )
        // Worker fleet observability (issue #100).
        // /workers/health must be registered before /workers/{worker_id} so axum
        // does not treat the literal "health" segment as a worker_id capture.
        .route("/workers/health", get(workers_health))
        .route("/workers", get(list_workers_handler))
        .route("/workers/{worker_id}", get(get_worker_handler))
        // Batch operations (issue #102): operator-facing fleet-wide cancel /
        // terminate / signal so an incident commander does not have to script
        // a one-off loop over GET /workflows.
        .route("/batch-operations", get(list_batch_operations))
        .route("/batch-operations", post(submit_batch_operation))
        .route("/batch-operations/{id}", get(get_batch_operation))
        // Audit trail (issue #158): read-only endpoint to query management
        // API mutations. See `audit::ALL_MUTATION_ROUTES` for covered paths.
        .route("/admin/audit", get(list_audit_records))
        .layer(Extension(api_state))
}

#[derive(Debug, Deserialize)]
struct SubmitBatchOperationRequest {
    /// `Cancel`, `Terminate`, or `Signal`.
    action: String,
    /// Mirrors the `GET /workflows` filter contract.
    #[serde(default)]
    filter: BatchFilter,
    /// Required when `action == "Signal"`.
    #[serde(default)]
    signal_name: Option<String>,
    /// Optional payload sent to each matched workflow when `action == "Signal"`.
    #[serde(default)]
    signal_payload: Option<Value>,
    /// Operator-supplied retry token. Re-submitting with the same key returns
    /// the existing `batch_job_id` instead of starting a duplicate batch.
    #[serde(default)]
    idempotency_key: Option<String>,
    /// Optional caller identity (e.g. the on-call handle) for audit.
    #[serde(default)]
    created_by: Option<String>,
}

#[derive(Debug, Serialize)]
struct SubmitBatchOperationResponse {
    batch_job_id: String,
}

#[derive(Debug, Deserialize)]
struct ListBatchOperationsQuery {
    status: Option<String>,
    action: Option<String>,
    limit: Option<i64>,
}

async fn submit_batch_operation(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<SubmitBatchOperationRequest>,
) -> Result<(axum::http::StatusCode, Json<SubmitBatchOperationResponse>), AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /batch-operations";
    let idempotency_key = request.idempotency_key.clone();

    let action: BatchAction = match request.action.parse() {
        Ok(a) => a,
        Err(err_msg) => {
            batch_submit_audit_failed(
                &api_state,
                &actor,
                &source,
                request_id.as_deref(),
                idempotency_key.as_deref(),
                &err_msg,
            )
            .await;
            return Err(AutumnError::bad_request_msg(err_msg));
        }
    };

    // Reject `state=` values outside the canonical list before we hit the DB
    // — otherwise the executor would silently match nothing and look like a
    // success.
    for state in &request.filter.states {
        if !KNOWN_WORKFLOW_STATES.contains(&state.as_str()) {
            let err_msg = format!("unknown workflow state '{state}' in batch filter");
            batch_submit_audit_failed(
                &api_state,
                &actor,
                &source,
                request_id.as_deref(),
                idempotency_key.as_deref(),
                &err_msg,
            )
            .await;
            return Err(AutumnError::bad_request_msg(err_msg));
        }
    }

    let pool = api_state.storage_pool().map_err(map_error)?;

    // Persist the row on the default shard. The executor will fan out across
    // every configured shard at run time via iter_shards().
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let submit_result = batch::submit_batch_job(
        &mut conn,
        BatchSubmission {
            action,
            filter: request.filter,
            signal_name: request.signal_name,
            signal_payload: request.signal_payload,
            idempotency_key: request.idempotency_key,
            created_by: request.created_by,
        },
    )
    .await;

    match submit_result {
        Err(e) => {
            let err_str = e.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_BATCH_SUBMIT,
                target_type: TARGET_BATCH,
                target_id: None,
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: idempotency_key.as_deref(),
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            Err(map_error(e))
        }
        Ok(job_id) => {
            let job_id_str = job_id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_BATCH_SUBMIT,
                target_type: TARGET_BATCH,
                target_id: Some(job_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: idempotency_key.as_deref(),
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: &source,
            };
            audit::insert_audit(&mut conn, &ar)
                .await
                .map_err(map_error)?;
            Ok((
                axum::http::StatusCode::ACCEPTED,
                Json(SubmitBatchOperationResponse {
                    batch_job_id: job_id_str,
                }),
            ))
        }
    }
}

async fn get_batch_operation(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Json<BatchJobView>, AutumnError> {
    let job_id = parse_uuid(&id, "batch job id")?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let row = batch::get_batch_job(&mut conn, job_id)
        .await
        .map_err(map_error)?
        .ok_or_else(|| AutumnError::not_found_msg(format!("batch job {id}")))?;
    Ok(Json(BatchJobView::from_row(row)))
}

async fn list_batch_operations(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<ListBatchOperationsQuery>,
) -> Result<Json<Vec<BatchJobView>>, AutumnError> {
    let status = query
        .status
        .as_deref()
        .map(BatchJobStatus::from_str_or_err)
        .transpose()?;
    let action = query
        .action
        .as_deref()
        .map(BatchAction::from_str_or_err)
        .transpose()?;
    let limit = query.limit.unwrap_or(50);

    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let rows = batch::list_batch_jobs(
        &mut conn,
        &batch::ListFilters {
            status,
            action,
            limit,
        },
    )
    .await
    .map_err(map_error)?;
    Ok(Json(rows.into_iter().map(BatchJobView::from_row).collect()))
}

trait ParseFromStrOrErr: Sized {
    fn from_str_or_err(s: &str) -> Result<Self, AutumnError>;
}

impl ParseFromStrOrErr for BatchJobStatus {
    fn from_str_or_err(s: &str) -> Result<Self, AutumnError> {
        s.parse::<Self>().map_err(AutumnError::bad_request_msg)
    }
}

impl ParseFromStrOrErr for BatchAction {
    fn from_str_or_err(s: &str) -> Result<Self, AutumnError> {
        s.parse::<Self>().map_err(AutumnError::bad_request_msg)
    }
}

// ── Audit trail read endpoint (issue #158) ────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct AuditListQuery {
    actor: Option<String>,
    operation: Option<String>,
    target_type: Option<String>,
    target_id: Option<String>,
    status: Option<String>,
    /// ISO 8601 timestamp lower bound (inclusive).
    since: Option<String>,
    /// ISO 8601 timestamp upper bound (exclusive).
    before: Option<String>,
    limit: Option<i64>,
}

async fn list_audit_records(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<AuditListQuery>,
) -> Result<Json<Vec<AuditRecord>>, AutumnError> {
    let limit = query
        .limit
        .unwrap_or(AuditFilters::default_limit())
        .clamp(1, 500);

    let since = query
        .since
        .as_deref()
        .map(parse_audit_datetime)
        .transpose()?;
    let before = query
        .before
        .as_deref()
        .map(parse_audit_datetime)
        .transpose()?;

    let filters = AuditFilters {
        actor: query.actor,
        operation: query.operation,
        target_type: query.target_type,
        target_id: query.target_id,
        status: query.status,
        since,
        before,
        limit,
    };

    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut records: Vec<AuditRecord> = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = audit::list_audit(&mut conn, &filters)
            .await
            .map_err(map_error)?;
        records.append(&mut rows);
    }

    // Merge shards: sort by occurred_at DESC, then id for determinism.
    records.sort_by(|a, b| {
        b.occurred_at
            .cmp(&a.occurred_at)
            .then_with(|| b.id.cmp(&a.id))
    });
    records.truncate(usize::try_from(limit).unwrap_or(usize::MAX));

    Ok(Json(records))
}

fn parse_audit_datetime(raw: &str) -> Result<chrono::DateTime<chrono::Utc>, AutumnError> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|_| {
            AutumnError::bad_request_msg(format!(
                "invalid datetime '{raw}'; expected RFC 3339 format, e.g. 2026-05-06T00:00:00Z"
            ))
        })
}

// ── Audit emission helpers ─────────────────────────────────────────────────────

/// Extract audit context (`actor`, `source`, `request_id`) from request headers.
fn audit_context(
    headers: &axum::http::HeaderMap,
    api_state: &HarvestApiState,
) -> (String, String, Option<String>) {
    let actor = api_state.extract_actor(headers);

    let source = headers
        .get(HEADER_SOURCE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| match s {
            "api" | "cli" | "ui" => Some(s.to_string()),
            _ => None,
        })
        .unwrap_or_else(|| SOURCE_API.to_string());

    let request_id = headers
        .get(HEADER_REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);

    (actor, source, request_id)
}

/// Run one tick of the batch executor across every configured shard.
///
/// Exposed for the plugin's lifecycle wiring; tests construct a
/// [`HarvestDbPool`] and call into `autumn_harvest::batch::run_executor_once`
/// directly with the underlying [`autumn_harvest::shard::ShardedDbPool`].
///
/// # Errors
///
/// Returns [`AutumnError`] when the executor cannot reach a shard or a
/// per-shard SQL operation fails. Per-target failures are recorded on the
/// job row and do not propagate as errors.
pub async fn run_batch_executor_once(
    pool: &HarvestDbPool,
    config: &BatchExecutorConfig,
) -> Result<(), AutumnError> {
    batch::run_executor_once(pool.sharded_pool(), config)
        .await
        .map_err(map_error)
}

#[cfg(test)]
mod stack_state_tests {
    use super::is_terminal_state;

    #[test]
    fn terminal_state_classifier_includes_timeout_and_continue_as_new() {
        assert!(is_terminal_state("COMPLETED"));
        assert!(is_terminal_state("FAILED"));
        assert!(is_terminal_state("CANCELLED"));
        assert!(is_terminal_state("TIMED_OUT"));
        assert!(is_terminal_state("CONTINUED_AS_NEW"));
        assert!(is_terminal_state("TERMINATED"));
        assert!(!is_terminal_state("RUNNING"));
    }
}

async fn list_workflows(
    Extension(api_state): Extension<HarvestApiState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<Vec<WorkflowExecution>>, AutumnError> {
    let filters = parse_workflow_filters(&pairs)?;
    let workflows = load_workflows_from_shards(&api_state, &filters).await?;
    Ok(Json(workflows))
}

/// Parse the management-API query string into a `WorkflowFilters`.
///
/// State values are comma-separated (or repeated `state=`) and validated
/// against `KNOWN_WORKFLOW_STATES`. `search_attr=` values must be `key:value`
/// and are repeatable; each entry contributes a separate JSONB containment
/// predicate so repeats narrow rather than widen.
pub(crate) fn parse_workflow_filters(
    pairs: &[(String, String)],
) -> Result<WorkflowFilters, AutumnError> {
    let mut limit_raw: Option<i64> = None;
    let mut filters = WorkflowFilters::default();

    for (key, value) in pairs {
        match key.as_str() {
            "limit" => {
                let parsed = value.parse::<i64>().map_err(|_| {
                    AutumnError::bad_request_msg(format!("invalid limit '{value}'"))
                })?;
                limit_raw = Some(parsed);
            }
            "state" => {
                for raw in value.split(',') {
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    if !KNOWN_WORKFLOW_STATES.contains(&trimmed) {
                        return Err(AutumnError::bad_request_msg(format!(
                            "unknown workflow state '{trimmed}'; expected one of {KNOWN_WORKFLOW_STATES:?}"
                        )));
                    }
                    let owned = trimmed.to_string();
                    if !filters.states.contains(&owned) {
                        filters.states.push(owned);
                    }
                }
            }
            "workflow_name" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.workflow_name = Some(trimmed.to_string());
                }
            }
            "search_attr" => {
                let (raw_key, raw_val) = value.split_once(':').ok_or_else(|| {
                    AutumnError::bad_request_msg(format!(
                        "invalid search_attr '{value}'; expected 'key:value'"
                    ))
                })?;
                let attr_key = raw_key.trim();
                if attr_key.is_empty() {
                    return Err(AutumnError::bad_request_msg(format!(
                        "search_attr '{value}' is missing a key"
                    )));
                }
                let mut object = serde_json::Map::with_capacity(1);
                object.insert(attr_key.to_string(), Value::String(raw_val.to_string()));
                filters.search_attrs.push(Value::Object(object));
            }
            _ => {
                // Ignore unknown query parameters so future additions stay non-breaking.
            }
        }
    }

    let limit = limit_raw
        .unwrap_or(DEFAULT_WORKFLOW_LIMIT)
        .clamp(1, MAX_WORKFLOW_LIMIT);
    Ok(filters.with_limit(limit))
}

async fn get_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowDetailsResponse>, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let history = store::load_history(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let events = history
        .events
        .into_iter()
        .map(|event| serde_json::to_value(event).map_err(HarvestError::from))
        .collect::<HarvestResult<Vec<_>>>()
        .map_err(map_error)?;

    Ok(Json(WorkflowDetailsResponse {
        parent_id: execution.parent_id,
        execution,
        history: events,
    }))
}

async fn list_workflow_children(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<WorkflowChildrenResponse>, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let filters = parse_workflow_children_filters(&pairs)?;

    {
        let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
        load_execution(&mut conn, exec_id)
            .await
            .map_err(map_error)?;
    }

    let mut rows = load_workflow_children_page_from_shards(&api_state, exec_id, &filters).await?;
    sort_workflow_child_rows(&mut rows);

    let next_cursor = if rows.len() > filters.limit {
        let cursor = encode_workflow_children_cursor(&rows[filters.limit - 1]);
        rows.truncate(filters.limit);
        Some(cursor)
    } else {
        None
    };

    Ok(Json(WorkflowChildrenResponse {
        items: rows.into_iter().map(WorkflowChildResponse::from).collect(),
        next_cursor,
    }))
}

async fn load_workflow_children_page_from_shards(
    api_state: &HarvestApiState,
    parent_id: ExecutionId,
    filters: &WorkflowChildrenFilters,
) -> Result<Vec<store::WorkflowChildRow>, AutumnError> {
    if filters.max_depth > 0 {
        return load_workflow_children_tree_from_shards(api_state, parent_id, filters).await;
    }

    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut rows = Vec::new();
    let query_filters = workflow_children_store_filters(filters);

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut shard_rows = store::load_workflow_children(&mut conn, parent_id, &query_filters, 0)
            .await
            .map_err(map_error)?;
        rows.append(&mut shard_rows);
    }

    Ok(rows)
}

async fn load_workflow_children_tree_from_shards(
    api_state: &HarvestApiState,
    parent_id: ExecutionId,
    filters: &WorkflowChildrenFilters,
) -> Result<Vec<store::WorkflowChildRow>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut rows = Vec::new();
    let mut frontier = vec![parent_id];
    let mut seen = HashSet::new();
    // Result filters cannot constrain traversal: a nonmatching child can have
    // matching descendants, and either row may live on any shard.
    let traversal_filters = store::WorkflowChildFilters::default();

    for depth in 0..=filters.max_depth {
        if frontier.is_empty() {
            break;
        }

        let mut next_frontier = Vec::new();
        for parent in &frontier {
            for (_shard, shard_pool) in pool.iter_shards() {
                let mut conn = acquire_conn(shard_pool).await?;
                let shard_rows =
                    store::load_workflow_children(&mut conn, *parent, &traversal_filters, depth)
                        .await
                        .map_err(map_error)?;
                for row in shard_rows {
                    if !seen.insert(row.exec_id.as_uuid()) {
                        continue;
                    }

                    next_frontier.push(row.exec_id);
                    if workflow_child_matches_filters(&row, filters)
                        && workflow_child_is_after_cursor(&row, filters.cursor.as_ref())
                    {
                        rows.push(row);
                    }
                }
            }
        }

        frontier = next_frontier;
    }

    Ok(rows)
}

fn workflow_children_store_filters(
    filters: &WorkflowChildrenFilters,
) -> store::WorkflowChildFilters {
    let query_limit = filters.limit.saturating_add(1);
    store::WorkflowChildFilters {
        statuses: filters.statuses.clone(),
        workflow_name: filters.workflow_name.clone(),
        cursor: filters
            .cursor
            .as_ref()
            .map(|cursor| store::WorkflowChildCursor {
                started_at: cursor.started_at,
                exec_id: cursor.exec_id,
            }),
        limit: Some(i64::try_from(query_limit).unwrap_or(i64::MAX)),
    }
}

fn parse_workflow_children_filters(
    pairs: &[(String, String)],
) -> Result<WorkflowChildrenFilters, AutumnError> {
    let mut filters = WorkflowChildrenFilters::default();

    for (key, value) in pairs {
        match key.as_str() {
            "status" => {
                for raw in value.split(',') {
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let status = parse_workflow_child_status(trimmed)?;
                    if !filters.statuses.contains(&status) {
                        filters.statuses.push(status);
                    }
                }
            }
            "workflow_name" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.workflow_name = Some(trimmed.to_string());
                }
            }
            "limit" => {
                let parsed = value.parse::<usize>().map_err(|_| {
                    AutumnError::bad_request_msg(format!("invalid limit '{value}'"))
                })?;
                filters.limit = parsed.clamp(1, MAX_WORKFLOW_CHILDREN_LIMIT);
            }
            "cursor" => {
                filters.cursor = Some(parse_workflow_children_cursor(value)?);
            }
            "depth" => {
                let parsed = value.parse::<u8>().map_err(|_| {
                    AutumnError::bad_request_msg(format!("invalid depth '{value}'"))
                })?;
                if parsed > MAX_WORKFLOW_CHILDREN_DEPTH {
                    return Err(AutumnError::bad_request_msg(format!(
                        "depth {parsed} exceeds maximum {MAX_WORKFLOW_CHILDREN_DEPTH}"
                    )));
                }
                filters.max_depth = parsed;
            }
            _ => {}
        }
    }

    Ok(filters)
}

fn parse_workflow_child_status(raw: &str) -> Result<String, AutumnError> {
    let normalized = raw
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .flat_map(char::to_lowercase)
        .collect::<String>();
    let status = match normalized.as_str() {
        "running" => "RUNNING",
        "failed" => "FAILED",
        "completed" => "COMPLETED",
        "cancelled" | "canceled" => "CANCELLED",
        "terminated" => "TERMINATED",
        "timedout" => "TIMED_OUT",
        "continuedasnew" => "CONTINUED_AS_NEW",
        _ => {
            return Err(AutumnError::bad_request_msg(format!(
                "unknown workflow child status '{raw}'; expected one of Running, Failed, Completed, Cancelled, Terminated, TimedOut, ContinuedAsNew"
            )));
        }
    };
    Ok(status.to_string())
}

fn parse_workflow_children_cursor(raw: &str) -> Result<WorkflowChildrenCursor, AutumnError> {
    let (started_at, exec_id) = raw.split_once('|').ok_or_else(|| {
        AutumnError::bad_request_msg("invalid cursor; expected '<started_at>|<exec_id>'")
    })?;
    let started_at = chrono::DateTime::parse_from_rfc3339(started_at)
        .map_err(|_| AutumnError::bad_request_msg("invalid cursor timestamp"))?
        .with_timezone(&chrono::Utc);
    let exec_id = exec_id
        .parse::<uuid::Uuid>()
        .map_err(|_| AutumnError::bad_request_msg("invalid cursor execution id"))?;

    Ok(WorkflowChildrenCursor {
        started_at,
        exec_id,
    })
}

fn workflow_child_matches_filters(
    row: &store::WorkflowChildRow,
    filters: &WorkflowChildrenFilters,
) -> bool {
    if !filters.statuses.is_empty() && !filters.statuses.contains(&row.status) {
        return false;
    }
    if let Some(name) = &filters.workflow_name
        && row.workflow_name != *name
    {
        return false;
    }
    true
}

fn workflow_child_is_after_cursor(
    row: &store::WorkflowChildRow,
    cursor: Option<&WorkflowChildrenCursor>,
) -> bool {
    let Some(cursor) = cursor else {
        return true;
    };

    row.started_at < cursor.started_at
        || (row.started_at == cursor.started_at && row.exec_id.as_uuid() < cursor.exec_id)
}

fn sort_workflow_child_rows(rows: &mut [store::WorkflowChildRow]) {
    rows.sort_by(|left, right| {
        right
            .started_at
            .cmp(&left.started_at)
            .then_with(|| right.exec_id.as_uuid().cmp(&left.exec_id.as_uuid()))
    });
}

fn encode_workflow_children_cursor(row: &store::WorkflowChildRow) -> String {
    format!(
        "{}|{}",
        row.started_at
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        row.exec_id
    )
}

fn workflow_child_status_label(status: &str) -> String {
    match status {
        "RUNNING" => "Running",
        "FAILED" => "Failed",
        "COMPLETED" => "Completed",
        "CANCELLED" => "Cancelled",
        "TERMINATED" => "Terminated",
        "TIMED_OUT" => "TimedOut",
        "CONTINUED_AS_NEW" => "ContinuedAsNew",
        other => other,
    }
    .to_string()
}

impl From<store::WorkflowChildRow> for WorkflowChildResponse {
    fn from(row: store::WorkflowChildRow) -> Self {
        Self {
            exec_id: row.exec_id.to_string(),
            workflow_name: row.workflow_name,
            status: workflow_child_status_label(&row.status),
            started_at: row.started_at,
            completed_at: row.completed_at,
            error_summary: row.error_summary,
            shard_id: row.shard_id,
            depth: row.depth,
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn get_workflow_stack(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowStackResponse>, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let exec_uuid = exec_id.as_uuid();
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let is_terminal = is_terminal_state(&execution.state);
    let last_event_id = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .select(diesel::dsl::max(harvest_events::event_id))
        .first::<Option<i32>>(&mut conn)
        .await
        .map_err(database_error)?
        .map_or(0_i64, i64::from);
    if is_terminal {
        return Ok(Json(WorkflowStackResponse {
            exec_id: exec_id.to_string(),
            workflow_id: execution.workflow_id,
            workflow_name: execution.workflow_name,
            state: execution.state,
            is_terminal,
            pending_activities: Vec::new(),
            pending_local_activities: Vec::new(),
            pending_timers: Vec::new(),
            pending_signals: Vec::new(),
            buffered_signals: Vec::new(),
            pending_child_workflows: Vec::new(),
            last_event_id,
        }));
    }

    let tasks = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_uuid)))
        .filter(harvest_task_queue::task_type.eq("activity"))
        .filter(harvest_task_queue::state.eq_any(["PENDING", "CLAIMED", "RUNNING", "BACKOFF"]))
        .select(autumn_harvest::models::TaskQueueItem::as_select())
        .load::<autumn_harvest::models::TaskQueueItem>(&mut conn)
        .await
        .map_err(database_error)?;
    let pending_activities = tasks
        .into_iter()
        .map(|t| PendingActivity {
            activity_exec_id: t.id.to_string(),
            activity_name: t.activity_name.unwrap_or_default(),
            queue: t.queue_name,
            scheduled_at: t.scheduled_at,
            attempt: t.attempt,
            max_attempts: t.max_attempts,
            task_status: t.state,
            claimed_by_worker_id: t.worker_id,
            last_heartbeat_at: t.last_heartbeat_at,
            next_retry_at: None,
            schedule_to_start_deadline: t.schedule_to_start.map(|d| t.scheduled_at + d),
            start_to_close_deadline: t.started_at.zip(t.start_to_close).map(|(s, d)| s + d),
            heartbeat_deadline: t
                .last_heartbeat_at
                .zip(t.heartbeat_timeout)
                .map(|(h, d)| h + d),
        })
        .collect::<Vec<_>>();
    let external_pending = harvest_external_tasks::table
        .filter(harvest_external_tasks::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_external_tasks::state.eq("PENDING"))
        .select(autumn_harvest::models::ExternalTask::as_select())
        .load::<autumn_harvest::models::ExternalTask>(&mut conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .map(|task| PendingActivity {
            activity_exec_id: task.activity_id.to_string(),
            activity_name: task.name,
            queue: task.queue,
            scheduled_at: task.created_at,
            attempt: 1,
            max_attempts: 1,
            task_status: "PENDING".to_string(),
            claimed_by_worker_id: None,
            last_heartbeat_at: None,
            next_retry_at: None,
            schedule_to_start_deadline: None,
            start_to_close_deadline: Some(task.schedule_to_close_at),
            heartbeat_deadline: None,
        })
        .collect::<Vec<_>>();
    let mut pending_activities = pending_activities;
    pending_activities.extend(external_pending);
    let pending_local_activities = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_events::event_type.eq_any([
            "LocalActivityScheduled",
            "LocalActivityCompleted",
            "LocalActivityFailed",
        ]))
        .order(harvest_events::event_id.asc())
        .select((
            harvest_events::event_type,
            harvest_events::event_data,
            harvest_events::timestamp,
        ))
        .load::<(String, serde_json::Value, chrono::DateTime<chrono::Utc>)>(&mut conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .fold(
            std::collections::BTreeMap::<String, PendingLocalActivity>::new(),
            |mut acc, (event_type, event_data, ts)| {
                let activity_id = event_data
                    .get("activity_id")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned);
                match (event_type.as_str(), activity_id) {
                    ("LocalActivityScheduled", Some(activity_exec_id)) => {
                        let activity_name = event_data
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        acc.insert(
                            activity_exec_id.clone(),
                            PendingLocalActivity {
                                activity_exec_id,
                                activity_name,
                                scheduled_at: ts,
                                attempt: 1,
                                max_attempts: 1,
                                task_status: "PENDING".to_string(),
                                last_heartbeat_at: None,
                                next_retry_at: None,
                                start_to_close_deadline: None,
                                heartbeat_deadline: None,
                            },
                        );
                    }
                    ("LocalActivityCompleted", Some(activity_exec_id)) => {
                        acc.remove(&activity_exec_id);
                    }
                    _ => {}
                }
                acc
            },
        )
        .into_values()
        .collect::<Vec<_>>();
    let pending_timers = harvest_timers::table
        .filter(harvest_timers::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_timers::fired.eq(false))
        .select((harvest_timers::timer_id, harvest_timers::fires_at))
        .load::<(String, chrono::DateTime<chrono::Utc>)>(&mut conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .map(|(timer_id, fires_at)| PendingTimer {
            timer_id,
            name: None,
            created_at: None,
            fires_at,
        })
        .collect::<Vec<_>>();
    let buffered_signals = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_signals::consumed.eq(false))
        .select((harvest_signals::signal_name, harvest_signals::received_at))
        .load::<(String, chrono::DateTime<chrono::Utc>)>(&mut conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .fold(
            std::collections::BTreeMap::<String, (i64, chrono::DateTime<chrono::Utc>)>::new(),
            |mut acc, (name, ts)| {
                acc.entry(name)
                    .and_modify(|entry| {
                        entry.0 += 1;
                        if ts < entry.1 {
                            entry.1 = ts;
                        }
                    })
                    .or_insert((1, ts));
                acc
            },
        )
        .into_iter()
        .map(
            |(signal_name, (buffered, oldest_received_at))| BufferedSignal {
                signal_name,
                buffered,
                oldest_received_at,
            },
        )
        .collect::<Vec<_>>();
    // Signal waiters are not directly materialized in a dedicated table today.
    // Avoid inferring waiters from generic workflow-task rows because parked
    // workflow tasks represent several suspension causes (timers, activities,
    // children, etc.) and would over-report signal waits.
    let pending_signals = Vec::new();
    let pending_child_workflows = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(exec_uuid)))
        .filter(harvest_workflow_executions::state.ne_all([
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
        ]))
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::workflow_name,
            harvest_workflow_executions::state,
        ))
        .load::<(uuid::Uuid, String, String)>(&mut conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .map(
            |(child_exec_id, child_workflow_name, state)| PendingChildWorkflow {
                child_exec_id: child_exec_id.to_string(),
                child_workflow_name,
                state,
            },
        )
        .collect::<Vec<_>>();
    Ok(Json(WorkflowStackResponse {
        exec_id: exec_id.to_string(),
        workflow_id: execution.workflow_id,
        workflow_name: execution.workflow_name,
        state: execution.state,
        is_terminal,
        pending_activities,
        pending_local_activities,
        pending_timers,
        pending_signals,
        buffered_signals,
        pending_child_workflows,
        last_event_id,
    }))
}

#[allow(clippy::too_many_lines)]
async fn start_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(workflow_name): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<StartWorkflowRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let runtime = match api_state.runtime() {
        Ok(r) => r,
        Err(e) => return map_error(e).into_response(),
    };

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /workflows/{workflow_name}/start";

    if !runtime.registry.workflows.contains_key(&workflow_name) {
        if let Ok(pool) = api_state.storage_pool()
            && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
        {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(workflow_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("workflow not registered"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return AutumnError::not_found_msg(format!("workflow '{workflow_name}'")).into_response();
    }

    let reuse_policy = match parse_reuse_policy(request.reuse_policy.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_WORKFLOW_START,
                    target_type: TARGET_WORKFLOW,
                    target_id: Some(workflow_name.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("invalid reuse policy"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return e.into_response();
        }
    };
    let workflow_id = request
        .workflow_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let queue_name = request
        .queue
        .or_else(|| runtime.queues.as_slice().first().cloned())
        .unwrap_or_else(|| "default".to_string());
    let input = request.input.unwrap_or(Value::Null);

    let shard = runtime
        .router
        .pick_for_new_workflow(&workflow_name, &workflow_id);
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = match db_conn_for_shard(&api_state, shard).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    // ADR-0001 §2.3: harvest.workflow.schedule — PRODUCER, parent = active HTTP span.
    // Emitted synchronously before the DB await so EnteredSpan (!Send) is dropped
    // before the async boundary.
    let trace_ctx = tracing::info_span!(
        "harvest.workflow.schedule",
        "otel.kind" = "producer",
        { ATTR_WORKFLOW_ID } = %workflow_name,
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_SHARD_ID } = i64::from(shard.as_i32()),
        { ATTR_QUEUE } = %queue_name,
    )
    .in_scope(|| runtime.registry.telemetry().capture_trace_context());

    let result = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: &workflow_name,
            workflow_id: &workflow_id,
            exec_id,
            input,
            parent_id: None,
            queue_name: &queue_name,
            execution_timeout: request
                .execution_timeout_secs
                .map(chrono::Duration::seconds),
            memo: request.memo.clone(),
            search_attrs: request.search_attrs.clone(),
            reuse_policy,
            trace_context: trace_ctx,
        },
    )
    .await;

    match result {
        Err(HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state,
        }) => {
            // AlreadyExists is a non-error outcome for some reuse policies;
            // record it as a failed audit so the caller can see the conflict.
            let exec_id_str = existing_exec_id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(exec_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("workflow already exists"),
                shard_id: Some(shard.as_i32()),
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            (
                axum::http::StatusCode::CONFLICT,
                Json(AlreadyExistsResponse {
                    existing_execution_id: existing_exec_id.to_string(),
                    existing_state,
                }),
            )
                .into_response()
        }
        Err(e) => {
            let err_str = e.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_START,
                target_type: TARGET_WORKFLOW,
                target_id: None,
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: Some(shard.as_i32()),
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            map_error(e).into_response()
        }
        Ok(start) => {
            let exec_id_str = start.exec_id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(exec_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: Some(shard.as_i32()),
                source: &source,
            };
            if let Err(audit_err) = audit::insert_audit(&mut conn, &ar).await {
                tracing::error!(error = %audit_err, "audit insert failed for workflow.start");
                return AutumnError::service_unavailable_msg(format!(
                    "audit insert failed: {audit_err}"
                ))
                .into_response();
            }
            (
                if start.created {
                    axum::http::StatusCode::CREATED
                } else {
                    axum::http::StatusCode::OK
                },
                Json(StartWorkflowResponse {
                    execution_id: start.exec_id.to_string(),
                    workflow_name: start.workflow_name,
                    workflow_id: start.workflow_id,
                    state: start.state,
                }),
            )
                .into_response()
        }
    }
}

async fn cancel_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CancelWorkflowRequest>,
) -> Result<(axum::http::StatusCode, Json<CancelWorkflowResponse>), AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /workflows/{id}/cancel";

    let exec_id = match parse_execution_id(&id) {
        Ok(eid) => eid,
        Err(e) => {
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_WORKFLOW_CANCEL,
                    target_type: TARGET_WORKFLOW,
                    target_id: Some(id.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("malformed execution id"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return Err(e);
        }
    };
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let reason = request
        .reason
        .as_deref()
        .unwrap_or("workflow cancellation requested");
    let exec_id_str = exec_id.to_string();

    let cancel_result = cancel_workflow_execution(&mut conn, exec_id, reason).await;
    match cancel_result {
        Err(e) => {
            let err_str = e.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_CANCEL,
                target_type: TARGET_WORKFLOW,
                target_id: Some(exec_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            Err(map_error(e))
        }
        Ok(cancelled) => {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_CANCEL,
                target_type: TARGET_WORKFLOW,
                target_id: Some(exec_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: &source,
            };
            audit::insert_audit(&mut conn, &ar)
                .await
                .map_err(map_error)?;
            Ok((
                axum::http::StatusCode::ACCEPTED,
                Json(CancelWorkflowResponse {
                    ok: true,
                    execution_id: cancelled.exec_id.to_string(),
                    state: cancelled.state,
                    reason: cancelled.reason,
                    newly_cancelled: cancelled.newly_cancelled,
                    failed_task_count: cancelled.failed_task_count,
                }),
            ))
        }
    }
}

async fn reset_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    Query(query): Query<ResetWorkflowQuery>,
    headers: axum::http::HeaderMap,
    Json(request): Json<WorkflowResetRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /workflows/{id}/reset";

    let exec_id = match parse_execution_id(&id) {
        Ok(eid) => eid,
        Err(e) => {
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_WORKFLOW_RESET,
                    target_type: TARGET_WORKFLOW,
                    target_id: Some(id.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("malformed execution id"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return e.into_response();
        }
    };
    let mut conn = match db_conn_for_execution(&api_state, exec_id).await {
        Ok(conn) => conn,
        Err(e) => return e.into_response(),
    };

    // Dry-run previews are read-only: no audit record needed.
    if query.dry_run {
        return match preview_workflow_reset(&mut conn, exec_id, request).await {
            Ok(plan) => (axum::http::StatusCode::OK, Json(plan)).into_response(),
            Err(error) => reset_error_response(error),
        };
    }

    let exec_id_str = exec_id.to_string();

    match reset_workflow_execution(&mut conn, exec_id, request).await {
        Ok(result) => {
            let new_exec_id_str = result.new_exec_id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_RESET,
                target_type: TARGET_WORKFLOW,
                target_id: Some(exec_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: &source,
            };
            if let Err(audit_err) = audit::insert_audit(&mut conn, &ar).await {
                tracing::error!(error = %audit_err, new_exec_id = %new_exec_id_str, "audit insert failed for workflow.reset");
                return AutumnError::service_unavailable_msg(format!(
                    "audit insert failed: {audit_err}"
                ))
                .into_response();
            }
            (
                axum::http::StatusCode::CREATED,
                Json(ResetWorkflowResponse::from(result)),
            )
                .into_response()
        }
        Err(error) => {
            let err_str = format!("{error:?}");
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_RESET,
                target_type: TARGET_WORKFLOW,
                target_id: Some(exec_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            reset_error_response(error)
        }
    }
}

fn reset_error_response(error: WorkflowResetError) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    match error {
        WorkflowResetError::InvalidPoint(invalid) => reset_invalid_point_response(invalid),
        WorkflowResetError::TerminalSource { exec_id, state } => (
            axum::http::StatusCode::CONFLICT,
            Json(ResetErrorResponse {
                message: format!("workflow execution {exec_id} is terminal ({state})"),
            }),
        )
            .into_response(),
        WorkflowResetError::ChildWorkflow { exec_id, parent_id } => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(ResetErrorResponse {
                message: format!(
                    "workflow execution {exec_id} is a child workflow of {parent_id}; reset the root parent in v1"
                ),
            }),
        )
            .into_response(),
        WorkflowResetError::ContinueAsNew => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(ResetErrorResponse {
                message: "continue-as-new histories cannot be reset in v1".to_string(),
            }),
        )
            .into_response(),
        WorkflowResetError::Harvest(error) => map_error(error).into_response(),
    }
}

fn reset_invalid_point_response(invalid: ResetInvalidPoint) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    (axum::http::StatusCode::BAD_REQUEST, Json(invalid)).into_response()
}

async fn signal_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, signal_name)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<Value>,
) -> Result<(axum::http::StatusCode, Json<BasicAck>), AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /workflows/{id}/signal/{signal_name}";

    let exec_id = match parse_execution_id(&id) {
        Ok(eid) => eid,
        Err(e) => {
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_WORKFLOW_SIGNAL,
                    target_type: TARGET_WORKFLOW,
                    target_id: Some(id.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("malformed execution id"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return Err(e);
        }
    };
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let exec_id_str = exec_id.to_string();

    if let Err(e) = load_execution(&mut conn, exec_id).await {
        let err_str = e.to_string();
        let ar = NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_SIGNAL,
            target_type: TARGET_WORKFLOW,
            target_id: Some(exec_id_str.as_str()),
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_FAILED,
            error_summary: Some(err_str.as_str()),
            shard_id: None,
            source: &source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
        return Err(map_error(e));
    }

    // Signal payload is intentionally not stored in the audit record (no PII).
    let signal_result = signal::send_signal(&mut conn, exec_id, &signal_name, payload).await;

    if let Err(e) = signal_result {
        let err_str = e.to_string();
        let ar = NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_SIGNAL,
            target_type: TARGET_WORKFLOW,
            target_id: Some(exec_id_str.as_str()),
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_FAILED,
            error_summary: Some(err_str.as_str()),
            shard_id: None,
            source: &source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
        return Err(map_error(e));
    }
    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_WORKFLOW_SIGNAL,
        target_type: TARGET_WORKFLOW,
        target_id: Some(exec_id_str.as_str()),
        route_or_command: route,
        request_id: request_id.as_deref(),
        idempotency_key: None,
        status: STATUS_SUCCEEDED,
        error_summary: None,
        shard_id: None,
        source: &source,
    };
    audit::insert_audit(&mut conn, &ar)
        .await
        .map_err(map_error)?;
    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(BasicAck { ok: true }),
    ))
}

async fn query_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, query_name)): Path<(String, String)>,
) -> Result<Json<Value>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let workflow = runtime
        .registry
        .workflows
        .get(&execution.workflow_name)
        .ok_or_else(|| {
            AutumnError::not_found_msg(format!(
                "workflow handler '{}' is not registered",
                execution.workflow_name
            ))
        })?;
    let history = store::load_history(&mut conn, exec_id)
        .await
        .map_err(map_error)?;

    let ctx = WorkflowContext::for_replay_with_state(
        exec_id,
        history.events,
        runtime.registry.shared_state(),
    );
    let _ = tokio::time::timeout(
        Duration::from_millis(100),
        (workflow.handler)(&ctx, execution.input.clone()),
    )
    .await;

    ctx.execute_query(&query_name).map(Json).map_err(map_error)
}

async fn list_dags(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<DagSummary>>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let schedules = load_schedules_from_shards(&api_state).await?;

    let dags = schedules
        .into_iter()
        .filter_map(|schedule| {
            let dag_name = schedule.dag_name.clone()?; // skip workflow-only rows
            Some(DagSummary {
                name: dag_name.clone(),
                schedule_expr: schedule.schedule_expr.clone(),
                is_paused: schedule.is_paused,
                next_run_at: schedule.next_run_at,
                max_active_runs: schedule.max_active_runs,
                catchup: schedule.catchup,
                task_count: runtime
                    .dags
                    .get(&dag_name)
                    .map_or(0, RegisteredDag::task_count),
            })
        })
        .collect();

    Ok(Json(dags))
}

async fn list_dag_runs(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
) -> Result<Json<Vec<DagRun>>, AutumnError> {
    let mut conn = db_conn_for_dag(&api_state, &dag_name).await?;
    let runs = harvest_dag_runs::table
        .filter(harvest_dag_runs::dag_name.eq(&dag_name))
        .order(harvest_dag_runs::created_at.desc())
        .select(DagRun::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    Ok(Json(runs))
}

async fn trigger_dag_run(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<DagTriggerRequest>,
) -> Result<(axum::http::StatusCode, Json<DagRun>), AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let shard = runtime.router.pick_for_dag(&dag_name);

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /dags/{dag_name}/trigger";

    let trigger_result = trigger_dag(
        pool.pool_for(shard).clone(),
        Arc::clone(&runtime.registry),
        Arc::clone(&runtime.dags),
        &dag_name,
        request.conf,
        runtime.scheduler,
    )
    .await;

    let mut audit_conn = acquire_conn(pool.pool_for(shard)).await?;

    match trigger_result {
        Err(e) => {
            let err_str = e.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_DAG_TRIGGER,
                target_type: TARGET_DAG,
                target_id: Some(dag_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: Some(shard.as_i32()),
                source: &source,
            };
            let _ = audit::insert_audit(&mut audit_conn, &ar).await;
            Err(map_error(e))
        }
        Ok(run) => {
            let run_id_str = run.id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_DAG_TRIGGER,
                target_type: TARGET_DAG,
                target_id: Some(dag_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: Some(shard.as_i32()),
                source: &source,
            };
            if let Err(audit_err) = audit::insert_audit(&mut audit_conn, &ar).await {
                tracing::error!(error = %audit_err, run_id = %run_id_str, "audit insert failed for dag.trigger");
                return Err(AutumnError::service_unavailable_msg(format!(
                    "audit insert failed: {audit_err}"
                )));
            }
            Ok((axum::http::StatusCode::CREATED, Json(run)))
        }
    }
}

async fn patch_dag(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<DagPauseRequest>,
) -> Result<Json<HarvestSchedule>, AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let mut conn = db_conn_for_dag(&api_state, &dag_name).await?;

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "PATCH /dags/{dag_name}";

    let update_result = diesel::update(dsl::harvest_schedules.filter(dsl::dag_name.eq(&dag_name)))
        .set((
            dsl::is_paused.eq(request.paused),
            dsl::updated_at.eq(chrono::Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .map_err(database_error);

    match update_result {
        Err(e) => {
            let err_str = e.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_DAG_PATCH,
                target_type: TARGET_DAG,
                target_id: Some(dag_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            Err(map_error(e))
        }
        Ok(0) => {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_DAG_PATCH,
                target_type: TARGET_DAG,
                target_id: Some(dag_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("dag not found"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            Err(AutumnError::not_found_msg(format!("dag '{dag_name}'")))
        }
        Ok(_) => {
            let schedule = dsl::harvest_schedules
                .filter(dsl::dag_name.eq(&dag_name))
                .select(HarvestSchedule::as_select())
                .first(&mut conn)
                .await
                .map_err(database_error)
                .map_err(map_error)?;

            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_DAG_PATCH,
                target_type: TARGET_DAG,
                target_id: Some(dag_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: &source,
            };
            audit::insert_audit(&mut conn, &ar)
                .await
                .map_err(map_error)?;
            Ok(Json(schedule))
        }
    }
}

async fn list_schedules(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<ScheduleEntry>>, AutumnError> {
    let schedules = load_schedules_from_shards(&api_state).await?;
    let entries = schedules
        .into_iter()
        .map(|s| {
            let (kind, name) = if let Some(ref dag_name) = s.dag_name {
                (ScheduleKind::Dag, dag_name.clone())
            } else if let Some(ref wf_name) = s.workflow_name {
                (ScheduleKind::Workflow, wf_name.clone())
            } else {
                // Should not occur given the CHECK constraint, but handle gracefully.
                (ScheduleKind::Dag, String::new())
            };
            ScheduleEntry {
                id: s.id,
                kind,
                name,
                schedule_expr: s.schedule_expr,
                is_paused: s.is_paused,
                next_run_at: s.next_run_at,
                last_run_at: s.last_run_at,
                max_active_runs: s.max_active_runs,
                catchup: s.catchup,
            }
        })
        .collect();
    Ok(Json(entries))
}

async fn schedule_create_audit_failed(
    api_state: &HarvestApiState,
    actor: &str,
    source: &str,
    request_id: Option<&str>,
    workflow_name: &str,
    error_summary: &str,
) {
    let Ok(pool) = api_state.storage_pool() else {
        return;
    };
    let Ok(mut conn) = acquire_conn(pool.default_pool()).await else {
        return;
    };
    let ar = NewAuditRecord {
        actor,
        operation: OP_SCHEDULE_CREATE,
        target_type: TARGET_SCHEDULE,
        target_id: Some(workflow_name),
        route_or_command: "POST /admin/schedules/workflow",
        request_id,
        idempotency_key: None,
        status: STATUS_FAILED,
        error_summary: Some(error_summary),
        shard_id: None,
        source,
    };
    let _ = audit::insert_audit(&mut conn, &ar).await;
}

async fn upsert_workflow_schedule_and_read_back(
    conn: &mut diesel_async::AsyncPgConnection,
    ws: &WorkflowSchedule,
) -> Result<ScheduleEntry, AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;
    autumn_harvest::register_workflow_schedules(conn, std::slice::from_ref(ws))
        .await
        .map_err(map_error)?;
    let row: autumn_harvest::models::HarvestSchedule = dsl::harvest_schedules
        .filter(dsl::workflow_name.eq(&ws.workflow_name))
        .select(autumn_harvest::models::HarvestSchedule::as_select())
        .first(conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    Ok(ScheduleEntry {
        id: row.id,
        kind: ScheduleKind::Workflow,
        name: ws.workflow_name.clone(),
        schedule_expr: row.schedule_expr,
        is_paused: row.is_paused,
        next_run_at: row.next_run_at,
        last_run_at: row.last_run_at,
        max_active_runs: row.max_active_runs,
        catchup: row.catchup,
    })
}

async fn create_workflow_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CreateWorkflowScheduleRequest>,
) -> Result<(axum::http::StatusCode, Json<ScheduleEntry>), AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /admin/schedules/workflow";

    if !runtime
        .registry
        .workflows
        .contains_key(&request.workflow_name)
    {
        let registered: Vec<&str> = runtime
            .registry
            .workflows
            .keys()
            .map(String::as_str)
            .collect();
        schedule_create_audit_failed(
            &api_state,
            &actor,
            &source,
            request_id.as_deref(),
            &request.workflow_name,
            "workflow not registered",
        )
        .await;
        return Err(AutumnError::not_found_msg(format!(
            "workflow '{}' is not registered; registered: {:?}",
            request.workflow_name, registered
        )));
    }

    let schedule = match parse_schedule_expr(&request.schedule_expr) {
        Ok(s) => s,
        Err(e) => {
            let err_summary = format!("invalid schedule_expr: {e}");
            schedule_create_audit_failed(
                &api_state,
                &actor,
                &source,
                request_id.as_deref(),
                &request.workflow_name,
                &err_summary,
            )
            .await;
            return Err(AutumnError::bad_request_msg(err_summary));
        }
    };

    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.pool_for(runtime.router().default_shard())).await?;

    let ws = WorkflowSchedule {
        workflow_name: request.workflow_name.clone(),
        schedule,
        input: request.input.clone(),
        catchup: request.catchup,
        max_active_runs: request.max_active_runs,
        paused: request.paused,
        queue_name: request.queue_name.clone(),
    };
    let entry = match upsert_workflow_schedule_and_read_back(&mut conn, &ws).await {
        Ok(e) => e,
        Err(e) => {
            let err_str = e.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_SCHEDULE_CREATE,
                target_type: TARGET_SCHEDULE,
                target_id: Some(request.workflow_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            return Err(e);
        }
    };

    let entry_id_str = entry.id.to_string();
    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_SCHEDULE_CREATE,
        target_type: TARGET_SCHEDULE,
        target_id: Some(entry_id_str.as_str()),
        route_or_command: route,
        request_id: request_id.as_deref(),
        idempotency_key: None,
        status: STATUS_SUCCEEDED,
        error_summary: None,
        shard_id: None,
        source: &source,
    };
    audit::insert_audit(&mut conn, &ar)
        .await
        .map_err(map_error)?;

    Ok((axum::http::StatusCode::CREATED, Json(entry)))
}

async fn pause_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Json<BasicAck>, AutumnError> {
    set_schedule_paused(&api_state, &id, true, &headers).await
}

async fn resume_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Json<BasicAck>, AutumnError> {
    set_schedule_paused(&api_state, &id, false, &headers).await
}

async fn set_schedule_paused(
    api_state: &HarvestApiState,
    id_str: &str,
    paused: bool,
    headers: &axum::http::HeaderMap,
) -> Result<Json<BasicAck>, AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let (actor, source, request_id) = audit_context(headers, api_state);
    let operation = if paused {
        OP_SCHEDULE_PAUSE
    } else {
        OP_SCHEDULE_RESUME
    };
    let route = if paused {
        "POST /admin/schedules/{id}/pause"
    } else {
        "POST /admin/schedules/{id}/resume"
    };

    let id = match parse_uuid(id_str, "schedule id") {
        Ok(u) => u,
        Err(e) => {
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation,
                    target_type: TARGET_SCHEDULE,
                    target_id: Some(id_str),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("malformed schedule id"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return Err(e);
        }
    };
    let pool = api_state.storage_pool().map_err(map_error)?;
    let id_str_owned = id.to_string();

    let mut updated_count = 0usize;
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let n = diesel::update(dsl::harvest_schedules.find(id))
            .set((
                dsl::is_paused.eq(paused),
                dsl::updated_at.eq(chrono::Utc::now()),
            ))
            .execute(&mut conn)
            .await
            .map_err(database_error)
            .map_err(map_error)?;
        updated_count += n;
        if updated_count > 0 {
            let ar = NewAuditRecord {
                actor: &actor,
                operation,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id_str_owned.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: &source,
            };
            audit::insert_audit(&mut conn, &ar)
                .await
                .map_err(map_error)?;
            break;
        }
    }

    if updated_count == 0 {
        if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
            let ar = NewAuditRecord {
                actor: &actor,
                operation,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id_str_owned.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("schedule not found"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return Err(AutumnError::not_found_msg(format!("schedule {id}")));
    }
    Ok(Json(BasicAck { ok: true }))
}

async fn batch_submit_audit_failed(
    api_state: &HarvestApiState,
    actor: &str,
    source: &str,
    request_id: Option<&str>,
    idempotency_key: Option<&str>,
    error_summary: &str,
) {
    let Ok(pool) = api_state.storage_pool() else {
        return;
    };
    let Ok(mut conn) = acquire_conn(pool.default_pool()).await else {
        return;
    };
    let ar = NewAuditRecord {
        actor,
        operation: OP_BATCH_SUBMIT,
        target_type: TARGET_BATCH,
        target_id: None,
        route_or_command: "POST /batch-operations",
        request_id,
        idempotency_key,
        status: STATUS_FAILED,
        error_summary: Some(error_summary),
        shard_id: None,
        source,
    };
    let _ = audit::insert_audit(&mut conn, &ar).await;
}

async fn dlq_bulk_audit_reject_empty_filter(
    api_state: &HarvestApiState,
    actor: &str,
    source: &str,
    request_id: Option<&str>,
    operation: &'static str,
    route: &'static str,
) {
    let Ok(pool) = api_state.storage_pool() else {
        return;
    };
    let Ok(mut conn) = acquire_conn(pool.default_pool()).await else {
        return;
    };
    let ar = NewAuditRecord {
        actor,
        operation,
        target_type: TARGET_DEAD_LETTER,
        target_id: None,
        route_or_command: route,
        request_id,
        idempotency_key: None,
        status: STATUS_FAILED,
        error_summary: Some("empty bulk filter"),
        shard_id: None,
        source,
    };
    let _ = audit::insert_audit(&mut conn, &ar).await;
}

async fn schedule_delete_audit_failed(
    pool: &HarvestDbPool,
    actor: &str,
    source: &str,
    request_id: Option<&str>,
    id_str: &str,
    error_summary: &'static str,
) {
    if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
        let ar = NewAuditRecord {
            actor,
            operation: OP_SCHEDULE_DELETE,
            target_type: TARGET_SCHEDULE,
            target_id: Some(id_str),
            route_or_command: "DELETE /admin/schedules/{id}",
            request_id,
            idempotency_key: None,
            status: STATUS_FAILED,
            error_summary: Some(error_summary),
            shard_id: None,
            source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
    }
}

#[allow(clippy::too_many_lines)]
async fn delete_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Json<BasicAck>, AutumnError> {
    use autumn_harvest::models::HarvestSchedule;
    use autumn_harvest::schema::harvest_schedules::dsl;

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "DELETE /admin/schedules/{id}";
    let pool = api_state.storage_pool().map_err(map_error)?;

    let id = match parse_uuid(&id, "schedule id") {
        Ok(u) => u,
        Err(e) => {
            schedule_delete_audit_failed(
                &pool,
                &actor,
                &source,
                request_id.as_deref(),
                &id,
                "malformed schedule id",
            )
            .await;
            return Err(e);
        }
    };
    let id_str = id.to_string();

    let mut deleted_count = 0usize;
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let row: Option<HarvestSchedule> = dsl::harvest_schedules
            .find(id)
            .select(HarvestSchedule::as_select())
            .first(&mut conn)
            .await
            .optional()
            .map_err(database_error)
            .map_err(map_error)?;
        let Some(row) = row else {
            continue;
        };
        if row.dag_name.is_some() {
            schedule_delete_audit_failed(
                &pool,
                &actor,
                &source,
                request_id.as_deref(),
                &id_str,
                "dag-managed schedule cannot be deleted via API",
            )
            .await;
            return Err(AutumnError::bad_request_msg(format!(
                "schedule {id} is managed by the DAG catalog and cannot be deleted via API; \
                 remove the DAG definition to stop scheduling"
            )));
        }
        if let Some(ref wf_name) = row.workflow_name {
            let runtime = api_state.runtime().map_err(map_error)?;
            let is_code_managed = runtime
                .workflow_schedules()
                .iter()
                .any(|ws| ws.workflow_name == *wf_name);
            if is_code_managed {
                schedule_delete_audit_failed(
                    &pool,
                    &actor,
                    &source,
                    request_id.as_deref(),
                    &id_str,
                    "code-managed schedule cannot be deleted via API",
                )
                .await;
                return Err(AutumnError::bad_request_msg(format!(
                    "schedule {id} is managed by the in-process workflow schedule catalog \
                     and cannot be deleted via API; it will be re-created on the next \
                     scheduler tick"
                )));
            }
        }
        let n = diesel::delete(dsl::harvest_schedules.find(id))
            .execute(&mut conn)
            .await
            .map_err(database_error)
            .map_err(map_error)?;
        deleted_count += n;
        if deleted_count > 0 {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_SCHEDULE_DELETE,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: &source,
            };
            audit::insert_audit(&mut conn, &ar)
                .await
                .map_err(map_error)?;
            break;
        }
    }

    if deleted_count == 0 {
        schedule_delete_audit_failed(
            &pool,
            &actor,
            &source,
            request_id.as_deref(),
            &id_str,
            "schedule not found",
        )
        .await;
        return Err(AutumnError::not_found_msg(format!("schedule {id}")));
    }
    Ok(Json(BasicAck { ok: true }))
}

fn parse_schedule_expr(expr: &str) -> Result<autumn_harvest::policy::Schedule, String> {
    use autumn_harvest::policy::Schedule;

    let trimmed = expr.trim();
    let schedule = if let Some(cron) = trimmed.strip_prefix("cron:") {
        Schedule::Cron(cron.trim().to_string())
    } else if let Some(secs_str) = trimmed.strip_prefix("interval:") {
        let secs: u64 = secs_str
            .trim()
            .parse()
            .map_err(|_| format!("invalid interval seconds '{secs_str}'"))?;
        if secs == 0 {
            return Err("interval must be at least 1 second".to_string());
        }
        Schedule::Interval(std::time::Duration::from_secs(secs))
    } else if trimmed == "manual" {
        Schedule::Manual
    } else {
        // Treat a bare expression as a cron string for convenience.
        Schedule::Cron(trimmed.to_string())
    };
    // Validate cron expressions eagerly so callers receive a 400 rather than
    // silently persisting an expression that will never fire.
    autumn_harvest::validate_schedule(&schedule)?;
    Ok(schedule)
}

async fn list_dead_letters(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<DeadLetterListQuery>,
) -> Result<Json<Vec<DeadLetter>>, AutumnError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let dead_letters = load_dead_letters_from_shards(&api_state, limit).await?;
    Ok(Json(dead_letters))
}

async fn replay_dead_letter(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<(axum::http::StatusCode, Json<ReplayDeadLetterResponse>), AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /dead-letters/{id}/replay";

    let dead_letter_id = match parse_uuid(&id, "dead-letter id") {
        Ok(u) => u,
        Err(e) => {
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_DLQ_REPLAY,
                    target_type: TARGET_DEAD_LETTER,
                    target_id: Some(id.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("malformed dead-letter id"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return Err(e);
        }
    };
    let dl_id_str = dead_letter_id.to_string();

    let replay_result = replay_dead_letter_from_shards(&api_state, dead_letter_id).await;

    // For the single-replay path we insert the audit record on the default pool
    // since the DLQ entry may live on any shard and we don't track which one.
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut audit_conn = acquire_conn(pool.default_pool()).await?;

    match replay_result {
        Err(e) => {
            let err_str = e.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_DLQ_REPLAY,
                target_type: TARGET_DEAD_LETTER,
                target_id: Some(dl_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut audit_conn, &ar).await;
            Err(e)
        }
        Ok(task_id) => {
            let task_id_str = task_id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_DLQ_REPLAY,
                target_type: TARGET_DEAD_LETTER,
                target_id: Some(dl_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: &source,
            };
            audit::insert_audit(&mut audit_conn, &ar)
                .await
                .map_err(map_error)?;
            Ok((
                axum::http::StatusCode::ACCEPTED,
                Json(ReplayDeadLetterResponse {
                    ok: true,
                    dead_letter_id: dl_id_str,
                    task_id: task_id_str,
                }),
            ))
        }
    }
}

async fn bulk_replay_dead_letters_handler(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Json(filter): Json<dlq::BulkDlqFilter>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /dead-letters/replay";

    if filter.is_empty() {
        dlq_bulk_audit_reject_empty_filter(
            &api_state,
            &actor,
            &source,
            request_id.as_deref(),
            OP_DLQ_REPLAY_BULK,
            route,
        )
        .await;
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "bulk filter must specify at least one criterion: \
                          activity_name, workflow_name, failed_after, or failed_before"
            })),
        )
            .into_response();
    }

    // Dry-run previews are read-only: no audit record needed.
    if filter.dry_run {
        return match bulk_replay_from_shards(&api_state, &filter).await {
            Ok(result) => (axum::http::StatusCode::OK, Json(result)).into_response(),
            Err(e) => map_error(e).into_response(),
        };
    }

    let replay_result = bulk_replay_from_shards(&api_state, &filter).await;
    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return map_error(e).into_response(),
    };
    let audit_conn = acquire_conn(pool.default_pool()).await;

    match replay_result {
        Ok(result) => {
            let status = if result.acted_on == 0 && !result.failures.is_empty() {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            } else {
                axum::http::StatusCode::OK
            };
            let (audit_status, audit_error) = if result.failures.is_empty() {
                (STATUS_SUCCEEDED, None)
            } else {
                (
                    STATUS_FAILED,
                    Some(format!("{} failures", result.failures.len())),
                )
            };
            match audit_conn {
                Ok(mut conn) => {
                    let ar = NewAuditRecord {
                        actor: &actor,
                        operation: OP_DLQ_REPLAY_BULK,
                        target_type: TARGET_DEAD_LETTER,
                        target_id: None,
                        route_or_command: route,
                        request_id: request_id.as_deref(),
                        idempotency_key: None,
                        status: audit_status,
                        error_summary: audit_error.as_deref(),
                        shard_id: None,
                        source: &source,
                    };
                    if let Err(e) = audit::insert_audit(&mut conn, &ar).await {
                        return map_error(e).into_response();
                    }
                }
                Err(e) => return e.into_response(),
            }
            (status, Json(result)).into_response()
        }
        Err(e) => {
            let err_str = e.to_string();
            if let Ok(mut conn) = audit_conn {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_DLQ_REPLAY_BULK,
                    target_type: TARGET_DEAD_LETTER,
                    target_id: None,
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some(err_str.as_str()),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            map_error(e).into_response()
        }
    }
}

async fn bulk_discard_dead_letters_handler(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Json(filter): Json<dlq::BulkDlqFilter>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /dead-letters/discard";

    if filter.is_empty() {
        dlq_bulk_audit_reject_empty_filter(
            &api_state,
            &actor,
            &source,
            request_id.as_deref(),
            OP_DLQ_DISCARD_BULK,
            route,
        )
        .await;
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "bulk filter must specify at least one criterion: \
                          activity_name, workflow_name, failed_after, or failed_before"
            })),
        )
            .into_response();
    }

    // Dry-run previews are read-only: no audit record needed.
    if filter.dry_run {
        return match bulk_discard_from_shards(&api_state, &filter).await {
            Ok(result) => (axum::http::StatusCode::OK, Json(result)).into_response(),
            Err(e) => map_error(e).into_response(),
        };
    }

    let discard_result = bulk_discard_from_shards(&api_state, &filter).await;
    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return map_error(e).into_response(),
    };
    let audit_conn = acquire_conn(pool.default_pool()).await;

    match discard_result {
        Ok(result) => {
            let status = if result.acted_on == 0 && !result.failures.is_empty() {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            } else {
                axum::http::StatusCode::OK
            };
            let (audit_status, audit_error) = if result.failures.is_empty() {
                (STATUS_SUCCEEDED, None)
            } else {
                (
                    STATUS_FAILED,
                    Some(format!("{} failures", result.failures.len())),
                )
            };
            match audit_conn {
                Ok(mut conn) => {
                    let ar = NewAuditRecord {
                        actor: &actor,
                        operation: OP_DLQ_DISCARD_BULK,
                        target_type: TARGET_DEAD_LETTER,
                        target_id: None,
                        route_or_command: route,
                        request_id: request_id.as_deref(),
                        idempotency_key: None,
                        status: audit_status,
                        error_summary: audit_error.as_deref(),
                        shard_id: None,
                        source: &source,
                    };
                    if let Err(e) = audit::insert_audit(&mut conn, &ar).await {
                        return map_error(e).into_response();
                    }
                }
                Err(e) => return e.into_response(),
            }
            (status, Json(result)).into_response()
        }
        Err(e) => {
            let err_str = e.to_string();
            if let Ok(mut conn) = audit_conn {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_DLQ_DISCARD_BULK,
                    target_type: TARGET_DEAD_LETTER,
                    target_id: None,
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some(err_str.as_str()),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            map_error(e).into_response()
        }
    }
}

async fn bulk_replay_from_shards(
    api_state: &HarvestApiState,
    filter: &dlq::BulkDlqFilter,
) -> Result<dlq::BulkDlqResult, HarvestError> {
    let pool = api_state.storage_pool()?;
    let mut total = dlq::BulkDlqResult {
        matched: 0,
        acted_on: 0,
        skipped: 0,
        ids: Vec::new(),
        dry_run: filter.dry_run,
        failures: Vec::new(),
    };

    // Enforce the limit as a global cap across all shards, not per-shard.
    // effective_limit() is guaranteed to be in [1, 1000] so both try_from
    // conversions below are infallible in practice.
    let mut remaining: u32 =
        u32::try_from(filter.effective_limit()).unwrap_or(dlq::DEFAULT_BULK_LIMIT);

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = shard_pool
            .get()
            .await
            .map_err(|e| HarvestError::Database(e.to_string()))?;

        if remaining == 0 {
            // Budget exhausted: count-only so matched reflects all shards.
            let shard_matched = dlq::count_bulk_filter_matches(&mut conn, filter)
                .await
                .map(|n| usize::try_from(n).unwrap_or(0))?;
            total.matched += shard_matched;
            continue;
        }

        let mut shard_filter = filter.clone();
        shard_filter.limit = Some(remaining);
        let shard_result = dlq::bulk_replay_dead_letters(&mut conn, &shard_filter).await?;
        // Rows consumed = acted + skipped + failed (or preview ids in dry-run).
        let consumed = shard_result.ids.len() + shard_result.skipped + shard_result.failures.len();
        remaining = remaining.saturating_sub(u32::try_from(consumed).unwrap_or(remaining));
        total.matched += shard_result.matched;
        total.acted_on += shard_result.acted_on;
        total.skipped += shard_result.skipped;
        total.ids.extend(shard_result.ids);
        total.failures.extend(shard_result.failures);
    }

    Ok(total)
}

async fn bulk_discard_from_shards(
    api_state: &HarvestApiState,
    filter: &dlq::BulkDlqFilter,
) -> Result<dlq::BulkDlqResult, HarvestError> {
    let pool = api_state.storage_pool()?;
    let mut total = dlq::BulkDlqResult {
        matched: 0,
        acted_on: 0,
        skipped: 0,
        ids: Vec::new(),
        dry_run: filter.dry_run,
        failures: Vec::new(),
    };

    // Enforce the limit as a global cap across all shards, not per-shard.
    let mut remaining: u32 =
        u32::try_from(filter.effective_limit()).unwrap_or(dlq::DEFAULT_BULK_LIMIT);

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = shard_pool
            .get()
            .await
            .map_err(|e| HarvestError::Database(e.to_string()))?;

        if remaining == 0 {
            // Budget exhausted: count-only so matched reflects all shards.
            let shard_matched = dlq::count_bulk_filter_matches(&mut conn, filter)
                .await
                .map(|n| usize::try_from(n).unwrap_or(0))?;
            total.matched += shard_matched;
            continue;
        }

        let mut shard_filter = filter.clone();
        shard_filter.limit = Some(remaining);
        let shard_result = dlq::bulk_discard_dead_letters(&mut conn, &shard_filter).await?;
        let consumed = shard_result.ids.len() + shard_result.skipped + shard_result.failures.len();
        remaining = remaining.saturating_sub(u32::try_from(consumed).unwrap_or(remaining));
        total.matched += shard_result.matched;
        total.acted_on += shard_result.acted_on;
        total.skipped += shard_result.skipped;
        total.ids.extend(shard_result.ids);
        total.failures.extend(shard_result.failures);
    }

    Ok(total)
}

async fn retention_status(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<RetentionStatus>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let status = runtime.retention.monitor.as_ref().map_or_else(
        || RetentionStatus {
            config: runtime.retention.config.clone(),
            per_shard: Vec::new(),
        },
        RetentionMonitor::snapshot,
    );
    Ok(Json(status))
}

async fn retention_run_now(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<BasicAck>, AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /admin/retention/run-now";

    let runtime = api_state.runtime().map_err(map_error)?;
    let Some(trigger) = runtime.retention.trigger.as_ref() else {
        if let Ok(pool) = api_state.storage_pool()
            && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
        {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_RETENTION_RUN_NOW,
                target_type: TARGET_RETENTION,
                target_id: None,
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("no local retention runtime owner"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return Err(AutumnError::service_unavailable_msg(
            "retention run-now unavailable: no local retention runtime owner",
        ));
    };

    let send_result = trigger.try_send(());

    let pool = api_state.storage_pool().map_err(map_error)?;
    if send_result.is_ok() {
        let mut conn = acquire_conn(pool.default_pool()).await?;
        let ar = NewAuditRecord {
            actor: &actor,
            operation: OP_RETENTION_RUN_NOW,
            target_type: TARGET_RETENTION,
            target_id: None,
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: &source,
        };
        audit::insert_audit(&mut conn, &ar)
            .await
            .map_err(map_error)?;
    } else if let Err(ref e) = send_result {
        let err_str = e.to_string();
        if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_RETENTION_RUN_NOW,
                target_type: TARGET_RETENTION,
                target_id: None,
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
    }

    send_result.map_err(|error| {
        AutumnError::service_unavailable_msg(format!(
            "retention run-now unavailable: failed to enqueue trigger ({error})"
        ))
    })?;
    Ok(Json(BasicAck { ok: true }))
}

async fn concurrency_status(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<ConcurrencyKeyStats>>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let pool = api_state.storage_pool().map_err(map_error)?;

    let mut merged: std::collections::HashMap<String, ConcurrencyKeyStats> =
        std::collections::HashMap::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let stats = queue::concurrency_key_stats(&mut conn, &runtime.queues)
            .await
            .map_err(map_error)?;
        for stat in stats {
            let entry = merged
                .entry(stat.key.clone())
                .or_insert_with(|| ConcurrencyKeyStats {
                    key: stat.key.clone(),
                    max_concurrent: stat.max_concurrent,
                    in_flight: 0,
                    pending: 0,
                });
            // Take the highest cap seen across shards, matching what
            // concurrency_key_stats() does within a shard (MAX(concurrency_cap)).
            entry.max_concurrent = entry.max_concurrent.max(stat.max_concurrent);
            entry.in_flight += stat.in_flight;
            entry.pending += stat.pending;
        }
    }

    let mut result: Vec<ConcurrencyKeyStats> = merged.into_values().collect();
    result.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(Json(result))
}

async fn health(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<HarvestHealth>, AutumnError> {
    let runtime = api_state.runtime().ok();
    let scheduler = runtime
        .as_ref()
        .map_or_else(SchedulerMonitor::offline, |runtime| {
            runtime.scheduler.clone()
        })
        .snapshot();

    Ok(Json(HarvestHealth {
        runtime_ready: runtime.is_some(),
        worker_id: runtime
            .as_ref()
            .and_then(|runtime| runtime.worker_id.clone()),
        queues: runtime
            .as_ref()
            .map_or_else(Vec::new, |runtime| runtime.queues.clone()),
        dag_count: runtime.as_ref().map_or(0, |runtime| runtime.dags.len()),
        scheduler,
    }))
}

pub(crate) async fn load_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<WorkflowExecution> {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
}

pub(crate) type PoolConn = deadpool::managed::Object<
    diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>,
>;

fn map_pool_error(error: &impl ToString) -> AutumnError {
    AutumnError::service_unavailable_msg(error.to_string())
}

pub(crate) async fn acquire_conn(pool: &DbPool) -> Result<PoolConn, AutumnError> {
    pool.get().await.map_err(|error| map_pool_error(&error))
}

pub(crate) async fn db_conn_for_execution(
    api_state: &HarvestApiState,
    exec_id: ExecutionId,
) -> Result<PoolConn, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    acquire_conn(pool.pool_for_execution(exec_id)).await
}

async fn db_conn_for_shard(
    api_state: &HarvestApiState,
    shard: ShardId,
) -> Result<PoolConn, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    acquire_conn(pool.pool_for(shard)).await
}

async fn db_conn_for_dag(
    api_state: &HarvestApiState,
    dag_name: &str,
) -> Result<PoolConn, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    db_conn_for_shard(api_state, runtime.router.pick_for_dag(dag_name)).await
}

pub(crate) async fn load_workflows(
    conn: &mut AsyncPgConnection,
    filters: &WorkflowFilters,
) -> HarvestResult<Vec<WorkflowExecution>> {
    use diesel::dsl::sql;
    use diesel::sql_types::{Bool, Jsonb};

    let mut query = harvest_workflow_executions::table
        .into_boxed()
        .order(harvest_workflow_executions::created_at.desc())
        .limit(filters.limit);
    if !filters.states.is_empty() {
        query = query.filter(harvest_workflow_executions::state.eq_any(filters.states.clone()));
    }
    if let Some(name) = &filters.workflow_name {
        query = query.filter(harvest_workflow_executions::workflow_name.eq(name.clone()));
    }
    // Each search_attr filter contributes its own `search_attrs @> {...}` predicate.
    // The `@>` operator hits the existing `idx_harvest_we_search` GIN index on
    // `search_attrs`; ANDing predicates means repeated keys narrow the result set.
    for predicate in &filters.search_attrs {
        query = query.filter(sql::<Bool>("search_attrs @> ").bind::<Jsonb, _>(predicate.clone()));
    }
    query
        .select(WorkflowExecution::as_select())
        .load(conn)
        .await
        .map_err(database_error)
}

pub(crate) async fn load_workflows_from_shards(
    api_state: &HarvestApiState,
    filters: &WorkflowFilters,
) -> Result<Vec<WorkflowExecution>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut workflows = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = load_workflows(&mut conn, filters)
            .await
            .map_err(map_error)?;
        workflows.append(&mut rows);
    }

    workflows.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    workflows.truncate(usize::try_from(filters.limit).unwrap_or(usize::MAX));
    Ok(workflows)
}

async fn load_schedules_from_shards(
    api_state: &HarvestApiState,
) -> Result<Vec<HarvestSchedule>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut schedules = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = harvest_schedules::table
            .order(harvest_schedules::dag_name.asc())
            .select(HarvestSchedule::as_select())
            .load(&mut conn)
            .await
            .map_err(database_error)
            .map_err(map_error)?;
        schedules.append(&mut rows);
    }

    schedules.sort_by(|left, right| {
        let left_name = left
            .dag_name
            .as_deref()
            .or(left.workflow_name.as_deref())
            .unwrap_or("");
        let right_name = right
            .dag_name
            .as_deref()
            .or(right.workflow_name.as_deref())
            .unwrap_or("");
        left_name
            .cmp(right_name)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(schedules)
}

async fn load_dead_letters_from_shards(
    api_state: &HarvestApiState,
    limit: i64,
) -> Result<Vec<DeadLetter>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut dead_letters = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = dlq::list_dead_letters(&mut conn, limit)
            .await
            .map_err(map_error)?;
        dead_letters.append(&mut rows);
    }

    dead_letters.sort_by(|left, right| {
        right
            .failed_at
            .cmp(&left.failed_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    dead_letters.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    Ok(dead_letters)
}

async fn replay_dead_letter_from_shards(
    api_state: &HarvestApiState,
    dead_letter_id: uuid::Uuid,
) -> Result<uuid::Uuid, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        match dlq::replay_dead_letter(&mut conn, dead_letter_id).await {
            Ok(task_id) => return Ok(task_id),
            Err(HarvestError::NotFound(_)) => {}
            Err(error) => return Err(map_error(error)),
        }
    }

    Err(AutumnError::not_found_msg(format!(
        "dead-letter {dead_letter_id}"
    )))
}

fn parse_reuse_policy(raw: Option<&str>) -> Result<WorkflowIdReusePolicy, AutumnError> {
    match raw {
        None | Some("" | "allow_duplicate") => Ok(WorkflowIdReusePolicy::AllowDuplicate),
        Some("reject_duplicate") => Ok(WorkflowIdReusePolicy::RejectDuplicate),
        Some("allow_duplicate_failed_only") => Ok(WorkflowIdReusePolicy::AllowDuplicateFailedOnly),
        Some("terminate_if_running") => Ok(WorkflowIdReusePolicy::TerminateIfRunning),
        Some(other) => Err(AutumnError::bad_request_msg(format!(
            "unknown reuse_policy '{other}'; expected one of: allow_duplicate, reject_duplicate, \
             allow_duplicate_failed_only, terminate_if_running"
        ))),
    }
}

pub(crate) fn parse_execution_id(raw: &str) -> Result<ExecutionId, AutumnError> {
    raw.parse::<ExecutionId>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid execution id '{raw}'")))
}

fn parse_uuid(raw: &str, label: &str) -> Result<uuid::Uuid, AutumnError> {
    raw.parse::<uuid::Uuid>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid {label} '{raw}'")))
}

pub(crate) fn map_error(error: HarvestError) -> AutumnError {
    match error {
        HarvestError::NotFound(message) | HarvestError::UpdateHandlerNotFound(message) => {
            AutumnError::not_found_msg(message)
        }
        HarvestError::Config(message)
        | HarvestError::NonDeterministic(message)
        | HarvestError::Cancelled(message)
        | HarvestError::WorkflowFailed {
            name: _,
            reason: message,
        } => AutumnError::bad_request_msg(message),
        HarvestError::UpdateRejected { reason } => {
            AutumnError::bad_request_msg(reason).with_status(axum::http::StatusCode::CONFLICT)
        }
        HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state,
        } => AutumnError::bad_request_msg(format!(
            "workflow execution already exists: {existing_exec_id} (state: {existing_state})"
        )),
        HarvestError::Database(message) => AutumnError::service_unavailable_msg(message),
        other => AutumnError::service_unavailable_msg(other.to_string()),
    }
}

// ── External activity completion (issue #92) ──────────────────────────────────

#[derive(Debug, Deserialize)]
struct CompleteExternalActivityRequest {
    output: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct FailExternalActivityRequest {
    error: String,
    #[serde(default)]
    retryable: bool,
}

#[derive(Debug, Deserialize)]
struct HeartbeatExternalActivityRequest {
    /// How many seconds to extend the schedule-to-close deadline from now.
    /// Defaults to the original `schedule_to_close_secs` if omitted.
    extend_by_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ExternalActivityAck {
    ok: bool,
    newly_resolved: bool,
}

async fn complete_external_activity(
    Extension(api_state): Extension<HarvestApiState>,
    Path(token_str): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CompleteExternalActivityRequest>,
) -> Result<Json<ExternalActivityAck>, AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /activities/external/{token}/complete";

    let token = match parse_external_token(&token_str) {
        Ok(t) => t,
        Err(e) => {
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_EXTERNAL_ACTIVITY_COMPLETE,
                    target_type: TARGET_EXTERNAL_ACTIVITY,
                    target_id: Some(token_str.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("malformed external token"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return Err(e);
        }
    };
    let output = request.output.unwrap_or(Value::Null);

    let complete_result = resolve_external_on_shards(&api_state, token, |conn, tok| {
        let out = output.clone();
        Box::pin(async move { external_task::complete_externally(conn, tok, out).await })
    })
    .await;

    let pool = api_state.storage_pool().map_err(map_error)?;
    if complete_result.is_ok() {
        let mut conn = acquire_conn(pool.default_pool()).await?;
        let ar = NewAuditRecord {
            actor: &actor,
            operation: OP_EXTERNAL_ACTIVITY_COMPLETE,
            target_type: TARGET_EXTERNAL_ACTIVITY,
            target_id: Some(token_str.as_str()),
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: &source,
        };
        audit::insert_audit(&mut conn, &ar)
            .await
            .map_err(map_error)?;
    } else if let Err(ref e) = complete_result {
        let err_str = e.to_string();
        if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_EXTERNAL_ACTIVITY_COMPLETE,
                target_type: TARGET_EXTERNAL_ACTIVITY,
                target_id: Some(token_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
    }

    let newly_resolved = complete_result?;
    Ok(Json(ExternalActivityAck {
        ok: true,
        newly_resolved,
    }))
}

async fn fail_external_activity(
    Extension(api_state): Extension<HarvestApiState>,
    Path(token_str): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<FailExternalActivityRequest>,
) -> Result<Json<ExternalActivityAck>, AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /activities/external/{token}/fail";

    let token = match parse_external_token(&token_str) {
        Ok(t) => t,
        Err(e) => {
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_EXTERNAL_ACTIVITY_FAIL,
                    target_type: TARGET_EXTERNAL_ACTIVITY,
                    target_id: Some(token_str.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("malformed external token"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return Err(e);
        }
    };

    let fail_result = resolve_external_on_shards(&api_state, token, |conn, tok| {
        let err = request.error.clone();
        let retryable = request.retryable;
        Box::pin(async move { external_task::fail_externally(conn, tok, err, retryable).await })
    })
    .await;

    let pool = api_state.storage_pool().map_err(map_error)?;
    if fail_result.is_ok() {
        let mut conn = acquire_conn(pool.default_pool()).await?;
        let ar = NewAuditRecord {
            actor: &actor,
            operation: OP_EXTERNAL_ACTIVITY_FAIL,
            target_type: TARGET_EXTERNAL_ACTIVITY,
            target_id: Some(token_str.as_str()),
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: &source,
        };
        audit::insert_audit(&mut conn, &ar)
            .await
            .map_err(map_error)?;
    } else if let Err(ref e) = fail_result {
        let err_str = e.to_string();
        if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_EXTERNAL_ACTIVITY_FAIL,
                target_type: TARGET_EXTERNAL_ACTIVITY,
                target_id: Some(token_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
    }

    let newly_resolved = fail_result?;
    Ok(Json(ExternalActivityAck {
        ok: true,
        newly_resolved,
    }))
}

async fn heartbeat_external_activity(
    Extension(api_state): Extension<HarvestApiState>,
    Path(token_str): Path<String>,
    Json(request): Json<HeartbeatExternalActivityRequest>,
) -> Result<Json<BasicAck>, AutumnError> {
    let token = parse_external_token(&token_str)?;
    let pool = api_state.storage_pool().map_err(map_error)?;

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        if let Some(task) = external_task::find_by_token(&mut conn, token)
            .await
            .map_err(map_error)?
        {
            // Default to the original configured duration so that omitting
            // extend_by_secs resets the deadline by the same fixed window every
            // time, regardless of how many heartbeats have already fired.
            let original_secs = u64::try_from(task.schedule_to_close_secs).unwrap_or(1);
            let extend_by = request.extend_by_secs.unwrap_or(original_secs);
            external_task::extend_deadline(&mut conn, token, extend_by)
                .await
                .map_err(map_error)?;
            return Ok(Json(BasicAck { ok: true }));
        }
    }

    Err(AutumnError::not_found_msg(format!(
        "external task token {token_str}"
    )))
}

fn parse_external_token(raw: &str) -> Result<ExternalActivityToken, AutumnError> {
    raw.parse::<ExternalActivityToken>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid external task token '{raw}'")))
}

/// Scan all shards for the external task identified by `token`, then invoke
/// `action` on the shard that owns it.  Returns `true` if the state transition
/// happened, `false` if the token was already in a terminal state (idempotent).
async fn resolve_external_on_shards<F>(
    api_state: &HarvestApiState,
    token: ExternalActivityToken,
    action: F,
) -> Result<bool, AutumnError>
where
    F: for<'c> Fn(
        &'c mut diesel_async::AsyncPgConnection,
        ExternalActivityToken,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = HarvestResult<bool>> + Send + 'c>,
    >,
{
    let pool = api_state.storage_pool().map_err(map_error)?;

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        if external_task::find_by_token(&mut conn, token)
            .await
            .map_err(map_error)?
            .is_some()
        {
            let result = action(&mut conn, token).await.map_err(map_error)?;
            return Ok(result);
        }
    }

    Err(AutumnError::not_found_msg(format!(
        "external task token {token}"
    )))
}

// ---------------------------------------------------------------------------
// Worker fleet observability (issue #100)
// ---------------------------------------------------------------------------

async fn list_workers_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<Vec<WorkerRow>>, AutumnError> {
    let filters = parse_worker_filters_api(&pairs)?;
    let stale_threshold = api_state.worker_stale_threshold();
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut results: Vec<WorkerRow> = Vec::new();

    // Use i64::MAX as the per-shard limit so apply_worker_filters performs no
    // truncation inside list_workers. Any MAX_LIMIT cap would silently drop
    // workers on a large shard before the global sort+truncate below, producing
    // incomplete results for fleets with more than MAX_LIMIT matching workers on
    // a single shard.
    let per_shard_filters = WorkerFilters {
        limit: i64::MAX,
        ..filters.clone()
    };
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = list_workers(&mut conn, &per_shard_filters, stale_threshold)
            .await
            .map_err(map_error)?;
        results.append(&mut rows);
    }

    // Sort by worker_id for deterministic output across shards, then apply the
    // real limit globally.
    results.sort_by(|a, b| a.worker.worker_id.cmp(&b.worker.worker_id));
    results.truncate(usize::try_from(filters.limit).unwrap_or(usize::MAX));
    Ok(Json(results))
}

async fn get_worker_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Path(worker_id): Path<String>,
) -> Result<Json<WorkerRow>, AutumnError> {
    let stale_threshold = api_state.worker_stale_threshold();
    let pool = api_state.storage_pool().map_err(map_error)?;

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        if let Some(row) = get_worker(&mut conn, &worker_id, stale_threshold)
            .await
            .map_err(map_error)?
        {
            return Ok(Json(row));
        }
    }

    Err(AutumnError::not_found_msg(format!("worker '{worker_id}'")))
}

async fn workers_health(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<FleetHealth>, AutumnError> {
    let stale_threshold = api_state.worker_stale_threshold();
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut combined = FleetHealth {
        healthy: 0,
        stale: 0,
        draining: 0,
        by_queue: std::collections::HashMap::new(),
        by_shard: std::collections::HashMap::new(),
    };

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let shard_health = fleet_health(&mut conn, stale_threshold)
            .await
            .map_err(map_error)?;
        combined.healthy += shard_health.healthy;
        combined.stale += shard_health.stale;
        combined.draining += shard_health.draining;
        for (queue, count) in shard_health.by_queue {
            *combined.by_queue.entry(queue).or_default() += count;
        }
        for (shard, count) in shard_health.by_shard {
            *combined.by_shard.entry(shard).or_default() += count;
        }
    }

    Ok(Json(combined))
}

/// Parse worker query-string parameters, mapping errors to `400 Bad Request`.
fn parse_worker_filters_api(pairs: &[(String, String)]) -> Result<WorkerFilters, AutumnError> {
    parse_worker_filters(pairs).map_err(AutumnError::bad_request_msg)
}

// ---------------------------------------------------------------------------
// Update primitive (issue #140)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct AdmitUpdateRequest {
    #[serde(default)]
    input: Value,
}

#[derive(Debug, Deserialize)]
struct AdmitUpdateQuery {
    /// Controls how long to wait for a result.
    /// `"admitted"` — return 202 as soon as the event is durably written.
    /// `"completed"` (default) — block until the handler returns or the timeout fires.
    wait: Option<String>,
    /// Wall-clock timeout in seconds for `wait=completed` mode. Default: 30.
    timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct UpdateAdmittedResponse {
    update_id: String,
}

#[derive(Debug, Serialize)]
struct UpdateCompletedResponse {
    update_id: String,
    output: Value,
}

#[derive(Debug, Serialize)]
struct UpdateFailedResponse {
    update_id: String,
    error: String,
}

/// `POST /workflows/{id}/update/{update_name}`
///
/// Durably appends an `UpdateAdmitted` event for the named handler, wakes the
/// workflow worker, then either returns immediately (`?wait=admitted`) or polls
/// for the terminal `UpdateCompleted`/`UpdateFailed` event (`?wait=completed`,
/// the default) until the configurable timeout fires.
async fn admit_update(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, update_name)): Path<(String, String)>,
    Query(query): Query<AdmitUpdateQuery>,
    Json(request): Json<AdmitUpdateRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let exec_id = match parse_execution_id(&id) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let mut conn = match db_conn_for_execution(&api_state, exec_id).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    let update_id = UpdateId::new();

    // Durably append the UpdateAdmitted event inside a FOR UPDATE transaction
    // that also verifies the execution is still RUNNING.  Doing the state check
    // and the insert under the same row-level lock prevents a TOCTOU race where
    // the workflow could complete between a separate state read and the insert.
    // UpdateRejected is returned if the execution is no longer RUNNING.
    if let Err(e) = store::admit_update_event(
        &mut conn,
        exec_id,
        update_id,
        update_name.clone(),
        request.input,
    )
    .await
    {
        return map_error(e).into_response();
    }

    // Wake the workflow worker so it picks up the new admitted update.
    if let Err(e) = queue::wake_workflow_task(&mut conn, exec_id).await {
        return map_error(e).into_response();
    }

    // Return immediately if the caller only wanted durable admission.
    let wait_mode = query.wait.as_deref().unwrap_or("completed");
    if wait_mode == "admitted" {
        return (
            axum::http::StatusCode::ACCEPTED,
            Json(UpdateAdmittedResponse {
                update_id: update_id.to_string(),
            }),
        )
            .into_response();
    }

    let timeout_secs = query.timeout_secs.unwrap_or(30);
    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return map_error(e).into_response(),
    };
    poll_update_result(&pool, exec_id, update_id, timeout_secs).await
}

/// Poll history until `update_id` resolves to `UpdateCompleted`/`UpdateFailed`
/// or the wall-clock `timeout_secs` elapses (→ 504 Gateway Timeout).
async fn poll_update_result(
    pool: &HarvestDbPool,
    exec_id: ExecutionId,
    update_id: UpdateId,
    timeout_secs: u64,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let poll_interval = Duration::from_millis(300);

    let poll_result = tokio::time::timeout(Duration::from_secs(timeout_secs), async {
        loop {
            let result = {
                // Scope the connection so it is returned to the pool before sleeping.
                let mut c = pool
                    .pool_for_execution(exec_id)
                    .get()
                    .await
                    .map_err(|e| HarvestError::Database(e.to_string()))?;
                let h = store::load_history(&mut c, exec_id).await?;
                // c is dropped here, releasing the connection back to the pool.
                match HistoryMatcher::new(h.events).match_update(update_id) {
                    HistoryMatch::Matched { output } => Some(Ok((true, output, String::new()))),
                    HistoryMatch::Failed { error, .. } => Some(Ok((false, Value::Null, error))),
                    _ => None,
                }
            };
            match result {
                Some(v) => return v,
                None => tokio::time::sleep(poll_interval).await,
            }
        }
    })
    .await;

    match poll_result {
        Ok(Ok((true, output, _))) => (
            axum::http::StatusCode::OK,
            Json(UpdateCompletedResponse {
                update_id: update_id.to_string(),
                output,
            }),
        )
            .into_response(),
        Ok(Ok((false, _, error))) => (
            axum::http::StatusCode::CONFLICT,
            Json(UpdateFailedResponse {
                update_id: update_id.to_string(),
                error,
            }),
        )
            .into_response(),
        Ok(Err(e)) => map_error(e).into_response(),
        Err(_timeout) => (
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            Json(serde_json::json!({
                "update_id": update_id.to_string(),
                "message": format!("update did not complete within {timeout_secs}s"),
            })),
        )
            .into_response(),
    }
}

/// `GET /workflows/{id}/update/{update_id}/result`
///
/// Look up the durable result of a previously admitted update by its
/// `update_id`. Returns:
/// - `200 OK` with `output` if the handler completed successfully.
/// - `409 Conflict` with `error` if the handler failed or the update was rejected.
/// - `202 Accepted` if the update is still in-flight.
/// - `404 Not Found` if no `UpdateAdmitted` event exists for the given ID.
async fn get_update_result(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, update_id_str)): Path<(String, String)>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let exec_id = match parse_execution_id(&id) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };
    let update_id: UpdateId = match update_id_str.parse() {
        Ok(id) => id,
        Err(_) => {
            return AutumnError::bad_request_msg(format!("invalid update id '{update_id_str}'"))
                .into_response();
        }
    };

    let mut conn = match db_conn_for_execution(&api_state, exec_id).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    let history = match store::load_history(&mut conn, exec_id).await {
        Ok(h) => h,
        Err(e) => return map_error(e).into_response(),
    };

    // Confirm the update was ever admitted.
    let admitted = history.events.iter().any(
        |ev| matches!(ev, WorkflowEvent::UpdateAdmitted { update_id: id, .. } if *id == update_id),
    );
    if !admitted {
        return AutumnError::not_found_msg(format!("update {update_id_str}")).into_response();
    }

    let matcher = HistoryMatcher::new(history.events);
    match matcher.match_update(update_id) {
        HistoryMatch::Matched { output } => (
            axum::http::StatusCode::OK,
            Json(UpdateCompletedResponse {
                update_id: update_id.to_string(),
                output,
            }),
        )
            .into_response(),
        HistoryMatch::Failed { error, .. } => (
            axum::http::StatusCode::CONFLICT,
            Json(UpdateFailedResponse {
                update_id: update_id.to_string(),
                error,
            }),
        )
            .into_response(),
        _ => (
            axum::http::StatusCode::ACCEPTED,
            Json(UpdateAdmittedResponse {
                update_id: update_id.to_string(),
            }),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::workers::WorkerHealth;

    fn pairs(values: &[(&str, &str)]) -> Vec<(String, String)> {
        values
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn bulk_dlq_filter_is_empty_when_only_limit_set() {
        let filter: autumn_harvest::dlq::BulkDlqFilter =
            serde_json::from_str(r#"{"limit": 100}"#).unwrap();
        assert!(
            filter.is_empty(),
            "filter with only limit should be considered empty"
        );
    }

    #[test]
    fn bulk_dlq_filter_is_not_empty_when_activity_name_set() {
        let filter: autumn_harvest::dlq::BulkDlqFilter =
            serde_json::from_str(r#"{"activity_name": "send_email"}"#).unwrap();
        assert!(!filter.is_empty());
    }

    #[test]
    fn bulk_dlq_filter_effective_limit_defaults_to_100() {
        let filter: autumn_harvest::dlq::BulkDlqFilter =
            serde_json::from_str(r#"{"activity_name": "foo"}"#).unwrap();
        assert_eq!(filter.effective_limit(), 100);
    }

    #[test]
    fn bulk_dlq_filter_effective_limit_hard_capped_at_1000() {
        let filter: autumn_harvest::dlq::BulkDlqFilter =
            serde_json::from_str(r#"{"activity_name": "foo", "limit": 9999}"#).unwrap();
        assert_eq!(filter.effective_limit(), 1000);
    }

    #[test]
    fn parse_workflow_filters_defaults_to_empty_filters_with_default_limit() {
        let filters = parse_workflow_filters(&[]).expect("no params should parse");
        assert_eq!(filters.limit, DEFAULT_WORKFLOW_LIMIT);
        assert!(filters.states.is_empty());
        assert!(filters.workflow_name.is_none());
        assert!(filters.search_attrs.is_empty());
    }

    #[test]
    fn parse_workflow_filters_accepts_comma_and_repeated_states() {
        let filters = parse_workflow_filters(&pairs(&[
            ("state", "RUNNING,FAILED"),
            ("state", "TIMED_OUT"),
        ]))
        .expect("multi-state should parse");
        assert_eq!(
            filters.states,
            vec![
                "RUNNING".to_string(),
                "FAILED".to_string(),
                "TIMED_OUT".to_string(),
            ]
        );
    }

    #[test]
    fn parse_workflow_filters_rejects_unknown_state() {
        let err = parse_workflow_filters(&pairs(&[("state", "BOGUS")]))
            .expect_err("unknown state must error");
        assert!(err.to_string().contains("unknown workflow state"));
    }

    #[test]
    fn parse_workflow_filters_builds_search_attr_predicates() {
        let filters = parse_workflow_filters(&pairs(&[
            ("search_attr", "tenant:acme"),
            ("search_attr", "customer_id:42"),
        ]))
        .expect("search_attr pairs should parse");
        assert_eq!(filters.search_attrs.len(), 2);
        assert_eq!(filters.search_attrs[0]["tenant"], "acme");
        assert_eq!(filters.search_attrs[1]["customer_id"], "42");
    }

    #[test]
    fn parse_workflow_filters_rejects_search_attr_without_separator() {
        let err = parse_workflow_filters(&pairs(&[("search_attr", "tenant")]))
            .expect_err("missing separator must error");
        assert!(err.to_string().contains("invalid search_attr"));
    }

    #[test]
    fn parse_workflow_filters_rejects_search_attr_with_empty_key() {
        let err = parse_workflow_filters(&pairs(&[("search_attr", ":acme")]))
            .expect_err("empty key must error");
        assert!(err.to_string().contains("missing a key"));
    }

    #[test]
    fn parse_workflow_filters_clamps_limit_to_documented_range() {
        let filters = parse_workflow_filters(&pairs(&[("limit", "9001")]))
            .expect("oversize limit should clamp");
        assert_eq!(filters.limit, MAX_WORKFLOW_LIMIT);

        let filters =
            parse_workflow_filters(&pairs(&[("limit", "0")])).expect("zero limit should clamp");
        assert_eq!(filters.limit, 1);
    }

    #[test]
    fn parse_workflow_filters_rejects_non_numeric_limit() {
        let err = parse_workflow_filters(&pairs(&[("limit", "abc")]))
            .expect_err("non-numeric limit must error");
        assert!(err.to_string().contains("invalid limit"));
    }

    #[test]
    fn parse_workflow_filters_ignores_unknown_query_keys() {
        let filters = parse_workflow_filters(&pairs(&[("ignored", "value")]))
            .expect("unknown keys should be skipped");
        assert!(filters.states.is_empty());
        assert!(filters.workflow_name.is_none());
    }

    #[test]
    fn parse_workflow_children_filters_accepts_statuses_limit_and_depth() {
        let filters = parse_workflow_children_filters(&pairs(&[
            ("status", "Failed,Running"),
            ("status", "TimedOut"),
            ("workflow_name", "billing_child"),
            ("limit", "9001"),
            ("depth", "2"),
        ]))
        .expect("children filters should parse");

        assert_eq!(
            filters.statuses,
            vec![
                "FAILED".to_string(),
                "RUNNING".to_string(),
                "TIMED_OUT".to_string(),
            ]
        );
        assert_eq!(filters.workflow_name.as_deref(), Some("billing_child"));
        assert_eq!(filters.limit, MAX_WORKFLOW_CHILDREN_LIMIT);
        assert_eq!(filters.max_depth, 2);
    }

    #[test]
    fn parse_workflow_children_filters_accepts_continued_as_new() {
        let filters = parse_workflow_children_filters(&pairs(&[("status", "ContinuedAsNew")]))
            .expect("ContinuedAsNew is a valid workflow execution state");

        assert_eq!(filters.statuses, vec!["CONTINUED_AS_NEW".to_string()]);
    }

    #[test]
    fn parse_workflow_children_filters_rejects_depth_above_cap() {
        let err = parse_workflow_children_filters(&pairs(&[("depth", "6")]))
            .expect_err("depth above cap must error");
        assert!(err.to_string().contains("exceeds maximum"));
    }

    #[test]
    fn parse_workflow_children_filters_rejects_unknown_status() {
        let err = parse_workflow_children_filters(&pairs(&[("status", "Zombie")]))
            .expect_err("unknown child status must error");
        assert!(err.to_string().contains("unknown workflow child status"));
    }

    #[test]
    fn parse_reuse_policy_none_defaults_to_allow_duplicate() {
        assert_eq!(
            parse_reuse_policy(None).unwrap(),
            WorkflowIdReusePolicy::AllowDuplicate
        );
    }

    #[test]
    fn parse_reuse_policy_empty_string_defaults_to_allow_duplicate() {
        assert_eq!(
            parse_reuse_policy(Some("")).unwrap(),
            WorkflowIdReusePolicy::AllowDuplicate
        );
    }

    #[test]
    fn parse_reuse_policy_accepts_all_known_values() {
        use WorkflowIdReusePolicy::*;
        let cases = [
            ("allow_duplicate", AllowDuplicate),
            ("reject_duplicate", RejectDuplicate),
            ("allow_duplicate_failed_only", AllowDuplicateFailedOnly),
            ("terminate_if_running", TerminateIfRunning),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_reuse_policy(Some(input)).unwrap(),
                expected,
                "failed for '{input}'"
            );
        }
    }

    #[test]
    fn parse_reuse_policy_unknown_value_returns_400_error() {
        let err = parse_reuse_policy(Some("bogus_policy")).expect_err("unknown value must error");
        let msg = err.to_string();
        assert!(
            msg.contains("bogus_policy"),
            "offending value must be echoed in error: {msg}"
        );
        assert!(
            msg.contains("unknown reuse_policy"),
            "error must mention unknown reuse_policy: {msg}"
        );
    }

    #[test]
    fn parse_reuse_policy_unknown_value_is_not_silent_fallback() {
        // Ensure "allow_DUPLICATE" (wrong case) is rejected, not silently coerced.
        assert!(
            parse_reuse_policy(Some("allow_DUPLICATE")).is_err(),
            "wrong case must not silently fall back"
        );
    }

    // -- Worker filter parsing via API wrapper --

    #[test]
    fn parse_worker_filters_api_defaults_when_no_params() {
        let f = parse_worker_filters_api(&[]).expect("empty params should succeed");
        assert_eq!(f.limit, WorkerFilters::DEFAULT_LIMIT);
        assert!(f.queue.is_none());
        assert!(f.shard_id.is_none());
        assert!(f.status.is_none());
        assert!(f.health.is_none());
    }

    #[test]
    fn parse_worker_filters_api_accepts_queue_filter() {
        let f = parse_worker_filters_api(&pairs(&[("queue", "email-workers")])).unwrap();
        assert_eq!(f.queue.as_deref(), Some("email-workers"));
    }

    #[test]
    fn parse_worker_filters_api_accepts_all_status_values() {
        for status in ["Active", "Draining", "Stopped"] {
            let f = parse_worker_filters_api(&pairs(&[("status", status)])).unwrap();
            assert_eq!(f.status.as_deref(), Some(status), "failed for {status}");
        }
    }

    #[test]
    fn parse_worker_filters_api_rejects_unknown_status_with_400() {
        let err = parse_worker_filters_api(&pairs(&[("status", "zombie")])).unwrap_err();
        assert!(err.to_string().contains("unknown status"), "error: {err}");
    }

    #[test]
    fn parse_worker_filters_api_accepts_health_healthy_and_stale() {
        let f = parse_worker_filters_api(&pairs(&[("health", "healthy")])).unwrap();
        assert_eq!(f.health, Some(WorkerHealth::Healthy));

        let f = parse_worker_filters_api(&pairs(&[("health", "stale")])).unwrap();
        assert_eq!(f.health, Some(WorkerHealth::Stale));
    }

    #[test]
    fn parse_worker_filters_api_rejects_unknown_health_with_400() {
        let err = parse_worker_filters_api(&pairs(&[("health", "unknown")])).unwrap_err();
        assert!(err.to_string().contains("unknown health"), "error: {err}");
    }

    #[test]
    fn parse_worker_filters_api_accepts_shard_id() {
        let f = parse_worker_filters_api(&pairs(&[("shard_id", "2")])).unwrap();
        assert_eq!(f.shard_id, Some(2));
    }

    #[test]
    fn parse_worker_filters_api_rejects_non_integer_shard_id() {
        let err = parse_worker_filters_api(&pairs(&[("shard_id", "bad")])).unwrap_err();
        assert!(err.to_string().contains("invalid shard_id"), "error: {err}");
    }

    #[test]
    fn parse_worker_filters_api_clamps_limit() {
        let f = parse_worker_filters_api(&pairs(&[("limit", "99999")])).unwrap();
        assert_eq!(f.limit, WorkerFilters::MAX_LIMIT);

        let f = parse_worker_filters_api(&pairs(&[("limit", "0")])).unwrap();
        assert_eq!(f.limit, 1);
    }

    #[test]
    fn harvest_api_state_stale_threshold_defaults_to_10s() {
        let state = HarvestApiState::new();
        assert_eq!(
            state.worker_stale_threshold(),
            std::time::Duration::from_secs(10)
        );
    }

    #[test]
    fn harvest_api_state_stale_threshold_can_be_overridden() {
        let state = HarvestApiState::new();
        state.set_worker_stale_threshold(std::time::Duration::from_secs(20));
        assert_eq!(
            state.worker_stale_threshold(),
            std::time::Duration::from_secs(20)
        );
    }
}
