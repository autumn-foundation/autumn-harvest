//! Axum management routes for Harvest workflows and DAGs.

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

use autumn_harvest::batch::{
    self, BatchAction, BatchExecutorConfig, BatchFilter, BatchJobStatus, BatchJobView,
    BatchSubmission,
};
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::dlq;
use autumn_harvest::error::{HarvestError, HarvestResult, database_error};
use autumn_harvest::external_task;
use autumn_harvest::models::{DagRun, DeadLetter, HarvestSchedule, WorkflowExecution};
use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest::queue::{self, ConcurrencyKeyStats};
use autumn_harvest::retention::{RetentionConfig, RetentionMonitor, RetentionStatus};
use autumn_harvest::scheduler::{
    DagCatalog, RegisteredDag, SchedulerMonitor, SchedulerSnapshot, trigger_dag,
};
use autumn_harvest::schema::{harvest_dag_runs, harvest_schedules, harvest_workflow_executions};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::signal;
use autumn_harvest::store;
use autumn_harvest::telemetry::{ATTR_EXECUTION_ID, ATTR_WORKFLOW_ID};
use autumn_harvest::types::{ExecutionId, ExternalActivityToken, ShardId, WorkflowIdReusePolicy};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::workers::{
    FleetHealth, WorkerFilters, WorkerRow, fleet_health, get_worker, list_workers,
    parse_worker_filters,
};
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

#[derive(Clone)]
pub struct HarvestApiState {
    runtime: Arc<Mutex<Option<HarvestApiRuntime>>>,
    storage_pool: Arc<Mutex<Option<HarvestDbPool>>>,
    /// `2 × worker_heartbeat_interval`; derived from `WorkerConfig` at startup.
    worker_stale_threshold: Arc<Mutex<std::time::Duration>>,
}

impl Default for HarvestApiState {
    fn default() -> Self {
        Self {
            runtime: Arc::default(),
            storage_pool: Arc::default(),
            worker_stale_threshold: Arc::new(Mutex::new(std::time::Duration::from_secs(10))),
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

    fn worker_stale_threshold(&self) -> std::time::Duration {
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

    fn storage_pool(&self) -> HarvestResult<HarvestDbPool> {
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
    execution: WorkflowExecution,
    history: Vec<Value>,
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
pub(crate) const KNOWN_WORKFLOW_STATES: &[&str] =
    &["RUNNING", "COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT"];

const DEFAULT_WORKFLOW_LIMIT: i64 = 50;
const MAX_WORKFLOW_LIMIT: i64 = 200;

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

#[derive(Debug, Deserialize)]
struct DeadLetterListQuery {
    limit: Option<i64>,
}

pub fn harvest_api_router(api_state: HarvestApiState) -> Router<AppState> {
    Router::new()
        .route("/workflows", get(list_workflows))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{workflow_name}/start", post(start_workflow))
        .route("/workflows/{id}/cancel", post(cancel_workflow))
        .route(
            "/workflows/{id}/signal/{signal_name}",
            post(signal_workflow),
        )
        .route("/workflows/{id}/query/{query_name}", get(query_workflow))
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
    Json(request): Json<SubmitBatchOperationRequest>,
) -> Result<(axum::http::StatusCode, Json<SubmitBatchOperationResponse>), AutumnError> {
    let action: BatchAction = request
        .action
        .parse()
        .map_err(AutumnError::bad_request_msg)?;

    // Reject `state=` values outside the canonical list before we hit the DB
    // — otherwise the executor would silently match nothing and look like a
    // success.
    for state in &request.filter.states {
        if !KNOWN_WORKFLOW_STATES.contains(&state.as_str()) {
            return Err(AutumnError::bad_request_msg(format!(
                "unknown workflow state '{state}' in batch filter"
            )));
        }
    }

    let pool = api_state.storage_pool().map_err(map_error)?;
    // Persist the row on the default shard. The executor will fan out across
    // every configured shard at run time via iter_shards().
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let job_id = batch::submit_batch_job(
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
    .await
    .map_err(map_error)?;

    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(SubmitBatchOperationResponse {
            batch_job_id: job_id.to_string(),
        }),
    ))
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
        execution,
        history: events,
    }))
}

async fn start_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(workflow_name): Path<String>,
    Json(request): Json<StartWorkflowRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let runtime = match api_state.runtime() {
        Ok(r) => r,
        Err(e) => return map_error(e).into_response(),
    };
    if !runtime.registry.workflows.contains_key(&workflow_name) {
        return AutumnError::not_found_msg(format!("workflow '{workflow_name}'")).into_response();
    }

    let reuse_policy = match parse_reuse_policy(request.reuse_policy.as_deref()) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
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
        }) => (
            axum::http::StatusCode::CONFLICT,
            Json(AlreadyExistsResponse {
                existing_execution_id: existing_exec_id.to_string(),
                existing_state,
            }),
        )
            .into_response(),
        Err(e) => map_error(e).into_response(),
        Ok(start) => (
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
            .into_response(),
    }
}

async fn cancel_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    Json(request): Json<CancelWorkflowRequest>,
) -> Result<(axum::http::StatusCode, Json<CancelWorkflowResponse>), AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let reason = request
        .reason
        .as_deref()
        .unwrap_or("workflow cancellation requested");
    let cancelled = cancel_workflow_execution(&mut conn, exec_id, reason)
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

async fn signal_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, signal_name)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> Result<(axum::http::StatusCode, Json<BasicAck>), AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    signal::send_signal(&mut conn, exec_id, &signal_name, payload)
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
    Json(request): Json<DagTriggerRequest>,
) -> Result<(axum::http::StatusCode, Json<DagRun>), AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let shard = runtime.router.pick_for_dag(&dag_name);
    let run = trigger_dag(
        pool.pool_for(shard).clone(),
        Arc::clone(&runtime.registry),
        Arc::clone(&runtime.dags),
        &dag_name,
        request.conf,
        runtime.scheduler,
    )
    .await
    .map_err(map_error)?;
    Ok((axum::http::StatusCode::CREATED, Json(run)))
}

async fn patch_dag(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
    Json(request): Json<DagPauseRequest>,
) -> Result<Json<HarvestSchedule>, AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let mut conn = db_conn_for_dag(&api_state, &dag_name).await?;
    let updated = diesel::update(dsl::harvest_schedules.filter(dsl::dag_name.eq(&dag_name)))
        .set((
            dsl::is_paused.eq(request.paused),
            dsl::updated_at.eq(chrono::Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    if updated == 0 {
        return Err(AutumnError::not_found_msg(format!("dag '{dag_name}'")));
    }

    let schedule = dsl::harvest_schedules
        .filter(dsl::dag_name.eq(&dag_name))
        .select(HarvestSchedule::as_select())
        .first(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    Ok(Json(schedule))
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

async fn create_workflow_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    Json(request): Json<CreateWorkflowScheduleRequest>,
) -> Result<(axum::http::StatusCode, Json<ScheduleEntry>), AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let runtime = api_state.runtime().map_err(map_error)?;

    // Validate: workflow_name must be registered.
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
        return Err(AutumnError::not_found_msg(format!(
            "workflow '{}' is not registered; registered: {:?}",
            request.workflow_name, registered
        )));
    }

    // Parse the schedule expression.
    let schedule = parse_schedule_expr(&request.schedule_expr)
        .map_err(|e| AutumnError::bad_request_msg(format!("invalid schedule_expr: {e}")))?;

    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.pool_for(runtime.router().default_shard())).await?;

    // Upsert the schedule row.
    let ws = WorkflowSchedule {
        workflow_name: request.workflow_name.clone(),
        schedule,
        input: request.input.clone(),
        catchup: request.catchup,
        max_active_runs: request.max_active_runs,
        paused: request.paused,
        queue_name: request.queue_name.clone(),
    };
    autumn_harvest::register_workflow_schedules(&mut conn, std::slice::from_ref(&ws))
        .await
        .map_err(map_error)?;

    // Read back the upserted row to return it.
    let row: autumn_harvest::models::HarvestSchedule = dsl::harvest_schedules
        .filter(dsl::workflow_name.eq(&request.workflow_name))
        .select(autumn_harvest::models::HarvestSchedule::as_select())
        .first(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    let entry = ScheduleEntry {
        id: row.id,
        kind: ScheduleKind::Workflow,
        name: request.workflow_name.clone(),
        schedule_expr: row.schedule_expr,
        is_paused: row.is_paused,
        next_run_at: row.next_run_at,
        last_run_at: row.last_run_at,
        max_active_runs: row.max_active_runs,
        catchup: row.catchup,
    };

    Ok((axum::http::StatusCode::CREATED, Json(entry)))
}

async fn pause_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Json<BasicAck>, AutumnError> {
    set_schedule_paused(&api_state, &id, true).await
}

async fn resume_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Json<BasicAck>, AutumnError> {
    set_schedule_paused(&api_state, &id, false).await
}

async fn set_schedule_paused(
    api_state: &HarvestApiState,
    id_str: &str,
    paused: bool,
) -> Result<Json<BasicAck>, AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let id = parse_uuid(id_str, "schedule id")?;
    let pool = api_state.storage_pool().map_err(map_error)?;

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
            break;
        }
    }

    if updated_count == 0 {
        return Err(AutumnError::not_found_msg(format!("schedule {id}")));
    }
    Ok(Json(BasicAck { ok: true }))
}

async fn delete_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Json<BasicAck>, AutumnError> {
    use autumn_harvest::models::HarvestSchedule;
    use autumn_harvest::schema::harvest_schedules::dsl;

    let id = parse_uuid(&id, "schedule id")?;
    let pool = api_state.storage_pool().map_err(map_error)?;

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
            break;
        }
    }

    if deleted_count == 0 {
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
) -> Result<(axum::http::StatusCode, Json<ReplayDeadLetterResponse>), AutumnError> {
    let dead_letter_id = parse_uuid(&id, "dead-letter id")?;
    let task_id = replay_dead_letter_from_shards(&api_state, dead_letter_id).await?;

    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(ReplayDeadLetterResponse {
            ok: true,
            dead_letter_id: dead_letter_id.to_string(),
            task_id: task_id.to_string(),
        }),
    ))
}

async fn bulk_replay_dead_letters_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Json(filter): Json<dlq::BulkDlqFilter>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    if filter.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "bulk filter must specify at least one criterion: \
                          activity_name, workflow_name, failed_after, or failed_before"
            })),
        )
            .into_response();
    }

    match bulk_replay_from_shards(&api_state, &filter).await {
        Ok(result) => {
            let status = if result.acted_on == 0 && !result.failures.is_empty() {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            } else {
                axum::http::StatusCode::OK
            };
            (status, Json(result)).into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}

async fn bulk_discard_dead_letters_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Json(filter): Json<dlq::BulkDlqFilter>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    if filter.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "bulk filter must specify at least one criterion: \
                          activity_name, workflow_name, failed_after, or failed_before"
            })),
        )
            .into_response();
    }

    match bulk_discard_from_shards(&api_state, &filter).await {
        Ok(result) => {
            let status = if result.acted_on == 0 && !result.failures.is_empty() {
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            } else {
                axum::http::StatusCode::OK
            };
            (status, Json(result)).into_response()
        }
        Err(e) => map_error(e).into_response(),
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
) -> Result<Json<BasicAck>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let trigger = runtime.retention.trigger.as_ref().ok_or_else(|| {
        AutumnError::service_unavailable_msg(
            "retention run-now unavailable: no local retention runtime owner",
        )
    })?;
    trigger.try_send(()).map_err(|error| {
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

async fn acquire_conn(pool: &DbPool) -> Result<PoolConn, AutumnError> {
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
        HarvestError::NotFound(message) => AutumnError::not_found_msg(message),
        HarvestError::Config(message)
        | HarvestError::NonDeterministic(message)
        | HarvestError::Cancelled(message)
        | HarvestError::WorkflowFailed {
            name: _,
            reason: message,
        } => AutumnError::bad_request_msg(message),
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
    Json(request): Json<CompleteExternalActivityRequest>,
) -> Result<Json<ExternalActivityAck>, AutumnError> {
    let token = parse_external_token(&token_str)?;
    let output = request.output.unwrap_or(Value::Null);

    let newly_resolved = resolve_external_on_shards(&api_state, token, |conn, tok| {
        let out = output.clone();
        Box::pin(async move { external_task::complete_externally(conn, tok, out).await })
    })
    .await?;

    Ok(Json(ExternalActivityAck {
        ok: true,
        newly_resolved,
    }))
}

async fn fail_external_activity(
    Extension(api_state): Extension<HarvestApiState>,
    Path(token_str): Path<String>,
    Json(request): Json<FailExternalActivityRequest>,
) -> Result<Json<ExternalActivityAck>, AutumnError> {
    let token = parse_external_token(&token_str)?;

    let newly_resolved = resolve_external_on_shards(&api_state, token, |conn, tok| {
        let err = request.error.clone();
        let retryable = request.retryable;
        Box::pin(async move { external_task::fail_externally(conn, tok, err, retryable).await })
    })
    .await?;

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
