//! Axum management routes for Harvest workflows and DAGs.
#![allow(clippy::literal_string_with_formatting_args)]

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::Extension;
use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
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
    OP_SCHEDULE_BACKFILL, OP_SCHEDULE_CREATE, OP_SCHEDULE_DELETE, OP_SCHEDULE_PAUSE,
    OP_SCHEDULE_RESUME, OP_WORKER_DRAIN, OP_WORKFLOW_CANCEL, OP_WORKFLOW_RESET, OP_WORKFLOW_SIGNAL,
    OP_WORKFLOW_SIGNAL_WITH_START, OP_WORKFLOW_START, SOURCE_API, STATUS_FAILED, STATUS_SUCCEEDED,
    TARGET_BATCH, TARGET_DAG, TARGET_DEAD_LETTER, TARGET_EXTERNAL_ACTIVITY, TARGET_RETENTION,
    TARGET_SCHEDULE, TARGET_WORKER, TARGET_WORKFLOW,
};
use autumn_harvest::batch::{
    self, BatchAction, BatchExecutorConfig, BatchFilter, BatchJobStatus, BatchJobView,
    BatchSubmission,
};
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::dlq;
use autumn_harvest::error::{HarvestError, HarvestResult, database_error};
use autumn_harvest::external_task;
use autumn_harvest::history_export::{
    DEFAULT_HISTORY_EXPORT_MAX_BYTES, HistoryExportDocument, HistoryExportError,
    HistoryExportRequest, HistoryPayloadPolicy, export_history,
};
use autumn_harvest::models::{
    AuditRecord, BackfillLogRow, DeadLetter, HarvestSchedule, NewAuditRecord, NewBackfillLogRow,
    WorkflowExecution,
};
use autumn_harvest::policy::{Schedule, WorkflowSchedule, compute_jitter_offset};
use autumn_harvest::queue::{self, ConcurrencyKeyStats};
use autumn_harvest::reset::{
    ResetInvalidPoint, ResetResult, WorkflowResetError, WorkflowResetRequest,
    preview_workflow_reset, reset_workflow_execution,
};
use autumn_harvest::retention::{RetentionConfig, RetentionMonitor, RetentionStatus};
use autumn_harvest::scheduler::{
    BackfillPlanError, DEFAULT_BACKFILL_MAX_COUNT, DagCatalog, RegisteredDag, SchedulerMonitor,
    SchedulerSnapshot, ensure_dag_schedule, parse_schedule_from_expr_pub, plan_backfill_timestamps,
    scheduled_workflow_id_pub, trigger_unified_dag,
};
use autumn_harvest::schema::{
    harvest_backfill_log, harvest_dead_letters, harvest_events, harvest_schedules, harvest_signals,
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
    DrainPreviewItem, DrainResponse, FleetHealth, PinnedExecutionRow, WorkerFilters, WorkerRow,
    drain_preview, fleet_health, get_worker, list_pinned_executions, list_workers,
    parse_worker_filters, request_drain,
};
use autumn_harvest::{HistoryMatch, HistoryMatcher, WorkflowEvent};
use autumn_harvest::{
    SignalWithStartOutcome, SignalWithStartParams, StartWorkflowParams, WorkflowHandleClient,
    WorkflowResult, cancel_workflow_execution, signal_with_start_workflow_execution,
    start_or_load_workflow_execution,
};

use crate::preflight::{PreflightReport, build_preflight_report};
use crate::shard_health::{ShardHealthReport, ShardReadiness, build_shard_health_report};
use crate::state::HarvestDbPool;
use crate::version_gate_retirement::{RetirementCheckQuery, build_retirement_check_report};
use crate::version_usage::{VersionUsageQuery, build_version_usage_report};

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
    registered_dag_names: Arc<HashSet<String>>,
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
    pub fn new(
        registry: Arc<HandlerRegistry>,
        dags: Arc<DagCatalog>,
        workflow_schedules: Arc<Vec<WorkflowSchedule>>,
        worker_id: Option<String>,
        queues: Vec<String>,
        scheduler: SchedulerMonitor,
        retention: HarvestRetentionRuntime,
        router: ShardRouter,
    ) -> Self {
        let mut registered_dag_names = dags
            .values()
            .filter(|dag| dag.is_unified)
            .map(|dag| dag.name.clone())
            .collect::<HashSet<_>>();
        registered_dag_names.extend(
            workflow_schedules
                .iter()
                .filter_map(|schedule| schedule.dag_name.clone()),
        );

        Self {
            registry,
            dags,
            registered_dag_names: Arc::new(registered_dag_names),
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

    /// Add DAG names that were promoted to the unified workflow execution path.
    #[must_use]
    pub fn with_registered_dag_names(mut self, names: impl IntoIterator<Item = String>) -> Self {
        let mut registered = (*self.registered_dag_names).clone();
        registered.extend(names);
        self.registered_dag_names = Arc::new(registered);
        self
    }

    #[must_use]
    pub(crate) fn is_registered_dag(&self, dag_name: &str) -> bool {
        self.registered_dag_names.contains(dag_name)
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
    /// Runtime profile used to decide whether unauthenticated admin routes are acceptable.
    deployment_profile: Arc<Mutex<String>>,
    /// Whether the management API was mounted behind an embedder-provided auth boundary.
    admin_auth_boundary: Arc<Mutex<bool>>,
    /// Autumn session key used by built-in guards when no outer auth boundary is configured.
    admin_auth_session_key: Arc<Mutex<String>>,
    /// When enabled, `/health` returns 503 until writable shards are ready.
    health_requires_shard_readiness: Arc<Mutex<bool>>,
    /// Default drain deadline offset used when `POST /workers/{id}/drain` omits `deadline_at`.
    /// Set from `WorkerConfig::shutdown_timeout` at startup; defaults to 30 s.
    worker_shutdown_timeout: Arc<Mutex<std::time::Duration>>,
    /// Per-shard Postgres URLs used by workflow result LISTEN/NOTIFY waits.
    workflow_result_notification_urls: Arc<Mutex<BTreeMap<ShardId, String>>>,
    /// Maximum wait accepted by `GET /workflows/{id}/result?wait=...`.
    workflow_result_max_wait: Arc<Mutex<std::time::Duration>>,
    /// Per-query execution timeout (issue #234); derived from `WorkerConfig::query_timeout`
    /// at startup. Defaults to 5 s.
    query_timeout: Arc<Mutex<std::time::Duration>>,
    /// Server-side ceiling on `execution_timeout` (issue #243).
    /// `None` = no ceiling enforced.
    max_workflow_execution_timeout: Arc<Mutex<Option<std::time::Duration>>>,
}

impl Default for HarvestApiState {
    fn default() -> Self {
        Self {
            runtime: Arc::default(),
            storage_pool: Arc::default(),
            worker_stale_threshold: Arc::new(Mutex::new(std::time::Duration::from_secs(10))),
            actor_extractor: Arc::default(),
            audit_retention_days: Arc::new(Mutex::new(None)),
            deployment_profile: Arc::new(Mutex::new("unknown".to_string())),
            admin_auth_boundary: Arc::new(Mutex::new(false)),
            admin_auth_session_key: Arc::new(Mutex::new("user_id".to_string())),
            health_requires_shard_readiness: Arc::new(Mutex::new(false)),
            worker_shutdown_timeout: Arc::new(Mutex::new(std::time::Duration::from_secs(30))),
            workflow_result_notification_urls: Arc::default(),
            workflow_result_max_wait: Arc::new(Mutex::new(std::time::Duration::from_secs(30))),
            query_timeout: Arc::new(Mutex::new(std::time::Duration::from_secs(5))),
            max_workflow_execution_timeout: Arc::new(Mutex::new(None)),
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

    /// Override the per-query execution timeout (default 5 s, issue #234).
    ///
    /// Call this at startup with `WorkerConfig::query_timeout` so the management
    /// API honours the same timeout as the worker's in-process query dispatch.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_query_timeout(&self, timeout: std::time::Duration) {
        *self
            .query_timeout
            .lock()
            .expect("harvest api state lock poisoned") = timeout;
    }

    /// Current per-query execution timeout.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn query_timeout(&self) -> std::time::Duration {
        *self
            .query_timeout
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Set the server-side ceiling for workflow execution timeouts (issue #243).
    ///
    /// Call this during startup from the plugin to propagate
    /// `BuiltHarvest::max_workflow_execution_timeout` into the API state so the
    /// `POST /workflows` handler can apply the cap to every start request.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_max_workflow_execution_timeout(&self, ceiling: Option<std::time::Duration>) {
        *self
            .max_workflow_execution_timeout
            .lock()
            .expect("harvest api state lock poisoned") = ceiling;
    }

    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn max_workflow_execution_timeout(&self) -> Option<std::time::Duration> {
        *self
            .max_workflow_execution_timeout
            .lock()
            .expect("harvest api state lock poisoned")
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

    /// Record the host application's deployment profile for preflight checks.
    ///
    /// `dev` allows an unauthenticated local management API; every other
    /// profile is treated as non-dev and must have an auth boundary.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_deployment_profile(&self, profile: impl Into<String>) {
        *self
            .deployment_profile
            .lock()
            .expect("harvest api state lock poisoned") = profile.into();
    }

    /// Mark whether the Harvest management API is mounted behind auth.
    ///
    /// This reports the boundary provided via [`crate::plugin::HarvestPlugin::api_with_auth`]
    /// or an equivalent standalone integration. It does not implement RBAC.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_admin_auth_boundary(&self, present: bool) {
        *self
            .admin_auth_boundary
            .lock()
            .expect("harvest api state lock poisoned") = present;
    }

    /// Set the Autumn session key used by built-in management guards.
    ///
    /// This mirrors `AppState::auth_session_key()` during plugin startup. Standalone
    /// integrations that mount `harvest_api_router` directly can call this to keep
    /// Harvest's built-in high-impact route guard aligned with their app auth config.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_admin_auth_session_key(&self, session_key: impl Into<String>) {
        *self
            .admin_auth_session_key
            .lock()
            .expect("harvest api state lock poisoned") = session_key.into();
    }

    /// Configure `/health` to fail when writable shard rollout readiness is not `ready`.
    ///
    /// The default is `false` so local single-shard development keeps a cheap
    /// liveness-style health check. Production deployments can enable this to
    /// make readiness probes and rollout pipelines gate on worker and scheduler
    /// coverage before accepting starts.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_health_requires_shard_readiness(&self, required: bool) {
        *self
            .health_requires_shard_readiness
            .lock()
            .expect("harvest api state lock poisoned") = required;
    }

    /// Returns `Some(days)` only when explicitly set via [`HarvestApiState::set_audit_retention_days`];
    /// `None` means "use the builder's retention config unchanged".
    pub(crate) fn audit_retention_days(&self) -> Option<i64> {
        *self
            .audit_retention_days
            .lock()
            .expect("harvest api state lock poisoned")
    }

    pub(crate) fn deployment_profile(&self) -> String {
        self.deployment_profile
            .lock()
            .expect("harvest api state lock poisoned")
            .clone()
    }

    pub(crate) fn admin_auth_boundary(&self) -> bool {
        *self
            .admin_auth_boundary
            .lock()
            .expect("harvest api state lock poisoned")
    }

    pub(crate) fn admin_auth_session_key(&self) -> String {
        self.admin_auth_session_key
            .lock()
            .expect("harvest api state lock poisoned")
            .clone()
    }

    fn health_requires_shard_readiness(&self) -> bool {
        *self
            .health_requires_shard_readiness
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Override the default deadline applied when `POST /workers/{id}/drain` does not
    /// supply a `deadline_at`. Defaults to 30 s (the `WorkerConfig::shutdown_timeout`
    /// default). Set this at startup from the actual `WorkerConfig`.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_worker_shutdown_timeout(&self, timeout: std::time::Duration) {
        *self
            .worker_shutdown_timeout
            .lock()
            .expect("harvest api state lock poisoned") = timeout;
    }

    pub(crate) fn worker_shutdown_timeout(&self) -> std::time::Duration {
        *self
            .worker_shutdown_timeout
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Configure the default-shard database URL used by workflow result waits.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_workflow_result_notification_database_url(&self, url: impl Into<String>) {
        self.set_workflow_result_notification_database_urls([(ShardId::new(0), url)]);
    }

    /// Configure per-shard database URLs used by workflow result waits.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_workflow_result_notification_database_urls<I, S>(&self, urls: I)
    where
        I: IntoIterator<Item = (ShardId, S)>,
        S: Into<String>,
    {
        *self
            .workflow_result_notification_urls
            .lock()
            .expect("harvest api state lock poisoned") = urls
            .into_iter()
            .map(|(shard, url)| (shard, url.into()))
            .collect();
    }

    /// Override the maximum long-poll wait for workflow result requests.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_workflow_result_max_wait(&self, max_wait: std::time::Duration) {
        *self
            .workflow_result_max_wait
            .lock()
            .expect("harvest api state lock poisoned") = max_wait;
    }

    pub(crate) fn workflow_result_max_wait(&self) -> std::time::Duration {
        *self
            .workflow_result_max_wait
            .lock()
            .expect("harvest api state lock poisoned")
    }

    fn workflow_result_notification_database_urls(
        &self,
    ) -> HarvestResult<BTreeMap<ShardId, String>> {
        let urls = self
            .workflow_result_notification_urls
            .lock()
            .expect("harvest api state lock poisoned")
            .clone();
        if urls.is_empty() {
            return Err(HarvestError::Config(
                "workflow result notification database URL is not configured".to_string(),
            ));
        }
        Ok(urls)
    }

    fn workflow_handle_client(&self) -> HarvestResult<WorkflowHandleClient> {
        let pool = self.storage_pool()?;
        let runtime = self.runtime()?;
        let urls = self.workflow_result_notification_database_urls()?;
        Ok(WorkflowHandleClient::new(
            pool.sharded_pool().clone(),
            runtime.router().clone(),
            urls,
        ))
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

    pub(crate) fn runtime(&self) -> HarvestResult<HarvestApiRuntime> {
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

impl HarvestApiRuntime {
    pub(crate) const fn registry(&self) -> &Arc<HandlerRegistry> {
        &self.registry
    }

    pub(crate) const fn dags(&self) -> &Arc<DagCatalog> {
        &self.dags
    }

    pub(crate) fn queues(&self) -> &[String] {
        &self.queues
    }

    pub(crate) fn scheduler_snapshot(&self) -> SchedulerSnapshot {
        self.scheduler.snapshot()
    }

    pub(crate) const fn retention_config(&self) -> &RetentionConfig {
        &self.retention.config
    }
}

#[derive(Debug, Serialize)]
struct WorkflowDetailsResponse {
    parent_id: Option<uuid::Uuid>,
    execution: WorkflowExecution,
    history: Vec<Value>,
    external_handoffs: Vec<ExternalHandoffResponse>,
}

#[derive(Debug, Serialize)]
struct WorkflowStackResponse {
    exec_id: String,
    workflow_id: String,
    workflow_name: String,
    state: String,
    is_terminal: bool,
    pending_activities: Vec<PendingActivity>,
    pending_external_handoffs: Vec<ExternalHandoffResponse>,
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

#[derive(Debug, Serialize)]
struct ExternalHandoffListResponse {
    status: String,
    items: Vec<ExternalHandoffResponse>,
    shard_coverage: ExternalHandoffShardCoverage,
}

#[derive(Debug, Serialize)]
struct ExternalHandoffDetailResponse {
    status: String,
    item: ExternalHandoffResponse,
    shard_coverage: ExternalHandoffShardCoverage,
}

#[derive(Debug, Serialize, Clone)]
struct ExternalHandoffShardCoverage {
    #[serde(rename = "inspected_shards")]
    inspected: Vec<i32>,
    #[serde(rename = "matched_shards")]
    matched: Vec<i32>,
    #[serde(rename = "unavailable_shards")]
    unavailable: Vec<UnavailableShard>,
}

#[derive(Debug, Serialize, Clone)]
struct UnavailableShard {
    shard_id: i32,
    reason: String,
}

#[derive(Debug, Serialize)]
struct ExternalHandoffResponse {
    token: String,
    workflow: ExternalHandoffWorkflow,
    activity: ExternalHandoffActivity,
    state: String,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    deadline_at: chrono::DateTime<chrono::Utc>,
    complete_path: String,
    fail_path: String,
    heartbeat_path: String,
    payloads: RedactedPayloadSummary,
}

#[derive(Debug, Serialize)]
struct ExternalHandoffWorkflow {
    execution_id: String,
    workflow_id: String,
    workflow_name: String,
    shard_id: i32,
}

#[derive(Debug, Serialize)]
struct ExternalHandoffActivity {
    activity_id: String,
    activity_name: String,
}

#[derive(Debug, Serialize)]
struct RedactedPayloadSummary {
    redacted: bool,
    summary: &'static str,
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
    shard_readiness_enforced: bool,
    shard_readiness: Option<ShardHealthReport>,
}

#[derive(Debug, Deserialize)]
struct ShardHealthQuery {
    candidate_shard: Option<i32>,
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

/// Request body for `POST /workflows/{workflow_name}/signal-with-start` (issue #244).
#[derive(Debug, Deserialize)]
struct SignalWithStartRequest {
    workflow_id: String,
    #[serde(default)]
    start_input: Option<Value>,
    signal_name: String,
    #[serde(default)]
    signal_payload: Option<Value>,
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    memo: Option<Value>,
    #[serde(default)]
    search_attrs: Option<Value>,
    #[serde(default)]
    execution_timeout_secs: Option<i64>,
    /// Same wire values as `POST /workflows/.../start` (`allow_duplicate`,
    /// `reject_duplicate`, `allow_duplicate_failed_only`, `terminate_if_running`).
    #[serde(default)]
    id_reuse_policy: Option<String>,
    /// Optional dedup key applied to the signal row. Repeated requests with
    /// the same `(execution_id, idempotency_key)` deliver the signal exactly
    /// once. Typically a stable upstream event id (Stripe `Stripe-Idempotency-Key`,
    /// GitHub `X-GitHub-Delivery`, etc.).
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct SignalWithStartResponse {
    execution_id: String,
    workflow_name: String,
    workflow_id: String,
    state: String,
    started_fresh: bool,
    signal_delivered: bool,
}

impl SignalWithStartResponse {
    fn from_outcome(outcome: SignalWithStartOutcome) -> Self {
        Self {
            execution_id: outcome.exec_id.to_string(),
            workflow_name: outcome.workflow_name,
            workflow_id: outcome.workflow_id,
            state: outcome.state,
            started_fresh: outcome.started_fresh,
            signal_delivered: outcome.signal_delivered,
        }
    }
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
#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ScheduleKind {
    Dag,
    Workflow,
}

/// Summary of the most recent backfill for a schedule (surfaced in `GET /admin/schedules`).
#[derive(Debug, Serialize)]
struct BackfillSummary {
    id: uuid::Uuid,
    actor: String,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
    dry_run: bool,
    total: i32,
    dispatched: i32,
    skipped: i32,
    failed: i32,
    status: String,
    error_summary: Option<String>,
    started_at: chrono::DateTime<chrono::Utc>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<BackfillLogRow> for BackfillSummary {
    fn from(r: BackfillLogRow) -> Self {
        Self {
            id: r.id,
            actor: r.actor,
            from: r.from_ts,
            to: r.to_ts,
            dry_run: r.dry_run,
            total: r.total,
            dispatched: r.dispatched,
            skipped: r.skipped,
            failed: r.failed,
            status: r.status,
            error_summary: r.error_summary,
            started_at: r.started_at,
            completed_at: r.completed_at,
        }
    }
}

/// A single schedule entry in the `GET /admin/schedules` list.
#[derive(Debug, Serialize)]
struct ScheduleEntry {
    id: uuid::Uuid,
    kind: ScheduleKind,
    name: String,
    schedule_expr: Option<String>,
    /// IANA timezone the cron expression is evaluated in.
    /// Always `"UTC"` for `Schedule::Cron` and non-cron schedules.
    timezone: String,
    is_paused: bool,
    paused_at: Option<chrono::DateTime<chrono::Utc>>,
    paused_by: Option<String>,
    pause_reason: Option<String>,
    next_run_at: Option<chrono::DateTime<chrono::Utc>>,
    last_run_at: Option<chrono::DateTime<chrono::Utc>>,
    max_active_runs: i32,
    catchup: bool,
    last_backfill: Option<BackfillSummary>,
    /// Maximum jitter window in seconds. 0 means no jitter.
    jitter_secs: i64,
    /// Effective next fire time = `next_run_at` + deterministic jitter offset.
    /// `None` when `next_run_at` is `None` or jitter is zero.
    effective_fire_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Overlap policy for this schedule (e.g. `"skip"`, `"buffer_one"`).
    overlap_policy: String,
    /// Number of firings currently buffered (non-zero only for `buffer_one` / `buffer_all`).
    buffered_count: usize,
    /// Maximum buffered slots under `buffer_all`. 0 for other policies.
    buffer_all_max: i32,
}

/// Optional request body for `POST /admin/schedules/{id}/pause` and `…/resume`.
#[derive(Debug, Deserialize, Default)]
struct PauseResumeRequest {
    #[serde(default)]
    reason: Option<String>,
}

/// Request body for `POST /admin/schedules/workflow`.
#[derive(Debug, Deserialize)]
struct CreateWorkflowScheduleRequest {
    workflow_name: String,
    schedule_expr: String,
    /// IANA timezone the cron expression is evaluated in (e.g.
    /// `"America/Los_Angeles"`, `"Europe/London"`, `"UTC"`).
    ///
    /// Defaults to `"UTC"`, preserving the pre-existing behavior.
    /// Ignored for `interval:*` and `manual` expressions.
    #[serde(default = "default_timezone")]
    timezone: String,
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
    /// Jitter window in seconds. `0` disables jitter (default).
    #[serde(default)]
    jitter_secs: u64,
    /// Overlap policy string (e.g. `"skip"`, `"buffer_one"`, `"buffer_all"`,
    /// `"cancel_other"`, `"terminate_other"`). Defaults to `"skip"`.
    #[serde(default = "default_overlap_policy")]
    overlap_policy: String,
    /// Maximum buffered slots under `BufferAll`. Defaults to `100`.
    #[serde(default = "default_buffer_all_max")]
    buffer_all_max: u32,
}

fn default_queue_name() -> String {
    "default".to_string()
}

fn default_timezone() -> String {
    "UTC".to_string()
}

const fn default_max_active_runs() -> u32 {
    1
}

fn default_overlap_policy() -> String {
    "skip".to_string()
}

const fn default_buffer_all_max() -> u32 {
    100
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
const DEFAULT_EXTERNAL_HANDOFF_LIMIT: i64 = 100;
const MAX_EXTERNAL_HANDOFF_LIMIT: i64 = 500;
const DEFAULT_HISTORY_BATCH_EXPORT_LIMIT: usize = 100;
const MAX_HISTORY_BATCH_EXPORT_LIMIT: usize = 1_000;

#[derive(Debug, Default, Clone)]
pub(crate) struct WorkflowFilters {
    pub(crate) limit: i64,
    pub(crate) states: Vec<String>,
    pub(crate) workflow_name: Option<String>,
    pub(crate) search_attrs: Vec<Value>,
    pub(crate) started_after: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) started_before: Option<chrono::DateTime<chrono::Utc>>,
    /// Prefix match on the execution UUID cast to text (e.g. "abc123").
    pub(crate) exec_id_prefix: Option<String>,
}

impl WorkflowFilters {
    pub(crate) const fn with_limit(mut self, limit: i64) -> Self {
        self.limit = limit;
        self
    }
}

#[derive(Debug, Clone)]
struct HistoryExportQuery {
    payload_policy: HistoryPayloadPolicy,
    max_bytes: usize,
}

#[derive(Debug, Clone)]
struct HistoryBatchExportQuery {
    payload_policy: HistoryPayloadPolicy,
    max_bytes: usize,
    workflow_name: Option<String>,
    states: Vec<String>,
    updated_after: Option<chrono::DateTime<chrono::Utc>>,
    updated_before: Option<chrono::DateTime<chrono::Utc>>,
    shard_id: Option<i32>,
    limit: usize,
}

#[derive(Debug, Serialize)]
struct HistoryBatchExportResponse {
    status: String,
    observed_at: chrono::DateTime<chrono::Utc>,
    payload_policy: HistoryPayloadPolicy,
    filters: HistoryBatchExportFiltersResponse,
    exports: Vec<HistoryExportDocument>,
    failures: Vec<HistoryExportFailure>,
    shard_coverage: ExternalHandoffShardCoverage,
}

#[derive(Debug, Serialize)]
struct HistoryBatchExportFiltersResponse {
    workflow_name: Option<String>,
    states: Vec<String>,
    updated_after: Option<chrono::DateTime<chrono::Utc>>,
    updated_before: Option<chrono::DateTime<chrono::Utc>>,
    shard_id: Option<i32>,
    limit: usize,
    max_bytes: usize,
}

#[derive(Debug, Serialize)]
struct HistoryExportFailure {
    execution_id: Option<String>,
    shard_id: i32,
    reason: String,
    actual_bytes: Option<usize>,
    max_bytes: Option<usize>,
}

#[derive(Debug, Clone, diesel::QueryableByName)]
struct HistoryExportCandidate {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::Int4)]
    shard_id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    state: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    last_history_event_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Default)]
struct HistoryBatchExportWork {
    candidates: Vec<HistoryExportCandidate>,
    exports: Vec<HistoryExportDocument>,
    failures: Vec<HistoryExportFailure>,
    inspected_shards: Vec<i32>,
    matched_shards: Vec<i32>,
    unavailable_shards: Vec<UnavailableShard>,
    saw_requested_shard: bool,
}

impl HistoryBatchExportWork {
    fn note_unavailable(&mut self, shard_id: i32, reason: String) {
        self.unavailable_shards
            .push(UnavailableShard { shard_id, reason });
    }

    fn normalize_coverage(&mut self) {
        self.inspected_shards.sort_unstable();
        self.inspected_shards.dedup();
        self.matched_shards.sort_unstable();
        self.matched_shards.dedup();
        self.unavailable_shards.sort_by_key(|shard| shard.shard_id);
    }

    fn status(&self) -> String {
        if self.unavailable_shards.is_empty() && self.failures.is_empty() {
            "complete"
        } else {
            "partial"
        }
        .to_string()
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

#[allow(clippy::too_many_lines)]
pub fn harvest_api_router(api_state: HarvestApiState) -> Router<AppState> {
    let require_admin = middleware::from_fn_with_state(api_state.clone(), require_harvest_admin);

    Router::new()
        .route("/workflows", get(list_workflows))
        .route(
            "/workflows/{id}/history/export",
            get(export_workflow_history),
        )
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{id}/result", get(get_workflow_result))
        .route("/workflows/{id}/children", get(list_workflow_children))
        .route("/workflows/{id}/stack", get(get_workflow_stack))
        .route("/workflows/{workflow_name}/start", post(start_workflow))
        .route(
            "/workflows/{workflow_name}/signal-with-start",
            post(signal_with_start_workflow),
        )
        .route(
            "/workflows/{id}/cancel",
            post(cancel_workflow).route_layer(require_admin.clone()),
        )
        .route("/workflows/{id}/reset", post(reset_workflow))
        .route(
            "/workflows/{id}/signal/{signal_name}",
            post(signal_workflow),
        )
        .route(
            "/workflows/{id}/query/{query_name}",
            get(query_workflow).post(query_workflow_post),
        )
        .route("/workflows/{id}/queries", get(list_workflow_queries))
        // Handler discovery (issue #346): enumerate declarative queries and updates for a type.
        .route(
            "/workflows/types/{workflow_name}/handlers",
            get(list_workflow_type_handlers),
        )
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
        .route(
            "/dead-letters",
            get(list_dead_letters).route_layer(require_admin.clone()),
        )
        .route(
            "/dead-letters/replay",
            post(bulk_replay_dead_letters_handler).route_layer(require_admin.clone()),
        )
        .route(
            "/dead-letters/discard",
            post(bulk_discard_dead_letters_handler).route_layer(require_admin.clone()),
        )
        .route(
            "/dead-letters/{id}/replay",
            post(replay_dead_letter).route_layer(require_admin.clone()),
        )
        .route("/health", get(health))
        .route("/admin/preflight", get(preflight))
        .route("/admin/shards/health", get(shards_health))
        .route("/admin/version-gates/usage", get(version_usage))
        .route(
            "/admin/version-gates/retirement-check",
            get(version_gate_retirement_check),
        )
        .route("/admin/retention", get(retention_status))
        .route("/admin/retention/run-now", post(retention_run_now))
        .route("/admin/concurrency", get(concurrency_status))
        .route("/admin/history/exports", get(export_workflow_histories))
        .route("/admin/external-handoffs", get(list_external_handoffs))
        .route(
            "/admin/external-handoffs/{token}",
            get(get_external_handoff),
        )
        // Schedule management (issue #91): unified list + workflow-schedule CRUD.
        // Schedule backfill (issue #177): bounded missed-run recovery.
        .route("/admin/schedules", get(list_schedules))
        .route("/admin/schedules/workflow", post(create_workflow_schedule))
        .route("/admin/schedules/{id}", get(get_schedule))
        .route("/admin/schedules/{id}/pause", post(pause_schedule))
        .route("/admin/schedules/{id}/resume", post(resume_schedule))
        .route("/admin/schedules/{id}/backfill", post(schedule_backfill))
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
        // Worker fleet observability (issue #100) + remote drain (issue #170).
        // Static paths (/workers/health, /workers/drain-preview) must be
        // registered before /workers/{worker_id} so axum does not capture the
        // literal segments as the worker_id path parameter.
        .route("/workers/health", get(workers_health))
        .route("/workers/drain-preview", get(drain_preview_handler))
        .route("/workers", get(list_workers_handler))
        .route("/workers/{worker_id}", get(get_worker_handler))
        .route("/workers/{worker_id}/drain", post(request_drain_handler))
        .route(
            "/workers/{worker_id}/pinned",
            get(worker_pinned_executions_handler),
        )
        // Batch operations (issue #102): operator-facing fleet-wide cancel /
        // terminate / signal so an incident commander does not have to script
        // a one-off loop over GET /workflows.
        .route("/batch-operations", get(list_batch_operations))
        .route(
            "/batch-operations",
            post(submit_batch_operation).route_layer(require_admin),
        )
        .route("/batch-operations/{id}", get(get_batch_operation))
        // Audit trail (issue #158): read-only endpoint to query management
        // API mutations. See `audit::ALL_MUTATION_ROUTES` for covered paths.
        .route("/admin/audit", get(list_audit_records))
        .layer(Extension(api_state))
}

pub(crate) async fn require_harvest_admin(
    State(api_state): State<HarvestApiState>,
    request: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    let session = request.extensions().get::<Session>().cloned();
    if has_harvest_admin_access(&api_state, session).await {
        next.run(request).await
    } else {
        AutumnError::unauthorized_msg("authentication required").into_response()
    }
}

async fn has_harvest_admin_access(api_state: &HarvestApiState, session: Option<Session>) -> bool {
    if api_state.admin_auth_boundary() {
        return true;
    }

    let session_key = api_state.admin_auth_session_key();
    if let Some(session) = session {
        session.contains_key(&session_key).await
    } else {
        false
    }
}

/// Canonical `(METHOD, path-template)` list for every route in `harvest_api_router`.
///
/// The contract regression test compares this list against `docs/api-contract.json`;
/// update both together whenever routes change.
#[must_use]
pub const fn management_api_routes() -> &'static [(&'static str, &'static str)] {
    &[
        // ── workflows ────────────────────────────────────────────────────────
        ("GET", "/workflows"),
        ("GET", "/workflows/{id}"),
        ("GET", "/workflows/{id}/result"),
        ("GET", "/workflows/{id}/history/export"),
        ("GET", "/workflows/{id}/children"),
        ("GET", "/workflows/{id}/stack"),
        ("POST", "/workflows/{workflow_name}/start"),
        ("POST", "/workflows/{workflow_name}/signal-with-start"),
        ("POST", "/workflows/{id}/cancel"),
        ("POST", "/workflows/{id}/reset"),
        ("POST", "/workflows/{id}/signal/{signal_name}"),
        ("GET", "/workflows/{id}/queries"),
        ("GET", "/workflows/{id}/query/{query_name}"),
        ("POST", "/workflows/{id}/query/{query_name}"),
        ("POST", "/workflows/{id}/update/{update_name}"),
        ("GET", "/workflows/{id}/update/{update_id}/result"),
        // ── DAGs ─────────────────────────────────────────────────────────────
        ("GET", "/dags"),
        ("GET", "/dags/{dag_name}/runs"),
        ("POST", "/dags/{dag_name}/trigger"),
        ("PATCH", "/dags/{dag_name}"),
        // ── dead-letter queue ─────────────────────────────────────────────────
        ("GET", "/dead-letters"),
        ("POST", "/dead-letters/replay"),
        ("POST", "/dead-letters/discard"),
        ("POST", "/dead-letters/{id}/replay"),
        // ── external activity handoff (issue #92) ────────────────────────────
        ("POST", "/activities/external/{token}/complete"),
        ("POST", "/activities/external/{token}/fail"),
        ("POST", "/activities/external/{token}/heartbeat"),
        // ── workers (issues #100, #170, #235) ────────────────────────────────
        ("GET", "/workers"),
        ("GET", "/workers/{worker_id}"),
        ("GET", "/workers/health"),
        ("GET", "/workers/drain-preview"),
        ("POST", "/workers/{worker_id}/drain"),
        ("GET", "/workers/{worker_id}/pinned"),
        // ── batch operations (issue #102) ─────────────────────────────────────
        ("GET", "/batch-operations"),
        ("POST", "/batch-operations"),
        ("GET", "/batch-operations/{id}"),
        // ── health & admin ────────────────────────────────────────────────────
        ("GET", "/health"),
        ("GET", "/admin/preflight"),
        ("GET", "/admin/shards/health"),
        ("GET", "/admin/version-gates/usage"),
        ("GET", "/admin/version-gates/retirement-check"),
        ("GET", "/admin/retention"),
        ("POST", "/admin/retention/run-now"),
        ("GET", "/admin/concurrency"),
        ("GET", "/admin/history/exports"),
        ("GET", "/admin/external-handoffs"),
        ("GET", "/admin/external-handoffs/{token}"),
        // ── schedules (issues #91, #177, #229) ───────────────────────────────
        ("GET", "/admin/schedules"),
        ("GET", "/admin/schedules/{id}"),
        ("POST", "/admin/schedules/workflow"),
        ("POST", "/admin/schedules/{id}/pause"),
        ("POST", "/admin/schedules/{id}/resume"),
        ("POST", "/admin/schedules/{id}/backfill"),
        ("DELETE", "/admin/schedules/{id}"),
        // ── audit (issue #158) ────────────────────────────────────────────────
        ("GET", "/admin/audit"),
    ]
}

/// Canonical request-body field registry for every mutating management route.
///
/// Each entry is `(method, path_template, fields)` where `fields` is
/// `Some(&[...])` for a structured body or `None` for a free-form body.
/// Compared against `docs/api-contract.json` by the regression test; update
/// both together when adding, removing, or renaming a request field.
#[must_use]
#[allow(clippy::too_many_lines)]
pub const fn management_api_request_fields()
-> &'static [(&'static str, &'static str, Option<&'static [&'static str]>)] {
    &[
        // ── workflows ────────────────────────────────────────────────────────
        (
            "POST",
            "/workflows/{workflow_name}/start",
            Some(&[
                "workflow_id",
                "input",
                "queue",
                "memo",
                "search_attrs",
                "execution_timeout_secs",
                "reuse_policy",
            ]),
        ),
        (
            "POST",
            "/workflows/{workflow_name}/signal-with-start",
            Some(&[
                "workflow_id",
                "start_input",
                "signal_name",
                "signal_payload",
                "queue",
                "memo",
                "search_attrs",
                "execution_timeout_secs",
                "id_reuse_policy",
                "idempotency_key",
            ]),
        ),
        ("POST", "/workflows/{id}/cancel", Some(&["reason"])),
        (
            "POST",
            "/workflows/{id}/reset",
            Some(&[
                "reset_to_event_id",
                "reason",
                "operator_id",
                "signal_reapply",
            ]),
        ),
        ("POST", "/workflows/{id}/signal/{signal_name}", None), // free-form
        (
            "POST",
            "/workflows/{id}/query/{query_name}",
            Some(&["args"]),
        ),
        (
            "POST",
            "/workflows/{id}/update/{update_name}",
            Some(&["input"]),
        ),
        // ── DAGs ─────────────────────────────────────────────────────────────
        ("POST", "/dags/{dag_name}/trigger", Some(&["conf"])),
        ("PATCH", "/dags/{dag_name}", Some(&["paused"])),
        // ── dead-letter queue ─────────────────────────────────────────────────
        (
            "POST",
            "/dead-letters/replay",
            Some(&[
                "activity_name",
                "workflow_name",
                "failed_after",
                "failed_before",
                "limit",
                "dry_run",
            ]),
        ),
        (
            "POST",
            "/dead-letters/discard",
            Some(&[
                "activity_name",
                "workflow_name",
                "failed_after",
                "failed_before",
                "limit",
                "dry_run",
            ]),
        ),
        ("POST", "/dead-letters/{id}/replay", Some(&[])),
        // ── external activity handoff ─────────────────────────────────────────
        (
            "POST",
            "/activities/external/{token}/complete",
            Some(&["output"]),
        ),
        (
            "POST",
            "/activities/external/{token}/fail",
            Some(&["error", "retryable"]),
        ),
        (
            "POST",
            "/activities/external/{token}/heartbeat",
            Some(&["extend_by_secs"]),
        ),
        // ── workers ───────────────────────────────────────────────────────────
        ("POST", "/workers/{worker_id}/drain", Some(&["deadline_at"])),
        // ── batch operations ──────────────────────────────────────────────────
        (
            "POST",
            "/batch-operations",
            Some(&[
                "action",
                "filter",
                "signal_name",
                "signal_payload",
                "idempotency_key",
                "created_by",
            ]),
        ),
        // ── admin ─────────────────────────────────────────────────────────────
        ("POST", "/admin/retention/run-now", Some(&[])),
        (
            "POST",
            "/admin/schedules/workflow",
            Some(&[
                "workflow_name",
                "schedule_expr",
                "input",
                "max_active_runs",
                "catchup",
                "paused",
                "queue_name",
            ]),
        ),
        ("POST", "/admin/schedules/{id}/pause", Some(&["reason"])),
        // Resume accepts an optional body for forward-compatibility but reason is not persisted
        // (pause_reason is cleared on resume and AuditRecord has no free-text notes field).
        ("POST", "/admin/schedules/{id}/resume", Some(&[])),
        (
            "POST",
            "/admin/schedules/{id}/backfill",
            Some(&["from", "to", "dry_run", "include_paused", "max_count"]),
        ),
        ("DELETE", "/admin/schedules/{id}", Some(&[])),
    ]
}

/// Canonical success-response field registry for every management route.
///
/// Each entry is `(method, path_template, fields)` where `fields` is
/// `Some(&[...])` for a structured object response or `None` for a free-form
/// response (array, external model, or polymorphic).  Compared against
/// `docs/api-contract.json` by the regression test; update both together when
/// adding, removing, or renaming a top-level response field.
#[must_use]
#[allow(clippy::too_many_lines)]
pub const fn management_api_response_fields()
-> &'static [(&'static str, &'static str, Option<&'static [&'static str]>)] {
    &[
        // ── workflows ────────────────────────────────────────────────────────
        ("GET", "/workflows", None), // Vec<WorkflowExecution>
        (
            "GET",
            "/workflows/{id}",
            Some(&["parent_id", "execution", "history", "external_handoffs"]),
        ),
        (
            "GET",
            "/workflows/{id}/result",
            Some(&["state", "output", "error", "completed_at"]),
        ),
        ("GET", "/workflows/{id}/history/export", None), // HistoryExportDocument (external)
        (
            "GET",
            "/workflows/{id}/children",
            Some(&["items", "next_cursor"]),
        ),
        (
            "GET",
            "/workflows/{id}/stack",
            Some(&[
                "exec_id",
                "workflow_id",
                "workflow_name",
                "state",
                "is_terminal",
                "pending_activities",
                "pending_external_handoffs",
                "pending_local_activities",
                "pending_timers",
                "pending_signals",
                "buffered_signals",
                "pending_child_workflows",
                "last_event_id",
            ]),
        ),
        (
            "POST",
            "/workflows/{workflow_name}/start",
            Some(&["execution_id", "workflow_name", "workflow_id", "state"]),
        ),
        (
            "POST",
            "/workflows/{workflow_name}/signal-with-start",
            Some(&[
                "execution_id",
                "workflow_name",
                "workflow_id",
                "state",
                "started_fresh",
                "signal_delivered",
            ]),
        ),
        (
            "POST",
            "/workflows/{id}/cancel",
            Some(&[
                "ok",
                "execution_id",
                "state",
                "reason",
                "newly_cancelled",
                "failed_task_count",
            ]),
        ),
        (
            "POST",
            "/workflows/{id}/reset",
            Some(&[
                "new_exec_id",
                "reset_from_exec_id",
                "reset_to_event_id",
                "events_carried_over",
                "source_tasks_cancelled",
                "source_timers_removed",
                "source_signals_dropped",
                "source_signals_buffered",
            ]),
        ),
        (
            "POST",
            "/workflows/{id}/signal/{signal_name}",
            Some(&["ok"]),
        ),
        ("GET", "/workflows/{id}/queries", None), // Vec<String> query names
        ("GET", "/workflows/{id}/query/{query_name}", None), // opaque handler return
        (
            "POST",
            "/workflows/{id}/query/{query_name}",
            Some(&["result"]),
        ), // {"result": <value>}
        ("POST", "/workflows/{id}/update/{update_name}", None), // polymorphic admitted/completed/failed
        ("GET", "/workflows/{id}/update/{update_id}/result", None), // polymorphic completed/failed
        // ── DAGs ─────────────────────────────────────────────────────────────
        ("GET", "/dags", None),                     // Vec<DagSummary>
        ("GET", "/dags/{dag_name}/runs", None),     // Vec<WorkflowExecution>
        ("POST", "/dags/{dag_name}/trigger", None), // StartWorkflowResponse
        ("PATCH", "/dags/{dag_name}", None),        // HarvestSchedule (external model)
        // ── dead-letter queue ─────────────────────────────────────────────────
        ("GET", "/dead-letters", None), // Vec<DeadLetter> (external model)
        (
            "POST",
            "/dead-letters/replay",
            Some(&[
                "matched", "acted_on", "skipped", "ids", "dry_run", "failures",
            ]),
        ),
        (
            "POST",
            "/dead-letters/discard",
            Some(&[
                "matched", "acted_on", "skipped", "ids", "dry_run", "failures",
            ]),
        ),
        (
            "POST",
            "/dead-letters/{id}/replay",
            Some(&["ok", "dead_letter_id", "task_id"]),
        ),
        // ── external activity handoff ─────────────────────────────────────────
        (
            "POST",
            "/activities/external/{token}/complete",
            Some(&["ok", "newly_resolved", "status", "current_state"]),
        ),
        (
            "POST",
            "/activities/external/{token}/fail",
            Some(&["ok", "newly_resolved", "status", "current_state"]),
        ),
        (
            "POST",
            "/activities/external/{token}/heartbeat",
            Some(&["ok", "newly_resolved", "status", "current_state"]),
        ),
        // ── workers ───────────────────────────────────────────────────────────
        ("GET", "/workers", None), // Vec<WorkerRow> (external model)
        ("GET", "/workers/{worker_id}", None), // WorkerRow (external model)
        (
            "GET",
            "/workers/health",
            Some(&["healthy", "stale", "draining", "by_queue", "by_shard"]),
        ),
        ("GET", "/workers/drain-preview", None), // Vec<DrainPreviewItem>
        (
            "POST",
            "/workers/{worker_id}/drain",
            Some(&[
                "worker_id",
                "outcome",
                "in_flight_count",
                "drain_deadline_at",
                "shard_ids",
                "unavailable_shards",
            ]),
        ),
        // ── batch operations ──────────────────────────────────────────────────
        ("GET", "/batch-operations", None), // Vec<BatchJobView> (external model)
        ("POST", "/batch-operations", Some(&["batch_job_id"])),
        ("GET", "/batch-operations/{id}", None), // BatchJobView (external model)
        // ── health & admin ────────────────────────────────────────────────────
        (
            "GET",
            "/health",
            Some(&[
                "runtime_ready",
                "worker_id",
                "queues",
                "dag_count",
                "scheduler",
                "shard_readiness_enforced",
                "shard_readiness",
            ]),
        ),
        (
            "GET",
            "/admin/preflight",
            Some(&["overall_status", "observed_at", "version", "checks"]),
        ),
        (
            "GET",
            "/admin/shards/health",
            Some(&[
                "overall_readiness",
                "observed_at",
                "freshness_window_secs",
                "candidate_shard",
                "shards",
            ]),
        ),
        (
            "GET",
            "/admin/version-gates/usage",
            Some(&["status", "observed_at", "filters", "items", "shards"]),
        ),
        (
            "GET",
            "/admin/version-gates/retirement-check",
            Some(&[
                "status",
                "safe_to_retire",
                "observed_at",
                "filters",
                "blockers",
                "shards",
            ]),
        ),
        ("GET", "/admin/retention", None), // RetentionStatus (external model)
        ("POST", "/admin/retention/run-now", Some(&["ok"])),
        ("GET", "/admin/concurrency", None), // Vec<ConcurrencyKeyStats> (external model)
        (
            "GET",
            "/admin/history/exports",
            Some(&[
                "status",
                "observed_at",
                "payload_policy",
                "filters",
                "exports",
                "failures",
                "shard_coverage",
            ]),
        ),
        (
            "GET",
            "/admin/external-handoffs",
            Some(&["status", "items", "shard_coverage"]),
        ),
        (
            "GET",
            "/admin/external-handoffs/{token}",
            Some(&["status", "item", "shard_coverage"]),
        ),
        // ── schedules ─────────────────────────────────────────────────────────
        ("GET", "/admin/schedules", None), // Vec<ScheduleEntry>
        (
            "GET",
            "/admin/schedules/{id}",
            Some(&[
                "id",
                "kind",
                "name",
                "schedule_expr",
                "is_paused",
                "paused_at",
                "paused_by",
                "pause_reason",
                "next_run_at",
                "last_run_at",
                "max_active_runs",
                "catchup",
                "last_backfill",
            ]),
        ),
        (
            "POST",
            "/admin/schedules/workflow",
            Some(&[
                "id",
                "kind",
                "name",
                "schedule_expr",
                "is_paused",
                "paused_at",
                "paused_by",
                "pause_reason",
                "next_run_at",
                "last_run_at",
                "max_active_runs",
                "catchup",
                "last_backfill",
            ]),
        ),
        ("POST", "/admin/schedules/{id}/pause", Some(&["ok"])),
        ("POST", "/admin/schedules/{id}/resume", Some(&["ok"])),
        (
            "POST",
            "/admin/schedules/{id}/backfill",
            Some(&[
                "status",
                "schedule_id",
                "kind",
                "name",
                "from",
                "to",
                "planned_timestamps",
                "total",
                "dispatched",
                "skipped",
                "failed",
                "skipped_reasons",
                "partial_shard_failures",
                "paused_schedule_warning",
            ]),
        ),
        ("DELETE", "/admin/schedules/{id}", Some(&["ok"])),
        // ── audit ─────────────────────────────────────────────────────────────
        ("GET", "/admin/audit", None), // Vec<AuditRecord> (external model)
    ]
}

async fn preflight(
    Extension(api_state): Extension<HarvestApiState>,
    axum::extract::State(autumn_state): axum::extract::State<AppState>,
) -> Json<PreflightReport> {
    api_state.set_deployment_profile(autumn_state.profile().to_string());
    Json(build_preflight_report(&api_state).await)
}

async fn shards_health(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<ShardHealthQuery>,
) -> Json<ShardHealthReport> {
    Json(build_shard_health_report(&api_state, query.candidate_shard).await)
}

async fn version_usage(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<VersionUsageQuery>,
) -> axum::response::Response {
    match build_version_usage_report(&api_state, query).await {
        Ok(report) => Json(report).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn version_gate_retirement_check(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<RetirementCheckQuery>,
) -> axum::response::Response {
    match build_retirement_check_report(&api_state, query).await {
        Ok(report) => Json(report).into_response(),
        Err(error) => error.into_response(),
    }
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

fn parse_history_export_query(
    pairs: &[(String, String)],
) -> Result<HistoryExportQuery, AutumnError> {
    let mut payload_policy = HistoryPayloadPolicy::Redacted;
    let mut max_bytes = DEFAULT_HISTORY_EXPORT_MAX_BYTES;

    for (key, value) in pairs {
        match key.as_str() {
            "payload_policy" | "payload-policy" => {
                payload_policy = value.parse().map_err(AutumnError::bad_request_msg)?;
            }
            "max_bytes" | "max-bytes" => {
                max_bytes = parse_history_max_bytes(value)?;
            }
            _ => {}
        }
    }

    Ok(HistoryExportQuery {
        payload_policy,
        max_bytes,
    })
}

fn parse_history_batch_export_query(
    pairs: &[(String, String)],
) -> Result<HistoryBatchExportQuery, AutumnError> {
    let mut query = HistoryBatchExportQuery {
        payload_policy: HistoryPayloadPolicy::Redacted,
        max_bytes: DEFAULT_HISTORY_EXPORT_MAX_BYTES,
        workflow_name: None,
        states: Vec::new(),
        updated_after: None,
        updated_before: None,
        shard_id: None,
        limit: DEFAULT_HISTORY_BATCH_EXPORT_LIMIT,
    };

    for (key, value) in pairs {
        match key.as_str() {
            "payload_policy" | "payload-policy" => {
                query.payload_policy = value.parse().map_err(AutumnError::bad_request_msg)?;
            }
            "max_bytes" | "max-bytes" => {
                query.max_bytes = parse_history_max_bytes(value)?;
            }
            "workflow_name" | "workflow-name" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    query.workflow_name = Some(trimmed.to_string());
                }
            }
            "state_group" | "state-group" => {
                query.states = states_for_history_state_group(value)?;
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
                    if !query.states.contains(&owned) {
                        query.states.push(owned);
                    }
                }
            }
            "updated_after" | "updated-after" => {
                query.updated_after = Some(parse_history_datetime(value)?);
            }
            "updated_before" | "updated-before" => {
                query.updated_before = Some(parse_history_datetime(value)?);
            }
            "shard_id" | "shard-id" | "shard" => {
                query.shard_id = Some(value.parse::<i32>().map_err(|_| {
                    AutumnError::bad_request_msg(format!("invalid shard_id '{value}'"))
                })?);
            }
            "limit" => {
                let parsed = value.parse::<usize>().map_err(|_| {
                    AutumnError::bad_request_msg(format!("invalid limit '{value}'"))
                })?;
                query.limit = parsed.clamp(1, MAX_HISTORY_BATCH_EXPORT_LIMIT);
            }
            _ => {}
        }
    }

    Ok(query)
}

fn parse_history_max_bytes(value: &str) -> Result<usize, AutumnError> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid max_bytes '{value}'")))?;
    if parsed == 0 {
        return Err(AutumnError::bad_request_msg(
            "max_bytes must be greater than 0",
        ));
    }
    Ok(parsed)
}

fn parse_history_datetime(value: &str) -> Result<chrono::DateTime<chrono::Utc>, AutumnError> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|_| {
            AutumnError::bad_request_msg(format!(
                "invalid datetime '{value}'; expected RFC 3339 format, e.g. 2026-05-06T00:00:00Z"
            ))
        })
}

fn states_for_history_state_group(value: &str) -> Result<Vec<String>, AutumnError> {
    match value {
        "active" => Ok(vec!["RUNNING".to_string()]),
        "terminal" => Ok(terminal_workflow_states()),
        "all" => Ok(Vec::new()),
        other => Err(AutumnError::bad_request_msg(format!(
            "unknown state_group '{other}'; expected active, terminal, or all"
        ))),
    }
}

fn terminal_workflow_states() -> Vec<String> {
    KNOWN_WORKFLOW_STATES
        .iter()
        .copied()
        .filter(|state| is_terminal_state(state))
        .map(ToOwned::to_owned)
        .collect()
}

async fn export_workflow_history(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> axum::response::Response {
    let exec_id = match parse_execution_id(&id) {
        Ok(id) => id,
        Err(error) => return error.into_response(),
    };
    let query = match parse_history_export_query(&pairs) {
        Ok(query) => query,
        Err(error) => return error.into_response(),
    };
    let mut conn = match db_conn_for_execution(&api_state, exec_id).await {
        Ok(conn) => conn,
        Err(error) => return error.into_response(),
    };
    let execution = match load_execution(&mut conn, exec_id).await {
        Ok(execution) => execution,
        Err(error) => return map_error(error).into_response(),
    };
    let history = match store::load_history(&mut conn, exec_id).await {
        Ok(history) => history,
        Err(error) => return map_error(error).into_response(),
    };

    match export_history_for_execution(&execution, history.events, &query) {
        Ok(document) => Json(document).into_response(),
        Err(error) => history_export_error_response(error),
    }
}

async fn export_workflow_histories(
    Extension(api_state): Extension<HarvestApiState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> axum::response::Response {
    let query = match parse_history_batch_export_query(&pairs) {
        Ok(query) => query,
        Err(error) => return error.into_response(),
    };
    match load_history_exports_from_shards(&api_state, &query).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => error.into_response(),
    }
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
    let handoff_filters = external_task::ExternalHandoffFilters {
        states: vec!["PENDING".to_string()],
        execution_id: Some(exec_id),
        limit: MAX_EXTERNAL_HANDOFF_LIMIT,
        ..external_task::ExternalHandoffFilters::default()
    };
    let external_handoffs = external_task::list_external_handoffs(&mut conn, &handoff_filters)
        .await
        .map_err(map_error)?
        .into_iter()
        .map(ExternalHandoffResponse::from)
        .collect();

    Ok(Json(WorkflowDetailsResponse {
        parent_id: execution.parent_id,
        execution,
        history: events,
        external_handoffs,
    }))
}

async fn get_workflow_result(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> axum::response::Response {
    let exec_id = match parse_execution_id(&id) {
        Ok(id) => id,
        Err(error) => return error.into_response(),
    };
    let wait = match parse_workflow_result_wait_query(&pairs, api_state.workflow_result_max_wait())
    {
        Ok(wait) => wait,
        Err(error) => return error.into_response(),
    };

    if wait.is_zero() {
        let snapshot = match workflow_result_snapshot(&api_state, exec_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => return error.into_response(),
        };
        return workflow_result_response(snapshot);
    }

    let client = match api_state.workflow_handle_client() {
        Ok(client) => client,
        Err(error) => return map_error(error).into_response(),
    };
    let handle = client.handle(exec_id);

    match handle.result_snapshot_with_wait(wait).await {
        Ok(Some(snapshot)) => workflow_result_response(snapshot),
        Ok(None) => workflow_result_pending_response(),
        Err(error) => map_error(error).into_response(),
    }
}

async fn workflow_result_snapshot(
    api_state: &HarvestApiState,
    exec_id: ExecutionId,
) -> Result<WorkflowResult, AutumnError> {
    let mut conn = db_conn_for_execution(api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    Ok(WorkflowResult::from_execution(&execution))
}

fn workflow_result_response(result: WorkflowResult) -> axum::response::Response {
    if result.is_terminal() {
        (StatusCode::OK, Json(result)).into_response()
    } else {
        workflow_result_pending_response()
    }
}

fn workflow_result_pending_response() -> axum::response::Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        axum::http::header::RETRY_AFTER,
        axum::http::HeaderValue::from_static("1"),
    );
    response
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

impl From<external_task::ExternalHandoffRow> for ExternalHandoffResponse {
    fn from(row: external_task::ExternalHandoffRow) -> Self {
        let token = row.token.to_string();
        Self {
            complete_path: format!("/activities/external/{token}/complete"),
            fail_path: format!("/activities/external/{token}/fail"),
            heartbeat_path: format!("/activities/external/{token}/heartbeat"),
            token,
            workflow: ExternalHandoffWorkflow {
                execution_id: row.workflow_exec_id.to_string(),
                workflow_id: row.workflow_id,
                workflow_name: row.workflow_name,
                shard_id: row.shard_id,
            },
            activity: ExternalHandoffActivity {
                activity_id: row.activity_id.to_string(),
                activity_name: row.activity_name,
            },
            state: row.state,
            created_at: row.created_at,
            updated_at: row.updated_at,
            deadline_at: row.deadline_at,
            payloads: RedactedPayloadSummary {
                redacted: true,
                summary: "raw workflow inputs, activity inputs, outputs, signals, and secrets are redacted",
            },
        }
    }
}

async fn list_external_handoffs(
    Extension(api_state): Extension<HarvestApiState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<ExternalHandoffListResponse>, AutumnError> {
    let filters = parse_external_handoff_filters(&pairs)?;
    let limit = filters.limit;
    let (mut rows, coverage) = load_external_handoffs_from_shards(&api_state, &filters).await?;
    sort_external_handoff_rows(&mut rows);
    rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    let status = external_handoff_status(&coverage);

    Ok(Json(ExternalHandoffListResponse {
        status,
        items: rows
            .into_iter()
            .map(ExternalHandoffResponse::from)
            .collect(),
        shard_coverage: coverage,
    }))
}

async fn get_external_handoff(
    Extension(api_state): Extension<HarvestApiState>,
    Path(token_str): Path<String>,
) -> Result<Json<ExternalHandoffDetailResponse>, AutumnError> {
    let token = parse_external_token(&token_str)?;
    let filters = external_task::ExternalHandoffFilters {
        token: Some(token),
        limit: 1,
        ..external_task::ExternalHandoffFilters::default()
    };
    let (mut rows, coverage) = load_external_handoffs_from_shards(&api_state, &filters).await?;
    sort_external_handoff_rows(&mut rows);

    let Some(row) = rows.into_iter().next() else {
        if coverage.unavailable.is_empty() {
            return Err(AutumnError::not_found_msg(format!(
                "external handoff token {token}"
            )));
        }
        return Err(AutumnError::service_unavailable_msg(format!(
            "external handoff token {token} was not found on inspected shards; unavailable shards: {}",
            unavailable_shards_summary(&coverage.unavailable)
        )));
    };

    Ok(Json(ExternalHandoffDetailResponse {
        status: external_handoff_status(&coverage),
        item: ExternalHandoffResponse::from(row),
        shard_coverage: coverage,
    }))
}

fn parse_external_handoff_filters(
    pairs: &[(String, String)],
) -> Result<external_task::ExternalHandoffFilters, AutumnError> {
    let mut limit_raw = None;
    let mut filters =
        external_task::ExternalHandoffFilters::default().with_limit(DEFAULT_EXTERNAL_HANDOFF_LIMIT);

    for (key, value) in pairs {
        match key.as_str() {
            "state" => {
                for raw in value.split(',') {
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let state = parse_external_handoff_state(trimmed)?;
                    if !filters.states.contains(&state) {
                        filters.states.push(state);
                    }
                }
            }
            "workflow_name" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.workflow_name = Some(trimmed.to_string());
                }
            }
            "execution_id" => {
                filters.execution_id = Some(parse_execution_id(value)?);
            }
            "activity_name" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.activity_name = Some(trimmed.to_string());
                }
            }
            "token" => {
                filters.token = Some(parse_external_token(value)?);
            }
            "shard_id" | "shard" => {
                let shard_id = value.parse::<i32>().map_err(|_| {
                    AutumnError::bad_request_msg(format!("invalid shard_id '{value}'"))
                })?;
                filters.shard_id = Some(shard_id);
            }
            "due_before" => {
                filters.due_before = Some(parse_external_handoff_datetime(value, "due_before")?);
            }
            "updated_before" => {
                filters.updated_before =
                    Some(parse_external_handoff_datetime(value, "updated_before")?);
            }
            "limit" => {
                let parsed = value.parse::<i64>().map_err(|_| {
                    AutumnError::bad_request_msg(format!("invalid limit '{value}'"))
                })?;
                limit_raw = Some(parsed);
            }
            _ => {}
        }
    }

    let limit = limit_raw
        .unwrap_or(DEFAULT_EXTERNAL_HANDOFF_LIMIT)
        .clamp(1, MAX_EXTERNAL_HANDOFF_LIMIT);
    Ok(filters.with_limit(limit))
}

fn parse_external_handoff_state(raw: &str) -> Result<String, AutumnError> {
    let normalized = raw
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .flat_map(char::to_lowercase)
        .collect::<String>();
    let state = match normalized.as_str() {
        "pending" => "PENDING",
        "completed" => "COMPLETED",
        "failed" => "FAILED",
        "timedout" => "TIMED_OUT",
        "cancelled" | "canceled" => "CANCELLED",
        _ => {
            return Err(AutumnError::bad_request_msg(format!(
                "unknown external handoff state '{raw}'; expected one of {:?}",
                external_task::KNOWN_EXTERNAL_TASK_STATES
            )));
        }
    };
    Ok(state.to_string())
}

fn parse_external_handoff_datetime(
    raw: &str,
    label: &str,
) -> Result<chrono::DateTime<chrono::Utc>, AutumnError> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|_| {
            AutumnError::bad_request_msg(format!(
                "invalid {label} '{raw}'; expected RFC 3339 format, e.g. 2026-05-08T00:00:00Z"
            ))
        })
}

async fn load_external_handoffs_from_shards(
    api_state: &HarvestApiState,
    filters: &external_task::ExternalHandoffFilters,
) -> Result<
    (
        Vec<external_task::ExternalHandoffRow>,
        ExternalHandoffShardCoverage,
    ),
    AutumnError,
> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut rows = Vec::new();
    let mut inspected_shards = Vec::new();
    let mut matched_shards = Vec::new();
    let mut unavailable_shards = Vec::new();
    let mut matched_target = false;

    for (shard, shard_pool) in pool.iter_shards() {
        let shard_id = shard.as_i32();
        if filters.shard_id.is_some_and(|target| target != shard_id) {
            continue;
        }
        matched_target = true;

        let mut conn = match acquire_conn(shard_pool).await {
            Ok(conn) => conn,
            Err(error) => {
                unavailable_shards.push(UnavailableShard {
                    shard_id,
                    reason: error.to_string(),
                });
                continue;
            }
        };
        inspected_shards.push(shard_id);

        match external_task::list_external_handoffs(&mut conn, filters).await {
            Ok(mut shard_rows) => {
                if !shard_rows.is_empty() {
                    matched_shards.push(shard_id);
                }
                rows.append(&mut shard_rows);
            }
            Err(error) => unavailable_shards.push(UnavailableShard {
                shard_id,
                reason: error.to_string(),
            }),
        }
    }

    if let Some(shard_id) = filters.shard_id
        && !matched_target
    {
        unavailable_shards.push(UnavailableShard {
            shard_id,
            reason: "shard pool is not configured".to_string(),
        });
    }

    inspected_shards.sort_unstable();
    inspected_shards.dedup();
    matched_shards.sort_unstable();
    matched_shards.dedup();
    unavailable_shards.sort_by_key(|shard| shard.shard_id);

    Ok((
        rows,
        ExternalHandoffShardCoverage {
            inspected: inspected_shards,
            matched: matched_shards,
            unavailable: unavailable_shards,
        },
    ))
}

fn sort_external_handoff_rows(rows: &mut [external_task::ExternalHandoffRow]) {
    rows.sort_by(|left, right| {
        left.deadline_at
            .cmp(&right.deadline_at)
            .then_with(|| right.updated_at.cmp(&left.updated_at))
            .then_with(|| left.token.as_uuid().cmp(&right.token.as_uuid()))
    });
}

fn external_handoff_status(coverage: &ExternalHandoffShardCoverage) -> String {
    if coverage.unavailable.is_empty() {
        "ok"
    } else if coverage.inspected.is_empty() {
        "unavailable"
    } else {
        "partial"
    }
    .to_string()
}

fn unavailable_shards_summary(shards: &[UnavailableShard]) -> String {
    shards
        .iter()
        .map(|shard| format!("{} ({})", shard.shard_id, shard.reason))
        .collect::<Vec<_>>()
        .join(", ")
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
            pending_external_handoffs: Vec::new(),
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
    let handoff_filters = external_task::ExternalHandoffFilters {
        states: vec!["PENDING".to_string()],
        execution_id: Some(exec_id),
        limit: MAX_EXTERNAL_HANDOFF_LIMIT,
        ..external_task::ExternalHandoffFilters::default()
    };
    let external_handoff_rows = external_task::list_external_handoffs(&mut conn, &handoff_filters)
        .await
        .map_err(map_error)?;
    let external_pending = external_handoff_rows
        .iter()
        .map(|task| PendingActivity {
            activity_exec_id: task.activity_id.to_string(),
            activity_name: task.activity_name.clone(),
            queue: "external".to_string(),
            scheduled_at: task.created_at,
            attempt: 1,
            max_attempts: 1,
            task_status: task.state.clone(),
            claimed_by_worker_id: None,
            last_heartbeat_at: None,
            next_retry_at: None,
            schedule_to_start_deadline: None,
            start_to_close_deadline: Some(task.deadline_at),
            heartbeat_deadline: None,
        })
        .collect::<Vec<_>>();
    let pending_external_handoffs = external_handoff_rows
        .into_iter()
        .map(ExternalHandoffResponse::from)
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
        pending_external_handoffs,
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
    maybe_session: Option<Extension<Session>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<StartWorkflowRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    if matches!(
        request.reuse_policy.as_deref(),
        Some("terminate_if_running")
    ) && !has_harvest_admin_access(&api_state, maybe_session.map(|Extension(session)| session))
        .await
    {
        return AutumnError::unauthorized_msg("authentication required").into_response();
    }

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

    if runtime.is_registered_dag(&workflow_name) {
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
                error_summary: Some("registered DAG cannot be started via workflow route"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return AutumnError::bad_request_msg(format!(
            "workflow '{workflow_name}' is a registered DAG; use POST /dags/{workflow_name}/trigger"
        ))
        .into_response();
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

    // Resolve per-key concurrency policy from WorkflowInfo (issue #247).
    let (concurrency_key, concurrency_limit) = runtime
        .registry
        .workflows
        .get(&workflow_name)
        .and_then(|info| info.concurrency.as_ref())
        .map_or((None, None), |policy| {
            let key = autumn_harvest::concurrency::resolve_concurrency_key(policy.key_expr, &input);
            (key, Some(policy.limit))
        });

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
            max_execution_timeout_ceiling: api_state
                .max_workflow_execution_timeout()
                .map(|d| chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX)),
            concurrency_key,
            concurrency_limit,
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

// ── SignalWithStart (issue #244) ──────────────────────────────────────────────

#[allow(clippy::too_many_lines)]
async fn signal_with_start_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(workflow_name): Path<String>,
    maybe_session: Option<Extension<Session>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<SignalWithStartRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let route = "POST /workflows/{workflow_name}/signal-with-start";

    if matches!(
        request.id_reuse_policy.as_deref(),
        Some("terminate_if_running")
    ) && !has_harvest_admin_access(&api_state, maybe_session.map(|Extension(session)| session))
        .await
    {
        let (actor, source, request_id) = audit_context(&headers, &api_state);
        if let Ok(pool) = api_state.storage_pool()
            && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
        {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_SIGNAL_WITH_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(workflow_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: request.idempotency_key.as_deref(),
                status: STATUS_FAILED,
                error_summary: Some("unauthorized: terminate_if_running requires admin access"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return AutumnError::unauthorized_msg("authentication required").into_response();
    }

    let runtime = match api_state.runtime() {
        Ok(r) => r,
        Err(e) => return map_error(e).into_response(),
    };

    let (actor, source, request_id) = audit_context(&headers, &api_state);

    if !runtime.registry.workflows.contains_key(&workflow_name) {
        if let Ok(pool) = api_state.storage_pool()
            && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
        {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_SIGNAL_WITH_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(workflow_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: request.idempotency_key.as_deref(),
                status: STATUS_FAILED,
                error_summary: Some("workflow not registered"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return AutumnError::not_found_msg(format!("workflow '{workflow_name}'")).into_response();
    }
    if runtime.is_registered_dag(&workflow_name) {
        if let Ok(pool) = api_state.storage_pool()
            && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
        {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_SIGNAL_WITH_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(workflow_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: request.idempotency_key.as_deref(),
                status: STATUS_FAILED,
                error_summary: Some(
                    "registered DAG cannot receive signal-with-start via workflow route",
                ),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return AutumnError::bad_request_msg(format!(
            "workflow '{workflow_name}' is a registered DAG; signal-with-start applies to plain workflows"
        ))
        .into_response();
    }

    let reuse_policy = match parse_reuse_policy(request.id_reuse_policy.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            if let Ok(pool) = api_state.storage_pool()
                && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_WORKFLOW_SIGNAL_WITH_START,
                    target_type: TARGET_WORKFLOW,
                    target_id: Some(workflow_name.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: request.idempotency_key.as_deref(),
                    status: STATUS_FAILED,
                    error_summary: Some("invalid id_reuse_policy"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return e.into_response();
        }
    };

    let workflow_id = request.workflow_id;
    let queue_name = request
        .queue
        .or_else(|| runtime.queues.as_slice().first().cloned())
        .unwrap_or_else(|| "default".to_string());
    let start_input = request.start_input.unwrap_or(Value::Null);
    let signal_payload = request.signal_payload.unwrap_or(Value::Null);

    // In multi-shard deployments with read-only shards the rendezvous hash can
    // map a (workflow_name, workflow_id) to a different writable shard than the
    // one that already holds the live execution. Search every readable shard
    // for an existing non-terminal execution first; only fall back to a fresh
    // shard when none is found. This ensures signal-with-start always attaches
    // to the correct run rather than accidentally starting a second one.
    //
    // Two correctness rules apply here:
    //
    //   1. Fail closed on shard scan errors. A transient lookup failure on the
    //      shard that owns the live run must not silently fall through to
    //      `pick_for_new_workflow`, otherwise a narrowed-writable-shard
    //      deployment can race into a duplicate start on a different shard.
    //
    //   2. Reuse the existing execution UUID *only* when we expect to attach
    //      to a live run. For terminal priors, or for `TerminateIfRunning`
    //      against a RUNNING/SUSPENDED prior, the core start path takes the
    //      `replace_execution` branch which seals the existing row and inserts
    //      a new one keyed by `request.exec_id`. Passing back the existing
    //      UUID would hit the primary-key constraint and the request would
    //      fail with a database error instead of replacing as documented.
    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return map_error(e).into_response(),
    };
    let mut found_shard: Option<(ShardId, PoolConn, ExecutionId)> = None;
    for (candidate_shard, shard_pool) in pool.iter_shards() {
        let mut shard_conn = match acquire_conn(shard_pool).await {
            Ok(c) => c,
            Err(e) => return e.into_response(),
        };
        let hit = match harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(&workflow_name))
            .filter(harvest_workflow_executions::workflow_id.eq(&workflow_id))
            .filter(harvest_workflow_executions::state.ne_all(["CONTINUED_AS_NEW", "TERMINATED"]))
            .select((
                harvest_workflow_executions::id,
                harvest_workflow_executions::state,
            ))
            .first::<(uuid::Uuid, String)>(&mut shard_conn)
            .await
            .optional()
        {
            Ok(hit) => hit,
            Err(e) => {
                return AutumnError::service_unavailable_msg(format!(
                    "shard {} lookup failed: {e}",
                    candidate_shard.as_i32()
                ))
                .into_response();
            }
        };
        if let Some((existing_uuid, existing_state)) = hit {
            // Attach (reuse UUID) only when the prior is live AND the policy
            // expects to attach. Every other path goes through replace_execution
            // and needs a fresh exec_id keyed for the same shard.
            let will_attach = matches!(existing_state.as_str(), "RUNNING" | "SUSPENDED")
                && matches!(
                    reuse_policy,
                    WorkflowIdReusePolicy::AllowDuplicate
                        | WorkflowIdReusePolicy::AllowDuplicateFailedOnly
                );
            let exec_id = if will_attach {
                ExecutionId::from_uuid(existing_uuid)
            } else {
                ExecutionId::new_for_shard(candidate_shard)
            };
            found_shard = Some((candidate_shard, shard_conn, exec_id));
            break;
        }
    }

    let (shard, mut conn, exec_id) = if let Some(tuple) = found_shard {
        tuple
    } else {
        let shard = runtime
            .router
            .pick_for_new_workflow(&workflow_name, &workflow_id);
        let conn = match db_conn_for_shard(&api_state, shard).await {
            Ok(c) => c,
            Err(e) => return e.into_response(),
        };
        (shard, conn, ExecutionId::new_for_shard(shard))
    };

    let trace_ctx = tracing::info_span!(
        "harvest.workflow.schedule",
        "otel.kind" = "producer",
        { ATTR_WORKFLOW_ID } = %workflow_name,
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_SHARD_ID } = i64::from(shard.as_i32()),
        { ATTR_QUEUE } = %queue_name,
    )
    .in_scope(|| runtime.registry.telemetry().capture_trace_context());

    let (concurrency_key, concurrency_limit) = runtime
        .registry
        .workflows
        .get(&workflow_name)
        .and_then(|info| info.concurrency.as_ref())
        .map_or((None, None), |policy| {
            let key =
                autumn_harvest::concurrency::resolve_concurrency_key(policy.key_expr, &start_input);
            (key, Some(policy.limit))
        });

    let result = signal_with_start_workflow_execution(
        &mut conn,
        SignalWithStartParams {
            workflow_name: &workflow_name,
            workflow_id: &workflow_id,
            exec_id,
            input: start_input,
            parent_id: None,
            queue_name: &queue_name,
            execution_timeout: request
                .execution_timeout_secs
                .map(chrono::Duration::seconds),
            memo: request.memo,
            search_attrs: request.search_attrs,
            reuse_policy,
            trace_context: trace_ctx,
            max_execution_timeout_ceiling: api_state
                .max_workflow_execution_timeout()
                .map(|d| chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX)),
            concurrency_key,
            concurrency_limit,
            signal_name: &request.signal_name,
            signal_payload,
            idempotency_key: request.idempotency_key.clone(),
        },
    )
    .await;

    match result {
        Err(HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state,
        }) => {
            let exec_id_str = existing_exec_id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_SIGNAL_WITH_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(exec_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: request.idempotency_key.as_deref(),
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
                operation: OP_WORKFLOW_SIGNAL_WITH_START,
                target_type: TARGET_WORKFLOW,
                target_id: None,
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: request.idempotency_key.as_deref(),
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: Some(shard.as_i32()),
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            map_error(e).into_response()
        }
        Ok(outcome) => {
            let exec_id_str = outcome.exec_id.to_string();
            let status_code = if outcome.started_fresh {
                axum::http::StatusCode::CREATED
            } else {
                axum::http::StatusCode::OK
            };
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_SIGNAL_WITH_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(exec_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: request.idempotency_key.as_deref(),
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: Some(shard.as_i32()),
                source: &source,
            };
            if let Err(audit_err) = audit::insert_audit(&mut conn, &ar).await {
                tracing::error!(
                    error = %audit_err,
                    "audit insert failed for workflow.signal_with_start"
                );
                return AutumnError::service_unavailable_msg(format!(
                    "audit insert failed: {audit_err}"
                ))
                .into_response();
            }
            (
                status_code,
                Json(SignalWithStartResponse::from_outcome(outcome)),
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
    let runtime = api_state.runtime().ok();
    let registry = runtime.as_ref().map(|r| r.registry().as_ref());

    match reset_workflow_execution(&mut conn, exec_id, request, registry).await {
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

/// Waker that records whether `wake()` was called synchronously during a poll.
///
/// Used by [`hydrate_ctx_for_query`] to distinguish genuine command-based
/// suspension (waker never fired) from ordinary async yields like
/// `tokio::task::yield_now()` (waker fires immediately, signalling the caller
/// to re-poll).
struct WokenFlag(std::sync::atomic::AtomicBool);

impl futures::task::ArcWake for WokenFlag {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        std::sync::atomic::AtomicBool::store(
            &arc_self.0,
            true,
            std::sync::atomic::Ordering::Release,
        );
    }
}

/// Hydrate a workflow context by replaying its history into the workflow
/// handler long enough for it to register its query handlers.
async fn hydrate_ctx_for_query(
    api_state: &HarvestApiState,
    exec_id: ExecutionId,
) -> Result<WorkflowContext, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let mut conn = db_conn_for_execution(api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;

    // Queries are only meaningful on running workflows.
    if is_terminal_state(&execution.state) {
        return Err(map_error(HarvestError::WorkflowNotRunning(exec_id)));
    }

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

    // Drop the DB connection before driving user code — prevents holding a
    // pool slot during replay, which would starve other management and worker
    // DB operations for the entire duration of the workflow replay.
    drop(conn);

    let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
        exec_id,
        history.events,
        runtime.registry.shared_state(),
        runtime.registry.history_policy(),
    );

    // Seed declarative query handlers (registered via `.queries(queries![...])`)
    // before replaying, so execute_query_with_args can find them.
    let wf_name = execution.workflow_name.as_str();
    for h in runtime
        .registry
        .query_handlers
        .iter()
        .filter(|h| h.workflow == wf_name)
    {
        ctx.register_declarative_query_handler(h);
    }

    // Drive the workflow future until it genuinely suspends on a workflow
    // command (activity, signal wait, timer). Recorded events resolve via
    // pre-sent oneshot channels so the entire history replays synchronously.
    // Some workflows interleave ordinary async yields (e.g. yield_now) before
    // registering query handlers; the WokenFlag waker detects those and keeps
    // us polling. A wall-clock deadline (query_timeout) prevents spinning on a
    // misbehaving workflow.
    let deadline = std::time::Instant::now() + api_state.query_timeout();
    let flag = Arc::new(WokenFlag(std::sync::atomic::AtomicBool::new(false)));
    {
        let waker = futures::task::waker_ref(&flag);
        let mut poll_cx = std::task::Context::from_waker(&waker);
        let handler_fut = (workflow.handler)(&ctx, execution.input.clone());
        tokio::pin!(handler_fut);
        loop {
            std::sync::atomic::AtomicBool::store(
                &flag.0,
                false,
                std::sync::atomic::Ordering::Release,
            );
            match handler_fut.as_mut().poll(&mut poll_cx) {
                std::task::Poll::Ready(_) => break,
                std::task::Poll::Pending => {
                    let was_woken = std::sync::atomic::AtomicBool::load(
                        &flag.0,
                        std::sync::atomic::Ordering::Acquire,
                    );
                    if !was_woken || std::time::Instant::now() >= deadline {
                        break;
                    }
                    // Waker was signalled immediately — the future yielded but
                    // wants to be re-polled (e.g. yield_now). Keep driving.
                }
            }
        }
    }

    Ok(ctx)
}

/// `GET /workflows/{id}/query/{query_name}` — query with no args (backward compat).
async fn query_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, query_name)): Path<(String, String)>,
) -> Result<Json<Value>, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let ctx = hydrate_ctx_for_query(&api_state, exec_id).await?;
    let start = Instant::now(); // measure handler invocation latency, not hydration cost

    let harvest_result = ctx.execute_query(&query_name);
    // Skip metric for not-found: query_name is user-supplied and recording it
    // creates unbounded cardinality; registered names are low-cardinality.
    if !matches!(&harvest_result, Err(HarvestError::QueryHandlerNotFound(_)))
        && let Ok(runtime) = api_state.runtime()
    {
        runtime.registry.telemetry().metrics.record_query_completed(
            &query_name,
            start.elapsed().as_secs_f64(),
            harvest_result.is_ok(),
        );
    }
    harvest_result.map_err(map_error).map(Json)
}

/// Request body for `POST /workflows/{id}/query/{query_name}`.
#[derive(Debug, Deserialize)]
struct QueryWorkflowRequest {
    #[serde(default)]
    args: Value,
}

/// Response body for `POST /workflows/{id}/query/{query_name}`.
#[derive(Debug, Serialize)]
struct QueryWorkflowResponse {
    result: Value,
}

/// `POST /workflows/{id}/query/{query_name}` — query with typed args (issue #234).
///
/// The request body is optional. Clients may omit the body entirely or send
/// `Content-Type: application/json` with an empty body; both default `args`
/// to `null`. A non-empty body must be `{"args": <value>}`.
async fn query_workflow_post(
    Extension(api_state): Extension<HarvestApiState>,
    Path((id, query_name)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<QueryWorkflowResponse>, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let ctx = hydrate_ctx_for_query(&api_state, exec_id).await?;
    let start = Instant::now(); // measure handler invocation latency, not hydration cost
    let args: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice::<QueryWorkflowRequest>(&body)
            .map(|r| r.args)
            .map_err(|e| AutumnError::bad_request_msg(format!("invalid JSON body: {e}")))?
    };

    let harvest_result = ctx.execute_query_with_args(&query_name, args);
    // Skip metric for not-found: user-supplied names create unbounded cardinality.
    if !matches!(&harvest_result, Err(HarvestError::QueryHandlerNotFound(_)))
        && let Ok(runtime) = api_state.runtime()
    {
        runtime.registry.telemetry().metrics.record_query_completed(
            &query_name,
            start.elapsed().as_secs_f64(),
            harvest_result.is_ok(),
        );
    }
    harvest_result
        .map_err(map_error)
        .map(|result| Json(QueryWorkflowResponse { result }))
}

/// `GET /workflows/{id}/queries` — list registered query handler names (issue #234).
///
/// Returns the names the workflow has registered via `register_query` /
/// `register_query_handler`, which the Vantage UI uses to populate the
/// *"Run query"* control.
async fn list_workflow_queries(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<String>>, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let ctx = hydrate_ctx_for_query(&api_state, exec_id).await?;
    let mut names = ctx.list_query_names();
    names.sort(); // deterministic order for UI
    Ok(Json(names))
}

/// Response body for `GET /workflows/types/{workflow_name}/handlers` (issue #346).
#[derive(Serialize)]
struct WorkflowTypeHandlers {
    workflow: String,
    queries: Vec<HandlerSummary>,
    updates: Vec<UpdateHandlerSummary>,
}

#[derive(Serialize)]
struct HandlerSummary {
    name: &'static str,
    input_type_hint: &'static str,
    output_type_hint: &'static str,
}

#[derive(Serialize)]
struct UpdateHandlerSummary {
    name: &'static str,
    input_type_hint: &'static str,
    output_type_hint: &'static str,
    has_validator: bool,
}

async fn list_workflow_type_handlers(
    Extension(api_state): Extension<HarvestApiState>,
    Path(workflow_name): Path<String>,
) -> Result<Json<WorkflowTypeHandlers>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;

    let queries = runtime
        .registry
        .query_handlers
        .iter()
        .filter(|h| h.workflow == workflow_name)
        .map(|h| HandlerSummary {
            name: h.name,
            input_type_hint: h.input_type_hint,
            output_type_hint: h.output_type_hint,
        })
        .collect();

    let updates = runtime
        .registry
        .update_handlers
        .iter()
        .filter(|h| h.workflow == workflow_name)
        .map(|h| UpdateHandlerSummary {
            name: h.name,
            input_type_hint: h.input_type_hint,
            output_type_hint: h.output_type_hint,
            has_validator: h.has_validator,
        })
        .collect();

    Ok(Json(WorkflowTypeHandlers {
        workflow: workflow_name,
        queries,
        updates,
    }))
}

fn schedule_expr_for_summary(schedule: &Schedule) -> String {
    match schedule {
        Schedule::Cron(expr) => format!("cron:{expr}"),
        Schedule::CronInTimezone { expr, tz } => format!("cron_tz:{tz}:{expr}"),
        Schedule::Interval(interval) => format!("interval:{}", interval.as_secs()),
        Schedule::Manual => "manual".to_string(),
    }
}

async fn list_dags(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<DagSummary>>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let schedules = load_schedules_from_shards(&api_state).await?;

    let mut dags = BTreeMap::new();
    for schedule in schedules {
        let Some(dag_name) = schedule.dag_name.clone() else {
            continue;
        };
        dags.insert(
            dag_name.clone(),
            DagSummary {
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
            },
        );
    }

    for (dag_name, dag) in runtime.dags.iter() {
        dags.entry(dag_name.clone()).or_insert_with(|| DagSummary {
            name: dag_name.clone(),
            schedule_expr: dag.schedule.as_ref().map(schedule_expr_for_summary),
            is_paused: false,
            next_run_at: None,
            max_active_runs: i32::try_from(dag.max_active_runs).unwrap_or(i32::MAX),
            catchup: dag.catchup,
            task_count: dag.task_count(),
        });
    }

    Ok(Json(dags.into_values().collect()))
}

async fn list_dag_runs(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
) -> Result<Json<Vec<WorkflowExecution>>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    if !runtime.is_registered_dag(&dag_name) {
        return Err(AutumnError::not_found_msg(format!(
            "DAG '{dag_name}' is not registered"
        )));
    }

    let pool = api_state.storage_pool().map_err(map_error)?;
    let shard = runtime.router.pick_for_dag(&dag_name);
    let mut conn = acquire_conn(pool.pool_for(shard)).await?;
    let runs = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(&dag_name))
        .order(harvest_workflow_executions::created_at.desc())
        .select(WorkflowExecution::as_select())
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
) -> Result<(axum::http::StatusCode, Json<StartWorkflowResponse>), AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let pool = api_state.storage_pool().map_err(map_error)?;

    if !runtime.is_registered_dag(&dag_name) {
        return Err(AutumnError::not_found_msg(format!(
            "DAG '{dag_name}' is not registered"
        )));
    }
    let dag =
        runtime.dags.get(&dag_name).cloned().ok_or_else(|| {
            AutumnError::not_found_msg(format!("DAG '{dag_name}' is not registered"))
        })?;

    let shard = runtime.router.pick_for_dag(&dag_name);
    let default_queue = dag
        .default_queue
        .as_deref()
        .unwrap_or("default")
        .to_string();
    let mut schedule_conn = acquire_conn(pool.pool_for(shard)).await?;
    ensure_dag_schedule(&mut schedule_conn, &dag)
        .await
        .map_err(map_error)?;
    drop(schedule_conn);

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /dags/{dag_name}/trigger";

    let trigger_result = trigger_unified_dag(
        pool.pool_for(shard).clone(),
        &dag_name,
        request.conf,
        shard,
        &default_queue,
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
        Ok(started) => {
            let exec_id_str = started.exec_id.to_string();
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
                tracing::error!(error = %audit_err, exec_id = %exec_id_str, "audit insert failed for dag.trigger");
                return Err(AutumnError::service_unavailable_msg(format!(
                    "audit insert failed: {audit_err}"
                )));
            }
            Ok((
                axum::http::StatusCode::CREATED,
                Json(StartWorkflowResponse {
                    execution_id: started.exec_id.to_string(),
                    workflow_name: started.workflow_name,
                    workflow_id: started.workflow_id,
                    state: started.state,
                }),
            ))
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

    let runtime = api_state.runtime().map_err(map_error)?;
    let mut conn = db_conn_for_dag(&api_state, &dag_name).await?;
    if let Some(dag) = runtime.dags.get(&dag_name) {
        ensure_dag_schedule(&mut conn, dag)
            .await
            .map_err(map_error)?;
    }

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

fn effective_fire_time(
    schedule_id: uuid::Uuid,
    next_run_at: Option<chrono::DateTime<chrono::Utc>>,
    jitter_secs: i64,
) -> Option<chrono::DateTime<chrono::Utc>> {
    let t = next_run_at?;
    if jitter_secs <= 0 {
        return None;
    }
    let jitter_window = std::time::Duration::from_secs(jitter_secs.cast_unsigned());
    let offset = compute_jitter_offset(schedule_id, t, jitter_window);
    chrono::Duration::from_std(offset).ok().map(|d| t + d)
}

async fn list_schedules(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<ScheduleEntry>>, AutumnError> {
    let schedules = load_schedules_from_shards(&api_state).await?;
    let schedule_ids: Vec<uuid::Uuid> = schedules.iter().map(|s| s.id).collect();

    // Best-effort: load the most recent backfill log row for each schedule.
    let recent_backfills = load_recent_backfills(&api_state, &schedule_ids).await;

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
            let last_backfill = recent_backfills
                .get(&s.id)
                .cloned()
                .map(BackfillSummary::from);
            let eft = effective_fire_time(s.id, s.next_run_at, s.jitter_secs);
            let buffered_count =
                autumn_harvest::scheduler::parse_buffered_runs_pub(&s.buffered_runs).len();
            ScheduleEntry {
                id: s.id,
                kind,
                name,
                schedule_expr: s.schedule_expr,
                timezone: s.timezone,
                is_paused: s.is_paused,
                paused_at: s.paused_at,
                paused_by: s.paused_by,
                pause_reason: s.pause_reason,
                next_run_at: s.next_run_at,
                last_run_at: s.last_run_at,
                max_active_runs: s.max_active_runs,
                catchup: s.catchup,
                last_backfill,
                jitter_secs: s.jitter_secs,
                effective_fire_time: eft,
                overlap_policy: s.overlap_policy.clone(),
                buffered_count,
                buffer_all_max: s.buffer_all_max,
            }
        })
        .collect();
    Ok(Json(entries))
}

async fn get_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> Result<Json<ScheduleEntry>, AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let id = parse_uuid(&id_str, "schedule id")?;
    let pool = api_state.storage_pool().map_err(map_error)?;

    let mut found: Option<HarvestSchedule> = None;
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
        if row.is_some() {
            found = row;
            break;
        }
    }

    let s = found.ok_or_else(|| AutumnError::not_found_msg(format!("schedule {id}")))?;

    let (kind, name) = if let Some(ref dag_name) = s.dag_name {
        (ScheduleKind::Dag, dag_name.clone())
    } else if let Some(ref wf_name) = s.workflow_name {
        (ScheduleKind::Workflow, wf_name.clone())
    } else {
        (ScheduleKind::Dag, String::new())
    };

    let last_backfill = load_recent_backfills(&api_state, std::slice::from_ref(&s.id))
        .await
        .remove(&s.id)
        .map(BackfillSummary::from);

    let buffered_count = autumn_harvest::scheduler::parse_buffered_runs_pub(&s.buffered_runs).len();
    Ok(Json(ScheduleEntry {
        effective_fire_time: effective_fire_time(s.id, s.next_run_at, s.jitter_secs),
        jitter_secs: s.jitter_secs,
        id: s.id,
        kind,
        name,
        schedule_expr: s.schedule_expr,
        timezone: s.timezone,
        is_paused: s.is_paused,
        paused_at: s.paused_at,
        paused_by: s.paused_by,
        pause_reason: s.pause_reason,
        next_run_at: s.next_run_at,
        last_run_at: s.last_run_at,
        max_active_runs: s.max_active_runs,
        catchup: s.catchup,
        last_backfill,
        overlap_policy: s.overlap_policy.clone(),
        buffered_count,
        buffer_all_max: s.buffer_all_max,
    }))
}

/// Load the most recent backfill log row for each of the given schedule IDs.
/// Returns a map from `schedule_id` to row. Silently returns empty map on any error.
async fn load_recent_backfills(
    api_state: &HarvestApiState,
    schedule_ids: &[uuid::Uuid],
) -> std::collections::HashMap<uuid::Uuid, BackfillLogRow> {
    use autumn_harvest::schema::harvest_backfill_log::dsl;

    if schedule_ids.is_empty() {
        return std::collections::HashMap::new();
    }
    let Ok(pool) = api_state.storage_pool() else {
        return std::collections::HashMap::new();
    };
    let Ok(mut conn) = acquire_conn(pool.default_pool()).await else {
        return std::collections::HashMap::new();
    };

    // Load all recent backfill rows for these schedules in one query, ordered by
    // started_at DESC, then de-duplicate keeping only the most recent per schedule.
    let rows: Vec<BackfillLogRow> = dsl::harvest_backfill_log
        .filter(dsl::schedule_id.eq_any(schedule_ids))
        .order((dsl::schedule_id, dsl::started_at.desc()))
        .select(BackfillLogRow::as_select())
        .load(&mut conn)
        .await
        .unwrap_or_default();

    let mut map: std::collections::HashMap<uuid::Uuid, BackfillLogRow> =
        std::collections::HashMap::new();
    for row in rows {
        map.entry(row.schedule_id).or_insert(row);
    }
    map
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

async fn reject_workflow_schedule_for_registered_dag(
    api_state: &HarvestApiState,
    runtime: &HarvestApiRuntime,
    actor: &str,
    source: &str,
    request_id: Option<&str>,
    workflow_name: &str,
) -> Result<(), AutumnError> {
    if !runtime.is_registered_dag(workflow_name) {
        return Ok(());
    }

    schedule_create_audit_failed(
        api_state,
        actor,
        source,
        request_id,
        workflow_name,
        "registered DAG cannot be scheduled via workflow schedule API",
    )
    .await;
    Err(AutumnError::bad_request_msg(format!(
        "workflow '{workflow_name}' is a registered DAG; manage its schedule through the DAG registration"
    )))
}

async fn reject_unknown_workflow_schedule_target(
    api_state: &HarvestApiState,
    runtime: &HarvestApiRuntime,
    actor: &str,
    source: &str,
    request_id: Option<&str>,
    workflow_name: &str,
) -> Result<(), AutumnError> {
    if runtime.registry.workflows.contains_key(workflow_name) {
        return Ok(());
    }

    let registered: Vec<&str> = runtime
        .registry
        .workflows
        .keys()
        .map(String::as_str)
        .collect();
    schedule_create_audit_failed(
        api_state,
        actor,
        source,
        request_id,
        workflow_name,
        "workflow not registered",
    )
    .await;
    Err(AutumnError::not_found_msg(format!(
        "workflow '{workflow_name}' is not registered; registered: {registered:?}"
    )))
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
    let buffered_count =
        autumn_harvest::scheduler::parse_buffered_runs_pub(&row.buffered_runs).len();
    Ok(ScheduleEntry {
        effective_fire_time: effective_fire_time(row.id, row.next_run_at, row.jitter_secs),
        jitter_secs: row.jitter_secs,
        id: row.id,
        kind: ScheduleKind::Workflow,
        name: ws.workflow_name.clone(),
        schedule_expr: row.schedule_expr,
        timezone: row.timezone,
        is_paused: row.is_paused,
        paused_at: row.paused_at,
        paused_by: row.paused_by,
        pause_reason: row.pause_reason,
        next_run_at: row.next_run_at,
        last_run_at: row.last_run_at,
        max_active_runs: row.max_active_runs,
        catchup: row.catchup,
        last_backfill: None, // newly created; no backfill history yet
        overlap_policy: row.overlap_policy.clone(),
        buffered_count,
        buffer_all_max: row.buffer_all_max,
    })
}

#[allow(clippy::too_many_lines)]
async fn create_workflow_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CreateWorkflowScheduleRequest>,
) -> Result<(axum::http::StatusCode, Json<ScheduleEntry>), AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /admin/schedules/workflow";

    reject_unknown_workflow_schedule_target(
        &api_state,
        &runtime,
        &actor,
        &source,
        request_id.as_deref(),
        &request.workflow_name,
    )
    .await?;

    reject_workflow_schedule_for_registered_dag(
        &api_state,
        &runtime,
        &actor,
        &source,
        request_id.as_deref(),
        &request.workflow_name,
    )
    .await?;

    let schedule = match parse_schedule_expr_with_tz(&request.schedule_expr, &request.timezone) {
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

    // Reject unknown overlap_policy strings with 400 before storing.
    // `from_db` is lenient for backward compat; user input is validated strictly.
    let overlap_policy = match autumn_harvest::OverlapPolicy::from_user_input(
        &request.overlap_policy,
    ) {
        Ok(p) => p,
        Err(v) => {
            let err_summary = format!(
                "invalid overlap_policy '{v}'; valid values: skip, buffer_one, buffer_all, cancel_other, terminate_other"
            );
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
    let ws = WorkflowSchedule {
        workflow_name: request.workflow_name.clone(),
        dag_name: None,
        schedule,
        input: request.input.clone(),
        catchup: request.catchup,
        max_active_runs: request.max_active_runs,
        paused: request.paused,
        queue_name: request.queue_name.clone(),
        jitter: std::time::Duration::from_secs(request.jitter_secs),
        overlap_policy,
        buffer_all_max: request.buffer_all_max,
        execution_timeout: None,
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
    body: Option<Json<PauseResumeRequest>>,
) -> Result<Json<BasicAck>, AutumnError> {
    let reason = body.and_then(|Json(r)| r.reason);
    set_schedule_paused(&api_state, &id, true, reason.as_deref(), &headers).await
}

async fn resume_schedule(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
    body: Option<Json<PauseResumeRequest>>,
) -> Result<Json<BasicAck>, AutumnError> {
    let reason = body.and_then(|Json(r)| r.reason);
    set_schedule_paused(&api_state, &id, false, reason.as_deref(), &headers).await
}

#[allow(clippy::too_many_lines)]
async fn set_schedule_paused(
    api_state: &HarvestApiState,
    id_str: &str,
    paused: bool,
    reason: Option<&str>,
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
    let now = chrono::Utc::now();

    let mut found_count = 0usize;
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;

        // Atomic conditional UPDATE: only applies when `is_paused` differs from
        // the requested state.  The `filter(dsl::is_paused.ne(paused))` predicate
        // makes the idempotency check part of the UPDATE itself, eliminating the
        // SELECT-then-UPDATE race condition that would otherwise let two
        // concurrent requests both overwrite paused_at/paused_by.
        let rows_updated: usize = if paused {
            // Pause: set metadata only on the transition false → true.
            diesel::update(
                dsl::harvest_schedules
                    .find(id)
                    .filter(dsl::is_paused.ne(true)),
            )
            .set((
                dsl::is_paused.eq(true),
                dsl::paused_at.eq(Some(now)),
                dsl::paused_by.eq(Some(actor.as_str())),
                dsl::pause_reason.eq(reason),
                dsl::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .await
            .map_err(database_error)
            .map_err(map_error)?
        } else {
            // Resume: clear metadata only on the transition true → false.
            diesel::update(
                dsl::harvest_schedules
                    .find(id)
                    .filter(dsl::is_paused.ne(false)),
            )
            .set((
                dsl::is_paused.eq(false),
                dsl::paused_at.eq(None::<chrono::DateTime<chrono::Utc>>),
                dsl::paused_by.eq(None::<&str>),
                dsl::pause_reason.eq(None::<&str>),
                dsl::updated_at.eq(now),
            ))
            .execute(&mut conn)
            .await
            .map_err(database_error)
            .map_err(map_error)?
        };

        if rows_updated == 0 {
            // Either not on this shard, or already in the requested state.
            // Distinguish the two with a cheap existence check.
            let exists: bool = diesel::select(diesel::dsl::exists(dsl::harvest_schedules.find(id)))
                .get_result(&mut conn)
                .await
                .map_err(database_error)
                .map_err(map_error)?;

            if !exists {
                continue; // not on this shard — try the next one
            }
            // Already in the requested state: idempotent no-op.
        }
        found_count += 1;

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

    if found_count == 0 {
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

// ── Schedule backfill (issue #177) ────────────────────────────────────────────

/// Request body for `POST /admin/schedules/{id}/backfill`.
#[derive(Debug, Deserialize)]
struct ScheduleBackfillRequest {
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    include_paused: bool,
    max_count: Option<usize>,
}

/// A shard that could not be reached or dispatched during a backfill.
#[derive(Debug, Serialize)]
struct BackfillShardFailure {
    shard_id: i32,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BackfillPriorPrecheck {
    NoPriorRun,
    PriorRunExists,
    PrecheckFailed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BackfillRunningCountCheck {
    Count(i64),
    CountFailed(String),
}

fn classify_backfill_prior_count(result: diesel::QueryResult<i64>) -> BackfillPriorPrecheck {
    match result {
        Ok(count) if count > 0 => BackfillPriorPrecheck::PriorRunExists,
        Ok(_) => BackfillPriorPrecheck::NoPriorRun,
        Err(error) => BackfillPriorPrecheck::PrecheckFailed(error.to_string()),
    }
}

fn classify_backfill_running_count(result: diesel::QueryResult<i64>) -> BackfillRunningCountCheck {
    match result {
        Ok(count) => BackfillRunningCountCheck::Count(count),
        Err(error) => BackfillRunningCountCheck::CountFailed(error.to_string()),
    }
}

/// Response for `POST /admin/schedules/{id}/backfill`.
#[derive(Debug, Serialize)]
struct ScheduleBackfillResponse {
    /// `"dry_run"`, `"complete"`, or `"partial"`.
    status: String,
    schedule_id: uuid::Uuid,
    kind: ScheduleKind,
    name: String,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
    planned_timestamps: Vec<chrono::DateTime<chrono::Utc>>,
    total: usize,
    dispatched: usize,
    skipped: usize,
    failed: usize,
    /// Machine-readable breakdown of why timestamps were skipped.
    /// Keys: `"already_exists"`, `"max_active_runs"`.
    skipped_reasons: std::collections::HashMap<String, usize>,
    partial_shard_failures: Vec<BackfillShardFailure>,
    /// Set when a DAG schedule is paused and `include_paused=true`: inserted
    /// QUEUED runs will not execute until the schedule is resumed because
    /// `activate_queued_runs` skips paused schedules.
    #[serde(skip_serializing_if = "Option::is_none")]
    paused_schedule_warning: Option<String>,
}

#[allow(clippy::too_many_lines)]
async fn schedule_backfill(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
    Json(request): Json<ScheduleBackfillRequest>,
) -> Result<Json<ScheduleBackfillResponse>, AutumnError> {
    let (actor, source, req_id) = audit_context(&headers, &api_state);
    let route = "POST /admin/schedules/{id}/backfill";
    let started_at = chrono::Utc::now();

    let schedule_id = parse_uuid(&id, "schedule id")?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let runtime = api_state.runtime().map_err(map_error)?;

    if request.to < request.from {
        return Err(AutumnError::bad_request_msg(
            "backfill 'to' must be at or after 'from'",
        ));
    }

    // Load the schedule row (fan out across shards; schedule rows are not shard-assigned).
    let schedule = load_schedule_by_id(&api_state, schedule_id).await?;

    // Respect paused state unless the caller explicitly opts in.
    if schedule.is_paused && !request.include_paused {
        return Err(AutumnError::bad_request_msg(format!(
            "schedule {schedule_id} is paused; pass include_paused=true to backfill a paused schedule"
        )));
    }

    let parsed_schedule = schedule
        .schedule_expr
        .as_deref()
        .and_then(parse_schedule_from_expr_pub);

    let max_count = request.max_count.unwrap_or(DEFAULT_BACKFILL_MAX_COUNT);

    let timestamps = plan_backfill_timestamps(
        parsed_schedule.as_ref(),
        request.from,
        request.to,
        max_count,
    )
    .map_err(|e| match e {
        BackfillPlanError::LimitExceeded { limit } => AutumnError::bad_request_msg(format!(
            "backfill window contains more than {limit} timestamps; lower the window or pass a higher max_count"
        )),
    })?;

    let total = timestamps.len();
    let (kind, name) = if let Some(ref dag_name) = schedule.dag_name {
        (ScheduleKind::Dag, dag_name.clone())
    } else if let Some(ref wf_name) = schedule.workflow_name {
        (ScheduleKind::Workflow, wf_name.clone())
    } else {
        return Err(AutumnError::service_unavailable_msg(
            "schedule row has neither dag_name nor workflow_name",
        ));
    };

    if kind == ScheduleKind::Dag && !runtime.is_registered_dag(&name) {
        return Err(AutumnError::not_found_msg(format!(
            "DAG '{name}' is not registered"
        )));
    }

    let max_active = i64::from(schedule.max_active_runs);

    if schedule.is_paused && request.include_paused && kind == ScheduleKind::Dag && !request.dry_run
    {
        return Err(AutumnError::bad_request_msg(format!(
            "paused DAG schedule {schedule_id} cannot be backfilled in non-dry-run mode; resume the schedule before dispatching backfill work"
        )));
    }

    // Dry-run: query current running count and project what would happen.
    if request.dry_run {
        let running = query_running_count_best_effort(&pool, &kind, &name).await;
        let already_exists = count_existing_in_window(&pool, &kind, &name, &timestamps).await;
        let remaining = total.saturating_sub(already_exists);
        let available_slots = usize::try_from((max_active - running).max(0)).unwrap_or(0);
        let would_dispatch = remaining.min(available_slots);
        let would_skip_max = remaining - would_dispatch;

        let mut skipped_reasons = std::collections::HashMap::new();
        if already_exists > 0 {
            skipped_reasons.insert("already_exists".to_string(), already_exists);
        }
        if would_skip_max > 0 {
            skipped_reasons.insert("max_active_runs".to_string(), would_skip_max);
        }

        let paused_schedule_warning = (schedule.is_paused
            && request.include_paused
            && kind == ScheduleKind::Dag)
            .then(|| {
                "Schedule is paused; backfilled DAG runs are QUEUED but will not \
                 execute until the schedule is resumed (`harvest schedule resume`)."
                    .to_string()
            });

        let would_skip = already_exists + would_skip_max;
        write_backfill_log(
            &pool,
            schedule_id,
            &actor,
            &source,
            request.from,
            request.to,
            true,
            total,
            would_dispatch,
            would_skip,
            0,
            "dry_run",
            None,
            started_at,
        )
        .await;
        write_audit(
            &pool,
            &actor,
            &source,
            req_id.as_deref(),
            route,
            &id,
            STATUS_SUCCEEDED,
            None,
        )
        .await;
        return Ok(Json(ScheduleBackfillResponse {
            status: "dry_run".to_string(),
            schedule_id,
            kind,
            name,
            from: request.from,
            to: request.to,
            planned_timestamps: timestamps,
            total,
            dispatched: would_dispatch,
            skipped: would_skip,
            failed: 0,
            skipped_reasons,
            partial_shard_failures: vec![],
            paused_schedule_warning,
        }));
    }

    // Non-dry-run: dispatch each timestamp idempotently, respecting max_active_runs.
    let dag_default_queue = if kind == ScheduleKind::Dag {
        runtime
            .dags()
            .get(&name)
            .and_then(|dag| dag.default_queue.as_deref())
    } else {
        None
    };
    let dispatch_queue = schedule
        .queue_name
        .as_deref()
        .or(dag_default_queue)
        .unwrap_or("default");
    let mut dispatched = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    let mut shard_failures: Vec<BackfillShardFailure> = Vec::new();
    let mut skipped_reasons: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    // Warn when the operator uses include_paused=true for a paused DAG schedule.
    // `activate_queued_runs` skips paused schedules, so the inserted QUEUED runs
    // will not execute until the schedule is resumed.
    let paused_schedule_warning =
        (schedule.is_paused && request.include_paused && kind == ScheduleKind::Dag).then(|| {
            "Schedule is paused; backfilled DAG runs are QUEUED but will not \
             execute until the schedule is resumed (`harvest schedule resume`)."
                .to_string()
        });

    // Count running executions once before the loop; track dispatched_this_call separately
    // so we don't re-query on every timestamp. This value gates max_active_runs,
    // so non-dry-run dispatch must not treat count failures as zero.
    let running_at_start = match query_running_count(&pool, &kind, &name).await {
        Ok(count) => count,
        Err(count_failures) => {
            let status = "partial";
            let error_summary = Some("one or more shard failures");
            write_backfill_log(
                &pool,
                schedule_id,
                &actor,
                &source,
                request.from,
                request.to,
                false,
                total,
                0,
                0,
                total,
                status,
                error_summary,
                started_at,
            )
            .await;
            let id_str = schedule_id.to_string();
            write_audit(
                &pool,
                &actor,
                &source,
                req_id.as_deref(),
                route,
                &id_str,
                STATUS_FAILED,
                error_summary,
            )
            .await;

            return Ok(Json(ScheduleBackfillResponse {
                status: status.to_string(),
                schedule_id,
                kind,
                name,
                from: request.from,
                to: request.to,
                planned_timestamps: timestamps,
                total,
                dispatched: 0,
                skipped: 0,
                failed: total,
                skipped_reasons,
                partial_shard_failures: count_failures,
                paused_schedule_warning,
            }));
        }
    };
    let mut dispatched_this_call: i64 = 0;

    match kind {
        ScheduleKind::Workflow => {
            let wf_name = name.clone();
            // Workflow schedules are always dispatched through the scheduler's own
            // connection, which uses `ExecutionId::new()` (ShardId::UNENCODED → default
            // shard). The backfill must write to the same shard so that the partial
            // unique index on (workflow_name, workflow_id) prevents the scheduler from
            // creating a second run for the same timestamp after the backfill window.
            let wf_shard_pool = pool.default_pool();
            for scheduled_for in &timestamps {
                // Respect max_active_runs: skip if we've already saturated the limit.
                if running_at_start + dispatched_this_call >= max_active {
                    skipped += 1;
                    *skipped_reasons
                        .entry("max_active_runs".to_string())
                        .or_insert(0) += 1;
                    continue;
                }

                let workflow_id = scheduled_workflow_id_pub(&wf_name, *scheduled_for);
                // Match the scheduler: ExecutionId::new() encodes ShardId::UNENCODED so
                // the execution lands on the default shard, same as tick_one_workflow_schedule.
                let exec_id = ExecutionId::new();
                let input = schedule
                    .workflow_input
                    .clone()
                    .unwrap_or(serde_json::Value::Null);

                let Ok(mut conn) = acquire_conn(wf_shard_pool).await else {
                    shard_failures.push(BackfillShardFailure {
                        shard_id: 0,
                        reason: "failed to acquire connection".to_string(),
                    });
                    failed += 1;
                    continue;
                };

                // Pre-check across ALL states including CONTINUED_AS_NEW / TERMINATED.
                // `start_or_load_workflow_execution` uses a partial unique index that
                // excludes sealed rows, so a sealed prior execution does not conflict
                // and would allow a duplicate to be created. Backfill must not reuse
                // sealed workflow IDs because the timestamp was already dispatched.
                let prior_check = harvest_workflow_executions::table
                    .filter(harvest_workflow_executions::workflow_name.eq(&wf_name))
                    .filter(harvest_workflow_executions::workflow_id.eq(&workflow_id))
                    .count()
                    .get_result::<i64>(&mut conn)
                    .await;
                match classify_backfill_prior_count(prior_check) {
                    BackfillPriorPrecheck::PriorRunExists => {
                        skipped += 1;
                        *skipped_reasons
                            .entry("already_exists".to_string())
                            .or_insert(0) += 1;
                        continue;
                    }
                    BackfillPriorPrecheck::NoPriorRun => {}
                    BackfillPriorPrecheck::PrecheckFailed(reason) => {
                        shard_failures.push(BackfillShardFailure {
                            shard_id: 0,
                            reason: format!(
                                "failed to check prior workflow execution for {workflow_id}: {reason}"
                            ),
                        });
                        failed += 1;
                        continue;
                    }
                }

                let result = start_or_load_workflow_execution(
                    &mut conn,
                    StartWorkflowParams {
                        workflow_name: &wf_name,
                        workflow_id: &workflow_id,
                        exec_id,
                        input: input.clone(),
                        parent_id: None,
                        queue_name: dispatch_queue,
                        execution_timeout: None,
                        memo: None,
                        search_attrs: None,
                        reuse_policy: WorkflowIdReusePolicy::RejectDuplicate,
                        trace_context: None,
                        max_execution_timeout_ceiling: None,
                        concurrency_key: None,
                        concurrency_limit: None,
                    },
                )
                .await;
                match result {
                    Ok(started) if started.created => {
                        dispatched += 1;
                        dispatched_this_call += 1;
                    }
                    Ok(_) | Err(HarvestError::AlreadyExists { .. }) => {
                        skipped += 1;
                        *skipped_reasons
                            .entry("already_exists".to_string())
                            .or_insert(0) += 1;
                    }
                    Err(e) => {
                        shard_failures.push(BackfillShardFailure {
                            shard_id: 0,
                            reason: e.to_string(),
                        });
                        failed += 1;
                    }
                }
            }
        }
        ScheduleKind::Dag => {
            let dag_name = name.clone();
            let shard_id = runtime.router().pick_for_dag(&dag_name);
            let shard_pool = pool.pool_for(shard_id);

            for scheduled_for in &timestamps {
                // Respect max_active_runs for DAGs (now counted via workflow executions).
                if running_at_start + dispatched_this_call >= max_active {
                    skipped += 1;
                    *skipped_reasons
                        .entry("max_active_runs".to_string())
                        .or_insert(0) += 1;
                    continue;
                }

                let Ok(mut conn) = acquire_conn(shard_pool).await else {
                    shard_failures.push(BackfillShardFailure {
                        shard_id: shard_id.as_i32(),
                        reason: "failed to acquire connection".to_string(),
                    });
                    failed += 1;
                    continue;
                };
                let workflow_id = scheduled_workflow_id_pub(&dag_name, *scheduled_for);
                let exec_id = autumn_harvest::types::ExecutionId::new_for_shard(shard_id);
                let dag_queue = schedule
                    .queue_name
                    .as_deref()
                    .or(dag_default_queue)
                    .unwrap_or("default");

                // Pre-check across ALL states including CONTINUED_AS_NEW / TERMINATED.
                // start_or_load_workflow_execution uses a partial unique index that
                // excludes sealed rows, so a sealed prior run wouldn't conflict and
                // a duplicate would be created for the same scheduled slot.
                let prior_check = harvest_workflow_executions::table
                    .filter(harvest_workflow_executions::workflow_name.eq(&dag_name))
                    .filter(harvest_workflow_executions::workflow_id.eq(&workflow_id))
                    .count()
                    .get_result::<i64>(&mut conn)
                    .await;
                match classify_backfill_prior_count(prior_check) {
                    BackfillPriorPrecheck::PriorRunExists => {
                        skipped += 1;
                        *skipped_reasons
                            .entry("already_exists".to_string())
                            .or_insert(0) += 1;
                        continue;
                    }
                    BackfillPriorPrecheck::NoPriorRun => {}
                    BackfillPriorPrecheck::PrecheckFailed(reason) => {
                        shard_failures.push(BackfillShardFailure {
                            shard_id: shard_id.as_i32(),
                            reason: format!(
                                "failed to check prior DAG execution for {workflow_id}: {reason}"
                            ),
                        });
                        failed += 1;
                        continue;
                    }
                }

                let start_result = start_or_load_workflow_execution(
                    &mut conn,
                    StartWorkflowParams {
                        workflow_name: &dag_name,
                        workflow_id: &workflow_id,
                        exec_id,
                        input: serde_json::json!({"_harvest_run_source": "backfill"}),
                        parent_id: None,
                        queue_name: dag_queue,
                        execution_timeout: None,
                        memo: None,
                        search_attrs: None,
                        reuse_policy: autumn_harvest::types::WorkflowIdReusePolicy::RejectDuplicate,
                        trace_context: None,
                        max_execution_timeout_ceiling: None,
                        concurrency_key: None,
                        concurrency_limit: None,
                    },
                )
                .await;
                match start_result {
                    Ok(started) if started.created => {
                        dispatched += 1;
                        dispatched_this_call += 1;
                    }
                    Ok(_) => {
                        // RejectDuplicate → already exists
                        skipped += 1;
                        *skipped_reasons
                            .entry("already_exists".to_string())
                            .or_insert(0) += 1;
                    }
                    Err(autumn_harvest::HarvestError::AlreadyExists { .. }) => {
                        skipped += 1;
                        *skipped_reasons
                            .entry("already_exists".to_string())
                            .or_insert(0) += 1;
                    }
                    Err(e) => {
                        shard_failures.push(BackfillShardFailure {
                            shard_id: shard_id.as_i32(),
                            reason: e.to_string(),
                        });
                        failed += 1;
                    }
                }
            }
        }
    }

    let status = if shard_failures.is_empty() && failed == 0 {
        "complete"
    } else {
        "partial"
    };
    let error_summary = if status == "partial" {
        Some("one or more shard failures")
    } else {
        None
    };

    write_backfill_log(
        &pool,
        schedule_id,
        &actor,
        &source,
        request.from,
        request.to,
        false,
        total,
        dispatched,
        skipped,
        failed,
        status,
        error_summary,
        started_at,
    )
    .await;
    let id_str = schedule_id.to_string();
    write_audit(
        &pool,
        &actor,
        &source,
        req_id.as_deref(),
        route,
        &id_str,
        if status == "complete" {
            STATUS_SUCCEEDED
        } else {
            STATUS_FAILED
        },
        error_summary,
    )
    .await;

    Ok(Json(ScheduleBackfillResponse {
        status: status.to_string(),
        schedule_id,
        kind,
        name,
        from: request.from,
        to: request.to,
        planned_timestamps: timestamps,
        total,
        dispatched,
        skipped,
        failed,
        skipped_reasons,
        partial_shard_failures: shard_failures,
        paused_schedule_warning,
    }))
}

/// Count RUNNING workflow executions or DAG runs for the named entity.
/// Returns the total count across all shards, or all shard failures that made
/// the count unsafe to use for `max_active_runs` enforcement.
async fn query_running_count(
    pool: &HarvestDbPool,
    kind: &ScheduleKind,
    name: &str,
) -> Result<i64, Vec<BackfillShardFailure>> {
    let mut total = 0i64;
    let mut failures = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            failures.push(BackfillShardFailure {
                shard_id: shard_id.as_i32(),
                reason: "failed to acquire connection while counting running backfill slots"
                    .to_string(),
            });
            continue;
        };
        // Both DAG and Workflow kinds query harvest_workflow_executions since
        // DAGs are now unified as workflows (issue #256 step 5).
        let count_result = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::workflow_name.eq(name))
            .filter(harvest_workflow_executions::state.eq("RUNNING"))
            .count()
            .get_result::<i64>(&mut conn)
            .await;
        let _ = kind; // consumed for pattern coverage above
        match classify_backfill_running_count(count_result) {
            BackfillRunningCountCheck::Count(count) => total += count,
            BackfillRunningCountCheck::CountFailed(reason) => {
                failures.push(BackfillShardFailure {
                    shard_id: shard_id.as_i32(),
                    reason: format!("failed to count running backfill slots: {reason}"),
                });
            }
        }
    }
    if failures.is_empty() {
        Ok(total)
    } else {
        Err(failures)
    }
}

/// Dry-run uses the running-count estimate for operator preview only, so it
/// stays best-effort and preserves the historical conservative response shape.
async fn query_running_count_best_effort(
    pool: &HarvestDbPool,
    kind: &ScheduleKind,
    name: &str,
) -> i64 {
    query_running_count(pool, kind, name).await.unwrap_or(0)
}

/// Count how many of the planned timestamps already have an execution or DAG run.
/// Used in dry-run mode to improve the accuracy of the would-dispatch estimate.
/// Returns 0 on any DB error (safe: slightly over-estimates would-dispatch).
async fn count_existing_in_window(
    pool: &HarvestDbPool,
    kind: &ScheduleKind,
    name: &str,
    timestamps: &[chrono::DateTime<chrono::Utc>],
) -> usize {
    if timestamps.is_empty() {
        return 0;
    }
    let mut total = 0usize;
    match kind {
        ScheduleKind::Workflow => {
            let workflow_ids: Vec<String> = timestamps
                .iter()
                .map(|ts| scheduled_workflow_id_pub(name, *ts))
                .collect();
            for (_, shard_pool) in pool.iter_shards() {
                let Ok(mut conn) = acquire_conn(shard_pool).await else {
                    continue;
                };
                let count: i64 = harvest_workflow_executions::table
                    .filter(harvest_workflow_executions::workflow_name.eq(name))
                    .filter(harvest_workflow_executions::workflow_id.eq_any(&workflow_ids))
                    .count()
                    .get_result(&mut conn)
                    .await
                    .unwrap_or(0);
                total += usize::try_from(count).unwrap_or(0);
            }
        }
        ScheduleKind::Dag => {
            let workflow_ids: Vec<String> = timestamps
                .iter()
                .map(|ts| scheduled_workflow_id_pub(name, *ts))
                .collect();
            for (_, shard_pool) in pool.iter_shards() {
                let Ok(mut conn) = acquire_conn(shard_pool).await else {
                    continue;
                };
                let count: i64 = harvest_workflow_executions::table
                    .filter(harvest_workflow_executions::workflow_name.eq(name))
                    .filter(harvest_workflow_executions::workflow_id.eq_any(&workflow_ids))
                    .count()
                    .get_result(&mut conn)
                    .await
                    .unwrap_or(0);
                total += usize::try_from(count).unwrap_or(0);
            }
        }
    }
    total
}

/// Write a backfill log row; silently swallows errors (best-effort durability).
#[allow(clippy::too_many_arguments)]
async fn write_backfill_log(
    pool: &HarvestDbPool,
    schedule_id: uuid::Uuid,
    actor: &str,
    source: &str,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
    dry_run: bool,
    total: usize,
    dispatched: usize,
    skipped: usize,
    failed: usize,
    status: &str,
    error_summary: Option<&str>,
    started_at: chrono::DateTime<chrono::Utc>,
) {
    let Ok(mut conn) = acquire_conn(pool.default_pool()).await else {
        return;
    };
    let row = NewBackfillLogRow {
        id: uuid::Uuid::new_v4(),
        schedule_id,
        actor: actor.to_string(),
        source: source.to_string(),
        from_ts: from,
        to_ts: to,
        dry_run,
        total: i32::try_from(total).unwrap_or(i32::MAX),
        dispatched: i32::try_from(dispatched).unwrap_or(i32::MAX),
        skipped: i32::try_from(skipped).unwrap_or(i32::MAX),
        failed: i32::try_from(failed).unwrap_or(i32::MAX),
        status: status.to_string(),
        error_summary: error_summary.map(str::to_string),
        started_at,
        completed_at: Some(chrono::Utc::now()),
    };
    let _ = diesel::insert_into(harvest_backfill_log::table)
        .values(&row)
        .execute(&mut conn)
        .await;
}

/// Write an audit record; silently swallows errors.
#[allow(clippy::too_many_arguments)]
async fn write_audit(
    pool: &HarvestDbPool,
    actor: &str,
    source: &str,
    request_id: Option<&str>,
    route: &str,
    target_id: &str,
    status: &str,
    error_summary: Option<&str>,
) {
    let Ok(mut conn) = acquire_conn(pool.default_pool()).await else {
        return;
    };
    let ar = NewAuditRecord {
        actor,
        operation: OP_SCHEDULE_BACKFILL,
        target_type: TARGET_SCHEDULE,
        target_id: Some(target_id),
        route_or_command: route,
        request_id,
        idempotency_key: None,
        status,
        error_summary,
        shard_id: None,
        source,
    };
    let _ = audit::insert_audit(&mut conn, &ar).await;
}

/// Load a schedule row by UUID across all shards.
async fn load_schedule_by_id(
    api_state: &HarvestApiState,
    schedule_id: uuid::Uuid,
) -> Result<HarvestSchedule, AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let pool = api_state.storage_pool().map_err(map_error)?;
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let row = dsl::harvest_schedules
            .find(schedule_id)
            .select(HarvestSchedule::as_select())
            .first(&mut conn)
            .await
            .optional()
            .map_err(database_error)
            .map_err(map_error)?;
        if let Some(r) = row {
            return Ok(r);
        }
    }
    Err(AutumnError::not_found_msg(format!(
        "schedule {schedule_id}"
    )))
}

fn parse_schedule_expr_with_tz(
    expr: &str,
    timezone: &str,
) -> Result<autumn_harvest::policy::Schedule, String> {
    use autumn_harvest::policy::Schedule;

    let trimmed = expr.trim();
    let schedule = if let Some(cron) = trimmed.strip_prefix("cron:") {
        let cron_expr = cron.trim().to_string();
        if timezone == "UTC" {
            Schedule::Cron(cron_expr)
        } else {
            Schedule::CronInTimezone { expr: cron_expr, tz: timezone.to_string() }
        }
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
        if timezone == "UTC" {
            Schedule::Cron(trimmed.to_string())
        } else {
            Schedule::CronInTimezone { expr: trimmed.to_string(), tz: timezone.to_string() }
        }
    };
    // Validate cron expressions eagerly (including timezone names) so callers
    // receive a 400 rather than silently persisting an expression that will
    // never fire or an unknown timezone that would misfire.
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

struct ParsedBulkDlqRequest {
    selector: DlqBulkSelector,
    return_to: Option<String>,
    wants_redirect: bool,
}

#[derive(Debug, Clone, Default)]
struct DlqBulkSelector {
    filter: dlq::BulkDlqFilter,
    dead_letter_id: Option<uuid::Uuid>,
    task_type: Option<String>,
    shard_id: Option<i32>,
}

impl DlqBulkSelector {
    const fn is_empty(&self) -> bool {
        self.filter.is_empty()
            && self.dead_letter_id.is_none()
            && self.task_type.is_none()
            && self.shard_id.is_none()
    }

    const fn dry_run(&self) -> bool {
        self.filter.dry_run
    }
}

#[derive(Debug, Deserialize)]
struct BulkDlqApiBody {
    #[serde(default)]
    dead_letter_id: Option<uuid::Uuid>,
    #[serde(default)]
    activity_name: Option<String>,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default, alias = "task_kind")]
    task_type: Option<String>,
    #[serde(default)]
    failed_after: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    failed_before: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    shard_id: Option<i32>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    dry_run: bool,
}

impl BulkDlqApiBody {
    fn into_selector(self) -> Result<DlqBulkSelector, AutumnError> {
        let task_type = self
            .task_type
            .as_deref()
            .map(parse_dlq_task_type_filter)
            .transpose()?;
        Ok(DlqBulkSelector {
            filter: dlq::BulkDlqFilter {
                activity_name: self.activity_name,
                workflow_name: self.workflow_name,
                failed_after: self.failed_after,
                failed_before: self.failed_before,
                limit: self.limit,
                dry_run: self.dry_run,
            },
            dead_letter_id: self.dead_letter_id,
            task_type,
            shard_id: self.shard_id,
        })
    }
}

fn parse_bulk_dlq_request(
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> Result<ParsedBulkDlqRequest, AutumnError> {
    if is_form_urlencoded(headers) {
        parse_bulk_dlq_form(body)
    } else {
        let body = serde_json::from_slice::<BulkDlqApiBody>(body)
            .map_err(|e| AutumnError::bad_request_msg(format!("invalid JSON body: {e}")))?;
        Ok(ParsedBulkDlqRequest {
            selector: body.into_selector()?,
            return_to: None,
            wants_redirect: false,
        })
    }
}

fn is_form_urlencoded(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| {
            mime.trim()
                .eq_ignore_ascii_case("application/x-www-form-urlencoded")
        })
}

fn parse_bulk_dlq_form(body: &[u8]) -> Result<ParsedBulkDlqRequest, AutumnError> {
    let raw = std::str::from_utf8(body)
        .map_err(|_| AutumnError::bad_request_msg("form body must be valid UTF-8"))?;
    let mut selector = DlqBulkSelector::default();
    let mut return_to = None;

    for (key, value) in parse_urlencoded_form(raw)? {
        let field = key.trim();
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        match field {
            "dead_letter_id" => {
                selector.dead_letter_id = Some(parse_uuid(value, "dead_letter_id")?);
            }
            "activity_name" => selector.filter.activity_name = Some(value.to_string()),
            "workflow_name" => selector.filter.workflow_name = Some(value.to_string()),
            "task_kind" | "task_type" => {
                selector.task_type = Some(parse_dlq_task_type_filter(value)?);
            }
            "failed_after" => {
                selector.filter.failed_after = Some(parse_utc_datetime(value, "failed_after")?);
            }
            "failed_before" => {
                selector.filter.failed_before = Some(parse_utc_datetime(value, "failed_before")?);
            }
            "limit" => selector.filter.limit = Some(parse_u32_field(value, "limit")?),
            "dry_run" => selector.filter.dry_run = parse_bool_field(value, "dry_run")?,
            "shard_id" => selector.shard_id = Some(parse_i32_field(value, "shard_id")?),
            "return_to" => return_to = Some(value.to_string()),
            _ => {}
        }
    }

    Ok(ParsedBulkDlqRequest {
        selector,
        return_to,
        wants_redirect: true,
    })
}

fn parse_urlencoded_form(raw: &str) -> Result<Vec<(String, String)>, AutumnError> {
    raw.split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            Ok((decode_form_component(key)?, decode_form_component(value)?))
        })
        .collect()
}

fn decode_form_component(input: &str) -> Result<String, AutumnError> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' => {
                if index + 2 >= bytes.len() {
                    return Err(AutumnError::bad_request_msg(
                        "invalid percent-encoded form component",
                    ));
                }
                let high = hex_value(bytes[index + 1]).ok_or_else(|| {
                    AutumnError::bad_request_msg("invalid percent-encoded form component")
                })?;
                let low = hex_value(bytes[index + 2]).ok_or_else(|| {
                    AutumnError::bad_request_msg("invalid percent-encoded form component")
                })?;
                out.push((high << 4) | low);
                index += 3;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8(out)
        .map_err(|_| AutumnError::bad_request_msg("form component must be valid UTF-8"))
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_dlq_task_type_filter(value: &str) -> Result<String, AutumnError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "activity" => Ok("ACTIVITY".to_string()),
        "workflow" => Ok("WORKFLOW".to_string()),
        other => Err(AutumnError::bad_request_msg(format!(
            "unknown task_type '{other}'; expected Activity or Workflow"
        ))),
    }
}

fn parse_utc_datetime(
    value: &str,
    field: &str,
) -> Result<chrono::DateTime<chrono::Utc>, AutumnError> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|_| {
            AutumnError::bad_request_msg(format!("invalid {field}; expected RFC 3339 timestamp"))
        })
}

fn parse_u32_field(value: &str, field: &str) -> Result<u32, AutumnError> {
    value.parse::<u32>().map_err(|_| {
        AutumnError::bad_request_msg(format!("invalid {field}; expected unsigned integer"))
    })
}

fn parse_i32_field(value: &str, field: &str) -> Result<i32, AutumnError> {
    value
        .parse::<i32>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid {field}; expected integer")))
}

fn parse_bool_field(value: &str, field: &str) -> Result<bool, AutumnError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(AutumnError::bad_request_msg(format!(
            "invalid {field}; expected boolean"
        ))),
    }
}

fn dlq_bulk_empty_filter_response(request: &ParsedBulkDlqRequest) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    const MESSAGE: &str = "bulk filter must specify at least one criterion: dead_letter_id, \
                           activity_name, workflow_name, task_type, failed_after, failed_before, \
                           or shard_id";

    if request.wants_redirect {
        return dlq_form_redirect(request.return_to.as_deref(), MESSAGE);
    }

    (
        axum::http::StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": MESSAGE })),
    )
        .into_response()
}

fn dlq_bulk_flash(result: &dlq::BulkDlqResult, verb: &str) -> String {
    if result.failures.is_empty() {
        format!(
            "{} dead-letter entr{} {verb}",
            result.acted_on,
            if result.acted_on == 1 { "y" } else { "ies" }
        )
    } else {
        format!(
            "{} dead-letter entries {verb}; {} failures",
            result.acted_on,
            result.failures.len()
        )
    }
}

struct DlqBulkAuditContext<'a> {
    actor: &'a str,
    source: &'a str,
    request_id: Option<&'a str>,
    operation: &'static str,
    route: &'static str,
    target_id: Option<&'a str>,
    shard_id: Option<i32>,
}

async fn insert_dlq_bulk_audit(
    api_state: &HarvestApiState,
    context: &DlqBulkAuditContext<'_>,
    status: &str,
    error_summary: Option<&str>,
) -> Result<(), AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let ar = NewAuditRecord {
        actor: context.actor,
        operation: context.operation,
        target_type: TARGET_DEAD_LETTER,
        target_id: context.target_id,
        route_or_command: context.route,
        request_id: context.request_id,
        idempotency_key: None,
        status,
        error_summary,
        shard_id: context.shard_id,
        source: context.source,
    };
    audit::insert_audit(&mut conn, &ar)
        .await
        .map(|_| ())
        .map_err(map_error)
}

fn dlq_form_redirect(return_to: Option<&str>, flash: &str) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    axum::response::Redirect::to(&dlq_redirect_location(return_to, flash)).into_response()
}

fn dlq_redirect_location(return_to: Option<&str>, flash: &str) -> String {
    let base = return_to
        .map(str::trim)
        .and_then(safe_dlq_redirect_base)
        .unwrap_or_else(|| "../ui/dead-letters".to_string());
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}flash={}", url_encode_for_redirect(flash))
}

fn safe_dlq_redirect_base(value: &str) -> Option<String> {
    if is_dead_letter_ui_return_path(value, "../ui/dead-letters")
        || is_dead_letter_ui_return_path(value, "/api/harvest/ui/dead-letters")
    {
        Some(value.to_string())
    } else if is_dead_letter_ui_return_path(value, "ui/dead-letters") {
        Some(format!("../{value}"))
    } else {
        None
    }
}

fn is_dead_letter_ui_return_path(value: &str, prefix: &str) -> bool {
    match value.strip_prefix(prefix) {
        Some("") => true,
        Some(rest) => rest.starts_with('?'),
        None => false,
    }
}

fn url_encode_for_redirect(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

async fn bulk_replay_dead_letters_handler(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let request = match parse_bulk_dlq_request(&headers, &body) {
        Ok(request) => request,
        Err(error) => return error.into_response(),
    };
    let (actor, mut source, request_id) = audit_context(&headers, &api_state);
    if request.wants_redirect && source == SOURCE_API {
        source = "ui".to_string();
    }
    let route = "POST /dead-letters/replay";

    if request.selector.is_empty() {
        dlq_bulk_audit_reject_empty_filter(
            &api_state,
            &actor,
            &source,
            request_id.as_deref(),
            OP_DLQ_REPLAY_BULK,
            route,
        )
        .await;
        return dlq_bulk_empty_filter_response(&request);
    }

    // Dry-run previews are read-only: no audit record needed.
    if request.selector.dry_run() {
        return match bulk_replay_from_shards(&api_state, &request.selector).await {
            Ok(result) => {
                if request.wants_redirect {
                    dlq_form_redirect(
                        request.return_to.as_deref(),
                        &dlq_bulk_flash(&result, "previewed"),
                    )
                } else {
                    (axum::http::StatusCode::OK, Json(result)).into_response()
                }
            }
            Err(e) => map_error(e).into_response(),
        };
    }

    let target_id = request.selector.dead_letter_id.map(|id| id.to_string());
    let shard_id = request.selector.shard_id;
    let replay_result = bulk_replay_from_shards(&api_state, &request.selector).await;
    let audit_context = DlqBulkAuditContext {
        actor: &actor,
        source: &source,
        request_id: request_id.as_deref(),
        operation: OP_DLQ_REPLAY_BULK,
        route,
        target_id: target_id.as_deref(),
        shard_id,
    };

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
            if let Err(error) = insert_dlq_bulk_audit(
                &api_state,
                &audit_context,
                audit_status,
                audit_error.as_deref(),
            )
            .await
            {
                return error.into_response();
            }
            if request.wants_redirect {
                dlq_form_redirect(
                    request.return_to.as_deref(),
                    &dlq_bulk_flash(&result, "replayed"),
                )
            } else {
                (status, Json(result)).into_response()
            }
        }
        Err(e) => {
            let err_str = e.to_string();
            let _ = insert_dlq_bulk_audit(
                &api_state,
                &audit_context,
                STATUS_FAILED,
                Some(err_str.as_str()),
            )
            .await;
            map_error(e).into_response()
        }
    }
}

async fn bulk_discard_dead_letters_handler(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let request = match parse_bulk_dlq_request(&headers, &body) {
        Ok(request) => request,
        Err(error) => return error.into_response(),
    };
    let (actor, mut source, request_id) = audit_context(&headers, &api_state);
    if request.wants_redirect && source == SOURCE_API {
        source = "ui".to_string();
    }
    let route = "POST /dead-letters/discard";

    if request.selector.is_empty() {
        dlq_bulk_audit_reject_empty_filter(
            &api_state,
            &actor,
            &source,
            request_id.as_deref(),
            OP_DLQ_DISCARD_BULK,
            route,
        )
        .await;
        return dlq_bulk_empty_filter_response(&request);
    }

    // Dry-run previews are read-only: no audit record needed.
    if request.selector.dry_run() {
        return match bulk_discard_from_shards(&api_state, &request.selector).await {
            Ok(result) => {
                if request.wants_redirect {
                    dlq_form_redirect(
                        request.return_to.as_deref(),
                        &dlq_bulk_flash(&result, "previewed"),
                    )
                } else {
                    (axum::http::StatusCode::OK, Json(result)).into_response()
                }
            }
            Err(e) => map_error(e).into_response(),
        };
    }

    let target_id = request.selector.dead_letter_id.map(|id| id.to_string());
    let shard_id = request.selector.shard_id;
    let discard_result = bulk_discard_from_shards(&api_state, &request.selector).await;
    let audit_context = DlqBulkAuditContext {
        actor: &actor,
        source: &source,
        request_id: request_id.as_deref(),
        operation: OP_DLQ_DISCARD_BULK,
        route,
        target_id: target_id.as_deref(),
        shard_id,
    };

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
            if let Err(error) = insert_dlq_bulk_audit(
                &api_state,
                &audit_context,
                audit_status,
                audit_error.as_deref(),
            )
            .await
            {
                return error.into_response();
            }
            if request.wants_redirect {
                dlq_form_redirect(
                    request.return_to.as_deref(),
                    &dlq_bulk_flash(&result, "discarded"),
                )
            } else {
                (status, Json(result)).into_response()
            }
        }
        Err(e) => {
            let err_str = e.to_string();
            let _ = insert_dlq_bulk_audit(
                &api_state,
                &audit_context,
                STATUS_FAILED,
                Some(err_str.as_str()),
            )
            .await;
            map_error(e).into_response()
        }
    }
}

async fn bulk_replay_from_shards(
    api_state: &HarvestApiState,
    selector: &DlqBulkSelector,
) -> Result<dlq::BulkDlqResult, HarvestError> {
    let pool = api_state.storage_pool()?;
    let runtime = api_state.runtime().ok();
    let registry = runtime.as_ref().map(|r| r.registry().as_ref());
    let mut total = dlq::BulkDlqResult {
        matched: 0,
        acted_on: 0,
        skipped: 0,
        ids: Vec::new(),
        dry_run: selector.dry_run(),
        failures: Vec::new(),
    };

    // Enforce the limit as a global cap across all shards, not per-shard.
    // effective_limit() is guaranteed to be in [1, 1000] so both try_from
    // conversions below are infallible in practice.
    let mut remaining: u32 =
        u32::try_from(selector.filter.effective_limit()).unwrap_or(dlq::DEFAULT_BULK_LIMIT);

    for (shard_id, shard_pool) in pool.iter_shards() {
        if selector
            .shard_id
            .is_some_and(|wanted| wanted != shard_id.as_i32())
        {
            continue;
        }
        let mut conn = shard_pool
            .get()
            .await
            .map_err(|e| HarvestError::Database(e.to_string()))?;

        if remaining == 0 {
            // Budget exhausted: count-only so matched reflects all shards.
            let shard_matched = count_api_bulk_filter_matches(&mut conn, selector)
                .await
                .map(|n| usize::try_from(n).unwrap_or(0))?;
            total.matched += shard_matched;
            continue;
        }

        let mut shard_selector = selector.clone();
        shard_selector.filter.limit = Some(remaining);
        let shard_result =
            bulk_replay_dead_letters_for_selector(&mut conn, &shard_selector, registry).await?;
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
    selector: &DlqBulkSelector,
) -> Result<dlq::BulkDlqResult, HarvestError> {
    let pool = api_state.storage_pool()?;
    let mut total = dlq::BulkDlqResult {
        matched: 0,
        acted_on: 0,
        skipped: 0,
        ids: Vec::new(),
        dry_run: selector.dry_run(),
        failures: Vec::new(),
    };

    // Enforce the limit as a global cap across all shards, not per-shard.
    let mut remaining: u32 =
        u32::try_from(selector.filter.effective_limit()).unwrap_or(dlq::DEFAULT_BULK_LIMIT);

    for (shard_id, shard_pool) in pool.iter_shards() {
        if selector
            .shard_id
            .is_some_and(|wanted| wanted != shard_id.as_i32())
        {
            continue;
        }
        let mut conn = shard_pool
            .get()
            .await
            .map_err(|e| HarvestError::Database(e.to_string()))?;

        if remaining == 0 {
            // Budget exhausted: count-only so matched reflects all shards.
            let shard_matched = count_api_bulk_filter_matches(&mut conn, selector)
                .await
                .map(|n| usize::try_from(n).unwrap_or(0))?;
            total.matched += shard_matched;
            continue;
        }

        let mut shard_selector = selector.clone();
        shard_selector.filter.limit = Some(remaining);
        let shard_result =
            bulk_discard_dead_letters_for_selector(&mut conn, &shard_selector).await?;
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

async fn count_api_bulk_filter_matches(
    conn: &mut AsyncPgConnection,
    selector: &DlqBulkSelector,
) -> HarvestResult<i64> {
    let mut query = harvest_dead_letters::table.into_boxed();
    query = apply_api_bulk_filters(query, selector);
    query.count().get_result(conn).await.map_err(database_error)
}

async fn query_dead_letters_for_api_bulk(
    conn: &mut AsyncPgConnection,
    selector: &DlqBulkSelector,
) -> HarvestResult<Vec<DeadLetter>> {
    let mut query = harvest_dead_letters::table
        .into_boxed()
        .order(harvest_dead_letters::failed_at.asc())
        .limit(selector.filter.effective_limit());
    query = apply_api_bulk_filters(query, selector);
    query
        .select(DeadLetter::as_select())
        .load(conn)
        .await
        .map_err(database_error)
}

fn apply_api_bulk_filters<'a>(
    mut query: harvest_dead_letters::BoxedQuery<'a, diesel::pg::Pg>,
    selector: &DlqBulkSelector,
) -> harvest_dead_letters::BoxedQuery<'a, diesel::pg::Pg> {
    if let Some(id) = selector.dead_letter_id {
        query = query.filter(harvest_dead_letters::id.eq(id));
    }
    if let Some(ref name) = selector.filter.activity_name {
        query = query.filter(harvest_dead_letters::activity_name.eq(name.clone()));
    }
    if let Some(ref task_type) = selector.task_type {
        query = query.filter(
            diesel::dsl::sql::<diesel::sql_types::Bool>("LOWER(task_type) = LOWER(")
                .bind::<diesel::sql_types::Text, _>(task_type.clone())
                .sql(")"),
        );
    }
    if let Some(after) = selector.filter.failed_after {
        query = query.filter(harvest_dead_letters::failed_at.ge(after));
    }
    if let Some(before) = selector.filter.failed_before {
        query = query.filter(harvest_dead_letters::failed_at.lt(before));
    }
    if let Some(ref workflow_name) = selector.filter.workflow_name {
        query = query.filter(
            diesel::dsl::sql::<diesel::sql_types::Bool>(
                "workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name = ",
            )
            .bind::<diesel::sql_types::Text, _>(workflow_name.clone())
            .sql(")"),
        );
    }
    query
}

async fn bulk_replay_dead_letters_for_selector(
    conn: &mut AsyncPgConnection,
    selector: &DlqBulkSelector,
    registry: Option<&HandlerRegistry>,
) -> HarvestResult<dlq::BulkDlqResult> {
    let matched = count_api_bulk_filter_matches(conn, selector)
        .await
        .map(|n| usize::try_from(n).unwrap_or(0))?;
    let rows = query_dead_letters_for_api_bulk(conn, selector).await?;
    let mut result = dlq::BulkDlqResult {
        matched,
        acted_on: 0,
        skipped: 0,
        ids: Vec::new(),
        dry_run: selector.dry_run(),
        failures: Vec::new(),
    };

    if selector.dry_run() {
        result.ids = rows.into_iter().map(|row| row.id.to_string()).collect();
        return Ok(result);
    }

    for row in rows {
        let id = row.id;
        match dlq::replay_dead_letter(conn, id, registry).await {
            Ok(_) => {
                result.acted_on += 1;
                result.ids.push(id.to_string());
            }
            Err(HarvestError::NotFound(_)) => result.skipped += 1,
            Err(error) => result.failures.push(dlq::BulkDlqFailure {
                id: id.to_string(),
                reason: error.to_string(),
            }),
        }
    }

    Ok(result)
}

async fn bulk_discard_dead_letters_for_selector(
    conn: &mut AsyncPgConnection,
    selector: &DlqBulkSelector,
) -> HarvestResult<dlq::BulkDlqResult> {
    let matched = count_api_bulk_filter_matches(conn, selector)
        .await
        .map(|n| usize::try_from(n).unwrap_or(0))?;
    let rows = query_dead_letters_for_api_bulk(conn, selector).await?;
    let mut result = dlq::BulkDlqResult {
        matched,
        acted_on: 0,
        skipped: 0,
        ids: Vec::new(),
        dry_run: selector.dry_run(),
        failures: Vec::new(),
    };

    if selector.dry_run() {
        result.ids = rows.into_iter().map(|row| row.id.to_string()).collect();
        return Ok(result);
    }

    for row in rows {
        let id = row.id;
        let deleted = diesel::delete(harvest_dead_letters::table.find(id))
            .execute(conn)
            .await
            .map_err(database_error)?;
        if deleted > 0 {
            result.acted_on += 1;
            result.ids.push(id.to_string());
        } else {
            result.skipped += 1;
        }
    }

    Ok(result)
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

    // Keyed by (concurrency_key, task_type) — same granularity as the claim
    // query — so workflow and activity caps for the same key are not collapsed.
    let mut merged: std::collections::HashMap<(String, String), ConcurrencyKeyStats> =
        std::collections::HashMap::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let stats = queue::concurrency_key_stats(&mut conn, &runtime.queues)
            .await
            .map_err(map_error)?;
        for stat in stats {
            let merge_key = (stat.key.clone(), stat.task_type.clone());
            let entry = merged
                .entry(merge_key)
                .or_insert_with(|| ConcurrencyKeyStats {
                    key: stat.key.clone(),
                    task_type: stat.task_type.clone(),
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
    result.sort_by(|a, b| a.key.cmp(&b.key).then(a.task_type.cmp(&b.task_type)));
    Ok(Json(result))
}

async fn health(Extension(api_state): Extension<HarvestApiState>) -> axum::response::Response {
    let runtime = api_state.runtime().ok();
    let scheduler = runtime
        .as_ref()
        .map_or_else(SchedulerMonitor::offline, |runtime| {
            runtime.scheduler.clone()
        })
        .snapshot();
    let shard_readiness_enforced = api_state.health_requires_shard_readiness();
    let shard_readiness = if shard_readiness_enforced {
        Some(build_shard_health_report(&api_state, None).await)
    } else {
        None
    };
    let status = if shard_readiness_enforced
        && shard_readiness
            .as_ref()
            .is_some_and(|report| report.overall_readiness != ShardReadiness::Ready)
    {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (
        status,
        Json(HarvestHealth {
            runtime_ready: runtime.is_some(),
            worker_id: runtime
                .as_ref()
                .and_then(|runtime| runtime.worker_id.clone()),
            queues: runtime
                .as_ref()
                .map_or_else(Vec::new, |runtime| runtime.queues.clone()),
            dag_count: runtime.as_ref().map_or(0, |runtime| runtime.dags.len()),
            scheduler,
            shard_readiness_enforced,
            shard_readiness,
        }),
    )
        .into_response()
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
    use diesel::sql_types::{Bool, Jsonb, Text};

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
    if let Some(after) = filters.started_after {
        query = query.filter(harvest_workflow_executions::started_at.ge(after));
    }
    if let Some(before) = filters.started_before {
        query = query.filter(harvest_workflow_executions::started_at.le(before));
    }
    if let Some(prefix) = &filters.exec_id_prefix {
        // Cast the UUID column to text and apply a case-insensitive prefix match.
        // Uses the `CAST(id AS TEXT) ILIKE $1` form so the query is index-friendly
        // for short prefixes and never requires a full sequential scan.
        let pattern = format!("{}%", prefix.to_lowercase());
        query = query.filter(sql::<Bool>("CAST(id AS TEXT) ILIKE ").bind::<Text, _>(pattern));
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

fn export_history_for_execution(
    execution: &WorkflowExecution,
    events: Vec<WorkflowEvent>,
    query: &HistoryExportQuery,
) -> Result<HistoryExportDocument, HistoryExportError> {
    export_history(HistoryExportRequest {
        workflow_name: execution.workflow_name.clone(),
        execution_id: ExecutionId::from_uuid(execution.id),
        shard_id: execution.shard_id,
        state: execution.state.clone(),
        events,
        exported_at: chrono::Utc::now(),
        payload_policy: query.payload_policy,
        max_bytes: Some(query.max_bytes),
    })
}

fn export_history_for_candidate(
    candidate: &HistoryExportCandidate,
    events: Vec<WorkflowEvent>,
    query: &HistoryExportQuery,
) -> Result<HistoryExportDocument, HistoryExportError> {
    export_history(HistoryExportRequest {
        workflow_name: candidate.workflow_name.clone(),
        execution_id: ExecutionId::from_uuid(candidate.id),
        shard_id: candidate.shard_id,
        state: candidate.state.clone(),
        events,
        exported_at: chrono::Utc::now(),
        payload_policy: query.payload_policy,
        max_bytes: Some(query.max_bytes),
    })
}

async fn load_history_exports_from_shards(
    api_state: &HarvestApiState,
    query: &HistoryBatchExportQuery,
) -> Result<HistoryBatchExportResponse, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut work = HistoryBatchExportWork::default();

    collect_history_export_candidates_from_shards(&pool, query, &mut work).await;
    export_selected_history_candidates(&pool, query, &mut work).await;
    if let Some(shard_id) = query.shard_id
        && !work.saw_requested_shard
    {
        work.note_unavailable(shard_id, "shard pool is not configured".to_string());
    }
    work.normalize_coverage();
    Ok(history_batch_export_response(query, work))
}

async fn collect_history_export_candidates_from_shards(
    pool: &HarvestDbPool,
    query: &HistoryBatchExportQuery,
    work: &mut HistoryBatchExportWork,
) {
    for (shard, shard_pool) in pool.iter_shards() {
        let shard_id = shard.as_i32();
        if query.shard_id.is_some_and(|target| target != shard_id) {
            continue;
        }
        work.saw_requested_shard = true;

        let mut conn = match acquire_conn(shard_pool).await {
            Ok(conn) => conn,
            Err(error) => {
                work.note_unavailable(shard_id, error.to_string());
                continue;
            }
        };
        work.inspected_shards.push(shard_id);

        let rows = match load_history_export_candidates(&mut conn, query).await {
            Ok(rows) => rows,
            Err(error) => {
                work.note_unavailable(shard_id, error.to_string());
                continue;
            }
        };
        if !rows.is_empty() {
            work.matched_shards.push(shard_id);
        }
        work.candidates.extend(rows);
    }
}

async fn export_selected_history_candidates(
    pool: &HarvestDbPool,
    query: &HistoryBatchExportQuery,
    work: &mut HistoryBatchExportWork,
) {
    sort_history_export_candidates(&mut work.candidates);
    let single_query = HistoryExportQuery {
        payload_policy: query.payload_policy,
        max_bytes: query.max_bytes,
    };
    let candidates = std::mem::take(&mut work.candidates);
    for candidate in candidates {
        if work.exports.len() >= query.limit {
            break;
        }
        export_history_candidate(pool, &candidate, &single_query, work).await;
    }
}

async fn export_history_candidate(
    pool: &HarvestDbPool,
    candidate: &HistoryExportCandidate,
    query: &HistoryExportQuery,
    work: &mut HistoryBatchExportWork,
) {
    let shard_id = candidate.shard_id;
    let exec_id = ExecutionId::from_uuid(candidate.id);
    let mut conn = match acquire_conn(pool.pool_for(ShardId::new(shard_id))).await {
        Ok(conn) => conn,
        Err(error) => {
            work.note_unavailable(shard_id, error.to_string());
            return;
        }
    };
    let history = match store::load_history(&mut conn, exec_id).await {
        Ok(history) => history,
        Err(error) => {
            work.failures.push(HistoryExportFailure {
                execution_id: Some(exec_id.to_string()),
                shard_id,
                reason: error.to_string(),
                actual_bytes: None,
                max_bytes: None,
            });
            return;
        }
    };
    match export_history_for_candidate(candidate, history.events, query) {
        Ok(document) => work.exports.push(document),
        Err(HistoryExportError::SizeLimitExceeded {
            actual_bytes,
            max_bytes,
        }) => work.failures.push(HistoryExportFailure {
            execution_id: Some(exec_id.to_string()),
            shard_id,
            reason: "history export exceeds max_bytes".to_string(),
            actual_bytes: Some(actual_bytes),
            max_bytes: Some(max_bytes),
        }),
        Err(error) => work.failures.push(HistoryExportFailure {
            execution_id: Some(exec_id.to_string()),
            shard_id,
            reason: error.to_string(),
            actual_bytes: None,
            max_bytes: None,
        }),
    }
}

fn history_batch_export_response(
    query: &HistoryBatchExportQuery,
    work: HistoryBatchExportWork,
) -> HistoryBatchExportResponse {
    HistoryBatchExportResponse {
        status: work.status(),
        observed_at: chrono::Utc::now(),
        payload_policy: query.payload_policy,
        filters: HistoryBatchExportFiltersResponse {
            workflow_name: query.workflow_name.clone(),
            states: query.states.clone(),
            updated_after: query.updated_after,
            updated_before: query.updated_before,
            shard_id: query.shard_id,
            limit: query.limit,
            max_bytes: query.max_bytes,
        },
        exports: work.exports,
        failures: work.failures,
        shard_coverage: ExternalHandoffShardCoverage {
            inspected: work.inspected_shards,
            matched: work.matched_shards,
            unavailable: work.unavailable_shards,
        },
    }
}

const HISTORY_EXPORT_CANDIDATES_SQL: &str = r"
SELECT
    w.id AS id,
    w.workflow_name AS workflow_name,
    w.shard_id AS shard_id,
    w.state AS state,
    COALESCE(MAX(e.timestamp), w.completed_at, w.started_at, w.created_at) AS last_history_event_at
FROM harvest_workflow_executions w
LEFT JOIN harvest_events e
    ON e.workflow_exec_id = w.id
WHERE ($1::TEXT IS NULL OR w.workflow_name = $1::TEXT)
  AND (cardinality($2::TEXT[]) = 0 OR w.state = ANY($2::TEXT[]))
GROUP BY w.id, w.workflow_name, w.shard_id, w.state, w.completed_at, w.started_at, w.created_at
HAVING ($3::TIMESTAMPTZ IS NULL OR COALESCE(MAX(e.timestamp), w.completed_at, w.started_at, w.created_at) >= $3::TIMESTAMPTZ)
   AND ($4::TIMESTAMPTZ IS NULL OR COALESCE(MAX(e.timestamp), w.completed_at, w.started_at, w.created_at) < $4::TIMESTAMPTZ)
ORDER BY last_history_event_at DESC, w.id DESC
LIMIT $5
";

const fn history_export_candidates_sql() -> &'static str {
    HISTORY_EXPORT_CANDIDATES_SQL
}

async fn load_history_export_candidates(
    conn: &mut AsyncPgConnection,
    filters: &HistoryBatchExportQuery,
) -> HarvestResult<Vec<HistoryExportCandidate>> {
    let limit = i64::try_from(filters.limit).unwrap_or(i64::MAX);
    diesel::sql_query(history_export_candidates_sql())
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
            filters.workflow_name.clone(),
        )
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(filters.states.clone())
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
            filters.updated_after,
        )
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(
            filters.updated_before,
        )
        .bind::<diesel::sql_types::BigInt, _>(limit)
        .load(conn)
        .await
        .map_err(database_error)
}

fn sort_history_export_candidates(candidates: &mut [HistoryExportCandidate]) {
    candidates.sort_by(|left, right| {
        right
            .last_history_event_at
            .cmp(&left.last_history_event_at)
            .then_with(|| right.id.cmp(&left.id))
    });
}

fn history_export_error_response(error: HistoryExportError) -> axum::response::Response {
    match error {
        HistoryExportError::SizeLimitExceeded {
            actual_bytes,
            max_bytes,
        } => (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "message": "history export exceeds max_bytes",
                "actual_bytes": actual_bytes,
                "max_bytes": max_bytes,
                "truncation_behavior": "fail"
            })),
        )
            .into_response(),
        HistoryExportError::Serialization(error) => {
            map_error(HarvestError::from(error)).into_response()
        }
    }
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
    let runtime = api_state.runtime().ok();
    let registry = runtime.as_ref().map(|r| r.registry().as_ref());

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        match dlq::replay_dead_letter(&mut conn, dead_letter_id, registry).await {
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

fn parse_workflow_result_wait_query(
    pairs: &[(String, String)],
    max_wait: Duration,
) -> Result<Duration, AutumnError> {
    let mut wait = Duration::ZERO;
    for (key, value) in pairs {
        match key.as_str() {
            "wait" => wait = parse_workflow_result_wait_duration(value)?,
            other => {
                return Err(AutumnError::bad_request_msg(format!(
                    "unknown workflow result query parameter '{other}'"
                )));
            }
        }
    }
    Ok(wait.min(max_wait))
}

fn parse_workflow_result_wait_duration(raw: &str) -> Result<Duration, AutumnError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(AutumnError::bad_request_msg(
            "invalid wait duration ''; expected milliseconds, seconds, minutes, or hours",
        ));
    }

    if let Some(ms) = value.strip_suffix("ms") {
        return parse_duration_amount(ms, "wait", Duration::from_millis);
    }
    if let Some(seconds) = value.strip_suffix('s') {
        return parse_duration_amount(seconds, "wait", Duration::from_secs);
    }
    if let Some(minutes) = value.strip_suffix('m') {
        return parse_duration_amount(minutes, "wait", |amount| {
            Duration::from_secs(amount.saturating_mul(60))
        });
    }
    if let Some(hours) = value.strip_suffix('h') {
        return parse_duration_amount(hours, "wait", |amount| {
            Duration::from_secs(amount.saturating_mul(60 * 60))
        });
    }

    parse_duration_amount(value, "wait", Duration::from_secs)
}

fn parse_duration_amount(
    raw: &str,
    label: &str,
    build: impl FnOnce(u64) -> Duration,
) -> Result<Duration, AutumnError> {
    let amount = raw.trim().parse::<u64>().map_err(|_| {
        AutumnError::bad_request_msg(format!(
            "invalid {label} duration '{raw}'; expected milliseconds, seconds, minutes, or hours"
        ))
    })?;
    Ok(build(amount))
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
        HarvestError::NotFound(message)
        | HarvestError::UpdateHandlerNotFound(message)
        | HarvestError::QueryHandlerNotFound(message) => AutumnError::not_found_msg(message),
        HarvestError::WorkflowNotRunning(exec_id) => {
            AutumnError::bad_request_msg(format!("workflow not running: {exec_id}"))
                .with_status(axum::http::StatusCode::CONFLICT)
        }
        // Intentional handler errors (returned Err, not panicked) → 400.
        HarvestError::QueryHandlerFailed(msg) => AutumnError::bad_request_msg(msg),
        // Actual panics are an engine fault → 503.
        HarvestError::QueryHandlerPanicked(msg) => {
            AutumnError::service_unavailable_msg(format!("query handler panicked: {msg}"))
        }
        HarvestError::QueryTimedOut {
            query_name,
            timeout_ms,
        } => AutumnError::bad_request_msg(format!(
            "query '{query_name}' timed out after {timeout_ms}ms"
        ))
        .with_status(axum::http::StatusCode::REQUEST_TIMEOUT),
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
    status: String,
    current_state: String,
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

    let (newly_resolved, current_state) = complete_result?;
    Ok(Json(ExternalActivityAck {
        ok: true,
        newly_resolved,
        status: if newly_resolved {
            "completed"
        } else {
            "already_terminal"
        }
        .to_string(),
        current_state,
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

    let (newly_resolved, current_state) = fail_result?;
    Ok(Json(ExternalActivityAck {
        ok: true,
        newly_resolved,
        status: if newly_resolved {
            "failed"
        } else {
            "already_terminal"
        }
        .to_string(),
        current_state,
    }))
}

async fn heartbeat_external_activity(
    Extension(api_state): Extension<HarvestApiState>,
    Path(token_str): Path<String>,
    Json(request): Json<HeartbeatExternalActivityRequest>,
) -> Result<Json<ExternalActivityAck>, AutumnError> {
    let token = parse_external_token(&token_str)?;
    let pool = api_state.storage_pool().map_err(map_error)?;

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        if let Some(task) = external_task::find_by_token(&mut conn, token)
            .await
            .map_err(map_error)?
        {
            if task.state != "PENDING" {
                return Ok(Json(ExternalActivityAck {
                    ok: true,
                    newly_resolved: false,
                    status: "already_terminal".to_string(),
                    current_state: task.state,
                }));
            }
            // Default to the original configured duration so that omitting
            // extend_by_secs resets the deadline by the same fixed window every
            // time, regardless of how many heartbeats have already fired.
            let original_secs = u64::try_from(task.schedule_to_close_secs).unwrap_or(1);
            let extend_by = request.extend_by_secs.unwrap_or(original_secs);
            external_task::extend_deadline(&mut conn, token, extend_by)
                .await
                .map_err(map_error)?;
            return Ok(Json(ExternalActivityAck {
                ok: true,
                newly_resolved: false,
                status: "extended".to_string(),
                current_state: "PENDING".to_string(),
            }));
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
/// happened, plus the current durable handoff state.
async fn resolve_external_on_shards<F>(
    api_state: &HarvestApiState,
    token: ExternalActivityToken,
    action: F,
) -> Result<(bool, String), AutumnError>
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
            let current_state = external_task::find_by_token(&mut conn, token)
                .await
                .map_err(map_error)?
                .map_or_else(|| "UNKNOWN".to_string(), |task| task.state);
            return Ok((result, current_state));
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
        // Skip unavailable shards rather than returning 500; the worker may
        // live on a reachable shard even when others are down, and the --wait
        // poll loop must not abort just because an unrelated shard is offline.
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        if let Some(row) = get_worker(&mut conn, &worker_id, stale_threshold)
            .await
            .map_err(map_error)?
        {
            return Ok(Json(row));
        }
    }

    Err(AutumnError::not_found_msg(format!("worker '{worker_id}'")))
}

/// `GET /workers/{worker_id}/pinned`
///
/// Lists workflow executions currently soft-pinned to the given worker by the
/// sticky-routing affinity mechanism (issue #235). An execution appears here
/// while its `sticky_worker_id` column points at this worker — i.e., after the
/// worker most recently parked a follow-up task for it and before the execution
/// either completes or is reclaimed by another worker after lease expiry.
///
/// Returns an empty array (not 404) if the worker exists but has no pinned
/// executions.
async fn worker_pinned_executions_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Path(worker_id): Path<String>,
) -> Result<Json<Vec<PinnedExecutionRow>>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut results = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = list_pinned_executions(&mut conn, &worker_id)
            .await
            .map_err(map_error)?;
        results.append(&mut rows);
    }

    results.sort_by_key(|r| r.started_at);
    Ok(Json(results))
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
// Remote drain controls (issue #170)
// ---------------------------------------------------------------------------

/// Request body for `POST /workers/{worker_id}/drain`.
#[derive(Debug, Deserialize)]
struct DrainWorkerRequest {
    /// Optional ISO 8601 deadline by which the worker must have drained.
    /// When absent the server uses its configured worker shutdown timeout.
    #[serde(default)]
    deadline_at: Option<String>,
}

#[allow(clippy::too_many_lines)]
async fn request_drain_handler(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(worker_id): Path<String>,
    Json(request): Json<DrainWorkerRequest>,
) -> Result<Json<DrainResponse>, AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let stale_threshold = api_state.worker_stale_threshold();
    let pool = api_state.storage_pool().map_err(map_error)?;

    // Track whether the deadline is operator-supplied so we can avoid
    // shortening an existing window when re-draining without --deadline.
    let (deadline_at, deadline_is_explicit) = if let Some(raw) = &request.deadline_at {
        let dt = chrono::DateTime::parse_from_rfc3339(raw).map_err(|_| {
            AutumnError::bad_request_msg(format!(
                "invalid deadline_at '{raw}'; expected RFC 3339 (e.g. 2026-05-09T12:00:00Z)"
            ))
        })?;
        (Some(dt.with_timezone(&chrono::Utc)), true)
    } else {
        // Compute a default deadline from the configured worker shutdown timeout so
        // operators always get a finite drain window even when they omit the field.
        let timeout = api_state.worker_shutdown_timeout();
        let computed = chrono::Duration::from_std(timeout)
            .ok()
            .map(|d| chrono::Utc::now() + d);
        (computed, false)
    };

    // Search every shard for the worker — workers are registered on exactly
    // one shard, so the first hit wins. Connection failures on individual shards
    // are recorded as unavailable rather than aborting the whole request (AC #8).
    let mut unavailable_shards: Vec<i32> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            unavailable_shards.push(shard_id.as_i32());
            continue;
        };

        let mut response = request_drain(
            &mut conn,
            &worker_id,
            deadline_at,
            deadline_is_explicit,
            stale_threshold,
        )
        .await
        .map_err(map_error)?;

        if response.outcome == autumn_harvest::workers::DrainOutcome::NotFound {
            continue;
        }

        response.unavailable_shards = std::mem::take(&mut unavailable_shards);

        let ar = NewAuditRecord {
            actor: &actor,
            source: &source,
            operation: OP_WORKER_DRAIN,
            target_type: TARGET_WORKER,
            target_id: Some(worker_id.as_str()),
            route_or_command: "POST /workers/{worker_id}/drain",
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: Some(shard_id.as_i32()),
        };
        audit::insert_audit(&mut conn, &ar)
            .await
            .map_err(map_error)?;

        return Ok(Json(response));
    }

    // Worker not found on any reachable shard. If some shards were unavailable
    // the worker may live there — return a degraded 200 rather than 404.
    // Write an audit record on any reachable shard so the attempt is traceable
    // even when the owning shard is down.
    if !unavailable_shards.is_empty() {
        'audit: for (_shard_id, shard_pool) in pool.iter_shards() {
            if let Ok(mut conn) = acquire_conn(shard_pool).await {
                let ar = NewAuditRecord {
                    actor: &actor,
                    source: &source,
                    operation: OP_WORKER_DRAIN,
                    target_type: TARGET_WORKER,
                    target_id: Some(worker_id.as_str()),
                    route_or_command: "POST /workers/{worker_id}/drain",
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some(
                        "degraded: worker not found on reachable shards; may exist on unavailable shard",
                    ),
                    shard_id: None,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
                break 'audit;
            }
        }
        return Ok(Json(DrainResponse {
            worker_id: worker_id.clone(),
            outcome: autumn_harvest::workers::DrainOutcome::NotFound,
            in_flight_count: 0,
            drain_deadline_at: None,
            shard_ids: vec![],
            unavailable_shards,
        }));
    }

    // All shards reachable but the worker ID was absent on every one.
    // Write an audit record on any available shard so that
    // `harvest audit list --operation worker.drain --target-id <id>`
    // shows the attempted drain even for a 404 response.
    'audit: for (_shard_id, shard_pool) in pool.iter_shards() {
        if let Ok(mut conn) = acquire_conn(shard_pool).await {
            let ar = NewAuditRecord {
                actor: &actor,
                source: &source,
                operation: OP_WORKER_DRAIN,
                target_type: TARGET_WORKER,
                target_id: Some(worker_id.as_str()),
                route_or_command: "POST /workers/{worker_id}/drain",
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("worker not found"),
                shard_id: None,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            break 'audit;
        }
    }
    Err(AutumnError::not_found_msg(format!("worker '{worker_id}'")))
}

async fn drain_preview_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<Vec<DrainPreviewItem>>, AutumnError> {
    let filters = parse_worker_filters_api(&pairs)?;
    let stale_threshold = api_state.worker_stale_threshold();
    let pool = api_state.storage_pool().map_err(map_error)?;

    let per_shard_filters = WorkerFilters {
        limit: i64::MAX,
        ..filters.clone()
    };

    let mut results: Vec<DrainPreviewItem> = Vec::new();
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut items = drain_preview(&mut conn, &per_shard_filters, stale_threshold)
            .await
            .map_err(map_error)?;
        results.append(&mut items);
    }

    results.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
    results.truncate(usize::try_from(filters.limit).unwrap_or(usize::MAX));
    Ok(Json(results))
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
    use testcontainers::ContainerAsync;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    fn pairs(values: &[(&str, &str)]) -> Vec<(String, String)> {
        values
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn dag_registration_marker_is_separate_from_workflow_registry() {
        let registry = Arc::new(HandlerRegistry::new(
            vec![autumn_harvest::WorkflowInfo {
                name: "workflow_only",
                module: "tests",
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
                execution_timeout: None,
                concurrency: None,
            }],
            vec![],
        ));
        let runtime = HarvestApiRuntime::new(
            registry,
            Arc::new(DagCatalog::default()),
            Arc::new(Vec::new()),
            None,
            Vec::new(),
            SchedulerMonitor::offline(),
            HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
            ShardRouter::single(),
        );

        assert!(!runtime.is_registered_dag("workflow_only"));

        let runtime = runtime.with_registered_dag_names(["workflow_only".to_string()]);
        assert!(runtime.is_registered_dag("workflow_only"));
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
    fn parse_external_handoff_filters_accepts_all_filters() {
        let filters = parse_external_handoff_filters(&pairs(&[
            ("state", "pending,failed"),
            ("workflow_name", "billing_checkout"),
            ("execution_id", "00000000-0000-0000-0000-000000000001"),
            ("activity_name", "manager_approval"),
            ("token", "11111111-1111-4111-8111-111111111111"),
            ("shard_id", "2"),
            ("due_before", "2026-05-08T12:00:00Z"),
            ("updated_before", "2026-05-08T13:00:00Z"),
            ("limit", "9001"),
        ]))
        .expect("handoff filters should parse");

        assert_eq!(filters.states, vec!["PENDING", "FAILED"]);
        assert_eq!(filters.workflow_name.as_deref(), Some("billing_checkout"));
        assert!(filters.execution_id.is_some());
        assert_eq!(filters.activity_name.as_deref(), Some("manager_approval"));
        assert!(filters.token.is_some());
        assert_eq!(filters.shard_id, Some(2));
        assert!(filters.due_before.is_some());
        assert!(filters.updated_before.is_some());
        assert_eq!(filters.limit, MAX_EXTERNAL_HANDOFF_LIMIT);
    }

    #[test]
    fn parse_external_handoff_filters_rejects_unknown_state() {
        let err = parse_external_handoff_filters(&pairs(&[("state", "zombie")]))
            .expect_err("unknown handoff state must error");
        assert!(err.to_string().contains("unknown external handoff state"));
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
    fn backfill_prior_precheck_treats_zero_as_no_prior_run() {
        assert_eq!(
            classify_backfill_prior_count(Ok(0)),
            BackfillPriorPrecheck::NoPriorRun
        );
    }

    #[test]
    fn backfill_prior_precheck_treats_positive_count_as_existing_run() {
        assert_eq!(
            classify_backfill_prior_count(Ok(1)),
            BackfillPriorPrecheck::PriorRunExists
        );
    }

    #[test]
    fn backfill_prior_precheck_treats_count_error_as_failure() {
        let decision =
            classify_backfill_prior_count(Err(diesel::result::Error::RollbackErrorOnCommit {
                rollback_error: Box::new(diesel::result::Error::NotFound),
                commit_error: Box::new(diesel::result::Error::NotFound),
            }));

        assert!(
            matches!(decision, BackfillPriorPrecheck::PrecheckFailed(_)),
            "count errors must not be interpreted as no prior run: {decision:?}"
        );
    }

    #[test]
    fn backfill_running_count_keeps_successful_counts() {
        assert_eq!(
            classify_backfill_running_count(Ok(7)),
            BackfillRunningCountCheck::Count(7)
        );
    }

    #[test]
    fn backfill_running_count_treats_query_error_as_failure() {
        let decision =
            classify_backfill_running_count(Err(diesel::result::Error::RollbackErrorOnCommit {
                rollback_error: Box::new(diesel::result::Error::NotFound),
                commit_error: Box::new(diesel::result::Error::NotFound),
            }));

        assert!(
            matches!(decision, BackfillRunningCountCheck::CountFailed(_)),
            "running-count errors must not be used as zero before dispatch: {decision:?}"
        );
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

    // -- Drain: AC #2 -- default deadline from shutdown timeout

    #[test]
    fn harvest_api_state_shutdown_timeout_defaults_to_30s() {
        let state = HarvestApiState::new();
        assert_eq!(
            state.worker_shutdown_timeout(),
            std::time::Duration::from_secs(30)
        );
    }

    #[test]
    fn harvest_api_state_shutdown_timeout_can_be_overridden() {
        let state = HarvestApiState::new();
        state.set_worker_shutdown_timeout(std::time::Duration::from_secs(60));
        assert_eq!(
            state.worker_shutdown_timeout(),
            std::time::Duration::from_secs(60)
        );
    }

    #[test]
    fn workflow_result_wait_query_defaults_to_zero() {
        let wait =
            parse_workflow_result_wait_query(&[], std::time::Duration::from_secs(30)).unwrap();
        assert_eq!(wait, std::time::Duration::ZERO);
    }

    #[test]
    fn workflow_result_wait_query_accepts_ms_and_caps_to_configured_max() {
        let wait = parse_workflow_result_wait_query(
            &pairs(&[("wait", "250ms")]),
            std::time::Duration::from_secs(30),
        )
        .unwrap();
        assert_eq!(wait, std::time::Duration::from_millis(250));

        let wait = parse_workflow_result_wait_query(
            &pairs(&[("wait", "45s")]),
            std::time::Duration::from_secs(2),
        )
        .unwrap();
        assert_eq!(wait, std::time::Duration::from_secs(2));
    }

    #[test]
    fn workflow_result_wait_query_rejects_invalid_duration() {
        let error = parse_workflow_result_wait_query(
            &pairs(&[("wait", "forever")]),
            std::time::Duration::from_secs(30),
        )
        .expect_err("invalid wait duration must fail");

        assert!(
            error.to_string().contains("invalid wait"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn workflow_result_pending_response_is_204_with_retry_after() {
        let response = workflow_result_pending_response();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(axum::http::header::RETRY_AFTER),
            Some(&axum::http::HeaderValue::from_static("1"))
        );
    }

    #[test]
    fn workflow_result_response_returns_204_for_running_snapshot() {
        let response = workflow_result_response(WorkflowResult::running());

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn workflow_result_immediate_snapshot_does_not_require_listener_config() {
        let Some((database_url, _container)) = setup_workflow_result_database().await else {
            return;
        };
        let exec_id = ExecutionId::new();
        let mut conn =
            <diesel_async::AsyncPgConnection as diesel_async::AsyncConnection>::establish(
                &database_url,
            )
            .await
            .expect("failed to connect to workflow result database");
        autumn_harvest::start_or_load_workflow_execution(
            &mut conn,
            autumn_harvest::StartWorkflowParams {
                workflow_name: "snapshot_listenerless",
                workflow_id: "snapshot-listenerless-1",
                exec_id,
                input: serde_json::json!({ "ok": true }),
                parent_id: None,
                queue_name: "default",
                execution_timeout: None,
                memo: None,
                search_attrs: None,
                reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                trace_context: None,
                max_execution_timeout_ceiling: None,
                concurrency_key: None,
                concurrency_limit: None,
            },
        )
        .await
        .expect("workflow execution should be seeded");

        let state = HarvestApiState::new();
        state.install_storage_pool(crate::state::HarvestDbPool::single(
            workflow_result_test_pool(&database_url),
        ));

        let response = get_workflow_result(
            Extension(state),
            Path(exec_id.to_string()),
            Query(Vec::new()),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    fn workflow_result_test_pool(database_url: &str) -> autumn_harvest::worker::DbPool {
        let manager = diesel_async::pooled_connection::AsyncDieselConnectionManager::<
            diesel_async::AsyncPgConnection,
        >::new(database_url);
        deadpool::managed::Pool::builder(manager)
            .max_size(4)
            .build()
            .expect("failed to build workflow result test pool")
    }

    async fn setup_workflow_result_database() -> Option<(String, ContainerAsync<Postgres>)> {
        let container = match Postgres::default().start().await {
            Ok(container) => container,
            Err(error) => {
                eprintln!(
                    "skipping workflow result listenerless snapshot test; Postgres container unavailable: {error}"
                );
                return None;
            }
        };
        let host = container
            .get_host()
            .await
            .expect("failed to get container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("failed to get container port");
        let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        autumn_web::migrate::run_pending(&database_url, autumn_harvest::MIGRATIONS)
            .expect("failed to run Harvest migrations");
        Some((database_url, container))
    }

    #[test]
    fn parse_history_export_query_defaults_to_redacted_policy_and_size_limit() {
        let query = parse_history_export_query(&[]).expect("empty history export query");

        assert_eq!(
            query.payload_policy,
            autumn_harvest::HistoryPayloadPolicy::Redacted
        );
        assert_eq!(
            query.max_bytes,
            autumn_harvest::DEFAULT_HISTORY_EXPORT_MAX_BYTES
        );
    }

    #[test]
    fn parse_history_export_query_accepts_full_policy_and_limit() {
        let query = parse_history_export_query(&pairs(&[
            ("payload_policy", "full"),
            ("max_bytes", "1048576"),
        ]))
        .expect("full payload query should parse");

        assert_eq!(
            query.payload_policy,
            autumn_harvest::HistoryPayloadPolicy::Full
        );
        assert_eq!(query.max_bytes, 1_048_576);
    }

    #[test]
    fn parse_history_export_query_rejects_unknown_policy() {
        let error = parse_history_export_query(&pairs(&[("payload_policy", "leaky")]))
            .expect_err("unknown payload policy must fail");

        assert!(
            error.to_string().contains("unknown payload_policy"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_history_batch_export_query_maps_state_group_to_states() {
        let query = parse_history_batch_export_query(&pairs(&[
            ("workflow_name", "billing_checkout"),
            ("state_group", "terminal"),
            ("updated_after", "2026-05-01T00:00:00Z"),
            ("updated_before", "2026-05-08T00:00:00Z"),
            ("shard_id", "2"),
            ("limit", "1000"),
        ]))
        .expect("batch export query should parse");

        assert_eq!(query.workflow_name.as_deref(), Some("billing_checkout"));
        assert_eq!(query.states, terminal_workflow_states());
        assert!(query.updated_after.is_some());
        assert!(query.updated_before.is_some());
        assert_eq!(query.shard_id, Some(2));
        assert_eq!(query.limit, 1000);
    }

    fn test_history_export_candidate(
        id: uuid::Uuid,
        shard_id: i32,
        last_history_event_at: chrono::DateTime<chrono::Utc>,
    ) -> HistoryExportCandidate {
        HistoryExportCandidate {
            id,
            workflow_name: "billing_checkout".to_string(),
            shard_id,
            state: "COMPLETED".to_string(),
            last_history_event_at,
        }
    }

    #[test]
    fn sort_history_export_candidates_orders_globally_before_limit() {
        let base = chrono::DateTime::parse_from_rfc3339("2026-05-08T00:00:00Z")
            .expect("valid timestamp")
            .with_timezone(&chrono::Utc);
        let older_shard0 =
            uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000001").expect("valid uuid");
        let newer_shard0 =
            uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000002").expect("valid uuid");
        let newest_shard1 =
            uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000003").expect("valid uuid");
        let mut candidates = vec![
            test_history_export_candidate(older_shard0, 0, base),
            test_history_export_candidate(newer_shard0, 0, base + chrono::Duration::minutes(1)),
            test_history_export_candidate(newest_shard1, 1, base + chrono::Duration::minutes(2)),
        ];

        sort_history_export_candidates(&mut candidates);
        let selected = candidates
            .into_iter()
            .take(2)
            .map(|candidate| candidate.id)
            .collect::<Vec<_>>();

        assert_eq!(selected, vec![newest_shard1, newer_shard0]);
    }

    #[test]
    fn history_export_candidate_query_filters_updated_window_by_latest_event_timestamp() {
        let sql = history_export_candidates_sql();

        assert!(sql.contains("MAX(e.timestamp)"));
        assert!(sql.contains("HAVING"));
        assert!(
            !sql.contains("COALESCE(completed_at, started_at, created_at) >="),
            "updated windows must not be based only on workflow row timestamps"
        );
    }

    // ── Timezone-aware schedule API ───────────────────────────────────────────

    #[test]
    fn parse_schedule_expr_with_tz_utc_produces_cron_variant() {
        use autumn_harvest::policy::Schedule;
        let result = parse_schedule_expr_with_tz("0 9 * * 1-5", "UTC")
            .expect("valid cron+UTC should parse");
        assert!(
            matches!(result, Schedule::Cron(_)),
            "UTC timezone must produce Schedule::Cron, not CronInTimezone: {result:?}"
        );
    }

    #[test]
    fn parse_schedule_expr_with_tz_non_utc_produces_cron_in_timezone_variant() {
        use autumn_harvest::policy::Schedule;
        let result = parse_schedule_expr_with_tz("0 9 * * 1-5", "America/Los_Angeles")
            .expect("valid cron+timezone should parse");
        assert!(
            matches!(&result, Schedule::CronInTimezone { tz, .. } if tz == "America/Los_Angeles"),
            "non-UTC timezone must produce Schedule::CronInTimezone: {result:?}"
        );
    }

    #[test]
    fn parse_schedule_expr_with_tz_unknown_timezone_is_rejected() {
        let result = parse_schedule_expr_with_tz("0 9 * * *", "Not/ATimezone");
        assert!(
            result.is_err(),
            "unknown timezone must be rejected with an error, got: {result:?}"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("Not/ATimezone"),
            "error should name the bad timezone: {err}"
        );
    }

    #[test]
    fn create_workflow_schedule_request_timezone_defaults_to_utc() {
        let json = r#"{"workflow_name":"my_wf","schedule_expr":"0 9 * * *"}"#;
        let req: CreateWorkflowScheduleRequest =
            serde_json::from_str(json).expect("should deserialize");
        assert_eq!(req.timezone, "UTC", "missing timezone must default to UTC");
    }

    #[test]
    fn create_workflow_schedule_request_timezone_round_trips() {
        let json =
            r#"{"workflow_name":"my_wf","schedule_expr":"0 9 * * *","timezone":"Europe/London"}"#;
        let req: CreateWorkflowScheduleRequest =
            serde_json::from_str(json).expect("should deserialize");
        assert_eq!(req.timezone, "Europe/London");
    }

    #[test]
    fn schedule_entry_timezone_field_is_serialized() {
        let entry = ScheduleEntry {
            id: uuid::Uuid::nil(),
            kind: ScheduleKind::Workflow,
            name: "test".to_string(),
            schedule_expr: Some("cron:0 9 * * *".to_string()),
            timezone: "Asia/Tokyo".to_string(),
            is_paused: false,
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            next_run_at: None,
            last_run_at: None,
            max_active_runs: 1,
            catchup: false,
            last_backfill: None,
            jitter_secs: 0,
            effective_fire_time: None,
            overlap_policy: "skip".to_string(),
            buffered_count: 0,
            buffer_all_max: 100,
        };
        let json = serde_json::to_string(&entry).expect("serialize");
        assert!(
            json.contains("\"timezone\":\"Asia/Tokyo\""),
            "timezone field must be present in JSON: {json}"
        );
    }
}
