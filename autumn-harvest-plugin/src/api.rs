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
use axum::routing::{delete, get, patch, post, put};
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use autumn_harvest::admission_gate::db as admission_gate_db;
use autumn_harvest::admission_gate::{AdmissionGateView, GateScope};
use autumn_harvest::audit::OP_BATCH_START;
use autumn_harvest::audit::{
    self, AuditFilters, HEADER_ACTOR, HEADER_REQUEST_ID, HEADER_SOURCE, OP_BATCH_SUBMIT,
    OP_BUILD_COMPAT_DECLARE, OP_BUILD_COMPAT_REVOKE, OP_BUILD_POLICY_SET, OP_CIRCUIT_FORCE_CLOSE,
    OP_CIRCUIT_FORCE_OPEN, OP_DAG_PATCH, OP_DAG_RETRY, OP_DAG_TRIGGER, OP_DLQ_DISCARD_BULK,
    OP_DLQ_REPLAY, OP_DLQ_REPLAY_BULK, OP_EXTERNAL_ACTIVITY_COMPLETE, OP_EXTERNAL_ACTIVITY_FAIL,
    OP_GATE_CREATE, OP_GATE_LIFT, OP_RETENTION_RUN_NOW, OP_SCHEDULE_BACKFILL, OP_SCHEDULE_CREATE,
    OP_SCHEDULE_DELETE, OP_SCHEDULE_PAUSE, OP_SCHEDULE_RESUME, OP_SCHEDULE_TRIGGER,
    OP_WORKER_DRAIN, OP_WORKFLOW_CANCEL, OP_WORKFLOW_PAUSE, OP_WORKFLOW_RESET, OP_WORKFLOW_RESUME,
    OP_WORKFLOW_SIGNAL, OP_WORKFLOW_SIGNAL_WITH_START, OP_WORKFLOW_START,
    OP_WORKFLOW_UPDATE_WITH_START, SOURCE_API, STATUS_FAILED, STATUS_SUCCEEDED, TARGET_BATCH,
    TARGET_BUILD_ROUTING, TARGET_CIRCUIT, TARGET_DAG, TARGET_DEAD_LETTER, TARGET_EXTERNAL_ACTIVITY,
    TARGET_GATE, TARGET_RETENTION, TARGET_SCHEDULE, TARGET_WORKER, TARGET_WORKFLOW,
};
use autumn_harvest::batch::{
    self, BatchAction, BatchExecutorConfig, BatchFilter, BatchJobStatus, BatchJobView,
    BatchSubmission,
};
use autumn_harvest::batch_start::{
    BATCH_START_BODY_HARD_LIMIT, BatchStartConfig, BatchStartItem, BatchStartItemResult,
    BatchStartItemStatus, DEFAULT_BATCH_START_MAX_BYTES, DEFAULT_BATCH_START_MAX_ITEMS,
};
use autumn_harvest::calendar::{
    BackfillSlot, calendar_excludes_weekends, create_calendar, delete_calendar, get_calendar,
    list_calendars, load_exclusions_for_calendar, plan_backfill_with_calendar,
    preview_schedule_firings, replace_calendar_exclusions,
};
use autumn_harvest::completion_trigger::{InputMapping, TerminalState};
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::dlq;
use autumn_harvest::error::{HarvestError, HarvestResult, database_error};
use autumn_harvest::external_task;
use autumn_harvest::history_export::{
    DEFAULT_HISTORY_EXPORT_MAX_BYTES, HistoryExportDocument, HistoryExportError,
    HistoryExportRequest, HistoryPayloadPolicy, export_history,
};
use autumn_harvest::models::{
    AuditRecord, BackfillLogRow, CompletionTriggerDb, DeadLetter, HarvestCalendar, HarvestSchedule,
    NewAuditRecord, NewBackfillLogRow, NewCompletionTriggerDb, RateLimitBucket, ScheduleDecision,
    WorkflowExecution,
};
use autumn_harvest::policy::{
    Schedule, SkipPolicy, WorkflowSchedule, compute_jitter_offset, validate_jitter,
};
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
    ExecutionId, ExternalActivityToken, Priority, ShardId, UpdateId, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::workers::{
    DrainPreviewItem, DrainResponse, FleetHealth, PinnedExecutionRow, WorkerFilters, WorkerRow,
    drain_preview, fleet_health, get_worker, list_pinned_executions, list_workers,
    parse_worker_filters, request_drain,
};
use autumn_harvest::{HistoryMatch, HistoryMatcher, WorkflowEvent};
use autumn_harvest::{
    SignalWithStartOutcome, SignalWithStartParams, StartWorkflowParams, UpdateWithStartOutcome,
    UpdateWithStartParams, WorkflowHandleClient, WorkflowResult, cancel_workflow_execution,
    pause_workflow_execution, resume_workflow_execution, signal_with_start_workflow_execution,
    start_or_load_workflow_execution, update_with_start_workflow_execution,
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

        let this = Self {
            registry,
            dags,
            registered_dag_names: Arc::new(registered_dag_names),
            workflow_schedules,
            worker_id,
            queues,
            scheduler,
            retention,
            router: router.clone(),
        };
        if let Some(first_queue) = this.queues.as_slice().first()
            && let Ok(mut lock) =
                autumn_harvest::completion_trigger::GLOBAL_DEFAULT_WORKFLOW_QUEUE.write()
            && lock.is_none()
        {
            *lock = Some(first_queue.clone());
        }
        autumn_harvest::shard::install_global_router(router);
        this
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
    /// SSE keepalive comment interval (issue #324). Default 15 s.
    sse_keepalive_interval: Arc<Mutex<std::time::Duration>>,
    /// Maximum SSE event buffer depth per stream (issue #324). Default 1024.
    sse_buffer_depth: Arc<Mutex<usize>>,
    /// Maximum allowed workflow start delay (issue #322).
    /// Defaults to 365 days.
    max_workflow_start_delay: Arc<Mutex<std::time::Duration>>,
    /// Hard caps for `POST /workflows/batch_start` (issue #357).
    batch_start_max_items: Arc<Mutex<usize>>,
    batch_start_max_bytes: Arc<Mutex<u64>>,
    /// In-process snapshot of active admission gates (issue #377).
    ///
    /// Refreshed from Postgres every ≤1 s by the background gate-refresh task
    /// so cross-replica gate creates/lifts propagate within the ≤2 s p95 SLO.
    /// Loaded from Postgres before the worker pool starts accepting new work so
    /// there is no admission window between plugin boot and re-apply.
    gate_cache: Arc<autumn_harvest::AdmissionGateCache>,
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
            sse_keepalive_interval: Arc::new(Mutex::new(std::time::Duration::from_secs(15))),
            sse_buffer_depth: Arc::new(Mutex::new(1024)),
            max_workflow_start_delay: Arc::new(Mutex::new(std::time::Duration::from_secs(
                365 * 24 * 60 * 60,
            ))),
            batch_start_max_items: Arc::new(Mutex::new(DEFAULT_BATCH_START_MAX_ITEMS)),
            batch_start_max_bytes: Arc::new(Mutex::new(DEFAULT_BATCH_START_MAX_BYTES)),
            gate_cache: Arc::new(autumn_harvest::AdmissionGateCache::new()),
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

    /// Override the SSE keepalive comment interval (default 15 s, issue #324).
    ///
    /// # Panics
    ///
    /// Panics if `interval` is zero or if the internal mutex is poisoned.
    pub fn set_sse_keepalive_interval(&self, interval: std::time::Duration) {
        assert!(
            !interval.is_zero(),
            "SSE keepalive interval must be non-zero"
        );
        *self
            .sse_keepalive_interval
            .lock()
            .expect("harvest api state lock poisoned") = interval;
    }

    /// Current SSE keepalive comment interval.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn sse_keepalive_interval(&self) -> std::time::Duration {
        *self
            .sse_keepalive_interval
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Override the SSE per-stream event buffer depth (default 1024, issue #324).
    ///
    /// When the producer falls behind by more than this many events the stream
    /// closes with a slow-consumer response.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_sse_buffer_depth(&self, depth: usize) {
        *self
            .sse_buffer_depth
            .lock()
            .expect("harvest api state lock poisoned") = depth;
    }

    /// Current SSE per-stream event buffer depth.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn sse_buffer_depth(&self) -> usize {
        *self
            .sse_buffer_depth
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Set the maximum start delay allowed (issue #322).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn set_max_workflow_start_delay(&self, delay: std::time::Duration) {
        *self
            .max_workflow_start_delay
            .lock()
            .expect("harvest api state lock poisoned") = delay;
    }

    /// Maximum allowed workflow start delay.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn max_workflow_start_delay(&self) -> std::time::Duration {
        *self
            .max_workflow_start_delay
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Set the hard caps for `POST /workflows/batch_start` (issue #357).
    ///
    /// Call this during startup with values from [`BatchStartConfig`] so the
    /// management API honours operator-configured limits.
    ///
    /// # Panics
    ///
    /// Panics if an internal mutex is poisoned.
    pub fn set_batch_start_config(&self, config: &BatchStartConfig) {
        *self
            .batch_start_max_items
            .lock()
            .expect("harvest api state lock poisoned") = config.max_items_per_batch;
        *self
            .batch_start_max_bytes
            .lock()
            .expect("harvest api state lock poisoned") = config.max_total_bytes;
    }

    /// Maximum items per `POST /workflows/batch_start` request.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn batch_start_max_items(&self) -> usize {
        *self
            .batch_start_max_items
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Maximum total bytes for a `POST /workflows/batch_start` request body.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn batch_start_max_bytes(&self) -> u64 {
        *self
            .batch_start_max_bytes
            .lock()
            .expect("harvest api state lock poisoned")
    }

    /// Return a clone of the shared admission gate cache (issue #377).
    ///
    /// The cache is populated at startup and refreshed every ≤1 s.
    #[must_use]
    pub fn gate_cache(&self) -> Arc<autumn_harvest::AdmissionGateCache> {
        Arc::clone(&self.gate_cache)
    }

    /// Switch the gate cache to fail-closed mode during plugin boot.
    ///
    /// The plugin calls this immediately after `new()` so any request that
    /// arrives in the window between the HTTP server binding and the boot-time
    /// gate load (see `initialize_gate_cache`) is rejected rather than allowed
    /// through a transient DB error. Standalone routers that do not call this
    /// start with an initialized-empty (fail-open) cache.
    pub fn arm_gate_cache_fail_closed(&self) {
        self.gate_cache.set_fail_closed();
    }

    /// Pre-load the gate cache from Postgres during plugin startup.
    ///
    /// Called before the worker pool starts so there is no admission window
    /// between boot and re-apply. Also spawns the background refresh task.
    pub fn initialize_gate_cache(&self, gates: Vec<autumn_harvest::AdmissionGate>) {
        self.gate_cache.refresh(gates);
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

    /// Return the LISTEN/NOTIFY database URL for the given shard (issue #324).
    pub(crate) fn sse_notification_url(&self, shard: ShardId) -> HarvestResult<String> {
        let urls = self.workflow_result_notification_database_urls()?;
        urls.get(&shard).cloned().ok_or_else(|| {
            HarvestError::Config(format!(
                "no SSE notification URL configured for shard {shard:?}"
            ))
        })
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

/// Coarse classification of why a workflow appears stalled (issue #486).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StallReason {
    PendingActivity,
    PendingChild,
    AwaitingSignal,
    SleepingTimer,
    NoPendingWork,
}

/// A workflow execution row augmented with stall-discovery fields (issue #486).
///
/// The `#[serde(flatten)]` preserves all existing `WorkflowExecution` fields
/// at the top level of the JSON object. The three new optional fields are
/// omitted when `None` (via `skip_serializing_if`) so callers not using the
/// stalled filter receive an identical response to the pre-#486 format.
#[derive(Debug, Clone, Serialize)]
pub struct StalledWorkflowRow {
    #[serde(flatten)]
    pub execution: WorkflowExecution,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_age_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stall_reason: Option<StallReason>,
}

impl From<WorkflowExecution> for StalledWorkflowRow {
    fn from(execution: WorkflowExecution) -> Self {
        Self {
            execution,
            last_event_at: None,
            last_event_age_seconds: None,
            stall_reason: None,
        }
    }
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

pub(crate) fn is_terminal_state(state: &str) -> bool {
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
struct PauseWorkflowResponse {
    ok: bool,
    execution_id: String,
    state: String,
    reason: Option<String>,
    actor: String,
    newly_paused: bool,
}

#[derive(Debug, Serialize)]
struct ResumeWorkflowResponse {
    ok: bool,
    execution_id: String,
    state: String,
    actor: String,
    pause_duration_secs: f64,
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

/// Request body for `POST /dags/{dag_name}/runs/{run_exec_id}/retry` (issue #366).
#[derive(Debug, Deserialize)]
struct DagRetryRequest {
    /// Non-empty list of declared DAG node names to retry from.
    #[serde(default)]
    from_nodes: Vec<String>,
    /// Operator-supplied recovery reason (recorded in the audit trail and the
    /// `WorkflowResetFork` event payload).
    #[serde(default)]
    reason: String,
    /// Operator identity for audit.
    #[serde(default)]
    operator_id: String,
    /// When `true`, compute and return the plan without performing any write.
    #[serde(default)]
    dry_run: bool,
}

/// Response body for a committed or dry-run DAG retry.
#[derive(Debug, Serialize)]
struct DagRetryResponse {
    /// `true` when this was a dry run (no write performed).
    dry_run: bool,
    /// The DAG name.
    dag_name: String,
    /// The source DAG run execution id.
    source_run_exec_id: String,
    /// The resolved 0-based reset event id passed to the #148 reset primitive.
    reset_to_event_id: i64,
    /// Nodes that will (re-)execute on the new run, sorted.
    nodes_to_re_execute: Vec<String>,
    /// Nodes whose recorded results are carried over, sorted.
    nodes_carried_over: Vec<String>,
    /// The new (forked) DAG run execution id. `None` for a dry run.
    #[serde(skip_serializing_if = "Option::is_none")]
    new_run_exec_id: Option<String>,
    /// Number of carried-over events on the new run. `None` for a dry run.
    #[serde(skip_serializing_if = "Option::is_none")]
    events_carried_over: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct StartWorkflowRequest {
    workflow_id: Option<String>,
    input: Option<Value>,
    queue: Option<String>,
    memo: Option<Value>,
    search_attrs: Option<Value>,
    execution_timeout_secs: Option<i64>,
    /// Soft SLA in seconds. Emits `harvest.workflow.sla_breached` once when exceeded;
    /// never terminates the run. Falls back to `WorkflowInfo::sla` when omitted.
    sla_secs: Option<i64>,
    /// How to handle a duplicate `(workflow_name, workflow_id)` collision.
    /// Omitted or `null` → `AllowDuplicate` (preserves existing wire behaviour).
    /// An unknown string value returns `400 Bad Request` with the offending value
    /// echoed in the response body.
    reuse_policy: Option<String>,
    #[serde(default)]
    start_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    delay: Option<String>,
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

/// Request body for `POST /workflows/{workflow_name}/update-with-start` (issue #479).
#[derive(Debug, Deserialize)]
struct UpdateWithStartRequest {
    workflow_id: String,
    #[serde(default)]
    start_input: Option<Value>,
    update_name: String,
    #[serde(default)]
    update_args: Option<Value>,
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    memo: Option<Value>,
    #[serde(default)]
    search_attrs: Option<Value>,
    #[serde(default)]
    execution_timeout_secs: Option<i64>,
    /// Same wire values as `POST /workflows/.../start`.
    #[serde(default)]
    id_reuse_policy: Option<String>,
    /// Optional dedup key. Repeated calls with the same key scoped to
    /// `(workflow_name, workflow_id)` are idempotent: exactly one update is
    /// admitted, no duplicate starts are created.
    #[serde(default)]
    idempotency_key: Option<String>,
    /// `"admitted"` → return 202 with `update_id` immediately after admission.
    /// `"completed"` (default) → poll until the update handler resolves.
    #[serde(default)]
    wait_for_stage: Option<String>,
    /// Seconds to wait for the update result (default 30, `wait_for_stage = "completed"` only).
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct UpdateWithStartResponse {
    execution_id: String,
    workflow_name: String,
    workflow_id: String,
    state: String,
    started_fresh: bool,
    update_id: String,
    /// Present when `wait_for_stage = "completed"` and the update succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
}

impl UpdateWithStartResponse {
    fn from_outcome(outcome: &UpdateWithStartOutcome) -> Self {
        Self {
            execution_id: outcome.exec_id.to_string(),
            workflow_name: outcome.workflow_name.clone(),
            workflow_id: outcome.workflow_id.clone(),
            state: outcome.state.clone(),
            started_fresh: outcome.started_fresh,
            update_id: outcome.update_id.to_string(),
            result: None,
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

#[derive(Debug, Default, Deserialize)]
struct PauseWorkflowRequest {
    #[serde(default)]
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
    /// Optional named calendar attached to this schedule (issue #337). `null` = no filtering.
    calendar_name: Option<String>,
    /// What to do when the fire date is calendar-excluded (issue #337).
    skip_policy: String,
    /// Auto-pause after this many consecutive execution failures (issue #360). `null` = disabled.
    consecutive_failure_limit: Option<i32>,
    /// Current consecutive failure count (issue #360). Resets to 0 on success or resume.
    consecutive_failure_count: i32,
    /// Timestamp when the schedule was automatically paused (issue #360). `null` = not auto-paused.
    auto_paused_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Absolute UTC cutoff for this schedule (issue #478). `null` = no cutoff (fires forever).
    end_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Total run budget for this schedule (issue #478). `null` = no limit (fires forever).
    max_runs: Option<i32>,
    /// Number of executions actually started by this schedule (issue #478).
    runs_started: i32,
    /// Derived remaining run budget: `max_runs - runs_started` when `max_runs` is set,
    /// `null` when `max_runs` is `null` (issue #478).
    remaining_runs: Option<i32>,
    /// Timestamp when the schedule was exhausted (issue #478). `null` = not yet exhausted.
    exhausted_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Machine-readable exhaustion reason (issue #478). One of `"end_at_reached"` or
    /// `"max_runs_exhausted"`. `null` when not exhausted.
    exhausted_reason: Option<String>,
    /// Effective catchup policy for this schedule (issue #484).
    /// One of `"skip_all"`, `"most_recent"`, `"window"`, `"unbounded"`.
    /// Derived from `catchup_policy` column when set, falling back to the `catchup` bool.
    catchup_policy_effective: String,
    /// Window duration in seconds for the `"window"` catchup policy (issue #484).
    /// `null` for all other policies.
    catchup_window_secs: Option<i64>,
    /// Number of missed slots dropped on the most recent recovery tick (issue #484).
    /// 0 when no recovery has occurred or the policy is `skip_all`/`unbounded`.
    catchup_dropped_last_recovery: i32,
    /// Timestamp of the most recent recovery tick that produced drops (issue #484).
    /// `null` when `catchup_dropped_last_recovery` is 0.
    last_catchup_at: Option<chrono::DateTime<chrono::Utc>>,
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
    /// Optional named calendar for business-day / holiday filtering (issue #337).
    #[serde(default)]
    calendar: Option<String>,
    /// What to do when the fire date is excluded by the calendar.
    /// Valid values: `"skip"`, `"run_next_business_day"`, `"run_prev_business_day"`.
    #[serde(default = "default_skip_policy")]
    skip_policy: String,
    /// Auto-pause after this many consecutive `FAILED`/`TIMED_OUT` execution completions.
    /// `null` (the default) disables auto-pause (issue #360).
    #[serde(default)]
    consecutive_failure_limit: Option<u32>,
    /// Absolute UTC cutoff for this schedule (issue #478).
    /// When `next_run_at >= end_at` the scheduler stops firing and marks the schedule
    /// exhausted. `null` (the default) = no cutoff.
    #[serde(default)]
    end_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Total run budget for this schedule (issue #478).
    /// Once `runs_started` reaches `max_runs` the schedule is exhausted. `null` = no limit.
    #[serde(default)]
    max_runs: Option<u32>,
    /// Bounded catchup policy mode (issue #484): one of `"skip_all"`,
    /// `"most_recent"`, `"window"`, `"unbounded"`. When omitted the legacy
    /// `catchup` bool governs catchup behaviour and the policy columns are left
    /// NULL. `"window"` additionally reads `catchup_window_secs`.
    ///
    /// Like every other field on this upsert endpoint, omitting it on an update
    /// resets the stored policy to "unset" (legacy-bool semantics); send the
    /// desired mode explicitly to retain a bounded policy.
    #[serde(default)]
    catchup_policy: Option<String>,
    /// Window length in seconds for `catchup_policy = "window"` (issue #484).
    /// Ignored for every other mode. `null`/omitted defaults to `0`.
    #[serde(default)]
    catchup_window_secs: Option<i64>,
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

fn default_skip_policy() -> String {
    "skip".to_string()
}

const fn default_buffer_all_max() -> u32 {
    100
}

/// Workflow execution states that the management API recognises in `state=`
/// filters. Anything outside this list is rejected with `400 Bad Request`.
pub(crate) const KNOWN_WORKFLOW_STATES: &[&str] = &[
    "RUNNING",
    "PAUSED",
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
    pub(crate) owner: Option<String>,
    pub(crate) severity: Option<String>,
    pub(crate) failure_cause: Option<String>,
    /// Only return executions with no event progress for this many minutes (issue #486).
    pub(crate) no_progress_minutes: Option<i64>,
    /// When true, include executions whose sole pending work is a future-dated
    /// durable timer (correctly sleeping). Default false = exclude sleepers.
    pub(crate) include_sleeping: bool,
    /// When true, return only executions whose soft SLA has been breached (#487).
    pub(crate) sla_breached: bool,
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
    owner: Option<String>,
}

// ── Admission gate handlers (issue #377) ──────────────────────────────────────

/// Request body for `POST /admin/gates`.
#[derive(Debug, Deserialize)]
struct CreateGateRequest {
    /// Scope kind: `"fleet"` | `"workflow_name"` | `"queue"` | `"shard_id"` | `"owner"`
    scope_kind: String,
    /// Scope value: omit for fleet; required for all other kinds.
    scope_value: Option<String>,
    /// Required human-readable reason; included in blocked-caller errors.
    reason: String,
    /// Optional extended message for the Vantage UI.
    message: Option<String>,
    /// ISO 8601 timestamp after which the gate self-clears.
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// `GET /admin/gates` — list all active (non-lifted) gates.
async fn list_gates_handler(
    Extension(api_state): Extension<HarvestApiState>,
) -> axum::response::Response {
    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return AutumnError::internal_server_error(e).into_response(),
    };
    let mut conn = match acquire_conn(pool.default_pool()).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    match admission_gate_db::list_gates(&mut conn).await {
        Ok(rows) => {
            let views: Vec<AdmissionGateView> = rows.iter().map(AdmissionGateView::from).collect();
            (StatusCode::OK, Json(serde_json::json!({ "gates": views }))).into_response()
        }
        Err(e) => AutumnError::internal_server_error(e).into_response(),
    }
}

/// `POST /admin/gates` — create an admission gate.
#[allow(clippy::too_many_lines)]
async fn create_gate_handler(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<CreateGateRequest>,
) -> axum::response::Response {
    let (actor, source, request_id) = audit_context(&headers, &api_state);

    // Validate scope/value combination before parsing: fleet scope must not
    // carry a scope_value (a caller who accidentally sends both intends a
    // narrower scope but would create a fleet-wide gate instead).
    if body.scope_kind == "fleet" && body.scope_value.is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "scope_kind 'fleet' must not include a scope_value"
            })),
        )
            .into_response();
    }

    let Some(scope) = GateScope::from_db(&body.scope_kind, body.scope_value.as_deref()) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("unknown scope_kind '{}'", body.scope_kind)
            })),
        )
            .into_response();
    };

    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return AutumnError::internal_server_error(e).into_response(),
    };
    let mut conn = match acquire_conn(pool.default_pool()).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    let result = admission_gate_db::create_gate(
        &mut conn,
        &scope,
        &body.reason,
        body.message.as_deref(),
        &actor,
        body.expires_at,
    )
    .await;

    match result {
        Ok(gate) => {
            // Refresh the in-process cache immediately so this replica honours
            // the gate without waiting for the background refresh cycle.
            // Fail-closed if the follow-up load fails: the gate was persisted
            // but we cannot read the updated list, so block all admissions on
            // this replica rather than leaving it with a stale open snapshot.
            match admission_gate_db::load_active_gates(&mut conn).await {
                Ok(fresh) => {
                    let count = i64::try_from(fresh.len()).unwrap_or(0);
                    api_state.gate_cache().refresh(fresh);
                    if let Ok(rt) = api_state.runtime() {
                        rt.registry()
                            .telemetry()
                            .metrics
                            .record_admission_gates_active(count);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "admission gate cache refresh failed after create; entering fail-closed mode"
                    );
                    api_state.gate_cache().set_fail_closed();
                }
            }

            let gate_id_str = gate.id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_GATE_CREATE,
                target_type: TARGET_GATE,
                target_id: Some(gate_id_str.as_str()),
                route_or_command: "POST /admin/gates",
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;

            (StatusCode::CREATED, Json(AdmissionGateView::from(gate))).into_response()
        }
        Err(e) => {
            let err_str = e.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_GATE_CREATE,
                target_type: TARGET_GATE,
                target_id: None,
                route_or_command: "POST /admin/gates",
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            match e {
                autumn_harvest::error::HarvestError::Config(msg) => {
                    // Active gate cap exceeded → 429; other config errors → 400.
                    let status = if msg.contains("active gate limit") {
                        StatusCode::TOO_MANY_REQUESTS
                    } else {
                        StatusCode::BAD_REQUEST
                    };
                    (status, Json(serde_json::json!({ "error": msg }))).into_response()
                }
                other => AutumnError::internal_server_error(other).into_response(),
            }
        }
    }
}

/// `DELETE /admin/gates/{id}` — lift (soft-delete) an admission gate.
async fn lift_gate_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<uuid::Uuid>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    let (actor, source, request_id) = audit_context(&headers, &api_state);

    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return AutumnError::internal_server_error(e).into_response(),
    };
    let mut conn = match acquire_conn(pool.default_pool()).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    let id_str = id.to_string();
    match admission_gate_db::lift_gate(&mut conn, id, &actor).await {
        Ok(Some(_gate)) => {
            // Refresh the in-process cache immediately. After a successful
            // lift the gate is removed; a load failure leaves the replica
            // fail-closed (blocking) rather than using the pre-lift snapshot.
            match admission_gate_db::load_active_gates(&mut conn).await {
                Ok(fresh) => {
                    let count = i64::try_from(fresh.len()).unwrap_or(0);
                    api_state.gate_cache().refresh(fresh);
                    if let Ok(rt) = api_state.runtime() {
                        rt.registry()
                            .telemetry()
                            .metrics
                            .record_admission_gates_active(count);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "admission gate cache refresh failed after lift; entering fail-closed mode"
                    );
                    api_state.gate_cache().set_fail_closed();
                }
            }

            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_GATE_LIFT,
                target_type: TARGET_GATE,
                target_id: Some(id_str.as_str()),
                route_or_command: "DELETE /admin/gates/{id}",
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;

            (StatusCode::OK, Json(serde_json::json!({ "lifted": true }))).into_response()
        }
        // Gate not found or already lifted — return lifted:false for idempotent
        // cleanup; callers do not need to pre-check existence before lifting.
        Ok(None) => (StatusCode::OK, Json(serde_json::json!({ "lifted": false }))).into_response(),
        Err(e) => AutumnError::internal_server_error(e).into_response(),
    }
}

#[allow(clippy::too_many_lines)]
pub fn harvest_api_router(api_state: HarvestApiState) -> Router<AppState> {
    let require_admin = middleware::from_fn_with_state(api_state.clone(), require_harvest_admin);

    Router::new()
        .route("/workflows", get(list_workflows))
        // Static /workflows/batch_start must be registered before any /workflows/{param}
        // routes so axum does not capture the literal segment as a path parameter.
        // The body limit is raised to the configured max_total_bytes ceiling so
        // Axum does not reject valid large batches before the handler can check.
        .route(
            "/workflows/batch_start",
            post(batch_start_workflows)
                .route_layer(require_admin.clone())
                // Replace Axum's 2 MiB default with the absolute hard ceiling so
                // bodies are bounded before buffering while still allowing operators
                // to raise BatchStartConfig.max_total_bytes up to 100 MiB. The
                // handler enforces the configured cap via api_state.batch_start_max_bytes().
                .layer(axum::extract::DefaultBodyLimit::max(
                    usize::try_from(BATCH_START_BODY_HARD_LIMIT).unwrap_or(usize::MAX),
                )),
        )
        // issue #373: static /workflows/registered must be registered before any
        // /workflows/{param} routes so axum does not capture "registered" as a path param.
        .route("/workflows/registered", get(list_registered_workflows))
        .route(
            "/workflows/registered/{name}/schema",
            get(get_registered_workflow_schema),
        )
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
            "/workflows/{workflow_name}/update-with-start",
            post(update_with_start_workflow),
        )
        .route(
            "/workflows/{id}/cancel",
            post(cancel_workflow).route_layer(require_admin.clone()),
        )
        .route(
            "/workflows/{id}/pause",
            post(pause_workflow).route_layer(require_admin.clone()),
        )
        .route(
            "/workflows/{id}/resume",
            post(resume_workflow).route_layer(require_admin.clone()),
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
        // Auth parity with `POST /workflows/{id}/reset` — the DAG retry endpoint
        // is a DAG-scoped reset built on the same #148 internals, so it inherits
        // the reset route's posture (no separate admin layer; issue #366).
        .route(
            "/dags/{dag_name}/runs/{run_exec_id}/retry",
            post(retry_dag_run),
        )
        .route("/dags/{dag_name}/trigger", post(trigger_dag_run))
        .route("/dags/{dag_name}", patch(patch_dag))
        .route(
            "/dead-letters",
            get(list_dead_letters).route_layer(require_admin.clone()),
        )
        .route(
            "/dead-letters/aggregate",
            get(aggregate_dead_letters).route_layer(require_admin.clone()),
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
        .route("/admin/rate-limits", get(list_rate_limits))
        .route(
            "/admin/rate-limits/{key}",
            post(set_rate_limit).route_layer(require_admin.clone()),
        )
        .route("/admin/circuits", get(list_circuits))
        .route("/admin/circuits/{activity_name}", get(get_circuit))
        .route(
            "/admin/circuits/{activity_name}/force-open",
            post(force_open_circuit).route_layer(require_admin.clone()),
        )
        .route(
            "/admin/circuits/{activity_name}/force-close",
            post(force_close_circuit).route_layer(require_admin.clone()),
        )
        .route("/admin/queues/scaling", get(queues_scaling_signal))
        .route("/admin/metrics", get(prometheus_metrics))
        .route("/admin/history/exports", get(export_workflow_histories))
        .route("/admin/external-handoffs", get(list_external_handoffs))
        .route(
            "/admin/external-handoffs/{token}",
            get(get_external_handoff),
        )
        // Completion triggers management (issue #517): list & create completion triggers.
        .route("/admin/completion-triggers", get(list_completion_triggers))
        .route(
            "/admin/completion-triggers",
            post(create_completion_trigger).route_layer(require_admin.clone()),
        )
        // Schedule management (issue #91): unified list + workflow-schedule CRUD.
        // Schedule backfill (issue #177): bounded missed-run recovery.
        .route("/admin/schedules", get(list_schedules))
        .route("/admin/schedules/workflow", post(create_workflow_schedule))
        .route("/admin/schedules/{id}", get(get_schedule))
        .route("/admin/schedules/{id}/pause", post(pause_schedule))
        .route("/admin/schedules/{id}/resume", post(resume_schedule))
        .route("/admin/schedules/{id}/backfill", post(schedule_backfill))
        .route("/admin/schedules/{id}/trigger", post(trigger_schedule_now))
        .route("/admin/schedules/{id}", delete(delete_schedule))
        .route("/admin/schedules/decisions", get(list_fleet_decisions))
        .route(
            "/admin/schedules/{id}/decisions",
            get(get_schedule_decisions),
        )
        .route(
            "/admin/schedules/{id}/preview",
            get(preview_schedule_firings_handler),
        )
        .route(
            "/admin/schedules/preview",
            post(preview_candidate_schedule_handler),
        )
        // Calendar management (issue #337): named exclusion sets for business-day aware scheduling.
        .route("/calendars", get(list_calendars_handler))
        .route(
            "/calendars",
            post(create_calendar_handler).route_layer(require_admin.clone()),
        )
        .route("/calendars/{name}", get(get_calendar_handler))
        .route(
            "/calendars/{name}",
            put(update_calendar_exclusions_handler).route_layer(require_admin.clone()),
        )
        .route(
            "/calendars/{name}",
            delete(delete_calendar_handler).route_layer(require_admin.clone()),
        )
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
            post(submit_batch_operation).route_layer(require_admin.clone()),
        )
        .route("/batch-operations/{id}", get(get_batch_operation))
        // Task priority management (issue #249): PATCH /tasks/{id} lets
        // operators re-prioritize a stuck pending task without restarting the
        // workflow. Already-running tasks ignore the change; the next retry
        // attempt uses the updated priority.
        .route("/tasks/{id}", patch(patch_task_priority))
        // Audit trail (issue #158): read-only endpoint to query management
        // API mutations. See `audit::ALL_MUTATION_ROUTES` for covered paths.
        .route("/admin/audit", get(list_audit_records))
        // SSE execution event stream (issue #324): live workflow event tail.
        // Gated behind the same mgmt auth provider as other management routes.
        .route(
            "/executions/{exec_id}/events/stream",
            get(stream_execution_events).route_layer(require_admin.clone()),
        )
        // Build routing management (issue #362): expose build policies,
        // compatibility declarations, and cross-shard reachability.
        // Mutating routes are admin-gated; the read route is open to any
        // authenticated operator (same posture as GET /workers).
        .route("/admin/build-routing", get(list_build_routing_handler))
        .route(
            "/admin/build-routing/policies",
            post(set_build_policy_handler).route_layer(require_admin.clone()),
        )
        .route(
            "/admin/build-routing/compat",
            get(list_build_compat_handler),
        )
        .route(
            "/admin/build-routing/compat",
            post(declare_compat_handler).route_layer(require_admin.clone()),
        )
        .route(
            "/admin/build-routing/compat/{build_id}/{compat_with}",
            delete(revoke_compat_handler).route_layer(require_admin.clone()),
        )
        .route(
            "/admin/build-routing/retire",
            post(retire_build_handler).route_layer(require_admin.clone()),
        )
        // Admission gates (issue #377): incident-response switch to halt new
        // workflow starts fleet-wide or for a scoped subset of work.
        // GET is read-only; POST/DELETE require admin access.
        .route("/admin/gates", get(list_gates_handler))
        .route(
            "/admin/gates",
            post(create_gate_handler).route_layer(require_admin.clone()),
        )
        .route(
            "/admin/gates/{id}",
            delete(lift_gate_handler).route_layer(require_admin.clone()),
        )
        // Stuck-task triage eligibility explainer (issue #380)
        .route(
            "/admin/queues/{queue_name}/eligibility",
            get(get_queue_eligibility).route_layer(require_admin.clone()),
        )
        .route(
            "/admin/tasks/{id}/eligibility",
            get(get_task_eligibility).route_layer(require_admin),
        )
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

pub(crate) async fn has_harvest_admin_access(
    api_state: &HarvestApiState,
    session: Option<Session>,
) -> bool {
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
        ("GET", "/workflows/registered"),
        ("GET", "/workflows/registered/{name}/schema"),
        ("GET", "/workflows/{id}"),
        ("GET", "/workflows/{id}/result"),
        ("GET", "/workflows/{id}/history/export"),
        ("GET", "/workflows/{id}/children"),
        ("GET", "/workflows/{id}/stack"),
        ("POST", "/workflows/{workflow_name}/start"),
        ("POST", "/workflows/{workflow_name}/signal-with-start"),
        ("POST", "/workflows/{workflow_name}/update-with-start"),
        ("POST", "/workflows/{id}/cancel"),
        ("POST", "/workflows/{id}/pause"),
        ("POST", "/workflows/{id}/resume"),
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
        ("POST", "/dags/{dag_name}/runs/{run_exec_id}/retry"),
        ("POST", "/dags/{dag_name}/trigger"),
        ("PATCH", "/dags/{dag_name}"),
        // ── dead-letter queue ─────────────────────────────────────────────────
        ("GET", "/dead-letters"),
        ("GET", "/dead-letters/aggregate"),
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
        // ── batch workflow start (issue #357) ─────────────────────────────────
        ("POST", "/workflows/batch_start"),
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
        ("GET", "/admin/rate-limits"),
        ("POST", "/admin/rate-limits/{key}"),
        ("GET", "/admin/circuits"),
        ("GET", "/admin/circuits/{activity_name}"),
        ("POST", "/admin/circuits/{activity_name}/force-open"),
        ("POST", "/admin/circuits/{activity_name}/force-close"),
        ("GET", "/admin/queues/scaling"),
        ("GET", "/admin/metrics"),
        ("GET", "/admin/history/exports"),
        ("GET", "/admin/external-handoffs"),
        ("GET", "/admin/external-handoffs/{token}"),
        // ── completion triggers (issue #517) ──────────────────────────────────
        ("GET", "/admin/completion-triggers"),
        ("POST", "/admin/completion-triggers"),
        // ── schedules (issues #91, #177, #229) ───────────────────────────────
        ("GET", "/admin/schedules"),
        ("GET", "/admin/schedules/{id}"),
        ("POST", "/admin/schedules/workflow"),
        ("POST", "/admin/schedules/{id}/pause"),
        ("POST", "/admin/schedules/{id}/resume"),
        ("POST", "/admin/schedules/{id}/backfill"),
        ("POST", "/admin/schedules/{id}/trigger"),
        ("DELETE", "/admin/schedules/{id}"),
        ("GET", "/admin/schedules/{id}/preview"),
        ("POST", "/admin/schedules/preview"),
        // ── calendars (issue #337) ────────────────────────────────────────────
        ("GET", "/calendars"),
        ("GET", "/calendars/{name}"),
        ("POST", "/calendars"),
        ("PUT", "/calendars/{name}"),
        ("DELETE", "/calendars/{name}"),
        // ── audit (issue #158) ────────────────────────────────────────────────
        ("GET", "/admin/audit"),
        // ── SSE execution event stream (issue #324) ───────────────────────────
        ("GET", "/executions/{exec_id}/events/stream"),
        // ── build routing management (issue #362) ─────────────────────────────
        ("GET", "/admin/build-routing"),
        ("POST", "/admin/build-routing/policies"),
        ("GET", "/admin/build-routing/compat"),
        ("POST", "/admin/build-routing/compat"),
        (
            "DELETE",
            "/admin/build-routing/compat/{build_id}/{compat_with}",
        ),
        ("POST", "/admin/build-routing/retire"),
        // ── admission gates (issue #377) ──────────────────────────────────────
        ("GET", "/admin/gates"),
        ("POST", "/admin/gates"),
        ("DELETE", "/admin/gates/{id}"),
        // ── stuck-task triage eligibility (issue #380) ────────────────────────
        ("GET", "/admin/queues/{queue_name}/eligibility"),
        ("GET", "/admin/tasks/{id}/eligibility"),
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
                "sla_secs",
                "reuse_policy",
                "start_at",
                "delay",
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
        (
            "POST",
            "/workflows/{workflow_name}/update-with-start",
            Some(&[
                "workflow_id",
                "start_input",
                "update_name",
                "update_args",
                "queue",
                "memo",
                "search_attrs",
                "execution_timeout_secs",
                "id_reuse_policy",
                "idempotency_key",
                "wait_for_stage",
                "timeout_secs",
            ]),
        ),
        // ── batch workflow start (issue #357) ─────────────────────────────────
        ("POST", "/workflows/batch_start", Some(&["items", "atomic"])),
        ("POST", "/workflows/{id}/cancel", Some(&["reason"])),
        ("POST", "/workflows/{id}/pause", Some(&["reason"])),
        ("POST", "/workflows/{id}/resume", Some(&[])),
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
        (
            "POST",
            "/dags/{dag_name}/runs/{run_exec_id}/retry",
            Some(&["from_nodes", "reason", "operator_id", "dry_run"]),
        ),
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
        (
            "POST",
            "/admin/completion-triggers",
            Some(&[
                "id",
                "source_workflow_name",
                "terminal_states",
                "target_workflow_name",
                "input_mapping",
                "queue_name",
            ]),
        ),
        ("POST", "/admin/retention/run-now", Some(&[])),
        (
            "POST",
            "/admin/rate-limits/{key}",
            Some(&["refill_rate", "burst"]),
        ),
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
                "calendar",
                "skip_policy",
                "catchup_policy",
                "catchup_window_secs",
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
        (
            "POST",
            "/admin/schedules/{id}/trigger",
            Some(&["reason", "overlap_policy"]),
        ),
        ("DELETE", "/admin/schedules/{id}", Some(&[])),
        // ── calendars (issue #337) ────────────────────────────────────────────
        ("POST", "/calendars", Some(&["name", "description"])),
        ("PUT", "/calendars/{name}", Some(&["exclusion_dates"])),
        ("DELETE", "/calendars/{name}", Some(&[])),
        // ── build routing (issue #362) ────────────────────────────────────────
        (
            "POST",
            "/admin/build-routing/policies",
            Some(&["queue_name", "build_id", "deployment_name"]),
        ),
        (
            "POST",
            "/admin/build-routing/compat",
            Some(&["build_id", "compatible_with"]),
        ),
        (
            "DELETE",
            "/admin/build-routing/compat/{build_id}/{compat_with}",
            Some(&[]),
        ),
        ("POST", "/admin/build-routing/retire", Some(&["build_id"])),
        // ── schedule preview (issue #348) ─────────────────────────────────────
        (
            "POST",
            "/admin/schedules/preview",
            Some(&[
                "schedule_expr",
                "timezone",
                "catchup",
                "max_active_runs",
                "paused",
                "jitter_secs",
                "overlap_policy",
                "buffer_all_max",
                "calendar",
                "skip_policy",
                "count",
                "from",
            ]),
        ),
        // ── admission gates (issue #377) ──────────────────────────────────────
        (
            "POST",
            "/admin/gates",
            Some(&[
                "scope_kind",
                "scope_value",
                "reason",
                "message",
                "expires_at",
            ]),
        ),
        ("DELETE", "/admin/gates/{id}", Some(&[])),
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
            "/workflows/{workflow_name}/update-with-start",
            Some(&[
                "execution_id",
                "workflow_name",
                "workflow_id",
                "state",
                "started_fresh",
                "update_id",
                "result",
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
            "/workflows/{id}/pause",
            Some(&[
                "ok",
                "execution_id",
                "state",
                "reason",
                "actor",
                "newly_paused",
            ]),
        ),
        (
            "POST",
            "/workflows/{id}/resume",
            Some(&[
                "ok",
                "execution_id",
                "state",
                "actor",
                "pause_duration_secs",
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
        ("GET", "/dags", None),                 // Vec<DagSummary>
        ("GET", "/dags/{dag_name}/runs", None), // Vec<WorkflowExecution>
        (
            "POST",
            "/dags/{dag_name}/runs/{run_exec_id}/retry",
            Some(&[
                "dry_run",
                "dag_name",
                "source_run_exec_id",
                "reset_to_event_id",
                "nodes_to_re_execute",
                "nodes_carried_over",
                "new_run_exec_id",
                "events_carried_over",
            ]),
        ),
        ("POST", "/dags/{dag_name}/trigger", None), // StartWorkflowResponse
        ("PATCH", "/dags/{dag_name}", None),        // HarvestSchedule (external model)
        // ── dead-letter queue ─────────────────────────────────────────────────
        ("GET", "/dead-letters", None), // Vec<DeadLetter> (external model)
        (
            "GET",
            "/dead-letters/aggregate",
            Some(&["total", "filtered_total", "groups", "truncated"]),
        ),
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
            "/workers/{worker_id}/pinned",
            Some(&[
                "execution_id",
                "workflow_name",
                "workflow_id",
                "state",
                "queue_name",
                "started_at",
                "sticky_until",
            ]),
        ),
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
        ("GET", "/admin/rate-limits", None), // Vec<RateLimitBucket> (external model)
        ("POST", "/admin/rate-limits/{key}", Some(&["ok"])),
        ("GET", "/admin/circuits", None), // Vec<CircuitSnapshot> (external model)
        (
            "GET",
            "/admin/circuits/{activity_name}",
            Some(&[
                "activity_name",
                "state",
                "forced_open",
                "last_trip",
                "rolling_failure_count",
                "time_until_probe_secs",
                "failure_threshold",
                "window_secs",
                "cooldown_secs",
            ]),
        ),
        (
            "POST",
            "/admin/circuits/{activity_name}/force-open",
            Some(&[
                "activity_name",
                "state",
                "forced_open",
                "last_trip",
                "rolling_failure_count",
                "time_until_probe_secs",
                "failure_threshold",
                "window_secs",
                "cooldown_secs",
            ]),
        ),
        (
            "POST",
            "/admin/circuits/{activity_name}/force-close",
            Some(&[
                "activity_name",
                "state",
                "forced_open",
                "last_trip",
                "rolling_failure_count",
                "time_until_probe_secs",
                "failure_threshold",
                "window_secs",
                "cooldown_secs",
            ]),
        ),
        ("GET", "/admin/queues/scaling", None),
        ("GET", "/admin/metrics", None),
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
        // ── completion triggers ───────────────────────────────────────────────
        ("GET", "/admin/completion-triggers", None),
        (
            "POST",
            "/admin/completion-triggers",
            Some(&[
                "id",
                "source_workflow_name",
                "terminal_states",
                "target_workflow_name",
                "input_mapping",
                "queue_name",
                "created_at",
                "updated_at",
            ]),
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
                "catchup_policy_effective",
                "catchup_window_secs",
                "catchup_dropped_last_recovery",
                "last_catchup_at",
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
                "catchup_policy_effective",
                "catchup_window_secs",
                "catchup_dropped_last_recovery",
                "last_catchup_at",
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
        (
            "POST",
            "/admin/schedules/{id}/trigger",
            Some(&["execution_id", "workflow_id", "triggered_at", "outcome"]),
        ),
        ("DELETE", "/admin/schedules/{id}", Some(&["ok"])),
        (
            "GET",
            "/admin/schedules/{id}/preview",
            Some(&[
                "entries",
                "is_paused",
                "pause_reason",
                "from",
                "count_requested",
            ]),
        ),
        (
            "POST",
            "/admin/schedules/preview",
            Some(&[
                "entries",
                "is_paused",
                "pause_reason",
                "from",
                "count_requested",
            ]),
        ),
        // ── calendars (issue #337) ────────────────────────────────────────────
        ("GET", "/calendars", None), // Vec<CalendarSummary>
        (
            "GET",
            "/calendars/{name}",
            Some(&[
                "name",
                "description",
                "built_in",
                "created_at",
                "updated_at",
                "exclusion_dates",
            ]),
        ),
        (
            "POST",
            "/calendars",
            Some(&[
                "name",
                "description",
                "built_in",
                "created_at",
                "updated_at",
            ]),
        ),
        ("PUT", "/calendars/{name}", None),    // 204 No Content
        ("DELETE", "/calendars/{name}", None), // 204 No Content
        // ── audit ─────────────────────────────────────────────────────────────
        ("GET", "/admin/audit", None), // Vec<AuditRecord> (external model)
        // ── SSE execution event stream (issue #324) ───────────────────────────
        ("GET", "/executions/{exec_id}/events/stream", None), // text/event-stream
        // ── build routing (issue #362) ────────────────────────────────────────
        (
            "GET",
            "/admin/build-routing",
            Some(&[
                "policies",
                "reachability",
                "diverged_queues",
                "shard_errors",
            ]),
        ),
        (
            "POST",
            "/admin/build-routing/policies",
            Some(&[
                "id",
                "queue_name",
                "build_id",
                "deployment_name",
                "created_at",
                "updated_at",
            ]),
        ),
        (
            "GET",
            "/admin/build-routing/compat",
            Some(&["entries", "diverged_pairs", "shard_errors"]),
        ),
        (
            "POST",
            "/admin/build-routing/compat",
            Some(&["id", "build_id", "compatible_with", "declared_at"]),
        ),
        (
            "DELETE",
            "/admin/build-routing/compat/{build_id}/{compat_with}",
            Some(&["revoked"]),
        ),
        (
            "POST",
            "/admin/build-routing/retire",
            Some(&[
                "build_id",
                "safe_to_retire",
                "open_executions",
                "pending_tasks",
            ]),
        ),
        // ── admission gates (issue #377) ──────────────────────────────────────
        (
            "GET",
            "/admin/gates",
            None, // array of AdmissionGate rows
        ),
        (
            "POST",
            "/admin/gates",
            Some(&[
                "id",
                "scope_kind",
                "scope_value",
                "reason",
                "created_at",
                "expires_at",
            ]),
        ),
        ("DELETE", "/admin/gates/{id}", Some(&["lifted"])),
        (
            "GET",
            "/admin/queues/{queue_name}/eligibility",
            Some(&[
                "queue_name",
                "pending_count",
                "oldest_pending_age_secs",
                "required_build_ids",
                "eligible_workers",
                "ineligible_workers",
                "summary",
                "shards",
                "shard_errors",
            ]),
        ),
        (
            "GET",
            "/admin/tasks/{id}/eligibility",
            Some(&[
                "task_id",
                "queue_name",
                "pending_count",
                "oldest_pending_age_secs",
                "required_build_id",
                "assigned_shard",
                "concurrency_key",
                "eligible_workers",
                "ineligible_workers",
                "summary",
            ]),
        ),
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
pub(crate) fn audit_context(
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
) -> Result<Json<Vec<StalledWorkflowRow>>, AutumnError> {
    let filters = parse_workflow_filters(&pairs)?;
    let rows = if filters.no_progress_minutes.is_some() {
        load_stalled_workflows_from_shards(&api_state, &filters).await?
    } else {
        load_workflows_from_shards(&api_state, &filters)
            .await?
            .into_iter()
            .map(StalledWorkflowRow::from)
            .collect()
    };
    Ok(Json(rows))
}

/// `GET /workflows/registered` — list all registered workflow types with
/// their optional JSON Schema, description, and type information (issue #373).
///
/// Returns an array of [`RegisteredWorkflowRecord`] objects sorted by name.
/// Workflows that have not opted into schema publishing return `null` for
/// `input_schema`, `output_schema`, and `error_schema`.
async fn list_registered_workflows(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<autumn_harvest::info::RegisteredWorkflowRecord>>, AutumnError> {
    let runtime = api_state.runtime()?;
    let mut records: Vec<_> = runtime
        .registry
        .workflows
        .values()
        .map(autumn_harvest::info::RegisteredWorkflowRecord::from_info)
        .collect();
    records.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(records))
}

/// `GET /workflows/registered/{name}/schema` — return the schema record for a
/// single registered workflow type (issue #373).
///
/// Returns `200` with the [`RegisteredWorkflowRecord`] when the workflow is
/// known, or `404` when the name is not registered.
async fn get_registered_workflow_schema(
    Extension(api_state): Extension<HarvestApiState>,
    Path(name): Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let runtime = match api_state.runtime() {
        Ok(r) => r,
        Err(e) => return map_error(e).into_response(),
    };
    runtime.registry.workflows.get(&name).map_or_else(
        || {
            AutumnError::not_found_msg(format!("workflow '{name}' is not registered"))
                .into_response()
        },
        |info| {
            Json(autumn_harvest::info::RegisteredWorkflowRecord::from_info(
                info,
            ))
            .into_response()
        },
    )
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
            "owner" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.owner = Some(trimmed.to_string());
                }
            }
            "severity" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.severity = Some(trimmed.to_string());
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
            "failure_cause" => {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    filters.failure_cause = Some(trimmed.to_string());
                }
            }
            "no_progress_minutes" => {
                let parsed = value.parse::<i64>().map_err(|_| {
                    AutumnError::bad_request_msg(format!(
                        "invalid no_progress_minutes '{value}'; expected a positive integer"
                    ))
                })?;
                if parsed < 1 {
                    return Err(AutumnError::bad_request_msg(
                        "no_progress_minutes must be >= 1".to_string(),
                    ));
                }
                filters.no_progress_minutes = Some(parsed);
            }
            "include_sleeping" => {
                filters.include_sleeping = value.trim().eq_ignore_ascii_case("true");
            }
            "sla_breached" => {
                filters.sla_breached = value.trim().eq_ignore_ascii_case("true");
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
        // PAUSED is a non-terminal active state (issue #383): it must be
        // enumerated everywhere active runs are, so the `active` group includes
        // it alongside RUNNING rather than silently omitting paused executions.
        "active" => Ok(vec!["RUNNING".to_string(), "PAUSED".to_string()]),
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
        "paused" => "PAUSED",
        "failed" => "FAILED",
        "completed" => "COMPLETED",
        "cancelled" | "canceled" => "CANCELLED",
        "terminated" => "TERMINATED",
        "timedout" => "TIMED_OUT",
        "continuedasnew" => "CONTINUED_AS_NEW",
        _ => {
            return Err(AutumnError::bad_request_msg(format!(
                "unknown workflow child status '{raw}'; expected one of Running, Paused, Failed, Completed, Cancelled, Terminated, TimedOut, ContinuedAsNew"
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
        "PAUSED" => "Paused",
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

    let rate_limit_keys: Vec<String> = tasks
        .iter()
        .filter(|t| t.state == "PENDING")
        .filter_map(|t| t.rate_limit_key.as_ref())
        .cloned()
        .collect();

    let mut blocked_keys = HashSet::new();
    if !rate_limit_keys.is_empty() {
        #[derive(diesel::QueryableByName)]
        struct KeyRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            key: String,
        }
        let rows: Vec<KeyRow> = diesel::sql_query(
            "SELECT key FROM harvest_rate_limit_buckets \
             WHERE key = ANY($1) \
               AND LEAST(burst, tokens + EXTRACT(EPOCH FROM (NOW() - last_refilled_at)) * refill_rate) < 1.0"
        )
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&rate_limit_keys)
        .load(&mut conn)
        .await
        .map_err(database_error)?;
        for r in rows {
            blocked_keys.insert(r.key);
        }
    }

    let pending_activities = tasks
        .into_iter()
        .map(|t| {
            let task_status = if t.state == "PENDING" {
                if let Some(ref key) = t.rate_limit_key {
                    if blocked_keys.contains(key) {
                        format!("waiting on rate_limit_key={key}")
                    } else {
                        t.state
                    }
                } else {
                    t.state
                }
            } else {
                t.state
            };

            PendingActivity {
                activity_exec_id: t.id.to_string(),
                activity_name: t.activity_name.unwrap_or_default(),
                queue: t.queue_name,
                scheduled_at: t.scheduled_at,
                attempt: t.attempt,
                max_attempts: t.max_attempts,
                task_status,
                claimed_by_worker_id: t.worker_id,
                last_heartbeat_at: t.last_heartbeat_at,
                next_retry_at: None,
                schedule_to_start_deadline: t.schedule_to_start.map(|d| t.scheduled_at + d),
                start_to_close_deadline: t.started_at.zip(t.start_to_close).map(|(s, d)| s + d),
                heartbeat_deadline: t
                    .last_heartbeat_at
                    .zip(t.heartbeat_timeout)
                    .map(|(h, d)| h + d),
            }
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

fn parse_delay_duration(raw: &str) -> Result<std::time::Duration, AutumnError> {
    let value = raw.trim();
    if value.is_empty() {
        return Err(AutumnError::bad_request_msg(
            "invalid delay duration ''; expected milliseconds, seconds, minutes, or hours",
        ));
    }

    if let Some(ms) = value.strip_suffix("ms") {
        return parse_duration_amount(ms, "delay", std::time::Duration::from_millis);
    }
    if let Some(seconds) = value.strip_suffix('s') {
        return parse_duration_amount(seconds, "delay", std::time::Duration::from_secs);
    }
    if let Some(minutes) = value.strip_suffix('m') {
        return parse_duration_amount(minutes, "delay", |amount| {
            std::time::Duration::from_secs(amount.saturating_mul(60))
        });
    }
    if let Some(hours) = value.strip_suffix('h') {
        return parse_duration_amount(hours, "delay", |amount| {
            std::time::Duration::from_secs(amount.saturating_mul(60 * 60))
        });
    }
    if let Some(days) = value.strip_suffix('d') {
        return parse_duration_amount(days, "delay", |amount| {
            std::time::Duration::from_secs(amount.saturating_mul(24 * 60 * 60))
        });
    }

    parse_duration_amount(value, "delay", std::time::Duration::from_secs)
}

/// Resolve a workflow's declared default SLA, clamped against its declared
/// `execution_timeout` (issue #487). An SLA softer than the hard timeout could
/// never fire (the timeout kills the run first), so cap it. Returns the
/// effective chrono SLA, or `None` when the workflow declares no SLA.
pub(crate) fn clamp_info_default_sla(
    info_sla: Option<std::time::Duration>,
    info_execution_timeout: Option<std::time::Duration>,
) -> Option<chrono::Duration> {
    info_sla
        .and_then(|d| chrono::Duration::from_std(d).ok())
        .map(|sla| {
            info_execution_timeout
                .and_then(|d| chrono::Duration::from_std(d).ok())
                .map_or(sla, |hard| sla.min(hard))
        })
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
    let explicit_workflow_id = request.workflow_id.is_some();
    let workflow_id = request
        .workflow_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let queue_name = request
        .queue
        .or_else(|| runtime.queues.as_slice().first().cloned())
        .unwrap_or_else(|| "default".to_string());
    let input = request.input.unwrap_or(Value::Null);

    // Compute target shard early so the gate check can filter by shard-scoped gates.
    let shard = runtime
        .router
        .pick_for_new_workflow(&workflow_name, &workflow_id);

    // issue #377: check admission gates before touching the DB.
    {
        let wf_owner = runtime
            .registry
            .workflows
            .get(&workflow_name)
            .and_then(|i| i.owner);
        let gates =
            api_state
                .gate_cache()
                .check(&workflow_name, &queue_name, shard.as_i32(), wf_owner);
        if let Some((gate_id, reason, scope_kind)) = gates {
            // Idempotent retry bypass: if the caller supplied an explicit
            // workflow_id, check whether a non-terminal execution already
            // exists on this shard.  start_or_load_workflow_execution with
            // AllowDuplicate (or AllowDuplicateFailedOnly for live runs) would
            // return it without creating new work, so the gate must not block.
            let is_idempotent_retry = if explicit_workflow_id {
                match api_state.storage_pool() {
                    Ok(pool) => match acquire_conn(pool.pool_for(shard)).await {
                        Ok(mut pre_conn) => harvest_workflow_executions::table
                            .filter(harvest_workflow_executions::workflow_name.eq(&workflow_name))
                            .filter(harvest_workflow_executions::workflow_id.eq(&workflow_id))
                            .filter(
                                harvest_workflow_executions::state
                                    .ne_all(["CONTINUED_AS_NEW", "TERMINATED"]),
                            )
                            .select(harvest_workflow_executions::id)
                            .first::<uuid::Uuid>(&mut pre_conn)
                            .await
                            .optional()
                            .unwrap_or(None)
                            .is_some(),
                        Err(_) => false,
                    },
                    Err(_) => false,
                }
            } else {
                false
            };
            if is_idempotent_retry {
                // Existing execution found — fall through to start_or_load which
                // will return it under AllowDuplicate without inserting anything.
            } else {
                // Truncate reason to 64 *characters* (not bytes) for bounded metric
                // cardinality; char_indices avoids splitting a multi-byte code point.
                let reason_label = match reason.char_indices().nth(64) {
                    Some((idx, _)) => &reason[..idx],
                    None => &reason,
                };
                runtime
                    .registry
                    .telemetry()
                    .metrics
                    .record_admission_blocked(scope_kind, reason_label);
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
                        error_summary: Some("admission blocked by gate"),
                        shard_id: None,
                        source: &source,
                    };
                    let _ = audit::insert_audit(&mut conn, &ar).await;
                }
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": "admission blocked",
                        "gate_id": gate_id,
                        "reason": reason,
                    })),
                )
                    .into_response();
            }
        }
    }

    // issue #373: validate input against the workflow's published JSON Schema (if any).
    // Runs before the delayed-start checks so bad inputs fail fast and never
    // reach the DB or appear in harvest_dead_letters.
    if let Some(info) = runtime.registry.workflows.get(&workflow_name)
        && let Err(violations) = info.validate_input(&input)
    {
        use serde_json::json;
        #[derive(serde::Serialize)]
        struct ValidationFailure {
            error: &'static str,
            violations: Vec<autumn_harvest::info::SchemaViolation>,
        }
        let body = json!(ValidationFailure {
            error: "input validation failed",
            violations,
        });
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
                error_summary: Some("input schema validation failed"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return (axum::http::StatusCode::BAD_REQUEST, Json(body)).into_response();
    }

    // Validate delayed start parameters (issue #322)
    if request.start_at.is_some() && request.delay.is_some() {
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
                error_summary: Some("Cannot specify both start_at and delay"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return AutumnError::bad_request_msg("Cannot specify both start_at and delay")
            .into_response();
    }

    let max_delay = api_state.max_workflow_start_delay();

    let delay = if let Some(ref delay_str) = request.delay {
        let std_d = match parse_delay_duration(delay_str) {
            Ok(d) => d,
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
                        error_summary: Some("invalid delay duration"),
                        shard_id: None,
                        source: &source,
                    };
                    let _ = audit::insert_audit(&mut conn, &ar).await;
                }
                return e.into_response();
            }
        };
        if std_d > max_delay {
            let err_msg = format!(
                "Requested delay ({std_d:?}) exceeds maximum permitted delay ({max_delay:?})",
            );
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
                    error_summary: Some("delay exceeds maximum permitted delay"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return AutumnError::bad_request_msg(err_msg).into_response();
        }
        let Ok(chrono_d) = chrono::Duration::from_std(std_d) else {
            return AutumnError::bad_request_msg("delay duration overflow").into_response();
        };
        Some(chrono_d)
    } else {
        None
    };

    if let Some(sa) = request.start_at {
        let max_delay_chrono =
            chrono::Duration::from_std(max_delay).unwrap_or_else(|_| chrono::Duration::days(365));
        let max_start_at = chrono::Utc::now() + max_delay_chrono;
        if sa > max_start_at {
            let err_msg = format!(
                "Requested start_at ({sa:?}) exceeds maximum permitted delay ({max_start_at:?})",
            );
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
                    error_summary: Some("start_at exceeds maximum permitted delay"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return AutumnError::bad_request_msg(err_msg).into_response();
        }
    }

    let max_delay_chrono = chrono::Duration::from_std(max_delay).ok();

    // Issue #252: compute effective cap; enforcement happens inside
    // start_or_load_workflow_execution so duplicate-resolution (409) runs first.
    let effective_wf_cap = runtime
        .registry
        .workflows
        .get(&workflow_name)
        .and_then(|info| info.max_input_bytes)
        .map_or(runtime.registry.max_workflow_input_bytes, |per_wf| {
            per_wf.max(runtime.registry.max_workflow_input_bytes)
        });

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

    // shard computed above (before gate check); reuse it here.
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

    let (owner, runbook_url, severity, info_sla, info_execution_timeout) = runtime
        .registry
        .workflows
        .get(&workflow_name)
        .map_or((None, None, None, None, None), |info| {
            (
                info.owner,
                info.runbook_url,
                info.severity,
                info.sla,
                info.execution_timeout,
            )
        });

    // Resolve effective SLA: request override → WorkflowInfo default → None.
    // `try_seconds` avoids a panic on an out-of-range untrusted `i64`, and the
    // non-negative filter rejects negative inputs (which would breach immediately).
    // The resolved SLA is clamped against the workflow's declared
    // `execution_timeout` default so an API-started run can't get a softer SLA
    // deadline than its declared hard timeout (the core also clamps against any
    // request-supplied execution_timeout).
    let effective_sla = request
        .sla_secs
        .filter(|&secs| secs >= 0)
        .and_then(chrono::Duration::try_seconds)
        .or_else(|| info_sla.and_then(|d| chrono::Duration::from_std(d).ok()))
        .map(|sla| {
            info_execution_timeout
                .and_then(|d| chrono::Duration::from_std(d).ok())
                .map_or(sla, |hard| sla.min(hard))
        });

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
            priority: Priority::default(),
            max_workflow_input_bytes: effective_wf_cap,
            start_at: request.start_at,
            delay,
            max_workflow_start_delay: max_delay_chrono,
            owner,
            runbook_url,
            severity,
            context_headers: None,
            sla: effective_sla,
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

// ── Batch workflow start (issue #357) ────────────────────────────────────────

/// Request body for `POST /workflows/batch_start`.
#[derive(Debug, Deserialize)]
struct BatchStartRequest {
    /// Workflows to start.
    items: Vec<BatchStartItem>,
    /// `true` = all-or-nothing; `false` = best-effort per-item results.
    atomic: bool,
}

/// Response for `atomic = false`: one entry per item.
#[derive(Debug, Serialize)]
struct BatchStartResponse {
    results: Vec<BatchStartItemResult>,
}

/// Response for `atomic = true` rejection: describes which items failed.
#[derive(Debug, Serialize)]
struct BatchStartRejectedResponse {
    message: String,
    rejected: Vec<BatchStartItemResult>,
}

#[allow(clippy::too_many_lines)]
async fn batch_start_workflows(
    Extension(api_state): Extension<HarvestApiState>,
    maybe_session: Option<Extension<Session>>,
    headers: axum::http::HeaderMap,
    body_bytes: Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    if !has_harvest_admin_access(&api_state, maybe_session.map(|Extension(s)| s)).await {
        return AutumnError::unauthorized_msg("authentication required").into_response();
    }

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /workflows/batch_start";

    // ── Enforce byte-size cap ────────────────────────────────────────────────
    let max_bytes = api_state.batch_start_max_bytes();
    let body_len = u64::try_from(body_bytes.len()).unwrap_or(u64::MAX);
    if body_len > max_bytes {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "error": format!(
                    "request body ({body_len} bytes) exceeds max_total_bytes ({max_bytes} bytes)"
                )
            })),
        )
            .into_response();
    }

    // ── Parse request ────────────────────────────────────────────────────────
    let request: BatchStartRequest = match serde_json::from_slice(&body_bytes) {
        Ok(r) => r,
        Err(e) => {
            return AutumnError::bad_request_msg(format!("invalid request body: {e}"))
                .into_response();
        }
    };

    // ── Enforce item count cap ───────────────────────────────────────────────
    let max_items = api_state.batch_start_max_items();
    if request.items.len() > max_items {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "error": format!(
                    "batch contains {} items; max_items_per_batch is {max_items}",
                    request.items.len()
                )
            })),
        )
            .into_response();
    }

    let runtime = match api_state.runtime() {
        Ok(r) => r,
        Err(e) => return map_error(e).into_response(),
    };

    // ── Pre-validate all items in-memory (issue #357) ───────────────────────
    // Check: workflow name registered, no DAG collision.
    // Schema validation (issue #373) is intentionally deferred to the per-shard
    // execution loop rather than done here. batch_start always uses AllowDuplicate,
    // so any item whose (workflow_name, workflow_id) already exists is returned as-is
    // without storing or deserializing the submitted input; rejecting those items in
    // pre-validation would cause spurious 400s on idempotent retries that carry stale
    // or omitted input — the same reason payload-size checks are deferred to
    // start_or_load_workflow_execution.
    let mut pre_rejected: Vec<BatchStartItemResult> = Vec::new();
    for (idx, item) in request.items.iter().enumerate() {
        let err = if !runtime.registry.workflows.contains_key(&item.workflow_name) {
            Some(format!(
                "workflow '{}' is not registered",
                item.workflow_name
            ))
        } else if runtime.is_registered_dag(&item.workflow_name) {
            Some(format!(
                "workflow '{}' is a registered DAG; use POST /dags/{{name}}/trigger",
                item.workflow_name
            ))
        } else {
            None
        };
        if let Some(reason) = err {
            pre_rejected.push(BatchStartItemResult {
                index: idx,
                status: BatchStartItemStatus::Rejected,
                execution_id: None,
                error: Some(reason),
            });
        }
    }

    // Atomic mode: fail fast if any item is invalid.
    if request.atomic && !pre_rejected.is_empty() {
        // Audit the rejection.
        if let Ok(pool) = api_state.storage_pool()
            && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
        {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_BATCH_START,
                target_type: TARGET_BATCH,
                target_id: None,
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("atomic batch rejected: one or more items invalid"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return (
            StatusCode::CONFLICT,
            Json(BatchStartRejectedResponse {
                message: format!(
                    "{} of {} items failed validation; no executions inserted (atomic=true)",
                    pre_rejected.len(),
                    request.items.len()
                ),
                rejected: pre_rejected,
            }),
        )
            .into_response();
    }

    // ── Derive per-item parameters ───────────────────────────────────────────
    let queue_name = runtime
        .queues
        .as_slice()
        .first()
        .cloned()
        .unwrap_or_else(|| "default".to_string());
    let max_wf_input_bytes = runtime.registry.max_workflow_input_bytes;
    let max_exec_timeout_ceiling = api_state
        .max_workflow_execution_timeout()
        .map(|d| chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX));

    // ── OTel batch span — created before any DB work (issue #357 telemetry AC) ─
    // Dynamic attributes (shards_touched, rejected_count) are recorded via
    // span.record() after the loop; the span is dropped at end of scope.
    let batch_span = tracing::info_span!(
        "harvest.batch.start",
        "batch.size" = request.items.len(),
        "batch.atomic" = request.atomic,
        "batch.shards_touched" = tracing::field::Empty,
        "batch.rejected_count" = tracing::field::Empty,
    );

    let mut results: Vec<BatchStartItemResult> = Vec::with_capacity(request.items.len());

    // Pre-populate with the pre-validation rejections so indexes stay aligned.
    let pre_rejected_idxs: std::collections::HashSet<usize> =
        pre_rejected.iter().map(|r| r.index).collect();
    for r in pre_rejected {
        results.push(r);
    }

    let mut rejected_count = pre_rejected_idxs.len();

    // ── Phase 1: compute routing data, gate-check with actual shard, group by shard ──
    // Precomputing workflow_ids here ensures the same value is used for shard
    // selection and for start_or_load_workflow_execution below.
    let item_queue = runtime
        .queues
        .as_slice()
        .first()
        .map_or("default", String::as_str);
    let mut shard_groups: std::collections::BTreeMap<ShardId, Vec<(usize, String)>> =
        std::collections::BTreeMap::new();
    let mut gate_rejected: Vec<BatchStartItemResult> = Vec::new();
    for (idx, item) in request.items.iter().enumerate() {
        if pre_rejected_idxs.contains(&idx) {
            continue;
        }
        let workflow_id = item
            .workflow_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let shard = runtime
            .router
            .pick_for_new_workflow(&item.workflow_name, &workflow_id);

        // issue #377: gate check with the actual target shard.
        let item_owner = runtime
            .registry
            .workflows
            .get(&item.workflow_name)
            .and_then(|i| i.owner);
        if let Some((gate_id, gate_reason, scope_kind)) = api_state.gate_cache().check(
            &item.workflow_name,
            item_queue,
            shard.as_i32(),
            item_owner,
        ) {
            // Idempotent retry bypass: if the caller supplied an explicit
            // workflow_id, check whether an active (RUNNING/SUSPENDED) execution
            // already exists on this shard.  AllowDuplicate would return the
            // existing run without creating anything new, so the gate must not
            // block an idempotent re-attach.  The DB call is made lazily — only
            // when the gate is actually active — so there is no overhead on the
            // common path where no gate is set.
            let is_idempotent_retry = if item.workflow_id.is_some() {
                match api_state.storage_pool() {
                    Ok(pool) => match acquire_conn(pool.pool_for(shard)).await {
                        Ok(mut pre_conn) => harvest_workflow_executions::table
                            .filter(
                                harvest_workflow_executions::workflow_name.eq(&item.workflow_name),
                            )
                            .filter(harvest_workflow_executions::workflow_id.eq(&workflow_id))
                            .filter(
                                // Mirror AllowDuplicate: any row except CONTINUED_AS_NEW
                                // or TERMINATED is returned without inserting a new one.
                                harvest_workflow_executions::state
                                    .ne_all(["CONTINUED_AS_NEW", "TERMINATED"]),
                            )
                            .select(harvest_workflow_executions::id)
                            .first::<uuid::Uuid>(&mut pre_conn)
                            .await
                            .optional()
                            .unwrap_or(None)
                            .is_some(),
                        Err(_) => false,
                    },
                    Err(_) => false,
                }
            } else {
                false
            };
            if is_idempotent_retry {
                // Active execution found — let Phase 2 return it via AllowDuplicate.
                shard_groups
                    .entry(shard)
                    .or_default()
                    .push((idx, workflow_id));
                continue;
            }
            let reason_label = match gate_reason.char_indices().nth(64) {
                Some((idx2, _)) => &gate_reason[..idx2],
                None => &gate_reason,
            };
            runtime
                .registry
                .telemetry()
                .metrics
                .record_admission_blocked(scope_kind, reason_label);
            gate_rejected.push(BatchStartItemResult {
                index: idx,
                status: BatchStartItemStatus::Rejected,
                execution_id: None,
                error: Some(format!(
                    "admission blocked by gate {gate_id}: {gate_reason}"
                )),
            });
            continue;
        }

        shard_groups
            .entry(shard)
            .or_default()
            .push((idx, workflow_id));
    }

    // Atomic mode: if any item was gate-rejected, fail the whole batch.
    if request.atomic && !gate_rejected.is_empty() {
        if let Ok(pool) = api_state.storage_pool()
            && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
        {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_BATCH_START,
                target_type: TARGET_BATCH,
                target_id: None,
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("atomic batch rejected: one or more items blocked by gate"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return (
            StatusCode::CONFLICT,
            Json(BatchStartRejectedResponse {
                message: format!(
                    "{} of {} items blocked by admission gate; no executions inserted (atomic=true)",
                    gate_rejected.len(),
                    request.items.len()
                ),
                rejected: gate_rejected,
            }),
        )
            .into_response();
    }

    // Non-atomic: add gate rejections to the result list.
    for r in gate_rejected {
        results.push(r);
        rejected_count += 1;
    }

    // ── Phase 2: per-shard, acquire ONE connection and process all items ──────
    // O(shards) connection acquisitions instead of O(items).
    for (shard, shard_items) in &shard_groups {
        let conn_result = db_conn_for_shard(&api_state, *shard).await;
        let mut conn = match conn_result {
            Ok(c) => c,
            Err(e) => {
                // All items on this shard are rejected due to connection failure.
                let err_str = e.to_string();
                for (idx, _) in shard_items {
                    rejected_count += 1;
                    results.push(BatchStartItemResult {
                        index: *idx,
                        status: BatchStartItemStatus::Rejected,
                        execution_id: None,
                        error: Some(err_str.clone()),
                    });
                }
                if request.atomic {
                    let () = audit_batch_start_failure(
                        &api_state,
                        &actor,
                        &source,
                        request_id.as_deref(),
                        route,
                        "atomic batch rejected: shard connection failure",
                    )
                    .await;
                    results.sort_by_key(|r| r.index);
                    return (
                        StatusCode::CONFLICT,
                        Json(BatchStartRejectedResponse {
                            message: "atomic batch aborted: shard connection failure".to_string(),
                            rejected: results,
                        }),
                    )
                        .into_response();
                }
                continue;
            }
        };

        for (idx, workflow_id) in shard_items {
            let item = &request.items[*idx];
            let input = item.input.clone().unwrap_or(Value::Null);

            // Workflow-specific cap takes precedence; global cap is the fallback.
            let effective_wf_cap = runtime
                .registry
                .workflows
                .get(&item.workflow_name)
                .and_then(|info| info.max_input_bytes)
                .map_or(max_wf_input_bytes, |per_wf| {
                    // Per-workflow limits only raise the global floor; consistent
                    // with single-start and signal-with-start paths.
                    per_wf.max(max_wf_input_bytes)
                });

            let (concurrency_key, concurrency_limit) = runtime
                .registry
                .workflows
                .get(&item.workflow_name)
                .and_then(|info| info.concurrency.as_ref())
                .map_or((None, None), |policy| {
                    let key = autumn_harvest::concurrency::resolve_concurrency_key(
                        policy.key_expr,
                        &input,
                    );
                    (key, Some(policy.limit))
                });

            let exec_id = ExecutionId::new_for_shard(*shard);

            let trace_ctx = tracing::info_span!(
                "harvest.workflow.schedule",
                "otel.kind" = "producer",
                { ATTR_WORKFLOW_ID } = %item.workflow_name,
                { ATTR_EXECUTION_ID } = %exec_id,
                { ATTR_SHARD_ID } = i64::from(shard.as_i32()),
                { ATTR_QUEUE } = %queue_name,
            )
            .in_scope(|| runtime.registry.telemetry().capture_trace_context());

            let (owner, runbook_url, severity, info_sla, info_execution_timeout) = runtime
                .registry
                .workflows
                .get(&item.workflow_name)
                .map_or((None, None, None, None, None), |info| {
                    (
                        info.owner,
                        info.runbook_url,
                        info.severity,
                        info.sla,
                        info.execution_timeout,
                    )
                });
            let sla = clamp_info_default_sla(info_sla, info_execution_timeout);

            let start_result = start_or_load_workflow_execution(
                &mut conn,
                StartWorkflowParams {
                    workflow_name: &item.workflow_name,
                    workflow_id,
                    exec_id,
                    input,
                    parent_id: None,
                    queue_name: &queue_name,
                    execution_timeout: None,
                    memo: None,
                    search_attrs: item.search_attributes.clone(),
                    reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                    trace_context: trace_ctx,
                    max_execution_timeout_ceiling: max_exec_timeout_ceiling,
                    concurrency_key,
                    concurrency_limit,
                    priority: Priority::default(),
                    max_workflow_input_bytes: effective_wf_cap,
                    start_at: None,
                    delay: None,
                    max_workflow_start_delay: None,
                    owner,
                    runbook_url,
                    severity,
                    context_headers: None,
                    sla,
                },
            )
            .await;

            match start_result {
                Ok(started) => {
                    // `harvest.workflow.started` is emitted by the worker on first
                    // claim (attempt == 1, no scheduling events). Emitting it here
                    // too would double-count every batch-started workflow vs. those
                    // started via the single-start endpoint. Do NOT call
                    // record_workflow_started here.
                    //
                    // If AllowDuplicate returned an existing execution, report it as
                    // rejected so callers know no new execution was inserted.
                    if started.created {
                        results.push(BatchStartItemResult {
                            index: *idx,
                            status: BatchStartItemStatus::Started,
                            execution_id: Some(started.exec_id.to_string()),
                            error: None,
                        });
                    } else {
                        rejected_count += 1;
                        results.push(BatchStartItemResult {
                            index: *idx,
                            status: BatchStartItemStatus::Rejected,
                            execution_id: Some(started.exec_id.to_string()),
                            error: Some(format!(
                                "workflow_id '{workflow_id}' already has an existing execution"
                            )),
                        });
                        if request.atomic {
                            let () = audit_batch_start_failure(
                                &api_state,
                                &actor,
                                &source,
                                request_id.as_deref(),
                                route,
                                "atomic batch rejected: duplicate workflow_id",
                            )
                            .await;
                            results.sort_by_key(|r| r.index);
                            return (
                                StatusCode::CONFLICT,
                                Json(BatchStartRejectedResponse {
                                    message: format!(
                                        "atomic batch aborted: item {idx} has an existing \
                                         execution for workflow_id '{workflow_id}'"
                                    ),
                                    rejected: results,
                                }),
                            )
                                .into_response();
                        }
                    }
                }
                Err(e) => {
                    rejected_count += 1;
                    let err_str = e.to_string();
                    results.push(BatchStartItemResult {
                        index: *idx,
                        status: BatchStartItemStatus::Rejected,
                        execution_id: None,
                        error: Some(err_str.clone()),
                    });
                    if request.atomic {
                        // Return all results so the client knows which items
                        // started before the failure (broken-atomicity visibility).
                        let () = audit_batch_start_failure(
                            &api_state,
                            &actor,
                            &source,
                            request_id.as_deref(),
                            route,
                            "atomic batch rejected: start failure",
                        )
                        .await;
                        results.sort_by_key(|r| r.index);
                        return (
                            StatusCode::CONFLICT,
                            Json(BatchStartRejectedResponse {
                                message: format!(
                                    "atomic batch aborted: item {idx} failed: {err_str}"
                                ),
                                rejected: results,
                            }),
                        )
                            .into_response();
                    }
                }
            }
        }
    }

    // Record dynamic attributes now that we have the final counts.
    batch_span.record("batch.shards_touched", shard_groups.len());
    batch_span.record("batch.rejected_count", rejected_count);

    // ── Audit the overall batch start ────────────────────────────────────────
    let started_count = results
        .iter()
        .filter(|r| r.status == BatchStartItemStatus::Started)
        .count();
    let audit_status = if rejected_count == 0 {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    let error_summary = if rejected_count > 0 {
        Some(
            format!("{rejected_count} of {} items rejected", request.items.len())
                .as_str()
                .to_owned(),
        )
    } else {
        None
    };
    if let Ok(pool) = api_state.storage_pool()
        && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
    {
        let error_summary_str = error_summary.as_deref();
        let ar = NewAuditRecord {
            actor: &actor,
            operation: OP_BATCH_START,
            target_type: TARGET_BATCH,
            target_id: None,
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: audit_status,
            error_summary: error_summary_str,
            shard_id: None,
            source: &source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
    }

    tracing::debug!(
        started = started_count,
        rejected = rejected_count,
        atomic = request.atomic,
        "batch_start complete"
    );

    // Sort results by index for deterministic output.
    results.sort_by_key(|r| r.index);
    (StatusCode::OK, Json(BatchStartResponse { results })).into_response()
}

/// Helper: write a failed audit record for an atomic batch-start rejection.
async fn audit_batch_start_failure(
    api_state: &HarvestApiState,
    actor: &str,
    source: &str,
    request_id: Option<&str>,
    route: &str,
    error_summary: &str,
) {
    if let Ok(pool) = api_state.storage_pool()
        && let Ok(mut conn) = acquire_conn(pool.default_pool()).await
    {
        let ar = NewAuditRecord {
            actor,
            operation: OP_BATCH_START,
            target_type: TARGET_BATCH,
            target_id: None,
            route_or_command: route,
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

    // Issue #252: resolve cap values. Both caps are enforced inside
    // signal_with_start_workflow_execution — after idempotency dedupe and
    // only on the fresh-start path for start_input — to avoid spurious 413s
    // on attach requests where start_input is never written.
    let effective_wf_cap = runtime
        .registry
        .workflows
        .get(&workflow_name)
        .and_then(|info| info.max_input_bytes)
        .map_or(runtime.registry.max_workflow_input_bytes, |per_wf| {
            per_wf.max(runtime.registry.max_workflow_input_bytes)
        });

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
    // The 4th element records whether this is a pure attach (existing RUNNING/SUSPENDED +
    // AllowDuplicate*). Used below to skip start_input schema validation on attach requests
    // where start_input is never written (mirrors the payload-cap deferral from issue #252).
    let mut found_shard: Option<(ShardId, PoolConn, ExecutionId, bool)> = None;
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
            // Only a RUNNING execution under a non-rejecting policy is a true
            // attach (signal delivered, start_input ignored). SUSPENDED is
            // upgraded to TerminateIfRunning by resolve_effective_signal_with_start_policy,
            // which writes a fresh execution using start_input — so validation must run.
            let will_attach = existing_state == "RUNNING"
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
            found_shard = Some((candidate_shard, shard_conn, exec_id, will_attach));
            break;
        }
    }

    let (shard, mut conn, exec_id, _will_attach) = if let Some(tuple) = found_shard {
        tuple
    } else {
        let shard = runtime
            .router
            .pick_for_new_workflow(&workflow_name, &workflow_id);
        let conn = match db_conn_for_shard(&api_state, shard).await {
            Ok(c) => c,
            Err(e) => return e.into_response(),
        };
        (shard, conn, ExecutionId::new_for_shard(shard), false)
    };

    // issue #377: check admission gates unconditionally.
    // Although `will_attach = true` means we observed an existing RUNNING execution,
    // that pre-scan is unlocked — the resolver can still start a fresh execution if
    // the RUNNING run completes before the FOR UPDATE lock is taken. Always checking
    // the gate closes that TOCTOU window at the cost of also blocking signals to
    // existing runs during an incident (use POST /workflows/{id}/signal instead).
    {
        let wf_owner = runtime
            .registry
            .workflows
            .get(&workflow_name)
            .and_then(|i| i.owner);
        let gate_hit =
            api_state
                .gate_cache()
                .check(&workflow_name, &queue_name, shard.as_i32(), wf_owner);
        if let Some((gate_id, reason, scope_kind)) = gate_hit {
            let reason_label = match reason.char_indices().nth(64) {
                Some((idx, _)) => &reason[..idx],
                None => &reason,
            };
            runtime
                .registry
                .telemetry()
                .metrics
                .record_admission_blocked(scope_kind, reason_label);
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_WORKFLOW_SIGNAL_WITH_START,
                    target_type: TARGET_WORKFLOW,
                    target_id: Some(workflow_name.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("admission blocked by gate"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "admission blocked",
                    "gate_id": gate_id,
                    "reason": reason,
                })),
            )
                .into_response();
        }
    }

    // issue #373: validate start_input against the workflow's published JSON Schema (if any).
    // Always runs unconditionally: the pre-scan is unlocked, so if the observed RUNNING
    // execution completes before the core resolver takes its FOR UPDATE lock, the core path
    // can escalate AllowDuplicate to a fresh start and write start_input without validation.
    // Validating unconditionally closes this TOCTOU window.
    if let Some(info) = runtime.registry.workflows.get(&workflow_name)
        && let Err(violations) = info.validate_input(&start_input)
    {
        let ar = NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_SIGNAL_WITH_START,
            target_type: TARGET_WORKFLOW,
            target_id: None,
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: request.idempotency_key.as_deref(),
            status: STATUS_FAILED,
            error_summary: Some("input validation failed"),
            shard_id: Some(shard.as_i32()),
            source: &source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({
                "error": "input validation failed",
                "violations": violations,
            })),
        )
            .into_response();
    }

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

    let (owner, runbook_url, severity, info_sla, info_execution_timeout) = runtime
        .registry
        .workflows
        .get(&workflow_name)
        .map_or((None, None, None, None, None), |info| {
            (
                info.owner,
                info.runbook_url,
                info.severity,
                info.sla,
                info.execution_timeout,
            )
        });
    let sla = clamp_info_default_sla(info_sla, info_execution_timeout);

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
            max_workflow_input_bytes: effective_wf_cap,
            max_signal_payload_bytes: runtime.registry.max_signal_payload_bytes,
            owner,
            runbook_url,
            severity,
            context_headers: None,
            sla,
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

// ── update-with-start (issue #479) ───────────────────────────────────────────

/// `POST /workflows/{workflow_name}/update-with-start`
///
/// Atomically starts a workflow if no live run for `(workflow_name, workflow_id)`
/// exists (subject to `id_reuse_policy`), or attaches to the existing run, and
/// admits exactly one update against the resolved execution.
///
/// Mirrors the `signal-with-start` contract: same reuse-policy × prior-state
/// matrix, same shard-routing and idempotency semantics.
#[allow(clippy::too_many_lines)]
async fn update_with_start_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(workflow_name): Path<String>,
    maybe_session: Option<Extension<Session>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<UpdateWithStartRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let route = "POST /workflows/{workflow_name}/update-with-start";

    // `terminate_if_running` requires admin access (same gate as signal_with_start).
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
                operation: OP_WORKFLOW_UPDATE_WITH_START,
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
                operation: OP_WORKFLOW_UPDATE_WITH_START,
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
                operation: OP_WORKFLOW_UPDATE_WITH_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(workflow_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: request.idempotency_key.as_deref(),
                status: STATUS_FAILED,
                error_summary: Some(
                    "registered DAG cannot receive update-with-start via workflow route",
                ),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        return AutumnError::bad_request_msg(format!(
            "workflow '{workflow_name}' is a registered DAG; update-with-start applies to plain workflows"
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
                    operation: OP_WORKFLOW_UPDATE_WITH_START,
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
    let update_args = request.update_args.unwrap_or(Value::Null);

    let effective_wf_cap = runtime
        .registry
        .workflows
        .get(&workflow_name)
        .and_then(|info| info.max_input_bytes)
        .map_or(runtime.registry.max_workflow_input_bytes, |per_wf| {
            per_wf.max(runtime.registry.max_workflow_input_bytes)
        });

    // Multi-shard scan: find any existing non-terminal execution for
    // (workflow_name, workflow_id) to determine the target shard and exec_id.
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
            // Reuse the execution UUID only when attaching to a live RUNNING or
            // SUSPENDED run under a non-rejecting policy. All other paths (terminal
            // prior, PAUSED, TerminateIfRunning) go through replace_execution and
            // need a fresh exec_id to avoid a primary-key conflict.
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

    // Admission gate check (unconditional — same rationale as signal-with-start).
    {
        let wf_owner = runtime
            .registry
            .workflows
            .get(&workflow_name)
            .and_then(|i| i.owner);
        let gate_hit =
            api_state
                .gate_cache()
                .check(&workflow_name, &queue_name, shard.as_i32(), wf_owner);
        if let Some((gate_id, reason, scope_kind)) = gate_hit {
            let reason_label = match reason.char_indices().nth(64) {
                Some((idx, _)) => &reason[..idx],
                None => &reason,
            };
            runtime
                .registry
                .telemetry()
                .metrics
                .record_admission_blocked(scope_kind, reason_label);
            {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_WORKFLOW_UPDATE_WITH_START,
                    target_type: TARGET_WORKFLOW,
                    target_id: Some(workflow_name.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("admission blocked by gate"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "admission blocked",
                    "gate_id": gate_id,
                    "reason": reason,
                })),
            )
                .into_response();
        }
    }

    // Validate start_input against the workflow's published JSON Schema (if any).
    if let Some(info) = runtime.registry.workflows.get(&workflow_name)
        && let Err(violations) = info.validate_input(&start_input)
    {
        let ar = NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_UPDATE_WITH_START,
            target_type: TARGET_WORKFLOW,
            target_id: None,
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: request.idempotency_key.as_deref(),
            status: STATUS_FAILED,
            error_summary: Some("input validation failed"),
            shard_id: Some(shard.as_i32()),
            source: &source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({
                "error": "input validation failed",
                "violations": violations,
            })),
        )
            .into_response();
    }

    // Derive update_id — deterministic from idempotency_key if provided.
    let update_id = request
        .idempotency_key
        .as_ref()
        .map_or_else(UpdateId::new, |key| {
            let namespace = uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .expect("static namespace UUID is valid");
            UpdateId::from_uuid(uuid::Uuid::new_v5(&namespace, key.as_bytes()))
        });

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

    let (owner, runbook_url, severity, info_sla, info_execution_timeout) = runtime
        .registry
        .workflows
        .get(&workflow_name)
        .map_or((None, None, None, None, None), |info| {
            (
                info.owner,
                info.runbook_url,
                info.severity,
                info.sla,
                info.execution_timeout,
            )
        });
    let sla = clamp_info_default_sla(info_sla, info_execution_timeout);

    let params = UpdateWithStartParams {
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
        update_id,
        update_name: request.update_name.clone(),
        update_args,
        idempotency_key: request.idempotency_key.clone(),
        max_workflow_input_bytes: effective_wf_cap,
        owner,
        runbook_url,
        severity,
        context_headers: None,
        sla,
    };

    let result = update_with_start_workflow_execution(&mut conn, params).await;

    match result {
        Err(HarvestError::AlreadyExists {
            existing_exec_id,
            existing_state,
        }) => {
            let exec_id_str = existing_exec_id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_UPDATE_WITH_START,
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
        Err(HarvestError::WorkflowPaused(paused_exec_id)) => {
            let exec_id_str = paused_exec_id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_UPDATE_WITH_START,
                target_type: TARGET_WORKFLOW,
                target_id: Some(exec_id_str.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: request.idempotency_key.as_deref(),
                status: STATUS_FAILED,
                error_summary: Some("workflow is paused"),
                shard_id: Some(shard.as_i32()),
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            (
                axum::http::StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "workflow is paused",
                    "execution_id": paused_exec_id.to_string(),
                })),
            )
                .into_response()
        }
        Err(e) => {
            let err_str = e.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_UPDATE_WITH_START,
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

            // If the caller wants to wait for the update result, poll history.
            let wait_for_stage = request.wait_for_stage.as_deref().unwrap_or("completed");
            let timeout_secs = request.timeout_secs.unwrap_or(30);

            // An idempotent retry (!update_admitted) should still poll when
            // wait_for_stage = "completed" — the update was previously admitted
            // with the same update_id and may already have a result in history.
            if wait_for_stage == "admitted" {
                // Return immediately with the update_id for the caller to poll.
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_WORKFLOW_UPDATE_WITH_START,
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
                        "audit insert failed for workflow.update_with_start"
                    );
                    return AutumnError::service_unavailable_msg(format!(
                        "audit insert failed: {audit_err}"
                    ))
                    .into_response();
                }
                return (
                    status_code,
                    Json(UpdateWithStartResponse::from_outcome(&outcome)),
                )
                    .into_response();
            }

            // Audit before the long poll to record the admission decision.
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_WORKFLOW_UPDATE_WITH_START,
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
                    "audit insert failed for workflow.update_with_start"
                );
                return AutumnError::service_unavailable_msg(format!(
                    "audit insert failed: {audit_err}"
                ))
                .into_response();
            }

            // Poll for the update result, then embed it in the response.
            let poll_response =
                poll_update_result(&pool, outcome.exec_id, outcome.update_id, timeout_secs).await;

            // Re-build response combining outcome + poll result.
            let mut base = UpdateWithStartResponse::from_outcome(&outcome);
            // Extract result from the poll response body when completed.
            let poll_resp = poll_response.into_parts();
            if poll_resp.0.status == axum::http::StatusCode::OK {
                // Decode the body to extract the output.
                if let Ok(body_bytes) = axum::body::to_bytes(poll_resp.1, usize::MAX).await
                    && let Ok(val) = serde_json::from_slice::<Value>(&body_bytes)
                {
                    base.result = val.get("output").cloned();
                }
                (status_code, Json(base)).into_response()
            } else {
                // Update failed or timed out — return a structured error body that
                // still carries execution_id and update_id so callers can retry or
                // inspect history without losing the context from the admitted update.
                let poll_status = poll_resp.0.status;
                let error_msg = axum::body::to_bytes(poll_resp.1, usize::MAX)
                    .await
                    .map_or_else(
                        |_| "update did not complete".to_string(),
                        |bytes| {
                            serde_json::from_slice::<Value>(&bytes)
                                .ok()
                                .and_then(|v| {
                                    v.get("error").and_then(|e| e.as_str()).map(String::from)
                                })
                                .unwrap_or_else(|| "update did not complete".to_string())
                        },
                    );
                let mut resp_body = UpdateWithStartResponse::from_outcome(&outcome);
                resp_body.result = Some(serde_json::json!({ "error": error_msg }));
                (poll_status, Json(resp_body)).into_response()
            }
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

    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder + Send + Sync> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry.telemetry().metrics),
        );
    let cancel_result =
        cancel_workflow_execution(&mut conn, exec_id, reason, metrics_ref.as_ref()).await;
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

/// Build a `409 Conflict` response from a state-conflict error (issue #383).
fn conflict_from(error: HarvestError) -> AutumnError {
    match error {
        // Only a genuine state conflict (e.g. "already terminal" / "not paused"),
        // surfaced by the core as `Config`, maps to 409. Everything else —
        // NotFound (404), Database (500), etc. — flows through the normal mapper
        // so a real persistence failure is not masked as a state conflict.
        HarvestError::Config(msg) => {
            AutumnError::bad_request_msg(msg).with_status(axum::http::StatusCode::CONFLICT)
        }
        other => map_error(other),
    }
}

/// `POST /workflows/{id}/pause` — halt new command dispatch for an execution
/// (issue #383). Returns 200 on success, 409 if the workflow is already
/// terminal, 404 if not found, 400 if the reason exceeds the length cap.
async fn pause_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
    // The request body is optional (issue #383): pausing without a reason is the
    // common case, so a no-body / no-content-type POST must still pause rather
    // than be rejected by the required-`Json` extractor before reaching the
    // defaulted `reason`.
    request: Option<Json<PauseWorkflowRequest>>,
) -> Result<(axum::http::StatusCode, Json<PauseWorkflowResponse>), AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /workflows/{id}/pause";
    let request = request.map(|Json(body)| body).unwrap_or_default();

    let exec_id = parse_execution_id(&id)?;
    // Reject an over-long reason at the boundary (400) so the only conflicts the
    // core can return below are genuine state conflicts (mapped to 409).
    if let Some(reason) = request.reason.as_deref()
        && reason.chars().count() > autumn_harvest::execution::MAX_PAUSE_REASON_LEN
    {
        return Err(AutumnError::bad_request_msg(format!(
            "pause reason exceeds {} characters",
            autumn_harvest::execution::MAX_PAUSE_REASON_LEN
        )));
    }

    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let exec_id_str = exec_id.to_string();
    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder + Send + Sync> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry.telemetry().metrics),
        );

    let result = pause_workflow_execution(
        &mut conn,
        exec_id,
        request.reason.as_deref(),
        &actor,
        metrics_ref.as_ref(),
    )
    .await;

    let (status, error_summary) = match &result {
        Ok(_) => (STATUS_SUCCEEDED, None),
        Err(e) => (STATUS_FAILED, Some(e.to_string())),
    };
    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_WORKFLOW_PAUSE,
        target_type: TARGET_WORKFLOW,
        target_id: Some(exec_id_str.as_str()),
        route_or_command: route,
        request_id: request_id.as_deref(),
        idempotency_key: None,
        status,
        error_summary: error_summary.as_deref(),
        shard_id: None,
        source: &source,
    };
    let _ = audit::insert_audit(&mut conn, &ar).await;

    match result {
        Ok(paused) => Ok((
            axum::http::StatusCode::OK,
            Json(PauseWorkflowResponse {
                ok: true,
                execution_id: paused.exec_id.to_string(),
                state: paused.state,
                reason: paused.reason,
                actor: paused.actor,
                newly_paused: paused.newly_paused,
            }),
        )),
        Err(e) => Err(conflict_from(e)),
    }
}

/// `POST /workflows/{id}/resume` — re-arm a paused execution (issue #383).
/// Returns 200 on success, 409 if the workflow is not in the `Paused` state,
/// 404 if not found.
async fn resume_workflow(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<(axum::http::StatusCode, Json<ResumeWorkflowResponse>), AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /workflows/{id}/resume";

    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let exec_id_str = exec_id.to_string();
    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder + Send + Sync> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry.telemetry().metrics),
        );

    let result = resume_workflow_execution(&mut conn, exec_id, &actor, metrics_ref.as_ref()).await;

    let (status, error_summary) = match &result {
        Ok(_) => (STATUS_SUCCEEDED, None),
        Err(e) => (STATUS_FAILED, Some(e.to_string())),
    };
    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_WORKFLOW_RESUME,
        target_type: TARGET_WORKFLOW,
        target_id: Some(exec_id_str.as_str()),
        route_or_command: route,
        request_id: request_id.as_deref(),
        idempotency_key: None,
        status,
        error_summary: error_summary.as_deref(),
        shard_id: None,
        source: &source,
    };
    let _ = audit::insert_audit(&mut conn, &ar).await;

    match result {
        Ok(resumed) => Ok((
            axum::http::StatusCode::OK,
            Json(ResumeWorkflowResponse {
                ok: true,
                execution_id: resumed.exec_id.to_string(),
                state: resumed.state,
                actor: resumed.actor,
                pause_duration_secs: resumed.pause_duration_secs,
            }),
        )),
        Err(e) => Err(conflict_from(e)),
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

// ── DAG retry-from-failed-node (issue #366) ─────────────────────────────────

/// Body returned when a DAG-retry node request fails validation (`400`).
#[derive(Debug, Serialize)]
struct DagRetryNodeErrorResponse {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    unknown_nodes: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    declared_nodes: Option<Vec<String>>,
}

/// Body returned when the resolved reset boundary is mid-side-effect (`409`).
#[derive(Debug, Serialize)]
struct DagRetryConflictResponse {
    message: String,
    reset_to_event_id: i64,
    nearest_valid_before: Option<i64>,
    nearest_valid_after: Option<i64>,
    remediation: String,
}

fn dag_retry_resolve_error_response(
    error: crate::dag_retry::DagRetryResolveError,
) -> axum::response::Response {
    use crate::dag_retry::DagRetryResolveError as E;
    use axum::response::IntoResponse as _;

    let body = match error {
        E::EmptyFromNodes => DagRetryNodeErrorResponse {
            message: "from_nodes must be a non-empty array of declared DAG node names".to_string(),
            unknown_nodes: None,
            declared_nodes: None,
        },
        E::UnknownNodes { unknown, declared } => DagRetryNodeErrorResponse {
            message: format!("unknown node(s) {unknown:?}; the DAG declares: {declared:?}"),
            unknown_nodes: Some(unknown),
            declared_nodes: Some(declared),
        },
        E::AmbiguousNodes { nodes } => DagRetryNodeErrorResponse {
            message: format!(
                "node name(s) {nodes:?} map to more than one task (the DAG reuses the activity); \
                 retry-from-node cannot disambiguate them in v1"
            ),
            unknown_nodes: None,
            declared_nodes: None,
        },
        E::NotAttempted { nodes } => DagRetryNodeErrorResponse {
            message: format!(
                "node(s) {nodes:?} were never attempted on this run; nothing to retry"
            ),
            unknown_nodes: None,
            declared_nodes: None,
        },
        E::AlreadySucceeded { nodes } => DagRetryNodeErrorResponse {
            message: format!(
                "node(s) {nodes:?} already succeeded; use DAG re-run for fresh execution"
            ),
            unknown_nodes: None,
            declared_nodes: None,
        },
        E::NoSchedulePoint => DagRetryNodeErrorResponse {
            message: "could not resolve a reset point for the requested nodes".to_string(),
            unknown_nodes: None,
            declared_nodes: None,
        },
    };
    (axum::http::StatusCode::BAD_REQUEST, Json(body)).into_response()
}

/// Map a #148 reset-validity rejection to the DAG-retry `409 Conflict` with a
/// remediation hint (issue #366's reset-validity boundary check).
fn dag_retry_invalid_point_response(invalid: &ResetInvalidPoint) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    (
        axum::http::StatusCode::CONFLICT,
        Json(DagRetryConflictResponse {
            message: format!(
                "reset boundary at event {} falls inside an unresolved side effect ({} open)",
                invalid.reset_to_event_id,
                invalid.unresolved_side_effects.len()
            ),
            reset_to_event_id: invalid.reset_to_event_id,
            nearest_valid_before: invalid.nearest_valid_before,
            nearest_valid_after: invalid.nearest_valid_after,
            remediation:
                "an upstream side effect (activity, timer, or child workflow) is still unresolved \
                 at the fork point; wait for it to settle or cancel the run first, then retry"
                    .to_string(),
        }),
    )
        .into_response()
}

/// Map a `WorkflowResetError` raised during DAG retry onto the DAG-retry HTTP
/// contract: the invalid-boundary case is a `409` (not the `400` the standalone
/// reset uses), everything else mirrors the standalone reset mapping.
fn dag_retry_reset_error_response(error: WorkflowResetError) -> axum::response::Response {
    match error {
        WorkflowResetError::InvalidPoint(invalid) => dag_retry_invalid_point_response(&invalid),
        other => reset_error_response(other),
    }
}

#[allow(clippy::too_many_lines)]
async fn retry_dag_run(
    Extension(api_state): Extension<HarvestApiState>,
    Path((dag_name, run_exec_id)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    Json(request): Json<DagRetryRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /dags/{dag_name}/runs/{run_exec_id}/retry";

    // Required audit fields.
    if request.reason.trim().is_empty() || request.operator_id.trim().is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(ResetErrorResponse {
                message: "`reason` and `operator_id` are required".to_string(),
            }),
        )
            .into_response();
    }

    // `operator_id` is the mandatory, authoritative "who" for this endpoint, so
    // it is the audit actor unless an explicit `X-Harvest-Actor` was supplied
    // (i.e. the actor extractor returned something other than the anonymous
    // default). This keeps committed retries attributable instead of recording
    // `anonymous` when the runbook curl / CLI only sets `operator_id`.
    let actor = if actor == "anonymous" {
        request.operator_id.trim().to_string()
    } else {
        actor
    };

    let runtime = match api_state.runtime() {
        Ok(rt) => rt,
        Err(e) => return map_error(e).into_response(),
    };

    // Resolve the DAG by name; reject classic (non-unified) DAGs.
    let Some(dag) = runtime.dags().get(&dag_name).cloned() else {
        return AutumnError::not_found_msg(format!("DAG '{dag_name}' is not registered"))
            .into_response();
    };
    if !dag.is_unified {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(ResetErrorResponse {
                message: format!(
                    "DAG '{dag_name}' is a classic (non-unified) DAG; retry-from-node is only \
                     supported for unified DAGs (see issue #256 step 5). Classic DAGs are being retired."
                ),
            }),
        )
            .into_response();
    }

    let exec_id = match parse_execution_id(&run_exec_id) {
        Ok(eid) => eid,
        Err(e) => return e.into_response(),
    };

    let mut conn = match db_conn_for_execution(&api_state, exec_id).await {
        Ok(conn) => conn,
        Err(e) => return e.into_response(),
    };

    // Load the source run and verify it belongs to this DAG.
    let execution = match load_execution(&mut conn, exec_id).await {
        Ok(ex) => ex,
        Err(e) => return map_error(e).into_response(),
    };
    if execution.workflow_name != dag_name {
        return AutumnError::not_found_msg(format!(
            "execution {exec_id} is not a run of DAG '{dag_name}'"
        ))
        .into_response();
    }

    // Source-state gating (issue #366):
    //  - Succeeded (COMPLETED) -> 409, use a fresh run.
    //  - Running / Suspended   -> 409, cancel first (matches #148 contract).
    //  - Terminated            -> 409, already superseded by a prior reset.
    //  - Failed/Cancelled/TimedOut -> accepted.
    match execution.state.as_str() {
        "FAILED" | "CANCELLED" | "TIMED_OUT" => {}
        "COMPLETED" => {
            return (
                axum::http::StatusCode::CONFLICT,
                Json(ResetErrorResponse {
                    message: "DAG run succeeded; use the schedule trigger-now / start endpoint \
                              for a fresh run"
                        .to_string(),
                }),
            )
                .into_response();
        }
        "RUNNING" | "SUSPENDED" => {
            return (
                axum::http::StatusCode::CONFLICT,
                Json(ResetErrorResponse {
                    message: "DAG run is still running; cancel it first, then retry".to_string(),
                }),
            )
                .into_response();
        }
        other => {
            return (
                axum::http::StatusCode::CONFLICT,
                Json(ResetErrorResponse {
                    message: format!("DAG run is in state {other}; cannot retry"),
                }),
            )
                .into_response();
        }
    }

    // Walk the recorded history and resolve the node request to a reset point.
    //
    // NOTE (v1 limitation, issue #366): this resolves the cut and node sets from
    // the *currently registered* `dag.definition`, while the #148 fork preserves
    // the source run's pinned `assigned_build_id` (reset.rs). If worker build-id
    // routing is in use (Phase 3.7, opt-in) AND the DAG topology changed across
    // an incompatible deploy, the worker that replays the fork may run a
    // different definition than the one used to pick the cut. v1 does not gate on
    // build compatibility; see docs/runbooks/dag-retry-from-failed-node.md.
    let history = match store::load_history(&mut conn, exec_id).await {
        Ok(h) => h,
        Err(e) => return map_error(e).into_response(),
    };
    let plan = match crate::dag_retry::resolve_retry_plan(
        &dag.definition,
        &history.events,
        &request.from_nodes,
    ) {
        Ok(plan) => plan,
        Err(e) => return dag_retry_resolve_error_response(e),
    };

    // Compose the reset request: the reason carries the DAG-retry annotation so
    // the audit trail (#158) and the WorkflowResetFork event read cleanly.
    let augmented_reason = format!(
        "{} | dag_retry: nodes=[{}]",
        request.reason.trim(),
        request.from_nodes.join(",")
    );
    let reset_request = WorkflowResetRequest {
        reset_to_event_id: plan.reset_to_event_id,
        reason: augmented_reason,
        operator_id: request.operator_id.trim().to_string(),
        signal_reapply: autumn_harvest::reset::ResetSignalReapplyPolicy::default(),
        allow_terminal_source: true,
    };

    // Dry-run: validate the boundary and return the plan without writing.
    if request.dry_run {
        return match preview_workflow_reset(&mut conn, exec_id, reset_request).await {
            Ok(_reset_plan) => (
                axum::http::StatusCode::OK,
                Json(DagRetryResponse {
                    dry_run: true,
                    dag_name: dag_name.clone(),
                    source_run_exec_id: exec_id.to_string(),
                    reset_to_event_id: plan.reset_to_event_id,
                    nodes_to_re_execute: plan.nodes_to_re_execute,
                    nodes_carried_over: plan.nodes_carried_over,
                    new_run_exec_id: None,
                    events_carried_over: None,
                }),
            )
                .into_response(),
            Err(error) => dag_retry_reset_error_response(error),
        };
    }

    // Commit the fork via the existing #148 reset internals.
    let registry = Some(runtime.registry().as_ref());
    let exec_id_str = exec_id.to_string();
    match reset_workflow_execution(&mut conn, exec_id, reset_request, registry).await {
        Ok(result) => {
            let new_exec_id_str = result.new_exec_id.to_string();
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_DAG_RETRY,
                target_type: TARGET_DAG,
                target_id: Some(dag_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: Some(exec_id.shard().as_i32()),
                source: &source,
            };
            if let Err(audit_err) = audit::insert_audit(&mut conn, &ar).await {
                tracing::error!(error = %audit_err, new_exec_id = %new_exec_id_str, "audit insert failed for dag.retry");
                return AutumnError::service_unavailable_msg(format!(
                    "audit insert failed: {audit_err}"
                ))
                .into_response();
            }
            (
                axum::http::StatusCode::CREATED,
                Json(DagRetryResponse {
                    dry_run: false,
                    dag_name: dag_name.clone(),
                    source_run_exec_id: exec_id_str,
                    reset_to_event_id: result.reset_to_event_id,
                    nodes_to_re_execute: plan.nodes_to_re_execute,
                    nodes_carried_over: plan.nodes_carried_over,
                    new_run_exec_id: Some(new_exec_id_str),
                    events_carried_over: Some(result.events_carried_over),
                }),
            )
                .into_response()
        }
        Err(error) => {
            let err_str = format!("{error:?}");
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_DAG_RETRY,
                target_type: TARGET_DAG,
                target_id: Some(dag_name.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some(err_str.as_str()),
                shard_id: Some(exec_id.shard().as_i32()),
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            dag_retry_reset_error_response(error)
        }
    }
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

    // Issue #252: enforce signal payload cap before inserting the signal row.
    if let Ok(runtime) = api_state.runtime() {
        check_signal_payload_cap(&payload, runtime.registry.max_signal_payload_bytes)?;
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

#[allow(clippy::too_many_lines)]
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

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /dags/{dag_name}/trigger";

    // issue #377: check admission gates before creating a new DAG run.
    {
        let gate_hit = api_state.gate_cache().check(
            &dag_name,
            &default_queue,
            shard.as_i32(),
            dag.owner.as_deref(),
        );
        if let Some((gate_id, gate_reason, scope_kind)) = gate_hit {
            let reason_label = match gate_reason.char_indices().nth(64) {
                Some((idx, _)) => &gate_reason[..idx],
                None => &gate_reason,
            };
            runtime
                .registry
                .telemetry()
                .metrics
                .record_admission_blocked(scope_kind, reason_label);
            if let Ok(mut audit_conn) = acquire_conn(pool.pool_for(shard)).await {
                let err_str = format!("admission blocked by gate {gate_id}: {gate_reason}");
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
            }
            return Err(AutumnError::service_unavailable_msg(format!(
                "admission blocked by gate {gate_id}: {gate_reason}"
            )));
        }
    }

    let mut schedule_conn = acquire_conn(pool.pool_for(shard)).await?;
    ensure_dag_schedule(&mut schedule_conn, &dag)
        .await
        .map_err(map_error)?;
    drop(schedule_conn);

    let trigger_result = trigger_unified_dag(
        pool.pool_for(shard).clone(),
        &dag_name,
        request.conf,
        shard,
        &default_queue,
        dag.owner.as_deref(),
        dag.runbook_url.as_deref(),
        dag.severity.as_deref(),
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
            let remaining_runs = s.max_runs.map(|max| (max - s.runs_started).max(0));
            let effective_policy = autumn_harvest::policy::CatchupPolicy::from_db(
                s.catchup_policy.as_deref(),
                s.catchup_window_secs,
                s.catchup,
            );
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
                overlap_policy: s.overlap_policy,
                buffered_count,
                buffer_all_max: s.buffer_all_max,
                calendar_name: s.calendar_name,
                skip_policy: s.skip_policy,
                consecutive_failure_limit: s.consecutive_failure_limit,
                consecutive_failure_count: s.consecutive_failure_count,
                auto_paused_at: s.auto_paused_at,
                end_at: s.end_at,
                max_runs: s.max_runs,
                runs_started: s.runs_started,
                remaining_runs,
                exhausted_at: s.exhausted_at,
                exhausted_reason: s.exhausted_reason,
                catchup_policy_effective: effective_policy.as_str().to_string(),
                catchup_window_secs: s.catchup_window_secs,
                catchup_dropped_last_recovery: s.last_catchup_dropped,
                last_catchup_at: s.last_catchup_at,
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
    let remaining_runs = s.max_runs.map(|max| (max - s.runs_started).max(0));
    let effective_policy = autumn_harvest::policy::CatchupPolicy::from_db(
        s.catchup_policy.as_deref(),
        s.catchup_window_secs,
        s.catchup,
    );
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
        calendar_name: s.calendar_name.clone(),
        skip_policy: s.skip_policy.clone(),
        consecutive_failure_limit: s.consecutive_failure_limit,
        consecutive_failure_count: s.consecutive_failure_count,
        auto_paused_at: s.auto_paused_at,
        end_at: s.end_at,
        max_runs: s.max_runs,
        runs_started: s.runs_started,
        remaining_runs,
        exhausted_at: s.exhausted_at,
        exhausted_reason: s.exhausted_reason,
        catchup_policy_effective: effective_policy.as_str().to_string(),
        catchup_window_secs: s.catchup_window_secs,
        catchup_dropped_last_recovery: s.last_catchup_dropped,
        last_catchup_at: s.last_catchup_at,
    }))
}

#[derive(Debug, serde::Deserialize)]
struct DecisionsQuery {
    since: Option<String>,
    decision: Option<String>,
    reason: Option<String>,
    limit: Option<i64>,
}

async fn get_schedule_decisions(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
    Query(query): Query<DecisionsQuery>,
) -> Result<Json<Vec<ScheduleDecision>>, AutumnError> {
    use autumn_harvest::schema::harvest_schedule_decisions::dsl;

    let id = parse_uuid(&id_str, "schedule id")?;
    let pool = api_state.storage_pool().map_err(map_error)?;

    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let since = query
        .since
        .as_deref()
        .map(parse_audit_datetime)
        .transpose()?;

    let mut records: Vec<ScheduleDecision> = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut q = dsl::harvest_schedule_decisions.into_boxed();

        q = q.filter(dsl::schedule_id.eq(id));

        if let Some(ref since_dt) = since {
            q = q.filter(dsl::occurred_at.ge(*since_dt));
        }
        if let Some(ref dec_val) = query.decision {
            q = q.filter(dsl::decision.eq(dec_val));
        }
        if let Some(ref reason_val) = query.reason {
            q = q.filter(dsl::reason_code.eq(reason_val));
        }

        let mut rows: Vec<ScheduleDecision> = q
            .order(dsl::occurred_at.desc())
            .limit(limit)
            .select(ScheduleDecision::as_select())
            .load(&mut conn)
            .await
            .map_err(database_error)
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

#[derive(Debug, serde::Deserialize)]
struct FleetDecisionsQuery {
    schedule_name: Option<String>,
    since: Option<String>,
    decision: Option<String>,
    reason: Option<String>,
    limit: Option<i64>,
}

async fn list_fleet_decisions(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<FleetDecisionsQuery>,
) -> Result<Json<Vec<ScheduleDecision>>, AutumnError> {
    use autumn_harvest::schema::harvest_schedule_decisions::dsl;

    let pool = api_state.storage_pool().map_err(map_error)?;

    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    let since = query
        .since
        .as_deref()
        .map(parse_audit_datetime)
        .transpose()?;

    let mut records: Vec<ScheduleDecision> = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut q = dsl::harvest_schedule_decisions.into_boxed();

        if let Some(ref name_val) = query.schedule_name {
            q = q.filter(dsl::schedule_name.eq(name_val));
        }
        if let Some(ref since_dt) = since {
            q = q.filter(dsl::occurred_at.ge(*since_dt));
        }
        if let Some(ref dec_val) = query.decision {
            q = q.filter(dsl::decision.eq(dec_val));
        }
        if let Some(ref reason_val) = query.reason {
            q = q.filter(dsl::reason_code.eq(reason_val));
        }

        let mut rows: Vec<ScheduleDecision> = q
            .order(dsl::occurred_at.desc())
            .limit(limit)
            .select(ScheduleDecision::as_select())
            .load(&mut conn)
            .await
            .map_err(database_error)
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
    let remaining_runs = row.max_runs.map(|max| (max - row.runs_started).max(0));
    let effective_policy = autumn_harvest::policy::CatchupPolicy::from_db(
        row.catchup_policy.as_deref(),
        row.catchup_window_secs,
        row.catchup,
    );
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
        overlap_policy: row.overlap_policy,
        buffered_count,
        buffer_all_max: row.buffer_all_max,
        calendar_name: row.calendar_name,
        skip_policy: row.skip_policy,
        consecutive_failure_limit: row.consecutive_failure_limit,
        consecutive_failure_count: row.consecutive_failure_count,
        auto_paused_at: row.auto_paused_at,
        end_at: row.end_at,
        max_runs: row.max_runs,
        runs_started: row.runs_started,
        remaining_runs,
        exhausted_at: row.exhausted_at,
        exhausted_reason: row.exhausted_reason,
        catchup_policy_effective: effective_policy.as_str().to_string(),
        catchup_window_secs: row.catchup_window_secs,
        catchup_dropped_last_recovery: row.last_catchup_dropped,
        last_catchup_at: row.last_catchup_at,
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
    let skip_policy = match SkipPolicy::from_user_input(&request.skip_policy) {
        Ok(p) => p,
        Err(v) => {
            let err_summary = format!(
                "invalid skip_policy '{v}'; valid values: skip, run_next_business_day, run_prev_business_day"
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

    // Reject unknown catchup_policy modes with 400 before storing (issue #484).
    // `from_db` is lenient for backward compat; user input is validated strictly.
    // `None` (omitted) preserves the legacy `catchup` bool and leaves the policy
    // columns NULL.
    let catchup_policy = match request.catchup_policy.as_deref() {
        Some(mode) => {
            match autumn_harvest::CatchupPolicy::from_user_input(mode, request.catchup_window_secs)
            {
                Ok(p) => Some(p),
                Err(v) => {
                    let err_summary = format!(
                        "invalid catchup_policy '{v}'; valid values: skip_all, most_recent, window, unbounded"
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
            }
        }
        None => None,
    };

    // Validate calendar name exists before storing. Return 400 for NotFound so
    // clients distinguish invalid input from transient DB failures (503).
    if let Some(cal_name) = &request.calendar {
        match get_calendar(&mut conn, cal_name).await {
            Ok(_) => {}
            Err(autumn_harvest::HarvestError::NotFound(_)) => {
                let err_summary = format!(
                    "calendar '{cal_name}' not found; create it first with POST /calendars"
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
            Err(e) => return Err(map_error(e)),
        }
    }

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
        calendar: request.calendar.clone(),
        skip_policy,
        consecutive_failure_limit: request.consecutive_failure_limit,
        end_at: request.end_at,
        // Normalize 0 → None: callers passing max_runs=0 intend "no limit".
        max_runs: request.max_runs.filter(|&n| n > 0),
        catchup_policy,
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

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateCompletionTriggerRequest {
    pub id: Option<uuid::Uuid>,
    pub source_workflow_name: String,
    pub terminal_states: Option<Vec<TerminalState>>,
    pub target_workflow_name: String,
    pub input_mapping: Option<InputMapping>,
    pub queue_name: Option<String>,
}

async fn list_completion_triggers(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<CompletionTriggerDb>>, AutumnError> {
    use autumn_harvest::schema::harvest_completion_triggers::dsl as triggers_dsl;

    let pool = api_state.storage_pool().map_err(map_error)?;
    let runtime = api_state.runtime().map_err(map_error)?;
    let mut conn = acquire_conn(pool.pool_for(runtime.router().default_shard())).await?;

    let rows = triggers_dsl::harvest_completion_triggers
        .order(triggers_dsl::created_at.asc())
        .select(CompletionTriggerDb::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    Ok(Json(rows))
}

#[allow(clippy::too_many_lines)]
async fn create_completion_trigger(
    Extension(api_state): Extension<HarvestApiState>,
    Json(request): Json<CreateCompletionTriggerRequest>,
) -> Result<(StatusCode, Json<CompletionTriggerDb>), AutumnError> {
    use autumn_harvest::schema::harvest_completion_triggers::dsl as triggers_dsl;
    use chrono::Utc;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let runtime = api_state.runtime().map_err(map_error)?;
    if !runtime
        .registry
        .workflows
        .contains_key(&request.source_workflow_name)
    {
        return Err(AutumnError::not_found_msg(format!(
            "source workflow '{}' is not registered",
            request.source_workflow_name
        )));
    }
    if !runtime
        .registry
        .workflows
        .contains_key(&request.target_workflow_name)
    {
        return Err(AutumnError::not_found_msg(format!(
            "target workflow '{}' is not registered",
            request.target_workflow_name
        )));
    }

    let trigger_id = request.id.unwrap_or_else(uuid::Uuid::new_v4);
    let states = request
        .terminal_states
        .unwrap_or_else(|| vec![TerminalState::Completed]);
    let mapping = request.input_mapping.unwrap_or(InputMapping::Passthrough);

    let states_val = serde_json::to_value(&states)
        .map_err(|e| AutumnError::bad_request_msg(format!("invalid terminal states: {e}")))?;
    let mapping_val = serde_json::to_value(&mapping)
        .map_err(|e| AutumnError::bad_request_msg(format!("invalid input mapping: {e}")))?;

    let new_row = NewCompletionTriggerDb {
        id: trigger_id,
        source_workflow_name: request.source_workflow_name.clone(),
        terminal_states: states_val,
        target_workflow_name: request.target_workflow_name.clone(),
        input_mapping: mapping_val,
        queue_name: request.queue_name.clone(),
        is_static: false,
    };

    let pool = api_state.storage_pool().map_err(map_error)?;

    // Fetch pre-existing trigger definitions from all shards to restore on failure (P2)
    let mut original_states = std::collections::HashMap::new();
    for (shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let existing = triggers_dsl::harvest_completion_triggers
            .filter(triggers_dsl::id.eq(trigger_id))
            .select(CompletionTriggerDb::as_select())
            .first::<CompletionTriggerDb>(&mut conn)
            .await
            .optional()
            .map_err(database_error)
            .map_err(map_error)?;
        original_states.insert(shard, existing);
    }

    let mut inserted_row = None;
    let mut completed_shards = Vec::new();

    for (shard, shard_pool) in pool.iter_shards() {
        let mut conn = match acquire_conn(shard_pool).await {
            Ok(c) => c,
            Err(e) => {
                for rolled_shard in completed_shards {
                    let rolled_pool = pool.pool_for(rolled_shard);
                    if let Ok(mut rollback_conn) = acquire_conn(rolled_pool).await {
                        if let Some(Some(old_row)) = original_states.get(&rolled_shard) {
                            let _ = diesel::update(
                                autumn_harvest::schema::harvest_completion_triggers::table
                                    .filter(triggers_dsl::id.eq(trigger_id)),
                            )
                            .set((
                                triggers_dsl::source_workflow_name
                                    .eq(&old_row.source_workflow_name),
                                triggers_dsl::terminal_states.eq(&old_row.terminal_states),
                                triggers_dsl::target_workflow_name
                                    .eq(&old_row.target_workflow_name),
                                triggers_dsl::input_mapping.eq(&old_row.input_mapping),
                                triggers_dsl::queue_name.eq(&old_row.queue_name),
                                triggers_dsl::is_static.eq(old_row.is_static),
                                triggers_dsl::created_at.eq(old_row.created_at),
                                triggers_dsl::updated_at.eq(old_row.updated_at),
                            ))
                            .execute(&mut rollback_conn)
                            .await;
                        } else {
                            let _ = diesel::delete(
                                autumn_harvest::schema::harvest_completion_triggers::table
                                    .filter(triggers_dsl::id.eq(trigger_id)),
                            )
                            .execute(&mut rollback_conn)
                            .await;
                        }
                    }
                }
                return Err(e);
            }
        };

        let row =
            match diesel::insert_into(autumn_harvest::schema::harvest_completion_triggers::table)
                .values(&new_row)
                .on_conflict(triggers_dsl::id)
                .do_update()
                .set((
                    triggers_dsl::source_workflow_name.eq(&new_row.source_workflow_name),
                    triggers_dsl::terminal_states.eq(&new_row.terminal_states),
                    triggers_dsl::target_workflow_name.eq(&new_row.target_workflow_name),
                    triggers_dsl::input_mapping.eq(&new_row.input_mapping),
                    triggers_dsl::queue_name.eq(&new_row.queue_name),
                    triggers_dsl::updated_at.eq(Utc::now()),
                ))
                .get_result::<CompletionTriggerDb>(&mut conn)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    for rolled_shard in completed_shards {
                        let rolled_pool = pool.pool_for(rolled_shard);
                        if let Ok(mut rollback_conn) = acquire_conn(rolled_pool).await {
                            if let Some(Some(old_row)) = original_states.get(&rolled_shard) {
                                let _ = diesel::update(
                                    autumn_harvest::schema::harvest_completion_triggers::table
                                        .filter(triggers_dsl::id.eq(trigger_id)),
                                )
                                .set((
                                    triggers_dsl::source_workflow_name
                                        .eq(&old_row.source_workflow_name),
                                    triggers_dsl::terminal_states.eq(&old_row.terminal_states),
                                    triggers_dsl::target_workflow_name
                                        .eq(&old_row.target_workflow_name),
                                    triggers_dsl::input_mapping.eq(&old_row.input_mapping),
                                    triggers_dsl::queue_name.eq(&old_row.queue_name),
                                    triggers_dsl::is_static.eq(old_row.is_static),
                                    triggers_dsl::created_at.eq(old_row.created_at),
                                    triggers_dsl::updated_at.eq(old_row.updated_at),
                                ))
                                .execute(&mut rollback_conn)
                                .await;
                            } else {
                                let _ = diesel::delete(
                                    autumn_harvest::schema::harvest_completion_triggers::table
                                        .filter(triggers_dsl::id.eq(trigger_id)),
                                )
                                .execute(&mut rollback_conn)
                                .await;
                            }
                        }
                    }
                    return Err(map_error(database_error(e)));
                }
            };

        inserted_row = Some(row);
        completed_shards.push(shard);
    }

    let inserted_row = inserted_row
        .ok_or_else(|| AutumnError::internal_server_error_msg("no database shards configured"))?;

    Ok((StatusCode::CREATED, Json(inserted_row)))
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
            // Resume: clear manual-pause metadata AND auto-pause state (issue #360).
            // Reset consecutive_failure_count to 0 so the counter starts fresh
            // and does not immediately re-trigger auto-pause on the next tick.
            // The filter matches `is_paused = true OR auto_paused_at IS NOT NULL`
            // so an auto-paused schedule can be resumed even when `is_paused = false`,
            // while a fully-active schedule receives no unnecessary UPDATE.
            diesel::update(
                dsl::harvest_schedules.find(id).filter(
                    dsl::is_paused
                        .ne(false)
                        .or(dsl::auto_paused_at.is_not_null()),
                ),
            )
            .set((
                dsl::is_paused.eq(false),
                dsl::paused_at.eq(None::<chrono::DateTime<chrono::Utc>>),
                dsl::paused_by.eq(None::<&str>),
                dsl::pause_reason.eq(None::<&str>),
                dsl::auto_paused_at.eq(None::<chrono::DateTime<chrono::Utc>>),
                dsl::consecutive_failure_count.eq(0),
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

// ── Schedule trigger-now (issue #343) ─────────────────────────────────────────

/// Optional request body for `POST /admin/schedules/{id}/trigger`.
#[derive(Debug, Default, Deserialize)]
struct ScheduleTriggerRequest {
    /// Free-text reason recorded in the audit trail.
    #[serde(default)]
    reason: Option<String>,
    /// Optional override of the schedule's configured overlap policy.
    #[serde(default)]
    overlap_policy: Option<String>,
}

/// Query parameters for `POST /admin/schedules/{id}/trigger`.
#[derive(Debug, Default, Deserialize)]
struct TriggerScheduleQuery {
    /// If `true`, trigger even if the schedule is paused.
    #[serde(default)]
    force: bool,
}

/// Response for `POST /admin/schedules/{id}/trigger`.
#[derive(Debug, Serialize)]
struct ScheduleTriggerResponse {
    /// Present when an execution was actually started; absent when skipped by overlap policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_id: Option<uuid::Uuid>,
    workflow_id: String,
    triggered_at: chrono::DateTime<chrono::Utc>,
    /// `"fired"` when a new execution was started, `"skipped_overlap"` when the
    /// effective overlap policy suppressed the run.
    outcome: String,
}

#[allow(clippy::too_many_lines)]
async fn trigger_schedule_now(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    Query(query): Query<TriggerScheduleQuery>,
    headers: axum::http::HeaderMap,
    body: Option<Json<ScheduleTriggerRequest>>,
) -> Result<Json<ScheduleTriggerResponse>, AutumnError> {
    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /admin/schedules/{id}/trigger";
    let body = body.map(|Json(b)| b).unwrap_or_default();

    let schedule_id = parse_uuid(&id, "schedule id")?;
    let pool = api_state.storage_pool().map_err(map_error)?;
    let runtime = api_state.runtime().map_err(map_error)?;

    // Load the schedule AND which shard it lives on. Budget accounting writes must
    // target the same shard — the schedule row may not be on the default shard for
    // unified-DAG schedules (issue #478).
    let (schedule, schedule_shard) =
        load_schedule_by_id_with_shard(&api_state, schedule_id).await?;

    let schedule_display_name = schedule
        .workflow_name
        .as_deref()
        .or(schedule.dag_name.as_deref())
        .unwrap_or("")
        .to_string();

    // Reject paused schedules unless ?force=true is passed.
    if schedule.is_paused && !query.force {
        if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_SCHEDULE_TRIGGER,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("rejected_paused"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
        runtime
            .registry
            .telemetry()
            .metrics
            .record_schedule_manual_trigger(&schedule_display_name, "rejected_paused");
        return Err(AutumnError::bad_request_msg(format!(
            "schedule {schedule_id} is paused; pass ?force=true to trigger a paused schedule"
        ))
        .with_status(axum::http::StatusCode::CONFLICT));
    }

    // Reject exhausted schedules (issue #478). Manual triggers cannot bypass
    // end_at / max_runs limits — the schedule has reached its terminal state.
    // Also check live bounds: the scheduler may not have processed the row yet
    // (e.g. next_run_at > end_at, or max_runs was tightened mid-flight), so
    // exhausted_at can be NULL even though the bounds are already violated.
    {
        let trigger_now = chrono::Utc::now();
        let live_end_at_exceeded = schedule.end_at.is_some_and(|end| trigger_now >= end);
        let live_budget_exhausted = schedule
            .max_runs
            .is_some_and(|max| max > 0 && schedule.runs_started >= max);
        if schedule.exhausted_at.is_some() || live_end_at_exceeded || live_budget_exhausted {
            if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_SCHEDULE_TRIGGER,
                    target_type: TARGET_SCHEDULE,
                    target_id: Some(id.as_str()),
                    route_or_command: route,
                    request_id: request_id.as_deref(),
                    idempotency_key: None,
                    status: STATUS_FAILED,
                    error_summary: Some("rejected_exhausted"),
                    shard_id: None,
                    source: &source,
                };
                let _ = audit::insert_audit(&mut conn, &ar).await;
            }
            runtime
                .registry
                .telemetry()
                .metrics
                .record_schedule_manual_trigger(&schedule_display_name, "rejected_exhausted");
            return Err(AutumnError::bad_request_msg(format!(
                "schedule {schedule_id} is exhausted (end_at or max_runs reached)"
            ))
            .with_status(axum::http::StatusCode::CONFLICT));
        }
    }

    // Resolve the effective overlap policy (body override takes precedence).
    let effective_overlap_policy = if let Some(ref op_str) = body.overlap_policy {
        match autumn_harvest::OverlapPolicy::from_user_input(op_str) {
            Ok(op) => op,
            Err(e) => {
                return Err(AutumnError::bad_request_msg(format!(
                    "invalid overlap_policy: {e}"
                )));
            }
        }
    } else {
        autumn_harvest::OverlapPolicy::from_db(&schedule.overlap_policy)
    };

    // Determine workflow name, input, and queue from the schedule.
    let (workflow_name, input, queue_name) = match (
        schedule.workflow_name.as_deref(),
        schedule.dag_name.as_deref(),
    ) {
        (Some(wf_name), _) => {
            let input = schedule
                .workflow_input
                .clone()
                .unwrap_or(serde_json::Value::Null);
            let queue = schedule
                .queue_name
                .as_deref()
                .unwrap_or("default")
                .to_string();
            (wf_name.to_string(), input, queue)
        }
        (None, Some(dag_name)) => {
            let dag_queue = runtime
                .dags()
                .get(dag_name)
                .and_then(|d| d.default_queue.as_deref())
                .or(schedule.queue_name.as_deref())
                .unwrap_or("default")
                .to_string();
            (dag_name.to_string(), serde_json::Value::Null, dag_queue)
        }
        (None, None) => {
            return Err(AutumnError::service_unavailable_msg(
                "schedule row has neither workflow_name nor dag_name",
            ));
        }
    };

    // Pre-generate triggered_at and workflow_id so the gate check uses the
    // actual execution shard (router-determined) rather than a hard-coded 0.
    let triggered_at = chrono::Utc::now();
    // Append a UUID v4 so concurrent trigger calls within the same millisecond
    // each produce a distinct workflow_id and start independent executions.
    let workflow_id = format!(
        "manual-{schedule_id}-{}-{}",
        triggered_at.timestamp_millis(),
        uuid::Uuid::new_v4().simple()
    );
    let trigger_exec_shard = runtime
        .router
        .pick_for_new_workflow(&workflow_name, &workflow_id);

    // issue #377: check admission gates before firing a manual trigger.
    {
        let dag_lookup_key = schedule.dag_name.as_deref().unwrap_or(&workflow_name);
        let wf_owner = runtime
            .registry
            .workflows
            .get(&workflow_name)
            .and_then(|i| i.owner)
            .or_else(|| {
                runtime
                    .dags()
                    .get(dag_lookup_key)
                    .and_then(|d| d.owner.as_deref())
            });
        let gate_hit = api_state.gate_cache().check(
            &workflow_name,
            &queue_name,
            trigger_exec_shard.as_i32(),
            wf_owner,
        );
        if let Some((gate_id, gate_reason, scope_kind)) = gate_hit {
            let reason_label = match gate_reason.char_indices().nth(64) {
                Some((idx, _)) => &gate_reason[..idx],
                None => &gate_reason,
            };
            runtime
                .registry
                .telemetry()
                .metrics
                .record_admission_blocked(scope_kind, reason_label);
            if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
                let err_str = format!("admission blocked by gate {gate_id}: {gate_reason}");
                let ar = NewAuditRecord {
                    actor: &actor,
                    operation: OP_SCHEDULE_TRIGGER,
                    target_type: TARGET_SCHEDULE,
                    target_id: Some(id.as_str()),
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
            return Err(AutumnError::service_unavailable_msg(format!(
                "admission blocked by gate {gate_id}: {gate_reason}"
            )));
        }
    }

    // triggered_at and workflow_id were pre-generated above for the gate check.

    // Acquire the DB connection early so it is available for both the overlap-skip
    // audit record and the normal start call.
    let mut conn = acquire_conn(pool.default_pool()).await?;
    // Acquire a separate connection on the schedule's own shard for budget accounting.
    // Schedule rows for unified-DAG schedules may live on a non-default shard; using
    // pool.default_pool() here would target zero rows (issue #478).
    let sched_pool = pool.pool_for(schedule_shard);
    let mut sched_conn = acquire_conn(sched_pool).await?;

    // For Skip policy, fail closed: if the running-count query fails on any shard,
    // treat it as saturated rather than silently firing through.
    if effective_overlap_policy == autumn_harvest::OverlapPolicy::Skip {
        let is_saturated =
            match query_running_count(&pool, &ScheduleKind::Workflow, &workflow_name).await {
                Ok(running) => running >= i64::from(schedule.max_active_runs),
                Err(_) => true,
            };
        if is_saturated {
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_SCHEDULE_TRIGGER,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: Some("skipped_overlap"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            runtime
                .registry
                .telemetry()
                .metrics
                .record_schedule_manual_trigger(&schedule_display_name, "skipped_overlap");
            return Ok(Json(ScheduleTriggerResponse {
                execution_id: None,
                workflow_id,
                triggered_at,
                outcome: "skipped_overlap".to_string(),
            }));
        }
    }

    // Atomically reserve one run slot against the max_runs budget before starting
    // the workflow (issue #478). The WHERE guard prevents concurrent manual triggers
    // from racing past the earlier in-memory admission check: only one of them
    // increments successfully when runs_started is at the limit.
    //
    // For unlimited schedules (max_runs IS NULL) the WHERE always matches, so
    // runs_started is incremented for observability and rows_affected = 1.
    //
    // If rows_affected = 0, either exhausted_at was set by a concurrent scheduler
    // tick/trigger between our admission check and this point, or a concurrent
    // trigger already consumed the last slot.
    {
        use autumn_harvest::schema::harvest_schedules::dsl as sdsl;
        let reserved = diesel::update(
            sdsl::harvest_schedules
                .find(schedule.id)
                .filter(sdsl::exhausted_at.is_null())
                .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(
                    "max_runs IS NULL OR runs_started < max_runs",
                )),
        )
        .set((
            sdsl::runs_started.eq(sdsl::runs_started + 1),
            sdsl::updated_at.eq(triggered_at),
        ))
        .execute(&mut sched_conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
        if reserved == 0 {
            runtime
                .registry
                .telemetry()
                .metrics
                .record_schedule_manual_trigger(&schedule_display_name, "rejected_exhausted");
            return Err(AutumnError::bad_request_msg(format!(
                "schedule {schedule_id} run budget exhausted (concurrent trigger or tick)"
            ))
            .with_status(axum::http::StatusCode::CONFLICT));
        }
    }

    // ExecutionId::new() embeds ShardId::UNENCODED, which ShardRouter resolves to
    // the default shard. This is consistent with how the scheduler fires executions
    // (all schedule-initiated runs land on the default shard in single-shard
    // deployments; multi-shard schedule pinning is a follow-up to issue #171).
    let exec_id = ExecutionId::new();

    let (owner, runbook_url, severity) = {
        let wf_meta = runtime
            .registry
            .workflows
            .get(&workflow_name)
            .map(|info| (info.owner, info.runbook_url, info.severity));
        let dag_meta = runtime.dags().get(&workflow_name).map(|dag| {
            (
                dag.owner.as_deref(),
                dag.runbook_url.as_deref(),
                dag.severity.as_deref(),
            )
        });
        match (wf_meta, dag_meta) {
            (Some((o, r, s)), Some((dag_owner, dag_runbook, dag_severity))) => {
                (o.or(dag_owner), r.or(dag_runbook), s.or(dag_severity))
            }
            (Some((o, r, s)), None) => (o, r, s),
            (None, Some((dag_owner, dag_runbook, dag_severity))) => {
                (dag_owner, dag_runbook, dag_severity)
            }
            (None, None) => (None, None, None),
        }
    };
    // Only registered workflows carry an SLA default; DAGs have no SLA concept.
    let sla = runtime
        .registry
        .workflows
        .get(&workflow_name)
        .and_then(|info| clamp_info_default_sla(info.sla, info.execution_timeout));

    let result = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: &workflow_name,
            workflow_id: &workflow_id,
            exec_id,
            input: input.clone(),
            parent_id: None,
            queue_name: &queue_name,
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner,
            runbook_url,
            severity,
            context_headers: None,
            sla,
        },
    )
    .await;

    let (exec_id_out, outcome) = match result {
        Ok(exec_result) => (Some(exec_result.exec_id.as_uuid()), "fired"),
        Err(e) => {
            // Undo the pre-increment so the budget reflects actual started workflows.
            // Do NOT guard on exhausted_at IS NULL: a scheduler tick could have raced
            // and set exhausted_at after our pre-increment, so the guard would silently
            // skip the rollback and leave runs_started inflated (issue #478).
            {
                use autumn_harvest::schema::harvest_schedules::dsl as sdsl;
                let _ = diesel::update(sdsl::harvest_schedules.find(schedule.id))
                    .set((
                        sdsl::runs_started.eq(sdsl::runs_started - 1),
                        sdsl::updated_at.eq(triggered_at),
                    ))
                    .execute(&mut sched_conn)
                    .await;
                // If the decrement brought runs_started below max_runs, clear any
                // exhaustion that our (now-reversed) pre-increment may have caused.
                // next_run_at is intentionally not restored here — a future
                // upsert_workflow_schedule call (e.g. on server restart for
                // code-declared schedules) will recalculate it.
                let _ = diesel::update(
                    sdsl::harvest_schedules
                        .find(schedule.id)
                        .filter(sdsl::exhausted_at.is_not_null())
                        .filter(sdsl::max_runs.is_null().or(diesel::dsl::sql::<
                            diesel::sql_types::Bool,
                        >(
                            "runs_started < max_runs"
                        ))),
                )
                .set((
                    sdsl::exhausted_at.eq(None::<chrono::DateTime<chrono::Utc>>),
                    sdsl::exhausted_reason.eq(None::<String>),
                    sdsl::updated_at.eq(triggered_at),
                ))
                .execute(&mut sched_conn)
                .await;
            }
            let ar = NewAuditRecord {
                actor: &actor,
                operation: OP_SCHEDULE_TRIGGER,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id.as_str()),
                route_or_command: route,
                request_id: request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_FAILED,
                error_summary: Some("start_failed"),
                shard_id: None,
                source: &source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
            runtime
                .registry
                .telemetry()
                .metrics
                .record_schedule_manual_trigger(&schedule_display_name, "start_failed");
            return Err(map_error(e));
        }
    };

    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_SCHEDULE_TRIGGER,
        target_type: TARGET_SCHEDULE,
        target_id: Some(id.as_str()),
        route_or_command: route,
        request_id: request_id.as_deref(),
        idempotency_key: None,
        status: STATUS_SUCCEEDED,
        error_summary: body.reason.as_deref(),
        shard_id: None,
        source: &source,
    };
    audit::insert_audit(&mut conn, &ar)
        .await
        .map_err(map_error)?;

    // The pre-increment above already wrote runs_started = runs_started + 1.
    // If that new value reaches max_runs, transition to exhausted now so the
    // schedule immediately disappears from the due-list. The guard on
    // exhausted_at IS NULL prevents a double-exhaustion race with the tick.
    {
        use autumn_harvest::schema::harvest_schedules::dsl as sdsl;
        let new_runs_started = schedule.runs_started.saturating_add(1);
        if schedule
            .max_runs
            .is_some_and(|max| max > 0 && new_runs_started >= max)
        {
            let _ = diesel::update(
                sdsl::harvest_schedules
                    .find(schedule.id)
                    .filter(sdsl::exhausted_at.is_null()),
            )
            .set((
                sdsl::exhausted_at.eq(Some(triggered_at)),
                sdsl::exhausted_reason.eq(Some("max_runs_exhausted")),
                sdsl::next_run_at.eq(None::<chrono::DateTime<chrono::Utc>>),
                sdsl::updated_at.eq(triggered_at),
            ))
            .execute(&mut sched_conn)
            .await;
        }
    }

    runtime
        .registry
        .telemetry()
        .metrics
        .record_schedule_manual_trigger(&schedule_display_name, outcome);

    Ok(Json(ScheduleTriggerResponse {
        execution_id: exec_id_out,
        workflow_id,
        triggered_at,
        outcome: outcome.to_string(),
    }))
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

    // Reject exhausted schedules — bounds have been reached and no more runs
    // should start via any path (issue #478). Also check live bounds in case
    // the scheduler hasn't yet processed an already-violated row.
    {
        let now = chrono::Utc::now();
        let live_end_at_exceeded = schedule.end_at.is_some_and(|end| now >= end);
        let live_budget_exhausted = schedule
            .max_runs
            .is_some_and(|max| max > 0 && schedule.runs_started >= max);
        if schedule.exhausted_at.is_some() || live_end_at_exceeded || live_budget_exhausted {
            return Err(AutumnError::bad_request_msg(format!(
                "schedule {schedule_id} is exhausted (end_at or max_runs reached); \
                 extend the limits before backfilling"
            ))
            .with_status(axum::http::StatusCode::CONFLICT));
        }
    }

    let parsed_schedule = schedule
        .schedule_expr
        .as_deref()
        .and_then(parse_schedule_from_expr_pub);

    let max_count = request.max_count.unwrap_or(DEFAULT_BACKFILL_MAX_COUNT);

    let limit_err = |e| match e {
        BackfillPlanError::LimitExceeded { limit } => AutumnError::bad_request_msg(format!(
            "backfill window contains more than {limit} timestamps; lower the window or pass a higher max_count"
        )),
    };
    // Each entry is (original_slot, fire_time). original_slot is the raw cron timestamp
    // used for deterministic workflow-ID generation so that calendar rebasing (e.g.
    // RunNextBusinessDay / RunPrevBusinessDay) cannot cause two distinct logical slots
    // that adjust to the same day to collide on the derived workflow ID.
    let timestamp_pairs: Vec<BackfillSlot> = if let Some(ref cal_name) = schedule.calendar_name {
        let mut conn = acquire_conn(pool.default_pool()).await?;
        let excluded_dates = load_exclusions_for_calendar(&mut conn, cal_name)
            .await
            .map_err(map_error)?;
        let skip_policy = SkipPolicy::from_db(&schedule.skip_policy);
        let exclude_weekends = calendar_excludes_weekends(cal_name);
        plan_backfill_with_calendar(
            parsed_schedule.as_ref(),
            request.from,
            request.to,
            max_count,
            &excluded_dates,
            skip_policy,
            exclude_weekends,
        )
        .map_err(limit_err)?
    } else {
        plan_backfill_timestamps(
            parsed_schedule.as_ref(),
            request.from,
            request.to,
            max_count,
        )
        .map_err(limit_err)?
        .into_iter()
        .map(|ts| (ts, ts))
        .collect()
    };
    // Filter out any timestamp pairs whose effective fire time is at or past
    // end_at (issue #478). This is a belt-and-suspenders guard: the exhaustion
    // check above already rejects schedules whose end_at has passed at request
    // time, but a tight end_at window could still contain some past-cutoff slots
    // among an otherwise valid batch.
    let timestamp_pairs: Vec<_> = if let Some(end_at) = schedule.end_at {
        timestamp_pairs
            .into_iter()
            .filter(|(_, ft)| *ft < end_at)
            .collect()
    } else {
        timestamp_pairs
    };

    // fire_times is the calendar-adjusted list used for display and dedup checks.
    let fire_times: Vec<chrono::DateTime<chrono::Utc>> =
        timestamp_pairs.iter().map(|(_, ft)| *ft).collect();

    let total = timestamp_pairs.len();
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
        // Workflow IDs are derived from original_slot (not fire_time), so duplicate
        // detection must use the same set of timestamps that dispatch will use.
        let original_slots: Vec<_> = timestamp_pairs.iter().map(|(orig, _)| *orig).collect();
        let already_exists =
            count_existing_in_window(&pool, &kind, schedule_id, &name, &original_slots).await;
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
            planned_timestamps: fire_times.clone(),
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
                planned_timestamps: fire_times.clone(),
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
            for (original_slot, _fire_time) in &timestamp_pairs {
                // Respect max_active_runs: skip if we've already saturated the limit.
                if running_at_start + dispatched_this_call >= max_active {
                    skipped += 1;
                    *skipped_reasons
                        .entry("max_active_runs".to_string())
                        .or_insert(0) += 1;
                    continue;
                }

                // Use original_slot (pre-calendar-rebase) for ID so that two distinct
                // logical slots that calendar-adjust to the same fire_time do not collide.
                let workflow_id = scheduled_workflow_id_pub(schedule_id, &wf_name, *original_slot);
                let legacy_workflow_id = {
                    let micros = original_slot.timestamp_subsec_micros();
                    if micros == 0 {
                        format!("sched:{}:{}", wf_name, original_slot.timestamp())
                    } else {
                        format!(
                            "sched:{}:{}.{:06}",
                            wf_name,
                            original_slot.timestamp(),
                            micros
                        )
                    }
                };
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
                    .filter(
                        harvest_workflow_executions::workflow_id
                            .eq(&workflow_id)
                            .or(harvest_workflow_executions::workflow_id.eq(&legacy_workflow_id)),
                    )
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

                let (owner, runbook_url, severity, info_sla, info_execution_timeout) = runtime
                    .registry
                    .workflows
                    .get(&wf_name)
                    .map_or((None, None, None, None, None), |info| {
                        (
                            info.owner,
                            info.runbook_url,
                            info.severity,
                            info.sla,
                            info.execution_timeout,
                        )
                    });
                let sla = clamp_info_default_sla(info_sla, info_execution_timeout);

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
                        priority: Priority::default(),
                        max_workflow_input_bytes: 0,
                        start_at: None,
                        delay: None,
                        max_workflow_start_delay: None,
                        owner,
                        runbook_url,
                        severity,
                        context_headers: None,
                        sla,
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

            for (original_slot, _fire_time) in &timestamp_pairs {
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
                // Use original_slot for ID, same as the workflow path above.
                let workflow_id = scheduled_workflow_id_pub(schedule_id, &dag_name, *original_slot);
                let legacy_workflow_id = {
                    let micros = original_slot.timestamp_subsec_micros();
                    if micros == 0 {
                        format!("sched:{}:{}", dag_name, original_slot.timestamp())
                    } else {
                        format!(
                            "sched:{}:{}.{:06}",
                            dag_name,
                            original_slot.timestamp(),
                            micros
                        )
                    }
                };
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
                    .filter(
                        harvest_workflow_executions::workflow_id
                            .eq(&workflow_id)
                            .or(harvest_workflow_executions::workflow_id.eq(&legacy_workflow_id)),
                    )
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

                let (owner, runbook_url, severity) =
                    runtime
                        .dags()
                        .get(&dag_name)
                        .map_or((None, None, None), |dag| {
                            (
                                dag.owner.as_deref(),
                                dag.runbook_url.as_deref(),
                                dag.severity.as_deref(),
                            )
                        });

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
                        priority: Priority::default(),
                        max_workflow_input_bytes: 0,
                        start_at: None,
                        delay: None,
                        max_workflow_start_delay: None,
                        owner,
                        runbook_url,
                        severity,
                        context_headers: None,

                        sla: None,
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
        planned_timestamps: fire_times,
        total,
        dispatched,
        skipped,
        failed,
        skipped_reasons,
        partial_shard_failures: shard_failures,
        paused_schedule_warning,
    }))
}

/// Count active (RUNNING or PAUSED) workflow executions or DAG runs for the
/// named entity. A PAUSED run still occupies an active slot for
/// `max_active_runs`/overlap enforcement (issue #383), matching the scheduler.
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
            .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
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
    schedule_id: uuid::Uuid,
    name: &str,
    timestamps: &[chrono::DateTime<chrono::Utc>],
) -> usize {
    if timestamps.is_empty() {
        return 0;
    }
    let mut total = 0usize;
    match kind {
        ScheduleKind::Workflow => {
            let mut workflow_ids: Vec<String> = timestamps
                .iter()
                .map(|ts| scheduled_workflow_id_pub(schedule_id, name, *ts))
                .collect();
            let legacy_ids: Vec<String> = timestamps
                .iter()
                .map(|ts| {
                    let micros = ts.timestamp_subsec_micros();
                    if micros == 0 {
                        format!("sched:{}:{}", name, ts.timestamp())
                    } else {
                        format!("sched:{}:{}.{:06}", name, ts.timestamp(), micros)
                    }
                })
                .collect();
            workflow_ids.extend(legacy_ids);
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
            let mut workflow_ids: Vec<String> = timestamps
                .iter()
                .map(|ts| scheduled_workflow_id_pub(schedule_id, name, *ts))
                .collect();
            let legacy_ids: Vec<String> = timestamps
                .iter()
                .map(|ts| {
                    let micros = ts.timestamp_subsec_micros();
                    if micros == 0 {
                        format!("sched:{}:{}", name, ts.timestamp())
                    } else {
                        format!("sched:{}:{}.{:06}", name, ts.timestamp(), micros)
                    }
                })
                .collect();
            workflow_ids.extend(legacy_ids);
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
    let (schedule, _shard) = load_schedule_by_id_with_shard(api_state, schedule_id).await?;
    Ok(schedule)
}

/// Like [`load_schedule_by_id`] but also returns the [`ShardId`] of the shard
/// the schedule was found on, so callers can route subsequent writes correctly.
async fn load_schedule_by_id_with_shard(
    api_state: &HarvestApiState,
    schedule_id: uuid::Uuid,
) -> Result<(HarvestSchedule, ShardId), AutumnError> {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let pool = api_state.storage_pool().map_err(map_error)?;
    for (shard, shard_pool) in pool.iter_shards() {
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
            return Ok((r, shard));
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
    let schedule = if let Some(rest) = trimmed.strip_prefix("cron_tz:") {
        // Canonical round-trip format emitted by GET /admin/schedules.
        // The embedded timezone takes precedence over the separate `timezone` field.
        let (tz, cron_expr) = rest.split_once(':').ok_or_else(|| {
            format!("malformed cron_tz expression '{expr}'; expected 'cron_tz:<tz>:<expr>'")
        })?;
        Schedule::CronInTimezone {
            expr: cron_expr.to_string(),
            tz: tz.to_string(),
        }
    } else if let Some(cron) = trimmed.strip_prefix("cron:") {
        let cron_expr = cron.trim().to_string();
        if timezone == "UTC" {
            Schedule::Cron(cron_expr)
        } else {
            Schedule::CronInTimezone {
                expr: cron_expr,
                tz: timezone.to_string(),
            }
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
            Schedule::CronInTimezone {
                expr: trimmed.to_string(),
                tz: timezone.to_string(),
            }
        }
    };
    // Validate cron expressions eagerly (including timezone names) so callers
    // receive a 400 rather than silently persisting an expression that will
    // never fire or an unknown timezone that would misfire.
    autumn_harvest::validate_schedule(&schedule)?;
    Ok(schedule)
}

#[derive(Debug, serde::Serialize)]
struct DeadLetterResponse {
    #[serde(flatten)]
    dead_letter: DeadLetter,
    runbook_url: Option<String>,
}

async fn list_dead_letters(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<DeadLetterListQuery>,
) -> Result<Json<Vec<DeadLetterResponse>>, AutumnError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let dead_letters =
        load_dead_letters_from_shards(&api_state, limit, query.owner.as_deref()).await?;

    let mut runbooks = std::collections::HashMap::new();
    let exec_ids: Vec<uuid::Uuid> = dead_letters
        .iter()
        .filter_map(|dl| dl.workflow_exec_id)
        .collect();

    if !exec_ids.is_empty() {
        let pool = api_state.storage_pool().map_err(map_error)?;
        let mut by_shard: std::collections::HashMap<_, Vec<uuid::Uuid>> =
            std::collections::HashMap::new();
        for id in exec_ids {
            let exec_id = ExecutionId::from_uuid(id);
            let shard = exec_id.shard();
            by_shard.entry(shard).or_default().push(id);
        }

        for (shard, ids) in by_shard {
            use autumn_harvest::schema::harvest_workflow_executions::dsl as wf_dsl;
            let shard_pool = pool.pool_for(shard);
            let mut conn = acquire_conn(shard_pool).await?;
            let rows: Vec<(uuid::Uuid, Option<String>)> = wf_dsl::harvest_workflow_executions
                .filter(wf_dsl::id.eq_any(&ids))
                .select((wf_dsl::id, wf_dsl::runbook_url))
                .load::<(uuid::Uuid, Option<String>)>(&mut conn)
                .await
                .map_err(HarvestError::from)
                .map_err(map_error)?;
            for (id, url) in rows {
                if let Some(u) = url {
                    runbooks.insert(id, u);
                }
            }
        }
    }

    let responses: Vec<DeadLetterResponse> = dead_letters
        .into_iter()
        .map(|dl| {
            let runbook_url = dl
                .workflow_exec_id
                .and_then(|id| runbooks.get(&id).cloned());
            DeadLetterResponse {
                dead_letter: dl,
                runbook_url,
            }
        })
        .collect();

    Ok(Json(responses))
}

/// `GET /dead-letters/aggregate` — root-cause aggregation for DLQ triage
/// (issue #385).
///
/// Groups dead-letter rows along a small named set of dimensions (`workflow_name`,
/// `activity_name`, `queue_name`, `task_type`, `time_bucket`, `failure_signature`)
/// and returns per-group counts plus a handful of representative
/// `dead_letter_id`s, fanning out across shards and merging the partials.
async fn aggregate_dead_letters(
    Extension(api_state): Extension<HarvestApiState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Result<Json<dlq::DlqAggregateResponse>, AutumnError> {
    let params = dlq::DlqAggregateParams::from_query_pairs(&pairs, chrono::Utc::now())
        .map_err(AutumnError::bad_request_msg)?;

    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut partials = Vec::new();
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let partial = dlq::aggregate_dead_letters(&mut conn, &params)
            .await
            .map_err(map_error)?;
        partials.push(partial);
    }

    Ok(Json(dlq::merge_dlq_aggregates(&params, partials)))
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

// ── Rate Limit Management ──────────────────────────────────────────────────

async fn list_rate_limits(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<RateLimitBucket>>, AutumnError> {
    use autumn_harvest::schema::harvest_rate_limit_buckets::dsl::harvest_rate_limit_buckets;

    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;

    let buckets = harvest_rate_limit_buckets
        .select(RateLimitBucket::as_select())
        .load::<RateLimitBucket>(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    Ok(Json(buckets))
}

#[derive(Debug, Deserialize)]
struct SetRateLimitRequest {
    refill_rate: f64,
    burst: f64,
}

async fn set_rate_limit(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(key_param): Path<String>,
    Json(request): Json<SetRateLimitRequest>,
) -> Result<Json<BasicAck>, AutumnError> {
    if request.refill_rate <= 0.0 {
        return Err(AutumnError::bad_request_msg(
            "refill_rate must be greater than zero",
        ));
    }
    if request.burst < 1.0 {
        return Err(AutumnError::bad_request_msg("burst must be at least 1.0"));
    }

    let (actor, source, request_id) = audit_context(&headers, &api_state);
    let route = "POST /admin/rate-limits/{key}";

    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;

    let query_result = diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at, created_at, updated_at) \
         VALUES ($1, $2, $3, $3, NOW(), NOW(), NOW()) \
         ON CONFLICT (key) DO UPDATE \
         SET refill_rate = EXCLUDED.refill_rate, \
             burst = EXCLUDED.burst, \
             tokens = LEAST(EXCLUDED.burst, harvest_rate_limit_buckets.tokens), \
             last_refilled_at = NOW(), \
             updated_at = NOW()"
    )
    .bind::<diesel::sql_types::Text, _>(&key_param)
    .bind::<diesel::sql_types::Double, _>(request.refill_rate)
    .bind::<diesel::sql_types::Double, _>(request.burst)
    .execute(&mut conn)
    .await;

    if let Err(e) = query_result {
        let err_str = e.to_string();
        let ar = NewAuditRecord {
            actor: &actor,
            operation: "rate_limit_override",
            target_type: "rate_limit",
            target_id: Some(&key_param),
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_FAILED,
            error_summary: Some(err_str.as_str()),
            shard_id: None,
            source: &source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
        Err(map_error(database_error(e)))
    } else {
        let ar = NewAuditRecord {
            actor: &actor,
            operation: "rate_limit_override",
            target_type: "rate_limit",
            target_id: Some(&key_param),
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: &source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
        Ok(Json(BasicAck { ok: true }))
    }
}

// ── Circuit Breaker Management (issue #369) ─────────────────────────────────

/// `GET /admin/circuits` — current state of every activity circuit breaker.
///
/// Reflects the in-process, per-shard breaker state observed by this runtime's
/// worker (closed/open/half-open, last-trip timestamp, rolling failure count,
/// and time-until-probe). Activities without a declared
/// [`CircuitBreakerPolicy`](autumn_harvest::policy::CircuitBreakerPolicy) are
/// omitted.
async fn list_circuits(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Json<Vec<autumn_harvest::circuit_breaker::CircuitSnapshot>>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let breakers = runtime.registry().circuit_breakers();
    Ok(Json(breakers.list(std::time::Instant::now())))
}

/// `GET /admin/circuits/{activity_name}` — state of a single breaker.
///
/// Returns `404` when the activity has no circuit-breaker policy.
async fn get_circuit(
    Extension(api_state): Extension<HarvestApiState>,
    Path(activity_name): Path<String>,
) -> Result<Json<autumn_harvest::circuit_breaker::CircuitSnapshot>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let breakers = runtime.registry().circuit_breakers();
    breakers
        .snapshot(&activity_name, std::time::Instant::now())
        .map(Json)
        .ok_or_else(|| {
            AutumnError::not_found_msg(format!(
                "no circuit breaker configured for activity '{activity_name}'"
            ))
        })
}

/// `POST /admin/circuits/{activity_name}/force-open` — operator pins the breaker
/// open for manual incident response. Returns `404` when the activity has no
/// circuit-breaker policy.
async fn force_open_circuit(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(activity_name): Path<String>,
) -> Result<Json<autumn_harvest::circuit_breaker::CircuitSnapshot>, AutumnError> {
    force_circuit(&api_state, &headers, &activity_name, true).await
}

/// `POST /admin/circuits/{activity_name}/force-close` — operator clears any pin
/// and resets the breaker to closed so normal tracking resumes. Returns `404`
/// when the activity has no circuit-breaker policy.
async fn force_close_circuit(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(activity_name): Path<String>,
) -> Result<Json<autumn_harvest::circuit_breaker::CircuitSnapshot>, AutumnError> {
    force_circuit(&api_state, &headers, &activity_name, false).await
}

async fn force_circuit(
    api_state: &HarvestApiState,
    headers: &axum::http::HeaderMap,
    activity_name: &str,
    open: bool,
) -> Result<Json<autumn_harvest::circuit_breaker::CircuitSnapshot>, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let breakers = runtime.registry().circuit_breakers();
    if !breakers.has_policy(activity_name) {
        return Err(AutumnError::not_found_msg(format!(
            "no circuit breaker configured for activity '{activity_name}'"
        )));
    }
    let now = std::time::Instant::now();
    if open {
        breakers.force_open(activity_name, now);
    } else {
        breakers.force_close(activity_name);
    }

    // Audit the manual action (best-effort): circuit state itself is in-process,
    // but the operator decision belongs in the audit trail (#158).
    let (actor, source, request_id) = audit_context(headers, api_state);
    let (operation, route) = if open {
        (
            OP_CIRCUIT_FORCE_OPEN,
            "POST /admin/circuits/{activity_name}/force-open",
        )
    } else {
        (
            OP_CIRCUIT_FORCE_CLOSE,
            "POST /admin/circuits/{activity_name}/force-close",
        )
    };
    if let Ok(pool) = api_state.storage_pool()
        && let Ok(mut conn) = pool.default_pool().get().await
    {
        let ar = NewAuditRecord {
            actor: &actor,
            operation,
            target_type: TARGET_CIRCUIT,
            target_id: Some(activity_name),
            route_or_command: route,
            request_id: request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: &source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
    }

    breakers
        .snapshot(activity_name, now)
        .map(Json)
        .ok_or_else(|| {
            AutumnError::not_found_msg(format!(
                "no circuit breaker configured for activity '{activity_name}'"
            ))
        })
}

// ---------------------------------------------------------------------------
// Worker-pool scaling signal and Prometheus metrics (KEDA/HPA autoscalers)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ScalingQuery {
    format: Option<String>,
}

async fn queues_scaling_signal(
    Extension(api_state): Extension<HarvestApiState>,
    Query(query): Query<ScalingQuery>,
) -> Result<axum::response::Response, AutumnError> {
    let signals = get_aggregated_scaling_signals(&api_state).await?;

    if query.format.as_deref() == Some("prometheus") {
        let body = format_prometheus_metrics(&signals);
        return Ok((
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response());
    }

    Ok(Json(signals).into_response())
}

async fn prometheus_metrics(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<axum::response::Response, AutumnError> {
    let signals = get_aggregated_scaling_signals(&api_state).await?;
    let body = format_prometheus_metrics(&signals);
    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response())
}

fn format_prometheus_metrics(signals: &[::autumn_harvest::queue::QueueScalingSignal]) -> String {
    let mut out = String::new();

    // 1. backlog
    writeln!(out, "# HELP harvest_queue_backlog Count of pending tasks ready for execution (scheduled_at <= NOW)").unwrap();
    writeln!(out, "# TYPE harvest_queue_backlog gauge").unwrap();
    for sig in signals {
        let escaped_queue = sig.queue.replace('\\', "\\\\").replace('"', "\\\"");
        writeln!(
            out,
            "harvest_queue_backlog{{queue=\"{}\"}} {}",
            escaped_queue, sig.backlog
        )
        .unwrap();
    }

    // 2. in_flight
    writeln!(
        out,
        "# HELP harvest_queue_in_flight Count of currently executing tasks"
    )
    .unwrap();
    writeln!(out, "# TYPE harvest_queue_in_flight gauge").unwrap();
    for sig in signals {
        let escaped_queue = sig.queue.replace('\\', "\\\\").replace('"', "\\\"");
        writeln!(
            out,
            "harvest_queue_in_flight{{queue=\"{}\"}} {}",
            escaped_queue, sig.in_flight
        )
        .unwrap();
    }

    // 3. scheduled
    writeln!(
        out,
        "# HELP harvest_queue_scheduled Count of future-scheduled tasks (scheduled_at > NOW)"
    )
    .unwrap();
    writeln!(out, "# TYPE harvest_queue_scheduled gauge").unwrap();
    for sig in signals {
        let escaped_queue = sig.queue.replace('\\', "\\\\").replace('"', "\\\"");
        writeln!(
            out,
            "harvest_queue_scheduled{{queue=\"{}\"}} {}",
            escaped_queue, sig.scheduled
        )
        .unwrap();
    }

    // 4. active_workers
    writeln!(out, "# HELP harvest_queue_active_workers Count of healthy, non-draining worker processes polling this queue").unwrap();
    writeln!(out, "# TYPE harvest_queue_active_workers gauge").unwrap();
    for sig in signals {
        let escaped_queue = sig.queue.replace('\\', "\\\\").replace('"', "\\\"");
        writeln!(
            out,
            "harvest_queue_active_workers{{queue=\"{}\"}} {}",
            escaped_queue, sig.active_workers
        )
        .unwrap();
    }

    out
}

async fn get_aggregated_scaling_signals(
    api_state: &HarvestApiState,
) -> Result<Vec<::autumn_harvest::queue::QueueScalingSignal>, AutumnError> {
    use ::autumn_harvest::models::HarvestWorker;
    use ::autumn_harvest::schema::harvest_workers;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let pool = api_state.storage_pool().map_err(map_error)?;
    let stale_threshold = api_state.worker_stale_threshold();

    // We'll group stats by queue name in-memory
    let mut task_stats: std::collections::HashMap<
        String,
        ::autumn_harvest::queue::QueueTaskCounts,
    > = std::collections::HashMap::new();
    let mut worker_stats: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;

        // 1. Fetch task counts per queue on this shard
        let shard_counts = ::autumn_harvest::queue::queue_task_counts(&mut conn)
            .await
            .map_err(map_error)?;
        for stat in shard_counts {
            let entry = task_stats.entry(stat.queue.clone()).or_insert_with(|| {
                ::autumn_harvest::queue::QueueTaskCounts {
                    queue: stat.queue.clone(),
                    backlog: 0,
                    in_flight: 0,
                    scheduled: 0,
                }
            });
            entry.backlog += stat.backlog;
            entry.in_flight += stat.in_flight;
            entry.scheduled += stat.scheduled;
        }

        // 2. Fetch workers on this shard
        let workers = harvest_workers::table
            .select(HarvestWorker::as_select())
            .load::<HarvestWorker>(&mut conn)
            .await
            .map_err(|e| map_error(::autumn_harvest::error::database_error(e)))?;

        for w in workers {
            let health = ::autumn_harvest::workers::WorkerHealth::classify(
                w.last_heartbeat_at,
                stale_threshold,
            );
            let is_active = w.status == ::autumn_harvest::workers::WorkerStatus::Active.as_str()
                && health == ::autumn_harvest::workers::WorkerHealth::Healthy;
            let queues = if is_active { w.queues.as_array() } else { None };
            if let Some(queues) = queues {
                for q in queues {
                    if let Some(name) = q.as_str() {
                        *worker_stats.entry(name.to_string()).or_default() += 1;
                    }
                }
            }
        }
    }

    // Merge task stats and worker stats
    let mut all_queues: std::collections::HashSet<String> = std::collections::HashSet::new();
    all_queues.extend(task_stats.keys().cloned());
    all_queues.extend(worker_stats.keys().cloned());

    let mut signals = Vec::new();
    for q in all_queues {
        let task_stat = task_stats.get(&q);
        let active_workers = worker_stats.get(&q).copied().unwrap_or(0);
        signals.push(::autumn_harvest::queue::QueueScalingSignal {
            queue: q.clone(),
            backlog: task_stat.map_or(0, |s| s.backlog),
            in_flight: task_stat.map_or(0, |s| s.in_flight),
            scheduled: task_stat.map_or(0, |s| s.scheduled),
            active_workers,
        });
    }

    signals.sort_by(|a, b| a.queue.cmp(&b.queue));
    Ok(signals)
}

// ---------------------------------------------------------------------------
// PATCH /tasks/{id} — re-prioritize a pending task (issue #249)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PatchTaskPriorityRequest {
    priority: Priority,
}

#[derive(Debug, Serialize)]
struct PatchTaskPriorityResponse {
    task_id: uuid::Uuid,
    priority: String,
    /// `true` when the task row was updated; `false` when the task is in a
    /// terminal state (the row still exists but is no longer claimable).
    updated: bool,
}

async fn patch_task_priority(
    Extension(api_state): Extension<HarvestApiState>,
    Path(task_id_str): Path<String>,
    Json(request): Json<PatchTaskPriorityRequest>,
) -> Result<impl IntoResponse, AutumnError> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid task id '{task_id_str}'")))?;

    let pool = api_state.storage_pool().map_err(map_error)?;

    // First pass: attempt the update across all shards.
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let updated =
            autumn_harvest::queue::update_task_priority(&mut conn, task_id, request.priority)
                .await
                .map_err(map_error)?;

        if updated {
            return Ok((
                StatusCode::OK,
                Json(PatchTaskPriorityResponse {
                    task_id,
                    priority: request.priority.to_string(),
                    updated: true,
                }),
            )
                .into_response());
        }
    }

    // No shard updated the row. Check whether the task exists at all (terminal
    // state) or is simply not present (404).
    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let exists = autumn_harvest::queue::task_exists(&mut conn, task_id)
            .await
            .map_err(map_error)?;

        if exists {
            return Ok((
                StatusCode::OK,
                Json(PatchTaskPriorityResponse {
                    task_id,
                    priority: request.priority.to_string(),
                    updated: false,
                }),
            )
                .into_response());
        }
    }

    Err(AutumnError::not_found_msg(format!("task {task_id}")))
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

pub(crate) async fn db_conn_for_shard(
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
    if let Some(owner) = &filters.owner {
        query = query.filter(harvest_workflow_executions::owner.eq(owner.clone()));
    }
    if let Some(severity) = &filters.severity {
        query = query.filter(harvest_workflow_executions::severity.eq(severity.clone()));
    }
    // Each search_attr filter contributes its own `search_attrs @> {...}` predicate.
    // The `@>` operator hits the existing `idx_harvest_we_search` GIN index on
    // `search_attrs`; ANDing predicates means repeated keys narrow the result set.
    for predicate in &filters.search_attrs {
        query = query.filter(sql::<Bool>("search_attrs @> ").bind::<Jsonb, _>(predicate.clone()));
    }
    if let Some(cause) = &filters.failure_cause {
        let predicate = serde_json::json!({ "failure_cause": cause });
        query = query.filter(sql::<Bool>("search_attrs @> ").bind::<Jsonb, _>(predicate));
    }
    if filters.sla_breached {
        query = query.filter(harvest_workflow_executions::sla_breached.eq(true));
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

/// Per-shard stall-discovery query (issue #486).
///
/// Returns executions in a non-terminal state whose most recent
/// `harvest_events` row is older than `no_progress_minutes`. By default,
/// executions whose only pending work is a future-dated durable timer are
/// excluded (correctly sleeping ≠ stalled); `include_sleeping = true` opts
/// them back in. Executions with overdue unfired timers are always included
/// regardless of `include_sleeping`.
///
/// Implementation uses five queries per shard:
/// 1. Stalled candidates — Diesel boxed query with a raw `NOT EXISTS` filter
///    for the age check (efficient via `idx_harvest_events_exec_last`)
/// 2. Batch last-event-at via `GROUP BY MAX(timestamp)`
/// 3. Pending activity, child, signal, and future-timer sets via `.eq_any`
#[allow(clippy::too_many_lines)]
pub(crate) async fn load_stalled_workflows(
    conn: &mut AsyncPgConnection,
    filters: &WorkflowFilters,
) -> HarvestResult<Vec<StalledWorkflowRow>> {
    use diesel::dsl::{max, sql};
    use diesel::sql_types::{BigInt, Bool};
    use std::collections::{HashMap, HashSet};

    let Some(minutes) = filters.no_progress_minutes else {
        return Ok(vec![]);
    };

    // PAUSED executions are intentionally blocked from making progress at the
    // claim layer, so after N minutes they are always "stalled" — a guaranteed
    // false positive.  Exclude them from the default scan; callers that want to
    // surface long-running pauses can filter explicitly with state=PAUSED.
    let active_states: Vec<&str> = if filters.states.is_empty() {
        vec!["RUNNING", "SUSPENDED"]
    } else {
        filters
            .states
            .iter()
            .map(String::as_str)
            .filter(|s| matches!(*s, "RUNNING" | "SUSPENDED" | "PAUSED"))
            .collect()
    };
    if active_states.is_empty() {
        return Ok(vec![]);
    }

    // ── Step 1: find stalled candidate executions ──────────────────────────
    // No per-shard limit here; the global limit is applied after cross-shard
    // sorting in load_stalled_workflows_from_shards, so oldest stalls are
    // never dropped by a premature per-shard truncation.
    let mut query = harvest_workflow_executions::table
        .into_boxed()
        .filter(harvest_workflow_executions::state.eq_any(active_states))
        // No event newer than N minutes — O(1) per candidate with the covering index.
        // Qualify the outer table's id to avoid ambiguity with harvest_events.id (Int8).
        .filter(
            sql::<Bool>(
                "NOT EXISTS (\
                    SELECT 1 FROM harvest_events \
                    WHERE workflow_exec_id = harvest_workflow_executions.id \
                    AND timestamp >= NOW() - ",
            )
            .bind::<BigInt, _>(minutes)
            .sql(" * INTERVAL '1 minute')"),
        )
        .order(harvest_workflow_executions::created_at.desc());

    if let Some(name) = &filters.workflow_name {
        query = query.filter(harvest_workflow_executions::workflow_name.eq(name.as_str()));
    }
    if let Some(owner) = &filters.owner {
        query = query.filter(harvest_workflow_executions::owner.eq(owner.as_str()));
    }
    if let Some(severity) = &filters.severity {
        query = query.filter(harvest_workflow_executions::severity.eq(severity.as_str()));
    }
    // Honor the soft-SLA filter on the stalled path too (issue #487): without
    // this, `?sla_breached=true&no_progress_minutes=N` would return unbreached
    // stalled rows because this loader bypasses `load_workflows`.
    if filters.sla_breached {
        query = query.filter(harvest_workflow_executions::sla_breached.eq(true));
    }

    if !filters.include_sleeping {
        // Include an execution if it has any non-timer pending work, OR has no
        // future-dated unfired timer (nothing to sleep on), OR has an overdue
        // unfired timer (should have progressed).  The only excluded case is
        // "correctly sleeping": a future-dated timer is the sole pending item
        // and no timers are overdue.
        query = query.filter(sql::<Bool>(
            "(\
                EXISTS(\
                    SELECT 1 FROM harvest_task_queue \
                    WHERE workflow_exec_id = harvest_workflow_executions.id \
                    AND state IN ('PENDING','CLAIMED','RUNNING','BACKOFF')\
                ) \
             OR EXISTS(\
                    SELECT 1 FROM harvest_workflow_executions c \
                    WHERE c.parent_id = harvest_workflow_executions.id \
                    AND c.state NOT IN (\
                        'COMPLETED','FAILED','CANCELLED',\
                        'TIMED_OUT','CONTINUED_AS_NEW','TERMINATED'\
                    )\
                ) \
             OR EXISTS(\
                    SELECT 1 FROM harvest_signals \
                    WHERE workflow_exec_id = harvest_workflow_executions.id AND consumed = false\
                ) \
             OR NOT EXISTS(\
                    SELECT 1 FROM harvest_timers \
                    WHERE workflow_exec_id = harvest_workflow_executions.id \
                    AND fired = false AND fires_at > NOW()\
                )\
             OR EXISTS(\
                    SELECT 1 FROM harvest_timers \
                    WHERE workflow_exec_id = harvest_workflow_executions.id \
                    AND fired = false AND fires_at <= NOW()\
                )\
            )",
        ));
    }

    let candidates: Vec<WorkflowExecution> = query
        .select(WorkflowExecution::as_select())
        .load(conn)
        .await
        .map_err(database_error)?;

    if candidates.is_empty() {
        return Ok(vec![]);
    }

    let exec_ids: Vec<uuid::Uuid> = candidates.iter().map(|e| e.id).collect();

    // ── Step 2: batch-fetch last_event_at per execution ────────────────────
    let last_event_ats: HashMap<uuid::Uuid, chrono::DateTime<chrono::Utc>> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq_any(&exec_ids))
        .group_by(harvest_events::workflow_exec_id)
        .select((
            harvest_events::workflow_exec_id,
            max(harvest_events::timestamp),
        ))
        .load::<(uuid::Uuid, Option<chrono::DateTime<chrono::Utc>>)>(conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .filter_map(|(id, ts)| ts.map(|t| (id, t)))
        .collect();

    // ── Step 3: any runnable task queue row (activity or workflow type) ────────
    // Checking all task_type values mirrors the sleeping-filter predicate so that
    // an execution with a stuck workflow task is not mislabelled no_pending_work.
    let has_activity: HashSet<uuid::Uuid> = harvest_task_queue::table
        .filter(
            harvest_task_queue::workflow_exec_id
                .eq_any(exec_ids.iter().map(|id| Some(*id)).collect::<Vec<_>>()),
        )
        .filter(harvest_task_queue::state.eq_any(["PENDING", "CLAIMED", "RUNNING", "BACKOFF"]))
        .select(harvest_task_queue::workflow_exec_id)
        .distinct()
        .load::<Option<uuid::Uuid>>(conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .flatten()
        .collect();

    // ── Step 4: non-terminal child workflows ────────────────────────────────
    let has_child: HashSet<uuid::Uuid> = harvest_workflow_executions::table
        .filter(
            harvest_workflow_executions::parent_id
                .eq_any(exec_ids.iter().map(|id| Some(*id)).collect::<Vec<_>>()),
        )
        .filter(harvest_workflow_executions::state.ne_all([
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
        ]))
        .select(harvest_workflow_executions::parent_id)
        .distinct()
        .load::<Option<uuid::Uuid>>(conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .flatten()
        .collect();

    // ── Step 5: unconsumed (buffered) signals ───────────────────────────────
    let has_signal: HashSet<uuid::Uuid> = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq_any(&exec_ids))
        .filter(harvest_signals::consumed.eq(false))
        .select(harvest_signals::workflow_exec_id)
        .distinct()
        .load::<uuid::Uuid>(conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .collect();

    // Capture now once — reused for timer comparison and age calculation to
    // ensure temporal consistency across both.
    let now = chrono::Utc::now();

    // ── Step 6: any unfired timer (future-dated OR overdue) ─────────────────
    // The sleeping-filter SQL already restricts "correctly sleeping" to
    // fires_at > NOW(), so overdue timers are never excluded from candidates.
    // Checking fired = false without a fires_at bound ensures those executions
    // are classified as SleepingTimer rather than falling through to NoPendingWork.
    let has_unfired_timer: HashSet<uuid::Uuid> = harvest_timers::table
        .filter(harvest_timers::workflow_exec_id.eq_any(&exec_ids))
        .filter(harvest_timers::fired.eq(false))
        .select(harvest_timers::workflow_exec_id)
        .distinct()
        .load::<uuid::Uuid>(conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .collect();

    Ok(candidates
        .into_iter()
        .map(|execution| {
            let id = execution.id;
            let last_event_at = last_event_ats.get(&id).copied();
            let last_event_age_seconds =
                last_event_at.map(|ts| (now - ts).to_std().map_or(0.0, |d| d.as_secs_f64()));
            let stall_reason = Some(if has_activity.contains(&id) {
                StallReason::PendingActivity
            } else if has_child.contains(&id) {
                StallReason::PendingChild
            } else if has_signal.contains(&id) {
                StallReason::AwaitingSignal
            } else if has_unfired_timer.contains(&id) {
                StallReason::SleepingTimer
            } else {
                StallReason::NoPendingWork
            });
            StalledWorkflowRow {
                execution,
                last_event_at,
                last_event_age_seconds,
                stall_reason,
            }
        })
        .collect())
}

/// Fan-out `load_stalled_workflows` across all configured shards and merge
/// results sorted oldest-stall-first, truncated to `filters.limit`.
pub(crate) async fn load_stalled_workflows_from_shards(
    api_state: &HarvestApiState,
    filters: &WorkflowFilters,
) -> Result<Vec<StalledWorkflowRow>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut rows: Vec<StalledWorkflowRow> = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut shard_rows = load_stalled_workflows(&mut conn, filters)
            .await
            .map_err(map_error)?;
        rows.append(&mut shard_rows);
    }

    // Sort oldest-stall-first: the most actionable workflows appear at the top.
    rows.sort_by(|a, b| {
        a.last_event_at
            .cmp(&b.last_event_at)
            .then_with(|| a.execution.id.cmp(&b.execution.id))
    });
    rows.truncate(usize::try_from(filters.limit).unwrap_or(usize::MAX));
    Ok(rows)
}

fn export_history_for_execution(
    execution: &WorkflowExecution,
    events: Vec<WorkflowEvent>,
    query: &HistoryExportQuery,
) -> Result<HistoryExportDocument, HistoryExportError> {
    let context_headers = execution.context_headers.as_ref().and_then(|v| {
        serde_json::from_value::<std::collections::HashMap<String, String>>(v.clone()).ok()
    });
    export_history(HistoryExportRequest {
        workflow_name: execution.workflow_name.clone(),
        execution_id: ExecutionId::from_uuid(execution.id),
        shard_id: execution.shard_id,
        state: execution.state.clone(),
        events,
        exported_at: chrono::Utc::now(),
        payload_policy: query.payload_policy,
        max_bytes: Some(query.max_bytes),
        context_headers,
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
        context_headers: None,
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
    owner: Option<&str>,
) -> Result<Vec<DeadLetter>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut dead_letters = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let mut rows = dlq::list_dead_letters(&mut conn, limit, owner)
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

fn check_signal_payload_cap(payload: &Value, cap: u64) -> Result<(), AutumnError> {
    let observed_bytes = serde_json::to_string(payload).map_or(0, |s| s.len() as u64);
    if cap > 0 && observed_bytes > cap {
        return Err(map_error(autumn_harvest::HarvestError::PayloadTooLarge {
            kind: autumn_harvest::error::PayloadKind::SignalPayload,
            observed_bytes,
            cap_bytes: cap,
            workflow_type: String::new(),
            activity_name: None,
        }));
    }
    Ok(())
}

// ── SSE execution event stream (issue #324) ──────────────────────────────────

/// Whether an event type name signals a terminal lifecycle transition.
fn is_terminal_event_type(event_type: &str) -> bool {
    matches!(
        event_type,
        "WorkflowCompleted"
            | "WorkflowFailed"
            | "WorkflowCancelled"
            | "WorkflowContinuedAsNew"
            | "WorkflowResetTerminated"
            | "WorkflowExecutionTimedOut"
    )
}

/// Map a terminal `WorkflowEvent` type name to a short state string.
fn terminal_event_type_to_state(event_type: &str) -> &'static str {
    match event_type {
        "WorkflowCompleted" => "completed",
        "WorkflowFailed" => "failed",
        "WorkflowCancelled" => "cancelled",
        "WorkflowContinuedAsNew" => "continued-as-new",
        "WorkflowResetTerminated" => "terminated",
        "WorkflowExecutionTimedOut" => "timed-out",
        _ => "terminal",
    }
}

/// `GET /executions/{exec_id}/events/stream`
///
/// Returns a `text/event-stream` response that tails every `WorkflowEvent`
/// appended to `harvest_events` for the given execution.  Uses Postgres
/// LISTEN/NOTIFY for sub-second delivery without holding a DB connection
/// between notifications (issue #324).
///
/// SSE wire format:
/// ```text
/// id: <harvest_events.id BIGSERIAL>
/// event: <WorkflowEvent::type_name()>
/// data: <JSON event value>
///
/// ```
///
/// Resume: send `Last-Event-ID: <id>` to replay events with `id > n` before
/// switching to live-tail mode.
///
/// Keepalive: `: ping\n\n` comments every `sse_keepalive_interval` (default 15 s).
///
/// Stream termination: `event: stream-end` followed by HTTP close when the
/// execution reaches a terminal state.
///
/// Backpressure: if the client reconnects with a `Last-Event-ID` that implies
/// more than `sse_buffer_depth` events to replay, returns `409 Conflict`.
#[allow(clippy::too_many_lines)]
async fn stream_execution_events(
    Extension(api_state): Extension<HarvestApiState>,
    Path(exec_id_raw): Path<String>,
    headers: axum::http::HeaderMap,
    session: Option<axum::extract::Extension<Session>>,
) -> axum::response::Response {
    use autumn_harvest::audit::{OP_EXECUTION_STREAM_OPEN, STATUS_SUCCEEDED, TARGET_WORKFLOW};
    use autumn_harvest::models::NewAuditRecord;
    use autumn_harvest::notify::{WorkflowEventListener, WorkflowEventWaitOutcome};
    use axum::response::sse::{Event, KeepAlive, Sse};
    use futures::SinkExt as _;

    // Parse execution ID
    let exec_id = match parse_execution_id(&exec_id_raw) {
        Ok(id) => id,
        Err(e) => return e.into_response(),
    };

    // Auth check — rejects unauthenticated requests with 401 (issue #174)
    if !has_harvest_admin_access(&api_state, session.map(|s| s.0)).await {
        return AutumnError::unauthorized_msg("authentication required").into_response();
    }

    // Extract Last-Event-ID for resume (harvest_events.id BIGSERIAL cursor).
    // An absent header means "start from the beginning" (cursor = -1).
    // A present but non-parseable value is a client error → 400.
    let last_row_id: i64 = match headers.get("last-event-id").and_then(|v| v.to_str().ok()) {
        None => -1,
        Some(s) => match s.parse::<i64>() {
            Ok(n) => n,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "invalid_last_event_id",
                        "message": "Last-Event-ID must be a valid i64"
                    })),
                )
                    .into_response();
            }
        },
    };

    // Resolve the LISTEN/NOTIFY database URL for this execution's shard
    let shard = exec_id.shard();
    let notification_url = match api_state.sse_notification_url(shard) {
        Ok(url) => url,
        Err(e) => return map_error(e).into_response(),
    };

    // Establish LISTEN connection before the backfill query to avoid the
    // race where new events are committed between the query and LISTEN setup
    let listener = match WorkflowEventListener::connect(&notification_url).await {
        Ok(l) => l,
        Err(e) => return map_error(e).into_response(),
    };

    // Get a pooled connection for the initial verification and backfill
    let mut conn = match db_conn_for_execution(&api_state, exec_id).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    // Verify the execution exists
    let execution = match load_execution(&mut conn, exec_id).await {
        Ok(e) => e,
        Err(e) => return map_error(e).into_response(),
    };

    // Load backfill events, capped at buffer_depth + 1. Fetching one extra lets
    // us distinguish "exactly buffer_depth events" from "client is too far behind"
    // without loading an unbounded history into memory.
    let buffer_depth = api_state.sse_buffer_depth();
    let backfill = match store::load_events_after_row_id(
        &mut conn,
        exec_id,
        last_row_id,
        i64::try_from(buffer_depth + 1).ok(),
    )
    .await
    {
        Ok(rows) => rows,
        Err(e) => return map_error(e).into_response(),
    };

    // Slow-consumer check: if reconnecting client is too far behind, return 409
    if backfill.len() > buffer_depth {
        let drop_id = backfill.last().map_or(last_row_id, |r| r.id);
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "slow_consumer",
                "drop_after_event_id": drop_id,
            })),
        )
            .into_response();
    }

    let terminal = is_terminal_state(&execution.state);
    let execution_state = execution.state.to_lowercase().replace('_', "-");

    // Audit stream open (issue #158: only stream open/close are audited, not per-event)
    // Capture audit context now so the producer task can write stream-close on exit.
    let (audit_actor, audit_source, audit_request_id) = audit_context(&headers, &api_state);
    {
        let target = exec_id.to_string();
        let ar = NewAuditRecord {
            actor: &audit_actor,
            operation: OP_EXECUTION_STREAM_OPEN,
            target_type: TARGET_WORKFLOW,
            target_id: Some(target.as_str()),
            route_or_command: "GET /executions/{exec_id}/events/stream",
            request_id: audit_request_id.as_deref(),
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: Some(shard.as_i32()),
            source: &audit_source,
        };
        let _ = audit::insert_audit(&mut conn, &ar).await;
    }

    // Release the pooled DB connection — SSE streams must not hold connections while idle
    drop(conn);

    // Bounded channel: capacity = buffer_depth.  When the receiver (axum SSE) drops,
    // further sends fail and the producer task shuts down cleanly.
    let (mut tx, rx) =
        futures::channel::mpsc::channel::<Result<Event, std::convert::Infallible>>(buffer_depth);

    let keepalive_interval = api_state.sse_keepalive_interval();
    let api_clone = api_state.clone();

    // Producer task: runs independently of the HTTP handler after we return
    tokio::spawn(async move {
        use autumn_harvest::audit::OP_EXECUTION_STREAM_CLOSE;

        // Helper: extract the inner payload from the adjacently-tagged envelope
        // `{"type":"...","data":{...}}` — the `event:` field already carries the
        // type, so `data:` should contain only the payload object.
        let sse_data = |event_data: &serde_json::Value| -> String {
            let inner = event_data.get("data").unwrap_or(event_data);
            serde_json::to_string(inner).unwrap_or_default()
        };

        // Helper: flush a slice of DB rows into the SSE channel.
        // Returns the last `row.id` seen and the first terminal state name found,
        // or breaks early if the channel is full / dropped.
        // Using a macro-style closure here because closures can't easily `break 'notify`.
        // We use a boolean return: (last_id, terminal_state, should_break).
        let send_rows =
            |rows: &[autumn_harvest::models::HarvestEvent],
             mut cur_last_seen: i64,
             tx: &mut futures::channel::mpsc::Sender<Result<Event, std::convert::Infallible>>|
             -> (i64, Option<&'static str>, bool) {
                let mut found_terminal: Option<&'static str> = None;
                for row in rows {
                    let sse_event = Event::default()
                        .id(row.id.to_string())
                        .event(row.event_type.as_str())
                        .data(sse_data(&row.event_data));
                    if tx.try_send(Ok(sse_event)).is_err() {
                        return (cur_last_seen, None, true);
                    }
                    cur_last_seen = row.id;
                    if is_terminal_event_type(&row.event_type) {
                        found_terminal = Some(terminal_event_type_to_state(&row.event_type));
                    }
                }
                (cur_last_seen, found_terminal, false)
            };

        // ── 1. Send backfill events (events already committed before this request) ──
        // Also track whether a terminal event appears in the backfill: the execution
        // may have transitioned between load_execution and load_events_after_row_id.
        let mut backfill_terminal: Option<&'static str> = None;
        for row in &backfill {
            let sse_event = Event::default()
                .id(row.id.to_string())
                .event(row.event_type.as_str())
                .data(sse_data(&row.event_data));
            if tx.send(Ok(sse_event)).await.is_err() {
                // Client disconnected during backfill — skip straight to close audit
                if let Ok(mut conn) = db_conn_for_execution(&api_clone, exec_id).await {
                    let target = exec_id.to_string();
                    let ar = NewAuditRecord {
                        actor: &audit_actor,
                        operation: OP_EXECUTION_STREAM_CLOSE,
                        target_type: TARGET_WORKFLOW,
                        target_id: Some(target.as_str()),
                        route_or_command: "GET /executions/{exec_id}/events/stream",
                        request_id: audit_request_id.as_deref(),
                        idempotency_key: None,
                        status: STATUS_SUCCEEDED,
                        error_summary: None,
                        shard_id: Some(shard.as_i32()),
                        source: &audit_source,
                    };
                    let _ = audit::insert_audit(&mut conn, &ar).await;
                }
                return;
            }
            if is_terminal_event_type(&row.event_type) {
                backfill_terminal = Some(terminal_event_type_to_state(&row.event_type));
            }
        }

        // ── 2. If already terminal (or backfill contained a terminal event), emit
        //       stream-end and close.  Use the backfill-detected state when present
        //       because it reflects the actual transition even if load_execution ran
        //       before the row was committed.
        let effective_terminal: Option<&str> =
            backfill_terminal.map(|s| s as &str).or(if terminal {
                Some(execution_state.as_str())
            } else {
                None
            });
        if let Some(state) = effective_terminal {
            let end_data = serde_json::json!({"reason": state}).to_string();
            let _ = tx
                .send(Ok(Event::default().event("stream-end").data(end_data)))
                .await;
        } else {
            // ── 3. Live-tail: LISTEN/NOTIFY loop ─────────────────────────────
            let mut last_seen_id = backfill.last().map_or(last_row_id, |r| r.id);
            let mut listener = listener;
            let buf_limit = i64::try_from(api_clone.sse_buffer_depth()).ok();

            'notify: loop {
                // Use 2× keepalive as notification timeout so the KeepAlive wrapper
                // has time to send its ping before this loop wakes and re-checks
                let wait_timeout = keepalive_interval.saturating_mul(2);
                match listener.wait_for_notification_timeout(wait_timeout).await {
                    Ok(WorkflowEventWaitOutcome::Notification(payload)) => {
                        // The harvest_events channel notifies for ALL executions on
                        // this shard; filter to ours
                        if payload.workflow_exec_id != exec_id.as_uuid() {
                            continue;
                        }

                        // Load new events from the pool — capped to buffer_depth so
                        // rapid bursts between notifications stay bounded in memory.
                        let new_rows = match db_conn_for_execution(&api_clone, exec_id).await {
                            Ok(mut conn) => {
                                match store::load_events_after_row_id(
                                    &mut conn,
                                    exec_id,
                                    last_seen_id,
                                    buf_limit,
                                )
                                .await
                                {
                                    Ok(rows) => rows,
                                    // DB failure: skip this notification batch.
                                    // The periodic TimedOut poll will catch missed
                                    // events if no further notifications arrive.
                                    Err(_) => continue,
                                }
                            }
                            Err(_) => continue,
                        };

                        let (new_id, terminal_state, should_break) =
                            send_rows(&new_rows, last_seen_id, &mut tx);
                        last_seen_id = new_id;

                        if should_break {
                            let err_data = serde_json::json!({
                                "error": "slow_consumer",
                                "drop_after_event_id": last_seen_id,
                            })
                            .to_string();
                            let _ = tx.try_send(Ok(Event::default()
                                .event("stream-error")
                                .data(err_data)));
                            break 'notify;
                        }

                        if let Some(state) = terminal_state {
                            let end_data = serde_json::json!({"reason": state}).to_string();
                            let _ = tx
                                .send(Ok(Event::default().event("stream-end").data(end_data)))
                                .await;
                            break 'notify;
                        }
                    }
                    Ok(WorkflowEventWaitOutcome::TimedOut) => {
                        // Check whether the client has disconnected while idle
                        // (send/try_send only detect disconnect on a write attempt).
                        if tx.is_closed() {
                            break 'notify;
                        }
                        // Periodic safety-net poll: catch any events missed due to a
                        // prior DB failure on a notification (e.g. terminal event).
                        let Ok(mut conn) = db_conn_for_execution(&api_clone, exec_id).await else {
                            continue 'notify;
                        };
                        let Ok(missed) = store::load_events_after_row_id(
                            &mut conn,
                            exec_id,
                            last_seen_id,
                            buf_limit,
                        )
                        .await
                        else {
                            continue 'notify;
                        };
                        let (new_id, terminal_state, should_break) =
                            send_rows(&missed, last_seen_id, &mut tx);
                        last_seen_id = new_id;
                        if should_break {
                            let err_data = serde_json::json!({
                                "error": "slow_consumer",
                                "drop_after_event_id": last_seen_id,
                            })
                            .to_string();
                            let _ = tx.try_send(Ok(Event::default()
                                .event("stream-error")
                                .data(err_data)));
                            break 'notify;
                        }
                        if let Some(state) = terminal_state {
                            let end_data = serde_json::json!({"reason": state}).to_string();
                            let _ = tx
                                .send(Ok(Event::default().event("stream-end").data(end_data)))
                                .await;
                            break 'notify;
                        }
                    }
                    Ok(WorkflowEventWaitOutcome::ChannelClosed) => {
                        // LISTEN connection dropped; reconnect and backfill any events
                        // that may have been committed while the connection was down.
                        let Ok(l) = WorkflowEventListener::connect(&notification_url).await else {
                            break 'notify;
                        };
                        listener = l;
                        // Backfill events missed during reconnection window
                        let Ok(mut conn) = db_conn_for_execution(&api_clone, exec_id).await else {
                            continue 'notify;
                        };
                        let Ok(missed) = store::load_events_after_row_id(
                            &mut conn,
                            exec_id,
                            last_seen_id,
                            buf_limit,
                        )
                        .await
                        else {
                            continue 'notify;
                        };
                        let (new_id, terminal_state, should_break) =
                            send_rows(&missed, last_seen_id, &mut tx);
                        last_seen_id = new_id;
                        if should_break {
                            let err_data = serde_json::json!({
                                "error": "slow_consumer",
                                "drop_after_event_id": last_seen_id,
                            })
                            .to_string();
                            let _ = tx.try_send(Ok(Event::default()
                                .event("stream-error")
                                .data(err_data)));
                            break 'notify;
                        }
                        if let Some(state) = terminal_state {
                            let end_data = serde_json::json!({"reason": state}).to_string();
                            let _ = tx
                                .send(Ok(Event::default().event("stream-end").data(end_data)))
                                .await;
                            break 'notify;
                        }
                    }
                    Err(_) => break 'notify,
                }
            }
        }

        // Audit stream close (issue #158) — fires on every producer exit path
        if let Ok(mut conn) = db_conn_for_execution(&api_clone, exec_id).await {
            let target = exec_id.to_string();
            let ar = NewAuditRecord {
                actor: &audit_actor,
                operation: OP_EXECUTION_STREAM_CLOSE,
                target_type: TARGET_WORKFLOW,
                target_id: Some(target.as_str()),
                route_or_command: "GET /executions/{exec_id}/events/stream",
                request_id: audit_request_id.as_deref(),
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: Some(shard.as_i32()),
                source: &audit_source,
            };
            let _ = audit::insert_audit(&mut conn, &ar).await;
        }
    });

    // Return the SSE response. axum's KeepAlive wrapper sends `: ping\n\n`
    // comments every keepalive_interval so proxies don't idle the connection.
    Sse::new(rx)
        .keep_alive(KeepAlive::new().interval(keepalive_interval).text("ping"))
        .into_response()
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
        | HarvestError::NonDeterministic {
            reason: message, ..
        }
        | HarvestError::Cancelled(message)
        | HarvestError::WorkflowFailed {
            name: _,
            reason: message,
        } => AutumnError::bad_request_msg(message),
        HarvestError::UpdateRejected { reason } => {
            AutumnError::bad_request_msg(reason).with_status(axum::http::StatusCode::CONFLICT)
        }
        // An update (or other mutation) was submitted against a paused
        // execution (issue #383): reject with 409 Conflict.
        HarvestError::WorkflowPaused(exec_id) => {
            AutumnError::bad_request_msg(format!("workflow paused: {exec_id}"))
                .with_status(axum::http::StatusCode::CONFLICT)
        }
        HarvestError::PayloadTooLarge {
            kind,
            observed_bytes,
            cap_bytes,
            workflow_type,
            ..
        } => AutumnError::bad_request_msg(format!(
            "payload too large: {kind} for workflow '{workflow_type}' exceeded cap of \
             {cap_bytes} bytes (observed {observed_bytes} bytes)"
        ))
        .with_status(axum::http::StatusCode::PAYLOAD_TOO_LARGE),
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

// ── Calendar management (issue #337) ─────────────────────────────────────────

/// Response body for calendar CRUD endpoints.
#[derive(Debug, Serialize)]
struct CalendarResponse {
    name: String,
    description: Option<String>,
    built_in: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<HarvestCalendar> for CalendarResponse {
    fn from(c: HarvestCalendar) -> Self {
        Self {
            name: c.name,
            description: c.description,
            built_in: c.built_in,
            created_at: c.created_at,
            updated_at: c.updated_at,
        }
    }
}

/// `GET /calendars` — list all calendars.
async fn list_calendars_handler(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<impl IntoResponse, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let calendars = list_calendars(&mut conn).await.map_err(map_error)?;
    let response: Vec<CalendarResponse> = calendars.into_iter().map(Into::into).collect();
    Ok((StatusCode::OK, Json(response)))
}

/// `GET /calendars/{name}` — get a single calendar with its exclusions.
#[derive(Debug, Serialize)]
struct CalendarDetailResponse {
    name: String,
    description: Option<String>,
    built_in: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    exclusion_dates: Vec<chrono::NaiveDate>,
}

async fn get_calendar_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let cal = get_calendar(&mut conn, &name).await.map_err(map_error)?;
    let exclusions = load_exclusions_for_calendar(&mut conn, &name)
        .await
        .map_err(map_error)?;
    Ok((
        StatusCode::OK,
        Json(CalendarDetailResponse {
            name: cal.name,
            description: cal.description,
            built_in: cal.built_in,
            created_at: cal.created_at,
            updated_at: cal.updated_at,
            exclusion_dates: exclusions,
        }),
    ))
}

/// Request body for `POST /calendars`.
#[derive(Debug, Deserialize)]
struct CreateCalendarRequest {
    name: String,
    description: Option<String>,
}

/// `POST /calendars` — create a custom calendar.
async fn create_calendar_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Json(body): Json<CreateCalendarRequest>,
) -> Result<impl IntoResponse, AutumnError> {
    if body.name.is_empty() {
        return Err(AutumnError::bad_request_msg(
            "calendar name must not be empty",
        ));
    }
    let pool = api_state.storage_pool().map_err(map_error)?;
    // Track whether at least one shard performed a fresh insert so we can
    // distinguish a successful new create from a duplicate-name conflict.
    let mut fresh_insert_cal: Option<_> = None;
    let mut conflict_cal: Option<_> = None;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        let mut conn = match acquire_conn(shard_pool).await {
            Ok(c) => c,
            Err(e) => {
                // Connection failure: continue so later shards are attempted;
                // collect the error so we can surface it if nothing succeeded.
                shard_errors.push(format!("shard {shard_id}: {e}"));
                continue;
            }
        };
        match create_calendar(&mut conn, &body.name, body.description.as_deref()).await {
            Ok(cal) => {
                if fresh_insert_cal.is_none() {
                    fresh_insert_cal = Some(cal);
                }
            }
            // "Already exists" on a later shard is an idempotent retry-safe no-op
            // (the shard already has the calendar from a prior attempt). Record
            // the existing row so we can return it if no shard performed a fresh
            // insert.
            Err(autumn_harvest::HarvestError::Config(ref msg))
                if msg.contains("already exists") =>
            {
                if conflict_cal.is_none() {
                    conflict_cal = Some(
                        get_calendar(&mut conn, &body.name)
                            .await
                            .map_err(map_error)?,
                    );
                }
            }
            Err(e) => return Err(map_error(e)),
        }
    }
    // At least one shard performed a fresh insert: this was a new calendar.
    if let Some(cal) = fresh_insert_cal {
        return Ok((StatusCode::CREATED, Json(CalendarResponse::from(cal))));
    }
    // Every reachable shard already had the calendar: the name is taken.
    if conflict_cal.is_some() {
        return Err(AutumnError::bad_request_msg(format!(
            "calendar '{}' already exists",
            body.name
        ))
        .with_status(axum::http::StatusCode::CONFLICT));
    }
    // Only connection errors (no shard was reached at all).
    if !shard_errors.is_empty() {
        return Err(AutumnError::service_unavailable_msg(format!(
            "calendar create could not reach all shards; retry to converge: {}",
            shard_errors.join("; ")
        )));
    }
    Err(AutumnError::service_unavailable_msg("no shards available"))
}

/// Request body for `PUT /calendars/{name}` — replace exclusion dates.
#[derive(Debug, Deserialize)]
struct UpdateCalendarRequest {
    /// Complete set of exclusion dates (replaces existing). Format: `"YYYY-MM-DD"`.
    exclusion_dates: Vec<chrono::NaiveDate>,
}

/// `PUT /calendars/{name}` — replace the exclusion set for a calendar.
async fn update_calendar_exclusions_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Path(name): Path<String>,
    Json(body): Json<UpdateCalendarRequest>,
) -> Result<impl IntoResponse, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    // Preflight: verify the calendar exists on every reachable shard before
    // writing to any. This ensures a 404 is returned cleanly without leaving
    // partial exclusion updates on earlier shards.
    for (_, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        match get_calendar(&mut conn, &name).await {
            Ok(_) => {}
            Err(autumn_harvest::HarvestError::NotFound(_)) => {
                return Err(AutumnError::not_found_msg(format!(
                    "calendar '{name}' not found"
                )));
            }
            Err(e) => return Err(map_error(e)),
        }
    }
    // Fanout: attempt all shards; collect failures so retries converge.
    // replace_calendar_exclusions is transactional so retrying a partial
    // fanout is safe.
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        let mut conn = match acquire_conn(shard_pool).await {
            Ok(c) => c,
            Err(e) => {
                shard_errors.push(format!("shard {shard_id}: {e}"));
                continue;
            }
        };
        if let Err(e) = replace_calendar_exclusions(&mut conn, &name, &body.exclusion_dates).await {
            shard_errors.push(format!("shard {shard_id}: {e}"));
        }
    }
    if !shard_errors.is_empty() {
        return Err(AutumnError::service_unavailable_msg(format!(
            "calendar exclusion update failed on {} shard(s); retry to converge: {}",
            shard_errors.len(),
            shard_errors.join("; ")
        )));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /calendars/{name}` — delete a custom calendar.
async fn delete_calendar_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut any_deleted = false;
    let mut all_not_found = true;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        let mut conn = match acquire_conn(shard_pool).await {
            Ok(c) => c,
            Err(e) => {
                shard_errors.push(format!("shard {shard_id}: {e}"));
                all_not_found = false;
                continue;
            }
        };
        match delete_calendar(&mut conn, &name).await {
            Ok(()) => {
                any_deleted = true;
                all_not_found = false;
            }
            // NotFound on this shard is idempotent: a prior partial delete or
            // the calendar was never on this shard. Continue so all shards
            // are attempted before deciding the response.
            Err(autumn_harvest::HarvestError::NotFound(_)) => {}
            // Config error means the calendar is built-in and cannot be deleted:
            // return 400 immediately since this is a client error, not transient.
            Err(autumn_harvest::HarvestError::Config(msg)) => {
                return Err(AutumnError::bad_request_msg(msg));
            }
            Err(e) => {
                shard_errors.push(format!("shard {shard_id}: {e}"));
                all_not_found = false;
            }
        }
    }
    // Every shard reported not-found and no errors: calendar never existed.
    if all_not_found && !any_deleted && shard_errors.is_empty() {
        return Err(AutumnError::not_found_msg(format!(
            "calendar '{name}' not found"
        )));
    }
    if !shard_errors.is_empty() {
        return Err(AutumnError::service_unavailable_msg(format!(
            "calendar delete failed on {} shard(s); retry to converge: {}",
            shard_errors.len(),
            shard_errors.join("; ")
        )));
    }
    Ok(StatusCode::NO_CONTENT)
}

// ── Schedule next-fires preview API (issues #337, #348) ──────────────────────

/// Query parameters for `GET /admin/schedules/{id}/preview`.
#[derive(Debug, Deserialize)]
struct SchedulePreviewQuery {
    /// Number of fire-time entries to return. Defaults to 10, max 100.
    #[serde(default = "default_preview_count")]
    count: usize,
    /// ISO-8601 UTC instant to start the preview from. Defaults to now.
    #[serde(default)]
    from: Option<String>,
}

const fn default_preview_count() -> usize {
    10
}

/// A single enriched entry in the schedule next-fires preview (issue #348).
#[derive(Debug, Serialize)]
struct ScheduleFirePreviewEntry {
    /// UTC wall-clock instant the cron/interval computed.
    scheduled_at: chrono::DateTime<chrono::Utc>,
    /// `scheduled_at` rendered in the schedule's configured timezone (RFC 3339).
    local_at: String,
    /// Effective fire time after calendar adjustment and jitter application.
    /// `None` means the firing is suppressed (calendar exclusion with `skip` policy).
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_at: Option<chrono::DateTime<chrono::Utc>>,
    /// `effective_at` rendered in the schedule's timezone. Omitted when `effective_at` is `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_local_at: Option<String>,
    /// Human-readable reason for this instant:
    /// - `"cron"` — fires as scheduled.
    /// - `"cron+jitter"` — fires at `effective_at` after deterministic jitter.
    /// - `"skipped:calendar-excluded"` — suppressed by calendar exclusion.
    /// - `"deferred:calendar"` — moved to a different day by calendar skip-policy.
    reason: String,
    /// Earliest possible fire time when jitter is enabled (`= scheduled_at`).
    /// Omitted when `jitter_secs = 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    jitter_earliest_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Latest possible fire time when jitter is enabled (`= scheduled_at + jitter_secs`).
    /// Omitted when `jitter_secs = 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    jitter_latest_at: Option<chrono::DateTime<chrono::Utc>>,
    /// `true` when the overlap policy (`"skip"` or `"buffer_one"`) could silently drop this
    /// firing if `max_active_runs` is already running at dispatch time. Preview is stateless
    /// and cannot know the future run count; treat this as an advisory warning.
    would_skip_if_active: bool,
}

/// Request body for `POST /admin/schedules/preview` — validate a candidate schedule
/// config and preview its next N fire times without persisting anything.
///
/// Same field set as `CreateWorkflowScheduleRequest` (issue #348).
#[derive(Debug, Deserialize)]
struct CandidateSchedulePreviewRequest {
    schedule_expr: String,
    #[serde(default = "default_timezone")]
    timezone: String,
    #[serde(default)]
    #[allow(dead_code)] // part of the public request shape; included for spec completeness
    catchup: bool,
    #[serde(default = "default_max_active_runs")]
    #[allow(dead_code)] // part of the public request shape; included for spec completeness
    max_active_runs: u32,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    jitter_secs: u64,
    #[serde(default = "default_overlap_policy")]
    overlap_policy: String,
    #[serde(default = "default_buffer_all_max")]
    #[allow(dead_code)] // part of the public request shape; included for spec completeness
    buffer_all_max: u32,
    #[serde(default)]
    calendar: Option<String>,
    #[serde(default = "default_skip_policy")]
    skip_policy: String,
    /// Number of entries to return. Defaults to 10, max 100.
    #[serde(default = "default_preview_count")]
    count: usize,
    /// ISO-8601 UTC instant to start the preview from. Defaults to now.
    #[serde(default)]
    from: Option<String>,
}

/// Convert a raw `ScheduleFirePreview` (from calendar.rs) into the enriched API entry
/// format required by issue #348. This is the pure transformation function that tests
/// exercise directly.
///
/// `pre_jitter_base` is the calendar-adjusted fire time BEFORE jitter was applied
/// (i.e., `effective_at` from the original unmodified entry). Jitter bounds are
/// computed from this base so that deferred-calendar entries report bounds on the
/// correct (adjusted) day, not the original excluded date.
fn build_preview_entry(
    raw: &autumn_harvest::calendar::ScheduleFirePreview,
    pre_jitter_base: Option<chrono::DateTime<chrono::Utc>>,
    jitter_secs: i64,
    timezone: &str,
    overlap_policy: &str,
) -> ScheduleFirePreviewEntry {
    let has_jitter = jitter_secs > 0;

    // Map old calendar.rs reason strings to the v2 reason vocabulary.
    let reason = if raw.reason.starts_with("SkippedByCalendar:") {
        "skipped:calendar-excluded".to_string()
    } else if raw.reason.starts_with("DeferredFrom:") {
        "deferred:calendar".to_string()
    } else if has_jitter {
        "cron+jitter".to_string()
    } else {
        "cron".to_string()
    };

    // Jitter bounds: [pre_jitter_base, pre_jitter_base + jitter_window].
    // Using the calendar-adjusted base (not raw.scheduled_at) ensures deferred
    // entries show bounds on the correct day, not the original excluded date.
    // try_seconds + checked_add_signed guard against oversized stored values
    // (e.g. a saturated i64::MAX written by the schedule create path).
    let (jitter_earliest_at, jitter_latest_at) = if has_jitter {
        pre_jitter_base.map_or((None, None), |base| {
            let latest =
                chrono::Duration::try_seconds(jitter_secs).and_then(|d| base.checked_add_signed(d));
            (Some(base), latest)
        })
    } else {
        (None, None)
    };

    let local_at = format_in_timezone(raw.scheduled_at, timezone);
    let effective_local_at = raw.effective_at.map(|t| format_in_timezone(t, timezone));

    // would_skip_if_active: advisory flag set when the overlap policy can silently
    // drop a firing if capacity is saturated at dispatch time. The `max_active_runs > 0`
    // guard is intentionally absent: max_active_runs = 0 means the scheduler's
    // `running >= max_active_runs` check is always true, so every firing is dropped.
    let would_skip_if_active =
        (overlap_policy == "skip" || overlap_policy == "buffer_one") && raw.effective_at.is_some();

    ScheduleFirePreviewEntry {
        scheduled_at: raw.scheduled_at,
        local_at,
        effective_at: raw.effective_at,
        effective_local_at,
        reason,
        jitter_earliest_at,
        jitter_latest_at,
        would_skip_if_active,
    }
}

/// Format a UTC timestamp in the given IANA timezone as RFC 3339.
/// Falls back to UTC representation when the timezone name is unknown.
fn format_in_timezone(utc: chrono::DateTime<chrono::Utc>, tz_name: &str) -> String {
    tz_name.parse::<chrono_tz::Tz>().map_or_else(
        |_| utc.to_rfc3339(),
        |tz| utc.with_timezone(&tz).to_rfc3339(),
    )
}

/// Parse an optional ISO-8601 `from` string. Returns the parsed instant when
/// present, `Utc::now()` when absent, and `Err(String)` when the string is
/// malformed or too far in the future (caller maps this to a 400 response).
///
/// Years above 9000 are rejected: interval schedules compute `from + period`
/// repeatedly, and very large `from` values can overflow `DateTime` arithmetic or
/// produce RFC 3339 strings (year > 9999) that most clients cannot parse.
fn parse_from_param(from: Option<&str>) -> Result<chrono::DateTime<chrono::Utc>, String> {
    from.map_or_else(
        || Ok(chrono::Utc::now()),
        |s| {
            let dt = chrono::DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .map_err(|e| format!("invalid `from` timestamp '{s}': {e}"))?;
            if chrono::Datelike::year(&dt) > 9000 {
                return Err(format!(
                    "invalid `from` timestamp '{s}': year must be \u{2264} 9000"
                ));
            }
            Ok(dt)
        },
    )
}

/// `GET /admin/schedules/{id}/preview?count=N&from=<ISO8601>` — preview the next N
/// planned firing instants for a saved schedule (issue #348 supersedes issue #337).
async fn preview_schedule_firings_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<uuid::Uuid>,
    Query(params): Query<SchedulePreviewQuery>,
) -> Result<impl IntoResponse, AutumnError> {
    use autumn_harvest::policy::SkipPolicy;
    use autumn_harvest::scheduler::parse_schedule_from_expr_pub;

    let count = params.count.clamp(1, 100);
    let from = parse_from_param(params.from.as_deref()).map_err(AutumnError::bad_request_msg)?;

    // load_schedule_by_id fans out across all shards so schedules on any shard are found.
    let schedule = load_schedule_by_id(&api_state, id).await?;

    // Paused schedules return zero entries with a paused reason summary.
    if schedule.is_paused {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "entries": [],
                "is_paused": true,
                "pause_reason": schedule.pause_reason,
                "from": from,
                "count_requested": count,
            })),
        ));
    }

    let parsed_schedule = schedule
        .schedule_expr
        .as_deref()
        .and_then(parse_schedule_from_expr_pub);
    let Some(ref sched) = parsed_schedule else {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "entries": [],
                "is_paused": false,
                "pause_reason": serde_json::Value::Null,
                "from": from,
                "count_requested": count,
            })),
        ));
    };

    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;
    let calendar_name = schedule.calendar_name.as_deref();
    let skip_policy = SkipPolicy::from_db(&schedule.skip_policy);

    let excluded_dates = if let Some(cal_name) = calendar_name {
        load_exclusions_for_calendar(&mut conn, cal_name)
            .await
            .map_err(map_error)?
    } else {
        vec![]
    };

    let jitter_secs = schedule.jitter_secs;
    let schedule_id = schedule.id;
    let timezone = &schedule.timezone;

    let mut raw_entries = preview_schedule_firings(
        sched,
        from,
        count,
        calendar_name,
        &excluded_dates,
        skip_policy,
    );

    // Capture pre-jitter calendar-adjusted bases before modifying entries in-place.
    // These become the lower bound of the jitter window for deferred-calendar entries.
    let pre_jitter_bases: Vec<Option<chrono::DateTime<chrono::Utc>>> =
        raw_entries.iter().map(|e| e.effective_at).collect();

    // Apply jitter to each effective_at (mirrors effective_fire_time logic).
    if jitter_secs > 0 {
        let jitter_window = std::time::Duration::from_secs(jitter_secs.cast_unsigned());
        for entry in &mut raw_entries {
            if let Some(t) = entry.effective_at {
                let offset = compute_jitter_offset(schedule_id, t, jitter_window);
                if let Ok(d) = chrono::Duration::from_std(offset) {
                    entry.effective_at = Some(t + d);
                }
            }
        }
    }

    let entries: Vec<ScheduleFirePreviewEntry> = raw_entries
        .iter()
        .zip(pre_jitter_bases.iter())
        .map(|(r, &base)| {
            build_preview_entry(r, base, jitter_secs, timezone, &schedule.overlap_policy)
        })
        .collect();

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "entries": entries,
            "is_paused": false,
            "pause_reason": serde_json::Value::Null,
            "from": from,
            "count_requested": count,
        })),
    ))
}

/// `POST /admin/schedules/preview` — validate a candidate schedule config and return
/// the next N firing instants without persisting anything (issue #348).
///
/// Returns `400 Bad Request` when the `schedule_expr` or `timezone` is invalid,
/// so operators get an actionable parse error before committing the config.
#[allow(clippy::too_many_lines)]
async fn preview_candidate_schedule_handler(
    Extension(api_state): Extension<HarvestApiState>,
    Json(body): Json<CandidateSchedulePreviewRequest>,
) -> Result<impl IntoResponse, AutumnError> {
    use autumn_harvest::policy::SkipPolicy;

    let count = body.count.clamp(1, 100);
    let from = parse_from_param(body.from.as_deref()).map_err(AutumnError::bad_request_msg)?;

    // Validate and parse the schedule expression; return 400 on error.
    // Infer `field` from the error message so timezone parse failures are
    // correctly attributed to the `timezone` field, not `schedule_expr`.
    let schedule = match parse_schedule_expr_with_tz(&body.schedule_expr, &body.timezone) {
        Ok(s) => s,
        Err(e) => {
            let field = if e.to_lowercase().contains("timezone") || e.to_lowercase().contains("tz")
            {
                "timezone"
            } else {
                "schedule_expr"
            };
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e, "field": field})),
            ));
        }
    };

    // Validate skip_policy strictly — from_db silently falls back to Skip.
    let skip_policy = match SkipPolicy::from_user_input(&body.skip_policy) {
        Ok(p) => p,
        Err(bad) => {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("unknown skip_policy '{bad}'; valid: skip, run_next_business_day, run_prev_business_day"),
                    "field": "skip_policy"
                })),
            ));
        }
    };

    // Validate overlap_policy strictly before it is used in build_preview_entry.
    if let Err(bad) = autumn_harvest::OverlapPolicy::from_user_input(&body.overlap_policy) {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("unknown overlap_policy '{bad}'; valid: skip, buffer_one, buffer_all, cancel_other, terminate_other"),
                "field": "overlap_policy"
            })),
        ));
    }

    // Validate jitter before i64 conversion; body.jitter_secs is u64 so an
    // overly large value would overflow chrono::Duration::seconds and panic.
    let jitter_duration = std::time::Duration::from_secs(body.jitter_secs);
    if let Err(e) = validate_jitter(&schedule, jitter_duration) {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e, "field": "jitter_secs"})),
        ));
    }
    // Safe after validate_jitter: valid jitter is at most 3600 s for cron,
    // or less than the interval period, both well within i64 range.
    let jitter_secs = i64::try_from(body.jitter_secs).unwrap_or(i64::MAX);

    // Verify the calendar exists before returning any result so that a typo
    // here gets a 400 even when the schedule is paused.  Exclusion dates are
    // only loaded for active schedules further below.
    let calendar_name = body.calendar.as_deref();
    if let Some(cal_name) = calendar_name {
        let pool = api_state.storage_pool().map_err(map_error)?;
        let mut conn = acquire_conn(pool.default_pool()).await?;
        match get_calendar(&mut conn, cal_name).await {
            Ok(_) => {}
            Err(autumn_harvest::HarvestError::NotFound(_)) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("calendar '{cal_name}' not found; create it first with POST /calendars"),
                        "field": "calendar"
                    })),
                ));
            }
            Err(e) => return Err(map_error(e)),
        }
    }

    // Paused candidate schedules return no entries. All validations above
    // (schedule_expr, timezone, skip_policy, overlap_policy, jitter, calendar)
    // run first so the preview rejects the same configs that POST /admin/schedules/workflow
    // would reject, regardless of pause state.
    if body.paused {
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "entries": [],
                "is_paused": true,
                "pause_reason": serde_json::Value::Null,
                "from": from,
                "count_requested": count,
            })),
        ));
    }

    let excluded_dates = if let Some(cal_name) = calendar_name {
        let pool = api_state.storage_pool().map_err(map_error)?;
        let mut conn = acquire_conn(pool.default_pool()).await?;
        load_exclusions_for_calendar(&mut conn, cal_name)
            .await
            .map_err(map_error)?
    } else {
        vec![]
    };
    // Use the timezone embedded in the schedule variant so local_at always
    // reflects what the DB will store after saving. CronInTimezone carries the
    // timezone explicitly; Interval and Manual are always UTC-based (Schedule::timezone_str()
    // returns "UTC" for those variants), so using body.timezone there would show a
    // timezone that the persisted schedule would not honour.
    let effective_timezone: &str = match &schedule {
        autumn_harvest::policy::Schedule::CronInTimezone { tz, .. } => tz.as_str(),
        _ => "UTC",
    };

    // Use a deterministic placeholder UUID so jitter offsets are stable per request.
    let schedule_id = uuid::Uuid::nil();

    let mut raw_entries = preview_schedule_firings(
        &schedule,
        from,
        count,
        calendar_name,
        &excluded_dates,
        skip_policy,
    );

    // Capture pre-jitter calendar-adjusted bases before modifying entries in-place.
    let pre_jitter_bases: Vec<Option<chrono::DateTime<chrono::Utc>>> =
        raw_entries.iter().map(|e| e.effective_at).collect();

    if jitter_secs > 0 {
        let jitter_window = std::time::Duration::from_secs(body.jitter_secs);
        for entry in &mut raw_entries {
            if let Some(t) = entry.effective_at {
                let offset = compute_jitter_offset(schedule_id, t, jitter_window);
                if let Ok(d) = chrono::Duration::from_std(offset) {
                    entry.effective_at = Some(t + d);
                }
            }
        }
    }

    let entries: Vec<ScheduleFirePreviewEntry> = raw_entries
        .iter()
        .zip(pre_jitter_bases.iter())
        .map(|(r, &base)| {
            build_preview_entry(
                r,
                base,
                jitter_secs,
                effective_timezone,
                &body.overlap_policy,
            )
        })
        .collect();

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "entries": entries,
            "is_paused": false,
            "pause_reason": serde_json::Value::Null,
            "from": from,
            "count_requested": count,
        })),
    ))
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

    let capable_of = pairs
        .iter()
        .find(|(k, _)| k == "capable_of")
        .map(|(_, v)| v.clone());

    if let Some(ref act_name) = capable_of {
        if let Ok(runtime) = api_state.runtime() {
            if let Some(activity) = runtime.registry().activities.get(act_name) {
                let target_queue = filters
                    .queue
                    .as_deref()
                    .unwrap_or_else(|| activity.default_queue.unwrap_or("default"));
                let parsed_reqs = activity.requires.and_then(|req_str| {
                    autumn_harvest::eligibility::parse_requirements(req_str).ok()
                });

                results.retain(|w| {
                    let is_subscribed =
                        w.worker.queues.as_array().is_some_and(|arr| {
                            arr.iter().any(|v| v.as_str() == Some(target_queue))
                        });
                    if !is_subscribed {
                        return false;
                    }

                    if activity.requires.is_some() && parsed_reqs.is_none() {
                        return false;
                    }

                    parsed_reqs.as_ref().is_none_or(|reqs| {
                        let worker_labels: std::collections::HashMap<String, String> =
                            serde_json::from_value(w.worker.labels.clone()).unwrap_or_default();
                        autumn_harvest::eligibility::matches_requirements(reqs, &worker_labels)
                    })
                });
            } else {
                results.clear();
            }
        } else {
            results.clear();
        }
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

// ---------------------------------------------------------------------------
// Build routing management (issue #362)
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize)]
struct BuildRoutingResponse {
    policies: Vec<autumn_harvest::build_routing::BuildPolicy>,
    reachability: Vec<autumn_harvest::build_routing::BuildReachability>,
    /// Queues whose active `build_id` differs across shards, indicating a partial-write incident.
    diverged_queues: Vec<String>,
    /// Shards that could not be reached; per-shard data may be incomplete.
    shard_errors: Vec<serde_json::Value>,
}

#[derive(Debug, serde::Serialize)]
struct CompatListResponse {
    entries: Vec<autumn_harvest::build_routing::BuildCompatEntry>,
    /// Compat pairs present on some shards but absent on others (partial fanout).
    /// A non-empty list means compatibility is inconsistent across shards for those pairs.
    diverged_pairs: Vec<serde_json::Value>,
    /// Shards that could not be reached during this read; the list may be incomplete.
    shard_errors: Vec<serde_json::Value>,
}

#[derive(Debug, serde::Deserialize)]
struct SetBuildPolicyBody {
    queue_name: String,
    build_id: String,
    #[serde(default)]
    deployment_name: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct DeclareCompatBody {
    build_id: String,
    compatible_with: String,
}

#[derive(Debug, serde::Serialize)]
struct RevokeCompatResponse {
    revoked: bool,
}

#[derive(Debug, serde::Deserialize)]
struct RetireBuildBody {
    build_id: String,
}

#[derive(Debug, serde::Serialize)]
struct RetireBuildResponse {
    build_id: String,
    safe_to_retire: bool,
    open_executions: i64,
    pending_tasks: i64,
}

/// `GET /admin/build-routing` — list policies + cross-shard reachability.
#[allow(clippy::too_many_lines)]
async fn list_build_routing_handler(
    Extension(api_state): Extension<HarvestApiState>,
) -> impl axum::response::IntoResponse {
    use std::collections::{HashMap, HashSet};

    use autumn_harvest::build_routing::{
        BuildPolicy, all_build_reachability, list_build_policies, merge_reachability,
    };

    let pool = match api_state.storage_pool().map_err(map_error) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    let stale_threshold = api_state.worker_stale_threshold();
    let mut per_shard_reachability: Vec<Vec<autumn_harvest::build_routing::BuildReachability>> =
        Vec::new();
    let mut shard_errors: Vec<serde_json::Value> = Vec::new();

    // Read policies AND reachability from every shard so we can detect divergence.
    // Policy mutations fan out to all shards; a partial-write leaves some shards
    // with a stale or absent policy row. We surface inconsistencies as `diverged_queues`.
    //
    // Divergence cases detected:
    // 1. Two responding shards have different build_id for the same queue name.
    // 2. A queue exists on at least one shard but is absent on another (partial write).
    let mut merged_policies: HashMap<String, BuildPolicy> = HashMap::new();
    // Each entry is the set of queue names reported by one successfully-read shard.
    let mut per_shard_seen: Vec<HashSet<String>> = Vec::new();
    let mut diverged: HashSet<String> = HashSet::new();

    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => {
                match list_build_policies(&mut conn).await.map_err(map_error) {
                    Ok(shard_policies) => {
                        let mut seen_on_shard: HashSet<String> = HashSet::new();
                        for policy in shard_policies {
                            seen_on_shard.insert(policy.queue_name.clone());
                            match merged_policies.get(&policy.queue_name) {
                                Some(existing) if existing.build_id != policy.build_id => {
                                    diverged.insert(policy.queue_name.clone());
                                    if policy.updated_at > existing.updated_at {
                                        merged_policies.insert(policy.queue_name.clone(), policy);
                                    }
                                }
                                Some(existing) if policy.updated_at > existing.updated_at => {
                                    merged_policies.insert(policy.queue_name.clone(), policy);
                                }
                                None => {
                                    merged_policies.insert(policy.queue_name.clone(), policy);
                                }
                                _ => {}
                            }
                        }
                        per_shard_seen.push(seen_on_shard);
                    }
                    Err(e) => {
                        shard_errors.push(serde_json::json!({
                            "shard_id": shard_id.as_i32(),
                            "error": e.to_string(),
                        }));
                    }
                }

                match all_build_reachability(&mut conn, stale_threshold)
                    .await
                    .map_err(map_error)
                {
                    Ok(r) => per_shard_reachability.push(r),
                    Err(e) => {
                        shard_errors.push(serde_json::json!({
                            "shard_id": shard_id.as_i32(),
                            "error": e.to_string(),
                        }));
                    }
                }
            }
            Err(e) => {
                shard_errors.push(serde_json::json!({
                    "shard_id": shard_id.as_i32(),
                    "error": e.to_string(),
                }));
            }
        }
    }

    // Case 2: queue present on at least one shard but absent on another.
    if per_shard_seen.len() > 1 {
        for queue_name in merged_policies.keys() {
            if per_shard_seen
                .iter()
                .any(|seen| !seen.contains(queue_name.as_str()))
            {
                diverged.insert(queue_name.clone());
            }
        }
    }

    // If every shard read failed (errors present, zero successful reads), return 503.
    // Returning 200 with empty data would mislead callers into treating a DB outage as
    // "no routing configured", which can cause unsafe rollout decisions.
    if !shard_errors.is_empty() && per_shard_seen.is_empty() && per_shard_reachability.is_empty() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({
                "error": "All shard reads failed; routing state cannot be determined.",
                "shard_errors": shard_errors,
            })),
        )
            .into_response();
    }

    let mut policies: Vec<BuildPolicy> = merged_policies.into_values().collect();
    policies.sort_by(|a, b| a.queue_name.cmp(&b.queue_name));
    let mut diverged_queues: Vec<String> = diverged.into_iter().collect();
    diverged_queues.sort();

    let reachability = merge_reachability(per_shard_reachability);
    axum::Json(BuildRoutingResponse {
        policies,
        reachability,
        diverged_queues,
        shard_errors,
    })
    .into_response()
}

/// `POST /admin/build-routing/policies` — set the active build policy for a queue.
///
/// Fans out to all shards so every shard's `get_build_policy()` call sees the new
/// policy when evaluating `assigned_build_id` at workflow-start time.
///
/// Uses fail-forward fan-out: attempts every shard and collects errors rather than
/// aborting on first failure, so a transient shard outage does not leave routing
/// state diverged across the remaining shards.
#[allow(clippy::too_many_lines)]
async fn set_build_policy_handler(
    headers: axum::http::HeaderMap,
    Extension(api_state): Extension<HarvestApiState>,
    axum::Json(body): axum::Json<SetBuildPolicyBody>,
) -> impl axum::response::IntoResponse {
    use autumn_harvest::build_routing::set_build_policy;

    let queue_name = body.queue_name.trim();
    let build_id = body.build_id.trim();
    if queue_name.is_empty() || build_id.is_empty() {
        return AutumnError::bad_request_msg("queue_name and build_id must not be empty")
            .into_response();
    }

    let pool = match api_state.storage_pool().map_err(map_error) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let deployment = body.deployment_name.as_deref().filter(|s| !s.is_empty());
    let (actor, source, request_id) = audit_context(&headers, &api_state);

    let mut last_policy = None;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        let mut conn = match acquire_conn(shard_pool).await {
            Ok(c) => c,
            Err(e) => {
                shard_errors.push(format!("shard {}: {e}", shard_id.as_i32()));
                continue;
            }
        };
        match set_build_policy(&mut conn, queue_name, build_id, deployment)
            .await
            .map_err(map_error)
        {
            Ok(p) => last_policy = Some(p),
            Err(e) => {
                shard_errors.push(format!("shard {}: {e}", shard_id.as_i32()));
            }
        }
    }

    // If every shard write failed, return 503 before attempting audit.
    if !shard_errors.is_empty() && last_policy.is_none() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({ "errors": shard_errors })),
        )
            .into_response();
    }
    let Some(policy) = last_policy else {
        return map_error(autumn_harvest::HarvestError::Config(
            "no shards configured".into(),
        ))
        .into_response();
    };

    // Audit write is required — fail the response if it cannot be persisted.
    let status = if shard_errors.is_empty() {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    let error_summary = if shard_errors.is_empty() {
        None
    } else {
        Some(shard_errors.join("; "))
    };
    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_BUILD_POLICY_SET,
        target_type: TARGET_BUILD_ROUTING,
        target_id: Some(queue_name),
        route_or_command: "POST /admin/build-routing/policies",
        request_id: request_id.as_deref(),
        idempotency_key: None,
        status,
        error_summary: error_summary.as_deref(),
        shard_id: None,
        source: &source,
    };
    match api_state.storage_pool() {
        Ok(audit_pool) => match acquire_conn(audit_pool.default_pool()).await {
            Ok(mut conn) => {
                if let Err(audit_err) = audit::insert_audit(&mut conn, &ar).await {
                    tracing::error!(
                        error = %audit_err,
                        "audit insert failed for build-routing.policy.set"
                    );
                    return AutumnError::service_unavailable_msg(format!(
                        "audit insert failed: {audit_err}"
                    ))
                    .into_response();
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "audit DB connection unavailable for build-routing.policy.set"
                );
                return AutumnError::service_unavailable_msg("audit DB connection unavailable")
                    .into_response();
            }
        },
        Err(e) => {
            tracing::error!(
                error = %e,
                "storage pool unavailable for audit in build-routing.policy.set"
            );
            return AutumnError::service_unavailable_msg("audit DB connection unavailable")
                .into_response();
        }
    }

    if shard_errors.is_empty() {
        (axum::http::StatusCode::OK, axum::Json(policy)).into_response()
    } else {
        (
            axum::http::StatusCode::MULTI_STATUS,
            axum::Json(serde_json::json!({
                "policy": policy,
                "shard_errors": shard_errors,
            })),
        )
            .into_response()
    }
}

/// `GET /admin/build-routing/compat` — list all compatibility declarations.
///
/// Reads from all shards and merges the results. Entries with the same
/// `(build_id, compatible_with)` key are deduplicated by keeping the latest
/// `declared_at` timestamp. Unreachable shards are listed in `shard_errors` in
/// the response so operators can distinguish a complete read from a partial one.
/// If no shard succeeds at all, returns 503.
#[allow(clippy::too_many_lines)]
async fn list_build_compat_handler(
    Extension(api_state): Extension<HarvestApiState>,
) -> impl axum::response::IntoResponse {
    use std::collections::{HashMap, HashSet};

    use autumn_harvest::build_routing::{BuildCompatEntry, list_build_compat};

    let pool = match api_state.storage_pool().map_err(map_error) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };

    let mut merged: HashMap<(String, String), BuildCompatEntry> = HashMap::new();
    // Per-shard presence: for each shard that successfully read compat, the set of
    // (build_id, compatible_with) pairs it returned. Used to detect pairs that exist
    // on some shards but were missed by others during a partial fanout.
    let mut per_shard_compat_seen: Vec<HashSet<(String, String)>> = Vec::new();
    let mut shard_errors: Vec<serde_json::Value> = Vec::new();
    let mut any_success = false;
    let mut last_err: Option<axum::response::Response> = None;

    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => match list_build_compat(&mut conn).await.map_err(map_error) {
                Ok(entries) => {
                    any_success = true;
                    let mut seen: HashSet<(String, String)> = HashSet::new();
                    for entry in entries {
                        let key = (entry.build_id.clone(), entry.compatible_with.clone());
                        seen.insert(key.clone());
                        merged
                            .entry(key)
                            .and_modify(|e| {
                                if entry.declared_at > e.declared_at {
                                    *e = entry.clone();
                                }
                            })
                            .or_insert(entry);
                    }
                    per_shard_compat_seen.push(seen);
                }
                Err(e) => {
                    shard_errors.push(serde_json::json!({
                        "shard_id": shard_id.as_i32(),
                        "error": e.to_string(),
                    }));
                    last_err = Some(e.into_response());
                }
            },
            Err(e) => {
                shard_errors.push(serde_json::json!({
                    "shard_id": shard_id.as_i32(),
                    "error": e.to_string(),
                }));
                last_err = Some(e.into_response());
            }
        }
    }

    if any_success {
        let mut entries: Vec<BuildCompatEntry> = merged.into_values().collect();
        entries.sort_by(|a, b| {
            a.build_id
                .cmp(&b.build_id)
                .then(a.compatible_with.cmp(&b.compatible_with))
        });

        // Detect pairs present on at least one shard but absent on another.
        let mut diverged_pairs: Vec<serde_json::Value> = if per_shard_compat_seen.len() > 1 {
            let mut pairs: Vec<serde_json::Value> = entries
                .iter()
                .filter(|e| {
                    let key = (e.build_id.clone(), e.compatible_with.clone());
                    per_shard_compat_seen
                        .iter()
                        .any(|seen| !seen.contains(&key))
                })
                .map(|e| {
                    serde_json::json!({
                        "build_id": e.build_id,
                        "compatible_with": e.compatible_with,
                    })
                })
                .collect();
            pairs.sort_by(|a, b| {
                a["build_id"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["build_id"].as_str().unwrap_or(""))
                    .then(
                        a["compatible_with"]
                            .as_str()
                            .unwrap_or("")
                            .cmp(b["compatible_with"].as_str().unwrap_or("")),
                    )
            });
            pairs
        } else {
            vec![]
        };
        diverged_pairs.dedup();

        axum::Json(CompatListResponse {
            entries,
            diverged_pairs,
            shard_errors,
        })
        .into_response()
    } else {
        last_err.unwrap_or_else(|| {
            map_error(autumn_harvest::HarvestError::Config(
                "no shards configured".into(),
            ))
            .into_response()
        })
    }
}

/// `POST /admin/build-routing/compat` — declare that build A can absorb build B's histories.
///
/// Fans out to all shards so every shard's `load_compat_set()` picks up the declaration.
/// Uses fail-forward fan-out: attempts every shard and collects errors.
#[allow(clippy::too_many_lines)]
async fn declare_compat_handler(
    headers: axum::http::HeaderMap,
    Extension(api_state): Extension<HarvestApiState>,
    axum::Json(body): axum::Json<DeclareCompatBody>,
) -> impl axum::response::IntoResponse {
    use autumn_harvest::build_routing::declare_compat;

    let build_id = body.build_id.trim();
    let compatible_with = body.compatible_with.trim();
    if build_id.is_empty() || compatible_with.is_empty() {
        return AutumnError::bad_request_msg("build_id and compatible_with must not be empty")
            .into_response();
    }

    let pool = match api_state.storage_pool().map_err(map_error) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let (actor, source, request_id) = audit_context(&headers, &api_state);

    let mut last_entry = None;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        let mut conn = match acquire_conn(shard_pool).await {
            Ok(c) => c,
            Err(e) => {
                shard_errors.push(format!("shard {}: {e}", shard_id.as_i32()));
                continue;
            }
        };
        match declare_compat(&mut conn, build_id, compatible_with)
            .await
            .map_err(map_error)
        {
            Ok(e) => last_entry = Some(e),
            Err(e) => {
                shard_errors.push(format!("shard {}: {e}", shard_id.as_i32()));
            }
        }
    }

    // If every shard write failed, return 503 before attempting audit.
    if !shard_errors.is_empty() && last_entry.is_none() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({ "errors": shard_errors })),
        )
            .into_response();
    }
    let Some(entry) = last_entry else {
        return map_error(autumn_harvest::HarvestError::Config(
            "no shards configured".into(),
        ))
        .into_response();
    };

    // Audit write is required — fail the response if it cannot be persisted.
    let status = if shard_errors.is_empty() {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    let error_summary = if shard_errors.is_empty() {
        None
    } else {
        Some(shard_errors.join("; "))
    };
    let target = format!("{build_id}→{compatible_with}");
    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_BUILD_COMPAT_DECLARE,
        target_type: TARGET_BUILD_ROUTING,
        target_id: Some(target.as_str()),
        route_or_command: "POST /admin/build-routing/compat",
        request_id: request_id.as_deref(),
        idempotency_key: None,
        status,
        error_summary: error_summary.as_deref(),
        shard_id: None,
        source: &source,
    };
    match api_state.storage_pool() {
        Ok(audit_pool) => match acquire_conn(audit_pool.default_pool()).await {
            Ok(mut conn) => {
                if let Err(audit_err) = audit::insert_audit(&mut conn, &ar).await {
                    tracing::error!(
                        error = %audit_err,
                        "audit insert failed for build-routing.compat.declare"
                    );
                    return AutumnError::service_unavailable_msg(format!(
                        "audit insert failed: {audit_err}"
                    ))
                    .into_response();
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "audit DB connection unavailable for build-routing.compat.declare"
                );
                return AutumnError::service_unavailable_msg("audit DB connection unavailable")
                    .into_response();
            }
        },
        Err(e) => {
            tracing::error!(
                error = %e,
                "storage pool unavailable for audit in build-routing.compat.declare"
            );
            return AutumnError::service_unavailable_msg("audit DB connection unavailable")
                .into_response();
        }
    }

    if shard_errors.is_empty() {
        (axum::http::StatusCode::CREATED, axum::Json(entry)).into_response()
    } else {
        (
            axum::http::StatusCode::MULTI_STATUS,
            axum::Json(serde_json::json!({
                "entry": entry,
                "shard_errors": shard_errors,
            })),
        )
            .into_response()
    }
}

/// `DELETE /admin/build-routing/compat/{build_id}/{compat_with}` — revoke a declaration.
///
/// Fans out to all shards. Returns `revoked=true` if any shard had the row.
/// Uses fail-forward fan-out: attempts every shard and collects errors.
#[allow(clippy::too_many_lines)]
async fn revoke_compat_handler(
    headers: axum::http::HeaderMap,
    Extension(api_state): Extension<HarvestApiState>,
    axum::extract::Path((build_id, compat_with)): axum::extract::Path<(String, String)>,
) -> impl axum::response::IntoResponse {
    use autumn_harvest::build_routing::revoke_compat;

    let pool = match api_state.storage_pool().map_err(map_error) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let (actor, source, request_id) = audit_context(&headers, &api_state);

    let mut any_revoked = false;
    let mut any_shard_succeeded = false;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        let mut conn = match acquire_conn(shard_pool).await {
            Ok(c) => c,
            Err(e) => {
                shard_errors.push(format!("shard {}: {e}", shard_id.as_i32()));
                continue;
            }
        };
        match revoke_compat(&mut conn, &build_id, &compat_with)
            .await
            .map_err(map_error)
        {
            Ok(r) => {
                any_shard_succeeded = true;
                any_revoked |= r;
            }
            Err(e) => {
                shard_errors.push(format!("shard {}: {e}", shard_id.as_i32()));
            }
        }
    }

    // If every shard write failed, return 503 before attempting audit.
    if !shard_errors.is_empty() && !any_shard_succeeded {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(serde_json::json!({ "errors": shard_errors })),
        )
            .into_response();
    }

    // Audit write is required — fail the response if it cannot be persisted.
    let status = if shard_errors.is_empty() {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    let error_summary = if shard_errors.is_empty() {
        None
    } else {
        Some(shard_errors.join("; "))
    };
    let target = format!("{build_id}→{compat_with}");
    let ar = NewAuditRecord {
        actor: &actor,
        operation: OP_BUILD_COMPAT_REVOKE,
        target_type: TARGET_BUILD_ROUTING,
        target_id: Some(target.as_str()),
        route_or_command: "DELETE /admin/build-routing/compat/{build_id}/{compat_with}",
        request_id: request_id.as_deref(),
        idempotency_key: None,
        status,
        error_summary: error_summary.as_deref(),
        shard_id: None,
        source: &source,
    };
    match api_state.storage_pool() {
        Ok(audit_pool) => match acquire_conn(audit_pool.default_pool()).await {
            Ok(mut conn) => {
                if let Err(audit_err) = audit::insert_audit(&mut conn, &ar).await {
                    tracing::error!(
                        error = %audit_err,
                        "audit insert failed for build-routing.compat.revoke"
                    );
                    return AutumnError::service_unavailable_msg(format!(
                        "audit insert failed: {audit_err}"
                    ))
                    .into_response();
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "audit DB connection unavailable for build-routing.compat.revoke"
                );
                return AutumnError::service_unavailable_msg("audit DB connection unavailable")
                    .into_response();
            }
        },
        Err(e) => {
            tracing::error!(
                error = %e,
                "storage pool unavailable for audit in build-routing.compat.revoke"
            );
            return AutumnError::service_unavailable_msg("audit DB connection unavailable")
                .into_response();
        }
    }

    if !shard_errors.is_empty() {
        return (
            axum::http::StatusCode::MULTI_STATUS,
            axum::Json(serde_json::json!({
                "revoked": any_revoked,
                "shard_errors": shard_errors,
            })),
        )
            .into_response();
    }
    axum::Json(RevokeCompatResponse {
        revoked: any_revoked,
    })
    .into_response()
}

/// `POST /admin/build-routing/retire` — confirm a build is safe to retire.
///
/// Returns the reachability snapshot. The caller is responsible for stopping
/// their old workers after confirming `safe_to_retire = true`.
async fn retire_build_handler(
    Extension(api_state): Extension<HarvestApiState>,
    axum::Json(body): axum::Json<RetireBuildBody>,
) -> impl axum::response::IntoResponse {
    use autumn_harvest::build_routing::{all_build_reachability, merge_reachability};

    if body.build_id.trim().is_empty() {
        return AutumnError::bad_request_msg("build_id must not be empty").into_response();
    }

    let pool = match api_state.storage_pool().map_err(map_error) {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let stale_threshold = api_state.worker_stale_threshold();
    let mut per_shard = Vec::new();

    for (shard_id, shard_pool) in pool.iter_shards() {
        let mut conn = match acquire_conn(shard_pool).await {
            Ok(c) => c,
            Err(e) => return e.into_response(),
        };
        let r = match all_build_reachability(&mut conn, stale_threshold).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    shard = shard_id.as_i32(),
                    "build reachability query failed during retire check"
                );
                return map_error(e).into_response();
            }
        };
        per_shard.push(r);
    }
    let merged = merge_reachability(per_shard);
    let reach = merged
        .iter()
        .find(|r| r.build_id == body.build_id.trim())
        .cloned()
        .unwrap_or_else(|| autumn_harvest::build_routing::BuildReachability {
            build_id: body.build_id.trim().to_string(),
            open_executions: 0,
            pending_tasks: 0,
            active_workers: 0,
            stale_workers: 0,
            safe_to_retire: true,
        });

    if reach.safe_to_retire {
        axum::Json(RetireBuildResponse {
            build_id: reach.build_id,
            safe_to_retire: true,
            open_executions: reach.open_executions,
            pending_tasks: reach.pending_tasks,
        })
        .into_response()
    } else {
        (
            axum::http::StatusCode::CONFLICT,
            axum::Json(RetireBuildResponse {
                build_id: reach.build_id,
                safe_to_retire: false,
                open_executions: reach.open_executions,
                pending_tasks: reach.pending_tasks,
            }),
        )
            .into_response()
    }
}

// ── Stuck-task triage eligibility explainer structures (issue #380) ──────────

#[derive(Debug, Serialize, Deserialize)]
pub struct QueueEligibilityResponse {
    pub queue_name: String,
    pub pending_count: i64,
    pub oldest_pending_age_secs: Option<i64>,
    pub required_build_ids: Vec<String>,
    pub eligible_workers: Vec<EligibleWorkerInfo>,
    pub ineligible_workers: Vec<IneligibleWorkerInfo>,
    pub summary: EligibilitySummary,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub shards: std::collections::BTreeMap<String, ShardEligibilityResponse>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shard_errors: Vec<EligibilityShardError>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShardEligibilityResponse {
    pub pending_count: i64,
    pub oldest_pending_age_secs: Option<i64>,
    pub required_build_ids: Vec<String>,
    pub eligible_workers: Vec<EligibleWorkerInfo>,
    pub ineligible_workers: Vec<IneligibleWorkerInfo>,
    pub summary: EligibilitySummary,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EligibilityShardError {
    pub shard_id: i32,
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EligibleWorkerInfo {
    pub worker_id: String,
    pub build_id: String,
    pub deployment_name: Option<String>,
    pub shard_assignments: Vec<i32>,
    pub status: String,
    pub in_flight_count: i32,
    pub max_concurrency: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IneligibleWorkerInfo {
    pub worker_id: String,
    pub reason_codes: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EligibilitySummary {
    pub diagnosis: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskEligibilityResponse {
    pub task_id: uuid::Uuid,
    pub queue_name: String,
    pub pending_count: i64,
    pub oldest_pending_age_secs: Option<i64>,
    pub required_build_id: Option<String>,
    pub assigned_shard: i32,
    pub concurrency_key: Option<String>,
    pub eligible_workers: Vec<EligibleWorkerInfo>,
    pub ineligible_workers: Vec<IneligibleWorkerInfo>,
    pub summary: EligibilitySummary,
}

#[allow(
    clippy::collapsible_if,
    clippy::cast_precision_loss,
    clippy::too_many_lines
)]
async fn evaluate_eligibility_for_shard(
    api_state: &HarvestApiState,
    shard_id: ShardId,
    queue_name: &str,
    target_task_id: Option<uuid::Uuid>,
) -> Result<ShardEligibilityResponse, AutumnError> {
    use std::collections::{HashMap, HashSet};
    let mut conn = db_conn_for_shard(api_state, shard_id).await?;

    let mut tasks = Vec::new();
    if let Some(task_id) = target_task_id {
        let task = harvest_task_queue::table
            .find(task_id)
            .select(autumn_harvest::models::TaskQueueItem::as_select())
            .first::<autumn_harvest::models::TaskQueueItem>(&mut conn)
            .await
            .optional()
            .map_err(database_error)?;
        if let Some(t) = task {
            tasks.push(t);
        }
    } else {
        tasks = harvest_task_queue::table
            .filter(harvest_task_queue::queue_name.eq(queue_name))
            .filter(harvest_task_queue::state.eq("PENDING"))
            .filter(harvest_task_queue::scheduled_at.le(chrono::Utc::now()))
            .filter(
                harvest_task_queue::schedule_to_close_at
                    .is_null()
                    .or(harvest_task_queue::schedule_to_close_at.gt(chrono::Utc::now())),
            )
            .order((
                harvest_task_queue::priority.desc(),
                harvest_task_queue::scheduled_at.asc(),
            ))
            .limit(1000)
            .select(autumn_harvest::models::TaskQueueItem::as_select())
            .load::<autumn_harvest::models::TaskQueueItem>(&mut conn)
            .await
            .map_err(database_error)?;
    }

    let pending_count = if target_task_id.is_some() {
        i64::from(tasks.iter().any(|t| {
            t.state == "PENDING"
                && t.scheduled_at <= chrono::Utc::now()
                && (t.schedule_to_close_at.is_none()
                    || t.schedule_to_close_at.unwrap() > chrono::Utc::now())
        }))
    } else {
        let count: i64 = harvest_task_queue::table
            .filter(harvest_task_queue::queue_name.eq(queue_name))
            .filter(harvest_task_queue::state.eq("PENDING"))
            .filter(harvest_task_queue::scheduled_at.le(chrono::Utc::now()))
            .filter(
                harvest_task_queue::schedule_to_close_at
                    .is_null()
                    .or(harvest_task_queue::schedule_to_close_at.gt(chrono::Utc::now())),
            )
            .count()
            .get_result(&mut conn)
            .await
            .map_err(database_error)?;
        count
    };

    let oldest_pending_age_secs = if target_task_id.is_some() {
        tasks
            .as_slice()
            .first()
            .and_then(|t| {
                if t.state == "PENDING"
                    && t.scheduled_at <= chrono::Utc::now()
                    && (t.schedule_to_close_at.is_none()
                        || t.schedule_to_close_at.unwrap() > chrono::Utc::now())
                {
                    Some(t)
                } else {
                    None
                }
            })
            .map(|t| {
                let age = chrono::Utc::now().signed_duration_since(t.scheduled_at);
                age.num_seconds()
            })
    } else {
        let oldest_scheduled: Option<chrono::DateTime<chrono::Utc>> = harvest_task_queue::table
            .filter(harvest_task_queue::queue_name.eq(queue_name))
            .filter(harvest_task_queue::state.eq("PENDING"))
            .filter(harvest_task_queue::scheduled_at.le(chrono::Utc::now()))
            .filter(
                harvest_task_queue::schedule_to_close_at
                    .is_null()
                    .or(harvest_task_queue::schedule_to_close_at.gt(chrono::Utc::now())),
            )
            .select(harvest_task_queue::scheduled_at)
            .order(harvest_task_queue::scheduled_at.asc())
            .first::<chrono::DateTime<chrono::Utc>>(&mut conn)
            .await
            .optional()
            .map_err(database_error)?;
        oldest_scheduled.map(|ts| {
            let age = chrono::Utc::now().signed_duration_since(ts);
            age.num_seconds()
        })
    };

    let mut required_build_ids = Vec::new();
    for t in tasks.iter().filter(|t| t.state == "PENDING") {
        if let Some(ref bid) = t.required_build_id {
            if !required_build_ids.contains(bid) {
                required_build_ids.push(bid.clone());
            }
        }
    }

    let stale_threshold = api_state.worker_stale_threshold();
    let workers = list_workers(
        &mut conn,
        &WorkerFilters {
            limit: i64::MAX,
            ..Default::default()
        },
        stale_threshold,
    )
    .await
    .map_err(map_error)?;

    let online_workers: Vec<_> = workers
        .into_iter()
        .filter(|w| w.health == autumn_harvest::workers::WorkerHealth::Healthy)
        .collect();

    let compat_set = autumn_harvest::build_routing::load_compat_set(&mut conn)
        .await
        .map_err(map_error)?;

    let mut keys_to_check = Vec::new();
    for t in tasks.iter().filter(|t| t.state == "PENDING") {
        if let Some(ref k) = t.concurrency_key {
            if !keys_to_check.contains(k) {
                keys_to_check.push(k.clone());
            }
        }
    }

    let mut running_map = HashMap::new();
    if !keys_to_check.is_empty() {
        #[derive(diesel::QueryableByName)]
        struct ConcurrencyRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            key: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            task_type: String,
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            running_count: i64,
        }

        let rows: Vec<ConcurrencyRow> = diesel::sql_query(
            "SELECT concurrency_key AS key, task_type, COUNT(*) AS running_count \
             FROM harvest_task_queue \
             WHERE state = 'RUNNING' \
               AND concurrency_key = ANY($1) \
               AND worker_id IS NOT NULL \
             GROUP BY concurrency_key, task_type",
        )
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&keys_to_check)
        .load(&mut conn)
        .await
        .map_err(database_error)?;

        for r in rows {
            running_map.insert((r.key, r.task_type), r.running_count);
        }
    }

    let cb_activities = api_state
        .runtime()
        .ok()
        .map(|r| {
            r.registry()
                .circuit_breakers()
                .tracked_activity_names()
                .to_vec()
        })
        .unwrap_or_default();

    let mut rate_limit_keys = Vec::new();
    for t in tasks.iter().filter(|t| t.state == "PENDING") {
        if let Some(ref rlk) = t.rate_limit_key {
            let has_cb = t
                .activity_name
                .as_ref()
                .is_some_and(|act_name| cb_activities.contains(act_name));
            if !has_cb && !rate_limit_keys.contains(rlk) {
                rate_limit_keys.push(rlk.clone());
            }
        }
    }

    let mut saturated_rate_limits = HashSet::new();
    if !rate_limit_keys.is_empty() {
        #[derive(diesel::QueryableByName)]
        struct RateLimitRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            key: String,
            #[diesel(sql_type = diesel::sql_types::Double)]
            tokens: f64,
            #[diesel(sql_type = diesel::sql_types::Double)]
            burst: f64,
            #[diesel(sql_type = diesel::sql_types::Double)]
            refill_rate: f64,
            #[diesel(sql_type = diesel::sql_types::Timestamptz)]
            last_refilled_at: chrono::DateTime<chrono::Utc>,
        }

        let rows: Vec<RateLimitRow> = diesel::sql_query(
            "SELECT key, tokens, burst, refill_rate, last_refilled_at \
             FROM harvest_rate_limit_buckets \
             WHERE key = ANY($1)",
        )
        .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&rate_limit_keys)
        .load(&mut conn)
        .await
        .map_err(database_error)?;

        for r in rows {
            let elapsed = chrono::Utc::now()
                .signed_duration_since(r.last_refilled_at)
                .num_milliseconds() as f64
                / 1000.0;
            let current_tokens = (r.tokens + elapsed * r.refill_rate).min(r.burst);
            if current_tokens < 1.0 {
                saturated_rate_limits.insert(r.key);
            }
        }
    }

    let mut eligible_workers = Vec::new();
    let mut ineligible_workers = Vec::new();

    let registry = api_state.runtime().map(|r| r.registry().clone()).ok();

    let pending_tasks: Vec<_> = tasks
        .iter()
        .filter(|t| {
            t.state == "PENDING"
                && t.scheduled_at <= chrono::Utc::now()
                && (t.schedule_to_close_at.is_none()
                    || t.schedule_to_close_at.unwrap() > chrono::Utc::now())
        })
        .collect();

    for w in &online_workers {
        let w_id = w.worker.worker_id.clone();
        let build_id = w.worker.build_id.clone();
        let deployment_name = w.worker.deployment_name.clone();
        let shard_assignments: Vec<i32> = w
            .worker
            .shard_assignments
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_i64().and_then(|n| i32::try_from(n).ok()))
                    .collect()
            })
            .unwrap_or_default();
        let status = w.worker.status.clone();

        let w_info = EligibleWorkerInfo {
            worker_id: w_id.clone(),
            build_id,
            deployment_name,
            shard_assignments: shard_assignments.clone(),
            status: status.clone(),
            in_flight_count: w.worker.in_flight_count,
            max_concurrency: w.worker.max_concurrency,
        };

        let mut worker_reasons = Vec::new();

        let subscribed_queues: Vec<String> = w
            .worker
            .queues
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(ToString::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if !subscribed_queues.contains(&queue_name.to_string()) {
            worker_reasons.push("wrong_queue_subscription".to_string());
        }

        if !shard_assignments.contains(&(shard_id.as_i32())) {
            worker_reasons.push("wrong_shard_assignment".to_string());
        }

        if status == "Draining" {
            worker_reasons.push("worker_draining".to_string());
        }

        if status == "Stopped" {
            worker_reasons.push("worker_stopped".to_string());
        }

        if !worker_reasons.is_empty() {
            ineligible_workers.push(IneligibleWorkerInfo {
                worker_id: w_id,
                reason_codes: worker_reasons,
            });
            continue;
        }

        if pending_tasks.is_empty() {
            eligible_workers.push(w_info);
        } else {
            let mut eligible_for_any = false;
            let mut task_failures = Vec::new();

            for t in &pending_tasks {
                let mut reasons = Vec::new();

                if !compat_set.is_eligible(&w.worker.build_id, t.required_build_id.as_deref()) {
                    reasons.push("build_incompatible".to_string());
                }

                if let (Some(sticky_worker), Some(sticky_until)) =
                    (&t.sticky_worker_id, &t.sticky_until)
                {
                    if *sticky_until > chrono::Utc::now() && w.worker.worker_id != *sticky_worker {
                        reasons.push("sticky_owned_by_other_worker".to_string());
                    }
                }

                if let (Some(key), Some(cap)) = (&t.concurrency_key, t.concurrency_cap) {
                    let running = running_map
                        .get(&(key.clone(), t.task_type.clone()))
                        .copied()
                        .unwrap_or(0);
                    if running >= i64::from(cap) {
                        reasons.push("concurrency_saturated".to_string());
                    }
                }

                if let Some(ref rlk) = t.rate_limit_key {
                    let has_cb = t
                        .activity_name
                        .as_ref()
                        .is_some_and(|act_name| cb_activities.contains(act_name));
                    if !has_cb && saturated_rate_limits.contains(rlk) {
                        reasons.push("rate_limit_saturated".to_string());
                    }
                }

                let parsed_reqs = if t.task_type == "activity" {
                    t.required_capabilities.as_ref().map_or_else(
                        || {
                            if let Some(ref act_name) = t.activity_name
                                && let Some(ref reg) = registry
                                && let Some(activity) = reg.activities.get(act_name)
                                && let Some(req_str) = activity.requires
                            {
                                autumn_harvest::eligibility::parse_requirements(req_str).ok()
                            } else {
                                None
                            }
                        },
                        |req_val| {
                            serde_json::from_value::<Vec<autumn_harvest::eligibility::Requirement>>(
                                req_val.clone(),
                            )
                            .ok()
                        },
                    )
                } else {
                    None
                };

                if let Some(reqs) = parsed_reqs {
                    let worker_labels: std::collections::HashMap<String, String> =
                        serde_json::from_value(w.worker.labels.clone()).unwrap_or_default();
                    for req in &reqs {
                        let satisfied = match req {
                            autumn_harvest::eligibility::Requirement::Exact { key, value } => {
                                worker_labels.get(key) == Some(value)
                            }
                            autumn_harvest::eligibility::Requirement::In { key, values } => {
                                worker_labels
                                    .get(key)
                                    .is_some_and(|val| values.contains(val))
                            }
                        };
                        if !satisfied {
                            match req {
                                autumn_harvest::eligibility::Requirement::Exact { key, value } => {
                                    reasons.push(format!("unsatisfied_requirement:{key}={value}"));
                                }
                                autumn_harvest::eligibility::Requirement::In { key, values } => {
                                    reasons.push(format!(
                                        "unsatisfied_requirement:{key} in [{}]",
                                        values.join(", ")
                                    ));
                                }
                            }
                        }
                    }
                }

                if reasons.is_empty() {
                    eligible_for_any = true;
                    break;
                }
                task_failures.push(reasons);
            }

            if eligible_for_any {
                eligible_workers.push(w_info);
            } else {
                let mut merged_reasons = HashSet::new();
                for tf in task_failures {
                    for r in tf {
                        merged_reasons.insert(r);
                    }
                }
                let mut reason_codes: Vec<String> = merged_reasons.into_iter().collect();
                reason_codes.sort();
                if reason_codes.is_empty() {
                    reason_codes.push("unknown".to_string());
                }
                ineligible_workers.push(IneligibleWorkerInfo {
                    worker_id: w_id,
                    reason_codes,
                });
            }
        }
    }

    let num_online = eligible_workers.len() + ineligible_workers.len();
    let diagnosis = if num_online == 0 {
        "no_online_workers".to_string()
    } else {
        let all_draining = eligible_workers.is_empty()
            && !ineligible_workers.is_empty()
            && ineligible_workers
                .iter()
                .all(|w| w.reason_codes == vec!["worker_draining".to_string()]);
        if all_draining {
            "all_draining".to_string()
        } else {
            let eligible_non_draining: Vec<_> = eligible_workers
                .iter()
                .filter(|w| w.status != "Draining")
                .collect();

            if eligible_workers.is_empty() {
                "no_eligible_workers".to_string()
            } else if !eligible_non_draining.is_empty() {
                let mut all_full = true;
                for w_info in &eligible_non_draining {
                    if w_info.in_flight_count < w_info.max_concurrency {
                        all_full = false;
                        break;
                    }
                }
                if all_full {
                    "all_capacity_full".to_string()
                } else {
                    "healthy".to_string()
                }
            } else {
                "healthy".to_string()
            }
        }
    };

    let summary = EligibilitySummary { diagnosis };

    Ok(ShardEligibilityResponse {
        pending_count,
        oldest_pending_age_secs,
        required_build_ids,
        eligible_workers,
        ineligible_workers,
        summary,
    })
}

#[allow(clippy::too_many_lines)]
async fn get_queue_eligibility(
    Extension(api_state): Extension<HarvestApiState>,
    Path(queue_name): Path<String>,
) -> Result<Json<QueueEligibilityResponse>, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;

    let mut shards = std::collections::BTreeMap::new();
    let mut shard_errors = Vec::new();

    let mut global_pending_count = 0;
    let mut global_oldest_pending_age_secs = None;
    let mut global_required_build_ids = std::collections::HashSet::new();

    let mut shard_eligible = std::collections::HashMap::new();
    let mut shard_ineligible = std::collections::HashMap::new();
    let mut online_worker_ids = std::collections::HashSet::new();

    for (shard_id, _shard_pool) in pool.iter_shards() {
        match evaluate_eligibility_for_shard(&api_state, shard_id, &queue_name, None).await {
            Ok(res) => {
                global_pending_count += res.pending_count;
                if let Some(age) = res.oldest_pending_age_secs {
                    global_oldest_pending_age_secs = Some(
                        global_oldest_pending_age_secs
                            .map_or(age, |current| std::cmp::max(current, age)),
                    );
                }
                for bid in &res.required_build_ids {
                    global_required_build_ids.insert(bid.clone());
                }

                shards.insert(shard_id.as_i32().to_string(), res);
            }
            Err(e) => {
                shard_errors.push(EligibilityShardError {
                    shard_id: shard_id.as_i32(),
                    error: e.to_string(),
                });
            }
        }
    }

    if shards.is_empty() && !shard_errors.is_empty() {
        let first_err = shard_errors.remove(0);
        return Err(AutumnError::service_unavailable_msg(format!(
            "failed to inspect any shard. First error on shard {}: {}",
            first_err.shard_id, first_err.error
        )));
    }

    let shards_with_pending: Vec<_> = shards
        .values()
        .filter(|res| res.pending_count > 0)
        .collect();

    let target_shards = if shards_with_pending.is_empty() {
        shards.values().collect::<Vec<_>>()
    } else {
        shards_with_pending
    };

    let mut worst_rank = 0;
    let mut worst_diag = "healthy".to_string();
    for shard_res in &target_shards {
        let diag = &shard_res.summary.diagnosis;
        let rank = match diag.as_str() {
            "no_online_workers" => 4,
            "no_eligible_workers" => 3,
            "all_draining" => 2,
            "all_capacity_full" => 1,
            _ => 0,
        };
        if rank > worst_rank {
            worst_rank = rank;
            worst_diag.clone_from(diag);
        }
    }
    let diagnosis = worst_diag;

    let responsible_shards: Vec<_> = target_shards
        .into_iter()
        .filter(|res| {
            let rank = match res.summary.diagnosis.as_str() {
                "no_online_workers" => 4,
                "no_eligible_workers" => 3,
                "all_draining" => 2,
                "all_capacity_full" => 1,
                _ => 0,
            };
            rank == worst_rank
        })
        .collect();

    for res in responsible_shards {
        let has_pending_tasks = res.pending_count > 0;

        for w in &res.eligible_workers {
            if has_pending_tasks || global_pending_count == 0 {
                shard_eligible.insert(w.worker_id.clone(), w.clone());
            }
            online_worker_ids.insert(w.worker_id.clone());
        }

        for w in &res.ineligible_workers {
            shard_ineligible
                .entry(w.worker_id.clone())
                .or_insert_with(std::collections::HashSet::new)
                .extend(w.reason_codes.iter().cloned());
            online_worker_ids.insert(w.worker_id.clone());
        }
    }

    let mut eligible_workers = Vec::new();
    let mut ineligible_workers = Vec::new();

    for w_id in online_worker_ids {
        if let Some(w_info) = shard_eligible.get(&w_id) {
            eligible_workers.push(w_info.clone());
        } else if let Some(reasons_set) = shard_ineligible.get(&w_id) {
            let mut reason_codes: Vec<String> = reasons_set.iter().cloned().collect();
            reason_codes.sort();
            ineligible_workers.push(IneligibleWorkerInfo {
                worker_id: w_id,
                reason_codes,
            });
        }
    }

    eligible_workers.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
    ineligible_workers.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));

    let mut req_builds: Vec<String> = global_required_build_ids.into_iter().collect();
    req_builds.sort();

    Ok(Json(QueueEligibilityResponse {
        queue_name,
        pending_count: global_pending_count,
        oldest_pending_age_secs: global_oldest_pending_age_secs,
        required_build_ids: req_builds,
        eligible_workers,
        ineligible_workers,
        summary: EligibilitySummary { diagnosis },
        shards,
        shard_errors,
    }))
}

async fn get_task_eligibility(
    Extension(api_state): Extension<HarvestApiState>,
    Path(task_id_str): Path<String>,
) -> Result<Json<TaskEligibilityResponse>, AutumnError> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| AutumnError::bad_request_msg(format!("invalid task id '{task_id_str}'")))?;

    let pool = api_state.storage_pool().map_err(map_error)?;

    for (shard_id, shard_pool) in pool.iter_shards() {
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        let Ok(Some(t)) = harvest_task_queue::table
            .find(task_id)
            .select(autumn_harvest::models::TaskQueueItem::as_select())
            .first::<autumn_harvest::models::TaskQueueItem>(&mut conn)
            .await
            .optional()
        else {
            continue;
        };

        drop(conn);

        let Ok(res) =
            evaluate_eligibility_for_shard(&api_state, shard_id, &t.queue_name, Some(task_id))
                .await
        else {
            continue;
        };

        let mut eligible_workers = res.eligible_workers;
        let mut ineligible_workers = res.ineligible_workers;
        eligible_workers.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
        ineligible_workers.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));

        return Ok(Json(TaskEligibilityResponse {
            task_id,
            queue_name: t.queue_name,
            pending_count: res.pending_count,
            oldest_pending_age_secs: res.oldest_pending_age_secs,
            required_build_id: t.required_build_id,
            assigned_shard: shard_id.as_i32(),
            concurrency_key: t.concurrency_key,
            eligible_workers,
            ineligible_workers,
            summary: res.summary,
        }));
    }

    Err(AutumnError::not_found_msg(format!(
        "task {task_id} not found"
    )))
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
                max_input_bytes: None,
                sla: None,
                owner: None,
                runbook_url: None,
                severity: None,
                description: None,
                input_schema: None,
                output_schema: None,
                error_schema: None,
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
    fn parse_workflow_filters_parses_sla_breached_flag() {
        // Absent → false (issue #487).
        let absent = parse_workflow_filters(&pairs(&[])).expect("empty filters parse");
        assert!(!absent.sla_breached);

        // Explicit true (case-insensitive).
        let on = parse_workflow_filters(&pairs(&[("sla_breached", "TRUE")]))
            .expect("sla_breached=true parses");
        assert!(on.sla_breached);

        // Explicit false.
        let off = parse_workflow_filters(&pairs(&[("sla_breached", "false")]))
            .expect("sla_breached=false parses");
        assert!(!off.sla_breached);

        // Any non-"true" value is treated as false, never an error.
        let other = parse_workflow_filters(&pairs(&[("sla_breached", "1")]))
            .expect("sla_breached=1 parses without error");
        assert!(!other.sla_breached);
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
    fn parse_workflow_children_filters_accepts_paused() {
        // PAUSED is a non-terminal active state (issue #383): it must be a valid
        // child-status filter so operators can narrow children to paused runs.
        let filters = parse_workflow_children_filters(&pairs(&[("status", "Paused")]))
            .expect("Paused is a valid workflow execution state");

        assert_eq!(filters.statuses, vec!["PAUSED".to_string()]);
        assert_eq!(workflow_child_status_label("PAUSED"), "Paused");
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
                priority: Priority::default(),
                max_workflow_input_bytes: 0,
                start_at: None,
                delay: None,
                max_workflow_start_delay: None,
                owner: None,
                runbook_url: None,
                severity: None,
                context_headers: None,

                sla: None,
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
        let result =
            parse_schedule_expr_with_tz("0 9 * * 1-5", "UTC").expect("valid cron+UTC should parse");
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
    fn parse_schedule_expr_with_tz_cron_tz_prefix_round_trips() {
        use autumn_harvest::policy::Schedule;
        // Simulate GET returning the canonical cron_tz: form, then POST-ing it back.
        let canonical = "cron_tz:America/Los_Angeles:0 9 * * 1-5";
        let result = parse_schedule_expr_with_tz(canonical, "UTC")
            .expect("cron_tz: prefixed expr must parse regardless of timezone param");
        assert!(
            matches!(&result, Schedule::CronInTimezone { tz, expr }
                if tz == "America/Los_Angeles" && expr == "0 9 * * 1-5"),
            "embedded timezone must be preserved: {result:?}"
        );
    }

    #[test]
    fn parse_schedule_expr_with_tz_cron_tz_prefix_malformed_is_rejected() {
        let result = parse_schedule_expr_with_tz("cron_tz:MissingColonExpr", "UTC");
        assert!(
            result.is_err(),
            "malformed cron_tz: must be rejected: {result:?}"
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
            calendar_name: None,
            skip_policy: "skip".to_string(),
            consecutive_failure_limit: None,
            consecutive_failure_count: 0,
            auto_paused_at: None,
            end_at: None,
            max_runs: None,
            runs_started: 0,
            remaining_runs: None,
            exhausted_at: None,
            exhausted_reason: None,
            catchup_policy_effective: "skip_all".to_string(),
            catchup_window_secs: None,
            catchup_dropped_last_recovery: 0,
            last_catchup_at: None,
        };
        let json = serde_json::to_string(&entry).expect("serialize");
        assert!(
            json.contains("\"timezone\":\"Asia/Tokyo\""),
            "timezone field must be present in JSON: {json}"
        );
    }

    // ── Schedule next-fires preview API (issue #348) ─────────────────────────

    #[test]
    fn schedule_preview_query_accepts_from_parameter() {
        let q: SchedulePreviewQuery =
            serde_json::from_str(r#"{"count": 5, "from": "2026-06-01T09:00:00Z"}"#)
                .expect("should deserialize");
        assert_eq!(q.count, 5);
        assert_eq!(q.from.as_deref(), Some("2026-06-01T09:00:00Z"));
    }

    #[test]
    fn schedule_preview_query_from_defaults_to_none() {
        let q: SchedulePreviewQuery =
            serde_json::from_str(r#"{"count": 10}"#).expect("should deserialize");
        assert!(q.from.is_none(), "missing `from` must default to None");
    }

    #[test]
    fn parse_from_param_accepts_valid_timestamp() {
        let result = parse_from_param(Some("2026-06-01T09:00:00Z"));
        assert!(result.is_ok(), "valid RFC3339 must parse successfully");
    }

    #[test]
    fn parse_from_param_rejects_far_future_year() {
        let result = parse_from_param(Some("9001-01-01T00:00:00Z"));
        assert!(
            result.is_err(),
            "year 9001 must be rejected to prevent DateTime overflow in schedule expansion"
        );
        assert!(
            result.unwrap_err().contains("year must be"),
            "error message must mention the year limit"
        );
    }

    #[test]
    fn parse_from_param_accepts_year_9000() {
        let result = parse_from_param(Some("9000-12-31T23:59:59Z"));
        assert!(
            result.is_ok(),
            "year 9000 is the boundary and must be accepted"
        );
    }

    #[test]
    fn candidate_schedule_preview_request_parses_minimal() {
        let json = r#"{"schedule_expr":"0 9 * * 1-5"}"#;
        let req: CandidateSchedulePreviewRequest =
            serde_json::from_str(json).expect("should deserialize minimal body");
        assert_eq!(req.schedule_expr, "0 9 * * 1-5");
        assert_eq!(req.timezone, "UTC");
        assert_eq!(req.jitter_secs, 0);
        assert_eq!(req.overlap_policy, "skip");
        assert_eq!(req.count, 10);
        assert!(req.from.is_none());
    }

    #[test]
    fn candidate_schedule_preview_request_parses_full_body() {
        let json = r#"{
            "schedule_expr": "0 9 * * 1-5",
            "timezone": "America/Los_Angeles",
            "jitter_secs": 300,
            "overlap_policy": "cancel_other",
            "count": 20,
            "from": "2026-06-01T09:00:00Z",
            "paused": true
        }"#;
        let req: CandidateSchedulePreviewRequest =
            serde_json::from_str(json).expect("should deserialize full body");
        assert_eq!(req.timezone, "America/Los_Angeles");
        assert_eq!(req.jitter_secs, 300);
        assert_eq!(req.overlap_policy, "cancel_other");
        assert_eq!(req.count, 20);
        assert_eq!(req.from.as_deref(), Some("2026-06-01T09:00:00Z"));
        assert!(req.paused);
    }

    #[test]
    fn build_preview_entry_reason_is_cron_without_jitter() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        let fire = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 9, 0, 0).unwrap();
        let preview = ScheduleFirePreview {
            scheduled_at: fire,
            effective_at: Some(fire),
            reason: "Fired".to_string(),
        };
        // pre_jitter_base = Some(fire) — no calendar adjustment, no jitter.
        let entry = build_preview_entry(&preview, Some(fire), 0, "UTC", "skip");
        assert_eq!(entry.reason, "cron", "no-jitter reason must be 'cron'");
        assert!(entry.jitter_earliest_at.is_none());
        assert!(entry.jitter_latest_at.is_none());
    }

    #[test]
    fn build_preview_entry_reason_is_cron_plus_jitter_with_jitter() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        let fire = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 9, 0, 0).unwrap();
        let effective = fire + chrono::Duration::seconds(120);
        let preview = ScheduleFirePreview {
            scheduled_at: fire,
            effective_at: Some(effective), // post-jitter effective_at
            reason: "Fired".to_string(),
        };
        // pre_jitter_base = Some(fire) — calendar-adjusted base BEFORE jitter.
        let entry = build_preview_entry(&preview, Some(fire), 300, "UTC", "skip");
        assert_eq!(
            entry.reason, "cron+jitter",
            "jitter reason must be 'cron+jitter'"
        );
        assert!(
            entry.jitter_earliest_at.is_some(),
            "must have jitter_earliest_at"
        );
        assert!(
            entry.jitter_latest_at.is_some(),
            "must have jitter_latest_at"
        );
        assert_eq!(
            entry.jitter_earliest_at.unwrap(),
            fire,
            "jitter_earliest_at must equal pre-jitter base"
        );
        assert_eq!(
            entry.jitter_latest_at.unwrap(),
            fire + chrono::Duration::seconds(300),
            "jitter_latest_at must equal pre_jitter_base + jitter_secs"
        );
    }

    #[test]
    fn build_preview_entry_jitter_bounds_use_deferred_date_not_original() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        // Simulates a July 4 holiday deferred to July 5 with jitter applied.
        let original = chrono::Utc.with_ymd_and_hms(2026, 7, 4, 9, 0, 0).unwrap();
        let deferred_base = chrono::Utc.with_ymd_and_hms(2026, 7, 5, 9, 0, 0).unwrap();
        let deferred_jittered = deferred_base + chrono::Duration::seconds(42);
        let preview = ScheduleFirePreview {
            scheduled_at: original,
            effective_at: Some(deferred_jittered), // calendar-adjusted + jitter
            reason: "DeferredFrom:2026-07-04".to_string(),
        };
        // pre_jitter_base = Some(deferred_base) — calendar-adjusted BEFORE jitter.
        let entry = build_preview_entry(&preview, Some(deferred_base), 300, "UTC", "skip");
        assert_eq!(
            entry.jitter_earliest_at.unwrap(),
            deferred_base,
            "jitter_earliest_at must be the deferred date, not the original excluded date"
        );
        assert_eq!(
            entry.jitter_latest_at.unwrap(),
            deferred_base + chrono::Duration::seconds(300),
            "jitter_latest_at must be deferred_base + jitter_secs"
        );
    }

    #[test]
    fn build_preview_entry_reason_is_skipped_calendar_excluded() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        let fire = chrono::Utc.with_ymd_and_hms(2026, 7, 4, 9, 0, 0).unwrap();
        let preview = ScheduleFirePreview {
            scheduled_at: fire,
            effective_at: None,
            reason: "SkippedByCalendar:us-holidays".to_string(),
        };
        // pre_jitter_base = None — suppressed entries have no base.
        let entry = build_preview_entry(&preview, None, 0, "UTC", "skip");
        assert_eq!(
            entry.reason, "skipped:calendar-excluded",
            "calendar-suppressed reason must be 'skipped:calendar-excluded'"
        );
        assert!(entry.effective_at.is_none());
    }

    #[test]
    fn build_preview_entry_reason_is_deferred_calendar() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        let fire = chrono::Utc.with_ymd_and_hms(2026, 7, 4, 9, 0, 0).unwrap();
        let deferred = chrono::Utc.with_ymd_and_hms(2026, 7, 5, 9, 0, 0).unwrap();
        let preview = ScheduleFirePreview {
            scheduled_at: fire,
            effective_at: Some(deferred),
            reason: "DeferredFrom:2026-07-04".to_string(),
        };
        let entry = build_preview_entry(&preview, Some(deferred), 0, "UTC", "skip");
        assert_eq!(
            entry.reason, "deferred:calendar",
            "calendar-deferred reason must be 'deferred:calendar'"
        );
    }

    #[test]
    fn build_preview_entry_would_skip_true_when_overlap_is_skip() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        let fire = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 9, 0, 0).unwrap();
        let preview = ScheduleFirePreview {
            scheduled_at: fire,
            effective_at: Some(fire),
            reason: "Fired".to_string(),
        };
        let entry = build_preview_entry(&preview, Some(fire), 0, "UTC", "skip");
        assert!(
            entry.would_skip_if_active,
            "skip overlap policy must set would_skip_if_active=true"
        );
    }

    #[test]
    fn build_preview_entry_would_skip_false_when_overlap_is_cancel_other() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        let fire = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 9, 0, 0).unwrap();
        let preview = ScheduleFirePreview {
            scheduled_at: fire,
            effective_at: Some(fire),
            reason: "Fired".to_string(),
        };
        let entry = build_preview_entry(&preview, Some(fire), 0, "UTC", "cancel_other");
        assert!(
            !entry.would_skip_if_active,
            "cancel_other overlap policy must set would_skip_if_active=false"
        );
    }

    #[test]
    fn build_preview_entry_would_skip_false_when_suppressed_by_calendar() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        let fire = chrono::Utc.with_ymd_and_hms(2026, 7, 4, 9, 0, 0).unwrap();
        let preview = ScheduleFirePreview {
            scheduled_at: fire,
            effective_at: None, // suppressed
            reason: "SkippedByCalendar:us-holidays".to_string(),
        };
        // Even with skip policy, a suppressed entry cannot be skipped-if-active.
        let entry = build_preview_entry(&preview, None, 0, "UTC", "skip");
        assert!(
            !entry.would_skip_if_active,
            "suppressed entries (effective_at=None) must not set would_skip_if_active"
        );
    }

    #[test]
    fn build_preview_entry_would_skip_true_when_max_active_runs_is_zero() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        let fire = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 9, 0, 0).unwrap();
        let preview = ScheduleFirePreview {
            scheduled_at: fire,
            effective_at: Some(fire),
            reason: "Fired".to_string(),
        };
        // max_active_runs = 0 means the scheduler's running >= max_active_runs check is
        // always true, so every firing is dropped. The advisory must fire.
        let entry = build_preview_entry(&preview, Some(fire), 0, "UTC", "skip");
        assert!(
            entry.would_skip_if_active,
            "max_active_runs=0 with skip policy must set would_skip_if_active=true"
        );
    }

    #[test]
    fn build_preview_entry_jitter_latest_at_is_none_for_oversized_jitter() {
        use autumn_harvest::calendar::ScheduleFirePreview;
        use chrono::TimeZone as _;
        let fire = chrono::Utc.with_ymd_and_hms(2026, 6, 1, 9, 0, 0).unwrap();
        let preview = ScheduleFirePreview {
            scheduled_at: fire,
            effective_at: Some(fire),
            reason: "Fired".to_string(),
        };
        // i64::MAX seconds overflows chrono Duration creation; must not panic.
        let entry = build_preview_entry(&preview, Some(fire), i64::MAX, "UTC", "skip");
        assert!(
            entry.jitter_earliest_at.is_some(),
            "jitter_earliest_at must still be present"
        );
        assert!(
            entry.jitter_latest_at.is_none(),
            "jitter_latest_at must be None when jitter_secs overflows DateTime"
        );
    }

    #[test]
    fn management_api_routes_includes_post_schedule_preview() {
        let routes = management_api_routes();
        assert!(
            routes.contains(&("POST", "/admin/schedules/preview")),
            "POST /admin/schedules/preview must be listed in management_api_routes; found: {routes:?}"
        );
    }

    #[test]
    fn management_api_response_fields_includes_post_schedule_preview() {
        let fields = management_api_response_fields();
        assert!(
            fields
                .iter()
                .any(|(m, p, _)| *m == "POST" && *p == "/admin/schedules/preview"),
            "POST /admin/schedules/preview must be listed in management_api_response_fields"
        );
    }

    // ── Candidate preview handler validation (issue #348 follow-up) ──────────

    #[test]
    fn candidate_request_invalid_skip_policy_fails_deserialization_gracefully() {
        // The struct accepts any string (validated at handler time), so
        // deserialization itself must succeed — handler rejects the bad value.
        let json = r#"{"schedule_expr":"0 9 * * *","skip_policy":"never"}"#;
        let req: CandidateSchedulePreviewRequest =
            serde_json::from_str(json).expect("struct accepts any string for skip_policy");
        assert_eq!(req.skip_policy, "never");
    }

    #[test]
    fn candidate_request_invalid_overlap_policy_fails_deserialization_gracefully() {
        let json = r#"{"schedule_expr":"0 9 * * *","overlap_policy":"unknown"}"#;
        let req: CandidateSchedulePreviewRequest =
            serde_json::from_str(json).expect("struct accepts any string for overlap_policy");
        assert_eq!(req.overlap_policy, "unknown");
    }

    #[test]
    fn candidate_request_large_jitter_u64_parses_into_struct() {
        // u64::MAX is representable in JSON and should parse into the struct;
        // the handler rejects it via validate_jitter (not a parse error).
        let json = format!(
            r#"{{"schedule_expr":"0 9 * * *","jitter_secs":{}}}"#,
            u64::MAX
        );
        let req: CandidateSchedulePreviewRequest =
            serde_json::from_str(&json).expect("u64::MAX must parse into the struct");
        assert_eq!(req.jitter_secs, u64::MAX);
    }

    #[test]
    fn parse_schedule_error_timezone_field_attributed_correctly() {
        // "Not/ATimezone" error message from validate_schedule contains "timezone".
        let result = parse_schedule_expr_with_tz("0 9 * * *", "Not/ATimezone");
        let err = result.unwrap_err();
        let field = if err.to_lowercase().contains("timezone") || err.to_lowercase().contains("tz")
        {
            "timezone"
        } else {
            "schedule_expr"
        };
        assert_eq!(
            field, "timezone",
            "timezone parse error must be attributed to 'timezone' field, not 'schedule_expr': {err}"
        );
    }

    #[test]
    fn parse_schedule_error_bad_cron_attributed_to_schedule_expr() {
        let result = parse_schedule_expr_with_tz("not a cron", "UTC");
        let err = result.unwrap_err();
        let field = if err.to_lowercase().contains("timezone") || err.to_lowercase().contains("tz")
        {
            "timezone"
        } else {
            "schedule_expr"
        };
        assert_eq!(
            field, "schedule_expr",
            "cron parse error must be attributed to 'schedule_expr': {err}"
        );
    }

    // ── issue #373: registered workflow schema endpoint tests ─────────────────

    fn make_schema_registry() -> Arc<HandlerRegistry> {
        fn my_input_schema() -> serde_json::Value {
            serde_json::json!({
                "type": "object",
                "properties": {
                    "user_id": {"type": "integer"},
                    "email": {"type": "string"}
                },
                "required": ["user_id", "email"]
            })
        }

        Arc::new(HandlerRegistry::new(
            vec![
                autumn_harvest::WorkflowInfo {
                    name: "schema_wf",
                    module: "tests",
                    handler: |_ctx, input| Box::pin(async move { Ok(input) }),
                    execution_timeout: None,
                    concurrency: None,
                    max_input_bytes: None,
                    sla: None,
                    owner: None,
                    runbook_url: None,
                    severity: None,
                    description: Some("A workflow with an input schema"),
                    input_schema: Some(my_input_schema),
                    output_schema: None,
                    error_schema: None,
                },
                autumn_harvest::WorkflowInfo {
                    name: "no_schema_wf",
                    module: "tests",
                    handler: |_ctx, input| Box::pin(async move { Ok(input) }),
                    execution_timeout: None,
                    concurrency: None,
                    max_input_bytes: None,
                    sla: None,
                    owner: None,
                    runbook_url: None,
                    severity: None,
                    description: None,
                    input_schema: None,
                    output_schema: None,
                    error_schema: None,
                },
            ],
            vec![],
        ))
    }

    #[test]
    fn registered_workflow_record_from_info_includes_schema() {
        let registry = make_schema_registry();
        let info = registry.workflows.get("schema_wf").unwrap();
        let record = autumn_harvest::info::RegisteredWorkflowRecord::from_info(info);

        assert_eq!(record.name, "schema_wf");
        assert_eq!(
            record.description.as_deref(),
            Some("A workflow with an input schema")
        );
        assert!(
            record.input_schema.is_some(),
            "input_schema must be present"
        );
        assert_eq!(
            record.input_schema.as_ref().unwrap()["type"],
            "object",
            "schema type must be 'object'"
        );
        assert!(record.output_schema.is_none());
        assert!(record.error_schema.is_none());
    }

    #[test]
    fn registered_workflow_record_from_info_nulls_for_no_schema() {
        let registry = make_schema_registry();
        let info = registry.workflows.get("no_schema_wf").unwrap();
        let record = autumn_harvest::info::RegisteredWorkflowRecord::from_info(info);

        assert_eq!(record.name, "no_schema_wf");
        assert!(record.description.is_none());
        assert!(record.input_schema.is_none());
        assert!(record.output_schema.is_none());
        assert!(record.error_schema.is_none());

        // Serialized JSON: input_schema / output_schema / error_schema are `null`
        let json = serde_json::to_value(&record).unwrap();
        assert!(json["input_schema"].is_null());
        assert!(json["output_schema"].is_null());
        assert!(json["error_schema"].is_null());
        // description is omitted (skip_serializing_if)
        assert!(
            json.get("description")
                .is_none_or(serde_json::Value::is_null)
        );
    }

    #[test]
    fn validate_input_rejects_missing_required_field() {
        let registry = make_schema_registry();
        let info = registry.workflows.get("schema_wf").unwrap();

        // Missing "email"
        let bad_input = serde_json::json!({"user_id": 42});
        let err = info.validate_input(&bad_input).unwrap_err();
        assert!(!err.is_empty(), "should have at least one violation");
        let found = err
            .iter()
            .any(|v| v.field_path.as_deref().is_some_and(|p| p.contains("email")));
        assert!(found, "violation must reference the 'email' field path");
    }

    #[test]
    fn validate_input_accepts_valid_input() {
        let registry = make_schema_registry();
        let info = registry.workflows.get("schema_wf").unwrap();

        let good = serde_json::json!({"user_id": 1, "email": "a@b.com"});
        assert!(
            info.validate_input(&good).is_ok(),
            "valid input must pass schema validation"
        );
    }

    #[test]
    fn validate_input_passes_through_when_no_schema() {
        let registry = make_schema_registry();
        let info = registry.workflows.get("no_schema_wf").unwrap();

        let any = serde_json::json!({"totally": "arbitrary"});
        assert!(
            info.validate_input(&any).is_ok(),
            "no-schema workflow must accept any input"
        );
    }

    #[test]
    fn schema_violation_serialises_with_field_path() {
        let v = autumn_harvest::info::SchemaViolation {
            message: "missing required field 'name'".to_string(),
            field_path: Some("/name".to_string()),
        };
        let json = serde_json::to_value(&v).unwrap();
        assert_eq!(json["message"], "missing required field 'name'");
        assert_eq!(json["field_path"], "/name");
    }

    // ── update-with-start route registration (issue #479) ────────────────────

    #[test]
    fn management_api_routes_includes_post_update_with_start() {
        let routes = management_api_routes();
        assert!(
            routes.contains(&("POST", "/workflows/{workflow_name}/update-with-start")),
            "POST /workflows/{{workflow_name}}/update-with-start must be listed in management_api_routes; found: {routes:?}"
        );
    }

    #[test]
    fn management_api_request_fields_includes_update_with_start() {
        let fields = management_api_request_fields();
        let entry = fields
            .iter()
            .find(|(m, p, _)| *m == "POST" && *p == "/workflows/{workflow_name}/update-with-start");
        assert!(
            entry.is_some(),
            "POST /workflows/{{workflow_name}}/update-with-start must be in management_api_request_fields"
        );
        let (_, _, body_fields) = entry.unwrap();
        let body = body_fields.expect("update-with-start must have a structured body");
        assert!(
            body.contains(&"workflow_id"),
            "must include workflow_id field"
        );
        assert!(
            body.contains(&"update_name"),
            "must include update_name field"
        );
        assert!(
            body.contains(&"update_args"),
            "must include update_args field"
        );
        assert!(
            body.contains(&"start_input"),
            "must include start_input field"
        );
    }

    #[test]
    fn management_api_response_fields_includes_update_with_start() {
        let fields = management_api_response_fields();
        assert!(
            fields
                .iter()
                .any(|(m, p, _)| *m == "POST"
                    && *p == "/workflows/{workflow_name}/update-with-start"),
            "POST /workflows/{{workflow_name}}/update-with-start must be in management_api_response_fields"
        );
    }
}
