//! Worker runtime — the main poll loop that claims and dispatches tasks.
//!
//! Each [`Worker`] runs a `tokio::select!`-driven loop: it either receives a
//! shutdown signal or polls the task queue for work. Claimed tasks are dispatched
//! via Tokio tasks bounded by semaphores so that at most
//! `max_concurrent_workflows` workflow tasks and `max_concurrent_activities`
//! activity tasks run concurrently on a single worker.
//!
//! The worker is deliberately "dumb" — it claims a row, looks up the handler in
//! the [`HandlerRegistry`], and spawns a task. The actual execution semantics
//! (replay, retries, heartbeats) live in the executor and context modules.

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use scoped_futures::ScopedFutureExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::builder::WorkerConfig;
use crate::completion_trigger::DeferredTriggerStart;
#[cfg(feature = "db")]
use crate::context::TransactionalState;
use crate::context::{
    ActivityContext, SharedState, WorkflowCommand, WorkflowHistoryPolicy, empty_shared_state,
};
use crate::dlq::{self, DeadLetterReason, NewDeadLetterEntry};
use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::execution::{
    apply_parent_close_cascade, cancel_workflow_execution_collect,
    check_and_report_unfinished_handlers, parent_close_cascade_event_count,
};
use crate::executor::{
    WorkflowExecuteSpanMeta, WorkflowOutcome, run_workflow_with_state_history_policy_and_caps,
};
use crate::external_task;
use crate::failure::{
    failure_is_non_retryable, parse_error_payload, parse_error_payload_full, parse_typed_payload,
};
use crate::info::{ActivityInfo, QueryHandlerInfo, UpdateHandlerInfo, WorkflowInfo};
use crate::models::{
    HarvestTimer, NewHarvestTimer, NewWorkflowExecution, TaskQueueItem, WorkflowExecution,
};
use crate::policy::RetryPolicy;
use crate::queue::{self, TaskType};
use crate::schema::{harvest_timers, harvest_workflow_executions};
use crate::signal;
use crate::store;
use crate::telemetry::{
    ATTR_ACTIVITY_NAME, ATTR_ATTEMPT, ATTR_EXECUTION_ID, ATTR_QUEUE, ATTR_SHARD_ID,
    ATTR_WORKFLOW_ID, ActivityStatus, SlotType, TraceContextCarrier, WorkflowStatus,
};
use crate::types::{
    ActivityExecId, ExecutionId, ExternalActivityToken, IdempotencyKey, ParentClosePolicy, TimerId,
    WorkerId,
};

/// Type alias for the deadpool-managed async Diesel connection pool.
pub type DbPool = deadpool::managed::Pool<
    diesel_async::pooled_connection::AsyncDieselConnectionManager<diesel_async::AsyncPgConnection>,
>;

// ---------------------------------------------------------------------------
// WorkerRuntimeConfig
// ---------------------------------------------------------------------------

/// Default interval between queue poll attempts when a worker is idle.
///
/// This is the single source of truth for the runtime poll interval: the
/// `From<WorkerConfig>` conversion below sets `poll_interval` from it, and
/// side-effect-free callers (e.g. the effective-config introspection snapshot,
/// issue #695) can read it directly instead of constructing a
/// [`WorkerRuntimeConfig`] — whose conversion writes the write-once
/// `GLOBAL_DEFAULT_WORKFLOW_QUEUE` and so must not run on a read-only path.
pub const DEFAULT_WORKER_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Ceiling for the overdue-schedule gauge's adaptive sampling interval (issue
/// #696).
///
/// For a slow / Manual-only fleet the overdue signal changes on minute-scale
/// cadence grace, so sampling it at the sub-second `poll_interval` (which runs
/// an unindexed RUNNING/PAUSED count per schedule × shards × workers) is
/// wasteful for no benefit — the sampler stays at this 30s ceiling. When a
/// fast (sub-30s) schedule is present the sampler instead adapts down toward its
/// cadence (never below the `poll_interval` floor) so `schedule_overdue` is
/// detected within its grace window (Codex round 4). See
/// [`next_overdue_sample_interval`].
pub const SCHEDULE_OVERDUE_SAMPLE_MAX: Duration = Duration::from_secs(30);

/// Compute the overdue sampler's next sleep interval (issue #696, Codex round 4).
///
/// Adapts to the fleet's fastest active cadence so the `harvest.schedule.overdue`
/// gauge is refreshed within the detection grace window even for sub-30s
/// schedules, while an all-slow / no-cadence fleet stays at the
/// [`SCHEDULE_OVERDUE_SAMPLE_MAX`] ceiling (the common case, preserving the
/// coarse-sampling perf win). The result is clamped to
/// `[poll_interval, SCHEDULE_OVERDUE_SAMPLE_MAX]` so it never busy-spins below
/// the worker poll interval. `min_cadence_step == None` (no active cadence-bearing
/// schedule) → the ceiling.
///
/// The floor is `poll_interval.min(ceiling)` so a deployment configured with a
/// `poll_interval` larger than 30s can never invert the clamp (it simply pins
/// the interval at the 30s ceiling).
#[must_use]
pub fn next_overdue_sample_interval(
    min_cadence_step: Option<Duration>,
    poll_interval: Duration,
) -> Duration {
    let ceiling = SCHEDULE_OVERDUE_SAMPLE_MAX;
    let floor = poll_interval.min(ceiling);
    min_cadence_step.map_or(ceiling, |step| step.clamp(floor, ceiling))
}

/// A `harvest.schedule.overdue` gauge series label: `(kind, name)`.
type ScheduleGaugeKey = (String, String);

/// Gauge labels to reset to `0` after an overdue sampling pass (issue #696,
/// Codex round 5).
///
/// When a schedule is deleted or renamed, a later pass stops emitting its
/// `(kind, name)` gauge series, but both the built-in scrape recorder and
/// metrics-rs retain the last value — so `harvest.schedule.overdue` would stay
/// at `1` for a gone schedule (keeping the alert firing) until the worker
/// restarts. This returns the previously-emitted labels absent from the current
/// pass so the caller can zero them.
///
/// **Safety gate:** returns empty when `pass_complete == false` (some shard
/// errored). A partial pass legitimately omits every label on the failed shard,
/// so zeroing "disappeared" labels then would wrongly clear a genuinely-overdue
/// schedule living on the unreachable shard.
#[must_use]
pub fn labels_to_clear<S: std::hash::BuildHasher>(
    previous: &std::collections::HashSet<ScheduleGaugeKey, S>,
    current: &std::collections::HashSet<ScheduleGaugeKey, S>,
    pass_complete: bool,
) -> Vec<ScheduleGaugeKey> {
    if !pass_complete {
        return Vec::new();
    }
    previous.difference(current).cloned().collect()
}

/// Validated, runtime-ready worker configuration.
///
/// Built from [`WorkerConfig`] (the user-facing builder) via `From`, which
/// auto-generates a unique worker ID.
#[derive(Debug, Clone)]
pub struct WorkerRuntimeConfig {
    /// Unique identifier for this worker instance.
    pub worker_id: String,
    /// Queue names this worker polls.
    pub queues: Vec<String>,
    /// Optional per-queue dispatch weights (issue #515). Empty = default unchanged behaviour.
    pub queue_weights: std::collections::HashMap<String, u32>,
    /// Optional Postgres URL for LISTEN/NOTIFY wakeups.
    pub notification_database_url: Option<String>,
    /// Optional per-shard LISTEN/NOTIFY database URLs for multi-shard workers
    /// (issue #522). Shards absent from this list fall back to polling.
    pub shard_notification_database_urls: Vec<(crate::types::ShardId, String)>,
    /// Maximum concurrent workflow task executions.
    pub max_concurrent_workflows: usize,
    /// Maximum concurrent activity task executions.
    pub max_concurrent_activities: usize,
    /// Interval between queue poll attempts when idle.
    pub poll_interval: Duration,
    /// Maximum time to wait for in-flight tasks during shutdown.
    pub shutdown_timeout: Duration,
    /// Grace period for an activity handler to unwind cooperatively after
    /// its workflow is cancelled before the worker hard-aborts it.
    pub cancellation_grace_period: Duration,
    /// Grace period during which subsequent tasks for a workflow are offered
    /// preferentially to this worker so its in-process LRU cache stays warm.
    /// Zero disables sticky routing entirely.
    pub sticky_timeout: Duration,
    /// Hard cap applied to each local activity attempt. If the activity does
    /// not complete within this window it is treated as a failure and retried
    /// (or the workflow fails if retries are exhausted).
    pub max_local_activity_start_to_close: Duration,
    /// Shard IDs this worker polls. Recorded in `harvest_workers` for fleet
    /// observability. Defaults to `[0]` for single-shard deployments.
    pub shard_assignments: Vec<crate::types::ShardId>,
    /// Heartbeat interval for worker liveness records in `harvest_workers`.
    /// Defaults to 5 seconds. Stale threshold is `2 × heartbeat_interval`.
    pub worker_heartbeat_interval: Duration,
    /// Immutable build identifier for this worker (issue #171).
    pub build_id: String,
    /// Optional deployment name for operator observability (issue #171).
    pub deployment_name: Option<String>,
    /// Maximum number of entries in the per-worker in-process LRU workflow
    /// state cache (issue #235). Defaults to 1000.
    pub workflow_cache_size: usize,
    /// Anti-starvation aging period (issue #249). Passed to `claim_task` so
    /// the claim SQL can boost effective priority for long-waiting tasks.
    /// `None` disables aging.
    pub priority_aging_secs: Option<u32>,
    /// Grace window before cross-workflow signaling fails for unknown target (issue #330).
    pub unknown_target_grace_window: Duration,
    /// Consecutive worker crashes a task may cause before quarantine (issue #367).
    /// `0` disables quarantine (reclaimed poison pills are re-queued forever).
    pub poison_pill_threshold: i32,
    /// Maximum wall-clock time a single workflow-task dispatch may run before
    /// the worker reclaims the concurrency slot (issue #494).
    /// `Duration::ZERO` disables the timeout (unbounded dispatch time).
    pub workflow_task_timeout: Duration,
    /// Maximum panic re-dispatches before a panicking-workflow run fails
    /// terminally (issue #782). `0` fails terminally on the first panic.
    pub workflow_panic_max_attempts: u32,
    /// Bounded-pause ceiling before the auto-resume scanner force-resumes a
    /// paused execution (issue #383). Default 24 hours.
    pub max_workflow_pause_duration: Duration,
    /// Capability labels for hardware-aware and regional routing (issue #382).
    pub labels: std::collections::HashMap<String, String>,
    #[cfg(feature = "db")]
    /// Optional sharded database pool for exact shard routing.
    pub sharded_pool: Option<crate::shard::ShardedDbPool>,
    /// Hard ceiling on durable event count per execution (issue #493). `None` = no ceiling.
    pub max_workflow_history_events: Option<u64>,
    /// Opt-in adaptive dispatch-slot tuner (issue #548). `None` = today's
    /// fixed-concurrency semaphore behaviour, byte-for-byte unchanged.
    pub slot_tuner: Option<crate::slot_tuner::SlotTunerConfig>,
    /// Advertised worker-session capacity (issue #606). `0` (the default)
    /// means sessions are disabled on this worker.
    pub max_concurrent_sessions: i32,
}

impl WorkerRuntimeConfig {
    /// Validate this configuration.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if `queues` is empty.
    pub fn validate(&self) -> HarvestResult<()> {
        if self.queues.is_empty() {
            return Err(HarvestError::Config(
                "worker must poll at least one queue".into(),
            ));
        }
        // Warn when queue_weights contains keys that are not in the queues list.
        // Those entries are silently ignored by effective_queue_weights, which
        // only iterates over self.queues, so misconfigured keys are invisible
        // at runtime.
        if !self.queue_weights.is_empty() {
            let queue_set: std::collections::HashSet<&str> =
                self.queues.iter().map(String::as_str).collect();
            for name in self.queue_weights.keys() {
                if !queue_set.contains(name.as_str()) {
                    tracing::warn!(
                        queue = %name,
                        "queue_weights entry has no matching bound queue and will be ignored"
                    );
                }
            }
        }
        // A degenerate band (min_slots > max_slots, or a configured value
        // outside the band) never fails worker startup — it degrades to an
        // inert (but harmless) tuner, matching the queue_weights precedent
        // above. `max_slots == 0` is different in kind, not just degree: it
        // makes the dispatch semaphore permanently empty, so the worker
        // would register as healthy in the fleet table and heartbeat
        // normally while being structurally unable to dispatch a single
        // task — a silent, hard-to-diagnose outage. That case is rejected
        // here instead of merely warned about.
        if let Some(tuner) = &self.slot_tuner {
            if tuner.max_slots == 0 {
                return Err(HarvestError::Config(
                    "slot_tuner max_slots is 0; the dispatch semaphore would have zero permits \
                     and this worker could never dispatch a task"
                        .into(),
                ));
            }
            // `min_slots == 0` is a liveness hazard, not just a degenerate
            // band: the default controller's grow signal depends on
            // observing a claim-to-dispatch permit wait, which requires a
            // task to actually be dispatched. If pool pressure ever shrinks
            // the target to 0, no task can dispatch, so no permit wait is
            // ever recorded and the worker is permanently stuck at zero
            // capacity — the same silent-outage shape as `max_slots == 0`.
            if tuner.min_slots == 0 {
                return Err(HarvestError::Config(
                    "slot_tuner min_slots is 0; the tuner could shrink to 0 slots and become \
                     permanently stuck there, since no task could dispatch to ever trigger a \
                     grow decision"
                        .into(),
                ));
            }
            for warning in crate::slot_tuner::validate_band(
                tuner.min_slots,
                tuner.max_slots,
                self.max_concurrent_workflows,
            ) {
                tracing::warn!(slot_type = "workflow", "{warning}");
            }
            for warning in crate::slot_tuner::validate_band(
                tuner.min_slots,
                tuner.max_slots,
                self.max_concurrent_activities,
            ) {
                tracing::warn!(slot_type = "activity", "{warning}");
            }
        }
        Ok(())
    }
}

impl From<WorkerConfig> for WorkerRuntimeConfig {
    fn from(cfg: WorkerConfig) -> Self {
        if let Some(first_queue) = cfg.queues.as_slice().first()
            && let Ok(mut lock) = crate::completion_trigger::GLOBAL_DEFAULT_WORKFLOW_QUEUE.write()
            && lock.is_none()
        {
            *lock = Some(first_queue.clone());
        }
        Self {
            worker_id: uuid::Uuid::new_v4().to_string(),
            queues: cfg.queues,
            queue_weights: cfg.queue_weights,
            notification_database_url: cfg.notification_database_url,
            shard_notification_database_urls: cfg.shard_notification_database_urls,
            max_concurrent_workflows: cfg.max_concurrent_workflows,
            max_concurrent_activities: cfg.max_concurrent_activities,
            poll_interval: DEFAULT_WORKER_POLL_INTERVAL,
            shutdown_timeout: cfg.shutdown_timeout,
            cancellation_grace_period: cfg.cancellation_grace_period,
            sticky_timeout: cfg.sticky_timeout,
            max_local_activity_start_to_close: cfg.max_local_activity_start_to_close,
            shard_assignments: cfg.shard_assignments,
            worker_heartbeat_interval: cfg.worker_heartbeat_interval,
            build_id: cfg.build_id,
            deployment_name: cfg.deployment_name,
            workflow_cache_size: cfg.workflow_cache_size,
            priority_aging_secs: cfg.priority_aging_secs,
            unknown_target_grace_window: cfg.unknown_target_grace_window,
            poison_pill_threshold: cfg.poison_pill_threshold,
            workflow_task_timeout: cfg.workflow_task_timeout,
            workflow_panic_max_attempts: cfg.workflow_panic_max_attempts,
            max_workflow_pause_duration: cfg.max_workflow_pause_duration,
            labels: cfg.labels,
            #[cfg(feature = "db")]
            sharded_pool: cfg.sharded_pool,
            max_workflow_history_events: cfg.max_workflow_history_events,
            slot_tuner: cfg.slot_tuner,
            max_concurrent_sessions: cfg.max_concurrent_sessions,
        }
    }
}

// ---------------------------------------------------------------------------
// HandlerRegistry
// ---------------------------------------------------------------------------

/// Stub dispatch fn for the internal worker-session acquire/release
/// activities (issue #606). `process_activity_task` always intercepts these
/// reserved names before consulting `registry.activities`, so this body
/// should be unreachable in practice; it fails loudly (rather than silently
/// no-opping) if that invariant is ever violated by a future change.
fn session_internal_stub_handler(
    _ctx: &ActivityContext,
    _input: serde_json::Value,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + '_>,
> {
    Box::pin(async move {
        Err(
            "internal session activity dispatched through the normal handler path; \
             this indicates an engine bug (process_activity_task should have \
             intercepted this reserved name first)"
                .to_string(),
        )
    })
}

/// Build the `ActivityInfo` registered for a reserved worker-session
/// activity name (issue #606) -- see [`session_internal_stub_handler`].
fn session_internal_activity_info(name: &'static str) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "autumn_harvest::sessions",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        default_schedule_to_close: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: session_internal_stub_handler,
    }
}

/// Fast name-to-handler lookup for workflows and activities.
///
/// Built once at startup from the vectors produced by the `workflows![]` and
/// `activities![]` macros, then shared via `Arc` across all poll iterations.
pub struct HandlerRegistry {
    /// Workflow handlers indexed by name.
    pub workflows: HashMap<String, WorkflowInfo>,
    /// Activity handlers indexed by name.
    pub activities: HashMap<String, ActivityInfo>,
    /// Declarative query handlers (issue #346), indexed by `(workflow, name)`.
    pub query_handlers: Vec<QueryHandlerInfo>,
    /// Declarative update handlers (issue #346), indexed by `(workflow, name)`.
    pub update_handlers: Vec<UpdateHandlerInfo>,
    /// Declarative signal handler metadata (issue #610), indexed by
    /// `(workflow, name)`. Published for interface discovery; runtime push
    /// handlers register inside the workflow body.
    pub signal_handlers: Vec<crate::info::SignalHandlerInfo>,
    /// Shared typed state visible to workflow and activity handlers.
    state: SharedState,
    /// Telemetry bundle (trace-context propagator + metrics recorder) applied
    /// around every dispatch.
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    /// History-size thresholds visible to workflow contexts.
    history_policy: WorkflowHistoryPolicy,
    /// Maximum allowed bytes for a single activity input payload (enforced at schedule time).
    pub max_activity_input_bytes: u64,
    /// Maximum allowed bytes for a child workflow input payload (enforced at schedule time).
    pub max_workflow_input_bytes: u64,
    /// Maximum allowed bytes for a single activity result payload (enforced at completion time).
    pub max_activity_result_bytes: u64,
    /// Maximum allowed bytes for a signal payload (enforced at signal-send time).
    pub max_signal_payload_bytes: u64,
    /// Per-activity circuit breakers (issue #369), shared with the management
    /// API so both the worker dispatch path and operators observe the same
    /// in-process state. Built from the registered activities' declared
    /// [`CircuitBreakerPolicy`](crate::policy::CircuitBreakerPolicy)s.
    circuit_breakers: Arc<crate::circuit_breaker::CircuitBreakerRegistry>,
    /// Maximum byte length for `current_details` strings passed to the
    /// workflow context (issue #473). Default: 1 KiB.
    pub max_current_details_bytes: usize,
    /// Server-side ceiling on `workflow_attempt` (issue #523). `None` = no
    /// ceiling. Applied to scheduler-fired starts so automated fires respect
    /// the same operator-configured cap as API/manual starts.
    pub max_workflow_attempts_ceiling: Option<u32>,
    /// Large-payload offloader (issue #524). `None` = no `PayloadStore`
    /// registered; all event writes/reads use the plain inline path unchanged.
    payload_offloader: Option<Arc<crate::payload_store::PayloadOffloader>>,
    /// Ordered activity execution interceptor chain (issue #680). Index 0 is
    /// the OUTERMOST wrapper. Empty (the default) = no interceptors, and the
    /// dispatch path takes a zero-overhead direct handler call.
    activity_interceptors: Vec<Arc<dyn crate::interceptor::ActivityInterceptor>>,
    /// Sandbox policy per WASM-backed activity (issue #965), keyed by activity
    /// name. The worker dispatch seam consults this before running a native
    /// handler: when a binding is present the activity's active module is
    /// resolved and its guest run instead. Empty = no WASM activities.
    #[cfg(feature = "wasm-activities")]
    wasm_activities: HashMap<String, crate::wasm_store::WasmBinding>,
    /// Shared engine + compiled-module cache for WASM activities (issue #965).
    /// One per worker, created lazily by the builder. `None` = no WASM
    /// activities registered.
    #[cfg(feature = "wasm-activities")]
    wasm_store: Option<Arc<crate::wasm_activities::WasmModuleStore>>,
    /// `(activity_name, module_bytes)` pairs published to the worker's shard
    /// database at startup (issue #965), so an embedder that only calls
    /// `HarvestBuilder::wasm_activity(...)` gets a working WASM activity with no
    /// manual publish step.
    #[cfg(feature = "wasm-activities")]
    wasm_module_registrations: Vec<(String, Vec<u8>)>,
    /// Builder-level default activity retry policy (issue #620). `None` = no
    /// floor configured; the schedule-time resolution is a pure no-op preserving
    /// today's behaviour byte-for-byte.
    default_activity_retry_policy: Option<crate::policy::RetryPolicy>,
    /// Builder-level default activity `start_to_close` (issue #620). `None` = no
    /// floor configured.
    default_activity_start_to_close: Option<Duration>,
}

impl HandlerRegistry {
    /// Create a new registry, indexing handlers by their `name` field.
    #[must_use]
    pub fn new(workflows: Vec<WorkflowInfo>, activities: Vec<ActivityInfo>) -> Self {
        Self::with_state(workflows, activities, empty_shared_state())
    }

    /// Build a map of `activity_name` → `required_capabilities` JSON for every
    /// registered activity that declares `requires`.
    ///
    /// Mirrors the per-row `required_capabilities` snapshot. Used by the
    /// stranded-work sampler and the shard-health coverage gate
    /// ([`crate::queue::apply_activity_requirements`]) to resolve eligibility for
    /// activity rows whose queue row left `required_capabilities` NULL even
    /// though the activity has requirements — the same fallback `claim_task`
    /// applies via its ineligible-activities gate. Unparseable requirement
    /// strings are skipped.
    #[must_use]
    pub fn activity_requirements_json(&self) -> HashMap<String, serde_json::Value> {
        let mut map = HashMap::new();
        for activity in self.activities.values() {
            let Some(requires) = activity.requires else {
                continue;
            };
            let Ok(reqs) = crate::eligibility::parse_requirements(requires) else {
                continue;
            };
            if let Ok(value) = serde_json::to_value(&reqs) {
                map.insert(activity.name.to_string(), value);
            }
        }
        map
    }

    /// Create a new registry with shared typed state.
    #[must_use]
    pub fn with_state(
        workflows: Vec<WorkflowInfo>,
        activities: Vec<ActivityInfo>,
        state: SharedState,
    ) -> Self {
        Self::with_state_and_telemetry(
            workflows,
            activities,
            state,
            Arc::new(crate::telemetry::TelemetryConfig::default()),
        )
    }

    /// Create a new registry with shared typed state and a telemetry bundle.
    ///
    /// Used by [`crate::builder::BuiltHarvest::into_worker_parts`] so worker
    /// instrumentation inherits whatever the application configured. Callers
    /// that do not care about telemetry should prefer [`Self::with_state`],
    /// which installs safe no-op defaults.
    #[must_use]
    pub fn with_state_and_telemetry(
        workflows: Vec<WorkflowInfo>,
        activities: Vec<ActivityInfo>,
        state: SharedState,
        telemetry: Arc<crate::telemetry::TelemetryConfig>,
    ) -> Self {
        let workflows: HashMap<String, WorkflowInfo> = workflows
            .into_iter()
            .map(|w| (w.name.to_string(), w))
            .collect();
        if let Ok(mut lock) = crate::completion_trigger::GLOBAL_WORKFLOW_METADATA.write() {
            let metadata = workflows
                .iter()
                .map(|(name, w)| {
                    (
                        name.clone(),
                        crate::completion_trigger::WorkflowMetadata {
                            concurrency: w.concurrency,
                            max_input_bytes: w.max_input_bytes,
                            owner: w.owner.map(String::from),
                            runbook_url: w.runbook_url.map(String::from),
                            severity: w.severity.map(String::from),
                            input_schema: w.input_schema,
                            sla: w.sla,
                            retry_policy: w.retry_policy.clone(),
                        },
                    )
                })
                .collect();
            *lock = Some(metadata);
        }
        let mut activities: HashMap<String, ActivityInfo> = activities
            .into_iter()
            .map(|a| (a.name.to_string(), a))
            .collect();
        // Worker sessions (issue #606): the internal acquire/release
        // activities have no author-registered handler -- `process_activity_task`
        // intercepts them by reserved name before this map is ever consulted
        // for dispatch. They still need an `ActivityInfo` entry so
        // `persist_scheduled_activities`'s `registry.activities.get(name)`
        // lookup succeeds at enqueue time; the stub handler below is never
        // actually invoked. Inserted unconditionally (after user activities
        // are collected) so a reserved name always resolves to the engine's
        // own entry.
        for name in [
            crate::context::SESSION_ACQUIRE_ACTIVITY_NAME,
            crate::context::SESSION_RELEASE_ACTIVITY_NAME,
        ] {
            activities.insert(name.to_string(), session_internal_activity_info(name));
        }
        // Circuit breakers are enforced on the task-dispatch path
        // (`process_activity_task`), which local activities bypass by running
        // inline. The `#[activity]` macro rejects `circuit_breaker` on local
        // activities at compile time; this filter is the defensive equivalent
        // for hand-built `ActivityInfo`s so a local activity never registers a
        // breaker that can appear configured in the admin API yet never trips.
        let circuit_policies: HashMap<String, crate::policy::CircuitBreakerPolicy> = activities
            .iter()
            .filter(|(_, info)| !info.is_local)
            .filter_map(|(name, info)| info.circuit_breaker.map(|p| (name.clone(), p)))
            .collect();
        Self {
            workflows,
            activities,
            query_handlers: Vec::new(),
            update_handlers: Vec::new(),
            signal_handlers: Vec::new(),
            state,
            telemetry,
            history_policy: WorkflowHistoryPolicy::default(),
            max_activity_input_bytes: crate::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            max_workflow_input_bytes: crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            max_activity_result_bytes: crate::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
            max_signal_payload_bytes: crate::builder::DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            max_current_details_bytes: crate::context::DEFAULT_CURRENT_DETAILS_CAP_BYTES,
            circuit_breakers: Arc::new(crate::circuit_breaker::CircuitBreakerRegistry::new(
                circuit_policies,
            )),
            max_workflow_attempts_ceiling: None,
            payload_offloader: None,
            activity_interceptors: Vec::new(),
            #[cfg(feature = "wasm-activities")]
            wasm_activities: HashMap::new(),
            #[cfg(feature = "wasm-activities")]
            wasm_store: None,
            #[cfg(feature = "wasm-activities")]
            wasm_module_registrations: Vec::new(),
            default_activity_retry_policy: None,
            default_activity_start_to_close: None,
        }
    }

    /// Set declarative query, update, and signal handler metadata
    /// (issues #346, #610).
    #[must_use]
    pub fn with_handler_infos(
        mut self,
        query_handlers: Vec<QueryHandlerInfo>,
        update_handlers: Vec<UpdateHandlerInfo>,
        signal_handlers: Vec<crate::info::SignalHandlerInfo>,
    ) -> Self {
        self.query_handlers = query_handlers;
        self.update_handlers = update_handlers;
        self.signal_handlers = signal_handlers;
        self
    }

    /// Create a new registry with shared state, telemetry, and history guardrails.
    #[must_use]
    pub fn with_state_telemetry_and_history_policy(
        workflows: Vec<WorkflowInfo>,
        activities: Vec<ActivityInfo>,
        state: SharedState,
        telemetry: Arc<crate::telemetry::TelemetryConfig>,
        history_policy: WorkflowHistoryPolicy,
    ) -> Self {
        Self::with_state_and_telemetry(workflows, activities, state, telemetry)
            .with_history_policy(history_policy)
    }

    /// Override the history guardrails carried by this registry.
    #[must_use]
    pub const fn with_history_policy(mut self, history_policy: WorkflowHistoryPolicy) -> Self {
        self.history_policy = history_policy;
        self
    }

    /// Set the payload size caps propagated from [`crate::builder::BuiltHarvest`].
    #[must_use]
    pub fn with_payload_caps(
        mut self,
        max_activity_input_bytes: u64,
        max_workflow_input_bytes: u64,
        max_activity_result_bytes: u64,
        max_signal_payload_bytes: u64,
    ) -> Self {
        self.max_activity_input_bytes = max_activity_input_bytes;
        self.max_workflow_input_bytes = max_workflow_input_bytes;
        self.max_activity_result_bytes = max_activity_result_bytes;
        self.max_signal_payload_bytes = max_signal_payload_bytes;
        if let Ok(mut lock) = crate::completion_trigger::GLOBAL_MAX_WORKFLOW_INPUT_BYTES.write() {
            *lock = max_workflow_input_bytes;
        }
        self
    }

    /// Set the max byte length for `current_details` strings (issue #473).
    #[must_use]
    pub const fn with_current_details_cap(mut self, cap_bytes: usize) -> Self {
        self.max_current_details_bytes = cap_bytes;
        self
    }

    /// Set the server-side ceiling on `workflow_attempt` (issue #523).
    #[must_use]
    pub fn with_max_workflow_attempts_ceiling(mut self, ceiling: Option<u32>) -> Self {
        self.max_workflow_attempts_ceiling = ceiling;
        #[cfg(feature = "db")]
        if let Ok(mut lock) =
            crate::completion_trigger::GLOBAL_MAX_WORKFLOW_ATTEMPTS_CEILING.write()
        {
            *lock = ceiling;
        }
        self
    }

    /// Attach the large-payload offloader (issue #524).
    #[must_use]
    pub fn with_payload_offloader(
        mut self,
        offloader: Option<Arc<crate::payload_store::PayloadOffloader>>,
    ) -> Self {
        self.payload_offloader = offloader;
        self
    }

    /// Borrow the configured large-payload offloader, if any (issue #524).
    #[must_use]
    pub fn payload_offloader(&self) -> Option<&crate::payload_store::PayloadOffloader> {
        self.payload_offloader.as_deref()
    }

    /// Clone the configured large-payload offloader handle for use in a
    /// `'static` background task (e.g. the retention sweep). Issue #524.
    #[must_use]
    pub fn payload_offloader_arc(&self) -> Option<Arc<crate::payload_store::PayloadOffloader>> {
        self.payload_offloader.clone()
    }

    /// Install the ordered activity execution interceptor chain (issue #680).
    ///
    /// Index 0 is the OUTERMOST wrapper; the activity handler is innermost.
    /// Applies to every activity execution on the worker — regular and local.
    #[must_use]
    pub fn with_activity_interceptors(
        mut self,
        interceptors: Vec<Arc<dyn crate::interceptor::ActivityInterceptor>>,
    ) -> Self {
        self.activity_interceptors = interceptors;
        self
    }

    /// Borrow the configured activity interceptor chain (issue #680). Empty when
    /// none are registered.
    #[must_use]
    pub fn activity_interceptors(&self) -> &[Arc<dyn crate::interceptor::ActivityInterceptor>] {
        &self.activity_interceptors
    }

    /// Install the WASM activity sandbox policies, shared module store, and the
    /// startup-publish module registrations (issue #965).
    #[cfg(feature = "wasm-activities")]
    #[must_use]
    pub fn with_wasm_activities(
        mut self,
        store: Arc<crate::wasm_activities::WasmModuleStore>,
        bindings: HashMap<String, crate::wasm_store::WasmBinding>,
        registrations: Vec<(String, Vec<u8>)>,
    ) -> Self {
        self.wasm_store = Some(store);
        self.wasm_activities = bindings;
        self.wasm_module_registrations = registrations;
        self
    }

    /// Borrow the WASM sandbox policy for `activity_name`, if it is WASM-backed
    /// (issue #965).
    #[cfg(feature = "wasm-activities")]
    #[must_use]
    pub fn wasm_binding(&self, activity_name: &str) -> Option<&crate::wasm_store::WasmBinding> {
        self.wasm_activities.get(activity_name)
    }

    /// Borrow the shared WASM module store, if any WASM activity is registered
    /// (issue #965).
    #[cfg(feature = "wasm-activities")]
    #[must_use]
    pub const fn wasm_store(&self) -> Option<&Arc<crate::wasm_activities::WasmModuleStore>> {
        self.wasm_store.as_ref()
    }

    /// The `(activity_name, module_bytes)` registrations to publish at worker
    /// startup (issue #965).
    #[cfg(feature = "wasm-activities")]
    #[must_use]
    pub fn wasm_module_registrations(&self) -> &[(String, Vec<u8>)] {
        &self.wasm_module_registrations
    }

    /// Install the builder-level default activity retry/timeout floor (issue #620).
    ///
    /// Both are `None` by default — an unset floor is a pure no-op preserving
    /// today's behaviour byte-for-byte. Resolved at schedule time as the
    /// lowest-priority fallback: a call-site override or an activity's own
    /// `#[activity(retry = …/start_to_close = …)]` default both win.
    #[must_use]
    pub fn with_activity_defaults(
        mut self,
        retry: Option<crate::policy::RetryPolicy>,
        start_to_close: Option<Duration>,
    ) -> Self {
        self.default_activity_retry_policy = retry;
        self.default_activity_start_to_close = start_to_close;
        self
    }

    /// Borrow the builder-level default activity retry policy (issue #620).
    #[must_use]
    pub fn default_activity_retry_policy(&self) -> Option<crate::policy::RetryPolicy> {
        self.default_activity_retry_policy.clone()
    }

    /// The builder-level default activity `start_to_close` (issue #620).
    #[must_use]
    pub const fn default_activity_start_to_close(&self) -> Option<Duration> {
        self.default_activity_start_to_close
    }

    /// Clone the shared state reference for runtime contexts.
    #[must_use]
    pub fn shared_state(&self) -> SharedState {
        Arc::clone(&self.state)
    }

    /// Access typed shared state for tests and diagnostics.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Access the telemetry bundle shared across worker dispatches.
    #[must_use]
    pub const fn telemetry(&self) -> &Arc<crate::telemetry::TelemetryConfig> {
        &self.telemetry
    }

    /// Access the per-activity circuit-breaker registry (issue #369).
    ///
    /// Shared (behind an `Arc`) with the management API so operators observe
    /// and force the same in-process breaker state the worker enforces.
    #[must_use]
    pub fn circuit_breakers(&self) -> Arc<crate::circuit_breaker::CircuitBreakerRegistry> {
        Arc::clone(&self.circuit_breakers)
    }

    /// History-size guardrails applied to workflow contexts run by this registry.
    #[must_use]
    pub const fn history_policy(&self) -> WorkflowHistoryPolicy {
        self.history_policy
    }

    /// Effective result-payload byte cap for the named activity (issue #252).
    ///
    /// Returns the per-activity `max_result_bytes` override raised against the
    /// global `max_activity_result_bytes` ceiling (`override.max(global)`),
    /// mirroring the worker's `effective_result_cap` resolution. Unknown
    /// activities fall back to the global cap. Read-only.
    ///
    /// Used by the management stack endpoint (issue #503) so a heartbeat
    /// checkpoint is judged against the same effective cap the activity's
    /// result would be, rather than the global default only.
    #[must_use]
    pub fn activity_result_cap(&self, name: &str) -> u64 {
        self.activities
            .get(name)
            .and_then(|a| a.max_result_bytes)
            .map_or(self.max_activity_result_bytes, |per| {
                per.max(self.max_activity_result_bytes)
            })
    }

    /// Effective input-payload byte cap for the named activity (issue #252).
    ///
    /// Returns the per-activity `max_input_bytes` override raised against the
    /// global `max_activity_input_bytes` ceiling (`override.max(global)`),
    /// mirroring [`Self::activity_result_cap`]. Unknown activities fall back
    /// to the global cap. Read-only.
    ///
    /// Used by the management stack endpoint (issue #608) so a decoded
    /// pending-activity input is judged against the same effective cap the
    /// activity's input was admitted under, rather than the global default
    /// only.
    #[must_use]
    pub fn activity_input_cap(&self, name: &str) -> u64 {
        self.activities
            .get(name)
            .and_then(|a| a.max_input_bytes)
            .map_or(self.max_activity_input_bytes, |per| {
                per.max(self.max_activity_input_bytes)
            })
    }
}

impl std::fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("HandlerRegistry");
        d.field("workflows", &self.workflows.keys())
            .field("activities", &self.activities.keys())
            .field("query_handler_count", &self.query_handlers.len())
            .field("update_handler_count", &self.update_handlers.len())
            .field("signal_handler_count", &self.signal_handlers.len())
            .field("state_count", &self.state.len())
            .field("telemetry", &self.telemetry)
            .field("history_policy", &self.history_policy)
            .field("max_activity_input_bytes", &self.max_activity_input_bytes)
            .field("max_workflow_input_bytes", &self.max_workflow_input_bytes)
            .field("max_activity_result_bytes", &self.max_activity_result_bytes)
            .field("max_signal_payload_bytes", &self.max_signal_payload_bytes)
            .field("max_current_details_bytes", &self.max_current_details_bytes)
            .field("circuit_breakers", &self.circuit_breakers)
            .field(
                "max_workflow_attempts_ceiling",
                &self.max_workflow_attempts_ceiling,
            )
            .field("payload_offloader", &self.payload_offloader.is_some())
            .field(
                "activity_interceptor_count",
                &self.activity_interceptors.len(),
            )
            .field(
                "default_activity_retry_policy",
                &self.default_activity_retry_policy,
            )
            .field(
                "default_activity_start_to_close",
                &self.default_activity_start_to_close,
            );
        #[cfg(feature = "wasm-activities")]
        d.field("wasm_activity_count", &self.wasm_activities.len())
            .field("wasm_store_configured", &self.wasm_store.is_some())
            .field(
                "wasm_module_registration_count",
                &self.wasm_module_registrations.len(),
            );
        d.finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimedTaskKind {
    Workflow,
    Activity,
}

impl ClaimedTaskKind {
    fn from_db(task_type: &str) -> HarvestResult<Self> {
        match task_type {
            task_type if task_type == TaskType::Workflow.as_str() => Ok(Self::Workflow),
            task_type if task_type == TaskType::Activity.as_str() => Ok(Self::Activity),
            other => Err(HarvestError::Config(format!(
                "unsupported task type in queue row: {other}"
            ))),
        }
    }
}

fn execution_id_from_uuid(id: uuid::Uuid) -> ExecutionId {
    id.to_string()
        .parse()
        .expect("database UUIDs must round-trip into ExecutionId")
}

const fn workflow_command_name(command: &WorkflowCommand) -> &'static str {
    match command {
        WorkflowCommand::ScheduleActivity { .. } => "ScheduleActivity",
        WorkflowCommand::WaitForActivity { .. } => "WaitForActivity",
        WorkflowCommand::ScheduleExternalActivity { .. } => "ScheduleExternalActivity",
        WorkflowCommand::StartTimer { .. } => "StartTimer",
        WorkflowCommand::StartChildWorkflow { .. } => "StartChildWorkflow",
        WorkflowCommand::RecordMarker { .. } => "RecordMarker",
        WorkflowCommand::RecordSideEffect { .. } => "RecordSideEffect",
        WorkflowCommand::WaitForSignal { .. } => "WaitForSignal",
        WorkflowCommand::Complete { .. } => "Complete",
        WorkflowCommand::Fail { .. } => "Fail",
        WorkflowCommand::ContinueAsNew { .. } => "ContinueAsNew",
        WorkflowCommand::RunLocalActivity { .. } => "RunLocalActivity",
        WorkflowCommand::RecordUpdateResult { .. } => "RecordUpdateResult",
        WorkflowCommand::UpsertSearchAttributes { .. } => "UpsertSearchAttributes",
        WorkflowCommand::SetCurrentDetails { .. } => "SetCurrentDetails",
        WorkflowCommand::PublishProgress { .. } => "PublishProgress",
        WorkflowCommand::SignalExternalWorkflow { .. } => "SignalExternalWorkflow",
        WorkflowCommand::RequestCancelExternalWorkflow { .. } => "RequestCancelExternalWorkflow",
        WorkflowCommand::SpawnDetachedChildWorkflow { .. } => "SpawnDetachedChildWorkflow",
        WorkflowCommand::CancelRaceLosers { .. } => "CancelRaceLosers",
        WorkflowCommand::ArmTimer { .. } => "ArmTimer",
        WorkflowCommand::CancelTimer { .. } => "CancelTimer",
    }
}

fn suspended_workflow_error(commands: &[WorkflowCommand]) -> String {
    if commands.is_empty() {
        return "workflow suspended without emitted commands; resumption is not implemented yet"
            .to_string();
    }

    let command_names = commands
        .iter()
        .map(workflow_command_name)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "workflow task suspended with unsupported commands ({command_names}); this command set is not implemented yet"
    )
}

#[cfg(test)]
fn all_commands_wait_for_signal(commands: &[WorkflowCommand]) -> bool {
    !commands.is_empty()
        && commands
            .iter()
            .all(|cmd| matches!(cmd, WorkflowCommand::WaitForSignal { .. }))
}

fn should_requeue_signal_wait(commands: &[WorkflowCommand]) -> bool {
    if commands.is_empty() {
        return false;
    }

    let has_wait = commands.iter().any(|cmd| {
        matches!(
            cmd,
            WorkflowCommand::WaitForSignal { .. }
                | WorkflowCommand::SignalExternalWorkflow { .. }
                | WorkflowCommand::RequestCancelExternalWorkflow { .. }
        )
    });

    let only_wait_or_bookkeeping = commands.iter().all(|cmd| {
        matches!(
            cmd,
            WorkflowCommand::WaitForSignal { .. }
                | WorkflowCommand::SignalExternalWorkflow { .. }
                | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                | WorkflowCommand::RecordMarker { .. }
                | WorkflowCommand::RecordSideEffect { .. }
                | WorkflowCommand::RecordUpdateResult { .. }
                | WorkflowCommand::UpsertSearchAttributes { .. }
                | WorkflowCommand::SetCurrentDetails { .. }
                | WorkflowCommand::PublishProgress { .. }
                | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
                | WorkflowCommand::CancelRaceLosers { .. }
                | WorkflowCommand::ArmTimer { .. }
                | WorkflowCommand::CancelTimer { .. }
        )
    });

    has_wait && only_wait_or_bookkeeping
}

fn only_bookkeeping_commands(commands: &[WorkflowCommand]) -> bool {
    !commands.is_empty()
        && commands.iter().all(|cmd| {
            matches!(
                cmd,
                WorkflowCommand::RecordMarker { .. }
                    | WorkflowCommand::RecordSideEffect { .. }
                    | WorkflowCommand::RecordUpdateResult { .. }
                    | WorkflowCommand::UpsertSearchAttributes { .. }
                    | WorkflowCommand::SetCurrentDetails { .. }
                    | WorkflowCommand::PublishProgress { .. }
                    | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
                    | WorkflowCommand::CancelRaceLosers { .. }
                    | WorkflowCommand::ArmTimer { .. }
                    | WorkflowCommand::CancelTimer { .. }
            )
        })
}

#[derive(Debug, Clone)]
struct ScheduledActivityCommand {
    activity_id: ActivityExecId,
    name: String,
    input: serde_json::Value,
    queue: String,
    retry_policy_override: Option<crate::policy::RetryPolicy>,
    start_to_close_override: Option<std::time::Duration>,
    /// Worker session this activity belongs to (issue #606). `None` for an
    /// ordinary activity dispatch.
    ///
    /// TODO(#606 step 9): consumed by `persist_scheduled_activities` to
    /// write the `harvest_task_queue.session_id` column and hard-pin
    /// `sticky_worker_id`/`sticky_until` from `session_worker_id`. Not yet
    /// wired -- `#[allow(dead_code)]` is temporary until that step lands.
    #[allow(dead_code)]
    session_id: Option<crate::types::SessionId>,
    /// The session's host worker id (issue #606); when `Some`, the enqueued
    /// task row is hard-pinned to this worker. `None` for a non-session
    /// activity. See the `session_id` TODO above.
    #[allow(dead_code)]
    session_worker_id: Option<String>,
    /// Per-call `schedule_to_start` override (issue #606), used only by the
    /// internal session-acquire dispatch. `None` for every ordinary
    /// activity. See the `session_id` TODO above.
    #[allow(dead_code)]
    schedule_to_start_override: Option<std::time::Duration>,
}

#[derive(Debug, Clone)]
struct StartedTimerCommand {
    timer_id: TimerId,
    duration_secs: u64,
}

#[derive(Debug, Clone)]
struct StartedChildWorkflowCommand {
    child_id: ExecutionId,
    workflow_name: String,
    input: serde_json::Value,
}

#[derive(Debug, Clone)]
struct ScheduledExternalActivityCommand {
    activity_id: ActivityExecId,
    token: ExternalActivityToken,
    name: String,
    input: serde_json::Value,
    queue: String,
    schedule_to_close_secs: u64,
}

#[derive(Debug)]
struct PreparedWorkflowTask {
    execution: WorkflowExecution,
    exec_id: ExecutionId,
    history_events: Vec<WorkflowEvent>,
    next_event_id: i32,
    timers_fired: Vec<TimerId>,
    signals_delivered: Vec<String>,
    /// `true` if the event history was served from the in-process LRU cache
    /// (only delta events were loaded from Postgres); `false` if the full
    /// history was loaded cold.
    was_cache_hit: bool,
}

#[derive(Debug, Clone)]
struct WorkflowTaskPersistence<'a> {
    task: &'a TaskQueueItem,
    worker_id: &'a str,
    exec_id: ExecutionId,
    next_event_id: i32,
    /// Grace window for pinning follow-up tasks to this worker's LRU cache.
    /// Zero disables sticky routing entirely.
    sticky_timeout: Duration,
    /// Decoded scheduled-carryover values frozen in this execution's `WorkflowStarted`
    /// event (issue #488). Propagated verbatim to a `continue_as_new` continuation so
    /// `ctx.last_completion_result()` / `ctx.last_error()` survive the fork (the
    /// continuation is the same logical scheduled run). `None`/`None` for non-scheduled
    /// runs. Plaintext here because it comes from the already-decoded replay history.
    carryover_result: Option<serde_json::Value>,
    carryover_error: Option<String>,
    /// Nominal scheduled fire-time frozen in this execution's `WorkflowStarted` event
    /// (issue #508). Propagated verbatim to a `continue_as_new` continuation so
    /// `ctx.scheduled_time()` survives the fork. `None` for non-scheduled runs.
    carryover_scheduled_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl<'a> WorkflowTaskPersistence<'a> {
    /// Build a sticky hint bound to this worker, or `None` when sticky routing
    /// is disabled (timeout == 0).
    const fn sticky_hint(&self) -> Option<queue::StickyHint<'a>> {
        if self.sticky_timeout.is_zero() {
            None
        } else {
            Some(queue::StickyHint::new(self.worker_id, self.sticky_timeout))
        }
    }
}

#[derive(Debug, Clone)]
struct SuspendedWorkflowContext<'a> {
    execution: &'a WorkflowExecution,
    persistence: WorkflowTaskPersistence<'a>,
    /// Handle for the `harvest.workflow.execute` span that is still open.
    /// Producer spans (`activity.schedule`, `child_workflow.start`) use this as
    /// their explicit parent so they are nested inside the executor cycle.
    execute_span: &'a tracing::Span,
    /// External-op ids resolved inline this cycle (issue #678/#1034). Non-empty
    /// for any mixed-suspension shape whose same-shard external op
    /// (`signal_external_workflow` / `request_cancel_external_workflow`) resolved
    /// INLINE this decision cycle; empty for a pure suspension. The arm-level
    /// self-wake in [`persist_workflow_outcome`]'s `Suspended` arm consumes it to
    /// re-pend the parked task immediately (#1034). For the timer shape it is
    /// still threaded into [`persist_started_timer`], but only to set the
    /// `mixed_signal_suspension` stamp the arm-level wake re-pends — no longer to
    /// self-wake there.
    resolved_inline_external: ResolvedExternalIds,
}

#[derive(Clone, Copy)]
struct DetachedSpawnPersistence<'a> {
    registry: &'a HandlerRegistry,
    parent_execution: &'a WorkflowExecution,
    execute_span: &'a tracing::Span,
}

impl DetachedSpawnPersistence<'_> {
    async fn persist(
        self,
        conn: &mut AsyncPgConnection,
        commands: &[WorkflowCommand],
    ) -> HarvestResult<()> {
        create_detached_child_executions(
            conn,
            self.registry,
            self.parent_execution,
            commands,
            self.execute_span,
        )
        .await
    }
}

/// Builds the complete ordered event list for a suspension batch by iterating
/// `commands` in emission order.
///
/// For each command:
/// - `RecordMarker` → `MarkerRecorded`
/// - `SpawnDetachedChildWorkflow` → `ChildWorkflowSpawnedDetached`
/// - Any other command → whatever `branch_event(cmd)` returns (`None` = skip)
///
/// Preserving exact command emission order is required by the replay engine's
/// sequential cursor: `match_detached_child_spawn` and all branch-event matchers
/// use a strict position-based cursor, so `ChildWorkflowSpawnedDetached` events
/// must appear at the same relative position as their `SpawnDetachedChildWorkflow`
/// commands rather than always being pre-pended before branch events.
///
/// Suspension paths without branch events (signal-wait, activity-wait) call this
/// via `pre_suspension_events_from_commands`; paths with branch events (schedule-
/// activity, start-timer, start-child-workflow, schedule-external) pass a closure
/// that emits the appropriate event for each matching command type.
fn build_suspension_events<F>(
    commands: &[WorkflowCommand],
    timer_events: &mut [Option<WorkflowEvent>],
    mut branch_event: F,
) -> Vec<WorkflowEvent>
where
    F: FnMut(&WorkflowCommand) -> Option<WorkflowEvent>,
{
    commands
        .iter()
        .enumerate()
        .filter_map(|(i, cmd)| match cmd {
            WorkflowCommand::RecordMarker { name, details } => {
                Some(WorkflowEvent::MarkerRecorded {
                    name: name.clone(),
                    details: details.clone(),
                })
            }
            WorkflowCommand::RecordSideEffect { kind, name, value } => {
                Some(WorkflowEvent::SideEffectRecorded {
                    kind: *kind,
                    name: name.clone(),
                    value: value.clone(),
                })
            }
            WorkflowCommand::SpawnDetachedChildWorkflow {
                child_id,
                workflow_name,
                input,
                parent_close_policy,
            } => Some(WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id: *child_id,
                workflow_name: workflow_name.clone(),
                input: input.clone(),
                parent_close_policy: *parent_close_policy,
            }),
            // Cancellable/renewable timer bookkeeping (issue #768): the
            // `TimerStarted` / `TimerCancelled` event resolved by the DB-mutation
            // phase (`plan_timer_lifecycle`) is interleaved here at the
            // `ArmTimer` / `CancelTimer` command's emission position — exactly
            // like `ChildWorkflowSpawnedDetached` above — so `replay`'s strictly
            // positional `match_timer_arm` sees the same order the live cycle
            // emitted. (Pre-FINDING-1 these were appended at the END of the
            // batch, nd-blocking a `start_timer` + `side_effect` same-cycle run.)
            WorkflowCommand::ArmTimer { .. } | WorkflowCommand::CancelTimer { .. } => {
                timer_events.get_mut(i).and_then(Option::take)
            }
            other => branch_event(other),
        })
        .collect()
}

/// Collects `MarkerRecorded`, `SideEffectRecorded`, `ChildWorkflowSpawnedDetached`,
/// and (issue #768) interleaved timer-lifecycle events in command emission order
/// for suspension paths that have no branch-specific events (signal-wait park,
/// activity-wait park). `timer_events` is the DB-mutation plan from
/// [`plan_timer_lifecycle`].
fn pre_suspension_events_from_commands(
    commands: &[WorkflowCommand],
    timer_events: &mut [Option<WorkflowEvent>],
) -> Vec<WorkflowEvent> {
    build_suspension_events(commands, timer_events, |_| None)
}

fn extract_single_command<T>(
    commands: &[WorkflowCommand],
    extractor: impl Fn(&WorkflowCommand) -> Option<T>,
) -> Option<T> {
    // RecordUpdateResult, RecordMarker, UpsertSearchAttributes, SetCurrentDetails, and
    // SpawnDetachedChildWorkflow are bookkeeping / fire-and-forget commands
    // that have already been (or are about to be) processed; they do not count
    // toward the suspension-type determination.
    let mut iter = commands.iter().filter(|cmd| {
        !matches!(
            cmd,
            WorkflowCommand::RecordMarker { .. }
                | WorkflowCommand::RecordSideEffect { .. }
                | WorkflowCommand::RecordUpdateResult { .. }
                | WorkflowCommand::UpsertSearchAttributes { .. }
                | WorkflowCommand::SetCurrentDetails { .. }
                | WorkflowCommand::PublishProgress { .. }
                | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
                | WorkflowCommand::CancelRaceLosers { .. }
                | WorkflowCommand::ArmTimer { .. }
                | WorkflowCommand::CancelTimer { .. }
        )
    });

    let first_cmd = iter.next()?;

    // Original behavior: return None if there's more than one non-marker command.
    if iter.next().is_some() {
        return None;
    }

    // Original behavior: extractor(cmd)? means we return None if the extractor yields None.
    extractor(first_cmd)
}

fn extract_all_scheduled_activities(
    commands: &[WorkflowCommand],
) -> Option<Vec<ScheduledActivityCommand>> {
    let mut scheduled = Vec::new();

    for cmd in commands {
        match cmd {
            WorkflowCommand::RecordMarker { .. }
            | WorkflowCommand::RecordSideEffect { .. }
            | WorkflowCommand::RecordUpdateResult { .. }
            | WorkflowCommand::UpsertSearchAttributes { .. }
            | WorkflowCommand::SetCurrentDetails { .. }
            | WorkflowCommand::PublishProgress { .. }
            | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
            | WorkflowCommand::CancelRaceLosers { .. }
            | WorkflowCommand::ArmTimer { .. }
            | WorkflowCommand::CancelTimer { .. } => {}
            WorkflowCommand::ScheduleActivity {
                activity_id,
                name,
                input,
                queue,
                retry_policy_override,
                start_to_close_override,
                session_id,
                session_worker_id,
                schedule_to_start_override,
                ..
            } => {
                scheduled.push(ScheduledActivityCommand {
                    activity_id: *activity_id,
                    name: name.clone(),
                    input: input.clone(),
                    queue: queue.clone(),
                    retry_policy_override: retry_policy_override.clone(),
                    start_to_close_override: *start_to_close_override,
                    session_id: *session_id,
                    session_worker_id: session_worker_id.clone(),
                    schedule_to_start_override: *schedule_to_start_override,
                });
            }
            _ => return None,
        }
    }

    if scheduled.is_empty() {
        None
    } else {
        Some(scheduled)
    }
}

fn extract_all_activity_waits(commands: &[WorkflowCommand]) -> Option<Vec<ActivityExecId>> {
    let mut activity_ids = Vec::new();

    for cmd in commands {
        match cmd {
            WorkflowCommand::RecordMarker { .. }
            | WorkflowCommand::RecordSideEffect { .. }
            | WorkflowCommand::RecordUpdateResult { .. }
            | WorkflowCommand::UpsertSearchAttributes { .. }
            | WorkflowCommand::SetCurrentDetails { .. }
            | WorkflowCommand::PublishProgress { .. }
            | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
            | WorkflowCommand::CancelRaceLosers { .. }
            | WorkflowCommand::ArmTimer { .. }
            | WorkflowCommand::CancelTimer { .. } => {}
            WorkflowCommand::WaitForActivity { activity_id, .. } => activity_ids.push(*activity_id),
            _ => return None,
        }
    }

    if activity_ids.is_empty() {
        None
    } else {
        Some(activity_ids)
    }
}

fn extract_started_timer_for_suspension(
    commands: &[WorkflowCommand],
) -> Option<StartedTimerCommand> {
    // Find all StartTimer commands.
    let mut timers = commands.iter().filter_map(|cmd| {
        if let WorkflowCommand::StartTimer {
            timer_id,
            duration_secs,
            ..
        } = cmd
        {
            Some(StartedTimerCommand {
                timer_id: timer_id.clone(),
                duration_secs: *duration_secs,
            })
        } else {
            None
        }
    });

    let first_timer = timers.next()?;

    // If there is more than one StartTimer command, we don't support parallel timers in this branch.
    if timers.next().is_some() {
        return None;
    }

    // Now verify that all other commands in the batch are either bookkeeping OR signal waits.
    let is_valid = commands.iter().all(|cmd| {
        matches!(
            cmd,
            WorkflowCommand::StartTimer { .. }
                | WorkflowCommand::WaitForSignal { .. }
                | WorkflowCommand::SignalExternalWorkflow { .. }
                | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                | WorkflowCommand::RecordMarker { .. }
                | WorkflowCommand::RecordSideEffect { .. }
                | WorkflowCommand::RecordUpdateResult { .. }
                | WorkflowCommand::UpsertSearchAttributes { .. }
                | WorkflowCommand::SetCurrentDetails { .. }
                | WorkflowCommand::PublishProgress { .. }
                | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
                | WorkflowCommand::CancelRaceLosers { .. }
                | WorkflowCommand::ArmTimer { .. }
                | WorkflowCommand::CancelTimer { .. }
        )
    });

    if is_valid { Some(first_timer) } else { None }
}

/// Extract all `StartChildWorkflow` commands when every non-bookkeeping command is
/// a child-workflow start.  Returns `Some(children)` (may have length > 1 for
/// parallel spawns) or `None` if any non-bookkeeping command is of a different type.
/// `RecordMarker` and `RecordUpdateResult` are considered bookkeeping and ignored.
fn extract_all_started_child_workflows(
    commands: &[WorkflowCommand],
) -> Option<Vec<StartedChildWorkflowCommand>> {
    let non_markers: Vec<&WorkflowCommand> = commands
        .iter()
        .filter(|c| {
            !matches!(
                c,
                WorkflowCommand::RecordMarker { .. }
                    | WorkflowCommand::RecordSideEffect { .. }
                    | WorkflowCommand::RecordUpdateResult { .. }
                    | WorkflowCommand::UpsertSearchAttributes { .. }
                    | WorkflowCommand::SetCurrentDetails { .. }
                    | WorkflowCommand::PublishProgress { .. }
                    | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
                    | WorkflowCommand::CancelRaceLosers { .. }
                    | WorkflowCommand::ArmTimer { .. }
                    | WorkflowCommand::CancelTimer { .. }
            )
        })
        .collect();

    if non_markers.is_empty() {
        return None;
    }

    non_markers
        .iter()
        .map(|cmd| {
            if let WorkflowCommand::StartChildWorkflow {
                child_id,
                workflow_name,
                input,
                ..
            } = cmd
            {
                Some(StartedChildWorkflowCommand {
                    child_id: *child_id,
                    workflow_name: workflow_name.clone(),
                    input: input.clone(),
                })
            } else {
                None
            }
        })
        .collect()
}

/// Extract the child-timeout race suspension shape (issue #779): **exactly one**
/// `StartChildWorkflow` **and exactly one** `StartTimer`, with every other command
/// pure bookkeeping. Returns `Some((child, timer))` for that shape, `None`
/// otherwise.
///
/// This is the deliberate enforcement point for the AC9 caveat "a child-timeout
/// wait cannot share a suspension batch with a new activity/child": any extra
/// `ScheduleActivity`, a second `StartChildWorkflow`, a second `StartTimer`, a
/// `WaitForSignal`, an `ArmTimer`/`CancelTimer`, or a `SpawnDetachedChildWorkflow`
/// makes this return `None`, so the batch falls through to the generic
/// "unsupported commands" failure (fail-loud) rather than silently dropping work.
///
/// Only pure bookkeeping that the earlier `handle_suspended_workflow` steps and
/// [`build_suspension_events`] fully handle is tolerated alongside the pair
/// (markers, side-effects, update results, search-attribute upserts,
/// current-details breadcrumbs, and `CancelRaceLosers` from a prior sequential
/// race). It is intentionally *additive*: it does not touch
/// [`extract_started_timer_for_suspension`] or [`extract_all_started_child_workflows`].
fn extract_child_timeout_race(
    commands: &[WorkflowCommand],
) -> Option<(StartedChildWorkflowCommand, StartedTimerCommand)> {
    let mut child: Option<StartedChildWorkflowCommand> = None;
    let mut timer: Option<StartedTimerCommand> = None;

    for cmd in commands {
        match cmd {
            WorkflowCommand::StartChildWorkflow {
                child_id,
                workflow_name,
                input,
                ..
            } => {
                if child.is_some() {
                    // A second child means this is a plain parallel child fan-out,
                    // not a child-timeout race.
                    return None;
                }
                child = Some(StartedChildWorkflowCommand {
                    child_id: *child_id,
                    workflow_name: workflow_name.clone(),
                    input: input.clone(),
                });
            }
            WorkflowCommand::StartTimer {
                timer_id,
                duration_secs,
                ..
            } => {
                if timer.is_some() {
                    return None;
                }
                // Require the reserved `__child_timeout:` prefix that
                // `spawn_child_workflow_timeout` stamps on its deadline timer.
                // Without this, an arbitrary
                // `tokio::join!(spawn_child_workflow(..), timer("mytimer", n))`
                // batch (one plain child + one ordinary timer) would be silently
                // treated as the child-timeout primitive, bypassing the previous
                // fail-loud "unsupported commands" behavior — and on a child-win
                // no `spawn_child_workflow_timeout` teardown would run to delete
                // that ordinary timer row, so the parent could complete with an
                // unfired `harvest_timers` dependency. Fall through to the generic
                // path (return None) when the prefix is absent.
                if !timer_id
                    .as_str()
                    .starts_with(crate::context::CHILD_TIMEOUT_TIMER_PREFIX)
                {
                    return None;
                }
                timer = Some(StartedTimerCommand {
                    timer_id: timer_id.clone(),
                    duration_secs: *duration_secs,
                });
            }
            // Pure bookkeeping tolerated alongside the pair. Deliberately excludes
            // ArmTimer/CancelTimer (would need plan_timer_lifecycle) and
            // SpawnDetachedChildWorkflow (would need create_detached_child_executions)
            // — neither is ever emitted in a child-timeout batch, and tolerating
            // them here would silently drop their events. Fail loud instead.
            WorkflowCommand::RecordMarker { .. }
            | WorkflowCommand::RecordSideEffect { .. }
            | WorkflowCommand::RecordUpdateResult { .. }
            | WorkflowCommand::UpsertSearchAttributes { .. }
            | WorkflowCommand::SetCurrentDetails { .. }
            | WorkflowCommand::PublishProgress { .. }
            | WorkflowCommand::CancelRaceLosers { .. } => {}
            // Anything else (activity, activity wait, signal wait, external
            // activity/signal/cancel, detached spawn, arm/cancel timer) → not this
            // shape.
            _ => return None,
        }
    }

    match (child, timer) {
        (Some(c), Some(t)) => Some((c, t)),
        _ => None,
    }
}

fn extract_single_schedule_external_activity(
    commands: &[WorkflowCommand],
) -> Option<ScheduledExternalActivityCommand> {
    extract_single_command(commands, |cmd| {
        let WorkflowCommand::ScheduleExternalActivity {
            activity_id,
            token,
            name,
            input,
            queue,
            schedule_to_close_secs,
            ..
        } = cmd
        else {
            return None;
        };

        Some(ScheduledExternalActivityCommand {
            activity_id: *activity_id,
            token: *token,
            name: name.clone(),
            input: input.clone(),
            queue: queue.clone(),
            schedule_to_close_secs: *schedule_to_close_secs,
        })
    })
}

// ── Local activity support ──────────────────────────────────────────────────

struct LocalActivityRun {
    activity_id: crate::types::ActivityExecId,
    name: String,
    input: serde_json::Value,
    /// Resolved per-attempt `start_to_close` as a full [`Duration`] (issue #620,
    /// Codex P2 — full precision, not secs/millis, so no floor is ever truncated
    /// to `0`). `None` defers to the worker cap.
    start_to_close: Option<Duration>,
    retry_policy: Option<crate::policy::RetryPolicy>,
    /// `true` when `LocalActivityScheduled` is already in the durable history —
    /// the worker crashed after appending it but before recording a terminal event.
    /// `run_local_activity_inline` must skip re-appending the scheduled event.
    already_scheduled: bool,
    /// Number of `LocalActivityFailed` events already durable in history.
    /// `run_local_activity_inline` starts its retry loop from `failed_attempts + 1`.
    failed_attempts: u32,
    /// Error from the last recorded `LocalActivityFailed`. Returned immediately
    /// when `failed_attempts >= max_attempts` without running the handler again.
    last_error: Option<String>,
}

struct LocalActivityCommandBatch {
    pre_schedule_events: Vec<WorkflowEvent>,
    post_schedule_events: Vec<WorkflowEvent>,
    detached_commands: Vec<WorkflowCommand>,
    run: LocalActivityRun,
}

enum LocalActivityInlineOutcome {
    Complete(Vec<WorkflowEvent>),
    HistoryCapReached {
        events: Vec<WorkflowEvent>,
        event_count: u64,
    },
}

fn local_activity_history_cap_reached(next_event_id: i32, cap: Option<u64>) -> Option<u64> {
    let cap = cap?;
    let count = u64::try_from(next_event_id).unwrap_or(u64::MAX);
    (count >= cap).then_some(count)
}

/// Extract a `RunLocalActivity` command from an owned command list.
///
/// Marker and detached-spawn events are split around the local activity command
/// so `LocalActivityScheduled` is written at its actual command position.
/// The `result_tx` inside the command is dropped immediately — the workflow
/// coroutine was already dropped when the 100 ms suspension timeout fired, so
/// nobody is listening on the receiving end.
fn extract_run_local_activity(commands: Vec<WorkflowCommand>) -> LocalActivityCommandBatch {
    // ⚡ Bolt: Pre-allocate vector capacity to avoid intermediate allocations
    let mut pre_schedule_events = Vec::with_capacity(commands.len());
    let mut post_schedule_events = Vec::new();
    let mut detached_commands = Vec::new();
    let mut local_run = None;
    for cmd in commands {
        match cmd {
            WorkflowCommand::RecordMarker { name, details } => {
                let event = WorkflowEvent::MarkerRecorded { name, details };
                if local_run.is_some() {
                    post_schedule_events.push(event);
                } else {
                    pre_schedule_events.push(event);
                }
            }
            WorkflowCommand::RecordSideEffect { kind, name, value } => {
                let event = WorkflowEvent::SideEffectRecorded { kind, name, value };
                if local_run.is_some() {
                    post_schedule_events.push(event);
                } else {
                    pre_schedule_events.push(event);
                }
            }
            WorkflowCommand::SpawnDetachedChildWorkflow {
                child_id,
                workflow_name,
                input,
                parent_close_policy,
            } => {
                detached_commands.push(WorkflowCommand::SpawnDetachedChildWorkflow {
                    child_id,
                    workflow_name: workflow_name.clone(),
                    input: input.clone(),
                    parent_close_policy,
                });
                let event = WorkflowEvent::ChildWorkflowSpawnedDetached {
                    child_id,
                    workflow_name,
                    input,
                    parent_close_policy,
                };
                if local_run.is_some() {
                    post_schedule_events.push(event);
                } else {
                    pre_schedule_events.push(event);
                }
            }
            WorkflowCommand::RunLocalActivity {
                activity_id,
                name,
                input,
                start_to_close,
                retry_policy,
                result_tx,
                already_scheduled,
                failed_attempts,
                last_error,
            } => {
                drop(result_tx); // coroutine already dropped; close the channel
                local_run = Some(LocalActivityRun {
                    activity_id,
                    name,
                    input,
                    start_to_close,
                    retry_policy,
                    already_scheduled,
                    failed_attempts,
                    last_error,
                });
            }
            _ => {} // unexpected alongside RunLocalActivity; ignore
        }
    }
    LocalActivityCommandBatch {
        pre_schedule_events,
        post_schedule_events,
        detached_commands,
        run: local_run.expect("called only after confirming RunLocalActivity is present"),
    }
}

// ---------------------------------------------------------------------------
// SignalExternalWorkflow inline dispatch (same-shard)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SignalExternalWorkflowRun {
    signal_id: crate::types::ExternalSignalId,
    target: ExecutionId,
    signal_name: String,
    payload: serde_json::Value,
    /// `true` when `ExternalSignalRequested` is already durable — worker crashed
    /// after appending it but before recording the terminal event. Skip
    /// re-appending and go straight to (re-)attempting delivery.
    already_requested: bool,
    /// Optional exactly-once delivery key, threaded from the
    /// `SignalExternalWorkflow` command into the persisted
    /// `ExternalSignalRequested` event and the delivery insert.
    idempotency_key: Option<String>,
}

#[derive(Clone)]
struct CancelExternalWorkflowRun {
    cancel_id: crate::types::ExternalCancelId,
    target: ExecutionId,
    already_requested: bool,
}

/// An item in the ordered inline-dispatch batch: either a marker event, a
/// signal run, or a cancel run. Preserving the original command-emission order
/// is required so that the replay cursor sees events in the exact same sequence
/// as during the live execution that produced them.
#[derive(Clone)]
enum SignalBatchItem {
    Marker(WorkflowEvent),
    Signal(SignalExternalWorkflowRun),
    Cancel(CancelExternalWorkflowRun),
}

/// Extract `SignalExternalWorkflow` and `RecordMarker` commands in emission
/// order.
///
/// `result_tx` channels are dropped immediately — the workflow coroutine is
/// not awaiting them during inline dispatch. `RecordUpdateResult` and
/// `UpsertSearchAttributes` commands are intentionally skipped here because
/// they were already persisted by the caller before this function is invoked.
fn extract_signal_external_workflow(commands: Vec<WorkflowCommand>) -> Vec<SignalBatchItem> {
    let mut items = Vec::with_capacity(commands.len());
    for cmd in commands {
        match cmd {
            WorkflowCommand::RecordMarker { name, details } => {
                items.push(SignalBatchItem::Marker(WorkflowEvent::MarkerRecorded {
                    name,
                    details,
                }));
            }
            WorkflowCommand::RecordSideEffect { kind, name, value } => {
                items.push(SignalBatchItem::Marker(WorkflowEvent::SideEffectRecorded {
                    kind,
                    name,
                    value,
                }));
            }
            WorkflowCommand::SignalExternalWorkflow {
                signal_id,
                target,
                signal_name,
                payload,
                result_tx,
                already_requested,
                idempotency_key,
            } => {
                drop(result_tx);
                items.push(SignalBatchItem::Signal(SignalExternalWorkflowRun {
                    signal_id,
                    target,
                    signal_name,
                    payload,
                    already_requested,
                    idempotency_key,
                }));
            }
            WorkflowCommand::RequestCancelExternalWorkflow {
                cancel_id,
                target,
                result_tx,
                already_requested,
            } => {
                drop(result_tx);
                items.push(SignalBatchItem::Cancel(CancelExternalWorkflowRun {
                    cancel_id,
                    target,
                    already_requested,
                }));
            }
            _ => {}
        }
    }
    items
}

/// External-op resolution ids that were appended to history INLINE in the
/// current decision cycle (issue #678).
///
/// For a mixed `select!{ timer(..), signal_external_workflow(..) }` batch whose
/// external target lives on the SAME shard, `persist_external_signal_inline`
/// resolves the `ExternalSignalDelivered`/`ExternalCancelDelivered` terminal
/// synchronously and returns it in `new_events`. The remaining `StartTimer`
/// command then parks the task at `fires_at` (up to an hour away). This value
/// threads the ids resolved THIS cycle down to [`persist_started_timer`] so it
/// can mark the parked row wakeable and self-wake immediately, rather than
/// sleeping out the timer.
///
/// It is populated ONLY from terminals appended this cycle — never a blanket
/// history scan — so a pure `ctx.timer()` sleep in a workflow that did an
/// earlier, fully-observed external op never false-wakes (see the correctness
/// note in [`persist_started_timer`]). Every suspension path other than the
/// mixed timer + external arm threads [`ResolvedExternalIds::default`] (empty),
/// so their behaviour is byte-for-byte unchanged.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ResolvedExternalIds {
    signal_ids: Vec<crate::types::ExternalSignalId>,
    cancel_ids: Vec<crate::types::ExternalCancelId>,
}

impl ResolvedExternalIds {
    const fn is_empty(&self) -> bool {
        self.signal_ids.is_empty() && self.cancel_ids.is_empty()
    }
}

/// Scan `new_events` — the events [`persist_external_signal_inline`] appended
/// for THIS batch on this decision cycle — for the terminals that resolved each
/// external op inline, returning their ids.
///
/// `new_events` contains ONLY the `External{Signal,Cancel}Requested` events plus
/// any inline terminal (`Delivered`/`Failed`) appended for this batch, so the
/// resolved-id set is fully derivable from it alone — the batch items aren't
/// needed. A `Requested` event with no matching terminal (still pending → outbox
/// route) contributes no id.
fn resolved_external_ids(new_events: &[WorkflowEvent]) -> ResolvedExternalIds {
    let mut resolved = ResolvedExternalIds::default();
    for event in new_events {
        match event {
            WorkflowEvent::ExternalSignalDelivered { signal_id }
            | WorkflowEvent::ExternalSignalFailed { signal_id, .. } => {
                resolved.signal_ids.push(*signal_id);
            }
            WorkflowEvent::ExternalCancelDelivered { cancel_id }
            | WorkflowEvent::ExternalCancelFailed { cancel_id, .. } => {
                resolved.cancel_ids.push(*cancel_id);
            }
            _ => {}
        }
    }
    resolved
}

/// Split a mixed command batch into signal-batch items and remaining workflow commands.
///
/// Used when a batch contains both `SignalExternalWorkflow` and other durable
/// commands (e.g. `ScheduleActivity`, `StartTimer`). The signal items are written
/// to history inline first; the remaining commands are passed to
/// `handle_suspended_workflow` for normal suspension.
///
/// `RecordUpdateResult` and `UpsertSearchAttributes` commands are dropped because
/// the caller persists them before invoking this function.
fn split_mixed_signal_batch(
    commands: Vec<WorkflowCommand>,
) -> (Vec<SignalBatchItem>, Vec<WorkflowCommand>) {
    let mut signal_items = Vec::new();
    let mut remaining = Vec::new();
    for cmd in commands {
        match cmd {
            WorkflowCommand::SignalExternalWorkflow {
                signal_id,
                target,
                signal_name,
                payload,
                result_tx,
                already_requested,
                idempotency_key,
            } => {
                drop(result_tx);
                signal_items.push(SignalBatchItem::Signal(SignalExternalWorkflowRun {
                    signal_id,
                    target,
                    signal_name,
                    payload,
                    already_requested,
                    idempotency_key,
                }));
            }
            WorkflowCommand::RequestCancelExternalWorkflow {
                cancel_id,
                target,
                result_tx,
                already_requested,
            } => {
                drop(result_tx);
                signal_items.push(SignalBatchItem::Cancel(CancelExternalWorkflowRun {
                    cancel_id,
                    target,
                    already_requested,
                }));
            }
            WorkflowCommand::RecordUpdateResult { .. }
            | WorkflowCommand::UpsertSearchAttributes { .. }
            | WorkflowCommand::SetCurrentDetails { .. }
            | WorkflowCommand::PublishProgress { .. } => {}
            other => remaining.push(other),
        }
    }
    (signal_items, remaining)
}

/// Deliver all `SignalExternalWorkflow` commands inline and append durability events.
///
/// Same-shard delivery: writes directly to `harvest_signals` and wakes the
/// target task. Cross-shard delivery requires the plugin's outbox and is
/// outside the scope of this function — cross-shard targets are reported as
/// `target_unknown`.
///
/// Processes each item in command-emission order so the replay cursor advances
/// correctly. Returns all newly-appended events in emission order so the caller
/// can extend its in-memory replay history without a DB round-trip.
///
/// Crash-recovery re-delivery is deduplicated when the request carried an
/// `idempotency_key`: the partial unique index on `harvest_signals` rejects the
/// duplicate row, and the recorded key is reused verbatim from history so a
/// re-dispatch cannot diverge from the original delivery.
///
/// Returned tuple: (new history events, next event id, deferred trigger starts
/// to spawn after commit, (`workflow_name`, `queue_name`) of targets newly
/// cancelled inline for terminal metrics).
type InlinePersistResult = (
    Vec<WorkflowEvent>,
    i32,
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(String, String)>,
    Vec<(ExecutionId, String)>,
);

#[allow(clippy::too_many_lines)]
async fn persist_external_signal_inline(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    items: Vec<SignalBatchItem>,
    next_event_id: &mut i32,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<Vec<WorkflowEvent>> {
    let start_next = *next_event_id;

    // Persist the whole inline batch (request + delivery + terminal appends) in a
    // single transaction so a concurrently-running outbox sweep (on another
    // connection/worker) never observes an `External{Signal,Cancel}Requested`
    // event without its terminal: after commit both are visible, before commit
    // neither is. Without this the outbox could see the half-written request,
    // deliver it, and append the terminal first, leaving the inline path to
    // append the same terminal at a now-stale `next_event_id` — a history write
    // conflict that fails the caller even though delivery succeeded (issue #492).
    let (new_events, final_next, deferred_starts, cancel_metrics, deferred_checks): InlinePersistResult = conn
        .transaction::<InlinePersistResult, HarvestError, _>(|conn| {
            async move {
                let mut new_events: Vec<WorkflowEvent> = Vec::new();
                let mut next = start_next;
                // Completion-trigger / cascade follow-up starts produced by
                // same-shard cancellations. These must be spawned only *after*
                // this outer transaction commits, otherwise a later rollback
                // would leave trigger workflows started for a cancellation that
                // never became durable (issue #492).
                let mut deferred_starts: Vec<crate::completion_trigger::DeferredTriggerStart> =
                    Vec::new();
                // (workflow_name, queue_name) of targets newly cancelled inline,
                // so the terminal metric is recorded after commit.
                let mut cancel_metrics: Vec<(String, String)> = Vec::new();
                let mut deferred_checks: Vec<(ExecutionId, String)> = Vec::new();

                for item in items {
                    match item {
                        SignalBatchItem::Marker(event) => {
                            store::append_events(conn, exec_id, std::slice::from_ref(&event), next)
                                .await?;
                            next += 1;
                            new_events.push(event);
                        }
                        SignalBatchItem::Signal(run) => {
                            if !run.already_requested {
                                let requested = WorkflowEvent::ExternalSignalRequested {
                                    signal_id: run.signal_id,
                                    target: run.target,
                                    signal_name: run.signal_name.clone(),
                                    payload: run.payload.clone(),
                                    idempotency_key: run.idempotency_key.clone(),
                                };
                                store::append_events(
                                    conn,
                                    exec_id,
                                    std::slice::from_ref(&requested),
                                    next,
                                )
                                .await?;
                                next += 1;
                                new_events.push(requested);
                            }

                            // If cross-shard, skip inline delivery entirely and let
                            // the background outbox handle it.
                            if run.target.shard() != exec_id.shard() {
                                continue;
                            }

                            // Same-shard delivery attempt. A deduped insert
                            // (`Ok(false)`, idempotency-key collision) means the
                            // signal already landed once — that is success, so
                            // both outcomes record `ExternalSignalDelivered`.
                            let terminal_opt = match signal::send_signal_idempotent(
                                conn,
                                run.target,
                                &run.signal_name,
                                run.payload,
                                run.idempotency_key.as_deref(),
                            )
                            .await
                            {
                                Ok(_delivered_or_deduped) => {
                                    Some(WorkflowEvent::ExternalSignalDelivered {
                                        signal_id: run.signal_id,
                                    })
                                }
                                Err(HarvestError::NotFound(_)) => {
                                    // Same-shard target not found: suspend inline
                                    // delivery and leave resolution to outbox.
                                    None
                                }
                                Err(HarvestError::Database(e)) => {
                                    return Err(HarvestError::Database(e));
                                }
                                Err(_) => Some(WorkflowEvent::ExternalSignalFailed {
                                    signal_id: run.signal_id,
                                    reason_code: "target_terminal".to_string(),
                                }),
                            };

                            if let Some(terminal) = terminal_opt {
                                store::append_events(
                                    conn,
                                    exec_id,
                                    std::slice::from_ref(&terminal),
                                    next,
                                )
                                .await?;
                                next += 1;
                                new_events.push(terminal);
                            }
                        }
                        SignalBatchItem::Cancel(run) => {
                            if !run.already_requested {
                                let requested = WorkflowEvent::ExternalCancelRequested {
                                    cancel_id: run.cancel_id,
                                    target: run.target,
                                };
                                store::append_events(
                                    conn,
                                    exec_id,
                                    std::slice::from_ref(&requested),
                                    next,
                                )
                                .await?;
                                next += 1;
                                new_events.push(requested);
                            }

                            // Cross-shard: leave for the outbox scanner.
                            if run.target.shard() != exec_id.shard() {
                                continue;
                            }

                            // Same-shard cancel attempt.
                            // Already-CANCELLED and already-terminal targets are
                            // no-op success (goal "target not running" already met).
                            // Use the collect variant so the target's
                            // completion-trigger starts are spawned only after this
                            // outer transaction commits (issue #492).
                            let terminal_opt = match cancel_workflow_execution_collect(
                                conn,
                                run.target,
                                "cancelled by external request",
                            )
                            .await
                            {
                                Err(HarvestError::NotFound(_)) => {
                                    // Target not found: leave for outbox grace window.
                                    None
                                }
                                Err(HarvestError::Database(e)) => {
                                    return Err(HarvestError::Database(e));
                                }
                                Ok((_cancelled, deferred, checks, metrics_opt)) => {
                                    deferred_starts.extend(deferred);
                                    deferred_checks.extend(checks);
                                    if let Some(m) = metrics_opt {
                                        cancel_metrics.push(m);
                                    }
                                    Some(WorkflowEvent::ExternalCancelDelivered {
                                        cancel_id: run.cancel_id,
                                    })
                                }
                                // Other Err (already terminal) = no-op success.
                                Err(_) => Some(WorkflowEvent::ExternalCancelDelivered {
                                    cancel_id: run.cancel_id,
                                }),
                            };

                            if let Some(terminal) = terminal_opt {
                                store::append_events(
                                    conn,
                                    exec_id,
                                    std::slice::from_ref(&terminal),
                                    next,
                                )
                                .await?;
                                next += 1;
                                new_events.push(terminal);
                            }
                        }
                    }
                }

                Ok((new_events, next, deferred_starts, cancel_metrics, deferred_checks))
            }
            .scope_boxed()
        })
        .await?;

    // The inline batch is durably committed: now spawn trigger/cascade follow-up
    // starts and record terminal metrics for any targets cancelled above.
    for start in deferred_starts {
        start.spawn();
    }
    for check in deferred_checks {
        let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, Some(metrics)).await;
    }
    for (workflow_name, queue_name) in cancel_metrics {
        metrics.record_workflow_terminal(
            &workflow_name,
            &queue_name,
            crate::telemetry::WorkflowStatus::Cancelled,
        );
    }

    *next_event_id = final_next;
    Ok(new_events)
}

/// Run a local activity inline, appending durability events to `harvest_events`.
///
/// Retries the handler up to `max_attempts` times (per the retry policy),
/// sleeping the computed backoff between attempts. Each attempt appends a
/// `LocalActivityFailed` event; on success a `LocalActivityCompleted` event is
/// appended. Returns all newly-appended events so the caller can extend its
/// in-memory replay history and avoid a DB round-trip.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn run_local_activity_inline(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    exec_id: ExecutionId,
    batch: LocalActivityCommandBatch,
    detached_spawns: DetachedSpawnPersistence<'_>,
    max_start_to_close: Duration,
    next_event_id: &mut i32,
    context_headers: std::sync::Arc<std::collections::HashMap<String, String>>,
    // Owning workflow task queue, used only as the `queue` label on the
    // contained-local-activity-panic metric (issue #782).
    queue_name: &str,
) -> HarvestResult<LocalActivityInlineOutcome> {
    let LocalActivityCommandBatch {
        pre_schedule_events,
        post_schedule_events,
        detached_commands,
        run,
    } = batch;
    let activity = registry.activities.get(&run.name).ok_or_else(|| {
        HarvestError::Config(format!("no activity handler registered for '{}'", run.name))
    })?;
    // Issue #965: a WASM-backed activity is dispatched through the remote
    // task-queue seam (`process_activity_task`), never inline. `wasm_activity()`
    // always registers a non-local activity, so this is unreachable via the
    // public API; guard defensively for a hand-built local `ActivityInfo` that
    // also has a WASM binding, rather than silently running the native stub.
    #[cfg(feature = "wasm-activities")]
    if registry.wasm_binding(&run.name).is_some() {
        return Err(HarvestError::Config(format!(
            "activity '{}' is WASM-backed and cannot run as a local activity; \
             register it as a non-local activity",
            run.name
        )));
    }
    if activity.default_schedule_to_close.is_some() {
        return Err(HarvestError::Config(format!(
            "activity '{}' is local but has schedule_to_close set; \
             local activities do not support schedule_to_close (use start_to_close instead)",
            run.name
        )));
    }
    let history_event_hard_cap = registry.history_policy().event_hard_cap();

    // Per-attempt timeout at FULL `Duration` precision (issue #620, Codex P2). A
    // subsecond OR sub-millisecond floor must NOT truncate to
    // `Duration::from_secs(0)`/`from_millis(0)` and instantly time out — the
    // command carries the exact `Duration`, honored directly, then clamped by the
    // worker cap (`Duration` implements `Ord`).
    let per_attempt_timeout = run
        .start_to_close
        .unwrap_or(max_start_to_close)
        .min(max_start_to_close);

    let max_attempts = run.retry_policy.as_ref().map_or(1, |p| p.max_attempts);

    // When the worker crashed after appending LocalActivityScheduled but before
    // recording a terminal event, skip re-appending to avoid a duplicate.
    let mut prefix_events = pre_schedule_events;
    if !run.already_scheduled {
        prefix_events.push(WorkflowEvent::LocalActivityScheduled {
            activity_id: run.activity_id,
            name: run.name.clone(),
            input: run.input.clone(),
            // Issue #620 (AC8; Codex P2): the resolution marker. Every #620+
            // schedule writes `true` at this first-schedule anchor so the
            // crash-recovery path treats the frozen retry/STC below as
            // authoritative (even when `None`), distinct from a pre-#620 legacy
            // event (marker absent → `false` → re-derive live).
            resolved: true,
            // Freeze the fully-resolved retry policy AND start_to_close (already
            // call → activity → builder resolved in
            // `execute_local_activity_raw_resolved`) so a crash-recovery replay
            // keeps this ORIGINAL budget/timeout even if the builder-level
            // default changed in the crash window. `None` = "explicitly resolved
            // to no floor" — still frozen, since the marker is `true`. Stored as
            // full-precision nanos so recovery restores the exact `Duration`.
            retry_policy: run.retry_policy.clone(),
            start_to_close_nanos: run
                .start_to_close
                .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)),
        });
    }
    prefix_events.extend(post_schedule_events);
    if !prefix_events.is_empty() || !detached_commands.is_empty() {
        let events = prefix_events.clone();
        let event_start = *next_event_id;
        conn.transaction::<(), HarvestError, _>(|conn| {
            async move {
                store::append_events(conn, exec_id, &events, event_start).await?;
                detached_spawns.persist(conn, &detached_commands).await
            }
            .scope_boxed()
        })
        .await?;
        *next_event_id += i32::try_from(prefix_events.len())
            .map_err(|_| HarvestError::Config("event count overflow".into()))?;
    }

    let mut all_new_events = prefix_events;
    if let Some(event_count) =
        local_activity_history_cap_reached(*next_event_id, history_event_hard_cap)
    {
        return Ok(LocalActivityInlineOutcome::HistoryCapReached {
            events: all_new_events,
            event_count,
        });
    }

    let handler = activity.handler;
    let local_idempotency_key = IdempotencyKey::from_activity_exec_id(run.activity_id);

    // When recovering after a crash-between-retries, all attempts up to
    // `failed_attempts` are already durable in history. If they already cover
    // max_attempts, every retry was exhausted before the crash — return the
    // last recorded error without executing the handler again.
    let start_attempt = run.failed_attempts + 1;
    if start_attempt > max_attempts {
        let error = run.last_error.unwrap_or_else(|| {
            format!(
                "local activity '{}' failed after {} attempts (recorded in history)",
                run.name, run.failed_attempts
            )
        });
        return Err(HarvestError::activity_failed(
            run.name.clone(),
            run.failed_attempts,
            &error,
        ));
    }

    // Track the error from the previous attempt to surface via previous_failure().
    // For crash-recovery (start_attempt > 1), the last recorded error is in run.last_error.
    let mut previous_failure: Option<String> = if start_attempt > 1 {
        run.last_error.clone()
    } else {
        None
    };

    // Issue #680: the activity interceptor chain wraps local activities too.
    // The invocation is `is_local = true`; the queue label is the owning
    // workflow task's queue. When no interceptors are registered
    // `dispatch_with_interceptors` is a zero-overhead direct handler call. NOTE:
    // for local activities the chain runs INSIDE the per-attempt local timeout
    // below, so interceptor time counts against the effective start_to_close.
    let interceptors = registry.activity_interceptors();
    let invocation = crate::interceptor::ActivityInvocation::new(&run.name, true, queue_name);

    for attempt in start_attempt..=max_attempts {
        let ctx =
            ActivityContext::new_local_activity(registry.shared_state(), CancellationToken::new())
                .with_context_headers(std::sync::Arc::clone(&context_headers))
                .with_metrics(registry.telemetry().metrics.clone())
                .with_idempotency_key(local_idempotency_key.clone())
                .with_attempt(attempt)
                .with_max_attempts(max_attempts)
                .with_previous_failure(previous_failure.clone());
        // Issue #782: contain a local-activity handler panic. Local activities
        // run inline in the workflow task, so an uncaught panic here would
        // unwind the whole workflow-task dispatch. Catch it and flatten into a
        // retryable typed HandlerPanic Err so it flows through the existing
        // Err branch (LocalActivityFailed retry path), honouring the retry
        // policy exactly like `Err(String)`.
        // Issue #782 (PR #1012 review): contain a panic during future
        // *construction* too — a hand-written local-activity handler may do
        // synchronous work before returning its boxed future, and that would
        // escape the poll-time `catch_unwind` below. Both a construction panic and
        // a poll panic converge on the same retryable typed HandlerPanic Err and
        // emit `record_activity_panic` exactly once per panicking attempt.
        let result = match crate::error::catch_construct(|| {
            crate::interceptor::dispatch_with_interceptors(
                interceptors,
                &invocation,
                &ctx,
                run.input.clone(),
                |input| (handler)(&ctx, input),
            )
        }) {
            Err(message) => {
                registry
                    .telemetry()
                    .metrics
                    .record_activity_panic(&run.name, queue_name);
                Err(handler_panic_activity_envelope(message))
            }
            Ok(fut) => {
                use futures::FutureExt as _;
                let caught = tokio::time::timeout(
                    per_attempt_timeout,
                    std::panic::AssertUnwindSafe(fut).catch_unwind(),
                )
                .await;
                match caught {
                    Ok(Ok(inner)) => inner,
                    Ok(Err(panic_payload)) => {
                        registry
                            .telemetry()
                            .metrics
                            .record_activity_panic(&run.name, queue_name);
                        Err(handler_panic_activity_envelope(
                            crate::error::panic_message(panic_payload),
                        ))
                    }
                    Err(_elapsed) => Err(format!(
                        "local activity '{}' timed out after {:?}",
                        run.name, per_attempt_timeout
                    )),
                }
            }
        };

        match result {
            Ok(output) => {
                let completed_event = WorkflowEvent::LocalActivityCompleted {
                    activity_id: run.activity_id,
                    output,
                };
                store::append_events(
                    conn,
                    exec_id,
                    std::slice::from_ref(&completed_event),
                    *next_event_id,
                )
                .await?;
                *next_event_id += 1;
                all_new_events.push(completed_event);
                if let Some(event_count) =
                    local_activity_history_cap_reached(*next_event_id, history_event_hard_cap)
                {
                    return Ok(LocalActivityInlineOutcome::HistoryCapReached {
                        events: all_new_events,
                        event_count,
                    });
                }
                return Ok(LocalActivityInlineOutcome::Complete(all_new_events));
            }
            Err(error) => {
                // Per issue #227: honour `ActivityFailure::non_retryable`
                // (and `RetryPolicy::non_retryable_errors`) for local
                // activities too. Without this check, a fail-fast local
                // activity would still retry up to `max_attempts`, defeating
                // the typed-failure guarantee documented in the README.
                let typed = parse_typed_payload(&error);
                let payload_non_retryable = typed.as_ref().is_some_and(|f| f.non_retryable);
                let typed_error_type = typed.as_ref().map(|f| f.error_type.as_str());
                let policy_non_retryable = run
                    .retry_policy
                    .as_ref()
                    .is_some_and(|p| p.is_non_retryable(typed_error_type, &error));
                let terminal_attempt =
                    attempt == max_attempts || payload_non_retryable || policy_non_retryable;

                // Persist the human-readable message in history events. For
                // typed `ActivityFailure` payloads we extract `.message` so
                // operators and the workflow's `HarvestError::ActivityFailed`
                // surface see "amount must be positive" rather than the
                // internal `{"harvest_activity_failure_v1":{...}}` envelope.
                // Mirrors what `finalize_activity_failure` does for regular
                // activities. (Local-activity events don't yet carry the
                // typed fields — see #227 follow-up for symmetry parity.)
                let stored_error = typed
                    .as_ref()
                    .map_or_else(|| error.clone(), |f| f.message.clone());
                let failed_event = WorkflowEvent::LocalActivityFailed {
                    activity_id: run.activity_id,
                    error: stored_error.clone(),
                    attempt,
                };

                if terminal_attempt {
                    let current_count = u64::try_from(*next_event_id).unwrap_or(u64::MAX);
                    let final_pair_would_exceed_cap = history_event_hard_cap
                        .is_some_and(|cap| current_count.saturating_add(2) > cap);
                    if final_pair_would_exceed_cap {
                        store::append_events(
                            conn,
                            exec_id,
                            std::slice::from_ref(&failed_event),
                            *next_event_id,
                        )
                        .await?;
                        *next_event_id += 1;
                        all_new_events.push(failed_event);
                        let event_count = u64::try_from(*next_event_id).unwrap_or(u64::MAX);
                        return Ok(LocalActivityInlineOutcome::HistoryCapReached {
                            events: all_new_events,
                            event_count,
                        });
                    }

                    // Final attempt: append LocalActivityFailed and
                    // LocalActivityExhausted atomically so a crash between the
                    // two cannot leave history without the terminal marker,
                    // which would make the policy-invariant guarantee unsound.
                    let exhausted_event = WorkflowEvent::LocalActivityExhausted {
                        activity_id: run.activity_id,
                        error: stored_error.clone(),
                        attempt,
                    };
                    let terminal_pair = [failed_event, exhausted_event];
                    store::append_events(conn, exec_id, &terminal_pair, *next_event_id).await?;
                    *next_event_id += i32::try_from(terminal_pair.len())
                        .map_err(|_| HarvestError::Config("event count overflow".into()))?;
                    all_new_events.extend(terminal_pair);
                    if let Some(event_count) =
                        local_activity_history_cap_reached(*next_event_id, history_event_hard_cap)
                    {
                        return Ok(LocalActivityInlineOutcome::HistoryCapReached {
                            events: all_new_events,
                            event_count,
                        });
                    }
                    // Must return here — without it, when `terminal_attempt` was
                    // set early by `payload_non_retryable` or `policy_non_retryable`
                    // (i.e. `attempt < max_attempts`), the `for` loop would
                    // re-execute the side-effecting handler on the next
                    // iteration, defeating the fail-fast guarantee.
                    return Ok(LocalActivityInlineOutcome::Complete(all_new_events));
                }

                // Non-terminal attempt: record the failure, optionally sleep,
                // and loop to the next attempt.
                store::append_events(
                    conn,
                    exec_id,
                    std::slice::from_ref(&failed_event),
                    *next_event_id,
                )
                .await?;
                *next_event_id += 1;
                all_new_events.push(failed_event);
                // Capture error for previous_failure() on the next attempt.
                previous_failure = Some(stored_error.clone());
                if let Some(event_count) =
                    local_activity_history_cap_reached(*next_event_id, history_event_hard_cap)
                {
                    return Ok(LocalActivityInlineOutcome::HistoryCapReached {
                        events: all_new_events,
                        event_count,
                    });
                }

                if let Some(delay) = run
                    .retry_policy
                    .as_ref()
                    .and_then(|p| p.next_delay(attempt))
                {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Ok(LocalActivityInlineOutcome::Complete(all_new_events))
}

fn chrono_duration_from_std(
    duration: Duration,
    field_name: &str,
) -> HarvestResult<chrono::Duration> {
    chrono::Duration::from_std(duration).map_err(|_| {
        HarvestError::Config(format!(
            "activity {field_name} duration exceeds chrono range"
        ))
    })
}

/// Resolve the effective start-to-close deadline for a WASM activity dispatch
/// (issue #965, AC1).
///
/// Mirrors the native dispatch path: a per-call `start_to_close` override
/// persisted on the task row (used by DAG / race activity calls) takes priority
/// over the activity's registration default, exactly as native scheduling
/// resolves `start_to_close_override.or(default_start_to_close)`. When the task
/// row carries no override (NULL column) we fall back to the registration
/// default; when both are absent the deadline is `None` and the runtime's
/// mandatory `max_wall_clock` ceiling (M2) still bounds the guest.
#[cfg(feature = "wasm-activities")]
fn wasm_effective_deadline(
    task_start_to_close: Option<chrono::Duration>,
    default_start_to_close: Option<Duration>,
) -> Option<Duration> {
    task_start_to_close
        .and_then(|d| d.to_std().ok())
        .or(default_start_to_close)
}

fn configured_retry_policy(task: &TaskQueueItem) -> HarvestResult<Option<RetryPolicy>> {
    task.retry_policy
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .map_err(HarvestError::from)
}

/// 1-based attempt counter for event recording. `pub(crate)` so
/// `timeout::force_fail_activity` (issue #765) fills the forced
/// `ActivityFailed` event's `attempt` field the exact same way the worker's
/// own `finalize_activity_failure` does.
pub(crate) fn task_attempt(task: &TaskQueueItem) -> u32 {
    u32::try_from(task.attempt.max(1)).unwrap_or(1)
}

#[allow(clippy::missing_const_for_fn)]
fn retry_stream_seed(task: &TaskQueueItem) -> u64 {
    let mut seed = 0xcbf2_9ce4_8422_2325_u64;
    if let Some(exec) = task.workflow_exec_id {
        let raw = exec.as_u128().to_le_bytes();
        seed ^= u64::from_le_bytes(raw[..8].try_into().unwrap_or([0_u8; 8]));
        seed = seed.wrapping_mul(0x1000_0000_01b3);
        seed ^= u64::from_le_bytes(raw[8..].try_into().unwrap_or([0_u8; 8]));
        seed = seed.wrapping_mul(0x1000_0000_01b3);
    }
    if let Some(activity) = task.activity_id {
        let raw = activity.as_u128().to_le_bytes();
        seed ^= u64::from_le_bytes(raw[..8].try_into().unwrap_or([0_u8; 8]));
        seed = seed.wrapping_mul(0x1000_0000_01b3);
        seed ^= u64::from_le_bytes(raw[8..].try_into().unwrap_or([0_u8; 8]));
        seed = seed.wrapping_mul(0x1000_0000_01b3);
    }
    seed
}

/// Read the current time from the database clock (`NOW()`).
///
/// Timer due-ness checks and the signal `received_at` column default both use
/// Postgres `NOW()`, so deadlines derived from this clock stay comparable to
/// them regardless of worker clock skew.
async fn db_clock_now(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<chrono::DateTime<chrono::Utc>> {
    use diesel::dsl::sql;
    use diesel::sql_types::Timestamptz;

    diesel::select(sql::<Timestamptz>("NOW()"))
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)
}

pub(crate) fn chrono_duration_from_secs(
    seconds: u64,
    field_name: &str,
) -> HarvestResult<chrono::Duration> {
    let seconds = i64::try_from(seconds).map_err(|_| {
        HarvestError::Config(format!("activity {field_name} exceeds i64 seconds range"))
    })?;
    chrono::Duration::try_seconds(seconds).ok_or_else(|| {
        HarvestError::Config(format!(
            "activity {field_name} exceeds chrono::Duration bounds"
        ))
    })
}

fn next_retry_delay(
    task: &TaskQueueItem,
    error: &str,
    retry_policy: Option<&RetryPolicy>,
) -> HarvestResult<Option<chrono::Duration>> {
    // Non-retryable (typed flag or policy `non_retryable_errors`, incl. legacy
    // `Err(String)`) short-circuits all remaining attempts. See
    // `failure_is_non_retryable` for the shared rule.
    if failure_is_non_retryable(error, retry_policy) {
        return Ok(None);
    }

    if let Some(policy) = retry_policy {
        return policy
            .next_delay_with_seed(task_attempt(task), retry_stream_seed(task))
            .map(|delay| chrono_duration_from_std(delay, "retry delay"))
            .transpose();
    }

    if task.attempt < task.max_attempts {
        return Ok(Some(chrono::Duration::seconds(1)));
    }

    Ok(None)
}

fn find_pending_scheduled_activity(
    history: &[WorkflowEvent],
    activity_name: &str,
) -> HarvestResult<ActivityExecId> {
    let terminal_ids = history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. } => Some(*activity_id),
            _ => None,
        })
        .collect::<HashSet<_>>();

    let mut pending = None;
    for event in history {
        if let WorkflowEvent::ActivityScheduled {
            activity_id, name, ..
        } = event
            && name == activity_name
            && !terminal_ids.contains(activity_id)
        {
            if pending.is_some() {
                return Err(HarvestError::non_deterministic_simple(format!(
                    "multiple pending scheduled activities named '{activity_name}' found in history"
                )));
            }
            pending = Some(*activity_id);
        }
    }

    pending.ok_or_else(|| {
        HarvestError::NotFound(format!(
            "no pending scheduled activity '{activity_name}' in workflow history"
        ))
    })
}

fn find_pending_scheduled_activity_by_id(
    history: &[WorkflowEvent],
    requested_activity_id: ActivityExecId,
    activity_name: &str,
) -> HarvestResult<ActivityExecId> {
    let mut scheduled = false;
    let mut terminal = false;

    for event in history {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if *activity_id == requested_activity_id => {
                if name != activity_name {
                    return Err(HarvestError::non_deterministic_simple(format!(
                        "activity task id '{}' was scheduled for '{name}', not '{activity_name}'",
                        requested_activity_id.as_uuid()
                    )));
                }
                scheduled = true;
            }
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. }
                if *activity_id == requested_activity_id =>
            {
                terminal = true;
            }
            _ => {}
        }
    }

    if scheduled && !terminal {
        Ok(requested_activity_id)
    } else if terminal {
        Err(HarvestError::NotFound(format!(
            "activity '{activity_name}' with id '{}' already has a terminal event",
            requested_activity_id.as_uuid()
        )))
    } else {
        Err(HarvestError::NotFound(format!(
            "no scheduled activity '{activity_name}' with id '{}' in workflow history",
            requested_activity_id.as_uuid()
        )))
    }
}

fn has_activity_terminal_event(history: &[WorkflowEvent], activity_id: ActivityExecId) -> bool {
    history.iter().any(|event| {
        matches!(
            event,
            WorkflowEvent::ActivityCompleted { activity_id: id, .. }
                | WorkflowEvent::ActivityFailed { activity_id: id, .. }
                | WorkflowEvent::ActivityTimedOut { activity_id: id, .. }
                if *id == activity_id
        )
    })
}

async fn lock_workflow_execution_and_load_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<store::EventHistory> {
    Ok(lock_workflow_execution_row_and_load_history(conn, exec_id)
        .await?
        .1)
}

/// Like [`lock_workflow_execution_and_load_history`], but also returns the
/// locked execution row itself — the `SELECT ... FOR UPDATE` loads the full
/// row anyway, so a caller that needs row-current execution metadata under
/// the lock (e.g. the retry path's PAUSED re-check, issue #609 post-review
/// hardening) gets it without a second query.
async fn lock_workflow_execution_row_and_load_history(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<(WorkflowExecution, store::EventHistory)> {
    let execution = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .for_update()
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

    let history = store::load_history(conn, exec_id).await?;
    Ok((execution, history))
}

async fn task_state_for_update(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
) -> HarvestResult<Option<String>> {
    use crate::schema::harvest_task_queue::dsl;

    dsl::harvest_task_queue
        .find(task_id)
        .for_update()
        .select(dsl::state)
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)
}

fn pending_activity_id_for_task(
    history: &[WorkflowEvent],
    task: &TaskQueueItem,
    activity_name: &str,
) -> HarvestResult<Option<ActivityExecId>> {
    if let Some(activity_id) = task.activity_id {
        let activity_id = ActivityExecId::from_uuid(activity_id);
        if has_activity_terminal_event(history, activity_id) {
            return Ok(None);
        }
        return find_pending_scheduled_activity_by_id(history, activity_id, activity_name)
            .map(Some);
    }

    find_pending_scheduled_activity(history, activity_name).map(Some)
}

async fn append_activity_started_if_pending(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    activity_name: &str,
    worker_id: &str,
) -> HarvestResult<Option<ActivityExecId>> {
    conn.transaction::<Option<ActivityExecId>, HarvestError, _>(|conn| {
        async move {
            let history = lock_workflow_execution_and_load_history(conn, exec_id).await?;
            let Some(activity_id) =
                pending_activity_id_for_task(&history.events, task, activity_name)?
            else {
                return Ok(None);
            };
            let Some(state) = task_state_for_update(conn, task.id).await? else {
                return Ok(None);
            };
            if state != "RUNNING" {
                return Ok(None);
            }

            let started_event = WorkflowEvent::ActivityStarted {
                activity_id,
                worker_id: WorkerId::new(worker_id),
            };
            store::append_events(conn, exec_id, &[started_event], history.next_event_id).await?;
            Ok(Some(activity_id))
        }
        .scope_boxed()
    })
    .await
}

async fn load_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<WorkflowExecution> {
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
}

fn terminal_execution_transition_error(
    exec_id: ExecutionId,
    state: &str,
    error: Option<&str>,
) -> HarvestError {
    match state {
        "CANCELLED" => HarvestError::Cancelled(error.map_or_else(
            || format!("workflow execution {exec_id} is cancelled"),
            ToOwned::to_owned,
        )),
        "RUNNING" => HarvestError::Config(format!(
            "workflow execution {exec_id} did not transition from RUNNING"
        )),
        state => HarvestError::Config(format!(
            "workflow execution {exec_id} is already terminal ({state})"
        )),
    }
}

async fn workflow_execution_transition_error(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<HarvestError> {
    use crate::schema::harvest_workflow_executions::dsl;

    dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select((dsl::state, dsl::error))
        .first::<(String, Option<String>)>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .map_or_else(
            || {
                Ok(HarvestError::NotFound(format!(
                    "workflow execution {exec_id}"
                )))
            },
            |(state, error)| {
                Ok(terminal_execution_transition_error(
                    exec_id,
                    &state,
                    error.as_deref(),
                ))
            },
        )
}

async fn update_workflow_execution_completed(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    worker_id: &str,
    output: &serde_json::Value,
) -> HarvestResult<()> {
    use crate::schema::harvest_workflow_executions::dsl;

    // Code-review fix (issue #603): read the pre-update block state so the
    // search_attrs clear below can be gated on it. A best-effort, unlocked
    // read is fine here -- a race against a concurrent (re-)block is
    // harmless (the block path stamps its own diagnostic independently, and
    // a missed belt-and-braces clear here is caught by the next terminal
    // transition or by `clear_nd_block`'s own guarded path).
    let was_nd_blocked = dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(dsl::nd_blocked_at.is_not_null())
        .first::<bool>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .unwrap_or(false);

    let updated = diesel::update(
        dsl::harvest_workflow_executions
            .find(exec_id.as_uuid())
            .filter(dsl::state.eq("RUNNING")),
    )
    .set((
        dsl::state.eq("COMPLETED"),
        dsl::output.eq(Some(output.clone())),
        dsl::error.eq(None::<String>),
        dsl::sticky_worker_id.eq(Some(worker_id.to_string())),
        dsl::completed_at.eq(Some(chrono::Utc::now())),
        // Belt-and-braces ND-block reset (issue #603): terminal paths that
        // bypass the pause-guarded persist transaction (and its clear hook)
        // must not leave a stale block marker on a closed run.
        dsl::nd_blocked_at.eq(None::<chrono::DateTime<chrono::Utc>>),
        dsl::nd_block_reason.eq(None::<String>),
        dsl::nd_block_count.eq(0),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(workflow_execution_transition_error(conn, exec_id).await?);
    }

    // Code-review fix (issue #603): the column reset above only clears the
    // marker; the search_attrs diagnostic must be cleared too, or a
    // previously-blocked execution that completes via a path bypassing the
    // pause-guarded transaction's `clear_nd_block` hook leaves a phantom
    // `failure_cause=non_determinism` on a run that actually completed fine.
    // Gated on `was_nd_blocked` (PR review fix): an unconditional clear here
    // would silently delete pre-existing user search_attrs of the same name
    // on rows created before these keys became reserved.
    // `nd_search_attrs_clear_patch`/`store::update_search_attrs` are defined
    // further below in this file; Rust item order doesn't matter here.
    if was_nd_blocked {
        crate::store::update_search_attrs(conn, exec_id, &nd_search_attrs_clear_patch()).await?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Non-terminal replay-non-determinism block (issue #603)
// ---------------------------------------------------------------------------

/// Base delay before the first blocked re-dispatch (issue #603).
const ND_BLOCK_BACKOFF_BASE_SECS: u64 = 5;

/// Ceiling on the blocked re-dispatch delay (issue #603). A permanently
/// diverging history is re-dispatched at most once per this interval, so a
/// blocked cohort can never hot-loop worker slots. Retries are otherwise
/// unbounded — the block is rate-limited, not attempt-capped, so a rollback at
/// any later time still resumes the execution (Temporal workflow-task-retry
/// semantics; see `docs/runbooks/nondeterminism-block.md`).
const ND_BLOCK_BACKOFF_CAP_SECS: u64 = 300;

/// Capped exponential backoff for blocked re-dispatches: `5s * 2^count`,
/// capped at 300s (issue #603).
///
/// `block_count` is the execution's `nd_block_count` *before* this block is
/// recorded, so the first block waits 5s and the seventh-and-later waits the
/// full 300s cap. Thin wrapper over the shared, more robust
/// [`crate::policy::compute_retry_delay`] (reuse per code-review finding) —
/// `attempt` is 1-based there, so `block_count` (0-based) maps to
/// `block_count + 1`; negative counts (impossible via the DB column, but
/// defensive) clamp to attempt 1.
fn nd_block_backoff(block_count: i32) -> Duration {
    let attempt = u32::try_from(block_count.max(0))
        .unwrap_or(u32::MAX)
        .saturating_add(1);
    crate::policy::compute_retry_delay(
        Duration::from_secs(ND_BLOCK_BACKOFF_BASE_SECS),
        2.0,
        Duration::from_secs(ND_BLOCK_BACKOFF_CAP_SECS),
        attempt,
    )
}

// ---------------------------------------------------------------------------
// Contained workflow handler-panic retry (issue #782)
// ---------------------------------------------------------------------------

/// Base delay before the first panic re-dispatch (issue #782). A short floor
/// (>0) prevents a fast, deterministic panic from hot-looping worker slots
/// while the operator hotfixes-and-redeploys.
const PANIC_RETRY_BACKOFF_BASE_SECS: u64 = 1;

/// Ceiling on the panic re-dispatch delay (issue #782). The panic budget is
/// small (default 3), so this cap is only reached with a deliberately-raised
/// `workflow_panic_max_attempts`.
const PANIC_RETRY_BACKOFF_CAP_SECS: u64 = 30;

/// Capped exponential backoff for a workflow panic re-dispatch: `1s * 2^(n-1)`,
/// capped at 30s (issue #782).
///
/// `strikes` is the panic-strike count **after** this cycle's increment (1-based),
/// so the first re-dispatch (`strikes == 1`) waits the base delay. Thin wrapper
/// over the shared [`crate::policy::compute_retry_delay`] (`attempt` is 1-based
/// there), so `strikes` maps directly to `attempt`; a `0` is clamped to `1`.
fn panic_retry_backoff(strikes: u32) -> Duration {
    crate::policy::compute_retry_delay(
        Duration::from_secs(PANIC_RETRY_BACKOFF_BASE_SECS),
        2.0,
        Duration::from_secs(PANIC_RETRY_BACKOFF_CAP_SECS),
        strikes.max(1),
    )
}

/// Whether a contained workflow panic should be re-dispatched or failed
/// terminally (issue #782). Pure decision, mirroring the poison-pill
/// `quarantine_decision` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PanicRetryDecision {
    /// Re-dispatch the workflow task with backoff (non-terminal).
    Requeue,
    /// Fail the run terminally with a typed `HandlerPanic` error.
    Terminal,
}

/// Decide retry-vs-terminal for a contained workflow panic given the
/// **post-increment** strike count and the configured budget (issue #782).
///
/// - `max == 0` disables panic-retry entirely: the first panic is terminal.
/// - Otherwise a strike **below** the budget re-dispatches; reaching the budget
///   (`strikes >= max`) fails terminally. With the default `max == 3`, panics
///   1 and 2 re-dispatch and panic 3 is terminal — i.e. exactly `max` panic
///   *entries* total, of which `max - 1` are re-dispatches.
const fn panic_retry_decision(strikes_after_increment: u32, max: u32) -> PanicRetryDecision {
    if max == 0 || strikes_after_increment >= max {
        PanicRetryDecision::Terminal
    } else {
        PanicRetryDecision::Requeue
    }
}

/// Increment (and return) the consecutive-panic strike count for `exec_id`
/// (issue #782). The mutex guard is scoped to this function so it is never held
/// across an `.await` or a DB call in the caller.
fn increment_panic_strike(
    strikes: &std::sync::Mutex<std::collections::HashMap<uuid::Uuid, u32>>,
    exec_id: uuid::Uuid,
) -> u32 {
    let mut guard = strikes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let count = guard.entry(exec_id).or_insert(0);
    *count = count.saturating_add(1);
    let result = *count;
    // Explicit early drop mirrors the sibling `workflow_task_timeout_strikes`
    // pattern and satisfies clippy::significant_drop_tightening (the guard must
    // outlive the `entry`/increment borrow, so it cannot be inlined).
    drop(guard);
    result
}

/// Clear the consecutive-panic strike entry for `exec_id` (issue #782), so a
/// non-panic cycle resets the count and the map does not grow unbounded. The
/// guard is scoped to this function.
fn clear_panic_strike(
    strikes: &std::sync::Mutex<std::collections::HashMap<uuid::Uuid, u32>>,
    exec_id: uuid::Uuid,
) {
    strikes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&exec_id);
}

/// Encode a contained **activity** handler-panic message as the typed
/// `harvest_activity_failure_v1` envelope carrying the engine-reserved
/// [`ERROR_TYPE_HANDLER_PANIC`](crate::failure::ERROR_TYPE_HANDLER_PANIC)
/// class (issue #782).
///
/// The failure is **retryable** — a caught activity panic follows the same
/// path as `Err(String)`, honouring the activity's retry policy — so it flows
/// through `handle_activity_result`'s existing `Err` branch and, on exhaustion,
/// dead-letters as ordinary retry-exhaustion rather than a poison-pill.
fn handler_panic_activity_envelope(message: String) -> String {
    use crate::failure::{ERROR_TYPE_HANDLER_PANIC, IntoActivityErrorString as _};
    crate::failure::ActivityFailure::retryable(ERROR_TYPE_HANDLER_PANIC, message)
        .into_error_payload()
}

/// Build the search-attrs diagnostic patch stamped on an execution when the
/// engine records a replay divergence (issues #480/#603): `failure_cause`
/// plus whichever of `event_index`/`expected`/`actual`/`workflow_type`/
/// `build_id` the [`crate::error::NonDeterministicDetails`] carries. Shared by
/// the terminal failure path (`update_workflow_execution_failed`) and the
/// non-terminal block path (`block_workflow_for_non_determinism`).
fn nd_search_attrs_patch(
    details: &crate::error::NonDeterministicDetails,
) -> std::collections::HashMap<String, Option<serde_json::Value>> {
    let mut patch = std::collections::HashMap::new();
    patch.insert(
        "failure_cause".to_string(),
        Some(serde_json::json!("non_determinism")),
    );
    if let Some(idx) = details.event_index {
        patch.insert("event_index".to_string(), Some(serde_json::json!(idx)));
    }
    if let Some(ref exp) = details.expected {
        patch.insert("expected".to_string(), Some(serde_json::json!(exp)));
    }
    if let Some(ref act) = details.actual {
        patch.insert("actual".to_string(), Some(serde_json::json!(act)));
    }
    if let Some(ref wf_type) = details.workflow_type {
        patch.insert(
            "workflow_type".to_string(),
            Some(serde_json::json!(wf_type)),
        );
    }
    if let Some(ref bid) = details.build_id {
        patch.insert("build_id".to_string(), Some(serde_json::json!(bid)));
    }
    patch
}

/// The inverse of [`nd_search_attrs_patch`]: every key that patch can stamp,
/// mapped to `None` so `store::update_search_attrs` deletes it. Applied when a
/// previously ND-blocked execution replays cleanly (rollback recovery), when a
/// terminal transition closes out a previously-blocked execution (belt-and-
/// braces reset), and when a reset-from-history fork is created from a
/// currently-blocked source (`reset.rs`, hence `pub(crate)`) — so no stale
/// divergence diagnostic survives on a healthy, terminal, or forked run.
pub(crate) fn nd_search_attrs_clear_patch()
-> std::collections::HashMap<String, Option<serde_json::Value>> {
    [
        "failure_cause",
        "event_index",
        "expected",
        "actual",
        "workflow_type",
        "build_id",
    ]
    .into_iter()
    .map(|k| (k.to_string(), None))
    .collect()
}

async fn update_workflow_execution_failed(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    worker_id: &str,
    error: &str,
    nd_details: Option<&crate::error::NonDeterministicDetails>,
) -> HarvestResult<()> {
    use crate::schema::harvest_workflow_executions::dsl;

    // Code-review fix (issue #603): see `update_workflow_execution_completed`
    // for the rationale -- read the pre-update block state so the stale-
    // diagnostic clear in the `None` arm below can be gated on it instead of
    // running unconditionally on every ordinary author-error failure.
    let was_nd_blocked = dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(dsl::nd_blocked_at.is_not_null())
        .first::<bool>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .unwrap_or(false);

    let updated = diesel::update(
        dsl::harvest_workflow_executions
            .find(exec_id.as_uuid())
            .filter(dsl::state.eq("RUNNING")),
    )
    .set((
        dsl::state.eq("FAILED"),
        dsl::output.eq(None::<serde_json::Value>),
        dsl::error.eq(Some(error.to_string())),
        dsl::sticky_worker_id.eq(Some(worker_id.to_string())),
        dsl::completed_at.eq(Some(chrono::Utc::now())),
        // Belt-and-braces ND-block reset (issue #603): an author failure (or
        // history-cap/dispatch-error terminal) closes the run — a stale block
        // marker must not survive on a terminal row.
        dsl::nd_blocked_at.eq(None::<chrono::DateTime<chrono::Utc>>),
        dsl::nd_block_reason.eq(None::<String>),
        dsl::nd_block_count.eq(0),
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(workflow_execution_transition_error(conn, exec_id).await?);
    }

    match nd_details {
        Some(details) => {
            crate::store::update_search_attrs(conn, exec_id, &nd_search_attrs_patch(details))
                .await?;
        }
        // Code-review fix (issue #603): the column reset above only clears
        // the nd_blocked_at/reason/count marker; when this failure carries
        // no fresh ND details (the common case post-gate — an author error,
        // or a history-cap/dispatch-error terminal), a *stale* diagnostic
        // from an earlier, now-resolved ND-block incident must also be
        // cleared from search_attrs, or the closed FAILED row keeps
        // displaying `failure_cause=non_determinism` for an unrelated
        // failure reason. Gated on `was_nd_blocked` (PR review fix): an
        // unconditional clear here would silently delete pre-existing user
        // search_attrs of the same name on rows created before these keys
        // became reserved.
        None if was_nd_blocked => {
            crate::store::update_search_attrs(conn, exec_id, &nd_search_attrs_clear_patch())
                .await?;
        }
        None => {}
    }

    Ok(())
}

/// Block an execution non-terminally on an engine-detected replay divergence
/// (issue #603).
///
/// Runs one row-locked transaction (mirroring the pause-guarded persistence
/// transaction in [`process_workflow_task`]) that:
/// 1. stamps `nd_blocked_at` / `nd_block_reason` and increments
///    `nd_block_count` on the execution row — `state` stays `RUNNING`;
/// 2. stamps the divergence diagnostic into `search_attrs`
///    ([`nd_search_attrs_patch`]);
/// 3. re-pends the workflow task with `scheduled_at = NOW() + backoff`
///    ([`queue::requeue_workflow_task_nd_blocked`]), unpinning sticky
///    affinity so a rolled-back worker can claim it.
///
/// It deliberately appends **zero** events (the divergent cycle's pending
/// commands were already discarded by the caller), never calls
/// `queue::fail_task`, and never runs the parent-close cascade, completion
/// triggers, or a parent wake — so root and child executions block uniformly
/// and a blocked child's parent simply stays suspended until the child
/// completes after the offending build is rolled back.
///
/// If an operator pause committed first (observed `PAUSED` under the row
/// lock), the task is re-parked instead — pause supersedes the block; the
/// next resume re-derives the same divergence (or replays cleanly under a
/// fixed build) on a fresh cycle.
#[allow(clippy::too_many_arguments)]
/// Check whether `exec_uuid` is currently `PAUSED` under a `FOR UPDATE` row
/// lock (serialising with `pause_workflow_execution`'s own lock), and if so,
/// re-park `task_id` under that same lock.
///
/// Shared by `process_workflow_task`'s own pause-guarded persistence
/// transaction and [`block_workflow_for_non_determinism`] (issue #603
/// code-review fix — both previously duplicated this identical shape).
///
/// Must be called from inside an open transaction on `conn`. Returns `true`
/// when the execution was `PAUSED` (the task has been re-parked and the
/// caller should discard its pending decision); `false` otherwise.
///
/// Discarding [`queue::park_workflow_task`]'s wake-requested return value is
/// safe at both call sites: each shares `pause_workflow_execution`'s `FOR
/// UPDATE` row lock, so a concurrent resume serialises after this check
/// commits and issues its own wake.
async fn check_paused_and_park(
    conn: &mut AsyncPgConnection,
    exec_uuid: uuid::Uuid,
    task_id: uuid::Uuid,
    worker_id: &str,
    sticky_timeout: Duration,
) -> HarvestResult<bool> {
    use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
    let locked_state: Option<String> = exec_dsl::harvest_workflow_executions
        .find(exec_uuid)
        .select(exec_dsl::state)
        .for_update()
        .first::<String>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    if locked_state.as_deref() != Some("PAUSED") {
        return Ok(false);
    }
    let sticky = if sticky_timeout.is_zero() {
        None
    } else {
        Some(queue::StickyHint::new(worker_id, sticky_timeout))
    };
    let _ = queue::park_workflow_task(conn, task_id, sticky).await?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn block_workflow_for_non_determinism(
    conn: &mut AsyncPgConnection,
    telemetry: &crate::telemetry::TelemetryConfig,
    task: &TaskQueueItem,
    execution: &WorkflowExecution,
    exec_id: ExecutionId,
    worker_id: &str,
    sticky_timeout: Duration,
    build_id: &str,
    error: &str,
    details: &crate::error::NonDeterministicDetails,
) -> HarvestResult<()> {
    let backoff = nd_block_backoff(execution.nd_block_count);
    let backoff_chrono = chrono::Duration::from_std(backoff).unwrap_or_default();
    let error_owned = error.to_string();
    let patch = nd_search_attrs_patch(details);
    let task_id = task.id;
    let exec_uuid = exec_id.as_uuid();

    let parked_paused = conn
        .transaction::<bool, HarvestError, _>(|conn| {
            let error_owned = error_owned.clone();
            let patch = patch.clone();
            async move {
                use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
                if check_paused_and_park(conn, exec_uuid, task_id, worker_id, sticky_timeout)
                    .await?
                {
                    return Ok(true);
                }

                let updated = diesel::update(
                    exec_dsl::harvest_workflow_executions
                        .find(exec_uuid)
                        .filter(exec_dsl::state.eq("RUNNING")),
                )
                .set((
                    exec_dsl::nd_blocked_at.eq(Some(chrono::Utc::now())),
                    exec_dsl::nd_block_reason.eq(Some(error_owned.clone())),
                    exec_dsl::nd_block_count.eq(exec_dsl::nd_block_count + 1),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
                if updated == 0 {
                    return Err(workflow_execution_transition_error(conn, exec_id).await?);
                }

                crate::store::update_search_attrs(conn, exec_id, &patch).await?;
                queue::requeue_workflow_task_nd_blocked(
                    conn,
                    task_id,
                    backoff_chrono,
                    &error_owned,
                )
                .await?;
                Ok(false)
            }
            .scope_boxed()
        })
        .await?;

    // Post-commit, best-effort telemetry. The #480 detection counter fires
    // unconditionally — a divergence WAS detected this cycle regardless of
    // what happened next — mirroring the pre-existing precedent in
    // `process_workflow_task`'s own terminal-telemetry block, which likewise
    // fires speculatively before its pause-guarded transaction even opens
    // (an accepted "best-effort, may over-count in a rare pause race"
    // pattern already established in this codebase).
    telemetry
        .metrics
        .record_workflow_non_determinism(&execution.workflow_name, build_id);

    if parked_paused {
        // The nd_blocked_at/reason/count columns were never actually
        // stamped in this branch (the transaction returned before that
        // UPDATE ran), so the #603 "entered the blocked state" counter must
        // NOT fire here — code-review fix: this used to return before any
        // telemetry at all, silently dropping the detection signal above too.
        tracing::warn!(
            execution_id = %exec_id,
            workflow = %execution.workflow_name,
            build_id,
            "harvest: replay non-determinism detected during a pause race; \
             execution re-parked, will re-evaluate on resume"
        );
        return Ok(());
    }

    telemetry
        .metrics
        .record_workflow_nondeterministic_block(&execution.workflow_name, &task.queue_name);
    tracing::warn!(
        execution_id = %exec_id,
        workflow = %execution.workflow_name,
        queue = %task.queue_name,
        build_id,
        block_count = execution.nd_block_count.saturating_add(1),
        backoff_secs = backoff.as_secs(),
        event_index = ?details.event_index,
        expected = ?details.expected,
        actual = ?details.actual,
        "harvest: replay non-determinism detected — execution blocked \
         non-terminally; roll back or fix the offending build to resume \
         (see docs/runbooks/nondeterminism-block.md)"
    );

    Ok(())
}

/// Clear the ND-block marker columns and search-attrs diagnostic on an
/// execution whose latest dispatch replayed cleanly (issue #603). Called
/// inside the same transaction that persists the recovered cycle's outcome so
/// recovery and marker-clearing are atomic.
async fn clear_nd_block(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> HarvestResult<()> {
    use crate::schema::harvest_workflow_executions::dsl;

    diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
        .set((
            dsl::nd_blocked_at.eq(None::<chrono::DateTime<chrono::Utc>>),
            dsl::nd_block_reason.eq(None::<String>),
            dsl::nd_block_count.eq(0),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    crate::store::update_search_attrs(conn, exec_id, &nd_search_attrs_clear_patch()).await?;
    Ok(())
}

fn resolve_workflow_concurrency(
    registry: &HandlerRegistry,
    workflow_name: &str,
    input: &serde_json::Value,
) -> (Option<String>, Option<u32>) {
    registry
        .workflows
        .get(workflow_name)
        .and_then(|info| info.concurrency.as_ref())
        .map_or((None, None), |policy| {
            let key = crate::concurrency::resolve_concurrency_key(policy.key_expr, input);
            (key, Some(policy.limit))
        })
}

#[allow(clippy::too_many_arguments)]
async fn persist_workflow_completion(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    output: serde_json::Value,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
) -> HarvestResult<(ExecutionId, Option<String>)> {
    let event = WorkflowEvent::WorkflowCompleted {
        output: output.clone(),
    };
    let (deferred, closed_children) = conn
        .transaction::<_, HarvestError, _>(|conn| {
            async move {
                store::append_events_offloaded(conn, exec_id, &[event], next_event_id, offloader)
                    .await?;
                update_workflow_execution_completed(conn, exec_id, worker_id, &output).await?;
                queue::complete_task(conn, task_id, output).await?;
                let (mut deferred, closed_children) =
                    apply_parent_close_cascade(conn, exec_id).await?;
                let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                    conn,
                    exec_id,
                    crate::completion_trigger::TerminalState::Completed,
                    metrics,
                )
                .await?;
                deferred.extend(triggers);
                Ok((deferred, closed_children))
            }
            .scope_boxed()
        })
        .await?;

    for start in deferred {
        start.spawn();
    }

    for (child_id, child_name) in closed_children {
        check_and_report_unfinished_handlers_for_worker(conn, child_id, Some(&child_name), metrics)
            .await;
    }

    Ok((exec_id, None))
}

async fn check_and_report_unfinished_handlers_for_worker(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    workflow_name: Option<&str>,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) {
    let name = match workflow_name {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => {
            if let Ok(exec) = crate::execution::load_execution(conn, exec_id).await {
                exec.workflow_name
            } else {
                String::new()
            }
        }
    };
    if !name.is_empty() {
        let check_res =
            crate::execution::check_and_report_unfinished_handlers(conn, exec_id, &name, metrics)
                .await;
        if let Err(e) = check_res {
            tracing::error!(execution_id = %exec_id, err = %e, "Failed to check and report unfinished handlers");
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn persist_workflow_failure(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    error: &str,
    nd_details: Option<&crate::error::NonDeterministicDetails>,
    execution: Option<&WorkflowExecution>,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
    // Concurrency key/limit from the current task so the retry inherits the same
    // per-key cap (issue #523 P2).
    concurrency_key: Option<String>,
    concurrency_limit: Option<u32>,
    // Priority from the current task so the retry inherits the same queue priority
    // and is not silently demoted behind normal work (issue #523 P2).
    priority: crate::types::Priority,
) -> HarvestResult<(bool, (ExecutionId, Option<String>))> {
    let error = error.to_string();
    // Decode the typed failure envelope once (issue #767). A legacy `Err(String)`
    // — and an engine non-determinism string (`nd_details.is_some()`) — decodes to
    // all-None typed fields with `message == error`, preserving legacy semantics.
    // `decoded.message` is the HUMAN message and is what the `execution.error` TEXT
    // column and the task reason must carry (never the envelope JSON, AC4); the
    // `WorkflowFailed` event carries the full typed fields.
    let decoded = crate::failure::decode_workflow_failure(&error);

    // Pre-compute the retry plan (pure, no DB) before entering the transaction.
    let retry_plan: Option<(ExecutionId, RetryPolicy, u32, std::time::Duration)> =
        // Issue #782: a contained handler-panic terminal must NOT also spawn a
        // #523 fresh-execution retry — the panic-retry budget WAS the retry.
        // Engine-reserved guard (independent of operator config): skip the retry
        // plan when the decoded error type is HandlerPanic. Belt-and-braces
        // alongside the `nd_details.is_none()` gate.
        if nd_details.is_none()
            && decoded.error_type.as_deref() != Some(crate::failure::ERROR_TYPE_HANDLER_PANIC)
        {
            execution.and_then(|exec| {
                let policy: RetryPolicy = exec
                    .workflow_retry_policy
                    .as_ref()
                    .and_then(|v| serde_json::from_value(v.clone()).ok())?;
                let attempt = exec.workflow_attempt.cast_unsigned();
                if attempt >= policy.max_attempts {
                    return None;
                }
                // The retry policy's `non_retryable_errors` class list controls the
                // #523 workflow-level retry loop, and a TYPED workflow failure
                // (issue #767) is classified by its `error_type` CLASS ONLY — never
                // by its human message text. This lets an operator halt the retry
                // loop for a specific typed failure class (e.g.
                // `"ValidationRejected"`) exactly like a typed activity failure,
                // without a retryable typed class being wrongly made terminal just
                // because its human message happens to coincide with a
                // `non_retryable_errors` pattern (Codex P2). For a typed failure we
                // therefore pass an empty raw string to `is_non_retryable`: it
                // matches on exact equality (`nr == raw_error`), so `""` can only
                // ever match a literally-empty pattern (degenerate config) and never
                // the message — the match reduces to a pure class check.
                //
                // `decoded.error_type` is `None` for a legacy `Err(String)` (and for
                // an engine non-determinism string), so those fall back to the
                // full-string match on the decoded HUMAN message (`decoded.message`)
                // — never the raw `harvest_workflow_failure_v1` envelope JSON. This
                // preserves legacy `non_retryable_errors` semantics (a workflow
                // returning `Err("fatal".into())` with `non_retryable_errors =
                // ["fatal"]` still halts, because the decoded message equals the raw
                // error for the legacy path).
                //
                // The `WorkflowFailure.non_retryable` FLAG itself stays advisory-only
                // and is deliberately NOT consulted here — it is a classification
                // hint for the caller / completion-trigger, not a control input to
                // the retry loop.
                let non_retryable = match decoded.error_type.as_deref() {
                    Some(error_type) => policy.is_non_retryable(Some(error_type), ""),
                    None => policy.is_non_retryable(None, &decoded.message),
                };
                if non_retryable {
                    return None;
                }
                // Use the execution ID bytes as a deterministic seed so jitter is
                // consistent for this chain and avoids thundering-herd retry storms.
                let seed = u64::from_le_bytes(
                    exec_id.as_uuid().as_bytes()[..8]
                        .try_into()
                        .unwrap_or([0u8; 8]),
                );
                let delay = crate::policy::compute_retry_delay_with_seed(&policy, attempt, seed);
                let retry_exec_id = ExecutionId::new_for_shard(exec_id.shard());
                Some((retry_exec_id, policy, attempt, delay))
            })
        } else {
            None
        };

    // Pre-compute fire_at once for the WorkflowRetryScheduled event (observability).
    // The retry start itself uses the *relative* delay, not this absolute timestamp:
    // an absolute `start_at` computed here can fall into the past before the nested
    // start validates it (millisecond-scale intervals or a slow failure transaction),
    // and the start path rejects a past `start_at` — which would permanently fail a
    // workflow that still has attempts remaining. A relative delay is validated as a
    // duration (never against `now`), so it can never be "in the past".
    #[allow(clippy::type_complexity)]
    let retry_fire_info: Option<(
        ExecutionId,
        RetryPolicy,
        u32,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::Duration>,
    )> = retry_plan.as_ref().map(|(rid, policy, attempt, delay)| {
        let delay_chrono = chrono::Duration::from_std(*delay).unwrap_or_default();
        let fire_at = chrono::Utc::now() + delay_chrono;
        let start_delay = (!delay.is_zero()).then_some(delay_chrono);
        (*rid, policy.clone(), *attempt, fire_at, start_delay)
    });

    let (deferred, retry_scheduled, deferred_checks) = conn
        .transaction::<(
            Vec<crate::completion_trigger::DeferredTriggerStart>,
            bool,
            Vec<(ExecutionId, String)>,
        ), HarvestError, _>(|conn| {
            let decoded = decoded.clone();
            let retry_fire_info = retry_fire_info.clone();
            let exec_ref = execution;
            async move {
                store::append_events(
                    conn,
                    exec_id,
                    &[WorkflowEvent::workflow_failed_typed(&decoded)],
                    next_event_id,
                )
                .await?;
                update_workflow_execution_failed(
                    conn,
                    exec_id,
                    worker_id,
                    &decoded.message,
                    nd_details,
                )
                .await?;
                queue::fail_task(conn, task_id, &decoded.message).await?;
                let mut deferred: Vec<crate::completion_trigger::DeferredTriggerStart> = Vec::new();
                let mut deferred_checks: Vec<(ExecutionId, String)> = Vec::new();

                let mut retry_committed = false;
                if let (Some(exec_ref), Some((rid, policy, attempt, fire_at, start_delay))) =
                    (exec_ref, retry_fire_info)
                    && attempt < policy.max_attempts
                {
                    let retry_workflow_id = rid.to_string();
                    let retry_params = crate::execution::StartWorkflowParams {
                        workflow_name: &exec_ref.workflow_name,
                        workflow_id: &retry_workflow_id,
                        exec_id: rid,
                        input: exec_ref.input.clone(),
                        parent_id: None,
                        queue_name: &exec_ref.queue_name,
                        execution_timeout: exec_ref.execution_timeout,
                        memo: exec_ref.memo.clone(),
                        search_attrs: exec_ref.search_attrs.clone(),
                        reuse_policy: crate::types::WorkflowIdReusePolicy::AllowDuplicate,
                        conflict_policy: crate::types::WorkflowIdConflictPolicy::Unspecified,
                        trace_context: None,
                        max_execution_timeout_ceiling: None,
                        concurrency_key: concurrency_key.clone(),
                        concurrency_limit,
                        priority,
                        max_workflow_input_bytes: 0,
                        start_at: None,
                        delay: start_delay,
                        max_workflow_start_delay: None,
                        owner: exec_ref.owner.as_deref(),
                        runbook_url: exec_ref.runbook_url.as_deref(),
                        severity: exec_ref.severity.as_deref(),
                        context_headers: exec_ref
                            .context_headers
                            .clone()
                            .and_then(|v| serde_json::from_value(v).ok()),
                        sla: exec_ref.sla,
                        schedule_id: exec_ref.schedule_id,
                        scheduled_for: exec_ref.scheduled_for,
                        workflow_attempt: attempt + 1,
                        workflow_retry_policy: Some(policy),
                        retry_of_exec_id: Some(exec_id.as_uuid()),
                        max_workflow_attempts_ceiling: None,
                        origin: exec_ref.origin.as_deref(),
                        // Workflow-level retry (issue #523) is the same
                        // logical run trying again — inherit the
                        // predecessor's completion-callback targets (#605)
                        // rather than silently dropping them.
                        completion_callbacks: exec_ref.completion_callbacks.clone(),
                        // Workflow-level retry (issue #523/#740) is the same
                        // logical run trying again — inherit the predecessor's
                        // start provenance rather than re-attributing it as a
                        // fresh `api` start.
                        start_source: crate::types::StartSource::from_str(
                            exec_ref.start_source.as_deref().unwrap_or("unknown"),
                        ),
                        start_source_ref: exec_ref.start_source_ref.as_deref(),
                        started_by: exec_ref.started_by.as_deref(),
                    };

                    match crate::execution::start_or_load_workflow_execution_collect(
                        conn,
                        retry_params,
                        true,
                        false,
                        metrics,
                        // Workflow-level retry (#523) is in-flight continuation of an
                        // existing logical run, not a fresh admission — never gated.
                        None,
                    )
                    .await
                    {
                        Ok((_started, retry_deferred, checks, _cancel_metrics)) => {
                            deferred.extend(retry_deferred);
                            deferred_checks.extend(checks);
                            store::append_single_event(
                                conn,
                                exec_id,
                                WorkflowEvent::WorkflowRetryScheduled {
                                    retry_exec_id: rid,
                                    attempt: attempt + 1,
                                    fire_at,
                                },
                            )
                            .await?;
                            retry_committed = true;
                        }
                        Err(e) => {
                            tracing::warn!(
                                execution_id = %exec_id,
                                error = %e,
                                "harvest: failed to start retry execution; not retrying"
                            );
                        }
                    }
                }

                if !retry_committed {
                    let (cascade, closed_children) =
                        apply_parent_close_cascade(conn, exec_id).await?;
                    deferred.extend(cascade);
                    deferred_checks.extend(closed_children);
                    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                        conn,
                        exec_id,
                        crate::completion_trigger::TerminalState::Failed,
                        metrics,
                    )
                    .await?;
                    deferred.extend(triggers);
                }

                Ok((deferred, retry_committed, deferred_checks))
            }
            .scope_boxed()
        })
        .await?;

    if retry_scheduled
        && let (Some(exec), Some((_, _, attempt, _, _))) = (execution, &retry_fire_info)
    {
        if let Some(m) = metrics {
            m.record_workflow_retry(&exec.workflow_name, &exec.queue_name);
        }
        let delay_secs = retry_plan.as_ref().map_or(0, |(_, _, _, d)| d.as_secs());
        tracing::info!(
            execution_id = %exec_id,
            attempt = attempt + 1,
            delay_secs,
            "harvest: workflow retry scheduled"
        );
    }

    for check in deferred_checks {
        let _ = check_and_report_unfinished_handlers(conn, check.0, &check.1, metrics).await;
    }

    for start in deferred {
        start.spawn();
    }

    let workflow_name = execution.map(|exec| exec.workflow_name.clone());
    Ok((retry_scheduled, (exec_id, workflow_name)))
}

/// Append `UpdateCompleted` or `UpdateFailed` events for each
/// `RecordUpdateResult` command in `commands`, in order.
///
/// Used to durably record in-flight update results before the terminal workflow
/// event (`WorkflowCompleted`, `WorkflowFailed`, or a suspension side-effect).
/// `next_event_id` is advanced by the number of events written.
async fn persist_update_result_commands(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    commands: &[WorkflowCommand],
    next_event_id: &mut i32,
) -> HarvestResult<()> {
    let events: Vec<WorkflowEvent> = commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::RecordUpdateResult { update_id, result } => Some(match result {
                Ok(output) => WorkflowEvent::UpdateCompleted {
                    update_id: *update_id,
                    output: output.clone(),
                },
                Err(error) => WorkflowEvent::UpdateFailed {
                    update_id: *update_id,
                    error: error.clone(),
                },
            }),
            _ => None,
        })
        .collect();

    if events.is_empty() {
        return Ok(());
    }

    let advanced_event_id = next_event_id
        .checked_add(i32::try_from(events.len()).unwrap_or(i32::MAX))
        .ok_or_else(|| crate::error::HarvestError::Database("Event ID overflow".to_string()))?;

    store::append_events(conn, exec_id, &events, *next_event_id).await?;
    *next_event_id = advanced_event_id;
    Ok(())
}

/// Collect `(update_name, completed)` pairs for every `RecordUpdateResult`
/// command in `pending_cmds`, resolving the update name from the
/// `UpdateAdmitted` events in `history_events` (issue #684).
///
/// Pure (no DB), unit-tested. `completed` is `true` for an `Ok` result
/// (→ `harvest.update.completed`) and `false` for an `Err` result
/// (→ `harvest.update.failed`). The pairs are collected **before** the persist
/// transaction (which moves `pending_cmds`) but emitted only in the worker's
/// `Persisted` arm, so a persist failure never over-counts. `RecordUpdateResult`
/// is produced once on live execution (replay short-circuits in
/// `execute_admitted_update`), so each update result is counted exactly once.
/// An update whose name cannot be resolved from history (should not happen —
/// admission always precedes the result) is labeled `"unknown"` rather than
/// dropped, keeping the label bounded.
///
/// The `name` label is bounded to the actual handler set (issue #684, Codex P2)
/// using the **result** as the discriminator, not the declarative registry: the
/// raw route `POST /workflows/{id}/update/{name}` can admit an unregistered
/// name, which then fails the workflow's handler lookup with the exact
/// `"update handler '<name>' not found"` error (see
/// [`crate::context::WorkflowContext::execute_admitted_update`]). Only that case
/// is bucketed to the
/// [`UNREGISTERED_UPDATE_NAME`](crate::telemetry::UNREGISTERED_UPDATE_NAME)
/// sentinel; every real handler — whether registered declaratively via
/// `#[update]` **or** imperatively via `ctx.register_update_handler` — keeps its
/// name. Bucketing against `registry.update_handlers` would be wrong here: it is
/// declarative-only and does not capture imperatively-registered handlers (the
/// common pattern), so it would mislabel legitimate updates as unregistered.
fn collect_update_result_metrics(
    history_events: &[WorkflowEvent],
    pending_cmds: &[WorkflowCommand],
) -> Vec<(String, bool, Option<chrono::DateTime<chrono::Utc>>)> {
    // Fast path: nothing to resolve or emit.
    if !pending_cmds
        .iter()
        .any(|c| matches!(c, WorkflowCommand::RecordUpdateResult { .. }))
    {
        return Vec::new();
    }

    // Resolve both the update name AND its admit timestamp (issue #781) from the
    // `UpdateAdmitted` events in history. The timestamp is the admit→terminal
    // latency histogram's start point; a `RecordUpdateResult` whose admit is not
    // in the loaded history (should not happen — admission always precedes the
    // result) yields name "unknown" AND `None` timestamp together, so its
    // histogram sample is skipped rather than fabricated.
    let mut admitted: std::collections::HashMap<
        crate::types::UpdateId,
        (&str, chrono::DateTime<chrono::Utc>),
    > = std::collections::HashMap::new();
    for event in history_events {
        if let WorkflowEvent::UpdateAdmitted {
            update_id,
            name,
            timestamp,
            ..
        } = event
        {
            admitted.insert(*update_id, (name.as_str(), *timestamp));
        }
    }

    pending_cmds
        .iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::RecordUpdateResult { update_id, result } => {
                let resolved = admitted.get(update_id).copied();
                let name = resolved.map_or("unknown", |(name, _)| name);
                let admit_ts = resolved.map(|(_, ts)| ts);
                let label = if is_unregistered_update_failure(name, result) {
                    crate::telemetry::UNREGISTERED_UPDATE_NAME
                } else {
                    name
                };
                Some((label.to_owned(), result.is_ok(), admit_ts))
            }
            _ => None,
        })
        .collect()
}

/// Admit→terminal latency in seconds for the `harvest.update.duration` histogram
/// (issue #781).
///
/// Pure and unit-tested. `admit_ts` is the recorded `UpdateAdmitted.timestamp`
/// (or `None` when the admit could not be resolved from history, in which case
/// the histogram sample is **skipped** — `None` is returned so the caller emits
/// nothing). `now` is the terminal time (`Utc::now()` at the post-commit emit).
/// A negative delta (clock skew across event appends) clamps to `0.0`, never a
/// garbage/negative sample.
#[allow(clippy::cast_precision_loss)] // realistic admit→terminal latencies (ms) are far within f64's exact range
fn update_admit_duration_secs(
    admit_ts: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<f64> {
    let admit = admit_ts?;
    let millis = (now - admit).num_milliseconds().max(0);
    Some(millis as f64 / 1000.0)
}

/// Whether a `RecordUpdateResult` is the "update handler not found" failure a
/// genuinely-unregistered update name produces (issue #684, Codex P2).
///
/// The exact error is `format!("update handler '{name}' not found")`, minted by
/// [`crate::context::WorkflowContext::execute_admitted_update`] when no handler
/// is registered for the admitted name. Matching the exact string (including the
/// name) keeps a real handler that happens to return a similar message from
/// being mislabeled.
fn is_unregistered_update_failure(name: &str, result: &Result<serde_json::Value, String>) -> bool {
    matches!(result, Err(error) if *error == format!("update handler '{name}' not found"))
}

/// Select the command list carrying this cycle's `RecordUpdateResult`s given
/// the workflow outcome (issue #684).
///
/// On a [`WorkflowOutcome::Suspended`] outcome the update results live **inside
/// the outcome's `commands`** — the executor returns `(Suspended { commands },
/// vec![])`, so the worker's second tuple element (`pending_cmds`) is empty.
/// On every other outcome (terminal-with-commands, continue-as-new) the update
/// results ride in `pending_cmds`. Reading only `pending_cmds` (as an earlier
/// cut did) silently undercounts the **common** case: an admitted update
/// completes and the long-running workflow loops back to a `wait_for_signal` /
/// activity and suspends. The persist path is the same single main transaction
/// in both cases (`handle_suspended_workflow` for suspend,
/// `persist_terminal_outcome_commands` for terminal), so the `Persisted`-arm
/// emission is post-commit and exactly-once for both once the source is right.
fn update_result_command_source<'a>(
    outcome: &'a WorkflowOutcome,
    pending_cmds: &'a [WorkflowCommand],
) -> &'a [WorkflowCommand] {
    match outcome {
        WorkflowOutcome::Suspended { commands } => commands,
        _ => pending_cmds,
    }
}

/// Emit `harvest.update.completed` / `harvest.update.failed` (issue #684) AND the
/// `harvest.update.duration` admit→terminal latency histogram (issue #781) for
/// each collected `(update_name, completed, admit_ts)` triple. Shared by the
/// worker's `Persisted`-arm terminal/suspend emission and the two inline
/// external-signal suspension branches, which persist their update results
/// outside the main transaction and would otherwise leave them uncounted.
///
/// The `update_name` is already bounded by `collect_update_result_metrics` (an
/// unregistered name's handler-not-found failure is bucketed to the
/// [`UNREGISTERED_UPDATE_NAME`](crate::telemetry::UNREGISTERED_UPDATE_NAME)
/// sentinel, issue #684 Codex P2), so this helper emits the name verbatim.
///
/// The histogram (issue #781) records `now - admit_ts` in seconds, capturing a
/// single `now` at the top so all results in one cycle share a terminal time.
/// A result whose admit could not be resolved from history (`admit_ts == None`)
/// records the counter but **skips** the histogram sample — never a fabricated
/// `0`. This piggybacks on the completed/failed counters' exact post-commit
/// path, so the histogram shares their delivery semantics.
fn emit_update_result_metrics(
    metrics: &dyn crate::telemetry::MetricsRecorder,
    workflow_name: &str,
    queue: &str,
    results: &[(String, bool, Option<chrono::DateTime<chrono::Utc>>)],
) {
    let now = chrono::Utc::now();
    for (update_name, completed, admit_ts) in results {
        let outcome = if *completed {
            metrics.record_update_completed(workflow_name, update_name, queue);
            "completed"
        } else {
            metrics.record_update_failed(workflow_name, update_name, queue);
            "failed"
        };
        if let Some(duration_secs) = update_admit_duration_secs(*admit_ts, now) {
            metrics.record_update_duration(
                workflow_name,
                update_name,
                queue,
                outcome,
                duration_secs,
            );
        }
    }
}

/// Extract the terminal outcome's `unhandled_signals` map for post-commit
/// emission (issue #684, Codex P2).
///
/// Populated only on [`WorkflowOutcome::Completed`] / [`WorkflowOutcome::Failed`];
/// empty for `Suspended` (not terminal) and `ContinuedAsNew` (carries no map).
/// The caller collects this **before** the persist transaction moves `outcome`
/// and emits it only in the `Persisted` arm, so `harvest.signal.unhandled`
/// counts durable terminal outcomes exactly once — a `ParkedPaused` discard or
/// a persist failure returns before the emit and never over-counts.
fn outcome_unhandled_signals(outcome: &WorkflowOutcome) -> std::collections::BTreeMap<String, u64> {
    match outcome {
        WorkflowOutcome::Completed {
            unhandled_signals, ..
        }
        | WorkflowOutcome::Failed {
            unhandled_signals, ..
        } => unhandled_signals.clone(),
        _ => std::collections::BTreeMap::new(),
    }
}

/// Emit `harvest.signal.unhandled` once per unconsumed occurrence from a
/// terminal outcome's `unhandled_signals` map (issue #684).
///
/// The map is grouped by signal name for the executor's bookkeeping, but the
/// signal `name` is NOT a metric label (issue #684, Codex P2 — free-form send
/// route, no declared registry to bound it). This sums the per-name counts and
/// emits one increment per unconsumed occurrence against the single
/// `(workflow, queue)` series, so the counter still reflects the terminal
/// outcome's total unconsumed-signal volume without the unbounded name
/// dimension.
///
/// Called only from the worker's post-commit `Persisted` arm (the same
/// discipline as `harvest.update.completed/failed`), so the counter represents
/// DURABLE terminal outcomes only: this arm is reached only after the persist
/// transaction commits, downstream of the #603 ND-block gate (`Failed{nd:Some}`
/// early-returns before persist) and `check_paused_and_park` (a claimed-then-
/// paused race returns via `ParkedPaused`, never here). Cancel/Terminate/
/// Execution-timeout/Parent-close never carry a driven matcher (and so never a
/// populated map). The map is empty for every non-terminal path, making this a
/// no-op there.
fn emit_unhandled_signal_metrics(
    metrics: &dyn crate::telemetry::MetricsRecorder,
    workflow_name: &str,
    queue: &str,
    by_name: &std::collections::BTreeMap<String, u64>,
) {
    let total: u64 = by_name.values().sum();
    for _ in 0..total {
        metrics.record_signal_unhandled(workflow_name, queue);
    }
}

/// Apply `UpsertSearchAttributes` patches from `commands` to `base` in memory.
///
/// Returns the patched value, or the original `base` if no patch commands exist.
fn apply_search_attrs_patch_in_memory(
    base: Option<serde_json::Value>,
    commands: &[WorkflowCommand],
) -> Option<serde_json::Value> {
    // ⚡ Bolt: Check for patches first to avoid unnecessary allocations
    let has_patches = commands
        .iter()
        .any(|cmd| matches!(cmd, WorkflowCommand::UpsertSearchAttributes { .. }));
    if !has_patches {
        return base;
    }

    // ⚡ Bolt: Apply patches directly to the JSON object instead of building an intermediate HashMap
    let mut obj = base
        .and_then(|v| {
            if let serde_json::Value::Object(m) = v {
                Some(m)
            } else {
                None
            }
        })
        .unwrap_or_default();

    for cmd in commands {
        if let WorkflowCommand::UpsertSearchAttributes { patch } = cmd {
            for (k, v) in patch {
                match v {
                    Some(val) => {
                        obj.insert(k.clone(), val.clone());
                    }
                    None => {
                        obj.remove(k);
                    }
                }
            }
        }
    }

    if obj.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(obj))
    }
}

/// In-memory equivalent of [`crate::store::update_search_attrs`]'s merge
/// semantics, for a raw key/value patch (as opposed to
/// [`apply_search_attrs_patch_in_memory`]'s `WorkflowCommand`-sourced
/// patches). `Some(v)` merges `v` in; `None` removes the key.
///
/// Unlike [`apply_search_attrs_patch_in_memory`], an empty result is **not**
/// collapsed to `None` — it stays `Some(Value::Object({}))` — matching
/// `store::update_search_attrs`'s own DB-write behavior exactly (it always
/// writes `Some(new_attrs)`, never clears the column to SQL `NULL`), so a
/// caller keeping an in-memory `WorkflowExecution` snapshot consistent with a
/// same-transaction `store::update_search_attrs` call sees byte-identical
/// `search_attrs` (issue #603 fix: kept the two callers' snapshots from
/// silently reintroducing cleared ND diagnostic keys, e.g. into a
/// continue-as-new successor built from the stale pre-clear reference).
pub(crate) fn apply_raw_search_attrs_patch_in_memory(
    base: Option<serde_json::Value>,
    patch: &std::collections::HashMap<String, Option<serde_json::Value>>,
) -> Option<serde_json::Value> {
    if patch.is_empty() {
        return base;
    }
    let mut merged = match base {
        Some(serde_json::Value::Object(m)) => m,
        _ => serde_json::Map::new(),
    };
    for (key, value) in patch {
        match value {
            Some(v) => {
                merged.insert(key.clone(), v.clone());
            }
            None => {
                merged.remove(key.as_str());
            }
        }
    }
    Some(serde_json::Value::Object(merged))
}

/// Apply `UpsertSearchAttributes` commands from a command list to the DB.
///
/// Multiple `UpsertSearchAttributes` commands are merged left-to-right before
/// the single DB update so the final result is one round-trip regardless of
/// how many `upsert_search_attrs` calls the workflow made in this cycle.
async fn persist_search_attrs_from_commands(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    commands: &[WorkflowCommand],
) -> HarvestResult<()> {
    // ⚡ Bolt: Lazily allocate the merged map only if there are patch commands
    let mut merged: Option<std::collections::HashMap<String, Option<serde_json::Value>>> = None;

    for cmd in commands {
        if let WorkflowCommand::UpsertSearchAttributes { patch } = cmd {
            let m = merged.get_or_insert_with(std::collections::HashMap::new);
            for (k, v) in patch {
                m.insert(k.clone(), v.clone());
            }
        }
    }

    let merged = match merged {
        Some(m) if !m.is_empty() => m,
        _ => return Ok(()),
    };

    store::update_search_attrs(conn, exec_id, &merged).await
}

/// The effective DB write resolved from the last `SetCurrentDetails` command
/// in a cycle, if any (issue #593). A 3-way enum rather than
/// `Option<Option<&str>>` (`clippy::option_option`) -- all three states are
/// meaningfully distinct outcomes, not a nested-optionality artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CurrentDetailsUpdate<'a> {
    /// No `SetCurrentDetails` command was present this cycle, or the last
    /// one's value was truncated down to empty without an explicit clear --
    /// skip the write and preserve whatever is already stored.
    NoOp,
    /// The last command was an author-issued explicit clear -- clear the
    /// column to `NULL`.
    Clear,
    /// The last command sets the column to this value.
    Set(&'a str),
}

/// Determines the last `SetCurrentDetails` command's effective DB write, if
/// any (issue #593). Pure, no-DB decision function factored out of
/// [`persist_current_details_from_commands`] so the last-write-wins /
/// empty-clears contract is directly unit-testable.
///
/// The clear decision is read from the command's `explicit_clear` flag --
/// set by the context from the caller's *pre-truncation* input -- rather than
/// from `value.is_empty()`. A non-empty input can truncate down to an empty
/// string when `current_details_cap` is `0` (or smaller than the input's
/// first UTF-8 character); that is a capacity artifact, not an author-issued
/// clear, so it resolves to `NoOp` (preserve whatever is already stored)
/// instead of erasing an existing breadcrumb (post-review hardening, PR #894).
fn latest_current_details_update(commands: &[WorkflowCommand]) -> CurrentDetailsUpdate<'_> {
    let Some((value, explicit_clear)) = commands.iter().rev().find_map(|cmd| {
        if let WorkflowCommand::SetCurrentDetails {
            value,
            explicit_clear,
        } = cmd
        {
            Some((value.as_str(), *explicit_clear))
        } else {
            None
        }
    }) else {
        return CurrentDetailsUpdate::NoOp;
    };
    if explicit_clear {
        CurrentDetailsUpdate::Clear
    } else if value.is_empty() {
        CurrentDetailsUpdate::NoOp
    } else {
        CurrentDetailsUpdate::Set(value)
    }
}

/// Persist the last `SetCurrentDetails` command to the execution row (issue #473).
///
/// Scans `commands` for [`WorkflowCommand::SetCurrentDetails`] variants and
/// writes the **last** value (last-write-wins) to
/// `harvest_workflow_executions.current_details` in a single `UPDATE`. An
/// explicit (author-issued) empty-string call clears the column to `NULL`
/// (issue #593). Does nothing when no `SetCurrentDetails` command is present,
/// or when the last command's value was truncated down to empty by an
/// extreme cap rather than explicitly cleared (see
/// [`latest_current_details_update`]).
async fn persist_current_details_from_commands(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    commands: &[WorkflowCommand],
) -> HarvestResult<()> {
    match latest_current_details_update(commands) {
        CurrentDetailsUpdate::NoOp => {}
        CurrentDetailsUpdate::Clear => {
            store::update_current_details(conn, exec_id, None).await?;
        }
        CurrentDetailsUpdate::Set(details) => {
            store::update_current_details(conn, exec_id, Some(details)).await?;
        }
    }
    Ok(())
}

/// Fire a best-effort `pg_notify` for each [`WorkflowCommand::PublishProgress`]
/// command in this decision cycle (issue #791).
///
/// Progress chunks are an **ephemeral** live-output side channel: they are never
/// recorded to `harvest_events` and never replayed. Fired on the worker's
/// persist connection alongside [`persist_current_details_from_commands`], so a
/// rolled-back cycle's `NOTIFY` is discarded (pg delivers only on commit) and
/// the retried cycle re-fires live — a committed chunk is never double-delivered.
///
/// One `pg_notify` per chunk (O(chunks) per cycle) preserves each chunk's
/// distinct `seq` and payload. The context suppresses `publish_progress` during
/// replay, so this only ever sees live-frontier chunks.
///
/// **Best-effort**: a notify failure is logged and swallowed, never failing the
/// workflow — a progress chunk is disposable. The context caps each chunk below
/// the Postgres 8000-byte `NOTIFY` limit, so a size-driven failure cannot occur.
///
/// **Per-cycle ceiling**: each chunk is one `SELECT pg_notify(...)` round-trip
/// in the persist transaction, so a pathological publish loop (thousands of
/// chunks in one decision cycle) would fire thousands of serial round-trips and
/// burst into Postgres' shared async NOTIFY queue. Chunks beyond
/// [`PROGRESS_MAX_CHUNKS_PER_CYCLE`] are DROPPED best-effort for that cycle with
/// a single aggregated `tracing::warn!` (not one per dropped chunk).
async fn notify_progress_from_commands(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    commands: &[WorkflowCommand],
) {
    let mut fired = 0usize;
    let mut dropped_over_cap = 0usize;
    for cmd in commands {
        if let WorkflowCommand::PublishProgress { seq, chunk } = cmd {
            if fired >= PROGRESS_MAX_CHUNKS_PER_CYCLE {
                dropped_over_cap += 1;
                continue;
            }
            fired += 1;
            if let Err(e) =
                crate::notify::notify_workflow_progress(conn, exec_id.as_uuid(), *seq, chunk).await
            {
                tracing::warn!(
                    workflow_exec_id = %exec_id.as_uuid(),
                    seq = *seq,
                    error = %e,
                    "failed to publish progress chunk (best-effort, dropped)"
                );
            }
        }
    }
    if dropped_over_cap > 0 {
        tracing::warn!(
            workflow_exec_id = %exec_id.as_uuid(),
            dropped = dropped_over_cap,
            cap = PROGRESS_MAX_CHUNKS_PER_CYCLE,
            "progress chunks exceeded per-cycle ceiling; excess dropped (best-effort)"
        );
    }
}

/// Per-decision-cycle ceiling on the number of `ctx.publish_progress` chunks the
/// worker forwards as `pg_notify` round-trips (issue #791). Each chunk is one
/// serial `SELECT pg_notify(...)` in the persist transaction; this bounds a
/// pathological publish loop's round-trips and its burst into the shared
/// Postgres async NOTIFY queue. Excess chunks in a single cycle are dropped
/// best-effort (progress is disposable).
const PROGRESS_MAX_CHUNKS_PER_CYCLE: usize = 10_000;

async fn persist_signal_wait_park(
    conn: &mut AsyncPgConnection,
    detached_spawns: DetachedSpawnPersistence<'_>,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    commands: &[WorkflowCommand],
    sticky: Option<queue::StickyHint<'_>>,
) -> HarvestResult<()> {
    let registry = detached_spawns.registry;
    // Park the workflow task (state=RUNNING, worker cleared) so it is not
    // confused with a timer-waiting task (state=PENDING). This ensures that
    // `wake_workflow_task` — which only targets RUNNING/parked rows — can
    // reliably distinguish signal waits from timer waits and will not
    // prematurely fire a pending timer when a signal is delivered.
    //
    // `had_wake_requested` closes a race the post-park checks below cannot
    // cover (PR #901 review): those checks re-load pending signals and
    // external signal/cancel terminals, but an *update* admitted via
    // `execute_update_in_process` (which appends `UpdateAdmitted` and calls
    // `wake_workflow_task` as two separate, non-transactional steps -- no
    // shared lock with this park) has no equivalent post-park re-check. A
    // wake landing in the gap between this transaction's park and its commit
    // would otherwise only be captured as `wake_requested = TRUE` and then
    // silently discarded here.
    let (deferred, had_wake_requested) = conn
        .transaction::<_, HarvestError, _>(|conn| {
            async move {
                // Cancellable/renewable timer bookkeeping (issue #768): resolve
                // the ArmTimer/CancelTimer row mutations FIRST, then interleave
                // their TimerStarted/TimerCancelled events into the suspension
                // batch at their command-emission positions (armed timers are
                // observed on the next wake, so the returned deadline is unused
                // here).
                let (mut timer_events, _min_fires_at) =
                    plan_timer_lifecycle(conn, exec_id, commands).await?;
                let marker_events =
                    pre_suspension_events_from_commands(commands, &mut timer_events);
                let events_len = i32::try_from(marker_events.len()).unwrap_or(i32::MAX);
                store::append_events(conn, exec_id, &marker_events, next_event_id).await?;
                detached_spawns.persist(conn, commands).await?;
                let mut race_next_event_id = next_event_id.saturating_add(events_len);
                let deferred = apply_race_loser_cancellations(
                    conn,
                    exec_id,
                    commands,
                    &mut race_next_event_id,
                    registry,
                )
                .await?;
                let had_wake_requested = queue::park_workflow_task(conn, task_id, sticky).await?;
                Ok((deferred, had_wake_requested))
            }
            .scope_boxed()
        })
        .await?;
    for start in deferred {
        start.spawn();
    }

    if had_wake_requested {
        queue::wake_workflow_task(conn, exec_id).await?;
        return Ok(());
    }

    // A signal may have arrived while this task was actively running (before the
    // park above).  `send_signal` would have called `wake_workflow_task` at that
    // point but found no parked task to wake.  Re-check now that we are parked
    // and self-wake if any unconsumed signals are waiting.
    //
    // Safety: if a new signal arrives *after* this check returns empty, its
    // `send_signal` caller will call `wake_workflow_task` and find this
    // RUNNING/parked task — so the wake is guaranteed regardless of timing.
    let pending = signal::load_pending_signals(conn, exec_id).await?;
    if !pending.is_empty() {
        queue::wake_workflow_task(conn, exec_id).await?;
        return Ok(());
    }

    // An external signal/cancel wait may have been resolved by the outbox
    // (External{Signal,Cancel}Delivered/Failed appended on another connection)
    // in the gap between the `*Requested` event committing and this park
    // committing. `wake_workflow_task` is a no-op when it fires before the task
    // is parked, so a cross-shard / NotFound caller could otherwise stay parked
    // until an unrelated wake. Re-check now that we are parked: if any in-flight
    // external request this wait depends on already has a terminal in history,
    // self-wake so the workflow re-runs and observes it (issue #492).
    let waited_signal_ids: Vec<crate::types::ExternalSignalId> = commands
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::SignalExternalWorkflow { signal_id, .. } => Some(*signal_id),
            _ => None,
        })
        .collect();
    let waited_cancel_ids: Vec<crate::types::ExternalCancelId> = commands
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::RequestCancelExternalWorkflow { cancel_id, .. } => Some(*cancel_id),
            _ => None,
        })
        .collect();

    if !waited_signal_ids.is_empty() || !waited_cancel_ids.is_empty() {
        let history = store::load_history(conn, exec_id).await?;
        let resolved = history.events.iter().any(|ev| match ev {
            WorkflowEvent::ExternalSignalDelivered { signal_id }
            | WorkflowEvent::ExternalSignalFailed { signal_id, .. } => {
                waited_signal_ids.contains(signal_id)
            }
            WorkflowEvent::ExternalCancelDelivered { cancel_id }
            | WorkflowEvent::ExternalCancelFailed { cancel_id, .. } => {
                waited_cancel_ids.contains(cancel_id)
            }
            _ => false,
        });
        if resolved {
            queue::wake_workflow_task(conn, exec_id).await?;
        }
    }
    Ok(())
}

async fn persist_activity_wait_park(
    conn: &mut AsyncPgConnection,
    detached_spawns: DetachedSpawnPersistence<'_>,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    commands: &[WorkflowCommand],
    activity_ids: &[ActivityExecId],
    sticky: Option<queue::StickyHint<'_>>,
) -> HarvestResult<()> {
    let registry = detached_spawns.registry;

    let deferred = conn
        .transaction::<_, HarvestError, _>(|conn| {
            async move {
                harvest_workflow_executions::table
                    .find(exec_id.as_uuid())
                    .for_update()
                    .select(WorkflowExecution::as_select())
                    .first(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?
                    .ok_or_else(|| {
                        HarvestError::NotFound(format!("workflow execution {exec_id}"))
                    })?;

                // Cancellable/renewable timer bookkeeping (issue #768): resolve
                // the ArmTimer/CancelTimer row mutations FIRST, then interleave
                // their TimerStarted/TimerCancelled events at their command
                // positions in the marker batch appended below (armed timers are
                // observed on the next wake; deadline unused here).
                let (mut timer_events, _min_fires_at) =
                    plan_timer_lifecycle(conn, exec_id, commands).await?;
                let marker_events =
                    pre_suspension_events_from_commands(commands, &mut timer_events);
                for event in marker_events {
                    store::append_single_event(conn, exec_id, event).await?;
                }
                detached_spawns.persist(conn, commands).await?;

                let history = store::load_history(conn, exec_id).await?;
                let has_terminal = activity_ids
                    .iter()
                    .any(|activity_id| has_activity_terminal_event(&history.events, *activity_id));

                let mut next_event_id = history.next_event_id;
                let deferred = apply_race_loser_cancellations(
                    conn,
                    exec_id,
                    commands,
                    &mut next_event_id,
                    registry,
                )
                .await?;

                // `had_wake_requested` closes the residual race window `has_terminal`
                // cannot cover (PR #901 review): an already-scheduled activity
                // completing (and calling `wake_workflow_task`) between the history
                // load above and this park's own atomic UPDATE would otherwise have
                // its wake silently dropped, since `wake_workflow_task` no-ops
                // against a still-claimed (`worker_id IS NOT NULL`) row and the
                // returned flag was previously discarded here.
                let had_wake_requested = queue::park_workflow_task(conn, task_id, sticky).await?;
                if has_terminal || had_wake_requested {
                    queue::wake_workflow_task(conn, exec_id).await?;
                }
                Ok(deferred)
            }
            .scope_boxed()
        })
        .await?;

    for start in deferred {
        start.spawn();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn persist_scheduled_activities(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    detached_spawns: DetachedSpawnPersistence<'_>,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    commands: &[WorkflowCommand],
    scheduled_activities: &[ScheduledActivityCommand],
    sticky: Option<queue::StickyHint<'_>>,
    execute_span: &tracing::Span,
    assigned_build_id: Option<&str>,
    parent_priority: i32,
    context_headers: Option<&serde_json::Value>,
    workflow_input: &serde_json::Value,
) -> HarvestResult<()> {
    // activity_events is built in scheduled_activities order (= ScheduleActivity command order).
    // After the loop we interleave them with marker/detached-spawn events in full command order.
    let mut activity_events: Vec<WorkflowEvent> = Vec::with_capacity(scheduled_activities.len());
    let mut enqueued = Vec::with_capacity(scheduled_activities.len());
    // Dynamic per-key rate-limit buckets (issue #699) to lazily register inside
    // the enqueue transaction: `(bucket_key, refill_rate, burst)`. Deduped so a
    // fan-out of N activities sharing one resolved tenant key ensures the bucket
    // once. Registration must happen so the fail-closed claim/dispatch gate has a
    // bucket row to read (else the task would stall forever).
    let mut dynamic_rate_buckets: Vec<(String, f64, f64)> = Vec::new();

    for scheduled in scheduled_activities {
        let activity = registry.activities.get(&scheduled.name).ok_or_else(|| {
            HarvestError::Config(format!(
                "no activity handler registered for '{}'",
                scheduled.name
            ))
        })?;

        let queue_name = if scheduled.queue.is_empty() {
            activity.default_queue.unwrap_or("default").to_string()
        } else {
            scheduled.queue.clone()
        };

        let mut params = queue::EnqueueParams::new(
            queue_name.clone(),
            TaskType::Activity,
            scheduled.input.clone(),
        );
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.activity_name = Some(scheduled.name.clone());
        params.activity_id = Some(scheduled.activity_id.as_uuid());
        params.required_build_id = assigned_build_id.map(str::to_string);
        // Inherit priority from the parent workflow task so high-priority
        // workflows' activities are also claimed ahead of lower-priority work
        // on the same queue (issue #249).
        params.priority = parent_priority;

        if let Some(requires) = activity.requires {
            let reqs = crate::eligibility::parse_requirements(requires).map_err(|err| {
                HarvestError::Config(format!(
                    "Invalid requirements for activity {}: {}",
                    activity.name, err
                ))
            })?;
            params.required_capabilities = Some(serde_json::to_value(&reqs)?);
        }

        // Issue #620: call-site override → activity default → builder default.
        //
        // Reserved worker-session internal activities (issue #606,
        // `__harvest_session_acquire` / `__harvest_session_release`) are
        // engine-internal machinery bounded by schedule_to_start /
        // SessionOptions::acquisition_timeout, NOT by a user retry/timeout
        // floor. They must NOT inherit the builder-level default — fall back to
        // the pre-feature resolution (call-site override → activity default) for
        // them so an operator's floor never governs session acquire/release.
        let is_reserved = crate::context::is_reserved_session_activity_name(&scheduled.name);
        let builder_retry_default = if is_reserved {
            None
        } else {
            registry.default_activity_retry_policy()
        };
        let effective_retry = crate::policy::resolve_effective_retry(
            scheduled.retry_policy_override.clone(),
            activity.default_retry_policy.clone(),
            builder_retry_default,
        );
        if let Some(retry_policy) = effective_retry {
            params.max_attempts = i32::try_from(retry_policy.max_attempts).map_err(|_| {
                HarvestError::Config(format!(
                    "activity '{}' retry policy max_attempts exceeds i32 range",
                    activity.name
                ))
            })?;
            params.retry_policy = Some(serde_json::to_value(retry_policy)?);
        }

        if let Some(timeout) = activity.default_heartbeat_timeout {
            params.heartbeat_timeout =
                Some(chrono_duration_from_std(timeout, "heartbeat timeout")?);
        }
        // Issue #620: call-site override → activity default → builder default.
        // Reserved session-internal activities skip the builder floor (see the
        // retry resolution above for the rationale).
        let builder_stc_default = if is_reserved {
            None
        } else {
            registry.default_activity_start_to_close()
        };
        let effective_stc = crate::policy::resolve_effective_start_to_close(
            scheduled.start_to_close_override,
            activity.default_start_to_close,
            builder_stc_default,
        );
        if let Some(timeout) = effective_stc {
            params.start_to_close =
                Some(chrono_duration_from_std(timeout, "start_to_close timeout")?);
        }
        // Issue #606: a per-call schedule_to_start override (used exclusively
        // by the internal session-acquire dispatch to bound acquisition by
        // SessionOptions::acquisition_timeout) takes priority over the
        // activity's registered default -- mirroring retry_policy_override /
        // start_to_close_override above. `None` for every ordinary activity.
        let effective_schedule_to_start = scheduled
            .schedule_to_start_override
            .or(activity.default_schedule_to_start);
        if let Some(timeout) = effective_schedule_to_start {
            params.schedule_to_start = Some(chrono_duration_from_std(
                timeout,
                "schedule_to_start timeout",
            )?);
        }
        if let Some(timeout) = activity.default_schedule_to_close {
            let deadline = chrono::Utc::now()
                + chrono_duration_from_std(timeout, "schedule_to_close timeout")?;
            params.schedule_to_close_at = Some(deadline);
        }

        let effective_key = activity
            .concurrency_key
            .map(ToString::to_string)
            .or_else(|| activity.max_concurrent.map(|_| activity.name.to_string()));
        if let Some(key) = effective_key {
            params.concurrency_key = Some(key);
            params.max_concurrent = activity.max_concurrent;
        }

        if let Some(expr) = activity.rate_limit_key_expr {
            // A dynamic per-key rate limit requires an `rps` to derive its
            // bucket refill rate from. `HarvestBuilder::try_build` AND the
            // worker-startup gate (`crate::builder::validate_activity_rate_limits`,
            // now run from `Worker::new`) both reject a dynamic key without one
            // (RateLimitKeyExprWithoutCap). This schedule-time guard STAYS as
            // defense-in-depth (the startup validation is the primary
            // comprehensive gate as of issue #699 review, Codex round-5 P2):
            // fail the schedule transaction loudly here rather than silently
            // enqueuing the activity with NO rate limit -- without this guard the
            // `if let Some(refill_rate)` block below is skipped entirely, leaving
            // both `params.rate_limit_key` and `dynamic_rate_buckets` unset, so
            // the activity runs unrated, the exact failure a rate-limiting
            // feature exists to prevent.
            let Some(refill_rate) = activity.rate_limit_rps else {
                return Err(HarvestError::Config(format!(
                    "activity '{}' declares a dynamic rate_limit(key = \"{}\") but has no \
                     rate_limit_rps; a dynamic per-key rate limit requires an rps. \
                     (HarvestBuilder::try_build rejects this; you likely built \
                     HandlerRegistry directly.)",
                    activity.name, expr
                )));
            };
            // Dynamic per-key rate limit (issue #699): resolve the bucket key from
            // the workflow input at enqueue time so each tenant gets its own RPS
            // bucket. A key that cannot be resolved (missing / null / non-object
            // input) is passed through as `None` and falls back to a shared
            // `dyn-rate:{expr}:U` bucket so the execution is still bounded, not
            // unbounded -- kept DISTINCT from a legitimately-resolved empty-string
            // tenant `Some("")` (which buckets under `L0:`) so the two never
            // cross-throttle (issue #699 review, Codex round-5 P2). We deliberately
            // do NOT `.unwrap_or_default()` here, which would collapse both onto
            // the same `L0:` bucket. Takes priority over the static
            // `rate_limit_key` path entirely.
            let resolved = crate::concurrency::resolve_concurrency_key(expr, workflow_input);
            let bucket_key = queue::dynamic_rate_bucket_key(expr, resolved.as_deref());
            // The bucket is ensured in the same transaction below, so the
            // fail-closed claim/dispatch gate always has a bucket row to read
            // (else the task would stall forever). `refill_rate` is guaranteed
            // present by the guard above.
            let burst = activity.rate_limit_burst.unwrap_or(refill_rate);
            if !dynamic_rate_buckets
                .iter()
                .any(|(k, _, _)| *k == bucket_key)
            {
                dynamic_rate_buckets.push((bucket_key.clone(), refill_rate, burst));
            }
            params.rate_limit_key = Some(bucket_key);
        } else {
            // A static `rate_limit_key` beginning with the reserved `dyn-rate:`
            // prefix would collide with the generated dynamic per-key buckets,
            // and since startup registration and lazy dynamic registration both
            // `INSERT ... ON CONFLICT (key) DO NOTHING`, the bucket's rate/burst
            // would become insertion-order dependent. The macro parse-site
            // reject, `HarvestBuilder::try_build`, AND the worker-startup gate
            // (`crate::builder::validate_activity_rate_limits`, run from
            // `Worker::new`) all reject this now; this schedule-time guard STAYS
            // as defense-in-depth behind the primary startup gate (issue #699
            // review, Codex round-5 P2). Fail the schedule transaction loudly
            // here, mirroring the dynamic-no-rps guard above. The literal mirrors
            // `queue::DYNAMIC_RATE_PREFIX` (`"dyn-rate"`) + `":"`, matching the
            // builder's own static-key reject.
            if let Some(key) = activity.rate_limit_key
                && key.starts_with("dyn-rate:")
            {
                return Err(HarvestError::Config(format!(
                    "activity '{}' sets a static rate_limit_key = \"{}\" beginning with the \
                     reserved `dyn-rate:` prefix (reserved for dynamic per-key buckets); \
                     this collides with the generated dynamic bucket namespace and would \
                     race first-writer-wins on the shared bucket's rate/burst. \
                     (HarvestBuilder::try_build rejects this; you likely built \
                     HandlerRegistry directly.)",
                    activity.name, key
                )));
            }
            let effective_rate_limit_key = activity
                .rate_limit_key
                .map(ToString::to_string)
                .or_else(|| {
                    if activity.rate_limit_rps.is_some() || activity.rate_limit_burst.is_some() {
                        Some(activity.name.to_string())
                    } else {
                        None
                    }
                });
            if let Some(key) = effective_rate_limit_key {
                params.rate_limit_key = Some(key);
            }
        }

        // Worker sessions (issue #606): a member activity carrying a
        // resolved host worker is hard-pinned via session_id + the ordinary
        // sticky_worker_id/sticky_timeout columns -- claim_task's session_id
        // gate makes this pin unconditional (it does not fail over on lease
        // expiry the way ordinary sticky routing does). The session-acquire
        // dispatch itself never sets session_worker_id (it has no resolved
        // host yet), so it is never hard-pinned here.
        if let (Some(session_id), Some(host_worker_id)) =
            (scheduled.session_id, &scheduled.session_worker_id)
        {
            params.session_id = Some(session_id.as_uuid());
            params.sticky_worker_id = Some(host_worker_id.clone());
            params.sticky_timeout = Some(crate::sessions::SESSION_MEMBER_STICKY_TIMEOUT);
        }

        activity_events.push(WorkflowEvent::ActivityScheduled {
            activity_id: scheduled.activity_id,
            name: scheduled.name.clone(),
            input: scheduled.input.clone(),
            queue: queue_name.clone(),
        });

        params.trace_context = tracing::info_span!(
            parent: execute_span,
            "harvest.activity.schedule",
            "otel.kind" = "producer",
            { ATTR_ACTIVITY_NAME } = %scheduled.name,
            { ATTR_EXECUTION_ID } = %exec_id,
            { ATTR_QUEUE } = %queue_name,
        )
        .in_scope(|| registry.telemetry().capture_trace_context());
        params.context_headers = context_headers.cloned();
        enqueued.push(params);
    }

    let offloader = registry.payload_offloader();
    // `had_wake_requested` closes a race no other check in this function
    // covers (PR #901 review): a signal or admitted update landing while this
    // transaction is still appending events / enqueueing the scheduled
    // activities -- before this park's own atomic UPDATE -- would otherwise
    // only be captured as `wake_requested = TRUE` on the still-claimed row
    // and then silently discarded. Unlike the child-completion or
    // activity-completion races, this is a *fresh* dispatch (the activities
    // being scheduled here cannot have completed yet), so no other in-band
    // check exists to catch it.
    let (deferred, had_wake_requested, synthesized_broken_session_failure) = conn
        .transaction::<_, HarvestError, _>(|conn| {
            async move {
                // Cancellable/renewable timer bookkeeping (issue #768): resolve
                // the ArmTimer/CancelTimer row mutations FIRST, then build the
                // event list in command emission order -- markers, detached-spawn
                // events, interleaved TimerStarted/TimerCancelled, and
                // ActivityScheduled events all at their actual command positions
                // so the replay engine's sequential cursor sees the same order as
                // command emission (armed timers observed on next wake; deadline
                // unused here).
                let (mut timer_events, _min_fires_at) =
                    plan_timer_lifecycle(conn, exec_id, commands).await?;
                let mut act_iter = activity_events.into_iter();
                let events = build_suspension_events(commands, &mut timer_events, |cmd| {
                    if matches!(cmd, WorkflowCommand::ScheduleActivity { .. }) {
                        act_iter.next()
                    } else {
                        None
                    }
                });
                let events_len = i32::try_from(events.len()).unwrap_or(i32::MAX);
                store::append_events_offloaded(conn, exec_id, &events, next_event_id, offloader)
                    .await?;
                detached_spawns.persist(conn, commands).await?;
                // Lazily register any dynamic per-key rate-limit buckets (issue
                // #699) in the same transaction as the enqueue so the fail-closed
                // claim/dispatch gate always finds a bucket row (else the task
                // would stall forever). ON CONFLICT DO NOTHING preserves operator
                // overrides and is idempotent across replays/re-dispatch.
                for (bucket_key, refill_rate, burst) in &dynamic_rate_buckets {
                    queue::ensure_rate_limit_bucket(conn, bucket_key, *refill_rate, *burst).await?;
                }
                let mut activity_task_ids = Vec::with_capacity(enqueued.len());
                for params in &enqueued {
                    activity_task_ids.push(queue::enqueue(conn, params).await?);
                }
                let mut race_next_event_id = next_event_id.saturating_add(events_len);
                let deferred = apply_race_loser_cancellations(
                    conn,
                    exec_id,
                    commands,
                    &mut race_next_event_id,
                    registry,
                )
                .await?;

                // Worker sessions (issue #606): a member activity's session
                // may have already left ACTIVE (the broken-session scanner
                // reclaimed it) in the gap between the workflow's previous
                // session activity call and this one -- the scanner only
                // ever revisits `state = 'ACTIVE'` rows, so a freshly
                // hard-pinned task enqueued here for an already-broken
                // session would otherwise sit PENDING against a dead or
                // draining host forever, with no future recovery path.
                // Immediately fail any such task with the same
                // ActivityFailed{SessionBroken} shape
                // `break_session_and_fail_members` uses for in-flight
                // tasks, so the workflow observes SessionBroken on its next
                // decision cycle instead of hanging.
                let session_ids: std::collections::HashSet<uuid::Uuid> = scheduled_activities
                    .iter()
                    .filter_map(|s| s.session_id.map(|id| id.as_uuid()))
                    .collect();
                let mut synthesized_broken_session_failure = false;
                if !session_ids.is_empty() {
                    use crate::schema::harvest_sessions::dsl as sess_dsl;
                    let broken: std::collections::HashMap<uuid::Uuid, String> =
                        sess_dsl::harvest_sessions
                            .filter(sess_dsl::id.eq_any(&session_ids))
                            .filter(sess_dsl::state.ne("ACTIVE"))
                            .select((sess_dsl::id, sess_dsl::broken_reason))
                            .load::<(uuid::Uuid, Option<String>)>(conn)
                            .await
                            .map_err(crate::error::database_error)?
                            .into_iter()
                            .map(|(id, reason)| {
                                (
                                    id,
                                    reason.unwrap_or_else(|| {
                                        "session is no longer ACTIVE".to_string()
                                    }),
                                )
                            })
                            .collect();

                    if !broken.is_empty() {
                        for (scheduled, activity_task_id) in
                            scheduled_activities.iter().zip(activity_task_ids.iter())
                        {
                            let Some(session_uuid) = scheduled.session_id.map(|id| id.as_uuid())
                            else {
                                continue;
                            };
                            let Some(reason) = broken.get(&session_uuid) else {
                                continue;
                            };
                            let failed_event = WorkflowEvent::ActivityFailed {
                                activity_id: scheduled.activity_id,
                                error: reason.clone(),
                                attempt: 1,
                                error_type: crate::failure::ERROR_TYPE_SESSION_BROKEN.to_string(),
                                non_retryable: true,
                                details: None,
                            };
                            store::append_events(
                                conn,
                                exec_id,
                                &[failed_event],
                                race_next_event_id,
                            )
                            .await?;
                            race_next_event_id = race_next_event_id.saturating_add(1);
                            queue::fail_task(conn, *activity_task_id, reason).await?;
                            synthesized_broken_session_failure = true;
                        }
                    }
                }

                let had_wake_requested = queue::park_workflow_task(conn, task_id, sticky).await?;
                Ok((
                    deferred,
                    had_wake_requested,
                    synthesized_broken_session_failure,
                ))
            }
            .scope_boxed()
        })
        .await?;

    for start in deferred {
        start.spawn();
    }

    // The synthesized SessionBroken failure(s) above are not tied to any
    // external wake source (they were resolved entirely within this
    // transaction), so the workflow must be woken unconditionally to
    // observe them on its next decision cycle -- `had_wake_requested` alone
    // would miss this case.
    if had_wake_requested || synthesized_broken_session_failure {
        queue::wake_workflow_task(conn, exec_id).await?;
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn persist_started_timer(
    conn: &mut AsyncPgConnection,
    detached_spawns: DetachedSpawnPersistence<'_>,
    exec_id: ExecutionId,
    next_event_id: i32,
    task_id: uuid::Uuid,
    commands: &[WorkflowCommand],
    timer: &StartedTimerCommand,
    sticky: Option<queue::StickyHint<'_>>,
    // Issue #678/#1034: external-op ids resolved INLINE this cycle by
    // `persist_external_signal_inline`. Non-empty only on the mixed timer +
    // same-shard external shape (a select!-style race whose external branch
    // already resolved). When non-empty this function force-marks the parked row
    // `mixed_signal_suspension` (the STAMP only) so the arm-level self-wake in
    // `persist_workflow_outcome` (#1034) can re-pend it via the primary re-pend
    // query — the wake itself no longer happens here. A pure timer sleep threads
    // the empty default: no stamp, no false-wake, and never a blanket history scan.
    resolved_inline_external: &ResolvedExternalIds,
) -> HarvestResult<()> {
    use tracing::Instrument;

    // Emit a span for the timer placement (not to be confused with
    // harvest.timer.fire which is emitted when the timer actually fires).
    let span = tracing::info_span!(
        "harvest.timer.start",
        "otel.kind" = "internal",
        timer.id = %timer.timer_id,
        timer.duration_secs = timer.duration_secs,
        { ATTR_EXECUTION_ID } = %exec_id,
    );

    let registry = detached_spawns.registry;

    let deferred = conn
        .transaction::<_, HarvestError, _>(|conn| {
        async move {
            use crate::schema::harvest_task_queue::dsl as queue_dsl;

            // Cancellable/renewable timer bookkeeping (issue #768) for any
            // *other* timers' ArmTimer/CancelTimer commands in this batch (the
            // suspending StartTimer's own id is skipped by plan_timer_lifecycle
            // for ArmTimer row inserts — its row insert / TimerStarted are owned
            // here). Build the event list in command emission order: markers,
            // detached-spawn events, interleaved TimerStarted/TimerCancelled, and
            // this StartTimer's own TimerStarted all at their actual command
            // positions.
            let (mut timer_events, _min_fires_at) =
                plan_timer_lifecycle(conn, exec_id, commands).await?;

            // FIX B (Codex P2 round 4, issue #768): resolve the classic
            // StartTimer's OWN row existence AFTER plan_timer_lifecycle, not
            // before the transaction. plan_timer_lifecycle honours a same-batch
            // `CancelTimer` for the SAME id by deleting its pending row; computing
            // `is_new` from a pre-pass snapshot would leave `is_new = false` and
            // then reschedule the parked task to a row this very batch deleted —
            // the workflow would hang and replay diverge. Re-querying here reflects
            // the delete, so a `cancel_timer("x"); ctx.timer("x", n)` in one task
            // correctly re-inserts a fresh classic-timer row.
            //
            // Anchor a fresh deadline to the database clock: timer due-ness and
            // the signal `received_at` default both come from Postgres NOW(), so
            // the chronological wake ingest (merge_wake_events) compares timestamps
            // from a single clock regardless of worker clock skew.
            let existing: Option<HarvestTimer> = harvest_timers::table
                .filter(harvest_timers::workflow_exec_id.eq(exec_id.as_uuid()))
                .filter(harvest_timers::timer_id.eq(timer.timer_id.as_str()))
                .filter(harvest_timers::fired.eq(false))
                .first::<HarvestTimer>(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            let fires_at = if let Some(ref ext) = existing {
                ext.fires_at
            } else {
                let fire_delay =
                    chrono_duration_from_secs(timer.duration_secs, "timer duration")?;
                let db_now = db_clock_now(conn).await?;
                db_now + fire_delay
            };
            let is_new = existing.is_none();
            // The suspending StartTimer's own TimerStarted (for a new timer) is
            // emitted at the StartTimer command's position via the branch closure.
            let mut timer_event = is_new.then(|| WorkflowEvent::TimerStarted {
                timer_id: timer.timer_id.clone(),
                duration_secs: timer.duration_secs,
            });

            let events = build_suspension_events(commands, &mut timer_events, |cmd| {
                if matches!(cmd, WorkflowCommand::StartTimer { .. }) {
                    timer_event.take()
                } else {
                    None
                }
            });
            let has_events = !events.is_empty();
            let events_len = i32::try_from(events.len()).unwrap_or(i32::MAX);

            if has_events {
                store::append_events(conn, exec_id, &events, next_event_id).await?;
            }
            detached_spawns.persist(conn, commands).await?;

            let mut race_next_event_id = next_event_id.saturating_add(events_len);
            let deferred = apply_race_loser_cancellations(
                conn,
                exec_id,
                commands,
                &mut race_next_event_id,
                registry,
            )
            .await?;

            if is_new {
                let new_timer = NewHarvestTimer {
                    workflow_exec_id: exec_id.as_uuid(),
                    timer_id: timer.timer_id.as_str(),
                    fires_at,
                };
                diesel::insert_into(harvest_timers::table)
                    .values(&new_timer)
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            }

            queue::reschedule_task(conn, task_id, fires_at).await?;
            let mut is_mixed = commands.iter().any(|cmd| {
                matches!(
                    cmd,
                    WorkflowCommand::WaitForSignal { .. }
                        | WorkflowCommand::SignalExternalWorkflow { .. }
                        | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                )
            });
            if !is_mixed {
                #[derive(diesel::deserialize::QueryableByName)]
                struct DummyRow {
                    #[diesel(sql_type = diesel::sql_types::Integer)]
                    #[allow(dead_code)]
                    dummy: i32,
                }
                let unresolved_exists: Result<Vec<DummyRow>, diesel::result::Error> = diesel::sql_query(
                    "SELECT 1 AS dummy FROM harvest_events e \
                     WHERE e.workflow_exec_id = $1 \
                       AND ( \
                         ( e.event_type = 'ExternalSignalRequested' \
                           AND NOT EXISTS ( \
                               SELECT 1 FROM harvest_events res \
                               WHERE res.workflow_exec_id = e.workflow_exec_id \
                                 AND res.event_type IN ('ExternalSignalDelivered', 'ExternalSignalFailed') \
                                 AND res.event_data->'data'->>'signal_id' = e.event_data->'data'->>'signal_id' \
                           ) ) \
                         OR \
                         ( e.event_type = 'ExternalCancelRequested' \
                           AND NOT EXISTS ( \
                               SELECT 1 FROM harvest_events res \
                               WHERE res.workflow_exec_id = e.workflow_exec_id \
                                 AND res.event_type IN ('ExternalCancelDelivered', 'ExternalCancelFailed') \
                                 AND res.event_data->'data'->>'cancel_id' = e.event_data->'data'->>'cancel_id' \
                           ) ) \
                       ) \
                     LIMIT 1"
                )
                .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
                .load(conn)
                .await;
                if let Ok(rows) = unresolved_exists
                    && !rows.is_empty()
                {
                    is_mixed = true;
                }
            }
            // Issue #678: a mixed timer + same-shard external op resolves the
            // external terminal INLINE this cycle, so the `unresolved_exists`
            // probe above (which looks for *unresolved* external requests) finds
            // nothing and would leave `is_mixed = false` — parking the timer for
            // up to `fires_at` with no wakeable sentinel. Force mixed so the
            // parked row carries `activity_name = 'mixed_signal_suspension'`; the
            // arm-level self-wake in `persist_workflow_outcome` (#1034) relies on
            // this stamp to re-pend the timer's PENDING row via its primary
            // re-pend query. This inner `conn.transaction` is a nested SAVEPOINT
            // inside the outer persist txn, so the park (and the re-pend the wake
            // performs) commit atomically with the outer commit (the NOTIFY
            // defers to it): there is no crash window between park and wake.
            if !resolved_inline_external.is_empty() {
                is_mixed = true;
            }
            let activity_name_val = if is_mixed {
                Some("mixed_signal_suspension".to_string())
            } else {
                None
            };
            diesel::update(queue_dsl::harvest_task_queue.find(task_id))
                .set(queue_dsl::activity_name.eq(activity_name_val))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            if sticky.is_some() {
                queue::set_task_sticky_affinity(conn, task_id, sticky).await?;
            }
            Ok(deferred)
        }
        .scope_boxed()
    })
        .instrument(span)
        .await?;

    for start in deferred {
        start.spawn();
    }

    // A signal may have arrived while this task was actively running (before
    // the park above). `send_signal` would have called `wake_workflow_task` at
    // that point but found neither a parked row nor a pending
    // mixed_signal_suspension row to pull forward — so without this re-check a
    // signal-or-deadline wait would sleep until `fires_at` even though its
    // signal already arrived. Mirror persist_signal_wait_park: re-check now
    // that the mixed park is committed and self-wake if signals are pending.
    //
    // Safety: if a new signal arrives *after* this check returns empty, its
    // `send_signal` caller will call `wake_workflow_task` and find the
    // committed PENDING mixed_signal_suspension row — so the wake is
    // guaranteed regardless of timing. Pure timer sleeps (no WaitForSignal in
    // the batch) are excluded: a pending signal must not fire a timer early.
    let waits_on_signal = commands
        .iter()
        .any(|cmd| matches!(cmd, WorkflowCommand::WaitForSignal { .. }));
    if waits_on_signal {
        let pending = signal::load_pending_signals(conn, exec_id).await?;
        if !pending.is_empty() {
            queue::wake_workflow_task(conn, exec_id).await?;
        }
    }

    // Issue #1034: the inline-resolved external self-wake is now applied
    // UNIFORMLY at the `Suspended` arm of persist_workflow_outcome (once, for
    // every suspension shape) rather than here. This function still force-marks
    // the parked row `mixed_signal_suspension` above (via the
    // `resolved_inline_external` stamp) so the arm-level wake's primary re-pend
    // query re-pends the timer's PENDING row; only the redundant wake CALL moved.
    Ok(())
}

#[allow(clippy::too_many_arguments)]
/// Atomically create zero or more child workflow executions and park the parent.
///
/// Children whose `child_id` already exists in `harvest_workflow_executions` are
/// silently skipped — this is the idempotent re-park path taken when the parent
/// wakes after one of several parallel children completes while others are still
/// running.  Only genuinely new children get rows inserted and tasks enqueued.
#[allow(clippy::too_many_lines)]
async fn persist_all_started_child_workflows(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task_id: uuid::Uuid,
    parent_execution: &WorkflowExecution,
    commands: &[WorkflowCommand],
    children: &[StartedChildWorkflowCommand],
    sticky: Option<queue::StickyHint<'_>>,
    execute_span: &tracing::Span,
) -> HarvestResult<()> {
    for child in children {
        if !registry.workflows.contains_key(&child.workflow_name) {
            return Err(HarvestError::Config(format!(
                "no workflow handler registered for '{}'",
                child.workflow_name
            )));
        }
    }

    let parent_exec_id = execution_id_from_uuid(parent_execution.id);
    let queue_name = parent_execution.queue_name.clone();
    let children = children.to_vec();
    // Pre-compute position-tagged pre-suspension events (markers + detached-spawns) and
    // the command indices of StartChildWorkflow commands so that inside the transaction —
    // after learning which children are genuinely new — we can merge everything into a
    // single event list in command emission order.
    let pre_events_by_pos: Vec<(usize, WorkflowEvent)> = commands
        .iter()
        .enumerate()
        .filter_map(|(i, cmd)| match cmd {
            WorkflowCommand::RecordMarker { name, details } => Some((
                i,
                WorkflowEvent::MarkerRecorded {
                    name: name.clone(),
                    details: details.clone(),
                },
            )),
            WorkflowCommand::RecordSideEffect { kind, name, value } => Some((
                i,
                WorkflowEvent::SideEffectRecorded {
                    kind: *kind,
                    name: name.clone(),
                    value: value.clone(),
                },
            )),
            WorkflowCommand::SpawnDetachedChildWorkflow {
                child_id,
                workflow_name,
                input,
                parent_close_policy,
            } => Some((
                i,
                WorkflowEvent::ChildWorkflowSpawnedDetached {
                    child_id: *child_id,
                    workflow_name: workflow_name.clone(),
                    input: input.clone(),
                    parent_close_policy: *parent_close_policy,
                },
            )),
            _ => None,
        })
        .collect();
    // children[k] corresponds to the k-th StartChildWorkflow command in `commands`.
    let start_child_cmd_indices: Vec<usize> = commands
        .iter()
        .enumerate()
        .filter_map(|(i, cmd)| {
            if matches!(cmd, WorkflowCommand::StartChildWorkflow { .. }) {
                Some(i)
            } else {
                None
            }
        })
        .collect();
    let shard_id = parent_execution.shard_id;

    // Clone telemetry and execute_span before the transaction closure so they
    // can be used inside the async move block without capturing references.
    let telemetry = registry.telemetry().clone();
    let execute_span = execute_span.clone();

    let deferred = conn
        .transaction::<_, HarvestError, _>(|conn| {
            let children = children.clone();
            let pre_events_by_pos = pre_events_by_pos.clone();
            let start_child_cmd_indices = start_child_cmd_indices.clone();
            let queue_name = queue_name.clone();
            let telemetry = telemetry.clone();
            let execute_span = execute_span.clone();
            async move {
                // Determine which children are genuinely new vs. already running.
                let requested_ids: Vec<uuid::Uuid> =
                    children.iter().map(|c| c.child_id.as_uuid()).collect();
                let existing_ids: HashSet<uuid::Uuid> = harvest_workflow_executions::table
                    .filter(harvest_workflow_executions::id.eq_any(&requested_ids))
                    .select(harvest_workflow_executions::id)
                    .load::<uuid::Uuid>(conn)
                    .await
                    .map_err(crate::error::database_error)?
                    .into_iter()
                    .collect();

                let new_children: Vec<&StartedChildWorkflowCommand> = children
                    .iter()
                    .filter(|c| !existing_ids.contains(&c.child_id.as_uuid()))
                    .collect();

                // ADR-0001 §2.8: emit harvest.child_workflow.start PRODUCER spans only
                // for genuinely new children (after the existing_ids filter).
                // `parent: &execute_span` makes each span a child of this executor
                // cycle's harvest.workflow.execute span even though that span's
                // instrumented future has already returned (the handle is still open).
                // EnteredSpan is !Send so each span must be fully dropped (via
                // .in_scope) before the next .await.
                let child_trace_ctxs: std::collections::HashMap<
                    uuid::Uuid,
                    Option<TraceContextCarrier>,
                > = new_children
                    .iter()
                    .map(|child| {
                        let ctx = tracing::info_span!(
                            parent: &execute_span,
                            "harvest.child_workflow.start",
                            "otel.kind" = "producer",
                            { ATTR_WORKFLOW_ID } = %child.workflow_name,
                            { ATTR_EXECUTION_ID } = %child.child_id,
                            { ATTR_SHARD_ID } = shard_id,
                        )
                        .in_scope(|| telemetry.capture_trace_context());
                        (child.child_id.as_uuid(), ctx)
                    })
                    .collect();

                // Build the parent event list in command emission order.
                // Markers, detached-spawn events, and ChildWorkflowStarted events are
                // interleaved at their actual command positions so the replay engine's
                // sequential cursor sees the same order as command emission.
                // append_single_event (not append_events) is used so each insert
                // re-reads MAX(event_id) under the parent-row FOR UPDATE lock, serialising
                // against concurrent ChildWorkflowCompleted/Failed appends from sibling
                // children that complete while this parent task is still RUNNING.
                let new_child_id_set: HashSet<uuid::Uuid> =
                    new_children.iter().map(|c| c.child_id.as_uuid()).collect();
                let mut child_events_by_pos: Vec<(usize, WorkflowEvent)> = start_child_cmd_indices
                    .iter()
                    .zip(children.iter())
                    .filter_map(|(pos, child)| {
                        if new_child_id_set.contains(&child.child_id.as_uuid()) {
                            Some((
                                *pos,
                                WorkflowEvent::ChildWorkflowStarted {
                                    child_id: child.child_id,
                                    workflow_name: child.workflow_name.clone(),
                                    input: child.input.clone(),
                                },
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();
                // Cancellable/renewable timer bookkeeping (issue #768): resolve
                // the ArmTimer/CancelTimer row mutations, then merge their
                // TimerStarted/TimerCancelled events into the parent event list at
                // their command-emission positions (armed timers observed on next
                // wake; deadline unused here).
                let (timer_events, _min_fires_at) =
                    plan_timer_lifecycle(conn, parent_exec_id, commands).await?;
                let mut timer_events_by_pos: Vec<(usize, WorkflowEvent)> = timer_events
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, ev)| ev.map(|e| (i, e)))
                    .collect();
                let mut all_events_by_pos = pre_events_by_pos;
                all_events_by_pos.append(&mut child_events_by_pos);
                all_events_by_pos.append(&mut timer_events_by_pos);
                all_events_by_pos.sort_unstable_by_key(|(i, _)| *i);
                let parent_events: Vec<WorkflowEvent> =
                    all_events_by_pos.into_iter().map(|(_, e)| e).collect();
                for event in parent_events {
                    store::append_single_event(conn, parent_exec_id, event).await?;
                }
                create_detached_child_executions(
                    conn,
                    registry,
                    parent_execution,
                    commands,
                    &execute_span,
                )
                .await?;

                let mut race_next_event_id = store::load_history(conn, parent_exec_id)
                    .await?
                    .next_event_id;
                let race_deferred = apply_race_loser_cancellations(
                    conn,
                    parent_exec_id,
                    commands,
                    &mut race_next_event_id,
                    registry,
                )
                .await?;

                // Insert rows and enqueue tasks for new children.
                // Provenance ref for every child in this fan-out is the parent
                // execution id (issue #740); compute it once outside the loop.
                let parent_exec_id_str = parent_exec_id.to_string();
                for child in &new_children {
                    let child_workflow_id = child.child_id.to_string();
                    let child_wf_info = registry.workflows.get(child.workflow_name.as_str());
                    let (
                        owner,
                        runbook_url,
                        severity,
                        child_sla,
                        child_execution_timeout,
                        child_retry_policy,
                    ) = child_wf_info.map_or((None, None, None, None, None, None), |w| {
                        (
                            w.owner,
                            w.runbook_url,
                            w.severity,
                            w.sla,
                            w.execution_timeout,
                            w.retry_policy.clone(),
                        )
                    });
                    let child_execution_timeout =
                        child_execution_timeout.and_then(|d| chrono::Duration::from_std(d).ok());
                    let child_sla = child_sla.and_then(|d| chrono::Duration::from_std(d).ok());
                    let child_deadline_at = child_execution_timeout.map(|d| chrono::Utc::now() + d);
                    let child_sla_deadline_at = child_sla.map(|d| chrono::Utc::now() + d);
                    let child_row = NewWorkflowExecution {
                        continued_from_exec_id: None,
                        first_exec_id: None,
                        id: child.child_id.as_uuid(),
                        workflow_name: &child.workflow_name,
                        workflow_id: &child_workflow_id,
                        run_id: uuid::Uuid::new_v4(),
                        shard_id,
                        input: child.input.clone(),
                        parent_id: Some(parent_exec_id.as_uuid()),
                        queue_name: &queue_name,
                        execution_timeout: child_execution_timeout,
                        deadline_at: child_deadline_at,
                        sla: child_sla,
                        sla_deadline_at: child_sla_deadline_at,
                        memo: None,
                        search_attrs: None,
                        assigned_build_id: parent_execution.assigned_build_id.clone(),
                        parent_close_policy: None, // awaited child
                        owner,
                        runbook_url,
                        severity,
                        context_headers: parent_execution.context_headers.clone(),
                        schedule_id: None, // child workflows are not scheduled fires
                        scheduled_for: None,
                        workflow_attempt: 1,
                        workflow_retry_policy: child_retry_policy
                            .and_then(|p| serde_json::to_value(&p).ok()),
                        retry_of_exec_id: None,
                        origin: None, // child workflow, not a schedule fire (issue #534)
                        // Children get only builder-wide default callback
                        // targets, resolved at their own terminal transition
                        // (issue #605) — no per-execution override here.
                        completion_callbacks: None,
                        start_source: Some(crate::types::StartSource::Child.as_str()),
                        start_source_ref: Some(parent_exec_id_str.as_str()),
                        started_by: None,
                    };
                    let child_started_event = WorkflowEvent::WorkflowStarted {
                        input: child.input.clone(),
                        timestamp: chrono::Utc::now(),
                        last_completion_result: None,
                        last_error: None,
                        scheduled_time: None, // child workflows are not scheduler-fired
                    };
                    let mut params = queue::EnqueueParams::new(
                        queue_name.clone(),
                        TaskType::Workflow,
                        child.input.clone(),
                    );
                    params.workflow_exec_id = Some(child.child_id.as_uuid());
                    params.required_build_id = parent_execution.assigned_build_id.clone();
                    (params.concurrency_key, params.max_concurrent) =
                        resolve_workflow_concurrency(registry, &child.workflow_name, &child.input);
                    params.trace_context = child_trace_ctxs
                        .get(&child.child_id.as_uuid())
                        .cloned()
                        .flatten();

                    diesel::insert_into(harvest_workflow_executions::table)
                        .values(&child_row)
                        .execute(conn)
                        .await
                        .map_err(crate::error::database_error)?;
                    store::append_events_offloaded(
                        conn,
                        child.child_id,
                        &[child_started_event],
                        0,
                        registry.payload_offloader(),
                    )
                    .await?;
                    queue::enqueue(conn, &params).await?;
                }

                // Check for already-terminal children only in the re-park path
                // (new_children is empty).  In the initial park path all children
                // were just created inside this transaction and are invisible to
                // other transactions until commit, so they cannot be terminal and
                // the check would always return false.  Skipping it also avoids a
                // lock-order inversion: append_single_event (for new ChildWorkflowStarted
                // events) holds the parent execution row lock, and then acquiring
                // child execution row locks via FOR UPDATE would be the inverse of
                // the child-completion order (child exec row → parent exec row via
                // wake_parent append_single_event).
                //
                // In the re-park path there are no append_single_event calls, so
                // lock order is child exec rows → parent task queue row, which
                // matches the child-completion path.  A terminal child here means
                // wake_workflow_task was a no-op while the parent was RUNNING, so
                // we re-wake after parking.
                let any_terminal = if new_children.is_empty() {
                    let child_states: Vec<String> = harvest_workflow_executions::table
                        .filter(harvest_workflow_executions::id.eq_any(&requested_ids))
                        .for_update()
                        .select(harvest_workflow_executions::state)
                        .load::<String>(conn)
                        .await
                        .map_err(crate::error::database_error)?;
                    child_states.iter().any(|s| {
                        matches!(
                            s.as_str(),
                            "COMPLETED" | "FAILED" | "TIMED_OUT" | "CANCELLED" | "TERMINATED"
                        )
                    })
                } else {
                    false
                };

                // `had_wake_requested` closes the residual race window the
                // `any_terminal` check above cannot cover: a child transitioning
                // to terminal (and calling `wake_workflow_task`) between that
                // check and this park's own atomic UPDATE would otherwise have
                // its wake silently dropped, since `wake_workflow_task` no-ops
                // against a still-claimed (`worker_id IS NOT NULL`) row.
                let had_wake_requested = queue::park_workflow_task(conn, task_id, sticky).await?;

                if any_terminal || had_wake_requested {
                    queue::wake_workflow_task(conn, parent_exec_id).await?;
                }

                Ok(race_deferred)
            }
            .scope_boxed()
        })
        .await?;

    for start in deferred {
        start.spawn();
    }
    Ok(())
}

/// Insert a single genuinely-new **awaited** child execution row, append its own
/// `WorkflowStarted` event, and enqueue its workflow task (issue #779).
///
/// This is a self-contained clone of the per-child body of
/// [`persist_all_started_child_workflows`] scoped to exactly one child. It is a
/// deliberate *addition* — the plain parallel-child persist path is left
/// byte-identical for merge safety — used only by [`persist_child_timeout_race`].
/// The parent's own `ChildWorkflowStarted` event is appended by the caller (in
/// command-emission order with the sibling `TimerStarted`); this helper only
/// touches the child row, the child's history, and the child's task.
async fn insert_awaited_child_execution(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    parent_execution: &WorkflowExecution,
    parent_exec_id: ExecutionId,
    child: &StartedChildWorkflowCommand,
    trace_context: Option<TraceContextCarrier>,
) -> HarvestResult<()> {
    let shard_id = parent_execution.shard_id;
    let queue_name = parent_execution.queue_name.clone();

    let child_workflow_id = child.child_id.to_string();
    // Provenance ref for the child is the parent execution id (issue #740).
    let parent_exec_id_str = parent_exec_id.to_string();
    let child_wf_info = registry.workflows.get(child.workflow_name.as_str());
    let (owner, runbook_url, severity, child_sla, child_execution_timeout, child_retry_policy) =
        child_wf_info.map_or((None, None, None, None, None, None), |w| {
            (
                w.owner,
                w.runbook_url,
                w.severity,
                w.sla,
                w.execution_timeout,
                w.retry_policy.clone(),
            )
        });
    let child_execution_timeout =
        child_execution_timeout.and_then(|d| chrono::Duration::from_std(d).ok());
    let child_sla = child_sla.and_then(|d| chrono::Duration::from_std(d).ok());
    let child_deadline_at = child_execution_timeout.map(|d| chrono::Utc::now() + d);
    let child_sla_deadline_at = child_sla.map(|d| chrono::Utc::now() + d);
    let child_row = NewWorkflowExecution {
        continued_from_exec_id: None,
        first_exec_id: None,
        id: child.child_id.as_uuid(),
        workflow_name: &child.workflow_name,
        workflow_id: &child_workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id,
        input: child.input.clone(),
        parent_id: Some(parent_exec_id.as_uuid()),
        queue_name: &queue_name,
        execution_timeout: child_execution_timeout,
        deadline_at: child_deadline_at,
        sla: child_sla,
        sla_deadline_at: child_sla_deadline_at,
        memo: None,
        search_attrs: None,
        assigned_build_id: parent_execution.assigned_build_id.clone(),
        parent_close_policy: None, // awaited child
        owner,
        runbook_url,
        severity,
        context_headers: parent_execution.context_headers.clone(),
        schedule_id: None, // child workflows are not scheduled fires
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: child_retry_policy.and_then(|p| serde_json::to_value(&p).ok()),
        retry_of_exec_id: None,
        origin: None, // child workflow, not a schedule fire (issue #534)
        // Children get only builder-wide default callback targets, resolved at
        // their own terminal transition (issue #605) — no per-execution override.
        completion_callbacks: None,
        start_source: Some(crate::types::StartSource::Child.as_str()),
        start_source_ref: Some(parent_exec_id_str.as_str()),
        started_by: None,
    };
    let child_started_event = WorkflowEvent::WorkflowStarted {
        input: child.input.clone(),
        timestamp: chrono::Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None, // child workflows are not scheduler-fired
    };
    let mut params =
        queue::EnqueueParams::new(queue_name.clone(), TaskType::Workflow, child.input.clone());
    params.workflow_exec_id = Some(child.child_id.as_uuid());
    params.required_build_id = parent_execution.assigned_build_id.clone();
    (params.concurrency_key, params.max_concurrent) =
        resolve_workflow_concurrency(registry, &child.workflow_name, &child.input);
    params.trace_context = trace_context;

    diesel::insert_into(harvest_workflow_executions::table)
        .values(&child_row)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    store::append_events_offloaded(
        conn,
        child.child_id,
        &[child_started_event],
        0,
        registry.payload_offloader(),
    )
    .await?;
    queue::enqueue(conn, &params).await?;
    Ok(())
}

/// Persist the child-timeout race suspension batch (issue #779): spawn the child
/// **and** arm the deadline timer atomically, then park the parent so it is woken
/// by whichever resolves first.
///
/// Mirrors the #476 signal-or-deadline mechanism ([`persist_started_timer`]): the
/// parent task is rescheduled to `fires_at` (so the durable timer fires it when
/// due) and stamped `activity_name = 'mixed_signal_suspension'` so an *early*
/// wake — here a child terminal, delivered by `wake_parent_for_child_completion`
/// / `wake_parent_for_child_failure` via [`queue::wake_workflow_task`] — re-pends
/// the future-scheduled `PENDING` row through
/// `primary_repend_workflow_task_query`'s second arm. Without that sentinel the
/// child-completion wake would match neither arm and be silently dropped, so the
/// parent would sleep until the deadline even when the child finished first.
///
/// The parent's `ChildWorkflowStarted` is appended **before** `TimerStarted` (the
/// `StartChildWorkflow` command is emitted first), in one transaction, because
/// `match_child_or_timer` positionally matches the pair.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn persist_child_timeout_race(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task_id: uuid::Uuid,
    parent_execution: &WorkflowExecution,
    commands: &[WorkflowCommand],
    child: &StartedChildWorkflowCommand,
    timer: &StartedTimerCommand,
    sticky: Option<queue::StickyHint<'_>>,
    execute_span: &tracing::Span,
) -> HarvestResult<()> {
    if !registry.workflows.contains_key(&child.workflow_name) {
        return Err(HarvestError::Config(format!(
            "no workflow handler registered for '{}'",
            child.workflow_name
        )));
    }

    let parent_exec_id = execution_id_from_uuid(parent_execution.id);
    let child_id = child.child_id;
    let telemetry = registry.telemetry().clone();
    let execute_span = execute_span.clone();

    let deferred = conn
        .transaction::<_, HarvestError, _>(|conn| {
            let telemetry = telemetry.clone();
            let execute_span = execute_span.clone();
            async move {
                use crate::schema::harvest_task_queue::dsl as queue_dsl;

                // ── child new-vs-existing ──
                let child_is_new = harvest_workflow_executions::table
                    .find(child.child_id.as_uuid())
                    .select(harvest_workflow_executions::id)
                    .first::<uuid::Uuid>(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?
                    .is_none();

                // ADR-0001 §2.8: emit a harvest.child_workflow.start PRODUCER span
                // only for a genuinely new child. Parent it to this executor
                // cycle's execute span (EnteredSpan is !Send, so capture inside
                // .in_scope before any await).
                let child_trace_ctx = if child_is_new {
                    tracing::info_span!(
                        parent: &execute_span,
                        "harvest.child_workflow.start",
                        "otel.kind" = "producer",
                        { ATTR_WORKFLOW_ID } = %child.workflow_name,
                        { ATTR_EXECUTION_ID } = %child.child_id,
                        { ATTR_SHARD_ID } = parent_execution.shard_id,
                    )
                    .in_scope(|| telemetry.capture_trace_context())
                } else {
                    None
                };

                // ── timer new-vs-existing + fires_at (DB clock) ──
                let existing_timer: Option<HarvestTimer> = harvest_timers::table
                    .filter(harvest_timers::workflow_exec_id.eq(parent_exec_id.as_uuid()))
                    .filter(harvest_timers::timer_id.eq(timer.timer_id.as_str()))
                    .filter(harvest_timers::fired.eq(false))
                    .first::<HarvestTimer>(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?;
                let timer_is_new = existing_timer.is_none();
                let fires_at = if let Some(ref ext) = existing_timer {
                    ext.fires_at
                } else {
                    let fire_delay =
                        chrono_duration_from_secs(timer.duration_secs, "child timeout duration")?;
                    let db_now = db_clock_now(conn).await?;
                    db_now + fire_delay
                };

                // ── append parent events in command emission order ──
                // StartChildWorkflow is pushed first, StartTimer second, so
                // ChildWorkflowStarted precedes TimerStarted (the positional
                // matcher relies on this). Tolerated bookkeeping (markers,
                // side-effects) is interleaved at its position by
                // build_suspension_events.
                //
                // Use `append_single_event` (parent-row FOR UPDATE + MAX(event_id)
                // re-read per insert), byte-for-byte like the plain
                // `persist_all_started_child_workflows` path — NOT a batch
                // `append_events` at the stale pre-handler `next_event_id`. The
                // child spawned by *this* batch is new and cannot complete
                // concurrently, but this same cycle may ALSO carry a
                // `CancelRaceLosers` for a still-RUNNING race-loser child
                // (`extract_child_timeout_race` deliberately tolerates it, so a
                // resolved `ctx.race()` immediately followed by
                // `spawn_child_workflow_timeout` lands here). That loser child can
                // append its OWN terminal (`ChildWorkflowCompleted`/`Failed`) onto
                // the parent via `wake_parent_for_child_completion`'s own
                // `append_single_event`, concurrently, at the same id. A stale
                // batch append would then collide on
                // UNIQUE(workflow_exec_id, event_id) and — routed through
                // `fail_execution_on_error` — terminally FAIL a healthy parent. The
                // FOR UPDATE re-read serialises the two appends instead (issue #779,
                // Codex round-12 P2).
                let mut timer_event = timer_is_new.then(|| WorkflowEvent::TimerStarted {
                    timer_id: timer.timer_id.clone(),
                    duration_secs: timer.duration_secs,
                });
                let mut child_started_event =
                    child_is_new.then(|| WorkflowEvent::ChildWorkflowStarted {
                        child_id: child.child_id,
                        workflow_name: child.workflow_name.clone(),
                        input: child.input.clone(),
                    });
                let events = build_suspension_events(commands, &mut [], |cmd| match cmd {
                    WorkflowCommand::StartChildWorkflow { .. } => child_started_event.take(),
                    WorkflowCommand::StartTimer { .. } => timer_event.take(),
                    _ => None,
                });
                for event in events {
                    store::append_single_event(conn, parent_exec_id, event).await?;
                }

                // Defensive: a suspension batch never carries CancelRaceLosers in
                // the common case, but a resolved `ctx.race()` in the same cycle
                // can (see above), so apply them for symmetry with
                // persist_all_started_child_workflows. No-op when absent. Re-read
                // the true next id under the same FOR UPDATE lock (the appends
                // above wrote a variable number of events) so the cursor never
                // reuses a consumed id.
                let mut race_next_event_id = store::next_event_id_for(conn, parent_exec_id).await?;
                let deferred = apply_race_loser_cancellations(
                    conn,
                    parent_exec_id,
                    commands,
                    &mut race_next_event_id,
                    registry,
                )
                .await?;

                // ── insert the child row + enqueue its task (only when new) ──
                if child_is_new {
                    insert_awaited_child_execution(
                        conn,
                        registry,
                        parent_execution,
                        parent_exec_id,
                        child,
                        child_trace_ctx,
                    )
                    .await?;
                }

                // ── arm the durable deadline timer (only when new) ──
                if timer_is_new {
                    let new_timer = NewHarvestTimer {
                        workflow_exec_id: parent_exec_id.as_uuid(),
                        timer_id: timer.timer_id.as_str(),
                        fires_at,
                    };
                    diesel::insert_into(harvest_timers::table)
                        .values(&new_timer)
                        .execute(conn)
                        .await
                        .map_err(crate::error::database_error)?;
                }

                // ── park the parent: PENDING at fires_at + mixed-signal sentinel ──
                // reschedule_task defers the row to fires_at (the timer fire). The
                // sentinel makes a child-terminal wake re-pend it early via the
                // second arm of primary_repend_workflow_task_query.
                //
                // NOTE: a child-timeout parked row deliberately reuses the
                // `mixed_signal_suspension` sentinel even though it has NO
                // WaitForSignal command — a future signal-wait edit keyed on this
                // exact literal must keep re-pending child-timeout rows too, or a
                // child-terminal wake would silently fail to pull this row forward.
                queue::reschedule_task(conn, task_id, fires_at).await?;
                diesel::update(queue_dsl::harvest_task_queue.find(task_id))
                    .set(queue_dsl::activity_name.eq(Some("mixed_signal_suspension".to_string())))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
                if sticky.is_some() {
                    queue::set_task_sticky_affinity(conn, task_id, sticky).await?;
                }

                Ok(deferred)
            }
            .scope_boxed()
        })
        .await?;

    for start in deferred {
        start.spawn();
    }

    // Self-wake re-check (R3): the child-completion wake
    // (`wake_parent_for_child_completion`/`_failure`) targets the parent task
    // row via `wake_workflow_task`. On the first live park the child was just
    // created in this same transaction, so it cannot be terminal yet. On a
    // re-park (both already started), a concurrent child terminal could commit
    // between our history check and this park's reschedule UPDATE — its
    // `wake_workflow_task` would then land on a row we re-parked over. Re-check
    // now that the park is committed: if the child is already terminal, re-wake
    // so the parent is not left sleeping until the deadline. If it is not
    // terminal yet, a later child terminal's own `wake_workflow_task` re-pends
    // the committed PENDING mixed-signal row — no gap.
    let child_terminal = harvest_workflow_executions::table
        .find(child_id.as_uuid())
        .select(harvest_workflow_executions::state)
        .first::<String>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .is_some_and(|s| {
            matches!(
                s.as_str(),
                "COMPLETED" | "FAILED" | "TIMED_OUT" | "CANCELLED" | "TERMINATED"
            )
        });
    if child_terminal {
        queue::wake_workflow_task(conn, parent_exec_id).await?;
    }

    Ok(())
}

/// Chronologically interleave due timer fires and pending signals for a
/// single wake-up ingest.
///
/// Recorded history order is the replay contract (issue #476): a signal
/// received **strictly before** a timer's deadline must be appended before
/// that timer's `TimerFired`, so a worker that claims the woken task late
/// (after `fires_at`) does not retroactively flip a signal-or-deadline race
/// to the timeout branch. A signal received at or after the deadline is
/// appended after the fire (ties go to the timer — the deadline was reached).
/// Relative order within each kind is preserved (`fires_at` for timers,
/// `received_at` for signals — the orders their loaders return).
fn merge_wake_events(
    due_timers: Vec<(TimerId, chrono::DateTime<chrono::Utc>)>,
    pending_signals: Vec<(String, serde_json::Value, chrono::DateTime<chrono::Utc>)>,
) -> Vec<WorkflowEvent> {
    let mut events = Vec::with_capacity(due_timers.len() + pending_signals.len());
    let mut timers = due_timers.into_iter().peekable();
    let mut signals = pending_signals.into_iter().peekable();

    loop {
        match (timers.peek(), signals.peek()) {
            (Some((_, fires_at)), Some((_, _, received_at))) => {
                if received_at < fires_at {
                    let (signal_name, payload, _) = signals.next().expect("peeked");
                    events.push(WorkflowEvent::SignalReceived {
                        signal_name,
                        payload,
                    });
                } else {
                    let (timer_id, _) = timers.next().expect("peeked");
                    events.push(WorkflowEvent::TimerFired { timer_id });
                }
            }
            (Some(_), None) => {
                let (timer_id, _) = timers.next().expect("peeked");
                events.push(WorkflowEvent::TimerFired { timer_id });
            }
            (None, Some(_)) => {
                let (signal_name, payload, _) = signals.next().expect("peeked");
                events.push(WorkflowEvent::SignalReceived {
                    signal_name,
                    payload,
                });
            }
            (None, None) => break,
        }
    }

    events
}

/// Ingest due timer fires and pending signals for a woken workflow task in a
/// single atomic batch, appending events in chronological occurrence order
/// (see [`merge_wake_events`]).
///
/// Returns the fired timer IDs and delivered signal names.
#[doc(hidden)] // exposed for the #779 transient-conflict integration test; not a stable API
pub async fn ingest_due_timers_and_signals(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    next_event_id: i32,
) -> HarvestResult<(Vec<TimerId>, Vec<String>)> {
    use crate::schema::harvest_timers::dsl;
    use diesel::dsl::sql;
    use diesel::sql_types::Timestamptz;

    let due_timers = dsl::harvest_timers
        .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(dsl::fired.eq(false))
        // Use the database clock here so timer replay stays consistent with the
        // queue claim path, which also uses Postgres NOW().
        .filter(dsl::fires_at.le(sql::<Timestamptz>("NOW()")))
        .order((dsl::fires_at.asc(), dsl::timer_id.asc()))
        .select(HarvestTimer::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let pending_signals = signal::load_pending_signals(conn, exec_id).await?;

    if due_timers.is_empty() && pending_signals.is_empty() {
        return Ok((vec![], vec![]));
    }

    let (timer_row_ids, timer_entries): (Vec<_>, Vec<_>) = due_timers
        .into_iter()
        .map(|timer| (timer.id, (TimerId::new(timer.timer_id), timer.fires_at)))
        .unzip();
    let fired_timer_ids: Vec<TimerId> = timer_entries.iter().map(|(id, _)| id.clone()).collect();

    let (signal_ids, signal_entries): (Vec<_>, Vec<_>) = pending_signals
        .into_iter()
        .map(|signal| {
            (
                signal.id,
                (signal.signal_name, signal.payload, signal.received_at),
            )
        })
        .unzip();
    let signal_names: Vec<String> = signal_entries
        .iter()
        .map(|(name, _, _)| name.clone())
        .collect();

    let events = merge_wake_events(timer_entries, signal_entries);

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &events, next_event_id).await?;
            if !timer_row_ids.is_empty() {
                diesel::update(dsl::harvest_timers.filter(dsl::id.eq_any(&timer_row_ids)))
                    .set(dsl::fired.eq(true))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            }
            if !signal_ids.is_empty() {
                signal::mark_signals_consumed(conn, &signal_ids).await?;
            }
            Ok(())
        }
        .scope_boxed()
    })
    .await?;

    Ok((fired_timer_ids, signal_names))
}

/// Re-drive the claimed parent workflow task after a transient wake-event-ingest
/// event-id conflict (issue #779), instead of terminally failing the run.
///
/// The task is currently claimed by this worker (`state = 'RUNNING'`,
/// `worker_id` set). We first [`park`](queue::park_workflow_task) it to release
/// the claim, then [`wake`](queue::wake_workflow_task) it to re-pend the row to
/// `PENDING` with an immediate `scheduled_at` and a `pg_notify`, so an idle
/// poller re-claims it promptly. This is the exact pause(park)/resume(wake)
/// mechanism used elsewhere; no event is appended, no state is changed, no
/// retry attempt is consumed.
///
/// `park`'s `wake_requested` return value is intentionally discarded: we always
/// wake immediately afterward on the same connection, so a wake that raced in
/// during the claim is subsumed by the wake we issue here.
async fn requeue_parent_on_transient_ingest_conflict(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
    sticky_timeout: Duration,
    exec_id: ExecutionId,
) -> HarvestResult<()> {
    let sticky = if sticky_timeout.is_zero() {
        None
    } else {
        Some(queue::StickyHint::new(worker_id, sticky_timeout))
    };
    let _ = queue::park_workflow_task(conn, task.id, sticky).await?;
    queue::wake_workflow_task(conn, exec_id).await?;
    Ok(())
}

/// Run the wake-event ingest ([`ingest_due_timers_and_signals`]), converting a
/// transient `(workflow_exec_id, event_id)` UNIQUE conflict into a re-drive of
/// the parent workflow task rather than a terminal failure (issue #779).
///
/// Returns:
/// - `Ok(Some((timers_fired, signals_delivered)))` — the ingest succeeded (or
///   was a no-op); proceed with this cycle.
/// - `Ok(None)` — a transient event-id conflict was detected; the parent task
///   has been re-pended for immediate re-claim and the caller must abandon this
///   cycle without failing the run.
/// - `Err(_)` — a genuine (non-conflict) error; the caller fails the execution
///   exactly as before.
///
/// The classification is scoped to the ingest boundary and is precise: only a
/// UNIQUE violation on the `harvest_events (workflow_exec_id, event_id)`
/// constraint is treated as transient (see
/// [`HarvestError::is_event_id_unique_violation`]). A successful ingest and any
/// genuine error are unaffected.
///
/// The retry is provably convergent: the winner's event is already committed, so
/// the re-driven task's fresh history load advances `next_event_id` past it, and
/// an already-`fired` timer is excluded from the next ingest — the same conflict
/// cannot recur, so there is no hot loop even though the re-pend schedules the
/// task for immediate re-claim.
#[doc(hidden)] // exposed for the #779 transient-conflict integration test; not a stable API
pub async fn ingest_wake_events_or_requeue(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
    sticky_timeout: Duration,
    exec_id: ExecutionId,
    next_event_id: i32,
) -> HarvestResult<Option<(Vec<TimerId>, Vec<String>)>> {
    match ingest_due_timers_and_signals(conn, exec_id, next_event_id).await {
        Ok(pair) => Ok(Some(pair)),
        Err(e) if e.is_event_id_unique_violation() => {
            requeue_parent_on_transient_ingest_conflict(
                conn,
                task,
                worker_id,
                sticky_timeout,
                exec_id,
            )
            .await?;
            tracing::warn!(
                task_id = %task.id,
                workflow_exec_id = %exec_id,
                "harvest: transient event-id conflict during wake-event ingest \
                 (a concurrent append committed the same event_id first); \
                 re-driving the parent workflow task instead of failing the run \
                 (issue #779)"
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

async fn fail_task_only(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    error: &str,
) -> HarvestResult<()> {
    queue::fail_task(conn, task_id, error).await
}

async fn fail_task_and_execution(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
    error: &str,
) -> HarvestResult<()> {
    let Some(exec_uuid) = task.workflow_exec_id else {
        return fail_task_only(conn, task.id, error).await;
    };

    let exec_id = execution_id_from_uuid(exec_uuid);
    let history = match store::load_history(conn, exec_id).await {
        Ok(h) => h,
        Err(history_error) => {
            tracing::warn!(
                task_id = %task.id,
                workflow_exec_id = %exec_id,
                error = %history_error,
                "failed to load workflow history while persisting task failure; updating rows without event append"
            );
            update_workflow_execution_failed(conn, exec_id, worker_id, error, None).await?;
            return queue::fail_task(conn, task.id, error).await;
        }
    };

    persist_workflow_failure(
        conn,
        task.id,
        exec_id,
        history.next_event_id,
        worker_id,
        error,
        None,
        None,
        None,
        None,
        None,
        crate::types::Priority::default(),
    )
    .await
    .map(|_| ())
}

async fn finalize_activity_completion(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    output: serde_json::Value,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
) -> HarvestResult<()> {
    let Some(activity_name) = task.activity_name.as_deref() else {
        return Ok(());
    };
    let completion_event = WorkflowEvent::ActivityCompleted {
        activity_id,
        output: output.clone(),
    };

    conn.transaction::<(), HarvestError, _>(|conn| {
        let output = output.clone();
        async move {
            let history = lock_workflow_execution_and_load_history(conn, exec_id).await?;
            if pending_activity_id_for_task(&history.events, task, activity_name)?.is_none() {
                return Ok(());
            }
            let Some(state) = task_state_for_update(conn, task.id).await? else {
                return Ok(());
            };
            if state != "RUNNING" {
                return Ok(());
            }
            store::append_events_offloaded(
                conn,
                exec_id,
                &[completion_event],
                history.next_event_id,
                offloader,
            )
            .await?;
            queue::complete_task(conn, task.id, output).await?;
            // Worker sessions (issue #606): a session member activity's
            // completion pushes the session's lease forward, so a
            // long-running but still-legitimate pipeline isn't reclaimed by
            // the broken-session scanner's `expires_at < NOW()` check just
            // because its steps individually outlast one sticky-timeout
            // window. `task.session_id` is `None` for both ordinary
            // activities and the reserved acquire/release activities
            // themselves (neither is dispatched through `Session::
            // execute_activity`), so this is scoped to genuine members only.
            if let Some(session_uuid) = task.session_id {
                crate::sessions::refresh_session_lease(
                    conn,
                    crate::types::SessionId::from_uuid(session_uuid),
                )
                .await?;
            }
            queue::wake_workflow_task(conn, exec_id).await
        }
        .scope_boxed()
    })
    .await
}

async fn finalize_activity_failure(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    error: &str,
) -> HarvestResult<()> {
    let Some(activity_name) = task.activity_name.as_deref() else {
        return Ok(());
    };
    let failure = parse_error_payload_full(error);
    let failed_event = WorkflowEvent::ActivityFailed {
        activity_id,
        error: failure.message,
        attempt: task_attempt(task),
        error_type: failure.error_type,
        non_retryable: failure.non_retryable,
        details: failure.details,
    };

    // NOTE: we deliberately do **not** insert a `harvest_dead_letters` row
    // here. The natural follow-up of `dlq::replay_dead_letter` on an
    // activity DLQ entry would re-enqueue an activity task with the same
    // `workflow_exec_id`/`activity_name`, but `process_activity_task` then
    // calls `find_pending_scheduled_activity`, which excludes scheduled
    // activities that already carry a terminal `ActivityFailed` event in
    // history. Inserting an un-replayable row would silently break the DLQ
    // contract. Workflow-level visibility is preserved via the
    // `ActivityFailed` event (carrying `error_type`, `non_retryable`,
    // `details`) and the `WorkflowFailed` event that follows when the
    // workflow propagates the error.
    conn.transaction::<(), HarvestError, _>(|conn| {
        let error = error.to_string();
        async move {
            let history = lock_workflow_execution_and_load_history(conn, exec_id).await?;
            if pending_activity_id_for_task(&history.events, task, activity_name)?.is_none() {
                return Ok(());
            }
            let Some(state) = task_state_for_update(conn, task.id).await? else {
                return Ok(());
            };
            if state != "RUNNING" {
                // If the task reached COMPLETED before the handler returned
                // (e.g. via run_transactional) and the handler then returned
                // Err, the error is discarded — the workflow already observed
                // ActivityCompleted.  Emit a warning so the misuse is visible.
                if state == "COMPLETED" {
                    tracing::warn!(
                        task_id = %task.id,
                        activity_name = %activity_name,
                        "activity handler returned Err but task is already COMPLETED \
                         (run_transactional committed it); the error is discarded and \
                         the workflow observes ActivityCompleted — run_transactional \
                         must be the final expression in the activity handler"
                    );
                }
                return Ok(());
            }
            store::append_events(conn, exec_id, &[failed_event], history.next_event_id).await?;
            queue::fail_task(conn, task.id, &error).await?;
            queue::wake_workflow_task(conn, exec_id).await
        }
        .scope_boxed()
    })
    .await
}

/// Materialize any DUE `__child_timeout:` deadline timers into the parent's
/// history as `TimerFired` events **before** an out-of-band child terminal is
/// appended (issue #779, Codex P1).
///
/// A child that completes/fails **after** its `spawn_child_workflow_timeout`
/// deadline has passed appends its terminal to the parent history out-of-band
/// (via [`wake_parent_for_child_completion`] / [`wake_parent_for_child_failure`]).
/// If the parent is claimed late, that terminal would land in history *before*
/// the deadline `TimerFired` (which is otherwise only ingested at parent claim,
/// see [`ingest_due_timers_and_signals`]). [`crate::replay::HistoryMatcher::match_child_or_timer`]
/// is a pure recorded-order matcher, so it would then declare the child the
/// winner and the primitive would return `Some`/`Err` instead of `None` —
/// violating the "deadline reached ⇒ `None`" contract even though the deadline
/// genuinely elapsed first. Ordering the due deadline **before** the child
/// terminal restores the #476 signal-or-deadline guarantee for the child case:
/// a deadline that has passed is observed by every replay regardless of when the
/// parent is claimed.
///
/// Correlation to the completing child is deliberately **not** required:
/// materializing every currently-due `__child_timeout` deadline before the
/// child terminal is correct — this child's own due deadline lands before its
/// terminal (⇒ `None`), and any unrelated sibling race's due deadline that
/// lands early is simply resolved by that race's own `match_child_or_timer`
/// scan. Scope is strictly `__child_timeout:` so unrelated `ctx.timer()` rows
/// are never pulled forward or reordered.
///
/// Double-fire safety vs the parent-claim ingest: the due rows are selected
/// `FOR UPDATE`, so `fired = false` is re-evaluated against the latest
/// committed row version (Postgres EvalPlanQual). A deadline the ingest path
/// already fired-and-committed is therefore excluded here, and a deadline this
/// path fires blocks the ingest's `SET fired = true` until the enclosing wake
/// transaction commits — so the two paths cannot durably append two
/// `TimerFired` events for the same timer. When no `__child_timeout` deadline
/// is due (a plain child-await, or a child that beat its deadline), this
/// returns `0` and appends nothing — byte-identical to the pre-#779 wake path.
#[doc(hidden)] // exposed for the #779 integration tests; not a stable API
pub async fn materialize_due_child_timeout_deadlines(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
) -> HarvestResult<usize> {
    use crate::schema::harvest_timers::dsl;
    use diesel::TextExpressionMethods;
    use diesel::dsl::sql;
    use diesel::sql_types::Timestamptz;

    // ── Per-table lock-ordering convention (issue #779, Codex round-11 review) ──
    //
    //   harvest_workflow_executions (parent row) → harvest_timers (FOR UPDATE)
    //
    // The execution row is the top-level aggregate lock everywhere in the engine
    // (every `store::append_single_event` takes it first), and the operator
    // cancel/terminate path — `execution::notify_awaited_parent_of_child_terminal`
    // — locks the parent execution row `FOR UPDATE` *before* calling this
    // materializer. This function's timer `FOR UPDATE` below therefore MUST be
    // acquired *after* the parent execution row, or it would be an ABBA inversion
    // against that operator path: the worker-wake and child-execution-timeout
    // callers reach the materializer *without* an outer parent lock, so if the
    // materializer took the timer lock first, two concurrent wakes of the same
    // overdue parent — one via a normal child completion/failure (timer-first)
    // and one via an operator cancel/terminate of a sibling child (execution-row
    // first) — would deadlock, and Postgres would abort a healthy terminal
    // notification. Taking the parent execution row `FOR UPDATE` here first
    // unifies every call site onto execution-row → timer, so no cycle is
    // possible. Re-locking a row the operator path already holds in the same
    // transaction is a no-op. If the parent execution row is gone, there is
    // nothing to fire a deadline against — return `Ok(0)` and let the caller's
    // own `append_single_event` surface the `NotFound` (unchanged behaviour).
    {
        use crate::models::WorkflowExecution;
        use crate::schema::harvest_workflow_executions;
        let parent_row: Option<WorkflowExecution> = harvest_workflow_executions::table
            .find(parent_exec_id.as_uuid())
            .for_update()
            .select(WorkflowExecution::as_select())
            .first(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;
        if parent_row.is_none() {
            return Ok(0);
        }
    }

    // Lock + read the parent's DUE child-timeout deadline timers. `FOR UPDATE`
    // makes the `fired = false` predicate exactly-once against the parent-claim
    // ingest (see the double-fire note above). The `\_\_child\_timeout:%`
    // pattern escapes the underscores (default Postgres LIKE escape char) so it
    // matches the literal `__child_timeout:` prefix and never a sibling prefix
    // such as `__signal_timeout:`. Database `NOW()` is used to stay consistent
    // with the ingest/claim clock.
    let due: Vec<HarvestTimer> = dsl::harvest_timers
        .filter(dsl::workflow_exec_id.eq(parent_exec_id.as_uuid()))
        .filter(dsl::fired.eq(false))
        .filter(dsl::timer_id.like(r"\_\_child\_timeout:%"))
        .filter(dsl::fires_at.le(sql::<Timestamptz>("NOW()")))
        .order(dsl::fires_at.asc())
        .for_update()
        .select(HarvestTimer::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    if due.is_empty() {
        return Ok(0);
    }

    let count = due.len();
    for timer in due {
        let timer_row_id = timer.id;
        // Append `TimerFired` under the same parent-row FOR UPDATE + MAX(event_id)
        // discipline the child-terminal append uses, so the deadline is ordered
        // chronologically ahead of the child terminal appended after this call.
        store::append_single_event(
            conn,
            parent_exec_id,
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new(timer.timer_id),
            },
        )
        .await?;
        diesel::update(dsl::harvest_timers.filter(dsl::id.eq(timer_row_id)))
            .set(dsl::fired.eq(true))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }

    Ok(count)
}

#[doc(hidden)] // exposed for the #779 integration tests; not a stable API
pub async fn wake_parent_for_child_completion(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    child_exec_id: ExecutionId,
    output: serde_json::Value,
) -> HarvestResult<()> {
    // #779 (Codex P1): order any DUE child-timeout deadline BEFORE the child
    // terminal so the pure recorded-order `match_child_or_timer` sees the
    // `TimerFired` first and resolves to the timeout branch (None) even when the
    // parent is claimed late (see materialize_due_child_timeout_deadlines).
    materialize_due_child_timeout_deadlines(conn, parent_exec_id).await?;
    // Use append_single_event (FOR UPDATE + MAX re-read) so concurrent sibling
    // child completions serialise around the parent execution row and cannot
    // collide on (workflow_exec_id, event_id).
    let event = WorkflowEvent::ChildWorkflowCompleted {
        child_id: child_exec_id,
        output,
    };
    store::append_single_event(conn, parent_exec_id, event).await?;
    queue::wake_workflow_task(conn, parent_exec_id).await
}

#[doc(hidden)] // exposed for the #779 integration tests; not a stable API
pub async fn wake_parent_for_child_failure(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    child_exec_id: ExecutionId,
    error: &str,
) -> HarvestResult<()> {
    // #779 (Codex P1): order any DUE child-timeout deadline BEFORE the child
    // terminal (mirrors wake_parent_for_child_completion) so an over-deadline
    // child FAILURE resolves to None, not Err.
    materialize_due_child_timeout_deadlines(conn, parent_exec_id).await?;
    // Decode the typed failure envelope (issue #767) so the parent's
    // `ChildWorkflowFailed` event carries the child's `error_type`/`details`/
    // `non_retryable`. The two seal-path callers pass engine reason strings,
    // which decode to all-None typed fields (legacy behaviour preserved).
    let decoded = crate::failure::decode_workflow_failure(error);
    let event = WorkflowEvent::child_workflow_failed_typed(child_exec_id, &decoded);
    store::append_single_event(conn, parent_exec_id, event).await?;
    queue::wake_workflow_task(conn, parent_exec_id).await
}

#[allow(clippy::too_many_arguments)]
async fn persist_child_workflow_completion(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    parent_exec_id: ExecutionId,
    output: serde_json::Value,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> HarvestResult<(ExecutionId, Option<String>)> {
    let event = WorkflowEvent::WorkflowCompleted {
        output: output.clone(),
    };

    let (deferred, closed_children) = conn
        .transaction::<_, HarvestError, _>(|conn| {
            let output = output.clone();
            async move {
                store::append_events(conn, exec_id, &[event], next_event_id).await?;
                update_workflow_execution_completed(conn, exec_id, worker_id, &output).await?;
                queue::complete_task(conn, task_id, output.clone()).await?;
                let (mut deferred, closed_children) =
                    apply_parent_close_cascade(conn, exec_id).await?;
                let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                    conn,
                    exec_id,
                    crate::completion_trigger::TerminalState::Completed,
                    metrics,
                )
                .await?;
                deferred.extend(triggers);
                wake_parent_for_child_completion(conn, parent_exec_id, exec_id, output).await?;
                Ok((deferred, closed_children))
            }
            .scope_boxed()
        })
        .await?;

    for start in deferred {
        start.spawn();
    }

    for (child_id, child_name) in closed_children {
        check_and_report_unfinished_handlers_for_worker(conn, child_id, Some(&child_name), metrics)
            .await;
    }

    Ok((exec_id, None))
}

#[allow(clippy::too_many_arguments)]
async fn persist_child_workflow_failure(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    parent_exec_id: ExecutionId,
    error: &str,
    nd_details: Option<&crate::error::NonDeterministicDetails>,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> HarvestResult<(ExecutionId, Option<String>)> {
    // Decode the child's typed failure envelope once (issue #767). The child's own
    // `WorkflowFailed` event carries the full typed fields; its `execution.error`
    // TEXT column and task reason carry the human `decoded.message` (never the
    // envelope JSON, AC4). The ORIGINAL raw `error` is forwarded to the parent so
    // the parent's `ChildWorkflowFailed` can recover the same typed fields.
    let decoded = crate::failure::decode_workflow_failure(error);
    let workflow_failure = WorkflowEvent::workflow_failed_typed(&decoded);

    let (deferred, closed_children) = conn
        .transaction::<_, HarvestError, _>(|conn| {
            let raw_error = error.to_string();
            let message = decoded.message.clone();
            async move {
                store::append_events(conn, exec_id, &[workflow_failure], next_event_id).await?;
                update_workflow_execution_failed(conn, exec_id, worker_id, &message, nd_details)
                    .await?;
                queue::fail_task(conn, task_id, &message).await?;
                let (mut deferred, closed_children) =
                    apply_parent_close_cascade(conn, exec_id).await?;
                let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                    conn,
                    exec_id,
                    crate::completion_trigger::TerminalState::Failed,
                    metrics,
                )
                .await?;
                deferred.extend(triggers);
                wake_parent_for_child_failure(conn, parent_exec_id, exec_id, &raw_error).await?;
                Ok((deferred, closed_children))
            }
            .scope_boxed()
        })
        .await?;

    for start in deferred {
        start.spawn();
    }

    for (child_id, child_name) in closed_children {
        check_and_report_unfinished_handlers_for_worker(conn, child_id, Some(&child_name), metrics)
            .await;
    }

    Ok((exec_id, None))
}

/// Perform the DB side-effects for all `SpawnDetachedChildWorkflow` commands in
/// `commands`: insert a child execution row, start the child's event log with
/// `WorkflowStarted`, and enqueue the child task.
///
/// The `ChildWorkflowSpawnedDetached` event written to the **parent** history is
/// handled separately by `pre_suspension_events_from_commands` so that it lands
/// at the correct position relative to `RecordMarker` events.
/// Callers must invoke this inside the same transaction that appends those
/// parent events, before the child task can be observed by another worker.
///
/// Non-detached-spawn commands in `commands` are silently skipped. Already-existing
/// child rows (idempotent re-run after a crash) are also skipped.
#[allow(clippy::too_many_lines)]
async fn create_detached_child_executions(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    parent_execution: &WorkflowExecution,
    commands: &[WorkflowCommand],
    execute_span: &tracing::Span,
) -> HarvestResult<()> {
    // Provenance ref for every detached child is the parent execution id (#740).
    let parent_exec_id_str = parent_execution.id.to_string();
    for cmd in commands {
        let WorkflowCommand::SpawnDetachedChildWorkflow {
            child_id,
            workflow_name,
            input,
            parent_close_policy,
        } = cmd
        else {
            continue;
        };

        if !registry.workflows.contains_key(workflow_name.as_str()) {
            return Err(HarvestError::Config(format!(
                "no workflow handler registered for '{workflow_name}'"
            )));
        }

        // Idempotent: skip if already created (crash-restart replay).
        let already_exists: bool = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq(child_id.as_uuid()))
            .count()
            .get_result::<i64>(conn)
            .await
            .map_err(crate::error::database_error)?
            > 0;
        if already_exists {
            continue;
        }

        let child_workflow_id = child_id.to_string();
        let detached_wf_info = registry.workflows.get(workflow_name.as_str());
        let (owner, runbook_url, severity, child_sla, detached_retry_policy) = detached_wf_info
            .map_or((None, None, None, None, None), |w| {
                (
                    w.owner,
                    w.runbook_url,
                    w.severity,
                    w.sla,
                    w.retry_policy.clone(),
                )
            });
        // Clamp the detached child's retry policy by the server-side ceiling (issue #523).
        // Detached children bypass StartWorkflowParams where the ceiling is normally applied,
        // so we apply it here before serializing the policy to the execution row.
        let detached_retry_policy = detached_retry_policy.map(|mut p| {
            if let Some(ceiling) = registry.max_workflow_attempts_ceiling {
                p.max_attempts = p.max_attempts.min(ceiling);
            }
            p
        });
        let child_sla = child_sla.and_then(|d| chrono::Duration::from_std(d).ok());
        let child_sla_deadline_at = child_sla.map(|d| chrono::Utc::now() + d);
        let child_row = NewWorkflowExecution {
            continued_from_exec_id: None,
            first_exec_id: None,
            id: child_id.as_uuid(),
            workflow_name: workflow_name.as_str(),
            workflow_id: &child_workflow_id,
            run_id: uuid::Uuid::new_v4(),
            shard_id: parent_execution.shard_id,
            input: input.clone(),
            parent_id: Some(parent_execution.id),
            queue_name: &parent_execution.queue_name,
            execution_timeout: None,
            deadline_at: None,
            sla: child_sla,
            sla_deadline_at: child_sla_deadline_at,
            memo: None,
            search_attrs: None,
            assigned_build_id: parent_execution.assigned_build_id.clone(),
            parent_close_policy: Some(parent_close_policy.as_str().to_string()),
            owner,
            runbook_url,
            severity,
            context_headers: parent_execution.context_headers.clone(),
            schedule_id: None, // detached child workflows are not scheduled fires
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: detached_retry_policy
                .and_then(|p| serde_json::to_value(&p).ok()),
            retry_of_exec_id: None,
            origin: None, // detached child workflow, not a schedule fire (issue #534)
            // Detached children get only builder-wide default callback
            // targets (issue #605) — no per-execution override here.
            completion_callbacks: None,
            start_source: Some(crate::types::StartSource::Child.as_str()),
            start_source_ref: Some(parent_exec_id_str.as_str()),
            started_by: None,
        };

        diesel::insert_into(harvest_workflow_executions::table)
            .values(&child_row)
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

        // Start child history.
        // Note: the ChildWorkflowSpawnedDetached event for the PARENT history is
        // written by the caller's pre_suspension_events_from_commands batch so
        // that it appears at the correct position relative to RecordMarker events.
        store::append_events(
            conn,
            *child_id,
            &[WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None, // child workflows are not scheduler-fired
            }],
            0,
        )
        .await?;

        // Enqueue child task.
        let mut params = queue::EnqueueParams::new(
            parent_execution.queue_name.clone(),
            TaskType::Workflow,
            input.clone(),
        );
        params.workflow_exec_id = Some(child_id.as_uuid());
        params.required_build_id = parent_execution.assigned_build_id.clone();
        (params.concurrency_key, params.max_concurrent) =
            resolve_workflow_concurrency(registry, workflow_name, input);
        params.trace_context = tracing::info_span!(
            parent: execute_span,
            "harvest.child_workflow.start",
            "otel.kind" = "producer",
            { ATTR_WORKFLOW_ID } = %workflow_name,
            { ATTR_EXECUTION_ID } = %child_id,
            { ATTR_SHARD_ID } = parent_execution.shard_id,
        )
        .in_scope(|| registry.telemetry().capture_trace_context());
        queue::enqueue(conn, &params).await?;
    }

    Ok(())
}

/// Poll the task queue row for `task_id` until its state leaves `RUNNING`,
/// at which point the caller should treat the activity as cancelled.
///
/// Transient DB errors are retried silently; only a state transition (or
/// row deletion) resolves the future.
async fn observe_task_cancellation(pool: &DbPool, task_id: uuid::Uuid) {
    use crate::schema::harvest_task_queue::dsl;

    const POLL_INTERVAL: Duration = Duration::from_millis(500);

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let Ok(mut conn) = pool.get().await else {
            continue;
        };

        let row = dsl::harvest_task_queue
            .find(task_id)
            .select(dsl::state)
            .first::<String>(&mut conn)
            .await
            .optional();

        if let Ok(Some(state)) = &row
            && state == "RUNNING"
        {
            continue;
        }
        if row.is_ok() {
            return;
        }
    }
}

/// Returns `true` when the cross-retry wall-clock deadline would be exceeded
/// before the next retry attempt could start (issue #378).
///
/// Pure so both the claim-time snapshot check and the in-transaction fresh
/// re-check (issue #609 post-review hardening) share one decision rule.
fn deadline_would_be_exceeded(
    deadline: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
    retry_delay: chrono::Duration,
) -> bool {
    deadline.is_some_and(|d| now + retry_delay >= d)
}

/// Returns `true` when the cross-retry wall-clock deadline would be exceeded
/// before the next retry attempt could start.
///
/// Used in the retry path to short-circuit requeue and instead emit a
/// `ScheduleToClose` timeout (issue #378). Evaluates the **claim-time task
/// snapshot** — since a pause/resume cycle (issue #609) can shift the row's
/// deadline while an attempt is in flight, a positive answer here is only a
/// candidate: [`record_schedule_to_close_activity_timeout`] re-validates
/// against the fresh row value under the execution row lock before failing
/// the task terminally.
fn schedule_to_close_deadline_exceeded(
    task: &TaskQueueItem,
    retry_delay: chrono::Duration,
) -> bool {
    deadline_would_be_exceeded(task.schedule_to_close_at, chrono::Utc::now(), retry_delay)
}

/// Non-locking read of whether the owning execution is currently `PAUSED`.
///
/// Gates the retry path's `schedule_to_close` deadline check (issue #609,
/// AC5): pause suspends the cross-retry deadline clock, so a failing activity
/// of a paused execution is requeued normally instead of deadline-failed —
/// resume shifts `schedule_to_close_at` forward by the pause span, and the
/// requeued row is `PENDING` so the shift covers it. This read is a fast-path
/// optimization only, not the guarantee: a pause committing after this read
/// is caught by [`record_schedule_to_close_activity_timeout`]'s authoritative
/// re-check of the execution state under the execution row lock (which
/// serializes against `pause_workflow_execution`'s own lock), so the race
/// window this unlocked read leaves open is closed there. Scoped: this extra
/// indexed read runs only when the task carries a deadline *and* that
/// deadline check already failed.
async fn owning_execution_is_paused(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<bool> {
    use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
    let state: Option<String> = exec_dsl::harvest_workflow_executions
        .find(exec_id.as_uuid())
        .select(exec_dsl::state)
        .first::<String>(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    Ok(state.as_deref() == Some("PAUSED"))
}

/// Outcome of the retry path's `schedule_to_close` deadline enforcement
/// attempt (issue #609 post-review hardening).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduleToCloseTimeoutOutcome {
    /// The timeout was recorded — or the task was concurrently resolved by
    /// another writer — so the caller must not requeue.
    Handled,
    /// The row-current deadline is no longer exceeded: a pause/resume cycle
    /// that completed while this attempt was in flight shifted
    /// `schedule_to_close_at` forward by the pause span (issue #609, AC5).
    /// The claim-time snapshot is stale; the caller must fall through to a
    /// normal retry requeue instead of failing the task terminally.
    DeadlineShifted,
    /// The owning execution is `PAUSED` as observed under the execution row
    /// lock (issue #609 post-review hardening, round 2): a pause committed
    /// after the caller's non-locking [`owning_execution_is_paused`] fast
    /// path read. Pause suspends the `schedule_to_close` clock, so the
    /// caller must fall through to a normal retry requeue — the requeued
    /// `PENDING` row is frozen by the claim gate and its deadline is shifted
    /// forward by the pause span on resume.
    ExecutionPaused,
}

/// Pure decision rule for the re-check
/// [`record_schedule_to_close_activity_timeout`] performs under the execution
/// row lock (issue #609 post-review hardening). `Some(outcome)` means bail
/// out without mutating anything; `None` means the deadline enforcement
/// proceeds (append `ActivityTimedOut { ScheduleToClose }` + fail the task).
///
/// Ordering matters: a concurrently-resolved task row wins over everything
/// (the caller must never requeue it), then a `PAUSED` owning execution wins
/// over the deadline arithmetic (pause suspends the clock even when the
/// row-current deadline is already exceeded — the resume-time shift will push
/// it forward), then the fresh-deadline check catches a stale claim-time
/// snapshot after a completed pause/resume cycle.
fn schedule_to_close_recheck_outcome(
    execution_state: &str,
    task_row: Option<&(String, Option<chrono::DateTime<chrono::Utc>>)>,
    now: chrono::DateTime<chrono::Utc>,
    retry_delay: chrono::Duration,
) -> Option<ScheduleToCloseTimeoutOutcome> {
    let Some((task_state, fresh_deadline)) = task_row else {
        return Some(ScheduleToCloseTimeoutOutcome::Handled);
    };
    if task_state != "RUNNING" {
        return Some(ScheduleToCloseTimeoutOutcome::Handled);
    }
    if execution_state == "PAUSED" {
        return Some(ScheduleToCloseTimeoutOutcome::ExecutionPaused);
    }
    if !deadline_would_be_exceeded(*fresh_deadline, now, retry_delay) {
        return Some(ScheduleToCloseTimeoutOutcome::DeadlineShifted);
    }
    None
}

/// Locked (`FOR UPDATE`) read of a task row's current state and
/// `schedule_to_close_at`, so the retry path's deadline decision is made
/// against the row-current value rather than the claim-time snapshot.
async fn task_state_and_deadline_for_update(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
) -> HarvestResult<Option<(String, Option<chrono::DateTime<chrono::Utc>>)>> {
    use crate::schema::harvest_task_queue::dsl;

    dsl::harvest_task_queue
        .find(task_id)
        .for_update()
        .select((dsl::state, dsl::schedule_to_close_at))
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)
}

/// Append `ActivityTimedOut { ScheduleToClose }` and fail the task row.
///
/// Called from the retry path when the cross-retry wall-clock deadline
/// (`schedule_to_close_at`) of the **claim-time snapshot** would be exceeded
/// before the next attempt starts.
///
/// The snapshot can be stale (issue #609 post-review hardening): the pause
/// primitive introduced the first post-enqueue mutator of
/// `schedule_to_close_at` — `resume_workflow_execution` shifts it forward by
/// the pause span. An attempt claimed before (or during) a pause that fails
/// after the matching resume would otherwise be deadline-failed against the
/// pre-shift value, charging paused wall-clock to the activity budget (the
/// exact outcome AC5 exists to prevent). This function therefore re-reads
/// the row's current deadline **inside** its transaction — which takes the
/// execution row lock via [`lock_workflow_execution_row_and_load_history`],
/// the same lock `resume_workflow_execution` holds while shifting, so the
/// read observes either the pre- or post-shift value, never a torn one — and
/// returns [`ScheduleToCloseTimeoutOutcome::DeadlineShifted`] without
/// mutating anything when the fresh deadline is no longer exceeded.
///
/// The caller's `owning_execution_is_paused` gate is likewise a non-locking
/// read taken *before* this transaction opens (issue #609 post-review
/// hardening, round 2): a pause committing in the gap would be observed here
/// as a now-`PAUSED` execution whose deadline is genuinely exceeded, and
/// deadline-failing it mid-pause would violate the pause-suspends-the-clock
/// contract. The locked execution row (free — the lock helper loads it
/// anyway) is therefore re-checked for `PAUSED`, returning
/// [`ScheduleToCloseTimeoutOutcome::ExecutionPaused`] without mutating
/// anything so the caller requeues normally. The whole decision is the pure
/// [`schedule_to_close_recheck_outcome`].
async fn record_schedule_to_close_activity_timeout(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    retry_delay: chrono::Duration,
) -> HarvestResult<ScheduleToCloseTimeoutOutcome> {
    let error = HarvestError::Timeout {
        timeout_type: crate::error::TimeoutType::ScheduleToClose,
        task_name: task
            .activity_name
            .clone()
            .unwrap_or_else(|| task.task_type.clone()),
    }
    .to_string();

    conn.transaction::<ScheduleToCloseTimeoutOutcome, HarvestError, _>(|conn| {
        let error = error.clone();
        async move {
            let (execution, history) =
                lock_workflow_execution_row_and_load_history(conn, exec_id).await?;
            let task_row = task_state_and_deadline_for_update(conn, task.id).await?;
            // Authoritative re-check under the execution row lock: bail
            // without mutation when the task was concurrently resolved, the
            // owning execution was paused after the caller's non-locking
            // fast-path read, or a concurrent resume shifted the deadline
            // into the future (this attempt still has budget).
            if let Some(outcome) = schedule_to_close_recheck_outcome(
                &execution.state,
                task_row.as_ref(),
                chrono::Utc::now(),
                retry_delay,
            ) {
                return Ok(outcome);
            }
            let timeout_event = WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type: crate::error::TimeoutType::ScheduleToClose,
            };
            store::append_events(conn, exec_id, &[timeout_event], history.next_event_id).await?;
            queue::fail_task(conn, task.id, &error).await?;
            queue::wake_workflow_task(conn, exec_id).await?;
            Ok(ScheduleToCloseTimeoutOutcome::Handled)
        }
        .scope_boxed()
    })
    .await
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
async fn handle_activity_result(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    worker_id: &str,
    retry_policy: Option<&crate::policy::RetryPolicy>,
    activity_result: Result<serde_json::Value, String>,
    max_result_bytes: u64,
    activity_name_for_cap: &str,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
    metrics: &dyn crate::telemetry::MetricsRecorder,
) -> HarvestResult<()> {
    match activity_result {
        Ok(output) => {
            let observed_bytes = serde_json::to_string(&output).map_or(0, |s| s.len() as u64);
            // Issue #524: an over-threshold result will be offloaded into a tiny
            // reference envelope, so it does not trip the #252 result cap.
            let offload_applies = offloader.is_some_and(|o| observed_bytes > o.threshold());
            if max_result_bytes > 0 && observed_bytes > max_result_bytes && !offload_applies {
                use crate::failure::IntoActivityErrorString as _;
                let error = crate::failure::ActivityFailure::non_retryable(
                    "PayloadTooLarge",
                    format!(
                        "activity '{activity_name_for_cap}' result exceeds cap: \
                         {observed_bytes} bytes (cap {max_result_bytes} bytes)"
                    ),
                )
                .into_error_payload();
                return finalize_activity_failure(conn, task, exec_id, activity_id, &error).await;
            }
            finalize_activity_completion(conn, task, exec_id, activity_id, output, offloader).await
        }
        Err(error) => {
            // Issue #782: emit the panic counter once per panicking attempt
            // (before the retry/terminal split). A contained activity panic is
            // classified by the typed HandlerPanic error type in the envelope;
            // it otherwise flows through the ordinary retryable-failure path.
            if crate::failure::parse_error_payload_full(&error).error_type
                == crate::failure::ERROR_TYPE_HANDLER_PANIC
            {
                metrics.record_activity_panic(activity_name_for_cap, &task.queue_name);
            }
            let delay_result = next_retry_delay(task, &error, retry_policy);
            let delay = fail_execution_on_error(conn, task, worker_id, delay_result).await?;

            if let Some(delay) = delay {
                // Pre-retry deadline check (issue #378): if the schedule_to_close
                // wall-clock deadline would be exceeded before the next attempt
                // starts, fail with ScheduleToClose instead of requeuing — unless
                // the owning execution is PAUSED (issue #609, AC5): the pause
                // suspends the deadline clock, so requeue normally and let the
                // resume-time shift push the deadline forward.
                if schedule_to_close_deadline_exceeded(task, delay)
                    && !owning_execution_is_paused(conn, exec_id).await?
                {
                    match record_schedule_to_close_activity_timeout(
                        conn,
                        task,
                        exec_id,
                        activity_id,
                        delay,
                    )
                    .await?
                    {
                        ScheduleToCloseTimeoutOutcome::Handled => return Ok(()),
                        // Stale claim-time snapshot: a concurrent pause/resume
                        // cycle shifted the row's deadline forward (issue #609
                        // post-review hardening) — the attempt still has
                        // budget, so fall through to the normal requeue below.
                        // ExecutionPaused is the sibling race (a pause
                        // committing after the non-locking gate above, caught
                        // under the execution row lock): pause suspends the
                        // deadline clock, so requeue normally — the PENDING
                        // row is frozen by the claim gate and its deadline is
                        // shifted forward on resume.
                        ScheduleToCloseTimeoutOutcome::DeadlineShifted
                        | ScheduleToCloseTimeoutOutcome::ExecutionPaused => {}
                    }
                }
                // Store the human-readable error for ActivityContext::previous_failure()
                // on the next attempt. Typed payloads are unwrapped to their message.
                let previous_error = crate::failure::parse_error_payload_full(&error).message;
                // AC2 (issue #528): retry counter — fires only after the
                // schedule_to_close deadline check passes (so a deadline-killed
                // attempt is NOT counted as a scheduled retry) and only when the
                // DB requeue actually succeeds (avoids inflating the counter on
                // transient DB errors or stale task state).
                let result = queue::requeue_for_retry(conn, task.id, delay, &previous_error).await;
                if result.is_ok() {
                    metrics.record_activity_retried(activity_name_for_cap, &task.queue_name);
                }
                return result;
            }

            finalize_activity_failure(conn, task, exec_id, activity_id, &error).await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_activity_future_with_cancellation(
    activity_name: &str,
    task_id: uuid::Uuid,
    cancellation_grace_period: Duration,
    activity_future: &mut (
             dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + Unpin
         ),
    mut cancellation_observer: impl std::future::Future<Output = ()> + Send + Unpin,
    cancel: tokio_util::sync::CancellationToken,
    span: tracing::Span,
) -> Result<serde_json::Value, String> {
    use tracing::Instrument;
    async {
        tokio::select! {
            biased;
            result = &mut *activity_future => result,
            () = &mut cancellation_observer => {
                cancel.cancel();
                tracing::info!(
                    task_id = %task_id,
                    activity = %activity_name,
                    grace_period_ms = %cancellation_grace_period.as_millis(),
                    "workflow cancellation detected for running activity; awaiting cooperative unwind"
                );
                tokio::time::timeout(cancellation_grace_period, activity_future)
                    .await
                    .unwrap_or_else(|_| {
                        tracing::warn!(
                            task_id = %task_id,
                            activity = %activity_name,
                            grace_period_ms = %cancellation_grace_period.as_millis(),
                            "activity ignored cancellation; hard-aborting handler"
                        );
                        Err(format!(
                            "workflow cancelled: activity '{activity_name}' exceeded {}ms cancellation grace period",
                            cancellation_grace_period.as_millis()
                        ))
                    })
            }
        }
    }
    .instrument(span)
    .await
}

/// Fallback defer delay when a rate-limited circuit-breaker activity has no
/// configured `rate_limit_rps` to derive a one-token refill interval from.
const RATE_LIMIT_DEFER_FALLBACK: Duration = Duration::from_millis(250);
/// Lower clamp on the dispatch-time rate-limit defer delay, so a very high RPS
/// can't spin the reschedule loop hot.
const RATE_LIMIT_DEFER_MIN: Duration = Duration::from_millis(50);
/// Upper clamp on the dispatch-time rate-limit defer delay, so a very low RPS
/// still re-evaluates `on_dispatch` (the breaker may have changed) reasonably soon.
const RATE_LIMIT_DEFER_MAX: Duration = Duration::from_secs(5);

/// Append `ActivityTimedOut { ScheduleToStart }` and fail a session-acquire
/// task whose `SessionOptions::acquisition_timeout` has elapsed (issue
/// #606). Mirrors [`record_schedule_to_close_activity_timeout`]'s shape.
/// Reaches `WorkflowContext::create_session`'s existing
/// `HarvestError::Timeout { timeout_type: ScheduleToStart, .. }` mapping
/// arm, surfacing `HarvestError::SessionAcquireTimeout` to the workflow.
async fn record_session_acquire_schedule_to_start_timeout(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
) -> HarvestResult<()> {
    let error = HarvestError::Timeout {
        timeout_type: crate::error::TimeoutType::ScheduleToStart,
        task_name: task
            .activity_name
            .clone()
            .unwrap_or_else(|| task.task_type.clone()),
    }
    .to_string();

    conn.transaction::<(), HarvestError, _>(|conn| {
        let error = error.clone();
        async move {
            let history = lock_workflow_execution_and_load_history(conn, exec_id).await?;
            let Some(state) = task_state_for_update(conn, task.id).await? else {
                return Ok(());
            };
            if state != "RUNNING" {
                return Ok(());
            }
            let timeout_event = WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type: crate::error::TimeoutType::ScheduleToStart,
            };
            store::append_events(conn, exec_id, &[timeout_event], history.next_event_id).await?;
            queue::fail_task(conn, task.id, &error).await?;
            queue::wake_workflow_task(conn, exec_id).await
        }
        .scope_boxed()
    })
    .await
}

/// Handle the internal session-acquire activity (issue #606) -- see the
/// interception in [`process_activity_task`].
///
/// On winning the in-process slot race: writes a `harvest_sessions`
/// `ACTIVE` row and completes the task with the host worker id as output --
/// the value `WorkflowContext::dispatch_session_acquire` decodes into the
/// session's physical binding. On losing the race (this worker is at
/// capacity, or sessions are disabled): reschedules the task with a
/// randomized backoff so it can be claimed by a different worker with a
/// free slot, without consuming a retry attempt.
async fn handle_session_acquire(
    pool: &DbPool,
    task: &TaskQueueItem,
    worker_id: &str,
    exec_id: ExecutionId,
    max_concurrent_sessions: i32,
    session_slots_in_use: &crate::sessions::SessionSlotRegistry,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<()> {
    let mut conn = pool.get().await.map_err(crate::error::database_error)?;

    let Some(activity_uuid) = task.activity_id else {
        return fail_task_only(
            &mut conn,
            task.id,
            "session-acquire task missing activity_id",
        )
        .await;
    };
    let activity_id = ActivityExecId::from_uuid(activity_uuid);

    let Some(session_id) = task
        .input
        .as_str()
        .and_then(|s| s.parse::<crate::types::SessionId>().ok())
    else {
        let error = "session-acquire task input is not a valid SessionId".to_string();
        fail_task_and_execution(&mut conn, task, worker_id, &error).await?;
        return Err(HarvestError::Config(error));
    };

    // Bound total elapsed time against `options.acquisition_timeout`
    // (carried as this task's `schedule_to_start`) *before* attempting to
    // claim a slot at all -- not only when this worker happens to be full.
    // `queue::claim_task` never filters out schedule-to-start-expired rows,
    // so a worker with a free slot that claims this task after the deadline
    // already elapsed (e.g. no session-capable worker was polling for a
    // while, or the timeout scanner hasn't caught up yet) must still reject
    // it rather than silently succeeding (issue #929 review). Comparing
    // directly against the task's original eligibility anchor (`created_at`,
    // falling back to `scheduled_at` for pre-#501 rows) also keeps the
    // deadline fixed regardless of how many times this task has deferred --
    // `defer_rate_limited_task` rewrites `scheduled_at` to `now + backoff`
    // on every call, and the generic schedule-to-start timeout scanner
    // computes its deadline as `scheduled_at + schedule_to_start`, so
    // relying on that scanner here would let every defer push the effective
    // deadline forward, making `acquisition_timeout` unbounded whenever
    // every worker on the queue is at session capacity (or has the default
    // `max_concurrent_sessions = 0`) for longer than one backoff interval.
    let eligible_since = task.created_at.unwrap_or(task.scheduled_at);
    let acquisition_timeout_exceeded = task
        .schedule_to_start
        .is_some_and(|budget| chrono::Utc::now() >= eligible_since + budget);
    if acquisition_timeout_exceeded {
        return record_session_acquire_schedule_to_start_timeout(
            &mut conn,
            task,
            exec_id,
            activity_id,
        )
        .await;
    }

    if !crate::sessions::try_acquire_session_slot(
        session_slots_in_use,
        max_concurrent_sessions,
        session_id,
    ) {
        // Lost the race: this worker is at its advertised session
        // capacity (or sessions are disabled: max_concurrent_sessions <= 0,
        // so every acquire loses). Reschedule with a randomized backoff so
        // the task can be claimed by a different, less-loaded worker --
        // this is not a failed attempt, so it must not consume the retry
        // budget (mirrors the rate-limit defer path, which has the
        // identical "not a real attempt" contract).
        let backoff = crate::sessions::acquire_retry_backoff(
            rand::random::<f64>(),
            crate::sessions::ACQUIRE_RETRY_BACKOFF_MIN,
            crate::sessions::ACQUIRE_RETRY_BACKOFF_MAX,
        );
        let scheduled_at = chrono::Utc::now()
            + chrono::Duration::from_std(backoff)
                .unwrap_or_else(|_| chrono::Duration::milliseconds(200));
        return queue::defer_rate_limited_task(&mut conn, task.id, scheduled_at).await;
    }

    let expires_at = chrono::Utc::now()
        + chrono::Duration::from_std(crate::sessions::SESSION_MEMBER_STICKY_TIMEOUT)
            .unwrap_or_else(|_| chrono::Duration::hours(24));
    let actual_host = match crate::sessions::record_session_acquired(
        &mut conn,
        session_id,
        exec_id,
        worker_id,
        &task.queue_name,
        expires_at,
    )
    .await
    {
        Ok(crate::sessions::SessionAcquireRecordOutcome::Active { host_worker_id }) => {
            host_worker_id
        }
        Ok(crate::sessions::SessionAcquireRecordOutcome::NotActive { reason }) => {
            // The session already left ACTIVE (e.g. the broken-session
            // scanner reclaimed it) before this retried acquire resolved --
            // release the slot just claimed (it was never actually granted)
            // and fail the *activity*, not the whole workflow, with a
            // typed, non-retryable SessionBroken failure. This reaches the
            // `create_session` error-mapping arm that maps
            // `ActivityFailed{error_type == ERROR_TYPE_SESSION_BROKEN}` to
            // `HarvestError::SessionBroken`, letting the workflow author
            // re-establish a fresh session instead of hard-pinning member
            // activities to a host already known to be dead/broken forever.
            crate::sessions::release_session_slot(session_slots_in_use, session_id);
            metrics.record_session_acquisition(
                &task.queue_name,
                crate::telemetry::SessionAcquisitionOutcome::Broken,
            );
            let payload = crate::failure::IntoActivityErrorString::into_error_payload(
                crate::failure::ActivityFailure::non_retryable(
                    crate::failure::ERROR_TYPE_SESSION_BROKEN,
                    format!(
                        "session {session_id} was already broken before acquisition resolved: \
                         {reason}"
                    ),
                ),
            );
            return finalize_activity_failure(&mut conn, task, exec_id, activity_id, &payload)
                .await;
        }
        Err(error) => {
            // Failed to durably record the session -- release the slot just
            // claimed (never leak it) and fail the task so the workflow
            // observes an ordinary activity failure instead of silently
            // wedging. The periodic reconciler (`reconcile_local_sessions`) is
            // a backstop for this same release should it ever be missed (e.g.
            // a crash between the DB error and this line).
            crate::sessions::release_session_slot(session_slots_in_use, session_id);
            let msg = error.to_string();
            fail_task_and_execution(&mut conn, task, worker_id, &msg).await?;
            return Err(error);
        }
    };
    if actual_host != worker_id {
        // Split-brain guard: `record_session_acquired` is idempotent
        // (`ON CONFLICT (id) DO NOTHING`), so a session-acquire task can be
        // (re)claimed and processed more than once for the same
        // `session_id` -- e.g. the poison-pill reclaimer requeues this task
        // after its earlier host crashed between committing the
        // `harvest_sessions` row and completing the task. When the
        // durably-recorded host is not *this* worker, this attempt never
        // actually won the session -- release the local slot just claimed
        // (it was speculative, not a real grant) rather than leaking it.
        crate::sessions::release_session_slot(session_slots_in_use, session_id);
    }
    metrics.record_session_acquisition(
        &task.queue_name,
        crate::telemetry::SessionAcquisitionOutcome::Acquired,
    );
    let output = serde_json::json!(actual_host);
    finalize_activity_completion(&mut conn, task, exec_id, activity_id, output, None).await
}

/// Handle the internal session-release activity (issue #606), dispatched by
/// [`crate::context::Session::complete`] and hard-pinned to the session's
/// host worker.
///
/// Marks the `harvest_sessions` row `COMPLETED` and frees the in-process
/// slot before completing the task normally.
async fn handle_session_release(
    pool: &DbPool,
    task: &TaskQueueItem,
    worker_id: &str,
    exec_id: ExecutionId,
    session_slots_in_use: &crate::sessions::SessionSlotRegistry,
) -> HarvestResult<()> {
    let mut conn = pool.get().await.map_err(crate::error::database_error)?;

    let Some(activity_uuid) = task.activity_id else {
        return fail_task_only(
            &mut conn,
            task.id,
            "session-release task missing activity_id",
        )
        .await;
    };
    let activity_id = ActivityExecId::from_uuid(activity_uuid);

    let Some(session_id) = task
        .input
        .as_str()
        .and_then(|s| s.parse::<crate::types::SessionId>().ok())
    else {
        let error = "session-release task input is not a valid SessionId".to_string();
        fail_task_and_execution(&mut conn, task, worker_id, &error).await?;
        return Err(HarvestError::Config(error));
    };

    if let Err(error) = crate::sessions::record_session_completed(&mut conn, session_id).await {
        let msg = error.to_string();
        fail_task_and_execution(&mut conn, task, worker_id, &msg).await?;
        return Err(error);
    }

    // Frees the in-process slot for a future acquire. Idempotent -- removing
    // an id that isn't present (e.g. a defensive double-release from a
    // manually-crafted or corrupted history) is a no-op.
    crate::sessions::release_session_slot(session_slots_in_use, session_id);

    let output = serde_json::Value::Null;
    finalize_activity_completion(&mut conn, task, exec_id, activity_id, output, None).await
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn process_activity_task(
    pool: &DbPool,
    registry: &HandlerRegistry,
    task: &TaskQueueItem,
    worker_id: &str,
    cancellation_grace_period: Duration,
    dispatched_at: std::time::Instant,
    max_concurrent_sessions: i32,
    session_slots_in_use: &crate::sessions::SessionSlotRegistry,
) -> HarvestResult<()> {
    let Some(exec_uuid) = task.workflow_exec_id else {
        let mut conn = pool.get().await.map_err(crate::error::database_error)?;
        return fail_task_only(&mut conn, task.id, "activity task missing workflow_exec_id").await;
    };
    let Some(activity_name) = task.activity_name.as_deref() else {
        let mut conn = pool.get().await.map_err(crate::error::database_error)?;
        return fail_task_only(&mut conn, task.id, "activity task missing activity_name").await;
    };
    let exec_id = execution_id_from_uuid(exec_uuid);

    // Worker sessions (issue #606): intercept the two reserved internal
    // activity names before any of the normal handler-dispatch machinery
    // (circuit breaker, rate limiting, ActivityStarted append) runs. Both
    // names are always registered in `registry.activities` (see
    // `session_internal_activity_info`) so the enqueue-time lookup in
    // `persist_scheduled_activities` succeeds, but their `handler` fn is a
    // stub that must never actually run.
    if activity_name == crate::context::SESSION_ACQUIRE_ACTIVITY_NAME {
        return handle_session_acquire(
            pool,
            task,
            worker_id,
            exec_id,
            max_concurrent_sessions,
            session_slots_in_use,
            registry.telemetry().metrics.as_ref(),
        )
        .await;
    }
    if activity_name == crate::context::SESSION_RELEASE_ACTIVITY_NAME {
        return handle_session_release(pool, task, worker_id, exec_id, session_slots_in_use).await;
    }

    let Some(activity) = registry.activities.get(activity_name) else {
        let error = format!("no activity handler registered for '{activity_name}'");
        let mut conn = pool.get().await.map_err(crate::error::database_error)?;
        fail_task_and_execution(&mut conn, task, worker_id, &error).await?;
        return Err(HarvestError::Config(error));
    };

    // Circuit breaker (issue #369): consult the breaker before doing anything
    // durable. `on_dispatch` is a pure in-process decision (it may admit the
    // half-open probe), so it is safe to run before we append ActivityStarted.
    let circuit_breakers = registry.circuit_breakers();
    let dispatch_decision = circuit_breakers.on_dispatch(activity_name, std::time::Instant::now());
    // The dispatch token is threaded into `on_result` below so the breaker can
    // fence stale stragglers by generation and gate the half-open probe.
    let circuit_token = match dispatch_decision {
        crate::circuit_breaker::DispatchDecision::Allow { token } => Some(token),
        crate::circuit_breaker::DispatchDecision::ShortCircuit { .. } => None,
    };

    // Dispatch-time rate limiting (issue #369): a circuit-breaker activity skips
    // the claim-time rate-limit gate/debit, so a genuine call (Allow) must reserve
    // a token here, gated on the authoritative `on_dispatch` decision. This runs
    // BEFORE appending ActivityStarted so a deferred (rate-limited) task leaves no
    // event behind — otherwise every defer/reclaim cycle would append a *duplicate*
    // ActivityStarted (an activity stays "pending" until a terminal event, so the
    // start event does not make `append_activity_started_if_pending` idempotent).
    // A short-circuit reserves nothing; only a real call consumes a token. The
    // `circuit_token.is_some()` guard means "decision is Allow"; the breaker guard
    // restricts this to activities whose claim-time rate limiting was skipped.
    if circuit_token.is_some()
        && activity.circuit_breaker.is_some()
        && let Some(key) = task.rate_limit_key.as_deref()
    {
        let mut conn = pool.get().await.map_err(crate::error::database_error)?;
        if !queue::try_consume_rate_limit_token(&mut conn, key).await? {
            // No token available (bucket empty, or fail-closed when the bucket
            // row is missing): defer this real call instead of running it.
            //
            // Releasing any half-open probe slot this dispatch just admitted is
            // essential — `on_dispatch` set `probe_in_flight = true`, and if we
            // returned without resolving it the breaker would stay HalfOpen
            // forever and short-circuit every later attempt. A rate-limit defer
            // is not a downstream health signal, so `on_cancelled` re-arms the
            // cooldown (rather than tripping or closing) and a fresh probe is
            // admitted once the cooldown re-elapses.
            if let Some(token) = circuit_token {
                circuit_breakers.on_cancelled(activity_name, token, std::time::Instant::now());
            }
            let refill_delay = activity
                .rate_limit_rps
                .filter(|rps| *rps > 0.0)
                .map_or(RATE_LIMIT_DEFER_FALLBACK, |rps| {
                    Duration::from_secs_f64(1.0 / rps)
                })
                .clamp(RATE_LIMIT_DEFER_MIN, RATE_LIMIT_DEFER_MAX);
            // Label the throttle metric by the bounded activity name, never the
            // rate-limit bucket key (which for a dynamic per-key limit,
            // issue #699, embeds unbounded tenant input — ADR-0001 §7).
            registry
                .telemetry()
                .metrics
                .record_rate_limit_throttled(activity_name);
            let scheduled_at = chrono::Utc::now()
                + chrono::Duration::from_std(refill_delay)
                    .unwrap_or_else(|_| chrono::Duration::seconds(5));
            queue::defer_rate_limited_task(&mut conn, task.id, scheduled_at).await?;
            return Ok(());
        }
    }

    // Setup phase: append ActivityStarted, then drop the connection so the pool
    // slot is free before the handler runs (prevents a deadlock when
    // `run_transactional` needs a second slot while max_size connections are held
    // by concurrent activity tasks). Appended AFTER the rate-limit reservation so
    // a deferred task never records a start it did not run; serves both the
    // short-circuit path (start + CircuitOpen failure) and the real-call path.
    let activity_id = {
        let mut conn = pool.get().await.map_err(crate::error::database_error)?;
        let started_result =
            append_activity_started_if_pending(&mut conn, task, exec_id, activity_name, worker_id)
                .await;
        let Some(id) = fail_execution_on_error(&mut conn, task, worker_id, started_result).await?
        else {
            // The activity will not run: it already has a terminal event, or the
            // task row stopped being RUNNING (cancelled / timed out concurrently).
            // Undo the side effects of the dispatch decision for this no-op so the
            // breaker and bucket aren't left skewed:
            //   - release any half-open probe `on_dispatch` admitted, or the
            //     breaker would stay HalfOpen with probe_in_flight forever and
            //     short-circuit every later attempt (no-op for non-probe tokens);
            //   - refund the token reserved above for the call that won't happen
            //     (only reached when the reservation succeeded — see the guard).
            if let Some(token) = circuit_token {
                circuit_breakers.on_cancelled(activity_name, token, std::time::Instant::now());
            }
            if circuit_token.is_some()
                && activity.circuit_breaker.is_some()
                && let Some(key) = task.rate_limit_key.as_deref()
                && let Err(error) = queue::refund_rate_limit_token(&mut conn, key).await
            {
                tracing::warn!(
                    rate_limit_key = %key,
                    error = %error,
                    "failed to refund rate-limit token after a no-op activity start"
                );
            }
            return Ok(());
        };
        id
        // conn is dropped here, returning the slot to the pool
    };

    // Schedule-to-start latency (issue #501): record here, once the activity has
    // genuinely started (ActivityStarted appended). This is *past* the
    // dispatch-time no-op gates — the rate-limit defer (`defer_rate_limited_task`)
    // and the already-terminal/cancelled no-op above both return before reaching
    // this point — so a deferred task no longer inflates the started-task count
    // and depresses the p99 capacity SLI during throttling. The circuit-breaker
    // short-circuit path falls through here too: it is a genuine start that fails
    // fast with CircuitOpen, so it is correctly counted. `schedule_to_start_secs`
    // measures from task eligibility, so the time the task spent waiting behind
    // the local concurrency permit in `dispatch_task` is still captured.
    registry.telemetry().metrics.record_schedule_to_start(
        &task.queue_name,
        queue::schedule_to_start_secs(
            task.scheduled_at,
            task.created_at,
            task.started_at.unwrap_or(task.scheduled_at),
        ) + dispatched_at.elapsed().as_secs_f64(),
    );

    if let crate::circuit_breaker::DispatchDecision::ShortCircuit {
        opened_at,
        retry_after,
    } = dispatch_decision
    {
        use crate::failure::IntoActivityErrorString as _;
        let payload =
            crate::failure::ActivityFailure::circuit_open(activity_name, opened_at, retry_after)
                .into_error_payload();
        let telemetry = registry.telemetry().clone();
        telemetry.metrics.record_activity_completed_with_error_type(
            activity_name,
            &task.queue_name,
            0.0,
            ActivityStatus::Failed,
            Some(crate::failure::ERROR_TYPE_CIRCUIT_OPEN),
        );
        telemetry.metrics.record_activity_failed(
            activity_name,
            "",
            crate::failure::ERROR_TYPE_CIRCUIT_OPEN,
            true,
        );
        telemetry.metrics.record_activity_attempt(
            activity_name,
            &task.queue_name,
            ActivityStatus::Failed,
        );
        let mut conn = pool.get().await.map_err(crate::error::database_error)?;
        let retry_policy_result = configured_retry_policy(task);
        let retry_policy =
            fail_execution_on_error(&mut conn, task, worker_id, retry_policy_result).await?;
        return handle_activity_result(
            &mut conn,
            task,
            exec_id,
            activity_id,
            worker_id,
            retry_policy.as_ref(),
            Err(payload),
            0,
            activity_name,
            registry.payload_offloader(),
            telemetry.metrics.as_ref(),
        )
        .await;
    }

    let cancel = CancellationToken::new();
    let heartbeat_tx =
        crate::heartbeat::spawn_heartbeat_flusher(task.id, pool.clone(), cancel.clone());
    let trace_carrier = task
        .trace_context
        .as_ref()
        .and_then(TraceContextCarrier::from_json);
    let activity_context_headers = task
        .context_headers
        .as_ref()
        .and_then(|v| {
            match serde_json::from_value::<std::collections::HashMap<String, String>>(v.clone()) {
                Ok(h) => Some(h),
                Err(e) => {
                    tracing::warn!(error = %e, "failed to deserialize activity context headers; propagating empty map");
                    None
                }
            }
        })
        .map_or_else(
            || std::sync::Arc::new(std::collections::HashMap::new()),
            std::sync::Arc::new,
        );
    let ctx = ActivityContext::new_with_cancellation_check(
        registry.shared_state(),
        Some(heartbeat_tx),
        task.heartbeat_details.clone(),
        cancel.clone(),
        task.id,
        pool.clone(),
    )
    .with_trace_context(trace_carrier.clone())
    .with_context_headers(activity_context_headers)
    .with_metrics(registry.telemetry().metrics.clone())
    .with_idempotency_key(IdempotencyKey::from_activity_exec_id(activity_id))
    .with_attempt(task_attempt(task))
    .with_max_attempts(u32::try_from(task.max_attempts.max(1)).unwrap_or(1))
    .with_previous_failure(if task_attempt(task) > 1 {
        // task.error carries the human-readable message from the previous
        // failed attempt, stored by requeue_for_retry at reschedule time.
        task.error.clone()
    } else {
        None
    });
    // Compute cap before the handler so run_transactional can enforce it
    // inside the transaction (before ActivityCompleted is committed).
    let effective_result_cap = activity
        .max_result_bytes
        .map_or(registry.max_activity_result_bytes, |per_activity| {
            per_activity.max(registry.max_activity_result_bytes)
        });
    #[cfg(feature = "db")]
    let ctx = ctx.with_transactional_state(TransactionalState {
        pool: pool.clone(),
        exec_id,
        activity_id,
        task_id: task.id,
        max_result_bytes: effective_result_cap,
    });

    let telemetry = registry.telemetry().clone();
    // ADR-0001 §3: restore the producer's trace context so the activity span
    // becomes a child of the workflow executor span that enqueued this task.
    let _parent_guard = trace_carrier
        .as_ref()
        .map(|carrier| telemetry.install_trace_context(carrier));
    // ADR-0001 §2.2: harvest.activity.execute — INTERNAL, parent = workflow span.
    let span = tracing::info_span!(
        "harvest.activity.execute",
        "otel.kind" = "internal",
        { ATTR_ACTIVITY_NAME } = %activity_name,
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_ATTEMPT } = task.attempt,
        { ATTR_QUEUE } = %task.queue_name,
    );
    let started_at = std::time::Instant::now();

    // Issue #782: wrap the activity handler in a panic-containing value adapter.
    // A caught panic is flattened into the existing `Result<Value, String>` Err
    // channel as a *retryable* typed HandlerPanic failure, preserving the exact
    // Output type + cancellation/grace-window semantics of
    // `execute_activity_future_with_cancellation` (the adapter wraps the whole
    // future object, so every poll — including the grace re-poll — is contained).
    // Without this the panic unwinds past the DB state-transition boundary and
    // leaves the task row stuck RUNNING on a live worker.
    // Issue #782 (PR #1012 review): also contain a panic during future
    // *construction* — a hand-written activity handler may do synchronous work
    // before returning its boxed future, and that would escape the poll-time
    // `catch_unwind`. On a construction panic, resolve immediately to the same
    // retryable typed HandlerPanic envelope; it flows through the identical
    // downstream path (cancellation adapter → `handle_activity_result`), which
    // emits `record_activity_panic` once by inspecting the envelope's error type,
    // so neither the metric nor the circuit-breaker treatment differs from a
    // poll-phase panic.
    // Issue #680: build the activity execution future through the configured
    // interceptor chain. When no interceptors are registered
    // `dispatch_with_interceptors` is a zero-allocation direct call to the same
    // `(activity.handler)(&ctx, input)` terminal as before (it constructs a
    // stack `ActivityInvocation` + slice borrow but boxes nothing extra), so the
    // dispatch path is functionally unchanged for the default (no-interceptor)
    // case. The
    // whole chain — interceptors and handler alike — is constructed inside
    // `catch_construct` and polled inside `catch_unwind`, so a panic in either
    // an interceptor or the handler is contained on the identical retryable
    // HandlerPanic path (issue #782). Interceptors run AFTER the circuit-breaker
    // admit gate above, so a circuit-open short-circuit never reaches here.
    let activity_interceptors = registry.activity_interceptors();
    let invocation =
        crate::interceptor::ActivityInvocation::new(activity_name, false, &task.queue_name);
    let activity_handler = activity.handler;

    // Issue #965: sandboxed WASM activity dispatch seam. When the activity is
    // WASM-bound (a `WasmBinding` is registered AND a shared module store is
    // configured), resolve its active module version and run the guest through
    // the SAME interceptor chain / `catch_construct` / `catch_unwind` /
    // cancellation wrapper as a native handler — so retry, circuit-breaker,
    // metrics, and DLQ behavior are identical. A pool-checkout failure during
    // resolution is mapped to a RETRYABLE typed failure (so the row leaves
    // RUNNING and can retry) rather than a raw `?` that would strand it. A
    // non-WASM activity takes the native path below byte-for-byte unchanged.
    #[cfg(feature = "wasm-activities")]
    let wasm_dispatch: Option<crate::wasm_store::WasmDispatch> =
        match (registry.wasm_binding(activity_name), registry.wasm_store()) {
            (Some(binding), Some(store)) => Some(match pool.get().await {
                Ok(mut conn) => {
                    crate::wasm_store::resolve_wasm_dispatch(
                        &mut conn,
                        store,
                        binding,
                        activity_name,
                        wasm_effective_deadline(
                            task.start_to_close,
                            activity.default_start_to_close,
                        ),
                        // Thread the task cancellation token so a cancelled guest
                        // is cooperatively interrupted (issue #965 review) within
                        // ~1 epoch tick, instead of holding a blocking-pool thread
                        // until its wall-clock ceiling.
                        Some(cancel.clone()),
                        // Thread the start-to-close anchor so `invoke` charges the
                        // whole pre-guest interval — resolution (this checkout +
                        // active-hash lookup + cold-cache byte fetch) plus compile
                        // — against the guest deadline, not just compile (issue
                        // #965 review round 7). `started_at` was captured above,
                        // just before this dispatch resolution began, so it
                        // aligns with the start-to-close clock that started when
                        // `ActivityStarted` was recorded.
                        started_at,
                    )
                    .await
                    // `conn` is dropped at the end of this arm, before the guest runs.
                }
                Err(e) => {
                    use crate::failure::IntoActivityErrorString as _;
                    crate::wasm_store::WasmDispatch::Fail(
                        crate::failure::ActivityFailure::wasm_module_lookup_failed(format!(
                            "failed to acquire a database connection to resolve the wasm module \
                             for activity '{activity_name}': {e}"
                        ))
                        .into_error_payload(),
                    )
                }
            }),
            _ => None,
        };

    // A `map_or_else` here would nest the ~50-line WASM-invoke terminal closure
    // inside a closure argument, which is markedly harder to read than the match.
    #[cfg(feature = "wasm-activities")]
    #[allow(clippy::option_if_let_else)]
    let constructed = match wasm_dispatch {
        Some(dispatch) => crate::error::catch_construct(|| {
            crate::interceptor::dispatch_with_interceptors(
                activity_interceptors,
                &invocation,
                &ctx,
                task.input.clone(),
                // `move` captures the resolved dispatch; it does NOT borrow
                // `&ctx`, so it composes with the chain's own `&ctx` borrow. The
                // explicit return type unifies the two `Box::pin` arms.
                move |input| -> crate::interceptor::ActivityInterceptorFuture<'_> {
                    match dispatch {
                        crate::wasm_store::WasmDispatch::Invoke(prepared) => {
                            Box::pin(async move {
                                // `invoke` is CPU-bound + blocking, so drive it on
                                // the blocking pool.
                                match tokio::task::spawn_blocking(move || prepared.invoke(&input))
                                    .await
                                {
                                    Ok(Ok(value)) => Ok(value),
                                    Ok(Err(failure)) => {
                                        use crate::failure::IntoActivityErrorString as _;
                                        Err(failure.into_error_payload())
                                    }
                                    Err(join_err) => {
                                        use crate::failure::IntoActivityErrorString as _;
                                        Err(crate::failure::ActivityFailure::wasm_trap(format!(
                                            "wasm blocking task failed to join: {join_err}"
                                        ))
                                        .into_error_payload())
                                    }
                                }
                            })
                        }
                        crate::wasm_store::WasmDispatch::Fail(payload) => {
                            Box::pin(futures::future::ready(Err(payload)))
                        }
                    }
                },
            )
        }),
        None => crate::error::catch_construct(|| {
            crate::interceptor::dispatch_with_interceptors(
                activity_interceptors,
                &invocation,
                &ctx,
                task.input.clone(),
                |input| (activity_handler)(&ctx, input),
            )
        }),
    };

    #[cfg(not(feature = "wasm-activities"))]
    let constructed = crate::error::catch_construct(|| {
        crate::interceptor::dispatch_with_interceptors(
            activity_interceptors,
            &invocation,
            &ctx,
            task.input.clone(),
            |input| (activity_handler)(&ctx, input),
        )
    });

    let mut activity_future = {
        use futures::FutureExt as _;
        match constructed {
            Ok(fut) => std::panic::AssertUnwindSafe(fut)
                .catch_unwind()
                .map(|caught| match caught {
                    Ok(inner) => inner,
                    Err(panic_payload) => Err(handler_panic_activity_envelope(
                        crate::error::panic_message(panic_payload),
                    )),
                })
                .left_future(),
            Err(message) => {
                futures::future::ready(Err(handler_panic_activity_envelope(message))).right_future()
            }
        }
    };
    let cancellation_observer = observe_task_cancellation(pool, task.id);
    tokio::pin!(cancellation_observer);

    let activity_result = execute_activity_future_with_cancellation(
        activity_name,
        task.id,
        cancellation_grace_period,
        &mut activity_future,
        cancellation_observer,
        cancel.clone(),
        span,
    )
    .await;
    // Whether this attempt was driven by cancellation: the observer cancels the
    // token when the workflow/task is cancelled mid-flight. A cancellation is
    // not evidence the downstream is unhealthy, so it must not count toward the
    // circuit breaker (issue #369 review). Captured before the unconditional
    // `cancel.cancel()` below.
    let was_cancelled = cancel.is_cancelled();

    // Pre-normalize oversized results to non-retryable failures BEFORE emitting
    // metrics so that an Ok result above the cap is counted as Failed, not Completed.
    // Issue #524: skip the cap when the offloader will handle the large payload —
    // finalize_activity_completion routes through append_events_offloaded, so the
    // result will be stored as a tiny reference envelope in harvest_events.
    let activity_result = match activity_result {
        Ok(output) if effective_result_cap > 0 => {
            let observed = serde_json::to_string(&output).map_or(0, |s| s.len() as u64);
            let offload_applies = registry
                .payload_offloader()
                .is_some_and(|o| observed > o.threshold());
            if observed > effective_result_cap && !offload_applies {
                use crate::failure::IntoActivityErrorString as _;
                let error = crate::failure::ActivityFailure::non_retryable(
                    "PayloadTooLarge",
                    format!(
                        "activity '{activity_name}' result exceeds cap: \
                         {observed} bytes (cap {effective_result_cap} bytes)"
                    ),
                )
                .into_error_payload();
                Err(error)
            } else {
                Ok(output)
            }
        }
        other => other,
    };

    // Issue #680: a transactional activity that called `ctx.run_transactional`
    // and committed has already atomically sealed its `ActivityCompleted` event
    // and task-COMPLETED transition inside the user's transaction — before an
    // outer interceptor regained control from `next.run`. Any result/error
    // transform the interceptor then applied is discarded from history (the seal
    // is immutable). When a self-commit occurred, the authoritative outcome is
    // that committed success, so metrics and the circuit breaker must reflect it
    // rather than the (possibly transformed) post-interceptor `activity_result`.
    // Reading here (before `activity_future` is dropped) is a shared borrow of
    // `ctx` alongside the future's own shared borrow — the future has already
    // resolved. On non-`db` builds `run_transactional` does not exist, so the
    // flag is always false.
    let committed_transactionally = ctx.transactional_commit_occurred();

    let duration_secs = started_at.elapsed().as_secs_f64();
    let status = if committed_transactionally || activity_result.is_ok() {
        ActivityStatus::Completed
    } else {
        ActivityStatus::Failed
    };
    // Parse the structured payload once and reuse for both the histogram
    // and the per-failure counter (so the `error.type` attribute is
    // consistent across `harvest.activity.duration` and
    // `harvest.activity.failed`). Suppressed for a self-committed activity: its
    // recorded outcome is a success, so it emits no failure attribute/counter.
    let failure_info = if committed_transactionally {
        None
    } else {
        activity_result
            .as_ref()
            .err()
            .map(|payload| parse_error_payload(payload))
    };
    telemetry.metrics.record_activity_completed_with_error_type(
        activity_name,
        &task.queue_name,
        duration_secs,
        status,
        failure_info.as_ref().map(|(et, _, _)| et.as_str()),
    );
    // AC1 (issue #528): single-family attempt counter for success-rate SLOs.
    // Fires for both outcomes so `completed / (completed + failed)` is one
    // metric family — the activity-level mirror of harvest.workflow.terminal.
    telemetry
        .metrics
        .record_activity_attempt(activity_name, &task.queue_name, status);
    if let Some((error_type, non_retryable, _)) = failure_info.as_ref() {
        // `workflow.type` is intentionally empty here: looking it up requires
        // an extra `harvest_workflow_executions` query per failure, and the
        // `MetricsRecorder` trait docs explicitly allow an empty string when
        // the workflow type is unknown at the call site. Plumbing it through
        // is tracked as a follow-up.
        telemetry
            .metrics
            .record_activity_failed(activity_name, "", error_type, *non_retryable);
    }
    cancel.cancel();
    drop(activity_future);

    // Finalization phase: re-acquire a connection now that the handler is done.
    let mut conn = pool.get().await.map_err(crate::error::database_error)?;
    let retry_policy_result = configured_retry_policy(task);
    let retry_policy =
        fail_execution_on_error(&mut conn, task, worker_id, retry_policy_result).await?;

    // Circuit breaker (issue #369): record this attempt's outcome. A close →
    // open trip (or half-open re-open) and a recovery to closed are surfaced as
    // the `harvest.activity.circuit.{tripped,closed}` counters so existing
    // alerting picks them up. Only retryable (downstream-style) failures trip
    // the breaker — a non-retryable permanent error (bad input) proves the
    // downstream answered and must not open the circuit for healthy callers.
    // Classification mirrors the retry decision (`failure_is_non_retryable`),
    // so it honours both the typed `non_retryable` flag and the retry policy's
    // `non_retryable_errors` list, including legacy `Err(String)` failures.
    // A cancellation-driven result is not evidence the downstream is unhealthy
    // (the workflow/task was cancelled out from under the attempt), so it is
    // excluded from breaker accounting entirely — neither a trip nor a probe
    // resolution. But if the cancelled attempt held the single half-open probe,
    // its slot must still be released via `on_cancelled`, or the breaker would
    // stay HalfOpen with `probe_in_flight = true` forever and short-circuit every
    // later dispatch. Only genuine handler outcomes feed the breaker as outcomes.
    let circuit_outcome = if was_cancelled {
        if let Some(token) = circuit_token {
            circuit_breakers.on_cancelled(activity_name, token, std::time::Instant::now());
        }
        None
    } else if committed_transactionally {
        // Issue #680: the committed transactional outcome is a success, so feed
        // the breaker `Success` regardless of any interceptor error transform —
        // a self-committed activity must never trip the circuit.
        Some(crate::circuit_breaker::AttemptOutcome::Success)
    } else {
        Some(match activity_result.as_ref() {
            Ok(_) => crate::circuit_breaker::AttemptOutcome::Success,
            Err(payload) if failure_is_non_retryable(payload, retry_policy.as_ref()) => {
                crate::circuit_breaker::AttemptOutcome::NonRetryableFailure
            }
            Err(_) => crate::circuit_breaker::AttemptOutcome::RetryableFailure,
        })
    };
    // `circuit_token` is always `Some` here: the short-circuit path returned
    // early above, so reaching this point means the attempt was dispatched.
    if let Some(transition) = circuit_token
        .zip(circuit_outcome)
        .and_then(|(token, outcome)| {
            circuit_breakers.on_result(activity_name, outcome, token, std::time::Instant::now())
        })
    {
        match transition {
            crate::circuit_breaker::CircuitTransition::Tripped => {
                telemetry.metrics.record_circuit_tripped(activity_name);
            }
            crate::circuit_breaker::CircuitTransition::Closed => {
                telemetry.metrics.record_circuit_closed(activity_name);
            }
        }
    }

    // Issue #680: a self-committed transactional activity has already sealed its
    // `ActivityCompleted` + task-COMPLETED atomically, so there is nothing left
    // to persist. Skip the finalize/retry path entirely: `handle_activity_result`
    // would at best no-op (the task is not RUNNING) and, on a retryable
    // post-interceptor `Err` with retry budget, would spuriously attempt a
    // `requeue_for_retry` that logs a NotFound against the already-COMPLETED
    // task. An outer interceptor cannot un-commit the sealed outcome, so any
    // result/error transform it applied after `next.run` is ignored; when that
    // transform turned the committed success into an `Err`, surface the misuse
    // with a single clear warning (mirroring the previous finalize-path warning).
    if committed_transactionally {
        if activity_result.is_err() {
            tracing::warn!(
                task_id = %task.id,
                activity_name = %activity_name,
                "an interceptor (or post-commit handler code) transformed the outcome of a \
                 transactional activity to an error; the transform is ignored — the handler \
                 sealed ActivityCompleted atomically via run_transactional, so the workflow \
                 observes the committed success"
            );
        }
        return Ok(());
    }

    // activity_result is already cap-normalized (oversized Ok → non-retryable Err);
    // pass 0 so handle_activity_result skips the redundant cap check.
    handle_activity_result(
        &mut conn,
        task,
        exec_id,
        activity_id,
        worker_id,
        retry_policy.as_ref(),
        activity_result,
        0,
        activity_name,
        registry.payload_offloader(),
        telemetry.metrics.as_ref(),
    )
    .await
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn persist_scheduled_external_activity(
    conn: &mut AsyncPgConnection,
    detached_spawns: DetachedSpawnPersistence<'_>,
    exec_id: ExecutionId,
    next_event_id: i32,
    task_id: uuid::Uuid,
    commands: &[WorkflowCommand],
    scheduled: &ScheduledExternalActivityCommand,
    sticky: Option<queue::StickyHint<'_>>,
) -> HarvestResult<()> {
    // If the token is already registered the awaiting event was already
    // appended by a prior run.  A workflow woken by a signal while still
    // waiting for external completion will re-emit ScheduleExternalActivity.
    // Use a fast non-locking check first; if the row exists, enter a
    // transaction that locks it to close the race with complete/fail_externally:
    // the management API holds FOR UPDATE on the external task row while it
    // appends the terminal event and calls wake_workflow_task.  Because the
    // workflow task is still RUNNING at that point, the wake is a no-op.  By
    // waiting for the same lock here we read the post-commit state and re-wake
    // the workflow ourselves if the task is already terminal, preventing an
    // indefinite park despite terminal history being present.
    if external_task::find_by_token(conn, scheduled.token)
        .await?
        .is_some()
    {
        let token = scheduled.token;
        let registry = detached_spawns.registry;
        let deferred = conn
            .transaction::<_, HarvestError, _>(|conn| {
                async move {
                    let locked = external_task::find_by_token_locked(conn, token).await?;

                    // Recompute the event offset inside the transaction: external
                    // completion may have appended a terminal event between replay
                    // start (when next_event_id was sampled) and here.  Using
                    // append_single_event serialises each append against concurrent
                    // writers via the per-execution FOR UPDATE it acquires.
                    //
                    // Cancellable/renewable timer bookkeeping (issue #768): resolve
                    // ArmTimer/CancelTimer row mutations FIRST, then interleave
                    // their TimerStarted/TimerCancelled events at their command
                    // positions in the marker batch (armed timers observed on next
                    // wake; deadline unused here).
                    let (mut timer_events, _min_fires_at) =
                        plan_timer_lifecycle(conn, exec_id, commands).await?;
                    let marker_events =
                        pre_suspension_events_from_commands(commands, &mut timer_events);
                    for event in marker_events {
                        store::append_single_event(conn, exec_id, event).await?;
                    }
                    detached_spawns.persist(conn, commands).await?;

                    let mut race_next_event_id =
                        store::load_history(conn, exec_id).await?.next_event_id;
                    let deferred = apply_race_loser_cancellations(
                        conn,
                        exec_id,
                        commands,
                        &mut race_next_event_id,
                        registry,
                    )
                    .await?;

                    // Park first to clear worker ownership; wake_workflow_task only
                    // ever moves parked rows.
                    //
                    // `had_wake_requested` closes the residual race window the
                    // `locked` check above cannot cover (PR #901 review): an
                    // external-task completion/timeout path that does not share
                    // `find_by_token_locked`'s row lock (e.g. a distinct token
                    // whose event still targets this same workflow task) racing
                    // between that check and this park's own atomic UPDATE would
                    // otherwise have its wake silently dropped.
                    let had_wake_requested =
                        queue::park_workflow_task(conn, task_id, sticky).await?;
                    if had_wake_requested || locked.is_some_and(|t| t.state != "PENDING") {
                        // The task is still RUNNING (owned by this worker), so a
                        // wake fired while it was terminal-ineligible would have been
                        // a no-op. Wake now so the next available worker picks up the
                        // terminal event.
                        queue::wake_workflow_task(conn, exec_id).await?;
                    }
                    Ok(deferred)
                }
                .scope_boxed()
            })
            .await?;
        for start in deferred {
            start.spawn();
        }
        return Ok(());
    }

    // `had_wake_requested` closes a race no other check in this (new-token)
    // branch covers (PR #901 review): a signal or admitted update landing
    // while this transaction is still recording the external task -- before
    // this park's own atomic UPDATE -- would otherwise only be captured as
    // `wake_requested = TRUE` on the still-claimed row and then silently
    // discarded, with no later recheck in this branch (unlike the
    // existing-token branch above, which re-checks the external task's own
    // state via `find_by_token_locked`).
    let registry = detached_spawns.registry;
    let (deferred, had_wake_requested) = conn
        .transaction::<_, HarvestError, _>(|conn| {
            async move {
                // Cancellable/renewable timer bookkeeping (issue #768): resolve
                // ArmTimer/CancelTimer row mutations FIRST, then build the event
                // list in command emission order so ScheduleExternalActivity's
                // ActivityAwaitingExternal event, markers, detached-spawn events,
                // and interleaved TimerStarted/TimerCancelled all land at their
                // actual command positions (armed timers observed on next wake;
                // deadline unused here).
                let (mut timer_events, _min_fires_at) =
                    plan_timer_lifecycle(conn, exec_id, commands).await?;
                let mut awaiting_event = Some(WorkflowEvent::ActivityAwaitingExternal {
                    activity_id: scheduled.activity_id,
                    token: scheduled.token,
                    name: scheduled.name.clone(),
                    input: scheduled.input.clone(),
                    queue: scheduled.queue.clone(),
                    schedule_to_close_secs: scheduled.schedule_to_close_secs,
                });
                let events = build_suspension_events(commands, &mut timer_events, |cmd| {
                    if matches!(cmd, WorkflowCommand::ScheduleExternalActivity { .. }) {
                        awaiting_event.take()
                    } else {
                        None
                    }
                });
                let events_len = i32::try_from(events.len()).unwrap_or(i32::MAX);
                store::append_events(conn, exec_id, &events, next_event_id).await?;
                detached_spawns.persist(conn, commands).await?;
                external_task::record_external_task(
                    conn,
                    exec_id,
                    scheduled.token,
                    scheduled.activity_id,
                    &scheduled.name,
                    &scheduled.queue,
                    scheduled.schedule_to_close_secs,
                )
                .await?;
                let mut race_next_event_id = next_event_id.saturating_add(events_len);
                let deferred = apply_race_loser_cancellations(
                    conn,
                    exec_id,
                    commands,
                    &mut race_next_event_id,
                    registry,
                )
                .await?;
                let had_wake_requested = queue::park_workflow_task(conn, task_id, sticky).await?;
                Ok((deferred, had_wake_requested))
            }
            .scope_boxed()
        })
        .await?;
    for start in deferred {
        start.spawn();
    }

    if had_wake_requested {
        queue::wake_workflow_task(conn, exec_id).await?;
    }
    Ok(())
}

async fn persist_bookkeeping_and_requeue_workflow(
    conn: &mut AsyncPgConnection,
    detached_spawns: DetachedSpawnPersistence<'_>,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    commands: &[WorkflowCommand],
    sticky: Option<queue::StickyHint<'_>>,
) -> HarvestResult<()> {
    let registry = detached_spawns.registry;

    let deferred = conn
        .transaction::<_, HarvestError, _>(|conn| {
            async move {
                // Cancellable/renewable timer bookkeeping (issue #768): resolve
                // the ArmTimer/CancelTimer row mutations FIRST, then interleave
                // their TimerStarted/TimerCancelled events into the suspension
                // batch at their command-emission positions. When this
                // bookkeeping-only batch armed a durable timer, `armed_fires_at`
                // carries that deadline so the parked task can be rescheduled to
                // it (this is the one path — `await_fire` — that reschedules to
                // the armed deadline).
                let (mut timer_events, armed_fires_at) =
                    plan_timer_lifecycle(conn, exec_id, commands).await?;
                let events = pre_suspension_events_from_commands(commands, &mut timer_events);
                let events_len = i32::try_from(events.len()).unwrap_or(i32::MAX);
                if !events.is_empty() {
                    store::append_events(conn, exec_id, &events, next_event_id).await?;
                }
                detached_spawns.persist(conn, commands).await?;
                let mut race_next_event_id = next_event_id.saturating_add(events_len);
                let deferred = apply_race_loser_cancellations(
                    conn,
                    exec_id,
                    commands,
                    &mut race_next_event_id,
                    registry,
                )
                .await?;
                // FINDING 2 (PR #901 dropped-wake class): honour `had_wake_requested`.
                // A wake that raced this park (a signal / admitted update /
                // child-completion landing while the row was still claimed — the
                // sliding-window "reset the timer on each event" path) is captured
                // as `wake_requested = TRUE` and read-and-cleared by
                // `park_workflow_task`. Discarding it would strand the workflow up
                // to a full timer duration. Waking now is safe: the armed timer
                // row is durable, so on re-run the workflow re-parks in
                // `await_fire`, re-arms idempotently, and reschedules to
                // `fires_at` again.
                let had_wake_requested = queue::park_workflow_task(conn, task_id, sticky).await?;
                if had_wake_requested {
                    queue::wake_workflow_task(conn, exec_id).await?;
                } else if let Some(fires_at) = armed_fires_at {
                    queue::reschedule_task(conn, task_id, fires_at).await?;
                } else {
                    queue::wake_workflow_task(conn, exec_id).await?;
                }
                Ok(deferred)
            }
            .scope_boxed()
        })
        .await?;

    for start in deferred {
        start.spawn();
    }
    Ok(())
}

/// Resolve the `WorkflowCommand::CancelRaceLosers` bookkeeping command(s) in
/// `commands` (issue #600): durably cancel every losing activity/child/timer
/// branch of a resolved `ctx.race()`, in the **same transaction** that
/// persists the race's winner marker (the caller runs this inside its own
/// `conn.transaction`), so a crash between the two can never leak a row.
///
/// Records the same telemetry a normal terminal outcome would: the
/// `harvest.activity.*` trio for a cancelled loser activity (mirroring the
/// circuit-breaker short-circuit convention of `duration_secs = 0.0` and
/// `workflow_type = ""` when the activity never actually ran) and
/// `harvest.workflow.terminal{outcome="cancelled"}` for a cancelled loser
/// child workflow.
///
/// Returns the child-cancellation `DeferredTriggerStart`s that the caller
/// must spawn **after** its outer transaction commits (mirrors the existing
/// external-cancel delivery pattern, issue #492) — never before, so a rolled
/// back cancellation cannot have already started trigger workflows.
#[doc(hidden)] // exposed for the #779 event-id-accounting integration test; not a stable API
pub async fn apply_race_loser_cancellations(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    commands: &[WorkflowCommand],
    next_event_id: &mut i32,
    registry: &HandlerRegistry,
) -> HarvestResult<Vec<crate::completion_trigger::DeferredTriggerStart>> {
    let mut deferred = Vec::new();
    let mut synthetic_events = Vec::new();
    let metrics = &registry.telemetry().metrics;

    for cmd in commands {
        let WorkflowCommand::CancelRaceLosers {
            activities,
            children,
            timers,
        } = cmd
        else {
            continue;
        };

        for activity_id in activities {
            // Only a still-open row gets a synthetic terminal: if the activity
            // genuinely completed first (raced the cancellation), a real
            // terminal event already exists (or is about to be appended by
            // that in-flight completion write) and must not be duplicated.
            if let Some((activity_name, queue_name)) =
                queue::cancel_activity_task(conn, *activity_id).await?
            {
                synthetic_events.push(WorkflowEvent::ActivityFailed {
                    activity_id: *activity_id,
                    error: "lost race to a sibling branch".to_string(),
                    attempt: 1,
                    error_type: "Error".to_string(),
                    non_retryable: true,
                    details: None,
                });
                metrics.record_activity_completed_with_error_type(
                    &activity_name,
                    &queue_name,
                    0.0,
                    ActivityStatus::Failed,
                    Some("Error"),
                );
                metrics.record_activity_failed(&activity_name, "", "Error", true);
                metrics.record_activity_attempt(
                    &activity_name,
                    &queue_name,
                    ActivityStatus::Failed,
                );
            }
        }

        for timer_id in timers {
            queue::delete_pending_timer(conn, exec_id, timer_id).await?;
        }

        for child_id in children {
            match crate::execution::cancel_workflow_execution_collect(
                conn,
                *child_id,
                "lost race to a sibling branch",
            )
            .await
            {
                Ok((_, mut starts, _closed_children, terminal_metric)) => {
                    deferred.append(&mut starts);
                    if let Some((child_workflow_name, queue_name)) = terminal_metric {
                        metrics.record_workflow_terminal(
                            &child_workflow_name,
                            &queue_name,
                            WorkflowStatus::Cancelled,
                        );
                        // A race child is always an *awaited* child of this very
                        // execution (parent_id = exec_id, parent_close_policy =
                        // None), so a genuine (newly-cancelled) cancellation here
                        // runs `notify_awaited_parent_of_child_terminal` inside the
                        // same transaction, appending onto *our own* history via
                        // self-computed ids. That append is the `ChildWorkflowFailed`
                        // terminal PLUS — since #779's deadline-materialization
                        // (Codex P2-D) — zero or more preceding `__child_timeout`
                        // `TimerFired` deadlines. Its event count is therefore
                        // *variable* (1 + N materialized), not a fixed 1, so a
                        // hardcoded `+1` here would leave the cursor short by N and
                        // the winner marker / terminal-outcome / synthetic-loser
                        // append that follows would reuse a consumed id and fail on
                        // `UNIQUE(workflow_exec_id, event_id)`. Re-read the true next
                        // event id under the parent row lock that `notify_awaited_
                        // parent_of_child_terminal`'s append already holds (Codex P2).
                        *next_event_id = store::next_event_id_for(conn, exec_id).await?;
                    }
                }
                Err(HarvestError::NotFound(_) | HarvestError::Config(_)) => {
                    // Benign no-op: the child is already gone, or already reached
                    // some other terminal state on its own (it also finished,
                    // just wasn't the winner) -- nothing to durably cancel, and no
                    // event was appended to our history.
                }
                Err(err) => {
                    // A real persistence failure must not be swallowed here:
                    // CancelRaceLosers is pushed exactly once, on the single
                    // cycle the winner marker is first recorded, and is never
                    // re-emitted on later replays (settle_race only re-verifies
                    // an already-recorded winner from then on). Silently
                    // continuing would leave this losing child running forever
                    // with no future chance to be cancelled. Propagate so the
                    // whole transaction rolls back and the next attempt
                    // re-derives and retries the cancellation.
                    return Err(err);
                }
            }
        }
    }

    if !synthetic_events.is_empty() {
        let inserted =
            store::append_events(conn, exec_id, &synthetic_events, *next_event_id).await?;
        *next_event_id = next_event_id.saturating_add(i32::try_from(inserted).unwrap_or(0));
    }

    Ok(deferred)
}

/// Pure event-emission plan for the **fresh-arm** timer commands
/// (`ArmTimer { for_await: false }`) in a batch (issue #768, Codex P2 round 4).
/// DB-independent, so it is unit-testable without a database and runs identically
/// on every suspension path.
///
/// A fresh arm (pushed by `start_timer` / `reset`) records a positional
/// `TimerStarted` event but inserts **no** `harvest_timers` row — a cancellable
/// timer becomes fire-eligible only when it is *awaited*
/// (`ArmTimer { for_await: true }`, resolved by the DB loop in
/// [`plan_timer_lifecycle`], which inserts the row and emits no event). This
/// function therefore populates only the fresh-arm indices.
///
/// Dedup:
///
/// - an `ArmTimer` whose id also has a `StartTimer` in the batch emits nothing
///   (the suspension path / `persist_started_timer` owns that id); and
/// - an `ArmTimer` for an id still *active* in this batch (a prior un-cancelled
///   fresh `ArmTimer` for the same id) emits nothing — a `CancelTimer` clears the
///   active state so a later re-arm (a same-cycle `reset`) emits again.
///
/// Returns a `Vec<Option<WorkflowEvent>>` aligned to `commands`; only fresh
/// `ArmTimer` indices are ever populated here. `CancelTimer` emission
/// (`TimerCancelled`) and `for_await: true` row inserts stay with the DB loop in
/// [`plan_timer_lifecycle`].
/// Detect a same-batch collision between a LIVE cancellable arm and a classic
/// `StartTimer` for the same id (issue #768, Codex P2 round 15).
///
/// Returns the colliding `timer_id` when the batch holds an `ArmTimer(X)` whose
/// fresh `TimerStarted` would be dropped by `arm_timer_events`' `StartTimer`-owned
/// skip **without** having been cancelled first — i.e. a `StartTimer(X)` appears
/// and there is at least one preceding `ArmTimer(X)` with no `CancelTimer(X)`
/// between that arm and the `StartTimer(X)`. The legit cancel-then-classic pattern
/// (`[ArmTimer(X,false), CancelTimer(X), StartTimer(X)]`) is NOT a collision — the
/// intervening cancel makes the arm dead — so it is not reported.
fn same_batch_uncancelled_arm_start_collision(commands: &[WorkflowCommand]) -> Option<String> {
    for (start_idx, cmd) in commands.iter().enumerate() {
        let WorkflowCommand::StartTimer { timer_id, .. } = cmd else {
            continue;
        };
        let id = timer_id.as_str();
        // Order-independent detection (issue #768, Codex P2 round 16). The ONLY
        // legit same-id `StartTimer` + `ArmTimer` combo is the cancel-then-classic
        // pattern `[ArmTimer(X,false), CancelTimer(X), StartTimer(X)]` — the arm is
        // dead before the classic start. Any other interleaving is a collision:
        //   (a) an arm is still LIVE at the StartTimer's position (cancellable-first
        //       — `[ArmTimer(X), StartTimer(X)]`), OR
        //   (b) there is ANY `ArmTimer(X)` AFTER the StartTimer (classic-first —
        //       `[StartTimer(X), ArmTimer(X)]`).
        // Scanning both directions makes the guard bidirectional and independent of
        // the batch's command order (the source-level check in `WorkflowContext`
        // already prevents both, so this is a defensive backstop).
        let mut arm_live = false;
        for prior in &commands[..start_idx] {
            match prior {
                WorkflowCommand::ArmTimer {
                    timer_id: arm_id, ..
                } if arm_id.as_str() == id => arm_live = true,
                WorkflowCommand::CancelTimer {
                    timer_id: cancel_id,
                } if cancel_id.as_str() == id => {
                    arm_live = false;
                }
                _ => {}
            }
        }
        if arm_live {
            return Some(id.to_string());
        }
        // (b) A cancellable arm placed AFTER the classic start of the same id is a
        // collision regardless of any preceding cancel.
        let arm_after = commands[start_idx + 1..].iter().any(|c| {
            matches!(c, WorkflowCommand::ArmTimer { timer_id: arm_id, .. } if arm_id.as_str() == id)
        });
        if arm_after {
            return Some(id.to_string());
        }
    }
    None
}

fn arm_timer_events(commands: &[WorkflowCommand]) -> Vec<Option<WorkflowEvent>> {
    let start_timer_ids: HashSet<&str> = commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::StartTimer { timer_id, .. } => Some(timer_id.as_str()),
            _ => None,
        })
        .collect();

    // Ids with an un-cancelled fresh `ArmTimer` seen so far in this batch. A
    // `CancelTimer` removes the id so a same-cycle `reset` (CancelTimer +
    // ArmTimer) re-emits `TimerStarted`.
    let mut active: HashSet<&str> = HashSet::new();
    let mut events: Vec<Option<WorkflowEvent>> = vec![None; commands.len()];

    for (i, cmd) in commands.iter().enumerate() {
        match cmd {
            WorkflowCommand::ArmTimer {
                timer_id,
                duration_secs,
                for_await: false,
            } => {
                if start_timer_ids.contains(timer_id.as_str()) {
                    continue;
                }
                if active.insert(timer_id.as_str()) {
                    events[i] = Some(WorkflowEvent::TimerStarted {
                        timer_id: timer_id.clone(),
                        duration_secs: *duration_secs,
                    });
                }
            }
            WorkflowCommand::CancelTimer { timer_id } => {
                active.remove(timer_id.as_str());
            }
            // `for_await: true` re-arms insert a row (DB loop) but emit no event.
            _ => {}
        }
    }

    events
}

/// Pure core of [`plan_timer_lifecycle`] (issue #768): decides (a) the positional
/// `TimerStarted`/`TimerCancelled` events for the batch and (b) which
/// `for_await: true` arm command **indices** contribute a deadline to
/// `min_fires_at`. DB-independent — the enclosing fn only resolves the actual
/// `fires_at` timestamps and performs the `harvest_timers` row upserts/deletes —
/// so it is unit-testable without a database.
///
/// A same-batch `CancelTimer(X)` supersedes an `ArmTimer(X)` re-arm in
/// **batch order** (round 7 fix, refined in rounds 9 and 11): a `for_await: true`
/// (await) arm for X contributes its deadline iff **no `CancelTimer(X)` appears
/// after it in the batch**. A `for_await: false` (fresh start/reset) arm never
/// contributes a firing row on its own. Without this exclusion, an `await_fire`
/// racing a sibling `cancel_timer(X)` in the same task would reschedule the parked
/// task to a now-deleted row's deadline instead of waking immediately to consume
/// the recorded `TimerCancelled`. The per-await-arm order-sensitivity lets a
/// reset-then-await in one task — `[CancelTimer(X), ArmTimer(X, false),
/// ArmTimer(X, true)]`, whose await arm is *last* — still arm the durable row in
/// the transaction that recorded the reset, while an await-then-reset
/// `[ArmTimer(X, true), CancelTimer(X), ArmTimer(X, false)]` — whose await arm has
/// a same-id cancel after it — does NOT arm a stale firing row (round 11 fix): the
/// live run would otherwise fire off the old await duration while replay, seeing
/// the recorded `TimerCancelled` first, resolves `Cancelled` — a divergence.
fn plan_timer_lifecycle_pure(
    commands: &[WorkflowCommand],
) -> (Vec<Option<WorkflowEvent>>, Vec<usize>) {
    // Fresh-arm (`for_await: false`) `TimerStarted` events (with in-batch active-set
    // + `StartTimer`-owned dedup).
    let mut events_by_index = arm_timer_events(commands);

    // An `ArmTimer` whose id also has a same-batch `StartTimer` (the classic
    // `ctx.timer` awaited in the same cycle) is owned by `persist_started_timer`.
    let start_timer_ids: std::collections::HashSet<&str> = commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::StartTimer { timer_id, .. } => Some(timer_id.as_str()),
            _ => None,
        })
        .collect();

    // Per-await-arm liveness against same-batch cancels, resolved in **batch
    // order** (rounds 9 + 11). A `for_await: true` (await) arm for id X
    // contributes its firing-row deadline IFF, evaluated at its own position:
    //   (forward, round 11) no `CancelTimer(X)` appears AFTER it in the batch —
    //     a later cancel supersedes the await regardless of any later
    //     `for_await: false` fresh arm re-establishing X. On replay such an await
    //     resolves against the recorded `TimerCancelled` that precedes the fresh
    //     arm's `TimerStarted`, so the live run must not arm a firing row off the
    //     stale await duration (that would fire live while replay resolves
    //     `Cancelled` — a divergence). This is the round-11 FINDING fix.
    //   (backward, round 9) X is not sitting cancelled at the arm — if a
    //     `CancelTimer(X)` precedes the await with no `ArmTimer(X, for_await:
    //     false)` fresh arm between that cancel and the await, X is dead and the
    //     await wakes now (cancel-then-await with no reset). A fresh arm between
    //     the cancel and the await re-establishes X (reset-then-await → arms).
    // A `for_await: false` (fresh start/reset) arm never contributes a firing row
    // itself — it only (re-)establishes the logical Armed state; a firing row is
    // created solely by a subsequent `await_fire` (`for_await: true`) that
    // survives BOTH checks (in this batch or a later one).
    //
    // The two checks together resolve the shapes an end-of-batch-liveness rule
    // conflated:
    //   - reset-then-await `[Cancel, Arm(false), Arm(true)]` → forward clean +
    //     backward re-established → arms in this transaction (deadline starts
    //     when the reset was recorded, not one claim later).
    //   - await-then-reset `[Arm(true, old), Cancel, Arm(false, new)]` → a cancel
    //     follows the await → does NOT arm (round 11); wake now, so live and
    //     replay both resolve `Cancelled`.
    //   - await-then-cancel `[Arm(true), Cancel]` and cancel-then-await
    //     `[Cancel, Arm(true)]` → forward cancel / backward-dead respectively →
    //     wake now (round 7 behavior preserved).
    let await_arm_contributes = |id: &str, at: usize| -> bool {
        // (forward) a later same-id cancel supersedes the await.
        let cancelled_after = commands[at + 1..].iter().any(|cmd| {
            matches!(cmd, WorkflowCommand::CancelTimer { timer_id } if timer_id.as_str() == id)
        });
        if cancelled_after {
            return false;
        }
        // (backward) X must not be cancelled-without-reset before this arm.
        let mut live = true;
        for cmd in &commands[..at] {
            match cmd {
                WorkflowCommand::CancelTimer { timer_id } if timer_id.as_str() == id => {
                    live = false;
                }
                WorkflowCommand::ArmTimer {
                    timer_id,
                    for_await: false,
                    ..
                } if timer_id.as_str() == id => {
                    live = true;
                }
                _ => {}
            }
        }
        live
    };

    let mut armed_indices = Vec::new();
    for (i, cmd) in commands.iter().enumerate() {
        match cmd {
            WorkflowCommand::ArmTimer {
                timer_id,
                for_await: true,
                ..
            } => {
                if start_timer_ids.contains(timer_id.as_str())
                    || !await_arm_contributes(timer_id.as_str(), i)
                {
                    continue;
                }
                armed_indices.push(i);
            }
            WorkflowCommand::CancelTimer { timer_id } => {
                events_by_index[i] = Some(WorkflowEvent::TimerCancelled {
                    timer_id: timer_id.clone(),
                });
            }
            // Fresh arms (`ArmTimer { for_await: false }`) never insert a row —
            // their `TimerStarted` is already in `events_by_index`.
            _ => {}
        }
    }

    (events_by_index, armed_indices)
}

/// DB-mutation phase (issue #768) for the `WorkflowCommand::ArmTimer` /
/// `WorkflowCommand::CancelTimer` bookkeeping commands: the durable side of
/// author-controlled cancellable/renewable timers.
///
/// Runs inside the caller's transaction (mirroring
/// [`apply_race_loser_cancellations`] and `DetachedSpawnPersistence::persist`),
/// **before** the caller appends the suspension event list. It performs only the
/// `harvest_timers` row upserts/deletes and decides which command position
/// should emit which `TimerStarted` / `TimerCancelled` event — it does **not**
/// append any events. The event/deadline plan itself is the pure, unit-tested
/// [`plan_timer_lifecycle_pure`]; this fn only resolves the actual `fires_at`
/// timestamps and performs the row upserts/deletes. The caller then merges those
/// events into the single position-ordered suspension event batch via
/// [`build_suspension_events`], so a `TimerStarted` lands at its `ArmTimer`
/// command's emission position instead of being forced to the end of the batch
/// (the FINDING-1 replay bug).
///
/// - `ArmTimer { for_await: false }` (a **fresh arm**, from `start_timer` /
///   `reset`): emits a positional `TimerStarted` event (resolved purely by
///   [`arm_timer_events`], with in-batch active-set + `StartTimer`-owned dedup)
///   but inserts **no** `harvest_timers` row. A cancellable timer becomes
///   fire-eligible only when it is awaited, so an armed-but-unawaited timer can
///   never fire spuriously while the workflow is parked on some other wait
///   (an activity, a signal, a child workflow) — this is the round-4 fix for the
///   spurious-fire-breaks-reset/replay bug.
/// - `ArmTimer { for_await: true }` (a **re-arm for firing**, from
///   `await_timer_fire`): upserts the `harvest_timers` row (`fires_at =
///   db_clock_now + duration`, dedup by `timer_id` — idempotent on re-park),
///   contributes its `fires_at` to `min_fires_at`, and emits **no** event (the
///   arm's `TimerStarted` was already recorded by the fresh arm). An id that also
///   has a `StartTimer` command in the batch is skipped (owned by
///   `persist_started_timer`), and an await arm followed by a same-id
///   `CancelTimer` later in the batch is skipped too (round 7 fix, per-await-arm
///   order-sensitive in rounds 9/11 — see [`plan_timer_lifecycle_pure`]): it
///   neither arms a row nor contributes to `min_fires_at`, so an `await_fire`
///   raced (or reset) by a sibling branch's later `cancel` wakes immediately
///   instead of rescheduling to the deleted row's deadline — while a
///   reset-then-await (whose await arm is last, with no cancel after it) still
///   arms the durable row in this transaction.
/// - `CancelTimer { id }`: delete the pending (`fired = false`) row via
///   [`queue::delete_pending_timer`] and resolve `TimerCancelled`. The delete
///   filters `fired = false`, so a fire that already committed is a no-op and the
///   recorded-history order (`TimerFired` vs `TimerCancelled`) decides the
///   observed outcome.
///
/// Row inserts are therefore driven entirely by the command's `for_await` field
/// (and same-batch cancel), not by which persist path runs: a fresh arm never
/// leaks a `harvest_timers` row on any path (including the terminal seal), and
/// `for_await: true` re-arms only ever appear in the bookkeeping-only
/// `await_fire` park.
///
/// Returns `(events_by_index, min_fires_at)`:
/// - `events_by_index` is aligned to `commands` (`len == commands.len()`); a
///   `Some` at a fresh-`ArmTimer` / `CancelTimer` index carries the event to emit
///   at that position. The caller passes it to [`build_suspension_events`].
/// - `min_fires_at` is the **minimum** armed `fires_at` across all contributing
///   `for_await: true` re-arms (or `None` when none armed a deadline — the case on
///   every path except `await_fire`, and also when every awaited arm was cancelled
///   in the same batch), so the bookkeeping-only `await_fire` park can reschedule
///   the workflow task to that instant.
async fn plan_timer_lifecycle(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    commands: &[WorkflowCommand],
) -> HarvestResult<(
    Vec<Option<WorkflowEvent>>,
    Option<chrono::DateTime<chrono::Utc>>,
)> {
    use crate::schema::harvest_timers;

    // Fast path: nothing to do when the batch carries no timer bookkeeping.
    if !commands.iter().any(|c| {
        matches!(
            c,
            WorkflowCommand::ArmTimer { .. } | WorkflowCommand::CancelTimer { .. }
        )
    }) {
        return Ok((vec![None; commands.len()], None));
    }

    // Defensive guard (issue #768, Codex P2 round 15): a batch must never carry a
    // LIVE, un-cancelled cancellable arm (`ArmTimer(X)`) alongside a classic
    // `StartTimer(X)` for the same id. `ctx.timer`/`sleep_until` already reject
    // that collision at the source (see `WorkflowContext::timer`), so this should
    // be unreachable — but if a future path ever lets both land in one batch, fail
    // fast here rather than silently dropping the arm's `TimerStarted`
    // (`arm_timer_events`' `StartTimer`-owned skip) and corrupting history. The
    // legit cancel-then-classic pattern — `[ArmTimer(X,false), CancelTimer(X),
    // StartTimer(X)]` — is NOT a collision (the arm was cancelled before the classic
    // start), so the check requires the arm to be uncancelled BEFORE the same-id
    // `StartTimer`.
    if let Some(id) = same_batch_uncancelled_arm_start_collision(commands) {
        return Err(HarvestError::Config(format!(
            "timer id '{id}' has both a live cancellable start_timer arm and a classic \
             ctx.timer/sleep_until in the same suspension batch — these two timer APIs must use \
             distinct ids (issue #768)"
        )));
    }

    // Pure event plan + the `for_await: true` arm indices that contribute a
    // deadline (already excludes `StartTimer`-owned and same-batch-cancelled ids).
    let (events_by_index, armed_indices) = plan_timer_lifecycle_pure(commands);

    // Row deletes for every `CancelTimer` (idempotent; `delete_pending_timer`
    // filters `fired = false`). Deletes run entirely BEFORE the insert loop below,
    // so for a reset-then-await id (delete the old row, then arm a fresh await row)
    // the insert wins and the row ends present — matching the `armed_indices` the
    // pure planner encodes. An await-raced-by-cancel and an await-then-reset id are
    // both excluded from `armed_indices` (the later same-id cancel supersedes the
    // await arm), so only their delete runs and the row ends absent.
    //
    // Invariant (issue #768, Codex P2 round 16): a `CancelTimer` here can only name
    // a CANCELLABLE timer's id. `WorkflowContext::cancel_timer`/`reset_timer` no-op
    // (emit no `CancelTimer`) unless the id is a live cancellable arm / not a
    // classic timer id, so `delete_pending_timer` can never delete a classic
    // `ctx.timer`/`sleep_until` row out from under its parked waiter.
    for cmd in commands {
        if let WorkflowCommand::CancelTimer { timer_id } = cmd {
            queue::delete_pending_timer(conn, exec_id, timer_id).await?;
        }
    }

    // Upsert the contributing armed rows and fold each `fires_at` into
    // `min_fires_at`.
    let mut min_fires_at: Option<chrono::DateTime<chrono::Utc>> = None;
    for &i in &armed_indices {
        let WorkflowCommand::ArmTimer {
            timer_id,
            duration_secs,
            ..
        } = &commands[i]
        else {
            continue;
        };
        let existing: Option<HarvestTimer> = harvest_timers::table
            .filter(harvest_timers::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(harvest_timers::timer_id.eq(timer_id.as_str()))
            .filter(harvest_timers::fired.eq(false))
            .first::<HarvestTimer>(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;

        let fires_at = if let Some(ref ext) = existing {
            ext.fires_at
        } else {
            let fire_delay = chrono_duration_from_secs(*duration_secs, "timer duration")?;
            let db_now = db_clock_now(conn).await?;
            let fires_at = db_now + fire_delay;
            let new_timer = NewHarvestTimer {
                workflow_exec_id: exec_id.as_uuid(),
                timer_id: timer_id.as_str(),
                fires_at,
            };
            diesel::insert_into(harvest_timers::table)
                .values(&new_timer)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            fires_at
        };
        min_fires_at =
            Some(min_fires_at.map_or(fires_at, |existing_min| existing_min.min(fires_at)));
    }

    Ok((events_by_index, min_fires_at))
}

#[allow(clippy::too_many_lines)]
async fn handle_suspended_workflow(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    mut context: SuspendedWorkflowContext<'_>,
    commands: &[WorkflowCommand],
) -> HarvestResult<()> {
    // Persist UpdateCompleted/UpdateFailed events for any update handlers that
    // ran in this execution cycle before the suspension side-effects.
    // next_event_id is advanced so subsequent persist calls use correct IDs.
    if let Err(e) = persist_update_result_commands(
        conn,
        context.persistence.exec_id,
        commands,
        &mut context.persistence.next_event_id,
    )
    .await
    {
        return fail_execution_on_error(
            conn,
            context.persistence.task,
            context.persistence.worker_id,
            Err(e),
        )
        .await;
    }

    // Apply any search-attribute merge-patches before recording the suspension.
    if let Err(e) =
        persist_search_attrs_from_commands(conn, context.persistence.exec_id, commands).await
    {
        return fail_execution_on_error(
            conn,
            context.persistence.task,
            context.persistence.worker_id,
            Err(e),
        )
        .await;
    }

    // Persist the last current_details breadcrumb from this execution cycle (issue #473).
    if let Err(e) =
        persist_current_details_from_commands(conn, context.persistence.exec_id, commands).await
    {
        return fail_execution_on_error(
            conn,
            context.persistence.task,
            context.persistence.worker_id,
            Err(e),
        )
        .await;
    }
    // Fire ephemeral progress chunks (issue #791) — best-effort, never fails the cycle.
    notify_progress_from_commands(conn, context.persistence.exec_id, commands).await;

    let sticky = context.persistence.sticky_hint();
    let detached_spawns = DetachedSpawnPersistence {
        registry,
        parent_execution: context.execution,
        execute_span: context.execute_span,
    };

    let result = if should_requeue_signal_wait(commands) {
        persist_signal_wait_park(
            conn,
            detached_spawns,
            context.persistence.task.id,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            commands,
            sticky,
        )
        .await
    } else if only_bookkeeping_commands(commands) {
        persist_bookkeeping_and_requeue_workflow(
            conn,
            detached_spawns,
            context.persistence.task.id,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            commands,
            sticky,
        )
        .await
    } else if let Some(scheduled) = extract_all_scheduled_activities(commands) {
        persist_scheduled_activities(
            conn,
            registry,
            detached_spawns,
            context.persistence.task.id,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            commands,
            &scheduled,
            sticky,
            context.execute_span,
            context.execution.assigned_build_id.as_deref(),
            context.persistence.task.priority,
            context.execution.context_headers.as_ref(),
            &context.execution.input,
        )
        .await
    } else if let Some(activity_ids) = extract_all_activity_waits(commands) {
        persist_activity_wait_park(
            conn,
            detached_spawns,
            context.persistence.task.id,
            context.persistence.exec_id,
            commands,
            &activity_ids,
            sticky,
        )
        .await
    } else if let Some((child, timer)) = extract_child_timeout_race(commands) {
        // Child-timeout race (issue #779): a single StartChildWorkflow + StartTimer
        // batch. Must be checked BEFORE the plain timer and plain child branches
        // so it is not misrouted to persist_started_timer (which would drop the
        // child) or persist_all_started_child_workflows (which would not arm the
        // deadline). extract_child_timeout_race rejects any other shape.
        let res = persist_child_timeout_race(
            conn,
            registry,
            context.persistence.task.id,
            context.execution,
            commands,
            &child,
            &timer,
            sticky,
            context.execute_span,
        )
        .await;
        if res.is_ok() {
            #[allow(clippy::cast_precision_loss)]
            let duration_secs = timer.duration_secs as f64;
            registry
                .telemetry()
                .metrics
                .record_timer_started(duration_secs);
        }
        res
    } else if let Some(timer) = extract_started_timer_for_suspension(commands) {
        let res = persist_started_timer(
            conn,
            detached_spawns,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            context.persistence.task.id,
            commands,
            &timer,
            sticky,
            &context.resolved_inline_external,
        )
        .await;
        if res.is_ok() {
            #[allow(clippy::cast_precision_loss)]
            let duration_secs = timer.duration_secs as f64;
            registry
                .telemetry()
                .metrics
                .record_timer_started(duration_secs);
        }
        res
    } else if let Some(children) = extract_all_started_child_workflows(commands) {
        persist_all_started_child_workflows(
            conn,
            registry,
            context.persistence.task.id,
            context.execution,
            commands,
            &children,
            sticky,
            context.execute_span,
        )
        .await
    } else if let Some(scheduled) = extract_single_schedule_external_activity(commands) {
        persist_scheduled_external_activity(
            conn,
            detached_spawns,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            context.persistence.task.id,
            commands,
            &scheduled,
            sticky,
        )
        .await
    } else {
        let error = suspended_workflow_error(commands);
        persist_workflow_failure(
            conn,
            context.persistence.task.id,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            context.persistence.worker_id,
            &error,
            None,
            None,
            None,
            None,
            None,
            crate::types::Priority::default(),
        )
        .await
        .map(|_| ())
    };

    fail_execution_on_error(
        conn,
        context.persistence.task,
        context.persistence.worker_id,
        result,
    )
    .await
}

async fn fail_execution_on_error<T>(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
    result: HarvestResult<T>,
) -> HarvestResult<T> {
    let error = match result {
        Ok(val) => return Ok(val),
        Err(e) => e,
    };
    fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
    Err(error)
}

/// Terminal-fail wrapper for the `process_workflow_task` drive loop that also
/// clears the execution's consecutive-panic strike entry (issue #782).
///
/// An early terminal-fail inside the drive loop (a transient error from an
/// inline persist/local-activity DB call) ends the execution permanently
/// *before* reaching the panic gate that would otherwise clear the strike. Any
/// strike entry left by a prior contained-panic re-dispatch must be cleared
/// here or it leaks one `u32` per such execution. The panic re-dispatch path
/// returns `Ok(())` and is never routed through this wrapper, so its
/// just-incremented strike is preserved (mirrors `workflow_task_timeout_strikes`).
async fn fail_workflow_execution_clearing_strikes<T>(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
    result: HarvestResult<T>,
    workflow_panic_strikes: &std::sync::Mutex<std::collections::HashMap<uuid::Uuid, u32>>,
    exec_id: uuid::Uuid,
) -> HarvestResult<T> {
    if result.is_err() {
        clear_panic_strike(workflow_panic_strikes, exec_id);
    }
    fail_execution_on_error(conn, task, worker_id, result).await
}

async fn load_task_execution(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
) -> HarvestResult<WorkflowExecution> {
    let error = match load_workflow_execution(conn, exec_id).await {
        Ok(val) => return Ok(val),
        Err(e) => e,
    };
    fail_task_only(conn, task.id, &error.to_string()).await?;
    Err(error)
}

async fn load_workflow_replay_state(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
    exec_id: ExecutionId,
    sticky_timeout: Duration,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
) -> HarvestResult<Option<(store::EventHistory, Vec<TimerId>, Vec<String>)>> {
    let history_result = store::load_history_inflated(
        conn,
        exec_id,
        &crate::payload_codec::PayloadCodecs::default(),
        offloader,
    )
    .await;
    let initial_history = fail_execution_on_error(conn, task, worker_id, history_result).await?;

    // Single chronological ingest: due timer fires and pending signals are
    // appended in occurrence order (signal received before a deadline lands
    // before that TimerFired) — see merge_wake_events. A transient event-id
    // conflict with a concurrent append_single_event committer re-drives the
    // task instead of failing the run (issue #779).
    let Some((timers_fired, signals_delivered)) = ingest_wake_events_or_requeue(
        conn,
        task,
        worker_id,
        sticky_timeout,
        exec_id,
        initial_history.next_event_id,
    )
    .await?
    else {
        return Ok(None);
    };

    let final_history_result = store::load_history_inflated(
        conn,
        exec_id,
        &crate::payload_codec::PayloadCodecs::default(),
        offloader,
    )
    .await;
    let final_history =
        fail_execution_on_error(conn, task, worker_id, final_history_result).await?;
    Ok(Some((final_history, timers_fired, signals_delivered)))
}

/// Prepare the workflow task, checking the in-process LRU cache first.
///
/// On a cache **hit** the worker already holds the event history snapshot from
/// the previous suspension in its local `WorkflowCache`.  Only delta events
/// (timer firings and signals appended since the last suspension) are loaded
/// from Postgres, and the full history is reconstructed as
/// `cached_events + delta_events`.  This cuts Postgres event-store reads from
/// `O(history_size)` to `O(new_events)` on warm executions.
///
/// On a cache **miss** (first task, evicted entry, or cache disabled when
/// `sticky_timeout == 0`) the function falls back to the full `load_history`
/// path.
async fn prepare_workflow_task_with_cache(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
    workflow_cache: &tokio::sync::Mutex<crate::cache::WorkflowCache>,
    sticky_timeout: Duration,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
) -> HarvestResult<Option<PreparedWorkflowTask>> {
    let Some(exec_uuid) = task.workflow_exec_id else {
        let error = HarvestError::Config("workflow task missing workflow_exec_id".into());
        fail_task_only(conn, task.id, &error.to_string()).await?;
        return Err(error);
    };
    let exec_id = execution_id_from_uuid(exec_uuid);

    // Only probe the cache when sticky routing is enabled (lease_ttl > 0).
    // With sticky_timeout == 0 the cache is permanently disabled: no lookups,
    // no inserts, no memory consumed — the whole warm-cache path is skipped.
    let cached = if sticky_timeout.is_zero() {
        None
    } else {
        // Brief lock to check cache without holding it during DB work.
        let mut guard = workflow_cache.lock().await;
        guard.get(&exec_uuid).cloned()
    };

    let execution = load_task_execution(conn, task, exec_id).await?;

    if let Some(ref cached_state) = cached {
        // Cache hit path: first load any events already appended since the
        // cache snapshot (e.g. by timeout.rs/external_task.rs via
        // append_single_event), then ingest timers/signals at the REAL current
        // next_event_id to avoid a unique-constraint collision on event_id.
        let existing_delta_result = store::load_history_since_inflated(
            conn,
            exec_id,
            cached_state.next_event_id,
            &crate::payload_codec::PayloadCodecs::default(),
            offloader,
        )
        .await;
        let existing_delta =
            fail_execution_on_error(conn, task, worker_id, existing_delta_result).await?;

        // Single chronological ingest of due timers + pending signals (see
        // merge_wake_events for the occurrence-order contract). A transient
        // event-id conflict with a concurrent append_single_event committer
        // re-drives the task instead of failing the run (issue #779).
        let Some((timers_fired, signals_delivered)) = ingest_wake_events_or_requeue(
            conn,
            task,
            worker_id,
            sticky_timeout,
            exec_id,
            existing_delta.next_event_id,
        )
        .await?
        else {
            return Ok(None);
        };

        // Load events appended by the ingest.
        let after_ingest_result = store::load_history_since_inflated(
            conn,
            exec_id,
            existing_delta.next_event_id,
            &crate::payload_codec::PayloadCodecs::default(),
            offloader,
        )
        .await;
        let after_ingest =
            fail_execution_on_error(conn, task, worker_id, after_ingest_result).await?;

        // Reconstruct full history: cached snapshot + any pre-existing delta +
        // ingested timer/signal events.
        let mut history_events = cached_state.events.clone();
        history_events.extend(existing_delta.events);
        history_events.extend(after_ingest.events);
        let next_event_id = after_ingest.next_event_id;

        Ok(Some(PreparedWorkflowTask {
            execution,
            exec_id,
            history_events,
            next_event_id,
            timers_fired,
            signals_delivered,
            was_cache_hit: true,
        }))
    } else {
        // Cache miss path: full history load. A transient event-id conflict
        // re-drives the task (issue #779), surfaced here as `None`.
        let Some((history, timers_fired, signals_delivered)) =
            load_workflow_replay_state(conn, task, worker_id, exec_id, sticky_timeout, offloader)
                .await?
        else {
            return Ok(None);
        };

        Ok(Some(PreparedWorkflowTask {
            execution,
            exec_id,
            history_events: history.events,
            next_event_id: history.next_event_id,
            timers_fired,
            signals_delivered,
            was_cache_hit: false,
        }))
    }
}

/// Atomically seal the current execution as `CONTINUED_AS_NEW` and start a
/// fresh execution with the same logical `WorkflowId`, a new `ExecutionId`,
/// and an empty event history. Pending unconsumed signals on the old
/// execution are reassigned to the new one so that signals delivered during
/// the transition window are not lost.
///
/// Continue-as-new is intentionally restricted to root workflows. Allowing it
/// from a child workflow would require either reparenting the new run (which
/// changes the spawn-time logical identity its parent recorded) or orphaning
/// the parent's `ChildWorkflow*` waiter, neither of which has a sound default
/// in Phase 1. Callers from a child workflow get an explicit failure instead.
async fn reject_child_continue_as_new(
    conn: &mut AsyncPgConnection,
    persistence: &WorkflowTaskPersistence<'_>,
    execution: &WorkflowExecution,
) -> HarvestResult<bool> {
    let Some(parent_exec_id) = execution.parent_id.map(execution_id_from_uuid) else {
        return Ok(false);
    };

    let error = "continue_as_new is not supported in child workflows in this release";
    if execution.parent_close_policy.is_some() {
        persist_workflow_failure(
            conn,
            persistence.task.id,
            persistence.exec_id,
            persistence.next_event_id,
            persistence.worker_id,
            error,
            None,
            None,
            None,
            None,
            None,
            crate::types::Priority::default(),
        )
        .await?;
    } else {
        persist_child_workflow_failure(
            conn,
            persistence.task.id,
            persistence.exec_id,
            persistence.next_event_id,
            persistence.worker_id,
            parent_exec_id,
            error,
            None,
            None,
        )
        .await?;
    }

    Ok(true)
}

#[allow(clippy::too_many_lines)]
async fn persist_workflow_continue_as_new(
    conn: &mut AsyncPgConnection,
    persistence: WorkflowTaskPersistence<'_>,
    execution: &WorkflowExecution,
    input: serde_json::Value,
    offloader: Option<&crate::payload_store::PayloadOffloader>,
) -> HarvestResult<()> {
    use crate::schema::{harvest_signals, harvest_workflow_executions};

    if reject_child_continue_as_new(conn, &persistence, execution).await? {
        return Ok(());
    }

    // Carry the predecessor's `last_completion_result` forward by its *stored*
    // representation (issue #524 / #488). If it was offloaded, copy the
    // reference envelope verbatim and record a new ref for the successor so the
    // blob is NOT re-uploaded; the offloader skips already-enveloped fields.
    let raw_carryover = store::load_raw_started_carryover(conn, persistence.exec_id).await?;
    let carried_lcr_ref = raw_carryover
        .as_ref()
        .and_then(crate::payload_store::extract_offload_ref);
    let carryover_for_event = raw_carryover.or_else(|| persistence.carryover_result.clone());

    // The new execution stays on the same shard so all of its event log,
    // queue rows, timers, and signals continue to live in the same Postgres
    // database as its predecessor.
    let new_exec_id = ExecutionId::new_for_shard(persistence.exec_id.shard());
    let task_id = persistence.task.id;
    let exec_id = persistence.exec_id;
    // Provenance ref for the successor is the predecessor execution id (#740).
    let predecessor_exec_id_str = exec_id.to_string();
    let next_event_id = persistence.next_event_id;
    let worker_id = persistence.worker_id;
    let started_event = WorkflowEvent::WorkflowStarted {
        input: input.clone(),
        timestamp: chrono::Utc::now(),
        // Preserve scheduled carryover across the fork (issue #488): the continuation is
        // the same logical scheduled run, so it must see the same frozen values rather
        // than re-resolving (which could pick up a newer sibling fire's output).
        last_completion_result: carryover_for_event,
        last_error: persistence.carryover_error.clone(),
        // Preserve the nominal scheduled slot across the fork (issue #508): a continued
        // run is the same logical scheduled run and must see the same slot. The row
        // already copies `scheduled_for` at the NewWorkflowExecution level (L4950).
        scheduled_time: persistence.carryover_scheduled_time,
    };
    let continued_event = WorkflowEvent::WorkflowContinuedAsNew {
        new_exec_id,
        input: input.clone(),
    };
    // Re-anchor deadline to the new execution's start time (issue #243).
    let new_deadline_at = execution.execution_timeout.map(|d| chrono::Utc::now() + d);
    // Re-anchor soft SLA deadline per-run (issue #487).
    let new_sla_deadline_at = execution.sla.map(|d| chrono::Utc::now() + d);

    let new_row = NewWorkflowExecution {
        id: new_exec_id.as_uuid(),
        workflow_name: &execution.workflow_name,
        workflow_id: &execution.workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: execution.shard_id,
        input: input.clone(),
        parent_id: None,
        queue_name: &execution.queue_name,
        execution_timeout: execution.execution_timeout,
        deadline_at: new_deadline_at,
        sla: execution.sla,
        sla_deadline_at: new_sla_deadline_at,
        memo: execution.memo.clone(),
        search_attrs: execution.search_attrs.clone(),
        assigned_build_id: execution.assigned_build_id.clone(),
        parent_close_policy: None, // root workflow
        owner: execution.owner.as_deref(),
        runbook_url: execution.runbook_url.as_deref(),
        severity: execution.severity.as_deref(),
        context_headers: execution.context_headers.clone(),
        schedule_id: execution.schedule_id, // preserve schedule lineage through continue-as-new
        // Same logical slot as the predecessor: keep carryover ordering stable so the
        // continuation isn't treated as a brand-new fire (issue #488).
        scheduled_for: execution.scheduled_for,
        // Continue-as-new starts a fresh run: attempt counter resets to 1,
        // but the retry policy is carried forward so the chain can still retry
        // transient failures on the continued run.
        workflow_attempt: 1,
        workflow_retry_policy: execution.workflow_retry_policy.clone(),
        retry_of_exec_id: None,
        // Preserve dispatch origin through continue-as-new so a continued scheduled run
        // stays attributed to its schedule's cadence (issue #534).
        origin: execution.origin.as_deref(),
        // Continue-as-new is the same logical run forking forward: preserve
        // the predecessor's completion-callback targets (issue #605).
        completion_callbacks: execution.completion_callbacks.clone(),
        // Back-link this successor to its predecessor and to the chain origin
        // (issue #701) so the run-chain timeline can walk the whole chain from any
        // member. The origin is the predecessor's own `first_exec_id` if it is
        // itself a successor, else the predecessor's own id.
        continued_from_exec_id: Some(exec_id.as_uuid()),
        first_exec_id: Some(execution.first_exec_id.unwrap_or(execution.id)),
        // A continue-as-new successor has its OWN provenance — it is never
        // re-attributed to the predecessor's source (issue #740 AC3). Ref is
        // the predecessor execution id.
        start_source: Some(crate::types::StartSource::ContinueAsNew.as_str()),
        start_source_ref: Some(predecessor_exec_id_str.as_str()),
        started_by: None,
    };
    let mut enqueue =
        queue::EnqueueParams::new(execution.queue_name.clone(), TaskType::Workflow, input);
    enqueue.workflow_exec_id = Some(new_exec_id.as_uuid());
    enqueue.required_build_id = execution.assigned_build_id.clone();
    // Propagate the concurrency key from the current task so the new run
    // continues to be governed by the same fair-share cap (issue #247).
    enqueue.concurrency_key = persistence.task.concurrency_key.clone();
    enqueue.max_concurrent = persistence
        .task
        .concurrency_cap
        .and_then(|cap| u32::try_from(cap).ok());
    enqueue.rate_limit_key = persistence.task.rate_limit_key.clone();

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            // Append the terminal continued-as-new marker to the old run.
            store::append_events_offloaded(
                conn,
                exec_id,
                &[continued_event],
                next_event_id,
                offloader,
            )
            .await?;

            // Seal the old execution. The CHECK constraint allows this state
            // value as of the continue-as-new migration; the partial unique
            // index on (workflow_name, workflow_id) excludes CONTINUED_AS_NEW
            // so the new row below can reuse the same logical identity.
            let updated =
                diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                    .filter(harvest_workflow_executions::state.eq("RUNNING"))
                    .set((
                        harvest_workflow_executions::state.eq("CONTINUED_AS_NEW"),
                        harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                        harvest_workflow_executions::error.eq(None::<String>),
                        harvest_workflow_executions::sticky_worker_id
                            .eq(Some(worker_id.to_string())),
                        harvest_workflow_executions::completed_at.eq(Some(chrono::Utc::now())),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;
            if updated == 0 {
                return Err(workflow_execution_transition_error(conn, exec_id).await?);
            }

            diesel::insert_into(harvest_workflow_executions::table)
                .values(&new_row)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            store::append_events_offloaded(conn, new_exec_id, &[started_event], 0, offloader)
                .await?;
            // Record the carried-forward blob reference for the successor so the
            // blob survives until the successor is also retained (issue #524).
            if let Some(ref carried) = carried_lcr_ref {
                store::insert_payload_refs(conn, new_exec_id, std::slice::from_ref(carried))
                    .await?;
            }

            // Reassign unconsumed signals to the new execution so signals
            // delivered while the workflow body was running do not disappear
            // through the transition. Consumed signals stay on the old run
            // for audit purposes.
            diesel::update(
                harvest_signals::table
                    .filter(harvest_signals::workflow_exec_id.eq(exec_id.as_uuid()))
                    .filter(harvest_signals::consumed.eq(false)),
            )
            .set(harvest_signals::workflow_exec_id.eq(new_exec_id.as_uuid()))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

            queue::enqueue(conn, &enqueue).await?;
            queue::complete_task(conn, task_id, serde_json::Value::Null).await?;
            Ok(())
        }
        .scope_boxed()
    })
    .await
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn persist_workflow_outcome(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    execution: &WorkflowExecution,
    persistence: WorkflowTaskPersistence<'_>,
    outcome: WorkflowOutcome,
    execute_span: &tracing::Span,
    // When `false` the caller is running inside a transaction and will call
    // the schedule counter helpers itself AFTER the transaction commits, so
    // they must not be called here (a failed counter query inside a Postgres
    // transaction aborts the whole transaction).
    update_schedule_counter: bool,
    // Issue #678/#1034: external-op ids resolved inline this cycle. The
    // `Suspended` arm self-wakes the parked task immediately when this is
    // non-empty (any mixed-suspension shape whose same-shard external op resolved
    // inline), and also threads it into the `SuspendedWorkflowContext`. Empty for
    // every non-suspension outcome and for a pure suspension.
    resolved_inline_external: ResolvedExternalIds,
) -> HarvestResult<(bool, Vec<(ExecutionId, Option<String>)>)> {
    let parent_exec_id = execution.parent_id.map(execution_id_from_uuid);
    // A detached child has parent_close_policy set (non-null). Detached children
    // do NOT wake their parent on completion or failure.
    let is_detached_child = execution.parent_close_policy.is_some();

    match (outcome, parent_exec_id) {
        (WorkflowOutcome::Completed { output, .. }, Some(parent_id)) if !is_detached_child => {
            let res = persist_child_workflow_completion(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                parent_id,
                output,
                Some(registry.telemetry().metrics.as_ref()),
            )
            .await?;
            Ok((false, vec![res]))
        }
        (WorkflowOutcome::Completed { output, .. }, _) => {
            // Root workflow or detached child completing — no parent wake.
            let res = persist_workflow_completion(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                output,
                Some(registry.telemetry().metrics.as_ref()),
                registry.payload_offloader(),
            )
            .await?;
            if update_schedule_counter {
                crate::scheduler::maybe_reset_schedule_failure_counter(
                    conn,
                    &execution.workflow_id,
                    &execution.workflow_name,
                    execution.schedule_id,
                    execution.origin.as_deref(),
                )
                .await;
            }
            Ok((false, vec![res]))
        }
        (
            WorkflowOutcome::Failed {
                error,
                non_deterministic_details,
                ..
            },
            Some(parent_id),
        ) if !is_detached_child => {
            let res = persist_child_workflow_failure(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                parent_id,
                &error,
                non_deterministic_details.as_ref(),
                Some(registry.telemetry().metrics.as_ref()),
            )
            .await?;
            if update_schedule_counter {
                crate::scheduler::maybe_increment_schedule_failure_counter(
                    conn,
                    &execution.workflow_id,
                    &execution.workflow_name,
                    execution.schedule_id,
                    execution.origin.as_deref(),
                    registry.telemetry().metrics.as_ref(),
                )
                .await;
            }
            Ok((false, vec![res]))
        }
        (
            WorkflowOutcome::Failed {
                error,
                non_deterministic_details,
                ..
            },
            _,
        ) => {
            // Root workflow or detached child failing — no parent wake.
            // Returns true if a retry was scheduled (propagate to caller so
            // the deferred schedule-failure counter can be suppressed).
            let (retry_scheduled, res) = persist_workflow_failure(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                &error,
                non_deterministic_details.as_ref(),
                Some(execution),
                Some(registry.telemetry().metrics.as_ref()),
                persistence.task.concurrency_key.clone(),
                persistence
                    .task
                    .concurrency_cap
                    .and_then(|c| u32::try_from(c).ok()),
                crate::types::Priority::from_i32(persistence.task.priority).unwrap_or_default(),
            )
            .await?;
            if update_schedule_counter && !retry_scheduled {
                crate::scheduler::maybe_increment_schedule_failure_counter(
                    conn,
                    &execution.workflow_id,
                    &execution.workflow_name,
                    execution.schedule_id,
                    execution.origin.as_deref(),
                    registry.telemetry().metrics.as_ref(),
                )
                .await;
            }
            Ok((retry_scheduled, vec![res]))
        }
        (WorkflowOutcome::Suspended { commands }, _) => {
            // Issue #1034: single arm-level self-wake when a same-shard external
            // op resolved INLINE this cycle (recorded in `resolved_inline_external`
            // by `persist_external_signal_inline`), replacing the timer-only
            // special case in `persist_started_timer` (#678). Gated on `!is_empty()`
            // so a pure suspension / outbox-routed / already-observed cycle never
            // false-wakes. Every re-pend-eligible park shape below is covered
            // (RUNNING-park, or `mixed_signal_suspension`-stamped for the timer
            // shape), so one wake re-pends the task via the primary re-pend query.
            // Inside the enclosing persist txn → park + re-pend commit atomically,
            // NOTIFY defers to the outer commit; no crash window. Full scope + the
            // one known limitation (the #768 armed cancellable-timer `await_fire`
            // remainder is not covered): see the changelog fragment
            // docs/changelog.d/pr-1034-inline-external-nontimer-wake.md.
            let wake_inline_external = !resolved_inline_external.is_empty();
            let exec_id = persistence.exec_id;
            let result = handle_suspended_workflow(
                conn,
                registry,
                SuspendedWorkflowContext {
                    execution,
                    persistence,
                    execute_span,
                    resolved_inline_external,
                },
                &commands,
            )
            .await;
            if result.is_ok() && wake_inline_external {
                queue::wake_workflow_task(conn, exec_id).await?;
            }
            result.map(|()| (false, Vec::new()))
        }
        (WorkflowOutcome::ContinuedAsNew { input }, _) => {
            // task/worker_id are Copy references; capture before persistence is moved.
            let task = persistence.task;
            let worker_id = persistence.worker_id;
            let exec_id = persistence.exec_id;
            let workflow_name = execution.workflow_name.clone();
            let result = persist_workflow_continue_as_new(
                conn,
                persistence,
                execution,
                input,
                registry.payload_offloader(),
            )
            .await;
            fail_execution_on_error(conn, task, worker_id, result)
                .await
                .map(|()| (false, vec![(exec_id, Some(workflow_name))]))
        }
    }
}

/// Outcome of the pause-guarded persistence transaction in
/// [`process_workflow_task`].
enum WorkflowPersistFlow {
    /// The execution was observed `PAUSED` under the row lock: the task was
    /// re-parked inside the same transaction and the pending decision discarded
    /// without persisting any new commands. Resume re-derives the decision on
    /// replay.
    ParkedPaused,
    /// The decision was persisted under the execution row lock.
    /// `retry_scheduled` is `true` when a workflow-level retry was atomically
    /// started inside the failure transaction; the deferred schedule-failure
    /// counter must be suppressed until the retry chain is exhausted.
    Persisted {
        retry_scheduled: bool,
        deferred_checks: Vec<(ExecutionId, Option<String>)>,
        /// `ctx.race()` loser-cancellation completion-trigger starts (issue
        /// #600), gathered from `persist_terminal_outcome_commands` — empty
        /// when the outcome path didn't run `apply_race_loser_cancellations`.
        /// Must only be spawned **after** this transaction commits (see
        /// `apply_race_loser_cancellations`'s doc comment): a start spawned
        /// before commit could run against a cancellation that later rolls
        /// back if this transaction fails.
        race_deferred_triggers: Vec<crate::completion_trigger::DeferredTriggerStart>,
    },
}

/// Map a terminal/suspended outcome to its deferred schedule-failure-counter
/// action: `Some(false)` resets (success), `Some(true)` increments (failure),
/// `None` leaves it untouched (suspended / continue-as-new). The action depends
/// only on Completed-vs-Failed, so it is identical for root and child variants.
const fn schedule_counter_action(outcome: &WorkflowOutcome) -> Option<bool> {
    match outcome {
        WorkflowOutcome::Completed { .. } => Some(false),
        // Defensive (issue #603): an engine-detected replay divergence blocks
        // the execution non-terminally at the gate in `process_workflow_task`
        // and never reaches this counter mapping — but if it ever did, a
        // blocked run is neither a schedule success nor a failure. Asserted
        // (not just commented) so a future regression that lets this arm
        // become reachable panics loudly in debug/test builds instead of
        // silently miscounting.
        WorkflowOutcome::Failed {
            non_deterministic_details: Some(_),
            ..
        } => {
            debug_assert!(
                false,
                "ND-carrying Failed outcome must be gated earlier in \
                 process_workflow_task, before schedule_counter_action is ever called"
            );
            None
        }
        WorkflowOutcome::Failed { .. } => Some(true),
        _ => None,
    }
}

/// Run the deferred schedule-failure-counter update in autocommit, *after* the
/// persistence transaction has committed. Counter queries are best-effort: a
/// failure here must never roll back the durably-persisted workflow decision,
/// which is why they run outside the persistence transaction (a failed query
/// inside a Postgres transaction would abort the whole transaction).
async fn run_deferred_schedule_counter(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    execution: &WorkflowExecution,
    counter_action: Option<bool>,
) {
    match counter_action {
        Some(false) => {
            crate::scheduler::maybe_reset_schedule_failure_counter(
                conn,
                &execution.workflow_id,
                &execution.workflow_name,
                execution.schedule_id,
                execution.origin.as_deref(),
            )
            .await;
        }
        Some(true) => {
            crate::scheduler::maybe_increment_schedule_failure_counter(
                conn,
                &execution.workflow_id,
                &execution.workflow_name,
                execution.schedule_id,
                execution.origin.as_deref(),
                registry.telemetry().metrics.as_ref(),
            )
            .await;
        }
        None => {}
    }
}

/// Persist a terminal (or continue-as-new) outcome that also carries pending
/// pre-suspension commands (update results, search-attr patches, detached
/// children, fan-out markers).
///
/// Runs entirely on the caller's connection **without opening its own
/// transaction** so the caller can wrap it — together with the authoritative
/// `FOR UPDATE` pause guard — in a single transaction (issue #383). Schedule
/// counters are deferred to the caller via [`run_deferred_schedule_counter`]
/// and run only after that outer transaction commits.
async fn persist_terminal_outcome_commands(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    execution: &WorkflowExecution,
    persistence: WorkflowTaskPersistence<'_>,
    outcome: WorkflowOutcome,
    pending_cmds: &[WorkflowCommand],
    execute_span: &tracing::Span,
) -> HarvestResult<(
    bool,
    Vec<(ExecutionId, Option<String>)>,
    Vec<crate::completion_trigger::DeferredTriggerStart>,
)> {
    let mut next_event_id = persistence.next_event_id;
    persist_update_result_commands(conn, persistence.exec_id, pending_cmds, &mut next_event_id)
        .await?;
    persist_search_attrs_from_commands(conn, persistence.exec_id, pending_cmds).await?;
    persist_current_details_from_commands(conn, persistence.exec_id, pending_cmds).await?;
    // Fire ephemeral progress chunks (issue #791) — best-effort, never fails the cycle.
    notify_progress_from_commands(conn, persistence.exec_id, pending_cmds).await;

    // Cancellable/renewable timer bookkeeping (issue #768) on a terminal cycle.
    // A sealing execution never awaits, so any `ArmTimer` here is a fresh arm
    // (`for_await: false`): it records `TimerStarted` (needed for positional
    // replay of a `start_timer`/`reset` that completes in the same task) but
    // inserts NO `harvest_timers` row — so no never-firing row leaks. A trailing
    // `cancel_timer`/`handle.cancel()` cleanup is still honoured (`TimerCancelled`
    // + row delete). Resolve the row mutations FIRST, then interleave the timer
    // events into the pre-terminal batch at each command's emission position
    // (FINDING 1).
    let (mut timer_events, _armed) =
        plan_timer_lifecycle(conn, persistence.exec_id, pending_cmds).await?;
    let pre_terminal = pre_suspension_events_from_commands(pending_cmds, &mut timer_events);
    if !pre_terminal.is_empty() {
        store::append_events(conn, persistence.exec_id, &pre_terminal, next_event_id).await?;
        next_event_id = next_event_id
            .checked_add(i32::try_from(pre_terminal.len()).unwrap_or(i32::MAX))
            .ok_or_else(|| crate::error::HarvestError::Database("Event ID overflow".to_string()))?;
    }

    // A ctx.race() resolving in the same workflow-task cycle as its terminal
    // Complete/Fail (the ≤5-line "race, then return" DX) durably cancels its
    // losers here, in the same transaction the caller wraps this whole
    // function in. The returned starts are NOT spawned here — the caller
    // must only spawn them after that outer transaction commits (see
    // `apply_race_loser_cancellations`'s doc comment).
    let race_deferred_triggers = apply_race_loser_cancellations(
        conn,
        persistence.exec_id,
        pending_cmds,
        &mut next_event_id,
        registry,
    )
    .await?;

    create_detached_child_executions(conn, registry, execution, pending_cmds, execute_span).await?;

    // `persist_search_attrs_from_commands` above wrote the UpsertSearchAttributes
    // patch to the DB, but `execution` is the row loaded before that update. A
    // workflow-retry started from this failure copies `execution.search_attrs`,
    // so apply the same in-memory patch first; otherwise the retry attempt loses
    // attributes set immediately before the transient failure (issue #523 P2).
    let patched_execution =
        match apply_search_attrs_patch_in_memory(execution.search_attrs.clone(), pending_cmds) {
            patched if patched == execution.search_attrs => None,
            patched => {
                let mut e = execution.clone();
                e.search_attrs = patched;
                Some(e)
            }
        };
    let effective_execution = patched_execution.as_ref().unwrap_or(execution);

    // `update_schedule_counter: false` — the caller runs counters after commit.
    // Returns true if a workflow retry was scheduled (root/detached failure path).
    let (retry_scheduled, deferred_checks) = persist_workflow_outcome(
        conn,
        registry,
        effective_execution,
        WorkflowTaskPersistence {
            next_event_id,
            ..persistence
        },
        outcome,
        execute_span,
        false,
        // Terminal-with-commands path never suspends, so no inline external
        // wake is threaded here (issue #678).
        ResolvedExternalIds::default(),
    )
    .await?;
    Ok((retry_scheduled, deferred_checks, race_deferred_triggers))
}

fn pending_update_result_event_count(commands: &[WorkflowCommand]) -> u64 {
    u64::try_from(
        commands
            .iter()
            .filter(|cmd| matches!(cmd, WorkflowCommand::RecordUpdateResult { .. }))
            .count(),
    )
    .unwrap_or(u64::MAX)
}

fn terminal_history_event_count(next_event_id: i32, pending_cmds: &[WorkflowCommand]) -> u64 {
    u64::try_from(next_event_id)
        .unwrap_or(0)
        .saturating_add(pending_update_result_event_count(pending_cmds))
        .saturating_add(1)
}

fn pre_suspension_event_count(commands: &[WorkflowCommand]) -> u64 {
    let simple_events = commands
        .iter()
        .filter(|cmd| {
            matches!(
                cmd,
                WorkflowCommand::RecordMarker { .. }
                    | WorkflowCommand::RecordSideEffect { .. }
                    | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
            )
        })
        .count();
    // Upper bound on the events apply_race_loser_cancellations may append: at most
    // one synthetic ActivityFailed per losing activity branch still open at
    // cancellation time (an already-completed branch produces none), plus at most
    // one ChildWorkflowFailed appended onto *this* execution's own history per
    // newly-cancelled losing child branch (every race child is an awaited child of
    // this execution, so a genuine cancellation notifies this parent inline via
    // notify_awaited_parent_of_child_terminal). Counting every listed activity/child
    // here can overcount but never undercount, so it only ever nudges the history
    // hard-cap check earlier -- never masks a real overflow.
    let race_cancel_events: usize = commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::CancelRaceLosers {
                activities,
                children,
                ..
            } => Some(activities.len().saturating_add(children.len())),
            _ => None,
        })
        .sum();
    let timer_lifecycle_events = timer_lifecycle_event_count(commands);
    u64::try_from(
        simple_events
            .saturating_add(race_cancel_events)
            .saturating_add(timer_lifecycle_events),
    )
    .unwrap_or(u64::MAX)
}

/// Conservative upper bound on the durable timer-lifecycle events an
/// `ArmTimer`/`CancelTimer` batch appends, for the history hard-cap preflight
/// (Codex P2, issue #768). `plan_timer_lifecycle`/`arm_timer_events`
/// append one `TimerStarted` per emitting `ArmTimer` and one `TimerCancelled`
/// per `CancelTimer`; without counting them a near-cap reset batch
/// (`CancelTimer` + `ArmTimer`) would pass the `>= cap` check as ~one pending
/// event yet append two, breaching the hard cap instead of failing to the
/// dead-letter queue first.
///
/// Counted **conservatively** — the preflight runs before the DB-dependent
/// dedup, so an `ArmTimer` whose durable row already exists (an idempotent
/// re-arm that appends nothing) is still counted `+1`. Over-counting only ever
/// nudges the cap check earlier (DLQ one event early), never masks an overflow.
///
/// An `ArmTimer` whose id also has a `StartTimer` in the same batch is
/// **excluded**: `plan_timer_lifecycle` skips it (the `StartTimer` suspension
/// path owns that id's row/event), and its `TimerStarted` is already counted by
/// the `extract_started_timer_for_suspension` branch — counting it here too
/// would double-count.
fn timer_lifecycle_event_count(commands: &[WorkflowCommand]) -> usize {
    let start_timer_ids: HashSet<&str> = commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::StartTimer { timer_id, .. } => Some(timer_id.as_str()),
            _ => None,
        })
        .collect();
    commands
        .iter()
        .filter(|cmd| match cmd {
            WorkflowCommand::ArmTimer { timer_id, .. } => {
                !start_timer_ids.contains(timer_id.as_str())
            }
            WorkflowCommand::CancelTimer { .. } => true,
            _ => false,
        })
        .count()
}

fn pending_detached_parent_close_cascade_event_count(commands: &[WorkflowCommand]) -> u64 {
    u64::try_from(
        commands
            .iter()
            .filter(|cmd| {
                matches!(
                    cmd,
                    WorkflowCommand::SpawnDetachedChildWorkflow {
                        parent_close_policy: ParentClosePolicy::RequestCancel
                            | ParentClosePolicy::Terminate,
                        ..
                    }
                )
            })
            .count(),
    )
    .unwrap_or(u64::MAX)
}

async fn terminal_parent_close_cascade_event_count(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    pending_cmds: &[WorkflowCommand],
) -> HarvestResult<u64> {
    let persisted = parent_close_cascade_event_count(conn, exec_id).await?;
    Ok(
        persisted.saturating_add(pending_detached_parent_close_cascade_event_count(
            pending_cmds,
        )),
    )
}

async fn new_child_workflow_event_count(
    conn: &mut AsyncPgConnection,
    children: &[StartedChildWorkflowCommand],
) -> HarvestResult<u64> {
    let requested_ids: Vec<uuid::Uuid> = children
        .iter()
        .map(|child| child.child_id.as_uuid())
        .collect();
    if requested_ids.is_empty() {
        return Ok(0);
    }

    let existing_ids: HashSet<uuid::Uuid> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq_any(&requested_ids))
        .select(harvest_workflow_executions::id)
        .load::<uuid::Uuid>(conn)
        .await
        .map_err(crate::error::database_error)?
        .into_iter()
        .collect();
    let requested = u64::try_from(children.len()).unwrap_or(u64::MAX);
    let existing = u64::try_from(existing_ids.len()).unwrap_or(u64::MAX);
    Ok(requested.saturating_sub(existing))
}

async fn suspended_command_event_count(
    conn: &mut AsyncPgConnection,
    workflow_exec_id: Option<uuid::Uuid>,
    commands: &[WorkflowCommand],
) -> HarvestResult<u64> {
    let update_events = pending_update_result_event_count(commands);
    let pre_susp_events = pre_suspension_event_count(commands);
    let bookkeeping_events = update_events.saturating_add(pre_susp_events);

    if should_requeue_signal_wait(commands) {
        return Ok(bookkeeping_events);
    }
    if let Some(activities) = extract_all_scheduled_activities(commands) {
        return Ok(
            bookkeeping_events.saturating_add(u64::try_from(activities.len()).unwrap_or(u64::MAX))
        );
    }
    if extract_all_activity_waits(commands).is_some() {
        return Ok(bookkeeping_events);
    }
    // Child-timeout race (issue #779): appends ChildWorkflowStarted + TimerStarted
    // for a fresh dispatch (both new), nothing on a re-park (both already
    // recorded). Checked before the plain timer/child branches so the shape is
    // counted correctly rather than falling through to the +1 default. Mirrors
    // the dispatch chain ordering.
    if let Some((child, timer)) = extract_child_timeout_race(commands) {
        let child_new = new_child_workflow_event_count(conn, std::slice::from_ref(&child)).await?;
        let timer_new = if let Some(exec_uuid) = workflow_exec_id {
            let existing: Option<HarvestTimer> = harvest_timers::table
                .filter(harvest_timers::workflow_exec_id.eq(exec_uuid))
                .filter(harvest_timers::timer_id.eq(timer.timer_id.as_str()))
                .filter(harvest_timers::fired.eq(false))
                .first::<HarvestTimer>(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            u64::from(existing.is_none())
        } else {
            1
        };
        return Ok(bookkeeping_events
            .saturating_add(child_new)
            .saturating_add(timer_new));
    }
    if let Some(timer) = extract_started_timer_for_suspension(commands) {
        let is_new = if let Some(exec_uuid) = workflow_exec_id {
            let existing: Option<HarvestTimer> = harvest_timers::table
                .filter(harvest_timers::workflow_exec_id.eq(exec_uuid))
                .filter(harvest_timers::timer_id.eq(timer.timer_id.as_str()))
                .filter(harvest_timers::fired.eq(false))
                .first::<HarvestTimer>(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            existing.is_none()
        } else {
            true
        };
        let timer_event = u64::from(is_new);
        return Ok(bookkeeping_events.saturating_add(timer_event));
    }
    if let Some(children) = extract_all_started_child_workflows(commands) {
        return Ok(bookkeeping_events
            .saturating_add(new_child_workflow_event_count(conn, &children).await?));
    }
    if let Some(scheduled) = extract_single_schedule_external_activity(commands) {
        let awaiting_event = u64::from(
            external_task::find_by_token(conn, scheduled.token)
                .await?
                .is_none(),
        );
        return Ok(bookkeeping_events.saturating_add(awaiting_event));
    }

    Ok(update_events.saturating_add(1))
}

#[allow(clippy::too_many_arguments)]
async fn move_workflow_to_dlq_for_history_cap(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    parent_exec_id: Option<ExecutionId>,
    reason: DeadLetterReason,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> HarvestResult<(Vec<DeferredTriggerStart>, Vec<(ExecutionId, String)>)> {
    let reason = reason.to_string();

    let (deferred, closed_children) = conn
        .transaction::<_, HarvestError, _>(|conn| {
            let reason = reason.clone();
            async move {
                use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
                let (owner, severity) = exec_dsl::harvest_workflow_executions
                    .find(exec_id.as_uuid())
                    .select((exec_dsl::owner, exec_dsl::severity))
                    .first::<(Option<String>, Option<String>)>(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?
                    .unwrap_or((None, None));
                dlq::dead_letter(
                    conn,
                    &NewDeadLetterEntry {
                        original_task_id: task.id,
                        queue_name: task.queue_name.clone(),
                        task_type: task.task_type.clone(),
                        workflow_exec_id: task.workflow_exec_id,
                        activity_name: task.activity_name.clone(),
                        input: task.input.clone(),
                        error: reason.clone(),
                        attempts: task.attempt,
                        owner,
                        severity,
                    },
                )
                .await?;
                store::append_events(
                    conn,
                    exec_id,
                    &[WorkflowEvent::workflow_failed(reason.clone())],
                    next_event_id,
                )
                .await?;
                update_workflow_execution_failed(conn, exec_id, worker_id, &reason, None).await?;
                queue::fail_task(conn, task.id, &reason).await?;
                // Drain any remaining sibling PENDING/RUNNING task rows so
                // they are not claimed after a future redrive reactivates the
                // execution to RUNNING. Mirrors the poison-pill quarantine and
                // workflow-task-timeout seal paths.
                queue::fail_open_tasks_for_execution(conn, exec_id, &reason).await?;
                let (mut deferred, closed_children) =
                    apply_parent_close_cascade(conn, exec_id).await?;
                let failed_triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                    conn,
                    exec_id,
                    crate::completion_trigger::TerminalState::Failed,
                    metrics,
                )
                .await?;
                deferred.extend(failed_triggers);
                if let Some(parent_exec_id) = parent_exec_id {
                    wake_parent_for_child_failure(conn, parent_exec_id, exec_id, &reason).await?;
                }
                Ok((deferred, closed_children))
            }
            .scope_boxed()
        })
        .await?;

    Ok((deferred, closed_children))
}

#[allow(clippy::too_many_arguments)]
async fn fail_workflow_for_history_cap(
    conn: &mut AsyncPgConnection,
    telemetry: &crate::telemetry::TelemetryConfig,
    task: &TaskQueueItem,
    execution: &WorkflowExecution,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    started_at: std::time::Instant,
    event_count: u64,
    cap: u64,
) -> HarvestResult<Vec<crate::completion_trigger::DeferredTriggerStart>> {
    let terminal_count = u64::try_from(next_event_id).unwrap_or(0).saturating_add(1);
    telemetry.metrics.record_workflow_completed(
        &execution.workflow_name,
        &task.queue_name,
        started_at.elapsed().as_secs_f64(),
        WorkflowStatus::Failed,
    );
    telemetry
        .metrics
        .record_workflow_history_size(&execution.workflow_name, terminal_count);
    telemetry.metrics.record_workflow_terminal(
        &execution.workflow_name,
        &task.queue_name,
        WorkflowStatus::Failed,
    );

    let reason = DeadLetterReason::HistoryCapExceeded {
        count: event_count,
        cap,
        workflow_type: execution.workflow_name.clone(),
    };
    let (deferred, closed_children) = move_workflow_to_dlq_for_history_cap(
        conn,
        task,
        exec_id,
        next_event_id,
        worker_id,
        execution.parent_id.map(execution_id_from_uuid),
        reason,
        Some(telemetry.metrics.as_ref()),
    )
    .await?;

    check_and_report_unfinished_handlers_for_worker(
        conn,
        exec_id,
        Some(&execution.workflow_name),
        Some(telemetry.metrics.as_ref()),
    )
    .await;

    for (child_id, child_name) in closed_children {
        check_and_report_unfinished_handlers_for_worker(
            conn,
            child_id,
            Some(&child_name),
            Some(telemetry.metrics.as_ref()),
        )
        .await;
    }

    Ok(deferred)
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn process_workflow_task(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task: &TaskQueueItem,
    worker_id: &str,
    build_id: &str,
    sticky_timeout: Duration,
    max_local_activity_start_to_close: Duration,
    workflow_cache: Arc<tokio::sync::Mutex<crate::cache::WorkflowCache>>,
    dispatched_at: std::time::Instant,
    // Issue #782: in-process contained-handler-panic strike map (per
    // execution) and the configured re-dispatch budget.
    workflow_panic_strikes: &Arc<std::sync::Mutex<std::collections::HashMap<uuid::Uuid, u32>>>,
    workflow_panic_max_attempts: u32,
) -> HarvestResult<()> {
    let Some(mut prepared) = prepare_workflow_task_with_cache(
        conn,
        task,
        worker_id,
        &workflow_cache,
        sticky_timeout,
        registry.payload_offloader(),
    )
    .await?
    else {
        // Issue #779: a transient wake-event-ingest event-id conflict re-drove
        // the parent task (park + wake). The run was NOT failed; abandon this
        // cycle and let the re-pended task be re-claimed with a fresh history
        // load that advances past the winner's committed event.
        return Ok(());
    };
    let Some(workflow) = registry.workflows.get(&prepared.execution.workflow_name) else {
        let error = format!(
            "no workflow handler registered for '{}'",
            prepared.execution.workflow_name
        );
        fail_task_and_execution(conn, task, worker_id, &error).await?;
        return Err(HarvestError::Config(error));
    };

    let telemetry = registry.telemetry().clone();

    // Emit cache hit/miss metric now that we know the workflow name.
    if prepared.was_cache_hit {
        telemetry
            .metrics
            .record_workflow_cache_hit(&prepared.execution.workflow_name, &task.queue_name);
    } else {
        telemetry
            .metrics
            .record_workflow_cache_miss(&prepared.execution.workflow_name, &task.queue_name);
    }

    let trace_carrier = task
        .trace_context
        .as_ref()
        .and_then(TraceContextCarrier::from_json);

    // ADR-0001 §2.6 + §2.7: emit harvest.signal.deliver and harvest.timer.fire
    // spans here, after the trace context is restored, so they are correlated
    // with the workflow execution trace rather than being orphaned.
    // EnteredSpan is !Send; .in_scope() drops it before any subsequent .await.
    // ADR-0001 §2.7: one span per fired timer.
    for timer_id in &prepared.timers_fired {
        tracing::info_span!(
            "harvest.timer.fire",
            "otel.kind" = "internal",
            { ATTR_EXECUTION_ID } = %prepared.exec_id,
            timer.id = %timer_id,
        )
        .in_scope(|| {});
    }
    for signal_name in &prepared.signals_delivered {
        tracing::info_span!(
            "harvest.signal.deliver",
            "otel.kind" = "consumer",
            { ATTR_WORKFLOW_ID } = prepared.execution.workflow_name.as_str(),
            { ATTR_EXECUTION_ID } = %prepared.exec_id,
            signal.name = signal_name.as_str(),
        )
        .in_scope(|| {});
        // Issue #684: count each durably-delivered signal. This is the single
        // live-only delivery choke point (ingest_due_timers_and_signals) and
        // each signal appears in `signals_delivered` exactly once (marked
        // consumed in harvest_signals), so the counter fires exactly once per
        // delivery. Never emitted on replay. The signal name is NOT a metric
        // label (issue #684, Codex P2 — free-form send route, no declared
        // registry to bound it); it stays a span-only attribute above.
        telemetry
            .metrics
            .record_signal_received(&prepared.execution.workflow_name, &task.queue_name);
    }

    // Emit workflow.started exactly once per execution.  Two independent
    // conditions must both hold:
    //
    // 1. task.attempt == 1: the task queue has never dispatched this execution
    //    before (attempt starts at 0 and is incremented to 1 on first claim;
    //    signal-resume paths increment it again on re-claim).
    //
    // 2. No scheduling events in history: guards against counting replayed
    //    first-dispatch tasks that already committed scheduling work.
    //    load_workflow_replay_state prepends SignalReceived/TimerFired for
    //    pending signals and fired timers, so checking raw length alone is
    //    unreliable for brand-new workflows.
    let has_scheduling_events = prepared.history_events.iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::ActivityScheduled { .. }
                | WorkflowEvent::TimerStarted { .. }
                | WorkflowEvent::ChildWorkflowStarted { .. }
                | WorkflowEvent::LocalActivityScheduled { .. }
                | WorkflowEvent::ActivityAwaitingExternal { .. }
                | WorkflowEvent::MarkerRecorded { .. }
        )
    });
    if task.attempt == 1 && !has_scheduling_events {
        telemetry
            .metrics
            .record_workflow_started(&prepared.execution.workflow_name, &task.queue_name);
    }

    // Schedule-to-start latency (issue #501): record here, at the point the
    // workflow handler genuinely begins executing, rather than at permit
    // acquisition in `dispatch_task`. Measuring at handler start keeps this a
    // true *wait* metric (it does not absorb the handler's own execution time)
    // while still capturing the local-permit wait, since `schedule_to_start_secs`
    // measures from task eligibility. The common paused case never reaches here —
    // `queue::claim_task` skips PAUSED executions — so the only re-park that can
    // record a sample is the rare claimed-then-paused race below, whose handler
    // did in fact run.
    telemetry.metrics.record_schedule_to_start(
        &task.queue_name,
        queue::schedule_to_start_secs(
            task.scheduled_at,
            task.created_at,
            task.started_at.unwrap_or(task.scheduled_at),
        ) + dispatched_at.elapsed().as_secs_f64(),
    );

    let started_at = std::time::Instant::now();

    // Drive the workflow in a loop so that local activities can be executed
    // inline without parking the task. Each iteration runs the workflow until
    // it suspends; if it suspends on a RunLocalActivity command the handler
    // is executed here, its events are appended to history, and the workflow
    // is re-run with the extended history. Any other suspension (regular
    // activity, timer, signal wait, …) breaks out of the loop.
    let mut history_events = prepared.history_events;
    let mut next_event_id = prepared.next_event_id;

    // Issue #678/#1034: external-op ids resolved INLINE during this decision
    // cycle. Set by the mixed-signal arm below (any suspension whose command
    // batch carries a `SignalExternalWorkflow` / `RequestCancelExternalWorkflow`);
    // every other break arm leaves it empty. Threaded into the persist
    // transaction so a `select!{ <branch>, signal_external_workflow(target) }`
    // whose target is same-shard (resolved inline) self-wakes immediately at
    // `persist_workflow_outcome`'s `Suspended` arm instead of parking on
    // `<branch>` (a timer until `fires_at`, a signal-wait indefinitely, or an
    // activity/child workflow until it finishes).
    let mut resolved_inline_external = ResolvedExternalIds::default();

    let loop_result = loop {
        // Recompute is_replay each iteration: after local-activity events are
        // appended the workflow re-runs in replay mode (history_events.len() > 1).
        // ADR-0001 §2.1: span metadata must reflect the current replay state so
        // harvest.replay and link.traceparent are accurate on every executor call.
        let is_replay = history_events.len() > 1;

        // ADR-0001 §3 + §4: install the producer's trace context only for live
        // (non-replay) iterations so the harvest.workflow.execute span is
        // correctly parented.  For replay iterations the context must NOT be
        // installed — replay spans must be new root spans (the original trace
        // may have long since expired).  Installing per-iteration ensures that
        // when local-activity events push history_events.len() > 1 the
        // transition to is_replay=true correctly clears the live parent context.
        let _iter_parent_guard = trace_carrier
            .as_ref()
            .filter(|_| !is_replay)
            .map(|c| telemetry.install_trace_context(c));

        let span_meta = WorkflowExecuteSpanMeta {
            workflow_name: prepared.execution.workflow_name.clone(),
            workflow_id: prepared.execution.workflow_id.clone(),
            shard_id: i64::from(prepared.execution.shard_id),
            queue_name: task.queue_name.clone(),
            is_replay,
            link_traceparent: trace_carrier
                .as_ref()
                .filter(|_| is_replay)
                .and_then(|c| c.link_traceparent.clone().or_else(|| c.traceparent.clone())),
            build_id: Some(build_id.to_string()),
            // Issue #772: thread the run's effective execution-timeout budget so
            // `ctx.deadline()` / `ctx.should_continue_as_new()` can reason about
            // the deadline for deadline-aware continue-as-new.
            execution_timeout: prepared.execution.execution_timeout,
            // Issue #772: thread the authoritative absolute deadline from the
            // row. Unlike `started_at + execution_timeout`, this is the effective
            // deadline the timeout scanner enforces — pause/resume (#383) and
            // redrive push it forward — so `ctx.deadline()` must read it directly
            // rather than recompute from the (now stale) start + timeout.
            deadline_at: prepared.execution.deadline_at,
            // Issue #698: thread the spawning parent's execution id from the
            // execution row so a child workflow can read it via `ctx.info()` /
            // `ctx.parent_execution_id()`. `None` for a top-level run.
            parent_execution_id: prepared.execution.parent_id.map(ExecutionId::from_uuid),
        };

        // Filter declarative handlers to those that target this workflow type.
        let wf_name = prepared.execution.workflow_name.as_str();
        let dq: Vec<&crate::info::QueryHandlerInfo> = registry
            .query_handlers
            .iter()
            .filter(|h| h.workflow == wf_name)
            .collect();
        let du: Vec<&crate::info::UpdateHandlerInfo> = registry
            .update_handlers
            .iter()
            .filter(|h| h.workflow == wf_name)
            .collect();

        let exec_context_headers = prepared
            .execution
            .context_headers
            .as_ref()
            .and_then(|v| {
                match serde_json::from_value::<std::collections::HashMap<String, String>>(v.clone()) {
                    Ok(h) => Some(h),
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to deserialize workflow execution context headers; propagating empty map");
                        None
                    }
                }
            })
            .unwrap_or_default();

        let (run_outcome, pending_cmds, execute_span) =
            run_workflow_with_state_history_policy_and_caps(
                prepared.exec_id,
                history_events.clone(),
                workflow.handler,
                task.input.clone(),
                registry.shared_state(),
                registry.history_policy(),
                Some(&span_meta),
                &dq,
                &du,
                wf_name,
                registry.max_activity_input_bytes,
                registry.max_signal_payload_bytes,
                workflow
                    .max_input_bytes
                    .map_or(registry.max_workflow_input_bytes, |per| {
                        per.max(registry.max_workflow_input_bytes)
                    }),
                registry.max_current_details_bytes,
                exec_context_headers.clone(),
                registry
                    .payload_offloader()
                    .map(crate::payload_store::PayloadOffloader::threshold),
                telemetry.metrics.clone(),
                // Issue #620: builder-level default activity retry/timeout floor,
                // consumed by the LOCAL activity path in `execute_local_activity_with_opts`.
                registry.default_activity_retry_policy(),
                registry.default_activity_start_to_close(),
            )
            .await;

        match run_outcome {
            WorkflowOutcome::Suspended { commands }
                if commands
                    .iter()
                    .any(|c| matches!(c, WorkflowCommand::RunLocalActivity { .. })) =>
            {
                // Apply any search-attribute patches before running the local
                // activity so that attributes are visible even if the worker
                // crashes during inline execution.
                persist_search_attrs_from_commands(conn, prepared.exec_id, &commands).await?;
                // Persist the current_details breadcrumb before inline execution (issue #473).
                persist_current_details_from_commands(conn, prepared.exec_id, &commands).await?;
                // Fire ephemeral progress chunks (issue #791) — best-effort, before inline run.
                notify_progress_from_commands(conn, prepared.exec_id, &commands).await;
                // Sync in-memory snapshot so a subsequent continue_as_new in the
                // same task copies the patched attrs to the successor row.
                prepared.execution.search_attrs = apply_search_attrs_patch_in_memory(
                    prepared.execution.search_attrs.take(),
                    &commands,
                );
                let detached_execute_span = execute_span.clone();
                // Local-activity re-run: drop this iteration's execute span
                // so the OTel span closes before we start inline execution.
                drop(execute_span);
                // If the batch also contains SignalExternalWorkflow or
                // RequestCancelExternalWorkflow commands, write their history events BEFORE
                // the local-activity events. This preserves correct replay ordering: on the
                // next run drain_early_signals stashes the external events so the matchers
                // see them before LocalActivityScheduled.
                let commands = if commands.iter().any(|c| {
                    matches!(
                        c,
                        WorkflowCommand::SignalExternalWorkflow { .. }
                            | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                    )
                }) {
                    let (signal_items, remaining) = split_mixed_signal_batch(commands);
                    if !signal_items.is_empty() {
                        let new_events = match persist_external_signal_inline(
                            conn,
                            prepared.exec_id,
                            signal_items,
                            &mut next_event_id,
                            &*telemetry.metrics,
                        )
                        .await
                        {
                            Ok(events) => events,
                            Err(e) => {
                                return fail_workflow_execution_clearing_strikes(
                                    conn,
                                    task,
                                    worker_id,
                                    Err::<(), _>(e),
                                    workflow_panic_strikes,
                                    prepared.exec_id.as_uuid(),
                                )
                                .await;
                            }
                        };
                        history_events.extend(new_events);
                        let current_history_event_count =
                            u64::try_from(history_events.len()).unwrap_or(u64::MAX);
                        if let Some(cap) = registry.history_policy().event_hard_cap()
                            && current_history_event_count >= cap
                        {
                            let deferred = fail_workflow_for_history_cap(
                                conn,
                                &telemetry,
                                task,
                                &prepared.execution,
                                prepared.exec_id,
                                next_event_id,
                                worker_id,
                                started_at,
                                current_history_event_count,
                                cap,
                            )
                            .await?;
                            for start in deferred {
                                start.spawn();
                            }
                            return Ok(());
                        }
                    }
                    remaining
                } else {
                    commands
                };
                let detached_spawns = DetachedSpawnPersistence {
                    registry,
                    parent_execution: &prepared.execution,
                    execute_span: &detached_execute_span,
                };
                let local_batch = extract_run_local_activity(commands);
                let local_context_headers = std::sync::Arc::new(exec_context_headers.clone());
                let inline_outcome = match run_local_activity_inline(
                    conn,
                    registry,
                    prepared.exec_id,
                    local_batch,
                    detached_spawns,
                    max_local_activity_start_to_close,
                    &mut next_event_id,
                    local_context_headers,
                    &task.queue_name,
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(e) => {
                        return fail_workflow_execution_clearing_strikes(
                            conn,
                            task,
                            worker_id,
                            Err::<(), _>(e),
                            workflow_panic_strikes,
                            prepared.exec_id.as_uuid(),
                        )
                        .await;
                    }
                };
                let new_events = match inline_outcome {
                    LocalActivityInlineOutcome::Complete(events) => events,
                    LocalActivityInlineOutcome::HistoryCapReached {
                        events,
                        event_count,
                    } => {
                        history_events.extend(events);
                        let deferred = fail_workflow_for_history_cap(
                            conn,
                            &telemetry,
                            task,
                            &prepared.execution,
                            prepared.exec_id,
                            next_event_id,
                            worker_id,
                            started_at,
                            event_count,
                            registry
                                .history_policy()
                                .event_hard_cap()
                                .expect("HistoryCapReached requires a configured hard cap"),
                        )
                        .await?;
                        for start in deferred {
                            start.spawn();
                        }
                        return Ok(());
                    }
                };
                history_events.extend(new_events);
                let current_history_event_count =
                    u64::try_from(history_events.len()).unwrap_or(u64::MAX);
                if let Some(cap) = registry.history_policy().event_hard_cap()
                    && current_history_event_count >= cap
                {
                    let deferred = fail_workflow_for_history_cap(
                        conn,
                        &telemetry,
                        task,
                        &prepared.execution,
                        prepared.exec_id,
                        next_event_id,
                        worker_id,
                        started_at,
                        current_history_event_count,
                        cap,
                    )
                    .await?;
                    for start in deferred {
                        start.spawn();
                    }
                    return Ok(());
                }
            }
            WorkflowOutcome::Suspended { commands }
                if commands.iter().any(|c| {
                    matches!(
                        c,
                        WorkflowCommand::SignalExternalWorkflow { .. }
                            | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                    )
                }) && commands.iter().all(|c| {
                    matches!(
                        c,
                        WorkflowCommand::SignalExternalWorkflow { .. }
                            | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                            | WorkflowCommand::RecordMarker { .. }
                            | WorkflowCommand::RecordSideEffect { .. }
                            | WorkflowCommand::RecordUpdateResult { .. }
                            | WorkflowCommand::UpsertSearchAttributes { .. }
                            | WorkflowCommand::SetCurrentDetails { .. }
                            | WorkflowCommand::PublishProgress { .. }
                    )
                }) =>
            {
                // Only enters this path when every non-bookkeeping command in the
                // batch is a SignalExternalWorkflow (or RecordMarker). Mixed batches
                // that also contain ScheduleActivity / StartTimer / etc. fall through
                // to the regular suspension path so those commands are not dropped.
                //
                // Persist bookkeeping commands (update-result events, search-attribute
                // patches) first, just as the RunLocalActivity path does.
                if let Err(e) = persist_update_result_commands(
                    conn,
                    prepared.exec_id,
                    &commands,
                    &mut next_event_id,
                )
                .await
                {
                    return fail_workflow_execution_clearing_strikes(
                        conn,
                        task,
                        worker_id,
                        Err::<(), _>(e),
                        workflow_panic_strikes,
                        prepared.exec_id.as_uuid(),
                    )
                    .await;
                }
                // Issue #684: the update results just persisted inline (autocommit)
                // are stripped from the reconstructed suspension below, so they
                // never reach the main-transaction Persisted-arm emission — emit
                // here, post-commit, or they would be silently uncounted.
                emit_update_result_metrics(
                    telemetry.metrics.as_ref(),
                    &prepared.execution.workflow_name,
                    &task.queue_name,
                    &collect_update_result_metrics(&history_events, &commands),
                );
                if let Err(e) =
                    persist_search_attrs_from_commands(conn, prepared.exec_id, &commands).await
                {
                    return fail_workflow_execution_clearing_strikes(
                        conn,
                        task,
                        worker_id,
                        Err::<(), _>(e),
                        workflow_panic_strikes,
                        prepared.exec_id.as_uuid(),
                    )
                    .await;
                }
                if let Err(e) =
                    persist_current_details_from_commands(conn, prepared.exec_id, &commands).await
                {
                    return fail_workflow_execution_clearing_strikes(
                        conn,
                        task,
                        worker_id,
                        Err::<(), _>(e),
                        workflow_panic_strikes,
                        prepared.exec_id.as_uuid(),
                    )
                    .await;
                }
                // Fire ephemeral progress chunks (issue #791) — best-effort, never fails the cycle.
                notify_progress_from_commands(conn, prepared.exec_id, &commands).await;
                prepared.execution.search_attrs = apply_search_attrs_patch_in_memory(
                    prepared.execution.search_attrs.take(),
                    &commands,
                );
                drop(execute_span);
                let items = extract_signal_external_workflow(commands);
                let items_clone = items.clone();
                let new_events = match persist_external_signal_inline(
                    conn,
                    prepared.exec_id,
                    items,
                    &mut next_event_id,
                    &*telemetry.metrics,
                )
                .await
                {
                    Ok(events) => events,
                    Err(e) => {
                        return fail_workflow_execution_clearing_strikes(
                            conn,
                            task,
                            worker_id,
                            Err::<(), _>(e),
                            workflow_panic_strikes,
                            prepared.exec_id.as_uuid(),
                        )
                        .await;
                    }
                };
                history_events.extend(new_events.clone());
                let current_history_event_count =
                    u64::try_from(history_events.len()).unwrap_or(u64::MAX);
                if let Some(cap) = registry.history_policy().event_hard_cap()
                    && current_history_event_count >= cap
                {
                    let deferred = fail_workflow_for_history_cap(
                        conn,
                        &telemetry,
                        task,
                        &prepared.execution,
                        prepared.exec_id,
                        next_event_id,
                        worker_id,
                        started_at,
                        current_history_event_count,
                        cap,
                    )
                    .await?;
                    for start in deferred {
                        start.spawn();
                    }
                    return Ok(());
                }

                // If any signal or cancel in the batch was not resolved inline (remains
                // pending/suspended), we must break the loop and suspend the workflow task.
                let mut all_resolved = true;
                for item in &items_clone {
                    match item {
                        SignalBatchItem::Signal(run) => {
                            let resolved = new_events.iter().any(|e| match e {
                                WorkflowEvent::ExternalSignalDelivered { signal_id }
                                | WorkflowEvent::ExternalSignalFailed { signal_id, .. } => {
                                    *signal_id == run.signal_id
                                }
                                _ => false,
                            });
                            if !resolved {
                                all_resolved = false;
                                break;
                            }
                        }
                        SignalBatchItem::Cancel(run) => {
                            let resolved = new_events.iter().any(|e| match e {
                                WorkflowEvent::ExternalCancelDelivered { cancel_id }
                                | WorkflowEvent::ExternalCancelFailed { cancel_id, .. } => {
                                    *cancel_id == run.cancel_id
                                }
                                _ => false,
                            });
                            if !resolved {
                                all_resolved = false;
                                break;
                            }
                        }
                        SignalBatchItem::Marker(_) => {}
                    }
                }

                if !all_resolved {
                    let mut reconstructed_commands = Vec::with_capacity(items_clone.len());
                    for item in items_clone {
                        match item {
                            SignalBatchItem::Marker(_) => {
                                // Already persisted via persist_external_signal_inline.
                                // Do not reconstruct or re-append to avoid duplicate marker events in history.
                            }
                            SignalBatchItem::Signal(run) => {
                                let (dummy_tx, _) = tokio::sync::oneshot::channel();
                                reconstructed_commands.push(
                                    WorkflowCommand::SignalExternalWorkflow {
                                        signal_id: run.signal_id,
                                        target: run.target,
                                        signal_name: run.signal_name,
                                        payload: run.payload,
                                        result_tx: dummy_tx,
                                        already_requested: run.already_requested,
                                        idempotency_key: run.idempotency_key,
                                    },
                                );
                            }
                            SignalBatchItem::Cancel(run) => {
                                let (dummy_tx, _) = tokio::sync::oneshot::channel();
                                reconstructed_commands.push(
                                    WorkflowCommand::RequestCancelExternalWorkflow {
                                        cancel_id: run.cancel_id,
                                        target: run.target,
                                        result_tx: dummy_tx,
                                        already_requested: run.already_requested,
                                    },
                                );
                            }
                        }
                    }

                    // Re-acquire a fresh execute_span so persist_workflow_outcome
                    // (via handle_suspended_workflow) gets a valid span reference.
                    let execute_span = tracing::Span::none();
                    break (
                        WorkflowOutcome::Suspended {
                            commands: reconstructed_commands,
                        },
                        pending_cmds,
                        execute_span,
                    );
                }
            }
            // Mixed batch: contains SignalExternalWorkflow or RequestCancelExternalWorkflow
            // AND other durable commands (ScheduleActivity, StartTimer, etc.). The "all
            // signals/cancels" guard above did not match. Write external-command events to
            // history FIRST (so drain_early_signals stashes them on the next replay pass),
            // then break with the remaining commands for handle_suspended_workflow.
            WorkflowOutcome::Suspended { commands }
                if commands.iter().any(|c| {
                    matches!(
                        c,
                        WorkflowCommand::SignalExternalWorkflow { .. }
                            | WorkflowCommand::RequestCancelExternalWorkflow { .. }
                    )
                }) =>
            {
                if let Err(e) = persist_update_result_commands(
                    conn,
                    prepared.exec_id,
                    &commands,
                    &mut next_event_id,
                )
                .await
                {
                    return fail_workflow_execution_clearing_strikes(
                        conn,
                        task,
                        worker_id,
                        Err::<(), _>(e),
                        workflow_panic_strikes,
                        prepared.exec_id.as_uuid(),
                    )
                    .await;
                }
                // Issue #684: `split_mixed_signal_batch` below drops
                // `RecordUpdateResult` from `remaining_commands`, so these
                // inline-persisted update results never reach the main-transaction
                // Persisted-arm emission — emit here, post-commit.
                emit_update_result_metrics(
                    telemetry.metrics.as_ref(),
                    &prepared.execution.workflow_name,
                    &task.queue_name,
                    &collect_update_result_metrics(&history_events, &commands),
                );
                if let Err(e) =
                    persist_search_attrs_from_commands(conn, prepared.exec_id, &commands).await
                {
                    return fail_workflow_execution_clearing_strikes(
                        conn,
                        task,
                        worker_id,
                        Err::<(), _>(e),
                        workflow_panic_strikes,
                        prepared.exec_id.as_uuid(),
                    )
                    .await;
                }
                if let Err(e) =
                    persist_current_details_from_commands(conn, prepared.exec_id, &commands).await
                {
                    return fail_workflow_execution_clearing_strikes(
                        conn,
                        task,
                        worker_id,
                        Err::<(), _>(e),
                        workflow_panic_strikes,
                        prepared.exec_id.as_uuid(),
                    )
                    .await;
                }
                // Fire ephemeral progress chunks (issue #791) — best-effort, never fails the cycle.
                notify_progress_from_commands(conn, prepared.exec_id, &commands).await;
                prepared.execution.search_attrs = apply_search_attrs_patch_in_memory(
                    prepared.execution.search_attrs.take(),
                    &commands,
                );
                drop(execute_span);
                let (signal_items, remaining_commands) = split_mixed_signal_batch(commands);
                let new_events = match persist_external_signal_inline(
                    conn,
                    prepared.exec_id,
                    signal_items,
                    &mut next_event_id,
                    &*telemetry.metrics,
                )
                .await
                {
                    Ok(events) => events,
                    Err(e) => {
                        return fail_workflow_execution_clearing_strikes(
                            conn,
                            task,
                            worker_id,
                            Err::<(), _>(e),
                            workflow_panic_strikes,
                            prepared.exec_id.as_uuid(),
                        )
                        .await;
                    }
                };
                // Issue #678/#1034: capture the external terminals appended INLINE
                // this cycle BEFORE `new_events` is moved into `history_events`.
                // When the remaining command batch parks below (a timer,
                // signal-wait, scheduled-activity, or child-workflow branch), the
                // arm-level self-wake in `persist_workflow_outcome`'s `Suspended`
                // arm re-pends the parked task immediately instead of waiting for
                // that branch to resolve. `new_events` contains only this batch's
                // Requested + inline terminal events, so the resolved-id set is
                // derivable from it alone; an item still unresolved (cross-shard /
                // NotFound → outbox) appended no terminal and contributes nothing.
                resolved_inline_external = resolved_external_ids(&new_events);
                let remaining_commands_with_unresolved = remaining_commands;
                history_events.extend(new_events);
                let current_history_event_count =
                    u64::try_from(history_events.len()).unwrap_or(u64::MAX);
                if let Some(cap) = registry.history_policy().event_hard_cap()
                    && current_history_event_count >= cap
                {
                    let deferred = fail_workflow_for_history_cap(
                        conn,
                        &telemetry,
                        task,
                        &prepared.execution,
                        prepared.exec_id,
                        next_event_id,
                        worker_id,
                        started_at,
                        current_history_event_count,
                        cap,
                    )
                    .await?;
                    for start in deferred {
                        start.spawn();
                    }
                    return Ok(());
                }
                // Re-acquire a fresh execute_span so persist_workflow_outcome
                // (via handle_suspended_workflow) gets a valid span reference.
                // The original span was dropped above.
                let execute_span = tracing::Span::none();
                break (
                    WorkflowOutcome::Suspended {
                        commands: remaining_commands_with_unresolved,
                    },
                    pending_cmds,
                    execute_span,
                );
            }
            other => break (other, pending_cmds, execute_span),
        }
    };

    let (outcome, mut pending_cmds, execute_span) = loop_result;

    // Issue #383: an operator may have paused this execution while this
    // workflow decision task was running. Pause is enforced at the claim layer
    // and only blocks *future* claims; this already-claimed task would
    // otherwise persist new activity/timer/child commands (or a terminal
    // outcome) after a successful pause, violating the pause guarantee. The
    // loop above only appended events for already-completed inline work (local
    // activities / external-signal sends), which replay reproduces
    // deterministically. If the execution became PAUSED, discard the pending
    // decision without persisting any new commands and re-park the task:
    // resume_workflow_execution wakes it, and the deterministic handler
    // re-derives the same commands on replay.
    //
    // Fast-path optimization only: this is a non-locking read in autocommit
    // that lets a decision paused well before persistence bail out *before*
    // computing metrics, history-cap, and cache state below. It is NOT the
    // authoritative guard — the persistence transaction further down opens with
    // a `FOR UPDATE` row lock on the execution that serializes with
    // `pause_workflow_execution`'s own lock and re-checks PAUSED under it, so a
    // pause committing in the gap between this read and the persist is still
    // caught and the decision discarded. The claim-layer gate
    // (`queue::claim_task`) prevents the task from ever being *claimed* while
    // PAUSED in the first place; this read just avoids wasted work in the
    // common case.
    {
        use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
        let current_state: Option<String> = exec_dsl::harvest_workflow_executions
            .find(prepared.exec_id.as_uuid())
            .select(exec_dsl::state)
            .first::<String>(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;
        if current_state.as_deref() == Some("PAUSED") {
            let sticky = if sticky_timeout.is_zero() {
                None
            } else {
                Some(queue::StickyHint::new(worker_id, sticky_timeout))
            };
            // Unlike the persist-time PAUSED check further down, this
            // fast-path read takes no lock, so it has no ordering guarantee
            // against `resume_workflow_execution` (PR #901 review): if resume
            // transitions PAUSED -> RUNNING and calls `wake_workflow_task`
            // between this read and this park's own atomic UPDATE, the wake
            // is captured as `wake_requested = TRUE` on this still-claimed
            // row -- and resume's wake is the *only* wake for this event, so
            // discarding it here (unlike other pause-discard sites) leaves a
            // resumed execution parked with nothing left to re-wake it.
            let had_wake_requested = queue::park_workflow_task(conn, task.id, sticky).await?;
            if had_wake_requested {
                queue::wake_workflow_task(conn, prepared.exec_id).await?;
            }
            drop(execute_span);
            return Ok(());
        }
    }

    // Issue #603: an engine-detected replay divergence must NOT terminally
    // fail the workflow — it is a recoverable code-deploy bug, not a workflow
    // outcome ("a workflow-task failure must never fail the workflow"). Gate
    // here, before ANY terminal side effect (cascade counting, history-cap
    // fail, terminal metrics, and — critically — the pre-terminal event
    // appends in `persist_terminal_outcome_commands`): the divergent cycle's
    // pending commands are untrustworthy, and persisting even one marker from
    // the bad build would poison history against the rolled-back code. The
    // whole decision is discarded exactly like the ParkedPaused path; the
    // block path appends zero events, stamps the diagnostic columns, and
    // re-pends the task with a capped-exponential backoff so a rollback at
    // any later time resumes the execution from where it was.
    //
    // The author-Err path is untouched: a workflow body's own `Err(...)`
    // carries `non_deterministic_details: None` and still fails terminally
    // below, exactly as before.
    if let WorkflowOutcome::Failed {
        error,
        non_deterministic_details: Some(details),
        ..
    } = &outcome
    {
        // An ND-block is a NON-panic outcome (handler_panic is always false when
        // non_deterministic_details is Some — see the executor's mutually
        // exclusive Failed constructions), so clear the consecutive-panic strike
        // like every other non-panic path (issue #782, Codex review). The panic
        // budget counts CONSECUTIVE panics; a stale strike surviving an ND-block
        // interlude would exhaust the budget early after a rollback.
        clear_panic_strike(workflow_panic_strikes, prepared.exec_id.as_uuid());
        drop(execute_span);
        return block_workflow_for_non_determinism(
            conn,
            &telemetry,
            task,
            &prepared.execution,
            prepared.exec_id,
            worker_id,
            sticky_timeout,
            build_id,
            error,
            details,
        )
        .await;
    }

    // Issue #782: a **contained handler panic** must NOT fail the workflow on
    // the first strike — buy time for a hotfix/redeploy by re-dispatching with
    // capped backoff up to `workflow_panic_max_attempts`, then fail terminally
    // with the typed HandlerPanic error. Placed here, beside the ND-block gate,
    // before ANY terminal side effect: the panicked cycle's `pending_cmds` are
    // untrustworthy and are discarded on both the retry and terminal paths (R5).
    // The `handler_panic` flag is set only by the executor's caught-panic arm,
    // so an author `Err(...)` (even one whose error type happens to be
    // "HandlerPanic") never reaches this gate.
    if let WorkflowOutcome::Failed {
        handler_panic: true,
        error,
        ..
    } = &outcome
    {
        // Emit on every panic entry (each retry AND the terminal), so the
        // counter reflects the true panic rate.
        telemetry
            .metrics
            .record_workflow_panic(&prepared.execution.workflow_name, &task.queue_name);

        let strikes = increment_panic_strike(workflow_panic_strikes, prepared.exec_id.as_uuid());

        match panic_retry_decision(strikes, workflow_panic_max_attempts) {
            PanicRetryDecision::Requeue => {
                // `panic_retry_backoff` is bounded by PANIC_RETRY_BACKOFF_CAP_SECS
                // (30s), always representable as a chrono::Duration; the fallback
                // is defensive and still non-zero so it can never hot-loop.
                let backoff = chrono::Duration::from_std(panic_retry_backoff(strikes))
                    .unwrap_or_else(|_| chrono::Duration::seconds(30));
                tracing::warn!(
                    execution_id = %prepared.exec_id,
                    workflow = %prepared.execution.workflow_name,
                    queue = %task.queue_name,
                    strikes,
                    max_attempts = workflow_panic_max_attempts,
                    backoff_secs = backoff.num_seconds(),
                    "harvest: workflow handler panicked; containing as a non-terminal \
                     re-dispatch (issue #782) — roll back or fix the panicking build"
                );
                drop(execute_span);
                // Discard the panicked cycle's pending commands (R5) and re-pend
                // the task with backoff. State stays RUNNING; no event appended.
                return queue::requeue_workflow_task_after_panic(conn, task.id, backoff, error)
                    .await;
            }
            PanicRetryDecision::Terminal => {
                // Budget exhausted (or disabled): clear the strike entry and
                // fall through to a CLEAN terminal WorkflowFailed carrying the
                // typed HandlerPanic error, discarding the panicked cycle's
                // pending commands.
                clear_panic_strike(workflow_panic_strikes, prepared.exec_id.as_uuid());
                pending_cmds = Vec::new();
                tracing::error!(
                    execution_id = %prepared.exec_id,
                    workflow = %prepared.execution.workflow_name,
                    queue = %task.queue_name,
                    strikes,
                    max_attempts = workflow_panic_max_attempts,
                    "harvest: workflow handler panic budget exhausted; failing the \
                     execution terminally with a typed HandlerPanic error (issue #782)"
                );
            }
        }
    } else {
        // Any non-panic workflow-task outcome (completed / suspended /
        // continued-as-new / author-Err failed) clears the consecutive-panic
        // strike counter so a later transient panic starts fresh and the map
        // does not grow unbounded (mirrors the timeout-strike clear discipline).
        // ND-blocked outcomes returned above and never reach here.
        clear_panic_strike(workflow_panic_strikes, prepared.exec_id.as_uuid());
    }

    let terminal_parent_close_cascade_events = if matches!(
        &outcome,
        WorkflowOutcome::Completed { .. } | WorkflowOutcome::Failed { .. }
    ) {
        match terminal_parent_close_cascade_event_count(conn, prepared.exec_id, &pending_cmds).await
        {
            Ok(count) => count,
            Err(error) => {
                return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(error)).await;
            }
        }
    } else {
        0
    };
    let pending_durable_event_count = match &outcome {
        WorkflowOutcome::Suspended { commands } => {
            match suspended_command_event_count(conn, task.workflow_exec_id, commands).await {
                Ok(count) => count,
                Err(error) => {
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(error))
                        .await;
                }
            }
        }
        WorkflowOutcome::ContinuedAsNew { .. } => pending_update_result_event_count(&pending_cmds)
            .saturating_add(pre_suspension_event_count(&pending_cmds)),
        WorkflowOutcome::Completed { .. } | WorkflowOutcome::Failed { .. } => {
            pending_update_result_event_count(&pending_cmds)
                .saturating_add(pre_suspension_event_count(&pending_cmds))
                .saturating_add(terminal_parent_close_cascade_events)
        }
    };
    let current_history_event_count = u64::try_from(history_events.len())
        .unwrap_or(u64::MAX)
        .saturating_add(pending_durable_event_count);

    if let Some(cap) = registry.history_policy().event_hard_cap()
        && current_history_event_count >= cap
        && !matches!(&outcome, WorkflowOutcome::ContinuedAsNew { .. })
    {
        let deferred = fail_workflow_for_history_cap(
            conn,
            &telemetry,
            task,
            &prepared.execution,
            prepared.exec_id,
            next_event_id,
            worker_id,
            started_at,
            current_history_event_count,
            cap,
        )
        .await?;
        for start in deferred {
            start.spawn();
        }
        return Ok(());
    }

    let status = match &outcome {
        WorkflowOutcome::Completed { .. } => WorkflowStatus::Completed,
        WorkflowOutcome::Failed { .. } => WorkflowStatus::Failed,
        WorkflowOutcome::Suspended { .. } => WorkflowStatus::Suspended,
        WorkflowOutcome::ContinuedAsNew { .. } => WorkflowStatus::ContinuedAsNew,
    };
    telemetry.metrics.record_workflow_completed(
        &prepared.execution.workflow_name,
        &task.queue_name,
        started_at.elapsed().as_secs_f64(),
        status,
    );
    if !matches!(&outcome, WorkflowOutcome::Suspended { .. }) {
        telemetry.metrics.record_workflow_history_size(
            &prepared.execution.workflow_name,
            terminal_history_event_count(next_event_id, &pending_cmds)
                .saturating_add(terminal_parent_close_cascade_events),
        );
    }
    if matches!(&outcome, WorkflowOutcome::ContinuedAsNew { .. }) {
        telemetry
            .metrics
            .record_workflow_continue_as_new(&prepared.execution.workflow_name);
    }
    // Emit the once-per-terminal-outcome counter (issue #519).
    // Suspended is not a terminal state — a workflow that suspends N times
    // and then completes must produce exactly one `completed` increment.
    //
    // Issue #684: harvest.signal.unhandled is NOT emitted here. It is collected
    // below (before the persist transaction moves `outcome`) and emitted
    // post-commit in the `Persisted` arm — the same discipline as
    // harvest.update.completed/failed — so it represents DURABLE terminal
    // outcomes only. This site (`record_workflow_terminal`, #519) keeps its own
    // pre-persist placement unchanged.
    match &outcome {
        WorkflowOutcome::Completed { .. } => {
            telemetry.metrics.record_workflow_terminal(
                &prepared.execution.workflow_name,
                &task.queue_name,
                WorkflowStatus::Completed,
            );
        }
        WorkflowOutcome::Failed {
            non_deterministic_details,
            ..
        } => {
            // Defensive (issue #603): an ND-carrying Failed outcome is gated
            // earlier into `block_workflow_for_non_determinism` (which emits
            // the detection counter itself) and never reaches this arm.
            // Asserted so a future regression that lets this happen panics
            // loudly in debug/test builds instead of silently double-counting
            // (or, worse, silently NOT counting once the gate is removed).
            debug_assert!(
                non_deterministic_details.is_none(),
                "ND-carrying Failed outcome must be gated earlier in \
                 process_workflow_task, before terminal metrics are recorded"
            );
            if non_deterministic_details.is_some() {
                telemetry
                    .metrics
                    .record_workflow_non_determinism(&prepared.execution.workflow_name, build_id);
            }
            telemetry.metrics.record_workflow_terminal(
                &prepared.execution.workflow_name,
                &task.queue_name,
                WorkflowStatus::Failed,
            );
        }
        WorkflowOutcome::ContinuedAsNew { .. } => telemetry.metrics.record_workflow_terminal(
            &prepared.execution.workflow_name,
            &task.queue_name,
            WorkflowStatus::ContinuedAsNew,
        ),
        WorkflowOutcome::Suspended { .. } => {} // not terminal — no counter
    }

    // Issue #603 fix: if this execution was previously ND-blocked, this cycle
    // replaying cleanly means the offending build was rolled back or fixed —
    // `clear_nd_block` (called inside the persistence transaction below) will
    // clear the DB row. Mirror that clear into the in-memory snapshot *before*
    // `execution_ref` is captured (same pattern as the pending_cmds patch
    // just below), so a same-cycle `ContinuedAsNew` successor built from
    // `execution_ref.search_attrs` doesn't silently reintroduce the six
    // diagnostic keys the transaction is about to delete from the DB.
    // `was_nd_blocked` is captured before mutating so the transaction's own
    // guard (which decides whether to call `clear_nd_block` at all) doesn't
    // read back the now-already-cleared in-memory value and skip the call.
    let was_nd_blocked = prepared.execution.nd_blocked_at.is_some();
    if was_nd_blocked {
        prepared.execution.nd_blocked_at = None;
        prepared.execution.nd_block_reason = None;
        prepared.execution.nd_block_count = 0;
        prepared.execution.search_attrs = apply_raw_search_attrs_patch_in_memory(
            prepared.execution.search_attrs.take(),
            &nd_search_attrs_clear_patch(),
        );
    }

    // Keep the in-memory execution snapshot current so that
    // persist_workflow_continue_as_new copies the patched attrs to the
    // successor row rather than the stale pre-patch snapshot.
    if !pending_cmds.is_empty() {
        prepared.execution.search_attrs = apply_search_attrs_patch_in_memory(
            prepared.execution.search_attrs.take(),
            &pending_cmds,
        );
    }

    // Pre-compute the cache action while `outcome` is still accessible (it
    // will be consumed by `persist_workflow_outcome` below).  We do NOT apply
    // the update yet: the cache must only be written AFTER persistence succeeds
    // so that a failed commit never leaves a warm cache snapshot pointing at
    // events that were never durably written.
    //
    // `Some(state)` → insert on success; `None` → evict on success.
    // Cache operations are skipped entirely when sticky routing is disabled.
    let pending_cache_update = if sticky_timeout.is_zero() {
        None
    } else if let WorkflowOutcome::Suspended { .. } = &outcome {
        Some(Some(crate::cache::CachedWorkflowState {
            events: history_events.clone(),
            next_event_id,
        }))
    } else {
        Some(None) // terminal — evict
    };

    // Extract this run's frozen carryover (issue #488) and scheduled slot (issue #508) from
    // the decoded WorkflowStarted (history_events[0]) so a continue_as_new continuation can
    // inherit them. Slice pattern (not .first()) avoids the in-scope Diesel RunQueryDsl::first
    // ambiguity.
    let (carryover_result, carryover_error, carryover_scheduled_time) = if let [
        WorkflowEvent::WorkflowStarted {
            last_completion_result,
            last_error,
            scheduled_time,
            ..
        },
        ..,
    ] =
        history_events.as_slice()
    {
        (
            last_completion_result.clone(),
            last_error.clone(),
            *scheduled_time,
        )
    } else {
        (None, None, None)
    };
    let persistence = WorkflowTaskPersistence {
        task,
        worker_id,
        exec_id: prepared.exec_id,
        next_event_id,
        sticky_timeout,
        carryover_result,
        carryover_error,
        carryover_scheduled_time,
    };

    // Issue #383: authoritatively enforce pause across the persistence path.
    //
    // The fast-path re-check above is best-effort (non-locking). Here we close
    // the residual race: open the persistence transaction with a `FOR UPDATE`
    // row lock on the execution — mirroring `pause_workflow_execution`'s own
    // lock — so the two serialize. If the operator's pause committed first, we
    // observe `PAUSED` under the lock and re-park the task *inside the same
    // transaction*, discarding the pending decision without persisting any new
    // commands (resume re-derives them deterministically on replay). Otherwise
    // pause blocks until this decision commits, which is exactly the
    // "already-dispatched work runs to completion" semantics. Schedule counters
    // are deferred to after the transaction commits (a best-effort counter
    // failure must never roll back the persisted decision).
    let is_terminal_with_commands =
        !pending_cmds.is_empty() && !matches!(&outcome, WorkflowOutcome::Suspended { .. });
    let counter_action = schedule_counter_action(&outcome);
    // Issue #684: collect update.completed/failed metric data now, while
    // `history_events`, `outcome`, and `pending_cmds` are all still in scope
    // (the persist transaction below moves `outcome`, `pending_cmds`, and
    // `task`). The `RecordUpdateResult`s live in `outcome.commands` on the
    // Suspended path (the common case) and in `pending_cmds` on the terminal /
    // continue-as-new paths — `update_result_command_source` picks the right
    // one. Emitted only post-commit in the `Persisted` arm so a persist failure
    // (and the ParkedPaused discard) never over-counts.
    let update_result_metrics = collect_update_result_metrics(
        &history_events,
        update_result_command_source(&outcome, &pending_cmds),
    );
    // Issue #684 (Codex P2): extract the terminal outcome's unhandled-signal
    // map now, before the persist transaction moves `outcome`. Emitted only
    // post-commit in the `Persisted` arm (same discipline as
    // update.completed/failed), so a ParkedPaused discard or persist failure
    // never counts a signal that never became a durable terminal — and a
    // retry/resume of that discarded cycle cannot double-count it. The map is
    // populated only on Completed/Failed and is empty for Suspended/CAN (and,
    // via the #603 gate returning early above, never reaches here on an
    // ND-carrying Failed), so this is a no-op on every non-terminal path.
    let unhandled_signal_metrics = outcome_unhandled_signals(&outcome);
    let update_metric_queue = task.queue_name.clone();
    let execution_ref = &prepared.execution;
    let exec_uuid = prepared.exec_id.as_uuid();

    let persist_flow = conn
        .transaction::<WorkflowPersistFlow, HarvestError, _>(|conn| {
            async move {
                if check_paused_and_park(conn, exec_uuid, task.id, worker_id, sticky_timeout)
                    .await?
                {
                    return Ok(WorkflowPersistFlow::ParkedPaused);
                }

                // Issue #603: this cycle replayed cleanly (the ND gate above
                // did not fire), so if the execution was previously blocked on
                // replay non-determinism the offending build has been rolled
                // back or fixed — clear the block marker atomically with the
                // recovered cycle's persisted outcome. Guarded on
                // `was_nd_blocked` (captured *before* the in-memory mutation
                // above) rather than re-reading `execution_ref.nd_blocked_at`,
                // which is already `None` here by the time this runs — so
                // never-blocked executions still pay nothing, and a
                // previously-blocked one still gets its DB row cleared.
                if was_nd_blocked {
                    clear_nd_block(conn, persistence.exec_id).await?;
                }

                let (retry_scheduled, deferred_checks, race_deferred_triggers) =
                    if is_terminal_with_commands {
                        persist_terminal_outcome_commands(
                            conn,
                            registry,
                            execution_ref,
                            persistence,
                            outcome,
                            &pending_cmds,
                            &execute_span,
                        )
                        .await?
                    } else {
                        let (retry_scheduled, deferred_checks) = persist_workflow_outcome(
                            conn,
                            registry,
                            execution_ref,
                            persistence,
                            outcome,
                            &execute_span,
                            false,
                            // Issue #678: carries any external-op terminal
                            // resolved inline this cycle into the Suspended arm
                            // so a mixed timer + external op self-wakes.
                            resolved_inline_external,
                        )
                        .await?;
                        (retry_scheduled, deferred_checks, Vec::new())
                    };
                Ok(WorkflowPersistFlow::Persisted {
                    retry_scheduled,
                    deferred_checks,
                    race_deferred_triggers,
                })
            }
            .scope_boxed()
        })
        .await;
    // execute_span is moved into and dropped by the transaction closure above,
    // closing the OTel span after all producer spans have been emitted as its
    // children.

    match persist_flow {
        Ok(WorkflowPersistFlow::ParkedPaused) => return Ok(()),
        Ok(WorkflowPersistFlow::Persisted {
            retry_scheduled,
            deferred_checks,
            race_deferred_triggers,
        }) => {
            // Only spawned now that the transaction above has committed (see
            // apply_race_loser_cancellations's doc comment) — spawning inside
            // the transaction closure could start a trigger workflow for a
            // cancellation that a later error in that same transaction rolls back.
            for start in race_deferred_triggers {
                start.spawn();
            }

            // Deferred best-effort schedule counters, in autocommit post-commit.
            // When a retry was scheduled the failure-counter increment is
            // suppressed: one failure chain counts as one failure (issue #523).
            let effective_counter = if retry_scheduled {
                None
            } else {
                counter_action
            };
            run_deferred_schedule_counter(conn, registry, &prepared.execution, effective_counter)
                .await;

            for (exec_id, name) in deferred_checks {
                check_and_report_unfinished_handlers_for_worker(
                    conn,
                    exec_id,
                    name.as_deref(),
                    Some(registry.telemetry().metrics.as_ref()),
                )
                .await;
            }

            // Issue #684: emit update.completed/update.failed post-commit,
            // exactly once per update result (data collected pre-persist above).
            emit_update_result_metrics(
                telemetry.metrics.as_ref(),
                &prepared.execution.workflow_name,
                &update_metric_queue,
                &update_result_metrics,
            );
            // Issue #684 (Codex P2): emit harvest.signal.unhandled post-commit
            // too, so it represents DURABLE terminal outcomes only (mirrors
            // update.completed/failed). This arm is reached only after the
            // persist transaction committed, which is downstream of both the
            // #603 ND-block gate (Failed{nd:Some} early-returns before persist)
            // and `check_paused_and_park` (a claimed-then-paused race returns
            // via the ParkedPaused arm below, never here). The map is empty on
            // every non-terminal outcome, so this is a no-op there.
            emit_unhandled_signal_metrics(
                telemetry.metrics.as_ref(),
                &prepared.execution.workflow_name,
                &update_metric_queue,
                &unhandled_signal_metrics,
            );
        }
        Err(error) => {
            // Preserve per-path error handling: a terminal-with-commands persist
            // failure durably fails the task + execution (matching the prior
            // `fail_execution_on_error` wrapping); a suspended/simple-terminal
            // persist failure propagates so the task is retried.
            if is_terminal_with_commands {
                return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(error)).await;
            }
            return Err(error);
        }
    }

    // Update the in-process LRU cache ONLY on successful persistence.
    // A Suspended outcome inserts the warm snapshot; terminal outcomes evict.
    // Skipped entirely when sticky routing is disabled (sticky_timeout == 0).
    if let Some(update) = pending_cache_update {
        let exec_uuid = prepared.exec_id.as_uuid();
        let mut guard = workflow_cache.lock().await;
        match update {
            Some(state) => guard.insert(exec_uuid, state),
            None => {
                guard.remove(&exec_uuid);
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_task(
    pool: &DbPool,
    registry: Arc<HandlerRegistry>,
    task: TaskQueueItem,
    worker_id: &str,
    build_id: &str,
    cancellation_grace_period: Duration,
    sticky_timeout: Duration,
    max_local_activity_start_to_close: Duration,
    workflow_cache: Arc<tokio::sync::Mutex<crate::cache::WorkflowCache>>,
    dispatched_at: std::time::Instant,
    max_concurrent_sessions: i32,
    session_slots_in_use: &crate::sessions::SessionSlotRegistry,
    // Issue #782: contained-handler-panic strike map + retry budget, consulted
    // only on the workflow path.
    workflow_panic_strikes: Arc<std::sync::Mutex<std::collections::HashMap<uuid::Uuid, u32>>>,
    workflow_panic_max_attempts: u32,
) -> HarvestResult<()> {
    let mut conn = pool.get().await.map_err(crate::error::database_error)?;

    match ClaimedTaskKind::from_db(&task.task_type)? {
        ClaimedTaskKind::Workflow => {
            process_workflow_task(
                &mut conn,
                registry.as_ref(),
                &task,
                worker_id,
                build_id,
                sticky_timeout,
                max_local_activity_start_to_close,
                workflow_cache,
                dispatched_at,
                &workflow_panic_strikes,
                workflow_panic_max_attempts,
            )
            .await
        }
        ClaimedTaskKind::Activity => {
            // Drop the connection before the handler runs: run_transactional
            // acquires a second pool slot inside the handler, and holding this
            // one during execution would cause a deadlock when max_size
            // connections are all claimed by concurrent activity tasks.
            drop(conn);
            process_activity_task(
                pool,
                registry.as_ref(),
                &task,
                worker_id,
                cancellation_grace_period,
                dispatched_at,
                max_concurrent_sessions,
                session_slots_in_use,
            )
            .await
        }
    }
}

/// Periodically sample per-queue pending-task counts and forward them to the
/// configured [`MetricsRecorder`](crate::telemetry::MetricsRecorder).
///
/// The sampler skips work entirely when the recorder is the default no-op
/// implementation (`is_enabled() == false`), so unconfigured deployments pay no
/// DB cost. This matters because the per-tick queries are not free: `queue_depths`
/// is a `GROUP BY` aggregate and `oldest_pending_ages` scans eligible pending rows
/// with correlated concurrency / rate-limit checks. Both feed gauges that are
/// discarded by the no-op recorder, so issuing them would be pure waste (issue #501
/// review).
///
/// Stops when the cancellation token fires. Queues with zero pending rows are
/// also reported (as depth 0) so gauges reset cleanly after drains.
fn spawn_queue_depth_sampler(
    // One pool per shard to aggregate over. Single-shard deployments pass a
    // one-element vec; multi-shard workers pass every shard in the ShardedDbPool
    // so the gauges reflect fleet-wide backlog rather than the default shard only
    // (issue #522 review). Every worker aggregates the same full set, so all
    // workers emit a consistent `harvest.queue.depth{queue}` value.
    pools: Vec<DbPool>,
    cancel: CancellationToken,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    queues: Vec<String>,
    interval: Duration,
    circuit_breaker_activities: Vec<String>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // No recorder configured: never issue the sampler SQL. The per-event
        // `record_*` calls are zero-cost, but these gauge-feeding queries are not.
        if !telemetry.metrics.is_enabled() {
            return;
        }
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }

            // Aggregate across every shard: depth is summed (total backlog),
            // oldest-pending-age is the max (the single oldest task fleet-wide).
            let mut depth_by_queue: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            let mut age_by_queue: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            // Track whether any shard read failed this tick. A partial aggregate
            // is misleading (a down shard looks like zero backlog) and zero-
            // filling on a total outage would clear the gauges and silence
            // autoscaling/alerts, so on ANY failure we skip the emit entirely and
            // let the gauges hold their last value — matching the pre-aggregation
            // single-pool path that skipped the sample on read failure (#522).
            let mut read_failed = false;
            for pool in &pools {
                let mut conn = match pool.get().await {
                    Ok(conn) => conn,
                    Err(error) => {
                        tracing::debug!(
                            error = %error,
                            "queue depth sampler could not acquire DB connection"
                        );
                        read_failed = true;
                        continue;
                    }
                };
                match queue::queue_depths(&mut conn, &queues).await {
                    Ok(depths) => {
                        for (queue_name, depth) in depths {
                            *depth_by_queue.entry(queue_name).or_insert(0) += depth;
                        }
                    }
                    Err(error) => {
                        tracing::debug!(error = %error, "queue depth sample failed");
                        read_failed = true;
                    }
                }
                match queue::oldest_pending_ages(&mut conn, &queues, &circuit_breaker_activities)
                    .await
                {
                    Ok(ages) => {
                        for (queue_name, age_secs) in ages {
                            let slot = age_by_queue.entry(queue_name).or_insert(0.0);
                            *slot = slot.max(age_secs);
                        }
                    }
                    Err(error) => {
                        tracing::debug!(error = %error, "oldest pending age sample failed");
                        read_failed = true;
                    }
                }
            }

            if read_failed {
                // Skip this tick rather than publish a partial/zeroed aggregate.
                if cancel.is_cancelled() {
                    break;
                }
                continue;
            }

            // Emit the aggregated gauges, zero-filling configured queues with no
            // rows so stale values do not linger after a queue drains. The depth
            // and age queries are scoped to `queues`, so no other queue appears.
            for queue_name in &queues {
                let depth = depth_by_queue.get(queue_name).copied().unwrap_or(0);
                telemetry
                    .metrics
                    .record_queue_depth(queue_name, u64::try_from(depth).unwrap_or(0));
                let age = age_by_queue.get(queue_name).copied().unwrap_or(0.0);
                telemetry
                    .metrics
                    .record_queue_oldest_pending_age(queue_name, age);
            }

            if cancel.is_cancelled() {
                break;
            }
        }
    })
}

/// Periodically sample per-concurrency-key stats and emit metrics/traces.
///
/// Runs on the same cadence as the queue-depth sampler. For each key that is
/// currently active (RUNNING or PENDING tasks), it emits:
///  - `record_concurrency_key_in_flight` with the current RUNNING count
///  - A `DEBUG` trace if any tasks are pending while the cap is saturated
fn spawn_concurrency_sampler(
    // One pool per shard to aggregate over (see `spawn_queue_depth_sampler`).
    pools: Vec<DbPool>,
    cancel: CancellationToken,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    queues: Vec<String>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }

            // Concurrency caps are per-shard (issue #247), so the same key can be
            // active on several shards. Aggregate in-flight and deferred counts
            // across shards per (key, task_type) for a fleet-wide view; the
            // per-shard saturation debug log still fires per shard below.
            let mut in_flight_by_key: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            let mut deferred_by_key: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            // Skip the whole tick on any shard read failure so a partial aggregate
            // doesn't under-report concurrency during a storage outage (#522).
            let mut read_failed = false;
            for pool in &pools {
                let mut conn = match pool.get().await {
                    Ok(conn) => conn,
                    Err(error) => {
                        tracing::debug!(
                            error = %error,
                            "concurrency sampler could not acquire DB connection"
                        );
                        read_failed = true;
                        continue;
                    }
                };
                match queue::concurrency_key_stats(&mut conn, &queues).await {
                    Ok(stats) => {
                        for stat in &stats {
                            // Grouped by (key, task_type) so workflow and activity
                            // budgets for the same key don't collide on the label.
                            let metric_key = format!("{}:{}", stat.key, stat.task_type);
                            *in_flight_by_key.entry(metric_key.clone()).or_insert(0) +=
                                u64::try_from(stat.in_flight).unwrap_or(0);
                            let saturated = stat.in_flight >= i64::from(stat.max_concurrent);
                            if saturated && stat.pending > 0 {
                                tracing::debug!(
                                    concurrency_key = %stat.key,
                                    task_type = %stat.task_type,
                                    in_flight = stat.in_flight,
                                    max_concurrent = stat.max_concurrent,
                                    deferred = stat.pending,
                                    "concurrency cap saturated; pending tasks deferred until a slot frees"
                                );
                                *deferred_by_key.entry(metric_key).or_insert(0) +=
                                    u64::try_from(stat.pending).unwrap_or(0);
                            }
                        }
                    }
                    Err(error) => {
                        tracing::debug!(error = %error, "concurrency key stats sample failed");
                        read_failed = true;
                    }
                }
            }

            if read_failed {
                if cancel.is_cancelled() {
                    break;
                }
                continue;
            }

            for (metric_key, in_flight) in &in_flight_by_key {
                telemetry
                    .metrics
                    .record_concurrency_key_in_flight(metric_key, *in_flight);
            }
            for (metric_key, deferred) in &deferred_by_key {
                telemetry
                    .metrics
                    .record_concurrency_key_deferred(metric_key, *deferred);
            }

            if cancel.is_cancelled() {
                break;
            }
        }
    })
}

/// Periodically sample rate limit buckets from the `harvest_rate_limit_buckets` table
/// and forward available tokens and refill rates to the configured [`MetricsRecorder`](crate::telemetry::MetricsRecorder).
///
/// Stops when the cancellation token fires.
fn spawn_rate_limit_sampler(
    // One pool per shard to aggregate over (see `spawn_queue_depth_sampler`).
    pools: Vec<DbPool>,
    cancel: CancellationToken,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }

            // Rate-limit buckets are per-shard. Aggregate per key across shards:
            // available tokens are summed (total budget fleet-wide) and the
            // refill rate is the max (it is configured identically per shard, so
            // max == any present value).
            //
            // Cardinality rule (issue #699 review, #1 / ADR-0001 §7): the sampler
            // query excludes unbounded per-tenant key families
            // (`dyn-rate:`/`start-throttle:`) because `key` is emitted as a metric
            // LABEL here and per-tenant buckets are never GC'd — labelling by a
            // caller-resolved key would create one time-series per tenant forever.
            // Per-tenant bucket state is observable via `GET /admin/rate-limits`.
            let mut tokens_by_key: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            let mut refill_by_key: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            // Skip the whole tick on any shard read failure so a partial aggregate
            // doesn't under-report available tokens during a storage outage (#522).
            let mut read_failed = false;
            for pool in &pools {
                let mut conn = match pool.get().await {
                    Ok(conn) => conn,
                    Err(error) => {
                        tracing::debug!(
                            error = %error,
                            "rate limit sampler could not acquire DB connection"
                        );
                        read_failed = true;
                        continue;
                    }
                };

                let result = queue::sample_rate_limit_buckets(&mut conn).await;

                match result {
                    Ok(buckets) => {
                        for bucket in buckets {
                            *tokens_by_key.entry(bucket.key.clone()).or_insert(0.0) +=
                                bucket.estimated_tokens;
                            let slot = refill_by_key.entry(bucket.key).or_insert(0.0);
                            *slot = slot.max(bucket.refill_rate);
                        }
                    }
                    Err(error) => {
                        tracing::debug!(error = %error, "rate limit sampler query failed");
                        read_failed = true;
                    }
                }
            }

            if read_failed {
                if cancel.is_cancelled() {
                    break;
                }
                continue;
            }

            for (key, tokens) in &tokens_by_key {
                telemetry
                    .metrics
                    .record_rate_limit_tokens_available(key, *tokens);
            }
            for (key, refill_rate) in &refill_by_key {
                telemetry
                    .metrics
                    .record_rate_limit_refill_rate(key, *refill_rate);
            }

            if cancel.is_cancelled() {
                break;
            }
        }
    })
}

/// Periodically sample the dead-letter queue entry count and forward it to
/// the configured [`MetricsRecorder`](crate::telemetry::MetricsRecorder).
///
/// Runs on the same cadence as the queue-depth sampler. For sharded
/// deployments the caller should spawn one instance per shard, passing the
/// shard-specific pool; single-shard deployments pass their single pool and
/// `shard_id = 0`.
///
/// Stops when the cancellation token fires.
fn spawn_dlq_depth_sampler(
    pool: DbPool,
    cancel: CancellationToken,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    shard_id: u16,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }

            let mut conn = match pool.get().await {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::debug!(
                        error = %error,
                        shard_id,
                        "dlq depth sampler could not acquire DB connection"
                    );
                    continue;
                }
            };

            match crate::dlq::dead_letter_count(&mut conn).await {
                Ok(count) => {
                    let depth = u64::try_from(count).unwrap_or(0);
                    telemetry.metrics.record_dlq_entries(shard_id, depth);
                }
                Err(error) => {
                    tracing::debug!(error = %error, "dlq depth sample failed");
                }
            }

            if cancel.is_cancelled() {
                break;
            }
        }
    })
}

/// Spawn the overdue-schedule sampler (issue #696).
///
/// Emits the `harvest.schedule.overdue` gauge (`1`/`0` per schedule) so a
/// stalled cron — the scheduler loop not ticking, `next_run_at` wedged in the
/// past, an HA claim that never released — is detected within one cadence grace
/// window instead of downstream. Runs on the worker (not the scheduler tick) so
/// a wedged tick cannot suppress its own health signal; a total scheduler
/// outage is still caught here as long as any worker is alive, and only a total
/// process outage falls back to the absence-of-signal alert. Iterates every
/// shard pool (`pools`) so a schedule wedged on one shard is surfaced while
/// others are healthy (AC5). Skipped entirely when no metrics recorder is
/// configured — the per-shard schedule/execution queries are not free.
#[cfg(feature = "db")]
fn spawn_schedule_overdue_sampler(
    // One pool per shard to aggregate over (see `spawn_queue_depth_sampler`).
    pools: Vec<DbPool>,
    cancel: CancellationToken,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    poll_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !telemetry.metrics.is_enabled() {
            return;
        }
        // Labels emitted on the previous pass, so a series that disappears from a
        // COMPLETE pass (schedule deleted/renamed) can be zeroed instead of
        // sticking at its last value (Codex round 5 F2).
        let mut previous_emitted: std::collections::HashSet<ScheduleGaugeKey> =
            std::collections::HashSet::new();
        loop {
            // Sample FIRST, then sleep (Codex round 5 F1): an eager pass on
            // startup emits the gauge and learns the adaptive cadence
            // immediately, so a worker that starts while a fast schedule is
            // already wedged detects it within one cadence grace window rather
            // than only after the initial ceiling sleep.
            let now = chrono::Utc::now();
            // Aggregate verdicts across ALL shard pools into one (kind, name) map
            // (overdue if any shard's same-named schedule is overdue) BEFORE
            // emitting, mirroring the queue_depth precedent. This closes the
            // cross-shard last-write-wins masking window a per-pool `.set()`
            // would leave if a name transiently existed on two shards.
            let mut by_key: std::collections::BTreeMap<ScheduleGaugeKey, bool> =
                std::collections::BTreeMap::new();
            // Minimum active cadence across every shard, for the next interval.
            let mut min_cadence: Option<Duration> = None;
            // A pass is COMPLETE only when every shard was queried without error.
            // Any connection or read failure makes it PARTIAL, which gates the
            // disappeared-label cleanup (a shard's schedules are absent then, not
            // deleted).
            let mut pass_complete = true;
            for pool in &pools {
                let mut conn = match pool.get().await {
                    Ok(conn) => conn,
                    Err(error) => {
                        pass_complete = false;
                        tracing::debug!(
                            error = %error,
                            "schedule overdue sampler could not acquire DB connection"
                        );
                        continue;
                    }
                };
                // A read failure only skips that shard's schedules (each schedule
                // lives on one shard, so its gauge holds its last value rather
                // than being zero-filled misleadingly).
                match crate::scheduler::overdue_schedule_pass(&mut conn, now).await {
                    Ok(pass) => {
                        for s in pass.samples {
                            let entry = by_key.entry((s.kind, s.name)).or_insert(false);
                            *entry = *entry || s.overdue;
                        }
                        if let Some(step) = pass.min_cadence_step {
                            min_cadence = Some(min_cadence.map_or(step, |cur| cur.min(step)));
                        }
                    }
                    Err(error) => {
                        pass_complete = false;
                        tracing::debug!(error = %error, "schedule overdue sample failed");
                    }
                }
            }
            let current_keys: std::collections::HashSet<ScheduleGaugeKey> =
                by_key.keys().cloned().collect();
            for ((kind, name), overdue) in by_key {
                telemetry
                    .metrics
                    .record_schedule_overdue(&kind, &name, overdue);
            }
            // Zero any label that vanished from a COMPLETE pass (deleted/renamed
            // schedule). Empty on a partial pass, so a transient shard outage
            // never zeroes a genuinely-overdue schedule on the failed shard.
            for (kind, name) in
                crate::worker::labels_to_clear(&previous_emitted, &current_keys, pass_complete)
            {
                telemetry
                    .metrics
                    .record_schedule_overdue(&kind, &name, false);
            }
            // Track emitted labels: prune to the current set on a complete pass
            // (cleared labels are now 0 and gone); grow-only on a partial pass so
            // a label that may live on the failed shard is never dropped.
            if pass_complete {
                previous_emitted = current_keys;
            } else {
                previous_emitted.extend(current_keys);
            }

            // Adapt the next sleep to the fleet's fastest active cadence, then
            // sleep (cancellable).
            let interval = crate::worker::next_overdue_sample_interval(min_cadence, poll_interval);
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
        }
    })
}

/// Spawn the worker slot-occupancy sampler (issue #531).
///
/// Reads the two dispatch `Semaphore`s' `available_permits()` against the
/// *live* dispatch target on the same cadence as the queue/DLQ samplers and
/// emits the `harvest.worker.slots_in_use` / `harvest.worker.slots_available`
/// gauges, labelled by `slot_type`. This is a pure in-memory read — no DB
/// access, no new lock contention. Skipped entirely when no metrics recorder
/// is configured.
///
/// `*_slot_target` is always present (issue #548): equal to the static
/// `max_concurrent_*` and never mutated when no slot tuner is configured, so
/// this sampler's output is byte-identical to before the tuner existed.
/// `Semaphore::available_permits()` already excludes any tuner-withheld
/// permits (they are genuinely acquired and held, not merely reserved), so
/// `crate::slot_tuner::slot_occupancy` computes occupancy directly from the
/// live target and the raw available-permits reading — see that function's
/// doc comment for why no separate "subtract withheld" step is needed.
fn spawn_worker_slot_sampler(
    workflow_semaphore: Arc<Semaphore>,
    workflow_slot_target: Arc<AtomicUsize>,
    activity_semaphore: Arc<Semaphore>,
    activity_slot_target: Arc<AtomicUsize>,
    cancel: CancellationToken,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }

            // UFCS avoids ambiguity with `RunQueryDsl::load` (in scope in this
            // module), which also matches a by-value `.load(...)` call.
            let wf_target = AtomicUsize::load(&workflow_slot_target, Ordering::Relaxed);
            let (wf_in_use, wf_available) = crate::slot_tuner::slot_occupancy(
                wf_target,
                workflow_semaphore.available_permits(),
            );
            telemetry
                .metrics
                .record_worker_slots(SlotType::Workflow, wf_in_use, wf_available);

            // Activity read is a separate snapshot; cross-type totals may reflect
            // different instants when a task is dispatched between the two reads.
            let act_target = AtomicUsize::load(&activity_slot_target, Ordering::Relaxed);
            let (act_in_use, act_available) = crate::slot_tuner::slot_occupancy(
                act_target,
                activity_semaphore.available_permits(),
            );
            telemetry
                .metrics
                .record_worker_slots(SlotType::Activity, act_in_use, act_available);
        }
    })
}

/// Spawn the stranded-work sampler (issue #522).
///
/// Iterates every shard visible through the pool (not just the shards this
/// worker is assigned to) so that a writable shard with queued work and no
/// covering worker is caught even when this worker is not assigned to it.
///
/// Per-queue coverage: for each shard the sampler loads the per-queue claimable
/// pending counts, then checks which of those queues have at least one healthy
/// active worker assigned to the shard. Tasks on queues with no such worker are
/// counted as stranded and emitted via `harvest.shard.stranded_pending`.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
fn spawn_stranded_work_sampler(
    sharded_pool: crate::shard::ShardedDbPool,
    freshness_window: Duration,
    cancel: CancellationToken,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    interval: Duration,
    // Static set of activity names with a circuit-breaker policy; these skip the
    // rate-limit gate at claim, so they stay claimable with an empty bucket.
    circuit_breaker_activities: Vec<String>,
    // Map of activity_name → required_capabilities JSON for activities that
    // declare `requires`; back-fills the eligibility gate for un-snapshotted rows.
    activity_requirements: std::collections::HashMap<String, serde_json::Value>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
            if cancel.is_cancelled() {
                break;
            }

            // Iterate ALL shards so unassigned writable shards with stranded
            // work are surfaced, not just the shards this worker covers.
            for (shard_id, shard_pool) in sharded_pool.iter_shards() {
                let shard_u16 = u16::try_from(shard_id.as_i32()).unwrap_or(0);

                // Claimable pending demands for this shard, grouped by
                // (queue, required_capabilities, ...) so coverage can honour the
                // same eligibility claim_task enforces (issue #522 review).
                let mut demands: Vec<crate::queue::ClaimablePendingDemand> = {
                    let mut conn = match shard_pool.get().await {
                        Ok(conn) => conn,
                        Err(error) => {
                            tracing::debug!(
                                shard_id = %shard_id.as_i32(),
                                error = %error,
                                "stranded-work sampler could not acquire DB connection"
                            );
                            continue;
                        }
                    };
                    match crate::queue::claimable_pending_demand_by_queue(
                        &mut conn,
                        &circuit_breaker_activities,
                    )
                    .await
                    {
                        Ok(demands) => demands,
                        Err(error) => {
                            tracing::debug!(
                                shard_id = %shard_id.as_i32(),
                                error = %error,
                                "stranded-work count query failed"
                            );
                            continue;
                        }
                    }
                };

                // Back-fill effective requirements for activity rows that didn't
                // snapshot required_capabilities (legacy/manual enqueues), so the
                // coverage check below applies the same activity eligibility
                // claim_task's $6 gate enforces.
                crate::queue::apply_activity_requirements(&mut demands, &activity_requirements);

                if demands.is_empty() {
                    // No claimable work — emit 0 so the gauge resets cleanly.
                    telemetry
                        .metrics
                        .record_shard_stranded_pending(shard_u16, 0);
                    continue;
                }

                // Healthy active workers assigned to this shard. Kept whole (not
                // collapsed to queue names) so the capability check below can see
                // each worker's polled queues *and* labels.
                let covering_workers: Vec<crate::workers::WorkerRow> = {
                    let Ok(mut conn) = shard_pool.get().await else {
                        continue;
                    };
                    let filters = crate::workers::WorkerFilters {
                        status: Some(crate::workers::WorkerStatus::Active.as_str().to_string()),
                        shard_id: Some(shard_id.as_i32()),
                        health: Some(crate::workers::WorkerHealth::Healthy),
                        limit: i64::MAX,
                        ..Default::default()
                    };
                    match crate::workers::list_workers(&mut conn, &filters, freshness_window).await
                    {
                        Ok(workers) => workers,
                        Err(error) => {
                            tracing::debug!(
                                error = %error,
                                "worker list query failed for stranded-work sampler"
                            );
                            continue;
                        }
                    }
                };

                // Build compatibility set for this shard (issue #171 routing).
                // Used to honour the same required_build_id eligibility
                // claim_task enforces. On load failure fall back to an empty set
                // (exact-match / legacy-worker rules still apply).
                let compat_set = {
                    let Ok(mut conn) = shard_pool.get().await else {
                        continue;
                    };
                    crate::build_routing::load_compat_set(&mut conn)
                        .await
                        .unwrap_or_default()
                };

                // A demand is covered when some covering worker polls its queue
                // AND satisfies its required_capabilities (the same Exact/In
                // label match claim_task applies) AND is build-eligible for its
                // required_build_id (the same exact/compatible/legacy rule) AND,
                // when the row is held by an unexpired sticky lease, *is* that
                // lease's owner (only it can claim until the lease expires). All
                // constraints are checked against the *same* worker so a task
                // needing several is not falsely covered by different workers
                // each satisfying only one. No requirement ⇒ that dimension is
                // trivially satisfied; unparseable capabilities fall back to the
                // other dimensions so the sampler never fabricates a
                // false-positive stranded signal.
                let demand_covered = |demand: &crate::queue::ClaimablePendingDemand| -> bool {
                    let reqs = demand.required_capabilities.as_ref().and_then(|caps| {
                        serde_json::from_value::<Vec<crate::eligibility::Requirement>>(caps.clone())
                            .ok()
                    });
                    covering_workers.iter().any(|w| {
                        let polls_queue = w.worker.queues.as_array().is_some_and(|qs| {
                            qs.iter()
                                .any(|v| v.as_str() == Some(demand.queue_name.as_str()))
                        });
                        if !polls_queue {
                            return false;
                        }
                        if demand
                            .sticky_owner
                            .as_deref()
                            .is_some_and(|owner| owner != w.worker.worker_id)
                        {
                            return false;
                        }
                        if !compat_set
                            .is_eligible(&w.worker.build_id, demand.required_build_id.as_deref())
                        {
                            return false;
                        }
                        reqs.as_ref().is_none_or(|reqs| {
                            let labels: std::collections::HashMap<String, String> =
                                serde_json::from_value(w.worker.labels.clone()).unwrap_or_default();
                            crate::eligibility::matches_requirements(reqs, &labels)
                        })
                    })
                };

                // Sum pending tasks across demands no covering worker can claim.
                let stranded: u64 = demands
                    .iter()
                    .filter(|demand| !demand_covered(demand))
                    .map(|demand| u64::try_from(demand.count).unwrap_or(0))
                    .sum();
                telemetry
                    .metrics
                    .record_shard_stranded_pending(shard_u16, stranded);
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

/// The worker runtime that polls the task queue and dispatches work.
#[derive(Debug)]
pub struct Worker {
    /// Validated runtime configuration.
    pub config: WorkerRuntimeConfig,
    /// Shared handler registry.
    pub registry: Arc<HandlerRegistry>,
    /// Set of activities that this worker cannot run because of unsatisfied requirements (issue #382).
    pub ineligible_activities: Vec<String>,
    /// Bounds concurrent workflow task executions.
    workflow_semaphore: Arc<Semaphore>,
    /// Bounds concurrent activity task executions.
    activity_semaphore: Arc<Semaphore>,
    /// Total permits behind `workflow_semaphore` (issue #548): the static
    /// `max_concurrent_workflows` when no slot tuner is configured, or the
    /// tuner's `max_slots` when one is. `drain_in_flight` uses this instead
    /// of `config.max_concurrent_workflows` so it waits for the *actual*
    /// permit count, which is unchanged (and therefore byte-identical) when
    /// no tuner is configured.
    workflow_permit_total: usize,
    /// Total permits behind `activity_semaphore` (issue #548). See
    /// `workflow_permit_total`.
    activity_permit_total: usize,
    /// Longest claim-to-dispatch permit-wait observed since the slot tuner's
    /// last tick, in microseconds (issue #548). `None` when no tuner is
    /// configured, so the hot dispatch path performs no extra work in the
    /// default (untuned) case. Reset to 0 by the tuner loop each tick
    /// (`AtomicU64::swap`).
    ///
    /// Deliberately **not** paired with a `TunedSlotRuntime`/live-target
    /// field on `Worker` itself: that type holds `OwnedSemaphorePermit`s
    /// (clippy treats it as significant-drop), and every long-lived `Worker`
    /// binding in the test suite would otherwise be flagged. Instead
    /// `spawn_monitoring_tasks` constructs the `TunedSlotRuntime` (and its
    /// paired live-target cell) locally and hands it directly to the spawned
    /// tuner-loop task in the same call, before any task can be dispatched —
    /// see the comment there.
    workflow_permit_wait_micros: Option<Arc<AtomicU64>>,
    /// Longest claim-to-dispatch permit-wait for the activity semaphore
    /// (issue #548). See `workflow_permit_wait_micros`.
    activity_permit_wait_micros: Option<Arc<AtomicU64>>,
    /// Set the first time `spawn_monitoring_tasks` runs to completion (issue
    /// #548 review). Guards against a hypothetical second invocation (e.g. a
    /// future caller wrapping `run`/`run_with_listener` in a retry loop)
    /// constructing a second `TunedSlotRuntime` over the same semaphore,
    /// which would race the first tuner-loop task over the same withheld
    /// permits and corrupt both the effective concurrency band and the
    /// `slot_target` gauges. No production call site does this today (see
    /// the doc comment on `spawn_monitoring_tasks`), but the guard is cheap
    /// (a single `AtomicBool`, trivial `Drop`) and turns a silent state race
    /// into a loud, diagnosable error instead.
    monitoring_started: std::sync::atomic::AtomicBool,
    /// Cancellation token for graceful shutdown.
    shutdown: CancellationToken,
    /// Set (and refreshed on every heartbeat) by the heartbeat task while the
    /// worker is draining.  Holds the absolute deadline from the operator's
    /// `drain_deadline_at` so that `drain_in_flight` can honour an extended
    /// window even after it has already started waiting.
    remote_drain_deadline: Arc<Mutex<Option<std::time::Instant>>>,
    /// Maximum `drain_deadline_at` value applied to `remote_drain_deadline` so far,
    /// shared across the per-shard heartbeat tasks (issue #522 review). A multi-
    /// shard worker runs one heartbeat per assigned shard, all sharing this value;
    /// `sync_drain_deadline` rejects any observed deadline that is not strictly
    /// greater than this maximum, preventing a lagging shard from reverting the
    /// cell to a stale shorter value.
    drain_deadline_max: Arc<Mutex<Option<chrono::DateTime<chrono::Utc>>>>,
    /// Per-worker in-process LRU cache for suspended workflow event histories.
    ///
    /// Populated after each suspension; consulted at the start of each workflow
    /// task to decide whether a delta load or a full history load is needed.
    /// Wrapped in `Arc<tokio::sync::Mutex<_>>` so it can be shared across
    /// concurrently-running task handler futures without cloning the events.
    workflow_cache: Arc<tokio::sync::Mutex<crate::cache::WorkflowCache>>,
    /// In-process consecutive-timeout strike counter per workflow execution
    /// (issue #494).
    ///
    /// Each time a workflow-task dispatch exceeds `workflow_task_timeout` the
    /// counter for that execution ID is incremented. Once it reaches
    /// `poison_pill_threshold` the task is quarantined to the DLQ rather than
    /// re-queued. The counter is cleared when a dispatch completes (either
    /// successfully or with an error) so only **consecutive** timeouts count.
    ///
    /// Keyed by `workflow_exec_id` (not `task.id`) so re-queued tasks from
    /// the same execution accumulate toward the same threshold.
    workflow_task_timeout_strikes:
        Arc<std::sync::Mutex<std::collections::HashMap<uuid::Uuid, i32>>>,
    /// In-process consecutive-panic strike counter per workflow execution
    /// (issue #782).
    ///
    /// Each contained workflow-handler panic increments the counter for that
    /// execution ID; once it reaches `workflow_panic_max_attempts` the run is
    /// failed terminally rather than re-dispatched. The counter is cleared on
    /// **any completed non-panic decision cycle** for that execution (completed
    /// / suspended / continued-as-new / author-Err failed / ND-blocked) AND on
    /// an **early terminal-fail** inside the drive loop (a transient error that
    /// takes the execution terminal FAILED before reaching the panic gate — see
    /// `fail_workflow_execution_clearing_strikes`), so a strike entry never
    /// outlives its execution. Only the panic **re-dispatch** path deliberately
    /// leaves the just-incremented strike in place (it returns `Ok(())` and is
    /// never routed through the clear); clearing there would reset the budget on
    /// every panic and hot-loop forever.
    ///
    /// Keyed by `workflow_exec_id` (not `task.id`) so re-dispatched tasks from
    /// the same execution accumulate toward the same budget. In-process and
    /// per-worker-instance: it resets on worker restart, intentionally granting
    /// a fresh budget to a hotfix redeploy while still bounding churn on a
    /// single long-lived worker. An out-of-band cancel/terminate/reset that
    /// takes the execution terminal without ever re-entering
    /// `process_workflow_task` may leak one bounded entry until worker restart —
    /// identical precedent to `workflow_task_timeout_strikes` (issue #494).
    workflow_panic_strikes: Arc<std::sync::Mutex<std::collections::HashMap<uuid::Uuid, u32>>>,
    /// In-process registry of worker sessions currently hosted by this
    /// worker (issue #606), bounded against `config.max_concurrent_sessions`
    /// via [`crate::sessions::try_acquire_session_slot`]. `0` (the default
    /// `max_concurrent_sessions`) means sessions are disabled, so the
    /// session-acquire interception in `process_activity_task` always loses
    /// the race and reschedules -- zero behavior change beyond that
    /// reschedule loop, which only ever fires for a workflow that calls
    /// `create_session` against a worker that never opted in. Tracks
    /// identity (`SessionId`), not just a count, so a periodic reconciler
    /// (see [`crate::sessions::reconcile_local_sessions`]) can detect and
    /// release a specific slot this worker's local view believes is still
    /// held but whose `harvest_sessions` row has since left `ACTIVE` --
    /// e.g. because the broken-session scanner reclaimed it, or because a
    /// prior release attempt failed after already freeing the slot
    /// in-memory. A `HashSet<SessionId>` rather than a
    /// `tokio::sync::Semaphore`/`OwnedSemaphorePermit` map -- see
    /// [`crate::sessions::SessionSlotRegistry`]'s doc comment for why.
    session_slots_in_use: crate::sessions::SessionSlotRegistry,
}

struct WorkerMonitoringHandles {
    queue_depth_sampler: tokio::task::JoinHandle<()>,
    concurrency_sampler: tokio::task::JoinHandle<()>,
    rate_limit_sampler: tokio::task::JoinHandle<()>,
    dlq_depth_samplers: Vec<tokio::task::JoinHandle<()>>,
    timeout_checkers: Vec<tokio::task::JoinHandle<()>>,
    poison_pill_reclaimers: Vec<tokio::task::JoinHandle<()>>,
    pause_auto_resumers: Vec<tokio::task::JoinHandle<()>>,
    /// Worker-session local-registry reconcilers (issue #606). Empty when
    /// `db` is disabled.
    session_slot_reconcilers: Vec<tokio::task::JoinHandle<()>>,
    history_oversized_sampler: tokio::task::JoinHandle<()>,
    worker_slot_sampler: Option<tokio::task::JoinHandle<()>>,
    stranded_work_sampler: Option<tokio::task::JoinHandle<()>>,
    /// Overdue-schedule gauge sampler (issue #696). `Some` under `db` (the task
    /// itself no-ops when metrics are disabled); `None` without `db`.
    schedule_overdue_sampler: Option<tokio::task::JoinHandle<()>>,
    /// Adaptive slot-tuner control loops (issue #548). Empty when no tuner
    /// is configured.
    slot_tuners: Vec<tokio::task::JoinHandle<()>>,
    /// The live dispatch-slot target for each semaphore (issue #548 review):
    /// the tuner's current resize target when a tuner is configured, or a
    /// fixed atomic holding the configured max otherwise. Threaded into
    /// `spawn_heartbeat_task` so fleet in-flight accounting tracks the
    /// worker's *actual* current capacity rather than the static
    /// `max_concurrent_*` config value, which would silently diverge from
    /// reality once the tuner resizes away from its initial target.
    workflow_slot_target: Arc<AtomicUsize>,
    activity_slot_target: Arc<AtomicUsize>,
}

/// Spawn the bounded-pause auto-resume scanner (issue #383).
///
/// Periodically force-resumes executions paused longer than
/// `max_workflow_pause_duration` so orphaned pauses cannot accumulate. Runs at
/// the worker heartbeat cadence (background maintenance, off the hot path).
fn spawn_pause_auto_resumer(
    pool: DbPool,
    cancel: CancellationToken,
    interval: Duration,
    max_pause_duration: Duration,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }

            match pool.get().await {
                Ok(mut conn) => {
                    match crate::execution::auto_resume_expired_pauses(
                        &mut conn,
                        max_pause_duration,
                        &*telemetry.metrics,
                    )
                    .await
                    {
                        Ok(n) if n > 0 => {
                            tracing::warn!(resumed = n, "auto-resumed over-long paused executions");
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!(error = %e, "pause auto-resume scan failed");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to acquire DB connection for pause auto-resume");
                }
            }

            if cancel.is_cancelled() {
                break;
            }
        }
    })
}

/// Periodically sample the count of in-flight (RUNNING) executions per
/// workflow type whose history size exceeds the soft `continue_as_new`
/// threshold and emit it as a gauge metric (issue #493, AC2).
///
/// A non-zero gauge value signals runaway workflow executions that are
/// growing toward the hard ceiling and should be addressed by the author
/// (e.g. via `ctx.should_continue_as_new()`) or by the operator (via the
/// hard ceiling or manual continue-as-new).
fn spawn_history_oversized_sampler(
    // One pool per shard to aggregate over (see `spawn_queue_depth_sampler`).
    pools: Vec<DbPool>,
    cancel: CancellationToken,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    soft_threshold: u64,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut reported_workflows = std::collections::HashSet::new();
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }

            // Oversized RUNNING executions live on the shard that owns them, so
            // sum the per-workflow counts across every shard for a fleet-wide
            // gauge.
            let mut count_by_workflow: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            // Skip the whole tick on any shard read failure so a partial aggregate
            // (or the zero-fill below) doesn't clear the gauge during an outage
            // (#522).
            let mut read_failed = false;
            for pool in &pools {
                let mut conn = match pool.get().await {
                    Ok(conn) => conn,
                    Err(error) => {
                        tracing::debug!(
                            error = %error,
                            "history oversized sampler could not acquire DB connection"
                        );
                        read_failed = true;
                        continue;
                    }
                };

                match sample_history_oversized_counts(&mut conn, soft_threshold).await {
                    Ok(counts) => {
                        for (workflow_name, count) in counts {
                            *count_by_workflow.entry(workflow_name).or_insert(0) += count;
                        }
                    }
                    Err(error) => {
                        tracing::debug!(error = %error, "history oversized sample failed");
                        read_failed = true;
                    }
                }
            }

            if read_failed {
                if cancel.is_cancelled() {
                    break;
                }
                continue;
            }

            let active_workflows: std::collections::HashSet<String> =
                count_by_workflow.keys().cloned().collect();
            for (workflow_name, count) in &count_by_workflow {
                telemetry
                    .metrics
                    .record_workflow_history_oversized(workflow_name, *count);
            }
            // Zero-fill workflows that were oversized last tick but no longer are
            // so stale gauge values do not linger.
            for workflow_name in reported_workflows.difference(&active_workflows) {
                telemetry
                    .metrics
                    .record_workflow_history_oversized(workflow_name, 0);
            }
            reported_workflows = active_workflows;

            if cancel.is_cancelled() {
                break;
            }
        }
    })
}

/// Query the count of RUNNING executions per workflow type that have
/// accumulated more than `soft_threshold` events.
///
/// Returns `(workflow_name, count)` pairs; omits workflow types with zero
/// oversized executions.
async fn sample_history_oversized_counts(
    conn: &mut diesel_async::AsyncPgConnection,
    soft_threshold: u64,
) -> crate::error::HarvestResult<Vec<(String, u64)>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        oversized_count: i64,
    }

    let rows: Vec<Row> = diesel::sql_query(
        "SELECT wf.workflow_name, COUNT(*)::bigint AS oversized_count \
         FROM harvest_workflow_executions wf \
         WHERE wf.state IN ('RUNNING', 'SUSPENDED') \
         AND (SELECT COUNT(*) FROM harvest_events he WHERE he.workflow_exec_id = wf.id) > $1 \
         GROUP BY wf.workflow_name",
    )
    .bind::<diesel::sql_types::BigInt, _>(i64::try_from(soft_threshold).unwrap_or(i64::MAX))
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.workflow_name,
                u64::try_from(r.oversized_count.max(0)).unwrap_or(0),
            )
        })
        .collect())
}

/// The pieces `Worker::new` needs to construct one dispatch semaphore,
/// whether or not an adaptive slot tuner (issue #548) is configured for it.
///
/// Deliberately holds no `TunedSlotRuntime` — that type owns
/// `OwnedSemaphorePermit`s and is constructed later, inside
/// `spawn_monitoring_tasks`, so `Worker` itself never holds significant-drop
/// state (see the comment on `Worker::workflow_permit_wait_micros`).
struct DispatchSemaphoreParts {
    semaphore: Arc<Semaphore>,
    /// Total permits behind `semaphore`: `configured_max` when untuned, or
    /// the tuner's `max_slots` when tuned.
    permit_total: usize,
    /// `Some` only when a tuner is configured; the dispatch path records the
    /// longest recent permit wait here for the tuner to consume each tick.
    permit_wait_micros: Option<Arc<AtomicU64>>,
}

/// Build one dispatch semaphore (workflow or activity), optionally sized for
/// an adaptive slot tuner (issue #548).
///
/// When `tuner_cfg` is `None` this is exactly today's
/// `Arc::new(Semaphore::new(configured_max))` — byte-for-byte identical
/// behaviour for every worker that has not opted in. When `Some`, the
/// semaphore is created with the tuner's `max_slots` permits, all initially
/// available; `spawn_monitoring_tasks` withholds it down to the clamped
/// initial target before the worker's poll loop starts (see
/// `TunedSlotRuntime::new`), so no task can ever be dispatched against the
/// un-withheld semaphore.
fn build_dispatch_semaphore(
    configured_max: usize,
    tuner_cfg: Option<&crate::slot_tuner::SlotTunerConfig>,
) -> DispatchSemaphoreParts {
    tuner_cfg.map_or_else(
        || DispatchSemaphoreParts {
            semaphore: Arc::new(Semaphore::new(configured_max)),
            permit_total: configured_max,
            permit_wait_micros: None,
        },
        |cfg| DispatchSemaphoreParts {
            semaphore: Arc::new(Semaphore::new(cfg.max_slots)),
            permit_total: cfg.max_slots,
            permit_wait_micros: Some(Arc::new(AtomicU64::new(0))),
        },
    )
}

impl Worker {
    /// Create a new worker from validated config and a handler registry.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if the config fails validation.
    pub fn new(config: WorkerRuntimeConfig, registry: Arc<HandlerRegistry>) -> HarvestResult<Self> {
        config.validate()?;

        // Validate the history ceiling against the soft threshold from the registry.
        // HarvestBuilder already enforces this, but WorkerConfig can set the ceiling
        // directly without going through the builder, so we re-check here where the
        // registry (and thus the actual threshold) is available.
        if let Some(ceiling) = config.max_workflow_history_events {
            let threshold = registry.history_policy().continue_as_new_threshold();
            if ceiling <= threshold {
                return Err(HarvestError::Config(format!(
                    "max_workflow_history_events ({ceiling}) must be strictly greater than \
                     continue_as_new_threshold ({threshold})"
                )));
            }
        }

        // Comprehensive rate-limit validation (issue #699, Codex round-5 P2).
        // A `HandlerRegistry` built directly (not via `HarvestBuilder::try_build`)
        // bypasses ALL builder validation, so an invalid rate-limit config
        // (a dynamic `rate_limit(key = …)` on a local activity, a dynamic key
        // without an rps, or two activities sharing a normalized key-expression
        // with conflicting rps/burst) would otherwise slip past and only be
        // caught piecemeal — or not at all — at schedule time. Run the same
        // shared validator the builder runs, here at the fallible worker-startup
        // boundary, so the whole class fails loud ONCE before the poll loop and
        // first enqueue. The ad-hoc schedule-time guards in
        // `persist_scheduled_activities`/`register_rate_limit_buckets` remain as
        // defense-in-depth; this startup gate is the primary comprehensive check.
        crate::builder::validate_activity_rate_limits(registry.activities.values())
            .map_err(|err| HarvestError::Config(err.to_string()))?;

        let mut ineligible_activities = Vec::new();
        for activity in registry.activities.values() {
            if let Some(requires) = activity.requires {
                let reqs = crate::eligibility::parse_requirements(requires).map_err(|err| {
                    HarvestError::Config(format!(
                        "Invalid requirements for activity {}: {}",
                        activity.name, err
                    ))
                })?;
                if !crate::eligibility::matches_requirements(&reqs, &config.labels) {
                    ineligible_activities.push(activity.name.to_string());
                }
            }
        }

        let workflow_parts =
            build_dispatch_semaphore(config.max_concurrent_workflows, config.slot_tuner.as_ref());
        let activity_parts =
            build_dispatch_semaphore(config.max_concurrent_activities, config.slot_tuner.as_ref());
        let workflow_cache = Arc::new(tokio::sync::Mutex::new(crate::cache::WorkflowCache::new(
            config.workflow_cache_size,
        )));
        Ok(Self {
            config,
            registry,
            ineligible_activities,
            workflow_semaphore: workflow_parts.semaphore,
            activity_semaphore: activity_parts.semaphore,
            workflow_permit_total: workflow_parts.permit_total,
            activity_permit_total: activity_parts.permit_total,
            workflow_permit_wait_micros: workflow_parts.permit_wait_micros,
            activity_permit_wait_micros: activity_parts.permit_wait_micros,
            monitoring_started: std::sync::atomic::AtomicBool::new(false),
            shutdown: CancellationToken::new(),
            remote_drain_deadline: Arc::new(Mutex::new(None)),
            drain_deadline_max: Arc::new(Mutex::new(None)),
            workflow_cache,
            workflow_task_timeout_strikes: Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            workflow_panic_strikes: Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            session_slots_in_use: crate::sessions::new_session_slot_registry(),
        })
    }

    /// Return the assigned shards that have no exact pool entry in the
    /// configured `sharded_pool`.
    ///
    /// A non-empty result is a `ShardedDbPool` misconfiguration: the worker
    /// would advertise coverage (in its fleet row and shard-health view) for
    /// shards it cannot actually poll or heartbeat, silently leaving their work
    /// unclaimed. Callers should treat a non-empty result as a startup error and
    /// refuse to run the process rather than partially serving the configured
    /// set (issue #522 review).
    ///
    /// Always empty when there is no `sharded_pool` (the no-sharded-pool / non-db
    /// paths map every assignment to the default pool, so nothing is missing).
    #[cfg(feature = "db")]
    #[must_use]
    pub fn missing_assigned_shard_pools(&self) -> Vec<i32> {
        self.config
            .sharded_pool
            .as_ref()
            .map_or_else(Vec::new, |sharded| {
                self.config
                    .shard_assignments
                    .iter()
                    .filter(|shard| sharded.exact_pool_for(**shard).is_none())
                    .map(|shard| shard.as_i32())
                    .collect()
            })
    }

    /// Run the main poll loop until shutdown is requested.
    ///
    /// This is the worker's entry point. It keeps polling until shutdown is
    /// requested, checking the cancellation token between poll iterations.
    ///
    /// When the worker has a `sharded_pool` with more than one shard,
    /// it drains all assigned shards via `run_poll_loop_multi` (issue #522).
    /// Otherwise it falls through to the existing single-shard path
    /// (`run_with_listener`) byte-for-byte unchanged.
    #[allow(clippy::too_many_lines)]
    pub async fn run(&self, pool: &DbPool) {
        // Defense-in-depth: refuse to start if ANY assigned shard is missing an
        // exact pool entry (issue #522 review). The authoritative check runs at
        // process startup (`HarvestRunner::start`) and fails the process before
        // this task is ever spawned; this guard remains so a direct `Worker::run`
        // caller that bypasses the runner still fails closed rather than
        // advertising coverage for shards it cannot serve.
        #[cfg(feature = "db")]
        {
            let missing = self.missing_assigned_shard_pools();
            if !missing.is_empty() {
                tracing::error!(
                    worker_id = %self.config.worker_id,
                    missing_shards = ?missing,
                    "one or more shard_assignments are missing from the sharded_pool; refusing to \
                     start this worker rather than advertising coverage for shards it cannot \
                     serve — check your ShardedDbPool configuration"
                );
                return;
            }
        }

        // Resolve the set of (ShardId, DbPool) claim targets from the sharded
        // pool when available, deduplicating by ShardId.
        #[cfg(feature = "db")]
        let shard_targets: Vec<(crate::types::ShardId, DbPool)> = {
            self.config.sharded_pool.as_ref().map_or_else(
                || {
                    self.config
                        .shard_assignments
                        .iter()
                        .map(|shard| (*shard, pool.clone()))
                        .collect()
                },
                |sharded| {
                    let mut seen = std::collections::HashSet::new();
                    self.config
                        .shard_assignments
                        .iter()
                        .filter(|shard| seen.insert(*shard))
                        .map(|shard| {
                            // Presence of an exact pool entry for every assigned
                            // shard is guaranteed by the missing-shard guard at
                            // run() entry, so this never falls back to the default
                            // pool under the wrong shard label (issue #522 review).
                            let shard_pool = sharded
                                .exact_pool_for(*shard)
                                .expect("assigned shard pool presence verified at run() entry")
                                .clone();
                            (*shard, shard_pool)
                        })
                        .collect()
                },
            )
        };
        #[cfg(not(feature = "db"))]
        let shard_targets: Vec<(crate::types::ShardId, DbPool)> = self
            .config
            .shard_assignments
            .iter()
            .map(|shard| (*shard, pool.clone()))
            .collect();

        // Issue #965: startup-seed. Make every builder-registered WASM activity
        // module available on each shard's database before the poll loop begins,
        // so an embedder who only calls `HarvestBuilder::wasm_activity(...)` and
        // starts the worker gets a working WASM activity with no manual publish.
        // This SEEDS (activate-only-if-absent), it does NOT publish: a restarted
        // older worker embedding v1 must not flip a shard the DB has already
        // hot-swapped to v2 back to v1 (issue #965 review). The embedded bytes
        // are always made fetchable-by-hash (so in-flight pinned attempts and
        // this worker can run them); they only become *active* when no active
        // version exists yet. Idempotent, and per-shard.
        //
        // Fail closed (issue #965 review): if the worker cannot seed its
        // advertised WASM modules on an assigned shard — because the connection
        // is unavailable or the seed itself errors (transient DB/migration/
        // validation failure) — refuse to start rather than enter the poll loop.
        // Otherwise the worker would advertise WASM activities that resolve to a
        // non-retryable `WasmModuleUnavailable` on that shard indefinitely. This
        // mirrors the missing-shard-pool guard at `run()` entry.
        #[cfg(feature = "wasm-activities")]
        {
            let registrations = self.registry.wasm_module_registrations();
            if !registrations.is_empty() {
                // Mirror the poll loop's `[] => pool` shape (see `claim_pool`
                // below): when no shard is explicitly assigned — the default,
                // legacy single-shard worker — `shard_targets` is empty, so the
                // worker polls and resolves against the caller's default `pool`.
                // Seed that same pool here. Otherwise a builder-registered WASM
                // module is never inserted and every WASM activity resolves to a
                // non-retryable `WasmModuleUnavailable` in the common single-shard
                // config (issue #965 review, Finding 24). With explicit shard
                // assignments, seed each shard's pool as before. Normalizing to
                // one list keeps seeding and polling in agreement.
                let seed_targets: Vec<(Option<crate::types::ShardId>, &DbPool)> =
                    if shard_targets.is_empty() {
                        vec![(None, pool)]
                    } else {
                        shard_targets
                            .iter()
                            .map(|(shard, shard_pool)| (Some(*shard), shard_pool))
                            .collect()
                    };
                for (shard, shard_pool) in seed_targets {
                    match shard_pool.get().await {
                        Ok(mut conn) => {
                            if let Err(e) = crate::wasm_store::seed_registered_wasm_modules(
                                &mut conn,
                                registrations,
                            )
                            .await
                            {
                                tracing::error!(
                                    worker_id = %self.config.worker_id,
                                    shard = ?shard,
                                    error = %e,
                                    "failed to seed registered wasm modules; refusing to start \
                                     this worker rather than advertising wasm activities it \
                                     cannot serve on this shard"
                                );
                                return;
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                worker_id = %self.config.worker_id,
                                shard = ?shard,
                                error = %e,
                                "failed to acquire a connection to seed registered wasm modules; \
                                 refusing to start this worker rather than advertising wasm \
                                 activities it cannot serve on this shard"
                            );
                            return;
                        }
                    }
                }
            }
        }

        // More than one distinct shard target → multi-shard loop.
        // One or zero targets (or single-pool fallback) → existing path.
        #[cfg(feature = "db")]
        let use_multi_shard = shard_targets.len() > 1
            && self
                .config
                .sharded_pool
                .as_ref()
                .is_some_and(|sp| sp.shard_ids().len() > 1);
        #[cfg(not(feature = "db"))]
        let use_multi_shard = false;

        if use_multi_shard {
            self.run_multi_shard(shard_targets, pool).await;
        } else {
            // Single-shard fast path. When exactly one shard target resolved,
            // claim from that shard's pool rather than the caller's default
            // pool — in a one-process-per-shard deployment assigned to a
            // non-default shard (e.g. shard_assignments = [1]) those differ,
            // and using the default pool would strand the assigned shard's
            // work. With no assignment, fall back to the default pool (legacy
            // behavior, byte-for-byte unchanged).
            // Match on the slice rather than calling `.first()`: the diesel
            // prelude's query-DSL traits are in scope, and probing `.first()`
            // on the Vec sends the trait solver into overflow (E0275).
            let claim_pool = match shard_targets.as_slice() {
                [(_, shard_pool), ..] => shard_pool,
                [] => pool,
            };
            // Listen on the database we actually poll. For a one-process-per-
            // shard deployment assigned to a non-default shard, the global
            // `notification_database_url` may point at a different shard, so a
            // NOTIFY fired on the polled shard would never wake this worker
            // (issue #522). Prefer this shard's entry in
            // `shard_notification_database_urls`.
            //
            // The global URL pairs with the default pool, so it is a safe
            // fallback only when the claim pool *is* that default pool: no
            // `ShardedDbPool` at all, or the resolved shard is the pool's
            // `default_shard()` (or is absent from the map, so `pool_for` falls
            // back to the default pool the global URL targets). A single-shard
            // wrapper — the shape every legacy single-shard deployment uses —
            // therefore keeps its global LISTEN. With real multi-shard routing
            // the resolved pool is shard-specific and the global URL may point
            // at a different database; a NOTIFY there would never wake this
            // worker, so an absent per-shard URL means poll-only for this shard
            // rather than risking a wrong-database listener (issue #522 review).
            #[cfg(feature = "db")]
            let global_safe = match (shard_targets.as_slice(), self.config.sharded_pool.as_ref()) {
                ([(shard, _), ..], Some(sp)) => {
                    *shard == sp.default_shard() || sp.exact_pool_for(*shard).is_none()
                }
                // No sharded pool, or no resolved shard: the claim pool is
                // the default pool the global URL targets.
                _ => true,
            };
            #[cfg(not(feature = "db"))]
            let global_safe = true;
            let listener_url: Option<&str> = match shard_targets.as_slice() {
                [(shard_id, _), ..] => {
                    let per_shard = self
                        .config
                        .shard_notification_database_urls
                        .iter()
                        .find(|(s, _)| s == shard_id)
                        .map(|(_, url)| url.as_str());
                    per_shard.or_else(|| {
                        global_safe
                            .then_some(self.config.notification_database_url.as_deref())
                            .flatten()
                    })
                }
                [] => self.config.notification_database_url.as_deref(),
            };
            let listener = match listener_url {
                Some(database_url) => {
                    match crate::notify::QueueListener::connect(database_url, &self.config.queues)
                        .await
                    {
                        Ok(listener) => {
                            tracing::info!(
                                worker_id = %self.config.worker_id,
                                queues = ?listener.queues(),
                                "worker LISTEN/NOTIFY listener connected"
                            );
                            Some(listener)
                        }
                        Err(error) => {
                            tracing::warn!(
                                worker_id = %self.config.worker_id,
                                error = %error,
                                "failed to start LISTEN/NOTIFY listener; falling back to polling"
                            );
                            None
                        }
                    }
                }
                None => None,
            };
            self.run_with_listener(claim_pool, listener).await;
        }
    }

    /// Multi-shard entry point (issue #522).
    ///
    /// Called by `run` when two or more distinct shard pools are active. Each
    /// shard gets its own optional LISTEN/NOTIFY listener (from
    /// `shard_notification_database_urls`), and the poll loop calls
    /// `poll_once(shard_pool)` per shard so that per-shard ACID locality is
    /// preserved for free — `dispatch_task` already clones whatever pool it
    /// receives into the spawned task.
    ///
    /// Fleet presence (register + heartbeat + status) is written to **each**
    /// assigned shard's pool so that the shard-health readiness gate (Slice B)
    /// can see a covering live worker per shard.
    async fn run_multi_shard(
        &self,
        shard_targets: Vec<(crate::types::ShardId, DbPool)>,
        default_pool: &DbPool,
    ) {
        tracing::info!(
            worker_id = %self.config.worker_id,
            queues = ?self.config.queues,
            shards = ?shard_targets.iter().map(|(s, _)| s.as_i32()).collect::<Vec<_>>(),
            "worker starting (multi-shard)"
        );

        // Register + rate-limit buckets on every shard pool.
        for (_, shard_pool) in &shard_targets {
            self.register_in_fleet(shard_pool).await;
            self.register_rate_limit_buckets(shard_pool).await;
        }

        // Monitoring tasks use the default pool for non-shard-specific monitors;
        // the stranded-work sampler (added in spawn_monitoring_tasks) uses the
        // full ShardedDbPool so it observes every shard. The slot tuner's
        // pool-pressure signal is likewise given every shard's own pool
        // (issue #548 review) so pressure on a non-default shard — the pools
        // tasks are actually dispatched against in this multi-shard path —
        // can still trigger a shrink, instead of only ever observing
        // `default_pool`.
        let shard_pools_for_pressure: Vec<DbPool> =
            shard_targets.iter().map(|(_, p)| p.clone()).collect();
        let monitors = self.spawn_monitoring_tasks(default_pool, &shard_pools_for_pressure);
        let heartbeat_cancel = CancellationToken::new();

        // Spawn one heartbeat task per shard pool so every shard's harvest_workers
        // table reflects this worker. All shards' heartbeats share the same
        // live-target atomics from the single `spawn_monitoring_tasks` call
        // above, so every shard's row reports the same tuned in-flight view.
        let heartbeat_handles: Vec<_> = shard_targets
            .iter()
            .map(|(_, shard_pool)| {
                self.spawn_heartbeat_task(
                    shard_pool,
                    Arc::clone(&monitors.workflow_slot_target),
                    Arc::clone(&monitors.activity_slot_target),
                    heartbeat_cancel.clone(),
                )
            })
            .collect();

        // Build per-shard listeners from shard_notification_database_urls.
        let mut shard_listeners: Vec<Option<crate::notify::QueueListener>> = Vec::new();
        for (shard_id, _) in &shard_targets {
            let listener_url = self
                .config
                .shard_notification_database_urls
                .iter()
                .find(|(s, _)| s == shard_id)
                .map(|(_, url)| url.as_str());
            let listener = if let Some(url) = listener_url {
                match crate::notify::QueueListener::connect(url, &self.config.queues).await {
                    Ok(l) => {
                        tracing::info!(
                            worker_id = %self.config.worker_id,
                            shard_id = %shard_id.as_i32(),
                            "per-shard LISTEN/NOTIFY listener connected"
                        );
                        Some(l)
                    }
                    Err(error) => {
                        tracing::warn!(
                            worker_id = %self.config.worker_id,
                            shard_id = %shard_id.as_i32(),
                            error = %error,
                            "per-shard LISTEN/NOTIFY failed; shard will fall back to polling"
                        );
                        None
                    }
                }
            } else {
                None
            };
            shard_listeners.push(listener);
        }

        self.run_poll_loop_multi(shard_targets.clone(), shard_listeners)
            .await;

        tracing::info!(worker_id = %self.config.worker_id, "shutdown signal received (multi-shard)");

        // Draining: transition status on every shard pool.
        for (_, shard_pool) in &shard_targets {
            self.transition_fleet_status(shard_pool, crate::workers::WorkerStatus::Draining)
                .await;
        }

        tracing::info!(worker_id = %self.config.worker_id, "draining in-flight tasks (multi-shard)");
        self.drain_in_flight().await;

        // Stopped: mark every shard pool's worker row stopped, then cancel heartbeats.
        for (_, shard_pool) in &shard_targets {
            self.transition_fleet_status(shard_pool, crate::workers::WorkerStatus::Stopped)
                .await;
        }
        heartbeat_cancel.cancel();

        for handle in heartbeat_handles {
            if let Err(error) = handle.await {
                tracing::warn!(
                    worker_id = %self.config.worker_id,
                    error = %error,
                    "worker heartbeat task failed during multi-shard shutdown"
                );
            }
        }
        // Reuse the cleanup path for the monitoring tasks.
        self.shutdown_and_cleanup_monitors(monitors).await;

        tracing::info!(worker_id = %self.config.worker_id, "worker stopped (multi-shard)");
    }

    /// Multi-shard poll loop (issue #522).
    ///
    /// Each iteration tries every shard in order. The first shard that yields
    /// a task causes an immediate `continue` so we stay hot when there is work
    /// anywhere. When all shards are idle we `tokio::select!` across all
    /// per-shard listeners (falling back to a `poll_interval` sleep for shards
    /// without a listener) before the next full scan.
    async fn run_poll_loop_multi(
        &self,
        shard_targets: Vec<(crate::types::ShardId, DbPool)>,
        mut shard_listeners: Vec<Option<crate::notify::QueueListener>>,
    ) {
        let n = shard_targets.len();
        // Rotating start index prevents the first shard from being permanently
        // favoured when multiple shards have work (fix #4).
        let mut start_idx = 0usize;

        while !self.shutdown.is_cancelled() {
            let mut any_claimed = false;
            for i in 0..n {
                let idx = (start_idx + i) % n;
                if self.poll_once(&shard_targets[idx].1).await {
                    any_claimed = true;
                    // Advance start past the shard that just claimed so the
                    // next hot iteration tries the next shard first.
                    start_idx = (idx + 1) % n;
                    break;
                }
            }
            if any_claimed {
                continue;
            }
            // All idle — rotate start so the next iteration begins at the next
            // shard in round-robin order regardless of which fired a NOTIFY.
            start_idx = (start_idx + 1) % n;

            // All shards idle — poll all per-shard listeners in round-robin
            // with a short per-listener timeout (fix #6). Notifications are
            // buffered in each listener's channel so no wake-up is lost.
            // Sequential round-robin avoids borrow-checker issues with
            // select_all over &mut references while still letting any listener
            // wake the loop.
            let poll_interval = self.config.poll_interval;
            let shutdown = &self.shutdown;

            if shard_listeners.iter().all(Option::is_none) {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = tokio::time::sleep(poll_interval) => {}
                }
            } else {
                // Poll each listener with a short cap; overall timeout = poll_interval.
                let per_check = Duration::from_millis(10).min(poll_interval);
                let deadline = tokio::time::Instant::now() + poll_interval;
                let mut notified = false;

                'notify_wait: while tokio::time::Instant::now() < deadline
                    && !shutdown.is_cancelled()
                {
                    let mut broken_idx: Option<usize> = None;
                    for (i, slot) in shard_listeners.iter_mut().enumerate() {
                        if let Some(listener) = slot.as_mut() {
                            match listener.wait_for_notification(per_check).await {
                                Ok(Some(_)) => {
                                    notified = true;
                                    break 'notify_wait;
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    tracing::warn!(
                                        worker_id = %self.config.worker_id,
                                        shard_idx = i,
                                        error = %error,
                                        "LISTEN/NOTIFY wait failed for shard; removing listener"
                                    );
                                    broken_idx = Some(i);
                                    break;
                                }
                            }
                        }
                    }
                    if let Some(i) = broken_idx {
                        shard_listeners[i] = None;
                        // All listeners gone: nothing left to await in this loop,
                        // so bail out instead of busy-spinning until the deadline.
                        if shard_listeners.iter().all(Option::is_none) {
                            break 'notify_wait;
                        }
                    }
                }
                if notified {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Clean up monitoring task handles (extracted for the multi-shard path).
    async fn shutdown_and_cleanup_monitors(&self, monitors: WorkerMonitoringHandles) {
        for handle in monitors.timeout_checkers {
            if let Err(error) = handle.await {
                tracing::warn!(error = %error, "timeout checker failed during shutdown");
            }
        }
        for handle in monitors.poison_pill_reclaimers {
            if let Err(error) = handle.await {
                tracing::warn!(error = %error, "poison-pill reclaimer failed during shutdown");
            }
        }
        for handle in monitors.session_slot_reconcilers {
            if let Err(error) = handle.await {
                tracing::warn!(error = %error, "session slot reconciler failed during shutdown");
            }
        }
        for handle in monitors.pause_auto_resumers {
            if let Err(error) = handle.await {
                tracing::warn!(error = %error, "pause auto-resumer failed during shutdown");
            }
        }
        if let Err(error) = monitors.queue_depth_sampler.await {
            tracing::warn!(error = %error, "queue depth sampler failed during shutdown");
        }
        if let Err(error) = monitors.concurrency_sampler.await {
            tracing::warn!(error = %error, "concurrency sampler failed during shutdown");
        }
        if let Err(error) = monitors.rate_limit_sampler.await {
            tracing::warn!(error = %error, "rate limit sampler failed during shutdown");
        }
        for sampler in monitors.dlq_depth_samplers {
            if let Err(error) = sampler.await {
                tracing::warn!(error = %error, "dlq depth sampler failed during shutdown");
            }
        }
        if let Err(error) = monitors.history_oversized_sampler.await {
            tracing::warn!(error = %error, "history oversized sampler failed during shutdown");
        }
        if let Some(handle) = monitors.worker_slot_sampler
            && let Err(error) = handle.await
        {
            tracing::warn!(error = %error, "worker slot sampler failed during shutdown");
        }
        if let Some(handle) = monitors.stranded_work_sampler
            && let Err(error) = handle.await
        {
            tracing::warn!(error = %error, "stranded-work sampler failed during shutdown");
        }
        if let Some(handle) = monitors.schedule_overdue_sampler
            && let Err(error) = handle.await
        {
            tracing::warn!(error = %error, "schedule overdue sampler failed during shutdown");
        }
        for handle in monitors.slot_tuners {
            if let Err(error) = handle.await {
                tracing::warn!(error = %error, "slot tuner loop failed during shutdown");
            }
        }
    }

    /// Run the worker loop using a pre-connected optional listener.
    ///
    /// This lets callers separate listener startup from task polling when they
    /// need tighter control over startup sequencing.
    pub async fn run_with_listener(
        &self,
        pool: &DbPool,
        listener: Option<crate::notify::QueueListener>,
    ) {
        tracing::info!(
            worker_id = %self.config.worker_id,
            queues = ?self.config.queues,
            "worker starting"
        );

        // Register this worker in the fleet table.
        self.register_in_fleet(pool).await;

        // Auto-register rate limit buckets for the activities configured on this worker.
        self.register_rate_limit_buckets(pool).await;

        let monitors = self.spawn_monitoring_tasks(pool, std::slice::from_ref(pool));
        let heartbeat_cancel = CancellationToken::new();
        let heartbeat_handle = self.spawn_heartbeat_task(
            pool,
            Arc::clone(&monitors.workflow_slot_target),
            Arc::clone(&monitors.activity_slot_target),
            heartbeat_cancel.clone(),
        );

        self.run_poll_loop(pool, listener).await;

        tracing::info!(worker_id = %self.config.worker_id, "shutdown signal received");

        // Transition to Draining before waiting for in-flight tasks.
        self.transition_fleet_status(pool, crate::workers::WorkerStatus::Draining)
            .await;

        tracing::info!(worker_id = %self.config.worker_id, "draining in-flight tasks");
        self.drain_in_flight().await;

        // All tasks complete — mark Stopped, then stop the heartbeat task.
        self.transition_fleet_status(pool, crate::workers::WorkerStatus::Stopped)
            .await;
        heartbeat_cancel.cancel();

        if let Err(error) = heartbeat_handle.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "worker heartbeat task failed during shutdown"
            );
        }
        self.shutdown_and_cleanup_monitors(monitors).await;

        tracing::info!(worker_id = %self.config.worker_id, "worker stopped");
    }

    // significant_drop_tightening: `workflow_runtime`/`activity_runtime`
    // (each holding withheld `OwnedSemaphorePermit`s — a "significant drop"
    // type) are constructed, read via `live_target_cell()`, and then moved
    // into their `TunedSlot` a few statements later for the tuner-loop
    // spawn. Clippy's suggested fix — collapsing construction directly into
    // `.live_target_cell()` — would drop the runtime (and release its
    // withheld permits back into the semaphore) immediately, which is
    // exactly the bug this withholding scheme exists to prevent. The lint
    // is a false positive here: both bindings genuinely have two uses.
    // `pressure_pools` (issue #548 review): the slot tuner's pool-pressure
    // signal must observe every pool this worker actually dispatches tasks
    // against, not just `pool`. For the single-shard path `pool` is the
    // only pool, so callers pass a one-element slice containing it; for
    // `run_multi_shard`, callers pass every shard's own pool so pressure on
    // any shard (not only the "default" one) can trigger a shrink. Every
    // other sampler in this function is unaffected and keeps using `pool`
    // alone, matching the pre-existing single-default-pool pattern
    // documented below.
    #[allow(clippy::too_many_lines, clippy::significant_drop_tightening)]
    fn spawn_monitoring_tasks(
        &self,
        pool: &DbPool,
        pressure_pools: &[DbPool],
    ) -> WorkerMonitoringHandles {
        // Pools the queue-depth/age, concurrency, rate-limit, and history-
        // oversized samplers aggregate over (issue #522 review). When a
        // ShardedDbPool is configured, sample every shard so these fleet-wide
        // gauges reflect non-default-shard backlog/throttling rather than only
        // the default shard; otherwise use the single default pool (legacy,
        // byte-for-byte unchanged). All workers aggregate the same full set, so
        // the gauges stay consistent across the fleet.
        #[cfg(feature = "db")]
        let sampler_pools: Vec<DbPool> = self.config.sharded_pool.as_ref().map_or_else(
            || vec![pool.clone()],
            |sp| sp.iter_shards().map(|(_, p)| p.clone()).collect(),
        );
        #[cfg(not(feature = "db"))]
        let sampler_pools: Vec<DbPool> = vec![pool.clone()];

        let queue_depth_sampler = spawn_queue_depth_sampler(
            sampler_pools.clone(),
            self.shutdown.clone(),
            self.registry.telemetry().clone(),
            self.config.queues.clone(),
            self.config.poll_interval,
            self.registry
                .circuit_breakers()
                .tracked_activity_names()
                .to_vec(),
        );
        let concurrency_sampler = spawn_concurrency_sampler(
            sampler_pools.clone(),
            self.shutdown.clone(),
            self.registry.telemetry().clone(),
            self.config.queues.clone(),
            self.config.poll_interval,
        );
        // DLQ depth gauge — one sampler per shard assignment so every shard
        // this worker owns is reported.  Single-shard deployments get one
        // sampler for shard 0; multi-shard workers (rare) get one per entry.
        // Each sampler uses the per-shard pool when a ShardedDbPool is available
        // so it queries the correct database (fix #5).
        let dlq_depth_samplers: Vec<_> = {
            let assignments = &self.config.shard_assignments;
            let shards: &[_] = if assignments.is_empty() {
                &[]
            } else {
                assignments.as_slice()
            };
            let mut handles: Vec<_> = shards
                .iter()
                .map(|shard| {
                    let shard_id = u16::try_from(shard.as_i32()).unwrap_or(0);
                    #[cfg(feature = "db")]
                    let shard_pool = self
                        .config
                        .sharded_pool
                        .as_ref()
                        .map_or_else(|| pool.clone(), |sp| sp.pool_for(*shard).clone());
                    #[cfg(not(feature = "db"))]
                    let shard_pool = pool.clone();
                    spawn_dlq_depth_sampler(
                        shard_pool,
                        self.shutdown.clone(),
                        self.registry.telemetry().clone(),
                        shard_id,
                        self.config.poll_interval,
                    )
                })
                .collect();
            if handles.is_empty() {
                handles.push(spawn_dlq_depth_sampler(
                    pool.clone(),
                    self.shutdown.clone(),
                    self.registry.telemetry().clone(),
                    0u16,
                    self.config.poll_interval,
                ));
            }
            handles
        };
        let rate_limit_sampler = spawn_rate_limit_sampler(
            sampler_pools.clone(),
            self.shutdown.clone(),
            self.registry.telemetry().clone(),
            self.config.poll_interval,
        );
        // Poison-pill reclaimer, pause auto-resumer, and timeout checker all run
        // per-shard so that orphaned tasks, over-long pauses, and timed-out
        // tasks/executions on every assigned shard are recovered (fix #3,
        // issue #522). When a ShardedDbPool is available each shard gets its own
        // instance; otherwise the single default pool is used.
        #[cfg(feature = "db")]
        let shard_pools_for_monitors: Vec<DbPool> = {
            let assignments = &self.config.shard_assignments;
            match (assignments.is_empty(), self.config.sharded_pool.as_ref()) {
                (false, Some(sp)) => assignments
                    .iter()
                    .map(|s| sp.pool_for(*s).clone())
                    .collect(),
                _ => vec![pool.clone()],
            }
        };
        #[cfg(not(feature = "db"))]
        let shard_pools_for_monitors: Vec<DbPool> = vec![pool.clone()];

        // Worker-stale threshold mirrors the fleet-health classifier:
        // 2 × heartbeat interval, with a 1 s floor for sub-second intervals.
        // Double *before* rounding to whole seconds so a fractional interval
        // (e.g. 1500ms → 3s, not 2s) keeps the documented liveness window, and
        // cap at one year so an absurd interval can never overflow the
        // chrono::Duration arithmetic in the reclaim path. Shared by the
        // poison-pill reclaimer and the worker-session broken-session scanner
        // (issue #606, folded into `enforce_timeouts_once`) -- both use the
        // exact same "is this worker's heartbeat too old" liveness signal.
        let doubled = self.config.worker_heartbeat_interval.saturating_mul(2);
        let worker_stale_secs = i64::try_from(doubled.as_secs())
            .unwrap_or(crate::poison_pill::MAX_WORKER_STALE_SECS)
            .saturating_add(i64::from(doubled.subsec_nanos() > 0))
            .clamp(1, crate::poison_pill::MAX_WORKER_STALE_SECS);

        // One timeout checker per assigned shard. `enforce_timeouts_once` scans
        // the connection's *own* database (find_timed_out_tasks, external-task
        // timeouts, workflow-execution deadlines, SLA breaches, history
        // ceiling, broken worker sessions), so a single checker on the default
        // pool would leave expired tasks/executions on the other shards stuck
        // RUNNING/PENDING forever. The cross-shard outbox helpers inside each
        // pass still receive the full sharded_pool + shard_assignments so
        // peer-shard delivery is unchanged.
        let timeout_checkers: Vec<_> = shard_pools_for_monitors
            .iter()
            .map(|shard_pool| {
                crate::timeout::spawn_timeout_checker(
                    shard_pool.clone(),
                    self.shutdown.clone(),
                    self.config.poll_interval,
                    self.registry.telemetry().clone(),
                    self.config.unknown_target_grace_window,
                    self.config.sharded_pool.clone(),
                    self.config.shard_assignments.clone(),
                    self.registry.circuit_breakers(),
                    self.config.max_workflow_history_events,
                    worker_stale_secs,
                )
            })
            .collect();

        let poison_pill_reclaimers: Vec<_> = shard_pools_for_monitors
            .iter()
            .map(|shard_pool| {
                crate::poison_pill::spawn_poison_pill_reclaimer(
                    shard_pool.clone(),
                    self.shutdown.clone(),
                    self.config.worker_heartbeat_interval,
                    self.config.poison_pill_threshold,
                    worker_stale_secs,
                    self.registry.telemetry().clone(),
                )
            })
            .collect();
        // Worker-session local-registry reconciler (issue #606): a session's
        // `harvest_sessions` row may live on any shard this worker touches
        // (session identity doesn't self-encode a shard the way ExecutionId
        // does), so — mirroring the poison-pill reclaimer above — one
        // reconciler per assigned shard pool checks this worker's *entire*
        // local registry against that shard's rows; a session hosted on a
        // different shard simply matches zero rows there and is a no-op for
        // that pool. Reuses the heartbeat cadence, matching the other
        // session-adjacent scanners (the broken-session scanner shares this
        // same interval via `enforce_timeouts_once`).
        let session_slot_reconcilers: Vec<_> = shard_pools_for_monitors
            .iter()
            .map(|shard_pool| {
                crate::sessions::spawn_session_slot_reconciler(
                    shard_pool.clone(),
                    Arc::clone(&self.session_slots_in_use),
                    self.shutdown.clone(),
                    self.config.worker_heartbeat_interval,
                )
            })
            .collect();
        let pause_auto_resumers: Vec<_> = shard_pools_for_monitors
            .iter()
            .map(|shard_pool| {
                spawn_pause_auto_resumer(
                    shard_pool.clone(),
                    self.shutdown.clone(),
                    self.config.worker_heartbeat_interval,
                    self.config.max_workflow_pause_duration,
                    self.registry.telemetry().clone(),
                )
            })
            .collect();
        let history_oversized_sampler = spawn_history_oversized_sampler(
            sampler_pools.clone(),
            self.shutdown.clone(),
            self.registry.telemetry().clone(),
            self.registry.history_policy().continue_as_new_threshold(),
            self.config.poll_interval,
        );
        // Adaptive slot-tuner control loop (issue #548): only spawned when
        // `WorkerConfig::with_slot_tuner` was configured. Unlike the sampler
        // below, this is NOT gated on `metrics.is_enabled()` — it is a
        // controller with a real effect on dispatch capacity, not a pure
        // observability sampler, so it must keep running even when no
        // metrics recorder is installed. Only the per-tick telemetry
        // emission inside the loop is gated.
        //
        // Both slot types are driven by ONE shared control-loop task (see
        // `crate::slot_tuner::spawn_slot_tuner_loop`): pool-pressure is a
        // worker-wide signal (both semaphores dispatch against the same
        // connection pool), so sampling it once per tick and reusing it for
        // both decisions avoids doubling the pool status-lock contention
        // that two independent per-type loops would cause.
        //
        // `TunedSlotRuntime` (which owns `OwnedSemaphorePermit`s) is
        // constructed here — a local, not a `Worker` field — and immediately
        // handed to the spawned tuner-loop task. `spawn_monitoring_tasks`
        // always runs before the worker's poll loop starts (see
        // `run_with_listener` / the multi-shard `run`), so no task can ever
        // be dispatched against the semaphore before it is withheld down to
        // the initial target here.
        //
        // `monitoring_started` guards specifically against a second
        // invocation of this function on the same `Worker` (e.g. a
        // hypothetical future caller wrapping `run`/`run_with_listener` in a
        // retry loop): constructing a second `TunedSlotRuntime` over the
        // same semaphore would race the first tuner-loop task over the same
        // withheld permits. No production call site does this today, but
        // the guard converts a silent state race into a loud, diagnosable
        // log line instead. It does not gate the harmless (if redundant)
        // samplers spawned elsewhere in this function.
        let already_monitoring = self
            .monitoring_started
            .swap(true, std::sync::atomic::Ordering::SeqCst);
        if already_monitoring && self.config.slot_tuner.is_some() {
            tracing::error!(
                worker_id = %self.config.worker_id,
                "spawn_monitoring_tasks called more than once on the same Worker; skipping \
                 slot-tuner re-initialization to avoid racing a second TunedSlotRuntime over \
                 the same dispatch semaphore (this should never happen in normal operation)"
            );
        }
        let mut slot_tuners: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let (workflow_slot_target, activity_slot_target) =
            if !already_monitoring && let Some(tuner_cfg) = self.config.slot_tuner.clone() {
                let workflow_runtime = crate::slot_tuner::TunedSlotRuntime::new(
                    Arc::clone(&self.workflow_semaphore),
                    self.config.max_concurrent_workflows,
                    tuner_cfg.min_slots,
                    tuner_cfg.max_slots,
                );
                let activity_runtime = crate::slot_tuner::TunedSlotRuntime::new(
                    Arc::clone(&self.activity_semaphore),
                    self.config.max_concurrent_activities,
                    tuner_cfg.min_slots,
                    tuner_cfg.max_slots,
                );
                let workflow_slot_target = workflow_runtime.live_target_cell();
                let activity_slot_target = activity_runtime.live_target_cell();
                let workflow_permit_wait = self
                    .workflow_permit_wait_micros
                    .clone()
                    .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
                let activity_permit_wait = self
                    .activity_permit_wait_micros
                    .clone()
                    .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
                // Owned so the closure can move it: sample every pool this
                // worker actually dispatches against (issue #548 review), not
                // just `pool`, so pressure on any one shard's pool can still
                // trigger a shrink for a multi-shard worker.
                let pools_for_pressure: Vec<DbPool> = if pressure_pools.is_empty() {
                    vec![pool.clone()]
                } else {
                    pressure_pools.to_vec()
                };
                slot_tuners.push(crate::slot_tuner::spawn_slot_tuner_loop(
                    crate::slot_tuner::TunedSlot {
                        runtime: workflow_runtime,
                        permit_wait_micros: workflow_permit_wait,
                        slot_type: SlotType::Workflow,
                    },
                    crate::slot_tuner::TunedSlot {
                        runtime: activity_runtime,
                        permit_wait_micros: activity_permit_wait,
                        slot_type: SlotType::Activity,
                    },
                    Arc::clone(&tuner_cfg.tuner),
                    move || {
                        let readings: Vec<crate::slot_tuner::PoolPressure> = pools_for_pressure
                            .iter()
                            .map(|p| {
                                let status = p.status();
                                crate::slot_tuner::PoolPressure {
                                    max_size: status.max_size,
                                    size: status.size,
                                    available: status.available,
                                    waiting: status.waiting,
                                }
                            })
                            .collect();
                        crate::slot_tuner::worst_pool_pressure(&readings)
                    },
                    self.shutdown.clone(),
                    self.config.poll_interval,
                    self.registry.telemetry().clone(),
                ));
                (workflow_slot_target, activity_slot_target)
            } else {
                (
                    Arc::new(AtomicUsize::new(self.workflow_permit_total)),
                    Arc::new(AtomicUsize::new(self.activity_permit_total)),
                )
            };

        // Worker slot-occupancy gauges (issue #531): a pure in-memory read of the
        // two dispatch semaphores against their configured maxima, on the same
        // cadence as the other samplers. Only started when metrics are enabled,
        // matching the stranded_work_sampler opt-in pattern.
        let worker_slot_sampler = self.registry.telemetry().metrics.is_enabled().then(|| {
            spawn_worker_slot_sampler(
                Arc::clone(&self.workflow_semaphore),
                Arc::clone(&workflow_slot_target),
                Arc::clone(&self.activity_semaphore),
                Arc::clone(&activity_slot_target),
                self.shutdown.clone(),
                self.registry.telemetry().clone(),
                self.config.poll_interval,
            )
        });

        // Stranded-work sampler (issue #522): emits a gauge per shard showing
        // how many claimable tasks have no live covering worker. Iterates ALL
        // shards visible through the pool (not just assigned shards) so an
        // uncovered writable shard is caught regardless of which worker runs
        // this sampler. Only started when a sharded pool is available and
        // metrics are enabled.
        #[cfg(feature = "db")]
        let stranded_work_sampler = self
            .config
            .sharded_pool
            .as_ref()
            .filter(|_| self.registry.telemetry().metrics.is_enabled())
            .map(|sp| {
                spawn_stranded_work_sampler(
                    sp.clone(),
                    // Reuse the heartbeat interval as freshness window so the
                    // worker-liveness check is consistent with the heartbeat.
                    self.config.worker_heartbeat_interval.saturating_mul(2),
                    self.shutdown.clone(),
                    self.registry.telemetry().clone(),
                    self.config.poll_interval,
                    self.registry
                        .circuit_breakers()
                        .tracked_activity_names()
                        .to_vec(),
                    self.registry.activity_requirements_json(),
                )
            });
        #[cfg(not(feature = "db"))]
        let stranded_work_sampler: Option<tokio::task::JoinHandle<()>> = None;

        // Overdue-schedule gauge sampler (issue #696): emits
        // `harvest.schedule.overdue` per schedule across every shard pool so a
        // stalled cron is detected within one cadence grace window. Runs on the
        // worker (not the scheduler tick) and no-ops internally when metrics are
        // disabled. Uses `sampler_pools` so single-shard and multi-shard
        // deployments both aggregate the full schedule set. The interval is
        // adaptive (Codex round 4): a coarse 30s ceiling for a slow/Manual fleet,
        // adapting down toward the fastest active cadence (never below
        // `poll_interval`) so a sub-30s schedule is still detected within its
        // grace window.
        #[cfg(feature = "db")]
        let schedule_overdue_sampler = Some(spawn_schedule_overdue_sampler(
            sampler_pools,
            self.shutdown.clone(),
            self.registry.telemetry().clone(),
            self.config.poll_interval,
        ));
        #[cfg(not(feature = "db"))]
        let schedule_overdue_sampler: Option<tokio::task::JoinHandle<()>> = None;

        WorkerMonitoringHandles {
            queue_depth_sampler,
            concurrency_sampler,
            rate_limit_sampler,
            dlq_depth_samplers,
            timeout_checkers,
            poison_pill_reclaimers,
            pause_auto_resumers,
            session_slot_reconcilers,
            history_oversized_sampler,
            worker_slot_sampler,
            stranded_work_sampler,
            schedule_overdue_sampler,
            slot_tuners,
            workflow_slot_target,
            activity_slot_target,
        }
    }

    // `workflow_slot_target`/`activity_slot_target` come from
    // `spawn_monitoring_tasks`'s `WorkerMonitoringHandles` (issue #548
    // review): the heartbeat's reported in-flight count must track the
    // dispatch semaphores' *current* tuned target, not the static
    // `max_concurrent_*` config value, since a tuner can resize away from
    // that value at any time. Untuned workers pass an atomic fixed at the
    // configured max, so `compute_in_flight`'s accounting is byte-identical
    // to before this field existed.
    fn spawn_heartbeat_task(
        &self,
        pool: &DbPool,
        workflow_slot_target: Arc<AtomicUsize>,
        activity_slot_target: Arc<AtomicUsize>,
        heartbeat_cancel: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        // Spawn the heartbeat background task with a dedicated cancel token so
        // that liveness updates continue during the Draining phase and only stop
        // after the Stopped transition is written.
        let shard_ids: Vec<i32> = self
            .config
            .shard_assignments
            .iter()
            .map(|s| s.as_i32())
            .collect();
        // See the matching comment in `register_in_fleet`: advertises the
        // tuned ceiling so it can never diverge from the tuned
        // in_flight_count this heartbeat reports.
        let max_concurrency =
            i32::try_from(self.workflow_permit_total + self.activity_permit_total)
                .unwrap_or(i32::MAX);
        crate::workers::spawn_worker_heartbeat(
            pool.clone(),
            crate::workers::WorkerRegistration {
                worker_id: self.config.worker_id.clone(),
                queues: self.config.queues.clone(),
                shard_assignments: shard_ids,
                max_concurrency,
                host: crate::workers::local_hostname(),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
                build_id: self.config.build_id.clone(),
                deployment_name: self.config.deployment_name.clone(),
                labels: self.config.labels.clone(),
                max_concurrent_sessions: self.config.max_concurrent_sessions,
            },
            Arc::clone(&self.workflow_semaphore),
            workflow_slot_target,
            Arc::clone(&self.activity_semaphore),
            activity_slot_target,
            self.config.worker_heartbeat_interval,
            heartbeat_cancel,
            self.shutdown.clone(),
            Arc::clone(&self.remote_drain_deadline),
            Arc::clone(&self.drain_deadline_max),
            Arc::clone(&self.session_slots_in_use),
        )
    }

    async fn run_poll_loop(
        &self,
        pool: &DbPool,
        mut listener: Option<crate::notify::QueueListener>,
    ) {
        while !self.shutdown.is_cancelled() {
            if self.poll_once(pool).await {
                continue;
            }

            if let Some(listener) = listener.as_mut() {
                match listener
                    .wait_for_notification(self.config.poll_interval)
                    .await
                {
                    Ok(Some(_)) => {
                        // Host-side timestamps can be slightly ahead of Postgres NOW(),
                        // so give newly notified tasks a brief moment to become claimable.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(
                            worker_id = %self.config.worker_id,
                            error = %error,
                            "LISTEN/NOTIFY wait failed; sleeping before retry"
                        );
                        tokio::time::sleep(self.config.poll_interval).await;
                    }
                }
            } else {
                tokio::time::sleep(self.config.poll_interval).await;
            }
        }
    }

    // A long, flat sequence of "drain this monitor handle and warn on
    // failure" blocks, one per `WorkerMonitoringHandles` field -- inherently
    // repetitive rather than complex; splitting it up would just move the
    // line count into a helper with an equally uninteresting signature.
    #[allow(clippy::too_many_lines)]
    /// Register or re-register this worker in the fleet table.
    async fn register_in_fleet(&self, pool: &DbPool) {
        let shard_ids: Vec<i32> = self
            .config
            .shard_assignments
            .iter()
            .map(|s| s.as_i32())
            .collect();
        // Advertises the tuned ceiling (max_slots), not the static config
        // value, so it never diverges from the tuned in_flight_count the
        // heartbeat reports (issue #548 review) — byte-identical to before
        // when no tuner is configured, since these fields equal the static
        // config values in that case.
        let max_concurrency =
            i32::try_from(self.workflow_permit_total + self.activity_permit_total)
                .unwrap_or(i32::MAX);
        let host = crate::workers::local_hostname();
        let version = env!("CARGO_PKG_VERSION");

        match pool.get().await {
            Ok(mut conn) => {
                if let Err(error) = crate::workers::register_worker(
                    &mut conn,
                    &self.config.worker_id,
                    &self.config.queues,
                    &shard_ids,
                    max_concurrency,
                    &host,
                    Some(version),
                    &self.config.build_id,
                    self.config.deployment_name.as_deref(),
                    &self.config.labels,
                    self.config.max_concurrent_sessions,
                )
                .await
                {
                    tracing::warn!(
                        worker_id = %self.config.worker_id,
                        error = %error,
                        "failed to register worker in fleet table; continuing without fleet registration"
                    );
                } else {
                    tracing::info!(
                        worker_id = %self.config.worker_id,
                        host = %host,
                        max_concurrency,
                        "worker registered in fleet"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    worker_id = %self.config.worker_id,
                    error = %error,
                    "failed to get pool connection for fleet registration"
                );
            }
        }
    }

    /// Auto-upsert rate-limiting buckets for activities registered on this worker.
    async fn register_rate_limit_buckets(&self, pool: &DbPool) {
        match pool.get().await {
            Ok(mut conn) => {
                for activity in self.registry.activities.values() {
                    // A dynamic rate_limit(key = ...) with no rps is an invalid
                    // declaration that `HarvestBuilder::try_build` rejects. A
                    // worker built via a direct `HandlerRegistry` bypasses that
                    // validation; surface it loudly at startup (the enqueue path
                    // also fails the schedule transaction -- see
                    // `persist_scheduled_activities`) rather than silently running
                    // the activity unrated (issue #699 review, Codex P2).
                    if activity.rate_limit_key_expr.is_some() && activity.rate_limit_rps.is_none() {
                        tracing::error!(
                            worker_id = %self.config.worker_id,
                            activity = %activity.name,
                            "activity declares a dynamic rate_limit(key = ...) without \
                             rate_limit_rps; it will fail at schedule time -- add an rps \
                             or remove the key"
                        );
                        continue;
                    }
                    let Some(refill_rate) = activity.rate_limit_rps else {
                        continue;
                    };
                    // Dynamic per-key limits (issue #699) register their buckets
                    // lazily at enqueue time (one per resolved tenant key), so
                    // there is no single static bucket to pre-register here.
                    if activity.rate_limit_key_expr.is_some() {
                        continue;
                    }
                    // A static `rate_limit_key` beginning with the reserved
                    // `dyn-rate:` prefix reaching a worker via a direct
                    // `HandlerRegistry` (bypassing the macro reject and
                    // `HarvestBuilder::try_build`) would collide with the
                    // generated dynamic per-key buckets; since both this
                    // registration and the lazy enqueue registration use
                    // `ON CONFLICT DO NOTHING`, the bucket's rate/burst would
                    // become insertion-order dependent. Skip it loudly here,
                    // mirroring the dynamic-no-rps guard above (issue #699
                    // review, Codex P2). The enqueue path also fails the
                    // schedule transaction (see `persist_scheduled_activities`).
                    if let Some(static_key) = activity.rate_limit_key
                        && static_key.starts_with("dyn-rate:")
                    {
                        tracing::error!(
                            worker_id = %self.config.worker_id,
                            activity = %activity.name,
                            key = %static_key,
                            "activity sets a static rate_limit_key beginning with the reserved \
                             `dyn-rate:` prefix; not registering this colliding bucket -- rename \
                             the key or use rate_limit(key = ...) for dynamic per-key buckets"
                        );
                        continue;
                    }
                    let burst = activity.rate_limit_burst.unwrap_or(refill_rate);
                    let key = activity.rate_limit_key.unwrap_or(activity.name);

                    // Insert rate limit bucket if it doesn't already exist.
                    // This preserves operator overrides.
                    if let Err(error) =
                        queue::ensure_rate_limit_bucket(&mut conn, key, refill_rate, burst).await
                    {
                        tracing::warn!(
                            worker_id = %self.config.worker_id,
                            key = %key,
                            error = %error,
                            "failed to auto-register rate limit bucket; continuing"
                        );
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    worker_id = %self.config.worker_id,
                    error = %error,
                    "failed to acquire connection for rate limit bucket registration"
                );
            }
        }
    }

    /// Transition this worker's status in the fleet table.
    async fn transition_fleet_status(&self, pool: &DbPool, status: crate::workers::WorkerStatus) {
        match pool.get().await {
            Ok(mut conn) => {
                if let Err(error) =
                    crate::workers::transition_status(&mut conn, &self.config.worker_id, status)
                        .await
                {
                    tracing::warn!(
                        worker_id = %self.config.worker_id,
                        ?status,
                        error = %error,
                        "failed to update worker fleet status"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    worker_id = %self.config.worker_id,
                    error = %error,
                    "failed to get pool connection for fleet status update"
                );
            }
        }
    }

    /// Emit rate-limit throttle metrics for all bound queues.
    ///
    /// Shared between the weighted and unweighted poll paths so that the
    /// throttle-recording logic only lives in one place.
    async fn emit_throttle_metrics(&self, conn: &mut AsyncPgConnection) {
        // `check_throttled_keys` returns bounded activity names (not raw bucket
        // keys), so labelling the throttle counter with them is cardinality-safe
        // for dynamic per-key limits (issue #699 / ADR-0001 §7).
        if let Ok(throttled_activities) =
            queue::check_throttled_keys(conn, &self.config.queues).await
        {
            for activity in throttled_activities {
                self.registry
                    .telemetry()
                    .metrics
                    .record_rate_limit_throttled(&activity);
            }
        }
    }

    /// Execute a single poll iteration.
    ///
    /// Gets a connection from the pool, tries to claim a task, dispatches it
    /// if found, or sleeps for `poll_interval` if the queue was empty.
    #[allow(clippy::too_many_lines)]
    async fn poll_once(&self, pool: &DbPool) -> bool {
        let mut conn = match pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!(error = %e, "failed to get connection from pool");
                return false;
            }
        };

        // Activities with a circuit breaker skip the claim-time rate-limit gate
        // and token debit entirely (issue #369): their rate limiting is enforced
        // authoritatively at dispatch in `process_activity_task`, gated on the
        // real `on_dispatch` decision, so a `CircuitOpen` short-circuit is claimed
        // and fast-failed at full speed during an outage while a genuine call
        // still atomically reserves a token. The set is static.
        let circuit_breakers = self.registry.circuit_breakers();
        let circuit_breaker_activities = circuit_breakers.tracked_activity_names();

        // --- Weighted queue selection (issue #515) ---
        //
        // When the operator has configured per-queue weights we compute a
        // weighted-random permutation of the bound queues and attempt a
        // single-queue `claim_task` call for each queue in that order,
        // dispatching the first task found.  This ensures dispatch share tracks
        // configured weights under sustained saturation while guaranteeing
        // forward progress for every non-zero-weight queue (no-starvation).
        //
        // When no weights are configured (the default) we fall through to the
        // original single `ANY($2)` claim call — byte-for-byte unchanged.
        if !self.config.queue_weights.is_empty() {
            let pairs = crate::queue_fairness::effective_queue_weights(
                &self.config.queues,
                &self.config.queue_weights,
            );
            let ordered =
                crate::queue_fairness::weighted_queue_order(&pairs, &mut rand::thread_rng());

            for queue_name in &ordered {
                let single_queue = std::slice::from_ref(queue_name);
                match queue::claim_task(
                    &mut conn,
                    single_queue,
                    &self.config.worker_id,
                    &self.config.build_id,
                    self.config.priority_aging_secs,
                    circuit_breaker_activities,
                    &self.ineligible_activities,
                )
                .await
                {
                    Ok(Some(task)) => {
                        tracing::debug!(
                            task_id = %task.id,
                            task_type = %task.task_type,
                            queue = %task.queue_name,
                            "claimed task (weighted)"
                        );
                        self.dispatch_task(task, pool);
                        return true;
                    }
                    Ok(None) => {
                        // Nothing in this queue; try the next in the permutation.
                    }
                    Err(e) => {
                        // Log the error and continue to the next queue in the
                        // permutation. A transient DB error on one queue must
                        // not starve the remaining queues in the ordered list
                        // — aborting the whole loop here would break the
                        // no-starvation guarantee stated in queue_fairness.rs.
                        tracing::warn!(error = %e, "failed to claim task from queue; trying next in permutation");
                    }
                }
            }

            // All queues tried — emit throttle metrics and report idle.
            self.emit_throttle_metrics(&mut conn).await;
            return false;
        }

        // --- Default (unweighted) path: original single ANY($2) query ---
        match queue::claim_task(
            &mut conn,
            &self.config.queues,
            &self.config.worker_id,
            &self.config.build_id,
            self.config.priority_aging_secs,
            circuit_breaker_activities,
            &self.ineligible_activities,
        )
        .await
        {
            Ok(Some(task)) => {
                tracing::debug!(
                    task_id = %task.id,
                    task_type = %task.task_type,
                    queue = %task.queue_name,
                    "claimed task"
                );
                // schedule-to-start latency is recorded once the handler
                // genuinely begins (in `process_workflow_task` /
                // `process_activity_task`, past the dispatch-time defer/no-op
                // gates), not here at claim time: the worker can over-claim past
                // `max_concurrent_*` (the semaphore gates execution, not
                // claiming), so measuring at claim would hide the time a task
                // spends waiting behind a local permit on a saturated worker —
                // exactly the capacity bottleneck the SLI is meant to page on.
                // `schedule_to_start_secs` measures from task eligibility, so that
                // permit wait is still captured in the recorded sample.
                self.dispatch_task(task, pool);
                true
            }
            Ok(None) => {
                self.emit_throttle_metrics(&mut conn).await;
                false
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to claim task");
                false
            }
        }
    }

    /// Spawn a bounded Tokio task for the claimed work item.
    #[allow(clippy::too_many_lines)]
    fn dispatch_task(&self, task: TaskQueueItem, pool: &DbPool) {
        // Debug-only tripwire (issue #548 review): dispatch must never race
        // ahead of `spawn_monitoring_tasks`, which withholds a tuned
        // semaphore's permits down to the operator's initial target. A
        // dispatch that lands before that withholding runs would see the
        // full un-withheld `max_slots` capacity instead. Both public entry
        // points (`run`, `run_with_listener`) already call
        // `spawn_monitoring_tasks` unconditionally before dispatch can
        // start, so this should never fire in practice — it exists to catch
        // a future ordering regression loudly in tests/CI rather than
        // silently over-provisioning capacity in production. Zero cost in
        // release builds.
        debug_assert!(
            std::sync::atomic::AtomicBool::load(
                &self.monitoring_started,
                std::sync::atomic::Ordering::SeqCst
            ),
            "dispatch_task called before spawn_monitoring_tasks initialized the worker; \
             a tuned semaphore would not yet be withheld to its initial target"
        );
        let kind = match ClaimedTaskKind::from_db(&task.task_type) {
            Ok(kind) => kind,
            Err(error) => {
                tracing::error!(
                    task_id = %task.id,
                    task_type = %task.task_type,
                    error = %error,
                    "claimed task has invalid task_type"
                );
                return;
            }
        };
        let semaphore = match kind {
            ClaimedTaskKind::Workflow => Arc::clone(&self.workflow_semaphore),
            ClaimedTaskKind::Activity => Arc::clone(&self.activity_semaphore),
        };
        // Only populated when a slot tuner is configured (issue #548); the
        // hot dispatch path performs no extra work otherwise.
        let permit_wait_micros = match kind {
            ClaimedTaskKind::Workflow => self.workflow_permit_wait_micros.clone(),
            ClaimedTaskKind::Activity => self.activity_permit_wait_micros.clone(),
        };
        let session_slots_in_use = Arc::clone(&self.session_slots_in_use);
        let max_concurrent_sessions = self.config.max_concurrent_sessions;

        // Per-queue dispatch counter for live split observability (issue #515).
        self.registry
            .telemetry()
            .metrics
            .record_task_dispatched(&task.queue_name);

        let pool = pool.clone();
        let registry = Arc::clone(&self.registry);
        let task_id = task.id;
        let task_type = task.task_type.clone();
        let worker_id = self.config.worker_id.clone();
        let build_id = self.config.build_id.clone();
        let cancellation_grace_period = self.config.cancellation_grace_period;
        let sticky_timeout = self.config.sticky_timeout;
        let max_local_activity_start_to_close = self.config.max_local_activity_start_to_close;
        let workflow_cache = Arc::clone(&self.workflow_cache);

        // Workflow-task timeout (issue #494): only apply to workflow tasks.
        // Local activities execute inline inside the workflow task, so the
        // effective budget must be at least as large as
        // max_local_activity_start_to_close to avoid prematurely killing a
        // workflow that is legitimately waiting for an in-progress local
        // activity.  When workflow_task_timeout is 0 the feature is disabled.
        let workflow_task_timeout = match kind {
            ClaimedTaskKind::Workflow => {
                if self.config.workflow_task_timeout.is_zero() {
                    Duration::ZERO
                } else {
                    self.config
                        .workflow_task_timeout
                        .max(self.config.max_local_activity_start_to_close)
                }
            }
            ClaimedTaskKind::Activity => Duration::ZERO,
        };
        let poison_pill_threshold = self.config.poison_pill_threshold;
        let timeout_strikes = Arc::clone(&self.workflow_task_timeout_strikes);
        // Issue #782: contained-handler-panic retry budget + strike map.
        let panic_strikes = Arc::clone(&self.workflow_panic_strikes);
        let workflow_panic_max_attempts = self.config.workflow_panic_max_attempts;
        let exec_id_for_timeout = task.workflow_exec_id;
        let telemetry = Arc::clone(&self.registry);

        // Monotonic instant captured the moment this worker received the claimed
        // task, before acquiring the local concurrency permit. The schedule-to-start
        // sample combines the DB-clock queue wait (claim time − eligibility, both
        // Postgres timestamps) with this host-monotonic local wait
        // (`dispatched_at.elapsed()` = permit wait + setup), so a host/Postgres
        // clock skew never leaks into the sample (issue #501 review).
        let dispatched_at = std::time::Instant::now();

        tokio::spawn(async move {
            // Acquire semaphore permit — blocks if at concurrency limit.
            let Ok(permit) = semaphore.acquire().await else {
                tracing::error!(task_id = %task_id, "semaphore closed");
                return;
            };

            // Feed the adaptive slot tuner's permit-wait signal (issue #548).
            // A lock-free fetch_max so concurrent dispatches never contend;
            // the tuner loop consumes (and resets) this via `swap(0, ..)`
            // once per tick, off this hot path entirely.
            if let Some(acc) = &permit_wait_micros {
                let wait_micros =
                    u64::try_from(dispatched_at.elapsed().as_micros()).unwrap_or(u64::MAX);
                acc.fetch_max(wait_micros, Ordering::Relaxed);
            }

            // schedule-to-start latency is recorded inside `process_workflow_task`
            // / `process_activity_task` at the point the handler genuinely begins
            // — *after* the dispatch-time defer/no-op gates (rate-limit defer,
            // claimed-then-paused re-park). Recording it here, before those gates,
            // would count a rate-limit deferral as a started task and depress the
            // p99 capacity SLI during throttling (issue #501). The sample still
            // captures the local-permit wait above via `dispatched_at`.

            tracing::info!(
                task_id = %task_id,
                task_type = %task_type,
                worker_id = %worker_id,
                "executing task"
            );

            // Apply per-workflow-task wall-clock budget when configured and
            // this is a workflow task with a non-zero timeout.
            if !workflow_task_timeout.is_zero() && task_type == "workflow" {
                match tokio::time::timeout(
                    workflow_task_timeout,
                    process_task(
                        &pool,
                        Arc::clone(&registry),
                        task,
                        &worker_id,
                        &build_id,
                        cancellation_grace_period,
                        sticky_timeout,
                        max_local_activity_start_to_close,
                        workflow_cache,
                        dispatched_at,
                        max_concurrent_sessions,
                        &session_slots_in_use,
                        Arc::clone(&panic_strikes),
                        workflow_panic_max_attempts,
                    ),
                )
                .await
                {
                    Ok(Ok(())) => {
                        // Success: clear the consecutive-timeout counter for
                        // this execution so a later transient timeout doesn't
                        // inherit previous strikes.
                        if let Some(exec_id) = exec_id_for_timeout {
                            timeout_strikes
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .remove(&exec_id);
                        }
                    }
                    Ok(Err(error)) => {
                        // Task returned a clean error (not a timeout): clear
                        // the strike counter and log the error normally.
                        if let Some(exec_id) = exec_id_for_timeout {
                            timeout_strikes
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .remove(&exec_id);
                        }
                        tracing::error!(
                            task_id = %task_id,
                            task_type = %task_type,
                            worker_id = %worker_id,
                            error = %error,
                            "task execution failed"
                        );
                    }
                    Err(_elapsed) => {
                        // Release the concurrency slot immediately so other
                        // tasks can be dispatched while we do recovery I/O
                        // (metric DB lookup + reset/quarantine).  The
                        // `process_task` future was already cancelled by
                        // tokio::time::timeout, but `permit` lives in this
                        // outer scope and would otherwise be held until the
                        // recovery awaits complete.
                        drop(permit);
                        let timeout_secs = workflow_task_timeout.as_secs();
                        let new_strikes = exec_id_for_timeout.map_or(1, |exec_id| {
                            let mut guard = timeout_strikes
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            let count = guard.entry(exec_id).or_insert(0);
                            *count += 1;
                            let result = *count;
                            drop(guard);
                            result
                        });

                        // Emit metric tagged by workflow+queue. We attempt a
                        // best-effort DB lookup for the workflow name; the
                        // metric is still emitted with "unknown" on failure
                        // rather than being dropped.
                        #[cfg(feature = "db")]
                        let (workflow_name_str, queue_name_str) = {
                            workflow_task_timeout_metric_names(&pool, exec_id_for_timeout).await
                        };
                        #[cfg(not(feature = "db"))]
                        let (workflow_name_str, queue_name_str) =
                            ("unknown".to_string(), "default".to_string());

                        telemetry
                            .telemetry()
                            .metrics
                            .record_workflow_task_timeout(&workflow_name_str, &queue_name_str);

                        tracing::warn!(
                            task_id = %task_id,
                            exec_id = ?exec_id_for_timeout,
                            strikes = new_strikes,
                            threshold = poison_pill_threshold,
                            timeout_secs = timeout_secs,
                            workflow = %workflow_name_str,
                            queue = %queue_name_str,
                            "workflow task timed out; concurrency slot reclaimed"
                        );

                        let decision = crate::poison_pill::quarantine_decision(
                            new_strikes,
                            poison_pill_threshold,
                        );

                        #[cfg(feature = "db")]
                        match decision {
                            crate::poison_pill::ReclaimAction::Quarantine => {
                                // Clear the in-process counter before the
                                // async DB call so a concurrent reclaim
                                // doesn't double-quarantine.
                                if let Some(exec_id) = exec_id_for_timeout {
                                    timeout_strikes
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                                        .remove(&exec_id);
                                }
                                quarantine_workflow_task_timeout(
                                    &pool,
                                    task_id,
                                    exec_id_for_timeout,
                                    &worker_id,
                                    new_strikes,
                                    timeout_secs,
                                    &workflow_name_str,
                                    &queue_name_str,
                                    &*telemetry.telemetry().metrics,
                                )
                                .await;
                            }
                            crate::poison_pill::ReclaimAction::Requeue => {
                                // Reset the task to PENDING so any worker can
                                // re-claim it on the next poll, without waiting
                                // for the orphan-reclaim staleness window.
                                reset_timed_out_workflow_task(&pool, task_id, &worker_id).await;
                            }
                        }
                        #[cfg(not(feature = "db"))]
                        let _ = decision;
                    }
                }
            } else {
                // No timeout configured, or not a workflow task: run unbounded.
                if let Err(error) = process_task(
                    &pool,
                    registry,
                    task,
                    &worker_id,
                    &build_id,
                    cancellation_grace_period,
                    sticky_timeout,
                    max_local_activity_start_to_close,
                    workflow_cache,
                    dispatched_at,
                    max_concurrent_sessions,
                    &session_slots_in_use,
                    panic_strikes,
                    workflow_panic_max_attempts,
                )
                .await
                {
                    tracing::error!(
                        task_id = %task_id,
                        task_type = %task_type,
                        worker_id = %worker_id,
                        error = %error,
                        "task execution failed"
                    );
                }
            }
        });
    }

    /// Wait for all in-flight tasks to finish (or the drain deadline expires).
    ///
    /// We wait until all semaphore permits are available again, meaning all
    /// spawned tasks have completed and dropped their permits.
    ///
    /// The deadline is read from `remote_drain_deadline` (set by the heartbeat
    /// task) rather than being snapshotted once.  The heartbeat task refreshes
    /// that cell on every tick while draining, so an operator-extended deadline
    /// (via a second POST .../drain with a later `deadline_at`) is picked up
    /// here without restarting the worker.
    async fn drain_in_flight(&self) {
        // Uses the actual permit count behind each semaphore (issue #548):
        // equal to `config.max_concurrent_*` when no slot tuner is
        // configured (byte-identical to before), or the tuner's `max_slots`
        // when one is — a tuner shrink only *withholds* permits, it never
        // reduces the semaphore's total, so waiting for the full total here
        // still correctly detects "every in-flight task has finished".
        let total_permits = self.workflow_permit_total + self.activity_permit_total;

        // Fixed fallback for local (non-remote) shutdowns: computed once so that
        // the 1-second tick in the loop cannot keep sliding it forward.
        let local_deadline = tokio::time::Instant::now() + self.config.shutdown_timeout;

        // Returns the current deadline: remote (refreshable) when set, otherwise
        // the fixed local_deadline computed above.
        let snapshot_deadline = || -> tokio::time::Instant {
            self.remote_drain_deadline
                .lock()
                .ok()
                .and_then(|g| *g)
                .map_or(local_deadline, tokio::time::Instant::from_std)
        };

        let sleep = tokio::time::sleep_until(snapshot_deadline());
        tokio::pin!(sleep);

        let drain = async {
            // Try to acquire ALL permits — when we can, all in-flight tasks are done.
            let _wf = self
                .workflow_semaphore
                .acquire_many(
                    u32::try_from(self.workflow_permit_total)
                        .unwrap_or(u32::MAX)
                        .min((1 << 31) - 1),
                )
                .await;
            let _act = self
                .activity_semaphore
                .acquire_many(
                    u32::try_from(self.activity_permit_total)
                        .unwrap_or(u32::MAX)
                        .min((1 << 31) - 1),
                )
                .await;
        };
        tokio::pin!(drain);

        // Poll for a refreshed deadline once per second so an extended window
        // updates the sleep timer without requiring a worker restart.
        let mut check = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );

        loop {
            tokio::select! {
                biased;
                () = &mut drain => return,
                () = &mut sleep => {
                    tracing::warn!(
                        worker_id = %self.config.worker_id,
                        total_permits,
                        "shutdown timeout elapsed — some tasks may still be running"
                    );
                    return;
                }
                _ = check.tick() => {
                    sleep.as_mut().reset(snapshot_deadline());
                }
            }
        }
    }

    /// Request graceful shutdown of this worker.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

// ---------------------------------------------------------------------------
// Workflow-task timeout helpers (issue #494)
// ---------------------------------------------------------------------------

/// Look up the workflow name and queue name for timeout metric labels.
///
/// Falls back to `("unknown", "default")` on any DB or pool failure so the
/// metric is always emitted even when the execution row is gone.
async fn workflow_task_timeout_metric_names(
    pool: &DbPool,
    exec_id: Option<uuid::Uuid>,
) -> (String, String) {
    use crate::schema::harvest_workflow_executions::dsl;

    let Some(exec_uuid) = exec_id else {
        return ("unknown".to_string(), "default".to_string());
    };
    let Ok(mut conn) = pool.get().await else {
        return ("unknown".to_string(), "default".to_string());
    };
    dsl::harvest_workflow_executions
        .find(exec_uuid)
        .select((dsl::workflow_name, dsl::queue_name))
        .first::<(String, String)>(&mut conn)
        .await
        .optional()
        .ok()
        .flatten()
        .unwrap_or_else(|| ("unknown".to_string(), "default".to_string()))
}

/// Move a timed-out workflow task to the DLQ and fail the owning execution.
///
/// Called when the consecutive in-memory timeout counter reaches
/// `poison_pill_threshold` (issue #494). Errors are logged and swallowed.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn quarantine_workflow_task_timeout(
    pool: &DbPool,
    task_id: uuid::Uuid,
    exec_id_opt: Option<uuid::Uuid>,
    worker_id: &str,
    new_strikes: i32,
    timeout_secs: u64,
    workflow_name: &str,
    queue_name: &str,
    metrics: &dyn crate::telemetry::MetricsRecorder,
) {
    use crate::schema::harvest_task_queue::dsl as task_dsl;
    use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
    use diesel::BoolExpressionMethods;

    let mut conn = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                task_id = %task_id,
                error = %e,
                "workflow task timeout quarantine: pool exhausted"
            );
            return;
        }
    };

    let reason = crate::dlq::DeadLetterReason::WorkflowTaskTimeout {
        task_timeout_strikes: new_strikes,
        timeout_secs,
    };
    let error_msg = reason.to_string();

    // Fetch the task row's input + attempt for the DLQ entry.
    let (input, attempts) = task_dsl::harvest_task_queue
        .find(task_id)
        .select((task_dsl::input, task_dsl::attempt))
        .first::<(serde_json::Value, i32)>(&mut conn)
        .await
        .optional()
        .ok()
        .flatten()
        .unwrap_or((serde_json::Value::Null, 1));

    // Fetch owner/severity/parent_id from the execution row for the DLQ
    // entry and parent notification.  Also record whether the execution row
    // exists so we can skip the event-append path inside the transaction when
    // the row has been deleted or archived (FK violation would otherwise roll
    // back the entire quarantine, leaving the task un-quarantined).
    let mut exec_exists = false;
    let mut parent_id_opt: Option<uuid::Uuid> = None;
    let mut parent_close_policy_opt: Option<String> = None;
    let mut workflow_id_str = String::new();
    let mut schedule_id_opt: Option<uuid::Uuid> = None;
    let mut origin_opt: Option<String> = None;
    let (owner, severity) = match exec_id_opt {
        Some(exec_uuid) => {
            let res = exec_dsl::harvest_workflow_executions
                .find(exec_uuid)
                .select((
                    exec_dsl::owner,
                    exec_dsl::severity,
                    exec_dsl::parent_id,
                    exec_dsl::parent_close_policy,
                    exec_dsl::workflow_id,
                    exec_dsl::schedule_id,
                    exec_dsl::origin,
                ))
                .first::<(
                    Option<String>,
                    Option<String>,
                    Option<uuid::Uuid>,
                    Option<String>,
                    String,
                    Option<uuid::Uuid>,
                    Option<String>,
                )>(&mut conn)
                .await
                .optional()
                .ok()
                .flatten();
            match res {
                Some((o, s, p, pcp, wid, sched_id, orig)) => {
                    exec_exists = true;
                    parent_id_opt = p;
                    parent_close_policy_opt = pcp;
                    workflow_id_str = wid;
                    schedule_id_opt = sched_id;
                    origin_opt = orig;
                    (o, s)
                }
                None => (None, None),
            }
        }
        None => (None, None),
    };

    let entry = crate::dlq::NewDeadLetterEntry {
        original_task_id: task_id,
        queue_name: queue_name.to_string(),
        task_type: "workflow".to_string(),
        workflow_exec_id: exec_id_opt,
        activity_name: None,
        input,
        error: error_msg.clone(),
        attempts,
        owner,
        severity,
    };

    let result = conn
        .transaction::<(
            Vec<crate::completion_trigger::DeferredTriggerStart>,
            Option<String>,
            Vec<(ExecutionId, String)>,
        ), HarvestError, _>(|conn| {
            let error_msg = error_msg.clone();
            let entry = entry.clone();
            let worker_id = worker_id.to_string();
            async move {
                dlq::dead_letter(conn, &entry).await?;
                queue::fail_task(conn, task_id, &error_msg).await?;

                let (deferred, queue_used, closed_children) = if let Some(exec_uuid) = exec_id_opt {
                    if exec_exists {
                        // Lock the execution row and re-check its *current* state
                        // before appending any event or mutating it further.
                        //
                        // A legitimate completion (or cancel/terminate/timeout) can
                        // commit in the window between this workflow task's own
                        // timeout firing and this transaction acquiring the lock —
                        // `tokio::time::timeout` racing an already-durable side
                        // effect is a well-known hazard. Proceeding unconditionally
                        // used to append a spurious `WorkflowFailed` event onto a
                        // run that actually finished some other way, and (per issue
                        // #605's completion-callback delivery, which re-reads the
                        // execution row inside `evaluate_triggers_for_execution`)
                        // could deliver a signed callback claiming `state: failed`
                        // while sourcing `output`/`completed_at` from the real,
                        // concurrently-committed terminal row.
                        let current_state: Option<String> = exec_dsl::harvest_workflow_executions
                            .find(exec_uuid)
                            .select(exec_dsl::state)
                            .for_update()
                            .first(conn)
                            .await
                            .optional()
                            .map_err(crate::error::database_error)?;

                        if matches!(current_state.as_deref(), Some("RUNNING" | "PAUSED")) {
                            // Drain sibling tasks (PENDING/RUNNING) so they are not
                            // claimed and run after the workflow has been terminally
                            // failed. Mirrors the poison-pill quarantine path.
                            diesel::update(
                                task_dsl::harvest_task_queue
                                    .filter(task_dsl::workflow_exec_id.eq(exec_uuid))
                                    .filter(
                                        task_dsl::state
                                            .eq("PENDING")
                                            .or(task_dsl::state.eq("RUNNING")),
                                    ),
                            )
                            .set((
                                task_dsl::state.eq("FAILED"),
                                task_dsl::error.eq(Some(error_msg.clone())),
                                task_dsl::completed_at.eq(Some(chrono::Utc::now())),
                            ))
                            .execute(conn)
                            .await
                            .map_err(crate::error::database_error)?;

                            let exec_id = execution_id_from_uuid(exec_uuid);
                            let history = crate::store::load_history(conn, exec_id).await?;
                            crate::store::append_events(
                                conn,
                                exec_id,
                                &[WorkflowEvent::workflow_failed(error_msg.clone())],
                                history.next_event_id,
                            )
                            .await?;
                            // update_workflow_execution_failed only transitions RUNNING
                            // rows; ignore errors — the DLQ entry and event append are
                            // the durable record.
                            let _ = update_workflow_execution_failed(
                                conn, exec_id, &worker_id, &error_msg, None,
                            )
                            .await;
                            // Also handle the PAUSED → FAILED transition: an operator
                            // may have paused the execution between dispatch and quarantine.
                            let _ = diesel::update(
                                exec_dsl::harvest_workflow_executions
                                    .find(exec_uuid)
                                    .filter(exec_dsl::state.eq("PAUSED")),
                            )
                            .set((
                                exec_dsl::state.eq("FAILED"),
                                exec_dsl::output.eq(None::<serde_json::Value>),
                                exec_dsl::error.eq(Some(error_msg.clone())),
                                exec_dsl::completed_at.eq(Some(chrono::Utc::now())),
                            ))
                            .execute(conn)
                            .await;
                            let (mut deferred, closed_children) =
                                apply_parent_close_cascade(conn, exec_id).await?;
                            let triggers =
                                crate::completion_trigger::evaluate_triggers_for_execution(
                                    conn,
                                    exec_id,
                                    crate::completion_trigger::TerminalState::Failed,
                                    Some(metrics),
                                )
                                .await?;
                            deferred.extend(triggers);
                            // Notify the parent only for awaited children (those without
                            // a parent_close_policy). Detached children are managed by
                            // apply_parent_close_cascade above, mirroring the poison-pill
                            // and timeout paths.
                            if parent_close_policy_opt.is_none()
                                && let Some(parent_uuid) = parent_id_opt
                            {
                                let parent_exec_id = execution_id_from_uuid(parent_uuid);
                                let _ = wake_parent_for_child_failure(
                                    conn,
                                    parent_exec_id,
                                    exec_id,
                                    &error_msg,
                                )
                                .await;
                            }
                            (deferred, Some(entry.queue_name.clone()), closed_children)
                        } else {
                            // The execution already reached a different terminal
                            // state (or was deleted/archived) before this
                            // quarantine's lock was acquired: this attempt lost the
                            // race. The timed-out task row itself is still
                            // failed/DLQ'd above (that part is about the task, not
                            // the workflow's own terminal outcome), but the
                            // execution — and any completion-callback delivery for
                            // it — must not be touched.
                            tracing::warn!(
                                exec_id = %exec_uuid,
                                current_state = ?current_state,
                                "workflow task timeout quarantine: execution already \
                                 left RUNNING/PAUSED before the quarantine lock was \
                                 acquired; skipping the WorkflowFailed transition"
                            );
                            (Vec::new(), None, Vec::new())
                        }
                    } else {
                        (Vec::new(), None, Vec::new())
                    }
                } else {
                    (Vec::new(), None, Vec::new())
                };

                Ok((deferred, queue_used, closed_children))
            }
            .scope_boxed()
        })
        .await;

    match result {
        Ok((deferred_starts, queue_used, closed_children)) => {
            if let Some(q) = queue_used {
                metrics.record_workflow_terminal(
                    workflow_name,
                    &q,
                    crate::telemetry::WorkflowStatus::Failed,
                );
                if let Some(exec_uuid) = exec_id_opt {
                    let exec_id = execution_id_from_uuid(exec_uuid);
                    check_and_report_unfinished_handlers_for_worker(
                        &mut conn,
                        exec_id,
                        Some(workflow_name),
                        Some(metrics),
                    )
                    .await;
                }
            }
            // Best-effort: count quarantine failures toward the schedule
            // auto-pause threshold (mirrors execution-timeout and normal failure
            // paths). Called after the transaction commits so a counter query
            // failure cannot roll back the quarantine.
            #[cfg(feature = "db")]
            if !workflow_id_str.is_empty() {
                crate::scheduler::maybe_increment_schedule_failure_counter(
                    &mut conn,
                    &workflow_id_str,
                    workflow_name,
                    schedule_id_opt,
                    origin_opt.as_deref(),
                    metrics,
                )
                .await;
            }

            for (child_id, child_name) in closed_children {
                check_and_report_unfinished_handlers_for_worker(
                    &mut conn,
                    child_id,
                    Some(&child_name),
                    Some(metrics),
                )
                .await;
            }

            for start in deferred_starts {
                start.spawn();
            }
        }
        Err(e) => {
            tracing::error!(
                task_id = %task_id,
                error = %e,
                "workflow task timeout quarantine: transaction failed"
            );
        }
    }
}

/// Reset a timed-out RUNNING workflow task back to PENDING so any worker can
/// re-claim it on the next poll cycle without waiting for the orphan-reclaim
/// staleness window (issue #494).
///
/// Uses an optimistic `WHERE state = 'RUNNING' AND worker_id = …` guard so a
/// concurrent reclaim or a different worker that somehow picked it up does not
/// get its state overwritten.
pub async fn reset_timed_out_workflow_task(pool: &DbPool, task_id: uuid::Uuid, worker_id: &str) {
    use crate::schema::harvest_task_queue::dsl;

    // Retry acquiring a pool connection: a transient pool saturation during
    // timeout handling would otherwise leave the task stuck in RUNNING on a
    // live worker (the orphan reclaimer skips tasks owned by live workers).
    let mut conn = {
        let mut last_err = None;
        let backoff_ms: &[u64] = &[0, 200, 500, 2_000];
        let mut result = None;
        for &delay_ms in backoff_ms {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            match pool.get().await {
                Ok(c) => {
                    result = Some(c);
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        task_id = %task_id,
                        worker_id = %worker_id,
                        error = %e,
                        "workflow task timeout reset: pool unavailable, retrying"
                    );
                    last_err = Some(e);
                }
            }
        }
        if let Some(c) = result {
            c
        } else {
            tracing::error!(
                task_id = %task_id,
                worker_id = %worker_id,
                error = ?last_err,
                "workflow task timeout reset: pool exhausted after retries; \
                 task may be stuck RUNNING until worker stops"
            );
            return;
        }
    };
    match diesel::update(
        dsl::harvest_task_queue
            .find(task_id)
            .filter(dsl::state.eq("RUNNING"))
            .filter(dsl::worker_id.eq(worker_id)),
    )
    .set((
        dsl::state.eq("PENDING"),
        dsl::worker_id.eq(None::<String>),
        dsl::started_at.eq(None::<chrono::DateTime<chrono::Utc>>),
        dsl::last_heartbeat_at.eq(None::<chrono::DateTime<chrono::Utc>>),
        // Clear sticky affinity so any worker can reclaim the task immediately
        // rather than waiting for the expired lease of the hung worker.
        dsl::sticky_worker_id.eq(None::<String>),
        dsl::sticky_until.eq(None::<chrono::DateTime<chrono::Utc>>),
    ))
    .execute(&mut conn)
    .await
    {
        Ok(n) if n > 0 => {
            tracing::debug!(
                task_id = %task_id,
                worker_id = %worker_id,
                "reset timed-out workflow task to PENDING"
            );
        }
        Ok(_) => {
            tracing::debug!(
                task_id = %task_id,
                worker_id = %worker_id,
                "timed-out workflow task already reclaimed or transitioned"
            );
        }
        Err(e) => {
            tracing::error!(
                task_id = %task_id,
                worker_id = %worker_id,
                error = %e,
                "failed to reset timed-out workflow task to PENDING"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Tests (unit, no DB)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ParentClosePolicy;
    use serde_json::Value;
    use tokio::sync::oneshot;

    // slot_occupancy (issue #531) moved to crate::slot_tuner; its unit tests
    // now live in slot_tuner.rs's test module alongside the tuner logic that
    // consumes it.

    // ── WASM effective start-to-close resolution (issue #965, AC1) ───────────
    //
    // Proves the WASM dispatch seam honors a per-task `start_to_close` override
    // (DAG / race calls) exactly like native dispatch, falling back to the
    // registration default only when the task row carries no override.
    //
    // Note: a full end-to-end assertion of the deadline the guest actually runs
    // under is a wall-clock race (the deadline only fires if the guest outlives
    // it), so we unit-test the exact resolution function the seam calls instead
    // — `resolve_wasm_dispatch` threads its `deadline` argument straight through
    // to `invoke_wasm_activity`, so proving the seam computes the right value is
    // proving the guest runs under it.
    #[cfg(feature = "wasm-activities")]
    #[test]
    fn wasm_effective_deadline_prefers_per_task_override() {
        // Per-call override present (e.g. a DAG-supplied 5m) beats the 30s
        // registration default — the override wins.
        assert_eq!(
            wasm_effective_deadline(
                Some(chrono::Duration::seconds(300)),
                Some(Duration::from_secs(30)),
            ),
            Some(Duration::from_secs(300)),
        );
        // A shorter override also wins over a longer default.
        assert_eq!(
            wasm_effective_deadline(
                Some(chrono::Duration::seconds(5)),
                Some(Duration::from_secs(30)),
            ),
            Some(Duration::from_secs(5)),
        );
    }

    #[cfg(feature = "wasm-activities")]
    #[test]
    fn wasm_effective_deadline_falls_back_to_default() {
        // No per-task override (NULL column) -> registration default applies.
        assert_eq!(
            wasm_effective_deadline(None, Some(Duration::from_secs(30))),
            Some(Duration::from_secs(30)),
        );
        // Neither present -> None, so the runtime max_wall_clock ceiling (M2)
        // is the only bound.
        assert_eq!(wasm_effective_deadline(None, None), None);
    }

    // ── Adaptive overdue sampler interval (issue #696, Codex round 4) ─────────

    #[test]
    fn overdue_interval_no_active_cadence_uses_ceiling() {
        // A slow / Manual-only fleet (no active cadence) stays at the 30s ceiling.
        assert_eq!(
            next_overdue_sample_interval(None, Duration::from_millis(500)),
            SCHEDULE_OVERDUE_SAMPLE_MAX
        );
    }

    #[test]
    fn overdue_interval_slow_cadence_clamps_to_ceiling() {
        // A cadence above the 30s ceiling (e.g. hourly) clamps to the ceiling.
        assert_eq!(
            next_overdue_sample_interval(
                Some(Duration::from_secs(3600)),
                Duration::from_millis(500)
            ),
            SCHEDULE_OVERDUE_SAMPLE_MAX
        );
    }

    #[test]
    fn overdue_interval_in_band_cadence_is_used() {
        // A 5s schedule (between the 500ms floor and 30s ceiling) samples at 5s.
        assert_eq!(
            next_overdue_sample_interval(Some(Duration::from_secs(5)), Duration::from_millis(500)),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn overdue_interval_fast_cadence_never_below_poll_floor() {
        // A 1s schedule present → sampled near its cadence (1s > 500ms floor),
        // NOT the 30s ceiling — the fast-schedule detection fix.
        assert_eq!(
            next_overdue_sample_interval(Some(Duration::from_secs(1)), Duration::from_millis(500)),
            Duration::from_secs(1)
        );
        // A cadence below the poll floor is clamped UP to the floor (no busy-spin).
        assert_eq!(
            next_overdue_sample_interval(Some(Duration::from_secs(1)), Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn overdue_interval_large_poll_interval_never_inverts_clamp() {
        // A poll_interval larger than the 30s ceiling pins at the ceiling rather
        // than panicking on an inverted clamp.
        assert_eq!(
            next_overdue_sample_interval(Some(Duration::from_secs(5)), Duration::from_secs(60)),
            SCHEDULE_OVERDUE_SAMPLE_MAX
        );
        assert_eq!(
            next_overdue_sample_interval(None, Duration::from_secs(60)),
            SCHEDULE_OVERDUE_SAMPLE_MAX
        );
    }

    // ── Disappeared gauge-label cleanup (issue #696, Codex round 5 F2) ─────────

    fn key(kind: &str, name: &str) -> ScheduleGaugeKey {
        (kind.to_string(), name.to_string())
    }

    fn key_set(pairs: &[(&str, &str)]) -> std::collections::HashSet<ScheduleGaugeKey> {
        pairs.iter().map(|(k, n)| key(k, n)).collect()
    }

    #[test]
    fn labels_to_clear_returns_disappeared_labels_on_complete_pass() {
        // previous={A,B} current={A} complete=true → [B].
        let previous = key_set(&[("workflow", "a"), ("workflow", "b")]);
        let current = key_set(&[("workflow", "a")]);
        let cleared = labels_to_clear(&previous, &current, true);
        assert_eq!(cleared, vec![key("workflow", "b")]);
    }

    #[test]
    fn labels_to_clear_is_empty_on_partial_pass() {
        // Same as above but complete=false → [] (a shard outage must not zero a
        // genuinely-overdue schedule living on the failed shard).
        let previous = key_set(&[("workflow", "a"), ("workflow", "b")]);
        let current = key_set(&[("workflow", "a")]);
        assert!(labels_to_clear(&previous, &current, false).is_empty());
    }

    #[test]
    fn labels_to_clear_is_empty_when_previous_subset_of_current() {
        // previous ⊆ current → [] (nothing disappeared).
        let previous = key_set(&[("workflow", "a")]);
        let current = key_set(&[("workflow", "a"), ("dag", "b")]);
        assert!(labels_to_clear(&previous, &current, true).is_empty());
    }

    #[test]
    fn labels_to_clear_is_empty_when_previous_empty() {
        // Empty previous (first pass) → [] regardless of current.
        let previous = std::collections::HashSet::new();
        let current = key_set(&[("workflow", "a")]);
        assert!(labels_to_clear(&previous, &current, true).is_empty());
    }

    fn default_runtime_config() -> WorkerRuntimeConfig {
        WorkerRuntimeConfig {
            worker_id: "test-worker-1".to_string(),
            queues: vec!["default".to_string()],
            notification_database_url: None,
            shard_notification_database_urls: Vec::new(),
            max_concurrent_workflows: 10,
            max_concurrent_activities: 20,
            poll_interval: Duration::from_millis(100),
            shutdown_timeout: Duration::from_secs(5),
            cancellation_grace_period: Duration::from_secs(5),
            sticky_timeout: Duration::from_secs(5),
            max_local_activity_start_to_close: Duration::from_secs(60),
            shard_assignments: vec![crate::types::ShardId::new(0)],
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: String::new(),
            deployment_name: None,
            workflow_cache_size: 1000,
            priority_aging_secs: None,
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,
            workflow_task_timeout: Duration::from_secs(10),
            workflow_panic_max_attempts: 3,
            max_workflow_pause_duration: Duration::from_secs(24 * 3600),
            max_workflow_history_events: None,
            labels: std::collections::HashMap::new(),
            queue_weights: std::collections::HashMap::new(),
            #[cfg(feature = "db")]
            sharded_pool: None,
            slot_tuner: None,
            max_concurrent_sessions: 0,
        }
    }

    #[test]
    fn worker_config_validates() {
        let cfg = default_runtime_config();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn runtime_config_carries_slot_tuner() {
        // WorkerRuntimeConfig must preserve the configured band across the
        // WorkerConfig -> WorkerRuntimeConfig conversion (issue #548).
        let worker_config = crate::builder::WorkerConfig::default()
            .with_slot_tuner(crate::slot_tuner::SlotTunerConfig::new(5, 50));
        let runtime_config = WorkerRuntimeConfig::from(worker_config);
        let tuner = runtime_config
            .slot_tuner
            .expect("slot_tuner must be carried through From<WorkerConfig>");
        assert_eq!(tuner.min_slots, 5);
        assert_eq!(tuner.max_slots, 50);
    }

    #[test]
    fn runtime_config_validate_warns_but_does_not_error_on_degenerate_band() {
        let mut cfg = default_runtime_config();
        cfg.slot_tuner = Some(crate::slot_tuner::SlotTunerConfig::new(50, 5));
        // A degenerate band (min > max) must not fail worker startup — it is
        // reported via tracing::warn! only (issue #548, queue_weights
        // precedent).
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn runtime_config_validate_rejects_zero_max_slots() {
        let mut cfg = default_runtime_config();
        cfg.slot_tuner = Some(crate::slot_tuner::SlotTunerConfig::new(0, 0));
        // max_slots == 0 makes the dispatch semaphore permanently empty; this
        // must be a hard startup error, not a warning (issue #548 review) —
        // a worker that can never dispatch a task must never look healthy.
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("max_slots is 0"));
    }

    #[test]
    fn runtime_config_validate_rejects_zero_min_slots() {
        let mut cfg = default_runtime_config();
        cfg.slot_tuner = Some(crate::slot_tuner::SlotTunerConfig::new(0, 10));
        // min_slots == 0 lets the tuner shrink to 0 slots under pool
        // pressure and get permanently stuck there, since no task can ever
        // dispatch to produce the permit-wait signal a grow decision
        // requires (issue #548 review). Hard error, matching max_slots == 0.
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("min_slots is 0"));
    }

    // ── Workflow-task timeout tests (issue #494) ──────────────────────────

    #[test]
    fn runtime_config_workflow_task_timeout_propagated() {
        // WorkerRuntimeConfig must expose workflow_task_timeout so dispatch_task
        // can apply the bounded budget.
        let cfg = default_runtime_config();
        assert_eq!(cfg.workflow_task_timeout, Duration::from_secs(10));
    }

    #[test]
    fn runtime_config_workflow_task_timeout_zero_disables() {
        let mut cfg = default_runtime_config();
        cfg.workflow_task_timeout = Duration::ZERO;
        assert!(cfg.workflow_task_timeout.is_zero());
    }

    // ── current_details empty-string-clears tests (issue #593) ────────────
    //
    // `latest_current_details_update` is the pure decision function behind
    // `persist_current_details_from_commands`: it scans a command list for
    // the last `SetCurrentDetails` and reports what the DB write should be.
    // `CurrentDetailsUpdate::NoOp` = no command present this cycle, or the
    // last command's `explicit_clear` is false and its value is empty (a
    // truncation artifact -- preserve whatever is already stored). `Clear` =
    // the last command has `explicit_clear = true`, which must clear
    // `current_details` to SQL NULL. `Set(s)` = set `current_details` to `s`.

    #[test]
    fn latest_current_details_update_none_when_no_command() {
        let cmds: Vec<WorkflowCommand> = Vec::new();
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::NoOp
        );
    }

    #[test]
    fn latest_current_details_update_returns_set_value() {
        let cmds = vec![WorkflowCommand::SetCurrentDetails {
            value: "step 2/4: processing payment".to_string(),
            explicit_clear: false,
        }];
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::Set("step 2/4: processing payment")
        );
    }

    #[test]
    fn latest_current_details_update_last_write_wins() {
        let cmds = vec![
            WorkflowCommand::SetCurrentDetails {
                value: "first".to_string(),
                explicit_clear: false,
            },
            WorkflowCommand::SetCurrentDetails {
                value: "second".to_string(),
                explicit_clear: false,
            },
            WorkflowCommand::SetCurrentDetails {
                value: "third".to_string(),
                explicit_clear: false,
            },
        ];
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::Set("third")
        );
    }

    #[test]
    fn latest_current_details_update_explicit_clear_clears() {
        let cmds = vec![WorkflowCommand::SetCurrentDetails {
            value: String::new(),
            explicit_clear: true,
        }];
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::Clear,
            "an author-issued empty string (explicit_clear=true) must resolve to a clear (NULL)"
        );
    }

    #[test]
    fn latest_current_details_update_set_then_explicit_clear_clears() {
        let cmds = vec![
            WorkflowCommand::SetCurrentDetails {
                value: "in progress".to_string(),
                explicit_clear: false,
            },
            WorkflowCommand::SetCurrentDetails {
                value: String::new(),
                explicit_clear: true,
            },
        ];
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::Clear,
            "last-write-wins applies to clears too: a trailing explicit clear wins"
        );
    }

    #[test]
    fn latest_current_details_update_explicit_clear_then_set_overwrites_clear() {
        let cmds = vec![
            WorkflowCommand::SetCurrentDetails {
                value: String::new(),
                explicit_clear: true,
            },
            WorkflowCommand::SetCurrentDetails {
                value: "now running".to_string(),
                explicit_clear: false,
            },
        ];
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::Set("now running")
        );
    }

    #[test]
    fn latest_current_details_update_ignores_other_command_types() {
        let cmds = vec![
            WorkflowCommand::RecordMarker {
                name: "unrelated".to_string(),
                details: Value::Null,
            },
            WorkflowCommand::SetCurrentDetails {
                value: "the real status".to_string(),
                explicit_clear: false,
            },
            WorkflowCommand::RecordMarker {
                name: "also-unrelated".to_string(),
                details: Value::Null,
            },
        ];
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::Set("the real status")
        );
    }

    // ── Truncated-to-empty vs. explicit clear (post-review hardening,
    //    issue #593 / PR #894) ────────────────────────────────────────────
    //
    // A non-empty status can truncate down to an empty `value` when
    // `current_details_cap` is 0 (or smaller than the input's first UTF-8
    // character). That must resolve to `NoOp` -- preserving whatever is
    // already stored -- rather than being confused with an author-issued
    // `set_current_details("")` clear.

    #[test]
    fn latest_current_details_update_truncated_to_empty_without_explicit_clear_is_noop() {
        let cmds = vec![WorkflowCommand::SetCurrentDetails {
            value: String::new(),
            explicit_clear: false,
        }];
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::NoOp,
            "a value truncated down to empty without explicit_clear must not \
             clear the column -- it must be a no-op that preserves the \
             existing breadcrumb"
        );
    }

    #[test]
    fn latest_current_details_update_trailing_truncated_noop_does_not_fall_back_to_earlier_set() {
        // Last-write-wins means the LAST command decides the outcome, even
        // when that outcome is an unrepresentable NoOp -- it must not silently
        // fall back to an earlier, valid Set in the same cycle. Falling back
        // would surprise the workflow author, who believes their last call
        // is the one that took effect.
        let cmds = vec![
            WorkflowCommand::SetCurrentDetails {
                value: "real status".to_string(),
                explicit_clear: false,
            },
            WorkflowCommand::SetCurrentDetails {
                value: String::new(),
                explicit_clear: false,
            },
        ];
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::NoOp
        );
    }

    #[test]
    fn latest_current_details_update_explicit_clear_after_truncated_noop_still_clears() {
        let cmds = vec![
            WorkflowCommand::SetCurrentDetails {
                value: String::new(),
                explicit_clear: false,
            },
            WorkflowCommand::SetCurrentDetails {
                value: String::new(),
                explicit_clear: true,
            },
        ];
        assert_eq!(
            latest_current_details_update(&cmds),
            CurrentDetailsUpdate::Clear
        );
    }

    #[test]
    fn workflow_task_timeout_threshold_decision_under_threshold() {
        // A consecutive-timeout count strictly below the threshold → Requeue.
        assert_eq!(
            crate::poison_pill::quarantine_decision(1, 3),
            crate::poison_pill::ReclaimAction::Requeue
        );
        assert_eq!(
            crate::poison_pill::quarantine_decision(2, 3),
            crate::poison_pill::ReclaimAction::Requeue
        );
    }

    #[test]
    fn workflow_task_timeout_threshold_decision_at_threshold() {
        // Exactly at or above threshold → Quarantine.
        assert_eq!(
            crate::poison_pill::quarantine_decision(3, 3),
            crate::poison_pill::ReclaimAction::Quarantine
        );
        assert_eq!(
            crate::poison_pill::quarantine_decision(4, 3),
            crate::poison_pill::ReclaimAction::Quarantine
        );
    }

    #[test]
    fn workflow_task_timeout_zero_threshold_always_requeues() {
        // threshold = 0 disables quarantine (backward compat).
        assert_eq!(
            crate::poison_pill::quarantine_decision(100, 0),
            crate::poison_pill::ReclaimAction::Requeue
        );
    }

    /// AC #7a — a `tokio::time::timeout` on a dispatch releases the semaphore
    /// permit when the budget expires, so the slot is not permanently consumed.
    #[tokio::test]
    async fn timeout_releases_semaphore_permit() {
        use tokio::sync::Semaphore;
        let sem = std::sync::Arc::new(Semaphore::new(1));
        let sem2 = sem.clone();
        let hung = async move {
            let _permit = sem2.acquire().await.expect("acquire");
            tokio::time::sleep(Duration::from_secs(60)).await;
        };
        let result = tokio::time::timeout(Duration::from_millis(20), hung).await;
        assert!(result.is_err(), "timeout must fire");
        assert_eq!(
            sem.available_permits(),
            1,
            "permit must be released after dispatch timeout"
        );
    }

    /// AC #7b — while one slot is stuck, a second concurrent dispatch still
    /// makes progress (does not block on the wedged future).
    #[tokio::test]
    async fn healthy_dispatch_proceeds_while_slot_is_timed_out() {
        use tokio::sync::{Semaphore, oneshot};
        let sem = std::sync::Arc::new(Semaphore::new(2));
        let (tx, rx) = oneshot::channel::<()>();
        let sem_hung = sem.clone();
        let hung = async move {
            let _permit = sem_hung.acquire().await.expect("acquire");
            tokio::time::sleep(Duration::from_secs(60)).await;
        };
        let sem_ok = sem.clone();
        let healthy = async move {
            let _permit = sem_ok.acquire().await.expect("acquire");
            tokio::time::sleep(Duration::from_millis(5)).await;
            let _ = tx.send(());
        };
        let (hung_result, ()) = tokio::join!(
            tokio::time::timeout(Duration::from_millis(30), hung),
            healthy,
        );
        assert!(hung_result.is_err(), "hung dispatch must time out");
        assert!(
            rx.await.is_ok(),
            "healthy dispatch must complete while hung dispatch is active"
        );
    }

    // ── merge_wake_events (issue #476 review) ─────────────────────────────

    #[test]
    fn merge_wake_events_signal_before_deadline_is_recorded_first() {
        let fires_at = chrono::Utc::now();
        let received_at = fires_at - chrono::Duration::seconds(30);
        let events = merge_wake_events(
            vec![(TimerId::new("__signal_timeout:1:approval"), fires_at)],
            vec![(
                "approval".to_string(),
                serde_json::json!({"approved": true}),
                received_at,
            )],
        );

        assert_eq!(events.len(), 2);
        assert!(
            matches!(&events[0], WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == "approval"),
            "a signal received before the deadline must be appended before TimerFired, got {events:?}"
        );
        assert!(matches!(&events[1], WorkflowEvent::TimerFired { .. }));
    }

    #[test]
    fn merge_wake_events_signal_after_deadline_is_recorded_after_timer() {
        let fires_at = chrono::Utc::now();
        let received_at = fires_at + chrono::Duration::seconds(30);
        let events = merge_wake_events(
            vec![(TimerId::new("__signal_timeout:1:approval"), fires_at)],
            vec![(
                "approval".to_string(),
                serde_json::json!({"approved": true}),
                received_at,
            )],
        );

        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0], WorkflowEvent::TimerFired { .. }));
        assert!(matches!(&events[1], WorkflowEvent::SignalReceived { .. }));
    }

    #[test]
    fn merge_wake_events_tie_goes_to_the_timer() {
        // A signal received exactly at the deadline did not beat it.
        let fires_at = chrono::Utc::now();
        let events = merge_wake_events(
            vec![(TimerId::new("t"), fires_at)],
            vec![("approval".to_string(), Value::Null, fires_at)],
        );

        assert!(matches!(&events[0], WorkflowEvent::TimerFired { .. }));
        assert!(matches!(&events[1], WorkflowEvent::SignalReceived { .. }));
    }

    #[test]
    fn merge_wake_events_preserves_relative_order_within_each_kind() {
        let base = chrono::Utc::now();
        let events = merge_wake_events(
            vec![
                (TimerId::new("t1"), base),
                (TimerId::new("t2"), base + chrono::Duration::seconds(10)),
            ],
            vec![
                (
                    "s1".to_string(),
                    Value::Null,
                    base - chrono::Duration::seconds(5),
                ),
                (
                    "s2".to_string(),
                    Value::Null,
                    base + chrono::Duration::seconds(5),
                ),
            ],
        );

        let kinds: Vec<String> = events
            .iter()
            .map(|e| match e {
                WorkflowEvent::TimerFired { timer_id } => timer_id.as_str().to_string(),
                WorkflowEvent::SignalReceived { signal_name, .. } => signal_name.clone(),
                other => panic!("unexpected event {other:?}"),
            })
            .collect();
        assert_eq!(kinds, vec!["s1", "t1", "s2", "t2"]);
    }

    #[test]
    fn merge_wake_events_handles_single_kind_batches() {
        let now = chrono::Utc::now();
        let only_timers = merge_wake_events(vec![(TimerId::new("t1"), now)], vec![]);
        assert_eq!(only_timers.len(), 1);
        assert!(matches!(&only_timers[0], WorkflowEvent::TimerFired { .. }));

        let only_signals = merge_wake_events(vec![], vec![("s1".to_string(), Value::Null, now)]);
        assert_eq!(only_signals.len(), 1);
        assert!(matches!(
            &only_signals[0],
            WorkflowEvent::SignalReceived { .. }
        ));
    }

    #[test]
    fn worker_config_rejects_empty_queues() {
        let cfg = WorkerRuntimeConfig {
            queues: vec![],
            ..default_runtime_config()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("queue"));
    }

    #[test]
    fn terminal_execution_transition_error_reports_cancelled_state() {
        let exec_id = ExecutionId::new();
        let error =
            terminal_execution_transition_error(exec_id, "CANCELLED", Some("operator stop"));

        assert!(
            matches!(error, HarvestError::Cancelled(message) if message.contains("operator stop"))
        );
    }

    #[test]
    fn terminal_execution_transition_error_reports_conflicting_terminal_state() {
        let exec_id = ExecutionId::new();
        let error = terminal_execution_transition_error(exec_id, "COMPLETED", None);

        assert!(
            matches!(error, HarvestError::Config(message) if message.contains("already terminal"))
        );
    }

    #[test]
    fn worker_config_from_builder() {
        let builder_cfg = WorkerConfig {
            queues: vec!["email".to_string(), "billing".to_string()],
            notification_database_url: Some("postgres://localhost/test".to_string()),
            shard_notification_database_urls: Vec::new(),
            max_concurrent_workflows: 5,
            max_concurrent_activities: 15,
            shutdown_timeout: Duration::from_secs(60),
            workflow_cache_size: 500,
            sticky_timeout: Duration::from_secs(3),
            cancellation_grace_period: Duration::from_secs(10),
            shard_assignments: vec![crate::types::ShardId::new(0)],
            max_local_activity_start_to_close: Duration::from_secs(60),
            default_activity_retry_policy: None,
            default_activity_start_to_close: None,
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: String::new(),
            deployment_name: None,
            query_timeout: Duration::from_secs(5),
            priority_aging_secs: None,
            max_workflow_start_delay: Duration::from_secs(365 * 24 * 3600),
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,
            workflow_task_timeout: Duration::from_secs(10),
            workflow_panic_max_attempts: 3,
            max_workflow_pause_duration: Duration::from_secs(24 * 3600),
            default_debounce_max_wait: Duration::from_secs(3600),
            labels: std::collections::HashMap::new(),
            max_workflow_history_events: None,
            queue_weights: std::collections::HashMap::new(),
            slot_tuner: None,
            max_concurrent_sessions: 0,
            #[cfg(feature = "db")]
            sharded_pool: None,
        };

        let runtime_cfg: WorkerRuntimeConfig = builder_cfg.into();

        assert_eq!(runtime_cfg.queues, vec!["email", "billing"]);
        assert_eq!(
            runtime_cfg.notification_database_url.as_deref(),
            Some("postgres://localhost/test")
        );
        assert_eq!(runtime_cfg.max_concurrent_workflows, 5);
        assert_eq!(runtime_cfg.max_concurrent_activities, 15);
        assert_eq!(runtime_cfg.shutdown_timeout, Duration::from_secs(60));
        assert_eq!(runtime_cfg.poll_interval, Duration::from_millis(500));
        assert_eq!(
            runtime_cfg.cancellation_grace_period,
            Duration::from_secs(10)
        );
        assert_eq!(
            runtime_cfg.unknown_target_grace_window,
            Duration::from_secs(5)
        );
        // worker_id should be a valid UUID
        assert!(uuid::Uuid::parse_str(&runtime_cfg.worker_id).is_ok());
    }

    #[test]
    fn handler_registry_indexes_by_name() {
        let wf = WorkflowInfo {
            mcp: false,
            name: "onboarding",
            module: "app::workflows",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            sla: None,
            concurrency: None,

            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        };

        let act = ActivityInfo {
            name: "send_email",
            module: "app::activities",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };

        let registry = HandlerRegistry::new(vec![wf], vec![act]);

        assert!(registry.workflows.contains_key("onboarding"));
        assert!(registry.activities.contains_key("send_email"));
        assert!(!registry.workflows.contains_key("nonexistent"));
    }

    // ── Worker sessions (issue #606) ────────────────────────────────────────

    #[test]
    fn handler_registry_always_registers_reserved_session_activities() {
        // No user activities registered at all -- the reserved acquire/
        // release entries must still be present, since
        // persist_scheduled_activities's registry.activities.get(name)
        // lookup must succeed for them at enqueue time (Hole #1).
        let registry = HandlerRegistry::new(vec![], vec![]);
        assert!(
            registry
                .activities
                .contains_key(crate::context::SESSION_ACQUIRE_ACTIVITY_NAME)
        );
        assert!(
            registry
                .activities
                .contains_key(crate::context::SESSION_RELEASE_ACTIVITY_NAME)
        );
    }

    #[test]
    fn handler_registry_reserved_session_activities_have_no_special_policies() {
        let registry = HandlerRegistry::new(vec![], vec![]);
        for name in [
            crate::context::SESSION_ACQUIRE_ACTIVITY_NAME,
            crate::context::SESSION_RELEASE_ACTIVITY_NAME,
        ] {
            let info = registry
                .activities
                .get(name)
                .expect("reserved session activity must be registered");
            assert!(
                !info.is_local,
                "session activities must be enqueued, never run inline"
            );
            assert!(info.circuit_breaker.is_none());
            assert!(info.requires.is_none());
            assert!(info.rate_limit_key.is_none());
            assert!(info.default_schedule_to_start.is_none());
        }
    }

    #[tokio::test]
    async fn session_internal_stub_handler_never_succeeds() {
        // Defense-in-depth: if the process_activity_task interception is
        // ever accidentally bypassed, the stub must fail loudly rather than
        // silently no-op.
        let ctx = ActivityContext::new_test();
        let result = session_internal_stub_handler(&ctx, serde_json::Value::Null).await;
        assert!(result.is_err());
    }

    #[test]
    fn new_session_slot_registry_starts_empty() {
        let registry = crate::sessions::new_session_slot_registry();
        assert_eq!(crate::sessions::session_slot_count(&registry), 0);
    }

    #[test]
    fn worker_rejects_invalid_config() {
        let cfg = WorkerRuntimeConfig {
            queues: vec![],
            ..default_runtime_config()
        };
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
        assert!(Worker::new(cfg, registry).is_err());
    }

    #[test]
    fn activity_result_cap_resolves_per_activity_override() {
        fn act(name: &'static str, max_result_bytes: Option<u64>) -> ActivityInfo {
            ActivityInfo {
                name,
                module: "test",
                default_retry_policy: None,
                default_start_to_close: None,
                default_heartbeat_timeout: None,
                default_schedule_to_start: None,
                default_schedule_to_close: None,
                default_queue: None,
                max_concurrent: None,
                concurrency_key: None,
                is_local: false,
                max_input_bytes: None,
                max_result_bytes,
                rate_limit_rps: None,
                rate_limit_burst: None,
                rate_limit_key: None,
                rate_limit_key_expr: None,
                circuit_breaker: None,
                requires: None,
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            }
        }

        let global = crate::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
        let registry = HandlerRegistry::new(
            vec![],
            vec![
                act("plain", None),
                act("big", Some(global * 4)),
                act("tiny", Some(1024)),
            ],
        );

        // No override -> global cap.
        assert_eq!(registry.activity_result_cap("plain"), global);
        // Higher override -> raised cap (full checkpoint visibility, #503).
        assert_eq!(registry.activity_result_cap("big"), global * 4);
        // Lower override -> never lowers below the global ceiling.
        assert_eq!(registry.activity_result_cap("tiny"), global);
        // Unknown activity -> global cap.
        assert_eq!(registry.activity_result_cap("nonexistent"), global);
    }

    #[test]
    fn activity_input_cap_resolves_per_activity_override() {
        fn act(name: &'static str, max_input_bytes: Option<u64>) -> ActivityInfo {
            ActivityInfo {
                name,
                module: "test",
                default_retry_policy: None,
                default_start_to_close: None,
                default_heartbeat_timeout: None,
                default_schedule_to_start: None,
                default_schedule_to_close: None,
                default_queue: None,
                max_concurrent: None,
                concurrency_key: None,
                is_local: false,
                max_input_bytes,
                max_result_bytes: None,
                rate_limit_rps: None,
                rate_limit_burst: None,
                rate_limit_key: None,
                rate_limit_key_expr: None,
                circuit_breaker: None,
                requires: None,
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            }
        }

        let global = crate::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES;
        let registry = HandlerRegistry::new(
            vec![],
            vec![
                act("plain", None),
                act("big", Some(global * 4)),
                act("tiny", Some(1024)),
            ],
        );

        // No override -> global cap.
        assert_eq!(registry.activity_input_cap("plain"), global);
        // Higher override -> raised cap (full input visibility, #608).
        assert_eq!(registry.activity_input_cap("big"), global * 4);
        // Lower override -> never lowers below the global ceiling.
        assert_eq!(registry.activity_input_cap("tiny"), global);
        // Unknown activity -> global cap.
        assert_eq!(registry.activity_input_cap("nonexistent"), global);
    }

    #[test]
    fn worker_creates_with_valid_config() {
        let cfg = default_runtime_config();
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
        let worker = Worker::new(cfg, registry);
        assert!(worker.is_ok());
    }

    /// Build a bare `ActivityInfo` with the rate-limit fields overridden,
    /// everything else defaulted — for the `Worker::new` positivity gate tests
    /// (issue #699 review, Codex P2).
    fn rate_limited_activity(
        name: &'static str,
        rate_limit_rps: Option<f64>,
        rate_limit_burst: Option<f64>,
        rate_limit_key_expr: Option<&'static str>,
    ) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "test",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps,
            rate_limit_burst,
            rate_limit_key: None,
            rate_limit_key_expr,
            circuit_breaker: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    #[test]
    fn worker_new_rejects_non_positive_dynamic_rate_limit_rps() {
        // A hand-built registry (bypassing the `#[activity]` macro) with a dynamic
        // per-key rate limit whose rps is 0.0 would create a `burst = tokens = 0`
        // bucket whose gate can never reach one token, permanently wedging every
        // scheduled activity on it. Worker::new must reject it up front.
        let act = rate_limited_activity("charge", Some(0.0), None, Some("input.tenant_id"));
        let cfg = default_runtime_config();
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![act]));
        let err = Worker::new(cfg, registry).unwrap_err();
        assert!(
            err.to_string().contains("rate_limit_rps"),
            "expected a non-positive rate_limit_rps config error, got: {err}"
        );
    }

    #[test]
    fn worker_new_rejects_non_positive_or_non_finite_rate_limit_burst() {
        // A valid positive rps paired with a non-positive / non-finite burst is
        // also rejected — the burst is what seeds the bucket's token ceiling.
        for bad_burst in [0.0_f64, -3.0, f64::NAN, f64::INFINITY] {
            let act = rate_limited_activity(
                "charge",
                Some(50.0),
                Some(bad_burst),
                Some("input.tenant_id"),
            );
            let cfg = default_runtime_config();
            let registry = Arc::new(HandlerRegistry::new(vec![], vec![act]));
            let err = Worker::new(cfg, registry).unwrap_err();
            assert!(
                err.to_string().contains("rate_limit_burst"),
                "burst {bad_burst}: expected a rate_limit_burst config error, got: {err}"
            );
        }
    }

    #[test]
    fn worker_new_accepts_valid_positive_dynamic_rate_limit() {
        // The valid positive config still starts cleanly.
        let act = rate_limited_activity("charge", Some(50.0), Some(20.0), Some("input.tenant_id"));
        let cfg = default_runtime_config();
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![act]));
        assert!(Worker::new(cfg, registry).is_ok());
    }

    #[test]
    fn worker_rejects_history_ceiling_at_or_below_soft_threshold() {
        // Default soft threshold is 10_000; ceiling must be strictly greater.
        let threshold = HandlerRegistry::new(vec![], vec![])
            .history_policy()
            .continue_as_new_threshold();

        for bad_ceiling in [0u64, 1, threshold.saturating_sub(1), threshold] {
            let cfg = WorkerRuntimeConfig {
                max_workflow_history_events: Some(bad_ceiling),
                ..default_runtime_config()
            };
            let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
            let err = Worker::new(cfg, registry).unwrap_err();
            assert!(
                err.to_string().contains("max_workflow_history_events"),
                "ceiling {bad_ceiling}: expected config error, got: {err}"
            );
        }
    }

    #[test]
    fn worker_accepts_history_ceiling_above_soft_threshold() {
        let threshold = HandlerRegistry::new(vec![], vec![])
            .history_policy()
            .continue_as_new_threshold();

        let cfg = WorkerRuntimeConfig {
            max_workflow_history_events: Some(threshold + 1),
            ..default_runtime_config()
        };
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
        assert!(Worker::new(cfg, registry).is_ok());
    }

    #[test]
    fn worker_shutdown_cancels_token() -> Result<(), crate::error::HarvestError> {
        let cfg = default_runtime_config();
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
        let worker = Worker::new(cfg, registry)?;

        assert!(!worker.shutdown.is_cancelled());
        worker.shutdown();
        assert!(worker.shutdown.is_cancelled());
        Ok(())
    }

    #[test]
    fn claimed_task_kind_uses_lowercase_db_values() -> Result<(), crate::error::HarvestError> {
        assert_eq!(
            ClaimedTaskKind::from_db("workflow")?,
            ClaimedTaskKind::Workflow
        );
        assert_eq!(
            ClaimedTaskKind::from_db("activity")?,
            ClaimedTaskKind::Activity
        );
        assert!(ClaimedTaskKind::from_db("WORKFLOW").is_err());
        Ok(())
    }

    #[test]
    fn all_commands_wait_for_signal_requires_non_empty() {
        let commands: Vec<WorkflowCommand> = vec![];
        assert!(!all_commands_wait_for_signal(&commands));
    }

    #[test]
    fn all_commands_wait_for_signal_only_accepts_wait_commands() {
        let (signal_tx, _signal_rx) = oneshot::channel::<serde_json::Value>();
        let (timer_tx, _timer_rx) = oneshot::channel::<()>();

        let only_wait = vec![WorkflowCommand::WaitForSignal {
            signal_name: "approved".to_string(),
            result_tx: signal_tx,
        }];
        assert!(all_commands_wait_for_signal(&only_wait));

        let mixed = vec![
            WorkflowCommand::WaitForSignal {
                signal_name: "approved".to_string(),
                result_tx: oneshot::channel::<serde_json::Value>().0,
            },
            WorkflowCommand::StartTimer {
                timer_id: crate::types::TimerId::new("t1"),
                duration_secs: 1,
                result_tx: timer_tx,
            },
        ];
        assert!(!all_commands_wait_for_signal(&mixed));
    }

    #[test]
    fn should_requeue_signal_wait_allows_marker_plus_wait() {
        let commands = vec![
            WorkflowCommand::RecordMarker {
                name: "version:gate".to_string(),
                details: serde_json::json!(2),
            },
            WorkflowCommand::WaitForSignal {
                signal_name: "approved".to_string(),
                result_tx: oneshot::channel::<serde_json::Value>().0,
            },
        ];
        assert!(should_requeue_signal_wait(&commands));
    }

    #[test]
    fn should_requeue_signal_wait_rejects_marker_only() {
        let commands = vec![WorkflowCommand::RecordMarker {
            name: "version:gate".to_string(),
            details: serde_json::json!(2),
        }];
        assert!(!should_requeue_signal_wait(&commands));
    }

    // ── resolved_external_ids (issue #678) ──────────────────────────────────
    //
    // Pure, no-DB scan of the external-op terminals a decision cycle appended
    // INLINE (in `new_events`). The mixed timer + external arm feeds the result
    // into `persist_started_timer` for an immediate self-wake. `new_events`
    // contains only this batch's `Requested` + inline terminal events, so the
    // resolved-id set is derivable from it alone.

    fn signal_requested(signal_id: crate::types::ExternalSignalId) -> WorkflowEvent {
        WorkflowEvent::ExternalSignalRequested {
            signal_id,
            target: ExecutionId::new(),
            signal_name: "s".to_string(),
            payload: serde_json::json!({}),
            idempotency_key: None,
        }
    }

    fn cancel_requested(cancel_id: crate::types::ExternalCancelId) -> WorkflowEvent {
        WorkflowEvent::ExternalCancelRequested {
            cancel_id,
            target: ExecutionId::new(),
        }
    }

    #[test]
    fn resolved_external_ids_matches_delivered_signal() {
        let sid = crate::types::ExternalSignalId::new();
        // Real `new_events` carries the Requested event alongside its terminal.
        let new_events = vec![
            signal_requested(sid),
            WorkflowEvent::ExternalSignalDelivered { signal_id: sid },
        ];
        let resolved = resolved_external_ids(&new_events);
        assert!(!resolved.is_empty());
        assert_eq!(resolved.signal_ids, vec![sid]);
        assert!(resolved.cancel_ids.is_empty());
    }

    #[test]
    fn resolved_external_ids_matches_delivered_cancel() {
        let cid = crate::types::ExternalCancelId::new();
        let new_events = vec![
            cancel_requested(cid),
            WorkflowEvent::ExternalCancelDelivered { cancel_id: cid },
        ];
        let resolved = resolved_external_ids(&new_events);
        assert!(!resolved.is_empty());
        assert_eq!(resolved.cancel_ids, vec![cid]);
        assert!(resolved.signal_ids.is_empty());
    }

    #[test]
    fn resolved_external_ids_requested_without_terminal_is_empty() {
        // A `Requested` event with no matching terminal is still pending
        // (cross-shard / NotFound → outbox route), so it contributes nothing
        // and the timer parks normally.
        let sid = crate::types::ExternalSignalId::new();
        let new_events = vec![signal_requested(sid)];
        let resolved = resolved_external_ids(&new_events);
        assert!(
            resolved.is_empty(),
            "an unresolved request must not self-wake"
        );
    }

    #[test]
    fn resolved_external_ids_counts_failed_terminals() {
        let sid = crate::types::ExternalSignalId::new();
        let cid = crate::types::ExternalCancelId::new();
        let new_events = vec![
            signal_requested(sid),
            WorkflowEvent::ExternalSignalFailed {
                signal_id: sid,
                reason_code: "target_unknown".to_string(),
            },
            cancel_requested(cid),
            WorkflowEvent::ExternalCancelFailed {
                cancel_id: cid,
                reason_code: "target_unknown".to_string(),
            },
        ];
        let resolved = resolved_external_ids(&new_events);
        assert_eq!(resolved.signal_ids, vec![sid]);
        assert_eq!(resolved.cancel_ids, vec![cid]);
    }

    #[test]
    fn resolved_external_ids_partial_resolution() {
        // Two signals requested this batch; only the first delivered inline
        // (the second went to the outbox → no terminal). Only the resolved id
        // is returned, so the self-wake is scoped to the branch that resolved.
        let sid_a = crate::types::ExternalSignalId::new();
        let sid_b = crate::types::ExternalSignalId::new();
        let new_events = vec![
            signal_requested(sid_a),
            WorkflowEvent::ExternalSignalDelivered { signal_id: sid_a },
            signal_requested(sid_b),
        ];
        let resolved = resolved_external_ids(&new_events);
        assert_eq!(resolved.signal_ids, vec![sid_a]);
        assert!(resolved.cancel_ids.is_empty());
    }

    #[test]
    fn resolved_external_ids_empty_is_empty() {
        let resolved = resolved_external_ids(&[]);
        assert!(resolved.is_empty());
    }

    // ── cancellable-timer event interleaving (issue #768, FINDING 1) ────────

    /// Codex P2 round 4 (issue #768): a fresh `ArmTimer { for_await: false }`
    /// (from `start_timer`/`reset`) emits a `TimerStarted` event for positional
    /// replay but the pure `arm_timer_events` never signals a `harvest_timers`
    /// row insert — a cancellable timer becomes fire-eligible only when it is
    /// awaited (`for_await: true`, resolved by the DB loop). So a `start_timer`
    /// that arms and then completes/parks in the same task records its arm event
    /// without leaking a never-firing row.
    #[test]
    fn fresh_arm_emits_timer_started_without_signalling_a_row() {
        let commands = vec![WorkflowCommand::ArmTimer {
            timer_id: crate::types::TimerId::new("x"),
            duration_secs: 300,
            for_await: false,
        }];
        let events = arm_timer_events(&commands);
        assert_eq!(events.len(), 1, "aligned to commands, got {events:?}");
        assert!(
            matches!(
                &events[0],
                Some(WorkflowEvent::TimerStarted { timer_id, duration_secs })
                    if timer_id.as_str() == "x" && *duration_secs == 300
            ),
            "a fresh ArmTimer must emit TimerStarted for replay, got {events:?}"
        );
    }

    /// A `for_await: true` re-arm (the `await_fire` row-insert command) emits
    /// **no** event from `arm_timer_events` — its `TimerStarted` was already
    /// recorded by the fresh arm; the DB loop only inserts the row.
    #[test]
    fn await_rearm_emits_no_event_from_the_pure_plan() {
        let commands = vec![WorkflowCommand::ArmTimer {
            timer_id: crate::types::TimerId::new("x"),
            duration_secs: 300,
            for_await: true,
        }];
        let events = arm_timer_events(&commands);
        assert!(
            events.iter().all(Option::is_none),
            "a for_await re-arm emits no event (row-only), got {events:?}"
        );
    }

    /// Issue #768 round 7: an `await_fire` re-arm (`for_await: true`) followed by a
    /// same-task sibling branch's `cancel_timer` in the SAME batch must NOT
    /// contribute a deadline to `min_fires_at` (`armed_indices` empty → the caller's
    /// `armed_fires_at` is `None` → the parked task wakes NOW instead of being
    /// rescheduled to the deleted row's deadline), and the cancel must emit
    /// `TimerCancelled`.
    #[test]
    fn plan_timer_lifecycle_pure_excludes_a_same_batch_cancelled_await_arm() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                for_await: true,
            },
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("x"),
            },
        ];
        let (events, armed) = plan_timer_lifecycle_pure(&commands);
        assert!(
            armed.is_empty(),
            "a same-batch-cancelled await arm must not contribute to min_fires_at, got {armed:?}"
        );
        assert!(
            matches!(&events[1], Some(WorkflowEvent::TimerCancelled { timer_id }) if timer_id.as_str() == "x"),
            "the cancel must emit TimerCancelled, got {events:?}"
        );
        assert!(
            events[0].is_none(),
            "the await re-arm records no positional event, got {events:?}"
        );

        // Cancel-then-await with NO fresh arm in between (a genuine cancellation
        // racing an await, not a reset): the cancel is the last establish/cancel op
        // for the id, so the timer is dead at end-of-batch and the await is dropped
        // (wake now). Distinct from reset-then-await, which interposes a fresh arm —
        // see `plan_timer_lifecycle_pure_arms_a_reset_then_await_in_one_batch`.
        let rev = vec![
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("x"),
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                for_await: true,
            },
        ];
        let (_ev, armed_rev) = plan_timer_lifecycle_pure(&rev);
        assert!(
            armed_rev.is_empty(),
            "cancel-then-await with no re-establishing fresh arm must wake now, got {armed_rev:?}"
        );
    }

    /// Round 9 FINDING: a reset-then-await in ONE task —
    /// `[CancelTimer(X), ArmTimer(X, for_await: false), ArmTimer(X, for_await: true)]`
    /// — must arm the durable row and contribute its deadline in THIS transaction.
    /// The fresh arm (`for_await: false`, from `reset`) re-establishes X *after* the
    /// cancel, so X is live at end-of-batch and the `await_fire` re-arm at index 2
    /// contributes. Before the round-9 fix the order-independent "cancel anywhere
    /// cancels the arm" rule dropped it, shifting the deadline to the next claim.
    #[test]
    fn plan_timer_lifecycle_pure_arms_a_reset_then_await_in_one_batch() {
        let commands = vec![
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("idle"),
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("idle"),
                duration_secs: 600,
                for_await: false,
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("idle"),
                duration_secs: 600,
                for_await: true,
            },
        ];
        let (events, armed) = plan_timer_lifecycle_pure(&commands);
        assert_eq!(
            armed,
            vec![2],
            "reset-then-await must arm the await re-arm at index 2, got {armed:?}"
        );
        // The cancel still emits TimerCancelled at its position; the fresh arm emits
        // TimerStarted; the await re-arm records no positional event.
        assert!(
            matches!(&events[0], Some(WorkflowEvent::TimerCancelled { timer_id }) if timer_id.as_str() == "idle"),
            "the reset's cancel must emit TimerCancelled, got {events:?}"
        );
        assert!(
            matches!(&events[1], Some(WorkflowEvent::TimerStarted { timer_id, .. }) if timer_id.as_str() == "idle"),
            "the reset's fresh arm must emit TimerStarted, got {events:?}"
        );
        assert!(
            events[2].is_none(),
            "the await re-arm records no positional event, got {events:?}"
        );
    }

    /// Round 11 FINDING: an await arm CANCELLED by a *later* same-batch reset must
    /// NOT contribute a firing row. Batch `[ArmTimer(X, for_await: true, old),
    /// CancelTimer(X), ArmTimer(X, for_await: false, new)]` (an `await_fire()`
    /// polled before a sibling `reset` in the same task): the await arm at index 0
    /// has a same-id `CancelTimer` after it, so it does NOT arm — the fresh
    /// `for_await: false` arm at index 2 re-establishes the logical Armed state but
    /// never contributes a firing row. `armed_indices` is therefore EMPTY → the
    /// parked task wakes NOW → live and replay both resolve `Cancelled` (replay
    /// sees the recorded `TimerCancelled` before the fresh arm's `TimerStarted`).
    /// The round-10 end-of-batch-liveness rule kept the await arm (X is live at
    /// end-of-batch because the fresh arm re-established it) and armed a firing row
    /// off the stale await duration → a live-vs-replay divergence.
    #[test]
    fn plan_timer_lifecycle_pure_excludes_an_await_arm_cancelled_by_a_later_reset() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                for_await: true,
            },
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("x"),
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 600,
                for_await: false,
            },
        ];
        let (events, armed) = plan_timer_lifecycle_pure(&commands);
        assert!(
            armed.is_empty(),
            "an await arm cancelled by a later same-batch reset must not contribute a \
             firing row (the fresh arm re-establishes state only), got {armed:?}"
        );
        assert!(
            events[0].is_none(),
            "the cancelled await re-arm records no positional event, got {events:?}"
        );
        assert!(
            matches!(&events[1], Some(WorkflowEvent::TimerCancelled { timer_id }) if timer_id.as_str() == "x"),
            "the reset's cancel must emit TimerCancelled, got {events:?}"
        );
        assert!(
            matches!(&events[2], Some(WorkflowEvent::TimerStarted { timer_id, .. }) if timer_id.as_str() == "x"),
            "the reset's fresh arm must emit TimerStarted, got {events:?}"
        );
    }

    /// Per-id order-sensitivity with two ids in one batch: one reset-then-await
    /// (arms) and one await-then-cancel (wakes now) must be resolved independently.
    #[test]
    fn plan_timer_lifecycle_pure_resolves_two_ids_independently_by_order() {
        let commands = vec![
            // id "keep": reset then await -> arms.
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("keep"),
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("keep"),
                duration_secs: 600,
                for_await: false,
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("keep"),
                duration_secs: 600,
                for_await: true,
            },
            // id "drop": await then a later sibling cancel -> wakes now.
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("drop"),
                duration_secs: 300,
                for_await: true,
            },
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("drop"),
            },
        ];
        let (_events, armed) = plan_timer_lifecycle_pure(&commands);
        assert_eq!(
            armed,
            vec![2],
            "only the reset-then-await id must arm; the await-then-cancel id is dropped, got {armed:?}"
        );
    }

    /// A `for_await: true` re-arm with NO same-batch cancel still contributes a
    /// deadline (the normal `await_fire` re-park path).
    #[test]
    fn plan_timer_lifecycle_pure_keeps_an_uncancelled_await_arm() {
        let commands = vec![WorkflowCommand::ArmTimer {
            timer_id: crate::types::TimerId::new("x"),
            duration_secs: 300,
            for_await: true,
        }];
        let (_ev, armed) = plan_timer_lifecycle_pure(&commands);
        assert_eq!(
            armed,
            vec![0],
            "an uncancelled await arm must contribute a deadline"
        );
    }

    /// An `ArmTimer` whose id also carries a `StartTimer` in the batch (the
    /// arm+`await_fire` same-cycle case) is owned by the suspension path
    /// (`persist_started_timer`); the arm helper emits nothing for it, or the
    /// event would be double-recorded.
    #[test]
    fn arm_events_skip_start_timer_owned_ids() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                for_await: false,
            },
            WorkflowCommand::StartTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                result_tx: oneshot::channel::<()>().0,
            },
        ];
        let events = arm_timer_events(&commands);
        assert!(
            events.iter().all(Option::is_none),
            "StartTimer-owned arm must emit nothing, got {events:?}"
        );
    }

    /// A LIVE cancellable arm colliding with a same-batch classic `StartTimer`
    /// for the same id is a hard invariant violation (issue #768, round 15): the
    /// arm's `TimerStarted` would be silently dropped, corrupting history. The
    /// persist guard must flag it.
    #[test]
    fn uncancelled_arm_start_collision_is_detected() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 30,
                for_await: false,
            },
            WorkflowCommand::StartTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 5,
                result_tx: oneshot::channel::<()>().0,
            },
        ];
        assert_eq!(
            same_batch_uncancelled_arm_start_collision(&commands).as_deref(),
            Some("x"),
            "uncancelled same-id arm + classic start must be flagged"
        );
    }

    /// Order-independence (issue #768, Codex P2 round 16): a classic `StartTimer(x)`
    /// placed BEFORE a same-id cancellable `ArmTimer(x)` in the batch is the reverse
    /// order of the prefix-only check, and must still be flagged.
    #[test]
    fn start_before_uncancelled_arm_collision_is_detected() {
        let commands = vec![
            WorkflowCommand::StartTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 5,
                result_tx: oneshot::channel::<()>().0,
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 30,
                for_await: false,
            },
        ];
        assert_eq!(
            same_batch_uncancelled_arm_start_collision(&commands).as_deref(),
            Some("x"),
            "classic-first then same-id cancellable arm must be flagged regardless of order"
        );
    }

    /// The legit cancel-then-classic pattern — `[ArmTimer(X,false),
    /// CancelTimer(X), StartTimer(X)]` — is NOT a collision: the arm was cancelled
    /// before the classic start, so the guard must stay silent.
    #[test]
    fn cancelled_arm_then_start_is_not_a_collision() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("idle"),
                duration_secs: 300,
                for_await: false,
            },
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("idle"),
            },
            WorkflowCommand::StartTimer {
                timer_id: crate::types::TimerId::new("idle"),
                duration_secs: 60,
                result_tx: oneshot::channel::<()>().0,
            },
        ];
        assert_eq!(
            same_batch_uncancelled_arm_start_collision(&commands),
            None,
            "cancel-then-classic reuse must not be flagged as a collision"
        );
    }

    /// A classic `StartTimer` whose id has no cancellable arm at all is fine.
    #[test]
    fn plain_start_timer_with_distinct_id_is_not_a_collision() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("a"),
                duration_secs: 30,
                for_await: false,
            },
            WorkflowCommand::StartTimer {
                timer_id: crate::types::TimerId::new("b"),
                duration_secs: 5,
                result_tx: oneshot::channel::<()>().0,
            },
        ];
        assert_eq!(
            same_batch_uncancelled_arm_start_collision(&commands),
            None,
            "distinct-id classic start must not collide with an arm"
        );
    }

    /// A same-cycle `reset` (`start_timer` then `handle.reset`) emits
    /// `[ArmTimer(x), CancelTimer(x), ArmTimer(x)]` (all fresh arms). Dedup:
    /// `TimerStarted` at BOTH arm positions, because the intervening `CancelTimer`
    /// clears the active state — a naive first-wins seen-set would wrongly drop the
    /// second `TimerStarted`. The `CancelTimer`'s `TimerCancelled` is emitted by
    /// the DB loop, not the pure arm helper, so its index stays `None` here.
    #[test]
    fn arm_events_reemit_after_intervening_cancel() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                for_await: false,
            },
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("x"),
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                for_await: false,
            },
        ];
        let events = arm_timer_events(&commands);
        assert!(
            matches!(&events[0], Some(WorkflowEvent::TimerStarted { .. })),
            "first arm emits, got {events:?}"
        );
        assert!(
            events[1].is_none(),
            "CancelTimer emission stays with the DB loop, not the arm helper: {events:?}"
        );
        assert!(
            matches!(&events[2], Some(WorkflowEvent::TimerStarted { .. })),
            "re-arm after a cancel must re-emit TimerStarted, got {events:?}"
        );
    }

    /// A re-arm of an id still active in the batch (no intervening cancel) emits
    /// nothing on the second arm — in-batch first-arm dedup.
    #[test]
    fn arm_events_dedup_repeat_arm_without_cancel() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                for_await: false,
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                for_await: false,
            },
        ];
        let events = arm_timer_events(&commands);
        assert!(
            matches!(&events[0], Some(WorkflowEvent::TimerStarted { .. })),
            "first arm emits, got {events:?}"
        );
        assert!(
            events[1].is_none(),
            "a repeat arm of an active id emits nothing, got {events:?}"
        );
    }

    /// Codex P2 (issue #768): a reset batch (`CancelTimer` + `ArmTimer`) appends
    /// two durable timer-lifecycle events (`TimerCancelled` + `TimerStarted`), so
    /// the history hard-cap preflight must count 2 — otherwise a near-cap reset
    /// slips past the `>= cap` check and breaches the hard cap.
    #[test]
    fn timer_lifecycle_count_counts_reset_batch_as_two() {
        let commands = vec![
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("idle"),
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("idle"),
                duration_secs: 300,
                for_await: false,
            },
        ];
        assert_eq!(timer_lifecycle_event_count(&commands), 2);
        // The shared preflight accessor must reflect it too (no other bookkeeping
        // in this batch).
        assert_eq!(pre_suspension_event_count(&commands), 2);
    }

    /// An `ArmTimer` whose id also carries a `StartTimer` in the batch is owned by
    /// the suspension path and counted by `extract_started_timer_for_suspension`;
    /// counting it here too would double-count, so the timer-lifecycle count
    /// excludes it. A `CancelTimer` for a *different* id still counts.
    #[test]
    fn timer_lifecycle_count_excludes_start_timer_owned_arm() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                for_await: false,
            },
            WorkflowCommand::StartTimer {
                timer_id: crate::types::TimerId::new("x"),
                duration_secs: 300,
                result_tx: oneshot::channel::<()>().0,
            },
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("other"),
            },
        ];
        // ArmTimer(x) excluded (StartTimer-owned); CancelTimer(other) counts.
        assert_eq!(timer_lifecycle_event_count(&commands), 1);
    }

    /// A batch with no timer bookkeeping counts zero timer-lifecycle events
    /// (backward-compatible: non-timer batches are unaffected).
    #[test]
    fn timer_lifecycle_count_is_zero_without_timer_commands() {
        let commands = vec![WorkflowCommand::RecordMarker {
            name: "m".to_string(),
            details: serde_json::Value::Null,
        }];
        assert_eq!(timer_lifecycle_event_count(&commands), 0);
        // But RecordMarker still counts as a pre-suspension event.
        assert_eq!(pre_suspension_event_count(&commands), 1);
    }

    /// A `start_timer` immediately followed same-cycle by a `new_uuid`/
    /// `system_now`/`random_*`/`side_effect` (guardrail-recommended primitives)
    /// emits `[ArmTimer(id), RecordSideEffect(..)]`. The recorded history MUST
    /// interleave `TimerStarted` at the `ArmTimer`'s command position — i.e.
    /// BEFORE `SideEffectRecorded` — because `replay::match_timer_arm` is strictly
    /// positional and `drain_early_signals` does not skip `SideEffectRecorded`.
    /// The pre-FINDING-1 worker appended timer-lifecycle events at the END of the
    /// batch, recording `[SideEffectRecorded, TimerStarted]` and nd-blocking the
    /// run on first resume.
    #[test]
    fn build_suspension_events_interleaves_timer_started_before_side_effect() {
        let commands = vec![
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("idle"),
                duration_secs: 300,
                for_await: false,
            },
            WorkflowCommand::RecordSideEffect {
                kind: crate::event::SideEffectKind::Uuid,
                name: None,
                value: serde_json::json!("018f-uuid"),
            },
        ];
        // The DB-mutation phase (plan_timer_lifecycle) resolved the ArmTimer at
        // index 0 to a newly-created row → emit TimerStarted at that position.
        let mut timer_events = vec![
            Some(WorkflowEvent::TimerStarted {
                timer_id: crate::types::TimerId::new("idle"),
                duration_secs: 300,
            }),
            None,
        ];
        let events = build_suspension_events(&commands, &mut timer_events, |_| None);
        assert_eq!(events.len(), 2, "got {events:?}");
        assert!(
            matches!(events[0], WorkflowEvent::TimerStarted { .. }),
            "TimerStarted must be at the ArmTimer position (index 0), got {events:?}"
        );
        assert!(
            matches!(events[1], WorkflowEvent::SideEffectRecorded { .. }),
            "SideEffectRecorded must follow TimerStarted, got {events:?}"
        );
    }

    /// A `reset` in the same cycle as a marker: `[RecordMarker, CancelTimer,
    /// ArmTimer]` must record `[MarkerRecorded, TimerCancelled, TimerStarted]`
    /// at each command's position.
    #[test]
    fn build_suspension_events_interleaves_reset_cancel_then_arm_in_position() {
        let commands = vec![
            WorkflowCommand::RecordMarker {
                name: "m".to_string(),
                details: serde_json::json!(1),
            },
            WorkflowCommand::CancelTimer {
                timer_id: crate::types::TimerId::new("idle"),
            },
            WorkflowCommand::ArmTimer {
                timer_id: crate::types::TimerId::new("idle"),
                duration_secs: 300,
                for_await: false,
            },
        ];
        let mut timer_events = vec![
            None,
            Some(WorkflowEvent::TimerCancelled {
                timer_id: crate::types::TimerId::new("idle"),
            }),
            Some(WorkflowEvent::TimerStarted {
                timer_id: crate::types::TimerId::new("idle"),
                duration_secs: 300,
            }),
        ];
        let events = build_suspension_events(&commands, &mut timer_events, |_| None);
        assert!(
            matches!(events[0], WorkflowEvent::MarkerRecorded { .. }),
            "got {events:?}"
        );
        assert!(
            matches!(events[1], WorkflowEvent::TimerCancelled { .. }),
            "got {events:?}"
        );
        assert!(
            matches!(events[2], WorkflowEvent::TimerStarted { .. }),
            "got {events:?}"
        );
    }

    // ── ctx.race() bookkeeping ignore-lists (issue #600) ────────────────────

    #[test]
    fn only_bookkeeping_commands_accepts_race_winner_plus_cancel_losers() {
        let commands = vec![
            WorkflowCommand::RecordMarker {
                name: "race_winner:1".to_string(),
                details: serde_json::json!(0),
            },
            WorkflowCommand::CancelRaceLosers {
                activities: vec![crate::types::ActivityExecId::new()],
                children: vec![],
                timers: vec![],
            },
        ];
        assert!(only_bookkeeping_commands(&commands));
    }

    #[test]
    fn extract_all_scheduled_activities_tolerates_cancel_race_losers() {
        let (tx, _rx) = oneshot::channel::<Result<serde_json::Value, String>>();
        let commands = vec![
            WorkflowCommand::RecordMarker {
                name: "race:1".to_string(),
                details: serde_json::json!(2),
            },
            WorkflowCommand::CancelRaceLosers {
                activities: vec![crate::types::ActivityExecId::new()],
                children: vec![],
                timers: vec![],
            },
            WorkflowCommand::ScheduleActivity {
                activity_id: crate::types::ActivityExecId::new(),
                name: "fetch_primary".to_string(),
                input: serde_json::Value::Null,
                queue: "default".to_string(),
                retry_policy_override: None,
                start_to_close_override: None,
                session_id: None,
                session_worker_id: None,
                schedule_to_start_override: None,
                result_tx: tx,
            },
        ];
        let scheduled = extract_all_scheduled_activities(&commands);
        assert!(scheduled.is_some());
        assert_eq!(scheduled.unwrap().len(), 1);
    }

    #[test]
    fn extract_all_scheduled_activities_preserves_session_fields() {
        let (tx, _rx) = oneshot::channel::<Result<serde_json::Value, String>>();
        let session_id = crate::types::SessionId::new();
        let commands = vec![WorkflowCommand::ScheduleActivity {
            activity_id: crate::types::ActivityExecId::new(),
            name: "transcode_chunk".to_string(),
            input: serde_json::Value::Null,
            queue: "gpu-workers".to_string(),
            retry_policy_override: None,
            start_to_close_override: None,
            session_id: Some(session_id),
            session_worker_id: Some("worker-7".to_string()),
            schedule_to_start_override: Some(std::time::Duration::from_secs(30)),
            result_tx: tx,
        }];
        let scheduled = extract_all_scheduled_activities(&commands)
            .expect("a single ScheduleActivity command must extract");
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].session_id, Some(session_id));
        assert_eq!(scheduled[0].session_worker_id.as_deref(), Some("worker-7"));
        assert_eq!(
            scheduled[0].schedule_to_start_override,
            Some(std::time::Duration::from_secs(30))
        );
    }

    #[test]
    fn extract_all_activity_waits_tolerates_cancel_race_losers() {
        let commands = vec![
            WorkflowCommand::CancelRaceLosers {
                activities: vec![crate::types::ActivityExecId::new()],
                children: vec![],
                timers: vec![],
            },
            WorkflowCommand::WaitForActivity {
                activity_id: crate::types::ActivityExecId::new(),
                result_tx: oneshot::channel::<Result<serde_json::Value, String>>().0,
            },
        ];
        assert!(extract_all_activity_waits(&commands).is_some());
    }

    #[test]
    fn workflow_command_name_covers_cancel_race_losers() {
        let cmd = WorkflowCommand::CancelRaceLosers {
            activities: vec![],
            children: vec![],
            timers: vec![],
        };
        assert_eq!(workflow_command_name(&cmd), "CancelRaceLosers");
    }

    // ── extract_child_timeout_race (issue #779) ──────────────────────────────

    /// Build the canonical child-timeout suspension batch: exactly one
    /// `StartChildWorkflow` followed by exactly one `StartTimer` — the shape
    /// `spawn_child_workflow_timeout` emits on the live/re-park path.
    fn child_timeout_batch() -> Vec<WorkflowCommand> {
        vec![
            WorkflowCommand::StartChildWorkflow {
                child_id: ExecutionId::new(),
                workflow_name: "timeout_child".to_string(),
                input: serde_json::json!({"id": 1}),
                result_tx: oneshot::channel::<Result<serde_json::Value, String>>().0,
            },
            WorkflowCommand::StartTimer {
                timer_id: TimerId::new("__child_timeout:1:timeout_child"),
                duration_secs: 300,
                result_tx: oneshot::channel::<()>().0,
            },
        ]
    }

    #[test]
    fn extract_child_timeout_race_accepts_one_child_one_timer_plus_bookkeeping() {
        // The exact shape spawn_child_workflow_timeout emits, plus a tolerated
        // bookkeeping side-effect (e.g. a prior ctx.system_now()) and a
        // CancelRaceLosers from a prior sequential race.
        let mut commands = vec![WorkflowCommand::RecordSideEffect {
            kind: crate::event::SideEffectKind::Now,
            name: None,
            value: serde_json::json!("2026-07-11T00:00:00Z"),
        }];
        commands.extend(child_timeout_batch());
        commands.push(WorkflowCommand::CancelRaceLosers {
            activities: vec![],
            children: vec![],
            timers: vec![],
        });

        let extracted = extract_child_timeout_race(&commands)
            .expect("one child + one timer + bookkeeping must extract");
        assert_eq!(extracted.0.workflow_name, "timeout_child");
        assert_eq!(
            extracted.1.timer_id.as_str(),
            "__child_timeout:1:timeout_child"
        );
        assert_eq!(extracted.1.duration_secs, 300);
    }

    #[test]
    fn extract_child_timeout_race_rejects_extra_activity_or_second_child() {
        // A new ScheduleActivity riding the batch must NOT match — this is the
        // AC9 caveat enforced by construction (fall through to fail-loud).
        let mut with_activity = child_timeout_batch();
        with_activity.push(WorkflowCommand::ScheduleActivity {
            activity_id: crate::types::ActivityExecId::new(),
            name: "send_email".to_string(),
            input: serde_json::Value::Null,
            queue: "default".to_string(),
            retry_policy_override: None,
            start_to_close_override: None,
            session_id: None,
            session_worker_id: None,
            schedule_to_start_override: None,
            result_tx: oneshot::channel::<Result<serde_json::Value, String>>().0,
        });
        assert!(
            extract_child_timeout_race(&with_activity).is_none(),
            "a child-timeout batch with an extra activity must not match"
        );

        // A second StartChildWorkflow means a plain parallel child fan-out, not a
        // child-timeout race.
        let mut with_second_child = child_timeout_batch();
        with_second_child.push(WorkflowCommand::StartChildWorkflow {
            child_id: ExecutionId::new(),
            workflow_name: "other_child".to_string(),
            input: serde_json::Value::Null,
            result_tx: oneshot::channel::<Result<serde_json::Value, String>>().0,
        });
        assert!(
            extract_child_timeout_race(&with_second_child).is_none(),
            "a batch with a second child must not match the child-timeout race"
        );
    }

    #[test]
    fn extract_child_timeout_race_rejects_second_timer_or_signal_wait() {
        // A second StartTimer means this is not the canonical one-child /
        // one-timer race — fall through to fail-loud rather than silently
        // dropping the extra timer's event.
        let mut with_second_timer = child_timeout_batch();
        with_second_timer.push(WorkflowCommand::StartTimer {
            timer_id: TimerId::new("__child_timeout:2:timeout_child"),
            duration_secs: 60,
            result_tx: oneshot::channel::<()>().0,
        });
        assert!(
            extract_child_timeout_race(&with_second_timer).is_none(),
            "a batch with a second timer must not match the child-timeout race"
        );

        // A WaitForSignal riding the batch is the signal-or-deadline shape
        // (issue #476), not a child-timeout race — it must not match here.
        let mut with_signal_wait = child_timeout_batch();
        with_signal_wait.push(WorkflowCommand::WaitForSignal {
            signal_name: "approval".to_string(),
            result_tx: oneshot::channel::<serde_json::Value>().0,
        });
        assert!(
            extract_child_timeout_race(&with_signal_wait).is_none(),
            "a child-timeout batch carrying a WaitForSignal must not match"
        );
    }

    #[test]
    fn extract_child_timeout_race_requires_reserved_timer_prefix() {
        // (a) A `__child_timeout:`-prefixed timer STILL matches (the real
        // primitive): child_timeout_batch() uses the reserved prefix.
        assert!(
            extract_child_timeout_race(&child_timeout_batch()).is_some(),
            "the reserved-prefix child-timeout batch must still match"
        );

        // (b) An ORDINARY timer id (a hand-rolled
        // `tokio::join!(spawn_child_workflow(..), timer("mytimer", n))` batch)
        // must be REJECTED — it must fall through to the generic fail-loud
        // "unsupported commands" path, exactly as before #779, so its ordinary
        // timer is never silently left undeleted on a child-win.
        let ordinary = vec![
            WorkflowCommand::StartChildWorkflow {
                child_id: ExecutionId::new(),
                workflow_name: "timeout_child".to_string(),
                input: serde_json::json!({"id": 1}),
                result_tx: oneshot::channel::<Result<serde_json::Value, String>>().0,
            },
            WorkflowCommand::StartTimer {
                timer_id: TimerId::new("mytimer"),
                duration_secs: 5,
                result_tx: oneshot::channel::<()>().0,
            },
        ];
        assert!(
            extract_child_timeout_race(&ordinary).is_none(),
            "a child + ordinary (non-`__child_timeout:`) timer must NOT match the \
             child-timeout race"
        );
    }

    #[test]
    fn extract_run_local_activity_preserves_detached_after_local_command() {
        let child_id = ExecutionId::new();
        let commands = vec![
            WorkflowCommand::RunLocalActivity {
                activity_id: crate::types::ActivityExecId::new(),
                name: "format_data".to_string(),
                input: serde_json::Value::Null,
                start_to_close: None,
                retry_policy: None,
                result_tx: oneshot::channel::<Result<serde_json::Value, String>>().0,
                already_scheduled: false,
                failed_attempts: 0,
                last_error: None,
            },
            WorkflowCommand::SpawnDetachedChildWorkflow {
                child_id,
                workflow_name: "monitor".to_string(),
                input: serde_json::Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
        ];

        let batch = extract_run_local_activity(commands);

        assert!(batch.pre_schedule_events.is_empty());
        assert_eq!(batch.run.name, "format_data");
        assert!(
            matches!(
                batch.post_schedule_events.as_slice(),
                [WorkflowEvent::ChildWorkflowSpawnedDetached {
                    child_id: recorded_child_id,
                    workflow_name,
                    input,
                    parent_close_policy: ParentClosePolicy::Abandon,
                }] if *recorded_child_id == child_id
                    && workflow_name == "monitor"
                    && *input == serde_json::Value::Null
            ),
            "detached spawn emitted after RunLocalActivity must be written after LocalActivityScheduled"
        );
    }

    #[test]
    fn extract_run_local_activity_preserves_detached_before_local_command() {
        let child_id = ExecutionId::new();
        let commands = vec![
            WorkflowCommand::SpawnDetachedChildWorkflow {
                child_id,
                workflow_name: "monitor".to_string(),
                input: serde_json::Value::Null,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
            WorkflowCommand::RunLocalActivity {
                activity_id: crate::types::ActivityExecId::new(),
                name: "format_data".to_string(),
                input: serde_json::Value::Null,
                start_to_close: None,
                retry_policy: None,
                result_tx: oneshot::channel::<Result<serde_json::Value, String>>().0,
                already_scheduled: false,
                failed_attempts: 0,
                last_error: None,
            },
        ];

        let batch = extract_run_local_activity(commands);

        assert!(batch.post_schedule_events.is_empty());
        assert!(
            matches!(
                batch.pre_schedule_events.as_slice(),
                [WorkflowEvent::ChildWorkflowSpawnedDetached {
                    child_id: recorded_child_id,
                    workflow_name,
                    input,
                    parent_close_policy: ParentClosePolicy::Abandon,
                }] if *recorded_child_id == child_id
                    && workflow_name == "monitor"
                    && *input == serde_json::Value::Null
            ),
            "detached spawn emitted before RunLocalActivity must stay before LocalActivityScheduled"
        );
    }

    #[test]
    fn havoc_chrono_duration_panic() {
        let max_safe_secs = i64::MAX as u64;
        let result = chrono_duration_from_secs(max_safe_secs, "timeout");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("exceeds chrono::Duration bounds")
        );
    }

    #[test]
    fn test_worker_ineligible_activities() {
        let act1 = ActivityInfo {
            name: "act_gpu",
            module: "app::activities",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            requires: Some("gpu = true"),
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };

        let act2 = ActivityInfo {
            name: "act_cpu",
            module: "app::activities",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            requires: Some("region in [us-east-1, us-west-2]"),
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };

        let registry = Arc::new(HandlerRegistry::new(vec![], vec![act1, act2]));

        // Worker with gpu = true, region = us-east-1
        let mut labels = std::collections::HashMap::new();
        labels.insert("gpu".to_string(), "true".to_string());
        labels.insert("region".to_string(), "us-east-1".to_string());
        let cfg = WorkerRuntimeConfig {
            labels,
            ..default_runtime_config()
        };
        let worker = Worker::new(cfg, registry.clone()).unwrap();
        assert!(worker.ineligible_activities.is_empty());

        // Worker with cpu only, region = us-east-1 (act_gpu is ineligible)
        let mut labels = std::collections::HashMap::new();
        labels.insert("region".to_string(), "us-east-1".to_string());
        let cfg = WorkerRuntimeConfig {
            labels,
            ..default_runtime_config()
        };
        let worker = Worker::new(cfg, registry.clone()).unwrap();
        assert_eq!(worker.ineligible_activities, vec!["act_gpu".to_string()]);

        // Worker with gpu = true, region = eu-west-1 (act_cpu is ineligible)
        let mut labels = std::collections::HashMap::new();
        labels.insert("gpu".to_string(), "true".to_string());
        labels.insert("region".to_string(), "eu-west-1".to_string());
        let cfg = WorkerRuntimeConfig {
            labels,
            ..default_runtime_config()
        };
        let worker = Worker::new(cfg, registry).unwrap();
        assert_eq!(worker.ineligible_activities, vec!["act_cpu".to_string()]);
    }

    // -----------------------------------------------------------------------
    // Non-terminal replay-non-determinism block (issue #603)
    // -----------------------------------------------------------------------

    #[test]
    fn nd_block_backoff_starts_at_base_and_doubles() {
        assert_eq!(nd_block_backoff(0), Duration::from_secs(5));
        assert_eq!(nd_block_backoff(1), Duration::from_secs(10));
        assert_eq!(nd_block_backoff(2), Duration::from_secs(20));
        assert_eq!(nd_block_backoff(3), Duration::from_secs(40));
        assert_eq!(nd_block_backoff(5), Duration::from_secs(160));
    }

    #[test]
    fn nd_block_backoff_caps_at_five_minutes() {
        // 5 * 2^6 = 320 > 300 — first count that hits the cap.
        assert_eq!(nd_block_backoff(6), Duration::from_secs(300));
        assert_eq!(nd_block_backoff(20), Duration::from_secs(300));
    }

    #[test]
    fn nd_block_backoff_saturates_on_extreme_counts() {
        // Negative counts (impossible via the DB column but defensive) clamp
        // to the base; huge counts must not overflow the shift.
        assert_eq!(nd_block_backoff(-1), Duration::from_secs(5));
        assert_eq!(nd_block_backoff(i32::MAX), Duration::from_secs(300));
    }

    // -----------------------------------------------------------------------
    // Contained workflow handler-panic retry (issue #782)
    // -----------------------------------------------------------------------

    #[test]
    fn panic_retry_backoff_starts_at_base_and_doubles() {
        assert_eq!(panic_retry_backoff(1), Duration::from_secs(1));
        assert_eq!(panic_retry_backoff(2), Duration::from_secs(2));
        assert_eq!(panic_retry_backoff(3), Duration::from_secs(4));
        assert_eq!(panic_retry_backoff(4), Duration::from_secs(8));
    }

    #[test]
    fn panic_retry_backoff_caps_and_clamps() {
        // 1 * 2^5 = 32 > 30 — first strike that hits the cap.
        assert_eq!(panic_retry_backoff(6), Duration::from_secs(30));
        assert_eq!(panic_retry_backoff(u32::MAX), Duration::from_secs(30));
        // A degenerate `0` clamps to attempt 1 (base delay), not a zero-length
        // hot-loop.
        assert_eq!(panic_retry_backoff(0), Duration::from_secs(1));
    }

    #[test]
    fn panic_retry_decision_truth_table() {
        // Default budget of 3: panics 1 and 2 re-dispatch, panic 3 is terminal.
        assert_eq!(panic_retry_decision(1, 3), PanicRetryDecision::Requeue);
        assert_eq!(panic_retry_decision(2, 3), PanicRetryDecision::Requeue);
        assert_eq!(panic_retry_decision(3, 3), PanicRetryDecision::Terminal);
        assert_eq!(panic_retry_decision(4, 3), PanicRetryDecision::Terminal);
        // max == 0 disables panic-retry: the first panic is terminal.
        assert_eq!(panic_retry_decision(1, 0), PanicRetryDecision::Terminal);
        // max == 1 is terminal on the first strike.
        assert_eq!(panic_retry_decision(1, 1), PanicRetryDecision::Terminal);
    }

    #[test]
    fn handler_panic_activity_envelope_is_retryable_handler_panic() {
        // A contained activity panic must encode as a *retryable* typed
        // HandlerPanic failure so it follows the retry policy (issue #782 AC1).
        let payload = handler_panic_activity_envelope("boom".to_string());
        let failure = crate::failure::parse_error_payload_full(&payload);
        assert_eq!(failure.error_type, crate::failure::ERROR_TYPE_HANDLER_PANIC);
        assert_eq!(failure.message, "boom");
        assert!(!failure.non_retryable);
    }

    #[test]
    fn activity_handler_construction_panic_is_contained_as_retryable_handler_panic() {
        // Issue #782 / PR #1012 review: a hand-written activity handler `fn` (the
        // supported public surface) that panics while *constructing* its future —
        // synchronous work before returning `Box::pin(...)` — is caught by the
        // dispatch sites' `catch_construct` guard, which the poll-time
        // `catch_unwind` cannot reach. Both the regular- and local-activity paths
        // then feed the extracted message into `handler_panic_activity_envelope`,
        // so a construction panic and a poll panic are byte-identical downstream:
        // the same retryable typed HandlerPanic failure (which
        // `handle_activity_result` counts once via the error-type check).
        fn constructing_panic_activity(
            _ctx: &crate::context::ActivityContext,
            _input: serde_json::Value,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + '_>,
        > {
            panic!("boom during activity future construction");
        }

        let handler: crate::info::ActivityHandlerFn = constructing_panic_activity;
        let ctx = crate::context::ActivityContext::new_test();
        // Match rather than `.expect_err()`: the `Ok` future variant is not
        // `Debug`, so `.expect_err()` would not compile.
        let message = match crate::error::catch_construct(|| handler(&ctx, serde_json::Value::Null))
        {
            Ok(_fut) => panic!("a construction-phase panic must be caught, not produce a future"),
            Err(message) => message,
        };
        assert_eq!(message, "boom during activity future construction");

        let payload = handler_panic_activity_envelope(message);
        let failure = crate::failure::parse_error_payload_full(&payload);
        assert_eq!(failure.error_type, crate::failure::ERROR_TYPE_HANDLER_PANIC);
        assert_eq!(failure.message, "boom during activity future construction");
        assert!(
            !failure.non_retryable,
            "a contained construction panic remains retryable, like a poll panic"
        );
    }

    /// Property tests for the private [`nd_block_backoff`] helper (issue #603).
    /// Kept in-crate because the fn is private and `worker` is `db`-gated, so it
    /// cannot be reached from `tests/property/`. `nd_block_backoff` is a thin
    /// wrapper over [`crate::policy::compute_retry_delay`] (covered directly by
    /// the `tests/property/policy_props.rs` suite); these properties pin its own
    /// cap / monotonicity / totality contract.
    ///
    /// `PROPTEST_CASES` overrides the (low) default case count; on-disk failure
    /// persistence is disabled to keep CI runners artifact-free.
    mod nd_block_backoff_props {
        use super::nd_block_backoff;
        use proptest::prelude::*;
        use std::time::Duration;

        const CAP: Duration = Duration::from_secs(300);

        fn config() -> proptest::test_runner::Config {
            let cases = std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|&c| c > 0)
                .unwrap_or(128);
            proptest::test_runner::Config {
                cases,
                failure_persistence: None,
                ..proptest::test_runner::Config::default()
            }
        }

        proptest! {
            #![proptest_config(config())]

            /// Never exceeds the 300s cap and never panics — for any `i32`,
            /// including `i32::MIN` and `i32::MAX`.
            #[test]
            fn never_exceeds_cap(count in any::<i32>()) {
                prop_assert!(nd_block_backoff(count) <= CAP);
            }

            /// Monotonic non-decreasing in `block_count` over the non-negative
            /// range (capped ties satisfy `<=`).
            #[test]
            fn monotonic_non_decreasing(count in 0i32..1_000) {
                prop_assert!(nd_block_backoff(count) <= nd_block_backoff(count + 1));
            }

            /// Counts at/above the cap threshold saturate exactly at the cap.
            #[test]
            fn large_counts_saturate(count in 6i32..=i32::MAX) {
                prop_assert_eq!(nd_block_backoff(count), CAP);
            }
        }
    }

    #[test]
    fn apply_raw_search_attrs_patch_in_memory_inserts_and_removes() {
        let base = Some(serde_json::json!({"tenant": "acme", "build_id": "v1"}));
        let mut patch = std::collections::HashMap::new();
        patch.insert("build_id".to_string(), Some(serde_json::json!("v2")));
        patch.insert("failure_cause".to_string(), None);
        let result = apply_raw_search_attrs_patch_in_memory(base, &patch);
        assert_eq!(
            result,
            Some(serde_json::json!({"tenant": "acme", "build_id": "v2"}))
        );
    }

    #[test]
    fn apply_raw_search_attrs_patch_in_memory_empty_patch_is_noop() {
        let base = Some(serde_json::json!({"tenant": "acme"}));
        let result =
            apply_raw_search_attrs_patch_in_memory(base.clone(), &std::collections::HashMap::new());
        assert_eq!(result, base);
    }

    #[test]
    fn apply_raw_search_attrs_patch_in_memory_removing_everything_stays_some_empty_object() {
        // Byte-for-byte parity with `store::update_search_attrs`, which always
        // writes `Some(new_attrs)` and never collapses to SQL NULL — unlike
        // the sibling `apply_search_attrs_patch_in_memory` (command-sourced),
        // which does collapse an empty result to `None`.
        let base = Some(serde_json::json!({"failure_cause": "non_determinism"}));
        let mut patch = std::collections::HashMap::new();
        patch.insert("failure_cause".to_string(), None);
        let result = apply_raw_search_attrs_patch_in_memory(base, &patch);
        assert_eq!(result, Some(serde_json::json!({})));
    }

    #[test]
    fn apply_raw_search_attrs_patch_in_memory_none_base_with_inserts_builds_object() {
        let mut patch = std::collections::HashMap::new();
        patch.insert("build_id".to_string(), Some(serde_json::json!("v1")));
        let result = apply_raw_search_attrs_patch_in_memory(None, &patch);
        assert_eq!(result, Some(serde_json::json!({"build_id": "v1"})));
    }

    #[test]
    fn nd_search_attrs_patch_full_details_stamps_all_six_keys() {
        let details = crate::error::NonDeterministicDetails {
            event_index: Some(7),
            expected: Some("ActivityScheduled".to_string()),
            actual: Some("TimerStarted".to_string()),
            workflow_type: Some("onboarding".to_string()),
            build_id: Some("v2.0.0".to_string()),
        };
        let patch = nd_search_attrs_patch(&details);
        assert_eq!(
            patch.get("failure_cause"),
            Some(&Some(serde_json::json!("non_determinism")))
        );
        assert_eq!(patch.get("event_index"), Some(&Some(serde_json::json!(7))));
        assert_eq!(
            patch.get("expected"),
            Some(&Some(serde_json::json!("ActivityScheduled")))
        );
        assert_eq!(
            patch.get("actual"),
            Some(&Some(serde_json::json!("TimerStarted")))
        );
        assert_eq!(
            patch.get("workflow_type"),
            Some(&Some(serde_json::json!("onboarding")))
        );
        assert_eq!(
            patch.get("build_id"),
            Some(&Some(serde_json::json!("v2.0.0")))
        );
        assert_eq!(patch.len(), 6);
    }

    #[test]
    fn nd_search_attrs_patch_sparse_details_stamps_only_failure_cause() {
        let details = crate::error::NonDeterministicDetails {
            event_index: None,
            expected: None,
            actual: None,
            workflow_type: None,
            build_id: None,
        };
        let patch = nd_search_attrs_patch(&details);
        assert_eq!(
            patch.get("failure_cause"),
            Some(&Some(serde_json::json!("non_determinism")))
        );
        assert_eq!(patch.len(), 1);
    }

    #[test]
    fn nd_search_attrs_clear_patch_removes_every_key_the_stamp_can_set() {
        // Key symmetry: recovery must delete exactly the key set the block
        // path can stamp, so a recovered execution carries no stale ND
        // diagnostic in search_attrs.
        let clear = nd_search_attrs_clear_patch();
        let full = nd_search_attrs_patch(&crate::error::NonDeterministicDetails {
            event_index: Some(1),
            expected: Some("e".to_string()),
            actual: Some("a".to_string()),
            workflow_type: Some("w".to_string()),
            build_id: Some("b".to_string()),
        });
        let mut clear_keys: Vec<&str> = clear.keys().map(String::as_str).collect();
        let mut stamp_keys: Vec<&str> = full.keys().map(String::as_str).collect();
        clear_keys.sort_unstable();
        stamp_keys.sort_unstable();
        assert_eq!(clear_keys, stamp_keys);
        // Every value in the clear patch is None (= delete the key).
        assert!(clear.values().all(Option::is_none));
    }

    #[test]
    #[should_panic(expected = "must be gated earlier in process_workflow_task")]
    fn schedule_counter_action_asserts_if_nd_carrying_failed_ever_reaches_it() {
        // Locks in the debug_assert! added as a code-review fix: this arm is
        // provably unreachable in production today (the early gate in
        // process_workflow_task always intercepts first), but if a future
        // regression ever routes an ND-carrying Failed outcome here, this
        // must panic loudly in debug/test builds rather than silently
        // returning None.
        let outcome = WorkflowOutcome::Failed {
            error: "non-deterministic replay: test".to_string(),
            handler_panic: false,
            unhandled_signals: std::collections::BTreeMap::new(),
            non_deterministic_details: Some(crate::error::NonDeterministicDetails {
                event_index: Some(0),
                expected: None,
                actual: None,
                workflow_type: None,
                build_id: None,
            }),
        };
        let _ = schedule_counter_action(&outcome);
    }

    // ── schedule_to_close retry-path deadline decision (issues #378 × #609) ─

    #[test]
    fn deadline_would_be_exceeded_none_is_unbounded() {
        let now = chrono::Utc::now();
        assert!(!deadline_would_be_exceeded(
            None,
            now,
            chrono::Duration::seconds(300)
        ));
    }

    #[test]
    fn deadline_would_be_exceeded_true_when_retry_lands_past_deadline() {
        let now = chrono::Utc::now();
        assert!(deadline_would_be_exceeded(
            Some(now + chrono::Duration::seconds(30)),
            now,
            chrono::Duration::seconds(300)
        ));
    }

    #[test]
    fn deadline_would_be_exceeded_false_after_a_resume_shift() {
        // Finding 1 (issue #609 post-review hardening): a pause/resume cycle
        // completing while the attempt was in flight shifts the row's
        // deadline forward; the fresh (shifted) value must clear the check
        // even though the stale claim-time snapshot would not.
        let now = chrono::Utc::now();
        let stale_snapshot = Some(now - chrono::Duration::seconds(1));
        let shifted_fresh = Some(now + chrono::Duration::hours(1));
        let delay = chrono::Duration::seconds(300);
        assert!(deadline_would_be_exceeded(stale_snapshot, now, delay));
        assert!(!deadline_would_be_exceeded(shifted_fresh, now, delay));
    }

    // ── schedule_to_close in-transaction re-check (issue #609, round 2) ─────

    #[test]
    fn recheck_missing_task_row_is_handled() {
        let now = chrono::Utc::now();
        assert_eq!(
            schedule_to_close_recheck_outcome("RUNNING", None, now, chrono::Duration::seconds(1)),
            Some(ScheduleToCloseTimeoutOutcome::Handled)
        );
    }

    #[test]
    fn recheck_concurrently_resolved_task_is_handled_even_when_paused() {
        // A task another writer already resolved must never be requeued —
        // Handled wins over the PAUSED check.
        let now = chrono::Utc::now();
        let row = ("FAILED".to_string(), Some(now - chrono::Duration::hours(1)));
        assert_eq!(
            schedule_to_close_recheck_outcome(
                "PAUSED",
                Some(&row),
                now,
                chrono::Duration::seconds(300)
            ),
            Some(ScheduleToCloseTimeoutOutcome::Handled)
        );
    }

    #[test]
    fn recheck_paused_execution_wins_over_an_exceeded_row_deadline() {
        // The P2 race this round closes: a pause committing after the
        // caller's non-locking gate but before this transaction's lock.
        // Both the snapshot and the row-current deadline are exceeded, so
        // only the locked PAUSED re-check can save the task from being
        // deadline-failed mid-pause.
        let now = chrono::Utc::now();
        let row = (
            "RUNNING".to_string(),
            Some(now - chrono::Duration::seconds(1)),
        );
        assert_eq!(
            schedule_to_close_recheck_outcome(
                "PAUSED",
                Some(&row),
                now,
                chrono::Duration::seconds(300)
            ),
            Some(ScheduleToCloseTimeoutOutcome::ExecutionPaused)
        );
    }

    #[test]
    fn recheck_shifted_deadline_aborts_the_timeout() {
        let now = chrono::Utc::now();
        let row = (
            "RUNNING".to_string(),
            Some(now + chrono::Duration::hours(1)),
        );
        assert_eq!(
            schedule_to_close_recheck_outcome(
                "RUNNING",
                Some(&row),
                now,
                chrono::Duration::seconds(300)
            ),
            Some(ScheduleToCloseTimeoutOutcome::DeadlineShifted)
        );
    }

    #[test]
    fn recheck_running_with_exceeded_deadline_proceeds_to_enforce() {
        let now = chrono::Utc::now();
        let row = (
            "RUNNING".to_string(),
            Some(now - chrono::Duration::seconds(1)),
        );
        assert_eq!(
            schedule_to_close_recheck_outcome(
                "RUNNING",
                Some(&row),
                now,
                chrono::Duration::seconds(300)
            ),
            None
        );
    }

    // ── collect_update_result_metrics (issue #684) ────────────────────────

    fn admitted(update_id: crate::types::UpdateId, name: &str) -> WorkflowEvent {
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: name.to_string(),
            input: Value::Null,
            timestamp: chrono::Utc::now(),
        }
    }

    /// The admit timestamp carried alongside the name/completed flag by
    /// `collect_update_result_metrics` (issue #781). The exact value is asserted
    /// by `collect_update_result_metrics_carries_admit_timestamp`; the other
    /// tests only care about name + completed, so they read it back verbatim.
    fn admit_ts_of(
        history: &[WorkflowEvent],
        id: crate::types::UpdateId,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        history.iter().find_map(|e| match e {
            WorkflowEvent::UpdateAdmitted {
                update_id,
                timestamp,
                ..
            } if *update_id == id => Some(*timestamp),
            _ => None,
        })
    }

    #[test]
    fn collect_update_result_metrics_empty_without_record_update_result() {
        let id = crate::types::UpdateId::new();
        let history = vec![admitted(id, "set_priority")];
        let cmds = vec![WorkflowCommand::RecordMarker {
            name: "m".into(),
            details: Value::Null,
        }];
        assert!(collect_update_result_metrics(&history, &cmds).is_empty());
    }

    #[test]
    fn collect_update_result_metrics_resolves_name_and_completed_flag() {
        let ok_id = crate::types::UpdateId::new();
        let err_id = crate::types::UpdateId::new();
        let history = vec![admitted(ok_id, "set_priority"), admitted(err_id, "cancel")];
        let cmds = vec![
            WorkflowCommand::RecordUpdateResult {
                update_id: ok_id,
                result: Ok(serde_json::json!("done")),
            },
            WorkflowCommand::RecordUpdateResult {
                update_id: err_id,
                result: Err("nope".into()),
            },
        ];
        let out = collect_update_result_metrics(&history, &cmds);
        assert_eq!(
            out,
            vec![
                (
                    "set_priority".to_owned(),
                    true,
                    admit_ts_of(&history, ok_id)
                ),
                ("cancel".to_owned(), false, admit_ts_of(&history, err_id))
            ]
        );
    }

    #[test]
    fn update_result_command_source_reads_suspended_outcome_commands_not_pending() {
        // Issue #684 regression: the common suspend path carries its
        // RecordUpdateResult INSIDE the Suspended outcome's `commands`, while
        // `pending_cmds` (the executor's second tuple element) is EMPTY. An
        // earlier cut collected from `pending_cmds` only and silently
        // undercounted every update that completed and then suspended.
        let id = crate::types::UpdateId::new();
        let history = vec![admitted(id, "set_priority")];
        let suspend_commands = vec![
            WorkflowCommand::RecordUpdateResult {
                update_id: id,
                result: Ok(serde_json::json!("done")),
            },
            // A benign bookkeeping command standing in for the wait the workflow
            // suspends on after the update completes.
            WorkflowCommand::RecordMarker {
                name: "m".into(),
                details: Value::Null,
            },
        ];
        let outcome = WorkflowOutcome::Suspended {
            commands: suspend_commands,
        };
        let pending_cmds: Vec<WorkflowCommand> = Vec::new();

        // The bug: reading only pending_cmds finds nothing.
        assert!(
            collect_update_result_metrics(&history, &pending_cmds).is_empty(),
            "pending_cmds is empty on the suspend path — this is the source of the bug"
        );
        // The fix: the source selector reaches into the Suspended commands.
        let source = update_result_command_source(&outcome, &pending_cmds);
        assert_eq!(
            collect_update_result_metrics(&history, source),
            vec![("set_priority".to_owned(), true, admit_ts_of(&history, id))],
            "the suspend path's update result must be collected from the outcome's commands"
        );
    }

    #[test]
    fn update_result_command_source_reads_pending_cmds_for_terminal_outcomes() {
        // Terminal (and continue-as-new) outcomes carry their RecordUpdateResult
        // in pending_cmds, NOT inside the outcome, so the selector must return
        // pending_cmds there.
        let id = crate::types::UpdateId::new();
        let pending_cmds = vec![WorkflowCommand::RecordUpdateResult {
            update_id: id,
            result: Err("nope".into()),
        }];
        for outcome in [
            WorkflowOutcome::Completed {
                output: Value::Null,
                unhandled_signals: std::collections::BTreeMap::new(),
            },
            WorkflowOutcome::Failed {
                error: "boom".into(),
                non_deterministic_details: None,
                handler_panic: false,
                unhandled_signals: std::collections::BTreeMap::new(),
            },
            WorkflowOutcome::ContinuedAsNew { input: Value::Null },
        ] {
            let source = update_result_command_source(&outcome, &pending_cmds);
            assert_eq!(
                source.len(),
                1,
                "terminal/CAN outcomes must read update results from pending_cmds"
            );
        }
    }

    #[test]
    fn collect_update_result_metrics_labels_unresolved_name_as_unknown() {
        // A RecordUpdateResult whose UpdateAdmitted is not in the loaded history
        // (should not happen in practice) is labeled "unknown", never dropped.
        let id = crate::types::UpdateId::new();
        let history: Vec<WorkflowEvent> = vec![];
        let cmds = vec![WorkflowCommand::RecordUpdateResult {
            update_id: id,
            result: Ok(Value::Null),
        }];
        assert_eq!(
            collect_update_result_metrics(&history, &cmds),
            vec![("unknown".to_owned(), true, None)]
        );
    }

    #[test]
    fn collect_update_result_metrics_buckets_only_handler_not_found_to_sentinel() {
        // Issue #684 (Codex P2): an unregistered update name — admitted via the
        // free-form raw route — fails with the exact "update handler '<name>'
        // not found" error; only that case buckets to __unregistered__. A real
        // handler that fails with any other error keeps its name (a real handler
        // may be declarative OR imperative, so bucketing against the declarative
        // registry would mislabel it).
        let unregistered_id = crate::types::UpdateId::new();
        let real_id = crate::types::UpdateId::new();
        let history = vec![
            admitted(unregistered_id, "totally_random_name"),
            admitted(real_id, "set_priority"),
        ];
        let cmds = vec![
            WorkflowCommand::RecordUpdateResult {
                update_id: unregistered_id,
                result: Err("update handler 'totally_random_name' not found".to_owned()),
            },
            WorkflowCommand::RecordUpdateResult {
                update_id: real_id,
                result: Err("validation failed".to_owned()),
            },
        ];
        assert_eq!(
            collect_update_result_metrics(&history, &cmds),
            vec![
                (
                    crate::telemetry::UNREGISTERED_UPDATE_NAME.to_owned(),
                    false,
                    admit_ts_of(&history, unregistered_id)
                ),
                (
                    "set_priority".to_owned(),
                    false,
                    admit_ts_of(&history, real_id)
                ),
            ]
        );
    }

    #[test]
    fn is_unregistered_update_failure_matches_exact_not_found_error_only() {
        let ok: Result<Value, String> = Ok(Value::Null);
        assert!(!is_unregistered_update_failure("set_val", &ok));
        assert!(is_unregistered_update_failure(
            "set_val",
            &Err("update handler 'set_val' not found".to_owned())
        ));
        // A different name in the error must not match this update's name.
        assert!(!is_unregistered_update_failure(
            "set_val",
            &Err("update handler 'other' not found".to_owned())
        ));
        // An unrelated error from a real handler is not bucketed.
        assert!(!is_unregistered_update_failure(
            "set_val",
            &Err("boom".to_owned())
        ));
    }

    // ── update admit→terminal latency histogram (issue #781) ──────────────

    #[test]
    fn update_admit_duration_secs_none_when_admit_missing() {
        // A RecordUpdateResult whose UpdateAdmitted is not in the loaded history
        // yields no admit timestamp → the histogram sample is SKIPPED (never a
        // bogus 0), while the completed/failed counter still fires.
        let now = chrono::Utc::now();
        assert_eq!(update_admit_duration_secs(None, now), None);
    }

    #[test]
    fn update_admit_duration_secs_measures_positive_delta_in_seconds() {
        let admit = chrono::Utc::now();
        let now = admit + chrono::Duration::milliseconds(2500);
        let secs = update_admit_duration_secs(Some(admit), now).expect("some duration");
        assert!((secs - 2.5).abs() < 1e-6, "expected ~2.5s, got {secs}");
    }

    #[test]
    fn update_admit_duration_secs_clamps_negative_skew_to_zero() {
        // Terminal time BEFORE the recorded admit (clock skew across appends)
        // must clamp to 0 — never emit a negative/garbage sample.
        let admit = chrono::Utc::now();
        let now = admit - chrono::Duration::seconds(5);
        assert_eq!(update_admit_duration_secs(Some(admit), now), Some(0.0));
    }

    #[test]
    fn collect_update_result_metrics_carries_admit_timestamp() {
        // Issue #781: collect now also carries the admit timestamp so emit can
        // compute admit→terminal latency. A resolved admit carries Some(ts); an
        // unresolved one carries None (name "unknown", no histogram sample).
        let ok_id = crate::types::UpdateId::new();
        let missing_id = crate::types::UpdateId::new();
        let admit_ts = chrono::Utc::now();
        let history = vec![WorkflowEvent::UpdateAdmitted {
            update_id: ok_id,
            name: "set_priority".to_string(),
            input: Value::Null,
            timestamp: admit_ts,
        }];
        let cmds = vec![
            WorkflowCommand::RecordUpdateResult {
                update_id: ok_id,
                result: Ok(Value::Null),
            },
            WorkflowCommand::RecordUpdateResult {
                update_id: missing_id,
                result: Ok(Value::Null),
            },
        ];
        let out = collect_update_result_metrics(&history, &cmds);
        assert_eq!(out[0], ("set_priority".to_owned(), true, Some(admit_ts)));
        assert_eq!(out[1], ("unknown".to_owned(), true, None));
    }

    // ── signal.unhandled post-commit collection (issue #684, Codex P2) ─────

    /// A recording sink capturing every `record_signal_unhandled` call so
    /// `emit_unhandled_signal_metrics` can be asserted directly. The signal name
    /// is not a label (issue #684, Codex P2), so each call carries only
    /// `(workflow, queue)`.
    #[derive(Default)]
    struct SignalUnhandledSink {
        calls: std::sync::Mutex<Vec<(String, String)>>,
    }
    impl crate::telemetry::MetricsRecorder for SignalUnhandledSink {
        fn record_signal_unhandled(&self, workflow_name: &str, queue: &str) {
            self.calls
                .lock()
                .unwrap()
                .push((workflow_name.to_owned(), queue.to_owned()));
        }
    }

    fn completed_with_signals(pairs: &[(&str, u64)]) -> WorkflowOutcome {
        WorkflowOutcome::Completed {
            output: Value::Null,
            unhandled_signals: pairs.iter().map(|(n, c)| ((*n).to_owned(), *c)).collect(),
        }
    }

    #[test]
    fn outcome_unhandled_signals_reads_terminal_maps_and_is_empty_otherwise() {
        // Completed / Failed carry the map; Suspended / ContinuedAsNew do not.
        // This is the pre-persist collection the worker feeds to the post-commit
        // `Persisted`-arm emission — so nothing to emit exists for a non-terminal
        // outcome, and the terminal map is captured before persist moves it.
        assert_eq!(
            outcome_unhandled_signals(&completed_with_signals(&[("late", 2)])),
            std::collections::BTreeMap::from([("late".to_owned(), 2u64)]),
        );
        assert_eq!(
            outcome_unhandled_signals(&WorkflowOutcome::Failed {
                error: "boom".into(),
                non_deterministic_details: None,
                handler_panic: false,
                unhandled_signals: std::collections::BTreeMap::from([("x".to_owned(), 1u64)]),
            }),
            std::collections::BTreeMap::from([("x".to_owned(), 1u64)]),
        );
        assert!(
            outcome_unhandled_signals(&WorkflowOutcome::Suspended { commands: vec![] }).is_empty(),
            "a Suspended outcome is not terminal — nothing to emit"
        );
        assert!(
            outcome_unhandled_signals(&WorkflowOutcome::ContinuedAsNew { input: Value::Null })
                .is_empty(),
            "continue-as-new carries no unhandled-signal map"
        );
    }

    #[test]
    fn emit_unhandled_signal_metrics_sums_the_map_without_a_name_label() {
        let sink = SignalUnhandledSink::default();
        let map =
            std::collections::BTreeMap::from([("a".to_owned(), 2u64), ("b".to_owned(), 1u64)]);
        emit_unhandled_signal_metrics(&sink, "wf", "q", &map);
        let calls = sink.calls.lock().unwrap().clone();
        // 2 occurrences of "a" + 1 of "b" = 3 total emissions, each against the
        // single (workflow, queue) series — the signal name is not a label
        // (issue #684, Codex P2), so the map is summed to preserve the total
        // unconsumed volume while dropping the unbounded name dimension.
        assert_eq!(calls.len(), 3);
        assert!(calls.iter().all(|(w, q)| w == "wf" && q == "q"));
    }

    #[test]
    fn emit_unhandled_signal_metrics_is_a_noop_for_the_empty_collected_map() {
        // The `Persisted` arm always calls emit with the pre-collected map; for
        // a non-terminal (or empty-terminal) outcome that map is empty, so no
        // counter fires. This is the structural companion to the DB test that a
        // ParkedPaused / persist-failure cycle — which returns before the
        // `Persisted` arm entirely — emits nothing.
        let sink = SignalUnhandledSink::default();
        emit_unhandled_signal_metrics(
            &sink,
            "wf",
            "q",
            &outcome_unhandled_signals(&WorkflowOutcome::Suspended { commands: vec![] }),
        );
        assert!(sink.calls.lock().unwrap().is_empty());
    }
}
