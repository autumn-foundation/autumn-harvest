//! Fluent API for registering workflows, activities, and configuring the worker.

use std::any::{Any, TypeId};
use std::sync::Arc;
use std::time::Duration;

use crate::batch_start::BatchStartConfig;
use crate::context::{SharedStateMap, WorkflowHistoryPolicy};
use crate::info::{
    ActivityInfo, DagInfo, QueryHandlerInfo, SignalHandlerInfo, UpdateHandlerInfo, WorkflowInfo,
};
use crate::payload_codec::{PayloadCodec, PayloadCodecs};
use crate::policy::WorkflowSchedule;
use crate::retention::RetentionConfig;
use crate::telemetry::TelemetryConfig;
use crate::types::ShardId;

/// Fluent builder for configuring the autumn-harvest engine.
///
/// In a full Autumn app, this is consumed by `HarvestPlugin` from the
/// `autumn-harvest-plugin` crate. In tests or standalone use, call
/// `.build()` directly.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::builder::{HarvestBuilder, WorkerConfig};
///
/// struct DatabasePool;
///
/// let built = HarvestBuilder::new()
///     .workflows(vec![]) // usually from workflows![]
///     .activities(vec![]) // usually from activities![]
///     .dags(vec![]) // usually from dags![]
///     .worker(WorkerConfig::default())
///     .state(DatabasePool)
///     .build();
///
/// assert_eq!(built.workflow_count(), 0);
/// assert!(built.state::<DatabasePool>().is_some());
/// ```
/// Default payload cap values (issue #252).
pub const DEFAULT_MAX_ACTIVITY_INPUT_BYTES: u64 = 2 * 1024 * 1024; // 2 MiB
/// Default maximum activity result payload size.
pub const DEFAULT_MAX_ACTIVITY_RESULT_BYTES: u64 = 2 * 1024 * 1024; // 2 MiB
/// Default maximum signal payload size.
pub const DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES: u64 = 256 * 1024; // 256 KiB
/// Default maximum workflow input payload size.
pub const DEFAULT_MAX_WORKFLOW_INPUT_BYTES: u64 = 2 * 1024 * 1024; // 2 MiB
/// Default maximum workflow start delay (365 days).
pub const DEFAULT_MAX_WORKFLOW_START_DELAY: Duration = Duration::from_secs(365 * 24 * 3600);
/// Default bounded-pause ceiling before auto-resume (24 hours, issue #383).
pub const DEFAULT_MAX_WORKFLOW_PAUSE_DURATION: Duration = Duration::from_secs(24 * 3600);
/// Default max-wait cap for debounced workflow starts (1 hour, issue #499).
pub const DEFAULT_DEBOUNCE_MAX_WAIT: Duration = Duration::from_secs(3600);
/// Default large-payload offload threshold (issue #524): 256 KiB.
///
/// Payload-bearing fields larger than this are offloaded to the configured
/// [`PayloadStore`](crate::payload_store::PayloadStore). Only takes effect when
/// a store is registered.
pub const DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD: u64 = 256 * 1024;
/// Default ceiling on an author-supplied `Retry-After` delay hint (issue #744): 15 minutes.
///
/// Bounds abuse from a misbehaving/malicious downstream without rejecting a
/// legitimate hint outright — an over-ceiling value is clamped down, never
/// an error. Configurable via [`WorkerConfig::with_retry_after_ceiling`].
pub const DEFAULT_RETRY_AFTER_CEILING: Duration = Duration::from_secs(15 * 60);

pub struct HarvestBuilder {
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    dags: Vec<DagInfo>,
    workflow_schedules: Vec<WorkflowSchedule>,
    auto_registered_dag_workflows: Vec<String>,
    /// Declarative query handlers collected via `queries![…]`.
    query_handlers: Vec<QueryHandlerInfo>,
    /// Declarative update handlers collected via `updates![…]`.
    update_handlers: Vec<UpdateHandlerInfo>,
    /// Declarative signal handler metadata collected via `signals![…]` (issue #610).
    signal_handlers: Vec<SignalHandlerInfo>,
    worker_config: WorkerConfig,
    state: SharedStateMap,
    telemetry: Option<TelemetryConfig>,
    retention: RetentionConfig,
    history_archiver: Option<Arc<dyn crate::retention::HistoryArchiver>>,
    /// Ordered activity execution interceptor chain (issue #680). Index 0 is the
    /// OUTERMOST wrapper; the activity handler is innermost. Empty (default) =
    /// no interceptors.
    activity_interceptors: Vec<Arc<dyn crate::interceptor::ActivityInterceptor>>,
    payload_codecs: PayloadCodecs,
    /// Embedder-supplied external blob store for large-payload offloading (issue #524).
    payload_store: Option<Arc<dyn crate::payload_store::PayloadStore>>,
    /// Byte threshold above which payload-bearing fields are offloaded (issue #524).
    payload_offload_threshold: u64,
    history_policy: WorkflowHistoryPolicy,
    /// Server-side ceiling on `execution_timeout` (issue #243).
    ///
    /// When set, any `start_workflow` call that requests an `execution_timeout`
    /// larger than this ceiling is rejected with [`BuildError::ExecutionTimeoutExceedsCeiling`].
    /// `None` means no ceiling is enforced.
    max_workflow_execution_timeout: Option<Duration>,
    /// Server-side ceiling on the chain-scoped lifetime cap (issue #617).
    ///
    /// Unlike `max_workflow_execution_timeout` (which only caps a *specified*
    /// per-run timeout), this ceiling ALSO acts as a fleet-wide DEFAULT: a
    /// workflow that declares no chain cap still inherits this value as its chain
    /// deadline, so an operator can cap every chain even when a workflow
    /// under-specifies. `None` means no chain cap is applied fleet-wide.
    max_workflow_chain_timeout: Option<Duration>,
    /// Optional hard ceiling on the number of durable events a RUNNING workflow
    /// execution may accumulate (issue #493). When a workflow's event count
    /// reaches or exceeds this value the execution is terminated with
    /// `WorkflowFailed` and a machine-readable reason. `None` = no ceiling.
    max_workflow_history_events: Option<u64>,
    /// Maximum allowed byte length for an activity input payload (issue #252).
    /// Default: 2 MiB.
    max_activity_input_bytes: u64,
    /// Maximum allowed byte length for an activity result payload (issue #252).
    /// Default: 2 MiB.
    max_activity_result_bytes: u64,
    /// Maximum allowed byte length for a signal payload (issue #252).
    /// Default: 256 KiB.
    max_signal_payload_bytes: u64,
    /// Maximum allowed byte length for a workflow start input payload (issue #252).
    /// Default: 2 MiB.
    max_workflow_input_bytes: u64,
    /// Maximum byte length for `current_details` strings set via
    /// `ctx.set_current_details(...)` (issue #473).
    /// Default: 1 KiB.
    max_current_details_bytes: usize,
    /// Opt-in durable workflow-log sink policy (issue #790). `None` = disabled.
    workflow_log_policy: Option<crate::context::WorkflowLogPolicy>,
    /// Server-side ceiling on workflow start delay (issue #322).
    /// Default: 365 days.
    max_workflow_start_delay: Option<Duration>,
    /// Grace window before cross-workflow signaling fails for unknown target (issue #330).
    unknown_target_grace_window: Option<Duration>,
    /// Hard caps for `POST /workflows/batch_start` (issue #357).
    batch_start_config: BatchStartConfig,
    /// Declarative completion triggers (issue #517).
    completion_triggers: Vec<crate::completion_trigger::CompletionTrigger>,
    /// Server-side ceiling on `workflow_attempt` (issue #523).
    /// When set, `retry_policy.max_attempts` is clamped to `min(max_attempts, ceiling)`.
    max_workflow_attempts: Option<u32>,
    /// Ceiling on the `[from, to]` window accepted by `GET /admin/usage`
    /// (issue #596). `None` uses `crate::usage::default_usage_window_ceiling()`.
    usage_window_ceiling: Option<Duration>,
    /// Cap on distinct groups `GET /admin/usage` will return before failing
    /// loudly with `413` (issue #596). `None` uses
    /// `crate::usage::default_usage_max_groups()`.
    usage_max_groups: Option<usize>,
    /// Builder-wide completion-callback configuration (issue #605): default
    /// targets, SSRF host allowlist, HMAC secret, retry policy, and an
    /// optional custom deliverer.
    completion_callback_config: crate::completion_callback::CompletionCallbackBuilderConfig,
    /// Retention window for request-scoped start idempotency keys (issue #808).
    /// A repeated `idempotency_key` within this window deduplicates onto the same
    /// execution; after it elapses the key is reusable. `None` uses
    /// [`crate::start_idempotency::DEFAULT_START_IDEMPOTENCY_WINDOW`] (24h).
    start_idempotency_window: Option<Duration>,
    /// Sandbox policy per registered WASM activity (issue #965), keyed by name.
    #[cfg(feature = "wasm-activities")]
    wasm_bindings: std::collections::HashMap<String, crate::wasm_store::WasmBinding>,
    /// Shared WASM engine + compiled-module cache, created lazily on the first
    /// `wasm_activity(...)` call (issue #965). `None` = no WASM activities.
    #[cfg(feature = "wasm-activities")]
    wasm_store: Option<Arc<crate::wasm_activities::WasmModuleStore>>,
    /// `(activity_name, module_bytes)` pairs published to each worker's shard DB
    /// at startup (issue #965).
    #[cfg(feature = "wasm-activities")]
    wasm_module_registrations: Vec<(String, Vec<u8>)>,
}

impl Default for HarvestBuilder {
    fn default() -> Self {
        Self {
            workflows: Vec::new(),
            activities: Vec::new(),
            dags: Vec::new(),
            workflow_schedules: Vec::new(),
            auto_registered_dag_workflows: Vec::new(),
            query_handlers: Vec::new(),
            update_handlers: Vec::new(),
            signal_handlers: Vec::new(),
            worker_config: WorkerConfig::default(),
            state: std::collections::HashMap::new(),
            telemetry: None,
            retention: crate::retention::RetentionConfig::default(),
            history_archiver: None,
            activity_interceptors: Vec::new(),
            payload_codecs: crate::payload_codec::PayloadCodecs::default(),
            payload_store: None,
            payload_offload_threshold: DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD,
            history_policy: crate::context::WorkflowHistoryPolicy::default(),
            max_workflow_execution_timeout: None,
            max_workflow_chain_timeout: None,
            max_workflow_history_events: None,
            max_activity_input_bytes: DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
            max_activity_result_bytes: DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
            max_signal_payload_bytes: DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
            max_workflow_input_bytes: DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            max_current_details_bytes: crate::context::DEFAULT_CURRENT_DETAILS_CAP_BYTES,
            workflow_log_policy: None,
            max_workflow_start_delay: None,
            unknown_target_grace_window: None,
            batch_start_config: BatchStartConfig::default(),
            completion_triggers: Vec::new(),
            max_workflow_attempts: None,
            usage_window_ceiling: None,
            usage_max_groups: None,
            completion_callback_config:
                crate::completion_callback::CompletionCallbackBuilderConfig::default(),
            start_idempotency_window: None,
            #[cfg(feature = "wasm-activities")]
            wasm_bindings: std::collections::HashMap::new(),
            #[cfg(feature = "wasm-activities")]
            wasm_store: None,
            #[cfg(feature = "wasm-activities")]
            wasm_module_registrations: Vec::new(),
        }
    }
}

impl std::fmt::Debug for HarvestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HarvestBuilder")
            .field("workflow_count", &self.workflows.len())
            .field("activity_count", &self.activities.len())
            .field("dag_count", &self.dags.len())
            .field("workflow_schedule_count", &self.workflow_schedules.len())
            .field(
                "auto_registered_dag_workflow_count",
                &self.auto_registered_dag_workflows.len(),
            )
            .field("query_handler_count", &self.query_handlers.len())
            .field("update_handler_count", &self.update_handlers.len())
            .field("signal_handler_count", &self.signal_handlers.len())
            .field("worker_config", &self.worker_config)
            .field("state_count", &self.state.len())
            .field("telemetry_configured", &self.telemetry.is_some())
            .field("retention", &self.retention)
            .field("payload_codecs", &"configured")
            .field("history_policy", &self.history_policy)
            .field(
                "max_workflow_execution_timeout",
                &self.max_workflow_execution_timeout,
            )
            .field(
                "max_workflow_chain_timeout",
                &self.max_workflow_chain_timeout,
            )
            .field(
                "max_workflow_history_events",
                &self.max_workflow_history_events,
            )
            .field("max_activity_input_bytes", &self.max_activity_input_bytes)
            .field("max_activity_result_bytes", &self.max_activity_result_bytes)
            .field("max_signal_payload_bytes", &self.max_signal_payload_bytes)
            .field("max_workflow_input_bytes", &self.max_workflow_input_bytes)
            .field("max_workflow_start_delay", &self.max_workflow_start_delay)
            .field(
                "unknown_target_grace_window",
                &self.unknown_target_grace_window,
            )
            .field("batch_start_config", &self.batch_start_config)
            .field("max_workflow_attempts", &self.max_workflow_attempts)
            .field("usage_window_ceiling", &self.usage_window_ceiling)
            .field("usage_max_groups", &self.usage_max_groups)
            .field(
                "completion_callback_default_target_count",
                &self.completion_callback_config.default_targets.len(),
            )
            .finish_non_exhaustive()
    }
}

/// Built harvest registration set produced by [`HarvestBuilder::build`].
pub struct BuiltHarvest {
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    dags: Vec<DagInfo>,
    workflow_schedules: Vec<WorkflowSchedule>,
    /// Declarative query handlers indexed by workflow name for fast lookup.
    query_handlers: Vec<QueryHandlerInfo>,
    /// Declarative update handlers indexed by workflow name for fast lookup.
    update_handlers: Vec<UpdateHandlerInfo>,
    /// Declarative signal handler metadata (issue #610).
    signal_handlers: Vec<SignalHandlerInfo>,
    worker_config: WorkerConfig,
    state: SharedStateMap,
    telemetry: Arc<TelemetryConfig>,
    retention: RetentionConfig,
    history_archiver: Option<Arc<dyn crate::retention::HistoryArchiver>>,
    /// Ordered activity execution interceptor chain (issue #680). Index 0 = outermost.
    activity_interceptors: Vec<Arc<dyn crate::interceptor::ActivityInterceptor>>,
    payload_codecs: PayloadCodecs,
    /// Configured large-payload offloader (issue #524). `None` = no store registered.
    payload_offloader: Option<Arc<crate::payload_store::PayloadOffloader>>,
    history_policy: WorkflowHistoryPolicy,
    /// Server-side ceiling on `execution_timeout` (issue #243). `None` = no ceiling.
    pub max_workflow_execution_timeout: Option<Duration>,
    /// Server-side ceiling on the chain-scoped lifetime cap AND fleet-wide chain
    /// default (issue #617). `None` = no chain cap applied fleet-wide.
    pub max_workflow_chain_timeout: Option<Duration>,
    /// Hard ceiling on durable event count per execution (issue #493). `None` = no ceiling.
    pub max_workflow_history_events: Option<u64>,
    /// Maximum allowed byte length for an activity input payload (issue #252).
    /// Default: 2 MiB.
    pub max_activity_input_bytes: u64,
    /// Maximum allowed byte length for an activity result payload (issue #252).
    /// Default: 2 MiB.
    pub max_activity_result_bytes: u64,
    /// Maximum allowed byte length for a signal payload (issue #252).
    /// Default: 256 KiB.
    pub max_signal_payload_bytes: u64,
    /// Maximum allowed byte length for a workflow start input payload (issue #252).
    /// Default: 2 MiB.
    pub max_workflow_input_bytes: u64,
    /// Maximum byte length for `current_details` strings (issue #473).
    /// Default: 1 KiB.
    pub max_current_details_bytes: usize,
    /// Opt-in durable workflow-log sink policy (issue #790). `None` = disabled.
    pub workflow_log_policy: Option<crate::context::WorkflowLogPolicy>,
    /// Server-side ceiling on workflow start delay (issue #322).
    /// Default: 365 days.
    pub max_workflow_start_delay: Duration,
    /// Grace window before cross-workflow signaling fails for unknown target (issue #330).
    pub unknown_target_grace_window: Duration,
    /// Hard caps for `POST /workflows/batch_start` (issue #357).
    pub batch_start_config: BatchStartConfig,
    /// Declarative completion triggers (issue #517).
    completion_triggers: Vec<crate::completion_trigger::CompletionTrigger>,
    /// Server-side ceiling on workflow retry attempts (issue #523). `None` = no ceiling.
    pub max_workflow_attempts: Option<u32>,
    /// Ceiling on the `[from, to]` window accepted by `GET /admin/usage`
    /// (issue #596). Defaults to 90 days.
    pub usage_window_ceiling: Duration,
    /// Cap on distinct groups `GET /admin/usage` will return before failing
    /// loudly with `413` (issue #596). Defaults to 10,000.
    pub usage_max_groups: usize,
    /// Resolved builder-wide completion-callback configuration (issue #605).
    /// `deliverer` is still `None` here when the embedder didn't supply a
    /// custom one — the plugin substitutes its default `reqwest`-based
    /// implementation at runtime startup.
    completion_callback_config: crate::completion_callback::CompletionCallbackBuilderConfig,
    /// Retention window for request-scoped start idempotency keys (issue #808).
    /// Defaults to [`crate::start_idempotency::DEFAULT_START_IDEMPOTENCY_WINDOW`]
    /// (24h) when unset on the builder.
    pub start_idempotency_window: Duration,
    /// Sandbox policy per registered WASM activity (issue #965), keyed by name.
    #[cfg(feature = "wasm-activities")]
    wasm_bindings: std::collections::HashMap<String, crate::wasm_store::WasmBinding>,
    /// Shared WASM engine + compiled-module cache (issue #965). `None` = no WASM
    /// activities registered.
    #[cfg(feature = "wasm-activities")]
    wasm_store: Option<Arc<crate::wasm_activities::WasmModuleStore>>,
    /// `(activity_name, module_bytes)` pairs published to each worker's shard DB
    /// at startup (issue #965).
    #[cfg(feature = "wasm-activities")]
    wasm_module_registrations: Vec<(String, Vec<u8>)>,
}

impl std::fmt::Debug for BuiltHarvest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltHarvest")
            .field("workflow_count", &self.workflows.len())
            .field("activity_count", &self.activities.len())
            .field("dag_count", &self.dags.len())
            .field("workflow_schedule_count", &self.workflow_schedules.len())
            .field("query_handler_count", &self.query_handlers.len())
            .field("update_handler_count", &self.update_handlers.len())
            .field("signal_handler_count", &self.signal_handlers.len())
            .field("worker_config", &self.worker_config)
            .field("state_count", &self.state.len())
            .field("telemetry", &self.telemetry)
            .field("retention", &self.retention)
            .field("payload_codecs", &"configured")
            .field("history_policy", &self.history_policy)
            .field(
                "max_workflow_execution_timeout",
                &self.max_workflow_execution_timeout,
            )
            .field(
                "max_workflow_chain_timeout",
                &self.max_workflow_chain_timeout,
            )
            .field(
                "max_workflow_history_events",
                &self.max_workflow_history_events,
            )
            .field("max_activity_input_bytes", &self.max_activity_input_bytes)
            .field("max_activity_result_bytes", &self.max_activity_result_bytes)
            .field("max_signal_payload_bytes", &self.max_signal_payload_bytes)
            .field("max_workflow_input_bytes", &self.max_workflow_input_bytes)
            .field("max_current_details_bytes", &self.max_current_details_bytes)
            .field("workflow_log_policy", &self.workflow_log_policy)
            .field("max_workflow_start_delay", &self.max_workflow_start_delay)
            .field(
                "unknown_target_grace_window",
                &self.unknown_target_grace_window,
            )
            .field("batch_start_config", &self.batch_start_config)
            .field("max_workflow_attempts", &self.max_workflow_attempts)
            .field("usage_window_ceiling", &self.usage_window_ceiling)
            .field("usage_max_groups", &self.usage_max_groups)
            .field(
                "completion_callback_default_target_count",
                &self.completion_callback_config.default_targets.len(),
            )
            .finish_non_exhaustive()
    }
}

/// A float wrapper that implements `Eq` and `PartialEq` by doing bitwise comparison.
/// Useful for keeping errors `Eq`-compliant.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct FloatEq(pub f64);

impl PartialEq for FloatEq {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for FloatEq {}

impl std::fmt::Display for FloatEq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Builder-time configuration errors.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum HarvestBuilderError {
    /// Retention configuration validation failed.
    #[error("invalid retention configuration: {0}")]
    InvalidRetention(String),

    /// Two activities sharing a `concurrency_key` declare different
    /// `max_concurrent` values. There is no silent precedence rule — the
    /// operator must pick one value and apply it consistently.
    ///
    /// `activities` lists each `(activity_name, max_concurrent)` pair that
    /// contributed to the conflict.
    #[error(
        "concurrency_key '{key}' has conflicting max_concurrent values across activities: {activities:?}"
    )]
    ConcurrencyKeyMismatch {
        /// The shared concurrency key.
        key: String,
        /// Each `(activity_name, max_concurrent)` pair with a conflicting value.
        activities: Vec<(String, u32)>,
    },

    /// An activity declares a `concurrency_key` but no `max_concurrent` cap.
    /// Without a cap the key is written to the queue row but the saturation
    /// predicate (`(SELECT COUNT(*) ...) < NULL`) is always null/unknown,
    /// silently bypassing the intended shared budget.
    #[error(
        "activity '{activity}' sets concurrency_key = \"{key}\" but has no max_concurrent; \
         either add max_concurrent or remove the concurrency_key"
    )]
    ConcurrencyKeyWithoutCap {
        /// The activity name.
        activity: String,
        /// The orphaned concurrency key.
        key: String,
    },

    /// An activity declares `max_concurrent = 0`, which makes the saturation
    /// check `(SELECT COUNT(*) ...) < 0` always false, permanently deferring
    /// every task for this activity.
    #[error(
        "activity '{activity}' has max_concurrent = 0; use max_concurrent >= 1 \
         or omit max_concurrent entirely to disable the cap"
    )]
    ZeroConcurrencyCap {
        /// The activity name.
        activity: String,
    },

    /// A [`WorkflowSchedule`] names a workflow that was not registered via
    /// `workflows![]`. The schedule is rejected at build time so the operator
    /// sees a clear error rather than silent no-ops at scheduler tick time.
    ///
    /// `workflow_name` is the name that was not found. `registered` lists every
    /// workflow name that was actually registered on this builder.
    #[error(
        "workflow_schedule references unknown workflow '{workflow_name}'; \
         registered workflows: {registered:?}"
    )]
    UnknownWorkflowSchedule {
        /// The unrecognised workflow name in the schedule.
        workflow_name: String,
        /// All workflow names currently registered on the builder.
        registered: Vec<String>,
    },

    /// A local activity declares a `start_to_close` that exceeds the worker's
    /// `max_local_activity_start_to_close` cap. Local activities run inline on
    /// the workflow worker and must not block it indefinitely.
    #[error(
        "local activity '{activity}' start_to_close ({actual:?}) exceeds the worker cap \
         ({cap:?}); lower start_to_close or raise WorkerConfig::max_local_activity_start_to_close"
    )]
    LocalActivityStartToCloseExceedsCap {
        /// The local activity name.
        activity: String,
        /// The declared `start_to_close` on the activity.
        actual: Duration,
        /// The configured worker cap.
        cap: Duration,
    },

    /// A [`WorkflowSchedule`] contains an invalid schedule value (malformed cron
    /// expression, zero-length interval, etc.). Caught at build time so the
    /// operator sees a clear error rather than silently-inert or wedging schedules.
    #[error("workflow_schedule for '{workflow_name}' has an invalid schedule: {reason}")]
    InvalidWorkflowSchedule {
        /// The workflow name whose schedule is invalid.
        workflow_name: String,
        /// Human-readable reason the schedule was rejected.
        reason: String,
    },

    /// A plain (non-DAG, non-canary) [`WorkflowSchedule`] opted into
    /// `all_writable_shards`, which is only supported for DAG schedules and the
    /// built-in synthetic liveness canary (issue #796).
    ///
    /// Registration honours the flag for any schedule, but the fire path only
    /// encodes the shard id into the minted `ExecutionId` for DAGs and canaries
    /// (it derives that from the workflow name / DAG-ness of the persisted
    /// `harvest_schedules` row, which carries no `all_writable_shards` column).
    /// A plain workflow opting in would register on every writable shard yet
    /// mint every execution on the default shard — duplicate runs plus
    /// cross-shard write inconsistency. Making it general-purpose would require
    /// persisting the flag as a `harvest_schedules` column (a migration), which
    /// #796 (AC10) deliberately avoids, so this combination is rejected here.
    #[error(
        "workflow_schedule for '{workflow_name}' sets all_writable_shards, which is \
         only supported for DAG schedules and the built-in liveness canary; a plain \
         workflow cannot use it because the fire path would mint executions on the \
         default shard (making it general-purpose requires a harvest_schedules \
         migration, avoided per issue #796 AC10)"
    )]
    AllWritableShardsUnsupported {
        /// The plain workflow name that improperly opted into `all_writable_shards`.
        workflow_name: String,
    },

    /// A normal workflow registration reused the name of a DAG that is
    /// auto-registered as a workflow for unified DAG execution.
    #[error(
        "workflow name '{name}' collides with an auto-registered DAG workflow; \
         register workflows and DAGs with distinct names"
    )]
    DagWorkflowNameCollision {
        /// The shared workflow/DAG name.
        name: String,
    },

    /// A DAG references an activity registered as local-only. Local activities
    /// run inline on the workflow worker and cannot be scheduled through the
    /// DAG activity queue lowering.
    #[error(
        "DAG '{dag}' references local activity '{activity}'; local activities cannot be used in DAG definitions"
    )]
    LocalActivityInDag {
        /// DAG containing the local activity task.
        dag: String,
        /// Local activity referenced by the DAG.
        activity: String,
    },

    /// A classic (non-unified) DAG contains a signal/timer gate node (issue
    /// #746). Gate nodes lower onto the unified workflow-handler execution path
    /// (`ctx.wait_for_signal`); the classic DAG executor has no way to suspend
    /// on a signal, so the gate would silently never fire. Enable the
    /// `unified-dag-execution` feature (on by default) so the `#[dag]` macro
    /// emits the workflow handler that can run gates.
    #[error(
        "DAG '{dag}' has a signal gate on signal '{signal}' but is not unified \
         (workflow_handler is None); signal gates require the unified-dag-execution path"
    )]
    DagSignalGateRequiresUnifiedExecution {
        /// DAG containing the signal gate.
        dag: String,
        /// The signal the gate waits on.
        signal: String,
    },

    /// A classic (non-unified) DAG declares a node compensator (issue #780).
    /// Compensation lowers onto the unified workflow-handler execution path
    /// (`run_unified_dag`'s terminal-failure `Saga` unwind); the classic DAG
    /// executor has no unwind step, so the compensator would silently never
    /// run. Enable the `unified-dag-execution` feature (on by default) so the
    /// `#[dag]` macro emits the workflow handler that can run compensations.
    #[error(
        "classic DAG '{dag}' declares compensator '{compensate}' on task '{task}' but is not \
         unified (workflow_handler is None); compensation requires the unified-dag-execution path"
    )]
    DagCompensationRequiresUnifiedExecution {
        /// DAG containing the compensated node.
        dag: String,
        /// The node declaring the compensator.
        task: String,
        /// The declared compensator activity name.
        compensate: String,
    },

    /// A workflow declares `ConcurrencyPolicy { limit: 0 }`, which makes the
    /// saturation check `(SELECT COUNT(*) ...) < 0` always false, permanently
    /// deferring every start for that workflow.
    #[error(
        "workflow '{workflow}' has a ConcurrencyPolicy with limit = 0; \
         use limit >= 1 or omit the concurrency policy to disable the cap"
    )]
    ZeroWorkflowConcurrencyLimit {
        /// The workflow name.
        workflow: String,
    },

    /// A workflow's `ThrottlePolicy` (issue #607) has a `burst` or
    /// `refill_per_sec` that could never actually pace admissions.
    ///
    /// `ThrottlePolicy::from_rate_str` — the path the `#[workflow(throttle(...))]`
    /// macro always uses — already rejects these values at parse time, but
    /// `ThrottlePolicy`'s fields are `pub` so an application can also
    /// construct one directly (a struct literal, or
    /// `WorkflowInfo::with_throttle`) and bypass that validation entirely
    /// (code review, issue #607). A `burst` below `1.0` (or non-finite) can
    /// never successfully debit a token — the token debit path only admits a
    /// start when the refilled bucket reaches `>= 1.0`, and refill is capped
    /// at `burst` — so every start under that key would defer forever. A
    /// non-finite or non-positive `refill_per_sec` either disables the
    /// throttle (an infinite refill effectively fills the bucket instantly,
    /// admitting everything) or freezes it once the initial burst is spent
    /// (a zero/negative/NaN refill never restores a drained bucket).
    /// Caught here, at build time, so every construction path is validated
    /// exactly once regardless of how the policy was built.
    #[error("workflow '{workflow}' has an invalid ThrottlePolicy: {reason}")]
    InvalidWorkflowThrottlePolicy {
        /// The workflow name.
        workflow: String,
        /// Which field was invalid and why.
        reason: String,
    },

    /// A [`WorkerConfig`] field has an invalid value.
    #[error("invalid worker configuration: {0}")]
    InvalidWorkerConfig(String),

    /// A [`crate::policy::Schedule::CronInTimezone`] variant declares a
    /// timezone name that is not a valid IANA entry. The name is rejected at
    /// build time so the operator sees a clear error rather than silently
    /// misfiring at the wrong time.
    #[error(
        "unknown timezone '{name}'; use an IANA timezone name \
         (e.g. \"America/Los_Angeles\", \"Europe/London\", \"UTC\")"
    )]
    UnknownTimezone {
        /// The unrecognised IANA timezone name.
        name: String,
    },

    /// Two activities sharing a `rate_limit_key` declare different
    /// `rate_limit_rps` or `rate_limit_burst` values.
    #[error(
        "rate_limit_key '{key}' has conflicting rate limit values across activities: {activities:?}"
    )]
    RateLimitKeyMismatch {
        /// The shared rate limit key.
        key: String,
        /// Each `(activity_name, rate_limit_rps, Option<rate_limit_burst>)` pair with a conflicting value.
        activities: Vec<(String, FloatEq, Option<FloatEq>)>,
    },

    /// An activity declares a `rate_limit_key` but no `rate_limit_rps`.
    #[error(
        "activity '{activity}' sets rate_limit_key = \"{key}\" but has no rate_limit_rps; \
         add rate_limit_rps or remove the rate_limit_key"
    )]
    RateLimitKeyWithoutCap {
        /// The activity name.
        activity: String,
        /// The orphaned rate limit key.
        key: String,
    },

    /// An activity declares a dynamic per-key rate limit (issue #699,
    /// `rate_limit(key = "…")`) but no `rps`. Parallel to
    /// [`Self::RateLimitKeyWithoutCap`] with dynamic-form wording so the fix
    /// names the right attribute.
    #[error(
        "activity '{activity}' declares rate_limit(key = \"{key}\") but no rps; \
         add rps, e.g. rate_limit(key = \"{key}\", rps = 50)"
    )]
    RateLimitKeyExprWithoutCap {
        /// The activity name.
        activity: String,
        /// The dynamic key expression.
        key: String,
    },

    /// A static `rate_limit_key` begins with the reserved `dyn-rate:` prefix
    /// (issue #699). That prefix namespaces per-key/dynamic rate-limit buckets;
    /// a static key beginning with it could collide with a generated dynamic
    /// bucket, so it is rejected to keep the static and dynamic bucket
    /// namespaces provably disjoint.
    #[error(
        "activity '{activity}' sets rate_limit_key = \"{key}\", which begins with the reserved \
         `dyn-rate:` prefix (reserved for per-key/dynamic rate-limit buckets); \
         choose a different rate_limit_key"
    )]
    RateLimitKeyReservedPrefix {
        /// The activity name.
        activity: String,
        /// The offending static key.
        key: String,
    },

    /// A local activity declares a dynamic per-key rate limit
    /// (issue #699, `rate_limit(key = "…")`). Local activities run inline on
    /// the workflow worker via `run_local_activity_inline`, bypassing the
    /// task-dispatch / enqueue path where per-key rate limiting is enforced, so
    /// the limit would be silently unenforced. The `#[activity]` macro rejects
    /// this at parse time; a hand-built `ActivityInfo` (a directly-constructed
    /// `HandlerRegistry`) bypasses the macro, so it is rejected here too.
    #[error(
        "activity '{activity}' is local (is_local = true) but declares a dynamic \
         rate_limit(key = \"{key}\"); local activities run inline on the workflow worker \
         and bypass the task-dispatch path where per-key rate limiting is enforced, so the \
         limit would be silently unenforced -- remove `local = true` or the rate_limit(key = ...)"
    )]
    RateLimitKeyExprOnLocalActivity {
        /// The activity name.
        activity: String,
        /// The dynamic key expression.
        key: String,
    },

    /// An activity declares a `rate_limit_rps` or `rate_limit_burst` that is not
    /// finite and strictly greater than zero (issue #699 review, Codex P2). The
    /// `#[activity]` macro rejects `<= 0` at parse time, but a hand-built
    /// `ActivityInfo` (a directly-constructed `HandlerRegistry`) bypasses the
    /// macro; a non-positive / non-finite rate yields a `burst = tokens = 0`
    /// bucket whose gate can never reach one token, permanently wedging every
    /// scheduled activity on that bucket. Match the macro's universal positivity
    /// check here (and go stricter — also reject `NaN`/`±inf`, which
    /// `n <= 0.0` alone lets through).
    #[error(
        "activity '{activity}' sets {field} to a non-positive or non-finite value; \
         {field} must be finite and greater than zero"
    )]
    RateLimitRateNotPositive {
        /// The activity name.
        activity: String,
        /// The offending field name (`rate_limit_rps` or `rate_limit_burst`).
        field: &'static str,
    },

    /// A completion trigger references an unknown workflow name as a source or target.
    #[non_exhaustive]
    #[error(
        "completion_trigger references unknown workflow '{workflow_name}' as {role}; \
         registered workflows: {registered:?}"
    )]
    UnknownCompletionTriggerWorkflow {
        /// The unrecognised workflow name in the trigger.
        workflow_name: String,
        /// The role the workflow plays ("source" or "target").
        role: &'static str,
        /// All workflow names currently registered on the builder.
        registered: Vec<String>,
    },

    /// A per-workflow-type retention override (issue #737) names a workflow
    /// type that is not registered on this builder — either an explicit
    /// `#[workflow]` or an auto-registered DAG workflow. Caught at build time
    /// so a typo'd override name is a clear error rather than a silently
    /// ignored override. Mirrors [`Self::UnknownCompletionTriggerWorkflow`].
    #[non_exhaustive]
    #[error(
        "retention override names unknown workflow type '{workflow_name}'; \
         register it with .workflows(...) or remove the override. \
         registered workflows: {registered:?}"
    )]
    UnknownRetentionOverrideWorkflow {
        /// The unrecognised workflow name in the retention override.
        workflow_name: String,
        /// All workflow names currently registered on the builder.
        registered: Vec<String>,
    },

    /// A completion trigger carries an output guard that fails
    /// [`crate::completion_trigger::TriggerCondition::validate`] — over the
    /// boundedness caps or with a malformed dotted path (issue #810). Never
    /// silently dropped: registration fails fast.
    #[non_exhaustive]
    #[error("completion_trigger '{trigger_id}' has an invalid condition: {message}")]
    InvalidCompletionTriggerCondition {
        /// The offending trigger's id.
        trigger_id: uuid::Uuid,
        /// The first validation violation found.
        message: String,
    },

    /// `max_workflow_history_events` is set but is not strictly greater than
    /// the configured soft `continue_as_new_threshold`.
    ///
    /// The hard ceiling must always be higher than the advisory threshold so a
    /// workflow that crosses the soft line gets a chance to rotate via
    /// `continue_as_new` before the ceiling terminates it.
    #[error(
        "max_workflow_history_events ({ceiling}) must be strictly greater than \
         history_continue_as_new_threshold ({threshold}); \
         raise the ceiling or lower the soft threshold"
    )]
    HistoryCeilingBelowSoftThreshold {
        /// The configured hard ceiling.
        ceiling: u64,
        /// The configured soft continue-as-new threshold.
        threshold: u64,
    },

    /// A builder-wide default completion-callback target (issue #605) failed
    /// SSRF validation against the configured
    /// [`crate::completion_callback::SsrfPolicy`] host allowlist.
    #[error("completion-callback default target '{url}' rejected: {rejection}")]
    CallbackTargetRejected {
        /// The rejected target URL.
        url: String,
        /// The machine-readable SSRF rejection reason.
        rejection: crate::completion_callback::SsrfRejection,
    },

    /// A native `#[activity]` registration shares its name with a WASM activity
    /// binding (issue #965 review). The native registration would win in the
    /// handler registry while the WASM binding lingered, so the worker's WASM
    /// dispatch seam would run the sandboxed guest under the native activity's
    /// metadata — a silent wrong-implementation. Rejected at build time; pick a
    /// distinct name for one of them.
    #[cfg(feature = "wasm-activities")]
    #[error(
        "activity '{activity}' is registered both as a native activity and as a WASM \
         activity binding; rename one so the name is unambiguous"
    )]
    WasmActivityNameCollision {
        /// The name registered as both native and WASM.
        activity: String,
    },
}

impl BuiltHarvest {
    /// Declarative completion triggers registered on the builder.
    #[must_use]
    pub fn completion_triggers(&self) -> &[crate::completion_trigger::CompletionTrigger] {
        &self.completion_triggers
    }

    #[must_use]
    pub const fn payload_codecs(&self) -> &PayloadCodecs {
        &self.payload_codecs
    }

    /// The `(activity_name, module_bytes)` WASM module registrations to publish
    /// at worker startup (issue #965). Empty when no WASM activity is registered.
    #[cfg(feature = "wasm-activities")]
    #[must_use]
    pub fn wasm_module_registrations(&self) -> &[(String, Vec<u8>)] {
        &self.wasm_module_registrations
    }

    /// The configured large-payload offloader, if a [`PayloadStore`] is
    /// registered (issue #524).
    ///
    /// [`PayloadStore`]: crate::payload_store::PayloadStore
    #[must_use]
    pub const fn payload_offloader(&self) -> Option<&Arc<crate::payload_store::PayloadOffloader>> {
        self.payload_offloader.as_ref()
    }

    /// The configured activity execution interceptor chain (issue #680), in
    /// registration order (index 0 = outermost).
    #[must_use]
    pub fn activity_interceptors(&self) -> &[Arc<dyn crate::interceptor::ActivityInterceptor>] {
        &self.activity_interceptors
    }

    /// History-size guardrails applied to workflow contexts and workers.
    #[must_use]
    pub const fn history_policy(&self) -> WorkflowHistoryPolicy {
        self.history_policy
    }

    /// Number of registered workflows.
    #[must_use]
    pub const fn workflow_count(&self) -> usize {
        self.workflows.len()
    }

    /// Number of registered activities.
    #[must_use]
    pub const fn activity_count(&self) -> usize {
        self.activities.len()
    }

    /// Number of registered DAGs.
    #[must_use]
    pub const fn dag_count(&self) -> usize {
        self.dags.len()
    }

    /// Number of registered workflow schedules.
    #[must_use]
    pub const fn workflow_schedule_count(&self) -> usize {
        self.workflow_schedules.len()
    }

    /// Registered workflow schedules.
    #[must_use]
    pub fn workflow_schedules(&self) -> &[WorkflowSchedule] {
        &self.workflow_schedules
    }

    /// Access typed shared state registered on the builder.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Worker configuration carried through the build step.
    #[must_use]
    pub const fn worker_config(&self) -> &WorkerConfig {
        &self.worker_config
    }

    /// Server-side ceiling on execution timeouts (issue #243).
    ///
    /// `None` means no ceiling is enforced; the per-workflow default and
    /// per-call override are accepted as-is.
    #[must_use]
    pub const fn max_workflow_execution_timeout_ceiling(&self) -> Option<Duration> {
        self.max_workflow_execution_timeout
    }

    /// Server-side ceiling on the chain-scoped lifetime cap, doubling as a
    /// fleet-wide chain default (issue #617). `None` = no chain cap fleet-wide.
    #[must_use]
    pub const fn max_workflow_chain_timeout_ceiling(&self) -> Option<Duration> {
        self.max_workflow_chain_timeout
    }

    /// Registered DAG metadata.
    #[must_use]
    pub fn dags(&self) -> &[DagInfo] {
        &self.dags
    }

    /// Registered workflow metadata.
    ///
    /// Used by the boot-time orphaned-workflow-type reachability gate
    /// (issue #700 AC4) to resolve the registered-name set from the owned
    /// `BuiltHarvest` **before** `HarvestRunner::start` spawns the worker poll
    /// loop — so the gate can refuse boot before any worker can claim and
    /// terminally fail an orphaned-type execution.
    #[must_use]
    pub fn workflow_infos(&self) -> &[WorkflowInfo] {
        &self.workflows
    }

    /// Declarative query handlers collected via `.queries(queries![…])`.
    #[must_use]
    pub fn query_handlers(&self) -> &[QueryHandlerInfo] {
        &self.query_handlers
    }

    /// Declarative update handlers collected via `.updates(updates![…])`.
    #[must_use]
    pub fn update_handlers(&self) -> &[UpdateHandlerInfo] {
        &self.update_handlers
    }

    /// Declarative signal handler metadata collected via `.signals(signals![…])`
    /// (issue #610).
    #[must_use]
    pub fn signal_handlers(&self) -> &[SignalHandlerInfo] {
        &self.signal_handlers
    }

    /// Returns all signal handler infos for the named workflow (issue #610).
    #[must_use]
    pub fn signal_handlers_for(&self, workflow_name: &str) -> Vec<&SignalHandlerInfo> {
        self.signal_handlers
            .iter()
            .filter(|h| h.workflow == workflow_name)
            .collect()
    }

    /// Returns all query handler infos for the named workflow.
    #[must_use]
    pub fn query_handlers_for(&self, workflow_name: &str) -> Vec<&QueryHandlerInfo> {
        self.query_handlers
            .iter()
            .filter(|h| h.workflow == workflow_name)
            .collect()
    }

    /// Returns all update handler infos for the named workflow.
    #[must_use]
    pub fn update_handlers_for(&self, workflow_name: &str) -> Vec<&UpdateHandlerInfo> {
        self.update_handlers
            .iter()
            .filter(|h| h.workflow == workflow_name)
            .collect()
    }

    /// Telemetry configuration (spans propagator + metrics recorder).
    #[must_use]
    pub const fn telemetry(&self) -> &Arc<TelemetryConfig> {
        &self.telemetry
    }

    /// Retention janitor configuration.
    #[must_use]
    pub const fn retention(&self) -> &RetentionConfig {
        &self.retention
    }

    /// Get the registered pre-retention history archiver hook.
    #[must_use]
    pub fn history_archiver(&self) -> Option<&Arc<dyn crate::retention::HistoryArchiver>> {
        self.history_archiver.as_ref()
    }

    /// Resolved builder-wide completion-callback configuration (issue #605).
    #[must_use]
    pub const fn completion_callback_config(
        &self,
    ) -> &crate::completion_callback::CompletionCallbackBuilderConfig {
        &self.completion_callback_config
    }

    /// Override the audit log retention window after the build step.
    ///
    /// Use this to apply a runtime-configured value (e.g. from `HarvestApiState`)
    /// without rebuilding the entire harvest configuration.
    pub const fn set_audit_retention_days(&mut self, days: i64) {
        self.retention.audit_retention_days = days;
    }

    /// Convert the built harvest registration into worker-ready parts.
    #[cfg(feature = "db")]
    #[must_use]
    pub fn into_worker_parts(
        self,
    ) -> (
        crate::worker::HandlerRegistry,
        Vec<DagInfo>,
        Vec<WorkflowSchedule>,
        WorkerConfig,
    ) {
        // issue #921 review: `autumn-harvest-plugin`'s runner is the only
        // installer of `GLOBAL_CALLBACK_CONFIG` this crate ships. A direct
        // (non-plugin) core embedder using this method never routes through
        // it, so completion-callback config would otherwise be silently
        // inert. See `install_global_callback_config_for_direct_worker`.
        crate::completion_callback::install_global_callback_config_for_direct_worker(
            &self.completion_callback_config,
        );
        // issue #808 review (Codex P2): the start-idempotency expiry sweep
        // (`enforce_timeouts_once` -> `sweep_expired_start_idempotency`) reads
        // its retention window from a process-global static, mirroring the
        // callback-config pattern above. `Plugin::build` installs it, but a
        // standalone `HarvestRunner` worker process funnels through this method
        // (via `into_worker_parts_with_extra_state`) without ever calling
        // `Plugin::build` — so in a split web/worker deployment the worker's
        // sweep would otherwise use the DEFAULT 24h window while the web app's
        // reserve honors a custom `start_idempotency_window`, deleting a claim
        // the reserve still considers live and letting a same-key retry create
        // a second execution. Install the configured window here too so every
        // worker (plugin-embedded or standalone) sweeps on the same window.
        crate::start_idempotency::set_purge_window_secs(self.start_idempotency_window);
        #[cfg_attr(not(feature = "wasm-activities"), allow(unused_mut))]
        let mut registry = crate::worker::HandlerRegistry::with_state_and_telemetry(
            self.workflows,
            self.activities,
            Arc::new(self.state),
            self.telemetry,
        )
        .with_handler_infos(
            self.query_handlers,
            self.update_handlers,
            self.signal_handlers,
        )
        .with_history_policy(self.history_policy)
        .with_payload_caps(
            self.max_activity_input_bytes,
            self.max_workflow_input_bytes,
            self.max_activity_result_bytes,
            self.max_signal_payload_bytes,
        )
        .with_current_details_cap(self.max_current_details_bytes)
        .with_workflow_log_policy(self.workflow_log_policy)
        .with_max_workflow_attempts_ceiling(self.max_workflow_attempts)
        .with_max_workflow_chain_timeout(self.max_workflow_chain_timeout)
        // Issue #743 review (PR #1141, Finding #3): thread the fleet-wide
        // execution_timeout ceiling into the registry so the scheduler tick,
        // buffered-drain, and manual DAG trigger paths -- which have no
        // `api_state` -- apply the SAME ceiling manual/HTTP starts already do.
        .with_max_workflow_execution_timeout(self.max_workflow_execution_timeout)
        // Issue #803: name the auto-registered unified DAGs so the core
        // continue-as-new path can reject a cross-type continuation into one
        // (the plugin's `is_registered_dag` is unavailable to the worker).
        .with_dag_workflow_names(
            self.dags
                .iter()
                .filter(|d| d.workflow_handler.is_some())
                .map(|d| d.name.to_string()),
        )
        .with_payload_offloader(self.payload_offloader.clone())
        .with_activity_interceptors(self.activity_interceptors.clone())
        .with_activity_defaults(
            self.worker_config.default_activity_retry_policy.clone(),
            self.worker_config.default_activity_start_to_close,
        )
        .with_retry_after_ceiling(self.worker_config.retry_after_ceiling);
        #[cfg(feature = "wasm-activities")]
        if let Some(store) = self.wasm_store {
            registry = registry.with_wasm_activities(
                store,
                self.wasm_bindings,
                self.wasm_module_registrations,
            );
        }
        (
            registry,
            self.dags,
            self.workflow_schedules,
            self.worker_config,
        )
    }

    /// Convert the built harvest registration into worker-ready parts while
    /// injecting additional typed runtime state.
    #[cfg(feature = "db")]
    #[must_use]
    pub fn into_worker_parts_with_extra_state(
        mut self,
        extra_state: SharedStateMap,
    ) -> (
        crate::worker::HandlerRegistry,
        Vec<DagInfo>,
        Vec<WorkflowSchedule>,
        WorkerConfig,
    ) {
        // See the identical calls in `into_worker_parts` above. This is the
        // method the standalone `HarvestRunner` worker actually funnels through
        // (runner.rs), so installing the configured start-idempotency sweep
        // window here is what closes the split web/worker dedup gap.
        crate::completion_callback::install_global_callback_config_for_direct_worker(
            &self.completion_callback_config,
        );
        crate::start_idempotency::set_purge_window_secs(self.start_idempotency_window);
        self.state.extend(extra_state);
        #[cfg_attr(not(feature = "wasm-activities"), allow(unused_mut))]
        let mut registry = crate::worker::HandlerRegistry::with_state_and_telemetry(
            self.workflows,
            self.activities,
            Arc::new(self.state),
            self.telemetry,
        )
        .with_handler_infos(
            self.query_handlers,
            self.update_handlers,
            self.signal_handlers,
        )
        .with_history_policy(self.history_policy)
        .with_payload_caps(
            self.max_activity_input_bytes,
            self.max_workflow_input_bytes,
            self.max_activity_result_bytes,
            self.max_signal_payload_bytes,
        )
        .with_current_details_cap(self.max_current_details_bytes)
        .with_workflow_log_policy(self.workflow_log_policy)
        .with_max_workflow_attempts_ceiling(self.max_workflow_attempts)
        .with_max_workflow_chain_timeout(self.max_workflow_chain_timeout)
        // Issue #743 review (PR #1141, Finding #3): thread the fleet-wide
        // execution_timeout ceiling into the registry so the scheduler tick,
        // buffered-drain, and manual DAG trigger paths -- which have no
        // `api_state` -- apply the SAME ceiling manual/HTTP starts already do.
        .with_max_workflow_execution_timeout(self.max_workflow_execution_timeout)
        // Issue #803: name the auto-registered unified DAGs so the core
        // continue-as-new path can reject a cross-type continuation into one
        // (the plugin's `is_registered_dag` is unavailable to the worker).
        .with_dag_workflow_names(
            self.dags
                .iter()
                .filter(|d| d.workflow_handler.is_some())
                .map(|d| d.name.to_string()),
        )
        .with_payload_offloader(self.payload_offloader.clone())
        .with_activity_interceptors(self.activity_interceptors.clone())
        .with_activity_defaults(
            self.worker_config.default_activity_retry_policy.clone(),
            self.worker_config.default_activity_start_to_close,
        )
        .with_retry_after_ceiling(self.worker_config.retry_after_ceiling);
        #[cfg(feature = "wasm-activities")]
        if let Some(store) = self.wasm_store {
            registry = registry.with_wasm_activities(
                store,
                self.wasm_bindings,
                self.wasm_module_registrations,
            );
        }
        (
            registry,
            self.dags,
            self.workflow_schedules,
            self.worker_config,
        )
    }
}

impl HarvestBuilder {
    /// Create a new empty builder.
    ///
    /// This starts the fluent configuration chain for registering definitions
    /// and options before finalizing them into a [`BuiltHarvest`] or worker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register completion triggers.
    #[must_use]
    pub fn completion_triggers(
        mut self,
        triggers: Vec<crate::completion_trigger::CompletionTrigger>,
    ) -> Self {
        self.completion_triggers.extend(triggers);
        self
    }

    /// Register a single completion trigger.
    #[must_use]
    pub fn completion_trigger(
        mut self,
        trigger: crate::completion_trigger::CompletionTrigger,
    ) -> Self {
        self.completion_triggers.push(trigger);
        self
    }

    /// Register workflow definitions (output of `workflows![]` macro).
    ///
    /// The runtime uses these definitions to route executions to the correct
    /// handler functions.
    #[must_use]
    pub fn workflows(mut self, workflows: Vec<WorkflowInfo>) -> Self {
        self.workflows.extend(workflows);
        self
    }

    /// Registered workflow metadata, in registration order.
    ///
    /// Used by the plugin's MCP tool generator (issue #597) to select
    /// `mcp`-flagged workflows before the runtime starts. Includes the
    /// auto-registered shadow `WorkflowInfo` for each unified DAG (see
    /// [`Self::dag_infos`]).
    #[must_use]
    pub fn workflow_infos(&self) -> &[WorkflowInfo] {
        &self.workflows
    }

    /// Registered activity metadata, in registration order.
    ///
    /// Pre-build accessor used by the plugin's built-in synthetic liveness
    /// canary (issue #796) to confirm the reserved canary activity is
    /// registered exactly once before the runtime starts.
    #[must_use]
    pub fn activity_infos(&self) -> &[ActivityInfo] {
        &self.activities
    }

    /// Registered workflow schedules, in registration order.
    ///
    /// Pre-build accessor used by the plugin's built-in synthetic liveness
    /// canary (issue #796) to assert the per-writable-shard probe schedule was
    /// registered.
    #[must_use]
    pub fn workflow_schedules(&self) -> &[WorkflowSchedule] {
        &self.workflow_schedules
    }

    /// Read access to the worker configuration.
    ///
    /// Pre-build counterpart of [`Self::worker_config_mut`], used by the
    /// synthetic liveness canary (issue #796) to confirm a probe queue is in
    /// the worker's drained-queue set.
    #[must_use]
    pub const fn worker_config(&self) -> &WorkerConfig {
        &self.worker_config
    }

    /// Read access to the retention configuration.
    ///
    /// Pre-build counterpart of the consuming [`Self::retention`] setter, used
    /// by the synthetic liveness canary (issue #796) to add per-workflow
    /// self-cleaning retention overrides while preserving any existing config.
    #[must_use]
    pub const fn retention_config(&self) -> &RetentionConfig {
        &self.retention
    }

    /// Registered DAG metadata, in registration order.
    ///
    /// Pre-build counterpart of [`BuiltHarvest::dags`]. Used by the plugin's
    /// MCP tool generator to distinguish a unified DAG's auto-registered
    /// `WorkflowInfo` (see [`Self::workflow_infos`]) from an ordinary
    /// workflow, so the DAG's `start` tool can route through the DAG trigger
    /// contract rather than generic workflow start.
    #[must_use]
    pub fn dag_infos(&self) -> &[DagInfo] {
        &self.dags
    }

    /// Registered declarative update handlers, in registration order.
    ///
    /// Pre-build counterpart of [`BuiltHarvest::update_handlers`] (issue #597).
    #[must_use]
    pub fn update_handlers(&self) -> &[UpdateHandlerInfo] {
        &self.update_handlers
    }

    /// The configured payload-codec registry.
    ///
    /// Pre-build counterpart of [`BuiltHarvest::payload_codecs`] (issue #608):
    /// lets the plugin mirror the registry onto its API state at `build()`
    /// time — alongside the `decode_payloads_on_read` opt-in flag — instead
    /// of waiting for the runtime startup hook, so there is no boot window
    /// where a decode-eligible request sees a default identity-only registry.
    /// `try_build` clones this registry verbatim, so the pre-build and
    /// post-build views are identical.
    #[must_use]
    pub const fn payload_codecs(&self) -> &PayloadCodecs {
        &self.payload_codecs
    }

    /// Registered declarative query handlers, in registration order.
    ///
    /// Pre-build counterpart of [`BuiltHarvest::query_handlers`] (issue #597).
    #[must_use]
    pub fn query_handlers(&self) -> &[QueryHandlerInfo] {
        &self.query_handlers
    }

    /// Registered declarative signal handler metadata, in registration order.
    ///
    /// Pre-build counterpart of [`BuiltHarvest::signal_handlers`] (issue #610).
    #[must_use]
    pub fn signal_handlers(&self) -> &[SignalHandlerInfo] {
        &self.signal_handlers
    }

    /// Register activity definitions (output of `activities![]` macro).
    ///
    /// The runtime maps activity tasks to these definitions for execution.
    #[must_use]
    pub fn activities(mut self, activities: Vec<ActivityInfo>) -> Self {
        self.activities.extend(activities);
        self
    }

    /// Register DAG definitions (output of `dags![]` macro).
    ///
    /// DAGs define graphs of steps that run according to a schedule.
    ///
    /// When the `unified-dag-execution` feature is enabled every DAG whose
    /// `workflow_handler` is populated (i.e. produced by the `#[dag]` macro
    /// with that feature on) is also auto-registered as a [`WorkflowInfo`] and,
    /// if it carries a schedule attribute, as a [`WorkflowSchedule`]. This
    /// wires unified DAGs into the standard workflow execution and scheduler
    /// paths without requiring separate `.workflow_schedule(...)` calls.
    #[must_use]
    pub fn dags(mut self, dags: Vec<DagInfo>) -> Self {
        for dag in dags {
            #[cfg(feature = "unified-dag-execution")]
            {
                if let Some(workflow_info) = dag.as_workflow_info() {
                    self.auto_registered_dag_workflows
                        .push(workflow_info.name.to_string());
                    self.workflows.push(workflow_info);
                }
                if let Some(workflow_schedule) = dag.as_workflow_schedule() {
                    self.workflow_schedules.push(workflow_schedule);
                }
            }
            self.dags.push(dag);
        }
        self
    }

    /// Register declarative query handlers (output of `queries![…]` macro).
    ///
    /// Each [`QueryHandlerInfo`] is associated with a specific workflow name via
    /// the `workflow = "…"` attribute. The runtime uses this list to auto-register
    /// handlers before the workflow function runs, and the management API exposes
    /// them via `GET /workflows/types/{name}/handlers`.
    ///
    /// Calling this method multiple times appends all provided handlers.
    #[must_use]
    pub fn queries(mut self, handlers: Vec<QueryHandlerInfo>) -> Self {
        self.query_handlers.extend(handlers);
        self
    }

    /// Register declarative update handlers (output of `updates![…]` macro).
    ///
    /// Each [`UpdateHandlerInfo`] is associated with a specific workflow name via
    /// the `workflow = "…"` attribute. The runtime uses this list to auto-register
    /// handlers before the workflow function runs, and the management API exposes
    /// them via `GET /workflows/types/{name}/handlers`.
    ///
    /// Calling this method multiple times appends all provided handlers.
    #[must_use]
    pub fn updates(mut self, handlers: Vec<UpdateHandlerInfo>) -> Self {
        self.update_handlers.extend(handlers);
        self
    }

    /// Register declarative signal handler metadata (output of `signals![…]`
    /// macro) for self-service interface discovery (issue #610).
    ///
    /// Each [`SignalHandlerInfo`] is associated with a specific workflow name via
    /// the `workflow = "…"` attribute and carries an optional payload schema and
    /// description. This metadata is published by the management API; it does not
    /// register a runtime handler (push handlers register inside the workflow body
    /// via [`WorkflowContext::register_signal_handler`](crate::context::WorkflowContext)).
    ///
    /// Calling this method multiple times appends all provided handlers.
    #[must_use]
    pub fn signals(mut self, handlers: Vec<SignalHandlerInfo>) -> Self {
        self.signal_handlers.extend(handlers);
        self
    }

    /// Register a per-workflow cron/interval schedule.
    ///
    /// The referenced `workflow_name` must appear in a prior (or subsequent)
    /// `.workflows(workflows![...])` call. [`Self::try_build`] validates this
    /// and returns [`HarvestBuilderError::UnknownWorkflowSchedule`] if the
    /// workflow is missing.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::builder::{HarvestBuilder, HarvestBuilderError};
    /// use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    ///
    /// // Referencing an unregistered workflow name is caught at try_build time.
    /// let result = HarvestBuilder::new()
    ///     .workflow_schedule(
    ///         WorkflowSchedule::new("daily_billing_report", Schedule::Cron("0 3 * * *".to_string()))
    ///     )
    ///     .try_build();
    /// assert!(matches!(result, Err(HarvestBuilderError::UnknownWorkflowSchedule { .. })));
    /// ```
    #[must_use]
    pub fn workflow_schedule(mut self, schedule: WorkflowSchedule) -> Self {
        self.workflow_schedules.push(schedule);
        self
    }

    /// Configure the worker (concurrency, queues, timeouts).
    ///
    /// See [`WorkerConfig`] for details on adjusting poll behavior.
    #[must_use]
    pub fn worker(mut self, config: WorkerConfig) -> Self {
        self.worker_config = config;
        self
    }

    /// Access mutable worker configuration.
    pub const fn worker_config_mut(&mut self) -> &mut WorkerConfig {
        &mut self.worker_config
    }

    #[cfg(feature = "db")]
    /// Set the sharded database pool on the worker config.
    #[must_use]
    pub fn with_sharded_pool(mut self, pool: crate::shard::ShardedDbPool) -> Self {
        self.worker_config.sharded_pool = Some(pool);
        self
    }

    /// Register typed shared state visible to workflow and activity handlers.
    ///
    /// State injected here can be retrieved in your handlers by calling
    /// `ctx.state::<T>()`. It is useful for sharing database connection pools,
    /// email clients, or configuration structs across tasks.
    ///
    /// Registering the same type more than once replaces the previous value.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(mut self, value: T) -> Self {
        self.state.insert(TypeId::of::<T>(), Box::new(value));
        self
    }

    /// Install a [`TelemetryConfig`] so the worker captures trace context at
    /// enqueue, reinstates it on claim, and emits workflow / activity / timer
    /// metrics through the supplied recorder.
    ///
    /// When unset, the runtime uses safe no-op defaults — telemetry is opt-in.
    #[must_use]
    pub fn payload_codec(mut self, codec: impl PayloadCodec + 'static) -> Self {
        self.payload_codecs.set_default(Arc::new(codec));
        self
    }

    /// Register a **keyed** payload codec under `key_id` for key rotation
    /// (issue #948).
    ///
    /// Unlike [`HarvestBuilder::payload_codec`], which installs one default
    /// codec, this builds a registry of codecs distinguished by *key material*:
    /// during a rotation two codecs share a `codec_id` (`"aes-gcm"`) and differ
    /// only in the key they hold, so `codec_id` cannot tell them apart and the
    /// stored envelope carries a `kid` instead.
    ///
    /// The **first** key registered becomes the active key (the one new writes
    /// are encoded under); rotate with
    /// [`HarvestBuilder::active_payload_codec_key`].
    ///
    /// Registering your pre-rotation codec under
    /// [`CODEC_LEGACY_KEY_ID`](crate::payload_codec::CODEC_LEGACY_KEY_ID) is
    /// what lets already-stored, `kid`-less history keep decoding.
    ///
    /// # Panics
    ///
    /// Panics when `key_id` is empty, longer than
    /// [`MAX_CODEC_KEY_ID_BYTES`](crate::payload_codec::MAX_CODEC_KEY_ID_BYTES),
    /// or contains anything outside ASCII alphanumerics and `-_.:`. A codec key
    /// id is a compile-time-constant deployment decision, not runtime input, so
    /// a malformed one is a configuration bug that must not boot.
    #[must_use]
    pub fn payload_codec_key(self, key_id: &str, codec: impl PayloadCodec + 'static) -> Self {
        self.payload_codecs
            .register_key(key_id, Arc::new(codec))
            .expect("invalid payload codec key id");
        self
    }

    /// Make an already-registered payload-codec key the **active** one — every
    /// new write encodes under it (issue #948).
    ///
    /// # Panics
    ///
    /// Panics when `key_id` was not registered with
    /// [`HarvestBuilder::payload_codec_key`]. Activating a key this process
    /// cannot encode with must not boot.
    #[must_use]
    pub fn active_payload_codec_key(self, key_id: &str) -> Self {
        self.payload_codecs
            .set_active_key(key_id)
            .expect("unregistered payload codec key id");
        self
    }

    /// Register an external [`PayloadStore`](crate::payload_store::PayloadStore)
    /// for large-payload offloading via claim-check (issue #524).
    ///
    /// Once registered, any payload-bearing field larger than
    /// [`payload_offload_threshold`](HarvestBuilder::payload_offload_threshold)
    /// is written to the store and replaced inline with a small reference
    /// envelope, so big blobs flow between steps without bloating
    /// `harvest_events` or tripping the #252 size cap. With no store registered,
    /// behaviour is unchanged.
    #[must_use]
    pub fn payload_store(mut self, store: impl crate::payload_store::PayloadStore) -> Self {
        self.payload_store = Some(Arc::new(store));
        self
    }

    /// Set the byte threshold above which payload-bearing fields are offloaded
    /// to the registered [`PayloadStore`](crate::payload_store::PayloadStore)
    /// (issue #524). Fields at or below the threshold stay inline. Default:
    /// [`DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD`] (256 KiB). No effect without a store.
    #[must_use]
    pub const fn payload_offload_threshold(mut self, bytes: u64) -> Self {
        self.payload_offload_threshold = bytes;
        self
    }

    #[must_use]
    pub fn telemetry(mut self, telemetry: TelemetryConfig) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Configure retention janitor behavior for completed workflow history.
    #[must_use]
    pub fn retention(mut self, retention: RetentionConfig) -> Self {
        self.retention = retention;
        self
    }

    /// Register a pre-retention history archiver hook.
    #[must_use]
    pub fn history_archiver(mut self, archiver: impl crate::retention::HistoryArchiver) -> Self {
        self.history_archiver = Some(Arc::new(archiver));
        self
    }

    /// Register an activity execution interceptor (issue #680).
    ///
    /// Interceptors wrap **every** activity execution on the worker — regular
    /// and local. Call this repeatedly to build an ordered chain: the
    /// **first-registered interceptor is the OUTERMOST wrapper** (runs first on
    /// the way in, last on the way out) and the activity handler is innermost.
    ///
    /// An interceptor may transform the input, transform the result or error,
    /// or short-circuit (returning without calling `next.run`, so the handler
    /// never runs). An interceptor error and panic are contained on the same
    /// retry / circuit-breaker / dead-letter path as a handler error/panic. See
    /// [`crate::interceptor`] for the full contract.
    #[must_use]
    pub fn activity_interceptor(
        mut self,
        interceptor: impl crate::interceptor::ActivityInterceptor,
    ) -> Self {
        self.activity_interceptors.push(Arc::new(interceptor));
        self
    }

    /// Register a sandboxed WebAssembly activity (issue #965).
    ///
    /// The activity is registered as a normal (non-local) `ActivityInfo` whose
    /// module bytes are published to each worker's shard database at startup and
    /// whose guest is run through the worker's WASM dispatch seam instead of a
    /// native handler. Capabilities are deny-all by default; grant them (and
    /// override the resource limits, queue, retry policy, or start-to-close) via
    /// the [`WasmActivityRegistration`](crate::wasm_store::WasmActivityRegistration)
    /// fluent setters.
    ///
    /// The shared WASM engine + compiled-module cache is created lazily on the
    /// first call and reused across every registered WASM activity.
    #[cfg(feature = "wasm-activities")]
    #[must_use]
    pub fn wasm_activity(
        mut self,
        registration: crate::wasm_store::WasmActivityRegistration,
    ) -> Self {
        // Leak the name/queue to `'static` for the placeholder `ActivityInfo`
        // (mirrors the MCP-tool route-generation precedent). A builder is a
        // one-time process-startup object, so a bounded leak per registered
        // activity is acceptable.
        let name: &'static str = Box::leak(registration.name.clone().into_boxed_str());
        let queue: Option<&'static str> = registration
            .queue
            .as_ref()
            .map(|q| &*Box::leak(q.clone().into_boxed_str()));
        self.activities.push(crate::info::ActivityInfo::wasm(
            name,
            queue,
            Some(registration.retry.clone()),
            registration.start_to_close,
            registration.schedule_to_close,
        ));
        self.wasm_bindings
            .insert(registration.name.clone(), registration.binding());
        if self.wasm_store.is_none() {
            self.wasm_store = Some(Arc::new(crate::wasm_activities::WasmModuleStore::new()));
        }
        // Keep the registration byte-blobs last-wins consistent with the binding
        // (`wasm_bindings.insert`) and the `ActivityInfo` registry, both of which
        // use the LATER definition on a duplicate name (issue #965 review). A
        // blind append would leave the STALE first blob's bytes in the vector;
        // `seed_registered_wasm_modules` activates the first same-name row when
        // none is active, so on a clean shard it would seed+activate those stale
        // bytes under the newer binding/retry/queue metadata — a mismatch.
        // Replace-on-insert so exactly one entry per name survives, matching the
        // last registration.
        if let Some(existing) = self
            .wasm_module_registrations
            .iter_mut()
            .find(|(existing_name, _)| *existing_name == registration.name)
        {
            existing.1 = registration.wasm_bytes;
        } else {
            self.wasm_module_registrations
                .push((registration.name, registration.wasm_bytes));
        }
        self
    }

    /// Register a builder-wide default completion-callback target (issue
    /// #605): a URL that receives a signed POST of the terminal result for
    /// every workflow whose effective target set includes it (per-execution
    /// targets set via a start option are unioned with these defaults).
    ///
    /// The target is validated against the configured SSRF host allowlist
    /// at [`try_build`](Self::try_build) time — call
    /// [`completion_callback_allowlist`](Self::completion_callback_allowlist)
    /// first if the target's host isn't already allowlisted, or `try_build`
    /// returns [`HarvestBuilderError::CallbackTargetRejected`].
    #[must_use]
    pub fn completion_callback_default(
        mut self,
        url: impl Into<String>,
        filter: crate::completion_callback::EventFilter,
    ) -> Self {
        self.completion_callback_config
            .default_targets
            .push(crate::completion_callback::CallbackTarget::new(url, filter));
        self
    }

    /// Configure the SSRF host allowlist for completion-callback targets
    /// (issue #605). Every registered target (builder default or
    /// per-execution) must match an entry here (exact host or `*.suffix`)
    /// or it is rejected at registration time.
    #[must_use]
    pub fn completion_callback_allowlist(
        mut self,
        allowlist: crate::completion_callback::HostAllowlist,
    ) -> Self {
        self.completion_callback_config.allowlist = allowlist;
        self
    }

    /// Permit `http://` completion-callback targets (default: `https://` only).
    #[must_use]
    pub const fn completion_callback_allow_http(mut self, allow: bool) -> Self {
        self.completion_callback_config.allow_http = allow;
        self
    }

    /// Permit IP-literal completion-callback target hosts, subject to the
    /// private/loopback/link-local rejection rules (default: rejected).
    #[must_use]
    pub const fn completion_callback_allow_ip_literals(mut self, allow: bool) -> Self {
        self.completion_callback_config.allow_ip_literals = allow;
        self
    }

    /// Set the HMAC secret used to sign every completion-callback delivery's
    /// `X-Harvest-Signature` header (issue #605), so receivers can verify
    /// authenticity and reject replays. If never called, deliveries are
    /// still signed but with an empty key — operators enabling completion
    /// callbacks should always set a real secret.
    #[must_use]
    pub fn completion_callback_secret(mut self, secret: impl Into<Vec<u8>>) -> Self {
        self.completion_callback_config.secret =
            Some(crate::completion_callback::CallbackSecret::new(secret));
        self
    }

    /// Override the default delivery retry policy (issue #605). Defaults to
    /// [`crate::completion_callback::default_delivery_retry_policy`] (~1 hour window).
    #[must_use]
    pub fn completion_callback_retry_policy(mut self, policy: crate::policy::RetryPolicy) -> Self {
        self.completion_callback_config.retry_policy = policy;
        self
    }

    /// Override the outbound HTTP transport for completion-callback delivery
    /// (issue #605). When not called, the plugin substitutes its default
    /// `reqwest`-based implementation at startup — core ships no HTTP client.
    #[must_use]
    pub fn completion_callback_deliverer(
        mut self,
        deliverer: impl crate::completion_callback::CompletionCallbackDeliverer,
    ) -> Self {
        self.completion_callback_config.deliverer = Some(Arc::new(deliverer));
        self
    }

    /// Override the soft history-size threshold used by
    /// [`crate::context::WorkflowContext::should_continue_as_new`].
    #[must_use]
    pub const fn history_continue_as_new_threshold(mut self, threshold: u64) -> Self {
        self.history_policy = self
            .history_policy
            .with_continue_as_new_threshold(threshold);
        self
    }

    /// Override the deadline fraction used by
    /// [`crate::context::WorkflowContext::should_continue_as_new`] for
    /// deadline-aware continue-as-new (issue #772). Clamped into `[0.0, 1.0]`.
    ///
    /// Defaults to
    /// [`DEFAULT_CONTINUE_AS_NEW_DEADLINE_FRACTION`](crate::context::DEFAULT_CONTINUE_AS_NEW_DEADLINE_FRACTION)
    /// (`0.8`).
    #[must_use]
    pub const fn history_continue_as_new_deadline_fraction(mut self, fraction: f64) -> Self {
        self.history_policy = self
            .history_policy
            .with_continue_as_new_deadline_fraction(fraction);
        self
    }

    /// Configure an opt-in hard cap for workflow history event counts.
    #[must_use]
    pub const fn history_event_hard_cap(mut self, cap: u64) -> Self {
        self.history_policy = self.history_policy.with_event_hard_cap(cap);
        self
    }

    /// Override the fraction of [`history_event_hard_cap`](Self::history_event_hard_cap)
    /// at which the operator early-warning soft threshold fires (issue #704).
    /// Clamped into `[0.0, 1.0]`; `0.0` disables the signal entirely (AC4).
    ///
    /// Has no effect unless a hard cap is also configured -- with no hard
    /// cap there is nothing to warn about approaching.
    ///
    /// Defaults to
    /// [`DEFAULT_HISTORY_BLOAT_WARN_FRACTION`](crate::context::DEFAULT_HISTORY_BLOAT_WARN_FRACTION)
    /// (`0.75`).
    #[must_use]
    pub const fn history_bloat_warn_fraction(mut self, fraction: f64) -> Self {
        self.history_policy = self
            .history_policy
            .with_history_bloat_warn_fraction(fraction);
        self
    }

    /// Set a server-side hard ceiling on the number of durable events a RUNNING
    /// workflow execution may accumulate (issue #493).
    ///
    /// When set, the background timeout scanner terminates any execution whose
    /// recorded event count reaches or exceeds `ceiling` with `WorkflowFailed`
    /// and a machine-readable error reason of the form
    /// `"history_ceiling_exceeded: event count {n} >= ceiling {c}"`.
    ///
    /// `None` (the default) means no ceiling is enforced.
    ///
    /// The ceiling MUST be strictly greater than `history_continue_as_new_threshold`
    /// (default 10,000). A misconfiguration is caught by [`Self::try_build`].
    #[must_use]
    pub const fn max_workflow_history_events(mut self, ceiling: Option<u64>) -> Self {
        self.max_workflow_history_events = ceiling;
        self
    }

    /// Set a server-side ceiling on `execution_timeout` for all workflows (issue #243).
    ///
    /// When set, any `start_workflow` call that provides an `execution_timeout`
    /// larger than this ceiling is rejected. This acts as a defense against client
    /// bugs that accidentally request absurdly long deadlines.
    ///
    /// `None` (the default) means no ceiling is enforced — per-workflow defaults
    /// and per-call overrides are accepted as-is.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::builder::HarvestBuilder;
    /// use std::time::Duration;
    ///
    /// let built = HarvestBuilder::new()
    ///     .max_workflow_execution_timeout(Duration::from_secs(86_400)) // 24h ceiling
    ///     .build();
    ///
    /// assert_eq!(built.max_workflow_execution_timeout, Some(Duration::from_secs(86_400)));
    /// ```
    #[must_use]
    pub const fn max_workflow_execution_timeout(mut self, ceiling: Duration) -> Self {
        self.max_workflow_execution_timeout = Some(ceiling);
        self
    }

    /// Set a server-side ceiling on the chain-scoped lifetime cap (issue #617).
    ///
    /// This ceiling caps any workflow-declared `chain_execution_timeout` AND acts
    /// as a fleet-wide default: a workflow that declares no chain cap still
    /// inherits this value as its chain deadline. `None` (the default) means no
    /// chain cap is applied fleet-wide.
    #[must_use]
    pub const fn max_workflow_chain_timeout(mut self, ceiling: Duration) -> Self {
        self.max_workflow_chain_timeout = Some(ceiling);
        self
    }

    /// Set a server-side ceiling on workflow retry attempts (issue #523).
    ///
    /// When set, `retry_policy.max_attempts` is clamped to `min(max_attempts, ceiling)`.
    /// `None` (the default) means no ceiling is enforced.
    #[must_use]
    pub const fn max_workflow_attempts(mut self, ceiling: u32) -> Self {
        self.max_workflow_attempts = Some(ceiling);
        self
    }

    /// Set the retention window for request-scoped start idempotency keys
    /// (issue #808).
    ///
    /// A repeated `idempotency_key` on `POST /workflows/{name}/start` within this
    /// window deduplicates onto the same execution (returning it as a no-op);
    /// once the window elapses, the same key is reusable. Defaults to
    /// [`crate::start_idempotency::DEFAULT_START_IDEMPOTENCY_WINDOW`] (24h).
    #[must_use]
    pub const fn start_idempotency_window(mut self, window: Duration) -> Self {
        self.start_idempotency_window = Some(window);
        self
    }

    /// The configured start-idempotency retention window, or `None` if unset
    /// (the default 24h applies at build time).
    ///
    /// Read-only pre-build accessor (issue #695).
    #[must_use]
    pub const fn start_idempotency_window_config(&self) -> Option<Duration> {
        self.start_idempotency_window
    }

    /// Override the ceiling on the `[from, to]` window accepted by
    /// `GET /admin/usage` (issue #596).
    ///
    /// Defaults to 90 days (`crate::usage::default_usage_window_ceiling()`).
    #[must_use]
    pub const fn usage_window_ceiling(mut self, ceiling: Duration) -> Self {
        self.usage_window_ceiling = Some(ceiling);
        self
    }

    /// Override the cap on distinct groups `GET /admin/usage` will return
    /// before failing loudly with `413` (issue #596).
    ///
    /// Defaults to 10,000 (`crate::usage::default_usage_max_groups()`). A
    /// chargeback report must never silently drop a low-volume tenant's
    /// data, so exceeding this cap is a hard error naming the ceiling
    /// rather than a silent top-N rollup.
    #[must_use]
    pub const fn usage_max_groups(mut self, cap: usize) -> Self {
        self.usage_max_groups = Some(cap);
        self
    }

    /// Set the global maximum byte length for activity input payloads (issue #252).
    ///
    /// Default: 2 MiB. Per-activity overrides declared via
    /// `#[activity(max_input_bytes = "…")]` raise (never lower) this ceiling for
    /// a specific activity.
    #[must_use]
    pub const fn max_activity_input_bytes(mut self, bytes: u64) -> Self {
        self.max_activity_input_bytes = bytes;
        self
    }

    /// Set the global maximum byte length for activity result payloads (issue #252).
    ///
    /// Default: 2 MiB. Per-activity overrides declared via
    /// `#[activity(max_result_bytes = "…")]` raise (never lower) this ceiling for
    /// a specific activity.
    #[must_use]
    pub const fn max_activity_result_bytes(mut self, bytes: u64) -> Self {
        self.max_activity_result_bytes = bytes;
        self
    }

    /// Set the global maximum byte length for signal payloads (issue #252).
    ///
    /// Default: 256 KiB. Enforcement happens at the management-API
    /// signal-send boundary before any `SignalReceived` event is appended.
    #[must_use]
    pub const fn max_signal_payload_bytes(mut self, bytes: u64) -> Self {
        self.max_signal_payload_bytes = bytes;
        self
    }

    /// Set the global maximum byte length for workflow start input payloads
    /// (issue #252).
    ///
    /// Default: 2 MiB. Enforcement happens at `start_workflow` time before the
    /// `WorkflowStarted` event or `harvest_workflow_executions` row is inserted.
    /// Per-workflow-type overrides declared via `#[workflow(max_input_bytes = "…")]`
    /// raise (never lower) this ceiling for a specific workflow type.
    #[must_use]
    pub const fn max_workflow_input_bytes(mut self, bytes: u64) -> Self {
        self.max_workflow_input_bytes = bytes;
        self
    }

    /// Set the global maximum byte length for `current_details` strings set via
    /// `ctx.set_current_details(...)` (issue #473).
    ///
    /// Values longer than the cap are silently truncated to the nearest valid
    /// UTF-8 character boundary at or before the cap. The truncation happens
    /// identically on the live and replay paths so it cannot itself cause a
    /// non-determinism divergence.
    ///
    /// Default: 1 KiB.
    #[must_use]
    pub const fn with_current_details_cap(mut self, cap_bytes: usize) -> Self {
        self.max_current_details_bytes = cap_bytes;
        self
    }

    /// Enable the **opt-in** durable per-execution workflow-log sink (issue #790).
    ///
    /// With a policy installed, the lines a workflow emits through the existing
    /// [`ctx.logger()`](crate::context::WorkflowContext::logger) /
    /// `ctx.log_info` / `ctx.log_warn` / `ctx.log_error` entry points (issue
    /// #379) are additionally persisted to the per-execution
    /// `harvest_workflow_logs` table and readable via
    /// `GET /api/harvest/workflows/{id}/logs` and the Vantage UI Logs panel —
    /// so triaging a failed run no longer requires pivoting to external log
    /// aggregation and correlating by execution id.
    ///
    /// **Off by default, and additive.** Without this call `ctx.logger()`
    /// behaves exactly as before: `tracing`-only, no command pushed, no write.
    /// The `tracing` sink is unchanged either way — this *adds* a sink, it does
    /// not replace one.
    ///
    /// Log lines are **observational only**: they are not part of the event
    /// history, carry no determinism guarantee, and must never be read back
    /// into workflow logic.
    ///
    /// ```rust,no_run
    /// # use autumn_harvest::builder::HarvestBuilder;
    /// # use autumn_harvest::context::WorkflowLogPolicy;
    /// # let builder = HarvestBuilder::new();
    /// // Defaults: 1,000 lines per execution, 4 KiB per line.
    /// let builder = builder.workflow_log_persistence(WorkflowLogPolicy::new());
    /// ```
    #[must_use]
    pub const fn workflow_log_persistence(
        mut self,
        policy: crate::context::WorkflowLogPolicy,
    ) -> Self {
        self.workflow_log_policy = Some(policy);
        self
    }

    /// Set the global maximum allowed start delay for a workflow (issue #322).
    ///
    /// Default: 365 days.
    #[must_use]
    pub const fn max_workflow_start_delay(mut self, delay: Duration) -> Self {
        self.max_workflow_start_delay = Some(delay);
        self
    }

    /// Set the grace window before cross-workflow signaling fails for unknown target (issue #330).
    ///
    /// Default: 5 seconds.
    #[must_use]
    pub const fn unknown_target_grace_window(mut self, window: Duration) -> Self {
        self.unknown_target_grace_window = Some(window);
        self
    }

    /// Override the hard caps for `POST /workflows/batch_start` (issue #357).
    ///
    /// Defaults: `max_items_per_batch = 1000`, `max_total_bytes = 10 MiB`.
    /// Both limits are checked before any execution row is inserted; exceeding
    /// either returns `413 Payload Too Large`.
    #[must_use]
    pub const fn batch_start_config(mut self, config: BatchStartConfig) -> Self {
        self.batch_start_config = config;
        self
    }

    /// Number of registered workflows (used in tests and diagnostics).
    #[must_use]
    pub const fn workflow_count(&self) -> usize {
        self.workflows.len()
    }

    /// Number of registered activities.
    #[must_use]
    pub const fn activity_count(&self) -> usize {
        self.activities.len()
    }

    /// Number of registered DAG definitions.
    #[must_use]
    pub const fn dag_count(&self) -> usize {
        self.dags.len()
    }

    /// Number of registered workflow schedules.
    #[must_use]
    pub const fn workflow_schedule_count(&self) -> usize {
        self.workflow_schedules.len()
    }

    /// Finalize the builder into a reusable harvest registration set.
    ///
    /// # Panics
    ///
    /// Panics when retention settings are invalid. Prefer [`Self::try_build`]
    /// if you want startup errors instead.
    #[must_use]
    pub fn build(self) -> BuiltHarvest {
        self.try_build()
            .expect("HarvestBuilder::build failed validation")
    }

    /// Finalize the builder into a reusable harvest registration set.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestBuilderError`] when retention settings are invalid,
    /// when activities sharing a `concurrency_key` declare different
    /// `max_concurrent` values, or when a [`WorkflowSchedule`] references a
    /// workflow name not registered on this builder.
    #[allow(clippy::too_many_lines)]
    pub fn try_build(self) -> Result<BuiltHarvest, HarvestBuilderError> {
        self.retention
            .validate()
            .map_err(HarvestBuilderError::InvalidRetention)?;
        validate_retention_overrides(
            &self.retention,
            &self.workflows,
            &self.auto_registered_dag_workflows,
        )?;

        if self.worker_config.worker_heartbeat_interval.is_zero() {
            return Err(HarvestBuilderError::InvalidWorkerConfig(
                "worker_heartbeat_interval must be greater than zero".to_string(),
            ));
        }
        // Issue #804 (Codex round-19 P1): surface a cadence the fleet-wide
        // capability-miss liveness window cannot vouch for. Never rejects — an
        // already-deployed slow fleet must keep booting.
        #[cfg(feature = "db")]
        warn_if_heartbeat_outruns_fleet_liveness(self.worker_config.worker_heartbeat_interval);

        validate_concurrency_keys(&self.activities)?;
        validate_workflow_concurrency_limits(&self.workflows)?;
        validate_workflow_throttle_policies(&self.workflows)?;
        validate_dag_workflow_name_collisions(
            &self.workflows,
            &self.auto_registered_dag_workflows,
        )?;
        validate_workflow_schedules(
            &self.workflow_schedules,
            &self.workflows,
            &self.auto_registered_dag_workflows,
        )?;
        validate_completion_triggers(
            &self.completion_triggers,
            &self.workflows,
            &self.auto_registered_dag_workflows,
        )?;
        validate_local_activity_timeouts(
            &self.activities,
            self.worker_config.max_local_activity_start_to_close,
        )?;
        validate_dags_do_not_use_local_activities(&self.dags, &self.activities)?;
        validate_classic_dags_have_no_signal_gates(&self.dags)?;
        validate_classic_dags_have_no_compensators(&self.dags)?;
        validate_dag_schedules(&self.dags)?;
        validate_activity_rate_limits(&self.activities)?;
        #[cfg(feature = "wasm-activities")]
        validate_wasm_activity_name_collisions(&self.wasm_bindings, &self.activities)?;
        if let Err((url, rejection)) = self.completion_callback_config.validate_default_targets() {
            return Err(HarvestBuilderError::CallbackTargetRejected { url, rejection });
        }

        if let Some(ceiling) = self.max_workflow_history_events {
            let threshold = self.history_policy.continue_as_new_threshold();
            if ceiling <= threshold {
                return Err(HarvestBuilderError::HistoryCeilingBelowSoftThreshold {
                    ceiling,
                    threshold,
                });
            }
        }

        let mut worker_config = self.worker_config;
        let max_workflow_start_delay = self
            .max_workflow_start_delay
            .unwrap_or(worker_config.max_workflow_start_delay);
        worker_config.max_workflow_start_delay = max_workflow_start_delay;

        let unknown_target_grace_window = self
            .unknown_target_grace_window
            .unwrap_or(worker_config.unknown_target_grace_window);
        worker_config.unknown_target_grace_window = unknown_target_grace_window;

        // Issue #948: the worker's background re-encryption sweep must read the
        // SAME codec registry the builder configured — not a fresh default,
        // which would hold no keys and silently sweep nothing. This is the one
        // choke point every built runtime passes through, so a runner cannot
        // forget to wire it. The registry's rotation state is shared across
        // clones, so a later `set_active_key` still reaches the running worker.
        worker_config.payload_codecs = self.payload_codecs.clone();

        let usage_window_ceiling = self
            .usage_window_ceiling
            .unwrap_or_else(crate::usage::default_usage_window_ceiling);
        let usage_max_groups = self
            .usage_max_groups
            .unwrap_or_else(crate::usage::default_usage_max_groups);

        let telemetry_arc = Arc::new(self.telemetry.unwrap_or_default());
        let payload_offloader = self.payload_store.clone().map(|store| {
            Arc::new(crate::payload_store::PayloadOffloader::new(
                store,
                self.payload_offload_threshold,
                telemetry_arc.metrics.clone(),
            ))
        });
        Ok(BuiltHarvest {
            workflows: self.workflows,
            activities: self.activities,
            dags: self.dags,
            workflow_schedules: self.workflow_schedules,
            query_handlers: self.query_handlers,
            update_handlers: self.update_handlers,
            signal_handlers: self.signal_handlers,
            worker_config,
            state: self.state,
            telemetry: telemetry_arc,
            retention: self.retention,
            history_archiver: self.history_archiver,
            activity_interceptors: self.activity_interceptors,
            payload_codecs: self.payload_codecs.clone(),
            payload_offloader,
            history_policy: self.history_policy,
            max_workflow_execution_timeout: self.max_workflow_execution_timeout,
            max_workflow_chain_timeout: self.max_workflow_chain_timeout,
            max_workflow_history_events: self.max_workflow_history_events,
            max_activity_input_bytes: self.max_activity_input_bytes,
            max_activity_result_bytes: self.max_activity_result_bytes,
            max_signal_payload_bytes: self.max_signal_payload_bytes,
            max_workflow_input_bytes: self.max_workflow_input_bytes,
            max_current_details_bytes: self.max_current_details_bytes,
            workflow_log_policy: self.workflow_log_policy,
            max_workflow_start_delay,
            unknown_target_grace_window,
            batch_start_config: self.batch_start_config,
            completion_triggers: self.completion_triggers,
            max_workflow_attempts: self.max_workflow_attempts,
            usage_window_ceiling,
            usage_max_groups,
            completion_callback_config: self.completion_callback_config,
            start_idempotency_window: self
                .start_idempotency_window
                .unwrap_or(crate::start_idempotency::DEFAULT_START_IDEMPOTENCY_WINDOW),
            #[cfg(feature = "wasm-activities")]
            wasm_bindings: self.wasm_bindings,
            #[cfg(feature = "wasm-activities")]
            wasm_store: self.wasm_store,
            #[cfg(feature = "wasm-activities")]
            wasm_module_registrations: self.wasm_module_registrations,
        })
    }
}

fn validate_dags_do_not_use_local_activities(
    dags: &[DagInfo],
    activities: &[ActivityInfo],
) -> Result<(), HarvestBuilderError> {
    use std::collections::HashSet;

    let local_activities = activities
        .iter()
        .filter(|activity| activity.is_local)
        .map(|activity| activity.name)
        .collect::<HashSet<_>>();
    if local_activities.is_empty() || dags.is_empty() {
        return Ok(());
    }

    for dag in dags {
        let Ok(definition) = dag.build_definition() else {
            continue;
        };
        for task in definition.tasks() {
            // A signal/timer gate (issue #746) stores its *signal* name in
            // `activity_name` but never dispatches an activity, so its name must
            // not be validated against registered local activities — otherwise a
            // gate and a local activity sharing a name (e.g. both `approval`)
            // would false-reject the build.
            if task.signal.is_some() {
                continue;
            }
            if local_activities.contains(task.activity_name.as_str()) {
                return Err(HarvestBuilderError::LocalActivityInDag {
                    dag: dag.name.to_string(),
                    activity: task.activity_name.clone(),
                });
            }
            // A compensator (issue #780) is dispatched through the same DAG
            // activity-queue lowering as a forward node, so a local activity is
            // just as invalid there. The error names the COMPENSATOR, not the
            // forward node that declares it.
            if let Some(compensate) = &task.compensate
                && local_activities.contains(compensate.as_str())
            {
                return Err(HarvestBuilderError::LocalActivityInDag {
                    dag: dag.name.to_string(),
                    activity: compensate.clone(),
                });
            }
        }
    }

    Ok(())
}

/// Reject a signal/timer gate node on a **classic** (non-unified) DAG
/// (`workflow_handler.is_none()`) — issue #746.
///
/// Gate nodes lower onto the unified workflow-handler path
/// (`ctx.wait_for_signal`); the classic DAG executor cannot suspend on a
/// signal, so the gate would silently never fire. A gate on a unified DAG
/// (`workflow_handler.is_some()`, the default `#[dag]` output) is allowed.
fn validate_classic_dags_have_no_signal_gates(dags: &[DagInfo]) -> Result<(), HarvestBuilderError> {
    for dag in dags {
        if dag.workflow_handler.is_some() {
            continue;
        }
        let Ok(definition) = dag.build_definition() else {
            continue;
        };
        for task in definition.tasks() {
            if let Some(gate) = &task.signal {
                return Err(HarvestBuilderError::DagSignalGateRequiresUnifiedExecution {
                    dag: dag.name.to_string(),
                    signal: gate.signal_name.clone(),
                });
            }
        }
    }
    Ok(())
}

/// Reject a node compensator on a **classic** (non-unified) DAG
/// (`workflow_handler.is_none()`) — issue #780.
///
/// Compensation lowers onto the unified workflow-handler path
/// (`run_unified_dag`'s terminal-failure `Saga` unwind); the classic DAG
/// executor has no unwind step, so the compensator would silently never run —
/// the worst possible failure mode for an undo. A compensator on a unified DAG
/// (`workflow_handler.is_some()`, the default `#[dag]` output) is allowed.
fn validate_classic_dags_have_no_compensators(dags: &[DagInfo]) -> Result<(), HarvestBuilderError> {
    for dag in dags {
        if dag.workflow_handler.is_some() {
            continue;
        }
        let Ok(definition) = dag.build_definition() else {
            continue;
        };
        for task in definition.tasks() {
            if let Some(compensate) = &task.compensate {
                return Err(
                    HarvestBuilderError::DagCompensationRequiresUnifiedExecution {
                        dag: dag.name.to_string(),
                        task: task.activity_name.clone(),
                        compensate: compensate.clone(),
                    },
                );
            }
        }
    }
    Ok(())
}

/// Verify that unified DAG auto-registration does not overwrite or get
/// overwritten by a normal workflow with the same name.
fn validate_dag_workflow_name_collisions(
    workflows: &[crate::info::WorkflowInfo],
    auto_registered_dag_workflows: &[String],
) -> Result<(), HarvestBuilderError> {
    use std::collections::HashMap;

    if auto_registered_dag_workflows.is_empty() {
        return Ok(());
    }

    let mut auto_counts: HashMap<&str, usize> = HashMap::new();
    for name in auto_registered_dag_workflows {
        *auto_counts.entry(name.as_str()).or_default() += 1;
    }

    let mut workflow_counts: HashMap<&str, usize> = HashMap::new();
    for workflow in workflows {
        *workflow_counts.entry(workflow.name).or_default() += 1;
    }

    for (name, auto_count) in auto_counts {
        if workflow_counts.get(name).copied().unwrap_or_default() > auto_count {
            return Err(HarvestBuilderError::DagWorkflowNameCollision {
                name: name.to_string(),
            });
        }
    }

    Ok(())
}

/// Verify that every [`WorkflowSchedule`] references a workflow name that is
/// actually registered on the builder. Fails fast with
/// [`HarvestBuilderError::UnknownWorkflowSchedule`] on the first mismatch.
fn validate_completion_triggers(
    triggers: &[crate::completion_trigger::CompletionTrigger],
    workflows: &[crate::info::WorkflowInfo],
    auto_registered_dag_workflows: &[String],
) -> Result<(), HarvestBuilderError> {
    if triggers.is_empty() {
        return Ok(());
    }
    let registered: Vec<String> = workflows
        .iter()
        .map(|w| w.name.to_string())
        .chain(auto_registered_dag_workflows.iter().cloned())
        .collect();
    for trigger in triggers {
        if !registered.contains(&trigger.source_workflow_name) {
            return Err(HarvestBuilderError::UnknownCompletionTriggerWorkflow {
                workflow_name: trigger.source_workflow_name.clone(),
                role: "source",
                registered,
            });
        }
        if !registered.contains(&trigger.target_workflow_name) {
            return Err(HarvestBuilderError::UnknownCompletionTriggerWorkflow {
                workflow_name: trigger.target_workflow_name.clone(),
                role: "target",
                registered,
            });
        }
        // Output-guard boundedness validation (issue #810): reject an
        // over-cap or malformed-path condition at build time so it can never
        // reach the terminal-commit path.
        if let Some(ref condition) = trigger.condition
            && let Err(message) = condition.validate()
        {
            return Err(HarvestBuilderError::InvalidCompletionTriggerCondition {
                trigger_id: trigger.id,
                message,
            });
        }
    }
    Ok(())
}

/// Warn when this worker heartbeats more slowly than the fleet-wide liveness
/// window its peers judge it by (issue #804, Codex round-19 P1).
///
/// Nothing in `harvest_workers` records a worker's cadence, so a peer running
/// the capability-miss fleet lookup cannot ask "is this row fresh *for the
/// worker that wrote it*" — it applies one fleet-wide window
/// ([`crate::worker::CAPABILITY_MISS_MIN_FLEET_STALE_SECS`]) to every row. A
/// worker configured past
/// [`crate::worker::MAX_SUPPORTED_HEARTBEAT_INTERVAL_FOR_FLEET_LIVENESS`] can
/// therefore look dead to a peer while it is perfectly healthy, and be omitted
/// from the fleet that decides whether "no capable worker is live" is true.
///
/// A warning rather than a rejection: an existing deployment already running a
/// slow cadence must keep booting (the same warn-never-error posture the
/// degenerate slot-tuner band and `queue_weights` take), and the consequence is
/// confined to one escalation bound — the distinct-worker bound and the absolute
/// ceiling are unaffected.
///
/// Returns whether the warning fired so the decision is testable without a
/// tracing subscriber.
#[cfg(feature = "db")]
fn warn_if_heartbeat_outruns_fleet_liveness(interval: Duration) -> bool {
    let ceiling = crate::worker::MAX_SUPPORTED_HEARTBEAT_INTERVAL_FOR_FLEET_LIVENESS;
    if interval <= ceiling {
        return false;
    }
    tracing::warn!(
        worker_heartbeat_interval_secs = interval.as_secs(),
        supported_ceiling_secs = ceiling.as_secs(),
        "harvest: worker_heartbeat_interval exceeds the cadence the capability-miss fleet \
         lookup can vouch for (issue #804); a peer may treat this worker as dead while it is \
         healthy and escalate a task this worker could have run. Lower the interval, or accept \
         that the configured-total redelivery bound may fire early for this fleet."
    );
    true
}

/// Validates that every per-workflow-type retention override (issue #737)
/// names a registered workflow type — either an explicitly registered
/// `#[workflow]` or an auto-registered DAG workflow. Catches typos at build
/// time rather than silently ignoring the override. Mirrors
/// [`validate_completion_triggers`].
fn validate_retention_overrides(
    retention: &crate::retention::RetentionConfig,
    workflows: &[crate::info::WorkflowInfo],
    auto_registered_dag_workflows: &[String],
) -> Result<(), HarvestBuilderError> {
    if retention.workflow_overrides().is_empty() {
        return Ok(());
    }
    let registered: Vec<String> = workflows
        .iter()
        .map(|w| w.name.to_string())
        .chain(auto_registered_dag_workflows.iter().cloned())
        .collect();
    for name in retention.workflow_overrides().keys() {
        if !registered.contains(name) {
            return Err(HarvestBuilderError::UnknownRetentionOverrideWorkflow {
                workflow_name: name.clone(),
                registered,
            });
        }
    }
    Ok(())
}

fn validate_workflow_schedules(
    schedules: &[WorkflowSchedule],
    workflows: &[crate::info::WorkflowInfo],
    auto_registered_dag_workflows: &[String],
) -> Result<(), HarvestBuilderError> {
    if schedules.is_empty() {
        return Ok(());
    }
    let registered: Vec<String> = workflows.iter().map(|w| w.name.to_string()).collect();
    for schedule in schedules {
        if !registered.contains(&schedule.workflow_name) {
            return Err(HarvestBuilderError::UnknownWorkflowSchedule {
                workflow_name: schedule.workflow_name.clone(),
                registered,
            });
        }
        if schedule.dag_name.is_none()
            && auto_registered_dag_workflows
                .iter()
                .any(|dag_name| dag_name == &schedule.workflow_name)
        {
            return Err(HarvestBuilderError::InvalidWorkflowSchedule {
                workflow_name: schedule.workflow_name.clone(),
                reason: "workflow schedule targets an auto-registered DAG workflow; use the DAG schedule registration instead".to_string(),
            });
        }
        // `all_writable_shards` is only honoured on the fire path for DAG
        // schedules and the built-in liveness canary (issue #796). A plain
        // workflow opting in would register on every writable shard yet mint
        // executions on the default shard (see the field docs). Reject fast so
        // the footgun surfaces as a clear build error instead of silent
        // duplicate/cross-shard-inconsistent runs at fire time.
        if schedule.all_writable_shards
            && schedule.dag_name.is_none()
            && !crate::canary::is_canary_workflow(&schedule.workflow_name)
        {
            return Err(HarvestBuilderError::AllWritableShardsUnsupported {
                workflow_name: schedule.workflow_name.clone(),
            });
        }
        // Reject zero-length intervals (would cause infinite loops in due_run_plan
        // with catchup=true) and invalid cron expressions (would silently never fire).
        if let crate::policy::Schedule::Interval(dur) = &schedule.schedule {
            if dur.is_zero() {
                return Err(HarvestBuilderError::InvalidWorkflowSchedule {
                    workflow_name: schedule.workflow_name.clone(),
                    reason: "interval must be at least 1 second".to_string(),
                });
            }
        } else {
            // Validate timezone names early so operators get a typed error rather
            // than a silent bad-timezone panic at first scheduler tick.
            if let crate::policy::Schedule::CronInTimezone { tz, .. } = &schedule.schedule
                && tz.parse::<chrono_tz::Tz>().is_err()
            {
                return Err(HarvestBuilderError::UnknownTimezone { name: tz.clone() });
            }
            if let Err(reason) = crate::policy::validate_schedule(&schedule.schedule) {
                return Err(HarvestBuilderError::InvalidWorkflowSchedule {
                    workflow_name: schedule.workflow_name.clone(),
                    reason,
                });
            }
        }
        if let Err(reason) = crate::policy::validate_jitter(&schedule.schedule, schedule.jitter) {
            return Err(HarvestBuilderError::InvalidWorkflowSchedule {
                workflow_name: schedule.workflow_name.clone(),
                reason,
            });
        }
    }
    Ok(())
}

fn validate_dag_schedules(dags: &[crate::info::DagInfo]) -> Result<(), HarvestBuilderError> {
    for dag in dags {
        let Some(schedule) = &dag.schedule else {
            continue;
        };
        if let crate::policy::Schedule::CronInTimezone { tz, .. } = schedule
            && tz.parse::<chrono_tz::Tz>().is_err()
        {
            return Err(HarvestBuilderError::UnknownTimezone { name: tz.clone() });
        }
        if let Err(reason) = crate::policy::validate_schedule(schedule) {
            return Err(HarvestBuilderError::InvalidWorkflowSchedule {
                workflow_name: dag.name.to_string(),
                reason,
            });
        }
    }
    Ok(())
}

/// Entry in the concurrency-key deduplication map.
struct ConcurrencyKeyEntry {
    first_cap: u32,
    contributors: Vec<(String, u32)>,
}

/// Verify that all activities sharing a `concurrency_key` agree on
/// `max_concurrent`. Fails fast with [`HarvestBuilderError::ConcurrencyKeyMismatch`]
/// if any disagreement is found.
fn validate_concurrency_keys(
    activities: &[crate::info::ActivityInfo],
) -> Result<(), HarvestBuilderError> {
    use std::collections::HashMap;

    let mut seen: HashMap<&str, ConcurrencyKeyEntry> = HashMap::new();

    for activity in activities {
        // max_concurrent = 0 makes the cap predicate always-true, permanently
        // deferring every task for that activity. Reject at build time.
        if activity.max_concurrent == Some(0) {
            return Err(HarvestBuilderError::ZeroConcurrencyCap {
                activity: activity.name.to_string(),
            });
        }

        // concurrency_key without max_concurrent silently bypasses the cap — reject it.
        if let (Some(key), None) = (activity.concurrency_key, activity.max_concurrent) {
            return Err(HarvestBuilderError::ConcurrencyKeyWithoutCap {
                activity: activity.name.to_string(),
                key: key.to_string(),
            });
        }

        // Activities with max_concurrent but no explicit concurrency_key use the
        // activity name as the effective key at runtime (persist_scheduled_activity
        // defaults it). Include them in the cross-activity cap consistency check.
        let Some(cap) = activity.max_concurrent else {
            continue;
        };
        let effective_key: &str = activity.concurrency_key.unwrap_or(activity.name);
        let entry = seen
            .entry(effective_key)
            .or_insert_with(|| ConcurrencyKeyEntry {
                first_cap: cap,
                contributors: Vec::new(),
            });
        entry.contributors.push((activity.name.to_string(), cap));

        if entry.first_cap != cap {
            return Err(HarvestBuilderError::ConcurrencyKeyMismatch {
                key: effective_key.to_string(),
                activities: entry.contributors.clone(),
            });
        }
    }

    Ok(())
}

struct RateLimitKeyEntry {
    first_rps: f64,
    first_burst: f64,
    contributors: Vec<(String, f64, Option<f64>)>,
}

/// Reject an activity whose `rate_limit_rps` or `rate_limit_burst` is not
/// finite and strictly greater than zero (issue #699 review, Codex P2).
///
/// Applies to static AND dynamic rate limits — the `#[activity]` macro enforces
/// positivity universally at parse time (`n <= 0.0` -> compile error), but a
/// hand-built `ActivityInfo` (a directly-constructed `HandlerRegistry`)
/// bypasses the macro; a non-positive / non-finite rps yields a
/// `burst = tokens = 0` bucket whose gate can never reach one token,
/// permanently wedging every scheduled activity on that bucket. This goes
/// stricter than the macro by also rejecting `NaN`/`±inf`, which `n <= 0.0`
/// alone lets through.
fn check_rate_limit_positive(
    activity: &crate::info::ActivityInfo,
) -> Result<(), HarvestBuilderError> {
    if let Some(rps) = activity.rate_limit_rps
        && (!rps.is_finite() || rps <= 0.0)
    {
        return Err(HarvestBuilderError::RateLimitRateNotPositive {
            activity: activity.name.to_string(),
            field: "rate_limit_rps",
        });
    }
    if let Some(burst) = activity.rate_limit_burst
        && (!burst.is_finite() || burst <= 0.0)
    {
        return Err(HarvestBuilderError::RateLimitRateNotPositive {
            activity: activity.name.to_string(),
            field: "rate_limit_burst",
        });
    }
    Ok(())
}

/// Verify that rate limiting attributes on activities are consistent and valid.
/// Validate every activity's rate-limit configuration (issue #699).
///
/// This is the single, comprehensive rate-limit gate. It is called from
/// [`HarvestBuilder::try_build`] (the builder path) **and** from
/// [`crate::worker::Worker::new`] (the direct-`HandlerRegistry` / worker-startup
/// path), so a worker constructed without going through the builder still
/// fails loud, once, before its poll loop and first enqueue — rather than
/// slipping an invalid config past the piecemeal schedule-time guards in
/// `persist_scheduled_activities` (issue #699 review, Codex round-5 P2).
///
/// Checks, per activity:
/// - a dynamic `rate_limit(key = …)` on a **local** activity — local activities
///   run inline and bypass the dispatch path entirely, so the limit would be
///   silently unenforced ([`HarvestBuilderError::RateLimitKeyExprOnLocalActivity`]);
/// - a dynamic `rate_limit(key = …)` without an `rps`
///   ([`HarvestBuilderError::RateLimitKeyExprWithoutCap`]);
/// - two activities sharing a normalized dynamic key-expression that declare
///   different `rps`/`burst` — the shared bucket's config would become
///   insertion-order dependent under `ON CONFLICT DO NOTHING`
///   ([`HarvestBuilderError::RateLimitKeyMismatch`]);
/// - a static `rate_limit_key` squatting the reserved `dyn-rate:` prefix
///   ([`HarvestBuilderError::RateLimitKeyReservedPrefix`]);
/// - a static `rate_limit_key`/`rate_limit_burst` without `rps`;
/// - two activities sharing a static key with different `rps`/`burst`.
///
/// Accepts an iterator of `&ActivityInfo` rather than a slice so both the
/// builder's `Vec<ActivityInfo>` and the worker registry's
/// `HashMap<String, ActivityInfo>::values()` can be validated without an owned
/// copy (`ActivityInfo` is not `Clone`). Mismatch detection is order-independent
/// (any two disagreeing configs reject regardless of which is seen first), so
/// the registry's non-deterministic iteration order does not affect whether a
/// conflicting set is rejected — only cosmetic ordering within the error's
/// contributor list.
pub(crate) fn validate_activity_rate_limits<'a>(
    activities: impl IntoIterator<Item = &'a crate::info::ActivityInfo>,
) -> Result<(), HarvestBuilderError> {
    use std::collections::HashMap;

    let mut seen: HashMap<&str, RateLimitKeyEntry> = HashMap::new();
    // Dynamic per-key rate-limit expressions (issue #699) live in an independent
    // map keyed on the dot-path EXPRESSION so static (bare-name / string) keys and
    // dynamic (`dyn-rate:{expr}:{tenant}`) buckets — which the enqueue path
    // namespaces so they can never collide — are validated separately.
    let mut seen_dynamic: HashMap<&str, RateLimitKeyEntry> = HashMap::new();

    for activity in activities {
        // Positivity (issue #699 review, Codex P2): reject a non-finite /
        // non-positive rps or burst — static OR dynamic — before either branch
        // (see `check_rate_limit_positive`).
        check_rate_limit_positive(activity)?;

        // Dynamic per-key rate limit (issue #699): validated in its own map and
        // never touches the static path below (its static `rate_limit_key` is
        // suppressed by the macro).
        if let Some(key_expr) = activity.rate_limit_key_expr {
            // A dynamic per-key rate limit on a LOCAL activity is silently
            // unenforced: local activities run inline via
            // `run_local_activity_inline`, bypassing the task-dispatch/enqueue
            // path where per-key rate limiting lives. The `#[activity]` macro
            // rejects this at parse time, but a hand-built `ActivityInfo`
            // (direct `HandlerRegistry`) bypasses the macro. Fail loud
            // (issue #699 review, Codex round-5 P2). (Scope: this rejects the
            // DYNAMIC form only; the pre-existing static-rate-on-local #332
            // asymmetry is a documented follow-up, not expanded here.)
            if activity.is_local {
                return Err(HarvestBuilderError::RateLimitKeyExprOnLocalActivity {
                    activity: activity.name.to_string(),
                    key: key_expr.to_string(),
                });
            }
            // A dynamic key expression needs a rate to bucket against.
            let Some(rps) = activity.rate_limit_rps else {
                return Err(HarvestBuilderError::RateLimitKeyExprWithoutCap {
                    activity: activity.name.to_string(),
                    key: key_expr.to_string(),
                });
            };
            // Normalize the `input.` prefix (issue #699 review, #6) so
            // `key = "input.tenant_id"` and `key = "tenant_id"` — which resolve
            // the same field and share one bucket — are validated together.
            let normalized_expr = key_expr.strip_prefix("input.").unwrap_or(key_expr);
            let effective_burst = activity.rate_limit_burst.unwrap_or(rps);
            let entry = seen_dynamic
                .entry(normalized_expr)
                .or_insert_with(|| RateLimitKeyEntry {
                    first_rps: rps,
                    first_burst: effective_burst,
                    contributors: Vec::new(),
                });
            entry
                .contributors
                .push((activity.name.to_string(), rps, activity.rate_limit_burst));
            if (entry.first_rps - rps).abs() > 1e-9
                || (entry.first_burst - effective_burst).abs() > 1e-9
            {
                let mapped = entry
                    .contributors
                    .iter()
                    .map(|(name, r, b)| (name.clone(), FloatEq(*r), b.map(FloatEq)))
                    .collect();
                return Err(HarvestBuilderError::RateLimitKeyMismatch {
                    key: key_expr.to_string(),
                    activities: mapped,
                });
            }
            continue;
        }

        // A static `rate_limit_key` must not squat the `dyn-rate:` namespace
        // reserved for per-key/dynamic buckets (issue #699). Both static and
        // dynamic keys register `ON CONFLICT DO NOTHING` against the shared
        // `harvest_rate_limit_buckets` table, so a static key colliding with a
        // generated `dyn-rate:{expr}:{tenant}` string would race
        // first-writer-wins on the bucket's rate/burst. Reject it up front.
        // (The `start-throttle:` prefix from #607 is a separate pre-existing
        // namespace; this validation reserves `dyn-rate:` — the one this PR
        // generates — and leaves `start-throttle:` as a follow-up.)
        // The literal mirrors `crate::queue::DYNAMIC_RATE_PREFIX` (the `queue`
        // module is `db`-gated, but this validation runs in every build) and the
        // macro's own compile-time reject in `autumn-harvest-macros`.
        if let Some(key) = activity.rate_limit_key
            && key.starts_with("dyn-rate:")
        {
            return Err(HarvestBuilderError::RateLimitKeyReservedPrefix {
                activity: activity.name.to_string(),
                key: key.to_string(),
            });
        }

        // rate_limit_key without rate_limit_rps silently bypasses or breaks — reject it.
        if let (Some(key), None) = (activity.rate_limit_key, activity.rate_limit_rps) {
            return Err(HarvestBuilderError::RateLimitKeyWithoutCap {
                activity: activity.name.to_string(),
                key: key.to_string(),
            });
        }

        // rate_limit_burst without rate_limit_rps is invalid.
        if activity.rate_limit_burst.is_some() && activity.rate_limit_rps.is_none() {
            return Err(HarvestBuilderError::InvalidWorkerConfig(format!(
                "activity '{}' declares rate_limit_burst but no rate_limit_rps",
                activity.name
            )));
        }

        let Some(rps) = activity.rate_limit_rps else {
            continue;
        };

        let effective_burst = activity.rate_limit_burst.unwrap_or(rps);
        let effective_key: &str = activity.rate_limit_key.unwrap_or(activity.name);
        let entry = seen
            .entry(effective_key)
            .or_insert_with(|| RateLimitKeyEntry {
                first_rps: rps,
                first_burst: effective_burst,
                contributors: Vec::new(),
            });
        entry
            .contributors
            .push((activity.name.to_string(), rps, activity.rate_limit_burst));

        if (entry.first_rps - rps).abs() > 1e-9
            || (entry.first_burst - effective_burst).abs() > 1e-9
        {
            let mapped = entry
                .contributors
                .iter()
                .map(|(name, r, b)| (name.clone(), FloatEq(*r), b.map(FloatEq)))
                .collect();
            return Err(HarvestBuilderError::RateLimitKeyMismatch {
                key: effective_key.to_string(),
                activities: mapped,
            });
        }
    }

    Ok(())
}

/// Reject a name registered as BOTH a WASM activity binding and a native
/// `#[activity]` (issue #965 review).
///
/// `wasm_activity(...)` pushes a placeholder `ActivityInfo` ([`is_wasm_stub`]) and
/// records a `WasmBinding`; a later native `.activities(...)` with the same name
/// wins in the handler registry (a `HashMap`, last-registration-wins) while the
/// WASM binding lingers, so the worker's WASM dispatch seam would still resolve
/// the binding and run the sandboxed guest under the native metadata — a silent
/// wrong-implementation. Fail closed instead.
///
/// [`is_wasm_stub`]: crate::info::ActivityInfo::is_wasm_stub
#[cfg(feature = "wasm-activities")]
fn validate_wasm_activity_name_collisions(
    wasm_bindings: &std::collections::HashMap<String, crate::wasm_store::WasmBinding>,
    activities: &[crate::info::ActivityInfo],
) -> Result<(), HarvestBuilderError> {
    for activity in activities {
        // A native (non-placeholder) activity whose name is also a WASM binding
        // is the ambiguous case. The WASM-activity placeholder itself IS a WASM
        // binding by construction, so exclude it via `is_wasm_stub`.
        if !activity.is_wasm_stub() && wasm_bindings.contains_key(activity.name) {
            return Err(HarvestBuilderError::WasmActivityNameCollision {
                activity: activity.name.to_string(),
            });
        }
    }
    Ok(())
}

/// Reject local activities whose `default_start_to_close` exceeds the worker
/// cap. Failing early gives operators a clear error instead of a runtime surprise.
fn validate_local_activity_timeouts(
    activities: &[crate::info::ActivityInfo],
    cap: Duration,
) -> Result<(), HarvestBuilderError> {
    for activity in activities {
        if !activity.is_local {
            continue;
        }
        if activity.default_start_to_close.is_some_and(|stc| stc > cap) {
            return Err(HarvestBuilderError::LocalActivityStartToCloseExceedsCap {
                activity: activity.name.to_string(),
                actual: activity.default_start_to_close.unwrap(),
                cap,
            });
        }
    }
    Ok(())
}

/// Reject workflows whose `ConcurrencyPolicy` declares `limit = 0`. A zero
/// limit makes the claim predicate `running < 0` always false, permanently
/// deferring every workflow start for that key. Catch it at build time.
fn validate_workflow_concurrency_limits(
    workflows: &[crate::info::WorkflowInfo],
) -> Result<(), HarvestBuilderError> {
    for wf in workflows {
        if wf.concurrency.is_some_and(|p| p.limit == 0) {
            return Err(HarvestBuilderError::ZeroWorkflowConcurrencyLimit {
                workflow: wf.name.to_string(),
            });
        }
    }
    Ok(())
}

/// Reject workflows whose `ThrottlePolicy` (issue #607) has a `burst` or
/// `refill_per_sec` that could never actually pace admissions.
///
/// `ThrottlePolicy::from_rate_str` (the only path `#[workflow(throttle(...))]`
/// uses) already enforces these same rules at parse time, but the struct's
/// fields are `pub`, so a direct construction (a struct literal, or
/// `WorkflowInfo::with_throttle`) bypasses that check entirely (code review).
/// Validated once here, at build time, regardless of how the policy was
/// built — mirrors `validate_workflow_concurrency_limits`'s precedent for
/// catching a similarly "silently permanently broken" policy shape.
fn validate_workflow_throttle_policies(
    workflows: &[crate::info::WorkflowInfo],
) -> Result<(), HarvestBuilderError> {
    for wf in workflows {
        let Some(policy) = wf.throttle else {
            continue;
        };
        if !policy.burst.is_finite() {
            return Err(HarvestBuilderError::InvalidWorkflowThrottlePolicy {
                workflow: wf.name.to_string(),
                reason: format!("burst must be a finite number, got {}", policy.burst),
            });
        }
        if policy.burst < 1.0 {
            return Err(HarvestBuilderError::InvalidWorkflowThrottlePolicy {
                workflow: wf.name.to_string(),
                reason: format!(
                    "burst must be >= 1.0 (a bucket capacity below one token \
                     can never successfully debit), got {}",
                    policy.burst
                ),
            });
        }
        if !policy.refill_per_sec.is_finite() || policy.refill_per_sec <= 0.0 {
            return Err(HarvestBuilderError::InvalidWorkflowThrottlePolicy {
                workflow: wf.name.to_string(),
                reason: format!(
                    "refill_per_sec must be a finite number > 0, got {}",
                    policy.refill_per_sec
                ),
            });
        }
    }
    Ok(())
}

/// Configuration for sticky cross-worker routing (issue #235).
///
/// Sticky routing keeps follow-up tasks for a workflow execution on the worker
/// that already has that execution's event history in its in-process LRU cache,
/// reducing cold event-history reloads from Postgres.
///
/// Sticky routing is **off by default**. Enable it via
/// [`WorkerConfig::with_sticky_routing`].
///
/// ## Trade-offs
///
/// | Parameter | Short TTL | Long TTL |
/// |-----------|-----------|----------|
/// | Cache hit rate | Lower (sticky window may expire before follow-up arrives) | Higher |
/// | Failover latency | Fast (expired window → any eligible worker claims) | Slower |
/// | Load distribution | Better (sticky windows expire quickly) | Skewed toward hot workers |
///
/// A 5–30 second `lease_ttl` is a reasonable starting point for most
/// deployments. See `docs/sticky-routing.md` for the full operator guide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StickyRoutingConfig {
    /// How long to prefer the owning worker for follow-up tasks after a
    /// workflow suspends.
    ///
    /// The task queue will offer tasks whose workflow has an active,
    /// unexpired sticky lease to the owning worker before any other eligible
    /// worker can claim them. Once the window elapses the task becomes
    /// claimable by any eligible worker (safe failover).
    pub lease_ttl: Duration,
}

/// Worker concurrency and queue configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Queues this worker polls. Defaults to `["default"]`.
    pub queues: Vec<String>,
    /// Optional per-queue dispatch weights for multi-queue worker fairness (issue #515).
    ///
    /// When non-empty, the worker uses weighted-random queue ordering on each
    /// poll iteration so that dispatch share tracks the configured weights under
    /// saturation. A queue absent from this map defaults to weight **1**
    /// (equal share with other un-weighted queues).
    ///
    /// A weight of **0** places the queue last (fallthrough-only): it is only
    /// drained when every positive-weight queue has no available work.
    ///
    /// **Default: empty** — the existing single `ANY($2)` claim query runs
    /// unchanged, preserving today's byte-for-byte behaviour for all workers
    /// that do not configure weights.
    pub queue_weights: std::collections::HashMap<String, u32>,
    /// Optional Postgres URL for LISTEN/NOTIFY wakeups.
    pub notification_database_url: Option<String>,
    /// Optional per-shard LISTEN/NOTIFY database URLs for multi-shard workers
    /// (issue #522).
    ///
    /// When a worker is assigned to multiple shards, each shard can optionally
    /// have its own notification URL so the worker can use LISTEN/NOTIFY for
    /// that shard's task queue instead of falling back to polling. Shards
    /// absent from this map fall back to poll-only behaviour for that shard.
    ///
    /// **Default: empty** — all assigned shards use polling.
    pub shard_notification_database_urls: Vec<(crate::types::ShardId, String)>,
    /// Maximum concurrent workflow executions on this worker.
    pub max_concurrent_workflows: usize,
    /// Maximum concurrent activity executions on this worker.
    pub max_concurrent_activities: usize,
    /// Graceful shutdown timeout.
    pub shutdown_timeout: Duration,
    /// Maximum cached in-memory workflow states (LRU eviction).
    pub workflow_cache_size: usize,
    /// How long to offer sticky tasks to the sticky worker before fallback.
    pub sticky_timeout: Duration,
    /// Grace period for an activity to finish cooperatively after its workflow
    /// is cancelled before the worker hard-aborts the handler task. Cancellation
    /// is cooperative -- activities should poll [`crate::context::ActivityContext::is_cancelled`]
    /// or call [`crate::context::ActivityContext::heartbeat`], but an uncooperative handler must
    /// not block a worker slot indefinitely.
    pub cancellation_grace_period: Duration,
    /// Shards this worker is responsible for polling.
    ///
    /// Defaults to **empty**, which means *auto*: the worker covers every
    /// shard this process has a pool for (issue #961, AC1). With **no** sharded
    /// pool it stays empty — there is no shard identity to resolve, and every
    /// read-side consumer already reads the empty array as "covers whatever
    /// shard this row was read from" (see
    /// [`crate::workers::shard_assignments_cover`] and issue #1150). Do **not**
    /// rely on a pool-less worker advertising shard `0`: it advertises no shard
    /// at all. Set an explicit list to narrow a worker to a subset — typically
    /// the one-worker-process-per-shard shape,
    /// `shard_assignments = vec![that_shard]`. See
    /// [`resolve_shard_assignments`] for the resolution rules; the effective
    /// (resolved) list is what `GET /admin/config` reports.
    pub shard_assignments: Vec<ShardId>,
    /// Hard cap on `start_to_close` for local activities.
    ///
    /// Local activities run inline on the workflow worker task. An unbounded
    /// timeout would block the worker indefinitely. Defaults to **60 seconds**.
    /// Any local activity registered with `start_to_close > cap` is rejected
    /// at builder `try_build()` time.
    pub max_local_activity_start_to_close: Duration,
    /// Builder-level default activity retry policy (issue #620).
    ///
    /// Applied at schedule time as the lowest-priority fallback in the
    /// precedence chain: call-site override → activity `#[activity(retry = …)]`
    /// default → this builder default → implicit fallback. `None` (the default)
    /// is opt-in — an unset floor leaves today's behaviour byte-for-byte
    /// unchanged. Set via [`WorkerConfig::with_default_activity_retry_policy`].
    pub default_activity_retry_policy: Option<crate::policy::RetryPolicy>,
    /// Builder-level default activity `start_to_close` timeout (issue #620).
    ///
    /// Same precedence as [`WorkerConfig::default_activity_retry_policy`]:
    /// call-site override → activity default → this builder default → no
    /// timeout. `None` (the default) is opt-in. For *local* activities the
    /// resolved value is still clamped by
    /// [`WorkerConfig::max_local_activity_start_to_close`]. Set via
    /// [`WorkerConfig::with_default_activity_start_to_close`].
    pub default_activity_start_to_close: Option<Duration>,
    /// Ceiling on an author-supplied `Retry-After` delay hint (issue #744).
    ///
    /// `ActivityFailure::retry_after` lets an activity author override the
    /// policy-computed backoff for a single attempt (e.g. to honor a
    /// downstream's `Retry-After` response header). This ceiling bounds that
    /// hint so a misbehaving/malicious downstream cannot park a task for an
    /// unbounded duration — an over-ceiling hint is clamped down, never
    /// rejected. Unlike the two builder-default floors above this is **not**
    /// opt-in: it always applies, with the sane default
    /// [`DEFAULT_RETRY_AFTER_CEILING`]. Set via
    /// [`WorkerConfig::with_retry_after_ceiling`].
    pub retry_after_ceiling: Duration,
    /// How often the worker upserts its liveness row in `harvest_workers`.
    /// Defaults to **5 seconds**. The API classifies a worker as stale after
    /// `2 × worker_heartbeat_interval` without a heartbeat.
    ///
    /// Not every subsystem derives the same window from this knob. The
    /// poison-pill reclaimer (#367) and the broken-session scanner (#606) use
    /// the bare `2 ×` value, but the capability-miss fleet lookup (#804) floors
    /// it at 120 s — see [`Self::capability_miss_max_redeliveries`] — because it
    /// judges *peers* whose cadence it cannot read. Lowering this interval
    /// therefore speeds the first two up and leaves the third unchanged for any
    /// value at or under 60 s.
    pub worker_heartbeat_interval: Duration,
    /// Immutable build identifier for this worker binary (issue #171).
    ///
    /// Set to a stable per-build token (Git SHA, semver tag, CI job ID, etc.)
    /// to enable build-aware task routing. Empty string = legacy behaviour
    /// where the worker can claim any task regardless of `required_build_id`.
    pub build_id: String,
    /// Optional human-readable deployment name for operator observability
    /// (issue #171), e.g. `"prod-blue"` or `"canary"`.
    pub deployment_name: Option<String>,
    /// Per-query execution timeout (issue #234).
    ///
    /// When a query handler takes longer than this to complete, the engine
    /// terminates the handler and returns [`HarvestError::QueryTimedOut`] to
    /// the caller. Defaults to **5 seconds**.
    pub query_timeout: Duration,
    /// Anti-starvation aging period for the priority claim query (issue #249).
    ///
    /// When `Some(K)`, a task's effective priority is boosted by `+1` for
    /// every `K` seconds it has been waiting in `PENDING` state. This ensures
    /// that low-priority tasks are not indefinitely starved under sustained
    /// high-priority load.
    ///
    /// A value of `0` is normalized to `None` (no aging). `None` is the
    /// default — existing deployments are unaffected.
    pub priority_aging_secs: Option<u32>,
    /// Maximum allowed start delay for a workflow (issue #322).
    /// Default: 365 days.
    pub max_workflow_start_delay: Duration,
    /// Grace window before cross-workflow signaling fails for unknown target (issue #330).
    /// Default: 5 seconds.
    pub unknown_target_grace_window: Duration,
    /// Consecutive worker crashes a task may cause before it is quarantined to
    /// the dead-letter queue instead of re-queued (issue #367).
    ///
    /// When the orphan-reclaim scanner reclaims a task from a dead worker, it
    /// increments the task's `crash_strikes`. Once `crash_strikes` reaches this
    /// threshold the task is moved to the DLQ and its owning workflow is failed
    /// terminally, rather than being re-dispatched to crash another worker.
    ///
    /// Defaults to **3**. Set to `0` to disable quarantine entirely (reclaimed
    /// poison pills are re-queued indefinitely — the legacy retry-loop
    /// behaviour).
    pub poison_pill_threshold: i32,
    /// Maximum number of **distinct workers** that may release a task back to
    /// `PENDING` because they had no handler registered for its
    /// workflow/activity type, before it is escalated to the ordinary
    /// terminal-failure path with a `no_capable_worker:` reason (issue #804).
    ///
    /// **No dead-letter row is written.** The escalation routes through
    /// `fail_task_and_execution_with_history`, which fails the task and the
    /// execution without inserting into `harvest_dead_letters` — the reason
    /// lives on the failed execution row, so an operator diagnosing an
    /// exhausted budget queries failed workflows, not the DLQ. (A DLQ entry on
    /// this path would also be indistinguishable from a poison-pill
    /// quarantine, [`poison_pill_threshold`], which a capability miss
    /// deliberately is not.)
    ///
    /// Any worker polling a queue can claim any task on it — the queue's
    /// non-blocking claim query has no capability filter, and cannot have one
    /// (a worker can enumerate the handlers it *has* registered, never the
    /// ones it has not).
    /// A claim by an incapable worker is therefore released for a capable peer
    /// rather than terminally failing the execution, which makes the routine
    /// mid-rolling-deploy window blameless.
    ///
    /// Each release defers the task by a capped exponential backoff (1 s
    /// doubling to 30 s), so the budget buys a dwell window comfortably longer
    /// than a pod flip.
    ///
    /// # The budget is per distinct worker
    ///
    /// The backoff makes a released task eligible to *every* worker again — the
    /// one that just released it included. If the budget counted total
    /// releases, one incapable worker winning the claim race `N + 1` times in a
    /// row would terminally fail a run while a capable peer sat live and idle,
    /// which is the very outage this setting exists to prevent. So the task
    /// records the **set** of workers that have missed it
    /// (`capability_miss_workers`), and a repeat miss by a worker already in
    /// that set is free: it still backs off, but consumes no budget.
    ///
    /// # The budget is gated on the live fleet
    ///
    /// A distinct count still has no relationship to how many workers are
    /// actually up: a rollout with `N + 1` old pods plus one new capable pod
    /// can hand `N + 1` distinct incapable ids to the budget while the capable
    /// pod is live and polling. So this budget may only terminate a task once
    /// the recorded miss set **covers the live fleet for the task's queue**
    /// (`harvest_workers` rows with a fresh heartbeat advertising it). That
    /// liveness window is **not** the poison-pill one: it is
    /// [`crate::worker::capability_miss_fleet_stale_secs`], which is
    /// `2 × worker_heartbeat_interval` **floored at 120 s**
    /// ([`crate::worker::CAPABILITY_MISS_MIN_FLEET_STALE_SECS`]). The reclaimer
    /// and the broken-session scanner judge rows they own with a window they
    /// chose; this query judges *peers*, whose cadence nothing in
    /// `harvest_workers` records — so a fast-heartbeating claimant must not
    /// declare a healthy peer on a slower cadence dead. At the default 5 s
    /// cadence the two windows are 120 s and 10 s.
    ///
    /// The timing consequence worth predicting: after a pod dies, its row keeps
    /// holding the evidence at "a capable peer may exist" for up to 120 s, and
    /// in that interval **both** evidence-derived bounds are withheld — this
    /// fleet-covering one *and* the distinct-worker one. The absolute release
    /// ceiling (`10 ×` the budget) is what still fires, so AC3 holds, but a task
    /// whose fleet cannot be shown covered waits on that ceiling rather than on
    /// `max_redeliveries`. The delay costs redeliveries, never the run. Tuning
    /// `worker_heartbeat_interval` *below* 60 s does not shorten it.
    /// While any live worker there has never missed the task, this bound is
    /// withheld.
    ///
    /// Two consequences: the effective bound is `max(N, live fleet size)`
    /// redeliveries — you cannot prove "no worker here has the handler" in
    /// fewer redeliveries than there are workers to ask — and a fleet whose
    /// workers are not registered at all (heartbeats disabled, or a different
    /// queue name advertised) falls back to bounding on `N` alone, with the
    /// escalation reason saying so rather than claiming a fleet conclusion.
    ///
    /// A *second* bound on **total** releases keeps `N` a real maximum for the
    /// common small deployment. Once the registry confirms every live worker on
    /// the queue has already missed the task, the distinct set cannot grow, so
    /// with a fleet smaller than the budget the distinct bound is unreachable —
    /// one incapable worker pins `distinct_after` at 1 forever. This bound
    /// therefore escalates at `N` total releases, but only on that same
    /// fleet-covering evidence: on evidence the registry cannot supply it would
    /// let a single worker exhaust the budget by winning the claim race
    /// repeatedly, which is exactly what the distinct count exists to prevent.
    ///
    /// Only a third, **ungated** absolute ceiling of `10 ×` this value remains
    /// for the cases neither gated bound can reach: a fleet the registry cannot
    /// describe at all, and a live worker that never claims (which keeps the
    /// evidence at "a capable peer may exist" and withholds *both* gated
    /// bounds). It reports the counts it actually observed rather than a
    /// fleet-wide conclusion, and the sustained-release alert fires long before
    /// it.
    ///
    /// Both the set and the total are reset by every path that proves the
    /// claiming worker *was* capable, so they measure **consecutive** misses —
    /// the same semantics [`poison_pill_threshold`] has for crashes.
    ///
    /// Defaults to **5**. Set to `0` to escalate on the **first** miss (the
    /// pre-#804 fail-fast behaviour) — note this is *not* an "unlimited
    /// releases" switch; releasing forever would let a genuinely-unregistered
    /// type bounce indefinitely, which is exactly what the budget bounds.
    ///
    /// [`poison_pill_threshold`]: Self::poison_pill_threshold
    pub capability_miss_max_redeliveries: u32,
    /// Maximum wall-clock time a single workflow-task dispatch may run before
    /// the worker reclaims the concurrency slot (issue #494).
    ///
    /// When a `#[workflow]` body does not reach a suspension point or complete
    /// within this budget, the dispatch is abandoned: its semaphore permit is
    /// released so other tasks can proceed and the task row is reset to
    /// `PENDING` for a subsequent attempt. Deterministic replay means a
    /// transient slow dispatch recovers safely on retry.
    ///
    /// After `poison_pill_threshold` consecutive timeouts for the same
    /// execution the task is escalated to the dead-letter queue rather than
    /// re-queued, so a permanently stuck workflow body never loops forever.
    ///
    /// Defaults to **10 seconds**. Set to [`Duration::ZERO`] to disable
    /// (workflow tasks run without a wall-clock budget — the behaviour before
    /// this field was added). Protection is **on by default**.
    pub workflow_task_timeout: Duration,
    /// Maximum number of times a workflow whose handler **panics** (unwinds) is
    /// re-dispatched with backoff before the run is failed terminally with a
    /// typed `HandlerPanic` error (issue #782).
    ///
    /// A caught workflow-body panic is treated as a recoverable, non-terminal
    /// condition for the first `workflow_panic_max_attempts` strikes so a bad
    /// deploy can be hotfixed-and-redeployed before in-flight runs fail. Once
    /// the budget is exhausted (a permanent panic bug) the run fails terminally,
    /// bounding the re-dispatch churn.
    ///
    /// The strike counter is **in-process and per-worker-instance**, so it
    /// resets on worker restart/redeploy. This is intentional: a redeploy of
    /// fixed code gets a fresh budget (exactly the "buy time to hotfix" goal),
    /// while a single long-lived worker still terminates a permanently-panicking
    /// run after this many consecutive strikes.
    ///
    /// Defaults to **3**. Set to `0` to fail terminally on the **first** panic
    /// (no panic-retry).
    pub workflow_panic_max_attempts: u32,
    /// Maximum wall-clock time a workflow execution may stay paused before the
    /// bounded-pause auto-resume scanner force-resumes it with
    /// `actor = "auto-resume(timeout)"` (issue #383).
    ///
    /// This prevents orphaned-pause backlogs when an operator pauses a workflow
    /// during an incident and never resumes it. Defaults to **24 hours**.
    pub max_workflow_pause_duration: Duration,
    /// Capability labels for hardware-aware and regional routing (issue #382).
    pub labels: std::collections::HashMap<String, String>,
    /// Hard ceiling on the number of history events a workflow may accumulate
    /// before the server terminates it (issue #493).
    ///
    /// When `Some(n)`, any `RUNNING` execution whose recorded event count
    /// reaches `n` is failed with a machine-readable `WorkflowFailed` event
    /// (`"history_ceiling_exceeded: event count {n} >= ceiling {n}"`).
    /// This is a server-side safety net for workflows that do not cooperate
    /// with `ctx.should_continue_as_new()`.
    ///
    /// Defaults to `None` (disabled).
    pub max_workflow_history_events: Option<u64>,
    /// Default maximum wait before a debounced workflow start is forced to fire,
    /// even if the burst has not settled (issue #499).
    ///
    /// Applied when a `DebouncePolicy.max_wait` is `None`. Prevents a
    /// continuously-retriggered workflow from being deferred indefinitely.
    ///
    /// Defaults to **1 hour**. Override via `with_default_debounce_max_wait`.
    pub default_debounce_max_wait: Duration,
    /// Lease TTL for a held durable mutex (`ctx.mutex`, issue #691).
    ///
    /// The lease scanner reclaims a lock whose lease has elapsed; the holder's
    /// own decision cycles renew it forward. Must exceed the worst-case
    /// wall-clock time a workflow spends inside a single held critical section
    /// (the interval between decision cycles that renew the lease) — see the
    /// `mutex` module's lease contract.
    ///
    /// Defaults to **60 seconds**. Override via `with_mutex_lease_ttl`.
    pub mutex_lease_ttl: Duration,
    /// Opt-in adaptive dispatch-slot tuner (issue #548).
    ///
    /// When `Some`, both dispatch semaphores (`max_concurrent_workflows` /
    /// `max_concurrent_activities`) are auto-resized within
    /// `[SlotTunerConfig::min_slots, SlotTunerConfig::max_slots]`, driven by
    /// in-process slot utilization, worker DB-pool pressure, and recent
    /// claim-to-dispatch permit-wait latency. The controller never resizes
    /// below `min_slots` (liveness floor) or above `max_slots` (hard safety
    /// cap); a shrink decision only withholds *new* permits and never cancels
    /// or reclaims an already-dispatched task, so graceful shutdown and
    /// draining are unaffected.
    ///
    /// **Defaults to `None`: byte-for-byte identical fixed-concurrency
    /// behaviour** — the worker uses `max_concurrent_workflows` /
    /// `max_concurrent_activities` exactly as it does today. Set via
    /// `with_slot_tuner`.
    pub slot_tuner: Option<crate::slot_tuner::SlotTunerConfig>,
    #[cfg(feature = "db")]
    /// Optional sharded database pool for exact shard routing.
    pub sharded_pool: Option<crate::shard::ShardedDbPool>,
    /// Advertised worker-session capacity (issue #606): the maximum number
    /// of `ctx.create_session(...)` sessions this worker will host
    /// concurrently. Opening a session consumes one slot; ending it
    /// (`Session::complete()`) or the broken-session scanner reclaiming a
    /// dead/expired session releases it.
    ///
    /// **Defaults to `0`: sessions disabled, zero behavior change** for
    /// existing deployments. Set via `with_max_concurrent_sessions`.
    pub max_concurrent_sessions: i32,
    /// Rows examined per shard, per scanner tick, by the lazy payload-codec
    /// re-encryption sweep (issue #948).
    ///
    /// This is the sweep's **rate limiter**: raise it to convert stored history
    /// faster, lower it to reduce the load a rotation puts on the scanner
    /// connection, or set it to `0` to stop the sweep entirely without a
    /// redeploy. The sweep is a no-op — not one statement issued — unless a
    /// keyed codec is registered, so this costs nothing on a deployment that has
    /// not adopted key rotation. Set via `with_codec_rotation_batch_size`.
    pub codec_rotation_batch_size: i64,
    /// The payload-codec registry this worker's background sweeps read
    /// (issue #948).
    ///
    /// Wired from `BuiltHarvest::payload_codecs()` by the runner/plugin. The
    /// rotation half of the registry is shared across clones, so flipping the
    /// active key is observed by an already-running worker with no restart.
    ///
    /// **[`HarvestBuilder::build`] owns this field and overwrites whatever is
    /// here with the builder's registry.** That is deliberate and one-way: the
    /// sweep reading a registry with no keys does nothing at all, silently, so
    /// the builder — the single choke point every built runtime passes through
    /// — is the one place that decides. Configure codecs with
    /// [`HarvestBuilder::payload_codec`] / [`HarvestBuilder::payload_codec_key`]
    /// rather than by setting this on a [`WorkerConfig`] you hand to
    /// [`HarvestBuilder::worker`]; a value set there does not survive `build()`.
    pub payload_codecs: PayloadCodecs,
}

/// Drop duplicate shard ids, preserving first-occurrence order (issue #797).
///
/// A duplicate entry in [`WorkerConfig::shard_assignments`] is never useful and
/// is actively harmful: the worker fans its per-shard control loops out over
/// the assignment list one-to-one, so `[0, 0]` spawns **two** timeout checkers,
/// **two** poison-pill reclaimers, and **two** pause auto-resumers against the
/// *same* database — doubling enforcement passes, connection pressure, and DB
/// load for no added coverage, since each pair scans identical rows.
///
/// It also collapses those instances onto one `harvest.scanner.tick` series
/// (they share both the `scanner` and `shard` label values), so a healthy
/// duplicate would keep `rate(...) > 0` and mask its wedged twin from the
/// `harvest_scanner_stalled` alert. Deduplicating at the config boundary fixes
/// the root cause rather than papering over the symptom with an unbounded
/// owner-id metric label — the registry's owner ids are monotonic and never
/// reused, so labelling by them would violate the ADR-0001 §7 bounded-label
/// rule in any process that restarts a runtime.
///
/// First-occurrence order is preserved rather than sorting, so a deliberate
/// polling order stays intact.
#[must_use]
pub(crate) fn dedup_shard_assignments(shards: Vec<ShardId>) -> Vec<ShardId> {
    let mut seen = std::collections::BTreeSet::new();
    shards
        .into_iter()
        .filter(|shard| seen.insert(*shard))
        .collect()
}

/// Resolve a worker's **effective** shard assignments (issue #961, AC1).
///
/// An **empty** [`WorkerConfig::shard_assignments`] means *auto*: cover every
/// shard this process actually has a pool for. Before issue #961 the default
/// was a literal `[ShardId::new(0)]`, so a multi-shard deployment that never
/// called [`WorkerConfig::with_shard_assignments`] silently drained only shard
/// 0 while workflows routed to every other shard sat permanently undispatched
/// — the exact silent failure #961 exists to close.
///
/// Resolution rules:
/// - **Non-empty explicit** → [`dedup_shard_assignments`] of the operator's
///   list, verbatim order. An explicit assignment is a deliberate decision (the
///   one-worker-process-per-shard shape) and is never widened.
/// - **Empty + a sharded pool** → every shard id in the pool, ascending, so the
///   round-robin poll order is deterministic across restarts.
/// - **Empty + no pool** → **empty**, preserved verbatim. There is no shard
///   identity to resolve, so fabricating a `[ShardId::new(0)]` would assert a
///   shard number this process never established — the write-side twin of
///   issue #1150, whose read-side consumers all normalize the empty array as
///   "covers whatever shard the row was read from". [`crate::shard::ShardRouter`]
///   also accepts an arbitrary default shard, so `0` is not a safe stand-in
///   even for a genuinely single-shard deployment. Single-shard stays
///   byte-for-byte unchanged (AC7) because the poll loop's shard is an
///   `Option` and `harvest.shard.dispatched` is emitted only when the worker
///   has a shard identity.
///
/// Deriving from the **pool** rather than the router's writable set is
/// deliberate: the pool is what this process can physically reach, so an
/// auto-resolved assignment can never trip
/// `worker::missing_assigned_shard_pools`. `runner.rs`'s `missing_router_shards`
/// check already refuses startup when a router shard has no pool, making the
/// pool a superset of the writable set in any startable deployment — and the
/// pool additionally covers *draining* (readable-but-not-writable) shards,
/// which still hold in-flight work that must be drained.
#[must_use]
pub(crate) fn resolve_shard_assignments(
    explicit: Vec<ShardId>,
    pool_shards: &[ShardId],
) -> Vec<ShardId> {
    let explicit = dedup_shard_assignments(explicit);
    if !explicit.is_empty() {
        return explicit;
    }
    if pool_shards.is_empty() {
        // No pool => no shard identity to resolve. PRESERVE the empty list
        // rather than fabricating `[ShardId::new(0)]` (issue #961 review,
        // Codex P2).
        //
        // A plain `DbPool` carries no shard number, so a worker here genuinely
        // does not know which logical shard its database is. Registering `[0]`
        // would *claim* shard 0 specifically, which is false whenever the
        // deployment's single/default shard is numbered anything else --
        // `ShardRouter::new` accepts an arbitrary `default_shard`. Shard health
        // and queue coverage would then report no worker for the shard this
        // process is actually draining. That is the write-side twin of the
        // read-side bug issue #1150 fixed, and
        // `shard_fanout::worker_covers_shard` already normalizes an empty
        // registration as "covers whatever shard this database is" precisely so
        // this case reads correctly.
        //
        // Empty is also the shape the rest of the worker already expects for a
        // legacy single-shard worker: it yields an empty `shard_targets`, whose
        // `[] => pool` arms in the claim path, the listener path and the WASM
        // seed path (issue #965 review, Finding 24) all resolve to the caller's
        // default pool.
        return Vec::new();
    }
    let mut auto: Vec<ShardId> = pool_shards.to_vec();
    auto.sort_unstable();
    auto.dedup();
    auto
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queues: vec!["default".to_string()],
            queue_weights: std::collections::HashMap::new(),
            notification_database_url: None,
            shard_notification_database_urls: Vec::new(),
            max_concurrent_workflows: 20,
            max_concurrent_activities: 50,
            shutdown_timeout: Duration::from_secs(30),
            workflow_cache_size: 1000,
            sticky_timeout: Duration::ZERO,
            cancellation_grace_period: Duration::from_secs(5),
            shard_assignments: Vec::new(),
            max_local_activity_start_to_close: Duration::from_secs(60),
            default_activity_retry_policy: None,
            default_activity_start_to_close: None,
            retry_after_ceiling: DEFAULT_RETRY_AFTER_CEILING,
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: String::new(),
            deployment_name: None,
            query_timeout: Duration::from_secs(5),
            priority_aging_secs: None,
            max_workflow_start_delay: DEFAULT_MAX_WORKFLOW_START_DELAY,
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,
            capability_miss_max_redeliveries: 5,
            workflow_task_timeout: Duration::from_secs(10),
            workflow_panic_max_attempts: 3,
            max_workflow_pause_duration: DEFAULT_MAX_WORKFLOW_PAUSE_DURATION,
            labels: std::collections::HashMap::new(),
            max_workflow_history_events: None,
            default_debounce_max_wait: DEFAULT_DEBOUNCE_MAX_WAIT,
            mutex_lease_ttl: crate::mutex::DEFAULT_MUTEX_LEASE_TTL,
            slot_tuner: None,
            #[cfg(feature = "db")]
            sharded_pool: None,
            max_concurrent_sessions: 0,
            codec_rotation_batch_size: crate::codec_rotation::CODEC_ROTATION_DEFAULT_BATCH,
            payload_codecs: PayloadCodecs::default(),
        }
    }
}

impl WorkerConfig {
    /// Replace the queue list.
    ///
    /// # Panics
    ///
    /// Panics if any of the provided queue names are empty strings.
    #[must_use]
    pub fn with_queues<'a>(mut self, queues: impl IntoIterator<Item = &'a str>) -> Self {
        self.queues = queues
            .into_iter()
            .map(|q| {
                assert!(!q.is_empty(), "queue name cannot be empty");
                q.to_owned()
            })
            .collect();
        self
    }

    /// Set per-queue dispatch weights for multi-queue worker fairness (issue #515).
    ///
    /// Each pair `(queue_name, weight)` assigns a relative dispatch probability
    /// to that queue. Under sustained saturation the empirical dispatch share
    /// per queue converges to `weight / sum(all_weights)`.
    ///
    /// Queues bound via [`with_queues`](Self::with_queues) but absent from this
    /// map default to weight **1**. A weight of **0** places the queue last
    /// (fallthrough-only). Calling this method with an empty iterator is a
    /// no-op and preserves the default unchanged behaviour.
    ///
    /// This method **merges** into any previously configured weights (consistent
    /// with [`with_labels`](Self::with_labels)). Repeated calls accumulate
    /// entries; a later entry for the same queue name overwrites the earlier one.
    #[must_use]
    pub fn with_queue_weights<S: Into<String>>(
        mut self,
        weights: impl IntoIterator<Item = (S, u32)>,
    ) -> Self {
        self.queue_weights
            .extend(weights.into_iter().map(|(k, v)| (k.into(), v)));
        self
    }

    /// Enable LISTEN/NOTIFY wakeups using a dedicated Postgres connection.
    #[must_use]
    pub fn with_notification_database_url(mut self, database_url: impl Into<String>) -> Self {
        self.notification_database_url = Some(database_url.into());
        self
    }

    /// Set per-shard LISTEN/NOTIFY notification URLs for multi-shard workers
    /// (issue #522).
    ///
    /// Each entry maps a shard ID to a Postgres URL. Shards not listed fall
    /// back to polling. The entries are additive — calling this method
    /// replaces the full list.
    #[must_use]
    pub fn with_shard_notification_database_urls(
        mut self,
        urls: impl IntoIterator<Item = (crate::types::ShardId, impl Into<String>)>,
    ) -> Self {
        self.shard_notification_database_urls = urls
            .into_iter()
            .map(|(shard, url)| (shard, url.into()))
            .collect();
        self
    }

    /// Override the cancellation grace period.
    ///
    /// After a workflow is cancelled, any running activity gets this long to
    /// notice cooperative cancellation (via [`crate::context::ActivityContext::is_cancelled`]
    /// or [`crate::context::ActivityContext::heartbeat`]) and unwind cleanly. If it is still
    /// running at the end of the grace period the worker aborts the handler
    /// task and marks the activity as cancelled.
    #[must_use]
    pub const fn with_cancellation_grace_period(mut self, grace_period: Duration) -> Self {
        self.cancellation_grace_period = grace_period;
        self
    }

    /// Assign which shards this worker is responsible for.
    ///
    /// An **empty** list means *auto*: cover every shard this process has a
    /// pool for (issue #961). It is **not** coerced to `[ShardId::new(0)]` —
    /// that coercion is what made a multi-shard worker silently single-shard.
    /// See [`resolve_shard_assignments`] for the full resolution rules.
    /// Duplicates are dropped — see [`dedup_shard_assignments`].
    #[must_use]
    pub fn with_shard_assignments(mut self, shards: impl IntoIterator<Item = ShardId>) -> Self {
        self.shard_assignments = dedup_shard_assignments(shards.into_iter().collect());
        self
    }

    /// Override the worker heartbeat interval (default 5 s).
    ///
    /// The management API classifies a worker as stale after
    /// `2 × worker_heartbeat_interval` without a heartbeat write.
    #[must_use]
    pub const fn with_worker_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.worker_heartbeat_interval = interval;
        self
    }

    /// Set the immutable build identifier for this worker (issue #171).
    ///
    /// Use a stable per-build token — a Git SHA, semver tag, or CI job ID.
    /// Workers without a build ID (the default empty string) behave as legacy
    /// workers and can claim any task regardless of build routing policy.
    #[must_use]
    pub fn with_build_id(mut self, build_id: impl Into<String>) -> Self {
        self.build_id = build_id.into();
        self
    }

    /// Set an optional human-readable deployment name (issue #171).
    ///
    /// For operator observability only — e.g. `"prod-blue"`, `"canary"`.
    /// Harvest does not use the deployment name for routing decisions.
    #[must_use]
    pub fn with_deployment_name(mut self, name: impl Into<String>) -> Self {
        self.deployment_name = Some(name.into());
        self
    }

    /// Override the per-query execution timeout (default 5 s, issue #234).
    ///
    /// When a query handler takes longer than this to complete, the engine
    /// terminates the handler and returns [`crate::error::HarvestError::QueryTimedOut`]
    /// to the caller.
    #[must_use]
    pub const fn with_query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// Enable priority aging to prevent low-priority task starvation (issue #249).
    ///
    /// When set to `K` seconds, a task's effective priority is boosted by `+1`
    /// for every `K` seconds it has been waiting in `PENDING` state. This
    /// bounds the maximum starvation time for `Low` priority tasks even under
    /// sustained high-priority load.
    ///
    /// A value of `0` is treated as "no aging" and normalised to `None`.
    /// Defaults to `None` (aging disabled) — existing deployments are
    /// unaffected.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use autumn_harvest::builder::WorkerConfig;
    ///
    /// // Low-priority tasks gain +1 effective priority every 5 minutes of waiting.
    /// let config = WorkerConfig::default().with_priority_aging_secs(300);
    /// assert_eq!(config.priority_aging_secs, Some(300));
    /// ```
    #[must_use]
    pub const fn with_priority_aging_secs(mut self, secs: u32) -> Self {
        self.priority_aging_secs = if secs == 0 { None } else { Some(secs) };
        self
    }

    /// Set the poison-pill quarantine threshold (issue #367).
    ///
    /// A task that crashes `threshold` workers in a row is quarantined to the
    /// dead-letter queue instead of being re-dispatched. Set to `0` to disable
    /// quarantine (reclaimed poison pills are re-queued indefinitely).
    ///
    /// ## Example
    ///
    /// ```rust
    /// use autumn_harvest::builder::WorkerConfig;
    ///
    /// let config = WorkerConfig::default().with_poison_pill_threshold(5);
    /// assert_eq!(config.poison_pill_threshold, 5);
    /// ```
    #[must_use]
    pub const fn with_poison_pill_threshold(mut self, threshold: i32) -> Self {
        self.poison_pill_threshold = threshold;
        self
    }

    /// Override the capability-miss redelivery budget (default 5, issue #804).
    ///
    /// A task claimed by a worker with no handler registered for its
    /// workflow/activity type is released back to `PENDING` for a capable peer,
    /// with capped exponential backoff. A budget of `N` grants exactly `N`
    /// releases; the `N + 1`th claim escalates to the ordinary terminal-failure
    /// path with a `no_capable_worker:` reason on the execution row. (That path
    /// writes no dead-letter entry.)
    ///
    /// Raise this if your rollouts legitimately take longer than the default
    /// dwell window — the five backoffs the default grants sum to ~31 s
    /// (1 + 2 + 4 + 8 + 16) on a single worker, and less in wall-clock terms on
    /// a wide fleet, where incapable peers consume releases in parallel. This
    /// trades a longer time-to-detect for fewer spurious escalations.
    ///
    /// `0` escalates on the **first** miss — the pre-#804 fail-fast behaviour.
    /// It is not an "off" switch for the feature; there is deliberately no
    /// unlimited-release mode, since that would let a genuinely-unregistered
    /// workflow type bounce around the fleet forever.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use autumn_harvest::builder::WorkerConfig;
    ///
    /// let config = WorkerConfig::default().with_capability_miss_max_redeliveries(10);
    /// assert_eq!(config.capability_miss_max_redeliveries, 10);
    /// ```
    #[must_use]
    pub const fn with_capability_miss_max_redeliveries(mut self, budget: u32) -> Self {
        self.capability_miss_max_redeliveries = budget;
        self
    }

    /// Override the per-workflow-task dispatch timeout (default 10 s, issue #494).
    ///
    /// A workflow-task dispatch that does not complete or suspend within this
    /// budget has its concurrency permit reclaimed immediately, allowing other
    /// tasks to proceed. The hung future is cancelled and the task row is reset
    /// to `PENDING` so any worker can re-claim it on the next poll.
    ///
    /// After `poison_pill_threshold` consecutive timeouts for the same
    /// execution the task is escalated to the DLQ rather than re-queued
    /// indefinitely (see [`with_poison_pill_threshold`]).
    ///
    /// Set to [`Duration::ZERO`] to disable (no wall-clock budget on workflow
    /// tasks — the behaviour before this setting was added).
    ///
    /// [`with_poison_pill_threshold`]: Self::with_poison_pill_threshold
    ///
    /// ## Example
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use autumn_harvest::builder::WorkerConfig;
    ///
    /// // Tighten to 5 s for a latency-sensitive deployment.
    /// let config = WorkerConfig::default().with_workflow_task_timeout(Duration::from_secs(5));
    /// assert_eq!(config.workflow_task_timeout, Duration::from_secs(5));
    ///
    /// // Disable the guard entirely.
    /// let config = WorkerConfig::default().with_workflow_task_timeout(Duration::ZERO);
    /// assert!(config.workflow_task_timeout.is_zero());
    /// ```
    #[must_use]
    pub const fn with_workflow_task_timeout(mut self, timeout: Duration) -> Self {
        self.workflow_task_timeout = timeout;
        self
    }

    /// Override the maximum number of panic re-dispatches before a
    /// panicking-workflow run fails terminally (issue #782).
    ///
    /// See [`workflow_panic_max_attempts`](Self::workflow_panic_max_attempts).
    /// Defaults to **3**; `0` fails terminally on the first panic.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use autumn_harvest::builder::WorkerConfig;
    ///
    /// let config = WorkerConfig::default().with_workflow_panic_max_attempts(5);
    /// assert_eq!(config.workflow_panic_max_attempts, 5);
    ///
    /// // Fail terminally on the first panic.
    /// let config = WorkerConfig::default().with_workflow_panic_max_attempts(0);
    /// assert_eq!(config.workflow_panic_max_attempts, 0);
    /// ```
    #[must_use]
    pub const fn with_workflow_panic_max_attempts(mut self, attempts: u32) -> Self {
        self.workflow_panic_max_attempts = attempts;
        self
    }

    /// Override the maximum start delay for a workflow (issue #322).
    ///
    /// Default: 365 days.
    #[must_use]
    pub const fn with_max_workflow_start_delay(mut self, delay: Duration) -> Self {
        self.max_workflow_start_delay = delay;
        self
    }

    /// Override the unknown target grace window for cross-workflow signaling (issue #330).
    ///
    /// Default: 5 seconds.
    #[must_use]
    pub const fn with_unknown_target_grace_window(mut self, window: Duration) -> Self {
        self.unknown_target_grace_window = window;
        self
    }

    /// Override the bounded-pause ceiling before auto-resume (issue #383).
    ///
    /// A workflow paused longer than this is force-resumed by the auto-resume
    /// scanner with `actor = "auto-resume(timeout)"`. Default: 24 hours.
    #[must_use]
    pub const fn with_max_workflow_pause_duration(mut self, max: Duration) -> Self {
        self.max_workflow_pause_duration = max;
        self
    }

    /// Enable sticky cross-worker routing (issue #235).
    ///
    /// Sticky routing is **off by default**. When enabled, each time a workflow
    /// suspends the task queue records a soft affinity lease pointing at the
    /// current worker. Subsequent tasks for that execution are offered to the
    /// owning worker first so its in-process LRU cache stays warm, reducing
    /// full event-history reloads from Postgres.
    ///
    /// When the lease expires (after `config.lease_ttl`) the task becomes
    /// claimable by any eligible worker — sticky routing never blocks progress.
    /// Note: worker drain or unhealthy status does **not** trigger early lease
    /// expiry; only the TTL controls when other workers can claim the task.
    ///
    /// See `docs/sticky-routing.md` for the full operator guide including
    /// the lease-TTL trade-off and interaction with shard assignments and
    /// build-id routing.
    ///
    /// # Example
    ///
    /// ```rust
    /// use autumn_harvest::builder::{StickyRoutingConfig, WorkerConfig};
    /// use std::time::Duration;
    ///
    /// let config = WorkerConfig::default()
    ///     .with_sticky_routing(StickyRoutingConfig {
    ///         lease_ttl: Duration::from_secs(10),
    ///     });
    /// ```
    #[must_use]
    pub const fn with_sticky_routing(mut self, config: StickyRoutingConfig) -> Self {
        self.sticky_timeout = config.lease_ttl;
        self
    }

    /// Attach a key-value capability label (issue #382).
    #[must_use]
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Attach a map or list of key-value capability labels (issue #382).
    #[must_use]
    pub fn with_labels(mut self, labels: impl IntoIterator<Item = (String, String)>) -> Self {
        self.labels.extend(labels);
        self
    }

    /// Set the hard ceiling on workflow history events (issue #493).
    ///
    /// Pass `None` to disable (the default). When set, any `RUNNING` execution
    /// that accumulates `n` or more events is terminated with
    /// `history_ceiling_exceeded`. Must be strictly greater than
    /// `continue_as_new_threshold` (checked at worker startup, not here).
    #[must_use]
    pub const fn with_max_workflow_history_events(mut self, ceiling: Option<u64>) -> Self {
        self.max_workflow_history_events = ceiling;
        self
    }

    /// Override the default max-wait cap applied to debounced workflow starts
    /// when `DebouncePolicy.max_wait` is `None` (issue #499).
    ///
    /// Prevents a continuously-retriggered workflow from being deferred
    /// indefinitely. Defaults to **1 hour**.
    #[must_use]
    pub const fn with_default_debounce_max_wait(mut self, max_wait: Duration) -> Self {
        self.default_debounce_max_wait = max_wait;
        self
    }

    /// Override the lease TTL for held durable mutexes (`ctx.mutex`, issue #691).
    ///
    /// Must exceed the worst-case wall-clock time a workflow spends inside a
    /// single held critical section (the interval between decision cycles that
    /// renew the lease); too short a TTL lets the scanner reclaim a lock while
    /// its holder is still mid-critical-section. Defaults to **60 seconds**.
    #[must_use]
    pub const fn with_mutex_lease_ttl(mut self, ttl: Duration) -> Self {
        self.mutex_lease_ttl = ttl;
        self
    }

    /// Install an adaptive dispatch-slot tuner (issue #548).
    ///
    /// Both dispatch semaphores are auto-resized within
    /// `[cfg.min_slots, cfg.max_slots]`. When this method is never called
    /// (`slot_tuner` stays `None`), the worker's fixed-concurrency semaphore
    /// behaviour is byte-for-byte identical to today.
    ///
    /// See [`crate::slot_tuner`] for the default controller's signals
    /// (slot utilization, worker DB-pool pressure, claim-to-dispatch permit
    /// wait) and `docs/operations/adaptive-slot-tuner.md` for the operator
    /// guide.
    #[must_use]
    pub fn with_slot_tuner(mut self, cfg: crate::slot_tuner::SlotTunerConfig) -> Self {
        self.slot_tuner = Some(cfg);
        self
    }

    #[cfg(feature = "db")]
    /// Set the sharded database pool for exact shard routing.
    #[must_use]
    pub fn with_sharded_pool(mut self, pool: crate::shard::ShardedDbPool) -> Self {
        self.sharded_pool = Some(pool);
        self
    }

    /// Advertise worker-session capacity (issue #606).
    ///
    /// Enables `ctx.create_session(...)` to acquire this worker: up to `n`
    /// sessions may be `ACTIVE` on it at once. Defaults to `0` (sessions
    /// disabled) — the single builder call adopting this feature costs the
    /// author, per the issue's success metric.
    #[must_use]
    pub const fn with_max_concurrent_sessions(mut self, n: i32) -> Self {
        self.max_concurrent_sessions = n;
        self
    }

    /// Set the lazy payload-codec re-encryption sweep's per-shard batch size
    /// (issue #948).
    ///
    /// `0` disables the sweep. See
    /// [`WorkerConfig::codec_rotation_batch_size`].
    #[must_use]
    pub const fn with_codec_rotation_batch_size(mut self, rows: i64) -> Self {
        self.codec_rotation_batch_size = rows;
        self
    }

    /// Set the builder-level default activity retry policy (issue #620).
    ///
    /// Resolved at schedule time as the lowest-priority fallback: a call-site
    /// override or an activity's own `#[activity(retry = …)]` default both win
    /// over this floor. Unset (the default) leaves today's behaviour
    /// byte-for-byte unchanged.
    #[must_use]
    pub fn with_default_activity_retry_policy(
        mut self,
        policy: crate::policy::RetryPolicy,
    ) -> Self {
        self.default_activity_retry_policy = Some(policy);
        self
    }

    /// Set the builder-level default activity `start_to_close` timeout (issue #620).
    ///
    /// Same precedence as [`WorkerConfig::with_default_activity_retry_policy`].
    /// For *local* activities the resolved value is still clamped by
    /// [`WorkerConfig::max_local_activity_start_to_close`].
    #[must_use]
    pub const fn with_default_activity_start_to_close(mut self, timeout: Duration) -> Self {
        self.default_activity_start_to_close = Some(timeout);
        self
    }

    /// Set the ceiling on an author-supplied `Retry-After` delay hint (issue
    /// #744). See [`WorkerConfig::retry_after_ceiling`] for the full
    /// semantics. Default: [`DEFAULT_RETRY_AFTER_CEILING`] (15 minutes).
    #[must_use]
    pub const fn with_retry_after_ceiling(mut self, ceiling: Duration) -> Self {
        self.retry_after_ceiling = ceiling;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::info::{DagInfo, WorkflowInfo};
    use crate::policy::Schedule;

    /// A duplicate shard would fan the per-shard control loops out twice
    /// against one database, and would collapse those two instances onto a
    /// single `(scanner, shard)` tick series so a healthy duplicate masks its
    /// wedged twin (issue #797).
    #[test]
    fn with_shard_assignments_drops_duplicate_shards() {
        let cfg = WorkerConfig::default().with_shard_assignments([
            ShardId::new(0),
            ShardId::new(1),
            ShardId::new(0),
        ]);
        assert_eq!(
            cfg.shard_assignments,
            vec![ShardId::new(0), ShardId::new(1)],
            "a repeated shard must not spawn a second set of control loops \
             against the same database",
        );
    }

    /// Ordering is a deliberate polling order, not an artifact — dedup must
    /// not reorder the shards an operator listed.
    #[test]
    fn dedup_shard_assignments_preserves_first_occurrence_order() {
        let deduped = dedup_shard_assignments(vec![
            ShardId::new(2),
            ShardId::new(0),
            ShardId::new(2),
            ShardId::new(1),
            ShardId::new(0),
        ]);
        assert_eq!(
            deduped,
            vec![ShardId::new(2), ShardId::new(0), ShardId::new(1)],
        );
    }

    /// AC1 (issue #961): a worker with **no explicit** shard assignments must
    /// cover every shard the process actually has a pool for, not just shard 0.
    ///
    /// Before this, `WorkerConfig::default().shard_assignments` was
    /// `[ShardId::new(0)]`, so a three-shard deployment that never called
    /// `with_shard_assignments` silently drained only shard 0 while workflows
    /// routed to shards 1 and 2 sat permanently undispatched — precisely the
    /// silent failure issue #961 exists to close.
    #[test]
    fn resolve_shard_assignments_empty_covers_every_pool_shard() {
        let resolved = resolve_shard_assignments(
            Vec::new(),
            &[ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        );
        assert_eq!(
            resolved,
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            "an unconfigured worker must auto-cover every shard it has a pool for",
        );
    }

    /// An explicit assignment is an operator decision and always wins — auto
    /// resolution must never widen a deliberately narrowed worker (the
    /// one-worker-process-per-shard deployment shape).
    #[test]
    fn resolve_shard_assignments_explicit_wins_over_pool() {
        let resolved = resolve_shard_assignments(
            vec![ShardId::new(1)],
            &[ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        );
        assert_eq!(resolved, vec![ShardId::new(1)]);
    }

    /// Explicit assignments still go through dedup — a duplicate would fan the
    /// per-shard control loops out twice against one database (issue #797).
    #[test]
    fn resolve_shard_assignments_dedups_explicit() {
        let resolved = resolve_shard_assignments(
            vec![ShardId::new(2), ShardId::new(0), ShardId::new(2)],
            &[ShardId::new(0), ShardId::new(1), ShardId::new(2)],
        );
        assert_eq!(resolved, vec![ShardId::new(2), ShardId::new(0)]);
    }

    /// **Issue #961 review (Codex P2).** With no sharded pool the resolver
    /// must PRESERVE the empty list, never fabricate `[ShardId::new(0)]`.
    ///
    /// A plain `DbPool` carries no shard number, so this worker genuinely does
    /// not know which logical shard its database is. Claiming `[0]` is false
    /// whenever the deployment's single/default shard is numbered anything else
    /// (`ShardRouter::new` accepts an arbitrary `default_shard`), and shard
    /// health / queue coverage would then report no worker for the shard the
    /// process is actually draining -- the write-side twin of the read-side bug
    /// issue #1150 fixed. Empty is the legacy representation
    /// `shard_fanout::worker_covers_shard` normalizes as "covers whatever shard
    /// this database is".
    #[test]
    fn resolve_shard_assignments_no_pool_preserves_the_empty_legacy_shape() {
        assert_eq!(
            resolve_shard_assignments(Vec::new(), &[]),
            Vec::<ShardId>::new(),
            "no pool means no shard identity; fabricating [0] would falsely \
             claim shard 0 on a deployment whose single shard is numbered \
             otherwise",
        );
    }

    /// AC7: a single-shard `ShardedDbPool` resolves to the same `[0]` a
    /// pool-less deployment gets, so wiring `ShardedDbPool::single` in changes
    /// nothing.
    #[test]
    fn resolve_shard_assignments_single_shard_pool_is_unchanged() {
        assert_eq!(
            resolve_shard_assignments(Vec::new(), &[ShardId::new(0)]),
            vec![ShardId::new(0)],
        );
    }

    /// Pool-derived shards are emitted in ascending shard order so the
    /// round-robin poll order is deterministic across restarts.
    #[test]
    fn resolve_shard_assignments_pool_order_is_ascending() {
        let resolved = resolve_shard_assignments(Vec::new(), &[ShardId::new(5), ShardId::new(1)]);
        assert_eq!(resolved, vec![ShardId::new(1), ShardId::new(5)]);
    }

    /// `with_shard_assignments([])` must record "auto" (empty) rather than
    /// coercing to `[0]` — coercion is what made a multi-shard worker silently
    /// single-shard.
    #[test]
    fn with_shard_assignments_empty_records_auto_not_shard_zero() {
        let cfg = WorkerConfig::default().with_shard_assignments(Vec::<ShardId>::new());
        assert!(
            cfg.shard_assignments.is_empty(),
            "an empty assignment list means 'auto: cover every pool shard', \
             not 'shard 0 only'",
        );
    }

    /// The default config must also mean "auto", so an embedder that never
    /// touches `shard_assignments` gets full coverage.
    #[test]
    fn default_worker_config_shard_assignments_are_auto() {
        assert!(WorkerConfig::default().shard_assignments.is_empty());
    }

    /// An all-duplicates list must still leave a usable assignment rather than
    /// collapsing to empty and tripping the single-shard fallback.
    #[test]
    fn with_shard_assignments_keeps_one_entry_when_every_entry_is_the_same() {
        let cfg = WorkerConfig::default().with_shard_assignments([
            ShardId::new(3),
            ShardId::new(3),
            ShardId::new(3),
        ]);
        assert_eq!(cfg.shard_assignments, vec![ShardId::new(3)]);
    }

    fn fake_workflow_info() -> WorkflowInfo {
        WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "test",
            module: "test",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
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
        }
    }

    fn fake_dag_info() -> DagInfo {
        fn build(_dag: &mut DagBuilder) {}

        DagInfo {
            name: "daily_etl",
            module: "test",
            schedule: Some(Schedule::Manual),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("default"),
            builder: build,
            workflow_handler: None,
            jitter: ::std::time::Duration::ZERO,
            overlap_policy: crate::policy::OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
            execution_timeout: None,
            sla: None,
        }
    }

    #[cfg(feature = "unified-dag-execution")]
    fn fake_unified_dag_info() -> DagInfo {
        fn build(_dag: &mut DagBuilder) {}

        DagInfo {
            name: "daily_etl",
            module: "test",
            schedule: Some(Schedule::Manual),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("default"),
            builder: build,
            workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
            jitter: ::std::time::Duration::ZERO,
            overlap_policy: crate::policy::OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
            execution_timeout: None,
            sla: None,
        }
    }

    #[test]
    fn harvest_builder_collects_workflows() {
        let builder = HarvestBuilder::new().workflows(vec![fake_workflow_info()]);
        assert_eq!(builder.workflow_count(), 1);
    }

    #[test]
    fn workflow_infos_accessor_exposes_registered_infos_with_mcp_flag() {
        // Issue #597: the plugin's MCP tool generator reads registered
        // workflows (incl. the mcp flag) from the builder before startup.
        let builder = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info(), fake_workflow_info().with_mcp()]);
        let infos = builder.workflow_infos();
        assert_eq!(infos.len(), 2);
        assert!(!infos[0].mcp);
        assert!(infos[1].mcp);
    }

    #[test]
    fn worker_heartbeat_interval_defaults_to_5s() {
        assert_eq!(
            WorkerConfig::default().worker_heartbeat_interval,
            Duration::from_secs(5)
        );
    }

    #[test]
    fn worker_heartbeat_interval_zero_is_rejected() {
        let result = HarvestBuilder::new()
            .worker(WorkerConfig::default().with_worker_heartbeat_interval(Duration::ZERO))
            .try_build();
        assert!(
            matches!(result, Err(HarvestBuilderError::InvalidWorkerConfig(_))),
            "expected InvalidWorkerConfig but got {result:?}"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn slow_heartbeat_warns_but_never_blocks_the_build() {
        // The default cadence -- and anything up to the supported ceiling --
        // must stay silent, or the warning is noise operators learn to ignore.
        assert!(!warn_if_heartbeat_outruns_fleet_liveness(
            WorkerConfig::default().worker_heartbeat_interval
        ));
        assert!(!warn_if_heartbeat_outruns_fleet_liveness(
            crate::worker::MAX_SUPPORTED_HEARTBEAT_INTERVAL_FOR_FLEET_LIVENESS
        ));
        // One second past the ceiling is where a peer can start mistaking this
        // worker for a dead one.
        assert!(warn_if_heartbeat_outruns_fleet_liveness(
            crate::worker::MAX_SUPPORTED_HEARTBEAT_INTERVAL_FOR_FLEET_LIVENESS
                + Duration::from_secs(1)
        ));
        // Warn, never reject: an already-deployed slow fleet must keep booting.
        assert!(
            HarvestBuilder::new()
                .worker(
                    WorkerConfig::default()
                        .with_worker_heartbeat_interval(Duration::from_secs(600))
                )
                .try_build()
                .is_ok(),
            "a slow heartbeat is a warning, not a build failure"
        );
    }

    #[test]
    fn worker_config_default_queues() {
        let config = WorkerConfig::default();
        assert!(config.queues.contains(&"default".to_string()));
        assert!(config.notification_database_url.is_none());
    }

    #[test]
    fn worker_config_builder_adds_queues() {
        let config = WorkerConfig::default().with_queues(["email-workers", "etl"]);
        assert!(config.queues.contains(&"email-workers".to_string()));
    }

    #[test]
    fn with_slot_tuner_sets_config_and_default_is_none() {
        assert!(WorkerConfig::default().slot_tuner.is_none());

        let config =
            WorkerConfig::default().with_slot_tuner(crate::slot_tuner::SlotTunerConfig::new(5, 50));
        let tuner = config.slot_tuner.expect("slot_tuner must be set");
        assert_eq!(tuner.min_slots, 5);
        assert_eq!(tuner.max_slots, 50);
    }

    #[test]
    fn worker_config_shard_notification_urls_default_empty() {
        let config = WorkerConfig::default();
        assert!(
            config.shard_notification_database_urls.is_empty(),
            "shard_notification_database_urls must default to empty"
        );
    }

    #[test]
    fn worker_config_with_shard_notification_database_urls_sets_entries() {
        use crate::types::ShardId;
        let config = WorkerConfig::default().with_shard_notification_database_urls([
            (ShardId::new(0), "postgres://host0/harvest"),
            (ShardId::new(1), "postgres://host1/harvest"),
        ]);
        assert_eq!(config.shard_notification_database_urls.len(), 2);
        assert_eq!(
            config.shard_notification_database_urls[0].0,
            ShardId::new(0)
        );
        assert_eq!(
            config.shard_notification_database_urls[1].0,
            ShardId::new(1)
        );
    }

    #[test]
    fn worker_config_default_max_pause_duration_is_24h() {
        let config = WorkerConfig::default();
        assert_eq!(
            config.max_workflow_pause_duration,
            Duration::from_secs(24 * 3600),
            "bounded-pause ceiling must default to 24 hours"
        );
    }

    #[test]
    fn worker_config_with_max_pause_duration_overrides() {
        let config =
            WorkerConfig::default().with_max_workflow_pause_duration(Duration::from_secs(60));
        assert_eq!(config.max_workflow_pause_duration, Duration::from_secs(60));
    }

    #[test]
    fn worker_config_with_empty_queues_clears_list() {
        let config = WorkerConfig::default().with_queues(Vec::<&str>::new());
        assert!(config.queues.is_empty());
    }

    #[test]
    fn worker_config_builder_sets_notification_database_url() {
        let config =
            WorkerConfig::default().with_notification_database_url("postgres://localhost/test");
        assert_eq!(
            config.notification_database_url.as_deref(),
            Some("postgres://localhost/test")
        );
    }

    #[test]
    fn harvest_builder_collects_dags() {
        let builder = HarvestBuilder::new().dags(vec![fake_dag_info()]);
        assert_eq!(builder.dag_count(), 1);
    }

    // ── Issue #780 — declarative DAG node compensation validations ──────────

    /// T6 — a compensator that resolves to a **local** activity is rejected by
    /// `validate_dags_do_not_use_local_activities`. A compensator is dispatched
    /// through the ordinary DAG activity-queue lowering (`execute_activity_raw_with_opts`),
    /// exactly like a forward node, so a local activity is just as invalid there.
    /// The error must name the COMPENSATOR (not the forward node that declares it).
    #[test]
    fn local_activity_compensator_is_rejected_by_the_builder() {
        fn forward() {}

        let dag_with_local_compensator = DagInfo {
            name: "etl_with_local_comp",
            module: "test",
            schedule: None,
            catchup: false,
            max_active_runs: 1,
            default_queue: None,
            builder: |dag: &mut DagBuilder| {
                let _node = dag.activity(forward).compensate_named("undo_forward");
            },
            // Unified, so the classic-DAG compensation guard (T7) cannot fire
            // first and mask the local-activity rejection under test.
            workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
            jitter: ::std::time::Duration::ZERO,
            overlap_policy: crate::policy::OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
            execution_timeout: None,
            sla: None,
        };

        let result = HarvestBuilder::new()
            .dags(vec![dag_with_local_compensator])
            .activities(vec![make_local_activity("undo_forward", None)])
            .try_build();

        let err = result.expect_err("a local-activity compensator must be rejected");
        assert!(
            matches!(
                err,
                HarvestBuilderError::LocalActivityInDag { ref activity, ref dag }
                    if activity == "undo_forward" && dag == "etl_with_local_comp"
            ),
            "the rejection must name the compensator and its DAG, got: {err:?}"
        );
    }

    /// T7 — a **classic** (non-unified) DAG (`workflow_handler: None`) that
    /// declares a compensator is rejected at `try_build`. Compensation lowers
    /// onto the unified workflow-handler path (`run_unified_dag`'s terminal
    /// unwind via `Saga`); the classic DAG executor has no unwind step, so the
    /// compensator would silently never run. Mirrors
    /// `DagSignalGateRequiresUnifiedExecution` (issue #746).
    #[test]
    fn classic_dag_with_a_compensator_is_rejected() {
        fn forward() {}

        let classic_compensated_dag = DagInfo {
            name: "classic_compensated_dag",
            module: "test",
            schedule: None,
            catchup: false,
            max_active_runs: 1,
            default_queue: None,
            builder: |dag: &mut DagBuilder| {
                let _node = dag.activity(forward).compensate_named("undo_forward");
            },
            // The unified-vs-classic discriminator.
            workflow_handler: None,
            jitter: ::std::time::Duration::ZERO,
            overlap_policy: crate::policy::OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
            execution_timeout: None,
            sla: None,
        };

        let result = HarvestBuilder::new()
            .dags(vec![classic_compensated_dag])
            .try_build();

        let err = result.expect_err("a classic DAG with a compensator must be rejected");
        assert!(
            matches!(
                err,
                HarvestBuilderError::DagCompensationRequiresUnifiedExecution {
                    ref dag,
                    ref task,
                    ref compensate,
                } if dag == "classic_compensated_dag"
                    && task == "forward"
                    && compensate == "undo_forward"
            ),
            "the rejection must name the DAG, the node, and the compensator, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("classic_compensated_dag") && msg.contains("undo_forward"),
            "the message must name the DAG and the compensator, got: {msg}"
        );
    }

    #[cfg(feature = "unified-dag-execution")]
    #[test]
    fn harvest_builder_rejects_workflow_schedule_targeting_auto_registered_dag_name() {
        let result = HarvestBuilder::new()
            .dags(vec![fake_unified_dag_info()])
            .workflow_schedule(WorkflowSchedule::new(
                "daily_etl",
                Schedule::Interval(Duration::from_secs(60)),
            ))
            .try_build();

        let err = result.unwrap_err();
        assert!(matches!(
            err,
            HarvestBuilderError::InvalidWorkflowSchedule {
                ref workflow_name,
                ..
            } if workflow_name == "daily_etl"
        ));
        assert!(
            err.to_string().contains("auto-registered DAG"),
            "error should explain the DAG/workflow schedule collision: {err}"
        );
    }

    #[test]
    fn harvest_builder_build_registers_shared_state() {
        let built = HarvestBuilder::new().state(String::from("hello")).build();

        assert_eq!(built.workflow_count(), 0);
        assert_eq!(built.activity_count(), 0);
        assert_eq!(built.dag_count(), 0);
        assert_eq!(built.state::<String>(), Some(&String::from("hello")));
        assert!(built.state::<u64>().is_none());
    }

    #[test]
    fn harvest_builder_build_defaults_telemetry_to_noop() {
        let built = HarvestBuilder::new().build();
        // Default is a safe no-op: capturing yields nothing.
        assert!(built.telemetry().capture_trace_context().is_none());
    }

    #[test]
    fn harvest_builder_defaults_history_guardrails() {
        let built = HarvestBuilder::new().build();
        let policy = built.history_policy();

        assert_eq!(policy.continue_as_new_threshold(), 10_000);
        assert_eq!(policy.event_hard_cap(), None);
    }

    #[test]
    fn harvest_builder_accepts_history_guardrail_overrides() {
        let built = HarvestBuilder::new()
            .history_continue_as_new_threshold(128)
            .history_event_hard_cap(256)
            .build();
        let policy = built.history_policy();

        assert_eq!(policy.continue_as_new_threshold(), 128);
        assert_eq!(policy.event_hard_cap(), Some(256));
    }

    #[test]
    fn harvest_builder_accepts_deadline_fraction_override() {
        // Issue #772: the deadline-aware continue-as-new fraction is
        // configurable and clamped into [0.0, 1.0].
        let built = HarvestBuilder::new()
            .history_continue_as_new_deadline_fraction(0.6)
            .build();
        let policy = built.history_policy();
        assert!((policy.continue_as_new_deadline_fraction() - 0.6).abs() < f64::EPSILON);

        let clamped = HarvestBuilder::new()
            .history_continue_as_new_deadline_fraction(2.0)
            .build();
        assert!(
            (clamped.history_policy().continue_as_new_deadline_fraction() - 1.0).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn harvest_builder_accepts_history_bloat_warn_fraction_override() {
        // Issue #704: the operator early-warning soft-threshold fraction is
        // configurable and clamped into [0.0, MAX_HISTORY_BLOAT_WARN_FRACTION]
        // -- strictly below 1.0, unlike the sibling deadline-fraction override
        // above (PR #1139 review, P2: worker.rs's hard-cap force-fail check
        // always wins the race against a warn threshold of exactly the cap,
        // so a fraction of 1.0 must never be reachable through this API).
        let built = HarvestBuilder::new()
            .history_bloat_warn_fraction(0.5)
            .build();
        let policy = built.history_policy();
        assert!((policy.history_bloat_warn_fraction() - 0.5).abs() < f64::EPSILON);

        let clamped = HarvestBuilder::new()
            .history_bloat_warn_fraction(2.0)
            .build();
        assert!(clamped.history_policy().history_bloat_warn_fraction() < 1.0);
        assert!(
            (clamped.history_policy().history_bloat_warn_fraction()
                - crate::context::MAX_HISTORY_BLOAT_WARN_FRACTION)
                .abs()
                < f64::EPSILON
        );

        // Exactly 1.0 must ALSO clamp strictly below 1.0 -- it is the one
        // value that would otherwise silently disable the signal.
        let exactly_one = HarvestBuilder::new()
            .history_bloat_warn_fraction(1.0)
            .build();
        assert!(exactly_one.history_policy().history_bloat_warn_fraction() < 1.0);

        // 0.0 explicitly disables the signal (AC4).
        let disabled = HarvestBuilder::new()
            .history_bloat_warn_fraction(0.0)
            .build();
        assert!(
            disabled
                .history_policy()
                .history_bloat_warn_fraction()
                .abs()
                < f64::EPSILON
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn harvest_builder_passes_history_policy_to_worker_registry() {
        // `into_worker_parts` unconditionally writes the process-global
        // start-idempotency sweep window; serialize against the sibling
        // `into_worker_parts_installs_configured_start_idempotency_purge_window`
        // test, which reads that global back (issue #808 / #620).
        let _guard = crate::start_idempotency::PURGE_WINDOW_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let built = HarvestBuilder::new()
            .history_continue_as_new_threshold(9)
            .history_event_hard_cap(11)
            .build();

        let (registry, _dags, _workflow_schedules, _worker_config) = built.into_worker_parts();

        assert_eq!(registry.history_policy().continue_as_new_threshold(), 9);
        assert_eq!(registry.history_policy().event_hard_cap(), Some(11));
    }

    // Issue #620: the two `into_worker_parts*` hunks copy the builder-level
    // activity defaults out of `WorkerConfig` and into the `HandlerRegistry`.
    // Every other #620 test injects them via `HandlerRegistry::
    // with_activity_defaults` directly, bypassing those hunks — so this test
    // drives the REAL public entry point (`WorkerConfig::with_default_*` ->
    // `.worker(..)` -> `.build()` -> `.into_worker_parts()`) and asserts the
    // registry carries the configured floor. Without the wiring hunks the
    // registry would report `None` and this test would fail.
    #[cfg(feature = "db")]
    #[test]
    fn harvest_builder_wires_activity_defaults_into_worker_registry() {
        use crate::policy::RetryPolicy;

        // `into_worker_parts` unconditionally writes the process-global
        // start-idempotency sweep window; serialize against the sibling
        // `into_worker_parts_installs_configured_start_idempotency_purge_window`
        // test, which reads that global back (issue #808 / #620).
        let _guard = crate::start_idempotency::PURGE_WINDOW_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let built = HarvestBuilder::new()
            .worker(
                WorkerConfig::default()
                    .with_default_activity_retry_policy(RetryPolicy::fixed(
                        7,
                        Duration::from_millis(25),
                    ))
                    .with_default_activity_start_to_close(Duration::from_secs(42)),
            )
            .build();

        let (registry, _dags, _workflow_schedules, _worker_config) = built.into_worker_parts();

        assert_eq!(
            registry
                .default_activity_retry_policy()
                .as_ref()
                .map(|p| p.max_attempts),
            Some(7),
            "the builder-level default retry policy must be wired into the registry"
        );
        assert_eq!(
            registry.default_activity_start_to_close(),
            Some(Duration::from_secs(42)),
            "the builder-level default start_to_close must be wired into the registry"
        );
    }

    // Issue #620: the `into_worker_parts_with_extra_state` hunk is a distinct
    // code path (used by the plugin's extra-state runner); assert it wires the
    // same floor. An empty extra-state map is the minimal input.
    #[cfg(feature = "db")]
    #[test]
    fn harvest_builder_extra_state_wires_activity_defaults_into_worker_registry() {
        use crate::policy::RetryPolicy;

        // `into_worker_parts_with_extra_state` unconditionally writes the
        // process-global start-idempotency sweep window; serialize against the
        // sibling
        // `into_worker_parts_installs_configured_start_idempotency_purge_window`
        // test, which reads that global back (issue #808 / #620).
        let _guard = crate::start_idempotency::PURGE_WINDOW_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let built = HarvestBuilder::new()
            .worker(
                WorkerConfig::default()
                    .with_default_activity_retry_policy(RetryPolicy::fixed(
                        6,
                        Duration::from_millis(15),
                    ))
                    .with_default_activity_start_to_close(Duration::from_secs(17)),
            )
            .build();

        let (registry, _dags, _workflow_schedules, _worker_config) =
            built.into_worker_parts_with_extra_state(crate::context::SharedStateMap::new());

        assert_eq!(
            registry
                .default_activity_retry_policy()
                .as_ref()
                .map(|p| p.max_attempts),
            Some(6),
        );
        assert_eq!(
            registry.default_activity_start_to_close(),
            Some(Duration::from_secs(17)),
        );
    }

    // Issue #744: the two `into_worker_parts*` hunks must also copy the
    // builder-level `retry_after_ceiling` out of `WorkerConfig` into the
    // `HandlerRegistry` so the worker's next-retry-delay computation sees the
    // operator-configured ceiling, not a hardcoded default.
    //
    // RED PHASE: `HandlerRegistry::retry_after_ceiling` does not exist yet —
    // this test fails to COMPILE against the missing method until the green
    // phase adds it.
    #[cfg(feature = "db")]
    #[test]
    fn harvest_builder_wires_retry_after_ceiling_into_worker_registry() {
        let _guard = crate::start_idempotency::PURGE_WINDOW_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let built = HarvestBuilder::new()
            .worker(WorkerConfig::default().with_retry_after_ceiling(Duration::from_secs(123)))
            .build();

        let (registry, _dags, _workflow_schedules, _worker_config) = built.into_worker_parts();

        assert_eq!(
            registry.retry_after_ceiling,
            Duration::from_secs(123),
            "the builder-configured retry_after ceiling must be wired into the registry"
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn harvest_builder_extra_state_wires_retry_after_ceiling_into_worker_registry() {
        let _guard = crate::start_idempotency::PURGE_WINDOW_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let built = HarvestBuilder::new()
            .worker(WorkerConfig::default().with_retry_after_ceiling(Duration::from_secs(77)))
            .build();

        let (registry, _dags, _workflow_schedules, _worker_config) =
            built.into_worker_parts_with_extra_state(crate::context::SharedStateMap::new());

        assert_eq!(registry.retry_after_ceiling, Duration::from_secs(77));
    }

    #[test]
    fn harvest_builder_telemetry_override_is_propagated() {
        use crate::telemetry::{TelemetryConfig, TraceContextCarrier, TraceContextPropagator};
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct StubProp {
            captured: AtomicUsize,
        }
        impl TraceContextPropagator for StubProp {
            fn capture(&self) -> Option<TraceContextCarrier> {
                self.captured.fetch_add(1, Ordering::SeqCst);
                Some(TraceContextCarrier::from_traceparent("00-f00-b44-01"))
            }
            fn install(&self, _carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
                Box::new(())
            }
        }

        let prop = std::sync::Arc::new(StubProp::default());
        let built = HarvestBuilder::new()
            .telemetry(TelemetryConfig::builder().propagator(prop.clone()).build())
            .build();

        assert_eq!(prop.captured.load(Ordering::SeqCst), 0);
        let carrier = built.telemetry().capture_trace_context().unwrap();
        assert_eq!(carrier.traceparent.as_deref(), Some("00-f00-b44-01"));
        assert_eq!(prop.captured.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "db")]
    #[test]
    fn built_harvest_into_worker_parts_preserves_shared_state() {
        // `into_worker_parts` unconditionally writes the process-global
        // start-idempotency sweep window; serialize against the sibling
        // `into_worker_parts_installs_configured_start_idempotency_purge_window`
        // test, which reads that global back (issue #808 / #620).
        let _guard = crate::start_idempotency::PURGE_WINDOW_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let built = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .activities(vec![ActivityInfo {
                name: "test_activity",
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
                rate_limit_rps: None,
                rate_limit_burst: None,
                rate_limit_key: None,
                rate_limit_key_expr: None,
                circuit_breaker: None,
                requires: None,
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            }])
            .state(String::from("haunted"))
            .build();

        let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();

        assert_eq!(registry.state::<String>(), Some(&String::from("haunted")));
        assert!(worker_config.queues.contains(&"default".to_string()));
    }

    /// `build()` owns `WorkerConfig::payload_codecs`: a registry set on a
    /// caller-supplied `WorkerConfig` does not survive, and the builder's wins
    /// (issue #948).
    ///
    /// The precedence is one-way on purpose. A sweep whose registry holds no
    /// keys does nothing *silently*, so the builder — the single choke point
    /// every built runtime passes through — decides, and a runner cannot half-
    /// wire rotation by configuring one of the two surfaces. This pins the
    /// direction so it cannot be inverted by accident; the setter that used to
    /// invite the mistake is gone.
    #[test]
    fn build_overwrites_a_worker_supplied_payload_codec_registry() {
        use crate::payload_codec::{IdentityCodec, PayloadCodecs};

        // A registry the caller puts on the WorkerConfig, holding a key.
        let caller_side = PayloadCodecs::default();
        caller_side
            .register_key("caller-key", std::sync::Arc::new(IdentityCodec))
            .expect("register caller key");
        assert!(caller_side.has_keyed_codecs());

        let built = HarvestBuilder::new()
            .worker(WorkerConfig {
                payload_codecs: caller_side,
                ..WorkerConfig::default()
            })
            .build();

        let effective = built.worker_config().payload_codecs.clone();
        assert!(
            !effective
                .registered_key_ids()
                .contains(&"caller-key".to_string()),
            "the caller's WorkerConfig registry must not survive build(); \
             configure codecs on the builder instead"
        );
    }

    /// Both worker-parts hops must thread the configured durable-log policy
    /// into the `HandlerRegistry` (issue #790).
    ///
    /// The registry's `workflow_log_policy` is the ONLY thing the executor
    /// consults to decide whether `ctx.log_*` pushes a `RecordLog` command, so a
    /// dropped hop turns the whole feature into a silent no-op for every
    /// deployment that configured it — the worker would run, the workflow would
    /// log to `tracing` exactly as before, and nothing would ever be persisted.
    /// Deleting either `.with_workflow_log_policy(...)` line must fail here.
    ///
    /// `into_worker_parts_with_extra_state` is covered too because it is the hop
    /// the plugin (and any app registering extra shared state) actually takes.
    #[cfg(feature = "db")]
    #[test]
    fn both_worker_parts_hops_thread_the_workflow_log_policy() {
        // Distinct from the defaults (1_000 / 4_096) so a hop that silently
        // substitutes `WorkflowLogPolicy::default()` also fails.
        let policy = crate::context::WorkflowLogPolicy::default()
            .with_max_lines(7)
            .with_max_message_bytes(64);

        let built = HarvestBuilder::new()
            .workflow_log_persistence(policy)
            .build();
        let (registry, _dags, _ws, _wc) = built.into_worker_parts();
        let threaded = registry
            .workflow_log_policy
            .expect("into_worker_parts must thread the configured log policy");
        assert_eq!(threaded.max_lines(), 7);
        assert_eq!(threaded.max_message_bytes(), 64);

        let built = HarvestBuilder::new()
            .workflow_log_persistence(policy)
            .build();
        let (registry, _dags, _ws, _wc) =
            built.into_worker_parts_with_extra_state(crate::context::SharedStateMap::new());
        let threaded = registry
            .workflow_log_policy
            .expect("into_worker_parts_with_extra_state must thread the configured log policy");
        assert_eq!(threaded.max_lines(), 7);
        assert_eq!(threaded.max_message_bytes(), 64);
    }

    /// AC6: with no policy configured the registry carries `None`, so the
    /// executor never even constructs the durable sink and `ctx.logger()` is
    /// byte-for-byte pre-#790.
    #[cfg(feature = "db")]
    #[test]
    fn worker_parts_carry_no_log_policy_unless_it_is_configured() {
        let (registry, _dags, _ws, _wc) = HarvestBuilder::new().build().into_worker_parts();
        assert!(
            registry.workflow_log_policy.is_none(),
            "the durable log sink must be OFF unless explicitly configured"
        );
    }

    /// The core worker build path (`into_worker_parts`) — which the standalone
    /// `HarvestRunner` worker funnels through via
    /// `into_worker_parts_with_extra_state` — must install the configured
    /// start-idempotency retention window into the process-global sweep static
    /// (issue #808, Codex P2). Without this, a split web/worker deployment
    /// running the sweep in a standalone runner would use the DEFAULT 24h window
    /// while the web app's reserve honors a custom window, deleting a claim the
    /// reserve still considers live and letting a same-key retry double-start.
    #[cfg(feature = "db")]
    #[test]
    fn into_worker_parts_installs_configured_start_idempotency_purge_window() {
        // Serialize against the sibling `purge_window_precision_and_clamping`
        // test, which mutates the same process-global static.
        let _guard = crate::start_idempotency::PURGE_WINDOW_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Start from a known baseline distinct from the custom value below.
        crate::start_idempotency::set_purge_window_secs(
            crate::start_idempotency::DEFAULT_START_IDEMPOTENCY_WINDOW,
        );

        // A custom window distinct from the 24h default and the sibling test's
        // values (7200s / 1.5s) so a stale/default read fails loudly.
        let custom = Duration::from_secs(3 * 24 * 3600);
        let built = HarvestBuilder::new()
            .start_idempotency_window(custom)
            .build();
        assert_eq!(built.start_idempotency_window, custom);

        let (_registry, _dags, _ws, _wc) = built.into_worker_parts();

        assert!(
            (crate::start_idempotency::purge_window_secs() - custom.as_secs_f64()).abs() < 1e-9,
            "into_worker_parts must install the configured start-idempotency \
             sweep window (expected {}s, got {}s)",
            custom.as_secs_f64(),
            crate::start_idempotency::purge_window_secs(),
        );

        // Restore the default so ordering-independent test runs don't leak state.
        crate::start_idempotency::set_purge_window_secs(
            crate::start_idempotency::DEFAULT_START_IDEMPOTENCY_WINDOW,
        );
    }

    #[test]
    #[should_panic(expected = "queue name cannot be empty")]
    fn worker_config_with_empty_queue_name_panics() {
        let _config = WorkerConfig::default().with_queues(["", "default"]);
    }

    #[test]
    fn worker_config_with_empty_iterator_clears_queues() {
        let config = WorkerConfig::default().with_queues(Vec::<&str>::new());
        assert!(config.queues.is_empty());
    }

    fn make_activity(
        name: &'static str,
        max_concurrent: Option<u32>,
        key: Option<&'static str>,
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
            max_concurrent,
            concurrency_key: key,
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
        }
    }

    fn make_local_activity(name: &'static str, start_to_close: Option<Duration>) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "test",
            default_retry_policy: None,
            default_start_to_close: start_to_close,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: true,
            max_input_bytes: None,
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

    #[test]
    fn builder_accepts_matching_concurrency_key_caps() {
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", Some(5), Some("stripe")),
                make_activity("act_b", Some(5), Some("stripe")),
            ])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_rejects_mismatched_concurrency_key_caps() {
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", Some(5), Some("stripe")),
                make_activity("act_b", Some(10), Some("stripe")),
            ])
            .try_build();
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            HarvestBuilderError::ConcurrencyKeyMismatch { ref key, .. } if key == "stripe"
        ));
        assert!(err.to_string().contains("stripe"));
    }

    #[test]
    fn builder_accepts_activities_without_concurrency_key() {
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", None, None),
                make_activity("act_b", Some(3), Some("sendgrid")),
            ])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_rejects_concurrency_key_without_cap() {
        // concurrency_key set but max_concurrent omitted — the cap predicate
        // would silently never fire (NULL cap bypasses the saturation check).
        let result = HarvestBuilder::new()
            .activities(vec![make_activity("act_a", None, Some("stripe"))])
            .try_build();
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            HarvestBuilderError::ConcurrencyKeyWithoutCap { ref activity, ref key }
                if activity == "act_a" && key == "stripe"
        ));
        assert!(err.to_string().contains("act_a"));
        assert!(err.to_string().contains("stripe"));
    }

    #[test]
    fn builder_rejects_implicit_key_cap_mismatch_with_explicit_key() {
        // act_a uses max_concurrent=5 with no key (implicit key = "act_a").
        // act_b explicitly declares key="act_a" with a different cap.
        // Both would resolve to the same effective key at runtime, so caps must agree.
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", Some(5), None),
                make_activity("act_b", Some(10), Some("act_a")),
            ])
            .try_build();
        let err = result.unwrap_err();
        assert!(
            matches!(err, HarvestBuilderError::ConcurrencyKeyMismatch { ref key, .. } if key == "act_a"),
            "expected ConcurrencyKeyMismatch for key 'act_a', got: {err}"
        );
    }

    #[test]
    fn builder_accepts_implicit_key_matching_explicit_key_same_cap() {
        // act_a: implicit key = "act_a", cap = 5
        // act_b: explicit key = "act_a", cap = 5 → same effective key and same cap → ok
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", Some(5), None),
                make_activity("act_b", Some(5), Some("act_a")),
            ])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_rejects_zero_concurrency_cap() {
        // max_concurrent = 0 makes the COUNT check always fail (0 running < 0 is
        // never true), permanently deferring every task for this activity.
        let result = HarvestBuilder::new()
            .activities(vec![make_activity("act_a", Some(0), Some("stripe"))])
            .try_build();
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            HarvestBuilderError::ZeroConcurrencyCap { ref activity }
                if activity == "act_a"
        ));
        assert!(err.to_string().contains("act_a"));
    }

    #[test]
    fn builder_rejects_zero_workflow_concurrency_limit() {
        use crate::concurrency::ConcurrencyPolicy;
        let result = HarvestBuilder::new()
            .workflows(vec![WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "report_wf",
                module: "test",
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: Some(ConcurrencyPolicy::new("input.tenant_id", 0)),

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
            }])
            .try_build();
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                HarvestBuilderError::ZeroWorkflowConcurrencyLimit { ref workflow }
                    if workflow == "report_wf"
            ),
            "expected ZeroWorkflowConcurrencyLimit, got: {err}"
        );
        assert!(err.to_string().contains("report_wf"));
    }

    /// Minimal `WorkflowInfo` carrying `throttle` and nothing else non-default,
    /// for the `validate_workflow_throttle_policies` tests below.
    fn throttled_wf_info(throttle: crate::throttle::ThrottlePolicy) -> WorkflowInfo {
        WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "report_wf",
            module: "test",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,
            debounce: None,
            batch: None,
            throttle: Some(throttle),
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }
    }

    #[test]
    fn builder_rejects_directly_constructed_throttle_policy_with_burst_below_one() {
        use crate::throttle::ThrottlePolicy;
        let result = HarvestBuilder::new()
            .workflows(vec![throttled_wf_info(ThrottlePolicy {
                refill_per_sec: 1.0,
                burst: 0.5,
                key_expr: None,
                schedule_to_start: None,
            })])
            .try_build();
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                HarvestBuilderError::InvalidWorkflowThrottlePolicy { ref workflow, .. }
                    if workflow == "report_wf"
            ),
            "expected InvalidWorkflowThrottlePolicy, got: {err}"
        );
        assert!(err.to_string().contains("burst"));
    }

    #[test]
    fn builder_rejects_directly_constructed_throttle_policy_with_infinite_burst() {
        use crate::throttle::ThrottlePolicy;
        let result = HarvestBuilder::new()
            .workflows(vec![throttled_wf_info(ThrottlePolicy {
                refill_per_sec: 1.0,
                burst: f64::INFINITY,
                key_expr: None,
                schedule_to_start: None,
            })])
            .try_build();
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                HarvestBuilderError::InvalidWorkflowThrottlePolicy { ref workflow, .. }
                    if workflow == "report_wf"
            ),
            "expected InvalidWorkflowThrottlePolicy, got: {err}"
        );
        assert!(err.to_string().contains("finite"));
    }

    #[test]
    fn builder_rejects_directly_constructed_throttle_policy_with_nonpositive_refill() {
        use crate::throttle::ThrottlePolicy;
        let result = HarvestBuilder::new()
            .workflows(vec![throttled_wf_info(ThrottlePolicy {
                refill_per_sec: 0.0,
                burst: 5.0,
                key_expr: None,
                schedule_to_start: None,
            })])
            .try_build();
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                HarvestBuilderError::InvalidWorkflowThrottlePolicy { ref workflow, .. }
                    if workflow == "report_wf"
            ),
            "expected InvalidWorkflowThrottlePolicy, got: {err}"
        );
        assert!(err.to_string().contains("refill_per_sec"));
    }

    #[test]
    fn builder_accepts_a_valid_directly_constructed_throttle_policy() {
        use crate::throttle::ThrottlePolicy;
        let result = HarvestBuilder::new()
            .workflows(vec![throttled_wf_info(ThrottlePolicy {
                refill_per_sec: 1.667,
                burst: 20.0,
                key_expr: Some("input.tenant_id"),
                schedule_to_start: None,
            })])
            .try_build();
        assert!(result.is_ok(), "expected Ok, got: {:?}", result.err());
    }

    #[test]
    fn builder_accepts_workflow_with_nonzero_concurrency_limit() {
        use crate::concurrency::ConcurrencyPolicy;
        let result = HarvestBuilder::new()
            .workflows(vec![WorkflowInfo {
                quota: None,
                declared_activities: None,
                declared_children: None,
                mcp: false,
                name: "report_wf",
                module: "test",
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
                execution_timeout: None,
                chain_execution_timeout: None,
                sla: None,
                concurrency: Some(ConcurrencyPolicy::new("input.tenant_id", 5)),

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
            }])
            .try_build();
        assert!(result.is_ok());
    }

    // ── Builder-level activity default floor (issue #620) ─────────────────
    //
    // RED PHASE: the `default_activity_retry_policy` / `default_activity_start_to_close`
    // fields and their `with_*` builder methods do not exist yet — this test
    // fails to COMPILE against the missing `WorkerConfig` symbols until the
    // green phase adds them. Both default to `None` so an unset config is
    // byte-for-byte identical to today (opt-in, AC1/AC6).

    #[test]
    fn worker_config_activity_defaults_default_to_none() {
        use crate::policy::RetryPolicy;

        let config = WorkerConfig::default();
        assert!(
            config.default_activity_retry_policy.is_none(),
            "default activity retry policy must be unset by default (opt-in)"
        );
        assert!(
            config.default_activity_start_to_close.is_none(),
            "default activity start_to_close must be unset by default (opt-in)"
        );

        // The two builder methods set the floors and are chainable.
        let configured = WorkerConfig::default()
            .with_default_activity_retry_policy(RetryPolicy::fixed(4, Duration::from_millis(50)))
            .with_default_activity_start_to_close(Duration::from_secs(300));
        assert_eq!(
            configured
                .default_activity_retry_policy
                .as_ref()
                .map(|p| p.max_attempts),
            Some(4),
        );
        assert_eq!(
            configured.default_activity_start_to_close,
            Some(Duration::from_secs(300)),
        );
    }

    // ── Retry-After ceiling (issue #744) ───────────────────────────────────
    //
    // RED PHASE: `WorkerConfig::retry_after_ceiling` / `with_retry_after_ceiling`
    // do not exist yet -- this test fails to COMPILE against the missing
    // symbols until the green phase adds them.

    #[test]
    fn worker_config_retry_after_ceiling_has_sane_default_and_is_configurable() {
        let config = WorkerConfig::default();
        assert_eq!(
            config.retry_after_ceiling,
            crate::builder::DEFAULT_RETRY_AFTER_CEILING,
            "the ceiling must always be present with a sane default (AC3)"
        );

        let configured = WorkerConfig::default().with_retry_after_ceiling(Duration::from_secs(90));
        assert_eq!(configured.retry_after_ceiling, Duration::from_secs(90));
    }

    // ── Local activity cap tests ──────────────────────────────────────────

    #[test]
    fn worker_config_max_local_activity_start_to_close_defaults_to_60s() {
        let config = WorkerConfig::default();
        assert_eq!(
            config.max_local_activity_start_to_close,
            Duration::from_secs(60)
        );
    }

    // ── Workflow-task timeout tests (issue #494) ──────────────────────────

    #[test]
    fn worker_config_workflow_task_timeout_defaults_to_10s() {
        assert_eq!(
            WorkerConfig::default().workflow_task_timeout,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn worker_config_with_workflow_task_timeout_overrides() {
        let config = WorkerConfig::default().with_workflow_task_timeout(Duration::from_secs(30));
        assert_eq!(config.workflow_task_timeout, Duration::from_secs(30));
    }

    #[test]
    fn worker_config_workflow_task_timeout_zero_disables() {
        let config = WorkerConfig::default().with_workflow_task_timeout(Duration::ZERO);
        assert_eq!(config.workflow_task_timeout, Duration::ZERO);
    }

    #[test]
    fn worker_config_poison_pill_threshold_defaults_to_3() {
        assert_eq!(WorkerConfig::default().poison_pill_threshold, 3);
    }

    #[test]
    fn worker_config_with_poison_pill_threshold_overrides() {
        let config = WorkerConfig::default().with_poison_pill_threshold(7);
        assert_eq!(config.poison_pill_threshold, 7);
    }

    #[test]
    fn worker_config_poison_pill_threshold_zero_disables() {
        let config = WorkerConfig::default().with_poison_pill_threshold(0);
        assert_eq!(config.poison_pill_threshold, 0);
    }

    // ── Capability-miss redelivery budget tests (issue #804) ──────────────

    #[test]
    fn worker_config_capability_miss_max_redeliveries_defaults_to_5() {
        // AC3 asks for a "configurable number ... (default small, e.g. 5)".
        assert_eq!(
            WorkerConfig::default().capability_miss_max_redeliveries,
            5,
            "the default redelivery budget must be small but large enough to \
             outlast a rolling pod flip"
        );
    }

    #[test]
    fn worker_config_with_capability_miss_max_redeliveries_overrides() {
        let config = WorkerConfig::default().with_capability_miss_max_redeliveries(12);
        assert_eq!(config.capability_miss_max_redeliveries, 12);
    }

    #[cfg(feature = "db")]
    #[test]
    fn worker_config_capability_miss_zero_escalates_immediately() {
        // `0` is NOT an off-switch for the feature: it means "escalate on the
        // first miss", i.e. the pre-#804 fail-fast behaviour. Pinned so nobody
        // later reinterprets 0 as "release forever" (which would let a
        // genuinely-unregistered type bounce indefinitely — the exact hazard
        // the budget exists to bound).
        let config = WorkerConfig::default().with_capability_miss_max_redeliveries(0);
        assert_eq!(config.capability_miss_max_redeliveries, 0);
        assert_eq!(
            crate::worker::capability_miss_decision(
                1,
                1,
                config.capability_miss_max_redeliveries,
                crate::worker::FleetCapabilityEvidence::AllLiveWorkersMissed,
            ),
            crate::worker::CapabilityMissAction::Escalate
        );
    }

    #[cfg(feature = "db")]
    #[test]
    fn worker_config_capability_miss_budget_is_threaded_into_the_runtime_config() {
        // The knob is inert unless it reaches WorkerRuntimeConfig — the struct
        // the worker's dispatch path actually reads. This is the same threading
        // contract `poison_pill_threshold` has.
        let config = WorkerConfig::default().with_capability_miss_max_redeliveries(9);
        let runtime: crate::worker::WorkerRuntimeConfig = config.into();
        assert_eq!(runtime.capability_miss_max_redeliveries, 9);
    }

    // ── Workflow panic-retry budget tests (issue #782) ────────────────────

    #[test]
    fn worker_config_workflow_panic_max_attempts_defaults_to_3() {
        assert_eq!(WorkerConfig::default().workflow_panic_max_attempts, 3);
    }

    #[test]
    fn worker_config_with_workflow_panic_max_attempts_overrides() {
        let config = WorkerConfig::default().with_workflow_panic_max_attempts(5);
        assert_eq!(config.workflow_panic_max_attempts, 5);
    }

    #[test]
    fn worker_config_workflow_panic_max_attempts_zero_is_terminal_on_first_panic() {
        let config = WorkerConfig::default().with_workflow_panic_max_attempts(0);
        assert_eq!(config.workflow_panic_max_attempts, 0);
    }

    // `WorkerRuntimeConfig` lives in the `db`-gated `worker` module, so this
    // threading assertion is only compiled under the `db` feature.
    #[cfg(feature = "db")]
    #[test]
    fn worker_runtime_config_threads_workflow_panic_max_attempts() {
        use crate::worker::WorkerRuntimeConfig;
        let cfg = WorkerConfig::default().with_workflow_panic_max_attempts(7);
        let runtime: WorkerRuntimeConfig = cfg.into();
        assert_eq!(runtime.workflow_panic_max_attempts, 7);
    }

    #[test]
    fn builder_accepts_local_activity_within_cap() {
        let result = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "compute_hash",
                Some(Duration::from_secs(30)),
            )])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_accepts_local_activity_with_no_start_to_close() {
        let result = HarvestBuilder::new()
            .activities(vec![make_local_activity("compute_hash", None)])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_rejects_local_activity_exceeding_cap() {
        let result = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "slow_local",
                Some(Duration::from_secs(120)),
            )])
            .try_build();
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                HarvestBuilderError::LocalActivityStartToCloseExceedsCap {
                    ref activity, ..
                } if activity == "slow_local"
            ),
            "expected LocalActivityStartToCloseExceedsCap, got {err}"
        );
        assert!(err.to_string().contains("slow_local"));
    }

    #[test]
    fn builder_rejects_local_activity_exactly_at_cap_boundary_when_exceeded() {
        // Exactly 60s is fine; 61s should fail.
        let at_cap = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "edge_case",
                Some(Duration::from_secs(60)),
            )])
            .try_build();
        assert!(at_cap.is_ok());

        let over_cap = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "edge_case",
                Some(Duration::from_secs(61)),
            )])
            .try_build();
        assert!(over_cap.is_err());
    }

    #[test]
    fn builder_accepts_custom_cap_that_fits_activity() {
        let worker = WorkerConfig {
            max_local_activity_start_to_close: Duration::from_secs(120),
            ..WorkerConfig::default()
        };
        let result = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "slow_local",
                Some(Duration::from_secs(90)),
            )])
            .worker(worker)
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn regular_activity_is_not_subject_to_local_cap() {
        // A regular activity with start_to_close > 60s should not be rejected
        // by the local activity cap validator.
        let result = HarvestBuilder::new()
            .activities(vec![ActivityInfo {
                name: "long_running",
                module: "test",
                default_retry_policy: None,
                default_start_to_close: Some(Duration::from_secs(300)),
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
            }])
            .try_build();
        assert!(result.is_ok());
    }

    // ── max_workflow_execution_timeout tests (issue #243) ─────────────────────

    #[test]
    fn builder_max_workflow_execution_timeout_defaults_to_none() {
        let built = HarvestBuilder::new().build();
        assert!(
            built.max_workflow_execution_timeout.is_none(),
            "default ceiling must be None"
        );
    }

    #[test]
    fn builder_max_workflow_execution_timeout_is_carried_through_build() {
        let ceiling = Duration::from_secs(86_400);
        let built = HarvestBuilder::new()
            .max_workflow_execution_timeout(ceiling)
            .build();
        assert_eq!(
            built.max_workflow_execution_timeout,
            Some(ceiling),
            "ceiling must survive build()"
        );
    }

    #[test]
    fn builder_max_workflow_execution_timeout_accessor_matches_field() {
        let ceiling = Duration::from_secs(3_600);
        let built = HarvestBuilder::new()
            .max_workflow_execution_timeout(ceiling)
            .build();
        assert_eq!(
            built.max_workflow_execution_timeout_ceiling(),
            Some(ceiling)
        );
    }

    // ── usage_window_ceiling tests (issue #596) ────────────────────────────────

    #[test]
    fn builder_usage_window_ceiling_defaults_to_ninety_days() {
        let built = HarvestBuilder::new().build();
        assert_eq!(
            built.usage_window_ceiling,
            crate::usage::default_usage_window_ceiling()
        );
    }

    #[test]
    fn builder_usage_window_ceiling_is_carried_through_build() {
        let ceiling = Duration::from_secs(3600);
        let built = HarvestBuilder::new().usage_window_ceiling(ceiling).build();
        assert_eq!(
            built.usage_window_ceiling, ceiling,
            "ceiling must survive build()"
        );
    }

    #[test]
    fn builder_usage_max_groups_defaults_to_ten_thousand() {
        let built = HarvestBuilder::new().build();
        assert_eq!(
            built.usage_max_groups,
            crate::usage::default_usage_max_groups()
        );
    }

    #[test]
    fn builder_usage_max_groups_is_carried_through_build() {
        let built = HarvestBuilder::new().usage_max_groups(42).build();
        assert_eq!(built.usage_max_groups, 42, "cap must survive build()");
    }

    // ── CronInTimezone builder validation ────────────────────────────────────

    #[test]
    fn builder_rejects_unknown_timezone_in_workflow_schedule() {
        let result = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .workflow_schedule(WorkflowSchedule::new(
                "test",
                Schedule::CronInTimezone {
                    expr: "0 9 * * *".to_string(),
                    tz: "Not/ATimezone".to_string(),
                },
            ))
            .try_build();

        let err = result.unwrap_err();
        assert!(
            matches!(&err, HarvestBuilderError::UnknownTimezone { name } if name == "Not/ATimezone"),
            "expected UnknownTimezone, got: {err:?}"
        );
    }

    #[test]
    fn builder_accepts_valid_timezone_in_workflow_schedule() {
        let result = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .workflow_schedule(WorkflowSchedule::new(
                "test",
                Schedule::CronInTimezone {
                    expr: "0 9 * * 1-5".to_string(),
                    tz: "America/Los_Angeles".to_string(),
                },
            ))
            .try_build();

        assert!(
            result.is_ok(),
            "valid timezone must be accepted: {result:?}"
        );
    }

    fn named_workflow_info(name: &'static str) -> WorkflowInfo {
        WorkflowInfo {
            name,
            ..fake_workflow_info()
        }
    }

    #[test]
    fn builder_rejects_all_writable_shards_on_plain_workflow_schedule() {
        // A plain (non-DAG, non-canary) workflow opting into all_writable_shards
        // must be rejected at build time — the fire path would mint executions
        // on the default shard (issue #796).
        let result = HarvestBuilder::new()
            .workflows(vec![named_workflow_info("nightly")])
            .workflow_schedule(
                WorkflowSchedule::new("nightly", Schedule::Interval(Duration::from_secs(60)))
                    .with_all_writable_shards(),
            )
            .try_build();

        let err = result.unwrap_err();
        assert!(
            matches!(
                &err,
                HarvestBuilderError::AllWritableShardsUnsupported { workflow_name }
                    if workflow_name == "nightly"
            ),
            "expected AllWritableShardsUnsupported, got: {err:?}"
        );
    }

    #[test]
    fn builder_accepts_all_writable_shards_on_canary_schedule() {
        // The built-in liveness canary (name starts with the reserved prefix)
        // is allowed to opt in.
        let result = HarvestBuilder::new()
            .workflows(vec![named_workflow_info("__harvest_canary_probe__default")])
            .workflow_schedule(
                WorkflowSchedule::new(
                    "__harvest_canary_probe__default",
                    Schedule::Interval(Duration::from_secs(30)),
                )
                .with_all_writable_shards(),
            )
            .try_build();

        assert!(
            result.is_ok(),
            "canary schedule with all_writable_shards must be accepted: {result:?}"
        );
    }

    #[test]
    fn builder_accepts_all_writable_shards_on_dag_schedule() {
        // A DAG schedule (dag_name Some) encodes its shard on the fire path, so
        // the flag is supported.
        let mut sched =
            WorkflowSchedule::new("daily_etl", Schedule::Interval(Duration::from_secs(60)))
                .with_all_writable_shards();
        sched.dag_name = Some("daily_etl".to_string());

        let result = HarvestBuilder::new()
            .workflows(vec![named_workflow_info("daily_etl")])
            .workflow_schedule(sched)
            .try_build();

        assert!(
            result.is_ok(),
            "DAG schedule with all_writable_shards must be accepted: {result:?}"
        );
    }

    #[test]
    fn builder_accepts_plain_workflow_schedule_without_the_flag() {
        // Default behaviour is unchanged: a plain schedule without the flag
        // builds fine.
        let result = HarvestBuilder::new()
            .workflows(vec![named_workflow_info("nightly")])
            .workflow_schedule(WorkflowSchedule::new(
                "nightly",
                Schedule::Interval(Duration::from_secs(60)),
            ))
            .try_build();

        assert!(
            result.is_ok(),
            "plain schedule without the flag must be accepted: {result:?}"
        );
    }

    #[test]
    fn builder_rejects_unknown_timezone_in_dag_schedule() {
        fn build(_dag: &mut DagBuilder) {}
        let dag = DagInfo {
            name: "daily_etl",
            module: "test",
            schedule: Some(Schedule::CronInTimezone {
                expr: "0 9 * * 1-5".to_string(),
                tz: "Not/ATimezone".to_string(),
            }),
            catchup: false,
            max_active_runs: 1,
            default_queue: None,
            builder: build,
            workflow_handler: None,
            jitter: std::time::Duration::ZERO,
            overlap_policy: crate::policy::OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
            execution_timeout: None,
            sla: None,
        };
        let result = HarvestBuilder::new().dags(vec![dag]).try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::UnknownTimezone { ref name }) if name == "Not/ATimezone"
            ),
            "unknown timezone in DAG schedule must be rejected at build time: {result:?}"
        );
    }

    #[test]
    fn builder_accepts_valid_timezone_in_dag_schedule() {
        fn build(_dag: &mut DagBuilder) {}
        let dag = DagInfo {
            name: "daily_etl",
            module: "test",
            schedule: Some(Schedule::CronInTimezone {
                expr: "0 9 * * 1-5".to_string(),
                tz: "America/New_York".to_string(),
            }),
            catchup: false,
            max_active_runs: 1,
            default_queue: None,
            builder: build,
            workflow_handler: None,
            jitter: std::time::Duration::ZERO,
            overlap_policy: crate::policy::OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
            mcp: false,
            execution_timeout: None,
            sla: None,
        };
        let result = HarvestBuilder::new().dags(vec![dag]).try_build();
        assert!(
            result.is_ok(),
            "valid timezone in DAG schedule must be accepted: {result:?}"
        );
    }

    #[test]
    fn builder_rejects_disagreeing_rate_limits_on_same_key() {
        let act1 = ActivityInfo {
            name: "act1",
            module: "test",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: Some(10.0),
            rate_limit_burst: Some(5.0),
            rate_limit_key: Some("stripe"),
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };
        let act2 = ActivityInfo {
            name: "act2",
            module: "test",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: Some(20.0), // mismatched rps!
            rate_limit_burst: Some(5.0),
            rate_limit_key: Some("stripe"),
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };

        let result = HarvestBuilder::new()
            .activities(vec![act1, act2])
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::RateLimitKeyMismatch { ref key, .. }) if key == "stripe"
            ),
            "expected RateLimitKeyMismatch error, got: {result:?}"
        );
    }

    #[test]
    fn builder_rejects_rate_limit_key_without_cap() {
        let act = ActivityInfo {
            name: "act1",
            module: "test",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None, // Missing RPS!
            rate_limit_burst: None,
            rate_limit_key: Some("stripe"),
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };

        let result = HarvestBuilder::new().activities(vec![act]).try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::RateLimitKeyWithoutCap { ref activity, ref key })
                    if activity == "act1" && key == "stripe"
            ),
            "expected RateLimitKeyWithoutCap error, got: {result:?}"
        );
    }

    #[test]
    fn builder_rejects_static_rate_limit_key_with_reserved_dyn_rate_prefix() {
        // A static `rate_limit_key` must not squat the `dyn-rate:` namespace
        // reserved for per-key/dynamic buckets (issue #699 review, Codex P2) —
        // it could collide first-writer-wins with a generated dynamic bucket.
        let act = ActivityInfo {
            name: "act1",
            module: "test",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: Some(50.0),
            rate_limit_burst: None,
            rate_limit_key: Some("dyn-rate:9:tenant_id:acme"),
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };
        let result = HarvestBuilder::new().activities(vec![act]).try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::RateLimitKeyReservedPrefix { ref activity, ref key })
                    if activity == "act1" && key == "dyn-rate:9:tenant_id:acme"
            ),
            "expected RateLimitKeyReservedPrefix, got: {result:?}"
        );
    }

    // ── Dynamic per-key rate limits (issue #699) ──────────────────────────────

    /// Build a bare `ActivityInfo` for the dynamic-per-key rate-limit tests.
    fn rate_activity(
        name: &'static str,
        rps: Option<f64>,
        burst: Option<f64>,
        key: Option<&'static str>,
        key_expr: Option<&'static str>,
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
            rate_limit_rps: rps,
            rate_limit_burst: burst,
            rate_limit_key: key,
            rate_limit_key_expr: key_expr,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    #[test]
    fn builder_rejects_dynamic_rate_limit_key_without_rps() {
        let act = rate_activity("charge", None, None, None, Some("input.tenant_id"));
        let result = HarvestBuilder::new().activities(vec![act]).try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::RateLimitKeyExprWithoutCap { ref activity, ref key })
                    if activity == "charge" && key == "input.tenant_id"
            ),
            "expected RateLimitKeyExprWithoutCap for dynamic key, got: {result:?}"
        );
        // The dynamic-form message names `rate_limit(...)` and `rps`, not the
        // static `rate_limit_key` wording (issue #699 review, #11).
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("rate_limit(key"),
            "message should name rate_limit(key = ...): {msg}"
        );
        assert!(msg.contains("rps"), "message should mention rps: {msg}");
        assert!(
            !msg.contains("rate_limit_key ="),
            "dynamic message must not use the static rate_limit_key wording: {msg}"
        );
    }

    #[test]
    fn builder_rejects_same_dynamic_key_expr_with_different_rps() {
        let a = rate_activity("charge", Some(10.0), None, None, Some("input.tenant_id"));
        let b = rate_activity("refund", Some(20.0), None, None, Some("input.tenant_id"));
        let result = HarvestBuilder::new().activities(vec![a, b]).try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::RateLimitKeyMismatch { ref key, .. })
                    if key == "input.tenant_id"
            ),
            "expected RateLimitKeyMismatch for dynamic key_expr, got: {result:?}"
        );
    }

    #[test]
    fn builder_normalizes_input_prefix_so_both_spellings_share_validation() {
        // `key = "input.tenant_id"` and `key = "tenant_id"` resolve the same
        // field and share one bucket, so a mismatched rps across the two spellings
        // must be caught by the mismatch guard (issue #699 review, #6).
        let a = rate_activity("charge", Some(10.0), None, None, Some("input.tenant_id"));
        let b = rate_activity("refund", Some(20.0), None, None, Some("tenant_id"));
        let result = HarvestBuilder::new().activities(vec![a, b]).try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::RateLimitKeyMismatch { .. })
            ),
            "the two `input.` spellings must be validated together, got: {result:?}"
        );

        // Same field, same rps across both spellings → OK, and they produce the
        // SAME normalized bucket key.
        let c = rate_activity("a", Some(10.0), None, None, Some("input.tenant_id"));
        let d = rate_activity("b", Some(10.0), None, None, Some("tenant_id"));
        assert!(
            HarvestBuilder::new()
                .activities(vec![c, d])
                .try_build()
                .is_ok(),
            "matching rps across both spellings must build"
        );
        // The bucket-key equality is proven directly against the (db-gated) queue
        // helper. The builder-validation half above runs on every feature set.
        #[cfg(feature = "db")]
        assert_eq!(
            crate::queue::dynamic_rate_bucket_key("input.tenant_id", Some("acme")),
            crate::queue::dynamic_rate_bucket_key("tenant_id", Some("acme")),
            "both spellings must share one bucket key"
        );
    }

    #[test]
    fn builder_accepts_matching_dynamic_key_expr_and_distinct_exprs() {
        // Same expr + same rps → OK; distinct exprs → OK; a static key alongside
        // is independent and does not collide.
        let a = rate_activity(
            "charge",
            Some(10.0),
            Some(20.0),
            None,
            Some("input.tenant_id"),
        );
        let b = rate_activity(
            "refund",
            Some(10.0),
            Some(20.0),
            None,
            Some("input.tenant_id"),
        );
        let c = rate_activity("notify", Some(5.0), None, None, Some("input.org"));
        let d = rate_activity("email", Some(3.0), None, Some("input.tenant_id"), None);
        let result = HarvestBuilder::new()
            .activities(vec![a, b, c, d])
            .try_build();
        assert!(
            result.is_ok(),
            "expected matching/distinct dynamic rate limits to build, got: {:?}",
            result.err()
        );
    }

    #[test]
    fn builder_static_and_dynamic_key_maps_are_independent() {
        // A static key "tenant" and a dynamic key_expr "tenant" (same string) must
        // NOT be conflated — they live in separate validation maps and namespaced
        // buckets, so mismatched rps across the two is not an error.
        let stat = rate_activity("s", Some(10.0), None, Some("tenant"), None);
        let dyn_ = rate_activity("d", Some(99.0), None, None, Some("tenant"));
        let result = HarvestBuilder::new()
            .activities(vec![stat, dyn_])
            .try_build();
        assert!(
            result.is_ok(),
            "static and dynamic keys sharing a string must be independent, got: {:?}",
            result.err()
        );
    }

    // ── BatchStartConfig (issue #357) ─────────────────────────────────────────

    #[test]
    fn built_harvest_batch_start_config_defaults_to_spec_values() {
        use crate::batch_start::{DEFAULT_BATCH_START_MAX_BYTES, DEFAULT_BATCH_START_MAX_ITEMS};
        let built = HarvestBuilder::new().build();
        assert_eq!(
            built.batch_start_config.max_items_per_batch,
            DEFAULT_BATCH_START_MAX_ITEMS
        );
        assert_eq!(
            built.batch_start_config.max_total_bytes,
            DEFAULT_BATCH_START_MAX_BYTES
        );
    }

    #[test]
    fn harvest_builder_batch_start_config_overrides_are_propagated() {
        use crate::batch_start::BatchStartConfig;
        let custom = BatchStartConfig {
            max_items_per_batch: 500,
            max_total_bytes: 5 * 1024 * 1024,
        };
        let built = HarvestBuilder::new()
            .batch_start_config(custom.clone())
            .build();
        assert_eq!(built.batch_start_config, custom);
    }

    #[test]
    fn harvest_builder_validates_static_completion_triggers() {
        use crate::completion_trigger::CompletionTrigger;

        // Both source and target registered -> Ok
        let trigger = CompletionTrigger::new("test", "test");
        let result = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .completion_triggers(vec![trigger])
            .try_build();
        assert!(
            result.is_ok(),
            "Expected builder success with registered workflows, got: {result:?}"
        );

        // Unknown source -> Error
        let trigger_bad_source = CompletionTrigger::new("unknown_source", "test");
        let result = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .completion_triggers(vec![trigger_bad_source])
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::UnknownCompletionTriggerWorkflow {
                    ref workflow_name,
                    role,
                    ..
                }) if workflow_name == "unknown_source" && role == "source"
            ),
            "Expected UnknownCompletionTriggerWorkflow error for source, got: {result:?}"
        );

        // Unknown target -> Error
        let trigger_bad_target = CompletionTrigger::new("test", "unknown_target");
        let result = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .completion_triggers(vec![trigger_bad_target])
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::UnknownCompletionTriggerWorkflow {
                    ref workflow_name,
                    role,
                    ..
                }) if workflow_name == "unknown_target" && role == "target"
            ),
            "Expected UnknownCompletionTriggerWorkflow error for target, got: {result:?}"
        );
    }

    #[test]
    fn validate_retention_overrides_registration() {
        use crate::retention::RetentionConfig;
        use std::time::Duration;

        let workflows = vec![fake_workflow_info()]; // registered name: "test"
        let dags = vec!["my_dag".to_string()];

        // Unknown override name -> Err
        let cfg = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("typo_wf", Duration::from_secs(60));
        let result = validate_retention_overrides(&cfg, &workflows, &dags);
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::UnknownRetentionOverrideWorkflow { ref workflow_name, ref registered })
                    if workflow_name == "typo_wf" && registered.contains(&"test".to_string())
            ),
            "Expected UnknownRetentionOverrideWorkflow naming the unknown type and listing registered, got: {result:?}"
        );

        // Known registered workflow name -> Ok
        let cfg = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("test", Duration::from_secs(60));
        assert!(validate_retention_overrides(&cfg, &workflows, &dags).is_ok());

        // Known auto-registered DAG workflow name -> Ok
        let cfg = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("my_dag", Duration::from_secs(60));
        assert!(validate_retention_overrides(&cfg, &workflows, &dags).is_ok());

        // Empty overrides -> Ok
        let cfg = RetentionConfig::with_max_age(Duration::from_secs(3600));
        assert!(validate_retention_overrides(&cfg, &workflows, &dags).is_ok());
    }

    #[test]
    fn harvest_builder_rejects_unknown_retention_override() {
        use crate::retention::RetentionConfig;
        use std::time::Duration;

        let cfg = RetentionConfig::with_max_age(Duration::from_secs(3600))
            .with_workflow_override("nope", Duration::from_secs(60));
        let result = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .retention(cfg)
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::UnknownRetentionOverrideWorkflow { ref workflow_name, .. })
                    if workflow_name == "nope"
            ),
            "Expected UnknownRetentionOverrideWorkflow naming the unknown type, got: {result:?}"
        );
    }

    #[test]
    fn harvest_builder_rejects_invalid_trigger_condition() {
        use crate::completion_trigger::{CompletionTrigger, TriggerCondition};

        // A structurally valid condition passes try_build.
        let ok_trigger =
            CompletionTrigger::new("test", "test").with_condition(TriggerCondition::Eq {
                path: "region".into(),
                value: serde_json::json!("EU"),
            });
        let result = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .completion_trigger(ok_trigger)
            .try_build();
        assert!(
            result.is_ok(),
            "Expected builder success with a valid condition, got: {result:?}"
        );

        // A condition over the boundedness caps fails try_build (issue #810).
        let mut over_deep = TriggerCondition::Exists { path: "a".into() };
        for _ in 0..crate::completion_trigger::MAX_CONDITION_DEPTH {
            over_deep = TriggerCondition::All(vec![over_deep]);
        }
        let trigger_id = uuid::Uuid::new_v4();
        let bad_trigger = CompletionTrigger::new("test", "test")
            .with_id(trigger_id)
            .with_condition(over_deep);
        let result = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .completion_trigger(bad_trigger)
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::InvalidCompletionTriggerCondition {
                    trigger_id: id,
                    ..
                }) if id == trigger_id
            ),
            "Expected InvalidCompletionTriggerCondition, got: {result:?}"
        );

        // A malformed dotted path fails try_build too.
        let bad_path =
            CompletionTrigger::new("test", "test").with_condition(TriggerCondition::Exists {
                path: "a..b".into(),
            });
        let result = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .completion_trigger(bad_path)
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::InvalidCompletionTriggerCondition { .. })
            ),
            "Expected InvalidCompletionTriggerCondition for malformed path, got: {result:?}"
        );
    }

    // --- AC3 / AC5 / AC6: max_workflow_history_events builder tests (issue #493) ---

    #[test]
    fn builder_max_workflow_history_events_defaults_to_none() {
        let built = HarvestBuilder::new().build();
        assert_eq!(
            built.max_workflow_history_events, None,
            "ceiling must default to None (no ceiling enforced)"
        );
    }

    #[test]
    fn builder_max_workflow_history_events_is_carried_through_build() {
        // Default soft threshold is 10_000; ceiling must be strictly greater.
        let built = HarvestBuilder::new()
            .max_workflow_history_events(Some(50_000))
            .build();
        assert_eq!(built.max_workflow_history_events, Some(50_000));
    }

    #[test]
    fn builder_max_workflow_history_events_none_clears_ceiling() {
        let built = HarvestBuilder::new()
            .max_workflow_history_events(Some(50_000))
            .max_workflow_history_events(None)
            .build();
        assert_eq!(built.max_workflow_history_events, None);
    }

    #[test]
    fn builder_max_workflow_history_events_must_be_strictly_greater_than_soft_threshold() {
        // Default soft threshold is 10_000; a ceiling of 9_999 must fail.
        let result = HarvestBuilder::new()
            .max_workflow_history_events(Some(9_999))
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::HistoryCeilingBelowSoftThreshold {
                    ceiling: 9_999,
                    threshold: 10_000,
                })
            ),
            "expected HistoryCeilingBelowSoftThreshold but got {result:?}"
        );
    }

    #[test]
    fn builder_max_workflow_history_events_exactly_equal_to_threshold_fails() {
        // Ceiling == soft threshold must also fail (strictly-greater required).
        let result = HarvestBuilder::new()
            .max_workflow_history_events(Some(10_000))
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::HistoryCeilingBelowSoftThreshold {
                    ceiling: 10_000,
                    threshold: 10_000,
                })
            ),
            "expected HistoryCeilingBelowSoftThreshold for equal values but got {result:?}"
        );
    }

    #[test]
    fn builder_max_workflow_history_events_error_message_is_informative() {
        let err = HarvestBuilder::new()
            .max_workflow_history_events(Some(5_000))
            .try_build()
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("5000"),
            "error message should contain ceiling: {msg}"
        );
        assert!(
            msg.contains("10000"),
            "error message should contain threshold: {msg}"
        );
    }

    #[test]
    fn builder_ceiling_above_custom_soft_threshold_passes() {
        // With a custom soft threshold of 500, a ceiling of 501 must succeed.
        let built = HarvestBuilder::new()
            .history_continue_as_new_threshold(500)
            .max_workflow_history_events(Some(501))
            .build();
        assert_eq!(built.max_workflow_history_events, Some(501));
    }

    #[test]
    fn builder_ceiling_equal_to_custom_soft_threshold_fails() {
        let result = HarvestBuilder::new()
            .history_continue_as_new_threshold(500)
            .max_workflow_history_events(Some(500))
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::HistoryCeilingBelowSoftThreshold {
                    ceiling: 500,
                    threshold: 500,
                })
            ),
            "expected failure when ceiling == custom threshold, got {result:?}"
        );
    }

    #[test]
    fn builder_completion_callback_default_target_is_stored() {
        use crate::completion_callback::{EventFilter, HostAllowlist};
        let built = HarvestBuilder::new()
            .completion_callback_allowlist(HostAllowlist::new().with_pattern("api.example.com"))
            .completion_callback_default("https://api.example.com/hook", EventFilter::AnyTerminal)
            .build();
        assert_eq!(built.completion_callback_config().default_targets.len(), 1);
        assert_eq!(
            built.completion_callback_config().default_targets[0].url,
            "https://api.example.com/hook"
        );
    }

    #[test]
    fn builder_rejects_a_non_allowlisted_completion_callback_default_target() {
        use crate::completion_callback::EventFilter;
        // No allowlist configured -> every domain host is rejected.
        let result = HarvestBuilder::new()
            .completion_callback_default("https://evil.com/hook", EventFilter::AnyTerminal)
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::CallbackTargetRejected { ref url, .. })
                    if url == "https://evil.com/hook"
            ),
            "expected CallbackTargetRejected, got {result:?}"
        );
    }

    #[test]
    fn builder_with_no_completion_callback_config_has_empty_defaults() {
        // Identical-behavior guarantee: an embedder who never touches the
        // completion-callback API gets an empty default-target list.
        let built = HarvestBuilder::new().build();
        assert!(
            built
                .completion_callback_config()
                .default_targets
                .is_empty()
        );
        assert!(built.completion_callback_config().deliverer.is_none());
    }

    #[test]
    fn builder_completion_callback_secret_and_retry_policy_are_stored() {
        use crate::policy::RetryPolicy;
        use std::time::Duration;
        let built = HarvestBuilder::new()
            .completion_callback_secret(b"shh".to_vec())
            .completion_callback_retry_policy(RetryPolicy::fixed(5, Duration::from_secs(2)))
            .build();
        assert!(built.completion_callback_config().secret.is_some());
        assert_eq!(
            built.completion_callback_config().retry_policy.max_attempts,
            5
        );
    }

    #[cfg(feature = "wasm-activities")]
    #[test]
    fn native_activity_shadowing_a_wasm_binding_is_rejected() {
        use crate::wasm_store::WasmActivityRegistration;

        // A native activity registered with the same name as a WASM activity is
        // ambiguous: the native handler wins in the registry while the WASM
        // binding lingers. try_build must reject it rather than silently run the
        // guest under native metadata.
        let result = HarvestBuilder::new()
            .wasm_activity(WasmActivityRegistration::new("checksum", vec![1, 2, 3]))
            .activities(vec![make_activity("checksum", None, None)])
            .try_build();
        assert!(
            matches!(
                result,
                Err(HarvestBuilderError::WasmActivityNameCollision { ref activity })
                    if activity == "checksum"
            ),
            "expected WasmActivityNameCollision, got {result:?}"
        );

        // A WASM activity with no native shadow builds fine.
        assert!(
            HarvestBuilder::new()
                .wasm_activity(WasmActivityRegistration::new("checksum", vec![1, 2, 3]))
                .try_build()
                .is_ok(),
            "a lone WASM activity must build"
        );
    }

    #[cfg(feature = "wasm-activities")]
    #[test]
    fn duplicate_wasm_activity_registration_is_last_wins() {
        use crate::policy::RetryPolicy;
        use crate::wasm_store::WasmActivityRegistration;
        use std::time::Duration;

        let builder = HarvestBuilder::new()
            .wasm_activity(
                WasmActivityRegistration::new("checksum", vec![1, 1, 1])
                    .with_queue("first")
                    .with_retry(RetryPolicy::fixed(2, Duration::from_millis(10))),
            )
            .wasm_activity(
                WasmActivityRegistration::new("checksum", vec![2, 2, 2])
                    .with_queue("second")
                    .with_retry(RetryPolicy::fixed(7, Duration::from_millis(20))),
            );

        // Exactly one registration entry for the name, carrying the LATER bytes.
        let regs: Vec<&(String, Vec<u8>)> = builder
            .wasm_module_registrations
            .iter()
            .filter(|(n, _)| n == "checksum")
            .collect();
        assert_eq!(regs.len(), 1, "duplicate name must not keep both blobs");
        assert_eq!(regs[0].1, vec![2, 2, 2], "must retain the later bytes");
    }
}
