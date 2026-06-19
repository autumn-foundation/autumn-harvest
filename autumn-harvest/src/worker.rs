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
    apply_parent_close_cascade, cancel_workflow_execution_collect, parent_close_cascade_event_count,
};
use crate::executor::{
    WorkflowExecuteSpanMeta, WorkflowOutcome, run_workflow_with_state_history_policy_and_caps,
};
use crate::external_task;
use crate::failure::{parse_error_payload, parse_error_payload_full, parse_typed_payload};
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
    ATTR_WORKFLOW_ID, ActivityStatus, TraceContextCarrier, WorkflowStatus,
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
    /// Optional Postgres URL for LISTEN/NOTIFY wakeups.
    pub notification_database_url: Option<String>,
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
            notification_database_url: cfg.notification_database_url,
            max_concurrent_workflows: cfg.max_concurrent_workflows,
            max_concurrent_activities: cfg.max_concurrent_activities,
            poll_interval: Duration::from_millis(500),
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
            max_workflow_pause_duration: cfg.max_workflow_pause_duration,
            labels: cfg.labels,
            #[cfg(feature = "db")]
            sharded_pool: cfg.sharded_pool,
            max_workflow_history_events: cfg.max_workflow_history_events,
        }
    }
}

// ---------------------------------------------------------------------------
// HandlerRegistry
// ---------------------------------------------------------------------------

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
}

impl HandlerRegistry {
    /// Create a new registry, indexing handlers by their `name` field.
    #[must_use]
    pub fn new(workflows: Vec<WorkflowInfo>, activities: Vec<ActivityInfo>) -> Self {
        Self::with_state(workflows, activities, empty_shared_state())
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
                        },
                    )
                })
                .collect();
            *lock = Some(metadata);
        }
        let activities: HashMap<String, ActivityInfo> = activities
            .into_iter()
            .map(|a| (a.name.to_string(), a))
            .collect();
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
        }
    }

    /// Set declarative query and update handlers (issue #346).
    #[must_use]
    pub fn with_handler_infos(
        mut self,
        query_handlers: Vec<QueryHandlerInfo>,
        update_handlers: Vec<UpdateHandlerInfo>,
    ) -> Self {
        self.query_handlers = query_handlers;
        self.update_handlers = update_handlers;
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
}

impl std::fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerRegistry")
            .field("workflows", &self.workflows.keys())
            .field("activities", &self.activities.keys())
            .field("query_handler_count", &self.query_handlers.len())
            .field("update_handler_count", &self.update_handlers.len())
            .field("state_count", &self.state.len())
            .field("telemetry", &self.telemetry)
            .field("history_policy", &self.history_policy)
            .field("max_activity_input_bytes", &self.max_activity_input_bytes)
            .field("max_workflow_input_bytes", &self.max_workflow_input_bytes)
            .field("max_activity_result_bytes", &self.max_activity_result_bytes)
            .field("max_signal_payload_bytes", &self.max_signal_payload_bytes)
            .field("max_current_details_bytes", &self.max_current_details_bytes)
            .field("circuit_breakers", &self.circuit_breakers)
            .finish()
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
        WorkflowCommand::SignalExternalWorkflow { .. } => "SignalExternalWorkflow",
        WorkflowCommand::RequestCancelExternalWorkflow { .. } => "RequestCancelExternalWorkflow",
        WorkflowCommand::SpawnDetachedChildWorkflow { .. } => "SpawnDetachedChildWorkflow",
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
                | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
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
                    | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
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
    mut branch_event: F,
) -> Vec<WorkflowEvent>
where
    F: FnMut(&WorkflowCommand) -> Option<WorkflowEvent>,
{
    commands
        .iter()
        .filter_map(|cmd| match cmd {
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
            other => branch_event(other),
        })
        .collect()
}

/// Collects `MarkerRecorded` and `ChildWorkflowSpawnedDetached` events in command
/// emission order for suspension paths that have no branch-specific events
/// (signal-wait park, activity-wait park).
fn pre_suspension_events_from_commands(commands: &[WorkflowCommand]) -> Vec<WorkflowEvent> {
    build_suspension_events(commands, |_| None)
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
                | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
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
            | WorkflowCommand::SpawnDetachedChildWorkflow { .. } => {}
            WorkflowCommand::ScheduleActivity {
                activity_id,
                name,
                input,
                queue,
                retry_policy_override,
                start_to_close_override,
                ..
            } => {
                scheduled.push(ScheduledActivityCommand {
                    activity_id: *activity_id,
                    name: name.clone(),
                    input: input.clone(),
                    queue: queue.clone(),
                    retry_policy_override: retry_policy_override.clone(),
                    start_to_close_override: *start_to_close_override,
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
            | WorkflowCommand::SpawnDetachedChildWorkflow { .. } => {}
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
                | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
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
                    | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
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
    start_to_close_secs: Option<u64>,
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
                start_to_close_secs,
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
                    start_to_close_secs,
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
            } => {
                drop(result_tx);
                items.push(SignalBatchItem::Signal(SignalExternalWorkflowRun {
                    signal_id,
                    target,
                    signal_name,
                    payload,
                    already_requested,
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
            } => {
                drop(result_tx);
                signal_items.push(SignalBatchItem::Signal(SignalExternalWorkflowRun {
                    signal_id,
                    target,
                    signal_name,
                    payload,
                    already_requested,
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
            | WorkflowCommand::SetCurrentDetails { .. } => {}
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
/// # At-least-once delivery on crash recovery
///
/// When `run.already_requested` is `true` the worker crashed after writing
/// `ExternalSignalRequested` but before the terminal event. Re-calling
/// `send_signal` may insert a duplicate row into `harvest_signals` if the
/// original insert committed before the crash. Exact-once delivery requires
/// storing the `signal_id` as a unique key on `harvest_signals`; that schema
/// change is deferred to a follow-up migration.
///
/// Returned tuple: (new history events, next event id, deferred trigger starts
/// to spawn after commit, (`workflow_name`, `queue_name`) of targets newly
/// cancelled inline for terminal metrics).
type InlinePersistResult = (
    Vec<WorkflowEvent>,
    i32,
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(String, String)>,
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
    let (new_events, final_next, deferred_starts, cancel_metrics): InlinePersistResult = conn
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

                            // Same-shard delivery attempt
                            let terminal_opt = match signal::send_signal(
                                conn,
                                run.target,
                                &run.signal_name,
                                run.payload,
                            )
                            .await
                            {
                                Ok(()) => Some(WorkflowEvent::ExternalSignalDelivered {
                                    signal_id: run.signal_id,
                                }),
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
                                Ok((cancelled, deferred)) => {
                                    deferred_starts.extend(deferred);
                                    if cancelled.newly_cancelled {
                                        cancel_metrics
                                            .push((cancelled.workflow_name, cancelled.queue_name));
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

                Ok((new_events, next, deferred_starts, cancel_metrics))
            }
            .scope_boxed()
        })
        .await?;

    // The inline batch is durably committed: now spawn trigger/cascade follow-up
    // starts and record terminal metrics for any targets cancelled above.
    for start in deferred_starts {
        start.spawn();
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
    if activity.default_schedule_to_close.is_some() {
        return Err(HarvestError::Config(format!(
            "activity '{}' is local but has schedule_to_close set; \
             local activities do not support schedule_to_close (use start_to_close instead)",
            run.name
        )));
    }
    let history_event_hard_cap = registry.history_policy().event_hard_cap();

    let per_attempt_timeout = run
        .start_to_close_secs
        .map_or(max_start_to_close, Duration::from_secs)
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

    for attempt in start_attempt..=max_attempts {
        let ctx =
            ActivityContext::new_local_activity(registry.shared_state(), CancellationToken::new())
                .with_context_headers(std::sync::Arc::clone(&context_headers))
                .with_idempotency_key(local_idempotency_key.clone())
                .with_attempt(attempt)
                .with_max_attempts(max_attempts)
                .with_previous_failure(previous_failure.clone());
        let result = tokio::time::timeout(per_attempt_timeout, (handler)(&ctx, run.input.clone()))
            .await
            .unwrap_or_else(|_| {
                Err(format!(
                    "local activity '{}' timed out after {:?}",
                    run.name, per_attempt_timeout
                ))
            });

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

fn configured_retry_policy(task: &TaskQueueItem) -> HarvestResult<Option<RetryPolicy>> {
    task.retry_policy
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .map_err(HarvestError::from)
}

fn task_attempt(task: &TaskQueueItem) -> u32 {
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

/// Whether `error` is non-retryable under the same rules `next_retry_delay`
/// applies: the typed-payload `non_retryable` flag *and* the retry policy's
/// `non_retryable_errors` list (which also matches legacy `Err(String)` values
/// the typed flag never sees). The circuit breaker reuses this so a burst of
/// permanent failures — typed or legacy — never trips the circuit.
fn failure_is_non_retryable(error: &str, retry_policy: Option<&RetryPolicy>) -> bool {
    let typed = parse_typed_payload(error);
    if typed.as_ref().is_some_and(|f| f.non_retryable) {
        return true;
    }
    if let Some(policy) = retry_policy {
        let typed_error_type = typed.as_ref().map(|f| f.error_type.as_str());
        return policy.is_non_retryable(typed_error_type, error);
    }
    false
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
    harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .for_update()
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

    store::load_history(conn, exec_id).await
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
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(workflow_execution_transition_error(conn, exec_id).await?);
    }

    Ok(())
}

async fn update_workflow_execution_failed(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    worker_id: &str,
    error: &str,
    nd_details: Option<&crate::error::NonDeterministicDetails>,
) -> HarvestResult<()> {
    use crate::schema::harvest_workflow_executions::dsl;

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
    ))
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;

    if updated == 0 {
        return Err(workflow_execution_transition_error(conn, exec_id).await?);
    }

    if let Some(details) = nd_details {
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
        crate::store::update_search_attrs(conn, exec_id, &patch).await?;
    }

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

async fn persist_workflow_completion(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    output: serde_json::Value,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> HarvestResult<()> {
    let event = WorkflowEvent::WorkflowCompleted {
        output: output.clone(),
    };
    let deferred = conn
        .transaction::<Vec<crate::completion_trigger::DeferredTriggerStart>, HarvestError, _>(
            |conn| {
                async move {
                    store::append_events(conn, exec_id, &[event], next_event_id).await?;
                    update_workflow_execution_completed(conn, exec_id, worker_id, &output).await?;
                    queue::complete_task(conn, task_id, output).await?;
                    let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
                    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                        conn,
                        exec_id,
                        crate::completion_trigger::TerminalState::Completed,
                        metrics,
                    )
                    .await?;
                    deferred.extend(triggers);
                    Ok(deferred)
                }
                .scope_boxed()
            },
        )
        .await?;

    for start in deferred {
        start.spawn();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn persist_workflow_failure(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    error: &str,
    nd_details: Option<&crate::error::NonDeterministicDetails>,
    metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> HarvestResult<()> {
    let error = error.to_string();
    let deferred = conn
        .transaction::<Vec<crate::completion_trigger::DeferredTriggerStart>, HarvestError, _>(
            |conn| {
                async move {
                    store::append_events(
                        conn,
                        exec_id,
                        &[WorkflowEvent::WorkflowFailed {
                            error: error.clone(),
                        }],
                        next_event_id,
                    )
                    .await?;
                    update_workflow_execution_failed(conn, exec_id, worker_id, &error, nd_details)
                        .await?;
                    queue::fail_task(conn, task_id, &error).await?;
                    let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
                    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                        conn,
                        exec_id,
                        crate::completion_trigger::TerminalState::Failed,
                        metrics,
                    )
                    .await?;
                    deferred.extend(triggers);
                    Ok(deferred)
                }
                .scope_boxed()
            },
        )
        .await?;

    for start in deferred {
        start.spawn();
    }
    Ok(())
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

/// Persist the last `SetCurrentDetails` command to the execution row (issue #473).
///
/// Scans `commands` for [`WorkflowCommand::SetCurrentDetails`] variants and
/// writes the **last** value (last-write-wins) to
/// `harvest_workflow_executions.current_details` in a single `UPDATE`.
/// Does nothing when no `SetCurrentDetails` command is present.
async fn persist_current_details_from_commands(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    commands: &[WorkflowCommand],
) -> HarvestResult<()> {
    // Last-write-wins: take the last SetCurrentDetails value in the command list.
    let last = commands.iter().rev().find_map(|cmd| {
        if let WorkflowCommand::SetCurrentDetails { value } = cmd {
            Some(value.as_str())
        } else {
            None
        }
    });

    if let Some(details) = last {
        store::update_current_details(conn, exec_id, details).await?;
    }
    Ok(())
}

async fn persist_signal_wait_park(
    conn: &mut AsyncPgConnection,
    detached_spawns: DetachedSpawnPersistence<'_>,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    commands: &[WorkflowCommand],
    sticky: Option<queue::StickyHint<'_>>,
) -> HarvestResult<()> {
    let marker_events = pre_suspension_events_from_commands(commands);
    // Park the workflow task (state=RUNNING, worker cleared) so it is not
    // confused with a timer-waiting task (state=PENDING). This ensures that
    // `wake_workflow_task` — which only targets RUNNING/parked rows — can
    // reliably distinguish signal waits from timer waits and will not
    // prematurely fire a pending timer when a signal is delivered.
    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &marker_events, next_event_id).await?;
            detached_spawns.persist(conn, commands).await?;
            queue::park_workflow_task(conn, task_id, sticky).await
        }
        .scope_boxed()
    })
    .await?;

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
    let marker_events = pre_suspension_events_from_commands(commands);

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .for_update()
                .select(WorkflowExecution::as_select())
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?
                .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

            for event in marker_events {
                store::append_single_event(conn, exec_id, event).await?;
            }
            detached_spawns.persist(conn, commands).await?;

            let history = store::load_history(conn, exec_id).await?;
            let has_terminal = activity_ids
                .iter()
                .any(|activity_id| has_activity_terminal_event(&history.events, *activity_id));

            queue::park_workflow_task(conn, task_id, sticky).await?;
            if has_terminal {
                queue::wake_workflow_task(conn, exec_id).await?;
            }
            Ok(())
        }
        .scope_boxed()
    })
    .await
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
) -> HarvestResult<()> {
    // activity_events is built in scheduled_activities order (= ScheduleActivity command order).
    // After the loop we interleave them with marker/detached-spawn events in full command order.
    let mut activity_events: Vec<WorkflowEvent> = Vec::with_capacity(scheduled_activities.len());
    let mut enqueued = Vec::with_capacity(scheduled_activities.len());

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

        let effective_retry = scheduled
            .retry_policy_override
            .clone()
            .or_else(|| activity.default_retry_policy.clone());
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
        let effective_stc = scheduled
            .start_to_close_override
            .or(activity.default_start_to_close);
        if let Some(timeout) = effective_stc {
            params.start_to_close =
                Some(chrono_duration_from_std(timeout, "start_to_close timeout")?);
        }
        if let Some(timeout) = activity.default_schedule_to_start {
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

        let effective_rate_limit_key =
            activity
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

    // Build the event list in command emission order: markers, detached-spawn events, and
    // ActivityScheduled events are interleaved at their actual command positions so that
    // the replay engine's sequential cursor sees the same order as command emission.
    let mut act_iter = activity_events.into_iter();
    let events = build_suspension_events(commands, |cmd| {
        if matches!(cmd, WorkflowCommand::ScheduleActivity { .. }) {
            act_iter.next()
        } else {
            None
        }
    });

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &events, next_event_id).await?;
            detached_spawns.persist(conn, commands).await?;
            for params in &enqueued {
                queue::enqueue(conn, params).await?;
            }
            queue::park_workflow_task(conn, task_id, sticky).await?;
            Ok(())
        }
        .scope_boxed()
    })
    .await
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
) -> HarvestResult<()> {
    use tracing::Instrument;

    // Check if the timer already exists and is active (unfired) in the database.
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
        let fire_delay = chrono_duration_from_secs(timer.duration_secs, "timer duration")?;
        // Anchor the deadline to the database clock: timer due-ness and the
        // signal `received_at` default both come from Postgres NOW(), so the
        // chronological wake ingest (merge_wake_events) compares timestamps
        // from a single clock regardless of worker clock skew.
        let db_now = db_clock_now(conn).await?;
        db_now + fire_delay
    };

    let is_new = existing.is_none();
    // Build the event list in command emission order. TimerStarted (for new timers) is
    // interleaved with marker/detached-spawn events at its actual command position.
    let mut timer_event = is_new.then(|| WorkflowEvent::TimerStarted {
        timer_id: timer.timer_id.clone(),
        duration_secs: timer.duration_secs,
    });
    let events = build_suspension_events(commands, |cmd| {
        if matches!(cmd, WorkflowCommand::StartTimer { .. }) {
            timer_event.take()
        } else {
            None
        }
    });

    // Emit a span for the timer placement (not to be confused with
    // harvest.timer.fire which is emitted when the timer actually fires).
    let span = tracing::info_span!(
        "harvest.timer.start",
        "otel.kind" = "internal",
        timer.id = %timer.timer_id,
        timer.duration_secs = timer.duration_secs,
        { ATTR_EXECUTION_ID } = %exec_id,
    );

    let has_events = !events.is_empty();

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            use crate::schema::harvest_task_queue::dsl as queue_dsl;

            if has_events {
                store::append_events(conn, exec_id, &events, next_event_id).await?;
            }
            detached_spawns.persist(conn, commands).await?;

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
            Ok(())
        }
        .scope_boxed()
    })
    .instrument(span)
    .await?;

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

    conn.transaction::<(), HarvestError, _>(|conn| {
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
            let mut all_events_by_pos = pre_events_by_pos;
            all_events_by_pos.append(&mut child_events_by_pos);
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

            // Insert rows and enqueue tasks for new children.
            for child in &new_children {
                let child_workflow_id = child.child_id.to_string();
                let (owner, runbook_url, severity, child_sla, child_execution_timeout) = registry
                    .workflows
                    .get(child.workflow_name.as_str())
                    .map_or((None, None, None, None, None), |w| {
                        (
                            w.owner,
                            w.runbook_url,
                            w.severity,
                            w.sla,
                            w.execution_timeout,
                        )
                    });
                let child_execution_timeout =
                    child_execution_timeout.and_then(|d| chrono::Duration::from_std(d).ok());
                let child_sla = child_sla.and_then(|d| chrono::Duration::from_std(d).ok());
                let child_deadline_at = child_execution_timeout.map(|d| chrono::Utc::now() + d);
                let child_sla_deadline_at = child_sla.map(|d| chrono::Utc::now() + d);
                let child_row = NewWorkflowExecution {
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
                };
                let child_started_event = WorkflowEvent::WorkflowStarted {
                    input: child.input.clone(),
                    timestamp: chrono::Utc::now(),
                    last_completion_result: None,
                    last_error: None,
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
                store::append_events(conn, child.child_id, &[child_started_event], 0).await?;
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

            queue::park_workflow_task(conn, task_id, sticky).await?;

            if any_terminal {
                queue::wake_workflow_task(conn, parent_exec_id).await?;
            }

            Ok(())
        }
        .scope_boxed()
    })
    .await
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
async fn ingest_due_timers_and_signals(
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
    )
    .await
}

async fn finalize_activity_completion(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
    output: serde_json::Value,
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
            store::append_events(conn, exec_id, &[completion_event], history.next_event_id).await?;
            queue::complete_task(conn, task.id, output).await?;
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

async fn wake_parent_for_child_completion(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    child_exec_id: ExecutionId,
    output: serde_json::Value,
) -> HarvestResult<()> {
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

async fn wake_parent_for_child_failure(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    child_exec_id: ExecutionId,
    error: &str,
) -> HarvestResult<()> {
    let event = WorkflowEvent::ChildWorkflowFailed {
        child_id: child_exec_id,
        error: error.to_string(),
    };
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
) -> HarvestResult<()> {
    let event = WorkflowEvent::WorkflowCompleted {
        output: output.clone(),
    };

    let deferred = conn
        .transaction::<Vec<crate::completion_trigger::DeferredTriggerStart>, HarvestError, _>(
            |conn| {
                let output = output.clone();
                async move {
                    store::append_events(conn, exec_id, &[event], next_event_id).await?;
                    update_workflow_execution_completed(conn, exec_id, worker_id, &output).await?;
                    queue::complete_task(conn, task_id, output.clone()).await?;
                    let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
                    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                        conn,
                        exec_id,
                        crate::completion_trigger::TerminalState::Completed,
                        metrics,
                    )
                    .await?;
                    deferred.extend(triggers);
                    wake_parent_for_child_completion(conn, parent_exec_id, exec_id, output).await?;
                    Ok(deferred)
                }
                .scope_boxed()
            },
        )
        .await?;

    for start in deferred {
        start.spawn();
    }
    Ok(())
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
) -> HarvestResult<()> {
    let workflow_failure = WorkflowEvent::WorkflowFailed {
        error: error.to_string(),
    };

    let deferred = conn
        .transaction::<Vec<crate::completion_trigger::DeferredTriggerStart>, HarvestError, _>(
            |conn| {
                let error = error.to_string();
                async move {
                    store::append_events(conn, exec_id, &[workflow_failure], next_event_id).await?;
                    update_workflow_execution_failed(conn, exec_id, worker_id, &error, nd_details)
                        .await?;
                    queue::fail_task(conn, task_id, &error).await?;
                    let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
                    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                        conn,
                        exec_id,
                        crate::completion_trigger::TerminalState::Failed,
                        metrics,
                    )
                    .await?;
                    deferred.extend(triggers);
                    wake_parent_for_child_failure(conn, parent_exec_id, exec_id, &error).await?;
                    Ok(deferred)
                }
                .scope_boxed()
            },
        )
        .await?;

    for start in deferred {
        start.spawn();
    }
    Ok(())
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
async fn create_detached_child_executions(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    parent_execution: &WorkflowExecution,
    commands: &[WorkflowCommand],
    execute_span: &tracing::Span,
) -> HarvestResult<()> {
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
        let (owner, runbook_url, severity, child_sla) = registry
            .workflows
            .get(workflow_name.as_str())
            .map_or((None, None, None, None), |w| {
                (w.owner, w.runbook_url, w.severity, w.sla)
            });
        let child_sla = child_sla.and_then(|d| chrono::Duration::from_std(d).ok());
        let child_sla_deadline_at = child_sla.map(|d| chrono::Utc::now() + d);
        let child_row = NewWorkflowExecution {
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
/// before the next retry attempt could start.
///
/// Used in the retry path to short-circuit requeue and instead emit a
/// `ScheduleToClose` timeout (issue #378).
fn schedule_to_close_deadline_exceeded(
    task: &TaskQueueItem,
    retry_delay: chrono::Duration,
) -> bool {
    let Some(deadline) = task.schedule_to_close_at else {
        return false;
    };
    chrono::Utc::now() + retry_delay >= deadline
}

/// Append `ActivityTimedOut { ScheduleToClose }` and fail the task row.
///
/// Called from the retry path when the cross-retry wall-clock deadline
/// (`schedule_to_close_at`) would be exceeded before the next attempt starts.
async fn record_schedule_to_close_activity_timeout(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    activity_id: ActivityExecId,
) -> HarvestResult<()> {
    let error = HarvestError::Timeout {
        timeout_type: crate::error::TimeoutType::ScheduleToClose,
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
                timeout_type: crate::error::TimeoutType::ScheduleToClose,
            };
            store::append_events(conn, exec_id, &[timeout_event], history.next_event_id).await?;
            queue::fail_task(conn, task.id, &error).await?;
            queue::wake_workflow_task(conn, exec_id).await
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
) -> HarvestResult<()> {
    match activity_result {
        Ok(output) => {
            let observed_bytes = serde_json::to_string(&output).map_or(0, |s| s.len() as u64);
            if max_result_bytes > 0 && observed_bytes > max_result_bytes {
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
            finalize_activity_completion(conn, task, exec_id, activity_id, output).await
        }
        Err(error) => {
            let delay_result = next_retry_delay(task, &error, retry_policy);
            let delay = fail_execution_on_error(conn, task, worker_id, delay_result).await?;

            if let Some(delay) = delay {
                // Pre-retry deadline check (issue #378): if the schedule_to_close
                // wall-clock deadline would be exceeded before the next attempt
                // starts, fail with ScheduleToClose instead of requeuing.
                if schedule_to_close_deadline_exceeded(task, delay) {
                    return record_schedule_to_close_activity_timeout(
                        conn,
                        task,
                        exec_id,
                        activity_id,
                    )
                    .await;
                }
                // Store the human-readable error for ActivityContext::previous_failure()
                // on the next attempt. Typed payloads are unwrapped to their message.
                let previous_error = crate::failure::parse_error_payload_full(&error).message;
                return queue::requeue_for_retry(conn, task.id, delay, &previous_error).await;
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

#[allow(clippy::too_many_lines)]
async fn process_activity_task(
    pool: &DbPool,
    registry: &HandlerRegistry,
    task: &TaskQueueItem,
    worker_id: &str,
    cancellation_grace_period: Duration,
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
            registry
                .telemetry()
                .metrics
                .record_rate_limit_throttled(key);
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

    let mut activity_future = (activity.handler)(&ctx, task.input.clone());
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
    let activity_result = match activity_result {
        Ok(output) if effective_result_cap > 0 => {
            let observed = serde_json::to_string(&output).map_or(0, |s| s.len() as u64);
            if observed > effective_result_cap {
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

    let duration_secs = started_at.elapsed().as_secs_f64();
    let status = if activity_result.is_ok() {
        ActivityStatus::Completed
    } else {
        ActivityStatus::Failed
    };
    // Parse the structured payload once and reuse for both the histogram
    // and the per-failure counter (so the `error.type` attribute is
    // consistent across `harvest.activity.duration` and
    // `harvest.activity.failed`).
    let failure_info = activity_result
        .as_ref()
        .err()
        .map(|payload| parse_error_payload(payload));
    telemetry.metrics.record_activity_completed_with_error_type(
        activity_name,
        &task.queue_name,
        duration_secs,
        status,
        failure_info.as_ref().map(|(et, _, _)| et.as_str()),
    );
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
    )
    .await
}

#[allow(clippy::too_many_arguments)]
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
        conn.transaction::<(), HarvestError, _>(|conn| {
            async move {
                let locked = external_task::find_by_token_locked(conn, token).await?;

                // Recompute the event offset inside the transaction: external
                // completion may have appended a terminal event between replay
                // start (when next_event_id was sampled) and here.  Using
                // append_single_event serialises each append against concurrent
                // writers via the per-execution FOR UPDATE it acquires.
                let marker_events = pre_suspension_events_from_commands(commands);
                for event in marker_events {
                    store::append_single_event(conn, exec_id, event).await?;
                }
                detached_spawns.persist(conn, commands).await?;

                if locked.is_some_and(|t| t.state != "PENDING") {
                    // The task is still RUNNING (owned by this worker), so
                    // wake_workflow_task (which only moves parked rows) is a
                    // no-op.  Park first to clear worker ownership, then wake
                    // so the next available worker picks up the terminal event.
                    queue::park_workflow_task(conn, task_id, sticky).await?;
                    queue::wake_workflow_task(conn, exec_id).await
                } else {
                    queue::park_workflow_task(conn, task_id, sticky).await
                }
            }
            .scope_boxed()
        })
        .await?;
        return Ok(());
    }

    // Build the event list in command emission order so that ScheduleExternalActivity's
    // ActivityAwaitingExternal event lands at its actual command position relative to any
    // markers or detached-spawn events in the same batch.
    let mut awaiting_event = Some(WorkflowEvent::ActivityAwaitingExternal {
        activity_id: scheduled.activity_id,
        token: scheduled.token,
        name: scheduled.name.clone(),
        input: scheduled.input.clone(),
        queue: scheduled.queue.clone(),
        schedule_to_close_secs: scheduled.schedule_to_close_secs,
    });
    let events = build_suspension_events(commands, |cmd| {
        if matches!(cmd, WorkflowCommand::ScheduleExternalActivity { .. }) {
            awaiting_event.take()
        } else {
            None
        }
    });

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
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
            queue::park_workflow_task(conn, task_id, sticky).await?;
            Ok(())
        }
        .scope_boxed()
    })
    .await
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
    let events = pre_suspension_events_from_commands(commands);

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            if !events.is_empty() {
                store::append_events(conn, exec_id, &events, next_event_id).await?;
            }
            detached_spawns.persist(conn, commands).await?;
            queue::park_workflow_task(conn, task_id, sticky).await?;
            queue::wake_workflow_task(conn, exec_id).await
        }
        .scope_boxed()
    })
    .await
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
        )
        .await
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
) -> HarvestResult<(store::EventHistory, Vec<TimerId>, Vec<String>)> {
    let history_result = store::load_history(conn, exec_id).await;
    let initial_history = fail_execution_on_error(conn, task, worker_id, history_result).await?;

    // Single chronological ingest: due timer fires and pending signals are
    // appended in occurrence order (signal received before a deadline lands
    // before that TimerFired) — see merge_wake_events.
    let ingest_result =
        ingest_due_timers_and_signals(conn, exec_id, initial_history.next_event_id).await;
    let (timers_fired, signals_delivered) =
        fail_execution_on_error(conn, task, worker_id, ingest_result).await?;

    let final_history_result = store::load_history(conn, exec_id).await;
    let final_history =
        fail_execution_on_error(conn, task, worker_id, final_history_result).await?;
    Ok((final_history, timers_fired, signals_delivered))
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
) -> HarvestResult<PreparedWorkflowTask> {
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
        let existing_delta_result =
            store::load_history_since(conn, exec_id, cached_state.next_event_id).await;
        let existing_delta =
            fail_execution_on_error(conn, task, worker_id, existing_delta_result).await?;

        // Single chronological ingest of due timers + pending signals (see
        // merge_wake_events for the occurrence-order contract).
        let ingest_result =
            ingest_due_timers_and_signals(conn, exec_id, existing_delta.next_event_id).await;
        let (timers_fired, signals_delivered) =
            fail_execution_on_error(conn, task, worker_id, ingest_result).await?;

        // Load events appended by the ingest.
        let after_ingest_result =
            store::load_history_since(conn, exec_id, existing_delta.next_event_id).await;
        let after_ingest =
            fail_execution_on_error(conn, task, worker_id, after_ingest_result).await?;

        // Reconstruct full history: cached snapshot + any pre-existing delta +
        // ingested timer/signal events.
        let mut history_events = cached_state.events.clone();
        history_events.extend(existing_delta.events);
        history_events.extend(after_ingest.events);
        let next_event_id = after_ingest.next_event_id;

        Ok(PreparedWorkflowTask {
            execution,
            exec_id,
            history_events,
            next_event_id,
            timers_fired,
            signals_delivered,
            was_cache_hit: true,
        })
    } else {
        // Cache miss path: full history load.
        let (history, timers_fired, signals_delivered) =
            load_workflow_replay_state(conn, task, worker_id, exec_id).await?;

        Ok(PreparedWorkflowTask {
            execution,
            exec_id,
            history_events: history.events,
            next_event_id: history.next_event_id,
            timers_fired,
            signals_delivered,
            was_cache_hit: false,
        })
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

async fn persist_workflow_continue_as_new(
    conn: &mut AsyncPgConnection,
    persistence: WorkflowTaskPersistence<'_>,
    execution: &WorkflowExecution,
    input: serde_json::Value,
) -> HarvestResult<()> {
    use crate::schema::{harvest_signals, harvest_workflow_executions};

    if reject_child_continue_as_new(conn, &persistence, execution).await? {
        return Ok(());
    }

    // The new execution stays on the same shard so all of its event log,
    // queue rows, timers, and signals continue to live in the same Postgres
    // database as its predecessor.
    let new_exec_id = ExecutionId::new_for_shard(persistence.exec_id.shard());
    let task_id = persistence.task.id;
    let exec_id = persistence.exec_id;
    let next_event_id = persistence.next_event_id;
    let worker_id = persistence.worker_id;
    let started_event = WorkflowEvent::WorkflowStarted {
        input: input.clone(),
        timestamp: chrono::Utc::now(),
        // Preserve scheduled carryover across the fork (issue #488): the continuation is
        // the same logical scheduled run, so it must see the same frozen values rather
        // than re-resolving (which could pick up a newer sibling fire's output).
        last_completion_result: persistence.carryover_result.clone(),
        last_error: persistence.carryover_error.clone(),
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
            store::append_events(conn, exec_id, &[continued_event], next_event_id).await?;

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

            store::append_events(conn, new_exec_id, &[started_event], 0).await?;

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

#[allow(clippy::too_many_lines)]
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
) -> HarvestResult<()> {
    let parent_exec_id = execution.parent_id.map(execution_id_from_uuid);
    // A detached child has parent_close_policy set (non-null). Detached children
    // do NOT wake their parent on completion or failure.
    let is_detached_child = execution.parent_close_policy.is_some();

    match (outcome, parent_exec_id) {
        (WorkflowOutcome::Completed { output }, Some(parent_id)) if !is_detached_child => {
            persist_child_workflow_completion(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                parent_id,
                output,
                Some(registry.telemetry().metrics.as_ref()),
            )
            .await
        }
        (WorkflowOutcome::Completed { output }, _) => {
            // Root workflow or detached child completing — no parent wake.
            let result = persist_workflow_completion(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                output,
                Some(registry.telemetry().metrics.as_ref()),
            )
            .await;
            if result.is_ok() && update_schedule_counter {
                crate::scheduler::maybe_reset_schedule_failure_counter(
                    conn,
                    &execution.workflow_id,
                    &execution.workflow_name,
                )
                .await;
            }
            result
        }
        (
            WorkflowOutcome::Failed {
                error,
                non_deterministic_details,
            },
            Some(parent_id),
        ) if !is_detached_child => {
            let result = persist_child_workflow_failure(
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
            .await;
            if result.is_ok() && update_schedule_counter {
                crate::scheduler::maybe_increment_schedule_failure_counter(
                    conn,
                    &execution.workflow_id,
                    &execution.workflow_name,
                    registry.telemetry().metrics.as_ref(),
                )
                .await;
            }
            result
        }
        (
            WorkflowOutcome::Failed {
                error,
                non_deterministic_details,
            },
            _,
        ) => {
            // Root workflow or detached child failing — no parent wake.
            let result = persist_workflow_failure(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                &error,
                non_deterministic_details.as_ref(),
                Some(registry.telemetry().metrics.as_ref()),
            )
            .await;
            if result.is_ok() && update_schedule_counter {
                crate::scheduler::maybe_increment_schedule_failure_counter(
                    conn,
                    &execution.workflow_id,
                    &execution.workflow_name,
                    registry.telemetry().metrics.as_ref(),
                )
                .await;
            }
            result
        }
        (WorkflowOutcome::Suspended { commands }, _) => {
            handle_suspended_workflow(
                conn,
                registry,
                SuspendedWorkflowContext {
                    execution,
                    persistence,
                    execute_span,
                },
                &commands,
            )
            .await
        }
        (WorkflowOutcome::ContinuedAsNew { input }, _) => {
            // task/worker_id are Copy references; capture before persistence is moved.
            let task = persistence.task;
            let worker_id = persistence.worker_id;
            let result =
                persist_workflow_continue_as_new(conn, persistence, execution, input).await;
            fail_execution_on_error(conn, task, worker_id, result).await
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
    Persisted,
}

/// Map a terminal/suspended outcome to its deferred schedule-failure-counter
/// action: `Some(false)` resets (success), `Some(true)` increments (failure),
/// `None` leaves it untouched (suspended / continue-as-new). The action depends
/// only on Completed-vs-Failed, so it is identical for root and child variants.
const fn schedule_counter_action(outcome: &WorkflowOutcome) -> Option<bool> {
    match outcome {
        WorkflowOutcome::Completed { .. } => Some(false),
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
            )
            .await;
        }
        Some(true) => {
            crate::scheduler::maybe_increment_schedule_failure_counter(
                conn,
                &execution.workflow_id,
                &execution.workflow_name,
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
) -> HarvestResult<()> {
    let mut next_event_id = persistence.next_event_id;
    persist_update_result_commands(conn, persistence.exec_id, pending_cmds, &mut next_event_id)
        .await?;
    persist_search_attrs_from_commands(conn, persistence.exec_id, pending_cmds).await?;
    persist_current_details_from_commands(conn, persistence.exec_id, pending_cmds).await?;

    let pre_terminal = pre_suspension_events_from_commands(pending_cmds);
    if !pre_terminal.is_empty() {
        store::append_events(conn, persistence.exec_id, &pre_terminal, next_event_id).await?;
        next_event_id = next_event_id
            .checked_add(i32::try_from(pre_terminal.len()).unwrap_or(i32::MAX))
            .ok_or_else(|| crate::error::HarvestError::Database("Event ID overflow".to_string()))?;
    }

    create_detached_child_executions(conn, registry, execution, pending_cmds, execute_span).await?;

    // `update_schedule_counter: false` — the caller runs counters after commit.
    persist_workflow_outcome(
        conn,
        registry,
        execution,
        WorkflowTaskPersistence {
            next_event_id,
            ..persistence
        },
        outcome,
        execute_span,
        false,
    )
    .await
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
    u64::try_from(
        commands
            .iter()
            .filter(|cmd| {
                matches!(
                    cmd,
                    WorkflowCommand::RecordMarker { .. }
                        | WorkflowCommand::RecordSideEffect { .. }
                        | WorkflowCommand::SpawnDetachedChildWorkflow { .. }
                )
            })
            .count(),
    )
    .unwrap_or(u64::MAX)
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

async fn move_workflow_to_dlq_for_history_cap(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    parent_exec_id: Option<ExecutionId>,
    reason: DeadLetterReason,
) -> HarvestResult<Vec<DeferredTriggerStart>> {
    let reason = reason.to_string();

    let deferred = conn
        .transaction::<Vec<crate::completion_trigger::DeferredTriggerStart>, HarvestError, _>(
            |conn| {
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
                        &[WorkflowEvent::WorkflowFailed {
                            error: reason.clone(),
                        }],
                        next_event_id,
                    )
                    .await?;
                    update_workflow_execution_failed(conn, exec_id, worker_id, &reason, None)
                        .await?;
                    queue::fail_task(conn, task.id, &reason).await?;
                    let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
                    let failed_triggers =
                        crate::completion_trigger::evaluate_triggers_for_execution(
                            conn,
                            exec_id,
                            crate::completion_trigger::TerminalState::Failed,
                            None,
                        )
                        .await?;
                    deferred.extend(failed_triggers);
                    if let Some(parent_exec_id) = parent_exec_id {
                        wake_parent_for_child_failure(conn, parent_exec_id, exec_id, &reason)
                            .await?;
                    }
                    Ok(deferred)
                }
                .scope_boxed()
            },
        )
        .await?;

    Ok(deferred)
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
    move_workflow_to_dlq_for_history_cap(
        conn,
        task,
        exec_id,
        next_event_id,
        worker_id,
        execution.parent_id.map(execution_id_from_uuid),
        reason,
    )
    .await
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
) -> HarvestResult<()> {
    let mut prepared =
        prepare_workflow_task_with_cache(conn, task, worker_id, &workflow_cache, sticky_timeout)
            .await?;
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
    let started_at = std::time::Instant::now();

    // Drive the workflow in a loop so that local activities can be executed
    // inline without parking the task. Each iteration runs the workflow until
    // it suspends; if it suspends on a RunLocalActivity command the handler
    // is executed here, its events are appended to history, and the workflow
    // is re-run with the extended history. Any other suspension (regular
    // activity, timer, signal wait, …) breaks out of the loop.
    let mut history_events = prepared.history_events;
    let mut next_event_id = prepared.next_event_id;

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
                                return fail_execution_on_error(
                                    conn,
                                    task,
                                    worker_id,
                                    Err::<(), _>(e),
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
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(e) => {
                        return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e))
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
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e)).await;
                }
                if let Err(e) =
                    persist_search_attrs_from_commands(conn, prepared.exec_id, &commands).await
                {
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e)).await;
                }
                if let Err(e) =
                    persist_current_details_from_commands(conn, prepared.exec_id, &commands).await
                {
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e)).await;
                }
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
                        return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e))
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
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e)).await;
                }
                if let Err(e) =
                    persist_search_attrs_from_commands(conn, prepared.exec_id, &commands).await
                {
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e)).await;
                }
                if let Err(e) =
                    persist_current_details_from_commands(conn, prepared.exec_id, &commands).await
                {
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e)).await;
                }
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
                        return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e))
                            .await;
                    }
                };
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

    let (outcome, pending_cmds, execute_span) = loop_result;

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
            queue::park_workflow_task(conn, task.id, sticky).await?;
            drop(execute_span);
            return Ok(());
        }
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
    match &outcome {
        WorkflowOutcome::Completed { .. } => telemetry.metrics.record_workflow_terminal(
            &prepared.execution.workflow_name,
            &task.queue_name,
            WorkflowStatus::Completed,
        ),
        WorkflowOutcome::Failed {
            non_deterministic_details,
            ..
        } => {
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

    // Extract this run's frozen carryover (issue #488) from the decoded WorkflowStarted
    // (history_events[0]) so a continue_as_new continuation can inherit it.
    // Slice pattern (not .first()) avoids the in-scope Diesel RunQueryDsl::first ambiguity.
    let (carryover_result, carryover_error) = if let [
        WorkflowEvent::WorkflowStarted {
            last_completion_result,
            last_error,
            ..
        },
        ..,
    ] = history_events.as_slice()
    {
        (last_completion_result.clone(), last_error.clone())
    } else {
        (None, None)
    };
    let persistence = WorkflowTaskPersistence {
        task,
        worker_id,
        exec_id: prepared.exec_id,
        next_event_id,
        sticky_timeout,
        carryover_result,
        carryover_error,
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
    let execution_ref = &prepared.execution;
    let exec_uuid = prepared.exec_id.as_uuid();

    let persist_flow = conn
        .transaction::<WorkflowPersistFlow, HarvestError, _>(|conn| {
            async move {
                use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
                let locked_state: Option<String> = exec_dsl::harvest_workflow_executions
                    .find(exec_uuid)
                    .select(exec_dsl::state)
                    .for_update()
                    .first::<String>(conn)
                    .await
                    .optional()
                    .map_err(crate::error::database_error)?;
                if locked_state.as_deref() == Some("PAUSED") {
                    let sticky = if sticky_timeout.is_zero() {
                        None
                    } else {
                        Some(queue::StickyHint::new(worker_id, sticky_timeout))
                    };
                    queue::park_workflow_task(conn, task.id, sticky).await?;
                    return Ok(WorkflowPersistFlow::ParkedPaused);
                }

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
                    .await?;
                } else {
                    persist_workflow_outcome(
                        conn,
                        registry,
                        execution_ref,
                        persistence,
                        outcome,
                        &execute_span,
                        false,
                    )
                    .await?;
                }
                Ok(WorkflowPersistFlow::Persisted)
            }
            .scope_boxed()
        })
        .await;
    // execute_span is moved into and dropped by the transaction closure above,
    // closing the OTel span after all producer spans have been emitted as its
    // children.

    match persist_flow {
        Ok(WorkflowPersistFlow::ParkedPaused) => return Ok(()),
        Ok(WorkflowPersistFlow::Persisted) => {
            // Deferred best-effort schedule counters, in autocommit post-commit.
            run_deferred_schedule_counter(conn, registry, &prepared.execution, counter_action)
                .await;
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
            )
            .await
        }
    }
}

/// Periodically sample per-queue pending-task counts and forward them to the
/// configured [`MetricsRecorder`](crate::telemetry::MetricsRecorder).
///
/// The sampler skips work entirely when the recorder is the default no-op
/// implementation, so unconfigured deployments pay no DB cost. It queries a
/// single `GROUP BY queue_name` aggregate per tick — cheap enough to run at
/// the same cadence as the poll interval.
///
/// Stops when the cancellation token fires. Queues with zero pending rows are
/// also reported (as depth 0) so gauges reset cleanly after drains.
fn spawn_queue_depth_sampler(
    pool: DbPool,
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

            let mut conn = match pool.get().await {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::debug!(
                        error = %error,
                        "queue depth sampler could not acquire DB connection"
                    );
                    continue;
                }
            };

            match queue::queue_depths(&mut conn, &queues).await {
                Ok(depths) => {
                    let mut observed: HashSet<&str> = HashSet::new();
                    for (queue_name, depth) in &depths {
                        observed.insert(queue_name.as_str());
                        telemetry
                            .metrics
                            .record_queue_depth(queue_name, u64::try_from(*depth).unwrap_or(0));
                    }
                    for queue_name in &queues {
                        if !observed.contains(queue_name.as_str()) {
                            telemetry.metrics.record_queue_depth(queue_name, 0);
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(error = %error, "queue depth sample failed");
                }
            }

            // Sample oldest-pending-age gauge alongside queue depth.
            match queue::oldest_pending_ages(&mut conn, &queues).await {
                Ok(ages) => {
                    let mut observed: HashSet<&str> = HashSet::new();
                    for (queue_name, age_secs) in &ages {
                        observed.insert(queue_name.as_str());
                        telemetry
                            .metrics
                            .record_queue_oldest_pending_age(queue_name, *age_secs);
                    }
                    // Reset queues with no eligible tasks to 0 so stale gauge
                    // values do not linger after a queue drains.
                    for queue_name in &queues {
                        if !observed.contains(queue_name.as_str()) {
                            telemetry
                                .metrics
                                .record_queue_oldest_pending_age(queue_name, 0.0);
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(error = %error, "oldest pending age sample failed");
                }
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
    pool: DbPool,
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

            let mut conn = match pool.get().await {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::debug!(
                        error = %error,
                        "concurrency sampler could not acquire DB connection"
                    );
                    continue;
                }
            };

            match queue::concurrency_key_stats(&mut conn, &queues).await {
                Ok(stats) => {
                    for stat in &stats {
                        // The stats are grouped by (key, task_type) so workflow
                        // and activity budgets for the same key don't collide on
                        // the same metric label.
                        let metric_key = format!("{}:{}", stat.key, stat.task_type);
                        telemetry.metrics.record_concurrency_key_in_flight(
                            &metric_key,
                            u64::try_from(stat.in_flight).unwrap_or(0),
                        );
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
                            telemetry.metrics.record_concurrency_key_deferred(
                                &metric_key,
                                u64::try_from(stat.pending).unwrap_or(0),
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(error = %error, "concurrency key stats sample failed");
                }
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
    pool: DbPool,
    cancel: CancellationToken,
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    #[derive(diesel::QueryableByName)]
    struct BucketRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        key: String,
        #[diesel(sql_type = diesel::sql_types::Double)]
        refill_rate: f64,
        #[diesel(sql_type = diesel::sql_types::Double)]
        estimated_tokens: f64,
    }

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
                        "rate limit sampler could not acquire DB connection"
                    );
                    continue;
                }
            };

            let result: Result<Vec<BucketRow>, _> = diesel::sql_query(
                "SELECT \
                     key, \
                     refill_rate, \
                     LEAST(burst, tokens + EXTRACT(EPOCH FROM (NOW() - last_refilled_at)) * refill_rate) AS estimated_tokens \
                 FROM harvest_rate_limit_buckets"
            )
            .load(&mut conn)
            .await;

            match result {
                Ok(buckets) => {
                    for bucket in buckets {
                        telemetry.metrics.record_rate_limit_tokens_available(
                            &bucket.key,
                            bucket.estimated_tokens,
                        );
                        telemetry
                            .metrics
                            .record_rate_limit_refill_rate(&bucket.key, bucket.refill_rate);
                    }
                }
                Err(error) => {
                    tracing::debug!(error = %error, "rate limit sampler query failed");
                }
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
    /// Cancellation token for graceful shutdown.
    shutdown: CancellationToken,
    /// Set (and refreshed on every heartbeat) by the heartbeat task while the
    /// worker is draining.  Holds the absolute deadline from the operator's
    /// `drain_deadline_at` so that `drain_in_flight` can honour an extended
    /// window even after it has already started waiting.
    remote_drain_deadline: Arc<Mutex<Option<std::time::Instant>>>,
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
}

struct WorkerMonitoringHandles {
    queue_depth_sampler: tokio::task::JoinHandle<()>,
    concurrency_sampler: tokio::task::JoinHandle<()>,
    rate_limit_sampler: tokio::task::JoinHandle<()>,
    dlq_depth_samplers: Vec<tokio::task::JoinHandle<()>>,
    timeout_checker: tokio::task::JoinHandle<()>,
    poison_pill_reclaimer: tokio::task::JoinHandle<()>,
    pause_auto_resumer: tokio::task::JoinHandle<()>,
    history_oversized_sampler: tokio::task::JoinHandle<()>,
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
    pool: DbPool,
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

            let mut conn = match pool.get().await {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::debug!(
                        error = %error,
                        "history oversized sampler could not acquire DB connection"
                    );
                    continue;
                }
            };

            match sample_history_oversized_counts(&mut conn, soft_threshold).await {
                Ok(counts) => {
                    let mut active_workflows = std::collections::HashSet::new();
                    for (workflow_name, count) in &counts {
                        telemetry
                            .metrics
                            .record_workflow_history_oversized(workflow_name, *count);
                        active_workflows.insert(workflow_name.clone());
                    }
                    for workflow_name in reported_workflows.difference(&active_workflows) {
                        telemetry
                            .metrics
                            .record_workflow_history_oversized(workflow_name, 0);
                    }
                    reported_workflows = active_workflows;
                }
                Err(error) => {
                    tracing::debug!(error = %error, "history oversized sample failed");
                }
            }

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

        let workflow_semaphore = Arc::new(Semaphore::new(config.max_concurrent_workflows));
        let activity_semaphore = Arc::new(Semaphore::new(config.max_concurrent_activities));
        let workflow_cache = Arc::new(tokio::sync::Mutex::new(crate::cache::WorkflowCache::new(
            config.workflow_cache_size,
        )));

        Ok(Self {
            config,
            registry,
            ineligible_activities,
            workflow_semaphore,
            activity_semaphore,
            shutdown: CancellationToken::new(),
            remote_drain_deadline: Arc::new(Mutex::new(None)),
            workflow_cache,
            workflow_task_timeout_strikes: Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        })
    }

    /// Run the main poll loop until shutdown is requested.
    ///
    /// This is the worker's entry point. It keeps polling until shutdown is
    /// requested, checking the cancellation token between poll iterations.
    pub async fn run(&self, pool: &DbPool) {
        let listener = match self.config.notification_database_url.as_deref() {
            Some(database_url) => {
                match crate::notify::QueueListener::connect(database_url, &self.config.queues).await
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
        self.run_with_listener(pool, listener).await;
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

        let monitors = self.spawn_monitoring_tasks(pool);
        let heartbeat_cancel = CancellationToken::new();
        let heartbeat_handle = self.spawn_heartbeat_task(pool, heartbeat_cancel.clone());

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

        self.shutdown_and_cleanup(heartbeat_handle, monitors).await;

        tracing::info!(worker_id = %self.config.worker_id, "worker stopped");
    }

    fn spawn_monitoring_tasks(&self, pool: &DbPool) -> WorkerMonitoringHandles {
        let queue_depth_sampler = spawn_queue_depth_sampler(
            pool.clone(),
            self.shutdown.clone(),
            self.registry.telemetry().clone(),
            self.config.queues.clone(),
            self.config.poll_interval,
        );
        let concurrency_sampler = spawn_concurrency_sampler(
            pool.clone(),
            self.shutdown.clone(),
            self.registry.telemetry().clone(),
            self.config.queues.clone(),
            self.config.poll_interval,
        );
        // DLQ depth gauge — one sampler per shard assignment so every shard
        // this worker owns is reported.  Single-shard deployments get one
        // sampler for shard 0; multi-shard workers (rare) get one per entry.
        let dlq_depth_samplers: Vec<_> = {
            let assignments = &self.config.shard_assignments;
            let shards: &[_] = if assignments.is_empty() {
                // Fallback: if no explicit assignments, sample with shard 0.
                &[]
            } else {
                assignments.as_slice()
            };
            let mut handles: Vec<_> = shards
                .iter()
                .map(|shard| {
                    let shard_id = u16::try_from(shard.as_i32()).unwrap_or(0);
                    spawn_dlq_depth_sampler(
                        pool.clone(),
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
            pool.clone(),
            self.shutdown.clone(),
            self.registry.telemetry().clone(),
            self.config.poll_interval,
        );
        let timeout_checker = crate::timeout::spawn_timeout_checker(
            pool.clone(),
            self.shutdown.clone(),
            self.config.poll_interval,
            self.registry.telemetry().clone(),
            self.config.unknown_target_grace_window,
            self.config.sharded_pool.clone(),
            self.config.shard_assignments.clone(),
            self.registry.circuit_breakers(),
            self.config.max_workflow_history_events,
        );
        // Worker-stale threshold mirrors the fleet-health classifier:
        // 2 × heartbeat interval, with a 1 s floor for sub-second intervals.
        // Double *before* rounding to whole seconds so a fractional interval
        // (e.g. 1500ms → 3s, not 2s) keeps the documented liveness window, and
        // cap at one year so an absurd interval can never overflow the
        // chrono::Duration arithmetic in the reclaim path.
        let doubled = self.config.worker_heartbeat_interval.saturating_mul(2);
        let worker_stale_secs = i64::try_from(doubled.as_secs())
            .unwrap_or(crate::poison_pill::MAX_WORKER_STALE_SECS)
            .saturating_add(i64::from(doubled.subsec_nanos() > 0))
            .clamp(1, crate::poison_pill::MAX_WORKER_STALE_SECS);
        let poison_pill_reclaimer = crate::poison_pill::spawn_poison_pill_reclaimer(
            pool.clone(),
            self.shutdown.clone(),
            // Orphan reclaim is background maintenance: sweep at the heartbeat
            // cadence rather than the (much shorter) task poll interval to keep
            // the liveness scan off the hot path.
            self.config.worker_heartbeat_interval,
            self.config.poison_pill_threshold,
            worker_stale_secs,
            self.registry.telemetry().clone(),
        );
        let pause_auto_resumer = spawn_pause_auto_resumer(
            pool.clone(),
            self.shutdown.clone(),
            self.config.worker_heartbeat_interval,
            self.config.max_workflow_pause_duration,
            self.registry.telemetry().clone(),
        );
        let history_oversized_sampler = spawn_history_oversized_sampler(
            pool.clone(),
            self.shutdown.clone(),
            self.registry.telemetry().clone(),
            self.registry.history_policy().continue_as_new_threshold(),
            self.config.poll_interval,
        );

        WorkerMonitoringHandles {
            queue_depth_sampler,
            concurrency_sampler,
            rate_limit_sampler,
            dlq_depth_samplers,
            timeout_checker,
            poison_pill_reclaimer,
            pause_auto_resumer,
            history_oversized_sampler,
        }
    }

    fn spawn_heartbeat_task(
        &self,
        pool: &DbPool,
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
        let max_concurrency = i32::try_from(
            self.config.max_concurrent_workflows + self.config.max_concurrent_activities,
        )
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
            },
            Arc::clone(&self.workflow_semaphore),
            self.config.max_concurrent_workflows,
            Arc::clone(&self.activity_semaphore),
            self.config.max_concurrent_activities,
            self.config.worker_heartbeat_interval,
            heartbeat_cancel,
            self.shutdown.clone(),
            Arc::clone(&self.remote_drain_deadline),
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

    async fn shutdown_and_cleanup(
        &self,
        heartbeat_handle: tokio::task::JoinHandle<()>,
        monitors: WorkerMonitoringHandles,
    ) {
        if let Err(error) = heartbeat_handle.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "worker heartbeat task failed during shutdown"
            );
        }
        if let Err(error) = monitors.timeout_checker.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "timeout checker task failed during shutdown"
            );
        }
        if let Err(error) = monitors.poison_pill_reclaimer.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "poison-pill reclaimer task failed during shutdown"
            );
        }
        if let Err(error) = monitors.pause_auto_resumer.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "pause auto-resume scanner failed during shutdown"
            );
        }
        if let Err(error) = monitors.queue_depth_sampler.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "queue depth sampler failed during shutdown"
            );
        }
        if let Err(error) = monitors.concurrency_sampler.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "concurrency sampler failed during shutdown"
            );
        }
        if let Err(error) = monitors.rate_limit_sampler.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "rate limit sampler failed during shutdown"
            );
        }
        for sampler in monitors.dlq_depth_samplers {
            if let Err(error) = sampler.await {
                tracing::warn!(
                    worker_id = %self.config.worker_id,
                    error = %error,
                    "dlq depth sampler failed during shutdown"
                );
            }
        }
        if let Err(error) = monitors.history_oversized_sampler.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "history oversized sampler failed during shutdown"
            );
        }
    }

    /// Register or re-register this worker in the fleet table.
    async fn register_in_fleet(&self, pool: &DbPool) {
        let shard_ids: Vec<i32> = self
            .config
            .shard_assignments
            .iter()
            .map(|s| s.as_i32())
            .collect();
        let max_concurrency = i32::try_from(
            self.config.max_concurrent_workflows + self.config.max_concurrent_activities,
        )
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
                    let Some(refill_rate) = activity.rate_limit_rps else {
                        continue;
                    };
                    let burst = activity.rate_limit_burst.unwrap_or(refill_rate);
                    let key = activity.rate_limit_key.unwrap_or(activity.name);

                    // Insert rate limit bucket if it doesn't already exist.
                    // This preserves operator overrides.
                    let q = diesel::sql_query(
                        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at) \
                         VALUES ($1, $2, $3, $3, NOW()) \
                         ON CONFLICT (key) DO NOTHING"
                    )
                    .bind::<diesel::sql_types::Text, _>(key)
                    .bind::<diesel::sql_types::Double, _>(refill_rate)
                    .bind::<diesel::sql_types::Double, _>(burst);

                    if let Err(error) = q.execute(&mut conn).await {
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

    /// Execute a single poll iteration.
    ///
    /// Gets a connection from the pool, tries to claim a task, dispatches it
    /// if found, or sleeps for `poll_interval` if the queue was empty.
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
                // Record schedule-to-start latency: wall-clock seconds between
                // the task becoming eligible (scheduled_at) and this worker
                // claiming it (started_at, set to NOW() in the claim CTE).
                // Clamped to 0 to guard against sub-millisecond clock skew.
                let started = task.started_at.unwrap_or_else(chrono::Utc::now);
                let wait_secs = (started - task.scheduled_at)
                    .max(chrono::Duration::zero())
                    .to_std()
                    .unwrap_or_default()
                    .as_secs_f64();
                self.registry
                    .telemetry()
                    .metrics
                    .record_schedule_to_start(&task.queue_name, wait_secs);
                self.dispatch_task(task, pool);
                true
            }
            Ok(None) => {
                if let Ok(throttled_keys) =
                    queue::check_throttled_keys(&mut conn, &self.config.queues).await
                {
                    for key in throttled_keys {
                        self.registry
                            .telemetry()
                            .metrics
                            .record_rate_limit_throttled(&key);
                    }
                }
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
        let exec_id_for_timeout = task.workflow_exec_id;
        let telemetry = Arc::clone(&self.registry);

        tokio::spawn(async move {
            // Acquire semaphore permit — blocks if at concurrency limit.
            let Ok(permit) = semaphore.acquire().await else {
                tracing::error!(task_id = %task_id, "semaphore closed");
                return;
            };

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
        let total_permits =
            self.config.max_concurrent_workflows + self.config.max_concurrent_activities;

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
                    u32::try_from(self.config.max_concurrent_workflows)
                        .unwrap_or(u32::MAX)
                        .min((1 << 31) - 1),
                )
                .await;
            let _act = self
                .activity_semaphore
                .acquire_many(
                    u32::try_from(self.config.max_concurrent_activities)
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
                ))
                .first::<(
                    Option<String>,
                    Option<String>,
                    Option<uuid::Uuid>,
                    Option<String>,
                    String,
                )>(&mut conn)
                .await
                .optional()
                .ok()
                .flatten();
            match res {
                Some((o, s, p, pcp, wid)) => {
                    exec_exists = true;
                    parent_id_opt = p;
                    parent_close_policy_opt = pcp;
                    workflow_id_str = wid;
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
        ), HarvestError, _>(|conn| {
            let error_msg = error_msg.clone();
            let entry = entry.clone();
            let worker_id = worker_id.to_string();
            async move {
                dlq::dead_letter(conn, &entry).await?;
                queue::fail_task(conn, task_id, &error_msg).await?;

                let (deferred, queue_used) = if let Some(exec_uuid) = exec_id_opt {
                    if exec_exists {
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
                            &[WorkflowEvent::WorkflowFailed {
                                error: error_msg.clone(),
                            }],
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
                        let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
                        let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                            conn,
                            exec_id,
                            crate::completion_trigger::TerminalState::Failed,
                            None,
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
                        (deferred, Some(entry.queue_name.clone()))
                    } else {
                        (Vec::new(), None)
                    }
                } else {
                    (Vec::new(), None)
                };

                Ok((deferred, queue_used))
            }
            .scope_boxed()
        })
        .await;

    match result {
        Ok((deferred_starts, queue_used)) => {
            if let Some(q) = queue_used {
                metrics.record_workflow_terminal(
                    workflow_name,
                    &q,
                    crate::telemetry::WorkflowStatus::Failed,
                );
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
                    metrics,
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

    fn default_runtime_config() -> WorkerRuntimeConfig {
        WorkerRuntimeConfig {
            worker_id: "test-worker-1".to_string(),
            queues: vec!["default".to_string()],
            notification_database_url: None,
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
            max_workflow_pause_duration: Duration::from_secs(24 * 3600),
            max_workflow_history_events: None,
            labels: std::collections::HashMap::new(),
            #[cfg(feature = "db")]
            sharded_pool: None,
        }
    }

    #[test]
    fn worker_config_validates() {
        let cfg = default_runtime_config();
        assert!(cfg.validate().is_ok());
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
            max_concurrent_workflows: 5,
            max_concurrent_activities: 15,
            shutdown_timeout: Duration::from_secs(60),
            workflow_cache_size: 500,
            sticky_timeout: Duration::from_secs(3),
            cancellation_grace_period: Duration::from_secs(10),
            shard_assignments: vec![crate::types::ShardId::new(0)],
            max_local_activity_start_to_close: Duration::from_secs(60),
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: String::new(),
            deployment_name: None,
            query_timeout: Duration::from_secs(5),
            priority_aging_secs: None,
            max_workflow_start_delay: Duration::from_secs(365 * 24 * 3600),
            unknown_target_grace_window: Duration::from_secs(5),
            poison_pill_threshold: 3,
            workflow_task_timeout: Duration::from_secs(10),
            max_workflow_pause_duration: Duration::from_secs(24 * 3600),
            labels: std::collections::HashMap::new(),
            max_workflow_history_events: None,
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
            name: "onboarding",
            module: "app::workflows",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            sla: None,
            concurrency: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
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
            circuit_breaker: None,
            requires: None,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        };

        let registry = HandlerRegistry::new(vec![wf], vec![act]);

        assert!(registry.workflows.contains_key("onboarding"));
        assert!(registry.activities.contains_key("send_email"));
        assert!(!registry.workflows.contains_key("nonexistent"));
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
    fn worker_creates_with_valid_config() {
        let cfg = default_runtime_config();
        let registry = Arc::new(HandlerRegistry::new(vec![], vec![]));
        let worker = Worker::new(cfg, registry);
        assert!(worker.is_ok());
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

    #[test]
    fn extract_run_local_activity_preserves_detached_after_local_command() {
        let child_id = ExecutionId::new();
        let commands = vec![
            WorkflowCommand::RunLocalActivity {
                activity_id: crate::types::ActivityExecId::new(),
                name: "format_data".to_string(),
                input: serde_json::Value::Null,
                start_to_close_secs: None,
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
                start_to_close_secs: None,
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
}
