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
use crate::context::{
    ActivityContext, SharedState, WorkflowCommand, WorkflowHistoryPolicy, empty_shared_state,
};
use crate::dlq::{self, DeadLetterReason, NewDeadLetterEntry};
use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::executor::{
    WorkflowExecuteSpanMeta, WorkflowOutcome, run_workflow_with_state_and_history_policy,
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
    ActivityExecId, ExecutionId, ExternalActivityToken, IdempotencyKey, TimerId, WorkerId,
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
        let workflows = workflows
            .into_iter()
            .map(|w| (w.name.to_string(), w))
            .collect();
        let activities = activities
            .into_iter()
            .map(|a| (a.name.to_string(), a))
            .collect();
        Self {
            workflows,
            activities,
            query_handlers: Vec::new(),
            update_handlers: Vec::new(),
            state,
            telemetry,
            history_policy: WorkflowHistoryPolicy::default(),
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
        WorkflowCommand::WaitForSignal { .. } => "WaitForSignal",
        WorkflowCommand::Complete { .. } => "Complete",
        WorkflowCommand::Fail { .. } => "Fail",
        WorkflowCommand::ContinueAsNew { .. } => "ContinueAsNew",
        WorkflowCommand::RunLocalActivity { .. } => "RunLocalActivity",
        WorkflowCommand::RecordUpdateResult { .. } => "RecordUpdateResult",
        WorkflowCommand::UpsertSearchAttributes { .. } => "UpsertSearchAttributes",
        WorkflowCommand::SignalExternalWorkflow { .. } => "SignalExternalWorkflow",
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

    let has_wait = commands
        .iter()
        .any(|cmd| matches!(cmd, WorkflowCommand::WaitForSignal { .. }));
    // RecordUpdateResult and UpsertSearchAttributes are bookkeeping already
    // handled before this check; they don't affect the signal-wait decision.
    let only_wait_or_bookkeeping = commands.iter().all(|cmd| {
        matches!(
            cmd,
            WorkflowCommand::WaitForSignal { .. }
                | WorkflowCommand::RecordMarker { .. }
                | WorkflowCommand::RecordUpdateResult { .. }
                | WorkflowCommand::UpsertSearchAttributes { .. }
        )
    });

    has_wait && only_wait_or_bookkeeping
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

#[derive(Debug, Clone, Copy)]
struct WorkflowTaskPersistence<'a> {
    task: &'a TaskQueueItem,
    worker_id: &'a str,
    exec_id: ExecutionId,
    next_event_id: i32,
    /// Grace window for pinning follow-up tasks to this worker's LRU cache.
    /// Zero disables sticky routing entirely.
    sticky_timeout: Duration,
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

#[derive(Debug, Clone, Copy)]
struct SuspendedWorkflowContext<'a> {
    execution: &'a WorkflowExecution,
    persistence: WorkflowTaskPersistence<'a>,
    /// Handle for the `harvest.workflow.execute` span that is still open.
    /// Producer spans (`activity.schedule`, `child_workflow.start`) use this as
    /// their explicit parent so they are nested inside the executor cycle.
    execute_span: &'a tracing::Span,
}

fn marker_events_from_commands(commands: &[WorkflowCommand]) -> Vec<WorkflowEvent> {
    commands
        .iter()
        .filter_map(|cmd| match cmd {
            WorkflowCommand::RecordMarker { name, details } => {
                Some(WorkflowEvent::MarkerRecorded {
                    name: name.clone(),
                    details: details.clone(),
                })
            }
            _ => None,
        })
        .collect()
}

fn extract_single_command<T>(
    commands: &[WorkflowCommand],
    extractor: impl Fn(&WorkflowCommand) -> Option<T>,
) -> Option<T> {
    // RecordUpdateResult, RecordMarker, and UpsertSearchAttributes are
    // bookkeeping commands that have already been (or are about to be)
    // processed; they do not count toward the suspension-type determination.
    let mut iter = commands.iter().filter(|cmd| {
        !matches!(
            cmd,
            WorkflowCommand::RecordMarker { .. }
                | WorkflowCommand::RecordUpdateResult { .. }
                | WorkflowCommand::UpsertSearchAttributes { .. }
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
            | WorkflowCommand::RecordUpdateResult { .. }
            | WorkflowCommand::UpsertSearchAttributes { .. } => {}
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
            | WorkflowCommand::RecordUpdateResult { .. }
            | WorkflowCommand::UpsertSearchAttributes { .. } => {}
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

fn extract_single_started_timer(commands: &[WorkflowCommand]) -> Option<StartedTimerCommand> {
    extract_single_command(commands, |cmd| {
        let WorkflowCommand::StartTimer {
            timer_id,
            duration_secs,
            ..
        } = cmd
        else {
            return None;
        };

        Some(StartedTimerCommand {
            timer_id: timer_id.clone(),
            duration_secs: *duration_secs,
        })
    })
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
                    | WorkflowCommand::RecordUpdateResult { .. }
                    | WorkflowCommand::UpsertSearchAttributes { .. }
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
/// Marker (`RecordMarker`) events are also extracted and returned so the
/// caller can append them to the event log before the local-activity events.
/// The `result_tx` inside the command is dropped immediately — the workflow
/// coroutine was already dropped when the 100 ms suspension timeout fired, so
/// nobody is listening on the receiving end.
fn extract_run_local_activity(
    commands: Vec<WorkflowCommand>,
) -> (Vec<WorkflowEvent>, LocalActivityRun) {
    // ⚡ Bolt: Pre-allocate vector capacity to avoid intermediate allocations
    let mut markers = Vec::with_capacity(commands.len());
    let mut local_run = None;
    for cmd in commands {
        match cmd {
            WorkflowCommand::RecordMarker { name, details } => {
                markers.push(WorkflowEvent::MarkerRecorded { name, details });
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
    (
        markers,
        local_run.expect("called only after confirming RunLocalActivity is present"),
    )
}

// ---------------------------------------------------------------------------
// SignalExternalWorkflow inline dispatch (same-shard)
// ---------------------------------------------------------------------------

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

/// An item in the ordered inline-dispatch batch: either a marker event or a
/// signal run. Preserving the original command-emission order is required so
/// that the replay cursor sees events in the exact same sequence as during the
/// live execution that produced them.
enum SignalBatchItem {
    Marker(WorkflowEvent),
    Signal(SignalExternalWorkflowRun),
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
            WorkflowCommand::RecordUpdateResult { .. }
            | WorkflowCommand::UpsertSearchAttributes { .. } => {}
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
async fn persist_external_signal_inline(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    items: Vec<SignalBatchItem>,
    next_event_id: &mut i32,
) -> HarvestResult<Vec<WorkflowEvent>> {
    let mut new_events: Vec<WorkflowEvent> = Vec::new();

    for item in items {
        match item {
            SignalBatchItem::Marker(event) => {
                store::append_events(conn, exec_id, std::slice::from_ref(&event), *next_event_id)
                    .await?;
                *next_event_id += 1;
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
                        *next_event_id,
                    )
                    .await?;
                    *next_event_id += 1;
                    new_events.push(requested);
                }

                let terminal = match signal::send_signal(
                    conn,
                    run.target,
                    &run.signal_name,
                    run.payload,
                )
                .await
                {
                    Ok(()) => WorkflowEvent::ExternalSignalDelivered {
                        signal_id: run.signal_id,
                    },
                    Err(HarvestError::NotFound(_)) => WorkflowEvent::ExternalSignalFailed {
                        signal_id: run.signal_id,
                        reason_code: "target_unknown".to_string(),
                    },
                    Err(HarvestError::Database(e)) => return Err(HarvestError::Database(e)),
                    Err(_) => WorkflowEvent::ExternalSignalFailed {
                        signal_id: run.signal_id,
                        reason_code: "target_terminal".to_string(),
                    },
                };

                store::append_events(
                    conn,
                    exec_id,
                    std::slice::from_ref(&terminal),
                    *next_event_id,
                )
                .await?;
                *next_event_id += 1;
                new_events.push(terminal);
            }
        }
    }

    Ok(new_events)
}

/// Run a local activity inline, appending durability events to `harvest_events`.
///
/// Retries the handler up to `max_attempts` times (per the retry policy),
/// sleeping the computed backoff between attempts. Each attempt appends a
/// `LocalActivityFailed` event; on success a `LocalActivityCompleted` event is
/// appended. Returns all newly-appended events so the caller can extend its
/// in-memory replay history and avoid a DB round-trip.
#[allow(clippy::too_many_lines)]
async fn run_local_activity_inline(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    exec_id: ExecutionId,
    marker_events: Vec<WorkflowEvent>,
    run: LocalActivityRun,
    max_start_to_close: Duration,
    next_event_id: &mut i32,
) -> HarvestResult<LocalActivityInlineOutcome> {
    let activity = registry.activities.get(&run.name).ok_or_else(|| {
        HarvestError::Config(format!("no activity handler registered for '{}'", run.name))
    })?;
    let history_event_hard_cap = registry.history_policy().event_hard_cap();

    let per_attempt_timeout = run
        .start_to_close_secs
        .map_or(max_start_to_close, Duration::from_secs)
        .min(max_start_to_close);

    let max_attempts = run.retry_policy.as_ref().map_or(1, |p| p.max_attempts);

    // When the worker crashed after appending LocalActivityScheduled but before
    // recording a terminal event, skip re-appending to avoid a duplicate.
    let mut prefix_events = marker_events;
    if !run.already_scheduled {
        prefix_events.push(WorkflowEvent::LocalActivityScheduled {
            activity_id: run.activity_id,
            name: run.name.clone(),
            input: run.input.clone(),
        });
    }
    if !prefix_events.is_empty() {
        store::append_events(conn, exec_id, &prefix_events, *next_event_id).await?;
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
        return Err(HarvestError::ActivityFailed {
            name: run.name.clone(),
            attempt: run.failed_attempts,
            source: error.into(),
        });
    }

    for attempt in start_attempt..=max_attempts {
        let ctx =
            ActivityContext::new_local_activity(registry.shared_state(), CancellationToken::new())
                .with_idempotency_key(local_idempotency_key.clone())
                .with_attempt(attempt);
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
    // Only consult the structured `error_type` when the payload was actually
    // the typed wire format — passing the synthetic "Error" fallback would
    // make a pre-existing `non_retryable_errors = ["Error"]` policy halt
    // retries on every legacy `Err(String)` failure.
    let typed = parse_typed_payload(error);
    if typed.as_ref().is_some_and(|f| f.non_retryable) {
        return Ok(None);
    }

    if let Some(policy) = retry_policy {
        let typed_error_type = typed.as_ref().map(|f| f.error_type.as_str());
        if policy.is_non_retryable(typed_error_type, error) {
            return Ok(None);
        }

        return policy
            .next_delay(task_attempt(task))
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
                return Err(HarvestError::NonDeterministic(format!(
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
                    return Err(HarvestError::NonDeterministic(format!(
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

    Ok(())
}

async fn persist_workflow_completion(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    output: serde_json::Value,
) -> HarvestResult<()> {
    let event = WorkflowEvent::WorkflowCompleted {
        output: output.clone(),
    };
    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &[event], next_event_id).await?;
            update_workflow_execution_completed(conn, exec_id, worker_id, &output).await?;
            queue::complete_task(conn, task_id, output).await
        }
        .scope_boxed()
    })
    .await
}

async fn persist_workflow_failure(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    error: &str,
) -> HarvestResult<()> {
    let error = error.to_string();
    conn.transaction::<(), HarvestError, _>(|conn| {
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
            update_workflow_execution_failed(conn, exec_id, worker_id, &error).await?;
            queue::fail_task(conn, task_id, &error).await
        }
        .scope_boxed()
    })
    .await
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

async fn persist_signal_wait_park(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    marker_events: &[WorkflowEvent],
    sticky: Option<queue::StickyHint<'_>>,
) -> HarvestResult<()> {
    // Park the workflow task (state=RUNNING, worker cleared) so it is not
    // confused with a timer-waiting task (state=PENDING). This ensures that
    // `wake_workflow_task` — which only targets RUNNING/parked rows — can
    // reliably distinguish signal waits from timer waits and will not
    // prematurely fire a pending timer when a signal is delivered.
    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, marker_events, next_event_id).await?;
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
    }
    Ok(())
}

async fn persist_activity_wait_park(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    commands: &[WorkflowCommand],
    activity_ids: &[ActivityExecId],
    sticky: Option<queue::StickyHint<'_>>,
) -> HarvestResult<()> {
    let marker_events = marker_events_from_commands(commands);

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
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    commands: &[WorkflowCommand],
    scheduled_activities: &[ScheduledActivityCommand],
    sticky: Option<queue::StickyHint<'_>>,
    execute_span: &tracing::Span,
    assigned_build_id: Option<&str>,
) -> HarvestResult<()> {
    let marker_events = marker_events_from_commands(commands);
    let mut events = marker_events;
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

        let effective_key = activity
            .concurrency_key
            .map(ToString::to_string)
            .or_else(|| activity.max_concurrent.map(|_| activity.name.to_string()));
        if let Some(key) = effective_key {
            params.concurrency_key = Some(key);
            params.max_concurrent = activity.max_concurrent;
        }

        events.push(WorkflowEvent::ActivityScheduled {
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
        enqueued.push(params);
    }

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &events, next_event_id).await?;
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

async fn persist_started_timer(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    next_event_id: i32,
    task_id: uuid::Uuid,
    commands: &[WorkflowCommand],
    timer: &StartedTimerCommand,
    sticky: Option<queue::StickyHint<'_>>,
) -> HarvestResult<()> {
    use tracing::Instrument;

    let marker_events = marker_events_from_commands(commands);
    let fire_delay = chrono_duration_from_secs(timer.duration_secs, "timer duration")?;
    let fires_at = chrono::Utc::now() + fire_delay;
    // Emit a span for the timer placement (not to be confused with
    // harvest.timer.fire which is emitted when the timer actually fires).
    let span = tracing::info_span!(
        "harvest.timer.start",
        "otel.kind" = "internal",
        timer.id = %timer.timer_id,
        timer.duration_secs = timer.duration_secs,
        { ATTR_EXECUTION_ID } = %exec_id,
    );
    let timer_started = WorkflowEvent::TimerStarted {
        timer_id: timer.timer_id.clone(),
        duration_secs: timer.duration_secs,
    };
    let mut events = marker_events;
    events.push(timer_started);

    let new_timer = NewHarvestTimer {
        workflow_exec_id: exec_id.as_uuid(),
        timer_id: timer.timer_id.as_str(),
        fires_at,
    };

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &events, next_event_id).await?;
            diesel::insert_into(harvest_timers::table)
                .values(&new_timer)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            queue::reschedule_task(conn, task_id, fires_at).await?;
            if sticky.is_some() {
                queue::set_task_sticky_affinity(conn, task_id, sticky).await?;
            }
            Ok(())
        }
        .scope_boxed()
    })
    .instrument(span)
    .await
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
    // Compute marker events outside the transaction (WorkflowCommand is not Clone).
    let marker_events = marker_events_from_commands(commands);
    let shard_id = parent_execution.shard_id;

    // Clone telemetry and execute_span before the transaction closure so they
    // can be used inside the async move block without capturing references.
    let telemetry = registry.telemetry().clone();
    let execute_span = execute_span.clone();

    conn.transaction::<(), HarvestError, _>(|conn| {
        let children = children.clone();
        let marker_events = marker_events.clone();
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

            // Append marker events + ChildWorkflowStarted for new children to parent.
            // Use append_single_event rather than append_events(…, next_event_id) so
            // that each insert re-reads MAX(event_id) under a parent-row FOR UPDATE
            // lock.  This serializes against concurrent ChildWorkflowCompleted/Failed
            // appends from sibling children that complete while this parent task is
            // still RUNNING, preventing a UNIQUE(workflow_exec_id, event_id) collision.
            let mut parent_events = marker_events;
            for child in &new_children {
                parent_events.push(WorkflowEvent::ChildWorkflowStarted {
                    child_id: child.child_id,
                    workflow_name: child.workflow_name.clone(),
                    input: child.input.clone(),
                });
            }
            for event in parent_events {
                store::append_single_event(conn, parent_exec_id, event).await?;
            }

            // Insert rows and enqueue tasks for new children.
            for child in &new_children {
                let child_workflow_id = child.child_id.to_string();
                let child_row = NewWorkflowExecution {
                    id: child.child_id.as_uuid(),
                    workflow_name: &child.workflow_name,
                    workflow_id: &child_workflow_id,
                    run_id: uuid::Uuid::new_v4(),
                    shard_id,
                    input: child.input.clone(),
                    parent_id: Some(parent_exec_id.as_uuid()),
                    queue_name: &queue_name,
                    execution_timeout: None,
                    deadline_at: None,
                    memo: None,
                    search_attrs: None,
                    assigned_build_id: parent_execution.assigned_build_id.clone(),
                };
                let child_started_event = WorkflowEvent::WorkflowStarted {
                    input: child.input.clone(),
                    timestamp: chrono::Utc::now(),
                };
                let mut params = queue::EnqueueParams::new(
                    queue_name.clone(),
                    TaskType::Workflow,
                    child.input.clone(),
                );
                params.workflow_exec_id = Some(child.child_id.as_uuid());
                params.required_build_id = parent_execution.assigned_build_id.clone();
                // Resolve per-key concurrency policy for the child workflow (issue #247).
                (params.concurrency_key, params.max_concurrent) = registry
                    .workflows
                    .get(&child.workflow_name)
                    .and_then(|info| info.concurrency.as_ref())
                    .map_or((None, None), |policy| {
                        let key = crate::concurrency::resolve_concurrency_key(
                            policy.key_expr,
                            &child.input,
                        );
                        (key, Some(policy.limit))
                    });
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

async fn ingest_pending_signals(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    next_event_id: i32,
) -> HarvestResult<Vec<String>> {
    let pending_signals = signal::load_pending_signals(conn, exec_id).await?;
    if pending_signals.is_empty() {
        return Ok(vec![]);
    }

    let (signal_ids, signals_data): (Vec<_>, Vec<_>) = pending_signals
        .into_iter()
        .map(|signal| {
            let name = signal.signal_name.clone();
            let event = WorkflowEvent::SignalReceived {
                signal_name: signal.signal_name,
                payload: signal.payload,
            };
            (signal.id, (name, event))
        })
        .unzip();

    let (signal_names, signal_events): (Vec<_>, Vec<_>) = signals_data.into_iter().unzip();

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &signal_events, next_event_id).await?;
            signal::mark_signals_consumed(conn, &signal_ids).await?;
            Ok(())
        }
        .scope_boxed()
    })
    .await?;

    Ok(signal_names)
}

async fn ingest_fired_timers(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    next_event_id: i32,
) -> HarvestResult<Vec<TimerId>> {
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

    if due_timers.is_empty() {
        return Ok(vec![]);
    }

    let (timer_row_ids, timer_events_and_ids): (Vec<_>, Vec<_>) = due_timers
        .into_iter()
        .map(|timer| {
            let timer_id = TimerId::new(timer.timer_id);
            (
                timer.id,
                (timer_id.clone(), WorkflowEvent::TimerFired { timer_id }),
            )
        })
        .unzip();

    let (fired_timer_ids, timer_events): (Vec<_>, Vec<_>) =
        timer_events_and_ids.into_iter().unzip();

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &timer_events, next_event_id).await?;
            diesel::update(dsl::harvest_timers.filter(dsl::id.eq_any(&timer_row_ids)))
                .set(dsl::fired.eq(true))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(())
        }
        .scope_boxed()
    })
    .await?;

    Ok(fired_timer_ids)
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
            update_workflow_execution_failed(conn, exec_id, worker_id, error).await?;
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

async fn persist_child_workflow_completion(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    parent_exec_id: ExecutionId,
    output: serde_json::Value,
) -> HarvestResult<()> {
    let event = WorkflowEvent::WorkflowCompleted {
        output: output.clone(),
    };

    conn.transaction::<(), HarvestError, _>(|conn| {
        let output = output.clone();
        async move {
            store::append_events(conn, exec_id, &[event], next_event_id).await?;
            update_workflow_execution_completed(conn, exec_id, worker_id, &output).await?;
            queue::complete_task(conn, task_id, output.clone()).await?;
            wake_parent_for_child_completion(conn, parent_exec_id, exec_id, output).await
        }
        .scope_boxed()
    })
    .await
}

async fn persist_child_workflow_failure(
    conn: &mut AsyncPgConnection,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    worker_id: &str,
    parent_exec_id: ExecutionId,
    error: &str,
) -> HarvestResult<()> {
    let workflow_failure = WorkflowEvent::WorkflowFailed {
        error: error.to_string(),
    };

    conn.transaction::<(), HarvestError, _>(|conn| {
        let error = error.to_string();
        async move {
            store::append_events(conn, exec_id, &[workflow_failure], next_event_id).await?;
            update_workflow_execution_failed(conn, exec_id, worker_id, &error).await?;
            queue::fail_task(conn, task_id, &error).await?;
            wake_parent_for_child_failure(conn, parent_exec_id, exec_id, &error).await
        }
        .scope_boxed()
    })
    .await
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
) -> HarvestResult<()> {
    match activity_result {
        Ok(output) => finalize_activity_completion(conn, task, exec_id, activity_id, output).await,
        Err(error) => {
            let delay_result = next_retry_delay(task, &error, retry_policy);
            let delay = fail_execution_on_error(conn, task, worker_id, delay_result).await?;

            if let Some(delay) = delay {
                return queue::requeue_for_retry(conn, task.id, delay).await;
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

#[allow(clippy::too_many_lines)]
async fn process_activity_task(
    pool: &DbPool,
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task: &TaskQueueItem,
    worker_id: &str,
    cancellation_grace_period: Duration,
) -> HarvestResult<()> {
    let Some(exec_uuid) = task.workflow_exec_id else {
        return fail_task_only(conn, task.id, "activity task missing workflow_exec_id").await;
    };
    let Some(activity_name) = task.activity_name.as_deref() else {
        return fail_task_only(conn, task.id, "activity task missing activity_name").await;
    };
    let exec_id = execution_id_from_uuid(exec_uuid);

    let Some(activity) = registry.activities.get(activity_name) else {
        let error = format!("no activity handler registered for '{activity_name}'");
        fail_task_and_execution(conn, task, worker_id, &error).await?;
        return Err(HarvestError::Config(error));
    };

    let started_result =
        append_activity_started_if_pending(conn, task, exec_id, activity_name, worker_id).await;
    let Some(activity_id) = fail_execution_on_error(conn, task, worker_id, started_result).await?
    else {
        return Ok(());
    };

    let cancel = CancellationToken::new();
    let heartbeat_tx =
        crate::heartbeat::spawn_heartbeat_flusher(task.id, pool.clone(), cancel.clone());
    let trace_carrier = task
        .trace_context
        .as_ref()
        .and_then(TraceContextCarrier::from_json);
    let ctx = ActivityContext::new_with_cancellation_check(
        registry.shared_state(),
        Some(heartbeat_tx),
        task.heartbeat_details.clone(),
        cancel.clone(),
        task.id,
        pool.clone(),
    )
    .with_trace_context(trace_carrier.clone())
    .with_idempotency_key(IdempotencyKey::from_activity_exec_id(activity_id))
    .with_attempt(task_attempt(task));

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

    let retry_policy_result = configured_retry_policy(task);
    let retry_policy = fail_execution_on_error(conn, task, worker_id, retry_policy_result).await?;

    handle_activity_result(
        conn,
        task,
        exec_id,
        activity_id,
        worker_id,
        retry_policy.as_ref(),
        activity_result,
    )
    .await
}

async fn persist_scheduled_external_activity(
    conn: &mut AsyncPgConnection,
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
                let marker_events = marker_events_from_commands(commands);
                for event in marker_events {
                    store::append_single_event(conn, exec_id, event).await?;
                }

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

    let marker_events = marker_events_from_commands(commands);
    let awaiting_event = WorkflowEvent::ActivityAwaitingExternal {
        activity_id: scheduled.activity_id,
        token: scheduled.token,
        name: scheduled.name.clone(),
        input: scheduled.input.clone(),
        queue: scheduled.queue.clone(),
        schedule_to_close_secs: scheduled.schedule_to_close_secs,
    };
    let mut events = marker_events;
    events.push(awaiting_event);

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &events, next_event_id).await?;
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

    let sticky = context.persistence.sticky_hint();

    let result = if should_requeue_signal_wait(commands) {
        let marker_events = marker_events_from_commands(commands);
        persist_signal_wait_park(
            conn,
            context.persistence.task.id,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            &marker_events,
            sticky,
        )
        .await
    } else if let Some(scheduled) = extract_all_scheduled_activities(commands) {
        persist_scheduled_activities(
            conn,
            registry,
            context.persistence.task.id,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            commands,
            &scheduled,
            sticky,
            context.execute_span,
            context.execution.assigned_build_id.as_deref(),
        )
        .await
    } else if let Some(activity_ids) = extract_all_activity_waits(commands) {
        persist_activity_wait_park(
            conn,
            context.persistence.task.id,
            context.persistence.exec_id,
            commands,
            &activity_ids,
            sticky,
        )
        .await
    } else if let Some(timer) = extract_single_started_timer(commands) {
        let res = persist_started_timer(
            conn,
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

    let timers_result = ingest_fired_timers(conn, exec_id, initial_history.next_event_id).await;
    let timers_fired = fail_execution_on_error(conn, task, worker_id, timers_result).await?;

    let history_after_timers_result = store::load_history(conn, exec_id).await;
    let history_after_timers =
        fail_execution_on_error(conn, task, worker_id, history_after_timers_result).await?;

    let signals_result =
        ingest_pending_signals(conn, exec_id, history_after_timers.next_event_id).await;
    let signals_delivered = fail_execution_on_error(conn, task, worker_id, signals_result).await?;

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

        let timers_result = ingest_fired_timers(conn, exec_id, existing_delta.next_event_id).await;
        let timers_fired = fail_execution_on_error(conn, task, worker_id, timers_result).await?;

        // Load events appended by timer ingestion.
        let after_timers_result =
            store::load_history_since(conn, exec_id, existing_delta.next_event_id).await;
        let after_timers =
            fail_execution_on_error(conn, task, worker_id, after_timers_result).await?;

        let signals_result =
            ingest_pending_signals(conn, exec_id, after_timers.next_event_id).await;
        let signals_delivered =
            fail_execution_on_error(conn, task, worker_id, signals_result).await?;

        // Load events appended by signal ingestion.
        let after_signals_result =
            store::load_history_since(conn, exec_id, after_timers.next_event_id).await;
        let after_signals =
            fail_execution_on_error(conn, task, worker_id, after_signals_result).await?;

        // Reconstruct full history: cached snapshot + any pre-existing delta +
        // timer events + signal events.
        let mut history_events = cached_state.events.clone();
        history_events.extend(existing_delta.events);
        history_events.extend(after_timers.events);
        history_events.extend(after_signals.events);
        let next_event_id = after_signals.next_event_id;

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
async fn persist_workflow_continue_as_new(
    conn: &mut AsyncPgConnection,
    persistence: WorkflowTaskPersistence<'_>,
    execution: &WorkflowExecution,
    input: serde_json::Value,
) -> HarvestResult<()> {
    use crate::schema::{harvest_signals, harvest_workflow_executions};

    if let Some(parent_exec_id) = execution.parent_id.map(execution_id_from_uuid) {
        let error =
            "continue_as_new is not supported in child workflows in this release".to_string();
        return persist_child_workflow_failure(
            conn,
            persistence.task.id,
            persistence.exec_id,
            persistence.next_event_id,
            persistence.worker_id,
            parent_exec_id,
            &error,
        )
        .await;
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
    };
    let continued_event = WorkflowEvent::WorkflowContinuedAsNew {
        new_exec_id,
        input: input.clone(),
    };
    // Re-anchor deadline to the new execution's start time (issue #243).
    let new_deadline_at = execution.execution_timeout.map(|d| chrono::Utc::now() + d);

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
        memo: execution.memo.clone(),
        search_attrs: execution.search_attrs.clone(),
        assigned_build_id: execution.assigned_build_id.clone(),
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

async fn persist_workflow_outcome(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    execution: &WorkflowExecution,
    persistence: WorkflowTaskPersistence<'_>,
    outcome: WorkflowOutcome,
    execute_span: &tracing::Span,
) -> HarvestResult<()> {
    let parent_exec_id = execution.parent_id.map(execution_id_from_uuid);

    match (outcome, parent_exec_id) {
        (WorkflowOutcome::Completed { output }, Some(parent_id)) => {
            persist_child_workflow_completion(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                parent_id,
                output,
            )
            .await
        }
        (WorkflowOutcome::Completed { output }, None) => {
            persist_workflow_completion(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                output,
            )
            .await
        }
        (WorkflowOutcome::Failed { error }, Some(parent_id)) => {
            persist_child_workflow_failure(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                parent_id,
                &error,
            )
            .await
        }
        (WorkflowOutcome::Failed { error }, None) => {
            persist_workflow_failure(
                conn,
                persistence.task.id,
                persistence.exec_id,
                persistence.next_event_id,
                persistence.worker_id,
                &error,
            )
            .await
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
            let result =
                persist_workflow_continue_as_new(conn, persistence, execution, input).await;
            fail_execution_on_error(conn, persistence.task, persistence.worker_id, result).await
        }
    }
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

fn marker_event_count(commands: &[WorkflowCommand]) -> u64 {
    u64::try_from(
        commands
            .iter()
            .filter(|cmd| matches!(cmd, WorkflowCommand::RecordMarker { .. }))
            .count(),
    )
    .unwrap_or(u64::MAX)
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
    commands: &[WorkflowCommand],
) -> HarvestResult<u64> {
    let update_events = pending_update_result_event_count(commands);
    let marker_events = marker_event_count(commands);
    let bookkeeping_events = update_events.saturating_add(marker_events);

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
    if extract_single_started_timer(commands).is_some() {
        return Ok(bookkeeping_events.saturating_add(1));
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
) -> HarvestResult<()> {
    let reason = reason.to_string();

    conn.transaction::<(), HarvestError, _>(|conn| {
        let reason = reason.clone();
        async move {
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
            update_workflow_execution_failed(conn, exec_id, worker_id, &reason).await?;
            queue::fail_task(conn, task.id, &reason).await?;
            if let Some(parent_exec_id) = parent_exec_id {
                wake_parent_for_child_failure(conn, parent_exec_id, exec_id, &reason).await?;
            }
            Ok(())
        }
        .scope_boxed()
    })
    .await
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
) -> HarvestResult<()> {
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

#[allow(clippy::too_many_lines)]
async fn process_workflow_task(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task: &TaskQueueItem,
    worker_id: &str,
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
            shard_id: i64::from(prepared.execution.shard_id),
            queue_name: task.queue_name.clone(),
            is_replay,
            link_traceparent: trace_carrier
                .as_ref()
                .filter(|_| is_replay)
                .and_then(|c| c.link_traceparent.clone().or_else(|| c.traceparent.clone())),
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

        let (run_outcome, pending_cmds, execute_span) = run_workflow_with_state_and_history_policy(
            prepared.exec_id,
            history_events.clone(),
            workflow.handler,
            task.input.clone(),
            registry.shared_state(),
            registry.history_policy(),
            Some(&span_meta),
            &dq,
            &du,
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
                // Sync in-memory snapshot so a subsequent continue_as_new in the
                // same task copies the patched attrs to the successor row.
                prepared.execution.search_attrs = apply_search_attrs_patch_in_memory(
                    prepared.execution.search_attrs.take(),
                    &commands,
                );
                // Local-activity re-run: drop this iteration's execute span
                // so the OTel span closes before we start inline execution.
                drop(execute_span);
                // If the batch also contains SignalExternalWorkflow commands,
                // write their history events BEFORE the local-activity events.
                // This preserves correct replay ordering: on the next run
                // drain_early_signals stashes the signal events so
                // match_external_signal sees them before LocalActivityScheduled.
                let commands = if commands
                    .iter()
                    .any(|c| matches!(c, WorkflowCommand::SignalExternalWorkflow { .. }))
                {
                    let (signal_items, remaining) = split_mixed_signal_batch(commands);
                    if !signal_items.is_empty() {
                        let new_events = match persist_external_signal_inline(
                            conn,
                            prepared.exec_id,
                            signal_items,
                            &mut next_event_id,
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
                            return fail_workflow_for_history_cap(
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
                            .await;
                        }
                    }
                    remaining
                } else {
                    commands
                };
                let (markers, local_run) = extract_run_local_activity(commands);
                let inline_outcome = run_local_activity_inline(
                    conn,
                    registry,
                    prepared.exec_id,
                    markers,
                    local_run,
                    max_local_activity_start_to_close,
                    &mut next_event_id,
                )
                .await?;
                let new_events = match inline_outcome {
                    LocalActivityInlineOutcome::Complete(events) => events,
                    LocalActivityInlineOutcome::HistoryCapReached {
                        events,
                        event_count,
                    } => {
                        history_events.extend(events);
                        return fail_workflow_for_history_cap(
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
                        .await;
                    }
                };
                history_events.extend(new_events);
                let current_history_event_count =
                    u64::try_from(history_events.len()).unwrap_or(u64::MAX);
                if let Some(cap) = registry.history_policy().event_hard_cap()
                    && current_history_event_count >= cap
                {
                    return fail_workflow_for_history_cap(
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
                    .await;
                }
            }
            WorkflowOutcome::Suspended { commands }
                if commands
                    .iter()
                    .any(|c| matches!(c, WorkflowCommand::SignalExternalWorkflow { .. }))
                    && commands.iter().all(|c| {
                        matches!(
                            c,
                            WorkflowCommand::SignalExternalWorkflow { .. }
                                | WorkflowCommand::RecordMarker { .. }
                                | WorkflowCommand::RecordUpdateResult { .. }
                                | WorkflowCommand::UpsertSearchAttributes { .. }
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
                prepared.execution.search_attrs = apply_search_attrs_patch_in_memory(
                    prepared.execution.search_attrs.take(),
                    &commands,
                );
                drop(execute_span);
                let items = extract_signal_external_workflow(commands);
                let new_events = match persist_external_signal_inline(
                    conn,
                    prepared.exec_id,
                    items,
                    &mut next_event_id,
                )
                .await
                {
                    Ok(events) => events,
                    Err(e) => {
                        return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e))
                            .await;
                    }
                };
                history_events.extend(new_events);
                let current_history_event_count =
                    u64::try_from(history_events.len()).unwrap_or(u64::MAX);
                if let Some(cap) = registry.history_policy().event_hard_cap()
                    && current_history_event_count >= cap
                {
                    return fail_workflow_for_history_cap(
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
                    .await;
                }
            }
            // Mixed batch: contains SignalExternalWorkflow AND other durable commands
            // (ScheduleActivity, StartTimer, etc.). The "all signals" guard above did
            // not match because not all commands are signals/markers. Write signal events
            // to history FIRST (so drain_early_signals stashes them on the next replay
            // pass), then break with the remaining commands for handle_suspended_workflow.
            WorkflowOutcome::Suspended { commands }
                if commands
                    .iter()
                    .any(|c| matches!(c, WorkflowCommand::SignalExternalWorkflow { .. })) =>
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
                )
                .await
                {
                    Ok(events) => events,
                    Err(e) => {
                        return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(e))
                            .await;
                    }
                };
                history_events.extend(new_events);
                let current_history_event_count =
                    u64::try_from(history_events.len()).unwrap_or(u64::MAX);
                if let Some(cap) = registry.history_policy().event_hard_cap()
                    && current_history_event_count >= cap
                {
                    return fail_workflow_for_history_cap(
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
                    .await;
                }
                // Re-acquire a fresh execute_span so persist_workflow_outcome
                // (via handle_suspended_workflow) gets a valid span reference.
                // The original span was dropped above.
                let execute_span = tracing::Span::none();
                break (
                    WorkflowOutcome::Suspended {
                        commands: remaining_commands,
                    },
                    pending_cmds,
                    execute_span,
                );
            }
            other => break (other, pending_cmds, execute_span),
        }
    };

    let (outcome, pending_cmds, execute_span) = loop_result;
    let pending_durable_event_count = match &outcome {
        WorkflowOutcome::Suspended { commands } => {
            match suspended_command_event_count(conn, commands).await {
                Ok(count) => count,
                Err(error) => {
                    return fail_execution_on_error(conn, task, worker_id, Err::<(), _>(error))
                        .await;
                }
            }
        }
        _ => pending_update_result_event_count(&pending_cmds),
    };
    let current_history_event_count = u64::try_from(history_events.len())
        .unwrap_or(u64::MAX)
        .saturating_add(pending_durable_event_count);

    if let Some(cap) = registry.history_policy().event_hard_cap()
        && current_history_event_count >= cap
        && !matches!(&outcome, WorkflowOutcome::ContinuedAsNew { .. })
    {
        return fail_workflow_for_history_cap(
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
        .await;
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
            terminal_history_event_count(next_event_id, &pending_cmds),
        );
    }
    if matches!(&outcome, WorkflowOutcome::ContinuedAsNew { .. }) {
        telemetry
            .metrics
            .record_workflow_continue_as_new(&prepared.execution.workflow_name);
    }

    // Append UpdateCompleted/UpdateFailed events and apply search-attribute
    // patches for any commands emitted during this live execution cycle before
    // the terminal event.  For Suspended outcomes these commands are inside the
    // variant and are handled inside handle_suspended_workflow; pending_cmds is
    // only non-empty for Completed/Failed/ContinuedAsNew outcomes.
    if !pending_cmds.is_empty() {
        persist_update_result_commands(conn, prepared.exec_id, &pending_cmds, &mut next_event_id)
            .await?;
        persist_search_attrs_from_commands(conn, prepared.exec_id, &pending_cmds).await?;
        // Keep the in-memory execution snapshot current so that
        // persist_workflow_continue_as_new copies the patched attrs to the
        // successor row rather than the stale pre-patch snapshot.
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

    persist_workflow_outcome(
        conn,
        registry,
        &prepared.execution,
        WorkflowTaskPersistence {
            task,
            worker_id,
            exec_id: prepared.exec_id,
            next_event_id,
            sticky_timeout,
        },
        outcome,
        &execute_span,
    )
    .await?;
    // execute_span is dropped here, closing the OTel span after all producer
    // spans have been emitted as its children.

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
                sticky_timeout,
                max_local_activity_start_to_close,
                workflow_cache,
            )
            .await
        }
        ClaimedTaskKind::Activity => {
            process_activity_task(
                pool,
                &mut conn,
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
}

struct WorkerMonitoringHandles {
    queue_depth_sampler: tokio::task::JoinHandle<()>,
    concurrency_sampler: tokio::task::JoinHandle<()>,
    dlq_depth_samplers: Vec<tokio::task::JoinHandle<()>>,
    timeout_checker: tokio::task::JoinHandle<()>,
}

impl Worker {
    /// Create a new worker from validated config and a handler registry.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::Config`] if the config fails validation.
    pub fn new(config: WorkerRuntimeConfig, registry: Arc<HandlerRegistry>) -> HarvestResult<Self> {
        config.validate()?;

        let workflow_semaphore = Arc::new(Semaphore::new(config.max_concurrent_workflows));
        let activity_semaphore = Arc::new(Semaphore::new(config.max_concurrent_activities));
        let workflow_cache = Arc::new(tokio::sync::Mutex::new(crate::cache::WorkflowCache::new(
            config.workflow_cache_size,
        )));

        Ok(Self {
            config,
            registry,
            workflow_semaphore,
            activity_semaphore,
            shutdown: CancellationToken::new(),
            remote_drain_deadline: Arc::new(Mutex::new(None)),
            workflow_cache,
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
        let timeout_checker = crate::timeout::spawn_timeout_checker(
            pool.clone(),
            self.shutdown.clone(),
            self.config.poll_interval,
            self.registry.telemetry().clone(),
        );

        WorkerMonitoringHandles {
            queue_depth_sampler,
            concurrency_sampler,
            dlq_depth_samplers,
            timeout_checker,
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
        for sampler in monitors.dlq_depth_samplers {
            if let Err(error) = sampler.await {
                tracing::warn!(
                    worker_id = %self.config.worker_id,
                    error = %error,
                    "dlq depth sampler failed during shutdown"
                );
            }
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

        match queue::claim_task(
            &mut conn,
            &self.config.queues,
            &self.config.worker_id,
            &self.config.build_id,
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
                self.dispatch_task(task, pool);
                true
            }
            Ok(None) => false,
            Err(e) => {
                tracing::error!(error = %e, "failed to claim task");
                false
            }
        }
    }

    /// Spawn a bounded Tokio task for the claimed work item.
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
        let cancellation_grace_period = self.config.cancellation_grace_period;
        let sticky_timeout = self.config.sticky_timeout;
        let max_local_activity_start_to_close = self.config.max_local_activity_start_to_close;
        let workflow_cache = Arc::clone(&self.workflow_cache);

        tokio::spawn(async move {
            // Acquire semaphore permit — blocks if at concurrency limit.
            let Ok(_permit) = semaphore.acquire().await else {
                tracing::error!(task_id = %task_id, "semaphore closed");
                return;
            };

            tracing::info!(
                task_id = %task_id,
                task_type = %task_type,
                worker_id = %worker_id,
                "executing task"
            );

            if let Err(error) = process_task(
                &pool,
                registry,
                task,
                &worker_id,
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
// Tests (unit, no DB)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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
        }
    }

    #[test]
    fn worker_config_validates() {
        let cfg = default_runtime_config();
        assert!(cfg.validate().is_ok());
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
            concurrency: None,
        };

        let act = ActivityInfo {
            name: "send_email",
            module: "app::activities",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: false,
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
}
