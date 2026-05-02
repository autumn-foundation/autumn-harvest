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
use std::sync::Arc;
use std::time::Duration;

use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use scoped_futures::ScopedFutureExt;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::builder::WorkerConfig;
use crate::context::{ActivityContext, SharedState, WorkflowCommand, empty_shared_state};
use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::executor::{WorkflowExecuteSpanMeta, WorkflowOutcome, run_workflow_with_state};
use crate::external_task;
use crate::info::{ActivityInfo, WorkflowInfo};
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
use crate::types::{ActivityExecId, ExecutionId, ExternalActivityToken, TimerId, WorkerId};

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
    /// Shared typed state visible to workflow and activity handlers.
    state: SharedState,
    /// Telemetry bundle (trace-context propagator + metrics recorder) applied
    /// around every dispatch.
    telemetry: Arc<crate::telemetry::TelemetryConfig>,
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
            state,
            telemetry,
        }
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
}

impl std::fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerRegistry")
            .field("workflows", &self.workflows.keys())
            .field("activities", &self.activities.keys())
            .field("state_count", &self.state.len())
            .field("telemetry", &self.telemetry)
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
        WorkflowCommand::ScheduleExternalActivity { .. } => "ScheduleExternalActivity",
        WorkflowCommand::StartTimer { .. } => "StartTimer",
        WorkflowCommand::StartChildWorkflow { .. } => "StartChildWorkflow",
        WorkflowCommand::RecordMarker { .. } => "RecordMarker",
        WorkflowCommand::WaitForSignal { .. } => "WaitForSignal",
        WorkflowCommand::Complete { .. } => "Complete",
        WorkflowCommand::Fail { .. } => "Fail",
        WorkflowCommand::ContinueAsNew { .. } => "ContinueAsNew",
        WorkflowCommand::RunLocalActivity { .. } => "RunLocalActivity",
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
    let only_wait_or_marker = commands.iter().all(|cmd| {
        matches!(
            cmd,
            WorkflowCommand::WaitForSignal { .. } | WorkflowCommand::RecordMarker { .. }
        )
    });

    has_wait && only_wait_or_marker
}

#[derive(Debug, Clone)]
struct ScheduledActivityCommand {
    activity_id: ActivityExecId,
    name: String,
    input: serde_json::Value,
    queue: String,
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
    let mut iter = commands
        .iter()
        .filter(|cmd| !matches!(cmd, WorkflowCommand::RecordMarker { .. }));

    let first_cmd = iter.next()?;

    // Original behavior: return None if there's more than one non-marker command.
    if iter.next().is_some() {
        return None;
    }

    // Original behavior: extractor(cmd)? means we return None if the extractor yields None.
    extractor(first_cmd)
}

fn extract_single_schedule_activity(
    commands: &[WorkflowCommand],
) -> Option<ScheduledActivityCommand> {
    extract_single_command(commands, |cmd| {
        let WorkflowCommand::ScheduleActivity {
            activity_id,
            name,
            input,
            queue,
            ..
        } = cmd
        else {
            return None;
        };

        Some(ScheduledActivityCommand {
            activity_id: *activity_id,
            name: name.clone(),
            input: input.clone(),
            queue: queue.clone(),
        })
    })
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

/// Extract all `StartChildWorkflow` commands when every non-marker command is
/// a child-workflow start.  Returns `Some(children)` (may have length > 1 for
/// parallel spawns) or `None` if any non-marker command is of a different type.
fn extract_all_started_child_workflows(
    commands: &[WorkflowCommand],
) -> Option<Vec<StartedChildWorkflowCommand>> {
    let non_markers: Vec<&WorkflowCommand> = commands
        .iter()
        .filter(|c| !matches!(c, WorkflowCommand::RecordMarker { .. }))
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
    let mut markers = Vec::new();
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
            } => {
                drop(result_tx); // coroutine already dropped; close the channel
                local_run = Some(LocalActivityRun {
                    activity_id,
                    name,
                    input,
                    start_to_close_secs,
                    retry_policy,
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

/// Run a local activity inline, appending durability events to `harvest_events`.
///
/// Retries the handler up to `max_attempts` times (per the retry policy),
/// sleeping the computed backoff between attempts. Each attempt appends a
/// `LocalActivityFailed` event; on success a `LocalActivityCompleted` event is
/// appended. Returns all newly-appended events so the caller can extend its
/// in-memory replay history and avoid a DB round-trip.
async fn run_local_activity_inline(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    exec_id: ExecutionId,
    marker_events: Vec<WorkflowEvent>,
    run: LocalActivityRun,
    max_start_to_close: Duration,
    next_event_id: &mut i32,
) -> HarvestResult<Vec<WorkflowEvent>> {
    let activity = registry.activities.get(&run.name).ok_or_else(|| {
        HarvestError::Config(format!("no activity handler registered for '{}'", run.name))
    })?;

    let per_attempt_timeout = run
        .start_to_close_secs
        .map_or(max_start_to_close, Duration::from_secs)
        .min(max_start_to_close);

    let max_attempts = run.retry_policy.as_ref().map_or(1, |p| p.max_attempts);

    let scheduled_event = WorkflowEvent::LocalActivityScheduled {
        activity_id: run.activity_id,
        name: run.name.clone(),
        input: run.input.clone(),
    };

    // Append marker events + scheduled event in a single call.
    let mut prefix_events = marker_events;
    prefix_events.push(scheduled_event);
    store::append_events(conn, exec_id, &prefix_events, *next_event_id).await?;
    *next_event_id += i32::try_from(prefix_events.len())
        .map_err(|_| HarvestError::Config("event count overflow".into()))?;

    let mut all_new_events = prefix_events;
    let ctx = ActivityContext::new(registry.shared_state(), None, CancellationToken::new());
    let handler = activity.handler;

    for attempt in 1..=max_attempts {
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
                return Ok(all_new_events);
            }
            Err(error) => {
                let failed_event = WorkflowEvent::LocalActivityFailed {
                    activity_id: run.activity_id,
                    error: error.clone(),
                    attempt,
                };
                store::append_events(
                    conn,
                    exec_id,
                    std::slice::from_ref(&failed_event),
                    *next_event_id,
                )
                .await?;
                *next_event_id += 1;
                all_new_events.push(failed_event);

                if attempt < max_attempts
                    && let Some(delay) = run
                        .retry_policy
                        .as_ref()
                        .and_then(|p| p.next_delay(attempt))
                {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Ok(all_new_events)
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

fn chrono_duration_from_secs(seconds: u64, field_name: &str) -> HarvestResult<chrono::Duration> {
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
    if let Some(policy) = retry_policy {
        if policy
            .non_retryable_errors
            .iter()
            .any(|non_retryable| non_retryable == error)
        {
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

#[allow(clippy::too_many_arguments)]
async fn persist_scheduled_activity(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    commands: &[WorkflowCommand],
    scheduled: &ScheduledActivityCommand,
    sticky: Option<queue::StickyHint<'_>>,
    execute_span: &tracing::Span,
) -> HarvestResult<()> {
    let activity = registry.activities.get(&scheduled.name).ok_or_else(|| {
        HarvestError::Config(format!(
            "no activity handler registered for '{}'",
            scheduled.name
        ))
    })?;
    let marker_events = marker_events_from_commands(commands);

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
    // trace_context is set below, inside the harvest.activity.schedule span,
    // so the downstream worker's harvest.activity.execute span is stitched to
    // the producer span rather than the parent workflow-execute context.

    if let Some(retry_policy) = activity.default_retry_policy.clone() {
        params.max_attempts = i32::try_from(retry_policy.max_attempts).map_err(|_| {
            HarvestError::Config(format!(
                "activity '{}' retry policy max_attempts exceeds i32 range",
                activity.name
            ))
        })?;
        params.retry_policy = Some(serde_json::to_value(retry_policy)?);
    }

    if let Some(timeout) = activity.default_heartbeat_timeout {
        params.heartbeat_timeout = Some(chrono_duration_from_std(timeout, "heartbeat timeout")?);
    }
    if let Some(timeout) = activity.default_start_to_close {
        params.start_to_close = Some(chrono_duration_from_std(timeout, "start_to_close timeout")?);
    }
    if let Some(timeout) = activity.default_schedule_to_start {
        params.schedule_to_start = Some(chrono_duration_from_std(
            timeout,
            "schedule_to_start timeout",
        )?);
    }
    // When max_concurrent is set but concurrency_key is omitted, default the
    // key to the activity name so the per-activity cap is enforced correctly.
    let effective_key = activity
        .concurrency_key
        .map(ToString::to_string)
        .or_else(|| activity.max_concurrent.map(|_| activity.name.to_string()));
    if let Some(key) = effective_key {
        params.concurrency_key = Some(key);
        params.max_concurrent = activity.max_concurrent;
    }

    let activity_events = vec![WorkflowEvent::ActivityScheduled {
        activity_id: scheduled.activity_id,
        name: scheduled.name.clone(),
        input: scheduled.input.clone(),
        queue: queue_name.clone(),
    }];
    let mut events = marker_events;
    events.extend(activity_events);

    // ADR-0001 §2.4: harvest.activity.schedule — PRODUCER, child of the
    // harvest.workflow.execute span for this cycle.  Using `parent: execute_span`
    // explicitly parents the span even though the execute span's instrumented
    // future has already completed; the handle is still open.  Context is
    // captured INSIDE in_scope so the queued task links back to this producer.
    params.trace_context = tracing::info_span!(
        parent: execute_span,
        "harvest.activity.schedule",
        "otel.kind" = "producer",
        { ATTR_ACTIVITY_NAME } = %scheduled.name,
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_QUEUE } = %queue_name,
    )
    .in_scope(|| registry.telemetry().capture_trace_context());

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &events, next_event_id).await?;
            queue::enqueue(conn, &params).await?;
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
                    memo: None,
                    search_attrs: None,
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
                        "COMPLETED" | "FAILED" | "TIMED_OUT" | "CANCELLED"
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
    next_event_id: i32,
    activity_id: ActivityExecId,
    output: serde_json::Value,
) -> HarvestResult<()> {
    let completion_event = WorkflowEvent::ActivityCompleted {
        activity_id,
        output: output.clone(),
    };

    conn.transaction::<(), HarvestError, _>(|conn| {
        let output = output.clone();
        async move {
            store::append_events(conn, exec_id, &[completion_event], next_event_id).await?;
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
    next_event_id: i32,
    activity_id: ActivityExecId,
    error: &str,
) -> HarvestResult<()> {
    let failed_event = WorkflowEvent::ActivityFailed {
        activity_id,
        error: error.to_string(),
        attempt: task_attempt(task),
    };

    conn.transaction::<(), HarvestError, _>(|conn| {
        let error = error.to_string();
        async move {
            store::append_events(conn, exec_id, &[failed_event], next_event_id).await?;
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
    next_event_id: i32,
    activity_id: ActivityExecId,
    worker_id: &str,
    retry_policy: Option<&crate::policy::RetryPolicy>,
    activity_result: Result<serde_json::Value, String>,
) -> HarvestResult<()> {
    match activity_result {
        Ok(output) => {
            finalize_activity_completion(conn, task, exec_id, next_event_id, activity_id, output)
                .await
        }
        Err(error) => {
            let delay_result = next_retry_delay(task, &error, retry_policy);
            let delay = fail_execution_on_error(conn, task, worker_id, delay_result).await?;

            if let Some(delay) = delay {
                return queue::requeue_for_retry(conn, task.id, delay).await;
            }

            finalize_activity_failure(conn, task, exec_id, next_event_id, activity_id, &error).await
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

    let history_result = store::load_history(conn, exec_id).await;
    let history = fail_execution_on_error(conn, task, worker_id, history_result).await?;

    let activity_id_result = find_pending_scheduled_activity(&history.events, activity_name);
    let activity_id = fail_execution_on_error(conn, task, worker_id, activity_id_result).await?;

    let started_event = WorkflowEvent::ActivityStarted {
        activity_id,
        worker_id: WorkerId::new(worker_id),
    };
    let append_result =
        store::append_events(conn, exec_id, &[started_event], history.next_event_id).await;
    fail_execution_on_error(conn, task, worker_id, append_result).await?;

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
        cancel.clone(),
        task.id,
        pool.clone(),
    )
    .with_trace_context(trace_carrier.clone());

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
    telemetry.metrics.record_activity_completed(
        activity_name,
        &task.queue_name,
        duration_secs,
        status,
    );
    cancel.cancel();
    drop(activity_future);

    let retry_policy_result = configured_retry_policy(task);
    let retry_policy = fail_execution_on_error(conn, task, worker_id, retry_policy_result).await?;

    handle_activity_result(
        conn,
        task,
        exec_id,
        history.next_event_id + 1,
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
    context: SuspendedWorkflowContext<'_>,
    commands: &[WorkflowCommand],
) -> HarvestResult<()> {
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
    } else if let Some(scheduled) = extract_single_schedule_activity(commands) {
        persist_scheduled_activity(
            conn,
            registry,
            context.persistence.task.id,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            commands,
            &scheduled,
            sticky,
            context.execute_span,
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

async fn prepare_workflow_task(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
) -> HarvestResult<PreparedWorkflowTask> {
    let Some(exec_uuid) = task.workflow_exec_id else {
        let error = HarvestError::Config("workflow task missing workflow_exec_id".into());
        fail_task_only(conn, task.id, &error.to_string()).await?;
        return Err(error);
    };
    let exec_id = execution_id_from_uuid(exec_uuid);
    let execution = load_task_execution(conn, task, exec_id).await?;
    let (history, timers_fired, signals_delivered) =
        load_workflow_replay_state(conn, task, worker_id, exec_id).await?;

    Ok(PreparedWorkflowTask {
        execution,
        exec_id,
        history_events: history.events,
        next_event_id: history.next_event_id,
        timers_fired,
        signals_delivered,
    })
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
        memo: execution.memo.clone(),
        search_attrs: execution.search_attrs.clone(),
    };
    let mut enqueue =
        queue::EnqueueParams::new(execution.queue_name.clone(), TaskType::Workflow, input);
    enqueue.workflow_exec_id = Some(new_exec_id.as_uuid());

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

#[allow(clippy::too_many_lines)]
async fn process_workflow_task(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task: &TaskQueueItem,
    worker_id: &str,
    sticky_timeout: Duration,
    max_local_activity_start_to_close: Duration,
) -> HarvestResult<()> {
    let prepared = prepare_workflow_task(conn, task, worker_id).await?;
    let Some(workflow) = registry.workflows.get(&prepared.execution.workflow_name) else {
        let error = format!(
            "no workflow handler registered for '{}'",
            prepared.execution.workflow_name
        );
        fail_task_and_execution(conn, task, worker_id, &error).await?;
        return Err(HarvestError::Config(error));
    };

    let telemetry = registry.telemetry().clone();
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

        let (run_outcome, execute_span) = run_workflow_with_state(
            prepared.exec_id,
            history_events.clone(),
            workflow.handler,
            task.input.clone(),
            registry.shared_state(),
            Some(&span_meta),
        )
        .await;

        match run_outcome {
            WorkflowOutcome::Suspended { commands }
                if commands
                    .iter()
                    .any(|c| matches!(c, WorkflowCommand::RunLocalActivity { .. })) =>
            {
                // Local-activity re-run: drop this iteration's execute span
                // so the OTel span closes before we start inline execution.
                drop(execute_span);
                let (markers, local_run) = extract_run_local_activity(commands);
                let new_events = run_local_activity_inline(
                    conn,
                    registry,
                    prepared.exec_id,
                    markers,
                    local_run,
                    max_local_activity_start_to_close,
                    &mut next_event_id,
                )
                .await?;
                history_events.extend(new_events);
            }
            other => break (other, execute_span),
        }
    };

    let (outcome, execute_span) = loop_result;

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
    .await
    // execute_span is dropped here, closing the OTel span after all producer
    // spans have been emitted as its children.
}

async fn process_task(
    pool: &DbPool,
    registry: Arc<HandlerRegistry>,
    task: TaskQueueItem,
    worker_id: &str,
    cancellation_grace_period: Duration,
    sticky_timeout: Duration,
    max_local_activity_start_to_close: Duration,
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
                        telemetry.metrics.record_concurrency_key_in_flight(
                            &stat.key,
                            u64::try_from(stat.in_flight).unwrap_or(0),
                        );
                        let saturated = stat.in_flight >= i64::from(stat.max_concurrent);
                        if saturated && stat.pending > 0 {
                            tracing::debug!(
                                concurrency_key = %stat.key,
                                in_flight = stat.in_flight,
                                max_concurrent = stat.max_concurrent,
                                deferred = stat.pending,
                                "concurrency cap saturated; pending tasks deferred until a slot frees"
                            );
                            telemetry.metrics.record_concurrency_key_deferred(
                                &stat.key,
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

        Ok(Self {
            config,
            registry,
            workflow_semaphore,
            activity_semaphore,
            shutdown: CancellationToken::new(),
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
    #[allow(clippy::too_many_lines)]
    pub async fn run_with_listener(
        &self,
        pool: &DbPool,
        mut listener: Option<crate::notify::QueueListener>,
    ) {
        tracing::info!(
            worker_id = %self.config.worker_id,
            queues = ?self.config.queues,
            "worker starting"
        );

        // Register this worker in the fleet table.
        self.register_in_fleet(pool).await;

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
        );

        // Spawn the heartbeat background task with a dedicated cancel token so
        // that liveness updates continue during the Draining phase and only stop
        // after the Stopped transition is written.
        let heartbeat_cancel = CancellationToken::new();
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
        let heartbeat_handle = crate::workers::spawn_worker_heartbeat(
            pool.clone(),
            crate::workers::WorkerRegistration {
                worker_id: self.config.worker_id.clone(),
                queues: self.config.queues.clone(),
                shard_assignments: shard_ids,
                max_concurrency,
                host: crate::workers::local_hostname(),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            },
            Arc::clone(&self.workflow_semaphore),
            self.config.max_concurrent_workflows,
            Arc::clone(&self.activity_semaphore),
            self.config.max_concurrent_activities,
            self.config.worker_heartbeat_interval,
            heartbeat_cancel.clone(),
        );

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
        if let Err(error) = timeout_checker.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "timeout checker task failed during shutdown"
            );
        }
        if let Err(error) = queue_depth_sampler.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "queue depth sampler failed during shutdown"
            );
        }
        if let Err(error) = concurrency_sampler.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "concurrency sampler failed during shutdown"
            );
        }
        for sampler in dlq_depth_samplers {
            if let Err(error) = sampler.await {
                tracing::warn!(
                    worker_id = %self.config.worker_id,
                    error = %error,
                    "dlq depth sampler failed during shutdown"
                );
            }
        }
        tracing::info!(worker_id = %self.config.worker_id, "worker stopped");
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

        match queue::claim_task(&mut conn, &self.config.queues, &self.config.worker_id).await {
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

    /// Wait for all in-flight tasks to finish (or timeout).
    ///
    /// We wait until all semaphore permits are available again, meaning all
    /// spawned tasks have completed and dropped their permits.
    async fn drain_in_flight(&self) {
        let total_permits =
            self.config.max_concurrent_workflows + self.config.max_concurrent_activities;

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

        if tokio::time::timeout(self.config.shutdown_timeout, drain)
            .await
            .is_err()
        {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                total_permits,
                "shutdown timeout elapsed — some tasks may still be running"
            );
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
