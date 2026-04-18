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
use crate::executor::{WorkflowOutcome, run_workflow_with_state};
use crate::info::{ActivityInfo, WorkflowInfo};
use crate::models::{
    HarvestTimer, NewHarvestTimer, NewWorkflowExecution, TaskQueueItem, WorkflowExecution,
};
use crate::policy::RetryPolicy;
use crate::queue::{self, TaskType};
use crate::schema::{harvest_timers, harvest_workflow_executions};
use crate::signal;
use crate::store;
use crate::types::{ActivityExecId, ExecutionId, TimerId, WorkerId};

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
}

impl std::fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerRegistry")
            .field("workflows", &self.workflows.keys().collect::<Vec<_>>())
            .field("activities", &self.activities.keys().collect::<Vec<_>>())
            .field("state_count", &self.state.len())
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
        WorkflowCommand::StartTimer { .. } => "StartTimer",
        WorkflowCommand::StartChildWorkflow { .. } => "StartChildWorkflow",
        WorkflowCommand::RecordMarker { .. } => "RecordMarker",
        WorkflowCommand::WaitForSignal { .. } => "WaitForSignal",
        WorkflowCommand::Complete { .. } => "Complete",
        WorkflowCommand::Fail { .. } => "Fail",
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

#[derive(Debug)]
struct PreparedWorkflowTask {
    execution: WorkflowExecution,
    exec_id: ExecutionId,
    history_events: Vec<WorkflowEvent>,
    next_event_id: i32,
}

#[derive(Debug, Clone, Copy)]
struct WorkflowTaskPersistence<'a> {
    task: &'a TaskQueueItem,
    worker_id: &'a str,
    exec_id: ExecutionId,
    next_event_id: i32,
}

#[derive(Debug, Clone, Copy)]
struct SuspendedWorkflowContext<'a> {
    execution: &'a WorkflowExecution,
    persistence: WorkflowTaskPersistence<'a>,
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

fn extract_single_schedule_activity(
    commands: &[WorkflowCommand],
) -> Option<ScheduledActivityCommand> {
    let mut scheduled = None;

    for cmd in commands {
        match cmd {
            WorkflowCommand::RecordMarker { .. } => {}
            WorkflowCommand::ScheduleActivity {
                activity_id,
                name,
                input,
                queue,
                ..
            } => {
                if scheduled.is_some() {
                    return None;
                }

                scheduled = Some(ScheduledActivityCommand {
                    activity_id: *activity_id,
                    name: name.clone(),
                    input: input.clone(),
                    queue: queue.clone(),
                });
            }
            _ => return None,
        }
    }

    scheduled
}

fn extract_single_started_timer(commands: &[WorkflowCommand]) -> Option<StartedTimerCommand> {
    let mut timer = None;

    for cmd in commands {
        match cmd {
            WorkflowCommand::RecordMarker { .. } => {}
            WorkflowCommand::StartTimer {
                timer_id,
                duration_secs,
                ..
            } => {
                if timer.is_some() {
                    return None;
                }

                timer = Some(StartedTimerCommand {
                    timer_id: timer_id.clone(),
                    duration_secs: *duration_secs,
                });
            }
            _ => return None,
        }
    }

    timer
}

fn extract_single_started_child_workflow(
    commands: &[WorkflowCommand],
) -> Option<StartedChildWorkflowCommand> {
    let mut child = None;

    for cmd in commands {
        match cmd {
            WorkflowCommand::RecordMarker { .. } => {}
            WorkflowCommand::StartChildWorkflow {
                child_id,
                workflow_name,
                input,
                ..
            } => {
                if child.is_some() {
                    return None;
                }

                child = Some(StartedChildWorkflowCommand {
                    child_id: *child_id,
                    workflow_name: workflow_name.clone(),
                    input: input.clone(),
                });
            }
            _ => return None,
        }
    }

    child
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
    Ok(chrono::Duration::seconds(seconds))
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

    let pending = history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled {
                activity_id, name, ..
            } if name == activity_name && !terminal_ids.contains(activity_id) => Some(*activity_id),
            _ => None,
        })
        .collect::<Vec<_>>();

    match pending.as_slice() {
        [activity_id] => Ok(*activity_id),
        [] => Err(HarvestError::NotFound(format!(
            "no pending scheduled activity '{activity_name}' in workflow history"
        ))),
        _ => Err(HarvestError::NonDeterministic(format!(
            "multiple pending scheduled activities named '{activity_name}' found in history"
        ))),
    }
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

async fn update_workflow_execution_completed(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    worker_id: &str,
    output: &serde_json::Value,
) -> HarvestResult<()> {
    use crate::schema::harvest_workflow_executions::dsl;

    let updated = diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
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
        return Err(HarvestError::NotFound(format!(
            "workflow execution {exec_id}"
        )));
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

    let updated = diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
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
        return Err(HarvestError::NotFound(format!(
            "workflow execution {exec_id}"
        )));
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
) -> HarvestResult<()> {
    // Park the workflow task (state=RUNNING, worker cleared) so it is not
    // confused with a timer-waiting task (state=PENDING). This ensures that
    // `wake_workflow_task` — which only targets RUNNING/parked rows — can
    // reliably distinguish signal waits from timer waits and will not
    // prematurely fire a pending timer when a signal is delivered.
    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, marker_events, next_event_id).await?;
            queue::park_workflow_task(conn, task_id).await
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

async fn persist_scheduled_activity(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task_id: uuid::Uuid,
    exec_id: ExecutionId,
    next_event_id: i32,
    commands: &[WorkflowCommand],
    scheduled: &ScheduledActivityCommand,
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

    let activity_events = vec![WorkflowEvent::ActivityScheduled {
        activity_id: scheduled.activity_id,
        name: scheduled.name.clone(),
        input: scheduled.input.clone(),
        queue: queue_name,
    }];
    let mut events = marker_events;
    events.extend(activity_events);

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &events, next_event_id).await?;
            queue::enqueue(conn, &params).await?;
            queue::park_workflow_task(conn, task_id).await?;
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
) -> HarvestResult<()> {
    let marker_events = marker_events_from_commands(commands);
    let fire_delay = chrono_duration_from_secs(timer.duration_secs, "timer duration")?;
    let fires_at = chrono::Utc::now() + fire_delay;
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
            queue::reschedule_task(conn, task_id, fires_at).await
        }
        .scope_boxed()
    })
    .await
}

async fn persist_started_child_workflow(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task_id: uuid::Uuid,
    parent_execution: &WorkflowExecution,
    next_event_id: i32,
    commands: &[WorkflowCommand],
    child: &StartedChildWorkflowCommand,
) -> HarvestResult<()> {
    if !registry.workflows.contains_key(&child.workflow_name) {
        return Err(HarvestError::Config(format!(
            "no workflow handler registered for '{}'",
            child.workflow_name
        )));
    }

    let marker_events = marker_events_from_commands(commands);
    let parent_exec_id = execution_id_from_uuid(parent_execution.id);
    let child_started_in_parent = WorkflowEvent::ChildWorkflowStarted {
        child_id: child.child_id,
        workflow_name: child.workflow_name.clone(),
        input: child.input.clone(),
    };
    let mut parent_events = marker_events;
    parent_events.push(child_started_in_parent);

    let child_workflow_id = child.child_id.to_string();
    let queue_name = parent_execution.queue_name.clone();
    let child_row = NewWorkflowExecution {
        id: child.child_id.as_uuid(),
        workflow_name: &child.workflow_name,
        workflow_id: &child_workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: parent_execution.shard_id,
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
    let mut params =
        queue::EnqueueParams::new(queue_name.clone(), TaskType::Workflow, child.input.clone());
    params.workflow_exec_id = Some(child.child_id.as_uuid());

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, parent_exec_id, &parent_events, next_event_id).await?;
            diesel::insert_into(harvest_workflow_executions::table)
                .values(&child_row)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            store::append_events(conn, child.child_id, &[child_started_event], 0).await?;
            queue::enqueue(conn, &params).await?;
            queue::park_workflow_task(conn, task_id).await?;
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
) -> HarvestResult<()> {
    let pending_signals = signal::load_pending_signals(conn, exec_id).await?;
    if pending_signals.is_empty() {
        return Ok(());
    }

    let mut signal_events = Vec::with_capacity(pending_signals.len());
    let mut signal_ids = Vec::with_capacity(pending_signals.len());

    for signal in pending_signals {
        signal_ids.push(signal.id);
        signal_events.push(WorkflowEvent::SignalReceived {
            signal_name: signal.signal_name,
            payload: signal.payload,
        });
    }

    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            store::append_events(conn, exec_id, &signal_events, next_event_id).await?;
            signal::mark_signals_consumed(conn, &signal_ids).await?;
            Ok(())
        }
        .scope_boxed()
    })
    .await
}

async fn ingest_fired_timers(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    next_event_id: i32,
) -> HarvestResult<()> {
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
        return Ok(());
    }

    let mut timer_events = Vec::with_capacity(due_timers.len());
    let mut timer_row_ids = Vec::with_capacity(due_timers.len());

    for timer in due_timers {
        timer_row_ids.push(timer.id);
        timer_events.push(WorkflowEvent::TimerFired {
            timer_id: TimerId::new(timer.timer_id),
        });
    }

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
    .await
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
    match store::load_history(conn, exec_id).await {
        Ok(history) => {
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
        Err(history_error) => {
            tracing::warn!(
                task_id = %task.id,
                workflow_exec_id = %exec_id,
                error = %history_error,
                "failed to load workflow history while persisting task failure; updating rows without event append"
            );
            update_workflow_execution_failed(conn, exec_id, worker_id, error).await?;
            queue::fail_task(conn, task.id, error).await
        }
    }
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
    let parent_history = store::load_history(conn, parent_exec_id).await?;
    let event = WorkflowEvent::ChildWorkflowCompleted {
        child_id: child_exec_id,
        output,
    };
    store::append_events(conn, parent_exec_id, &[event], parent_history.next_event_id).await?;
    queue::wake_workflow_task(conn, parent_exec_id).await
}

async fn wake_parent_for_child_failure(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    child_exec_id: ExecutionId,
    error: &str,
) -> HarvestResult<()> {
    let parent_history = store::load_history(conn, parent_exec_id).await?;
    let event = WorkflowEvent::ChildWorkflowFailed {
        child_id: child_exec_id,
        error: error.to_string(),
    };
    store::append_events(conn, parent_exec_id, &[event], parent_history.next_event_id).await?;
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

async fn process_activity_task(
    pool: &DbPool,
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task: &TaskQueueItem,
    worker_id: &str,
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

    let history = match store::load_history(conn, exec_id).await {
        Ok(history) => history,
        Err(error) => {
            fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
            return Err(error);
        }
    };

    let activity_id = match find_pending_scheduled_activity(&history.events, activity_name) {
        Ok(activity_id) => activity_id,
        Err(error) => {
            fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
            return Err(error);
        }
    };

    let started_event = WorkflowEvent::ActivityStarted {
        activity_id,
        worker_id: WorkerId::new(worker_id),
    };
    if let Err(error) =
        store::append_events(conn, exec_id, &[started_event], history.next_event_id).await
    {
        fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
        return Err(error);
    }

    let cancel = CancellationToken::new();
    let heartbeat_tx =
        crate::heartbeat::spawn_heartbeat_flusher(task.id, pool.clone(), cancel.clone());
    let ctx = ActivityContext::new(registry.shared_state(), Some(heartbeat_tx), cancel.clone());

    let activity_result = (activity.handler)(&ctx, task.input.clone()).await;
    cancel.cancel();

    let retry_policy = match configured_retry_policy(task) {
        Ok(retry_policy) => retry_policy,
        Err(error) => {
            fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
            return Err(error);
        }
    };

    match activity_result {
        Ok(output) => {
            finalize_activity_completion(
                conn,
                task,
                exec_id,
                history.next_event_id + 1,
                activity_id,
                output,
            )
            .await
        }
        Err(error) => {
            let delay = match next_retry_delay(task, &error, retry_policy.as_ref()) {
                Ok(delay) => delay,
                Err(delay_error) => {
                    fail_task_and_execution(conn, task, worker_id, &delay_error.to_string())
                        .await?;
                    return Err(delay_error);
                }
            };

            if let Some(delay) = delay {
                return queue::requeue_for_retry(conn, task.id, delay).await;
            }

            finalize_activity_failure(
                conn,
                task,
                exec_id,
                history.next_event_id + 1,
                activity_id,
                &error,
            )
            .await
        }
    }
}

async fn handle_suspended_workflow(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    context: SuspendedWorkflowContext<'_>,
    commands: &[WorkflowCommand],
) -> HarvestResult<()> {
    if should_requeue_signal_wait(commands) {
        let marker_events = marker_events_from_commands(commands);
        let result = persist_signal_wait_park(
            conn,
            context.persistence.task.id,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            &marker_events,
        )
        .await;
        return settle_suspended_result(
            conn,
            context.persistence.task,
            context.persistence.worker_id,
            result,
        )
        .await;
    }

    if let Some(scheduled) = extract_single_schedule_activity(commands) {
        let result = persist_scheduled_activity(
            conn,
            registry,
            context.persistence.task.id,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            commands,
            &scheduled,
        )
        .await;
        return settle_suspended_result(
            conn,
            context.persistence.task,
            context.persistence.worker_id,
            result,
        )
        .await;
    }

    if let Some(timer) = extract_single_started_timer(commands) {
        let result = persist_started_timer(
            conn,
            context.persistence.exec_id,
            context.persistence.next_event_id,
            context.persistence.task.id,
            commands,
            &timer,
        )
        .await;
        return settle_suspended_result(
            conn,
            context.persistence.task,
            context.persistence.worker_id,
            result,
        )
        .await;
    }

    if let Some(child) = extract_single_started_child_workflow(commands) {
        let result = persist_started_child_workflow(
            conn,
            registry,
            context.persistence.task.id,
            context.execution,
            context.persistence.next_event_id,
            commands,
            &child,
        )
        .await;
        return settle_suspended_result(
            conn,
            context.persistence.task,
            context.persistence.worker_id,
            result,
        )
        .await;
    }

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
}

async fn settle_suspended_result<T>(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
    result: HarvestResult<T>,
) -> HarvestResult<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
            Err(error)
        }
    }
}

async fn load_task_execution(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    exec_id: ExecutionId,
) -> HarvestResult<WorkflowExecution> {
    match load_workflow_execution(conn, exec_id).await {
        Ok(execution) => Ok(execution),
        Err(error) => {
            fail_task_only(conn, task.id, &error.to_string()).await?;
            Err(error)
        }
    }
}

async fn load_workflow_replay_state(
    conn: &mut AsyncPgConnection,
    task: &TaskQueueItem,
    worker_id: &str,
    exec_id: ExecutionId,
) -> HarvestResult<store::EventHistory> {
    let initial_history = match store::load_history(conn, exec_id).await {
        Ok(history) => history,
        Err(error) => {
            fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
            return Err(error);
        }
    };

    if let Err(error) = ingest_fired_timers(conn, exec_id, initial_history.next_event_id).await {
        fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
        return Err(error);
    }

    let history_after_timers = match store::load_history(conn, exec_id).await {
        Ok(history) => history,
        Err(error) => {
            fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
            return Err(error);
        }
    };

    if let Err(error) =
        ingest_pending_signals(conn, exec_id, history_after_timers.next_event_id).await
    {
        fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
        return Err(error);
    }

    match store::load_history(conn, exec_id).await {
        Ok(history) => Ok(history),
        Err(error) => {
            fail_task_and_execution(conn, task, worker_id, &error.to_string()).await?;
            Err(error)
        }
    }
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
    let history = load_workflow_replay_state(conn, task, worker_id, exec_id).await?;

    Ok(PreparedWorkflowTask {
        execution,
        exec_id,
        history_events: history.events,
        next_event_id: history.next_event_id,
    })
}

async fn persist_workflow_outcome(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    execution: &WorkflowExecution,
    persistence: WorkflowTaskPersistence<'_>,
    outcome: WorkflowOutcome,
) -> HarvestResult<()> {
    match outcome {
        WorkflowOutcome::Completed { output } => {
            if let Some(parent_uuid) = execution.parent_id {
                persist_child_workflow_completion(
                    conn,
                    persistence.task.id,
                    persistence.exec_id,
                    persistence.next_event_id,
                    persistence.worker_id,
                    execution_id_from_uuid(parent_uuid),
                    output,
                )
                .await
            } else {
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
        }
        WorkflowOutcome::Failed { error } => {
            if let Some(parent_uuid) = execution.parent_id {
                persist_child_workflow_failure(
                    conn,
                    persistence.task.id,
                    persistence.exec_id,
                    persistence.next_event_id,
                    persistence.worker_id,
                    execution_id_from_uuid(parent_uuid),
                    &error,
                )
                .await
            } else {
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
        }
        WorkflowOutcome::Suspended { commands } => {
            handle_suspended_workflow(
                conn,
                registry,
                SuspendedWorkflowContext {
                    execution,
                    persistence,
                },
                &commands,
            )
            .await
        }
    }
}

async fn process_workflow_task(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    task: &TaskQueueItem,
    worker_id: &str,
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

    let outcome = run_workflow_with_state(
        prepared.exec_id,
        prepared.history_events,
        workflow.handler,
        task.input.clone(),
        registry.shared_state(),
    )
    .await;

    persist_workflow_outcome(
        conn,
        registry,
        &prepared.execution,
        WorkflowTaskPersistence {
            task,
            worker_id,
            exec_id: prepared.exec_id,
            next_event_id: prepared.next_event_id,
        },
        outcome,
    )
    .await
}

async fn process_task(
    pool: &DbPool,
    registry: Arc<HandlerRegistry>,
    task: TaskQueueItem,
    worker_id: &str,
) -> HarvestResult<()> {
    let mut conn = match pool.get().await {
        Ok(conn) => conn,
        Err(error) => {
            return Err(crate::error::database_error(error));
        }
    };

    match ClaimedTaskKind::from_db(&task.task_type)? {
        ClaimedTaskKind::Workflow => {
            process_workflow_task(&mut conn, registry.as_ref(), &task, worker_id).await
        }
        ClaimedTaskKind::Activity => {
            process_activity_task(pool, &mut conn, registry.as_ref(), &task, worker_id).await
        }
    }
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
        let timeout_checker = crate::timeout::spawn_timeout_checker(
            pool.clone(),
            self.shutdown.clone(),
            self.config.poll_interval,
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

        tracing::info!(worker_id = %self.config.worker_id, "draining in-flight tasks");
        self.drain_in_flight().await;
        if let Err(error) = timeout_checker.await {
            tracing::warn!(
                worker_id = %self.config.worker_id,
                error = %error,
                "timeout checker task failed during shutdown"
            );
        }
        tracing::info!(worker_id = %self.config.worker_id, "worker stopped");
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

            if let Err(error) = process_task(&pool, registry, task, &worker_id).await {
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
    #[allow(clippy::cast_possible_truncation)] // concurrency limits are well under u32::MAX
    async fn drain_in_flight(&self) {
        let total_permits =
            self.config.max_concurrent_workflows + self.config.max_concurrent_activities;

        let drain = async {
            // Try to acquire ALL permits — when we can, all in-flight tasks are done.
            let _wf = self
                .workflow_semaphore
                .acquire_many(self.config.max_concurrent_workflows as u32)
                .await;
            let _act = self
                .activity_semaphore
                .acquire_many(self.config.max_concurrent_activities as u32)
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
    fn worker_config_from_builder() {
        let builder_cfg = WorkerConfig {
            queues: vec!["email".to_string(), "billing".to_string()],
            notification_database_url: Some("postgres://localhost/test".to_string()),
            max_concurrent_workflows: 5,
            max_concurrent_activities: 15,
            shutdown_timeout: Duration::from_secs(60),
            workflow_cache_size: 500,
            sticky_timeout: Duration::from_secs(3),
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
}
