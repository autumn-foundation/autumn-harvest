//! Workflow reset and fork recovery primitives.

use std::collections::BTreeMap;
use std::fmt;

use chrono::Utc;
use diesel::prelude::*;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use scoped_futures::ScopedFutureExt as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{HarvestError, database_error};
use crate::event::WorkflowEvent;
use crate::models::{HarvestEvent, NewWorkflowExecution, WorkflowExecution};
use crate::queue::{self, EnqueueParams, TaskType};
use crate::schema::{
    harvest_events, harvest_external_tasks, harvest_signals, harvest_task_queue, harvest_timers,
    harvest_workflow_executions,
};
use crate::types::{ExecutionId, ShardId};

/// How undelivered source signals are handled during reset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResetSignalReapplyPolicy {
    /// Discard undelivered source signals.
    #[default]
    Drop,
    /// Re-enqueue undelivered source signals onto the fork as fresh rows.
    Buffer,
}

impl ResetSignalReapplyPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Buffer => "buffer",
        }
    }
}

impl Serialize for ResetSignalReapplyPolicy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ResetSignalReapplyPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Option::<String>::deserialize(deserializer)?;
        match raw.as_deref().unwrap_or("drop") {
            "drop" => Ok(Self::Drop),
            "buffer" => Ok(Self::Buffer),
            other => Err(serde::de::Error::custom(format!(
                "unknown signal_reapply '{other}'; expected 'drop' or 'buffer'"
            ))),
        }
    }
}

/// Request body for resetting one workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowResetRequest {
    pub reset_to_event_id: i64,
    pub reason: String,
    pub operator_id: String,
    #[serde(default)]
    pub signal_reapply: ResetSignalReapplyPolicy,
}

impl WorkflowResetRequest {
    fn normalized(mut self) -> Self {
        self.reason = non_empty_or(self.reason.trim(), "workflow reset requested");
        self.operator_id = non_empty_or(self.operator_id.trim(), "unknown");
        self
    }
}

fn non_empty_or(value: &str, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

/// A side effect that is still unresolved at a proposed reset boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetUnresolvedSideEffect {
    pub kind: String,
    pub side_effect_id: String,
    pub name: Option<String>,
    pub scheduled_event_id: i64,
}

/// Valid reset-boundary plan, also used as dry-run output after DB counts are attached.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetPlan {
    pub reset_to_event_id: i64,
    pub events_carried_over: usize,
    pub unresolved_side_effects: Vec<ResetUnresolvedSideEffect>,
    pub nearest_valid_before: Option<i64>,
    pub nearest_valid_after: Option<i64>,
    pub source_tasks_to_cancel: usize,
    pub source_timers_to_remove: usize,
    pub source_signals_to_drop: usize,
    pub source_signals_to_buffer: usize,
}

impl ResetPlan {
    const fn valid(reset_to_event_id: i64, events_carried_over: usize) -> Self {
        Self {
            reset_to_event_id,
            events_carried_over,
            unresolved_side_effects: Vec::new(),
            nearest_valid_before: Some(reset_to_event_id),
            nearest_valid_after: Some(reset_to_event_id),
            source_tasks_to_cancel: 0,
            source_timers_to_remove: 0,
            source_signals_to_drop: 0,
            source_signals_to_buffer: 0,
        }
    }
}

/// Invalid reset-boundary details surfaced by the management API as `400`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetInvalidPoint {
    pub message: String,
    pub reset_to_event_id: i64,
    pub last_event_id: i64,
    pub unresolved_side_effects: Vec<ResetUnresolvedSideEffect>,
    pub nearest_valid_before: Option<i64>,
    pub nearest_valid_after: Option<i64>,
}

impl fmt::Display for ResetInvalidPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ResetInvalidPoint {}

/// Result of a committed workflow reset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetResult {
    pub new_exec_id: ExecutionId,
    pub reset_from_exec_id: ExecutionId,
    pub reset_to_event_id: i64,
    pub events_carried_over: usize,
    pub source_tasks_cancelled: usize,
    pub source_timers_removed: usize,
    pub source_signals_dropped: usize,
    pub source_signals_buffered: usize,
}

/// Errors specific to the reset workflow.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowResetError {
    #[error(transparent)]
    InvalidPoint(#[from] ResetInvalidPoint),
    #[error("workflow execution {exec_id} is terminal ({state})")]
    TerminalSource { exec_id: ExecutionId, state: String },
    #[error("workflow execution {exec_id} is a child workflow; reset the root parent in v1")]
    ChildWorkflow {
        exec_id: ExecutionId,
        parent_id: Uuid,
    },
    #[error("continue-as-new histories cannot be reset in v1")]
    ContinueAsNew,
    #[error(transparent)]
    Harvest(#[from] HarvestError),
}

impl From<diesel::result::Error> for WorkflowResetError {
    fn from(error: diesel::result::Error) -> Self {
        Self::Harvest(database_error(error))
    }
}

/// Validate that `reset_to_event_id` lands on a decision boundary.
///
/// The history slice is assumed to be ordered by `event_id` and contiguous,
/// which is the invariant maintained by `harvest_events`.
///
/// # Errors
///
/// Returns [`ResetInvalidPoint`] when the target is out of range, the history
/// contains `WorkflowContinuedAsNew`, or unresolved side effects are open at the
/// requested boundary.
pub fn validate_reset_point(
    events: &[WorkflowEvent],
    reset_to_event_id: i64,
) -> Result<ResetPlan, ResetInvalidPoint> {
    let last_event_id = i64::try_from(events.len())
        .ok()
        .and_then(|len| len.checked_sub(1))
        .unwrap_or(-1);

    if events
        .iter()
        .any(|event| matches!(event, WorkflowEvent::WorkflowContinuedAsNew { .. }))
    {
        return Err(ResetInvalidPoint {
            message: "continue-as-new histories cannot be reset in v1".to_string(),
            reset_to_event_id,
            last_event_id,
            unresolved_side_effects: Vec::new(),
            nearest_valid_before: nearest_valid_boundary(events, last_event_id, true),
            nearest_valid_after: None,
        });
    }

    if reset_to_event_id < 0 || reset_to_event_id > last_event_id {
        return Err(ResetInvalidPoint {
            message: format!(
                "reset_to_event_id {reset_to_event_id} is outside history range 0..={last_event_id}"
            ),
            reset_to_event_id,
            last_event_id,
            unresolved_side_effects: Vec::new(),
            nearest_valid_before: nearest_valid_boundary(events, last_event_id, true),
            nearest_valid_after: None,
        });
    }

    let target = usize::try_from(reset_to_event_id).map_err(|_| ResetInvalidPoint {
        message: format!("reset_to_event_id {reset_to_event_id} cannot be represented"),
        reset_to_event_id,
        last_event_id,
        unresolved_side_effects: Vec::new(),
        nearest_valid_before: None,
        nearest_valid_after: None,
    })?;

    let (valid_boundaries, unresolved_at_target) = boundary_validity(events, target);
    if valid_boundaries[target] {
        return Ok(ResetPlan::valid(reset_to_event_id, target + 1));
    }

    let nearest_valid_before = valid_boundaries
        .iter()
        .take(target)
        .enumerate()
        .rev()
        .find_map(|(idx, valid)| valid.then_some(i64::try_from(idx).unwrap_or(i64::MAX)));
    let nearest_valid_after = valid_boundaries
        .iter()
        .enumerate()
        .skip(target + 1)
        .find_map(|(idx, valid)| valid.then_some(i64::try_from(idx).unwrap_or(i64::MAX)));

    Err(ResetInvalidPoint {
        message: format!(
            "event {reset_to_event_id} is not a valid reset boundary; {} side effect(s) are unresolved",
            unresolved_at_target.len()
        ),
        reset_to_event_id,
        last_event_id,
        unresolved_side_effects: unresolved_at_target,
        nearest_valid_before,
        nearest_valid_after,
    })
}

fn nearest_valid_boundary(
    events: &[WorkflowEvent],
    start_event_id: i64,
    search_before: bool,
) -> Option<i64> {
    if events.is_empty() {
        return None;
    }
    let start = usize::try_from(start_event_id).ok()?;
    let (valid, _) = boundary_validity(events, start.min(events.len() - 1));
    if search_before {
        valid
            .iter()
            .take(start.saturating_add(1))
            .enumerate()
            .rev()
            .find_map(|(idx, ok)| ok.then_some(i64::try_from(idx).unwrap_or(i64::MAX)))
    } else {
        valid
            .iter()
            .enumerate()
            .skip(start)
            .find_map(|(idx, ok)| ok.then_some(i64::try_from(idx).unwrap_or(i64::MAX)))
    }
}

fn boundary_validity(
    events: &[WorkflowEvent],
    target: usize,
) -> (Vec<bool>, Vec<ResetUnresolvedSideEffect>) {
    let mut pending = BTreeMap::<String, ResetUnresolvedSideEffect>::new();
    let mut valid = Vec::with_capacity(events.len());
    let mut unresolved_at_target = Vec::new();

    for (idx, event) in events.iter().enumerate() {
        let event_id = i64::try_from(idx).unwrap_or(i64::MAX);
        apply_event_to_pending(event_id, event, &mut pending);
        let boundary_is_valid = idx == 0 && matches!(event, WorkflowEvent::WorkflowStarted { .. })
            || pending.is_empty();
        valid.push(boundary_is_valid);
        if idx == target {
            unresolved_at_target = pending.values().cloned().collect();
        }
    }

    (valid, unresolved_at_target)
}

fn apply_event_to_pending(
    event_id: i64,
    event: &WorkflowEvent,
    pending: &mut BTreeMap<String, ResetUnresolvedSideEffect>,
) {
    match event {
        WorkflowEvent::ActivityScheduled {
            activity_id, name, ..
        } => insert_pending(
            pending,
            "ActivityScheduled",
            activity_id.to_string(),
            Some(name.clone()),
            event_id,
        ),
        WorkflowEvent::ActivityCompleted { activity_id, .. }
        | WorkflowEvent::ActivityFailed { activity_id, .. }
        | WorkflowEvent::ActivityTimedOut { activity_id, .. } => {
            remove_pending(pending, "ActivityScheduled", &activity_id.to_string());
            remove_pending(
                pending,
                "ActivityAwaitingExternal",
                &activity_id.to_string(),
            );
        }
        WorkflowEvent::TimerStarted { timer_id, .. } => insert_pending(
            pending,
            "TimerStarted",
            timer_id.to_string(),
            Some(timer_id.to_string()),
            event_id,
        ),
        WorkflowEvent::TimerFired { timer_id } => {
            remove_pending(pending, "TimerStarted", &timer_id.to_string());
        }
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name,
            ..
        } => insert_pending(
            pending,
            "ChildWorkflowStarted",
            child_id.to_string(),
            Some(workflow_name.clone()),
            event_id,
        ),
        WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
        | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
            remove_pending(pending, "ChildWorkflowStarted", &child_id.to_string());
        }
        WorkflowEvent::LocalActivityScheduled {
            activity_id, name, ..
        } => insert_pending(
            pending,
            "LocalActivityScheduled",
            activity_id.to_string(),
            Some(name.clone()),
            event_id,
        ),
        WorkflowEvent::LocalActivityCompleted { activity_id, .. }
        | WorkflowEvent::LocalActivityFailed { activity_id, .. } => {
            remove_pending(pending, "LocalActivityScheduled", &activity_id.to_string());
        }
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id, name, ..
        } => insert_pending(
            pending,
            "ActivityAwaitingExternal",
            activity_id.to_string(),
            Some(name.clone()),
            event_id,
        ),
        WorkflowEvent::ActivityCompletedExternally { activity_id, .. }
        | WorkflowEvent::ActivityFailedExternally { activity_id, .. } => {
            remove_pending(
                pending,
                "ActivityAwaitingExternal",
                &activity_id.to_string(),
            );
        }
        WorkflowEvent::UpdateAdmitted {
            update_id, name, ..
        } => insert_pending(
            pending,
            "UpdateAdmitted",
            update_id.to_string(),
            Some(name.clone()),
            event_id,
        ),
        WorkflowEvent::UpdateCompleted { update_id, .. }
        | WorkflowEvent::UpdateFailed { update_id, .. } => {
            remove_pending(pending, "UpdateAdmitted", &update_id.to_string());
        }
        _ => {}
    }
}

fn insert_pending(
    pending: &mut BTreeMap<String, ResetUnresolvedSideEffect>,
    kind: &str,
    side_effect_id: String,
    name: Option<String>,
    scheduled_event_id: i64,
) {
    pending.insert(
        pending_key(kind, &side_effect_id),
        ResetUnresolvedSideEffect {
            kind: kind.to_string(),
            side_effect_id,
            name,
            scheduled_event_id,
        },
    );
}

fn remove_pending(pending: &mut BTreeMap<String, ResetUnresolvedSideEffect>, kind: &str, id: &str) {
    pending.remove(&pending_key(kind, id));
}

fn pending_key(kind: &str, id: &str) -> String {
    format!("{kind}:{id}")
}

/// Dry-run a reset without committing any changes.
///
/// # Errors
///
/// Returns [`WorkflowResetError`] if the source execution or reset point is invalid.
pub async fn preview_workflow_reset(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    request: WorkflowResetRequest,
) -> Result<ResetPlan, WorkflowResetError> {
    let request = request.normalized();
    let execution = load_source_execution(conn, exec_id, false).await?;
    validate_source_execution(exec_id, &execution)?;
    let rows = load_event_rows(conn, exec_id).await?;
    let events = decode_events(&rows)?;
    let mut plan = validate_reset_point(&events, request.reset_to_event_id)?;
    attach_side_effect_counts(conn, exec_id, request.signal_reapply, &mut plan).await?;
    Ok(plan)
}

/// Fork a running workflow execution at a valid event boundary.
///
/// The operation is single-shard and transactional. Existing source event rows
/// are never modified; carried-over rows are inserted as new rows for the fork.
///
/// # Errors
///
/// Returns [`WorkflowResetError`] if validation fails or any persistence step fails.
pub async fn reset_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    request: WorkflowResetRequest,
) -> Result<ResetResult, WorkflowResetError> {
    let request = request.normalized();
    conn.transaction::<ResetResult, WorkflowResetError, _>(|conn| {
        async move {
            let source = load_source_execution(conn, exec_id, true).await?;
            validate_source_execution(exec_id, &source)?;

            let rows = load_event_rows(conn, exec_id).await?;
            let events = decode_events(&rows)?;
            let plan = validate_reset_point(&events, request.reset_to_event_id)?;

            let new_exec_id = ExecutionId::new_for_shard(ShardId::new(source.shard_id));
            let source_next_event_id = rows.last().map_or(0, |row| row.event_id.saturating_add(1));

            terminate_source_execution(conn, exec_id, new_exec_id, &request, source_next_event_id)
                .await?;
            let fork = insert_fork_execution(conn, &source, new_exec_id).await?;
            copy_carried_events(conn, new_exec_id, &rows, request.reset_to_event_id).await?;
            append_fork_marker(conn, new_exec_id, exec_id, &request, &plan).await?;

            let source_tasks_cancelled = queue::cancel_open_tasks_for_execution(
                conn,
                exec_id,
                &format!("workflow reset to {new_exec_id}: {}", request.reason),
            )
            .await?;
            let source_timers_removed = remove_pending_timers(conn, exec_id).await?;
            let source_external_cancelled = cancel_pending_external_tasks(conn, exec_id).await?;
            let signals_buffered =
                reapply_or_drop_signals(conn, exec_id, new_exec_id, request.signal_reapply).await?;

            enqueue_fork_workflow_task(conn, &fork, new_exec_id).await?;

            Ok(ResetResult {
                new_exec_id,
                reset_from_exec_id: exec_id,
                reset_to_event_id: request.reset_to_event_id,
                events_carried_over: plan.events_carried_over,
                source_tasks_cancelled: source_tasks_cancelled + source_external_cancelled,
                source_timers_removed,
                source_signals_dropped: match request.signal_reapply {
                    ResetSignalReapplyPolicy::Drop => signals_buffered,
                    ResetSignalReapplyPolicy::Buffer => 0,
                },
                source_signals_buffered: match request.signal_reapply {
                    ResetSignalReapplyPolicy::Drop => 0,
                    ResetSignalReapplyPolicy::Buffer => signals_buffered,
                },
            })
        }
        .scope_boxed()
    })
    .await
}

fn validate_source_execution(
    exec_id: ExecutionId,
    execution: &WorkflowExecution,
) -> Result<(), WorkflowResetError> {
    if execution.state != "RUNNING" {
        return Err(WorkflowResetError::TerminalSource {
            exec_id,
            state: execution.state.clone(),
        });
    }
    if let Some(parent_id) = execution.parent_id {
        return Err(WorkflowResetError::ChildWorkflow { exec_id, parent_id });
    }
    Ok(())
}

async fn load_source_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    lock: bool,
) -> Result<WorkflowExecution, WorkflowResetError> {
    let query = harvest_workflow_executions::table.find(exec_id.as_uuid());
    if lock {
        query
            .for_update()
            .select(WorkflowExecution::as_select())
            .first(conn)
            .await
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")).into())
    } else {
        query
            .select(WorkflowExecution::as_select())
            .first(conn)
            .await
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")).into())
    }
}

async fn load_event_rows(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Result<Vec<HarvestEvent>, WorkflowResetError> {
    harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
        .order(harvest_events::event_id.asc())
        .select(HarvestEvent::as_select())
        .load(conn)
        .await
        .map_err(database_error)
        .map_err(WorkflowResetError::from)
}

fn decode_events(rows: &[HarvestEvent]) -> Result<Vec<WorkflowEvent>, WorkflowResetError> {
    rows.iter()
        .map(|row| serde_json::from_value(row.event_data.clone()).map_err(HarvestError::from))
        .collect::<Result<Vec<_>, _>>()
        .map_err(WorkflowResetError::from)
}

async fn attach_side_effect_counts(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    policy: ResetSignalReapplyPolicy,
    plan: &mut ResetPlan,
) -> Result<(), WorkflowResetError> {
    plan.source_tasks_to_cancel = count_open_task_rows(conn, exec_id).await?;
    plan.source_timers_to_remove = count_pending_timers(conn, exec_id).await?;
    let signals = count_unconsumed_signals(conn, exec_id).await?;
    match policy {
        ResetSignalReapplyPolicy::Drop => plan.source_signals_to_drop = signals,
        ResetSignalReapplyPolicy::Buffer => plan.source_signals_to_buffer = signals,
    }
    Ok(())
}

async fn count_open_task_rows(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Result<usize, WorkflowResetError> {
    let queued = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
        .filter(harvest_task_queue::state.eq_any(["PENDING", "RUNNING"]))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    let external = harvest_external_tasks::table
        .filter(harvest_external_tasks::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_external_tasks::state.eq("PENDING"))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    Ok(usize::try_from(queued.saturating_add(external)).unwrap_or(usize::MAX))
}

async fn count_pending_timers(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Result<usize, WorkflowResetError> {
    let count = harvest_timers::table
        .filter(harvest_timers::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_timers::fired.eq(false))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

async fn count_unconsumed_signals(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Result<usize, WorkflowResetError> {
    let count = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(harvest_signals::consumed.eq(false))
        .count()
        .get_result::<i64>(conn)
        .await
        .map_err(database_error)?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

async fn terminate_source_execution(
    conn: &mut AsyncPgConnection,
    source_exec_id: ExecutionId,
    new_exec_id: ExecutionId,
    request: &WorkflowResetRequest,
    source_next_event_id: i32,
) -> Result<(), WorkflowResetError> {
    crate::store::append_events(
        conn,
        source_exec_id,
        &[WorkflowEvent::WorkflowResetTerminated {
            reset_to_exec_id: new_exec_id,
            reason: request.reason.clone(),
            operator_id: request.operator_id.clone(),
        }],
        source_next_event_id,
    )
    .await?;

    diesel::update(harvest_workflow_executions::table.find(source_exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .set((
            harvest_workflow_executions::state.eq("TERMINATED"),
            harvest_workflow_executions::output.eq(None::<Value>),
            harvest_workflow_executions::error.eq(Some(format!(
                "workflow reset to {new_exec_id}: {}",
                request.reason
            ))),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;

    Ok(())
}

async fn insert_fork_execution(
    conn: &mut AsyncPgConnection,
    source: &WorkflowExecution,
    new_exec_id: ExecutionId,
) -> Result<WorkflowExecution, WorkflowResetError> {
    let row = NewWorkflowExecution {
        id: new_exec_id.as_uuid(),
        workflow_name: &source.workflow_name,
        workflow_id: &source.workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: source.shard_id,
        input: source.input.clone(),
        parent_id: None,
        queue_name: &source.queue_name,
        execution_timeout: source.execution_timeout,
        memo: source.memo.clone(),
        search_attrs: source.search_attrs.clone(),
    };

    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .returning(WorkflowExecution::as_returning())
        .get_result(conn)
        .await
        .map_err(database_error)
        .map_err(WorkflowResetError::from)
}

#[derive(Insertable)]
#[diesel(table_name = harvest_events)]
struct NewHarvestEventOwned {
    workflow_exec_id: Uuid,
    event_id: i32,
    event_type: String,
    event_data: Value,
}

async fn copy_carried_events(
    conn: &mut AsyncPgConnection,
    new_exec_id: ExecutionId,
    source_rows: &[HarvestEvent],
    reset_to_event_id: i64,
) -> Result<(), WorkflowResetError> {
    let rows = source_rows
        .iter()
        .filter(|row| i64::from(row.event_id) <= reset_to_event_id)
        .map(|row| NewHarvestEventOwned {
            workflow_exec_id: new_exec_id.as_uuid(),
            event_id: row.event_id,
            event_type: row.event_type.clone(),
            event_data: row.event_data.clone(),
        })
        .collect::<Vec<_>>();

    if rows.is_empty() {
        return Ok(());
    }

    diesel::insert_into(harvest_events::table)
        .values(&rows)
        .execute(conn)
        .await
        .map_err(database_error)?;
    Ok(())
}

async fn append_fork_marker(
    conn: &mut AsyncPgConnection,
    new_exec_id: ExecutionId,
    source_exec_id: ExecutionId,
    request: &WorkflowResetRequest,
    plan: &ResetPlan,
) -> Result<(), WorkflowResetError> {
    let marker_event_id = i32::try_from(plan.events_carried_over)
        .map_err(|_| HarvestError::Database("reset carried too many events".to_string()))?;
    crate::store::append_events(
        conn,
        new_exec_id,
        &[WorkflowEvent::WorkflowResetFork {
            reset_from_exec_id: source_exec_id,
            reset_to_event_id: request.reset_to_event_id,
            reason: request.reason.clone(),
            operator_id: request.operator_id.clone(),
        }],
        marker_event_id,
    )
    .await?;
    Ok(())
}

async fn remove_pending_timers(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Result<usize, WorkflowResetError> {
    diesel::delete(
        harvest_timers::table
            .filter(harvest_timers::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(harvest_timers::fired.eq(false)),
    )
    .execute(conn)
    .await
    .map_err(database_error)
    .map_err(WorkflowResetError::from)
}

async fn cancel_pending_external_tasks(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Result<usize, WorkflowResetError> {
    diesel::update(
        harvest_external_tasks::table
            .filter(harvest_external_tasks::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(harvest_external_tasks::state.eq("PENDING")),
    )
    .set(harvest_external_tasks::state.eq("CANCELLED"))
    .execute(conn)
    .await
    .map_err(database_error)
    .map_err(WorkflowResetError::from)
}

#[derive(Debug, Queryable, Selectable, Clone)]
#[diesel(table_name = harvest_signals)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct SignalForReset {
    id: Uuid,
    signal_name: String,
    payload: Value,
}

#[derive(Insertable)]
#[diesel(table_name = harvest_signals)]
struct NewSignalForReset {
    workflow_exec_id: Uuid,
    signal_name: String,
    payload: Value,
}

async fn reapply_or_drop_signals(
    conn: &mut AsyncPgConnection,
    source_exec_id: ExecutionId,
    new_exec_id: ExecutionId,
    policy: ResetSignalReapplyPolicy,
) -> Result<usize, WorkflowResetError> {
    let signals: Vec<SignalForReset> = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(source_exec_id.as_uuid()))
        .filter(harvest_signals::consumed.eq(false))
        .order((
            harvest_signals::received_at.asc(),
            harvest_signals::id.asc(),
        ))
        .select((
            harvest_signals::id,
            harvest_signals::signal_name,
            harvest_signals::payload,
        ))
        .load(conn)
        .await
        .map_err(database_error)?;

    if policy == ResetSignalReapplyPolicy::Buffer && !signals.is_empty() {
        let new_rows = signals
            .iter()
            .map(|signal| NewSignalForReset {
                workflow_exec_id: new_exec_id.as_uuid(),
                signal_name: signal.signal_name.clone(),
                payload: signal.payload.clone(),
            })
            .collect::<Vec<_>>();
        diesel::insert_into(harvest_signals::table)
            .values(&new_rows)
            .execute(conn)
            .await
            .map_err(database_error)?;
    }

    if !signals.is_empty() {
        let ids = signals.iter().map(|signal| signal.id).collect::<Vec<_>>();
        diesel::update(harvest_signals::table.filter(harvest_signals::id.eq_any(ids)))
            .set(harvest_signals::consumed.eq(true))
            .execute(conn)
            .await
            .map_err(database_error)?;
    }

    Ok(signals.len())
}

async fn enqueue_fork_workflow_task(
    conn: &mut AsyncPgConnection,
    fork: &WorkflowExecution,
    new_exec_id: ExecutionId,
) -> Result<(), WorkflowResetError> {
    let mut enqueue = EnqueueParams::new(
        fork.queue_name.clone(),
        TaskType::Workflow,
        fork.input.clone(),
    );
    enqueue.workflow_exec_id = Some(new_exec_id.as_uuid());
    queue::enqueue(conn, &enqueue).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::Value;

    use crate::event::WorkflowEvent;
    use crate::types::{ActivityExecId, ExecutionId, TimerId};

    use super::{ResetSignalReapplyPolicy, validate_reset_point};

    #[test]
    fn reset_point_allows_workflow_started_boundary() {
        let events = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        }];

        let plan = validate_reset_point(&events, 0).expect("workflow start is always valid");
        assert_eq!(plan.reset_to_event_id, 0);
        assert_eq!(plan.events_carried_over, 1);
        assert!(plan.unresolved_side_effects.is_empty());
    }

    #[test]
    fn reset_point_rejects_unresolved_activity_with_hint() {
        let activity_id = ActivityExecId::new();
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "charge_card".into(),
                input: Value::Null,
                queue: "billing".into(),
            },
            WorkflowEvent::MarkerRecorded {
                name: "after-schedule".into(),
                details: Value::Null,
            },
        ];

        let err = validate_reset_point(&events, 1).expect_err("activity is still unresolved");
        assert_eq!(err.reset_to_event_id, 1);
        assert_eq!(err.nearest_valid_before, Some(0));
        assert_eq!(err.nearest_valid_after, None);
        assert_eq!(err.unresolved_side_effects.len(), 1);
        assert_eq!(err.unresolved_side_effects[0].kind, "ActivityScheduled");
        assert_eq!(
            err.unresolved_side_effects[0].side_effect_id,
            activity_id.to_string()
        );
    }

    #[test]
    fn reset_point_allows_resolved_timer_boundary() {
        let timer_id = TimerId::new("cooldown");
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::TimerStarted {
                timer_id: timer_id.clone(),
                duration_secs: 30,
            },
            WorkflowEvent::TimerFired { timer_id },
        ];

        let plan = validate_reset_point(&events, 2).expect("timer has fired");
        assert_eq!(plan.reset_to_event_id, 2);
        assert_eq!(plan.events_carried_over, 3);
    }

    #[test]
    fn signal_reapply_policy_defaults_to_drop_and_parses_buffer() {
        assert_eq!(
            serde_json::from_str::<ResetSignalReapplyPolicy>("null").unwrap(),
            ResetSignalReapplyPolicy::Drop
        );
        assert_eq!(
            serde_json::from_str::<ResetSignalReapplyPolicy>(r#""buffer""#).unwrap(),
            ResetSignalReapplyPolicy::Buffer
        );
    }

    #[test]
    fn reset_point_rejects_continue_as_new_history() {
        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
            },
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: ExecutionId::new(),
                input: Value::Null,
            },
        ];

        let err =
            validate_reset_point(&events, 1).expect_err("continue-as-new reset is out of scope");
        assert!(
            err.message
                .contains("continue-as-new histories cannot be reset")
        );
    }
}
