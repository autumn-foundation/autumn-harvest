//! External activity task management — token-based async completion.
//!
//! When a workflow calls `execute_activity_external`, the worker records a row
//! here that maps the opaque `ExternalActivityToken` to the pending
//! `(workflow_exec_id, activity_id)`.  The management API resolves that mapping
//! in O(log n) via the `idx_harvest_ext_token` index and then appends the
//! appropriate terminal event and wakes the workflow.

use chrono::Utc;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::error::{HarvestError, HarvestResult, database_error};
use crate::event::WorkflowEvent;
use crate::models::{ExternalTask, NewExternalTask};
use crate::schema::{harvest_external_tasks, harvest_workflow_executions};
use crate::store;
use crate::types::{ActivityExecId, ExecutionId, ExternalActivityToken};

/// External activity states persisted in `harvest_external_tasks`.
pub const KNOWN_EXTERNAL_TASK_STATES: &[&str] =
    &["PENDING", "COMPLETED", "FAILED", "TIMED_OUT", "CANCELLED"];

/// Filters for operator-facing external activity handoff queries.
#[derive(Debug, Clone)]
pub struct ExternalHandoffFilters {
    pub states: Vec<String>,
    pub workflow_name: Option<String>,
    pub execution_id: Option<ExecutionId>,
    pub activity_name: Option<String>,
    pub token: Option<ExternalActivityToken>,
    pub shard_id: Option<i32>,
    pub due_before: Option<chrono::DateTime<Utc>>,
    pub updated_before: Option<chrono::DateTime<Utc>>,
    pub limit: i64,
}

impl Default for ExternalHandoffFilters {
    fn default() -> Self {
        Self {
            states: Vec::new(),
            workflow_name: None,
            execution_id: None,
            activity_name: None,
            token: None,
            shard_id: None,
            due_before: None,
            updated_before: None,
            limit: 100,
        }
    }
}

impl ExternalHandoffFilters {
    #[must_use]
    pub const fn with_limit(mut self, limit: i64) -> Self {
        self.limit = limit;
        self
    }
}

/// Redacted, operator-facing external handoff row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalHandoffRow {
    pub token: ExternalActivityToken,
    pub workflow_exec_id: ExecutionId,
    pub workflow_id: String,
    pub workflow_name: String,
    pub activity_id: ActivityExecId,
    pub activity_name: String,
    pub state: String,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
    pub deadline_at: chrono::DateTime<Utc>,
    pub shard_id: i32,
}

type ExternalHandoffProjection = (
    uuid::Uuid,
    uuid::Uuid,
    String,
    String,
    uuid::Uuid,
    String,
    String,
    chrono::DateTime<Utc>,
    chrono::DateTime<Utc>,
    chrono::DateTime<Utc>,
    i32,
);

impl From<ExternalHandoffProjection> for ExternalHandoffRow {
    fn from(row: ExternalHandoffProjection) -> Self {
        let (
            token,
            workflow_exec_id,
            workflow_id,
            workflow_name,
            activity_id,
            activity_name,
            state,
            created_at,
            updated_at,
            deadline_at,
            shard_id,
        ) = row;
        Self {
            token: ExternalActivityToken::from_uuid(token),
            workflow_exec_id: ExecutionId::from_uuid(workflow_exec_id),
            workflow_id,
            workflow_name,
            activity_id: ActivityExecId::from_uuid(activity_id),
            activity_name,
            state,
            created_at,
            updated_at,
            deadline_at,
            shard_id,
        }
    }
}

/// Insert a new external-task row, linking `token` → `(exec_id, activity_id)`.
///
/// Uses `ON CONFLICT DO NOTHING` so replaying a workflow that already recorded
/// the awaiting event is safely idempotent.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the INSERT fails.
pub async fn record_external_task(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    token: ExternalActivityToken,
    activity_id: ActivityExecId,
    name: &str,
    queue: &str,
    schedule_to_close_secs: u64,
) -> HarvestResult<()> {
    let dur = crate::worker::chrono_duration_from_secs(
        schedule_to_close_secs,
        "external task schedule to close",
    )?;

    let schedule_to_close_at = Utc::now().checked_add_signed(dur).ok_or_else(|| {
        crate::error::HarvestError::Database("Datetime addition overflow".to_string())
    })?;

    let row = NewExternalTask {
        token: token.as_uuid(),
        workflow_exec_id: exec_id.as_uuid(),
        activity_id: activity_id.as_uuid(),
        name,
        queue,
        schedule_to_close_at,
        schedule_to_close_secs: i64::try_from(schedule_to_close_secs).unwrap_or(i64::MAX),
    };

    diesel::insert_into(harvest_external_tasks::table)
        .values(&row)
        .on_conflict(harvest_external_tasks::token)
        .do_nothing()
        .execute(conn)
        .await
        .map_err(database_error)?;

    Ok(())
}

/// Look up an external task by its opaque token.
///
/// Returns `None` when the token is unknown (wrong shard, already deleted, etc.).
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the query fails.
pub async fn find_by_token(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
) -> HarvestResult<Option<ExternalTask>> {
    harvest_external_tasks::table
        .filter(harvest_external_tasks::token.eq(token.as_uuid()))
        .select(ExternalTask::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// List external activity handoffs for operator read surfaces.
///
/// Rows intentionally expose only task identity, token, state, and timing
/// metadata. Raw workflow/activity payloads remain in event history and are
/// not selected here.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the query fails.
pub async fn list_external_handoffs(
    conn: &mut AsyncPgConnection,
    filters: &ExternalHandoffFilters,
) -> HarvestResult<Vec<ExternalHandoffRow>> {
    let mut query = harvest_external_tasks::table
        .inner_join(harvest_workflow_executions::table)
        .select((
            harvest_external_tasks::token,
            harvest_external_tasks::workflow_exec_id,
            harvest_workflow_executions::workflow_id,
            harvest_workflow_executions::workflow_name,
            harvest_external_tasks::activity_id,
            harvest_external_tasks::name,
            harvest_external_tasks::state,
            harvest_external_tasks::created_at,
            harvest_external_tasks::updated_at,
            harvest_external_tasks::schedule_to_close_at,
            harvest_workflow_executions::shard_id,
        ))
        .into_boxed::<diesel::pg::Pg>();

    if !filters.states.is_empty() {
        query = query.filter(harvest_external_tasks::state.eq_any(filters.states.clone()));
    }
    if let Some(workflow_name) = &filters.workflow_name {
        query = query.filter(harvest_workflow_executions::workflow_name.eq(workflow_name));
    }
    if let Some(exec_id) = filters.execution_id {
        query = query.filter(harvest_external_tasks::workflow_exec_id.eq(exec_id.as_uuid()));
    }
    if let Some(activity_name) = &filters.activity_name {
        query = query.filter(harvest_external_tasks::name.eq(activity_name));
    }
    if let Some(token) = filters.token {
        query = query.filter(harvest_external_tasks::token.eq(token.as_uuid()));
    }
    if let Some(shard_id) = filters.shard_id {
        query = query.filter(harvest_workflow_executions::shard_id.eq(shard_id));
    }
    if let Some(due_before) = filters.due_before {
        query = query.filter(harvest_external_tasks::schedule_to_close_at.le(due_before));
    }
    if let Some(updated_before) = filters.updated_before {
        query = query.filter(harvest_external_tasks::updated_at.le(updated_before));
    }

    query
        .order((
            harvest_external_tasks::schedule_to_close_at.asc(),
            harvest_external_tasks::updated_at.desc(),
            harvest_external_tasks::token.asc(),
        ))
        .limit(filters.limit)
        .load::<ExternalHandoffProjection>(conn)
        .await
        .map_err(database_error)
        .map(|rows| rows.into_iter().map(ExternalHandoffRow::from).collect())
}

/// Look up one operator-facing external handoff by token.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the query fails.
pub async fn find_handoff_by_token(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
) -> HarvestResult<Option<ExternalHandoffRow>> {
    let filters = ExternalHandoffFilters {
        token: Some(token),
        limit: 1,
        ..ExternalHandoffFilters::default()
    };
    Ok(list_external_handoffs(conn, &filters)
        .await?
        .into_iter()
        .next())
}

/// Look up an external task by its opaque token, acquiring a row-level lock.
///
/// Like [`find_by_token`] but uses `FOR UPDATE`, serializing the caller
/// against concurrent writers (complete/fail) that also lock this row.
/// Must be called inside a transaction.
///
/// Returns `None` when the token is unknown (wrong shard, already deleted, etc.).
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the query fails.
pub async fn find_by_token_locked(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
) -> HarvestResult<Option<ExternalTask>> {
    harvest_external_tasks::table
        .filter(harvest_external_tasks::token.eq(token.as_uuid()))
        .for_update()
        .select(ExternalTask::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// Mark an external activity as successfully completed.
///
/// Appends `ActivityCompletedExternally` to the event history and wakes the
/// parked workflow task.  Returns `true` if the state transition happened,
/// `false` if the token was already in a terminal state (idempotent).
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when `token` is unknown on this shard.
pub async fn complete_externally(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
    output: serde_json::Value,
) -> HarvestResult<bool> {
    conn.transaction::<bool, HarvestError, _>(async |conn| {
        let task = lock_task(conn, token).await?;

        if task.state != "PENDING" {
            return Ok(false);
        }

        let exec_id = ExecutionId::from_uuid(task.workflow_exec_id);
        let activity_id = ActivityExecId::from_uuid(task.activity_id);

        diesel::update(harvest_external_tasks::table.find(task.id))
            .set((
                harvest_external_tasks::state.eq("COMPLETED"),
                harvest_external_tasks::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(database_error)?;

        let event = WorkflowEvent::ActivityCompletedExternally {
            activity_id,
            token,
            output,
        };
        store::append_single_event(conn, exec_id, event).await?;
        crate::queue::wake_workflow_task(conn, exec_id).await?;

        Ok(true)
    })
    .await
}

/// Mark an external activity as failed.
///
/// Appends `ActivityFailedExternally` to the event history and wakes the
/// parked workflow task.  Returns `true` if the state transition happened,
/// `false` if the token was already in a terminal state (idempotent).
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when `token` is unknown on this shard.
pub async fn fail_externally(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
    error: String,
    retryable: bool,
) -> HarvestResult<bool> {
    conn.transaction::<bool, HarvestError, _>(async |conn| {
        let task = lock_task(conn, token).await?;

        if task.state != "PENDING" {
            return Ok(false);
        }

        let exec_id = ExecutionId::from_uuid(task.workflow_exec_id);
        let activity_id = ActivityExecId::from_uuid(task.activity_id);

        diesel::update(harvest_external_tasks::table.find(task.id))
            .set((
                harvest_external_tasks::state.eq("FAILED"),
                harvest_external_tasks::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(database_error)?;

        let event = WorkflowEvent::ActivityFailedExternally {
            activity_id,
            token,
            error,
            retryable,
        };
        store::append_single_event(conn, exec_id, event).await?;
        crate::queue::wake_workflow_task(conn, exec_id).await?;

        Ok(true)
    })
    .await
}

/// Extend the deadline for a pending external activity.
///
/// Appends `ActivityExternalDeadlineExtended` to the event history and updates
/// `schedule_to_close_at` by `extend_by_secs` seconds from now.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when `token` is unknown on this shard.
/// Returns [`HarvestError::Config`] if the task is already in a terminal state.
pub async fn extend_deadline(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
    extend_by_secs: u64,
) -> HarvestResult<()> {
    conn.transaction::<(), HarvestError, _>(async |conn| {
        let task = lock_task(conn, token).await?;

        if task.state != "PENDING" {
            return Err(HarvestError::Config(format!(
                "external task {token} is already terminal ({}); cannot extend deadline",
                task.state
            )));
        }

        let exec_id = ExecutionId::from_uuid(task.workflow_exec_id);
        let activity_id = ActivityExecId::from_uuid(task.activity_id);

        let dur =
            crate::worker::chrono_duration_from_secs(extend_by_secs, "external task extend by")?;

        let new_deadline = Utc::now().checked_add_signed(dur).ok_or_else(|| {
            crate::error::HarvestError::Database("Datetime addition overflow".to_string())
        })?;

        diesel::update(harvest_external_tasks::table.find(task.id))
            .set((
                harvest_external_tasks::schedule_to_close_at.eq(new_deadline),
                harvest_external_tasks::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(database_error)?;

        let event = WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, token };
        store::append_single_event(conn, exec_id, event).await?;

        Ok(())
    })
    .await
}

async fn lock_task(
    conn: &mut AsyncPgConnection,
    token: ExternalActivityToken,
) -> HarvestResult<ExternalTask> {
    harvest_external_tasks::table
        .filter(harvest_external_tasks::token.eq(token.as_uuid()))
        .for_update()
        .select(ExternalTask::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("external task token {token}")))
}
