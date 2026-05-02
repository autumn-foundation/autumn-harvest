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
use scoped_futures::ScopedFutureExt;

use crate::error::{HarvestError, HarvestResult, database_error};
use crate::event::WorkflowEvent;
use crate::models::{ExternalTask, NewExternalTask};
use crate::schema::harvest_external_tasks;
use crate::store;
use crate::types::{ActivityExecId, ExecutionId, ExternalActivityToken};

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
    conn.transaction::<bool, HarvestError, _>(|conn| {
        async move {
            let task = lock_task(conn, token).await?;

            if task.state != "PENDING" {
                return Ok(false);
            }

            let exec_id = ExecutionId::from_uuid(task.workflow_exec_id);
            let activity_id = ActivityExecId::from_uuid(task.activity_id);

            diesel::update(harvest_external_tasks::table.find(task.id))
                .set(harvest_external_tasks::state.eq("COMPLETED"))
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
        }
        .scope_boxed()
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
    conn.transaction::<bool, HarvestError, _>(|conn| {
        async move {
            let task = lock_task(conn, token).await?;

            if task.state != "PENDING" {
                return Ok(false);
            }

            let exec_id = ExecutionId::from_uuid(task.workflow_exec_id);
            let activity_id = ActivityExecId::from_uuid(task.activity_id);

            diesel::update(harvest_external_tasks::table.find(task.id))
                .set(harvest_external_tasks::state.eq("FAILED"))
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
        }
        .scope_boxed()
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
    conn.transaction::<(), HarvestError, _>(|conn| {
        async move {
            let task = lock_task(conn, token).await?;

            if task.state != "PENDING" {
                return Err(HarvestError::Config(format!(
                    "external task {token} is already terminal ({}); cannot extend deadline",
                    task.state
                )));
            }

            let exec_id = ExecutionId::from_uuid(task.workflow_exec_id);
            let activity_id = ActivityExecId::from_uuid(task.activity_id);

            let dur = crate::worker::chrono_duration_from_secs(
                extend_by_secs,
                "external task extend by",
            )?;

            let new_deadline = Utc::now().checked_add_signed(dur).ok_or_else(|| {
                crate::error::HarvestError::Database("Datetime addition overflow".to_string())
            })?;

            diesel::update(harvest_external_tasks::table.find(task.id))
                .set(harvest_external_tasks::schedule_to_close_at.eq(new_deadline))
                .execute(conn)
                .await
                .map_err(database_error)?;

            let event = WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, token };
            store::append_single_event(conn, exec_id, event).await?;

            Ok(())
        }
        .scope_boxed()
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
