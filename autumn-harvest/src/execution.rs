//! Workflow execution persistence helpers.
//!
//! The public start helper in this module gives callers idempotent workflow
//! start semantics scoped to `(workflow_name, workflow_id)`.

use chrono::Utc;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use scoped_futures::ScopedFutureExt;
use uuid::Uuid;

use crate::error::{HarvestError, HarvestResult, database_error};
use crate::event::WorkflowEvent;
use crate::models::{NewWorkflowExecution, WorkflowExecution};
use crate::queue::{self, EnqueueParams, TaskType};
use crate::schema::harvest_workflow_executions;
use crate::store;
use crate::types::ExecutionId;

/// Parameters for starting a workflow execution.
///
/// `exec_id` is the workflow's routing key: its UUID carries the target
/// [`crate::types::ShardId`] in its first two bytes (see
/// [`ExecutionId::new_for_shard`]). In multi-shard deployments the caller
/// picks the shard via [`crate::ShardRouter`] and mints the id with
/// [`ExecutionId::new_for_shard`] before calling this helper. Single-shard
/// deployments can pass `ExecutionId::new_for_shard(ShardId::new(0))` or, for
/// tests and non-production code, the sentinel-producing `ExecutionId::new()`.
#[derive(Debug, Clone)]
pub struct StartWorkflowParams<'a> {
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub exec_id: ExecutionId,
    pub input: serde_json::Value,
    pub parent_id: Option<Uuid>,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
}

impl StartWorkflowParams<'_> {
    /// Shard derived from the encoded `exec_id`, used to populate the row's
    /// `shard_id` column. Returns `0` when the caller passed an unencoded id
    /// (tests / legacy call sites), matching the pre-sharding default.
    #[must_use]
    pub fn shard_id(&self) -> i32 {
        let shard = self.exec_id.shard();
        if shard.is_unencoded() {
            0
        } else {
            shard.as_i32()
        }
    }
}

/// Result of an idempotent workflow start attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedWorkflowExecution {
    pub exec_id: ExecutionId,
    pub workflow_name: String,
    pub workflow_id: String,
    pub state: String,
    pub created: bool,
}

impl StartedWorkflowExecution {
    fn from_row(execution: WorkflowExecution, created: bool) -> Self {
        Self {
            exec_id: ExecutionId::from_uuid(execution.id),
            workflow_name: execution.workflow_name,
            workflow_id: execution.workflow_id,
            state: execution.state,
            created,
        }
    }
}

/// Result of a workflow cancellation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelledWorkflowExecution {
    /// Cancelled workflow execution ID.
    pub exec_id: ExecutionId,
    /// Final execution state.
    pub state: String,
    /// Stored cancellation reason.
    pub reason: String,
    /// `true` when this request performed the terminal transition.
    pub newly_cancelled: bool,
    /// Number of pending/running task rows failed by this request.
    pub failed_task_count: usize,
}

impl CancelledWorkflowExecution {
    fn idempotent(exec_id: ExecutionId, execution: WorkflowExecution) -> Self {
        Self {
            exec_id,
            state: execution.state,
            reason: execution
                .error
                .unwrap_or_else(|| "workflow already cancelled".to_string()),
            newly_cancelled: false,
            failed_task_count: 0,
        }
    }

    fn newly_cancelled(exec_id: ExecutionId, reason: String, failed_task_count: usize) -> Self {
        Self {
            exec_id,
            state: "CANCELLED".to_string(),
            reason,
            newly_cancelled: true,
            failed_task_count,
        }
    }
}

/// Start a workflow execution or load the existing one if the same
/// `(workflow_name, workflow_id)` has already been published.
///
/// New starts insert the execution row, append `WorkflowStarted`, and enqueue
/// the initial workflow task in one transaction. Duplicate starts return the
/// previously-created execution without appending extra events or queue work.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] for insert/query failures and propagates
/// queue/event-store failures from the new-start transaction.
pub async fn start_or_load_workflow_execution(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
) -> HarvestResult<StartedWorkflowExecution> {
    let exec_id = request.exec_id;
    let shard_id_value = request.shard_id();
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: request.workflow_name,
        workflow_id: request.workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: shard_id_value,
        input: request.input.clone(),
        parent_id: request.parent_id,
        queue_name: request.queue_name,
        execution_timeout: request.execution_timeout,
        memo: request.memo.clone(),
        search_attrs: request.search_attrs.clone(),
    };
    let mut enqueue = EnqueueParams::new(
        request.queue_name.to_owned(),
        TaskType::Workflow,
        request.input.clone(),
    );
    enqueue.workflow_exec_id = Some(exec_id.as_uuid());

    conn.transaction::<StartedWorkflowExecution, HarvestError, _>(|conn| {
        let row = row;
        let enqueue = enqueue.clone();
        let request = request.clone();
        async move {
            // `on_conflict_do_nothing()` (no explicit target) lets Postgres
            // arbitrate against the partial unique index installed by the
            // continue-as-new migration, which only enforces uniqueness on
            // rows whose state is not `CONTINUED_AS_NEW`. A previously sealed
            // continue-as-new chain therefore does not block reusing the same
            // (workflow_name, workflow_id).
            let inserted = diesel::insert_into(harvest_workflow_executions::table)
                .values(&row)
                .on_conflict_do_nothing()
                .returning(WorkflowExecution::as_returning())
                .get_result(conn)
                .await
                .optional()
                .map_err(database_error)?;

            if let Some(execution) = inserted {
                let started_event = WorkflowEvent::WorkflowStarted {
                    input: request.input.clone(),
                    timestamp: Utc::now(),
                };
                store::append_events(conn, exec_id, &[started_event], 0).await?;
                queue::enqueue(conn, &enqueue).await?;
                return Ok(StartedWorkflowExecution::from_row(execution, true));
            }

            let execution =
                load_workflow_execution_by_key(conn, request.workflow_name, request.workflow_id)
                    .await?;
            Ok(StartedWorkflowExecution::from_row(execution, false))
        }
        .scope_boxed()
    })
    .await
}

/// Cancel a running workflow execution.
///
/// Cancellation is a durable terminal transition: this appends a
/// `WorkflowCancelled` event, marks the execution `CANCELLED`, and fails every
/// pending or running task associated with the execution. Repeating the same
/// operation against an already-cancelled execution is idempotent and does not
/// append another event.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the execution does not exist,
/// [`HarvestError::Config`] when the execution is already terminal for another
/// reason, and [`HarvestError::Database`] for persistence failures.
pub async fn cancel_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<CancelledWorkflowExecution> {
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "workflow cancellation requested".to_string()
    } else {
        reason.to_string()
    };

    conn.transaction::<CancelledWorkflowExecution, HarvestError, _>(|conn| {
        async move {
            let execution = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .select(WorkflowExecution::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(database_error)?
                .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

            match execution.state.as_str() {
                "RUNNING" => {}
                "CANCELLED" => {
                    return Ok(CancelledWorkflowExecution::idempotent(exec_id, execution));
                }
                state => {
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is already terminal ({state})"
                    )));
                }
            }

            let history = store::load_history(conn, exec_id).await?;
            store::append_events(
                conn,
                exec_id,
                &[WorkflowEvent::WorkflowCancelled {
                    reason: reason.clone(),
                }],
                history.next_event_id,
            )
            .await?;

            let updated =
                diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                    .filter(harvest_workflow_executions::state.eq("RUNNING"))
                    .set((
                        harvest_workflow_executions::state.eq("CANCELLED"),
                        harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                        harvest_workflow_executions::error.eq(Some(reason.clone())),
                        harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
                    ))
                    .execute(conn)
                    .await
                    .map_err(database_error)?;

            if updated == 0 {
                return Err(HarvestError::Config(format!(
                    "workflow execution {exec_id} is no longer running"
                )));
            }

            let failed_task_count = queue::fail_open_tasks_for_execution(
                conn,
                exec_id,
                &format!("workflow cancelled: {reason}"),
            )
            .await?;

            Ok(CancelledWorkflowExecution::newly_cancelled(
                exec_id,
                reason,
                failed_task_count,
            ))
        }
        .scope_boxed()
    })
    .await
}

async fn load_workflow_execution_by_key(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<WorkflowExecution> {
    // After continue-as-new, several rows may share the same
    // (workflow_name, workflow_id): every sealed run carries
    // state='CONTINUED_AS_NEW' and only one row remains active. Filtering on
    // state mirrors the partial unique index and returns the row that callers
    // expect to keep operating against.
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_workflow_executions::state.ne("CONTINUED_AS_NEW"))
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| {
            HarvestError::NotFound(format!("workflow execution {workflow_name}/{workflow_id}"))
        })
}
