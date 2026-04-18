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
#[derive(Debug, Clone)]
pub struct StartWorkflowParams<'a> {
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub shard_id: i32,
    pub input: serde_json::Value,
    pub parent_id: Option<Uuid>,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
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
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: request.workflow_name,
        workflow_id: request.workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: request.shard_id,
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
            let inserted = diesel::insert_into(harvest_workflow_executions::table)
                .values(&row)
                .on_conflict((
                    harvest_workflow_executions::workflow_name,
                    harvest_workflow_executions::workflow_id,
                ))
                .do_nothing()
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

async fn load_workflow_execution_by_key(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<WorkflowExecution> {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| {
            HarvestError::NotFound(format!("workflow execution {workflow_name}/{workflow_id}"))
        })
}
