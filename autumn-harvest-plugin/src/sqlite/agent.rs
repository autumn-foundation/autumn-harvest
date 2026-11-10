//! The durable agent through the [`HarvestSqlite`] handle.
//!
//! Register the agent with `HarvestPlugin::agent`. These calls then send
//! ops, read state and send decisions through the handle. They mirror
//! `autumn_harvest_agent::sqlite`, which drives a `SqliteRuntime` of its own.
//! Here the driver of the plugin runs each op, so no call drives.

use autumn_harvest::ExecutionId;
use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY};
use autumn_harvest_agent::{AGENT_WORKFLOW_NAME, Agent, AgentId, AgentOp, AgentState, Approval};
use serde_json::Value;

use super::{
    ExecutionFilter, ExecutionSummary, HarvestSqlite, HarvestSqliteError, SignalWithStartOutcome,
    SignalWithStartParams, SqliteError,
};

/// Send `op` to the durable agent `id`. Start the agent when it has no run.
///
/// A steer or an interrupt goes first, as its own signal, to the running
/// run. That run then sees it while an op is in flight. Pass the message id
/// as `idempotency_key`, so a client retry delivers the op once.
///
/// # Errors
///
/// Returns the refusal of the backend, or [`HarvestSqliteError::Stopped`].
pub async fn send(
    harvest: &HarvestSqlite,
    id: &AgentId,
    op: &AgentOp,
    idempotency_key: Option<&str>,
) -> Result<SignalWithStartOutcome, HarvestSqliteError> {
    // With no run, nothing is in flight, and the op alone is enough.
    if let Some((name, payload)) = Agent::urgency_signal(op)
        && let Some(exec) = current_run(harvest, id).await?
    {
        let key = idempotency_key.map(|key| format!("{name}:{key}"));
        harvest.signal(exec, name, payload, key).await?;
    }
    signal(harvest, id, Agent::payload(op), idempotency_key).await
}

/// Delete the durable agent `id`. Its run ends once its inbox is empty.
///
/// # Errors
///
/// Returns the refusal of the backend, or [`HarvestSqliteError::Stopped`].
pub async fn delete(
    harvest: &HarvestSqlite,
    id: &AgentId,
    idempotency_key: Option<&str>,
) -> Result<SignalWithStartOutcome, HarvestSqliteError> {
    signal(harvest, id, Agent::delete_payload(), idempotency_key).await
}

/// Stage one entity message for the agent `id`.
async fn signal(
    harvest: &HarvestSqlite,
    id: &AgentId,
    payload: Value,
    idempotency_key: Option<&str>,
) -> Result<SignalWithStartOutcome, HarvestSqliteError> {
    harvest
        .signal_with_start(SignalWithStartParams {
            workflow_name: AGENT_WORKFLOW_NAME.to_owned(),
            workflow_id: id.as_str().to_owned(),
            // SQLite keeps a caller input, so start from an empty checkpoint.
            input: serde_json::json!({}),
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            signal_name: ENTITY_OP_SIGNAL.to_owned(),
            signal_payload: payload,
            idempotency_key: idempotency_key.map(ToOwned::to_owned),
        })
        .await
}

/// The running execution of the agent `id`, if it has one.
///
/// # Errors
///
/// Returns a read error.
pub async fn current_run(
    harvest: &HarvestSqlite,
    id: &AgentId,
) -> Result<Option<ExecutionId>, HarvestSqliteError> {
    let filter = ExecutionFilter::new()
        .workflow_name(AGENT_WORKFLOW_NAME)
        .workflow_id(id.as_str())
        .state("RUNNING")
        .limit(1);
    let page = harvest.list(filter).await?;
    Ok(page
        .executions
        .iter()
        .find_map(ExecutionSummary::execution_id))
}

/// The committed state of the agent `id`. Returns `None` when the agent has
/// no running run.
///
/// # Errors
///
/// Returns the query error, or an error when the state does not decode.
pub async fn state(
    harvest: &HarvestSqlite,
    id: &AgentId,
) -> Result<Option<AgentState>, HarvestSqliteError> {
    let Some(exec) = current_run(harvest, id).await? else {
        return Ok(None);
    };
    let value = harvest.query(exec, ENTITY_STATE_QUERY, Value::Null).await?;
    let state = serde_json::from_value(value).map_err(SqliteError::from)?;
    Ok(Some(state))
}

/// Send a reviewer decision to the approval wait that `signal` names, on
/// the running run of the agent `id`.
///
/// The approval notice of the harness names `signal`. A decision for a wait
/// that already timed out stays unread. It never releases a later call.
/// Returns `false` when `idempotency_key` was already delivered.
///
/// # Errors
///
/// Returns the refusal of the backend, such as an agent with no running
/// run, or [`HarvestSqliteError::Stopped`].
pub async fn decide(
    harvest: &HarvestSqlite,
    id: &AgentId,
    signal: &str,
    approval: &Approval,
    idempotency_key: Option<&str>,
) -> Result<bool, HarvestSqliteError> {
    let payload = serde_json::to_value(approval).map_err(SqliteError::from)?;
    let delivery = harvest
        .signal_by_workflow_id(
            AGENT_WORKFLOW_NAME,
            id.as_str(),
            signal,
            payload,
            idempotency_key.map(ToOwned::to_owned),
        )
        .await?;
    Ok(delivery.delivered)
}
