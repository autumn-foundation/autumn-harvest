//! The durable agent through the [`HarvestSqlite`] handle.
//!
//! Register the agent with `HarvestPlugin::agent`. These calls then send
//! ops, read state and send decisions through the handle. They mirror the
//! durable agent calls of `autumn_harvest_agent::sqlite`, which drives a
//! `SqliteRuntime` of its own. Here the driver of the plugin runs each op, so
//! no call drives.
//!
//! An activity runs on the driver task. A call from an activity body can
//! wait for that same driver, and then it hangs. Make these calls from app
//! code.

use autumn_harvest::ExecutionId;
use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY};
use autumn_harvest_agent::{
    AGENT_WORKFLOW_NAME, Agent, AgentId, AgentOp, AgentState, Approval, Control,
};
use serde_json::Value;

use super::{
    ExecutionFilter, ExecutionSummary, HarvestSqlite, HarvestSqliteError, SignalWithStartOutcome,
    SignalWithStartParams, SqliteError,
};

/// Send `op` to the durable agent `id`. Start the agent when it has no run.
///
/// A steer or an interrupt goes first, as its own signal, to the running
/// run. One driver command finds that run and stages the signal. With no
/// running run, nothing is in flight, so the op alone is enough. Pass the
/// message id as `idempotency_key`, so a client retry delivers the op once.
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
    if let Some((name, payload)) = Agent::urgency_signal(op) {
        let key = idempotency_key.map(|key| format!("{name}:{key}"));
        match harvest
            .signal_by_workflow_id(AGENT_WORKFLOW_NAME, id.as_str(), name, payload, key)
            .await
        {
            Ok(_)
            | Err(HarvestSqliteError::Sqlite(
                SqliteError::WorkflowIdNotFound { .. } | SqliteError::WorkflowNotRunning { .. },
            )) => {}
            Err(error) => return Err(error),
        }
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

/// Send `Control::ReloadProfile` to each running agent of `kind`. Returns
/// the agents, sorted by id.
///
/// Register the new harness first. The driver of the plugin runs each
/// reload. The op goes to the running run only, so an agent with no running
/// run stays ended. Each call uses its own idempotency key. The filter is
/// the workflow id prefix `<kind>/`, as in `autumn_harvest_agent::sqlite`.
///
/// # Errors
///
/// Returns a list or a send error, or [`HarvestSqliteError::Stopped`].
/// Agents that got the op before it keep it.
pub async fn reload_profiles(
    harvest: &HarvestSqlite,
    kind: &str,
) -> Result<Vec<AgentId>, HarvestSqliteError> {
    let filter = ExecutionFilter::new()
        .workflow_name(AGENT_WORKFLOW_NAME)
        .state("RUNNING")
        .workflow_id_prefix(format!("{kind}/"));
    let mut agents = Vec::new();
    let mut cursor = None;
    loop {
        let page = cursor.map_or_else(|| filter.clone(), |cursor| filter.clone().before(cursor));
        let found = harvest.list(page).await?;
        agents.extend(found.executions.iter().filter_map(|summary| {
            AgentId::parse(summary.workflow_id.value()?)
                .ok()
                .filter(|id| id.kind() == kind)
        }));
        match found.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    agents.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    agents.dedup();
    let key = format!(
        "reload-profiles:{kind}:{}",
        chrono::Utc::now().timestamp_millis()
    );
    let payload = Agent::payload(&AgentOp::Control(Control::ReloadProfile));
    for id in &agents {
        match harvest
            .signal_by_workflow_id(
                AGENT_WORKFLOW_NAME,
                id.as_str(),
                ENTITY_OP_SIGNAL,
                payload.clone(),
                Some(key.clone()),
            )
            .await
        {
            Ok(_)
            | Err(HarvestSqliteError::Sqlite(
                SqliteError::WorkflowIdNotFound { .. } | SqliteError::WorkflowNotRunning { .. },
            )) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(agents)
}

/// The running execution of the agent `id`, if it has one.
///
/// # Errors
///
/// Returns a read error, or [`HarvestSqliteError::Stopped`].
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
/// Returns `false` when `idempotency_key` was already delivered. To decide
/// a one-shot `agent_loop` run, call `HarvestSqlite::signal` with its id.
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
