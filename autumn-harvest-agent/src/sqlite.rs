//! The agent loop on the embedded SQLite backend.
//!
//! The harness lives in process state, and `ctx.state::<T>()` is always `None`
//! on this backend. [`register`] therefore wraps each harness method in a
//! closure for `register_activity_with_body`. The closure
//! runs the async body with `block_in_place` on the current Tokio runtime, so
//! drive the runtime from a multi-thread Tokio runtime.

use std::sync::Arc;

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY};
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest_sqlite::{
    ExecutionFilter, ExecutionId, RunState, SignalWithStart, SignalWithStartOutcome, SqliteResult,
    SqliteRuntime,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::agent::{
    AGENT_WORKFLOW_NAME, Agent, AgentId, AgentOp, AgentState, agent_info, agent_memory_reset_info,
    agent_profile_info,
};
use crate::approval::Approval;
use crate::harness::AgentHarness;
use crate::heartbeat::{HEARTBEAT_WORKFLOW_NAME, HeartbeatTask, agent_heartbeat_info};
use crate::types::AgentTask;
use crate::workflow::{
    WORKFLOW_NAME, agent_deliver_info, agent_discover_peers_info, agent_loop_info,
    agent_memory_snapshot_info, agent_model_turn_info, agent_precheck_info, agent_tool_call_info,
};

/// Register the agent workflows and their activities against `harness`.
///
/// Call it once per runtime, before the first drive. A runtime that reopens a
/// database must register again before it resumes a run.
pub fn register(rt: &mut SqliteRuntime, harness: Arc<AgentHarness>) {
    rt.register_workflow(&agent_loop_info());
    rt.register_workflow(&agent_heartbeat_info());
    rt.register_workflow(&agent_info());
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_profile_info(), move |input| {
        call(input, |kind: String| {
            std::future::ready(Ok::<_, String>(h.agent_profile(&kind)))
        })
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_memory_reset_info(), move |input| {
        call(input, |scope| h.memory_reset(scope))
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_model_turn_info(), move |input| {
        call(input, |request| h.model_turn(request))
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_tool_call_info(), move |input| {
        call(input, |request| h.tool_call(request))
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_memory_snapshot_info(), move |input| {
        call(input, |scope| h.memory_snapshot(scope))
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_deliver_info(), move |input| {
        call(input, |report| h.deliver(report))
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_discover_peers_info(), move |input| {
        call(input, |names: Vec<String>| {
            std::future::ready(Ok::<_, String>(h.discover_peers(&names)))
        })
    });
    rt.register_activity_with_body(&agent_precheck_info(), move |input| {
        call(input, |task| harness.precheck_tick(task))
    });
}

/// Start one agent run. Returns its execution id.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the start.
pub fn start(rt: &mut SqliteRuntime, task: &AgentTask) -> SqliteResult<ExecutionId> {
    rt.start_workflow(WORKFLOW_NAME, serde_json::to_value(task)?)
}

/// Start one heartbeat tick. Returns its execution id.
///
/// Call it from the app on each tick. To let the engine start the ticks,
/// pass `heartbeat::schedule` to `SqliteRuntime::upsert_schedule` instead
/// (issue #2031).
///
/// # Errors
///
/// Returns an error when the runtime cannot record the start.
pub fn start_heartbeat(rt: &mut SqliteRuntime, task: &HeartbeatTask) -> SqliteResult<ExecutionId> {
    rt.start_workflow(HEARTBEAT_WORKFLOW_NAME, serde_json::to_value(task)?)
}

/// Send a reviewer decision to the wait that `signal` names.
///
/// Read `signal` from `RunState::WaitingSignal`. A decision for a wait that
/// already timed out stays unread. It never releases a later call.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the signal.
pub fn decide(
    rt: &mut SqliteRuntime,
    exec: ExecutionId,
    signal: &str,
    approval: &Approval,
) -> SqliteResult<()> {
    rt.send_signal(exec, signal, serde_json::to_value(approval)?)
}

/// Send `op` to the durable agent `id`. Start the agent when it has no run.
///
/// The send does not drive the agent. Call [`drive`] for that. Pass the
/// message id as `idempotency_key`, so a client retry delivers the op once.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the signal.
pub fn send(
    rt: &mut SqliteRuntime,
    id: &AgentId,
    op: &AgentOp,
    idempotency_key: Option<&str>,
) -> SqliteResult<SignalWithStartOutcome> {
    // A steer or an interrupt goes first, to the run that is in flight. With
    // no run, nothing is in flight, and the op alone is enough.
    if let Some((name, payload)) = Agent::urgency_signal(op)
        && let Some(exec) = current_run(rt, id)?
    {
        let key = idempotency_key.map(|key| format!("{name}:{key}"));
        rt.send_signal_idempotent(exec, name, payload, key.as_deref())?;
    }
    signal(rt, id, Agent::payload(op), idempotency_key)
}

/// Delete the durable agent `id`. Its run ends once its inbox is empty.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the signal.
pub fn delete(
    rt: &mut SqliteRuntime,
    id: &AgentId,
    idempotency_key: Option<&str>,
) -> SqliteResult<SignalWithStartOutcome> {
    signal(rt, id, Agent::delete_payload(), idempotency_key)
}

/// Stage one entity message for the agent `id`.
fn signal(
    rt: &mut SqliteRuntime,
    id: &AgentId,
    payload: Value,
    idempotency_key: Option<&str>,
) -> SqliteResult<SignalWithStartOutcome> {
    rt.signal_with_start(SignalWithStart {
        workflow_name: AGENT_WORKFLOW_NAME,
        workflow_id: id.as_str(),
        // SQLite keeps a caller input, so start from an empty checkpoint.
        input: serde_json::json!({}),
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        signal_name: ENTITY_OP_SIGNAL,
        signal_payload: payload,
        idempotency_key,
    })
}

/// The running execution of the agent `id`, if it has one.
///
/// # Errors
///
/// Returns an error when the runtime cannot read its index.
pub fn current_run(rt: &SqliteRuntime, id: &AgentId) -> SqliteResult<Option<ExecutionId>> {
    let filter = ExecutionFilter::new()
        .workflow_name(AGENT_WORKFLOW_NAME)
        .workflow_id(id.as_str())
        .state("RUNNING");
    let page = rt.list_executions(&filter)?;
    Ok(page
        .executions
        .iter()
        .find_map(autumn_harvest_sqlite::ExecutionSummary::execution_id))
}

/// Drive the agent `id` until it blocks. Follow each checkpoint to the next
/// run. Returns `None` when the agent has no running run.
///
/// # Errors
///
/// Returns an error when the runtime cannot drive the run.
pub async fn drive(rt: &mut SqliteRuntime, id: &AgentId) -> SqliteResult<Option<RunState>> {
    let Some(mut exec) = current_run(rt, id)? else {
        return Ok(None);
    };
    loop {
        match rt.run_until_blocked(exec).await? {
            RunState::ContinuedAsNew(next) => exec = next,
            other => return Ok(Some(other)),
        }
    }
}

/// The committed state of the agent `id`. Returns `None` when the agent has
/// no running run.
///
/// # Errors
///
/// Returns an error when the query fails or the state does not decode.
pub fn state(
    rt: &SqliteRuntime,
    id: &AgentId,
) -> impl Future<Output = SqliteResult<Option<AgentState>>> + Send + 'static {
    // Start the query now. The future then holds no borrow of the runtime.
    let query = current_run(rt, id)
        .map(|exec| exec.map(|exec| rt.query(exec, ENTITY_STATE_QUERY, Value::Null)));
    async move {
        let Some(query) = query? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_value(query.await?)?))
    }
}

/// Decode `input`, run `body` to completion, and encode its result.
fn call<I, O, F, Fut>(input: Value, body: F) -> Result<Value, String>
where
    I: DeserializeOwned,
    O: Serialize,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = Result<O, String>>,
{
    let request: I =
        serde_json::from_value(input).map_err(|e| format!("malformed activity input: {e}"))?;
    let handle = multi_thread_handle()?;
    let output = tokio::task::block_in_place(|| handle.block_on(body(request)))?;
    serde_json::to_value(output).map_err(|e| format!("activity output is not JSON: {e}"))
}

/// The current Tokio runtime, when it can run a body with `block_in_place`.
///
/// `block_in_place` panics on a current-thread runtime. A retry cannot fix
/// that, so the call fails at once and names the fix.
fn multi_thread_handle() -> Result<tokio::runtime::Handle, String> {
    let refuse = |why: &str| {
        ActivityFailure::non_retryable(
            "MultiThreadRuntimeRequired",
            format!("{why}: drive the SQLite runtime from a multi-thread Tokio runtime"),
        )
        .into_error_payload()
    };
    let handle =
        tokio::runtime::Handle::try_current().map_err(|_| refuse("no Tokio runtime is running"))?;
    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
        return Err(refuse("the Tokio runtime is current-thread"));
    }
    Ok(handle)
}
