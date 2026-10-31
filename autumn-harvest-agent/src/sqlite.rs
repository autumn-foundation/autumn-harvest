//! The agent loop on the embedded SQLite backend.
//!
//! The harness lives in process state, and `ctx.state::<T>()` is always `None`
//! on this backend. [`register`] therefore wraps each harness method in a
//! closure for `register_activity_with_body`. The closure
//! runs the async body with `block_in_place` on the current Tokio runtime, so
//! drive the runtime from a multi-thread Tokio runtime.
//!
//! The model turn registers with `register_activity_with_ledger` instead
//! (issue #2275). Its closure returns the cost ledger call of the turn, which
//! the Postgres activity records through its context. The runtime writes the
//! row with the completion event, so a re-run after a crash counts once. Read
//! the rows with `SqliteRuntime::llm_ledger`.

use std::sync::Arc;

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_QUERY};
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::llm_ledger::LlmCall;
use autumn_harvest_sqlite::{
    ExecutionFilter, ExecutionId, RunState, SignalWithStart, SignalWithStartOutcome, SqliteReader,
    SqliteResult, SqliteRuntime,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::agent::{
    AGENT_WORKFLOW_NAME, Agent, AgentId, AgentOp, AgentState, Control, INTERRUPT_SIGNAL, Interrupt,
    agent_info, agent_memory_reset_info, agent_profile_info, agent_workspace_floor_info,
    agent_workspace_reset_info, agent_workspace_retain_info,
};
use crate::approval::Approval;
use crate::error::{AgentError, ErrorKind};
use crate::harness::{AgentHarness, CacheTenant};
use crate::heartbeat::{HEARTBEAT_WORKFLOW_NAME, HeartbeatTask, agent_heartbeat_info};
use crate::mesh_control::{MESH_ATTR, MeshPause, MeshSpend, Spend};
use crate::model::BoxFuture;
use crate::types::{AgentTask, ModelTurnRequest};
use crate::workflow::{
    WORKFLOW_NAME, agent_deliver_info, agent_discover_peers_info, agent_loop_info,
    agent_memory_snapshot_info, agent_model_turn_info, agent_precheck_info, agent_tool_call_info,
};
use autumn_harvest::chrono::{DateTime, Utc};

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
    rt.register_activity_with_body(&agent_workspace_retain_info(), move |input| {
        call(input, |mount| h.workspace_retain(mount))
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_workspace_floor_info(), move |input| {
        call(input, |scope: String| {
            let h = Arc::clone(&h);
            async move { h.workspace_floor(&scope).await }
        })
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_workspace_reset_info(), move |input| {
        call(input, |mount: crate::workspace::WorkspaceMount| {
            let h = Arc::clone(&h);
            async move { h.workspace_reset(mount).await }
        })
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_ledger(&agent_model_turn_info(), move |input, at| {
        call_with_ledger(input, |request: ModelTurnRequest| async {
            // The tenant is the one `AgentHarness::model_turn` uses.
            let tenant = CacheTenant::declared(request.tenant.as_deref());
            let model = request.model.clone();
            // The budget window reads the drive clock, as the ledger rows do.
            let (turn, latency) = h
                .model_turn_timed_in(request, tenant, |_| {}, None, at)
                .await?;
            // A cache hit calls no model, so it has no latency and no call.
            let calls = latency
                .map(|latency| h.ledger_call(model.as_deref(), &turn.usage, latency))
                .into_iter()
                .collect();
            Ok((turn, calls))
        })
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

/// The spend of a mesh from the cost ledger of a SQLite file (issue #2272).
///
/// It sums the ledger rows of every run whose search attribute
/// [`MESH_ATTR`] names the mesh. Each read opens its own read-only handle,
/// so it sees what the runtime committed.
#[derive(Debug, Clone)]
pub struct LedgerSpend {
    path: std::path::PathBuf,
}

impl LedgerSpend {
    /// Read the ledger of the database file at `path`.
    #[must_use]
    pub fn open(path: impl AsRef<std::path::Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
        }
    }

    fn read(&self, mesh: &str, since: DateTime<Utc>) -> SqliteResult<Spend> {
        let reader = SqliteReader::open(&self.path)?;
        let filter = ExecutionFilter::new().search_attribute(MESH_ATTR, serde_json::json!(mesh));
        let mut runs = std::collections::HashSet::new();
        let mut cursor = None;
        loop {
            let page =
                cursor.map_or_else(|| filter.clone(), |cursor| filter.clone().before(cursor));
            let found = reader.list_executions(&page)?;
            runs.extend(
                found
                    .executions
                    .iter()
                    .filter_map(autumn_harvest_sqlite::ExecutionSummary::execution_id),
            );
            match found.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        // A row of a simulated drive can be later than the wall clock, so the
        // read has no upper bound (issue #2272).
        let rows = reader.llm_ledger_between(since, DateTime::<Utc>::MAX_UTC)?;
        let spend = rows.iter().filter(|row| runs.contains(&row.exec_id)).fold(
            Spend::default(),
            |spend, row| Spend {
                tokens: spend
                    .tokens
                    .saturating_add(row.input_tokens)
                    .saturating_add(row.output_tokens),
                cost_usd_micros: spend
                    .cost_usd_micros
                    .saturating_add(row.cost_usd_micros.unwrap_or(0)),
            },
        );
        Ok(spend)
    }
}

impl MeshSpend for LedgerSpend {
    fn spent<'a>(
        &'a self,
        mesh: &'a str,
        since: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Spend, AgentError>> {
        let read = self
            .read(mesh, since)
            .map_err(|err| AgentError::new(ErrorKind::Transport, err.to_string()));
        Box::pin(std::future::ready(read))
    }
}

/// Pause every running agent of `mesh`, and return the audit record (issue
/// #2272).
///
/// Each agent gets an interrupt, so an op in flight stops, and then a
/// `Control::Pause` with `reason`. A paused agent holds its work and skips its
/// ticks until a `Control::Resume`. The record also goes to the tracing
/// target `harvest.audit`.
///
/// # Errors
///
/// Returns a list or a send error. Agents that got the pause before it stay
/// paused.
pub fn pause_mesh(rt: &mut SqliteRuntime, mesh: &str, reason: &str) -> SqliteResult<MeshPause> {
    let at_ms = Utc::now().timestamp_millis();
    let filter = ExecutionFilter::new()
        .workflow_name(AGENT_WORKFLOW_NAME)
        .state("RUNNING")
        .search_attribute(MESH_ATTR, serde_json::json!(mesh));
    let mut targets = Vec::new();
    let mut cursor = None;
    loop {
        let page = cursor.map_or_else(|| filter.clone(), |cursor| filter.clone().before(cursor));
        let found = rt.list_executions(&page)?;
        for summary in &found.executions {
            let id = summary
                .workflow_id
                .value()
                .and_then(|id| AgentId::parse(id).ok());
            if let (Some(exec), Some(id)) = (summary.execution_id(), id) {
                targets.push((exec, id));
            }
        }
        match found.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let key = format!("pause-mesh:{mesh}:{at_ms}");
    let pause = AgentOp::Control(Control::Pause {
        reason: Some(reason.to_owned()),
    });
    let mut agents = Vec::with_capacity(targets.len());
    for (exec, id) in targets {
        let interrupt = serde_json::to_value(Interrupt { message_id: None })?;
        let signal_key = format!("{INTERRUPT_SIGNAL}:{key}");
        rt.send_signal_idempotent(exec, INTERRUPT_SIGNAL, interrupt, Some(&signal_key))?;
        send(rt, &id, &pause, Some(&key))?;
        agents.push(id);
    }
    agents.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let record = MeshPause {
        mesh: mesh.to_owned(),
        reason: reason.to_owned(),
        at_ms,
        agents,
    };
    tracing::info!(
        target: "harvest.audit",
        mesh = %record.mesh,
        reason = %record.reason,
        agents = record.agents.len(),
        "a mesh was paused"
    );
    Ok(record)
}

/// Decode `input`, run `body` to completion, and encode its result.
fn call<I, O, F, Fut>(input: Value, body: F) -> Result<Value, String>
where
    I: DeserializeOwned,
    O: Serialize,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = Result<O, String>>,
{
    call_with_ledger(input, |request| async move {
        Ok((body(request).await?, Vec::new()))
    })
    .map(|(output, _calls)| output)
}

/// [`call`], for a body that also returns its LLM calls (issue #2275).
fn call_with_ledger<I, O, F, Fut>(input: Value, body: F) -> Result<(Value, Vec<LlmCall>), String>
where
    I: DeserializeOwned,
    O: Serialize,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = Result<(O, Vec<LlmCall>), String>>,
{
    let request: I =
        serde_json::from_value(input).map_err(|e| format!("malformed activity input: {e}"))?;
    let handle = multi_thread_handle()?;
    let (output, calls) = tokio::task::block_in_place(|| handle.block_on(body(request)))?;
    let output =
        serde_json::to_value(output).map_err(|e| format!("activity output is not JSON: {e}"))?;
    Ok((output, calls))
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
