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
    agent_info, agent_memory_reset_info, agent_profile_info, agent_profile_overlay_info,
    agent_workspace_floor_info, agent_workspace_reset_info, agent_workspace_retain_info,
};
use crate::answer_check::{CheckRequest, admission, agent_answer_check_info};
use crate::approval::Approval;
use crate::consolidate::{
    CONSOLIDATE_WORKFLOW_NAME, CONSOLIDATION_ATTR, ConsolidateTask, agent_consolidate_info,
    agent_consolidate_read_info, agent_consolidate_turn_info, agent_consolidate_write_info,
    agent_episode_append_info, agent_episode_reset_info,
};
use crate::consolidate_all::{
    AgentDirectory, CONSOLIDATE_ALL_WORKFLOW_NAME, ConsolidateAll, LastPass,
    agent_consolidate_all_info, agent_consolidate_due_info,
};
use crate::error::{AgentError, ErrorKind};
use crate::harness::{AgentHarness, CacheTenant};
use crate::heartbeat::{HEARTBEAT_WORKFLOW_NAME, HeartbeatTask, agent_heartbeat_info};
use crate::mesh_control::{MESH_ATTR, MeshPause, MeshSpend, Spend};
use crate::model::BoxFuture;
use crate::refine::agent_refine_propose_info;
use crate::refine::propose::run_stage_metered;
use crate::sampling::{
    DRAWS_WORKFLOW_NAME, DrawsInput, SAMPLE_WORKFLOW_NAME, SampleInput, VoteKeyRequest,
    agent_draws_info, agent_sample_info, agent_sampling_policy_info, agent_vote_key_info,
    run_vote_key,
};
use crate::score::{SampleScore, agent_sample_score_info, agent_score_info, run_score};
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
    register_sampling(rt, &harness);
    rt.register_workflow(&agent_consolidate_info());
    rt.register_workflow(&agent_consolidate_all_info());
    register_episodes(rt, &harness);
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_profile_info(), move |input| {
        call(input, |kind: String| {
            std::future::ready(Ok::<_, String>(h.profile_read(&kind)))
        })
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_profile_overlay_info(), move |input| {
        call(input, |request: crate::agent::profile_read::OverlayRead| {
            let h = Arc::clone(&h);
            async move { h.overlaid_profile(&request).await }
        })
    });
    let h = Arc::clone(&harness);
    rt.register_activity_with_body(&agent_memory_reset_info(), move |input| {
        call(input, |scope| h.memory_reset(scope))
    });
    register_workspace(rt, &harness);
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
    register_refine(rt, &harness);
    rt.register_activity_with_body(&agent_precheck_info(), move |input| {
        call(input, |task| harness.precheck_tick(task))
    });
}

/// Register the strategy runs, their policy read and the key of a vote.
fn register_sampling(rt: &mut SqliteRuntime, harness: &Arc<AgentHarness>) {
    rt.register_workflow(&agent_draws_info());
    rt.register_workflow(&agent_sample_info());
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_sampling_policy_info(), move |input| {
        call(input, |kind: String| {
            let h = Arc::clone(&h);
            async move { h.sampling_policy(&kind).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_vote_key_info(), move |input| {
        call(input, |request: VoteKeyRequest| {
            std::future::ready(run_vote_key(&h, &request))
        })
    });
    // SQLite keeps no run tenant, so the task tenant applies (issue #2442).
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_answer_check_info(), move |input| {
        call(input, |request: CheckRequest| {
            let h = Arc::clone(&h);
            async move { admission(&h, &request, request.tenant.as_deref()).await }
        })
    });
}

/// Register the refine and score activities against `harness` (issues
/// #2379, #2394, #2441 and #2443).
fn register_refine(rt: &mut SqliteRuntime, harness: &Arc<AgentHarness>) {
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&crate::refine::agent_refine_policy_info(), move |input| {
        call(input, |kind: String| {
            let h = Arc::clone(&h);
            async move { h.refine_policy(&kind).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_ledger(&agent_refine_propose_info(), move |input, _at| {
        call_with_ledger(input, |request| {
            let h = Arc::clone(&h);
            async move { run_stage_metered(&h, request).await }
        })
    });
    #[cfg(feature = "eval")]
    register_refine_run(rt, harness);
    let h = Arc::clone(harness);
    rt.register_activity_with_ledger(&agent_score_info(), move |input, _at| {
        call_with_ledger(input, |request| {
            let h = Arc::clone(&h);
            async move {
                // SQLite has no run budget, so a judge call passes the check.
                let pass = Box::new(|| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>);
                let (verdict, calls) = run_score(&h, request, pass).await;
                verdict.map(|verdict| (verdict, calls))
            }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_ledger(&agent_sample_score_info(), move |input, _at| {
        call_with_ledger(input, |request| {
            let h = Arc::clone(&h);
            async move {
                // SQLite has no run budget, so a judge call passes the check.
                let pass = Box::new(|| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>);
                let (verdict, calls) = run_score(&h, request, pass).await;
                verdict.map(|verdict| (SampleScore::new(verdict, &calls), calls))
            }
        })
    });
}

/// Register the refine run and its steps (issue #2380). Each step that
/// spends returns its cost ledger calls with its result.
#[cfg(feature = "eval")]
fn register_refine_run(rt: &mut SqliteRuntime, harness: &Arc<AgentHarness>) {
    use crate::refine::compose::{run_compose, run_select};
    use crate::refine::gate::{run_adopt, run_decide};
    use crate::refine::run::{probe_calls, run_probe, run_start};
    use crate::refine::{
        agent_refine_adopt_info, agent_refine_compose_info, agent_refine_decide_info,
        agent_refine_info, agent_refine_probe_info, agent_refine_select_info,
        agent_refine_start_info,
    };

    rt.register_workflow(&agent_refine_info());
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_refine_adopt_info(), move |input| {
        call(input, |input| {
            let h = Arc::clone(&h);
            async move { run_adopt(&h, input).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_refine_decide_info(), move |input| {
        call(input, |input| {
            let h = Arc::clone(&h);
            async move { run_decide(&h, input).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_refine_start_info(), move |input| {
        call(input, |input| {
            let h = Arc::clone(&h);
            async move { run_start(&h, input).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_ledger(&agent_refine_probe_info(), move |input, _at| {
        call_with_ledger(input, |request| {
            let h = Arc::clone(&h);
            async move {
                let result = run_probe(&h, request).await?;
                let calls = probe_calls(&result);
                Ok((result, calls))
            }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_ledger(&agent_refine_compose_info(), move |input, _at| {
        call_with_ledger(input, |request| {
            let h = Arc::clone(&h);
            async move { run_compose(&h, request).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_ledger(&agent_refine_select_info(), move |input, _at| {
        call_with_ledger(input, |request| {
            let h = Arc::clone(&h);
            async move { run_select(&h, request).await }
        })
    });
}

/// Start one refine run (issue #2380). Returns its execution id. Drive it
/// with `SqliteRuntime::run_until_blocked`. Install a [`Sources`] on the
/// harness first.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the start.
#[cfg(feature = "eval")]
pub fn start_refine(
    rt: &mut SqliteRuntime,
    input: &crate::refine::RefineInput,
) -> SqliteResult<ExecutionId> {
    rt.start_workflow(
        crate::refine::REFINE_WORKFLOW_NAME,
        serde_json::to_value(input)?,
    )
}

/// Send a decision of a person to the adoption gate of the refine run
/// `exec`.
///
/// The gate drops a decision that arrives before its wait opens. Wait for
/// the search attribute `refine_status = waiting_approval` first.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the signal.
pub fn decide_refine(
    rt: &mut SqliteRuntime,
    exec: ExecutionId,
    decision: &crate::refine::RefineDecision,
) -> SqliteResult<()> {
    rt.send_signal(
        exec,
        &crate::refine::refine_approval_signal(exec),
        serde_json::to_value(decision)?,
    )
}

/// The source runs of a refine run, from a SQLite file (issue #2380).
///
/// Each read opens its own read-only handle, so it sees what the runtime
/// committed. SQLite has no fork, so a live re-probe returns
/// [`LiveProbeError::Unsupported`](crate::refine::LiveProbeError::Unsupported).
/// Install it with `AgentHarness::refine_sources`.
#[derive(Debug, Clone)]
pub struct Sources {
    path: std::path::PathBuf,
}

impl Sources {
    /// Read the runs of the database file at `path`.
    #[must_use]
    pub fn open(path: impl AsRef<std::path::Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
        }
    }

    fn read(&self, run: ExecutionId) -> SqliteResult<Option<crate::refine::SourceRecord>> {
        let reader = SqliteReader::open(&self.path)?;
        let description = match reader.describe(run) {
            Ok(description) => description,
            Err(autumn_harvest_sqlite::SqliteError::ExecutionNotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };
        let summary = description.summary;
        let text = |value: Option<&String>| value.cloned().unwrap_or_default();
        let state = text(summary.state.value());
        Ok(Some(crate::refine::SourceRecord {
            workflow_name: text(summary.workflow_name.value()),
            workflow_id: text(summary.workflow_id.value()),
            ended: !state.is_empty() && state != "RUNNING",
            events: reader.load_history(run)?,
        }))
    }
}

impl crate::refine::RefineSources for Sources {
    fn source(
        &self,
        run: ExecutionId,
    ) -> BoxFuture<'_, Result<Option<crate::refine::SourceRecord>, AgentError>> {
        let read = self.read(run).map_err(|err| transport(&err));
        Box::pin(std::future::ready(read))
    }

    fn live_probe<'a>(
        &'a self,
        _request: crate::refine::LiveRequest<'a>,
    ) -> BoxFuture<'a, Result<crate::refine::LiveEnd, crate::refine::LiveProbeError>> {
        Box::pin(std::future::ready(Err(
            crate::refine::LiveProbeError::Unsupported("SQLite"),
        )))
    }
}

/// Register the activities that retain, floor and reset a workspace.
///
/// `register` calls it so that function stays under the line limit.
fn register_workspace(rt: &mut SqliteRuntime, harness: &Arc<AgentHarness>) {
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_workspace_retain_info(), move |input| {
        call(input, |mount| h.workspace_retain(mount))
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_workspace_floor_info(), move |input| {
        call(input, |scope: String| {
            let h = Arc::clone(&h);
            async move { h.workspace_floor(&scope).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_workspace_reset_info(), move |input| {
        call(input, |mount: crate::workspace::WorkspaceMount| {
            let h = Arc::clone(&h);
            async move { h.workspace_reset(mount).await }
        })
    });
}

/// Register the activities of the episode log and of its pass (issue
/// #2454). The model turn records its ledger call with the completion.
fn register_episodes(rt: &mut SqliteRuntime, harness: &Arc<AgentHarness>) {
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_episode_append_info(), move |input| {
        call(input, |append| {
            let h = Arc::clone(&h);
            async move { crate::consolidate::append_episode(&h, append).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_episode_reset_info(), move |input| {
        call(input, |erase| {
            let h = Arc::clone(&h);
            async move { crate::consolidate::erase_episodes(&h, erase).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&crate::refine::agent_refine_lesson_info(), move |input| {
        call(input, |request| {
            let h = Arc::clone(&h);
            async move { crate::refine::lessons::run_write(&h, request).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&crate::refine::agent_lesson_match_info(), move |input| {
        call(input, |agent| {
            let h = Arc::clone(&h);
            async move { crate::refine::lessons::run_match(&h, agent).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&crate::refine::agent_lesson_erase_info(), move |input| {
        call(input, |request| {
            let h = Arc::clone(&h);
            async move { crate::refine::lessons::run_erase(&h, request).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_consolidate_read_info(), move |input| {
        call(input, |agent: AgentId| {
            let h = Arc::clone(&h);
            async move { crate::consolidate::read_pass(&h, &agent).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_consolidate_write_info(), move |input| {
        call(input, |write| {
            let h = Arc::clone(&h);
            async move { crate::consolidate::write_facts(&h, write).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_body(&agent_consolidate_due_info(), move |input| {
        call(input, |task: ConsolidateAll| {
            let h = Arc::clone(&h);
            async move { crate::consolidate_all::read_due(&h, &task).await }
        })
    });
    let h = Arc::clone(harness);
    rt.register_activity_with_ledger(&agent_consolidate_turn_info(), move |input, at| {
        call_with_ledger(input, |turn| {
            let h = Arc::clone(&h);
            async move {
                // SQLite has no run budget, so the turn passes the check.
                let pass = Box::new(|| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>);
                let (facts, calls) = crate::consolidate::run_turn(&h, turn, pass, at).await;
                facts.map(|facts| (facts, calls))
            }
        })
    });
}

/// Start one consolidation pass of the agent `id` (issue #2454). Returns
/// its execution id. Drive it with `SqliteRuntime::run_until_blocked`.
///
/// For every agent of a scheduled kind, pass `consolidate::schedule` to
/// `SqliteRuntime::upsert_schedule`, or call [`consolidate_all`].
///
/// # Errors
///
/// Returns an error when the runtime cannot record the start.
pub fn consolidate(rt: &mut SqliteRuntime, id: &AgentId) -> SqliteResult<ExecutionId> {
    let task = ConsolidateTask::new(id.clone());
    rt.start_workflow(CONSOLIDATE_WORKFLOW_NAME, serde_json::to_value(task)?)
}

/// Start one firing of the dispatcher `agent_consolidate_all` (issue
/// #2540). Returns its execution id. `SqliteRuntime::run_until_blocked`
/// drives it and each pass that it starts.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the start.
pub fn consolidate_all(rt: &mut SqliteRuntime, task: &ConsolidateAll) -> SqliteResult<ExecutionId> {
    rt.start_workflow(CONSOLIDATE_ALL_WORKFLOW_NAME, serde_json::to_value(task)?)
}

/// Start one agent run. Returns its execution id.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the start.
pub fn start(rt: &mut SqliteRuntime, task: &AgentTask) -> SqliteResult<ExecutionId> {
    rt.start_workflow(WORKFLOW_NAME, serde_json::to_value(task)?)
}

/// Start one strategy run (issue #2439). Returns its execution id.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the start.
pub fn start_draws(rt: &mut SqliteRuntime, input: &DrawsInput) -> SqliteResult<ExecutionId> {
    rt.start_workflow(DRAWS_WORKFLOW_NAME, serde_json::to_value(input)?)
}

/// Start one `agent_sample` run (issue #2443). Returns its execution id.
///
/// # Errors
///
/// Returns an error when the runtime cannot record the start.
pub fn start_sample(rt: &mut SqliteRuntime, input: &SampleInput) -> SqliteResult<ExecutionId> {
    rt.start_workflow(SAMPLE_WORKFLOW_NAME, serde_json::to_value(input)?)
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

/// The agent directory of a SQLite file (issue #2540).
///
/// It lists the running agents of a kind and the running passes of an
/// agent by their search attributes. Each read opens its own read-only
/// handle, so it sees what the runtime committed. Install it with
/// `AgentHarness::directory`.
#[derive(Debug, Clone)]
pub struct Directory {
    path: std::path::PathBuf,
}

impl Directory {
    /// Read the runs of the database file at `path`.
    #[must_use]
    pub fn open(path: impl AsRef<std::path::Path>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
        }
    }

    /// The workflow ids of the running runs that `filter` matches.
    fn running(&self, filter: &ExecutionFilter) -> SqliteResult<Vec<String>> {
        let reader = SqliteReader::open(&self.path)?;
        let filter = filter.clone().state("RUNNING");
        let mut ids = Vec::new();
        let mut cursor = None;
        loop {
            let page =
                cursor.map_or_else(|| filter.clone(), |cursor| filter.clone().before(cursor));
            let found = reader.list_executions(&page)?;
            ids.extend(
                found
                    .executions
                    .iter()
                    .filter_map(|summary| summary.workflow_id.value().map(ToOwned::to_owned)),
            );
            match found.next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(ids)
    }

    /// The last pass of `agent`. A running pass comes first. Each list
    /// reads one row, newest first.
    fn read_last_pass(&self, agent: &AgentId) -> SqliteResult<Option<LastPass>> {
        let reader = SqliteReader::open(&self.path)?;
        let mut filter = ExecutionFilter::new()
            .workflow_name(CONSOLIDATE_WORKFLOW_NAME)
            .search_attribute(CONSOLIDATION_ATTR, serde_json::json!(agent.as_str()));
        filter.limit = Some(1);
        let newest = |filter: &ExecutionFilter| -> SqliteResult<Option<i64>> {
            let page = reader.list_executions(filter)?;
            Ok(page
                .executions
                .first()
                .and_then(|summary| summary.started_at.value())
                .map(DateTime::timestamp_millis))
        };
        if let Some(started_ms) = newest(&filter.clone().state("RUNNING"))? {
            return Ok(Some(LastPass {
                running: true,
                started_ms,
            }));
        }
        Ok(newest(&filter)?.map(|started_ms| LastPass {
            running: false,
            started_ms,
        }))
    }
}

/// A SQLite read error as a retryable agent error.
fn transport(err: &autumn_harvest_sqlite::SqliteError) -> AgentError {
    AgentError::new(ErrorKind::Transport, err.to_string())
}

impl AgentDirectory for Directory {
    fn agents<'a>(&'a self, kind: &'a str) -> BoxFuture<'a, Result<Vec<AgentId>, AgentError>> {
        Box::pin(async move {
            let filter = ExecutionFilter::new()
                .workflow_name(AGENT_WORKFLOW_NAME)
                .search_attribute("agent_kind", serde_json::json!(kind));
            let ids = self.running(&filter).map_err(|err| transport(&err))?;
            Ok(ids
                .iter()
                .filter_map(|id| AgentId::parse(id).ok())
                .collect())
        })
    }

    fn last_pass<'a>(
        &'a self,
        agent: &'a AgentId,
    ) -> BoxFuture<'a, Result<Option<LastPass>, AgentError>> {
        Box::pin(async move { self.read_last_pass(agent).map_err(|err| transport(&err)) })
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

/// SQLite has no fork, so it cannot run a live probe (issue #2378).
#[cfg(feature = "eval")]
impl crate::eval::LiveProbeHost for SqliteRuntime {
    fn source_run(
        &self,
        _source: ExecutionId,
    ) -> BoxFuture<'_, Result<crate::eval::SourceRun, crate::eval::EvalError>> {
        Box::pin(std::future::ready(Err(
            crate::eval::EvalError::Unsupported("SQLite"),
        )))
    }

    fn fork_live(
        &self,
        _source: ExecutionId,
        _input: Value,
    ) -> BoxFuture<'_, Result<crate::eval::ProbeFork, crate::eval::EvalError>> {
        Box::pin(std::future::ready(Err(
            crate::eval::EvalError::Unsupported("SQLite"),
        )))
    }
}
