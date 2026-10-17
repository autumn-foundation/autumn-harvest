//! Register the example on the SQLite backend, and drive a session.
//!
//! One process is the single writer. [`register`] adds the agent workflows,
//! this example's workflows and every activity. [`start`] starts a session
//! under the workflow id `autoresearch/<tag>`, and refuses a tag in use, as
//! upstream refuses a branch that exists. [`drive`] carries the session
//! forward until it ends or needs a person.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::builder::{BuiltHarvest, HarvestBuilder};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::quota::QuotaPolicy;
use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::approval::approval_call_id;
use autumn_harvest_agent::eval::{Candidate, evaluate};
use autumn_harvest_agent::heartbeat;
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{AgentHarness, Approval, ModelTurn, agent_heartbeat_info, sqlite};
use autumn_harvest_sqlite::{
    ExecutionFilter, ExecutionId, ExecutionSummary, RunState, SqliteRuntime, StartOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::agent::RESEARCHER_ROLE;
use crate::inbox::{Command, parse_token, token};
use crate::scripted::FIX_MODE;
use crate::workflows::{
    self, HINT, POLICY, SESSION, STOP, SessionConfig, SessionInput, SessionReport, with_program,
};

/// The workflow id prefix of a session. The rest is the tag.
pub const SESSION_PREFIX: &str = "autoresearch/";

/// The workflow id of the session with `tag`: the branch name.
#[must_use]
pub fn session_id(tag: &str) -> String {
    format!("{SESSION_PREFIX}{tag}")
}

/// Register the agent loop, the two workflows and every activity.
///
/// Call it on every open: registrations live in process memory.
pub fn register(rt: &mut SqliteRuntime, harness: Arc<AgentHarness>) {
    sqlite::register(rt, harness);
    for workflow in workflows::all() {
        rt.register_workflow(&workflow);
    }
    for activity in crate::activities::all() {
        rt.register_activity(&activity);
    }
}

/// Start a session. The tag must be set.
///
/// The start records a memo (the lab and the GPU) and the first search
/// attributes (the tag and the phase).
///
/// # Errors
///
/// Returns an error when the tag is empty or a session with the tag exists.
pub fn start(rt: &mut SqliteRuntime, config: &SessionConfig) -> Result<ExecutionId, String> {
    let tag = config.tag.trim();
    if tag.is_empty() {
        return Err("a session needs a tag".into());
    }
    let input = SessionInput {
        config: config.clone(),
        carry: None,
    };
    let input = serde_json::to_value(input).map_err(|e| e.to_string())?;
    let options = StartOptions::new()
        .workflow_id(session_id(tag))
        // Upstream: the branch for a tag must not exist. A used tag is refused.
        .reuse_policy(WorkflowIdReusePolicy::RejectDuplicate)
        .memo(json!({"lab": config.lab, "gpu": config.gpu, "trainer": config.trainer}))
        .search_attribute("tag", json!(tag))
        .search_attribute("phase", json!("setup"));
    let started = rt
        .start_workflow_with_options(SESSION, input, options)
        .map_err(|e| format!("cannot start the session {tag}: {e}"))?;
    if !started.created {
        return Err(format!("the tag {tag} is in use: pick another tag"));
    }
    Ok(started.exec_id)
}

/// One gated tool call that waits for a person.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    /// The token that `approve` and `deny` take.
    pub token: String,
    /// The tool name.
    pub tool: String,
    /// The arguments of the call.
    pub arguments: Value,
}

/// Where a drive stopped.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// The session completed.
    Done(SessionReport),
    /// The session failed, with the reason.
    Failed(String),
    /// The session continued as new. Drive the successor.
    Continued(ExecutionId),
    /// A gated call waits for a person.
    Approval(Pending),
    /// The session waits for a timer, a mutex or a run in flight.
    Waiting,
}

/// Drive the session `exec` until it ends or blocks. Follows a blocked child
/// down to the run that waits.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub async fn drive(rt: &mut SqliteRuntime, exec: ExecutionId) -> Result<Step, String> {
    let mut run = exec;
    // A session waits on an experiment, which waits on an agent run.
    for _ in 0..8 {
        let state = rt.run_until_blocked(run).await.map_err(|e| e.to_string())?;
        match state {
            RunState::WaitingWorkflow(target) => run = target,
            RunState::WaitingSignal(signal) if run != exec => {
                return Ok(pending(rt, run, &signal).map_or(Step::Waiting, Step::Approval));
            }
            RunState::Completed(output) if run == exec => {
                let report = serde_json::from_value(output)
                    .map_err(|e| format!("the session output does not decode: {e}"))?;
                return Ok(Step::Done(report));
            }
            RunState::ContinuedAsNew(next) if run == exec => return Ok(Step::Continued(next)),
            RunState::Failed(error)
            | RunState::Cancelled(error)
            | RunState::TimedOut(error)
            | RunState::Terminated(error)
                if run == exec =>
            {
                return Ok(Step::Failed(error));
            }
            // A child ended or waits on a timer or a mutex: the session
            // goes on at the next drive.
            _ => return Ok(Step::Waiting),
        }
    }
    Ok(Step::Waiting)
}

/// The gated call that the approval wait `signal` of `run` holds.
fn pending(rt: &SqliteRuntime, run: ExecutionId, signal: &str) -> Option<Pending> {
    let call_id = approval_call_id(signal)?;
    let call = rt
        .load_history(run)
        .ok()?
        .into_iter()
        .rev()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted { output, .. } => {
                serde_json::from_value::<ModelTurn>(output).ok()
            }
            _ => None,
        })
        .find_map(|turn| turn.calls().into_iter().find(|call| call.id == call_id))?;
    Some(Pending {
        token: token(&run.to_string(), signal),
        tool: call.name,
        arguments: call.arguments,
    })
}

/// Deliver one inbox command, keyed by its inbox id.
///
/// Returns a line for the operator.
///
/// # Errors
///
/// Returns an error when the target is unknown or not running.
pub fn apply(rt: &mut SqliteRuntime, id: &str, command: &Command) -> Result<String, String> {
    let (session, name, payload) = match command {
        Command::Hint { session, text } => (session, HINT, json!({"text": text, "from": "human"})),
        Command::Stop { session, reason } => (session, STOP, json!({ "reason": reason })),
        Command::Policy { session, policy } => {
            policy.validate()?;
            let payload = serde_json::to_value(policy).map_err(|e| e.to_string())?;
            (session, POLICY, payload)
        }
        Command::Approve { token } => return decide(rt, id, token, &Approval::Approve),
        Command::Deny { token, reason } => {
            let reject = Approval::Reject {
                reason: reason.clone(),
            };
            return decide(rt, id, token, &reject);
        }
    };
    // The inbox id is the key: a resend after a crash is not staged twice.
    let delivery = rt
        .send_signal_by_workflow_id(SESSION, session, name, payload, Some(id))
        .map_err(|e| format!("cannot signal {session}: {e}"))?;
    Ok(if delivery.delivered {
        format!("{name} sent to {session}")
    } else {
        format!("{name} for {session} was already delivered")
    })
}

/// Send a decision to the approval wait that `token` names.
fn decide(
    rt: &mut SqliteRuntime,
    id: &str,
    token: &str,
    approval: &Approval,
) -> Result<String, String> {
    let (exec, signal) = parse_token(token)?;
    let exec = ExecutionId::from_str(&exec).map_err(|e| format!("{exec} is not a run id: {e}"))?;
    let payload = serde_json::to_value(approval).map_err(|e| e.to_string())?;
    let staged = rt
        .send_signal_idempotent(exec, &signal, payload, Some(id))
        .map_err(|e| format!("cannot decide {token}: {e}"))?;
    Ok(if staged {
        format!("decision sent for {token}")
    } else {
        format!("the decision for {token} was already sent")
    })
}

/// The running run of the session with `tag`, across `continue_as_new`.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub fn find_session(rt: &SqliteRuntime, tag: &str) -> Result<Option<ExecutionId>, String> {
    let filter = ExecutionFilter::new()
        .workflow_name(SESSION)
        .workflow_id(session_id(tag))
        .state("RUNNING")
        .limit(1);
    let page = rt.list_executions(&filter).map_err(|e| e.to_string())?;
    Ok(page
        .executions
        .first()
        .and_then(ExecutionSummary::execution_id))
}

/// The nightly schedule: a session at 23:00 UTC with the tag derived from
/// the start time. A night that is still running skips the next slot.
///
/// # Errors
///
/// Returns an error when the config does not encode.
pub fn nightly(config: &SessionConfig) -> Result<WorkflowSchedule, String> {
    let mut config = config.clone();
    config.tag.clear();
    let input = SessionInput {
        config,
        carry: None,
    };
    let input = serde_json::to_value(input).map_err(|e| e.to_string())?;
    // `max_active_runs` is one by default: a night still running skips a slot.
    Ok(WorkflowSchedule::new(SESSION, Schedule::Cron("0 23 * * *".into())).with_input(input))
}

/// The heartbeat schedule: a tick every 30 minutes that watches `lab`.
///
/// # Errors
///
/// Returns an error when the tick does not encode.
pub fn heartbeat(lab: &std::path::Path) -> Result<WorkflowSchedule, String> {
    let every = Schedule::Interval(Duration::from_secs(30 * 60));
    heartbeat::schedule(every, &crate::agent::heartbeat_task(lab)).map_err(|e| e.to_string())
}

/// The same workflows and activities on a Postgres `HarvestBuilder`, with an
/// LLM budget on `agent_loop` and both schedules. The SQLite backend refuses
/// a budget, so the budget exists on Postgres only.
///
/// # Errors
///
/// Returns an error when a schedule does not encode.
pub fn postgres(harness: AgentHarness, config: &SessionConfig) -> Result<BuiltHarvest, String> {
    let lab = std::path::PathBuf::from(&config.lab);
    // 400k tokens per run and 20 US dollars per tenant per day.
    let budget = QuotaPolicy::new("tenant")
        .with_max_run_llm_tokens(400_000)
        .with_max_tenant_llm_cost_usd_micros(20_000_000)
        .with_tenant_llm_window_secs(86_400);
    let mut flows = workflows::all();
    flows.push(agent_loop_info().with_quota(budget));
    flows.push(agent_heartbeat_info());
    let mut steps = crate::activities::all();
    steps.extend(autumn_harvest_agent::activities());
    Ok(HarvestBuilder::new()
        .workflows(flows)
        .activities(steps)
        .workflow_schedule(nightly(config)?)
        .workflow_schedule(heartbeat(&lab)?)
        .state(harness)
        .build())
}

/// The result of [`verify`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyReport {
    /// Histories that replayed.
    pub replayed: usize,
    /// One line per history that did not replay.
    pub failures: Vec<String>,
}

/// One stored run: what [`verify`] replays.
#[derive(Debug, Clone)]
pub struct StoredRun {
    /// The workflow name.
    pub name: String,
    /// The execution id.
    pub exec: ExecutionId,
    /// The workflow id.
    pub workflow_id: String,
    /// The parent run of a child.
    pub parent: Option<ExecutionId>,
    /// The state, such as `RUNNING` or `COMPLETED`.
    pub state: String,
    /// The whole event log.
    pub history: Vec<WorkflowEvent>,
}

/// Read every stored run, for [`verify`].
///
/// # Errors
///
/// Returns an error when the backend fails or a run row does not read.
pub fn stored_runs(rt: &SqliteRuntime) -> Result<Vec<StoredRun>, String> {
    let mut runs = Vec::new();
    let mut filter = ExecutionFilter::new().limit(1_000);
    loop {
        let page = rt.list_executions(&filter).map_err(|e| e.to_string())?;
        for run in &page.executions {
            let (Some(exec), Some(name), Some(workflow_id), Some(state)) = (
                run.execution_id(),
                run.workflow_name.clone().into_value(),
                run.workflow_id.clone().into_value(),
                run.state.clone().into_value(),
            ) else {
                return Err("a run row does not read".into());
            };
            runs.push(StoredRun {
                history: rt.load_history(exec).map_err(|e| e.to_string())?,
                parent: rt.parent_execution(exec).map_err(|e| e.to_string())?,
                name,
                exec,
                workflow_id,
                state,
            });
        }
        match page.next {
            Some(cursor) => filter = filter.before(cursor),
            None => return Ok(runs),
        }
    }
}

/// Replay each of `runs` on the core `WorkflowReplayer` with the workflows
/// of this build. Run it before a deploy: a failure names a run that the
/// new code would break.
pub async fn verify(runs: Vec<StoredRun>) -> VerifyReport {
    let mut flows = workflows::all();
    flows.push(agent_loop_info());
    flows.push(agent_heartbeat_info());
    let replayer = WorkflowReplayer::new().register(flows);
    let mut report = VerifyReport::default();
    for run in runs {
        let label = format!("{} {}", run.name, run.exec);
        match replay_one(&replayer, run).await {
            Ok(()) => report.replayed += 1,
            Err(why) => report.failures.push(format!("{label}: {why}")),
        }
    }
    report
}

/// Replay one history. A run that has not ended replays as the deploy
/// canary does: it may stop at its frontier.
async fn replay_one(replayer: &WorkflowReplayer, run: StoredRun) -> Result<(), String> {
    let StoredRun {
        name,
        exec,
        workflow_id,
        parent,
        state,
        mut history,
    } = run;
    let running = state == "RUNNING";
    if running {
        // An activity in flight has a start and no end. The matcher does not
        // consume such a start at the frontier.
        let ended: Vec<_> = history
            .iter()
            .filter_map(|event| match event {
                WorkflowEvent::ActivityCompleted { activity_id, .. }
                | WorkflowEvent::ActivityFailed { activity_id, .. }
                | WorkflowEvent::ActivityTimedOut { activity_id, .. } => Some(*activity_id),
                _ => None,
            })
            .collect();
        history.retain(|event| match event {
            WorkflowEvent::ActivityStarted { activity_id, .. } => ended.contains(activity_id),
            _ => true,
        });
    }
    let recorded_failure = history.iter().rev().find_map(|event| match event {
        WorkflowEvent::WorkflowFailed { error, .. } => Some(error.clone()),
        _ => None,
    });
    let snapshot = serde_json::from_value(json!({
        "workflow_name": name,
        "execution_id": exec,
        "events": history,
        "parent_execution_id": parent,
        "workflow_id": workflow_id,
    }))
    .map_err(|e| format!("cannot build a snapshot: {e}"))?;
    let result = if running {
        replayer.replay_canary_snapshot(snapshot).await
    } else {
        replayer.replay_from_snapshot(snapshot).await
    };
    match (&result.status, recorded_failure) {
        (ReplayStatus::ReplaySucceeded, _) => Ok(()),
        // A failed run must fail the same way again.
        (ReplayStatus::WorkflowFailed { error, .. }, Some(recorded))
            if autumn_harvest::failure::decode_workflow_failure(error).message == recorded =>
        {
            Ok(())
        }
        _ => Err(result.to_string()),
    }
}

/// The finished proposal runs of the researcher, newest first.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub fn proposals(rt: &SqliteRuntime) -> Result<Vec<ExecutionId>, String> {
    let filter = ExecutionFilter::new()
        .workflow_name(autumn_harvest_agent::WORKFLOW_NAME)
        .state("COMPLETED")
        .limit(1_000);
    let page = rt.list_executions(&filter).map_err(|e| e.to_string())?;
    let mut runs = Vec::new();
    for exec in page
        .executions
        .iter()
        .filter_map(ExecutionSummary::execution_id)
    {
        let started = rt
            .load_history(exec)
            .map_err(|e| e.to_string())?
            .into_iter()
            .find_map(|event| match event {
                WorkflowEvent::WorkflowStarted { input, .. } => Some(input),
                _ => None,
            });
        let is_proposal = started.is_some_and(|task| {
            task["system"]
                .as_str()
                .is_some_and(|s| s.contains(RESEARCHER_ROLE))
                && !task["input"].as_str().is_some_and(|i| i.contains(FIX_MODE))
        });
        if is_proposal {
            runs.push(exec);
        }
    }
    Ok(runs)
}

/// Re-drive a recorded proposal with `program` in place of the recorded
/// `program.md`, on `harness`. No tool runs, and no row lands. Read the
/// history with `SqliteRuntime::load_history`.
///
/// # Errors
///
/// Returns an error when the history is not an agent run, or it cannot be
/// evaluated.
pub async fn eval(
    history: &[WorkflowEvent],
    harness: AgentHarness,
    program: &str,
) -> Result<autumn_harvest_agent::eval::Evaluation, String> {
    let system = history
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::WorkflowStarted { input, .. } => {
                input["system"].as_str().map(str::to_string)
            }
            _ => None,
        })
        .ok_or("the history is not a recorded agent run with a system prompt")?;
    let candidate = Candidate::new(harness).system_prompt(with_program(&system, program));
    evaluate(history, &candidate)
        .await
        .map_err(|e| format!("cannot evaluate the run: {e}"))
}
