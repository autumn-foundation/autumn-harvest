//! Register the example on the SQLite backend, and drive a session.
//!
//! One process is the single writer. [`register`] adds the agent workflows,
//! this example's workflows and every activity. [`start`] starts a session
//! under the workflow id `autoresearch/<tag>`, and refuses a tag in use, as
//! upstream refuses a branch that exists. [`drive`] carries the session
//! forward until it ends or needs a person.
//!
//! A session started by the nightly schedule has an id that the engine
//! picks. So the commands find a running session by its `tag` search
//! attribute, not by its id.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::WorkflowIdReusePolicy;
use autumn_harvest::builder::{BuiltHarvest, HarvestBuilder};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::policy::{Schedule, WorkflowSchedule};
use autumn_harvest::quota::QuotaPolicy;
use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};
use autumn_harvest_agent::approval::approval_call_id;
use autumn_harvest_agent::eval::{Candidate, evaluate};
use autumn_harvest_agent::heartbeat;
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{AgentHarness, Approval, ModelTurn, agent_heartbeat_info, sqlite};
use autumn_harvest_sqlite::{
    ExecutionFilter, ExecutionId, ExecutionOutcome, ExecutionPage, ExecutionSummary, RunState,
    SqliteError, SqliteResult, SqliteRuntime, StartOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::agent::RESEARCHER_ROLE;
use crate::inbox::{Command, parse_token, token};
use crate::scripted::FIX_MODE;
use crate::workflows::{
    self, HINT, POLICY, SESSION, STOP, SessionConfig, SessionInput, SessionReport, with_program,
};

/// The workflow id prefix of a session: the branch prefix. The rest is the
/// tag.
pub const SESSION_PREFIX: &str = crate::lab::BRANCH_PREFIX;

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
    crate::lab::check_tag(tag)?;
    crate::activities::check_trainer(&config.trainer)?;
    config.policy.validate()?;
    let mut config = config.clone();
    config.tag = tag.to_string();
    config.lab = canonical_lab(&config.lab);
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
    /// The session waits for this mutex. Another run holds it.
    Mutex(String),
    /// The session waits for a timer or a run in flight.
    Waiting,
}

/// Drive the detached report agent `run` to its end. Only that run, and the
/// runs it waits on, move. Another session in the database waits for its
/// own runner.
///
/// # Errors
///
/// Returns an error when `run` is not a run id, or the backend fails. Also
/// returns one when the run waits on a person, or ends with no report.
pub async fn drive_report(rt: &mut SqliteRuntime, run: &str) -> Result<(), String> {
    let mut exec = ExecutionId::from_str(run).map_err(|e| format!("{run} is not a run id: {e}"))?;
    loop {
        let state = rt
            .run_until_blocked(exec)
            .await
            .map_err(|e| format!("cannot drive the report {run}: {e}"))?;
        match state {
            RunState::Completed(_) => return Ok(()),
            RunState::ContinuedAsNew(next) => exec = next,
            // A retry waits on a timer, a peer holds a mutex, or a child waits.
            RunState::WaitingTimer
            | RunState::WaitingMutex(_)
            | RunState::WaitingWorkflow(_)
            | RunState::InProgress => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            RunState::WaitingSignal(signal) => {
                return Err(format!("the report {run} waits on {signal}"));
            }
            RunState::Failed(why)
            | RunState::Cancelled(why)
            | RunState::TimedOut(why)
            | RunState::Terminated(why) => {
                return Err(format!("the report {run} wrote no report: {why}"));
            }
        }
    }
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
        let state = rt
            .run_until_blocked(run)
            .await
            .map_err(|e| format!("cannot drive {run}: {e}"))?;
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
            RunState::WaitingMutex(key) => return Ok(Step::Mutex(key)),
            RunState::Failed(error)
            | RunState::Cancelled(error)
            | RunState::TimedOut(error)
            | RunState::Terminated(error)
                if run == exec =>
            {
                return Ok(Step::Failed(error));
            }
            // A child ended or waits on a timer: the session goes on at the
            // next drive.
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

/// Why an inbox command did not land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// It can never land, such as a stop for a session that ended.
    Permanent(String),
    /// It can land later, such as when the database is busy.
    Transient(String),
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Permanent(why) | Self::Transient(why) => f.write_str(why),
        }
    }
}

impl Refused {
    /// A backend error, sorted by whether a retry can help.
    fn backend(context: &str, error: &SqliteError) -> Self {
        let why = format!("{context}: {error}");
        match error {
            SqliteError::Sqlite(_)
            | SqliteError::DatabaseLocked { .. }
            | SqliteError::Io { .. }
            | SqliteError::TaskFailed { .. } => Self::Transient(why),
            _ => Self::Permanent(why),
        }
    }
}

/// Deliver one inbox command, keyed by its inbox id.
///
/// Returns a line for the operator.
///
/// # Errors
///
/// Returns [`Refused`] when the command cannot land now or ever.
pub fn apply(rt: &mut SqliteRuntime, id: &str, command: &Command) -> Result<String, Refused> {
    let (tag, name, payload) = match command {
        Command::Hint { tag, text } => (tag, HINT, json!({"text": text, "from": "human"})),
        Command::Stop { tag, reason } => (tag, STOP, json!({ "reason": reason })),
        Command::Policy { tag, policy } => {
            policy.validate().map_err(Refused::Permanent)?;
            let payload =
                serde_json::to_value(policy).map_err(|e| Refused::Permanent(e.to_string()))?;
            (tag, POLICY, payload)
        }
        Command::Approve { token } => return decide(rt, token, &Approval::Approve),
        Command::Deny { token, reason } => {
            let reject = Approval::Reject {
                reason: reason.clone(),
            };
            return decide(rt, token, &reject);
        }
    };
    let Some((_, session)) = find_session(rt, tag).map_err(Refused::Transient)? else {
        // A scheduled session publishes its tag in its first cycle. A process
        // that died before that cycle leaves a running session with no tag.
        // It can be the one, so the command waits.
        let untagged = running_sessions(rt)
            .map_err(Refused::Transient)?
            .iter()
            .any(|(_, tag)| tag.is_empty());
        let why = format!("no running session has the tag {tag}");
        return Err(if untagged {
            Refused::Transient(format!("{why} yet: a session has no tag so far"))
        } else {
            Refused::Permanent(why)
        });
    };
    // The inbox id is the key: a resend after a crash is not staged twice.
    let delivery = rt
        .send_signal_by_workflow_id(SESSION, &session, name, payload, Some(id))
        .map_err(|e| Refused::backend(&format!("cannot signal {session}"), &e))?;
    Ok(if delivery.delivered {
        format!("{name} sent to {session}")
    } else {
        format!("{name} for {session} was already delivered")
    })
}

/// Send a decision to the approval wait that `token` names. The token is
/// the key, so a second decision for one wait is not staged.
fn decide(rt: &mut SqliteRuntime, token: &str, approval: &Approval) -> Result<String, Refused> {
    let (exec, signal) = parse_token(token).map_err(Refused::Permanent)?;
    if approval_call_id(&signal).is_none() {
        return Err(Refused::Permanent(format!(
            "{token} does not name an approval wait"
        )));
    }
    let exec = ExecutionId::from_str(&exec)
        .map_err(|e| Refused::Permanent(format!("{exec} is not a run id: {e}")))?;
    let payload = serde_json::to_value(approval).map_err(|e| Refused::Permanent(e.to_string()))?;
    let staged = rt
        .send_signal_idempotent(exec, &signal, payload, Some(token))
        .map_err(|e| Refused::backend(&format!("cannot decide {token}"), &e))?;
    Ok(if staged {
        format!("decision sent for {token}")
    } else {
        format!("a decision for {token} was already sent")
    })
}

/// The running session with `tag`, across `continue_as_new`: its run and
/// its workflow id.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub fn find_session(
    rt: &SqliteRuntime,
    tag: &str,
) -> Result<Option<(ExecutionId, String)>, String> {
    let filter = ExecutionFilter::new()
        .workflow_name(SESSION)
        .search_attribute("tag", json!(tag))
        .state("RUNNING")
        .limit(1);
    let page = rt.list_executions(&filter).map_err(|e| e.to_string())?;
    Ok(page
        .executions
        .first()
        .and_then(|run| Some((run.execution_id()?, run.workflow_id.clone().into_value()?))))
}

/// Every running session: its run and its tag. A session that has not set
/// its tag yet has an empty tag.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub fn running_sessions(rt: &SqliteRuntime) -> Result<Vec<(ExecutionId, String)>, String> {
    let filter = ExecutionFilter::new()
        .workflow_name(SESSION)
        .state("RUNNING");
    let page = rt.list_executions(&filter).map_err(|e| e.to_string())?;
    Ok(page
        .executions
        .iter()
        .filter_map(|run| {
            let tag = run
                .search_attributes
                .clone()
                .into_value()
                .and_then(|attrs| attrs.get("tag")?.as_str().map(str::to_string))
                .unwrap_or_default();
            Some((run.execution_id()?, tag))
        })
        .collect())
}

/// The config of the session run `exec`, as its history records it.
///
/// # Errors
///
/// Returns an error when the run is unknown or its input does not decode.
pub fn session_config(rt: &SqliteRuntime, exec: ExecutionId) -> Result<SessionConfig, String> {
    let run = rt.describe(exec).map_err(|e| e.to_string())?;
    let input = run
        .input_json
        .into_value()
        .ok_or_else(|| format!("{exec} has no input"))?;
    let input: SessionInput = serde_json::from_str(&input)
        .map_err(|e| format!("the input of {exec} does not decode: {e}"))?;
    Ok(input.config)
}

/// Fail when the session run `exec` records a model other than `model`.
/// A session that records no model resumes with any.
///
/// # Errors
///
/// Returns an error that names both models when they differ.
pub fn check_model(rt: &SqliteRuntime, exec: ExecutionId, model: &str) -> Result<(), String> {
    let recorded = session_config(rt, exec)?.model;
    if recorded.is_empty() || recorded == model {
        Ok(())
    } else {
        Err(format!(
            "the session runs with the model {recorded}, not {model}. \
             Resume it with the same --model and --claude-model"
        ))
    }
}

/// Why `run` cannot go on while `others`, as `(tag, model)`, wait for a
/// runner, with the commands that free it. Only one runner can drive a
/// database, so no other runner can finish them.
///
/// `waiting_for` names the mutex that the session of `run` waits for. Two
/// sessions that wait for each other take turns, one experiment for each
/// `run`, so the message then puts the stop first. Each `run` and `stop`
/// command works as printed: it has the database, the model and the tag of
/// the session. The `serve` hint names no options, because `serve` writes
/// its nightly schedule from its own options.
#[must_use]
pub fn finish_first(
    lab: &std::path::Path,
    db: Option<&std::path::Path>,
    others: &[(String, String)],
    waiting_for: Option<&str>,
) -> String {
    let names: Vec<String> = others
        .iter()
        .map(|(tag, _)| {
            if tag.is_empty() {
                "a session with no tag yet".to_string()
            } else {
                format!("the session tagged {tag:?}")
            }
        })
        .collect();
    let (tag, model) = others
        .iter()
        .find(|(tag, _)| !tag.is_empty())
        .or_else(|| others.first())
        .map_or(("", ""), |(tag, model)| (tag.as_str(), model.as_str()));
    let lab = shell_word(&lab.display().to_string());
    let db = db.map_or_else(String::new, |db| {
        format!(" --db {}", shell_word(&db.display().to_string()))
    });
    let model = model_flags(model);
    let tag_flag = if tag.is_empty() {
        String::new()
    } else {
        format!(" --tag {}", shell_word(tag))
    };
    let resume = format!("`autoresearch run --lab {lab}{db}{model}{tag_flag}`");
    // `serve` writes its nightly schedule from its own options, so the
    // message names no `serve` command that could replace them.
    let serve = "`autoresearch serve` with your usual options drives every session.";
    let head = format!(
        "{} is running, and only one runner can drive this database.",
        names.join(", ")
    );
    if tag.is_empty() {
        return format!("{head} Resume it first with {resume}. {serve}");
    }
    let stop = format!("`autoresearch stop --lab {lab}{tag_flag}`");
    waiting_for.map_or_else(
        || {
            format!(
                "{head} Finish it first with {resume}. To end it after its experiment, \
                 post {stop} before that. {serve}"
            )
        },
        |key| {
            format!(
                "The session waits for {key}. {head} The two would take turns, one \
                 experiment for each `run`. End it first: post {stop}, then run {resume}. \
                 It ends after its experiment. {serve}"
            )
        },
    )
}

/// The flags that pick the model of `label`, such as
/// ` --model claude --claude-model <id>`. The scripted model is the default,
/// so it needs none.
fn model_flags(label: &str) -> String {
    label
        .strip_prefix("claude:")
        .map_or_else(String::new, |id| {
            format!(" --model claude --claude-model {}", shell_word(id))
        })
}

/// `word` as one shell word: as it is, or in single quotes.
#[must_use]
pub fn shell_word(word: &str) -> String {
    let plain = !word.is_empty()
        && word
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "/._-:=+@%,".contains(c));
    if plain {
        word.to_string()
    } else {
        format!("'{}'", word.replace('\'', "'\\''"))
    }
}

/// The `approve` and `deny` commands that decide `token` in `lab`. Each
/// word is quoted for a POSIX shell, so a command works as printed.
#[must_use]
pub fn decision_commands(lab: &std::path::Path, token: &str) -> [String; 2] {
    let lab = shell_word(&lab.display().to_string());
    let token = shell_word(token);
    ["approve", "deny"].map(|verb| format!("autoresearch {verb} --lab {lab} {token}"))
}

/// Fail when an unfinished run has another lab than `lab`, or another
/// model than `model`.
///
/// `serve` drives every unfinished run, and `run` must not start work that
/// needs another model than one that is still unfinished. A run of an agent
/// belongs to a session or to a heartbeat. A session records its lab and
/// its model, and its report agent can run on after the session ends. A
/// heartbeat run names the lab that it watches. It has the model of the
/// `serve` that started it, `heartbeat_model`, when one is known.
///
/// A runner serves one lab, and the agent tools of a run find their lab by
/// its path. So a run of another lab in the same database must wait for a
/// runner of that lab.
///
/// # Errors
///
/// Returns an error that names the run and both labs or both models when
/// they differ, or when the backend fails.
pub fn check_agents(
    rt: &SqliteRuntime,
    lab: &std::path::Path,
    model: &str,
    heartbeat_model: Option<&str>,
) -> Result<(), String> {
    let lab = canonical_lab(&lab.display().to_string());
    let filter = ExecutionFilter::new().state("RUNNING").limit(1000);
    let page = rt.list_executions(&filter).map_err(|e| e.to_string())?;
    let mut checked = HashSet::new();
    for run in &page.executions {
        let Some(exec) = run.execution_id() else {
            continue;
        };
        let root = root_of(rt, exec)?;
        if !checked.insert(root) {
            continue;
        }
        let name = rt
            .describe(root)
            .map_err(|e| e.to_string())?
            .summary
            .workflow_name
            .into_value()
            .unwrap_or_default();
        if name == SESSION {
            let config = session_config(rt, root)?;
            let id = run.workflow_id.clone().into_value().unwrap_or_default();
            if canonical_lab(&config.lab) != lab {
                return Err(other_lab(&format!("the session {id}"), &config.lab));
            }
            let recorded = config.model;
            if recorded.is_empty() || recorded == model {
                continue;
            }
            let ended = !matches!(
                rt.outcome(root).map_err(|e| e.to_string())?,
                ExecutionOutcome::Running
            );
            // The session ended, and its report agent runs on. Only `serve`
            // drives such a run.
            return Err(if ended {
                format!(
                    "the report agent {id} runs with the model {recorded}, not {model}, \
                     and it is not finished. Let `autoresearch serve` finish it with the \
                     same --model and --claude-model"
                )
            } else {
                format!(
                    "the session {id} runs with the model {recorded}, not {model}, and it is \
                     not finished. Resume it with the same --model and --claude-model"
                )
            });
        } else if name == heartbeat::HEARTBEAT_WORKFLOW_NAME {
            if let Some(watched) = watched_lab(rt, root)?
                && canonical_lab(&watched) != lab
            {
                return Err(other_lab("a heartbeat run", &watched));
            }
            let Some(started) = heartbeat_model.filter(|started| *started != model) else {
                continue;
            };
            return Err(format!(
                "a heartbeat run that `serve` started with the model {started} is not \
                 finished. Run `autoresearch serve{}` until it ends",
                model_flags(started)
            ));
        }
    }
    Ok(())
}

/// Why a runner of one lab cannot drive `what`, a run of the lab `other`.
fn other_lab(what: &str, other: &str) -> String {
    format!(
        "{what} belongs to the lab {other} and is not finished. Only a runner \
         with `--lab {}` can drive it",
        shell_word(other)
    )
}

/// The lab that the heartbeat run `exec` watches: the session id of its
/// task. `None` when the task names no session.
fn watched_lab(rt: &SqliteRuntime, exec: ExecutionId) -> Result<Option<String>, String> {
    let input = rt
        .describe(exec)
        .map_err(|e| e.to_string())?
        .input_json
        .into_value()
        .unwrap_or_default();
    let task: autumn_harvest_agent::heartbeat::HeartbeatTask = serde_json::from_str(&input)
        .map_err(|e| format!("the input of {exec} does not decode: {e}"))?;
    Ok(task.session_id.map(|id| id.as_str().to_string()))
}

/// Remove the finished runs that closed more than `older_than` ago, and
/// return how many went.
///
/// A detached report agent runs on after its session ends. A purge that
/// removes the ended session removes the model that the report needs. Then
/// `run` and `serve` can no longer check that model. So such a purge is
/// refused until the report ends.
///
/// # Errors
///
/// Returns an error that names the unfinished run, or a backend error.
pub fn purge(rt: &mut SqliteRuntime, older_than: Duration) -> Result<u64, String> {
    // The cutoff of `purge_terminal`. The purge holds the writer lock, so no
    // run closes between the two reads of the clock. A window past the
    // clock purges nothing.
    if let Some(cutoff) = chrono::TimeDelta::from_std(older_than)
        .ok()
        .and_then(|window| chrono::Utc::now().checked_sub_signed(window))
    {
        check_purge(rt, cutoff)?;
    }
    rt.purge_terminal(older_than).map_err(|e| e.to_string())
}

/// Fail when a purge with `cutoff` would remove a run above an unfinished
/// run.
fn check_purge(rt: &SqliteRuntime, cutoff: chrono::DateTime<chrono::Utc>) -> Result<(), String> {
    let filter = ExecutionFilter::new().state("RUNNING").limit(1000);
    let page = rt.list_executions(&filter).map_err(|e| e.to_string())?;
    for run in &page.executions {
        let Some(mut child) = run.execution_id() else {
            continue;
        };
        // A session, an experiment and two agent runs nest at most four deep.
        for _ in 0..16 {
            let Some(parent) = rt.parent_execution(child).map_err(|e| e.to_string())? else {
                break;
            };
            let closed = match rt.describe(parent) {
                Ok(described) => described.summary.closed_at.into_value(),
                Err(SqliteError::ExecutionNotFound(_)) => break,
                Err(e) => return Err(e.to_string()),
            };
            if closed.is_some_and(|closed| closed < cutoff) {
                let id = run.workflow_id.clone().into_value().unwrap_or_default();
                return Err(format!(
                    "the agent run {id} is not finished, and the purge would remove the \
                     run that started it. Let `autoresearch serve` finish it with the \
                     same --model and --claude-model, then purge"
                ));
            }
            child = parent;
        }
    }
    Ok(())
}

/// The top run above `exec`: the run that no other run started.
///
/// `purge` can remove a terminal parent while its detached child runs on.
/// The top run that still exists is then the root.
fn root_of(rt: &SqliteRuntime, exec: ExecutionId) -> Result<ExecutionId, String> {
    let mut run = exec;
    // A session, an experiment and two agent runs nest at most four deep.
    for _ in 0..16 {
        let Some(parent) = rt.parent_execution(run).map_err(|e| e.to_string())? else {
            return Ok(run);
        };
        match rt.parent_execution(parent) {
            Err(SqliteError::ExecutionNotFound(_)) => return Ok(run),
            Err(e) => return Err(e.to_string()),
            Ok(_) => run = parent,
        }
    }
    Ok(run)
}

/// The newest run of each of the `limit` newest sessions, newest first.
///
/// `continue_as_new` gives a session a chain of runs with one workflow id.
/// Only the newest run of a chain stands for its session, so a long session
/// cannot crowd another one out. `list` reads one page, as
/// `SqliteReader::list_executions` does.
///
/// # Errors
///
/// Returns an error when a page cannot be read.
pub fn latest_sessions(
    list: impl Fn(&ExecutionFilter) -> SqliteResult<ExecutionPage>,
    limit: usize,
) -> Result<Vec<ExecutionSummary>, String> {
    let mut seen = HashSet::new();
    let mut sessions = Vec::new();
    let mut filter = ExecutionFilter::new().workflow_name(SESSION).limit(100);
    while sessions.len() < limit {
        let page = list(&filter).map_err(|e| e.to_string())?;
        for run in page.executions {
            let id = run.workflow_id.clone().into_value().unwrap_or_default();
            if sessions.len() < limit && seen.insert(id) {
                sessions.push(run);
            }
        }
        let Some(cursor) = page.next else {
            break;
        };
        filter = filter.before(cursor);
    }
    Ok(sessions)
}

/// How a session ended.
#[derive(Debug, Clone, PartialEq)]
pub enum Ended {
    /// It completed with this report.
    Done(SessionReport),
    /// It failed, or it was cancelled or terminated, for this reason.
    Failed(String),
}

/// The newest session runs that ended, by workflow id, less those in `seen`.
///
/// Each run that this returns goes into `seen`, so a caller names it once.
/// A run that continued as new did not end.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub fn newly_ended<S: std::hash::BuildHasher>(
    rt: &SqliteRuntime,
    seen: &mut HashSet<ExecutionId, S>,
) -> Result<Vec<(String, Ended)>, String> {
    let filter = ExecutionFilter::new().workflow_name(SESSION).limit(100);
    let page = rt.list_executions(&filter).map_err(|e| e.to_string())?;
    let mut ended = Vec::new();
    for run in &page.executions {
        let Some(exec) = run.execution_id() else {
            continue;
        };
        if seen.contains(&exec) {
            continue;
        }
        let end = match rt.outcome(exec).map_err(|e| e.to_string())? {
            ExecutionOutcome::Running => continue,
            ExecutionOutcome::Terminated(state) if state == "CONTINUED_AS_NEW" => {
                seen.insert(exec);
                continue;
            }
            ExecutionOutcome::Completed(output) => serde_json::from_value(output)
                .map_or_else(|e| Ended::Failed(format!("no report: {e}")), Ended::Done),
            ExecutionOutcome::Failed(why) => Ended::Failed(why),
            ExecutionOutcome::Terminated(state) => {
                let why = rt
                    .describe(exec)
                    .ok()
                    .and_then(|run| run.error.into_value())
                    .unwrap_or_default();
                Ended::Failed(format!("{state}: {why}"))
            }
        };
        seen.insert(exec);
        let id = run.workflow_id.clone().into_value().unwrap_or_default();
        ended.push((id, end));
    }
    Ok(ended)
}

/// The nightly schedule: a session at 23:00 UTC with the tag derived from
/// the start time. A night that is still running skips the next slot.
///
/// # Errors
///
/// Returns an error when the config does not encode.
pub fn nightly(config: &SessionConfig) -> Result<WorkflowSchedule, String> {
    crate::activities::check_trainer(&config.trainer)?;
    config.policy.validate()?;
    let mut config = config.clone();
    config.tag.clear();
    config.lab = canonical_lab(&config.lab);
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
    let lab = std::path::PathBuf::from(canonical_lab(&lab.display().to_string()));
    heartbeat::schedule(every, &crate::agent::heartbeat_task(&lab)).map_err(|e| e.to_string())
}

/// The canonical form of the lab path `lab`. The lab mutex and the agent
/// runs name the lab by it. A path that does not resolve stays as it is,
/// and setup then names the problem.
fn canonical_lab(lab: &str) -> String {
    std::path::Path::new(lab)
        .canonicalize()
        .ok()
        .and_then(|root| root.to_str().map(str::to_string))
        .unwrap_or_else(|| lab.to_string())
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

/// Drop the progress events of each activity in flight: its start and its
/// heartbeats. Such an activity has no end, and the matcher does not consume
/// its progress at the frontier.
fn trim_frontier(history: &mut Vec<WorkflowEvent>) {
    let ended: HashSet<_> = history
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityCompleted { activity_id, .. }
            | WorkflowEvent::ActivityFailed { activity_id, .. }
            | WorkflowEvent::ActivityTimedOut { activity_id, .. } => Some(*activity_id),
            _ => None,
        })
        .collect();
    history.retain(|event| match event {
        WorkflowEvent::ActivityStarted { activity_id, .. }
        | WorkflowEvent::ActivityHeartbeat { activity_id, .. } => ended.contains(activity_id),
        _ => true,
    });
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
        trim_frontier(&mut history);
    }
    let recorded_failure = history.iter().rev().find_map(|event| match event {
        WorkflowEvent::WorkflowFailed { error, .. } => Some(error.clone()),
        _ => None,
    });
    let snapshot = HistorySnapshot {
        workflow_name: name,
        execution_id: exec,
        events: history,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: parent,
        start_provenance: None,
        workflow_id: Some(workflow_id),
        queue_name: None,
    };
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

/// The newest finished proposal run of the researcher. It reads one page
/// of runs at a time, with their inputs, and stops at the first proposal.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub fn latest_proposal(rt: &SqliteRuntime) -> Result<Option<ExecutionId>, String> {
    let mut filter = ExecutionFilter::new()
        .workflow_name(autumn_harvest_agent::WORKFLOW_NAME)
        .state("COMPLETED")
        .limit(100);
    loop {
        let mut found = None;
        let next = rt
            .visit_executions(&filter, |run| {
                let is_proposal = run
                    .input_json
                    .clone()
                    .into_value()
                    .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                    .is_some_and(|task| {
                        task["system"]
                            .as_str()
                            .is_some_and(|s| s.contains(RESEARCHER_ROLE))
                            && !task["input"].as_str().is_some_and(|i| i.contains(FIX_MODE))
                    });
                if is_proposal && found.is_none() {
                    found = run.summary.execution_id();
                }
            })
            .map_err(|e| format!("cannot list the agent runs: {e}"))?;
        match (found, next) {
            (Some(exec), _) => return Ok(Some(exec)),
            (None, Some(cursor)) => filter = filter.before(cursor),
            (None, None) => return Ok(None),
        }
    }
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use autumn_harvest::types::{ActivityExecId, WorkerId};
    use serde_json::Value;

    use super::*;

    /// An activity in flight has a start and heartbeats, and no end. The
    /// canary stops at it, so neither may stay in the history.
    #[test]
    fn the_frontier_loses_each_event_of_an_activity_in_flight() {
        let (done, open) = (ActivityExecId::new(), ActivityExecId::new());
        let worker = WorkerId::new("w");
        let mut history = vec![
            WorkflowEvent::ActivityStarted {
                activity_id: done,
                worker_id: worker.clone(),
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: done,
                details: Value::Null,
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: done,
                output: Value::Null,
            },
            WorkflowEvent::ActivityStarted {
                activity_id: open,
                worker_id: worker,
            },
            WorkflowEvent::ActivityHeartbeat {
                activity_id: open,
                details: Value::Null,
            },
        ];
        trim_frontier(&mut history);
        let kinds: Vec<&str> = history.iter().map(WorkflowEvent::type_name).collect();
        assert_eq!(
            kinds,
            ["ActivityStarted", "ActivityHeartbeat", "ActivityCompleted"]
        );
    }

    /// The commands in a refusal must work as printed. They name the
    /// database, the model and the tag of the other session. A session with
    /// no tag yet gets no tag.
    /// A lab path can hold a space, a quote or shell syntax. The printed
    /// `approve` and `deny` commands still pass each word as it is.
    #[cfg(unix)]
    #[test]
    fn the_decision_commands_work_as_printed() {
        let lab = std::path::Path::new("/tmp/my lab/it's $(touch x)");
        let token = "0192-abc/tool_approval:0:1:call 'x'";
        for command in decision_commands(lab, token) {
            let words = command.replacen("autoresearch", "printf '%s\\n'", 1);
            let out = std::process::Command::new("sh")
                .args(["-c", &words])
                .output()
                .unwrap();
            let got = String::from_utf8(out.stdout).unwrap();
            let got: Vec<&str> = got.lines().collect();
            assert_eq!(
                got[1..],
                ["--lab", "/tmp/my lab/it's $(touch x)", token],
                "{command}"
            );
        }
    }

    #[test]
    fn a_refusal_names_commands_that_work_as_printed() {
        let lab = std::path::Path::new("/labs/my lab");
        let db = std::path::Path::new("/dbs/night.db");
        let claude = vec![("y".to_string(), "claude:claude-x".to_string())];
        let text = finish_first(lab, Some(db), &claude, None);
        assert!(text.contains("tagged \"y\""), "{text}");
        assert!(
            text.contains(
                "autoresearch run --lab '/labs/my lab' --db /dbs/night.db --model claude \
                 --claude-model claude-x --tag y"
            ),
            "{text}"
        );
        assert!(
            text.contains("autoresearch stop --lab '/labs/my lab' --tag y"),
            "{text}"
        );
        assert!(
            text.contains("`autoresearch serve` with your usual options"),
            "{text}"
        );

        // A session with no tag yet resumes with no tag, and no stop names it.
        let untagged = vec![(String::new(), "scripted".to_string())];
        let text = finish_first(lab, None, &untagged, None);
        assert!(!text.contains("<tag>") && !text.contains("--tag"), "{text}");
        assert!(
            text.contains("autoresearch run --lab '/labs/my lab'`"),
            "{text}"
        );

        // Two sessions that wait for each other take turns one experiment at
        // a time, so the stop comes first.
        let scripted = vec![("b".to_string(), "scripted".to_string())];
        let text = finish_first(lab, None, &scripted, Some("lab:/labs/my lab"));
        let stop = text.find("autoresearch stop").unwrap();
        let run = text.find("autoresearch run").unwrap();
        assert!(stop < run, "{text}");
        assert!(text.contains("lab:/labs/my lab"), "{text}");
    }
}
