//! Register the example on the SQLite backend, and drive a session.
//!
//! One process is the single writer. [`register`] adds the agent workflows,
//! this example's workflows and every activity. [`start`] starts a session
//! under the workflow id `autoresearch/<tag>`, and refuses a tag in use, as
//! upstream refuses a branch that exists. [`drive`] carries the session
//! forward until it ends or needs a person.

use std::sync::Arc;

use autumn_harvest::policy::WorkflowSchedule;
use autumn_harvest_agent::AgentHarness;
use autumn_harvest_sqlite::{ExecutionId, SqliteRuntime};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::inbox::Command;
use crate::workflows::{SessionConfig, SessionReport};

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
    let _ = (rt, harness);
    unimplemented!("green phase")
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
    let _ = (rt, config);
    unimplemented!("green phase")
}

/// One gated tool call that waits for a person.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    let _ = (rt, exec);
    unimplemented!("green phase")
}

/// Deliver one inbox command, keyed by its inbox id.
///
/// Returns a line for the operator.
///
/// # Errors
///
/// Returns an error when the target is unknown or not running.
pub fn apply(rt: &mut SqliteRuntime, id: &str, command: &Command) -> Result<String, String> {
    let _ = (rt, id, command);
    unimplemented!("green phase")
}

/// The running run of the session with `tag`, across `continue_as_new`.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub fn find_session(rt: &SqliteRuntime, tag: &str) -> Result<Option<ExecutionId>, String> {
    let _ = (rt, tag);
    unimplemented!("green phase")
}

/// The nightly schedule: a session at 23:00 UTC with the tag derived from
/// the start time. A night that is still running skips the next slot.
///
/// # Errors
///
/// Returns an error when the config does not encode.
pub fn nightly(config: &SessionConfig) -> Result<WorkflowSchedule, String> {
    let _ = config;
    unimplemented!("green phase")
}

/// The heartbeat schedule: a tick every 30 minutes that watches `lab`.
///
/// # Errors
///
/// Returns an error when the tick does not encode.
pub fn heartbeat(lab: &std::path::Path) -> Result<WorkflowSchedule, String> {
    let _ = lab;
    unimplemented!("green phase")
}

/// The same workflows and activities on a Postgres `HarvestBuilder`, with an
/// LLM budget on `agent_loop`. The SQLite backend refuses a budget, so the
/// budget exists on Postgres only.
#[must_use]
pub fn postgres(harness: AgentHarness) -> autumn_harvest::builder::BuiltHarvest {
    let _ = harness;
    unimplemented!("green phase")
}

/// The result of [`verify`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyReport {
    /// Histories that replayed.
    pub replayed: usize,
    /// One line per history that did not replay.
    pub failures: Vec<String>,
}

/// Replay every stored history on the core `WorkflowReplayer` with the
/// workflows of this build. Run it before a deploy: a failure names a run
/// that the new code would break.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub async fn verify(rt: &SqliteRuntime) -> Result<VerifyReport, String> {
    let _ = rt;
    unimplemented!("green phase")
}

/// The finished proposal runs of the researcher, newest first.
///
/// # Errors
///
/// Returns an error when the backend fails.
pub fn proposals(rt: &SqliteRuntime) -> Result<Vec<ExecutionId>, String> {
    let _ = rt;
    unimplemented!("green phase")
}

/// Re-drive the recorded proposal `run` with `program` in place of the
/// recorded `program.md`, on `harness`. No tool runs, and no row lands.
///
/// # Errors
///
/// Returns an error when the history cannot be read or evaluated.
pub async fn eval(
    rt: &SqliteRuntime,
    harness: AgentHarness,
    run: ExecutionId,
    program: &str,
) -> Result<autumn_harvest_agent::eval::Evaluation, String> {
    let _ = (rt, harness, run, program);
    unimplemented!("green phase")
}
