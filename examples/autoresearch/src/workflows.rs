//! The two workflows: a research session and one experiment.
//!
//! [`research_session`] is the night. It runs setup and a baseline, then one
//! [`experiment`] child per idea. It continues as new every
//! `experiments_per_run` experiments, so its history stays small. Signals
//! steer it, a query reads it, and an update answers a what-if.
//!
//! [`experiment`] is one idea. An `agent_loop` child proposes an edit. A saga
//! commits it, with a reset as the compensation. A GPU mutex guards the
//! training. The keep rule decides, and a discard or a crash compensates.

use autumn_harvest::prelude::*;
use autumn_harvest_agent::AgentTask;
use serde::{Deserialize, Serialize};

use crate::results::ResultRow;
use crate::rules::{Best, KeepPolicy, Verdict};
use crate::trainer::{TrainRun, TrainerSpec};

/// The registered name of [`research_session`].
pub const SESSION: &str = "research_session";
/// The registered name of [`experiment`].
pub const EXPERIMENT: &str = "experiment";
/// The signal that adds an idea for a later experiment.
pub const HINT: &str = "hint";
/// The signal that ends the session after the experiment in flight.
pub const STOP: &str = "stop";
/// The signal that replaces the keep policy.
pub const POLICY: &str = "policy";
/// The query that reads the session.
pub const STATUS: &str = "status";
/// The update that answers "would this result be kept?".
pub const WHATIF: &str = "whatif";
/// A crash gets this many fix proposals.
pub const MAX_FIXES: u32 = 1;
/// The rows of recent history that a prompt and the status show.
pub const RECENT_ROWS: usize = 10;
/// The line that separates `program.md` from the session context in a
/// system prompt. `eval` swaps the text above it.
pub const CONTEXT_MARKER: &str = "<!-- autoresearch: session context -->";

/// The bounds of each agent run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSettings {
    /// Tool rounds per agent run.
    pub max_steps: u32,
    /// Tokens per agent run.
    pub max_total_tokens: u32,
    /// How long a gated call waits for a person.
    pub approval_timeout_secs: u64,
}

impl Default for AgentSettings {
    fn default() -> Self {
        Self {
            max_steps: 12,
            max_total_tokens: 400_000,
            approval_timeout_secs: 900,
        }
    }
}

/// The settings of one session. History records them at the start.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionConfig {
    /// The lab path.
    pub lab: String,
    /// The run tag. Empty means "derive it from the start time".
    pub tag: String,
    /// The GPU that the session trains on. It names the mutex.
    pub gpu: String,
    /// How to train.
    pub trainer: TrainerSpec,
    /// A directory that setup requires, such as `~/.cache/autoresearch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,
    /// Experiments after the baseline. Zero runs until a `stop` signal.
    pub max_experiments: u32,
    /// The session continues as new after this many experiments.
    pub experiments_per_run: u32,
    /// The keep rule.
    pub policy: KeepPolicy,
    /// The chance, in percent, that an experiment tries a bolder idea.
    pub explore_percent: u8,
    /// The workflow ids of sibling sessions. A kept idea goes to each.
    #[serde(default)]
    pub siblings: Vec<String>,
    /// The bounds of each agent run.
    #[serde(default)]
    pub agent: AgentSettings,
    /// Start a detached agent that writes the morning report at the end.
    #[serde(default)]
    pub report: bool,
}

impl SessionConfig {
    /// A session on `lab` with the simulated trainer and the defaults.
    #[must_use]
    pub fn new(lab: impl Into<String>, tag: impl Into<String>) -> Self {
        Self {
            lab: lab.into(),
            tag: tag.into(),
            gpu: "gpu0".into(),
            trainer: TrainerSpec::default(),
            data_dir: None,
            max_experiments: 8,
            experiments_per_run: 20,
            policy: KeepPolicy::default(),
            explore_percent: 10,
            siblings: Vec::new(),
            agent: AgentSettings::default(),
            report: true,
        }
    }
}

/// An idea for a later experiment, from a person or a sibling session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hint {
    /// The idea.
    pub text: String,
    /// Who sent it: `human` or a sibling workflow id.
    pub from: String,
}

/// How many experiments ended in each status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    /// Kept.
    pub kept: u32,
    /// Discarded.
    pub discarded: u32,
    /// Crashed.
    pub crashed: u32,
}

/// The state that a session carries across `continue_as_new`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Carry {
    /// The resolved tag.
    pub tag: String,
    /// The research branch.
    pub branch: String,
    /// `program.md` at the start of the session.
    pub program: String,
    /// The base of the next experiment.
    pub best: Best,
    /// Experiments after the baseline so far.
    pub done: u32,
    /// The last [`RECENT_ROWS`] rows.
    pub recent: Vec<ResultRow>,
    /// Hints not yet used.
    pub hints: Vec<Hint>,
    /// The keep rule now in force.
    pub policy: KeepPolicy,
    /// The status counts so far, baseline included.
    pub counts: Counts,
    /// Runs of this session before this one.
    pub runs: u32,
}

/// The input of [`research_session`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInput {
    /// The settings.
    pub config: SessionConfig,
    /// `None` on the first run. Set by `continue_as_new`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carry: Option<Carry>,
}

/// The output of [`research_session`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionReport {
    /// The resolved tag.
    pub tag: String,
    /// The research branch.
    pub branch: String,
    /// Experiments after the baseline.
    pub experiments: u32,
    /// The best kept result.
    pub best: Best,
    /// The status counts, baseline included.
    pub counts: Counts,
    /// The reason of a `stop` signal, if one ended the session.
    pub stopped: Option<String>,
    /// The execution id of the detached report agent, if one started.
    pub report_run: Option<String>,
}

/// What the `status` query returns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusView {
    /// The resolved tag. Empty before setup.
    pub tag: String,
    /// `setup`, `baseline`, `experimenting` or `done`.
    pub phase: String,
    /// Experiments after the baseline so far.
    pub experiments: u32,
    /// The best kept result so far.
    pub best: Option<Best>,
    /// The keep rule now in force.
    pub policy: KeepPolicy,
    /// Hints not yet used.
    pub hints: Vec<Hint>,
    /// The last rows.
    pub recent: Vec<ResultRow>,
}

/// The input of the `whatif` update.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WhatIf {
    /// A `val_bpb` to test.
    pub val_bpb: f64,
    /// Its peak memory in GB.
    #[serde(default)]
    pub memory_gb: f64,
    /// Lines that the change adds.
    #[serde(default)]
    pub lines_added: u32,
    /// Lines that the change removes.
    #[serde(default)]
    pub lines_removed: u32,
}

/// The input of [`experiment`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExperimentInput {
    /// The lab path.
    pub lab: String,
    /// The run tag.
    pub tag: String,
    /// The workflow id of the session, for the agent memory and the logs.
    pub session: String,
    /// The GPU mutex key suffix.
    pub gpu: String,
    /// How to train.
    pub trainer: TrainerSpec,
    /// 0 for the baseline, then 1, 2, ...
    pub number: u32,
    /// Train `HEAD` as it is, with no proposal.
    pub baseline: bool,
    /// The base result. `None` only for the baseline.
    pub best: Option<Best>,
    /// The keep rule.
    pub policy: KeepPolicy,
    /// An idea to try first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<Hint>,
    /// Try a bolder idea.
    #[serde(default)]
    pub explore: bool,
    /// The last rows, for the prompt.
    #[serde(default)]
    pub recent: Vec<ResultRow>,
    /// `program.md`: the system prompt of the researcher.
    pub program: String,
    /// The bounds of each agent run.
    #[serde(default)]
    pub agent: AgentSettings,
}

/// The output of [`experiment`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExperimentOutcome {
    /// The row that `results.tsv` got.
    pub row: ResultRow,
    /// The base after this experiment: the new commit on a keep.
    pub best: Option<Best>,
    /// The decision.
    pub verdict: Verdict,
    /// An approved `rewind` moved the base to an older kept commit.
    pub rewound: bool,
    /// Fix proposals after a crash.
    pub fixes: u32,
}

/// One night of research. See the module docs.
///
/// # Errors
///
/// Returns an error when setup fails for good, or when the baseline
/// crashes: the loop has nothing to compare against.
#[workflow]
pub async fn research_session(
    ctx: &WorkflowContext,
    input: SessionInput,
) -> Result<SessionReport, String> {
    let _ = (ctx, input);
    Err("green phase".into())
}

/// One idea: propose, commit, train, decide, record.
///
/// # Errors
///
/// Returns an error when a git step fails for good, or a child fails.
#[workflow]
pub async fn experiment(
    ctx: &WorkflowContext,
    input: ExperimentInput,
) -> Result<ExperimentOutcome, String> {
    let _ = (ctx, input);
    Err("green phase".into())
}

/// The task of the researcher agent for one experiment.
#[must_use]
pub fn proposal_task(input: &ExperimentInput) -> AgentTask {
    let _ = input;
    unimplemented!("green phase")
}

/// The task of the researcher agent after a crash.
#[must_use]
pub fn fix_task(input: &ExperimentInput, run: &TrainRun) -> AgentTask {
    let _ = (input, run);
    unimplemented!("green phase")
}

/// Swap the `program.md` part of a recorded system prompt for `program`.
/// The session context below [`CONTEXT_MARKER`] stays.
#[must_use]
pub fn with_program(system: &str, program: &str) -> String {
    let _ = (system, program);
    unimplemented!("green phase")
}

/// The two workflows of the example, for registration.
#[must_use]
pub fn all() -> Vec<WorkflowInfo> {
    vec![research_session_info(), experiment_info()]
}
