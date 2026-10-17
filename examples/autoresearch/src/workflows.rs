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
//!
//! The session holds a mutex on its lab for each experiment. So two sessions
//! in one lab take turns, and each experiment checks out its own branch
//! first.
//!
//! Flow labels follow the session. A run that reads untrusted text can put
//! it into a memory note or an answer line, and later runs read both. So the
//! labels of each run go into the next run of the session, and with each
//! hint to a sibling.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use autumn_harvest::ParentClosePolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::saga::Saga;
use autumn_harvest_agent::flow::labels;
use autumn_harvest_agent::workflow::agent_loop_info;
use autumn_harvest_agent::{AgentReport, AgentStop, AgentTask, Mandate, MemoryScope, SessionId};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::activities::{
    CommitInput, CommitRef, LabInput, RecordInput, SetupInput, SetupReport, TrainInput,
    commit_info, find_kept_info, head_info, parse_summary_info, record_info, reset_info,
    setup_info, train_info,
};
use crate::agent::{RESEARCHER_ROLE, REVIEWER, report_task};
use crate::lab::CommitInfo;
use crate::results::{ResultRow, short, table};
use crate::rules::{Best, KeepPolicy, Measured, Status, Verdict, decide, memory_gb};
use crate::scripted::{EXPLORE, FIX_MODE, GIVE_UP, HINT as HINT_LINE};
use crate::summary::Summary;
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
    /// The run tag. Empty means "derive it from the start time", such as
    /// `20261017-2300`.
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
    /// The flow labels of the sender. The receiver adds them to its own.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub labels: BTreeSet<String>,
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
    /// The flow labels that the runs of this session exercised.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub flow_labels: BTreeSet<String>,
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
    /// The session branch.
    pub branch: String,
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
    /// The flow labels of the session so far. Each agent run starts with them.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub flow_labels: BTreeSet<String>,
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
    /// The flow labels of the session after this experiment.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub flow_labels: BTreeSet<String>,
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
    let SessionInput { config, carry } = input;
    // A run that cannot fit its activity would crash each experiment. A
    // policy that a signal could not carry would judge each one wrongly.
    crate::activities::check_trainer(&config.trainer)?;
    config.policy.validate()?;
    let view = Arc::new(Mutex::new(StatusView {
        tag: carry.as_ref().map_or_else(String::new, |c| c.tag.clone()),
        phase: "setup".into(),
        experiments: carry.as_ref().map_or(0, |c| c.done),
        best: carry.as_ref().map(|c| c.best.clone()),
        policy: carry.as_ref().map_or(config.policy, |c| c.policy),
        hints: carry.as_ref().map_or_else(Vec::new, |c| c.hints.clone()),
        recent: carry.as_ref().map_or_else(Vec::new, |c| c.recent.clone()),
    }));
    register_handlers(ctx, &view);
    let mut carry = match carry {
        Some(carry) => carry,
        None => first_run(ctx, &config, &view).await?,
    };
    show(&view, &carry, "experimenting");
    publish(ctx, &carry, "experimenting")?;
    let mut this_run = 0_u32;
    let mut stopped = None;
    loop {
        if let Some(reason) = absorb(ctx, &mut carry)? {
            stopped = Some(reason);
            break;
        }
        show(&view, &carry, "experimenting");
        if config.max_experiments > 0 && carry.done >= config.max_experiments {
            break;
        }
        if this_run >= config.experiments_per_run.max(1) {
            // Bound the history: a new run with the same workflow id carries on.
            carry.runs = carry.runs.saturating_add(1);
            let next = SessionInput {
                config: config.clone(),
                carry: Some(carry),
            };
            ctx.continue_as_new(serde_json::to_value(next).map_err(|e| e.to_string())?)
                .await
                .map_err(|e| e.to_string())?;
            return Err("continue_as_new returned".into());
        }
        let explore = ctx.random_range(0_u8..100) < config.explore_percent;
        let hint = (!carry.hints.is_empty()).then(|| carry.hints.remove(0));
        let input = ExperimentInput {
            number: carry.done + 1,
            baseline: false,
            best: Some(carry.best.clone()),
            policy: carry.policy,
            hint,
            explore,
            recent: carry.recent.clone(),
            flow_labels: carry.flow_labels.clone(),
            ..experiment_input(ctx, &config, &carry.tag, &carry.branch, &carry.program)
        };
        let outcome = run_experiment(ctx, &config, &input).await?;
        this_run += 1;
        carry.done += 1;
        count(&mut carry.counts, outcome.row.status);
        remember(&mut carry.recent, outcome.row.clone());
        carry
            .flow_labels
            .extend(outcome.flow_labels.iter().cloned());
        if let Some(best) = outcome.best.clone() {
            carry.best = best;
        }
        if outcome.row.status == Status::Keep {
            share(ctx, &config, &carry, &outcome.row).await;
        }
        show(&view, &carry, "experimenting");
        publish(ctx, &carry, "experimenting")?;
    }
    show(&view, &carry, "done");
    publish(ctx, &carry, "done")?;
    let report_run = if config.report {
        let task = report_task(&config.lab, &carry.tag, &summary(&carry));
        let run = ctx
            .spawn_child_workflow_detached(&agent_loop_info(), &task, ParentClosePolicy::Abandon)
            .map_err(|e| format!("cannot start the report agent: {e}"))?;
        Some(run.to_string())
    } else {
        None
    };
    Ok(SessionReport {
        tag: carry.tag,
        branch: carry.branch,
        experiments: carry.done,
        best: carry.best,
        counts: carry.counts,
        stopped,
        report_run,
    })
}

/// The first run of a session: setup and the baseline.
async fn first_run(
    ctx: &WorkflowContext,
    config: &SessionConfig,
    view: &Arc<Mutex<StatusView>>,
) -> Result<Carry, String> {
    // A derived tag holds the year, so a nightly slot never meets the branch
    // of the same slot a year before.
    let tag = if config.tag.trim().is_empty() {
        ctx.now().format("%Y%m%d-%H%M").to_string()
    } else {
        config.tag.clone()
    };
    lock(view).tag.clone_from(&tag);
    // Commands find a session by its tag. A scheduled session has no other
    // name, so the tag goes out before the first wait for the lab.
    ctx.upsert_search_attrs([("tag".to_string(), Some(json!(tag)))])
        .map_err(|e| e.to_string())?;
    // The setup and the baseline run while the session holds its lab.
    let lab = ctx
        .mutex(lab_key(&config.lab))
        .acquire()
        .await
        .map_err(|e| format!("cannot take the lab: {e}"))?;
    let setup: SetupReport = ctx
        .execute_activity(
            &setup_info(),
            SetupInput {
                lab: config.lab.clone(),
                tag: tag.clone(),
                data_dir: config.data_dir.clone(),
            },
        )
        .await
        .map_err(|e| format!("setup failed: {e}"))?;
    // The wall-clock start, recorded once so replay reads the same value.
    let started = ctx.system_now();
    ctx.upsert_search_attrs([
        ("started_at".to_string(), Some(json!(started.to_rfc3339()))),
        ("phase".to_string(), Some(json!("baseline"))),
    ])
    .map_err(|e| e.to_string())?;
    lock(view).phase = "baseline".into();
    let baseline = experiment_input(ctx, config, &tag, &setup.branch, &setup.program);
    let outcome: ExperimentOutcome = ctx
        .spawn_child_workflow(&experiment_info(), &baseline)
        .await
        .map_err(|e| format!("the baseline failed: {e}"))?;
    lab.release();
    let (Status::Keep, Some(best)) = (outcome.row.status, outcome.best.clone()) else {
        return Err(format!(
            "the baseline crashed, so there is nothing to compare against: {}",
            outcome.row.description
        ));
    };
    let mut counts = Counts::default();
    count(&mut counts, Status::Keep);
    Ok(Carry {
        tag,
        branch: setup.branch,
        program: setup.program,
        best,
        done: 0,
        recent: vec![outcome.row],
        hints: Vec::new(),
        policy: config.policy,
        counts,
        runs: 0,
        flow_labels: BTreeSet::new(),
    })
}

/// The input of an experiment of this session: the baseline as given. The
/// loop sets the fields of a later experiment.
fn experiment_input(
    ctx: &WorkflowContext,
    config: &SessionConfig,
    tag: &str,
    branch: &str,
    program: &str,
) -> ExperimentInput {
    ExperimentInput {
        lab: config.lab.clone(),
        tag: tag.to_string(),
        branch: branch.to_string(),
        session: ctx.workflow_id().to_string(),
        gpu: config.gpu.clone(),
        trainer: config.trainer.clone(),
        number: 0,
        baseline: true,
        best: None,
        policy: config.policy,
        hint: None,
        explore: false,
        recent: Vec::new(),
        program: program.to_string(),
        agent: config.agent,
        flow_labels: BTreeSet::new(),
    }
}

/// The name of the mutex on a lab.
fn lab_key(lab: &str) -> String {
    format!("lab:{lab}")
}

/// Run one experiment child while the session holds its lab. A child that
/// fails for good becomes a crash row.
async fn run_experiment(
    ctx: &WorkflowContext,
    config: &SessionConfig,
    input: &ExperimentInput,
) -> Result<ExperimentOutcome, String> {
    let lab = ctx
        .mutex(lab_key(&config.lab))
        .acquire()
        .await
        .map_err(|e| format!("cannot take the lab: {e}"))?;
    let outcome = match ctx.spawn_child_workflow(&experiment_info(), input).await {
        Ok(outcome) => outcome,
        Err(error) => failed_experiment(ctx, input, &error.to_string()).await?,
    };
    lab.release();
    Ok(outcome)
}

/// Register the `status` query and the `whatif` update.
fn register_handlers(ctx: &WorkflowContext, view: &Arc<Mutex<StatusView>>) {
    let read = Arc::clone(view);
    ctx.register_query(STATUS, move || {
        serde_json::to_value(&*lock(&read)).unwrap_or(Value::Null)
    });
    let read = Arc::clone(view);
    ctx.register_update_handler(WHATIF, validate_whatif, move |input| {
        let (policy, best) = {
            let view = lock(&read);
            (view.policy, view.best.clone())
        };
        async move {
            let what: WhatIf = serde_json::from_value(input).map_err(|e| e.to_string())?;
            let measured = Measured {
                val_bpb: Some(what.val_bpb),
                memory_gb: what.memory_gb,
                lines_added: what.lines_added,
                lines_removed: what.lines_removed,
            };
            serde_json::to_value(decide(&policy, best.as_ref(), &measured))
                .map_err(|e| e.to_string())
        }
    });
}

/// The validator of `whatif`: a finite, positive `val_bpb` and a memory
/// that is not negative.
fn validate_whatif(input: &Value) -> Result<(), String> {
    let what: WhatIf = serde_json::from_value(input.clone()).map_err(|e| e.to_string())?;
    if !what.val_bpb.is_finite() || what.val_bpb <= 0.0 {
        return Err(format!(
            "val_bpb must be finite and positive, not {}",
            what.val_bpb
        ));
    }
    if !what.memory_gb.is_finite() || what.memory_gb < 0.0 {
        return Err(format!(
            "memory_gb must be finite and not negative, not {}",
            what.memory_gb
        ));
    }
    Ok(())
}

/// Read the signals that arrived since the last experiment. Returns the
/// reason of a `stop`. A payload that does not decode is logged and skipped,
/// so one bad signal cannot fail the night.
fn absorb(ctx: &WorkflowContext, carry: &mut Carry) -> Result<Option<String>, String> {
    for raw in ctx.drain_signals_raw(HINT).map_err(|e| e.to_string())? {
        match serde_json::from_value::<Hint>(raw) {
            Ok(hint) if !hint.text.trim().is_empty() => {
                carry.flow_labels.extend(hint.labels.iter().cloned());
                carry.hints.push(hint);
            }
            _ => ctx.log_warn("a hint signal was not {text, from}; it is skipped"),
        }
    }
    for raw in ctx.drain_signals_raw(POLICY).map_err(|e| e.to_string())? {
        match serde_json::from_value::<KeepPolicy>(raw) {
            Ok(policy) if policy.validate().is_ok() => carry.policy = policy,
            _ => ctx.log_warn("a policy signal was not a valid keep policy; it is skipped"),
        }
    }
    Ok(ctx
        .drain_signals_raw(STOP)
        .map_err(|e| e.to_string())?
        .into_iter()
        .next()
        .map(|raw| raw["reason"].as_str().unwrap_or("stopped").to_string()))
}

/// Tell each sibling session about a kept idea. A sibling that does not
/// exist, or has ended, only logs a warning.
async fn share(ctx: &WorkflowContext, config: &SessionConfig, carry: &Carry, row: &ResultRow) {
    let me = ctx.workflow_id().to_string();
    let hint = Hint {
        text: format!(
            "{me} kept {}: {} (val_bpb {:.6})",
            row.commit, row.description, row.val_bpb
        ),
        from: me.clone(),
        labels: carry.flow_labels.clone(),
    };
    for sibling in &config.siblings {
        // One key per experiment, so a replayed send dedupes.
        let key = format!("{me}#{}#{}", carry.runs, carry.done);
        let sent = ctx
            .signal_external_workflow_by_id_with_idempotency(SESSION, sibling, HINT, &hint, key)
            .await;
        if let Err(error) = sent {
            ctx.log_warn(&format!(
                "cannot tell {sibling} about {}: {error}",
                row.commit
            ));
        }
    }
}

/// A crash row for an experiment child that failed for good, after a reset
/// to the base so the next experiment starts clean.
///
/// The labels of the child are lost with it. It could have read a paper, so
/// the session counts it as untrusted input.
async fn failed_experiment(
    ctx: &WorkflowContext,
    input: &ExperimentInput,
    error: &str,
) -> Result<ExperimentOutcome, String> {
    let base = input
        .best
        .clone()
        .ok_or("a failed experiment has no base to reset to")?;
    // The row names the hint, so a person can send it again.
    let hint = input
        .hint
        .as_ref()
        .map_or_else(String::new, |h| format!(" (hint: {})", h.text));
    let row = ResultRow {
        commit: short(&base.commit),
        val_bpb: 0.0,
        memory_gb: 0.0,
        status: Status::Crash,
        description: format!("experiment {} failed{hint}: {error}", input.number),
    };
    reset_and_record(ctx, input, &base.commit, &row).await?;
    let mut flow_labels = input.flow_labels.clone();
    flow_labels.insert(labels::UNTRUSTED_INPUT.to_string());
    Ok(ExperimentOutcome {
        verdict: Verdict {
            status: Status::Crash,
            score: 0.0,
            reason: "the experiment workflow failed".into(),
        },
        row,
        best: Some(base),
        rewound: false,
        fixes: 0,
        flow_labels,
    })
}

/// Reset the session branch to `commit`, then record `row`: the end of an
/// experiment that trained nothing.
async fn reset_and_record(
    ctx: &WorkflowContext,
    input: &ExperimentInput,
    commit: &str,
    row: &ResultRow,
) -> Result<(), String> {
    reset(ctx, input, commit).await?;
    record(ctx, input, row).await
}

/// Reset the session branch to `commit`.
async fn reset(ctx: &WorkflowContext, input: &ExperimentInput, commit: &str) -> Result<(), String> {
    ctx.execute_activity::<_, String>(
        &reset_info(),
        CommitRef {
            lab: input.lab.clone(),
            branch: input.branch.clone(),
            commit: commit.to_string(),
        },
    )
    .await
    .map(drop)
    .map_err(|e| format!("cannot reset the branch to {commit}: {e}"))
}

/// Append `row` to `results.tsv`.
async fn record(
    ctx: &WorkflowContext,
    input: &ExperimentInput,
    row: &ResultRow,
) -> Result<(), String> {
    ctx.execute_activity::<_, bool>(
        &record_info(),
        RecordInput {
            lab: input.lab.clone(),
            row: row.clone(),
        },
    )
    .await
    .map(drop)
    .map_err(|e| format!("cannot record the row: {e}"))
}

/// The head of the session branch. The step checks the branch out first.
async fn head(ctx: &WorkflowContext, input: &ExperimentInput) -> Result<String, String> {
    ctx.execute_activity(
        &head_info(),
        LabInput {
            lab: input.lab.clone(),
            branch: input.branch.clone(),
        },
    )
    .await
    .map_err(|e| format!("cannot read HEAD: {e}"))
}

const fn count(counts: &mut Counts, status: Status) {
    match status {
        Status::Keep => counts.kept += 1,
        Status::Discard => counts.discarded += 1,
        Status::Crash => counts.crashed += 1,
    }
}

fn remember(recent: &mut Vec<ResultRow>, row: ResultRow) {
    recent.push(row);
    let extra = recent.len().saturating_sub(RECENT_ROWS);
    recent.drain(..extra);
}

fn lock(view: &Mutex<StatusView>) -> std::sync::MutexGuard<'_, StatusView> {
    view.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Copy the carry into the view that the query reads.
fn show(view: &Mutex<StatusView>, carry: &Carry, phase: &str) {
    let mut view = lock(view);
    view.tag.clone_from(&carry.tag);
    phase.clone_into(&mut view.phase);
    view.experiments = carry.done;
    view.best = Some(carry.best.clone());
    view.policy = carry.policy;
    view.hints.clone_from(&carry.hints);
    view.recent.clone_from(&carry.recent);
}

/// Publish the search attributes of the session.
fn publish(ctx: &WorkflowContext, carry: &Carry, phase: &str) -> Result<(), String> {
    ctx.upsert_search_attrs([
        ("phase".to_string(), Some(json!(phase))),
        ("experiments".to_string(), Some(json!(carry.done))),
        ("best_bpb".to_string(), Some(json!(carry.best.val_bpb))),
        (
            "best_commit".to_string(),
            Some(json!(short(&carry.best.commit))),
        ),
    ])
    .map_err(|e| e.to_string())
}

/// One idea: propose, commit, train, decide, record.
///
/// It has no execution timeout. A timeout counts wall time, a stopped
/// runner included, so a resume the next day would throw away paid work.
/// The step budget, the approval timeout and the trainer timeout bound it.
///
/// # Errors
///
/// Returns an error when a git step fails for good, or a child fails.
#[workflow]
pub async fn experiment(
    ctx: &WorkflowContext,
    input: ExperimentInput,
) -> Result<ExperimentOutcome, String> {
    let mut saga = Saga::new(ctx);
    // The first step checks out the session branch.
    let start = head(ctx, &input).await?;
    let mut labels = input.flow_labels.clone();
    let proposal = if input.baseline {
        Proposal {
            commit: start,
            description: "baseline".into(),
            base: None,
            rewound: false,
            lines: (0, 0),
        }
    } else {
        match propose(ctx, &mut saga, &input, &mut labels).await? {
            Proposed::Change(proposal) => proposal,
            Proposed::NoChange(idle) => return no_change(ctx, &input, idle, labels).await,
        }
    };
    let (measured, commit, fixes) = measure(ctx, &mut saga, &input, &proposal, &mut labels).await?;
    let verdict = decide(&input.policy, proposal.base.as_ref(), &measured);
    let crashed = verdict.status == Status::Crash;
    let row = ResultRow {
        commit: short(&commit),
        val_bpb: if crashed {
            0.0
        } else {
            measured.val_bpb.unwrap_or(0.0)
        },
        memory_gb: if crashed { 0.0 } else { measured.memory_gb },
        status: verdict.status,
        description: proposal.description,
    };
    if verdict.status != Status::Keep {
        // The saga resets each commit of this experiment, newest first.
        saga.compensate_all()
            .await
            .map_err(|e| format!("cannot reset the branch: {e}"))?;
    }
    let base = proposal.base;
    finish(
        ctx,
        &input,
        Ended {
            commit,
            row,
            verdict,
            base,
            rewound: proposal.rewound,
            fixes,
            labels,
        },
    )
    .await
}

/// A committed change, ready to train.
struct Proposal {
    commit: String,
    description: String,
    /// The base to beat. An approved `rewind` can move it. `None` only for
    /// the baseline.
    base: Option<Best>,
    rewound: bool,
    /// Lines added and removed.
    lines: (u32, u32),
}

/// A proposal that changed nothing.
struct Idle {
    base: Best,
    rewound: bool,
    answer: String,
}

/// What the agent proposed.
enum Proposed {
    /// A committed change.
    Change(Proposal),
    /// No change.
    NoChange(Idle),
}

/// Ask the agent for one change and commit it as a saga step. Adds the
/// labels of the agent run to `labels`.
async fn propose<'a>(
    ctx: &'a WorkflowContext,
    saga: &mut Saga<'a>,
    input: &ExperimentInput,
    labels: &mut BTreeSet<String>,
) -> Result<Proposed, String> {
    let old = input
        .best
        .clone()
        .ok_or("an experiment that is not the baseline needs a base")?;
    let report: AgentReport = ctx
        .spawn_child_workflow(&agent_loop_info(), proposal_task(input))
        .await
        .map_err(|e| format!("the proposal agent failed: {e}"))?;
    labels.extend(report.flow_labels.iter().cloned());
    let head = head(ctx, input).await?;
    // Only an approved `rewind` moves the branch during a proposal.
    let rewound = head != old.commit;
    let base = if rewound {
        let kept: Option<Best> = ctx
            .execute_activity(
                &find_kept_info(),
                CommitRef {
                    lab: input.lab.clone(),
                    branch: input.branch.clone(),
                    commit: head.clone(),
                },
            )
            .await
            .map_err(|e| format!("cannot read results.tsv: {e}"))?;
        kept.ok_or_else(|| format!("the branch moved to {head}, which is not a kept commit"))?
    } else {
        old
    };
    let answer = answer_line(&report);
    if report.stop != AgentStop::Completed || answer.starts_with(GIVE_UP) {
        return Ok(Proposed::NoChange(Idle {
            base,
            rewound,
            answer,
        }));
    }
    let info = commit_step(ctx, saga, input, &head, &answer).await?;
    if !info.changed {
        return Ok(Proposed::NoChange(Idle {
            base,
            rewound,
            answer,
        }));
    }
    Ok(Proposed::Change(Proposal {
        commit: info.commit,
        description: answer,
        base: Some(base),
        rewound,
        lines: (info.lines_added, info.lines_removed),
    }))
}

/// Train the proposal under the GPU mutex. A crash gets up to [`MAX_FIXES`]
/// fix proposals. Returns the measure, the last commit and the fix count.
async fn measure<'a>(
    ctx: &'a WorkflowContext,
    saga: &mut Saga<'a>,
    input: &ExperimentInput,
    proposal: &Proposal,
    labels: &mut BTreeSet<String>,
) -> Result<(Measured, String, u32), String> {
    let mut commit = proposal.commit.clone();
    let mut lines = proposal.lines;
    let mut fixes = 0;
    loop {
        let run = train_once(ctx, input, &commit).await?;
        // Upstream: a run past its time budget is a failure, even when it
        // printed a summary before the kill.
        let summary: Option<Summary> = if run.timed_out {
            None
        } else {
            ctx.execute_local_activity(&parse_summary_info(), run.log_tail.clone())
                .await
                .map_err(|e| format!("cannot parse run.log: {e}"))?
        };
        let crashed = Measured {
            val_bpb: None,
            memory_gb: 0.0,
            lines_added: lines.0,
            lines_removed: lines.1,
        };
        if let Some(summary) = summary {
            let measured = Measured {
                val_bpb: Some(summary.val_bpb),
                memory_gb: memory_gb(summary.peak_vram_mb),
                ..crashed
            };
            return Ok((measured, commit, fixes));
        }
        if input.baseline || fixes >= MAX_FIXES {
            return Ok((crashed, commit, fixes));
        }
        // Upstream: fix a dumb crash and run again.
        fixes += 1;
        let fix: AgentReport = ctx
            .spawn_child_workflow(&agent_loop_info(), fix_task(input, &run, labels))
            .await
            .map_err(|e| format!("the fix agent failed: {e}"))?;
        labels.extend(fix.flow_labels.iter().cloned());
        let answer = answer_line(&fix);
        if fix.stop != AgentStop::Completed || answer.starts_with(GIVE_UP) {
            return Ok((crashed, commit, fixes));
        }
        let info = commit_step(ctx, saga, input, &commit, &answer).await?;
        if !info.changed {
            return Ok((crashed, commit, fixes));
        }
        commit = info.commit;
        lines = (lines.0 + info.lines_added, lines.1 + info.lines_removed);
    }
}

/// Commit `train.py` on `parent` as a saga step. Its compensation resets
/// to the parent.
async fn commit_step<'a>(
    ctx: &'a WorkflowContext,
    saga: &mut Saga<'a>,
    input: &ExperimentInput,
    parent: &str,
    message: &str,
) -> Result<CommitInfo, String> {
    let (lab, branch) = (input.lab.clone(), input.branch.clone());
    let commit = CommitInput {
        lab: input.lab.clone(),
        branch: input.branch.clone(),
        parent: parent.to_string(),
        message: message.to_string(),
    };
    saga.step(
        || async move {
            ctx.execute_activity::<_, CommitInfo>(&commit_info(), commit)
                .await
        },
        move |info: CommitInfo| async move {
            if info.changed {
                let target = CommitRef {
                    lab,
                    branch,
                    commit: info.parent,
                };
                ctx.execute_activity::<_, String>(&reset_info(), target)
                    .await
                    .map(drop)
            } else {
                Ok(())
            }
        },
    )
    .await
    .map_err(|e| format!("cannot commit train.py: {e}"))
}

/// Train `commit` under the GPU mutex. An activity that fails for good is a
/// crash, with its error as the log.
async fn train_once(
    ctx: &WorkflowContext,
    input: &ExperimentInput,
    commit: &str,
) -> Result<TrainRun, String> {
    let guard = ctx
        .mutex(format!("gpu:{}", input.gpu))
        .acquire()
        .await
        .map_err(|e| format!("cannot take the GPU: {e}"))?;
    let run = ctx
        .execute_activity::<_, TrainRun>(
            &train_info(),
            TrainInput {
                lab: input.lab.clone(),
                branch: input.branch.clone(),
                trainer: input.trainer.clone(),
                commit: commit.to_string(),
            },
        )
        .await;
    guard.release();
    Ok(run.unwrap_or_else(|error| TrainRun {
        exit_code: None,
        timed_out: false,
        log_tail: format!("autoresearch: the training activity failed: {error}"),
    }))
}

/// An experiment whose agent proposed no change: reset the work tree and
/// log a discard with no run. The row holds the numbers of the base, so a
/// plot of the non-crash rows does not drop to zero.
async fn no_change(
    ctx: &WorkflowContext,
    input: &ExperimentInput,
    idle: Idle,
    labels: BTreeSet<String>,
) -> Result<ExperimentOutcome, String> {
    let row = ResultRow {
        commit: short(&idle.base.commit),
        val_bpb: idle.base.val_bpb,
        memory_gb: idle.base.memory_gb,
        status: Status::Discard,
        description: format!("no change: {}", idle.answer),
    };
    reset(ctx, input, &idle.base.commit).await?;
    let ended = Ended {
        commit: idle.base.commit.clone(),
        row,
        verdict: Verdict {
            status: Status::Discard,
            score: 0.0,
            reason: "the agent proposed no change".into(),
        },
        base: Some(idle.base),
        rewound: idle.rewound,
        fixes: 0,
        labels,
    };
    finish(ctx, input, ended).await
}

/// How an experiment ended.
struct Ended {
    /// The full id of the commit that the row names.
    commit: String,
    row: ResultRow,
    verdict: Verdict,
    base: Option<Best>,
    rewound: bool,
    fixes: u32,
    labels: BTreeSet<String>,
}

/// Record the row, publish it, and build the outcome.
async fn finish(
    ctx: &WorkflowContext,
    input: &ExperimentInput,
    ended: Ended,
) -> Result<ExperimentOutcome, String> {
    let Ended {
        commit,
        row,
        verdict,
        base,
        rewound,
        fixes,
        labels,
    } = ended;
    record(ctx, input, &row).await?;
    ctx.upsert_search_attrs([
        ("tag".to_string(), Some(json!(input.tag))),
        ("outcome".to_string(), Some(json!(row.status.as_str()))),
        ("val_bpb".to_string(), Some(json!(row.val_bpb))),
    ])
    .map_err(|e| e.to_string())?;
    let best = if verdict.status == Status::Keep {
        Some(Best {
            commit,
            val_bpb: row.val_bpb,
            memory_gb: row.memory_gb,
        })
    } else {
        base
    };
    Ok(ExperimentOutcome {
        row,
        best,
        verdict,
        rewound,
        fixes,
        flow_labels: labels,
    })
}

/// The account of a finished session for its morning report. The first
/// line holds the counts and the best result. The last rows follow.
fn summary(carry: &Carry) -> String {
    let Counts {
        kept,
        discarded,
        crashed,
    } = carry.counts;
    format!(
        "{} experiments, {kept} kept, {discarded} discarded, {crashed} crashed, \
         best val_bpb {:.6} at {}.\n\nThe last rows of the session:\n\n{}",
        carry.done,
        carry.best.val_bpb,
        short(&carry.best.commit),
        table(&carry.recent)
    )
}

/// The first line of an agent answer, cut to 200 characters.
fn answer_line(report: &AgentReport) -> String {
    report
        .text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("(no answer)")
        .chars()
        .take(200)
        .collect()
}

/// The system prompt of the researcher: `program.md`, then the context.
fn system_prompt(input: &ExperimentInput) -> String {
    let best = input.best.as_ref().map_or_else(
        || "none yet".to_string(),
        |b| format!("val_bpb {:.6} at commit {}", b.val_bpb, short(&b.commit)),
    );
    format!(
        "{}\n\n{CONTEXT_MARKER}\n{RESEARCHER_ROLE}\n\n## Session\n\n\
         Session {}, experiment {}. The base is {best}.\n\n## Recent results\n\n{}",
        input.program.trim_end(),
        input.session,
        input.number,
        table(&input.recent)
    )
}

/// The shared parts of each researcher task. `labels` are the flow labels
/// that earlier runs of the session exercised.
fn researcher_task(input: &ExperimentInput, text: String, labels: &BTreeSet<String>) -> AgentTask {
    AgentTask::new(text)
        .flow_labels(labels.iter().cloned())
        .system(system_prompt(input))
        .session(SessionId::new(input.lab.clone()))
        .memory(MemoryScope::new(input.session.clone()))
        .max_steps(input.agent.max_steps)
        .max_total_tokens(input.agent.max_total_tokens)
        .approval_timeout(Duration::from_secs(input.agent.approval_timeout_secs))
}

/// The task of the researcher agent for one experiment.
#[must_use]
pub fn proposal_task(input: &ExperimentInput) -> AgentTask {
    let mut lines = vec![format!(
        "Run experiment {} of session {}. Make one change to train.py.",
        input.number, input.session
    )];
    if let Some(hint) = &input.hint {
        lines.push(format!("{HINT_LINE}{}", hint.text));
        lines.push(format!("The hint comes from {}.", hint.from));
    }
    if input.explore {
        lines.push(EXPLORE.into());
        lines.push("Try a bolder idea than usual.".into());
    }
    researcher_task(input, lines.join("\n"), &input.flow_labels)
        .mandate(Mandate::new("researcher").peers([REVIEWER]))
}

/// The task of the researcher agent after a crash. `labels` holds the
/// labels of the proposal run too.
#[must_use]
pub fn fix_task(input: &ExperimentInput, run: &TrainRun, labels: &BTreeSet<String>) -> AgentTask {
    let how = if run.timed_out {
        "ran past its time budget"
    } else {
        "crashed"
    };
    researcher_task(
        input,
        format!(
            "{FIX_MODE}\nThe run of experiment {} {how}. The end of run.log:\n\n{}",
            input.number, run.log_tail
        ),
        labels,
    )
}

/// Swap the `program.md` part of a recorded system prompt for `program`.
/// The session context below [`CONTEXT_MARKER`] stays.
#[must_use]
pub fn with_program(system: &str, program: &str) -> String {
    match system.split_once(CONTEXT_MARKER) {
        Some((_, context)) => format!("{}\n\n{CONTEXT_MARKER}{context}", program.trim_end()),
        None => program.to_string(),
    }
}

/// The two workflows of the example, for registration.
#[must_use]
pub fn all() -> Vec<WorkflowInfo> {
    vec![research_session_info(), experiment_info()]
}
