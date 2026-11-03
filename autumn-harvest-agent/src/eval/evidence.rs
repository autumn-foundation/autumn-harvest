//! The evidence of a probe as workspace files (issue #2377).
//!
//! [`write_bundle`] writes the evidence of a probe into one workspace
//! snapshot (issue #2274). An editor reads the files. Its prompt holds
//! only the short pointer in `MISSION.md`, so no prompt cap cuts a run.
//!
//! ```text
//! task.md                  the user message of the source run
//! rollouts/source.md       the full source run
//! rollouts/<id>.md         the run of each candidate
//! diffs/<id>.md            the turn diffs of each candidate
//! overlay/current.json     the overlay that the editor changes
//! flow_labels.json         the flow labels of the source run
//! MISSION.md               a short pointer for the editor
//! ```
//!
//! # The rules
//!
//! - **Parts.** A Markdown file over the part cap splits into numbered
//!   parts, as in `rollouts/source.part-001.md`. The parts joined in order
//!   equal the single text. Nothing is cut. `formal/verus/src/evidence_parts.rs`
//!   proves that the parts tile the text.
//! - **One put.** The writer builds the bundle in memory, then saves it
//!   with one [`WorkspaceStore::save`]. A put that stops before its rename
//!   leaves no key, so no reader sees a partial bundle.
//! - **Erasure.** A source with an erasure tombstone gives
//!   [`EvidenceError::Erased`] before any decode.
//! - **Flow labels.** `flow_labels.json` holds each label that the source
//!   run recorded. [`read_bundle`] returns them. A run that reads the
//!   bundle must start with them, as a run that continues a transcript does.
//! - **Anonymous ids.** Each candidate has an [`anon`](super::anon) id. The
//!   files list the candidates in id order. Only the manifest maps an id to
//!   a candidate.
//! - **No identity in a file.** The writer masks each candidate overlay
//!   digest and each name of [`EvidenceWorkspace::hide`] in the Markdown
//!   files. It then checks every file and refuses a leak. The current
//!   overlay goes in as it is, so it can name its own model. A candidate
//!   digest in it is still a leak.
//! - **Fences.** Each recorded text sits in a fence that is longer than any
//!   backtick run in it. So a text cannot forge a heading of the bundle.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::ActivityExecId;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::anon::{self, CandidateKey};
use super::{
    Divergence, EvalError, Evaluation, RunEnd, Side, TurnDecision, Verdict, decode, task_of,
};
use crate::agent::overlay::ProfileOverlay;
use crate::error::AgentError;
use crate::memory::MemorySnapshot;
use crate::message::{ChatMessage, ContentPart};
use crate::policy::ToolDecision;
use crate::response_cache::hex;
use crate::types::{AgentReport, ModelTurn, ToolOutcome};
use crate::workflow::{agent_memory_snapshot_info, agent_model_turn_info, agent_tool_call_info};
use crate::workspace::{Workspace, WorkspaceStore};

pub use crate::refine::bundle::{
    BundleCandidate, BundleFile, BundleManifest, DIFFS_DIR, LABELS_PATH, MISSION_PATH,
    OVERLAY_PATH, PART_INFIX, ROLLOUTS_DIR, SOURCE_STEM, TASK_PATH, TASK_STEM,
};

/// The default part cap: 256 KiB.
pub const DEFAULT_PART_BYTES: usize = 256 * 1024;

/// The smallest part cap. One UTF-8 character fits in it.
pub const MIN_PART_BYTES: usize = 4;

/// The text that takes the place of a hidden name.
pub const MASK: &str = "[hidden]";

/// Why a bundle cannot be written or read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EvidenceError {
    /// The source holds an erasure tombstone. No bundle forks an erased run.
    #[error("the source run was erased, so it gives no evidence")]
    Erased,
    /// The source is not an agent run.
    #[error("the source history is not an agent run: {0}")]
    NotAnAgentRun(String),
    /// A recorded payload does not decode.
    #[error("a payload does not decode: {0}")]
    Undecodable(String),
    /// A file still holds a hidden name after the mask. The writer wrote
    /// nothing.
    #[error("the file {path} names a hidden candidate identity")]
    Leak {
        /// The path of the file.
        path: String,
    },
    /// The workspace store failed, or the bundle is over a store cap.
    #[error("the workspace store failed: {0}")]
    Workspace(#[source] AgentError),
    /// The snapshot is not an evidence bundle.
    #[error("the snapshot is not an evidence bundle: {0}")]
    NotABundle(String),
}

impl From<EvalError> for EvidenceError {
    fn from(error: EvalError) -> Self {
        match error {
            EvalError::ErasedSource => Self::Erased,
            EvalError::NotAnAgentRun(why) => Self::NotAnAgentRun(why),
            other => Self::Undecodable(other.to_string()),
        }
    }
}

/// Where a bundle goes, and how it is written.
#[derive(Debug, Clone)]
pub struct EvidenceWorkspace {
    store: WorkspaceStore,
    scope: String,
    generation: u64,
    seed: u64,
    part_bytes: usize,
    hidden: BTreeSet<String>,
}

impl EvidenceWorkspace {
    /// A bundle in `scope` of `store`, at generation 0, with seed 0 and the
    /// part cap [`DEFAULT_PART_BYTES`].
    #[must_use]
    pub fn new(store: WorkspaceStore, scope: impl Into<String>) -> Self {
        Self {
            store,
            scope: scope.into(),
            generation: 0,
            seed: 0,
            part_bytes: DEFAULT_PART_BYTES,
            hidden: BTreeSet::new(),
        }
    }

    /// Save the snapshot at `generation`. See [`WorkspaceStore::save`].
    #[must_use]
    pub const fn generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }

    /// Give the candidates the ids of `seed`. The same seed gives the same
    /// ids and the same files.
    #[must_use]
    pub const fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Split a Markdown file over `bytes` into parts. A cap below
    /// [`MIN_PART_BYTES`] gives that minimum.
    #[must_use]
    pub fn part_bytes(mut self, bytes: usize) -> Self {
        self.part_bytes = bytes.max(MIN_PART_BYTES);
        self
    }

    /// Keep `name` out of every file, for example a model name or an
    /// overlay version id. An empty name hides nothing.
    #[must_use]
    pub fn hide(mut self, name: impl Into<String>) -> Self {
        self.hidden.insert(name.into());
        self
    }
}

/// A bundle as [`read_bundle`] loads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bundle {
    /// The files of the snapshot.
    pub files: Workspace,
    /// The flow labels of the source run.
    pub flow_labels: BTreeSet<String>,
}

/// Write the evidence of a probe into one workspace snapshot.
///
/// `source` is the history of the source run. `evaluations` are the
/// candidates against it, as [`evaluate`](super::evaluate) returns them.
/// `overlay` is the overlay that the editor changes.
///
/// The writer saves nothing when it fails.
///
/// # Errors
///
/// - [`EvidenceError::Erased`] for an erased source.
/// - [`EvidenceError::NotAnAgentRun`] or [`EvidenceError::Undecodable`] for
///   a source that does not decode.
/// - [`EvidenceError::Leak`] when a file still names a hidden identity.
/// - [`EvidenceError::Workspace`] when the store fails or the bundle is
///   over a store cap.
pub async fn write_bundle(
    workspace: &EvidenceWorkspace,
    source: &[WorkflowEvent],
    evaluations: &[Evaluation],
    overlay: &ProfileOverlay,
) -> Result<BundleManifest, EvidenceError> {
    let task = task_of(source)?;
    let source_text = render_source(source)?;
    let flow_labels = source_labels(source)?;

    let ids = anon::assign(
        workspace.seed,
        &source_run_id(source),
        &candidate_keys(evaluations),
    );
    let mut order: Vec<usize> = (0..evaluations.len()).collect();
    order.sort_by(|a, b| ids[*a].cmp(&ids[*b]));
    let candidates: Vec<BundleCandidate> = order
        .iter()
        .map(|&index| BundleCandidate {
            id: ids[index].to_string(),
            index,
        })
        .collect();

    let digests: Vec<&str> = evaluations
        .iter()
        .map(|evaluation| evaluation.overlay_digest.as_str())
        .collect();
    let needles: Vec<&str> = digests
        .iter()
        .copied()
        .chain(workspace.hidden.iter().map(String::as_str))
        .collect();

    let mut texts = vec![
        (TASK_STEM.to_owned(), task.input),
        (SOURCE_STEM.to_owned(), source_text),
    ];
    for candidate in &candidates {
        let evaluation = &evaluations[candidate.index];
        let id = &candidate.id;
        texts.push((
            format!("{ROLLOUTS_DIR}/{id}"),
            render_rollout(id, evaluation),
        ));
        texts.push((format!("{DIFFS_DIR}/{id}"), render_diff(id, evaluation)));
    }

    let mut files: BTreeMap<String, String> = BTreeMap::new();
    for (stem, text) in &texts {
        let masked = mask(text, &needles);
        let parts = split_parts(&masked, workspace.part_bytes);
        for (path, part) in part_paths(stem, parts.len().max(1)).into_iter().zip(
            // An empty text still gives one empty file.
            if parts.is_empty() { vec![""] } else { parts },
        ) {
            check_leak(&path, part, &needles)?;
            files.insert(path, part.to_owned());
        }
    }
    let overlay_text = json_text(overlay)?;
    // The editor needs the real overlay, so only a digest counts here.
    check_leak(OVERLAY_PATH, &overlay_text, &digests)?;
    files.insert(OVERLAY_PATH.to_owned(), overlay_text);
    files.insert(LABELS_PATH.to_owned(), json_text(&flow_labels)?);

    let mut listed: Vec<BundleFile> = files
        .iter()
        .map(|(path, text)| BundleFile {
            path: path.clone(),
            bytes: text.len() as u64,
        })
        .collect();
    let mission = crate::refine::bundle::mission_text(&listed, &candidates);
    check_leak(MISSION_PATH, &mission, &needles)?;
    listed.push(BundleFile {
        path: MISSION_PATH.to_owned(),
        bytes: mission.len() as u64,
    });
    listed.sort_by(|a, b| a.path.cmp(&b.path));
    files.insert(MISSION_PATH.to_owned(), mission);

    let mut snapshot = Workspace::default();
    for (path, text) in files {
        snapshot
            .write(&path, text.into_bytes())
            .map_err(EvidenceError::Workspace)?;
    }
    let head = workspace
        .store
        .save(&workspace.scope, workspace.generation, &snapshot)
        .await
        .map_err(EvidenceError::Workspace)?;
    Ok(BundleManifest {
        scope: workspace.scope.clone(),
        head,
        files: listed,
        candidates,
        flow_labels,
    })
}

/// Load the bundle that `manifest` names.
///
/// # Errors
///
/// Returns [`EvidenceError::Workspace`] when the store cannot load the
/// snapshot, and [`EvidenceError::NotABundle`] when it has no readable
/// `flow_labels.json`.
pub async fn read_bundle(
    store: &WorkspaceStore,
    manifest: &BundleManifest,
) -> Result<Bundle, EvidenceError> {
    let files = store
        .load(&manifest.mount())
        .await
        .map_err(EvidenceError::Workspace)?;
    let labels = files
        .read_text(LABELS_PATH)
        .ok_or_else(|| EvidenceError::NotABundle(format!("it has no {LABELS_PATH}")))?;
    let flow_labels = serde_json::from_str(labels)
        .map_err(|e| EvidenceError::NotABundle(format!("{LABELS_PATH} does not decode: {e}")))?;
    Ok(Bundle { files, flow_labels })
}

/// The flow labels that the source run recorded.
///
/// The set holds the task labels and the labels of each model request and
/// tool call. It also holds the labels of each model turn, labelled memory
/// snapshot, child run and report. A label that a run tree exercised shows
/// in at least one of them.
///
/// # Errors
///
/// As [`render_source`].
pub fn source_labels(history: &[WorkflowEvent]) -> Result<BTreeSet<String>, EvidenceError> {
    let task = task_of(history)?;
    let mut labels = task.flow_labels;
    let snapshot = agent_memory_snapshot_info().name;
    let mut scheduled: HashMap<ActivityExecId, &str> = HashMap::new();
    for event in history {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name,
                input,
                ..
            } => {
                scheduled.insert(*activity_id, name.as_str());
                add_strings(&mut labels, input.get("flow_labels"));
                add_strings(&mut labels, input.get("labels"));
            }
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output,
            } => {
                if scheduled.get(activity_id) == Some(&snapshot) {
                    let decoded: MemorySnapshot =
                        decode(output.clone()).map_err(EvidenceError::Undecodable)?;
                    if let MemorySnapshot::Labelled { labels: more, .. } = decoded {
                        labels.extend(more);
                    }
                } else {
                    // A model turn holds `labels`. A report holds
                    // `flow_labels`. Another output holds neither.
                    add_strings(&mut labels, output.get("labels"));
                    add_strings(&mut labels, output.get("flow_labels"));
                }
            }
            WorkflowEvent::ChildWorkflowStarted { input, .. } => {
                add_strings(&mut labels, input.get("flow_labels"));
            }
            WorkflowEvent::ChildWorkflowCompleted { output, .. }
            | WorkflowEvent::WorkflowCompleted { output } => {
                add_strings(&mut labels, output.get("flow_labels"));
            }
            _ => {}
        }
    }
    Ok(labels)
}

/// Add each string of `value` to `labels`. `value` is a list of strings,
/// or a list of such lists.
fn add_strings(labels: &mut BTreeSet<String>, value: Option<&Value>) {
    let Some(Value::Array(items)) = value else {
        return;
    };
    for item in items {
        match item {
            Value::String(label) => {
                labels.insert(label.clone());
            }
            Value::Array(inner) => {
                labels.extend(inner.iter().filter_map(Value::as_str).map(str::to_owned));
            }
            _ => {}
        }
    }
}

/// The run id of the source: the run id of its first model request. A run
/// with no model request gives the digest of its first event.
fn source_run_id(history: &[WorkflowEvent]) -> String {
    let model = agent_model_turn_info().name;
    history
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, input, .. } if name == model => input
                .get("run_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            _ => None,
        })
        .unwrap_or_else(|| {
            let first = history
                .first()
                .and_then(|event| serde_json::to_vec(event).ok());
            hex(&Sha256::digest(first.unwrap_or_default()))
        })
}

/// The anonymous-id key of each evaluation.
///
/// The key is the overlay digest. Two evaluations with one digest take
/// their occurrence in the order of the SHA-256 of their JSON. So the input
/// order does not change an id.
fn candidate_keys(evaluations: &[Evaluation]) -> Vec<CandidateKey<'_>> {
    let sums: Vec<String> = evaluations
        .iter()
        .map(|evaluation| {
            hex(&Sha256::digest(
                serde_json::to_vec(evaluation).unwrap_or_default(),
            ))
        })
        .collect();
    (0..evaluations.len())
        .map(|at| {
            let digest = evaluations[at].overlay_digest.as_str();
            let before = (0..evaluations.len())
                .filter(|&other| {
                    evaluations[other].overlay_digest == digest
                        && (sums[other].as_str(), other) < (sums[at].as_str(), at)
                })
                .count();
            CandidateKey {
                digest,
                occurrence: u32::try_from(before).unwrap_or(u32::MAX),
            }
        })
        .collect()
}

/// Refuse `text` at `path` when it holds a needle.
fn check_leak(path: &str, text: &str, needles: &[&str]) -> Result<(), EvidenceError> {
    if anon::leaks(text, needles).is_empty() {
        Ok(())
    } else {
        Err(EvidenceError::Leak {
            path: path.to_owned(),
        })
    }
}

/// `value` as pretty JSON with a final newline.
fn json_text<T: Serialize>(value: &T) -> Result<String, EvidenceError> {
    serde_json::to_string_pretty(value)
        .map(|text| text + "\n")
        .map_err(|e| EvidenceError::Undecodable(e.to_string()))
}

/// The source run as one text.
///
/// The text holds the task and each model turn with its calls and
/// decisions. It also holds each tool result, each other activity, signal
/// and child run, and the end. It cuts nothing. [`split_parts`] of it gives the parts of
/// `rollouts/source.md`.
///
/// # Errors
///
/// Returns [`EvidenceError::Erased`] for an erased history,
/// [`EvidenceError::NotAnAgentRun`] for a history that is not an agent run,
/// and [`EvidenceError::Undecodable`] for a payload that does not decode.
pub fn render_source(history: &[WorkflowEvent]) -> Result<String, EvidenceError> {
    let task = task_of(history)?;
    let mut out = String::from("# Source run\n\n## Task\n\n### System\n\n");
    push_text(&mut out, task.system.as_deref().unwrap_or_default());
    if !task.history.is_empty() {
        out.push_str("### Earlier messages\n\n");
        for message in &task.history {
            push_message(&mut out, message);
        }
    }
    out.push_str("### User\n\n");
    push_text(&mut out, &task.input);

    let mut scheduled: HashMap<ActivityExecId, (&str, &Value)> = HashMap::new();
    let mut turn = 0_usize;
    for event in history {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name,
                input,
                ..
            } => {
                scheduled.insert(*activity_id, (name.as_str(), input));
            }
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output,
            } => push_completed(&mut out, scheduled.get(activity_id), output, &mut turn)?,
            WorkflowEvent::ActivityFailed {
                activity_id, error, ..
            } => {
                let name = scheduled
                    .get(activity_id)
                    .map_or("unknown", |(name, _)| name);
                let _ = write!(out, "### Activity `{name}` failed\n\n");
                push_text(&mut out, error);
            }
            WorkflowEvent::ActivityTimedOut {
                activity_id,
                timeout_type,
            } => {
                let name = scheduled
                    .get(activity_id)
                    .map_or("unknown", |(name, _)| name);
                let _ = write!(
                    out,
                    "### Activity `{name}` timed out ({timeout_type:?})\n\n"
                );
            }
            WorkflowEvent::SignalReceived {
                signal_name,
                payload,
            } => {
                out.push_str("### Signal\n\n");
                push_text(&mut out, signal_name);
                push_json(&mut out, payload);
            }
            WorkflowEvent::ChildWorkflowStarted { input, .. } => {
                out.push_str("### Child run started\n\n");
                push_json(&mut out, input);
            }
            WorkflowEvent::ChildWorkflowCompleted { output, .. } => {
                out.push_str("### Child run completed\n\n");
                push_json(&mut out, output);
            }
            WorkflowEvent::ChildWorkflowFailed { error, .. } => {
                out.push_str("### Child run failed\n\n");
                push_text(&mut out, error);
            }
            WorkflowEvent::WorkflowRedriven { .. } => out.push_str("## Redrive\n\n"),
            WorkflowEvent::WorkflowCompleted { output } => {
                let report: AgentReport =
                    decode(output.clone()).map_err(EvidenceError::Undecodable)?;
                out.push_str("## End\n\n");
                push_end(&mut out, &RunEnd::Completed(report));
            }
            WorkflowEvent::WorkflowFailed { error, .. } => {
                out.push_str("## End\n\n");
                push_end(&mut out, &RunEnd::Failed(error.clone()));
            }
            WorkflowEvent::WorkflowCancelled { reason } => {
                out.push_str("## End\n\nThe run was cancelled.\n\n");
                push_text(&mut out, reason);
            }
            WorkflowEvent::WorkflowExecutionTimedOut { .. } => {
                out.push_str("## End\n\nThe run timed out.\n\n");
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Write the result of one completed activity of the source run. A model
/// turn takes the next turn number.
///
/// # Errors
///
/// Returns [`EvidenceError::Undecodable`] for a model turn or a tool
/// outcome that does not decode.
fn push_completed(
    out: &mut String,
    scheduled: Option<&(&str, &Value)>,
    output: &Value,
    turn: &mut usize,
) -> Result<(), EvidenceError> {
    let model = agent_model_turn_info().name;
    let tool = agent_tool_call_info().name;
    match scheduled {
        Some((name, _)) if *name == model => {
            let recorded: ModelTurn = decode(output.clone()).map_err(EvidenceError::Undecodable)?;
            let _ = write!(out, "## Turn {turn}\n\n");
            push_decision(out, &TurnDecision::of(&recorded));
            *turn += 1;
        }
        Some((name, input)) if *name == tool => {
            let outcome: ToolOutcome =
                decode(output.clone()).map_err(EvidenceError::Undecodable)?;
            let tool_name = input
                .pointer("/call/name")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let flag = if outcome.is_error { " (error)" } else { "" };
            let _ = write!(out, "### Tool result: `{tool_name}`{flag}\n\n");
            push_text(out, &outcome.content);
        }
        Some((name, _)) => {
            let _ = write!(out, "### Activity `{name}`\n\n");
            push_json(out, output);
        }
        None => {
            out.push_str("### An activity with no schedule\n\n");
            push_json(out, output);
        }
    }
    Ok(())
}

/// The run of one candidate: each turn and the end.
fn render_rollout(id: &str, evaluation: &Evaluation) -> String {
    let mut out = format!("# Candidate {id}\n\n");
    let _ = write!(
        out,
        "Recorded tool outputs: {}. Stubbed tool calls: {}. The turn cap stopped the run: {}.\n\n",
        evaluation.replayed_tool_calls,
        evaluation.stubbed_tool_calls,
        yes_no(evaluation.turn_cap_reached),
    );
    for diff in &evaluation.turns {
        if let Some(decision) = &diff.candidate {
            let _ = write!(out, "## Turn {}\n\n", diff.turn);
            push_decision(&mut out, decision);
        }
    }
    out.push_str("## End\n\n");
    push_end(&mut out, &evaluation.candidate);
    out
}

/// The turn diffs of one candidate against the source.
fn render_diff(id: &str, evaluation: &Evaluation) -> String {
    let mut out = format!("# Diff of {id} against the source\n\n");
    let first = evaluation
        .first_divergence
        .map_or_else(|| "none".to_owned(), |turn| turn.to_string());
    let _ = write!(
        out,
        "First divergent turn: {first}. The run ends differ: {}. Stubbed tool calls: {}. \
         The turn cap stopped the run: {}.\n\n",
        yes_no(evaluation.end_diverged),
        evaluation.stubbed_tool_calls,
        yes_no(evaluation.turn_cap_reached),
    );
    for diff in &evaluation.turns {
        let _ = write!(
            out,
            "## Turn {}: {}\n\n",
            diff.turn,
            verdict_text(diff.verdict)
        );
        for (side, decision) in [("Source", &diff.recorded), (id, &diff.candidate)] {
            let _ = write!(out, "### {side}\n\n");
            match decision {
                Some(decision) => push_decision(&mut out, decision),
                None => out.push_str("No turn.\n\n"),
            }
        }
    }
    out.push_str("## End\n\n### Source\n\n");
    push_end(&mut out, &evaluation.recorded);
    let _ = write!(out, "### {id}\n\n");
    push_end(&mut out, &evaluation.candidate);
    out
}

/// The text of a verdict.
fn verdict_text(verdict: Verdict) -> String {
    match verdict {
        Verdict::Same => "same".to_owned(),
        Verdict::Reworded => "reworded".to_owned(),
        Verdict::Diverged(divergence) => {
            let kind = match divergence {
                Divergence::Shape => "an answer against tool calls",
                Divergence::Calls => "other tool calls",
                Divergence::Policy => "another policy decision",
                Divergence::Stop => "another stop",
                Divergence::Missing(Side::Recorded) => "no source turn",
                Divergence::Missing(Side::Candidate) => "no candidate turn",
            };
            format!("diverged, {kind}")
        }
    }
}

const fn yes_no(flag: bool) -> &'static str {
    if flag { "yes" } else { "no" }
}

/// Write one turn decision: the text, each call and its decision, and the
/// stop reason.
fn push_decision(out: &mut String, decision: &TurnDecision) {
    if decision.dropped {
        out.push_str("The reply was over the result cap, so the worker dropped it.\n\n");
    }
    if !decision.text.is_empty() {
        push_text(out, &decision.text);
    }
    for (position, call) in decision.calls.iter().enumerate() {
        let _ = write!(out, "Call {position}: `{}`\n\n", call.name);
        push_json(out, &call.arguments);
        match &call.decision {
            ToolDecision::Allow => out.push_str("Decision: allow.\n\n"),
            ToolDecision::RequireApproval { reason } => {
                out.push_str("Decision: ask a person.\n\n");
                push_text(out, reason);
            }
            ToolDecision::Deny { reason } => {
                out.push_str("Decision: deny.\n\n");
                push_text(out, reason);
            }
        }
    }
    let _ = write!(out, "Stop: `{}`.\n\n", name_of(&decision.stop));
}

/// Write how a run ended.
fn push_end(out: &mut String, end: &RunEnd) {
    match end {
        RunEnd::Completed(report) => {
            let _ = write!(
                out,
                "The run completed. Stop: `{}`. Steps: {}. Tool calls: {}.\n\n",
                name_of(&report.stop),
                report.steps_used,
                report.tool_calls,
            );
            push_text(out, &report.text);
            if let Some(error) = &report.error {
                out.push_str("Error:\n\n");
                push_text(out, error);
            }
        }
        RunEnd::Failed(error) => {
            out.push_str("The run failed.\n\n");
            push_text(out, error);
        }
    }
}

/// Write one earlier message of the task.
fn push_message(out: &mut String, message: &ChatMessage) {
    let _ = write!(out, "#### {}\n\n", name_of(&message.role));
    for part in &message.content {
        match part {
            ContentPart::Text(text) => push_text(out, text),
            ContentPart::ToolCall {
                name, arguments, ..
            } => {
                let _ = write!(out, "Call: `{name}`\n\n");
                push_json(out, arguments);
            }
            ContentPart::ToolResult { content, .. } => {
                out.push_str("Tool result:\n\n");
                push_text(out, content);
            }
        }
    }
}

/// The serde name of a unit variant, such as `end_turn`.
fn name_of<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(name)) => name,
        Ok(other) => other.to_string(),
        Err(_) => "unknown".to_owned(),
    }
}

/// Write `text` in a fence.
fn push_text(out: &mut String, text: &str) {
    out.push_str(&fenced(text));
    out.push('\n');
}

/// Write `value` as pretty JSON in a fence.
fn push_json(out: &mut String, value: &Value) {
    let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    push_text(out, &text);
}

/// `text` in a Markdown fence that is longer than any backtick run in it.
/// So `text` cannot close the fence.
fn fenced(text: &str) -> String {
    let mut longest = 0_usize;
    let mut run = 0_usize;
    for c in text.chars() {
        run = if c == '`' { run + 1 } else { 0 };
        longest = longest.max(run);
    }
    let fence = "`".repeat(longest.max(2) + 1);
    format!("{fence}\n{text}\n{fence}\n")
}

/// `text` with each needle replaced by [`MASK`]. The longest needle goes
/// first, so a name that holds a shorter name is masked whole.
fn mask(text: &str, needles: &[&str]) -> String {
    let mut sorted: Vec<&str> = needles.iter().copied().filter(|n| !n.is_empty()).collect();
    sorted.sort_by(|a, b| b.len().cmp(&a.len()).then(a.cmp(b)));
    sorted
        .into_iter()
        .fold(text.to_owned(), |text, needle| text.replace(needle, MASK))
}

/// `text` in parts of at most `cap` bytes. A cap below [`MIN_PART_BYTES`]
/// gives that minimum. An empty text has no parts.
///
/// A part ends after the last newline in its window, else on the last
/// UTF-8 boundary in it. The parts joined in order equal `text`.
#[must_use]
pub fn split_parts(text: &str, cap: usize) -> Vec<&str> {
    let cap = cap.max(MIN_PART_BYTES);
    let mut parts = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let end = part_end(start, text.len(), cap, cut_at(text, start, cap));
        // `cut_at` gives a UTF-8 boundary, so `get` always succeeds. A miss
        // keeps the rest whole rather than cut it.
        if let Some(part) = text.get(start..end) {
            parts.push(part);
            start = end;
        } else {
            parts.push(text.get(start..).unwrap_or_default());
            break;
        }
    }
    parts
}

/// The best end of a part that starts at `start`: after the last newline
/// in the window, else the last UTF-8 boundary in it.
fn cut_at(text: &str, start: usize, cap: usize) -> usize {
    let limit = start.saturating_add(cap).min(text.len());
    let window = text.as_bytes().get(start..limit).unwrap_or_default();
    if let Some(newline) = window.iter().rposition(|&b| b == b'\n') {
        return start + newline + 1;
    }
    (start + 1..=limit)
        .rev()
        .find(|&at| text.is_char_boundary(at))
        .unwrap_or(limit)
}

/// The end of the part that starts at `start`, in a text of `len` bytes,
/// with the cap `cap` and the best cut `cut`.
///
/// The rest of the text is one part when it fits. Else a cut inside the
/// window wins. Else the part ends at the cap. With `start < len` and
/// `cap > 0`, the result is above `start`, at most `start + cap` and at
/// most `len`. `formal/verus/src/evidence_parts.rs` proves it.
pub(crate) const fn part_end(start: usize, len: usize, cap: usize, cut: usize) -> usize {
    if len - start <= cap {
        len
    } else if start < cut && cut <= start + cap {
        cut
    } else {
        start + cap
    }
}

/// The paths of a file with `stem` in `count` parts.
///
/// One part is `<stem>.md`. More parts are `<stem>.part-001.md` and on,
/// with at least three digits, so the paths sort in part order.
fn part_paths(stem: &str, count: usize) -> Vec<String> {
    if count <= 1 {
        return vec![format!("{stem}.md")];
    }
    let width = count.to_string().len().max(3);
    (1..=count)
        .map(|n| format!("{stem}{PART_INFIX}{n:0width$}.md"))
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::sync::Arc;

    use autumn_harvest::payload_codec::PayloadCodecs;
    use autumn_harvest::types::ActivityExecId;
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::workflow::agent_model_turn_info;
    use crate::workspace::sync_log::{self, Kind};
    use crate::workspace::{DirWorkspaceBlobs, WorkspaceBlobs};

    /// A small completed run with one model turn that answers `text`.
    fn history(text: &str) -> Vec<WorkflowEvent> {
        let turn = ActivityExecId::new();
        vec![
            WorkflowEvent::WorkflowStarted {
                input: serde_json::to_value(crate::AgentTask::new("hello")).unwrap(),
                timestamp: chrono::Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: turn,
                name: agent_model_turn_info().name.to_owned(),
                input: json!({"run_id": "run-1", "flow_labels": ["seen"]}),
                queue: "default".to_owned(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id: turn,
                output: json!({
                    "content": [{"text": text}],
                    "stop": "end_turn",
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                    "decisions": [],
                }),
            },
            WorkflowEvent::WorkflowFailed {
                error: "stopped".to_owned(),
                error_type: None,
                details: None,
                non_retryable: None,
            },
        ]
    }

    #[test]
    fn a_short_text_is_one_part_and_an_empty_text_is_none() {
        assert_eq!(split_parts("abc", 8), vec!["abc"]);
        assert_eq!(split_parts("", 8), Vec::<&str>::new());
    }

    #[test]
    fn a_part_ends_after_a_newline_when_the_window_holds_one() {
        let parts = split_parts("aa\nbb\ncc\n", 7);
        assert_eq!(parts, vec!["aa\nbb\n", "cc\n"]);
    }

    #[test]
    fn a_part_never_splits_a_character() {
        // Each character is 3 bytes, so a 4-byte cap holds one.
        let parts = split_parts("ééé€€", MIN_PART_BYTES);
        assert_eq!(parts.concat(), "ééé€€");
        assert!(parts.iter().all(|part| part.len() <= MIN_PART_BYTES));
    }

    #[test]
    fn a_cap_below_the_minimum_still_ends() {
        let parts = split_parts("€€", 0);
        assert_eq!(parts, vec!["€", "€"]);
    }

    #[test]
    fn part_paths_sort_in_part_order() {
        assert_eq!(part_paths("rollouts/source", 1), vec!["rollouts/source.md"]);
        let many = part_paths("diffs/c-1", 1_200);
        assert_eq!(many[0], "diffs/c-1.part-0001.md");
        assert_eq!(many[1_199], "diffs/c-1.part-1200.md");
        let mut sorted = many.clone();
        sorted.sort();
        assert_eq!(sorted, many);
        assert_eq!(part_paths("t", 2), vec!["t.part-001.md", "t.part-002.md"]);
    }

    #[test]
    fn a_fence_is_longer_than_any_backtick_run_inside() {
        let text = "x\n````\n## Turn 9\n";
        let block = fenced(text);
        assert!(block.starts_with("`````\n"), "{block}");
        assert!(block.ends_with("\n`````\n"), "{block}");
        assert!(block.contains(text));
        assert_eq!(fenced("plain"), "```\nplain\n```\n");
    }

    #[test]
    fn a_mask_hides_each_name_and_the_longest_name_first() {
        let masked = mask("fast-model and fast", &["fast", "fast-model"]);
        assert_eq!(masked, format!("{MASK} and {MASK}"));
        assert_eq!(mask("keep", &[""]), "keep");
    }

    #[test]
    fn a_source_render_holds_the_task_the_turns_and_the_end() {
        let text = render_source(&history("All ```` done")).unwrap();
        assert!(text.contains("hello"), "{text}");
        assert!(text.contains("All ```` done"), "{text}");
        assert!(text.contains("stopped"), "{text}");
    }

    #[test]
    fn the_labels_come_from_each_recorded_request() {
        let labels = source_labels(&history("hi")).unwrap();
        assert!(labels.contains("seen"), "{labels:?}");
    }

    #[test]
    fn a_history_that_is_not_an_agent_run_is_refused() {
        let result = render_source(&[]);
        assert!(
            matches!(result, Err(EvidenceError::NotAnAgentRun(_))),
            "{result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stopped_put_leaves_no_bundle() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Arc::new(DirWorkspaceBlobs::new(dir.path()));
        let store = WorkspaceStore::new(blobs.clone(), PayloadCodecs::default());
        let workspace = EvidenceWorkspace::new(store.clone(), "evidence:stop").part_bytes(16);
        let source = history("a long answer\nin many lines\nof text\n");

        // The put writes its scratch file, then stops before the rename.
        sync_log::fail(Kind::File, dir.path());
        let stopped = write_bundle(&workspace, &source, &[], &ProfileOverlay::default()).await;
        sync_log::heal(dir.path());

        assert!(
            matches!(stopped, Err(EvidenceError::Workspace(_))),
            "{stopped:?}"
        );
        assert!(
            blobs.list("evidence:stop").await.unwrap().is_empty(),
            "no key and no scratch file is visible"
        );

        // The same bundle then lands whole, as one key.
        let manifest = write_bundle(&workspace, &source, &[], &ProfileOverlay::default())
            .await
            .unwrap();
        assert_eq!(
            blobs.list("evidence:stop").await.unwrap(),
            vec![manifest.head.key()]
        );
        let bundle = read_bundle(&store, &manifest).await.unwrap();
        assert_eq!(bundle.files.files().len(), manifest.files.len());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_crash_after_the_scratch_write_shows_no_head() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = Arc::new(DirWorkspaceBlobs::new(dir.path()));
        let store = WorkspaceStore::new(blobs.clone(), PayloadCodecs::default());
        let workspace = EvidenceWorkspace::new(store.clone(), "evidence:crash");
        let source = history("done");
        let manifest = write_bundle(&workspace, &source, &[], &ProfileOverlay::default())
            .await
            .unwrap();
        // A crash before the rename leaves the scratch file and no key.
        let key = manifest.head.key();
        let bytes = blobs.get("evidence:crash", &key).await.unwrap().unwrap();
        assert!(blobs.delete("evidence:crash", &key).await.unwrap());
        let folder = std::fs::read_dir(dir.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let scratch = folder.path().join(format!("{key}.tmp-{}", "a".repeat(32)));
        std::fs::write(&scratch, bytes).unwrap();

        let read = read_bundle(&store, &manifest).await;

        assert!(matches!(read, Err(EvidenceError::Workspace(_))), "{read:?}");
        let names = blobs.list("evidence:crash").await.unwrap();
        assert_eq!(names.len(), 1, "{names:?}");
        assert_ne!(names[0], key, "only the scratch name is listed");
    }

    proptest! {
        #[test]
        fn the_parts_join_to_the_text_and_fit_the_cap(
            text in "(\\PC|\n){0,400}",
            cap in 0_usize..64,
        ) {
            let parts = split_parts(&text, cap);
            prop_assert_eq!(parts.concat(), text.clone());
            let cap = cap.max(MIN_PART_BYTES);
            for (at, part) in parts.iter().enumerate() {
                prop_assert!(!part.is_empty());
                prop_assert!(part.len() <= cap);
                // A part that is not the last ends after a newline when its
                // window holds one.
                let window_end = parts[..=at].iter().map(|p| p.len()).sum::<usize>();
                let start = window_end - part.len();
                if at + 1 < parts.len() {
                    let limit = (start + cap).min(text.len());
                    let window = &text.as_bytes()[start..limit];
                    if window.contains(&b'\n') {
                        prop_assert!(part.ends_with('\n'), "{:?}", part);
                    }
                }
            }
        }
    }
}
