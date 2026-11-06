//! The `agent_refine` workflow (issue #2380).
//!
//! A refine run takes recorded runs of one kind. It runs Propose, Probe and
//! Compose on each of them, with a fixed budget. It ends with a
//! [`RefineReport`]. It never runs on a live op. The design is in
//! `docs/design/DESIGN-2380.md`.
//!
//! 1. `agent_refine_policy` reads the policy of the kind once (issue
//!    #2394).
//! 2. [`agent_refine_start`] checks every source run before any model
//!    call. It reads the base profile, the active overlay, and the profile
//!    and the evidence text of each source. Each stage of a source checks
//!    its replies against the source profile, as its probe does.
//! 3. Each source runs in input order. The overlay that a source chooses
//!    is the current overlay of the next source.
//! 4. The run checks its budget before each step. A spent budget ends it as
//!    [`RefineStatus::BudgetExhausted`], with no chosen overlay.
//! 5. A run that ran its whole plan checks its counts with
//!    [`RefineLedger::check`].
//! 6. A run that chose an overlay runs the adoption gate of
//!    [`gate`](super::gate). A dry run ends before it.
//!
//! Each stage, probe and selection is one recorded activity. A crash
//! re-runs only the step in flight.

use std::collections::BTreeSet;
use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::types::ExecutionId;
use serde::{Deserialize, Serialize};

use super::adopt::RefineStatus;
use super::compose::{
    ComposeRequest, Composition, ProbedCandidate, SelectRequest, SelectedBy, Selection,
    agent_refine_compose_info, agent_refine_select_info, source_events,
};
use super::gate::{Gate, GateRecord, set_failed, set_status};
use super::ledger::{MAX_REFINE_K, MAX_SOURCE_RUNS, RefineError, RefineLedger, RefineMode};
use super::policy::{PolicyCeilings, RefinePolicy};
use super::policy_read::read_refine_policy;
use super::propose::{
    Dimension, Evidence, ProposalAttempts, ProposeRequest, SpentDetails, Stage, StageOutcome,
    agent_refine_propose_info,
};
use super::sources::{LiveEnd, LiveProbeError, LiveRequest};
use crate::agent::overlay::{Ceilings, ProfileOverlay};
use crate::agent::overlay_store::{OverlayVersion, StoredOverlay};
use crate::agent::{AGENT_WORKFLOW_NAME, AgentId, AgentProfile};
use crate::eval::anon::AnonId;
use crate::eval::evidence::render_source;
use crate::eval::{Candidate, Evaluation, RunEnd, check_source, evaluate_routed, task_of};
use crate::harness::{AgentHarness, truncate};
use crate::mesh_control::{MESH_ATTR, MeshBudget};
use crate::message::TokenUsage;
use crate::tool::ToolEffect;
use crate::workflow::WORKFLOW_NAME;

/// The registered workflow name of a refine run.
pub const REFINE_WORKFLOW_NAME: &str = "agent_refine";

/// The most characters of a stage error or a note in the report.
const MAX_NOTE_CHARS: usize = 500;

/// The error types of a stage failure that no retry or fallback can fix.
/// The run fails on them. Any other stage failure falls back.
const FATAL_STAGE_ERRORS: &[&str] = &[
    "OverlayProposerMissing",
    "OverlayComposerMissing",
    "CurrentOverlayRefused",
    "RefineSourcesMissing",
    "RefineSourceNotFound",
    "AgentHarnessMissing",
    "MultiThreadRuntimeRequired",
    "Config",
    "Authentication",
    "Budget",
];

/// The input of [`agent_refine`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefineInput {
    /// The agent kind to refine.
    pub kind: String,
    /// The recorded runs of the kind: 1 to [`MAX_SOURCE_RUNS`], each once.
    pub source_runs: Vec<ExecutionId>,
    /// The width of each round: 1 to [`MAX_REFINE_K`].
    pub k: u32,
    /// How the run moves through its steps.
    pub mode: RefineMode,
    /// The seed of the opaque ids and of each stage.
    pub seed: u64,
    /// The cap on the spend of the run. `None` sets no cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<MeshBudget>,
    /// End after the loop with the chosen overlay. A dry run stores no
    /// version and opens no gate.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
}

impl RefineInput {
    /// A run of `kind` over `source_runs`, with no budget.
    #[must_use]
    pub fn new(
        kind: impl Into<String>,
        source_runs: Vec<ExecutionId>,
        k: u32,
        mode: RefineMode,
        seed: u64,
    ) -> Self {
        Self {
            kind: kind.into(),
            source_runs,
            k,
            mode,
            seed,
            budget: None,
            dry_run: false,
        }
    }

    /// Make the run a dry run. It ends with the chosen overlay, stores no
    /// version and opens no gate.
    #[must_use]
    pub const fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Cap the spend of the run with `budget`.
    ///
    /// The run compares its own recorded tokens and cost with the caps.
    /// The period does not apply. The run also sets its search attribute
    /// `agent_mesh` to the mesh of the budget, so the spend of the mesh
    /// holds its cost ledger rows.
    #[must_use]
    pub fn budget(mut self, budget: MeshBudget) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Check the bounds of the input.
    ///
    /// # Errors
    ///
    /// Returns [`RefineError::InvalidInput`] with the reason.
    pub fn validate(&self) -> Result<(), RefineError> {
        let invalid = |why: String| Err(RefineError::InvalidInput(why));
        if !crate::mesh::valid_name(&self.kind) {
            return invalid(format!(
                "the kind {:?} is not a valid agent name",
                truncate(&self.kind, crate::mesh::MAX_NAME_LEN)
            ));
        }
        if !(1..=MAX_REFINE_K).contains(&self.k) {
            return invalid(format!(
                "k must be from 1 to {MAX_REFINE_K}, not {}",
                self.k
            ));
        }
        if !(1..=MAX_SOURCE_RUNS).contains(&self.source_runs.len()) {
            return invalid(format!(
                "a run takes 1 to {MAX_SOURCE_RUNS} source runs, not {}",
                self.source_runs.len()
            ));
        }
        let unique: BTreeSet<String> = self.source_runs.iter().map(ToString::to_string).collect();
        if unique.len() != self.source_runs.len() {
            return invalid("a source run is named twice".to_owned());
        }
        Ok(())
    }
}

/// One source run, as the start step read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceBrief {
    /// The source run.
    pub run: ExecutionId,
    /// The source run as text, for the proposers.
    pub evidence: String,
    /// The profile of the recorded task. A probe applies each overlay to
    /// it, so each stage of the source checks its replies against it too.
    pub profile: AgentProfile,
}

/// The input of [`agent_refine_start`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartRequest {
    /// The input of the run.
    pub input: RefineInput,
    /// The ceilings of the recorded policy read.
    pub ceilings: PolicyCeilings,
}

/// The output of [`agent_refine_start`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefineStart {
    /// The base profile of the kind.
    pub profile: AgentProfile,
    /// The active overlay of the kind, or the empty overlay.
    pub current: ProfileOverlay,
    /// Each source run, in input order.
    pub sources: Vec<SourceBrief>,
    /// `true` when the worker has an `OverlaySelector`. With none, a select
    /// step asks no model, so the budget gate skips it.
    #[serde(default)]
    pub selector: bool,
    /// The policy ceilings, resolved against the base profile.
    pub ceilings: Ceilings,
    /// The version that was active at the start. An approval of the gate
    /// expects it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<OverlayVersion>,
    /// `true` when the step read [`Self::active`]. A record of an older
    /// release has no such field. Its run then ends with the chosen overlay
    /// and opens no gate, as that release did.
    #[serde(default)]
    pub active_read: bool,
}

/// The part of the loop that a stage plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StageRole {
    /// An INV proposer stage.
    Inv,
    /// A DOM proposer stage.
    Dom,
    /// A composer stage.
    Compose,
}

impl From<Dimension> for StageRole {
    fn from(dimension: Dimension) -> Self {
        match dimension {
            Dimension::Inv => Self::Inv,
            Dimension::Dom => Self::Dom,
        }
    }
}

/// The live re-probe of one probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "live", rename_all = "snake_case")]
#[non_exhaustive]
pub enum LiveReprobe {
    /// The replay probe stubbed no tool call.
    NotNeeded,
    /// The backend has no fork, so the re-probe did not run.
    Skipped {
        /// Why.
        reason: String,
    },
    /// The fork or its wait failed.
    Failed {
        /// The error.
        error: String,
    },
    /// The re-probe ran on a fork.
    Ran(LiveEnd),
}

/// The input of [`agent_refine_probe`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeRequest {
    /// The source run.
    pub source: ExecutionId,
    /// The overlay to probe.
    pub overlay: ProfileOverlay,
    /// The ceilings of the run.
    pub ceilings: Ceilings,
    /// The tool effects that a live re-probe may run.
    pub effects: BTreeSet<ToolEffect>,
}

/// The output of [`agent_refine_probe`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbeResult {
    /// The replay probe against the recorded source.
    pub evaluation: Evaluation,
    /// The live re-probe.
    pub live: LiveReprobe,
    /// Each named model route that the replay probe asked for, such as the
    /// route of the source task or of a recorded peer. A pool redacts each
    /// one. A result recorded before this field decodes with none.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub routes: BTreeSet<String>,
}

/// One probe in the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeRecord {
    /// The digest of the overlay.
    pub overlay_digest: String,
    /// The first divergent turn.
    pub first_divergence: Option<usize>,
    /// Whether the run ends differ.
    pub end_diverged: bool,
    /// Whether the candidate run completed.
    pub completed: bool,
    /// The candidate tool calls that got a recorded outcome.
    pub replayed_tool_calls: u32,
    /// The candidate tool calls that got the stub outcome.
    pub stubbed_tool_calls: u32,
    /// Whether the turn cap stopped the candidate run.
    pub turn_cap_reached: bool,
    /// The live re-probe.
    pub live: LiveReprobe,
    /// The tokens of the candidate turns.
    pub usage: TokenUsage,
    /// The cost of the candidate turns.
    pub cost_usd_micros: u64,
}

impl ProbeRecord {
    fn of(result: &ProbeResult) -> Self {
        let evaluation = &result.evaluation;
        Self {
            overlay_digest: evaluation.overlay_digest.clone(),
            first_divergence: evaluation.first_divergence,
            end_diverged: evaluation.end_diverged,
            completed: matches!(evaluation.candidate, RunEnd::Completed(_)),
            replayed_tool_calls: evaluation.replayed_tool_calls,
            stubbed_tool_calls: evaluation.stubbed_tool_calls,
            turn_cap_reached: evaluation.turn_cap_reached,
            live: result.live.clone(),
            usage: evaluation.candidate_usage,
            cost_usd_micros: evaluation.candidate_cost_usd_micros,
        }
    }
}

/// One stage in the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageRecord {
    /// The source run of the stage.
    pub source: ExecutionId,
    /// The step, from 1. A parallel round is step 1.
    pub step: u32,
    /// The part of the loop.
    pub role: StageRole,
    /// The sample number, from 1.
    pub sample: u32,
    /// The editor calls of the stage.
    pub attempts: u32,
    /// The result.
    pub outcome: StageOutcome,
    /// The tokens that the editor charged.
    pub usage: TokenUsage,
    /// The cost that the editor charged.
    pub cost_usd_micros: u64,
    /// The probe of a candidate stage. A `KeptBase` or a `FellBack` stage
    /// has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<ProbeRecord>,
    /// What the run could not do for the stage.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

/// One selection in the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectionRecord {
    /// The source run of the selection.
    pub source: ExecutionId,
    /// The index of the chosen stage in [`RefineReport::stages`].
    pub stage: Option<usize>,
    /// The opaque id of the pick.
    pub id: Option<AnonId>,
    /// Who made the pick.
    pub by: SelectedBy,
    /// Why the rule replaced the pick of the installed selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// The output of [`agent_refine`].
///
/// It holds no id of the refine run and no time, so one seed and one
/// script give byte-identical reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefineReport {
    /// The refined kind.
    pub kind: String,
    /// The mode of the run.
    pub mode: RefineMode,
    /// The width of each round.
    pub k: u32,
    /// The seed of the run.
    pub seed: u64,
    /// The source runs, in input order.
    pub source_runs: Vec<ExecutionId>,
    /// How the run ended. The loop gives [`RefineStatus::Unchanged`] or
    /// [`RefineStatus::BudgetExhausted`]. A dry run gives
    /// [`RefineStatus::Chosen`]. The gate gives each other status.
    pub status: RefineStatus,
    /// The chosen overlay. `None` when the run changed nothing or spent its
    /// budget.
    pub chosen: Option<ProfileOverlay>,
    /// Each stage, in run order.
    pub stages: Vec<StageRecord>,
    /// Each selection, in run order.
    pub selections: Vec<SelectionRecord>,
    /// The counts and the spend.
    pub ledger: RefineLedger,
    /// The adoption gate. `None` when the run chose no overlay, when the
    /// store found no change, when the pointer moved before the gate, and
    /// for a dry run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<GateRecord>,
}

/// A failure payload of `err` with its name.
fn failure(err: &RefineError) -> String {
    ActivityFailure::non_retryable(err.code(), err.to_string())
        .with_details(serde_json::to_value(err).unwrap_or_default())
        .into_error_payload()
}

/// The kind of the source `record`, or `None` when it names none.
fn kind_of(record: &super::sources::SourceRecord) -> Result<Option<String>, String> {
    if record.workflow_name == AGENT_WORKFLOW_NAME {
        return Ok(AgentId::parse(&record.workflow_id)
            .ok()
            .map(|id| id.kind().to_owned()));
    }
    task_of(&record.events)
        .map(|task| task.kind)
        .map_err(|err| err.to_string())
}

/// The active version of `kind`, or `None` with no store.
async fn active_of(harness: &AgentHarness, kind: &str) -> Result<Option<StoredOverlay>, String> {
    match harness.overlays() {
        None => Ok(None),
        Some(store) => store
            .active(kind)
            .await
            .map_err(|err| format!("the overlay store failed: {err}")),
    }
}

/// Check every source of `input`, then read the base profile, the active
/// overlay and the evidence of each source.
///
/// # Errors
///
/// Returns the failure payload of a [`RefineError`], or a retryable error
/// when a read fails.
pub(crate) async fn run_start(
    harness: &AgentHarness,
    request: StartRequest,
) -> Result<RefineStart, String> {
    let StartRequest { input, ceilings } = request;
    input.validate().map_err(|err| failure(&err))?;
    let sources = harness.sources().ok_or_else(|| {
        ActivityFailure::non_retryable(
            super::compose::SOURCES_MISSING,
            "no RefineSources is installed: pass one to AgentHarness::refine_sources",
        )
        .into_error_payload()
    })?;
    let mut briefs = Vec::with_capacity(input.source_runs.len());
    for &run in &input.source_runs {
        let name = run.to_string();
        let record = sources
            .source(run)
            .await
            .map_err(|err| format!("the source read failed: {err}"))?
            .ok_or_else(|| failure(&RefineError::SourceNotFound { run: name.clone() }))?;
        if record.workflow_name != WORKFLOW_NAME && record.workflow_name != AGENT_WORKFLOW_NAME {
            return Err(failure(&RefineError::SourceNotAgentRun {
                run: name,
                workflow: truncate(&record.workflow_name, crate::mesh::MAX_NAME_LEN),
            }));
        }
        if !record.ended {
            return Err(failure(&RefineError::SourceInFlight { run: name }));
        }
        let found = kind_of(&record).map_err(|reason| {
            failure(&RefineError::SourceNotReplayable {
                run: name.clone(),
                reason,
            })
        })?;
        if found.as_deref() != Some(input.kind.as_str()) {
            return Err(failure(&RefineError::SourceOtherKind {
                run: name,
                kind: input.kind.clone(),
                found: found.map(|kind| truncate(&kind, crate::mesh::MAX_NAME_LEN)),
            }));
        }
        if record.workflow_name == AGENT_WORKFLOW_NAME {
            return Err(failure(&RefineError::SourceNotReplayable {
                run: name,
                reason: "the replay probe reads agent_loop runs only, not a durable agent run"
                    .to_owned(),
            }));
        }
        let not_replayable = |reason: String| {
            failure(&RefineError::SourceNotReplayable {
                run: name.clone(),
                reason,
            })
        };
        check_source(&record.events).map_err(|err| not_replayable(err.to_string()))?;
        let evidence =
            render_source(&record.events).map_err(|err| not_replayable(err.to_string()))?;
        let task = task_of(&record.events).map_err(|err| not_replayable(err.to_string()))?;
        let mut source_profile = crate::eval::source_profile(&task);
        source_profile.kind.clone_from(&input.kind);
        briefs.push(SourceBrief {
            run,
            evidence,
            profile: source_profile,
        });
    }
    let profile = harness.agent_profile(&input.kind).ok_or_else(|| {
        failure(&RefineError::InvalidInput(format!(
            "the harness has no profile of the kind {:?}",
            input.kind
        )))
    })?;
    let ceilings = ceilings.resolve(&profile);
    let active = active_of(harness, &input.kind).await?;
    let current = active
        .as_ref()
        .map(|stored| stored.overlay().clone())
        .unwrap_or_default();
    // Each stage and probe of a source starts from `current` on the source
    // profile. A source that the ceilings refuse would fail its first stage
    // after earlier sources spent, so the start step refuses it now.
    for brief in &briefs {
        if let Err(err) = crate::agent::overlay::apply(&brief.profile, &current, &ceilings, harness)
        {
            return Err(failure(&RefineError::SourceOverCeilings {
                run: brief.run.to_string(),
                reason: truncate(&err.to_string(), MAX_NOTE_CHARS),
            }));
        }
    }
    Ok(RefineStart {
        profile,
        current,
        sources: briefs,
        selector: harness.overlay_selector().is_some(),
        ceilings,
        active: active.as_ref().map(StoredOverlay::version),
        active_read: true,
    })
}

/// The start step of a refine run, as an activity. See [`run_start`].
///
/// # Errors
///
/// Returns a named [`RefineError`] payload for a bad input or source.
#[activity(
    start_to_close = "10m",
    retry = RetryPolicy::exponential(3, Duration::from_secs(1))
)]
pub async fn agent_refine_start(
    ctx: &ActivityContext,
    request: StartRequest,
) -> Result<RefineStart, String> {
    run_start(crate::workflow::harness(ctx)?, request).await
}

/// Probe `request.overlay` against its recorded source, and re-probe it
/// live when the replay stubbed a tool call.
///
/// # Errors
///
/// Returns a non-retryable payload when the evaluation fails, and the
/// errors of the source read.
pub(crate) async fn run_probe(
    harness: &AgentHarness,
    request: ProbeRequest,
) -> Result<ProbeResult, String> {
    let events = source_events(harness, request.source).await?;
    let candidate = Candidate::new(harness.clone())
        .overlay(request.overlay.clone())
        .ceilings(request.ceilings.clone());
    let (evaluation, routes) = Box::pin(evaluate_routed(&events, &candidate))
        .await
        .map_err(|err| {
            ActivityFailure::non_retryable("RefineProbeFailed", format!("the probe failed: {err}"))
                .into_error_payload()
        })?;
    let live = if evaluation.stubbed_tool_calls == 0 {
        LiveReprobe::NotNeeded
    } else {
        let sources = harness.sources().ok_or_else(|| {
            ActivityFailure::non_retryable(
                super::compose::SOURCES_MISSING,
                "no RefineSources is installed: pass one to AgentHarness::refine_sources",
            )
            .into_error_payload()
        })?;
        let ran = sources
            .live_probe(LiveRequest {
                harness,
                source: request.source,
                overlay: &request.overlay,
                effects: &request.effects,
                ceilings: &request.ceilings,
            })
            .await;
        match ran {
            Ok(end) => LiveReprobe::Ran(end),
            Err(LiveProbeError::Unsupported(backend)) => LiveReprobe::Skipped {
                reason: format!("live re-probe skipped: {backend} has no fork"),
            },
            Err(err) => LiveReprobe::Failed {
                error: truncate(&err.to_string(), MAX_NOTE_CHARS),
            },
        }
    };
    Ok(ProbeResult {
        evaluation,
        live,
        routes,
    })
}

/// One probe, as an activity. See [`run_probe`].
///
/// # Errors
///
/// The errors of [`run_probe`].
#[activity(
    start_to_close = "2h",
    retry = RetryPolicy::exponential(5, Duration::from_secs(5))
)]
pub async fn agent_refine_probe(
    ctx: &ActivityContext,
    request: ProbeRequest,
) -> Result<ProbeResult, String> {
    let result = run_probe(crate::workflow::harness(ctx)?, request).await?;
    super::propose::record_calls(ctx, probe_calls(&result));
    Ok(result)
}

/// The cost ledger call of one probe. The probe records one row for the
/// sum of its candidate turns, under the unknown model id.
pub(crate) fn probe_calls(result: &ProbeResult) -> Vec<autumn_harvest::llm_ledger::LlmCall> {
    let evaluation = &result.evaluation;
    if evaluation.candidate_usage == TokenUsage::default() {
        return Vec::new();
    }
    let mut call = autumn_harvest::llm_ledger::LlmCall::new(
        crate::model::UNKNOWN_MODEL_ID,
        u64::from(evaluation.candidate_usage.input_tokens),
        u64::from(evaluation.candidate_usage.output_tokens),
    );
    if evaluation.candidate_cost_usd_micros > 0 {
        call = call.with_cost_usd_micros(evaluation.candidate_cost_usd_micros);
    }
    vec![call]
}

/// The state of one refine run.
struct Refine<'a> {
    ctx: &'a WorkflowContext,
    input: &'a RefineInput,
    /// Every source of the run, in input order.
    sources: &'a [SourceBrief],
    /// The registered profile of the kind.
    kind_profile: &'a AgentProfile,
    ceilings: Ceilings,
    attempts: ProposalAttempts,
    effects: BTreeSet<ToolEffect>,
    current: ProfileOverlay,
    ledger: RefineLedger,
    stages: Vec<StageRecord>,
    /// The probe of each stage, by stage index. Not in the report.
    probes: Vec<Option<ProbedCandidate>>,
    selections: Vec<SelectionRecord>,
    exhausted: bool,
    /// Whether the worker has a selector.
    selector: bool,
}

/// Whether a stage that failed with `err` falls back to the current
/// overlay. Only a failed or timed-out activity falls back, and only with a
/// kind that a fallback can fix. Any other engine error fails the run, such
/// as a cancel or an input over the cap.
fn falls_back(err: &HarvestError) -> bool {
    match err {
        HarvestError::ActivityFailed { error_type, .. } => {
            !FATAL_STAGE_ERRORS.contains(&error_type.as_str())
        }
        HarvestError::Timeout { .. } => true,
        _ => false,
    }
}

/// The text of `err` with no attempt number. A replay and a live run then
/// give the same text.
fn error_text(err: &HarvestError) -> String {
    let text = match err {
        HarvestError::ActivityFailed {
            error_type, source, ..
        } => format!("{error_type}: {source}"),
        HarvestError::Timeout { .. } => "the activity timed out".to_owned(),
        other => other.to_string(),
    };
    truncate(&text, MAX_NOTE_CHARS)
}

/// The spend that the last attempt of a failed step reports.
fn spent_of(err: &HarvestError) -> SpentDetails {
    err.activity_details()
        .and_then(|details| serde_json::from_value(details.clone()).ok())
        .unwrap_or_default()
}

/// Whether `spend` is above a cap of `budget`. A spend at the cap is not
/// above it.
fn over(budget: &MeshBudget, spend: &crate::mesh_control::Spend) -> bool {
    budget.max_tokens.is_some_and(|max| spend.tokens > max)
        || budget
            .max_cost_usd_micros
            .is_some_and(|max| spend.cost_usd_micros > max)
}

/// The run failure of a step that failed with `err`.
fn step_failure(what: &str, err: &HarvestError) -> String {
    failure(&RefineError::Stage(truncate(
        &format!("{what}: {}", error_text(err)),
        MAX_NOTE_CHARS,
    )))
}

impl Refine<'_> {
    /// Whether the budget is spent. A spent budget ends the run.
    fn spent(&mut self) -> bool {
        if self
            .input
            .budget
            .as_ref()
            .is_some_and(|budget| budget.is_spent(&self.ledger.spent()))
        {
            self.exhausted = true;
        }
        self.exhausted
    }

    /// Record one stage. Returns its index.
    fn record(&mut self, mut record: StageRecord) -> usize {
        self.ledger.stage(&record.outcome);
        self.ledger.spend(&record.usage, record.cost_usd_micros);
        if let StageOutcome::FellBack { error } = &mut record.outcome {
            *error = truncate(error, MAX_NOTE_CHARS);
        }
        self.stages.push(record);
        self.probes.push(None);
        self.stages.len() - 1
    }

    /// Run one proposer stage of `dimension` from `base`.
    async fn propose(
        &mut self,
        source: &SourceBrief,
        step: u32,
        dimension: Dimension,
        sample: u32,
        base: &ProfileOverlay,
    ) -> Result<usize, String> {
        let request = ProposeRequest {
            dimension,
            evidence: Evidence::text(source.evidence.clone()),
            profile: source.profile.clone(),
            current: base.clone(),
            ceilings: self.ceilings.clone(),
            attempts: self.attempts,
            sample,
            others: self.others(source),
        };
        let stage: Stage = match self
            .ctx
            .execute_activity(&agent_refine_propose_info(), request)
            .await
        {
            Ok(stage) => stage,
            Err(err) if !falls_back(&err) => {
                return Err(step_failure("a proposer stage failed", &err));
            }
            Err(err) => {
                let spent = spent_of(&err);
                Stage {
                    dimension,
                    attempts: 0,
                    outcome: StageOutcome::FellBack {
                        error: error_text(&err),
                    },
                    usage: spent.usage,
                    cost_usd_micros: spent.cost_usd_micros,
                }
            }
        };
        Ok(self.record(StageRecord {
            source: source.run,
            step,
            role: dimension.into(),
            sample,
            attempts: stage.attempts,
            outcome: stage.outcome,
            usage: stage.usage,
            cost_usd_micros: stage.cost_usd_micros,
            probe: None,
            notes: Vec::new(),
        }))
    }

    /// Run one compose stage from `base` over the probes of `stages`.
    async fn compose(
        &mut self,
        source: &SourceBrief,
        step: u32,
        sample: u32,
        base: &ProfileOverlay,
        stages: &[usize],
    ) -> Result<usize, String> {
        let request = ComposeRequest {
            source: source.run,
            profile: source.profile.clone(),
            current: base.clone(),
            ceilings: self.ceilings.clone(),
            attempts: self.attempts,
            sample,
            seed: self.input.seed,
            probes: self.probed(stages),
            others: self.others(source),
        };
        let composition: Composition = match self
            .ctx
            .execute_activity(&agent_refine_compose_info(), request)
            .await
        {
            Ok(composition) => composition,
            Err(err) if !falls_back(&err) => {
                return Err(step_failure("a composer stage failed", &err));
            }
            Err(err) => {
                let spent = spent_of(&err);
                Composition {
                    attempts: 0,
                    outcome: StageOutcome::FellBack {
                        error: error_text(&err),
                    },
                    usage: spent.usage,
                    cost_usd_micros: spent.cost_usd_micros,
                    notes: Vec::new(),
                }
            }
        };
        Ok(self.record(StageRecord {
            source: source.run,
            step,
            role: StageRole::Compose,
            sample,
            attempts: composition.attempts,
            outcome: composition.outcome,
            usage: composition.usage,
            cost_usd_micros: composition.cost_usd_micros,
            probe: None,
            notes: composition.notes,
        }))
    }

    /// The profiles of each source other than `source`, and the registered
    /// profile of the kind. A stage reply must apply to each of them. So the
    /// run can carry it to any source, and the kind can take the chosen
    /// overlay.
    fn others(&self, source: &SourceBrief) -> Vec<AgentProfile> {
        self.sources
            .iter()
            .filter(|other| other.run != source.run)
            .map(|other| other.profile.clone())
            .chain(std::iter::once(self.kind_profile.clone()))
            .collect()
    }

    /// The probed candidates of `stages`, in that order.
    fn probed(&self, stages: &[usize]) -> Vec<ProbedCandidate> {
        stages
            .iter()
            .filter_map(|&at| self.probes.get(at).cloned().flatten())
            .collect()
    }

    /// Probe each candidate of `stages`. Returns the probed stages. Returns
    /// `None` when the budget ends the run first.
    async fn probe_all(
        &mut self,
        source: &SourceBrief,
        stages: &[usize],
    ) -> Result<Option<Vec<usize>>, String> {
        let mut probed = Vec::new();
        for &at in stages {
            let StageOutcome::Proposed(overlay) = &self.stages[at].outcome else {
                continue;
            };
            let overlay = overlay.clone();
            if self.spent() {
                return Ok(None);
            }
            let request = ProbeRequest {
                source: source.run,
                overlay: overlay.clone(),
                ceilings: self.ceilings.clone(),
                effects: self.effects.clone(),
            };
            let result: ProbeResult = self
                .ctx
                .execute_activity(&agent_refine_probe_info(), request)
                .await
                .map_err(|err| step_failure("a probe failed", &err))?;
            self.ledger.probe();
            let record = ProbeRecord::of(&result);
            self.ledger.spend(&record.usage, record.cost_usd_micros);
            match &record.live {
                LiveReprobe::NotNeeded => {}
                LiveReprobe::Ran(end) => {
                    self.ledger.live_reprobes = self.ledger.live_reprobes.saturating_add(1);
                    self.ledger.spend(&end.usage, end.cost_usd_micros);
                }
                LiveReprobe::Skipped { reason } => self.stages[at].notes.push(reason.clone()),
                LiveReprobe::Failed { error } => self.stages[at].notes.push(error.clone()),
            }
            self.stages[at].probe = Some(record);
            self.probes[at] = Some(ProbedCandidate {
                overlay,
                evaluation: result.evaluation,
                routes: result.routes,
            });
            probed.push(at);
        }
        Ok(Some(probed))
    }

    /// Select one of the probed compositions `stages`. Returns the chosen
    /// stage.
    async fn select(
        &mut self,
        source: &SourceBrief,
        stages: &[usize],
    ) -> Result<Option<usize>, String> {
        let request = SelectRequest {
            source: source.run,
            seed: self.input.seed,
            probes: self.probed(stages),
        };
        let selection: Selection = match self
            .ctx
            .execute_activity(&agent_refine_select_info(), request)
            .await
        {
            Ok(selection) => selection,
            Err(err) if falls_back(&err) => self.rule_of(stages, &err),
            Err(err) => return Err(step_failure("a selection failed", &err)),
        };
        self.ledger.selections = self.ledger.selections.saturating_add(1);
        self.ledger
            .spend(&selection.usage, selection.cost_usd_micros);
        let stage = selection.index.and_then(|index| stages.get(index).copied());
        self.selections.push(SelectionRecord {
            source: source.run,
            stage,
            id: selection.id,
            by: selection.by,
            note: selection.note,
        });
        Ok(stage)
    }

    /// The pick of a select step that failed with `err`: a completed probe
    /// first, then the fewest stubbed tool calls, then the first stage.
    fn rule_of(&self, stages: &[usize], err: &HarvestError) -> Selection {
        let index = stages
            .iter()
            .enumerate()
            .filter_map(|(index, &at)| Some((index, self.stages.get(at)?.probe.as_ref()?)))
            .min_by_key(|(index, probe)| (!probe.completed, probe.stubbed_tool_calls, *index))
            .map(|(index, _)| index);
        let spent = spent_of(err);
        Selection {
            index,
            id: None,
            by: SelectedBy::Rule,
            usage: spent.usage,
            cost_usd_micros: spent.cost_usd_micros,
            note: Some(truncate(
                &format!(
                    "the select step failed, so the rule picked: {}",
                    error_text(err)
                ),
                MAX_NOTE_CHARS,
            )),
        }
    }

    /// The overlay of the candidate stage `at`.
    fn overlay_of(&self, at: usize) -> Option<ProfileOverlay> {
        match &self.stages.get(at)?.outcome {
            StageOutcome::Proposed(overlay) => Some(overlay.clone()),
            _ => None,
        }
    }

    /// One parallel round on `source`. Returns `false` when the budget
    /// ends the run.
    async fn parallel(&mut self, source: &SourceBrief) -> Result<bool, String> {
        let base = self.current.clone();
        let k = self.input.k;
        let mut proposals = Vec::new();
        for dimension in [Dimension::Inv, Dimension::Dom] {
            for sample in 1..=k {
                if self.spent() {
                    return Ok(false);
                }
                proposals.push(self.propose(source, 1, dimension, sample, &base).await?);
            }
        }
        let Some(probed) = self.probe_all(source, &proposals).await? else {
            return Ok(false);
        };
        let mut compositions = Vec::new();
        for sample in 1..=k {
            if self.spent() {
                return Ok(false);
            }
            compositions.push(self.compose(source, 1, sample, &base, &probed).await?);
        }
        let Some(composed) = self.probe_all(source, &compositions).await? else {
            return Ok(false);
        };
        if composed.is_empty() {
            return Ok(true);
        }
        // A rule pick asks no model, so only a selector needs budget.
        if self.selector && self.spent() {
            return Ok(false);
        }
        if let Some(chosen) = self.select(source, &composed).await?
            && let Some(overlay) = self.overlay_of(chosen)
        {
            self.current = overlay;
        }
        Ok(true)
    }

    /// k sequential steps on `source`. Returns `false` when the budget
    /// ends the run.
    async fn sequential(&mut self, source: &SourceBrief) -> Result<bool, String> {
        for step in 1..=self.input.k {
            let base = self.current.clone();
            let mut proposals = Vec::new();
            for dimension in [Dimension::Inv, Dimension::Dom] {
                if self.spent() {
                    return Ok(false);
                }
                proposals.push(self.propose(source, step, dimension, step, &base).await?);
            }
            let Some(probed) = self.probe_all(source, &proposals).await? else {
                return Ok(false);
            };
            if self.spent() {
                return Ok(false);
            }
            let composition = self.compose(source, step, step, &base, &probed).await?;
            if self.probe_all(source, &[composition]).await?.is_none() {
                return Ok(false);
            }
            if let Some(overlay) = self.overlay_of(composition) {
                self.current = overlay;
            }
        }
        Ok(true)
    }
}

/// One refine run. See the [module docs](self).
///
/// # Errors
///
/// Returns a named [`RefineError`] payload. A bad input or source fails
/// before any model call. A stage can fail for good. A ledger can break its
/// identity.
#[workflow]
pub async fn agent_refine(
    ctx: &WorkflowContext,
    input: RefineInput,
) -> Result<RefineReport, String> {
    input.validate().map_err(|err| failure(&err))?;
    let policy: RefinePolicy = read_refine_policy(ctx, &input.kind)
        .await
        .map_err(|err| err.to_string())?;
    let request = StartRequest {
        input: input.clone(),
        ceilings: policy.ceilings.clone(),
    };
    let start: RefineStart = ctx
        .execute_activity(&agent_refine_start_info(), request)
        .await
        .map_err(|err| start_failure(&err))?;
    if let Some(budget) = &input.budget
        && let Err(err) =
            ctx.upsert_search_attrs([(MESH_ATTR.to_owned(), Some(serde_json::json!(budget.mesh)))])
    {
        tracing::warn!(error = %err, "the refine mesh attribute was not set");
    }
    let mut run = Refine {
        ctx,
        input: &input,
        sources: &start.sources,
        kind_profile: &start.profile,
        ceilings: start.ceilings.clone(),
        attempts: policy.proposal_attempts,
        effects: policy.probe_effects.clone(),
        current: start.current.clone(),
        ledger: RefineLedger::default(),
        stages: Vec::new(),
        probes: Vec::new(),
        selections: Vec::new(),
        exhausted: false,
        selector: start.selector,
    };
    for source in &start.sources {
        let more = match input.mode {
            RefineMode::Parallel => run.parallel(source).await?,
            RefineMode::Sequential => run.sequential(source).await?,
        };
        if !more {
            break;
        }
    }
    // The last step can spend past the cap, so check once more. A spend at
    // the cap after the whole plan passes. Only a spend above it fails.
    let exhausted = run.exhausted
        || input
            .budget
            .as_ref()
            .is_some_and(|budget| over(budget, &run.ledger.spent()));
    let (status, chosen) = if exhausted {
        (RefineStatus::BudgetExhausted, None)
    } else {
        let sources = u32::try_from(start.sources.len()).unwrap_or(u32::MAX);
        run.ledger
            .check(input.k, sources)
            .map_err(|err| failure(&err))?;
        if run.current == start.current {
            (RefineStatus::Unchanged, None)
        } else {
            (RefineStatus::Chosen, Some(run.current.clone()))
        }
    };
    let (status, gate) = match &chosen {
        Some(overlay) if status == RefineStatus::Chosen && !input.dry_run && start.active_read => {
            let gate = Gate {
                ctx,
                kind: &input.kind,
                chosen: overlay,
                expected: start.active,
                policy: &policy,
                ceilings: &start.ceilings,
            };
            match gate.run().await {
                Ok(ended) => ended,
                Err(err) => {
                    set_failed(ctx, &input.kind);
                    return Err(err);
                }
            }
        }
        _ => (status, None),
    };
    set_status(ctx, &input.kind, status);
    Ok(RefineReport {
        kind: input.kind.clone(),
        mode: input.mode,
        k: input.k,
        seed: input.seed,
        source_runs: input.source_runs.clone(),
        status,
        chosen,
        stages: run.stages,
        selections: run.selections,
        ledger: run.ledger,
        gate,
    })
}

/// The run failure of a start step that failed with `err`. A named
/// refine error keeps its name.
fn start_failure(err: &HarvestError) -> String {
    match err {
        HarvestError::ActivityFailed {
            error_type,
            details,
            source,
            ..
        } if error_type.starts_with("Refine") => {
            let mut failure =
                ActivityFailure::non_retryable(error_type.clone(), source.to_string());
            if let Some(details) = details {
                failure = failure.with_details(details.clone());
            }
            failure.into_error_payload()
        }
        other => step_failure("the start step failed", other),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn input(k: u32, runs: usize) -> RefineInput {
        RefineInput::new(
            "support",
            (0..runs).map(|_| ExecutionId::new()).collect(),
            k,
            RefineMode::Parallel,
            1,
        )
    }

    #[test]
    fn the_input_bounds_are_checked() {
        assert_eq!(input(1, 1).validate(), Ok(()));
        assert_eq!(input(MAX_REFINE_K, MAX_SOURCE_RUNS).validate(), Ok(()));
        for bad in [
            input(0, 1),
            input(MAX_REFINE_K + 1, 1),
            input(1, 0),
            input(1, MAX_SOURCE_RUNS + 1),
        ] {
            let err = bad.validate().unwrap_err();
            assert_eq!(err.code(), "RefineInvalidInput", "{err}");
        }
        let mut twice = input(1, 1);
        twice.source_runs.push(twice.source_runs[0]);
        assert!(twice.validate().is_err());
        let mut kind = input(1, 1);
        kind.kind = "Not A Kind".to_owned();
        assert!(kind.validate().is_err());
    }

    #[test]
    fn a_start_record_of_an_older_release_opens_no_gate() {
        let start = RefineStart {
            profile: AgentProfile::new("support"),
            current: ProfileOverlay::default(),
            sources: Vec::new(),
            selector: false,
            ceilings: Ceilings::from_profile(&AgentProfile::new("support")),
            active: None,
            active_read: true,
        };
        let mut old = serde_json::to_value(&start).unwrap();
        old.as_object_mut().unwrap().remove("active_read");
        let read: RefineStart = serde_json::from_value(old).unwrap();
        assert!(!read.active_read);
        let again: RefineStart =
            serde_json::from_value(serde_json::to_value(&start).unwrap()).unwrap();
        assert!(again.active_read);
    }

    #[test]
    fn an_input_with_an_unknown_key_does_not_decode() {
        let mut value = serde_json::to_value(input(1, 1)).unwrap();
        value["probe_effects"] = serde_json::json!(["write"]);
        assert!(serde_json::from_value::<RefineInput>(value).is_err());
    }

    #[test]
    fn a_failure_payload_carries_the_name() {
        let payload = failure(&RefineError::SourceInFlight {
            run: "r".to_owned(),
        });
        let parsed = autumn_harvest::failure::parse_typed_payload(&payload).unwrap();
        assert_eq!(parsed.error_type, "RefineSourceInFlight");
        assert!(parsed.non_retryable);
    }
}
