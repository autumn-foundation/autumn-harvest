//! The compose and select steps of a refine run (issue #2380).
//!
//! A composer builds one overlay from the probes of a round. It reads
//! each probed candidate under an opaque id, with the anonymised pool of
//! issue #2376. With a workspace store on the harness, it also reads an
//! evidence bundle of issue #2377. Its stage runs the rules of a proposer
//! stage: bounded attempts, `KeptBase` and `FellBack`.
//!
//! A selector picks one composition of the pool by its id. With no
//! selector, the run picks by the probe results and asks no model. See
//! [`Selection`].

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::llm_ledger::LlmCall;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::types::ExecutionId;
use serde::{Deserialize, Serialize};

use super::bundle::BundleManifest;
use super::lessons::{MAX_LESSON_CHARS, MAX_WHY_CHARS};
use super::meter::{Charge, Meter, total};
use super::propose::{
    Ask, Edit, ProposalAttempts, ProposerReply, StageOutcome, edit_stage, record_calls,
    spent_failure,
};
use super::run::LiveReprobe;
use crate::agent::AgentProfile;
use crate::agent::overlay::{Ceilings, ProfileOverlay};
use crate::error::AgentError;
use crate::eval::anon::AnonId;
use crate::eval::evidence::{EvidenceWorkspace, candidate_ids, write_bundle};
use crate::eval::{AnonymisedPool, Evaluation, LiveCheck, RunEnd, recorded_end, recorded_pool};
use crate::harness::{AgentHarness, truncate};
use crate::message::TokenUsage;
use crate::model::BoxFuture;

/// The error type of a compose step with no composer installed.
const COMPOSER_MISSING: &str = "OverlayComposerMissing";

/// The error type of a step with no source reader installed.
pub(crate) const SOURCES_MISSING: &str = "RefineSourcesMissing";

/// The most characters of a note.
const MAX_NOTE_CHARS: usize = 500;

/// One probed candidate under its opaque id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComposeCandidate {
    /// The opaque id. The pool and the bundle use the same id.
    pub id: AnonId,
    /// The overlay of the candidate.
    pub overlay: ProfileOverlay,
}

/// What a composer reads for one attempt.
///
/// It is `#[non_exhaustive]`, so a new field does not break a composer.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ComposeContext<'a> {
    /// The base profile of the operator.
    pub profile: &'a AgentProfile,
    /// The ceilings of the run. A reply above them is invalid.
    pub ceilings: &'a Ceilings,
    /// The overlay that the stage edits.
    pub current: &'a ProfileOverlay,
    /// The probed candidates, in id order.
    pub candidates: &'a [ComposeCandidate],
    /// The anonymised pool of their probes.
    pub report: &'a AnonymisedPool,
    /// The pool as text, as [`AnonymisedPool::render`] writes it.
    pub text: &'a str,
    /// The evidence bundle, when the harness has a workspace store.
    pub bundle: Option<&'a BundleManifest>,
    /// The composition number of the round, from 1.
    pub sample: u32,
    /// The attempt number. The first attempt is 1.
    pub attempt: u32,
    /// Why the stage refused the reply of the last attempt.
    pub rejection: Option<&'a str>,
    meter: Option<&'a Meter>,
}

impl<'a> ComposeContext<'a> {
    /// The context of attempt 1 of composition 1, with no bundle. A test of
    /// an app composer builds one with it.
    #[must_use]
    pub const fn new(
        profile: &'a AgentProfile,
        ceilings: &'a Ceilings,
        current: &'a ProfileOverlay,
        candidates: &'a [ComposeCandidate],
        report: &'a AnonymisedPool,
        text: &'a str,
    ) -> Self {
        Self {
            profile,
            ceilings,
            current,
            candidates,
            report,
            text,
            bundle: None,
            sample: 1,
            attempt: 1,
            rejection: None,
            meter: None,
        }
    }

    /// Record one model call of the composer. The stage writes a cost
    /// ledger row for it, and the run counts it in its budget.
    pub fn charge(&self, charge: Charge) {
        if let Some(meter) = self.meter {
            meter.charge(charge);
        }
    }
}

/// A composer of overlays.
///
/// An app implements it, as it does
/// [`OverlayProposer`](super::OverlayProposer). The error kinds work as for
/// a proposer. The composed text is model output. Treat it as untrusted
/// until the adoption gate (issue #2381) accepts it.
pub trait OverlayComposer: Send + Sync + fmt::Debug {
    /// Compose an overlay for one attempt.
    fn compose<'a>(
        &'a self,
        context: &'a ComposeContext<'a>,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>>;
}

/// One probe that a compose or a select step reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProbedCandidate {
    /// The overlay of the candidate.
    pub overlay: ProfileOverlay,
    /// Its probe.
    pub evaluation: Evaluation,
    /// Each named model route that its probe asked for. The pool redacts
    /// each one. An input recorded before this field decodes with none.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub routes: BTreeSet<String>,
    /// The live re-probe of the probe. The pool shows it, and the rule
    /// never picks a live failure. An input recorded before this field
    /// decodes with none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live: Option<LiveReprobe>,
}

/// The input of [`agent_refine_compose`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComposeRequest {
    /// The source run of the probes.
    pub source: ExecutionId,
    /// The base profile.
    pub profile: AgentProfile,
    /// The overlay that the stage edits.
    pub current: ProfileOverlay,
    /// The ceilings of the run.
    pub ceilings: Ceilings,
    /// The number of attempts.
    pub attempts: ProposalAttempts,
    /// The composition number of the round, from 1.
    pub sample: u32,
    /// The seed of the refine run.
    pub seed: u64,
    /// The probes of the round.
    pub probes: Vec<ProbedCandidate>,
    /// The profiles of the other sources of the run, and the profile of the
    /// kind. A valid reply applies to each of them too.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub others: Vec<AgentProfile>,
}

/// The output of [`agent_refine_compose`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Composition {
    /// The composer calls of the recorded run of the stage.
    pub attempts: u32,
    /// The result.
    pub outcome: StageOutcome,
    /// The tokens that the composer charged.
    #[serde(default)]
    pub usage: TokenUsage,
    /// The cost that the composer charged.
    #[serde(default)]
    pub cost_usd_micros: u64,
    /// What the step could not do, such as write the bundle.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

/// The names that a pool and a bundle of `probes` hide, as in
/// `compare_anonymised`. They are each overlay digest, each model route and
/// the provider id of each route.
///
/// The routes are the default route, each overlay model and each route that
/// a probe asked for. A probe can inherit the route of its source task, or
/// ask for the route of a recorded peer.
fn identities_of(harness: &AgentHarness, probes: &[ProbedCandidate]) -> Vec<String> {
    let mut routes: BTreeSet<Option<String>> = BTreeSet::from([None]);
    let mut identities: Vec<String> = Vec::new();
    for probe in probes {
        identities.push(probe.evaluation.overlay_digest.clone());
        routes.insert(probe.overlay.model.clone());
        routes.extend(probe.routes.iter().cloned().map(Some));
    }
    for route in routes {
        if let Ok(model) = harness.model(route.as_deref())
            && let Some(id) = model.model_id()
        {
            identities.push(id.to_owned());
        }
        identities.extend(route);
    }
    identities
}

/// The pool of `probes` of the source `events`, and the ids in input order.
///
/// Each failure text of the pool loses each name of [`identities_of`].
fn pool_of(
    harness: &AgentHarness,
    events: &[WorkflowEvent],
    seed: u64,
    probes: &[ProbedCandidate],
) -> Result<(Vec<ComposeCandidate>, AnonymisedPool), String> {
    let evaluations: Vec<Evaluation> = probes
        .iter()
        .map(|probe| probe.evaluation.clone())
        .collect();
    let ids = candidate_ids(seed, events, &evaluations);
    let identities = identities_of(harness, probes);
    let recorded = recorded_end(events).map_err(|err| err.to_string())?;
    let entries = ids
        .iter()
        .cloned()
        .zip(evaluations)
        .zip(probes)
        .map(|((id, evaluation), probe)| (id, evaluation, probe.live.as_ref().map(LiveCheck::from)))
        .collect();
    let pool = recorded_pool(recorded, entries, identities);
    let mut candidates: Vec<ComposeCandidate> = ids
        .into_iter()
        .zip(probes)
        .map(|(id, probe)| ComposeCandidate {
            id,
            overlay: probe.overlay.clone(),
        })
        .collect();
    candidates.sort_by(|a, b| a.id.cmp(&b.id));
    Ok((candidates, pool))
}

/// The history of `source`, read through the installed source reader.
pub(crate) async fn source_events(
    harness: &AgentHarness,
    source: ExecutionId,
) -> Result<Vec<WorkflowEvent>, String> {
    let sources = harness.sources().ok_or_else(|| {
        ActivityFailure::non_retryable(
            SOURCES_MISSING,
            "no RefineSources is installed: pass one to AgentHarness::refine_sources",
        )
        .into_error_payload()
    })?;
    let record = sources
        .source(source)
        .await
        .map_err(|err| format!("the source read failed: {err}"))?
        .ok_or_else(|| {
            ActivityFailure::non_retryable(
                "RefineSourceNotFound",
                format!("the source run {source} does not exist"),
            )
            .into_error_payload()
        })?;
    Ok(record.events)
}

/// The composer of one stage and what it reads.
struct ComposeAsk<'a> {
    composer: &'a dyn OverlayComposer,
    profile: &'a AgentProfile,
    ceilings: &'a Ceilings,
    current: &'a ProfileOverlay,
    candidates: &'a [ComposeCandidate],
    report: &'a AnonymisedPool,
    text: &'a str,
    bundle: Option<&'a BundleManifest>,
    sample: u32,
}

impl Ask for ComposeAsk<'_> {
    fn ask<'a>(
        &'a self,
        attempt: u32,
        rejection: Option<&'a str>,
        meter: &'a Meter,
    ) -> BoxFuture<'a, Result<ProposerReply, AgentError>> {
        Box::pin(async move {
            let context = ComposeContext {
                profile: self.profile,
                ceilings: self.ceilings,
                current: self.current,
                candidates: self.candidates,
                report: self.report,
                text: self.text,
                bundle: self.bundle,
                sample: self.sample,
                attempt,
                rejection,
                meter: Some(meter),
            };
            self.composer.compose(&context).await
        })
    }
}

/// Runs one compose stage on `harness`, with the cost ledger call of each
/// charged model call.
///
/// # Errors
///
/// Returns a non-retryable payload when no composer or no source reader is
/// installed, or for the errors of a proposer stage.
pub(crate) async fn run_compose(
    harness: &AgentHarness,
    request: ComposeRequest,
) -> Result<(Composition, Vec<LlmCall>), String> {
    let composer = harness.overlay_composer().ok_or_else(|| {
        ActivityFailure::non_retryable(
            COMPOSER_MISSING,
            "no OverlayComposer is installed: pass one to AgentHarness::composer",
        )
        .into_error_payload()
    })?;
    let events = source_events(harness, request.source).await?;
    let (candidates, report) = pool_of(harness, &events, request.seed, &request.probes)?;
    let text = report.render();
    let mut notes = Vec::new();
    let bundle = match harness.workspace_store_opt() {
        None => None,
        Some(store) => {
            let mut workspace = EvidenceWorkspace::new(
                store.clone(),
                format!("refine/{}", request.source),
                request.seed,
            );
            // The bundle hides the same names as the pool. It renders the
            // evaluations again, so a hidden name of the pool must be hidden
            // here too.
            for name in identities_of(harness, &request.probes) {
                workspace = workspace.hide(name);
            }
            let evaluations: Vec<Evaluation> = request
                .probes
                .iter()
                .map(|probe| probe.evaluation.clone())
                .collect();
            match write_bundle(&workspace, &events, &evaluations, &request.current).await {
                Ok(written) => Some(written.manifest),
                Err(err) => {
                    notes.push(truncate(
                        &format!("the evidence bundle was not written: {err}"),
                        MAX_NOTE_CHARS,
                    ));
                    None
                }
            }
        }
    };
    let ask = ComposeAsk {
        composer: composer.as_ref(),
        profile: &request.profile,
        ceilings: &request.ceilings,
        current: &request.current,
        candidates: &candidates,
        report: &report,
        text: &text,
        bundle: bundle.as_ref(),
        sample: request.sample,
    };
    let edit = Edit {
        profile: &request.profile,
        current: &request.current,
        ceilings: &request.ceilings,
        attempts: request.attempts,
        others: &request.others,
    };
    let edited = edit_stage(harness, &edit, &ask).await?;
    let (usage, cost_usd_micros) = total(&edited.charges);
    let calls = edited.charges.iter().map(Charge::ledger_call).collect();
    Ok((
        Composition {
            attempts: edited.attempts,
            outcome: edited.outcome,
            usage,
            cost_usd_micros,
            notes,
        },
        calls,
    ))
}

/// One compose stage, as an activity.
///
/// # Errors
///
/// The errors of a proposer stage, and a non-retryable error when no
/// composer or no source reader is installed.
#[activity(
    start_to_close = "2h",
    retry = RetryPolicy::exponential(4, Duration::from_secs(2))
)]
pub async fn agent_refine_compose(
    ctx: &ActivityContext,
    request: ComposeRequest,
) -> Result<Composition, String> {
    let (composition, calls) = run_compose(crate::workflow::harness(ctx)?, request).await?;
    record_calls(ctx, calls);
    Ok(composition)
}

/// What a selector reads.
///
/// It is `#[non_exhaustive]`, so a new field does not break a selector.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct SelectContext<'a> {
    /// The probed compositions, in id order.
    pub candidates: &'a [ComposeCandidate],
    /// The anonymised pool of their probes.
    pub report: &'a AnonymisedPool,
    /// The pool as text.
    pub text: &'a str,
    meter: Option<&'a Meter>,
}

impl<'a> SelectContext<'a> {
    /// A context with no meter. A test of an app selector builds one with
    /// it.
    #[must_use]
    pub const fn new(
        candidates: &'a [ComposeCandidate],
        report: &'a AnonymisedPool,
        text: &'a str,
    ) -> Self {
        Self {
            candidates,
            report,
            text,
            meter: None,
        }
    }

    /// Record one model call of the selector.
    pub fn charge(&self, charge: Charge) {
        if let Some(meter) = self.meter {
            meter.charge(charge);
        }
    }
}

/// A selector of one composition.
///
/// It returns the id of the pick. In five cases, the rule of
/// [`SelectedBy::Rule`] picks in its place:
///
/// - no selector is installed;
/// - the selector names an id outside the pool;
/// - it names a candidate that failed its live re-probe;
/// - it fails with a kind that a retry cannot fix;
/// - it does not answer in time.
///
/// The step asks no selector when each candidate failed its live re-probe.
/// It then picks none. [`SelectContext::report`] shows the live end of each
/// candidate.
///
/// A retryable error retries the step.
pub trait OverlaySelector: Send + Sync + fmt::Debug {
    /// Pick one id of `context.candidates`.
    fn select<'a>(
        &'a self,
        context: &'a SelectContext<'a>,
    ) -> BoxFuture<'a, Result<AnonId, AgentError>>;

    /// Pick one id, and say why and what the pick teaches. The lesson
    /// library keeps that text when the pick goes live. The default calls
    /// [`select`](Self::select) and gives no text.
    ///
    /// The refine run calls `choose` only. A selector that overrides it
    /// still implements `select`.
    fn choose<'a>(
        &'a self,
        context: &'a SelectContext<'a>,
    ) -> BoxFuture<'a, Result<Choice, AgentError>> {
        Box::pin(async move { self.select(context).await.map(Choice::new) })
    }
}

/// The pick of a selector, with its reasons.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Choice {
    /// The opaque id of the pick.
    pub id: AnonId,
    /// Why the selector picked it. Empty for no text.
    pub why: String,
    /// What the pick teaches the next refine. Empty for no text.
    pub lesson: String,
}

impl Choice {
    /// A pick of `id` with no text.
    #[must_use]
    pub const fn new(id: AnonId) -> Self {
        Self {
            id,
            why: String::new(),
            lesson: String::new(),
        }
    }

    /// Say why the selector picked the id.
    #[must_use]
    pub fn why(mut self, why: impl Into<String>) -> Self {
        self.why = why.into();
        self
    }

    /// Say what the pick teaches the next refine.
    #[must_use]
    pub fn lesson(mut self, lesson: impl Into<String>) -> Self {
        self.lesson = lesson.into();
        self
    }
}

/// `text` cut to `max` characters, or `None` for a blank text.
fn reason(text: &str, max: usize) -> Option<String> {
    let text = text.trim();
    (!text.is_empty()).then(|| truncate(text, max))
}

/// The input of [`agent_refine_select`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectRequest {
    /// The source run of the probes.
    pub source: ExecutionId,
    /// The seed of the refine run.
    pub seed: u64,
    /// The probed compositions. Each one is a candidate.
    pub probes: Vec<ProbedCandidate>,
}

/// Who made a selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectedBy {
    /// The installed selector.
    Selector,
    /// The rule. A live failure is out of the pick. Then a completed end
    /// comes first, then the fewest stubbed tool calls, then the lowest id.
    Rule,
}

/// The output of [`agent_refine_select`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    /// The index of the pick in [`SelectRequest::probes`]. `None` for an
    /// empty pool, and for a pool where each candidate failed its live
    /// re-probe.
    pub index: Option<usize>,
    /// The opaque id of the pick.
    pub id: Option<AnonId>,
    /// Who made the pick.
    pub by: SelectedBy,
    /// The tokens that the selector charged.
    #[serde(default)]
    pub usage: TokenUsage,
    /// The cost that the selector charged.
    #[serde(default)]
    pub cost_usd_micros: u64,
    /// Why the rule replaced the pick of the installed selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Why the selector picked the id. A rule pick has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    /// What the pick teaches the next refine. A rule pick has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lesson: Option<String>,
}

/// The rule pick of `pool`. A live failure is out of the pick. Then a
/// completed end comes first, then the fewest stubbed tool calls, then the
/// lowest id. The pool lists its entries in id order. See
/// [`rule`](super::rule).
fn rule_pick(pool: &AnonymisedPool) -> Option<AnonId> {
    let at = super::rule::rule_pick(pool.candidates.iter().map(|entry| {
        (
            entry.live.as_ref().is_some_and(LiveCheck::failed),
            matches!(entry.end, RunEnd::Completed(_)),
            entry.stubbed_tool_calls,
        )
    }))?;
    pool.candidates.get(at).map(|entry| entry.id.clone())
}

/// Whether the pool entry of `id` is a live failure.
fn live_failed(pool: &AnonymisedPool, id: &AnonId) -> bool {
    pool.candidates
        .iter()
        .find(|entry| entry.id == *id)
        .and_then(|entry| entry.live.as_ref())
        .is_some_and(LiveCheck::failed)
}

/// Runs one select step on `harness`.
///
/// # Errors
///
/// Returns a retryable payload when the selector fails with a retryable
/// kind, and the errors of the source read.
pub(crate) async fn run_select(
    harness: &AgentHarness,
    request: SelectRequest,
) -> Result<(Selection, Vec<LlmCall>), String> {
    let events = source_events(harness, request.source).await?;
    let (candidates, report) = pool_of(harness, &events, request.seed, &request.probes)?;
    let ids: Vec<AnonId> = candidate_ids(
        request.seed,
        &events,
        &request
            .probes
            .iter()
            .map(|probe| probe.evaluation.clone())
            .collect::<Vec<_>>(),
    );
    let index_of = |id: &AnonId| ids.iter().position(|known| known == id);
    let rule = rule_pick(&report);
    let mut selection = Selection {
        index: rule.as_ref().and_then(index_of),
        id: rule,
        by: SelectedBy::Rule,
        usage: TokenUsage::default(),
        cost_usd_micros: 0,
        note: None,
        why: None,
        lesson: None,
    };
    if selection.id.is_none() && !candidates.is_empty() {
        // Each candidate is a live failure. No pick can stand, so the step
        // asks no selector.
        selection.note =
            Some("each candidate failed its live re-probe, so the rule picked none".to_owned());
        return Ok((selection, Vec::new()));
    }
    let Some(selector) = harness.overlay_selector() else {
        return Ok((selection, Vec::new()));
    };
    if candidates.is_empty() {
        return Ok((selection, Vec::new()));
    }
    let text = report.render();
    let meter = Meter::default();
    let context = SelectContext {
        candidates: &candidates,
        report: &report,
        text: &text,
        meter: Some(&meter),
    };
    let budget = harness.proposer_budget();
    let picked = tokio::time::timeout(budget, selector.choose(&context)).await;
    let charges = meter.take();
    let (usage, cost_usd_micros) = total(&charges);
    selection.usage = usage;
    selection.cost_usd_micros = cost_usd_micros;
    match picked {
        Ok(Ok(choice)) => match index_of(&choice.id) {
            Some(_) if live_failed(&report, &choice.id) => {
                selection.note = Some(truncate(
                    &format!(
                        "the selector named {}, which failed its live re-probe",
                        choice.id
                    ),
                    MAX_NOTE_CHARS,
                ));
            }
            Some(index) => {
                selection.index = Some(index);
                selection.why = reason(&choice.why, MAX_WHY_CHARS);
                selection.lesson = reason(&choice.lesson, MAX_LESSON_CHARS);
                selection.id = Some(choice.id);
                selection.by = SelectedBy::Selector;
            }
            None => {
                selection.note = Some(truncate(
                    &format!("the selector named {}, which is not in the pool", choice.id),
                    MAX_NOTE_CHARS,
                ));
            }
        },
        Ok(Err(err)) if err.kind().is_retryable() => return Err(spent_failure(&err, &charges)),
        Ok(Err(err)) => {
            selection.note = Some(truncate(
                &format!("the selector failed: {err}"),
                MAX_NOTE_CHARS,
            ));
        }
        Err(_) => {
            selection.note = Some(format!(
                "the selector did not answer within {} ms",
                budget.as_millis()
            ));
        }
    }
    let calls = charges.iter().map(Charge::ledger_call).collect();
    Ok((selection, calls))
}

/// One select step, as an activity.
///
/// # Errors
///
/// The errors of the source read, and a retryable selector error.
#[activity(
    start_to_close = "30m",
    retry = RetryPolicy::exponential(4, Duration::from_secs(2))
)]
pub async fn agent_refine_select(
    ctx: &ActivityContext,
    request: SelectRequest,
) -> Result<Selection, String> {
    let (selection, calls) = run_select(crate::workflow::harness(ctx)?, request).await?;
    record_calls(ctx, calls);
    Ok(selection)
}
