//! The `agent_sample` workflow: best-of-N and rejection sampling (issue
//! #2443, epic #2438).
//!
//! The run reads the profile and the sampling policy of its kind once
//! each, as `agent_draws` does. It admits the request, then runs the draws
//! in waves. Each draw is a child `agent_loop` run with the run id
//! `{root}:s{n}`, so a crash costs only the draws in flight.
//!
//! - The draws share one token budget: the cap of the profile, narrowed by
//!   the input. Each wave splits the rest with `sibling_cap`.
//! - After each wave, the `agent_sample_score` activity scores each draw.
//!   History records each verdict.
//! - Best-of-N runs each draw, then calls [`select`]. Rejection stops after
//!   the first wave that holds a pass.
//! - A cascade asks one tier at a time, each with its own model. It stops
//!   at the first tier that accepts. [`cascade_step`] skips each tier that
//!   the mandate forbids.
//!
//! The design is in `docs/design/DESIGN-2443.md` and
//! `docs/design/DESIGN-2445.md`.

use std::num::NonZeroU32;

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use super::admit::{
    Admission, Ask, CascadeStep, Refusal, SampleStep, admit, admit_tiers, cascade_step, sample_step,
};
use super::draws::{draw_task, profile_task, read_policy, read_profile};
use super::policy::{ScorerRef, Strategy};
use autumn_harvest::llm_budget::LlmBudgetExceeded;

use crate::bounds::{exceeds_activity_input, sibling_cap};
use crate::draw::{DrawId, run_draw_tasks};
use crate::harness::cut_bytes;
use crate::mesh::valid_name;
use crate::message::TokenUsage;
use crate::score::{
    FiniteF64, SampleScore, ScoreInput, ScoreRequest, Verdict, agent_sample_score_info, select,
};
use crate::types::{AgentReport, AgentStop, AgentTask};

/// The workflow name of [`agent_sample`].
pub const SAMPLE_WORKFLOW_NAME: &str = "agent_sample";

/// One tier of a cascade: a harness model and the lowest score that the
/// tier accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Tier {
    /// The harness model of the tier, as `AgentHarness::model_named` names
    /// it. The policy must list it in `cascade_models`.
    pub model: String,
    /// The lowest score that the tier accepts.
    pub threshold: FiniteF64,
}

impl Tier {
    /// A tier that asks `model` and accepts a score at or above
    /// `threshold`.
    #[must_use]
    pub fn new(model: impl Into<String>, threshold: FiniteF64) -> Self {
        Self {
            model: model.into(),
            threshold,
        }
    }
}

/// The strategy of one `agent_sample` run.
///
/// It encodes as `{"best_of_n": {"n": 3}}`,
/// `{"rejection": {"max_attempts": 4, "threshold": 0.8}}` or
/// `{"cascade": {"tiers": [{"model": "small", "threshold": 0.8}]}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum SampleStrategy {
    /// Run `n` draws, score each, and select the best.
    BestOfN {
        /// The draws.
        n: u32,
    },
    /// Run draws until one scores at or above `threshold`, up to
    /// `max_attempts` draws.
    Rejection {
        /// The most draws.
        max_attempts: u32,
        /// The lowest score that passes.
        threshold: FiniteF64,
    },
    /// Ask each tier in order, one draw per tier. Stop at the first answer
    /// that scores at or above the threshold of its tier.
    Cascade {
        /// The tiers, the cheapest first.
        tiers: Vec<Tier>,
    },
}

impl SampleStrategy {
    /// Best-of-`n`.
    #[must_use]
    pub const fn best_of_n(n: u32) -> Self {
        Self::BestOfN { n }
    }

    /// Rejection sampling.
    #[must_use]
    pub const fn rejection(max_attempts: u32, threshold: FiniteF64) -> Self {
        Self::Rejection {
            max_attempts,
            threshold,
        }
    }

    /// A cascade over `tiers`, the cheapest first.
    #[must_use]
    pub fn cascade(tiers: impl IntoIterator<Item = Tier>) -> Self {
        Self::Cascade {
            tiers: tiers.into_iter().collect(),
        }
    }

    /// The policy strategy, such as `best_of_n`.
    #[must_use]
    pub const fn policy_strategy(&self) -> Strategy {
        match self {
            Self::BestOfN { .. } => Strategy::BestOfN,
            Self::Rejection { .. } => Strategy::Rejection,
            Self::Cascade { .. } => Strategy::Cascade,
        }
    }

    /// The most draws of the run. A cascade has one draw per tier.
    #[must_use]
    pub fn draws(&self) -> u32 {
        match self {
            Self::BestOfN { n } => *n,
            Self::Rejection { max_attempts, .. } => *max_attempts,
            Self::Cascade { tiers } => u32::try_from(tiers.len()).unwrap_or(u32::MAX),
        }
    }
}

/// The input of one `agent_sample` run.
///
/// It denies unknown keys, so it never carries a policy field. It holds no
/// task: the profile of the kind builds the task of each draw.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SampleInput {
    /// The agent kind. Its profile builds each draw, and its policy applies.
    pub kind: String,
    /// The user message of each draw.
    pub input: String,
    /// The strategy.
    pub strategy: SampleStrategy,
    /// The scorer. The policy must allow it.
    pub scorer: ScorerRef,
    /// The most draws at the same time. `None` takes the cap of the policy
    /// for best-of-N, and 1 for rejection and for a cascade. A cascade
    /// refuses a value above 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<NonZeroU32>,
    /// The token budget of the whole run tree. It narrows the cap of the
    /// profile, and never raises it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_tokens: Option<u32>,
}

impl SampleInput {
    /// A run of `strategy` over `input` for the kind `kind`.
    #[must_use]
    pub fn new(
        kind: impl Into<String>,
        input: impl Into<String>,
        strategy: SampleStrategy,
        scorer: ScorerRef,
    ) -> Self {
        Self {
            kind: kind.into(),
            input: input.into(),
            strategy,
            scorer,
            concurrency: None,
            max_total_tokens: None,
        }
    }

    /// Run at most `n` draws at the same time. A value of 0 takes the
    /// default of the strategy.
    #[must_use]
    pub const fn concurrency(mut self, n: u32) -> Self {
        self.concurrency = NonZeroU32::new(n);
        self
    }

    /// Cap the tokens of the whole run tree. The cap of the profile still
    /// applies when it is lower.
    #[must_use]
    pub const fn max_total_tokens(mut self, max: u32) -> Self {
        self.max_total_tokens = Some(max);
        self
    }
}

/// The result of the selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleOutcome {
    /// The run selected this draw.
    Selected {
        /// The draw.
        draw: DrawId,
    },
    /// No tier of a cascade accepted. The run selected the best draw with
    /// [`select`].
    Escalated {
        /// The draw.
        draw: DrawId,
    },
    /// No draw passed.
    NoAcceptedDraw,
}

/// A cascade tier whose model the mandate forbids. The run never asks it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SkippedTier {
    /// The index of the tier.
    pub tier: u32,
    /// The model of the tier.
    pub model: String,
    /// Why the run skipped the tier.
    pub refusal: Refusal,
}

/// Why the run started no more draws.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SampleStop {
    /// Each planned draw ran.
    Completed,
    /// A draw passed the threshold of a rejection run.
    Accepted,
    /// A budget refused the next draw.
    BudgetExceeded,
    /// A score failed, so the run started no more draws. The verdict of
    /// the draw names the error.
    ScoreFailed,
}

/// The record of one draw.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SampleDraw {
    /// The draw.
    pub draw: DrawId,
    /// The run id of the draw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// The harness model of a cascade tier. `None` for each other
    /// strategy, so its report keeps its bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Why the draw run ended.
    pub stop: AgentStop,
    /// The verdict of the scorer.
    pub verdict: Verdict,
    /// The tokens of the draw run and of its judge calls.
    pub usage: TokenUsage,
    /// The cost of the draw run and of its judge calls, in millionths of a
    /// US dollar. An unpriced call adds 0.
    #[serde(default, skip_serializing_if = "crate::agent::is_zero")]
    pub cost_usd_micros: u64,
}

/// The result of one `agent_sample` run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SampleReport {
    /// The agent kind.
    pub kind: String,
    /// The strategy.
    pub strategy: SampleStrategy,
    /// The revision of the policy that the run read. 0 for every default.
    pub revision: u64,
    /// The most draws at the same time.
    pub concurrency: u32,
    /// The selection.
    pub outcome: SampleOutcome,
    /// Why the run stopped.
    pub stopped: SampleStop,
    /// The draws that ran.
    pub attempts: u32,
    /// The report of the selected draw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected: Option<AgentReport>,
    /// Each draw, in draw order.
    pub draws: Vec<SampleDraw>,
    /// Each cascade tier whose model the mandate forbids, in tier order.
    /// The run never asks such a tier. A skipped tier encodes as
    /// `{"tier": 0, "model": "m", "refusal": {"refusal": "not_permitted"}}`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<SkippedTier>,
    /// The tokens of the run tree, judge calls included.
    pub usage: TokenUsage,
    /// The cost of the run tree, judge calls included, in millionths of a
    /// US dollar.
    #[serde(default, skip_serializing_if = "crate::agent::is_zero")]
    pub cost_usd_micros: u64,
}

/// Run one sample: draw, score and select.
///
/// # Errors
///
/// Returns an error that starts with
/// [`REFUSED_PREFIX`](super::REFUSED_PREFIX) when the policy refuses the
/// run. No model call runs before it. Returns an error when a read fails
/// for good, or when a draw run fails in the engine. A failed score does
/// not fail the run: it stops the run with `score_failed`.
#[workflow]
pub async fn agent_sample(
    ctx: &WorkflowContext,
    input: SampleInput,
) -> Result<SampleReport, String> {
    if !valid_name(&input.kind) {
        return Err(Refusal::InvalidKind.error_text());
    }
    let Some(profile) = read_profile(ctx, &input.kind).await? else {
        return Err(Refusal::UnknownKind.error_text());
    };
    let read = read_policy(ctx, &input.kind).await?;
    let concurrency = input
        .concurrency
        .map_or_else(|| default_concurrency(&input.strategy), NonZeroU32::get);
    let ask = Ask {
        strategy: input.strategy.policy_strategy(),
        draws: input.strategy.draws(),
        concurrency,
        scorer: Some(&input.scorer),
    };
    let admission = admit(&read.policy, &ask).map_err(|refusal| refusal.error_text())?;
    if let SampleStrategy::Cascade { tiers } = &input.strategy {
        if concurrency > 1 {
            let refusal = Refusal::TooManyConcurrentDraws {
                asked: concurrency,
                max: 1,
            };
            return Err(refusal.error_text());
        }
        admit_tiers(&read.policy, tiers).map_err(|refusal| refusal.error_text())?;
    }
    let task = draw_task(
        &profile_task(&profile, &input.input),
        &admission.draw_effects,
    );
    let budget = tree_budget(task.max_total_tokens, input.max_total_tokens);
    let ended = if let SampleStrategy::Cascade { tiers } = &input.strategy {
        run_tiers(ctx, &input, tiers, &task, budget).await?
    } else {
        let (scored, stopped) = run_waves(ctx, &input, &task, &admission, budget).await?;
        Ended {
            scored,
            skipped: Vec::new(),
            stopped,
        }
    };
    Ok(report(input, read.revision, admission.concurrency, ended))
}

/// The concurrency of a run that asks for none.
///
/// Best-of-N takes the cap of the policy (0). Rejection and a cascade stop
/// at the first pass, so they run one draw at a time. A wider wave would
/// pay for draws after the pass.
const fn default_concurrency(strategy: &SampleStrategy) -> u32 {
    match strategy {
        SampleStrategy::BestOfN { .. } => 0,
        SampleStrategy::Rejection { .. } | SampleStrategy::Cascade { .. } => 1,
    }
}

/// The token budget of the run tree: the smaller of the two caps.
const fn tree_budget(profile: Option<u32>, input: Option<u32>) -> Option<u32> {
    match (profile, input) {
        (Some(a), Some(b)) => Some(if a < b { a } else { b }),
        (Some(cap), None) | (None, Some(cap)) => Some(cap),
        (None, None) => None,
    }
}

/// One draw that ran, with its record and its report.
struct Scored {
    record: SampleDraw,
    report: AgentReport,
}

/// The draws of a run, the tiers that it skipped, and why it stopped.
struct Ended {
    scored: Vec<Scored>,
    skipped: Vec<SkippedTier>,
    stopped: SampleStop,
}

/// Run the waves of the run, and score each draw.
///
/// [`sample_step`] gives the width of each wave. Draw `i` of a wave gets
/// the token cap `sibling_cap(left, width, i)`. After the score, the spend
/// of each draw and of its judge calls counts against the budget.
///
/// The run starts no more draws after a rejection wave with a pass. It
/// also stops after a failed score, and when a budget refuses a draw or a
/// judge call. A pass wins over the other stops, because the run has its
/// answer.
async fn run_waves(
    ctx: &WorkflowContext,
    input: &SampleInput,
    task: &AgentTask,
    admission: &Admission,
    budget: Option<u32>,
) -> Result<(Vec<Scored>, SampleStop), String> {
    let mut scored: Vec<Scored> = Vec::new();
    let mut spent: u32 = 0;
    loop {
        let started = u32::try_from(scored.len()).unwrap_or(u32::MAX);
        let left = budget.map_or(0, |max| max.saturating_sub(spent));
        let step = sample_step(
            admission.draws,
            started,
            admission.concurrency,
            budget.is_some(),
            left,
        );
        let width = match step {
            SampleStep::Done => return Ok((scored, SampleStop::Completed)),
            SampleStep::BudgetExceeded => return Ok((scored, SampleStop::BudgetExceeded)),
            SampleStep::Wave { width } => width,
        };
        let tasks = (0..width)
            .map(|index| {
                let mut draw = task.clone();
                if budget.is_some() {
                    draw.max_total_tokens = Some(sibling_cap(left, width, index));
                }
                (DrawId::new(started + index), draw)
            })
            .collect();
        let ended = run_draw_tasks(ctx, tasks).await?;
        let mut halt = None;
        let mut passed = false;
        for (id, report) in (started..).zip(ended) {
            let report = report?;
            if report.stop == AgentStop::BudgetExceeded {
                halt = halt.or(Some(SampleStop::BudgetExceeded));
            }
            let (one, halted) = score_draw(ctx, input, task, DrawId::new(id), report, halt).await?;
            halt = halt.or(halted);
            passed |= passes(&input.strategy, one.record.draw, &one.record.verdict);
            spent = spent.saturating_add(one.record.usage.total());
            scored.push(one);
        }
        if passed && matches!(input.strategy, SampleStrategy::Rejection { .. }) {
            return Ok((scored, SampleStop::Accepted));
        }
        if let Some(stop) = halt {
            return Ok((scored, stop));
        }
    }
}

/// Whether the mandate of `task` allows the harness model `model`.
fn permits(task: &AgentTask, model: &str) -> bool {
    task.mandate
        .as_ref()
        .is_none_or(|mandate| mandate.allows_model(Some(model)))
}

/// Each tier whose model the mandate forbids, as `allowed` gives it.
///
/// The mandate does not change during a run. So the list does not depend
/// on why the run stops.
fn forbidden(tiers: &[Tier], allowed: &[bool]) -> Vec<SkippedTier> {
    tiers
        .iter()
        .zip(allowed)
        .enumerate()
        .filter(|(_, (_, allowed))| !**allowed)
        .map(|(index, (tier, _))| SkippedTier {
            tier: u32::try_from(index).unwrap_or(u32::MAX),
            model: tier.model.clone(),
            refusal: Refusal::NotPermitted,
        })
        .collect()
}

/// Ask the tiers of a cascade in order, and score each answer.
///
/// [`cascade_step`] gives the next tier. Tier `i` is draw `i`, and it asks
/// the model of the tier with the rest of the token budget. The run skips
/// each tier that the mandate forbids, and the report names each one.
///
/// The run stops at the first tier that accepts. It also stops after a
/// failed score, and when a budget refuses a tier or a judge call. A tier
/// that fails gets a reject, and the run asks the next tier.
async fn run_tiers(
    ctx: &WorkflowContext,
    input: &SampleInput,
    tiers: &[Tier],
    task: &AgentTask,
    budget: Option<u32>,
) -> Result<Ended, String> {
    let allowed: Vec<bool> = tiers
        .iter()
        .map(|tier| permits(task, &tier.model))
        .collect();
    let skipped = forbidden(tiers, &allowed);
    let mut scored: Vec<Scored> = Vec::new();
    let mut spent: u32 = 0;
    let mut next = 0;
    loop {
        let left = budget.map_or(0, |max| max.saturating_sub(spent));
        let (index, tier) = match cascade_step(&allowed, next, budget.is_some(), left) {
            CascadeStep::Done => {
                return Ok(Ended {
                    scored,
                    skipped,
                    stopped: SampleStop::Completed,
                });
            }
            CascadeStep::BudgetExceeded => {
                return Ok(Ended {
                    scored,
                    skipped,
                    stopped: SampleStop::BudgetExceeded,
                });
            }
            CascadeStep::Run { tier } => {
                let Some(found) = tiers.get(tier) else {
                    return Err(format!("the cascade has no tier {tier}"));
                };
                (tier, found)
            }
        };
        let id = DrawId::new(u32::try_from(index).unwrap_or(u32::MAX));
        let mut draw = task.clone();
        draw.model = Some(tier.model.clone());
        if budget.is_some() {
            draw.max_total_tokens = Some(left);
        }
        let ended = run_draw_tasks(ctx, vec![(id, draw.clone())]).await?;
        let Some(report) = ended.into_iter().next() else {
            return Err(format!("the tier {index} returned no report"));
        };
        let report = report?;
        let halt = (report.stop == AgentStop::BudgetExceeded).then_some(SampleStop::BudgetExceeded);
        let (mut one, halted) = score_draw(ctx, input, &draw, id, report, halt).await?;
        one.record.model = Some(tier.model.clone());
        spent = spent.saturating_add(one.record.usage.total());
        let accepted = passes(&input.strategy, id, &one.record.verdict);
        scored.push(one);
        if accepted {
            return Ok(Ended {
                scored,
                skipped,
                stopped: SampleStop::Accepted,
            });
        }
        if let Some(stop) = halt.or(halted) {
            return Ok(Ended {
                scored,
                skipped,
                stopped: stop,
            });
        }
        next = index + 1;
    }
}

/// The most bytes of one tool result that a score input keeps.
const MAX_SCORED_TOOL_BYTES: usize = 16 * 1024;

/// The score request of one draw, or `None` when it does not fit the
/// activity input cap.
///
/// Each tool result is cut to [`MAX_SCORED_TOOL_BYTES`]. A request that
/// still does not fit would fail the run after the draws were paid.
fn score_request(
    input: &SampleInput,
    task: &AgentTask,
    report: &AgentReport,
) -> Option<ScoreRequest> {
    let mut score = ScoreInput::from_run(task, report);
    for tool in &mut score.tools {
        let content = std::mem::take(&mut tool.content);
        tool.content = cut_bytes(content, MAX_SCORED_TOOL_BYTES);
    }
    let request = ScoreRequest::new(input.scorer.id.as_str(), score)
        .admitted_judges(input.scorer.judge_model.clone());
    (!exceeds_activity_input(&request)).then_some(request)
}

/// Score one draw in the recorded `agent_sample_score` activity.
///
/// These draws get a reject and no score call, so they never pay for a
/// judge:
///
/// - a draw that failed, or that a budget stopped;
/// - a draw after a stop in its wave (`halt`);
/// - a draw whose score input does not fit the activity input cap.
///
/// A failed score gives a reject that names the error, and a stop: a budget
/// refusal gives `BudgetExceeded`, and each other failure `ScoreFailed`. A
/// judge call may name only the judge model that the run admitted.
///
/// # Errors
///
/// Returns an engine error, such as a cancellation, as text.
async fn score_draw(
    ctx: &WorkflowContext,
    input: &SampleInput,
    task: &AgentTask,
    draw: DrawId,
    report: AgentReport,
    halt: Option<SampleStop>,
) -> Result<(Scored, Option<SampleStop>), String> {
    let unscored = |reason: &str| (Verdict::reject(reason), TokenUsage::default(), 0);
    let mut stop = None;
    let (verdict, usage, cost) = if report.stop == AgentStop::Failed {
        unscored("the draw failed, so the run did not score it")
    } else if report.stop == AgentStop::BudgetExceeded {
        unscored("a budget stopped the draw, so the run did not score it")
    } else if halt.is_some() {
        unscored("the run stopped before it scored the draw")
    } else if let Some(request) = score_request(input, task, &report) {
        match ctx
            .execute_activity::<_, SampleScore>(&agent_sample_score_info(), request)
            .await
        {
            Ok(scored) => (scored.verdict, scored.usage, scored.cost_usd_micros),
            Err(err) if LlmBudgetExceeded::is_refusal(&err) => {
                stop = Some(SampleStop::BudgetExceeded);
                unscored("the LLM budget refused the score")
            }
            // A failed attempt records no ledger row, so the score spent
            // nothing that the report must count.
            Err(err @ HarvestError::ActivityFailed { .. }) => {
                stop = Some(SampleStop::ScoreFailed);
                unscored(&format!("the score failed: {err}"))
            }
            Err(err) => return Err(err.to_string()),
        }
    } else {
        unscored("the score input is too large to record")
    };
    let record = SampleDraw {
        draw,
        run_id: report.run_id.clone(),
        model: None,
        stop: report.stop,
        verdict,
        usage: report.usage.saturating_add(usage),
        cost_usd_micros: report.cost_usd_micros.saturating_add(cost),
    };
    Ok((Scored { record, report }, stop))
}

/// Whether `verdict` of the draw `draw` passes the check of `strategy`.
///
/// Best-of-N passes each score, and `select` never picks a reject.
/// Rejection passes a score at or above the threshold. A cascade passes a
/// score at or above the threshold of the tier of the draw.
fn passes(strategy: &SampleStrategy, draw: DrawId, verdict: &Verdict) -> bool {
    let at_least =
        |threshold: FiniteF64| verdict.as_score().is_some_and(|score| score >= threshold);
    match strategy {
        SampleStrategy::BestOfN { .. } => !verdict.is_reject(),
        SampleStrategy::Rejection { threshold, .. } => at_least(*threshold),
        SampleStrategy::Cascade { tiers } => usize::try_from(draw.get())
            .ok()
            .and_then(|index| tiers.get(index))
            .is_some_and(|tier| at_least(tier.threshold)),
    }
}

/// The selection of a run.
///
/// The run selects the best draw that passes. When no tier of a cascade
/// accepts, it selects the best draw that has a score, as `escalated`.
fn outcome(strategy: &SampleStrategy, scored: &[Scored]) -> SampleOutcome {
    if let Some(draw) = best(strategy, scored, true) {
        return SampleOutcome::Selected { draw };
    }
    if matches!(strategy, SampleStrategy::Cascade { .. })
        && let Some(draw) = best(strategy, scored, false)
    {
        return SampleOutcome::Escalated { draw };
    }
    SampleOutcome::NoAcceptedDraw
}

/// The best draw by [`select`]. With `passing`, only a draw that passes
/// the check of `strategy` counts. Without it, each draw with a score
/// counts.
fn best(strategy: &SampleStrategy, scored: &[Scored], passing: bool) -> Option<DrawId> {
    let mut verdicts: Vec<(DrawId, Verdict)> = Vec::with_capacity(scored.len());
    for one in scored {
        let record = &one.record;
        let keep = if passing {
            passes(strategy, record.draw, &record.verdict)
        } else {
            !record.verdict.is_reject()
        };
        if keep {
            verdicts.push((record.draw, record.verdict.clone()));
        }
    }
    select(&verdicts)
}

/// The report of the run: the selection, each draw and the totals.
fn report(input: SampleInput, revision: u64, concurrency: u32, ended: Ended) -> SampleReport {
    let Ended {
        scored,
        skipped,
        stopped,
    } = ended;
    let outcome = outcome(&input.strategy, &scored);
    let chosen = match outcome {
        SampleOutcome::Selected { draw } | SampleOutcome::Escalated { draw } => Some(draw),
        SampleOutcome::NoAcceptedDraw => None,
    };
    let usage = scored.iter().fold(TokenUsage::default(), |usage, one| {
        usage.saturating_add(one.record.usage)
    });
    let cost_usd_micros = scored.iter().fold(0_u64, |cost, one| {
        cost.saturating_add(one.record.cost_usd_micros)
    });
    let attempts = u32::try_from(scored.len()).unwrap_or(u32::MAX);
    let mut selected = None;
    let mut draws = Vec::with_capacity(scored.len());
    for one in scored {
        if Some(one.record.draw) == chosen {
            selected = Some(one.report);
        }
        draws.push(one.record);
    }
    SampleReport {
        kind: input.kind,
        strategy: input.strategy,
        revision,
        concurrency,
        outcome,
        stopped,
        attempts,
        selected,
        draws,
        skipped,
        usage,
        cost_usd_micros,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tree_budget_is_the_smaller_cap() {
        assert_eq!(tree_budget(None, None), None);
        assert_eq!(tree_budget(Some(9), None), Some(9));
        assert_eq!(tree_budget(None, Some(4)), Some(4));
        assert_eq!(tree_budget(Some(9), Some(4)), Some(4));
        assert_eq!(tree_budget(Some(3), Some(4)), Some(3));
    }

    #[test]
    fn a_rejection_passes_a_score_at_the_threshold() {
        let at = FiniteF64::new(0.5).unwrap();
        let strategy = SampleStrategy::rejection(3, at);
        let draw = DrawId::new(0);
        assert!(passes(&strategy, draw, &Verdict::score(0.5).unwrap()));
        assert!(!passes(&strategy, draw, &Verdict::score(0.49).unwrap()));
        assert!(!passes(&strategy, draw, &Verdict::reject("no")));
        let best = SampleStrategy::best_of_n(3);
        assert!(passes(&best, draw, &Verdict::score(-1e300).unwrap()));
        assert!(!passes(&best, draw, &Verdict::reject("no")));
    }

    #[test]
    fn a_cascade_passes_at_the_threshold_of_the_tier_of_the_draw() {
        let tier = |model: &str, value| Tier::new(model, FiniteF64::new(value).unwrap());
        let cascade = SampleStrategy::cascade([tier("cheap", 0.9), tier("strong", 0.5)]);
        let score = Verdict::score(0.6).unwrap();
        assert!(!passes(&cascade, DrawId::new(0), &score));
        assert!(passes(&cascade, DrawId::new(1), &score));
        assert!(!passes(&cascade, DrawId::new(2), &score), "no tier 2");
        assert!(!passes(&cascade, DrawId::new(1), &Verdict::reject("no")));
    }

    #[test]
    fn a_cascade_runs_one_draw_at_a_time_by_default() {
        let cascade = SampleStrategy::cascade([Tier::new("m", FiniteF64::ONE)]);
        assert_eq!(default_concurrency(&cascade), 1);
        assert_eq!(default_concurrency(&SampleStrategy::best_of_n(2)), 0);
    }

    #[test]
    fn the_report_names_each_forbidden_tier() {
        let tiers = [
            Tier::new("a", FiniteF64::ONE),
            Tier::new("b", FiniteF64::ONE),
            Tier::new("c", FiniteF64::ONE),
        ];
        let named: Vec<(u32, String)> = forbidden(&tiers, &[true, false, false])
            .into_iter()
            .map(|skip| (skip.tier, skip.model))
            .collect();
        assert_eq!(named, [(1, "b".to_owned()), (2, "c".to_owned())]);
        assert_eq!(forbidden(&tiers, &[true, true, true]), []);
    }

    #[test]
    fn a_score_input_cuts_each_tool_result() {
        let huge = "x".repeat(MAX_SCORED_TOOL_BYTES * 4);
        let report: AgentReport = serde_json::from_value(serde_json::json!({
            "stop": "completed",
            "text": "done",
            "steps_used": 1,
            "tool_calls": 1,
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "messages": [
                {"role": "assistant", "content": [
                    {"tool_call": {"id": "c1", "name": "read", "arguments": {}}}
                ]},
                {"role": "user", "content": [
                    {"tool_result": {"tool_call_id": "c1", "content": huge}}
                ]}
            ],
        }))
        .unwrap();
        let input = SampleInput::new(
            "k",
            "x",
            SampleStrategy::best_of_n(1),
            ScorerRef::deterministic("exact_match").unwrap(),
        );
        let request = score_request(&input, &AgentTask::new("x"), &report).unwrap();
        assert_eq!(request.input.tools.len(), 1);
        assert!(request.input.tools[0].content.len() <= MAX_SCORED_TOOL_BYTES);
        assert_eq!(
            request.admitted_judges,
            Some(std::collections::BTreeSet::new())
        );
    }

    #[test]
    fn the_strategy_maps_to_the_policy_strategy() {
        assert_eq!(
            SampleStrategy::best_of_n(4).policy_strategy(),
            Strategy::BestOfN
        );
        assert_eq!(SampleStrategy::best_of_n(4).draws(), 4);
        let rejection = SampleStrategy::rejection(5, FiniteF64::ONE);
        assert_eq!(rejection.policy_strategy(), Strategy::Rejection);
        assert_eq!(rejection.draws(), 5);
    }
}
