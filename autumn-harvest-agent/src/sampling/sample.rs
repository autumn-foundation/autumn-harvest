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
//! - A durable agent op that carries an [`OpSample`] runs the same core
//!   with the task of the op. The mandate of that task bounds the draws.
//!
//! The designs are in `docs/design/DESIGN-2443.md` and
//! `docs/design/DESIGN-2449.md`.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use super::admit::{Admission, Ask, Refusal, SampleStep, admit, sample_step};
use super::draws::{draw_task, profile_task, read_policy, read_profile};
use super::policy::{ScorerRef, Strategy};
use autumn_harvest::llm_budget::LlmBudgetExceeded;

use crate::bounds::{exceeds_activity_input, sibling_cap};
use crate::draw::{DrawId, run_draw_tasks};
use crate::harness::cut_bytes;
use crate::mesh::{Mandate, Refusal as MeshRefusal, valid_name};
use crate::message::TokenUsage;
use crate::score::{
    FiniteF64, SampleScore, ScoreInput, ScoreRequest, Verdict, agent_sample_score_info, select,
};
use crate::types::{AgentReport, AgentStop, AgentTask};

/// The workflow name of [`agent_sample`].
pub const SAMPLE_WORKFLOW_NAME: &str = "agent_sample";

/// The strategy of one `agent_sample` run.
///
/// It encodes as `{"best_of_n": {"n": 3}}` or
/// `{"rejection": {"max_attempts": 4, "threshold": 0.8}}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

    /// The policy strategy, such as `best_of_n`.
    #[must_use]
    pub const fn policy_strategy(self) -> Strategy {
        match self {
            Self::BestOfN { .. } => Strategy::BestOfN,
            Self::Rejection { .. } => Strategy::Rejection,
        }
    }

    /// The most draws of the run.
    #[must_use]
    pub const fn draws(self) -> u32 {
        match self {
            Self::BestOfN { n } => n,
            Self::Rejection { max_attempts, .. } => max_attempts,
        }
    }
}

/// A strategy that an agent op carries.
///
/// The agent runs the op through the core of [`agent_sample`]. The policy
/// of the kind and the mandate of the op apply. It denies unknown keys, so
/// it never carries a policy field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpSample {
    /// The strategy.
    pub strategy: SampleStrategy,
    /// The scorer. The policy must allow it.
    pub scorer: ScorerRef,
    /// The most draws at the same time. `None` takes the default of the
    /// strategy, as in [`SampleInput`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<NonZeroU32>,
}

impl OpSample {
    /// A sample of `strategy`, scored with `scorer`.
    #[must_use]
    pub const fn new(strategy: SampleStrategy, scorer: ScorerRef) -> Self {
        Self {
            strategy,
            scorer,
            concurrency: None,
        }
    }

    /// Run at most `n` draws at the same time. A value of 0 takes the
    /// default of the strategy.
    #[must_use]
    pub const fn concurrency(mut self, n: u32) -> Self {
        self.concurrency = NonZeroU32::new(n);
        self
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
    /// for best-of-N, and 1 for rejection.
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
    /// No draw passed.
    NoAcceptedDraw,
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

impl SampleStop {
    /// The wire name, such as `budget_exceeded`.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Accepted => "accepted",
            Self::BudgetExceeded => "budget_exceeded",
            Self::ScoreFailed => "score_failed",
        }
    }
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
    /// The end of the period of a mesh budget that refused the draw, in
    /// epoch milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parked_until_ms: Option<i64>,
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
    /// The tokens of the run tree, judge calls included.
    pub usage: TokenUsage,
    /// The cost of the run tree, judge calls included, in millionths of a
    /// US dollar.
    #[serde(default, skip_serializing_if = "crate::agent::is_zero")]
    pub cost_usd_micros: u64,
    /// The flow labels of each draw that ran, selected or not. A draw that
    /// the run does not select still read what its tools returned.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub flow_labels: BTreeSet<String>,
}

/// The patch marker of the mandate check of [`agent_sample`]. A run that
/// started before the check reads no marker, so its replay skips the check.
const SAMPLE_MANDATE_PATCH: &str = "agent_sample_mandate";

/// Why the sample core did not give a report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SampleFailure {
    /// The policy or the mandate refused the run before its first draw.
    Refused(Refusal),
    /// A read failed for good, or a draw run failed in the engine.
    Engine(String),
}

impl SampleFailure {
    /// The error text, such as `sampling refused (<code>): <reason>`.
    pub fn text(&self) -> String {
        match self {
            Self::Refused(refusal) => refusal.error_text(),
            Self::Engine(err) => err.clone(),
        }
    }
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
    let mut task = profile_task(&profile, &input.input);
    task.max_total_tokens = tree_budget(task.max_total_tokens, input.max_total_tokens);
    let sample = OpSample {
        strategy: input.strategy,
        scorer: input.scorer,
        concurrency: input.concurrency,
    };
    let mandate_check = ctx.patched(SAMPLE_MANDATE_PATCH);
    sample_task(ctx, &input.kind, &sample, &task, mandate_check)
        .await
        .map_err(|failure| failure.text())
}

/// The core of [`agent_sample`]: admit, draw, score and select.
///
/// `task` is the task of each draw before [`draw_task`]. Its token cap is
/// the budget of the whole run. With `mandate_check`, the mandate of
/// `task` admits the draws first. Each draw counts as one delegation, and
/// the draws split the delegations left with `sibling_cap`.
///
/// A durable agent calls it for an op that carries an [`OpSample`]. That
/// task holds the transcript, the labels and the narrowed mandate of the
/// op.
///
/// # Errors
///
/// Returns [`SampleFailure::Refused`] before any model call when the
/// mandate or the policy refuses the run. Returns
/// [`SampleFailure::Engine`] when a read fails for good, or when a draw run
/// fails in the engine.
pub async fn sample_task(
    ctx: &WorkflowContext,
    kind: &str,
    sample: &OpSample,
    task: &AgentTask,
    mandate_check: bool,
) -> Result<SampleReport, SampleFailure> {
    let delegations = match (&task.mandate, mandate_check) {
        (Some(mandate), true) => Some(mandate_admits(mandate, sample)?),
        _ => None,
    };
    let read = read_policy(ctx, kind)
        .await
        .map_err(SampleFailure::Engine)?;
    let ask = Ask {
        strategy: sample.strategy.policy_strategy(),
        draws: sample.strategy.draws(),
        concurrency: sample
            .concurrency
            .map_or_else(|| default_concurrency(sample.strategy), NonZeroU32::get),
        scorer: Some(&sample.scorer),
    };
    let admission = admit(&read.policy, &ask).map_err(SampleFailure::Refused)?;
    let budget = task.max_total_tokens;
    let draw = draw_task(task, &admission.draw_effects);
    let waves = Waves {
        sample,
        task: &draw,
        admission: &admission,
        budget,
        delegations,
    };
    let (scored, stopped) = run_waves(ctx, &waves)
        .await
        .map_err(SampleFailure::Engine)?;
    Ok(report(
        kind,
        sample.strategy,
        read.revision,
        admission.concurrency,
        scored,
        stopped,
    ))
}

/// Admit the draws of `sample` under `mandate`. Returns the delegations
/// left for the draws to split.
///
/// A judge model must be a model that the mandate allows, as each model
/// of the subtree must be.
fn mandate_admits(mandate: &Mandate, sample: &OpSample) -> Result<u32, SampleFailure> {
    let refused = |refusal| SampleFailure::Refused(Refusal::Mandate { mandate: refusal });
    let left = mandate
        .admit_draws(sample.strategy.draws(), 0)
        .map_err(refused)?;
    if let Some(judge) = &sample.scorer.judge_model
        && !mandate.allows_model(Some(judge))
    {
        return Err(refused(MeshRefusal::ModelForbidden));
    }
    Ok(left)
}

/// The concurrency of a run that asks for none.
///
/// Best-of-N takes the cap of the policy (0). Rejection stops at the first
/// pass, so it runs one draw at a time. A wider wave would pay for draws
/// after the pass.
const fn default_concurrency(strategy: SampleStrategy) -> u32 {
    match strategy {
        SampleStrategy::BestOfN { .. } => 0,
        SampleStrategy::Rejection { .. } => 1,
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

/// The fixed parts of the waves of one run.
struct Waves<'a> {
    /// The strategy and the scorer.
    sample: &'a OpSample,
    /// The task of each draw, after [`draw_task`].
    task: &'a AgentTask,
    /// What the policy admitted.
    admission: &'a Admission,
    /// The token budget of the run tree.
    budget: Option<u32>,
    /// The delegations that the draws split, when a mandate admitted them.
    delegations: Option<u32>,
}

/// Run the waves of the run, and score each draw.
///
/// [`sample_step`] gives the width of each wave. Draw `i` of a wave gets
/// the token cap `sibling_cap(left, width, i)`. After the score, the spend
/// of each draw and of its judge calls counts against the budget. With
/// `delegations`, draw `n` of the run gets `sibling_cap(delegations, draws,
/// n)` delegations, so the draws never pass the fan-out of the mandate.
///
/// The run starts no more draws after a rejection wave with a pass. It
/// also stops after a failed score, and when a budget refuses a draw or a
/// judge call. A pass wins over the other stops, because the run has its
/// answer.
async fn run_waves(
    ctx: &WorkflowContext,
    waves: &Waves<'_>,
) -> Result<(Vec<Scored>, SampleStop), String> {
    let Waves {
        sample,
        task,
        admission,
        budget,
        delegations,
    } = *waves;
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
                if let (Some(rest), Some(mandate)) = (delegations, draw.mandate.as_mut()) {
                    mandate.max_delegations = sibling_cap(rest, admission.draws, started + index);
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
            let (one, halted) =
                score_draw(ctx, sample, task, DrawId::new(id), report, halt).await?;
            halt = halt.or(halted);
            passed |= passes(sample.strategy, &one.record.verdict);
            spent = spent.saturating_add(one.record.usage.total());
            scored.push(one);
        }
        if passed && matches!(sample.strategy, SampleStrategy::Rejection { .. }) {
            return Ok((scored, SampleStop::Accepted));
        }
        if let Some(stop) = halt {
            return Ok((scored, stop));
        }
    }
}

/// The most bytes of one tool result that a score input keeps.
const MAX_SCORED_TOOL_BYTES: usize = 16 * 1024;

/// The score request of one draw, or `None` when it does not fit the
/// activity input cap.
///
/// The request holds the tool results of the draw only. A transcript that
/// starts with the history of `task` drops the history first. Each tool
/// result is cut to [`MAX_SCORED_TOOL_BYTES`]. A request that still does
/// not fit would fail the run after the draws were paid.
fn score_request(
    sample: &OpSample,
    task: &AgentTask,
    report: &AgentReport,
) -> Option<ScoreRequest> {
    let mut score = ScoreInput::from_run(task, report);
    if !task.history.is_empty() && report.messages.starts_with(&task.history) {
        let fresh = report
            .messages
            .get(task.history.len()..)
            .unwrap_or_default();
        score.tools = crate::score::tool_records(fresh);
    }
    for tool in &mut score.tools {
        let content = std::mem::take(&mut tool.content);
        tool.content = cut_bytes(content, MAX_SCORED_TOOL_BYTES);
    }
    let request = ScoreRequest::new(sample.scorer.id.as_str(), score)
        .admitted_judges(sample.scorer.judge_model.clone());
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
    sample: &OpSample,
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
    } else if let Some(request) = score_request(sample, task, &report) {
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
        stop: report.stop,
        verdict,
        usage: report.usage.saturating_add(usage),
        cost_usd_micros: report.cost_usd_micros.saturating_add(cost),
        parked_until_ms: report.parked_until_ms,
    };
    Ok((Scored { record, report }, stop))
}

/// Whether `verdict` passes the check of `strategy`.
///
/// Best-of-N passes each score, and `select` never picks a reject.
/// Rejection passes a score at or above the threshold.
fn passes(strategy: SampleStrategy, verdict: &Verdict) -> bool {
    match strategy {
        SampleStrategy::BestOfN { .. } => !verdict.is_reject(),
        SampleStrategy::Rejection { threshold, .. } => {
            verdict.as_score().is_some_and(|score| score >= threshold)
        }
    }
}

/// The report of the run: the selection, each draw and the totals.
fn report(
    kind: &str,
    strategy: SampleStrategy,
    revision: u64,
    concurrency: u32,
    scored: Vec<Scored>,
    stopped: SampleStop,
) -> SampleReport {
    let passing: Vec<(DrawId, Verdict)> = scored
        .iter()
        .filter(|one| passes(strategy, &one.record.verdict))
        .map(|one| (one.record.draw, one.record.verdict.clone()))
        .collect();
    let chosen = select(&passing);
    let outcome = chosen.map_or(SampleOutcome::NoAcceptedDraw, |draw| {
        SampleOutcome::Selected { draw }
    });
    let usage = scored.iter().fold(TokenUsage::default(), |usage, one| {
        usage.saturating_add(one.record.usage)
    });
    let cost_usd_micros = scored.iter().fold(0_u64, |cost, one| {
        cost.saturating_add(one.record.cost_usd_micros)
    });
    let flow_labels = scored
        .iter()
        .flat_map(|one| one.report.flow_labels.iter().cloned())
        .collect();
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
        kind: kind.to_owned(),
        strategy,
        revision,
        concurrency,
        outcome,
        stopped,
        attempts,
        selected,
        draws,
        usage,
        cost_usd_micros,
        flow_labels,
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
        assert!(passes(strategy, &Verdict::score(0.5).unwrap()));
        assert!(!passes(strategy, &Verdict::score(0.49).unwrap()));
        assert!(!passes(strategy, &Verdict::reject("no")));
        let best = SampleStrategy::best_of_n(3);
        assert!(passes(best, &Verdict::score(-1e300).unwrap()));
        assert!(!passes(best, &Verdict::reject("no")));
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
        let sample = OpSample::new(
            SampleStrategy::best_of_n(1),
            ScorerRef::deterministic("exact_match").unwrap(),
        );
        let request = score_request(&sample, &AgentTask::new("x"), &report).unwrap();
        assert_eq!(request.input.tools.len(), 1);
        assert!(request.input.tools[0].content.len() <= MAX_SCORED_TOOL_BYTES);
        assert_eq!(
            request.admitted_judges,
            Some(std::collections::BTreeSet::new())
        );
    }

    #[test]
    fn a_score_input_drops_the_tool_results_of_the_history() {
        let turn = |id: &str, content: &str| {
            serde_json::json!([
                {"role": "assistant", "content": [
                    {"tool_call": {"id": id, "name": "read", "arguments": {}}}
                ]},
                {"role": "user", "content": [
                    {"tool_result": {"tool_call_id": id, "content": content}}
                ]}
            ])
        };
        let history: Vec<crate::message::ChatMessage> =
            serde_json::from_value(turn("old", "old result")).unwrap();
        let mut messages = history.clone();
        messages.extend(
            serde_json::from_value::<Vec<crate::message::ChatMessage>>(turn("new", "new result"))
                .unwrap(),
        );
        let report: AgentReport = serde_json::from_value(serde_json::json!({
            "stop": "completed",
            "text": "done",
            "steps_used": 1,
            "tool_calls": 1,
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "messages": messages,
        }))
        .unwrap();
        let sample = OpSample::new(
            SampleStrategy::best_of_n(1),
            ScorerRef::deterministic("exact_match").unwrap(),
        );
        let mut task = AgentTask::new("x");
        task.history = history;
        let request = score_request(&sample, &task, &report).unwrap();
        let contents: Vec<&str> = request
            .input
            .tools
            .iter()
            .map(|tool| tool.content.as_str())
            .collect();
        assert_eq!(contents, ["new result"]);
        let plain = score_request(&sample, &AgentTask::new("x"), &report).unwrap();
        assert_eq!(plain.input.tools.len(), 2, "no history keeps each result");
    }

    #[test]
    fn a_stop_code_is_its_wire_name() {
        for stop in [
            SampleStop::Completed,
            SampleStop::Accepted,
            SampleStop::BudgetExceeded,
            SampleStop::ScoreFailed,
        ] {
            assert_eq!(serde_json::json!(stop), serde_json::json!(stop.code()));
        }
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
