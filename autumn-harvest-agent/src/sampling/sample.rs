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
//!
//! The design is in `docs/design/DESIGN-2443.md`.

use std::num::NonZeroU32;

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use super::admit::{Admission, Ask, Refusal, SampleStep, admit, admit_single, sample_step};
use super::draws::{draw_task, profile_task, read_policy, read_profile_versioned};
use super::policy::{SamplingPolicy, ScorerRef, Strategy};
use autumn_harvest::llm_budget::LlmBudgetExceeded;

use crate::agent::overlay_store::OverlayVersion;
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

/// The strategy of one `agent_sample` run.
///
/// It encodes as `"single"`, `{"best_of_n": {"n": 3}}` or
/// `{"rejection": {"max_attempts": 4, "threshold": 0.8}}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum SampleStrategy {
    /// Run one draw and score it. The policy needs no strategy for it,
    /// because the default policy makes each run a single draw.
    Single,
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
    /// One draw.
    #[must_use]
    pub const fn single() -> Self {
        Self::Single
    }

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

    /// The policy strategy, such as `best_of_n`. `None` for a single
    /// draw, which needs no strategy in the policy.
    #[must_use]
    pub const fn policy_strategy(self) -> Option<Strategy> {
        match self {
            Self::Single => None,
            Self::BestOfN { .. } => Some(Strategy::BestOfN),
            Self::Rejection { .. } => Some(Strategy::Rejection),
        }
    }

    /// The most draws of the run.
    #[must_use]
    pub const fn draws(self) -> u32 {
        match self {
            Self::Single => 1,
            Self::BestOfN { n } => n,
            Self::Rejection { max_attempts, .. } => max_attempts,
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
    /// for best-of-N, and 1 for rejection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<NonZeroU32>,
    /// The token budget of the whole run tree. It narrows the cap of the
    /// profile, and never raises it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_total_tokens: Option<u32>,
    /// The expected answer. Each score input carries it, so a scorer such
    /// as [`ExactMatch::per_item`](crate::ExactMatch::per_item) can compare
    /// with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    /// Drop the transcript of the selected draw from the report. A sweep
    /// sets it, so each child result in its history stays short.
    #[serde(default, skip_serializing_if = "crate::types::is_false")]
    pub brief: bool,
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
            expected: None,
            brief: false,
        }
    }

    /// Give each score input the expected answer `expected`.
    #[must_use]
    pub fn expected(mut self, expected: impl Into<String>) -> Self {
        self.expected = Some(expected.into());
        self
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
    /// The overlay version of the profile that built each draw. `None` is
    /// the bare profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_version: Option<OverlayVersion>,
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
    /// Whether an LLM budget refused a model call or a judge call of the
    /// run. A spent token cap does not set it.
    #[serde(default, skip_serializing_if = "crate::types::is_false")]
    pub refused: bool,
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
    let Some((profile, overlay_version)) = read_profile_versioned(ctx, &input.kind).await? else {
        return Err(Refusal::UnknownKind.error_text());
    };
    let read = read_policy(ctx, &input.kind).await?;
    let admission = admit_sample(&read.policy, &input).map_err(|refusal| refusal.error_text())?;
    let task = draw_task(
        &profile_task(&profile, &input.input),
        &admission.draw_effects,
    );
    let budget = tree_budget(task.max_total_tokens, input.max_total_tokens);
    let brief = input.brief;
    let (scored, stopped, refused) = run_waves(ctx, &input, &task, &admission, budget).await?;
    let mut report = report(input, read.revision, admission.concurrency, scored, stopped);
    report.overlay_version = overlay_version;
    report.refused = refused;
    if brief && let Some(selected) = &mut report.selected {
        selected.messages.clear();
    }
    Ok(report)
}

/// Compare the strategy, the concurrency and the scorer of `input` with
/// `policy`.
///
/// A single draw needs no strategy in the policy. It still meets the
/// concurrency and the scorer checks.
///
/// # Errors
///
/// Returns the first [`Refusal`].
pub(super) fn admit_sample(
    policy: &SamplingPolicy,
    input: &SampleInput,
) -> Result<Admission, Refusal> {
    let concurrency = input
        .concurrency
        .map_or_else(|| default_concurrency(input.strategy), NonZeroU32::get);
    input.strategy.policy_strategy().map_or_else(
        || admit_single(policy, concurrency, Some(&input.scorer)),
        |strategy| {
            let ask = Ask {
                strategy,
                draws: input.strategy.draws(),
                concurrency,
                scorer: Some(&input.scorer),
            };
            admit(policy, &ask)
        },
    )
}

/// The concurrency of a run that asks for none.
///
/// Best-of-N takes the cap of the policy (0). Rejection stops at the first
/// pass, so it runs one draw at a time. A wider wave would pay for draws
/// after the pass. A single draw runs alone.
const fn default_concurrency(strategy: SampleStrategy) -> u32 {
    match strategy {
        SampleStrategy::BestOfN { .. } => 0,
        SampleStrategy::Single | SampleStrategy::Rejection { .. } => 1,
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
///
/// The flag is `true` when an LLM budget refused a model call or a judge
/// call. A spent token cap does not set it.
async fn run_waves(
    ctx: &WorkflowContext,
    input: &SampleInput,
    task: &AgentTask,
    admission: &Admission,
    budget: Option<u32>,
) -> Result<(Vec<Scored>, SampleStop, bool), String> {
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
            SampleStep::Done => return Ok((scored, SampleStop::Completed, false)),
            SampleStep::BudgetExceeded => return Ok((scored, SampleStop::BudgetExceeded, false)),
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
        // The first stop names the halt. A refusal counts on its own, so a
        // refusal after a failed score still stops a sweep.
        let mut refused = false;
        for (id, report) in (started..).zip(ended) {
            let report = report?;
            let draw_stop = report.stop;
            if draw_stop == AgentStop::BudgetExceeded {
                halt = halt.or(Some(SampleStop::BudgetExceeded));
            }
            let (one, halted) = score_draw(ctx, input, task, DrawId::new(id), report, halt).await?;
            refused |= is_refusal(draw_stop, halted);
            halt = halt.or(halted);
            passed |= passes(input.strategy, &one.record.verdict);
            spent = spent.saturating_add(one.record.usage.total());
            scored.push(one);
        }
        if passed && matches!(input.strategy, SampleStrategy::Rejection { .. }) {
            return Ok((scored, SampleStop::Accepted, refused));
        }
        if let Some(stop) = halt {
            return Ok((scored, stop, refused));
        }
    }
}

/// Whether an LLM budget refused the draw that ended with `draw_stop`, or
/// its judge call. `halted` is the stop that its score gave.
///
/// Only an LLM budget ends a draw with `budget_exceeded`. A spent token
/// cap ends it with `tokens_exhausted`.
const fn is_refusal(draw_stop: AgentStop, halted: Option<SampleStop>) -> bool {
    matches!(draw_stop, AgentStop::BudgetExceeded)
        || matches!(halted, Some(SampleStop::BudgetExceeded))
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
    score.expected.clone_from(&input.expected);
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
        stop: report.stop,
        verdict,
        usage: report.usage.saturating_add(usage),
        cost_usd_micros: report.cost_usd_micros.saturating_add(cost),
    };
    Ok((Scored { record, report }, stop))
}

/// Whether `verdict` passes the check of `strategy`.
///
/// Best-of-N passes each score, and `select` never picks a reject.
/// Rejection passes a score at or above the threshold.
fn passes(strategy: SampleStrategy, verdict: &Verdict) -> bool {
    match strategy {
        SampleStrategy::Single | SampleStrategy::BestOfN { .. } => !verdict.is_reject(),
        SampleStrategy::Rejection { threshold, .. } => {
            verdict.as_score().is_some_and(|score| score >= threshold)
        }
    }
}

/// The report of the run: the selection, each draw and the totals.
fn report(
    input: SampleInput,
    revision: u64,
    concurrency: u32,
    scored: Vec<Scored>,
    stopped: SampleStop,
) -> SampleReport {
    let passing: Vec<(DrawId, Verdict)> = scored
        .iter()
        .filter(|one| passes(input.strategy, &one.record.verdict))
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
        overlay_version: None,
        concurrency,
        outcome,
        stopped,
        attempts,
        selected,
        draws,
        refused: false,
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
            Some(Strategy::BestOfN)
        );
        assert_eq!(SampleStrategy::best_of_n(4).draws(), 4);
        let rejection = SampleStrategy::rejection(5, FiniteF64::ONE);
        assert_eq!(rejection.policy_strategy(), Some(Strategy::Rejection));
        assert_eq!(rejection.draws(), 5);
        assert_eq!(SampleStrategy::single().policy_strategy(), None);
        assert_eq!(SampleStrategy::single().draws(), 1);
        assert_eq!(
            serde_json::to_value(SampleStrategy::single()).unwrap(),
            serde_json::json!("single")
        );
    }

    fn single(scorer: &str) -> SampleInput {
        SampleInput::new(
            "k",
            "x",
            SampleStrategy::single(),
            ScorerRef::deterministic(scorer).unwrap(),
        )
    }

    #[test]
    fn a_refusal_counts_after_another_stop_in_its_wave() {
        // A failed score halts the wave first. A later refused draw still
        // counts, because the flag folds each draw on its own.
        let draws = [
            (AgentStop::Completed, Some(SampleStop::ScoreFailed)),
            (AgentStop::BudgetExceeded, None),
        ];
        let refused = draws.iter().fold(false, |acc, (stop, halted)| {
            acc | is_refusal(*stop, *halted)
        });
        assert!(refused);
        assert!(is_refusal(
            AgentStop::Completed,
            Some(SampleStop::BudgetExceeded)
        ));
        assert!(!is_refusal(AgentStop::TokensExhausted, None));
        assert!(!is_refusal(
            AgentStop::Completed,
            Some(SampleStop::ScoreFailed)
        ));
    }

    #[test]
    fn a_single_draw_needs_no_strategy_in_the_policy() {
        let admission = admit_sample(&SamplingPolicy::default(), &single("exact_match")).unwrap();
        assert_eq!((admission.draws, admission.concurrency), (1, 1));
        let judge = SampleInput {
            scorer: ScorerRef::judge("judge", "m").unwrap(),
            ..single("exact_match")
        };
        assert_eq!(
            admit_sample(&SamplingPolicy::default(), &judge)
                .unwrap_err()
                .code(),
            "scorer_not_allowed",
            "the scorer check still applies"
        );
        let wide = single("exact_match").concurrency(2);
        assert_eq!(
            admit_sample(&SamplingPolicy::default(), &wide)
                .unwrap_err()
                .code(),
            "too_many_concurrent_draws"
        );
    }

    #[test]
    fn a_score_request_carries_the_expected_answer() {
        let report: AgentReport = serde_json::from_value(serde_json::json!({
            "stop": "completed", "text": "4", "steps_used": 1, "tool_calls": 0,
            "usage": {"input_tokens": 1, "output_tokens": 1}, "messages": [],
        }))
        .unwrap();
        let input = single("exact_match").expected("4");
        let request = score_request(&input, &AgentTask::new("x"), &report).unwrap();
        assert_eq!(request.input.expected.as_deref(), Some("4"));
    }
}
