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
//! - Beam search runs one model turn per draw. Each round expands each live
//!   beam, scores each step and keeps the best beams with [`beam_rank`].
//!
//! The design is in `docs/design/DESIGN-2443.md` and
//! `docs/design/DESIGN-2446.md`.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use super::admit::{
    Admission, Ask, Refusal, SampleStep, admit, beam_draws, beam_slots, sample_step,
};
use super::draws::{draw_task, profile_task, read_policy, read_profile};
use super::policy::{ScorerRef, Strategy};
use autumn_harvest::llm_budget::LlmBudgetExceeded;

use crate::bounds::{exceeds_activity_input, sibling_cap};
use crate::draw::{DrawId, run_draw_tasks};
use crate::harness::cut_bytes;
use crate::mesh::valid_name;
use crate::message::{ChatMessage, ChatRole, TokenUsage};
use crate::score::{
    FiniteF64, SampleScore, ScoreInput, ScoreRequest, Verdict, agent_sample_score_info, select,
};
use crate::types::{AgentReport, AgentStop, AgentTask};

/// The workflow name of [`agent_sample`].
pub const SAMPLE_WORKFLOW_NAME: &str = "agent_sample";

/// The strategy of one `agent_sample` run.
///
/// It encodes as `{"best_of_n": {"n": 3}}`,
/// `{"rejection": {"max_attempts": 4, "threshold": 0.8}}` or
/// `{"beam": {"width": 2, "expansion": 2, "max_rounds": 3}}`.
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
    /// Step-level beam search. Each round expands each live beam with
    /// `expansion` model turns, scores each step and keeps the best
    /// `width` beams. The scorer of the input is the step scorer.
    Beam {
        /// The most beams that a round keeps.
        width: u32,
        /// The model turns that each live beam gets in a round.
        expansion: u32,
        /// The most rounds.
        max_rounds: u32,
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

    /// Beam search of `width` beams, `expansion` turns per beam and at
    /// most `max_rounds` rounds.
    #[must_use]
    pub const fn beam(width: u32, expansion: u32, max_rounds: u32) -> Self {
        Self::Beam {
            width,
            expansion,
            max_rounds,
        }
    }

    /// The policy strategy, such as `best_of_n`.
    #[must_use]
    pub const fn policy_strategy(self) -> Strategy {
        match self {
            Self::BestOfN { .. } => Strategy::BestOfN,
            Self::Rejection { .. } => Strategy::Rejection,
            Self::Beam { .. } => Strategy::Beam,
        }
    }

    /// The most draws of the run. For a beam, it is
    /// `width × expansion × max_rounds`, saturated at `u32::MAX`.
    #[must_use]
    pub const fn draws(self) -> u32 {
        match self {
            Self::BestOfN { n } => n,
            Self::Rejection { max_attempts, .. } => max_attempts,
            Self::Beam {
                width,
                expansion,
                max_rounds,
            } => beam_draws(width, expansion, max_rounds),
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
    /// for best-of-N and beam search, and 1 for rejection.
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
///
/// A beam run selects among its complete beams only.
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
    /// A beam run ended its last round with a live beam. The run selects
    /// among the beams that are complete.
    RoundsExhausted,
}

/// What a beam round did with one step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BeamFate {
    /// The round kept the step. The next round expands it, unless the run
    /// stops after this round.
    Live,
    /// The round kept the step, and its turn is a final answer. It leaves
    /// the pool.
    Complete,
    /// The round did not keep the step.
    Pruned,
}

/// The place of one step in a beam run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BeamStep {
    /// The round of the step, from 0. The first round expands the user
    /// message.
    pub round: u32,
    /// The step that this step extends. `None` in the first round.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<DrawId>,
    /// What the round did with the step.
    pub fate: BeamFate,
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
    /// The place of the step in a beam run. `None` for each other strategy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub beam: Option<BeamStep>,
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
    /// The draws that ran. For a beam, each step is a draw.
    pub attempts: u32,
    /// The report of the selected draw. For a beam, its transcript is the
    /// whole path, but its usage and counts cover its last step only. The
    /// totals of the run are on this report.
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
    let ask = Ask {
        strategy: input.strategy.policy_strategy(),
        draws: input.strategy.draws(),
        concurrency: input
            .concurrency
            .map_or_else(|| default_concurrency(input.strategy), NonZeroU32::get),
        scorer: Some(&input.scorer),
    };
    let admission = admit(&read.policy, &ask).map_err(|refusal| refusal.error_text())?;
    let task = draw_task(
        &profile_task(&profile, &input.input),
        &admission.draw_effects,
    );
    let budget = tree_budget(task.max_total_tokens, input.max_total_tokens);
    let (scored, stopped) = match input.strategy {
        SampleStrategy::Beam {
            width,
            expansion,
            max_rounds,
        } => {
            let shape = BeamShape {
                width,
                expansion,
                max_rounds,
            };
            run_beam(ctx, &input, &task, &admission, budget, shape).await?
        }
        _ => run_waves(ctx, &input, &task, &admission, budget).await?,
    };
    Ok(report(
        input,
        read.revision,
        admission.concurrency,
        scored,
        stopped,
    ))
}

/// The concurrency of a run that asks for none.
///
/// Best-of-N takes the cap of the policy (0). Rejection stops at the first
/// pass, so it runs one draw at a time. A wider wave would pay for draws
/// after the pass.
const fn default_concurrency(strategy: SampleStrategy) -> u32 {
    match strategy {
        SampleStrategy::BestOfN { .. } | SampleStrategy::Beam { .. } => 0,
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
            passed |= passes(input.strategy, &one.record.verdict);
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

/// The bounds of a beam run.
#[derive(Debug, Clone, Copy)]
struct BeamShape {
    width: u32,
    expansion: u32,
    max_rounds: u32,
}

/// One live beam: the step that ends it, its transcript and the flow
/// labels that it exercised.
struct Beam {
    draw: Option<DrawId>,
    messages: Vec<ChatMessage>,
    flow_labels: BTreeSet<String>,
}

/// Run the rounds of a beam run, and score each step.
///
/// The first round has one beam: the user message. Each round expands each live
/// beam `expansion` times, in the order of the live beams. Each step is a
/// child run in step mode. It sends the beam as its history and asks for
/// one new model turn. So a kept beam pays only for its new turn.
///
/// The steps of a round run in waves of [`sample_step`], under the token
/// budget of the run tree. After the round, [`beam_rank`] keeps the best
/// steps, at most `width` less the complete beams. A kept step with a final
/// answer is complete. Each other kept step is live in the next round.
///
/// The run stops when `width` beams are complete or no beam is live. It
/// stops with `rounds_exhausted` after `max_rounds` with a live beam. A
/// budget refusal or a failed score stops it after the round is ranked.
async fn run_beam(
    ctx: &WorkflowContext,
    input: &SampleInput,
    task: &AgentTask,
    admission: &Admission,
    budget: Option<u32>,
    shape: BeamShape,
) -> Result<(Vec<Scored>, SampleStop), String> {
    let mut scored: Vec<Scored> = Vec::new();
    let mut spent: u32 = 0;
    let mut complete: u32 = 0;
    let mut live = vec![Beam {
        draw: None,
        messages: vec![ChatMessage::text(ChatRole::User, input.input.clone())],
        flow_labels: task.flow_labels.clone(),
    }];
    for round in 0..shape.max_rounds {
        let slots = beam_slots(shape.width, complete);
        if slots == 0 || live.is_empty() {
            return Ok((scored, SampleStop::Completed));
        }
        let parents: Vec<&Beam> = live
            .iter()
            .flat_map(|beam| std::iter::repeat_n(beam, shape.expansion as usize))
            .collect();
        let first = scored.len();
        let halt = Box::pin(run_steps(
            ctx,
            Steps {
                input,
                task,
                admission,
                budget,
                round,
                parents: &parents,
            },
            &mut scored,
            &mut spent,
        ))
        .await?;
        live = keep_beams(&mut scored[first..], slots, &mut complete);
        if let Some(stop) = halt {
            return Ok((scored, stop));
        }
    }
    let stop = if beam_slots(shape.width, complete) == 0 || live.is_empty() {
        SampleStop::Completed
    } else {
        SampleStop::RoundsExhausted
    };
    Ok((scored, stop))
}

/// The steps of one beam round.
struct Steps<'a> {
    input: &'a SampleInput,
    task: &'a AgentTask,
    admission: &'a Admission,
    budget: Option<u32>,
    round: u32,
    /// The beam of each step, in step order.
    parents: &'a [&'a Beam],
}

/// Run the steps of one round in waves, score each, and add each to
/// `scored` as pruned. Returns the stop of a budget refusal or of a failed
/// score.
///
/// Step `n` of the run has the draw id `n`. So each step has its own draw
/// id, run id and cache key, also when two steps share a beam.
async fn run_steps(
    ctx: &WorkflowContext,
    steps: Steps<'_>,
    scored: &mut Vec<Scored>,
    spent: &mut u32,
) -> Result<Option<SampleStop>, String> {
    let planned = u32::try_from(steps.parents.len()).unwrap_or(u32::MAX);
    let mut started: u32 = 0;
    loop {
        let left = steps.budget.map_or(0, |max| max.saturating_sub(*spent));
        let step = sample_step(
            planned,
            started,
            steps.admission.concurrency,
            steps.budget.is_some(),
            left,
        );
        let width = match step {
            SampleStep::Done => return Ok(None),
            SampleStep::BudgetExceeded => return Ok(Some(SampleStop::BudgetExceeded)),
            SampleStep::Wave { width } => width,
        };
        let base = u32::try_from(scored.len()).unwrap_or(u32::MAX);
        let wave = &steps.parents[started as usize..(started + width) as usize];
        let tasks = (0..width)
            .zip(wave)
            .map(|(index, parent)| {
                let mut one = step_task(steps.task, parent, steps.round);
                if steps.budget.is_some() {
                    one.max_total_tokens = Some(sibling_cap(left, width, index));
                }
                (DrawId::new(base + index), one)
            })
            .collect();
        let ended = run_draw_tasks(ctx, tasks).await?;
        let mut halt = None;
        for ((id, report), parent) in (base..).zip(ended).zip(wave) {
            let report = report?;
            if report.stop == AgentStop::BudgetExceeded {
                halt = halt.or(Some(SampleStop::BudgetExceeded));
            }
            let draw = DrawId::new(id);
            let (mut one, halted) =
                score_draw(ctx, steps.input, steps.task, draw, report, halt).await?;
            halt = halt.or(halted);
            one.record.beam = Some(BeamStep {
                round: steps.round,
                parent: parent.draw,
                fate: BeamFate::Pruned,
            });
            *spent = spent.saturating_add(one.record.usage.total());
            scored.push(one);
        }
        if halt.is_some() {
            return Ok(halt);
        }
        started += width;
    }
}

/// The task of one beam step in `round`: `task` in step mode, with `beam`
/// as its history.
///
/// A step continues the run of its beam, so it also continues its flow
/// labels and its step count. A beam of round `r` holds `r` tool rounds.
/// So the step may run `max_steps - r` tool rounds, as one run of the task
/// would. With none left, the model may still answer.
///
/// A step makes one model call. It has no context reset, because a reset
/// rewrites the beam. It has no answer check, because a rejected answer
/// asks the model again.
fn step_task(task: &AgentTask, beam: &Beam, round: u32) -> AgentTask {
    AgentTask {
        history: beam.messages.clone(),
        flow_labels: beam.flow_labels.clone(),
        max_steps: task.max_steps.saturating_sub(round),
        beam_step: true,
        answer_check: None,
        context_reset: None,
        ..task.clone()
    }
}

/// Keep the best steps of one round, and return the live beams.
///
/// A step can stay only with a score. It is complete when its turn is a
/// final answer. `complete` counts the complete beams of the run.
fn keep_beams(round: &mut [Scored], slots: u32, complete: &mut u32) -> Vec<Beam> {
    let ranked: Vec<(DrawId, Verdict)> = round
        .iter()
        .filter(|one| matches!(one.record.stop, AgentStop::Completed | AgentStop::StepEnded))
        .map(|one| (one.record.draw, one.record.verdict.clone()))
        .collect();
    let mut live = Vec::new();
    for draw in beam_rank(&ranked, slots) {
        let Some(one) = round.iter_mut().find(|one| one.record.draw == draw) else {
            continue;
        };
        let complete_now = one.record.stop == AgentStop::Completed;
        if let Some(beam) = one.record.beam.as_mut() {
            beam.fate = if complete_now {
                BeamFate::Complete
            } else {
                BeamFate::Live
            };
        }
        if complete_now {
            *complete = complete.saturating_add(1);
        } else {
            live.push(Beam {
                draw: Some(draw),
                messages: one.report.messages.clone(),
                flow_labels: one.report.flow_labels.clone(),
            });
        }
    }
    live
}

/// The best `keep` draws of `verdicts`, best first.
///
/// It calls [`select`] once per kept draw, over the draws that are left.
/// So a tie goes to the lowest draw id, and a reject is never kept.
#[must_use]
pub fn beam_rank(verdicts: &[(DrawId, Verdict)], keep: u32) -> Vec<DrawId> {
    let mut left: Vec<(DrawId, Verdict)> = verdicts.to_vec();
    let mut kept = Vec::new();
    while u32::try_from(kept.len()).unwrap_or(u32::MAX) < keep {
        let Some(best) = select(&left) else {
            break;
        };
        left.retain(|(draw, _)| *draw != best);
        kept.push(best);
    }
    kept
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
    let beam = matches!(input.strategy, SampleStrategy::Beam { .. });
    let (verdict, usage, cost) = if report.stop == AgentStop::Failed {
        unscored("the draw failed, so the run did not score it")
    } else if report.stop == AgentStop::BudgetExceeded {
        unscored("a budget stopped the draw, so the run did not score it")
    } else if beam && !matches!(report.stop, AgentStop::Completed | AgentStop::StepEnded) {
        unscored("the step did not end with an answer or a tool round")
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
        beam: None,
    };
    Ok((Scored { record, report }, stop))
}

/// Whether `verdict` passes the check of `strategy`.
///
/// Best-of-N and beam search pass each score, and `select` never picks a
/// reject. Rejection passes a score at or above the threshold.
fn passes(strategy: SampleStrategy, verdict: &Verdict) -> bool {
    match strategy {
        SampleStrategy::BestOfN { .. } | SampleStrategy::Beam { .. } => !verdict.is_reject(),
        SampleStrategy::Rejection { threshold, .. } => {
            verdict.as_score().is_some_and(|score| score >= threshold)
        }
    }
}

/// Whether `record` may be selected. A beam step must be a complete beam.
/// Each draw of another strategy may be.
fn selectable(record: &SampleDraw) -> bool {
    record
        .beam
        .is_none_or(|beam| beam.fate == BeamFate::Complete)
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
        .filter(|one| passes(input.strategy, &one.record.verdict) && selectable(&one.record))
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
        concurrency,
        outcome,
        stopped,
        attempts,
        selected,
        draws,
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

    fn ids(ranked: &[DrawId]) -> Vec<u32> {
        ranked.iter().map(|draw| draw.get()).collect()
    }

    #[test]
    fn a_beam_rank_keeps_the_top_scores_and_never_a_reject() {
        let score = |value| Verdict::score(value).unwrap();
        let entries = [
            (DrawId::new(0), score(0.4)),
            (DrawId::new(1), score(0.9)),
            (DrawId::new(2), Verdict::reject("no")),
            (DrawId::new(3), score(0.9)),
        ];
        assert_eq!(
            ids(&beam_rank(&entries, 2)),
            [1, 3],
            "a tie goes to the lower id"
        );
        assert_eq!(ids(&beam_rank(&entries, 9)), [1, 3, 0]);
        assert_eq!(beam_rank(&entries, 0), Vec::<DrawId>::new());
    }

    proptest::proptest! {
        #[test]
        fn a_beam_rank_is_the_sorted_prefix(
            scores in proptest::collection::vec(proptest::option::of(-3_i8..=3), 0..=12),
            keep in 0_u32..=14,
        ) {
            let entries: Vec<(DrawId, Verdict)> = scores
                .iter()
                .enumerate()
                .map(|(i, score)| {
                    let verdict = score.map_or_else(
                        || Verdict::reject("no"),
                        |value| Verdict::score(f64::from(value)).unwrap(),
                    );
                    (DrawId::new(u32::try_from(i).unwrap()), verdict)
                })
                .rev()
                .collect();
            let mut sorted: Vec<(i8, u32)> = scores
                .iter()
                .enumerate()
                .filter_map(|(i, score)| score.map(|v| (v, u32::try_from(i).unwrap())))
                .collect();
            sorted.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            let want: Vec<u32> = sorted
                .into_iter()
                .take(keep as usize)
                .map(|(_, id)| id)
                .collect();
            proptest::prop_assert_eq!(ids(&beam_rank(&entries, keep)), want);
        }
    }

    /// A pruned step of the second round with `stop` and `verdict`.
    fn step(draw: u32, stop: AgentStop, verdict: Verdict) -> Scored {
        let report: AgentReport = serde_json::from_value(serde_json::json!({
            "stop": stop,
            "text": "",
            "steps_used": 1,
            "tool_calls": 1,
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "messages": [],
            "flow_labels": ["seen"],
        }))
        .unwrap();
        let record = SampleDraw {
            draw: DrawId::new(draw),
            run_id: None,
            stop,
            verdict,
            usage: TokenUsage::default(),
            cost_usd_micros: 0,
            beam: Some(BeamStep {
                round: 1,
                parent: None,
                fate: BeamFate::Pruned,
            }),
        };
        Scored { record, report }
    }

    #[test]
    fn keep_beams_fills_the_slots_by_score_and_counts_the_complete() {
        let score = |value| Verdict::score(value).unwrap();
        let mut round = vec![
            step(0, AgentStop::StepEnded, score(0.2)),
            step(1, AgentStop::Completed, score(0.9)),
            step(2, AgentStop::StepEnded, score(0.5)),
            step(3, AgentStop::StepsExhausted, score(1.0)),
            step(4, AgentStop::Completed, Verdict::reject("no")),
        ];
        let mut complete = 1;
        let live = keep_beams(&mut round, 2, &mut complete);

        assert_eq!(complete, 2, "draw 1 completes");
        let ids: Vec<Option<u32>> = live.iter().map(|beam| beam.draw.map(DrawId::get)).collect();
        assert_eq!(ids, [Some(2)], "a step with another stop is never kept");
        assert!(live[0].flow_labels.contains("seen"));
        let fates: Vec<BeamFate> = round
            .iter()
            .map(|one| one.record.beam.unwrap().fate)
            .collect();
        assert_eq!(
            fates,
            [
                BeamFate::Pruned,
                BeamFate::Complete,
                BeamFate::Live,
                BeamFate::Pruned,
                BeamFate::Pruned,
            ]
        );
        assert!(keep_beams(&mut round, 0, &mut complete).is_empty());
    }

    #[test]
    fn the_beam_strategy_defaults_to_the_concurrency_cap() {
        assert_eq!(default_concurrency(SampleStrategy::beam(2, 2, 2)), 0);
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
