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
//! - A vote keys each draw with an [`AnswerKey`], and needs no scorer. It
//!   stops after the first wave whose leader cannot lose.
//! - A cascade asks one tier at a time, each with its own model. It stops
//!   at the first tier that accepts. [`cascade_step`] skips each tier that
//!   the mandate forbids.
//! - Beam search runs one model turn per draw. Each round expands each live
//!   beam, scores each step and keeps the best beams with [`beam_rank`].
//!
//! The design is in `docs/design/DESIGN-2443.md`,
//! `docs/design/DESIGN-2444.md`, `docs/design/DESIGN-2445.md` and
//! `docs/design/DESIGN-2446.md`.

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use super::admit::{
    Admission, Ask, CascadeStep, Refusal, SampleStep, admit, admit_tiers, beam_draws, beam_slots,
    cascade_step, sample_step,
};
use super::draws::{draw_task, profile_task, read_policy, read_profile};
use super::policy::{ScorerRef, Strategy};
use super::vote::{
    AnswerKey, Tally, VoteCount, VoteKeyReply, VoteKeyRequest, agent_vote_key_info, fit_key,
};
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
/// `{"vote": {"n": 5, "key": "last_number"}}`,
/// `{"cascade": {"tiers": [{"model": "small", "threshold": 0.8}]}}` or
/// `{"beam": {"width": 2, "expansion": 2, "max_rounds": 3}}`.
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
    /// Run up to `n` draws, key each answer, and select the most common
    /// key. A tie goes to the key whose first vote has the lowest draw id.
    Vote {
        /// The most draws.
        n: u32,
        /// How each answer maps to its key.
        key: AnswerKey,
    },
    /// Ask each tier in order, one draw per tier. Stop at the first answer
    /// that scores at or above the threshold of its tier.
    Cascade {
        /// The tiers, the cheapest first.
        tiers: Vec<Tier>,
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

    /// A vote of up to `n` draws under `key`.
    #[must_use]
    pub const fn vote(n: u32, key: AnswerKey) -> Self {
        Self::Vote { n, key }
    }

    /// A cascade over `tiers`, the cheapest first.
    #[must_use]
    pub fn cascade(tiers: impl IntoIterator<Item = Tier>) -> Self {
        Self::Cascade {
            tiers: tiers.into_iter().collect(),
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
    pub const fn policy_strategy(&self) -> Strategy {
        match self {
            Self::BestOfN { .. } => Strategy::BestOfN,
            Self::Rejection { .. } => Strategy::Rejection,
            Self::Vote { .. } => Strategy::Vote,
            Self::Cascade { .. } => Strategy::Cascade,
            Self::Beam { .. } => Strategy::Beam,
        }
    }

    /// The most draws of the run. A cascade has one draw per tier. For a
    /// beam, it is `width × expansion × max_rounds`, saturated at
    /// `u32::MAX`.
    #[must_use]
    pub fn draws(&self) -> u32 {
        match self {
            Self::BestOfN { n } | Self::Vote { n, .. } => *n,
            Self::Rejection { max_attempts, .. } => *max_attempts,
            Self::Cascade { tiers } => u32::try_from(tiers.len()).unwrap_or(u32::MAX),
            Self::Beam {
                width,
                expansion,
                max_rounds,
            } => beam_draws(*width, *expansion, *max_rounds),
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
    /// The scorer. The policy must allow it. Best-of-N and rejection need
    /// one. A vote takes none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scorer: Option<ScorerRef>,
    /// The most draws at the same time. `None` takes the cap of the policy
    /// for best-of-N, a vote and beam search, and 1 for rejection and for
    /// a cascade. A cascade refuses a value above 1.
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
            scorer: Some(scorer),
            concurrency: None,
            max_total_tokens: None,
        }
    }

    /// A vote of up to `n` draws over `input` for the kind `kind`. Each
    /// answer maps to its key with `key`.
    #[must_use]
    pub fn vote(kind: impl Into<String>, input: impl Into<String>, n: u32, key: AnswerKey) -> Self {
        Self {
            kind: kind.into(),
            input: input.into(),
            strategy: SampleStrategy::vote(n, key),
            scorer: None,
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
    /// The run selected this draw. For a vote, it is the first draw of
    /// the winning key.
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
    /// No draw passed. For a vote, no draw had a key.
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
    /// A score or a vote key failed, so the run started no more draws.
    /// The verdict of the draw names the error.
    ScoreFailed,
    /// The leader of a vote keeps the lead whatever the draws left vote,
    /// so the run started no more draws.
    Settled,
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
    /// The harness model of a cascade tier. `None` for each other
    /// strategy, so its report keeps its bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Why the draw run ended.
    pub stop: AgentStop,
    /// The verdict of the scorer. For a vote, a draw with a key gets a
    /// score of 1, and a draw with no key gets a reject with the reason.
    pub verdict: Verdict,
    /// The key of the answer, for a vote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
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
    /// The votes of each key, the winner first. Empty unless the run is a
    /// vote.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tally: Vec<VoteCount>,
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
    let voting = matches!(input.strategy, SampleStrategy::Vote { .. });
    let concurrency = input
        .concurrency
        .map_or_else(|| default_concurrency(&input.strategy), NonZeroU32::get);
    let ask = Ask {
        strategy: input.strategy.policy_strategy(),
        draws: input.strategy.draws(),
        concurrency,
        // A vote refuses a scorer below, so the policy never checks one.
        scorer: if voting { None } else { input.scorer.as_ref() },
    };
    let admission = admit(&read.policy, &ask).map_err(|refusal| refusal.error_text())?;
    let judge = judge_of(&input).map_err(|refusal| refusal.error_text())?;
    if let Judge::Vote(key @ AnswerKey::Custom(id)) = judge {
        probe_key(ctx, key, id).await?;
    }
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
    let ended = match (&input.strategy, judge) {
        (SampleStrategy::Cascade { tiers }, Judge::Score(rule)) => {
            run_tiers(ctx, &input, rule, tiers, &task, budget).await?
        }
        (
            SampleStrategy::Beam {
                width,
                expansion,
                max_rounds,
            },
            Judge::Score(rule),
        ) => {
            let shape = BeamShape {
                width: *width,
                expansion: *expansion,
                max_rounds: *max_rounds,
            };
            let run = BeamRun {
                input: &input,
                scorer: rule,
                task: &task,
                admission: &admission,
                budget,
            };
            let (scored, stopped) = run_beam(ctx, run, shape).await?;
            Ended {
                scored,
                skipped: Vec::new(),
                stopped,
            }
        }
        _ => {
            let (scored, stopped) =
                run_waves(ctx, judge, &input, &task, &admission, budget).await?;
            Ended {
                scored,
                skipped: Vec::new(),
                stopped,
            }
        }
    };
    Ok(report(input, read.revision, admission.concurrency, ended))
}

/// The concurrency of a run that asks for none.
///
/// Best-of-N, a vote and beam search take the cap of the policy (0).
/// Rejection and a cascade stop at the first pass, so they run one draw at
/// a time. A wider wave would pay for draws after the pass.
const fn default_concurrency(strategy: &SampleStrategy) -> u32 {
    match strategy {
        SampleStrategy::BestOfN { .. }
        | SampleStrategy::Vote { .. }
        | SampleStrategy::Beam { .. } => 0,
        SampleStrategy::Rejection { .. } | SampleStrategy::Cascade { .. } => 1,
    }
}

/// How the run judges each draw.
#[derive(Debug, Clone, Copy)]
enum Judge<'a> {
    /// Score each draw with this scorer.
    Score(&'a ScorerRef),
    /// Key each draw for a vote.
    Vote(&'a AnswerKey),
}

/// The judge of `input`.
///
/// # Errors
///
/// Best-of-N and rejection with no scorer give `NoScorer`. A vote with a
/// scorer gives `ScorerNotUsed`, and a vote with a bad key gives
/// `InvalidAnswerKey`.
fn judge_of(input: &SampleInput) -> Result<Judge<'_>, Refusal> {
    match (&input.strategy, &input.scorer) {
        (SampleStrategy::Vote { .. }, Some(_)) => Err(Refusal::ScorerNotUsed),
        (SampleStrategy::Vote { key, .. }, None) if key.is_valid() => Ok(Judge::Vote(key)),
        (SampleStrategy::Vote { .. }, None) => Err(Refusal::InvalidAnswerKey),
        (_, Some(scorer)) => Ok(Judge::Score(scorer)),
        (strategy, None) => Err(Refusal::NoScorer {
            strategy: strategy.policy_strategy(),
        }),
    }
}

/// Check that the harness has the app key function `id`, before the first
/// draw.
///
/// # Errors
///
/// Returns the `unknown_answer_key` refusal when the harness lacks it, and
/// an engine error as text.
async fn probe_key(ctx: &WorkflowContext, key: &AnswerKey, id: &str) -> Result<(), String> {
    let reply: VoteKeyReply = ctx
        .execute_activity(
            &agent_vote_key_info(),
            VoteKeyRequest::new(key.clone(), None),
        )
        .await
        .map_err(|err| err.to_string())?;
    if reply.known {
        return Ok(());
    }
    Err(Refusal::UnknownAnswerKey {
        key: crate::harness::truncate(id, crate::mesh::MAX_NAME_LEN),
    }
    .error_text())
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
    /// The delegations of the beam that a beam step extends. 0 for a draw.
    parent_delegations: u32,
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
/// The run starts no more draws after a rejection wave with a pass, and
/// after a vote wave whose leader cannot lose. It also stops after a
/// failed score, and when a budget refuses a draw or a judge call. A pass
/// wins over the other stops, because the run has its answer. A settled
/// vote wins over a budget stop. A failed key wins over a settled vote, so
/// the stop names the broken key.
async fn run_waves(
    ctx: &WorkflowContext,
    judge: Judge<'_>,
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
            let draw = DrawId::new(id);
            let (one, halted) = match judge {
                Judge::Score(rule) => {
                    score_draw(ctx, rule, false, task, draw, report, halt).await?
                }
                Judge::Vote(key) => vote_draw(ctx, key, draw, report).await?,
            };
            halt = merge_stop(halt, halted);
            passed |= passes(&input.strategy, one.record.draw, &one.record.verdict);
            spent = spent.saturating_add(one.record.usage.total());
            scored.push(one);
        }
        if passed && matches!(input.strategy, SampleStrategy::Rejection { .. }) {
            return Ok((scored, SampleStop::Accepted));
        }
        let ran = u32::try_from(scored.len()).unwrap_or(u32::MAX);
        let left = admission.draws.saturating_sub(ran);
        let decided = matches!(judge, Judge::Vote(_)) && halt != Some(SampleStop::ScoreFailed);
        if decided && left > 0 && tally_of(&scored).settled(left) {
            return Ok((scored, SampleStop::Settled));
        }
        if let Some(stop) = halt {
            return Ok((scored, stop));
        }
    }
}

/// The stop of a wave after a draw adds the stop `next`.
///
/// The first stop holds, except that a failed score or key wins over each
/// other stop. So a broken key is never hidden behind a budget stop.
fn merge_stop(halt: Option<SampleStop>, next: Option<SampleStop>) -> Option<SampleStop> {
    match next {
        Some(SampleStop::ScoreFailed) => next,
        _ => halt.or(next),
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
    rule: &ScorerRef,
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
        let (mut one, halted) = score_draw(ctx, rule, false, &draw, id, report, halt).await?;
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

/// The bounds of a beam run.
#[derive(Debug, Clone, Copy)]
struct BeamShape {
    width: u32,
    expansion: u32,
    max_rounds: u32,
}

/// The fixed inputs of a beam run.
#[derive(Clone, Copy)]
struct BeamRun<'a> {
    input: &'a SampleInput,
    /// The step scorer.
    scorer: &'a ScorerRef,
    task: &'a AgentTask,
    admission: &'a Admission,
    budget: Option<u32>,
}

/// One live beam: the step that ends it, its transcript, the flow labels
/// that it exercised and the delegations that it used.
struct Beam {
    draw: Option<DrawId>,
    messages: Vec<ChatMessage>,
    flow_labels: BTreeSet<String>,
    delegations: u32,
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
    run: BeamRun<'_>,
    shape: BeamShape,
) -> Result<(Vec<Scored>, SampleStop), String> {
    let BeamRun { input, task, .. } = run;
    let mut scored: Vec<Scored> = Vec::new();
    let mut spent: u32 = 0;
    let mut complete: u32 = 0;
    let mut live = vec![Beam {
        draw: None,
        messages: vec![ChatMessage::text(ChatRole::User, input.input.clone())],
        flow_labels: task.flow_labels.clone(),
        delegations: 0,
    }];
    for round in 0..shape.max_rounds {
        let slots = beam_slots(shape.width, complete);
        if slots == 0 || live.is_empty() {
            return Ok((scored, SampleStop::Completed));
        }
        // A plain loop, so the determinism analysis sees each call.
        let mut parents: Vec<&Beam> = Vec::new();
        for beam in &live {
            for _ in 0..shape.expansion {
                parents.push(beam);
            }
        }
        let first = scored.len();
        let halt = Box::pin(run_steps(
            ctx,
            Steps {
                run,
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
    run: BeamRun<'a>,
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
    let BeamRun {
        scorer,
        task,
        admission,
        budget,
        ..
    } = steps.run;
    let planned = u32::try_from(steps.parents.len()).unwrap_or(u32::MAX);
    let mut started: u32 = 0;
    loop {
        let left = budget.map_or(0, |max| max.saturating_sub(*spent));
        let step = sample_step(
            planned,
            started,
            admission.concurrency,
            budget.is_some(),
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
                let mut one = step_task(task, parent, steps.round);
                if budget.is_some() {
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
            let (mut one, halted) = score_draw(ctx, scorer, true, task, draw, report, halt).await?;
            halt = halt.or(halted);
            one.parent_delegations = parent.delegations;
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
/// labels, its step count and its delegation count. A beam of round `r`
/// holds `r` tool rounds. So the step may run `max_steps - r` tool rounds,
/// as one run of the task would. With none left, the model may still
/// answer. The mandate of the step allows only the delegations that the
/// beam has not used.
///
/// A step makes one model call. It has no context reset, because a reset
/// rewrites the beam. It has no answer check, because a rejected answer
/// asks the model again.
fn step_task(task: &AgentTask, beam: &Beam, round: u32) -> AgentTask {
    let mut mandate = task.mandate.clone();
    if let Some(mandate) = mandate.as_mut() {
        mandate.max_delegations = mandate.max_delegations.saturating_sub(beam.delegations);
    }
    AgentTask {
        mandate,
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
        let parent_delegations = one.parent_delegations;
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
                delegations: parent_delegations.saturating_add(used_delegations(&one.report)),
            });
        }
    }
    live
}

/// The delegations that `report` ran. A refused delegation uses none.
fn used_delegations(report: &AgentReport) -> u32 {
    let used = report
        .delegations
        .iter()
        .filter(|record| record.refusal.is_none())
        .count();
    u32::try_from(used).unwrap_or(u32::MAX)
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
    scorer: &ScorerRef,
    task: &AgentTask,
    report: &AgentReport,
) -> Option<ScoreRequest> {
    let mut score = ScoreInput::from_run(task, report);
    for tool in &mut score.tools {
        let content = std::mem::take(&mut tool.content);
        tool.content = cut_bytes(content, MAX_SCORED_TOOL_BYTES);
    }
    let request =
        ScoreRequest::new(scorer.id.as_str(), score).admitted_judges(scorer.judge_model.clone());
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
    scorer: &ScorerRef,
    beam: bool,
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
    } else if beam && !matches!(report.stop, AgentStop::Completed | AgentStop::StepEnded) {
        unscored("the step did not end with an answer or a tool round")
    } else if halt.is_some() {
        unscored("the run stopped before it scored the draw")
    } else if let Some(request) = score_request(scorer, task, &report) {
        match ctx
            .execute_activity::<_, SampleScore>(&agent_sample_score_info(), request)
            .await
        {
            Ok(result) => (result.verdict, result.usage, result.cost_usd_micros),
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
        key: None,
        usage: report.usage.saturating_add(usage),
        cost_usd_micros: report.cost_usd_micros.saturating_add(cost),
        beam: None,
    };
    let one = Scored {
        record,
        report,
        parent_delegations: 0,
    };
    Ok((one, stop))
}

/// Key one draw for a vote.
///
/// Only a draw that completed casts a vote. The recorded `agent_vote_key`
/// activity applies the key, so replay reads it. A draw with a key gets a
/// score of 1. A draw with no key gets a reject that names the reason.
///
/// A failed key activity gives a reject and the stop `ScoreFailed`. So
/// does a worker that has no function for an app key.
///
/// # Errors
///
/// Returns an engine error, such as a cancellation, as text.
async fn vote_draw(
    ctx: &WorkflowContext,
    key: &AnswerKey,
    draw: DrawId,
    report: AgentReport,
) -> Result<(Scored, Option<SampleStop>), String> {
    let mut stop = None;
    let request = VoteKeyRequest::new(key.clone(), Some(report.text.clone()));
    let found = if report.stop != AgentStop::Completed {
        Err("the draw did not complete, so it casts no vote".to_owned())
    } else if exceeds_activity_input(&request) {
        Err("the answer is too large to record, so it casts no vote".to_owned())
    } else {
        match ctx
            .execute_activity::<_, VoteKeyReply>(&agent_vote_key_info(), request)
            .await
        {
            Ok(reply) if !reply.known => {
                stop = Some(SampleStop::ScoreFailed);
                Err("the worker has no function for the answer key".to_owned())
            }
            Ok(reply) => reply.key.and_then(fit_key).ok_or_else(no_key),
            Err(err @ HarvestError::ActivityFailed { .. }) => {
                stop = Some(SampleStop::ScoreFailed);
                Err(format!("the answer key failed: {err}"))
            }
            Err(err) => return Err(err.to_string()),
        }
    };
    let (key, verdict) = match found {
        Ok(key) => (Some(key), Verdict::Score(FiniteF64::ONE)),
        Err(reason) => (None, Verdict::reject(reason)),
    };
    let record = SampleDraw {
        draw,
        run_id: report.run_id.clone(),
        stop: report.stop,
        verdict,
        key,
        usage: report.usage,
        cost_usd_micros: report.cost_usd_micros,
        beam: None,
        model: None,
    };
    let scored = Scored {
        record,
        report,
        parent_delegations: 0,
    };
    Ok((scored, stop))
}

/// The reject reason of a draw whose answer has no key.
fn no_key() -> String {
    "the answer has no key, so the draw casts no vote".to_owned()
}

/// The votes of the draws in `scored`.
fn tally_of(scored: &[Scored]) -> Tally {
    let mut tally = Tally::new();
    for one in scored {
        tally.cast(one.record.draw, one.record.key.as_deref());
    }
    tally
}

/// Whether `verdict` of the draw `draw` passes the check of `strategy`.
///
/// Best-of-N, a vote and beam search pass each score, and `select` never
/// picks a reject. Rejection passes a score at or above the threshold. A cascade
/// passes a score at or above the threshold of the tier of the draw.
fn passes(strategy: &SampleStrategy, draw: DrawId, verdict: &Verdict) -> bool {
    let at_least =
        |threshold: FiniteF64| verdict.as_score().is_some_and(|score| score >= threshold);
    match strategy {
        SampleStrategy::BestOfN { .. }
        | SampleStrategy::Vote { .. }
        | SampleStrategy::Beam { .. } => !verdict.is_reject(),
        SampleStrategy::Rejection { threshold, .. } => at_least(*threshold),
        SampleStrategy::Cascade { tiers } => usize::try_from(draw.get())
            .ok()
            .and_then(|index| tiers.get(index))
            .is_some_and(|tier| at_least(tier.threshold)),
    }
}

/// Whether `record` may be selected. A beam step must be a complete beam.
/// Each draw of another strategy may be.
fn selectable(record: &SampleDraw) -> bool {
    record
        .beam
        .is_none_or(|beam| beam.fate == BeamFate::Complete)
}

/// The selection of a run.
///
/// A vote selects the first draw of the winning key. Each other run
/// selects the best draw that passes. When no tier of a cascade accepts,
/// it selects the best draw that has a score, as `escalated`.
fn outcome(strategy: &SampleStrategy, scored: &[Scored]) -> SampleOutcome {
    if matches!(strategy, SampleStrategy::Vote { .. }) {
        return tally_of(scored)
            .winner()
            .map_or(SampleOutcome::NoAcceptedDraw, |winner| {
                SampleOutcome::Selected { draw: winner.first }
            });
    }
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

/// The best selectable draw by [`select`]. With `passing`, only a draw
/// that passes the check of `strategy` counts. Without it, each draw with
/// a score counts.
fn best(strategy: &SampleStrategy, scored: &[Scored], passing: bool) -> Option<DrawId> {
    let mut verdicts: Vec<(DrawId, Verdict)> = Vec::with_capacity(scored.len());
    for one in scored {
        let record = &one.record;
        let keep = if passing {
            passes(strategy, record.draw, &record.verdict)
        } else {
            !record.verdict.is_reject()
        };
        if keep && selectable(record) {
            verdicts.push((record.draw, record.verdict.clone()));
        }
    }
    select(&verdicts)
}

/// The report of the run: the selection, each draw and the totals.
///
/// A vote selects the first draw of the winning key, and reports the full
/// tally.
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
    let tally = if matches!(input.strategy, SampleStrategy::Vote { .. }) {
        tally_of(&scored).counts()
    } else {
        Vec::new()
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
        tally,
        skipped,
        usage,
        cost_usd_micros,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failed_score_wins_over_each_other_stop() {
        use SampleStop::{BudgetExceeded, ScoreFailed};
        assert_eq!(merge_stop(None, None), None);
        assert_eq!(merge_stop(None, Some(BudgetExceeded)), Some(BudgetExceeded));
        assert_eq!(merge_stop(Some(BudgetExceeded), None), Some(BudgetExceeded));
        assert_eq!(
            merge_stop(Some(BudgetExceeded), Some(ScoreFailed)),
            Some(ScoreFailed),
            "a later failed key replaces an earlier budget stop"
        );
        assert_eq!(
            merge_stop(Some(ScoreFailed), Some(BudgetExceeded)),
            Some(ScoreFailed)
        );
    }

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
        let vote = SampleStrategy::vote(3, AnswerKey::Trimmed);
        assert!(passes(&vote, draw, &Verdict::Score(FiniteF64::ONE)));
        assert!(!passes(&vote, draw, &Verdict::reject("no key")));
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
        let request = score_request(
            input.scorer.as_ref().unwrap(),
            &AgentTask::new("x"),
            &report,
        )
        .unwrap();
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
            model: None,
            stop,
            verdict,
            key: None,
            usage: TokenUsage::default(),
            cost_usd_micros: 0,
            beam: Some(BeamStep {
                round: 1,
                parent: None,
                fate: BeamFate::Pruned,
            }),
        };
        Scored {
            record,
            report,
            parent_delegations: 0,
        }
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
        assert_eq!(default_concurrency(&SampleStrategy::beam(2, 2, 2)), 0);
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
