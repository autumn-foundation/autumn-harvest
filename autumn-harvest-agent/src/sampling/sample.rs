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
//!
//! The design is in `docs/design/DESIGN-2443.md` and
//! `docs/design/DESIGN-2444.md`.

use std::num::NonZeroU32;

use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use super::admit::{Admission, Ask, Refusal, SampleStep, admit, sample_step};
use super::draws::{draw_task, profile_task, read_policy, read_profile};
use super::policy::{ScorerRef, Strategy};
use super::vote::{
    AnswerKey, MAX_KEY_BYTES, Tally, VoteCount, VoteKeyReply, VoteKeyRequest, agent_vote_key_info,
};
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

/// The strategy of one `agent_sample` run.
///
/// It encodes as `{"best_of_n": {"n": 3}}`,
/// `{"rejection": {"max_attempts": 4, "threshold": 0.8}}` or
/// `{"vote": {"n": 5, "key": "last_number"}}`.
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

    /// The policy strategy, such as `best_of_n`.
    #[must_use]
    pub const fn policy_strategy(&self) -> Strategy {
        match self {
            Self::BestOfN { .. } => Strategy::BestOfN,
            Self::Rejection { .. } => Strategy::Rejection,
            Self::Vote { .. } => Strategy::Vote,
        }
    }

    /// The most draws of the run.
    #[must_use]
    pub const fn draws(&self) -> u32 {
        match self {
            Self::BestOfN { n } | Self::Vote { n, .. } => *n,
            Self::Rejection { max_attempts, .. } => *max_attempts,
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
    /// for best-of-N and a vote, and 1 for rejection.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleOutcome {
    /// The run selected this draw. For a vote, it is the first draw of
    /// the winning key.
    Selected {
        /// The draw.
        draw: DrawId,
    },
    /// No draw passed. For a vote, no draw had a key.
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
    /// A score or an app key failed, so the run started no more draws.
    /// The verdict of the draw names the error.
    ScoreFailed,
    /// The leader of a vote keeps the lead whatever the draws left vote,
    /// so the run started no more draws.
    Settled,
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
    /// The votes of each key, the winner first. Empty unless the run is a
    /// vote.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tally: Vec<VoteCount>,
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
    let ask = Ask {
        strategy: input.strategy.policy_strategy(),
        draws: input.strategy.draws(),
        concurrency: input
            .concurrency
            .map_or_else(|| default_concurrency(&input.strategy), NonZeroU32::get),
        // A vote refuses a scorer below, so the policy never checks one.
        scorer: if voting { None } else { input.scorer.as_ref() },
    };
    let admission = admit(&read.policy, &ask).map_err(|refusal| refusal.error_text())?;
    let judge = judge_of(&input).map_err(|refusal| refusal.error_text())?;
    if let Judge::Vote(AnswerKey::Custom(id)) = judge {
        probe_key(ctx, id).await?;
    }
    let task = draw_task(
        &profile_task(&profile, &input.input),
        &admission.draw_effects,
    );
    let budget = tree_budget(task.max_total_tokens, input.max_total_tokens);
    let (scored, stopped) = run_waves(ctx, judge, &input, &task, &admission, budget).await?;
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
/// Best-of-N and a vote take the cap of the policy (0). Rejection stops at
/// the first pass, so it runs one draw at a time. A wider wave would pay
/// for draws after the pass.
const fn default_concurrency(strategy: &SampleStrategy) -> u32 {
    match strategy {
        SampleStrategy::BestOfN { .. } | SampleStrategy::Vote { .. } => 0,
        SampleStrategy::Rejection { .. } => 1,
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
async fn probe_key(ctx: &WorkflowContext, id: &str) -> Result<(), String> {
    let reply: VoteKeyReply = ctx
        .execute_activity(&agent_vote_key_info(), VoteKeyRequest::new(id, None))
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
/// and a settled vote win over the other stops, because the run has its
/// answer.
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
                Judge::Score(scorer) => score_draw(ctx, scorer, task, draw, report, halt).await?,
                Judge::Vote(key) => vote_draw(ctx, key, draw, report).await?,
            };
            halt = halt.or(halted);
            passed |= passes(&input.strategy, &one.record.verdict);
            spent = spent.saturating_add(one.record.usage.total());
            scored.push(one);
        }
        if passed && matches!(input.strategy, SampleStrategy::Rejection { .. }) {
            return Ok((scored, SampleStop::Accepted));
        }
        let ran = u32::try_from(scored.len()).unwrap_or(u32::MAX);
        let left = admission.draws.saturating_sub(ran);
        if matches!(judge, Judge::Vote(_)) && left > 0 && tally_of(&scored).settled(left) {
            return Ok((scored, SampleStop::Settled));
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
    } else if let Some(request) = score_request(scorer, task, &report) {
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
        key: None,
        usage: report.usage.saturating_add(usage),
        cost_usd_micros: report.cost_usd_micros.saturating_add(cost),
    };
    Ok((Scored { record, report }, stop))
}

/// Key one draw for a vote.
///
/// Only a draw that completed casts a vote. A built-in key runs here, as
/// a pure function of the recorded answer. An app key runs in the recorded
/// `agent_vote_key` activity. A draw with a key gets a score of 1. A draw
/// with no key gets a reject that names the reason.
///
/// A failed app key gives a reject and the stop `ScoreFailed`.
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
    let found = if report.stop != AgentStop::Completed {
        Err("the draw did not complete, so it casts no vote".to_owned())
    } else if let AnswerKey::Custom(id) = key {
        let request = VoteKeyRequest::new(id.as_str(), Some(report.text.clone()));
        if exceeds_activity_input(&request) {
            Err("the answer is too large to record, so it casts no vote".to_owned())
        } else {
            match ctx
                .execute_activity::<_, VoteKeyReply>(&agent_vote_key_info(), request)
                .await
            {
                Ok(reply) => reply.key.ok_or_else(no_key),
                Err(err @ HarvestError::ActivityFailed { .. }) => {
                    stop = Some(SampleStop::ScoreFailed);
                    Err(format!("the answer key failed: {err}"))
                }
                Err(err) => return Err(err.to_string()),
            }
        }
    } else {
        key.key_of(&report.text).ok_or_else(no_key)
    };
    let (key, verdict) = match found {
        Ok(key) if !key.is_empty() && key.len() <= MAX_KEY_BYTES => {
            (Some(key), Verdict::Score(FiniteF64::ONE))
        }
        Ok(_) => (None, Verdict::reject(no_key())),
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
    };
    Ok((Scored { record, report }, stop))
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

/// Whether `verdict` passes the check of `strategy`.
///
/// Best-of-N and a vote pass each score, and `select` never picks a
/// reject. Rejection passes a score at or above the threshold.
fn passes(strategy: &SampleStrategy, verdict: &Verdict) -> bool {
    match strategy {
        SampleStrategy::BestOfN { .. } | SampleStrategy::Vote { .. } => !verdict.is_reject(),
        SampleStrategy::Rejection { threshold, .. } => {
            verdict.as_score().is_some_and(|score| score >= *threshold)
        }
    }
}

/// The report of the run: the selection, each draw and the totals.
///
/// A vote selects the first draw of the winning key, and reports the full
/// tally. Each other strategy calls [`select`] over the draws that pass.
fn report(
    input: SampleInput,
    revision: u64,
    concurrency: u32,
    scored: Vec<Scored>,
    stopped: SampleStop,
) -> SampleReport {
    let (chosen, tally) = if matches!(input.strategy, SampleStrategy::Vote { .. }) {
        let tally = tally_of(&scored);
        (tally.winner().map(|winner| winner.first), tally.counts())
    } else {
        let passing: Vec<(DrawId, Verdict)> = scored
            .iter()
            .filter(|one| passes(&input.strategy, &one.record.verdict))
            .map(|one| (one.record.draw, one.record.verdict.clone()))
            .collect();
        (select(&passing), Vec::new())
    };
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
        tally,
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
        assert!(passes(&strategy, &Verdict::score(0.5).unwrap()));
        assert!(!passes(&strategy, &Verdict::score(0.49).unwrap()));
        assert!(!passes(&strategy, &Verdict::reject("no")));
        let best = SampleStrategy::best_of_n(3);
        assert!(passes(&best, &Verdict::score(-1e300).unwrap()));
        assert!(!passes(&best, &Verdict::reject("no")));
        let vote = SampleStrategy::vote(3, AnswerKey::Trimmed);
        assert!(passes(&vote, &Verdict::Score(FiniteF64::ONE)));
        assert!(!passes(&vote, &Verdict::reject("no key")));
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
