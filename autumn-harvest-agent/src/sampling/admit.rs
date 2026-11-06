//! The admission kernel of a strategy run (issue #2439).
//!
//! [`admit`] compares one strategy request with the policy of its kind.
//! It runs before the first model call. A request that asks for more than
//! the policy allows gets a named [`Refusal`].
//!
//! [`draw_plan`] and [`wave_end`] hold the integer part.
//! [`admit_answer_check`] admits the answer check of a run (issue #2442).
//! [`answer_retry_cap`] and [`retry_after_reject`] hold its integer part.
//! `formal/verus/src/agent_sampling.rs` proves them.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::policy::{MAX_ANSWER_RETRIES, SamplingPolicy, ScorerId, ScorerRef, Scorers, Strategy};
use crate::answer_check::AnswerCheck;
use crate::tool::ToolEffect;
use crate::types::MAX_PARALLEL_CHILDREN;

/// Why a strategy run or an answer check is refused before its first model
/// call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "refusal", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Refusal {
    /// The policy does not list the strategy.
    StrategyNotAllowed {
        /// The strategy that the run asked for.
        strategy: Strategy,
    },
    /// The run asks for no draw.
    NoDraws,
    /// The run asks for more draws than `max_draws`.
    TooManyDraws {
        /// The draws that the run asked for.
        asked: u32,
        /// The cap of the policy.
        max: u32,
    },
    /// The run asks for more concurrent draws than the policy allows.
    TooManyConcurrentDraws {
        /// The concurrency that the run asked for.
        asked: u32,
        /// The cap of the policy.
        max: u32,
    },
    /// The policy does not allow the scorer.
    ScorerNotAllowed {
        /// The scorer that the run asked for.
        scorer: ScorerId,
    },
    /// A judge scorer names a model outside `judge_models`.
    JudgeModelNotAllowed {
        /// The model that the judge names.
        model: String,
    },
    /// The answer check asks for more retries than the policy allows
    /// (issue #2442).
    TooManyAnswerRetries {
        /// The retries that the check asked for.
        asked: u32,
        /// The cap of the policy.
        max: u32,
    },
    /// The harness has no scorer with the id of the answer check (issue
    /// #2442).
    UnknownScorer {
        /// The scorer id.
        scorer: ScorerId,
    },
    /// The profile of the kind belongs to another tenant (issue #2442).
    OtherTenant,
    /// A sweep has no item.
    NoItems,
    /// A sweep has more items than [`MAX_SWEEP_ITEMS`](super::MAX_SWEEP_ITEMS).
    TooManyItems {
        /// The items of the sweep.
        asked: u32,
        /// The cap.
        max: u32,
    },
    /// One item of a sweep is larger than
    /// [`MAX_SWEEP_ITEM_BYTES`](super::MAX_SWEEP_ITEM_BYTES).
    ItemTooLarge {
        /// The index of the item.
        item: u32,
        /// The cap, in bytes.
        max: u64,
    },
    /// The kind is not a valid agent kind.
    InvalidKind,
    /// No agent profile has the kind, so no task can be built.
    UnknownKind,
}

/// The prefix of the error of a refused run.
pub const REFUSED_PREFIX: &str = "sampling refused";

impl Refusal {
    /// The stable code of the refusal, such as `strategy_not_allowed`.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::StrategyNotAllowed { .. } => "strategy_not_allowed",
            Self::NoDraws => "no_draws",
            Self::TooManyDraws { .. } => "too_many_draws",
            Self::TooManyConcurrentDraws { .. } => "too_many_concurrent_draws",
            Self::ScorerNotAllowed { .. } => "scorer_not_allowed",
            Self::JudgeModelNotAllowed { .. } => "judge_model_not_allowed",
            Self::TooManyAnswerRetries { .. } => "too_many_answer_retries",
            Self::UnknownScorer { .. } => "unknown_scorer",
            Self::OtherTenant => "other_tenant",
            Self::NoItems => "no_items",
            Self::TooManyItems { .. } => "too_many_items",
            Self::ItemTooLarge { .. } => "item_too_large",
            Self::InvalidKind => "invalid_kind",
            Self::UnknownKind => "unknown_kind",
        }
    }

    /// The error of a refused run: `sampling refused (<code>): <reason>`.
    #[must_use]
    pub fn error_text(&self) -> String {
        format!("{REFUSED_PREFIX} ({}): {self}", self.code())
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StrategyNotAllowed { strategy } => {
                write!(f, "the policy of this kind does not allow {strategy}")
            }
            Self::NoDraws => f.write_str("the run asks for no draw"),
            Self::TooManyDraws { asked, max } => {
                write!(f, "the run asks for {asked} draws; the policy allows {max}")
            }
            Self::TooManyConcurrentDraws { asked, max } => write!(
                f,
                "the run asks for {asked} concurrent draws; the policy allows {max}"
            ),
            Self::ScorerNotAllowed { scorer } => {
                write!(
                    f,
                    "the policy of this kind does not allow the scorer {scorer}"
                )
            }
            Self::JudgeModelNotAllowed { model } => {
                write!(
                    f,
                    "the policy of this kind does not allow the judge model {model:?}"
                )
            }
            Self::TooManyAnswerRetries { asked, max } => write!(
                f,
                "the run asks for {asked} answer retries; the policy allows {max}"
            ),
            Self::UnknownScorer { scorer } => write!(f, "the harness has no scorer {scorer}"),
            Self::OtherTenant => f.write_str("the profile of the kind belongs to another tenant"),
            Self::NoItems => f.write_str("the sweep has no item"),
            Self::TooManyItems { asked, max } => {
                write!(f, "the sweep has {asked} items; the cap is {max}")
            }
            Self::ItemTooLarge { item, max } => {
                write!(f, "item {item} is larger than {max} bytes")
            }
            Self::InvalidKind => f.write_str("the kind is not a valid agent kind"),
            Self::UnknownKind => f.write_str("no agent profile has the kind"),
        }
    }
}

/// What a strategy run asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ask<'a> {
    /// The strategy.
    pub strategy: Strategy,
    /// The draws of the whole run.
    pub draws: u32,
    /// The concurrency. 0 asks for the cap of the policy.
    pub concurrency: u32,
    /// The scorer, if any.
    pub scorer: Option<&'a ScorerRef>,
}

/// What a strategy run may do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Admission {
    /// The draws of the run.
    pub draws: u32,
    /// The most draws that run at the same time.
    pub concurrency: u32,
    /// The tool effects of each draw.
    pub draw_effects: BTreeSet<ToolEffect>,
}

/// The integer part of an admission. See [`draw_plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrawPlan {
    /// Run `draws` draws, at most `concurrency` at the same time.
    Run {
        /// The draws.
        draws: u32,
        /// The concurrency.
        concurrency: u32,
    },
    /// The run asks for no draw.
    NoDraws,
    /// The run asks for more draws than the cap.
    TooManyDraws,
    /// The run asks for more concurrency than the cap.
    TooManyConcurrent,
}

/// The draws and the concurrency of a run.
///
/// `asked` is the concurrency that the run asks for, or 0 for the cap. The
/// concurrency is never above `max_concurrent`, [`MAX_PARALLEL_CHILDREN`]
/// or `draws`, and never below 1.
///
/// `formal/verus/src/agent_sampling.rs` proves it.
#[must_use]
pub const fn draw_plan(max_draws: u32, max_concurrent: u32, draws: u32, asked: u32) -> DrawPlan {
    if draws == 0 {
        return DrawPlan::NoDraws;
    }
    if draws > max_draws {
        return DrawPlan::TooManyDraws;
    }
    let mut cap = max_concurrent;
    if cap > MAX_PARALLEL_CHILDREN {
        cap = MAX_PARALLEL_CHILDREN;
    }
    if cap == 0 {
        cap = 1;
    }
    if asked > cap {
        return DrawPlan::TooManyConcurrent;
    }
    let mut concurrency = if asked == 0 { cap } else { asked };
    if concurrency > draws {
        concurrency = draws;
    }
    DrawPlan::Run { draws, concurrency }
}

/// The end of the wave that starts at draw `start`: the draws
/// `start..wave_end` run at the same time.
///
/// A wave holds at least one draw and at most `concurrency`, and it never
/// passes `draws`. A `concurrency` of 0 counts as 1.
///
/// `formal/verus/src/agent_sampling.rs` proves it.
#[must_use]
pub const fn wave_end(draws: u32, concurrency: u32, start: u32) -> u32 {
    if start >= draws {
        return draws;
    }
    let width = if concurrency == 0 { 1 } else { concurrency };
    let left = draws - start;
    if left < width { draws } else { start + width }
}

/// The retry cap of an answer check: `asked`, or `None` when `asked` is
/// over `policy_max` or over [`MAX_ANSWER_RETRIES`].
///
/// `formal/verus/src/agent_sampling.rs` proves it.
#[must_use]
pub const fn answer_retry_cap(policy_max: u32, asked: u32) -> Option<u32> {
    let mut cap = policy_max;
    if cap > MAX_ANSWER_RETRIES {
        cap = MAX_ANSWER_RETRIES;
    }
    if asked > cap {
        return None;
    }
    Some(asked)
}

/// Whether the model may answer again after `rejections` rejected answers,
/// under the retry cap `cap`.
///
/// With every answer rejected, a run asks for `cap + 1` answers.
/// `formal/verus/src/agent_sampling.rs` proves it.
#[must_use]
pub const fn retry_after_reject(rejections: u32, cap: u32) -> bool {
    rejections <= cap
}

/// Compare the answer check `check` with `policy` (issue #2442). Returns
/// the retry cap of the run.
///
/// The checks go in order: the retries, the scorer, then the judge model.
///
/// # Errors
///
/// Returns the first [`Refusal`].
pub fn admit_answer_check(policy: &SamplingPolicy, check: &AnswerCheck) -> Result<u32, Refusal> {
    let Some(cap) = answer_retry_cap(policy.max_answer_retries, check.retries) else {
        return Err(Refusal::TooManyAnswerRetries {
            asked: check.retries,
            max: policy.max_answer_retries.min(MAX_ANSWER_RETRIES),
        });
    };
    check_scorer(policy, &check.scorer)?;
    Ok(cap)
}

/// The next step of a sample run (issue #2443). See [`sample_step`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleStep {
    /// Start the next `width` draws at the same time.
    Wave {
        /// The draws of the wave.
        width: u32,
    },
    /// Each planned draw started.
    Done,
    /// Draws are left, but the token budget is spent.
    BudgetExceeded,
}

/// The next step of a sample run that started `started` of `draws` draws.
///
/// `budgeted` says whether the run has a token budget, and `tokens_left`
/// holds the rest of it. A wave holds at most `concurrency` draws, the
/// draws left and the tokens left. So each draw of a wave gets a share of
/// at least 1 token from `sibling_cap`. A `concurrency` of 0 counts as 1.
///
/// `formal/verus/src/agent_sampling.rs` proves it.
#[must_use]
pub const fn sample_step(
    draws: u32,
    started: u32,
    concurrency: u32,
    budgeted: bool,
    tokens_left: u32,
) -> SampleStep {
    if started >= draws {
        return SampleStep::Done;
    }
    if budgeted && tokens_left == 0 {
        return SampleStep::BudgetExceeded;
    }
    let mut width = wave_end(draws, concurrency, started) - started;
    if budgeted && width > tokens_left {
        width = tokens_left;
    }
    SampleStep::Wave { width }
}

/// The concurrency of a sweep. See [`sweep_plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepPlan {
    /// Run at most `items` items at the same time, each with at most
    /// `draws` draws at the same time.
    Run {
        /// The items at the same time.
        items: u32,
        /// The draws of one item at the same time.
        draws: u32,
    },
    /// The sweep asks for more items at the same time than the cap.
    TooManyConcurrent,
}

/// The concurrency of a sweep under the policy cap `max_concurrent`.
///
/// `asked` is the item concurrency that the sweep asks for, or 0 for the
/// cap. `wide` is the most draws that one item can run at the same time.
/// The items and their draws share the cap, so `items * draws` is never
/// above the cap. Both are at least 1.
///
/// `formal/verus/src/agent_sweep.rs` proves it.
#[must_use]
pub const fn sweep_plan(max_concurrent: u32, asked: u32, wide: u32) -> SweepPlan {
    let mut cap = max_concurrent;
    if cap > MAX_PARALLEL_CHILDREN {
        cap = MAX_PARALLEL_CHILDREN;
    }
    if cap == 0 {
        cap = 1;
    }
    if asked > cap {
        return SweepPlan::TooManyConcurrent;
    }
    let items = if asked == 0 { cap } else { asked };
    let mut draws = cap / items;
    if draws > wide {
        draws = wide;
    }
    if draws == 0 {
        draws = 1;
    }
    SweepPlan::Run { items, draws }
}

/// Compare `ask` with `policy`.
///
/// The checks go in order: the strategy, the draw count, the concurrency,
/// the scorer, then the judge model.
///
/// # Errors
///
/// Returns the first [`Refusal`].
pub fn admit(policy: &SamplingPolicy, ask: &Ask<'_>) -> Result<Admission, Refusal> {
    if !policy.strategies.contains(&ask.strategy) {
        return Err(Refusal::StrategyNotAllowed {
            strategy: ask.strategy,
        });
    }
    admit_draws(policy, ask.draws, ask.concurrency, ask.scorer)
}

/// Compare a single draw with `policy`.
///
/// The default policy makes each run a single draw, so the policy needs no
/// strategy for it. The concurrency and the scorer checks still apply.
///
/// # Errors
///
/// Returns the first [`Refusal`].
pub fn admit_single(
    policy: &SamplingPolicy,
    concurrency: u32,
    scorer: Option<&ScorerRef>,
) -> Result<Admission, Refusal> {
    admit_draws(policy, 1, concurrency, scorer)
}

/// The checks of [`admit`] after the strategy check.
fn admit_draws(
    policy: &SamplingPolicy,
    asked_draws: u32,
    asked_concurrency: u32,
    scorer: Option<&ScorerRef>,
) -> Result<Admission, Refusal> {
    let (draws, concurrency) = match draw_plan(
        policy.max_draws,
        policy.max_concurrent_draws,
        asked_draws,
        asked_concurrency,
    ) {
        DrawPlan::Run { draws, concurrency } => (draws, concurrency),
        DrawPlan::NoDraws => return Err(Refusal::NoDraws),
        DrawPlan::TooManyDraws => {
            return Err(Refusal::TooManyDraws {
                asked: asked_draws,
                max: policy.max_draws,
            });
        }
        DrawPlan::TooManyConcurrent => {
            return Err(Refusal::TooManyConcurrentDraws {
                asked: asked_concurrency,
                max: policy.max_concurrent_draws.min(MAX_PARALLEL_CHILDREN),
            });
        }
    };
    if let Some(scorer) = scorer {
        check_scorer(policy, scorer)?;
    }
    Ok(Admission {
        draws,
        concurrency,
        draw_effects: policy.draw_effects.clone(),
    })
}

/// Refuse a scorer that the policy does not allow, then a judge model
/// outside `judge_models`.
///
/// The request says whether a scorer is a judge. No scorer runs yet. The
/// scorer registry of #2441 must classify each id, and must refuse a judge
/// id that names no model.
fn check_scorer(policy: &SamplingPolicy, scorer: &ScorerRef) -> Result<(), Refusal> {
    let listed = match &policy.scorers {
        // Each scorer that calls no model. A judge never passes.
        Scorers::Deterministic => scorer.judge_model.is_none(),
        Scorers::Only(ids) => ids.contains(&scorer.id),
    };
    if !listed {
        return Err(Refusal::ScorerNotAllowed {
            scorer: scorer.id.clone(),
        });
    }
    match &scorer.judge_model {
        Some(model) if !policy.judge_models.contains(model) => Err(Refusal::JudgeModelNotAllowed {
            model: crate::harness::truncate(model, crate::mesh::MAX_ITEM_LEN),
        }),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::{prop_assert, prop_assert_eq, proptest};

    fn allowing(strategy: Strategy, max: u32, concurrent: u32) -> SamplingPolicy {
        SamplingPolicy {
            strategies: BTreeSet::from([strategy]),
            max_draws: max,
            max_concurrent_draws: concurrent,
            ..SamplingPolicy::default()
        }
    }

    fn ask(strategy: Strategy, draws: u32) -> Ask<'static> {
        Ask {
            strategy,
            draws,
            concurrency: 0,
            scorer: None,
        }
    }

    #[test]
    fn a_sweep_plan_shares_the_cap() {
        assert_eq!(sweep_plan(4, 2, 3), SweepPlan::Run { items: 2, draws: 2 });
        assert_eq!(sweep_plan(4, 0, 3), SweepPlan::Run { items: 4, draws: 1 });
        assert_eq!(sweep_plan(8, 3, 1), SweepPlan::Run { items: 3, draws: 1 });
        assert_eq!(sweep_plan(2, 3, 1), SweepPlan::TooManyConcurrent);
        assert_eq!(sweep_plan(0, 0, 0), SweepPlan::Run { items: 1, draws: 1 });
    }

    proptest! {
        #[test]
        fn a_sweep_plan_never_passes_the_cap(
            max_concurrent in 0_u32..64,
            asked in 0_u32..64,
            wide in 0_u32..300,
        ) {
            let cap = max_concurrent.clamp(1, MAX_PARALLEL_CHILDREN);
            match sweep_plan(max_concurrent, asked, wide) {
                SweepPlan::Run { items, draws } => {
                    prop_assert!(asked <= cap);
                    prop_assert!(items >= 1 && draws >= 1);
                    prop_assert!(items * draws <= cap);
                    prop_assert!(draws <= wide.max(1));
                    prop_assert!(asked == 0 || items == asked);
                }
                SweepPlan::TooManyConcurrent => prop_assert!(asked > cap),
            }
        }
    }

    #[test]
    fn the_default_policy_refuses_every_strategy() {
        let policy = SamplingPolicy::default();
        for strategy in [
            Strategy::BestOfN,
            Strategy::Rejection,
            Strategy::Vote,
            Strategy::Cascade,
            Strategy::Beam,
        ] {
            assert_eq!(
                admit(&policy, &ask(strategy, 1)),
                Err(Refusal::StrategyNotAllowed { strategy })
            );
        }
    }

    #[test]
    fn the_strategy_check_comes_first() {
        let policy = allowing(Strategy::Vote, 2, 1);
        assert_eq!(
            admit(&policy, &ask(Strategy::BestOfN, 99)),
            Err(Refusal::StrategyNotAllowed {
                strategy: Strategy::BestOfN
            })
        );
    }

    #[test]
    fn an_admission_takes_the_draw_effects_of_the_policy() {
        let mut policy = allowing(Strategy::BestOfN, 4, 2);
        policy.draw_effects = BTreeSet::from([ToolEffect::ReadOnly, ToolEffect::Internal]);
        let admitted = admit(&policy, &ask(Strategy::BestOfN, 4)).unwrap();
        assert_eq!(admitted.draws, 4);
        assert_eq!(admitted.concurrency, 2);
        assert_eq!(admitted.draw_effects, policy.draw_effects);
    }

    #[test]
    fn the_plan_caps_the_concurrency() {
        assert_eq!(draw_plan(4, 2, 0, 0), DrawPlan::NoDraws);
        assert_eq!(draw_plan(4, 2, 5, 0), DrawPlan::TooManyDraws);
        assert_eq!(draw_plan(4, 2, 4, 3), DrawPlan::TooManyConcurrent);
        assert_eq!(
            draw_plan(4, 2, 4, 0),
            DrawPlan::Run {
                draws: 4,
                concurrency: 2
            }
        );
        assert_eq!(
            draw_plan(4, 8, 3, 0),
            DrawPlan::Run {
                draws: 3,
                concurrency: 3
            },
            "never more workers than draws"
        );
        assert_eq!(
            draw_plan(100, 100, 100, 0),
            DrawPlan::Run {
                draws: 100,
                concurrency: MAX_PARALLEL_CHILDREN
            },
            "the engine bound holds over a bad policy"
        );
        assert_eq!(
            draw_plan(4, 0, 4, 0),
            DrawPlan::Run {
                draws: 4,
                concurrency: 1
            }
        );
    }

    #[test]
    fn the_waves_of_four_by_two() {
        assert_eq!(wave_end(4, 2, 0), 2);
        assert_eq!(wave_end(4, 2, 2), 4);
        assert_eq!(wave_end(5, 2, 4), 5);
        assert_eq!(wave_end(4, 0, 0), 1);
        assert_eq!(wave_end(4, 2, 9), 4);
    }

    #[test]
    fn a_judge_needs_a_listed_scorer_and_a_listed_model() {
        let judge = ScorerRef::judge("judge", "big").unwrap();
        let exact = ScorerRef::deterministic("exact_match").unwrap();
        let mut policy = allowing(Strategy::BestOfN, 2, 1);
        let with = |scorer| Ask {
            scorer: Some(scorer),
            ..ask(Strategy::BestOfN, 2)
        };
        assert!(admit(&policy, &with(&exact)).is_ok());
        assert_eq!(
            admit(&policy, &with(&judge)).unwrap_err().code(),
            "scorer_not_allowed"
        );
        policy.scorers = Scorers::Only(BTreeSet::from([judge.id.clone()]));
        assert_eq!(
            admit(&policy, &with(&exact)).unwrap_err().code(),
            "scorer_not_allowed",
            "a listed set allows only its ids"
        );
        assert_eq!(
            admit(&policy, &with(&judge)),
            Err(Refusal::JudgeModelNotAllowed {
                model: "big".to_owned()
            })
        );
        policy.judge_models.insert("big".to_owned());
        assert!(admit(&policy, &with(&judge)).is_ok());
    }

    #[test]
    fn the_error_text_names_the_code() {
        let text = Refusal::TooManyDraws { asked: 5, max: 4 }.error_text();
        assert_eq!(
            text,
            "sampling refused (too_many_draws): the run asks for 5 draws; the policy allows 4"
        );
    }

    fn check(retries: u32) -> AnswerCheck {
        AnswerCheck::new("k", ScorerRef::deterministic("exact_match").unwrap()).retries(retries)
    }

    #[test]
    fn the_retry_cap_is_the_ask_within_the_policy() {
        assert_eq!(answer_retry_cap(0, 0), Some(0));
        assert_eq!(answer_retry_cap(2, 1), Some(1));
        assert_eq!(answer_retry_cap(2, 2), Some(2));
        assert_eq!(answer_retry_cap(2, 3), None);
        assert_eq!(
            answer_retry_cap(u32::MAX, MAX_ANSWER_RETRIES + 1),
            None,
            "the engine bound holds over a bad policy"
        );
    }

    #[test]
    fn a_cap_of_two_allows_three_answers() {
        assert!(retry_after_reject(1, 2));
        assert!(retry_after_reject(2, 2));
        assert!(!retry_after_reject(3, 2));
        assert!(!retry_after_reject(1, 0), "a cap of 0 allows one answer");
    }

    #[test]
    fn an_answer_check_needs_its_retries_and_its_scorer_in_the_policy() {
        let mut policy = SamplingPolicy::default();
        assert_eq!(admit_answer_check(&policy, &check(0)), Ok(0));
        assert_eq!(
            admit_answer_check(&policy, &check(1)),
            Err(Refusal::TooManyAnswerRetries { asked: 1, max: 0 })
        );
        policy.max_answer_retries = 3;
        assert_eq!(admit_answer_check(&policy, &check(2)), Ok(2));
        let judge = AnswerCheck::new("k", ScorerRef::judge("judge", "big").unwrap());
        assert_eq!(
            admit_answer_check(&policy, &judge).unwrap_err().code(),
            "scorer_not_allowed"
        );
        policy.scorers = Scorers::Only(BTreeSet::from([judge.scorer.id.clone()]));
        assert_eq!(
            admit_answer_check(&policy, &judge).unwrap_err().code(),
            "judge_model_not_allowed"
        );
        policy.judge_models.insert("big".to_owned());
        assert_eq!(admit_answer_check(&policy, &judge), Ok(0));
        let text = Refusal::TooManyAnswerRetries { asked: 4, max: 3 }.error_text();
        assert_eq!(
            text,
            "sampling refused (too_many_answer_retries): the run asks for 4 answer retries; the policy allows 3"
        );
    }

    #[test]
    fn a_sample_step_of_four_draws_and_ten_tokens() {
        assert_eq!(
            sample_step(4, 0, 2, true, 10),
            SampleStep::Wave { width: 2 }
        );
        assert_eq!(sample_step(4, 2, 2, true, 1), SampleStep::Wave { width: 1 });
        assert_eq!(sample_step(4, 2, 2, true, 0), SampleStep::BudgetExceeded);
        assert_eq!(sample_step(4, 4, 2, true, 0), SampleStep::Done);
        assert_eq!(
            sample_step(4, 3, 2, false, 0),
            SampleStep::Wave { width: 1 }
        );
        assert_eq!(
            sample_step(4, 0, 0, false, 0),
            SampleStep::Wave { width: 1 }
        );
    }

    proptest! {
        #[test]
        fn a_sample_step_never_passes_a_bound(
            draws in 0_u32..=40,
            started in 0_u32..=44,
            concurrency in 0_u32..=20,
            budgeted: bool,
            tokens_left in 0_u32..=50,
        ) {
            match sample_step(draws, started, concurrency, budgeted, tokens_left) {
                SampleStep::Done => prop_assert!(started >= draws),
                SampleStep::BudgetExceeded => {
                    prop_assert!(started < draws && budgeted && tokens_left == 0);
                }
                SampleStep::Wave { width } => {
                    prop_assert!(width >= 1);
                    prop_assert!(started + width <= draws);
                    prop_assert!(width <= concurrency.max(1));
                    if budgeted {
                        prop_assert!(width <= tokens_left);
                        // Each draw of the wave gets at least 1 token.
                        for index in 0..width {
                            prop_assert!(crate::bounds::sibling_cap(tokens_left, width, index) >= 1);
                        }
                    }
                }
            }
        }
    }

    proptest! {
        #[test]
        fn an_admission_never_passes_a_cap(
            max in 1_u32..=300,
            concurrent in 0_u32..=40,
            draws in 0_u32..=320,
            asked in 0_u32..=40,
        ) {
            if let DrawPlan::Run { draws: run, concurrency } = draw_plan(max, concurrent, draws, asked) {
                prop_assert_eq!(run, draws);
                prop_assert!(run >= 1 && run <= max);
                prop_assert!(concurrency >= 1);
                prop_assert!(concurrency <= run);
                prop_assert!(concurrency <= MAX_PARALLEL_CHILDREN);
                prop_assert!(concurrency <= concurrent.max(1));
                // The waves cover every draw once, each no wider than the cap.
                let mut start = 0;
                let mut seen = 0;
                while start < run {
                    let end = wave_end(run, concurrency, start);
                    prop_assert!(end > start && end - start <= concurrency);
                    seen += end - start;
                    start = end;
                }
                prop_assert_eq!(seen, run);
            }
        }

        #[test]
        fn the_answers_of_a_run_never_pass_the_cap(
            policy_max in 0_u32..=40,
            asked in 0_u32..=40,
        ) {
            if let Some(cap) = answer_retry_cap(policy_max, asked) {
                prop_assert!(cap <= policy_max && cap <= MAX_ANSWER_RETRIES);
                // Count the answers of a run whose every answer is rejected.
                let mut answers = 1_u32;
                while retry_after_reject(answers, cap) {
                    answers += 1;
                }
                prop_assert_eq!(answers, cap + 1);
            } else {
                prop_assert!(asked > policy_max.min(MAX_ANSWER_RETRIES));
            }
        }
    }
}
