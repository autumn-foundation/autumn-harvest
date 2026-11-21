# DESIGN-2445: the model cascade strategy

Issue #2445. Parent epic #2438. Needs #2443 (`agent_sample`). Related:
#2271 (a model per agent), #1996 (cost ledger), #1997 (LLM budgets).

## 1. Problem

An operator picks one model for all traffic of a kind. The strong model
overpays on easy inputs. The cheap model loses quality on hard inputs.

A cascade asks a cheap model first. It scores the answer. It asks a
stronger model only when the score is low. Harvest has each part: a model
registry, a scorer, a cost ledger and a budget. It has no rule that joins
them.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where the strategy lives | a new workflow · **a new `SampleStrategy::Cascade` in `agent_sample`** |
| The shape of a tier | a model only, one threshold for the run · **a model and a threshold per tier** |
| Which models a tier may name | any model of the harness · `judge_models` · **a new policy key, `cascade_models`** |
| How the policy check runs | in the draw · **in admission, before the first model call** |
| A tier that the mandate forbids | refuse the run · fail the draw · **skip the tier, and name it in the report** |
| The draw id of a tier | a counter of the draws that ran · **the tier index** |
| The concurrency | the cap of the policy · **1; a cascade asks one tier at a time** |
| A tier that fails | stop the run · **a reject, then the next tier** |
| No tier accepts | `NoAcceptedDraw` · **`Escalated { draw }`, the best verdict by `select`** |
| The model of each tier in the report | only in the ledger · **`SampleDraw::model`, and the ledger row** |
| The step kernel | reuse `sample_step` · **a new `cascade_step` that also skips forbidden tiers** |

## 3. Reverse brainstorming

"How could a cascade spend more, or pick worse, than it may?" Each answer
gives a rule.

| Failure | Rule |
|---|---|
| The cheap tier accepts, and the strong tier runs too. | The run stops after the first tier that accepts. A test counts the calls of the strong model. |
| A caller names an expensive model that the operator did not allow. | `cascade_models` admits each tier model before the first model call. The default is empty. |
| A crash after tier 1 calls tier 1 again. | Each tier is a child run, and each score is a recorded activity. Replay reads both. |
| A forbidden tier fails the whole run. | The run checks `Mandate::allows_model` first, skips the tier and names it with `not_permitted`. |
| Two tiers run at the same time and both are paid. | A cascade runs one tier at a time. A wider `concurrency` is refused. |
| The tiers spend N times the cap. | The tiers share one token budget. Each tier gets the rest. |
| A NaN threshold accepts each score. | A threshold is a `FiniteF64`. A NaN does not decode. |
| The ledger cannot tell the tiers apart. | Each tier asks its own harness model, so each ledger row holds the model id of its tier. |
| An older worker drops `cascade_models`. | The policy denies unknown keys. An older worker fails closed. The key skips its default on encode. |
| A tier index runs twice. | `cascade_step` returns a tier at or after `next`, and the run moves `next` past it. Verus proves the step and the loop. |
| `agent_draws` runs a cascade as a plain fan-out of the profile model. | `agent_draws` refuses `cascade` with `sample_only`. |
| A best-of-N report gains a field and changes its bytes. | `SampleDraw::model` is set for a cascade draw only. |

## 4. Six thinking hats

- **White (facts).** `agent_sample` runs draws in waves and scores each.
  `AgentTask::model` picks a harness model. The ledger records
  `AgentModel::model_id()` for each turn. `Mandate::allows_model` checks a
  model name. The loop already fails a run whose mandate forbids its
  model.
- **Red (feelings).** An operator wants a smaller bill with no loss of
  quality. A cascade that still calls the strong model each time would
  feel like a broken promise.
- **Black (risks).** `SampleStrategy` gains a `Vec`, so it is no longer
  `Copy`. That is a break of the Rust API, and the crate is pre-1.0. The
  wire form of the old variants does not change. A new outcome variant
  breaks an exhaustive `match` downstream.
- **Yellow (benefits).** One run gives the answer, the score of each tier
  and the cost of each tier. Each tier is durable, so a crash costs only
  the tier in flight.
- **Green (ideas).** A later issue can give each tier its own scorer, or
  a tier of more than one draw. The `Tier` type leaves room for both.
- **Blue (process).** Spec and proof of `cascade_step` first. Then one red
  test per acceptance line. Then the code, the docs and a review from
  several angles.

## 5. Decision

```rust
pub struct Tier { pub model: String, pub threshold: FiniteF64 }
pub enum SampleStrategy {
    BestOfN { n: u32 },
    Rejection { max_attempts: u32, threshold: FiniteF64 },
    Cascade { tiers: Vec<Tier> },
}
pub enum SampleOutcome {
    Selected { draw: DrawId },
    Escalated { draw: DrawId },
    NoAcceptedDraw,
}
pub struct SkippedTier { pub tier: u32, pub model: String, pub refusal: Refusal }
// SampleReport gains `skipped`, and SampleDraw gains `model`.
// SamplingPolicy gains `cascade_models: BTreeSet<String>`.
```

1. `admit` checks the strategy, the draw count (the tier count), the
   concurrency, the scorer and the judge model. `admit_tiers` then checks
   that `cascade_models` lists each tier model. A miss fails the run with
   `sampling refused (tier_model_not_allowed)`.
2. A cascade asks for a concurrency of 1. After the checks of step 1, a
   larger ask is refused with `too_many_concurrent_draws`.
3. `cascade_step` (the kernel) gives the next tier: the first tier at or
   after `next` that the mandate allows. With no such tier, the step is
   `Done`. With no token left, the step is `BudgetExceeded`.
4. The report names each tier that the mandate forbids with
   `not_permitted`. The mandate does not change during a run, so the list
   does not depend on why the run stops.
5. Tier `i` is draw `i`. It runs the task of the profile with
   `model = tiers[i].model`, and with the rest of the token budget.
6. The run scores the tier. A score at or above the tier threshold
   accepts it. The run then stops with `accepted`, and the outcome is
   `selected`.
7. A failed tier gets a reject, and the run asks the next tier. A budget
   stop or a failed score stops the run, as for the other strategies.
8. When no tier accepts, `select` picks the best verdict of the tiers that
   ran. The outcome is `escalated`. With each tier rejected, it is
   `no_accepted_draw`. A budget stop or a failed score can also give
   `escalated`, and `stopped` names the reason.
9. `agent_draws` refuses `cascade` with `sample_only`.

**Known limits.**

- One scorer scores each tier. A judge that is stronger than a tier still
  pays for itself on each tier.
- A tier model that the harness does not hold fails its draw. The run
  then asks the next tier.

## 6. Acceptance criteria and their tests

| Criterion | Test |
|---|---|
| The cheap tier accepts; the strong model is not called | `the_cheap_tier_accepts_and_the_strong_model_is_not_called` in `tests/sample_cascade.rs` |
| The cheap tier is low; both called once; strong selected | `a_low_cheap_score_escalates_to_the_strong_tier` |
| No tier accepts; the best is `Escalated` | `with_no_tier_accepting_the_best_answer_is_escalated` |
| A tier outside the mandate is skipped and named | `a_tier_outside_the_mandate_is_skipped_and_named` |
| One ledger row per tier with its model id | `the_ledger_has_one_row_per_tier_with_its_model_id` |
| A crash after tier 1 resumes at tier 2 | `a_crash_after_tier_one_resumes_at_tier_two` in `tests/sample_cascade_crash.rs` |
| A tier model outside the policy is refused first | `a_tier_model_outside_the_policy_is_refused_before_any_model_call` |
| The kernel and the loop | `cascade_step` and `cascade_order` in `formal/verus/src/agent_sampling.rs`, and the proptest `a_cascade_step_never_runs_a_forbidden_tier` |
| Review follow-ups | `a_budget_stop_still_names_each_forbidden_tier`, `the_strategy_check_comes_before_the_cascade_concurrency_check`, `a_tier_model_that_the_harness_lacks_fails_and_the_next_tier_runs`, `a_cascade_with_no_tier_is_refused`, `agent_draws_refuses_a_cascade` and `a_best_of_n_report_records_no_draw_model` |
| The guide | `docs/agent-adapter.md`, section 20, "Model cascade" |
