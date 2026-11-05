# DESIGN-2443: the `agent_sample` workflow

Issue #2443. Parent epic #2438. Needs #2439 (policy), #2440 (draw
identity) and #2441 (scorers and `select`). Related: #2270 (concurrent
children) and #2148 (budget tree).

## 1. Problem

Cascades runs a program many times and keeps the best trace. Its draws
are not durable. A crash in its thread pool loses each draw, paid or not.

Harvest has the parts: `agent_draws` admits a strategy run and runs its
draws as child runs. `agent_score` records a verdict. `select` picks the
best draw. No workflow joins them, and no workflow stops on a budget.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Who builds the draw task | the caller gives an `AgentTask` · **the profile of the kind, as in `agent_draws`** |
| The strategy type | change the policy `Strategy` · **a new `SampleStrategy` with data, mapped to `Strategy` for the policy** |
| The scorer | a scorer object in the input · **a `ScorerRef` that the policy admits, run by id in an activity** |
| Where the score runs | in `agent_score` · **in a new activity, `agent_sample_score`, that also returns the judge usage** |
| How draws run | one batch · **waves of `run_draw_tasks`, the per-draw form of `run_draws`** |
| How rejection stops | after each draw · **after each wave; a wave holds at most the admitted concurrency** |
| What a rejection picks | the first pass · **`select` over the draws that pass, so a tie goes to the lowest id** |
| The budget tree | a new budget type · **the token cap of the profile, narrowed by the input, split over each wave with `sibling_cap`** |
| How a draw reports its cost | price the usage again · **each model turn carries the cost that the ledger recorded** |
| A judge outside `judge_models` | trust the scorer · **the score request pins the admitted judge model, and `ScoreContext::chat` refuses each other model** |

## 3. Reverse brainstorming

"How could a sample run spend, write or lie more than it may?" Each
answer gives a rule.

| Failure | Rule |
|---|---|
| A crash re-runs draws that ended. | Each draw is a child run. History records its end. Replay reads it. |
| N draws make N writes. | Each draw gets `draw_effects` met with the profile effects. The default is `[read_only]`. |
| The draws spend N times the cap. | The draws share one token budget. Each wave splits the rest with `sibling_cap`. The shares sum to the rest. |
| A wave starts a draw with a share of 0. A cap of 0 still allows one call. | A wave holds at most as many draws as tokens are left. With 0 left, the run starts no draw. |
| A budget refuses a draw, and the run starts the next one. | A draw that ends `budget_exceeded` stops the run. The run selects among the draws that it has. |
| A judge calls a model that the policy did not admit. | The request pins the admitted judge model. `chat` refuses each other model with `Config`. |
| A failed draw calls a paid judge. | The run does not score a failed draw. It records a reject. |
| A tie depends on the completion order. | `select` gives a tie to the lowest draw id. Verus proves it (#2441). |
| A NaN threshold accepts each score. | `threshold` is a `FiniteF64`. A NaN does not decode. |
| A rejection run reads `max_attempts` from the input, past the policy. | The policy admits `max_attempts` as the draw count. |
| Judge tokens are not in the per-draw usage. | `agent_sample_score` returns the usage and cost of its judge calls. The run adds them to the draw. |
| A policy field rides in the input. | `SampleInput` denies unknown fields. |

## 4. Six thinking hats

- **White (facts).** `agent_draws` reads the profile and the policy once.
  `run_draws` gives each draw the run id `{root}:s{n}`. `agent_score`
  returns a `Verdict` only. The ledger records the cost of each turn, but
  the workflow does not see it. `sibling_cap` splits a token rest, and
  Verus proves that the shares sum to the rest (#2270).
- **Red (feelings).** An operator fears N writes and N times the spend
  most. Both defaults must hold with no setting.
- **Black (risks).** A cost field on `ModelTurn` touches a recorded type.
  The field is optional and serde skips it when empty, so old histories
  decode. The loop checks a token cap between calls, so one call can pass
  it, as for a delegation.
- **Yellow (benefits).** Each draw is durable. A crash costs only the draws
  in flight. One report holds each verdict, usage and cost.
- **Green (ideas).** The same loop serves the vote (#2444) and the cascade
  (#2445). The step kernel is small, so Verus proves it.
- **Blue (process).** Red tests first, one per acceptance line. Then the
  kernel, its proof, the workflow and the docs. Then a review from several
  angles.

## 5. Decision

```rust
pub enum SampleStrategy {
    BestOfN { n: u32 },
    Rejection { max_attempts: u32, threshold: FiniteF64 },
}
pub struct SampleInput {
    kind: String, input: String, strategy: SampleStrategy,
    scorer: ScorerRef, concurrency: Option<NonZeroU32>,
    max_total_tokens: Option<u32>,
}
pub enum SampleOutcome { Selected { draw: DrawId }, NoAcceptedDraw }
pub enum SampleStop { Completed, Accepted, BudgetExceeded, ScoreFailed }
pub struct SampleReport {
    kind, strategy, revision, concurrency, outcome, stopped, attempts,
    selected: Option<AgentReport>, draws: Vec<SampleDraw>,
    usage: TokenUsage, cost_usd_micros: u64,
}
pub struct SampleDraw { draw, run_id, stop, verdict, usage, cost_usd_micros }
```

1. `agent_sample` reads the profile and the policy once each, as
   `agent_draws` does. `admit` checks the strategy, the draw count
   (`n` or `max_attempts`), the concurrency and the scorer.
2. The budget is the token cap of the profile, narrowed by
   `max_total_tokens`. `None` means no cap.
3. `sample_step` (the kernel) gives the width of the next wave. The width
   is at most the concurrency, the draws left and the tokens left. With
   tokens left at 0 and draws left, the step is `BudgetExceeded`.
4. Each wave is one `run_draw_tasks` batch, the per-draw form of
   `run_draws`. Draw `i` of a wave gets the cap `sibling_cap(left, width,
   i)`.
5. After a wave, the run scores each draw with `agent_sample_score`. A
   draw that failed, or that a budget stopped, gets a reject and no score
   call. So it never pays for a judge.
6. A draw that ends `budget_exceeded`, or a judge call that the LLM budget
   refuses, stops the run with `budget_exceeded`. Any other failed score
   stops it with `score_failed`, and the verdict names the error. The run
   keeps the report of each paid draw.
7. Best-of-N runs each draw, then calls `select`. Rejection runs one draw
   at a time unless the input asks for more. It stops after the first
   wave with a score at or above `threshold`, and calls `select` over the
   draws that pass. A pass wins over the other stops.
8. The run cuts each tool result of a score input to 16 KiB. A score
   input that still does not fit the activity input cap gets a reject.
9. Each model turn carries `cost_usd_micros`, the cost that the ledger
   recorded for it. `AgentReport` sums it, as it sums `usage`.

After the score, the spend of each draw and of its judge calls counts
against the budget.

**Known limits.**

- The loop checks a token cap between model calls. A share of 1 token
  still buys one call. So each draw of the last wave can pass its share
  by one call. The next wave then has no tokens, and the run stops.
- The LLM budget of the run checks a judge call (#2441). The token budget
  of the tree does not. The judge tokens count only after the score.
- A `Scorer` does not say whether it is a judge. Under the default policy,
  a caller can name a judge scorer with no judge model. The pin then
  refuses its first judge call, and the run stops with `score_failed`
  after one wave.
- The input text is in each draw and in each score input. `max_draws`
  bounds the copies.

## 6. Acceptance criteria and their tests

| Criterion | Test |
|---|---|
| Best-of-3 picks `"b"`, 3 verdicts | `best_of_three_picks_the_answer_that_the_scorer_prefers` in `tests/sample.rs` |
| A tie goes to the lower draw id | `a_tie_goes_to_the_lower_draw_id` |
| A crash resumes only the draw in flight | `a_crash_with_draw_two_in_flight_resumes_only_draw_two` in `tests/sample_crash.rs` |
| Rejection stops at the second draw | `rejection_stops_at_the_first_draw_that_meets_the_threshold` |
| Rejection with each draw rejected | `rejection_with_each_draw_rejected_ends_with_no_accepted_draw` |
| A budget for 2 of 4 draws | `a_token_budget_for_two_of_four_draws_stops_with_budget_exceeded` |
| A default draw never runs a `Write` tool | `a_default_draw_gets_a_refusal_for_a_write_tool` |
| Per-draw usage sums to the ledger | `the_per_draw_usage_sums_to_the_ledger_of_the_run_tree` |
| SQLite and Postgres | `tests/sample.rs` and `tests/sample_postgres.rs` |
| The kernel | `formal/verus/src/agent_sampling.rs` and the proptest `a_sample_step_never_passes_a_bound` |
| Review follow-ups | `a_wave_splits_the_rest_by_sibling_cap`, `a_failed_draw_is_rejected_and_not_scored`, `judge_spend_counts_against_the_tree_budget`, `rejection_with_a_wide_policy_still_draws_one_at_a_time`, `the_sample_history_replays_clean`, and `a_judge_call_to_a_model_that_the_run_did_not_admit_fails_the_score` |
