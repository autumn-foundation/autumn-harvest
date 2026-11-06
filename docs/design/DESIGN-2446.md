# DESIGN-2446: step-level beam search

Issue #2446. Parent epic #2438. Needs #2440 (draw identity), #2441
(scorers and `select`) and #2443 (`agent_sample`).

## 1. Problem

Best-of-N scores whole runs only. A run that goes wrong at step 2 still
pays for each later step. Cascades `beam_search_by_score` scores each
partial trace and keeps the best `nbeams`. A clone of a trace replays the
recorded prefix, so it does not pay again for that prefix.

Harvest has no way to run one model turn from a transcript, score it and
go on from the best turns.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where the strategy lives | a new workflow · **a third `SampleStrategy` of `agent_sample`** |
| How one turn runs | model and tool activities in the beam workflow · **a child `agent_loop` run in step mode** |
| How a step continues a beam | a new user message · **`history` holds the beam, and the step adds no message** |
| How a step ends | `max_steps: 0` · **a new stop, `step_ended`, after the first tool round** |
| The step scorer | a new trait · **a `Scorer` that the input names, run in `agent_sample_score`** |
| How a round keeps beams | sort by score · **`select` again and again over the rest, so a tie goes to the lowest draw id** |
| The draw count | count each round live · **`width × expansion × max_rounds`, admitted before the first call** |
| The report | a new type · **`SampleReport`, with a `beam` record on each draw** |

## 3. Reverse brainstorming

"How could a beam run pay twice, write or lie?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| A kept beam runs its prefix again. | A step continues `history`. It sends the prefix as data and asks for one new turn. |
| Two expansions of one beam share a cached answer. | Each expansion has its own draw id, so its cache key differs. |
| A crash in round 2 runs round 1 again. | Each step is a child run. History records its end and its verdict. |
| A step runs a write tool. | A step gets `draw_effects`, as a draw does. |
| A beam request asks for more turns than the policy allows. | `admit` checks `width × expansion × max_rounds` against `max_draws`. The product saturates, so an overflow is refused too. |
| A width of 0 runs nothing and reports a pass. | A product of 0 is `no_draws`. |
| A rejected beam stays live. | Only a scored beam can stay. A reject never beats a score. |
| A failed step stays live. | A step that does not end `completed` or `step_ended` gets a reject and no score call. |
| A tie depends on the completion order. | Each rank uses `select`. The draw ids follow the parent order, and the order is recorded. |
| A complete beam runs again. | A beam that ends with a final answer leaves the pool. |
| The live pool grows past the width. | Each round keeps at most `width` minus the complete beams. |
| A policy field rides in the input. | `SampleInput` denies unknown fields. |
| A path runs more tool rounds than the profile allows. | A step of round `r` gets `max_steps - r`. |
| An answer-only scorer rejects each tool step, so no beam passes a tool round. | `ExactMatch` and `JsonSchema` give a `step_ended` step the score 0. |
| A step drops the flow labels of its beam, so a flow rule misses a pair of labels from two rounds. | Each beam keeps the flow labels of its last step. The next step gets them as `flow_labels`, as a continuation does. |

## 4. Six thinking hats

- **White (facts).** `agent_loop` adds a user message for `input` in each
  segment. `history` holds earlier turns. `run_draw_tasks` gives each draw
  the run id `{root}:s{n}`. `agent_sample_score` returns a verdict and the
  judge spend. `select` is proved in Verus (#2441).
- **Red (feelings).** An operator wants to see each round, not only the
  winner. Each draw record names its round, its parent and its fate.
- **Black (risks).** `AgentStop` and `AgentTask` are recorded types. The
  new field is skipped when false, so old histories decode. `AgentStop`
  is `non_exhaustive`. The loop guard of a step sees one turn only, so it
  cannot see a loop across rounds. `max_rounds` bounds that loop.
- **Yellow (benefits).** A bad path is cut after one turn. A crash costs
  only the steps in flight. The budget, the effects and the scorer gate of
  `agent_sample` apply unchanged. Verus proves the draw bound of the plan.
- **Green (ideas).** A later issue can share a cached prefix across the
  expansions of one beam. It can also check the size of each beam against
  the budget left before a wave.
- **Blue (process).** Red tests first, one per acceptance line. Then the
  step mode, the kernel and its proof, the beam loop and the docs.

## 5. Decision

```rust
pub enum SampleStrategy {
    BestOfN { n: u32 },
    Rejection { max_attempts: u32, threshold: FiniteF64 },
    Beam { width: u32, expansion: u32, max_rounds: u32 },
}
pub enum SampleStop { Completed, Accepted, BudgetExceeded, ScoreFailed, RoundsExhausted }
pub struct BeamStep { round: u32, parent: Option<DrawId>, fate: BeamFate }
pub enum BeamFate { Live, Complete, Pruned }
```

1. `SampleStrategy::Beam` maps to the policy strategy `beam`. The policy
   admits `beam_draws(width, expansion, max_rounds)`, the saturated
   product, as the draw count. The scorer of the input is the step scorer.
2. `AgentTask::beam_step` puts a run in step mode. The run adds no user
   message, because `history` holds the beam. It ends after its first
   model turn. A final answer ends it `completed`. A tool round ends it
   `step_ended`, and the transcript holds the turn and its results. A step
   has no answer check and no context reset, so it makes one model call.
   Its `max_steps` is the profile cap less the tool rounds of its beam.
3. Round 0 has one beam: the user message. Each round expands each live
   beam `expansion` times. Expansion `j` of beam `b` gets the next draw
   id, in the parent order. The steps run in waves of `sample_step`, under
   the shared token budget of `agent_sample`.
4. The run scores each step with `agent_sample_score`. The input holds the
   whole transcript of the beam. `ExactMatch` and `JsonSchema` give a
   `step_ended` step the score 0.
5. The round keeps the top `width - complete` steps by `beam_rank`, which
   calls `select` again and again. A kept step that completed joins the
   complete pool. Any other kept step is live.
6. The run stops with `completed` when `width` beams are complete or no
   live beam is left. It stops with `rounds_exhausted` after `max_rounds`.
   A budget refusal or a failed score stops it as in `agent_sample`.
7. The outcome is `select` over the complete beams. With none, it is
   `no_accepted_draw`. `selected` is the report of the best complete beam,
   so its transcript is the path.
8. Each draw record holds `beam`: its round, its parent and its fate. So
   the report holds the verdicts of each round.

**Known limits.**

- Each step sends the whole prefix as input tokens. A step pays for the
  prefix tokens, but it asks for no turn of the prefix again. A step can
  pass its share of the budget by the size of its beam.
- A step that delegates also runs the model calls of its child.
- The loop guard of a step sees only its own turn.
- A step input holds the whole transcript. The request cap of the profile
  stops a step with `transcript_full`, and the step is pruned.

## 6. Acceptance criteria and their tests

| Criterion | Test |
|---|---|
| Width 2, expansion 2 finds the known path; model calls equal the expansions; no turn asked twice | `a_beam_of_width_two_finds_the_path_that_the_step_scorer_prefers` in `tests/sample_beam.rs` |
| A crash in round 2 resumes in round 2 | `a_crash_in_round_two_resumes_in_round_two` in `tests/sample_beam_crash.rs` |
| `max_rounds` with no complete beam gives `no_accepted_draw` | `max_rounds_with_no_complete_beam_selects_nothing` |
| A product over `max_draws` is refused before a model call | `a_beam_over_max_draws_is_refused_before_any_model_call` |
| Same history, same selected beam | `the_beam_history_replays_to_the_same_selection` |
| A flow rule sees the labels of each round | `a_beam_carries_its_flow_labels_into_the_next_step` |
| Review follow-ups | `a_built_in_scorer_keeps_a_tool_step_and_checks_the_answer`, `a_beam_path_never_runs_more_tool_rounds_than_the_profile_allows`, `a_round_keeps_by_score_not_by_draw_order`, `keep_beams_fills_the_slots_by_score_and_counts_the_complete` |
| `beam` must be in `strategies` | `a_kind_that_does_not_list_beam_is_refused` |
| The kernel | `formal/verus/src/agent_sampling.rs` and the unit tests of `admit.rs` |
