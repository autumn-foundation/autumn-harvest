# DESIGN-2444: the vote strategy

Issue #2444. Parent epic #2438. Needs #2443 (`agent_sample`) and #2439
(policy).

## 1. Problem

Self-consistency samples many answers and keeps the most common one. It
needs no scorer and no judge. It works when an answer has a canonical
form, such as a number, a label or a tool name.

A naive vote pays for each draw. When the leader cannot lose, each draw
after that point is waste.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where the vote runs | a new workflow · **a third `SampleStrategy` of `agent_sample`, so it shares the waves, the budget and the draw effects** |
| The scorer field | keep it required and ignore it · **make it optional: best-of-N and rejection refuse `None`, and a vote refuses `Some`** |
| The built-in keys | run them in an activity · **run them in workflow code: they are pure functions of a recorded answer** |
| An app key function | a closure in the input · **an `AnswerKeyFn` on the harness, called by id in the recorded `agent_vote_key` activity** |
| An unknown app key | fail at the first draw · **probe the harness before the first draw, and refuse with `unknown_answer_key`** |
| `JsonField(path)` | a dotted path · **a JSON pointer (RFC 6901), read with `Value::pointer`** |
| `LastNumber` | parse as `f64` · **a decimal text in canonical form: no commas, no leading or trailing zeros, no `-0`** |
| Which draws vote | each draw · **a draw that completed; a failed or capped draw casts no vote** |
| Where the early stop runs | after each draw · **after each wave; a wave is the set of draws in flight** |
| The kernel shape | string maps · **integer slices: a key is the index of its first appearance, so Verus checks the engine copy** |
| The per-draw verdict | a new field only · **`Score(1.0)` for a vote, and a reject that names the reason for no vote, plus the key** |
| The default concurrency | 1 · **the cap of the policy, as for best-of-N; a caller who wants the early stop sooner sets a lower value** |

## 3. Reverse brainstorming

"How could a vote pick the wrong answer, or spend more than it may?"
Each answer gives a rule.

| Failure | Rule |
|---|---|
| The early stop picks a key that the full run would not pick. | Stop only when `lead > second + left`. Verus proves that the winner then holds for each way the left draws vote. |
| The winner depends on the order in which draws end. | The tally is a function of the multiset of `(draw, key)` votes. Verus proves it. The run also records draws in draw order. |
| A tie depends on the order of the keys in a map. | A tie goes to the key whose first vote has the lowest draw id. |
| A crash re-runs draws that ended. | Each draw is a child run. Replay reads each end and rebuilds the tally. |
| A vote carries a judge and pays for it. | A vote refuses a scorer before any model call. |
| A vote names an app key that the harness lacks, and pays for each draw first. | The run probes the harness once, before the first draw. |
| A long answer makes a long key, and the report copies it. | A key over 1 KiB casts no vote. |
| A failed draw votes with a partial answer. | Only a draw that ends `completed` votes. |
| A kind that does not list `vote` runs one. | `admit` checks the strategy first, before any model call. |
| An app key function is slow or not pure, and replay sees another key. | The `agent_vote_key` activity records each app key. Replay reads it. |
| No draw has a key, and the run still names a winner. | The outcome is `no_accepted_draw`. |

## 4. Six thinking hats

- **White (facts).** `agent_sample` runs waves of child runs under one
  token budget (#2443). `select` gives a tie to the lowest draw id. The
  policy already lists `vote`. Draws end in draw order in the
  `run_draw_tasks` result.
- **Red (feelings).** An operator wants the vote to cost less, never to
  answer differently. The early stop must never change the answer.
- **Black (risks).** `SampleStrategy` loses `Copy`, because a JSON path
  is a `String`. `SampleInput::scorer` becomes an `Option`. Both change
  the public API. An old input still decodes, because `scorer` is still
  read when present. A change to a built-in key is a change to workflow
  code, so it needs the usual care for in-flight runs.
- **Yellow (benefits).** A vote needs no judge, so its only spend is the
  draws. The early stop cuts the draws that cannot change the result.
- **Green (ideas).** The tally gives a confidence signal: the share of
  the winner. A later issue can add a draw cap from that share.
- **Blue (process).** The Verus spec and proof first. Then red tests, one
  per acceptance line. Then the kernel, the workflow and the docs. Then a
  review from several angles.

## 5. Decision

```rust
pub enum SampleStrategy {
    BestOfN { n: u32 },
    Rejection { max_attempts: u32, threshold: FiniteF64 },
    Vote { n: u32, key: AnswerKey },
}
pub enum AnswerKey { Trimmed, LastNumber, JsonField(String), Custom(String) }
pub trait AnswerKeyFn { fn id(&self) -> &str; fn key(&self, answer: &str) -> Option<String>; }
pub enum SampleStop { Completed, Accepted, BudgetExceeded, ScoreFailed, Settled }
pub struct VoteCount { key: String, votes: u32, first: DrawId }
pub struct Tally { /* the votes of a run */ }
```

1. `admit` checks the strategy, the draw count and the concurrency. Then
   the run checks the shape: best-of-N and rejection need a scorer, a
   vote refuses one, and a vote needs a valid key.
2. A `Custom` key probes the harness once in `agent_vote_key`. A missing
   function gives `unknown_answer_key`.
3. Each wave runs as for best-of-N. A draw that completed gets its key:
   a built-in key in workflow code, an app key in `agent_vote_key`.
4. After each wave, `Tally::settled(left)` compares `lead` and `second`
   with the draws not yet started. When the leader cannot lose, the run
   stops with `settled`.
5. The winner is the key with the most votes. A tie goes to the key whose
   first vote has the lowest draw id. The run selects that first draw.
   The report holds the full tally, the winner first.

The integer kernel is in `sampling/vote.rs`: `key_votes`, `pick_key`,
`vote_winner`, `runner_up` and `vote_settled`.
`formal/verus/src/agent_vote.rs` proves:

- the winner has the most votes, and the lowest first draw on a tie;
- when `vote_settled` holds, each extension by at most `left` votes keeps
  the winner;
- two vote sequences with the same multiset give the same winner.

**Known limits.**

- The early stop checks between waves. With a wide wave, the run can
  start draws that cannot change the result.
- A key compares as exact text. `LastNumber` reads `1e3` as two numbers.
- The early stop is always on. The proptest
  `an_early_stop_never_changes_the_winner` compares it with a full run.

## 6. Acceptance criteria and their tests

| Criterion | Test |
|---|---|
| Verus proof of both properties | `theorem_an_early_stop_keeps_the_winner` and `theorem_the_winner_does_not_depend_on_the_order` in `formal/verus/src/agent_vote.rs` |
| n = 5, concurrency 1, first 3 answers share a key: 3 draws | `three_matching_answers_of_five_stop_the_vote_at_three_draws` in `tests/vote.rs` |
| Keys `a, b, a, b, c`: `a` wins | `a_tie_goes_to_the_key_that_voted_first` |
| Early stop equals full run | the proptest `an_early_stop_never_changes_the_winner` in `sampling/vote.rs` |
| A draw with no key casts no vote | `a_draw_with_no_key_casts_no_vote` and `a_vote_with_no_key_ends_with_no_accepted_draw` |
| Crash resumes with the recorded tally | `a_crash_in_a_vote_resumes_with_the_recorded_tally` in `tests/vote_crash.rs` |
| `vote` must be in the policy | `a_kind_that_does_not_list_vote_is_refused_before_any_model_call` |
