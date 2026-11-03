# DESIGN-2441 — Scorer contract and selection kernel

Issue #2441. Parent epic #2438. After Cascades (arXiv 2207.10342).

## 1. Problem

Cascades gives each trace a score. `observe` and `factor` add to it, and
`reject` sets it to minus infinity. Each inference method then ranks the
traces by that score.

Harvest cannot score a run. `eval::Evaluation` says where two runs diverge.
It does not say which run is better. `AgentReport` has no quality signal.
`ChatResponse` has no log-probabilities, so a score must come from a
verifier: a pure check, a test run or a judge model.

This design adds the scorer contract, a recorded `agent_score` activity, two
built-in scorers and a proven selection kernel. The strategies that use them
come later (#2443 to #2446).

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where a score comes from | log-probabilities · **a `Scorer` that returns a `Verdict`** · a tool call of the model |
| How a reject is stored | `Score(-inf)` · **`Verdict::Reject(reason)`** · `Option<f64>` |
| How a score refuses NaN | a check in `select` · **a `FiniteF64` newtype that refuses NaN and both infinities** · a clamp |
| Where the scorer runs | in workflow code · **in the `agent_score` activity** · in the strategy child run |
| How a judge call is metered | the app records it · **`ScoreContext::chat`, which checks the budget and keeps the ledger call** · a task-local meter |
| How the kernel orders floats | `f64::partial_cmp` · `f64::total_cmp` · **an `i64` order key, so the kernel is integer code** |
| How the kernel breaks a tie | first in input order · **the lowest draw id** · random |
| What the proof covers | the f64 path · **the integer kernel and the key of the bits; a proptest covers the step from floats to bits** |
| The judge interface | a judge field in `ScoreInput` · **a second `score` argument, `&ScoreContext`** · a global model |

## 3. Reverse brainstorming

"How could the scorer break a run, or the selection lie?" Each answer gives
a rule.

| Failure | Rule |
|---|---|
| Replay calls the scorer again, and a judge gives another verdict. | The scorer runs only in `agent_score`. History records its verdict. Replay reads the record. |
| A scorer returns NaN. Each comparison is then false, and the winner depends on the input order. | `FiniteF64` refuses NaN and both infinities. A non-finite score fails the activity with `ErrorKind::Decode`. |
| `-0.0` and `0.0` order apart, so a tie breaks on the sign bit. | `FiniteF64` stores `-0.0` as `0.0`. |
| A reject beats a low score. | A reject has no key. The kernel never picks an entry with no key while a keyed entry exists. |
| The winner depends on the order in which draws end. | A tie goes to the lowest draw id. The proof shows that the result depends only on the set of entries. |
| A judge call spends tokens that no ledger sees. | `ScoreContext::chat` keeps a ledger call for each judge call. The activity records each one under the run that calls it. |
| A scorer fails after a paid judge call. A failed attempt writes no row, so each retry pays again unseen. | An error after a paid judge call is never retried. At most one attempt goes unrecorded. |
| A failed budget read after a paid judge call keeps its retryable payload. | The refusal payload keeps its type, but turns non-retryable after a paid call. |
| Concurrent judge calls pass the ledger cap together. | `ScoreContext::chat` reserves a ledger slot before each call. A failed or cancelled call frees its slot. |
| A judge call runs past a spent budget. | `ScoreContext::chat` checks the LLM budget before each call. A refusal fails the activity with `LlmBudgetExceeded`, even when the scorer hides the error. The check sees committed spend only (section 5). |
| A scorer logs a huge reject reason or error, and history grows. | `run_score` cuts a reason and an error message to `MAX_REJECT_REASON_BYTES`. |
| A recorded verdict does not decode after an upgrade. | `Verdict` encodes as `{"score": x}` or `{"reject": "..."}`. A unit test pins the shape. |
| A scorer accepts a cut answer. | The built-in scorers reject a run that did not stop as `completed`. |
| A judge or a test-run scorer hangs. | Each judge call has the model timeout of the harness. `AgentHarness::score_timeout` bounds the whole score below `start_to_close`. SQLite needs that bound, because it reports `start_to_close` only after the body returns. |
| Two scorers share an id, so history names the wrong one. | The harness keys scorers by id. A later scorer with the same id replaces the earlier one. |
| An unknown scorer id retries forever. | It fails the activity at once, as `ScorerMissing`. |

## 4. Six thinking hats

- **White (facts).** `agent_model_turn` already checks the budget, then
  records a ledger call. The SQLite runtime records ledger calls from the
  closure result of `register_activity_with_ledger`. A failed attempt writes
  no row on either backend. `info::validate_against_schema` is the engine
  JSON Schema check. Verus cannot reason about `f64` order.
- **Red (feeling).** An unproven argmax would be the weak point of every
  strategy. Each strategy trusts it to pick the right draw.
- **Black (risks).** A non-finite score. Order dependence. A judge that
  bypasses the budget. A verdict that replay recomputes. Section 3 answers
  each one.
- **Yellow (value).** One run can now carry a recorded quality signal.
  `select` serves best-of-N, rejection, cascade and beam alike.
- **Green (alternatives).** A task-local meter keeps the issue signature, but
  hides the coupling. An explicit `&ScoreContext` argument shows it. Verus
  float support is weak, so the kernel orders an `i64` key. Verus proves
  that the key keeps the order of the bits. A proptest checks the floats.
- **Blue (process).** Spec, proof, red tests, then code. The Verus file
  comes first. Each acceptance criterion has a test.

## 5. Decision

```rust
pub struct FiniteF64(f64);                   // no NaN, no infinity, no -0.0
pub enum Verdict { Score(FiniteF64), Reject(String) }
// `DrawId` is `draw::DrawId` of issue #2440.
pub trait Scorer: Send + Sync + Debug {
    fn id(&self) -> &str;
    fn score<'a>(&'a self, input: &'a ScoreInput, cx: &'a ScoreContext<'_>)
        -> BoxFuture<'a, Result<Verdict, AgentError>>;
}
pub fn select(verdicts: &[(DrawId, Verdict)]) -> Option<DrawId>;
```

1. `ScoreInput` holds the task input, the final answer, the stop reason and
   the tool results of the run. `ScoreInput::from_run` builds it from an
   `AgentTask` and its `AgentReport`.
2. `AgentHarness::scorer` installs a scorer under its id. The `agent_score`
   activity takes a `ScoreRequest`: a scorer id and a `ScoreInput`. It
   returns the `Verdict`. History records it, so replay never calls the
   scorer again.
3. `Verdict::score(x)` refuses a non-finite `x` with `ErrorKind::Decode`. A
   recorded verdict that holds one does not decode.
4. A judge scorer calls `ScoreContext::chat`. The call picks a harness model,
   checks the LLM budget, calls the model under the model timeout, and keeps
   a ledger call. The activity records the ledger calls under the run that
   calls it, so a strategy run owns its judge tokens. A budget refusal fails
   the activity with the refusal, also when the scorer swallows the error.
5. `ExactMatch` and `JsonSchema` ship with the crate. Each gives
   `Score(1.0)` on a match and a reject with a reason on a miss.
6. `select` maps each score to an `i64` order key, then calls the kernel
   `best_draw`. The kernel picks the highest key, and the lowest draw id on a
   tie. It returns `None` only when no entry has a key.

**The order key.** For a finite `x`, `key = bits ^ ((bits >> 63) >>> 1)`,
where `bits` is `x.to_bits()` as `i64`. This is the order of
`f64::total_cmp`. Verus proves that the key keeps the sign-magnitude order
of the bits. IEEE 754 orders finite values in that order, except `-0.0`.
`FiniteF64` stores `-0.0` as `0.0`, so `a < b` exactly when
`key(a) < key(b)`. A proptest checks this last step.

**Failed attempts.** A failed attempt writes no ledger row on either
backend. So an error after a paid judge call fails the activity for good.
A retry cannot pay again unseen. `score_timeout` bounds a score at 13
minutes by default, below the 15-minute `start_to_close`. A score that ends
on the timeout keeps the same rule.

**Known limits.** No run budget exists on SQLite, so there a judge call is
not checked. The mesh budget does not cover a judge call yet. #2449 adds
strategies to a mesh. The budget sums committed ledger rows only. The judge
calls of one attempt therefore see the spend from before that attempt. A
model turn makes one call per attempt, so it has no such gap.

## 6. Acceptance criteria and their tests

| Criterion | Test |
|---|---|
| Verus proves the three properties | `formal/verus/src/agent_select.rs`: `theorem_the_winner_has_the_top_score`, `theorem_a_reject_never_wins_while_a_score_exists`, `theorem_the_result_does_not_depend_on_the_order`, with `theorem_the_order_key_keeps_the_bit_order` for the key |
| Any permutation gives the same draw | proptest `select_does_not_depend_on_the_order` in `score.rs` |
| `[(0, Reject), (1, Score(-1e300))]` gives `Some(1)` | `a_very_low_score_beats_a_reject` in `score.rs` |
| Only rejects, or no entry, give `None` | `only_rejects_give_none` and `no_entry_gives_none` in `score.rs` |
| A NaN score fails `agent_score` with `Decode` | `a_nan_score_fails_the_activity_with_decode` in `score.rs` and `a_nan_score_fails_the_run_with_decode` in `tests/score.rs` |
| A crash after the score replays the verdict, 1 call | `a_crash_after_the_score_replays_the_verdict` in `tests/score.rs` |
| Judge tokens in the ledger; a zero budget stops the call | `judge_tokens_land_in_the_ledger_of_the_run` in `tests/score.rs`, `a_zero_run_budget_stops_the_judge_before_its_call` in `score.rs`, and both again on Postgres in `tests/score_postgres.rs` |
| `ExactMatch` and `JsonSchema` accept and reject | four tests in `score.rs` |
| `comment-hygiene.py --base origin/trunk-dev` passes | the `lint` job |
