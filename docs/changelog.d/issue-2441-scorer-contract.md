## Agents — Scorer contract and selection kernel (issue #2441, epic #2438)

The new `score` module gives a run a recorded quality signal. A `Scorer`
checks one run and returns a `Verdict`: `Score(FiniteF64)` or
`Reject(reason)`. `FiniteF64` refuses NaN and both infinities, and stores
`-0.0` as `0.0`. `AgentHarness::scorer` installs a scorer under its id.

The activity `agent_score` calls the scorer that a `ScoreRequest` names.
History records the verdict, so replay never calls the scorer again. A
non-finite score fails the activity with `ErrorKind::Decode`. An unknown
scorer fails it as `ScorerMissing`. `activities()` and `sqlite::register`
add the new activity.

A judge scorer calls `ScoreContext::chat`. Each call checks the LLM budget
first, then keeps one cost ledger call under the run that scores. An error
after a paid judge call is never retried. `AgentHarness::score_timeout`
bounds a score at 13 minutes. A budget
refusal fails the activity with `LlmBudgetExceeded`, even when the scorer
hides the error. SQLite has no run budget, so there the check passes.

`ExactMatch` and `JsonSchema` ship with the crate. `select` picks the draw
with the highest score. A tie goes to the lowest draw id, and a reject never
beats a score.

No event, migration or SQL changes. The design is in
`docs/design/DESIGN-2441.md`.

Tests: unit tests in `score.rs` cover the verdict, the kernel, each
built-in scorer and the judge path. Proptests cover the order key and
permutations. `tests/score.rs` aborts a child process after a score and
resumes the run on SQLite. The scorer stays at one call. It also checks a
NaN score and the judge ledger rows. `tests/score_postgres.rs` runs a real
worker. It checks the ledger rows, and that a run budget of 0 stops the
judge before its call. `formal/verus/src/agent_select.rs` mirrors `pick`,
`best_draw` and `key_of_bits`. The winner has the top key. A reject never
wins while a score exists. The input order does not change the result. The
key keeps the order of the bits, and a proptest checks the floats.
