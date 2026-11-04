## Score evaluations (issue #2447)

`eval::evaluate` and `eval::compare` now report a scorer verdict for each
run end. Divergence is not quality, so an experiment can now compare
methods by score.

- `Candidate::scorer(Arc<dyn Scorer>)` adds a `score::Scorer` (issue #2441).
- `Evaluation` gains `recorded_verdict`, `candidate_verdict` and
  `scorer_usage`. With no scorer, each is `None` and the JSON has no new
  key. A golden test pins that JSON.
- The scorer reads the final answer of each completed end. A failed end
  gets no verdict. The harness scores the recorded report. It reads no
  verdict from history.
- A judge scorer calls the models of the candidate harness. Its tokens go
  to `scorer_usage`, not to the candidate usage. `ScoreContext::usage`
  sums them.
- The harness scores the source before the first candidate model call. A
  scorer error fails with `EvalError::Scorer`. It names the side and holds
  the judge tokens before the error.
- `Comparison::first_verdict` and `second_verdict` give the verdict of
  each candidate. Two
  candidates with the same scorer and harness share one source verdict.
- `compare_anonymised` keeps the verdicts. It redacts each reject reason,
  with each judge route and its model id, then cuts it.
- Breaking for a struct literal or an exhaustive destructure of
  `Evaluation` or `AnonymisedEvaluation`: each has three new public fields.
- No new `WorkflowEvent` variant and no migration.

Tests: `autumn-harvest-agent/tests/eval.rs`, section "Scored evaluations".
Design: `docs/design/DESIGN-2447.md`.
