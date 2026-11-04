## Score evaluations (issue #2447)

`eval::evaluate` and `eval::compare` now report a scorer verdict for each
run end. Divergence is not quality, so an experiment can now compare
methods by score.

- `Candidate::scorer(Arc<dyn Scorer>)` adds a scorer of issue #2441.
- `Evaluation` gains `recorded_verdict`, `candidate_verdict` and
  `scorer_usage`. With no scorer, each is `None` and the JSON has no new
  key. A golden test pins that JSON.
- The scorer reads the final answer of each completed end. A failed end
  gets no verdict. The source verdict is computed now, not read from
  history.
- A judge scorer calls the models of the candidate harness. Its tokens go
  to `scorer_usage`, not to the candidate usage. `ScoreContext::usage`
  sums them.
- The source is scored before the first candidate model call. A scorer
  error fails with `EvalError::Scorer` and names the side.
- `Comparison::verdict(Side)` gives the verdict of each candidate. Two
  candidates with the same scorer and harness share one source verdict.
- `compare_anonymised` keeps the verdicts. It redacts each reject reason.
- No new `WorkflowEvent` variant and no migration.

Tests: `autumn-harvest-agent/tests/eval.rs`, section "Scored evaluations".
Design: `docs/design/DESIGN-2447.md`.
