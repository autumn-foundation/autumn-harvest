# DESIGN-2447 — Score evaluations

Issue #2447. Parent epic #2438. Needs #2441.

## 1. Problem

`eval::evaluate` reports where a candidate diverges from a recorded run.
Divergence is not quality. A candidate that diverges at turn 1 with a better
answer looks the same as one with a worse answer. Cascades pairs each trace
with a score, so an experiment can compare methods by score.

This design adds a scorer to a candidate. The report then holds a verdict
for each run end.

## 2. Brainstorming

Options for each open question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where the scorer comes from | a scorer id in the harness · **`Candidate::scorer(Arc<dyn Scorer>)`** · an `evaluate` argument |
| Where the scorer runs | the `agent_score` activity in the test engine · **a direct call after the engine run** |
| Which models a judge calls | a separate judge harness · **the candidate harness, through `ScoreContext::chat`** |
| How the report holds a verdict | a `RunEnd` field · **`Option` fields on `Evaluation`, skipped when `None`** |
| How the report holds judge tokens | added to the candidate usage · **`Evaluation::scorer_usage`, apart from the candidate** |
| When the source is scored | after the candidate run · **before the first candidate model call** |
| How a scorer error shows | a reject · `None` · **`EvalError::Scorer`** |
| How `compare` scores the source | once per evaluation · **once per scorer and harness, shared** |
| How `Comparison` shows the verdicts | two new fields · **`Comparison::verdict(Side)` over the two evaluations** |

## 3. Reverse brainstorming

"How could a scored evaluation lie, or cost too much?" Each answer gives a
rule.

| Failure | Rule |
|---|---|
| A report with no scorer changes shape, and an old reader breaks. | Each new field is `None` with no scorer, and serde skips it. A golden test pins the JSON. |
| A failed end gets a verdict, and the report ranks a crash. | A failed end gets `None`. The scorer never sees it. |
| Judge tokens enter the candidate usage, and a cheap candidate looks expensive. | Judge tokens go to `scorer_usage` only. The candidate report keeps its own usage. |
| A broken scorer fails after the candidate paid for each turn. | The source is scored first. A scorer error then costs no candidate call. |
| A scorer error reads as a reject, so a bad judge sinks a good candidate. | A scorer error fails the evaluation with `EvalError::Scorer`. It names the side. |
| A non-finite score enters the report. | `Verdict` holds a `FiniteF64` only (#2441). A scorer that returns one fails with `ErrorKind::Decode`. |
| A judge hangs, and the evaluation never ends. | `AgentHarness::score_timeout` bounds each score. |
| A huge reject reason fills the report. | The reason is cut to `MAX_REJECT_REASON_BYTES`. |
| `compare` scores the source twice, and a judge with a temperature gives two source verdicts. | The source is scored once per scorer and harness. Both evaluations share that verdict. |
| Two candidates share a source verdict, but each judge calls its own harness. | A source verdict is shared only for the same scorer and the same harness. |
| A pool report leaks a model name through a reject reason. | `compare_anonymised` redacts each reject reason, as it does an error text. |
| The source verdict is read from history, but the source had no scorer. | The source verdict is computed now, from the recorded report. |

## 4. Six thinking hats

- **White (facts).** `ScoreInput::from_run` builds the input from a task and
  a report. `ScoreContext::unmetered` gives a context with no budget and no
  ledger. The evaluation writes no ledger row and has no run budget. A
  `RunEnd::Completed` report holds the final answer and the transcript.
- **Red (feeling).** A score next to a divergence is what an operator wants
  to read first. A second field for the same verdict would confuse.
- **Black (risks).** Shape drift of the JSON. Judge tokens in the wrong
  bucket. A paid candidate run lost to a scorer fault. Section 3 answers
  each one.
- **Yellow (value).** An experiment can rank overlays and models by score,
  not only by divergence. The same `Scorer` serves the eval harness and the
  `agent_score` activity.
- **Green (alternatives).** Run `agent_score` in the test engine. It needs a
  mock with the same logic, and the history is thrown away. A direct call is
  simpler. Retry a scorer error. A judge call can be paid before the error,
  so a retry pays again unseen. The evaluation fails instead.
- **Blue (process).** Red tests for each criterion, then the code, then the
  refactor. The docs and the changelog fragment come last.

## 5. Decision

```rust
impl Candidate {
    pub fn scorer(self, scorer: Arc<dyn Scorer>) -> Self;
}
pub struct Evaluation {
    // ...
    pub recorded_verdict: Option<score::Verdict>,
    pub candidate_verdict: Option<score::Verdict>,
    pub scorer_usage: Option<TokenUsage>,
}
impl Comparison {
    pub fn verdict(&self, side: Side) -> Option<&score::Verdict>;
}
```

1. With no scorer, the three fields are `None`. Serde skips them, so the
   JSON does not change.
2. The source is scored before the first candidate model call. A source that
   failed gets `None`.
3. After the candidate run, a completed candidate end is scored. A failed end
   gets `None`.
4. Each score calls the scorer with `ScoreContext::unmetered` over the
   candidate harness, under `AgentHarness::score_timeout`. `scorer_usage`
   sums the usage of each judge reply. It is zero for a pure scorer.
5. A scorer error, or a score past the timeout, fails with
   `EvalError::Scorer`. It names the side that the scorer failed on.
6. `compare` and `compare_anonymised` score the source once per scorer and
   harness. A judge calls the candidate harness, so a different harness can
   give a different verdict. Each is the same when it is the same `Arc`.
   The first evaluation with that pair holds the usage of that score.
7. `Comparison::verdict(Side::Recorded)` is the verdict of the first
   candidate, as in `Comparison::turns`.
8. `AnonymisedEvaluation` keeps the three fields. A reject reason is
   redacted. The text shows each verdict that is present.
9. The scorer does not enter `Candidate::digest`. It judges the candidate.
   It is not a part of it.

## 6. Acceptance criteria and their tests

All tests are in `autumn-harvest-agent/tests/eval.rs`.

| Criterion | Test |
|---|---|
| `ExactMatch("42")`, source `"41"`, candidate `"42"` | `exact_match_rejects_the_source_and_scores_the_candidate` |
| No scorer: both `None`, same JSON | `with_no_scorer_the_report_keeps_its_json` |
| A failed candidate end has no verdict | `a_failed_candidate_end_has_no_verdict` |
| Judge tokens in `scorer_usage` only | `judge_tokens_go_to_scorer_usage_not_to_the_candidate` |
| `compare` gives both verdicts per side | `compare_with_a_scorer_gives_both_verdicts_per_side` |
| Extra: a failed source, a scorer error, a timeout, a pool | `a_failed_source_end_has_no_verdict`, `compare_scores_the_source_once_per_scorer`, `a_scorer_error_on_the_source_costs_no_candidate_call`, `a_scorer_error_on_the_candidate_names_the_candidate`, `a_scorer_past_its_timeout_fails_the_evaluation`, `a_pool_keeps_the_verdicts_and_redacts_each_reject_reason` |
| Section 12 shows a scored comparison | `docs/agent-adapter.md`, "Score a comparison" |
