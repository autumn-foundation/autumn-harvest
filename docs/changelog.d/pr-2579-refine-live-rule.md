## `agent_refine`: a live re-probe counts in selection (issue #2579)

Before, the live end of a probe stayed in the report. The composer, the
selector and the rule pick could not read it. The design is in
`docs/design/DESIGN-2579.md`.

- **The pool shows the live end.** `ProbedCandidate::live` holds the
  `LiveReprobe` of the probe. `AnonymisedEvaluation::live` holds the new
  `eval::LiveCheck`: `not_needed`, `skipped`, `no_end`, `completed` or
  `failed`. It holds no fork id and no cost. Each error loses each
  identity of the pool. The pool text adds the line `Live re-probe`.
- **A live failure.** A fork that ended and did not complete is a live
  failure. A failed fork or wait is `no_end`, and it is not a live
  failure. `LiveReprobe::failed` and `LiveCheck::failed` name it.
- **The rule.** The new kernel `refine::rule` orders the candidates. A
  live failure is out of the pick. Then a completed end, then the fewest
  stubbed tool calls, then the lowest id. The select step and the fallback
  of the run use it. `formal/verus/src/refine_rule.rs` proves it.
- **The selector.** A pick of a live failure is refused, with a note. The
  rule pick stands. When each candidate failed live, the step asks no
  selector and picks none.
- **Sequential mode.** A composition that failed live does not become the
  current overlay. The stage notes say so.

The patch `agent_refine_live_rule` gates the fallback rule and the
sequential carry. A run that started before the change replays with the
old rule. No new `WorkflowEvent` variant and no migration. A request or
an entry recorded before the change decodes with no live end.

**API notes.** `ProbedCandidate` gets `live`, and `AnonymisedEvaluation`
gets `live`. A struct literal of either type needs the new field.
`LiveCheck` is `#[non_exhaustive]`.

Tests:

- `refine_live.rs`: a scripted `RefineSources` fails some live runs on
  SQLite. A live failure changes the rule pick. A selector pick of one is
  refused. A pool of live failures picks none and asks no selector.
  Sequential mode keeps the overlay before a live failure. The composer
  reads each live end, redacted.
- Unit tests: `refine/rule.rs` (with a proptest against a model),
  `eval/anonymise.rs` (the live line and its redaction) and `refine/run.rs`
  (the map to `LiveCheck`, the fallback under the patch, an old decode).
