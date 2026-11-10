## `agent_refine`: a live re-probe counts in selection (issue #2579)

Before, the live end of a probe stayed in the report. The composer, the
selector and the rule pick could not read it. The design is in
`docs/design/DESIGN-2579.md`.

- **The pool shows the live end.** `ProbedCandidate::live` holds the
  `LiveReprobe` of the probe. `AnonymisedEvaluation::live` holds the new
  `eval::LiveCheck`: `not_needed`, `skipped`, `no_end`, `completed` or
  `failed`. It holds no fork id, no cost and no host error. The pool
  redacts each identity in a run error, and each named route of the
  harness now counts as one. The pool text adds the line `Live re-probe`.
- **A live failure.** A fork that did not complete is a live failure: it
  failed, ended in another state, or did not end in time. A failed fork
  call or wait is `no_end`, and it is not a live failure.
  `LiveReprobe::failed` and `LiveCheck::failed` name it.
- **The rule.** The new kernel `refine::rule` orders the candidates. A
  live failure is out of the pick. Then a completed end comes first, then
  the fewest stubbed tool calls, then the lowest id. The select step and
  the fallback of the run use it. `formal/verus/src/refine_rule.rs` proves
  it.
- **The selector.** The select step refuses a pick of a live failure and
  adds a note. The rule pick stands. When each candidate failed live, the
  step asks no selector and picks none.
- **The carry.** A composition that failed live does not become the
  current overlay, in either mode. The stage notes say so.

The patch `agent_refine_live_rule` gates the fallback rule, the carry of
each mode and `ProbedCandidate::live`. A run that started before the
change replays with the old rule and its recorded inputs, so a strict
replay passes. The change adds no `WorkflowEvent` variant and no
migration. A request or an entry recorded before the change decodes with
no live end.

**API notes.** `ProbedCandidate` gets `live`, and `AnonymisedEvaluation`
gets `live`. A struct literal of either type needs the new field.
`LiveCheck` is `#[non_exhaustive]`.

Tests:

- `refine_live.rs`: a scripted `RefineSources` fails some live runs on
  SQLite, or loses their fork. A live failure changes the rule pick. The
  select step refuses a selector pick of one, and a failed select step
  falls back to the same rule. A pool of live failures picks none and asks
  no selector. Sequential mode keeps the overlay before a live failure. A
  lost fork is no live failure. The composer reads each live end,
  redacted. A history with no patch marker replays strictly.
- Unit tests: `refine/rule.rs` (with a proptest against a model),
  `eval/anonymise.rs` (the live line and its redaction) and `refine/run.rs`
  (the map to `LiveCheck`, the fallback under the patch, an old decode).
