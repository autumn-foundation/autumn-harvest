## R&D — physical backout, saga or hybrid per workflow (issue #2012)

An R&D spike asks whether Harvest can pick physical backout or a saga per
workflow from contention. The write-up is
`docs/rnd/contention-adaptive-atomicity.md`. **Verdict: go with changes.**

- **Backout runner.** `atomicity::backout::run_backout` runs one
  transaction. `Steps::step` gives each step a savepoint through nested
  `diesel-async` transactions. A step conflict makes the whole run retry
  through `tx_retry`, even when the body ignores the step error.
- **Harness.** One order workflow runs as `backout`, as `saga` (the real
  `saga::Saga` helper) and as `hybrid` (an escrow reserve step, then a
  backout). Each cell checks two invariants after the drain and measures
  the hot-lock hold.
- **Rule and verdict.** `rule::choose` and `verdict::judge` implement the
  rule and criteria that `DESIGN-2012.md` fixed before the measurement.
  Each criterion holds, fails or is inconclusive when the ranges overlap.
- **Results.** Backout gives 1.55x the saga's goodput on a cold key with
  short steps. On a hot key with 20 ms steps, the saga and the hybrid give
  2.9x and 3.0x the goodput of backout. The rule fails in one cell, hot
  with short steps. It must use the measured hot-lock hold, not the
  configured step time.

The spike is behind the new `atomicity-spike` feature, off by default. The
module is `#[doc(hidden)]` and has no production caller. No migration. No
new `WorkflowEvent` variant.

The PR also fixes two test targets that no longer compiled on
`trunk-dev`. `signal_tests` and the plugin's `mcp_tasks_integration` still
set `WorkflowResetRequest::refuse_erased_source`, which issue #1999
removed.

The evidence is 30 unit tests for the rule, the verdict and the harness
math, and 10 Postgres tests and one ignored measurement in
`atomicity_spike_tests`. The `contention_adaptive_atomicity_docs` guard
pins the report to the code.
