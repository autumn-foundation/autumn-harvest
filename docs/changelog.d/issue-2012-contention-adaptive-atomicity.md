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
  backout). Each cell checks two invariants after the drain.
- **Rule and verdict.** `rule::choose` and `verdict::judge` implement the
  rule and criteria that `DESIGN-2012.md` fixed before the measurement.
- **Results.** Backout gives 1.53x the saga's goodput on a cold key with
  short steps. On a hot key with 20 ms steps, the saga and the hybrid give
  2.9x the goodput of backout. The rule fails in one cell, hot with short
  steps: it must use the observed hot-lock hold, not the configured step
  time.

The module is `#[doc(hidden)]` and has no production caller. No migration.
No new `WorkflowEvent` variant.

Also fixes two test targets that no longer compiled on `trunk-dev`:
`signal_tests` and the plugin's `mcp_tasks_integration` still set
`WorkflowResetRequest::refuse_erased_source`, which issue #1999 removed.

Tests: 24 unit tests for the rule, the verdict and the harness math.
`atomicity_spike_tests` (11 Postgres tests, one `#[ignore]`d measurement)
and the `contention_adaptive_atomicity_docs` guard.
