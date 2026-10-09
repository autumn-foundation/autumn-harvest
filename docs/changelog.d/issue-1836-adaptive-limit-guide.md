## Phase — Adaptive concurrency limit: operator guide and example (issue #1836)

The adaptive concurrency limit shipped with design notes and metrics, but
without an operator guide or an example. This follow-up adds both. It changes
no code in the library.

- New runbook `docs/runbooks/activity-concurrency-limit.md`. It covers when to
  use the limit and when to use a breaker, a retry budget or the slot tuner.
  It also covers how to turn it on, each policy field and when to change it,
  how the cap moves, what happens at the cap, the metrics with PromQL
  queries, and troubleshooting.
- New example `autumn-harvest/examples/adaptive_concurrency_limit.rs`. It
  shows the `WorkerConfig` setup. It then drives the real limiter against a
  simulated dependency on a virtual clock, so it needs no database. The cap
  settles at 29 against a knee of 20, falls to 14 when the knee drops to 8,
  and holds near 8 when calls above the knee fail.
- Chapter 7 of the getting-started guide gains a short section with a code
  sample. The design notes, the slot-tuner guide and the circuit-breaker
  runbook link to the new runbook.

No new `WorkflowEvent` variant, no migration, no schema change.
