## Measurement — Resident-state hit rate (issue #2007)

#2114 added the counter `harvest.workflow.resident{workflow, queue,
outcome}`. This change measures it on two workloads and records the
results in `docs/rnd/2026-10-11-resident-hit-rate.md`.

- **e2e bench.** The `throughput` scenario reports the `outcome` counts of
  its measured loop in its notes. Its recorder returns `false` from
  `is_enabled`, so the worker starts no metric sampler. The recorder
  forwards query durations to the commit probe when the probe is on.
  `build_registry_with_metrics` builds the bench registry with any
  recorder.
- **Agent loop.** The new example crate `examples/agent-loop-hit-rate` runs
  the real `agent_loop` on a Postgres worker with an offline model. It sets
  the step cap of each run to `TURNS`. It fails if the decision total
  differs from `RUNS × (2 × TURNS + 2)`. CI lints it and runs its unit
  tests.

**Results.** On one worker, every run resumed warm after its first
decision. The bench reached 73.9% to 75.0%, and the agent loop reached 90.0%
at 4 tool turns and 96.2% at 12. Each replay was `cold`.

**Finding.** With two claim loops (#2106), the bench made 61 to 73 extra
decisions in 1,200 runs, each one `cold`. With one claim loop it made
exactly four per run. The record gives the numbers. The cause is open.

No engine change. No migration.
