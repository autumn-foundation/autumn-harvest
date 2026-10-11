## R&D — Speculative durable execution, a DST spike (issue #2011)

Issue #2011 asks whether a worker can run the next decision while the
previous commit flushes. The answer is a no-go. The engine does not
change. See `docs/rnd/speculative-execution-spike.md`.

**Model.** `autumn_harvest::dst::speculate` is a discrete-event model with
no database. A seed fixes every duration and fault. It has three modes:
`serial` (the engine today), `gated` and `eager` (libDSE). It has two
fences, `epoch` and `prefix-only`, and two logging modes, `full` and
`reads-only` (Halfmoon). Six invariants cover owner commits, replay,
the commit gate, effects, output and convergence. Each draw depends only
on the seed and a key, so two modes pair seed by seed. Two planted repair
defects prove that the invariants catch a bad discard.

**Probe.** `HARVEST_BENCH_COMMIT_PROBE=1` makes the e2e bench record
persist and claim durations and run latency. The probe starts no metric
sampler. Without it, the bench keeps `NoOpMetrics`.

**Results.** On the bench workflow, commits are 2.6 % of unloaded
latency and 1.6 % under load. Gated speculation saves about 0 %, and it
keeps every invariant. Eager release saves 2.95 %, but it runs effects
twice and strands 6.1 % of runs under faults. The prefix check alone
keeps every safety invariant except owner identity.

**Gate.** `speculative_execution_docs` guards the report in the `lint`
job. The change adds no migration and no new `WorkflowEvent` variant.
