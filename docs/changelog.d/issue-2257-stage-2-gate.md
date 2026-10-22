## Measurement — Stage 2 gate: cold decisions and history load (issue #2257)

Stage 2 of epic #2247 is an owner-local disk cache (#2258). It ships only
if cold decisions pass 20% of decisions for a week, or the cold p99
passes 50 ms. This change builds the instruments for that gate and
records its bench half in `docs/rnd/2026-10-22-stage-2-gate.md`.

- **`harvest.workflow.resident.duration{workflow, queue, outcome}`.** A new
  histogram, in seconds. The worker records it with each
  `harvest.workflow.resident` count. The clock starts before the history
  load and stops at the end of the first drive. The p99 of
  `outcome="cold"` is the cold p99 of the gate. `MetricsRecorder` gains
  `record_workflow_resident_duration` with a no-op default, so an existing
  recorder still builds. The metrics-rs adapter bridges it.
- **Dashboard.** The starter pack gains "Stage 2 gate: cold share and cold
  p99".
- **`examples/history-load-gate`.** A new measurement crate. It times one
  history load from Postgres, through the worker load path, and from a
  local SQLite file, through `SqliteRuntime::load_history`. It times the
  cold load of every live run after an owner restart. It drives the real
  `WorkflowCache` to model the cold share of the 1,000-entry cap. It
  applies the thresholds to fleet values: one trip gives `go`, both values
  with no trip give `no-go`, anything else is `undecided`. CI lints it and
  runs its tests.

**Results.** One Postgres read of a 10,000-event history takes about
55 ms. The cold path reads the history twice, about 140 ms. At the
50,000-event cap it reads for about 0.9 s. A local SQLite read is 1.9 to
3.6 times faster than one Postgres read. Under owner mode, more than 1,250
live runs per owner give a cold share above 20% from the cache cap alone.

**Finding.** The cold path decodes the full history twice. A single read
would halve it with no new store.

**Verdict:** undecided until a fleet week gives both values. No migration.
No new `WorkflowEvent` variant.
