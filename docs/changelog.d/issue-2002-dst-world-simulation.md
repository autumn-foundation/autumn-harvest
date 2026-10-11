## Testing — The real worker loop under deterministic simulation (issue #2002)

Issue #1830 simulated the activity claim protocol on an in-memory oracle.
It did not run `worker.rs`. The new world simulation runs the real worker
loop on Postgres, and a seed picks every step.

**Harness.** `autumn_harvest::dst::world` is test infrastructure, not a
stable API. It needs no database. A seeded planner picks one action per
step. The actions are:

- a worker poll, a beat, a stall, a crash, an abandoned claim or a restart;
- a signal from a client;
- a schedule scan or a fire claim, by one of two scheduler replicas;
- a reclaimer pass, a sweeper pass or a clock advance.

A `World` applies the action, and the driver awaits it. The driver then
reads each history as facts and checks 11 invariants. A run has a fault
phase and a drain phase. The drain phase injects no fault.
`Converges` fails a run whose work does not finish.

**Postgres world.** `tests/integration/dst_world_tests.rs` runs 3 real
`Worker`s on a fresh database per run. A poll calls the new
`#[doc(hidden)]` hook `Worker::dst_poll_once`. It runs one `poll_once` and
waits for the task body. `Worker::dst_register` writes the liveness row.
The scope covers these paths:

- the claim, cold and warm decisions, and the resident path;
- timers and signals;
- the scheduler fire claim, with stale snapshots;
- the orphan reclaimer and the timeout sweeper, on abandoned claims.

`scheduler::due_workflow_schedules` now holds the due-list query of the
scheduler tick, so the simulation reads the same list.

**Clock.** A shift moves every `timestamptz` column of each `harvest_*`
table back. Each step shifts the clock by one minute. An advance shifts it
by one more tick (one day) plus 1 ms. The event log keeps its timestamps.
A workload deadline is a whole number of ticks. Steps are at least one
minute apart, so a shorter engine duration compares by virtual time. The
extra millisecond keeps two schedule slots from sharing one workflow id.
No clock seam enters production code.

**Replay.** Each seed runs twice on fresh databases, and the two reports
must be equal. A failure prints a command that sets the seed, the plant and
the checks. `HARVEST_DST_WORLD_PLANT=foreign-state` plants a workflow that
branches on its worker. Seed 0 then fails `Deterministic`. The sticky
window of the first worker ends, and the cold replay on another worker
takes the other branch. `a_planted_failure_replays_from_its_seed_alone` rebuilds the
config from the replay variables alone. It gets the same violation and
trace.

**CI.** `dst_world_tests` is a `linux` row of the integration manifest. It
runs 4 seeds per PR. The nightly `world` job runs 1,000 seeds in 4 shards,
and a failure opens the alert issue.

**No migration. No new `WorkflowEvent` variant.** `Worker` gains two
`#[doc(hidden)]` hooks. See `docs/testing/simulation.md` and the amendment
to ADR 0004.
