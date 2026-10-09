## Testing — The real worker loop under deterministic simulation (issue #2002)

Issue #1830 simulated the activity claim protocol on an in-memory oracle.
It did not run `worker.rs`. The new world simulation runs the real worker
loop on Postgres, and a seed picks every step.

**Harness.** `autumn_harvest::dst::world` is test infrastructure, not a
stable API. It needs no database. A seeded planner picks one action per
step: a worker poll, a beat, a stall, a crash, a restart, a signal, a
schedule scan, a fire claim, a reclaimer pass, a sweeper pass or a clock
advance. A `World` applies the action and the driver awaits it. The driver
then reads each execution's history as facts and checks 11 invariants. A
run has a fault phase and a drain phase. The drain phase injects no fault,
and `Converges` fails a run whose work does not finish.

**Postgres world.** `tests/integration/dst_world_tests.rs` runs 3 real
`Worker`s on a fresh database per run. A poll calls the new
`#[doc(hidden)]` hook `Worker::dst_poll_once`. It runs one `poll_once` and
waits for the task body. `Worker::dst_register` writes the liveness row.
The scope is the claim, cold and warm decisions, the resident path, timers,
signals, the scheduler fire claim with two racing replicas, the orphan
reclaimer and the timeout sweeper.

**Clock.** An advance moves every `timestamptz` column of each `harvest_*`
table back by one tick (one hour) plus 1 ms. The event log keeps its
timestamps. A workload deadline is a whole number of ticks, so it comes due
after the same number of advances on every run. The extra millisecond
keeps two schedule slots from sharing one workflow id. No clock seam enters
production code.

**Replay.** Each seed runs twice on fresh databases, and the two reports
must be equal. A failure prints a command that sets the seed, the plant and
the checks. `HARVEST_DST_WORLD_PLANT=foreign-state` plants a workflow that
branches on its worker. Seed 0 then fails `Deterministic`: a stale resident
state declines, and the cold replay on another worker takes the other
branch. `a_planted_failure_replays_from_its_seed_alone` rebuilds the config
from the replay variables alone and gets the same violation and trace.

**CI.** `dst_world_tests` is a `linux` row of the integration manifest. It
runs 4 seeds per PR. The nightly `world` job runs 2,000 seeds in release
mode, in 4 shards, and a failure opens the alert issue.

**No migration. No new `WorkflowEvent` variant.** `Worker` gains two
`#[doc(hidden)]` hooks. See `docs/testing/simulation.md` and the amendment
to ADR 0004.
