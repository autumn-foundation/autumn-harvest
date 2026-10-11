# Deterministic simulation testing

Issue #1830 adds a seeded simulator for the activity claim protocol.
[ADR 0004](../adr/0004-deterministic-simulation-testing.md) records why it
uses an in-memory oracle and not a hypervisor.

A seed fixes every scheduling choice, every clock advance and every fault.
A failing seed therefore replays exactly, on any machine.

Issue #2002 adds a second harness, the world simulation. It runs the real
`worker.rs` poll loop on Postgres. See
[The world simulation](#the-world-simulation).

## What it simulates

The module is `autumn_harvest::dst`. It is test infrastructure, not a
stable API. One thread applies one store operation per step:

| Op | Statement |
|---|---|
| `Beat` | The worker's liveness row in `harvest_workers`. |
| `Claim` | `queue::claim_task`. |
| `Start` | The start fence, `append_activity_started_if_pending`. |
| `Heartbeat` | `queue::record_heartbeat`. |
| `Release` | `queue::release_unstarted_claim`. It subtracts 1 from `attempt`. |
| `Complete` | `finalize_activity_completion`. |
| `Scan` | `poison_pill::orphaned_running_tasks_query`. |
| `Requeue` | `poison_pill::requeue_orphan_stmt`. |

The actors are 3 workers with 2 activity slots each, and the orphan
reclaimer. The scan and the requeue are separate steps, so a worker can beat
between them. The clock is virtual.

The faults are:

- **Stall.** The worker keeps its claims but stops all work, as in a long GC
  pause or a partition. Its liveness row goes stale, so the reclaimer can
  requeue a row that the worker still holds.
- **Crash.** The worker loses its claims. It restarts later with a new id.

## Invariants

The harness checks six safety invariants of `formal/tla/ActivityClaim.tla`.
Each claim gets a ghost sequence number that the store never sees.

| Invariant | Meaning |
|---|---|
| `AtMostOneTerminal` | At most one terminal write takes effect per task. |
| `TerminalStateHasOneEvent` | A completed row has exactly one terminal write. |
| `TerminalByCurrentClaim` | A terminal write takes effect only while its claim is current. |
| `OwnerWritesByCurrentClaim` | Every owner write takes effect only while its claim is current. |
| `HeartbeatByCurrentClaim` | A heartbeat takes effect only while its claim is current. |
| `ClaimIdsAreUnique` | No two live claims share `(task, worker_id, attempt)`. |

A run stops at the first failed invariant. The TLA+ spec does not count a
start as an owner write. This harness does, so `OwnerWritesByCurrentClaim`
is stricter here.

## The pre-fix bug

`Fencing::StateOnly` gives owner writes the guard from before issue #1789:
`state = 'RUNNING'` only. With only `TerminalByCurrentClaim` checked, seed 3
is the first seed that fails. The trace ends with these lines (excerpt):

```text
0170 t=087034 reclaimer requeue t1 w2.3 s3 -> requeued
0176 t=090072 w2.3/s0 #6 heartbeat t1 a4 -> lease-lost
0183 t=092872 w2.3/s1 claim t1 -> claimed a5
0185 t=094170 w2.3/s0 #6 complete t1 a4 -> applied
0185 t=094170 VIOLATION TerminalByCurrentClaim: Complete by claim #6 took effect on t1, held by claim #7
```

Worker `w2.3` stalled, so the reclaimer requeued its claim 6 (attempt 4).
The same worker then claimed the row again in another slot, as claim 7
(attempt 5). The stale claim 6 still finished the task, so its result won.
This is the counter-example of `ActivityClaimPreFix.cfg` in
[`formal-methods.md`](formal-methods.md).

The `dst` target asserts these results:

- `pre_fix_guard_reproduces_issue_1789`: the pre-fix sweep fails at seed 3.
- `claim_epoch_guard_rejects_the_stale_write_on_the_same_seed`: the same
  operations on a store with the claim epoch reject the stale write.
- `pre_fix_release_reuses_a_live_fencing_token`: a stale release lowers
  `attempt`, so a new claim reuses a live pair. The fixed run of that seed
  passes.

## Run it

```bash
cargo test -p autumn-harvest --no-default-features --test dst
```

The `seed_sweep` test runs 200 seeds by default. These variables change it:

| Variable | Meaning |
|---|---|
| `HARVEST_DST_SEEDS` | The number of seeds. It must be 1 or more. |
| `HARVEST_DST_SEED_BASE` | The first seed. The default is 0. |
| `HARVEST_DST_SEED` | Run this one seed only. |
| `HARVEST_DST_FENCING` | `claim-epoch` (the default) or `state-only`. |
| `HARVEST_DST_CHECKS` | A comma list of invariant names. The default is all. |

Every sweep runs each seed twice and compares the traces, the operation
logs and the counters. A difference shows nondeterminism in the harness,
and the sweep fails.

## Replay a failing seed

A failure prints the seed, the invariant, the last 40 trace lines and a
command such as:

```bash
HARVEST_DST_SEED=3 HARVEST_DST_FENCING=state-only HARVEST_DST_CHECKS=TerminalByCurrentClaim \
  cargo test -p autumn-harvest --no-default-features --test dst replay_one_seed -- --nocapture
```

The command sets every variable that the config reads, so the replay stops
at the same violation. The replay runs the seed twice, so a nondeterminism
failure also reproduces. It prints the full trace. Each line starts with the
step number and the virtual time.

In `w2.3/s1 #7`, `w2.3` is worker 2 in its third incarnation. `s1` is its
second slot, because slots count from 0. `#7` is the ghost claim number. In
`t1 a5`, `t1` is task 1 and `a5` is attempt 5.

`golden_traces_are_equal_on_every_platform` pins the traces of four seeds.
CI runs it on Linux, macOS and Windows, so a seed means the same run on
every platform.

## The differential test

`autumn-harvest/tests/integration/dst_differential_tests.rs` replays each
run's operation log on a real Postgres. It needs Docker or
`HARVEST_TEST_DATABASE_URL`.

- It uses the statements in the table above. `Beat` is a test-local upsert.
- The scan binds the simulated clock in place of `NOW()`. The liveness rows
  hold simulated time too.
- Outcomes and rows must equal the oracle's after every step.
- After every step, a completed row must have exactly one
  `ActivityCompleted` event, and any other row none.
- With `state-only`, the writes copy the pre-#1789 guard of the TLA+ spec.
  They are not production statements.

```bash
HARVEST_DST_SEEDS=50 cargo test -p autumn-harvest --test integration \
  dst_differential_tests:: -- --test-threads=1
```

A failure prints a command that replays that one seed on Postgres.

A green run shows that the oracle models the SQL for the compared columns.
A change to an operation's SQL that the oracle does not copy fails this
test.

## Nightly

`.github/workflows/dst-nightly.yml` runs every night:

- `sweep`: 4,000,000 oracle seeds in release mode, in 4 shards.
- `differential`: 2,000 seeds against Postgres, in 4 shards.
- `world`: 1,000 world seeds on Postgres, in 4 shards.
- `alert`: a failed scheduled run opens an issue, or comments on the open
  one. A step timeout counts as a failure.

Each job takes its seed base from the run number, so each night tests new
seeds. A manual dispatch can set the seed base and the sweep size.

## The world simulation

Issue #2002. The oracle harness drives single store statements. The world
simulation drives whole actors, and its workers run the real worker loop.

`autumn_harvest::dst::world` holds the driver. It needs no database. A
`World` applies each action. `PgWorld` in
`autumn-harvest/tests/integration/dst_world_tests.rs` is the Postgres world.

### Actors and code

| Action | Code that runs |
|---|---|
| `Poll` | `Worker::dst_poll_once`: one `poll_once`, then the task body to its end. |
| `Beat` | `workers::heartbeat_worker`. |
| `Signal` | `signal::send_signal`. |
| `ScheduleScan` | `scheduler::due_workflow_schedules`, the due list of the scheduler tick. |
| `ScheduleFire` | `scheduler::claim_and_fire_workflow_schedule`, the fire claim. |
| `Reclaim` | `poison_pill::reclaim_orphaned_tasks`. |
| `Sweep` | `timeout::enforce_timeouts_once`. |
| `Advance` | The clock. See below. |
| `Abandon` | `queue::claim_task`, then the start fence for an activity. The worker then dies. |
| `Stall`, `Crash`, `Restart` | Faults. |

A poll can run a cold decision, a warm decision on resident state, a
declined resident decision, an activity, or a task with no work left. Timers fire inside a decision.
Two scheduler replicas race for each fire claim. A scan and its fire are
separate steps, so a fire can use a stale snapshot.

The workload is 3 workers, 3 client workflows and one schedule. A client
workflow runs an activity, a timer, a signal wait and a second activity.
Each step awaits one command, so each decision can stay resident. The
schedule fires every 2 ticks, 3 times.

Faults:

- **Stall.** The worker keeps its memory, but it does not poll or beat.
  Its resident state can go stale.
- **Crash.** The worker drops its memory. It restarts later with a new id.
- **Abandon.** The worker claims a task and dies before it runs the task.
  For an activity, it passes the start fence first. The reclaimer requeues
  the claim after the worker goes stale. The sweeper times out a started
  activity after its start-to-close deadline of one tick.

The reclaimer quarantines no task in this workload. Its strike limit is out
of reach.

A run has a fault phase of 120 steps. A drain phase of up to 400 steps
follows. It has no fault, and it ends when all work is complete.

### The clock

The clock moves in ticks of one day. A shift subtracts an interval from
every `timestamptz` column of each `harvest_*` table:

- Each step shifts the clock by one minute.
- `Advance` shifts it by one more tick plus 1 ms.

The event log keeps its timestamps. Only the sweeper reads them, for
external signals and awaits, which the workload does not use.

A stored instant is its virtual time plus the real time of the write.
These rules make each comparison a function of the seed:

- A workload deadline is a whole number of ticks. A run has at most 1,000
  steps, so its minute shifts stay below one tick. A deadline thus comes
  due after the same number of advances on every run.
- Two writes in two steps are at least one minute apart. A shorter engine
  duration, such as the 30 s claim handicap of a new start, thus compares
  by virtual time. Real time cannot decide it.
- Two instants with equal virtual times compare by real time. The real
  times follow the order of the writes, and the seed fixes that order.
- The extra millisecond keeps two virtual instants apart. The engine builds
  ids from instants, such as the workflow id of a schedule slot.
- A step must take less than 20 s of real time, its snapshot included. A
  slower step stops the run as a harness error.

A scheduler holds its scan between two steps. The clock shifts the held row
with the table, so a stale snapshot can still win its claim.

### World invariants

| Invariant | Meaning |
|---|---|
| `OneTerminal` | An execution has at most one terminal event, and it is the last event. |
| `StatusMatchesHistory` | A terminal status has a terminal event, and the other way round. |
| `ActivityResultOnce` | An activity has at most one `ActivityCompleted`. |
| `TimerFiresOnce` | A timer fires at most once. |
| `TimerNotEarly` | A timer fires no earlier than its start plus its duration. |
| `ScheduleSlotOnce` | A schedule slot fires at most once. |
| `ScheduleNotEarly` | A slot fires no earlier than its due time. |
| `FireStartsRun` | A fire claim that wins starts exactly one execution. |
| `Deterministic` | No execution fails or blocks on non-determinism. |
| `ExpectedOutput` | A completed execution returns the expected value. |
| `Converges` | The drain phase completes all work. |

Each run gets a fresh database, cloned from a migrated template. Each seed
runs twice, and the two reports must be equal.

### The planted defect

`HARVEST_DST_WORLD_PLANT=foreign-state` plants a defect. The client
workflow picks its timer id from the index of the worker that runs it.
Resident state hides the defect while the workflow stays on one worker. A
fault moves the workflow, and a cold replay takes the other branch. Seed 0
fails `Deterministic` (excerpt):

```text
0067 t=009 w3.4 poll -> cold
0067 t=009   c0 + TimerStarted nap 86400s
0079 t=012 w3.4 poll -> activity
0083 t=012 w2.4 poll -> cold
0083 t=012   c0 + TimerFired nap
0083 t=012   c0 blocked
0083 t=012 VIOLATION Deterministic: c0 blocked or failed in state RUNNING
```

Worker `w3` has an even index, so `c0` started the timer `nap`. The sticky
window of `w3` then ended, and worker `w2` took `c0`. Its cold replay chose
`nap-odd`, and the engine blocked the run on non-determinism. Each decision
on one worker would have hidden the defect: a warm decision does not run
the branch again.

`a_planted_failure_replays_from_its_seed_alone` asserts that a config built
from the replay command alone gives the same violation and the same trace.

### Run it

```bash
HARVEST_DST_SEEDS=50 cargo test -p autumn-harvest --test integration \
  dst_world_tests::world_seed_sweep -- --nocapture --test-threads=1
```

It needs Docker or `HARVEST_TEST_DATABASE_URL`, and a role that can create
databases. These variables change it:

| Variable | Meaning |
|---|---|
| `HARVEST_DST_SEEDS`, `HARVEST_DST_SEED_BASE`, `HARVEST_DST_SEED` | As for the oracle sweep. The default is 4 seeds. |
| `HARVEST_DST_WORLD_PLANT` | `none` (the default) or `foreign-state`. |
| `HARVEST_DST_WORLD_CHECKS` | A comma list of world invariant names. The default is all. |
| `HARVEST_DST_WORLD_KEEP_DB` | Keep each world database for a look after the run. |

A failure prints the seed, the invariant, the last 40 trace lines and a
replay command:

```bash
HARVEST_DST_SEED=0 HARVEST_DST_WORLD_PLANT=foreign-state HARVEST_DST_WORLD_CHECKS=Deterministic \
  cargo test -p autumn-harvest --test integration \
  dst_world_tests::replay_one_world_seed -- --nocapture --test-threads=1
```

The replay runs the seed twice and prints the full trace. In
`0083 t=012 w2.4 poll -> cold`, `0083` is the step, `t=012` is the tick,
and `w2.4` is worker 2 in its fourth incarnation. `c1` is client
workflow 1, and `s0` is the first scheduled run. `a1` is the second
activity of its execution.

## The speculation model

Issue #2011 asks whether a worker can run the next decision while the
previous commit flushes. `autumn_harvest::dst::speculate` models that
question. It is an R&D spike with no database, and the engine does not
use it. [The spike report](../rnd/speculative-execution-spike.md) gives
the results and the verdict.

The model is a discrete-event simulation. Its clock counts virtual
microseconds, so it can measure latency. A seed fixes every duration and
fault, and every seed runs twice.

| Knob | Values |
|---|---|
| `HARVEST_DST_SPEC_MODE` | `serial` (the engine today), `gated`, `eager` |
| `HARVEST_DST_SPEC_FENCE` | `epoch`, `prefix-only` |
| `HARVEST_DST_SPEC_LOGGING` | `full`, `reads-only` |
| `HARVEST_DST_SPEC_PLANT` | `none`, `keep-on-failure` |
| `HARVEST_DST_SPEC_WORKLOAD` | `chain`, `fan-out` |
| `HARVEST_DST_SPEC_CHECKS` | a comma list of invariant names |

```sh
HARVEST_DST_SEEDS=1000 HARVEST_DST_SPEC_MODE=gated \
  cargo test --release -p autumn-harvest --no-default-features --test dst \
  speculate::speculation_sweep -- --nocapture
```

A failure prints the replay command. It sets `HARVEST_DST_SEED` and runs
`speculate::replay_one_speculation_seed`.

## Add an operation

1. Add the variant to `Op` and its semantics to `OracleStore`.
2. Add the statement to `PgStore` in the differential test. The match in
   `PgStore::apply` is exhaustive, so the build fails until you do.
3. Add an actor action and a `describe` arm in `sim.rs`. Add an invariant
   if the operation needs one.
4. Add a row to the table at the top of this page.
5. Update the values in `golden_traces_are_equal_on_every_platform`. If the
   pre-fix seed moves, update `pre_fix_guard_reproduces_issue_1789`, this
   page and the changelog fragment.

## Limits

- The oracle harness drives the store statements, not the `worker.rs`
  loop. The world simulation runs the loop, but one step is one whole
  poll. A race between two statements of one cycle is out of its scope.
- A world crash falls between two steps. An abandoned claim models a death
  before the task body, but no worker dies inside a body. The chaos
  `kill_at` points can close this gap.
- The world workload never fails an activity, and its reclaimer never
  quarantines. So it does not reach the quarantine or the `FAILED` state.
- The scheduler draws actions from fixed weights. Unlike probabilistic
  concurrency testing (PCT), it does not search for rare interleavings.
- The oracle harness does not model the timeout sweeper, the `FAILED`
  state or the poison-pill quarantine. Its reclaimer always requeues, so
  many long runs reach strike counts that production would quarantine. The
  world simulation runs the real sweeper and reclaimer, and abandoned
  claims give them work.
