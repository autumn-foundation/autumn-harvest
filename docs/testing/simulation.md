# Deterministic simulation testing

Issue #1830 adds a seeded simulator for the activity claim protocol.
[ADR 0004](../adr/0004-deterministic-simulation-testing.md) records why it
uses an in-memory oracle and not a hypervisor.

A seed fixes every scheduling choice, every clock advance and every fault.
A failing seed therefore replays exactly, on any machine.

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
- `alert`: a failed scheduled run opens an issue, or comments on the open
  one. A step timeout counts as a failure.

Each job takes its seed base from the run number, so each night tests new
seeds. A manual dispatch can set the seed base and the sweep size.

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

- The harness drives the store statements, not the `worker.rs` loop.
- The scheduler draws actions from fixed weights. Unlike probabilistic
  concurrency testing (PCT), it does not search for rare interleavings.
- The harness does not model the timeout sweeper, the `FAILED` state or the
  poison-pill quarantine. The reclaimer always requeues, so many long runs
  reach strike counts that production would quarantine.
