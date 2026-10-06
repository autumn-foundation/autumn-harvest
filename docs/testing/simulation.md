# Deterministic simulation testing

Issue #1830 adds a seeded simulator for the activity claim protocol.
[ADR 0004](../adr/0004-deterministic-simulation-testing.md) records why it
uses an in-memory oracle and not a hypervisor.

A seed fixes every scheduling choice, every clock advance and every fault.
A failing seed therefore replays exactly, on any machine.

## What it simulates

The module is `autumn_harvest::dst`. One thread applies one store operation
per step:

| Op | Production statement |
|---|---|
| `Beat` | The worker's liveness row in `harvest_workers`. |
| `Claim` | `queue::claim_task`. |
| `Start` | The start fence, `append_activity_started_if_pending`. |
| `Heartbeat` | `queue::record_heartbeat`. |
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

The invariants are those of `formal/tla/ActivityClaim.tla`. Each claim gets
a ghost sequence number that the store never sees.

| Invariant | Meaning |
|---|---|
| `AtMostOneTerminal` | At most one terminal write takes effect per task. |
| `TerminalByCurrentClaim` | A terminal write takes effect only while its claim is current. |
| `OwnerWritesByCurrentClaim` | Every owner write takes effect only while its claim is current. |
| `HeartbeatByCurrentClaim` | A heartbeat takes effect only while its claim is current. |
| `ClaimIdsAreUnique` | No two live claims share `(task, worker_id, attempt)`. |

A run stops at the first failed invariant.

## The pre-fix bug

`Fencing::StateOnly` gives owner writes the guard from before issue #1789:
`state = 'RUNNING'` only. A check of `TerminalByCurrentClaim` alone then
fails first at seed 6:

```text
0071 t=034151 w1.3/s1 #7 start t2 a2 -> applied
0076 t=036447 w2.1/s0 #4 start t2 a1 -> applied
0078 t=036619 w2.1/s0 #4 complete t2 a1 -> applied
0078 t=036619 VIOLATION TerminalByCurrentClaim: Complete by claim #4 took effect on t2, held by claim #7
```

Claim 4 (`w2.1`, attempt 1) was requeued while its worker looked dead.
Claim 7 (`w1.3`, attempt 2) then took the row. The stale claim 4 still
finished the task, so its result won. This is the race of the
`ActivityClaimPreFix.cfg` counter-example in
[`formal-methods.md`](formal-methods.md). With `Fencing::ClaimEpoch`, seed 6
passes, and the fence rejects the stale write.

The `dst` target asserts both results:
`pre_fix_guard_reproduces_issue_1789` and
`claim_epoch_guard_rejects_the_stale_write_on_the_same_seed`.

## Run it

```bash
cargo test -p autumn-harvest --no-default-features --test dst
```

The `seed_sweep` test runs 200 seeds by default. These variables change it:

| Variable | Meaning |
|---|---|
| `HARVEST_DST_SEEDS` | The number of seeds. |
| `HARVEST_DST_SEED_BASE` | The first seed. The default is 0. |
| `HARVEST_DST_SEED` | Run this one seed only. |
| `HARVEST_DST_FENCING` | `claim-epoch` (the default) or `state-only`. |
| `HARVEST_DST_CHECKS` | A comma list of invariant names. The default is all. |

Every sweep runs each seed twice and compares the traces. A difference is a
source of nondeterminism in the harness, and the sweep fails.

## Replay a failing seed

A failure prints the seed, the invariant, the last 40 trace lines and a
command such as:

```bash
HARVEST_DST_SEED=6 HARVEST_DST_FENCING=state-only HARVEST_DST_CHECKS=TerminalByCurrentClaim \
  cargo test -p autumn-harvest --no-default-features --test dst replay_one_seed -- --nocapture
```

The command names every config value, so the replay stops at the same
violation. It prints the full trace, one line per step. In
`w1.3/s1 #7 start t2 a2`, `w1.3/s1` is slot 1 of worker 1 in its third
incarnation, `#7` is the ghost claim number, and `t2 a2` is task 2,
attempt 2.

`golden_traces_are_equal_on_every_platform` pins the traces of seeds 0 to 2.
CI runs it on Linux, macOS and Windows, so a seed means the same run on
every platform.

## The differential test

`autumn-harvest/tests/integration/dst_differential_tests.rs` replays each
run's operation log on a real Postgres. It uses the statements in the table
above and requires the oracle's outcome and rows after every step. The scan
binds the simulated clock in place of `NOW()`. The liveness rows hold
simulated time too.

```bash
HARVEST_DST_SEEDS=50 cargo test -p autumn-harvest --test integration \
  dst_differential_tests:: -- --test-threads=1
```

A green differential run shows that the oracle models the SQL. A change to
an operation's SQL that the oracle does not copy fails this test.

## Nightly

`.github/workflows/dst-nightly.yml` runs every night:

- `sweep`: 1,000,000 oracle seeds in release mode, in 4 shards. The seed
  base comes from the run number, so each night tests new seeds.
- `differential`: 2,000 seeds against Postgres, in 4 shards.
- `alert`: a failed scheduled run opens an issue, or comments on the open
  one.

A manual dispatch can set the seed base and the sweep size.

## Add an operation

1. Add the variant to `Op` and its semantics to `OracleStore`.
2. Add the production statement to `PgStore` in the differential test.
3. Add an actor action in `sim.rs` and an invariant if the operation needs
   one.
4. Add a row to the table at the top of this page.

## Limits

- The harness drives the store statements, not the `worker.rs` loop.
- The scheduler draws actions from fixed weights. It does not search for
  rare interleavings, as PCT does.
- The oracle does not model the poison-pill quarantine. The reclaimer always
  requeues.
