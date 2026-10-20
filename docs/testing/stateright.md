# Stateright model checks

[Stateright](https://www.stateright.rs) is a model checker for Rust. The
`stateright` test target uses it to visit every reachable state of two DST
oracles within a bound. [ADR 0011](../adr/0011-stateright-model-checks.md)
records why.

The repository already has three tools for these protocols. Each one leaves
a gap that this target fills:

| Tool | What it checks | Gap |
|---|---|---|
| TLA+ and TLC ([`formal-methods.md`](formal-methods.md)) | Every state of a spec | The spec restates the protocol by hand. |
| DST ([`simulation.md`](simulation.md)) | The Rust oracle, one schedule per seed | A sweep samples. It can miss a rare schedule. |
| Postgres differential test | The oracle against the production SQL | It replays the schedules that DST picked. |
| **Stateright** | **Every schedule of the Rust oracle, within a bound** | The bound. |

The checker calls the same `OracleStore` and `OracleOwnerStore` that the
differential test compares with Postgres. A guard change in an oracle is
therefore checked on every schedule, with no hand-written spec in between.

## Run it

```bash
cargo test -p autumn-harvest --no-default-features --test stateright
```

The run needs no database. It takes about 10 seconds in a debug build on 4
cores. The no-db test job runs it on Linux, macOS and Windows, because it is
part of `cargo test -p autumn-harvest --no-default-features`.

Set `HARVEST_STATERIGHT_DEEP=1` to check the larger bounds of the nightly
run. Use a release build for them:

```bash
HARVEST_STATERIGHT_DEEP=1 \
  cargo test --release -p autumn-harvest --no-default-features --test stateright
```

They take about 1 minute on 4 cores. The `stateright` job of
`.github/workflows/dst-nightly.yml` runs them every night.

Each check is breadth first, so a counter-example is short. The checker uses
one thread for each core. With several threads, a path is short but not
always the shortest.

## The activity claim model

`tests/stateright/activity_claim.rs` drives `OracleStore`, the oracle of the
activity claim protocol of issue #1789. The actions are those of `dst::run`:

| Action | Oracle operation |
|---|---|
| `Beat` | `Op::Beat` at the current tick. |
| `Claim` | `Op::Claim` into a free slot, for each task. |
| `Write` | `Op::Start`, `Op::Heartbeat`, `Op::Release` or `Op::Complete` by a slot's claim. |
| `Crash` | The worker loses its claims. Its rows stay `RUNNING`. |
| `Restart` | The next incarnation, with a new worker id, beats. |
| `Scan` | `Op::Scan`. It replaces the reclaimer's orphan list. |
| `Requeue` | `Op::Requeue` of the first orphan. |
| `Tick` | The clock moves 1 tick. A worker is dead 1 tick after its last beat. |

A stall needs no action, because any worker may skip any number of steps. A
slot follows the rules of `dst::run` after each write. A lost heartbeat
cancels the activity under the claim epoch only.

The ghost checker is `dst::Ghost`, the one that `dst::run` uses. Both
harnesses therefore judge a run by the same six invariants of
`formal/tla/ActivityClaim.tla`. A step that breaks an invariant records it in
the state, and an `always` property checks the record.

| Bound | Per PR | Nightly |
|---|---|---|
| Design: workers, slots, tasks | 2, 2, 1 | 2, 2, 1 |
| Design: claims, ticks, incarnations | 3, 1, 2 | 3, 2, 2 |
| Two rows: workers, slots, tasks | 2, 1, 2 | 2, 1, 2 |
| Two rows: claims, ticks, incarnations | 3, 1, 1 | 3, 2, 2 |

Two slots let a stalled worker claim its own requeued row again. That is the
trace of issue #1789. The per-PR design bound has about 156,000 states. The
nightly one has about 737,000.

With `Fencing::StateOnly`, the guard from before issue #1789, the checker
finds this counter-example to `TerminalByCurrentClaim` in 7 steps:

1. `w1.1` claims the row in slot 1: attempt 1.
2. Slot 1 starts the activity.
3. The clock ticks. `w1.1` sent no beat, so it looks dead.
4. The reclaimer scans and finds the row.
5. The reclaimer requeues the row.
6. `w1.1` claims the row again in slot 0: attempt 2.
7. Slot 1, the stale claim, completes the task. Its write matches
   `state = 'RUNNING'`, so its result wins.

This is the trace of `ActivityClaimPreFix.cfg`. The same pre-fix model also
breaks `OwnerWritesByCurrentClaim`, `HeartbeatByCurrentClaim` and
`ClaimIdsAreUnique`.

The witnesses are `sometimes` properties. The design must reach each one:

| Witness | Meaning |
|---|---|
| `TaskCompleted` | A task row reaches `COMPLETED`. |
| `StaleWriteFenced` | The guard rejects a write by a claim that the row no longer holds. |
| `StaleOwnerAfterFinish` | A started claim still runs on a row that another claim finished. `ActivityClaimReach.cfg` reaches the same state. |

## The owner-lease model

`tests/stateright/owner_lease.rs` drives `OracleOwnerStore`, the store half
of the fenced owner lease of issue #2250. Stage 1a (issue #2251) must match
that oracle on Postgres. The owner half copies the scheduler of
`dst::owner`. That scheduler draws one action per step, with a bias toward
the four nemeses. This model takes every enabled action, so it needs no bias
and no stall.

The actions are those of `formal/tla/OwnerLease.tla`: `Acquire`, `Release`,
`Renew`, `Begin`, `Lock`, `Commit`, `Write`, `Ack`, `Receive` and `Expire`.
The invariants are the seven `OwnerInvariant` values. The bound is 2
workers and 2 signals. The epoch stops at 3 per PR, about 658,000 states,
and at 4 nightly, about 7,100,000 states.

`EveryNemesisSeen` must hold in one behavior of the design. It is the
negation of `SomeNemesisUnseen`, which `OwnerLeaseReach.cfg` must break. The
behavior reaches all five tags: `stale-commit`, `flap`,
`append-after-expiry`, `renew-in-flight` and `ack-before-bump`.

Each red `OwnerLease` row of `formal/tla/models.txt` names a defect and an
invariant. The test reads the manifest and each config, so a new TLA+
counter-example must fail here too:

| Defect | Must break |
|---|---|
| `no-fence` | `OneOwnerPerEpoch` |
| `unlocked-fence` | `OneOwnerPerEpoch` |
| `holder-fence` | `DecisionOnFreshHistory` |
| `keep-cache` | `DecisionOnFreshHistory` |
| `pipeline` | `DecisionOnFreshHistory` |
| `renew-bumps` | `OwnerNeverFencesItself` |
| `memory-ack` | `NoAckedSignalLost` |

## The tests

| Test | Asserts |
|---|---|
| `the_claim_epoch_keeps_every_invariant` | The design keeps every invariant and reaches every witness. |
| `the_claim_epoch_keeps_every_invariant_across_two_rows` | Two rows keep every invariant. |
| `the_pre_fix_guard_lets_a_stale_owner_finish_the_task` | The pre-fix guard breaks `TerminalByCurrentClaim` with the #1789 trace. |
| `the_pre_fix_guard_breaks_the_other_owner_invariants` | The pre-fix guard breaks three more invariants. |
| `every_dst_invariant_is_a_property` | Each `dst::Invariant` is an `always` property. |
| `every_tla_counter_example_has_a_stateright_match` | Each red `ActivityClaim` row has a test here. |
| `the_design_keeps_every_invariant_and_reaches_every_nemesis` | The owner design keeps every invariant and reaches every nemesis. |
| `every_tla_defect_fails_here_on_the_same_invariant` | Each TLA+ defect breaks its invariant here. |
| `the_no_fence_counter_example_is_a_stale_owner_commit` | The `no-fence` event comes from a worker that pinned an older epoch. |
| `every_simulator_invariant_is_a_property` | Each `OwnerInvariant` is an `always` property. |

## Mutation check

Two mutations of the oracles show that the passing checks can fail:

- Remove `row.attempt == claim.attempt` from `OracleStore::accepts`. The
  design check breaks `TerminalByCurrentClaim`. The same worker claims its
  requeued row again, so the worker id alone cannot tell the claims apart.
- Remove the fence-lock check from `OracleOwnerStore::acquire`. The owner
  design check breaks `OneOwnerPerEpoch`.

## Limits

- A pass holds within the bound only. The nightly bounds are larger, not
  unbounded.
- The models check safety only. Stateright checks an `eventually` property
  on acyclic paths only, and a bound ends every path. A liveness check would
  report the bound, not the protocol.
- The owner half of the owner-lease model is a copy of `dst::owner`. A change
  to that scheduler must change this model too. The defect test catches a
  defect that the copy loses, but not every drift.
- The activity clock counts ticks, not milliseconds. One tick is the whole
  liveness window.
- Like DST, the models do not run `worker.rs`. They check the oracles. The
  differential test checks that the oracles match the SQL.

## Extend a model

1. Add the action to `actions` and its effect to `next_state`. Keep one action
   for each oracle operation.
2. If the change adds an invariant to `dst::Invariant` or `OwnerInvariant`,
   add an `always` property. The `every_*_is_a_property` tests fail until
   you do.
3. If the change adds a red row to `formal/tla/models.txt`, the
   cross-check tests fail until a test here breaks the same invariant.
4. Keep the per-PR bound under about 5 seconds in a debug build. Put a larger
   bound behind `HARVEST_STATERIGHT_DEEP`.
5. Update this page.
